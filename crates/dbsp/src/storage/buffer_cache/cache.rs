//! A buffer-cache based on LRU eviction.
//!
//! This is a layer over a storage backend that adds a cache of a
//! client-provided function of the blocks.
use std::fmt::Debug;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::{collections::BTreeMap, ops::Range};

use futures::future::Either;
use futures::stream::FuturesUnordered;
use futures::{future, pin_mut, StreamExt};
use tokio::sync::{oneshot, watch};

use crate::storage::backend::{BlockLocation, FileId, FileReader, FileWriter};
use crate::storage::file::reader::{CorruptionError, Error};
use crate::{storage::backend::StorageError, storage::buffer_cache::FBuf};

/// A key for the block cache.
///
/// The block size could be part of the key, but we'll never read a given offset
/// with more than one size so it's also not necessary.
///
/// It's important that the sort order is by `fd` first and `offset` second, so
/// that [`CacheKey::fd_range`] can work.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct CacheKey {
    /// File being cached.
    file_id: FileId,

    /// Offset in file.
    offset: u64,
}

impl CacheKey {
    fn new(file_id: FileId, offset: u64) -> Self {
        Self { file_id, offset }
    }

    /// Returns a range that would contain all of the blocks for the specified
    /// `fd`.
    fn file_range(file_id: FileId) -> Range<CacheKey> {
        Self { file_id, offset: 0 }..Self {
            file_id: file_id.after(),
            offset: 0,
        }
    }
}

/// A value in the block cache.
struct CacheValue<E>
where
    E: CacheEntry,
{
    /// Cached interpretation of `block`.
    aux: E,

    /// Serial number for LRU purposes.  Blocks with higher serial numbers have
    /// been used more recently.
    serial: u64,
}

pub trait CacheEntry: Clone + Send
where
    Self: Sized,
{
    fn cost(&self) -> usize;
    fn from_read(raw: Arc<FBuf>, location: BlockLocation) -> Result<Self, Error>;
    fn from_write(raw: Arc<FBuf>, location: BlockLocation) -> Result<Self, Error>;
}

struct CacheInner<E>
where
    E: CacheEntry,
{
    /// Cache contents.
    cache: BTreeMap<CacheKey, CacheValue<E>>,

    /// Map from LRU serial number to cache key.  The element with the smallest
    /// serial number was least recently used.
    lru: BTreeMap<u64, CacheKey>,

    /// Serial number to use the next time we touch a block.
    next_serial: u64,

    /// Sum over `cache[*].block.cost()`.
    cur_cost: usize,

    /// Maximum `size`, in bytes.
    max_cost: usize,
}

impl<E> Default for BufferCache<E>
where
    E: CacheEntry,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<E> CacheInner<E>
where
    E: CacheEntry,
{
    fn new() -> Self {
        Self {
            cache: BTreeMap::new(),
            lru: BTreeMap::new(),
            next_serial: 0,
            cur_cost: 0,
            max_cost: 1024 * 1024 * 256,
        }
    }

    #[allow(dead_code)]
    fn check_invariants(&self) {
        assert_eq!(self.cache.len(), self.lru.len());
        let mut cost = 0;
        for (key, value) in self.cache.iter() {
            assert_eq!(self.lru.get(&value.serial), Some(key));
            cost += value.aux.cost();
        }
        for (serial, key) in self.lru.iter() {
            assert_eq!(self.cache.get(key).unwrap().serial, *serial);
        }
        assert_eq!(cost, self.cur_cost);
    }

    fn debug_check_invariants(&self) {
        #[cfg(debug_assertions)]
        self.check_invariants()
    }

    fn delete_file(&mut self, file_id: FileId) {
        let offsets: Vec<_> = self
            .cache
            .range(CacheKey::file_range(file_id))
            .map(|(k, v)| (k.offset, v.serial))
            .collect();
        for (offset, serial) in offsets {
            self.lru.remove(&serial).unwrap();
            self.cur_cost -= self
                .cache
                .remove(&CacheKey::new(file_id, offset))
                .unwrap()
                .aux
                .cost();
        }
        self.debug_check_invariants();
    }

    fn get(&mut self, key: CacheKey) -> Option<&E> {
        if let Some(value) = self.cache.get_mut(&key) {
            self.lru.remove(&value.serial);
            value.serial = self.next_serial;
            self.lru.insert(value.serial, key);
            self.next_serial += 1;
            Some(&value.aux)
        } else {
            None
        }
    }

    fn evict_to(&mut self, max_size: usize) {
        while self.cur_cost > max_size {
            let (_serial, key) = self.lru.pop_first().unwrap();
            let value = self.cache.remove(&key).unwrap();
            self.cur_cost -= value.aux.cost();
        }
        self.debug_check_invariants();
    }

    fn insert(&mut self, key: CacheKey, aux: E) {
        let cost = aux.cost();
        self.evict_to(self.max_cost.saturating_sub(cost));
        if let Some(old_value) = self.cache.insert(
            key,
            CacheValue {
                aux,
                serial: self.next_serial,
            },
        ) {
            self.lru.remove(&old_value.serial);
            self.cur_cost -= old_value.aux.cost();
        }
        self.lru.insert(self.next_serial, key);
        self.cur_cost += cost;
        self.next_serial += 1;
        self.debug_check_invariants();
    }
}

/// A cache on top of a storage [backend](crate::storage::backend).
pub struct BufferCache<E>
where
    E: CacheEntry,
{
    inner: Mutex<CacheInner<E>>,
}

impl<E> Debug for BufferCache<E>
where
    E: CacheEntry,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BufferCache").finish()
    }
}

impl<E> Default for CacheInner<E>
where
    E: CacheEntry,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<E> BufferCache<E>
where
    E: CacheEntry,
{
    /// Creates a new cache on top of `backend`.
    ///
    /// It's best to use a single `StorageCache` for all uses of a given
    /// `backend`, because otherwise the cache will end up with duplicates.
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(CacheInner::new()),
        }
    }

    /// Reads `size` bytes at the given `offset` from `file` from the cache, or
    /// from the underlying storage if it is not cached.
    pub fn read<F, T>(
        &self,
        file: &dyn FileReader,
        location: BlockLocation,
        convert: F,
    ) -> Result<T, Error>
    where
        F: Fn(&E) -> Result<T, ()>,
    {
        let key = CacheKey::new(file.file_id(), location.offset);
        if let Some(aux) = self.inner.lock().unwrap().get(key) {
            return convert(aux)
                .map_err(|_| Error::Corruption(CorruptionError::BadBlockType(location)));
        }

        let block = file.read_block(location)?;
        let aux = E::from_read(block, location)?;
        self.inner.lock().unwrap().insert(key, aux.clone());
        convert(&aux).map_err(|_| Error::Corruption(CorruptionError::BadBlockType(location)))
    }

    pub fn write(
        &self,
        file: &mut dyn FileWriter,
        offset: u64,
        data: FBuf,
    ) -> Result<(), StorageError> {
        let data = file.write_block(offset, data)?;
        let location = BlockLocation::new(offset, data.len()).unwrap();
        let aux = E::from_write(data, location).unwrap();
        self.inner
            .lock()
            .unwrap()
            .insert(CacheKey::new(file.file_id(), offset), aux);
        Ok(())
    }

    pub fn evict(&self, file: &dyn FileReader) {
        self.inner.lock().unwrap().delete_file(file.file_id());
    }
}

/// Context for asynchronous cached I/O.
///
/// This context allows for batching cached I/O to a [FileReader] in async Rust.
/// Each async task uses [Self::read] to do I/O, which blocks if the read cannot
/// be satisfied from cache. [Self::execute_tasks] runs all of the tasks in
/// parallel, launching a round of I/O whenever all of the unfinished tasks
/// block.
pub struct AsyncCacheContext<E>
where
    E: CacheEntry,
{
    /// The underlying cache.
    cache: Arc<BufferCache<E>>,

    /// Identifies the file we're reading.
    file_id: FileId,

    /// [BTreeMap] is a better choice than `HashMap` for this because issuing
    /// I/O in sorted order is usually a good idea.
    requests: Mutex<BTreeMap<u64, (usize, Vec<oneshot::Sender<Result<Arc<FBuf>, StorageError>>>)>>,

    n_requests: watch::Sender<usize>,
}

impl<E> AsyncCacheContext<E>
where
    E: CacheEntry,
{
    pub fn new(cache: Arc<BufferCache<E>>, file: &dyn FileReader) -> Self {
        Self {
            cache,
            file_id: file.file_id(),
            requests: Mutex::new(BTreeMap::new()),
            n_requests: watch::channel(0).0,
        }
    }

    /// Reads the bytes at `location` from the file.  If the read can be
    /// satisfied from cache, this completes quickly. Otherwise, it blocks until
    /// [Self::execute_tasks] runs I/O for all of the blocking tasks in a batch.
    pub async fn read<F, T>(&self, location: BlockLocation, convert: F) -> Result<T, Error>
    where
        F: Fn(&E) -> Result<T, ()>,
    {
        let key = CacheKey::new(self.file_id, location.offset);
        if let Some(aux) = self.cache.inner.lock().unwrap().get(key) {
            return convert(aux)
                .map_err(|_| Error::Corruption(CorruptionError::BadBlockType(location)));
        }

        let (sender, receiver) = oneshot::channel();

        {
            let mut requests = self.requests.lock().unwrap();
            let (size2, senders) = requests
                .entry(location.offset)
                .or_insert((location.size, Vec::new()));
            debug_assert_eq!(location.size, *size2);
            senders.push(sender);
        }

        self.n_requests.send_modify(|n| *n += 1);
        let block = receiver.await.unwrap()?; // XXX unwrap
        let aux = E::from_read(block, location)?;
        self.cache.inner.lock().unwrap().insert(key, aux.clone());
        convert(&aux).map_err(|_| Error::Corruption(CorruptionError::BadBlockType(location)))
    }

    /// Waits until `goal` threads have blocked on I/O in [Self::read].
    pub async fn wait(&self, goal: usize) {
        self.n_requests
            .subscribe()
            .wait_for(|n| *n >= goal)
            .await
            .unwrap();
    }

    /// Runs all of the pending I/O and wakes up threads blocked in [Self::read].
    pub async fn run_io_batch<R>(&self, file: &R)
    where
        R: FileReader + ?Sized,
    {
        let requests = std::mem::take(&mut *self.requests.lock().unwrap());
        let n_requests = requests
            .values()
            .map(|(_offset, result_senders)| result_senders.len())
            .sum::<usize>();
        self.n_requests.send_modify(|n| *n -= n_requests);
        let blocks = requests
            .iter()
            .map(|(offset, (size, _))| *offset..*offset + *size as u64)
            .collect::<Vec<_>>();
        let (sender, receiver) = oneshot::channel();
        file.read_async(
            blocks,
            Box::new(
                move |result| sender.send(result).unwrap(), // XXX unwrap
            ),
        );
        let result = receiver.await.unwrap(); // XXX unwrap
        for (result, (_, result_senders)) in result.into_iter().zip(requests.into_values()) {
            for result_sender in result_senders {
                result_sender.send(result.clone()).unwrap(); // XXX unwrap
            }
        }
    }

    /// Execute all of the `tasks` on `file` until all of them run to
    /// completion, returning a vector of their return values in the same order.
    ///
    /// Internally, this runs in a series of rounds, where in each round we run
    /// each task until it either completes or blocks on I/O on `file`. At the
    /// end of the round, if any tasks are still left, we do all of the I/O on
    /// all of the tasks in a single batch of reads.
    pub async fn execute_tasks<F, T, R>(
        &self,
        file: &R,
        tasks: impl IntoIterator<Item = F>,
    ) -> Vec<T>
    where
        F: Future<Output = T>,
        R: FileReader + ?Sized,
    {
        let mut tasks = tasks
            .into_iter()
            .enumerate()
            .map(|(index, future)| async move { (index, future.await) })
            .collect::<FuturesUnordered<_>>();
        let mut outputs = Vec::with_capacity(tasks.len());
        for _ in 0..tasks.len() {
            outputs.push(None);
        }
        while !tasks.is_empty() {
            let wait = self.wait(tasks.len());
            pin_mut!(wait);
            match future::select(tasks.next(), wait).await {
                Either::Left((Some((index, output)), _)) => {
                    // A task has completed.
                    outputs[index] = Some(output);
                }
                Either::Left((None, _)) => {
                    // Unreachable because we know that `tasks` is not empty.
                    unreachable!()
                }
                Either::Right((_, _)) => {
                    // All of the tasks we launched have blocked on I/O. Launch a batch
                    // of I/O and wait for it to complete.
                    self.run_io_batch(file).await;
                }
            }
        }
        outputs.into_iter().map(|output| output.unwrap()).collect()
    }
}
