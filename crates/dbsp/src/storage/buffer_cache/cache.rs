//! A buffer-cache based on LRU eviction.
//!
//! This is a layer over a storage backend that adds a cache of a
//! client-provided function of the blocks.
use std::{
    cell::RefCell,
    collections::BTreeMap,
    future::Future,
    ops::Range,
    path::{Path, PathBuf},
    rc::Rc,
};

use crc32c::crc32c;
use metrics::counter;

use crate::{
    storage::backend::{
        metrics::{BUFFER_CACHE_HIT, BUFFER_CACHE_MISS},
        FileHandle, ImmutableFileHandle, StorageControl, StorageError, StorageExecutor,
        StorageRead, StorageWrite,
    },
    storage::buffer_cache::FBuf,
};

use crate::storage::file::reader::{CorruptionError, Error};

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
    fd: i64,

    /// Offset in file.
    offset: u64,
}

impl CacheKey {
    /// Returns a range that would contain all of the blocks for the specified
    /// `fd`.
    fn fd_range(fd: i64) -> Range<CacheKey> {
        Self { fd, offset: 0 }..Self {
            fd: fd + 1,
            offset: 0,
        }
    }
}

impl From<(&FileHandle, u64)> for CacheKey {
    fn from(source: (&FileHandle, u64)) -> Self {
        Self {
            fd: source.0.into(),
            offset: source.1,
        }
    }
}

impl From<(&ImmutableFileHandle, u64)> for CacheKey {
    fn from(source: (&ImmutableFileHandle, u64)) -> Self {
        Self {
            fd: source.0.into(),
            offset: source.1,
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

pub trait CacheEntry: Clone
where
    Self: Sized,
{
    fn cost(&self) -> usize;
    fn from_read(raw: Rc<FBuf>, offset: u64, size: usize) -> Result<Self, Error>;
    fn from_write(raw: Rc<FBuf>, offset: u64, size: usize) -> Result<Self, Error>;
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
            max_cost: 1024 * 1024 * 128,
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

    fn delete_file(&mut self, fd: i64) {
        let offsets: Vec<_> = self
            .cache
            .range(CacheKey::fd_range(fd))
            .map(|(k, v)| (k.offset, v.serial))
            .collect();
        for (offset, serial) in offsets {
            self.lru.remove(&serial).unwrap();
            self.cur_cost -= self
                .cache
                .remove(&CacheKey { fd, offset })
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
            let (_key, value) = self.cache.pop_first().unwrap();
            self.lru.remove(&value.serial);
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

/// A cache on top of a storage [backend](crate::backend).
pub struct BufferCache<B, E>
where
    B: StorageRead + StorageWrite + StorageControl + StorageExecutor,
    E: CacheEntry,
{
    backend: Rc<B>,
    inner: RefCell<CacheInner<E>>,
}

impl<B, E> BufferCache<B, E>
where
    B: StorageRead + StorageWrite + StorageControl + StorageExecutor,
    E: CacheEntry,
{
    /// Creates a new cache on top of `backend`.
    ///
    /// It's best to use a single `StorageCache` for all uses of a given
    /// `backend`, because otherwise the cache will end up with duplicates.  The
    /// easiest way is to get the cache from
    /// [`BufferCache::default_for_thread()`].
    pub fn new(backend: Rc<B>) -> Self {
        Self {
            backend,
            inner: RefCell::new(CacheInner::new()),
        }
    }

    pub async fn read<F, T>(
        &self,
        fd: &ImmutableFileHandle,
        offset: u64,
        size: usize,
        convert: F,
    ) -> Result<T, Error>
    where
        F: Fn(&E) -> Result<T, ()>,
    {
        let key = CacheKey::from((fd, offset));
        if let Some(aux) = self.inner.borrow_mut().get(key) {
            counter!(BUFFER_CACHE_HIT).increment(1);
            return convert(aux)
                .map_err(|_| Error::Corruption(CorruptionError::BadBlockType { offset, size }));
        }

        counter!(BUFFER_CACHE_MISS).increment(1);

        let block = self.backend.read_block(fd, offset, size).await?;
        let aux = E::from_read(block, offset, size)?;
        let retval = convert(&aux)
            .map_err(|_| Error::Corruption(CorruptionError::BadBlockType { offset, size }));
        self.inner.borrow_mut().insert(key, aux.clone());
        retval
    }

    pub async fn write(
        &self,
        fd: &FileHandle,
        offset: u64,
        mut data: FBuf,
    ) -> Result<(), StorageError> {
        let checksum = crc32c(&data[4..]).to_le_bytes();
        data[..4].copy_from_slice(checksum.as_slice());

        let data = self.backend.write_block(fd, offset, data).await?;
        let size = data.len();
        let aux = E::from_write(data, offset, size).unwrap();
        self.inner
            .borrow_mut()
            .insert(CacheKey::from((fd, offset)), aux);
        Ok(())
    }

    pub async fn complete(
        &self,
        fd: FileHandle,
    ) -> Result<(ImmutableFileHandle, PathBuf), StorageError> {
        self.backend.complete(fd).await
    }

    pub async fn get_size(&self, fd: &ImmutableFileHandle) -> Result<u64, StorageError> {
        self.backend.get_size(fd).await
    }
}

impl<B, E> StorageControl for BufferCache<B, E>
where
    B: StorageRead + StorageWrite + StorageControl + StorageExecutor,
    E: CacheEntry,
{
    async fn create(&self) -> Result<FileHandle, StorageError> {
        self.backend.create().await
    }
    async fn create_named<P: AsRef<Path>>(&self, name: P) -> Result<FileHandle, StorageError> {
        self.backend.create_named(name).await
    }
    async fn delete(&self, fd: ImmutableFileHandle) -> Result<(), StorageError> {
        self.inner.borrow_mut().delete_file((&fd).into());
        self.backend.delete(fd).await
    }
    async fn delete_mut(&self, fd: FileHandle) -> Result<(), StorageError> {
        self.inner.borrow_mut().delete_file((&fd).into());
        self.backend.delete_mut(fd).await
    }
}

impl<B, E> StorageExecutor for BufferCache<B, E>
where
    B: StorageRead + StorageWrite + StorageControl + StorageExecutor,
    E: CacheEntry,
{
    fn block_on<F>(&self, future: F) -> F::Output
    where
        F: Future,
    {
        self.backend.block_on(future)
    }
}
