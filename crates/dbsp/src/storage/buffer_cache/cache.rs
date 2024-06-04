//! A buffer-cache based on LRU eviction.
//!
//! This is a layer over a storage backend that adds a cache of a
//! client-provided function of the blocks.
use crossbeam_skiplist::SkipMap;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::{
    ops::Range,
    path::{Path, PathBuf},
};

use crc32c::crc32c;
use metrics::counter;

use crate::storage::backend::Backend;
use crate::storage::file::reader::{CorruptionError, Error};
use crate::{
    storage::backend::{
        metrics::{BUFFER_CACHE_HIT, BUFFER_CACHE_MISS},
        FileHandle, ImmutableFileHandle, Storage, StorageError,
    },
    storage::buffer_cache::FBuf,
    Runtime,
};

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
    serial: AtomicU64,
}

pub trait CacheEntry: Clone + Send
where
    Self: Sized,
{
    fn cost(&self) -> usize;
    fn from_read(raw: Arc<FBuf>, offset: u64, size: usize) -> Result<Self, Error>;
    fn from_write(raw: Arc<FBuf>, offset: u64, size: usize) -> Result<Self, Error>;
}

struct CacheInner<E>
where
    E: CacheEntry,
{
    /// Cache contents.
    cache: SkipMap<CacheKey, CacheValue<E>>,

    /// Map from LRU serial number to cache key.  The element with the smallest
    /// serial number was least recently used.
    lru: SkipMap<u64, CacheKey>,

    /// Serial number to use the next time we touch a block.
    next_serial: AtomicU64,

    /// Sum over `cache[*].block.cost()`.
    cur_cost: AtomicUsize,

    /// Maximum `size`, in bytes.
    max_cost: AtomicUsize,
}

impl<E> Default for BufferCache<E>
where
    E: CacheEntry + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<E> CacheInner<E>
where
    E: CacheEntry + 'static,
{
    fn new() -> Self {
        Self {
            cache: SkipMap::new(),
            lru: SkipMap::new(),
            next_serial: AtomicU64::new(0),
            cur_cost: AtomicUsize::new(0),
            max_cost: AtomicUsize::new(1024 * 1024 * 128),
        }
    }

    fn delete_file(&self, fd: i64) {
        let offsets: Vec<_> = self
            .cache
            .range(CacheKey::fd_range(fd))
            .map(|e| (e.key().offset, e.value().serial.load(Ordering::Relaxed)))
            .collect();
        for (offset, serial) in offsets {
            self.lru.remove(&serial).unwrap();
            let to_remove = self.cache.remove(&CacheKey { fd, offset }).unwrap();
            self.cur_cost
                .fetch_sub(to_remove.value().aux.cost(), Ordering::Relaxed);
        }
    }

    fn get(&self, key: CacheKey) -> Option<E> {
        if let Some(entry) = self.cache.get(&key) {
            self.lru
                .remove(&entry.value().serial.load(Ordering::Relaxed));
            entry.value().serial.store(
                self.next_serial.fetch_add(1, Ordering::Relaxed),
                Ordering::Relaxed,
            );
            self.lru
                .insert(entry.value().serial.load(Ordering::Relaxed), key);
            Some(entry.value().aux.clone())
        } else {
            None
        }
    }

    fn evict_to(&self, max_size: usize) {
        while self.cur_cost.load(Ordering::Relaxed) > max_size {
            let entry = self.cache.pop_front().unwrap();
            self.lru
                .remove(&entry.value().serial.load(Ordering::Relaxed));
            self.cur_cost
                .fetch_sub(entry.value().aux.cost(), Ordering::Relaxed);
        }
    }

    fn insert(&self, key: CacheKey, aux: E) {
        let max_cost = self.max_cost.load(Ordering::Relaxed);
        let cost = aux.cost();
        self.evict_to(max_cost.saturating_sub(cost));

        if let Some(entry) = self.cache.remove(&key) {
            self.lru
                .remove(&entry.value().serial.load(Ordering::Relaxed));
            self.cur_cost
                .fetch_sub(entry.value().aux.cost(), Ordering::Relaxed);
        }
        let next_serial = self.next_serial.fetch_add(1, Ordering::Relaxed);
        self.cache.insert(
            key,
            CacheValue {
                aux,
                serial: AtomicU64::new(next_serial),
            },
        );

        self.lru.insert(next_serial, key);
        self.cur_cost.fetch_add(cost, Ordering::Relaxed);
    }
}

/// A cache on top of a storage [backend](crate::storage::backend).
pub struct BufferCache<E>
where
    E: CacheEntry,
{
    inner: CacheInner<E>,
}

impl<E> BufferCache<E>
where
    E: CacheEntry + 'static,
{
    /// Creates a new cache on top of `backend`.
    ///
    /// It's best to use a single `StorageCache` for all uses of a given
    /// `backend`, because otherwise the cache will end up with duplicates.
    pub fn new() -> Self {
        Self {
            inner: CacheInner::new(),
        }
    }

    /// Returns the (thread-local) storage backend.
    pub fn backend() -> Rc<Backend> {
        thread_local! {
            pub static DEFAULT_BACKEND: Rc<Backend> = Rc::new(Runtime::new_backend());
        }
        DEFAULT_BACKEND.with(|rc| rc.clone())
    }

    pub fn read<F, T>(
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
        if let Some(aux) = self.inner.get(key) {
            counter!(BUFFER_CACHE_HIT).increment(1);
            return convert(&aux)
                .map_err(|_| Error::Corruption(CorruptionError::BadBlockType { offset, size }));
        }

        counter!(BUFFER_CACHE_MISS).increment(1);

        let block = Self::backend().read_block(fd, offset, size)?;
        let aux = E::from_read(block, offset, size)?;
        let retval = convert(&aux)
            .map_err(|_| Error::Corruption(CorruptionError::BadBlockType { offset, size }));
        self.inner.insert(key, aux.clone());
        retval
    }

    pub fn write(&self, fd: &FileHandle, offset: u64, mut data: FBuf) -> Result<(), StorageError> {
        let checksum = crc32c(&data[4..]).to_le_bytes();
        data[..4].copy_from_slice(checksum.as_slice());

        let data = Self::backend().write_block(fd, offset, data)?;
        let size = data.len();
        let aux = E::from_write(data, offset, size).unwrap();
        self.inner.insert(CacheKey::from((fd, offset)), aux);
        Ok(())
    }
}

impl<E> Storage for BufferCache<E>
where
    E: CacheEntry + 'static,
{
    fn create(&self) -> Result<FileHandle, StorageError> {
        Self::backend().create()
    }
    fn create_named(&self, name: &Path) -> Result<FileHandle, StorageError> {
        Self::backend().create_named(name)
    }
    fn open(&self, name: &Path) -> Result<ImmutableFileHandle, StorageError> {
        Self::backend().open(name)
    }
    fn delete(&self, fd: ImmutableFileHandle) -> Result<(), StorageError> {
        self.inner.delete_file((&fd).into());
        Self::backend().delete(fd)
    }
    fn delete_mut(&self, fd: FileHandle) -> Result<(), StorageError> {
        self.inner.delete_file((&fd).into());
        Self::backend().delete_mut(fd)
    }

    fn evict(&self, fd: ImmutableFileHandle) -> Result<(), StorageError> {
        self.inner.delete_file((&fd).into());
        Ok(())
    }

    fn base(&self) -> PathBuf {
        Self::backend().base()
    }

    fn write_block(
        &self,
        fd: &FileHandle,
        offset: u64,
        data: FBuf,
    ) -> Result<Arc<FBuf>, StorageError> {
        let data = Self::backend().write_block(fd, offset, data)?;
        let size = data.len();
        let aux = E::from_write(data.clone(), offset, size).unwrap();
        self.inner.insert(CacheKey::from((fd, offset)), aux);
        Ok(data)
    }

    fn complete(&self, fd: FileHandle) -> Result<(ImmutableFileHandle, PathBuf), StorageError> {
        Self::backend().complete(fd)
    }

    fn prefetch(&self, fd: &ImmutableFileHandle, offset: u64, size: usize) {
        Self::backend().prefetch(fd, offset, size)
    }

    fn read_block(
        &self,
        fd: &ImmutableFileHandle,
        offset: u64,
        size: usize,
    ) -> Result<Arc<FBuf>, StorageError> {
        Self::backend().read_block(fd, offset, size)
    }

    fn get_size(&self, fd: &ImmutableFileHandle) -> Result<u64, StorageError> {
        Self::backend().get_size(fd)
    }
}
