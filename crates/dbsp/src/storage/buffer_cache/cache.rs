//! A buffer-cache based on LRU eviction.
//!
//! This is a layer over a storage backend that adds a cache of a
//! client-provided function of the blocks.
use std::fmt::Debug;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::{
    collections::BTreeMap,
    ops::Range,
    path::{Path, PathBuf},
};

use crc32c::crc32c;

use crate::storage::backend::Backend;
use crate::storage::file::reader::{CorruptionError, Error};
use crate::{
    storage::backend::{FileHandle, ImmutableFileHandle, Storage, StorageError},
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
    serial: u64,
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
        if let Some(aux) = self.inner.lock().unwrap().get(key) {
            return convert(aux)
                .map_err(|_| Error::Corruption(CorruptionError::BadBlockType { offset, size }));
        }

        let block = Self::backend().read_block(fd, offset, size)?;
        let aux = E::from_read(block, offset, size)?;
        let retval = convert(&aux)
            .map_err(|_| Error::Corruption(CorruptionError::BadBlockType { offset, size }));
        self.inner.lock().unwrap().insert(key, aux.clone());
        retval
    }

    pub fn write(&self, fd: &FileHandle, offset: u64, mut data: FBuf) -> Result<(), StorageError> {
        let checksum = crc32c(&data[4..]).to_le_bytes();
        data[..4].copy_from_slice(checksum.as_slice());

        let data = Self::backend().write_block(fd, offset, data)?;
        let size = data.len();
        let aux = E::from_write(data, offset, size).unwrap();
        self.inner
            .lock()
            .unwrap()
            .insert(CacheKey::from((fd, offset)), aux);
        Ok(())
    }
}

impl<E> Storage for BufferCache<E>
where
    E: CacheEntry,
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
        self.inner.lock().unwrap().delete_file((&fd).into());
        Self::backend().delete(fd)
    }
    fn delete_mut(&self, fd: FileHandle) -> Result<(), StorageError> {
        self.inner.lock().unwrap().delete_file((&fd).into());
        Self::backend().delete_mut(fd)
    }

    fn evict(&self, fd: ImmutableFileHandle) -> Result<(), StorageError> {
        self.inner.lock().unwrap().delete_file((&fd).into());
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
        self.inner
            .lock()
            .unwrap()
            .insert(CacheKey::from((fd, offset)), aux);
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
