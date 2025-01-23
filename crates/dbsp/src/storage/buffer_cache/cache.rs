//! A buffer-cache based on LRU eviction.
//!
//! This is a layer over a storage backend that adds a cache of a
//! client-provided function of the blocks.
use std::fmt::Debug;
use std::path::Path;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::{collections::BTreeMap, ops::Range};

use crate::storage::backend::{Backend, FileId, FileReader, FileWriter, Storage};
use crate::{storage::backend::StorageError, Runtime};

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

pub trait CacheEntry: Clone + Send {
    fn cost(&self) -> usize;
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

    /// Returns the (thread-local) storage backend.
    pub fn backend() -> Rc<Backend> {
        thread_local! {
            pub static DEFAULT_BACKEND: Rc<Backend> = Rc::new(Runtime::new_backend());
        }
        DEFAULT_BACKEND.with(|rc| rc.clone())
    }

    pub fn get(&self, file: &dyn FileReader, offset: u64) -> Option<E> {
        self.inner
            .lock()
            .unwrap()
            .get(CacheKey::new(file.file_id(), offset))
            .cloned()
    }

    pub fn insert(&self, file_id: FileId, offset: u64, aux: E) {
        self.inner
            .lock()
            .unwrap()
            .insert(CacheKey::new(file_id, offset), aux);
    }

    pub fn evict(&self, file: &dyn FileReader) {
        self.inner.lock().unwrap().delete_file(file.file_id());
    }
}

impl<E> Storage for BufferCache<E>
where
    E: CacheEntry,
{
    fn create(&self) -> Result<Box<dyn FileWriter>, StorageError> {
        Self::backend().create()
    }
    fn create_named(&self, name: &Path) -> Result<Box<dyn FileWriter>, StorageError> {
        Self::backend().create_named(name)
    }

    fn open(&self, name: &Path) -> Result<Arc<dyn FileReader>, StorageError> {
        Self::backend().open(name)
    }
}
