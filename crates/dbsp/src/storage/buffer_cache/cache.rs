//! A buffer-cache based on LRU eviction.
//!
//! This is a layer over a storage backend that adds a cache of a
//! client-provided function of the blocks.
use std::borrow::Cow;
use std::fmt::Debug;
use std::ops::{Add, AddAssign};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::{collections::BTreeMap, ops::Range};

use enum_map::{Enum, EnumMap};

use crate::circuit::metadata::{MetaItem, OperatorMeta};
use crate::storage::backend::{BlockLocation, FileId, FileReader};

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

impl<E> CacheInner<E>
where
    E: CacheEntry,
{
    fn new(max_cost: usize) -> Self {
        Self {
            cache: BTreeMap::new(),
            lru: BTreeMap::new(),
            next_serial: 0,
            cur_cost: 0,
            max_cost,
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

impl<E> BufferCache<E>
where
    E: CacheEntry,
{
    /// Creates a new cache on top of `backend`.
    ///
    /// It's best to use a single `StorageCache` for all uses of a given
    /// `backend`, because otherwise the cache will end up with duplicates.
    ///
    /// `max_cost` limits the size of the cache. It is denominated in terms of
    /// [CacheEntry::cost] for `E`.
    pub fn new(max_cost: usize) -> Self {
        Self {
            inner: Mutex::new(CacheInner::new(max_cost)),
        }
    }

    pub fn get(
        &self,
        file: &dyn FileReader,
        location: BlockLocation,
        stats: &AtomicCacheStats,
    ) -> Option<E> {
        let result = self
            .inner
            .lock()
            .unwrap()
            .get(CacheKey::new(file.file_id(), location.offset))
            .cloned();

        stats.record(
            if result.is_some() {
                CacheAccess::Hit
            } else {
                CacheAccess::Miss
            },
            location,
        );

        result
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

    /// Returns `(cur_cost, max_cost)`, reporting the amount of the cache that
    /// is currently used and the maximum value, both denominated in terms of
    /// [CacheEntry::cost] for `E`.
    pub fn occupancy(&self) -> (usize, usize) {
        let inner = self.inner.lock().unwrap();
        (inner.cur_cost, inner.max_cost)
    }
}

/// Cache statistics that can be accessed atomically for multithread updates.
#[derive(Debug, Default)]
pub struct AtomicCacheStats(EnumMap<CacheAccess, AtomicCacheCounts>);

impl AtomicCacheStats {
    /// Records that `location` was access in the cache with effect `access`.
    fn record(&self, access: CacheAccess, location: BlockLocation) {
        self.0[access].count.fetch_add(1, Ordering::Relaxed);
        self.0[access]
            .bytes
            .fetch_add(location.size as u64, Ordering::Relaxed);
    }

    /// Reads out the statistics for processing.
    pub fn read(&self) -> CacheStats {
        CacheStats(EnumMap::from_fn(|access| self.0[access].read()))
    }
}

/// Whether a cache access was a hit or a miss.
#[derive(Copy, Clone, Debug, Enum)]
pub enum CacheAccess {
    /// Cache hit.
    Hit,

    /// Cache miss.
    Miss,
}

impl CacheAccess {
    fn name(&self) -> &'static str {
        match self {
            Self::Hit => "cache hits",
            Self::Miss => "cache misses",
        }
    }
}

/// Cache counts that can be accessed atomically for multithread updates.
#[derive(Debug, Default)]
pub struct AtomicCacheCounts {
    /// Number of accessed blocks.
    count: AtomicU64,

    /// Total bytes across all the accesses.
    bytes: AtomicU64,
}

impl AtomicCacheCounts {
    /// Reads out the counts for processing.
    fn read(&self) -> CacheCounts {
        CacheCounts {
            count: self.count.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
        }
    }
}

/// Cache access statistics.
#[derive(Copy, Clone, Debug, Default)]
pub struct CacheStats(pub EnumMap<CacheAccess, CacheCounts>);

impl CacheStats {
    /// Are the statistics all zero?
    pub fn is_empty(&self) -> bool {
        self.0.values().all(CacheCounts::is_empty)
    }

    /// Adds metadata for these cache statistics to `meta`.
    pub fn metadata(&self, meta: &mut OperatorMeta) {
        meta.extend(
            self.0
                .iter()
                .map(|(access, counts)| (Cow::from(access.name()), counts.meta_item())),
        );

        let hits = self.0[CacheAccess::Hit].count;
        let misses = self.0[CacheAccess::Miss].count;
        meta.extend(metadata! {
            "hit rate" => MetaItem::Percent { numerator: hits, denominator: hits + misses }
        });
    }
}

impl Add for CacheStats {
    type Output = Self;

    fn add(mut self, rhs: Self) -> Self::Output {
        self.add_assign(rhs);
        self
    }
}

impl AddAssign for CacheStats {
    fn add_assign(&mut self, rhs: Self) {
        for (access, counts) in &mut self.0 {
            *counts += rhs.0[access];
        }
    }
}

/// Cache counts that can be accessed atomically for multithread updates.
#[derive(Copy, Clone, Debug, Default)]
pub struct CacheCounts {
    /// Number of accessed blocks.
    pub count: u64,

    /// Total bytes across all the accesses.
    pub bytes: u64,
}

impl CacheCounts {
    /// Are the counts all zero?
    pub fn is_empty(&self) -> bool {
        self.count == 0 && self.bytes == 0
    }

    /// Returns a [MetaItem] for these cache counts.
    pub fn meta_item(&self) -> MetaItem {
        MetaItem::count_and_bytes(self.count, self.bytes)
    }
}

impl AddAssign for CacheCounts {
    fn add_assign(&mut self, rhs: Self) {
        self.count += rhs.count;
        self.bytes += rhs.bytes;
    }
}
