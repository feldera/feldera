//! A buffer-cache based on LRU eviction.
//!
//! This is a layer over a storage backend that adds a cache of a
//! client-provided function of the blocks.
use std::any::Any;
use std::borrow::Cow;
use std::fmt::{Debug, Display};
use std::ops::{Add, AddAssign};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use std::{collections::BTreeMap, ops::Range};

use enum_map::{Enum, EnumMap};

use crate::circuit::metadata::{MetaItem, OperatorMeta};
use crate::circuit::runtime::ThreadType;
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
struct CacheValue {
    /// Cached interpretation of `block`.
    aux: Arc<dyn CacheEntry>,
}

pub trait CacheEntry: Send + Sync {
    fn cost(&self) -> usize;
    fn as_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync>;
}

struct CacheInner {
    /// Cache contents.
    cache: BTreeMap<CacheKey, CacheValue>,

    /// Sum over `cache[*].block.cost()`.
    cur_cost: usize,

    /// Maximum `size`, in bytes.
    max_cost: usize,
}

impl CacheInner {
    fn new(max_cost: usize) -> Self {
        Self {
            cache: BTreeMap::new(),
            cur_cost: 0,
            max_cost,
        }
    }

    fn delete_file(&mut self, file_id: FileId) {
        let offsets = self
            .cache
            .range(CacheKey::file_range(file_id))
            .map(|(k, _v)| k.offset)
            .collect::<Vec<_>>();
        for offset in offsets {
            self.cur_cost -= self
                .cache
                .remove(&CacheKey::new(file_id, offset))
                .unwrap()
                .aux
                .cost();
        }
    }

    fn get(&self, key: CacheKey) -> Option<Arc<dyn CacheEntry>> {
        self.cache.get(&key).map(|value| value.aux.clone())
    }

    fn insert(&mut self, key: CacheKey, aux: Arc<dyn CacheEntry>) {
        let cost = aux.cost();
        if let Some(old_value) = self.cache.insert(key, CacheValue { aux }) {
            self.cur_cost -= old_value.aux.cost();
        }
        self.cur_cost += cost;
    }
}

/// A cache on top of a storage [backend](crate::storage::backend).
pub struct BufferCache
{
    inner: RwLock<CacheInner>,
}

impl Debug for BufferCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BufferCache").finish()
    }
}

impl BufferCache {
    /// Creates a new cache on top of `backend`.
    ///
    /// It's best to use a single `StorageCache` for all uses of a given
    /// `backend`, because otherwise the cache will end up with duplicates.
    ///
    /// `max_cost` limits the size of the cache. It is denominated in terms of
    /// [CacheEntry::cost].
    pub fn new(max_cost: usize) -> Self {
        Self {
            inner: RwLock::new(CacheInner::new(max_cost)),
        }
    }

    pub fn get(
        &self,
        file: &dyn FileReader,
        location: BlockLocation,
    ) -> Option<Arc<dyn CacheEntry>> {
        self.inner
            .read()
            .unwrap()
            .get(CacheKey::new(file.file_id(), location.offset))
            .clone()
    }

    pub fn insert(&self, file_id: FileId, offset: u64, aux: Arc<dyn CacheEntry>) {
        self.inner
            .write()
            .unwrap()
            .insert(CacheKey::new(file_id, offset), aux);
    }

    pub fn evict(&self, file: &dyn FileReader) {
        self.inner.write().unwrap().delete_file(file.file_id());
    }

    /// Returns `(cur_cost, max_cost)`, reporting the amount of the cache that
    /// is currently used and the maximum value, both denominated in terms of
    /// [CacheEntry::cost] for `CacheEntry`.
    pub fn occupancy(&self) -> (usize, usize) {
        let inner = self.inner.read().unwrap();
        (inner.cur_cost, inner.max_cost)
    }
}

/// Cache statistics that can be accessed atomically for multithread updates.
#[derive(Debug, Default)]
pub struct AtomicCacheStats(EnumMap<ThreadType, EnumMap<CacheAccess, AtomicCacheCounts>>);

impl AtomicCacheStats {
    /// Records that `location` was access in the cache with effect `access`.
    pub fn record(&self, access: CacheAccess, duration: Duration, location: BlockLocation) {
        self.0[ThreadType::current()][access].record(duration, location);
    }

    /// Reads out the statistics for processing.
    pub fn read(&self) -> CacheStats {
        CacheStats(EnumMap::from_fn(|thread_type| {
            EnumMap::from_fn(|access| self.0[thread_type][access].read())
        }))
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

impl Display for CacheAccess {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Hit => write!(f, "cache hits"),
            Self::Miss => write!(f, "cache misses"),
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

    /// Elapsed time, in nanoseconds.
    elapsed_ns: AtomicU64,
}

impl AtomicCacheCounts {
    pub fn record(&self, duration: Duration, location: BlockLocation) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.bytes
            .fetch_add(location.size as u64, Ordering::Relaxed);
        self.elapsed_ns
            .fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
    }

    /// Reads out the counts for processing.
    fn read(&self) -> CacheCounts {
        CacheCounts {
            count: self.count.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
            elapsed: Duration::from_nanos(self.elapsed_ns.load(Ordering::Relaxed)),
        }
    }
}

/// Cache access statistics.
#[derive(Copy, Clone, Debug, Default)]
pub struct CacheStats(pub EnumMap<ThreadType, EnumMap<CacheAccess, CacheCounts>>);

impl CacheStats {
    /// Adds metadata for these cache statistics to `meta`, if they are nonzero.
    pub fn metadata(&self, meta: &mut OperatorMeta) {
        for (thread_type, accesses) in &self.0 {
            if !accesses.values().all(CacheCounts::is_empty) {
                meta.extend(accesses.iter().map(|(access, counts)| {
                    (
                        Cow::from(format!("{thread_type} {access}")),
                        counts.meta_item(),
                    )
                }));

                let hits = accesses[CacheAccess::Hit].count;
                let misses = accesses[CacheAccess::Miss].count;
                meta.extend([(
                    Cow::from(format!("{thread_type} cache hit rate")),
                    MetaItem::Percent {
                        numerator: hits,
                        denominator: hits + misses,
                    },
                )]);
            }
        }
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
        for (thread_type, accesses) in &mut self.0 {
            for (access, counts) in accesses {
                *counts += rhs.0[thread_type][access];
            }
        }
    }
}

/// Cache counts.
#[derive(Copy, Clone, Debug, Default, PartialEq)]
pub struct CacheCounts {
    /// Number of accessed blocks.
    pub count: u64,

    /// Total bytes across all the accesses.
    pub bytes: u64,

    /// Elapsed time.
    pub elapsed: Duration,
}

impl CacheCounts {
    /// Are the counts all zero?
    pub fn is_empty(&self) -> bool {
        self.count == 0 && self.bytes == 0
    }

    /// Returns a [MetaItem] for these cache counts.
    pub fn meta_item(&self) -> MetaItem {
        MetaItem::CacheCounts(*self)
    }
}

impl Add for CacheCounts {
    type Output = Self;

    fn add(mut self, rhs: Self) -> Self::Output {
        self.add_assign(rhs);
        self
    }
}

impl AddAssign for CacheCounts {
    fn add_assign(&mut self, rhs: Self) {
        self.count += rhs.count;
        self.bytes += rhs.bytes;
        self.elapsed += rhs.elapsed;
    }
}
