use feldera_types::config::dev_tweaks::BufferCacheStrategy;

use crate::{BufferCache, CacheEntry};
use std::any::Any;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::hash::RandomState;
use std::marker::PhantomData;
use std::ops::RangeBounds;
use std::sync::Mutex;

/// A weighted, thread-safe LRU cache.
pub struct LruCache<K, V, S = RandomState> {
    /// Mutable cache state guarded by a single mutex.
    inner: Mutex<CacheInner<K, V>>,
    /// Retains the public hash-builder type parameter used by shared builders.
    marker: PhantomData<fn() -> S>,
}

/// Mutable state for [`LruCache`].
struct CacheInner<K, V> {
    /// Cache contents.
    cache: BTreeMap<K, CacheValue<V>>,
    /// Map from LRU serial number to cache key.
    lru: BTreeMap<u64, K>,
    /// Serial number to use the next time we touch a key.
    next_serial: u64,
    /// Sum over `cache[*].aux.cost()`.
    cur_cost: usize,
    /// Maximum total cost.
    max_cost: usize,
}

/// Resident value stored by [`LruCache`].
struct CacheValue<V> {
    /// Cached value.
    aux: V,
    /// Recency serial used by the LRU queue.
    serial: u64,
}

impl<K, V, S> LruCache<K, V, S> {
    /// Default shard count reported by [`LruCache::shard_count`].
    pub const DEFAULT_SHARDS: usize = 1;
}

impl<K, V> LruCache<K, V, RandomState>
where
    K: Ord + Clone + Debug,
    V: CacheEntry + Clone,
{
    /// Creates a cache with the default hash builder.
    pub fn new(max_cost: usize) -> Self {
        Self::with_hasher(max_cost, RandomState::new())
    }
}

// explicit allow, we do have `is_empty` in the trait so this is a false positive
#[allow(clippy::len_without_is_empty)]
impl<K, V, S> LruCache<K, V, S>
where
    K: Ord + Clone + Debug,
    V: CacheEntry + Clone,
{
    /// Creates a cache with an explicit hash builder.
    pub fn with_hasher(max_cost: usize, _hash_builder: S) -> Self {
        Self {
            inner: Mutex::new(CacheInner::new(max_cost)),
            marker: PhantomData,
        }
    }

    /// Inserts or replaces `key` with `value`.
    pub fn insert(&self, key: K, value: V) {
        self.inner.lock().unwrap().insert(key, value);
    }

    /// Looks up `key` and returns a clone of the stored value.
    pub fn get(&self, key: &K) -> Option<V> {
        self.inner.lock().unwrap().get(key.clone())
    }

    /// Removes `key` if present and returns the removed value.
    pub fn remove(&self, key: &K) -> Option<V> {
        self.inner.lock().unwrap().remove(key)
    }

    /// Removes every entry whose key matches `predicate`.
    pub fn remove_if<F>(&self, predicate: F)
    where
        F: Fn(&K) -> bool,
    {
        self.inner.lock().unwrap().remove_if(predicate)
    }

    /// Removes every entry whose key falls within `range`.
    pub fn remove_range<R>(&self, range: R) -> usize
    where
        R: RangeBounds<K>,
    {
        self.inner.lock().unwrap().remove_range(range)
    }

    /// Returns `true` if `key` is currently resident.
    pub fn contains_key(&self, key: &K) -> bool {
        self.inner.lock().unwrap().contains_key(key)
    }

    /// Returns the number of resident entries.
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    /// Returns the total resident cost.
    pub fn total_charge(&self) -> usize {
        self.inner.lock().unwrap().cur_cost
    }

    /// Returns the configured total cost capacity.
    pub fn total_capacity(&self) -> usize {
        self.inner.lock().unwrap().max_cost
    }

    /// Returns the number of shards reported by this backend.
    pub fn shard_count(&self) -> usize {
        Self::DEFAULT_SHARDS
    }

    /// Returns `(used_charge, capacity)` for shard `idx`.
    ///
    /// # Panics
    ///
    /// Panics if `idx != 0`.
    #[cfg(test)]
    pub fn shard_usage(&self, idx: usize) -> (usize, usize) {
        assert_eq!(idx, 0, "shard index out of bounds");
        let inner = self.inner.lock().unwrap();
        (inner.cur_cost, inner.max_cost)
    }

    #[cfg(test)]
    pub(crate) fn validate_invariants(&self) {
        self.inner.lock().unwrap().check_invariants();
    }
}

impl<K, V, S> BufferCache<K, V> for LruCache<K, V, S>
where
    K: Ord + Clone + Debug + Send + Sync + 'static,
    V: CacheEntry + Clone + Send + Sync + 'static,
    S: Send + Sync + 'static,
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn strategy(&self) -> BufferCacheStrategy {
        BufferCacheStrategy::Lru
    }

    fn insert(&self, key: K, value: V) {
        self.insert(key, value);
    }

    fn get(&self, key: K) -> Option<V> {
        self.inner.lock().unwrap().get(key)
    }

    fn remove(&self, key: &K) -> Option<V> {
        self.remove(key)
    }

    fn remove_if(&self, predicate: &dyn Fn(&K) -> bool) {
        self.remove_if(|key| predicate(key))
    }

    fn contains_key(&self, key: &K) -> bool {
        self.contains_key(key)
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn total_charge(&self) -> usize {
        self.total_charge()
    }

    fn total_capacity(&self) -> usize {
        self.total_capacity()
    }

    fn shard_count(&self) -> usize {
        self.shard_count()
    }

    #[cfg(test)]
    fn shard_usage(&self, idx: usize) -> (usize, usize) {
        self.shard_usage(idx)
    }
}

impl<K, V> CacheInner<K, V>
where
    K: Ord + Clone + Debug,
    V: CacheEntry + Clone,
{
    /// Creates an empty cache with `max_cost` capacity.
    fn new(max_cost: usize) -> Self {
        Self {
            cache: BTreeMap::new(),
            lru: BTreeMap::new(),
            next_serial: 0,
            cur_cost: 0,
            max_cost,
        }
    }

    /// Checks the cache/LRU bookkeeping invariants.
    #[cfg(any(test, debug_assertions))]
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

    /// Runs invariant checks in debug builds.
    fn debug_check_invariants(&self) {
        #[cfg(debug_assertions)]
        self.check_invariants()
    }

    /// Looks up `key`, refreshes its recency, and returns the cached value.
    fn get(&mut self, key: K) -> Option<V> {
        if let Some(value) = self.cache.get_mut(&key) {
            self.lru.remove(&value.serial);
            value.serial = self.next_serial;
            self.lru.insert(value.serial, key);
            self.next_serial += 1;
            Some(value.aux.clone())
        } else {
            None
        }
    }

    /// Evicts least-recently-used entries until `cur_cost <= max_cost`.
    fn evict_to(&mut self, max_cost: usize) {
        while self.cur_cost > max_cost {
            // lru and cache are kept in sync by all mutating methods;
            // since cur_cost > max_cost >= 0, at least one entry exists.
            let (_serial, key) = self.lru.pop_first().unwrap();
            let value = self.cache.remove(&key).unwrap();
            self.cur_cost -= value.aux.cost();
        }
        self.debug_check_invariants();
    }

    /// Inserts or replaces `key` with `aux`.
    fn insert(&mut self, key: K, aux: V) {
        let cost = aux.cost();
        self.evict_to(self.max_cost.saturating_sub(cost));
        if let Some(old_value) = self.cache.insert(
            key.clone(),
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

    /// Removes `key` if it is present and returns the removed value.
    fn remove(&mut self, key: &K) -> Option<V> {
        let value = self.cache.remove(key)?;
        self.lru.remove(&value.serial).unwrap();
        self.cur_cost -= value.aux.cost();
        self.debug_check_invariants();
        Some(value.aux)
    }

    /// Removes every entry whose key matches `predicate`.
    fn remove_if<F>(&mut self, predicate: F)
    where
        F: Fn(&K) -> bool,
    {
        let keys: Vec<K> = self
            .cache
            .keys()
            .filter(|key| predicate(key))
            .cloned()
            .collect();
        for key in keys {
            let _ = self.remove(&key);
        }
    }

    /// Removes every entry whose key falls within `range`.
    fn remove_range<R>(&mut self, range: R) -> usize
    where
        R: RangeBounds<K>,
    {
        let victims: Vec<(K, u64)> = self
            .cache
            .range(range)
            .map(|(key, value)| (key.clone(), value.serial))
            .collect();

        let removed = victims.len();
        for (key, serial) in victims {
            self.lru.remove(&serial).unwrap();
            self.cur_cost -= self.cache.remove(&key).unwrap().aux.cost();
        }
        self.debug_check_invariants();
        removed
    }

    /// Returns `true` if `key` is resident.
    fn contains_key(&self, key: &K) -> bool {
        self.cache.contains_key(key)
    }

    /// Returns the number of resident entries.
    fn len(&self) -> usize {
        self.cache.len()
    }
}
