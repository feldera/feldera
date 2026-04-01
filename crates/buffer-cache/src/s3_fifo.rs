use crate::{BufferCache, CacheEntry};
use feldera_types::config::dev_tweaks::BufferCacheStrategy;
use quick_cache::{OptionsBuilder, Weighter, sync::Cache as QuickCache};
use std::any::Any;
use std::hash::{BuildHasher, Hash, RandomState};

/// `quick_cache` requires an item-count estimate and uses it to decide whether
/// the requested shard count can be honored.
///
/// Setting the estimate to at least 32 items per shard keeps the requested
/// shard count intact without introducing a capacity-based heuristic.
const MIN_ESTIMATED_ITEMS_PER_SHARD: usize = 32;

/// Converts [`CacheEntry::cost`] into the weight expected by `quick_cache`.
#[derive(Clone, Copy, Default)]
struct CacheEntryWeighter;

impl<K, V> Weighter<K, V> for CacheEntryWeighter
where
    V: CacheEntry,
{
    fn weight(&self, _key: &K, value: &V) -> u64 {
        value.cost() as u64
    }
}

/// A sharded, weighted, thread-safe S3-FIFO cache backed directly by
/// `quick_cache`.
pub struct S3FifoCache<K, V, S = RandomState> {
    /// Shared `quick_cache` backend.
    cache: QuickCache<K, V, CacheEntryWeighter, S>,
    /// Hash builder retained only so tests can mirror `quick_cache`'s shard
    /// selection.
    #[cfg(test)]
    hash_builder: S,
}

impl<K, V, S> S3FifoCache<K, V, S> {
    /// Default power-of-two shard count used by [`S3FifoCache::new`].
    pub const DEFAULT_SHARDS: usize = 256;
}

impl<K, V> S3FifoCache<K, V, RandomState>
where
    K: Eq + Hash + Clone,
    V: CacheEntry + Clone,
{
    /// Creates a cache with [`Self::DEFAULT_SHARDS`] shards.
    pub fn new(total_capacity_bytes: usize) -> Self {
        Self::with_hasher(
            total_capacity_bytes,
            S3FifoCache::<K, V>::DEFAULT_SHARDS,
            RandomState::new(),
        )
    }

    /// Creates a cache with an explicit shard count.
    ///
    /// # Panics
    ///
    /// Panics if `num_shards == 0` or if `num_shards` is not a power of two.
    pub fn with_shards(total_capacity_bytes: usize, num_shards: usize) -> Self {
        Self::with_hasher(total_capacity_bytes, num_shards, RandomState::new())
    }
}

// explicit allow, we do have `is_empty` in the trait so this is a false positive
#[allow(clippy::len_without_is_empty)]
impl<K, V, S> S3FifoCache<K, V, S>
where
    K: Eq + Hash + Clone,
    V: CacheEntry + Clone,
    S: BuildHasher + Clone,
{
    /// Creates a cache with an explicit shard count and hash builder.
    ///
    /// Because `quick_cache` uses equal-capacity shards internally, the actual
    /// backend capacity may round up when `total_capacity_bytes` is not evenly
    /// divisible by `num_shards`.
    ///
    /// # Panics
    ///
    /// Panics if `num_shards == 0` or if `num_shards` is not a power of two.
    pub fn with_hasher(total_capacity_bytes: usize, num_shards: usize, hash_builder: S) -> Self {
        assert!(num_shards > 0, "num_shards must be > 0");
        assert!(
            num_shards.is_power_of_two(),
            "num_shards must be a power of two"
        );

        let options = OptionsBuilder::new()
            .shards(num_shards)
            .estimated_items_capacity(minimum_estimated_items(num_shards))
            .weight_capacity(total_capacity_bytes as u64)
            .build()
            .expect("valid quick_cache options");

        Self {
            #[cfg(test)]
            hash_builder: hash_builder.clone(),
            cache: QuickCache::with_options(
                options,
                CacheEntryWeighter,
                hash_builder,
                Default::default(),
            ),
        }
    }

    /// Inserts or replaces `key` with `value`.
    pub fn insert(&self, key: K, value: V) {
        self.cache.insert(key, value);
    }

    /// Looks up `key` and returns a clone of the stored value.
    pub fn get(&self, key: &K) -> Option<V> {
        self.cache.get(key)
    }

    /// Removes `key` if present and returns the removed value.
    pub fn remove(&self, key: &K) -> Option<V> {
        self.cache.remove(key).map(|(_, value)| value)
    }

    /// Removes all entries matching `predicate` and returns the number removed.
    pub fn remove_if<F>(&self, predicate: F)
    where
        F: Fn(&K) -> bool,
    {
        self.cache.retain(|key, _value| !predicate(key))
    }

    /// Returns `true` if `key` is currently present.
    pub fn contains_key(&self, key: &K) -> bool {
        self.cache.contains_key(key)
    }

    /// Returns the current number of live entries.
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    /// Returns the current total weighted charge.
    pub fn total_charge(&self) -> usize {
        self.cache.weight() as usize
    }

    /// Returns the backend's configured total weighted capacity.
    ///
    /// This may be larger than the requested constructor argument when the
    /// requested total capacity is not evenly divisible across shards.
    pub fn total_capacity(&self) -> usize {
        self.cache.capacity() as usize
    }

    /// Returns the number of backend shards.
    pub fn shard_count(&self) -> usize {
        self.cache.num_shards()
    }

    /// Returns `(used_charge, capacity)` for backend shard `idx`.
    ///
    /// # Panics
    ///
    /// Panics if `idx >= self.shard_count()`.
    #[cfg(test)]
    pub fn shard_usage(&self, idx: usize) -> (usize, usize) {
        assert!(idx < self.shard_count(), "shard index out of bounds");
        let used = self
            .cache
            .iter()
            .filter(|(key, _value)| self.shard_index(key) == idx)
            .map(|(_key, value)| value.cost())
            .sum();
        (used, self.cache.shard_capacity() as usize)
    }

    /// Validates the wrapper invariants we rely on in Feldera tests:
    /// iteration agrees with the public accounting APIs and each resident key
    /// maps to the backend shard reported by `shard_usage()`.
    #[cfg(test)]
    pub(crate) fn validate_invariants(&self) {
        let shard_count = self.shard_count();
        let mut shard_usage = vec![0usize; shard_count];
        let mut total_len = 0usize;
        let mut total_charge = 0usize;

        for (key, value) in self.cache.iter() {
            let shard_idx = self.shard_index(&key);
            assert!(shard_idx < shard_count, "invalid backend shard index");
            let weight = value.cost();
            shard_usage[shard_idx] += weight;
            total_len += 1;
            total_charge += weight;
        }

        for (idx, used) in shard_usage.into_iter().enumerate() {
            let (reported_used, reported_capacity) = self.shard_usage(idx);
            assert_eq!(reported_used, used, "per-shard charge mismatch");
            assert!(
                used <= reported_capacity,
                "used {} exceeds capacity {}",
                used,
                reported_capacity
            );
        }

        assert_eq!(total_len, self.len(), "global resident count mismatch");
        assert_eq!(total_charge, self.total_charge(), "global charge mismatch");
        assert!(
            total_charge <= self.total_capacity(),
            "total charge exceeds backend capacity"
        );
    }

    /// Mirrors `quick_cache`'s shard selection logic for test-only shard
    /// accounting and validation.
    ///
    /// The hash function is not exposed in the public API of `quick_cache`.
    #[cfg(test)]
    pub(crate) fn shard_index(&self, key: &K) -> usize {
        let shard_mask = (self.shard_count() - 1) as u64;
        (self
            .hash_builder
            .hash_one(key)
            .rotate_right(usize::BITS / 2)
            & shard_mask) as usize
    }
}

/// Returns the minimum item-count estimate needed to preserve a requested
/// shard count in `quick_cache`.
fn minimum_estimated_items(num_shards: usize) -> usize {
    num_shards.saturating_mul(MIN_ESTIMATED_ITEMS_PER_SHARD)
}

impl<K, V, S> BufferCache<K, V> for S3FifoCache<K, V, S>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: CacheEntry + Clone + Send + Sync + 'static,
    S: BuildHasher + Clone + Send + Sync + 'static,
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn strategy(&self) -> BufferCacheStrategy {
        BufferCacheStrategy::S3Fifo
    }

    fn insert(&self, key: K, value: V) {
        self.insert(key, value);
    }

    fn get(&self, key: K) -> Option<V> {
        self.get(&key)
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
