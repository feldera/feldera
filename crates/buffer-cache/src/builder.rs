use crate::ThreadType;
use crate::{CacheEntry, LruCache, S3FifoCache, SharedBufferCache};
use enum_map::{Enum, EnumMap};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::hash::{BuildHasher, Hash, RandomState};
use std::marker::PhantomData;
use tracing::warn;

/// Selects which eviction strategy backs a cache instance.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BufferCacheStrategy {
    /// Use the sharded S3-FIFO cache backed by `quick_cache`.
    #[default]
    S3Fifo,

    /// Use the mutex-protected weighted LRU cache.
    Lru,
}

/// Controls how caches are shared across a foreground/background worker pair.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BufferCacheAllocationStrategy {
    /// Share one cache across a foreground/background worker pair.
    #[default]
    SharedPerWorkerPair,

    /// Create a separate cache for each foreground/background thread.
    PerThread,

    /// Share one cache across all foreground/background threads.
    Global,
}

/// Builds the cache layout used by DBSP runtime worker pairs.
pub struct BufferCacheBuilder<K, V, S = RandomState> {
    /// Eviction strategy used for newly constructed caches.
    strategy: BufferCacheStrategy,
    /// Optional override for the sharded backend shard count.
    max_buckets: Option<usize>,
    /// Sharing policy across worker-pair cache slots.
    allocation_strategy: BufferCacheAllocationStrategy,
    /// Hash builder shared by newly constructed caches.
    hash_builder: S,
    /// Keeps the builder generic over the cache key and value types.
    marker: PhantomData<fn(K) -> V>,
}

impl<K, V> BufferCacheBuilder<K, V, RandomState> {
    /// Creates a builder that uses the default hash builder.
    pub fn new() -> Self {
        Self {
            strategy: BufferCacheStrategy::default(),
            max_buckets: None,
            allocation_strategy: BufferCacheAllocationStrategy::default(),
            hash_builder: RandomState::new(),
            marker: PhantomData,
        }
    }
}

impl<K, V> Default for BufferCacheBuilder<K, V, RandomState> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V, S> BufferCacheBuilder<K, V, S> {
    /// Sets the eviction strategy for caches created by this builder.
    pub fn with_buffer_cache_strategy(mut self, strategy: BufferCacheStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    /// Sets the optional shard-count override for sharded backends.
    pub fn with_buffer_max_buckets(mut self, max_buckets: Option<usize>) -> Self {
        self.max_buckets = max_buckets;
        self
    }

    /// Sets how caches are shared across each worker pair.
    pub fn with_buffer_cache_allocation_strategy(
        mut self,
        allocation_strategy: BufferCacheAllocationStrategy,
    ) -> Self {
        self.allocation_strategy = allocation_strategy;
        self
    }

    /// Sets the hash builder used for newly constructed caches.
    pub fn with_hash_builder<NewS>(self, hash_builder: NewS) -> BufferCacheBuilder<K, V, NewS> {
        BufferCacheBuilder {
            strategy: self.strategy,
            max_buckets: self.max_buckets,
            allocation_strategy: self.allocation_strategy,
            hash_builder,
            marker: PhantomData,
        }
    }
}

impl<K, V, S> BufferCacheBuilder<K, V, S>
where
    K: Eq + Hash + Ord + Clone + Debug + Send + Sync + 'static,
    V: CacheEntry + Clone + Send + Sync + 'static,
    S: BuildHasher + Clone + Send + Sync + 'static,
{
    /// Builds one cache slot per [`ThreadType`] for each worker pair.
    pub fn build(
        &self,
        worker_pairs: usize,
        total_capacity_bytes: usize,
    ) -> Vec<EnumMap<ThreadType, SharedBufferCache<K, V>>> {
        if worker_pairs == 0 {
            return Vec::new();
        }

        let per_thread_capacity = total_capacity_bytes / worker_pairs / ThreadType::LENGTH;
        let per_pair_capacity = total_capacity_bytes / worker_pairs;

        match self.strategy {
            BufferCacheStrategy::Lru => {
                if self.allocation_strategy == BufferCacheAllocationStrategy::Global {
                    warn!(
                        "unsupported buffer cache allocation strategy {:?} set in dev_tweaks for LRU cache, falling back to `per_thread`",
                        self.allocation_strategy
                    );
                }
                (0..worker_pairs)
                    .map(|_| self.build_thread_slots(per_thread_capacity))
                    .collect()
            }
            BufferCacheStrategy::S3Fifo => match self.allocation_strategy {
                BufferCacheAllocationStrategy::PerThread => (0..worker_pairs)
                    .map(|_| self.build_thread_slots(per_thread_capacity))
                    .collect(),
                BufferCacheAllocationStrategy::SharedPerWorkerPair => (0..worker_pairs)
                    .map(|_| Self::shared_thread_slots(self.build_s3_fifo(per_pair_capacity)))
                    .collect(),
                BufferCacheAllocationStrategy::Global => {
                    let cache = self.build_s3_fifo(total_capacity_bytes);
                    (0..worker_pairs)
                        .map(|_| Self::shared_thread_slots(cache.clone()))
                        .collect()
                }
            },
        }
    }

    /// Builds one cache instance using the currently selected strategy.
    pub fn build_single(&self, capacity_bytes: usize) -> SharedBufferCache<K, V> {
        match self.strategy {
            BufferCacheStrategy::Lru => self.build_lru(capacity_bytes),
            BufferCacheStrategy::S3Fifo => self.build_s3_fifo(capacity_bytes),
        }
    }

    /// Constructs a weighted LRU cache.
    fn build_lru(&self, capacity_bytes: usize) -> SharedBufferCache<K, V> {
        let cache: SharedBufferCache<K, V> = std::sync::Arc::new(LruCache::with_hasher(
            capacity_bytes,
            self.hash_builder.clone(),
        ));
        cache
    }

    /// Constructs a sharded S3-FIFO cache.
    fn build_s3_fifo(&self, capacity_bytes: usize) -> SharedBufferCache<K, V> {
        let shards = normalize_sharded_cache_shards(self.max_buckets);
        let cache: SharedBufferCache<K, V> = std::sync::Arc::new(S3FifoCache::with_hasher(
            capacity_bytes,
            shards,
            self.hash_builder.clone(),
        ));
        cache
    }

    /// Builds a separate cache slot for each thread type.
    fn build_thread_slots(
        &self,
        capacity_bytes: usize,
    ) -> EnumMap<ThreadType, SharedBufferCache<K, V>> {
        EnumMap::from_fn(|_| self.build_single(capacity_bytes))
    }

    /// Reuses a single cache backend for every thread-type slot in a worker pair.
    fn shared_thread_slots(
        cache: SharedBufferCache<K, V>,
    ) -> EnumMap<ThreadType, SharedBufferCache<K, V>> {
        EnumMap::from_fn(|_| cache.clone())
    }
}

/// Normalizes an optional bucket count to the shard count expected by the
/// sharded S3-FIFO backend.
fn normalize_sharded_cache_shards(max_buckets: Option<usize>) -> usize {
    match max_buckets {
        None | Some(0) => S3FifoCache::<(), ()>::DEFAULT_SHARDS,
        Some(buckets) => buckets
            .checked_next_power_of_two()
            .unwrap_or(S3FifoCache::<(), ()>::DEFAULT_SHARDS),
    }
}
