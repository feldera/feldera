use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Optional settings for tweaking Feldera internals.
///
/// These settings reflect experiments that may come and go and change from
/// version to version.  Users should not consider them to be stable.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(default)]
pub struct DevTweaks {
    /// Buffer-cache implementation to use for storage reads.
    ///
    /// The default is `s3_fifo`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub buffer_cache_strategy: Option<BufferCacheStrategy>,

    /// Override the number of buckets/shards used by sharded buffer caches.
    ///
    /// This only applies when `buffer_cache_strategy = "s3_fifo"`. Values are
    /// rounded up to the next power of two because the current implementation
    /// shards by `hash(key) & (n - 1)`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub buffer_max_buckets: Option<usize>,

    /// How S3-FIFO caches are assigned to foreground/background workers.
    ///
    /// This only applies when `buffer_cache_strategy = "s3_fifo"`. The
    /// default is `shared_per_worker_pair`; LRU always uses `per_thread`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub buffer_cache_allocation_strategy: Option<BufferCacheAllocationStrategy>,

    /// Target number of cached bytes retained in each `FBuf` slab size class.
    ///
    /// The default is 16 MiB.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fbuf_slab_bytes_per_class: Option<usize>,

    /// Whether to asynchronously fetch keys needed for the join operator from
    /// storage.  Asynchronous fetching should be faster for high-latency
    /// storage, such as object storage, but it could use excessive amounts of
    /// memory if the number of keys fetched is very large.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fetch_join: Option<bool>,

    /// Whether to asynchronously fetch keys needed for the distinct operator
    /// from storage.  Asynchronous fetching should be faster for high-latency
    /// storage, such as object storage, but it could use excessive amounts of
    /// memory if the number of keys fetched is very large.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fetch_distinct: Option<bool>,

    /// Which merger to use.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub merger: Option<MergerType>,

    /// If set, the maximum amount of storage, in MiB, for the POSIX backend to
    /// allow to be in use before failing all writes with [StorageFull].  This
    /// is useful for testing on top of storage that does not implement its own
    /// quota mechanism.
    ///
    /// [StorageFull]: std::io::ErrorKind::StorageFull
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_mb_max: Option<u64>,

    /// Attempt to print a stack trace on stack overflow.
    ///
    /// To be used for debugging only; do not enable in production.
    // NOTE: this flag is handled manually in `adapters/src/server.rs` before
    // parsing DevTweaks. If the name or type of this field changes, make sure to
    // adjust `server.rs` accordingly.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stack_overflow_backtrace: Option<bool>,

    /// Controls the maximal number of records output by splitter operators
    /// (joins, distinct, aggregation, rolling window and group operators) at
    /// each step.
    ///
    /// The default value is 10,000 records.
    // TODO: It would be better if the value were denominated in bytes rather
    // than records, and if it were configurable per-operator.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub splitter_chunk_size_records: Option<u64>,

    /// Enable adaptive joins.
    ///
    /// Adaptive joins dynamically change their partitioning policy to avoid skew.
    ///
    /// Adaptive joins are disabled by default.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub adaptive_joins: Option<bool>,

    /// The minimum relative improvement threshold for the join balancer.
    ///
    /// The join balancer is a component that dynamically chooses an optimal
    /// partitioning policy for adaptive join operators.  This parameter
    /// prevents the join balancer from making changes to the partitioning
    /// policy if the improvement is not significant, since the overhead of such
    /// rebalancing, especially when performed frequently, can exceed the
    /// benefits.
    ///
    /// A rebalancing is considered significant if the relative estimated
    /// improvement for the cluster of joins where the rebalancing is applied is
    /// at least this threshold.
    ///
    /// A rebalancing is applied if both this threshold and
    /// `balancer_min_absolute_improvement_threshold` are met.
    ///
    /// The default value is 1.2.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub balancer_min_relative_improvement_threshold: Option<f64>,

    /// The minimum absolute improvement threshold for the balancer.
    ///
    /// The join balancer is a component that dynamically chooses an optimal
    /// partitioning policy for adaptive join operators.  This parameter
    /// prevents the join balancer from making changes to the partitioning
    /// policy if the improvement is not significant, since the overhead of such
    /// rebalancing, especially when performed frequently, can exceed the
    /// benefits.
    ///
    /// A rebalancing is considered significant if the absolute estimated
    /// improvement for the cluster of joins where the rebalancing is applied is
    /// at least this threshold. The cost model used by the balancer is based on
    /// the number of records in the largest partition of a collection.
    ///
    /// A rebalancing is applied if both this threshold and
    /// `balancer_min_relative_improvement_threshold` are met.
    ///
    /// The default value is 10,000.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub balancer_min_absolute_improvement_threshold: Option<u64>,

    /// Factor that discourages the use of the Balance policy in a perfectly balanced collection.
    ///
    /// Assuming a perfectly balanced key distribution, the Balance policy is slightly less efficient than Shard,
    /// since it requires computing the hash of the entire key/value pair. This factor discourages the use of this policy
    /// if the skew is `<balancer_balance_tax`.
    ///
    /// The default value is 1.1.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub balancer_balance_tax: Option<f64>,

    /// The balancer threshold for checking for an improved partitioning policy for a stream.
    ///
    /// Finding a good partitioning policy for a circuit involves solving an optimization problem,
    /// which can be relatively expensive. Instead of doing this on every step, the balancer only
    /// checks for an improved partitioning policy if the key distribution of a stream has changed
    /// significantly since the current solution was computed.  Specifically, it only kicks in when
    /// the size of at least one shard of at least one stream in the cluster has changed by more than
    /// this threshold.
    ///
    /// The default value is 0.1.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub balancer_key_distribution_refresh_threshold: Option<f64>,

    /// False-positive rate for Bloom filters on batches on storage, as a
    /// fraction f, where 0 < f < 1.
    ///
    /// The false-positive rate trades off between the amount of memory used by
    /// Bloom filters and how frequently storage needs to be searched for keys
    /// that are not actually present.  Typical false-positive rates and their
    /// corresponding memory costs are:
    ///
    /// - 0.1: 4.8 bits per key
    /// - 0.01: 9.6 bits per key
    /// - 0.001: 14.4 bits per key
    /// - 0.0001: 19.2 bits per key (default)
    ///
    /// Values outside the valid range, such as 0.0, disable Bloom filters.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bloom_false_positive_rate: Option<f64>,

    /// Whether file-backed batches may use roaring membership filters when the
    /// key type supports them.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enable_roaring: Option<bool>,

    /// Maximum batch size in records for level 0 merges.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_level0_batch_size_records: Option<u16>,

    /// The number of merger threads.
    ///
    /// The default value is equal to the number of worker threads.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub merger_threads: Option<u16>,

    /// Additional bias the merger assigns to records with negative weights
    /// (retractions) to promote them to higher levels of the LSM tree sooner.
    ///
    /// Reasonable values for this parameter are in the range [0, 10].
    ///
    /// The default value is 0, which means that retractions are not given
    /// any additional bias.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub negative_weight_multiplier: Option<u16>,
}

impl DevTweaks {
    pub fn buffer_cache_strategy(&self) -> BufferCacheStrategy {
        self.buffer_cache_strategy.unwrap_or_default()
    }
    pub fn buffer_cache_allocation_strategy(&self) -> BufferCacheAllocationStrategy {
        self.buffer_cache_allocation_strategy.unwrap_or_default()
    }
    pub fn effective_buffer_cache_allocation_strategy(&self) -> BufferCacheAllocationStrategy {
        match self.buffer_cache_strategy() {
            BufferCacheStrategy::S3Fifo => self.buffer_cache_allocation_strategy(),
            BufferCacheStrategy::Lru => BufferCacheAllocationStrategy::PerThread,
        }
    }
    pub fn fetch_join(&self) -> bool {
        self.fetch_join.unwrap_or(false)
    }
    pub fn fetch_distinct(&self) -> bool {
        self.fetch_distinct.unwrap_or(false)
    }
    pub fn merger(&self) -> MergerType {
        self.merger.unwrap_or_default()
    }
    pub fn stack_overflow_backtrace(&self) -> bool {
        self.stack_overflow_backtrace.unwrap_or(false)
    }
    pub fn splitter_chunk_size_records(&self) -> u64 {
        self.splitter_chunk_size_records.unwrap_or(10_000)
    }
    pub fn adaptive_joins(&self) -> bool {
        self.adaptive_joins.unwrap_or(false)
    }
    pub fn balancer_min_relative_improvement_threshold(&self) -> f64 {
        self.balancer_min_relative_improvement_threshold
            .unwrap_or(1.2)
    }
    pub fn balancer_min_absolute_improvement_threshold(&self) -> u64 {
        self.balancer_min_absolute_improvement_threshold
            .unwrap_or(10_000)
    }
    pub fn balancer_balance_tax(&self) -> f64 {
        self.balancer_balance_tax.unwrap_or(1.1)
    }
    pub fn balancer_key_distribution_refresh_threshold(&self) -> f64 {
        self.balancer_key_distribution_refresh_threshold
            .unwrap_or(0.1)
    }
    pub fn bloom_false_positive_rate(&self) -> f64 {
        self.bloom_false_positive_rate.unwrap_or(0.0001)
    }
    pub fn enable_roaring(&self) -> bool {
        self.enable_roaring.unwrap_or(true)
    }
    pub fn negative_weight_multiplier(&self) -> u16 {
        self.negative_weight_multiplier.unwrap_or(0)
    }
}

/// Selects which eviction strategy backs a cache instance.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum BufferCacheStrategy {
    /// Use the sharded S3-FIFO cache backed by `quick_cache`.
    #[default]
    S3Fifo,

    /// Use the mutex-protected weighted LRU cache.
    Lru,
}

/// Controls how caches are shared across a foreground/background worker pair.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
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

/// Which merger to use.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum MergerType {
    /// Newer merger, which should be faster for high-latency storage, such as
    /// object storage, but it likely needs tuning.
    PushMerger,

    /// The old standby, with known performance.
    #[default]
    ListMerger,
}
