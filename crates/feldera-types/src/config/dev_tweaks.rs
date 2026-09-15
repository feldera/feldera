use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// The largest value either merge threshold accepts.
///
/// A spine blocks its producer at 128 loose batches, and a level below its
/// threshold never merges.  With threshold `n`, `14 + 7 * (n - 1)` batches
/// can wait with no merge due, which reaches 128 at `n = 18`.  15 is a
/// conservative choice; `merge_threshold_test` pins it.
pub const MAX_MIN_MERGE_BATCHES: u16 = 15;

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
    /// allow to be in use before failing all writes with `StorageFull`.  This
    /// is useful for testing on top of storage that does not implement its own
    /// quota mechanism.
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

    /// How many keys a lazy input map resolves against its integral before it
    /// yields to the rest of the circuit.
    ///
    /// The map yields once it has produced a chunk of adjustments, which bounds
    /// a step by its output.  A transaction that rewrites keys with the values
    /// they already hold produces almost no adjustments, so this bounds the same
    /// step by its input.
    ///
    /// The default is 100,000.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lazy_input_map_keys_per_step: Option<u64>,

    /// Minimum size of the key blocks in the batches a lazy input map's
    /// accumulator writes for itself, in bytes.  A power of two, at least
    /// 4096.
    ///
    /// The map resolves a transaction by walking the accumulated updates in
    /// key order without reading values, one storage request per key block,
    /// so larger blocks mean fewer requests.  Only the accumulator's own
    /// batches (merge outputs and spills) take this size; the batches that
    /// reach the integral keep it until its merger rewrites them at the
    /// default size.  The default is 32768; 8192 is the file writer's default.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lazy_input_map_key_block_bytes: Option<u64>,

    /// Pause a spine's background merging while a lazy input map resolves a
    /// transaction.
    ///
    /// Resolving walks the integral, which reads from the same disk the
    /// mergers do.  Holding the mergers off for the walk separates what the
    /// walk costs on its own from what it costs behind a merger, at the price
    /// of leaving the spines unmerged until the transaction commits.
    ///
    /// This is a diagnostic, off by default.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lazy_input_map_pause_merging: Option<bool>,

    /// How many data blocks a layer-file cursor that declared a sequential
    /// walk reads ahead of its position.
    ///
    /// Each block a cursor steps into that the buffer cache does not hold is a
    /// device round trip the worker waits out, and a key column is a strided
    /// subset of its file that the kernel's readahead never serves.  Reading
    /// ahead this many blocks keeps that many round trips in flight.  Zero
    /// disables it.  The default is 8.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub layer_file_read_ahead_blocks: Option<u64>,

    /// Enable adaptive joins.
    ///
    /// Adaptive joins dynamically change their partitioning policy to avoid skew.
    ///
    /// Adaptive joins are disabled by default.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub adaptive_joins: Option<bool>,

    /// Evict eagerly from buffer caches as files get deleted.
    ///
    /// This is an optimization that drops files from
    /// the cache as soon as they are deleted.
    ///
    /// It has unknown (no?) performance benefits from what I can tell.
    ///
    /// Historically it made sense to do this for two reasons:
    /// a) we know with 100% guarantee that the file won't ever be
    ///    read again.
    /// b) we could do this in O(logn) time with the LRU cache.
    ///    This is no longer true for s3-fifo where it is O(n).
    ///
    /// If the eviction is expensive, (many small objects in the cache)
    /// this can cause a regression.
    ///
    /// New default disables this behavior by making it false.
    ///
    /// If this doesn't cause regression we will remove this option
    /// in the future.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub eager_evict: Option<bool>,

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
    /// improvement across the collections whose partitioning policy the
    /// rebalancing changes is at least this threshold. Collections that keep
    /// their policy are excluded, since they cost the same either way.
    ///
    /// A rebalancing is applied if both this threshold and
    /// `balancer_min_absolute_improvement_threshold` are met.
    ///
    /// The default value is 1.2.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default, deserialize_with = "crate::serde_via_value::deserialize")]
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
    /// improvement across the collections whose partitioning policy the
    /// rebalancing changes is at least this threshold. The cost model used by the
    /// balancer is based on the number of records in the largest partition of a
    /// collection.
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
    #[serde(default, deserialize_with = "crate::serde_via_value::deserialize")]
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
    #[serde(default, deserialize_with = "crate::serde_via_value::deserialize")]
    pub balancer_key_distribution_refresh_threshold: Option<f64>,

    /// False-positive rate for Bloom filters on batches on storage.
    ///
    /// Deprecated: use `storage.bloom_false_positive_rate` instead.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default, deserialize_with = "crate::serde_via_value::deserialize")]
    pub bloom_false_positive_rate: Option<f64>,

    /// Whether file-backed batches may use roaring membership filters when the
    /// key type supports them.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enable_roaring: Option<bool>,

    /// Maximum batch size in records for level 0 merges.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_level0_batch_size_records: Option<u16>,

    /// Minimum number of batches an accumulator's spine merges at once,
    /// at every level above level 1.
    ///
    /// Zero restores the built-in minimum.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_accumulator_merge_batches: Option<u16>,

    /// Minimum number of batches an integral's spine merges at once, at
    /// every level above level 1.
    ///
    /// Unset or zero keeps the built-in minimum.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_integral_merge_batches: Option<u16>,

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

    /// Don't automatically start a transaction for every step.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disable_auto_transaction: Option<bool>,

    /// Override the timestamp returned by SQL `NOW()` at pipeline start.
    ///
    /// When set, the clock connector anchors `NOW()` to this RFC 3339
    /// timestamp the first time the pipeline starts and advances at
    /// wall-clock cadence from there:
    /// `NOW() = now_offset + (wall_clock - wall_clock_at_start)`.
    ///
    /// Any RFC 3339 timestamp parseable by `chrono::DateTime<Utc>` is
    /// accepted (years `0001` through `9999`), in the past or future
    /// relative to wall clock.
    ///
    /// This is a testing knob for queries that depend on `NOW()`.
    ///
    /// On resume the clock continues from the last journaled `NOW()`;
    /// `now_offset`'s value is honored only on a fresh start:
    ///
    /// | Initial run | Resume from checkpoint | Post-replay `NOW()` |
    /// |---|---|---|
    /// | no offset | no offset | wall clock (unchanged) |
    /// | offset    | offset    | wall-clock pace from the last journaled value; the new offset value is ignored |
    /// | offset    | no offset | jumps to wall clock (explicit opt-out of the anchor) |
    /// | no offset | offset    | wall-clock pace from the last journaled value; the new offset value is ignored |
    #[serde(skip_serializing_if = "Option::is_none")]
    pub now_offset: Option<DateTime<Utc>>,

    /// Drive `NOW()` from an external HTTP endpoint instead of wall clock.
    ///
    /// When `true`, the clock connector emits one initial tick (using
    /// `now_offset` if set, otherwise wall clock) and then holds that
    /// value.  Subsequent calls to `POST /clock/advance` move `NOW()`
    /// forward by the requested delta.  Negative deltas are rejected;
    /// the clock is forward-only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub now_http_driven: Option<bool>,

    /// Enable streaming exchange.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub streaming_exchange: Option<bool>,

    /// Maximum number of bytes of queued but unacknowledged exchange messages
    /// per pair of remote host and message type.
    ///
    /// A sender that pushes past this budget waits for the receiver to
    /// acknowledge earlier messages before it queues more.  There are three
    /// message types, so a host buffers up to three times this many bytes for
    /// each of the other hosts, plus any single message that exceeds the
    /// budget on its own.
    ///
    /// The default is 10,000,000 bytes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exchange_channel_capacity_bytes: Option<usize>,

    /// Optimize input operators during transaction commit.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub optimize_input_during_commit: Option<bool>,

    /// Options not understood by this particular version.
    ///
    /// This allows the pipeline manager to take options that a custom or old
    /// runtime version accepts.
    #[serde(flatten)]
    pub other_options: BTreeMap<String, serde_json::Value>,
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
    pub fn lazy_input_map_keys_per_step(&self) -> u64 {
        self.lazy_input_map_keys_per_step.unwrap_or(100_000)
    }
    pub fn lazy_input_map_key_block_bytes(&self) -> u64 {
        self.lazy_input_map_key_block_bytes.unwrap_or(32 * 1024)
    }
    pub fn layer_file_read_ahead_blocks(&self) -> u64 {
        self.layer_file_read_ahead_blocks.unwrap_or(8)
    }
    pub fn lazy_input_map_pause_merging(&self) -> bool {
        self.lazy_input_map_pause_merging.unwrap_or(false)
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
        // Roaring is enabled by default, but `enable_roaring = false` remains
        // available as a kill switch while the feature is still being tuned.
        self.enable_roaring.unwrap_or(true)
    }

    /// Rejects settings outside their valid range, naming the field, the
    /// value and the range.
    pub fn validate(&self) -> Result<(), String> {
        for (name, value) in [
            (
                "min_accumulator_merge_batches",
                self.min_accumulator_merge_batches,
            ),
            (
                "min_integral_merge_batches",
                self.min_integral_merge_batches,
            ),
        ] {
            if let Some(value) = value
                && value > MAX_MIN_MERGE_BATCHES
            {
                return Err(format!(
                    "dev_tweaks.{name} is {value}, but the valid range is 0 through \
                     {MAX_MIN_MERGE_BATCHES}; 0 keeps the built-in minimum"
                ));
            }
        }
        Ok(())
    }

    /// Batches an accumulator's spine waits for above level 1; zero means the
    /// built-in minimum.
    pub fn min_accumulator_merge_batches(&self) -> u16 {
        self.min_accumulator_merge_batches.unwrap_or(10)
    }

    /// Batches an integral's spine waits for above level 1; zero means the
    /// built-in minimum.
    pub fn min_integral_merge_batches(&self) -> u16 {
        self.min_integral_merge_batches.unwrap_or(0)
    }

    pub fn negative_weight_multiplier(&self) -> u16 {
        self.negative_weight_multiplier.unwrap_or(0)
    }

    pub fn disable_auto_transaction(&self) -> bool {
        self.disable_auto_transaction.unwrap_or(false)
    }

    /// Configured `now_offset` as milliseconds since the Unix epoch,
    /// or `None` if no override is set.
    pub fn now_offset_ms(&self) -> Option<i64> {
        self.now_offset.map(|target| target.timestamp_millis())
    }

    pub fn now_http_driven(&self) -> bool {
        self.now_http_driven.unwrap_or(false)
    }

    pub fn streaming_exchange(&self) -> bool {
        self.streaming_exchange.unwrap_or(true)
    }

    pub fn exchange_channel_capacity_bytes(&self) -> usize {
        self.exchange_channel_capacity_bytes.unwrap_or(10_000_000)
    }

    pub fn optimize_input_during_commit(&self) -> bool {
        self.optimize_input_during_commit.unwrap_or(true)
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::config::{PipelineConfig, RuntimeConfig};

    use super::*;

    /// Either merge threshold is accepted up to the bound and rejected past
    /// it, by name, with the value and the range in the message.
    #[test]
    fn merge_thresholds_are_bounded() {
        assert_eq!(DevTweaks::default().validate(), Ok(()));
        for field in [
            "min_accumulator_merge_batches",
            "min_integral_merge_batches",
        ] {
            let with = |value: u16| {
                let mut tweaks = DevTweaks::default();
                if field == "min_accumulator_merge_batches" {
                    tweaks.min_accumulator_merge_batches = Some(value);
                } else {
                    tweaks.min_integral_merge_batches = Some(value);
                }
                tweaks
            };
            assert_eq!(with(0).validate(), Ok(()), "{field} = 0");
            assert_eq!(
                with(MAX_MIN_MERGE_BATCHES).validate(),
                Ok(()),
                "{field} at the bound"
            );
            let error = with(MAX_MIN_MERGE_BATCHES + 1)
                .validate()
                .expect_err("a value past the bound must be rejected");
            assert!(error.contains(field), "{error}");
            assert!(error.contains("16"), "{error}");
            assert!(error.contains("0 through 15"), "{error}");
        }
    }

    /// Regression test: `Option<f64>` fields inside `DevTweaks` must
    /// survive a JSON-string round-trip through `PipelineConfig`, which
    /// uses `#[serde(flatten)]` on `RuntimeConfig`. With `serde_json`'s
    /// `arbitrary_precision` feature enabled, the serde `Content` buffer
    /// represents numbers as maps, which breaks plain `f64`
    /// deserialization (serde-rs/json#1157). The `serde_via_value`
    /// workaround on each `Option<f64>` field fixes this.
    #[test]
    fn dev_tweaks_f64_roundtrip_through_pipeline_config() {
        let rc = RuntimeConfig {
            dev_tweaks: DevTweaks {
                bloom_false_positive_rate: Some(0.0),
                balancer_balance_tax: Some(1.1),
                balancer_min_relative_improvement_threshold: Some(1.2),
                balancer_key_distribution_refresh_threshold: Some(0.1),
                ..Default::default()
            },
            ..Default::default()
        };
        let pc = PipelineConfig {
            global: rc,
            multihost: None,
            name: Some("test-pipeline".into()),
            given_name: None,
            storage_config: None,
            secrets_dir: None,
            inputs: Default::default(),
            outputs: Default::default(),
            program_ir: None,
        };

        // JSON string round-trip (the path the pipeline process takes).
        let json = serde_json::to_string_pretty(&pc).unwrap();
        let pc2: PipelineConfig = serde_json::from_str(&json)
            .expect("JSON string round-trip of PipelineConfig with f64 dev_tweaks must succeed");
        assert_eq!(pc2.global.dev_tweaks.bloom_false_positive_rate, Some(0.0));
        assert_eq!(pc2.global.dev_tweaks.balancer_balance_tax, Some(1.1));
        assert_eq!(
            pc2.global
                .dev_tweaks
                .balancer_min_relative_improvement_threshold,
            Some(1.2)
        );
        assert_eq!(
            pc2.global
                .dev_tweaks
                .balancer_key_distribution_refresh_threshold,
            Some(0.1)
        );

        // serde_json::Value round-trip (the path the pipeline manager takes).
        let value = serde_json::to_value(&pc).unwrap();
        let pc3: PipelineConfig = serde_json::from_value(value)
            .expect("Value round-trip of PipelineConfig with f64 dev_tweaks must succeed");
        assert_eq!(pc3.global.dev_tweaks.bloom_false_positive_rate, Some(0.0));
    }

    #[test]
    fn other_options() {
        let dt =
            serde_json::from_value::<DevTweaks>(json!({"xyzzy": 1.0, "foobar": {"key": "value"}}))
                .unwrap();
        assert_eq!(
            &dt.other_options,
            &BTreeMap::from_iter([
                (String::from("xyzzy"), json!(1.0)),
                (String::from("foobar"), json!({"key": "value"}))
            ]),
        );
    }
}
