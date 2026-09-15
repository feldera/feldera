use num_format::{Locale, ToFormattedString};
use serde::{
    Serialize, Serializer,
    ser::{SerializeSeq, SerializeStruct},
};
use size_of::{HumanBytes, TotalSize};
use std::{
    borrow::Cow,
    collections::BTreeMap,
    fmt::{self, Display, Write},
    ops::{Deref, DerefMut},
    panic::Location,
    time::Duration,
};

use crate::storage::buffer_cache::CacheCounts;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, PartialOrd, Ord)]
#[repr(transparent)]
pub struct MetricId(pub Cow<'static, str>);

impl Display for MetricId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum CircuitMetricCategory {
    State,
    Inputs,
    Outputs,
    Cache,
    Time,
    Balancer,
    Multihost,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CircuitMetric {
    pub name: MetricId,
    pub category: CircuitMetricCategory,
    pub advanced: bool,
    pub description: &'static str,
}

pub type MetricLabels = Vec<(Cow<'static, str>, Cow<'static, str>)>;

#[derive(Debug, Clone)]
pub struct MetricReading {
    metric_id: MetricId,
    labels: MetricLabels,
    value: MetaItem,
}

impl MetricReading {
    pub fn new(metric_id: MetricId, labels: MetricLabels, value: MetaItem) -> Self {
        Self {
            metric_id,
            labels,
            value,
        }
    }
}

pub const USED_MEMORY_BYTES: MetricId = MetricId(Cow::Borrowed("used_memory_bytes"));
pub const ALLOCATED_MEMORY_BYTES: MetricId = MetricId(Cow::Borrowed("allocated_memory_bytes"));
pub const MEMORY_ALLOCATIONS_COUNT: MetricId = MetricId(Cow::Borrowed("memory_allocations_count"));
pub const SHARED_MEMORY_BYTES: MetricId = MetricId(Cow::Borrowed("shared_memory_bytes"));
pub const STATE_RECORDS_COUNT: MetricId = MetricId(Cow::Borrowed("state_records_count"));
pub const INPUT_RECORDS_COUNT: MetricId = MetricId(Cow::Borrowed("input_records_count"));
pub const INPUT_BATCHES_STATS: MetricId = MetricId(Cow::Borrowed("input_batches_stats"));
pub const OUTPUT_BATCHES_STATS: MetricId = MetricId(Cow::Borrowed("output_batches_stats"));
pub const OUTPUT_ADJUSTMENT_STATS: MetricId = MetricId(Cow::Borrowed("output_adjustment_stats"));
pub const STEP_DURATION_HISTOGRAM: MetricId = MetricId(Cow::Borrowed("step_duration_histogram"));
pub const UNREAD_UPDATES_COUNT: MetricId = MetricId(Cow::Borrowed("unread_updates_count"));
pub const CONFLICTING_UPDATES_COUNT: MetricId =
    MetricId(Cow::Borrowed("conflicting_updates_count"));
pub const EXCHANGE_WAIT_TIME_SECONDS: MetricId =
    MetricId(Cow::Borrowed("exchange_wait_time_seconds"));
pub const EXCHANGE_SERIALIZATION_TIME_SECONDS: MetricId =
    MetricId(Cow::Borrowed("exchange_serialization_time_seconds"));
pub const EXCHANGE_SERIALIZED_BYTES: MetricId =
    MetricId(Cow::Borrowed("exchange_serialized_bytes"));
pub const EXCHANGE_DESERIALIZATION_TIME_SECONDS: MetricId =
    MetricId(Cow::Borrowed("exchange_deserialization_time_seconds"));
pub const EXCHANGE_DESERIALIZED_BYTES: MetricId =
    MetricId(Cow::Borrowed("exchange_deserialized_bytes"));
pub const KEY_DISTRIBUTION: MetricId = MetricId(Cow::Borrowed("key_distribution"));
pub const SIZE_DISTRIBUTION: MetricId = MetricId(Cow::Borrowed("size_distribution"));
pub const LOCAL_SHARD_RECORDS_COUNT: MetricId =
    MetricId(Cow::Borrowed("local_shard_records_count"));
pub const BALANCER_POLICY: MetricId = MetricId(Cow::Borrowed("balancer_policy"));
pub const RABALANCINGS_COUNT: MetricId = MetricId(Cow::Borrowed("rebalancings_count"));
pub const REBALANCING_IN_PROGRESS: MetricId =
    MetricId(Cow::Borrowed("rebalancing_in_progress_bool"));
pub const ACCUMULATOR_RECORDS_TO_REPARTITION_COUNT: MetricId =
    MetricId(Cow::Borrowed("accumulator_records_to_repartition_count"));
pub const INTEGRAL_RECORDS_TO_REPARTITION_COUNT: MetricId =
    MetricId(Cow::Borrowed("integral_records_to_repartition_count"));
pub const TOTAL_REBALANCING_TIME_SECONDS: MetricId =
    MetricId(Cow::Borrowed("total_rebalancing_time_seconds"));
pub const INPROGRESS_REBALANCING_TIME_SECONDS: MetricId =
    MetricId(Cow::Borrowed("inprogress_rebalancing_time_seconds"));
pub const LEFT_INPUT_BATCHES_STATS: MetricId = MetricId(Cow::Borrowed("left_input_batches_stats"));
pub const RIGHT_INPUT_BATCHES_STATS: MetricId =
    MetricId(Cow::Borrowed("right_input_batches_stats"));
pub const RETAINMENT_BOUNDS: MetricId = MetricId(Cow::Borrowed("retainment_bounds"));
pub const LEFT_INPUT_RECORDS_COUNT: MetricId = MetricId(Cow::Borrowed("left_input_records_count"));
pub const RIGHT_INPUT_INTEGRAL_RECORDS_COUNT: MetricId =
    MetricId(Cow::Borrowed("right_input_integral_records_count"));
pub const COMPUTED_OUTPUT_RECORDS_COUNT: MetricId =
    MetricId(Cow::Borrowed("computed_output_records_count"));
pub const OUTPUT_REDUNDANCY_PERCENT: MetricId =
    MetricId(Cow::Borrowed("output_redundancy_percent"));
pub const CACHE_FOREGROUND_HITS: MetricId = MetricId(Cow::Borrowed("foreground_cache_hits"));
pub const CACHE_FOREGROUND_MISSES: MetricId = MetricId(Cow::Borrowed("foreground_cache_misses"));
pub const CACHE_BACKGROUND_HITS: MetricId = MetricId(Cow::Borrowed("background_cache_hits"));
pub const CACHE_BACKGROUND_MISSES: MetricId = MetricId(Cow::Borrowed("background_cache_misses"));
pub const CACHE_FOREGROUND_PREFETCHES: MetricId =
    MetricId(Cow::Borrowed("foreground_cache_prefetches"));
pub const CACHE_FOREGROUND_WAITS: MetricId = MetricId(Cow::Borrowed("foreground_cache_waits"));
pub const CACHE_BACKGROUND_PREFETCHES: MetricId =
    MetricId(Cow::Borrowed("background_cache_prefetches"));
pub const CACHE_BACKGROUND_WAITS: MetricId = MetricId(Cow::Borrowed("background_cache_waits"));
pub const CACHE_FOREGROUND_HIT_RATE_PERCENT: MetricId =
    MetricId(Cow::Borrowed("foreground_cache_hit_rate_percent"));
pub const CACHE_BACKGROUND_HIT_RATE_PERCENT: MetricId =
    MetricId(Cow::Borrowed("background_cache_hit_rate_percent"));
pub const LOOSE_BATCHES_COUNT: MetricId = MetricId(Cow::Borrowed("loose_batches_count"));
pub const MERGING_BATCHES_COUNT: MetricId = MetricId(Cow::Borrowed("merging_batches_count"));
pub const LOOSE_MEMORY_RECORDS_COUNT: MetricId =
    MetricId(Cow::Borrowed("loose_memory_records_count"));
pub const LOOSE_STORAGE_RECORDS_COUNT: MetricId =
    MetricId(Cow::Borrowed("loose_storage_records_count"));
pub const MERGING_MEMORY_RECORDS_COUNT: MetricId =
    MetricId(Cow::Borrowed("merging_memory_records_count"));
pub const MERGING_STORAGE_RECORDS_COUNT: MetricId =
    MetricId(Cow::Borrowed("merging_storage_records_count"));
pub const COMPLETED_MERGES: MetricId = MetricId(Cow::Borrowed("completed_merges"));
pub const COMPACTION_STATE: MetricId = MetricId(Cow::Borrowed("compaction_state"));
pub const NEGATIVE_WEIGHT_COUNT: MetricId = MetricId(Cow::Borrowed("negative_weight_count"));
pub const BLOOM_FILTER_BITS_PER_KEY: MetricId =
    MetricId(Cow::Borrowed("bloom_filter_bits_per_key"));
pub const BLOOM_FILTER_HITS_COUNT: MetricId = MetricId(Cow::Borrowed("bloom_filter_hits_count"));
pub const BLOOM_FILTER_MISSES_COUNT: MetricId =
    MetricId(Cow::Borrowed("bloom_filter_misses_count"));
pub const BLOOM_FILTER_HIT_RATE_PERCENT: MetricId =
    MetricId(Cow::Borrowed("bloom_filter_hit_rate_percent"));
pub const BLOOM_FILTER_SIZE_BYTES: MetricId = MetricId(Cow::Borrowed("bloom_filter_size_bytes"));
pub const ROARING_FILTER_HITS_COUNT: MetricId =
    MetricId(Cow::Borrowed("roaring_filter_hits_count"));
pub const ROARING_FILTER_MISSES_COUNT: MetricId =
    MetricId(Cow::Borrowed("roaring_filter_misses_count"));
pub const ROARING_FILTER_HIT_RATE_PERCENT: MetricId =
    MetricId(Cow::Borrowed("roaring_filter_hit_rate_percent"));
pub const ROARING_FILTER_SIZE_BYTES: MetricId =
    MetricId(Cow::Borrowed("roaring_filter_size_bytes"));
pub const RANGE_FILTER_HITS_COUNT: MetricId = MetricId(Cow::Borrowed("range_filter_hits_count"));
pub const RANGE_FILTER_MISSES_COUNT: MetricId =
    MetricId(Cow::Borrowed("range_filter_misses_count"));
pub const RANGE_FILTER_HIT_RATE_PERCENT: MetricId =
    MetricId(Cow::Borrowed("range_filter_hit_rate_percent"));
pub const RANGE_FILTER_SIZE_BYTES: MetricId = MetricId(Cow::Borrowed("range_filter_size_bytes"));
pub const SPINE_COUNT: MetricId = MetricId(Cow::Borrowed("spine_count"));
pub const SPINE_BATCHES_COUNT: MetricId = MetricId(Cow::Borrowed("spine_batches_count"));
pub const SPINE_STORAGE_SIZE_BYTES: MetricId = MetricId(Cow::Borrowed("spine_storage_size_bytes"));
pub const MERGING_SIZE_BYTES: MetricId = MetricId(Cow::Borrowed("merging_size_bytes"));
pub const MERGE_REDUCTION_PERCENT: MetricId = MetricId(Cow::Borrowed("merge_reduction_percent"));
pub const MERGE_BACKPRESSURE_WAIT_TIME_SECONDS: MetricId =
    MetricId(Cow::Borrowed("merge_backpressure_wait_time_seconds"));
pub const SPINE_FLUSH_BATCH_TIME_SECONDS: MetricId =
    MetricId(Cow::Borrowed("spine_flush_batch_time_seconds"));
pub const SPINE_ADD_BATCH_TIME_SECONDS: MetricId =
    MetricId(Cow::Borrowed("spine_add_batch_time_seconds"));
pub const INVOCATIONS_COUNT: MetricId = MetricId(Cow::Borrowed("invocations_count"));
pub const RUNTIME_SECONDS: MetricId = MetricId(Cow::Borrowed("runtime_seconds"));
/// The fraction of an operator's runtime that it is actually running as opposed
/// to blocking in the kernel (e.g. because it is waiting for I/O).
pub const RUNTIME_NONBLOCKING_PERCENT: MetricId = MetricId(Cow::Borrowed("nonblocking_percent"));
pub const RUNTIME_PERCENT: MetricId = MetricId(Cow::Borrowed("runtime_percent"));
pub const CIRCUIT_WAIT_BY_REASON_SECONDS: MetricId =
    MetricId(Cow::Borrowed("circuit_wait_by_reason_seconds"));
pub const CIRCUIT_WAIT_TIME_SECONDS: MetricId =
    MetricId(Cow::Borrowed("circuit_wait_time_seconds"));
pub const STEPS_COUNT: MetricId = MetricId(Cow::Borrowed("steps_count"));
pub const CIRCUIT_RUNTIME_SECONDS: MetricId = MetricId(Cow::Borrowed("circuit_runtime_seconds"));

/// CPU time the worker thread used while evaluating steps.
pub const CIRCUIT_CPU_TIME_SECONDS: MetricId = MetricId(Cow::Borrowed("circuit_cpu_time_seconds"));

/// Fraction of the circuit's step time spent on the CPU.
pub const CIRCUIT_NONBLOCKING_PERCENT: MetricId =
    MetricId(Cow::Borrowed("circuit_nonblocking_percent"));
pub const CIRCUIT_IDLE_TIME_SECONDS: MetricId =
    MetricId(Cow::Borrowed("circuit_idle_time_seconds"));
pub const CIRCUIT_RUNTIME_ELAPSED_SECONDS: MetricId =
    MetricId(Cow::Borrowed("circuit_runtime_elapsed_seconds"));
pub const FOREGROUND_CACHE_OCCUPANCY: MetricId =
    MetricId(Cow::Borrowed("foreground_cache_occupancy"));
pub const BACKGROUND_CACHE_OCCUPANCY: MetricId =
    MetricId(Cow::Borrowed("background_cache_occupancy"));
pub const PREFIX_BATCHES_STATS: MetricId = MetricId(Cow::Borrowed("prefix_batches_stats"));
pub const INPUT_INTEGRAL_RECORDS_COUNT: MetricId =
    MetricId(Cow::Borrowed("input_integral_records_count"));

pub const CIRCUIT_METRICS: [CircuitMetric; 89] = [
    // State
    CircuitMetric {
        name: USED_MEMORY_BYTES,
        category: CircuitMetricCategory::State,
        advanced: true,
        description: "Bytes used by the in-memory state of the operator. See also 'allocated_memory_bytes'.",
    },
    CircuitMetric {
        name: ALLOCATED_MEMORY_BYTES,
        category: CircuitMetricCategory::State,
        advanced: false,
        description: "Total bytes reserved by the operator's in-memory state, including unused capacity; can exceed 'used_memory_bytes'.",
    },
    CircuitMetric {
        name: MEMORY_ALLOCATIONS_COUNT,
        category: CircuitMetricCategory::State,
        advanced: true,
        description: "Number of contiguous memory regions allocated for the operator's in-memory state.",
    },
    CircuitMetric {
        name: SHARED_MEMORY_BYTES,
        category: CircuitMetricCategory::State,
        advanced: true,
        description: "Bytes of in-memory state possibly shared with other operators.",
    },
    CircuitMetric {
        name: STATE_RECORDS_COUNT,
        category: CircuitMetricCategory::State,
        advanced: false,
        description: "Number of records stored in operator state (both in-memory and on-disk).",
    },
    CircuitMetric {
        name: SPINE_BATCHES_COUNT,
        category: CircuitMetricCategory::State,
        advanced: false,
        description: "Number of batches in the spine.",
    },
    CircuitMetric {
        name: SPINE_STORAGE_SIZE_BYTES,
        category: CircuitMetricCategory::State,
        advanced: false,
        description: "Size of the spine.",
    },
    CircuitMetric {
        name: MERGING_BATCHES_COUNT,
        category: CircuitMetricCategory::State,
        advanced: false,
        description: "Total number of currently merging batches in the operator's state.",
    },
    CircuitMetric {
        name: MERGING_MEMORY_RECORDS_COUNT,
        category: CircuitMetricCategory::State,
        advanced: false,
        description: "The number of in-memory records in merging batches.",
    },
    CircuitMetric {
        name: MERGING_STORAGE_RECORDS_COUNT,
        category: CircuitMetricCategory::State,
        advanced: false,
        description: "The number of on-disk records in merging batches.",
    },
    CircuitMetric {
        name: MERGING_SIZE_BYTES,
        category: CircuitMetricCategory::State,
        advanced: true,
        description: "Approximate combined in-memory and on-disk size of currently merging batches.",
    },
    CircuitMetric {
        name: MERGE_REDUCTION_PERCENT,
        category: CircuitMetricCategory::State,
        advanced: false,
        description: "Merge reduction. across all merges performed by the operator.",
    },
    CircuitMetric {
        name: LOOSE_BATCHES_COUNT,
        category: CircuitMetricCategory::State,
        advanced: false,
        description: "The number of loose batches, i.e., batches that are not being merged, in the operator's state.",
    },
    CircuitMetric {
        name: LOOSE_MEMORY_RECORDS_COUNT,
        category: CircuitMetricCategory::State,
        advanced: false,
        description: "The number of in-memory records in loose batches.",
    },
    CircuitMetric {
        name: LOOSE_STORAGE_RECORDS_COUNT,
        category: CircuitMetricCategory::State,
        advanced: false,
        description: "The number of on-disk records in loose batches.",
    },
    CircuitMetric {
        name: NEGATIVE_WEIGHT_COUNT,
        category: CircuitMetricCategory::State,
        advanced: false,
        description: "Total number of negative weight records in the spine.",
    },
    CircuitMetric {
        name: BLOOM_FILTER_BITS_PER_KEY,
        category: CircuitMetricCategory::State,
        advanced: false,
        description: "Average number of bits per key across batches that use a Bloom filter.",
    },
    CircuitMetric {
        name: BLOOM_FILTER_SIZE_BYTES,
        category: CircuitMetricCategory::State,
        advanced: false,
        description: "Size of the Bloom filter in bytes.",
    },
    CircuitMetric {
        name: BLOOM_FILTER_HITS_COUNT,
        category: CircuitMetricCategory::State,
        advanced: true,
        description: "The number of hits across all Bloom filters. The hits are summed across the Bloom filters for all batches in the spine.",
    },
    CircuitMetric {
        name: BLOOM_FILTER_MISSES_COUNT,
        category: CircuitMetricCategory::State,
        advanced: true,
        description: "The number of misses across all Bloom filters. The misses are summed across the Bloom filters for all batches in the spine.",
    },
    CircuitMetric {
        name: BLOOM_FILTER_HIT_RATE_PERCENT,
        category: CircuitMetricCategory::State,
        advanced: false,
        description: "Hit rate of the Bloom filter.",
    },
    CircuitMetric {
        name: ROARING_FILTER_SIZE_BYTES,
        category: CircuitMetricCategory::State,
        advanced: false,
        description: "Size of the bitmap filter in bytes.",
    },
    CircuitMetric {
        name: ROARING_FILTER_HITS_COUNT,
        category: CircuitMetricCategory::State,
        advanced: true,
        description: "The number of hits across all bitmap filters. The hits are summed across the bitmap filters for all batches in the spine.",
    },
    CircuitMetric {
        name: ROARING_FILTER_MISSES_COUNT,
        category: CircuitMetricCategory::State,
        advanced: true,
        description: "The number of misses across all bitmap filters. The misses are summed across the bitmap filters for all batches in the spine.",
    },
    CircuitMetric {
        name: ROARING_FILTER_HIT_RATE_PERCENT,
        category: CircuitMetricCategory::State,
        advanced: false,
        description: "Hit rate of the bitmap filter.",
    },
    CircuitMetric {
        name: RANGE_FILTER_SIZE_BYTES,
        category: CircuitMetricCategory::State,
        advanced: true,
        description: "Size of the cached range filter in bytes.",
    },
    CircuitMetric {
        name: RANGE_FILTER_HITS_COUNT,
        category: CircuitMetricCategory::State,
        advanced: true,
        description: "The number of hits across all range filters. The hits are summed across the range filters for all batches in the spine.",
    },
    CircuitMetric {
        name: RANGE_FILTER_MISSES_COUNT,
        category: CircuitMetricCategory::State,
        advanced: true,
        description: "The number of misses across all range filters. The misses are summed across the range filters for all batches in the spine.",
    },
    CircuitMetric {
        name: RANGE_FILTER_HIT_RATE_PERCENT,
        category: CircuitMetricCategory::State,
        advanced: false,
        description: "Hit rate of the range filter.",
    },
    CircuitMetric {
        name: RETAINMENT_BOUNDS,
        category: CircuitMetricCategory::State,
        advanced: true,
        description: "Bounds used by the garbage collector to discard unused state.",
    },
    CircuitMetric {
        name: SIZE_DISTRIBUTION,
        category: CircuitMetricCategory::State,
        advanced: true,
        description: "Distribution of sizes in batches. The metric value is an array of counts, one for each batch in the state.",
    },
    CircuitMetric {
        name: COMPLETED_MERGES,
        category: CircuitMetricCategory::State,
        advanced: true,
        description: "Information about the batches that were compacted (merged).",
    },
    CircuitMetric {
        name: COMPACTION_STATE,
        category: CircuitMetricCategory::State,
        advanced: true,
        description: "State of the compaction process.",
    },
    // Inputs
    CircuitMetric {
        name: INPUT_RECORDS_COUNT,
        category: CircuitMetricCategory::Inputs,
        advanced: false,
        description: "Total input records ingested by the operator.",
    },
    CircuitMetric {
        name: INPUT_BATCHES_STATS,
        category: CircuitMetricCategory::Inputs,
        advanced: false,
        description: "Distribution of input batch sizes processed by the operator.",
    },
    CircuitMetric {
        name: LEFT_INPUT_BATCHES_STATS,
        category: CircuitMetricCategory::Inputs,
        advanced: false,
        description: "Distribution of input batch sizes in the left input stream of an asof-join operator.",
    },
    CircuitMetric {
        name: RIGHT_INPUT_BATCHES_STATS,
        category: CircuitMetricCategory::Inputs,
        advanced: false,
        description: "Distribution of input batch sizes in the right input stream of an asof-join operator.",
    },
    CircuitMetric {
        name: LEFT_INPUT_RECORDS_COUNT,
        category: CircuitMetricCategory::Inputs,
        advanced: false,
        description: "Total number of input records received in the left input stream of a join operator.",
    },
    CircuitMetric {
        name: RIGHT_INPUT_INTEGRAL_RECORDS_COUNT,
        category: CircuitMetricCategory::Inputs,
        advanced: false,
        description: "The size of the integral of the right input stream of a join operator.",
    },
    CircuitMetric {
        name: PREFIX_BATCHES_STATS,
        category: CircuitMetricCategory::Inputs,
        advanced: false,
        description: "Distribution of prefix batch sizes ingested by a match operator.",
    },
    CircuitMetric {
        name: INPUT_INTEGRAL_RECORDS_COUNT,
        category: CircuitMetricCategory::Inputs,
        advanced: false,
        description: "The size of the integral input to a match operator.",
    },
    // Outputs
    CircuitMetric {
        name: OUTPUT_BATCHES_STATS,
        category: CircuitMetricCategory::Outputs,
        advanced: false,
        description: "Distribution of output batch sizes produced by the operator.",
    },
    CircuitMetric {
        name: OUTPUT_ADJUSTMENT_STATS,
        category: CircuitMetricCategory::Outputs,
        advanced: false,
        description: "Distribution of the sizes of the adjustments an input map resolves a transaction's updates into.",
    },
    CircuitMetric {
        name: UNREAD_UPDATES_COUNT,
        category: CircuitMetricCategory::Inputs,
        advanced: true,
        description: "Keys a lazy input map resolved without reading the update it collected, because the key was written once and the transaction held no deletes. Against 'input_batches_stats' it says how often the shortcut applied.",
    },
    CircuitMetric {
        name: CONFLICTING_UPDATES_COUNT,
        category: CircuitMetricCategory::Multihost,
        advanced: false,
        description: "Updates that arrived at the same step as another update to their key, which happens when several hosts ingest that key in one transaction.",
    },
    CircuitMetric {
        name: COMPUTED_OUTPUT_RECORDS_COUNT,
        category: CircuitMetricCategory::Outputs,
        advanced: false,
        description: "Total number of output records computed by the operator. This number can include duplicate records and be greater than the number of output records produced by the operator.",
    },
    CircuitMetric {
        name: OUTPUT_REDUNDANCY_PERCENT,
        category: CircuitMetricCategory::Outputs,
        advanced: false,
        description: "Percentage of redundant output records eliminated during compaction.",
    },
    // Time
    CircuitMetric {
        name: INVOCATIONS_COUNT,
        category: CircuitMetricCategory::Time,
        advanced: true,
        description: "Number of times the operator has been invoked.",
    },
    CircuitMetric {
        name: RUNTIME_SECONDS,
        category: CircuitMetricCategory::Time,
        advanced: false,
        description: "Total time spent evaluating the operator.",
    },
    CircuitMetric {
        name: RUNTIME_PERCENT,
        category: CircuitMetricCategory::Time,
        advanced: false,
        description: "Percentage of time spent evaluating the operator as a fraction of the total runtime of all operators in the circuit.",
    },
    CircuitMetric {
        name: CIRCUIT_WAIT_BY_REASON_SECONDS,
        category: CircuitMetricCategory::Time,
        advanced: true,
        description: "'circuit_wait_time_seconds' split by what the worker was waiting for.",
    },
    CircuitMetric {
        name: CIRCUIT_WAIT_TIME_SECONDS,
        category: CircuitMetricCategory::Time,
        advanced: false,
        description: "Time during a step when the worker was not running: its async runtime had nothing to run, for example while waiting for other workers at an exchange or for background work to finish, or its thread was blocked in a storage system call, reading a layer-file block, spilling blocks, fsyncing a layer file, or creating, renaming and unlinking one. 'circuit_wait_by_reason_seconds' says which. A system call counts only for the part its thread spends off the CPU: the rest of it runs in the kernel on the worker's own thread and is already in 'circuit_cpu_time_seconds'. Other blocking an operator does without yielding is not counted here and shows up instead as a low 'circuit_nonblocking_percent'.",
    },
    CircuitMetric {
        name: STEPS_COUNT,
        category: CircuitMetricCategory::Time,
        advanced: false,
        description: "Number of steps performed by the circuit.",
    },
    CircuitMetric {
        name: CIRCUIT_RUNTIME_SECONDS,
        category: CircuitMetricCategory::Time,
        advanced: false,
        description: "Total time spent evaluating the circuit: operator runtime, circuit wait time ('circuit_wait_time_seconds'), and time blocked in the kernel or descheduled. See also 'circuit_cpu_time_seconds'.",
    },
    CircuitMetric {
        name: CIRCUIT_CPU_TIME_SECONDS,
        category: CircuitMetricCategory::Time,
        advanced: false,
        description: "CPU time the worker thread used while evaluating steps. Subtracting this and 'circuit_wait_time_seconds' from 'circuit_runtime_seconds' leaves the time the worker was blocked in the kernel or descheduled.",
    },
    CircuitMetric {
        name: CIRCUIT_NONBLOCKING_PERCENT,
        category: CircuitMetricCategory::Time,
        advanced: true,
        description: "Fraction of the circuit's step time spent on the CPU, as opposed to waiting for other workers, blocking in the kernel, or being descheduled.",
    },
    CircuitMetric {
        name: CIRCUIT_IDLE_TIME_SECONDS,
        category: CircuitMetricCategory::Time,
        advanced: false,
        description: "Time between one step ending and the next beginning. This time includes waiting for the other workers to finish their step and for the pipeline to initiate the next step.",
    },
    CircuitMetric {
        name: CIRCUIT_RUNTIME_ELAPSED_SECONDS,
        category: CircuitMetricCategory::Time,
        advanced: true,
        description: "Time elapsed while the circuit is executing a step, multiplied by the number of foreground and background threads.",
    },
    CircuitMetric {
        name: EXCHANGE_WAIT_TIME_SECONDS,
        category: CircuitMetricCategory::Time,
        advanced: true,
        description: "Total time the exchange operator spent waiting for data from other workers. This includes the time between the operator sent local data to its peers and the time it received data from all peers. Other operators in the circuit may be running during this time.",
    },
    CircuitMetric {
        name: MERGE_BACKPRESSURE_WAIT_TIME_SECONDS,
        category: CircuitMetricCategory::Time,
        advanced: false,
        description: "Time spent waiting for backpressure.",
    },
    CircuitMetric {
        name: SPINE_FLUSH_BATCH_TIME_SECONDS,
        category: CircuitMetricCategory::Time,
        advanced: true,
        description: "Time an operator spent in the spine's pre-insert flush, which writes a batch to storage when the batch is large enough that the spine would rather not hold it in memory. The operator's thread is blocked throughout, so this time counts against 'circuit_nonblocking_percent' without showing up as a wait.",
    },
    CircuitMetric {
        name: SPINE_ADD_BATCH_TIME_SECONDS,
        category: CircuitMetricCategory::Time,
        advanced: true,
        description: "Time an operator spent inserting a batch into the spine, excluding the eager spill and the backpressure wait that bracket it. This is mostly contention on the lock that the mergers hold while they rearrange the same state.",
    },
    CircuitMetric {
        name: STEP_DURATION_HISTOGRAM,
        category: CircuitMetricCategory::Time,
        advanced: true,
        description: "How long the operator held each of the steps it ran in, as a distribution rather than a total. Workers meet at a barrier every step, so a step costs every worker what the slowest one spent in it, and only the spread says whether that is happening.",
    },
    CircuitMetric {
        name: RUNTIME_NONBLOCKING_PERCENT,
        category: CircuitMetricCategory::Time,
        advanced: true,
        description: "Fraction of operator runtime spent running, as opposed to blocking in the kernel (e.g. because it is waiting for I/O) or descheduled by the kernel (because there are fewer CPUs than runnable threads).",
    },
    // Balancer
    CircuitMetric {
        name: KEY_DISTRIBUTION,
        category: CircuitMetricCategory::Balancer,
        advanced: true,
        description: "Distribution of input keys received by the local worker. The metric value is an array of counts, where each index corresponds to a worker and the value at that index is the number of input records mapped to that worker based on the hash of the key.",
    },
    CircuitMetric {
        name: LOCAL_SHARD_RECORDS_COUNT,
        category: CircuitMetricCategory::Balancer,
        advanced: false,
        description: "Total number of input records mapped to the local worker based on the hash of the key. This metric is used to measure the skew of the input stream. The stream is skewed if the number of records mapped to some of the workers is significantly higher from the average number of records per worker.",
    },
    CircuitMetric {
        name: BALANCER_POLICY,
        category: CircuitMetricCategory::Balancer,
        advanced: false,
        description: "Current balancing policy.",
    },
    CircuitMetric {
        name: RABALANCINGS_COUNT,
        category: CircuitMetricCategory::Balancer,
        advanced: false,
        description: "Number of stream rebalancing events. A rebalancing event occurs when the balancing policy is changed and requires repartitioning of the input stream across workers.",
    },
    CircuitMetric {
        name: REBALANCING_IN_PROGRESS,
        category: CircuitMetricCategory::Balancer,
        advanced: false,
        description: "Indicates if rebalancing is currently in progress.",
    },
    CircuitMetric {
        name: ACCUMULATOR_RECORDS_TO_REPARTITION_COUNT,
        category: CircuitMetricCategory::Balancer,
        advanced: true,
        description: "The number of records in the local accumulator that need to be repartitioned in the current rebalance.",
    },
    CircuitMetric {
        name: INTEGRAL_RECORDS_TO_REPARTITION_COUNT,
        category: CircuitMetricCategory::Balancer,
        advanced: true,
        description: "The number of records in the local integral operator that need to be repartitioned in the current rebalance.",
    },
    CircuitMetric {
        name: TOTAL_REBALANCING_TIME_SECONDS,
        category: CircuitMetricCategory::Balancer,
        advanced: false,
        description: "Total time spent rebalancing the stream.",
    },
    CircuitMetric {
        name: INPROGRESS_REBALANCING_TIME_SECONDS,
        category: CircuitMetricCategory::Balancer,
        advanced: false,
        description: "Elapsed time for the current rebalance.",
    },
    // Cache
    CircuitMetric {
        name: CACHE_FOREGROUND_HITS,
        category: CircuitMetricCategory::Cache,
        advanced: false,
        description: "Statistics about cache hits in the foreground thread.",
    },
    CircuitMetric {
        name: CACHE_FOREGROUND_MISSES,
        category: CircuitMetricCategory::Cache,
        advanced: false,
        description: "Statistics about cache misses in the foreground thread.",
    },
    CircuitMetric {
        name: CACHE_BACKGROUND_HITS,
        category: CircuitMetricCategory::Cache,
        advanced: false,
        description: "Statistics about cache hits in the background thread.",
    },
    CircuitMetric {
        name: CACHE_BACKGROUND_MISSES,
        category: CircuitMetricCategory::Cache,
        advanced: false,
        description: "Statistics about cache misses in the background thread.",
    },
    CircuitMetric {
        name: CACHE_FOREGROUND_PREFETCHES,
        category: CircuitMetricCategory::Cache,
        advanced: true,
        description: "Blocks the foreground thread asked storage for ahead of need, so that a cursor walking forward finds them in the cache. Bytes are the blocks' sizes; the time is zero, since the reads run in the background.",
    },
    CircuitMetric {
        name: CACHE_FOREGROUND_WAITS,
        category: CircuitMetricCategory::Cache,
        advanced: true,
        description: "Blocks the foreground thread wanted while a read issued ahead of need was still in flight. The time is how long it waited; each is a round trip the read-ahead did not fully hide.",
    },
    CircuitMetric {
        name: CACHE_BACKGROUND_PREFETCHES,
        category: CircuitMetricCategory::Cache,
        advanced: true,
        description: "Blocks a background thread asked storage for ahead of need.",
    },
    CircuitMetric {
        name: CACHE_BACKGROUND_WAITS,
        category: CircuitMetricCategory::Cache,
        advanced: true,
        description: "Blocks a background thread wanted while a read issued ahead of need was still in flight, and the time it waited for them.",
    },
    CircuitMetric {
        name: CACHE_FOREGROUND_HIT_RATE_PERCENT,
        category: CircuitMetricCategory::Cache,
        advanced: false,
        description: "Cache hit rate for the foreground thread.",
    },
    CircuitMetric {
        name: CACHE_BACKGROUND_HIT_RATE_PERCENT,
        category: CircuitMetricCategory::Cache,
        advanced: false,
        description: "Cache hit rate for the background thread.",
    },
    CircuitMetric {
        name: FOREGROUND_CACHE_OCCUPANCY,
        category: CircuitMetricCategory::Cache,
        advanced: false,
        description: "Occupancy of the foreground cache.",
    },
    CircuitMetric {
        name: BACKGROUND_CACHE_OCCUPANCY,
        category: CircuitMetricCategory::Cache,
        advanced: false,
        description: "Occupancy of the background cache.",
    },
    CircuitMetric {
        name: EXCHANGE_SERIALIZATION_TIME_SECONDS,
        category: CircuitMetricCategory::Multihost,
        advanced: false,
        description: "Time spent serializing data to send to other hosts.",
    },
    CircuitMetric {
        name: EXCHANGE_SERIALIZED_BYTES,
        category: CircuitMetricCategory::Multihost,
        advanced: false,
        description: "Amount of data serialized to send to other hosts.",
    },
    CircuitMetric {
        name: EXCHANGE_DESERIALIZATION_TIME_SECONDS,
        category: CircuitMetricCategory::Multihost,
        advanced: false,
        description: "Time spent deserializing data received from other hosts.",
    },
    CircuitMetric {
        name: EXCHANGE_DESERIALIZED_BYTES,
        category: CircuitMetricCategory::Multihost,
        advanced: false,
        description: "Amount of serialized data received from other hosts.",
    },
];

/// An operator's location within the source program
pub type OperatorLocation = Option<&'static Location<'static>>;

/// The label to a metadata item
pub type MetaLabel = Cow<'static, str>;

/// Stats about batch sizes.
///
/// Can be used to track the distribution of batch sizes in input/output streams.
/// Batches here don't have to be DBSP `Batch`s. These can be vector of tuples or
/// anything else that has a size.
// TODO: add a histogram.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct BatchSizeStats {
    /// Smallest batch size.
    min: usize,

    /// Largest batch size.
    max: usize,

    /// The number of batches.
    cnt: usize,

    /// Total size.
    total: usize,
}

impl BatchSizeStats {
    pub const fn new() -> Self {
        Self {
            min: usize::MAX,
            max: 0,
            cnt: 0,
            total: 0,
        }
    }

    pub fn add_batch(&mut self, size: usize) {
        self.cnt += 1;
        self.total = self.total.wrapping_add(size);
        self.min = if size < self.min { size } else { self.min };
        self.max = if size > self.max { size } else { self.max };
    }

    pub fn total_size(&self) -> usize {
        self.total
    }

    pub fn metadata(&self) -> MetaItem {
        MetaItem::Map(BTreeMap::from([
            (Cow::Borrowed("batches_count"), MetaItem::Count(self.cnt)),
            (
                Cow::Borrowed("min_records_count"),
                MetaItem::Count(if self.cnt == 0 { 0 } else { self.min }),
            ),
            (
                Cow::Borrowed("max_records_count"),
                MetaItem::Count(self.max),
            ),
            (
                Cow::Borrowed("avg_records_count"),
                MetaItem::Count(self.total.checked_div(self.cnt).unwrap_or(0)),
            ),
            (
                Cow::Borrowed("total_records_count"),
                MetaItem::Count(self.total),
            ),
        ]))
    }
}

/// A log-scale histogram of durations.
///
/// A mean says how long something took on average; a histogram says whether
/// that average describes anything.  Buckets are spaced so that each covers
/// about a quarter of an octave, which holds the error on any reading to under
/// 20% while keeping the whole histogram in a fixed array with no allocation
/// and no locking on the recording path.
#[derive(Clone, Debug, PartialEq)]
pub struct DurationHistogram {
    /// Samples per bucket, in microseconds, indexed by [`Self::bucket`].
    buckets: [u64; Self::BUCKETS],
    count: u64,
    total: Duration,
    min: Duration,
    max: Duration,
}

impl Default for DurationHistogram {
    fn default() -> Self {
        Self::new()
    }
}

impl DurationHistogram {
    /// Buckets per octave, as a power of two.
    const SUB_BITS: u32 = 2;
    const SUB: u64 = 1 << Self::SUB_BITS;

    /// Enough octaves to reach an hour, past which a bucket saturates.
    const OCTAVES: usize = 32;
    const BUCKETS: usize = (Self::OCTAVES + 1) * Self::SUB as usize;

    pub const fn new() -> Self {
        Self {
            buckets: [0; Self::BUCKETS],
            count: 0,
            total: Duration::ZERO,
            min: Duration::MAX,
            max: Duration::ZERO,
        }
    }

    /// The bucket `micros` falls in.
    ///
    /// Below [`Self::SUB`] each microsecond has a bucket of its own; above it,
    /// the octave picks a group of [`Self::SUB`] and the leading mantissa bits
    /// pick within the group.  Monotonic in `micros`, which is what lets a
    /// reader treat the buckets as a sorted distribution.
    fn bucket(micros: u64) -> usize {
        if micros < Self::SUB {
            return micros as usize;
        }
        let octave = u64::BITS - 1 - micros.leading_zeros();
        let index = (octave - Self::SUB_BITS + 1) as usize * Self::SUB as usize
            + ((micros >> (octave - Self::SUB_BITS)) & (Self::SUB - 1)) as usize;
        index.min(Self::BUCKETS - 1)
    }

    /// The smallest duration in microseconds that `bucket` holds.
    fn bucket_start(bucket: usize) -> u64 {
        let bucket = bucket as u64;
        if bucket < Self::SUB {
            return bucket;
        }
        let octave = bucket / Self::SUB + Self::SUB_BITS as u64 - 1;
        let mantissa = bucket % Self::SUB;
        (Self::SUB + mantissa) << (octave - Self::SUB_BITS as u64)
    }

    pub fn add(&mut self, sample: Duration) {
        self.buckets[Self::bucket(sample.as_micros().min(u64::MAX as u128) as u64)] += 1;
        self.count += 1;
        // Saturating because this runs for the life of the operator and a
        // total that panics is worse than a total that stops being exact.
        self.total = self.total.saturating_add(sample);
        self.min = self.min.min(sample);
        self.max = self.max.max(sample);
    }

    pub fn count(&self) -> u64 {
        self.count
    }

    /// The smallest duration that at least `fraction` of the samples are under.
    ///
    /// Reads off the bucket a sample falls in, so the answer is that bucket's
    /// lower bound rather than the sample itself.
    pub fn quantile(&self, fraction: f64) -> Duration {
        if self.count == 0 {
            return Duration::ZERO;
        }
        let want = (fraction * self.count as f64).ceil() as u64;
        let mut seen = 0;
        for (bucket, samples) in self.buckets.iter().enumerate() {
            seen += samples;
            if seen >= want.max(1) {
                return Duration::from_micros(Self::bucket_start(bucket)).max(self.min);
            }
        }
        self.max
    }

    pub fn mean(&self) -> Duration {
        self.total
            .checked_div(self.count.try_into().unwrap_or(u32::MAX))
            .unwrap_or(Duration::ZERO)
    }

    pub fn metadata(&self) -> MetaItem {
        if self.count == 0 {
            return MetaItem::Map(BTreeMap::from([(
                Cow::Borrowed("samples_count"),
                MetaItem::Count(0),
            )]));
        }

        // Only the buckets that hold something, so a distribution spanning four
        // octaves reads as four lines rather than a hundred and thirty.
        let mut items = BTreeMap::from([
            (
                Cow::Borrowed("samples_count"),
                MetaItem::Count(self.count as usize),
            ),
            (Cow::Borrowed("min"), MetaItem::Duration(self.min)),
            (
                Cow::Borrowed("p50"),
                MetaItem::Duration(self.quantile(0.50)),
            ),
            (
                Cow::Borrowed("p90"),
                MetaItem::Duration(self.quantile(0.90)),
            ),
            (
                Cow::Borrowed("p99"),
                MetaItem::Duration(self.quantile(0.99)),
            ),
            (Cow::Borrowed("max"), MetaItem::Duration(self.max)),
            (Cow::Borrowed("mean"), MetaItem::Duration(self.mean())),
            (Cow::Borrowed("total"), MetaItem::Duration(self.total)),
        ]);
        // An ordered array rather than a map, so the buckets stay in
        // increasing order and a reader sees the shape of the distribution.
        // Empty buckets are dropped: a span of four octaves reads as a handful
        // of lines rather than all hundred and thirty-two.
        let buckets = self
            .buckets
            .iter()
            .enumerate()
            .filter(|(_, samples)| **samples > 0)
            .map(|(bucket, samples)| {
                MetaItem::Map(BTreeMap::from([
                    (
                        Cow::Borrowed("ge"),
                        MetaItem::Duration(Duration::from_micros(Self::bucket_start(bucket))),
                    ),
                    (Cow::Borrowed("samples"), MetaItem::Count(*samples as usize)),
                ]))
            })
            .collect();
        items.insert(Cow::Borrowed("buckets"), MetaItem::Array(buckets));
        MetaItem::Map(items)
    }
}

/// General metadata about an operator's execution
#[derive(Debug, Clone, PartialEq, Default)]
pub struct OperatorMeta {
    entries: BTreeMap<(MetricId, MetricLabels), MetaItem>,
}

#[derive(Serialize)]
struct MetricReadingRef<'a> {
    metric_id: &'a MetricId,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    labels: &'a MetricLabels,
    value: &'a MetaItem,
}

impl Serialize for OperatorMeta {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.entries.len()))?;
        for ((metric_id, labels), value) in &self.entries {
            let reading = MetricReadingRef {
                metric_id,
                labels,
                value,
            };
            seq.serialize_element(&reading)?;
        }
        seq.end()
    }
}

impl OperatorMeta {
    /// Create a new `OperatorMeta`
    pub const fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
        }
    }

    pub fn get(&self, metric_id: MetricId) -> Option<MetaItem> {
        self.entries.get(&(metric_id, Vec::new())).cloned()
    }

    /// Every reading of `metric_id`, with the labels that tell them apart.
    ///
    /// [`Self::get`] answers for the one reading that carries no labels; a
    /// metric that reports a value per label, such as one wait time per reason,
    /// is read here.
    pub fn readings(
        &self,
        metric_id: &MetricId,
    ) -> impl Iterator<Item = (&MetricLabels, &MetaItem)> {
        self.entries
            .iter()
            .filter(move |((id, _), _)| id == metric_id)
            .map(|((_, labels), value)| (labels, value))
    }

    pub fn merge(&mut self, other: &Self) {
        for (label, src) in &other.entries {
            if src.is_mergeable() {
                if let Some(dst) = self.entries.get_mut(label) {
                    if let Some(merged) = src.merge(dst) {
                        *dst = merged;
                    } else {
                        self.entries.remove(label);
                    }
                } else {
                    self.entries.insert(label.clone(), src.clone());
                }
            }
        }
    }

    pub fn insert(&mut self, metric_id: MetricId, labels: MetricLabels, value: MetaItem) {
        self.entries.insert((metric_id, labels), value);
    }
}

impl Deref for OperatorMeta {
    type Target = BTreeMap<(MetricId, MetricLabels), MetaItem>;

    fn deref(&self) -> &Self::Target {
        &self.entries
    }
}

impl DerefMut for OperatorMeta {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.entries
    }
}

impl Extend<MetricReading> for OperatorMeta {
    fn extend<T>(&mut self, iter: T)
    where
        T: IntoIterator<Item = MetricReading>,
    {
        for reading in iter {
            self.entries
                .insert((reading.metric_id, reading.labels), reading.value);
        }
    }
}

impl<const N: usize> From<[MetricReading; N]> for OperatorMeta {
    fn from(array: [MetricReading; N]) -> Self {
        let mut this = Self::new();
        this.extend(array);
        this
    }
}

impl<'a> From<&'a [MetricReading]> for OperatorMeta {
    fn from(slice: &'a [MetricReading]) -> Self {
        let mut this = Self::new();
        this.extend(slice.iter().cloned());
        this
    }
}
/// An operator metadata entry
#[derive(Debug, Clone, PartialEq)]
pub enum MetaItem {
    /// An integer with no particular semantics.
    Int(usize),

    /// An integer count of something.
    ///
    /// This should be used for kinds of things that make sense to summarize by
    /// adding, e.g. counts of allocations or stored batches.
    Count(usize),

    /// A percentage in terms of a numerator and denominator. Separating these
    /// makes it possible to aggregate them.
    Percent {
        numerator: u64,
        denominator: u64,
    },

    CacheCounts(CacheCounts),

    String(String),
    Array(Vec<Self>),
    Map(BTreeMap<Cow<'static, str>, MetaItem>),
    Bytes(HumanBytes),
    Duration(Duration),
    Bool(bool),
}

#[derive(Serialize)]
struct PercentValue {
    numerator: u64,
    denominator: u64,
}

impl MetaItem {
    pub fn as_string(&self) -> Option<&str> {
        match self {
            MetaItem::String(string) => Some(string.as_str()),
            _ => None,
        }
    }
}

impl Serialize for MetaItem {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        fn serialize_typed<S, T>(
            serializer: S,
            type_name: &'static str,
            value: &T,
        ) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
            T: ?Sized + Serialize,
        {
            let mut state = serializer.serialize_struct("MetaItem", 2)?;
            state.serialize_field("type", type_name)?;
            state.serialize_field("value", value)?;
            state.end()
        }

        match self {
            MetaItem::Int(x) => {
                let value = *x as u64;
                serialize_typed(serializer, "int", &value)
            }
            MetaItem::Count(x) => {
                let value = *x as u64;
                serialize_typed(serializer, "count", &value)
            }
            MetaItem::Percent {
                numerator,
                denominator,
            } => {
                let value = PercentValue {
                    numerator: *numerator,
                    denominator: *denominator,
                };
                serialize_typed(serializer, "percent", &value)
            }
            MetaItem::CacheCounts(cache_counts) => {
                serialize_typed(serializer, "cachecounts", cache_counts)
            }
            MetaItem::String(x) => serialize_typed(serializer, "string", x),
            MetaItem::Array(meta_items) => meta_items.serialize(serializer),
            MetaItem::Map(operator_meta) => operator_meta.serialize(serializer),
            MetaItem::Bytes(human_bytes) => {
                let value = human_bytes.into_inner() as i128;
                serialize_typed(serializer, "bytes", &value)
            }
            MetaItem::Duration(duration) => serialize_typed(serializer, "duration", duration),
            MetaItem::Bool(bool) => serialize_typed(serializer, "bool", bool),
        }
    }
}

impl MetaItem {
    pub fn bytes(bytes: usize) -> Self {
        Self::Bytes(HumanBytes::from(bytes))
    }

    pub fn format(&self, output: &mut dyn Write) -> fmt::Result {
        match self {
            Self::Int(int) | Self::Count(int) => {
                write!(output, "{}", int.to_formatted_string(&Locale::en))
            }
            Self::Percent {
                numerator,
                denominator,
            } => {
                let percent = (*numerator as f64) / (*denominator as f64) * 100.0;
                if !percent.is_nan() && !percent.is_infinite() {
                    write!(output, "{percent:.02}%")
                } else {
                    write!(output, "(undefined)")
                }
            }
            Self::CacheCounts(CacheCounts {
                count,
                bytes,
                elapsed,
            }) => {
                if *count > 0 {
                    write!(
                        output,
                        "{count} ({}) over {:.1} s ({} ns/op)",
                        HumanBytes::new(*bytes),
                        elapsed.as_secs_f64(),
                        elapsed.as_nanos() / *count as u128
                    )
                } else {
                    write!(output, "none")
                }
            }
            Self::String(string) => output.write_str(string),
            Self::Bytes(bytes) => write!(output, "{bytes}"),
            Self::Duration(duration) => write!(output, "{duration:#?}"),

            Self::Array(array) => {
                output.write_char('[')?;
                for (idx, item) in array.iter().enumerate() {
                    item.format(output)?;
                    if idx != array.len() - 1 {
                        output.write_str(", ")?
                    }
                }
                output.write_char(']')
            }

            Self::Map(map) => {
                output.write_char('{')?;
                for (idx, (label, item)) in map.iter().enumerate() {
                    output.write_str(label)?;
                    output.write_str(": ")?;
                    item.format(output)?;

                    if idx != map.len() - 1 {
                        output.write_str(", ")?;
                    }
                }
                output.write_char('}')
            }
            Self::Bool(bool) => write!(output, "{bool}"),
        }
    }

    pub fn is_mergeable(&self) -> bool {
        matches!(
            self,
            MetaItem::Count(_)
                | MetaItem::Bytes(_)
                | MetaItem::CacheCounts(..)
                | MetaItem::Duration(_)
                | MetaItem::Percent { .. }
        )
    }

    pub fn merge(&self, other: &Self) -> Option<Self> {
        match (self, other) {
            (Self::Count(a), Self::Count(b)) => Some(Self::Count(a + b)),
            (
                Self::Percent {
                    numerator: an,
                    denominator: ad,
                },
                Self::Percent {
                    numerator: bn,
                    denominator: bd,
                },
            ) => Some(Self::Percent {
                numerator: an + bn,
                denominator: ad + bd,
            }),
            (Self::Bytes(a), Self::Bytes(b)) => Some(Self::Bytes(HumanBytes {
                bytes: a.bytes + b.bytes,
            })),
            (Self::CacheCounts(a), Self::CacheCounts(b)) => Some(Self::CacheCounts(*a + *b)),
            (Self::Duration(a), Self::Duration(b)) => Some(Self::Duration(a.saturating_add(*b))),
            _ => None,
        }
    }
}

impl Default for MetaItem {
    fn default() -> Self {
        Self::String(String::new())
    }
}

impl From<Duration> for MetaItem {
    fn from(duration: Duration) -> Self {
        Self::Duration(duration)
    }
}

impl From<HumanBytes> for MetaItem {
    fn from(bytes: HumanBytes) -> Self {
        Self::Bytes(bytes)
    }
}

impl From<String> for MetaItem {
    fn from(string: String) -> Self {
        Self::String(string)
    }
}

impl From<usize> for MetaItem {
    fn from(int: usize) -> Self {
        Self::Int(int)
    }
}

impl From<bool> for MetaItem {
    fn from(bool: bool) -> Self {
        Self::Bool(bool)
    }
}

#[macro_export]
macro_rules! metadata {
    ($($name:expr_2021 => $value:expr_2021),* $(,)?) => {
        [$(($crate::circuit::metadata::MetricReading::new($name, Vec::new(), $crate::circuit::metadata::MetaItem::from($value))),)*]
    };
}

impl From<TotalSize> for MetaItem {
    fn from(size: TotalSize) -> Self {
        Self::Map(BTreeMap::from([
            (
                Cow::Borrowed("allocated bytes"),
                Self::bytes(size.total_bytes()),
            ),
            (Cow::Borrowed("used bytes"), Self::bytes(size.used_bytes())),
            (
                Cow::Borrowed("allocations"),
                Self::Count(size.distinct_allocations()),
            ),
            (
                Cow::Borrowed("shared bytes"),
                Self::bytes(size.shared_bytes()),
            ),
        ]))
    }
}

#[cfg(test)]
mod test {
    use super::{DurationHistogram, MetaItem};
    use std::time::Duration;

    /// Buckets have to be ordered for the histogram to read as a
    /// distribution, and each has to contain what it claims to.
    #[test]
    fn buckets_are_ordered_and_hold_what_they_claim() {
        let mut previous = 0;
        for micros in 0..100_000u64 {
            let bucket = DurationHistogram::bucket(micros);
            assert!(bucket >= previous, "bucket fell at {micros} us");
            previous = bucket;

            let start = DurationHistogram::bucket_start(bucket);
            assert!(
                start <= micros,
                "{micros} us landed in a bucket starting at {start}"
            );
            let next = DurationHistogram::bucket_start(bucket + 1);
            assert!(
                micros < next,
                "{micros} us landed below a bucket ending at {next}"
            );
        }
    }

    /// A bucket reading is worth using only if it is close to the sample that
    /// produced it.  Four buckets to the octave puts the floor within 20%.
    #[test]
    fn a_bucket_is_within_a_fifth_of_its_samples() {
        for micros in DurationHistogram::SUB..10_000_000 {
            let start = DurationHistogram::bucket_start(DurationHistogram::bucket(micros));
            assert!(
                (micros - start) as f64 / micros as f64 <= 0.2,
                "{micros} us reads as {start} us"
            );
        }
    }

    /// The largest bucket saturates rather than running off the end of the
    /// array, so a pathological sample cannot panic the recording path.
    #[test]
    fn an_enormous_sample_saturates() {
        let mut histogram = DurationHistogram::new();
        histogram.add(Duration::from_secs(u32::MAX as u64));
        histogram.add(Duration::MAX);
        assert_eq!(histogram.count(), 2);
    }

    #[test]
    fn an_empty_histogram_reads_as_empty() {
        let histogram = DurationHistogram::new();
        assert_eq!(histogram.count(), 0);
        assert_eq!(histogram.quantile(0.5), Duration::ZERO);
        assert_eq!(histogram.mean(), Duration::ZERO);
    }

    /// Quantiles are what say whether a mean describes anything, so they have
    /// to track the distribution rather than its average.
    #[test]
    fn quantiles_follow_the_distribution() {
        let mut histogram = DurationHistogram::new();
        for _ in 0..99 {
            histogram.add(Duration::from_millis(1));
        }
        histogram.add(Duration::from_secs(10));

        assert_eq!(histogram.count(), 100);
        for (fraction, expected) in [(0.5, 1), (0.9, 1), (0.99, 1)] {
            let quantile = histogram.quantile(fraction);
            assert!(
                (Duration::from_micros(800)..=Duration::from_millis(expected)).contains(&quantile),
                "p{} of a millisecond distribution read as {quantile:?}",
                fraction * 100.0
            );
        }
        assert_eq!(histogram.max, Duration::from_secs(10));
        // The one outlier is a hundredth of the samples but 99% of the total,
        // which is exactly the case a mean alone hides.
        assert!(histogram.mean() > Duration::from_millis(100));
    }

    #[test]
    fn the_metadata_lists_only_the_buckets_that_hold_something() {
        let mut histogram = DurationHistogram::new();
        histogram.add(Duration::from_millis(1));
        histogram.add(Duration::from_millis(1));
        histogram.add(Duration::from_secs(1));

        let MetaItem::Map(items) = histogram.metadata() else {
            panic!("a histogram reads as a map");
        };
        let Some(MetaItem::Array(buckets)) = items.get("buckets") else {
            panic!("a histogram lists its buckets");
        };
        assert_eq!(buckets.len(), 2);

        let mut previous = Duration::ZERO;
        let mut samples = 0;
        for bucket in buckets {
            let MetaItem::Map(bucket) = bucket else {
                panic!("a bucket reads as a map");
            };
            let (Some(MetaItem::Duration(ge)), Some(MetaItem::Count(count))) =
                (bucket.get("ge"), bucket.get("samples"))
            else {
                panic!("a bucket carries a bound and a count");
            };
            assert!(*ge > previous, "buckets are listed out of order");
            previous = *ge;
            samples += count;
        }
        assert_eq!(samples, 3);
    }
}
