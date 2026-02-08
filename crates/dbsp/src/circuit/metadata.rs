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
pub const EXCHANGE_WAIT_TIME_SECONDS: MetricId =
    MetricId(Cow::Borrowed("exchange_wait_time_seconds"));
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
pub const BLOOM_FILTER_BITS_PER_KEY: MetricId =
    MetricId(Cow::Borrowed("bloom_filter_bits_per_key"));
pub const BLOOM_FILTER_HITS_COUNT: MetricId = MetricId(Cow::Borrowed("bloom_filter_hits_count"));
pub const BLOOM_FILTER_MISSES_COUNT: MetricId =
    MetricId(Cow::Borrowed("bloom_filter_misses_count"));
pub const BLOOM_FILTER_HIT_RATE_PERCENT: MetricId =
    MetricId(Cow::Borrowed("bloom_filter_hit_rate_percent"));
pub const BLOOM_FILTER_SIZE_BYTES: MetricId = MetricId(Cow::Borrowed("bloom_filter_size_bytes"));
pub const SPINE_BATCHES_COUNT: MetricId = MetricId(Cow::Borrowed("spine_batches_count"));
pub const SPINE_STORAGE_SIZE_BYTES: MetricId = MetricId(Cow::Borrowed("spine_storage_size_bytes"));
pub const MERGING_SIZE_BYTES: MetricId = MetricId(Cow::Borrowed("merging_size_bytes"));
pub const MERGE_REDUCTION_PERCENT: MetricId = MetricId(Cow::Borrowed("merge_reduction_percent"));
pub const MERGE_BACKPRESSURE_WAIT_TIME_SECONDS: MetricId =
    MetricId(Cow::Borrowed("merge_backpressure_wait_time_seconds"));
pub const INVOCATIONS_COUNT: MetricId = MetricId(Cow::Borrowed("invocations_count"));
pub const RUNTIME_SECONDS: MetricId = MetricId(Cow::Borrowed("runtime_seconds"));
pub const RUNTIME_PERCENT: MetricId = MetricId(Cow::Borrowed("runtime_percent"));
pub const CIRCUIT_WAIT_TIME_SECONDS: MetricId =
    MetricId(Cow::Borrowed("circuit_wait_time_seconds"));
pub const STEPS_COUNT: MetricId = MetricId(Cow::Borrowed("steps_count"));
pub const CIRCUIT_RUNTIME_SECONDS: MetricId = MetricId(Cow::Borrowed("circuit_runtime_seconds"));
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

pub const CIRCUIT_METRICS: [CircuitMetric; 61] = [
    // State
    CircuitMetric {
        name: USED_MEMORY_BYTES,
        category: CircuitMetricCategory::State,
        advanced: false,
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
        advanced: false,
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
        name: BLOOM_FILTER_BITS_PER_KEY,
        category: CircuitMetricCategory::State,
        advanced: false,
        description: "Average number of bits per key in the Bloom filter.",
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
        advanced: false,
        description: "The number of hits across all Bloom filters. The hits are summed across the Bloom filters for all batches in the spine.",
    },
    CircuitMetric {
        name: BLOOM_FILTER_MISSES_COUNT,
        category: CircuitMetricCategory::State,
        advanced: false,
        description: "The number of misses across all Bloom filters. The misses are summed across the Bloom filters for all batches in the spine.",
    },
    CircuitMetric {
        name: BLOOM_FILTER_HIT_RATE_PERCENT,
        category: CircuitMetricCategory::State,
        advanced: false,
        description: "Hit rate of the Bloom filter.",
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
        advanced: false,
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
        name: CIRCUIT_WAIT_TIME_SECONDS,
        category: CircuitMetricCategory::Time,
        advanced: false,
        description: "Time the circuit scheduler spent waiting for an operator to become ready.",
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
        description: "Total time spent evaluating the circuit, including operators runtime and circuit wait time ('circuit_wait_time_seconds').",
    },
    CircuitMetric {
        name: CIRCUIT_IDLE_TIME_SECONDS,
        category: CircuitMetricCategory::Time,
        advanced: false,
        description: "Total time spent between circuit invocations, waiting for new data from input connectors or for output connector queues to clear out.",
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
                MetaItem::Count(if self.cnt == 0 {
                    0
                } else {
                    self.total / self.cnt
                }),
            ),
            (
                Cow::Borrowed("total_records_count"),
                MetaItem::Count(self.total),
            ),
        ]))
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
