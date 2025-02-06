//! Storage metrics.
//!
//! The constants defined in this module are the names of metrics that the
//! backends maintain via [`metrics`] crate interfaces.

use crate::circuit::GlobalNodeId;
use crate::Runtime;
use ::metrics::{describe_counter, describe_gauge, describe_histogram, Unit as MetricUnit};

/// Total number of files created.
pub const FILES_CREATED: &str = "disk.total_files_created";

/// Total number of files deleted.
pub const FILES_DELETED: &str = "disk.total_files_deleted";

/// Total number of successful disk writes.
pub const WRITES_SUCCESS: &str = "disk.total_writes_success";

/// Total number of failed disk writes.
pub const WRITES_FAILED: &str = "disk.total_writes_failed";

/// Total number of successful disk reads.
pub const READS_SUCCESS: &str = "disk.total_reads_success";

/// Total number of failed disk reads.
pub const READS_FAILED: &str = "disk.total_reads_failed";

/// Total number of bytes successfully written.
pub const TOTAL_BYTES_WRITTEN: &str = "disk.total_bytes_written";

/// Total number of bytes successfully read.
pub const TOTAL_BYTES_READ: &str = "disk.total_bytes_read";

/// Histogram of read latency.
pub const READ_LATENCY: &str = "disk.read_latency";

/// Histogram of write latency.
pub const WRITE_LATENCY: &str = "disk.write_latency";

/// Total number of buffer cache hits.
pub const BUFFER_CACHE_HIT: &str = "disk.buffer_cache_hit";

/// Total number of buffer cache misses.
pub const BUFFER_CACHE_MISS: &str = "disk.buffer_cache_miss";

/// Total number of compactions done by compaction thread.
pub const TOTAL_COMPACTIONS: &str = "file.compacted";

/// Sizes of batches that get compacted.
pub const COMPACTION_SIZE: &str = "file.compaction_size";

/// This counts the differences by subtracting the size of old batches
/// from a newly formed batch. It represents the total sum of entries
/// saved through compaction.
pub const COMPACTION_SIZE_SAVINGS: &str = "file.compaction_size";

/// Compaction duration for a single batch.
pub const COMPACTION_DURATION: &str = "file.compaction_duration";

/// Time in nanoseconds a worker was stalled waiting for more merges to complete.
pub const COMPACTION_STALL_TIME: &str = "file.compaction_stall_time";

/// Number of records dropped due to LATENESS annotations
pub const TOTAL_LATE_RECORDS: &str = "records.late";

/// Runtime in microseconds of an Operator evaluation
pub const OPERATOR_EVAL_DURATION: &str = "operator.runtime_micros";

/// Number of batches in the spines at each level.
pub const BATCHES_PER_LEVEL: &str = "spine.batches_per_level";

/// Number of pending merges in spines at each level.
pub const ONGOING_MERGES_PER_LEVEL: &str = "spine.ongoing_merges";

/// Creates the appropriate metric name for this metric.
/// As these metrics are DBSP related, they are prefixed with `dbsp_`.
fn metric_name(name: &str) -> String {
    format!("dbsp_{}", name)
}

/// A metric of type `Gauge`.
///
/// Gauge represents a single value that can go up or down over time, and always starts out
/// with an initial value of zero.
#[derive(Clone, size_of::SizeOf)]
pub(crate) struct Gauge(#[size_of(skip)] metrics::Gauge);

impl Gauge {
    /// Describe and initialize a new [`Gauge`].
    ///
    /// The following labels are set to this gauge by default:
    /// `worker` => Appropriate runtime worker thread index
    /// `gid` => The global node id of the current operator
    pub(crate) fn new(
        name: &str,
        description: Option<String>,
        unit: Option<&str>,
        gid: &GlobalNodeId,
        mut labels: Vec<(String, String)>,
    ) -> Self {
        labels.push(("worker".to_owned(), Runtime::worker_index().to_string()));
        labels.push(("gid".to_owned(), gid.metrics_id()));

        let unit: Option<metrics::Unit> = unit.and_then(metrics::Unit::from_string);
        let name = metric_name(name);
        let description = description.unwrap_or_default();

        match unit {
            Some(unit) => {
                describe_gauge!(name.clone(), unit, description);
            }
            None => {
                describe_gauge!(name.clone(), description);
            }
        };

        Self(metrics::gauge!(name, &labels[..]))
    }

    /// Set the value of this [`Gauge`] to `value`.
    pub(crate) fn set(&self, value: f64) {
        self.0.set(value)
    }

    /// Increment the value of this [`Gauge`] by `value`.
    #[allow(unused)]
    pub(crate) fn increment(&self, value: f64) {
        self.0.increment(value)
    }

    /// Decrement the value of this [`Gauge`] by `value`.
    #[allow(unused)]
    pub(crate) fn decrement(&self, value: f64) {
        self.0.decrement(value)
    }
}

/// Metric of type `Histogram`.
///
/// Histograms measure the distribution of values for a given set of measurements,
/// and start with no initial values.
#[derive(Clone, size_of::SizeOf)]
pub(crate) struct Histogram(#[size_of(skip)] metrics::Histogram);

impl Histogram {
    /// Describe and initialize a new [`Histogram`].
    ///
    /// The following labels are set to this histogram by default:
    /// `worker` => Appropriate runtime worker thread index
    /// `gid` => The global node id of the current operator
    #[allow(unused)]
    pub(crate) fn new(
        name: &str,
        description: Option<String>,
        unit: Option<&str>,
        gid: &GlobalNodeId,
        mut labels: Vec<(String, String)>,
    ) -> Self {
        labels.push(("worker".to_owned(), Runtime::worker_index().to_string()));
        labels.push(("gid".to_owned(), gid.metrics_id()));

        let unit: Option<metrics::Unit> = unit.and_then(metrics::Unit::from_string);
        let name = metric_name(name);
        let description = description.unwrap_or_default();

        match unit {
            Some(unit) => {
                describe_histogram!(name.clone(), unit, description);
            }
            None => {
                describe_histogram!(name.clone(), description);
            }
        };

        Self(metrics::histogram!(name, &labels[..]))
    }

    #[allow(unused)]
    pub(crate) fn record(&self, value: f64) {
        self.0.record(value)
    }
}

/// Metric of type `Counter`.
///
/// Counters represent a single monotonic value, which means the value can only be incremented,
/// not decremented, and always starts out with an initial value of zero.
#[derive(Clone, size_of::SizeOf)]
pub(crate) struct Counter(#[size_of(skip)] metrics::Counter);

impl Counter {
    /// Describe and initialize a new [`Counter`].
    ///
    /// The following labels are set to this counter by default:
    /// `worker` => Appropriate runtime worker thread index
    /// `gid` => The global node id of the current operator
    #[allow(unused)]
    pub(crate) fn new(
        name: &str,
        description: Option<String>,
        unit: Option<&str>,
        gid: &GlobalNodeId,
        mut labels: Vec<(String, String)>,
    ) -> Self {
        labels.push(("worker".to_owned(), Runtime::worker_index().to_string()));
        labels.push(("gid".to_owned(), gid.metrics_id()));

        let unit: Option<metrics::Unit> = unit.and_then(metrics::Unit::from_string);
        let name = metric_name(name);
        let description = description.unwrap_or_default();

        match unit {
            Some(unit) => {
                describe_counter!(name.clone(), unit, description);
            }
            None => {
                describe_counter!(name.clone(), description);
            }
        };

        Self(metrics::counter!(name, &labels[..]))
    }

    /// Increments this [`Counter`] by `value`.
    #[allow(unused)]
    pub(crate) fn increment(&self, value: u64) {
        self.0.increment(value)
    }
}

/// Adds descriptions for the metrics we expose.
pub(crate) fn describe_metrics() {
    // Storage backend metrics.
    describe_counter!(FILES_CREATED, "total number of files created");
    describe_counter!(FILES_DELETED, "total number of files deleted");
    describe_counter!(WRITES_SUCCESS, "total number of disk writes");
    describe_counter!(WRITES_FAILED, "total number of failed writes");
    describe_counter!(READS_SUCCESS, "total number of disk reads");
    describe_counter!(READS_FAILED, "total number of failed reads");

    describe_counter!(
        TOTAL_BYTES_WRITTEN,
        MetricUnit::Bytes,
        "total number of bytes written to disk"
    );
    describe_counter!(
        TOTAL_BYTES_READ,
        MetricUnit::Bytes,
        "total number of bytes read from disk"
    );

    describe_histogram!(READ_LATENCY, MetricUnit::Seconds, "Read request latency");
    describe_histogram!(WRITE_LATENCY, MetricUnit::Seconds, "Write request latency");
    describe_histogram!(
        OPERATOR_EVAL_DURATION,
        MetricUnit::Microseconds,
        "evaluation duration of an operator"
    );

    // Buffer cache metrics.
    describe_counter!(BUFFER_CACHE_HIT, "total number of buffer cache hits");
    describe_counter!(BUFFER_CACHE_MISS, "total number of buffer cache misses");

    // Compactor metrics.
    describe_counter!(TOTAL_COMPACTIONS, "total number of compactions");
    describe_histogram!(
        COMPACTION_SIZE,
        MetricUnit::Count,
        "Batch sizes encountered in compaction"
    );
    describe_counter!(
        COMPACTION_SIZE_SAVINGS,
        MetricUnit::Count,
        "Eliminated entries in batches through merging"
    );
    describe_histogram!(
        COMPACTION_DURATION,
        MetricUnit::Seconds,
        "Time to compact batch"
    );

    describe_counter!(
        COMPACTION_STALL_TIME,
        MetricUnit::Seconds,
        "Time a worker was stalled waiting for more merges to complete"
    );

    describe_counter!(
        TOTAL_LATE_RECORDS,
        "number of records that were dropped due to LATENESS"
    );
}
