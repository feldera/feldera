//! Storage metrics.
//!
//! The constants defined in this module are the names of metrics that the
//! backends maintain via [`metrics`] crate interfaces.

use crate::circuit::GlobalNodeId;
use crate::Runtime;
use ::metrics::{
    describe_counter, describe_histogram, SharedString as MetricString, Unit as MetricUnit,
};
use lazy_static::lazy_static;
use metrics::{describe_gauge, Counter, Gauge, Histogram, IntoLabels, KeyName};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

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

/// Time a worker was stalled waiting for more merges to complete.
pub const COMPACTION_STALL_TIME: &str = "file.compaction_stall_time";

/// Number of records dropped due to LATENESS annotations
pub const TOTAL_LATE_RECORDS: &str = "records.late";

/// Runtime in microseconds of an Operator evaluation
pub const OPERATOR_EVAL_DURATION: &str = "operator.runtime_micros";

/// Holds a map of the [`DBSPMetric`] and their equivalent [`metrics`] type.
///
/// [`RwLock`]s here should be very efficient, as reading is the most common operation.
/// Writing is only necessary while adding a new metric.
///
/// Updating the inner type in the map doesn't require mutable access due to the interior
/// mutability of the [`Counter`], [`Gauge`] and [`Histogram`] types.
#[allow(unused)]
#[derive(Default)]
struct MetricsRecorder {
    counters: RwLock<HashMap<DBSPMetric, Counter>>,
    gauge: RwLock<HashMap<DBSPMetric, Gauge>>,
    histogram: RwLock<HashMap<DBSPMetric, Histogram>>,
}

lazy_static! {
    /// Global recorder for DBSP related metrics.
    static ref METRICS_RECORDER: Arc<MetricsRecorder> = Arc::new(MetricsRecorder::new());
}

impl MetricsRecorder {
    fn new() -> Self {
        Self::default()
    }

    /// Registers a new [`Counter`] and saves it to the map.
    #[allow(unused)]
    fn register_counter(
        &self,
        metric: DBSPMetric,
        unit: Option<MetricUnit>,
        description: Option<String>,
    ) {
        {
            let c = self.counters.read().unwrap();
            if c.contains_key(&metric) {
                return;
            }
        }

        metric.describe(MetricType::Counter, unit, description);

        {
            let mut c = self.counters.write().unwrap();
            let counter = metrics::counter!(metric.name(), metric.labels.into_labels());
            c.insert(metric, counter);
        }
    }

    /// Increments the [`Counter`] for the given `metric` by `value`.
    #[allow(unused)]
    fn increment_counter(&self, metric: DBSPMetric, value: u64) {
        let c = self.counters.read().unwrap();
        if let Some(counter) = c.get(&metric) {
            counter.increment(value);
        }
    }

    /// Registers a new [`Gauge`] and saves it to the map.
    fn register_gauge(
        &self,
        metric: DBSPMetric,
        unit: Option<MetricUnit>,
        description: Option<String>,
    ) {
        {
            let c = self.gauge.read().unwrap();
            if c.contains_key(&metric) {
                return;
            }
        }

        metric.describe(MetricType::Gauge, unit, description);

        {
            let mut c = self.gauge.write().unwrap();
            let gauge = metrics::gauge!(metric.name(), metric.labels.into_labels());
            c.insert(metric, gauge);
        }
    }

    /// Sets value of the [`Gauge`] for the given `metric` to `value`.
    fn set_gauge(&self, metric: DBSPMetric, value: f64) {
        let g = self.gauge.read().unwrap();
        if let Some(gauge) = g.get(&metric) {
            gauge.set(value);
        }
    }

    /// Registers a new [`Histogram`] and saves it to the map.
    fn register_histogram(
        &self,
        metric: DBSPMetric,
        unit: Option<MetricUnit>,
        description: Option<String>,
    ) {
        {
            let c = self.histogram.read().unwrap();
            if c.contains_key(&metric) {
                return;
            }
        }

        metric.describe(MetricType::Histogram, unit, description);

        {
            let mut c = self.histogram.write().unwrap();
            let h = metrics::histogram!(metric.name(), metric.labels.into_labels());
            c.insert(metric, h);
        }
    }

    /// Records this `value` to the [`Histogram`] for the given `metric`.
    fn record_histogram(&self, metric: DBSPMetric, value: f64) {
        let h = self.histogram.read().unwrap();
        if let Some(histogram) = h.get(&metric) {
            histogram.record(value);
        }
    }
}

enum MetricType {
    #[allow(unused)]
    Counter,
    Gauge,
    Histogram,
}

#[derive(Debug, Hash, PartialOrd, PartialEq, Eq, Clone)]
pub(crate) struct DBSPMetric {
    /// Name of the metric.
    key: KeyName,

    /// The global node ID of the operator.
    global_node_id: GlobalNodeId,

    /// The worker index of the operator.
    worker_index: usize,

    /// Optional key-value pairs that provide additional metadata about this
    /// metric.
    labels: Vec<(MetricString, MetricString)>,
}

impl DBSPMetric {
    fn new(
        key: KeyName,
        global_node_id: GlobalNodeId,
        labels: Vec<(MetricString, MetricString)>,
    ) -> Self {
        Self {
            key,
            global_node_id,
            worker_index: Runtime::worker_index(),
            labels,
        }
    }

    fn describe(
        &self,
        metric_type: MetricType,
        unit: Option<MetricUnit>,
        description: Option<String>,
    ) {
        let description = description.unwrap_or_default();
        let key = self.key.clone();
        match metric_type {
            MetricType::Counter => {
                if let Some(unit) = unit {
                    describe_counter!(key, unit, description);
                } else {
                    describe_counter!(key, description);
                }
            }
            MetricType::Gauge => {
                if let Some(unit) = unit {
                    describe_gauge!(key, unit, description);
                } else {
                    describe_gauge!(key, description);
                }
            }
            MetricType::Histogram => {
                if let Some(unit) = unit {
                    describe_histogram!(key, unit, description);
                } else {
                    describe_histogram!(key, description);
                }
            }
        }
    }

    fn name(&self) -> String {
        format!("dbsp_{}", self.key.as_str(),)
    }
}

/// Sets the value for the [`Gauge`] to `value`.
///
/// The metric is named in the form: `dbsp_{name}`.
/// Automatically adds the worker index and global node id as labels.
pub(crate) fn gauge(
    global_id: GlobalNodeId,
    name: String,
    value: f64,
    mut labels: Vec<(MetricString, MetricString)>,
    unit: Option<MetricUnit>,
    description: Option<String>,
) {
    labels.push((
        "worker".to_string().into(),
        Runtime::worker_index().to_string().into(),
    ));
    labels.push((
        "global_id".to_string().into(),
        global_id.metrics_id().into(),
    ));

    let metric = DBSPMetric::new(name.into(), global_id, labels);
    METRICS_RECORDER.register_gauge(metric.clone(), unit, description);

    METRICS_RECORDER.set_gauge(metric, value);
}

/// Records the `value` in the [`Histogram`].
///
/// The metric is named in the form: `dbsp_{name}`.
/// Automatically adds the worker index and global node id as labels.
pub(crate) fn histogram(
    global_id: GlobalNodeId,
    name: String,
    value: f64,
    mut labels: Vec<(MetricString, MetricString)>,
    unit: Option<MetricUnit>,
    description: Option<String>,
) {
    labels.push((
        "worker".to_string().into(),
        Runtime::worker_index().to_string().into(),
    ));
    labels.push((
        "global_id".to_string().into(),
        global_id.metrics_id().into(),
    ));

    let metric = DBSPMetric::new(name.into(), global_id, labels);
    METRICS_RECORDER.register_histogram(metric.clone(), unit, description);

    METRICS_RECORDER.record_histogram(metric, value);
}

/// Increments the [`Counter`] by the `value`.
///
/// The metric is named in the form: `dbsp_{name}`.
/// Automatically adds worker index and global node id as labels.
#[allow(unused)]
pub(crate) fn counter(
    global_id: GlobalNodeId,
    name: String,
    value: u64,
    mut labels: Vec<(MetricString, MetricString)>,
    unit: Option<MetricUnit>,
    description: Option<String>,
) {
    labels.push((
        "worker".to_string().into(),
        Runtime::worker_index().to_string().into(),
    ));
    labels.push((
        "global_id".to_string().into(),
        global_id.metrics_id().into(),
    ));

    let metric = DBSPMetric::new(name.into(), global_id, labels);
    METRICS_RECORDER.register_counter(metric.clone(), unit, description);

    METRICS_RECORDER.increment_counter(metric, value);
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
