//! What merge mode reports, and the one thing it warns about.
//!
//! An operator watches `probe_files_pruned` against `probe_files_scanned` to see whether
//! pruning earns its keep; a scanned count that tracks the table's file count rather than the
//! change rate means the key set and the table's clustering disagree.
//!
//! A table administrator watches `tombstone_ratio`, since every update leaves a superseded row
//! behind and reads stay proportional to the total until an OPTIMIZE reclaims them. It is the
//! one condition here worth a log line, at most once an hour, naming the remedy.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use feldera_adapterlib::metrics::{ConnectorHistogram, ConnectorMetrics, ValueType};
use feldera_storage::histogram::ExponentialHistogram;
use parking_lot::Mutex;
use tracing::warn;

use super::flush::FlushMetrics;

/// Ratio of superseded to total rows above which the table needs compacting. Twenty percent
/// is where the cost of applying vectors starts to show, with time left to act.
const TOMBSTONE_WARN_RATIO: f64 = 0.2;

/// How often the compaction warning may repeat.
const WARN_INTERVAL: Duration = Duration::from_secs(3600);

/// Cumulative counters for one merge-mode connector.
#[derive(Debug, Default)]
pub struct MergeMetrics {
    rows_appended: AtomicU64,
    rows_tombstoned: AtomicU64,
    files_appended: AtomicU64,
    files_dropped: AtomicU64,
    keys_probed: AtomicU64,
    keys_not_found: AtomicU64,
    lookup_passes: AtomicU64,
    probe_files_scanned: AtomicU64,
    probe_files_pruned: AtomicU64,
    probe_row_groups_scanned: AtomicU64,
    probe_row_groups_pruned: AtomicU64,
    bytes_written: AtomicU64,
    /// Live rows and superseded rows in the table as of the last flush, scaled by 1000 so
    /// the ratio survives an integer counter.
    tombstone_ratio_permille: AtomicU64,
    /// Wall time of a whole flush. The counters say how much a lookup read; only this says
    /// how long the object store took to serve it.
    flush_latency: ExponentialHistogram,
    last_warning: Mutex<Option<Instant>>,
    /// The connector was asked to maintain the table itself.
    maintains_table: AtomicBool,
    /// Set when a maintenance run fails, so the warning it suppresses comes back.
    maintenance_failing: AtomicBool,
    compactions: AtomicU64,
    compaction_failures: AtomicU64,
    reclaimed_rows: AtomicU64,
    /// 1 while the last run left mostly-superseded files behind.
    reclaim_incomplete: AtomicU64,
}

impl MergeMetrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// The connector maintains this table itself, so the compaction advice below is moot.
    pub fn maintains_the_table(&self) {
        self.maintains_table.store(true, Ordering::Relaxed);
    }

    /// Fold in one maintenance run.
    pub fn record_compaction(&self, rows_reclaimed: u64, incomplete: bool) {
        self.compactions.fetch_add(1, Ordering::Relaxed);
        self.reclaimed_rows
            .fetch_add(rows_reclaimed, Ordering::Relaxed);
        self.reclaim_incomplete
            .store(incomplete as u64, Ordering::Relaxed);
        self.maintenance_failing.store(false, Ordering::Relaxed);
    }

    /// Record a maintenance run that failed. The table is intact, but nothing was reclaimed.
    pub fn record_compaction_failure(&self) {
        self.compaction_failures.fetch_add(1, Ordering::Relaxed);
        self.maintenance_failing.store(true, Ordering::Relaxed);
    }

    /// Record how long one flush took, whatever its outcome.
    pub fn record_flush_latency(&self, elapsed: Duration) {
        self.flush_latency.record_duration(elapsed);
    }

    /// Fold one flush's counts in.
    pub fn record(&self, flush: &FlushMetrics) {
        let add = |counter: &AtomicU64, value: u64| {
            counter.fetch_add(value, Ordering::Relaxed);
        };
        add(&self.rows_appended, flush.rows_appended);
        add(&self.rows_tombstoned, flush.dv.rows_tombstoned);
        add(&self.files_appended, flush.files_appended as u64);
        add(&self.files_dropped, flush.dv.files_dropped as u64);
        add(&self.keys_probed, flush.keys_probed);
        add(&self.keys_not_found, flush.probe.keys_not_found);
        add(&self.lookup_passes, flush.lookup_passes as u64);
        add(&self.probe_files_scanned, flush.probe.files_scanned as u64);
        add(&self.probe_files_pruned, flush.probe.files_pruned as u64);
        add(
            &self.probe_row_groups_scanned,
            flush.probe.row_groups_scanned as u64,
        );
        add(
            &self.probe_row_groups_pruned,
            flush.probe.row_groups_pruned as u64,
        );
        add(&self.bytes_written, flush.bytes_written);
    }

    /// Record the table's superseded-row ratio, warning when it crosses the threshold.
    ///
    /// `live` and `superseded` describe the whole table, as the snapshot says after a commit.
    /// The warning asks an operator to maintain the table, so it is suppressed while the
    /// connector does that itself -- and due again as soon as that stops working. The gauge
    /// is recorded either way.
    pub fn record_tombstone_ratio(&self, live: u64, superseded: u64, endpoint: &str, uri: &str) {
        let total = live + superseded;
        if total == 0 {
            return;
        }
        let ratio = superseded as f64 / total as f64;
        self.tombstone_ratio_permille
            .store((ratio * 1000.0) as u64, Ordering::Relaxed);

        let maintained = self.maintains_table.load(Ordering::Relaxed)
            && !self.maintenance_failing.load(Ordering::Relaxed);
        if maintained || ratio < TOMBSTONE_WARN_RATIO {
            return;
        }

        // The condition persists until somebody compacts, so a line per flush would bury
        // everything else.
        let mut last = self.last_warning.lock();
        if last.is_some_and(|at| at.elapsed() < WARN_INTERVAL) {
            return;
        }
        *last = Some(Instant::now());
        drop(last);

        warn!(
            "delta_table {endpoint}: {:.0}% of the rows in '{uri}' are superseded versions \
             ({superseded} superseded, {live} live), and reads still scan them. Set the \
             connector's 'optimize_interval_secs' to reclaim them automatically, or compact \
             the table yourself. See 'Compaction is required' in the Delta output connector \
             documentation.",
            ratio * 100.0
        );
    }
}

impl ConnectorMetrics for MergeMetrics {
    fn metrics(&self) -> Vec<(&'static str, &'static str, ValueType, f64)> {
        let get = |counter: &AtomicU64| counter.load(Ordering::Relaxed) as f64;
        vec![
            (
                "output_connector_delta_merge_rows_appended_total",
                "Row versions appended to the target Delta table: inserts plus the new side \
                 of updates.",
                ValueType::Counter,
                get(&self.rows_appended),
            ),
            (
                "output_connector_delta_merge_rows_superseded_total",
                "Rows marked deleted by a deletion vector: deletes plus the old side of \
                 updates.",
                ValueType::Counter,
                get(&self.rows_tombstoned),
            ),
            (
                "output_connector_delta_merge_files_appended_total",
                "Data files added to the target table.",
                ValueType::Counter,
                get(&self.files_appended),
            ),
            (
                "output_connector_delta_merge_files_dropped_total",
                "Data files removed outright because every row in them was superseded.",
                ValueType::Counter,
                get(&self.files_dropped),
            ),
            (
                "output_connector_delta_merge_keys_probed_total",
                "Changed keys whose row had to be located in the target table. Below the \
                 changed-key count by however many inserts skipped the lookup.",
                ValueType::Counter,
                get(&self.keys_probed),
            ),
            (
                "output_connector_delta_merge_keys_not_found_total",
                "Keys the lookup did not find in the target table. A delete of an absent row \
                 is a no-op, so this is not an error, but a sustained rate means the table \
                 has diverged from the view.",
                ValueType::Counter,
                get(&self.keys_not_found),
            ),
            (
                "output_connector_delta_merge_lookup_passes_total",
                "Lookup passes run. Above one per flush only when the key set exceeded \
                 `lookup_chunk_bytes`.",
                ValueType::Counter,
                get(&self.lookup_passes),
            ),
            (
                "output_connector_delta_merge_probe_files_scanned_total",
                "Data files whose footer the lookup opened.",
                ValueType::Counter,
                get(&self.probe_files_scanned),
            ),
            (
                "output_connector_delta_merge_probe_files_pruned_total",
                "Data files the lookup skipped without opening, on the statistics in the \
                 Delta log or on their partition.",
                ValueType::Counter,
                get(&self.probe_files_pruned),
            ),
            (
                "output_connector_delta_merge_probe_row_groups_scanned_total",
                "Row groups whose key columns the lookup read.",
                ValueType::Counter,
                get(&self.probe_row_groups_scanned),
            ),
            (
                "output_connector_delta_merge_probe_row_groups_pruned_total",
                "Row groups the lookup skipped on their footer statistics.",
                ValueType::Counter,
                get(&self.probe_row_groups_pruned),
            ),
            (
                "output_connector_delta_merge_bytes_written_total",
                "Bytes written to object storage: new data files plus deletion vectors.",
                ValueType::Counter,
                get(&self.bytes_written),
            ),
            (
                "output_connector_delta_merge_compactions_total",
                "Maintenance runs the connector completed, when \
                 'optimize_interval_secs' asks it to maintain the table.",
                ValueType::Counter,
                get(&self.compactions),
            ),
            (
                "output_connector_delta_merge_compaction_failures_total",
                "Maintenance runs that failed. The table is left intact and the next run \
                 retries, but nothing was reclaimed by this one.",
                ValueType::Counter,
                get(&self.compaction_failures),
            ),
            (
                "output_connector_delta_merge_reclaimed_rows_total",
                "Superseded rows the connector's own maintenance removed from storage.",
                ValueType::Counter,
                get(&self.reclaimed_rows),
            ),
            (
                "output_connector_delta_merge_reclaim_incomplete",
                "1 when the last maintenance run ran out of time with files still mostly \
                 superseded. Sustained, it means reclamation is falling behind the writes.",
                ValueType::Gauge,
                get(&self.reclaim_incomplete),
            ),
            (
                "output_connector_delta_merge_tombstone_ratio_permille",
                "Superseded rows per thousand rows in the target table, as of the last \
                 flush. The signal for when to run OPTIMIZE.",
                ValueType::Gauge,
                get(&self.tombstone_ratio_permille),
            ),
        ]
    }

    fn histograms(&self) -> Vec<ConnectorHistogram> {
        vec![ConnectorHistogram {
            name: "output_connector_delta_merge_flush_latency_microseconds",
            help: "Wall time of one merge-mode flush: the row lookup, the writes and the \
                   commit. The lookup dominates it, so this is where object-store latency \
                   shows up -- the scanned and pruned counters cannot.",
            snapshot: self.flush_latency.snapshot(),
        }]
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn flush(rows_appended: u64, superseded: u64, pruned: usize, scanned: usize) -> FlushMetrics {
        let mut metrics = FlushMetrics {
            rows_appended,
            ..Default::default()
        };
        metrics.dv.rows_tombstoned = superseded;
        metrics.probe.files_pruned = pruned;
        metrics.probe.files_scanned = scanned;
        metrics
    }

    fn value(metrics: &MergeMetrics, name: &str) -> f64 {
        metrics
            .metrics()
            .into_iter()
            .find(|(n, ..)| *n == name)
            .unwrap_or_else(|| panic!("no metric named {name}"))
            .3
    }

    #[test]
    fn counters_accumulate_across_flushes() {
        let metrics = MergeMetrics::new();
        metrics.record(&flush(10, 3, 5, 1));
        metrics.record(&flush(4, 1, 2, 3));

        assert_eq!(
            value(&metrics, "output_connector_delta_merge_rows_appended_total"),
            14.0
        );
        assert_eq!(
            value(
                &metrics,
                "output_connector_delta_merge_rows_superseded_total"
            ),
            4.0
        );
        assert_eq!(
            value(
                &metrics,
                "output_connector_delta_merge_probe_files_pruned_total"
            ),
            7.0
        );
        assert_eq!(
            value(
                &metrics,
                "output_connector_delta_merge_probe_files_scanned_total"
            ),
            4.0
        );
    }

    #[test]
    fn tombstone_ratio_tracks_the_table() {
        let metrics = MergeMetrics::new();
        metrics.record_tombstone_ratio(900, 100, "e", "uri");
        assert_eq!(
            value(
                &metrics,
                "output_connector_delta_merge_tombstone_ratio_permille"
            ),
            100.0
        );

        metrics.record_tombstone_ratio(500, 500, "e", "uri");
        assert_eq!(
            value(
                &metrics,
                "output_connector_delta_merge_tombstone_ratio_permille"
            ),
            500.0
        );
    }

    /// An empty table has no ratio. Without the guard it is NaN, and `NaN < threshold` is
    /// false, so the connector would advise compacting a table with nothing in it.
    #[test]
    fn an_empty_table_reports_no_ratio() {
        let metrics = MergeMetrics::new();
        metrics.record_tombstone_ratio(0, 0, "e", "uri");

        assert_eq!(
            value(
                &metrics,
                "output_connector_delta_merge_tombstone_ratio_permille"
            ),
            0.0
        );
        assert!(
            metrics.last_warning.lock().is_none(),
            "nothing to advise on"
        );
    }

    /// The latency histogram must be exported and must reflect what was recorded.
    #[test]
    fn flush_latency_is_exported() {
        let metrics = MergeMetrics::new();
        let recorded = |m: &MergeMetrics| -> u64 {
            m.histograms()[0]
                .snapshot
                .iter_buckets()
                .map(|b| b.count)
                .sum()
        };

        let empty = metrics.histograms();
        assert_eq!(empty.len(), 1);
        assert_eq!(recorded(&metrics), 0, "nothing recorded yet");

        metrics.record_flush_latency(Duration::from_millis(5));
        metrics.record_flush_latency(Duration::from_millis(50));

        let exported = metrics.histograms();
        assert_eq!(
            exported[0].name,
            "output_connector_delta_merge_flush_latency_microseconds"
        );
        assert_eq!(recorded(&metrics), 2);
        assert_eq!(
            exported[0].snapshot.sum(),
            55_000,
            "the histogram must be exported in the microseconds its name claims"
        );
    }

    /// A connector that maintains the table itself must not tell the operator to do it --
    /// and must start telling them again the moment its own maintenance stops working.
    #[test]
    fn the_warning_follows_whether_maintenance_works() {
        let metrics = MergeMetrics::new();
        metrics.maintains_the_table();
        metrics.record_tombstone_ratio(100, 900, "e", "uri");

        assert!(metrics.last_warning.lock().is_none(), "no advice is due");
        assert_eq!(
            value(
                &metrics,
                "output_connector_delta_merge_tombstone_ratio_permille"
            ),
            900.0,
            "the gauge is still recorded"
        );

        metrics.record_compaction_failure();
        metrics.record_tombstone_ratio(100, 900, "e", "uri");
        assert!(
            metrics.last_warning.lock().is_some(),
            "a table nothing is successfully maintaining needs the operator"
        );

        // A later run that works takes the advice back.
        *metrics.last_warning.lock() = None;
        metrics.record_compaction(4, false);
        metrics.record_tombstone_ratio(100, 900, "e", "uri");
        assert!(
            metrics.last_warning.lock().is_none(),
            "maintenance recovered"
        );
    }

    /// What the maintenance runs report, so falling behind is visible without reading logs.
    #[test]
    fn maintenance_runs_are_counted() {
        let metrics = MergeMetrics::new();
        assert_eq!(
            value(&metrics, "output_connector_delta_merge_compactions_total"),
            0.0
        );

        metrics.record_compaction(40, true);
        metrics.record_compaction_failure();

        assert_eq!(
            value(&metrics, "output_connector_delta_merge_compactions_total"),
            1.0
        );
        assert_eq!(
            value(
                &metrics,
                "output_connector_delta_merge_compaction_failures_total"
            ),
            1.0
        );
        assert_eq!(
            value(
                &metrics,
                "output_connector_delta_merge_reclaimed_rows_total"
            ),
            40.0
        );
        assert_eq!(
            value(&metrics, "output_connector_delta_merge_reclaim_incomplete"),
            1.0,
            "the run left work behind"
        );

        metrics.record_compaction(1, false);
        assert_eq!(
            value(&metrics, "output_connector_delta_merge_reclaim_incomplete"),
            0.0,
            "a run that finished clears it"
        );
        assert_eq!(
            value(
                &metrics,
                "output_connector_delta_merge_reclaimed_rows_total"
            ),
            41.0
        );
    }

    /// The warning is rate limited, so a persistent condition does not log per flush.
    #[test]
    fn the_compaction_warning_is_rate_limited() {
        let metrics = MergeMetrics::new();
        metrics.record_tombstone_ratio(1000, 10, "e", "uri");
        assert!(
            metrics.last_warning.lock().is_none(),
            "a ratio below the threshold must not warn"
        );

        metrics.record_tombstone_ratio(100, 900, "e", "uri");
        let first = *metrics.last_warning.lock();
        assert!(first.is_some(), "the first crossing must warn");

        metrics.record_tombstone_ratio(100, 900, "e", "uri");
        assert_eq!(
            *metrics.last_warning.lock(),
            first,
            "a second crossing within the interval must not warn again"
        );
    }
}
