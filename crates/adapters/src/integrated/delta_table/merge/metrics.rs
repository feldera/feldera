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
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use feldera_adapterlib::metrics::{ConnectorMetrics, ValueType};
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
    last_warning: Mutex<Option<Instant>>,
}

impl MergeMetrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
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
    pub fn record_tombstone_ratio(&self, live: u64, superseded: u64, endpoint: &str, uri: &str) {
        let total = live + superseded;
        if total == 0 {
            return;
        }
        let ratio = superseded as f64 / total as f64;
        self.tombstone_ratio_permille
            .store((ratio * 1000.0) as u64, Ordering::Relaxed);

        if ratio < TOMBSTONE_WARN_RATIO {
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
             ({superseded} superseded, {live} live). Merge mode supersedes a row without \
             rewriting the file that holds it, so reads stay proportional to the total until \
             a compaction reclaims them. Run OPTIMIZE on the table, on a schedule, or set \
             the connector's 'optimize_interval_secs' if Feldera is its only writer.",
            ratio * 100.0
        );
    }
}

impl ConnectorMetrics for MergeMetrics {
    fn metrics(&self) -> Vec<(&'static str, &'static str, ValueType, f64)> {
        let get = |counter: &AtomicU64| counter.load(Ordering::Relaxed) as f64;
        vec![
            (
                "delta_merge_rows_appended_total",
                "Row versions appended to the target Delta table: inserts plus the new side \
                 of updates.",
                ValueType::Counter,
                get(&self.rows_appended),
            ),
            (
                "delta_merge_rows_superseded_total",
                "Rows marked deleted by a deletion vector: deletes plus the old side of \
                 updates.",
                ValueType::Counter,
                get(&self.rows_tombstoned),
            ),
            (
                "delta_merge_files_appended_total",
                "Data files added to the target table.",
                ValueType::Counter,
                get(&self.files_appended),
            ),
            (
                "delta_merge_files_dropped_total",
                "Data files removed outright because every row in them was superseded.",
                ValueType::Counter,
                get(&self.files_dropped),
            ),
            (
                "delta_merge_keys_probed_total",
                "Changed keys whose row had to be located in the target table. Below the \
                 changed-key count by however many inserts skipped the lookup.",
                ValueType::Counter,
                get(&self.keys_probed),
            ),
            (
                "delta_merge_keys_not_found_total",
                "Keys the lookup did not find in the target table. A delete of an absent row \
                 is a no-op, so this is not an error, but a sustained rate means the table \
                 has diverged from the view.",
                ValueType::Counter,
                get(&self.keys_not_found),
            ),
            (
                "delta_merge_lookup_passes_total",
                "Lookup passes run. Above one per flush only when the key set exceeded \
                 `lookup_chunk_bytes`.",
                ValueType::Counter,
                get(&self.lookup_passes),
            ),
            (
                "delta_merge_probe_files_scanned_total",
                "Data files whose footer the lookup opened.",
                ValueType::Counter,
                get(&self.probe_files_scanned),
            ),
            (
                "delta_merge_probe_files_pruned_total",
                "Data files the lookup skipped without opening, on the statistics in the \
                 Delta log or on their partition.",
                ValueType::Counter,
                get(&self.probe_files_pruned),
            ),
            (
                "delta_merge_probe_row_groups_scanned_total",
                "Row groups whose key columns the lookup read.",
                ValueType::Counter,
                get(&self.probe_row_groups_scanned),
            ),
            (
                "delta_merge_probe_row_groups_pruned_total",
                "Row groups the lookup skipped on their footer statistics.",
                ValueType::Counter,
                get(&self.probe_row_groups_pruned),
            ),
            (
                "delta_merge_bytes_written_total",
                "Bytes written to object storage: new data files plus deletion vectors.",
                ValueType::Counter,
                get(&self.bytes_written),
            ),
            (
                "delta_merge_tombstone_ratio_permille",
                "Superseded rows per thousand rows in the target table, as of the last \
                 flush. The signal for when to run OPTIMIZE.",
                ValueType::Gauge,
                get(&self.tombstone_ratio_permille),
            ),
        ]
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

        assert_eq!(value(&metrics, "delta_merge_rows_appended_total"), 14.0);
        assert_eq!(value(&metrics, "delta_merge_rows_superseded_total"), 4.0);
        assert_eq!(value(&metrics, "delta_merge_probe_files_pruned_total"), 7.0);
        assert_eq!(
            value(&metrics, "delta_merge_probe_files_scanned_total"),
            4.0
        );
    }

    #[test]
    fn tombstone_ratio_tracks_the_table() {
        let metrics = MergeMetrics::new();
        metrics.record_tombstone_ratio(900, 100, "e", "uri");
        assert_eq!(
            value(&metrics, "delta_merge_tombstone_ratio_permille"),
            100.0
        );

        metrics.record_tombstone_ratio(500, 500, "e", "uri");
        assert_eq!(
            value(&metrics, "delta_merge_tombstone_ratio_permille"),
            500.0
        );
    }

    /// An empty table has no ratio to report, and must not divide by zero.
    #[test]
    fn an_empty_table_reports_no_ratio() {
        let metrics = MergeMetrics::new();
        metrics.record_tombstone_ratio(0, 0, "e", "uri");
        assert_eq!(value(&metrics, "delta_merge_tombstone_ratio_permille"), 0.0);
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
