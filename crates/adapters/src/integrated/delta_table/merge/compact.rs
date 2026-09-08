//! Compaction driven by the connector, for tables nothing else maintains.
//!
//! Merge mode marks a superseded row deleted rather than rewriting its file, so old versions
//! stay in the table until something rewrites that file. Compacting is normally the table
//! administrator's job, so `optimize_interval_secs` is off by default; it is there for tables
//! where Feldera is the only writer.
//!
//! Compaction runs in the background because its cost follows the size of the table, not the
//! size of the flush, so running it inline would stall output. A flush that loses the race is
//! redone against the new files, which merge mode must handle anyway to survive an
//! administrator's own `OPTIMIZE`.
//!
//! delta-rs plans `OPTIMIZE` on file size alone: it skips a file at the target size, and
//! drops a bin holding one file. Either way a mostly superseded file keeps its rows.
//! [`super::rewrite`] runs after the packing and rewrites those on their own.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dbsp::circuit::tokio::TOKIO;
use deltalake::open_table_with_storage_options;
use deltalake::table::builder::ensure_table_uri;
use feldera_types::transport::delta_table::DeltaTableWriterConfig;
use parking_lot::Mutex;
use tracing::{info, warn};

use super::metrics::MergeMetrics;
use super::rewrite::{ReclaimMetrics, reclaim_superseded_rows};

/// Whether a compaction is in flight, and when the next one may start.
struct Schedule {
    running: bool,
    next_run: Instant,
}

/// Runs `OPTIMIZE` on the target table, one at a time and no more often than asked.
pub struct Compactor {
    uri: String,
    endpoint_name: String,
    storage_options: HashMap<String, String>,
    interval: Duration,
    schedule: Arc<Mutex<Schedule>>,
    metrics: Arc<MergeMetrics>,
}

impl Compactor {
    /// Build a compactor, or `None` when the connector was not asked to compact.
    pub fn new(
        config: &DeltaTableWriterConfig,
        endpoint_name: &str,
        metrics: Arc<MergeMetrics>,
    ) -> Option<Self> {
        Self::starting_at(config, endpoint_name, metrics, Instant::now())
    }

    /// Split from [`Self::new`] so a test can pin the instant the cadence counts from.
    fn starting_at(
        config: &DeltaTableWriterConfig,
        endpoint_name: &str,
        metrics: Arc<MergeMetrics>,
        now: Instant,
    ) -> Option<Self> {
        let interval = Duration::from_secs(config.optimize_interval_secs?);
        Some(Self {
            uri: config.uri.clone(),
            endpoint_name: endpoint_name.to_string(),
            storage_options: config.object_store_config.clone(),
            interval,
            schedule: Arc::new(Mutex::new(Schedule {
                running: false,
                // One interval away, not immediately: naming a cadence asks for compaction
                // on that cadence, not a rewrite of the whole table at startup.
                next_run: now + interval,
            })),
            metrics,
        })
    }

    /// Take the next compaction slot if one is due and none is in flight.
    ///
    /// Split from [`Self::maybe_start`] so a test can drive the clock instead of waiting.
    fn claim(&self, now: Instant) -> bool {
        let mut schedule = self.schedule.lock();
        if schedule.running || now < schedule.next_run {
            return false;
        }
        schedule.running = true;
        // Set here as well as on release, so a compaction that fails immediately does not
        // retry on the very next flush.
        schedule.next_run = now + self.interval;
        true
    }

    /// Give the slot back, one interval from now.
    fn release(schedule: &Mutex<Schedule>, interval: Duration, now: Instant) {
        let mut schedule = schedule.lock();
        schedule.running = false;
        schedule.next_run = now + interval;
    }

    /// Start a compaction if one is due. Called after every successful flush, so the common
    /// path is one lock and one clock read; never blocks on the compaction itself.
    pub fn maybe_start(&self) {
        if !self.claim(Instant::now()) {
            return;
        }

        let schedule = self.schedule.clone();
        let uri = self.uri.clone();
        let endpoint_name = self.endpoint_name.clone();
        let storage_options = self.storage_options.clone();
        let interval = self.interval;
        let metrics = self.metrics.clone();

        TOKIO.spawn(async move {
            // A guard, not a call at the end: delta-rs and DataFusion can panic, and a
            // claimed slot never given back would disable compaction for the whole run.
            let _slot = Slot { schedule, interval };
            let started = Instant::now();
            match compact(&uri, storage_options, interval).await {
                Ok(outcome) => {
                    metrics.record_compaction(
                        outcome.reclaim.rows_reclaimed,
                        outcome.reclaim.stopped_early,
                    );
                    info!(
                        "delta_table {endpoint_name}: compacted '{uri}' in {:.1}s, \
                     {} file(s) rewritten into {}, and reclaimed {} superseded row(s) from \
                     {} mostly-superseded file(s){}",
                        started.elapsed().as_secs_f64(),
                        outcome.files_removed,
                        outcome.files_added,
                        outcome.reclaim.rows_reclaimed,
                        outcome.reclaim.files_rewritten,
                        if outcome.reclaim.stopped_early {
                            ". More files are still mostly superseded; the next run continues"
                        } else {
                            ""
                        },
                    )
                }
                // A failed compaction leaves a valid table, so warn rather than fail the
                // endpoint: the connector keeps writing.
                Err(e) => {
                    metrics.record_compaction_failure();
                    warn!(
                        "delta_table {endpoint_name}: unable to compact '{uri}': {e}. Whatever \
                         the run had already compacted is committed and the table is intact; \
                         the connector retries in {}s. Compact the table yourself if this \
                         persists.",
                        interval.as_secs(),
                    )
                }
            }
        });
    }
}

/// Holds the compaction slot for as long as a compaction is in flight.
struct Slot {
    schedule: Arc<Mutex<Schedule>>,
    interval: Duration,
}

impl Drop for Slot {
    fn drop(&mut self) {
        Compactor::release(&self.schedule, self.interval, Instant::now());
    }
}

/// What one compaction did, for the log line.
struct Outcome {
    files_added: u64,
    files_removed: u64,
    reclaim: ReclaimMetrics,
}

/// How many files delta-rs may rewrite at once during the bin-packing.
///
/// It defaults to one task per core. Those are CPU-bound parquet rewrites on [`TOKIO`], the
/// shared runtime the writer blocks on to flush, so the default would stall flushes.
const MAX_COMPACTION_TASKS: usize = 2;

/// How long the bin-packing may accumulate rewrites before committing them.
///
/// delta-rs commits once at the end by default, so a long run holds every action in memory
/// and one conflicting write discards all of it.
const COMMIT_INTERVAL: Duration = Duration::from_secs(60);

/// Open the table and compact it, within `budget`. No target file size is set, so delta-rs
/// takes the table's own `delta.targetFileSize` rather than the connector second-guessing it.
///
/// Only the reclamation half can stop early. delta-rs offers no deadline for the
/// bin-packing, so a run that spends the whole budget there reclaims nothing this time.
async fn compact(
    uri: &str,
    storage_options: HashMap<String, String>,
    budget: Duration,
) -> anyhow::Result<Outcome> {
    let url = ensure_table_uri(uri)?;
    let started = Instant::now();

    // Opened fresh, not shared with the writer: the writer's snapshot only advances with its
    // own commits, and this must see everything committed now.
    let table = open_table_with_storage_options(url, storage_options).await?;
    let (mut table, metrics) = table
        .optimize()
        .with_max_concurrent_tasks(MAX_COMPACTION_TASKS)
        .with_min_commit_interval(COMMIT_INTERVAL)
        .await?;

    // After the bin-packing, never before: a packed file loses its vector, so packing first
    // drops files from this pass. The other order would rewrite a file here and pack it again.
    // What the packing spent comes off the budget, so the two together stay inside it.
    let reclaim =
        reclaim_superseded_rows(&mut table, budget.saturating_sub(started.elapsed())).await?;

    Ok(Outcome {
        files_added: metrics.num_files_added,
        files_removed: metrics.num_files_removed,
        reclaim,
    })
}

#[cfg(test)]
mod test {
    use feldera_types::transport::delta_table::{
        DeltaTableUpdateMode, DeltaTableWriteMode, DeltaTableWriterConfig, DeltaVariantEncoding,
    };
    use tempfile::TempDir;

    use super::*;

    fn config_with(uri: &str, interval: Option<u64>) -> DeltaTableWriterConfig {
        DeltaTableWriterConfig {
            uri: uri.to_string(),
            mode: DeltaTableWriteMode::Append,
            variant_encoding: DeltaVariantEncoding::default(),
            update_mode: DeltaTableUpdateMode::Merge,
            lookup_chunk_bytes: 1 << 20,
            max_concurrent_probes: 4,
            checkpoint_interval: None,
            log_retention_duration: None,
            enable_expired_log_cleanup: None,
            max_retries: Some(0),
            threads: Some(1),
            optimize_interval_secs: interval,
            object_store_config: Default::default(),
        }
    }

    /// No interval means the connector must not compact at all.
    #[test]
    fn compaction_is_off_unless_asked_for() {
        assert!(
            Compactor::new(&config_with("memory://", None), "e", MergeMetrics::new()).is_none()
        );
        assert!(
            Compactor::new(
                &config_with("memory://", Some(60)),
                "e",
                MergeMetrics::new()
            )
            .is_some()
        );
    }

    /// The first compaction waits a full interval rather than firing on the first flush.
    ///
    /// A pipeline adopting a large table would otherwise rewrite it at startup, which is not
    /// what naming a cadence asks for.
    #[test]
    fn the_first_compaction_waits_one_interval() {
        let start = Instant::now();
        let compactor = Compactor::starting_at(
            &config_with("memory://", Some(60)),
            "e",
            MergeMetrics::new(),
            start,
        )
        .unwrap();

        assert!(!compactor.claim(start), "must not compact at startup");
        assert!(
            !compactor.claim(start + Duration::from_secs(59)),
            "must not compact before the interval elapses"
        );
        assert!(
            compactor.claim(start + Duration::from_secs(60)),
            "must compact once the interval has elapsed"
        );
    }

    /// One compaction at a time, and a full interval after the last one finishes.
    #[test]
    fn one_compaction_at_a_time() {
        let compactor = Compactor::new(
            &config_with("memory://", Some(60)),
            "e",
            MergeMetrics::new(),
        )
        .unwrap();
        let start = Instant::now();
        let due = start + Duration::from_secs(60);
        assert!(compactor.claim(due));

        // While it runs, no second compaction starts however long it takes.
        assert!(
            !compactor.claim(due + Duration::from_secs(600)),
            "a compaction must not start while one is in flight"
        );

        // The gap is measured from the end, so a compaction that outran its interval does
        // not immediately trigger the next one.
        let finished = due + Duration::from_secs(600);
        Compactor::release(&compactor.schedule, compactor.interval, finished);
        assert!(
            !compactor.claim(finished + Duration::from_secs(59)),
            "the interval must be measured from when the last compaction finished"
        );
        assert!(compactor.claim(finished + Duration::from_secs(60)));
    }

    /// A compaction that panics must still give the slot back.
    ///
    /// delta-rs and DataFusion are not panic-free, and a slot never released would leave
    /// `optimize_interval_secs` silently ignored for the life of the process.
    #[test]
    fn a_panicking_compaction_releases_the_slot() {
        let compactor = Compactor::new(
            &config_with("memory://", Some(60)),
            "e",
            MergeMetrics::new(),
        )
        .unwrap();
        let due = Instant::now() + Duration::from_secs(60);
        assert!(compactor.claim(due));

        let slot = Slot {
            schedule: compactor.schedule.clone(),
            interval: compactor.interval,
        };
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _slot = slot;
            panic!("compaction blew up");
        }));
        std::panic::set_hook(previous);

        assert!(unwound.is_err(), "the fixture did not panic");
        assert!(
            !compactor.schedule.lock().running,
            "the slot must be free again after a panic"
        );
    }

    /// OPTIMIZE must apply the deletion vectors it rewrites. Every superseded row is still
    /// in a data file and dead only by its vector, so bin-packing without applying them would
    /// bring them all back.
    #[tokio::test]
    async fn compacting_applies_deletion_vectors() {
        use super::super::test::{
            append_ids, deletion_vector_files, fixture_table, live_ids, tombstone_ids,
        };

        let dir = TempDir::new().unwrap();
        let table = fixture_table(&dir, &[1, 2, 3], true).await;
        // Two files, so OPTIMIZE has something to bin-pack.
        let table = append_ids(table, &[4, 5, 6]).await;
        let table = tombstone_ids(table, &[2, 5]).await;

        // Otherwise the test would pass on a table with no vector to apply.
        assert_eq!(
            deletion_vector_files(&dir).len(),
            1,
            "the fixture left no deletion vector for OPTIMIZE to apply"
        );
        assert_eq!(live_ids(&table).await, vec![1, 3, 4, 6]);

        let uri = dir.path().to_str().unwrap();
        let outcome = compact(uri, Default::default(), Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(outcome.files_removed, 2, "both files should bin-pack");
        assert_eq!(outcome.files_added, 1);

        let compacted =
            open_table_with_storage_options(ensure_table_uri(uri).unwrap(), Default::default())
                .await
                .unwrap();
        assert_eq!(
            live_ids(&compacted).await,
            vec![1, 3, 4, 6],
            "compaction resurrected the superseded rows"
        );
    }
}
