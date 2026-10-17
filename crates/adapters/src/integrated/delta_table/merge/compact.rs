//! Compaction driven by the connector, for tables nothing else maintains (`auto_compact`).
//!
//! A step starts after a commit and the next flush waits for it, so the two never race.

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result as AnyResult;
use dbsp::circuit::tokio::TOKIO;
use deltalake::DeltaTable;
use feldera_types::transport::delta_table::DeltaTableWriterConfig;
use futures::FutureExt;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use super::metrics::MergeMetrics;
use super::rewrite::{
    Candidate, Plan, ReclaimMetrics, has_candidate, pack_files, plan_rewrites,
    reclaim_superseded_rows,
};

/// Pause after a failed step, so a lasting failure does not repeat after every flush.
const FAILURE_BACKOFF: Duration = Duration::from_secs(600);

/// A step past this is stopped, so a hung request cannot hold up every later flush.
const STEP_LIMIT: Duration = Duration::from_secs(600);

/// What one step after a commit does.
enum Work {
    Nothing,
    /// Pack these small files into one.
    Pack(Vec<Candidate>),
    /// Rewrite one mostly superseded file.
    Reclaim,
}

pub struct Compactor {
    report: Report,
    failed_at: Option<Instant>,
    /// Whether the last step packed, so a reclaim that is due goes next.
    packed_last: bool,
    /// [`STEP_LIMIT`], as a field so a test can shorten it.
    step_limit: Duration,
    /// The step the last commit started; yields when it failed, if it did.
    running: Option<JoinHandle<Option<Instant>>>,
}

impl Compactor {
    /// `None` unless the connector was asked to compact.
    pub fn new(
        config: &DeltaTableWriterConfig,
        endpoint_name: &str,
        metrics: Arc<MergeMetrics>,
    ) -> Option<Self> {
        config.auto_compact.then(|| Self {
            report: Report {
                uri: config.uri.clone(),
                endpoint_name: endpoint_name.to_string(),
                metrics,
            },
            failed_at: None,
            packed_last: false,
            step_limit: STEP_LIMIT,
            running: None,
        })
    }

    /// Starts one compaction step on `table`, which the caller has just committed to.
    ///
    /// Does not wait for it: the next flush must call [`Self::wait_for_step`] first.
    pub fn after_commit(&mut self, table: &DeltaTable, shutdown: &CancellationToken) {
        if let Some(step) = self.running.take() {
            debug_assert!(false, "the flush did not wait for the last step");
            // Left running, it would race the next flush.
            step.abort();
        }
        if self
            .failed_at
            .is_some_and(|at| at.elapsed() < FAILURE_BACKOFF)
        {
            return;
        }
        let planned = match plan_rewrites(table, true) {
            Ok(planned) => planned,
            Err(e) => {
                self.failed(&format!("unable to plan a compaction: {e:#}"));
                return;
            }
        };
        // Only a `REORG` by the operator reclaims such a file.
        self.report.metrics.set_too_big_to_reclaim(planned.too_big);
        let work = choose(planned, self.packed_last);
        self.packed_last = matches!(work, Work::Pack(_));
        // A clone, so the step needs no reopen and the writer's table stays as it is.
        let table = table.clone();
        match work {
            // Nothing to do also means nothing left to reclaim, so the gauge must not linger.
            Work::Nothing => self.report.metrics.reclaim_caught_up(),
            Work::Pack(files) => self.start(shutdown, pack(table, files)),
            Work::Reclaim => self.start(shutdown, reclaim(table)),
        }
    }

    /// Spawns `step`, which reports itself, as no flush may come to collect its result.
    fn start(
        &mut self,
        shutdown: &CancellationToken,
        step: impl Future<Output = AnyResult<Outcome>> + Send + 'static,
    ) {
        let (shutdown, report, limit) = (shutdown.clone(), self.report.clone(), self.step_limit);
        self.running = Some(TOKIO.spawn(async move {
            let started = Instant::now();
            let step = tokio::time::timeout(limit, shutdown.run_until_cancelled(step));
            // A delta-rs panic fails the step only.
            let error = match AssertUnwindSafe(step).catch_unwind().await {
                Ok(Ok(Some(Ok(outcome)))) => {
                    report.done(&outcome, started.elapsed());
                    return None;
                }
                Ok(Ok(Some(Err(e)))) => format!("unable to compact: {e:#}"),
                Ok(Ok(None)) => {
                    info!(
                        "delta_table {}: compaction of '{}' stopped by shutdown",
                        report.endpoint_name, report.uri
                    );
                    return None;
                }
                Ok(Err(_)) => format!("the compaction step took over {limit:?}, so it was stopped"),
                Err(panic) => format!("compaction panicked: {}", panic_message(&*panic)),
            };
            report.failed(&error);
            Some(Instant::now())
        }));
    }

    /// Waits for the step the last commit started.
    ///
    /// # Returns
    ///
    /// Whether there was one, so the table may have changed and the flush must reload it.
    pub fn wait_for_step(&mut self) -> bool {
        let Some(step) = self.running.take() else {
            return false;
        };
        let started = Instant::now();
        // Ends at shutdown too: the step stops itself then.
        match TOKIO.block_on(step) {
            Ok(failed_at) => self.failed_at = failed_at.or(self.failed_at),
            Err(e) => self.failed(&format!("compaction failed: {e}")),
        }
        self.report
            .metrics
            .record_compaction_wait(started.elapsed());
        true
    }

    fn failed(&mut self, error: &str) {
        self.failed_at = Some(Instant::now());
        self.report.failed(error);
    }
}

impl Drop for Compactor {
    /// A step must not outlive its connector.
    fn drop(&mut self) {
        if let Some(step) = &self.running {
            step.abort();
        }
    }
}

fn panic_message(panic: &(dyn std::any::Any + Send)) -> &str {
    panic
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("no message")
}

/// Logs and metrics of a step.
#[derive(Clone)]
struct Report {
    uri: String,
    endpoint_name: String,
    metrics: Arc<MergeMetrics>,
}

impl Report {
    fn done(&self, outcome: &Outcome, took: Duration) {
        self.metrics.record_compaction(
            outcome.reclaim.rows_reclaimed,
            outcome.reclaim.stopped_early,
        );
        let reclaim = &outcome.reclaim;
        let what = if outcome.files_removed > 0 {
            format!(
                "packed {} file(s) into {}, dropping {} superseded row(s)",
                outcome.files_removed, outcome.files_added, reclaim.rows_reclaimed
            )
        } else {
            format!(
                "rewrote {} file(s), dropping {} superseded row(s)",
                reclaim.files_rewritten, reclaim.rows_reclaimed
            )
        };
        // Debug: a backlog runs a step after every commit.
        debug!(
            "delta_table {}: compacted '{}' in {:.1}s: {what}",
            self.endpoint_name,
            self.uri,
            took.as_secs_f64(),
        );
    }

    fn failed(&self, error: &str) {
        self.metrics.record_compaction_failure();
        warn!(
            "delta_table {}: {error}. The table '{}' is intact; the connector tries again at \
             the first commit after {} minutes. If this keeps failing, turn 'auto_compact' \
             off and compact the table another way, as 'Compaction is required' in the Delta \
             output connector documentation describes.",
            self.endpoint_name,
            self.uri,
            FAILURE_BACKOFF.as_secs() / 60,
        );
    }
}

/// A pack first, but a due reclaim after a pack, so steady small files cannot starve it.
fn choose(planned: Plan, packed_last: bool) -> Work {
    if planned.reclaim && (planned.pack.is_empty() || packed_last) {
        Work::Reclaim
    } else if !planned.pack.is_empty() {
        Work::Pack(planned.pack)
    } else {
        Work::Nothing
    }
}

/// What `after_commit` would do with `table`.
#[cfg(test)]
fn plan(table: &DeltaTable, packed_last: bool) -> AnyResult<Work> {
    Ok(choose(plan_rewrites(table, true)?, packed_last))
}

/// What one step did, for the log line and the metrics.
pub(super) struct Outcome {
    files_added: usize,
    files_removed: usize,
    reclaim: ReclaimMetrics,
}

async fn pack(mut table: DeltaTable, files: Vec<Candidate>) -> AnyResult<Outcome> {
    let packed = pack_files(&mut table, &files).await?;
    Ok(Outcome {
        files_added: packed.files_added,
        files_removed: packed.files_removed,
        reclaim: ReclaimMetrics {
            rows_reclaimed: packed.rows_reclaimed,
            // So the gauge does not clear on every pack. Unknown counts as work left: the pack
            // has committed, so it must not fail now.
            stopped_early: has_candidate(&table).unwrap_or(true),
            ..ReclaimMetrics::default()
        },
    })
}

async fn reclaim(mut table: DeltaTable) -> AnyResult<Outcome> {
    Ok(Outcome {
        files_added: 0,
        files_removed: 0,
        reclaim: reclaim_superseded_rows(&mut table, 1).await?,
    })
}

/// One step on `table`, as `after_commit` starts it, naming what it did.
#[cfg(test)]
pub(super) async fn compact_once(table: DeltaTable) -> AnyResult<Option<&'static str>> {
    Ok(match plan(&table, false)? {
        Work::Nothing => None,
        Work::Pack(files) => {
            pack(table, files).await?;
            Some("pack")
        }
        Work::Reclaim => {
            reclaim(table).await?;
            Some("reclaim")
        }
    })
}

/// Pack the small files of `table` now, whether or not they fill a target file.
#[cfg(test)]
pub(super) async fn pack_small_files(table: DeltaTable) -> AnyResult<Outcome> {
    let files = plan_rewrites(&table, false)?.pack;
    pack(table, files).await
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use deltalake::TableProperty;
    use feldera_types::transport::delta_table::{
        DeltaTableUpdateMode, DeltaTableWriteMode, DeltaTableWriterConfig, DeltaVariantEncoding,
    };
    use tempfile::TempDir;

    use super::super::test::{
        append_ids, append_ids_in_row_groups, fixture_table, live_ids, tombstone_ids,
    };
    use super::*;

    fn config_with(uri: &str, auto_compact: bool) -> DeltaTableWriterConfig {
        DeltaTableWriterConfig {
            uri: uri.to_string(),
            mode: DeltaTableWriteMode::Append,
            variant_encoding: DeltaVariantEncoding::default(),
            update_mode: DeltaTableUpdateMode::Merge,
            lookup_chunk_size: feldera_types::size::ConfigSize::from_mib(1),
            max_concurrent_probes: 4,
            checkpoint_interval: None,
            log_retention_duration: None,
            enable_expired_log_cleanup: None,
            max_retries: Some(0),
            threads: Some(1),
            auto_compact,
            object_store_config: Default::default(),
        }
    }

    fn compactor() -> Compactor {
        Compactor::new(&config_with("memory://", true), "e", MergeMetrics::new()).unwrap()
    }

    /// One step after a commit, waited for as the next flush does. Returns whether it ran.
    fn step(compactor: &mut Compactor, table: &DeltaTable) -> bool {
        compactor.after_commit(table, &CancellationToken::new());
        compactor.wait_for_step()
    }

    /// Polls `done`, failing after a minute rather than hanging the test.
    fn wait_until(what: &str, done: impl Fn() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(60);
        while !done() {
            assert!(Instant::now() < deadline, "timed out waiting until {what}");
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// Three packable small files over a store whose data reads can be held.
    fn hung_pack(dir: &TempDir) -> (Arc<super::super::test::FlakyStore>, DeltaTable) {
        use super::super::test::{FlakyStore, table_over};

        TOKIO.block_on(async {
            let table = small_files(dir, 3).await;
            let target = sizes(&table).iter().sum::<u64>() * 5 / 6;
            with_target_file_size(table, target).await
        });
        let store = FlakyStore::new(0, 0);
        let table = TOKIO.block_on(table_over(store.clone(), dir));
        store.hang_data_reads();
        (store, table)
    }

    /// A table of `files` one-file appends, three ids each.
    async fn small_files(dir: &TempDir, files: i64) -> DeltaTable {
        let mut table = fixture_table(dir, &[0, 1, 2], true).await;
        for f in 1..files {
            table = append_ids(table, &[3 * f, 3 * f + 1, 3 * f + 2]).await;
        }
        table
    }

    async fn with_target_file_size(table: DeltaTable, bytes: u64) -> DeltaTable {
        table
            .set_tbl_properties()
            .with_properties(HashMap::from([(
                TableProperty::TargetFileSize.as_ref().to_string(),
                bytes.to_string(),
            )]))
            .await
            .unwrap()
    }

    fn sizes(table: &DeltaTable) -> Vec<u64> {
        table
            .snapshot()
            .unwrap()
            .log_data()
            .into_iter()
            .map(|f| f.size() as u64)
            .collect()
    }

    fn paths(table: &DeltaTable) -> Vec<String> {
        table
            .snapshot()
            .unwrap()
            .log_data()
            .into_iter()
            .map(|f| f.path().to_string())
            .collect()
    }

    fn vectored_files(table: &DeltaTable) -> usize {
        table
            .snapshot()
            .unwrap()
            .log_data()
            .into_iter()
            .filter(|f| f.deletion_vector_descriptor().is_some())
            .count()
    }

    fn reopen(dir: &TempDir) -> DeltaTable {
        TOKIO.block_on(async {
            deltalake::open_table_with_storage_options(
                deltalake::table::builder::ensure_table_uri(dir.path().to_str().unwrap()).unwrap(),
                Default::default(),
            )
            .await
            .unwrap()
        })
    }

    /// Without `auto_compact` the connector must not compact.
    #[test]
    fn compaction_is_off_unless_asked_for() {
        assert!(
            Compactor::new(&config_with("memory://", false), "e", MergeMetrics::new()).is_none()
        );
        assert!(
            Compactor::new(&config_with("memory://", true), "e", MergeMetrics::new()).is_some()
        );
    }

    /// A pack waits until the small files fill one target file, then packs what fits in one.
    #[test]
    fn packs_once_the_small_files_fill_one_target_file() {
        let dir = TempDir::new().unwrap();
        let table = TOKIO.block_on(async {
            let table = small_files(&dir, 3).await;
            // Two files short of the target and three past it, each under half of it.
            let target = sizes(&table).iter().sum::<u64>() * 5 / 6;
            with_target_file_size(table, target).await
        });
        assert!(matches!(plan(&table, false).unwrap(), Work::Pack(_)));

        assert!(step(&mut compactor(), &table));
        // Two of the three fit in one target file; the third waits for the next step.
        let compacted = reopen(&dir);
        assert_eq!(
            sizes(&compacted).len(),
            2,
            "the small files were not packed"
        );
        assert_eq!(
            TOKIO.block_on(live_ids(&compacted)),
            (0..9).collect::<Vec<_>>()
        );
    }

    /// Fewer small files than one target file is not worth a step.
    #[test]
    fn too_few_small_files_are_left_for_later() {
        let dir = TempDir::new().unwrap();
        let table = TOKIO.block_on(async {
            let table = small_files(&dir, 2).await;
            let target = sizes(&table).iter().sum::<u64>() * 5 / 4;
            with_target_file_size(table, target).await
        });
        assert!(matches!(plan(&table, false).unwrap(), Work::Nothing));
        assert!(!step(&mut compactor(), &table));
        assert_eq!(sizes(&reopen(&dir)).len(), 2);
    }

    /// Small files spread one per partition must not start a step: none has a file to pack with.
    #[test]
    fn one_small_file_per_partition_is_not_packable() {
        use super::super::test::partitioned_fixture_table;

        let dir = TempDir::new().unwrap();
        let table = TOKIO.block_on(async {
            let table = partitioned_fixture_table(&dir, &[(1, "a"), (2, "b"), (3, "c")]).await;
            let target = sizes(&table).iter().sum::<u64>() * 5 / 6;
            with_target_file_size(table, target).await
        });
        assert_eq!(
            sizes(&table).len(),
            3,
            "the fixture is not one file per partition"
        );
        assert!(matches!(plan(&table, false).unwrap(), Work::Nothing));
    }

    /// A pack takes one partition's small files and keeps their rows in that partition.
    #[tokio::test]
    async fn a_pack_stays_within_one_partition() {
        use super::super::test::partitioned_fixture_table;

        let dir = TempDir::new().unwrap();
        partitioned_fixture_table(&dir, &[(1, "a"), (2, "b")]).await;
        partitioned_fixture_table(&dir, &[(3, "a"), (4, "b")]).await;
        let table = partitioned_fixture_table(&dir, &[(5, "a")]).await;

        let outcome = pack_small_files(table).await.unwrap();
        assert_eq!(
            outcome.files_removed, 3,
            "partition a has the most small bytes"
        );
        assert_eq!(outcome.files_added, 1);
        let after = deltalake::open_table_with_storage_options(
            deltalake::table::builder::ensure_table_uri(dir.path().to_str().unwrap()).unwrap(),
            Default::default(),
        )
        .await
        .unwrap();
        assert_eq!(live_ids(&after).await, vec![1, 2, 3, 4, 5]);
        let paths = paths(&after);
        assert_eq!(
            paths.iter().filter(|p| p.starts_with("payload=a")).count(),
            1
        );
        assert_eq!(
            paths.iter().filter(|p| p.starts_with("payload=b")).count(),
            2
        );
    }

    /// A partition packs only once its own small files fill a target, or its output is repacked.
    #[test]
    fn a_partition_packs_once_its_own_small_files_fill_a_target() {
        use super::super::test::partitioned_fixture_table;

        let dir = TempDir::new().unwrap();
        let rows = [(1, "a"), (2, "b"), (3, "c"), (4, "d")];
        let table = TOKIO.block_on(async {
            partitioned_fixture_table(&dir, &rows).await;
            let table = partitioned_fixture_table(&dir, &rows).await;
            let s = *sizes(&table).iter().max().unwrap();
            // Each partition holds two files, under half the target; all eight pass it.
            with_target_file_size(table, 6 * s).await
        });
        assert!(matches!(plan(&table, false).unwrap(), Work::Nothing));

        // Partition `a` alone now fills the target.
        let table = TOKIO.block_on(async {
            let mut table = table;
            for id in 10..16 {
                table = partitioned_fixture_table(&dir, &[(id, "a")]).await;
            }
            table
        });
        assert!(step(&mut compactor(), &table));
        let after = paths(&reopen(&dir));
        let in_partition = |p: &str| after.iter().filter(|f| f.starts_with(p)).count();
        assert!(in_partition("payload=a") < 8, "partition a was not packed");
        for p in ["payload=b", "payload=c", "payload=d"] {
            assert_eq!(
                in_partition(p),
                2,
                "{p} was packed with too little to fill a target"
            );
        }
    }

    /// Fifty small files of one tier pack early, or a slow pipeline piles up thousands first.
    #[test]
    fn fifty_small_files_pack_before_they_fill_a_target() {
        let dir = TempDir::new().unwrap();
        // Far under the default 100 MiB target.
        let table = TOKIO.block_on(small_files(&dir, 49));
        assert!(matches!(plan(&table, false).unwrap(), Work::Nothing));

        let table = TOKIO.block_on(append_ids(table, &[1000, 1001, 1002]));
        assert!(matches!(plan(&table, false).unwrap(), Work::Pack(files) if files.len() == 50));
        assert!(step(&mut compactor(), &table));
        assert_eq!(sizes(&reopen(&dir)).len(), 1);
    }

    /// A pack's output waits for files of its own tier, or every fifty new files rewrite it.
    #[test]
    fn a_packed_file_waits_for_files_of_its_own_size() {
        let dir = TempDir::new().unwrap();
        let table = TOKIO.block_on(async {
            let ids: Vec<i64> = (100_000..102_000).collect();
            let table = fixture_table(&dir, &ids, true).await;
            let mut table = table;
            for f in 0..50i64 {
                table = append_ids(table, &[3 * f, 3 * f + 1, 3 * f + 2]).await;
            }
            table
        });
        let larger = table
            .snapshot()
            .unwrap()
            .log_data()
            .into_iter()
            .max_by_key(|f| f.size())
            .unwrap()
            .path()
            .to_string();
        let tiers: std::collections::HashSet<u32> = sizes(&table)
            .into_iter()
            .map(super::super::rewrite::size_tier)
            .collect();
        assert_eq!(tiers.len(), 2, "the fixture needs two size tiers");

        assert!(step(&mut compactor(), &table));
        let after = paths(&reopen(&dir));
        assert!(
            after.contains(&larger),
            "the larger file was packed with the small ones"
        );
        assert_eq!(after.len(), 2);
    }

    /// With nothing left to do, the reclaim gauge must not keep saying work is pending.
    #[test]
    fn nothing_to_do_clears_the_reclaim_gauge() {
        use feldera_adapterlib::metrics::ConnectorMetrics;

        let dir = TempDir::new().unwrap();
        let table = TOKIO.block_on(small_files(&dir, 2));
        let mut compactor = compactor();
        compactor.report.metrics.record_compaction(0, true);

        assert!(!step(&mut compactor, &table));
        let gauge = compactor
            .report
            .metrics
            .metrics()
            .into_iter()
            .find(|(name, ..)| *name == "output_connector_delta_merge_reclaim_incomplete")
            .unwrap()
            .3;
        assert_eq!(gauge, 0.0);
    }

    /// A backlog is packed one target file per step, so no flush waits for all of it.
    #[test]
    fn a_backlog_is_packed_one_target_file_at_a_time() {
        let dir = TempDir::new().unwrap();
        let (table, target) = TOKIO.block_on(async {
            let table = small_files(&dir, 12).await;
            // Each file under half the target; all twelve add up to several targets.
            let target = *sizes(&table).iter().max().unwrap() * 5 / 2;
            (with_target_file_size(table, target).await, target)
        });
        let before = paths(&table);

        assert!(step(&mut compactor(), &table));
        let after = reopen(&dir);
        let rewritten: Vec<_> = before
            .iter()
            .filter(|p| !paths(&after).contains(p))
            .collect();
        let rewritten_bytes: u64 = table
            .snapshot()
            .unwrap()
            .log_data()
            .into_iter()
            .filter(|f| rewritten.contains(&&f.path().to_string()))
            .map(|f| f.size() as u64)
            .sum();
        assert!(rewritten.len() >= 2, "nothing was packed");
        assert!(
            rewritten_bytes <= target,
            "one step rewrote {rewritten_bytes} bytes, more than one target file ({target})"
        );
    }

    /// Only small files are packed, or one step could rewrite the whole table.
    #[test]
    fn mid_size_files_stay_out_of_a_pack() {
        let dir = TempDir::new().unwrap();
        let table = TOKIO.block_on(async {
            let mut table = fixture_table(&dir, &[0], true).await;
            for f in 1..=6i64 {
                let ids: Vec<i64> = (f * 1000..f * 1000 + 300).collect();
                table = append_ids_in_row_groups(table, &ids, 300, 20).await;
                table = append_ids(table, &[f * 1000 + 999]).await;
            }
            // The 300-row files at about three quarters of the target.
            let mid = *sizes(&table).iter().max().unwrap();
            with_target_file_size(table, mid * 4 / 3).await
        });
        let mid = *sizes(&table).iter().max().unwrap();
        let mid_files: Vec<String> = table
            .snapshot()
            .unwrap()
            .log_data()
            .into_iter()
            .filter(|f| f.size() as u64 * 2 > mid)
            .map(|f| f.path().to_string())
            .collect();
        assert_eq!(mid_files.len(), 6, "the fixture has no mid-size files");

        TOKIO.block_on(pack_small_files(table)).unwrap();
        let after = paths(&reopen(&dir));
        for file in &mid_files {
            assert!(
                after.contains(file),
                "the pack rewrote mid-size file {file}"
            );
        }
        assert_eq!(
            after.len(),
            7,
            "the seven small files were not packed into one"
        );
    }

    /// A step reclaims one superseded file, so a reclaim backlog is spread over flushes.
    #[test]
    fn reclaims_one_file_per_step() {
        let dir = TempDir::new().unwrap();
        let table = TOKIO.block_on(async {
            let table = fixture_table(&dir, &[1, 2, 3, 4], true).await;
            let table = append_ids(table, &[5, 6, 7, 8]).await;
            // Both files at the target: not small, so nothing packs and only reclaim runs.
            let target = *sizes(&table).iter().max().unwrap();
            let table = with_target_file_size(table, target).await;
            tombstone_ids(table, &[2, 3, 4, 6, 7, 8]).await
        });
        assert_eq!(vectored_files(&table), 2);

        let mut compactor = compactor();
        assert!(step(&mut compactor, &table));
        let table = reopen(&dir);
        assert_eq!(
            vectored_files(&table),
            1,
            "one step must rewrite exactly one file"
        );

        assert!(step(&mut compactor, &table));
        let table = reopen(&dir);
        assert_eq!(vectored_files(&table), 0);
        assert!(matches!(plan(&table, false).unwrap(), Work::Nothing));
        assert_eq!(TOKIO.block_on(live_ids(&table)), vec![1, 5]);
    }

    /// A flush-sized file is reclaimed under a small target, or its superseded rows stay for ever.
    #[test]
    fn a_file_the_flush_could_write_is_reclaimed_under_a_small_target() {
        let dir = TempDir::new().unwrap();
        let table = TOKIO.block_on(async {
            let ids: Vec<i64> = (0..1000).collect();
            let table = fixture_table(&dir, &ids, true).await;
            let size = sizes(&table)[0];
            let table = with_target_file_size(table, size / 3).await;
            tombstone_ids(table, &(0..900).collect::<Vec<_>>()).await
        });
        assert_eq!(vectored_files(&table), 1);
        assert!(matches!(plan(&table, false).unwrap(), Work::Reclaim));
    }

    /// A file over the reclaim limit is left alone, or one rewrite of it would stall a flush.
    #[test]
    fn a_file_too_big_to_rewrite_is_left_alone() {
        use super::super::rewrite::reclaimable;

        let dir = TempDir::new().unwrap();
        let table = TOKIO.block_on(async {
            let ids: Vec<i64> = (0..1000).collect();
            let table = fixture_table(&dir, &ids, true).await;
            tombstone_ids(table, &(0..900).collect::<Vec<_>>()).await
        });
        let file = table
            .snapshot()
            .unwrap()
            .log_data()
            .into_iter()
            .next()
            .unwrap();
        let size = file.size() as u64;
        assert!(
            reclaimable(&file, size),
            "the fixture is not worth reclaiming"
        );
        assert!(!reclaimable(&file, size - 1));
    }

    /// A step must report a mostly superseded file it cannot rewrite, or nobody hears of it.
    #[test]
    fn a_file_too_big_to_reclaim_is_reported() {
        use super::super::test::commit;
        use deltalake::kernel::Action;

        let dir = TempDir::new().unwrap();
        let table = TOKIO.block_on(async {
            let ids: Vec<i64> = (0..1000).collect();
            let table = fixture_table(&dir, &ids, true).await;
            let mut table = tombstone_ids(table, &(0..900).collect::<Vec<_>>()).await;
            // Planning reads only the log, so a file can claim to be past any limit.
            let file = table
                .snapshot()
                .unwrap()
                .log_data()
                .into_iter()
                .next()
                .unwrap();
            #[allow(deprecated)]
            let mut add = file.add_action();
            add.size = 1 << 40;
            add.data_change = false;
            // Two commits: in one, a remove and an add of the same file leave it removed.
            commit(&mut table, vec![Action::Remove(file.remove_action(false))]).await;
            commit(&mut table, vec![Action::Add(add)]).await;
            table
        });
        let mut compactor = compactor();
        compactor.report.metrics.maintains_the_table();
        assert!(!step(&mut compactor, &table), "a step rewrote the file");

        compactor
            .report
            .metrics
            .record_tombstone_ratio(100, 900, "e", "uri");
        assert!(
            compactor.report.metrics.warned(),
            "the operator was not told to reclaim the file"
        );
    }

    /// A due reclaim follows a pack, or steady small files starve it.
    #[test]
    fn packs_and_reclaims_take_turns() {
        let dir = TempDir::new().unwrap();
        let table = TOKIO.block_on(async {
            let table = small_files(&dir, 3).await;
            let target = sizes(&table).iter().sum::<u64>() * 5 / 6;
            let table = with_target_file_size(table, target).await;
            tombstone_ids(table, &[0, 1]).await
        });
        assert!(matches!(plan(&table, false).unwrap(), Work::Pack(_)));
        assert!(matches!(plan(&table, true).unwrap(), Work::Reclaim));

        let mut compactor = compactor();
        assert!(step(&mut compactor, &table));
        assert!(
            compactor.packed_last,
            "the next step does not know a pack ran"
        );
    }

    /// A failed step pauses compaction rather than failing again after every flush.
    #[test]
    fn a_failed_step_pauses_compaction() {
        let dir = TempDir::new().unwrap();
        let table = TOKIO.block_on(async {
            let table = small_files(&dir, 3).await;
            let target = sizes(&table).iter().sum::<u64>() * 5 / 6;
            with_target_file_size(table, target).await
        });
        // Data files gone from storage fail the pack's read.
        for entry in std::fs::read_dir(dir.path()).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().is_some_and(|x| x == "parquet") {
                std::fs::remove_file(path).unwrap();
            }
        }

        let mut compactor = compactor();
        step(&mut compactor, &table);
        assert!(
            compactor.failed_at.is_some(),
            "the fixture did not fail the step"
        );
        assert!(!step(&mut compactor, &table), "the next flush ran it again");
    }

    /// A pack must report reclaim work it left, or the gauge clears while that work remains.
    #[tokio::test]
    async fn a_pack_reports_the_reclaim_work_left() {
        let dir = TempDir::new().unwrap();
        let ids: Vec<i64> = (0..1000).collect();
        let table = fixture_table(&dir, &ids, true).await;
        let big = sizes(&table)[0];
        let table = append_ids(table, &[1000, 1001]).await;
        let table = append_ids(table, &[1002, 1003]).await;
        // The big file just past the target: too big to pack, small enough to reclaim.
        let table = with_target_file_size(table, big - 1).await;
        let table = tombstone_ids(table, &(0..600).collect::<Vec<_>>()).await;

        let outcome = pack_small_files(table).await.unwrap();
        assert_eq!(outcome.files_removed, 2, "the small files did not pack");
        assert!(
            outcome.reclaim.stopped_early,
            "the mostly superseded file was not reported"
        );
    }

    /// A pack must lose to a commit that changed its files, or it brings back deleted rows.
    #[test]
    fn a_pack_loses_to_a_commit_that_changed_its_files() {
        let dir = TempDir::new().unwrap();
        let table = TOKIO.block_on(async {
            let table = small_files(&dir, 3).await;
            let target = sizes(&table).iter().sum::<u64>() * 5 / 6;
            with_target_file_size(table, target).await
        });
        let Work::Pack(files) = plan(&table, false).unwrap() else {
            panic!("the fixture is not packable");
        };
        // Another writer deletes a row from every file, so from the picked ones too, after the
        // plan read the table.
        let changed = TOKIO.block_on(tombstone_ids(reopen(&dir), &[0, 3, 6]));
        let before = TOKIO.block_on(live_ids(&changed));

        let error = TOKIO
            .block_on(pack(table, files))
            .err()
            .expect("the pack must lose to the concurrent commit");
        assert!(
            format!("{error:#}").contains("removed or rewrote"),
            "unexpected error: {error:#}"
        );
        assert_eq!(TOKIO.block_on(live_ids(&reopen(&dir))), before);
    }

    /// A pack takes at most 100 files, as it reads them one after another.
    #[test]
    fn a_pack_bounds_the_number_of_files_it_reads() {
        let dir = TempDir::new().unwrap();
        let table = TOKIO.block_on(small_files(&dir, 300));
        let Work::Pack(files) = plan(&table, false).unwrap() else {
            panic!("the fixture is not packable");
        };
        assert_eq!(files.len(), 100, "one pack picked {} files", files.len());
    }

    /// A panicking step reports itself and pauses compaction, as no flush may collect it.
    #[test]
    fn a_panicked_step_reports_itself() {
        use feldera_adapterlib::metrics::ConnectorMetrics;

        let mut compactor = compactor();
        let failures = |compactor: &Compactor| {
            compactor
                .report
                .metrics
                .metrics()
                .into_iter()
                .find(|(name, ..)| {
                    *name == "output_connector_delta_merge_compaction_failures_total"
                })
                .unwrap()
                .3
        };
        compactor.start(&CancellationToken::new(), async { panic!("injected") });
        wait_until("the panic is reported", || failures(&compactor) == 1.0);

        assert!(compactor.wait_for_step());
        assert!(
            compactor.failed_at.is_some(),
            "the panic did not pause compaction"
        );
        assert_eq!(failures(&compactor), 1.0, "the panic was counted twice");
    }

    /// A shutdown stops a running pack, or the teardown waits for the whole rewrite.
    #[test]
    fn a_shutdown_stops_a_running_pack() {
        let dir = TempDir::new().unwrap();
        let (store, table) = hung_pack(&dir);
        let shutdown = CancellationToken::new();
        let mut compactor = compactor();
        compactor.after_commit(&table, &shutdown);
        wait_until("the step reads", || store.hung_reads() > 0);

        shutdown.cancel();
        let (done, finished) = std::sync::mpsc::channel();
        std::thread::spawn(move || done.send(compactor.wait_for_step()).unwrap());
        finished
            .recv_timeout(Duration::from_secs(60))
            .expect("the shutdown did not stop the running pack");
        assert_eq!(sizes(&reopen(&dir)).len(), 3, "a stopped pack committed");
    }

    /// A hung step is stopped at its time limit, or it holds up every later flush.
    #[test]
    fn a_hung_step_is_stopped() {
        let dir = TempDir::new().unwrap();
        let (store, table) = hung_pack(&dir);
        let mut compactor = compactor();
        compactor.step_limit = Duration::from_millis(100);
        compactor.after_commit(&table, &CancellationToken::new());

        let (done, finished) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            compactor.wait_for_step();
            done.send(compactor).unwrap();
        });
        let compactor = finished
            .recv_timeout(Duration::from_secs(60))
            .expect("the hung step held up the flush");
        assert!(
            compactor.failed_at.is_some(),
            "the stopped step did not pause compaction"
        );
        wait_until("the stopped step stops reading", || store.hung_reads() == 0);
        assert_eq!(sizes(&reopen(&dir)).len(), 3, "a stopped step committed");
    }

    /// A step must not outlive its connector.
    #[test]
    fn dropping_the_compactor_stops_its_step() {
        let dir = TempDir::new().unwrap();
        let (store, table) = hung_pack(&dir);
        let mut compactor = compactor();
        compactor.after_commit(&table, &CancellationToken::new());
        wait_until("the step reads", || store.hung_reads() > 0);

        drop(compactor);
        wait_until("the dropped step stops reading", || store.hung_reads() == 0);
        assert_eq!(sizes(&reopen(&dir)).len(), 3, "a dropped step committed");
    }

    /// A pack must apply deletion vectors, or it brings back every superseded row.
    #[tokio::test]
    async fn packing_applies_deletion_vectors() {
        use super::super::test::deletion_vector_files;

        let dir = TempDir::new().unwrap();
        let table = fixture_table(&dir, &[1, 2, 3], true).await;
        let table = append_ids(table, &[4, 5, 6]).await;
        let table = tombstone_ids(table, &[2, 5]).await;

        // Otherwise the test would pass on a table with no vector to apply.
        assert_eq!(
            deletion_vector_files(&dir).len(),
            1,
            "the fixture left no deletion vector to apply"
        );
        assert_eq!(live_ids(&table).await, vec![1, 3, 4, 6]);

        let outcome = pack_small_files(table).await.unwrap();
        assert_eq!(outcome.files_removed, 2, "both files should pack");
        assert_eq!(outcome.files_added, 1);
        assert_eq!(outcome.reclaim.rows_reclaimed, 2);

        let compacted = deltalake::open_table_with_storage_options(
            deltalake::table::builder::ensure_table_uri(dir.path().to_str().unwrap()).unwrap(),
            Default::default(),
        )
        .await
        .unwrap();
        assert_eq!(
            live_ids(&compacted).await,
            vec![1, 3, 4, 6],
            "packing resurrected the superseded rows"
        );
    }
}
