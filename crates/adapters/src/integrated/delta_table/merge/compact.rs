//! Compaction driven by the connector, for tables nothing else maintains (`auto_compact`).
//!
//! It runs on the writer's thread right after a commit, as Delta Lake's auto compaction does:
//! beside a flush, the two raced and whichever committed second redid all its work. Each run
//! is about one target file of work, so the flush behind it is delayed by a bounded amount.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result as AnyResult;
use dbsp::circuit::tokio::TOKIO;
use deltalake::DeltaTable;
use feldera_types::transport::delta_table::DeltaTableWriterConfig;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::super::output::TARGET_FILE_SIZE;
use super::metrics::MergeMetrics;
use super::rewrite::{ReclaimMetrics, has_candidate, partition_values_of, reclaim_superseded_rows};

/// More small files than this many target files would stall a flush for minutes to pack.
const MAX_PACK_TARGETS: u64 = 4;

const BACKLOG_WARNING_INTERVAL: Duration = Duration::from_secs(3600);

/// Spaces out runs that fail, which would otherwise retry, and fail, after every flush.
const FAILURE_BACKOFF: Duration = Duration::from_secs(600);

/// These CPU-bound rewrites run on [`TOKIO`], which every connector shares.
const MAX_COMPACTION_TASKS: usize = 2;

/// What one run after a commit does.
#[derive(Debug, PartialEq, Eq)]
enum Work {
    Nothing,
    /// Pack `bytes` of small files.
    Pack {
        bytes: u64,
    },
    /// Rewrite one mostly superseded file.
    Reclaim,
    /// More small files than one run may pack.
    Backlog {
        bytes: u64,
        limit: u64,
    },
}

pub struct Compactor {
    uri: String,
    endpoint_name: String,
    metrics: Arc<MergeMetrics>,
    /// Small-file bytes when a pack last rewrote nothing: delta-rs packs only neighbouring
    /// files, and a layout it cannot pack must not be retried after every flush.
    unpackable_at: Option<u64>,
    failed_at: Option<Instant>,
    backlog_warned: Option<Instant>,
}

impl Compactor {
    /// `None` unless the connector was asked to compact.
    pub fn new(
        config: &DeltaTableWriterConfig,
        endpoint_name: &str,
        metrics: Arc<MergeMetrics>,
    ) -> Option<Self> {
        config.auto_compact.then(|| Self {
            uri: config.uri.clone(),
            endpoint_name: endpoint_name.to_string(),
            metrics,
            unpackable_at: None,
            failed_at: None,
            backlog_warned: None,
        })
    }

    /// One bounded step of compaction on `table`, which the caller has just committed to.
    /// Returns whether the table may have changed, so the next flush must reload it.
    pub fn after_commit(&mut self, table: &DeltaTable, shutdown: &CancellationToken) -> bool {
        if self
            .failed_at
            .is_some_and(|at| at.elapsed() < FAILURE_BACKOFF)
        {
            return false;
        }
        let work = match self.plan(table) {
            Ok(work) => work,
            Err(e) => {
                self.failed(&format!("unable to plan a compaction: {e:#}"));
                return false;
            }
        };
        // A clone, so the run needs no reopen; spawned, so a delta-rs panic fails the run only.
        let (table, shutdown) = (table.clone(), shutdown.clone());
        let run = match work {
            Work::Nothing => return false,
            Work::Backlog { bytes, limit } => {
                self.warn_backlog(bytes, limit);
                return false;
            }
            Work::Pack { .. } => {
                TOKIO.spawn(async move { shutdown.run_until_cancelled(pack(table)).await })
            }
            Work::Reclaim => {
                TOKIO.spawn(async move { shutdown.run_until_cancelled(reclaim(table)).await })
            }
        };
        let started = Instant::now();
        match TOKIO.block_on(run) {
            Ok(Some(Ok(outcome))) => {
                if let Work::Pack { bytes } = work {
                    self.unpackable_at = (outcome.files_removed == 0).then_some(bytes);
                }
                self.metrics.record_compaction(
                    outcome.reclaim.rows_reclaimed,
                    outcome.reclaim.stopped_early,
                );
                info!(
                    "delta_table {}: compacted '{}' in {:.1}s: packed {} file(s) into {}, and \
                     reclaimed {} superseded row(s) from {} file(s)",
                    self.endpoint_name,
                    self.uri,
                    started.elapsed().as_secs_f64(),
                    outcome.files_removed,
                    outcome.files_added,
                    outcome.reclaim.rows_reclaimed,
                    outcome.reclaim.files_rewritten,
                );
            }
            Ok(Some(Err(e))) => self.failed(&format!("unable to compact: {e:#}")),
            Ok(None) => info!(
                "delta_table {}: compaction of '{}' stopped by shutdown",
                self.endpoint_name, self.uri
            ),
            Err(e) => self.failed(&format!("compaction failed: {e}")),
        }
        true
    }

    /// Decide what this run does, from the snapshot alone.
    fn plan(&self, table: &DeltaTable) -> AnyResult<Work> {
        let snapshot = table.snapshot()?;
        let target = snapshot
            .snapshot()
            .table_properties()
            .target_file_size
            .unwrap_or(TARGET_FILE_SIZE)
            .get();

        // Files under half the target, by partition: any two of them fit in one target file.
        let mut small: HashMap<Vec<(String, String)>, (u64, u64)> = HashMap::new();
        for view in snapshot.log_data() {
            let size = view.size() as u64;
            if size >= target / 2 {
                continue;
            }
            let mut partition: Vec<(String, String)> = partition_values_of(&view)
                .into_iter()
                .map(|(column, value)| (column, format!("{value:?}")))
                .collect();
            partition.sort();
            let (files, bytes) = small.entry(partition).or_default();
            *files += 1;
            *bytes += size;
        }
        // delta-rs drops a bin of one file, so a partition's only small file does not count.
        let bytes: u64 = small
            .values()
            .filter(|(files, _)| *files >= 2)
            .map(|(_, bytes)| bytes)
            .sum();

        let limit = MAX_PACK_TARGETS * target;
        if bytes > limit {
            return Ok(Work::Backlog { bytes, limit });
        }
        if bytes >= target && self.unpackable_at.is_none_or(|at| bytes >= at + target) {
            return Ok(Work::Pack { bytes });
        }
        if has_candidate(table)? {
            return Ok(Work::Reclaim);
        }
        Ok(Work::Nothing)
    }

    fn failed(&mut self, error: &str) {
        self.failed_at = Some(Instant::now());
        self.metrics.record_compaction_failure();
        warn!(
            "delta_table {}: {error}. The table '{}' is intact; the connector tries again in \
             {} minutes. If this keeps failing, turn 'auto_compact' off and run OPTIMIZE on \
             the table instead.",
            self.endpoint_name,
            self.uri,
            FAILURE_BACKOFF.as_secs() / 60,
        );
    }

    fn warn_backlog(&mut self, bytes: u64, limit: u64) {
        if self
            .backlog_warned
            .is_some_and(|at| at.elapsed() < BACKLOG_WARNING_INTERVAL)
        {
            return;
        }
        self.backlog_warned = Some(Instant::now());
        warn!(
            "delta_table {}: '{}' holds {} MiB of small files, more than the {} MiB one \
             compaction packs, so 'auto_compact' leaves them alone rather than stall a flush. \
             Run OPTIMIZE on the table once; the connector keeps it compacted from then on.",
            self.endpoint_name,
            self.uri,
            bytes >> 20,
            limit >> 20,
        );
    }
}

/// What one run did, for the log line and the metrics.
pub(super) struct Outcome {
    files_added: u64,
    files_removed: u64,
    reclaim: ReclaimMetrics,
}

/// Bin-pack the small files to the table's own `delta.targetFileSize`.
pub(super) async fn pack(table: DeltaTable) -> AnyResult<Outcome> {
    let (table, metrics) = table
        .optimize()
        .with_max_concurrent_tasks(MAX_COMPACTION_TASKS)
        .await?;
    Ok(Outcome {
        files_added: metrics.num_files_added,
        files_removed: metrics.num_files_removed,
        // Packing reclaims nothing, so only say whether reclaiming still has work.
        reclaim: ReclaimMetrics {
            stopped_early: has_candidate(&table)?,
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

#[cfg(test)]
mod test {
    use deltalake::TableProperty;
    use feldera_types::transport::delta_table::{
        DeltaTableUpdateMode, DeltaTableWriteMode, DeltaTableWriterConfig, DeltaVariantEncoding,
    };
    use tempfile::TempDir;

    use super::super::test::{append_ids, fixture_table, live_ids, tombstone_ids};
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

    /// Compacting is the table administrator's job unless the connector is asked to do it.
    #[test]
    fn compaction_is_off_unless_asked_for() {
        assert!(
            Compactor::new(&config_with("memory://", false), "e", MergeMetrics::new()).is_none()
        );
        assert!(
            Compactor::new(&config_with("memory://", true), "e", MergeMetrics::new()).is_some()
        );
    }

    /// A pack waits until the small files fill one target file, so every run does about the
    /// same work instead of one per flush.
    #[test]
    fn packs_once_the_small_files_fill_one_target_file() {
        let dir = TempDir::new().unwrap();
        let table = TOKIO.block_on(async {
            let table = small_files(&dir, 3).await;
            // Two files short of the target and three past it, each under half of it.
            let target = sizes(&table).iter().sum::<u64>() * 5 / 6;
            with_target_file_size(table, target).await
        });
        let mut compactor = compactor();
        assert!(matches!(compactor.plan(&table).unwrap(), Work::Pack { .. }));

        assert!(compactor.after_commit(&table, &CancellationToken::new()));
        // delta-rs packs bins up to the target, so two of the three fill one.
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

    /// Fewer small files than one target file is not worth a run.
    #[test]
    fn too_few_small_files_are_left_for_later() {
        let dir = TempDir::new().unwrap();
        let table = TOKIO.block_on(async {
            let table = small_files(&dir, 2).await;
            let target = sizes(&table).iter().sum::<u64>() * 5 / 4;
            with_target_file_size(table, target).await
        });
        let mut compactor = compactor();
        assert_eq!(compactor.plan(&table).unwrap(), Work::Nothing);
        assert!(!compactor.after_commit(&table, &CancellationToken::new()));
        assert_eq!(sizes(&reopen(&dir)).len(), 2);
    }

    /// delta-rs never packs a partition's only file, so small files spread one per partition
    /// must not trigger a run that would rewrite nothing.
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
        assert_eq!(compactor().plan(&table).unwrap(), Work::Nothing);
    }

    /// A backlog is left to the operator: packing it in one run would stall the flush.
    #[test]
    fn a_backlog_of_small_files_is_left_alone() {
        let dir = TempDir::new().unwrap();
        let table = TOKIO.block_on(async {
            let table = small_files(&dir, 12).await;
            // Each file under half the target, all twelve past four targets.
            let largest = *sizes(&table).iter().max().unwrap();
            with_target_file_size(table, largest * 5 / 2).await
        });
        let mut compactor = compactor();
        assert!(matches!(
            compactor.plan(&table).unwrap(),
            Work::Backlog { .. }
        ));
        assert!(!compactor.after_commit(&table, &CancellationToken::new()));
        assert_eq!(sizes(&reopen(&dir)).len(), 12, "the backlog was packed");
    }

    /// A layout delta-rs could not pack is planned again only once another target file of
    /// small files has arrived, not after every flush.
    #[test]
    fn an_unpackable_layout_waits_for_more_small_files() {
        let dir = TempDir::new().unwrap();
        let table = TOKIO.block_on(async {
            let table = small_files(&dir, 3).await;
            let target = sizes(&table).iter().sum::<u64>() * 5 / 6;
            with_target_file_size(table, target).await
        });
        let mut compactor = compactor();
        let Work::Pack { bytes } = compactor.plan(&table).unwrap() else {
            panic!("the fixture is not packable");
        };

        compactor.unpackable_at = Some(bytes);
        assert_eq!(compactor.plan(&table).unwrap(), Work::Nothing);

        compactor.unpackable_at = Some(0);
        assert!(matches!(compactor.plan(&table).unwrap(), Work::Pack { .. }));
    }

    /// A run reclaims one superseded file, so a reclaim backlog is spread over flushes.
    #[test]
    fn reclaims_one_file_per_run() {
        let dir = TempDir::new().unwrap();
        let table = TOKIO.block_on(async {
            let table = fixture_table(&dir, &[1, 2, 3, 4], true).await;
            let table = append_ids(table, &[5, 6, 7, 8]).await;
            // Every file past the target, so nothing packs and only the reclaim runs.
            let table = with_target_file_size(table, 1).await;
            tombstone_ids(table, &[2, 3, 4, 6, 7, 8]).await
        });
        assert_eq!(vectored_files(&table), 2);

        let mut compactor = compactor();
        assert!(compactor.after_commit(&table, &CancellationToken::new()));
        let table = reopen(&dir);
        assert_eq!(
            vectored_files(&table),
            1,
            "one run must rewrite exactly one file"
        );

        assert!(compactor.after_commit(&table, &CancellationToken::new()));
        let table = reopen(&dir);
        assert_eq!(vectored_files(&table), 0);
        assert_eq!(compactor.plan(&table).unwrap(), Work::Nothing);
        assert_eq!(TOKIO.block_on(live_ids(&table)), vec![1, 5]);
    }

    /// A failed run pauses compaction rather than failing again after every flush.
    #[test]
    fn a_failed_run_pauses_compaction() {
        let dir = TempDir::new().unwrap();
        let table = TOKIO.block_on(async {
            let table = small_files(&dir, 3).await;
            let target = sizes(&table).iter().sum::<u64>() * 5 / 6;
            with_target_file_size(table, target).await
        });
        // A data file gone from storage fails the pack's read.
        let parquet = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().path())
            .find(|p| p.extension().is_some_and(|x| x == "parquet"))
            .unwrap();
        std::fs::remove_file(parquet).unwrap();

        let mut compactor = compactor();
        compactor.after_commit(&table, &CancellationToken::new());
        assert!(
            compactor.failed_at.is_some(),
            "the fixture did not fail the run"
        );
        assert!(
            !compactor.after_commit(&table, &CancellationToken::new()),
            "the next flush ran it again"
        );
    }

    /// A pack must report reclaim work it left, or the incomplete gauge would clear on every
    /// pack while mostly superseded files remain.
    #[tokio::test]
    async fn a_pack_reports_the_reclaim_work_left() {
        let dir = TempDir::new().unwrap();
        let ids: Vec<i64> = (0..1000).collect();
        let table = fixture_table(&dir, &ids, true).await;
        let big = sizes(&table)[0];
        let table = append_ids(table, &[1000, 1001]).await;
        let table = append_ids(table, &[1002, 1003]).await;
        // The big file past the target, so only the two small ones pack.
        let table = with_target_file_size(table, big - 1).await;
        let table = tombstone_ids(table, &(0..600).collect::<Vec<_>>()).await;

        let outcome = pack(table).await.unwrap();
        assert_eq!(outcome.files_removed, 2, "the small files did not pack");
        assert!(
            outcome.reclaim.stopped_early,
            "the mostly superseded file was not reported"
        );
    }

    /// A shutdown stops the compaction, or it would hold up the teardown.
    #[test]
    fn a_shutdown_stops_the_compaction() {
        let dir = TempDir::new().unwrap();
        let table = TOKIO.block_on(async {
            let table = small_files(&dir, 3).await;
            let target = sizes(&table).iter().sum::<u64>() * 5 / 6;
            with_target_file_size(table, target).await
        });
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        compactor().after_commit(&table, &shutdown);
        assert_eq!(
            sizes(&reopen(&dir)).len(),
            3,
            "a compaction ran after the shutdown"
        );
    }

    /// OPTIMIZE must apply the deletion vectors it rewrites. Every superseded row is still
    /// in a data file and dead only by its vector, so bin-packing without applying them would
    /// bring them all back.
    #[tokio::test]
    async fn compacting_applies_deletion_vectors() {
        use super::super::test::deletion_vector_files;

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

        let outcome = pack(table).await.unwrap();
        assert_eq!(outcome.files_removed, 2, "both files should bin-pack");
        assert_eq!(outcome.files_added, 1);

        let compacted = deltalake::open_table_with_storage_options(
            deltalake::table::builder::ensure_table_uri(dir.path().to_str().unwrap()).unwrap(),
            Default::default(),
        )
        .await
        .unwrap();
        assert_eq!(
            live_ids(&compacted).await,
            vec![1, 3, 4, 6],
            "compaction resurrected the superseded rows"
        );
    }
}
