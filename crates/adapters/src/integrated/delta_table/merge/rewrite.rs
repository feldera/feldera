//! Reclaiming storage from data files whose rows are mostly superseded.
//!
//! Merge mode marks a superseded row deleted in its file's deletion vector, so the row stays
//! in storage, and in every scan of that file, until something rewrites the file. `OPTIMIZE`
//! does not: it bin-packs files below the target size and drops single-file bins, so a file
//! that has reached the target size is never rewritten however dead it is, and its vector
//! grows without bound.
//!
//! This rewrites exactly those files: read the live rows, write them as a new file, commit
//! the swap. `data_change` is false on both actions, as `OPTIMIZE` sets it, so a streaming
//! reader sees no row appear or disappear.
//!
//! One file per commit, re-planned against a fresh snapshot each time. That keeps the vector
//! being applied current with the version the commit declares as its read version, so a flush
//! that tombstones more rows in the same file mid-rewrite loses the race rather than having
//! its tombstones undone: both sides `remove` the path, which delta-rs rejects as a
//! delete/delete conflict.

use std::collections::HashMap;
use std::num::NonZeroU64;
use std::time::{Duration, Instant};

use anyhow::{Result as AnyResult, anyhow};
use arrow::array::RecordBatch;
use arrow::compute::cast;
use arrow::datatypes::{Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use delta_kernel::expressions::Scalar;
use delta_kernel::table_properties::TableProperties;
use deltalake::datafusion::prelude::{SessionConfig, SessionContext};
use deltalake::kernel::{Action, DeletionVectorDescriptor, LogicalFileView, Remove};
use deltalake::operations::get_num_idx_cols_and_stats_columns;
use deltalake::operations::write::writer::{DeltaWriter, WriterConfig};
use deltalake::protocol::DeltaOperation;
use deltalake::{DeltaTable, Path};
use futures::StreamExt;
use std::sync::Arc;
use tracing::debug;

use super::super::deletion_vector::{ReadMode, filtered_parquet_table, read_deletion_vector};
use super::super::output::TARGET_FILE_SIZE;
use super::commit_actions;

/// Fraction of a file's rows that must be superseded before it is rewritten.
///
/// At one half the bytes moved equal the bytes reclaimed, and the table holds 1.33x its live
/// bytes. Delta Spark's 0.05 default holds 1.03x but rewrites 19x as much; this connector
/// writes continuously, so it takes the storage over the write volume.
const MIN_SUPERSEDED_FRACTION: f64 = 0.5;

/// What one reclamation pass did.
#[derive(Debug, Default, Clone, Copy)]
pub struct ReclaimMetrics {
    pub files_rewritten: usize,
    /// Superseded rows the pass removed from storage.
    pub rows_reclaimed: u64,
    pub bytes_written: u64,
    /// The budget ran out with candidates left, so the pass is incomplete.
    pub stopped_early: bool,
}

/// A file worth rewriting, as the planning snapshot describes it.
struct Candidate {
    path: String,
    deletion_vector: DeletionVectorDescriptor,
    physical_rows: u64,
    superseded_rows: u64,
    partition_values: HashMap<String, Scalar>,
    /// Captured from the same snapshot, so it names the vector being replaced.
    remove: Remove,
}

/// Rewrite every file whose rows are mostly superseded, one commit each, until `budget` runs
/// out.
///
/// Re-plans between files. A rewritten file has no vector left, so it cannot be picked again
/// and the loop terminates. A failure ends the pass and leaves the rest for the next one.
///
/// Stops between files, never inside one: a file is what commits, so stopping between them
/// loses no work. A large backlog is cleared over several passes.
pub async fn reclaim_superseded_rows(
    table: &mut DeltaTable,
    budget: Duration,
) -> AnyResult<ReclaimMetrics> {
    let mut metrics = ReclaimMetrics::default();
    // One partition: the scan shares the runtime a flush blocks on, and reclaiming storage
    // must not outbid the writes it is making room for.
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    let deadline = Instant::now() + budget;

    loop {
        table
            .update_incremental(None)
            .await
            .map_err(|e| anyhow!("unable to refresh the Delta table before reclaiming: {e}"))?;

        let Some(candidate) = next_candidate(table)? else {
            return Ok(metrics);
        };

        if Instant::now() >= deadline {
            metrics.stopped_early = true;
            return Ok(metrics);
        }

        debug!(
            "delta merge mode: rewriting '{}', {} of {} rows superseded",
            candidate.path, candidate.superseded_rows, candidate.physical_rows
        );
        let bytes = match rewrite_one(table, &candidate, &ctx).await {
            Ok(bytes) => bytes,
            // Each file is its own commit, so the ones before this are durable. Saying so
            // keeps the operator from reading the failure as "nothing was reclaimed".
            Err(e) if metrics.files_rewritten > 0 => {
                return Err(anyhow!(
                    "{e:#} (this pass had already reclaimed {} superseded row(s) from {} \
                     file(s), which are committed)",
                    metrics.rows_reclaimed,
                    metrics.files_rewritten,
                ));
            }
            Err(e) => return Err(e),
        };

        metrics.files_rewritten += 1;
        metrics.rows_reclaimed += candidate.superseded_rows;
        metrics.bytes_written += bytes;
    }
}

/// The first file in the snapshot past the threshold, or `None` when none is.
///
/// A file without statistics has no row count to judge against, so it is left alone: a
/// missing statistic must not be read as "every row is dead".
fn next_candidate(table: &DeltaTable) -> AnyResult<Option<Candidate>> {
    let snapshot = table
        .snapshot()
        .map_err(|e| anyhow!("unable to read the Delta table snapshot: {e}"))?;

    for view in snapshot.log_data() {
        let Some(deletion_vector) = view.deletion_vector_descriptor() else {
            continue;
        };
        let physical_rows = view.num_records().map(|r| r as u64);
        let superseded_rows = deletion_vector.cardinality.max(0) as u64;
        if !worth_rewriting(physical_rows, superseded_rows) {
            continue;
        }

        return Ok(Some(Candidate {
            path: view.path().to_string(),
            deletion_vector,
            physical_rows: physical_rows.expect("worth_rewriting rejects an unknown row count"),
            superseded_rows,
            partition_values: partition_values_of(&view),
            remove: view.remove_action(false),
        }));
    }
    Ok(None)
}

/// Whether rewriting a file would reclaim more than it moves.
///
/// `physical_rows` is `None` when the file carries no row-count statistic. That is not
/// "every row is dead": judging it so would rewrite a live file down to nothing, so an
/// unknown row count always answers no.
fn worth_rewriting(physical_rows: Option<u64>, superseded_rows: u64) -> bool {
    match physical_rows {
        Some(rows) if rows > 0 => {
            (superseded_rows as f64) / (rows as f64) >= MIN_SUPERSEDED_FRACTION
        }
        _ => false,
    }
}

fn partition_values_of(view: &LogicalFileView) -> HashMap<String, Scalar> {
    view.partition_values()
        .map(|values| {
            values
                .fields()
                .iter()
                .zip(values.values().iter())
                .map(|(field, value)| (field.name().to_string(), value.clone()))
                .collect()
        })
        .unwrap_or_default()
}

/// Rewrite one file's live rows and commit the swap, returning the bytes written.
///
/// A failed commit leaves the rows it wrote behind for VACUUM, as a failed flush does: the
/// error can mean the commit landed and only its response was lost.
async fn rewrite_one(
    table: &mut DeltaTable,
    candidate: &Candidate,
    ctx: &SessionContext,
) -> AnyResult<u64> {
    let snapshot = table
        .snapshot()
        .map_err(|e| anyhow!("unable to read the Delta table snapshot: {e}"))?;
    let properties = snapshot.snapshot().table_properties();
    let partition_columns = snapshot.metadata().partition_columns().to_vec();
    let table_schema = snapshot.snapshot().arrow_schema();

    // The data file holds every column but the partition ones, whose values live in the log.
    let file_schema = Arc::new(ArrowSchema::new(
        table_schema
            .fields()
            .iter()
            .filter(|f| !partition_columns.contains(f.name()))
            .cloned()
            .collect::<Vec<_>>(),
    ));

    let superseded = read_deletion_vector(&candidate.deletion_vector, table).await?;
    // delta-rs has already decoded the path, so `Path::from` would encode it twice.
    let path = Path::parse(candidate.path.as_str())
        .unwrap_or_else(|_| Path::from(candidate.path.as_str()));
    let provider = filtered_parquet_table(
        table.object_store(),
        path,
        superseded,
        file_schema,
        ReadMode::NotInBitmap,
    )
    .await?;

    let target_size = rewrite_target_size(properties);
    let (num_indexed_cols, stats_columns) =
        get_num_idx_cols_and_stats_columns(Some(properties), HashMap::new());
    let mut writer = DeltaWriter::new(
        table.object_store(),
        WriterConfig::new(
            table_schema.clone(),
            partition_columns,
            None,
            Some(target_size),
            None,
            num_indexed_cols,
            stats_columns,
        ),
    );

    let mut batches = ctx
        .read_table(provider)
        .map_err(|e| anyhow!("unable to plan a read of '{}': {e}", candidate.path))?
        .execute_stream()
        .await
        .map_err(|e| anyhow!("unable to read '{}': {e}", candidate.path))?;

    while let Some(batch) = batches.next().await {
        let batch = batch.map_err(|e| {
            anyhow!(
                "error reading the live rows of '{}' to rewrite them: {e}",
                candidate.path
            )
        })?;
        if batch.num_rows() == 0 {
            continue;
        }
        // Through `write`, not `write_partition`: the writer then derives each file's
        // partition from the data, so no row can be filed under the wrong partition.
        let batch = with_partition_columns(&batch, &table_schema, &candidate.partition_values)
            .map_err(|e| anyhow!("rewriting '{}': {e}", candidate.path))?;
        writer
            .write(&batch)
            .await
            .map_err(|e| anyhow!("error writing the live rows of '{}': {e:?}", candidate.path))?;
    }

    let added = writer
        .close()
        .await
        .map_err(|e| anyhow!("error closing the rewrite of '{}': {e:?}", candidate.path))?;
    let bytes = added.iter().map(|a| a.size.max(0) as u64).sum();

    let mut actions: Vec<Action> = Vec::with_capacity(added.len() + 1);
    actions.push(Action::Remove(candidate.remove.clone()));
    actions.extend(added.into_iter().map(|mut add| {
        // Reclaiming storage moves no rows in or out of the table, so a reader following the
        // log must not see these as new data.
        add.data_change = false;
        Action::Add(add)
    }));

    commit_actions(
        table,
        actions,
        DeltaOperation::Optimize {
            predicate: None,
            target_size: target_size.get() as i64,
        },
    )
    .await
    .map_err(|e| anyhow!("committing the rewrite of '{}': {e}", candidate.path))?;
    Ok(bytes)
}

/// How large a file the rewrite should write.
///
/// The table's own target, which is what OPTIMIZE bin-packs to. Writing to any other size
/// would leave every rewritten file for the next run's bin-packing to move again.
fn rewrite_target_size(properties: &TableProperties) -> NonZeroU64 {
    properties.target_file_size.unwrap_or(TARGET_FILE_SIZE)
}

/// Restore the partition columns, which the data file does not store.
///
/// Their values are constant across the file and come from the log.
fn with_partition_columns(
    batch: &RecordBatch,
    table_schema: &ArrowSchemaRef,
    partition_values: &HashMap<String, Scalar>,
) -> AnyResult<RecordBatch> {
    let mut columns = Vec::with_capacity(table_schema.fields().len());
    for field in table_schema.fields() {
        if let Some(column) = batch.column_by_name(field.name()) {
            columns.push(column.clone());
            continue;
        }
        let value = partition_values.get(field.name()).ok_or_else(|| {
            anyhow!(
                "partition column '{}' has no value in the log",
                field.name()
            )
        })?;
        let array = value
            .to_array(batch.num_rows())
            .map_err(|e| anyhow!("unable to expand partition column '{}': {e}", field.name()))?;
        columns.push(if array.data_type() == field.data_type() {
            array
        } else {
            cast(&array, field.data_type())
                .map_err(|e| anyhow!("unable to cast partition column '{}': {e}", field.name()))?
        });
    }
    RecordBatch::try_new(table_schema.clone(), columns)
        .map_err(|e| anyhow!("unable to rebuild a batch with its partition columns: {e}"))
}

#[cfg(test)]
mod test {
    use super::super::test::{fixture_table, live_ids, partitioned_fixture_table, tombstone_ids};
    use super::*;
    use deltalake::TableProperty;
    use tempfile::TempDir;

    /// Data files the snapshot still references, with the size of each one's vector.
    fn vectored_files(table: &DeltaTable) -> Vec<(String, i64)> {
        table
            .snapshot()
            .unwrap()
            .log_data()
            .into_iter()
            .filter_map(|f| {
                f.deletion_vector_descriptor()
                    .map(|dv| (f.path().to_string(), dv.cardinality))
            })
            .collect()
    }

    /// Every action in the table's latest commit, as JSON.
    fn last_commit_actions(dir: &TempDir) -> Vec<serde_json::Value> {
        let log = dir.path().join("_delta_log");
        let latest = std::fs::read_dir(&log)
            .unwrap()
            .filter_map(|e| {
                let path = e.unwrap().path();
                (path.extension()? == "json").then_some(path)
            })
            .max()
            .expect("no commit files");
        std::fs::read_to_string(latest)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    /// Force every data file past the bin-packing threshold, which is what makes OPTIMIZE
    /// skip it: the case merge mode creates and plain compaction cannot reclaim.
    async fn with_tiny_target_file_size(table: DeltaTable) -> DeltaTable {
        table
            .set_tbl_properties()
            .with_properties(HashMap::from([(
                TableProperty::TargetFileSize.as_ref().to_string(),
                "1".to_string(),
            )]))
            .await
            .unwrap()
    }

    /// The whole point: a file OPTIMIZE refuses to touch must still give its storage back.
    ///
    /// OPTIMIZE bin-packs only files below the target size and drops single-file bins, so a
    /// file at or above that size keeps every superseded row for ever. Without the rewrite
    /// this test fails at the first assertion after `optimize`.
    #[tokio::test]
    async fn a_file_optimize_skips_is_still_reclaimed() {
        let dir = TempDir::new().unwrap();
        let table = fixture_table(&dir, &[1, 2, 3, 4, 5, 6], true).await;
        let table = with_tiny_target_file_size(table).await;
        // Four of six rows superseded, past the half the rewrite asks for.
        let table = tombstone_ids(table, &[2, 3, 4, 5]).await;
        assert_eq!(live_ids(&table).await, vec![1, 6]);

        let (mut table, metrics) = table.optimize().await.unwrap();
        assert_eq!(
            (metrics.num_files_removed, metrics.total_files_skipped),
            (0, 1),
            "the fixture must exercise a file OPTIMIZE skips"
        );
        assert_eq!(vectored_files(&table).len(), 1, "OPTIMIZE left the vector");

        let reclaimed = reclaim_superseded_rows(&mut table, Duration::from_secs(60))
            .await
            .unwrap();

        assert_eq!(reclaimed.files_rewritten, 1);
        assert_eq!(reclaimed.rows_reclaimed, 4);
        assert!(
            vectored_files(&table).is_empty(),
            "the rewritten file must carry no vector: {:?}",
            vectored_files(&table)
        );
        assert_eq!(
            live_ids(&table).await,
            vec![1, 6],
            "a rewrite must not change what a reader sees"
        );
    }

    /// An exhausted budget must stop before the next file, not after it: a pass that checked
    /// too late would rewrite one more file every time.
    #[tokio::test]
    async fn an_exhausted_budget_stops_the_pass_with_work_left() {
        let dir = TempDir::new().unwrap();
        let table = fixture_table(&dir, &[1, 2, 3, 4, 5, 6], true).await;
        let table = with_tiny_target_file_size(table).await;
        let mut table = tombstone_ids(table, &[2, 3, 4, 5]).await;

        let reclaimed = reclaim_superseded_rows(&mut table, Duration::ZERO)
            .await
            .unwrap();

        assert!(reclaimed.stopped_early, "a candidate was left unrewritten");
        assert_eq!(reclaimed.files_rewritten, 0);
        assert_eq!(
            vectored_files(&table).len(),
            1,
            "no file may be rewritten once the budget is gone"
        );
        assert_eq!(live_ids(&table).await, vec![1, 6]);
    }

    /// A rewrite moves no rows in or out of the table, so a reader following the log must not
    /// see one. `data_change: false` is how the log says so, and it is what OPTIMIZE writes.
    #[tokio::test]
    async fn a_rewrite_is_not_a_data_change() {
        let dir = TempDir::new().unwrap();
        let table = fixture_table(&dir, &[1, 2, 3, 4], true).await;
        let mut table = tombstone_ids(table, &[2, 3, 4]).await;

        assert_eq!(
            reclaim_superseded_rows(&mut table, Duration::from_secs(60))
                .await
                .unwrap()
                .files_rewritten,
            1
        );

        let actions = last_commit_actions(&dir);
        let flags: Vec<bool> = actions
            .iter()
            .filter_map(|a| {
                a.get("add")
                    .or_else(|| a.get("remove"))
                    .and_then(|f| f["dataChange"].as_bool())
            })
            .collect();
        assert_eq!(
            flags.len(),
            2,
            "expected one add and one remove: {actions:?}"
        );
        assert!(
            flags.iter().all(|changed| !changed),
            "every action must be dataChange=false: {actions:?}"
        );
    }

    /// Below the threshold the rewrite would move more bytes than it reclaims, so it must not
    /// run. Without this the pass would rewrite the whole table on every interval.
    #[tokio::test]
    async fn a_lightly_superseded_file_is_left_alone() {
        let dir = TempDir::new().unwrap();
        let table = fixture_table(&dir, &[1, 2, 3, 4, 5, 6], true).await;
        let mut table = tombstone_ids(table, &[2]).await;
        assert_eq!(vectored_files(&table).len(), 1);

        let reclaimed = reclaim_superseded_rows(&mut table, Duration::from_secs(60))
            .await
            .unwrap();

        assert_eq!(
            reclaimed.files_rewritten, 0,
            "one row in six is not worth a rewrite"
        );
        assert_eq!(vectored_files(&table).len(), 1, "the vector must remain");
        assert_eq!(live_ids(&table).await, vec![1, 3, 4, 5, 6]);
    }

    /// A flush that supersedes more rows in a file mid-rewrite must win.
    ///
    /// The rewrite copies the live rows as its planning snapshot saw them, so committing it
    /// over that flush would bring the flush's rows back with no error anywhere. Both sides
    /// `remove` the path, which delta-rs rejects; the whole safety of one-file-per-commit
    /// rests on that, so this pins it. Which conflict it reports is delta-rs's business.
    #[tokio::test]
    async fn a_flush_during_a_rewrite_makes_the_rewrite_lose() {
        let dir = TempDir::new().unwrap();
        let table = fixture_table(&dir, &[1, 2, 3, 4, 5, 6], true).await;
        let mut table = tombstone_ids(table, &[2, 3, 4, 5]).await;
        assert_eq!(live_ids(&table).await, vec![1, 6]);

        // Plan the rewrite, then let a flush supersede row 1 in the same file through a
        // handle of its own, leaving this handle's snapshot behind.
        let candidate = next_candidate(&table)
            .unwrap()
            .expect("four rows of six are superseded");
        let uri =
            deltalake::table::builder::ensure_table_uri(dir.path().to_str().unwrap()).unwrap();
        let concurrent = deltalake::open_table(uri).await.unwrap();
        tombstone_ids(concurrent, &[1]).await;

        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
        let error = rewrite_one(&mut table, &candidate, &ctx)
            .await
            .expect_err("the rewrite must not commit over the flush that superseded row 1");
        assert!(
            format!("{error:#}").contains("CommitConflict"),
            "expected a conflict over the file both sides removed, got: {error:#}"
        );

        // The flush's tombstone stands. The next pass re-plans against it and reclaims then.
        table.update_incremental(None).await.unwrap();
        assert_eq!(live_ids(&table).await, vec![6]);
    }

    /// The rewritten rows must land back in their own partition. Filing them anywhere else
    /// would make every partition-pruned read wrong, for us and for every other reader.
    #[tokio::test]
    async fn a_rewrite_keeps_rows_in_their_partition() {
        let dir = TempDir::new().unwrap();
        let table = partitioned_fixture_table(
            &dir,
            &[(1, "a"), (2, "a"), (3, "a"), (4, "b"), (5, "b"), (6, "b")],
        )
        .await;
        // Two thirds of partition "a" superseded, none of "b".
        let mut table = tombstone_ids(table, &[1, 2]).await;

        let reclaimed = reclaim_superseded_rows(&mut table, Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(
            reclaimed.files_rewritten, 1,
            "only partition \"a\" qualifies"
        );

        let paths: Vec<String> = table
            .snapshot()
            .unwrap()
            .log_data()
            .into_iter()
            .map(|f| f.path().to_string())
            .collect();
        assert!(
            paths
                .iter()
                .all(|p| p.starts_with("payload=a/") || p.starts_with("payload=b/")),
            "a rewritten file left its partition directory: {paths:?}"
        );
        assert_eq!(live_ids(&table).await, vec![3, 4, 5, 6]);
        assert!(vectored_files(&table).is_empty());
    }

    /// A rewrite must write the size the table asks for, so the next bin-packing leaves its
    /// output alone. Only the connector's own default fills in when the table names none.
    #[test]
    fn the_rewrite_writes_the_table_s_target_size() {
        let mut properties = TableProperties::default();
        assert_eq!(rewrite_target_size(&properties), TARGET_FILE_SIZE);

        properties.target_file_size = NonZeroU64::new(64 << 20);
        assert_eq!(rewrite_target_size(&properties).get(), 64 << 20);
    }

    /// The threshold decision, including the two ways a file must never be judged dead.
    ///
    /// Driven directly: delta-rs always writes a row count, so the unknown-count branch is
    /// unreachable through the writer, and it is the branch whose inverse would rewrite a
    /// live file down to nothing.
    #[test]
    fn worth_rewriting_needs_a_known_row_count_and_half_the_rows() {
        assert!(
            !worth_rewriting(None, 100),
            "an unknown row count is not a dead file"
        );
        assert!(
            !worth_rewriting(Some(0), 0),
            "an empty file has nothing to reclaim"
        );

        assert!(
            !worth_rewriting(Some(10), 4),
            "below half is not worth the bytes"
        );
        assert!(worth_rewriting(Some(10), 5), "half is the threshold");
        assert!(
            worth_rewriting(Some(10), 10),
            "a fully superseded file qualifies"
        );
    }
}
