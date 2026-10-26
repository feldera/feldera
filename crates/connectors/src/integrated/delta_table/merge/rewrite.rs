//! Rewriting data files: packing small ones, and reclaiming mostly superseded ones.
//!
//! Commits go through [`commit_actions`], not delta-rs `OPTIMIZE`, so a flush that tombstones
//! a file mid-rewrite makes the rewrite lose instead of being undone. `data_change` is false,
//! so a streaming reader sees no row appear or disappear.

use std::collections::HashMap;
use std::num::NonZeroU64;

use anyhow::{Result as AnyResult, anyhow};
use arrow::array::RecordBatch;
use arrow::compute::cast;
use arrow::datatypes::{Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use delta_kernel::expressions::Scalar;
use delta_kernel::table_properties::TableProperties;
use deltalake::datafusion::prelude::{SessionConfig, SessionContext};
use deltalake::kernel::{Action, DeletionVectorDescriptor, LogicalFileView, Remove};
use deltalake::operations::get_num_idx_cols_and_stats_columns;
use deltalake::operations::write::writer::DeltaWriter;
use deltalake::protocol::DeltaOperation;
use deltalake::{DeltaTable, Path};
use futures::StreamExt;
use std::sync::Arc;
use tracing::debug;

use super::super::ReadSchema;
use super::super::deletion_vector::{
    MaskedFile, ReadMode, filtered_parquet_table, read_deletion_vector,
};
use super::super::output::{TARGET_FILE_SIZE, bounded_writer_config};
use super::commit_actions;
use crate::integrated::delta_table::bounded_upload::bound_uploads;

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
    /// The pass reached its file limit with candidates left, so it is incomplete.
    pub stopped_early: bool,
}

/// Larger files are left to `REORG TABLE ... APPLY (PURGE)`: one rewrite would stall a flush.
const MAX_REWRITE_TARGETS: u64 = 2;

/// The largest file a reclaim rewrites: never below two flush files, which ignore the target.
fn max_rewrite_size(target: u64) -> u64 {
    MAX_REWRITE_TARGETS * target.max(TARGET_FILE_SIZE.get())
}

/// A partition also packs once this many small files share a tier, as Delta Lake's
/// `autoCompact.minNumFiles` does, so a slow pipeline does not pile up thousands first.
const MIN_PACK_FILES: usize = 50;

/// Files per pack: they are read one after another, so thousands would be slow however small.
const MAX_PACK_FILES: usize = 2 * MIN_PACK_FILES;

/// A power-of-[`MIN_PACK_FILES`] size band: a pack lands a tier up, so each byte is rewritten
/// once per tier, not once per pack.
pub(super) fn size_tier(size: u64) -> u32 {
    (size.max(1) as f64).log(MIN_PACK_FILES as f64) as u32
}

/// A file to rewrite, as the planning snapshot describes it.
pub(super) struct Candidate {
    path: String,
    deletion_vector: Option<DeletionVectorDescriptor>,
    physical_rows: u64,
    superseded_rows: u64,
    partition_values: HashMap<String, Scalar>,
    /// Captured from the same snapshot, so it names the vector being replaced.
    remove: Remove,
}

impl Candidate {
    /// `None` without a row count, which the rewrite checks its output against.
    fn of(view: &LogicalFileView) -> Option<Self> {
        let deletion_vector = view.deletion_vector_descriptor();
        Some(Self {
            path: view.path().to_string(),
            physical_rows: view.num_records()? as u64,
            superseded_rows: deletion_vector
                .as_ref()
                .map_or(0, |dv| dv.cardinality.max(0) as u64),
            deletion_vector,
            partition_values: partition_values_of(view),
            remove: view.remove_action(false),
        })
    }

    fn live_rows(&self) -> u64 {
        self.physical_rows.saturating_sub(self.superseded_rows)
    }
}

/// What packing a set of small files did.
#[derive(Debug, Default, Clone, Copy)]
pub struct PackMetrics {
    pub files_removed: usize,
    pub files_added: usize,
    /// Superseded rows the small files held, dropped along the way.
    pub rows_reclaimed: u64,
}

/// Rewrite the files whose rows are mostly superseded, one commit each, at most `max_files`.
///
/// Re-plans between files. A rewritten file has no vector left, so it cannot be picked again
/// and the loop terminates. A failure ends the pass and leaves the rest for the next one.
///
/// Stops between files, never inside one: a file is what commits, so stopping between them
/// loses no work. A large backlog is cleared over several passes.
pub async fn reclaim_superseded_rows(
    table: &mut DeltaTable,
    max_files: usize,
) -> AnyResult<ReclaimMetrics> {
    let mut metrics = ReclaimMetrics::default();
    // One partition: the scan runs on the runtime every connector shares.
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));

    loop {
        table
            .update_incremental(None)
            .await
            .map_err(|e| anyhow!("unable to refresh the Delta table before reclaiming: {e}"))?;

        let Some(candidate) = next_candidate(table)? else {
            return Ok(metrics);
        };

        if metrics.files_rewritten >= max_files {
            metrics.stopped_early = true;
            return Ok(metrics);
        }

        debug!(
            "delta merge mode: rewriting '{}', {} of {} rows superseded",
            candidate.path, candidate.superseded_rows, candidate.physical_rows
        );
        let bytes = match rewrite_files(table, std::slice::from_ref(&candidate), &ctx).await {
            Ok((bytes, _)) => bytes,
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

/// Whether any file is worth reclaiming now.
pub(super) fn has_candidate(table: &DeltaTable) -> AnyResult<bool> {
    Ok(next_candidate(table)?.is_some())
}

/// The first file in the snapshot past the threshold, or `None` when none is.
fn next_candidate(table: &DeltaTable) -> AnyResult<Option<Candidate>> {
    let snapshot = table
        .snapshot()
        .map_err(|e| anyhow!("unable to read the Delta table snapshot: {e}"))?;
    let max_size =
        max_rewrite_size(rewrite_target_size(snapshot.snapshot().table_properties()).get());
    Ok(snapshot
        .log_data()
        .into_iter()
        .find(|view| reclaimable(view, max_size))
        .and_then(|view| Candidate::of(&view)))
}

/// Whether `view` is worth reclaiming and small enough to rewrite in one step.
///
/// A file without statistics has no row count to judge against, so it is left alone: a
/// missing statistic must not be read as "every row is dead".
pub(super) fn reclaimable(view: &LogicalFileView, max_size: u64) -> bool {
    view.size() as u64 <= max_size && mostly_superseded(view)
}

fn mostly_superseded(view: &LogicalFileView) -> bool {
    view.deletion_vector_descriptor().is_some_and(|dv| {
        worth_rewriting(
            view.num_records().map(|r| r as u64),
            dv.cardinality.max(0) as u64,
        )
    })
}

/// What one pass over the snapshot found.
pub(super) struct Plan {
    /// The small files to pack, or none.
    pub pack: Vec<Candidate>,
    /// A file is worth reclaiming.
    pub reclaim: bool,
    /// A file is mostly superseded but too big to reclaim, so only `REORG` reclaims it.
    pub too_big: bool,
}

/// Finds, in one pass, the small files to pack and what is left to reclaim.
///
/// Small is under half the target, so any two fit together and a pick always packs.
///
/// # Arguments
///
/// * `when_full` - pack only small files that fill a target file or [`MIN_PACK_FILES`] of one
///   tier; otherwise any partition's small files.
pub(super) fn plan_rewrites(table: &DeltaTable, when_full: bool) -> AnyResult<Plan> {
    let snapshot = table
        .snapshot()
        .map_err(|e| anyhow!("unable to read the Delta table snapshot: {e}"))?;
    let target = rewrite_target_size(snapshot.snapshot().table_properties()).get();
    let max_rewrite = max_rewrite_size(target);

    // Grouped as views, which are cheap, and turned into candidates only once picked.
    let mut partitions: HashMap<Vec<(String, String)>, Vec<LogicalFileView>> = HashMap::new();
    let (mut reclaim, mut too_big) = (false, false);
    for view in snapshot.log_data() {
        if mostly_superseded(&view) {
            if view.size() as u64 <= max_rewrite {
                reclaim = true;
            } else {
                too_big = true;
            }
        }
        if view.size() as u64 >= target / 2 || view.num_records().is_none() {
            continue;
        }
        let mut partition: Vec<(String, String)> = partition_values_of(&view)
            .into_iter()
            .map(|(column, value)| (column, format!("{value:?}")))
            .collect();
        partition.sort();
        partitions.entry(partition).or_default().push(view);
    }
    // A partition's only small file has nothing to be packed with.
    let bytes = |files: &[LogicalFileView]| files.iter().map(|f| f.size() as u64).sum::<u64>();
    let mut sets: Vec<Vec<LogicalFileView>> = Vec::new();
    for files in partitions.into_values() {
        if files.len() < 2 {
            continue;
        }
        if !when_full || bytes(&files) >= target {
            sets.push(files);
            continue;
        }
        let mut tiers: HashMap<u32, Vec<LogicalFileView>> = HashMap::new();
        for file in files {
            tiers
                .entry(size_tier(file.size() as u64))
                .or_default()
                .push(file);
        }
        sets.extend(
            tiers
                .into_values()
                .filter(|tier| tier.len() >= MIN_PACK_FILES),
        );
    }
    let Some(mut files) = sets.into_iter().max_by_key(|files| bytes(files)) else {
        return Ok(Plan {
            pack: Vec::new(),
            reclaim,
            too_big,
        });
    };
    // Smallest first, so a pick from a full set holds over half a target and is rarely repacked.
    files.sort_by_key(|f| f.size());
    // Bounded in bytes and in files, so a step stays small however many files wait.
    let mut picked: Vec<Candidate> = Vec::new();
    let mut picked_bytes = 0;
    for view in &files {
        let size = view.size() as u64;
        if picked.len() >= MAX_PACK_FILES || picked_bytes + size > target {
            break;
        }
        picked_bytes += size;
        picked.extend(Candidate::of(view));
    }
    Ok(Plan {
        pack: picked,
        reclaim,
        too_big,
    })
}

/// Rewrite `files`, all from one partition, into as few target-size files as they fill.
pub(super) async fn pack_files(
    table: &mut DeltaTable,
    files: &[Candidate],
) -> AnyResult<PackMetrics> {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    let (_, files_added) = rewrite_files(table, files, &ctx).await?;
    Ok(PackMetrics {
        files_removed: files.len(),
        files_added,
        rows_reclaimed: files.iter().map(|f| f.superseded_rows).sum(),
    })
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

/// Rewrite the live rows of `files`, all from one partition, and commit the swap. Returns the
/// bytes and the number of files written.
///
/// A failed commit leaves the rows it wrote behind for VACUUM, as a failed flush does: the
/// error can mean the commit landed and only its response was lost.
async fn rewrite_files(
    table: &mut DeltaTable,
    files: &[Candidate],
    ctx: &SessionContext,
) -> AnyResult<(u64, usize)> {
    let Some(first) = files.first() else {
        return Ok((0, 0));
    };
    let what = match files.len() {
        1 => format!("'{}'", first.path),
        n => format!("{n} files starting with '{}'", first.path),
    };
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

    let mut masked = Vec::with_capacity(files.len());
    for file in files {
        let superseded = match &file.deletion_vector {
            Some(dv) => read_deletion_vector(dv, table).await?,
            None => Default::default(),
        };
        masked.push(MaskedFile {
            // delta-rs has already decoded the path, so `Path::from` would encode it twice.
            path: Path::parse(file.path.as_str())
                .unwrap_or_else(|_| Path::from(file.path.as_str())),
            bitmap: superseded,
        });
    }
    let provider = filtered_parquet_table(
        table.object_store(),
        masked,
        ReadSchema::new(&file_schema),
        ReadMode::NotInBitmap,
        // Read in physical order: splitting buys nothing here.
        1,
    )
    .await?;

    let target_size = rewrite_target_size(properties);
    let (num_indexed_cols, stats_columns) =
        get_num_idx_cols_and_stats_columns(Some(properties), HashMap::new());
    let mut writer = DeltaWriter::new(
        bound_uploads(table.object_store()),
        bounded_writer_config(
            table_schema.clone(),
            partition_columns,
            target_size,
            num_indexed_cols,
            stats_columns,
        ),
    );

    let mut batches = ctx
        .read_table(provider)
        .map_err(|e| anyhow!("unable to plan a read of {what}: {e}"))?
        .execute_stream()
        .await
        .map_err(|e| anyhow!("unable to read {what}: {e}"))?;

    let mut rows_written: u64 = 0;
    while let Some(batch) = batches.next().await {
        let batch = batch
            .map_err(|e| anyhow!("error reading the live rows of {what} to rewrite them: {e}"))?;
        if batch.num_rows() == 0 {
            continue;
        }
        rows_written += batch.num_rows() as u64;
        // Through `write`, not `write_partition`: the writer then derives each file's
        // partition from the data, so no row can be filed under the wrong partition.
        let batch = with_partition_columns(&batch, &table_schema, &first.partition_values)
            .map_err(|e| anyhow!("rewriting {what}: {e}"))?;
        writer
            .write(&batch)
            .await
            .map_err(|e| anyhow!("error writing the live rows of {what}: {e:?}"))?;
    }

    let added = writer
        .close()
        .await
        .map_err(|e| anyhow!("error closing the rewrite of {what}: {e:?}"))?;
    let bytes = added.iter().map(|a| a.size.max(0) as u64).sum();

    // The commit below removes the originals, so a read that returned the wrong rows would
    // delete them. Nothing downstream would notice: the table would simply be missing rows,
    // or holding resurrected ones. Checked here, before anything is committed, so a mismatch
    // leaves the table untouched and only orphans what this attempt wrote.
    let live_rows: u64 = files.iter().map(Candidate::live_rows).sum();
    if rows_written != live_rows {
        return Err(anyhow!(
            "internal error: rewriting {what} produced {rows_written} live rows where the \
             log declares {live_rows}; the table is unchanged"
        ));
    }

    let files_added = added.len();
    let mut actions: Vec<Action> = Vec::with_capacity(added.len() + files.len());
    actions.extend(files.iter().map(|f| Action::Remove(f.remove.clone())));
    actions.extend(added.into_iter().map(|mut add| {
        // A rewrite moves no rows in or out of the table, so a reader following the log must
        // not see these as new data.
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
    .map_err(|e| anyhow!("committing the rewrite of {what}: {e}"))?;
    Ok((bytes, files_added))
}

/// How large a file the rewrite should write.
///
/// The table's own target, which is what OPTIMIZE bin-packs to. Writing to any other size
/// would leave every rewritten file for the next run's bin-packing to move again.
fn rewrite_target_size(properties: &TableProperties) -> NonZeroU64 {
    properties.target_file_size.unwrap_or(TARGET_FILE_SIZE)
}

/// Rebuild `batch` in the table's schema, restoring the partition columns.
///
/// A partition column's value is constant across the file and comes from the log.
/// Every column is cast to the type the table declares, because the read asks for
/// view types and the file this rewrite writes must match the table.
fn with_partition_columns(
    batch: &RecordBatch,
    table_schema: &ArrowSchemaRef,
    partition_values: &HashMap<String, Scalar>,
) -> AnyResult<RecordBatch> {
    let mut columns = Vec::with_capacity(table_schema.fields().len());
    for field in table_schema.fields() {
        let array = match batch.column_by_name(field.name()) {
            Some(column) => column.clone(),
            None => {
                let value = partition_values.get(field.name()).ok_or_else(|| {
                    anyhow!(
                        "partition column '{}' has no value in the log",
                        field.name()
                    )
                })?;
                value.to_array(batch.num_rows()).map_err(|e| {
                    anyhow!("unable to expand partition column '{}': {e}", field.name())
                })?
            }
        };
        columns.push(if array.data_type() == field.data_type() {
            array
        } else {
            cast(&array, field.data_type())
                .map_err(|e| anyhow!("unable to cast column '{}': {e}", field.name()))?
        });
    }
    RecordBatch::try_new(table_schema.clone(), columns)
        .map_err(|e| anyhow!("unable to rebuild a batch with its partition columns: {e}"))
}

#[cfg(test)]
mod test {
    use super::super::test::{
        append_ids, commit, fixture_table, live_ids, partitioned_fixture_table, tombstone_ids,
    };
    use super::super::tombstone::{Tombstones, write_deletion_vectors};
    use super::*;
    use arrow::array::{Array, ListBuilder, MapBuilder, StringBuilder};
    use arrow::util::pretty::pretty_format_batches;
    use deltalake::TableProperty;
    use deltalake::kernel::{DataType as DeltaDataType, StructField};
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

    /// Put every data file at or past the target, so OPTIMIZE packs none of them: the case
    /// merge mode creates and plain compaction cannot reclaim.
    async fn with_files_at_target_size(table: DeltaTable) -> DeltaTable {
        let smallest = table
            .snapshot()
            .unwrap()
            .log_data()
            .into_iter()
            .map(|f| f.size())
            .min()
            .unwrap();
        table
            .set_tbl_properties()
            .with_properties(HashMap::from([(
                TableProperty::TargetFileSize.as_ref().to_string(),
                smallest.to_string(),
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
        let table = with_files_at_target_size(table).await;
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

        let reclaimed = reclaim_superseded_rows(&mut table, usize::MAX)
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

    /// A live-row count out of step with the file must not cost the table rows.
    ///
    /// The rewrite commits a `remove` of the original, so it deletes whatever it did not
    /// read. If the count it was planned against is wrong -- a descriptor out of step with
    /// its bitmap, or changed `RowSelection` semantics underneath -- the rows simply vanish
    /// and nothing downstream says so. It must refuse instead, leaving the table as it was.
    #[tokio::test]
    async fn a_rewrite_that_reads_the_wrong_row_count_is_refused() {
        let dir = TempDir::new().unwrap();
        let table = fixture_table(&dir, &[1, 2, 3, 4, 5, 6], true).await;
        let table = with_files_at_target_size(table).await;
        let mut table = tombstone_ids(table, &[2, 3, 4, 5]).await;

        let mut candidate = next_candidate(&table)
            .unwrap()
            .expect("the fixture must offer a candidate");
        assert_eq!((candidate.physical_rows, candidate.superseded_rows), (6, 4));
        // One more superseded row than the bitmap holds: the read still yields the true two
        // live rows, against the one the plan now expects.
        candidate.superseded_rows += 1;

        let before = live_ids(&table).await;
        let ctx = SessionContext::new_with_config(SessionConfig::new());
        let error = rewrite_files(&mut table, std::slice::from_ref(&candidate), &ctx)
            .await
            .expect_err("a wrong live-row count must fail the rewrite");
        assert!(
            format!("{error:#}").contains("the log declares"),
            "unexpected error: {error:#}"
        );
        assert_eq!(
            live_ids(&table).await,
            before,
            "a refused rewrite must leave every row where it was"
        );
    }

    /// A spent file limit stops the pass before the next file, not after it.
    #[tokio::test]
    async fn a_spent_file_limit_stops_the_pass_with_work_left() {
        let dir = TempDir::new().unwrap();
        let table = fixture_table(&dir, &[1, 2, 3, 4, 5, 6], true).await;
        let table = with_files_at_target_size(table).await;
        let mut table = tombstone_ids(table, &[2, 3, 4, 5]).await;

        let reclaimed = reclaim_superseded_rows(&mut table, 0).await.unwrap();

        assert!(reclaimed.stopped_early, "a candidate was left unrewritten");
        assert_eq!(reclaimed.files_rewritten, 0);
        assert_eq!(
            vectored_files(&table).len(),
            1,
            "no file may be rewritten once the limit is spent"
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
            reclaim_superseded_rows(&mut table, usize::MAX)
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

        let reclaimed = reclaim_superseded_rows(&mut table, usize::MAX)
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
    /// over that flush would bring the flush's rows back with no error anywhere. The flush
    /// changed the file's deletion vector, which `commit_actions` refuses to commit over; the
    /// whole safety of one-file-per-commit rests on that, so this pins it.
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
        let error = rewrite_files(&mut table, std::slice::from_ref(&candidate), &ctx)
            .await
            .expect_err("the rewrite must not commit over the flush that superseded row 1");
        assert!(
            format!("{error:#}").contains("removed or rewrote"),
            "expected a conflict over the file both sides removed, got: {error:#}"
        );

        // The flush's tombstone stands. The next pass re-plans against it and reclaims then.
        table.update_incremental(None).await.unwrap();
        assert_eq!(live_ids(&table).await, vec![6]);
    }

    /// A compaction that already replaced the file must make a second rewrite of it lose.
    ///
    /// Both remove the file with `dataChange: false`, which delta-rs no longer treats as a
    /// conflict (delta-rs#4568), so only `commit_actions`'s own check stands between this and
    /// every live row of the file being held twice.
    #[tokio::test]
    async fn a_rewrite_loses_to_a_concurrent_compaction_of_its_file() {
        let dir = TempDir::new().unwrap();
        let table = fixture_table(&dir, &[1, 2, 3, 4, 5, 6], true).await;
        let mut table = tombstone_ids(table, &[2, 3, 4, 5]).await;
        let candidate = next_candidate(&table).unwrap().unwrap();

        let uri =
            deltalake::table::builder::ensure_table_uri(dir.path().to_str().unwrap()).unwrap();
        let mut concurrent = deltalake::open_table(uri).await.unwrap();
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
        rewrite_files(&mut concurrent, std::slice::from_ref(&candidate), &ctx)
            .await
            .unwrap();

        let error = rewrite_files(&mut table, std::slice::from_ref(&candidate), &ctx)
            .await
            .expect_err("the file is already compacted away");
        assert!(
            format!("{error:#}").contains("removed or rewrote"),
            "{error:#}"
        );
        table.update_incremental(None).await.unwrap();
        assert_eq!(live_ids(&table).await, vec![1, 6]);
    }

    /// A commit that touches none of the rewrite's file must not make it lose, or a busy
    /// pipeline, which commits while every rewrite runs, would never reclaim anything.
    #[tokio::test]
    async fn a_rewrite_commits_over_an_unrelated_append() {
        let dir = TempDir::new().unwrap();
        let table = fixture_table(&dir, &[1, 2, 3, 4, 5, 6], true).await;
        let mut table = tombstone_ids(table, &[2, 3, 4, 5]).await;
        let candidate = next_candidate(&table).unwrap().unwrap();

        let uri =
            deltalake::table::builder::ensure_table_uri(dir.path().to_str().unwrap()).unwrap();
        append_ids(deltalake::open_table(uri).await.unwrap(), &[7]).await;

        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
        rewrite_files(&mut table, std::slice::from_ref(&candidate), &ctx)
            .await
            .unwrap();
        assert!(
            vectored_files(&table).is_empty(),
            "the rewrite must have committed"
        );
        assert_eq!(live_ids(&table).await, vec![1, 6, 7]);
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

        let reclaimed = reclaim_superseded_rows(&mut table, usize::MAX)
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

    /// A rewrite must preserve a nested column exactly.
    ///
    /// The read asks for view types at every depth, so a rewrite casts each column back to
    /// what the table declares before writing it. A scalar string would pass on the cast
    /// alone; a struct, list and map are where a per-column cast either handles the nesting
    /// or silently reshapes it.
    #[tokio::test]
    async fn a_rewrite_preserves_nested_columns() {
        use arrow::array::{ArrayRef, Int64Array, StringArray, StructArray};
        use arrow::datatypes::{DataType, Field as ArrowField, Schema as ArrowSchema};
        use deltalake::kernel::{ArrayType, MapType, PrimitiveType, StructType};
        use deltalake::operations::create::CreateBuilder;
        use deltalake::protocol::SaveMode;
        use std::sync::Arc;

        let dir = TempDir::new().unwrap();
        let table = CreateBuilder::new()
            .with_location(dir.path().to_str().unwrap())
            .with_save_mode(SaveMode::Ignore)
            .with_columns(vec![
                StructField::new("id", DeltaDataType::Primitive(PrimitiveType::Long), true),
                StructField::new(
                    "tags",
                    DeltaDataType::Array(Box::new(ArrayType::new(
                        DeltaDataType::Primitive(PrimitiveType::String),
                        true,
                    ))),
                    true,
                ),
                StructField::new(
                    "props",
                    DeltaDataType::Map(Box::new(MapType::new(
                        DeltaDataType::Primitive(PrimitiveType::String),
                        DeltaDataType::Primitive(PrimitiveType::String),
                        true,
                    ))),
                    true,
                ),
                StructField::new(
                    "nested",
                    DeltaDataType::Struct(Box::new(
                        StructType::try_new(vec![StructField::new(
                            "label",
                            DeltaDataType::Primitive(PrimitiveType::String),
                            true,
                        )])
                        .unwrap(),
                    )),
                    true,
                ),
            ])
            .with_configuration_property(TableProperty::EnableDeletionVectors, Some("true"))
            .await
            .unwrap();

        let mut tags = ListBuilder::new(StringBuilder::new());
        for row in [["x", "y"], ["z", "w"]] {
            for tag in row {
                tags.values().append_value(tag);
            }
            tags.append(true);
        }
        let mut props = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
        for value in ["one", "two"] {
            props.keys().append_value("k");
            props.values().append_value(value);
            props.append(true).unwrap();
        }
        let nested = StructArray::from(vec![(
            Arc::new(ArrowField::new("label", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec!["first", "second"])) as ArrayRef,
        )]);
        let batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                ArrowField::new("id", DataType::Int64, true),
                ArrowField::new(
                    "tags",
                    DataType::List(Arc::new(ArrowField::new("item", DataType::Utf8, true))),
                    true,
                ),
                ArrowField::new(
                    "props",
                    DataType::Map(
                        Arc::new(ArrowField::new(
                            "entries",
                            DataType::Struct(
                                vec![
                                    ArrowField::new("keys", DataType::Utf8, false),
                                    ArrowField::new("values", DataType::Utf8, true),
                                ]
                                .into(),
                            ),
                            false,
                        )),
                        false,
                    ),
                    true,
                ),
                ArrowField::new("nested", nested.data_type().clone(), true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(tags.finish()),
                Arc::new(props.finish()),
                Arc::new(nested),
            ],
        )
        .unwrap();
        let mut table = table.write(vec![batch]).await.unwrap();

        // Supersede row 0, leaving row 1 for the rewrite to carry across.
        let path = table
            .snapshot()
            .unwrap()
            .log_data()
            .into_iter()
            .map(|f| f.path().to_string())
            .next()
            .unwrap();
        let mut tombstones = Tombstones::new();
        tombstones.insert(&path, 0);
        let dv = write_deletion_vectors(&tombstones, &table).await.unwrap();
        commit(&mut table, dv.actions).await;

        let reclaimed = reclaim_superseded_rows(&mut table, usize::MAX)
            .await
            .unwrap();
        assert_eq!(reclaimed.files_rewritten, 1);
        assert!(
            vectored_files(&table).is_empty(),
            "the rewritten file must carry no vector"
        );

        let ctx = SessionContext::new();
        ctx.register_table("t", table.table_provider().await.unwrap())
            .unwrap();
        let rows = ctx
            .sql("select id, tags[1], tags[2], props['k'], nested.label from t")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let row = pretty_format_batches(&rows).unwrap().to_string();
        for expected in ["2", "z", "w", "two", "second"] {
            assert!(
                row.contains(expected),
                "the rewrite lost '{expected}' from the surviving row:\n{row}"
            );
        }
    }
}
