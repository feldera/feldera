//! One merge-mode flush: walk the batch once, append new rows, tombstone superseded ones,
//! commit.
//!
//! The whole flush runs while the output batch is still available, which is what lets a
//! conflicting commit retry from the top. A retry cannot reuse the previous attempt's row
//! ordinals: a compaction changes file paths, and an ordinal only means something within one.
//!
//! ```text
//! for each key in the batch, in key order:
//!     insert or update -> serialize the new row into the append writer
//!     delete or update -> serialize the key into the lookup chunk
//!     (an insert also needs a lookup unless the table started empty)
//!
//! when the chunk fills, and once at the end:
//!     locate its keys in the table -> (file, physical row ordinal) pairs
//!
//! commit: the appended files, plus a remove/add pair per tombstoned file
//! ```
//!
//! Nothing derived from the batch is held whole: appended rows stream into the writer, and
//! removal keys are held as encoded bytes, one chunk at a time.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Result as AnyResult, anyhow};
use arrow::datatypes::{Field as ArrowField, Schema as ArrowSchema};
use delta_kernel::expressions::Scalar;
use deltalake::DeltaTable;
use deltalake::kernel::{Action, Add, LogicalFileView};
use deltalake::logstore::ObjectStoreRef;
use deltalake::operations::write::writer::{DeltaWriter, WriterConfig};
use deltalake::protocol::{DeltaOperation, SaveMode};
use serde_arrow::ArrayBuilder;
use serde_arrow::schema::SerdeArrowSchema;
use tracing::debug;

use crate::RecordFormat;
use crate::catalog::{SerBatchReader, SerCursor, SplitCursorBuilder};
use crate::util::{IndexedOperationType, indexed_operation_type};
use feldera_types::program_schema::{Relation, SqlIdentifier};

use super::super::WriteError;
use super::super::output::TARGET_FILE_SIZE;
use super::chunk::LookupChunk;
use super::commit_actions;
use super::key::{self, KeyEncoder};
use super::probe::Pruning;
use super::probe::{Candidate, ProbeMetrics, locate};
use super::prune::PartitionFilter;
use super::startup::{MergeSetup, Regime, StatsConfig};
use super::tombstone::{DvWriteMetrics, Tombstones, write_deletion_vectors};
use super::transient;

/// Rows buffered in the append writers before a chunk is written out, over the whole flush.
///
/// A flush that splits into ranges divides this between them rather than giving each the
/// whole of it, so its buffers cost the same whatever `threads` says.
pub(super) const APPEND_CHUNK_ROWS: usize = 100_000;
/// Target-sized files a range must be worth before a flush splits into one more.
///
/// A range's last file is however full it happens to be, so a split costs one partial file
/// per range. One file per range would make a range mostly that partial file; three holds
/// the waste under a sixth of what the range writes.
const FILES_PER_RANGE: u64 = 3;

/// Rows a range buffers before writing, never so few that a write is pure overhead.
pub(super) const MIN_RANGE_CHUNK_ROWS: usize = 8_192;

/// Keys buffered before they are encoded into the lookup chunk. Encoding works on an arrow
/// batch, so keys are gathered into one first.
const KEY_BATCH_ROWS: usize = 8192;

/// Where a flush's time went.
///
/// A flush reports minutes against a large table and the counters alone cannot say which
/// phase spent them.
///
/// The phases partition one flush and sum to `total`, but only while the flush walks a
/// single key range.  `threads` splits it into ranges that run at once, and the three phases
/// a range performs -- `batch_walk`, `probe` and `append` -- are then summed across ranges:
/// they measure the work the flush did rather than the time it took, and can exceed `total`.
/// The three outside a range stay wall time whatever the thread count.
#[derive(Debug, Default, Clone, Copy)]
pub struct FlushTimings {
    /// The whole flush, including every phase below.
    pub total: Duration,
    /// Walking the Delta log: once to build the probe's candidate list, once after the
    /// commit to count the table's rows.  Grows with the table's file count, not the batch.
    pub log_scan: Duration,
    /// Walking the batch: serializing each row and encoding each key.
    pub batch_walk: Duration,
    /// Locating the rows to supersede, summed over every lookup pass.
    pub probe: Duration,
    /// Encoding appended rows to parquet and streaming them to the object store.
    pub append: Duration,
    /// Reading, merging and writing the deletion vectors.
    pub deletion_vectors: Duration,
    /// Writing the commit to the Delta log.
    pub commit: Duration,
}

impl FlushTimings {
    /// Add one range's timings. `total` belongs to the flush, not to a range, so it is left
    /// alone.
    fn merge(&mut self, range: &Self) {
        self.log_scan += range.log_scan;
        self.batch_walk += range.batch_walk;
        self.probe += range.probe;
        self.append += range.append;
        self.deletion_vectors += range.deletion_vectors;
        self.commit += range.commit;
    }
}

/// Runs `f`, adding its wall time to `slot`.
async fn timed<T, F: Future<Output = T>>(slot: &mut Duration, f: F) -> T {
    let start = Instant::now();
    let value = f.await;
    *slot += start.elapsed();
    value
}

/// What one flush did. Reported to the controller and asserted on by the tests.
#[derive(Debug, Default, Clone, Copy)]
pub struct FlushMetrics {
    /// Rows appended: inserts plus the new side of updates.
    pub rows_appended: u64,
    /// Keys that reached the lookup, below the changed-key count by what the insert
    /// shortcut saved.
    pub keys_probed: u64,
    /// Lookup passes run. Above one only when the key set exceeded `lookup_chunk_bytes`.
    pub lookup_passes: usize,
    pub probe: ProbeMetrics,
    pub dv: DvWriteMetrics,
    /// Data files appended by this flush.
    pub files_appended: usize,
    /// Bytes written to the object store: new data files plus the packed vector object.
    pub bytes_written: u64,
    /// Live rows in the table after this flush, from the snapshot's statistics.
    pub table_live_rows: u64,
    /// Rows in the table that a deletion vector covers, after this flush.
    pub table_superseded_rows: u64,
    /// Key ranges this flush walked. Above one when `threads` split the batch.
    pub ranges: usize,
    pub timings: FlushTimings,
}

impl FlushMetrics {
    /// Absorb what one key range produced.
    ///
    /// Counters add. So do the timings, which makes them the work the flush did rather than
    /// the time it took: ranges run at once, so their spans overlap in wall time.
    fn merge_range(&mut self, range: &Self) {
        self.rows_appended += range.rows_appended;
        self.keys_probed += range.keys_probed;
        self.lookup_passes += range.lookup_passes;
        self.probe.merge(&range.probe);
        self.timings.merge(&range.timings);
    }
}

/// One range's share of the budgets a flush spends as a whole.
///
/// Both are configured for a flush, not for a thread, so ranges divide them. Left
/// undivided, `threads` would quietly multiply the memory the buffers cost and the requests
/// the lookup has in flight.
#[derive(Debug, Clone, Copy)]
pub(super) struct RangeBudget {
    /// Rows this range buffers before it writes them.
    pub(super) chunk_rows: usize,
    /// Data files this range's lookup reads at once.
    pub(super) probes: usize,
}

impl RangeBudget {
    fn split(writer: &MergeWriter, ranges: usize) -> Self {
        Self::divide(writer.max_concurrent_probes, ranges)
    }

    fn divide(max_concurrent_probes: usize, ranges: usize) -> Self {
        let ranges = ranges.max(1);
        Self {
            chunk_rows: (APPEND_CHUNK_ROWS / ranges).max(MIN_RANGE_CHUNK_ROWS),
            probes: (max_concurrent_probes / ranges).max(1),
        }
    }

    /// The division alone, without a writer to take the configured value from.
    #[cfg(test)]
    pub(super) fn for_test(max_concurrent_probes: usize, ranges: usize) -> Self {
        Self::divide(max_concurrent_probes, ranges)
    }
}

/// What one key range produced, before the ranges are joined into one commit.
struct RangeOutput {
    added: Vec<Add>,
    tombstones: Tombstones,
    metrics: FlushMetrics,
    violations: Vec<anyhow::Error>,
}

/// Everything a flush needs that outlives it.
pub struct MergeWriter {
    key_encoder: KeyEncoder,
    /// Serde schema of the key columns alone, for turning cursor keys into arrow.
    key_serde_schema: SerdeArrowSchema,
    /// Serde schema of the full row, for the append side.
    row_serde_schema: SerdeArrowSchema,
    row_arrow_schema: Arc<ArrowSchema>,
    partition_columns: Vec<String>,
    partition_key_columns: Vec<String>,
    stats_config: StatsConfig,
    key_arrow_fields: Vec<ArrowField>,
    prune_on_stats: bool,
    regime: Regime,
    lookup_chunk_bytes: usize,
    max_concurrent_probes: usize,
    view_name: SqlIdentifier,
    index_name: SqlIdentifier,
    /// Rows a key range must be worth before a flush splits the batch, whatever the rate
    /// below says. Pinned by a test so the parallel path is reachable without a fixture
    /// large enough to fill a [`TARGET_FILE_SIZE`] file.
    #[cfg(test)]
    rows_per_range_override: Option<usize>,
    /// Bytes one appended row took in the last flush that wrote any, or zero before then.
    ///
    /// A range writes its own files, so a range worth less than [`TARGET_FILE_SIZE`] costs
    /// a file below the size the writer aims for.  Rows are what a split can be measured in
    /// up front and bytes are what the cost is in, so the rate between them is carried from
    /// one flush to the next.
    bytes_per_row: AtomicU64,
}

/// The failure a flush reports when several ranges fail at once.
///
/// A deterministic failure outranks a transient one whichever range raised it: the flush
/// cannot succeed on retry, and reporting the transient error instead would rewrite the whole
/// batch on every attempt -- for ever, under the default `max_retries: None`.
fn worse(existing: Option<WriteError>, failure: WriteError) -> Option<WriteError> {
    match (existing, failure) {
        (None, failure) => Some(failure),
        (Some(WriteError::Transient(_)), failure @ WriteError::Deterministic(_)) => Some(failure),
        (Some(existing), _) => Some(existing),
    }
}

impl MergeWriter {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        setup: MergeSetup,
        key_schema: &Relation,
        row_serde_schema: SerdeArrowSchema,
        row_arrow_schema: Arc<ArrowSchema>,
        lookup_chunk_bytes: usize,
        max_concurrent_probes: usize,
        view_name: SqlIdentifier,
    ) -> AnyResult<Self> {
        // Keys are built as arrow arrays of the *table's* types, the same types the probe
        // reads out of parquet, so both sides encode to comparable bytes.
        let key_serde_schema = SerdeArrowSchema::try_from(setup.key_arrow_fields.as_slice())
            .map_err(|e| anyhow!("unable to build the key encoder schema: {e}"))?;

        Ok(Self {
            key_encoder: setup.key_encoder,
            key_serde_schema,
            row_serde_schema,
            row_arrow_schema,
            partition_columns: setup.partition_columns,
            partition_key_columns: setup.partition_key_columns,
            stats_config: setup.stats_config,
            key_arrow_fields: setup.key_arrow_fields,
            prune_on_stats: setup.prune_on_stats,
            regime: setup.regime,
            lookup_chunk_bytes,
            max_concurrent_probes,
            view_name,
            #[cfg(test)]
            rows_per_range_override: None,
            bytes_per_row: AtomicU64::new(0),
            index_name: key_schema.name.clone(),
        })
    }

    /// Apply one batch to `table` and commit, refreshing the table to the new version.
    ///
    /// The whole batch is taken rather than a cursor over it, because a flush may walk it as
    /// several key ranges, and because a retry has to re-run the lookup against whatever
    /// paths exist now.
    ///
    /// `retrying` must be set on every attempt after the first. A commit error can mean the
    /// commit landed and only its response was lost, so the rows an earlier attempt appended
    /// may be in the table: [`Regime::Owned`]'s insert shortcut is unsound from then on, and
    /// skipping the lookup would leave two live rows for one key.
    ///
    /// `progress` counts the rows written so far, for the controller to report while a large
    /// flush is still running. The caller owns resetting it.
    ///
    /// `threads` splits the batch into that many key ranges, each walked and written by its
    /// own task. Ranges are disjoint, so their appended files concatenate and their
    /// tombstones union; the flush still commits once.
    ///
    /// A flush that fails part-way leaves the data files it had already streamed behind,
    /// which `VACUUM` collects; nothing is committed, so no reader sees them. Splitting the
    /// batch does not change that, only how many ranges can be the one that failed.
    #[allow(clippy::too_many_arguments)]
    pub async fn flush(
        self: &Arc<Self>,
        table: &mut DeltaTable,
        object_store: ObjectStoreRef,
        batch: Arc<dyn SerBatchReader>,
        format: RecordFormat,
        threads: usize,
        retrying: bool,
        on_uniqueness_violation: &mut dyn FnMut(anyhow::Error),
        progress: Arc<AtomicU64>,
    ) -> Result<FlushMetrics, WriteError> {
        let mut metrics = FlushMetrics::default();
        let started = Instant::now();
        let result = self
            .flush_phases(
                table,
                object_store,
                batch,
                format,
                threads,
                retrying,
                on_uniqueness_violation,
                progress,
                &mut metrics,
            )
            .await;
        metrics.timings.total = started.elapsed();
        result.map(|()| metrics)
    }

    /// Rows a range must be worth for the flush to be split into one.
    ///
    /// Only a flush that follows one knows what a row costs, so the floor comes in two parts:
    /// [`APPEND_CHUNK_ROWS`] until a flush has written, then the rows [`FILES_PER_RANGE`]
    /// files take at the rate that flush wrote at.
    ///
    /// The first floor is deliberately permissive -- a backfill is the flush most worth
    /// splitting, and it is the one with no rate to go on -- so it buys speed with files:
    /// measured on a 2.2 GB backfill needing 21 files, 8 ranges wrote 24 and 16 wrote 32.
    /// `threads` is what bounds that; the second floor keeps the steady stream of update
    /// flushes that follows from fragmenting the table for ever.
    fn rows_per_range(&self) -> usize {
        #[cfg(test)]
        if let Some(rows) = self.rows_per_range_override {
            return rows.max(1);
        }
        match self.bytes_per_row.load(Ordering::Relaxed) {
            0 => APPEND_CHUNK_ROWS,
            rate => {
                ((FILES_PER_RANGE * TARGET_FILE_SIZE.get() / rate) as usize).max(APPEND_CHUNK_ROWS)
            }
        }
    }

    /// Split every `rows` rows, so a test need not build a fixture large enough to fill a
    /// [`TARGET_FILE_SIZE`] file to reach the parallel path.
    #[cfg(test)]
    pub(super) fn split_every(&mut self, rows: usize) {
        self.rows_per_range_override = Some(rows);
    }

    /// Key ranges to walk in parallel, at most one per thread.
    ///
    /// A range needs its own [`DeltaWriter`], so splitting a small flush only fragments it
    /// into files below [`TARGET_FILE_SIZE`]. Once a flush has written rows, the floor is
    /// the rows that size takes; before then it is [`APPEND_CHUNK_ROWS`], which splits a
    /// first flush large enough to be worth splitting and leaves a small one whole.
    fn ranges(
        &self,
        batch: &Arc<dyn SerBatchReader>,
        format: &RecordFormat,
        threads: usize,
    ) -> Vec<SplitCursorBuilder> {
        let wanted = threads
            .min(batch.key_count() / self.rows_per_range())
            .max(1);
        // One range is the common case, and it needs no bounds: `partition_keys` samples the
        // batch to find them, which is work with nothing to split.
        if wanted <= 1 {
            return SplitCursorBuilder::from_bounds(
                batch.clone(),
                &*batch.keys_factory().default_box(),
                0,
                format.clone(),
            )
            .into_iter()
            .collect();
        }

        let mut bounds = batch.keys_factory().default_box();
        batch.partition_keys(wanted, &mut *bounds);
        (0..=bounds.len())
            .filter_map(|i| {
                SplitCursorBuilder::from_bounds(batch.clone(), &*bounds, i, format.clone())
            })
            .collect()
    }

    /// The flush itself. Split out so [`Self::flush`] times the whole of it in one place,
    /// including whatever a failure ran before it gave up.
    #[allow(clippy::too_many_arguments)]
    async fn flush_phases(
        self: &Arc<Self>,
        table: &mut DeltaTable,
        object_store: ObjectStoreRef,
        batch: Arc<dyn SerBatchReader>,
        format: RecordFormat,
        threads: usize,
        retrying: bool,
        on_uniqueness_violation: &mut dyn FnMut(anyhow::Error),
        progress: Arc<AtomicU64>,
        metrics: &mut FlushMetrics,
    ) -> Result<(), WriteError> {
        // The snapshot the lookup runs against. The commit declares it as its read version,
        // so a conflicting change to these files is caught rather than overwritten.
        let scan_started = Instant::now();
        let candidates = Arc::new(self.snapshot_files(table)?);
        metrics.timings.log_scan += scan_started.elapsed();

        let ranges = self.ranges(&batch, &format, threads);
        metrics.ranges = ranges.len();
        let outputs = self
            .walk_ranges(ranges, object_store, candidates, retrying, progress.clone())
            .await?;

        let mut added = Vec::new();
        let mut tombstones = Tombstones::new();
        for output in outputs {
            // Reported after the join rather than from inside a range, so the order does not
            // depend on which range happened to finish first.
            for violation in output.violations {
                on_uniqueness_violation(violation);
            }
            added.extend(output.added);
            tombstones.merge(output.tombstones);
            metrics.merge_range(&output.metrics);
        }

        metrics.files_appended = added.len();
        metrics.bytes_written = added.iter().map(|a| a.size.max(0) as u64).sum();
        if metrics.rows_appended > 0 && metrics.bytes_written > 0 {
            self.bytes_per_row.store(
                (metrics.bytes_written / metrics.rows_appended).max(1),
                Ordering::Relaxed,
            );
        }

        self.commit(table, added, tombstones, metrics).await?;
        // Counted too, or a delete-only flush reports no progress while it is working.
        progress.fetch_add(metrics.dv.rows_tombstoned, Ordering::Relaxed);
        let scan_started = Instant::now();
        count_table_rows(table, metrics);
        metrics.timings.log_scan += scan_started.elapsed();
        Ok(())
    }

    /// Walk every range, in parallel when there is more than one.
    ///
    /// A single range runs inline: spawning for it would cost a task and hand the work to
    /// another thread for no gain.
    async fn walk_ranges(
        self: &Arc<Self>,
        ranges: Vec<SplitCursorBuilder>,
        object_store: ObjectStoreRef,
        candidates: Arc<Vec<Candidate>>,
        retrying: bool,
        progress: Arc<AtomicU64>,
    ) -> Result<Vec<RangeOutput>, WriteError> {
        let budget = RangeBudget::split(self, ranges.len());
        if ranges.len() <= 1 {
            let mut outputs = Vec::with_capacity(ranges.len());
            for range in ranges {
                outputs.push(
                    self.clone()
                        .walk_range(
                            range,
                            object_store.clone(),
                            candidates.clone(),
                            retrying,
                            progress.clone(),
                            budget,
                        )
                        .await?,
                );
            }
            return Ok(outputs);
        }

        // Each range is mostly serialization, so a spawned range occupies a runtime worker
        // for as long as it runs. This is what the non-merge path does with `threads` too.
        let mut handles = Vec::with_capacity(ranges.len());
        for range in ranges {
            let writer = self.clone();
            let object_store = object_store.clone();
            let candidates = candidates.clone();
            let progress = progress.clone();
            handles.push(tokio::spawn(async move {
                writer
                    .walk_range(range, object_store, candidates, retrying, progress, budget)
                    .await
            }));
        }

        // Every handle is awaited even after one fails, so no range is left writing to the
        // object store while the flush reports its error.
        let mut outputs = Vec::with_capacity(handles.len());
        let mut failure = None;
        for handle in handles {
            match handle.await {
                Ok(Ok(output)) => outputs.push(output),
                Ok(Err(e)) => failure = worse(failure, e),
                Err(e) => {
                    failure = worse(failure, transient(format!("a merge range panicked: {e}")))
                }
            }
        }
        match failure {
            Some(e) => Err(e),
            None => Ok(outputs),
        }
    }

    /// Walk one key range: append its new rows, look up the rows they supersede.
    async fn walk_range(
        self: Arc<Self>,
        range: SplitCursorBuilder,
        object_store: ObjectStoreRef,
        candidates: Arc<Vec<Candidate>>,
        retrying: bool,
        progress: Arc<AtomicU64>,
        budget: RangeBudget,
    ) -> Result<RangeOutput, WriteError> {
        let walk_started = Instant::now();
        let mut metrics = FlushMetrics::default();
        let mut violations = Vec::new();
        let mut split = range.build();
        let cursor: &mut dyn SerCursor = &mut split;

        let mut appends = self.append_writer(object_store.clone());
        let mut rows = ArrayBuilder::new(self.row_serde_schema.clone()).map_err(|e| {
            WriteError::Deterministic(anyhow!("error creating the row builder: {e}"))
        })?;
        let mut keys = KeyChunk::new(self.clone(), object_store, candidates, budget.probes)?;
        let mut buffered_rows = 0;

        while cursor.key_valid() {
            let op = match indexed_operation_type(&self.view_name, &self.index_name, cursor) {
                Ok(op) => op,
                Err(e) => {
                    // A key with two values has no single row to write. Skip it and let the
                    // controller report it, as cdc mode does.
                    violations.push(e);
                    cursor.step_key();
                    continue;
                }
            };

            let Some(op) = op else {
                cursor.step_key();
                continue;
            };

            if self.needs_lookup(&op, retrying) {
                // A flattened cursor reads its key out of the current value.
                cursor.rewind_vals();
                keys.push(cursor, &mut metrics).await?;
            }

            if matches!(
                op,
                IndexedOperationType::Insert | IndexedOperationType::Upsert
            ) {
                position_at_new_value(cursor);
                cursor
                    .serialize_val_to_arrow(&mut rows)
                    .map_err(WriteError::Deterministic)?;
                buffered_rows += 1;
                metrics.rows_appended += 1;

                if buffered_rows >= budget.chunk_rows {
                    timed(
                        &mut metrics.timings.append,
                        write_rows(&mut rows, &mut appends),
                    )
                    .await?;
                    progress.fetch_add(buffered_rows as u64, Ordering::Relaxed);
                    buffered_rows = 0;
                }
            }

            cursor.step_key();
        }

        if buffered_rows > 0 {
            timed(
                &mut metrics.timings.append,
                write_rows(&mut rows, &mut appends),
            )
            .await?;
            progress.fetch_add(buffered_rows as u64, Ordering::Relaxed);
        }
        let tombstones = keys.finish(&mut metrics).await?;

        // Transient: the writer streamed data files to the object store, so a failure here
        // is I/O.  It cannot be retried in place, only by redoing the flush.
        let added = timed(&mut metrics.timings.append, appends.close())
            .await
            .map_err(|e| transient(format!("error closing the Delta writer: {e:?}")))?;

        // What the walk cost is what is left of it once its two timed phases are taken out.
        // Timing the loop body directly would read the clock once per row.
        metrics.timings.batch_walk = walk_started
            .elapsed()
            .saturating_sub(metrics.timings.probe + metrics.timings.append);

        Ok(RangeOutput {
            added,
            tombstones,
            metrics,
            violations,
        })
    }

    /// Whether the row this operation supersedes has to be located in the table.
    ///
    /// Deletes and updates always. An insert supersedes nothing the view held, but the table
    /// may hold a row for that key from an earlier run or from an earlier attempt at this
    /// same batch, so it skips the lookup only on a first attempt against a table that
    /// started empty.
    fn needs_lookup(&self, op: &IndexedOperationType, retrying: bool) -> bool {
        match op {
            IndexedOperationType::Insert => retrying || self.regime.insert_needs_lookup(),
            IndexedOperationType::Delete | IndexedOperationType::Upsert => true,
        }
    }

    /// Data files in the table's current snapshot, each carrying the log's statistics and the
    /// partition values of any key column the file does not store.
    fn snapshot_files(&self, table: &DeltaTable) -> Result<Vec<Candidate>, WriteError> {
        let snapshot = table.snapshot().map_err(|e| {
            WriteError::Deterministic(anyhow!("unable to read the Delta table snapshot: {e}"))
        })?;

        Ok(snapshot
            .log_data()
            .into_iter()
            .map(|file| {
                Candidate::from_log(
                    &file,
                    self.partition_keys_of(&file),
                    &self.key_encoder,
                    self.prune_on_stats,
                )
            })
            .collect())
    }

    /// Values of the key columns this file keeps in the log rather than in its data.
    fn partition_keys_of(&self, file: &LogicalFileView) -> HashMap<String, Scalar> {
        if self.partition_key_columns.is_empty() {
            return HashMap::new();
        }
        let Some(Scalar::Struct(values)) = file.partition_values().map(Scalar::Struct) else {
            return HashMap::new();
        };

        let mut keys = HashMap::with_capacity(self.partition_key_columns.len());
        for name in &self.partition_key_columns {
            if let Some(index) = values.fields().iter().position(|f| f.name() == name)
                && let Some(value) = values.values().get(index)
            {
                keys.insert(name.clone(), value.clone());
            }
        }
        keys
    }

    /// The writer for appended rows, collecting statistics the way the table asks for them.
    fn append_writer(&self, object_store: ObjectStoreRef) -> DeltaWriter {
        let config = WriterConfig::new(
            self.row_arrow_schema.clone(),
            self.partition_columns.clone(),
            None,
            // Without a target size delta-rs writes one object per flush, whatever its
            // size, and an object store rejects a multipart upload past 10000 parts.
            Some(TARGET_FILE_SIZE),
            None,
            self.stats_config.num_indexed_cols,
            self.stats_config.stats_columns.clone(),
        );
        DeltaWriter::new(object_store, config)
    }

    /// Write the deletion vectors and commit everything as one version.
    ///
    /// A failed commit leaves its vector object behind for VACUUM: the error can mean the
    /// commit landed and only its response was lost, so the object may be a live reference.
    async fn commit(
        &self,
        table: &mut DeltaTable,
        added: Vec<Add>,
        tombstones: Tombstones,
        metrics: &mut FlushMetrics,
    ) -> Result<(), WriteError> {
        let dv = timed(
            &mut metrics.timings.deletion_vectors,
            write_deletion_vectors(&tombstones, table),
        )
        .await?;
        metrics.dv = dv.metrics;
        metrics.bytes_written += dv.metrics.dv_bytes as u64;

        let mut actions: Vec<Action> = added.into_iter().map(Action::Add).collect();
        actions.extend(dv.actions);

        if actions.is_empty() {
            return Ok(());
        }

        // Transient: a commit fails when it loses a conflict to concurrent maintenance, or
        // when the log write itself fails.  Both want the flush redone against the table as
        // it now stands, which is the caller's retry.
        timed(
            &mut metrics.timings.commit,
            commit_actions(
                table,
                actions,
                DeltaOperation::Write {
                    mode: SaveMode::Append,
                    partition_by: None,
                    predicate: None,
                },
            ),
        )
        .await
        // Kept whole rather than re-wrapped, so the commit's own context survives.
        .map_err(WriteError::Transient)
    }
}

/// Read the table's live and superseded row counts out of the committed snapshot.
///
/// A file without statistics contributes nothing, so these are a floor. They only drive the
/// compaction warning, which is advice, so a floor is good enough.
fn count_table_rows(table: &DeltaTable, metrics: &mut FlushMetrics) {
    let Ok(snapshot) = table.snapshot() else {
        return;
    };
    for file in snapshot.log_data() {
        let Some(rows) = file.num_records() else {
            continue;
        };
        let superseded = file
            .deletion_vector_descriptor()
            .map(|dv| dv.cardinality.max(0) as u64)
            .unwrap_or(0);
        metrics.table_superseded_rows += superseded;
        metrics.table_live_rows += (rows as u64).saturating_sub(superseded);
    }
}

/// Leave the cursor on the value the view holds after this step. An upsert exposes the old
/// value (weight -1) before the new one (weight +1).
fn position_at_new_value(cursor: &mut dyn SerCursor) {
    cursor.rewind_vals();
    debug_assert!(cursor.val_valid());
    if cursor.weight() < 0 {
        cursor.step_val();
    }
    debug_assert!(cursor.val_valid());
}

async fn write_rows(
    builder: &mut ArrayBuilder,
    writer: &mut DeltaWriter,
) -> Result<(), WriteError> {
    let batch = builder.to_record_batch().map_err(|e| {
        WriteError::Deterministic(anyhow!("error building an arrow batch of new rows: {e}"))
    })?;
    writer.write(&batch).await.map_err(|e| {
        transient(format!(
            "error writing {} new rows: {e:?}",
            batch.num_rows()
        ))
    })?;
    Ok(())
}

/// Accumulates the keys whose rows must be located, and runs the lookup when it fills.
///
/// Split out from the walk so the walk reads as the algorithm, not as buffer management.
struct KeyChunk {
    writer: Arc<MergeWriter>,
    store: ObjectStoreRef,
    candidates: Arc<Vec<Candidate>>,
    /// Data files read at once, this range's share of `max_concurrent_probes`.
    probes: usize,
    builder: ArrayBuilder,
    buffered: usize,
    chunk: LookupChunk,
    /// Partitions the buffered keys belong to, when partition columns are key columns.
    partitions: Option<PartitionFilter>,
    tombstones: Tombstones,
}

impl KeyChunk {
    fn new(
        writer: Arc<MergeWriter>,
        store: ObjectStoreRef,
        candidates: Arc<Vec<Candidate>>,
        probes: usize,
    ) -> Result<Self, WriteError> {
        Ok(Self {
            builder: ArrayBuilder::new(writer.key_serde_schema.clone()).map_err(|e| {
                WriteError::Deterministic(anyhow!("error creating the key builder: {e}"))
            })?,
            buffered: 0,
            chunk: LookupChunk::new(writer.lookup_chunk_bytes),
            partitions: PartitionFilter::new(
                writer.key_encoder.column_names(),
                &writer.key_arrow_fields,
                &writer.partition_key_columns,
            )
            .map_err(WriteError::Deterministic)?,
            tombstones: Tombstones::new(),
            writer,
            store,
            candidates,
            probes,
        })
    }

    /// Add the key the cursor is on.
    async fn push(
        &mut self,
        cursor: &mut dyn SerCursor,
        metrics: &mut FlushMetrics,
    ) -> Result<(), WriteError> {
        cursor
            .serialize_key_to_arrow(&mut self.builder)
            .map_err(WriteError::Deterministic)?;
        self.buffered += 1;
        metrics.keys_probed += 1;

        if self.buffered >= KEY_BATCH_ROWS {
            self.encode_buffered()?;
            if self.chunk.is_full() {
                self.run_lookup(metrics).await?;
            }
        }
        Ok(())
    }

    /// Encode the remaining keys, run the last lookup, and return what to tombstone.
    async fn finish(mut self, metrics: &mut FlushMetrics) -> Result<Tombstones, WriteError> {
        self.encode_buffered()?;
        if !self.chunk.is_empty() {
            self.run_lookup(metrics).await?;
        }
        Ok(self.tombstones)
    }

    /// Turn the buffered keys into comparable bytes in the chunk.
    ///
    /// Every failure here is deterministic: it is encoding, not I/O.
    fn encode_buffered(&mut self) -> Result<(), WriteError> {
        if self.buffered == 0 {
            return Ok(());
        }
        let batch = self.builder.to_record_batch().map_err(|e| {
            WriteError::Deterministic(anyhow!("error building an arrow batch of keys: {e}"))
        })?;
        let columns = self
            .writer
            .key_encoder
            .columns_of(&batch)
            .map_err(WriteError::Deterministic)?;
        if let Some(filter) = &mut self.partitions {
            filter.record(&columns).map_err(WriteError::Deterministic)?;
        }
        if key::contains_null(&columns) {
            self.chunk.note_null_key();
        }
        let rows = self
            .writer
            .key_encoder
            .encode_columns(&columns)
            .map_err(WriteError::Deterministic)?;
        self.chunk
            .extend(&rows)
            .map_err(WriteError::Deterministic)?;
        self.buffered = 0;
        Ok(())
    }

    async fn run_lookup(&mut self, metrics: &mut FlushMetrics) -> Result<(), WriteError> {
        self.chunk.sort();
        let probed = timed(
            &mut metrics.timings.probe,
            locate(
                &self.chunk,
                &self.candidates,
                self.store.clone(),
                &self.writer.key_encoder,
                self.probes,
                Pruning::new(self.writer.prune_on_stats, self.partitions.as_ref()),
                &mut self.tombstones,
            ),
        )
        .await?;

        metrics.lookup_passes += 1;
        metrics.probe.merge(&probed);
        if probed.keys_not_found > 0 {
            // Not an error: a delete of an absent row is a no-op, an update of one is an
            // insert. A sustained rate means the table has diverged from the view.
            debug!(
                "delta merge mode: {} of {} keys in this lookup pass are not in the table",
                probed.keys_not_found,
                self.chunk.len()
            );
        }
        self.chunk.clear();
        if let Some(filter) = &mut self.partitions {
            filter.clear();
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::array::{Int64Array, RecordBatch, StringArray};
    use deltalake::operations::create::CreateBuilder;
    use deltalake::protocol::SaveMode;
    use deltalake::table::config::TableProperty;
    use serde_json::Value;
    use tempfile::TempDir;

    use super::*;
    use crate::integrated::delta_table::merge::startup::prepare;
    use crate::integrated::delta_table::merge::test::{
        arrow_schema, fixture_columns, key_relation,
    };

    /// Append one row through the merge writer and return the statistics it recorded, with
    /// `delta.dataSkippingNumIndexedCols` set to `num_indexed_cols`.
    async fn stats_of_appended_file(num_indexed_cols: Option<&str>) -> Value {
        let dir = TempDir::new().unwrap();
        let table = CreateBuilder::new()
            .with_location(dir.path().to_str().unwrap())
            .with_save_mode(SaveMode::Ignore)
            .with_columns(fixture_columns())
            .with_configuration_property(TableProperty::EnableDeletionVectors, Some("true"))
            .with_configuration_property(
                TableProperty::DataSkippingNumIndexedCols,
                num_indexed_cols,
            )
            .await
            .unwrap();

        let setup = prepare(&table, &Some(key_relation()), &fixture_columns()).unwrap();
        let schema = Arc::new(arrow_schema());
        let writer = MergeWriter::new(
            setup,
            &key_relation(),
            SerdeArrowSchema::try_from(schema.fields().as_ref()).unwrap(),
            schema.clone(),
            1 << 20,
            1,
            SqlIdentifier::new("v", false),
        )
        .unwrap();

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1i64])),
                Arc::new(StringArray::from(vec!["x"])),
            ],
        )
        .unwrap();

        let mut appends = writer.append_writer(table.object_store());
        appends.write(&batch).await.unwrap();
        let added = appends.close().await.unwrap();

        serde_json::from_str(added[0].stats.as_ref().unwrap()).unwrap()
    }

    /// A connector picking its own column count would make its files skippable on a different
    /// set of columns than every other writer's on the same table.
    #[tokio::test]
    async fn the_writer_follows_the_table_s_statistics_configuration() {
        let stats = stats_of_appended_file(Some("1")).await;
        let mins = stats["minValues"].as_object().unwrap();

        assert!(mins.contains_key("id"), "the first column must be indexed");
        assert!(
            !mins.contains_key("payload"),
            "'delta.dataSkippingNumIndexedCols = 1' asks for one column, got {mins:?}"
        );
    }

    /// Negative control: without the property both columns are indexed, so the test above
    /// reads the property rather than a writer that always stops at one.
    #[tokio::test]
    async fn the_default_configuration_indexes_every_column() {
        let stats = stats_of_appended_file(None).await;
        let mins = stats["minValues"].as_object().unwrap();

        assert!(mins.contains_key("id"));
        assert!(mins.contains_key("payload"));
    }
    /// A transient failure must not hide a deterministic one raised by another range.
    ///
    /// The retry loop fails a deterministic error fast because no attempt can succeed, but
    /// retries a transient one -- for ever, under the default `max_retries: None`. Reporting
    /// the transient error of whichever range happened to be joined first would therefore
    /// rewrite the whole batch on every attempt and orphan its files each time.
    #[test]
    fn a_deterministic_range_failure_outranks_a_transient_one() {
        let transient = || WriteError::Transient(anyhow!("socket"));
        let deterministic = || WriteError::Deterministic(anyhow!("two values for one key"));

        let joined = |first: Option<WriteError>, second: WriteError| {
            matches!(worse(first, second), Some(WriteError::Deterministic(_)))
        };

        // Whichever order the ranges are joined in.
        assert!(joined(Some(transient()), deterministic()));
        assert!(joined(Some(deterministic()), transient()));
        assert!(joined(None, deterministic()));

        // And a flush whose ranges only failed transiently stays retryable.
        assert!(!joined(None, transient()));
        assert!(!joined(Some(transient()), transient()));
    }
}
