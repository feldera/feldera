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
use std::sync::Arc;

use anyhow::{Result as AnyResult, anyhow};
use arrow::datatypes::{Field as ArrowField, Schema as ArrowSchema};
use delta_kernel::expressions::Scalar;
use deltalake::DeltaTable;
use deltalake::kernel::transaction::{CommitBuilder, CommitProperties, TableReference};
use deltalake::kernel::{Action, Add, LogicalFileView};
use deltalake::logstore::ObjectStoreRef;
use deltalake::operations::write::writer::{DeltaWriter, WriterConfig};
use deltalake::protocol::{DeltaOperation, SaveMode};
use serde_arrow::ArrayBuilder;
use serde_arrow::schema::SerdeArrowSchema;
use tracing::debug;

use crate::catalog::SerCursor;
use crate::util::{IndexedOperationType, indexed_operation_type};
use feldera_types::program_schema::{Relation, SqlIdentifier};

use super::super::output::TARGET_FILE_SIZE;
use super::chunk::LookupChunk;
use super::key::{self, KeyEncoder};
use super::probe::Pruning;
use super::probe::{Candidate, ProbeMetrics, locate};
use super::prune::PartitionFilter;
use super::startup::{MergeSetup, Regime, StatsConfig};
use super::tombstone::{DvWriteMetrics, Tombstones, write_deletion_vectors};

/// Rows buffered in the append writer before a chunk is written out.
const APPEND_CHUNK_ROWS: usize = 100_000;

/// Keys buffered before they are encoded into the lookup chunk. Encoding works on an arrow
/// batch, so keys are gathered into one first.
const KEY_BATCH_ROWS: usize = 8192;

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
            index_name: key_schema.name.clone(),
        })
    }

    /// Apply one batch to `table` and commit, refreshing the table to the new version.
    ///
    /// The caller rebuilds `cursor` on every attempt, because a retry has to re-run the lookup
    /// against whatever paths exist now.
    ///
    /// `retrying` must be set on every attempt after the first. A commit error can mean the
    /// commit landed and only its response was lost, so the rows an earlier attempt appended
    /// may be in the table: [`Regime::Owned`]'s insert shortcut is unsound from then on, and
    /// skipping the lookup would leave two live rows for one key.
    pub async fn flush(
        &self,
        table: &mut DeltaTable,
        object_store: ObjectStoreRef,
        cursor: &mut dyn SerCursor,
        retrying: bool,
        on_uniqueness_violation: &mut dyn FnMut(anyhow::Error),
    ) -> AnyResult<FlushMetrics> {
        let mut metrics = FlushMetrics::default();

        // The snapshot the lookup runs against. The commit declares it as its read version,
        // so a conflicting change to these files is caught rather than overwritten.
        let candidates = self.snapshot_files(table)?;

        let mut appends = self.append_writer(object_store);
        let mut rows = ArrayBuilder::new(self.row_serde_schema.clone())
            .map_err(|e| anyhow!("error creating the row builder: {e}"))?;
        let mut keys = KeyChunk::new(self, table, &candidates)?;
        let mut buffered_rows = 0;

        while cursor.key_valid() {
            let op = match indexed_operation_type(&self.view_name, &self.index_name, cursor) {
                Ok(op) => op,
                Err(e) => {
                    // A key with two values has no single row to write. Skip it and let the
                    // controller report it, as cdc mode does.
                    on_uniqueness_violation(e);
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
                cursor.serialize_val_to_arrow(&mut rows)?;
                buffered_rows += 1;
                metrics.rows_appended += 1;

                if buffered_rows >= APPEND_CHUNK_ROWS {
                    write_rows(&mut rows, &mut appends).await?;
                    buffered_rows = 0;
                }
            }

            cursor.step_key();
        }

        if buffered_rows > 0 {
            write_rows(&mut rows, &mut appends).await?;
        }
        let tombstones = keys.finish(&mut metrics).await?;

        let added = appends
            .close()
            .await
            .map_err(|e| anyhow!("error closing the Delta writer: {e:?}"))?;
        metrics.files_appended = added.len();
        metrics.bytes_written = added.iter().map(|a| a.size.max(0) as u64).sum();

        self.commit(table, added, tombstones, &mut metrics).await?;
        count_table_rows(table, &mut metrics);
        Ok(metrics)
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
    fn snapshot_files(&self, table: &DeltaTable) -> AnyResult<Vec<Candidate>> {
        let snapshot = table
            .snapshot()
            .map_err(|e| anyhow!("unable to read the Delta table snapshot: {e}"))?;

        Ok(snapshot
            .log_data()
            .into_iter()
            .map(|file| {
                Candidate::from_log(&file, self.partition_keys_of(&file), self.prune_on_stats)
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
    ) -> AnyResult<()> {
        let dv = write_deletion_vectors(&tombstones, table).await?;
        metrics.dv = dv.metrics;
        metrics.bytes_written += dv.metrics.dv_bytes as u64;

        let mut actions: Vec<Action> = added.into_iter().map(Action::Add).collect();
        actions.extend(dv.actions);

        if actions.is_empty() {
            return Ok(());
        }

        commit(table, actions).await
    }
}

/// Commit a flush's actions and advance the table to the committed version.
///
/// The table is deliberately *not* refreshed first: keeping the lookup's snapshot as the
/// commit's read version is what lets delta-rs tell a concurrent append it can reorder from a
/// compaction that replaced the files this flush addressed. The second comes back as a
/// conflict and the flush retries from the top.
async fn commit(table: &mut DeltaTable, actions: Vec<Action>) -> AnyResult<()> {
    let read_snapshot = table.state.as_ref().map(|s| s as &dyn TableReference);

    let finalized = CommitBuilder::from(CommitProperties::default())
        .with_actions(actions)
        .build(
            read_snapshot,
            table.log_store(),
            DeltaOperation::Write {
                mode: SaveMode::Append,
                partition_by: None,
                predicate: None,
            },
        )
        .await
        .map_err(|e| {
            anyhow!(
                "error committing to the Delta table (read version: {:?}): {e:?}",
                table.version()
            )
        })?;

    // The next flush looks up rows in this snapshot, so it must include what this one
    // appended; otherwise an update to a row inserted here would duplicate it.
    table.state = Some(finalized.snapshot);
    Ok(())
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

async fn write_rows(builder: &mut ArrayBuilder, writer: &mut DeltaWriter) -> AnyResult<()> {
    let batch = builder
        .to_record_batch()
        .map_err(|e| anyhow!("error building an arrow batch of new rows: {e}"))?;
    writer
        .write(&batch)
        .await
        .map_err(|e| anyhow!("error writing {} new rows: {e:?}", batch.num_rows()))?;
    Ok(())
}

/// Accumulates the keys whose rows must be located, and runs the lookup when it fills.
///
/// Split out from the walk so the walk reads as the algorithm, not as buffer management.
struct KeyChunk<'a> {
    writer: &'a MergeWriter,
    table: &'a DeltaTable,
    candidates: &'a [Candidate],
    builder: ArrayBuilder,
    buffered: usize,
    chunk: LookupChunk,
    /// Partitions the buffered keys belong to, when partition columns are key columns.
    partitions: Option<PartitionFilter>,
    tombstones: Tombstones,
}

impl<'a> KeyChunk<'a> {
    fn new(
        writer: &'a MergeWriter,
        table: &'a DeltaTable,
        candidates: &'a [Candidate],
    ) -> AnyResult<Self> {
        Ok(Self {
            writer,
            table,
            candidates,
            builder: ArrayBuilder::new(writer.key_serde_schema.clone())
                .map_err(|e| anyhow!("error creating the key builder: {e}"))?,
            buffered: 0,
            chunk: LookupChunk::new(writer.lookup_chunk_bytes),
            partitions: PartitionFilter::new(
                writer.key_encoder.column_names(),
                &writer.key_arrow_fields,
                &writer.partition_key_columns,
            )?,
            tombstones: Tombstones::new(),
        })
    }

    /// Add the key the cursor is on.
    async fn push(
        &mut self,
        cursor: &mut dyn SerCursor,
        metrics: &mut FlushMetrics,
    ) -> AnyResult<()> {
        cursor.serialize_key_to_arrow(&mut self.builder)?;
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
    async fn finish(mut self, metrics: &mut FlushMetrics) -> AnyResult<Tombstones> {
        self.encode_buffered()?;
        if !self.chunk.is_empty() {
            self.run_lookup(metrics).await?;
        }
        Ok(self.tombstones)
    }

    /// Turn the buffered keys into comparable bytes in the chunk.
    fn encode_buffered(&mut self) -> AnyResult<()> {
        if self.buffered == 0 {
            return Ok(());
        }
        let batch = self
            .builder
            .to_record_batch()
            .map_err(|e| anyhow!("error building an arrow batch of keys: {e}"))?;
        let columns = self.writer.key_encoder.columns_of(&batch)?;
        if let Some(filter) = &mut self.partitions {
            filter.record(&columns)?;
        }
        if key::contains_null(&columns) {
            self.chunk.note_null_key();
        }
        let rows = self.writer.key_encoder.encode_columns(&columns)?;
        self.chunk.extend(&rows)?;
        self.buffered = 0;
        Ok(())
    }

    async fn run_lookup(&mut self, metrics: &mut FlushMetrics) -> AnyResult<()> {
        self.chunk.sort();
        let probed = locate(
            &self.chunk,
            self.candidates,
            self.table,
            &self.writer.key_encoder,
            self.writer.max_concurrent_probes,
            Pruning::new(self.writer.prune_on_stats, self.partitions.as_ref()),
            &mut self.tombstones,
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

        let setup = prepare(&table, &Some(key_relation()), &fixture_columns(), 1).unwrap();
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
}
