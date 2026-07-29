//! Deletion-vector (DV) support for the Delta Lake input connector.
//!
//! A deletion vector marks deleted rows by their *physical position within an
//! immutable Parquet file* — not by any key; the file's live rows are the rest.
//! delta-rs applies DVs during
//! snapshot reads, but the follow/cdc path reads each `Add`/`Remove` action's
//! file directly and must apply the action's DV itself. We decode the DV with
//! `delta_kernel`, which owns the on-disk format, and turn it into a Parquet
//! [`RowSelection`] that drops the deleted rows during decode. The deletion is
//! thus fully applied (deleted rows are never emitted), and memory stays bounded
//! to one batch.

use anyhow::{Result as AnyResult, anyhow};
use arrow::array::{Array, ArrayRef, StructArray, new_null_array};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Fields, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_stream::try_stream;
use datafusion::catalog::TableProvider;
use datafusion::catalog::streaming::StreamingTable;
use datafusion::common::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use delta_kernel::actions::deletion_vector::{
    DeletionVectorDescriptor as KernelDvDescriptor, DeletionVectorStorageType,
};
use deltalake::kernel::{DeletionVectorDescriptor, StorageType};
use deltalake::logstore::LogStore;
use deltalake::{DeltaTable, ObjectStore, Path};
use futures_util::StreamExt;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};
use roaring::RoaringTreemap;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

/// Convert the delta-rs descriptor into its `delta_kernel` equivalent, which
/// owns the decoding logic. The fields are identical; only the storage-type
/// enum differs.
fn to_kernel_descriptor(dv: &DeletionVectorDescriptor) -> KernelDvDescriptor {
    KernelDvDescriptor {
        storage_type: match dv.storage_type {
            StorageType::UuidRelativePath => DeletionVectorStorageType::PersistedRelative,
            StorageType::Inline => DeletionVectorStorageType::Inline,
            StorageType::AbsolutePath => DeletionVectorStorageType::PersistedAbsolute,
        },
        path_or_inline_dv: dv.path_or_inline_dv.clone(),
        offset: dv.offset,
        size_in_bytes: dv.size_in_bytes,
        cardinality: dv.cardinality,
    }
}

/// Decode a deletion vector into the bitmap of deleted row positions.
///
/// `delta_kernel`'s `read` handles all three storage types ("i" inline,
/// "u" relative sidecar, "p" absolute sidecar). It may block on I/O to fetch
/// a sidecar file, so it runs on the blocking pool.
pub(crate) async fn read_deletion_vector(
    dv: &DeletionVectorDescriptor,
    table: &DeltaTable,
) -> AnyResult<RoaringTreemap> {
    let log_store = table.log_store();
    let storage = log_store.engine(None).storage_handler();

    // Sidecar paths resolve via `Url::join`, which drops the last path
    // segment unless the base URL ends with '/'.
    let mut table_root = log_store.config().location().clone();
    if !table_root.path().ends_with('/') {
        table_root.set_path(&format!("{}/", table_root.path()));
    }
    let kernel_dv = to_kernel_descriptor(dv);
    // Displays as the spec string: "u", "i", or "p".
    let storage_str = dv.storage_type.to_string();
    // An inline DV stores the whole bitmap here; truncate for error messages.
    let path_or_inline = abbreviate(&dv.path_or_inline_dv);

    tokio::task::spawn_blocking(move || kernel_dv.read(storage, &table_root))
        .await
        .map_err(|e| {
            anyhow!(
                "deletion vector decode task failed (storageType='{storage_str}', \
                 pathOrInlineDv='{path_or_inline}'): {e}"
            )
        })?
        .map_err(|e| {
            anyhow!(
                "failed to decode deletion vector (storageType='{storage_str}', \
                 pathOrInlineDv='{path_or_inline}'): {e}"
            )
        })
}

/// Shorten `value` to at most 64 characters for use in error messages.
fn abbreviate(value: &str) -> String {
    const MAX_CHARS: usize = 64;
    match value.char_indices().nth(MAX_CHARS) {
        None => value.to_string(),
        Some((cut, _)) => format!("{}…", &value[..cut]),
    }
}

/// Build a [`TableProvider`] over the Parquet file at `path` that reads the rows
/// `mode` picks out of `bitmap` (see [`ReadMode`]).
///
/// The bitmap indexes rows by physical position, so the file is read in order
/// through a single-partition [`StreamingTable`] (a `ListingTable` could split
/// and reorder it). Row selection happens inside the Parquet decoder, so memory
/// stays bounded to one batch.
///
/// `logical_schema` is the table's Arrow schema, restricted by the caller to the
/// columns it wants read. Batches are projected to it by name (missing columns
/// become NULL), and it doubles as the Parquet projection: columns it does not
/// name are never decoded. The caller must restrict it itself, because
/// [`StreamingTable`] does not push projections down.
pub(crate) async fn filtered_parquet_table(
    store: Arc<dyn ObjectStore>,
    path: Path,
    bitmap: RoaringTreemap,
    logical_schema: SchemaRef,
    mode: ReadMode,
) -> AnyResult<Arc<dyn TableProvider>> {
    let partition = MaskedParquetPartition {
        store,
        path,
        bitmap: Arc::new(bitmap),
        schema: Arc::clone(&logical_schema),
        mode,
    };
    let provider = StreamingTable::try_new(logical_schema, vec![Arc::new(partition)])
        .map_err(|e| anyhow!("failed to build DV-filtered streaming table: {e}"))?;
    Ok(Arc::new(provider))
}

/// Which rows [`filtered_parquet_table`] reads, relative to its `bitmap`.
#[derive(Clone, Copy)]
pub(crate) enum ReadMode {
    /// Read only the rows in the bitmap: a deletion-vector delta (the rows a
    /// same-path rewrite masked or un-masked).
    InBitmap,
    /// Read the rows not in the bitmap: apply a deletion vector, so the file's
    /// live rows come through.
    NotInBitmap,
}

impl ReadMode {
    /// Does the read keep the bitmap rows (rather than the rest)?
    fn reads_bitmap_rows(self) -> bool {
        matches!(self, ReadMode::InBitmap)
    }
}

/// Single-partition [`PartitionStream`] that lazily opens a Parquet file and
/// keeps or drops the rows flagged by `bitmap` (per `mode`) as batches flow
/// through.
struct MaskedParquetPartition {
    store: Arc<dyn ObjectStore>,
    path: Path,
    /// Row positions this partition acts on. A `RoaringTreemap` stays compact
    /// even for dense sets, and it is dropped once the partition finishes
    /// streaming, so the footprint is bounded and short-lived.
    bitmap: Arc<RoaringTreemap>,
    /// The Delta logical schema; may differ from the file's own schema under
    /// schema evolution.
    schema: SchemaRef,
    /// Which rows to read, relative to `bitmap`.
    mode: ReadMode,
}

impl fmt::Debug for MaskedParquetPartition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MaskedParquetPartition")
            .field("path", &self.path)
            .field("bitmap_rows", &self.bitmap.len())
            .finish()
    }
}

/// A field's Parquet field id. The data file stamps `PARQUET:field_id`; the Delta
/// read schema carries `delta.columnMapping.id`. Either identifies the same column.
fn field_id(field: &Field) -> Option<&str> {
    field
        .metadata()
        .get("PARQUET:field_id")
        .or_else(|| field.metadata().get("delta.columnMapping.id"))
        .map(String::as_str)
}

/// Index a field list by field id, skipping fields without one.
fn field_index_by_id(fields: &Fields) -> HashMap<&str, usize> {
    fields
        .iter()
        .enumerate()
        .filter_map(|(i, f)| field_id(f).map(|id| (id, i)))
        .collect()
}

fn conversion_error(
    file: &str,
    column: &str,
    from: &DataType,
    to: &DataType,
    cause: impl fmt::Display,
) -> DataFusionError {
    DataFusionError::External(
        format!(
            "Delta file reader: cannot read column '{column}' of file '{file}': the file stores \
             it as {from:?}, which is not convertible to the {to:?} that the Delta table's \
             schema declares: {cause}"
        )
        .into(),
    )
}

/// Convert a file column `array` to the `target` type the read schema expects.
/// `file` and `column` (dotted for a nested child) locate it in errors.
///
/// Under column mapping a struct's field names differ between the file and the
/// schema (a file may use logical names, the schema uses `col-<id>`), so a struct
/// is rebuilt: each target child takes the source child with the same field id, or
/// the child at the same position when neither side carries an id (unmapped). A
/// target child the file lacks is null-filled, matching how [`project_to_logical`]
/// handles a missing top-level column. Non-struct types (scalars, lists, maps)
/// have no such names to match, so `cast` handles them, including type and
/// container differences like `List` vs `LargeList`.
fn realign_array(
    array: &ArrayRef,
    target: &DataType,
    file: &str,
    column: &str,
) -> Result<ArrayRef, DataFusionError> {
    let cast_to_target = || {
        cast(array, target)
            .map_err(|e| conversion_error(file, column, array.data_type(), target, e))
    };
    let DataType::Struct(target_fields) = target else {
        return if array.data_type() == target {
            Ok(Arc::clone(array))
        } else {
            cast_to_target()
        };
    };
    let Some(source) = array.as_any().downcast_ref::<StructArray>() else {
        return cast_to_target();
    };
    let src_idx_by_id = field_index_by_id(source.fields());
    let children = target_fields
        .iter()
        .enumerate()
        .map(|(pos, tf)| {
            // With an id, match by id only: falling back to position would risk
            // grabbing an unrelated column. Without one (unmapped), use position.
            let idx = match field_id(tf) {
                Some(id) => src_idx_by_id.get(id).copied(),
                None => Some(pos),
            };
            match idx.and_then(|i| source.columns().get(i)) {
                Some(child) => realign_array(
                    child,
                    tf.data_type(),
                    file,
                    &format!("{column}.{}", tf.name()),
                ),
                None => Ok(new_null_array(tf.data_type(), source.len())),
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    // The children match `target_fields` by construction, so this rejects only a
    // null-filled child of a NOT NULL target field, i.e. a column the file lacks
    // that the table requires.
    StructArray::try_new(target_fields.clone(), children, source.nulls().cloned())
        .map(|s| Arc::new(s) as ArrayRef)
        .map_err(|e| conversion_error(file, column, array.data_type(), target, e))
}

/// Project `batch` onto `logical_schema`, matching columns by field id (falling
/// back to name), rebuilding nested shapes and casting leaves, and null-filling
/// columns the file lacks. Field-id matching handles `columnMapping.mode=id`
/// tables, whose files name columns logically rather than by physical `col-<id>`.
///
/// Partition columns come out NULL (Delta stores them in `partitionValues`, not
/// in the file), a pre-existing limitation of the connector's Parquet reader.
fn project_to_logical(
    batch: &RecordBatch,
    logical_schema: &SchemaRef,
    file: &str,
) -> Result<RecordBatch, DataFusionError> {
    let num_rows = batch.num_rows();
    let file_schema = batch.schema();
    let file_idx_by_id = field_index_by_id(file_schema.fields());
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(logical_schema.fields().len());
    for field in logical_schema.fields().iter() {
        let source = field_id(field)
            .and_then(|id| file_idx_by_id.get(id).copied())
            .or_else(|| file_schema.index_of(field.name()).ok());
        let col = match source {
            Some(idx) => realign_array(batch.column(idx), field.data_type(), file, field.name())?,
            None => new_null_array(field.data_type(), num_rows),
        };
        columns.push(col);
    }
    RecordBatch::try_new(Arc::clone(logical_schema), columns).map_err(|e| {
        DataFusionError::External(
            format!(
                "Delta file reader: file '{file}' does not satisfy the Delta table's schema: {e}. \
                 A column the file lacks is read as NULL, which the table rejects when it \
                 declares the column NOT NULL."
            )
            .into(),
        )
    })
}

/// Build the [`ProjectionMask`] selecting the root file columns `logical_schema`
/// wants, matched by field id (falling back to name); the rest are never decoded.
fn logical_projection_mask(
    builder: &ParquetRecordBatchStreamBuilder<ParquetObjectReader>,
    logical_schema: &SchemaRef,
) -> ProjectionMask {
    let want_ids: HashSet<&str> = logical_schema
        .fields()
        .iter()
        .filter_map(|f| field_id(f))
        .collect();
    let roots = builder
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| {
            field_id(field).is_some_and(|id| want_ids.contains(id))
                || logical_schema.column_with_name(field.name()).is_some()
        })
        .map(|(idx, _)| idx);
    ProjectionMask::roots(builder.parquet_schema(), roots)
}

/// Append a [`RowSelector`] for `count` rows to `selectors`, selecting or
/// skipping them per `select`, merging into the previous selector when it is
/// the same kind. A zero-length selector is a no-op. `select`/skip is parquet's
/// [`RowSelector`] vocabulary, one level below [`ReadMode`].
fn push_selector(selectors: &mut Vec<RowSelector>, select: bool, count: u64) {
    if count == 0 {
        return;
    }
    let count = count as usize;
    match selectors.last_mut() {
        Some(last) if last.skip != select => last.row_count += count,
        _ if select => selectors.push(RowSelector::select(count)),
        _ => selectors.push(RowSelector::skip(count)),
    }
}

/// Build the [`RowSelection`] over `0..total_rows` that `mode` implies for
/// `bitmap`. Positions past `total_rows` are ignored.
///
/// The result holds one selector per run of like rows, so it is O(runs): small
/// when the bitmap is clustered, O(bitmap) only when bitmap rows alternate with
/// the rest. It is built per file, consumed to start the Parquet stream, then
/// dropped.
fn bitmap_to_selection(bitmap: &RoaringTreemap, total_rows: u64, mode: ReadMode) -> RowSelection {
    // A bitmap row is selected exactly when `mode` reads bitmap rows; the gaps
    // between them are selected in the other case.
    let select_bitmap_rows = mode.reads_bitmap_rows();
    let mut selectors: Vec<RowSelector> = Vec::new();
    let mut cursor: u64 = 0;
    for pos in bitmap.iter().take_while(|&pos| pos < total_rows) {
        push_selector(&mut selectors, !select_bitmap_rows, pos - cursor); // gap before this row
        push_selector(&mut selectors, select_bitmap_rows, 1); // the bitmap row itself
        cursor = pos + 1;
    }
    push_selector(&mut selectors, !select_bitmap_rows, total_rows - cursor); // trailing gap
    RowSelection::from(selectors)
}

/// `PartitionStream` is DataFusion's lazy single-partition row source;
/// `StreamingTable` drives one per partition, calling `execute` to start the
/// batch stream. Ours opens the Parquet file and masks deleted rows on the fly.
impl PartitionStream for MaskedParquetPartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let store = Arc::clone(&self.store);
        let path = self.path.clone();
        let bitmap = Arc::clone(&self.bitmap);
        let logical_schema = Arc::clone(&self.schema);
        let mode = self.mode;

        let stream = try_stream! {
            let reader = ParquetObjectReader::new(store, path.clone());
            let builder = ParquetRecordBatchStreamBuilder::new(reader)
                .await
                .map_err(|e| DataFusionError::External(
                    format!("failed to open Parquet file '{path}': {e}").into()))?;
            // `num_rows()` is `i64` because Parquet's metadata is signed
            // throughout; it is non-negative for any file whose footer parsed
            // (which it did, just above).
            let total_rows = builder.metadata().file_metadata().num_rows() as u64;
            // Decode only the columns the logical schema names.
            let mask = logical_projection_mask(&builder, &logical_schema);
            // Pick rows inside the decoder: skip the flagged rows (apply a DV) or
            // keep only them (read a DV delta).
            let selection = bitmap_to_selection(&bitmap, total_rows, mode);
            let mut parquet_stream = builder
                .with_projection(mask)
                .with_row_selection(selection)
                .build()
                .map_err(|e| DataFusionError::External(
                    format!("failed to build Parquet stream for '{path}': {e}").into()))?;

            while let Some(batch) = parquet_stream.next().await {
                let batch = batch.map_err(|e| DataFusionError::External(
                    format!("error reading Parquet file '{path}': {e}").into()))?;
                if batch.num_rows() > 0 {
                    yield project_to_logical(&batch, &logical_schema, path.as_ref())?;
                }
            }
        };

        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int32Array, Int64Array, StringArray, StructArray};
    use arrow::datatypes::{
        DataType as ArrowDataType, Field as ArrowField, Fields as ArrowFields,
        Schema as ArrowSchema,
    };
    use datafusion::prelude::SessionContext;
    use deltalake::{DeltaTableBuilder, ensure_table_uri};
    use proptest::prelude::*;
    use tempfile::TempDir;

    /// Stands in for the data file the reader is decoding; it appears in errors.
    const TEST_FILE: &str = "part-00000.parquet";

    fn with_id(field: ArrowField, key: &str, id: &str) -> ArrowField {
        field.with_metadata(HashMap::from([(key.to_string(), id.to_string())]))
    }

    // The read schema's list kind may differ from the file's (Delta `List` vs a
    // file's `LargeList`); realign must coerce the container instead of failing.
    #[test]
    fn realign_array_coerces_list_containers() {
        use arrow::array::{LargeListBuilder, StringBuilder};
        let mut b = LargeListBuilder::new(StringBuilder::new());
        b.values().append_value("a");
        b.values().append_value("b");
        b.append(true);
        b.values().append_value("c");
        b.append(true);
        let source: ArrayRef = Arc::new(b.finish());

        let target =
            ArrowDataType::List(Arc::new(ArrowField::new("item", ArrowDataType::Utf8, true)));
        let out = realign_array(&source, &target, TEST_FILE, "items").unwrap();
        assert_eq!(out.data_type(), &target);
        assert_eq!(out.len(), 2);
    }

    // A columnMapping.mode=id file names columns logically (`op`, `after`) and
    // carries `PARQUET:field_id`; the Delta read schema uses physical `col-<id>`
    // names and `delta.columnMapping.id`. project_to_logical must pair them by
    // field id, not name, else it null-fills and drops the data.
    #[test]
    fn project_to_logical_matches_by_field_id() {
        let file_after: ArrayRef = Arc::new(StructArray::from(vec![(
            Arc::new(with_id(
                ArrowField::new("transaction__id", ArrowDataType::Utf8, true),
                "PARQUET:field_id",
                "2",
            )),
            Arc::new(StringArray::from(vec!["t1"])) as ArrayRef,
        )]));
        let file_op: ArrayRef = Arc::new(StringArray::from(vec!["INSERT"]));
        let file_schema = Arc::new(ArrowSchema::new(vec![
            with_id(
                ArrowField::new("after", file_after.data_type().clone(), true),
                "PARQUET:field_id",
                "1",
            ),
            with_id(
                ArrowField::new("op", ArrowDataType::Utf8, false),
                "PARQUET:field_id",
                "8",
            ),
        ]));
        let batch = RecordBatch::try_new(file_schema, vec![file_after, file_op]).unwrap();

        let read_schema = Arc::new(ArrowSchema::new(vec![
            with_id(
                ArrowField::new(
                    "col-1",
                    ArrowDataType::Struct(ArrowFields::from(vec![with_id(
                        ArrowField::new("col-2", ArrowDataType::Utf8, true),
                        "delta.columnMapping.id",
                        "2",
                    )])),
                    true,
                ),
                "delta.columnMapping.id",
                "1",
            ),
            with_id(
                ArrowField::new("col-8", ArrowDataType::Utf8, false),
                "delta.columnMapping.id",
                "8",
            ),
        ]));

        let out = project_to_logical(&batch, &read_schema, TEST_FILE).unwrap();
        assert_eq!(out.schema().field(1).name(), "col-8");
        let op = out
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            op.value(0),
            "INSERT",
            "op resolved by field id, not null-filled"
        );
        let after = out
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(
            after
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "t1"
        );
    }

    // A struct child the file lacks (e.g. a field added to the struct after the
    // file was written) must null-fill, not error or grab a wrong-id sibling.
    #[test]
    fn realign_array_null_fills_missing_struct_child() {
        let source: ArrayRef = Arc::new(StructArray::from(vec![(
            Arc::new(with_id(
                ArrowField::new("id", ArrowDataType::Utf8, true),
                "PARQUET:field_id",
                "2",
            )),
            Arc::new(StringArray::from(vec!["t1", "t2"])) as ArrayRef,
        )]));

        // Target wants both id 2 (present) and id 3 (absent from the file).
        let target = ArrowDataType::Struct(ArrowFields::from(vec![
            with_id(
                ArrowField::new("col-2", ArrowDataType::Utf8, true),
                "delta.columnMapping.id",
                "2",
            ),
            with_id(
                ArrowField::new("col-3", ArrowDataType::Utf8, true),
                "delta.columnMapping.id",
                "3",
            ),
        ]));

        let out = realign_array(&source, &target, TEST_FILE, "after").unwrap();
        let out = out.as_any().downcast_ref::<StructArray>().unwrap();
        let present = out
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(present.value(0), "t1");
        let missing = out
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(missing.len(), 2);
        assert!(missing.is_null(0) && missing.is_null(1));
    }

    // A user hitting a type mismatch sees only the physical `col-<uuid>` name on
    // disk, so the error has to name the column, the file, and both types. Arrow's
    // bare cast error carries none of that.
    #[test]
    fn conversion_error_names_column_file_and_types() {
        let file_child: ArrayRef = Arc::new(StringArray::from(vec!["not-a-timestamp"]));
        let source: ArrayRef = Arc::new(StructArray::from(vec![(
            Arc::new(with_id(
                ArrowField::new("amount", ArrowDataType::Utf8, true),
                "PARQUET:field_id",
                "2",
            )),
            file_child,
        )]));
        // Utf8 to a fixed-size binary is not a cast Arrow supports.
        let target = ArrowDataType::Struct(ArrowFields::from(vec![with_id(
            ArrowField::new("col-2", ArrowDataType::FixedSizeBinary(16), true),
            "delta.columnMapping.id",
            "2",
        )]));

        let err = realign_array(&source, &target, TEST_FILE, "after")
            .expect_err("Utf8 does not cast to FixedSizeBinary")
            .to_string();

        // The nested child, not just the top-level column.
        assert!(err.contains("'after.col-2'"), "{err}");
        assert!(err.contains(TEST_FILE), "{err}");
        assert!(err.contains("Utf8"), "{err}");
        assert!(err.contains("FixedSizeBinary(16)"), "{err}");
    }

    /// Expand a [`RowSelection`] into the row positions it selects.
    fn selected_rows(selection: &RowSelection) -> Vec<u64> {
        let mut rows = Vec::new();
        let mut pos: u64 = 0;
        for selector in selection.iter() {
            if !selector.skip {
                rows.extend(pos..pos + selector.row_count as u64);
            }
            pos += selector.row_count as u64;
        }
        rows
    }

    /// Rows in `0..total_rows` that are not in `deleted`.
    fn expected_rows(deleted: &[u64], total_rows: u64) -> Vec<u64> {
        (0..total_rows)
            .filter(|row| !deleted.contains(row))
            .collect()
    }

    fn check(deleted: &[u64], total_rows: u64) {
        let bitmap = RoaringTreemap::from_iter(deleted.iter().copied());
        let selection = bitmap_to_selection(&bitmap, total_rows, ReadMode::NotInBitmap);
        assert_eq!(
            selected_rows(&selection),
            expected_rows(deleted, total_rows)
        );
    }

    #[test]
    fn empty_bitmap_selects_everything() {
        check(&[], 0);
        check(&[], 10);
    }

    #[test]
    fn leading_and_trailing_deletes() {
        check(&[0], 5);
        check(&[4], 5);
        check(&[0, 4], 5);
    }

    #[test]
    fn contiguous_run_merges_into_one_skip() {
        check(&[2, 3, 4], 10);
        let bitmap = RoaringTreemap::from_iter([2u64, 3, 4]);
        let selection = bitmap_to_selection(&bitmap, 10, ReadMode::NotInBitmap);
        // select(2), skip(3), select(5)
        assert_eq!(selection.iter().count(), 3);
    }

    #[test]
    fn all_rows_deleted_selects_nothing() {
        check(&[0, 1, 2], 3);
    }

    #[test]
    fn positions_past_eof_are_ignored() {
        check(&[1, 7, 100], 5);
        check(&[5], 5);
    }

    proptest! {
        /// The selection must pick exactly the complement of the bitmap.
        #[test]
        fn selection_is_complement_of_bitmap(
            deleted in proptest::collection::btree_set(0u64..200, 0..50),
            total_rows in 0u64..200,
        ) {
            let deleted: Vec<u64> = deleted.into_iter().collect();
            check(&deleted, total_rows);
        }
    }

    /// Rows in `0..total_rows` that *are* in `kept`.
    fn expected_kept_rows(kept: &[u64], total_rows: u64) -> Vec<u64> {
        (0..total_rows).filter(|row| kept.contains(row)).collect()
    }

    fn check_keep(kept: &[u64], total_rows: u64) {
        let bitmap = RoaringTreemap::from_iter(kept.iter().copied());
        let selection = bitmap_to_selection(&bitmap, total_rows, ReadMode::InBitmap);
        assert_eq!(
            selected_rows(&selection),
            expected_kept_rows(kept, total_rows)
        );
    }

    #[test]
    fn keep_selection_edge_cases() {
        check_keep(&[], 0);
        check_keep(&[], 10); // keep nothing
        check_keep(&[0], 5); // leading
        check_keep(&[4], 5); // trailing
        check_keep(&[1, 2, 3], 10); // contiguous run
        check_keep(&[0, 1, 2], 3); // keep everything
        check_keep(&[1, 7, 100], 5); // positions past EOF ignored
    }

    proptest! {
        /// The keep-selection picks exactly the bitmap, and is the complement of
        /// the skip-selection over the same bitmap.
        #[test]
        fn keep_selection_is_the_bitmap(
            kept in proptest::collection::btree_set(0u64..200, 0..50),
            total_rows in 0u64..200,
        ) {
            let kept: Vec<u64> = kept.into_iter().collect();
            check_keep(&kept, total_rows);

            let bitmap = RoaringTreemap::from_iter(kept.iter().copied());
            let keep = selected_rows(&bitmap_to_selection(&bitmap, total_rows, ReadMode::InBitmap));
            let skip = selected_rows(&bitmap_to_selection(&bitmap, total_rows, ReadMode::NotInBitmap));
            let mut union: Vec<u64> = keep.iter().chain(skip.iter()).copied().collect();
            union.sort_unstable();
            prop_assert!(keep.iter().all(|r| !skip.contains(r)));
            prop_assert_eq!(union, (0..total_rows).collect::<Vec<_>>());
        }
    }

    /// Every delta-rs storage type maps to its `delta_kernel` counterpart and
    /// the remaining descriptor fields are copied verbatim. The decode itself
    /// is `delta_kernel`'s, but this conversion is ours, so it is tested here:
    /// the `"i"`/`"p"` arms are otherwise never hit by the Spark fixtures,
    /// which only produce `"u"`.
    #[test]
    fn to_kernel_descriptor_maps_all_storage_types() {
        use deltalake::kernel::{DeletionVectorDescriptor, StorageType};

        let cases = [
            (
                StorageType::UuidRelativePath,
                DeletionVectorStorageType::PersistedRelative,
            ),
            (StorageType::Inline, DeletionVectorStorageType::Inline),
            (
                StorageType::AbsolutePath,
                DeletionVectorStorageType::PersistedAbsolute,
            ),
        ];

        for (storage_type, expected) in cases {
            let dv = DeletionVectorDescriptor {
                storage_type,
                path_or_inline_dv: "vBn[lQ{`".to_string(),
                offset: Some(1),
                size_in_bytes: 34,
                cardinality: 2,
            };
            let kernel = to_kernel_descriptor(&dv);

            assert_eq!(kernel.storage_type, expected);
            assert_eq!(kernel.path_or_inline_dv, dv.path_or_inline_dv);
            assert_eq!(kernel.offset, dv.offset);
            assert_eq!(kernel.size_in_bytes, dv.size_in_bytes);
            assert_eq!(kernel.cardinality, dv.cardinality);
        }
    }

    // DV *decoding* is `delta_kernel`'s code and is tested upstream for all
    // three storage types ("u"/"i"/"p"), so there are no decode tests here;
    // [`read_deletion_vector`] adds only descriptor conversion and URL fixup,
    // which the Spark-driven Python e2e tests exercise. What follows tests
    // the masked Parquet reader, which is entirely ours.

    /// A `DeltaTable` rooted at `dir`, built without loading a log: the test
    /// needs only the table's local object store, so the directory does not
    /// have to hold a Delta table at all.
    fn unloaded_table(dir: &std::path::Path) -> DeltaTable {
        DeltaTableBuilder::from_url(ensure_table_uri(dir.to_str().unwrap()).unwrap())
            .unwrap()
            .build()
            .unwrap()
    }

    /// End-to-end check of [`filtered_parquet_table`] with [`ReadMode::NotInBitmap`]
    /// against a real Parquet file: DV-flagged rows are skipped inside the
    /// decoder, and the logical schema drives the read. A file column it omits is
    /// pruned, a column it widens is cast (`Int32` to `Int64`), and a column the
    /// file lacks comes back NULL (schema evolution).
    #[tokio::test]
    async fn masked_reader_applies_dv_and_logical_schema() {
        const TOTAL_ROWS: usize = 200;
        let dir = TempDir::new().unwrap();

        // File schema: `id` (narrower than the logical type) plus a `payload`
        // column the logical schema omits.
        let file_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("payload", ArrowDataType::Utf8, false),
        ]));
        let ids = Int32Array::from_iter_values(0..TOTAL_ROWS as i32);
        let payloads = StringArray::from_iter_values((0..TOTAL_ROWS).map(|i| format!("row_{i}")));
        let batch = RecordBatch::try_new(
            Arc::clone(&file_schema),
            vec![Arc::new(ids), Arc::new(payloads)],
        )
        .unwrap();
        let file = std::fs::File::create(dir.path().join("data.parquet")).unwrap();
        let mut writer = parquet::arrow::ArrowWriter::try_new(file, file_schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let logical = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int64, true),
            ArrowField::new("added_later", ArrowDataType::Utf8, true),
        ]));
        let deleted = RoaringTreemap::from_iter((0..TOTAL_ROWS as u64).filter(|i| i % 2 == 0));

        let store = unloaded_table(dir.path()).log_store().object_store(None);
        let provider = filtered_parquet_table(
            store,
            Path::from("data.parquet"),
            deleted,
            Arc::clone(&logical),
            ReadMode::NotInBitmap,
        )
        .await
        .unwrap();
        let batches = SessionContext::new()
            .read_table(provider)
            .unwrap()
            .collect()
            .await
            .unwrap();

        let mut got: Vec<i64> = Vec::new();
        for batch in &batches {
            assert_eq!(
                batch.schema().as_ref(),
                logical.as_ref(),
                "batch schema must equal the declared logical schema"
            );
            let id = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            got.extend(id.iter().map(|v| v.unwrap()));
            assert_eq!(
                batch.column(1).null_count(),
                batch.num_rows(),
                "the column absent from the file must be all NULL"
            );
        }
        got.sort();
        let expected: Vec<i64> = (0..TOTAL_ROWS as i64).filter(|i| i % 2 != 0).collect();
        assert_eq!(got, expected, "masked rows mismatch");
    }

    /// End-to-end check of [`filtered_parquet_table`] with [`ReadMode::InBitmap`]:
    /// the inverse of the masked reader. Given a bitmap of row positions (a
    /// deletion-vector delta), it reads *only* those rows and applies the logical
    /// schema, so a K-row change costs K rows. Uses the same file/schema shape as
    /// the masked-reader test.
    #[tokio::test]
    async fn selected_reader_reads_only_the_bitmap_rows() {
        const TOTAL_ROWS: usize = 200;
        let dir = TempDir::new().unwrap();

        let file_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("payload", ArrowDataType::Utf8, false),
        ]));
        let ids = Int32Array::from_iter_values(0..TOTAL_ROWS as i32);
        let payloads = StringArray::from_iter_values((0..TOTAL_ROWS).map(|i| format!("row_{i}")));
        let batch = RecordBatch::try_new(
            Arc::clone(&file_schema),
            vec![Arc::new(ids), Arc::new(payloads)],
        )
        .unwrap();
        let file = std::fs::File::create(dir.path().join("data.parquet")).unwrap();
        let mut writer = parquet::arrow::ArrowWriter::try_new(file, file_schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let logical = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int64, true),
            ArrowField::new("added_later", ArrowDataType::Utf8, true),
        ]));
        // A sparse, clustered set spanning both ends, so runs and gaps are exercised.
        let wanted: Vec<u64> = [0u64, 1, 2, 50, 51, 199].into_iter().collect();
        let selected = RoaringTreemap::from_iter(wanted.iter().copied());

        let store = unloaded_table(dir.path()).log_store().object_store(None);
        let provider = filtered_parquet_table(
            store,
            Path::from("data.parquet"),
            selected,
            Arc::clone(&logical),
            ReadMode::InBitmap,
        )
        .await
        .unwrap();
        let batches = SessionContext::new()
            .read_table(provider)
            .unwrap()
            .collect()
            .await
            .unwrap();

        let mut got: Vec<i64> = Vec::new();
        for batch in &batches {
            assert_eq!(batch.schema().as_ref(), logical.as_ref());
            let id = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            got.extend(id.iter().map(|v| v.unwrap()));
            assert_eq!(
                batch.column(1).null_count(),
                batch.num_rows(),
                "the column absent from the file must be all NULL"
            );
        }
        got.sort();
        let expected: Vec<i64> = wanted.iter().map(|&r| r as i64).collect();
        assert_eq!(got, expected, "must read exactly the bitmap rows");
    }
}
