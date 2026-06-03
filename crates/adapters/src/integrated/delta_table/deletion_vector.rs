//! Deletion-vector (DV) support for the Delta Lake input connector.
//!
//! delta-rs applies DVs during snapshot reads, but the follow/cdc path reads
//! each `Add`/`Remove` action's Parquet file directly and must apply the
//! action's DV itself. We decode the DV with `delta_kernel`, which owns the
//! on-disk format, and turn it into a Parquet [`RowSelection`], so deleted
//! rows are skipped during decode and memory stays bounded to one batch.

use anyhow::{Result as AnyResult, anyhow};
use arrow::array::{ArrayRef, new_null_array};
use arrow::compute::cast;
use arrow::datatypes::SchemaRef;
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

/// Build a [`TableProvider`] over the Parquet file at `path` that skips the
/// rows flagged by `bitmap`.
///
/// The bitmap indexes rows by physical position, so the file is read in
/// order through a single-partition [`StreamingTable`] (a `ListingTable`
/// could split and reorder it). The Parquet decoder skips deleted rows via a
/// [`RowSelection`]; memory stays bounded to one batch.
///
/// `logical_schema` is the table's Arrow schema, optionally restricted to
/// the columns the caller reads. Batches are projected to it by name
/// (missing columns become NULL), and it doubles as the Parquet projection:
/// columns it does not name are never decoded. Restrict it up front, because
/// [`StreamingTable`] does not push projections down.
pub(crate) async fn masked_parquet_table(
    store: Arc<dyn ObjectStore>,
    path: Path,
    bitmap: RoaringTreemap,
    logical_schema: SchemaRef,
) -> AnyResult<Arc<dyn TableProvider>> {
    let partition = MaskedParquetPartition {
        store,
        path,
        bitmap: Arc::new(bitmap),
        schema: Arc::clone(&logical_schema),
    };
    let provider = StreamingTable::try_new(logical_schema, vec![Arc::new(partition)])
        .map_err(|e| anyhow!("failed to build DV-masked streaming table: {e}"))?;
    Ok(Arc::new(provider))
}

/// Single-partition [`PartitionStream`] that lazily opens a Parquet file and
/// drops rows flagged by `bitmap` as batches flow through.
struct MaskedParquetPartition {
    store: Arc<dyn ObjectStore>,
    path: Path,
    bitmap: Arc<RoaringTreemap>,
    /// The Delta logical schema; may differ from the file's own schema under
    /// schema evolution.
    schema: SchemaRef,
}

impl fmt::Debug for MaskedParquetPartition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MaskedParquetPartition")
            .field("path", &self.path)
            .field("deleted_rows", &self.bitmap.len())
            .finish()
    }
}

/// Project `batch` onto `logical_schema`: match columns by name, cast when
/// the Arrow types differ cosmetically, and null-fill columns the file lacks.
/// This mirrors DataFusion's default `SchemaAdapter`, which we cannot use
/// here: DataFusion 53 deprecates it for an adapter that only works inside
/// its own scan operators.
///
/// Partition columns also come out NULL (Delta stores them in
/// `partitionValues`, not in the file). This is the same pre-existing
/// limitation as the connector's `ListingTable` path.
fn project_to_logical(
    batch: &RecordBatch,
    logical_schema: &SchemaRef,
) -> Result<RecordBatch, DataFusionError> {
    let num_rows = batch.num_rows();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(logical_schema.fields().len());
    for field in logical_schema.fields().iter() {
        let col = match batch.schema().column_with_name(field.name()) {
            Some((idx, file_field)) => {
                if file_field.data_type() == field.data_type() {
                    Arc::clone(batch.column(idx))
                } else {
                    // Tolerate cosmetic differences (e.g. `List<Utf8>` vs
                    // `List<Utf8, field: 'element'>`) and benign promotions.
                    cast(batch.column(idx), field.data_type()).map_err(|e| {
                        DataFusionError::External(
                            format!(
                                "deletion-vector reader: cannot adapt file column '{}' \
                                 ({:?}) to Delta logical type {:?}: {e}",
                                field.name(),
                                file_field.data_type(),
                                field.data_type(),
                            )
                            .into(),
                        )
                    })?
                }
            }
            None => new_null_array(field.data_type(), num_rows),
        };
        columns.push(col);
    }
    RecordBatch::try_new(Arc::clone(logical_schema), columns).map_err(|e| {
        DataFusionError::External(
            format!("deletion-vector reader: projected batch rejected by logical schema: {e}")
                .into(),
        )
    })
}

/// Build the [`ProjectionMask`] selecting the root file columns that
/// `logical_schema` names; the rest are never decoded.
fn logical_projection_mask(
    builder: &ParquetRecordBatchStreamBuilder<ParquetObjectReader>,
    logical_schema: &SchemaRef,
) -> ProjectionMask {
    // Root Arrow fields map one-to-one, in order, to root Parquet columns.
    let roots = builder
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| logical_schema.column_with_name(field.name()).is_some())
        .map(|(idx, _)| idx);
    ProjectionMask::roots(builder.parquet_schema(), roots)
}

/// Build the [`RowSelection`] selecting every row in `0..total_rows` that is
/// not in `bitmap`. Positions past `total_rows` are ignored.
fn bitmap_to_row_selection(bitmap: &RoaringTreemap, total_rows: u64) -> RowSelection {
    let mut selectors: Vec<RowSelector> = Vec::new();
    // First row not yet covered by a selector.
    let mut cursor: u64 = 0;
    for deleted in bitmap.iter().take_while(|&pos| pos < total_rows) {
        if deleted > cursor {
            selectors.push(RowSelector::select((deleted - cursor) as usize));
        }
        // Extend the previous skip when deletes are consecutive.
        match selectors.last_mut() {
            Some(last) if last.skip => last.row_count += 1,
            _ => selectors.push(RowSelector::skip(1)),
        }
        cursor = deleted + 1;
    }
    if cursor < total_rows {
        selectors.push(RowSelector::select((total_rows - cursor) as usize));
    }
    RowSelection::from(selectors)
}

impl PartitionStream for MaskedParquetPartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let store = Arc::clone(&self.store);
        let path = self.path.clone();
        let bitmap = Arc::clone(&self.bitmap);
        let logical_schema = Arc::clone(&self.schema);

        let stream = try_stream! {
            let reader = ParquetObjectReader::new(store, path.clone());
            let builder = ParquetRecordBatchStreamBuilder::new(reader)
                .await
                .map_err(|e| DataFusionError::External(
                    format!("failed to open Parquet file '{path}': {e}").into()))?;
            let total_rows = builder.metadata().file_metadata().num_rows().max(0) as u64;
            // Decode only the columns the logical schema names.
            let mask = logical_projection_mask(&builder, &logical_schema);
            // Skip deleted rows inside the decoder.
            let selection = bitmap_to_row_selection(&bitmap, total_rows);
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
                    yield project_to_logical(&batch, &logical_schema)?;
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
    use arrow::array::{Array, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
    use datafusion::prelude::SessionContext;
    use deltalake::{DeltaTableBuilder, ensure_table_uri};
    use proptest::prelude::*;
    use tempfile::TempDir;

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
        let selection = bitmap_to_row_selection(&bitmap, total_rows);
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
        let selection = bitmap_to_row_selection(&bitmap, 10);
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

    /// End-to-end check of [`masked_parquet_table`] against a real Parquet
    /// file: DV-flagged rows are skipped inside the decoder, and the logical
    /// schema drives the read. A file column it omits is pruned, a column it
    /// widens is cast (`Int32` to `Int64`), and a column the file lacks comes
    /// back NULL (schema evolution).
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
        let provider = masked_parquet_table(
            store,
            Path::from("data.parquet"),
            deleted,
            Arc::clone(&logical),
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
}
