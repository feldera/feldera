//! Deletion-vector (DV) support for the Delta Lake input connector.
//!
//! Snapshot reads apply deletion vectors transparently inside delta-rs, but the
//! follow/cdc path reads the Parquet file referenced by each `Add`/`Remove`
//! action directly and therefore must apply the action's deletion vector itself.
//!
//! delta-rs exposes only the raw [`DeletionVectorDescriptor`]; the bitmap
//! decoder (base85 / CRC / roaring) lives in `delta_kernel`. We reuse it
//! rather than reimplement the on-disk format. We then wrap the file's
//! Parquet stream in a DataFusion [`StreamingTable`] that drops the
//! DV-flagged rows batch-by-batch, so memory stays bounded to a single
//! Parquet batch (matching the non-DV path's behavior).

use anyhow::{Result as AnyResult, anyhow};
use arrow::array::{ArrayRef, BooleanArray, new_null_array};
use arrow::compute::{cast, filter_record_batch};
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
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};
use roaring::RoaringTreemap;
use std::fmt;
use std::sync::Arc;

/// Translate a delta-rs deletion-vector descriptor into the `delta_kernel`
/// descriptor that owns the decoding logic. The two types carry identical
/// fields; only the storage-type enum differs in spelling.
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

/// Spec name of a [`StorageType`] (`"u"`, `"i"`, `"p"`).
///
/// Exposed `pub(crate)` so tests can build the matching `storageType` field
/// when synthesising commit records — keeping the table in one place
/// guarantees the spec strings stay in lock-step with the enum.
pub(crate) fn storage_type_str(st: StorageType) -> &'static str {
    match st {
        StorageType::UuidRelativePath => "u",
        StorageType::Inline => "i",
        StorageType::AbsolutePath => "p",
    }
}

/// Decode a deletion vector into the bitmap of deleted physical row positions.
///
/// `delta_kernel`'s `read` handles all three storage types (inline `i`,
/// uuid-relative sidecar `u`, absolute sidecar `p`). It is synchronous and may
/// perform blocking I/O to fetch a `.bin` sidecar, so we run it on the
/// blocking pool. The engine is built the same way delta-rs builds it
/// internally (from the table's root object store), guaranteeing consistent
/// path resolution.
pub(crate) async fn read_deletion_vector(
    dv: &DeletionVectorDescriptor,
    table: &DeltaTable,
) -> AnyResult<RoaringTreemap> {
    let log_store = table.log_store();
    let storage = log_store.engine(None).storage_handler();

    // The table root must end with a slash: `DeletionVectorDescriptor::read`
    // resolves the sidecar path via `Url::join`, which otherwise drops the last
    // path segment.
    let mut table_root = log_store.config().location().clone();
    if !table_root.path().ends_with('/') {
        table_root.set_path(&format!("{}/", table_root.path()));
    }
    let kernel_dv = to_kernel_descriptor(dv);
    let storage_str = storage_type_str(dv.storage_type);
    let path_or_inline = dv.path_or_inline_dv.clone();

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

/// Build a [`TableProvider`] over `path` that drops the rows flagged by
/// `bitmap` on the fly.
///
/// The bitmap indexes physical row positions within the file, so the file
/// must be read in order with a running offset. We do not route this through
/// the connector's `ListingTable` (which would split a single file across
/// partitions and reorder rows); instead we expose a single-partition
/// [`StreamingTable`] whose stream applies the DV mask per Parquet batch.
/// Memory stays bounded to one batch, regardless of file size.
///
/// `logical_schema` is the Delta table's current Arrow schema (the same one
/// the non-DV path declares on its [`ListingTable`]). Each batch read from
/// the Parquet file is projected to that schema by column name, with missing
/// columns null-filled — matching how DataFusion's `ListingTable` adapts file
/// batches under schema evolution.
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
    /// The Delta logical schema — the shape the table presents to DataFusion.
    /// May differ from the file's own schema under column add/drop/reorder.
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

/// Re-shape `batch` into a row that matches `logical_schema`: pick each column
/// by name from the file batch (casting to the logical type when the Arrow
/// representations differ in field metadata, list element naming, etc.), or
/// fill with NULLs when the column is absent. Mirrors DataFusion's default
/// `SchemaAdapter` policy closely enough for Delta-table reads.
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

impl PartitionStream for MaskedParquetPartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let store = Arc::clone(&self.store);
        let path = self.path.clone();
        let bitmap = Arc::clone(&self.bitmap);
        let schema = Arc::clone(&self.schema);
        let logical_schema = Arc::clone(&self.schema);

        let stream = try_stream! {
            let reader = ParquetObjectReader::new(store, path.clone());
            let builder = ParquetRecordBatchStreamBuilder::new(reader)
                .await
                .map_err(|e| DataFusionError::External(
                    format!("failed to open Parquet file '{path}': {e}").into()))?;
            let mut parquet_stream = builder
                .build()
                .map_err(|e| DataFusionError::External(
                    format!("failed to build Parquet stream for '{path}': {e}").into()))?;

            let mut row_offset: u64 = 0;
            while let Some(batch) = parquet_stream.next().await {
                let batch = batch.map_err(|e| DataFusionError::External(
                    format!("error reading Parquet file '{path}': {e}").into()))?;
                let num_rows = batch.num_rows() as u64;
                // Keep rows whose physical position is *not* set in the deletion vector.
                let mask: BooleanArray = (0..num_rows)
                    .map(|i| !bitmap.contains(row_offset + i))
                    .collect();
                row_offset += num_rows;
                let filtered = filter_record_batch(&batch, &mask)
                    .map_err(|e| DataFusionError::External(
                        format!("error applying deletion vector to '{path}': {e}").into()))?;
                if filtered.num_rows() > 0 {
                    yield project_to_logical(&filtered, &logical_schema)?;
                }
            }
        };

        Box::pin(RecordBatchStreamAdapter::new(schema, stream))
    }
}
