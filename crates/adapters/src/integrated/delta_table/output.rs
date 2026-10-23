use crate::catalog::{ArrowRowSink, CursorWithPolarity, SerBatchReader, SerKeyRange};
use crate::controller::{ControllerInner, EndpointId};
use crate::format::MAX_DUPLICATES;
use crate::format::parquet::{ArrowSchemaOptions, relation_to_arrow_fields};
use crate::integrated::delta_table::merge::compact::Compactor;
use crate::integrated::delta_table::merge::flush::MergeWriter;
use crate::integrated::delta_table::merge::metrics::MergeMetrics;
use crate::integrated::delta_table::merge::startup;
use crate::integrated::delta_table::merge::transient;
use crate::integrated::delta_table::{WriteError, register_storage_handlers};
use crate::transport::Step;
use crate::util::{IndexedOperationType, indexed_operation_type};
use crate::{
    AsyncErrorCallback, ControllerError, Encoder, OutputConsumer, OutputEndpoint, RecordFormat,
    SerCursor,
};
use anyhow::{Result as AnyResult, anyhow, bail};
use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    SchemaRef as ArrowSchemaRef,
};
use chrono::Utc;
use dbsp::circuit::tokio::TOKIO;
use delta_kernel::engine::arrow_conversion::TryFromArrow;
use delta_kernel::table_features::ColumnMappingMode;
use delta_kernel::table_properties::DataSkippingNumIndexedCols;
use deltalake::DeltaTable;
use deltalake::kernel::transaction::{CommitBuilder, CommitProperties, TableReference};
use deltalake::kernel::{Action, Add, ArrayType, DataType, MapType, StructField, StructType};
use deltalake::logstore::ObjectStoreRef;

use super::bounded_upload::bound_uploads;
use deltalake::operations::create::CreateBuilder;
use deltalake::operations::write::writer::{ArrowWriterOptions, DeltaWriter, WriterConfig};
use deltalake::protocol::{DeltaOperation, SaveMode};
use feldera_adapterlib::catalog::SerCursorFlattened;
use feldera_adapterlib::transport::OutputBatchType;
use feldera_types::program_schema::{ColumnType, SqlType};
use feldera_types::serde_with_context::serde_config::{
    BinaryFormat, DecimalFormat, UuidFormat, VariantFormat,
};
use feldera_types::serde_with_context::{DateFormat, SqlSerdeConfig, TimeFormat, TimestampFormat};
use feldera_types::transport::delta_table::DeltaTableWriteMode;
use feldera_types::transport::delta_table::DeltaVariantEncoding;
use feldera_types::{
    adapter_stats::ConnectorHealth, program_schema::Relation,
    transport::delta_table::DeltaTableWriterConfig,
};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::cmp::min;
use std::collections::HashMap;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Instant;
use tokio::time::{Duration, sleep};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, info, info_span, warn};

/// Arrow serde config for writing Delta tables.
pub fn delta_output_serde_config(variant_encoding: DeltaVariantEncoding) -> SqlSerdeConfig {
    SqlSerdeConfig {
        timestamp_format: TimestampFormat::MicrosSinceEpoch,
        timestamp_tz_format: TimestampFormat::MicrosSinceEpoch,
        time_format: TimeFormat::NanosSigned,
        date_format: DateFormat::String("%Y-%m-%d"),
        decimal_format: DecimalFormat::String,
        variant_format: match variant_encoding {
            DeltaVariantEncoding::Variant => VariantFormat::ParquetVariant,
            DeltaVariantEncoding::JsonString => VariantFormat::JsonString,
        },
        binary_format: BinaryFormat::Array,
        uuid_format: UuidFormat::String,
    }
}

/// Restore the Delta `variant` type wherever the SQL schema says `VARIANT`.
///
/// `try_from_arrow` sees a Parquet variant as the struct of two binary buffers
/// it is encoded as, and maps it to a Delta struct. Walking the SQL type
/// alongside the Delta type puts the variants back, including the ones nested
/// in a `ROW`, an `ARRAY` or a `MAP`, whose data is written as a variant
/// either way.
pub(crate) fn delta_variant_types(
    sql_type: &ColumnType,
    delta_type: DataType,
) -> AnyResult<DataType> {
    Ok(match sql_type.typ {
        SqlType::Variant => DataType::unshredded_variant(),
        SqlType::Struct => {
            let (DataType::Struct(fields), Some(sql_fields)) =
                (&delta_type, sql_type.fields.as_ref())
            else {
                return Ok(delta_type);
            };
            let mut nested = Vec::with_capacity(sql_fields.len());
            for field in fields.fields() {
                let Some(sql_field) = sql_fields.iter().find(|f| &f.name.name() == field.name())
                else {
                    nested.push(field.clone());
                    continue;
                };
                // Rewriting the type must keep the field's metadata, which
                // carries its column mapping id.
                nested.push(
                    StructField::new(
                        field.name(),
                        delta_variant_types(&sql_field.columntype, field.data_type().clone())?,
                        field.is_nullable(),
                    )
                    .with_metadata(field.metadata().clone()),
                );
            }
            DataType::Struct(Box::new(StructType::try_new(nested)?))
        }
        SqlType::Array => {
            let (DataType::Array(array), Some(component)) =
                (&delta_type, sql_type.component.as_ref())
            else {
                return Ok(delta_type);
            };
            DataType::Array(Box::new(ArrayType::new(
                delta_variant_types(component, array.element_type().clone())?,
                array.contains_null(),
            )))
        }
        SqlType::Map => {
            let (DataType::Map(map), Some(key), Some(value)) =
                (&delta_type, sql_type.key.as_ref(), sql_type.value.as_ref())
            else {
                return Ok(delta_type);
            };
            DataType::Map(Box::new(MapType::new(
                delta_variant_types(key, map.key_type().clone())?,
                delta_variant_types(value, map.value_type().clone())?,
                map.value_contains_null(),
            )))
        }
        _ => delta_type,
    })
}

/// Fail when an existing table stores a `VARIANT` column in the other encoding.
///
/// Appending to a table created before the connector wrote Delta `variant`
/// columns is the likeliest upgrade failure, and nothing else catches it: the
/// writer validates each batch against the schema it computed itself, and the
/// commit does not check schemas at all, so the connector would write variant
/// buffers into a column the table declares a `string`. The table stays
/// readable by nobody, and the damage surfaces in someone else's query.
pub(crate) fn check_variant_encoding(
    table: &DeltaTable,
    computed: &[StructField],
    encoding: DeltaVariantEncoding,
) -> AnyResult<()> {
    let Ok(snapshot) = table.snapshot() else {
        // A table the connector just created matches by construction.
        return Ok(());
    };
    let existing = snapshot.schema();

    for field in computed {
        let Some(current) = existing.field(field.name()) else {
            continue;
        };
        let wanted = matches!(field.data_type(), DataType::Variant(_));
        let found = matches!(current.data_type(), DataType::Variant(_));
        if wanted == found {
            continue;
        }

        let (is, set) = if found {
            ("the Delta `variant` type", "variant")
        } else {
            ("a `string`", "json_string")
        };
        return Err(anyhow!(
            "column '{}' of the existing Delta table stores VARIANT as {is}, but the connector \
             is configured to write it as {}. Set 'variant_encoding' to '{set}', or write to a \
             new table.",
            field.name(),
            match encoding {
                DeltaVariantEncoding::Variant => "the Delta `variant` type",
                DeltaVariantEncoding::JsonString => "a `string`",
            },
        ));
    }
    Ok(())
}

/// Refuse a column-mapped table: we write logical names, so readers would see NULL.
fn check_column_mapping(table: &DeltaTable) -> AnyResult<()> {
    let snapshot = table.snapshot()?;
    match snapshot.snapshot().table_properties().column_mapping_mode {
        None | Some(ColumnMappingMode::None) => Ok(()),
        Some(mode) => Err(anyhow!(
            "the existing Delta table sets 'delta.columnMapping.mode' to '{}', which this \
             connector cannot write: the rows would be stored under column names no reader \
             resolves, and would read back as NULL. Write to a table without column mapping.",
            format!("{mode:?}").to_lowercase()
        )),
    }
}

struct DeltaTableWriterInner {
    endpoint_id: EndpointId,
    endpoint_name: String,
    config: DeltaTableWriterConfig,
    /// The view's columns, which the cursors append rows of.
    row_schema: Arc<ArrowSchema>,
    /// The table's columns: the view's, and in cdc mode `__feldera_op` and `__feldera_ts`.
    arrow_schema: Arc<ArrowSchema>,
    struct_fields: Vec<StructField>,
    key_schema: Option<Relation>,
    value_schema: Relation,
    controller: Weak<ControllerInner>,
    /// Running count of records written by all worker threads in the current batch.
    /// Updated atomically by parallel tokio tasks during `flush_chunk`.
    /// Reset to 0 at the start of each batch.
    ///
    /// Shared with the controller's `OutputEndpointMetrics` via `Arc`, so
    /// progress is visible to the metrics snapshot without any extra
    /// synchronisation.
    records_written: Arc<AtomicU64>,
    is_index: bool,
    /// `ControllerInner::shutdown_token`, which ends an in-flight write
    /// instead of letting it outlive the pipeline.  See
    /// [encode_and_write_range].
    shutdown: CancellationToken,
}

pub struct DeltaTableWriter {
    inner: Arc<DeltaTableWriterInner>,
    object_store: ObjectStoreRef,
    task: WriterTask,
    threads: usize,
    pending_actions: Vec<Add>,
    num_rows: usize,
    /// Present in merge mode, which applies and commits a batch in `encode` rather than
    /// writing files there and committing in `batch_end`.
    merge: Option<Arc<MergeWriter>>,
    merge_metrics: Option<Arc<MergeMetrics>>,
    /// Present when `auto_compact` asks the connector to compact the table itself.
    compactor: Option<Compactor>,
    /// Uniqueness violations handed to the controller, for the test that pins how often a
    /// retried batch reports them. Tests build the endpoint with no controller to report to.
    #[cfg(any(test, feature = "testing"))]
    merge_violations: u64,
    /// A merge batch failed for good. Its commit may still have landed, so the next flush
    /// must read the table again before it looks anything up.
    merge_reload: bool,
}

/// Limit on the number of records buffered in memory in the encoder.
const CHUNK_SIZE: usize = 100_000;

/// Size at which the writer closes the current Parquet file and starts a new one.
///
/// delta-rs rolls over only when the writer is given a target size; without one it
/// writes a single object per key range until the batch ends. Object stores cap a
/// multipart upload at 10000 parts, so an unbounded file fails outright once it
/// grows past that, and a table written as a handful of huge files is slow for
/// readers to scan. 100 MiB is the size delta-rs itself used before the target
/// became optional.
pub(super) const TARGET_FILE_SIZE: NonZeroU64 = NonZeroU64::new(100 * 1024 * 1024).unwrap();

/// Bounds row groups so the writer waits on uploads instead of buffering whole files.
const MAX_ROW_GROUP_BYTES: usize = 16 * 1024 * 1024;

/// The config of every data-file writer this connector runs, so all of them bound memory.
pub(super) fn bounded_writer_config(
    schema: ArrowSchemaRef,
    partition_columns: Vec<String>,
    target_file_size: NonZeroU64,
    num_indexed_cols: DataSkippingNumIndexedCols,
    stats_columns: Option<Vec<String>>,
) -> WriterConfig {
    // delta-rs defaults plus the row group bound.
    let writer_properties = WriterProperties::builder()
        .set_created_by(format!("delta-rs version {}", deltalake::crate_version()))
        .set_compression(Compression::SNAPPY)
        .set_max_row_group_bytes(Some(MAX_ROW_GROUP_BYTES))
        .build();
    // Serial encoding: delta-rs's parallel default ignores `max_row_group_bytes`
    // and buffers the whole file, which removes backpressure from a slow store.
    let arrow_options = ArrowWriterOptions::new().with_enable_parallel_encoding(false);
    // delta-rs checks the file size once per slice; 8192-row slices overshoot by tens of MiB.
    const WRITE_BATCH_ROWS: usize = 1024;
    WriterConfig::new(
        schema,
        partition_columns,
        Some(writer_properties),
        Some(arrow_options),
        Some(target_file_size),
        Some(WRITE_BATCH_ROWS),
        num_indexed_cols,
        stats_columns,
    )
}

impl DeltaTableWriter {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        endpoint_id: EndpointId,
        endpoint_name: &str,
        config: &DeltaTableWriterConfig,
        key_schema: &Option<Relation>,
        value_schema: &Relation,
        controller: Weak<ControllerInner>,
        shutdown: CancellationToken,
        continue_previous_state: bool,
        is_index: bool,
        output_buffered: bool,
    ) -> Result<Self, ControllerError> {
        config.validate().map_err(|e| {
            ControllerError::invalid_transport_configuration(endpoint_name, &e.to_string())
        })?;

        if let Some(advice) = unbuffered_merge_advice(config, output_buffered) {
            warn!("delta_table {endpoint_name}: {advice}");
        }

        let threads = config.threads.unwrap_or(1);

        if threads > 1 && !is_index {
            return Err(ControllerError::invalid_transport_configuration(
                endpoint_name,
                "Parallel writes (threads > 1) require the view to have a unique key to \
                 ensure correct ordering of inserts and deletes. Please specify the `index` \
                 property in the connector configuration. For more details, see: \
                 https://docs.feldera.com/connectors/unique_keys",
            ));
        }

        register_storage_handlers();

        // Merge mode keeps the table in sync with the view, so it holds exactly the view's
        // columns; cdc mode appends a change log, tagging each row with the operation.
        let parquet_variant = config.variant_encoding == DeltaVariantEncoding::Variant;
        let mut arrow_fields = relation_to_arrow_fields(
            &value_schema.fields,
            ArrowSchemaOptions::new(true).with_parquet_variant(parquet_variant),
        );
        let row_schema = Arc::new(ArrowSchema::new(arrow_fields.clone()));
        if !config.is_merge() {
            arrow_fields.push(ArrowField::new("__feldera_op", ArrowDataType::Utf8, true));
            arrow_fields.push(ArrowField::new("__feldera_ts", ArrowDataType::Int64, true));
        }

        let arrow_schema = Arc::new(ArrowSchema::new(arrow_fields));

        let mut struct_fields: Vec<_> = vec![];

        // A Parquet variant is `struct<metadata, value>` in Arrow, which says
        // nothing about being a variant, so the SQL schema has to put the
        // variants back afterwards, at every depth.
        let sql_types: HashMap<String, &ColumnType> = value_schema
            .fields
            .iter()
            .map(|f| (f.name.name(), &f.columntype))
            .collect();

        for f in arrow_schema.fields.iter() {
            let mut data_type = DataType::try_from_arrow(f.data_type()).map_err(|e| {
                ControllerError::output_transport_error(
                    endpoint_name,
                    true,
                    anyhow!("error converting arrow field '{f}' to a Delta Lake field: {e}"),
                )
            })?;
            if parquet_variant && let Some(sql_type) = sql_types.get(f.name()) {
                data_type = delta_variant_types(sql_type, data_type).map_err(|e| {
                    ControllerError::output_transport_error(
                        endpoint_name,
                        true,
                        anyhow!(
                            "error declaring the Delta type of column '{}': {e}",
                            f.name()
                        ),
                    )
                })?;
            }
            struct_fields.push(StructField::new(f.name(), data_type, f.is_nullable()));
        }

        let inner = Arc::new(DeltaTableWriterInner {
            endpoint_id,
            endpoint_name: endpoint_name.to_string(),
            config: config.clone(),
            row_schema,
            arrow_schema,
            struct_fields,
            key_schema: key_schema.clone(),
            value_schema: value_schema.clone(),
            controller,
            records_written: Arc::new(AtomicU64::new(0)),
            is_index,
            shutdown,
        });

        // Register the progress counter with the controller's metrics.
        // add_output() has already been called, so the metrics slot exists.
        if let Some(controller) = inner.controller.upgrade() {
            controller
                .status
                .register_batch_progress_counter(&inner.endpoint_id, inner.records_written.clone());
        }

        // Create or open the delta table.
        // Panic safety: block_on() panics if called from a tokio async context.
        // new() is called from sync controller code (connect_output), so this is fine.
        let task = TOKIO
            .block_on(WriterTask::create(inner.clone(), continue_previous_state))
            .map_err(|e| {
                ControllerError::output_transport_error(
                    endpoint_name,
                    true,
                    anyhow!(
                        "error creating or opening delta table '{}': {e}",
                        config.uri
                    ),
                )
            })?;

        // Merge mode only: every range of a flush writes through this one store, so the cap
        // bounds the whole connector. cdc keeps main's per-file upload concurrency.
        let object_store = if config.is_merge() {
            bound_uploads(task.delta_table.object_store())
        } else {
            task.delta_table.object_store()
        };

        let merge = if config.is_merge() {
            Some(build_merge_writer(&inner, &task.delta_table).map_err(|e| {
                ControllerError::invalid_transport_configuration(endpoint_name, &format!("{e:#}"))
            })?)
        } else {
            None
        };

        // Registered here rather than in the endpoint constructor: `add_output` has already
        // run, so the metrics slot exists, and registering before it does drops them.
        let merge_metrics = merge.is_some().then(MergeMetrics::new);
        let compactor = merge_metrics
            .clone()
            .and_then(|metrics| Compactor::new(config, endpoint_name, metrics));
        if let (Some(metrics), Some(controller)) = (&merge_metrics, inner.controller.upgrade()) {
            if compactor.is_some() {
                metrics.maintains_the_table();
            }
            controller
                .status
                .set_output_custom_metrics(inner.endpoint_id, metrics.clone());
        }

        Ok(Self {
            inner,
            object_store,
            task,
            threads,
            pending_actions: Vec::new(),
            num_rows: 0,
            merge: merge.map(Arc::new),
            merge_metrics,
            compactor,
            #[cfg(any(test, feature = "testing"))]
            merge_violations: 0,
            merge_reload: false,
        })
    }
}

/// What to tell an operator running merge mode without output buffering, if anything.
///
/// Not a configuration error: it works, and costs a whole lookup pass per step of the
/// circuit rather than per batch. Returned rather than logged so the rule can be tested.
fn unbuffered_merge_advice(
    config: &DeltaTableWriterConfig,
    output_buffered: bool,
) -> Option<String> {
    (config.is_merge() && !output_buffered).then(|| {
        "'update_mode: merge' locates the rows to supersede once per flush, so without \
         output buffering it pays that cost for every step of the circuit. Set \
         'enable_output_buffer' on the connector, with 'max_output_buffer_time_millis', to \
         flush on a cadence instead. See \
         https://docs.feldera.com/connectors#configuring-the-output-buffer"
            .to_string()
    })
}

/// Check the target table and build the merge-mode writer.
fn build_merge_writer(inner: &DeltaTableWriterInner, table: &DeltaTable) -> AnyResult<MergeWriter> {
    let setup = startup::prepare(table, &inner.key_schema, &inner.struct_fields)?;

    MergeWriter::new(
        setup,
        inner
            .key_schema
            .as_ref()
            .expect("startup::prepare rejects a missing key schema"),
        inner.arrow_schema.clone(),
        inner.config.lookup_chunk_size.as_usize(),
        inner.config.max_concurrent_probes,
        inner.value_schema.name.clone(),
    )
}

struct WriterTask {
    inner: Arc<DeltaTableWriterInner>,
    delta_table: DeltaTable,
}

/// Retry the future `op` with exponential backoff of up to 10 seconds until it
/// succeeds, `config.max_retries` is reached, or the pipeline goes down.
///
/// `warn!` and set health status to unhealthy on each failure, clear the health
/// status on success.
///
/// The shutdown check is not optional.  Every caller runs on the controller's
/// output thread, a DBSP aux thread that `RuntimeHandle::kill` joins, and the
/// loop is unbounded when `max_retries` is `None`, which is the documented
/// default.  A retry that outlives the shutdown hangs the teardown.
macro_rules! retry {
    ($self:ident, $description:expr, $op:expr) => {{
        let mut retry_count = 0;

        // Owned, so that awaiting it does not borrow `$self` alongside `$op`.
        let shutdown = $self.inner.shutdown.clone();

        let attempts = async {
            let mut backoff = Duration::from_secs(1);
            let max_backoff = Duration::from_secs(10);

            loop {
                match $op.await {
                    Ok(result) => {
                        if let Some(controller) = $self.inner.controller.upgrade() {
                            controller.update_output_connector_health(
                                $self.inner.endpoint_id,
                                ConnectorHealth::healthy(),
                            );
                        }
                        if retry_count > 0 {
                            info!(
                                "delta_table {}: {description} succeeded after {retry_count} attempts",
                                &$self.inner.endpoint_name,
                                description = $description
                            );
                        }
                        return Ok(result);
                    }
                    Err(e)
                        if $self.inner.config.max_retries.is_none()
                            || retry_count < $self.inner.config.max_retries.unwrap() =>
                    {
                        retry_count += 1;
                        let message = format!(
                            "{description} failed after {retry_count} attempts (retrying in {backoff:?}): {e:?}",
                            description = $description
                        );

                        if let Some(controller) = $self.inner.controller.upgrade() {
                            controller.update_output_connector_health(
                                $self.inner.endpoint_id,
                                ConnectorHealth::unhealthy(&message),
                            );
                        }
                        warn!("delta_table {}: {message}", &$self.inner.endpoint_name);

                        sleep(backoff).await;
                        backoff = std::cmp::min(backoff * 2, max_backoff);
                    }
                    Err(e) => {
                        retry_count += 1;

                        let message = format!(
                            "{description} failed after {retry_count} attempts: {e:?}",
                            description = $description
                        );

                        return Err(anyhow!(message));
                    }
                }
            }
        };

        // One shutdown consult for the whole loop; see `encode_and_write_range`
        // for why that is enough, and why it beats one per await.
        shutdown
            .run_until_cancelled(attempts)
            .await
            .unwrap_or_else(|| Err(shutdown_aborted($description, retry_count)))
    }};
}

impl WriterTask {
    fn current_version(&self) -> String {
        if let Some(version) = self.delta_table.version() {
            version.to_string()
        } else {
            "none".to_string()
        }
    }

    async fn create(
        inner: Arc<DeltaTableWriterInner>,
        continue_previous_state: bool,
    ) -> AnyResult<Self> {
        let storage_options = inner.config.object_store_config.clone();

        // On restart (resuming from a checkpoint), open the existing table
        // without truncating or error-checking.  This prevents data loss when
        // the pipeline auto-restarts with `truncate` mode, and avoids spurious
        // errors with `error_if_exists` mode.
        let save_mode = match inner.config.mode {
            // I expected `SaveMode::Append` to be the correct setting, but
            // that always returns an error.
            DeltaTableWriteMode::Append => SaveMode::Ignore,
            DeltaTableWriteMode::Truncate => {
                if continue_previous_state {
                    SaveMode::Ignore
                } else {
                    SaveMode::Overwrite
                }
            }
            DeltaTableWriteMode::ErrorIfExists => {
                if continue_previous_state {
                    SaveMode::Ignore
                } else {
                    SaveMode::ErrorIfExists
                }
            }
        };

        info!(
            "delta_table {}: {} delta table '{}' in '{save_mode:?}' mode",
            &inner.endpoint_name,
            if continue_previous_state {
                "reopening"
            } else {
                "opening or creating"
            },
            &inner.config.uri,
        );

        let delta_table = {
            let mut retry_count = 0;
            const MAX_RETRIES: u32 = 10;

            // We've seen the table builder get stuck forever in S3 authentication for some configurations
            // (see https://github.com/delta-io/delta-rs/issues/3768). So we add a timeout and retry logic
            // in that case.
            //
            // In other situations, the operation fails returning a timeout error. There is no easy way to
            // distinguish such errors from permanent failures such as incorrect credentials, we therefore
            // resort to checking the returned error message for the word "timeout".
            let mut operation_timeout: Duration = Duration::from_secs(60);

            loop {
                let checkpoint_interval = match inner.config.checkpoint_interval {
                    Some(0) => None,
                    Some(interval) => Some(interval.to_string()),
                    None => Some("10".to_string()),
                };
                let create_future = CreateBuilder::new()
                    .with_location(inner.config.uri.clone())
                    .with_save_mode(save_mode)
                    .with_storage_options(storage_options.clone())
                    .with_columns(inner.struct_fields.clone())
                    .with_configuration_property(
                        deltalake::TableProperty::CheckpointInterval,
                        checkpoint_interval,
                    )
                    .with_configuration_property(
                        deltalake::TableProperty::LogRetentionDuration,
                        inner.config.log_retention_duration.clone(),
                    )
                    .with_configuration_property(
                        deltalake::TableProperty::EnableExpiredLogCleanup,
                        inner
                            .config
                            .enable_expired_log_cleanup
                            .map(|b| b.to_string()),
                    )
                    // A table merge mode creates must allow deletion vectors. delta-rs
                    // applies creation properties only when it really creates the table, so
                    // an existing one without the property fails the startup check instead,
                    // which names the ALTER TABLE to run.
                    .with_configuration_property(
                        deltalake::TableProperty::EnableDeletionVectors,
                        inner.config.is_merge().then_some("true"),
                    );

                match tokio::time::timeout(operation_timeout, create_future).await {
                    Ok(Ok(table)) => break table,
                    Ok(Err(e)) => {
                        // Debug-format `e` as the timeout error is often found toward the end of the error chain.
                        let is_timeout = format!("{:?}", e).to_lowercase().contains("timeout");

                        if is_timeout && retry_count < MAX_RETRIES {
                            retry_count += 1;
                            let backoff_ms = min(1000 * (1 << (retry_count - 1)), 10_000);
                            warn!(
                                "delta_table {}: error creating or opening delta table '{}' after {retry_count} attempts (retrying in {backoff_ms} ms): {e:?}",
                                &inner.endpoint_name, &inner.config.uri,
                            );

                            sleep(Duration::from_millis(backoff_ms)).await;
                        } else {
                            return Err(anyhow!(
                                "error creating or opening delta table '{}': {e:?}",
                                inner.config.uri
                            ));
                        }
                    }
                    Err(_timeout) => {
                        if retry_count >= MAX_RETRIES {
                            return Err(anyhow!(
                                "timeout creating or opening delta table '{}' after {retry_count} attempts",
                                inner.config.uri,
                            ));
                        } else {
                            warn!(
                                "delta_table {}: timeout creating or opening delta table '{}' after {retry_count} attempts, retrying",
                                &inner.endpoint_name, &inner.config.uri,
                            );
                            retry_count += 1;
                            if operation_timeout < Duration::from_secs(240) {
                                operation_timeout *= 2;
                            }
                        }
                    }
                }
            }
        };

        // `SaveMode::Ignore` keeps an existing table's schema and throws away
        // the one computed here, and nothing downstream compares the two, so a
        // mismatched VARIANT column would be written as the wrong encoding
        // with no error at all.
        check_variant_encoding(
            &delta_table,
            &inner.struct_fields,
            inner.config.variant_encoding,
        )?;
        check_column_mapping(&delta_table)?;

        info!(
            "delta_table {}: opened delta table '{}' (current table version {})",
            &inner.endpoint_name,
            &inner.config.uri,
            if let Some(version) = delta_table.version() {
                version.to_string()
            } else {
                "none".to_string()
            }
        );

        // `checkpoint_interval`, `log_retention_duration`, and `enable_expired_log_cleanup` are
        // only honoured by delta-rs when the table is freshly created / in truncate mode.  When we open an existing
        // table (e.g. `mode = append` against an existing table, or any resume from a pipeline
        // checkpoint) the values supplied in the connector config are ignored and the
        // table keeps whatever properties it was created with.  Compare the user's intent against
        // what actually landed in the table metadata and warn on any discrepancy.
        warn_about_table_property_discrepancies(&inner, &delta_table);

        Ok(Self { inner, delta_table })
    }

    async fn commit(&mut self, actions: &[Add]) -> AnyResult<()> {
        // The snapshot version for the next commit is computed as the current version + 1.
        // We need to update the current version manually, since it doesn't happen automatically.
        self.delta_table
            .update_incremental(None)
            .await
            .map_err(|e| {
                anyhow!(format!(
                    "updating Delta table version before commit (current version: {}): {e:?}",
                    self.current_version()
                ))
            })?;

        // `CommitBuilder::default()` leaves `post_commit_hook` unset, so delta-rs skips the
        // post-commit hook entirely and never writes `_last_checkpoint` / `*.checkpoint.parquet`,
        // regardless of `delta.checkpointInterval`. Use default commit properties so checkpoint
        // creation runs when `(version + 1) % checkpoint_interval == 0`.
        CommitBuilder::from(CommitProperties::default())
            .with_actions(
                actions
                    .iter()
                    .map(|add| Action::Add(add.clone()))
                    .collect::<Vec<_>>(),
            )
            .build(
                self.delta_table
                    .state
                    .as_ref()
                    .map(|state| state as &dyn TableReference),
                self.delta_table.log_store(),
                DeltaOperation::Write {
                    mode: SaveMode::Append,
                    partition_by: None,
                    predicate: None,
                },
            )
            .await
            .map_err(|e| {
                anyhow!(format!(
                    "error committing changes to the Delta table (current version: {}): {e:?}",
                    self.current_version()
                ))
            })?;

        Ok(())
    }

    async fn commit_with_retry(&mut self, actions: &[Add]) -> AnyResult<()> {
        retry!(
            self,
            "committing Delta table transaction",
            self.commit(actions)
        )
    }
}

/// The error an operation reports when a shutdown cuts it short.
fn shutdown_aborted(operation: &str, retry_count: u32) -> anyhow::Error {
    anyhow!("{operation} aborted after {retry_count} attempts: the pipeline is shutting down")
}

/// Encode a key range and stream-write it to a `DeltaWriter`, retrying transient failures.
///
/// On retry, a fresh cursor is created from `range` and a new `DeltaWriter`
/// is created. Any Parquet files written by a failed attempt become orphans that
/// Delta `VACUUM` will clean up.
///
/// Only transient I/O errors are retried; deterministic errors (e.g., non-unique keys,
/// serialization failures) are returned immediately.
async fn encode_and_write_range(
    range: Box<dyn SerKeyRange>,
    inner: Arc<DeltaTableWriterInner>,
    object_store: ObjectStoreRef,
    micros: i64,
) -> AnyResult<(Vec<Add>, usize)> {
    // This function has its own retry loop instead of using the `retry!` macro because:
    // Multiple ranges run in parallel; the `retry!` macro clears the connector
    // health status on success, which would be incorrect here; a single range
    // succeeding must not mask failures in other ranges.
    let mut retry_count: u32 = 0;
    let attempts = async {
        let mut backoff = Duration::from_secs(1);
        let max_backoff = Duration::from_secs(10);

        loop {
            // A shutdown part-way through drops this future at its next await
            // point, abandoning the Parquet files the attempt had already put in
            // the object store.  A failed attempt leaves the same orphans, and
            // Delta `VACUUM` collects them.
            match write_once(range.as_ref(), &inner, &object_store, micros).await {
                Ok((ref actions, rows)) => {
                    if retry_count > 0 {
                        info!(
                            "delta_table {}: Delta table write succeeded after {retry_count} retries ({rows} rows, {} files)",
                            inner.endpoint_name,
                            actions.len(),
                        );
                    }
                    return Ok((actions.clone(), rows));
                }
                Err(WriteError::Deterministic(e)) => {
                    return Err(e);
                }
                Err(WriteError::Transient(e))
                    if inner.config.max_retries.is_none()
                        || retry_count < inner.config.max_retries.unwrap() =>
                {
                    retry_count += 1;
                    let message = format!(
                        "Delta table write failed (attempt {retry_count}, retrying in {backoff:?}): {e:?}"
                    );
                    if let Some(controller) = inner.controller.upgrade() {
                        controller.update_output_connector_health(
                            inner.endpoint_id,
                            ConnectorHealth::unhealthy(&message),
                        );
                    }
                    warn!("delta_table {}: {message}", inner.endpoint_name);

                    sleep(backoff).await;
                    backoff = std::cmp::min(backoff * 2, max_backoff);
                }
                Err(WriteError::Transient(e)) => {
                    return Err(anyhow!(
                        "Delta table write failed after {retry_count} retries: {e}"
                    ));
                }
            }
        }
    };

    // One shutdown consult for the whole loop.  `run_until_cancelled` answers
    // `None` without polling an already-cancelled token's future, so no attempt
    // begins after the shutdown, and dropping the loop makes every await inside
    // it an abort point without naming any of them here.  A new await in the
    // body therefore cannot reintroduce the hang.
    inner
        .shutdown
        .run_until_cancelled(attempts)
        .await
        .unwrap_or_else(|| Err(shutdown_aborted("Delta table write", retry_count)))
}

/// Encodes and writes the range once, undoing its contribution to the shared
/// progress counter unless it succeeds.
///
/// [AttemptProgress] owns the row count so that the rollback also happens when
/// this future is dropped mid-write, which is how a shutdown ends it.  The
/// caller cannot do the rollback itself: the count lives in a borrow this
/// future holds, and the caller only ever sees the future, not inside it.
async fn write_once(
    range: &dyn SerKeyRange,
    inner: &DeltaTableWriterInner,
    object_store: &ObjectStoreRef,
    micros: i64,
) -> Result<(Vec<Add>, usize), WriteError> {
    let mut progress = AttemptProgress {
        inner,
        rows_written: 0,
        succeeded: false,
    };
    let result = stream_encode_and_write(
        range,
        inner,
        object_store.clone(),
        micros,
        &mut progress.rows_written,
    )
    .await;
    progress.succeeded = result.is_ok();
    result
}

/// The rows one write attempt added to the shared progress counter, rolled
/// back on drop unless the attempt succeeded.
struct AttemptProgress<'a> {
    inner: &'a DeltaTableWriterInner,
    rows_written: u64,
    succeeded: bool,
}

impl Drop for AttemptProgress<'_> {
    fn drop(&mut self) {
        if !self.succeeded {
            rollback_progress(self.inner, self.rows_written);
        }
    }
}

/// Subtract a failed attempt's contribution from the shared progress counter.
///
/// This subtracts exactly what the failed attempt added (its `total_rows`), so
/// it does not interfere with other ranges.
fn rollback_progress(inner: &DeltaTableWriterInner, written: u64) {
    if written == 0 {
        return;
    }
    inner.records_written.fetch_sub(written, Ordering::Relaxed);
}

/// Warn when user-supplied table-creation properties (`checkpoint_interval`,
/// `log_retention_duration`, `enable_expired_log_cleanup`) do not match what is actually in the
/// table's configuration.  The most common cause is that the table already existed when the
/// pipeline started (so delta-rs kept the original properties and ignored ours), but it could
/// be due to other external modifications to the table. We just want to surface any discrepancies.
/// We pass the properties to delta-rs as strings, so we read them back as strings to avoid any
/// conversion ambiguity.
fn warn_about_table_property_discrepancies(
    inner: &DeltaTableWriterInner,
    delta_table: &DeltaTable,
) {
    let snapshot = match delta_table.snapshot() {
        Ok(s) => s,
        Err(_) => return,
    };
    let actual = snapshot.metadata().configuration();

    let mut discrepancies: Vec<String> = Vec::new();

    let mut check = |key: &str, requested: &str| {
        let effective = actual.get(key).map(|s| s.as_str()).unwrap_or("<unset>");
        if effective != requested {
            discrepancies.push(format!(
                "{key}: configured {requested:?}, in table {effective:?}"
            ));
        }
    };

    if let Some(interval) = inner.config.checkpoint_interval {
        check("delta.checkpointInterval", &interval.to_string());
    }
    if let Some(duration) = inner.config.log_retention_duration.as_deref() {
        check("delta.logRetentionDuration", duration);
    }
    if let Some(enabled) = inner.config.enable_expired_log_cleanup {
        check("delta.enableExpiredLogCleanup", &enabled.to_string());
    }

    if !discrepancies.is_empty() {
        warn!(
            "delta_table {}: table at '{}' has properties that conflict with the connector configuration. \
            This usually indicates the table was created with different settings or modified externally. \
            Conflicting connector properties not applied: {}",
            &inner.endpoint_name,
            &inner.config.uri,
            discrepancies.join("; "),
        );
    }
}

/// Build a `RecordBatch` from `rows` and their operations `ops` (deterministic), write it
/// via `writer` (transient I/O), and report progress.
///
/// `rows_written` accumulates the number of records successfully written to the
/// object store across all chunks in this attempt. The caller uses it both as
/// the return value (on success) and for rollback (on failure).
async fn flush_chunk(
    rows: &mut dyn ArrowRowSink,
    ops: &mut Vec<&'static str>,
    micros: i64,
    writer: &mut DeltaWriter,
    inner: &DeltaTableWriterInner,
    rows_written: &mut u64,
) -> Result<(), WriteError> {
    let batch = rows
        .finish()
        .and_then(|rows| with_change_columns(rows, ops, micros, &inner.arrow_schema))
        .map_err(|e| WriteError::Deterministic(anyhow!("error generating arrow arrays: {e}")))?;
    ops.clear();
    let num_rows = batch.num_rows();
    writer
        .write(&batch)
        .await
        .map_err(|e| WriteError::Transient(anyhow!("error writing {num_rows} records: {e:?}")))?;
    let n = num_rows as u64;
    *rows_written += n;
    inner.records_written.fetch_add(n, Ordering::Relaxed);
    Ok(())
}

/// Returns `rows` with the change log columns of cdc mode: `__feldera_op`, the operation of
/// each row in `ops`, and `__feldera_ts`, the time of the batch in microseconds since the
/// UNIX epoch, so that it has the columns of `schema`.
fn with_change_columns(
    rows: RecordBatch,
    ops: &[&str],
    micros: i64,
    schema: &ArrowSchemaRef,
) -> AnyResult<RecordBatch> {
    let mut columns = rows.columns().to_vec();
    columns.push(Arc::new(StringArray::from(ops.to_vec())));
    columns.push(Arc::new(Int64Array::from(vec![micros; ops.len()])));
    Ok(RecordBatch::try_new(schema.clone(), columns)?)
}

/// Single-attempt streaming encode + write for one key range.
///
/// Encodes records from the cursor in chunks of `CHUNK_SIZE` and writes each chunk
/// to the `DeltaWriter` immediately, avoiding buffering all `RecordBatch`es in memory.
///
/// `rows_written` accumulates how many records this attempt added to the shared
/// `records_written` counter, so the caller can roll back on failure.
///
/// Returns [`WriteError::Deterministic`] for data-dependent failures (serialization,
/// validation) and [`WriteError::Transient`] for I/O failures (object store writes).
async fn stream_encode_and_write(
    range: &dyn SerKeyRange,
    inner: &DeltaTableWriterInner,
    object_store: ObjectStoreRef,
    micros: i64,
    rows_written: &mut u64,
) -> Result<(Vec<Add>, usize), WriteError> {
    let num_indexed_cols = min(32, inner.arrow_schema.fields.len() as u64);
    let writer_config = bounded_writer_config(
        inner.arrow_schema.clone(),
        vec![],
        TARGET_FILE_SIZE,
        DataSkippingNumIndexedCols::NumColumns(num_indexed_cols),
        None,
    );
    let mut writer = DeltaWriter::new(object_store, writer_config);
    let cursor = range.cursor().map_err(WriteError::Transient)?;
    let mut rows = cursor
        .arrow_sink(inner.row_schema.clone())
        .map_err(WriteError::Deterministic)?;
    let mut ops = Vec::new();
    let mut num_records = 0;
    let index_name = inner.key_schema.as_ref().map(|s| &s.name);

    if let Some(index_name) = index_name
        && inner.is_index
    {
        let mut cursor = cursor;

        while cursor.key_valid() {
            let op =
                match indexed_operation_type(&inner.value_schema.name, index_name, cursor.as_mut())
                {
                    Ok(op) => op,
                    Err(e) => {
                        if let Some(controller) = inner.controller.upgrade() {
                            controller.output_transport_error(
                                inner.endpoint_id,
                                &inner.endpoint_name,
                                false,
                                e,
                                Some("delta_uniqueness_violation"),
                            );
                        }
                        None
                    }
                };

            if let Some(op) = op {
                cursor.rewind_vals();

                match op {
                    IndexedOperationType::Insert => {
                        cursor
                            .append_val_to_arrow(rows.as_mut())
                            .map_err(WriteError::Deterministic)?;
                        ops.push("i");
                    }
                    IndexedOperationType::Delete => {
                        cursor
                            .append_val_to_arrow(rows.as_mut())
                            .map_err(WriteError::Deterministic)?;
                        ops.push("d");
                    }
                    IndexedOperationType::Upsert => {
                        assert!(cursor.val_valid());

                        if cursor.weight() < 0 {
                            cursor.step_val();
                        }
                        assert!(cursor.val_valid());

                        cursor
                            .append_val_to_arrow(rows.as_mut())
                            .map_err(WriteError::Deterministic)?;
                        ops.push("u");
                    }
                };

                num_records += 1;

                if num_records >= CHUNK_SIZE {
                    flush_chunk(
                        rows.as_mut(),
                        &mut ops,
                        micros,
                        &mut writer,
                        inner,
                        rows_written,
                    )
                    .await?;
                    num_records = 0;
                }
            };

            cursor.step_key().map_err(WriteError::Transient)?;
        }
    } else {
        let mut cursor = if inner.key_schema.is_some() {
            CursorWithPolarity::new(Box::new(SerCursorFlattened::new(cursor)))
        } else {
            CursorWithPolarity::new(cursor)
        };

        while cursor.key_valid() {
            if !cursor.val_valid() {
                cursor.step_key().map_err(WriteError::Transient)?;
                continue;
            }

            let mut w = cursor.weight();
            if !(-MAX_DUPLICATES..=MAX_DUPLICATES).contains(&w) {
                return Err(WriteError::Deterministic(anyhow!(
                    "Unable to output record with very large weight {w}. \
                     Consider adjusting your SQL queries to avoid duplicate output records, \
                     e.g., using 'SELECT DISTINCT'."
                )));
            }

            while w != 0 {
                if w > 0 {
                    cursor
                        .append_key_to_arrow(rows.as_mut())
                        .map_err(WriteError::Deterministic)?;
                    ops.push("i");
                    w -= 1;
                } else {
                    cursor
                        .append_key_to_arrow(rows.as_mut())
                        .map_err(WriteError::Deterministic)?;
                    ops.push("d");
                    w += 1;
                }
                num_records += 1;

                if num_records >= CHUNK_SIZE {
                    flush_chunk(
                        rows.as_mut(),
                        &mut ops,
                        micros,
                        &mut writer,
                        inner,
                        rows_written,
                    )
                    .await?;
                    num_records = 0;
                }
            }
            cursor.step_key().map_err(WriteError::Transient)?;
        }
    }

    if num_records > 0 {
        flush_chunk(
            rows.as_mut(),
            &mut ops,
            micros,
            &mut writer,
            inner,
            rows_written,
        )
        .await?;
    }

    let actions = writer
        .close()
        .await
        .map_err(|e| WriteError::Transient(anyhow!("error closing writer: {e:?}")))?;
    Ok((actions, *rows_written as usize))
}

impl OutputConsumer for DeltaTableWriter {
    fn max_buffer_size_bytes(&self) -> usize {
        usize::MAX
    }

    fn batch_start(&mut self, _step: Step, _batch_type: OutputBatchType) {
        self.pending_actions.clear();
        self.num_rows = 0;
        self.inner.records_written.store(0, Ordering::Relaxed);
    }

    fn push_buffer(&mut self, _buffer: &[u8], _num_records: usize) {
        unreachable!()
    }

    fn push_key(
        &mut self,
        _key: Option<&[u8]>,
        _val: Option<&[u8]>,
        _headers: &[(&str, Option<&[u8]>)],
        _num_records: usize,
    ) {
        unreachable!()
    }

    fn batch_end(&mut self) {
        // Merge mode applied and committed the batch in `encode`, and reported it there.
        // Its progress count still ends here, so an idle endpoint does not read as busy.
        if self.merge.is_some() || self.pending_actions.is_empty() {
            self.inner.records_written.store(0, Ordering::Relaxed);
            return;
        }

        let _span = info_span!(
            "delta_output",
            endpoint = &*self.inner.endpoint_name,
            table = &*self.inner.config.uri,
        )
        .entered();

        let num_bytes: usize = self.pending_actions.iter().map(|a| a.size as usize).sum();
        let num_rows = self.num_rows;
        let actions = std::mem::take(&mut self.pending_actions);
        self.num_rows = 0;

        // Panic safety: block_on() panics if called from a tokio async context.
        // batch_end() is called from the dedicated output thread (output_thread_func).
        if let Err(e) = TOKIO.block_on(self.task.commit_with_retry(&actions)) {
            self.inner.records_written.store(0, Ordering::Relaxed);
            // A commit that the shutdown cut short is a teardown, not an
            // endpoint fault, and the batch is discarded either way.  The
            // Parquet files it wrote are orphans that `VACUUM` collects.
            if self.inner.shutdown.is_cancelled() {
                info!("delta_table {}: {e}", &self.inner.endpoint_name);
            } else if let Some(controller) = self.inner.controller.upgrade() {
                controller.output_transport_error(
                    self.inner.endpoint_id,
                    &self.inner.endpoint_name,
                    false,
                    e,
                    Some("delta_batch_end"),
                )
            };
            return;
        }

        self.inner.records_written.store(0, Ordering::Relaxed);
        if let Some(controller) = self.inner.controller.upgrade() {
            controller
                .update_output_connector_health(self.inner.endpoint_id, ConnectorHealth::healthy());
            controller
                .status
                .output_buffer(self.inner.endpoint_id, num_bytes, num_rows);
        }
    }
}

/// Stop `merge` skipping the lookup for inserts, logging why the first time.
fn forfeit_ownership(merge: &MergeWriter, endpoint_name: &str, reason: &str) {
    if merge.forfeit_ownership() {
        warn!("delta_table {endpoint_name}: {reason}; from now on every insert is looked up");
    }
}

impl DeltaTableWriter {
    /// Apply and commit one batch in merge mode, retrying transient failures.
    ///
    /// The whole flush happens here rather than committing in `batch_end` as cdc mode does,
    /// because a retry re-runs the row lookup and that needs the batch, which lives only for
    /// the duration of this call.
    fn encode_merge(&mut self, batch: Arc<dyn SerBatchReader>) -> AnyResult<()> {
        let result = self.merge_with_retries(batch);
        if result.is_err() {
            // The controller drops a failed batch, so its changes never reach the table.
            self.merge_reload = true;
            let merge = self.merge.as_ref().expect("caller checked for merge mode");
            forfeit_ownership(
                merge,
                &self.inner.endpoint_name,
                "a batch could not be applied",
            );
        }
        result
    }

    fn merge_with_retries(&mut self, batch: Arc<dyn SerBatchReader>) -> AnyResult<()> {
        // Destructured so the merge writer and the table can be borrowed at once.
        let Self {
            inner,
            object_store,
            task,
            merge,
            merge_metrics,
            compactor,
            threads,
            #[cfg(any(test, feature = "testing"))]
            merge_violations,
            merge_reload,
            ..
        } = self;
        let merge = merge.as_ref().expect("caller checked for merge mode");
        let metrics_sink = merge_metrics
            .as_ref()
            .expect("merge mode registers its metrics");

        let span = info_span!(
            "delta_output_merge",
            endpoint = &*inner.endpoint_name,
            table = &*inner.config.uri,
        );
        let format =
            RecordFormat::Parquet(delta_output_serde_config(inner.config.variant_encoding));
        // Run beside this flush, the step the last commit started would race it.
        if let Some(compactor) = compactor.as_mut()
            && compactor.wait_for_step()
        {
            *merge_reload = true;
        }

        let mut retry_count: u32 = 0;
        let mut backoff = Duration::from_secs(1);
        let max_backoff = Duration::from_secs(10);

        loop {
            let retrying = retry_count > 0;
            // A retry rewrites the same rows, so its progress restarts rather than adds.
            inner.records_written.store(0, Ordering::Relaxed);
            let mut report_violation = |e: anyhow::Error| {
                // The key is skipped, so the table keeps its old row: a later insert of that
                // key must find it.
                forfeit_ownership(merge, &inner.endpoint_name, "a key was skipped");
                // First attempt only: a retry walks the same batch and would report the
                // same keys again, once per attempt, for as long as the commit fails.
                if retrying {
                    return;
                }
                #[cfg(any(test, feature = "testing"))]
                {
                    *merge_violations += 1;
                }
                if let Some(controller) = inner.controller.upgrade() {
                    controller.output_transport_error(
                        inner.endpoint_id,
                        &inner.endpoint_name,
                        false,
                        e,
                        Some("delta_uniqueness_violation"),
                    );
                }
            };

            // Panic safety: block_on() panics if called from a tokio async context.
            // encode() is called from the dedicated output thread (output_thread_func).
            let started = Instant::now();
            // Cancellable as a whole, as the cdc write is: a flush over many files or a slow
            // store would otherwise hold the teardown. A dropped flush is a crash mid-flush.
            let result = TOKIO.block_on(
                inner.shutdown.run_until_cancelled(
                    async {
                        // A failed attempt may have committed anyway: a lookup in the old snapshot
                        // misses its rows and appends them twice, so a failed reload fails the retry.
                        if retrying || *merge_reload {
                            task.delta_table
                                .update_incremental(None)
                                .await
                                .map_err(|e| {
                                    transient(format!("reloading the Delta table: {e}"))
                                })?;
                            // A reload can bring in an ALTER TABLE, but the writer's schema and key
                            // encoder are fixed at startup, so recheck what startup required.
                            check_column_mapping(&task.delta_table)
                                .and_then(|()| {
                                    merge.check_table(&task.delta_table, &inner.struct_fields)
                                })
                                .map_err(|e| {
                                    WriteError::Deterministic(anyhow!(
                                        "the Delta table changed while the pipeline was running, \
                                     and merge mode can no longer write to it: {e:#}\n\
                                     Undo the change, or adapt the view to it, then restart \
                                     the pipeline."
                                    ))
                                })?;
                        }
                        merge
                            .flush(
                                &mut task.delta_table,
                                object_store.clone(),
                                batch.clone(),
                                format.clone(),
                                *threads,
                                retrying,
                                &mut report_violation,
                                inner.records_written.clone(),
                            )
                            .await
                    }
                    .instrument(span.clone()),
                ),
            );
            // Recorded whatever the outcome: a flush that fails slowly is the interesting one.
            metrics_sink.record_flush_latency(started.elapsed());
            let Some(result) = result else {
                inner.records_written.store(0, Ordering::Relaxed);
                return Err(shutdown_aborted(
                    "merging a batch into the Delta table",
                    retry_count,
                ));
            };

            match result {
                Ok(metrics) => {
                    *merge_reload = false;
                    metrics_sink.record(&metrics);
                    metrics_sink.record_tombstone_ratio(
                        metrics.table_live_rows,
                        metrics.table_superseded_rows,
                        &inner.endpoint_name,
                        &inner.config.uri,
                    );
                    if let Some(controller) = inner.controller.upgrade() {
                        controller.update_output_connector_health(
                            inner.endpoint_id,
                            ConnectorHealth::healthy(),
                        );
                        // Both sides, matching what cdc mode reports: it writes a row per
                        // delete, so counting only appends would make a delete-only flush
                        // look idle.
                        controller.status.output_buffer(
                            inner.endpoint_id,
                            metrics.bytes_written as usize,
                            (metrics.rows_appended + metrics.dv.rows_tombstoned) as usize,
                        );
                    }
                    // Not waited for, so the batch is acknowledged now.
                    if let Some(compactor) = compactor.as_mut() {
                        compactor.after_commit(&task.delta_table, &inner.shutdown);
                    }

                    debug!(
                        "delta_table {}: merged batch ({} rows appended, {} rows tombstoned in \
                         {} file(s), {} keys probed in {} pass(es), {} not found)",
                        inner.endpoint_name,
                        metrics.rows_appended,
                        metrics.dv.rows_tombstoned,
                        metrics.dv.files_touched + metrics.dv.files_dropped,
                        metrics.keys_probed,
                        metrics.lookup_passes,
                        metrics.probe.keys_not_found,
                    );
                    return Ok(());
                }
                // Retrying replays the same rows against the same schema, so it can only
                // burn the pipeline's time.  Without this the default `max_retries: None`
                // makes one bad batch retry for ever.
                Err(WriteError::Deterministic(e)) => {
                    inner.records_written.store(0, Ordering::Relaxed);
                    return Err(anyhow!(
                        "merging a batch into the Delta table failed and cannot succeed on \
                         retry: {e:#}"
                    ));
                }
                Err(WriteError::Transient(e))
                    if inner.config.max_retries.is_none()
                        || retry_count < inner.config.max_retries.unwrap() =>
                {
                    retry_count += 1;
                    let message = format!(
                        "merging a batch into the Delta table failed (attempt {retry_count}, \
                         retrying in {backoff:?}): {e:?}"
                    );
                    if let Some(controller) = inner.controller.upgrade() {
                        controller.update_output_connector_health(
                            inner.endpoint_id,
                            ConnectorHealth::unhealthy(&message),
                        );
                    }
                    warn!("delta_table {}: {message}", inner.endpoint_name);
                    let shutdown = inner.shutdown.clone();
                    // Constructed inside the runtime, which `sleep` requires.
                    let slept = TOKIO
                        .block_on(shutdown.run_until_cancelled(async { sleep(backoff).await }));
                    // The loop is unbounded by default, and it runs on a thread the runtime
                    // joins on teardown, so it has to stop when the pipeline does.
                    if slept.is_none() {
                        inner.records_written.store(0, Ordering::Relaxed);
                        return Err(shutdown_aborted(
                            "merging a batch into the Delta table",
                            retry_count,
                        ));
                    }
                    backoff = min(backoff * 2, max_backoff);
                }
                // Named rather than a catch-all, so `{e:#}` reaches anyhow's chain instead
                // of stopping at `WriteError`'s own `Display`.
                Err(WriteError::Transient(e)) => {
                    inner.records_written.store(0, Ordering::Relaxed);
                    return Err(anyhow!(
                        "merging a batch into the Delta table failed after {retry_count} \
                         retries: {e:#}"
                    ));
                }
            }
        }
    }
}

impl Encoder for DeltaTableWriter {
    fn consumer(&mut self) -> &mut dyn OutputConsumer {
        self
    }

    fn encode(&mut self, batch: Arc<dyn SerBatchReader>) -> AnyResult<()> {
        if self.merge.is_some() {
            return self.encode_merge(batch);
        }

        let ranges = batch.split(
            self.threads,
            RecordFormat::Parquet(delta_output_serde_config(
                self.inner.config.variant_encoding,
            )),
        )?;
        if ranges.is_empty() {
            return Ok(());
        }

        let micros = Utc::now().timestamp_micros();

        let span = info_span!(
            "delta_output",
            endpoint = &*self.inner.endpoint_name,
            table = &*self.inner.config.uri,
        );

        // Panic safety: block_on() panics if called from a tokio async context.
        // encode() is called from the dedicated output thread (output_thread_func).
        let results = TOKIO.block_on(async {
            let mut handles = Vec::with_capacity(ranges.len());
            for range in ranges {
                let inner = self.inner.clone();
                let object_store = self.object_store.clone();
                handles.push(tokio::spawn(
                    encode_and_write_range(range, inner, object_store, micros)
                        .instrument(span.clone()),
                ));
            }
            let mut results = Vec::with_capacity(handles.len());
            for handle in handles {
                results.push(
                    handle
                        .await
                        .unwrap_or_else(|e| Err(anyhow!("write task panicked: {e}"))),
                );
            }
            results
        });

        let mut errors = Vec::new();
        let mut succeeded_ranges = 0usize;
        for result in results {
            match result {
                Ok((mut actions, rows)) => {
                    self.pending_actions.append(&mut actions);
                    self.num_rows += rows;
                    succeeded_ranges += 1;
                }
                Err(e) => errors.push(e),
            }
        }
        if !errors.is_empty() {
            if succeeded_ranges > 0 {
                warn!(
                    "delta_table {}: {} range(s) succeeded but {} failed; \
                     dropping {} file action(s) from this commit (orphaned files will be cleaned up by VACUUM)",
                    self.inner.endpoint_name,
                    succeeded_ranges,
                    errors.len(),
                    self.pending_actions.len(),
                );
            }
            self.pending_actions.clear();
            self.num_rows = 0;
            // Failed ranges already rolled back their own contributions, but
            // successful ranges' records are still counted. Since we're dropping
            // all actions, reset to 0.
            self.inner.records_written.store(0, Ordering::Relaxed);
            let msg = errors
                .iter()
                .map(|e| format!("{e:#}"))
                .collect::<Vec<_>>()
                .join("; ");

            bail!("{} write task(s) failed: {msg}", errors.len());
        }

        Ok(())
    }
}

impl OutputEndpoint for DeltaTableWriter {
    fn connect(&mut self, _async_error_callback: AsyncErrorCallback) -> AnyResult<()> {
        todo!()
    }

    fn max_buffer_size_bytes(&self) -> usize {
        todo!()
    }

    fn batch_start(&mut self, _step: Step, _batch_type: OutputBatchType) -> AnyResult<()> {
        unreachable!()
    }

    fn push_buffer(&mut self, _buffer: &[u8]) -> AnyResult<()> {
        unreachable!()
    }

    fn push_key(
        &mut self,
        _key: Option<&[u8]>,
        _val: Option<&[u8]>,
        _headers: &[(&str, Option<&[u8]>)],
    ) -> AnyResult<()> {
        unreachable!()
    }

    fn batch_end(&mut self) -> AnyResult<()> {
        // flush/commit anything
        unreachable!()
    }

    fn is_fault_tolerant(&self) -> bool {
        // TODO: make this connector fault tolerant.  Delta tables already allow atomic
        // updates, we just need to record the step-to-table-snapshot mapping somewhere.
        false
    }
}

#[cfg(any(test, feature = "testing"))]
pub mod tests;

/// Tests of the writer that reach no engine. The `delta_output` connector suite covers the
/// writer with an engine.
#[cfg(test)]
mod parallel {
    use std::path::Path;
    use std::sync::Weak;
    use std::sync::atomic::Ordering;

    use feldera_types::transport::delta_table::{DeltaTableWriteMode, DeltaTableWriterConfig};
    use tempfile::TempDir;
    use tokio_util::sync::CancellationToken;

    use super::DeltaTableWriter;
    use super::tests::{
        key_relation, make_endpoint, make_merge_endpoint, records_written, try_make_endpoint,
        value_relation,
    };
    use crate::controller::EndpointId;

    /// Only merge mode caps part uploads. A shared cap halved cdc throughput against a store
    /// with 150 ms part latency, so cdc keeps main's per-file concurrency.
    #[test]
    fn only_merge_mode_caps_part_uploads() {
        let dir = TempDir::new().unwrap();
        let cdc = make_endpoint(4, dir.path().to_str().unwrap(), true);
        assert!(!cdc.object_store.to_string().starts_with("BoundedUpload"));

        let dir = TempDir::new().unwrap();
        let merge = make_merge_endpoint(dir.path().to_str().unwrap(), DeltaTableWriteMode::Append);
        assert!(merge.object_store.to_string().starts_with("BoundedUpload"));
    }

    /// Merge mode without output buffering pays a lookup per step of the circuit, so it is
    /// worth a word at startup. Only merge mode, and only when buffering is off.
    #[test]
    fn unbuffered_merge_is_worth_a_word() {
        let of = |mode: &str| -> DeltaTableWriterConfig {
            serde_json::from_value(serde_json::json!({"uri": "/tmp/t", "update_mode": mode}))
                .unwrap()
        };
        let config = of("merge");
        assert!(super::unbuffered_merge_advice(&config, false).is_some());
        assert!(super::unbuffered_merge_advice(&config, true).is_none());

        assert!(
            super::unbuffered_merge_advice(&of("cdc"), false).is_none(),
            "cdc mode appends without reading the table, so buffering is only a file-size \
             question and the existing documentation covers it"
        );
    }

    /// [AttemptProgress] takes an attempt's rows back out of the shared
    /// progress counter unless the attempt succeeded.
    ///
    /// This is the whole reason the struct exists: a shutdown drops the write
    /// future mid-flight, and the row count lives inside that future, where no
    /// caller can reach it to roll it back.
    #[test]
    fn a_dropped_attempt_rolls_its_rows_back() {
        let table_dir = TempDir::new().unwrap();
        let table_uri = table_dir.path().display().to_string();
        let endpoint = make_endpoint(1, &table_uri, true);
        let inner = &*endpoint.inner;

        // An attempt that never reports success, which is what a dropped
        // write leaves behind.
        {
            let mut progress = super::AttemptProgress {
                inner,
                rows_written: 0,
                succeeded: false,
            };
            inner.records_written.fetch_add(7, Ordering::Relaxed);
            progress.rows_written = 7;
        }
        assert_eq!(records_written(&endpoint), 0);

        // A successful attempt keeps its rows.
        {
            let mut progress = super::AttemptProgress {
                inner,
                rows_written: 0,
                succeeded: false,
            };
            inner.records_written.fetch_add(7, Ordering::Relaxed);
            progress.rows_written = 7;
            progress.succeeded = true;
        }
        assert_eq!(records_written(&endpoint), 7);
    }

    /// Appending to a column-mapped table must fail, not write rows that read back as NULL.
    /// delta-rs refused the table before 2.0; `check_column_mapping` does now.
    #[test]
    fn appending_to_a_column_mapped_table_is_refused() {
        let table_dir = TempDir::new().unwrap();
        let table_uri = table_dir.path().display().to_string();

        // Let the connector create the table, then turn column mapping on.
        drop(make_endpoint(1, &table_uri, true));
        enable_column_mapping(&table_uri);

        let error = try_make_endpoint(
            1,
            &table_uri,
            true,
            DeltaTableWriteMode::Append,
            false,
            Some(0),
        )
        .err()
        .expect("appending to a column-mapped table must fail");
        let error = error.to_string();
        assert!(
            error.contains("columnMapping"),
            "the error must name column mapping, got: {error}"
        );
    }

    #[test]
    fn test_non_indexed_rejects_multi_thread() {
        let table_dir = TempDir::new().unwrap();
        let table_uri = table_dir.path().display().to_string();
        let key_schema = None;
        let result = DeltaTableWriter::new(
            EndpointId::default(),
            "test_endpoint",
            &DeltaTableWriterConfig {
                variant_encoding: Default::default(),
                uri: table_uri,
                mode: DeltaTableWriteMode::Truncate,
                max_retries: Some(0),
                threads: Some(4),
                auto_compact: false,

                object_store_config: Default::default(),
                checkpoint_interval: None,
                log_retention_duration: None,
                update_mode: Default::default(),
                lookup_chunk_size: feldera_types::size::ConfigSize::from_mib(1),
                max_concurrent_probes: 4,
                enable_expired_log_cleanup: None,
            },
            &key_schema,
            &value_relation(),
            Weak::new(),
            CancellationToken::new(),
            false,
            false,
            true,
        );
        assert!(
            result.is_err(),
            "threads > 1 without key_schema should be rejected"
        );
    }

    /// `log_retention_duration` and `enable_expired_log_cleanup` should land on the created
    /// Delta table's metadata when set, and be absent when not set.
    #[test]
    fn test_log_retention_table_properties() {
        use dbsp::circuit::tokio::TOKIO;
        use deltalake::open_table;
        use std::time::Duration;

        // Case 1: neither option set — neither property should appear in the table metadata.
        // `TempDir` is kept in scope until end of test; its `Drop` removes the directory once
        // both `_endpoint` and the `open_table` future have finished using it.
        let table_dir = TempDir::new().unwrap();
        let table_uri = table_dir.path().display().to_string();
        let _endpoint = make_endpoint(1, &table_uri, true);

        let url = url::Url::from_file_path(&table_uri).unwrap();
        let table = TOKIO.block_on(async move { open_table(url).await.unwrap() });
        let config = table.snapshot().unwrap().table_config();
        assert!(
            config.log_retention_duration.is_none(),
            "logRetentionDuration should not be set when option is unset"
        );
        assert!(
            config.enable_expired_log_cleanup.is_none(),
            "enableExpiredLogCleanup should not be set when option is unset"
        );

        // Case 2: both options set — both properties should be reflected in the table metadata.
        let table_dir = TempDir::new().unwrap();
        let table_uri = table_dir.path().display().to_string();
        let _endpoint = DeltaTableWriter::new(
            EndpointId::default(),
            "test_endpoint",
            &DeltaTableWriterConfig {
                variant_encoding: Default::default(),
                uri: table_uri.clone(),
                mode: DeltaTableWriteMode::Truncate,
                max_retries: Some(0),
                threads: Some(1),
                auto_compact: false,

                object_store_config: Default::default(),
                checkpoint_interval: None,
                log_retention_duration: Some("interval 7 days".to_string()),
                update_mode: Default::default(),
                lookup_chunk_size: feldera_types::size::ConfigSize::from_mib(1),
                max_concurrent_probes: 4,
                enable_expired_log_cleanup: Some(false),
            },
            &Some(key_relation()),
            &value_relation(),
            Weak::new(),
            CancellationToken::new(),
            false,
            true,
            true,
        )
        .expect("failed to create endpoint");

        let url = url::Url::from_file_path(&table_uri).unwrap();
        let table = TOKIO.block_on(async move { open_table(url).await.unwrap() });
        let config = table.snapshot().unwrap().table_config();
        assert_eq!(
            config.log_retention_duration,
            Some(Duration::from_secs(7 * 24 * 60 * 60)),
            "logRetentionDuration should match the configured interval",
        );
        assert_eq!(
            config.enable_expired_log_cleanup,
            Some(false),
            "enableExpiredLogCleanup should be set to false",
        );
    }

    /// Verify that threads=0 is rejected in config validation.
    #[test]
    fn test_threads_zero_rejected() {
        let config = DeltaTableWriterConfig {
            variant_encoding: Default::default(),
            uri: "/tmp/test".to_string(),
            mode: DeltaTableWriteMode::Truncate,
            max_retries: Some(0),
            threads: Some(0),
            auto_compact: false,

            object_store_config: Default::default(),
            checkpoint_interval: None,
            log_retention_duration: None,
            update_mode: Default::default(),
            lookup_chunk_size: feldera_types::size::ConfigSize::from_mib(1),
            max_concurrent_probes: 4,
            enable_expired_log_cleanup: None,
        };
        assert!(config.validate().is_err());
    }

    /// Give `field` and every field nested inside it a physical name, the way
    /// a writer that turns column mapping on rewrites the schema.
    fn annotate_column_mapping(field: &mut serde_json::Value, next_id: &mut i64) {
        *next_id += 1;
        field["metadata"]["delta.columnMapping.id"] = serde_json::json!(*next_id);
        field["metadata"]["delta.columnMapping.physicalName"] =
            serde_json::json!(format!("col-{next_id}"));
        annotate_nested(&mut field["type"], next_id);
    }

    /// Walk a Delta type, annotating the fields of every struct under it.
    fn annotate_nested(data_type: &mut serde_json::Value, next_id: &mut i64) {
        let Some(kind) = data_type.get("type").and_then(|t| t.as_str()) else {
            return;
        };
        match kind {
            "struct" => {
                for field in data_type["fields"].as_array_mut().unwrap() {
                    annotate_column_mapping(field, next_id);
                }
            }
            "array" => annotate_nested(&mut data_type["elementType"], next_id),
            "map" => {
                annotate_nested(&mut data_type["keyType"], next_id);
                annotate_nested(&mut data_type["valueType"], next_id);
            }
            _ => {}
        }
    }

    /// Rewrite version 0 of `table_uri` so the table declares
    /// `delta.columnMapping.mode = 'name'`, giving every column, nested ones
    /// included, a physical name of its own, the way a table written with
    /// column mapping on looks.
    fn enable_column_mapping(table_uri: &str) {
        let log = Path::new(table_uri)
            .join("_delta_log")
            .join("00000000000000000000.json");
        let mut out = Vec::new();
        for line in std::fs::read_to_string(&log).unwrap().lines() {
            let mut action: serde_json::Value = serde_json::from_str(line).unwrap();
            if let Some(protocol) = action.get_mut("protocol") {
                // The connector creates a table-features protocol (3/7), so
                // column mapping is declared as a feature rather than by
                // raising the version.
                for features in ["readerFeatures", "writerFeatures"] {
                    let list = protocol[features]
                        .as_array_mut()
                        .expect("the created table declares feature lists");
                    list.push(serde_json::json!("columnMapping"));
                }
            }
            if let Some(metadata) = action.get_mut("metaData") {
                metadata["configuration"]["delta.columnMapping.mode"] = serde_json::json!("name");
                let mut schema: serde_json::Value =
                    serde_json::from_str(metadata["schemaString"].as_str().unwrap()).unwrap();
                let mut next_id = 0i64;
                for field in schema["fields"].as_array_mut().unwrap() {
                    annotate_column_mapping(field, &mut next_id);
                }
                metadata["configuration"]["delta.columnMapping.maxColumnId"] =
                    serde_json::json!(next_id);
                metadata["schemaString"] = serde_json::json!(schema.to_string());
            }
            out.push(serde_json::to_string(&action).unwrap());
        }
        std::fs::write(&log, out.join("\n") + "\n").unwrap();
    }
}
