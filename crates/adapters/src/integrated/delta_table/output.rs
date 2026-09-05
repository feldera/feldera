use crate::catalog::{CursorWithPolarity, SerBatchReader, SplitCursorBuilder};
use crate::controller::{ControllerInner, EndpointId};
use crate::format::MAX_DUPLICATES;
use crate::format::parquet::{ArrowSchemaOptions, relation_to_arrow_fields};
use crate::integrated::delta_table::merge::compact::Compactor;
use crate::integrated::delta_table::merge::flush::MergeWriter;
use crate::integrated::delta_table::merge::metrics::MergeMetrics;
use crate::integrated::delta_table::merge::startup;
use crate::integrated::delta_table::register_storage_handlers;
use crate::transport::Step;
use crate::util::{IndexedOperationType, indexed_operation_type};
use crate::{
    AsyncErrorCallback, ControllerError, Encoder, OutputConsumer, OutputEndpoint, RecordFormat,
    SerCursor,
};
use anyhow::{Result as AnyResult, anyhow, bail};
use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use chrono::Utc;
use dbsp::circuit::tokio::TOKIO;
use delta_kernel::engine::arrow_conversion::TryFromArrow;
use delta_kernel::table_properties::DataSkippingNumIndexedCols;
use deltalake::DeltaTable;
use deltalake::kernel::transaction::{CommitBuilder, CommitProperties, TableReference};
use deltalake::kernel::{Action, Add, ArrayType, DataType, MapType, StructField, StructType};
use deltalake::logstore::ObjectStoreRef;
use deltalake::operations::create::CreateBuilder;
use deltalake::operations::write::writer::{DeltaWriter, WriterConfig};
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
use serde::Serialize;
use serde_arrow::ArrayBuilder;
use serde_arrow::schema::SerdeArrowSchema;
use std::cmp::min;
use std::collections::HashMap;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use tokio::time::{Duration, sleep};
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
                nested.push(StructField::new(
                    field.name(),
                    delta_variant_types(&sql_field.columntype, field.data_type().clone())?,
                    field.is_nullable(),
                ));
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

struct DeltaTableWriterInner {
    endpoint_id: EndpointId,
    endpoint_name: String,
    config: DeltaTableWriterConfig,
    serde_arrow_schema: SerdeArrowSchema,
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
    merge: Option<MergeWriter>,
    merge_metrics: Option<Arc<MergeMetrics>>,
    /// Present when `optimize_interval_secs` asks the connector to compact the table itself.
    compactor: Option<Compactor>,
    /// Uniqueness violations handed to the controller, for the test that pins how often a
    /// retried batch reports them. Tests build the endpoint with no controller to report to.
    #[cfg(test)]
    merge_violations: u64,
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
const TARGET_FILE_SIZE: NonZeroU64 = NonZeroU64::new(100 * 1024 * 1024).unwrap();

impl DeltaTableWriter {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        endpoint_id: EndpointId,
        endpoint_name: &str,
        config: &DeltaTableWriterConfig,
        key_schema: &Option<Relation>,
        value_schema: &Relation,
        controller: Weak<ControllerInner>,
        continue_previous_state: bool,
        is_index: bool,
    ) -> Result<Self, ControllerError> {
        config.validate().map_err(|e| {
            ControllerError::invalid_transport_configuration(endpoint_name, &e.to_string())
        })?;

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
        if !config.is_merge() {
            arrow_fields.push(ArrowField::new("__feldera_op", ArrowDataType::Utf8, true));
            arrow_fields.push(ArrowField::new("__feldera_ts", ArrowDataType::Int64, true));
        }

        // Create serde arrow schema.
        let serde_arrow_schema =
            SerdeArrowSchema::try_from(arrow_fields.as_slice()).map_err(|e| {
                ControllerError::SchemaParseError {
                    error: format!("Unable to convert schema to parquet/arrow: {e}"),
                }
            })?;

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
            serde_arrow_schema,
            arrow_schema,
            struct_fields,
            key_schema: key_schema.clone(),
            value_schema: value_schema.clone(),
            controller,
            records_written: Arc::new(AtomicU64::new(0)),
            is_index,
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
                        &config.uri
                    ),
                )
            })?;

        let object_store = task.delta_table.object_store();

        let merge = if config.is_merge() {
            Some(
                build_merge_writer(&inner, &task.delta_table, threads).map_err(|e| {
                    ControllerError::invalid_transport_configuration(
                        endpoint_name,
                        &format!("{e:#}"),
                    )
                })?,
            )
        } else {
            None
        };

        // Registered here rather than in the endpoint constructor: `add_output` has already
        // run, so the metrics slot exists, and registering before it does drops them.
        let merge_metrics = merge.is_some().then(MergeMetrics::new);
        let compactor = merge
            .is_some()
            .then(|| Compactor::new(config, endpoint_name))
            .flatten();
        if let (Some(metrics), Some(controller)) = (&merge_metrics, inner.controller.upgrade()) {
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
            merge,
            merge_metrics,
            compactor,
            #[cfg(test)]
            merge_violations: 0,
        })
    }
}

/// Check the target table and build the merge-mode writer.
fn build_merge_writer(
    inner: &DeltaTableWriterInner,
    table: &DeltaTable,
    threads: usize,
) -> AnyResult<MergeWriter> {
    let setup = startup::prepare(table, &inner.key_schema, &inner.struct_fields, threads)?;

    MergeWriter::new(
        setup,
        inner
            .key_schema
            .as_ref()
            .expect("startup::prepare rejects a missing key schema"),
        inner.serde_arrow_schema.clone(),
        inner.arrow_schema.clone(),
        inner.config.lookup_chunk_bytes,
        inner.config.max_concurrent_probes,
        inner.value_schema.name.clone(),
    )
}

struct WriterTask {
    inner: Arc<DeltaTableWriterInner>,
    delta_table: DeltaTable,
}

/// Retry `op` with exponential backoff  of up to 10 seconds until it succeeds or config.max_retries is reached.
///
/// `warn!` and set health status to unhealthy on each failure, clear the health status on success.
macro_rules! retry {
    ($self:ident, $description:expr, $op:expr) => {{
        let mut retry_count = 0;
        let mut backoff = Duration::from_secs(1);
        let max_backoff = Duration::from_secs(10);
        loop {
            match $op {
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
                    break Ok(result);
                }
                Err(e) if $self.inner.config.max_retries.is_none() || retry_count < $self.inner.config.max_retries.unwrap() => {
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

                    break Err(anyhow!(message));
                }
            }
        }
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
        let mut storage_options = inner.config.object_store_config.clone();

        // FIXME: S3 does not support the atomic rename operation required by delta. This is not a problem
        // with a single writer, but multiple writers require an external coordinator service.
        // `delta-rs` users tend to rely on the DynamoDB lock client for this
        // (see `object_store::aws::DynamoCommit`), but that only helps if all writers use the
        // same lock service.  For now we simply tell the object store client to use unsafe renames
        // and hope for the best.  Without this config option, writes to S3-based delta tables will fail.
        storage_options.insert("AWS_S3_ALLOW_UNSAFE_RENAME".to_string(), "true".to_string());

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
                                &inner.config.uri
                            ));
                        }
                    }
                    Err(_timeout) => {
                        if retry_count >= MAX_RETRIES {
                            return Err(anyhow!(
                                "timeout creating or opening delta table '{}' after {retry_count} attempts",
                                &inner.config.uri,
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
            self.commit(actions).await
        )
    }
}

/// Error classification for Delta table write operations.
///
/// Separates deterministic failures (which will recur on every attempt) from
/// transient I/O failures (which may succeed on retry).
enum WriteError {
    /// Data-dependent error that will recur identically on retry.
    /// Examples: non-unique keys, schema mismatches, serialization failures.
    Deterministic(anyhow::Error),
    /// Transient I/O error that may resolve on retry.
    /// Examples: object store timeouts, network failures.
    Transient(anyhow::Error),
}

/// Encode a key range and stream-write it to a `DeltaWriter`, retrying transient failures.
///
/// On retry, a fresh cursor is rebuilt from `cursor_builder` and a new `DeltaWriter`
/// is created. Any Parquet files written by a failed attempt become orphans that
/// Delta `VACUUM` will clean up.
///
/// Only transient I/O errors are retried; deterministic errors (e.g., non-unique keys,
/// serialization failures) are returned immediately.
async fn encode_and_write_range(
    cursor_builder: SplitCursorBuilder,
    inner: Arc<DeltaTableWriterInner>,
    object_store: ObjectStoreRef,
    micros: i64,
) -> AnyResult<(Vec<Add>, usize)> {
    // This function has its own retry loop instead of using the `retry!` macro because:
    // Multiple ranges run in parallel; the `retry!` macro clears the connector
    // health status on success, which would be incorrect here; a single range
    // succeeding must not mask failures in other ranges.
    let mut retry_count: u32 = 0;
    let mut backoff = Duration::from_secs(1);
    let max_backoff = Duration::from_secs(10);

    loop {
        let mut rows_written: u64 = 0;

        match stream_encode_and_write(
            &cursor_builder,
            &inner,
            object_store.clone(),
            micros,
            &mut rows_written,
        )
        .await
        {
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
                rollback_progress(&inner, rows_written);
                return Err(e);
            }
            Err(WriteError::Transient(e))
                if inner.config.max_retries.is_none()
                    || retry_count < inner.config.max_retries.unwrap() =>
            {
                rollback_progress(&inner, rows_written);
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
                rollback_progress(&inner, rows_written);
                return Err(anyhow!(
                    "Delta table write failed after {retry_count} retries: {e}"
                ));
            }
        }
    }
}

/// Subtract a failed attempt's contribution from the shared progress counter.
///
/// On retry or terminal failure, this subtracts exactly what the failed
/// attempt added (its `total_rows`) — no interference with other ranges.
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

/// Build a `RecordBatch` from `builder` (deterministic), write it via `writer`
/// (transient I/O), and report progress.
///
/// `rows_written` accumulates the number of records successfully written to the
/// object store across all chunks in this attempt. The caller uses it both as
/// the return value (on success) and for rollback (on failure).
async fn flush_chunk(
    builder: &mut ArrayBuilder,
    writer: &mut DeltaWriter,
    inner: &DeltaTableWriterInner,
    rows_written: &mut u64,
) -> Result<(), WriteError> {
    let batch = builder
        .to_record_batch()
        .map_err(|e| WriteError::Deterministic(anyhow!("error generating arrow arrays: {e}")))?;
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
    cursor_builder: &SplitCursorBuilder,
    inner: &DeltaTableWriterInner,
    object_store: ObjectStoreRef,
    micros: i64,
    rows_written: &mut u64,
) -> Result<(Vec<Add>, usize), WriteError> {
    let num_indexed_cols = min(32, inner.arrow_schema.fields.len() as u64);
    let writer_config = WriterConfig::new(
        inner.arrow_schema.clone(),
        vec![],
        None,
        Some(TARGET_FILE_SIZE),
        None,
        DataSkippingNumIndexedCols::NumColumns(num_indexed_cols),
        None,
    );
    let mut writer = DeltaWriter::new(object_store, writer_config);
    let mut insert_builder = ArrayBuilder::new(inner.serde_arrow_schema.clone())
        .map_err(|e| WriteError::Deterministic(anyhow!("error creating array builder: {e}")))?;
    let mut num_records = 0;
    let index_name = inner.key_schema.as_ref().map(|s| &s.name);

    if let Some(index_name) = index_name
        && inner.is_index
    {
        let mut cursor = cursor_builder.build();

        while cursor.key_valid() {
            let op = match indexed_operation_type(&inner.value_schema.name, index_name, &mut cursor)
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
                    IndexedOperationType::Insert => cursor
                        .serialize_val_to_arrow_with_metadata(
                            &Meta::new("i", micros),
                            &mut insert_builder,
                        )
                        .map_err(WriteError::Deterministic)?,
                    IndexedOperationType::Delete => cursor
                        .serialize_val_to_arrow_with_metadata(
                            &Meta::new("d", micros),
                            &mut insert_builder,
                        )
                        .map_err(WriteError::Deterministic)?,
                    IndexedOperationType::Upsert => {
                        assert!(cursor.val_valid());

                        if cursor.weight() < 0 {
                            cursor.step_val();
                        }
                        assert!(cursor.val_valid());

                        cursor
                            .serialize_val_to_arrow_with_metadata(
                                &Meta::new("u", micros),
                                &mut insert_builder,
                            )
                            .map_err(WriteError::Deterministic)?;
                    }
                };

                num_records += 1;

                if num_records >= CHUNK_SIZE {
                    flush_chunk(&mut insert_builder, &mut writer, inner, rows_written).await?;
                    num_records = 0;
                }
            };

            cursor.step_key();
        }
    } else {
        let cursor = cursor_builder.build();

        let mut cursor = if inner.key_schema.is_some() {
            CursorWithPolarity::new(Box::new(SerCursorFlattened::new(Box::new(cursor))))
        } else {
            CursorWithPolarity::new(Box::new(cursor))
        };

        while cursor.key_valid() {
            if !cursor.val_valid() {
                cursor.step_key();
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
                        .serialize_key_to_arrow_with_metadata(
                            &Meta::new("i", micros),
                            &mut insert_builder,
                        )
                        .map_err(WriteError::Deterministic)?;
                    w -= 1;
                } else {
                    cursor
                        .serialize_key_to_arrow_with_metadata(
                            &Meta::new("d", micros),
                            &mut insert_builder,
                        )
                        .map_err(WriteError::Deterministic)?;
                    w += 1;
                }
                num_records += 1;

                if num_records >= CHUNK_SIZE {
                    flush_chunk(&mut insert_builder, &mut writer, inner, rows_written).await?;
                    num_records = 0;
                }
            }
            cursor.step_key();
        }
    }

    if num_records > 0 {
        flush_chunk(&mut insert_builder, &mut writer, inner, rows_written).await?;
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
        if self.merge.is_some() || self.pending_actions.is_empty() {
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
            if let Some(controller) = self.inner.controller.upgrade() {
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

/// Metadata added to each record, representing the type and order of operations.
#[derive(Serialize)]
struct Meta<'a> {
    /// `i` for insert, `d` for delete, `u` for update.
    __feldera_op: &'a str,

    /// Timestamp in microseconds since UNIX epoch when the batch of updates
    /// was output by the pipeline.
    __feldera_ts: i64,
}

impl<'a> Meta<'a> {
    fn new(op: &'a str, ts: i64) -> Self {
        Meta {
            __feldera_op: op,
            __feldera_ts: ts,
        }
    }
}

impl DeltaTableWriter {
    /// Apply and commit one batch in merge mode, retrying transient failures.
    ///
    /// The whole flush happens here rather than committing in `batch_end` as cdc mode does,
    /// because a retry re-runs the row lookup and that needs the batch, which lives only for
    /// the duration of this call.
    fn encode_merge(&mut self, batch: Arc<dyn SerBatchReader>) -> AnyResult<()> {
        // Destructured so the merge writer and the table can be borrowed at once.
        let Self {
            inner,
            object_store,
            task,
            merge,
            merge_metrics,
            compactor,
            #[cfg(test)]
            merge_violations,
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

        let mut retry_count: u32 = 0;
        let mut backoff = Duration::from_secs(1);
        let max_backoff = Duration::from_secs(10);

        loop {
            let mut cursor = batch.cursor(format.clone())?;
            // First attempt only: a retry walks the same batch and would report the same
            // keys again, once per attempt, for as long as the commit keeps conflicting.
            let first_attempt = retry_count == 0;
            let mut report_violation = |e: anyhow::Error| {
                if !first_attempt {
                    return;
                }
                #[cfg(test)]
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
            let result = TOKIO.block_on(
                merge
                    .flush(
                        &mut task.delta_table,
                        object_store.clone(),
                        &mut *cursor,
                        &mut report_violation,
                    )
                    .instrument(span.clone()),
            );

            match result {
                Ok(metrics) => {
                    metrics_sink.record(&metrics);
                    metrics_sink.record_tombstone_ratio(
                        metrics.table_live_rows,
                        metrics.table_superseded_rows,
                        &inner.endpoint_name,
                        &inner.config.uri,
                    );
                    inner.records_written.store(0, Ordering::Relaxed);
                    if let Some(controller) = inner.controller.upgrade() {
                        controller.update_output_connector_health(
                            inner.endpoint_id,
                            ConnectorHealth::healthy(),
                        );
                        controller.status.output_buffer(
                            inner.endpoint_id,
                            metrics.bytes_written as usize,
                            metrics.rows_appended as usize,
                        );
                    }
                    // After the commit, never before: a compaction starting mid-flush would
                    // replace the files that flush is addressing, forcing a needless retry.
                    if let Some(compactor) = compactor {
                        compactor.maybe_start();
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
                Err(e)
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
                    TOKIO.block_on(async {
                        // Constructed inside the runtime, which `sleep` requires.
                        sleep(backoff).await;

                        // The retry must run against the table as it now stands: a conflict
                        // means maintenance replaced the files this attempt addressed, so
                        // redoing the lookup against the old snapshot would conflict for ever.
                        if let Err(e) = task.delta_table.update_incremental(None).await {
                            warn!(
                                "delta_table {}: unable to reload the table before retrying: {e}",
                                inner.endpoint_name
                            );
                        }
                    });
                    backoff = min(backoff * 2, max_backoff);
                }
                Err(e) => {
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

        let threads = self.threads;
        let mut bounds = batch.keys_factory().default_box();
        batch.partition_keys(threads, &mut *bounds);

        let mut cursor_builders = Vec::new();
        for i in 0..=bounds.len() {
            let Some(cb) = SplitCursorBuilder::from_bounds(
                batch.clone(),
                &*bounds,
                i,
                RecordFormat::Parquet(delta_output_serde_config(
                    self.inner.config.variant_encoding,
                )),
            ) else {
                continue;
            };
            cursor_builders.push(cb);
        }
        if cursor_builders.is_empty() {
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
            let mut handles = Vec::with_capacity(cursor_builders.len());
            for cursor_builder in cursor_builders {
                let inner = self.inner.clone();
                let object_store = self.object_store.clone();
                handles.push(tokio::spawn(
                    encode_and_write_range(cursor_builder, inner, object_store, micros)
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

#[cfg(test)]
mod parallel {
    use std::collections::BTreeMap;
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;
    use std::sync::{Arc, Weak};

    use dbsp::utils::Tup2;
    use dbsp::{OrdIndexedZSet, OrdZSet};
    use feldera_sqllib::{
        ByteArray, Date, F32, F64, SqlDecimal, SqlString, Timestamp, Uuid, Variant,
    };
    use feldera_types::deserialize_table_record;
    use feldera_types::program_schema::{ColumnType, Relation, SqlIdentifier};
    use feldera_types::transport::delta_table::{
        DeltaTableUpdateMode, DeltaTableWriteMode, DeltaTableWriterConfig, DeltaVariantEncoding,
    };
    use tempfile::TempDir;

    use crate::catalog::SerBatch;
    use crate::controller::EndpointId;
    use crate::format::Encoder;
    use crate::format::parquet::test::load_parquet_file;
    use crate::integrated::delta_table::delta_input_serde_config;
    use crate::static_compile::seroutput::SerBatchImpl;
    use dbsp::DBData;
    use dbsp::dynamic::{DynData, Erase};
    use feldera_types::serde_with_context::{SerializeWithContext, SqlSerdeConfig};

    use crate::test::data::{
        DeltaTestKey, DeltaTestKeyBinary, DeltaTestKeyDecimal, DeltaTestKeyDouble, DeltaTestKeyInt,
        DeltaTestKeyString, DeltaTestKeyStruct, DeltaTestKeyTimestamp, DeltaTestKeyUuid,
        DeltaTestStruct, TestStruct,
    };
    use crate::test::list_files_recursive;
    use feldera_adapterlib::transport::OutputBatchType;

    use super::DeltaTableWriter;

    // ── Output record type (DeltaTestStruct fields + metadata columns) ──

    #[derive(Debug, Default, PartialEq, Eq, PartialOrd, Ord, Clone)]
    struct OutputRecord {
        bigint: i64,
        binary: ByteArray,
        boolean: bool,
        date: Date,
        decimal_10_3: SqlDecimal<10, 3>,
        double: F64,
        float: F32,
        int: i32,
        smallint: i16,
        string: String,
        unused: Option<String>,
        timestamp_ntz: Timestamp,
        tinyint: i8,
        string_array: Vec<String>,
        struct1: TestStruct,
        struct_array: Vec<TestStruct>,
        string_string_map: BTreeMap<String, String>,
        string_struct_map: BTreeMap<String, TestStruct>,
        variant: Variant,
        uuid: Uuid,
        __feldera_op: String,
        __feldera_ts: i64,
    }

    deserialize_table_record!(OutputRecord["OutputRecord", Variant, 22] {
        (bigint, "bigint", false, i64, |_| None),
        (binary, "binary", false, ByteArray, |_| None),
        (boolean, "boolean", false, bool, |_| None),
        (date, "date", false, Date, |_| None),
        (decimal_10_3, "decimal_10_3", false, SqlDecimal<10, 3>, |_| None),
        (double, "double", false, F64, |_| None),
        (float, "float", false, F32, |_| None),
        (int, "int", false, i32, |_| None),
        (smallint, "smallint", false, i16, |_| None),
        (string, "string", false, String, |_| None),
        (unused, "unused", false, Option<String>, |_| Some(None)),
        (timestamp_ntz, "timestamp_ntz", false, Timestamp, |_| None),
        (tinyint, "tinyint", false, i8, |_| None),
        (string_array, "string_array", false, Vec<String>, |_| None),
        (struct1, "struct1", false, TestStruct, |_| None),
        (struct_array, "struct_array", false, Vec<TestStruct>, |_| None),
        (string_string_map, "string_string_map", false, BTreeMap<String, String>, |_| None),
        (string_struct_map, "string_struct_map", false, BTreeMap<String, TestStruct>, |_| None),
        (variant, "variant", false, Variant, |_| None),
        (uuid, "uuid", false, Uuid, |_| None),
        (__feldera_op, "__feldera_op", false, String, |_| None),
        (__feldera_ts, "__feldera_ts", false, i64, |_| None)
    });

    impl OutputRecord {
        fn to_data_record(&self) -> DeltaTestStruct {
            DeltaTestStruct {
                bigint: self.bigint,
                binary: self.binary.clone(),
                boolean: self.boolean,
                date: self.date,
                decimal_10_3: self.decimal_10_3,
                double: self.double,
                float: self.float,
                int: self.int,
                smallint: self.smallint,
                string: self.string.clone(),
                unused: self.unused.clone(),
                timestamp_ntz: self.timestamp_ntz,
                tinyint: self.tinyint,
                string_array: self.string_array.clone(),
                struct1: self.struct1.clone(),
                struct_array: self.struct_array.clone(),
                string_string_map: self.string_string_map.clone(),
                string_struct_map: self.string_struct_map.clone(),
                variant: self.variant.clone(),
                uuid: self.uuid.clone(),
            }
        }
    }

    // ── Helpers ────────────────────────────────────────────────────

    fn key_relation() -> Relation {
        Relation {
            name: SqlIdentifier::new("test_idx", false),
            fields: vec![feldera_types::program_schema::Field::new(
                "bigint".into(),
                ColumnType::bigint(false),
            )],
            materialized: false,
            properties: BTreeMap::new(),
            primary_key: None,
        }
    }

    fn value_relation() -> Relation {
        let mut rel = DeltaTestStruct::relation_schema();
        rel.materialized = true;
        rel
    }

    fn make_endpoint(threads: usize, table_uri: &str, indexed: bool) -> DeltaTableWriter {
        make_endpoint_ex(
            threads,
            table_uri,
            indexed,
            DeltaTableWriteMode::Truncate,
            false,
        )
    }

    fn make_endpoint_ex(
        threads: usize,
        table_uri: &str,
        indexed: bool,
        mode: DeltaTableWriteMode,
        continue_previous_state: bool,
    ) -> DeltaTableWriter {
        let key_schema = if indexed { Some(key_relation()) } else { None };
        DeltaTableWriter::new(
            EndpointId::default(),
            "test_endpoint",
            &DeltaTableWriterConfig {
                variant_encoding: Default::default(),
                uri: table_uri.to_string(),
                mode,
                max_retries: Some(0),
                threads: Some(threads),
                optimize_interval_secs: None,

                object_store_config: Default::default(),
                checkpoint_interval: None,
                log_retention_duration: None,
                update_mode: Default::default(),
                lookup_chunk_bytes: 1 << 20,
                max_concurrent_probes: 4,
                enable_expired_log_cleanup: None,
            },
            &key_schema,
            &value_relation(),
            Weak::new(),
            continue_previous_state,
            indexed,
        )
        .expect("failed to create endpoint")
    }

    // ── merge mode ──

    fn make_merge_endpoint(table_uri: &str, mode: DeltaTableWriteMode) -> DeltaTableWriter {
        make_merge_endpoint_ex(table_uri, mode, 1 << 20, 0, key_relation())
    }

    fn make_merge_endpoint_ex(
        table_uri: &str,
        mode: DeltaTableWriteMode,
        lookup_chunk_bytes: usize,
        max_retries: u32,
        key_schema: Relation,
    ) -> DeltaTableWriter {
        DeltaTableWriter::new(
            EndpointId::default(),
            "test_merge_endpoint",
            &DeltaTableWriterConfig {
                uri: table_uri.to_string(),
                mode,
                variant_encoding: DeltaVariantEncoding::default(),
                update_mode: DeltaTableUpdateMode::Merge,
                max_retries: Some(max_retries),
                threads: Some(1),
                optimize_interval_secs: None,

                object_store_config: Default::default(),
                checkpoint_interval: None,
                log_retention_duration: None,
                lookup_chunk_bytes,
                max_concurrent_probes: 4,
                enable_expired_log_cleanup: None,
            },
            &Some(key_schema),
            &value_relation(),
            Weak::new(),
            false,
            true,
        )
        .expect("failed to create merge endpoint")
    }

    /// Rows the table currently holds, as any reader would see them.
    ///
    /// Through DataFusion, so deletion vectors are applied. Reading the parquet files directly
    /// as the cdc tests do would show every superseded row still present.
    fn read_merge_output(table_uri: &str) -> Vec<DeltaTestStruct> {
        use crate::integrated::delta_table::delta_input_serde_config;
        use dbsp::circuit::tokio::TOKIO;
        use deltalake::datafusion::prelude::SessionContext;
        use deltalake::open_table;
        use feldera_types::serde_with_context::DeserializeWithContext;

        let url = url::Url::from_file_path(table_uri).unwrap();
        TOKIO.block_on(async move {
            let table = open_table(url).await.unwrap();
            let ctx = SessionContext::new();
            let provider = table.table_provider().await.unwrap();
            let batches = ctx.read_table(provider).unwrap().collect().await.unwrap();

            let mut rows = Vec::new();
            for batch in batches.iter() {
                let de = serde_arrow::Deserializer::from_record_batch(batch).unwrap();
                let mut batch_rows = Vec::<DeltaTestStruct>::deserialize_with_context(
                    de,
                    &delta_input_serde_config(),
                )
                .unwrap();
                rows.append(&mut batch_rows);
            }
            rows.sort();
            rows
        })
    }

    /// Assert the table holds exactly the model's rows.
    fn assert_matches_model(table_uri: &str, model: &BTreeMap<i64, DeltaTestStruct>, step: &str) {
        let actual = read_merge_output(table_uri);
        let mut expected: Vec<_> = model.values().cloned().collect();
        expected.sort();

        assert_eq!(
            actual.len(),
            expected.len(),
            "after {step}: table has {} row(s), the view has {}",
            actual.len(),
            expected.len()
        );
        assert_eq!(
            actual, expected,
            "after {step}: table diverged from the view"
        );
    }

    /// Merge mode must leave the table equal to the view after every commit.
    ///
    /// Walks every transition a key can make -- absent to present, updated, deleted, deleted
    /// again, reinserted -- then mixes them in one batch, where a per-key branch that works in
    /// isolation tends to break.
    #[test]
    fn merge_tracks_the_view() {
        let dir = TempDir::new().unwrap();
        let uri = dir.path().to_str().unwrap();
        let mut endpoint = make_merge_endpoint(uri, DeltaTableWriteMode::Append);
        let mut model: BTreeMap<i64, DeltaTestStruct> = BTreeMap::new();

        let records = make_records(12);
        let updated: Vec<DeltaTestStruct> = (100..112).map(make_record).collect();

        // An empty batch must not change the table.
        encode_batch(&mut endpoint, &build_insert_batch(&[]));
        assert_matches_model(uri, &model, "an empty batch");

        // Inserts.
        encode_batch(&mut endpoint, &build_insert_batch(&records[0..6]));
        for r in &records[0..6] {
            model.insert(r.bigint, r.clone());
        }
        assert_matches_model(uri, &model, "six inserts");

        // Updates: the old row is superseded, not duplicated. `updated[i]` carries the same
        // key as `records[i]` only if the key is the record index, so rebuild it here.
        let updates: Vec<(DeltaTestStruct, DeltaTestStruct)> = (0..3)
            .map(|i| {
                let mut new = updated[i].clone();
                new.bigint = records[i].bigint;
                (records[i].clone(), new)
            })
            .collect();
        encode_batch(&mut endpoint, &build_upsert_batch(&updates));
        for (_, new) in &updates {
            model.insert(new.bigint, new.clone());
        }
        assert_matches_model(uri, &model, "three updates");

        // Deletes.
        encode_batch(&mut endpoint, &build_delete_batch(&records[4..6]));
        for r in &records[4..6] {
            model.remove(&r.bigint);
        }
        assert_matches_model(uri, &model, "two deletes");

        // Deleting a key the table does not hold is a no-op, not an error.
        encode_batch(&mut endpoint, &build_delete_batch(&records[8..10]));
        assert_matches_model(uri, &model, "deleting absent keys");

        // Reinserting a deleted key must leave one live row, not two.
        encode_batch(&mut endpoint, &build_insert_batch(&records[4..5]));
        model.insert(records[4].bigint, records[4].clone());
        assert_matches_model(uri, &model, "reinserting a deleted key");

        // One batch mixing all three operations.
        let mut mixed = Vec::new();
        let mut new_third = updated[3].clone();
        new_third.bigint = records[3].bigint;
        mixed.push((records[3].clone(), new_third.clone()));
        encode_batch(&mut endpoint, &build_upsert_batch(&mixed));
        model.insert(new_third.bigint, new_third);
        encode_batch(&mut endpoint, &build_insert_batch(&records[6..8]));
        for r in &records[6..8] {
            model.insert(r.bigint, r.clone());
        }
        assert_matches_model(uri, &model, "a mixed batch");

        // Everything deleted: no live rows, and the table should hold no data files either.
        let live: Vec<DeltaTestStruct> = model.values().cloned().collect();
        encode_batch(&mut endpoint, &build_delete_batch(&live));
        model.clear();
        assert_matches_model(uri, &model, "deleting every row");
    }

    /// A connector that opens a table it did not fill must still supersede what is in it.
    ///
    /// The insert shortcut is only sound when the table started empty. Here a second connector
    /// opens a populated table and is fed an *insert* for a key already in it; the old row
    /// must be tombstoned anyway. This is also what a pipeline restart looks like.
    #[test]
    fn merge_supersedes_a_row_left_by_an_earlier_run() {
        let dir = TempDir::new().unwrap();
        let uri = dir.path().to_str().unwrap();
        let records = make_records(4);

        {
            let mut first = make_merge_endpoint(uri, DeltaTableWriteMode::Append);
            encode_batch(&mut first, &build_insert_batch(&records));
        }

        // A second connector on the same table sees data it did not write.
        let mut second = make_merge_endpoint(uri, DeltaTableWriteMode::Append);
        let mut replacement = make_record(500);
        replacement.bigint = records[1].bigint;
        encode_batch(&mut second, &build_insert_batch(&[replacement.clone()]));

        let mut model: BTreeMap<i64, DeltaTestStruct> =
            records.iter().map(|r| (r.bigint, r.clone())).collect();
        model.insert(replacement.bigint, replacement);
        assert_matches_model(uri, &model, "an insert onto a row from an earlier run");
    }

    /// A key set larger than `lookup_chunk_bytes` is split into several lookup passes, and
    /// the result must not depend on how many.
    ///
    /// The budget below is small enough that a few keys cross it, so the flush runs the
    /// multi-pass path that a default-sized budget never reaches.
    #[test]
    fn merge_is_unaffected_by_chunking() {
        let dir = TempDir::new().unwrap();
        let uri = dir.path().to_str().unwrap();
        let records = make_records(40);

        let mut endpoint =
            make_merge_endpoint_ex(uri, DeltaTableWriteMode::Append, 1, 0, key_relation());
        encode_batch(&mut endpoint, &build_insert_batch(&records));

        // Update half the rows in one batch, so the removal side spans many chunks.
        let updates: Vec<(DeltaTestStruct, DeltaTestStruct)> = (0..20)
            .map(|i| {
                let mut new = make_record(1000 + i);
                new.bigint = records[i].bigint;
                (records[i].clone(), new)
            })
            .collect();
        encode_batch(&mut endpoint, &build_upsert_batch(&updates));

        let mut model: BTreeMap<i64, DeltaTestStruct> =
            records.iter().map(|r| (r.bigint, r.clone())).collect();
        for (_, new) in &updates {
            model.insert(new.bigint, new.clone());
        }
        assert_matches_model(uri, &model, "20 updates across many lookup chunks");
    }

    /// Deletion vector objects at the table root.
    fn vector_files(table_uri: &str) -> Vec<String> {
        std::fs::read_dir(table_uri)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|name| name.starts_with("deletion_vector_"))
            .collect()
    }

    /// Value of one of the connector's exported metrics.
    fn merge_metric(endpoint: &DeltaTableWriter, name: &str) -> f64 {
        use feldera_adapterlib::metrics::ConnectorMetrics;

        endpoint
            .merge_metrics
            .as_ref()
            .expect("not a merge-mode endpoint")
            .metrics()
            .into_iter()
            .find(|(n, ..)| *n == name)
            .unwrap_or_else(|| panic!("no metric named {name}"))
            .3
    }

    /// An insert-only flush onto a table that started empty must not read the table at all.
    /// That shortcut is what makes a bootstrap affordable.
    #[test]
    fn merge_skips_the_lookup_for_inserts_into_an_empty_table() {
        let dir = TempDir::new().unwrap();
        let uri = dir.path().to_str().unwrap();
        let mut endpoint = make_merge_endpoint(uri, DeltaTableWriteMode::Append);

        encode_batch(&mut endpoint, &build_insert_batch(&make_records(50)));

        assert_eq!(
            merge_metric(&endpoint, "delta_merge_rows_appended_total"),
            50.0
        );
        assert_eq!(
            merge_metric(&endpoint, "delta_merge_keys_probed_total"),
            0.0,
            "an insert-only flush onto an empty table looked rows up anyway"
        );
        assert_eq!(
            merge_metric(&endpoint, "delta_merge_probe_files_scanned_total"),
            0.0
        );
    }

    /// The lookup must skip files whose key range cannot hold any changed key, or the cost of
    /// a flush grows with the table rather than with the change.
    ///
    /// Five flushes leave five files with disjoint key ranges; a sixth updating two keys from
    /// one of them must open that file and skip the other four.
    #[test]
    fn merge_prunes_files_outside_the_changed_key_range() {
        let dir = TempDir::new().unwrap();
        let uri = dir.path().to_str().unwrap();
        let mut endpoint = make_merge_endpoint(uri, DeltaTableWriteMode::Append);

        // Disjoint key ranges, one file each. `make_record(i)` keys on `i`.
        for range in 0..5 {
            let batch: Vec<DeltaTestStruct> =
                (range * 100..range * 100 + 20).map(make_record).collect();
            encode_batch(&mut endpoint, &build_insert_batch(&batch));
        }
        let scanned_before = merge_metric(&endpoint, "delta_merge_probe_files_scanned_total");

        // Update two keys that live in the third file only.
        let updates: Vec<(DeltaTestStruct, DeltaTestStruct)> = [205i64, 206]
            .iter()
            .map(|key| {
                let old = make_record(*key as usize);
                let mut new = make_record(9000 + *key as usize);
                new.bigint = *key;
                (old, new)
            })
            .collect();
        encode_batch(&mut endpoint, &build_upsert_batch(&updates));

        let scanned =
            merge_metric(&endpoint, "delta_merge_probe_files_scanned_total") - scanned_before;
        let pruned = merge_metric(&endpoint, "delta_merge_probe_files_pruned_total");
        assert_eq!(scanned, 1.0, "opened {scanned} files, expected 1");
        assert_eq!(pruned, 4.0, "pruned {pruned} files, expected 4");

        // And the answer is still right, which is what makes the skipping meaningful.
        let mut model: BTreeMap<i64, DeltaTestStruct> = BTreeMap::new();
        for range in 0..5 {
            for i in range * 100..range * 100 + 20 {
                let record = make_record(i);
                model.insert(record.bigint, record);
            }
        }
        for (_, new) in &updates {
            model.insert(new.bigint, new.clone());
        }
        assert_matches_model(uri, &model, "an update confined to one file");
    }

    /// A flush must converge after a compaction replaced the files it was addressing.
    ///
    /// The one concurrency claim merge mode makes to administrators: OPTIMIZE is safe to run
    /// against a table a pipeline is writing. The commit conflicts and the flush redoes the
    /// lookup against the new files. Without the reload it would conflict for ever, so this
    /// fails rather than merely running slow.
    #[test]
    fn merge_converges_after_a_concurrent_compaction() {
        use dbsp::circuit::tokio::TOKIO;
        use deltalake::open_table;

        let dir = TempDir::new().unwrap();
        let uri = dir.path().to_str().unwrap();
        let mut endpoint =
            make_merge_endpoint_ex(uri, DeltaTableWriteMode::Append, 1 << 20, 2, key_relation());

        let records = make_records(8);
        encode_batch(&mut endpoint, &build_insert_batch(&records[0..4]));
        encode_batch(&mut endpoint, &build_insert_batch(&records[4..8]));

        // Compact behind the endpoint's back. Its in-memory snapshot still names the two
        // files this replaces, which is exactly the state the retry has to recover from.
        let url = url::Url::from_file_path(uri).unwrap();
        let compacted = TOKIO.block_on(async move {
            let table = open_table(url).await.unwrap();
            table
                .optimize()
                .with_target_size(std::num::NonZeroU64::new(64 << 20).unwrap())
                .await
                .unwrap()
                .1
        });
        assert!(
            compacted.num_files_removed >= 2,
            "the fixture did not compact anything: {compacted:?}"
        );

        let updates: Vec<(DeltaTestStruct, DeltaTestStruct)> = (0..2)
            .map(|i| {
                let mut new = make_record(500 + i);
                new.bigint = records[i].bigint;
                (records[i].clone(), new)
            })
            .collect();
        encode_batch(&mut endpoint, &build_upsert_batch(&updates));

        let mut model: BTreeMap<i64, DeltaTestStruct> =
            records.iter().map(|r| (r.bigint, r.clone())).collect();
        for (_, new) in &updates {
            model.insert(new.bigint, new.clone());
        }
        assert_matches_model(uri, &model, "an update after a concurrent compaction");
    }

    /// A duplicate key must be reported once per batch, not once per attempt.
    ///
    /// The retry walks the same batch, so a conflicting commit would otherwise repeat every
    /// violation on every attempt -- without a `max_retries` limit, for ever.
    #[test]
    fn a_uniqueness_violation_is_reported_once_across_retries() {
        use dbsp::circuit::tokio::TOKIO;
        use deltalake::open_table;

        let dir = TempDir::new().unwrap();
        let uri = dir.path().to_str().unwrap();
        // One retry allowed, so a conflict produces exactly two attempts.
        let mut endpoint =
            make_merge_endpoint_ex(uri, DeltaTableWriteMode::Append, 1 << 20, 1, key_relation());

        let records = make_records(8);
        encode_batch(&mut endpoint, &build_insert_batch(&records[0..4]));
        encode_batch(&mut endpoint, &build_insert_batch(&records[4..8]));

        // Compact behind the endpoint's back so its next commit conflicts and retries.
        let url = url::Url::from_file_path(uri).unwrap();
        TOKIO.block_on(async move {
            let table = open_table(url).await.unwrap();
            table
                .optimize()
                .with_target_size(std::num::NonZeroU64::new(64 << 20).unwrap())
                .await
                .unwrap();
        });

        // One real update (to force the conflicting commit) plus one key inserted twice.
        let mut new = make_record(500);
        new.bigint = records[0].bigint;
        let mut tuples = vec![
            Tup2(
                Tup2(
                    DeltaTestKey {
                        bigint: records[0].bigint,
                    },
                    records[0].clone(),
                ),
                -1i64,
            ),
            Tup2(Tup2(DeltaTestKey { bigint: new.bigint }, new.clone()), 1i64),
        ];
        tuples.push(Tup2(
            Tup2(
                DeltaTestKey {
                    bigint: records[5].bigint,
                },
                records[5].clone(),
            ),
            2i64,
        ));
        let zset = OrdIndexedZSet::from_tuples((), tuples);
        let batch: Arc<dyn SerBatch> =
            Arc::new(SerBatchImpl::<_, DeltaTestKey, DeltaTestStruct>::new(zset));

        encode_batch(&mut endpoint, &batch);
        assert_eq!(
            endpoint.merge_violations, 1,
            "the retry reported the same duplicate key again"
        );
    }

    /// A failed commit must leave its deletion vector object alone.
    ///
    /// An error can also mean the commit landed and only its response was lost, in which case
    /// the object is one the live version references and deleting it breaks every reader.
    /// Forced here with a conflict, the one commit failure a test can produce on demand.
    #[test]
    fn a_failed_commit_keeps_its_deletion_vector_file() {
        use dbsp::circuit::tokio::TOKIO;
        use deltalake::open_table;

        let dir = TempDir::new().unwrap();
        let uri = dir.path().to_str().unwrap();
        // No retries, so the conflicting attempt is the last one.
        let mut endpoint =
            make_merge_endpoint_ex(uri, DeltaTableWriteMode::Append, 1 << 20, 0, key_relation());

        let records = make_records(8);
        encode_batch(&mut endpoint, &build_insert_batch(&records[0..4]));
        encode_batch(&mut endpoint, &build_insert_batch(&records[4..8]));

        // Compact behind the endpoint's back so its next commit conflicts.
        let url = url::Url::from_file_path(uri).unwrap();
        TOKIO.block_on(async move {
            let table = open_table(url).await.unwrap();
            table
                .optimize()
                .with_target_size(std::num::NonZeroU64::new(64 << 20).unwrap())
                .await
                .unwrap();
        });

        let mut new = make_record(500);
        new.bigint = records[0].bigint;
        let batch = build_upsert_batch(&[(records[0].clone(), new)]);
        endpoint.consumer().batch_start(0, OutputBatchType::Delta);
        assert!(
            endpoint.encode(batch.arc_as_batch_reader()).is_err(),
            "the fixture did not produce a conflicting commit"
        );

        assert_eq!(
            vector_files(uri).len(),
            1,
            "the failed commit deleted its deletion vector file"
        );
    }

    /// OPTIMIZE must drop the rows the connector's vectors mark deleted, not resurrect them.
    ///
    /// We tell administrators to run OPTIMIZE, and a compaction that rewrote a file while
    /// ignoring its vector would bring every superseded row back. Nothing in the connector
    /// could detect that afterwards.
    #[test]
    fn compaction_drops_the_rows_a_vector_marks_deleted() {
        use dbsp::circuit::tokio::TOKIO;
        use deltalake::open_table;

        let dir = TempDir::new().unwrap();
        let uri = dir.path().to_str().unwrap();
        let mut endpoint = make_merge_endpoint(uri, DeltaTableWriteMode::Append);

        // Five rows in one file, then supersede two of them. Superseding a strict subset is
        // what leaves a vector behind: superseding all five would drop the file whole.
        let records = make_records(5);
        encode_batch(&mut endpoint, &build_insert_batch(&records));

        let updates: Vec<(DeltaTestStruct, DeltaTestStruct)> = (0..2)
            .map(|i| {
                let mut new = make_record(900 + i);
                new.bigint = records[i].bigint;
                (records[i].clone(), new)
            })
            .collect();
        encode_batch(&mut endpoint, &build_upsert_batch(&updates));

        let mut model: BTreeMap<i64, DeltaTestStruct> =
            records.iter().map(|r| (r.bigint, r.clone())).collect();
        for (_, new) in &updates {
            model.insert(new.bigint, new.clone());
        }
        assert_matches_model(uri, &model, "before compaction");

        // Without this the test would pass on a table that has no vector to respect.
        assert_eq!(
            vector_files(uri).len(),
            1,
            "the fixture wrote no deletion vector"
        );

        let url = url::Url::from_file_path(uri).unwrap();
        let compacted = TOKIO.block_on(async move {
            let table = open_table(url).await.unwrap();
            table
                .optimize()
                .with_target_size(std::num::NonZeroU64::new(64 << 20).unwrap())
                .await
                .unwrap()
                .1
        });
        assert!(
            compacted.num_files_removed >= 2,
            "the fixture did not compact anything: {compacted:?}"
        );

        assert_matches_model(uri, &model, "after compaction");
    }

    /// No data file is ever rewritten, which is the point of the design. A file may leave the
    /// table once every row in it is tombstoned, but no file's contents move to a new path.
    #[test]
    fn merge_never_rewrites_a_data_file() {
        use dbsp::circuit::tokio::TOKIO;
        use deltalake::open_table;

        let dir = TempDir::new().unwrap();
        let uri = dir.path().to_str().unwrap();
        let mut endpoint = make_merge_endpoint(uri, DeltaTableWriteMode::Append);
        let records = make_records(8);

        let paths = |uri: &str| -> Vec<String> {
            let url = url::Url::from_file_path(uri).unwrap();
            TOKIO.block_on(async move {
                let table = open_table(url).await.unwrap();
                let mut paths: Vec<String> = table
                    .snapshot()
                    .unwrap()
                    .log_data()
                    .into_iter()
                    .map(|f| f.path().to_string())
                    .collect();
                paths.sort();
                paths
            })
        };

        encode_batch(&mut endpoint, &build_insert_batch(&records[0..4]));
        let after_insert = paths(uri);
        assert_eq!(after_insert.len(), 1);

        // Update every row in that file. Its path must survive, carrying a vector.
        let updates: Vec<(DeltaTestStruct, DeltaTestStruct)> = (0..3)
            .map(|i| {
                let mut new = make_record(200 + i);
                new.bigint = records[i].bigint;
                (records[i].clone(), new)
            })
            .collect();
        encode_batch(&mut endpoint, &build_upsert_batch(&updates));

        let after_update = paths(uri);
        assert!(
            after_update.contains(&after_insert[0]),
            "the original data file was rewritten: {after_insert:?} -> {after_update:?}"
        );
    }

    // ── merge mode: key types ──

    /// A key relation naming one column of the test schema.
    ///
    /// The column type comes from the view, so the test cannot drift from what the view
    /// actually declares.
    fn key_relation_on(column: &str) -> Relation {
        let field = value_relation()
            .fields
            .into_iter()
            .find(|f| f.name.name() == column)
            .unwrap_or_else(|| panic!("no column '{column}' in the test schema"));
        Relation {
            name: SqlIdentifier::new("test_idx", false),
            fields: vec![field],
            materialized: false,
            properties: BTreeMap::new(),
            primary_key: None,
        }
    }

    /// One batch of `(key, row, weight)` triples, for any key type.
    fn keyed_batch<K>(tuples: Vec<Tup2<Tup2<K, DeltaTestStruct>, i64>>) -> Arc<dyn SerBatch>
    where
        K: DBData + Erase<DynData> + SerializeWithContext<SqlSerdeConfig> + Send + Sync,
    {
        let zset = OrdIndexedZSet::from_tuples((), tuples);
        Arc::new(SerBatchImpl::<_, K, DeltaTestStruct>::new(zset))
    }

    /// Assert the table holds exactly `expected`, as a reader applying deletion vectors sees it.
    fn assert_table_rows(table_uri: &str, expected: &[DeltaTestStruct], step: &str) {
        let actual = read_merge_output(table_uri);
        let mut expected = expected.to_vec();
        expected.sort();
        assert_eq!(
            actual.len(),
            expected.len(),
            "after {step}: table has {} row(s), the view has {}",
            actual.len(),
            expected.len()
        );
        assert_eq!(
            actual, expected,
            "after {step}: table diverged from the view"
        );
    }

    /// Drive one key type through insert, update and delete, and check that the table still
    /// equals the view.
    ///
    /// `key_of` reads the key out of a row. `carry_key` copies the key column from the old
    /// row to the new one, which is what makes an update an update instead of a second
    /// insert.
    fn merge_key_type_case<K>(
        column: &str,
        key_of: impl Fn(&DeltaTestStruct) -> K,
        carry_key: impl Fn(&mut DeltaTestStruct, &DeltaTestStruct),
    ) where
        K: DBData + Erase<DynData> + SerializeWithContext<SqlSerdeConfig> + Send + Sync,
    {
        let dir = TempDir::new().unwrap();
        let uri = dir.path().to_str().unwrap();
        let mut endpoint = make_merge_endpoint_ex(
            uri,
            DeltaTableWriteMode::Append,
            1 << 20,
            0,
            key_relation_on(column),
        );

        // Small indices keep every column of `make_record` distinct, so whichever column
        // the test keys on is unique across the batch.
        let records = make_records(12);
        encode_batch(
            &mut endpoint,
            &keyed_batch(
                records
                    .iter()
                    .map(|r| Tup2(Tup2(key_of(r), r.clone()), 1i64))
                    .collect(),
            ),
        );
        assert_table_rows(uri, &records, &format!("12 inserts keyed on '{column}'"));

        // Update the first four: same key, every other column different. A connector that
        // did not find the old rows would leave 16 rows here instead of 12.
        let updates: Vec<(DeltaTestStruct, DeltaTestStruct)> = (0..4)
            .map(|i| {
                let mut new = make_record(500 + i);
                carry_key(&mut new, &records[i]);
                (records[i].clone(), new)
            })
            .collect();
        let mut tuples = Vec::new();
        for (old, new) in &updates {
            tuples.push(Tup2(Tup2(key_of(old), old.clone()), -1i64));
            tuples.push(Tup2(Tup2(key_of(new), new.clone()), 1i64));
        }
        encode_batch(&mut endpoint, &keyed_batch(tuples));

        let mut expected: Vec<DeltaTestStruct> =
            updates.iter().map(|(_, new)| new.clone()).collect();
        expected.extend_from_slice(&records[4..]);
        assert_table_rows(uri, &expected, &format!("4 updates keyed on '{column}'"));

        // Delete two rows the table really holds.
        encode_batch(
            &mut endpoint,
            &keyed_batch(
                records[10..12]
                    .iter()
                    .map(|r| Tup2(Tup2(key_of(r), r.clone()), -1i64))
                    .collect(),
            ),
        );
        expected.retain(|r| r.bigint != records[10].bigint && r.bigint != records[11].bigint);
        assert_table_rows(uri, &expected, &format!("2 deletes keyed on '{column}'"));
    }

    /// One test per key type, so a failure names the type that broke.
    ///
    /// These types cover the distinct risks in the key path: integer encoding, variable
    /// length, offset width, timestamp units, decimal precision and scale, pruning turned
    /// off by a float, and a composite ROW key.
    macro_rules! merge_key_type_test {
        ($test:ident, $column:literal, $key:ident, $field:ident) => {
            #[test]
            fn $test() {
                merge_key_type_case(
                    $column,
                    |r: &DeltaTestStruct| $key {
                        $field: r.$field.clone(),
                    },
                    |new: &mut DeltaTestStruct, old: &DeltaTestStruct| {
                        new.$field = old.$field.clone()
                    },
                );
            }
        };
    }

    merge_key_type_test!(merge_keyed_on_int, "int", DeltaTestKeyInt, int);
    merge_key_type_test!(merge_keyed_on_string, "string", DeltaTestKeyString, string);
    merge_key_type_test!(merge_keyed_on_binary, "binary", DeltaTestKeyBinary, binary);
    merge_key_type_test!(
        merge_keyed_on_timestamp,
        "timestamp_ntz",
        DeltaTestKeyTimestamp,
        timestamp_ntz
    );
    merge_key_type_test!(
        merge_keyed_on_decimal,
        "decimal_10_3",
        DeltaTestKeyDecimal,
        decimal_10_3
    );
    merge_key_type_test!(merge_keyed_on_uuid, "uuid", DeltaTestKeyUuid, uuid);
    merge_key_type_test!(merge_keyed_on_row, "struct1", DeltaTestKeyStruct, struct1);

    // A DOUBLE key turns range pruning off, because NaN is left out of min/max statistics.
    // Reading every file is slower but must give the same answer, which is what this checks.
    merge_key_type_test!(merge_keyed_on_double, "double", DeltaTestKeyDouble, double);

    fn build_insert_batch(records: &[DeltaTestStruct]) -> Arc<dyn SerBatch> {
        let tuples: Vec<_> = records
            .iter()
            .map(|r| Tup2(Tup2(DeltaTestKey { bigint: r.bigint }, r.clone()), 1i64))
            .collect();
        let zset = OrdIndexedZSet::from_tuples((), tuples);
        Arc::new(SerBatchImpl::<_, DeltaTestKey, DeltaTestStruct>::new(zset))
    }

    fn build_delete_batch(records: &[DeltaTestStruct]) -> Arc<dyn SerBatch> {
        let tuples: Vec<_> = records
            .iter()
            .map(|r| Tup2(Tup2(DeltaTestKey { bigint: r.bigint }, r.clone()), -1i64))
            .collect();
        let zset = OrdIndexedZSet::from_tuples((), tuples);
        Arc::new(SerBatchImpl::<_, DeltaTestKey, DeltaTestStruct>::new(zset))
    }

    fn build_upsert_batch(updates: &[(DeltaTestStruct, DeltaTestStruct)]) -> Arc<dyn SerBatch> {
        let mut tuples = Vec::new();
        for (old, new) in updates {
            assert_eq!(old.bigint, new.bigint);
            tuples.push(Tup2(
                Tup2(DeltaTestKey { bigint: old.bigint }, old.clone()),
                -1i64,
            ));
            tuples.push(Tup2(
                Tup2(DeltaTestKey { bigint: new.bigint }, new.clone()),
                1i64,
            ));
        }
        let zset = OrdIndexedZSet::from_tuples((), tuples);
        Arc::new(SerBatchImpl::<_, DeltaTestKey, DeltaTestStruct>::new(zset))
    }

    fn build_non_indexed_batch(records: &[DeltaTestStruct], weight: i64) -> Arc<dyn SerBatch> {
        let tuples: Vec<_> = records.iter().map(|r| Tup2(r.clone(), weight)).collect();
        let zset = OrdZSet::from_keys((), tuples);
        Arc::new(SerBatchImpl::<_, DeltaTestStruct, ()>::new(zset))
    }

    fn encode_batch(endpoint: &mut DeltaTableWriter, batch: &Arc<dyn SerBatch>) {
        endpoint.consumer().batch_start(0, OutputBatchType::Delta);
        endpoint
            .encode(batch.clone().arc_as_batch_reader())
            .unwrap();
        endpoint.consumer().batch_end();
    }

    fn read_output(table_uri: &str) -> Vec<OutputRecord> {
        let parquet_files =
            list_files_recursive(Path::new(table_uri), OsStr::from_bytes(b"parquet")).unwrap();
        let mut records = Vec::new();
        for path in parquet_files {
            let mut batch: Vec<OutputRecord> =
                load_parquet_file(&path, &delta_input_serde_config());
            records.append(&mut batch);
        }
        records
    }

    /// Read only the files referenced by the current delta table snapshot,
    /// ignoring orphaned parquet files left behind by `SaveMode::Overwrite`.
    fn read_delta_output(table_uri: &str) -> Vec<OutputRecord> {
        use dbsp::circuit::tokio::TOKIO;
        use deltalake::open_table;

        let url = url::Url::from_file_path(table_uri).unwrap();
        let table = TOKIO.block_on(async move { open_table(url).await.unwrap() });
        let base = Path::new(table_uri);
        let mut records = Vec::new();
        for uri in table.get_file_uris().unwrap() {
            let mut batch: Vec<OutputRecord> =
                load_parquet_file(&base.join(&*uri), &delta_input_serde_config());
            records.append(&mut batch);
        }
        records
    }

    fn make_record(i: usize) -> DeltaTestStruct {
        DeltaTestStruct {
            bigint: i as i64,
            binary: ByteArray::from_vec(vec![i as u8, (i >> 8) as u8]),
            boolean: i.is_multiple_of(2),
            date: Date::from_days(i as i32 % 100_000),
            decimal_10_3: SqlDecimal::<10, 3>::new((i as i128 % 1_000_000) * 1000, 3).unwrap(),
            double: F64::new((i as f64).trunc()),
            float: F32::new((i as f32).trunc()),
            int: i as i32,
            smallint: (i % 32000) as i16,
            string: format!("record_{i}"),
            unused: if i.is_multiple_of(3) {
                None
            } else {
                Some(format!("unused_{i}"))
            },
            timestamp_ntz: Timestamp::from_milliseconds(1704070800000 + i as i64 * 1000),
            tinyint: (i % 120) as i8,
            string_array: vec![format!("arr_{i}")],
            struct1: TestStruct {
                id: i as u32,
                b: i.is_multiple_of(2),
                i: Some(i as i64),
                s: format!("s_{i}"),
            },
            struct_array: vec![TestStruct {
                id: i as u32,
                b: false,
                i: None,
                s: format!("sa_{i}"),
            }],
            string_string_map: BTreeMap::from([(format!("key_{i}"), format!("val_{i}"))]),
            string_struct_map: BTreeMap::from([(
                format!("sk_{i}"),
                TestStruct {
                    id: i as u32,
                    b: true,
                    i: Some(i as i64 * 2),
                    s: format!("sm_{i}"),
                },
            )]),
            variant: Variant::Map(
                std::iter::once((
                    Variant::String(SqlString::from_ref("foo")),
                    Variant::String(SqlString::from(i.to_string())),
                ))
                .collect::<BTreeMap<Variant, Variant>>()
                .into(),
            ),
            uuid: Uuid::from_bytes([i as u8; 16]),
        }
    }

    fn make_records(n: usize) -> Vec<DeltaTestStruct> {
        (0..n).map(make_record).collect()
    }

    // ── Tests ──────────────────────────────────────────────────────

    fn insert_test(threads: usize) {
        let table_dir = TempDir::new().unwrap();
        let table_uri = table_dir.path().display().to_string();

        let records = make_records(100);
        let batch = build_insert_batch(&records);
        let mut endpoint = make_endpoint(threads, &table_uri, true);

        encode_batch(&mut endpoint, &batch);

        let output = read_output(&table_uri);
        assert_eq!(output.len(), 100);
        for rec in &output {
            assert_eq!(rec.__feldera_op, "i");
        }
        // Verify data fields match
        let mut output_data: Vec<DeltaTestStruct> =
            output.iter().map(|r| r.to_data_record()).collect();
        output_data.sort();
        let mut expected = records.clone();
        expected.sort();
        assert_eq!(output_data, expected);
    }

    #[test]
    fn test_insert_single_thread() {
        insert_test(1);
    }

    #[test]
    fn test_insert_multi_thread() {
        insert_test(4);
    }

    /// A batch bigger than `TARGET_FILE_SIZE` must land in more than one Parquet
    /// file. delta-rs rolls over only when the writer config carries a target
    /// size; without one it writes a single object per key range, which an object
    /// store rejects once the multipart upload passes 10000 parts.
    #[test]
    fn test_batch_larger_than_target_file_size_rolls_over() {
        const PAYLOAD_LEN: usize = 8 * 1024;
        let table_dir = TempDir::new().unwrap();
        let table_uri = table_dir.path().display().to_string();

        let target = super::TARGET_FILE_SIZE.get() as usize;
        let rows = (target * 5 / 4).div_ceil(PAYLOAD_LEN);
        let records: Vec<DeltaTestStruct> = (0..rows)
            .map(|i| DeltaTestStruct {
                string: incompressible_string(i as u64, PAYLOAD_LEN),
                ..make_record(i)
            })
            .collect();

        let mut endpoint = make_endpoint(1, &table_uri, true);
        encode_batch(&mut endpoint, &build_insert_batch(&records));

        let files =
            list_files_recursive(Path::new(&table_uri), OsStr::from_bytes(b"parquet")).unwrap();
        assert!(
            files.len() > 1,
            "{rows} rows of {PAYLOAD_LEN} bytes each exceed the {target} byte target \
             but landed in {} file(s)",
            files.len()
        );
        // Read through the delta log, so a rolled-over file that never made it
        // into the commit shows up as missing rows instead of being picked up
        // off disk.
        assert_eq!(read_delta_output(&table_uri).len(), rows);
    }

    /// Deterministic printable ASCII that Snappy cannot shrink and that Parquet
    /// cannot dictionary-encode away, so the written file size tracks `len`.
    fn incompressible_string(seed: u64, len: usize) -> String {
        let mut state = seed | 1;
        let mut bytes = Vec::with_capacity(len + 8);
        while bytes.len() < len {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            bytes.extend((state >> 8).to_le_bytes().map(|b| b' ' + b % 95));
        }
        bytes.truncate(len);
        String::from_utf8(bytes).unwrap()
    }

    fn upsert_test(threads: usize) {
        let table_dir = TempDir::new().unwrap();
        let table_uri = table_dir.path().display().to_string();

        let records = make_records(50);
        let insert_batch = build_insert_batch(&records);
        let mut endpoint = make_endpoint(threads, &table_uri, true);

        encode_batch(&mut endpoint, &insert_batch);

        // Upsert: update records 0..10
        let updates: Vec<_> = (0..10)
            .map(|i| {
                let old = records[i].clone();
                let new = DeltaTestStruct {
                    boolean: !old.boolean,
                    int: old.int + 1000,
                    string: format!("updated_{}", old.bigint),
                    ..old.clone()
                };
                (old, new)
            })
            .collect();
        let upsert_batch = build_upsert_batch(&updates);
        encode_batch(&mut endpoint, &upsert_batch);

        let output = read_output(&table_uri);
        // First batch: 50 inserts, second batch: 10 upserts
        let inserts: Vec<_> = output.iter().filter(|r| r.__feldera_op == "i").collect();
        let upserts: Vec<_> = output.iter().filter(|r| r.__feldera_op == "u").collect();
        assert_eq!(inserts.len(), 50);
        assert_eq!(upserts.len(), 10);
    }

    #[test]
    fn test_upsert_single_thread() {
        upsert_test(1);
    }

    #[test]
    fn test_upsert_multi_thread() {
        upsert_test(4);
    }

    fn delete_test(threads: usize) {
        let table_dir = TempDir::new().unwrap();
        let table_uri = table_dir.path().display().to_string();

        let records = make_records(50);
        let insert_batch = build_insert_batch(&records);
        let mut endpoint = make_endpoint(threads, &table_uri, true);

        encode_batch(&mut endpoint, &insert_batch);

        // Delete records 0..10
        let to_delete: Vec<_> = records[0..10].to_vec();
        let delete_batch = build_delete_batch(&to_delete);
        encode_batch(&mut endpoint, &delete_batch);

        let output = read_output(&table_uri);
        let inserts: Vec<_> = output.iter().filter(|r| r.__feldera_op == "i").collect();
        let deletes: Vec<_> = output.iter().filter(|r| r.__feldera_op == "d").collect();
        assert_eq!(inserts.len(), 50);
        assert_eq!(deletes.len(), 10);
    }

    #[test]
    fn test_delete_single_thread() {
        delete_test(1);
    }

    #[test]
    fn test_delete_multi_thread() {
        delete_test(4);
    }

    fn non_indexed_insert_test(threads: usize) {
        let table_dir = TempDir::new().unwrap();
        let table_uri = table_dir.path().display().to_string();

        let records = make_records(100);
        let batch = build_non_indexed_batch(&records, 1);
        let mut endpoint = make_endpoint(threads, &table_uri, false);

        encode_batch(&mut endpoint, &batch);

        let output = read_output(&table_uri);
        assert_eq!(output.len(), 100);
        for rec in &output {
            assert_eq!(rec.__feldera_op, "i");
        }
        let mut output_data: Vec<DeltaTestStruct> =
            output.iter().map(|r| r.to_data_record()).collect();
        output_data.sort();
        let mut expected = records;
        expected.sort();
        assert_eq!(output_data, expected);
    }

    #[test]
    fn test_non_indexed_insert_single_thread() {
        non_indexed_insert_test(1);
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
                optimize_interval_secs: None,

                object_store_config: Default::default(),
                checkpoint_interval: None,
                log_retention_duration: None,
                update_mode: Default::default(),
                lookup_chunk_bytes: 1 << 20,
                max_concurrent_probes: 4,
                enable_expired_log_cleanup: None,
            },
            &key_schema,
            &value_relation(),
            Weak::new(),
            false,
            false,
        );
        assert!(
            result.is_err(),
            "threads > 1 without key_schema should be rejected"
        );
    }

    fn empty_batch_test(threads: usize) {
        let table_dir = TempDir::new().unwrap();
        let table_uri = table_dir.path().display().to_string();

        let batch = build_insert_batch(&[]);
        let mut endpoint = make_endpoint(threads, &table_uri, true);

        // Should not crash on empty batch.
        encode_batch(&mut endpoint, &batch);

        let output = read_output(&table_uri);
        assert_eq!(output.len(), 0);
    }

    #[test]
    fn test_empty_batch_single_thread() {
        empty_batch_test(1);
    }

    #[test]
    fn test_empty_batch_multi_thread() {
        empty_batch_test(4);
    }

    fn multiple_batches_test(threads: usize) {
        let table_dir = TempDir::new().unwrap();
        let table_uri = table_dir.path().display().to_string();

        let mut endpoint = make_endpoint(threads, &table_uri, true);

        // Batch 1: insert 50 records
        let records = make_records(50);
        let insert_batch = build_insert_batch(&records);
        encode_batch(&mut endpoint, &insert_batch);

        // Batch 2: insert 50 more records (ids 50..100)
        let more_records: Vec<DeltaTestStruct> = (50..100).map(make_record).collect();
        let insert_batch2 = build_insert_batch(&more_records);
        encode_batch(&mut endpoint, &insert_batch2);

        // Batch 3: upsert records 0..5
        let updates: Vec<_> = (0..5)
            .map(|i| {
                let old = records[i].clone();
                let new = DeltaTestStruct {
                    boolean: !old.boolean,
                    int: old.int + 1000,
                    string: format!("updated_{}", old.bigint),
                    ..old.clone()
                };
                (old, new)
            })
            .collect();
        let upsert_batch = build_upsert_batch(&updates);
        encode_batch(&mut endpoint, &upsert_batch);

        // Batch 4: delete records 90..100
        let to_delete: Vec<_> = more_records[40..50].to_vec();
        let delete_batch = build_delete_batch(&to_delete);
        encode_batch(&mut endpoint, &delete_batch);

        let output = read_output(&table_uri);
        let inserts = output.iter().filter(|r| r.__feldera_op == "i").count();
        let upserts = output.iter().filter(|r| r.__feldera_op == "u").count();
        let deletes = output.iter().filter(|r| r.__feldera_op == "d").count();

        assert_eq!(inserts, 100); // 50 + 50
        assert_eq!(upserts, 5);
        assert_eq!(deletes, 10);
    }

    #[test]
    fn test_multiple_batches_single_thread() {
        multiple_batches_test(1);
    }

    #[test]
    fn test_multiple_batches_multi_thread() {
        multiple_batches_test(4);
    }

    // ── Failure scenario tests ────────────────────────────────────

    /// Write to a read-only directory should fail with no retries.
    #[test]
    fn test_write_failure_readonly_dir() {
        let table_dir = TempDir::new().unwrap();
        let table_uri = table_dir.path().display().to_string();

        // Create the endpoint first (needs writable dir to create the table).
        let records = make_records(10);
        let batch = build_insert_batch(&records);
        let mut endpoint = make_endpoint(1, &table_uri, true);

        // Make directory read-only to trigger write failure.
        std::fs::set_permissions(table_dir.path(), std::fs::Permissions::from_mode(0o555)).unwrap();

        endpoint.consumer().batch_start(0, OutputBatchType::Delta);
        let result = endpoint.encode(batch.arc_as_batch_reader());

        // Restore permissions before asserting (so TempDir cleanup succeeds).
        std::fs::set_permissions(table_dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();

        assert!(result.is_err(), "write to read-only dir should fail");
    }

    /// Exhausting max_retries should propagate the error.
    #[test]
    fn test_retry_exhaustion() {
        let table_dir = TempDir::new().unwrap();
        let table_uri = table_dir.path().display().to_string();

        let records = make_records(10);
        let batch = build_insert_batch(&records);

        // Create endpoint with max_retries=1.
        let key_schema = Some(key_relation());
        let mut endpoint = DeltaTableWriter::new(
            EndpointId::default(),
            "test_endpoint",
            &DeltaTableWriterConfig {
                variant_encoding: Default::default(),
                uri: table_uri.clone(),
                mode: DeltaTableWriteMode::Truncate,
                max_retries: Some(1),
                threads: Some(1),
                optimize_interval_secs: None,

                object_store_config: Default::default(),
                checkpoint_interval: None,
                log_retention_duration: None,
                update_mode: Default::default(),
                lookup_chunk_bytes: 1 << 20,
                max_concurrent_probes: 4,
                enable_expired_log_cleanup: None,
            },
            &key_schema,
            &value_relation(),
            Weak::new(),
            false,
            true,
        )
        .expect("failed to create endpoint");

        // Make directory read-only to trigger write failure.
        std::fs::set_permissions(table_dir.path(), std::fs::Permissions::from_mode(0o555)).unwrap();

        endpoint.consumer().batch_start(0, OutputBatchType::Delta);
        let result = endpoint.encode(batch.arc_as_batch_reader());

        // Restore permissions.
        std::fs::set_permissions(table_dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();

        assert!(result.is_err(), "should fail after exhausting retries");
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
                optimize_interval_secs: None,

                object_store_config: Default::default(),
                checkpoint_interval: None,
                log_retention_duration: Some("interval 7 days".to_string()),
                update_mode: Default::default(),
                lookup_chunk_bytes: 1 << 20,
                max_concurrent_probes: 4,
                enable_expired_log_cleanup: Some(false),
            },
            &Some(key_relation()),
            &value_relation(),
            Weak::new(),
            false,
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
            optimize_interval_secs: None,

            object_store_config: Default::default(),
            checkpoint_interval: None,
            log_retention_duration: None,
            update_mode: Default::default(),
            lookup_chunk_bytes: 1 << 20,
            max_concurrent_probes: 4,
            enable_expired_log_cleanup: None,
        };
        assert!(config.validate().is_err());
    }

    // ── Progress counter tests ────────────────────────────────────

    use std::sync::atomic::Ordering;

    fn records_written(endpoint: &DeltaTableWriter) -> u64 {
        endpoint.inner.records_written.load(Ordering::Relaxed)
    }

    #[test]
    fn test_progress_counter_single_thread() {
        let table_dir = TempDir::new().unwrap();
        let table_uri = table_dir.path().display().to_string();

        let records = make_records(100);
        let batch = build_insert_batch(&records);
        let mut endpoint = make_endpoint(1, &table_uri, true);

        assert_eq!(records_written(&endpoint), 0);
        endpoint.consumer().batch_start(0, OutputBatchType::Delta);
        endpoint
            .encode(batch.clone().arc_as_batch_reader())
            .unwrap();
        assert_eq!(records_written(&endpoint), 100);
        endpoint.consumer().batch_end();
        assert_eq!(records_written(&endpoint), 0);
    }

    #[test]
    fn test_progress_counter_multi_thread() {
        let table_dir = TempDir::new().unwrap();
        let table_uri = table_dir.path().display().to_string();

        let records = make_records(100);
        let batch = build_insert_batch(&records);
        let mut endpoint = make_endpoint(4, &table_uri, true);

        endpoint.consumer().batch_start(0, OutputBatchType::Delta);
        endpoint
            .encode(batch.clone().arc_as_batch_reader())
            .unwrap();
        assert_eq!(records_written(&endpoint), 100);
        endpoint.consumer().batch_end();
        assert_eq!(records_written(&endpoint), 0);
    }

    #[test]
    fn test_progress_counter_empty_batch() {
        let table_dir = TempDir::new().unwrap();
        let table_uri = table_dir.path().display().to_string();

        let batch = build_insert_batch(&[]);
        let mut endpoint = make_endpoint(1, &table_uri, true);

        endpoint.consumer().batch_start(0, OutputBatchType::Delta);
        endpoint
            .encode(batch.clone().arc_as_batch_reader())
            .unwrap();
        assert_eq!(records_written(&endpoint), 0);
        endpoint.consumer().batch_end();
        assert_eq!(records_written(&endpoint), 0);
    }

    #[test]
    fn test_progress_counter_multiple_batches() {
        let table_dir = TempDir::new().unwrap();
        let table_uri = table_dir.path().display().to_string();

        let mut endpoint = make_endpoint(1, &table_uri, true);

        // Batch 1: 50 records.
        let records1 = make_records(50);
        let batch1 = build_insert_batch(&records1);
        endpoint.consumer().batch_start(0, OutputBatchType::Delta);
        endpoint
            .encode(batch1.clone().arc_as_batch_reader())
            .unwrap();
        assert_eq!(records_written(&endpoint), 50);
        endpoint.consumer().batch_end();
        assert_eq!(records_written(&endpoint), 0);

        // Batch 2: 30 records (ids 50..80).
        let records2: Vec<_> = (50..80).map(make_record).collect();
        let batch2 = build_insert_batch(&records2);
        endpoint.consumer().batch_start(1, OutputBatchType::Delta);
        endpoint
            .encode(batch2.clone().arc_as_batch_reader())
            .unwrap();
        assert_eq!(records_written(&endpoint), 30);
        endpoint.consumer().batch_end();
        assert_eq!(records_written(&endpoint), 0);
    }

    #[test]
    fn test_progress_counter_resets_on_batch_start() {
        let table_dir = TempDir::new().unwrap();
        let table_uri = table_dir.path().display().to_string();

        let records = make_records(50);
        let batch = build_insert_batch(&records);
        let mut endpoint = make_endpoint(1, &table_uri, true);

        endpoint.consumer().batch_start(0, OutputBatchType::Delta);
        endpoint
            .encode(batch.clone().arc_as_batch_reader())
            .unwrap();
        assert_eq!(records_written(&endpoint), 50);

        // batch_start without batch_end resets the counter.
        endpoint.consumer().batch_start(1, OutputBatchType::Delta);
        assert_eq!(records_written(&endpoint), 0);
    }

    #[test]
    fn test_progress_counter_resets_on_write_failure() {
        let table_dir = TempDir::new().unwrap();
        let table_uri = table_dir.path().display().to_string();

        let records = make_records(10);
        let batch = build_insert_batch(&records);
        let mut endpoint = make_endpoint(1, &table_uri, true);

        // Make directory read-only to trigger write failure.
        std::fs::set_permissions(table_dir.path(), std::fs::Permissions::from_mode(0o555)).unwrap();

        endpoint.consumer().batch_start(0, OutputBatchType::Delta);
        let result = endpoint.encode(batch.arc_as_batch_reader());

        // Restore permissions before asserting.
        std::fs::set_permissions(table_dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();

        assert!(result.is_err());
        // encode() resets progress to 0 on failure.
        assert_eq!(records_written(&endpoint), 0);
    }

    #[test]
    fn test_progress_counter_resets_after_retries() {
        let table_dir = TempDir::new().unwrap();
        let table_uri = table_dir.path().display().to_string();

        let records = make_records(10);
        let batch = build_insert_batch(&records);

        let key_schema = Some(key_relation());
        let mut endpoint = DeltaTableWriter::new(
            EndpointId::default(),
            "test_endpoint",
            &DeltaTableWriterConfig {
                variant_encoding: Default::default(),
                uri: table_uri.clone(),
                mode: DeltaTableWriteMode::Truncate,
                max_retries: Some(1),
                threads: Some(1),
                optimize_interval_secs: None,

                object_store_config: Default::default(),
                checkpoint_interval: None,
                log_retention_duration: None,
                update_mode: Default::default(),
                lookup_chunk_bytes: 1 << 20,
                max_concurrent_probes: 4,
                enable_expired_log_cleanup: None,
            },
            &key_schema,
            &value_relation(),
            Weak::new(),
            false,
            true,
        )
        .expect("failed to create endpoint");

        // Make directory read-only to trigger write failure with retries.
        std::fs::set_permissions(table_dir.path(), std::fs::Permissions::from_mode(0o555)).unwrap();

        endpoint.consumer().batch_start(0, OutputBatchType::Delta);
        let result = endpoint.encode(batch.arc_as_batch_reader());

        // Restore permissions.
        std::fs::set_permissions(table_dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();

        assert!(result.is_err(), "should fail after exhausting retries");
        // encode() resets progress to 0 after all ranges fail.
        assert_eq!(records_written(&endpoint), 0);
    }

    #[test]
    fn test_progress_large_batch_increments() {
        let table_dir = TempDir::new().unwrap();
        let table_uri = table_dir.path().display().to_string();

        // Use > CHUNK_SIZE records to trigger multiple flush_chunk calls.
        let num = super::CHUNK_SIZE + 500;
        let records = make_records(num);
        let batch = build_insert_batch(&records);
        let mut endpoint = make_endpoint(1, &table_uri, true);

        endpoint.consumer().batch_start(0, OutputBatchType::Delta);
        endpoint
            .encode(batch.clone().arc_as_batch_reader())
            .unwrap();
        assert_eq!(records_written(&endpoint), num as u64);

        // Verify data was written correctly.
        endpoint.consumer().batch_end();
        assert_eq!(records_written(&endpoint), 0);
        let output = read_output(&table_uri);
        assert_eq!(output.len(), num);
    }

    /// Simulate a pipeline restart: drop the first endpoint, create a new one
    /// on the same table with `continue_previous_state=true`. Data written before the
    /// restart must survive.
    #[test]
    fn test_truncate_preserves_data_across_restart() {
        let table_dir = TempDir::new().unwrap();
        let table_uri = table_dir.path().display().to_string();

        // First start: write 50 records.
        let records = make_records(50);
        let batch = build_insert_batch(&records);
        {
            let mut endpoint =
                make_endpoint_ex(1, &table_uri, true, DeltaTableWriteMode::Truncate, false);
            encode_batch(&mut endpoint, &batch);
        }

        assert_eq!(read_delta_output(&table_uri).len(), 50);

        // Restart: create a new endpoint with continue_previous_state=true.
        let more_records: Vec<DeltaTestStruct> = (50..80).map(make_record).collect();
        let batch2 = build_insert_batch(&more_records);
        {
            let mut endpoint =
                make_endpoint_ex(1, &table_uri, true, DeltaTableWriteMode::Truncate, true);
            encode_batch(&mut endpoint, &batch2);
        }

        // All 80 records (50 original + 30 new) must be present in the
        // delta log.  read_delta_output reads through the log, so orphaned
        // parquet files left by SaveMode::Overwrite are not counted.
        let output = read_delta_output(&table_uri);
        assert_eq!(output.len(), 80);
    }

    /// First-start truncation still clears pre-existing data.
    #[test]
    fn test_truncate_clears_data_on_first_start() {
        let table_dir = TempDir::new().unwrap();
        let table_uri = table_dir.path().display().to_string();

        // Seed the table via a first endpoint (simulates pre-existing data).
        let records = make_records(50);
        let batch = build_insert_batch(&records);
        {
            let mut endpoint =
                make_endpoint_ex(1, &table_uri, true, DeltaTableWriteMode::Truncate, false);
            encode_batch(&mut endpoint, &batch);
        }
        assert_eq!(read_output(&table_uri).len(), 50);

        // New pipeline start (continue_previous_state=false) with truncate: old data is wiped.
        let new_records: Vec<DeltaTestStruct> = (100..110).map(make_record).collect();
        let batch2 = build_insert_batch(&new_records);
        {
            let mut endpoint =
                make_endpoint_ex(1, &table_uri, true, DeltaTableWriteMode::Truncate, false);
            encode_batch(&mut endpoint, &batch2);
        }

        // Only the 10 new records should be present in the delta table snapshot.
        // (Orphaned parquet files from the truncated table may still exist on
        // disk, so we read through the delta log rather than scanning all files.)
        let output = read_delta_output(&table_uri);
        assert_eq!(output.len(), 10);
    }

    /// `error_if_exists` mode must not fail on restart.
    #[test]
    fn test_error_if_exists_succeeds_on_restart() {
        let table_dir = TempDir::new().unwrap();
        let table_uri = table_dir.path().display().to_string();

        // First start: create table.
        {
            let mut endpoint = make_endpoint_ex(
                1,
                &table_uri,
                true,
                DeltaTableWriteMode::ErrorIfExists,
                false,
            );
            let batch = build_insert_batch(&make_records(10));
            encode_batch(&mut endpoint, &batch);
        }

        // Restart: should open the existing table without error.
        let endpoint = make_endpoint_ex(
            1,
            &table_uri,
            true,
            DeltaTableWriteMode::ErrorIfExists,
            true,
        );
        drop(endpoint);
    }
}
