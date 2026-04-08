use crate::catalog::{CursorWithPolarity, SerBatchReader, SplitCursorBuilder};
use crate::controller::{ControllerInner, EndpointId};
use crate::format::MAX_DUPLICATES;
use crate::format::parquet::relation_to_arrow_fields;
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
use deltalake::kernel::{Action, Add, DataType, StructField};
use deltalake::logstore::ObjectStoreRef;
use deltalake::operations::create::CreateBuilder;
use deltalake::operations::write::writer::{DeltaWriter, WriterConfig};
use deltalake::protocol::{DeltaOperation, SaveMode};
use feldera_types::serde_with_context::serde_config::{
    BinaryFormat, DecimalFormat, UuidFormat, VariantFormat,
};
use feldera_types::serde_with_context::{DateFormat, SqlSerdeConfig, TimeFormat, TimestampFormat};
use feldera_types::transport::delta_table::DeltaTableWriteMode;
use feldera_types::{
    adapter_stats::ConnectorHealth, program_schema::Relation,
    transport::delta_table::DeltaTableWriterConfig,
};
use serde::Serialize;
use serde_arrow::ArrayBuilder;
use serde_arrow::schema::SerdeArrowSchema;
use std::cmp::min;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use tokio::time::{Duration, sleep};
use tracing::{Instrument, info, info_span, warn};

/// Arrow serde config for reading/writing Delta tables.
pub const fn delta_arrow_serde_config() -> &'static SqlSerdeConfig {
    &SqlSerdeConfig {
        timestamp_format: TimestampFormat::MicrosSinceEpoch,
        time_format: TimeFormat::NanosSigned,
        date_format: DateFormat::String("%Y-%m-%d"),
        decimal_format: DecimalFormat::String,
        variant_format: VariantFormat::JsonString,
        binary_format: BinaryFormat::Array,
        uuid_format: UuidFormat::String,
    }
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
}

pub struct DeltaTableWriter {
    inner: Arc<DeltaTableWriterInner>,
    object_store: ObjectStoreRef,
    task: WriterTask,
    threads: usize,
    pending_actions: Vec<Add>,
    num_rows: usize,
}

/// Limit on the number of records buffered in memory in the encoder.
const CHUNK_SIZE: usize = 100_000;

impl DeltaTableWriter {
    pub fn new(
        endpoint_id: EndpointId,
        endpoint_name: &str,
        config: &DeltaTableWriterConfig,
        key_schema: &Option<Relation>,
        value_schema: &Relation,
        controller: Weak<ControllerInner>,
    ) -> Result<Self, ControllerError> {
        config.validate().map_err(|e| {
            ControllerError::invalid_transport_configuration(endpoint_name, &e.to_string())
        })?;

        let threads = config.threads.unwrap_or(1);

        if threads > 1 && key_schema.is_none() {
            return Err(ControllerError::invalid_transport_configuration(
                endpoint_name,
                "Parallel writes (threads > 1) require the view to have a unique key to \
                 ensure correct ordering of inserts and deletes. Please specify the `index` \
                 property in the connector configuration. For more details, see: \
                 https://docs.feldera.com/connectors/unique_keys",
            ));
        }

        register_storage_handlers();

        // Create arrow schema
        let mut arrow_fields = relation_to_arrow_fields(&value_schema.fields, true);
        arrow_fields.push(ArrowField::new("__feldera_op", ArrowDataType::Utf8, true));
        arrow_fields.push(ArrowField::new("__feldera_ts", ArrowDataType::Int64, true));

        // Create serde arrow schema.
        let serde_arrow_schema =
            SerdeArrowSchema::try_from(arrow_fields.as_slice()).map_err(|e| {
                ControllerError::SchemaParseError {
                    error: format!("Unable to convert schema to parquet/arrow: {e}"),
                }
            })?;

        let arrow_schema = Arc::new(ArrowSchema::new(arrow_fields));

        let mut struct_fields: Vec<_> = vec![];

        for f in arrow_schema.fields.iter() {
            let data_type = DataType::try_from_arrow(f.data_type()).map_err(|e| {
                ControllerError::output_transport_error(
                    endpoint_name,
                    true,
                    anyhow!("error converting arrow field '{f}' to a Delta Lake field: {e}"),
                )
            })?;
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
            .block_on(WriterTask::create(inner.clone()))
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

        Ok(Self {
            inner,
            object_store,
            task,
            threads,
            pending_actions: Vec::new(),
            num_rows: 0,
        })
    }
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

    async fn create(inner: Arc<DeltaTableWriterInner>) -> AnyResult<Self> {
        let mut storage_options = inner.config.object_store_config.clone();

        // FIXME: S3 does not support the atomic rename operation required by delta. This is not a problem
        // with a single writer, but multiple writers require an external coordinator service.
        // `delta-rs` users tend to rely on the DynamoDB lock client for this
        // (see `object_store::aws::DynamoCommit`), but that only helps if all writers use the
        // same lock service.  For now we simply tell the object store client to use unsafe renames
        // and hope for the best.  Without this config option, writes to S3-based delta tables will fail.
        storage_options.insert("AWS_S3_ALLOW_UNSAFE_RENAME".to_string(), "true".to_string());

        let save_mode = match inner.config.mode {
            // I expected `SaveMode::Append` to be the correct setting, but
            // that always returns an error.
            DeltaTableWriteMode::Append => SaveMode::Ignore,
            DeltaTableWriteMode::Truncate => SaveMode::Overwrite,
            DeltaTableWriteMode::ErrorIfExists => SaveMode::ErrorIfExists,
        };

        info!(
            "delta_table {}: opening or creating delta table '{}' in '{save_mode:?}' mode",
            &inner.endpoint_name, &inner.config.uri
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
        None,
        None,
        DataSkippingNumIndexedCols::NumColumns(num_indexed_cols),
        None,
    );
    let mut writer = DeltaWriter::new(object_store, writer_config);
    let mut insert_builder = ArrayBuilder::new(inner.serde_arrow_schema.clone())
        .map_err(|e| WriteError::Deterministic(anyhow!("error creating array builder: {e}")))?;
    let mut num_records = 0;
    let index_name = inner.key_schema.as_ref().map(|s| &s.name);

    if let Some(index_name) = index_name {
        let mut cursor = cursor_builder.build();

        while cursor.key_valid() {
            let op = indexed_operation_type(&inner.value_schema.name, index_name, &mut cursor)
                .map_err(WriteError::Deterministic)?;

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
        let mut cursor = CursorWithPolarity::new(Box::new(cursor));

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

    fn batch_start(&mut self, _step: Step) {
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
        if self.pending_actions.is_empty() {
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

impl Encoder for DeltaTableWriter {
    fn consumer(&mut self) -> &mut dyn OutputConsumer {
        self
    }

    fn encode(&mut self, batch: Arc<dyn SerBatchReader>) -> AnyResult<()> {
        let threads = self.threads;
        let mut bounds = batch.keys_factory().default_box();
        batch.partition_keys(threads, &mut *bounds);

        let mut cursor_builders = Vec::new();
        for i in 0..=bounds.len() {
            let Some(cb) = SplitCursorBuilder::from_bounds(
                batch.clone(),
                &*bounds,
                i,
                RecordFormat::Parquet(delta_arrow_serde_config().clone()),
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

    fn batch_start(&mut self, _step: Step) -> AnyResult<()> {
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
    use feldera_types::transport::delta_table::{DeltaTableWriteMode, DeltaTableWriterConfig};
    use tempfile::TempDir;

    use crate::catalog::SerBatch;
    use crate::controller::EndpointId;
    use crate::format::Encoder;
    use crate::format::parquet::test::load_parquet_file;
    use crate::static_compile::seroutput::SerBatchImpl;
    use crate::test::data::{DeltaTestKey, DeltaTestStruct, TestStruct};
    use crate::test::list_files_recursive;

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
        }
    }

    fn value_relation() -> Relation {
        let mut rel = DeltaTestStruct::relation_schema();
        rel.materialized = true;
        rel
    }

    fn make_endpoint(threads: usize, table_uri: &str, indexed: bool) -> DeltaTableWriter {
        let key_schema = if indexed { Some(key_relation()) } else { None };
        DeltaTableWriter::new(
            EndpointId::default(),
            "test_endpoint",
            &DeltaTableWriterConfig {
                uri: table_uri.to_string(),
                mode: DeltaTableWriteMode::Truncate,
                max_retries: Some(0),
                threads: Some(threads),
                object_store_config: Default::default(),
                checkpoint_interval: None,
            },
            &key_schema,
            &value_relation(),
            Weak::new(),
        )
        .expect("failed to create endpoint")
    }

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
        endpoint.consumer().batch_start(0);
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
            let mut batch: Vec<OutputRecord> = load_parquet_file(&path);
            records.append(&mut batch);
        }
        records
    }

    fn make_record(i: usize) -> DeltaTestStruct {
        DeltaTestStruct {
            bigint: i as i64,
            binary: ByteArray::from_vec(vec![i as u8, (i >> 8) as u8]),
            boolean: i % 2 == 0,
            date: Date::from_days(i as i32 % 100_000),
            decimal_10_3: SqlDecimal::<10, 3>::new((i as i128 % 1_000_000) * 1000, 3).unwrap(),
            double: F64::new((i as f64).trunc()),
            float: F32::new((i as f32).trunc()),
            int: i as i32,
            smallint: (i % 32000) as i16,
            string: format!("record_{i}"),
            unused: if i % 3 == 0 {
                None
            } else {
                Some(format!("unused_{i}"))
            },
            timestamp_ntz: Timestamp::from_milliseconds(1704070800000 + i as i64 * 1000),
            tinyint: (i % 120) as i8,
            string_array: vec![format!("arr_{i}")],
            struct1: TestStruct {
                id: i as u32,
                b: i % 2 == 0,
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
                uri: table_uri,
                mode: DeltaTableWriteMode::Truncate,
                max_retries: Some(0),
                threads: Some(4),
                object_store_config: Default::default(),
                checkpoint_interval: None,
            },
            &key_schema,
            &value_relation(),
            Weak::new(),
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

        endpoint.consumer().batch_start(0);
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
                uri: table_uri.clone(),
                mode: DeltaTableWriteMode::Truncate,
                max_retries: Some(1),
                threads: Some(1),
                object_store_config: Default::default(),
                checkpoint_interval: None,
            },
            &key_schema,
            &value_relation(),
            Weak::new(),
        )
        .expect("failed to create endpoint");

        // Make directory read-only to trigger write failure.
        std::fs::set_permissions(table_dir.path(), std::fs::Permissions::from_mode(0o555)).unwrap();

        endpoint.consumer().batch_start(0);
        let result = endpoint.encode(batch.arc_as_batch_reader());

        // Restore permissions.
        std::fs::set_permissions(table_dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();

        assert!(result.is_err(), "should fail after exhausting retries");
    }

    /// Verify that threads=0 is rejected in config validation.
    #[test]
    fn test_threads_zero_rejected() {
        let config = DeltaTableWriterConfig {
            uri: "/tmp/test".to_string(),
            mode: DeltaTableWriteMode::Truncate,
            max_retries: Some(0),
            threads: Some(0),
            object_store_config: Default::default(),
            checkpoint_interval: None,
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
        endpoint.consumer().batch_start(0);
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

        endpoint.consumer().batch_start(0);
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

        endpoint.consumer().batch_start(0);
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
        endpoint.consumer().batch_start(0);
        endpoint
            .encode(batch1.clone().arc_as_batch_reader())
            .unwrap();
        assert_eq!(records_written(&endpoint), 50);
        endpoint.consumer().batch_end();
        assert_eq!(records_written(&endpoint), 0);

        // Batch 2: 30 records (ids 50..80).
        let records2: Vec<_> = (50..80).map(make_record).collect();
        let batch2 = build_insert_batch(&records2);
        endpoint.consumer().batch_start(1);
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

        endpoint.consumer().batch_start(0);
        endpoint
            .encode(batch.clone().arc_as_batch_reader())
            .unwrap();
        assert_eq!(records_written(&endpoint), 50);

        // batch_start without batch_end resets the counter.
        endpoint.consumer().batch_start(1);
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

        endpoint.consumer().batch_start(0);
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
                uri: table_uri.clone(),
                mode: DeltaTableWriteMode::Truncate,
                max_retries: Some(1),
                threads: Some(1),
                object_store_config: Default::default(),
                checkpoint_interval: None,
            },
            &key_schema,
            &value_relation(),
            Weak::new(),
        )
        .expect("failed to create endpoint");

        // Make directory read-only to trigger write failure with retries.
        std::fs::set_permissions(table_dir.path(), std::fs::Permissions::from_mode(0o555)).unwrap();

        endpoint.consumer().batch_start(0);
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

        endpoint.consumer().batch_start(0);
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
}
