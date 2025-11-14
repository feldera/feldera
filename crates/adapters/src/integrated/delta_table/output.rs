use crate::catalog::{CursorWithPolarity, SerBatchReader};
use crate::controller::{ControllerInner, EndpointId};
use crate::format::parquet::relation_to_arrow_fields;
use crate::format::MAX_DUPLICATES;
use crate::integrated::delta_table::register_storage_handlers;
use crate::transport::Step;
use crate::util::{indexed_operation_type, IndexedOperationType};
use crate::{
    AsyncErrorCallback, ControllerError, Encoder, OutputConsumer, OutputEndpoint, RecordFormat,
    SerCursor,
};
use anyhow::{anyhow, bail, Error as AnyError, Result as AnyResult};
use arrow::array::RecordBatch;
use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use chrono::Utc;
use dbsp::circuit::tokio::TOKIO;
use deltalake::kernel::transaction::{CommitBuilder, TableReference};
use deltalake::kernel::{Action, DataType, StructField};
use deltalake::operations::create::CreateBuilder;
use deltalake::operations::write::writer::{DeltaWriter, WriterConfig};
use deltalake::protocol::{DeltaOperation, SaveMode};
use deltalake::DeltaTable;
use feldera_types::program_schema::SqlIdentifier;
use feldera_types::serde_with_context::serde_config::{
    BinaryFormat, DecimalFormat, UuidFormat, VariantFormat,
};
use feldera_types::serde_with_context::{DateFormat, SqlSerdeConfig, TimeFormat, TimestampFormat};
use feldera_types::transport::delta_table::DeltaTableWriteMode;
use feldera_types::{program_schema::Relation, transport::delta_table::DeltaTableWriterConfig};
use serde::Serialize;
use serde_arrow::schema::SerdeArrowSchema;
use serde_arrow::ArrayBuilder;
use std::cmp::min;
use std::sync::{Arc, Weak};
use std::thread;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::{sleep, Duration};
use tracing::{info, trace, warn};

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
}

pub struct DeltaTableWriter {
    inner: Arc<DeltaTableWriterInner>,
    command_sender: Sender<Command>,
    response_receiver: Receiver<Result<(), (AnyError, bool)>>,
}

/// Limit on the number of records buffered in memory in the encoder.
static CHUNK_SIZE: usize = 100_000;

/// Commands sent to the tokio runtime that performs the actual
/// delta table operations.
enum Command {
    BatchStart,
    Insert(RecordBatch),
    BatchEnd,
}

impl DeltaTableWriter {
    pub fn new(
        endpoint_id: EndpointId,
        endpoint_name: &str,
        config: &DeltaTableWriterConfig,
        key_schema: &Option<Relation>,
        value_schema: &Relation,
        controller: Weak<ControllerInner>,
    ) -> Result<Self, ControllerError> {
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
            let data_type = DataType::try_from(f.data_type()).map_err(|e| {
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
        });
        let inner_clone = inner.clone();

        let (command_sender, command_receiver) = channel::<Command>(1);
        let (response_sender, mut response_receiver) = channel::<Result<(), (AnyError, bool)>>(1);

        // Start tokio runtime.
        thread::Builder::new()
            .name(format!("{endpoint_name}-delta-output-tokio-wrapper"))
            .spawn(move || {
                TOKIO.block_on(async {
                    let _ = Self::worker_task(inner_clone, command_receiver, response_sender).await;
                })
            })
            .expect("failed to spawn output delta connector tokio wrapper thread");

        response_receiver
            .blocking_recv()
            .ok_or_else(|| {
                ControllerError::output_transport_error(
                    endpoint_name,
                    true,
                    anyhow!("worker thread terminated unexpectedly during initialization"),
                )
            })?
            .map_err(|(e, _)| ControllerError::output_transport_error(endpoint_name, true, e))?;

        let writer = Self {
            inner,
            command_sender,
            response_receiver,
        };

        Ok(writer)
    }

    fn view_name(&self) -> &SqlIdentifier {
        &self.inner.value_schema.name
    }

    fn command(&mut self, command: Command) -> Result<(), (AnyError, bool)> {
        self.command_sender
            .blocking_send(command)
            .map_err(|_| (anyhow!("worker thread terminated unexpectedly"), true))?;
        self.response_receiver
            .blocking_recv()
            .ok_or_else(|| (anyhow!("worker thread terminated unexpectedly"), true))?
    }

    fn insert_record_batch(&mut self, builder: &mut ArrayBuilder) -> AnyResult<()> {
        let batch = builder
            .to_record_batch()
            .map_err(|e| anyhow!("error generating arrow arrays: {e}"))?;
        self.command(Command::Insert(batch))
            .map_err(|(e, _fatal)| e)
    }

    async fn worker_task(
        inner: Arc<DeltaTableWriterInner>,
        mut command_receiver: Receiver<Command>,
        response_sender: Sender<Result<(), (AnyError, bool)>>,
    ) {
        let mut task = match WriterTask::create(inner.clone()).await {
            Ok(task) => {
                let _ = response_sender.send(Ok(())).await;
                task
            }
            Err(e) => {
                let _ = response_sender
                    .send(Err((
                        anyhow!(
                            "error creating or opening delta table '{}': {e}",
                            &inner.config.uri
                        ),
                        false,
                    )))
                    .await;
                return;
            }
        };

        loop {
            match command_receiver.recv().await {
                Some(Command::BatchStart) => {
                    task.batch_start().await;
                    // Ignore closed channel, we'll handle it at the next loop iteration.
                    let _ = response_sender.send(Ok(())).await;
                }
                Some(Command::BatchEnd) => match task.batch_end().await {
                    Ok(()) => {
                        let _ = response_sender.send(Ok(())).await;
                    }
                    Err(e) => {
                        let _ = response_sender.send(Err((e, false))).await;
                    }
                },
                Some(Command::Insert(batch)) => match task.insert(batch).await {
                    Ok(()) => {
                        let _ = response_sender.send(Ok(())).await;
                    }
                    Err(e) => {
                        let _ = response_sender.send(Err((e, false))).await;
                    }
                },
                None => {
                    trace!(
                        "delta_table {}: endpoint is shutting down",
                        &inner.endpoint_name
                    );
                    return;
                }
            }
        }
    }
}

struct WriterTask {
    inner: Arc<DeltaTableWriterInner>,
    delta_table: DeltaTable,
    writer: Option<DeltaWriter>,
    num_rows: usize,
}

impl WriterTask {
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
                let create_future = CreateBuilder::new()
                    .with_location(inner.config.uri.clone())
                    .with_save_mode(save_mode)
                    .with_storage_options(storage_options.clone())
                    .with_columns(inner.struct_fields.clone());

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
                                &inner.endpoint_name,
                                &inner.config.uri,
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
                                &inner.endpoint_name,
                                &inner.config.uri,
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
            delta_table.version()
        );

        Ok(Self {
            inner,
            delta_table,
            writer: None,
            num_rows: 0,
        })
    }
    async fn batch_start(&mut self) {
        trace!(
            "delta_table {}: starting a new output batch",
            &self.inner.endpoint_name,
        );

        self.num_rows = 0;

        // TODO: make target_file_size configurable.
        // TODO: configure WriterProperties, e.g., do we want to set WriterProperties::sorting_columns?
        let writer_config = WriterConfig::new(
            self.inner.arrow_schema.clone(),
            vec![],
            None,
            None,
            None,
            min(32, self.inner.arrow_schema.fields.len() as i32),
            None,
        );

        self.writer = Some(DeltaWriter::new(
            self.delta_table.object_store(),
            writer_config,
        ));
    }

    async fn batch_end(&mut self) -> AnyResult<()> {
        trace!(
            "delta_table {}: finished writing output records, committing (current table version: {})",
            &self.inner.endpoint_name,
            self.delta_table.version()
        );
        if let Some(writer) = self.writer.take() {
            let actions = writer
                .close()
                .await
                .map_err(|e| anyhow!("error flushing {} Parquet rows: {e}", self.num_rows))?;

            if actions.is_empty() {
                return Ok(());
            }

            let num_bytes = actions.iter().map(|action| action.size as usize).sum();

            // The snapshot version for the next commit is computed as the current version + 1.
            // We need to update the current version manually, since it doesn't happen automatically.
            self.delta_table
                .update_incremental(None)
                .await
                .map_err(|e| anyhow!("error updating delta table version before commit: {e}"))?;

            CommitBuilder::default()
                .with_actions(actions.into_iter().map(Action::Add).collect::<Vec<_>>())
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
                .map_err(|e| anyhow!("error committing changes to the delta table: {e}"))?;

            if let Some(controller) = self.inner.controller.upgrade() {
                controller
                    .status
                    .output_buffer(self.inner.endpoint_id, num_bytes, self.num_rows)
            };
            Ok(())
        } else {
            bail!(
                "delta_table {}: received a BatchEnd without a matching BatchStart",
                &self.inner.endpoint_name
            )
        }
    }

    async fn insert(&mut self, batch: RecordBatch) -> AnyResult<()> {
        if let Some(writer) = &mut self.writer {
            self.num_rows += batch.num_rows();
            trace!(
                "delta_table {}: writing {} records",
                &self.inner.endpoint_name,
                self.num_rows,
            );

            // TODO: add logic to retry failed writes.
            writer
                .write(&batch)
                .await
                .map_err(|e| anyhow!("error writing batch with {} rows: {e}", batch.num_rows()))
        } else {
            bail!(
                "delta_table {}: received Data without a matching BatchStart",
                &self.inner.endpoint_name
            );
        }
    }
}

impl OutputConsumer for DeltaTableWriter {
    fn max_buffer_size_bytes(&self) -> usize {
        usize::MAX
    }

    fn batch_start(&mut self, _step: Step) {
        self.command(Command::BatchStart)
            .unwrap_or_else(|(e, fatal)| {
                if let Some(controller) = self.inner.controller.upgrade() {
                    controller.output_transport_error(
                        self.inner.endpoint_id,
                        &self.inner.endpoint_name,
                        fatal,
                        e,
                        Some("delta_batch_start"),
                    )
                };
            });
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
        self.command(Command::BatchEnd)
            .unwrap_or_else(|(e, fatal)| {
                if let Some(controller) = self.inner.controller.upgrade() {
                    controller.output_transport_error(
                        self.inner.endpoint_id,
                        &self.inner.endpoint_name,
                        fatal,
                        e,
                        Some("delta_batch_end"),
                    )
                };
            });
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

    fn encode(&mut self, batch: &dyn SerBatchReader) -> AnyResult<()> {
        let micros = Utc::now().timestamp_micros();
        let mut insert_builder = ArrayBuilder::new(self.inner.serde_arrow_schema.clone())?;

        let mut num_insert_records = 0;

        let index_name = &self.inner.key_schema.as_ref().map(|s| s.name.to_owned());

        if let Some(index_name) = &index_name {
            let mut cursor =
                batch.cursor(RecordFormat::Parquet(delta_arrow_serde_config().clone()))?;

            while cursor.key_valid() {
                if let Some(op) =
                    indexed_operation_type(self.view_name(), index_name, cursor.as_mut())?
                {
                    cursor.rewind_vals();

                    match op {
                        IndexedOperationType::Insert => cursor
                            .serialize_val_to_arrow_with_metadata(
                                &Meta::new("i", micros),
                                &mut insert_builder,
                            )?,
                        IndexedOperationType::Delete => cursor
                            .serialize_val_to_arrow_with_metadata(
                                &Meta::new("d", micros),
                                &mut insert_builder,
                            )?,
                        IndexedOperationType::Upsert => {
                            assert!(cursor.val_valid());

                            if cursor.weight() < 0 {
                                cursor.step_val();
                            }
                            assert!(cursor.val_valid());

                            cursor.serialize_val_to_arrow_with_metadata(
                                &Meta::new("u", micros),
                                &mut insert_builder,
                            )?;
                        }
                    };

                    num_insert_records += 1;

                    // Split batch into chunks.  This does not affect the number or size of generated
                    // parquet files, since that is controlled by the `DeltaWriter`, but it limits
                    // the amount of memory used by `builder`.
                    if num_insert_records >= CHUNK_SIZE {
                        self.insert_record_batch(&mut insert_builder)?;
                        num_insert_records = 0;
                    }
                };

                cursor.step_key();
            }
        } else {
            let mut cursor = CursorWithPolarity::new(
                batch.cursor(RecordFormat::Parquet(delta_arrow_serde_config().clone()))?,
            );
            while cursor.key_valid() {
                if !cursor.val_valid() {
                    cursor.step_key();
                    continue;
                }

                let mut w = cursor.weight();
                if !(-MAX_DUPLICATES..=MAX_DUPLICATES).contains(&w) {
                    bail!("Unable to output record with very large weight {w}. Consider adjusting your SQL queries to avoid duplicate output records, e.g., using 'SELECT DISTINCT'.");
                }

                while w != 0 {
                    if w > 0 {
                        cursor.serialize_key_to_arrow_with_metadata(
                            &Meta::new("i", micros),
                            &mut insert_builder,
                        )?;
                        w -= 1;
                    } else {
                        cursor.serialize_key_to_arrow_with_metadata(
                            &Meta::new("d", micros),
                            &mut insert_builder,
                        )?;
                        w += 1;
                    }
                    num_insert_records += 1;
                    // Split batch into chunks.  This does not affect the number or size of generated
                    // parquet files, since that is controlled by the `DeltaWriter`, but it limits
                    // the amount of memory used by `builder`.
                    if num_insert_records >= CHUNK_SIZE {
                        self.insert_record_batch(&mut insert_builder)?;
                        num_insert_records = 0;
                    }
                }
                cursor.step_key();
            }
        }

        if num_insert_records > 0 {
            self.insert_record_batch(&mut insert_builder)?;
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
