use crate::catalog::{CursorWithPolarity, SerBatchReader};
use crate::controller::{ControllerInner, EndpointId};
use crate::format::parquet::relation_to_arrow_fields;
use crate::format::relation_to_parquet_schema;
use crate::format::MAX_DUPLICATES;
use crate::integrated::delta_table::register_storage_handlers;
use crate::transport::Step;
use crate::{
    AsyncErrorCallback, ControllerError, Encoder, OutputConsumer, OutputEndpoint, RecordFormat,
    SerCursor,
};
use anyhow::{anyhow, bail, Error as AnyError, Result as AnyResult};
use arrow::array::RecordBatch;
use arrow::datatypes::Schema as ArrowSchema;
use dbsp::circuit::tokio::TOKIO;
use deltalake::kernel::{Action, DataType, StructField};
use deltalake::operations::create::CreateBuilder;
use deltalake::operations::transaction;
use deltalake::operations::transaction::{CommitBuilder, TableReference};
use deltalake::operations::writer::{DeltaWriter, WriterConfig};
use deltalake::protocol::{DeltaOperation, SaveMode};
use deltalake::DeltaTable;
use feldera_types::serde_with_context::serde_config::{
    BinaryFormat, DecimalFormat, UuidFormat, VariantFormat,
};
use feldera_types::serde_with_context::{DateFormat, SqlSerdeConfig, TimeFormat, TimestampFormat};
use feldera_types::transport::delta_table::DeltaTableWriteMode;
use feldera_types::{program_schema::Relation, transport::delta_table::DeltaTableWriterConfig};
use serde_arrow::schema::SerdeArrowSchema;
use serde_arrow::ArrayBuilder;
use std::cmp::min;
use std::sync::{Arc, Weak};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tracing::{debug, error, info, trace};

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
    controller: Weak<ControllerInner>,
}

pub struct DeltaTableWriter {
    inner: Arc<DeltaTableWriterInner>,
    command_sender: Sender<Command>,
    response_receiver: Receiver<Result<(), (AnyError, bool)>>,
    skipped_deletes: usize,
}

/// Limit on the number of records buffered in memory in the encoder.
static CHUNK_SIZE: usize = 1_000_000;

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
        schema: &Relation,
        controller: Weak<ControllerInner>,
    ) -> Result<Self, ControllerError> {
        register_storage_handlers();

        // Create arrow schema
        let arrow_fields = relation_to_arrow_fields(&schema.fields, true);

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
            controller,
        });
        let inner_clone = inner.clone();

        let (command_sender, command_receiver) = channel::<Command>(1);
        let (response_sender, mut response_receiver) = channel::<Result<(), (AnyError, bool)>>(1);

        // Start tokio runtime.
        std::thread::spawn(move || {
            TOKIO.block_on(async {
                let _ = Self::worker_task(inner_clone, command_receiver, response_sender).await;
            })
        });

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
            skipped_deletes: 0,
        };

        Ok(writer)
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

        let delta_table = CreateBuilder::new()
            .with_location(inner.config.uri.clone())
            .with_save_mode(save_mode)
            .with_storage_options(storage_options)
            .with_columns(inner.struct_fields.clone())
            .await
            .map_err(|e| {
                anyhow!(
                    "error creating or opening delta table '{}': {e}",
                    &inner.config.uri
                )
            })?;

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
                    )
                };
            });
    }
}

impl Encoder for DeltaTableWriter {
    fn consumer(&mut self) -> &mut dyn OutputConsumer {
        self
    }

    fn encode(&mut self, batch: &dyn SerBatchReader) -> AnyResult<()> {
        let mut insert_builder = ArrayBuilder::new(self.inner.serde_arrow_schema.clone())?;

        let mut num_insert_records = 0;

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

            if w < 0 {
                // TODO: we don't support deletes in the parquet format yet.
                // Log the first delete, and then each 10,000's delete.
                if self.skipped_deletes % 10_000 == 0 {
                    error!(
                        "delta table {}: received a 'delete' record, but deletes are not currently supported; record will be dropped (total number of dropped deletes: {})",
                        self.inner.endpoint_name,
                        self.skipped_deletes + 1,
                    );
                }
                self.skipped_deletes += 1;

                cursor.step_key();
                continue;
            }

            while w != 0 {
                cursor.serialize_key_to_arrow(&mut insert_builder)?;
                w -= 1;
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
