use crate::catalog::{CursorWithPolarity, SerBatchReader};
use crate::controller::{ControllerInner, EndpointId};
use crate::format::relation_to_parquet_schema;
use crate::format::MAX_DUPLICATES;
use crate::transport::Step;
use crate::{
    AsyncErrorCallback, ControllerError, Encoder, OutputConsumer, OutputEndpoint, RecordFormat,
    SerCursor,
};
use anyhow::{anyhow, bail, Error as AnyError, Result as AnyResult};
use arrow::array::RecordBatch;
use arrow::datatypes::Schema as ArrowSchema;
use deltalake::kernel::{Action, DataType, StructField};
use deltalake::operations::create::CreateBuilder;
use deltalake::operations::transaction::commit;
use deltalake::operations::writer::{DeltaWriter, WriterConfig};
use deltalake::protocol::{DeltaOperation, SaveMode};
use deltalake::DeltaTable;
use log::{debug, trace};
use pipeline_types::{program_schema::Relation, transport::delta_table::DeltaTableWriterConfig};
use serde_arrow::schema::SerdeArrowSchema;
use serde_arrow::ArrowBuilder;
use std::sync::{Arc, Weak};
use tokio::sync::mpsc::{channel, Receiver, Sender};

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
        // Register url handlers, so URL's like `s3://...` are recognized.
        // I hope it is harmless to do this every time a DeltaTablWriter is created.
        deltalake::aws::register_handlers(None);
        deltalake::azure::register_handlers(None);
        deltalake::gcp::register_handlers(None);

        // Create serde arrow schema.
        let serde_arrow_schema = relation_to_parquet_schema(schema, true)?;

        // Create arrow schema
        let arrow_schema = Arc::new(ArrowSchema::new(
            serde_arrow_schema.to_arrow_fields().map_err(|e| {
                ControllerError::schema_validation_error(&format!(
                    "error converting serde schema to arrow schema: {e}"
                ))
            })?,
        ));

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
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .expect("Could not create Tokio runtime");
            rt.block_on(async {
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

    fn insert_record_batch(&mut self, builder: &mut ArrowBuilder) -> AnyResult<()> {
        let arrays = builder
            .build_arrays()
            .map_err(|e| anyhow!("error generating arrow arrays: {e}"))?;
        let batch = RecordBatch::try_new(self.inner.arrow_schema.clone(), arrays)
            .map_err(|e| anyhow!("error generating record batch: {e}"))?;
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
        trace!(
            "delta_table {}: opening or creating delta table '{}'",
            &inner.endpoint_name,
            &inner.config.uri
        );

        let delta_table = CreateBuilder::new()
            .with_location(inner.config.uri.clone())
            // I expected `SaveMode::Append` to be the correct setting, but
            // that always returns an error.
            .with_save_mode(SaveMode::Ignore)
            .with_storage_options(inner.config.object_store_config.clone())
            .with_columns(inner.struct_fields.clone())
            .await
            .map_err(|e| {
                anyhow!(
                    "error creating or opening delta table '{}': {e}",
                    &inner.config.uri
                )
            })?;

        debug!(
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
        let writer_config =
            WriterConfig::new(self.inner.arrow_schema.clone(), vec![], None, None, None);

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
                .update()
                .await
                .map_err(|e| anyhow!("error updating delta table version before commit: {e}"))?;

            commit(
                self.delta_table.log_store().as_ref(),
                &actions.into_iter().map(Action::Add).collect::<Vec<_>>(),
                DeltaOperation::Write {
                    mode: SaveMode::Append,
                    partition_by: None,
                    predicate: None,
                },
                self.delta_table.state.as_ref(),
                None,
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

    fn push_key(&mut self, _key: &[u8], _val: &[u8], _num_records: usize) {
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
        let fields = self.inner.serde_arrow_schema.to_arrow_fields()?;
        let mut insert_builder = ArrowBuilder::new(&fields)?;

        let mut num_insert_records = 0;

        let mut cursor = CursorWithPolarity::new(
            batch.cursor(RecordFormat::Parquet(self.inner.serde_arrow_schema.clone()))?,
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

    fn push_key(&mut self, _key: &[u8], _val: &[u8]) -> AnyResult<()> {
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
mod test {
    use crate::format::parquet::test::load_parquet_file;
    use crate::test::{list_files_recursive, test_circuit, wait, TestStruct2};
    use crate::Controller;
    use env_logger::Env;
    use pipeline_types::config::PipelineConfig;
    use pipeline_types::serde_with_context::{SerializeWithContext, SqlSerdeConfig};
    use proptest::collection::vec;
    use proptest::prelude::{Arbitrary, ProptestConfig, Strategy};
    use proptest::proptest;
    use std::collections::HashMap;
    use std::ffi::OsStr;
    use std::io::Write;
    use std::os::unix::ffi::OsStrExt;
    use std::path::Path;
    use tempfile::{NamedTempFile, TempDir};

    /// Test function that works for both local FS and remote object stores.
    ///
    /// * `verify` - verify the final contents of the delta table is equivalent to
    ///   `data`.  Currently only works for tables in the local FS.
    ///
    /// TODO: implement verification using the delta table API rather than
    /// by reading parquet files directly.  I guess the best way to do this is
    /// to build an input connector.
    fn delta_table_output_test(
        data: Vec<TestStruct2>,
        table_uri: &str,
        object_store_config: &HashMap<String, String>,
        verify: bool,
    ) {
        let _ = env_logger::Builder::from_env(Env::default().default_filter_or("info"))
            .is_test(true)
            .try_init();

        // Output buffer size.
        // Requires: `num_records % buffer_size = 0`.
        let buffer_size = 2000;

        // Buffer timeout.  Must be much longer than what it takes the pipeline
        // to process `buffer_size` records.
        let buffer_timeout_ms = 100;

        println!("delta_table_output_test: preparing input file");
        let mut input_file = NamedTempFile::new().unwrap();
        for v in data.iter() {
            let buffer: Vec<u8> = Vec::new();
            let mut serializer = serde_json::Serializer::new(buffer);
            v.serialize_with_context(&mut serializer, &SqlSerdeConfig::default())
                .unwrap();
            input_file
                .as_file_mut()
                .write_all(&serializer.into_inner())
                .unwrap();
            input_file.write_all(b"\n").unwrap();
        }

        let mut storage_options = String::new();
        for (key, val) in object_store_config.iter() {
            storage_options += &format!("                {key}: \"{val}\"\n");
        }

        // Create controller.
        let config_str = format!(
            r#"
name: test
workers: 4
inputs:
    test_input1:
        stream: test_input1
        transport:
            name: file_input
            config:
                path: "{}"
        format:
            name: json
            config:
                update_format: "raw"
outputs:
    test_output1:
        stream: test_output1
        transport:
            name: "delta_table_output"
            config:
                uri: "{table_uri}"
{}
        enable_output_buffer: true
        max_output_buffer_size_records: {buffer_size}
        max_output_buffer_time_millis: {buffer_timeout_ms}
"#,
            input_file.path().display(),
            storage_options,
        );

        println!(
            "delta_table_output_test: {} records, input file: {}, table uri: {table_uri}",
            data.len(),
            input_file.path().display(),
        );
        let config: PipelineConfig = serde_yaml::from_str(&config_str).unwrap();

        let controller = Controller::with_config(
            |workers| Ok(test_circuit::<TestStruct2>(workers, &TestStruct2::schema())),
            &config,
            Box::new(move |e| panic!("delta_table_output_test: error: {e}")),
        )
        .unwrap();

        controller.start();

        wait(|| controller.status().pipeline_complete(), 20_000);

        if verify {
            let parquet_files =
                list_files_recursive(&Path::new(table_uri), OsStr::from_bytes(b"parquet")).unwrap();

            // // Uncomment to inspect the input JSON file.
            // std::mem::forget(input_file);

            let mut output_records = Vec::with_capacity(data.len());
            for parquet_file in parquet_files {
                let mut records: Vec<TestStruct2> = load_parquet_file(&parquet_file);
                output_records.append(&mut records);
            }

            output_records.sort();

            assert_eq!(output_records, data);
        }

        controller.stop().unwrap();
        println!("delta_table_output_test: success");
    }

    /// Generate up to `max_records` _unique_ records.
    fn data(max_records: usize) -> impl Strategy<Value = Vec<TestStruct2>> {
        vec(TestStruct2::arbitrary(), 0..max_records).prop_map(|vec| {
            let mut idx = 0;
            vec.into_iter()
                .map(|mut x| {
                    x.field = idx;
                    idx += 1;
                    x
                })
                .collect()
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(2))]
        #[test]
        fn delta_table_output_file_proptest(data in data(100_000))
        {
            let table_dir = TempDir::new().unwrap();
            delta_table_output_test(data, &table_dir.path().display().to_string(), &HashMap::new(), true);
            // // Uncomment to inspect output parquet files produced by the test.
            // std::mem::forget(table_dir);
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(1))]

        #[cfg(feature = "delta-s3-test")]
        #[test]
        fn delta_table_output_s3_proptest(data in data(100_000))
        {
            let uuid = uuid::Uuid::new_v4();
            let object_store_config = [
                ("aws_access_key_id".to_string(), std::env::var("DELTA_TABLE_TEST_AWS_ACCESS_KEY_ID").unwrap()),
                ("aws_secret_access_key".to_string(), std::env::var("DELTA_TABLE_TEST_AWS_SECRET_ACCESS_KEY").unwrap()),
                // AWS region must be specified (see https://github.com/delta-io/delta-rs/issues/1095).
                ("aws_region".to_string(), "us-east-2".to_string()),
                ("AWS_S3_ALLOW_UNSAFE_RENAME".to_string(), "true".to_string()),
            ]
                .into_iter()
                .collect::<HashMap<_,_>>();

            // TODO: enable verification when it's supported for S3.
            delta_table_output_test(data.clone(), &format!("s3://feldera-delta-table-test/{uuid}/"), &object_store_config, false);
            delta_table_output_test(data, &format!("s3://feldera-delta-table-test/{uuid}/"), &object_store_config, false);
        }

    }
}
