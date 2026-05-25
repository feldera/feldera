use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Weak;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Result as AnyResult, anyhow, bail};
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::types::{AttributeValue, DeleteRequest, PutRequest, WriteRequest};
use dbsp::circuit::tokio::TOKIO;
use feldera_adapterlib::catalog::SplitCursorBuilder;
use feldera_adapterlib::metrics::{ConnectorMetrics, ValueType};
use feldera_adapterlib::transport::{AsyncErrorCallback, OutputBatchType, Step};
use feldera_types::program_schema::{Relation, SqlIdentifier};
use feldera_types::transport::dynamodb::{DynamoDBWriteMode, DynamoDBWriterConfig};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::{debug, info, info_span, warn};

use crate::ControllerError;
use crate::catalog::{RecordFormat, SerBatchReader, SerCursor};
use crate::controller::{ControllerInner, EndpointId};
use crate::format::{Encoder, OutputConsumer};
use crate::transport::OutputEndpoint;
use crate::util::{IndexedOperationType, indexed_operation_type};

use super::helpers;

#[derive(Default)]
pub(crate) struct DynamoDBOutputMetrics {
    retries: AtomicU64,
    batches_written: AtomicU64,
    records_written: AtomicU64,
    bytes_written: AtomicU64,
    write_chunks: AtomicU64,
    duplicate_keys_skipped: AtomicU64,
}

impl DynamoDBOutputMetrics {
    fn record_committed_batch(&self, records: u64, bytes: u64, retries: u64) {
        self.records_written.fetch_add(records, Ordering::Relaxed);
        self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
        self.retries.fetch_add(retries, Ordering::Relaxed);
        self.batches_written.fetch_add(1, Ordering::Relaxed);
    }

    fn record_write_chunk(&self) {
        self.write_chunks.fetch_add(1, Ordering::Relaxed);
    }

    fn record_duplicate_key_skip(&self) {
        self.duplicate_keys_skipped.fetch_add(1, Ordering::Relaxed);
    }
}

impl ConnectorMetrics for DynamoDBOutputMetrics {
    fn metrics(&self) -> Vec<(&'static str, &'static str, ValueType, f64)> {
        vec![
            (
                "dynamodb_output_retries_total",
                "Total number of DynamoDB write retries performed by the output connector.",
                ValueType::Counter,
                self.retries.load(Ordering::Relaxed) as f64,
            ),
            (
                "dynamodb_output_batches_written_total",
                "Total number of output batches successfully written to DynamoDB.",
                ValueType::Counter,
                self.batches_written.load(Ordering::Relaxed) as f64,
            ),
            (
                "dynamodb_output_records_written_total",
                "Total number of records successfully written to DynamoDB.",
                ValueType::Counter,
                self.records_written.load(Ordering::Relaxed) as f64,
            ),
            (
                "dynamodb_output_bytes_written_total",
                "Total number of encoded bytes successfully written to DynamoDB.",
                ValueType::Counter,
                self.bytes_written.load(Ordering::Relaxed) as f64,
            ),
            (
                "dynamodb_output_write_chunks_total",
                "Total number of DynamoDB write chunks submitted by the output connector.",
                ValueType::Counter,
                self.write_chunks.load(Ordering::Relaxed) as f64,
            ),
            (
                "dynamodb_output_duplicate_keys_skipped_total",
                "Total number of output records skipped because their key was not unique.",
                ValueType::Counter,
                self.duplicate_keys_skipped.load(Ordering::Relaxed) as f64,
            ),
        ]
    }
}

#[derive(Clone, Copy)]
enum BroadcastCommand {
    BatchStart,
    BatchEnd,
    Shutdown,
}

enum WorkerCommand {
    Broadcast(BroadcastCommand),
    Encode(SplitCursorBuilder),
}

enum WorkerResult {
    Ok {
        num_bytes: usize,
        num_rows: usize,
        num_retries: u64,
    },
    Err(anyhow::Error),
}

pub(crate) struct DynamoDBWorker {
    endpoint_id: EndpointId,
    endpoint_name: String,
    controller: Weak<ControllerInner>,
    table: String,
    write_mode: DynamoDBWriteMode,
    batch_size: usize,
    max_retries: Option<u8>,
    client: Client,
    key_schema: Relation,
    value_schema: Relation,
    /// Write requests accumulated since the last flush. Flushed when the count
    /// reaches `batch_size` or the byte total reaches `max_buffer_size_bytes`.
    pub(crate) pending: Vec<WriteRequest>,
    pending_bytes: usize,
    max_buffer_size_bytes: usize,
    pub(crate) records_written: Arc<AtomicU64>,
    pub(crate) metrics: Arc<DynamoDBOutputMetrics>,
    /// Limits the number of DynamoDB write requests in flight at once.
    write_semaphore: Arc<Semaphore>,
    /// Tokio tasks spawned by `flush()` writing batches to DynamoDB concurrently.
    /// Completed tasks are drained eagerly in `flush()` and fully at `batch_end`.
    pending_writes: JoinSet<AnyResult<u64>>,
    completed_retries: u64,
}

impl DynamoDBWorker {
    #[cfg(test)]
    pub(crate) fn with_client(
        client: Client,
        endpoint_name: &str,
        config: &DynamoDBWriterConfig,
        key_schema: &Relation,
        value_schema: &Relation,
        records_written: Arc<AtomicU64>,
    ) -> Self {
        let metrics = Arc::new(DynamoDBOutputMetrics::default());
        Self::with_client_and_controller(
            client,
            EndpointId::default(),
            endpoint_name,
            Weak::new(),
            config,
            key_schema,
            value_schema,
            records_written,
            metrics,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn with_client_and_controller(
        client: Client,
        endpoint_id: EndpointId,
        endpoint_name: &str,
        controller: Weak<ControllerInner>,
        config: &DynamoDBWriterConfig,
        key_schema: &Relation,
        value_schema: &Relation,
        records_written: Arc<AtomicU64>,
        metrics: Arc<DynamoDBOutputMetrics>,
    ) -> Self {
        Self {
            endpoint_id,
            endpoint_name: endpoint_name.to_string(),
            controller,
            table: config.table.clone(),
            write_mode: config.write_mode,
            batch_size: config.effective_batch_size(),
            max_retries: config.max_retries,
            client,
            key_schema: key_schema.clone(),
            value_schema: value_schema.clone(),
            pending: Vec::new(),
            pending_bytes: 0,
            max_buffer_size_bytes: config.max_buffer_size_bytes,
            records_written,
            metrics,
            write_semaphore: Arc::new(Semaphore::new(config.max_concurrent_requests)),
            pending_writes: JoinSet::new(),
            completed_retries: 0,
        }
    }

    fn view_name(&self) -> &SqlIdentifier {
        &self.value_schema.name
    }

    fn index_name(&self) -> &SqlIdentifier {
        &self.key_schema.name
    }

    /// Drains `pending_writes`, collecting retries and errors.
    fn drain_pending_writes(&mut self) -> (u64, Vec<anyhow::Error>) {
        let mut total_retries = std::mem::take(&mut self.completed_retries);
        let mut errors = Vec::new();
        TOKIO.block_on(async {
            while let Some(result) = self.pending_writes.join_next().await {
                match result {
                    Ok(Ok(retries)) => total_retries += retries,
                    Ok(Err(e)) => errors.push(e),
                    Err(e) => errors.push(anyhow!("write task panicked: {e}")),
                }
            }
        });
        (total_retries, errors)
    }

    pub(crate) fn batch_start_inner(&mut self) {
        self.pending.clear();
        self.pending_bytes = 0;
        self.completed_retries = 0;
    }

    pub(crate) fn batch_end_inner(&mut self) -> AnyResult<(usize, usize, u64)> {
        let flush_result = self.flush();

        let (total_retries, mut errors) = self.drain_pending_writes();

        let (bytes, rows) = match flush_result {
            Ok(result) => result,
            Err(e) => {
                errors.push(e);
                (0, 0)
            }
        };

        if !errors.is_empty() {
            let msg = errors
                .iter()
                .map(|e| format!("{e:#}"))
                .collect::<Vec<_>>()
                .join("; ");
            bail!("DynamoDB writes failed: {msg}");
        }

        Ok((bytes, rows, total_retries))
    }

    fn push_request(&mut self, request: WriteRequest, bytes: usize) -> AnyResult<(usize, usize)> {
        self.pending.push(request);
        self.pending_bytes += bytes;

        if self.pending.len() >= self.batch_size || self.pending_bytes >= self.max_buffer_size_bytes
        {
            self.flush()
        } else {
            Ok((0, 0))
        }
    }

    pub(crate) fn flush(&mut self) -> AnyResult<(usize, usize)> {
        if self.pending.is_empty() {
            return Ok((0, 0));
        }

        // Block until a write slot is free, then drain any completed tasks before spawning.
        let permit = TOKIO
            .block_on(Arc::clone(&self.write_semaphore).acquire_owned())
            .map_err(|_| anyhow!("DynamoDB write semaphore closed"))?;
        while let Some(result) = self.pending_writes.try_join_next() {
            match result {
                Ok(Ok(retries)) => self.completed_retries += retries,
                Ok(Err(e)) => return Err(e),
                Err(e) => bail!("write task panicked: {e}"),
            }
        }

        let requests = std::mem::replace(&mut self.pending, Vec::with_capacity(self.batch_size));
        let rows = requests.len();
        let bytes = std::mem::take(&mut self.pending_bytes);

        let client = self.client.clone();
        let endpoint_name = self.endpoint_name.clone();
        let table = self.table.clone();
        let write_mode = self.write_mode;
        let max_retries: Option<usize> = self.max_retries.map(|n| n as usize);
        let metrics = self.metrics.clone();

        metrics.record_write_chunk();
        self.pending_writes.spawn_on(
            async move {
                let _permit = permit;
                let start = Instant::now();
                let result: AnyResult<u64> = match write_mode {
                    DynamoDBWriteMode::Batch => {
                        helpers::write_batch_chunk(
                            client,
                            endpoint_name.clone(),
                            table,
                            requests,
                            max_retries,
                        )
                        .await
                    }
                    DynamoDBWriteMode::Transactional => {
                        let transact_items = requests
                            .iter()
                            .map(|r| helpers::to_transact_item(&table, r))
                            .collect::<AnyResult<Vec<_>>>()?;
                        helpers::write_transact_chunk(
                            client,
                            endpoint_name.clone(),
                            transact_items,
                            max_retries,
                        )
                        .await
                    }
                };
                debug!(
                    endpoint = %endpoint_name,
                    rows,
                    bytes,
                    elapsed_ms = start.elapsed().as_millis(),
                    success = result.is_ok(),
                    "dynamodb: flushed batch",
                );
                result
            },
            TOKIO.handle(),
        );

        self.records_written
            .fetch_add(rows as u64, Ordering::Relaxed);
        Ok((bytes, rows))
    }

    pub(crate) fn encode_cursor(
        &mut self,
        cursor: &mut dyn SerCursor,
    ) -> AnyResult<(usize, usize)> {
        let mut num_bytes = 0usize;
        let mut num_rows = 0usize;

        while cursor.key_valid() {
            let op = match indexed_operation_type(self.view_name(), self.index_name(), cursor) {
                Ok(op) => op,
                Err(e) => {
                    self.metrics.record_duplicate_key_skip();
                    if let Some(controller) = self.controller.upgrade() {
                        controller.output_transport_error(
                            self.endpoint_id,
                            &self.endpoint_name,
                            false,
                            e,
                            Some("dynamodb_uniqueness_violation"),
                        );
                    } else {
                        warn!(
                            endpoint = %self.endpoint_name,
                            "dynamodb: skipping non-unique output key: {e:#}"
                        );
                    }
                    None
                }
            };

            if let Some(op) = op {
                cursor.rewind_vals();
                let (request, bytes) = match op {
                    IndexedOperationType::Insert => {
                        let item = self.item(cursor)?;
                        let bytes = helpers::item_size(&item);
                        (
                            WriteRequest::builder()
                                .put_request(PutRequest::builder().set_item(Some(item)).build()?)
                                .build(),
                            bytes,
                        )
                    }
                    IndexedOperationType::Delete => {
                        let key = self.key(cursor)?;
                        let bytes = helpers::item_size(&key);
                        (
                            WriteRequest::builder()
                                .delete_request(
                                    DeleteRequest::builder().set_key(Some(key)).build()?,
                                )
                                .build(),
                            bytes,
                        )
                    }
                    IndexedOperationType::Upsert => {
                        if cursor.weight() < 0 {
                            cursor.step_val();
                        }
                        let item = self.item(cursor)?;
                        let bytes = helpers::item_size(&item);
                        (
                            WriteRequest::builder()
                                .put_request(PutRequest::builder().set_item(Some(item)).build()?)
                                .build(),
                            bytes,
                        )
                    }
                };

                let (flushed_bytes, flushed_rows) = self.push_request(request, bytes)?;
                num_bytes += flushed_bytes;
                num_rows += flushed_rows;
            }

            cursor.step_key();
        }

        Ok((num_bytes, num_rows))
    }

    fn item(&mut self, cursor: &mut dyn SerCursor) -> AnyResult<HashMap<String, AttributeValue>> {
        cursor.val_to_dynamodb_item()
    }

    fn key(&mut self, cursor: &mut dyn SerCursor) -> AnyResult<HashMap<String, AttributeValue>> {
        let key = cursor.key_to_dynamodb_item()?;
        if key.is_empty() {
            bail!("unreachable: dynamodb output connector requires a non-empty index key");
        }
        Ok(key)
    }

    fn run(
        mut self,
        cmd_rx: crossbeam::channel::Receiver<WorkerCommand>,
        result_tx: crossbeam::channel::Sender<WorkerResult>,
    ) {
        while let Ok(cmd) = cmd_rx.recv() {
            match cmd {
                WorkerCommand::Broadcast(BroadcastCommand::BatchStart) => {
                    self.batch_start_inner();
                    let _ = result_tx.send(WorkerResult::Ok {
                        num_bytes: 0,
                        num_rows: 0,
                        num_retries: 0,
                    });
                }
                WorkerCommand::Encode(cursor_builder) => {
                    let mut cursor = cursor_builder.build();
                    match self.encode_cursor(&mut cursor) {
                        Ok((num_bytes, num_rows)) => {
                            let _ = result_tx.send(WorkerResult::Ok {
                                num_bytes,
                                num_rows,
                                num_retries: 0,
                            });
                        }
                        Err(e) => {
                            let _ = result_tx.send(WorkerResult::Err(e));
                        }
                    }
                }
                WorkerCommand::Broadcast(BroadcastCommand::BatchEnd) => {
                    match self.batch_end_inner() {
                        Ok((num_bytes, num_rows, num_retries)) => {
                            let _ = result_tx.send(WorkerResult::Ok {
                                num_bytes,
                                num_rows,
                                num_retries,
                            });
                        }
                        Err(e) => {
                            let _ = result_tx.send(WorkerResult::Err(e));
                        }
                    }
                }
                WorkerCommand::Broadcast(BroadcastCommand::Shutdown) => {
                    let (_, errors) = self.drain_pending_writes();
                    if !errors.is_empty() {
                        let msg = errors
                            .iter()
                            .map(|e| format!("{e:#}"))
                            .collect::<Vec<_>>()
                            .join("; ");
                        warn!(
                            endpoint = %self.endpoint_name,
                            "dynamodb: write error(s) on shutdown (data may be lost): {msg}"
                        );
                    }
                    break;
                }
            }
        }
    }
}

pub(crate) struct WorkerHandle {
    cmd_tx: crossbeam::channel::Sender<WorkerCommand>,
    result_rx: crossbeam::channel::Receiver<WorkerResult>,
    thread: Option<std::thread::JoinHandle<()>>,
}

pub struct DynamoDBOutputEndpoint {
    pub(crate) endpoint_id: EndpointId,
    pub(crate) endpoint_name: String,
    pub(crate) config: DynamoDBWriterConfig,
    pub(crate) controller: Weak<ControllerInner>,
    pub(crate) handles: Vec<WorkerHandle>,
    pub(crate) records_written: Arc<AtomicU64>,
    pub(crate) metrics: Arc<DynamoDBOutputMetrics>,
    pub(crate) num_bytes: usize,
    pub(crate) num_rows: usize,
    // Periodic throughput summary state.
    pub(crate) rows_since_last_log: u64,
    pub(crate) bytes_since_last_log: u64,
    pub(crate) retries_since_last_log: u64,
    pub(crate) batches_since_last_log: u64,
    pub(crate) last_throughput_log: Instant,
}

impl Drop for DynamoDBOutputEndpoint {
    fn drop(&mut self) {
        for handle in &self.handles {
            let _ = handle
                .cmd_tx
                .send(WorkerCommand::Broadcast(BroadcastCommand::Shutdown));
        }
        for handle in &mut self.handles {
            if let Some(thread) = handle.thread.take() {
                let _ = thread.join();
            }
        }
    }
}

impl DynamoDBOutputEndpoint {
    pub fn new(
        endpoint_id: EndpointId,
        endpoint_name: &str,
        config: &DynamoDBWriterConfig,
        key_schema: &Option<Relation>,
        value_schema: &Relation,
        controller: Weak<ControllerInner>,
        is_index: bool,
    ) -> Result<Self, ControllerError> {
        config.validate().map_err(|e| {
            ControllerError::invalid_transport_configuration(endpoint_name, &e.to_string())
        })?;

        if !is_index || key_schema.is_none() {
            return Err(ControllerError::not_supported(
                "DynamoDB output connector requires the view to have a unique key. Please specify the `index` property in the connector configuration. For more details, see: https://docs.feldera.com/connectors/unique_keys",
            ));
        }

        let key_schema = key_schema.as_ref().unwrap();
        let records_written = Arc::new(AtomicU64::new(0));
        let metrics = Arc::new(DynamoDBOutputMetrics::default());
        if let Some(controller) = controller.upgrade() {
            controller
                .status
                .register_batch_progress_counter(&endpoint_id, records_written.clone());
            controller
                .status
                .set_output_custom_metrics(endpoint_id, metrics.clone());
        }

        let client = helpers::make_client(config);
        let mut handles = Vec::with_capacity(config.threads);
        for i in 0..config.threads {
            let worker = DynamoDBWorker::with_client_and_controller(
                client.clone(),
                endpoint_id,
                endpoint_name,
                controller.clone(),
                config,
                key_schema,
                value_schema,
                records_written.clone(),
                metrics.clone(),
            );

            let (cmd_tx, cmd_rx) = crossbeam::channel::bounded(1);
            let (result_tx, result_rx) = crossbeam::channel::bounded(1);
            let thread_name = format!("dynamodb-output-{endpoint_name}-{i}");
            let thread = std::thread::Builder::new()
                .name(thread_name)
                .spawn(move || worker.run(cmd_rx, result_tx))
                .map_err(|e| {
                    ControllerError::output_transport_error(
                        endpoint_name,
                        true,
                        anyhow!("failed to spawn worker thread: {e}"),
                    )
                })?;

            handles.push(WorkerHandle {
                cmd_tx,
                result_rx,
                thread: Some(thread),
            });
        }

        Ok(Self {
            endpoint_id,
            endpoint_name: endpoint_name.to_string(),
            config: config.clone(),
            controller,
            handles,
            records_written,
            metrics,
            num_bytes: 0,
            num_rows: 0,
            rows_since_last_log: 0,
            bytes_since_last_log: 0,
            retries_since_last_log: 0,
            batches_since_last_log: 0,
            last_throughput_log: Instant::now(),
        })
    }

    fn broadcast_and_collect(&mut self, command: BroadcastCommand) -> AnyResult<u64> {
        for handle in &self.handles {
            let _ = handle.cmd_tx.send(WorkerCommand::Broadcast(command));
        }

        let mut num_retries = 0u64;
        let mut errors = Vec::new();
        for handle in &self.handles {
            match handle.result_rx.recv() {
                Ok(WorkerResult::Ok {
                    num_bytes,
                    num_rows,
                    num_retries: retries,
                }) => {
                    self.num_bytes += num_bytes;
                    self.num_rows += num_rows;
                    num_retries += retries;
                }
                Ok(WorkerResult::Err(e)) => errors.push(e),
                Err(_) => errors.push(anyhow!("worker thread disconnected")),
            }
        }

        if !errors.is_empty() {
            let msg = errors
                .iter()
                .map(|e| format!("{e:#}"))
                .collect::<Vec<_>>()
                .join("; ");
            bail!("{} DynamoDB worker(s) failed: {msg}", errors.len());
        }

        Ok(num_retries)
    }
}

impl OutputConsumer for DynamoDBOutputEndpoint {
    fn max_buffer_size_bytes(&self) -> usize {
        self.config.max_buffer_size_bytes
    }

    fn batch_start(&mut self, _step: Step, _batch_type: OutputBatchType) {
        self.records_written.store(0, Ordering::Relaxed);
        self.num_bytes = 0;
        self.num_rows = 0;
        if let Err(error) = self
            .broadcast_and_collect(BroadcastCommand::BatchStart)
            .map(|_| ())
        {
            if let Some(controller) = self.controller.upgrade() {
                controller.output_transport_error(
                    self.endpoint_id,
                    &self.endpoint_name,
                    true,
                    error,
                    None,
                );
            }
        }
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
        const THROUGHPUT_LOG_INTERVAL: Duration = Duration::from_secs(60);

        match self.broadcast_and_collect(BroadcastCommand::BatchEnd) {
            Ok(batch_retries) => {
                let num_bytes = std::mem::take(&mut self.num_bytes);
                let num_rows = std::mem::take(&mut self.num_rows);

                if let Some(controller) = self.controller.upgrade() {
                    controller
                        .status
                        .output_buffer(self.endpoint_id, num_bytes, num_rows);
                }
                self.metrics.record_committed_batch(
                    num_rows as u64,
                    num_bytes as u64,
                    batch_retries,
                );

                self.rows_since_last_log += num_rows as u64;
                self.bytes_since_last_log += num_bytes as u64;
                self.retries_since_last_log += batch_retries;
                self.batches_since_last_log += 1;

                let since_last = self.last_throughput_log.elapsed();
                if since_last >= THROUGHPUT_LOG_INTERVAL {
                    let secs = since_last.as_secs_f64();
                    let avg_retries = if self.batches_since_last_log > 0 {
                        self.retries_since_last_log as f64 / self.batches_since_last_log as f64
                    } else {
                        0.0
                    };
                    info!(
                        "dynamodb throughput: {:.0} rows/sec, {:.0} bytes/sec, \
                         avg {:.2} retries/batch ({} batches over {:.1}s)",
                        self.rows_since_last_log as f64 / secs,
                        self.bytes_since_last_log as f64 / secs,
                        avg_retries,
                        self.batches_since_last_log,
                        secs,
                    );
                    self.rows_since_last_log = 0;
                    self.bytes_since_last_log = 0;
                    self.retries_since_last_log = 0;
                    self.batches_since_last_log = 0;
                    self.last_throughput_log = Instant::now();
                }
            }
            Err(error) => {
                if let Some(controller) = self.controller.upgrade() {
                    controller.output_transport_error(
                        self.endpoint_id,
                        &self.endpoint_name,
                        true,
                        error,
                        None,
                    );
                }
            }
        }

        self.num_bytes = 0;
        self.num_rows = 0;
        self.records_written.store(0, Ordering::Relaxed);
    }
}

impl Encoder for DynamoDBOutputEndpoint {
    fn consumer(&mut self) -> &mut dyn OutputConsumer {
        self
    }

    fn encode(&mut self, batch: Arc<dyn SerBatchReader>) -> AnyResult<()> {
        let _span = info_span!(
            "dynamodb_output",
            endpoint = &*self.endpoint_name,
            table = &*self.config.table,
        )
        .entered();

        let num_workers = self.handles.len();
        let mut bounds = batch.keys_factory().default_box();
        batch.partition_keys(num_workers, &mut *bounds);

        let mut workers_dispatched = 0;
        for i in 0..=bounds.len() {
            let Some(cursor_builder) =
                SplitCursorBuilder::from_bounds(batch.clone(), &*bounds, i, RecordFormat::DynamoDB)
            else {
                continue;
            };

            assert!(
                workers_dispatched <= num_workers,
                "DynamoDB output connector split batch into more partitions than worker threads"
            );

            self.handles[workers_dispatched]
                .cmd_tx
                .send(WorkerCommand::Encode(cursor_builder))
                .map_err(|_| anyhow!("worker thread disconnected"))?;
            workers_dispatched += 1;
        }

        let mut errors = Vec::new();
        for i in 0..workers_dispatched {
            match self.handles[i].result_rx.recv() {
                Ok(WorkerResult::Ok {
                    num_bytes,
                    num_rows,
                    ..
                }) => {
                    self.num_bytes += num_bytes;
                    self.num_rows += num_rows;
                }
                Ok(WorkerResult::Err(e)) => errors.push(e),
                Err(_) => errors.push(anyhow!("worker thread disconnected")),
            }
        }

        if !errors.is_empty() {
            let msg = errors
                .iter()
                .map(|e| format!("{e:#}"))
                .collect::<Vec<_>>()
                .join("; ");
            bail!("{} DynamoDB worker(s) failed: {msg}", errors.len());
        }

        Ok(())
    }
}

impl OutputEndpoint for DynamoDBOutputEndpoint {
    fn connect(&mut self, _: AsyncErrorCallback) -> AnyResult<()> {
        todo!()
    }

    fn max_buffer_size_bytes(&self) -> usize {
        self.config.max_buffer_size_bytes
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

    fn is_fault_tolerant(&self) -> bool {
        false
    }
}
