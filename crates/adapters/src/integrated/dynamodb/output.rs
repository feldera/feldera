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
use super::metrics::DynamoDBOutputMetrics;

/// DynamoDB rejects any item larger than 400 KB (name plus value bytes). A
/// record at or below this size may still be written; one above it never can,
/// so the connector drops it during encoding. The value is compared against
/// [`helpers::item_size`], which is an approximation of DynamoDB's own size
/// accounting.
const MAX_ITEM_SIZE_BYTES: usize = 400 * 1024;

/// Minimum interval between throughput log lines emitted at batch end. The
/// connector accumulates rows/bytes/retries across batches and logs an average
/// at most once per interval to keep the log readable under high batch rates.
const THROUGHPUT_LOG_INTERVAL: Duration = Duration::from_secs(60);

// The connector runs a pool of worker threads, each owning a DynamoDB client
// and write buffer for a disjoint key range. The endpoint drives them over
// channels using the message types below: it hands each worker the key range to
// encode and broadcasts batch lifecycle events, and each worker reports back the
// outcome.

/// Lifecycle event broadcast to every worker (as opposed to the per-worker
/// [`WorkerCommand::Encode`]). `BatchStart`/`BatchEnd` bracket a step so workers
/// can reset state and flush; `Shutdown` tells them to drain and exit.
#[derive(Clone, Copy)]
enum BroadcastCommand {
    BatchStart,
    BatchEnd,
    Shutdown,
}

/// A unit of work sent to a single worker thread: either a broadcast lifecycle
/// event, or an `Encode` request carrying the key-range split that worker should
/// encode and write.
enum WorkerCommand {
    Broadcast(BroadcastCommand),
    Encode(SplitCursorBuilder),
}

/// A worker's reply to a [`WorkerCommand`]. `num_retries` is propagated so the
/// endpoint can aggregate it into connector metrics; `Err` surfaces a write
/// failure back to the controller.
enum WorkerResult {
    Ok { num_retries: u64 },
    Err(anyhow::Error),
}

pub(crate) struct DynamoDBWorker {
    endpoint_id: EndpointId,
    endpoint_name: String,
    controller: Weak<ControllerInner>,
    table: String,
    write_mode: DynamoDBWriteMode,
    allow_cross_step_write_overlap: bool,
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
    /// Records successfully written to DynamoDB in the current batch.
    pub(crate) records_written: Arc<AtomicU64>,
    /// Bytes successfully written to DynamoDB in the current batch.
    pub(crate) bytes_written: Arc<AtomicU64>,
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
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
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
        bytes_written: Arc<AtomicU64>,
        metrics: Arc<DynamoDBOutputMetrics>,
    ) -> Self {
        Self {
            endpoint_id,
            endpoint_name: endpoint_name.to_string(),
            controller,
            table: config.table.clone(),
            write_mode: config.write_mode,
            allow_cross_step_write_overlap: config.allow_cross_step_write_overlap,
            batch_size: config.effective_batch_size(),
            max_retries: config.max_retries,
            client,
            key_schema: key_schema.clone(),
            value_schema: value_schema.clone(),
            pending: Vec::new(),
            pending_bytes: 0,
            max_buffer_size_bytes: config.max_buffer_size_bytes,
            records_written,
            bytes_written,
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

    /// Drains completed writes without waiting for in-flight writes, collecting retries and errors.
    fn drain_completed_writes(&mut self) -> (u64, Vec<anyhow::Error>) {
        let mut total_retries = std::mem::take(&mut self.completed_retries);
        let mut errors = Vec::new();

        while let Some(result) = self.pending_writes.try_join_next() {
            match result {
                Ok(Ok(retries)) => total_retries += retries,
                Ok(Err(e)) => errors.push(e),
                Err(e) => errors.push(anyhow!("write task panicked: {e}")),
            }
        }

        (total_retries, errors)
    }

    /// Drains all `pending_writes`, collecting retries and errors.
    fn drain_pending_writes(&mut self) -> (u64, Vec<anyhow::Error>) {
        let (mut total_retries, mut errors) = self.drain_completed_writes();
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

    pub(crate) fn batch_end_inner(&mut self) -> AnyResult<u64> {
        let flush_result = self.flush();

        let (total_retries, mut errors) = if self.allow_cross_step_write_overlap {
            self.drain_completed_writes()
        } else {
            self.drain_pending_writes()
        };

        if let Err(e) = flush_result {
            errors.push(e);
        }

        if !errors.is_empty() {
            let msg = errors
                .iter()
                .map(|e| format!("{e:#}"))
                .collect::<Vec<_>>()
                .join("; ");
            bail!("DynamoDB writes failed: {msg}");
        }

        Ok(total_retries)
    }

    fn push_request(&mut self, request: WriteRequest, bytes: usize) -> AnyResult<()> {
        self.pending.push(request);
        self.pending_bytes += bytes;

        if self.pending.len() >= self.batch_size || self.pending_bytes >= self.max_buffer_size_bytes
        {
            self.flush()
        } else {
            Ok(())
        }
    }

    pub(crate) fn flush(&mut self) -> AnyResult<()> {
        if self.pending.is_empty() {
            return Ok(());
        }

        // Block until a write slot is free, then drain any completed tasks before spawning.
        let wait_start = Instant::now();
        let permit = TOKIO
            .block_on(Arc::clone(&self.write_semaphore).acquire_owned())
            .map_err(|_| anyhow!("DynamoDB write semaphore closed"))?;
        self.metrics.record_semaphore_wait(wait_start.elapsed());
        let (retries, errors) = self.drain_completed_writes();
        self.completed_retries += retries;
        if !errors.is_empty() {
            let msg = errors
                .iter()
                .map(|e| format!("{e:#}"))
                .collect::<Vec<_>>()
                .join("; ");
            bail!("DynamoDB writes failed: {msg}");
        }

        let requests = std::mem::replace(&mut self.pending, Vec::with_capacity(self.batch_size));
        let rows = requests.len();
        let bytes = std::mem::take(&mut self.pending_bytes);

        let client = self.client.clone();
        let endpoint_name = self.endpoint_name.clone();
        let table = self.table.clone();
        let write_mode = self.write_mode;
        let max_retries: Option<usize> = self.max_retries.map(|n| n as usize);
        let allow_cross_step_write_overlap = self.allow_cross_step_write_overlap;
        let endpoint_id = self.endpoint_id;
        let task_controller = self.controller.clone();
        self.metrics.record_write_chunk();
        let task_metrics = self.metrics.clone();
        let task_records_written = self.records_written.clone();
        let task_bytes_written = self.bytes_written.clone();
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
                            &task_metrics,
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
                            &task_metrics,
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
                // Count rows and bytes as written only once the chunk lands in
                // DynamoDB; a failed chunk drops its items rather than recording
                // progress.
                match result {
                    Ok(retries) => {
                        task_records_written.fetch_add(rows as u64, Ordering::Relaxed);
                        task_bytes_written.fetch_add(bytes as u64, Ordering::Relaxed);
                        task_metrics.record_retries(retries);
                        if allow_cross_step_write_overlap
                            && let Some(controller) = task_controller.upgrade()
                        {
                            controller.status.output_buffer(endpoint_id, bytes, rows);
                        }
                        Ok(retries)
                    }
                    Err(error) => {
                        if allow_cross_step_write_overlap {
                            if let Some(controller) = task_controller.upgrade() {
                                controller.output_transport_error(
                                    endpoint_id,
                                    &endpoint_name,
                                    false,
                                    error,
                                    Some("dynamodb_async_write"),
                                );
                            } else {
                                warn!(
                                    endpoint = %endpoint_name,
                                    "dynamodb: write failed after endpoint shutdown"
                                );
                            }
                            Ok(0)
                        } else {
                            Err(error)
                        }
                    }
                }
            },
            TOKIO.handle(),
        );

        Ok(())
    }

    pub(crate) fn encode_cursor(&mut self, cursor: &mut dyn SerCursor) -> AnyResult<()> {
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

                if bytes > MAX_ITEM_SIZE_BYTES {
                    // The item exceeds DynamoDB's 400 KB limit and will be
                    // rejected on every attempt. That rejection fails the whole
                    // `BatchWriteItem` chunk / `TransactWriteItems` transaction
                    // it would share with valid items, so drop it here: the rest
                    // of the batch still lands, and we waste no retries on a
                    // write that can never succeed.
                    self.metrics.record_oversized_item_dropped();
                    let error = anyhow!(
                        "dropping record that exceeds DynamoDB's {MAX_ITEM_SIZE_BYTES}-byte \
                         item-size limit (estimated {bytes} bytes)"
                    );
                    if let Some(controller) = self.controller.upgrade() {
                        controller.output_transport_error(
                            self.endpoint_id,
                            &self.endpoint_name,
                            false,
                            error,
                            Some("dynamodb_item_too_large"),
                        );
                    } else {
                        warn!(endpoint = %self.endpoint_name, "dynamodb: {error:#}");
                    }
                } else {
                    self.push_request(request, bytes)?;
                }
            }

            cursor.step_key();
        }

        Ok(())
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
                    let _ = result_tx.send(WorkerResult::Ok { num_retries: 0 });
                }
                WorkerCommand::Encode(cursor_builder) => {
                    let mut cursor = cursor_builder.build();
                    match self.encode_cursor(&mut cursor) {
                        Ok(()) => {
                            let _ = result_tx.send(WorkerResult::Ok { num_retries: 0 });
                        }
                        Err(e) => {
                            let _ = result_tx.send(WorkerResult::Err(e));
                        }
                    }
                }
                WorkerCommand::Broadcast(BroadcastCommand::BatchEnd) => {
                    match self.batch_end_inner() {
                        Ok(num_retries) => {
                            let _ = result_tx.send(WorkerResult::Ok { num_retries });
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
    pub(crate) bytes_written: Arc<AtomicU64>,
    // Rows, bytes, retries, and batches written since the last throughput log,
    // used to print average throughput once per `THROUGHPUT_LOG_INTERVAL`.
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
        Self::new_with_metrics(
            endpoint_id,
            endpoint_name,
            config,
            key_schema,
            value_schema,
            controller,
            is_index,
            Arc::new(DynamoDBOutputMetrics::default()),
        )
    }

    /// Constructs the endpoint using a caller-supplied metrics handle, shared with
    /// every worker thread. [`new`](Self::new) creates a fresh handle; tests pass
    /// one they retain so they can inspect connector metrics after driving the
    /// endpoint.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_with_metrics(
        endpoint_id: EndpointId,
        endpoint_name: &str,
        config: &DynamoDBWriterConfig,
        key_schema: &Option<Relation>,
        value_schema: &Relation,
        controller: Weak<ControllerInner>,
        is_index: bool,
        metrics: Arc<DynamoDBOutputMetrics>,
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
        let bytes_written = Arc::new(AtomicU64::new(0));
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
                bytes_written.clone(),
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
            bytes_written,
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
                    num_retries: retries,
                }) => {
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
        self.bytes_written.store(0, Ordering::Relaxed);
        if let Err(error) = self
            .broadcast_and_collect(BroadcastCommand::BatchStart)
            .map(|_| ())
            && let Some(controller) = self.controller.upgrade()
        {
            controller.output_transport_error(
                self.endpoint_id,
                &self.endpoint_name,
                true,
                error,
                None,
            );
        }
    }

    fn push_buffer(&mut self, _buffer: &[u8], _num_records: usize) {
        unreachable!("dynamodb integrated endpoint encodes batches via encode(), not push_buffer")
    }

    fn push_key(
        &mut self,
        _key: Option<&[u8]>,
        _val: Option<&[u8]>,
        _headers: &[(&str, Option<&[u8]>)],
        _num_records: usize,
    ) {
        unreachable!("dynamodb integrated endpoint encodes batches via encode(), not push_key")
    }

    fn batch_end(&mut self) {
        match self.broadcast_and_collect(BroadcastCommand::BatchEnd) {
            Ok(batch_retries) => {
                let num_rows = self.records_written.load(Ordering::Relaxed);
                let num_bytes = self.bytes_written.load(Ordering::Relaxed);

                if !self.config.allow_cross_step_write_overlap
                    && let Some(controller) = self.controller.upgrade()
                {
                    controller.status.output_buffer(
                        self.endpoint_id,
                        num_bytes as usize,
                        num_rows as usize,
                    );
                }
                self.rows_since_last_log += num_rows;
                self.bytes_since_last_log += num_bytes;
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
                    // A write failure here means the chunk exhausted its retries
                    // and the affected items were dropped (counted in
                    // `failed_items`).
                    controller.output_transport_error(
                        self.endpoint_id,
                        &self.endpoint_name,
                        false,
                        error,
                        None,
                    );
                }
            }
        }

        self.records_written.store(0, Ordering::Relaxed);
        self.bytes_written.store(0, Ordering::Relaxed);
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
                workers_dispatched < num_workers,
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
                Ok(WorkerResult::Ok { .. }) => {}
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
        unreachable!("DynamoDB is an integrated endpoint; connect() is never called")
    }

    fn max_buffer_size_bytes(&self) -> usize {
        self.config.max_buffer_size_bytes
    }

    fn push_buffer(&mut self, _buffer: &[u8]) -> AnyResult<()> {
        unreachable!("dynamodb integrated endpoint does not use OutputEndpoint::push_buffer")
    }

    fn push_key(
        &mut self,
        _key: Option<&[u8]>,
        _val: Option<&[u8]>,
        _headers: &[(&str, Option<&[u8]>)],
    ) -> AnyResult<()> {
        unreachable!("dynamodb integrated endpoint does not use OutputEndpoint::push_key")
    }

    fn is_fault_tolerant(&self) -> bool {
        false
    }
}
