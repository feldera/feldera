use crate::transport::{
    InputEndpoint, InputQueue, InputReaderCommand, IntegratedInputEndpoint, NonFtInputReaderCommand,
};
use crate::{ControllerError, InputConsumer, InputReader, PipelineState, RecordFormat};
use anyhow::{Result as AnyResult, anyhow};
use chrono::Utc;
use dbsp::circuit::tokio::TOKIO;
use etl::config::{
    BatchConfig, InvalidatedSlotBehavior, MemoryBackpressureConfig, PgConnectionConfig,
    PipelineConfig, TableSyncCopyConfig, TcpKeepaliveConfig,
};
use etl::data::{ArrayCell, Cell, OldTableRow, TableRow, UpdatedTableRow};
use etl::destination::{
    Destination, DestinationWriteStatus, DropTableForCopyResult, TableCopyBatchId,
    WriteEventsDurability, WriteEventsResult, WriteTableRowsResult,
};
use etl::error::{ErrorKind, EtlResult};
use etl::etl_error;
use etl::event::Event;
use etl::pipeline::{Pipeline, ShutdownTx};
use etl::schema::{ReplicatedTableSchema, TableId};
use etl::store::{
    PostgresStore, SchemaStore, StateStore, TableRetryPolicy, TableState, TableStateType,
};
use feldera_adapterlib::catalog::{DeCollectionStream, InputCollectionHandle};
use feldera_adapterlib::format::ParseError;
use feldera_adapterlib::transport::{Resume, Watermark};
use feldera_types::config::FtModel;
use feldera_types::coordination::Completion;
use feldera_types::format::json::JsonFlavor;
use feldera_types::transport::postgres::{PostgresCdcReaderConfig, PostgresTlsConfig};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use tokio::select;
use tokio::sync::mpsc;
use tokio::sync::watch::{Receiver, Sender, channel};
use tracing::{debug, error, info, warn};
use url::Url;
use xxhash_rust::xxh3::xxh3_64;

use super::tls::make_etl_tls_config;

/// Deferred async result senders waiting for step completion.
type DeferredSenders = Vec<WriteEventsResult>;

/// Error the destination returns when Feldera terminates the connector while
/// etl is waiting to hand over a batch. etl persists it as a table error, so
/// the next start rolls it back (see [`reconcile_table_state`]).
const TERMINATED_BEFORE_BATCH: &str =
    "Postgres CDC input connector terminated before accepting batch";

/// Error etl records when the connector drops a write's result without
/// answering it, which happens to every deferred acknowledgment when Feldera
/// stops the connector. etl's table-sync worker persists it as a `ManualRetry`
/// table error, which etl never retries on its own. The text is etl's, from
/// `destination/async_result.rs`, where etl's own unit test pins it.
const RESULT_DROPPED_ON_SHUTDOWN: &str = "Async result channel closed before sending";

/// Whether an etl table error was caused by Feldera stopping the connector.
/// Such writes were never acknowledged, so replaying them is safe.
fn is_shutdown_error(reason: &str) -> bool {
    reason.contains(TERMINATED_BEFORE_BATCH) || reason.contains(RESULT_DROPPED_ON_SHUTDOWN)
}

/// Integrated input connector that reads from Postgres via logical replication (CDC).
pub struct PostgresCdcInputEndpoint {
    inner: Arc<PostgresCdcInputInner>,
}

impl PostgresCdcInputEndpoint {
    pub fn new(
        endpoint_name: &str,
        config: &PostgresCdcReaderConfig,
        consumer: Box<dyn InputConsumer>,
    ) -> Result<Self, ControllerError> {
        config.validate().map_err(|e| {
            ControllerError::invalid_transport_configuration(endpoint_name, &e.to_string())
        })?;

        Ok(Self {
            inner: Arc::new(PostgresCdcInputInner::new(
                endpoint_name,
                config.clone(),
                consumer,
            )),
        })
    }
}

impl InputEndpoint for PostgresCdcInputEndpoint {
    fn fault_tolerance(&self) -> Option<FtModel> {
        Some(FtModel::AtLeastOnce)
    }
}

impl IntegratedInputEndpoint for PostgresCdcInputEndpoint {
    fn open(
        self: Box<Self>,
        input_handle: &InputCollectionHandle,
        resume_info: Option<serde_json::Value>,
    ) -> AnyResult<Box<dyn InputReader>> {
        Ok(Box::new(PostgresCdcInputReader::new(
            &self.inner,
            input_handle,
            resume_info,
        )?))
    }
}

struct PostgresCdcInputReader {
    sender: Sender<PipelineState>,
    inner: Arc<PostgresCdcInputInner>,
}

impl PostgresCdcInputReader {
    fn new(
        endpoint: &Arc<PostgresCdcInputInner>,
        input_handle: &InputCollectionHandle,
        resume_info: Option<serde_json::Value>,
    ) -> AnyResult<Self> {
        // The connector reports a barrier for every step until a complete copy
        // of the table is in the circuit, so no checkpoint and no suspended
        // state can hold a partial copy. Resuming from one therefore needs no
        // redo. Fault tolerance without any checkpoint is the case #6121
        // describes: the circuit starts empty while etl may have recorded the
        // table sync as complete, so the copy has to be read again. A restart
        // without fault tolerance keeps resuming from the replication slot
        // alone.
        let redo_snapshot = resume_info.is_none() && endpoint.strict;
        let (sender, receiver) = channel(PipelineState::Paused);
        let endpoint_clone = endpoint.clone();

        let (init_status_sender, init_status_receiver) =
            tokio::sync::oneshot::channel::<Result<(), ControllerError>>();

        let input_stream = input_handle
            .handle
            .configure_deserializer(RecordFormat::Json(JsonFlavor::Datagen))?;

        // Non-nullable columns of the Feldera table, in canonical form.
        // Each of them must exist in the PostgreSQL table.
        let feldera_required_columns: Vec<String> = input_handle
            .schema
            .fields
            .iter()
            .filter(|f| !f.columntype.nullable)
            .map(|f| f.name.name())
            .collect();

        thread::Builder::new()
            .name("postgres-cdc-input-tokio-wrapper".to_string())
            .spawn(move || {
                TOKIO.block_on(async {
                    let _ = endpoint_clone
                        .worker_task(
                            input_stream,
                            feldera_required_columns,
                            receiver,
                            init_status_sender,
                            redo_snapshot,
                        )
                        .await;
                })
            })
            .expect("failed to create Postgres CDC input connector thread");

        init_status_receiver.blocking_recv().map_err(|_| {
            ControllerError::input_transport_error(
                &endpoint.endpoint_name,
                true,
                anyhow!("worker thread terminated unexpectedly during initialization"),
            )
        })??;

        Ok(Self {
            sender,
            inner: endpoint.clone(),
        })
    }
}

impl InputReader for PostgresCdcInputReader {
    fn as_any(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }

    fn request(&self, command: InputReaderCommand) {
        if matches!(command, InputReaderCommand::Replay { .. }) {
            panic!(
                "replay command is not supported by PostgresCdcInputReader; this is a bug, please report it to Feldera developers: https://github.com/feldera/feldera/issues/"
            );
        }

        match command.as_nonft().unwrap() {
            NonFtInputReaderCommand::Queue => {
                // Flush queue to circuit, collecting timestamps for watermarks.
                let (buffer_size, _hasher, flushed) = self.inner.queue.flush_with_aux();

                let watermarks: Vec<Watermark> = flushed
                    .iter()
                    .map(|(ts, _)| Watermark::new(*ts, None))
                    .collect();
                // The flush is what puts the rows in the circuit, so it is
                // also what answers a held copy barrier.
                let snapshot_buffers = count_snapshot_buffers(&flushed);
                let barrier_held = self.inner.copy.note_buffers_flushed(snapshot_buffers);

                let copy_open = self.inner.copy.copy_open();
                // A pending checkpoint stops the controller from stepping on
                // the checkpoint timer, so with etl's copy barrier held and
                // only queued buffers left, ask for the step that clears it.
                // Queued buffers ask for a step of their own, so this is a
                // backstop.
                if copy_open && barrier_held {
                    self.inner.consumer.request_step();
                }
                let resume = resume_for(copy_open, self.inner.pipeline_id);

                // Report data to controller with resume metadata (must be
                // called exactly once per Queue command).
                self.inner
                    .consumer
                    .extended(buffer_size, Some(resume), watermarks);

                // Take any deferred senders that write_events stored.
                let senders: DeferredSenders =
                    std::mem::take(&mut *self.inner.pending_senders.lock().unwrap());

                if !senders.is_empty() {
                    if let Some(tx) = self.inner.completion_task_tx.as_ref() {
                        // Snapshot total_completed_steps AFTER flush.  The
                        // data will land in the next step (>completed), so
                        // this value is the correct lower bound for both
                        // fast mode (fire when completed_steps > this) and
                        // strict mode (fire when checkpointed_steps > this,
                        // per the `total_checkpointed_steps >= n` semantics).
                        let step_at_flush = self
                            .inner
                            .step_completion_rx
                            .as_ref()
                            .map(|rx| rx.borrow().total_completed_steps)
                            .unwrap_or(0);
                        let _ = tx.send((step_at_flush, senders));
                    } else {
                        // No completion tracking — fire immediately.
                        for sender in senders {
                            sender.send(Ok(DestinationWriteStatus::Durable));
                        }
                    }
                }
            }
            NonFtInputReaderCommand::Transition(state) => drop(self.sender.send_replace(state)),
        }
    }

    fn is_closed(&self) -> bool {
        self.inner.queue.is_empty() && self.sender.is_closed()
    }
}

impl Drop for PostgresCdcInputReader {
    fn drop(&mut self) {
        self.disconnect();
    }
}

/// The checkpoint barrier the connector holds while a copy of the source table
/// is on its way into the circuit.
///
/// etl issues a terminal empty `write_table_rows` once every copy worker of an
/// attempt has finished, and records the copy as finished only if that call
/// answers `Durable`. The connector therefore answers each batch of copy rows
/// with `Accepted` and holds the terminal call until the buffers it queued have
/// been flushed into the circuit. Until that happens the reader reports
/// [`Resume::Barrier`], so neither a checkpoint nor a suspended state can hold
/// a partial copy (#6121).
struct CopyBarrier {
    state: Mutex<CopyBarrierState>,
    /// Snapshot buffers queued since the connector started, never reset. etl
    /// records no state transition between the start and the end of a copy, so
    /// for a copy under way this and `flushed` are the only signs of progress
    /// [`TableProgress`] has.
    queued: AtomicUsize,
    /// Snapshot buffers flushed into the circuit since the connector started,
    /// never reset. Once etl has handed over its last batch and waits on the
    /// terminal barrier, `queued` stands still and this is what still moves.
    flushed: AtomicUsize,
}

/// The barrier proper, which the destination and the reader move in step.
struct CopyBarrierState {
    /// A copy of the source table is under way, or its rows have not all
    /// reached the circuit.
    copy_open: bool,
    /// Snapshot buffers queued and not yet flushed into the circuit. Streamed
    /// events share the queue, so "the queue is empty" is the wrong test.
    pending: usize,
    /// etl's terminal copy barrier, held until `pending` reaches zero. Calling
    /// it answers etl with `Durable`.
    terminal: Option<Box<dyn FnOnce() + Send>>,
}

impl CopyBarrier {
    /// A barrier that is up. What etl has stored about the source table decides
    /// whether it stays up, and the connector reads that only once it has
    /// connected; see [`CopyBarrier::set_copy_open`].
    fn new() -> Self {
        Self {
            state: Mutex::new(CopyBarrierState {
                copy_open: true,
                pending: 0,
                terminal: None,
            }),
            queued: AtomicUsize::new(0),
            flushed: AtomicUsize::new(0),
        }
    }

    /// Whether a checkpoint would hold an incomplete copy, which is what the
    /// reader reports a barrier for.
    fn copy_open(&self) -> bool {
        self.state.lock().unwrap().copy_open
    }

    /// Record what etl's stored state says about the source table, before etl
    /// runs. A table whose sync etl has completed needs no barrier: etl copies
    /// it no more, so no terminal barrier would ever arrive to lower one.
    fn set_copy_open(&self, open: bool) {
        self.state.lock().unwrap().copy_open = open;
    }

    /// Note a copy of the source table, and say whether the barrier went back
    /// up. etl hands over no row of an attempt before it announces the attempt
    /// through [`Destination::drop_table_for_copy`] or the first batch of it.
    fn note_copy_batch(&self) -> bool {
        !std::mem::replace(&mut self.state.lock().unwrap().copy_open, true)
    }

    /// Account for one snapshot buffer, before it is queued, so the reader
    /// never counts a flush it has not seen queued.
    fn note_buffer_queued(&self) {
        self.queued.fetch_add(1, Ordering::AcqRel);
        self.state.lock().unwrap().pending += 1;
    }

    /// Hold etl's terminal copy barrier until every queued snapshot buffer has
    /// reached the circuit, answering it at once when none is left, as for an
    /// empty table. `confirm` answers etl with `Durable`.
    ///
    /// Reports whether the barrier came down here, away from the reader's step.
    /// The reader reports the lowered barrier only on its next step, and
    /// nothing else makes that step happen, so the caller has to ask for it.
    fn hold_terminal(&self, confirm: Box<dyn FnOnce() + Send>) -> bool {
        let mut state = self.state.lock().unwrap();
        debug_assert!(
            state.terminal.is_none(),
            "etl waits for one copy barrier before it issues the next"
        );
        if state.pending > 0 {
            state.terminal = Some(confirm);
            return false;
        }
        state.copy_open = false;
        drop(state);
        confirm();
        true
    }

    /// Account for snapshot buffers the reader has flushed into the circuit,
    /// answering a held terminal barrier once the last of them lands, and say
    /// whether a terminal barrier is still held.
    ///
    /// Buffers of an attempt etl abandoned may still sit in the queue when the
    /// next attempt's barrier arrives. Both counts cover every attempt, so
    /// waiting for `pending` to reach zero waits for more than the current
    /// attempt, never for less.
    fn note_buffers_flushed(&self, buffers: usize) -> bool {
        self.flushed.fetch_add(buffers, Ordering::AcqRel);
        let mut state = self.state.lock().unwrap();
        state.pending = state.pending.saturating_sub(buffers);
        if state.pending > 0 {
            return state.terminal.is_some();
        }
        let Some(confirm) = state.terminal.take() else {
            return false;
        };
        state.copy_open = false;
        drop(state);
        confirm();
        false
    }

    /// Snapshot buffers queued so far, which the monitor watches as the sign
    /// that a copy in progress is making progress.
    fn snapshot_buffers_queued(&self) -> usize {
        self.queued.load(Ordering::Acquire)
    }

    /// Snapshot buffers flushed into the circuit so far, the sign of progress
    /// a copy draining into a slow circuit gives after etl has handed over its
    /// last batch.
    fn snapshot_buffers_flushed(&self) -> usize {
        self.flushed.load(Ordering::Acquire)
    }
}

struct PostgresCdcInputInner {
    endpoint_name: String,
    config: PostgresCdcReaderConfig,
    consumer: Box<dyn InputConsumer>,
    queue: Arc<InputQueue<QueueAux>>,
    /// Deterministic pipeline ID used for replication slot naming and resume.
    pipeline_id: u64,
    /// Deferred async result senders from `write_events`, waiting to be paired
    /// with a step number during the next `Queue` command.
    pending_senders: Arc<Mutex<DeferredSenders>>,
    /// Watch receiver for step completion — used to snapshot `step_at_flush`
    /// in the Queue handler.  Always tracks `total_completed_steps`.
    step_completion_rx: Option<tokio::sync::watch::Receiver<Completion>>,
    /// Watcher source for the background task.  Taken once by `worker_task_inner`.
    /// `Strict` when fault tolerance is enabled (gates slot on checkpoint);
    /// `Fast` otherwise (gates slot on step completion).
    watcher_rx: Mutex<Option<WatcherReceiver>>,
    /// Sender for passing (step_at_flush, senders) to the background task.
    /// Created once at construction time if completion tracking is available.
    completion_task_tx: Option<mpsc::UnboundedSender<(u64, DeferredSenders)>>,
    /// Receiver half, taken once by worker_task_inner to spawn the background task.
    completion_task_rx: Mutex<Option<mpsc::UnboundedReceiver<(u64, DeferredSenders)>>>,
    /// etl shutdown handle for the currently running pipeline.
    /// Used to stop etl workers when Feldera terminates the connector.
    etl_shutdown_tx: Mutex<Option<ShutdownTx>>,
    /// The checkpoint barrier of the initial copy, which the destination, this
    /// reader and [`TableErrorMonitor`] share; see [`CopyBarrier`].
    copy: Arc<CopyBarrier>,
    /// Fault tolerance is enabled: the slot advances only past checkpoints.
    strict: bool,
}

impl PostgresCdcInputInner {
    fn new(
        endpoint_name: &str,
        config: PostgresCdcReaderConfig,
        consumer: Box<dyn InputConsumer>,
    ) -> Self {
        let queue = Arc::new(InputQueue::new(consumer.clone()));
        let step_completion_rx = consumer.completion_watcher();

        let pipeline_id = pipeline_id(&config.uri, &config.publication, &config.source_table);

        // Use strict mode (gate slot on checkpoint) when fault tolerance is enabled;
        // fast mode (gate slot on step completion) otherwise.
        let watcher_rx = match consumer.checkpoint_watcher() {
            Some(rx) => Some(WatcherReceiver::Strict(rx)),
            None => step_completion_rx.clone().map(WatcherReceiver::Fast),
        };
        let strict = matches!(watcher_rx, Some(WatcherReceiver::Strict(_)));

        let (completion_task_tx, completion_task_rx) = if watcher_rx.is_some() {
            let (tx, rx) = mpsc::unbounded_channel();
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };

        Self {
            endpoint_name: endpoint_name.to_string(),
            config,
            consumer,
            queue,
            pipeline_id,
            pending_senders: Arc::new(Mutex::new(Vec::new())),
            step_completion_rx,
            watcher_rx: Mutex::new(watcher_rx),
            completion_task_tx,
            completion_task_rx: Mutex::new(completion_task_rx),
            etl_shutdown_tx: Mutex::new(None),
            copy: Arc::new(CopyBarrier::new()),
            strict,
        }
    }

    async fn worker_task(
        self: Arc<Self>,
        input_stream: Box<dyn DeCollectionStream>,
        feldera_required_columns: Vec<String>,
        receiver: Receiver<PipelineState>,
        init_status_sender: tokio::sync::oneshot::Sender<Result<(), ControllerError>>,
        redo_snapshot: bool,
    ) {
        self.clone()
            .worker_task_inner(
                input_stream,
                feldera_required_columns,
                receiver,
                init_status_sender,
                redo_snapshot,
            )
            .await;
        debug!(
            "postgres_cdc {}: worker task terminated",
            &self.endpoint_name
        );
    }

    async fn worker_task_inner(
        self: Arc<Self>,
        input_stream: Box<dyn DeCollectionStream>,
        feldera_required_columns: Vec<String>,
        receiver: Receiver<PipelineState>,
        init_status_sender: tokio::sync::oneshot::Sender<Result<(), ControllerError>>,
        redo_snapshot: bool,
    ) {
        let pg_conn = match parse_pg_uri(&self.config.uri, &self.config.tls, &self.endpoint_name) {
            Ok(conn) => conn,
            Err(e) => {
                let _ =
                    init_status_sender.send(Err(ControllerError::invalid_transport_configuration(
                        &self.endpoint_name,
                        &format!("failed to parse Postgres URI: {e}"),
                    )));
                return;
            }
        };

        let pipeline_config = PipelineConfig {
            id: self.pipeline_id,
            publication_name: self.config.publication.clone(),
            pg_connection: pg_conn.clone(),
            // etl stores its replication state in the source database itself, so
            // the state store reuses the source connection.
            store_pg_connection: None,
            // etl names its replication slots after the pipeline id alone, and
            // failover slots need PostgreSQL 17 or later, so the connector
            // keeps the default slot configuration.
            replication_slot: Default::default(),
            batch: BatchConfig::default(),
            table_error_retry_delay_ms: PipelineConfig::DEFAULT_TABLE_ERROR_RETRY_DELAY_MS,
            table_error_retry_max_attempts: PipelineConfig::DEFAULT_TABLE_ERROR_RETRY_MAX_ATTEMPTS,
            max_table_sync_workers: PipelineConfig::DEFAULT_MAX_TABLE_SYNC_WORKERS,
            max_copy_connections_per_table: PipelineConfig::DEFAULT_MAX_COPY_CONNECTIONS_PER_TABLE,
            memory_refresh_interval_ms: PipelineConfig::DEFAULT_MEMORY_REFRESH_INTERVAL_MS,
            memory_backpressure: Some(MemoryBackpressureConfig::default()),
            table_sync_copy: TableSyncCopyConfig::IncludeAllTables,
            table_sync_monitor_refresh_interval_ms:
                PipelineConfig::DEFAULT_TABLE_SYNC_MONITOR_REFRESH_INTERVAL_MS,
            invalidated_slot_behavior: InvalidatedSlotBehavior::default(),
            // etl reads the source schema through helper functions that its own
            // source migrations install, so the initial copy needs them.
            run_source_migrations: true,
        };

        // Use PostgresStore to persist table replication phases across restarts.
        // This allows etl to resume from the replication slot position instead of
        // re-snapshotting the entire table on restart.
        let store = match PostgresStore::new(self.pipeline_id, pg_conn).await {
            Ok(store) => store,
            Err(e) => {
                let _ = init_status_sender.send(Err(ControllerError::input_transport_error(
                    &self.endpoint_name,
                    true,
                    anyhow!("failed to initialize PostgresStore: {e}"),
                )));
                return;
            }
        };

        match reconcile_table_state(
            &store,
            &self.config.source_table,
            redo_snapshot,
            &self.endpoint_name,
        )
        .await
        {
            Ok(copy_outstanding) => self.copy.set_copy_open(copy_outstanding),
            Err(e) => {
                let _ = init_status_sender.send(Err(ControllerError::input_transport_error(
                    &self.endpoint_name,
                    true,
                    anyhow!(
                        "could not reconcile the replication state of table '{}' before \
                         starting: {e}. The connector keeps its resume position in schema 'etl' \
                         of the source database and must read and update it at startup; fix the \
                         reported problem, then restart the pipeline",
                        self.config.source_table
                    ),
                )));
                return;
            }
        }

        let pending_senders = if self.step_completion_rx.is_some() {
            Some(Arc::clone(&self.pending_senders))
        } else {
            None
        };

        let destination = FelderaDestination {
            input_stream: Arc::new(Mutex::new(input_stream)),
            queue: Arc::clone(&self.queue),
            source_table: self.config.source_table.clone(),
            endpoint_name: self.endpoint_name.clone(),
            feldera_required_columns,
            pending_senders,
            pipeline_state_rx: receiver.clone(),
            copy: Arc::clone(&self.copy),
            consumer: self.consumer.clone(),
        };

        let table_error_monitor = TableErrorMonitor {
            endpoint_name: self.endpoint_name.clone(),
            consumer: self.consumer.clone(),
            store: store.clone(),
            source_table: self.config.source_table.clone(),
            publication: self.config.publication.clone(),
            copy: Arc::clone(&self.copy),
            pipeline_state_rx: receiver.clone(),
        };
        let mut pipeline = Pipeline::new(pipeline_config, store, destination);
        self.set_etl_shutdown_tx(pipeline.shutdown_tx());

        match pipeline.start().await {
            Ok(()) => {
                info!(
                    "postgres_cdc {}: etl pipeline started for publication '{}', table '{}'",
                    &self.endpoint_name, &self.config.publication, &self.config.source_table,
                );
                let _ = init_status_sender.send(Ok(()));
            }
            Err(e) => {
                let _ = init_status_sender.send(Err(ControllerError::input_transport_error(
                    &self.endpoint_name,
                    true,
                    anyhow!("failed to start etl pipeline: {e}"),
                )));
                self.shutdown_etl_pipeline();
                return;
            }
        }

        // Spawn the completion watcher background task if tracking is available.
        // The watcher and the channel were created in new(); we take them here
        // after etl has started so startup failures do not leave a task behind.
        let mut completion_handle = match (
            self.watcher_rx.lock().unwrap().take(),
            self.completion_task_rx.lock().unwrap().take(),
        ) {
            (Some(watcher), Some(rx)) => Some(tokio::spawn(completion_watcher_task(
                watcher,
                rx,
                self.endpoint_name.clone(),
            ))),
            _ => None,
        };

        // Run the pipeline alongside a watcher for non-retriable per-table
        // errors. etl marks a table errored (e.g. on a source schema change)
        // without failing the whole pipeline, so `pipeline.wait` would block
        // forever while the input silently stalls; the watcher reports such an
        // error so the controller fails the endpoint instead.
        let mut receiver_clone = receiver.clone();
        let pipeline_wait = pipeline.wait();
        tokio::pin!(pipeline_wait);
        let (pipeline_result, report_error) = select! {
            result = &mut pipeline_wait => (result, true),
            // Drop the watch guard inside the block: holding it while awaiting
            // `pipeline_wait` blocks etl tasks that read the same watch in
            // `wait_unpaused`, once the reader's `Drop` queues another write.
            _ = async {
                let _ = receiver_clone
                    .wait_for(|state| state == &PipelineState::Terminated)
                    .await;
            } => {
                debug!(
                    "postgres_cdc {}: received termination command; shutting down etl pipeline",
                    &self.endpoint_name
                );
                self.shutdown_etl_pipeline();
                abort_completion_watcher(&mut completion_handle).await;
                (pipeline_wait.as_mut().await, false)
            }
            _ = table_error_monitor.run() => {
                self.shutdown_etl_pipeline();
                abort_completion_watcher(&mut completion_handle).await;
                (pipeline_wait.as_mut().await, false)
            }
        };

        if let Err(e) = pipeline_result {
            if report_error && *receiver.borrow() != PipelineState::Terminated {
                error!(
                    "postgres_cdc {}: etl pipeline error: {e}",
                    &self.endpoint_name
                );
                self.consumer.error(true, anyhow!(e), None);
            } else {
                debug!(
                    "postgres_cdc {}: etl pipeline stopped during shutdown: {e}",
                    &self.endpoint_name
                );
            }
        }

        abort_completion_watcher(&mut completion_handle).await;

        self.consumer.eoi();
    }

    fn set_etl_shutdown_tx(&self, shutdown_tx: ShutdownTx) {
        *self.etl_shutdown_tx.lock().unwrap() = Some(shutdown_tx);
    }

    fn shutdown_etl_pipeline(&self) {
        if let Some(shutdown_tx) = self.etl_shutdown_tx.lock().unwrap().take() {
            let _ = shutdown_tx.shutdown();
        }
    }
}

impl Drop for PostgresCdcInputInner {
    fn drop(&mut self) {
        self.shutdown_etl_pipeline();
    }
}

/// Monitor that turns etl table-state failures into Feldera connector failures.
struct TableErrorMonitor {
    endpoint_name: String,
    consumer: Box<dyn InputConsumer>,
    store: PostgresStore,
    source_table: String,
    publication: String,
    /// Checkpoint barrier of the initial copy, which the monitor only reads:
    /// the copy it counts buffers for is the one sign of progress a copy under
    /// way gives.
    copy: Arc<CopyBarrier>,
    /// Errors recorded while the pipeline is not running wait until it runs
    /// again; the shutdown errors among them are the connector's own doing
    /// and the next start rolls them back.
    pipeline_state_rx: Receiver<PipelineState>,
}

/// How long etl may leave this connector's table where it stands, while having
/// no other table of the publication left to copy, before the connector
/// reports that it is reading nothing. The report is a backstop for a table
/// etl never picks up, most often one the publication does not carry: without
/// it the connector would ingest nothing, report nothing, and hold its
/// checkpoint barrier for the life of the pipeline.
const TABLE_STALL_REPORT: Duration = Duration::from_secs(600);

/// What the monitor watches to tell whether etl is moving the source table
/// forward.
// etl's `TableState` implements `PartialEq` and not `Eq`.
#[derive(Debug, Clone, PartialEq)]
struct TableProgress {
    /// The etl table the source table resolves to, once etl has stored its
    /// schema.
    table_id: Option<TableId>,
    /// Its replication state, which etl records at each step of the copy.
    state: Option<TableState>,
    /// Snapshot buffers the copy has queued and flushed into the circuit.
    /// Between the start and the end of a copy etl records no state
    /// transition, so for a copy under way these are the only signs of progress
    /// there are. Once etl has handed over its last batch and waits on the
    /// terminal barrier, only the flushed count still moves, and a slow circuit
    /// may take longer than the stall report to drain it.
    snapshot_buffers_queued: usize,
    snapshot_buffers_flushed: usize,
}

impl TableErrorMonitor {
    /// Surface a non-retriable replication error on the source table as a
    /// fatal endpoint error, and report a table etl leaves untouched.
    ///
    /// When etl cannot continue replicating a table — most notably after a
    /// source schema change, which Feldera does not support — it marks the
    /// table `Errored` and stops applying its changes but keeps the pipeline
    /// running. From Feldera's side the input would then silently stall. This
    /// polls etl's state store and, on an error whose retry policy is `NoRetry`
    /// or `ManualRetry` (i.e. it will not clear on its own), reports it via the
    /// consumer so the controller fails the endpoint. `TimedRetry` errors are
    /// left alone: etl retries them and, once retries are exhausted, the apply
    /// worker propagates the failure through `pipeline.wait`.
    async fn run(self) {
        const POLL_INTERVAL: Duration = Duration::from_millis(500);

        let mut last_progress: Option<TableProgress> = None;
        let mut progress_seen_at = Instant::now();

        loop {
            tokio::time::sleep(POLL_INTERVAL).await;

            // Read the states before resolving the source table, and keep the
            // states of the other tables: etl marks a table `Errored` from the
            // same worker that would have stored its schema, and it stores the
            // schema only after it has created the copy slot and read the
            // schema from the source, so a table that errors on the way there
            // has a state and no name.
            let Some(states) = self.read_table_states().await else {
                // The poll learned nothing about etl, so it says nothing about
                // whether etl is moving.
                progress_seen_at = Instant::now();
                continue;
            };

            let target = match target_table_id(&self.store, &self.source_table).await {
                Ok(target) => target,
                Err(e) => {
                    debug!(
                        "postgres_cdc {}: failed to resolve the source table: {e}",
                        &self.endpoint_name
                    );
                    progress_seen_at = Instant::now();
                    continue;
                }
            };

            if states
                .keys()
                .any(|table_id| self.report_table_error(&states, *table_id, target))
            {
                return;
            }

            let progress = TableProgress {
                table_id: target,
                state: target.and_then(|table_id| states.get(&table_id).cloned()),
                snapshot_buffers_queued: self.copy.snapshot_buffers_queued(),
                snapshot_buffers_flushed: self.copy.snapshot_buffers_flushed(),
            };
            if last_progress.as_ref() != Some(&progress) || self.nothing_to_report(&states, target)
            {
                last_progress = Some(progress);
                progress_seen_at = Instant::now();
                continue;
            }

            if progress_seen_at.elapsed() >= TABLE_STALL_REPORT {
                self.report_stall(&states, target);
                return;
            }
        }
    }

    /// etl's state for every table of the publication, or `None` when the
    /// store cannot be read. A read that fails says nothing about etl, and the
    /// connector accuses it of nothing.
    async fn read_table_states(&self) -> Option<Arc<BTreeMap<TableId, TableState>>> {
        match self.store.get_table_states().await {
            Ok(states) => Some(states),
            Err(e) => {
                debug!(
                    "postgres_cdc {}: failed to read table replication states: {e}",
                    &self.endpoint_name
                );
                None
            }
        }
    }

    /// Whether the source table standing still says nothing about etl, so that
    /// the stall clock has to start over:
    ///
    /// - the Feldera pipeline is not running, which holds every copy where it
    ///   stands;
    /// - the table sync of the source table has completed, after which the
    ///   connector streams changes and a table nobody writes to is meant to be
    ///   quiet;
    /// - etl has another table of the publication still to copy. It copies a
    ///   bounded number of tables at a time, so the source table may be
    ///   waiting its turn, and a copy under way records no state transition to
    ///   tell a busy etl from a stuck one.
    fn nothing_to_report(
        &self,
        states: &BTreeMap<TableId, TableState>,
        target: Option<TableId>,
    ) -> bool {
        if *self.pipeline_state_rx.borrow() != PipelineState::Running {
            return true;
        }
        if target.is_some_and(|table_id| states.get(&table_id).is_some_and(sync_completed)) {
            return true;
        }
        states.iter().any(|(table_id, state)| {
            Some(*table_id) != target
                && !sync_completed(state)
                && !matches!(state, TableState::Errored { .. })
        })
    }

    /// Report a non-retriable error on a table of the publication, and say
    /// whether one was reported. etl stops replicating an errored table but
    /// keeps the pipeline running, so from Feldera's side nothing else would
    /// say why the input stalled; a table this connector never reads is
    /// reported too, since the same publication feeds it.
    fn report_table_error(
        &self,
        states: &BTreeMap<TableId, TableState>,
        table_id: TableId,
        target: Option<TableId>,
    ) -> bool {
        let Some(TableState::Errored {
            reason,
            solution,
            retry_policy,
            ..
        }) = states.get(&table_id)
        else {
            return false;
        };

        // A timed retry clears on its own; leave it to etl.
        if matches!(retry_policy, TableRetryPolicy::TimedRetry { .. }) {
            return false;
        }

        // Feldera stopping the connector is what leaves a shutdown error
        // behind, and the next start rolls it back, so report nothing while
        // the pipeline is paused or terminated. An error seen while the
        // pipeline runs is one the rollback could not clear, or one etl raised
        // mid-run; the table has stalled either way.
        if *self.pipeline_state_rx.borrow() != PipelineState::Running {
            return false;
        }

        let detail = match solution {
            Some(solution) => format!("{reason} ({solution})"),
            None => reason.clone(),
        };
        let table = if Some(table_id) == target {
            format!(
                "source table '{}' (etl table {table_id})",
                &self.source_table
            )
        } else {
            format!("table {table_id}")
        };
        let error = anyhow!("postgres replication error on {table}: {detail}");
        error!("postgres_cdc {}: {error}", &self.endpoint_name);
        self.consumer.error(true, error, None);
        true
    }

    /// Report a source table etl leaves where it stands with nothing else to
    /// copy. The connector would otherwise ingest nothing, report nothing, and
    /// hold its checkpoint barrier for the life of the pipeline.
    fn report_stall(&self, states: &BTreeMap<TableId, TableState>, target: Option<TableId>) {
        // A non-retriable error on another table was reported the moment it
        // appeared, so the errors left here are the ones etl still retries.
        // They are the likeliest reason etl never reached this table, and
        // while the source table is unresolved one of them may be on it under
        // a name etl never got as far as storing.
        let other_errors: Vec<String> = states
            .iter()
            .filter(|(table_id, _)| Some(**table_id) != target)
            .filter_map(|(table_id, state)| match state {
                TableState::Errored { reason, .. } => Some(format!("table {table_id}: {reason}")),
                _ => None,
            })
            .collect();
        let errors = if other_errors.is_empty() {
            String::new()
        } else {
            format!(
                ". etl reports errors on other tables of the publication: {}",
                other_errors.join("; ")
            )
        };

        let error = match target {
            Some(table_id) => {
                let state = match states.get(&table_id) {
                    Some(state) => format!("{state:?}"),
                    None => "unknown".to_string(),
                };
                anyhow!(
                    "etl has not moved source table '{}' (etl table {table_id}) forward in {} \
                     seconds, having no other table of publication '{}' left to copy. Its \
                     replication state is {state}. Check that the pipeline and Postgres server \
                     logs report no replication failure{errors}",
                    &self.source_table,
                    TABLE_STALL_REPORT.as_secs(),
                    &self.publication,
                )
            }
            None => anyhow!(
                "table '{}' has not appeared in the replication state of publication '{}' in {} \
                 seconds, in which etl had no other table of the publication left to copy. Check \
                 that the publication carries this table (an unqualified name refers to schema \
                 'public') and that the pipeline and Postgres server logs report no replication \
                 failure{errors}",
                &self.source_table,
                &self.publication,
                TABLE_STALL_REPORT.as_secs(),
            ),
        };
        error!("postgres_cdc {}: {error}", &self.endpoint_name);
        self.consumer.error(true, error, None);
    }
}

/// etl Destination implementation that pushes data into a Feldera DeCollectionStream.
#[derive(Clone)]
struct FelderaDestination {
    input_stream: Arc<Mutex<Box<dyn DeCollectionStream>>>,
    queue: Arc<InputQueue<QueueAux>>,
    source_table: String,
    endpoint_name: String,
    /// Canonical names of the non-nullable Feldera columns. Each must be present
    /// (by name) in the target Postgres table schema etl passes with each target
    /// batch/event. Nullable and extra columns need not match.
    feldera_required_columns: Vec<String>,
    /// Deferred async result senders. If `Some`, write_events stores senders here
    /// instead of firing them immediately. The Queue handler picks them up.
    pending_senders: Option<Arc<Mutex<DeferredSenders>>>,
    /// Pipeline state receiver used to stop accepting new etl batches while the
    /// Feldera pipeline is paused.
    pipeline_state_rx: Receiver<PipelineState>,
    /// Checkpoint barrier of the initial copy, which every batch of copy rows
    /// raises and etl's terminal barrier lowers; see
    /// [`FelderaDestination::hold_copy_barrier`].
    copy: Arc<CopyBarrier>,
    /// Asks the controller for the step that reports a barrier the destination
    /// lowered away from the reader; see
    /// [`FelderaDestination::hold_copy_barrier`].
    consumer: Box<dyn InputConsumer>,
}

/// Per-entry auxiliary data on the input queue.
#[derive(Debug, Clone, Copy, Default)]
struct QueueAux {
    /// The entry holds rows of the initial table copy.
    snapshot: bool,
}

/// Snapshot buffers among the entries a flush put into the circuit.
fn count_snapshot_buffers(flushed: &[(chrono::DateTime<Utc>, QueueAux)]) -> usize {
    flushed.iter().filter(|(_, aux)| aux.snapshot).count()
}

/// What the reader reports a step as resumable from. etl's `PostgresStore`
/// holds the replication position, so a resumable step needs no metadata
/// beyond an identifier. While a copy of the table is on its way to the
/// circuit, the step is a barrier instead: the controller then defers a
/// checkpoint or a suspend rather than recording a partial copy it could not
/// finish on resume.
fn resume_for(copy_open: bool, pipeline_id: u64) -> Resume {
    if copy_open {
        Resume::Barrier
    } else {
        Resume::Seek {
            seek: json!({ "pipeline_id": pipeline_id }),
        }
    }
}

impl Destination for FelderaDestination {
    fn name() -> &'static str {
        "feldera"
    }

    async fn drop_table_for_copy(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        async_result: DropTableForCopyResult<()>,
    ) -> EtlResult<()> {
        self.wait_unpaused().await?;

        // Feldera owns no physical destination object to drop; the data lives
        // in the circuit, and a copy that starts over simply re-snapshots
        // through `write_table_rows`.
        //
        // The call still carries the news that a copy is starting over, and it
        // is the earliest such news etl gives: it precedes both the durable
        // state of the new copy and its first row. Take it for the source
        // table, and let `write_table_rows` take it again if this hook stays
        // silent, as it does whenever etl finds no destination metadata to
        // drop.
        let name = replicated_table_schema.name();
        if self.is_target_table(&name.schema, &name.name) {
            self.note_copy_batch();
        }

        debug!(
            "postgres_cdc {}: nothing to drop for table '{name}'; the copy is read \
             into the circuit",
            &self.endpoint_name,
        );
        async_result.send(Ok(()));
        Ok(())
    }

    async fn write_table_rows(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        batch_id: Option<TableCopyBatchId>,
        table_rows: Vec<TableRow>,
        async_result: WriteTableRowsResult,
    ) -> EtlResult<()> {
        self.wait_unpaused().await?;

        let target_columns = self.column_names_for_target_schema(replicated_table_schema)?;
        let column_names = match classify_copy_write(target_columns, batch_id.is_some()) {
            // Rows of another table of the publication reach no Feldera table,
            // so they are durable the moment they are dropped, and so is the
            // barrier that closes their copy.
            CopyWrite::OtherTable => {
                async_result.send(Ok(DestinationWriteStatus::Durable));
                return Ok(());
            }
            CopyWrite::TargetBarrier => {
                self.hold_copy_barrier(async_result);
                return Ok(());
            }
            CopyWrite::TargetBatch(column_names) => column_names,
        };

        self.note_copy_batch();

        let mut stream = self.input_stream.lock().unwrap();
        let mut bytes = 0;
        let mut errors = Vec::new();
        let timestamp = Utc::now();

        for row in &table_rows {
            let cells = row.values();
            let json_value = row_to_json(cells, &column_names);

            let json_str = json_value.to_string();
            if let Err(e) = stream.insert(json_str.as_bytes(), &None) {
                errors.push(ParseError::text_event_error(
                    "Failed to deserialize CDC snapshot row",
                    e,
                    0,
                    Some(&json_str),
                    None,
                ));
            }
            bytes += json_str.len();

            if bytes >= 2 * 1024 * 1024 {
                self.push_snapshot_buffer((stream.take_all(), errors), timestamp);
                bytes = 0;
                errors = Vec::new();
            }
        }

        if bytes > 0 || !errors.is_empty() {
            self.push_snapshot_buffer((stream.take_all(), errors), timestamp);
        }

        // The rows are queued, not yet in the circuit. etl's terminal barrier
        // is what answers for them, once they have been flushed.
        async_result.send(Ok(DestinationWriteStatus::Accepted));
        Ok(())
    }

    async fn write_events(
        &self,
        events: Vec<Event>,
        // The connector reports no streaming write as merely accepted, so it
        // owes etl no durability and answers `RequireDurable` exactly as it
        // answers `MayDefer`: `Durable`, once the data has been stepped through
        // the circuit.
        _durability: WriteEventsDurability,
        async_result: WriteEventsResult,
    ) -> EtlResult<()> {
        self.wait_unpaused().await?;

        let mut stream = self.input_stream.lock().unwrap();
        let mut queued = 0usize;
        let mut bytes = 0;
        let mut errors = Vec::new();
        let timestamp = Utc::now();

        for event in &events {
            match event {
                Event::Insert(insert) => {
                    let Some(cols) =
                        self.column_names_for_target_schema(&insert.replicated_table_schema)?
                    else {
                        continue;
                    };
                    let json_value = row_to_json(insert.table_row.values(), &cols);
                    let json_str = json_value.to_string();
                    if let Err(e) = stream.insert(json_str.as_bytes(), &None) {
                        errors.push(ParseError::text_event_error(
                            "Failed to deserialize CDC insert",
                            e,
                            0,
                            Some(&json_str),
                            None,
                        ));
                    }
                    bytes += json_str.len();
                }
                Event::Update(update) => {
                    let Some(cols) =
                        self.column_names_for_target_schema(&update.replicated_table_schema)?
                    else {
                        continue;
                    };
                    // The new row is authoritative only when complete. A partial
                    // image (PostgreSQL `UnchangedToast` columns etl could not
                    // reconstruct) cannot be turned into a correct Feldera row,
                    // so skip the whole update rather than emit a half-applied
                    // delete-without-insert.
                    let UpdatedTableRow::Full(new_row) = &update.updated_table_row else {
                        warn!(
                            "postgres_cdc {}: skipping update with a partial row image \
                             (unchanged TOAST columns); set REPLICA IDENTITY FULL on the source \
                             table to receive complete rows",
                            &self.endpoint_name
                        );
                        continue;
                    };
                    // Delete the old row first, if PostgreSQL supplied one.
                    if let Some(old_row) = &update.old_table_row {
                        let old_str =
                            old_row_to_json(&update.replicated_table_schema, &cols, old_row)
                                .to_string();
                        if let Err(e) = stream.delete(old_str.as_bytes(), &None) {
                            errors.push(ParseError::text_event_error(
                                "Failed to deserialize CDC update (old)",
                                e,
                                0,
                                Some(&old_str),
                                None,
                            ));
                        }
                        bytes += old_str.len();
                    }
                    // Insert the new row.
                    let new_str = row_to_json(new_row.values(), &cols).to_string();
                    if let Err(e) = stream.insert(new_str.as_bytes(), &None) {
                        errors.push(ParseError::text_event_error(
                            "Failed to deserialize CDC update (new)",
                            e,
                            0,
                            Some(&new_str),
                            None,
                        ));
                    }
                    bytes += new_str.len();
                }
                Event::Delete(delete) => {
                    let Some(cols) =
                        self.column_names_for_target_schema(&delete.replicated_table_schema)?
                    else {
                        continue;
                    };
                    if let Some(old_row) = &delete.old_table_row {
                        let old_str =
                            old_row_to_json(&delete.replicated_table_schema, &cols, old_row)
                                .to_string();
                        if let Err(e) = stream.delete(old_str.as_bytes(), &None) {
                            errors.push(ParseError::text_event_error(
                                "Failed to deserialize CDC delete",
                                e,
                                0,
                                Some(&old_str),
                                None,
                            ));
                        }
                        bytes += old_str.len();
                    }
                }
                Event::Truncate(_) => {
                    warn!(
                        "postgres_cdc {}: received TRUNCATE event, ignoring",
                        &self.endpoint_name
                    );
                }
                // Relation events carry only schema, no row data. etl detects
                // schema changes upstream (it refuses to forward a Relation
                // whose schema differs from the resolved one) and marks the
                // table errored; we surface that via `TableErrorMonitor`.
                Event::Relation(_) | Event::Begin(_) | Event::Commit(_) | Event::Unsupported => {}
            }

            if bytes >= 2 * 1024 * 1024 {
                self.queue.push_with_aux(
                    (stream.take_all(), errors),
                    timestamp,
                    QueueAux::default(),
                );
                queued += 1;
                bytes = 0;
                errors = Vec::new();
            }
        }

        if bytes > 0 || !errors.is_empty() {
            self.queue
                .push_with_aux((stream.take_all(), errors), timestamp, QueueAux::default());
            queued += 1;
        }

        // A write that queued nothing, because it was an empty durability
        // barrier or carried only another table's events, has nothing to wait
        // for. Deferring it would also risk wedging etl, which pauses its
        // intake while a streaming write is in flight: with nothing in the
        // queue, no step has to follow that would fire the sender.
        if queued == 0 {
            async_result.send(Ok(DestinationWriteStatus::Durable));
            return Ok(());
        }

        // Defer or fire the async result.
        if let Some(ref pending) = self.pending_senders {
            pending.lock().unwrap().push(async_result);
        } else {
            async_result.send(Ok(DestinationWriteStatus::Durable));
        }

        Ok(())
    }
}

impl FelderaDestination {
    /// Raise the checkpoint barrier for the copy this call belongs to, and
    /// report a copy that starts over.
    ///
    /// etl announces an attempt before it hands over any row of it, so no row
    /// of a new copy can reach the queue while the barrier is down (#6121).
    fn note_copy_batch(&self) {
        if self.copy.note_copy_batch() {
            info!(
                "postgres_cdc {}: etl is copying table '{}' again; checkpoints wait \
                 for the new copy",
                &self.endpoint_name, &self.source_table
            );
        }
    }

    /// Hold etl's terminal copy barrier until every queued row of the copy has
    /// reached the circuit.
    ///
    /// etl waits for this result before it records the copy as finished, so a
    /// copy the circuit does not hold in full can neither be checkpointed nor
    /// be skipped on the next start (#6121).
    fn hold_copy_barrier(&self, async_result: WriteTableRowsResult) {
        let lowered = self.copy.hold_terminal(Box::new(move || {
            async_result.send(Ok(DestinationWriteStatus::Durable))
        }));
        if lowered {
            // The circuit already held every row of the copy, so no flush is
            // left to report the lowered barrier. A checkpoint deferred on that
            // barrier stops the controller from stepping on its own, so ask for
            // the step that reports it; without it the checkpoint waits for
            // good.
            self.consumer.request_step();
        }
    }

    /// Queue a buffer of initial-copy rows and account for it, so the reader
    /// can tell when the last snapshot row has reached the circuit.
    fn push_snapshot_buffer(
        &self,
        buffer: (
            Option<Box<dyn feldera_adapterlib::format::InputBuffer>>,
            Vec<ParseError>,
        ),
        timestamp: chrono::DateTime<Utc>,
    ) {
        // Account for the buffer first, so the reader never counts a flush it
        // has not seen queued.
        self.copy.note_buffer_queued();
        self.queue
            .push_with_aux(buffer, timestamp, QueueAux { snapshot: true });
    }

    /// Wait until the Feldera pipeline is running before accepting a new etl
    /// batch.
    async fn wait_unpaused(&self) -> EtlResult<()> {
        let mut rx = self.pipeline_state_rx.clone();
        match rx.wait_for(|state| state != &PipelineState::Paused).await {
            Ok(state) if *state == PipelineState::Running => Ok(()),
            Ok(_) => Err(etl_error!(
                ErrorKind::DestinationError,
                TERMINATED_BEFORE_BATCH
            )),
            // Same marker as above so startup rolls this back too.
            Err(_) => Err(etl_error!(
                ErrorKind::DestinationError,
                TERMINATED_BEFORE_BATCH,
                "state channel closed"
            )),
        }
    }

    fn is_target_table(&self, schema_name: &str, table_name: &str) -> bool {
        is_target_table(&self.source_table, schema_name, table_name)
    }

    /// Resolve the replicated column names for `schema`.
    ///
    /// Returns `Some(column_names)`, in row-payload order, if `schema` describes
    /// the configured `source_table`, or `None` if it is a different table in
    /// the publication (whose rows are skipped).
    ///
    /// etl carries the table schema with every batch and event, so the connector
    /// uses that schema directly instead of caching target-table metadata.
    fn column_names_for_target_schema(
        &self,
        schema: &ReplicatedTableSchema,
    ) -> EtlResult<Option<Vec<String>>> {
        // A different table in the publication — not ours.
        let name = schema.name();
        if !self.is_target_table(&name.schema, &name.name) {
            return Ok(None);
        }

        let column_names: Vec<String> = replicated_column_names(schema);
        self.validate_columns(&name.name, &column_names)?;
        Ok(Some(column_names))
    }

    /// Verify that every non-nullable Feldera column exists (by name) in the
    /// target Postgres table.
    /// Nullable Feldera columns and extra Postgres columns are allowed to differ.
    fn validate_columns(&self, pg_table: &str, pg_columns: &[String]) -> EtlResult<()> {
        let pg_set: BTreeSet<&str> = pg_columns.iter().map(String::as_str).collect();
        let missing: Vec<&str> = self
            .feldera_required_columns
            .iter()
            .map(String::as_str)
            .filter(|c| !pg_set.contains(c))
            .collect();

        if missing.is_empty() {
            return Ok(());
        }

        Err(etl_error!(
            ErrorKind::ValidationError,
            "Postgres CDC source table is missing required Feldera columns",
            format!(
                "table '{pg_table}': non-nullable Feldera columns absent from the Postgres table: \
                 {missing:?}. Every non-nullable Feldera column must exist (by name) in the \
                 source table."
            )
        ))
    }
}

/// What one `write_table_rows` call asks of the connector, where
/// `target_columns` holds the replicated column names when the call describes
/// the source table and `has_batch_id` says whether it carries rows.
///
/// etl carries a batch id with every batch of rows and none with the terminal
/// write it issues once every copy worker has finished, so the absent batch id
/// is what marks the copy's durability barrier.
fn classify_copy_write(target_columns: Option<Vec<String>>, has_batch_id: bool) -> CopyWrite {
    match (target_columns, has_batch_id) {
        (Some(column_names), true) => CopyWrite::TargetBatch(column_names),
        (Some(_), false) => CopyWrite::TargetBarrier,
        (None, _) => CopyWrite::OtherTable,
    }
}

/// What one `write_table_rows` call asks of the connector.
#[derive(Debug)]
enum CopyWrite {
    /// Rows of the source table, to queue for the circuit.
    TargetBatch(Vec<String>),
    /// The terminal durability barrier of the source table's copy.
    TargetBarrier,
    /// A table this connector does not read; nothing of it reaches the circuit.
    OtherTable,
}

/// Whether `schema_name.table_name` is the connector's source table. An
/// unqualified `source_table` names a table in `public`, as the connector
/// documentation says; matching a bare name in any schema would let a
/// publication that carries `sales.orders` and `archive.orders` merge both
/// into one Feldera table (#6126).
fn is_target_table(source_table: &str, schema_name: &str, table_name: &str) -> bool {
    let qualified = format!("{schema_name}.{table_name}");
    source_table == qualified
        || source_table == format!("\"{schema_name}\".\"{table_name}\"")
        || (schema_name == "public" && source_table == table_name)
}

/// Whether etl has completed the initial table sync for a table in `state`.
///
/// etl excludes `FinishedCopy` on purpose: a copy that finished without a
/// durable catchup handoff is read again on the next start, so the table is not
/// done with its first read.
fn sync_completed(state: &TableState) -> bool {
    TableStateType::from(state).has_completed_table_sync()
}

/// Postgres OID of `source_table`, if etl has stored its schema.
///
/// etl keeps one schema row per version of a table and never prunes the rows
/// of a table that was dropped, so a source table that was dropped and
/// recreated leaves several ids under one name. etl keeps replication state
/// only for the table it replicates now, so that state picks the current one.
async fn target_table_id(store: &PostgresStore, source_table: &str) -> EtlResult<Option<TableId>> {
    let mut ids: Vec<TableId> = store
        .get_table_schemas()
        .await?
        .iter()
        .filter(|schema| is_target_table(source_table, &schema.name.schema, &schema.name.name))
        .map(|schema| schema.id)
        .collect();
    ids.sort();
    ids.dedup();
    for id in ids.iter().rev() {
        if store.get_table_state(*id).await?.is_some() {
            return Ok(Some(*id));
        }
    }
    Ok(ids.last().copied())
}

/// Bring etl's persisted state for `source_table` in line with what Feldera
/// has durably ingested, before etl starts, and say whether the connector
/// still owes the circuit a copy of the table.
///
/// 1. Roll back errors that a restart makes moot: errors etl recorded because
///    Feldera terminated the connector mid-batch, whose writes were never
///    acknowledged and are safe to replay, and timed retries, whose deadline
///    etl does not honor across a restart (it never respawns the worker of
///    an `Errored` table, so the table would stall for good). A rollback that
///    fails resets the table to `Init` and reads it again: that clears the
///    error at the cost of a copy, where failing the start would need the
///    state edited by hand.
/// 2. With `redo_snapshot`, reset a completed table sync to `Init`. The circuit
///    starts empty because there is no checkpoint to resume from, whereas etl
///    would skip the copy and stream from the slot. Steps report a barrier
///    until the copy is in the circuit, so this is the only case where a
///    resumed pipeline can be missing it.
///
/// Returns whether etl still has the source table to copy, which is what the
/// connector holds its checkpoint barrier for until the copy is in the circuit.
async fn reconcile_table_state(
    store: &PostgresStore,
    source_table: &str,
    redo_snapshot: bool,
    endpoint_name: &str,
) -> EtlResult<bool> {
    store.load_table_states().await?;
    store.load_table_schemas().await?;
    let Some(table_id) = target_table_id(store, source_table).await? else {
        return Ok(true);
    };
    let Some(mut state) = store.get_table_state(table_id).await? else {
        return Ok(true);
    };

    while let TableState::Errored {
        reason,
        retry_policy,
        ..
    } = &state
        && (is_shutdown_error(reason)
            || matches!(retry_policy, TableRetryPolicy::TimedRetry { .. }))
    {
        let rolled_back = reason.clone();
        // etl deletes the row it rolls back from, so a table whose history
        // holds only the error has nothing to go back to. Read the table
        // again instead: that clears the error and costs a copy, where
        // failing the start would need the state edited by hand and leaving
        // it would stall the table for good.
        match store.rollback_table_state(table_id).await {
            Ok(previous) => state = previous,
            Err(e) => {
                warn!(
                    "postgres_cdc {endpoint_name}: cannot roll back error \
                     '{rolled_back}' for table {table_id} ({e}); reading the table again"
                );
                store.update_table_state(table_id, TableState::Init).await?;
                return Ok(true);
            }
        }
        info!(
            "postgres_cdc {endpoint_name}: rolled back error '{rolled_back}' for table \
             {table_id}; resuming from state {state:?}"
        );
    }

    if sync_completed(&state) {
        if !redo_snapshot {
            return Ok(false);
        }
        warn!(
            "postgres_cdc {endpoint_name}: no checkpoint holds the initial copy of table \
             {table_id}; reading it again"
        );
        store.update_table_state(table_id, TableState::Init).await?;
    }
    Ok(true)
}

/// Replicated column names of `schema`, in the order etl emits cell values for
/// a row.
fn replicated_column_names(schema: &ReplicatedTableSchema) -> Vec<String> {
    schema.column_schemas().map(|c| c.name.clone()).collect()
}

/// Convert an old-row image (carried by updates and deletes) to JSON.
///
/// A [`OldTableRow::Full`] image holds every replicated column, in the same
/// order as `full_columns`. A [`OldTableRow::Key`] image holds only the
/// replica-identity columns, so its values must be paired with the identity
/// column names instead.
fn old_row_to_json(
    schema: &ReplicatedTableSchema,
    full_columns: &[String],
    old_row: &OldTableRow,
) -> Value {
    match old_row {
        OldTableRow::Full(row) => row_to_json(row.values(), full_columns),
        OldTableRow::Key(row) => {
            let identity_columns: Vec<String> = schema
                .identity_column_schemas()
                .map(|c| c.name.clone())
                .collect();
            row_to_json(row.values(), &identity_columns)
        }
    }
}

/// Convert a row of cells to a JSON object using the given column names.
fn row_to_json(cells: &[Cell], column_names: &[String]) -> Value {
    let mut map = serde_json::Map::new();
    for (i, cell) in cells.iter().enumerate() {
        let col_name = column_names
            .get(i)
            .cloned()
            .unwrap_or_else(|| format!("col_{i}"));
        map.insert(col_name, cell_to_json(cell));
    }
    Value::Object(map)
}

/// Convert an etl Cell to a serde_json Value.
fn cell_to_json(cell: &Cell) -> Value {
    match cell {
        Cell::Null => Value::Null,
        Cell::Bool(b) => json!(b),
        Cell::String(s) => json!(s),
        Cell::I16(n) => json!(n),
        Cell::I32(n) => json!(n),
        Cell::U32(n) => json!(n),
        Cell::I64(n) => json!(n),
        Cell::F32(f) => {
            if f.is_nan() || f.is_infinite() {
                Value::Null
            } else {
                json!(f)
            }
        }
        Cell::F64(f) => {
            if f.is_nan() || f.is_infinite() {
                Value::Null
            } else {
                json!(f)
            }
        }
        Cell::Numeric(n) => {
            // Preserve precision by encoding as string.
            json!(n.to_string())
        }
        Cell::Date(d) => json!(d.to_string()),
        Cell::Time(t) => json!(t.to_string()),
        // Feldera has no time-with-time-zone type, so the cell keeps the text
        // form Postgres itself uses, UTC offset included.
        Cell::TimeTz(t) => json!(t.to_string()),
        Cell::Timestamp(ts) => json!(ts.format("%Y-%m-%dT%H:%M:%S%.f").to_string()),
        Cell::TimestampTz(ts) => json!(ts.to_rfc3339()),
        Cell::Uuid(u) => json!(u.to_string()),
        Cell::Json(j) => j.clone(),
        Cell::Bytes(b) => {
            // Encode as byte array to match Datagen's BinaryFormat::Array.
            json!(b)
        }
        Cell::Array(arr) => array_cell_to_json(arr),
    }
}

/// Convert an etl ArrayCell to a JSON array.
fn array_cell_to_json(arr: &ArrayCell) -> Value {
    match arr {
        ArrayCell::Bool(v) => json!(v),
        ArrayCell::String(v) => json!(v),
        ArrayCell::I16(v) => json!(v),
        ArrayCell::I32(v) => json!(v),
        ArrayCell::U32(v) => json!(v),
        ArrayCell::I64(v) => json!(v),
        ArrayCell::F32(v) => {
            let vals: Vec<Value> = v
                .iter()
                .map(|opt| match opt {
                    Some(f) if f.is_nan() || f.is_infinite() => Value::Null,
                    Some(f) => json!(f),
                    None => Value::Null,
                })
                .collect();
            Value::Array(vals)
        }
        ArrayCell::F64(v) => {
            let vals: Vec<Value> = v
                .iter()
                .map(|opt| match opt {
                    Some(f) if f.is_nan() || f.is_infinite() => Value::Null,
                    Some(f) => json!(f),
                    None => Value::Null,
                })
                .collect();
            Value::Array(vals)
        }
        ArrayCell::Numeric(v) => {
            let vals: Vec<Value> = v
                .iter()
                .map(|opt| match opt {
                    Some(n) => json!(n.to_string()),
                    None => Value::Null,
                })
                .collect();
            Value::Array(vals)
        }
        ArrayCell::Date(v) => {
            let vals: Vec<Value> = v
                .iter()
                .map(|opt| match opt {
                    Some(d) => json!(d.to_string()),
                    None => Value::Null,
                })
                .collect();
            Value::Array(vals)
        }
        ArrayCell::Time(v) => {
            let vals: Vec<Value> = v
                .iter()
                .map(|opt| match opt {
                    Some(t) => json!(t.to_string()),
                    None => Value::Null,
                })
                .collect();
            Value::Array(vals)
        }
        ArrayCell::TimeTz(v) => {
            let vals: Vec<Value> = v
                .iter()
                .map(|opt| match opt {
                    Some(t) => json!(t.to_string()),
                    None => Value::Null,
                })
                .collect();
            Value::Array(vals)
        }
        ArrayCell::Timestamp(v) => {
            let vals: Vec<Value> = v
                .iter()
                .map(|opt| match opt {
                    Some(ts) => json!(ts.format("%Y-%m-%dT%H:%M:%S%.f").to_string()),
                    None => Value::Null,
                })
                .collect();
            Value::Array(vals)
        }
        ArrayCell::TimestampTz(v) => {
            let vals: Vec<Value> = v
                .iter()
                .map(|opt| match opt {
                    Some(ts) => json!(ts.to_rfc3339()),
                    None => Value::Null,
                })
                .collect();
            Value::Array(vals)
        }
        ArrayCell::Uuid(v) => {
            let vals: Vec<Value> = v
                .iter()
                .map(|opt| match opt {
                    Some(u) => json!(u.to_string()),
                    None => Value::Null,
                })
                .collect();
            Value::Array(vals)
        }
        ArrayCell::Json(v) => {
            let vals: Vec<Value> = v
                .iter()
                .map(|opt| match opt {
                    Some(j) => j.clone(),
                    None => Value::Null,
                })
                .collect();
            Value::Array(vals)
        }
        ArrayCell::Bytes(v) => {
            let vals: Vec<Value> = v
                .iter()
                .map(|opt| match opt {
                    Some(b) => json!(b),
                    None => Value::Null,
                })
                .collect();
            Value::Array(vals)
        }
    }
}

/// Typed watch receiver used by the completion watcher background task.
///
/// `Fast` waits for step completion (`total_completed_steps`); used when fault
/// tolerance is not enabled.  `Strict` waits for checkpoint completion; used
/// when fault tolerance is enabled so the replication slot only advances past
/// the last durable checkpoint, preserving at-least-once correctness for
/// stateful circuits after a crash.
enum WatcherReceiver {
    Fast(tokio::sync::watch::Receiver<Completion>),
    Strict(tokio::sync::watch::Receiver<u64>),
}

impl WatcherReceiver {
    async fn changed(&mut self) -> Result<(), tokio::sync::watch::error::RecvError> {
        match self {
            Self::Fast(rx) => rx.changed().await,
            Self::Strict(rx) => rx.changed().await,
        }
    }

    fn frontier(&self) -> u64 {
        match self {
            Self::Fast(rx) => rx.borrow().total_completed_steps,
            Self::Strict(rx) => *rx.borrow(),
        }
    }
}

/// Background task that fires deferred ETL async result senders when the
/// completion frontier passes the step recorded at Queue time.
///
/// Each entry is `(step_at_flush, senders)` where `step_at_flush` is the
/// value of `total_completed_steps` at the time the data was flushed to the
/// circuit.  The data lands in the next step, so we fire when the frontier
/// strictly exceeds `step_at_flush`.
async fn completion_watcher_task(
    mut watcher: WatcherReceiver,
    mut pending_rx: mpsc::UnboundedReceiver<(u64, DeferredSenders)>,
    endpoint_name: String,
) {
    let mut waiting: Vec<(u64, DeferredSenders)> = Vec::new();

    loop {
        tokio::select! {
            result = watcher.changed() => {
                if result.is_err() {
                    break; // Sender dropped (pipeline shutting down)
                }
                let f = watcher.frontier();
                fire_completed(&mut waiting, f);
            }
            maybe_entry = pending_rx.recv() => {
                match maybe_entry {
                    Some((step_at_flush, senders)) => {
                        let f = watcher.frontier();
                        if f > step_at_flush {
                            // Already past the threshold — fire immediately.
                            for sender in senders {
                                sender.send(Ok(DestinationWriteStatus::Durable));
                            }
                        } else {
                            waiting.push((step_at_flush, senders));
                        }
                    }
                    None => break, // Channel closed
                }
            }
        }
    }

    // On shutdown, remaining senders are dropped. AsyncResult's Drop impl
    // sends an error to the ETL side, causing it to shut down gracefully.
    debug!(
        "postgres_cdc {endpoint_name}: completion watcher exiting with {} pending entries",
        waiting.len()
    );
}

/// Fires deferred senders whose data has been fully processed.
fn fire_completed(waiting: &mut Vec<(u64, DeferredSenders)>, completed_steps: u64) {
    waiting.retain_mut(|(step_at_flush, senders)| {
        if completed_steps > *step_at_flush {
            for sender in senders.drain(..) {
                sender.send(Ok(DestinationWriteStatus::Durable));
            }
            false
        } else {
            true
        }
    });
}

async fn abort_completion_watcher(handle: &mut Option<tokio::task::JoinHandle<()>>) {
    if let Some(handle) = handle.take() {
        handle.abort();
        let _ = handle.await;
    }
}

/// Deterministic pipeline ID derived from the connection config.
///
/// Stable across Rust versions (xxh3) and across password rotations: the
/// identity string excludes the password and other volatile fields, so
/// rotating the password does not change the ID — which would otherwise
/// orphan the replication slot and stored etl state and force a full
/// re-snapshot. etl names its replication slots after this ID (e.g.
/// `supabase_etl_apply_<id>`), so tests reconstruct it to clean up slots.
pub(crate) fn pipeline_id(uri: &str, publication: &str, source_table: &str) -> u64 {
    xxh3_64(stable_connection_identity(uri, publication, source_table).as_bytes())
}

/// Build a stable identity string for pipeline_id hashing.
///
/// Extracts host/port/database from the URI (excludes password, username,
/// and query parameters) combined with publication and source_table.
/// Falls back to the raw URI if parsing fails — the pipeline will likely
/// fail startup shortly after anyway with a clearer error.
fn stable_connection_identity(uri: &str, publication: &str, source_table: &str) -> String {
    let (host, port, db) = match Url::parse(uri) {
        Ok(url) => {
            let host = url.host_str().unwrap_or("").to_string();
            let port = url.port().unwrap_or(5432);
            let db = url.path().trim_start_matches('/').to_string();
            (host, port, db)
        }
        Err(_) => return format!("{uri}\0{publication}\0{source_table}"),
    };
    format!("{host}:{port}/{db}\0{publication}\0{source_table}")
}

/// Parse a Postgres URI into etl's PgConnectionConfig.
fn parse_pg_uri(
    uri: &str,
    tls: &PostgresTlsConfig,
    endpoint_name: &str,
) -> AnyResult<PgConnectionConfig> {
    let url = Url::parse(uri)?;

    let host = url
        .host_str()
        .ok_or_else(|| anyhow!("missing host in URI"))?
        .to_string();
    let port = url.port().unwrap_or(5432);
    let username = url.username().to_string();
    if username.is_empty() {
        return Err(anyhow!("missing username in URI"));
    }
    let password = url.password().map(|p| p.to_string().into());
    let name = url.path().trim_start_matches('/').to_string();
    if name.is_empty() {
        return Err(anyhow!("missing database name in URI"));
    }

    Ok(PgConnectionConfig {
        host,
        // No separate numeric address; etl resolves `host` itself.
        hostaddr: None,
        port,
        name,
        username,
        password,
        tls: make_etl_tls_config(tls, endpoint_name)?,
        keepalive: TcpKeepaliveConfig::default(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::{MockDeZSet, MockInputConsumer, TestStruct};
    use chrono::{FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
    use etl::data::{PgNumeric, PgTimeTz};
    use feldera_adapterlib::catalog::DeCollectionHandle;
    use serde_json::json;
    use std::str::FromStr;
    use std::sync::atomic::AtomicBool;

    /// The startup rollback recognizes both errors a stop leaves behind: the
    /// one the connector records itself when Feldera stops it mid-batch, and
    /// the one etl records for a deferred result the connector dropped.
    #[test]
    fn shutdown_error_matcher_accepts_both_stop_literals() {
        let terminated = etl_error!(
            ErrorKind::DestinationError,
            TERMINATED_BEFORE_BATCH,
            "state channel closed"
        );
        assert!(is_shutdown_error(&terminated.to_string()));
        let dropped = etl_error!(
            ErrorKind::DestinationError,
            RESULT_DROPPED_ON_SHUTDOWN,
            "table sync worker drained during shutdown"
        );
        assert!(is_shutdown_error(&dropped.to_string()));
        // A rollback that fired on an unrelated destination error would replay
        // writes etl already acknowledged, so the matcher must stay narrow.
        assert!(!is_shutdown_error("connection refused"));
        assert!(!is_shutdown_error(
            "Table copy durability barrier did not confirm durability"
        ));
        // Spelled out here so an edit to either constant is deliberate. The
        // second is etl's text: check `destination/async_result.rs` on an etl
        // revision bump.
        assert_eq!(
            TERMINATED_BEFORE_BATCH,
            "Postgres CDC input connector terminated before accepting batch"
        );
        assert_eq!(
            RESULT_DROPPED_ON_SHUTDOWN,
            "Async result channel closed before sending"
        );
    }

    // -----------------------------------------------------------------------
    // cell_to_json unit tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_cell_null() {
        assert_eq!(cell_to_json(&Cell::Null), Value::Null);
    }

    #[test]
    fn test_cell_bool() {
        assert_eq!(cell_to_json(&Cell::Bool(true)), json!(true));
        assert_eq!(cell_to_json(&Cell::Bool(false)), json!(false));
    }

    #[test]
    fn test_cell_string() {
        assert_eq!(cell_to_json(&Cell::String("hello".into())), json!("hello"));
        assert_eq!(cell_to_json(&Cell::String("".into())), json!(""));
        // Unicode
        assert_eq!(
            cell_to_json(&Cell::String("caf\u{00e9}".into())),
            json!("caf\u{00e9}")
        );
    }

    #[test]
    fn test_cell_integers() {
        assert_eq!(cell_to_json(&Cell::I16(42)), json!(42));
        assert_eq!(cell_to_json(&Cell::I16(-1)), json!(-1));
        assert_eq!(cell_to_json(&Cell::I32(100_000)), json!(100_000));
        assert_eq!(
            cell_to_json(&Cell::U32(4_000_000_000)),
            json!(4_000_000_000u64)
        );
        assert_eq!(cell_to_json(&Cell::I64(i64::MAX)), json!(i64::MAX));
        assert_eq!(cell_to_json(&Cell::I64(i64::MIN)), json!(i64::MIN));
    }

    #[test]
    fn test_cell_f32() {
        assert_eq!(cell_to_json(&Cell::F32(3.5)), json!(3.5f32));
        // NaN and infinity produce null
        assert_eq!(cell_to_json(&Cell::F32(f32::NAN)), Value::Null);
        assert_eq!(cell_to_json(&Cell::F32(f32::INFINITY)), Value::Null);
        assert_eq!(cell_to_json(&Cell::F32(f32::NEG_INFINITY)), Value::Null);
    }

    #[test]
    fn test_cell_f64() {
        assert_eq!(cell_to_json(&Cell::F64(2.25)), json!(2.25f64));
        assert_eq!(cell_to_json(&Cell::F64(f64::NAN)), Value::Null);
        assert_eq!(cell_to_json(&Cell::F64(f64::INFINITY)), Value::Null);
        assert_eq!(cell_to_json(&Cell::F64(f64::NEG_INFINITY)), Value::Null);
    }

    #[test]
    fn test_cell_numeric() {
        let n = PgNumeric::from_str("123.456").unwrap();
        let v = cell_to_json(&Cell::Numeric(n));
        assert_eq!(v, json!("123.456"));
    }

    #[test]
    fn test_cell_date() {
        let d = NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
        let v = cell_to_json(&Cell::Date(d));
        assert_eq!(v, json!("2024-06-15"));
    }

    #[test]
    fn test_cell_time() {
        let t = NaiveTime::from_hms_opt(14, 30, 0).unwrap();
        let v = cell_to_json(&Cell::Time(t));
        assert_eq!(v, json!("14:30:00"));
    }

    #[test]
    fn test_cell_timetz() {
        let t = PgTimeTz::new(
            NaiveTime::from_hms_opt(14, 30, 0).unwrap(),
            FixedOffset::east_opt(2 * 3600).unwrap(),
        );
        let v = cell_to_json(&Cell::TimeTz(t));
        assert_eq!(v, json!("14:30:00+02"));
    }

    #[test]
    fn test_cell_timestamp() {
        let dt = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            NaiveTime::from_hms_opt(12, 0, 0).unwrap(),
        );
        let v = cell_to_json(&Cell::Timestamp(dt));
        assert_eq!(v, json!("2024-01-01T12:00:00"));
    }

    #[test]
    fn test_cell_timestamptz() {
        let dt = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();
        let v = cell_to_json(&Cell::TimestampTz(dt));
        // RFC 3339 format
        assert_eq!(v, json!("2024-01-01T12:00:00+00:00"));
    }

    #[test]
    fn test_cell_uuid() {
        let u = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let v = cell_to_json(&Cell::Uuid(u));
        assert_eq!(v, json!("550e8400-e29b-41d4-a716-446655440000"));
    }

    #[test]
    fn test_cell_json() {
        let j = json!({"key": "value", "num": 42});
        let v = cell_to_json(&Cell::Json(j.clone()));
        assert_eq!(v, j);
    }

    #[test]
    fn test_cell_bytes() {
        let v = cell_to_json(&Cell::Bytes(vec![0xde, 0xad, 0xbe, 0xef]));
        assert_eq!(v, json!([0xde, 0xad, 0xbe, 0xef]));
    }

    #[test]
    fn test_cell_bytes_empty() {
        let v = cell_to_json(&Cell::Bytes(vec![]));
        assert_eq!(v, json!([]));
    }

    // -----------------------------------------------------------------------
    // array_cell_to_json unit tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_array_bool() {
        let arr = ArrayCell::Bool(vec![Some(true), Some(false), None]);
        let v = array_cell_to_json(&arr);
        assert_eq!(v, json!([true, false, null]));
    }

    #[test]
    fn test_array_string() {
        let arr = ArrayCell::String(vec![Some("a".into()), None, Some("b".into())]);
        let v = array_cell_to_json(&arr);
        assert_eq!(v, json!(["a", null, "b"]));
    }

    #[test]
    fn test_array_i16() {
        let arr = ArrayCell::I16(vec![Some(1), Some(-2), None]);
        let v = array_cell_to_json(&arr);
        assert_eq!(v, json!([1, -2, null]));
    }

    #[test]
    fn test_array_i32() {
        let arr = ArrayCell::I32(vec![Some(100), None]);
        let v = array_cell_to_json(&arr);
        assert_eq!(v, json!([100, null]));
    }

    #[test]
    fn test_array_i64() {
        let arr = ArrayCell::I64(vec![Some(i64::MAX), None, Some(0)]);
        let v = array_cell_to_json(&arr);
        assert_eq!(v, json!([i64::MAX, null, 0]));
    }

    #[test]
    fn test_array_f32_with_special() {
        let arr = ArrayCell::F32(vec![Some(1.5), None, Some(f32::NAN), Some(f32::INFINITY)]);
        let v = array_cell_to_json(&arr);
        assert_eq!(v, json!([1.5f32, null, null, null]));
    }

    #[test]
    fn test_array_f64_with_special() {
        let arr = ArrayCell::F64(vec![Some(2.5), Some(f64::NEG_INFINITY), None]);
        let v = array_cell_to_json(&arr);
        assert_eq!(v, json!([2.5f64, null, null]));
    }

    #[test]
    fn test_array_numeric() {
        let n = PgNumeric::from_str("99.99").unwrap();
        let arr = ArrayCell::Numeric(vec![Some(n), None]);
        let v = array_cell_to_json(&arr);
        assert_eq!(v, json!(["99.99", null]));
    }

    #[test]
    fn test_array_date() {
        let d = NaiveDate::from_ymd_opt(2024, 12, 25).unwrap();
        let arr = ArrayCell::Date(vec![Some(d), None]);
        let v = array_cell_to_json(&arr);
        assert_eq!(v, json!(["2024-12-25", null]));
    }

    #[test]
    fn test_array_time() {
        let t = NaiveTime::from_hms_opt(8, 30, 0).unwrap();
        let arr = ArrayCell::Time(vec![Some(t), None]);
        let v = array_cell_to_json(&arr);
        assert_eq!(v, json!(["08:30:00", null]));
    }

    #[test]
    fn test_array_timetz() {
        let t = PgTimeTz::new(
            NaiveTime::from_hms_opt(8, 30, 0).unwrap(),
            FixedOffset::west_opt(5 * 3600).unwrap(),
        );
        let arr = ArrayCell::TimeTz(vec![Some(t), None]);
        let v = array_cell_to_json(&arr);
        assert_eq!(v, json!(["08:30:00-05", null]));
    }

    #[test]
    fn test_array_timestamp() {
        let dt = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2024, 6, 1).unwrap(),
            NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
        );
        let arr = ArrayCell::Timestamp(vec![Some(dt), None]);
        let v = array_cell_to_json(&arr);
        assert_eq!(v, json!(["2024-06-01T00:00:00", null]));
    }

    #[test]
    fn test_array_timestamptz() {
        let dt = Utc.with_ymd_and_hms(2024, 6, 1, 0, 0, 0).unwrap();
        let arr = ArrayCell::TimestampTz(vec![Some(dt), None]);
        let v = array_cell_to_json(&arr);
        assert_eq!(v, json!(["2024-06-01T00:00:00+00:00", null]));
    }

    #[test]
    fn test_array_uuid() {
        let u = uuid::Uuid::parse_str("12345678-1234-1234-1234-123456789abc").unwrap();
        let arr = ArrayCell::Uuid(vec![Some(u), None]);
        let v = array_cell_to_json(&arr);
        assert_eq!(v, json!(["12345678-1234-1234-1234-123456789abc", null]));
    }

    #[test]
    fn test_array_json() {
        let j = json!({"a": 1});
        let arr = ArrayCell::Json(vec![Some(j.clone()), None]);
        let v = array_cell_to_json(&arr);
        assert_eq!(v, json!([{"a": 1}, null]));
    }

    #[test]
    fn test_array_bytes() {
        let arr = ArrayCell::Bytes(vec![Some(vec![0xca, 0xfe]), None, Some(vec![])]);
        let v = array_cell_to_json(&arr);
        assert_eq!(v, json!([[0xca, 0xfe], null, []]));
    }

    // -----------------------------------------------------------------------
    // row_to_json unit tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_row_to_json_basic() {
        let cells = vec![Cell::I32(1), Cell::String("hello".into()), Cell::Bool(true)];
        let cols = vec!["id".into(), "name".into(), "active".into()];
        let v = row_to_json(&cells, &cols);
        assert_eq!(v, json!({"id": 1, "name": "hello", "active": true}));
    }

    #[test]
    fn test_row_to_json_with_null() {
        let cells = vec![Cell::I32(42), Cell::Null];
        let cols = vec!["id".into(), "value".into()];
        let v = row_to_json(&cells, &cols);
        assert_eq!(v, json!({"id": 42, "value": null}));
    }

    #[test]
    fn test_row_to_json_more_cells_than_columns() {
        // Extra cells get auto-generated column names
        let cells = vec![Cell::I32(1), Cell::I32(2), Cell::I32(3)];
        let cols = vec!["a".into(), "b".into()];
        let v = row_to_json(&cells, &cols);
        assert_eq!(v, json!({"a": 1, "b": 2, "col_2": 3}));
    }

    #[test]
    fn test_row_to_json_all_types() {
        let d = NaiveDate::from_ymd_opt(2024, 3, 15).unwrap();
        let t = NaiveTime::from_hms_opt(10, 30, 0).unwrap();
        let ts = NaiveDateTime::new(d, t);
        let tstz = Utc.with_ymd_and_hms(2024, 3, 15, 10, 30, 0).unwrap();
        let u = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let n = PgNumeric::from_str("123.45").unwrap();

        let cells = vec![
            Cell::Bool(true),
            Cell::I16(16),
            Cell::I32(32),
            Cell::U32(4_000_000_000),
            Cell::I64(64),
            Cell::F32(1.5),
            Cell::F64(2.5),
            Cell::Numeric(n),
            Cell::String("text".into()),
            Cell::Date(d),
            Cell::Time(t),
            Cell::Timestamp(ts),
            Cell::TimestampTz(tstz),
            Cell::Uuid(u),
            Cell::Json(json!({"key": "val"})),
            Cell::Bytes(vec![0xab, 0xcd]),
            Cell::Null,
            Cell::Array(ArrayCell::I32(vec![Some(1), Some(2), None])),
        ];
        let cols: Vec<String> = vec![
            "bool_col",
            "i16_col",
            "i32_col",
            "u32_col",
            "i64_col",
            "f32_col",
            "f64_col",
            "numeric_col",
            "text_col",
            "date_col",
            "time_col",
            "ts_col",
            "tstz_col",
            "uuid_col",
            "json_col",
            "bytes_col",
            "null_col",
            "arr_col",
        ]
        .into_iter()
        .map(String::from)
        .collect();

        let v = row_to_json(&cells, &cols);
        assert_eq!(v["bool_col"], json!(true));
        assert_eq!(v["i16_col"], json!(16));
        assert_eq!(v["i32_col"], json!(32));
        assert_eq!(v["u32_col"], json!(4_000_000_000u64));
        assert_eq!(v["i64_col"], json!(64));
        assert_eq!(v["f32_col"], json!(1.5f32));
        assert_eq!(v["f64_col"], json!(2.5f64));
        assert_eq!(v["numeric_col"], json!("123.45"));
        assert_eq!(v["text_col"], json!("text"));
        assert_eq!(v["date_col"], json!("2024-03-15"));
        assert_eq!(v["time_col"], json!("10:30:00"));
        assert_eq!(v["ts_col"], json!("2024-03-15T10:30:00"));
        assert_eq!(v["tstz_col"], json!("2024-03-15T10:30:00+00:00"));
        assert_eq!(v["uuid_col"], json!("550e8400-e29b-41d4-a716-446655440000"));
        assert_eq!(v["json_col"], json!({"key": "val"}));
        assert_eq!(v["bytes_col"], json!([0xab, 0xcd]));
        assert_eq!(v["null_col"], Value::Null);
        assert_eq!(v["arr_col"], json!([1, 2, null]));
    }

    // -----------------------------------------------------------------------
    // stable_connection_identity unit tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_stable_identity_excludes_password() {
        let id1 = stable_connection_identity(
            "postgres://user:oldpass@localhost:5432/db",
            "pub",
            "public.tbl",
        );
        let id2 = stable_connection_identity(
            "postgres://user:newpass@localhost:5432/db",
            "pub",
            "public.tbl",
        );
        assert_eq!(
            id1, id2,
            "rotating the password should not change the stable identity"
        );
    }

    #[test]
    fn test_stable_identity_excludes_username() {
        let id1 =
            stable_connection_identity("postgres://alice@localhost:5432/db", "pub", "public.tbl");
        let id2 =
            stable_connection_identity("postgres://bob@localhost:5432/db", "pub", "public.tbl");
        assert_eq!(id1, id2);
    }

    #[test]
    fn test_stable_identity_changes_with_host() {
        let id1 = stable_connection_identity("postgres://u:p@host1:5432/db", "pub", "public.tbl");
        let id2 = stable_connection_identity("postgres://u:p@host2:5432/db", "pub", "public.tbl");
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_stable_identity_changes_with_publication() {
        let id1 = stable_connection_identity("postgres://u:p@host:5432/db", "pub1", "public.tbl");
        let id2 = stable_connection_identity("postgres://u:p@host:5432/db", "pub2", "public.tbl");
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_stable_identity_invalid_uri_falls_back() {
        // Parse failure falls back to using the raw URI — identity is still
        // deterministic, just less robust to URI format variations.
        let id = stable_connection_identity("not a valid uri", "pub", "tbl");
        assert!(id.contains("not a valid uri"));
    }

    // -----------------------------------------------------------------------
    // Copy barrier unit tests
    // -----------------------------------------------------------------------
    //
    // These drive the production [`CopyBarrier`] through the calls the
    // destination and the reader make: `note_copy_batch`, `note_buffer_queued`
    // and `hold_terminal` for the destination, `note_buffers_flushed` for the
    // reader. etl keeps the constructor of a `WriteTableRowsResult` private, so
    // the terminal barrier is answered here by setting a flag, exactly as the
    // destination answers it by sending `Durable`.

    /// Answer a terminal barrier by raising `answered`.
    fn terminal(answered: &Arc<AtomicBool>) -> Box<dyn FnOnce() + Send> {
        let answered = Arc::clone(answered);
        Box::new(move || answered.store(true, Ordering::Release))
    }

    /// Whether the barrier has been answered.
    fn answered(flag: &Arc<AtomicBool>) -> bool {
        flag.load(Ordering::Acquire)
    }

    #[test]
    fn a_terminal_barrier_waits_for_the_last_snapshot_buffer() {
        let barrier = CopyBarrier::new();
        assert!(
            !barrier.note_copy_batch(),
            "the first copy finds the barrier already up"
        );
        barrier.note_buffer_queued();
        barrier.note_buffer_queued();

        let flag = Arc::new(AtomicBool::new(false));
        assert!(
            !barrier.hold_terminal(terminal(&flag)),
            "the barrier stays up, so the reader's own flush reports it"
        );
        assert!(!answered(&flag), "two buffers are still queued");

        assert!(
            barrier.note_buffers_flushed(1),
            "the barrier is held while a buffer is left"
        );
        assert!(!answered(&flag));
        assert!(barrier.copy_open());

        assert!(
            !barrier.note_buffers_flushed(1),
            "the last buffer answers the barrier"
        );
        assert!(answered(&flag));
        assert!(!barrier.copy_open());
    }

    #[test]
    fn a_terminal_barrier_with_nothing_queued_is_answered_at_once() {
        // What an empty source table, or one whose rows the circuit already
        // holds, looks like from here.
        let barrier = CopyBarrier::new();
        barrier.note_copy_batch();

        let flag = Arc::new(AtomicBool::new(false));
        assert!(
            barrier.hold_terminal(terminal(&flag)),
            "the barrier came down away from the reader, which must be stepped \
             for it to report the copy done"
        );
        assert!(answered(&flag));
        assert!(!barrier.copy_open());
    }

    #[test]
    fn a_copy_that_starts_over_raises_the_barrier_again() {
        let barrier = CopyBarrier::new();
        barrier.note_copy_batch();
        barrier.note_buffer_queued();
        let first = Arc::new(AtomicBool::new(false));
        assert!(!barrier.hold_terminal(terminal(&first)));
        barrier.note_buffers_flushed(1);
        assert!(answered(&first));
        assert!(!barrier.copy_open());

        // etl abandons the copy it recorded and reads the table again.
        assert!(
            barrier.note_copy_batch(),
            "a batch that arrives after a copy closed starts a new one"
        );
        assert!(barrier.copy_open());

        barrier.note_buffer_queued();
        let second = Arc::new(AtomicBool::new(false));
        assert!(!barrier.hold_terminal(terminal(&second)));
        assert!(!answered(&second), "the second copy waits on its own rows");
        barrier.note_buffers_flushed(1);
        assert!(answered(&second));
        assert!(!barrier.copy_open());
    }

    #[test]
    fn another_tables_copy_is_no_business_of_the_barrier() {
        let columns = vec!["id".to_string()];
        // A table this connector does not read, rows and terminal write alike.
        assert!(matches!(
            classify_copy_write(None, true),
            CopyWrite::OtherTable
        ));
        assert!(matches!(
            classify_copy_write(None, false),
            CopyWrite::OtherTable
        ));
        // The source table: rows carry a batch id, the terminal barrier does
        // not.
        assert!(matches!(
            classify_copy_write(Some(columns.clone()), true),
            CopyWrite::TargetBatch(names) if names == columns
        ));
        assert!(matches!(
            classify_copy_write(Some(columns), false),
            CopyWrite::TargetBarrier
        ));
    }

    // -----------------------------------------------------------------------
    // parse_pg_uri unit tests
    // -----------------------------------------------------------------------

    fn parse_uri(uri: &str) -> AnyResult<PgConnectionConfig> {
        parse_pg_uri(uri, &PostgresTlsConfig::default(), "test")
    }

    #[test]
    fn test_parse_pg_uri_basic() {
        let config = parse_uri("postgres://user:pass@localhost:5432/mydb").unwrap();
        assert_eq!(config.host, "localhost");
        assert_eq!(config.port, 5432);
        assert_eq!(config.username, "user");
        assert!(config.password.is_some());
        assert_eq!(config.name, "mydb");
    }

    #[test]
    fn test_parse_pg_uri_default_port() {
        let config = parse_uri("postgres://user:pass@host.example.com/testdb").unwrap();
        assert_eq!(config.port, 5432);
        assert_eq!(config.host, "host.example.com");
    }

    #[test]
    fn test_parse_pg_uri_no_password() {
        let config = parse_uri("postgres://user@localhost/mydb").unwrap();
        assert!(config.password.is_none());
    }

    #[test]
    fn test_parse_pg_uri_missing_username() {
        let result = parse_uri("postgres://localhost/mydb");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_pg_uri_missing_database() {
        let result = parse_uri("postgres://user:pass@localhost");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_pg_uri_custom_port() {
        let config = parse_uri("postgres://user:pass@db.host:15432/mydb").unwrap();
        assert_eq!(config.port, 15432);
    }

    #[test]
    fn test_parse_pg_uri_invalid_scheme() {
        let result = parse_uri("not_a_uri");
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // Target table matching / column resolution tests
    // -----------------------------------------------------------------------

    #[test]
    fn unqualified_source_table_means_public() {
        assert!(is_target_table("orders", "public", "orders"));
        assert!(!is_target_table("orders", "public", "users"));
        // A publication can carry the same table name in several schemas, and
        // a bare name must not pull in every one of them.
        assert!(!is_target_table("orders", "sales", "orders"));
        assert!(!is_target_table("orders", "archive", "orders"));
    }

    #[test]
    fn qualified_source_table_matches_its_schema_only() {
        assert!(is_target_table("public.orders", "public", "orders"));
        assert!(!is_target_table("other.orders", "public", "orders"));
        assert!(is_target_table("sales.orders", "sales", "orders"));
        assert!(!is_target_table("sales.orders", "archive", "orders"));
    }

    #[test]
    fn quoted_source_table_matches_its_schema_only() {
        assert!(is_target_table("\"public\".\"orders\"", "public", "orders"));
        assert!(is_target_table("\"sales\".\"orders\"", "sales", "orders"));
        assert!(!is_target_table("\"other\".\"orders\"", "public", "orders"));
    }

    /// Mirrors `validate_columns`: every non-nullable Feldera column must exist
    /// (by name) in the Postgres source table.
    fn missing_required<'a>(pg_columns: &[&str], feldera_required: &[&'a str]) -> Vec<&'a str> {
        let pg: BTreeSet<&str> = pg_columns.iter().copied().collect();
        feldera_required
            .iter()
            .copied()
            .filter(|c| !pg.contains(c))
            .collect()
    }

    #[test]
    fn test_required_columns_present() {
        // All required present, different order -> valid.
        assert!(missing_required(&["name", "id"], &["id", "name"]).is_empty());
        // Extra Postgres column (extra) -> still valid.
        assert!(missing_required(&["id", "name", "extra"], &["id", "name"]).is_empty());
        // No required columns (all Feldera columns nullable) -> always valid.
        assert!(missing_required(&["id"], &[]).is_empty());
        // A required column absent from Postgres -> reported missing.
        assert_eq!(missing_required(&["id"], &["id", "name"]), vec!["name"]);
        // Renamed columns (source id,name vs required c0,c1) -> both missing.
        assert_eq!(
            missing_required(&["id", "name"], &["c0", "c1"]),
            vec!["c0", "c1"]
        );
    }

    // -----------------------------------------------------------------------
    // Destination-to-barrier glue
    // -----------------------------------------------------------------------
    //
    // The barrier tests above call the barrier directly, so they cannot see a
    // caller go missing. These drive the production destination and the queue
    // the reader flushes, and cross the two through the same two functions the
    // reader's Queue handler uses.

    /// A destination with a live queue and a running pipeline, sharing `copy`.
    fn destination(copy: &Arc<CopyBarrier>) -> FelderaDestination {
        let stream = MockDeZSet::<TestStruct, TestStruct>::new()
            .configure_deserializer(RecordFormat::Json(JsonFlavor::Datagen))
            .unwrap();
        let (_state_tx, pipeline_state_rx) = channel(PipelineState::Running);
        FelderaDestination {
            input_stream: Arc::new(Mutex::new(stream)),
            queue: Arc::new(InputQueue::new(Box::new(MockInputConsumer::new()))),
            source_table: "public.t".to_string(),
            endpoint_name: "cdc_in".to_string(),
            feldera_required_columns: Vec::new(),
            pending_senders: None,
            pipeline_state_rx,
            copy: Arc::clone(copy),
            consumer: Box::new(MockInputConsumer::new()),
        }
    }

    #[test]
    fn a_queued_snapshot_buffer_holds_the_terminal_barrier_until_the_reader_flushes_it() {
        let copy = Arc::new(CopyBarrier::new());
        let destination = destination(&copy);
        destination.push_snapshot_buffer((None, Vec::new()), Utc::now());
        destination.push_snapshot_buffer((None, Vec::new()), Utc::now());

        let flag = Arc::new(AtomicBool::new(false));
        assert!(
            !copy.hold_terminal(terminal(&flag)),
            "two buffers sit in the queue, so the barrier is held"
        );
        assert!(!answered(&flag));

        // The reader's flush, as the Queue handler performs it.
        let (_, _, flushed) = destination.queue.flush_with_aux();
        assert_eq!(count_snapshot_buffers(&flushed), 2);
        assert!(!copy.note_buffers_flushed(count_snapshot_buffers(&flushed)));
        assert!(answered(&flag), "the flush answered etl");
        assert!(!copy.copy_open());
    }

    #[test]
    fn a_flush_alone_moves_the_progress_clock() {
        let copy = CopyBarrier::new();
        let progress = |copy: &CopyBarrier| TableProgress {
            table_id: None,
            state: None,
            snapshot_buffers_queued: copy.snapshot_buffers_queued(),
            snapshot_buffers_flushed: copy.snapshot_buffers_flushed(),
        };
        copy.note_buffer_queued();
        let terminal_held = progress(&copy);
        // etl has handed over its last batch: nothing more is queued, and the
        // circuit drains what is. That must not read as a stall.
        copy.note_buffers_flushed(1);
        assert_ne!(terminal_held, progress(&copy));
    }

    #[test]
    fn the_reader_reports_a_barrier_while_the_copy_is_open() {
        assert!(matches!(resume_for(true, 7), Resume::Barrier));
        match resume_for(false, 7) {
            Resume::Seek { seek } => assert_eq!(seek, json!({ "pipeline_id": 7 })),
            _ => panic!("a finished copy resumes from a seek"),
        }
    }
}
