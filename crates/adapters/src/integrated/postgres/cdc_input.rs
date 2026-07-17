//! PostgreSQL change-data-capture input connector.
//!
//! [`PostgresCdcInputInner::worker_task_inner`] constructs and runs an `etl` [`Pipeline`] that
//! snapshots a PostgreSQL publication and then follows its logical replication stream. `etl`
//! persists table-copy phases and replication progress in the source database through its
//! [`PostgresStore`]. The connector forwards only the configured `source_table` to Feldera, even
//! when the publication contains other tables.
//!
//! # Data flow
//!
//! ```text
//! PostgreSQL snapshot and WAL
//!             |
//!             v
//!     etl Pipeline + PostgresStore
//!             |
//!             v
//! FelderaDestination (rows/events -> JSON inserts and deletes)
//!             |
//!             v
//! CdcInputQueue (parsed buffers + deferred acknowledgments)
//!             |
//!             v
//! PostgresCdcInputReader -> Feldera circuit step
//! ```
//!
//! [`FelderaDestination`] converts snapshot rows and replication events into Feldera input
//! records. It splits large writes into bounded buffers and pushes them into [`CdcInputQueue`].
//! When the controller requests input, [`PostgresCdcInputReader`] flushes up to the connector's
//! batch limit. Every deferred acknowledgment encountered in that flush is assigned to the same
//! circuit step, which has flushed all the data covered by each acknowledgment.
//!
//! # Acknowledgments and durability
//!
//! etl waits for each destination acknowledgment before it can continue. With completion
//! tracking, snapshot batches are accepted while their data accumulates in the queue.
//!
//! etl then sends an empty write that terminates a snapshot, which is queued behind that data
//! as a durability barrier; etl finishes the copy only after the barrier becomes durable.
//!
//! A streaming write's [`DeferredAck`] is attached to its final queue entry (each streaming write may
//! produce multiple queue entries). Once that entry is flushed, the completion watcher ties the
//! acknowledgment to the resulting circuit step. It reports `Durable` after the step completes in
//! fast mode, or after a checkpoint containing the step completes when fault tolerance is enabled.
//! This prevents the PostgreSQL replication slot from advancing beyond Feldera's completion frontier.
//!
//! A streaming acknowledgment marked `MayDefer` that waits longer than
//! `streaming_ack_hold_ms` may be reported as `Accepted`. This lets etl continue reading WAL
//! without advancing the replication slot; a later durable acknowledgment confirms the accepted
//! prefix. etl marks terminal writes as `RequireDurable`; those acknowledgments never time out and
//! wait for the completion frontier. While an acknowledgment remains queued, the reader requests
//! bounded steps so it does not wait for the controller's normal buffer timeout.
//!
//! # Lifecycle and errors
//!
//! Pausing the Feldera pipeline stops the destination from accepting new etl batches; already
//! queued records can still drain. [`TableStateMonitor`] turns non-retriable source-table errors,
//! such as unsupported source schema changes, into connector errors instead of allowing the input
//! to stall silently. Terminating or dropping the connector shuts down the etl pipeline and its
//! completion watcher.
//!
//! etl normally drains pending streaming writes during shutdown. If the completion watcher or
//! destination side disappears first, etl observes the dropped acknowledgment as a destination
//! error and may persist it in the table state. The same state can be left by a failed run in
//! which etl has enough time to observe the closed acknowledgment before the process exits. Since
//! Feldera never reported that write as [`DestinationWriteStatus::Durable`], the saved PostgreSQL
//! replication position does not include it and the write must be replayed. On startup,
//! `discard_shutdown_errors` therefore defaults to `true` and rolls back only this dropped-ack
//! error. Other persisted table errors remain intact unless `discard_table_errors` is enabled.
//!
//! # Transactions
//!
//! With `transaction_mode: snapshot`, initial synchronization is split into two Feldera
//! transactions: one for the PostgreSQL table copy and one for the WAL catchup. etl cannot start
//! WAL catchup until the table copy has been marked durable. With fault tolerance enabled, this
//! means the copy transaction must first be committed and checkpointed; the catchup transaction
//! can then run and become durable independently.
//!
//! The copy transaction commits at etl's terminal copy barrier, and the catchup transaction commits
//! with its terminal durable write. Sometimes there are no WAL records to catch up: the table-sync
//! worker is already at the handoff LSN, so etl moves directly to `SyncDone` without calling the
//! destination. No catchup transaction is opened in that case; the table state monitor only retires
//! the pending catchup phase. Writes from the main apply worker are not grouped into
//! connector-managed transactions, so steady-state CDC continues normally.

use crate::transport::{
    InputEndpoint, InputQueue, InputQueueEntry, InputReaderCommand, IntegratedInputEndpoint,
    NonFtInputReaderCommand,
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
    Destination, DestinationWriteStatus, DropTableForCopyResult, WriteEventsDurability,
    WriteEventsResult, WriteTableRowsResult,
};
use etl::error::{ErrorKind, EtlResult};
use etl::etl_error;
use etl::event::Event;
use etl::pipeline::{Pipeline, ShutdownTx};
use etl::schema::{ReplicatedTableSchema, TableId};
use etl::store::{PostgresStore, SchemaStore, StateStore, TableRetryPolicy, TableState};
use feldera_adapterlib::catalog::{DeCollectionStream, InputCollectionHandle};
use feldera_adapterlib::format::{InputBuffer, ParseError};
use feldera_adapterlib::transport::{Resume, Watermark};
use feldera_types::config::FtModel;
use feldera_types::coordination::Completion;
use feldera_types::format::json::JsonFlavor;
use feldera_types::transport::postgres::{
    PostgresCdcReaderConfig, PostgresCdcTransactionMode, PostgresTlsConfig,
};
use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::future::pending;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use tokio::select;
use tokio::sync::mpsc;
use tokio::sync::watch::{Receiver, Sender, channel};
use tokio::time::{Instant as TokioInstant, sleep_until};
use tracing::{debug, error, info, warn};
use url::Url;
use xxhash_rust::xxh3::xxh3_64;

use super::tls::make_etl_tls_config;

const DROPPED_DESTINATION_ACK_ERROR: &str =
    "[DestinationError] Async result channel closed before sending";
const MAX_ERROR_ROLLBACKS_PER_TABLE: usize = 32;
const MAX_QUEUED_BUFFER_BYTES: usize = 2 * 1024 * 1024;

/// An etl write acknowledgment deferred until Feldera has processed the data it covers.
/// The ack is tagged by write type because snapshot and stream acks follow different rules.
enum DeferredAck {
    /// etl sends an empty table-copy write once we've accepted the snapshot batches.
    /// We need to keep it around to mark that the snapshot has now become durable.
    /// Answering `Accepted` on this empty table-copy write is illegal.
    SnapshotCopyBarrier(WriteTableRowsResult),
    /// Each `write_events` call may produce multiple smaller batches; we attach
    /// this ack to the last one. `write_events` fires both during a table's
    /// post-copy catchup and in steady-state streaming.
    ///
    /// We answer `Durable` as soon as the completion frontier passes it. A
    /// `MayDefer` write may instead be answered `Accepted` after
    /// `streaming_ack_hold_ms`; a `RequireDurable` write waits for the frontier.
    Stream {
        result: WriteEventsResult,
        durability: WriteEventsDurability,
    },
    #[cfg(test)]
    Test {
        kind: TestAckKind,
        tx: std::sync::mpsc::Sender<TestAckStatus>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg(test)]
enum TestAckKind {
    SnapshotCopyBarrier,
    StreamMayDefer,
    StreamRequireDurable,
}

impl DeferredAck {
    /// Report the ack as `Durable`, letting etl advance the replication slot
    /// or finish the table copy.
    fn complete(self) {
        match self {
            Self::SnapshotCopyBarrier(result) | Self::Stream { result, .. } => {
                result.send(Ok(DestinationWriteStatus::Durable))
            }
            #[cfg(test)]
            Self::Test { tx, .. } => {
                let _ = tx.send(TestAckStatus::Durable);
            }
        }
    }

    /// Report a stream ack as `Accepted`: Feldera has accepted the rows for processing,
    /// but they are not durable yet, so etl may keep reading WAL without moving the slot.
    fn accept(self) {
        match self {
            Self::Stream {
                result,
                durability: WriteEventsDurability::MayDefer,
            } => result.send(Ok(DestinationWriteStatus::Accepted)),
            Self::Stream {
                durability: WriteEventsDurability::RequireDurable,
                ..
            } => unreachable!("a required-durability stream ack is never accepted"),
            Self::SnapshotCopyBarrier(_) => {
                unreachable!("the snapshot copy barrier is never accepted")
            }
            #[cfg(test)]
            Self::Test {
                kind: TestAckKind::StreamMayDefer,
                tx,
            } => {
                let _ = tx.send(TestAckStatus::Accepted);
            }
            #[cfg(test)]
            Self::Test { .. } => {
                unreachable!("a non-deferrable ack is never accepted")
            }
        }
    }

    fn is_stream(&self) -> bool {
        match self {
            Self::SnapshotCopyBarrier(_) => false,
            Self::Stream { .. } => true,
            #[cfg(test)]
            Self::Test { kind, .. } => matches!(
                kind,
                TestAckKind::StreamMayDefer | TestAckKind::StreamRequireDurable
            ),
        }
    }

    /// Whether etl permits this stream write to complete as `Accepted`.
    fn may_defer(&self) -> bool {
        match self {
            Self::SnapshotCopyBarrier(_) => false,
            Self::Stream { durability, .. } => *durability == WriteEventsDurability::MayDefer,
            #[cfg(test)]
            Self::Test { kind, .. } => *kind == TestAckKind::StreamMayDefer,
        }
    }
}

/// Deferred etl acks stored as auxiliary data on a queue entry.
/// Each ack answers one etl destination call.
type DeferredSenders = Vec<DeferredAck>;

/// FIFO input queue for parsed CDC data. Each entry carries [`DeferredSenders`]
/// as auxiliary data; see [`DeferredAck`] for what each variant answers and when.
///
/// One etl write may be parsed into several queue entries but has at most one
/// ack, so most entries carry none; the ack sits on the write's final entry.
///
/// ```text
/// etl write([e1, e2, e3, e4])
///     |
///     +--> queue entry: [e1, e2]  acks: []
///     +--> queue entry: [e3]      acks: []
///     `--> queue entry: [e4]      acks: [ack]
///
/// FIFO flush order: entry 1 -> entry 2 -> entry 3 + ack
/// ```
///
/// That positioning is what lets [`PostgresCdcInputReader::request`] hand each
/// ack to a step that flushed everything it covers. A bounded flush may cross
/// several ack boundaries; all of those acks share the same step completion.
type CdcInputQueue = InputQueue<DeferredSenders>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg(test)]
enum TestAckStatus {
    Accepted,
    Durable,
}

struct PendingAcks {
    /// `total_completed_steps` observed right after the queue flush. The
    /// flushed data lands in the next step, so these acks are done once
    /// `total_completed_steps >= step_at_flush + 1` (or the corresponding
    /// checkpoint frontier reaches that step in strict mode).
    step_at_flush: u64,
    /// When the acks were assigned to a step. The stream ack hold deadline
    /// counts from here.
    held_since: Instant,
    senders: DeferredSenders,
}

/// Two ways an ack reaches the completion watcher.
///
/// A stream ack that times out as `Accepted` has no further data of its own
/// to confirm it durable; only a later write can do that. A `write_events`
/// call whose events all get filtered out (e.g. a change to another table in
/// the publication, or an update we cannot reconstruct) is our chance to do
/// so, but it queues no Feldera rows and so never goes through the input
/// queue: it never gets a `step_at_flush` to anchor it. This variant gives it
/// a path to the watcher without one.
enum CompletionMessage {
    /// A write that queued data. Resolved against the `step_at_flush` on
    /// [`PendingAcks`].
    Queued(PendingAcks),
    /// A stream write that produced no Feldera rows: there is no queue entry,
    /// so no step to wait on. But it is still an ack we can answer, and
    /// answering it `Durable` means the completion frontier has caught up with
    /// everything before it, including any earlier stream ack that timed out
    /// as `Accepted`, so it doubles as confirmation for that data too.
    NoRowStream(DeferredAck),
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
        _resume_info: Option<serde_json::Value>,
    ) -> AnyResult<Box<dyn InputReader>> {
        Ok(Box::new(PostgresCdcInputReader::new(
            &self.inner,
            input_handle,
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
    ) -> AnyResult<Self> {
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
                // Flush the queue to the circuit, collecting timestamps for
                // watermarks and every ack encountered by this bounded flush.
                // Each ack sits on the final entry of the data it covers, so
                // all collected acks can share this step's completion frontier.
                let (buffer_size, _hasher, flushed) = self.inner.queue.flush_with_aux();

                let mut watermarks = Vec::new();
                let mut senders = Vec::new();
                for (ts, mut flushed_senders) in flushed {
                    watermarks.push(Watermark::new(ts, None));
                    senders.append(&mut flushed_senders);
                }

                self.inner
                    .queued_acks
                    .fetch_sub(senders.len(), Ordering::Release);

                // Build resume metadata so Feldera can checkpoint our position.
                // The actual resume state is managed by etl's PostgresStore;
                // we just need a stable identifier so the controller knows we
                // support resumption.
                let resume_metadata = json!({
                    "pipeline_id": self.inner.pipeline_id,
                });
                let resume = Resume::Seek {
                    seek: resume_metadata,
                };

                // Report data to controller with resume metadata (must be
                // called exactly once per Queue command).
                self.inner
                    .consumer
                    .extended(buffer_size, Some(resume), watermarks);

                if !senders.is_empty() {
                    if let Some(tx) = self.inner.completion_task_tx.as_ref() {
                        // The data we just flushed lands in the next step, so
                        // remember how many steps have completed right now.
                        // The watcher answers these acks once the frontier
                        // passes this value: after the step itself in fast
                        // mode, after its checkpoint in strict mode.
                        let step_at_flush = self
                            .inner
                            .step_completion_rx
                            .as_ref()
                            .map(|rx| rx.borrow().total_completed_steps)
                            .unwrap_or(0);
                        let _ = tx.send(CompletionMessage::Queued(PendingAcks {
                            step_at_flush,
                            held_since: Instant::now(),
                            senders,
                        }));
                    } else {
                        // No completion tracking: complete immediately.
                        for sender in senders {
                            sender.complete();
                        }
                    }
                }

                // etl blocks on each write's ack before it can proceed, so an
                // ack stuck in the queue stalls replication. That can happen:
                // a flush may stop at `max_batch_size` before reaching the
                // ack's entry. If the leftover tail is smaller than
                // `min_batch_size_records`, the controller may wait until the
                // buffer timeout before scheduling another step, stalling
                // replication in the meantime. So while any ack is still queued,
                // keep requesting a step.
                if self.inner.queued_acks.load(Ordering::Acquire) > 0 {
                    self.inner.consumer.request_step();
                }
            }
            NonFtInputReaderCommand::Transition(state) => {
                if state == PipelineState::Terminated {
                    self.inner.shutdown_etl_pipeline();
                }
                let _ = self.sender.send_replace(state);
            }
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

struct PostgresCdcInputInner {
    endpoint_name: String,
    config: PostgresCdcReaderConfig,
    consumer: Box<dyn InputConsumer>,
    queue: Arc<CdcInputQueue>,
    /// Deterministic pipeline ID used for replication slot naming and resume.
    pipeline_id: u64,
    /// How many `DeferredAck`s are sitting in `queue`, waiting to be flushed.
    /// While nonzero, the reader keeps requesting bounded steps; otherwise an
    /// ack behind a tail smaller than `min_batch_size_records` could sit there
    /// forever with etl blocked on it.
    queued_acks: Arc<AtomicUsize>,
    /// Watch receiver for step completion, used to capture `step_at_flush` in
    /// the Queue handler. Always tracks `total_completed_steps`.
    step_completion_rx: Option<tokio::sync::watch::Receiver<Completion>>,
    /// Watcher source for the background task. Taken once by `worker_task_inner`.
    /// `Strict` when fault tolerance is enabled (gates slot on checkpoint);
    /// `Fast` otherwise (gates slot on step completion).
    watcher_rx: Mutex<Option<WatcherReceiver>>,
    /// Sender for passing pending acks to the background task.
    /// Created at construction time if completion tracking is available.
    completion_task_tx: Option<mpsc::UnboundedSender<CompletionMessage>>,
    /// Receiver half, taken once by worker_task_inner to spawn the background task.
    completion_task_rx: Mutex<Option<mpsc::UnboundedReceiver<CompletionMessage>>>,
    /// etl shutdown handle for the currently running pipeline.
    /// Used to stop etl workers when Feldera terminates the connector.
    etl_shutdown_tx: Mutex<Option<ShutdownTx>>,
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
            queued_acks: Arc::new(AtomicUsize::new(0)),
            step_completion_rx,
            watcher_rx: Mutex::new(watcher_rx),
            completion_task_tx,
            completion_task_rx: Mutex::new(completion_task_rx),
            etl_shutdown_tx: Mutex::new(None),
        }
    }

    async fn worker_task(
        self: Arc<Self>,
        input_stream: Box<dyn DeCollectionStream>,
        feldera_required_columns: Vec<String>,
        receiver: Receiver<PipelineState>,
        init_status_sender: tokio::sync::oneshot::Sender<Result<(), ControllerError>>,
    ) {
        self.clone()
            .worker_task_inner(
                input_stream,
                feldera_required_columns,
                receiver,
                init_status_sender,
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
            batch: BatchConfig::default(),
            table_error_retry_delay_ms: PipelineConfig::DEFAULT_TABLE_ERROR_RETRY_DELAY_MS,
            table_error_retry_max_attempts: PipelineConfig::DEFAULT_TABLE_ERROR_RETRY_MAX_ATTEMPTS,
            max_table_sync_workers: PipelineConfig::DEFAULT_MAX_TABLE_SYNC_WORKERS,
            max_copy_connections_per_table: PipelineConfig::DEFAULT_MAX_COPY_CONNECTIONS_PER_TABLE,
            memory_refresh_interval_ms: PipelineConfig::DEFAULT_MEMORY_REFRESH_INTERVAL_MS,
            replication_lag_refresh_interval_ms:
                PipelineConfig::DEFAULT_REPLICATION_LAG_REFRESH_INTERVAL_MS,
            memory_backpressure: Some(MemoryBackpressureConfig::default()),
            table_sync_copy: TableSyncCopyConfig::IncludeAllTables,
            invalidated_slot_behavior: InvalidatedSlotBehavior::default(),
            run_source_migrations: true,
        };

        // Persist table phases and slot progress across restarts. Once a table
        // copy completes, this avoids repeating it on an ordinary restart.
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

        let discard_all_errors = self.config.discard_table_errors;
        if discard_all_errors || self.config.discard_shutdown_errors {
            let discard_result = async {
                store.load_table_states().await?;
                store.load_table_schemas().await?;
                discard_matching_table_errors(
                    &self.endpoint_name,
                    &self.config.source_table,
                    &store,
                    discard_all_errors,
                )
                .await
            }
            .await;
            if let Err(e) = discard_result {
                let _ = init_status_sender.send(Err(ControllerError::input_transport_error(
                    &self.endpoint_name,
                    true,
                    anyhow!("failed to discard persisted etl table errors: {e}"),
                )));
                return;
            }
        }

        let destination = FelderaDestination::new(
            input_stream,
            self.endpoint_name.clone(),
            self.config.source_table.clone(),
            self.config.transaction_mode,
            self.pipeline_id,
            Arc::clone(&self.queue),
            store.clone(),
            feldera_required_columns,
            self.completion_task_tx.clone(),
            Arc::clone(&self.queued_acks),
            receiver.clone(),
        );
        let snapshot_transactions = destination.snapshot_transactions();

        let table_state_monitor = TableStateMonitor {
            endpoint_name: self.endpoint_name.clone(),
            source_table: self.config.source_table.clone(),
            consumer: self.consumer.clone(),
            store: store.clone(),
            snapshot_transactions,
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
                Duration::from_millis(self.config.streaming_ack_hold_ms),
            ))),
            _ => None,
        };

        // Run the pipeline alongside a watcher for non-retriable source-table
        // errors. etl marks a table errored (e.g. on a source schema change)
        // without failing the whole pipeline, so `pipeline.wait` would block
        // forever while the input silently stalls; the watcher reports such an
        // error so the controller fails the endpoint instead.
        let mut receiver_clone = receiver.clone();
        let mut pipeline_wait = Box::pin(pipeline.wait());
        let (pipeline_result, report_error) = select! {
            result = &mut pipeline_wait => (result, true),
            _ = receiver_clone.wait_for(|state| state == &PipelineState::Terminated) => {
                debug!(
                    "postgres_cdc {}: received termination command; shutting down etl pipeline",
                    &self.endpoint_name
                );
                self.shutdown_etl_pipeline();
                abort_completion_watcher(&mut completion_handle).await;
                (pipeline_wait.as_mut().await, false)
            }
            _ = table_state_monitor.run() => {
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

/// Roll back matching persisted `Errored` states for the configured source
/// table before starting etl. Errored states can stack, so keep rolling back
/// until another state surfaces. If rollback fails or reaches the rollback
/// limit, reset the table to `Init`.
async fn discard_matching_table_errors(
    endpoint_name: &str,
    source_table: &str,
    store: &PostgresStore,
    discard_all: bool,
) -> EtlResult<()> {
    let Some(table_id) = target_table_id(store, source_table).await? else {
        return Ok(());
    };
    let Some(state) = store.get_table_state(table_id).await? else {
        return Ok(());
    };
    if !should_discard_table_error(&state, discard_all) {
        return Ok(());
    }

    warn!(
        "postgres_cdc {}: discarding persisted etl {} error for table {table_id} before startup",
        endpoint_name,
        if discard_all { "table" } else { "shutdown" },
    );

    let mut discarded = 0usize;
    let mut state = state;
    while should_discard_table_error(&state, discard_all) {
        if discarded == MAX_ERROR_ROLLBACKS_PER_TABLE {
            warn!(
                "postgres_cdc {}: table {table_id} still has a matching etl error after \
                 {MAX_ERROR_ROLLBACKS_PER_TABLE} rollbacks; resetting table state to init",
                endpoint_name
            );
            store.update_table_state(table_id, TableState::Init).await?;
            break;
        }

        match store.rollback_table_state(table_id).await {
            Ok(restored_state) => {
                discarded += 1;
                info!(
                    "postgres_cdc {}: discarded etl table error for table {table_id}, \
                             restored previous state {restored_state}",
                    endpoint_name
                );
                state = restored_state;
            }
            Err(e) => {
                warn!(
                    "postgres_cdc {}: failed to roll back etl table error for table \
                             {table_id}: {e}; resetting table state to init",
                    endpoint_name
                );
                store.update_table_state(table_id, TableState::Init).await?;
                discarded += 1;
                break;
            }
        }
    }

    debug!(
        "postgres_cdc {}: discarded {discarded} etl table error state(s) for table {table_id}",
        endpoint_name
    );

    Ok(())
}

fn should_discard_table_error(state: &TableState, discard_all: bool) -> bool {
    match state {
        TableState::Errored { reason, .. } => {
            discard_all || reason == DROPPED_DESTINATION_ACK_ERROR
        }
        _ => false,
    }
}

impl Drop for PostgresCdcInputInner {
    fn drop(&mut self) {
        self.shutdown_etl_pipeline();
    }
}

/// Monitor the configured source table's etl state.
struct TableStateMonitor {
    endpoint_name: String,
    source_table: String,
    consumer: Box<dyn InputConsumer>,
    store: PostgresStore,
    snapshot_transactions: Arc<SnapshotTransactions>,
}

impl TableStateMonitor {
    /// Retire an idle catchup phase and surface non-retriable source-table
    /// errors as fatal endpoint errors.
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
    ///
    /// etl can enter catchup with its current LSN already at the target. It then
    /// moves to `SyncDone` with an empty batch, so there is no destination write
    /// to retire the pending catchup phase.
    async fn run(self) {
        const POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

        loop {
            tokio::time::sleep(POLL_INTERVAL).await;

            let states = match self.store.get_table_states().await {
                Ok(states) => states,
                Err(e) => {
                    debug!(
                        "postgres_cdc {}: failed to read table replication states: {e}",
                        &self.endpoint_name
                    );
                    continue;
                }
            };

            let table_id = match target_table_id(&self.store, &self.source_table).await {
                Ok(Some(table_id)) => table_id,
                Ok(None) => continue,
                Err(e) => {
                    debug!(
                        "postgres_cdc {}: failed to resolve source table state: {e}",
                        &self.endpoint_name
                    );
                    continue;
                }
            };
            let Some(state) = states.get(&table_id) else {
                continue;
            };

            if matches!(state, TableState::SyncDone { .. } | TableState::Ready) {
                self.snapshot_transactions
                    .finish_catchup_after_etl_completion(table_id);
            }

            let TableState::Errored {
                reason,
                solution,
                retry_policy,
                ..
            } = state
            else {
                continue;
            };

            // A timed retry clears on its own; leave it to etl.
            if matches!(retry_policy, TableRetryPolicy::TimedRetry { .. }) {
                continue;
            }

            let detail = match solution {
                Some(solution) => format!("{reason} ({solution})"),
                None => reason.clone(),
            };
            error!(
                "postgres_cdc {}: table {table_id} replication errored: {detail}",
                &self.endpoint_name
            );
            self.consumer.error(
                true,
                anyhow!("postgres replication error on table {table_id}: {detail}"),
                None,
            );
            return;
        }
    }
}

enum SnapshotPhase {
    /// Phase is unknown until startup reconciles etl's durable table state.
    Uninitialized,
    /// Transactions are disabled, or initial sync is already complete.
    Inactive,
    /// Initial table COPY.
    Copy {
        started: bool,
        table_id: Option<TableId>,
    },
    /// WAL catchup after a durable COPY.
    Catchup { started: bool, table_id: TableId },
}

impl SnapshotPhase {
    fn start_copy(&mut self, table_id: TableId) -> bool {
        let Self::Copy {
            started,
            table_id: copy_table_id,
        } = self
        else {
            return false;
        };

        *copy_table_id = Some(table_id);
        if *started {
            false
        } else {
            *started = true;
            true
        }
    }

    fn finish_copy(&mut self, table_id: TableId) -> bool {
        let Self::Copy {
            started,
            table_id: copy_table_id,
        } = self
        else {
            return false;
        };

        debug_assert!(copy_table_id.is_none_or(|id| id == table_id));
        let commit = *started;
        *self = Self::Catchup {
            started: false,
            table_id,
        };
        commit
    }

    fn start_catchup(&mut self, table_id: Option<TableId>, has_data: bool) -> bool {
        let Self::Catchup {
            started,
            table_id: catchup_table_id,
        } = self
        else {
            return false;
        };

        if table_id != Some(*catchup_table_id) || !has_data || *started {
            false
        } else {
            *started = true;
            true
        }
    }

    fn finish_catchup(&mut self) -> bool {
        let Self::Catchup { started, .. } = self else {
            return false;
        };

        let commit = *started;
        *self = Self::Inactive;
        commit
    }
}

/// Shares snapshot transaction state between etl workers and the table-state
/// monitor.
///
/// The COPY transaction starts when the first row buffer is queued, so an empty
/// table does not create a transaction. On restart, `Destination::startup`
/// reads etl's table state, which is persisted in the source Postgres database,
/// to restore whether this connector is copying, catching up, or already live.
struct SnapshotTransactions {
    source_table: String,
    transaction_mode: PostgresCdcTransactionMode,
    pipeline_id: u64,
    queue: Arc<CdcInputQueue>,
    startup_store: Mutex<Option<PostgresStore>>,
    phase: Mutex<SnapshotPhase>,
}

impl SnapshotTransactions {
    fn new(
        source_table: String,
        transaction_mode: PostgresCdcTransactionMode,
        pipeline_id: u64,
        queue: Arc<CdcInputQueue>,
        store: PostgresStore,
    ) -> Self {
        Self {
            source_table,
            transaction_mode,
            pipeline_id,
            queue,
            startup_store: Mutex::new(Some(store)),
            phase: Mutex::new(SnapshotPhase::Uninitialized),
        }
    }

    async fn initialize(&self) -> EtlResult<()> {
        let store = self.startup_store.lock().unwrap().take().ok_or_else(|| {
            etl_error!(
                ErrorKind::DestinationError,
                "Postgres CDC destination initialized more than once"
            )
        })?;
        let phase = self.initial_phase(&store).await?;
        *self.phase.lock().unwrap() = phase;
        Ok(())
    }

    /// Restore the transaction phase after a connector restart. This is the
    /// only path that can enter catchup without finishing COPY in this process.
    async fn initial_phase(&self, store: &PostgresStore) -> EtlResult<SnapshotPhase> {
        if self.transaction_mode == PostgresCdcTransactionMode::None {
            return Ok(SnapshotPhase::Inactive);
        }

        let table_id = target_table_id(store, &self.source_table).await?;
        let Some(table_id) = table_id else {
            // On the first run etl has not stored the source schema yet. The
            // first write_table_rows call supplies the table id.
            return Ok(SnapshotPhase::Copy {
                started: false,
                table_id: None,
            });
        };

        let state = store.get_table_state(table_id).await?;
        Ok(match state {
            None | Some(TableState::Init | TableState::DataSync) => SnapshotPhase::Copy {
                started: false,
                table_id: Some(table_id),
            },
            Some(
                TableState::FinishedCopy | TableState::SyncWait { .. } | TableState::Catchup { .. },
            ) => SnapshotPhase::Catchup {
                started: false,
                table_id,
            },
            Some(TableState::SyncDone { .. } | TableState::Ready | TableState::Errored { .. }) => {
                SnapshotPhase::Inactive
            }
        })
    }

    fn lock_phase(&self) -> std::sync::MutexGuard<'_, SnapshotPhase> {
        let phase = self.phase.lock().unwrap();
        assert!(
            !matches!(&*phase, SnapshotPhase::Uninitialized),
            "etl calls destination startup before submitting writes"
        );
        phase
    }

    /// Start the COPY transaction on its first queued row buffer.
    fn start_copy(&self, table_id: TableId) -> Option<Option<String>> {
        self.lock_phase()
            .start_copy(table_id)
            .then(|| Some(self.transaction_label("copy")))
    }

    fn finish_copy(&self, table_id: TableId) -> bool {
        self.lock_phase().finish_copy(table_id)
    }

    fn start_catchup(&self, table_id: Option<TableId>, has_data: bool) -> Option<Option<String>> {
        self.lock_phase()
            .start_catchup(table_id, has_data)
            .then(|| Some(self.transaction_label("catchup")))
    }

    fn finish_catchup(&self) -> bool {
        self.lock_phase().finish_catchup()
    }

    /// Finish catchup when etl reaches `SyncDone` without sending a terminal
    /// destination write.
    ///
    /// etl can take this path when idle progress reaches the catchup target:
    /// <https://github.com/supabase/etl/pull/902#discussion_r3600970636>
    fn finish_catchup_after_etl_completion(&self, table_id: TableId) {
        let mut phase = self.phase.lock().unwrap();
        let should_finish = match &*phase {
            SnapshotPhase::Catchup {
                table_id: catchup_table_id,
                ..
            } => *catchup_table_id == table_id,
            _ => false,
        };
        let commit = should_finish && phase.finish_catchup();
        if commit {
            // Earlier catchup writes opened a transaction, but etl reached
            // SyncDone without a terminal write that could close it.
            self.queue.push_entry(
                InputQueueEntry::new_with_aux(Utc::now(), DeferredSenders::new())
                    .with_commit_transaction(true),
                Vec::new(),
            );
        }
    }

    /// Labels are intentionally stable across connector restarts: each
    /// pipeline has at most one copy and one catchup transaction.
    fn transaction_label(&self, phase: &str) -> String {
        format!("snapshot-{phase}-{}", self.pipeline_id)
    }
}

/// etl Destination implementation that pushes data into a Feldera DeCollectionStream.
#[derive(Clone)]
struct FelderaDestination {
    input_stream: Arc<Mutex<Box<dyn DeCollectionStream>>>,
    queue: Arc<CdcInputQueue>,
    source_table: String,
    endpoint_name: String,
    snapshot_transactions: Arc<SnapshotTransactions>,
    /// Canonical names of the non-nullable Feldera columns. Each must be present
    /// (by name) in the target Postgres table schema etl passes with each target
    /// batch/event. Nullable and extra columns need not match.
    feldera_required_columns: Vec<String>,
    /// Sends deferred acks to the completion watcher. When absent, writes are
    /// acked immediately.
    completion_task_tx: Option<mpsc::UnboundedSender<CompletionMessage>>,
    /// How many `DeferredAck`s are sitting in the queue, waiting to be
    /// flushed. See [`PostgresCdcInputInner::queued_acks`].
    queued_acks: Arc<AtomicUsize>,
    /// Pipeline state receiver used to stop accepting new etl batches while the
    /// Feldera pipeline is paused.
    pipeline_state_rx: Receiver<PipelineState>,
}

impl Destination for FelderaDestination {
    fn name() -> &'static str {
        "feldera"
    }

    async fn startup(&self) -> EtlResult<()> {
        self.snapshot_transactions.initialize().await
    }

    async fn drop_table_for_copy(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        async_result: DropTableForCopyResult<()>,
    ) -> EtlResult<()> {
        self.wait_unpaused().await?;

        // Feldera owns no physical destination object to drop; the data lives in
        // the circuit. A copy restart simply re-snapshots through
        // `write_table_rows`, so there is nothing to remove here.
        warn!(
            "postgres_cdc {}: drop_table_for_copy called for table '{}', ignoring",
            &self.endpoint_name,
            replicated_table_schema.name()
        );
        async_result.send(Ok(()));
        Ok(())
    }

    async fn write_table_rows(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        table_rows: Vec<TableRow>,
        async_result: WriteTableRowsResult,
    ) -> EtlResult<()> {
        self.wait_unpaused().await?;

        // A different table in the publication resolves to `None` and is skipped.
        let column_names = match self.column_names_for_target_schema(replicated_table_schema)? {
            Some(columns) => columns,
            None => {
                async_result.send(Ok(DestinationWriteStatus::Durable));
                return Ok(());
            }
        };

        let mut stream = self.input_stream.lock().unwrap();
        let mut bytes = 0;
        let mut errors = Vec::new();
        let mut queued_data = false;
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

            if bytes >= MAX_QUEUED_BUFFER_BYTES {
                self.push_queue_entry(
                    (stream.take_all(), errors),
                    timestamp,
                    DeferredSenders::new(),
                    self.snapshot_transactions
                        .start_copy(replicated_table_schema.id()),
                    false,
                );
                queued_data = true;
                bytes = 0;
                errors = Vec::new();
            }
        }

        if bytes > 0 || !errors.is_empty() {
            self.push_queue_entry(
                (stream.take_all(), errors),
                timestamp,
                DeferredSenders::new(),
                self.snapshot_transactions
                    .start_copy(replicated_table_schema.id()),
                false,
            );
            queued_data = true;
        }

        let commit_transaction = !queued_data
            && self
                .snapshot_transactions
                .finish_copy(replicated_table_schema.id());
        self.ack_snapshot_copy(queued_data, commit_transaction, async_result);
        Ok(())
    }

    async fn write_events(
        &self,
        events: Vec<Event>,
        durability: WriteEventsDurability,
        async_result: WriteEventsResult,
    ) -> EtlResult<()> {
        self.wait_unpaused().await?;

        let target_table_id = self.target_table_id_for_events(&events);
        let mut stream = self.input_stream.lock().unwrap();
        let mut bytes = 0;
        let mut errors = Vec::new();
        let mut queue_entries = Vec::new();
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
                // table errored; we surface that via `TableStateMonitor`.
                Event::Relation(_) => {}
                Event::Begin(_) | Event::Commit(_) | Event::Unsupported => {}
            }

            if bytes >= MAX_QUEUED_BUFFER_BYTES {
                queue_entries.push((stream.take_all(), errors));
                bytes = 0;
                errors = Vec::new();
            }
        }

        if bytes > 0 || !errors.is_empty() {
            queue_entries.push((stream.take_all(), errors));
        }
        let ack = DeferredAck::Stream {
            result: async_result,
            durability,
        };
        let terminal = durability == WriteEventsDurability::RequireDurable;
        let mut start_transaction = self
            .snapshot_transactions
            .start_catchup(target_table_id, !queue_entries.is_empty());
        let commit_transaction = terminal && self.snapshot_transactions.finish_catchup();

        if let Some(last_entry) = queue_entries.pop() {
            for entry in queue_entries {
                self.push_queue_entry(
                    entry,
                    timestamp,
                    DeferredSenders::new(),
                    start_transaction.take(),
                    false,
                );
            }
            self.queue_stream_ack(
                last_entry,
                timestamp,
                ack,
                start_transaction,
                commit_transaction,
            );
        } else if commit_transaction {
            // Catchup can end with only BEGIN/COMMIT events and no row buffer.
            // Queue an empty entry so the Feldera transaction still closes
            // after all preceding catchup data.
            self.queue_stream_ack((None, Vec::new()), timestamp, ack, None, true);
        } else {
            self.ack_no_row_stream_write(ack);
        }

        Ok(())
    }
}

impl FelderaDestination {
    #[allow(clippy::too_many_arguments)]
    fn new(
        input_stream: Box<dyn DeCollectionStream>,
        endpoint_name: String,
        source_table: String,
        transaction_mode: PostgresCdcTransactionMode,
        pipeline_id: u64,
        queue: Arc<CdcInputQueue>,
        store: PostgresStore,
        feldera_required_columns: Vec<String>,
        completion_task_tx: Option<mpsc::UnboundedSender<CompletionMessage>>,
        queued_acks: Arc<AtomicUsize>,
        pipeline_state_rx: Receiver<PipelineState>,
    ) -> Self {
        let snapshot_transactions = Arc::new(SnapshotTransactions::new(
            source_table.clone(),
            transaction_mode,
            pipeline_id,
            Arc::clone(&queue),
            store,
        ));
        Self {
            input_stream: Arc::new(Mutex::new(input_stream)),
            queue,
            source_table,
            endpoint_name,
            snapshot_transactions,
            feldera_required_columns,
            completion_task_tx,
            queued_acks,
            pipeline_state_rx,
        }
    }

    fn snapshot_transactions(&self) -> Arc<SnapshotTransactions> {
        Arc::clone(&self.snapshot_transactions)
    }

    fn target_table_id_for_events(&self, events: &[Event]) -> Option<TableId> {
        events.iter().find_map(|event| match event {
            Event::Insert(insert) => {
                self.target_table_id_for_schema(&insert.replicated_table_schema)
            }
            Event::Update(update) => {
                self.target_table_id_for_schema(&update.replicated_table_schema)
            }
            Event::Delete(delete) => {
                self.target_table_id_for_schema(&delete.replicated_table_schema)
            }
            Event::Relation(relation) => {
                self.target_table_id_for_schema(&relation.replicated_table_schema)
            }
            Event::Truncate(truncate) => truncate
                .truncated_tables
                .iter()
                .find_map(|schema| self.target_table_id_for_schema(schema)),
            Event::Begin(_) | Event::Commit(_) | Event::Unsupported => None,
        })
    }

    fn target_table_id_for_schema(&self, schema: &ReplicatedTableSchema) -> Option<TableId> {
        let name = schema.name();
        self.is_target_table(&name.schema, &name.name)
            .then(|| schema.id())
    }

    fn push_queue_entry(
        &self,
        (buffer, errors): (Option<Box<dyn InputBuffer>>, Vec<ParseError>),
        timestamp: chrono::DateTime<Utc>,
        senders: DeferredSenders,
        start_transaction: Option<Option<String>>,
        commit_transaction: bool,
    ) {
        self.queue.push_entry(
            InputQueueEntry::new_with_aux(timestamp, senders)
                .with_buffer(buffer)
                .with_start_transaction(start_transaction)
                .with_commit_transaction(commit_transaction),
            errors,
        );
    }

    fn queue_stream_ack(
        &self,
        entry: (Option<Box<dyn InputBuffer>>, Vec<ParseError>),
        timestamp: chrono::DateTime<Utc>,
        ack: DeferredAck,
        start_transaction: Option<Option<String>>,
        commit_transaction: bool,
    ) {
        if self.completion_task_tx.is_some() {
            self.queued_acks.fetch_add(1, Ordering::Release);
            self.push_queue_entry(
                entry,
                timestamp,
                vec![ack],
                start_transaction,
                commit_transaction,
            );
            // etl waits for this ack before sending more events. Keep taking
            // steps until the queue entry carrying it is flushed.
            self.queue.consumer.request_step();
        } else {
            self.push_queue_entry(
                entry,
                timestamp,
                DeferredSenders::new(),
                start_transaction,
                commit_transaction,
            );
            ack.complete();
        }
    }

    /// Answer a table-copy write.
    ///
    /// A batch that queued rows is answered `Accepted` right away: Feldera owns
    /// the rows, and etl keeps copying. This also forces etl to send its terminal
    /// empty write, including when completion tracking is unavailable, so the
    /// copy transaction always gets its commit barrier.
    ///
    /// The one empty write etl sends at the end of the copy (an empty table
    /// sends only this) is the table's durability barrier. With tracking, queue
    /// it behind all snapshot data until the watcher passes its step. Without
    /// tracking, queue an empty commit entry when it closes a copy transaction,
    /// then answer `Durable` immediately.
    fn ack_snapshot_copy(
        &self,
        queued_data: bool,
        commit_transaction: bool,
        async_result: WriteTableRowsResult,
    ) {
        let tracking = self.completion_task_tx.is_some();
        match immediate_snapshot_copy_status(tracking, queued_data) {
            Some(DestinationWriteStatus::Accepted) => {
                async_result.send(Ok(DestinationWriteStatus::Accepted));
            }
            Some(DestinationWriteStatus::Durable) => {
                if commit_transaction {
                    self.push_queue_entry(
                        (None, Vec::new()),
                        Utc::now(),
                        DeferredSenders::new(),
                        None,
                        true,
                    );
                }
                async_result.send(Ok(DestinationWriteStatus::Durable));
            }
            None => {
                // We only copy the configured source table. write_table_rows
                // ignores every other table in the publication and immediately
                // marks its batches durable.
                self.queued_acks.fetch_add(1, Ordering::Release);
                self.push_queue_entry(
                    (None, Vec::new()),
                    Utc::now(),
                    vec![DeferredAck::SnapshotCopyBarrier(async_result)],
                    None,
                    commit_transaction,
                );
            }
        }
    }

    /// Answer a stream write that produced no Feldera rows.
    ///
    /// The completion watcher answers `Durable` if the frontier already covers
    /// all earlier `Accepted` data. Otherwise a `MayDefer` write is `Accepted`,
    /// while a `RequireDurable` write waits for that frontier.
    fn ack_no_row_stream_write(&self, ack: DeferredAck) {
        if !ack.is_stream() {
            unreachable!("snapshot copy barriers are handled by ack_snapshot_copy")
        } else if let Some(tx) = self.completion_task_tx.as_ref() {
            // A no-row write is etl's chance to hear that data it previously
            // got an Accepted for has since become durable.
            let _ = tx.send(CompletionMessage::NoRowStream(ack));
        } else {
            // Without completion tracking, data writes are already Durable.
            ack.complete();
        }
    }

    /// Wait until the Feldera pipeline is running before accepting a new etl
    /// batch.
    async fn wait_unpaused(&self) -> EtlResult<()> {
        let mut rx = self.pipeline_state_rx.clone();
        match rx.wait_for(|state| state != &PipelineState::Paused).await {
            Ok(state) if *state == PipelineState::Running => Ok(()),
            Ok(_) => Err(etl_error!(
                ErrorKind::DestinationError,
                "Postgres CDC input connector terminated before accepting batch"
            )),
            Err(_) => Err(etl_error!(
                ErrorKind::DestinationError,
                "Postgres CDC input connector state channel closed before accepting batch"
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

/// Return the COPY status that can be reported immediately.
fn immediate_snapshot_copy_status(
    tracking: bool,
    queued_data: bool,
) -> Option<DestinationWriteStatus> {
    match (tracking, queued_data) {
        (_, true) => Some(DestinationWriteStatus::Accepted),
        (false, false) => Some(DestinationWriteStatus::Durable),
        (true, false) => None,
    }
}

async fn target_table_id(store: &PostgresStore, source_table: &str) -> EtlResult<Option<TableId>> {
    Ok(store
        .get_table_schemas()
        .await?
        .iter()
        .find(|schema| is_target_table(source_table, &schema.name.schema, &schema.name.name))
        .map(|schema| schema.id))
}

fn is_target_table(source_table: &str, schema_name: &str, table_name: &str) -> bool {
    let qualified = format!("{schema_name}.{table_name}");
    source_table == qualified
        || source_table == table_name
        || source_table == format!("\"{schema_name}\".\"{table_name}\"")
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
/// tolerance is not enabled. `Strict` waits for checkpoint completion; used
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

/// Background task that answers deferred acks.
///
/// Each [`PendingAcks`] remembers how many steps had completed when its data
/// was flushed. The data lands in the next step, so its acks are `Durable` once
/// the frontier moves strictly past `step_at_flush`. `MayDefer` stream acks that
/// wait longer than the hold deadline are answered `Accepted` instead, and a
/// later write confirms them once the frontier catches up. `RequireDurable`
/// acks always wait for the frontier.
async fn completion_watcher_task(
    mut watcher: WatcherReceiver,
    mut pending_rx: mpsc::UnboundedReceiver<CompletionMessage>,
    endpoint_name: String,
    streaming_ack_hold: Duration,
) {
    let mut waiting: Vec<PendingAcks> = Vec::new();
    // The latest step whose stream ack timed out as Accepted. etl applies
    // events in one ordered stream, so a Durable answer for any later step
    // also covers this one; forget it once that happens.
    let mut accepted_stream_step = None;

    loop {
        let next_stream_deadline = earliest_stream_deadline(&waiting, streaming_ack_hold);
        tokio::select! {
            result = watcher.changed() => {
                if result.is_err() {
                    break; // Sender dropped (pipeline shutting down)
                }
                let frontier = watcher.frontier();
                if let Some(completed_step) = fire_completed(&mut waiting, frontier)
                    && accepted_stream_step.is_some_and(|step| step <= completed_step)
                {
                    accepted_stream_step = None;
                }
            }
            maybe_message = pending_rx.recv() => {
                match maybe_message {
                    Some(CompletionMessage::Queued(entry)) => {
                        let frontier = watcher.frontier();
                        if frontier > entry.step_at_flush {
                            // Already past the threshold: complete immediately.
                            let completes_stream = entry.senders.iter().any(DeferredAck::is_stream);
                            for sender in entry.senders {
                                sender.complete();
                            }
                            if completes_stream
                                && accepted_stream_step
                                    .is_some_and(|step| step <= entry.step_at_flush)
                            {
                                accepted_stream_step = None;
                            }
                        } else {
                            waiting.push(entry);
                        }
                    }
                    Some(CompletionMessage::NoRowStream(ack)) => {
                        complete_no_row_stream_ack(
                            ack,
                            watcher.frontier(),
                            &mut accepted_stream_step,
                            &mut waiting,
                        );
                    }
                    None => break, // Channel closed
                }
            }
            _ = async {
                match next_stream_deadline {
                    Some(deadline) => sleep_until(TokioInstant::from_std(deadline)).await,
                    None => pending::<()>().await,
                }
            } => {
                debug!(
                    "postgres_cdc {endpoint_name}: stream ack hold expired, \
                     accepting non-durable stream acks"
                );
                if let Some(step) = accept_expired_stream_acks(
                    &mut waiting,
                    Instant::now(),
                    streaming_ack_hold,
                ) {
                    accepted_stream_step = Some(
                        accepted_stream_step.map_or(step, |previous: u64| previous.max(step)),
                    );
                }
            }
        }
    }

    // Dropping the remaining senders here is safe: the connector signals etl
    // shutdown before aborting this task, so etl's copy loop sees the
    // shutdown before it sees the dropped ack, and its table-sync error
    // handler does not persist errors that surface after a shutdown request.
    debug!(
        "postgres_cdc {endpoint_name}: completion watcher exiting with {} pending entries",
        waiting.len()
    );
}

/// Answers `Durable` for every waiting ack whose step the frontier has
/// passed. Returns the latest step whose stream ack became `Durable`, so the
/// caller can clear `accepted_stream_step` when that Durable covers it.
fn fire_completed(waiting: &mut Vec<PendingAcks>, frontier: u64) -> Option<u64> {
    let mut completed_stream_step = None;
    waiting.retain_mut(|entry| {
        if frontier > entry.step_at_flush {
            for sender in entry.senders.drain(..) {
                if sender.is_stream() {
                    completed_stream_step = Some(
                        completed_stream_step.map_or(entry.step_at_flush, |step: u64| {
                            step.max(entry.step_at_flush)
                        }),
                    );
                }
                sender.complete();
            }
            false
        } else {
            true
        }
    });
    completed_stream_step
}

/// Answer a no-row stream write against the latest accepted prefix.
///
/// If the prefix is already complete, answer `Durable`. Otherwise answer a
/// `MayDefer` write as `Accepted`, or retain a `RequireDurable` write until the
/// frontier passes the accepted step.
fn complete_no_row_stream_ack(
    ack: DeferredAck,
    frontier: u64,
    accepted_stream_step: &mut Option<u64>,
    waiting: &mut Vec<PendingAcks>,
) {
    debug_assert!(ack.is_stream());
    let Some(accepted_step) = *accepted_stream_step else {
        ack.complete();
        return;
    };

    if frontier > accepted_step {
        *accepted_stream_step = None;
        ack.complete();
    } else if ack.may_defer() {
        ack.accept();
    } else {
        waiting.push(PendingAcks {
            step_at_flush: accepted_step,
            held_since: Instant::now(),
            senders: vec![ack],
        });
    }
}

/// The next moment a waiting stream ack's hold expires, if any.
fn earliest_stream_deadline(
    waiting: &[PendingAcks],
    streaming_ack_hold: Duration,
) -> Option<Instant> {
    waiting
        .iter()
        .filter(|entry| entry.senders.iter().any(DeferredAck::may_defer))
        .map(|entry| entry.held_since + streaming_ack_hold)
        .min()
}

/// Answers `Accepted` for deferrable stream acks whose hold has expired.
/// Snapshot barriers and required-durability stream acks stay queued.
/// Returns the latest step whose stream ack was accepted.
fn accept_expired_stream_acks(
    waiting: &mut Vec<PendingAcks>,
    now: Instant,
    streaming_ack_hold: Duration,
) -> Option<u64> {
    let mut accepted_stream_step = None;
    waiting.retain_mut(|entry| {
        if now >= entry.held_since + streaming_ack_hold {
            let senders = std::mem::take(&mut entry.senders);
            entry.senders = senders
                .into_iter()
                .filter_map(|sender| {
                    if sender.may_defer() {
                        accepted_stream_step = Some(
                            accepted_stream_step.map_or(entry.step_at_flush, |step: u64| {
                                step.max(entry.step_at_flush)
                            }),
                        );
                        sender.accept();
                        None
                    } else {
                        Some(sender)
                    }
                })
                .collect();
        }

        !entry.senders.is_empty()
    });
    accepted_stream_step
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
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
    use etl::data::PgNumeric;
    use serde_json::json;
    use std::str::FromStr;

    fn errored_table_state(reason: &str) -> TableState {
        serde_json::from_value(json!({
            "type": "errored",
            "reason": reason,
            "solution": null,
            "retry_policy": { "type": "manual_retry" },
        }))
        .unwrap()
    }

    #[test]
    fn discard_shutdown_errors_only_matches_dropped_destination_acks() {
        let shutdown_error = errored_table_state(DROPPED_DESTINATION_ACK_ERROR);
        let replication_error = errored_table_state("source schema changed");

        assert!(should_discard_table_error(&shutdown_error, false));
        assert!(!should_discard_table_error(&replication_error, false));
        assert!(should_discard_table_error(&shutdown_error, true));
        assert!(should_discard_table_error(&replication_error, true));
    }

    #[test]
    fn snapshot_transaction_moves_from_copy_to_catchup() {
        let table_id = TableId::new(42);
        let mut phase = SnapshotPhase::Copy {
            started: false,
            table_id: None,
        };

        assert!(phase.start_copy(table_id));
        assert!(!phase.start_copy(table_id));
        assert!(phase.finish_copy(table_id));
        assert!(matches!(
            &phase,
            SnapshotPhase::Catchup {
                started,
                table_id: id,
            } if !*started && *id == table_id
        ));

        assert!(phase.start_catchup(Some(table_id), true));
        assert!(!phase.start_catchup(Some(table_id), true));
        assert!(phase.finish_catchup());
        assert!(matches!(phase, SnapshotPhase::Inactive));
    }

    #[test]
    fn snapshot_catchup_only_starts_for_target_table_with_data() {
        let table_id = TableId::new(42);
        let mut phase = SnapshotPhase::Catchup {
            started: false,
            table_id,
        };

        assert!(!phase.start_catchup(Some(TableId::new(43)), true));
        assert!(!phase.start_catchup(Some(table_id), false));
        assert!(!phase.start_catchup(None, true));
        assert!(phase.start_catchup(Some(table_id), true));
    }

    #[test]
    fn snapshot_empty_copy_moves_to_catchup_without_commit() {
        let table_id = TableId::new(42);
        let mut phase = SnapshotPhase::Copy {
            started: false,
            table_id: Some(table_id),
        };

        assert!(!phase.finish_copy(table_id));
        assert!(matches!(
            &phase,
            SnapshotPhase::Catchup {
                started,
                table_id: id,
            } if !*started && *id == table_id
        ));
    }

    #[test]
    fn snapshot_unstarted_catchup_finishes_without_commit() {
        let mut phase = SnapshotPhase::Catchup {
            started: false,
            table_id: TableId::new(42),
        };

        assert!(!phase.finish_catchup());
        assert!(matches!(phase, SnapshotPhase::Inactive));
    }

    #[test]
    fn snapshot_copy_without_tracking_forces_terminal_barrier() {
        assert_eq!(
            immediate_snapshot_copy_status(false, true),
            Some(DestinationWriteStatus::Accepted)
        );
        assert_eq!(
            immediate_snapshot_copy_status(false, false),
            Some(DestinationWriteStatus::Durable)
        );
    }

    #[test]
    fn tracked_snapshot_copy_defers_terminal_barrier() {
        assert_eq!(
            immediate_snapshot_copy_status(true, true),
            Some(DestinationWriteStatus::Accepted)
        );
        assert_eq!(immediate_snapshot_copy_status(true, false), None);
    }

    fn test_ack(kind: TestAckKind, tx: std::sync::mpsc::Sender<TestAckStatus>) -> DeferredAck {
        DeferredAck::Test { kind, tx }
    }

    fn test_required_stream_ack(tx: std::sync::mpsc::Sender<TestAckStatus>) -> DeferredAck {
        DeferredAck::Test {
            kind: TestAckKind::StreamRequireDurable,
            tx,
        }
    }

    #[test]
    fn watcher_deadline_accepts_only_deferrable_stream_acks() {
        let (tx, rx) = std::sync::mpsc::channel();
        let now = Instant::now();
        let hold = Duration::from_secs(2);
        let mut waiting = vec![PendingAcks {
            step_at_flush: 7,
            held_since: now - Duration::from_secs(3),
            senders: vec![
                test_ack(TestAckKind::StreamMayDefer, tx.clone()),
                test_required_stream_ack(tx.clone()),
                test_ack(TestAckKind::SnapshotCopyBarrier, tx),
            ],
        }];

        let accepted_step = accept_expired_stream_acks(&mut waiting, now, hold);

        assert_eq!(accepted_step, Some(7));
        assert_eq!(rx.recv().unwrap(), TestAckStatus::Accepted);
        assert!(rx.try_recv().is_err());
        assert_eq!(waiting.len(), 1);
        assert_eq!(waiting[0].senders.len(), 2);
        assert!(waiting[0].senders.iter().all(|ack| !ack.may_defer()));

        assert_eq!(fire_completed(&mut waiting, 8), Some(7));

        assert_eq!(rx.recv().unwrap(), TestAckStatus::Durable);
        assert_eq!(rx.recv().unwrap(), TestAckStatus::Durable);
        assert!(waiting.is_empty());
    }

    #[test]
    fn watcher_frontier_completes_all_acks_sharing_a_step() {
        let (tx, rx) = std::sync::mpsc::channel();
        let now = Instant::now();
        let mut waiting = vec![PendingAcks {
            step_at_flush: 7,
            held_since: now,
            senders: vec![
                test_ack(TestAckKind::StreamMayDefer, tx.clone()),
                test_ack(TestAckKind::SnapshotCopyBarrier, tx),
            ],
        }];

        assert_eq!(fire_completed(&mut waiting, 7), None);
        assert!(rx.try_recv().is_err());
        assert_eq!(waiting.len(), 1);
        assert_eq!(waiting[0].senders.len(), 2);

        assert_eq!(fire_completed(&mut waiting, 8), Some(7));

        let mut statuses = vec![rx.recv().unwrap(), rx.recv().unwrap()];
        statuses.sort_by_key(|status| match status {
            TestAckStatus::Accepted => 0,
            TestAckStatus::Durable => 1,
        });
        assert_eq!(
            statuses,
            vec![TestAckStatus::Durable, TestAckStatus::Durable]
        );
        assert!(waiting.is_empty());
    }

    #[test]
    fn no_row_stream_ack_makes_checkpointed_accepted_data_durable() {
        let (tx, rx) = std::sync::mpsc::channel();
        let mut accepted_stream_step = Some(7);
        let mut waiting = Vec::new();

        complete_no_row_stream_ack(
            test_ack(TestAckKind::StreamMayDefer, tx.clone()),
            7,
            &mut accepted_stream_step,
            &mut waiting,
        );
        assert_eq!(rx.recv().unwrap(), TestAckStatus::Accepted);
        assert_eq!(accepted_stream_step, Some(7));
        assert!(waiting.is_empty());

        complete_no_row_stream_ack(
            test_ack(TestAckKind::StreamMayDefer, tx),
            8,
            &mut accepted_stream_step,
            &mut waiting,
        );
        assert_eq!(rx.recv().unwrap(), TestAckStatus::Durable);
        assert_eq!(accepted_stream_step, None);
        assert!(waiting.is_empty());
    }

    #[test]
    fn no_row_stream_ack_is_durable_without_accepted_data() {
        let (tx, rx) = std::sync::mpsc::channel();
        let mut accepted_stream_step = None;
        let mut waiting = Vec::new();

        complete_no_row_stream_ack(
            test_ack(TestAckKind::StreamMayDefer, tx),
            0,
            &mut accepted_stream_step,
            &mut waiting,
        );

        assert_eq!(rx.recv().unwrap(), TestAckStatus::Durable);
        assert_eq!(accepted_stream_step, None);
        assert!(waiting.is_empty());
    }

    #[test]
    fn required_no_row_stream_ack_waits_for_accepted_prefix() {
        let (tx, rx) = std::sync::mpsc::channel();
        let mut accepted_stream_step = Some(7);
        let mut waiting = Vec::new();

        complete_no_row_stream_ack(
            test_required_stream_ack(tx),
            7,
            &mut accepted_stream_step,
            &mut waiting,
        );

        assert!(rx.try_recv().is_err());
        assert_eq!(accepted_stream_step, Some(7));
        assert_eq!(waiting.len(), 1);
        assert!(!waiting[0].senders[0].may_defer());

        assert_eq!(fire_completed(&mut waiting, 8), Some(7));
        assert_eq!(rx.recv().unwrap(), TestAckStatus::Durable);
        assert!(waiting.is_empty());
    }

    #[test]
    fn snapshot_copy_barrier_completes_durable_only() {
        // The copy barrier must never be reported as merely accepted: etl fails
        // a table copy whose terminal barrier does not confirm durability.
        let (tx, rx) = std::sync::mpsc::channel();
        DeferredAck::Test {
            kind: TestAckKind::SnapshotCopyBarrier,
            tx: tx.clone(),
        }
        .complete();
        assert_eq!(rx.recv().unwrap(), TestAckStatus::Durable);
    }

    #[test]
    #[should_panic(expected = "never accepted")]
    fn snapshot_copy_barrier_never_accepts() {
        let (tx, _rx) = std::sync::mpsc::channel();
        DeferredAck::Test {
            kind: TestAckKind::SnapshotCopyBarrier,
            tx,
        }
        .accept();
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
        assert_eq!(cell_to_json(&Cell::F32(3.14)), json!(3.14f32));
        // NaN and infinity produce null
        assert_eq!(cell_to_json(&Cell::F32(f32::NAN)), Value::Null);
        assert_eq!(cell_to_json(&Cell::F32(f32::INFINITY)), Value::Null);
        assert_eq!(cell_to_json(&Cell::F32(f32::NEG_INFINITY)), Value::Null);
    }

    #[test]
    fn test_cell_f64() {
        assert_eq!(cell_to_json(&Cell::F64(2.718)), json!(2.718f64));
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

    /// Test the is_target_table logic extracted for direct verification.
    /// This mirrors FelderaDestination::is_target_table without needing to
    /// construct the full struct.
    fn target_table_matches(source_table: &str, schema_name: &str, table_name: &str) -> bool {
        let qualified = format!("{schema_name}.{table_name}");
        source_table == qualified
            || source_table == table_name
            || source_table == format!("\"{schema_name}\".\"{table_name}\"")
    }

    #[test]
    fn test_target_table_unqualified() {
        assert!(target_table_matches("orders", "public", "orders"));
        assert!(!target_table_matches("orders", "public", "users"));
    }

    #[test]
    fn test_target_table_qualified() {
        assert!(target_table_matches("public.orders", "public", "orders"));
        assert!(!target_table_matches("other.orders", "public", "orders"));
    }

    #[test]
    fn test_target_table_quoted() {
        assert!(target_table_matches(
            "\"public\".\"orders\"",
            "public",
            "orders"
        ));
        assert!(!target_table_matches(
            "\"other\".\"orders\"",
            "public",
            "orders"
        ));
    }

    #[test]
    fn test_target_table_different_schema() {
        assert!(!target_table_matches("myschema.orders", "public", "orders"));
        assert!(target_table_matches(
            "myschema.orders",
            "myschema",
            "orders"
        ));
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
}
