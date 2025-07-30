//! An I/O controller that coordinates the creation, reconfiguration,
//! teardown of input/output adapters, and implements runtime flow control.
//!
//! # Design
//!
//! The circuit thread owns the `DBSPHandle` and calls `step()` on it whenever
//! there is some input data available for the circuit.  It can be configured
//! to improve batching by slightly delaying the `step()` call if the number of
//! available input records is below some used-defined threshold.
//!
//! The backpressure thread controls the flow of data through transport
//! endpoints, pausing the endpoints either when the amount of data buffered by
//! the endpoint exceeds a user-defined threshold or in response to an explicit
//! user request.
//!
//! Both tasks require monitoring the state of the input buffers.  To this end,
//! the controller expects transports to report the number of bytes and records
//! buffered via `InputConsumer::buffered`.

use crate::catalog::OutputCollectionHandles;
use crate::controller::checkpoint::CheckpointOffsets;
use crate::controller::journal::Journal;
use crate::controller::stats::{InputEndpointMetrics, OutputEndpointMetrics};
use crate::create_integrated_output_endpoint;
use crate::server::metrics::{LabelStack, MetricsFormatter, MetricsWriter};
use crate::transport::clock::now_endpoint_config;
use crate::transport::Step;
use crate::transport::{input_transport_config_to_endpoint, output_transport_config_to_endpoint};
use crate::util::run_on_thread_pool;
use crate::{
    catalog::SerBatch, CircuitCatalog, Encoder, InputConsumer, OutputConsumer, OutputEndpoint,
    ParseError, PipelineState, TransportInputEndpoint,
};
use anyhow::{anyhow, Error as AnyError};
use arrow::datatypes::Schema;
use atomic::Atomic;
use checkpoint::Checkpoint;
use crossbeam::{
    queue::SegQueue,
    sync::{Parker, ShardedLock, Unparker},
};
use datafusion::prelude::*;
use dbsp::circuit::metrics::{
    COMPACTION_STALL_TIME, DBSP_STEP, FILES_CREATED, FILES_DELETED, TOTAL_LATE_RECORDS,
};
use dbsp::circuit::tokio::TOKIO;
use dbsp::circuit::{CircuitStorageConfig, DevTweaks, Mode};
use dbsp::storage::backend::{StorageBackend, StoragePath};
use dbsp::{
    circuit::{CircuitConfig, Layout},
    profile::GraphProfile,
    DBSPHandle,
};
use enum_map::EnumMap;
use feldera_adapterlib::transport::Resume;
use feldera_adapterlib::utils::datafusion::execute_query_text;
use feldera_ir::LirCircuit;
use feldera_storage::checkpoint_synchronizer::CheckpointSynchronizer;
use feldera_storage::metrics::{READ_LATENCY, SYNC_LATENCY, WRITE_LATENCY};
use feldera_types::checkpoint::CheckpointMetadata;
use feldera_types::format::json::JsonLines;
use feldera_types::secret_resolver::resolve_secret_references_in_connector_config;
use feldera_types::suspend::{PermanentSuspendError, SuspendError, TemporarySuspendError};
use governor::DefaultDirectRateLimiter;
use governor::Quota;
use governor::RateLimiter;
use journal::StepMetadata;
use nonzero_ext::nonzero;
use rmpv::Value as RmpValue;
use serde_json::Value as JsonValue;
use stats::StepResults;
use std::borrow::Cow;
use std::collections::HashMap;
use std::collections::HashSet;
use std::io::ErrorKind;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::PathBuf;
use std::sync::atomic::AtomicI64;
use std::sync::mpsc::{channel, sync_channel, Receiver, SendError, Sender};
use std::sync::LazyLock;
use std::thread;
use std::{
    collections::{BTreeMap, BTreeSet},
    mem,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};
use tokio::sync::oneshot;
use tokio::sync::Mutex as TokioMutex;
use tracing::{debug, error, info, trace, warn};
use validate::validate_config;

mod checkpoint;
mod error;
mod journal;
mod stats;
mod validate;

use crate::adhoc::create_session_context;
use crate::adhoc::table::AdHocTable;
use crate::catalog::{SerBatchReader, SerTrace, SyncSerBatchReader};
use crate::format::parquet::relation_to_arrow_fields;
use crate::format::{get_input_format, get_output_format};
use crate::integrated::create_integrated_input_endpoint;
pub use error::{ConfigError, ControllerError};
pub use feldera_types::config::{
    ConnectorConfig, FormatConfig, InputEndpointConfig, OutputEndpointConfig, PipelineConfig,
    RuntimeConfig, TransportConfig,
};
use feldera_types::config::{FileBackendConfig, FtConfig, FtModel, OutputBufferConfig};
use feldera_types::constants::{STATE_FILE, STEPS_FILE};
use feldera_types::format::json::{JsonFlavor, JsonParserConfig, JsonUpdateFormat};
use feldera_types::program_schema::{canonical_identifier, SqlIdentifier};
pub use stats::{CompletionToken, ControllerStatus, InputEndpointStatus};

/// Maximal number of concurrent API connections per circuit
/// (including both input and output connections).
// TODO: make this configurable.
pub(crate) const MAX_API_CONNECTIONS: u64 = 100;

pub(crate) type EndpointId = u64;

/// Controller that coordinates the creation, reconfiguration, teardown of
/// input/output adapters, and implements runtime flow control.
///
/// The controller instantiates the input and output pipelines according to a
/// user-provided [configuration](`PipelineConfig`) and exposes an API to
/// reconfigure and monitor the pipelines at runtime.
///
/// # Lifecycle
///
/// A pipeline process has a [PipelineState], which is the state requested by
/// the client, one of [Running], [Paused], or [Terminated]. This state is
/// initially [Paused].  Calls to [start], [pause], [initiate_stop], and [stop]
/// change the client-requested state.  Once the state is set to [Terminated],
/// it can never be changed back to [Running] or [Paused].  Since the pipeline
/// process always starts up paused, it is the pipeline manager's job to ensure
/// continuity when the process restarts.
///
/// The following diagram illustrates internal pipeline process states and their
/// possible transitions:
///
/// ```text
///      ┌──Initializing──┐
///      │        │       │
///      │        │       │
///      ▼        │       ▼
///    Replaying  │  Bootstrapping
///      │        │       │
///      │        │       │
///      │        │       │
///      ▼        ▼       ▼
/// ┌───────────────────────────┐
/// │ (default)                 │
/// │  Paused◄────────►Running  │
/// │     │                │    │
/// │     │                │    │
/// │     └──►Terminated◄──┘    │
/// └───────────────────────────┘
///     client-requested state
/// ```
///
/// The following list describes states and transitions in more details:
///
/// * Initializing: Before the circuit thread starts its main loop, the pipeline
///   can be considered to be initializing. This transitions to one of
///   replaying, bootstrapping, or the client-requested state.
///
/// * Replaying (aka restoring): If fault tolerance is enabled (whether
///   [FtModel::AtLeastOnce] or [FtModel::ExactlyOnce]), the pipeline reads and
///   replay the steps in the journal. When replay is done, the pipeline
///   transitions to the client-requested state.
///
///   Adding and removing input and output connectors, and ad-hoc queries, will
///   fail while a pipeline is replaying.
///
///   [is_replaying] reports whether the pipeline is currently replaying.
///
/// * Bootstrapping: If the pipeline is resuming from a checkpoint, and the
///   circuit was modified since the checkpoint, then the pipeline process
///   "bootstrap" the circuit to adjust the results to match the new
///   circuit. When bootstrapping is done, the pipeline transitions to the
///   client-requested state.  Bootstrapping and replaying are currently
///   mutually exclusive--if both would be required, the pipeline process gives
///   up and fails the circuit.
///
///   Adding and removing input connectors will fail with an error while the
///   pipeline is bootstrapping.
///
/// * Paused: In this state, the pipeline tells input connectors to stop reading
///   new records into their input buffers. However, if accumulated records
///   already exist in their buffers, the circuit will continue to execute steps
///   until all of them are drained (for pipeline setups with deep buffers or a
///   slow circuit, this can take minutes or longer). This state transitions to
///   running or terminated in response to client request.
///
/// * Running: In this process, the pipeline tells input connectors to read new
///   records into their input buffers (up to a per-connector, configurable
///   buffer limit). When enough records accumulate or a timer expires, or when
///   the clock ticks (all of these are configurable), the circuit steps. This
///   state transitions to paused or terminated in response to client request.
///
/// * Terminated: the circuit is dead and won't come back without creating a new
///   pipeline process. This state never transitions.
///
/// [Running]: PipelineState::Running
/// [Paused]: PipelineState::Paused
/// [Terminated]: PipelineState::Terminated
/// [start]: Controller::start
/// [stop]: Controller::stop
/// [pause]: Controller::pause
/// [initiate_stop]: Controller::initiate_stop
/// [is_replaying]: Controller::is_replaying
pub struct Controller {
    inner: Arc<ControllerInner>,

    /// The circuit thread handle (see module-level docs).
    circuit_thread_handle: JoinHandle<Result<(), ControllerError>>,
}

/// Type of the callback argument to [`Controller::start_graph_profile`].
pub type GraphProfileCallbackFn = Box<dyn FnOnce(Result<GraphProfile, ControllerError>) + Send>;

/// Type of the callback argument to [`Controller::start_checkpoint`].
pub type CheckpointCallbackFn = Box<dyn FnOnce(Result<Checkpoint, Arc<ControllerError>>) + Send>;

/// Type of the callback argument to [`Controller::start_suspend`].
pub type SuspendCallbackFn = Box<dyn FnOnce(Result<(), Arc<ControllerError>>) + Send>;

/// Type of the callback argument to [`Controller::start_checkpoint_sync`].
pub type SyncCheckpointCallbackFn = Box<dyn FnOnce(Result<(), Arc<ControllerError>>) + Send>;

/// A command that [Controller] can send to [Controller::circuit_thread].
///
/// There is no type for a command reply.  Instead, the command implementation
/// uses a callback embedded in the command to reply.
enum Command {
    GraphProfile(GraphProfileCallbackFn),
    Checkpoint(CheckpointCallbackFn),
    Suspend(SuspendCallbackFn),
    SyncCheckpoint((uuid::Uuid, SyncCheckpointCallbackFn)),
}

impl Command {
    pub fn flush(self) {
        match self {
            Command::GraphProfile(callback) => callback(Err(ControllerError::ControllerExit)),
            Command::Checkpoint(callback) => {
                callback(Err(Arc::new(ControllerError::ControllerExit)))
            }
            Command::Suspend(callback) => callback(Err(Arc::new(ControllerError::ControllerExit))),
            Command::SyncCheckpoint((_, callback)) => {
                callback(Err(Arc::new(ControllerError::ControllerExit)))
            }
        }
    }
}

impl Controller {
    /// Create a new I/O controller for a circuit.
    ///
    /// Creates a new instance of `Controller` that wraps `circuit`,  with
    /// input and output endpoints specified by `config`.  The controller is
    /// created with all endpoints in a paused state.  Call [`Self::start`]
    /// to unpause the endpoints and start ingesting data.
    ///
    /// # Arguments
    ///
    /// * `circuit` - A handle to a DBSP circuit managed by this controller. The
    ///   controller takes ownership of the circuit.
    ///
    /// * `catalog` - A catalog of input and output streams of the circuit.
    ///
    /// * `config` - Controller configuration, including global config settings
    ///   and individual endpoint configs.
    ///
    /// * `error_cb` - Error callback.  The controller doesn't implement its own
    ///   error handling policy, but simply forwards most errors to this
    ///   callback.
    ///
    /// # Errors
    ///
    /// The method may fail for the following reasons:
    ///
    /// * The input configuration is invalid, e.g., specifies an unknown
    ///   transport or data format.
    ///
    /// * One or more of the endpoints fails to initialize.
    pub fn with_config<F>(
        circuit_factory: F,
        config: &PipelineConfig,
        error_cb: Box<dyn Fn(Arc<ControllerError>) + Send + Sync>,
    ) -> Result<Self, ControllerError>
    where
        F: FnOnce(CircuitConfig) -> Result<(DBSPHandle, Box<dyn CircuitCatalog>), ControllerError>
            + Send
            + 'static,
    {
        validate_config(config)?;

        let (circuit_thread_handle, inner) = {
            // A channel to communicate circuit initialization status.
            // The `circuit_factory` closure must be invoked in the context of
            // the circuit thread, because the circuit handle it returns doesn't
            // implement `Send`.  So we need this channel to communicate circuit
            // initialization status back to this thread.  On success, the worker
            // thread adds a catalog to `inner`, and returns it wrapped in an `Arc`.
            let (init_status_sender, init_status_receiver) =
                sync_channel::<Result<Arc<ControllerInner>, ControllerError>>(0);
            let config = config.clone();
            let handle = thread::Builder::new()
                .name("circuit-thread".to_string())
                .spawn(move || {
                    match CircuitThread::new(circuit_factory, config, error_cb) {
                        Err(error) => {
                            let _ = init_status_sender.send(Err(error));
                            Ok(())
                        }
                        Ok(circuit_thread) => {
                            let _ = init_status_sender.send(Ok(circuit_thread.controller.clone()));
                            circuit_thread.run().inspect_err(|error| {
                                // Log the error before returning it from the
                                // thread: otherwise, only [Controller::stop]
                                // will join the thread and report the error.
                                error!("circuit thread died with error: {error}")
                            })
                        }
                    }
                })
                .expect("failed to spawn circuit-thread");
            // If `recv` fails, it indicates that the circuit thread panicked
            // during initialization.
            let inner = init_status_receiver
                .recv()
                .map_err(|_| ControllerError::dbsp_panic())??;

            (handle, inner)
        };

        Ok(Self {
            inner,
            circuit_thread_handle,
        })
    }

    pub fn lir(&self) -> &LirCircuit {
        &self.inner.lir
    }

    /// Connect a new input endpoint with specified name and configuration.
    ///
    /// Creates an endpoint with data transport and format specified by
    /// `config` and starts streaming data from the endpoint if the pipeline
    /// is running.
    ///
    /// # Errors
    ///
    /// The method may fail for the following reasons:
    ///
    /// * The endpoint configuration is invalid, e.g., specifies an unknown
    ///   transport or data format.
    ///
    /// * The endpoint fails to initialize, e.g., because the network address or
    ///   filename specified in the transport config is unreachable.
    pub fn connect_input(
        &self,
        endpoint_name: &str,
        config: &InputEndpointConfig,
        resume_info: Option<JsonValue>,
    ) -> Result<EndpointId, ControllerError> {
        debug!("Connecting input endpoint '{endpoint_name}'; config: {config:?}");
        self.inner.fail_if_bootstrapping_or_restoring()?;
        self.inner.connect_input(endpoint_name, config, resume_info)
    }

    /// Disconnect an existing input endpoint.
    ///
    /// This method is asynchronous and may return before all endpoint
    /// threads have terminated.
    pub fn disconnect_input(&self, endpoint_id: &EndpointId) {
        self.inner.disconnect_input(endpoint_id)
    }

    pub fn session_context(&self) -> Result<SessionContext, ControllerError> {
        self.inner.fail_if_restoring()?;
        Ok(self.inner.session_ctxt.clone())
    }

    /// Connect a previously instantiated input endpoint.
    ///
    /// Used to connect an endpoint instantiated manually rather than from an
    /// [`InputEndpointConfig`].
    ///
    /// # Arguments
    ///
    /// * `endpoint_name` - endpoint name unique within the pipeline.
    ///
    /// * `endpoint_config` - endpoint config.
    ///
    /// * `endpoint` - transport endpoint object.
    pub fn add_input_endpoint(
        &self,
        endpoint_name: &str,
        endpoint_config: InputEndpointConfig,
        endpoint: Box<dyn TransportInputEndpoint>,
        resume_info: Option<JsonValue>,
    ) -> Result<EndpointId, ControllerError> {
        self.inner.fail_if_bootstrapping_or_restoring()?;
        self.inner
            .add_input_endpoint(endpoint_name, endpoint_config, Some(endpoint), resume_info)
    }

    /// Disconnect an existing output endpoint.
    ///
    /// This method is asynchronous and may return before all endpoint
    /// threads have terminated.
    pub fn disconnect_output(&self, endpoint_id: &EndpointId) {
        debug!("Disconnecting output endpoint {endpoint_id}");

        self.inner.disconnect_output(endpoint_id)
    }

    /// Connect a previously instantiated output endpoint.
    ///
    /// Used to connect an endpoint instantiated manually rather than from an
    /// [`OutputEndpointConfig`].
    ///
    /// # Arguments
    ///
    /// * `endpoint_name` - endpoint name unique within the pipeline.
    ///
    /// * `endpoint_config` - (partial) endpoint config.  Only `format.name` and
    ///   `stream` fields need to be initialized.
    ///
    /// * `endpoint` - transport endpoint object.
    pub fn add_output_endpoint(
        &self,
        endpoint_name: &str,
        endpoint_config: &OutputEndpointConfig,
        endpoint: Box<dyn OutputEndpoint>,
    ) -> Result<EndpointId, ControllerError> {
        debug!("Adding output endpoint '{endpoint_name}'; config: {endpoint_config:?}");
        self.inner.fail_if_restoring()?;

        self.inner
            .add_output_endpoint(endpoint_name, endpoint_config, Some(endpoint))
    }

    /// Increment the number of active API connections.
    ///
    /// API connections are created dynamically via the `ingress` and `egress`
    /// REST API endpoints.
    ///
    /// Fails if the number of connections exceeds the current limit,
    /// returning the number of existing API connections.
    pub fn register_api_connection(&self) -> Result<(), u64> {
        self.inner.register_api_connection()
    }

    /// Decrement the number of active API connections.
    pub fn unregister_api_connection(&self) {
        self.inner.unregister_api_connection();
    }

    /// Return the number of active API connections.
    pub fn num_api_connections(&self) -> u64 {
        self.inner.num_api_connections()
    }

    /// Force the circuit to perform a step even if all of its
    /// input buffers are empty or nearly empty.
    pub fn request_step(&self) {
        self.inner.request_step();
    }

    /// Change the state of all input endpoints to running.
    ///
    /// Start streaming data through all connected input endpoints.
    pub fn start(&self) {
        debug!("Starting the pipeline");

        self.inner.start();
    }

    /// Pause all input endpoints.
    ///
    /// Sends a pause command to all input endpoints.  Upon receiving the
    /// command, the endpoints must stop pushing data to the pipeline.  This
    /// method is asynchronous and may return before all endpoints have been
    /// fully paused.
    pub fn pause(&self) {
        debug!("Pausing the pipeline");
        self.inner.pause();
    }

    /// Pause specified input endpoint.
    ///
    /// Sets `paused_by_user` flag of the endpoint to `true`.
    /// This method is asynchronous and may return before the endpoint has been
    /// fully paused.
    pub fn pause_input_endpoint(&self, endpoint_name: &str) -> Result<(), ControllerError> {
        self.inner.pause_input_endpoint(endpoint_name)
    }

    // Start or resume specified input endpoint.
    //
    // Sets `paused_by_user` flag of the endpoint to `false`.
    pub fn start_input_endpoint(&self, endpoint_name: &str) -> Result<(), ControllerError> {
        self.inner.start_input_endpoint(endpoint_name)
    }

    // Returns whether the specified input endpoint is paused by the user.
    pub fn is_input_endpoint_paused(&self, endpoint_name: &str) -> Result<bool, ControllerError> {
        self.inner.is_input_endpoint_paused(endpoint_name)
    }

    pub fn input_endpoint_status(&self, endpoint_name: &str) -> Result<JsonValue, ControllerError> {
        self.inner.input_endpoint_status(endpoint_name)
    }

    /// Lookup input endpoint by name.
    pub fn input_endpoint_id_by_name(
        &self,
        endpoint_name: &str,
    ) -> Result<EndpointId, ControllerError> {
        self.inner.input_endpoint_id_by_name(endpoint_name)
    }

    /// Returns whether the controller is replaying a fault tolerance log.
    pub fn is_replaying(&self) -> bool {
        self.inner.restoring.load(Ordering::Relaxed)
    }

    pub fn output_endpoint_status(
        &self,
        endpoint_name: &str,
    ) -> Result<JsonValue, ControllerError> {
        self.inner.output_endpoint_status(endpoint_name)
    }

    /// Returns controller status.
    pub fn status(&self) -> &ControllerStatus {
        // Update pipeline stats computed on-demand.
        self.inner.status.update(self.can_suspend().err());
        &self.inner.status
    }

    pub fn catalog(&self) -> &Arc<Box<dyn CircuitCatalog>> {
        &self.inner.catalog
    }

    /// Triggers a dump of the circuit's performance profile to the file system.
    /// The profile will be written asynchronously, probably after this function
    /// returns.
    pub fn dump_profile(&self) {
        debug!("Generating DBSP profile dump");
        self.start_graph_profile(Box::new(|profile| {
            match profile.map(|profile| {
                profile
                    .dump("profile")
                    .map_err(|e| ControllerError::io_error(String::from("dumping profile"), e))
            }) {
                Ok(Ok(path)) => info!("Dumped DBSP profile to {}", path.display()),
                Ok(Err(e)) | Err(e) => error!("Failed to write circuit profile: {e}"),
            }
        }));
    }

    /// Triggers a profiling operation in the running pipeline. `cb` will be
    /// called with the profile when it is ready, probably after this function
    /// returns.
    ///
    /// The callback-based nature of this function makes it useful in
    /// asynchronous contexts.
    pub fn start_graph_profile(&self, cb: GraphProfileCallbackFn) {
        self.inner.send_command(Command::GraphProfile(cb));
    }

    /// Triggers a checkpoint operation. `cb` will be called when it completes.
    ///
    /// The callback-based nature of this function makes it useful in
    /// asynchronous contexts.
    pub fn start_checkpoint(&self, cb: CheckpointCallbackFn) {
        self.inner.send_command(Command::Checkpoint(cb));
    }

    /// Triggers a sync checkpoint operation. `cb` will be called when it
    /// completes.
    ///
    /// The callback-based nature of this function makes it useful in
    /// asynchronous contexts.
    pub fn start_sync_checkpoint(&self, checkpoint: uuid::Uuid, cb: SyncCheckpointCallbackFn) {
        self.inner
            .send_command(Command::SyncCheckpoint((checkpoint, cb)));
    }

    /// Checkpoints the pipeline.
    ///
    /// This is a blocking wrapper around [Self::start_checkpoint].
    pub fn checkpoint(&self) -> Result<Checkpoint, Arc<ControllerError>> {
        let (sender, receiver) = oneshot::channel();
        self.start_checkpoint(Box::new(move |result| sender.send(result).unwrap()));
        receiver.blocking_recv().unwrap()
    }

    /// Triggers a suspend operation. `cb` will be called when it completes.
    ///
    /// The callback-based nature of this function makes it useful in
    /// asynchronous contexts.
    pub fn start_suspend(&self, cb: SuspendCallbackFn) {
        self.inner.send_command(Command::Suspend(cb));
    }

    /// Suspends the pipeline.
    ///
    /// This is a blocking wrapper around [Self::start_suspend].
    pub fn suspend(&self) -> Result<(), Arc<ControllerError>> {
        let (sender, receiver) = oneshot::channel();
        self.start_suspend(Box::new(move |result| sender.send(result).unwrap()));
        receiver.blocking_recv().unwrap()
    }

    /// Returns whether this pipeline supports suspend-and-resume.  The result
    /// can change over time; see [SuspendError] for details.
    pub fn can_suspend(&self) -> Result<(), SuspendError> {
        self.inner.can_suspend()
    }

    /// Initiate controller termination, but don't block waiting for it to finish.
    /// Can be used inside callbacks invoked by the controller without risking a deadlock.
    pub fn initiate_stop(&self) {
        self.inner.stop();
    }

    /// Terminate the controller, stop all input endpoints and destroy the
    /// circuit.
    pub fn stop(self) -> Result<(), ControllerError> {
        debug!("Stopping the circuit");

        self.initiate_stop();
        self.circuit_thread_handle
            .join()
            .map_err(|_| ControllerError::controller_panic())??;
        Ok(())
    }

    /// Check whether the pipeline has processed all input data to completion.
    ///
    /// Returns `true` when the following conditions are satisfied:
    ///
    /// * All input endpoints have signalled end-of-input.
    /// * All input records received from all endpoints have been processed by
    ///   the circuit.
    /// * All output records have been sent to respective output transport
    ///   endpoints.
    ///
    /// Note that, depending on the type and configuration of the output
    /// transport, this may not guarantee that all output records have been
    /// written to a persistent storage or delivered to the recipient.
    pub fn pipeline_complete(&self) -> bool {
        self.inner.status.pipeline_complete()
    }

    pub fn write_metrics<F>(
        &self,
        metrics: &mut MetricsWriter<F>,
        labels: &LabelStack,
        status: &ControllerStatus,
    ) where
        F: MetricsFormatter,
    {
        metrics.gauge(
            "mpu_msecs",
            "cpu time used by the pipeline process in milliseconds",
            |w| w.write_value(labels, status.global_metrics.cpu_msecs()),
        );
        metrics.gauge(
            "rss_bytes",
            "resident set size of the pipeline process in bytes",
            |w| w.write_value(labels, status.global_metrics.rss_bytes()),
        );
        metrics.gauge(
            "buffered_input_records",
            "total number of records currently buffered by all endpoints",
            |w| w.write_value(labels, status.num_buffered_input_records()),
        );
        metrics.gauge(
            "total_input_records",
            "total number of input records received from all connectors",
            |w| w.write_value(labels, status.num_total_input_records()),
        );
        metrics.gauge(
            "total_processed_records",
            "total number of input records processed by the pipeline",
            |w| w.write_value(labels, status.num_total_processed_records()),
        );
        metrics.gauge("pipeline_complete", "boolean, 1 if true", |w| {
            w.write_value(labels, status.pipeline_complete() as u8)
        });
        metrics.counter(
            "records.late",
            "Number of records dropped due to LATENESS annotations",
            |w| w.write_value(labels, &TOTAL_LATE_RECORDS),
        );
        metrics.counter(
            "file.compaction_stall_time",
            "Time in nanoseconds a worker was stalled waiting for more merges to complete",
            |w| w.write_value(labels, &COMPACTION_STALL_TIME),
        );
        metrics.counter(
            "file.total_files_created",
            "Total number of files created",
            |w| w.write_value(labels, &FILES_CREATED),
        );
        metrics.counter(
            "file.total_files_deleted",
            "Total number of files deleted",
            |w| w.write_value(labels, &FILES_DELETED),
        );
        metrics.counter(
            "feldera.dbsp.step",
            "Total number of DBSP steps executed",
            |w| w.write_value(labels, &DBSP_STEP),
        );

        metrics.histogram("read_latency_us", "Read latency in microseconds", |w| {
            w.write_histogram(labels, &READ_LATENCY.snapshot())
        });
        metrics.histogram("write_latency_us", "Write latency in microseconds", |w| {
            w.write_histogram(labels, &WRITE_LATENCY.snapshot())
        });
        metrics.histogram("sync_latency_us", "Sync latency in microseconds", |w| {
            w.write_histogram(labels, &SYNC_LATENCY.snapshot())
        });

        fn write_input_metric<F, M>(
            metrics: &mut MetricsWriter<F>,
            labels: &LabelStack,
            status: &ControllerStatus,
            name: &str,
            func: M,
        ) where
            F: MetricsFormatter,
            M: Fn(&InputEndpointMetrics) -> &AtomicU64,
        {
            metrics.gauge(name, "", |w| {
                for input in status.input_status().values() {
                    w.write_value(
                        &labels.with("endpoint", &input.endpoint_name),
                        func(&input.metrics),
                    );
                }
            });
        }
        write_input_metric(metrics, labels, status, "input_total_bytes", |m| {
            &m.total_bytes
        });
        write_input_metric(metrics, labels, status, "input_total_records", |m| {
            &m.total_records
        });
        write_input_metric(metrics, labels, status, "input_buffered_records", |m| {
            &m.buffered_records
        });
        write_input_metric(metrics, labels, status, "input_num_transport_errors", |m| {
            &m.num_transport_errors
        });
        write_input_metric(metrics, labels, status, "input_num_parse_errors", |m| {
            &m.num_parse_errors
        });

        fn write_output_metric<F, M>(
            metrics: &mut MetricsWriter<F>,
            labels: &LabelStack,
            status: &ControllerStatus,
            name: &str,
            func: M,
        ) where
            F: MetricsFormatter,
            M: Fn(&OutputEndpointMetrics) -> &AtomicU64,
        {
            metrics.gauge(name, "", |w| {
                for output in status.output_status().values() {
                    w.write_value(
                        &labels.with("endpoint", &output.endpoint_name),
                        func(&output.metrics),
                    );
                }
            });
        }
        write_output_metric(metrics, labels, status, "output_transmitted_bytes", |m| {
            &m.transmitted_bytes
        });
        write_output_metric(metrics, labels, status, "output_transmitted_records", |m| {
            &m.transmitted_records
        });
        write_output_metric(metrics, labels, status, "output_buffered_records", |m| {
            &m.buffered_records
        });
        write_output_metric(metrics, labels, status, "output_buffered_batches", |m| {
            &m.buffered_batches
        });
        write_output_metric(
            metrics,
            labels,
            status,
            "output_num_transport_errors",
            |m| &m.num_transport_errors,
        );
        write_output_metric(metrics, labels, status, "output_num_encode_errors", |m| {
            &m.num_encode_errors
        });
    }

    /// Execute a SQL query over materialized tables and views;
    /// return result as a text table.
    pub async fn execute_query_text(&self, query: &str) -> Result<String, AnyError> {
        execute_query_text(&self.session_context()?, query).await
    }

    /// Like execute_query_text, but can run outside of async runtime.
    pub fn execute_query_text_sync(&self, query: &str) -> Result<String, AnyError> {
        TOKIO.block_on(async { self.execute_query_text(query).await })
    }

    /// Generate a completion token for the specified endpoint.
    ///
    /// A completion token is a bookmark that identifies the current set of records ingested
    /// by the endpoint, including records buffered in its input queue. It can be passed to
    /// `completion_status` to determine whether all these records have been processed to
    /// completion.
    pub fn completion_token(
        &self,
        endpoint_name: &str,
    ) -> Result<CompletionToken, ControllerError> {
        let endpoint_id = self.input_endpoint_id_by_name(endpoint_name)?;

        self.status().completion_token(endpoint_name, endpoint_id)
    }

    /// Check whether all records identified by the completion token
    /// have been fully processed by the pipeline.
    pub fn completion_status(&self, token: &CompletionToken) -> Result<bool, ControllerError> {
        self.status().completion_status(token)
    }
}

struct CircuitThread {
    controller: Arc<ControllerInner>,
    circuit: DBSPHandle,
    command_receiver: Receiver<Command>,
    backpressure_thread: BackpressureThread,
    _storage_thread: Option<StorageThread>,
    ft: Option<FtState>,
    parker: Parker,
    last_checkpoint: Instant,

    checkpoint_delay_warning: Option<LongOperationWarning>,
    checkpoint_requests: Vec<CheckpointRequest>,

    /// Currently only allows one request at a time.
    sync_checkpoint_request: Option<(uuid::Uuid, SyncCheckpointCallbackFn)>,

    /// Storage backend for writing checkpoints.
    storage: Option<Arc<dyn StorageBackend>>,

    /// The step currently running or replaying.
    step: Step,

    /// Metadata for `step - 1`; that is, the metadata that would be part of a
    /// [Checkpoint] for `step`, to allow the input endpoints to seek to the
    /// starting point for reading data for `step`.
    ///
    /// This is empty if `step` is 0, which means that we can't checkpoint at
    /// step 0 if there is going to be a journal, because to replay a journal
    /// record into an endpoint we first need to seek that endpoint.
    input_metadata: HashMap<String, Option<Resume>>,
}

impl CircuitThread {
    /// Circuit thread function: holds the handle to the circuit, calls `step`
    /// on it whenever input data is available, pushes output batches
    /// produced by the circuit to output pipelines.
    fn new<F>(
        circuit_factory: F,
        config: PipelineConfig,
        error_cb: Box<dyn Fn(Arc<ControllerError>) + Send + Sync>,
    ) -> Result<Self, ControllerError>
    where
        F: FnOnce(CircuitConfig) -> Result<(DBSPHandle, Box<dyn CircuitCatalog>), ControllerError>,
    {
        let ft_model = config.global.fault_tolerance.model;
        let ControllerInit {
            pipeline_config,
            circuit_config,
            processed_records,
            step,
            input_metadata,
        } = ControllerInit::new(config.clone())?;
        let storage = circuit_config
            .storage
            .as_ref()
            .map(|storage| storage.backend.clone());

        let (mut circuit, catalog) = circuit_factory(circuit_config)?;
        let lir = circuit.lir()?;

        // Seek each input endpoint to its initial offset.
        //
        // If we're not restoring from a checkpoint, `input_metadata` will be empty so
        // this will do nothing.
        let resume_info = if let Some(input_metadata) = &input_metadata {
            let mut resume_info = HashMap::new();

            for (endpoint_name, seek) in input_metadata.iter() {
                let Some(endpoint_config) = config.inputs.get(endpoint_name.as_str()) else {
                    info!("Found checkpointed state for input connector '{endpoint_name}', but the connector is not present in the new pipeline configuration; this connector will not be added to the pipeline");
                    continue;
                };

                let node_id = catalog
                    .input_collection_handle(&SqlIdentifier::from(&endpoint_config.stream))
                    .unwrap()
                    .node_id;

                if let Some(replay_info) = circuit.bootstrap_info() {
                    if replay_info.need_backfill.contains(&node_id) {
                        info!("Found checkpointed state for input connector '{endpoint_name}', but the table that the connector is attached to has been modified and its state has been cleared; the connector will restart from scratch");
                        continue;
                    }
                }

                resume_info.insert(endpoint_name.clone(), seek.clone());
            }
            resume_info
        } else {
            HashMap::new()
        };

        let (parker, backpressure_thread, command_receiver, controller) = ControllerInner::new(
            pipeline_config,
            catalog,
            lir,
            error_cb,
            processed_records,
            &resume_info,
        )?;

        controller
            .status
            .set_bootstrap_in_progress(circuit.bootstrap_in_progress());

        let input_metadata = input_metadata.map(|input_metadata| {
            input_metadata
                .into_iter()
                .map(|(name, seek)| (name, Some(Resume::Seek { seek })))
                .collect()
        });

        let ft = match ft_model {
            Some(FtModel::ExactlyOnce) => {
                let backend = storage.clone().unwrap();
                let mut ft = if input_metadata.is_some() {
                    FtState::open(backend, step, controller.clone())
                } else {
                    FtState::create(backend, controller.clone())
                }?;

                // Normally, the pipeline can be modified between a suspend and a resume, but not between a failure
                // and a recovery. If the pipeline has been modified (bootstrap_in_progress returns true), and its
                // replay journal is not empty (is_replaying), we may not be able to recover it reliably, so just
                // give up now.
                if ft.is_replaying() && circuit.bootstrap_in_progress() {
                    return Err(ControllerError::checkpoint_does_not_match_pipeline());
                }

                // Disable journaling while we're bootstrapping the circuit.
                if circuit.bootstrap_in_progress() {
                    ft.disable();
                }

                Some(ft)
            }
            Some(FtModel::AtLeastOnce) => None,
            None => {
                if let Some(backend) = &storage {
                    // We're not fault-tolerant (not even at-least-once), so
                    // it's not a good idea to resume from the same checkpoint
                    // twice.  Delete it.
                    backend
                        .delete_if_exists(&StoragePath::from(STATE_FILE))
                        .map_err(|error| {
                            ControllerError::storage_error(
                                "delete non-FT checkpoint following resume",
                                error,
                            )
                        })?;
                }
                None
            }
        };

        let storage_thread = storage.as_ref().map(|backend| {
            StorageThread::new(
                &**backend,
                controller.status.global_metrics.storage_bytes.clone(),
                controller.status.global_metrics.storage_mb_secs.clone(),
            )
        });

        Ok(Self {
            controller,
            ft,
            circuit,
            command_receiver,
            backpressure_thread,
            _storage_thread: storage_thread,
            storage,
            parker,
            last_checkpoint: Instant::now(),
            checkpoint_delay_warning: None,
            checkpoint_requests: Vec::new(),
            sync_checkpoint_request: None,
            step,
            input_metadata: input_metadata.unwrap_or_default(),
        })
    }

    /// Main loop of the circuit thread.
    fn run(mut self) -> Result<(), ControllerError> {
        let config = &self.controller.status.pipeline_config;
        let mut trigger = StepTrigger::new(self.controller.clone());
        if config.global.cpu_profiler {
            self.circuit.enable_cpu_profiler().unwrap_or_else(|e| {
                error!("Failed to enable CPU profiler: {e}");
            });
        }

        self.finish_replaying();

        loop {
            // Run received commands.  Commands can initiate checkpoint
            // requests, so attempt to execute those afterward.  Executing a
            // checkpoint request can then terminate the pipeline, so check for
            // that right afterward.
            self.run_commands();
            if self.checkpoint_requested() {
                self.checkpoint();
            }

            if self.sync_checkpoint_requested() {
                self.sync_checkpoint();
            }

            if self.controller.state() == PipelineState::Terminated {
                break;
            }

            // Backpressure in the output pipeline: wait for room in output buffers to
            // become available.
            if self.controller.output_buffers_full() {
                debug!("circuit thread: park waiting for output buffer space");
                self.parker.park();
                debug!("circuit thread: unparked");
                continue;
            }

            match trigger.trigger(
                self.last_checkpoint,
                self.replaying(),
                self.circuit.bootstrap_in_progress(),
                self.checkpoint_requested(),
            ) {
                Action::Step => {
                    if !self.step()? {
                        break;
                    }
                }
                Action::Checkpoint => self.checkpoint_requests.push(CheckpointRequest::Scheduled),
                Action::Park(Some(deadline)) => self.parker.park_deadline(deadline),
                Action::Park(None) => self.parker.park(),
            }
        }
        self.controller.status.set_state(PipelineState::Terminated);
        self.flush_commands_and_requests();
        self.circuit
            .kill()
            .map_err(|_| ControllerError::dbsp_panic())
    }

    fn finish_replaying(&mut self) {
        if !self.replaying() {
            self.controller.restoring.store(false, Ordering::Release);
            self.backpressure_thread.start();
        }
    }

    fn step(&mut self) -> Result<bool, ControllerError> {
        let total_consumed = match self.input_step()? {
            Some(total_consumed) => total_consumed,
            None => return Ok(false),
        };

        self.step += 1;

        // Wake up the backpressure thread to unpause endpoints blocked due to
        // backpressure.
        self.controller.unpark_backpressure();
        debug!("circuit thread: calling 'circuit.step'");
        self.circuit
            .step()
            .unwrap_or_else(|e| self.controller.error(Arc::new(e.into())));
        debug!("circuit thread: 'circuit.step' returned");

        // If bootstrapping has completed, update the status flag.
        self.controller
            .status
            .set_bootstrap_in_progress(self.circuit.bootstrap_in_progress());

        // Update `trace_snapshot` to the latest traces.
        //
        // We do this before updating `total_processed_records` so that ad hoc
        // query results always reflect all data that we have reported
        // processing; otherwise, there is a race for any code that runs a query
        // as soon as input has been processed.
        self.update_snapshot();

        // Record that we've processed the records.
        let processed_records = self.processed_records(total_consumed);

        // Push output batches to output pipelines.
        if let Some(ft) = self.ft.as_mut() {
            ft.sync_step()?;
        }
        self.push_output(processed_records);

        if let Some(ft) = self.ft.as_mut() {
            ft.next_step(self.step)?;
            self.finish_replaying();
        }
        self.controller.unpark_backpressure();

        self.controller
            .status
            .global_metrics
            .runtime_elapsed_msecs
            .store(
                self.circuit.runtime_elapsed().as_millis() as u64,
                Ordering::Relaxed,
            );

        Ok(true)
    }

    // Update `trace_snapshot` to the latest traces.
    //
    // This updates what ad hoc snapshots query.
    fn update_snapshot(&mut self) {
        let mut consistent_snapshot = self.controller.trace_snapshot.blocking_lock();
        for (name, clh) in self.controller.catalog.output_iter() {
            if let Some(ih) = &clh.integrate_handle {
                consistent_snapshot.insert(name.clone(), ih.take_from_all());
            }
        }
    }

    fn checkpoint_requested(&self) -> bool {
        !self.checkpoint_requests.is_empty()
    }

    fn checkpoint(&mut self) {
        fn inner(this: &mut CircuitThread) -> Result<Checkpoint, Arc<ControllerError>> {
            this.controller
                .can_suspend()
                .map_err(|e| Arc::new(ControllerError::SuspendError(e)))?;

            // Replace the input adapter configuration in the pipeline configuration
            // by the current inputs. (HTTP input adapters might have been added or
            // removed.)
            let config = PipelineConfig {
                inputs: this
                    .controller
                    .status
                    .input_status()
                    .iter()
                    .map(|(_id, status)| {
                        (Cow::from(status.endpoint_name.clone()), {
                            let mut config = status.config.clone();
                            config.connector_config.paused = status.is_paused_by_user();
                            config
                        })
                    })
                    .collect(),
                ..this.controller.status.pipeline_config.clone()
            };

            let input_metadata = this
                .input_metadata
                .iter()
                .map(|(name, resume)| {
                    (
                        name.clone(),
                        resume.as_ref().unwrap().seek().unwrap().clone(),
                    )
                })
                .collect();
            let processed_records = this
                .controller
                .status
                .global_metrics
                .num_total_processed_records();
            let checkpoint = this
                .circuit
                .commit_with_metadata(this.step, processed_records)
                .map_err(|e| Arc::new(ControllerError::from(e)))
                .and_then(|circuit| {
                    let uuid = circuit.uuid.to_string();
                    let checkpoint = Checkpoint {
                        circuit: Some(circuit),
                        step: this.step,
                        config,
                        processed_records,
                        input_metadata: CheckpointOffsets(input_metadata),
                    };
                    checkpoint
                        .write(
                            &**this.storage.as_ref().unwrap(),
                            &StoragePath::from(STATE_FILE),
                        )
                        .map_err(Arc::new)?;
                    checkpoint
                        .write(
                            &**this.storage.as_ref().unwrap(),
                            &StoragePath::from(uuid).child(STATE_FILE),
                        )
                        .map(|()| checkpoint)
                        .map_err(Arc::new)
                })?;

            // TODO: check the requested checkpoint UUID
            if this.sync_checkpoint_request.is_none() {
                if let Err(error) = this.circuit.gc_checkpoint() {
                    warn!("error removing old checkpoints: {error}");
                }
            }
            if let Some(ft) = &mut this.ft {
                ft.checkpointed()?;
            }
            Ok(checkpoint)
        }

        let result = match inner(self) {
            Err(e)
                if matches!(
                    e.as_ref(),
                    ControllerError::SuspendError(SuspendError::Temporary(_))
                ) =>
            {
                let ControllerError::SuspendError(error) = e.as_ref() else {
                    unreachable!()
                };

                self.checkpoint_delay_warning
                    .get_or_insert_with(|| LongOperationWarning::new(Duration::from_secs(1)))
                    .check(|elapsed| {
                        info!(
                            "checkpoint delayed {} seconds because of: {error}",
                            elapsed.as_secs()
                        )
                    });
                return;
            }
            Err(error) => {
                warn!("checkpoint failed: {error}");
                Err(error)
            }
            Ok(checkpoint) => Ok(checkpoint),
        };
        self.checkpoint_delay_warning = None;

        // Update the last checkpoint time *after* executing the checkpoint, so
        // that if the checkpoint takes a long time and we have a short
        // checkpoint interval, we get to do other work too.
        //
        // We always update `last_checkpoint`, even if there was an error,
        // because we do not want to spend all our time checkpointing if there's
        // a problem and a short checkpoint interval.
        self.last_checkpoint = Instant::now();

        for request in self.checkpoint_requests.drain(..) {
            match request {
                CheckpointRequest::Scheduled => (),
                CheckpointRequest::CheckpointCommand(callback) => callback(result.clone()),
                CheckpointRequest::SuspendCommand(callback) => {
                    self.controller.status.set_state(PipelineState::Terminated);
                    if let Err(e) = &result {
                        self.controller.error(e.clone());
                    }
                    callback(result.clone().map(|_| ()))
                }
            }
        }

        // We may have disabled FT during backfill. Re-enable it after
        // reaching a checkpoint.
        if let Some(ft) = &mut self.ft {
            ft.enable();
        }
    }

    /// Reads and executes all the commands pending from
    /// `self.command_receiver`.
    fn run_commands(&mut self) {
        while let Ok(command) = self.command_receiver.try_recv() {
            match command {
                Command::GraphProfile(reply_callback) => reply_callback(
                    self.circuit
                        .graph_profile()
                        .map_err(ControllerError::dbsp_error),
                ),
                Command::Checkpoint(reply_callback) => {
                    self.checkpoint_requests
                        .push(CheckpointRequest::CheckpointCommand(reply_callback));
                }
                Command::Suspend(reply_callback) => {
                    self.checkpoint_requests
                        .push(CheckpointRequest::SuspendCommand(reply_callback));
                }
                Command::SyncCheckpoint((uuid, reply_callback)) => {
                    self.sync_checkpoint_request = Some((uuid, reply_callback));
                }
            }
        }
    }

    /// Reads and replies to all of the commands pending from
    /// `self.command_receiver` and all of the pending checkpoints in
    /// `self.checkpoint_requests`, without executing them.
    fn flush_commands_and_requests(&mut self) {
        for request in self.checkpoint_requests.drain(..) {
            request.flush();
        }
        for command in self.command_receiver.try_iter() {
            command.flush();
        }
    }

    /// Requests all of the input adapters to flush their input to the circuit,
    /// and waits for them to finish doing it.
    ///
    /// Returns:
    ///
    /// - `Ok(Some(total_consumed))` if successful.
    /// - `Ok(None)` if the pipeline was terminated before all the input could
    ///   be flushed.  The pipeline is in an inconsistent state (some input
    ///   adapters are still flushing data to the circuit) that can't be
    ///   recovered, so the pipeline process should exit as soon as it can.
    /// - `Err(error)` if there was an error.
    fn input_step(&mut self) -> Result<Option<u64>, ControllerError> {
        // No ingestion during bootstrap.
        if self.controller.status.bootstrap_in_progress() {
            return Ok(Some(0));
        }
        let mut step_metadata = HashMap::new();
        if let Some(replay_step) = self.ft.as_ref().and_then(|ft| ft.replay_step.as_ref()) {
            // Reuse input logs from the original step.  Usually, these logs are
            // not used for anything, since we're replaying, but in the corner
            // case where this is the final step to replay and we immediately
            // checkpoint, the checkpoint must have the log records so that, on
            // resume, it can seek the input connectors to the proper location.
            for (name, log) in replay_step.input_logs.iter() {
                let endpoint_id = self.controller.input_endpoint_id_by_name(name).unwrap();
                step_metadata.insert(
                    endpoint_id,
                    (
                        name.clone(),
                        StepResults {
                            resume: Some(Resume::Replay {
                                seek: log.metadata.clone(),
                                replay: log.data.clone(),
                                hash: log.checksums.hash,
                            }),
                            num_records: log.checksums.num_records,
                        },
                    ),
                );
            }
        }

        // If `checkpoint_requests` is nonempty, then a client has requested a
        // checkpoint, but we haven't been able to write that checkpoint yet.
        // The reasons we haven't written depends on whether exactly-once fault
        // tolerance is enabled:
        //
        // - If exactly once fault tolerance is enabled, then replay has to
        //   complete before we can write the checkpoint
        //   ([TemporarySuspendError::Replaying]).  No special action is needed.
        //
        // - If exactly once fault tolerance is not enabled, then some input
        //   endpoint is presenting a checkpoint barrier because it is between
        //   possible checkpoint steps
        //   ([TemporarySuspendError::InputEndpointBarrier]).  In that case, we
        //   will only flush input for the barrier endpoints (otherwise, it's
        //   possible non-barrier endpoints will become barriers and we
        //   definitely don't want that).
        let barriers_only = self.checkpoint_requested() && self.ft.is_none();

        // Collect the ids of the endpoints that we'll flush to the circuit.
        //
        // The set of endpoint ids could change while we're waiting, because
        // adapters can be hot-added and hot-removed (and this really happens
        // with HTTP and ad-hoc input adapters). We only wait for the ones that
        // exist when we start flushing input. New ones will be processed in
        // future steps.
        let statuses = self.controller.status.input_status();
        let mut waiting = HashSet::with_capacity(statuses.len());
        for (&endpoint_id, status) in statuses.iter() {
            if barriers_only && !status.is_barrier() {
                if let Some(resume) = self.input_metadata.remove(&status.endpoint_name) {
                    step_metadata.insert(
                        endpoint_id,
                        (
                            status.endpoint_name.clone(),
                            StepResults {
                                num_records: 0,
                                resume,
                            },
                        ),
                    );
                }
            } else {
                if !self.replaying() {
                    if let Some(reader) = status.reader.as_ref() {
                        reader.queue(self.checkpoint_requested())
                    }
                } else {
                    // We already started the input adapters replaying. The set of
                    // input adapters can't change during replay (because we disable
                    // the APIs that could do that during replay).
                }
                waiting.insert(endpoint_id);
            }
        }
        drop(statuses);

        let mut total_consumed = 0;

        let mut long_op_warning = LongOperationWarning::new(Duration::from_secs(10));
        loop {
            // Process input and check for failed input adapters.
            let statuses = self.controller.status.input_status();
            let mut failed = Vec::new();
            waiting.retain(|&endpoint_id| {
                let Some(status) = statuses.get(&endpoint_id) else {
                    // Input adapter was deleted without yielding any input.
                    return false;
                };
                let Some(results) = mem::take(&mut *status.progress.lock().unwrap()) else {
                    // Check for failure.
                    //
                    // We check for results first, because if an input adapter
                    // queues its last input batch and then exits, we want to
                    // take the input.
                    if status
                        .reader
                        .as_ref()
                        .map(|reader| reader.is_closed())
                        .unwrap_or(false)
                    {
                        if status.metrics.end_of_input.load(Ordering::Acquire) {
                            warn!("Input endpoint {} exited", status.endpoint_name);
                        } else {
                            warn!("Input endpoint {} failed", status.endpoint_name);
                        }
                        failed.push(endpoint_id);
                        return false;
                    } else {
                        return true;
                    }
                };

                // Input received.
                total_consumed += results.num_records;
                step_metadata.insert(endpoint_id, (status.endpoint_name.clone(), results));
                false
            });
            drop(statuses);
            for endpoint_id in failed {
                self.controller.disconnect_input(&endpoint_id);
            }

            // Are we done?
            if waiting.is_empty() {
                break;
            }

            // Warn if we've waited a long time.
            long_op_warning.check(|elapsed| {
                warn!(
                    "still waiting to complete input step after {} seconds",
                    elapsed.as_secs()
                )
            });

            // Wait for something to change.
            self.parker.park_timeout(Duration::from_secs(1));
            if self.controller.state() == PipelineState::Terminated {
                return Ok(None);
            }
        }

        // Save metadata for checkpointing this step.
        self.input_metadata = match self.ft.as_ref().and_then(|ft| ft.replay_step.as_ref()) {
            None => {
                // Common case: We just completed a normal step.  Collect its
                // metadata.
                let mut input_metadata = HashMap::new();

                let statuses = self.controller.status.input_status();
                for (&endpoint_id, (_name, results)) in step_metadata.iter() {
                    let Some(endpoint) = statuses.get(&endpoint_id) else {
                        // The endpoint has been removed, so there's nothing to record
                        // in the checkpoint.
                        continue;
                    };
                    input_metadata.insert(endpoint.endpoint_name.clone(), results.resume.clone());
                    let barrier = results
                        .resume
                        .as_ref()
                        .is_some_and(|resume| resume.is_barrier());
                    endpoint.set_barrier(barrier);
                }
                drop(statuses);

                input_metadata
            }
            Some(replay_step) => {
                // Special case: We just replayed a step.  Make a copy of the
                // metadata to handle the corner case where we execute a
                // checkpoint immediately after we finish replaying but before
                // we execute our first non-replayed step.  See the
                // `ft_immediate_checkpoints` test.
                replay_step
                    .input_logs
                    .iter()
                    .map(|(name, log)| {
                        (
                            name.clone(),
                            Some(Resume::Seek {
                                seek: log.metadata.clone(),
                            }),
                        )
                    })
                    .collect()
            }
        };

        if let Some(ft) = &mut self.ft {
            ft.write_step(step_metadata, self.step)?;
        }

        Ok(Some(total_consumed))
    }

    /// Reports that `total_consumed` records have been consumed.
    ///
    /// Returns the total number of records processed.
    fn processed_records(&mut self, total_consumed: u64) -> u64 {
        let processed_records = self
            .controller
            .status
            .global_metrics
            .processed_records(total_consumed);
        self.controller.status.update_total_completed_records();
        processed_records
    }

    /// Pushes all of the records to the output.
    ///
    /// `processed_records` is the total number of records processed by the
    /// pipeline *before* this step.
    fn push_output(&mut self, processed_records: u64) {
        let outputs = self.controller.outputs.read().unwrap();
        for (_stream, (output_handles, endpoints)) in outputs.iter_by_stream() {
            let delta_batch = output_handles.delta_handle.as_ref().take_from_all();
            let num_delta_records = delta_batch.iter().map(|b| b.len()).sum();

            let mut delta_batch = Some(delta_batch);

            for (i, endpoint_id) in endpoints.iter().enumerate() {
                let endpoint = outputs.lookup_by_id(endpoint_id).unwrap();

                self.controller
                    .status
                    .enqueue_batch(*endpoint_id, num_delta_records);

                let batch = if i == endpoints.len() - 1 {
                    delta_batch.take().unwrap()
                } else {
                    delta_batch.as_ref().unwrap().clone()
                };

                endpoint.queue.push((self.step, batch, processed_records));

                // Wake up the output thread.  We're not trying to be smart here and
                // wake up the thread conditionally if it was previously idle, as I
                // don't expect this to make any real difference.
                endpoint.unparker.unpark();
            }
        }
        drop(outputs);
    }

    fn replaying(&self) -> bool {
        self.ft.as_ref().is_some_and(|ft| ft.is_replaying())
    }

    fn sync_checkpoint_requested(&self) -> bool {
        self.sync_checkpoint_request.is_some()
    }

    #[allow(unused)]
    pub fn list_checkpoints(&mut self) -> Result<Vec<CheckpointMetadata>, Arc<ControllerError>> {
        self.circuit
            .list_checkpoints()
            .map_err(|e| Arc::new(ControllerError::dbsp_error(e)))
    }

    fn sync_checkpoint(&mut self) {
        let Some((uuid, cb)) = self.sync_checkpoint_request.take() else {
            return;
        };

        let Some((_, options)) = self.controller.status.pipeline_config.storage() else {
            cb(Err(Arc::new(ControllerError::storage_error(
                "cannot sync checkpoints when storage is disabled".to_owned(),
                dbsp::storage::backend::StorageError::StorageDisabled,
            ))));
            return;
        };

        let feldera_types::config::StorageBackendConfig::File(FileBackendConfig {
            sync: Some(ref sync),
            ..
        }) = options.backend
        else {
            cb(Err(Arc::new(ControllerError::storage_error(
                "syncing checkpoint is only supported with file backend".to_owned(),
                dbsp::storage::backend::StorageError::BackendNotSupported(Box::new(
                    options.backend.clone(),
                )),
            ))));
            return;
        };

        let Some(synchronizer) = inventory::iter::<&dyn CheckpointSynchronizer>
            .into_iter()
            .next()
        else {
            cb(Err(Arc::new(ControllerError::checkpoint_push_error(
                "no checkpoint synchronizer found; are enterprise features enabled?".to_owned(),
            ))));
            return;
        };

        let Some(ref storage) = self.storage else {
            tracing::error!("storage is set to None");
            cb(Err(Arc::new(ControllerError::checkpoint_push_error(
                "cannot push checkpoints to object store: storage is set to None".to_owned(),
            ))));
            return;
        };

        let storage = storage.to_owned();
        let config = sync.to_owned();

        thread::Builder::new()
            .name("s3-synchronizer".to_string())
            .spawn(move || {
                if let Err(err) = synchronizer.push(uuid, storage.clone(), config.clone()) {
                    cb(Err(Arc::new(ControllerError::checkpoint_push_error(
                        err.to_string(),
                    ))));
                } else {
                    cb(Ok(()))
                }
            })
            .expect("failed to spawn s3-synchronizer thread");
    }
}

enum CheckpointRequest {
    Scheduled,
    CheckpointCommand(CheckpointCallbackFn),
    SuspendCommand(SuspendCallbackFn),
}

impl CheckpointRequest {
    pub fn flush(self) {
        match self {
            CheckpointRequest::Scheduled => (),
            CheckpointRequest::CheckpointCommand(callback) => {
                callback(Err(Arc::new(ControllerError::ControllerExit)))
            }
            CheckpointRequest::SuspendCommand(callback) => {
                callback(Err(Arc::new(ControllerError::ControllerExit)))
            }
        }
    }
}

/// Tracks fault-tolerant state in a controller [CircuitThread].
struct FtState {
    /// Used to temporarily disable journaling.
    enabled: bool,

    /// The controller.
    controller: Arc<ControllerInner>,

    /// The journal.
    journal: Journal,

    /// The journal record that we're replaying, if we're replaying.
    replay_step: Option<StepMetadata>,

    /// Input endpoint ids, names, and whether the endpoints are paused, at the
    /// time we wrote the last step, so that we can log changes for the replay
    /// log.
    input_endpoints: HashMap<EndpointId, (String, bool)>,
}

impl FtState {
    /// Initializes fault tolerance state from storage.
    fn open(
        backend: Arc<dyn StorageBackend>,
        step: Step,
        controller: Arc<ControllerInner>,
    ) -> Result<Self, ControllerError> {
        info!("{STEPS_FILE}: opening to start from step {step}");
        let journal = Journal::open(backend, &StoragePath::from(STEPS_FILE));
        let replay_step = journal.read(step)?;
        if let Some(record) = &replay_step {
            // Start replaying the step.
            Self::replay_step(step, record, &controller)?;
        }
        Ok(Self {
            enabled: true,
            input_endpoints: Self::initial_input_endpoints(&controller),
            controller,
            replay_step,
            journal,
        })
    }

    /// Creates new fault tolerance state on storage.
    fn create(
        backend: Arc<dyn StorageBackend>,
        controller: Arc<ControllerInner>,
    ) -> Result<Self, ControllerError> {
        let config = controller.status.pipeline_config.clone();
        for file in [STATE_FILE, STEPS_FILE] {
            backend
                .delete_if_exists(&StoragePath::from(file))
                .map_err(|error| {
                    ControllerError::storage_error("initializing fault tolerant pipeline", error)
                })?;
        }

        info!("{STEPS_FILE}: creating");
        let journal = Journal::create(backend.clone(), &StoragePath::from(STEPS_FILE))?;

        info!("{STATE_FILE}: creating");
        let checkpoint = Checkpoint {
            circuit: None,
            step: 0,
            config,
            processed_records: 0,
            input_metadata: CheckpointOffsets::default(),
        };
        checkpoint.write(&*backend, &StoragePath::from(STATE_FILE))?;

        Ok(Self {
            enabled: true,
            input_endpoints: Self::initial_input_endpoints(&controller),
            controller,
            replay_step: None,
            journal,
        })
    }

    fn disable(&mut self) {
        self.enabled = false;
    }

    fn enable(&mut self) {
        self.enabled = true;
    }

    fn initial_input_endpoints(
        controller: &ControllerInner,
    ) -> HashMap<EndpointId, (String, bool)> {
        controller
            .status
            .inputs
            .read()
            .unwrap()
            .iter()
            .map(|(id, status)| {
                (
                    *id,
                    (status.endpoint_name.clone(), status.is_paused_by_user()),
                )
            })
            .collect()
    }

    fn replay_step(
        step: Step,
        metadata: &StepMetadata,
        controller: &Arc<ControllerInner>,
    ) -> Result<(), ControllerError> {
        if metadata.step != step {
            return Err(ControllerError::UnexpectedStep {
                actual: metadata.step,
                expected: step,
            });
        }
        info!("replaying input step {}", step);
        for endpoint_name in &metadata.remove_inputs {
            let endpoint_id = controller.input_endpoint_id_by_name(endpoint_name)?;
            controller.disconnect_input(&endpoint_id);
        }
        for (endpoint_name, config) in &metadata.add_inputs {
            controller.connect_input(endpoint_name, config, None)?;
        }
        for (endpoint_name, pause) in &metadata.changed_inputs {
            if *pause {
                controller.pause_input_endpoint(endpoint_name)?;
            } else {
                controller.start_input_endpoint(endpoint_name)?;
            }
        }
        for (endpoint_name, log) in &metadata.input_logs {
            let endpoint_id = controller.input_endpoint_id_by_name(endpoint_name)?;
            if let Some(reader) = controller.status.input_status()[&endpoint_id]
                .reader
                .as_ref()
            {
                reader.replay(log.metadata.clone(), log.data.clone())
            }
        }
        Ok(())
    }

    /// Writes `step_metadata` to the step writer.
    fn write_step(
        &mut self,
        step_metadata: HashMap<u64, (String, StepResults)>,
        step: Step,
    ) -> Result<(), ControllerError> {
        if !self.enabled {
            return Ok(());
        }
        match &self.replay_step {
            None => {
                let mut remove_inputs = HashSet::new();
                let mut add_inputs = HashMap::new();
                let mut changed_inputs = HashMap::new();
                let inputs = self.controller.status.inputs.read().unwrap();

                // Stop recording if the controller is shutting down. Avoid race with
                // `stop`, which removes input endpoints from the pipeline. Without this
                // check we may end up recording these endpoints in `remove_inputs`.
                if self.controller.state() == PipelineState::Terminated {
                    return Ok(());
                }
                self.input_endpoints
                    .retain(|endpoint_id, (endpoint_name, paused)| {
                        if let Some(endpoint) = inputs.get(endpoint_id) {
                            let now_paused = endpoint.is_paused_by_user();
                            if *paused != now_paused {
                                changed_inputs.insert(endpoint_name.clone(), now_paused);
                                *paused = now_paused;
                            }
                            true
                        } else {
                            remove_inputs.insert(endpoint_name.clone());
                            false
                        }
                    });
                for (endpoint_id, status) in inputs.iter() {
                    self.input_endpoints.entry(*endpoint_id).or_insert_with(|| {
                        add_inputs.insert(status.endpoint_name.clone(), status.config.clone());
                        (status.endpoint_name.clone(), status.is_paused_by_user())
                    });
                }
                drop(inputs);

                let input_logs = step_metadata
                    .into_values()
                    .map(|(name, result)| (name, result.try_into().unwrap()))
                    .collect();
                let step_metadata = StepMetadata {
                    step,
                    remove_inputs,
                    add_inputs,
                    changed_inputs,
                    input_logs,
                };
                self.journal.write(&step_metadata)?;
            }
            Some(record) => {
                let mut logged = HashMap::new();
                for (name, log) in &record.input_logs {
                    logged.insert(name, log.checksums);
                }

                let mut replayed = HashMap::new();
                for (name, results) in step_metadata.values() {
                    replayed.insert(name, results.checksums().unwrap());
                }

                if replayed != logged {
                    let error = format!("Logged and replayed step {step} contained different numbers of records or hashes:\nLogged: {logged:?}\nReplayed: {replayed:?}");
                    error!("{error}");
                    return Err(ControllerError::ReplayFailure { error });
                }
            }
        };
        Ok(())
    }

    /// Waits for the step writer to commit the step (written by
    /// [Self::write_step]) to stable storage.
    fn sync_step(&mut self) -> Result<(), ControllerError> {
        //self.journal.wait()?;
        Ok(())
    }

    /// If we just replayed a step, try to replay the next one too.
    fn next_step(&mut self, step: Step) -> Result<(), ControllerError> {
        if !self.enabled {
            return Ok(());
        }

        if self.is_replaying() {
            // Read a step.
            self.replay_step = self.journal.read(step)?;
            match &self.replay_step {
                None => {
                    // No more steps to replay.
                    info!("replay complete, starting pipeline");
                    self.replay_step = None;
                    self.input_endpoints = Self::initial_input_endpoints(&self.controller);
                }
                Some(record) => {
                    // There's a step to replay.
                    Self::replay_step(step, record, &self.controller)?;
                }
            };
        }
        Ok(())
    }

    /// Truncates the journal (because we just checkpointed).
    fn checkpointed(&mut self) -> Result<(), ControllerError> {
        self.journal.truncate()?;
        Ok(())
    }

    fn is_replaying(&self) -> bool {
        self.replay_step.is_some()
    }
}

/// Decides when to trigger a step.
struct StepTrigger {
    /// Time when `max_buffering_delay` expires.
    buffer_timeout: Option<Instant>,

    controller: Arc<ControllerInner>,

    /// Maximum time to wait before stepping after receiving at least one record
    /// but fewer than `min_batch_size_records`.
    max_buffering_delay: Duration,

    /// Minimum number of records to receive before unconditionally triggering a
    /// step.
    min_batch_size_records: u64,

    /// Time between automatic checkpoints.
    checkpoint_interval: Option<Duration>,

    /// The circuit needs to perform an initial step even if there are no
    /// new inputs in order to initialize state snapshot for ad hoc queries.
    needs_first_step: bool,

    /// The circuit is bootstrapping. Used to detect the transition from bootstrapping
    /// to normal mode.
    bootstrapping: bool,
}

/// Action for the controller to take.
#[derive(Debug)]
enum Action {
    /// Park until time `.0`, or forever if `None`.
    Park(Option<Instant>),

    /// Write a checkpoint.
    Checkpoint,

    /// Step the circuit.
    Step,
}

impl StepTrigger {
    /// Returns a new [StepTrigger].
    fn new(controller: Arc<ControllerInner>) -> Self {
        let config = &controller.status.pipeline_config.global;
        let max_buffering_delay = Duration::from_micros(config.max_buffering_delay_usecs);
        let min_batch_size_records = config.min_batch_size_records;
        let checkpoint_interval = config.fault_tolerance.checkpoint_interval();
        Self {
            controller,
            buffer_timeout: None,
            max_buffering_delay,
            min_batch_size_records,
            checkpoint_interval,
            needs_first_step: true,
            bootstrapping: false,
        }
    }

    /// Determines when to trigger the next step, given:
    ///
    /// - The time of the last checkpoint.
    /// - Whether we're currently `replaying`.
    /// - Whether the pipeline is currently `bootstrapping`.
    /// - Whether the pipeline is currently `running`.
    /// - Whether a checkpoint has already been requested.
    ///
    /// Returns the action for the controller to take.
    fn trigger(
        &mut self,
        last_checkpoint: Instant,
        replaying: bool,
        bootstrapping: bool,
        checkpoint_requested: bool,
    ) -> Action {
        // If any input endpoints are blocking suspend, then those are the only
        // ones that we count; otherwise, count all of them.
        //
        // An input endpoint is blocking suspend if it has a barrier and a
        // checkpoint has been requested.
        let mut buffered_records = EnumMap::<bool, u64>::default();
        for status in self.controller.status.input_status().values() {
            buffered_records[checkpoint_requested && status.is_barrier()] +=
                status.metrics.buffered_records.load(Ordering::Relaxed);
        }
        let buffered_records = if buffered_records[true] > 0 {
            buffered_records[true]
        } else {
            buffered_records[false]
        };

        // Time of the next checkpoint.
        let checkpoint = self
            .checkpoint_interval
            .map(|interval| last_checkpoint + interval);

        fn step(trigger: &mut StepTrigger) -> Action {
            trigger.needs_first_step = false;
            trigger.buffer_timeout = None;
            Action::Step
        }

        let now = Instant::now();

        // The last condition detects a transition from bootstrapping to normal
        // operation and makes sure that the circuit performs an extra step in the normal
        // mode in order to initialize output table snapshots of output relations that
        // did not participate in bootstrapping.
        let result = if replaying || bootstrapping || self.bootstrapping {
            step(self)
        } else if checkpoint.is_some_and(|t| now >= t) && !checkpoint_requested {
            Action::Checkpoint
        } else if self.controller.status.unset_step_requested()
            || buffered_records > self.min_batch_size_records
            || self.needs_first_step
            || self.buffer_timeout.is_some_and(|t| now >= t)
        {
            step(self)
        } else {
            if buffered_records > 0 && self.buffer_timeout.is_none() {
                self.buffer_timeout = Some(now + self.max_buffering_delay);
            }
            let wakeup = [self.buffer_timeout, checkpoint]
                .into_iter()
                .flatten()
                .min();
            Action::Park(wakeup)
        };

        self.bootstrapping = bootstrapping;

        result
    }
}

/// Controller initialization.
///
/// When we start a controller, we do one of:
///
/// - Start from an existing checkpoint.
///
/// - Start a new pipeline with a new initial checkpoint.
///
/// - Start a new pipeline without any checkpoint support.
///
/// This structure handles all these cases.
struct ControllerInit {
    /// The circuit configuration.
    circuit_config: CircuitConfig,

    /// The pipeline configuration.
    ///
    /// This will differ from the one passed into [ControllerInit::new] if a
    /// checkpoint is read, because a checkpoint includes the pipeline
    /// configuration.
    pipeline_config: PipelineConfig,

    /// Initial counter for `total_processed_records`.
    processed_records: u64,

    /// The first step that the circuit will execute.
    step: Step,

    /// Metadata for seeking to input endpoint initial positions.
    ///
    /// This is `Some` iff we read a checkpoint.
    input_metadata: Option<HashMap<String, JsonValue>>,
}

impl ControllerInit {
    fn without_resume(
        config: PipelineConfig,
        storage: Option<CircuitStorageConfig>,
    ) -> Result<Self, ControllerError> {
        Ok(Self {
            circuit_config: Self::circuit_config(&config, storage)?,
            pipeline_config: config,
            processed_records: 0,
            step: 0,
            input_metadata: None,
        })
    }
    fn new(config: PipelineConfig) -> Result<Self, ControllerError> {
        let Some((storage_config, storage_options)) = config.storage() else {
            if !config.global.fault_tolerance.is_enabled() {
                info!("storage not configured, so suspend-and-resume and fault tolerance will not be available");
                return Self::without_resume(config, None);
            } else {
                return Err(ControllerError::Config {
                    config_error: Box::new(ConfigError::FtRequiresStorage),
                });
            }
        };
        let storage =
            CircuitStorageConfig::for_config(storage_config.clone(), storage_options.clone())
                .map_err(|error| {
                    ControllerError::storage_error("failed to initialize storage", error)
                })?;

        #[cfg(feature = "feldera-enterprise")]
        {
            use feldera_storage::checkpoint_synchronizer::CheckpointSynchronizer;
            use feldera_types::config::FileBackendConfig;

            if let feldera_types::config::StorageBackendConfig::File(FileBackendConfig {
                sync: Some(ref sync),
                ..
            }) = storage.options.backend
            {
                if sync.start_from_checkpoint.is_some() {
                    let Some(synchronizer) = inventory::iter::<&dyn CheckpointSynchronizer>
                        .into_iter()
                        .next()
                    else {
                        return Err(ControllerError::checkpoint_fetch_error(
                            "no checkpoint synchronizer found; are enterprise features enabled?"
                                .to_owned(),
                        ));
                    };

                    if let Err(err) = synchronizer
                        .pull(storage.backend.clone(), sync.to_owned())
                        .map_err(|e| ControllerError::checkpoint_fetch_error(e.to_string()))
                    {
                        if sync.strict_start_from {
                            return Err(err);
                        } else {
                            tracing::error!("{}", err.to_string())
                        }
                    }
                }
            }
        }

        // Try to read a checkpoint.
        let checkpoint = match Checkpoint::read(&*storage.backend, &StoragePath::from(STATE_FILE)) {
            Err(error) if error.kind() == ErrorKind::NotFound => {
                info!("starting fresh pipeline without resuming from checkpoint");
                return Self::without_resume(config, Some(storage));
            }
            Err(error) => return Err(error),
            Ok(checkpoint) => checkpoint,
        };

        let Checkpoint {
            circuit,
            step,
            config: checkpoint_config,
            processed_records,
            input_metadata,
        } = checkpoint;
        info!("resuming from checkpoint made at step {step}");

        let storage = storage.with_init_checkpoint(circuit.map(|circuit| circuit.uuid));

        // Merge `config` (the configuration provided by the pipeline manager)
        // with `checkpoint_config` (the configuration read from the
        // checkpoint).
        //
        // We want to take each setting from `config` if we can, or from
        // `checkpoint_config` if we're not prepared to handle changes.
        //
        // This is intentionally written without using `..default` syntax, so
        // that we get a compiler error for new fields.  That's because it's
        // hard to guess whether any new settings are ones that we can adopt
        // without change elsewhere.
        let config = PipelineConfig {
            global: RuntimeConfig {
                // Can't change number of workers yet.
                workers: checkpoint_config.global.workers,

                // The checkpoint determines the fault tolerance model, but the
                // pipeline manager can override the details of the
                // configuration (so far just the checkpoint interval).
                fault_tolerance: FtConfig {
                    model: checkpoint_config.global.fault_tolerance.model,
                    checkpoint_interval_secs: config
                        .global
                        .fault_tolerance
                        .checkpoint_interval_secs,
                },

                // Take all the other settings from the pipeline manager.
                storage: config.global.storage,
                cpu_profiler: config.global.cpu_profiler,
                tracing: config.global.tracing,
                tracing_endpoint_jaeger: config.global.tracing_endpoint_jaeger,
                min_batch_size_records: config.global.min_batch_size_records,
                max_buffering_delay_usecs: config.global.max_buffering_delay_usecs,
                resources: config.global.resources,
                clock_resolution_usecs: config.global.clock_resolution_usecs,
                pin_cpus: config.global.pin_cpus,
                provisioning_timeout_secs: config.global.provisioning_timeout_secs,
                max_parallel_connector_init: config.global.max_parallel_connector_init,
                init_containers: config.global.init_containers,
                checkpoint_during_suspend: config.global.checkpoint_during_suspend,
                http_workers: config.global.http_workers,
                io_workers: config.global.io_workers,
                dev_tweaks: config.global.dev_tweaks.clone(),
                logging: config.global.logging,
            },

            // Adapter configuration has to come from the checkpoint.
            inputs: checkpoint_config.inputs,
            outputs: checkpoint_config.outputs,

            // Other settings from the pipeline manager.
            secrets_dir: config.secrets_dir,
            name: config.name,
            storage_config: config.storage_config,
        };

        Ok(Self {
            circuit_config: Self::circuit_config(&config, Some(storage))?,
            pipeline_config: config,
            step,
            input_metadata: Some(input_metadata.0),
            processed_records,
        })
    }

    fn circuit_config(
        pipeline_config: &PipelineConfig,
        storage: Option<CircuitStorageConfig>,
    ) -> Result<CircuitConfig, ControllerError> {
        Ok(CircuitConfig {
            layout: Layout::new_solo(pipeline_config.global.workers as usize),
            pin_cpus: pipeline_config.global.pin_cpus.clone(),
            storage,
            mode: Mode::Persistent,
            dev_tweaks: DevTweaks::from_config(&pipeline_config.global.dev_tweaks),
        })
    }
}

struct BackpressureThread {
    exit: Arc<AtomicBool>,
    join_handle: Option<JoinHandle<()>>,
    controller: Arc<ControllerInner>,
    parker: Option<Parker>,
    unparker: Unparker,
}

impl BackpressureThread {
    /// Prepares to start a backpressure thread, but doesn't start it yet.
    fn new(controller: Arc<ControllerInner>, parker: Parker) -> Self {
        let exit = Arc::new(AtomicBool::new(false));
        let unparker = parker.unparker().clone();
        Self {
            exit: exit.clone(),
            controller,
            parker: Some(parker),
            unparker,
            join_handle: None,
        }
    }

    /// Starts the backpressure thread.
    ///
    /// This only has an effect once.
    fn start(&mut self) {
        if let Some(parker) = self.parker.take() {
            let exit = self.exit.clone();
            let controller = self.controller.clone();
            self.join_handle = Some(
                thread::Builder::new()
                    .name("backpressure-thread".to_string())
                    .spawn(move || Self::backpressure_thread(controller, parker, exit))
                    .expect("failed to spawn backpressure-thread"),
            );
        }
    }

    fn backpressure_thread(
        controller: Arc<ControllerInner>,
        parker: Parker,
        exit: Arc<AtomicBool>,
    ) {
        let mut running_endpoints = HashSet::new();

        while !exit.load(Ordering::Acquire) {
            let globally_running = match controller.state() {
                PipelineState::Paused => false,
                PipelineState::Running => true,
                PipelineState::Terminated => return,
            };

            let bootstrap_in_progress = controller.status.bootstrap_in_progress();

            for (epid, ep) in controller.status.input_status().iter() {
                let should_run = globally_running
                    && !bootstrap_in_progress
                    && !ep.is_paused_by_user()
                    && !ep.is_full();
                match should_run {
                    true => {
                        if running_endpoints.insert(*epid) {
                            if let Some(reader) = ep.reader.as_ref() {
                                reader.extend()
                            }
                        }
                    }
                    false => {
                        if running_endpoints.remove(epid) {
                            if let Some(reader) = ep.reader.as_ref() {
                                reader.pause()
                            }
                        }
                    }
                }
            }

            parker.park();
        }
    }
}

impl Drop for BackpressureThread {
    fn drop(&mut self) {
        self.exit.store(true, Ordering::Release);
        self.unparker.unpark();
        if let Some(join_handle) = self.join_handle.take() {
            let _ = join_handle.join();
        }
    }
}

/// Storage thread.
///
/// For now, this just wakes up once a second to update storage statistics in
/// [GlobalControllerMetrics].
struct StorageThread {
    exit: Arc<AtomicBool>,
    join_handle: Option<JoinHandle<()>>,
}

impl StorageThread {
    /// Starts a storage thread.
    fn new(
        storage_backend: &dyn StorageBackend,
        storage_bytes: Arc<AtomicU64>,
        storage_mb_secs: Arc<AtomicU64>,
    ) -> Self {
        let exit = Arc::new(AtomicBool::new(false));
        let usage = storage_backend.usage();
        let join_handle = thread::Builder::new()
            .name("dbsp-storage".into())
            .spawn({
                let exit = exit.clone();
                move || Self::storage_thread(usage, storage_bytes, storage_mb_secs, exit)
            })
            .unwrap();
        Self {
            exit,
            join_handle: Some(join_handle),
        }
    }

    /// Thread function.
    fn storage_thread(
        usage: Arc<AtomicI64>,
        storage_bytes: Arc<AtomicU64>,
        storage_mb_secs: Arc<AtomicU64>,
        exit: Arc<AtomicBool>,
    ) {
        let mut last = Instant::now();
        let mut storage_byte_msecs = 0;
        while !exit.load(Ordering::Acquire) {
            // Measure.
            let elapsed_msecs = last.elapsed().as_millis();
            let usage_bytes = usage.load(Ordering::Relaxed).max(0) as u128;
            last = Instant::now();

            // Update internal statistic.
            storage_byte_msecs += usage_bytes * elapsed_msecs;

            // Update published statistics.
            storage_bytes.store(usage_bytes as u64, Ordering::Relaxed);
            storage_mb_secs.store(
                (storage_byte_msecs / (1024 * 1024 * 1000))
                    .try_into()
                    .unwrap(),
                Ordering::Relaxed,
            );

            std::thread::park_timeout(Duration::from_secs(1));
        }
    }
}

impl Drop for StorageThread {
    fn drop(&mut self) {
        self.exit.store(true, Ordering::Release);
        if let Some(join_handle) = self.join_handle.take() {
            join_handle.thread().unpark();
            let _ = join_handle.join();
        }
    }
}

/// A lock-free queue used to send output batches from the circuit thread
/// to output endpoint threads.  Each entry is annotated with a progress label
/// that is equal to the number of input records fully processed by
/// DBSP before emitting this batch of outputs.  The label increases
/// monotonically over time.
type BatchQueue = SegQueue<(Step, Vec<Arc<dyn SerBatch>>, u64)>;

/// State tracked by the controller for each output endpoint.
struct OutputEndpointDescr {
    /// Endpoint name.
    endpoint_name: String,

    /// Stream name that the endpoint is connected to.
    stream_name: String,

    /// FIFO queue of batches read from the stream.
    queue: Arc<BatchQueue>,

    /// Used to notify the endpoint thread that the endpoint is being
    /// disconnected.
    disconnect_flag: Arc<AtomicBool>,

    /// Unparker for the endpoint thread.
    unparker: Unparker,
}

impl OutputEndpointDescr {
    pub fn new(endpoint_name: &str, stream_name: &str, unparker: Unparker) -> Self {
        Self {
            endpoint_name: endpoint_name.to_string(),
            stream_name: canonical_identifier(stream_name),
            queue: Arc::new(SegQueue::new()),
            disconnect_flag: Arc::new(AtomicBool::new(false)),
            unparker,
        }
    }
}

type StreamEndpointMap = BTreeMap<String, (OutputCollectionHandles, BTreeSet<EndpointId>)>;

struct OutputEndpoints {
    by_id: BTreeMap<EndpointId, OutputEndpointDescr>,
    by_stream: StreamEndpointMap,
}

impl OutputEndpoints {
    fn new() -> Self {
        Self {
            by_id: BTreeMap::new(),
            by_stream: BTreeMap::new(),
        }
    }

    fn iter_by_stream(
        &self,
    ) -> impl Iterator<
        Item = (
            &'_ String,
            &'_ (OutputCollectionHandles, BTreeSet<EndpointId>),
        ),
    > {
        self.by_stream.iter()
    }

    fn lookup_by_id(&self, endpoint_id: &EndpointId) -> Option<&OutputEndpointDescr> {
        self.by_id.get(endpoint_id)
    }

    fn lookup_by_name(&self, endpoint_name: &str) -> Option<&OutputEndpointDescr> {
        self.by_id
            .values()
            .find(|ep| ep.endpoint_name == endpoint_name)
    }

    fn insert(
        &mut self,
        endpoint_id: EndpointId,
        handles: OutputCollectionHandles,
        endpoint_descr: OutputEndpointDescr,
    ) {
        self.by_stream
            .entry(endpoint_descr.stream_name.clone())
            .or_insert_with(|| (handles, BTreeSet::new()))
            .1
            .insert(endpoint_id);
        self.by_id.insert(endpoint_id, endpoint_descr);
    }

    fn remove(&mut self, endpoint_id: &EndpointId) -> Option<OutputEndpointDescr> {
        self.by_id.remove(endpoint_id).inspect(|descr| {
            self.by_stream
                .get_mut(&descr.stream_name)
                .map(|(_, endpoints)| endpoints.remove(endpoint_id));
        })
    }
}

/// Buffer used by the output endpoint thread to accumulate outputs.
struct OutputBuffer {
    #[allow(unused)]
    endpoint_name: String,

    buffer: Option<Box<dyn SerTrace>>,

    /// Step number of the last update in the buffer.
    ///
    /// The endpoint will wait for this step to commit before sending the buffer
    /// out.
    buffered_step: Step,

    /// Time when the first batch was pushed to the buffer.
    buffer_since: Instant,

    /// Number of input records that will be fully processed after the buffer is flushed.
    ///
    /// This is a part of the progress tracking mechanism, which tracks the number of inputs
    /// to the pipeline that have been processed to completion.  It is currently used
    /// to determine when the circuit has run to completion.
    buffered_processed_records: u64,
}

impl OutputBuffer {
    /// Create an empty buffer.
    fn new(endpoint_name: &str) -> Self {
        Self {
            endpoint_name: endpoint_name.to_string(),
            buffer: None,
            buffered_step: 0,
            buffer_since: Instant::now(),
            buffered_processed_records: 0,
        }
    }

    /// Insert `batch` into the buffer.
    fn insert(&mut self, batch: Arc<dyn SerBatch>, step: Step, processed_records: u64) {
        if let Some(buffer) = &mut self.buffer {
            buffer.insert(batch);
        } else {
            self.buffer = Some(batch.into_trace());
            self.buffer_since = Instant::now();
        }
        self.buffered_step = step;
        self.buffered_processed_records = processed_records;
    }

    /// Returns `true` when it is time to flush the buffer either because it's full or
    /// because the max buffering timeout has expired.
    fn flush_needed(&self, config: &OutputBufferConfig) -> bool {
        if let Some(buffer) = &self.buffer {
            let buffer = buffer.as_ref();
            if buffer.len() >= config.max_output_buffer_size_records {
                return true;
            }

            if self.buffer_since.elapsed().as_millis()
                > config.max_output_buffer_time_millis as u128
            {
                return true;
            }
        }

        false
    }

    /// Time when the oldest data was inserted in the buffer.
    fn buffer_since(&self) -> Option<Instant> {
        if self.buffer.is_some() {
            Some(self.buffer_since)
        } else {
            None
        }
    }

    /// Return the contents of the buffer leaving it empty.
    fn take_buffer(&mut self) -> Option<Box<dyn SerTrace>> {
        self.buffer.take()
    }
}

pub type ConsistentSnapshots =
    Arc<TokioMutex<BTreeMap<SqlIdentifier, Vec<Arc<dyn SyncSerBatchReader>>>>>;

/// Controller state sharable across threads.
///
/// A reference to this struct is held by each input probe and by both
/// controller threads.
pub struct ControllerInner {
    pub status: Arc<ControllerStatus>,
    secrets_dir: PathBuf,
    num_api_connections: AtomicU64,
    command_sender: Sender<Command>,
    catalog: Arc<Box<dyn CircuitCatalog>>,
    lir: LirCircuit,
    // Always lock this after the catalog is locked to avoid deadlocks
    trace_snapshot: ConsistentSnapshots,
    next_input_id: Atomic<EndpointId>,
    outputs: ShardedLock<OutputEndpoints>,
    next_output_id: Atomic<EndpointId>,
    circuit_thread_unparker: Unparker,
    backpressure_thread_unparker: Unparker,
    error_cb: Box<dyn Fn(Arc<ControllerError>) + Send + Sync>,
    session_ctxt: SessionContext,
    fault_tolerance: Option<FtModel>,

    /// Is the circuit thread still restoring from a checkpoint (this includes the journal replay phase)?
    restoring: AtomicBool,
}

impl ControllerInner {
    fn new(
        config: PipelineConfig,
        catalog: Box<dyn CircuitCatalog>,
        lir: LirCircuit,
        error_cb: Box<dyn Fn(Arc<ControllerError>) + Send + Sync>,
        processed_records: u64,
        resume_info: &HashMap<String, JsonValue>,
    ) -> Result<(Parker, BackpressureThread, Receiver<Command>, Arc<Self>), ControllerError> {
        let status = Arc::new(ControllerStatus::new(config.clone(), processed_records));
        let circuit_thread_parker = Parker::new();
        let backpressure_thread_parker = Parker::new();
        let (command_sender, command_receiver) = channel();
        let session_ctxt = create_session_context(&config)?;
        let controller = Arc::new(Self {
            status,
            secrets_dir: config.secrets_dir().to_path_buf(),
            num_api_connections: AtomicU64::new(0),
            command_sender,
            catalog: Arc::new(catalog),
            lir,
            trace_snapshot: Arc::new(TokioMutex::new(BTreeMap::new())),
            next_input_id: Atomic::new(0),
            outputs: ShardedLock::new(OutputEndpoints::new()),
            next_output_id: Atomic::new(0),
            circuit_thread_unparker: circuit_thread_parker.unparker().clone(),
            backpressure_thread_unparker: backpressure_thread_parker.unparker().clone(),
            error_cb,
            session_ctxt,
            fault_tolerance: config.global.fault_tolerance.model,
            restoring: AtomicBool::new(config.global.fault_tolerance.is_enabled()),
        });
        controller.initialize_adhoc_queries();

        // Initialize input and output endpoints using a thread pool to initialize multiple connectors in parallel.
        let source_tasks =
            controller
                .status
                .pipeline_config
                .inputs
                .iter()
                .map(|(input_name, input_config)| {
                    let controller = controller.clone();
                    let input_name = input_name.clone();
                    let input_config = input_config.clone();

                    let resume_info = resume_info.get(&*input_name).cloned();

                    Box::new(move || {
                        catch_unwind(AssertUnwindSafe(|| {
                            controller.connect_input(&input_name, &input_config, resume_info)
                        }))
                        .unwrap_or_else(|_| Err(ControllerError::ControllerPanic))
                    }) as Box<dyn FnOnce() -> Result<_, ControllerError> + Send>
                });

        let sink_tasks =
            controller
                .status
                .pipeline_config
                .outputs
                .iter()
                .map(|(output_name, output_config)| {
                    let controller = controller.clone();
                    let output_name = output_name.clone();
                    let output_config = output_config.clone();
                    Box::new(move || {
                        catch_unwind(AssertUnwindSafe(|| {
                            controller.connect_output(&output_name, &output_config)
                        }))
                        .unwrap_or_else(|_| Err(ControllerError::ControllerPanic))
                    }) as Box<dyn FnOnce() -> Result<_, ControllerError> + Send>
                });

        let pool_size = config.max_parallel_connector_init();
        run_on_thread_pool(
            "connector-init",
            pool_size as usize,
            source_tasks.chain(sink_tasks),
        )?;

        let _ = controller.connect_input("now", &now_endpoint_config(&config), None);

        let backpressure_thread =
            BackpressureThread::new(controller.clone(), backpressure_thread_parker);
        Ok((
            circuit_thread_parker,
            backpressure_thread,
            command_receiver,
            controller,
        ))
    }

    fn input_endpoint_id_by_name(
        &self,
        endpoint_name: &str,
    ) -> Result<EndpointId, ControllerError> {
        self.status.input_endpoint_id_by_name(endpoint_name)
    }

    fn output_endpoint_id_by_name(
        &self,
        endpoint_name: &str,
    ) -> Result<EndpointId, ControllerError> {
        self.status.output_endpoint_id_by_name(endpoint_name)
    }

    fn initialize_adhoc_queries(self: &Arc<Self>) {
        // Sync feldera catalog with datafusion catalog
        for (name, clh) in self.catalog.output_iter() {
            let arrow_fields = relation_to_arrow_fields(&clh.value_schema.fields, false);
            let input_handle = self
                .catalog
                .input_collection_handle(name)
                .map(|ich| ich.handle.fork());

            let adhoc_tbl = Arc::new(AdHocTable::new(
                clh.integrate_handle.is_some(),
                Arc::downgrade(self),
                input_handle,
                name.clone(),
                Arc::new(Schema::new(arrow_fields)),
                self.trace_snapshot.clone(),
            ));

            // This should never fail (we're not registering the same table twice).
            let r = self
                .session_ctxt
                .register_table(name.sql_name(), adhoc_tbl)
                .expect("table registration failed");
            assert!(r.is_none(), "table {name} already registered");
        }
    }

    fn connect_input(
        self: &Arc<Self>,
        endpoint_name: &str,
        endpoint_config: &InputEndpointConfig,
        resume_info: Option<JsonValue>,
    ) -> Result<EndpointId, ControllerError> {
        let endpoint = input_transport_config_to_endpoint(
            &endpoint_config.connector_config.transport,
            endpoint_name,
            &self.secrets_dir,
        )
        .map_err(|e| ControllerError::input_transport_error(endpoint_name, true, e))?;

        // If `endpoint` is `None`, it means that the endpoint config specifies an integrated
        // input connector.  Such endpoints are instantiated inside `add_input_endpoint`.
        self.add_input_endpoint(
            endpoint_name,
            endpoint_config.clone(),
            endpoint,
            resume_info,
        )
    }

    pub fn disconnect_input(self: &Arc<Self>, endpoint_id: &EndpointId) {
        debug!("Disconnecting input endpoint {endpoint_id}");

        if let Some(ep) = self.status.remove_input(endpoint_id) {
            if let Some(reader) = ep.reader.as_ref() {
                reader.disconnect()
            }
            self.unpark_circuit();
            self.unpark_backpressure();
        }
    }

    pub fn add_input_endpoint(
        self: &Arc<Self>,
        endpoint_name: &str,
        endpoint_config: InputEndpointConfig,
        endpoint: Option<Box<dyn TransportInputEndpoint>>,
        resume_info: Option<JsonValue>,
    ) -> Result<EndpointId, ControllerError> {
        debug!("Adding input endpoint '{endpoint_name}'; config: {endpoint_config:?}");

        // NOTE: We release the lock after the check below and then re-acquire it in the end of the function
        // to actually insert the new inpoint in the map. This means that this function is racey (a concurrent
        // invocation can insert an endpoint with the same name). I think it's ok the way we use it: when
        // initializing the pipeline, we have an endpoint map with names that are guaranteed to be unique;
        // hence it's safe to call `add_input_endpoint` concurrently. In the future we may need to maintain
        // a separate set of reserved connector names to avoid the race. The alternative solution that keeps
        // the lock across the entire body of the function isn't good, because it will force serial connector
        // initialization.
        if self
            .status
            .inputs
            .read()
            .unwrap()
            .values()
            .any(|ep| ep.endpoint_name == endpoint_name)
        {
            Err(ControllerError::duplicate_input_endpoint(endpoint_name))?;
        }

        let resolved_connector_config = resolve_secret_references_in_connector_config(
            &self.secrets_dir,
            &endpoint_config.connector_config,
        )
        .map_err(|e| ControllerError::pipeline_config_parse_error(&e))?;

        // Create input pipeline, consisting of a transport endpoint and parser.

        let input_handle = self
            .catalog
            .input_collection_handle(&SqlIdentifier::from(&endpoint_config.stream))
            .ok_or_else(|| {
                ControllerError::unknown_input_stream(endpoint_name, &endpoint_config.stream)
            })?;

        let endpoint_id = self.next_input_id.fetch_add(1, Ordering::AcqRel);

        let probe = Box::new(InputProbe::new(
            endpoint_id,
            endpoint_name,
            &endpoint_config.connector_config,
            self.clone(),
        ));
        let fault_tolerance = match endpoint {
            Some(endpoint) => {
                // Create parser.
                let format_config = match (
                    &resolved_connector_config.transport,
                    &resolved_connector_config.format,
                ) {
                    (TransportConfig::Datagen(_), None) => FormatConfig {
                        name: Cow::from("json"),
                        config: serde_yaml::to_value(JsonParserConfig {
                            update_format: JsonUpdateFormat::Raw,
                            json_flavor: JsonFlavor::Datagen,
                            array: true,
                            lines: JsonLines::Multiple,
                        })
                        .unwrap(),
                    },
                    (TransportConfig::Datagen(_), Some(_)) =>
                        return Err(ControllerError::input_format_not_supported(
                            endpoint_name,
                            "datagen endpoints do not support custom formats: remove the 'format' section from connector specification",
                        )),
                    (_, Some(format)) => format.clone(),
                    (_, None) => return Err(ControllerError::input_format_not_specified(endpoint_name)),
                };

                let format = get_input_format(&format_config.name).ok_or_else(|| {
                    ControllerError::unknown_input_format(endpoint_name, &format_config.name)
                })?;

                let parser =
                    format.new_parser(endpoint_name, input_handle, &format_config.config)?;

                let fault_tolerance = endpoint.fault_tolerance();

                // Register the endpoint, so that if the the `open` call below signals `eoi` to the controller,
                // the eoi status is recorded and not dropped on the floor.
                self.status.inputs.write().unwrap().insert(
                    endpoint_id,
                    InputEndpointStatus::new(endpoint_name, endpoint_config, fault_tolerance),
                );

                match endpoint
                    .open(probe, parser, input_handle.schema.clone(), resume_info)
                    .map_err(|e| ControllerError::input_transport_error(endpoint_name, true, e))
                {
                    Ok(reader) => {
                        self.status
                            .inputs
                            .write()
                            .unwrap()
                            .get_mut(&endpoint_id)
                            .unwrap()
                            .reader = Some(reader);
                    }
                    Err(e) => {
                        self.status.inputs.write().unwrap().remove(&endpoint_id);
                        return Err(e);
                    }
                }

                fault_tolerance
            }
            None => {
                let endpoint = create_integrated_input_endpoint(
                    endpoint_name,
                    &resolved_connector_config,
                    probe,
                )?;

                let fault_tolerance = endpoint.fault_tolerance();

                self.status.inputs.write().unwrap().insert(
                    endpoint_id,
                    InputEndpointStatus::new(endpoint_name, endpoint_config, fault_tolerance),
                );

                match endpoint
                    .open(input_handle, resume_info)
                    .map_err(|e| ControllerError::input_transport_error(endpoint_name, true, e))
                {
                    Ok(reader) => {
                        self.status
                            .inputs
                            .write()
                            .unwrap()
                            .get_mut(&endpoint_id)
                            .unwrap()
                            .reader = Some(reader);
                    }
                    Err(e) => {
                        self.status.inputs.write().unwrap().remove(&endpoint_id);
                        return Err(e);
                    }
                }
                fault_tolerance
            }
        };

        if fault_tolerance < self.fault_tolerance {
            return Err(ControllerError::input_transport_error(
                endpoint_name,
                true,
                anyhow!("pipeline requires {} fault tolerance but endpoint only supplies {} fault tolerance",
                        FtModel::option_as_str(self.fault_tolerance),
                        FtModel::option_as_str(fault_tolerance)
                )));
        }

        self.unpark_backpressure();
        Ok(endpoint_id)
    }

    fn register_api_connection(&self) -> Result<(), u64> {
        let num_connections = self.num_api_connections.load(Ordering::Acquire);

        if num_connections >= MAX_API_CONNECTIONS {
            Err(num_connections)
        } else {
            self.num_api_connections.fetch_add(1, Ordering::AcqRel);
            Ok(())
        }
    }

    fn unregister_api_connection(&self) {
        let old = self.num_api_connections.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(old > 0);
    }

    fn num_api_connections(&self) -> u64 {
        self.num_api_connections.load(Ordering::Acquire)
    }

    pub fn request_step(&self) {
        self.status.request_step(&self.circuit_thread_unparker);
    }

    /// Unpark the circuit thread.
    fn unpark_circuit(&self) {
        self.circuit_thread_unparker.unpark();
    }

    /// Unpark the backpressure thread.
    fn unpark_backpressure(&self) {
        self.backpressure_thread_unparker.unpark();
    }

    fn connect_output(
        self: &Arc<Self>,
        endpoint_name: &str,
        endpoint_config: &OutputEndpointConfig,
    ) -> Result<EndpointId, ControllerError> {
        let endpoint = output_transport_config_to_endpoint(
            &endpoint_config.connector_config.transport,
            endpoint_name,
            self.fault_tolerance == Some(FtModel::ExactlyOnce),
            &self.secrets_dir,
        )
        .map_err(|e| ControllerError::output_transport_error(endpoint_name, true, e))?;

        // If `endpoint` is `None`, it means that the endpoint config specifies an integrated
        // output connector.  Such endpoints are instantiated inside `add_output_endpoint`.
        self.add_output_endpoint(endpoint_name, endpoint_config, endpoint)
    }

    fn disconnect_output(&self, endpoint_id: &EndpointId) {
        let mut outputs = self.outputs.write().unwrap();

        if let Some(ep) = outputs.remove(endpoint_id) {
            ep.disconnect_flag.store(true, Ordering::Release);
            ep.unparker.unpark();
            self.status.remove_output(endpoint_id);
            // The circuit thread may be waiting for output buffer space.
            self.unpark_circuit();
        }
    }

    fn add_output_endpoint(
        self: &Arc<Self>,
        endpoint_name: &str,
        endpoint_config: &OutputEndpointConfig,
        endpoint: Option<Box<dyn OutputEndpoint>>,
    ) -> Result<EndpointId, ControllerError> {
        // NOTE: We release the lock after the check below and then re-acquire it in the end of the function
        // to actually insert the new inpoint in the map. This means that this function is racey (a concurrent
        // invocation can insert an endpoint with the same name). I think it's ok the way we use it: when
        // initializing the pipeline, we have an endpoint map with names that are guaranteed to be unique;
        // hence it's safe to call `add_output_endpoint` concurrently. In the future we may need to maintain
        // a separate set of reserved endpoint names to avoid the race. The alternative solution that keeps
        // the lock across the entire body of the function isn't good, because it will force serial connector
        // initialization.
        if self
            .outputs
            .read()
            .unwrap()
            .lookup_by_name(endpoint_name)
            .is_some()
        {
            Err(ControllerError::duplicate_output_endpoint(endpoint_name))?;
        }

        let resolved_connector_config = resolve_secret_references_in_connector_config(
            &self.secrets_dir,
            &endpoint_config.connector_config,
        )
        .map_err(|e| ControllerError::pipeline_config_parse_error(&e))?;

        // Create output pipeline, consisting of an encoder, output probe and
        // transport endpoint; run the pipeline in a separate thread.
        //
        // ┌───────┐   ┌───────────┐   ┌────────┐
        // │encoder├──►│OutputProbe├──►│endpoint├──►
        // └───────┘   └───────────┘   └────────┘

        // Lookup output handle in catalog.
        let (handles, stream_name) = if let Some(index) = &endpoint_config.connector_config.index {
            if self
                .catalog
                .output_handles(&SqlIdentifier::from(&endpoint_config.stream))
                .is_none()
            {
                return Err(ControllerError::unknown_output_stream(
                    endpoint_name,
                    &endpoint_config.stream,
                ));
            };

            let handle = self
                .catalog
                .output_handles(&SqlIdentifier::from(index))
                .ok_or_else(|| ControllerError::unknown_index(endpoint_name, index))?;

            if handle.index_of.is_none() {
                return Err(ControllerError::not_an_index(endpoint_name, index));
            }

            if handle.index_of != Some(SqlIdentifier::from(&endpoint_config.stream)) {
                return Err(ControllerError::unknown_output_stream(
                    endpoint_name,
                    &endpoint_config.stream,
                ));
            }

            (handle, index.clone())
        } else {
            (
                self.catalog
                    .output_handles(&SqlIdentifier::from(&endpoint_config.stream))
                    .ok_or_else(|| {
                        ControllerError::unknown_output_stream(
                            endpoint_name,
                            &endpoint_config.stream,
                        )
                    })?,
                endpoint_config.stream.to_string(),
            )
        };

        let endpoint_id = self.next_output_id.fetch_add(1, Ordering::AcqRel);
        let endpoint_name_str = endpoint_name.to_string();

        let self_weak = Arc::downgrade(self);

        endpoint_config
            .connector_config
            .output_buffer_config
            .validate()
            .map_err(|e| ControllerError::invalid_output_buffer_configuration(endpoint_name, &e))?;

        let encoder = if let Some(mut endpoint) = endpoint {
            endpoint
                .connect(Box::new(move |fatal: bool, e: AnyError| {
                    if let Some(controller) = self_weak.upgrade() {
                        controller.output_transport_error(endpoint_id, &endpoint_name_str, fatal, e)
                    }
                }))
                .map_err(|e| ControllerError::output_transport_error(endpoint_name, true, e))?;

            // Create probe.
            let probe = Box::new(OutputProbe::new(
                endpoint_id,
                endpoint_name,
                endpoint,
                self.clone(),
            ));

            // Create encoder.
            let format_config = resolved_connector_config
                .format
                .as_ref()
                .ok_or_else(|| ControllerError::output_format_not_specified(endpoint_name))?
                .clone();

            let format = get_output_format(&format_config.name).ok_or_else(|| {
                ControllerError::unknown_output_format(endpoint_name, &format_config.name)
            })?;
            format.new_encoder(
                endpoint_name,
                &resolved_connector_config,
                &handles.key_schema,
                &handles.value_schema,
                probe,
            )?
        } else {
            // `endpoint` is `None` - instantiate an integrated endpoint.
            let endpoint = create_integrated_output_endpoint(
                endpoint_id,
                endpoint_name,
                &resolved_connector_config,
                &handles.key_schema,
                &handles.value_schema,
                self_weak,
            )?;

            endpoint.into_encoder()
        };

        let parker = Parker::new();
        let endpoint_descr =
            OutputEndpointDescr::new(endpoint_name, &stream_name, parker.unparker().clone());
        let queue = endpoint_descr.queue.clone();
        let disconnect_flag = endpoint_descr.disconnect_flag.clone();
        let controller = self.clone();

        self.outputs
            .write()
            .unwrap()
            .insert(endpoint_id, handles.clone(), endpoint_descr);

        let endpoint_name_string = endpoint_name.to_string();
        let output_buffer_config = endpoint_config
            .connector_config
            .output_buffer_config
            .clone();

        // Initialize endpoint stats.
        self.status
            .add_output(&endpoint_id, endpoint_name, endpoint_config);

        // Thread to run the output pipeline.
        thread::Builder::new()
            .name(format!("{endpoint_name_string}-output"))
            .spawn(move || {
                Self::output_thread_func(
                    endpoint_id,
                    endpoint_name_string,
                    output_buffer_config,
                    encoder,
                    parker,
                    queue,
                    disconnect_flag,
                    controller,
                )
            })
            .expect("failed to spawn output thread");

        Ok(endpoint_id)
    }

    fn merge_batches(mut data: Vec<Arc<dyn SerBatch>>) -> Arc<dyn SerBatch> {
        let last = data.pop().unwrap();

        last.merge(data)
    }

    fn push_batch_to_encoder(
        batch: &dyn SerBatchReader,
        endpoint_id: EndpointId,
        endpoint_name: &str,
        encoder: &mut dyn Encoder,
        step: Step,
        controller: &ControllerInner,
    ) {
        encoder.consumer().batch_start(step);
        encoder
            .encode(batch)
            .unwrap_or_else(|e| controller.encode_error(endpoint_id, endpoint_name, e));
        encoder.consumer().batch_end();
    }

    #[allow(clippy::too_many_arguments)]
    fn output_thread_func(
        endpoint_id: EndpointId,
        endpoint_name: String,
        output_buffer_config: OutputBufferConfig,
        mut encoder: Box<dyn Encoder>,
        parker: Parker,
        queue: Arc<BatchQueue>,
        disconnect_flag: Arc<AtomicBool>,
        controller: Arc<ControllerInner>,
    ) {
        let mut output_buffer = OutputBuffer::new(&endpoint_name);

        loop {
            if controller.state() == PipelineState::Terminated {
                return;
            }

            if disconnect_flag.load(Ordering::Acquire) {
                return;
            }

            if output_buffer.flush_needed(&output_buffer_config) {
                // One of the triggering conditions for flushing the output buffer is satisfied:
                // go ahead and flush the buffer; we will check for more messages at the next iteration
                // of the loop.
                Self::push_batch_to_encoder(
                    output_buffer.take_buffer().unwrap().as_batch_reader(),
                    endpoint_id,
                    &endpoint_name,
                    encoder.as_mut(),
                    output_buffer.buffered_step,
                    &controller,
                );

                controller
                    .status
                    .output_buffered_batches(endpoint_id, output_buffer.buffered_processed_records);
            } else if let Some((step, data, processed_records)) = queue.pop() {
                // Dequeue the next output batch. If output buffering is enabled, push it to the
                // buffer; we will check if the buffer needs to be flushed at the next iteration of
                // the loop.  If buffering is disabled, push the buffer directly to the encoder.

                let num_records = data.iter().map(|b| b.len()).sum();
                let consolidated = Self::merge_batches(data);

                // trace!("Pushing {num_records} records to output endpoint {endpoint_name}");

                // Buffer the new output if buffering is enabled.
                if output_buffer_config.enable_output_buffer {
                    output_buffer.insert(consolidated, step, processed_records);
                    controller.status.buffer_batch(
                        endpoint_id,
                        num_records,
                        &controller.circuit_thread_unparker,
                    );
                } else {
                    Self::push_batch_to_encoder(
                        consolidated.as_batch_reader(),
                        endpoint_id,
                        &endpoint_name,
                        encoder.as_mut(),
                        step,
                        &controller,
                    );

                    // `num_records` output records have been transmitted --
                    // update output stats, wake up the circuit thread if the
                    // number of queued records drops below high watermark.
                    controller.status.output_batch(
                        endpoint_id,
                        processed_records,
                        num_records,
                        &controller.circuit_thread_unparker,
                    );
                }
            } else {
                trace!("Queue is empty -- wait for the circuit thread to wake us up when more data is available");
                if let Some(buffer_since) = output_buffer.buffer_since() {
                    // Buffering is enabled: wake us up when the buffer timeout has expired.
                    let timeout = output_buffer_config.max_output_buffer_time_millis as i128
                        - buffer_since.elapsed().as_millis() as i128;
                    if timeout > 0 {
                        parker.park_timeout(Duration::from_millis(timeout as u64));
                    }
                } else {
                    parker.park();
                }
            }
        }
    }

    fn state(&self) -> PipelineState {
        self.status.state()
    }

    fn start(&self) {
        self.status.set_state(PipelineState::Running);

        // Usually, it is sufficient to unpark the backpressure thread,
        // which will unpause connectors, which will in turn produce new
        // inputs, which will wake up the circuit thread. We unpark
        // the circuit thread manually to address the corner case where
        // the pipeline doesn't yet have any inputs, yet the circuit needs
        // to make the first step to initialize view snapshots used for
        // ad hoc queries (see `trigger()`).
        self.unpark_circuit();
        self.unpark_backpressure();
    }

    fn pause(&self) {
        self.status.set_state(PipelineState::Paused);
        self.unpark_backpressure();
    }

    fn stop(&self) {
        // We need to disconnect endpoints to make sure the controller gets deallocated;
        // otherwise circular referenced between endpoints and the controller will leave
        // a cycle of garbage.

        // Mark pipeline as terminated before removing endpoints, so endpoint removal doesn't
        // end up getting recorded in the journal.
        self.status.set_state(PipelineState::Terminated);

        // Prevent nested panic when stopping the pipeline in response to a panic.
        let Ok(mut inputs) = self.status.inputs.write() else {
            error!("Error shutting down the pipeline: failed to acquire a poisoned lock. This indicates that the pipeline is an inconsistent state.");
            return;
        };

        for ep in inputs.values() {
            if let Some(reader) = ep.reader.as_ref() {
                reader.disconnect()
            }
        }
        inputs.clear();

        self.unpark_circuit();
        self.unpark_backpressure();
    }

    fn set_input_endpoint_paused(
        &self,
        endpoint_name: &str,
        paused: bool,
    ) -> Result<(), ControllerError> {
        let was_paused = self
            .status
            .set_input_endpoint_paused(&self.input_endpoint_id_by_name(endpoint_name)?, paused)
            .ok_or_else(|| ControllerError::unknown_input_endpoint(endpoint_name))?;

        // If this was a real change, then we need to write this to the journal,
        // if we have one.
        if paused != was_paused && self.fault_tolerance == Some(FtModel::ExactlyOnce) {
            self.request_step();
        }

        self.unpark_backpressure();

        Ok(())
    }

    fn pause_input_endpoint(&self, endpoint_name: &str) -> Result<(), ControllerError> {
        self.set_input_endpoint_paused(endpoint_name, true)
    }

    fn start_input_endpoint(&self, endpoint_name: &str) -> Result<(), ControllerError> {
        self.set_input_endpoint_paused(endpoint_name, false)
    }

    fn is_input_endpoint_paused(&self, endpoint_name: &str) -> Result<bool, ControllerError> {
        self.status
            .is_input_endpoint_paused(&self.input_endpoint_id_by_name(endpoint_name)?)
            .ok_or_else(|| ControllerError::unknown_input_endpoint(endpoint_name))
    }

    fn input_endpoint_status(&self, endpoint_name: &str) -> Result<JsonValue, ControllerError> {
        let endpoint_id = self.input_endpoint_id_by_name(endpoint_name)?;
        Ok(serde_json::to_value(&self.status.input_status()[&endpoint_id]).unwrap())
    }

    fn output_endpoint_status(&self, endpoint_name: &str) -> Result<JsonValue, ControllerError> {
        let endpoint_id = self.output_endpoint_id_by_name(endpoint_name)?;
        Ok(serde_json::to_value(&self.status.output_status()[&endpoint_id]).unwrap())
    }

    fn send_command(&self, command: Command) {
        match self.command_sender.send(command) {
            Ok(()) => self.unpark_circuit(),
            Err(SendError(command)) => command.flush(),
        }
    }

    fn error(&self, error: Arc<ControllerError>) {
        (self.error_cb)(error);
    }

    /// Process an input transport error.
    ///
    /// Update endpoint stats and notify the error callback.
    pub fn input_transport_error(
        &self,
        endpoint_id: EndpointId,
        endpoint_name: &str,
        fatal: bool,
        error: AnyError,
    ) {
        self.status
            .input_transport_error(endpoint_id, fatal, &error);
        self.error(Arc::new(ControllerError::input_transport_error(
            endpoint_name,
            fatal,
            error,
        )));
    }

    pub fn parse_error(&self, endpoint_id: EndpointId, endpoint_name: &str, error: ParseError) {
        self.status.parse_error(endpoint_id);
        self.error(Arc::new(ControllerError::parse_error(endpoint_name, error)));
    }

    pub fn encode_error(&self, endpoint_id: EndpointId, endpoint_name: &str, error: AnyError) {
        self.status.encode_error(endpoint_id);
        self.error(Arc::new(ControllerError::encode_error(
            endpoint_name,
            error,
        )));
    }

    /// Process an output transport error.
    ///
    /// Update endpoint stats and notify the error callback.
    pub fn output_transport_error(
        &self,
        endpoint_id: EndpointId,
        endpoint_name: &str,
        fatal: bool,
        error: AnyError,
    ) {
        self.status
            .output_transport_error(endpoint_id, fatal, &error);
        self.error(Arc::new(ControllerError::output_transport_error(
            endpoint_name,
            fatal,
            error,
        )));
    }

    /// Update counters after receiving a new input batch.
    ///
    /// See [ControllerStatus::input_batch].
    pub fn input_batch(&self, endpoint_id: Option<(EndpointId, usize)>, num_records: usize) {
        // We update the individual endpoint metrics, then the global metrics.
        // The order is important because global metrics updates can unpark the
        // circuit thread, and the circuit thread reads the endpoint metrics.
        // Updating in the wrong order can cause the circuit thread to park
        // itself indefinitely.
        if let Some((endpoint_id, num_bytes)) = endpoint_id {
            self.status.input_batch_from_endpoint(
                endpoint_id,
                num_bytes,
                num_records,
                &self.backpressure_thread_unparker,
            )
        }

        if num_records > 0 {
            self.status
                .input_batch_global(num_records, &self.circuit_thread_unparker);
        }
    }

    /// Update counters after receiving an end-of-input event on an input
    /// endpoint.
    ///
    /// See [`ControllerStatus::eoi`].
    pub fn eoi(&self, endpoint_id: EndpointId) {
        self.status.eoi(
            endpoint_id,
            &self.circuit_thread_unparker,
            &self.backpressure_thread_unparker,
        )
    }

    fn output_buffers_full(&self) -> bool {
        self.status.output_buffers_full()
    }

    fn warn_restoring() -> ControllerError {
        static RATE_LIMIT: LazyLock<DefaultDirectRateLimiter> =
            LazyLock::new(|| RateLimiter::direct(Quota::per_minute(nonzero!(10u32))));
        if RATE_LIMIT.check().is_ok() {
            warn!("Failing request because restore from checkpoint is in progress");
        }
        ControllerError::RestoreInProgress
    }

    fn warn_bootstrapping() -> ControllerError {
        static RATE_LIMIT: LazyLock<DefaultDirectRateLimiter> =
            LazyLock::new(|| RateLimiter::direct(Quota::per_minute(nonzero!(10u32))));
        if RATE_LIMIT.check().is_ok() {
            warn!("Failing request while bootstrapping is in progress");
        }
        ControllerError::BootstrapInProgress
    }

    fn fail_if_restoring(&self) -> Result<(), ControllerError> {
        if self.restoring.load(Ordering::Acquire) {
            Err(Self::warn_restoring())
        } else {
            Ok(())
        }
    }

    fn fail_if_bootstrapping(&self) -> Result<(), ControllerError> {
        if self.status.bootstrap_in_progress() {
            Err(Self::warn_bootstrapping())
        } else {
            Ok(())
        }
    }

    /// Some operations are not allowed while the pipeline is either bootstrapping or restoring
    /// from a checkpoint.
    fn fail_if_bootstrapping_or_restoring(&self) -> Result<(), ControllerError> {
        self.fail_if_bootstrapping()?;
        self.fail_if_restoring()?;
        Ok(())
    }

    /// Returns whether this pipeline supports suspend-and-resume.
    pub fn can_suspend(&self) -> Result<(), SuspendError> {
        // First, check for reasons we can't suspend.
        let mut permanent = Vec::new();
        #[cfg(not(feature = "feldera-enterprise"))]
        #[cfg(not(test))]
        permanent.push(PermanentSuspendError::EnterpriseFeature);
        if self.status.pipeline_config.global.storage.is_none() {
            permanent.push(PermanentSuspendError::StorageRequired);
        }
        for endpoint_stats in self.status.input_status().values() {
            if endpoint_stats.fault_tolerance.is_none() {
                permanent.push(PermanentSuspendError::UnsupportedInputEndpoint(
                    endpoint_stats.endpoint_name.clone(),
                ));
            }
        }
        if !permanent.is_empty() {
            return Err(SuspendError::Permanent(permanent));
        }

        // Second, check for reasons for suspend to be delayed.
        let mut temporary = Vec::new();
        if self.restoring.load(Ordering::Acquire) {
            temporary.push(TemporarySuspendError::Replaying);
        }
        if self.status.bootstrap_in_progress() {
            temporary.push(TemporarySuspendError::Bootstrapping);
        }
        for endpoint_stats in self.status.input_status().values() {
            if endpoint_stats.is_barrier() {
                temporary.push(TemporarySuspendError::InputEndpointBarrier(
                    endpoint_stats.endpoint_name.clone(),
                ));
            }
        }
        if !temporary.is_empty() {
            Err(SuspendError::Temporary(temporary))
        } else {
            Ok(())
        }
    }
}

/// An [InputConsumer] for an input adapter to use.
#[derive(Clone)]
struct InputProbe {
    endpoint_id: EndpointId,
    endpoint_name: String,
    controller: Arc<ControllerInner>,
    max_batch_size: usize,
}

impl InputProbe {
    fn new(
        endpoint_id: EndpointId,
        endpoint_name: &str,
        connector_config: &ConnectorConfig,
        controller: Arc<ControllerInner>,
    ) -> Self {
        Self {
            endpoint_id,
            endpoint_name: endpoint_name.to_owned(),
            controller,
            max_batch_size: connector_config.max_batch_size as usize,
        }
    }
}

impl Drop for InputProbe {
    fn drop(&mut self) {
        // Wake up [CircuitThread::flush_input_to_circuit] so that it recognizes
        // that the input adapter has failed.
        self.controller.circuit_thread_unparker.unpark();
    }
}

impl InputConsumer for InputProbe {
    fn max_batch_size(&self) -> usize {
        self.max_batch_size
    }

    fn pipeline_fault_tolerance(&self) -> Option<FtModel> {
        Some(
            self.controller
                .fault_tolerance
                .unwrap_or(FtModel::AtLeastOnce),
        )
    }

    fn parse_errors(&self, errors: Vec<ParseError>) {
        for error in errors {
            self.controller
                .parse_error(self.endpoint_id, &self.endpoint_name, error);
        }
    }

    fn buffered(&self, num_records: usize, num_bytes: usize) {
        self.controller
            .input_batch(Some((self.endpoint_id, num_bytes)), num_records);
    }

    fn replayed(&self, num_records: usize, hash: u64) {
        self.controller.status.extended(
            self.endpoint_id,
            StepResults {
                num_records: num_records as u64,
                resume: Some(Resume::Replay {
                    // These values for `seek` and `replay` are bogus, but they
                    // will not be written to the journal (because they were
                    // read from the journal).
                    seek: JsonValue::Null,
                    replay: RmpValue::Nil,
                    hash,
                }),
            },
            &self.controller.backpressure_thread_unparker,
        );
        self.controller.unpark_circuit();
    }

    fn extended(&self, num_records: usize, resume: Option<Resume>) {
        #[cfg(debug_assertions)]
        {
            let resume_ft = resume.as_ref().map(Resume::fault_tolerance);
            let pipeline_ft = self.controller.fault_tolerance;
            debug_assert!(resume_ft >= self.controller.fault_tolerance, "endpoint {} produced input at fault tolerance level {resume_ft:?} in pipeline with fault tolerance level {pipeline_ft:?}", &self.endpoint_name);
        }
        self.controller.status.extended(
            self.endpoint_id,
            StepResults {
                num_records: num_records as u64,
                resume,
            },
            &self.controller.backpressure_thread_unparker,
        );
        self.controller.unpark_circuit();
    }

    fn eoi(&self) {
        self.controller.eoi(self.endpoint_id);
    }

    fn request_step(&self) {
        self.controller.request_step();
    }

    fn error(&self, fatal: bool, error: AnyError) {
        self.controller
            .input_transport_error(self.endpoint_id, &self.endpoint_name, fatal, error);
    }
}

/// An output probe inserted between the encoder and the output transport
/// endpoint to track stats.
struct OutputProbe {
    endpoint_id: EndpointId,
    endpoint_name: String,
    endpoint: Box<dyn OutputEndpoint>,
    controller: Arc<ControllerInner>,
}

impl OutputProbe {
    pub fn new(
        endpoint_id: EndpointId,
        endpoint_name: &str,
        endpoint: Box<dyn OutputEndpoint>,
        controller: Arc<ControllerInner>,
    ) -> Self {
        Self {
            endpoint_id,
            endpoint_name: endpoint_name.to_owned(),
            endpoint,
            controller,
        }
    }
}

impl OutputConsumer for OutputProbe {
    fn max_buffer_size_bytes(&self) -> usize {
        self.endpoint.max_buffer_size_bytes()
    }

    fn batch_start(&mut self, step: Step) {
        self.endpoint.batch_start(step).unwrap_or_else(|e| {
            self.controller
                .output_transport_error(self.endpoint_id, &self.endpoint_name, false, e);
        })
    }

    fn push_buffer(&mut self, buffer: &[u8], num_records: usize) {
        let num_bytes = buffer.len();

        match self.endpoint.push_buffer(buffer) {
            Ok(()) => {
                self.controller
                    .status
                    .output_buffer(self.endpoint_id, num_bytes, num_records);
            }
            Err(error) => {
                self.controller.output_transport_error(
                    self.endpoint_id,
                    &self.endpoint_name,
                    false,
                    error,
                );
            }
        }
    }

    fn push_key(
        &mut self,
        key: Option<&[u8]>,
        val: Option<&[u8]>,
        headers: &[(&str, Option<&[u8]>)],
        num_records: usize,
    ) {
        let num_bytes =
            key.map(|k| k.len()).unwrap_or_default() + val.map(|v| v.len()).unwrap_or_default();

        match self.endpoint.push_key(key, val, headers) {
            Ok(()) => {
                self.controller
                    .status
                    .output_buffer(self.endpoint_id, num_bytes, num_records);
            }
            Err(error) => {
                self.controller.output_transport_error(
                    self.endpoint_id,
                    &self.endpoint_name,
                    false,
                    error,
                );
            }
        }
    }

    fn batch_end(&mut self) {
        self.endpoint.batch_end().unwrap_or_else(|e| {
            self.controller
                .output_transport_error(self.endpoint_id, &self.endpoint_name, false, e);
        })
    }
}

struct LongOperationWarning {
    start: Instant,
    warn_threshold: Duration,
}

impl LongOperationWarning {
    fn new(warn_threshold: Duration) -> Self {
        Self {
            start: Instant::now(),
            warn_threshold,
        }
    }

    fn check(&mut self, warn: impl FnOnce(Duration)) {
        let elapsed = self.start.elapsed();
        if elapsed >= self.warn_threshold {
            warn(elapsed);
            self.warn_threshold *= 2;
        }
    }
}

#[cfg(test)]
mod test;
