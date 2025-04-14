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
use crate::create_integrated_output_endpoint;
use crate::transport::Step;
use crate::transport::{input_transport_config_to_endpoint, output_transport_config_to_endpoint};
use crate::util::run_on_thread_pool;
use crate::{
    catalog::SerBatch, CircuitCatalog, Encoder, InputConsumer, OutputConsumer, OutputEndpoint,
    ParseError, PipelineState, TransportInputEndpoint,
};
use anyhow::Error as AnyError;
use arrow::datatypes::Schema;
use atomic::Atomic;
use checkpoint::Checkpoint;
use crossbeam::{
    queue::SegQueue,
    sync::{Parker, ShardedLock, Unparker},
};
use datafusion::prelude::*;
use dbsp::circuit::tokio::TOKIO;
use dbsp::circuit::CircuitStorageConfig;
use dbsp::storage::backend::{StorageBackend, StoragePath};
use dbsp::{
    circuit::{CircuitConfig, Layout},
    profile::GraphProfile,
    DBSPHandle,
};
use feldera_adapterlib::utils::datafusion::execute_query_text;
use feldera_types::format::json::JsonLines;
use governor::DefaultDirectRateLimiter;
use governor::Quota;
use governor::RateLimiter;
use journal::{InputChecksums, InputLog, StepInputChecksums, StepMetadata};
use metrics_exporter_prometheus::PrometheusHandle;
use metrics_util::debugging::Snapshotter;
use nonzero_ext::nonzero;
use rmpv::Value as RmpValue;
use serde_json::Value as JsonValue;
use stats::{CanSuspend, StepResults};
use std::borrow::Cow;
use std::collections::HashMap;
use std::collections::HashSet;
use std::io::ErrorKind;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::AtomicI64;
use std::sync::mpsc::{channel, sync_channel, Receiver, Sender};
use std::sync::LazyLock;
use std::thread;
use std::{
    collections::{BTreeMap, BTreeSet},
    mem,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    thread::{spawn, JoinHandle},
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
use feldera_types::config::{FtConfig, OutputBufferConfig};
use feldera_types::format::json::{JsonFlavor, JsonParserConfig, JsonUpdateFormat};
use feldera_types::program_schema::{canonical_identifier, SqlIdentifier};
pub use stats::{ControllerMetric, ControllerStatus, InputEndpointStatus, OutputEndpointStatus};

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
pub struct Controller {
    inner: Arc<ControllerInner>,

    /// The circuit thread handle (see module-level docs).
    circuit_thread_handle: JoinHandle<Result<(), ControllerError>>,
}

/// Type of the callback argument to [`Controller::start_graph_profile`].
pub type GraphProfileCallbackFn = Box<dyn FnOnce(Result<GraphProfile, ControllerError>) + Send>;

/// Type of the callback argument to [`Controller::start_checkpoint`].
pub type CheckpointCallbackFn = Box<dyn FnOnce(Result<Checkpoint, ControllerError>) + Send>;

/// Type of the callback argument to [`Controller::start_suspend`].
pub type SuspendCallbackFn = Box<dyn FnOnce(Result<(), ControllerError>) + Send>;

/// A command that [Controller] can send to [Controller::circuit_thread].
///
/// There is no type for a command reply.  Instead, the command implementation
/// uses a callback embedded in the command to reply.
enum Command {
    GraphProfile(GraphProfileCallbackFn),
    Checkpoint(CheckpointCallbackFn),
    Suspend(SuspendCallbackFn),
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
        error_cb: Box<dyn Fn(ControllerError) + Send + Sync>,
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
            let handle =
                spawn(
                    move || match CircuitThread::new(circuit_factory, config, error_cb) {
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
                    },
                );
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
    ) -> Result<EndpointId, ControllerError> {
        debug!("Connecting input endpoint '{endpoint_name}'; config: {config:?}");
        self.inner.fail_if_restoring()?;
        self.inner.connect_input(endpoint_name, config)
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
    ) -> Result<EndpointId, ControllerError> {
        self.inner.fail_if_restoring()?;
        self.inner
            .add_input_endpoint(endpoint_name, endpoint_config, Some(endpoint))
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

    pub fn input_endpoint_status(&self, endpoint_name: &str) -> Result<JsonValue, ControllerError> {
        self.inner.input_endpoint_status(endpoint_name)
    }

    pub fn output_endpoint_status(
        &self,
        endpoint_name: &str,
    ) -> Result<JsonValue, ControllerError> {
        self.inner.output_endpoint_status(endpoint_name)
    }

    // Start or resume specified input endpoint.
    //
    // Sets `paused_by_user` flag of the endpoint to `false`.
    pub fn start_input_endpoint(&self, endpoint_name: &str) -> Result<(), ControllerError> {
        self.inner.start_input_endpoint(endpoint_name)
    }

    /// Returns controller status.
    pub fn status(&self) -> &ControllerStatus {
        // Update pipeline stats computed on-demand.
        self.inner.status.update(self.can_suspend());
        &self.inner.status
    }

    pub fn metrics_snapshotter(&self) -> Arc<Snapshotter> {
        metrics_recorder::snapshotter()
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
        self.inner.graph_profile(cb)
    }

    /// Triggers a checkpoint operation. `cb` will be called when it completes.
    ///
    /// The callback-based nature of this function makes it useful in
    /// asynchronous contexts.
    pub fn start_checkpoint(&self, cb: CheckpointCallbackFn) {
        match self.inner.fail_if_restoring() {
            Err(error) => cb(Err(error)),
            Ok(()) => self.inner.checkpoint(cb),
        }
    }

    /// Checkpoints the pipeline.
    ///
    /// This is a blocking wrapper around [Self::start_checkpoint].
    pub fn checkpoint(&self) -> Result<Checkpoint, ControllerError> {
        let (sender, receiver) = oneshot::channel();
        self.start_checkpoint(Box::new(move |result| sender.send(result).unwrap()));
        receiver.blocking_recv().unwrap()
    }

    /// Triggers a suspend operation. `cb` will be called when it completes.
    ///
    /// The callback-based nature of this function makes it useful in
    /// asynchronous contexts.
    pub fn start_suspend(&self, cb: SuspendCallbackFn) {
        match self.inner.fail_if_restoring() {
            Err(error) => cb(Err(error)),
            Ok(()) => self.inner.suspend(cb),
        }
    }

    /// Suspends the pipeline.
    ///
    /// This is a blocking wrapper around [Self::start_suspend].
    pub fn suspend(&self) -> Result<(), ControllerError> {
        let (sender, receiver) = oneshot::channel();
        self.start_suspend(Box::new(move |result| sender.send(result).unwrap()));
        receiver.blocking_recv().unwrap()
    }

    /// Returns whether this pipeline supports suspend-and-resume.
    pub fn can_suspend(&self) -> CanSuspend {
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

    pub(crate) fn metrics(&self) -> PrometheusHandle {
        metrics_recorder::prometheus_handle()
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

    /// Storage backend for writing checkpoints.
    storage: Option<Arc<dyn StorageBackend>>,

    /// The step currently running or replaying.
    step: Step,

    /// Metadata for `step - 1`; that is, the metadata that would be part of a
    /// [Checkpoint] for `step`, to allow the input endpoints to seek to the
    /// starting point for reading data for `step`.
    ///
    /// This is only `None` if `step` is 0.
    input_metadata: Option<CheckpointOffsets>,
}

impl CircuitThread {
    /// Circuit thread function: holds the handle to the circuit, calls `step`
    /// on it whenever input data is available, pushes output batches
    /// produced by the circuit to output pipelines.
    fn new<F>(
        circuit_factory: F,
        config: PipelineConfig,
        error_cb: Box<dyn Fn(ControllerError) + Send + Sync>,
    ) -> Result<Self, ControllerError>
    where
        F: FnOnce(CircuitConfig) -> Result<(DBSPHandle, Box<dyn CircuitCatalog>), ControllerError>,
    {
        let ft = config.global.fault_tolerance.is_enabled();
        let ControllerInit {
            pipeline_config,
            circuit_config,
            processed_records,
            step,
            input_metadata,
        } = ControllerInit::new(config)?;
        let storage = circuit_config
            .storage
            .as_ref()
            .map(|storage| storage.backend.clone());
        let (circuit, catalog) = circuit_factory(circuit_config)?;
        let (parker, backpressure_thread, command_receiver, controller) =
            ControllerInner::new(pipeline_config, catalog, error_cb, processed_records)?;

        // Seek each input endpoint to its initial offset.
        //
        // If we're not restoring from a checkpoint, `input_metadata` will be empty so
        // this will do nothing.
        if let Some(input_metadata) = &input_metadata {
            for (endpoint_name, metadata) in &input_metadata.0 {
                let endpoint_id = controller.input_endpoint_id_by_name(endpoint_name)?;
                controller.status.input_status()[&endpoint_id]
                    .reader
                    .seek(metadata.clone());
            }
        }

        let ft = if ft {
            let backend = storage.clone().unwrap();
            let ft = if input_metadata.is_some() {
                FtState::open(backend, step, controller.clone())
            } else {
                FtState::create(backend, controller.clone())
            };
            Some(ft?)
        } else {
            if let Some(backend) = &storage {
                // We're not fault-tolerant, so it's not a good idea to resume
                // from the same checkpoint twice.  Delete it.
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
            step,
            input_metadata,
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
            if !self.run_commands() {
                break;
            }
            let running = match self.controller.state() {
                PipelineState::Running => true,
                PipelineState::Paused => false,
                PipelineState::Terminated => break,
            };

            // Backpressure in the output pipeline: wait for room in output buffers to
            // become available.
            if self.controller.output_buffers_full() {
                debug!("circuit thread: park waiting for output buffer space");
                self.parker.park();
                debug!("circuit thread: unparked");
                continue;
            }

            match trigger.trigger(self.last_checkpoint, self.replaying(), running) {
                Action::Step => {
                    let done = !self.step()?;
                    self.controller
                        .status
                        .global_metrics
                        .runtime_elapsed_msecs
                        .store(
                            self.circuit.runtime_elapsed().as_millis() as u64,
                            Ordering::Relaxed,
                        );

                    if done {
                        break;
                    }
                }
                Action::Checkpoint => drop(self.checkpoint()),
                Action::Park(Some(deadline)) => self.parker.park_deadline(deadline),
                Action::Park(None) => self.parker.park(),
            }
        }
        self.flush_commands();
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
        let Ok(FlushedInput {
            total_consumed,
            step_metadata,
        }) = self.flush_input_to_circuit()
        else {
            return Ok(false);
        };
        self.input_metadata = Some(CheckpointOffsets(
            step_metadata
                .iter()
                .map(|(name, log)| (name.clone(), log.metadata.clone()))
                .collect(),
        ));
        if let Some(ft) = self.ft.as_mut() {
            ft.write_step(step_metadata, self.step)?;
        }
        self.step += 1;

        // Wake up the backpressure thread to unpause endpoints blocked due to
        // backpressure.
        self.controller.unpark_backpressure();
        debug!("circuit thread: calling 'circuit.step'");
        self.circuit
            .step()
            .unwrap_or_else(|e| self.controller.error(e.into()));
        debug!("circuit thread: 'circuit.step' returned");

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

    fn checkpoint(&mut self) -> Result<Checkpoint, ControllerError> {
        fn inner(this: &mut CircuitThread) -> Result<Checkpoint, ControllerError> {
            match this.controller.can_suspend() {
                CanSuspend::No => Err(ControllerError::not_supported(
                    "suspend requires storage and fault-tolerant input endpoints",
                )),
                CanSuspend::NotNow => Err(ControllerInner::warn_restoring()),
                CanSuspend::Yes => Ok(()),
            }?;

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
                        (
                            Cow::from(status.endpoint_name.clone()),
                            status.config.clone(),
                        )
                    })
                    .collect(),
                ..this.controller.status.pipeline_config.clone()
            };

            let checkpoint = this
                .circuit
                .commit()
                .map_err(ControllerError::from)
                .and_then(|circuit| {
                    let checkpoint = Checkpoint {
                        circuit: Some(circuit),
                        step: this.step,
                        config,
                        processed_records: this
                            .controller
                            .status
                            .global_metrics
                            .num_total_processed_records(),
                        input_metadata: this.input_metadata.clone().unwrap_or_default(),
                    };
                    checkpoint
                        .write(
                            &**this.storage.as_ref().unwrap(),
                            &StoragePath::from(STATE_FILE),
                        )
                        .map(|()| checkpoint)
                })?;
            if let Some(ft) = &mut this.ft {
                ft.checkpointed()?;
            }
            Ok(checkpoint)
        }

        let result = inner(self);
        if let Err(error) = result.as_ref() {
            warn!("checkpoint failed: {error}");
        }

        // Update the last checkpoint time *after* executing the checkpoint, so
        // that if the checkpoint takes a long time and we have a short
        // checkpoint interval, we get to do other work too.
        //
        // We always update `last_checkpoint`, even if there was an error,
        // because we do not want to spend all our time checkpointing if there's
        // a problem and a short checkpoint interval.
        self.last_checkpoint = Instant::now();

        result
    }

    /// Reads and executes all the commands pending from
    /// `self.command_receiver`.
    fn run_commands(&mut self) -> bool {
        while let Ok(command) = self.command_receiver.try_recv() {
            match command {
                Command::GraphProfile(reply_callback) => reply_callback(
                    self.circuit
                        .graph_profile()
                        .map_err(ControllerError::dbsp_error),
                ),
                Command::Checkpoint(reply_callback) => {
                    reply_callback(self.checkpoint());
                }
                Command::Suspend(reply_callback) => {
                    reply_callback(self.checkpoint().map(|_| ()));
                    return false;
                }
            }
        }
        true
    }

    /// Reads and replies to all of the commands pending from
    /// `self.command_receiver` without executing them.
    fn flush_commands(&mut self) {
        for command in self.command_receiver.try_iter() {
            match command {
                Command::GraphProfile(callback) => callback(Err(ControllerError::ControllerExit)),
                Command::Checkpoint(callback) => callback(Err(ControllerError::ControllerExit)),
                Command::Suspend(callback) => callback(Err(ControllerError::ControllerExit)),
            }
        }
    }

    /// Requests all of the input adapters to flush their input to the circuit,
    /// and waits for them to finish doing it.
    ///
    /// Returns information about the input that was flushed, or `Err(())` if
    /// the pipeline should shut down.
    fn flush_input_to_circuit(&mut self) -> Result<FlushedInput, ()> {
        // Collect the ids of the endpoints that we'll flush to the circuit.
        //
        // The set of endpoint ids could change while we're waiting, because
        // adapters can be hot-added and hot-removed (and this really happens
        // with HTTP and ad-hoc input adapters). We only wait for the ones that
        // exist when we start flushing input. New ones will be processed in
        // future steps.
        let statuses = self.controller.status.input_status();
        let mut waiting = HashSet::with_capacity(statuses.len());
        for (endpoint_id, status) in statuses.iter() {
            if !self.replaying() {
                status.reader.queue();
            } else {
                // We already started the input adapters replaying. The set of
                // input adapters can't change during replay (because we disable
                // the APIs that could do that during replay).
            }
            waiting.insert(*endpoint_id);
        }
        drop(statuses);

        let mut total_consumed = 0;
        let mut step_metadata = HashMap::new();

        let start_time = Instant::now();
        let mut warn_threshold = Duration::from_secs(10);
        loop {
            // Process input and check for failed input adapters.
            let statuses = self.controller.status.input_status();
            let mut failed = Vec::new();
            waiting.retain(|endpoint_id| {
                let Some(status) = statuses.get(endpoint_id) else {
                    // Input adapter was deleted without yielding any input.
                    return false;
                };
                let Some(results) = mem::take(&mut *status.progress.lock().unwrap()) else {
                    // Check for failure.
                    //
                    // We check for results first, because if an input adapter
                    // queues its last input batch and then exits, we want to
                    // take the input.
                    if status.reader.is_closed() {
                        if status.metrics.end_of_input.load(Ordering::Acquire) {
                            warn!("Input endpoint {} exited", status.endpoint_name);
                        } else {
                            warn!("Input endpoint {} failed", status.endpoint_name);
                        }
                        failed.push(*endpoint_id);
                        return false;
                    } else {
                        return true;
                    }
                };

                // Input received.
                total_consumed += results.num_records;
                step_metadata.insert(
                    status.endpoint_name.clone(),
                    InputLog {
                        data: results.data.unwrap_or(RmpValue::Nil),
                        metadata: results.metadata.unwrap_or(JsonValue::Null),
                        checksums: InputChecksums {
                            num_records: results.num_records,
                            hash: results.hash,
                        },
                    },
                );
                false
            });
            drop(statuses);
            for endpoint_id in failed {
                self.controller.disconnect_input(&endpoint_id);
            }

            // Are we done?
            if waiting.is_empty() {
                let step_metadata = if let Some(ft) = &self.ft {
                    if let Some(replay_step) = &ft.replay_step {
                        // Reuse input logs from the original step.  (The ones
                        // just produced are empty, because replay doesn't
                        // produce logs, since they'd be identical to the
                        // original ones.)
                        //
                        // Usually, these logs are not used for anything, since
                        // we're replaying, but in the corner case where this is
                        // the final step to replay and we immediately
                        // checkpoint, the checkpoint must have the log records
                        // so that, on resume, it can seek the input connectors
                        // to the proper location.
                        replay_step.input_logs.clone()
                    } else {
                        step_metadata
                    }
                } else {
                    step_metadata
                };

                return Ok(FlushedInput {
                    total_consumed,
                    step_metadata,
                });
            }

            // Warn if we've waited a long time.
            if start_time.elapsed() >= warn_threshold {
                warn!(
                    "still waiting to complete input step after {} seconds",
                    warn_threshold.as_secs()
                );
                warn_threshold *= 2;
            }

            // Wait for something to change.
            self.parker.park_timeout(Duration::from_secs(1));
            if self.controller.state() == PipelineState::Terminated {
                return Err(());
            }
        }
    }

    /// Reports that `total_consumed` records have been consumed.
    ///
    /// Returns the total number of records processed by the pipeline *before*
    /// this step.
    fn processed_records(&mut self, total_consumed: u64) -> u64 {
        self.controller
            .status
            .global_metrics
            .processed_records(total_consumed)
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
}

struct FlushedInput {
    /// Number of records consumed by the circuit.
    total_consumed: u64,

    /// Metadata to write to the steps log.
    step_metadata: HashMap<String, InputLog>,
}

/// Tracks fault-tolerant state in a controller [CircuitThread].
struct FtState {
    /// The controller.
    controller: Arc<ControllerInner>,

    /// The journal.
    journal: Journal,

    /// The journal record that we're replaying, if we're replaying.
    replay_step: Option<StepMetadata>,

    /// Input endpoint ids and names at the time we wrote the last step,
    /// so that we can log changes for the replay log.
    input_endpoints: HashMap<EndpointId, String>,
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
            input_endpoints: Self::initial_input_endpoints(&controller),
            controller,
            replay_step: None,
            journal,
        })
    }

    fn initial_input_endpoints(controller: &ControllerInner) -> HashMap<EndpointId, String> {
        controller
            .status
            .inputs
            .read()
            .unwrap()
            .iter()
            .map(|(id, status)| (*id, status.endpoint_name.clone()))
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
            controller.connect_input(endpoint_name, config)?;
        }
        for (endpoint_name, log) in &metadata.input_logs {
            let endpoint_id = controller.input_endpoint_id_by_name(endpoint_name)?;
            controller.status.input_status()[&endpoint_id]
                .reader
                .replay(log.metadata.clone(), log.data.clone());
        }
        Ok(())
    }

    /// Writes `step_metadata` to the step writer.
    fn write_step(
        &mut self,
        input_logs: HashMap<String, InputLog>,
        step: Step,
    ) -> Result<(), ControllerError> {
        match &self.replay_step {
            None => {
                let mut remove_inputs = HashSet::new();
                let mut add_inputs = HashMap::new();
                let inputs = self.controller.status.inputs.read().unwrap();
                for (endpoint_id, endpoint_name) in self.input_endpoints.iter() {
                    if !inputs.contains_key(endpoint_id) {
                        remove_inputs.insert(endpoint_name.clone());
                    }
                }
                for (endpoint_id, status) in inputs.iter() {
                    if !self.input_endpoints.contains_key(endpoint_id) {
                        add_inputs.insert(status.endpoint_name.clone(), status.config.clone());
                    }
                }
                if !remove_inputs.is_empty() || !add_inputs.is_empty() {
                    self.input_endpoints = inputs
                        .iter()
                        .map(|(id, status)| (*id, status.endpoint_name.clone()))
                        .collect();
                }
                drop(inputs);

                let step_metadata = StepMetadata {
                    step,
                    remove_inputs,
                    add_inputs,
                    input_logs,
                };
                self.journal.write(&step_metadata)?;
            }
            Some(record) => {
                let logged = StepInputChecksums::from(&record.input_logs);
                let replayed = StepInputChecksums::from(&input_logs);
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
    /// Time when `clock_resolution` expires.
    tick: Option<Instant>,

    /// Time when `max_buffering_delay` expires.
    buffer_timeout: Option<Instant>,

    controller: Arc<ControllerInner>,

    /// Maximum time to wait before stepping after receiving at least one record
    /// but fewer than `min_batch_size_records`.
    max_buffering_delay: Duration,

    /// Minimum number of records to receive before unconditionally triggering a
    /// step.
    min_batch_size_records: u64,

    /// Time between clock ticks.
    clock_resolution: Option<Duration>,

    /// Time between automatic checkpoints.
    checkpoint_interval: Option<Duration>,
}

/// Action for the controller to take.
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
        let clock_resolution = config.clock_resolution_usecs.map(Duration::from_micros);
        let checkpoint_interval = config.fault_tolerance.checkpoint_interval();
        Self {
            controller,
            tick: clock_resolution.map(|delay| Instant::now() + delay),
            buffer_timeout: None,
            max_buffering_delay,
            min_batch_size_records,
            clock_resolution,
            checkpoint_interval,
        }
    }

    /// Determines when to trigger the next step, given whether we're currently
    /// `replaying` and whether the pipeline is currently `running`.  Returns
    /// `None` to trigger a step right away, and otherwise how long to wait.
    fn trigger(&mut self, last_checkpoint: Instant, replaying: bool, running: bool) -> Action {
        let buffered_records = self.controller.status.num_buffered_input_records();

        // `self.tick` but `None` if we're not running.
        let tick = running.then_some(self.tick).flatten();

        // Time of the next checkpoint.
        let checkpoint = self
            .checkpoint_interval
            .map(|interval| last_checkpoint + interval);

        fn step(trigger: &mut StepTrigger, now: Instant) -> Action {
            trigger.tick = trigger.clock_resolution.map(|delay| now + delay);
            trigger.buffer_timeout = None;
            Action::Step
        }

        let now = Instant::now();
        if replaying {
            step(self, now)
        } else if checkpoint.is_some_and(|t| now >= t) {
            Action::Checkpoint
        } else if self.controller.status.unset_step_requested()
            || buffered_records > self.min_batch_size_records
            || tick.is_some_and(|t| now >= t)
            || self.buffer_timeout.is_some_and(|t| now >= t)
        {
            step(self, now)
        } else {
            if buffered_records > 0 && self.buffer_timeout.is_none() {
                self.buffer_timeout = Some(now + self.max_buffering_delay);
            }
            let wakeup = [tick, self.buffer_timeout, checkpoint]
                .into_iter()
                .flatten()
                .min();
            Action::Park(wakeup)
        }
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
    input_metadata: Option<CheckpointOffsets>,
}

pub const STATE_FILE: &str = "state.json";

pub const STEPS_FILE: &str = "steps.bin";

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
        // Initialize the metrics recorder early.  If we don't install it before
        // creating the circuit, DBSP metrics will not be recorded.
        let pipeline_name = config
            .name
            .as_ref()
            .map_or_else(|| "unnamed".to_string(), |n| n.clone());
        metrics_recorder::init(pipeline_name);

        let Some((storage_config, storage_options)) = config.storage() else {
            if !config.global.fault_tolerance.is_enabled() {
                info!("storage not configured, so suspend-and-resume and fault tolerance will not be available");
                return Self::without_resume(config, None);
            } else {
                return Err(ControllerError::Config {
                    config_error: ConfigError::FtRequiresStorage,
                });
            }
        };
        let storage =
            CircuitStorageConfig::for_config(storage_config.clone(), storage_options.clone())
                .map_err(|error| {
                    ControllerError::storage_error("failed to initialize storage", error)
                })?;

        // Try to read a checkpoint.
        let checkpoint = match Checkpoint::read(&*storage.backend, &StoragePath::from(STATE_FILE)) {
            Err(error) if error.kind() == ErrorKind::NotFound => {
                info!("no checkpoint found for resume ({error})",);
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
            },

            // Adapter configuration has to come from the checkpoint.
            inputs: checkpoint_config.inputs,
            outputs: checkpoint_config.outputs,

            // Other settings from the pipeline manager.
            name: config.name,
            storage_config: config.storage_config,
        };

        Ok(Self {
            circuit_config: Self::circuit_config(&config, Some(storage))?,
            pipeline_config: config,
            step,
            input_metadata: Some(input_metadata),
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
        })
    }
}

mod metrics_recorder {
    use std::sync::{Arc, OnceLock};

    use metrics::set_global_recorder;
    use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
    use metrics_util::{
        debugging::{DebuggingRecorder, Snapshotter},
        layers::FanoutBuilder,
    };

    static METRIC_HANDLES: OnceLock<(Arc<Snapshotter>, PrometheusHandle)> = OnceLock::new();

    /// Initializes the global metrics recorder.
    ///
    /// This has to happen early, before the circuit is initialized; otherwise,
    /// DBSP metrics won't be recorded.
    pub fn init(pipeline_name: String) {
        METRIC_HANDLES.get_or_init(|| {
            let debugging_recorder = DebuggingRecorder::new();
            let snapshotter = debugging_recorder.snapshotter();
            let prometheus_recorder = PrometheusBuilder::new()
                .add_global_label("pipeline", pipeline_name)
                .build_recorder();
            let prometheus_handle = prometheus_recorder.handle();
            let builder = FanoutBuilder::default()
                .add_recorder(debugging_recorder)
                .add_recorder(prometheus_recorder);

            set_global_recorder(builder.build()).expect("failed to install metrics exporter");

            (Arc::new(snapshotter), prometheus_handle)
        });
    }

    /// Returns the global metrics snapshotter. [init] must already have been called.
    pub fn snapshotter() -> Arc<Snapshotter> {
        METRIC_HANDLES.get().unwrap().0.clone()
    }

    /// Returns the global Prometheus handle. [init] must already have been called.
    pub fn prometheus_handle() -> PrometheusHandle {
        METRIC_HANDLES.get().unwrap().1.clone()
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
            self.join_handle = Some(spawn(move || {
                Self::backpressure_thread(controller, parker, exit)
            }));
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

            for (epid, ep) in controller.status.input_status().iter() {
                let should_run = globally_running && !ep.is_paused_by_user() && !ep.is_full();
                match should_run {
                    true => {
                        if running_endpoints.insert(*epid) {
                            ep.reader.extend();
                        }
                    }
                    false => {
                        if running_endpoints.remove(epid) {
                            ep.reader.pause();
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
    num_api_connections: AtomicU64,
    command_sender: Sender<Command>,
    catalog: Arc<Box<dyn CircuitCatalog>>,
    // Always lock this after the catalog is locked to avoid deadlocks
    trace_snapshot: ConsistentSnapshots,
    next_input_id: Atomic<EndpointId>,
    outputs: ShardedLock<OutputEndpoints>,
    next_output_id: Atomic<EndpointId>,
    circuit_thread_unparker: Unparker,
    backpressure_thread_unparker: Unparker,
    error_cb: Box<dyn Fn(ControllerError) + Send + Sync>,
    session_ctxt: SessionContext,

    /// Is fault tolerance enabled?
    fault_tolerant: bool,

    /// Is the circuit thread still restoring from a checkpoint?
    restoring: AtomicBool,
}

impl ControllerInner {
    fn new(
        config: PipelineConfig,
        catalog: Box<dyn CircuitCatalog>,
        error_cb: Box<dyn Fn(ControllerError) + Send + Sync>,
        processed_records: u64,
    ) -> Result<(Parker, BackpressureThread, Receiver<Command>, Arc<Self>), ControllerError> {
        let status = Arc::new(ControllerStatus::new(config.clone(), processed_records));
        let circuit_thread_parker = Parker::new();
        let backpressure_thread_parker = Parker::new();
        let (command_sender, command_receiver) = channel();
        let session_ctxt = create_session_context(&config)?;
        let controller = Arc::new(Self {
            status,
            num_api_connections: AtomicU64::new(0),
            command_sender,
            catalog: Arc::new(catalog),
            trace_snapshot: Arc::new(TokioMutex::new(BTreeMap::new())),
            next_input_id: Atomic::new(0),
            outputs: ShardedLock::new(OutputEndpoints::new()),
            next_output_id: Atomic::new(0),
            circuit_thread_unparker: circuit_thread_parker.unparker().clone(),
            backpressure_thread_unparker: backpressure_thread_parker.unparker().clone(),
            error_cb,
            session_ctxt,
            fault_tolerant: config.global.fault_tolerance.is_enabled(),
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
                    Box::new(move || {
                        catch_unwind(AssertUnwindSafe(|| {
                            controller.connect_input(&input_name, &input_config)
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
        let inputs = self.status.input_status();

        for (endpoint_id, descr) in inputs.iter() {
            if descr.endpoint_name == endpoint_name {
                return Ok(*endpoint_id);
            }
        }

        Err(ControllerError::unknown_input_endpoint(endpoint_name))
    }

    fn output_endpoint_id_by_name(
        &self,
        endpoint_name: &str,
    ) -> Result<EndpointId, ControllerError> {
        let outputs = self.status.output_status();

        for (endpoint_id, descr) in outputs.iter() {
            if descr.endpoint_name == endpoint_name {
                return Ok(*endpoint_id);
            }
        }

        Err(ControllerError::unknown_output_endpoint(endpoint_name))
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
    ) -> Result<EndpointId, ControllerError> {
        let endpoint = input_transport_config_to_endpoint(
            endpoint_config.connector_config.transport.clone(),
            endpoint_name,
            self.fault_tolerant,
        )
        .map_err(|e| ControllerError::input_transport_error(endpoint_name, true, e))?;

        // If `endpoint` is `None`, it means that the endpoint config specifies an integrated
        // input connector.  Such endpoints are instantiated inside `add_input_endpoint`.
        self.add_input_endpoint(endpoint_name, endpoint_config.clone(), endpoint)
    }

    pub fn disconnect_input(self: &Arc<Self>, endpoint_id: &EndpointId) {
        debug!("Disconnecting input endpoint {endpoint_id}");

        if let Some(ep) = self.status.remove_input(endpoint_id) {
            ep.reader.disconnect();
            self.unpark_circuit();
            self.unpark_backpressure();
        }
    }

    pub fn add_input_endpoint(
        self: &Arc<Self>,
        endpoint_name: &str,
        endpoint_config: InputEndpointConfig,
        endpoint: Option<Box<dyn TransportInputEndpoint>>,
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
        let (reader, is_fault_tolerant) = match endpoint {
            Some(endpoint) => {
                // Create parser.
                let format_config = if endpoint_config.connector_config.transport.name()
                    != "datagen"
                {
                    endpoint_config
                        .connector_config
                        .format
                        .as_ref()
                        .ok_or_else(|| ControllerError::input_format_not_specified(endpoint_name))?
                        .clone()
                } else {
                    if endpoint_config.connector_config.format.is_some() {
                        return Err(ControllerError::input_format_not_supported(
                            endpoint_name,
                            "datagen endpoints do not support custom formats: remove the 'format' section from connector specification",
                        ));
                    }
                    FormatConfig {
                        name: Cow::from("json"),
                        config: serde_yaml::to_value(JsonParserConfig {
                            update_format: JsonUpdateFormat::Raw,
                            json_flavor: JsonFlavor::Datagen,
                            array: true,
                            lines: JsonLines::Multiple,
                        })
                        .unwrap(),
                    }
                };

                let format = get_input_format(&format_config.name).ok_or_else(|| {
                    ControllerError::unknown_input_format(endpoint_name, &format_config.name)
                })?;

                let parser =
                    format.new_parser(endpoint_name, input_handle, &format_config.config)?;

                let reader = endpoint
                    .open(probe, parser, input_handle.schema.clone())
                    .map_err(|e| ControllerError::input_transport_error(endpoint_name, true, e))?;
                (reader, endpoint.is_fault_tolerant())
            }
            None => {
                let endpoint =
                    create_integrated_input_endpoint(endpoint_name, &endpoint_config, probe)?;

                let is_fault_tolerant = endpoint.is_fault_tolerant();
                let reader = endpoint
                    .open(input_handle)
                    .map_err(|e| ControllerError::input_transport_error(endpoint_name, true, e))?;
                (reader, is_fault_tolerant)
            }
        };

        self.status.inputs.write().unwrap().insert(
            endpoint_id,
            InputEndpointStatus::new(endpoint_name, endpoint_config, reader, is_fault_tolerant),
        );

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
            endpoint_config.connector_config.transport.clone(),
            endpoint_name,
            self.fault_tolerant,
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
            let format_config = endpoint_config
                .connector_config
                .format
                .as_ref()
                .ok_or_else(|| ControllerError::output_format_not_specified(endpoint_name))?
                .clone();

            let format = get_output_format(&format_config.name).ok_or_else(|| {
                ControllerError::unknown_output_format(endpoint_name, &format_config.name)
            })?;
            format.new_encoder(
                endpoint_name,
                &endpoint_config.connector_config,
                &handles.key_schema,
                &handles.value_schema,
                probe,
            )?
        } else {
            // `endpoint` is `None` - instantiate an integrated endpoint.
            let endpoint = create_integrated_output_endpoint(
                endpoint_id,
                endpoint_name,
                endpoint_config,
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

        // Thread to run the output pipeline.
        spawn(move || {
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
        });

        // Initialize endpoint stats.
        self.status
            .add_output(&endpoint_id, endpoint_name, endpoint_config);

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
        self.unpark_backpressure();
    }

    fn pause(&self) {
        self.status.set_state(PipelineState::Paused);
        self.unpark_backpressure();
    }

    fn stop(&self) {
        // Prevent nested panic when stopping the pipeline in response to a panic.
        let Ok(mut inputs) = self.status.inputs.write() else {
            error!("Error shutting down the pipeline: failed to acquire a poisoned lock. This indicates that the pipeline is an inconsistent state.");
            return;
        };

        for ep in inputs.values() {
            ep.reader.disconnect();
        }
        inputs.clear();

        self.status.set_state(PipelineState::Terminated);

        self.unpark_circuit();
        self.unpark_backpressure();
    }

    fn pause_input_endpoint(&self, endpoint_name: &str) -> Result<(), ControllerError> {
        let endpoint_id = self.input_endpoint_id_by_name(endpoint_name)?;
        self.status.pause_input_endpoint(&endpoint_id);
        self.unpark_backpressure();
        Ok(())
    }

    fn input_endpoint_status(&self, endpoint_name: &str) -> Result<JsonValue, ControllerError> {
        let endpoint_id = self.input_endpoint_id_by_name(endpoint_name)?;
        Ok(serde_json::to_value(&self.status.input_status()[&endpoint_id]).unwrap())
    }

    fn output_endpoint_status(&self, endpoint_name: &str) -> Result<JsonValue, ControllerError> {
        let endpoint_id = self.output_endpoint_id_by_name(endpoint_name)?;
        Ok(serde_json::to_value(&self.status.output_status()[&endpoint_id]).unwrap())
    }

    fn start_input_endpoint(&self, endpoint_name: &str) -> Result<(), ControllerError> {
        let endpoint_id = self.input_endpoint_id_by_name(endpoint_name)?;
        self.status.start_input_endpoint(&endpoint_id);
        self.unpark_backpressure();
        Ok(())
    }

    fn graph_profile(&self, cb: GraphProfileCallbackFn) {
        self.command_sender.send(Command::GraphProfile(cb)).unwrap();
        self.unpark_circuit();
    }

    fn checkpoint(&self, cb: CheckpointCallbackFn) {
        self.command_sender.send(Command::Checkpoint(cb)).unwrap();
        self.unpark_circuit();
    }

    fn suspend(&self, cb: SuspendCallbackFn) {
        self.command_sender.send(Command::Suspend(cb)).unwrap();
        self.unpark_circuit();
    }

    fn error(&self, error: ControllerError) {
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
        self.error(ControllerError::input_transport_error(
            endpoint_name,
            fatal,
            error,
        ));
    }

    pub fn parse_error(&self, endpoint_id: EndpointId, endpoint_name: &str, error: ParseError) {
        self.status.parse_error(endpoint_id);
        self.error(ControllerError::parse_error(endpoint_name, error));
    }

    pub fn encode_error(&self, endpoint_id: EndpointId, endpoint_name: &str, error: AnyError) {
        self.status.encode_error(endpoint_id);
        self.error(ControllerError::encode_error(endpoint_name, error));
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
        self.error(ControllerError::output_transport_error(
            endpoint_name,
            fatal,
            error,
        ));
    }

    /// Update counters after receiving a new input batch.
    ///
    /// See [ControllerStatus::input_batch].
    pub fn input_batch(&self, endpoint_id: Option<(EndpointId, usize)>, num_records: usize) {
        if num_records > 0 {
            self.status
                .input_batch_global(num_records, &self.circuit_thread_unparker);
        }

        if let Some((endpoint_id, num_bytes)) = endpoint_id {
            self.status.input_batch_from_endpoint(
                endpoint_id,
                num_bytes,
                num_records,
                &self.backpressure_thread_unparker,
            )
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

    fn fail_if_restoring(&self) -> Result<(), ControllerError> {
        if self.restoring.load(Ordering::Acquire) {
            Err(Self::warn_restoring())
        } else {
            Ok(())
        }
    }

    /// Returns whether this pipeline supports suspend-and-resume.
    pub fn can_suspend(&self) -> CanSuspend {
        if self
            .status
            .input_status()
            .values()
            .all(|endpoint_stats| endpoint_stats.is_fault_tolerant)
            && self.status.pipeline_config.global.storage.is_some()
        {
            if self.restoring.load(Ordering::Acquire) {
                CanSuspend::NotNow
            } else {
                CanSuspend::Yes
            }
        } else {
            CanSuspend::No
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

    fn is_pipeline_fault_tolerant(&self) -> bool {
        self.controller.fault_tolerant
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
        self.controller.status.completed(
            self.endpoint_id,
            StepResults {
                num_records: num_records as u64,
                hash,
                metadata: None,
                data: None,
            },
            &self.controller.backpressure_thread_unparker,
        );
        self.controller.unpark_circuit();
    }

    fn extended(&self, num_records: usize, hash: u64, metadata: JsonValue, data: RmpValue) {
        self.controller.status.completed(
            self.endpoint_id,
            StepResults {
                num_records: num_records as u64,
                hash,
                metadata: Some(metadata),
                data: Some(data),
            },
            &self.controller.backpressure_thread_unparker,
        );
        self.controller.unpark_circuit();
    }

    fn eoi(&self) {
        self.controller.eoi(self.endpoint_id);
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

#[cfg(test)]
mod test {
    use crate::{
        test::{
            generate_test_batch, init_test_logger, test_circuit, wait, TestStruct,
            DEFAULT_TIMEOUT_MS,
        },
        Controller, PipelineConfig,
    };
    use csv::{ReaderBuilder as CsvReaderBuilder, WriterBuilder as CsvWriterBuilder};
    use std::{
        fs::{create_dir, remove_file, File},
        io::Write,
        thread::sleep,
        time::Duration,
    };
    use tempfile::{NamedTempFile, TempDir};
    use tracing::info;

    use proptest::prelude::*;

    #[test]
    fn test_start_after_cyclic() {
        init_test_logger();

        let config_str = r#"
name: test
workers: 4
inputs:
    test_input1.endpoint1:
        stream: test_input1
        labels:
            - label1
        start_after: label2
        transport:
            name: file_input
            config:
                path: "file1"
        format:
            name: json
            config:
                array: true
                update_format: raw
    test_input1.endpoint2:
        stream: test_input1
        labels:
            - label2
        start_after: label1
        transport:
            name: file_input
            config:
                path: file2
        format:
            name: json
            config:
                array: true
                update_format: raw
    "#
        .to_string();

        let config: PipelineConfig = serde_yaml::from_str(&config_str).unwrap();
        let Err(err) = Controller::with_config(
            |circuit_config| {
                Ok(test_circuit::<TestStruct>(
                    circuit_config,
                    &TestStruct::schema(),
                ))
            },
            &config,
            Box::new(|e| panic!("error: {e}")),
        ) else {
            panic!("expected to fail")
        };

        assert_eq!(&err.to_string(), "invalid controller configuration: cyclic 'start_after' dependency detected: endpoint 'test_input1.endpoint1' with label 'label1' waits for endpoint 'test_input1.endpoint2' with label 'label2', which waits for endpoint 'test_input1.endpoint1' with label 'label1'");
    }

    #[test]
    fn test_start_after() {
        init_test_logger();

        // Two JSON files with a few records each.
        let temp_input_file1 = NamedTempFile::new().unwrap();
        let temp_input_file2 = NamedTempFile::new().unwrap();

        temp_input_file1
            .as_file()
            .write_all(br#"[{"id": 1, "b": true, "s": "foo"}, {"id": 2, "b": true, "s": "foo"}]"#)
            .unwrap();
        temp_input_file2
            .as_file()
            .write_all(br#"[{"id": 3, "b": true, "s": "foo"}, {"id": 4, "b": true, "s": "foo"}]"#)
            .unwrap();

        // Controller configuration with two input connectors;
        // the second starts after the first one finishes.
        let config_str = format!(
            r#"
name: test
workers: 4
inputs:
    test_input1.endpoint1:
        stream: test_input1
        transport:
            name: file_input
            labels:
                - backfill
            config:
                path: {:?}
        format:
            name: json
            config:
                array: true
                update_format: raw
    test_input1.endpoint2:
        stream: test_input1
        start_after: backfill
        transport:
            name: file_input
            config:
                path: {:?}
        format:
            name: json
            config:
                array: true
                update_format: raw
    "#,
            temp_input_file1.path().to_str().unwrap(),
            temp_input_file2.path().to_str().unwrap(),
        );

        let config: PipelineConfig = serde_yaml::from_str(&config_str).unwrap();
        let controller = Controller::with_config(
            |circuit_config| {
                Ok(test_circuit::<TestStruct>(
                    circuit_config,
                    &TestStruct::schema(),
                ))
            },
            &config,
            Box::new(|e| panic!("error: {e}")),
        )
        .unwrap();

        controller.start();

        // Wait 3 seconds, assert(no data in the output table)

        // Unpause the first connector.

        wait(|| controller.pipeline_complete(), DEFAULT_TIMEOUT_MS).unwrap();

        let result = controller
            .execute_query_text_sync("select * from test_output1 order by id")
            .unwrap();

        let expected = r#"+----+------+---+-----+
| id | b    | i | s   |
+----+------+---+-----+
| 1  | true |   | foo |
| 2  | true |   | foo |
| 3  | true |   | foo |
| 4  | true |   | foo |
+----+------+---+-----+"#;

        assert_eq!(&result, expected);
        controller.stop().unwrap();
    }

    // TODO: Parameterize this with config string, so we can test different
    // input/output formats and transports when we support more than one.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(30))]
        #[test]
        fn proptest_csv_file(
            data in generate_test_batch(5000),
            min_batch_size_records in 1..100usize,
            max_buffering_delay_usecs in 1..2000usize,
            input_buffer_size_bytes in 1..1000usize,
            output_buffer_size_records in 1..100usize)
        {
            let temp_input_file = NamedTempFile::new().unwrap();
            let temp_output_path = NamedTempFile::new().unwrap().into_temp_path();
            let output_path = temp_output_path.to_str().unwrap().to_string();
            temp_output_path.close().unwrap();

            let config_str = format!(
                r#"
min_batch_size_records: {min_batch_size_records}
max_buffering_delay_usecs: {max_buffering_delay_usecs}
name: test
workers: 4
inputs:
    test_input1:
        stream: test_input1
        transport:
            name: file_input
            config:
                path: {:?}
                buffer_size_bytes: {input_buffer_size_bytes}
                follow: false
        format:
            name: csv
outputs:
    test_output1:
        stream: test_output1
        transport:
            name: file_output
            config:
                path: {:?}
        format:
            name: csv
            config:
                buffer_size_records: {output_buffer_size_records}
        "#,
            temp_input_file.path().to_str().unwrap(),
            output_path,
            );

            info!("input file: {}", temp_input_file.path().to_str().unwrap());
            info!("output file: {output_path}");
            let config: PipelineConfig = serde_yaml::from_str(&config_str).unwrap();
            let controller = Controller::with_config(
                    |circuit_config| Ok(test_circuit::<TestStruct>(circuit_config, &[])),
                    &config,
                    Box::new(|e| panic!("error: {e}")),
                )
                .unwrap();

            let mut writer = CsvWriterBuilder::new()
                .has_headers(false)
                .from_writer(temp_input_file.as_file());

            for val in data.iter().cloned() {
                writer.serialize(val).unwrap();
            }
            writer.flush().unwrap();
            println!("wait for {} records", data.len());
            controller.start();

            // Wait for the pipeline to output all records.
            wait(|| controller.pipeline_complete(), DEFAULT_TIMEOUT_MS).unwrap();

            assert_eq!(controller.status().output_status().get(&0).unwrap().transmitted_records(), data.len() as u64);

            controller.stop().unwrap();

            let mut expected = data;
            expected.sort();

            let mut actual: Vec<_> = CsvReaderBuilder::new()
                .has_headers(false)
                .from_path(&output_path)
                .unwrap()
                .deserialize::<(TestStruct, i32)>()
                .map(|res| {
                    let (val, weight) = res.unwrap();
                    assert_eq!(weight, 1);
                    val
                })
                .collect();
            actual.sort();

            // Don't leave garbage in the FS.
            remove_file(&output_path).unwrap();

            assert_eq!(actual, expected);
        }
    }

    #[derive(Clone)]
    struct FtTestRound {
        n_records: usize,
        do_checkpoint: bool,
    }

    impl FtTestRound {
        fn with_checkpoint(n_records: usize) -> Self {
            Self {
                n_records,
                do_checkpoint: true,
            }
        }
        fn without_checkpoint(n_records: usize) -> Self {
            Self {
                n_records,
                do_checkpoint: false,
            }
        }
    }

    /// Runs a basic test of fault tolerance.
    ///
    /// The test proceeds in multiple rounds. For each element of `rounds`, the
    /// test writes `n_records` records to the input file, and starts the
    /// pipeline and waits for it to process the data.  If `do_checkpoint` is
    /// true, it creates a new checkpoint. Then it stops the pipeline, checks
    /// that the output is as expected, and goes on to the next round.
    fn test_ft(rounds: &[FtTestRound]) {
        init_test_logger();
        let tempdir = TempDir::new().unwrap();

        // This allows the temporary directory to be deleted when we finish.  If
        // you want to keep it for inspection instead, comment out the following
        // line and then remove the comment markers on the two lines after that.
        let tempdir_path = tempdir.path();
        //let tempdir_path = tempdir.into_path();
        //println!("{}", tempdir_path.display());

        let storage_dir = tempdir_path.join("storage");
        create_dir(&storage_dir).unwrap();
        let input_path = tempdir_path.join("input.csv");
        let input_file = File::create(&input_path).unwrap();
        let output_path = tempdir_path.join("output.csv");

        let config_str = format!(
            r#"
name: test
workers: 4
storage_config:
    path: {storage_dir:?}
storage: true
fault_tolerance: {{}}
clock_resolution_usecs: null
inputs:
    test_input1:
        stream: test_input1
        transport:
            name: file_input
            config:
                path: {input_path:?}
                follow: true
        format:
            name: csv
outputs:
    test_output1:
        stream: test_output1
        transport:
            name: file_output
            config:
                path: {output_path:?}
        format:
            name: csv
            config:
        "#
        );

        let config: PipelineConfig = serde_yaml::from_str(&config_str).unwrap();

        let mut writer = CsvWriterBuilder::new()
            .has_headers(false)
            .from_writer(&input_file);

        // Number of records written to the input.
        let mut total_records = 0usize;

        // Number of input records included in the latest checkpoint (always <=
        // total_records).
        let mut checkpointed_records = 0usize;

        for (
            round,
            FtTestRound {
                n_records,
                do_checkpoint,
            },
        ) in rounds.iter().cloned().enumerate()
        {
            println!(
                "--- round {round}: add {n_records} records, {} --- ",
                if do_checkpoint {
                    "and checkpoint"
                } else {
                    "no checkpoint"
                }
            );

            // Write records to the input file.
            println!(
                "Writing records {total_records}..{}",
                total_records + n_records
            );
            if n_records > 0 {
                for id in total_records..total_records + n_records {
                    writer.serialize(TestStruct::for_id(id as u32)).unwrap();
                }
                writer.flush().unwrap();
                total_records += n_records;
            }

            // Start pipeline.
            println!("start pipeline");
            let controller = Controller::with_config(
                |circuit_config| Ok(test_circuit::<TestStruct>(circuit_config, &[])),
                &config,
                Box::new(|e| panic!("error: {e}")),
            )
            .unwrap();
            controller.start();

            // Wait for the records that are not in the checkpoint to be
            // processed or replayed.
            let expect_n = total_records - checkpointed_records;
            println!(
                "wait for {} records {checkpointed_records}..{total_records}",
                expect_n
            );
            let mut last_n = 0;
            wait(
                || {
                    let n = controller
                        .status()
                        .output_status()
                        .get(&0)
                        .unwrap()
                        .transmitted_records() as usize;
                    if n > last_n {
                        println!("received {n} records");
                        last_n = n;
                    }
                    n >= expect_n
                },
                10_000,
            )
            .unwrap();

            // No more records should arrive, but give the controller some time
            // to send some more in case there's a bug.
            sleep(Duration::from_millis(100));

            // Then verify that the number is as expected.
            assert_eq!(
                controller
                    .status()
                    .output_status()
                    .get(&0)
                    .unwrap()
                    .transmitted_records(),
                expect_n as u64
            );

            // Checkpoint, if requested.
            if do_checkpoint {
                println!("checkpoint");
                controller.checkpoint().unwrap();
            }

            // Stop controller.
            println!("stop controller");
            controller.stop().unwrap();

            // Read output and compare. Our output adapter, which is not
            // fault-tolerant, truncates the output file to length 0 each
            // time. Therefore, the output file should contain all the records
            // in `checkpointed_records..total_records`.
            let mut actual = CsvReaderBuilder::new()
                .has_headers(false)
                .from_path(&output_path)
                .unwrap()
                .deserialize::<(TestStruct, i32)>()
                .map(|res| {
                    let (val, weight) = res.unwrap();
                    assert_eq!(weight, 1);
                    val
                })
                .collect::<Vec<_>>();
            actual.sort();

            assert_eq!(actual.len(), expect_n);
            for (record, expect_record) in actual
                .into_iter()
                .zip((checkpointed_records..).map(|id| TestStruct::for_id(id as u32)))
            {
                assert_eq!(record, expect_record);
            }

            if do_checkpoint {
                checkpointed_records = total_records;
            }
            println!();
        }
    }

    fn _test_concurrent_init(max_parallel_connector_init: u64) {
        init_test_logger();

        // Two JSON files with a few records each.
        let (_temp_input_files, connectors): (Vec<_>, Vec<_>) = (0..100)
            .map(|i| {
                let file = NamedTempFile::new().unwrap();
                file.as_file()
                    .write_all(&format!(r#"[{{"id": {i}, "b": true, "s": "foo"}}]"#).into_bytes())
                    .unwrap();
                let path = file.path().to_str().unwrap();
                let config = format!(
                    r#"
    test_input1.endpoint{i}:
        stream: test_input1
        transport:
            name: file_input
            config:
                path: {path:?}
        format:
            name: json
            config:
                array: true
                update_format: raw"#
                );
                (file, config)
            })
            .unzip();

        let connectors = connectors.join("\n");

        // Controller configuration with 100 input connectors.
        let config_str = format!(
            r#"
name: test
workers: 4
max_parallel_connector_init: {max_parallel_connector_init}
inputs:
{connectors}
    "#
        );

        println!("config: {config_str}");

        let config: PipelineConfig = serde_yaml::from_str(&config_str).unwrap();
        let controller = Controller::with_config(
            |circuit_config| {
                Ok(test_circuit::<TestStruct>(
                    circuit_config,
                    &TestStruct::schema(),
                ))
            },
            &config,
            Box::new(|e| panic!("error: {e}")),
        )
        .unwrap();

        controller.start();

        wait(|| controller.pipeline_complete(), DEFAULT_TIMEOUT_MS).unwrap();

        let result = controller
            .execute_query_text_sync("select count(*) from test_output1")
            .unwrap();

        let expected = r#"+----------+
| count(*) |
+----------+
| 100      |
+----------+"#;

        assert_eq!(&result, expected);
        controller.stop().unwrap();
    }

    #[test]
    fn test_concurrent_init() {
        _test_concurrent_init(1);
        _test_concurrent_init(10);
        _test_concurrent_init(100);
    }

    #[test]
    fn test_connector_init_error() {
        init_test_logger();

        // Two JSON files with a few records each.
        let (_temp_input_files, connectors): (Vec<_>, Vec<_>) = (0..20)
            .map(|i| {
                let file = NamedTempFile::new().unwrap();
                file.as_file()
                    .write_all(&format!(r#"[{{"id": {i}, "b": true, "s": "foo"}}]"#).into_bytes())
                    .unwrap();
                let path = file.path().to_str().unwrap();
                let config = format!(
                    r#"
    test_input1.endpoint{i}:
        stream: test_input1
        transport:
            name: file_input
            config:
                path: {path:?}
        format:
            name: json
            config:
                array: true
                update_format: raw"#
                );
                (file, config)
            })
            .unzip();

        let connectors = connectors.join("\n");

        // Controller configuration with two input connectors;
        // the second starts after the first one finishes.
        let config_str = format!(
            r#"
name: test
workers: 4
inputs:
{connectors}
    test_input1.error_endpoint:
        stream: test_input1
        transport:
            name: file_input
            config:
                path: path_does_not_exist
        format:
            name: json
            config:
                array: true
                update_format: raw"#
        );

        println!("config: {config_str}");

        let config: PipelineConfig = serde_yaml::from_str(&config_str).unwrap();
        let result = Controller::with_config(
            |circuit_config| {
                Ok(test_circuit::<TestStruct>(
                    circuit_config,
                    &TestStruct::schema(),
                ))
            },
            &config,
            Box::new(|e| panic!("error: {e}")),
        );

        assert!(result.is_err());
    }

    #[test]
    fn ft_with_checkpoints() {
        test_ft(&[
            FtTestRound::with_checkpoint(2500),
            FtTestRound::with_checkpoint(2500),
            FtTestRound::with_checkpoint(2500),
            FtTestRound::with_checkpoint(2500),
            FtTestRound::with_checkpoint(2500),
        ]);
    }

    #[test]
    fn ft_without_checkpoints() {
        test_ft(&[
            FtTestRound::without_checkpoint(2500),
            FtTestRound::without_checkpoint(2500),
            FtTestRound::without_checkpoint(2500),
            FtTestRound::without_checkpoint(2500),
            FtTestRound::without_checkpoint(2500),
        ]);
    }

    #[test]
    fn ft_alternating() {
        test_ft(&[
            FtTestRound::with_checkpoint(2500),
            FtTestRound::without_checkpoint(2500),
            FtTestRound::with_checkpoint(2500),
            FtTestRound::without_checkpoint(2500),
            FtTestRound::with_checkpoint(2500),
            FtTestRound::without_checkpoint(2500),
            FtTestRound::with_checkpoint(2500),
            FtTestRound::without_checkpoint(2500),
            FtTestRound::with_checkpoint(2500),
            FtTestRound::without_checkpoint(2500),
        ]);
    }

    #[test]
    fn ft_initially_zero_without_checkpoint() {
        test_ft(&[
            FtTestRound::without_checkpoint(0),
            FtTestRound::without_checkpoint(2500),
            FtTestRound::without_checkpoint(0),
            FtTestRound::with_checkpoint(2500),
            FtTestRound::without_checkpoint(2500),
            FtTestRound::with_checkpoint(2500),
            FtTestRound::without_checkpoint(2500),
            FtTestRound::with_checkpoint(2500),
        ]);
    }

    #[test]
    fn ft_initially_zero_with_checkpoint() {
        test_ft(&[
            FtTestRound::with_checkpoint(0),
            FtTestRound::without_checkpoint(2500),
            FtTestRound::without_checkpoint(0),
            FtTestRound::with_checkpoint(2500),
            FtTestRound::without_checkpoint(2500),
            FtTestRound::with_checkpoint(2500),
            FtTestRound::without_checkpoint(2500),
            FtTestRound::with_checkpoint(2500),
        ]);
    }

    /// Runs a basic test of suspend and resume, without fault tolerance.
    ///
    /// For each element of `rounds`, the test writes the specified number of
    /// records to the input file, and starts the pipeline and waits for it to
    /// process the data.  Then it suspends the pipeline, checks that the output
    /// is as expected, and goes on to the next round.
    fn test_suspend(rounds: &[usize]) {
        init_test_logger();
        let tempdir = TempDir::new().unwrap();

        // This allows the temporary directory to be deleted when we finish.  If
        // you want to keep it for inspection instead, comment out the following
        // line and then remove the comment markers on the two lines after that.
        let tempdir_path = tempdir.path();
        //let tempdir_path = tempdir.into_path();
        //println!("{}", tempdir_path.display());

        let storage_dir = tempdir_path.join("storage");
        create_dir(&storage_dir).unwrap();
        let input_path = tempdir_path.join("input.csv");
        let input_file = File::create(&input_path).unwrap();
        let output_path = tempdir_path.join("output.csv");

        let config_str = format!(
            r#"
name: test
workers: 4
storage_config:
    path: {storage_dir:?}
storage: true
clock_resolution_usecs: null
inputs:
    test_input1:
        stream: test_input1
        transport:
            name: file_input
            config:
                path: {input_path:?}
                follow: true
        format:
            name: csv
outputs:
    test_output1:
        stream: test_output1
        transport:
            name: file_output
            config:
                path: {output_path:?}
        format:
            name: csv
            config:
        "#
        );

        let config: PipelineConfig = serde_yaml::from_str(&config_str).unwrap();

        let mut writer = CsvWriterBuilder::new()
            .has_headers(false)
            .from_writer(&input_file);

        // Number of records written to the input.
        let mut total_records = 0usize;

        // Number of input records included in the latest checkpoint (always <=
        // total_records).
        let mut checkpointed_records = 0usize;

        for (round, n_records) in rounds.iter().copied().enumerate() {
            println!("--- round {round}: add {n_records} records and suspend --- ");

            // Write records to the input file.
            println!(
                "Writing records {total_records}..{}",
                total_records + n_records
            );
            if n_records > 0 {
                for id in total_records..total_records + n_records {
                    writer.serialize(TestStruct::for_id(id as u32)).unwrap();
                }
                writer.flush().unwrap();
                total_records += n_records;
            }

            // Start pipeline.
            println!("start pipeline");
            let controller = Controller::with_config(
                |circuit_config| Ok(test_circuit::<TestStruct>(circuit_config, &[])),
                &config,
                Box::new(|e| panic!("error: {e}")),
            )
            .unwrap();
            controller.start();

            // Wait for the records that are not in the checkpoint to be
            // processed or replayed.
            let expect_n = total_records - checkpointed_records;
            println!(
                "wait for {} records {checkpointed_records}..{total_records}",
                expect_n
            );
            let mut last_n = 0;
            wait(
                || {
                    let n = controller
                        .status()
                        .output_status()
                        .get(&0)
                        .unwrap()
                        .transmitted_records() as usize;
                    if n > last_n {
                        println!("received {n} records");
                        last_n = n;
                    }
                    n >= expect_n
                },
                10_000,
            )
            .unwrap();

            // No more records should arrive, but give the controller some time
            // to send some more in case there's a bug.
            sleep(Duration::from_millis(100));

            // Then verify that the number is as expected.
            assert_eq!(
                controller
                    .status()
                    .output_status()
                    .get(&0)
                    .unwrap()
                    .transmitted_records(),
                expect_n as u64
            );

            // Suspend.
            println!("suspend");
            controller.suspend().unwrap();

            // Stop controller.
            println!("stop controller");
            controller.stop().unwrap();

            // Read output and compare. Our output adapter, which is not
            // fault-tolerant, truncates the output file to length 0 each
            // time. Therefore, the output file should contain all the records
            // in `checkpointed_records..total_records`.
            let mut actual = CsvReaderBuilder::new()
                .has_headers(false)
                .from_path(&output_path)
                .unwrap()
                .deserialize::<(TestStruct, i32)>()
                .map(|res| {
                    let (val, weight) = res.unwrap();
                    assert_eq!(weight, 1);
                    val
                })
                .collect::<Vec<_>>();
            actual.sort();

            assert_eq!(actual.len(), expect_n);
            for (record, expect_record) in actual
                .into_iter()
                .zip((checkpointed_records..).map(|id| TestStruct::for_id(id as u32)))
            {
                assert_eq!(record, expect_record);
            }

            checkpointed_records = total_records;
            println!();
        }
    }

    #[test]
    fn suspend() {
        test_suspend(&[2500, 2500, 2500, 2500, 2500]);
    }
}
