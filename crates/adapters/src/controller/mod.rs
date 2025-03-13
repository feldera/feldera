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
use crate::create_integrated_output_endpoint;
use crate::transport::Step;
use crate::transport::{input_transport_config_to_endpoint, output_transport_config_to_endpoint};
use crate::{
    catalog::SerBatch, CircuitCatalog, Encoder, InputConsumer, OutputConsumer, OutputEndpoint,
    ParseError, PipelineState, TransportInputEndpoint,
};
use anyhow::Error as AnyError;
use arrow::datatypes::Schema;
use atomic::Atomic;
use crossbeam::{
    queue::SegQueue,
    sync::{Parker, ShardedLock, Unparker},
};
use datafusion::prelude::*;
use dbsp::circuit::tokio::TOKIO;
use dbsp::circuit::CircuitStorageConfig;
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
use metadata::Checkpoint;
use metadata::InputLog;
use metadata::StepMetadata;
use metadata::StepRw;
use metrics::set_global_recorder;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use metrics_util::{
    debugging::{DebuggingRecorder, Snapshotter},
    layers::FanoutBuilder,
};
use nonzero_ext::nonzero;
use rmpv::Value as RmpValue;
use serde_json::Value as JsonValue;
use std::borrow::Cow;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::sync::mpsc::{channel, sync_channel, Receiver, Sender};
use std::sync::LazyLock;
use std::{
    collections::{BTreeMap, BTreeSet},
    io::Error as IoError,
    mem,
    sync::OnceLock,
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
use uuid::Uuid;
use validate::validate_config;

mod error;
mod metadata;
mod stats;
mod validate;

use crate::adhoc::table::AdHocTable;
use crate::catalog::{SerBatchReader, SerTrace, SyncSerBatchReader};
use crate::format::parquet::relation_to_arrow_fields;
use crate::format::{get_input_format, get_output_format};
use crate::integrated::create_integrated_input_endpoint;
pub use error::{ConfigError, ControllerError};
use feldera_types::config::OutputBufferConfig;
pub use feldera_types::config::{
    ConnectorConfig, FormatConfig, InputEndpointConfig, OutputEndpointConfig, PipelineConfig,
    RuntimeConfig, TransportConfig,
};
use feldera_types::format::json::{JsonFlavor, JsonParserConfig, JsonUpdateFormat};
use feldera_types::program_schema::{canonical_identifier, SqlIdentifier};
pub use stats::{ControllerStatus, InputEndpointStatus, OutputEndpointStatus};

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

/// A command that [Controller] can send to [Controller::circuit_thread].
///
/// There is no type for a command reply.  Instead, the command implementation
/// uses a callback embedded in the command to reply.
enum Command {
    GraphProfile(GraphProfileCallbackFn),
    Checkpoint(CheckpointCallbackFn),
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
                            circuit_thread.run()
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
        // Update pipeline metrics computed on-demand.
        self.inner
            .status
            .update(self.inner.metrics_snapshotter.snapshot());
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

    pub fn checkpoint(&self) -> Result<Checkpoint, ControllerError> {
        let (sender, receiver) = oneshot::channel();
        self.start_checkpoint(Box::new(move |result| sender.send(result).unwrap()));
        receiver.blocking_recv().unwrap()
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
        self.inner.prometheus_handle.clone()
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
    ft: Option<FtState>,
    parker: Parker,
    last_checkpoint: Instant,
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
        let ControllerInit {
            pipeline_config,
            circuit_config,
            ft,
            processed_records,
        } = ControllerInit::new(config)?;
        let (circuit, catalog) = circuit_factory(circuit_config)?;
        let (parker, backpressure_thread, command_receiver, controller) = ControllerInner::new(
            pipeline_config,
            catalog,
            error_cb,
            ft.is_some(),
            processed_records,
        )?;

        let ft = ft
            .map(|ft| FtState::new(ft, controller.clone()))
            .transpose()?;

        Ok(Self {
            controller,
            ft,
            circuit,
            command_receiver,
            backpressure_thread,
            parker,
            last_checkpoint: Instant::now(),
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
            self.run_commands();
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
                    let start = Instant::now();
                    let done = !self.step()?;
                    self.controller
                        .status
                        .global_metrics
                        .runtime_elapsed_msecs
                        .fetch_add(
                            start.elapsed().as_millis() as u64
                                * self.controller.status.pipeline_config.global.workers as u64
                                * 2,
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
        if let Some(ft) = self.ft.as_mut() {
            ft.write_step(step_metadata)?;
        }

        // Wake up the backpressure thread to unpause endpoints blocked due to
        // backpressure.
        self.controller.unpark_backpressure();
        debug!("circuit thread: calling 'circuit.step'");
        self.circuit
            .step()
            .unwrap_or_else(|e| self.controller.error(e.into()));
        debug!("circuit thread: 'circuit.step' returned");

        let processed_records = self.processed_records(total_consumed);

        // Update `trace_snapshot` to the latest traces
        self.update_snapshot();

        // Push output batches to output pipelines.
        if let Some(ft) = self.ft.as_mut() {
            ft.sync_step()?;
        }
        self.push_output(processed_records);

        if let Some(ft) = self.ft.as_mut() {
            ft.next_step()?;
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
            this.controller.fail_if_restoring()?;
            let Some(ft) = this.ft.as_mut() else {
                return Err(ControllerError::NotSupported {
                    error: String::from(
                        "cannot checkpoint circuit because fault tolerance is not enabled",
                    ),
                });
            };
            ft.checkpoint(&mut this.circuit)
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
    fn run_commands(&mut self) {
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
            }
        }
    }

    /// Reads and replies to all of the commands pending from
    /// `self.command_receiver` without executing them.
    fn flush_commands(&mut self) {
        for command in self.command_receiver.try_iter() {
            match command {
                Command::GraphProfile(callback) => callback(Err(ControllerError::ControllerExit)),
                Command::Checkpoint(callback) => callback(Err(ControllerError::ControllerExit)),
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
                        value: results.metadata.unwrap_or(RmpValue::Nil),
                        num_records: results.num_records,
                        hash: results.hash,
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

                endpoint.queue.push((
                    self.ft.as_ref().map_or(0, |ft| ft.step),
                    batch,
                    processed_records,
                ));

                // Wake up the output thread.  We're not trying to be smart here and
                // wake up the thread conditionally if it was previously idle, as I
                // don't expect this to make any real difference.
                endpoint.unparker.unpark();
            }
        }
        drop(outputs);
    }

    fn replaying(&self) -> bool {
        self.ft.as_ref().map_or(false, |ft| ft.replaying)
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

    /// The step currently running or replaying.
    step: Step,

    /// The step reader/writer.
    ///
    /// This is always non-`None` unless a fatal error occurs.
    step_rw: Option<StepRw>,

    /// Whether we are currently replaying a previous step.
    replaying: bool,

    /// Metadata for the last step we read or wrote. This is only `None` if
    /// `step` is 0.
    prev_step_metadata: Option<StepMetadata>,

    // Input endpoint ids and names at the time we wrote the last step,
    // so that we can log changes for the replay log.
    input_endpoints: HashMap<EndpointId, String>,
}

impl FtState {
    /// Returns a new [FtState] for `ft` and `controller`.
    fn new(ft: FtInit, controller: Arc<ControllerInner>) -> Result<Self, ControllerError> {
        let FtInit { step, step_rw } = ft;

        let (step_metadata, prev_step_metadata, step_rw) = match step_rw {
            Some(step_rw) if step > 0 => {
                let (step_rw, prev_step_metadata) =
                    step_rw.into_reader().unwrap().seek(step - 1)?;
                for (endpoint_name, metadata) in &prev_step_metadata.input_logs {
                    let endpoint_id = controller.input_endpoint_id_by_name(endpoint_name)?;
                    controller.status.input_status()[&endpoint_id]
                        .reader
                        .seek(metadata.value.clone());
                }
                let (step_rw, step_metadata) =
                    Self::replay_step(StepRw::Reader(step_rw), step, &controller)?;
                (step_metadata, Some(prev_step_metadata), step_rw)
            }
            Some(step_rw) => {
                let (step_rw, step_metadata) = Self::replay_step(step_rw, step, &controller)?;
                (step_metadata, None, step_rw)
            }
            None => {
                let config = controller.status.pipeline_config.clone();
                let path = storage_path(&config).unwrap();
                let state_path = state_path(&config).unwrap();
                let steps_path = steps_path(&config).unwrap();

                fs::create_dir_all(path).map_err(|error| {
                    ControllerError::io_error(String::from("controller startup"), error)
                })?;
                let _ = fs::remove_file(&state_path);
                let _ = fs::remove_dir(&steps_path);

                let checkpoint = Checkpoint {
                    circuit: None,
                    step: 0,
                    config,
                    processed_records: 0,
                };
                checkpoint.write(&state_path)?;

                (None, None, StepRw::create(&steps_path)?)
            }
        };

        Ok(Self {
            input_endpoints: Self::initial_input_endpoints(&controller),
            controller,
            step,
            step_rw: Some(step_rw),
            replaying: step_metadata.is_some(),
            prev_step_metadata: step_metadata.or(prev_step_metadata),
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
        step_rw: StepRw,
        step: Step,
        controller: &Arc<ControllerInner>,
    ) -> Result<(StepRw, Option<StepMetadata>), ControllerError> {
        // Read a step.
        let (metadata, step_rw) = step_rw.read()?;

        let Some(metadata) = metadata else {
            // No more steps to replay.
            info!("input replay complete");
            return Ok((step_rw, None));
        };

        // There's a step to replay.
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
        for (endpoint_name, metadata) in &metadata.input_logs {
            let endpoint_id = controller.input_endpoint_id_by_name(endpoint_name)?;
            controller.status.input_status()[&endpoint_id]
                .reader
                .replay(metadata.value.clone());
        }
        Ok((step_rw, Some(metadata)))
    }

    /// Writes `step_metadata` to the step writer.
    fn write_step(&mut self, input_logs: HashMap<String, InputLog>) -> Result<(), ControllerError> {
        if let Some(step_writer) = self.step_rw.as_mut().and_then(|rw| rw.as_writer()) {
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
                step: self.step,
                remove_inputs,
                add_inputs,
                input_logs,
            };
            step_writer.write(&step_metadata)?;
            self.prev_step_metadata = Some(step_metadata);
        } else if self.replaying {
            let prev_step_metadata = self.prev_step_metadata.as_ref().unwrap();
            let logged = prev_step_metadata
                .input_logs
                .iter()
                .map(|(endpoint_name, entry)| {
                    (endpoint_name.as_str(), (entry.num_records, entry.hash))
                })
                .collect::<BTreeMap<_, _>>();
            let replayed = input_logs
                .iter()
                .map(|(endpoint_name, entry)| {
                    (endpoint_name.as_str(), (entry.num_records, entry.hash))
                })
                .collect::<BTreeMap<_, _>>();
            if logged != replayed {
                let error = format!("Logged and replayed step {} contained different numbers of records or hashes:\nLogged: {logged:?}\nReplayed: {replayed:?}", self.step);
                error!("{error}");
                return Err(ControllerError::ReplayFailure { error });
            }
        }
        Ok(())
    }

    /// Waits for the step writer to commit the step (written by
    /// [Self::write_step]) to stable storage.
    fn sync_step(&mut self) -> Result<(), ControllerError> {
        if let Some(step_writer) = self.step_rw.as_mut().and_then(|rw| rw.as_writer()) {
            step_writer.wait()?;
        }
        Ok(())
    }

    /// Advances to the next step.
    ///
    /// If we were replaying before, this attempts to replay the next step too.
    fn next_step(&mut self) -> Result<(), ControllerError> {
        self.step += 1;
        if self.replaying {
            let (step_rw, step_metadata) =
                Self::replay_step(self.step_rw.take().unwrap(), self.step, &self.controller)?;
            self.step_rw = Some(step_rw);
            self.replaying = step_metadata.is_some();
            if self.replaying {
                self.prev_step_metadata = step_metadata;
            } else {
                info!("replay complete, starting pipeline");
                self.input_endpoints = Self::initial_input_endpoints(&self.controller);
            }
        }
        Ok(())
    }

    /// Writes out a checkpoint for `circuit`.
    fn checkpoint(&mut self, circuit: &mut DBSPHandle) -> Result<Checkpoint, ControllerError> {
        // Replace the input adapter configuration in the pipeline configuration
        // by the current inputs. (HTTP input adapters might have been added or
        // removed.)
        let config = PipelineConfig {
            inputs: self
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
            ..self.controller.status.pipeline_config.clone()
        };

        let checkpoint = circuit
            .commit()
            .map_err(ControllerError::from)
            .and_then(|circuit| {
                let checkpoint = Checkpoint {
                    circuit: Some(circuit),
                    step: self.step,
                    config,
                    processed_records: self
                        .controller
                        .status
                        .global_metrics
                        .num_total_processed_records(),
                };
                let state_path = state_path(&self.controller.status.pipeline_config).unwrap();
                checkpoint.write(&state_path).map(|()| checkpoint)
            })?;

        if self
            .prev_step_metadata
            .as_ref()
            .is_none_or(|psm| self.step > psm.step)
        {
            self.step_rw = Some(
                self.step_rw
                    .take()
                    .unwrap()
                    .truncate(&self.prev_step_metadata)?,
            );
        } else {
            // We have the current step's metadata instead of the previous
            // one's. This is what happens if we checkpoint immediately after
            // resume. No big deal, we'll just skip truncating the steps file
            // until next checkpoint.
        }

        Ok(checkpoint)
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
        let checkpoint_interval =
            config
                .fault_tolerance
                .and_then(|ft| match ft.checkpoint_interval_secs {
                    0 => None,
                    secs => Some(Duration::from_secs(secs)),
                });
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

        let now = Instant::now();
        if replaying
            || self.controller.status.unset_step_requested()
            || buffered_records > self.min_batch_size_records
            || tick.map_or(false, |t| now >= t)
            || self.buffer_timeout.map_or(false, |t| now >= t)
        {
            self.tick = self.clock_resolution.map(|delay| now + delay);
            self.buffer_timeout = None;
            Action::Step
        } else if checkpoint.map_or(false, |t| now >= t) {
            Action::Checkpoint
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

    /// Fault-tolerance initialization, if FT will be enabled.
    ft: Option<FtInit>,

    /// Initial counter for `total_processed_records`.
    processed_records: u64,
}

struct FtInit {
    /// The first step that the circuit will execute.
    step: Step,

    /// The step reader/writer, if we've already opened it..
    step_rw: Option<StepRw>,
}

fn storage_path(config: &PipelineConfig) -> Option<&Path> {
    config.storage_config.as_ref().map(|storage| storage.path())
}

fn state_path(config: &PipelineConfig) -> Option<PathBuf> {
    storage_path(config).map(|path| path.join("state.json"))
}

fn steps_path(config: &PipelineConfig) -> Option<PathBuf> {
    storage_path(config).map(|path| path.join("steps.bin"))
}

impl ControllerInit {
    fn new(config: PipelineConfig) -> Result<Self, ControllerError> {
        if config.global.fault_tolerance.is_none() {
            info!("fault tolerance is disabled in configuration");
            return Ok(Self {
                circuit_config: Self::circuit_config(&config, None)?,
                pipeline_config: config,
                ft: None,
                processed_records: 0,
            });
        };
        let Some(state_path) = state_path(&config) else {
            return Err(ControllerError::Config {
                config_error: ConfigError::FtRequiresStorage,
            });
        };
        let steps_path = steps_path(&config).unwrap();

        fn startup_io_error(error: IoError) -> ControllerError {
            ControllerError::io_error(String::from("controller startup"), error)
        }

        // If a checkpoint exists, use it.
        if fs::exists(&state_path).map_err(startup_io_error)? {
            // Open the existing checkpoint.
            info!(
                "{}: resuming fault tolerant pipeline from a saved checkpoint",
                state_path.display()
            );
            let Checkpoint {
                circuit,
                step,
                config,
                processed_records,
            } = Checkpoint::read(&state_path)?;

            // There might be a steps file already (there must be, if
            // we're not at step 0). Open it, if so; otherwise, create a
            // new one.
            let step_rw = if fs::exists(&steps_path).map_err(startup_io_error)? || step > 0 {
                info!(
                    "{}: opening to start from step {}",
                    steps_path.display(),
                    step
                );
                StepRw::open(&steps_path)?
            } else {
                info!("{}: creating", steps_path.display());
                StepRw::create(&steps_path)?
            };
            Ok(Self {
                circuit_config: Self::circuit_config(&config, circuit.map(|circuit| circuit.uuid))?,
                pipeline_config: config,
                ft: Some(FtInit {
                    step,
                    step_rw: Some(step_rw),
                }),
                processed_records,
            })
        } else {
            // We're starting a new fault-tolerant pipeline.
            //
            // Defer creating the checkpoint and steps files on disk because we
            // might fail to initialize[*] and if that happens we want to be
            // able to try again with a new configuration, rather than
            // triggering the "open the existing checkpoint" flow above and
            // mysterioulsy failing again for the same reason as before.
            //
            // [*] For example, if the configured input adapters aren't
            // fault-tolerant.
            info!(
                "{}: creating new fault tolerant pipeline",
                state_path.display()
            );
            Ok(Self {
                circuit_config: Self::circuit_config(&config, None)?,
                pipeline_config: config,
                ft: Some(FtInit {
                    step: 0,
                    step_rw: None,
                }),
                processed_records: 0,
            })
        }
    }
    fn circuit_config(
        pipeline_config: &PipelineConfig,
        init_checkpoint: Option<Uuid>,
    ) -> Result<CircuitConfig, ControllerError> {
        Ok(CircuitConfig {
            layout: Layout::new_solo(pipeline_config.global.workers as usize),
            pin_cpus: pipeline_config.global.pin_cpus.clone(),
            // Put the circuit's checkpoints in a `circuit` subdirectory of the
            // storage directory.
            storage: if let Some(options) = &pipeline_config.global.storage {
                if let Some(config) = &pipeline_config.storage_config {
                    Some(CircuitStorageConfig {
                        config: config.clone(),
                        options: options.clone(),
                        init_checkpoint,
                    })
                } else {
                    return Err(ControllerError::not_supported(
                        "Pipeline requires storage but runner does not have storage configured",
                    ));
                }
            } else {
                None
            },
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
    metrics_snapshotter: Arc<Snapshotter>,
    prometheus_handle: PrometheusHandle,
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
        fault_tolerant: bool,
        processed_records: u64,
    ) -> Result<(Parker, BackpressureThread, Receiver<Command>, Arc<Self>), ControllerError> {
        let pipeline_name = config
            .name
            .as_ref()
            .map_or_else(|| "unnamed".to_string(), |n| n.clone());
        let status = Arc::new(ControllerStatus::new(config, processed_records));
        let (metrics_snapshotter, prometheus_handle) =
            Self::install_metrics_recorder(pipeline_name);
        let circuit_thread_parker = Parker::new();
        let backpressure_thread_parker = Parker::new();
        let (command_sender, command_receiver) = channel();
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
            metrics_snapshotter,
            prometheus_handle,
            session_ctxt: SessionContext::new(),
            fault_tolerant,
            restoring: AtomicBool::new(fault_tolerant),
        });
        controller.initialize_adhoc_queries();
        for (input_name, input_config) in controller.status.pipeline_config.inputs.iter() {
            controller.connect_input(input_name, input_config)?;
        }
        for (output_name, output_config) in controller.status.pipeline_config.outputs.iter() {
            controller.connect_output(output_name, output_config)?;
        }
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

    /// Sets the global metrics recorder and returns a `Snapshotter` and
    /// a `PrometheusHandle` to get metrics in a prometheus compatible format.
    fn install_metrics_recorder(pipeline_name: String) -> (Arc<Snapshotter>, PrometheusHandle) {
        static METRIC_HANDLES: OnceLock<(Arc<Snapshotter>, PrometheusHandle)> = OnceLock::new();
        METRIC_HANDLES
            .get_or_init(|| {
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
            })
            .clone()
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

        let mut inputs = self.status.inputs.write().unwrap();

        if inputs.values().any(|ep| ep.endpoint_name == endpoint_name) {
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
        let reader = match endpoint {
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

                endpoint
                    .open(probe, parser, input_handle.schema.clone())
                    .map_err(|e| ControllerError::input_transport_error(endpoint_name, true, e))?
            }
            None => {
                let endpoint =
                    create_integrated_input_endpoint(endpoint_name, &endpoint_config, probe)?;

                endpoint
                    .open(input_handle)
                    .map_err(|e| ControllerError::input_transport_error(endpoint_name, true, e))?
            }
        };

        inputs.insert(
            endpoint_id,
            InputEndpointStatus::new(endpoint_name, endpoint_config, reader),
        );

        drop(inputs);

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
        let mut outputs = self.outputs.write().unwrap();

        if outputs.lookup_by_name(endpoint_name).is_some() {
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

        outputs.insert(endpoint_id, handles.clone(), endpoint_descr);

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

        drop(outputs);

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

    fn fail_if_restoring(&self) -> Result<(), ControllerError> {
        if self.restoring.load(Ordering::Acquire) {
            static RATE_LIMIT: LazyLock<DefaultDirectRateLimiter> =
                LazyLock::new(|| RateLimiter::direct(Quota::per_minute(nonzero!(10u32))));
            if RATE_LIMIT.check().is_ok() {
                warn!("Failing request because restore from checkpoint is in progress");
            }
            Err(ControllerError::RestoreInProgress)
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
            num_records as u64,
            hash,
            None,
            &self.controller.backpressure_thread_unparker,
        );
        self.controller.unpark_circuit();
    }

    fn extended(&self, num_records: usize, hash: u64, metadata: RmpValue) {
        self.controller.status.completed(
            self.endpoint_id,
            num_records as u64,
            hash,
            Some(metadata),
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
    /// true, it creates a new checkpoint. Then it stops the checkpoint, checks
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

            // Read output and compare. Our output adapter, which is not FT,
            // truncates the output file to length 0 each time. Therefore, the
            // output file should contain all the records in
            // `checkpointed_records..total_records`.
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
}
