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
use crate::transport::input_transport_config_is_fault_tolerant;
use crate::transport::InputReader;
use crate::transport::Step;
use crate::transport::{input_transport_config_to_endpoint, output_transport_config_to_endpoint};
use crate::InputBuffer;
use crate::{
    catalog::SerBatch, CircuitCatalog, Encoder, InputConsumer, InputFormat, OutputConsumer,
    OutputEndpoint, OutputFormat, ParseError, PipelineState, TransportInputEndpoint,
};
use anyhow::Error as AnyError;
use arrow::datatypes::Schema;
use atomic::Atomic;
use crossbeam::{
    queue::SegQueue,
    sync::{Parker, ShardedLock, Unparker},
};
use datafusion::prelude::*;
use dbsp::circuit::checkpointer::CheckpointMetadata;
use dbsp::{
    circuit::{CircuitConfig, Layout, StorageConfig},
    profile::GraphProfile,
    DBSPHandle,
};
use log::{debug, error, info, trace};
use metadata::Checkpoint;
use metadata::StepMetadata;
use metadata::StepRw;
use metrics::set_global_recorder;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use metrics_util::{
    debugging::{DebuggingRecorder, Snapshotter},
    layers::FanoutBuilder,
};
use rmpv::Value as RmpValue;
use stats::StepProgress;
use std::borrow::Cow;
use std::cmp::min;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs;
use std::path::PathBuf;
use std::sync::mpsc::{channel, sync_channel, Receiver, Sender};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::Error as IoError,
    mem,
    sync::OnceLock,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    thread::{spawn, JoinHandle},
    time::{Duration, Instant},
};
use tokio::sync::{oneshot::Sender as OneshotSender, Mutex as TokioMutex};
use uuid::Uuid;

mod error;
mod metadata;
mod stats;

use crate::adhoc::table::AdHocTable;
use crate::catalog::{SerBatchReader, SerTrace, SyncSerBatchReader};
use crate::format::parquet::relation_to_arrow_fields;
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

/// Type of the callback argumen to [`Controller::start_graph_profile`].
pub type GraphProfileCallbackFn = Box<dyn FnOnce(Result<GraphProfile, ControllerError>) + Send>;

/// A command that [Controller] can send to [Controller::circuit_thread].
///
/// There is no type for a command reply.  Instead, the command implementation
/// uses a callback or [Sender] embedded in the command to reply.
enum Command {
    GraphProfile(GraphProfileCallbackFn),
    Checkpoint(Sender<Result<Checkpoint, ControllerError>>),
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
        self.inner.connect_input(endpoint_name, config)
    }

    /// Disconnect an existing input endpoint.
    ///
    /// This method is asynchronous and may return before all endpoint
    /// threads have terminated.
    pub fn disconnect_input(&self, endpoint_id: &EndpointId) {
        debug!("Disconnecting input endpoint {endpoint_id}");

        self.inner.disconnect_input(endpoint_id)
    }

    pub fn session_context(&self) -> SessionContext {
        self.inner.session_ctxt.clone()
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
        debug!("Adding input endpoint '{endpoint_name}'; config: {endpoint_config:?}");

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

        self.inner
            .add_output_endpoint(endpoint_name, endpoint_config, Some(endpoint))
    }

    /// Reports whether the circuit is fault tolerant.  A circuit is fault
    /// tolerant if it computes a deterministic function and all of its inputs
    /// and outputs are fault tolerant.  This function assumes that the circuit
    /// is deterministic.
    pub fn is_fault_tolerant(&self) -> bool {
        self.inner.has_fault_tolerant_inputs() && self.inner.has_fault_tolerant_outputs()
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

    pub fn checkpoint(&self) -> Result<Checkpoint, ControllerError> {
        self.inner.checkpoint()
    }
}

struct CircuitThread {
    controller: Arc<ControllerInner>,
    step: Step,
    circuit: DBSPHandle,
    command_receiver: Receiver<Command>,
    state_path: Option<PathBuf>,
    step_rw: Option<StepRw>,
    backpressure_thread: BackpressureThread,
    parker: Parker,
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
            step,
            pipeline_config,
            circuit_config,
            state_path,
            step_rw,
        } = ControllerInit::new(config)?;
        let (circuit, catalog) = circuit_factory(circuit_config)?;
        let (parker, backpressure_thread, command_receiver, controller) =
            ControllerInner::new(pipeline_config, catalog, error_cb)?;

        Ok(Self {
            controller,
            step,
            circuit,
            command_receiver,
            state_path,
            step_rw,
            backpressure_thread,
            parker,
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

        self.seek_to_initial_step()?;
        let mut replaying = self.replay_step()?;

        if !replaying {
            self.backpressure_thread.start();
        }

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

            if let Some(wait) = trigger.trigger(replaying, running) {
                wait.park(&self.parker);
                continue;
            }

            let Ok(FlushedInput {
                total_consumed,
                notifications,
                step_metadata,
            }) = self.flush_input_to_circuit(replaying)
            else {
                break;
            };
            if !replaying {
                self.write_step(step_metadata)?;
            }

            // Wake up the backpressure thread to unpause endpoints blocked due to
            // backpressure.
            self.controller.unpark_backpressure();
            self.step();

            let processed_records = self.processed_records(total_consumed, notifications);

            // Update `trace_snapshot` to the latest traces
            self.update_snapshot();

            // Push output batches to output pipelines.
            if !replaying {
                self.sync_step()?;
            }
            self.push_output(processed_records);

            self.step += 1;
            if replaying {
                replaying = self.replay_step()?;
                if !replaying && running {
                    info!("starting pipeline");
                    self.backpressure_thread.start();
                }
            }
            self.controller.unpark_backpressure();
        }
        self.flush_commands();
        self.circuit
            .kill()
            .map_err(|_| ControllerError::dbsp_panic())
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

    /// Reads and executes all the commands pending from
    /// `self.command_receiver`.
    fn run_commands(&mut self) {
        for command in self.command_receiver.try_iter() {
            match command {
                Command::GraphProfile(reply_callback) => reply_callback(
                    self.circuit
                        .graph_profile()
                        .map_err(ControllerError::dbsp_error),
                ),
                Command::Checkpoint(reply_sender) => {
                    let result = if let Some(state_path) = self.state_path.as_ref() {
                        self.circuit
                            .commit()
                            .map_err(ControllerError::from)
                            .and_then(|circuit| {
                                let checkpoint = Checkpoint {
                                    circuit,
                                    step: self.step,
                                    config: self.controller.status.pipeline_config.clone(),
                                };
                                checkpoint.write(state_path).map(|()| checkpoint)
                            })
                    } else {
                        Err(ControllerError::NotSupported {
                            error: String::from(
                                "cannot checkpoint circuit because storage is not configured",
                            ),
                        })
                    };
                    let _ = reply_sender.send(result);
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
                Command::Checkpoint(_) => (),
            }
        }
    }

    /// Tries to seek `self.step_rw` (if any) to the initial step indicated in
    /// `self.step`.
    fn seek_to_initial_step(&mut self) -> Result<(), ControllerError> {
        if self.step > 0 {
            let (new_step_rw, prev_step_metadata) = self
                .step_rw
                .take()
                .unwrap()
                .into_reader()
                .unwrap()
                .seek(self.step - 1)?;
            self.step_rw = Some(StepRw::Reader(new_step_rw));
            for (endpoint_name, metadata) in prev_step_metadata.input_endpoints {
                let endpoint_id = self.controller.input_endpoint_id_by_name(&endpoint_name)?;
                self.controller.inputs.lock().unwrap()[&endpoint_id]
                    .reader
                    .seek(metadata);
            }
        }
        Ok(())
    }

    /// Tries to start replaying the current step `self.step` by reading its
    /// metadata from `self.step_rw` and passing it to all of the input
    /// endpoints.  Return true if replaying was initiated or false if we've
    /// completed all the replaying for this pipeline.
    fn replay_step(&mut self) -> Result<bool, ControllerError> {
        // Read a step.
        let Some(rw) = self.step_rw.take() else {
            // There's no step reader/writer, so we're not replaying.
            return Ok(false);
        };
        let (metadata, rw) = rw.read()?;
        self.step_rw = Some(rw);

        let Some(metadata) = metadata else {
            // No more steps to replay.
            return Ok(false);
        };

        // There's a step to replay.
        if metadata.step != self.step {
            return Err(ControllerError::UnexpectedStep {
                actual: metadata.step,
                expected: self.step,
            });
        }
        info!("replaying input step {}", self.step);
        for (endpoint_name, metadata) in metadata.input_endpoints {
            let endpoint_id = self.controller.input_endpoint_id_by_name(&endpoint_name)?;
            self.controller.inputs.lock().unwrap()[&endpoint_id]
                .reader
                .replay(metadata);
            *self.controller.status.inputs.read().unwrap()[&endpoint_id]
                .progress
                .lock()
                .unwrap() = StepProgress::Started;
        }
        Ok(true)
    }

    /// Requests all of the input adapters to flush their input to the circuit,
    /// and waits for them to finish doing it.
    ///
    /// Returns the total number of records consumed, a vector of notifications
    /// to send when the records have been processed, and the corresponding
    /// steps log entries.
    fn flush_input_to_circuit(&mut self, replaying: bool) -> Result<FlushedInput, ()> {
        loop {
            // Collect inputs that need [InputReader::queue] to be called. Then,
            // separately, call it on each of them. We don't do it in a single
            // step because that causes a deadlock due to nesting locks.
            //
            // We don't just start all of the inputs in a single pass because
            // inputs can be added or removed (particularly HTTP inputs) when we
            // drop the lock.
            let mut all_complete = true;
            let mut need_start = Vec::new();
            for (endpoint_id, status) in self.controller.status.input_status().iter() {
                let mut progress = status.progress.lock().unwrap();
                match *progress {
                    StepProgress::NotStarted => {
                        assert!(!replaying);
                        need_start.push(*endpoint_id);
                        all_complete = false;
                        *progress = StepProgress::Started;
                    }
                    StepProgress::Started => all_complete = false,
                    StepProgress::Complete { .. } => (),
                }
            }
            if !need_start.is_empty() {
                let inputs = self.controller.inputs.lock().unwrap();
                for endpoint_id in need_start {
                    if let Some(descr) = inputs.get(&endpoint_id) {
                        descr.reader.queue();
                    }
                }
            }
            if all_complete {
                let mut total_consumed = 0;
                let mut step_metadata = HashMap::new();
                for (id, status) in self.controller.status.input_status().iter() {
                    match mem::replace(
                        &mut *status.progress.lock().unwrap(),
                        StepProgress::NotStarted,
                    ) {
                        StepProgress::Complete {
                            num_records,
                            metadata,
                        } => {
                            total_consumed += num_records;
                            if let Some(metadata) = metadata {
                                step_metadata.insert(status.endpoint_name.clone(), metadata);
                            }
                        }
                        StepProgress::NotStarted => {
                            info!("race adding endpoint {id}");
                        }
                        StepProgress::Started => unreachable!(),
                    }
                }
                let input_queue = mem::take(&mut *self.controller.input_queue.lock().unwrap());
                let notifications = input_queue
                    .into_iter()
                    .map(|(mut buffer, notification)| {
                        total_consumed += buffer.flush_all() as u64;
                        notification
                    })
                    .collect::<Vec<_>>();

                return Ok(FlushedInput {
                    total_consumed,
                    notifications,
                    step_metadata,
                });
            }

            self.parker.park();
            if self.controller.state() == PipelineState::Terminated {
                return Err(());
            }
        }
    }

    /// Reports that `total_consumed` records have been consumed and calls all
    /// of the `notifications` to let them know that they're done.
    ///
    /// Returns the total number of records processed by the pipeline *before*
    /// this step.
    fn processed_records(
        &mut self,
        total_consumed: u64,
        notifications: Vec<OneshotSender<()>>,
    ) -> u64 {
        let processed_records = self
            .controller
            .status
            .global_metrics
            .processed_records(total_consumed);
        for notification in notifications {
            let _ = notification.send(());
        }
        processed_records
    }

    /// Writes `step_metadata` to the step writer.
    fn write_step(
        &mut self,
        step_metadata: HashMap<String, RmpValue>,
    ) -> Result<(), ControllerError> {
        if let Some(step_writer) = self.step_rw.as_mut().and_then(|rw| rw.as_writer()) {
            step_writer.write(&StepMetadata {
                step: self.step,
                input_endpoints: step_metadata,
            })?;
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

    /// Executes a step in the circuit.
    fn step(&mut self) {
        debug!("circuit thread: calling 'circuit.step'");
        self.circuit
            .step()
            .unwrap_or_else(|e| self.controller.error(e.into()));
        debug!("circuit thread: 'circuit.step' returned");
    }
}

struct FlushedInput {
    /// Number of records consumed by the circuit.
    total_consumed: u64,

    /// Notifications to emit when the records have been processed.
    notifications: Vec<OneshotSender<()>>,

    /// Metadata to write to the steps log.
    step_metadata: HashMap<String, RmpValue>,
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
}

/// When the next step will definitely trigger (if not before).
enum Wait {
    /// At time `.0`.
    Time(Instant),

    /// No step is scheduled.
    Forever,
}

impl Wait {
    // Park until the next step will trigger (or until we're unparked).
    fn park(self, parker: &Parker) {
        match self {
            Wait::Time(t) => parker.park_deadline(t),
            Wait::Forever => parker.park(),
        }
    }
}

impl StepTrigger {
    /// Returns a new [StepTrigger].
    fn new(controller: Arc<ControllerInner>) -> Self {
        let config = &controller.status.pipeline_config;
        let max_buffering_delay = Duration::from_micros(config.global.max_buffering_delay_usecs);
        let min_batch_size_records = config.global.min_batch_size_records;
        let clock_resolution = config
            .global
            .clock_resolution_usecs
            .map(Duration::from_micros);
        Self {
            controller,
            tick: clock_resolution.map(|delay| Instant::now() + delay),
            buffer_timeout: None,
            max_buffering_delay,
            min_batch_size_records,
            clock_resolution,
        }
    }

    /// Determines when to trigger the next step, given whether we're currently
    /// `replaying` and whether the pipeline is currently `running`.  Returns
    /// `None` to trigger a step right away, and otherwise how long to wait.
    fn trigger(&mut self, replaying: bool, running: bool) -> Option<Wait> {
        let buffered_records = self.controller.status.num_buffered_input_records();

        // `self.tick` but `None` if we're not running.
        let tick = running.then_some(self.tick).flatten();

        if replaying
            || self.controller.status.unset_step_requested()
            || buffered_records > self.min_batch_size_records
            || tick.map_or(false, |t| Instant::now() >= t)
            || self.buffer_timeout.map_or(false, |t| Instant::now() >= t)
        {
            self.tick = self.clock_resolution.map(|delay| Instant::now() + delay);
            self.buffer_timeout = None;
            None
        } else {
            if buffered_records > 0 && self.buffer_timeout.is_none() {
                self.buffer_timeout = Some(Instant::now() + self.max_buffering_delay);
            }
            match (tick, self.buffer_timeout) {
                (Some(t1), Some(t2)) => Some(Wait::Time(min(t1, t2))),
                (Some(t), None) | (None, Some(t)) => Some(Wait::Time(t)),
                (None, None) => Some(Wait::Forever),
            }
        }
    }
}

/// Controller initialization.
///
/// When we start a controller, we do one of:
///
/// - Start from an existing checkpoint.
///
/// - Start a new pipeline while a new initial checkpoint.
///
/// - Start a new pipeline without any checkpoint support.
///
/// This structure handles all these cases.
struct ControllerInit {
    /// The first step that the circuit will execute.
    step: Step,

    /// The pipeline configuration.
    ///
    /// This will differ from the one passed into [ControllerInit::new] if a
    /// checkpoint is read, because a checkpoint includes the pipeline
    /// configuration.
    pipeline_config: PipelineConfig,

    /// The circuit configuration.
    circuit_config: CircuitConfig,

    /// The path to the `state.json` checkpoint file, if fault tolerance is
    /// enabled.
    state_path: Option<PathBuf>,

    /// The step reader/writer, if fault tolerance is enabled.
    step_rw: Option<StepRw>,
}

impl ControllerInit {
    fn new(config: PipelineConfig) -> Result<Self, ControllerError> {
        let Some(path) = config.storage_config.as_ref().map(|storage| storage.path()) else {
            info!("disabling fault tolerance because storage is not enabled");
            return Self::without_checkpoint(config);
        };

        fn startup_io_error(error: IoError) -> ControllerError {
            ControllerError::io_error(String::from("controller startup"), error)
        }
        let state_path = path.join("state.json");
        let steps_path = path.join("steps.bin");
        if fs::exists(&state_path).map_err(startup_io_error)? {
            // Open the existing checkpoint.
            info!(
                "{}: initializing fault tolerance from saved state",
                state_path.display()
            );
            let checkpoint = Checkpoint::read(&state_path)?;

            // There might be a steps file already (there must be, if
            // we're not at step 0). Open it, if so; otherwise, create a
            // new one.
            let step_rw =
                if fs::exists(&steps_path).map_err(startup_io_error)? || checkpoint.step > 0 {
                    info!(
                        "{}: opening to start from step {}",
                        steps_path.display(),
                        checkpoint.step
                    );
                    StepRw::open(&steps_path)?
                } else {
                    info!("{}: creating", steps_path.display());
                    StepRw::create(&steps_path)?
                };
            Self::with_checkpoint(checkpoint, state_path, step_rw)
        } else if config.inputs.values().all(|config| {
            input_transport_config_is_fault_tolerant(&config.connector_config.transport)
        }) {
            // We don't have an existing checkpoint but this pipeline
            // can be fault tolerant, so start one and create a new
            // steps file for it.
            info!(
                "{}: creating new fault tolerant pipeline",
                state_path.display()
            );
            let checkpoint = Checkpoint {
                circuit: CheckpointMetadata::default(),
                step: 0,
                config: config.clone(),
            };
            fs::create_dir_all(path).map_err(startup_io_error)?;
            checkpoint.write(&state_path)?;

            info!("{}: creating", steps_path.display());
            let step_rw = StepRw::create(&steps_path)?;
            Self::with_checkpoint(checkpoint, state_path, step_rw)
        } else {
            // We don't have an existing check point and this pipeline
            // cannot be fault-tolerant.
            info!("{}: disabling fault tolerance (saved state file does not exist and circuit has at least one non-fault-tolerant input)", path.display());
            Self::without_checkpoint(config)
        }
    }
    fn without_checkpoint(pipeline_config: PipelineConfig) -> Result<Self, ControllerError> {
        let circuit_config =
            Self::circuit_config(&pipeline_config, CheckpointMetadata::default().uuid)?;
        Ok(Self {
            step: 0,
            pipeline_config,
            circuit_config,
            state_path: None,
            step_rw: None,
        })
    }
    fn with_checkpoint(
        checkpoint: Checkpoint,
        state_path: PathBuf,
        step_rw: StepRw,
    ) -> Result<Self, ControllerError> {
        let circuit_config = Self::circuit_config(&checkpoint.config, checkpoint.circuit.uuid)?;
        Ok(Self {
            step: checkpoint.step,
            pipeline_config: checkpoint.config,
            circuit_config,
            state_path: Some(state_path),
            step_rw: Some(step_rw),
        })
    }
    fn circuit_config(
        pipeline_config: &PipelineConfig,
        init_checkpoint: Uuid,
    ) -> Result<CircuitConfig, ControllerError> {
        Ok(CircuitConfig {
            layout: Layout::new_solo(pipeline_config.global.workers as usize),
            // Put the circuit's checkpoints in a `circuit` subdirectory of the
            // storage directory.
            storage: pipeline_config
                .storage_config
                .as_ref()
                .map(|storage| {
                    let path = storage.path().join("circuit");
                    if !fs::exists(&path)? {
                        fs::create_dir(&path)?;
                    }
                    Ok(StorageConfig {
                        path: {
                            // This `unwrap` should be OK because `path` came
                            // from a `String` anyway.
                            path.into_os_string().into_string().unwrap()
                        },
                        cache: storage.cache,
                    })
                })
                .transpose()
                .map_err(|error| {
                    ControllerError::io_error(
                        String::from("Failed to create checkpoint storage directory"),
                        error,
                    )
                })?,
            min_storage_bytes: if pipeline_config.global.storage {
                // This reduces the files stored on disk to a reasonable number.
                pipeline_config
                    .global
                    .min_storage_bytes
                    .unwrap_or(1024 * 1024)
            } else {
                usize::MAX
            },
            init_checkpoint,
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

            for (epid, ep) in controller.inputs.lock().unwrap().iter() {
                let should_run = globally_running
                    && !controller.status.input_endpoint_paused_by_user(epid)
                    && !controller.status.input_endpoint_full(epid);
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

/// State tracked by the controller for each input endpoint.
struct InputEndpointDescr {
    endpoint_name: String,
    reader: Box<dyn InputReader>,
}

impl InputEndpointDescr {
    pub fn new(endpoint_name: &str, reader: Box<dyn InputReader>) -> Self {
        Self {
            endpoint_name: endpoint_name.to_owned(),
            reader,
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

    /// Whether the output endpoint can discard duplicate output.
    is_fault_tolerant: bool,
}

impl OutputEndpointDescr {
    pub fn new(
        endpoint_name: &str,
        stream_name: &str,
        unparker: Unparker,
        is_fault_tolerant: bool,
    ) -> Self {
        Self {
            endpoint_name: endpoint_name.to_string(),
            stream_name: canonical_identifier(stream_name),
            queue: Arc::new(SegQueue::new()),
            disconnect_flag: Arc::new(AtomicBool::new(false)),
            unparker,
            is_fault_tolerant,
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
    inputs: Mutex<BTreeMap<EndpointId, InputEndpointDescr>>,
    next_input_id: Atomic<EndpointId>,
    outputs: ShardedLock<OutputEndpoints>,
    next_output_id: Atomic<EndpointId>,
    circuit_thread_unparker: Unparker,
    backpressure_thread_unparker: Unparker,
    error_cb: Box<dyn Fn(ControllerError) + Send + Sync>,
    metrics_snapshotter: Arc<Snapshotter>,
    prometheus_handle: PrometheusHandle,
    session_ctxt: SessionContext,
    #[allow(clippy::type_complexity)]
    input_queue: Mutex<Vec<(Box<dyn InputBuffer>, OneshotSender<()>)>>,
}

impl ControllerInner {
    fn new(
        config: PipelineConfig,
        catalog: Box<dyn CircuitCatalog>,
        error_cb: Box<dyn Fn(ControllerError) + Send + Sync>,
    ) -> Result<(Parker, BackpressureThread, Receiver<Command>, Arc<Self>), ControllerError> {
        let pipeline_name = config
            .name
            .as_ref()
            .map_or_else(|| "unnamed".to_string(), |n| n.clone());
        let status = Arc::new(ControllerStatus::new(config));
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
            inputs: Mutex::new(BTreeMap::new()),
            next_input_id: Atomic::new(0),
            outputs: ShardedLock::new(OutputEndpoints::new()),
            next_output_id: Atomic::new(0),
            circuit_thread_unparker: circuit_thread_parker.unparker().clone(),
            backpressure_thread_unparker: backpressure_thread_parker.unparker().clone(),
            error_cb,
            metrics_snapshotter,
            prometheus_handle,
            session_ctxt: SessionContext::new(),
            input_queue: Mutex::new(Vec::new()),
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
        let inputs = self.inputs.lock().unwrap();

        for (endpoint_id, descr) in inputs.iter() {
            if descr.endpoint_name == endpoint_name {
                return Ok(*endpoint_id);
            }
        }

        Err(ControllerError::unknown_input_endpoint(endpoint_name))
    }

    fn initialize_adhoc_queries(self: &Arc<Self>) {
        // Sync feldera catalog with datafusion catalog
        for (name, clh) in self.catalog.output_iter() {
            let arrow_fields = relation_to_arrow_fields(&clh.schema.fields, false);
            let input_handle = self
                .catalog
                .input_collection_handle(name)
                .map(|ich| ich.handle.fork());

            let adhoc_tbl = Arc::new(AdHocTable::new(
                clh.integrate_handle.is_some(),
                Arc::downgrade(self),
                input_handle,
                clh.schema.name.clone(),
                Arc::new(Schema::new(arrow_fields)),
                self.trace_snapshot.clone(),
            ));

            // This should never fail (we're not registering the same table twice).
            let r = self
                .session_ctxt
                .register_table(clh.schema.name.sql_name(), adhoc_tbl)
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
        )
        .map_err(|e| ControllerError::input_transport_error(endpoint_name, true, e))?;

        // If `endpoint` is `None`, it means that the endpoint config specifies an integrated
        // input connector.  Such endpoints are instantiated inside `add_input_endpoint`.
        self.add_input_endpoint(endpoint_name, endpoint_config.clone(), endpoint)
    }

    fn disconnect_input(self: &Arc<Self>, endpoint_id: &EndpointId) {
        let mut inputs = self.inputs.lock().unwrap();

        if let Some(ep) = inputs.remove(endpoint_id) {
            ep.reader.disconnect();
            self.status.remove_input(endpoint_id);
            self.unpark_circuit();
            self.unpark_backpressure();
        }
    }

    fn add_input_endpoint(
        self: &Arc<Self>,
        endpoint_name: &str,
        endpoint_config: InputEndpointConfig,
        endpoint: Option<Box<dyn TransportInputEndpoint>>,
    ) -> Result<EndpointId, ControllerError> {
        let mut inputs = self.inputs.lock().unwrap();
        let paused = endpoint_config.connector_config.paused;

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
                        })
                        .unwrap(),
                    }
                };

                let format =
                    <dyn InputFormat>::get_format(&format_config.name).ok_or_else(|| {
                        ControllerError::unknown_input_format(endpoint_name, &format_config.name)
                    })?;

                let parser =
                    format.new_parser(endpoint_name, input_handle, &format_config.config)?;
                // Initialize endpoint stats.
                self.status.add_input(
                    &endpoint_id,
                    endpoint_name,
                    endpoint_config,
                    endpoint.is_fault_tolerant(),
                );

                endpoint
                    .open(probe, parser, input_handle.schema.clone())
                    .map_err(|e| ControllerError::input_transport_error(endpoint_name, true, e))?
            }
            None => {
                let endpoint =
                    create_integrated_input_endpoint(endpoint_name, &endpoint_config, probe)?;

                // Initialize endpoint stats.
                self.status.add_input(
                    &endpoint_id,
                    endpoint_name,
                    endpoint_config,
                    endpoint.is_fault_tolerant(),
                );

                endpoint
                    .open(input_handle)
                    .map_err(|e| ControllerError::input_transport_error(endpoint_name, true, e))?
            }
        };

        if self.state() == PipelineState::Running && !paused {
            reader.extend();
        }

        inputs.insert(endpoint_id, InputEndpointDescr::new(endpoint_name, reader));

        drop(inputs);

        self.unpark_backpressure();
        Ok(endpoint_id)
    }

    fn has_fault_tolerant_inputs(&self) -> bool {
        self.status
            .input_status()
            .values()
            .all(|status| status.is_fault_tolerant)
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
        let handles = self
            .catalog
            .output_handles(&SqlIdentifier::from(&endpoint_config.stream))
            .ok_or_else(|| {
                ControllerError::unknown_output_stream(endpoint_name, &endpoint_config.stream)
            })?;

        let endpoint_id = self.next_output_id.fetch_add(1, Ordering::AcqRel);
        let endpoint_name_str = endpoint_name.to_string();

        let self_weak = Arc::downgrade(self);

        let is_fault_tolerant;

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
            is_fault_tolerant = endpoint.is_fault_tolerant();

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

            let format = <dyn OutputFormat>::get_format(&format_config.name).ok_or_else(|| {
                ControllerError::unknown_output_format(endpoint_name, &format_config.name)
            })?;
            format.new_encoder(
                endpoint_name,
                &endpoint_config.connector_config,
                &handles.schema,
                probe,
            )?
        } else {
            // `endpoint` is `None` - instantiate an integrated endpoint.
            let endpoint = create_integrated_output_endpoint(
                endpoint_id,
                endpoint_name,
                endpoint_config,
                &handles.schema,
                self_weak,
            )?;

            is_fault_tolerant = endpoint.is_fault_tolerant();

            endpoint.into_encoder()
        };

        let parker = Parker::new();
        let endpoint_descr = OutputEndpointDescr::new(
            endpoint_name,
            &endpoint_config.stream,
            parker.unparker().clone(),
            is_fault_tolerant,
        );
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

    fn has_fault_tolerant_outputs(&self) -> bool {
        self.outputs
            .read()
            .unwrap()
            .by_id
            .values()
            .all(|descr| descr.is_fault_tolerant)
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
        let Ok(mut inputs) = self.inputs.lock() else {
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

    /// Adds `buffer` to be processed by the circuit in the next step. When
    /// processing is complete, `completion` will be notified.
    ///
    /// Ordinarily, an input adapter should deliver buffers to the circuit when
    /// the controller asks it to with [InputReader::queue].  This method allows
    /// code outside an input adapter to queue buffers to the circuit.
    pub fn queue_buffer(&self, buffer: Box<dyn InputBuffer>, completion: OneshotSender<()>) {
        let num_records = buffer.len();

        // We intentionally keep the lock across the call to
        // `input_batch_global` to avoid the race described on
        // [InputConsumer::queued].
        let mut input_queue = self.input_queue.lock().unwrap();
        input_queue.push((buffer, completion));
        self.status
            .input_batch_global(num_records, &self.circuit_thread_unparker);
    }

    /// Update counters after receiving an end-of-input event on an input
    /// endpoint.
    ///
    /// See [`ControllerStatus::eoi`].
    pub fn eoi(&self, endpoint_id: EndpointId) {
        self.status.eoi(endpoint_id, &self.circuit_thread_unparker)
    }

    fn output_buffers_full(&self) -> bool {
        self.status.output_buffers_full()
    }

    fn checkpoint(&self) -> Result<Checkpoint, ControllerError> {
        let (sender, receiver) = channel();
        self.command_sender
            .send(Command::Checkpoint(sender))
            .map_err(|_| ControllerError::ControllerExit)?;
        self.unpark_circuit();
        receiver
            .recv()
            .map_err(|_| ControllerError::ControllerExit)?
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

impl InputConsumer for InputProbe {
    fn max_batch_size(&self) -> usize {
        self.max_batch_size
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

    fn replayed(&self, num_records: usize) {
        self.controller
            .status
            .completed(self.endpoint_id, num_records as u64, None);
        self.controller.unpark_circuit();
    }

    fn extended(&self, num_records: usize, metadata: RmpValue) {
        self.controller
            .status
            .completed(self.endpoint_id, num_records as u64, Some(metadata));
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

    fn push_key(&mut self, key: &[u8], val: Option<&[u8]>, num_records: usize) {
        let num_bytes = key.len() + val.map(|v| v.len()).unwrap_or_default();

        match self.endpoint.push_key(key, val) {
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
        fs::{remove_file, File},
        thread::sleep,
        time::Duration,
    };
    use tempfile::{NamedTempFile, TempDir};

    use proptest::prelude::*;

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

            println!("input file: {}", temp_input_file.path().to_str().unwrap());
            println!("output file: {output_path}");
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
