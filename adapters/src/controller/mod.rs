//! An I/O controller that coordinates the creation, reconfiguration,
//! teardown of input/output adapters, and implements runtime flow control.
//!
//! # Design
//!
//! The controller logic is split into two tasks that run in separate threads.
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
//! the controller injects `InputProbe`s between each input endpoint and format
//! parser:
//!
//! ```text
//!                ┌────────────┐
//!                │ controller │
//!                └────────────┘
//!                      ▲
//!                      │stats
//!   ┌────────┐   ┌─────┴────┐   ┌──────┐   ┌──────┐
//!   │endpoint├──►│InputProbe├──►│parser├──►│handle│
//!   └────────┘   └──────────┘   └──────┘   └──────┘
//! ```
//!
//! The probe passes the data through to the parser, while counting the number
//! of transmitted bytes and records and updating respective performance
//! counters in the controller.

use crate::{
    Catalog, Encoder, InputConsumer, InputEndpoint, InputFormat, InputTransport, OutputConsumer,
    OutputEndpoint, OutputFormat, OutputTransport, Parser, PipelineState, SerBatch,
    SerOutputBatchHandle,
};
use anyhow::{Error as AnyError, Result as AnyResult};
use crossbeam::{
    queue::SegQueue,
    sync::{Parker, ShardedLock, Unparker},
};
use dbsp::DBSPHandle;
use num_traits::FromPrimitive;
use std::{
    collections::{BTreeMap, HashSet},
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc, Mutex,
    },
    thread::{spawn, JoinHandle},
    time::{Duration, Instant},
};

mod config;
mod error;
mod stats;

pub use config::{
    ControllerConfig, ControllerInnerConfig, FormatConfig, GlobalControllerConfig,
    InputEndpointConfig, OutputEndpointConfig,
};
use error::ControllerError;
use stats::ControllerStats;

type EndpointId = u64;

/// Controller that coordinates the creation, reconfiguration, teardown of
/// input/output adapters, and implements runtime flow control.
///
/// The controller instantiates the input and output pipelines according to a
/// user-provided [configuration](`ControllerConfig`) and exposes an API to
/// reconfigure and monitor the pipelines at runtime.
pub struct Controller {
    inner: Arc<ControllerInner>,

    /// The circuit thread handle (see module-level docs).
    circuit_thread_handle: JoinHandle<AnyResult<()>>,

    /// The backpressure thread handle (see module-level docs).
    backpressure_thread_handle: JoinHandle<()>,
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
    pub fn with_config(
        circuit: DBSPHandle,
        catalog: Catalog,
        config: &ControllerConfig,
        error_cb: Box<dyn Fn(ControllerError) + Send + Sync>,
    ) -> AnyResult<Self> {
        let circuit_thread_parker = Parker::new();
        let circuit_thread_unparker = circuit_thread_parker.unparker().clone();

        let backpressure_thread_parker = Parker::new();
        let backpressure_thread_unparker = backpressure_thread_parker.unparker().clone();

        let inner = Arc::new(ControllerInner::new(
            catalog,
            &config.global,
            circuit_thread_unparker,
            backpressure_thread_unparker,
            error_cb,
        ));

        let backpressure_thread_handle = {
            let inner = inner.clone();
            spawn(move || Self::backpressure_thread(inner, backpressure_thread_parker))
        };

        let circuit_thread_handle = {
            let inner = inner.clone();
            spawn(move || Self::circuit_thread(circuit, inner, circuit_thread_parker))
        };

        for (input_name, input_config) in config.inputs.iter() {
            inner.connect_input(input_name, input_config)?;
        }

        for (output_name, output_config) in config.outputs.iter() {
            inner.connect_output(output_name, output_config)?;
        }

        Ok(Self {
            inner,
            circuit_thread_handle,
            backpressure_thread_handle,
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
    ) -> AnyResult<()> {
        self.inner.connect_input(endpoint_name, config)
    }

    /// Change the state of all input endpoints to running.
    ///
    /// Start streaming data through all connected input endpoints.
    pub fn start(&self) {
        self.inner.start();
    }

    /// Pause all input endpoints.
    ///
    /// Sends a pause command to all input endpoints.  Upon receiving the
    /// command, the endpoints must stop pushing data to the pipeline.  This
    /// method is asynchronous and may return before all endpoints have been
    /// fully paused.
    pub fn pause(&self) {
        self.inner.pause();
    }

    /// Returns controller stats.
    pub fn stats(&self) -> &ControllerStats {
        &self.inner.stats
    }

    /// Terminate the controller, stop all input endpoints and destroy the
    /// circuit.
    pub fn stop(self) -> AnyResult<()> {
        self.inner.stop();
        self.circuit_thread_handle
            .join()
            .map_err(|_| AnyError::msg("circuit thread panicked"))??;
        self.backpressure_thread_handle
            .join()
            .map_err(|_| AnyError::msg("backpressure thread panicked"))?;
        Ok(())
    }

    /// Circuit thread function: holds the handle to the circuit, calls `step`
    /// on it whenever input data is available, pushes output batches
    /// produced by the circuit to output pipelines.
    fn circuit_thread(
        mut circuit: DBSPHandle,
        controller: Arc<ControllerInner>,
        parker: Parker,
    ) -> AnyResult<()> {
        let mut start: Option<Instant> = None;

        let config = controller.config.read().unwrap();
        let max_buffering_delay = Duration::from_micros(config.global.max_buffering_delay_usecs);
        let min_batch_size_records = config.global.min_batch_size_records;
        drop(config);

        loop {
            match controller.state() {
                PipelineState::Running | PipelineState::Paused => {
                    // Backpressure in the output pipeline: wait for room in output buffers to
                    // become available.
                    if controller.output_buffers_full() {
                        parker.park();
                        continue;
                    }

                    let buffered_records = controller.stats.num_buffered_input_records();

                    // We have sufficient buffered inputs or the buffering delay has expired --
                    // kick the circuit to consume buffered data.  Use strict inequality in case
                    // `min_batch_size_records` is 0.
                    if buffered_records > min_batch_size_records
                        || start
                            .map(|start| start.elapsed() >= max_buffering_delay)
                            .unwrap_or(false)
                    {
                        start = None;
                        // Reset all counters of buffered records and bytes to 0.
                        controller.stats.consume_buffered_inputs();
                        // Wake up the backpressure thread to unpause endpoints blocked due to
                        // backpressure.
                        controller.unpark_backpressure();
                        circuit
                            .step()
                            .unwrap_or_else(|e| controller.error(ControllerError::dbsp_error(e)));

                        // Push output batches to output pipelines.
                        let outputs = controller.outputs.read().unwrap();
                        for (endpoint_id, output) in outputs.iter() {
                            // TODO: add an endpoint config option to consolidate output batches.
                            let batch = output.output_handle.take_from_all();
                            let num_records = batch.iter().map(|b| b.len()).sum();

                            // Increment stats first, so we don't end up with negative counts.
                            controller.stats.enqueue_batch(*endpoint_id, num_records);
                            output.queue.push(batch);

                            // Wake up the output thread.  We're not trying to be smart here and
                            // wake up the thread conditionally if it was previously idle, as I
                            // don't expect this to make any real difference.
                            output.unparker.unpark();
                        }
                    } else if buffered_records > 0 {
                        // We have some buffered data, but less than `min_batch_size_records` --
                        // wait up to `max_buffering_delay` for more data to
                        // arrive.
                        if start.is_none() {
                            start = Some(Instant::now());
                        }
                        parker.park_timeout(Duration::from_millis(1));
                    } else {
                        parker.park();
                    }
                }
                PipelineState::Terminated => {
                    circuit
                        .kill()
                        .map_err(|_| AnyError::msg("dbsp thead panicked"))?;
                    return Ok(());
                }
            }
        }
    }

    /// Backpressure thread function.
    fn backpressure_thread(controller: Arc<ControllerInner>, parker: Parker) {
        // `global_pause` flag is `true` when the entire controller is paused
        // (the controller starts in a paused state and switches between
        // paused and running states in responsed to `Controller::start()` and
        // `Controller::pause()` methods).
        let mut global_pause = true;

        // Endpoints paused due to backpressure.
        let mut paused_endpoints = HashSet::new();

        loop {
            let inputs = controller.inputs.lock().unwrap();

            match controller.state() {
                PipelineState::Paused => {
                    // Pause circuit if not yet paused.
                    if !global_pause {
                        for (epid, ep) in inputs.iter() {
                            // Pause the endpoint unless it's already paused due to backpressure.
                            if !paused_endpoints.contains(epid) {
                                ep.endpoint.pause().unwrap_or_else(|e| {
                                    controller.error(ControllerError::transport_error(
                                        &ep.endpoint_name,
                                        e,
                                    ))
                                });
                            }
                        }
                    }
                    global_pause = true;
                }
                PipelineState::Running => {
                    // Resume endpoints that have buffer space, pause endpoints with full buffers.
                    for (epid, ep) in inputs.iter() {
                        if controller
                            .stats
                            .input_endpoint_full(epid, &controller.config.read().unwrap())
                        {
                            // The endpoint is full and is not yet in the paused state -- pause it
                            // now.
                            if !global_pause && !paused_endpoints.contains(epid) {
                                ep.endpoint.pause().unwrap_or_else(|e| {
                                    controller.error(ControllerError::transport_error(
                                        &ep.endpoint_name,
                                        e,
                                    ))
                                });
                            }
                            paused_endpoints.insert(*epid);
                        } else {
                            // The endpoint is paused when it should be running -- unpause it.
                            if global_pause || paused_endpoints.contains(epid) {
                                ep.endpoint.start().unwrap_or_else(|e| {
                                    controller.error(ControllerError::transport_error(
                                        &ep.endpoint_name,
                                        e,
                                    ))
                                });
                            }
                            paused_endpoints.remove(epid);
                        }
                    }
                    global_pause = false;
                }
                PipelineState::Terminated => return,
            }

            drop(inputs);

            parker.park();
        }
    }
}

/// State tracked by the controller for each input endpoint.
struct InputEndpointState {
    endpoint_name: String,
    endpoint: Box<dyn InputEndpoint>,
}

impl InputEndpointState {
    pub fn new(endpoint_name: &str, endpoint: Box<dyn InputEndpoint>) -> Self {
        Self {
            endpoint_name: endpoint_name.to_owned(),
            endpoint,
        }
    }
}

/// A lock-free queue used to send output batches from the circuit thread
/// to output endpoint threads.
type BatchQueue = SegQueue<Vec<Box<dyn SerBatch>>>;

/// State tracked by the controller for each output endpoint.
struct OutputEndpointState {
    /// Endpoint name.
    endpoint_name: String,

    /// Output stream name from the circuit catalog.
    ///
    /// Note: we currently assume that each output endpoint is connected to
    /// exactly one output stream.  This can be generalized to a
    /// many-to-many relation, e.g., a single endpoint that implements a
    /// database connection can read from multiple output streams (one per DB
    /// table). Likewise, there's no reason the same output stream cannot be
    /// sent to multiple destinations. Like with the rest of this design, we
    /// keep things simple until we understand real use cases better.
    stream_name: String,

    /// Handle for the output stream.
    output_handle: Box<dyn SerOutputBatchHandle>,

    /// FIFO queue of batches read from the stream.
    queue: Arc<BatchQueue>,

    /// Unparker for the endpoint thread.
    unparker: Unparker,
}

impl OutputEndpointState {
    pub fn new(
        endpoint_name: &str,
        stream_name: &str,
        output_handle: Box<dyn SerOutputBatchHandle>,
        unparker: Unparker,
    ) -> Self {
        Self {
            endpoint_name: endpoint_name.to_string(),
            stream_name: stream_name.to_string(),
            output_handle,
            queue: Arc::new(SegQueue::new()),
            unparker,
        }
    }
}

/// Controller state sharable across threads.
///
/// A reference to this struct is held by each input probe and by both
/// controller threads.
struct ControllerInner {
    config: ShardedLock<ControllerInnerConfig>,
    stats: ControllerStats,
    state: AtomicU32,
    catalog: Arc<Mutex<Catalog>>,
    inputs: Mutex<BTreeMap<EndpointId, InputEndpointState>>,
    outputs: ShardedLock<BTreeMap<EndpointId, OutputEndpointState>>,
    circuit_thread_unparker: Unparker,
    backpressure_thread_unparker: Unparker,
    error_cb: Box<dyn Fn(ControllerError) + Send + Sync>,
}

impl ControllerInner {
    fn new(
        catalog: Catalog,
        global_config: &GlobalControllerConfig,
        circuit_thread_unparker: Unparker,
        backpressure_thread_unparker: Unparker,
        error_cb: Box<dyn Fn(ControllerError) + Send + Sync>,
    ) -> Self {
        let stats = ControllerStats::new();
        let state = AtomicU32::new(PipelineState::Paused as u32);
        let config = ShardedLock::new(ControllerInnerConfig::new(global_config.clone()));

        Self {
            config,
            stats,
            state,
            catalog: Arc::new(Mutex::new(catalog)),
            inputs: Mutex::new(BTreeMap::new()),
            outputs: ShardedLock::new(BTreeMap::new()),
            circuit_thread_unparker,
            backpressure_thread_unparker,
            error_cb,
        }
    }

    fn connect_input(
        self: &Arc<Self>,
        endpoint_name: &str,
        endpoint_config: &InputEndpointConfig,
    ) -> AnyResult<()> {
        let mut inputs = self.inputs.lock().unwrap();

        if inputs.values().any(|ep| ep.endpoint_name == endpoint_name) {
            Err(ControllerError::duplicate_input_endpoint(endpoint_name))?;
        }

        // Create input pipeline, consisting of a transport endpoint, controller
        // probe, and parser.
        //
        // ┌────────┐   ┌──────────┐   ┌──────┐
        // │endpoint├──►│InputProbe├──►│parser├──►
        // └────────┘   └──────────┘   └──────┘

        // Create parser.
        let format = <dyn InputFormat>::get_format(&endpoint_config.format.name)
            .ok_or_else(|| ControllerError::unknown_input_format(&endpoint_config.format.name))?;
        let parser = format.new_parser(&endpoint_config.format.config, &self.catalog)?;

        // Create probe.
        let endpoint_id = inputs.keys().rev().next().map(|k| k + 1).unwrap_or(0);
        let probe = Box::new(InputProbe::new(
            endpoint_id,
            endpoint_name,
            parser,
            self.clone(),
            self.circuit_thread_unparker.clone(),
            self.backpressure_thread_unparker.clone(),
        ));

        // Create transport endpoint.
        let transport = <dyn InputTransport>::get_transport(&endpoint_config.transport.name)
            .ok_or_else(|| {
                ControllerError::unknown_input_transport(&endpoint_config.transport.name)
            })?;

        let endpoint = transport.new_endpoint(&endpoint_config.transport.config, probe)?;

        inputs.insert(
            endpoint_id,
            InputEndpointState::new(endpoint_name, endpoint),
        );

        drop(inputs);

        // Record endpoint config in `self.config`.
        let mut config = self.config.write().unwrap();
        config.inputs.insert(endpoint_id, endpoint_config.clone());

        // Initialize endpoint stats.
        self.stats.add_input(&endpoint_id);

        self.unpark_backpressure();
        Ok(())
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
    ) -> AnyResult<()> {
        let mut outputs = self.outputs.write().unwrap();

        if outputs.values().any(|ep| ep.endpoint_name == endpoint_name) {
            Err(ControllerError::duplicate_output_endpoint(endpoint_name))?;
        }
        for ep in outputs.values() {
            if ep.stream_name == endpoint_config.stream {
                Err(ControllerError::duplicate_output_stream_consumer(
                    &ep.stream_name,
                    &ep.endpoint_name,
                    endpoint_name,
                ))?;
            }
        }

        // Create output pipeline, consisting of an encoder, output probe and
        // transport endpoint; run the pipeline in a separate thread.
        //
        // ┌───────┐   ┌───────────┐   ┌────────┐
        // │encoder├──►│OutputProbe├──►│endpoint├──►
        // └───────┘   └───────────┘   └────────┘

        // Lookup output handle in catalog.
        let collection_handle = self
            .catalog
            .lock()
            .unwrap()
            .output_batch_handle(&endpoint_config.stream)
            .ok_or_else(|| ControllerError::unknown_output_stream(&endpoint_config.stream))?
            .fork();

        // Create transport endpoint.
        let transport = <dyn OutputTransport>::get_transport(&endpoint_config.transport.name)
            .ok_or_else(|| {
                ControllerError::unknown_output_transport(&endpoint_config.transport.name)
            })?;

        let endpoint = transport.new_endpoint(&endpoint_config.transport.config)?;

        // Create probe.
        let endpoint_id = outputs.keys().rev().next().map(|k| k + 1).unwrap_or(0);
        let probe = Box::new(OutputProbe::new(
            endpoint_id,
            endpoint_name,
            endpoint,
            self.clone(),
        ));

        // Create encoder.
        let format = <dyn OutputFormat>::get_format(&endpoint_config.format.name)
            .ok_or_else(|| ControllerError::unknown_output_format(&endpoint_config.format.name))?;
        let encoder = format.new_encoder(&endpoint_config.format.config, probe)?;

        let parker = Parker::new();
        let endpoint_state = OutputEndpointState::new(
            endpoint_name,
            &endpoint_config.stream,
            collection_handle,
            parker.unparker().clone(),
        );
        let queue = endpoint_state.queue.clone();
        let controller = self.clone();

        outputs.insert(endpoint_id, endpoint_state);

        let endpoint_name_string = endpoint_name.to_string();
        // Thread to run the output pipeline.
        spawn(move || {
            Self::output_thread_func(
                endpoint_id,
                endpoint_name_string,
                encoder,
                parker,
                queue,
                controller,
            )
        });

        drop(outputs);

        // Record endpoint config in `self.config`.
        let mut config = self.config.write().unwrap();
        config.outputs.insert(endpoint_id, endpoint_config.clone());

        // Initialize endpoint stats.
        self.stats.add_output(&endpoint_id);

        Ok(())
    }

    fn output_thread_func(
        endpoint_id: EndpointId,
        endpoint_name: String,
        mut encoder: Box<dyn Encoder>,
        parker: Parker,
        queue: Arc<BatchQueue>,
        controller: Arc<ControllerInner>,
    ) {
        loop {
            if controller.state() == PipelineState::Terminated {
                return;
            }

            // Dequeue the next output batch and push it to the encoder.
            if let Some(data) = queue.pop() {
                let num_records = data.iter().map(|b| b.len()).sum();

                encoder.encode(data.as_slice()).unwrap_or_else(|e| {
                    controller.error(ControllerError::encoder_error(&endpoint_name, e))
                });

                // `num_records` output records have been transmitted --
                // update output stats, wake up the circuit thread if the
                // number of queued records drops below high water mark.
                controller.stats.output_batch(
                    endpoint_id,
                    num_records,
                    &controller.config.read().unwrap(),
                    &controller.circuit_thread_unparker,
                );
            } else {
                // Queue is empty -- wait for the circuit thread to wake us up when
                // more data is available.
                parker.park();
            }
        }
    }

    fn state(self: &Arc<Self>) -> PipelineState {
        PipelineState::from_u32(self.state.load(Ordering::Acquire)).unwrap()
    }

    fn start(self: &Arc<Self>) {
        self.state
            .store(PipelineState::Running as u32, Ordering::Release);
        self.unpark_backpressure();
    }

    fn pause(self: &Arc<Self>) {
        self.state
            .store(PipelineState::Paused as u32, Ordering::Release);
        self.unpark_backpressure();
    }

    fn stop(self: &Arc<Self>) {
        let mut inputs = self.inputs.lock().unwrap();

        for ep in inputs.values() {
            ep.endpoint.disconnect();
        }
        inputs.clear();

        self.state
            .store(PipelineState::Terminated as u32, Ordering::Release);

        self.unpark_circuit();
        self.unpark_backpressure();
    }

    fn error(&self, error: ControllerError) {
        (self.error_cb)(error);
    }

    fn output_buffers_full(&self) -> bool {
        self.stats.output_buffers_full(&self.config.read().unwrap())
    }
}

/// An input probe inserted between the transport endpoint and the parser to
/// track stats and errors.
struct InputProbe {
    endpoint_id: EndpointId,
    endpoint_name: String,
    parser: Box<dyn Parser>,
    controller: Arc<ControllerInner>,
    circuit_thread_unparker: Unparker,
    backpressure_thread_unparker: Unparker,
}

impl InputProbe {
    fn new(
        endpoint_id: EndpointId,
        endpoint_name: &str,
        parser: Box<dyn Parser>,
        controller: Arc<ControllerInner>,
        circuit_thread_unparker: Unparker,
        backpressure_thread_unparker: Unparker,
    ) -> Self {
        Self {
            endpoint_id,
            endpoint_name: endpoint_name.to_owned(),
            parser,
            controller,
            circuit_thread_unparker,
            backpressure_thread_unparker,
        }
    }
}

/// `InputConsumer` interface exposed to the transport endpoint.
impl InputConsumer for InputProbe {
    fn input(&mut self, data: &[u8]) {
        // println!("input consumer {} bytes", data.len());
        // Pass input buffer to the parser.
        match self.parser.input(data) {
            Ok(num_records) => {
                // Success: push data to the input handle, update stats.
                self.parser.flush();
                self.controller.stats.input_batch(
                    self.endpoint_id,
                    data.len(),
                    num_records,
                    &self.controller.config.read().unwrap(),
                    &self.circuit_thread_unparker,
                    &self.backpressure_thread_unparker,
                );
            }
            Err(error) => {
                self.parser.clear();
                self.controller
                    .error(ControllerError::parse_error(&self.endpoint_name, error));
            }
        }
    }

    fn eoi(&mut self) {
        // The endpoint reached end-of-file.  Notify and flush the parser (even though
        // no new data has been received, the parser may contain some partially
        // parsed data and may be waiting for, e.g., and end-of-line or
        // end-of-file to finish parsing it).
        match self.parser.eoi() {
            Ok(num_records) => {
                self.parser.flush();
                self.controller.stats.eoi(
                    self.endpoint_id,
                    num_records,
                    &self.controller.config.read().unwrap(),
                    &self.circuit_thread_unparker,
                );
            }
            Err(error) => {
                self.parser.clear();
                self.controller
                    .error(ControllerError::parse_error(&self.endpoint_name, error));
            }
        }
    }

    fn error(&mut self, error: AnyError) {
        self.controller
            .error(ControllerError::transport_error(&self.endpoint_name, error));
    }

    fn fork(&self) -> Box<dyn InputConsumer> {
        Box::new(Self::new(
            self.endpoint_id,
            &self.endpoint_name,
            self.parser.fork(),
            self.controller.clone(),
            self.circuit_thread_unparker.clone(),
            self.backpressure_thread_unparker.clone(),
        ))
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
    fn push_buffer(&mut self, buffer: &[u8]) {
        let num_bytes = buffer.len();

        match self.endpoint.push_buffer(buffer) {
            Ok(()) => {
                self.controller
                    .stats
                    .output_buffer(self.endpoint_id, num_bytes);
            }
            Err(error) => {
                self.controller
                    .error(ControllerError::transport_error(&self.endpoint_name, error));
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{test::wait, Catalog, Controller, ControllerConfig};
    use bincode::{Decode, Encode};
    use csv::{ReaderBuilder as CsvReaderBuilder, WriterBuilder as CsvWriterBuilder};
    use dbsp::{DBSPHandle, Runtime};
    use serde::{Deserialize, Serialize};
    use serde_yaml;
    use size_of::SizeOf;
    use std::fs::remove_file;
    use tempfile::NamedTempFile;

    use proptest::{collection, prelude::*};
    use proptest_derive::Arbitrary;

    #[derive(
        Debug,
        PartialEq,
        Eq,
        PartialOrd,
        Ord,
        Serialize,
        Deserialize,
        Clone,
        Hash,
        SizeOf,
        Encode,
        Decode,
        Arbitrary,
    )]
    struct TestStruct {
        id: u32,
        b: bool,
        i: Option<i64>,
        s: String,
    }

    fn test_data(size: usize) -> impl Strategy<Value = Vec<TestStruct>> {
        collection::vec(any::<TestStruct>(), 0..=size)
    }

    fn test_circuit(workers: usize) -> (DBSPHandle, Catalog) {
        let (circuit, (input, output)) = Runtime::init_circuit(workers, |circuit| {
            let (input, hinput) = circuit.add_input_zset::<TestStruct, i32>();

            let houtput = input.output();
            (hinput, houtput)
        })
        .unwrap();

        let mut catalog = Catalog::new();
        catalog.register_input_zset_handle("test_input1", input);
        catalog.register_output_batch_handle("test_output1", output);

        (circuit, catalog)
    }

    // TODO: Parameterize this with config string, so we can test different
    // input/output formats and transports when we support more than one.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(30))]
        #[test]
        fn proptest_csv_file(
            data in test_data(5000),
            min_batch_size_records in 1..100usize,
            max_buffering_delay_usecs in 1..2000usize,
            input_buffer_size_bytes in 1..1000usize,
            output_buffer_size_records in 1..100usize)
        {
            // Assign unique indexes to records to make sure that all records are unique,
            // all weights are eualt to 1, and so the number of output records is equal to
            // the number of input records.
            let data: Vec<TestStruct> = data.iter().enumerate().map(|(idx, val)| {
                let mut val = val.clone();
                val.id = idx as u32;
                val
            }).collect();

            let (circuit, catalog) = test_circuit(4);

            let temp_input_file = NamedTempFile::new().unwrap();
            let temp_output_path = NamedTempFile::new().unwrap().into_temp_path();
            let output_path = temp_output_path.to_str().unwrap().to_string();
            temp_output_path.close().unwrap();

            let config_str = format!(
                r#"
min_batch_size_records: {min_batch_size_records}
max_buffering_delay_usecs: {max_buffering_delay_usecs}
inputs:
    test_input1:
        transport:
            name: file
            config:
                path: {:?}
                buffer_size_bytes: {input_buffer_size_bytes}
                follow: false
        format:
            name: csv
            config:
                input_stream: test_input1
outputs:
    test_output1:
        stream: test_output1
        transport:
            name: file
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
            let config: ControllerConfig = serde_yaml::from_str(&config_str).unwrap();

            let controller = Controller::with_config(
                circuit,
                catalog,
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
            controller.start();

            // Wait for the pipeline to output all records.
            wait(|| {
                controller.stats().output_stats().get(&0).unwrap().transmitted_records() == data.len() as u64
            }, None);

            controller.stop().unwrap();

            let mut expected = data.clone();
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
}
