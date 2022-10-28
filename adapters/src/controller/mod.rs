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
    Catalog, InputConsumer, InputEndpoint, InputFormat, InputTransport, Parser, PipelineState,
};
use anyhow::{Error as AnyError, Result as AnyResult};
use crossbeam::sync::{Parker, ShardedLock, Unparker};
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
    InputEndpointConfig,
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
    circuit_thread_unparker: Unparker,

    /// The backpressure thread handle (see module-level docs).
    backpressure_thread_handle: JoinHandle<()>,
    backpressure_thread_unparker: Unparker,
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
    ///   error handling policy, but simply forwards most errors up the stack.
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
        let inner = Arc::new(ControllerInner::new(catalog, &config.global, error_cb));

        let backpressure_thread_parker = Parker::new();
        let backpressure_thread_unparker = backpressure_thread_parker.unparker().clone();

        let backpressure_thread_handle = {
            let inner = inner.clone();
            spawn(move || Self::backpressure_thread(inner, backpressure_thread_parker))
        };

        let circuit_thread_parker = Parker::new();
        let circuit_thread_unparker = circuit_thread_parker.unparker().clone();

        let circuit_thread_handle = {
            let inner = inner.clone();
            let backpressure_thread_unparker = backpressure_thread_unparker.clone();
            spawn(move || {
                Self::circuit_thread(
                    circuit,
                    inner,
                    circuit_thread_parker,
                    backpressure_thread_unparker,
                )
            })
        };

        for (input_name, input_config) in config.inputs.iter() {
            inner.connect_input(
                input_name,
                input_config,
                circuit_thread_unparker.clone(),
                backpressure_thread_unparker.clone(),
            )?;
        }

        Ok(Self {
            inner,
            circuit_thread_handle,
            circuit_thread_unparker,
            backpressure_thread_handle,
            backpressure_thread_unparker,
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
        self.inner.connect_input(
            endpoint_name,
            config,
            self.circuit_thread_unparker.clone(),
            self.backpressure_thread_unparker.clone(),
        )?;
        self.unpark_backpressure();
        Ok(())
    }

    /// Change the state of all input endpoints to running.
    ///
    /// Start streaming data through all connected input endpoints.
    pub fn start(&self) {
        self.inner.start();
        self.unpark_backpressure();
    }

    /// Pause all input endpoints.
    ///
    /// Sends a pause command to all input endpoints.  Upon receiving the
    /// command, the endpoints must stop pushing data to the pipeline.  This
    /// method is asynchronous and may return before all endpoints have been
    /// fully paused.
    pub fn pause(&self) {
        self.inner.pause();
        self.unpark_backpressure();
    }

    /// Terminate the controller, stop all input endpoints and destroy the
    /// circuit.
    pub fn stop(self) -> AnyResult<()> {
        self.inner.stop();
        self.unpark_circuit();
        self.unpark_backpressure();
        self.circuit_thread_handle
            .join()
            .map_err(|_| AnyError::msg("circuit thread panicked"))??;
        self.backpressure_thread_handle
            .join()
            .map_err(|_| AnyError::msg("backpressure thread panicked"))?;
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

    /// Circuit thread function.
    fn circuit_thread(
        mut circuit: DBSPHandle,
        controller: Arc<ControllerInner>,
        parker: Parker,
        backpressure_thread_unparker: Unparker,
    ) -> AnyResult<()> {
        let mut start: Option<Instant> = None;

        let config = controller.config.read().unwrap();
        let max_buffering_delay = Duration::from_micros(config.global.max_buffering_delay_usecs);
        let min_batch_size_records = config.global.min_batch_size_records;
        drop(config);

        loop {
            match PipelineState::from_u32(controller.state.load(Ordering::Acquire)) {
                Some(PipelineState::Running) | Some(PipelineState::Paused) => {
                    let buffered_records = controller.stats.num_buffered_records();

                    // We have sufficient buffered inputs or the buffering delay has expired --
                    // kick the circuit to consume buffered data.
                    if buffered_records >= min_batch_size_records
                        || start
                            .map(|start| start.elapsed() >= max_buffering_delay)
                            .unwrap_or(false)
                    {
                        start = None;
                        // Reset all counters of buffered records and bytes to 0.
                        controller.stats.consume_buffered();
                        // Wake up the backpressure thread to unpause endpoints blocked due to
                        // backpressure.
                        backpressure_thread_unparker.unpark();
                        circuit
                            .step()
                            .unwrap_or_else(|e| controller.error(ControllerError::dbsp_error(e)));
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
                Some(PipelineState::Terminated) => {
                    circuit
                        .kill()
                        .map_err(|_| AnyError::msg("dbsp thead panicked"))?;
                    return Ok(());
                }
                _ => unreachable!(),
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

            match PipelineState::from_u32(controller.state.load(Ordering::Acquire)) {
                Some(PipelineState::Paused) => {
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
                Some(PipelineState::Running) => {
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
                Some(PipelineState::Terminated) => return,
                _ => unreachable!(),
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
    error_cb: Box<dyn Fn(ControllerError) + Send + Sync>,
}

impl ControllerInner {
    fn new(
        catalog: Catalog,
        global_config: &GlobalControllerConfig,
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
            error_cb,
        }
    }

    fn connect_input(
        self: &Arc<Self>,
        endpoint_name: &str,
        endpoint_config: &InputEndpointConfig,
        circuit_thread_unparker: Unparker,
        backpressure_thread_unparker: Unparker,
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
        let endpoint_id = inputs.last_key_value().map(|(k, _)| k + 1).unwrap_or(0);
        let probe = Box::new(InputProbe::new(
            endpoint_id,
            endpoint_name,
            parser,
            self.clone(),
            circuit_thread_unparker,
            backpressure_thread_unparker,
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

        Ok(())
    }

    fn start(self: &Arc<Self>) {
        self.state
            .store(PipelineState::Running as u32, Ordering::Release);
    }

    fn pause(self: &Arc<Self>) {
        self.state
            .store(PipelineState::Paused as u32, Ordering::Release);
    }

    fn stop(self: &Arc<Self>) {
        let mut inputs = self.inputs.lock().unwrap();

        for ep in inputs.values() {
            ep.endpoint.disconnect();
        }
        inputs.clear();

        self.state
            .store(PipelineState::Terminated as u32, Ordering::Release);
    }

    fn error(&self, error: ControllerError) {
        (self.error_cb)(error);
    }
}

// An input probe inserted between the transport endpoint and the parser to
// track stats and errors.
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
                // Error: propagate the error up the stack.
                self.parser.clear();
                self.controller
                    .error(ControllerError::parse_error(&self.endpoint_name, error));
            }
        }
    }

    fn eof(&mut self) {
        // The endpoint reached end-of-file.  Notify and flush the parser (even though
        // no new data has been received, the parser may contain some partially
        // parsed data and may be waiting for, e.g., and end-of-line or
        // end-of-file to finish parsing it).
        match self.parser.eof() {
            Ok(num_records) => {
                self.parser.flush();
                self.controller.stats.eof(
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

#[cfg(test)]
mod test {
    use crate::{Catalog, Controller, ControllerConfig};
    use bincode::{Decode, Encode};
    use dbsp::{algebra::F32, DBSPHandle, Runtime};
    use serde::{Deserialize, Serialize};
    use serde_yaml;
    use size_of::SizeOf;
    use tempfile::NamedTempFile;

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
    )]
    struct TestStruct {
        b: bool,
        f: F32,
        s: Option<String>,
    }

    /*impl TestStruct {
        fn new(b: bool, f: F32, s: Option<String>) -> Self {
            Self { b, f, s }
        }
    }*/

    fn test_circuit(workers: usize) -> (DBSPHandle, Catalog) {
        let (circuit, (input, _output)) = Runtime::init_circuit(workers, |circuit| {
            let (input, hinput) = circuit.add_input_zset::<TestStruct, i32>();

            let houtput = input.integrate().output();
            (hinput, houtput)
        })
        .unwrap();

        let mut catalog = Catalog::new();
        catalog.register_input_zset("test_input1", input);

        (circuit, catalog)
    }

    #[test]
    fn csv_file_test() {
        let (circuit, catalog) = test_circuit(4);

        let temp_file = NamedTempFile::new().unwrap();

        let config_str = format!(
            r#"
inputs:
    test_input1:
        transport:
            name: file
            config:
                path: {:?}
                buffer_size: 5
                follow: true
        format:
            name: csv
            config:
                input_stream: test_input1
        "#,
            temp_file.path().to_str().unwrap()
        );

        let config: ControllerConfig = serde_yaml::from_str(&config_str).unwrap();

        let controller = Controller::with_config(
            circuit,
            catalog,
            &config,
            Box::new(|e| panic!("error: {e}")),
        )
        .unwrap();

        // TODO: feed some data, make sure it appears in the output stream.

        controller.stop().unwrap();
    }
}
