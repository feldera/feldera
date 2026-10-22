use crate::catalog::InputCollectionHandle;
use crate::format::{InputBuffer, Splitter, get_input_format};
use crate::{InputConsumer, ParseError, Parser, controller::FormatConfig};
use anyhow::{Error as AnyError, anyhow};
use dbsp::operator::StagedBuffers;
use feldera_adapterlib::ConnectorMetadata;
use feldera_adapterlib::format::BufferSize;
use feldera_adapterlib::metrics::ConnectorMetrics;
use feldera_adapterlib::transport::{Resume, Watermark};
use feldera_types::adapter_stats::ConnectorHealth;
use feldera_types::config::FtModel;
use std::sync::{Arc, Mutex, MutexGuard};

pub type ErrorCallback = Box<dyn FnMut(bool, &AnyError) + Send>;

/// Inner state of `MockInputConsumer` shared by all clones of the consumer.
pub struct MockInputConsumerState {
    /// `eoi` has been received since the last `reset`.
    pub eoi: bool,

    /// Number of times `extended` has been called since the last `reset`.
    pub n_extended: usize,

    /// Value reported by `InputConsumer::max_batch_size`.
    ///
    /// Unlimited by default, so a test that wants an endpoint to split a parsed
    /// buffer across steps has to lower it.
    max_batch_size: usize,

    /// The last error received from the endpoint since the last `reset`.
    pub endpoint_error: Option<AnyError>,

    /// Callback to invoke on transport error.
    ///
    /// Panics on error if `None`.
    error_cb: Option<ErrorCallback>,

    pub transaction_in_progress: bool,

    /// True if the transaction in progress is one that every host requests
    /// (see `InputConsumer::start_transaction_on_all_hosts`).
    pub transaction_on_all_hosts: bool,

    /// What `InputConsumer::open_transaction` reports.  `None`, the default,
    /// acts like a single-host pipeline.  `Some(None)` acts like a multihost
    /// pipeline with no transaction open, so that a connector that requests
    /// a transaction waits for it.
    pub open_transaction: Option<Option<i64>>,

    /// The endpoint's custom metrics, captured from `set_custom_metrics`.
    ///
    /// Lets a test read a connector's own in-process counters (e.g. a
    /// consumer-created/deleted count) instead of polling an external
    /// service for the same fact. The in-process value is available the
    /// instant the connector's own call returns, with no network round
    /// trip, so a test built on it is not sensitive to how fast that
    /// external service happens to respond under load.
    custom_metrics: Option<Arc<dyn ConnectorMetrics>>,
}

impl MockInputConsumerState {
    fn new() -> Self {
        Self {
            eoi: false,
            n_extended: 0,
            max_batch_size: usize::MAX,
            endpoint_error: None,
            error_cb: None,
            transaction_in_progress: false,
            transaction_on_all_hosts: false,
            open_transaction: None,
            custom_metrics: None,
        }
    }

    /// Reset all fields to defaults.
    pub fn reset(&mut self) {
        self.eoi = false;
        self.n_extended = 0;
        self.endpoint_error = None;
        self.transaction_in_progress = false;
        self.transaction_on_all_hosts = false;
        // `custom_metrics` is not reset: the endpoint sets it once, at
        // `open()`, not on every step.
    }
}

/// An implementation of `InputConsumer`
#[derive(Clone)]
pub struct MockInputConsumer(Arc<Mutex<MockInputConsumerState>>);

impl Default for MockInputConsumer {
    fn default() -> Self {
        Self::new()
    }
}

impl MockInputConsumer {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(MockInputConsumerState::new())))
    }

    pub fn reset(&self) {
        self.state().reset();
    }

    /// Caps the records an endpoint may hand to the circuit per step.
    pub fn set_max_batch_size(&self, max_batch_size: usize) {
        self.state().max_batch_size = max_batch_size;
    }

    pub fn state(&self) -> MutexGuard<'_, MockInputConsumerState> {
        self.0.lock().unwrap()
    }

    pub fn on_error(&self, error_cb: Option<ErrorCallback>) {
        self.state().error_cb = error_cb;
    }

    pub fn get_error(&self) -> Option<String> {
        self.state().endpoint_error.as_ref().map(|e| e.to_string())
    }

    /// The named value of one of the endpoint's custom metrics, read from
    /// its own counters rather than from whatever external service it
    /// talks to.
    ///
    /// Returns `None` if the endpoint never called `set_custom_metrics`, or
    /// if `name` is not one of the metrics it reports.
    pub fn custom_metric(&self, name: &str) -> Option<f64> {
        self.state()
            .custom_metrics
            .as_ref()?
            .metrics()
            .into_iter()
            .find(|(metric_name, ..)| *metric_name == name)
            .map(|(_, _, _, value)| value)
    }
}

impl InputConsumer for MockInputConsumer {
    fn error(&self, fatal: bool, error: AnyError, _tag: Option<&'static str>) {
        let mut state = self.state();

        if let Some(error_cb) = &mut state.error_cb {
            error_cb(fatal, &error);
        } else {
            panic!(
                "mock_input_consumer: {} transport error '{error}'",
                if fatal { "fatal" } else { "non-fatal" }
            );
        }
        state.endpoint_error = Some(error);
    }

    fn eoi(&self) {
        let mut state = self.state();
        state.eoi = true;
    }

    fn max_batch_size(&self) -> usize {
        self.state().max_batch_size
    }

    fn pipeline_fault_tolerance(&self) -> Option<FtModel> {
        Some(FtModel::ExactlyOnce)
    }

    fn parse_errors(&self, _errors: Vec<ParseError>) {}

    fn buffered(&self, _amt: BufferSize) {}

    fn replayed(&self, _num_records: BufferSize, _hash: u64) {}

    fn request_step(&self) {}

    fn extended(&self, _amt: BufferSize, _resume: Option<Resume>, _watermark: Vec<Watermark>) {
        self.state().n_extended += 1;
    }

    fn start_transaction(&self, _label: Option<&str>) {
        let mut state = self.state();
        state.transaction_in_progress = true;
        state.transaction_on_all_hosts = false;
    }

    fn start_transaction_on_all_hosts(&self, _label: Option<&str>) {
        let mut state = self.state();
        state.transaction_in_progress = true;
        state.transaction_on_all_hosts = true;
    }

    fn commit_transaction(&self) {
        let mut state = self.state();
        state.transaction_in_progress = false;
        state.transaction_on_all_hosts = false;
    }

    fn update_connector_health(&self, _health: ConnectorHealth) {}

    fn open_transaction(&self) -> Option<Option<i64>> {
        self.state().open_transaction
    }

    fn set_custom_metrics(&self, metrics: Arc<dyn ConnectorMetrics>) {
        self.state().custom_metrics = Some(metrics);
    }

    fn completion_watcher(
        &self,
    ) -> Option<tokio::sync::watch::Receiver<feldera_types::coordination::Completion>> {
        None
    }
}

pub struct MockInputParserState {
    /// All data received from the endpoint since the last `reset`.
    pub data: Vec<u8>,

    /// The last result returned by the parser.
    pub parser_result: Option<Vec<ParseError>>,

    /// Parser to push data to.
    parser: Box<dyn Parser>,

    /// Callback to invoke on transport or parser error.
    ///
    /// Panics on error if `None`.
    error_cb: Option<ErrorCallback>,
}

impl MockInputParserState {
    fn new(parser: Box<dyn Parser>) -> Self {
        Self {
            data: Vec::new(),
            parser_result: None,
            parser,
            error_cb: None,
        }
    }
}

#[derive(Clone)]
pub struct MockInputParser(Arc<Mutex<MockInputParserState>>);

impl MockInputParser {
    fn new(parser: Box<dyn Parser>) -> Self {
        Self(Arc::new(Mutex::new(MockInputParserState::new(parser))))
    }

    pub fn from_handle(input_handle: &InputCollectionHandle, format_config: &FormatConfig) -> Self {
        let format = get_input_format(&format_config.name).unwrap();
        let parser = format
            .new_parser("mock_input_endpoint", input_handle, &format_config.config)
            .unwrap();
        Self::new(parser)
    }

    /// Reset all fields to defaults.
    pub fn reset(&self) {
        let mut state = self.0.lock().unwrap();
        state.data.clear();
        state.parser_result = None;
    }

    pub fn state(&self) -> MutexGuard<'_, MockInputParserState> {
        self.0.lock().unwrap()
    }

    pub fn on_error(&self, error_cb: Option<ErrorCallback>) {
        let mut state = self.0.lock().unwrap();
        state.error_cb = error_cb;
    }
}

impl Parser for MockInputParser {
    fn parse(
        &mut self,
        data: &[u8],
        metadata: Option<ConnectorMetadata>,
    ) -> (Option<Box<dyn InputBuffer>>, Vec<ParseError>) {
        let mut state = self.0.lock().unwrap();
        state.data.extend_from_slice(data);
        let (buffer, errors) = state.parser.parse(data, metadata);

        for error in errors.iter() {
            // println!("parser returned '{:?}'", state.parser_result);
            if let Some(error_cb) = &mut state.error_cb {
                error_cb(false, &anyhow!(error.clone()));
            } else {
                panic!("mock_input_consumer: parse error '{error}'");
            }
        }

        state.parser_result = Some(errors.clone());
        (buffer, errors)
    }

    fn fork(&self) -> Box<dyn Parser> {
        let state = self.0.lock().unwrap();
        Box::new(Self::new(state.parser.fork()))
    }

    fn splitter(&self) -> Box<dyn Splitter> {
        let state = self.0.lock().unwrap();
        state.parser.splitter()
    }

    fn stage(&self, buffers: Vec<Box<dyn InputBuffer>>) -> Box<dyn StagedBuffers> {
        self.0.lock().unwrap().parser.stage(buffers)
    }
}
