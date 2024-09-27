use crate::catalog::InputCollectionHandle;
use crate::format::{InputBuffer, Splitter};
use crate::transport::Step;
use crate::{controller::FormatConfig, InputConsumer, InputFormat, ParseError, Parser};
use anyhow::{anyhow, Error as AnyError};
use std::sync::{Arc, Mutex, MutexGuard};

pub type ErrorCallback = Box<dyn FnMut(bool, &AnyError) + Send>;

/// Inner state of `MockInputConsumer` shared by all clones of the consumer.
pub struct MockInputConsumerState {
    /// `eoi` has been received since the last `reset`.
    pub eoi: bool,

    /// The last error received from the endpoint since the last `reset`.
    pub endpoint_error: Option<AnyError>,

    /// Callback to invoke on transport error.
    ///
    /// Panics on error if `None`.
    error_cb: Option<ErrorCallback>,
}

impl MockInputConsumerState {
    fn new() -> Self {
        Self {
            eoi: false,
            endpoint_error: None,
            error_cb: None,
        }
    }

    /// Reset all fields to defaults.
    pub fn reset(&mut self) {
        self.eoi = false;
        self.endpoint_error = None;
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

    pub fn state(&self) -> MutexGuard<MockInputConsumerState> {
        self.0.lock().unwrap()
    }

    pub fn on_error(&self, error_cb: Option<ErrorCallback>) {
        self.state().error_cb = error_cb;
    }
}

impl InputConsumer for MockInputConsumer {
    fn error(&self, fatal: bool, error: AnyError) {
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

    fn start_step(&self, _step: Step) {}

    fn committed(&self, _step: Step) {}

    fn queued(&self, _num_bytes: usize, _num_records: usize, _errors: Vec<ParseError>) {}
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
        let format = <dyn InputFormat>::get_format(&format_config.name).unwrap();
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

    pub fn state(&self) -> MutexGuard<MockInputParserState> {
        self.0.lock().unwrap()
    }

    pub fn on_error(&self, error_cb: Option<ErrorCallback>) {
        let mut state = self.0.lock().unwrap();
        state.error_cb = error_cb;
    }
}

impl Parser for MockInputParser {
    fn parse(&mut self, data: &[u8]) -> (Option<Box<dyn InputBuffer>>, Vec<ParseError>) {
        let mut state = self.0.lock().unwrap();
        state.data.extend_from_slice(data);
        let (buffer, errors) = state.parser.parse(data);

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
}
