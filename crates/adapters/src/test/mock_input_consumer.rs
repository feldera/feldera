use crate::catalog::InputCollectionHandle;
use crate::{
    controller::PipelineFormatConfig, transport::Step, InputConsumer, InputFormat, ParseError,
    Parser,
};
use anyhow::{anyhow, Error as AnyError};
use std::sync::{Arc, Mutex, MutexGuard};

pub type ErrorCallback = Box<dyn FnMut(bool, &AnyError) + Send>;

/// Inner state of `MockInputConsumer` shared by all clones of the consumer.
pub struct MockInputConsumerState {
    /// All data received from the endpoint since the last `reset`.
    pub data: Vec<u8>,

    /// `eoi` has been received since the last `reset`.
    pub eoi: bool,

    /// The last error received from the endpoint since the last `reset`.
    pub endpoint_error: Option<AnyError>,

    /// The last result returned by the parser.
    pub parser_result: Option<(usize, Vec<ParseError>)>,

    /// Parser to push data to.
    parser: Box<dyn Parser>,

    /// Callback to invoke on transport or parser error.
    ///
    /// Panics on error if `None`.
    error_cb: Option<ErrorCallback>,
}

impl MockInputConsumerState {
    fn new(parser: Box<dyn Parser>) -> Self {
        Self {
            data: Vec::new(),
            eoi: false,
            endpoint_error: None,
            parser_result: None,
            parser,
            error_cb: None,
        }
    }

    pub fn from_handle(
        input_handle: &InputCollectionHandle,
        format_config: &PipelineFormatConfig,
    ) -> Self {
        let format = <dyn InputFormat>::get_format(&format_config.name).unwrap();
        let parser = format
            .new_parser("mock_input_endpoint", input_handle, &format_config.config)
            .unwrap();
        Self::new(parser)
    }

    /// Reset all fields to defaults.
    pub fn reset(&mut self) {
        self.data.clear();
        self.eoi = false;
        self.endpoint_error = None;
        self.parser_result = None;
    }
}

/// An implementation of `InputConsumer`
#[derive(Clone)]
pub struct MockInputConsumer(Arc<Mutex<MockInputConsumerState>>);

impl MockInputConsumer {
    pub fn from_handle(
        input_handle: &InputCollectionHandle,
        format_config: &PipelineFormatConfig,
    ) -> Self {
        Self(Arc::new(Mutex::new(MockInputConsumerState::from_handle(
            input_handle,
            format_config,
        ))))
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

    fn input(&mut self, data: &[u8], fragment: bool) -> Vec<ParseError> {
        // println!("input");
        let mut state = self.state();

        state.data.extend_from_slice(data);
        let (num_records, errors) = if fragment {
            state.parser.input_fragment(data)
        } else {
            state.parser.input_chunk(data)
        };

        for error in errors.iter() {
            // println!("parser returned '{:?}'", state.parser_result);
            if let Some(error_cb) = &mut state.error_cb {
                error_cb(false, &anyhow!(error.clone()));
            } else {
                panic!("mock_input_consumer: parse error '{error}'");
            }
        }

        state.parser_result = Some((num_records, errors.clone()));
        errors
    }
}

impl InputConsumer for MockInputConsumer {
    fn input_fragment(&mut self, data: &[u8]) -> Vec<ParseError> {
        self.input(data, true)
    }

    fn input_chunk(&mut self, data: &[u8]) -> Vec<ParseError> {
        self.input(data, false)
    }

    fn error(&mut self, fatal: bool, error: AnyError) {
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

    fn eoi(&mut self) -> Vec<ParseError> {
        let mut state = self.state();
        state.eoi = true;

        let (_num_records, errors) = state.parser.eoi();
        for error in errors.iter() {
            if let Some(error_cb) = &mut state.error_cb {
                error_cb(false, &anyhow!(error.clone()));
            } else {
                panic!("mock_input_consumer: parse error '{error}'");
            }
        }
        errors
    }

    fn fork(&self) -> Box<dyn InputConsumer> {
        Box::new(self.clone())
    }

    fn start_step(&mut self, _step: Step) {}

    fn committed(&mut self, _step: Step) {}
}
