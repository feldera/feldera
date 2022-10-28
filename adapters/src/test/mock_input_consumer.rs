use crate::{controller::FormatConfig, Catalog, InputConsumer, InputFormat, Parser};
use anyhow::{Error as AnyError, Result as AnyResult};
use std::sync::{Arc, Mutex, MutexGuard};

/// Inner state of `MockInputConsumer` shared by all clones of the consumer.
pub struct MockInputConsumerState {
    /// All data received from the endpoint since the last `reset`.
    pub data: Vec<u8>,

    /// `eof` has been received since the last `reset`.
    pub eof: bool,

    /// The last error received from the endpoint since the last `reset`.
    pub endpoint_error: Option<AnyError>,

    /// The last result returned by the parser.
    pub parser_result: Option<AnyResult<usize>>,

    /// Parser to push data to.
    parser: Box<dyn Parser>,

    /// Callback to invoke on transport or parser error.
    ///
    /// Panics on error if `None`.
    error_cb: Option<Box<dyn FnMut(&AnyError) + Send>>,
}

impl MockInputConsumerState {
    fn new(parser: Box<dyn Parser>) -> Self {
        Self {
            data: Vec::new(),
            eof: false,
            endpoint_error: None,
            parser_result: None,
            parser,
            error_cb: None,
        }
    }

    pub fn from_config(format_config: &FormatConfig, catalog: &Arc<Mutex<Catalog>>) -> Self {
        let format = <dyn InputFormat>::get_format(&format_config.name).unwrap();
        let parser = format.new_parser(&format_config.config, catalog).unwrap();
        Self::new(parser)
    }

    /// Reset all fields to defaults.
    pub fn reset(&mut self) {
        self.data.clear();
        self.eof = false;
        self.endpoint_error = None;
        self.parser_result = None;
    }
}

/// An implementation of `InputConsumer`
#[derive(Clone)]
pub struct MockInputConsumer(Arc<Mutex<MockInputConsumerState>>);

impl MockInputConsumer {
    pub fn from_config(format_config: &FormatConfig, catalog: &Arc<Mutex<Catalog>>) -> Self {
        Self(Arc::new(Mutex::new(MockInputConsumerState::from_config(
            format_config,
            catalog,
        ))))
    }

    pub fn reset(&self) {
        self.state().reset();
    }

    pub fn state(&self) -> MutexGuard<MockInputConsumerState> {
        self.0.lock().unwrap()
    }

    pub fn on_error(&self, error_cb: Option<Box<dyn FnMut(&AnyError) + Send>>) {
        self.state().error_cb = error_cb;
    }
}

impl InputConsumer for MockInputConsumer {
    fn input(&mut self, data: &[u8]) {
        // println!("input");
        let mut state = self.state();

        state.data.extend_from_slice(data);
        let parser_result = state.parser.input(data);
        // println!("parser returned '{:?}'", state.parser_result);
        if let Err(e) = &parser_result {
            if let Some(error_cb) = &mut state.error_cb {
                error_cb(e);
            } else {
                panic!("mock_input_consumer: parse error '{e}'");
            }
        }
        state.parser_result = Some(parser_result);
        state.parser.flush();
    }

    fn error(&mut self, error: AnyError) {
        let mut state = self.state();

        if let Some(error_cb) = &mut state.error_cb {
            error_cb(&error);
        } else {
            panic!("mock_input_consumer: transport error '{error}'");
        }
        state.endpoint_error = Some(error);
    }

    fn eof(&mut self) {
        self.state().eof = true;
    }

    fn fork(&self) -> Box<dyn InputConsumer> {
        Box::new(self.clone())
    }
}
