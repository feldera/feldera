use crate::{controller::FormatConfig, DeCollectionHandle, InputConsumer, InputFormat, Parser};
use anyhow::{anyhow, Error as AnyError, Result as AnyResult};
use erased_serde::Deserializer as ErasedDeserializer;
use std::sync::{Arc, Mutex, MutexGuard};

pub type ErrorCallback = Box<dyn FnMut(&AnyError) + Send>;

/// Inner state of `MockInputConsumer` shared by all clones of the consumer.
pub struct MockInputConsumerState {
    /// All data received from the endpoint since the last `reset`.
    pub data: Vec<u8>,

    /// `eoi` has been received since the last `reset`.
    pub eoi: bool,

    /// The last error received from the endpoint since the last `reset`.
    pub endpoint_error: Option<AnyError>,

    /// The last result returned by the parser.
    pub parser_result: Option<AnyResult<usize>>,

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
        input_handle: &dyn DeCollectionHandle,
        format_config: &FormatConfig,
    ) -> Self {
        let format = <dyn InputFormat>::get_format(&format_config.name).unwrap();
        let parser = format
            .new_parser(
                input_handle,
                &mut <dyn ErasedDeserializer>::erase(&format_config.config),
            )
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
        input_handle: &dyn DeCollectionHandle,
        format_config: &FormatConfig,
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

    fn input(&mut self, data: &[u8], fragment: bool) -> AnyResult<()> {
        // println!("input");
        let mut state = self.state();

        state.data.extend_from_slice(data);
        let parser_result = if fragment {
            state.parser.input_fragment(data)
        } else {
            state.parser.input_chunk(data)
        };
        // println!("parser returned '{:?}'", state.parser_result);
        if let Err(e) = &parser_result {
            if let Some(error_cb) = &mut state.error_cb {
                error_cb(e);
            } else {
                panic!("mock_input_consumer: parse error '{e}'");
            }
        }

        // Wrap AnyError in Arc so we can clone it.
        let parser_result = parser_result.map_err(|e| Arc::new(e));
        let parser_result_clone = parser_result.clone();

        state.parser_result = Some(parser_result.map_err(|e| anyhow!(e)));
        state.parser.flush();
        parser_result_clone.map(|_| ()).map_err(|e| anyhow!(e))
    }
}

impl InputConsumer for MockInputConsumer {
    fn input_fragment(&mut self, data: &[u8]) -> AnyResult<()> {
        self.input(data, true)
    }

    fn input_chunk(&mut self, data: &[u8]) -> AnyResult<()> {
        self.input(data, false)
    }

    fn error(&mut self, _fatal: bool, error: AnyError) {
        let mut state = self.state();

        if let Some(error_cb) = &mut state.error_cb {
            error_cb(&error);
        } else {
            panic!("mock_input_consumer: transport error '{error}'");
        }
        state.endpoint_error = Some(error);
    }

    fn eoi(&mut self) -> AnyResult<()> {
        let mut state = self.state();
        state.eoi = true;

        match state.parser.eoi() {
            Ok(_num_records) => {
                state.parser.flush();
                Ok(())
            }
            Err(error) => {
                if let Some(error_cb) = &mut state.error_cb {
                    error_cb(&error);
                } else {
                    panic!("mock_input_consumer: parse error '{error}'");
                }
                Err(error)
            }
        }
    }

    fn fork(&self) -> Box<dyn InputConsumer> {
        Box::new(self.clone())
    }
}
