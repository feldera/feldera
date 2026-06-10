use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::Result as AnyResult;
use feldera_adapterlib::format::{BufferSize, Parser};
use feldera_adapterlib::transport::{
    InputConsumer, InputEndpoint, InputReader, InputReaderCommand, Resume, TransportInputEndpoint,
};
use feldera_types::config::FtModel;
use feldera_types::program_schema::Relation;
use rmpv::Value as RmpValue;
use serde_json::Value as JsonValue;

/// Input transport endpoint that produces no data.
///
/// Useful for tables that require a connector declaration but need no input,
/// or in tests where a finite empty input stream is needed.
pub(crate) struct EmptyInputEndpoint;

impl InputEndpoint for EmptyInputEndpoint {
    fn fault_tolerance(&self) -> Option<FtModel> {
        // Every step produces no data, so every step can be replayed identically.
        Some(FtModel::ExactlyOnce)
    }
}

impl TransportInputEndpoint for EmptyInputEndpoint {
    fn open(
        &self,
        consumer: Box<dyn InputConsumer>,
        _parser: Box<dyn Parser>,
        _schema: Relation,
        _resume_info: Option<JsonValue>,
    ) -> AnyResult<Box<dyn InputReader>> {
        Ok(Box::new(EmptyInputReader {
            consumer,
            closed: AtomicBool::new(false),
        }))
    }
}

pub(crate) struct EmptyInputReader {
    consumer: Box<dyn InputConsumer>,
    closed: AtomicBool,
}

impl InputReader for EmptyInputReader {
    fn as_any(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }

    fn request(&self, command: InputReaderCommand) {
        match command {
            InputReaderCommand::Extend => self.consumer.eoi(),
            InputReaderCommand::Queue { .. } => self.consumer.extended(
                BufferSize::empty(),
                Some(Resume::Replay {
                    seek: JsonValue::Null,
                    replay: RmpValue::Nil,
                    hash: 0,
                }),
                vec![],
            ),
            InputReaderCommand::Replay { .. } => self.consumer.replayed(BufferSize::empty(), 0),
            InputReaderCommand::Disconnect => self.closed.store(true, Ordering::Release),
            InputReaderCommand::Pause => {}
        }
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}
