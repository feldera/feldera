//! Data transports.
//!
//! Data transport adapters implement support for a specific streaming
//! technology like Kafka.  A transport adapter carries data without
//! interpreting it (data interpretation is the job of **data format** adapters
//! found in [dbsp_adapters::format](crate::format)).
//!
//! Both input and output data transport adapters exist.  Some transports
//! have both input and output variants, and others only have one.
//!
//! Data transports are created and configured through Yaml, with a string name
//! that designates a transport and a transport-specific Yaml object to
//! configure it.  Transport configuration is encapsulated in
//! [`dbsp_adapters::TransportConfig`](crate::TransportConfig).
//!
//! To obtain a transport, create an endpoint with it, and then start reading it
//! from the beginning:
//!
//! ```ignore
//! let endpoint = input_transport_config_to_endpoint(config.clone());
//! let reader = endpoint.open(consumer, 0);
//! ```
use crate::{InputBuffer, ParseError, Parser, PipelineState};
use anyhow::{Error as AnyError, Result as AnyResult};
use dyn_clone::DynClone;
#[cfg(feature = "with-pubsub")]
use pubsub::PubSubInputEndpoint;
use serde_json::Value as JsonValue;
use std::collections::VecDeque;
use std::sync::Mutex;

mod file;
pub mod http;

pub mod url;

mod datagen;

mod s3;
mod secret_resolver;

#[cfg(feature = "with-kafka")]
pub(crate) mod kafka;

#[cfg(feature = "with-nexmark")]
mod nexmark;

#[cfg(feature = "with-pubsub")]
mod pubsub;

use crate::catalog::InputCollectionHandle;
use feldera_types::config::TransportConfig;
use feldera_types::program_schema::Relation;

use crate::transport::datagen::GeneratorEndpoint;
use crate::transport::file::{FileInputEndpoint, FileOutputEndpoint};
#[cfg(feature = "with-kafka")]
use crate::transport::kafka::{
    KafkaFtInputEndpoint, KafkaFtOutputEndpoint, KafkaInputEndpoint, KafkaOutputEndpoint,
};
#[cfg(feature = "with-nexmark")]
use crate::transport::nexmark::NexmarkEndpoint;
use crate::transport::s3::S3InputEndpoint;
use crate::transport::url::UrlInputEndpoint;

/// Step number for fault-tolerant circuits.
///
/// The step number increases by 1 each time the circuit runs; that is, it
/// tracks the global clock for the outermost circuit.  The first step is
/// numbered zero.
///
/// A [fault-tolerant](crate#fault-tolerance) output transport divides output
/// into steps numbered sequentially.  If a given step is written multiple
/// times, the endpoint must discard the later writes.
pub type Step = u64;

/// Creates an input transport endpoint instance using an input transport configuration.
///
/// Returns an error if there is a invalid configuration for the endpoint.
/// Returns `None` if the transport configuration variant is incompatible with an input endpoint.
pub fn input_transport_config_to_endpoint(
    config: TransportConfig,
    endpoint_name: &str,
) -> AnyResult<Option<Box<dyn TransportInputEndpoint>>> {
    match config {
        TransportConfig::FileInput(config) => Ok(Some(Box::new(FileInputEndpoint::new(config)))),
        #[cfg(feature = "with-kafka")]
        TransportConfig::KafkaInput(config) => match config.fault_tolerance {
            false => Ok(Some(Box::new(KafkaInputEndpoint::new(
                config,
                endpoint_name,
            )?))),
            true => Ok(Some(Box::new(KafkaFtInputEndpoint::new(config)?))),
        },
        #[cfg(not(feature = "with-kafka"))]
        TransportConfig::KafkaInput(_) => Ok(None),
        #[cfg(feature = "with-pubsub")]
        TransportConfig::PubSubInput(config) => {
            Ok(Some(Box::new(PubSubInputEndpoint::new(config.clone())?)))
        }
        #[cfg(not(feature = "with-pubsub"))]
        TransportConfig::PubSubInput(_) => Ok(None),
        TransportConfig::UrlInput(config) => Ok(Some(Box::new(UrlInputEndpoint::new(config)))),
        TransportConfig::S3Input(config) => Ok(Some(Box::new(S3InputEndpoint::new(config)?))),
        TransportConfig::Datagen(config) => {
            Ok(Some(Box::new(GeneratorEndpoint::new(config.clone()))))
        }
        #[cfg(feature = "with-nexmark")]
        TransportConfig::Nexmark(config) => {
            Ok(Some(Box::new(NexmarkEndpoint::new(config.clone()))))
        }
        #[cfg(not(feature = "with-nexmark"))]
        TransportConfig::Nexmark(_) => Ok(None),
        TransportConfig::FileOutput(_)
        | TransportConfig::KafkaOutput(_)
        | TransportConfig::DeltaTableInput(_)
        | TransportConfig::DeltaTableOutput(_)
        | TransportConfig::HttpInput
        | TransportConfig::HttpOutput => Ok(None),
    }
}

pub fn input_transport_config_is_fault_tolerant(config: &TransportConfig) -> bool {
    if let Ok(Some(endpoint)) = input_transport_config_to_endpoint(config.clone()) {
        endpoint.is_fault_tolerant()
    } else {
        false
    }
}

/// Creates an output transport endpoint instance using an output transport configuration.
///
/// Returns an error if there is a invalid configuration for the endpoint.
/// Returns `None` if the transport configuration variant is incompatible with an output endpoint.
pub fn output_transport_config_to_endpoint(
    config: TransportConfig,
    endpoint_name: &str,
) -> AnyResult<Option<Box<dyn OutputEndpoint>>> {
    match config {
        TransportConfig::FileOutput(config) => Ok(Some(Box::new(FileOutputEndpoint::new(config)?))),
        #[cfg(feature = "with-kafka")]
        TransportConfig::KafkaOutput(config) => match config.fault_tolerance {
            None => Ok(Some(Box::new(KafkaOutputEndpoint::new(
                config,
                endpoint_name,
            )?))),
            Some(_) => Ok(Some(Box::new(KafkaFtOutputEndpoint::new(config)?))),
        },
        _ => Ok(None),
    }
}

/// A configured input endpoint.
///
pub trait InputEndpoint: Send {
    /// Whether this endpoint is [fault tolerant](crate#fault-tolerance).
    ///
    /// A fault-tolerant endpoint must support [InputReaderCommand::Seek] and
    /// [InputReaderCommand::Replay].
    fn is_fault_tolerant(&self) -> bool;
}

pub trait TransportInputEndpoint: InputEndpoint {
    /// Creates a new input endpoint. The endpoint should use `parser` to parse
    /// data into records. Returns an [`InputReader`] for reading the endpoint's
    /// data.  The endpoint will use `consumer` to report its progress.
    ///
    /// The reader is initially paused.
    fn open(
        &self,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        schema: Relation,
    ) -> AnyResult<Box<dyn InputReader>>;
}

pub trait IntegratedInputEndpoint: InputEndpoint {
    fn open(&self, input_handle: &InputCollectionHandle) -> AnyResult<Box<dyn InputReader>>;
}

/// Commands for an [InputReader] to execute.
#[derive(Debug)]
pub enum InputReaderCommand {
    /// Tells a fault-tolerant input reader to seek past the data already read
    /// in the step whose metadata is given by the value.
    ///
    /// # Contraints
    ///
    /// Only fault-tolerant input readers need to accept this. If it is given,
    /// it will be issued only once and before any of the other commands.
    Seek(JsonValue),

    /// Tells the input reader to replay the step described in `metadata` by
    /// reading and flushing buffers for the data in the step, and then
    /// [InputConsumer::replayed] to signal completion.
    ///
    /// The input reader should report the data that it queues to
    /// [InputConsumer::buffered] as it does the replay.
    ///
    /// The input reader doesn't have to process other commands while it does
    /// the replay.
    ///
    /// # Contraints
    ///
    /// Only fault-tolerant input readers need to accept this. It will be issued
    /// zero or more times, after [InputReaderCommand::Seek] but before any
    /// other commands.
    Replay(JsonValue),

    /// Tells the input reader to accept further input. The first time it
    /// receives this command, the reader should start from:
    ///
    /// - If [InputReaderCommand::Seek] or [InputReaderCommand::Replay] was
    ///   previously issued, then just beyond the end of the data from the last
    ///   call.
    ///
    /// - Otherwise, from the beginning of the input.
    ///
    /// The input reader should report the data that it queues to
    /// [InputConsumer::buffered] as it queues it.
    ///
    /// # Constraints
    ///
    /// The controller will not call this function:
    ///
    /// - Twice on a given reader without an intervening
    ///   [InputReaderCommand::Pause].
    ///
    /// - If it requested a replay (with [InputReaderCommand::Replay]) and the reader
    ///   hasn't yet reported that the replay is complete.
    Extend,

    /// Tells the input reader to stop reading more input.
    ///
    /// The controller uses this to limit the number of buffered records and to
    /// respond to user requests to pause the pipeline.
    ///
    /// # Constraints
    ///
    /// The controller issues this only after a paired
    /// [InputReaderCommand::Extend].
    Pause,

    /// Tells the input reader to flush input buffers to the circuit.
    ///
    /// The input reader can call [InputConsumer::max_batch_size] to find out
    /// how many records it should flush. When it's done, it must call
    /// [InputConsumer::extended] to report it.
    ///
    /// # Constraints
    ///
    /// The controller won't issue this command before it first issues [InputReaderCommand::Extend].
    Queue,

    /// Tells the reader it's going to be dropped soon and should clean up.
    ///
    /// The reader can continue to queue some data buffers afterward if that's
    /// the easiest implementation.
    ///
    /// # Constraints
    ///
    /// The controller calls this only once and won't call any other functions
    /// for a given reader after it calls this one.
    Disconnect,
}

impl InputReaderCommand {
    /// Returns this command translated to a [NonFtInputReaderCommand], or
    /// `None` if that is not possible (because this command is only for
    /// fault-tolerant endpoints).
    pub fn as_nonft(&self) -> Option<NonFtInputReaderCommand> {
        match self {
            InputReaderCommand::Seek(_) | InputReaderCommand::Replay(_) => None,
            InputReaderCommand::Queue => Some(NonFtInputReaderCommand::Queue),
            InputReaderCommand::Extend => {
                Some(NonFtInputReaderCommand::Transition(PipelineState::Running))
            }
            InputReaderCommand::Pause => {
                Some(NonFtInputReaderCommand::Transition(PipelineState::Paused))
            }
            InputReaderCommand::Disconnect => Some(NonFtInputReaderCommand::Transition(
                PipelineState::Terminated,
            )),
        }
    }
}

/// A subset of [InputReaderCommand] that only includes the commands for
/// non-fault-tolerant connectors.
#[derive(Debug)]
pub enum NonFtInputReaderCommand {
    /// Equivalent to [InputReaderCommand::Queue].
    Queue,

    /// Equivalencies:
    ///
    /// - `Transition(PipelineState::Paused)`: [InputReaderCommand::Pause].
    ///
    /// - `Transition(PipelineState::Running)`: [InputReaderCommand::Extend].
    ///
    /// - `Transition(PipelineState::Terminated)`: [InputReaderCommand::Disconnect].
    Transition(PipelineState),
}

/// A thread-safe queue for collecting and flushing input buffers.
///
/// Commonly used by `InputReader` implementations for staging buffers from
/// worker threads.
pub struct InputQueue {
    pub queue: Mutex<VecDeque<Box<dyn InputBuffer>>>,
    pub consumer: Box<dyn InputConsumer>,
}

impl InputQueue {
    pub fn new(consumer: Box<dyn InputConsumer>) -> Self {
        Self {
            queue: Mutex::new(VecDeque::new()),
            consumer,
        }
    }

    /// Appends `buffer`, if non-`None` to the queue.  Reports to the controller
    /// that `num_bytes` have been received and at least partially parsed, and
    /// that `errors` have occurred during parsing.
    pub fn push(
        &self,
        (buffer, errors): (Option<Box<dyn InputBuffer>>, Vec<ParseError>),
        num_bytes: usize,
    ) {
        self.consumer.parse_errors(errors);
        match buffer {
            Some(buffer) if !buffer.is_empty() => {
                let num_records = buffer.len();

                let mut queue = self.queue.lock().unwrap();
                queue.push_back(buffer);
                self.consumer.buffered(num_records, num_bytes);
            }
            _ => self.consumer.buffered(num_bytes, 0),
        }
    }

    pub fn queue(&self) {
        let mut total = 0;
        let n = self.consumer.max_batch_size();
        while total < n {
            let Some(mut buffer) = self.queue.lock().unwrap().pop_front() else {
                break;
            };
            total += buffer.flush(n - total);
            if !buffer.is_empty() {
                self.queue.lock().unwrap().push_front(buffer);
                break;
            }
        }
        self.consumer.extended(total, serde_json::Value::Null);
    }

    pub fn len(&self) -> usize {
        self.queue.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Reads data from an endpoint.
///
/// Use [`TransportInputEndpoint::open`] to obtain an [`InputReader`].
pub trait InputReader: Send {
    /// Requests the input reader to execute `command`.
    fn request(&self, command: InputReaderCommand);

    fn seek(&self, metadata: JsonValue) {
        self.request(InputReaderCommand::Seek(metadata));
    }

    fn replay(&self, metadata: JsonValue) {
        self.request(InputReaderCommand::Replay(metadata));
    }

    fn extend(&self) {
        self.request(InputReaderCommand::Extend);
    }

    fn pause(&self) {
        self.request(InputReaderCommand::Pause);
    }

    fn queue(&self) {
        self.request(InputReaderCommand::Queue);
    }

    fn disconnect(&self) {
        self.request(InputReaderCommand::Disconnect);
    }
}

/// Input stream consumer.
///
/// A transport endpoint pushes binary data downstream via an instance of this
/// trait.
pub trait InputConsumer: Send + Sync + DynClone {
    /// Returns the maximum number of records that an `InputReader` should queue
    /// in response to a [InputReaderCommand::Queue] command.
    ///
    /// Nothing keeps the endpoint from queuing more than this if necessary (for
    /// example, if for the sake of lateness it needs to group more than this
    /// number of records together).
    fn max_batch_size(&self) -> usize;
    fn parse_errors(&self, errors: Vec<ParseError>);
    fn buffered(&self, num_records: usize, num_bytes: usize);
    fn replayed(&self, num_records: usize);
    fn extended(&self, num_records: usize, metadata: JsonValue);

    /// Reports that the endpoint has reached end of input and that no more data
    /// will be received from the endpoint.
    ///
    /// If the endpoint has already indicated that it has buffered records then
    /// the controller will request them in future [InputReaderCommand::Queue]
    /// messages. The endpoint must not make further calls to
    /// [InputConsumer::buffered] or [InputConsumer::parse_errors].
    fn eoi(&self);

    /// Endpoint failed.
    ///
    /// Reports that the endpoint failed and that it will not queue any more
    /// data.
    fn error(&self, fatal: bool, error: AnyError);
}

dyn_clone::clone_trait_object!(InputConsumer);

pub type AsyncErrorCallback = Box<dyn Fn(bool, AnyError) + Send + Sync>;

/// A configured output transport endpoint.
///
/// Output endpoints come in two flavors:
///
/// * A [fault-tolerant](crate#fault-tolerance) endpoint accepts output that has
///   been divided into numbered steps.  If it is given output associated with a
///   step number that has already been output, then it discards the duplicate.
///   It must also keep data written to the output transport from becoming
///   visible to downstream readers until `batch_end` is called.  (This works
///   for output to Kafka, which supports transactional output.  If it is
///   difficult for some future fault-tolerant output endpoint, then the API
///   could be adjusted to support writing output only after it can become
///   immediately visible.)
///
/// * A non-fault-tolerant endpoint does not have a concept of steps and ignores
///   them.
pub trait OutputEndpoint: Send {
    /// Finishes establishing the connection to the output endpoint.
    ///
    /// If the endpoint encounters any errors during output, now or later, it
    /// invokes `async_error_callback` to notify the client about asynchronous
    /// errors, i.e., errors that happen outside the context of the
    /// [`OutputEndpoint::push_buffer`] method. For instance, a reliable message
    /// bus like Kafka may notify the endpoint about a failure to deliver a
    /// previously sent message via an async callback. If the endpoint is unable
    /// to handle this error, it must forward it to the client via the
    /// `async_error_callback`.  The first argument of the callback is a flag
    /// that indicates a fatal error that the endpoint cannot recover from.
    fn connect(&mut self, async_error_callback: AsyncErrorCallback) -> AnyResult<()>;

    /// Maximum buffer size that this transport can transmit.
    /// The encoder should not generate buffers exceeding this size.
    fn max_buffer_size_bytes(&self) -> usize;

    /// Notifies the output endpoint that data subsequently written by
    /// `push_buffer` belong to the given `step`.
    ///
    /// A [fault-tolerant](crate#fault-tolerance) endpoint has additional
    /// requirements:
    ///
    /// 1. If data for the given step has been written before, the endpoint
    ///    should discard it.
    ///
    /// 2. The output batch must not be made visible to downstream readers
    ///    before the next call to `batch_end`.
    fn batch_start(&mut self, _step: Step) -> AnyResult<()> {
        Ok(())
    }

    fn push_buffer(&mut self, buffer: &[u8]) -> AnyResult<()>;

    /// Output a message consisting of a key/value pair, with optional value.
    ///
    /// This API is implemented by Kafka and other transports that transmit
    /// messages consisting of key and value fields and in invoked by
    /// Kafka-specific data formats that rely on this message structure,
    /// e.g., Debezium. If a given transport does not implement this API, it
    /// should return an error.
    fn push_key(&mut self, key: &[u8], val: Option<&[u8]>) -> AnyResult<()>;

    /// Notifies the output endpoint that output for the current step is
    /// complete.
    ///
    /// A fault-tolerant output endpoint may now make the output batch visible
    /// to readers.
    fn batch_end(&mut self) -> AnyResult<()> {
        Ok(())
    }

    /// Whether this endpoint is [fault tolerant](crate#fault-tolerance).
    fn is_fault_tolerant(&self) -> bool;
}
