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
use crate::{InputBuffer, ParseError, Parser};
use anyhow::{Error as AnyError, Result as AnyResult};
use dyn_clone::DynClone;
#[cfg(feature = "with-pubsub")]
use pubsub::PubSubInputEndpoint;
use std::collections::VecDeque;
use std::ops::Range;
use std::sync::{atomic::AtomicU64, Mutex};

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

/// Step number for fault-tolerant input and output.
///
/// A [fault-tolerant](crate#fault-tolerance) data transport divides input into
/// steps numbered sequentially.  The first step is numbered zero.  If a given
/// step is read multiple times, it will have the same content as the first
/// time.
///
/// The step number increases by 1 each time the circuit runs; that is, it
/// tracks the global clock for the outermost circuit.
pub type Step = u64;

/// Atomic version of [`Step`].
pub type AtomicStep = AtomicU64;

/// Creates an input transport endpoint instance using an input transport configuration.
///
/// Returns an error if there is a invalid configuration for the endpoint.
/// Returns `None` if the transport configuration variant is incompatible with an input endpoint.
pub fn input_transport_config_to_endpoint(
    config: TransportConfig,
) -> AnyResult<Option<Box<dyn TransportInputEndpoint>>> {
    match config {
        TransportConfig::FileInput(config) => Ok(Some(Box::new(FileInputEndpoint::new(config)))),
        #[cfg(feature = "with-kafka")]
        TransportConfig::KafkaInput(config) => match config.fault_tolerance {
            None => Ok(Some(Box::new(KafkaInputEndpoint::new(config)?))),
            Some(_) => Ok(Some(Box::new(KafkaFtInputEndpoint::new(config)?))),
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

/// Creates an output transport endpoint instance using an output transport configuration.
///
/// Returns an error if there is a invalid configuration for the endpoint.
/// Returns `None` if the transport configuration variant is incompatible with an output endpoint.
pub fn output_transport_config_to_endpoint(
    config: TransportConfig,
) -> AnyResult<Option<Box<dyn OutputEndpoint>>> {
    match config {
        TransportConfig::FileOutput(config) => Ok(Some(Box::new(FileOutputEndpoint::new(config)?))),
        #[cfg(feature = "with-kafka")]
        TransportConfig::KafkaOutput(config) => match config.fault_tolerance {
            None => Ok(Some(Box::new(KafkaOutputEndpoint::new(config)?))),
            Some(_) => Ok(Some(Box::new(KafkaFtOutputEndpoint::new(config)?))),
        },
        _ => Ok(None),
    }
}

/// A configured input endpoint.
///
/// Input endpoints come in two flavors:
///
/// * A [fault-tolerant](crate#fault-tolerance) endpoint divides its input into
///   numbered steps.  A given step always contains the same data if it is read
///   more than once.
///
/// * A non-fault-tolerant endpoint does not have a concept of steps and need
///   not yield the same data each time it is read.
pub trait InputEndpoint: Send {
    /// Whether this endpoint is [fault tolerant](crate#fault-tolerance).
    fn is_fault_tolerant(&self) -> bool;

    /// For a fault-tolerant endpoint, notifies the endpoint that steps less
    /// than `step` aren't needed anymore.  It may optionally discard them.
    ///
    /// This is a no-op for non-fault-tolerant endpoints.
    fn expire(&self, _step: Step) {}

    /// For a fault-tolerant endpoint, determines and returns the range of steps
    /// that a reader for this endpoint can read without adding new ones.
    ///
    /// Panics for non-fault-tolerant endpoints.
    fn steps(&self) -> AnyResult<Range<Step>> {
        debug_assert!(!self.is_fault_tolerant());
        unreachable!()
    }
}

pub trait TransportInputEndpoint: InputEndpoint {
    /// Returns an [`InputReader`] for reading the endpoint's data.  For a
    /// fault-tolerant endpoint, `step` indicates the first step to be read; for
    /// a non-fault-tolerant endpoint, it is ignored.
    ///
    /// Data and status will be passed to `consumer`.
    ///
    /// The reader is initially paused.  The caller may call
    /// [`InputReader::start`] to start reading.
    fn open(
        &self,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        start_step: Step,
        schema: Relation,
    ) -> AnyResult<Box<dyn InputReader>>;
}

pub trait IntegratedInputEndpoint: InputEndpoint {
    fn open(
        &self,
        input_handle: &InputCollectionHandle,
        start_step: Step,
    ) -> AnyResult<Box<dyn InputReader>>;
}

/// Reads data from an endpoint.
///
/// Use [`TransportInputEndpoint::open`] to obtain an [`InputReader`].
///
/// A new reader is initially paused.  Call [`InputReader::start`] to start
/// reading.
pub trait InputReader: Send {
    /// Start or resume the endpoint.
    fn start(&self, step: Step) -> AnyResult<()>;

    /// Pause the endpoint.  The endpoint should stop reading additional data.
    ///
    /// This allows the controller to manage memory consumption and respond to
    /// user requests to pause the circuit or the endpoint.
    fn pause(&self) -> AnyResult<()>;

    /// Requests that the endpoint completes steps up to `_step`.  This is
    /// meaningful only for fault-tolerant endpoints.
    ///
    /// An endpoint may complete steps even without a call to this function.  It
    /// might, for example, limit the size of a single step and therefore
    /// complete once a step fills up to the maximum size.
    fn complete(&self, _step: Step) {}

    /// A reader reads records into an internal buffer.  This method requests
    /// the reader to write the `n` oldest of those records to the input handle
    /// (or as many as it has if that is less than `n`).  Some endpoints might
    /// have to write records in groups, so that they actually write more than
    /// `n`.  In any case, this method returns the number actually written.
    fn flush(&self, n: usize) -> usize;

    fn flush_all(&self) -> usize {
        self.flush(usize::MAX)
    }

    /// Disconnect the endpoint.
    ///
    /// Disconnect the endpoint and stop receiving data.  This is the last
    /// method invoked before the endpoint object is dropped.  It may return
    /// before the dataflow has been fully terminated, i.e., few additional
    /// data buffers may be pushed downstream before the endpoint gets
    /// disconnected.
    fn disconnect(&self);
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
    ///
    /// Using this method automatically satisfies the requirements described for
    /// [InputConsumer::queued].
    pub fn push(
        &self,
        num_bytes: usize,
        (buffer, errors): (Option<Box<dyn InputBuffer>>, Vec<ParseError>),
    ) {
        match buffer {
            Some(buffer) if !buffer.is_empty() => {
                let num_records = buffer.len();
                let mut guard = self.queue.lock().unwrap();
                guard.push_back(buffer);
                self.consumer.queued(num_bytes, num_records, errors);
            }
            _ => self.consumer.queued(num_bytes, 0, errors),
        }
    }

    /// Implements [InputBuffer::flush] for `InputQueue`-based endpoints.
    pub fn flush(&self, n: usize) -> usize {
        let mut total = 0;
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
        total
    }

    pub fn is_empty(&self) -> bool {
        self.queue.lock().unwrap().is_empty()
    }
}

/// Input stream consumer.
///
/// A transport endpoint pushes binary data downstream via an instance of this
/// trait.
///
/// For a fault-tolerant endpoint, where the data is divided into steps, there
/// is some special terminology:
///
///   * "Completed" steps.  A step is "completed" when the endpoint has added
///     all of the data to it that it is going to.  The reader indicates that a
///     step `step`, and all prior steps, are completed by starting the next
///     step with a call to `InputConsumer::start_step(step + 1)`.
///
///     A completed step may not yet be durable.  Completion indicates that the
///     endpoint is writing it to stable storage, but that might not be done
///     yet.  The controller can start processing the input step but it should
///     not yet yield any side effects that can't be retracted.
///
///   * "Committed" steps, that is, durable ones.  This is the term for a
///     completed step that has been written to stable storage.  The reader
///     indicates that `step`, and all prior steps, have committed by calling
///     `InputConsumer::committed(step)`.
pub trait InputConsumer: Send + Sync + DynClone {
    /// Indicates that upcoming calls are for `step`.
    fn start_step(&self, step: Step);

    /// Steps numbered less than `step` been durably recorded.  (If recording a
    /// step fails, then [`InputConsumer::error`] is called instead.)
    fn committed(&self, step: Step);

    /// Reports that the endpoint has parsed `num_bytes` of data and internally
    /// queued `num_records` records, and reports `errors` that occurred during
    /// parsing. The client can call [InputReader::flush] to load the records
    /// into the circuit.
    ///
    /// Currently, an input adapter is expected to call this method in a fashion
    /// that is atomic with adding records to their internal queue, that is,
    /// while holding whatever lock the input adapter uses to add to its
    /// queue. Otherwise:
    ///
    /// * If the inpuit adapter calls this method before adding to its queue,
    ///   the controller could request it to flush records that haven't yet been
    ///   added.  This, in turn, could cause an empty step that could result in
    ///   unexpected output, e.g. the Python `test_avg_distinct` could output a
    ///   record with nulls.
    ///
    /// * If the input adapter calls this method after adding to its queue, then
    ///   the flush operation might flush more records than the controller
    ///   requests, which in turn could cause the controller's buffer counter to
    ///   become negative (or rather to wrap around to a very large positive
    ///   integer).
    ///
    /// We could avoid the latter problem by treating negative buffer counts as
    /// zero. Maybe that is a better approach. Another way to handle it would be
    /// to require adapters to never flush more records than requested. That's
    /// currently not a viable approach for the Nexmark connector, but if we can
    /// switch to using a single stream of records instead of three that are
    /// tightly synchronized, then we could make that work too.
    ///
    /// An input adapter that uses [InputQueue] automatically satisfies the
    /// requirement above.
    ///
    /// The considerations above only apply to `num_records`.  Reporting
    /// `num_bytes` and `errors` isn't racy in the same way, so it's fine to
    /// report them at any time if `num_records` is 0.
    fn queued(&self, num_bytes: usize, num_records: usize, errors: Vec<ParseError>);

    /// Endpoint failed.
    ///
    /// Endpoint failed; no more data will be received from this endpoint.
    fn error(&self, fatal: bool, error: AnyError);

    /// End-of-input-stream notification.
    ///
    /// No more data will be received from the endpoint.
    fn eoi(&self);
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
