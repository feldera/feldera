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
use crate::format::ParseError;
use anyhow::{Error as AnyError, Result as AnyResult};
use std::ops::Range;
use std::sync::atomic::AtomicU64;

mod file;
pub mod http;

pub mod url;

mod s3;
mod secret_resolver;

#[cfg(feature = "with-kafka")]
pub(crate) mod kafka;

use pipeline_types::config::TransportConfig;

use crate::transport::file::{FileInputEndpoint, FileOutputEndpoint};
#[cfg(feature = "with-kafka")]
use crate::transport::kafka::{
    KafkaFtInputEndpoint, KafkaFtOutputEndpoint, KafkaInputEndpoint, KafkaOutputEndpoint,
};
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
) -> AnyResult<Option<Box<dyn InputEndpoint>>> {
    match config {
        TransportConfig::FileInput(config) => Ok(Some(Box::new(FileInputEndpoint::new(config)))),
        #[cfg(feature = "with-kafka")]
        TransportConfig::KafkaInput(config) => match config.fault_tolerance {
            None => Ok(Some(Box::new(KafkaInputEndpoint::new(config)?))),
            Some(_) => Ok(Some(Box::new(KafkaFtInputEndpoint::new(config)?))),
        },
        TransportConfig::UrlInput(config) => Ok(Some(Box::new(UrlInputEndpoint::new(config)))),
        TransportConfig::S3Input(config) => Ok(Some(Box::new(S3InputEndpoint::new(config)))),
        _ => Ok(None),
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

/// A configured input transport endpoint.
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
        start_step: Step,
    ) -> AnyResult<Box<dyn InputReader>>;

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

/// Reads data from an endpoint.
///
/// Use [`InputEndpoint::open`] to obtain an [`InputReader`].
///
/// A new reader is initially paused.  Call [`InputReader::start`] to start
/// reading.
pub trait InputReader: Send {
    /// Start or resume the endpoint.
    ///
    /// The endpoint must start receiving data and pushing it downstream to the
    /// consumer passed to [`InputEndpoint::open`].
    ///
    /// A fault-tolerant endpoint must not push data for a step greater than
    /// `step`.  If `step` completes, then it must still report it by calling
    /// `InputConsumer::start_step(step + 1)`, but it must not subsequently call
    /// [`InputConsumer::input_fragment`] or [`InputConsumer::input_chunk`]
    /// before the client calls [`InputReader::start(step + 1)`].
    ///
    /// A non-fault-tolerant endpoint may ignore `step`.
    fn start(&self, step: Step) -> AnyResult<()>;

    /// Pause the endpoint.
    ///
    /// The endpoint must stop pushing data downstream.  This method may
    /// return before the dataflow has been fully paused, i.e., few additional
    /// data buffers may be pushed downstream before the endpoint goes quiet.
    fn pause(&self) -> AnyResult<()>;

    /// Requests that the endpoint completes steps up to `_step`.  This is
    /// meaningful only for fault-tolerant endpoints.
    ///
    /// An endpoint may complete steps even without a call to this function.  It
    /// might, for example, limit the size of a single step and therefore
    /// complete once a step fills up to the maximum size.
    fn complete(&self, _step: Step) {}

    /// Disconnect the endpoint.
    ///
    /// Disconnect the endpoint and stop receiving data.  This is the last
    /// method invoked before the endpoint object is dropped.  It may return
    /// before the dataflow has been fully terminated, i.e., few additional
    /// data buffers may be pushed downstream before the endpoint gets
    /// disconnected.
    fn disconnect(&self);
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
pub trait InputConsumer: Send {
    /// Indicates that upcoming calls are for `step`.
    fn start_step(&mut self, step: Step);

    /// Push a fragment of the input stream to the consumer.
    ///
    /// `data` is not guaranteed to start or end on a record boundary.
    /// The parser is responsible for identifying record boundaries and
    /// buffering incomplete records to get prepended to the next
    /// input fragment.
    ///
    /// A fault-tolerant input transport keeps the order of fragments the same
    /// for a given step from one read to the next.
    fn input_fragment(&mut self, data: &[u8]) -> Vec<ParseError>;

    /// Push a chunk of data to the consumer.
    ///
    /// The chunk is expected to contain complete records only.
    ///
    /// Some data in a fault-tolerant input transport might not have an
    /// inherently defined order within a step.  The input endpoint may shuffle
    /// unordered chunks within a step from one read to the next.  For example,
    /// the fault-tolerant Kafka reader will provide chunks from a given Kafka
    /// partition in the same order on each read, but it might interleave chunks
    /// from different partitions differently each time.
    fn input_chunk(&mut self, data: &[u8]) -> Vec<ParseError>;

    /// Steps numbered less than `step` been durably recorded.  (If recording a
    /// step fails, then [`InputConsumer::error`] is called instead.)
    fn committed(&mut self, step: Step);

    /// Endpoint failed.
    ///
    /// Endpoint failed; no more data will be received from this endpoint.
    fn error(&mut self, fatal: bool, error: AnyError);

    /// End-of-input-stream notification.
    ///
    /// No more data will be received from the endpoint.
    fn eoi(&mut self) -> Vec<ParseError>;

    /// Create a new consumer instance.
    ///
    /// Used by multithreaded transport endpoints to create multiple parallel
    /// input pipelines.
    fn fork(&self) -> Box<dyn InputConsumer>;
}

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
    /// invokes `async_error_callback` notify the client about asynchronous
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

    /// Output a message consisting of a key/value pair.
    ///
    /// This API is implemented by Kafka and other transports that transmit
    /// messages consisting of key and value fields and in invoked by
    /// Kafka-specific data formats that rely on this message structure,
    /// e.g., Debezium. If a given transport does not implement this API, it
    /// should return an error.
    fn push_key(&mut self, key: &[u8], val: &[u8]) -> AnyResult<()>;

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
