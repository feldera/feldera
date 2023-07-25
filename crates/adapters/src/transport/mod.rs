//! Data transports.
//!
//! Data transport adapters implement support for a specific streaming
//! technology like Kafka.  A transport adapter carries data without
//! interpreting it (data interpretation is the job of **data format** adapters
//! found in [dbsp_adapters::format](crate::format)).
//!
//! Both input and output data transport adapters exist.  Every current form of
//! transport has both an input and an output adapter, but transports added in
//! the future might only be suitable for input or for output.
//!
//! Data transports are created and configured through Yaml, with a string name
//! that designates a transport and a transport-specific Yaml object to
//! configure it.  Transport configuration is encapsulated in
//! [`dbsp_adapters::TransportConfig`](crate::TransportConfig).
//!
//! The following transports are currently supported:
//!
//!   * `file`, for input from a file via [`FileInputTransport`] or output to a
//!     file via [`FileOutputTransport`].
//!
//!   * `kafka`, for input from [Kafka](https://kafka.apache.org/) via
//!     [`KafkaInputTransport`] or output to Kafka via [`KafkaOutputTransport`],
//!     if the `with-kafka` feature is enabled.
//!
//! To obtain a transport and create an endpoint with it:
//!
//! ```ignore
//! let transport = <dyn InputTransport>::get_transport(transport_name).unwrap();
//! let endpoint = transport.new_endpoint(endpoint_name, &config, consumer);
//! ```
use crate::OutputEndpointConfig;
use anyhow::{Error as AnyError, Result as AnyResult};
use once_cell::sync::Lazy;
use serde_yaml::Value as YamlValue;
use std::borrow::Cow;
use std::collections::BTreeMap;

mod file;

#[cfg(feature = "server")]
pub mod http;

#[cfg(feature = "with-kafka")]
pub(crate) mod kafka;

pub use file::{FileInputConfig, FileInputTransport, FileOutputConfig, FileOutputTransport};

#[cfg(feature = "with-kafka")]
pub use kafka::{
    KafkaInputConfig, KafkaInputTransport, KafkaLogLevel, KafkaOutputConfig, KafkaOutputTransport,
};

/// Static map of supported input transports.
// TODO: support for registering new transports at runtime in order to allow
// external crates to implement new transports.
static INPUT_TRANSPORT: Lazy<BTreeMap<&'static str, Box<dyn InputTransport>>> = Lazy::new(|| {
    BTreeMap::from([
        (
            "file",
            Box::new(FileInputTransport) as Box<dyn InputTransport>,
        ),
        #[cfg(feature = "with-kafka")]
        (
            "kafka",
            Box::new(KafkaInputTransport) as Box<dyn InputTransport>,
        ),
    ])
});

/// Static map of supported output transports.
static OUTPUT_TRANSPORT: Lazy<BTreeMap<&'static str, Box<dyn OutputTransport>>> = Lazy::new(|| {
    BTreeMap::from([
        (
            "file",
            Box::new(FileOutputTransport) as Box<dyn OutputTransport>,
        ),
        #[cfg(feature = "with-kafka")]
        (
            "kafka",
            Box::new(KafkaOutputTransport) as Box<dyn OutputTransport>,
        ),
    ])
});

/// Trait that represents a specific data transport.
///
/// This is a factory trait that creates transport endpoints for a specific
/// data format.
pub trait InputTransport: Send + Sync {
    /// Unique name of the data transport.
    fn name(&self) -> Cow<'static, str>;

    /// Create a new transport endpoint.
    ///
    /// Create and initializes a transport endpoint.  The endpoint will push
    /// received data to the provided input consumer.  The endpoint is created
    /// in a paused state.
    ///
    /// # Arguments
    ///
    /// * `name` - unique input endpoint name.
    ///
    /// * `config` - Transport-specific configuration.
    ///
    /// * `consumer` - Input consumer that will receive data from the endpoint.
    ///
    /// # Errors
    ///
    /// Fails if the specified configuration is invalid or the endpoint failed
    /// to initialize (e.g., the endpoint was not able to establish a network
    /// connection).
    fn new_endpoint(&self, name: &str, config: &YamlValue) -> AnyResult<Box<dyn InputEndpoint>>;
}

impl dyn InputTransport {
    /// Lookup input transport by `name`, which should be e.g. `file` for a file
    /// transport.
    pub fn get_transport(name: &str) -> Option<&'static dyn InputTransport> {
        INPUT_TRANSPORT.get(name).map(|f| &**f)
    }
}

/// Input transport endpoint receives a stream of bytes via the underlying
/// data transport protocol and pushes it to the associated [`InputConsumer`].
pub trait InputEndpoint: Send {
    fn connect(&mut self, consumer: Box<dyn InputConsumer>) -> AnyResult<()>;

    /// Pause the endpoint.
    ///
    /// The endpoint must stop pushing data downstream.  This method may
    /// return before the dataflow has been fully paused, i.e., few additional
    /// data buffers may be pushed downstream before the endpoint goes quiet.
    fn pause(&self) -> AnyResult<()>;

    /// Start or restart the endpoint.
    ///
    /// The endpoint must start receiving data and pushing it downstream.
    fn start(&self) -> AnyResult<()>;

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
// TODO: `input_owned`.
pub trait InputConsumer: Send {
    /// Push a fragment of the input stream to the consumer.
    ///
    /// `data` is not guaranteed to start or end on a record boundary.
    /// The parser is responsible for identifying record boundaries and
    /// buffering incomplete records to get prepended to the next
    /// input fragment.
    fn input_fragment(&mut self, data: &[u8]) -> AnyResult<()>;

    /// Push a chunk of data to the consumer.
    ///
    /// The chunk is expected to contain complete records only.
    fn input_chunk(&mut self, data: &[u8]) -> AnyResult<()>;

    /// Endpoint failed.
    ///
    /// Endpoint failed; no more data will be received from this endpoint.
    fn error(&mut self, fatal: bool, error: AnyError);

    /// End-of-input-stream notification.
    ///
    /// No more data will be received from the endpoint.
    fn eoi(&mut self) -> AnyResult<()>;

    /// Create a new consumer instance.
    ///
    /// Used by multithreaded transport endpoints to create multiple parallel
    /// input pipelines.
    fn fork(&self) -> Box<dyn InputConsumer>;
}

/// Trait that represents a specific data transport.
///
/// This is a factory trait that creates outgoing transport endpoint instances.
pub trait OutputTransport: Send + Sync {
    /// Unique name of the data transport.
    fn name(&self) -> Cow<'static, str>;

    /// Create a new transport endpoint.
    ///
    /// Create and initializes a transport endpoint.  The endpoint will push
    /// received data to the provided input consumer.  The endpoint is created
    /// in a paused state.
    ///
    /// # Arguments
    ///
    /// * `name` - unique input endpoint name.
    ///
    /// * `config` - Transport-specific configuration.
    ///
    /// * `async_error_callback` - the endpoint must invoke this callback to
    ///   notify the client about asynchronous errors, i.e., errors that happen
    ///   outside the context of the [`OutputEndpoint::push_buffer`] method. For
    ///   instance, a reliable message bus like Kafka may notify the endpoint
    ///   about a failure to deliver a previously sent message via an async
    ///   callback. If the endpoint is unable to handle this error, it must
    ///   forward it to the client via the `async_error_callback`.  The first
    ///   argument of the callback is a flag that indicates a fatal error that
    ///   the endpoint cannot recover from.
    ///
    /// # Errors
    ///
    /// Fails if the specified configuration is invalid or the endpoint failed
    /// to initialize (e.g., the endpoint was not able to establish a network
    /// connection).
    fn new_endpoint(
        &self,
        name: &str,
        config: &OutputEndpointConfig,
    ) -> AnyResult<Box<dyn OutputEndpoint>>;
}

impl dyn OutputTransport {
    /// Lookup output transport by name.
    pub fn get_transport(name: &str) -> Option<&'static dyn OutputTransport> {
        OUTPUT_TRANSPORT.get(name).map(|f| &**f)
    }
}

pub type AsyncErrorCallback = Box<dyn Fn(bool, AnyError) + Send + Sync>;

pub trait OutputEndpoint: Send {
    fn connect(&self, async_error_callback: AsyncErrorCallback) -> AnyResult<()>;

    fn batch_start(&mut self) -> AnyResult<()> {
        Ok(())
    }

    fn push_buffer(&mut self, buffer: &[u8]) -> AnyResult<()>;

    fn batch_end(&mut self) -> AnyResult<()> {
        Ok(())
    }
}
