use anyhow::{Error as AnyError, Result as AnyResult};
use once_cell::sync::Lazy;
use serde_yaml::Value as YamlValue;
use std::borrow::Cow;
use std::collections::BTreeMap;

mod file;

#[cfg(feature = "with-kafka")]
mod kafka;

pub use file::{FileInputTransport, FileOutputTransport};

#[cfg(feature = "with-kafka")]
pub use kafka::KafkaInputTransport;

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
    BTreeMap::from([(
        "file",
        Box::new(FileOutputTransport) as Box<dyn OutputTransport>,
    )])
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
    /// * `config` - Transport-specific configuration.
    ///
    /// * `consumer` - Input consumer that will receive data from the endpoint.
    ///
    /// # Errors
    ///
    /// Fails if the specified configuration is invalid or the endpoint failed
    /// to initialize (e.g., the endpoint was not able to establish a network
    /// connection).
    fn new_endpoint(
        &self,
        config: &YamlValue,
        consumer: Box<dyn InputConsumer>,
    ) -> AnyResult<Box<dyn InputEndpoint>>;
}

impl dyn InputTransport {
    /// Lookup input transport by name.
    pub fn get_transport(name: &str) -> Option<&'static dyn InputTransport> {
        INPUT_TRANSPORT.get(name).map(|f| &**f)
    }
}

/// Input transport endpoint receives a stream of bytes via the underlying
/// data transport protocol and pushes it to the associated [`InputConsumer`].
pub trait InputEndpoint: Send {
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
    /// Push a chunk of data to the consumer.
    fn input(&mut self, data: &[u8]);

    /// Endpoint failed.
    ///
    /// Endpoint failed; no more data will be received from this endpoint.
    fn error(&mut self, error: AnyError);

    /// End-of-input-stream notification.
    ///
    /// No more data will be received from the endpoint.
    fn eoi(&mut self);

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
    /// * `config` - Transport-specific configuration.
    ///
    /// # Errors
    ///
    /// Fails if the specified configuration is invalid or the endpoint failed
    /// to initialize (e.g., the endpoint was not able to establish a network
    /// connection).
    fn new_endpoint(&self, config: &YamlValue) -> AnyResult<Box<dyn OutputEndpoint>>;
}

impl dyn OutputTransport {
    /// Lookup output transport by name.
    pub fn get_transport(name: &str) -> Option<&'static dyn OutputTransport> {
        OUTPUT_TRANSPORT.get(name).map(|f| &**f)
    }
}

pub trait OutputEndpoint: Send {
    fn push_buffer(&mut self, buffer: &[u8]) -> AnyResult<()>;
}
