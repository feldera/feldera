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
use std::fmt::Display;
use std::marker::PhantomData;
use std::sync::mpsc::{Receiver, RecvError, TryRecvError};

use adhoc::AdHocInputEndpoint;
use anyhow::{anyhow, Result as AnyResult};
use http::HttpInputEndpoint;
#[cfg(feature = "with-pubsub")]
use pubsub::PubSubInputEndpoint;
use rmpv::ext::Error as RmpDecodeError;

use serde::Deserialize;
pub mod adhoc;
mod file;
pub mod http;

pub mod url;

mod s3;
mod secret_resolver;

#[cfg(feature = "with-kafka")]
pub(crate) mod kafka;

#[cfg(feature = "with-nexmark")]
mod nexmark;

#[cfg(feature = "with-pubsub")]
mod pubsub;

use feldera_types::config::TransportConfig;

use crate::transport::file::{FileInputEndpoint, FileOutputEndpoint};
#[cfg(feature = "with-kafka")]
use crate::transport::kafka::{
    KafkaFtInputEndpoint, KafkaFtOutputEndpoint, KafkaInputEndpoint, KafkaOutputEndpoint,
};
#[cfg(feature = "with-nexmark")]
use crate::transport::nexmark::NexmarkEndpoint;
use crate::transport::s3::S3InputEndpoint;
use crate::transport::url::UrlInputEndpoint;
use feldera_datagen::GeneratorEndpoint;

pub use feldera_adapterlib::transport::*;

/// Creates an input transport endpoint instance using an input transport configuration.
///
/// If `fault_tolerant` is true, this function succeeds only if it can create a
/// fault-tolerant endpoint.
///
/// Returns an error if there is a invalid configuration for the endpoint.
/// Returns `None` if the transport configuration variant is incompatible with an input endpoint.
#[allow(unused_variables)]
pub fn input_transport_config_to_endpoint(
    config: TransportConfig,
    endpoint_name: &str,
    fault_tolerant: bool,
) -> AnyResult<Option<Box<dyn TransportInputEndpoint>>> {
    let endpoint: Box<dyn TransportInputEndpoint> = match config {
        TransportConfig::FileInput(config) => Box::new(FileInputEndpoint::new(config)),
        #[cfg(feature = "with-kafka")]
        TransportConfig::KafkaInput(config) => match fault_tolerant {
            false => Box::new(KafkaInputEndpoint::new(config, endpoint_name)?),
            true => Box::new(KafkaFtInputEndpoint::new(config)?),
        },
        #[cfg(not(feature = "with-kafka"))]
        TransportConfig::KafkaInput(_) => return Ok(None),
        #[cfg(feature = "with-pubsub")]
        TransportConfig::PubSubInput(config) => Box::new(PubSubInputEndpoint::new(config.clone())?),
        #[cfg(not(feature = "with-pubsub"))]
        TransportConfig::PubSubInput(_) => return Ok(None),
        TransportConfig::UrlInput(config) => Box::new(UrlInputEndpoint::new(config)),
        TransportConfig::S3Input(config) => Box::new(S3InputEndpoint::new(config)?),
        TransportConfig::Datagen(config) => Box::new(GeneratorEndpoint::new(config.clone())),
        #[cfg(feature = "with-nexmark")]
        TransportConfig::Nexmark(config) => Box::new(NexmarkEndpoint::new(config.clone())),
        #[cfg(not(feature = "with-nexmark"))]
        TransportConfig::Nexmark(_) => return Ok(None),
        TransportConfig::HttpInput(config) => Box::new(HttpInputEndpoint::new(config)),
        TransportConfig::AdHocInput(config) => Box::new(AdHocInputEndpoint::new(config)),
        TransportConfig::FileOutput(_)
        | TransportConfig::KafkaOutput(_)
        | TransportConfig::DeltaTableInput(_)
        | TransportConfig::DeltaTableOutput(_)
        | TransportConfig::HttpOutput => return Ok(None),
    };
    if fault_tolerant && !endpoint.is_fault_tolerant() {
        return Err(anyhow!(
            "fault tolerance for endpoint could not be configured"
        ));
    }
    Ok(Some(endpoint))
}

/// Creates an output transport endpoint instance using an output transport configuration.
///
/// If `fault_tolerant` is true, this function attempts to create a
/// fault-tolerant output endpoint (but it will still return a non-FT endpoint
/// if that's all it can do).
///
/// Returns an error if there is a invalid configuration for the endpoint.
/// Returns `None` if the transport configuration variant is incompatible with an output endpoint.
#[allow(unused_variables)]
pub fn output_transport_config_to_endpoint(
    config: TransportConfig,
    endpoint_name: &str,
    fault_tolerant: bool,
) -> AnyResult<Option<Box<dyn OutputEndpoint>>> {
    match config {
        TransportConfig::FileOutput(config) => Ok(Some(Box::new(FileOutputEndpoint::new(config)?))),
        #[cfg(feature = "with-kafka")]
        TransportConfig::KafkaOutput(config) => match fault_tolerant {
            false => Ok(Some(Box::new(KafkaOutputEndpoint::new(
                config,
                endpoint_name,
            )?))),
            true => Ok(Some(Box::new(KafkaFtOutputEndpoint::new(config)?))),
        },
        _ => Ok(None),
    }
}

/// A [Receiver] wrapper for [InputReaderCommand] for fault-tolerant connectors.
///
/// A fault-tolerant connector wants to receive, in order:
///
/// - Zero or one [InputReaderCommand::Seek]s.
///
/// - Zero or more [InputReaderCommand::Replays].
///
/// - Zero or more other commands.
///
/// This helps with that.
// This is used by Kafka and Nexmark but both of those are optional.
#[allow(dead_code)]
struct InputCommandReceiver<T> {
    receiver: Receiver<InputReaderCommand>,
    buffer: Option<InputReaderCommand>,
    _phantom: PhantomData<T>,
}

/// Error type returned by some [InputCommandReceiver] methods.
///
/// We could just use `anyhow` and that would probably be just as good though.
#[derive(Debug)]
enum InputCommandReceiverError {
    Disconnected,
    DecodeError(RmpDecodeError),
}

impl std::error::Error for InputCommandReceiverError {}

impl Display for InputCommandReceiverError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InputCommandReceiverError::Disconnected => write!(f, "sender disconnected"),
            InputCommandReceiverError::DecodeError(e) => e.fmt(f),
        }
    }
}

impl From<RecvError> for InputCommandReceiverError {
    fn from(_: RecvError) -> Self {
        Self::Disconnected
    }
}

impl From<RmpDecodeError> for InputCommandReceiverError {
    fn from(value: RmpDecodeError) -> Self {
        Self::DecodeError(value)
    }
}

// This is used by Kafka and Nexmark but both of those are optional.
#[allow(dead_code)]
impl<T> InputCommandReceiver<T> {
    fn new(receiver: Receiver<InputReaderCommand>) -> Self {
        Self {
            receiver,
            buffer: None,
            _phantom: PhantomData,
        }
    }

    fn recv_seek(&mut self) -> Result<Option<T>, InputCommandReceiverError>
    where
        T: for<'a> Deserialize<'a>,
    {
        debug_assert!(self.buffer.is_none());
        match self.recv()? {
            InputReaderCommand::Seek(metadata) => Ok(Some(rmpv::ext::from_value::<T>(metadata)?)),
            InputReaderCommand::Disconnect => Err(InputCommandReceiverError::Disconnected),
            other => {
                self.put_back(other);
                Ok(None)
            }
        }
    }

    fn recv_replay(&mut self) -> Result<Option<T>, InputCommandReceiverError>
    where
        T: for<'a> Deserialize<'a>,
    {
        match self.recv()? {
            InputReaderCommand::Seek(_) => unreachable!(),
            InputReaderCommand::Replay(metadata) => Ok(Some(rmpv::ext::from_value::<T>(metadata)?)),
            other => {
                self.put_back(other);
                Ok(None)
            }
        }
    }

    fn recv(&mut self) -> Result<InputReaderCommand, RecvError> {
        match self.buffer.take() {
            Some(value) => Ok(value),
            None => self.receiver.recv(),
        }
    }

    fn try_recv(&mut self) -> Result<Option<InputReaderCommand>, RecvError> {
        if let Some(command) = self.buffer.take() {
            Ok(Some(command))
        } else {
            match self.receiver.try_recv() {
                Ok(command) => Ok(Some(command)),
                Err(TryRecvError::Empty) => Ok(None),
                Err(TryRecvError::Disconnected) => Err(RecvError),
            }
        }
    }

    fn put_back(&mut self, value: InputReaderCommand) {
        assert!(self.buffer.is_none());
        self.buffer = Some(value);
    }
}
