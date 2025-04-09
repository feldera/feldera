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
use adhoc::AdHocInputEndpoint;
use anyhow::{anyhow, Result as AnyResult};
use http::HttpInputEndpoint;
#[cfg(feature = "with-pubsub")]
use pubsub::PubSubInputEndpoint;

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

#[cfg(feature = "with-redis")]
mod redis;

use feldera_types::config::TransportConfig;

#[cfg(feature = "with-redis")]
use redis::output::RedisOutputEndpoint;

use crate::transport::file::{FileInputEndpoint, FileOutputEndpoint};
#[cfg(feature = "with-kafka")]
use crate::transport::kafka::{KafkaFtInputEndpoint, KafkaFtOutputEndpoint, KafkaOutputEndpoint};
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
        TransportConfig::KafkaInput(config) => Box::new(KafkaFtInputEndpoint::new(config)?),
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
        | TransportConfig::PostgresInput(_)
        | TransportConfig::HttpOutput
        | TransportConfig::RedisOutput(_)
        | TransportConfig::IcebergInput(_) => return Ok(None),
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
        #[cfg(feature = "with-redis")]
        TransportConfig::RedisOutput(config) => {
            Ok(Some(Box::new(RedisOutputEndpoint::new(config)?)))
        }
        _ => Ok(None),
    }
}
