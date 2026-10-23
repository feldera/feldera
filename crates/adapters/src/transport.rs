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
use std::path::Path;
use tokio_util::sync::CancellationToken;

use adhoc::AdHocInputEndpoint;
use anyhow::Result as AnyResult;
use clock::ClockEndpoint;
use feldera_types::secret_resolver::resolve_secret_references_via_json;
use http::HttpInputEndpoint;
#[cfg(feature = "with-pubsub")]
use pubsub::PubSubInputEndpoint;

pub mod adhoc;
mod empty;
pub(crate) mod file;
pub mod http;
mod null;

pub mod url;

pub mod clock;
pub(crate) mod s3;

#[cfg(feature = "with-kafka")]
pub(crate) mod kafka;

#[cfg(feature = "with-nats")]
pub(crate) mod nats;

#[cfg(feature = "with-s2")]
pub(crate) mod s2;

#[cfg(feature = "with-pubsub")]
pub(crate) mod pubsub;

#[cfg(feature = "with-redis")]
pub(crate) mod redis;

#[cfg(any(test, feature = "testing"))]
pub mod datagen_tests;

use feldera_types::config::TransportConfig;
use feldera_types::coordination::InputShard;

#[cfg(feature = "with-redis")]
use redis::output::RedisOutputEndpoint;

use crate::transport::empty::EmptyInputEndpoint;
use crate::transport::file::{FileInputEndpoint, FileOutputEndpoint};
#[cfg(any(test, feature = "testing"))]
pub use crate::transport::file::{set_barrier, set_failure};
#[cfg(feature = "with-kafka")]
use crate::transport::kafka::{KafkaFtInputEndpoint, KafkaFtOutputEndpoint, KafkaOutputEndpoint};
use crate::transport::null::NullOutputEndpoint;

#[cfg(feature = "with-nats")]
use crate::transport::nats::{NatsInputEndpoint, NatsOutputEndpoint};

#[cfg(feature = "with-s2")]
use crate::transport::s2::{S2InputEndpoint, S2OutputEndpoint};

use crate::transport::s3::S3InputEndpoint;
use crate::transport::url::UrlInputEndpoint;
use feldera_datagen::GeneratorEndpoint;

pub use feldera_adapterlib::transport::*;

/// An input transport that another crate provides.
///
/// The Nexmark generator is one: its record types derive the static engine's traits, so it
/// lives with that engine. A crate registers a factory with `inventory::submit!`, and
/// [`input_transport_config_to_endpoint`] asks the registered factories for the configurations
/// that this crate does not build itself.
pub trait InputTransportFactory: Sync {
    /// Returns the endpoint that `config` describes, or `None` if `config` names a transport
    /// that this factory does not build.
    ///
    /// # Arguments
    ///
    /// * `config` - the transport configuration of an input endpoint, with its secrets
    ///   resolved.
    ///
    /// # Returns
    ///
    /// The endpoint, an error if `config` is invalid, or `None`.
    fn endpoint(
        &self,
        config: &TransportConfig,
    ) -> Option<AnyResult<Box<dyn TransportInputEndpoint>>>;
}

inventory::collect!(&'static dyn InputTransportFactory);

/// Returns the endpoint that a registered [`InputTransportFactory`] builds for `config`, or
/// `None` if no registered factory builds one.
fn registered_input_endpoint(
    config: &TransportConfig,
) -> Option<AnyResult<Box<dyn TransportInputEndpoint>>> {
    inventory::iter::<&dyn InputTransportFactory>
        .into_iter()
        .find_map(|factory| factory.endpoint(config))
}

/// Creates an input transport endpoint instance using an input transport
/// configuration, resolving secrets by reading `secrets_dir`.
///
/// `shard` is `Some` for a distributed connector, and then it specifies the
/// part of the input that the endpoint reads.  Only transports for which
/// [TransportConfig::supports_distribution] is true accept `Some`.
///
/// Returns an error if there is a invalid configuration for the endpoint.
/// Returns `None` if the transport configuration variant is incompatible with an input endpoint.
#[allow(unused_variables)]
pub fn input_transport_config_to_endpoint(
    config: &TransportConfig,
    endpoint_name: &str,
    secrets_dir: &Path,
    shard: Option<InputShard>,
) -> AnyResult<Option<Box<dyn TransportInputEndpoint>>> {
    debug_assert!(shard.is_none() || config.supports_distribution());
    let config = resolve_secret_references_via_json(secrets_dir, config)?;
    let endpoint: Box<dyn TransportInputEndpoint> = match config {
        TransportConfig::FileInput(config) => Box::new(FileInputEndpoint::new(config)),
        #[cfg(feature = "with-kafka")]
        TransportConfig::KafkaInput(config) => Box::new(KafkaFtInputEndpoint::new(config, shard)?),
        #[cfg(not(feature = "with-kafka"))]
        TransportConfig::KafkaInput(_) => return Ok(None),
        #[cfg(feature = "with-nats")]
        TransportConfig::NatsInput(config) => Box::new(NatsInputEndpoint::new(*config)?),
        #[cfg(not(feature = "with-nats"))]
        TransportConfig::NatsInput(_) => return Ok(None),
        #[cfg(feature = "with-s2")]
        TransportConfig::S2Input(config) => Box::new(S2InputEndpoint::new(config)?),
        #[cfg(not(feature = "with-s2"))]
        TransportConfig::S2Input(_) => return Ok(None),
        #[cfg(feature = "with-pubsub")]
        TransportConfig::PubSubInput(config) => Box::new(PubSubInputEndpoint::new(config.clone())?),
        #[cfg(not(feature = "with-pubsub"))]
        TransportConfig::PubSubInput(_) => return Ok(None),
        TransportConfig::UrlInput(config) => Box::new(UrlInputEndpoint::new(config)),
        TransportConfig::S3Input(config) => Box::new(S3InputEndpoint::new(config)?),
        TransportConfig::Datagen(config) => Box::new(GeneratorEndpoint::new(config.clone())),
        config @ TransportConfig::Nexmark(_) => match registered_input_endpoint(&config) {
            Some(endpoint) => endpoint?,
            None => return Ok(None),
        },
        TransportConfig::HttpInput(config) => Box::new(HttpInputEndpoint::new(config)),
        TransportConfig::AdHocInput(config) => Box::new(AdHocInputEndpoint::new(config)),
        TransportConfig::ClockInput(config) => Box::new(ClockEndpoint::new(config)?),
        TransportConfig::EmptyInput => Box::new(EmptyInputEndpoint),
        TransportConfig::FileOutput(_)
        | TransportConfig::KafkaOutput(_)
        | TransportConfig::DeltaTableInput(_)
        | TransportConfig::DeltaTableOutput(_)
        | TransportConfig::DynamoDBOutput(_)
        | TransportConfig::PostgresInput(_)
        | TransportConfig::PostgresCdcInput(_)
        | TransportConfig::PostgresOutput(_)
        | TransportConfig::HttpOutput(_)
        | TransportConfig::NatsOutput(_)
        | TransportConfig::RedisOutput(_)
        | TransportConfig::IcebergInput(_)
        | TransportConfig::S2Output(_)
        | TransportConfig::NullOutput => return Ok(None),
    };
    Ok(Some(endpoint))
}

/// Creates an output transport endpoint instance using an output transport
/// configuration, resolving secrets by reading `secrets_dir`.
///
/// If `fault_tolerant` is true, this function attempts to create a
/// fault-tolerant output endpoint (but it will still return a non-FT endpoint
/// if that's all it can do).
///
/// `shutdown` is `ControllerInner::shutdown_token`; an endpoint that
/// waits on something outside the pipeline uses it to stop waiting when the
/// pipeline goes down.
///
/// Returns an error if there is a invalid configuration for the endpoint.
/// Returns `None` if the transport configuration variant is incompatible with an output endpoint.
#[allow(unused_variables)]
pub fn output_transport_config_to_endpoint(
    config: &TransportConfig,
    endpoint_name: &str,
    fault_tolerant: bool,
    secrets_dir: &Path,
    shutdown: CancellationToken,
) -> AnyResult<Option<Box<dyn OutputEndpoint>>> {
    let config = resolve_secret_references_via_json(secrets_dir, config)?;
    match config {
        TransportConfig::FileOutput(config) => Ok(Some(Box::new(FileOutputEndpoint::new(config)?))),
        #[cfg(feature = "with-kafka")]
        TransportConfig::KafkaOutput(config) => match fault_tolerant {
            false => Ok(Some(Box::new(KafkaOutputEndpoint::new(
                config,
                endpoint_name,
                shutdown,
            )?))),
            true => Ok(Some(Box::new(KafkaFtOutputEndpoint::new(
                config, shutdown,
            )?))),
        },
        #[cfg(feature = "with-redis")]
        TransportConfig::RedisOutput(config) => {
            Ok(Some(Box::new(RedisOutputEndpoint::new(config)?)))
        }
        #[cfg(feature = "with-s2")]
        TransportConfig::S2Output(config) => Ok(Some(Box::new(S2OutputEndpoint::new(config)?))),
        #[cfg(feature = "with-nats")]
        TransportConfig::NatsOutput(config) => {
            Ok(Some(Box::new(NatsOutputEndpoint::new(config, shutdown)?)))
        }
        TransportConfig::NullOutput => Ok(Some(Box::new(NullOutputEndpoint))),
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use feldera_types::config::TransportConfig;
    use feldera_types::secret_resolver::default_secrets_directory;
    use serde_json::json;

    use super::input_transport_config_to_endpoint;

    /// A transport that another crate provides gives no endpoint while no factory for it is
    /// registered, as in this crate's own tests.
    #[test]
    fn an_unregistered_transport_gives_no_endpoint() {
        let config: TransportConfig =
            serde_json::from_value(json!({ "name": "nexmark", "config": { "table": "bid" } }))
                .unwrap();
        let endpoint =
            input_transport_config_to_endpoint(&config, "test", default_secrets_directory(), None)
                .unwrap();
        assert!(endpoint.is_none());
    }
}
