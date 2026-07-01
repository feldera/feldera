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

use adhoc::AdHocInputEndpoint;
use anyhow::Result as AnyResult;
use clock::ClockEndpoint;
use feldera_types::secret_resolver::resolve_secret_references_via_json;
use http::HttpInputEndpoint;
#[cfg(feature = "with-pubsub")]
use pubsub::PubSubInputEndpoint;

pub mod adhoc;
mod empty;
mod file;
pub mod http;
mod null;

pub mod url;

pub mod clock;
mod s3;

#[cfg(feature = "with-kafka")]
pub(crate) mod kafka;

#[cfg(feature = "with-nats")]
pub(crate) mod nats;

#[cfg(feature = "with-nexmark")]
mod nexmark;

#[cfg(feature = "with-pubsub")]
mod pubsub;

#[cfg(feature = "with-redis")]
mod redis;

use feldera_types::config::TransportConfig;

#[cfg(feature = "with-redis")]
use redis::output::RedisOutputEndpoint;

use crate::transport::empty::EmptyInputEndpoint;
#[cfg(test)]
pub use crate::transport::file::set_barrier;
use crate::transport::file::{FileInputEndpoint, FileOutputEndpoint};
#[cfg(feature = "with-kafka")]
use crate::transport::kafka::{KafkaFtInputEndpoint, KafkaFtOutputEndpoint, KafkaOutputEndpoint};
use crate::transport::null::NullOutputEndpoint;

#[cfg(feature = "with-nats")]
use crate::transport::nats::NatsInputEndpoint;

#[cfg(feature = "with-nexmark")]
use crate::transport::nexmark::NexmarkEndpoint;
use crate::transport::s3::S3InputEndpoint;
use crate::transport::url::UrlInputEndpoint;
use feldera_datagen::GeneratorEndpoint;

pub use feldera_adapterlib::transport::*;

pub fn builtin_input_transport_registry() -> InputTransportRegistry {
    let mut registry = InputTransportRegistry::new();
    registry.register(TransportConfig::FILE_INPUT, Box::new(FileInputFactory));
    #[cfg(feature = "with-kafka")]
    registry.register(TransportConfig::KAFKA_INPUT, Box::new(KafkaInputFactory));
    #[cfg(feature = "with-nats")]
    registry.register(TransportConfig::NATS_INPUT, Box::new(NatsInputFactory));
    #[cfg(feature = "with-pubsub")]
    registry.register(TransportConfig::PUB_SUB_INPUT, Box::new(PubSubInputFactory));
    registry.register(TransportConfig::URL_INPUT, Box::new(UrlInputFactory));
    registry.register(TransportConfig::S3_INPUT, Box::new(S3InputFactory));
    registry.register(TransportConfig::DATAGEN, Box::new(DatagenInputFactory));
    #[cfg(feature = "with-nexmark")]
    registry.register(TransportConfig::NEXMARK, Box::new(NexmarkInputFactory));
    registry.register(TransportConfig::HTTP_INPUT, Box::new(HttpInputFactory));
    registry.register(TransportConfig::ADHOC_INPUT, Box::new(AdHocInputFactory));
    registry.register(TransportConfig::CLOCK, Box::new(ClockInputFactory));
    registry.register(TransportConfig::EMPTY_INPUT, Box::new(EmptyInputFactory));
    registry
}

pub fn builtin_output_transport_registry() -> OutputTransportRegistry {
    let mut registry = OutputTransportRegistry::new();
    registry.register(TransportConfig::FILE_OUTPUT, Box::new(FileOutputFactory));
    #[cfg(feature = "with-kafka")]
    registry.register(TransportConfig::KAFKA_OUTPUT, Box::new(KafkaOutputFactory));
    #[cfg(feature = "with-redis")]
    registry.register(TransportConfig::REDIS_OUTPUT, Box::new(RedisOutputFactory));
    registry.register(TransportConfig::NULL_OUTPUT, Box::new(NullOutputFactory));
    registry
}

struct FileInputFactory;

impl InputTransportEndpointFactory for FileInputFactory {
    fn create(
        &self,
        config: &TransportConfig,
    ) -> AnyResult<Option<Box<dyn TransportInputEndpoint>>> {
        match config {
            TransportConfig::FileInput(config) => {
                Ok(Some(Box::new(FileInputEndpoint::new(config))))
            }
            _ => Ok(None),
        }
    }
}

#[cfg(feature = "with-kafka")]
struct KafkaInputFactory;

#[cfg(feature = "with-kafka")]
impl InputTransportEndpointFactory for KafkaInputFactory {
    fn create(
        &self,
        config: &TransportConfig,
    ) -> AnyResult<Option<Box<dyn TransportInputEndpoint>>> {
        match config {
            TransportConfig::KafkaInput(config) => {
                Ok(Some(Box::new(KafkaFtInputEndpoint::new(config)?)))
            }
            _ => Ok(None),
        }
    }
}

#[cfg(feature = "with-nats")]
struct NatsInputFactory;

#[cfg(feature = "with-nats")]
impl InputTransportEndpointFactory for NatsInputFactory {
    fn create(
        &self,
        config: &TransportConfig,
    ) -> AnyResult<Option<Box<dyn TransportInputEndpoint>>> {
        match config {
            TransportConfig::NatsInput(config) => {
                Ok(Some(Box::new(NatsInputEndpoint::new(config)?)))
            }
            _ => Ok(None),
        }
    }
}

#[cfg(feature = "with-pubsub")]
struct PubSubInputFactory;

#[cfg(feature = "with-pubsub")]
impl InputTransportEndpointFactory for PubSubInputFactory {
    fn create(
        &self,
        config: &TransportConfig,
    ) -> AnyResult<Option<Box<dyn TransportInputEndpoint>>> {
        match config {
            TransportConfig::PubSubInput(config) => {
                Ok(Some(Box::new(PubSubInputEndpoint::new(config.clone())?)))
            }
            _ => Ok(None),
        }
    }
}

struct UrlInputFactory;

impl InputTransportEndpointFactory for UrlInputFactory {
    fn create(
        &self,
        config: &TransportConfig,
    ) -> AnyResult<Option<Box<dyn TransportInputEndpoint>>> {
        match config {
            TransportConfig::UrlInput(config) => Ok(Some(Box::new(UrlInputEndpoint::new(config)))),
            _ => Ok(None),
        }
    }
}

struct S3InputFactory;

impl InputTransportEndpointFactory for S3InputFactory {
    fn create(
        &self,
        config: &TransportConfig,
    ) -> AnyResult<Option<Box<dyn TransportInputEndpoint>>> {
        match config {
            TransportConfig::S3Input(config) => Ok(Some(Box::new(S3InputEndpoint::new(config)?))),
            _ => Ok(None),
        }
    }
}

struct DatagenInputFactory;

impl InputTransportEndpointFactory for DatagenInputFactory {
    fn create(
        &self,
        config: &TransportConfig,
    ) -> AnyResult<Option<Box<dyn TransportInputEndpoint>>> {
        match config {
            TransportConfig::Datagen(config) => {
                Ok(Some(Box::new(GeneratorEndpoint::new(config.clone()))))
            }
            _ => Ok(None),
        }
    }
}

#[cfg(feature = "with-nexmark")]
struct NexmarkInputFactory;

#[cfg(feature = "with-nexmark")]
impl InputTransportEndpointFactory for NexmarkInputFactory {
    fn create(
        &self,
        config: &TransportConfig,
    ) -> AnyResult<Option<Box<dyn TransportInputEndpoint>>> {
        match config {
            TransportConfig::Nexmark(config) => {
                Ok(Some(Box::new(NexmarkEndpoint::new(config.clone()))))
            }
            _ => Ok(None),
        }
    }
}

struct HttpInputFactory;

impl InputTransportEndpointFactory for HttpInputFactory {
    fn create(
        &self,
        config: &TransportConfig,
    ) -> AnyResult<Option<Box<dyn TransportInputEndpoint>>> {
        match config {
            TransportConfig::HttpInput(config) => {
                Ok(Some(Box::new(HttpInputEndpoint::new(config))))
            }
            _ => Ok(None),
        }
    }
}

struct AdHocInputFactory;

impl InputTransportEndpointFactory for AdHocInputFactory {
    fn create(
        &self,
        config: &TransportConfig,
    ) -> AnyResult<Option<Box<dyn TransportInputEndpoint>>> {
        match config {
            TransportConfig::AdHocInput(config) => {
                Ok(Some(Box::new(AdHocInputEndpoint::new(config))))
            }
            _ => Ok(None),
        }
    }
}

struct ClockInputFactory;

impl InputTransportEndpointFactory for ClockInputFactory {
    fn create(
        &self,
        config: &TransportConfig,
    ) -> AnyResult<Option<Box<dyn TransportInputEndpoint>>> {
        match config {
            TransportConfig::ClockInput(config) => Ok(Some(Box::new(ClockEndpoint::new(config)?))),
            _ => Ok(None),
        }
    }
}

struct EmptyInputFactory;

impl InputTransportEndpointFactory for EmptyInputFactory {
    fn create(
        &self,
        config: &TransportConfig,
    ) -> AnyResult<Option<Box<dyn TransportInputEndpoint>>> {
        match config {
            TransportConfig::EmptyInput => Ok(Some(Box::new(EmptyInputEndpoint))),
            _ => Ok(None),
        }
    }
}

struct FileOutputFactory;

impl OutputTransportEndpointFactory for FileOutputFactory {
    fn create(
        &self,
        config: &TransportConfig,
        _endpoint_name: &str,
        _fault_tolerant: bool,
    ) -> AnyResult<Option<Box<dyn OutputEndpoint>>> {
        match config {
            TransportConfig::FileOutput(config) => {
                Ok(Some(Box::new(FileOutputEndpoint::new(config)?)))
            }
            _ => Ok(None),
        }
    }
}

#[cfg(feature = "with-kafka")]
struct KafkaOutputFactory;

#[cfg(feature = "with-kafka")]
impl OutputTransportEndpointFactory for KafkaOutputFactory {
    fn create(
        &self,
        config: &TransportConfig,
        endpoint_name: &str,
        fault_tolerant: bool,
    ) -> AnyResult<Option<Box<dyn OutputEndpoint>>> {
        match config {
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
}

#[cfg(feature = "with-redis")]
struct RedisOutputFactory;

#[cfg(feature = "with-redis")]
impl OutputTransportEndpointFactory for RedisOutputFactory {
    fn create(
        &self,
        config: &TransportConfig,
        _endpoint_name: &str,
        _fault_tolerant: bool,
    ) -> AnyResult<Option<Box<dyn OutputEndpoint>>> {
        match config {
            TransportConfig::RedisOutput(config) => {
                Ok(Some(Box::new(RedisOutputEndpoint::new(config)?)))
            }
            _ => Ok(None),
        }
    }
}

struct NullOutputFactory;

impl OutputTransportEndpointFactory for NullOutputFactory {
    fn create(
        &self,
        config: &TransportConfig,
        _endpoint_name: &str,
        _fault_tolerant: bool,
    ) -> AnyResult<Option<Box<dyn OutputEndpoint>>> {
        match config {
            TransportConfig::NullOutput => Ok(Some(Box::new(NullOutputEndpoint))),
            _ => Ok(None),
        }
    }
}

/// Creates an input transport endpoint instance using an input transport
/// configuration, resolving secrets by reading `secrets_dir`.
///
/// Returns an error if there is a invalid configuration for the endpoint.
/// Returns `None` if the transport configuration variant is incompatible with an input endpoint.
#[allow(unused_variables)]
pub fn input_transport_config_to_endpoint(
    config: &TransportConfig,
    endpoint_name: &str,
    secrets_dir: &Path,
) -> AnyResult<Option<Box<dyn TransportInputEndpoint>>> {
    let config = resolve_secret_references_via_json(secrets_dir, config)?;
    builtin_input_transport_registry().create_endpoint(&config)
}

/// Creates an output transport endpoint instance using an output transport
/// configuration, resolving secrets by reading `secrets_dir`.
///
/// If `fault_tolerant` is true, this function attempts to create a
/// fault-tolerant output endpoint (but it will still return a non-FT endpoint
/// if that's all it can do).
///
/// Returns an error if there is a invalid configuration for the endpoint.
/// Returns `None` if the transport configuration variant is incompatible with an output endpoint.
#[allow(unused_variables)]
pub fn output_transport_config_to_endpoint(
    config: &TransportConfig,
    endpoint_name: &str,
    fault_tolerant: bool,
    secrets_dir: &Path,
) -> AnyResult<Option<Box<dyn OutputEndpoint>>> {
    let config = resolve_secret_references_via_json(secrets_dir, config)?;
    builtin_output_transport_registry().create_endpoint(&config, endpoint_name, fault_tolerant)
}

#[cfg(test)]
mod tests {
    use super::*;
    use feldera_types::config::FtModel;

    #[test]
    fn builtin_input_registry_creates_empty_input_endpoint() {
        let secrets_dir = tempfile::tempdir().unwrap();

        let endpoint = input_transport_config_to_endpoint(
            &TransportConfig::EmptyInput,
            "empty",
            secrets_dir.path(),
        )
        .unwrap()
        .unwrap();

        assert_eq!(endpoint.fault_tolerance(), Some(FtModel::ExactlyOnce));
    }

    #[test]
    fn builtin_output_registry_creates_null_output_endpoint() {
        let secrets_dir = tempfile::tempdir().unwrap();

        let endpoint = output_transport_config_to_endpoint(
            &TransportConfig::NullOutput,
            "null",
            true,
            secrets_dir.path(),
        )
        .unwrap()
        .unwrap();

        assert!(endpoint.is_fault_tolerant());
        assert_eq!(endpoint.max_buffer_size_bytes(), usize::MAX);
    }

    #[test]
    fn wrong_direction_transport_configs_still_return_none() {
        let secrets_dir = tempfile::tempdir().unwrap();

        assert!(
            input_transport_config_to_endpoint(
                &TransportConfig::NullOutput,
                "null",
                secrets_dir.path()
            )
            .unwrap()
            .is_none()
        );
        assert!(
            output_transport_config_to_endpoint(
                &TransportConfig::EmptyInput,
                "empty",
                false,
                secrets_dir.path()
            )
            .unwrap()
            .is_none()
        );
    }

    #[test]
    fn explicit_transport_registries_dispatch_by_transport_name() {
        let mut input_registry = InputTransportRegistry::new();
        assert!(
            input_registry
                .create_endpoint(&TransportConfig::EmptyInput)
                .unwrap()
                .is_none()
        );
        input_registry.register(TransportConfig::EMPTY_INPUT, Box::new(EmptyInputFactory));
        assert!(
            input_registry
                .create_endpoint(&TransportConfig::EmptyInput)
                .unwrap()
                .is_some()
        );

        let mut output_registry = OutputTransportRegistry::new();
        assert!(
            output_registry
                .create_endpoint(&TransportConfig::NullOutput, "null", true)
                .unwrap()
                .is_none()
        );
        output_registry.register(TransportConfig::NULL_OUTPUT, Box::new(NullOutputFactory));
        assert!(
            output_registry
                .create_endpoint(&TransportConfig::NullOutput, "null", true)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn builtin_transport_registries_include_compiled_transport_names() {
        let input_registry = builtin_input_transport_registry();
        assert!(input_registry.get(TransportConfig::FILE_INPUT).is_some());
        #[cfg(feature = "with-kafka")]
        assert!(input_registry.get(TransportConfig::KAFKA_INPUT).is_some());
        #[cfg(feature = "with-nats")]
        assert!(input_registry.get(TransportConfig::NATS_INPUT).is_some());
        #[cfg(feature = "with-pubsub")]
        assert!(input_registry.get(TransportConfig::PUB_SUB_INPUT).is_some());
        assert!(input_registry.get(TransportConfig::URL_INPUT).is_some());
        assert!(input_registry.get(TransportConfig::S3_INPUT).is_some());
        assert!(input_registry.get(TransportConfig::DATAGEN).is_some());
        #[cfg(feature = "with-nexmark")]
        assert!(input_registry.get(TransportConfig::NEXMARK).is_some());
        assert!(input_registry.get(TransportConfig::HTTP_INPUT).is_some());
        assert!(input_registry.get(TransportConfig::ADHOC_INPUT).is_some());
        assert!(input_registry.get(TransportConfig::CLOCK).is_some());
        assert!(input_registry.get(TransportConfig::EMPTY_INPUT).is_some());

        let output_registry = builtin_output_transport_registry();
        assert!(output_registry.get(TransportConfig::FILE_OUTPUT).is_some());
        #[cfg(feature = "with-kafka")]
        assert!(output_registry.get(TransportConfig::KAFKA_OUTPUT).is_some());
        #[cfg(feature = "with-redis")]
        assert!(output_registry.get(TransportConfig::REDIS_OUTPUT).is_some());
        assert!(output_registry.get(TransportConfig::NULL_OUTPUT).is_some());
    }
}
