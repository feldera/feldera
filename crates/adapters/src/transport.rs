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
        let config = config.deserialize_config()?;
        Ok(Some(Box::new(FileInputEndpoint::new(config))))
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
        let config = config.deserialize_config()?;
        Ok(Some(Box::new(KafkaFtInputEndpoint::new(config)?)))
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
        let config = config.deserialize_config()?;
        Ok(Some(Box::new(NatsInputEndpoint::new(config)?)))
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
        let config = config.deserialize_config()?;
        Ok(Some(Box::new(PubSubInputEndpoint::new(config)?)))
    }
}

struct UrlInputFactory;

impl InputTransportEndpointFactory for UrlInputFactory {
    fn create(
        &self,
        config: &TransportConfig,
    ) -> AnyResult<Option<Box<dyn TransportInputEndpoint>>> {
        let config = config.deserialize_config()?;
        Ok(Some(Box::new(UrlInputEndpoint::new(config))))
    }
}

struct S3InputFactory;

impl InputTransportEndpointFactory for S3InputFactory {
    fn create(
        &self,
        config: &TransportConfig,
    ) -> AnyResult<Option<Box<dyn TransportInputEndpoint>>> {
        let config = config.deserialize_config()?;
        Ok(Some(Box::new(S3InputEndpoint::new(config)?)))
    }
}

struct DatagenInputFactory;

impl InputTransportEndpointFactory for DatagenInputFactory {
    fn create(
        &self,
        config: &TransportConfig,
    ) -> AnyResult<Option<Box<dyn TransportInputEndpoint>>> {
        let config = config.deserialize_config()?;
        Ok(Some(Box::new(GeneratorEndpoint::new(config))))
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
        let config = config.deserialize_config()?;
        Ok(Some(Box::new(NexmarkEndpoint::new(config))))
    }
}

struct HttpInputFactory;

impl InputTransportEndpointFactory for HttpInputFactory {
    fn create(
        &self,
        config: &TransportConfig,
    ) -> AnyResult<Option<Box<dyn TransportInputEndpoint>>> {
        let config = config.deserialize_config()?;
        Ok(Some(Box::new(HttpInputEndpoint::new(config))))
    }
}

struct AdHocInputFactory;

impl InputTransportEndpointFactory for AdHocInputFactory {
    fn create(
        &self,
        config: &TransportConfig,
    ) -> AnyResult<Option<Box<dyn TransportInputEndpoint>>> {
        let config = config.deserialize_config()?;
        Ok(Some(Box::new(AdHocInputEndpoint::new(config))))
    }
}

struct ClockInputFactory;

impl InputTransportEndpointFactory for ClockInputFactory {
    fn create(
        &self,
        config: &TransportConfig,
    ) -> AnyResult<Option<Box<dyn TransportInputEndpoint>>> {
        let config = config.deserialize_config()?;
        Ok(Some(Box::new(ClockEndpoint::new(config)?)))
    }
}

struct EmptyInputFactory;

impl InputTransportEndpointFactory for EmptyInputFactory {
    fn create(
        &self,
        config: &TransportConfig,
    ) -> AnyResult<Option<Box<dyn TransportInputEndpoint>>> {
        Ok(Some(Box::new(EmptyInputEndpoint)))
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
        let config = config.deserialize_config()?;
        Ok(Some(Box::new(FileOutputEndpoint::new(config)?)))
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
        let config = config.deserialize_config()?;
        match fault_tolerant {
            false => Ok(Some(Box::new(KafkaOutputEndpoint::new(
                config,
                endpoint_name,
            )?))),
            true => Ok(Some(Box::new(KafkaFtOutputEndpoint::new(config)?))),
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
        let config = config.deserialize_config()?;
        Ok(Some(Box::new(RedisOutputEndpoint::new(config)?)))
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
        Ok(Some(Box::new(NullOutputEndpoint)))
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
    let registry = builtin_input_transport_registry();
    input_transport_config_to_endpoint_with_registry(config, endpoint_name, secrets_dir, &registry)
}

fn input_transport_config_to_endpoint_with_registry(
    config: &TransportConfig,
    _endpoint_name: &str,
    secrets_dir: &Path,
    registry: &InputTransportRegistry,
) -> AnyResult<Option<Box<dyn TransportInputEndpoint>>> {
    let config = resolve_secret_references_via_json(secrets_dir, config)?;
    registry.create_endpoint(&config)
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
    let registry = builtin_output_transport_registry();
    output_transport_config_to_endpoint_with_registry(
        config,
        endpoint_name,
        fault_tolerant,
        secrets_dir,
        &registry,
    )
}

fn output_transport_config_to_endpoint_with_registry(
    config: &TransportConfig,
    endpoint_name: &str,
    fault_tolerant: bool,
    secrets_dir: &Path,
    registry: &OutputTransportRegistry,
) -> AnyResult<Option<Box<dyn OutputEndpoint>>> {
    let config = resolve_secret_references_via_json(secrets_dir, config)?;
    registry.create_endpoint(&config, endpoint_name, fault_tolerant)
}

#[cfg(test)]
mod tests {
    use super::*;
    use feldera_types::config::FtModel;
    use serde_json::json;
    use std::fs::{File, create_dir_all};
    use std::io::Write;
    use std::path::Path;

    struct SecretAssertingInputFactory;

    impl InputTransportEndpointFactory for SecretAssertingInputFactory {
        fn create(
            &self,
            config: &TransportConfig,
        ) -> AnyResult<Option<Box<dyn TransportInputEndpoint>>> {
            assert_eq!(config.config["path"], json!("resolved-input-path"));
            Ok(None)
        }
    }

    struct SecretAssertingOutputFactory;

    impl OutputTransportEndpointFactory for SecretAssertingOutputFactory {
        fn create(
            &self,
            config: &TransportConfig,
            _endpoint_name: &str,
            _fault_tolerant: bool,
        ) -> AnyResult<Option<Box<dyn OutputEndpoint>>> {
            assert_eq!(config.config["path"], json!("resolved-output-path"));
            Ok(None)
        }
    }

    fn write_test_secret(secrets_dir: &Path, key: &str, value: &str) {
        let name_dir = secrets_dir.join("kubernetes").join("transport");
        create_dir_all(&name_dir).unwrap();
        let mut file = File::create(name_dir.join(key)).unwrap();
        file.write_all(value.as_bytes()).unwrap();
    }

    #[test]
    fn builtin_input_registry_creates_empty_input_endpoint() {
        let secrets_dir = tempfile::tempdir().unwrap();

        let endpoint = input_transport_config_to_endpoint(
            &TransportConfig::without_config(TransportConfig::EMPTY_INPUT),
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
            &TransportConfig::without_config(TransportConfig::NULL_OUTPUT),
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
                &TransportConfig::without_config(TransportConfig::NULL_OUTPUT),
                "null",
                secrets_dir.path()
            )
            .unwrap()
            .is_none()
        );
        assert!(
            output_transport_config_to_endpoint(
                &TransportConfig::without_config(TransportConfig::EMPTY_INPUT),
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
                .create_endpoint(&TransportConfig::without_config(
                    TransportConfig::EMPTY_INPUT
                ))
                .unwrap()
                .is_none()
        );
        input_registry.register(TransportConfig::EMPTY_INPUT, Box::new(EmptyInputFactory));
        assert!(
            input_registry
                .create_endpoint(&TransportConfig::without_config(
                    TransportConfig::EMPTY_INPUT
                ))
                .unwrap()
                .is_some()
        );
        input_registry.register(String::from("dynamic_input"), Box::new(EmptyInputFactory));
        assert!(input_registry.get("dynamic_input").is_some());

        let mut output_registry = OutputTransportRegistry::new();
        assert!(
            output_registry
                .create_endpoint(
                    &TransportConfig::without_config(TransportConfig::NULL_OUTPUT),
                    "null",
                    true
                )
                .unwrap()
                .is_none()
        );
        output_registry.register(TransportConfig::NULL_OUTPUT, Box::new(NullOutputFactory));
        assert!(
            output_registry
                .create_endpoint(
                    &TransportConfig::without_config(TransportConfig::NULL_OUTPUT),
                    "null",
                    true
                )
                .unwrap()
                .is_some()
        );
        output_registry.register(String::from("dynamic_output"), Box::new(NullOutputFactory));
        assert!(output_registry.get("dynamic_output").is_some());
    }

    #[test]
    fn input_transport_secrets_are_resolved_before_factory_parsing() {
        let secrets_dir = tempfile::tempdir().unwrap();
        write_test_secret(secrets_dir.path(), "input", "resolved-input-path");

        let config = TransportConfig::new(
            "secret_asserting_input",
            json!({"path": "${secret:kubernetes:transport/input}"}),
        );

        let mut input_registry = InputTransportRegistry::new();
        input_registry.register(
            "secret_asserting_input",
            Box::new(SecretAssertingInputFactory),
        );

        assert!(
            input_transport_config_to_endpoint_with_registry(
                &config,
                "input",
                secrets_dir.path(),
                &input_registry,
            )
            .unwrap()
            .is_none()
        )
    }

    #[test]
    fn output_transport_secrets_are_resolved_before_factory_parsing() {
        let secrets_dir = tempfile::tempdir().unwrap();
        write_test_secret(secrets_dir.path(), "output", "resolved-output-path");

        let config = TransportConfig::new(
            "secret_asserting_output",
            json!({"path": "${secret:kubernetes:transport/output}"}),
        );

        let mut output_registry = OutputTransportRegistry::new();
        output_registry.register(
            "secret_asserting_output",
            Box::new(SecretAssertingOutputFactory),
        );

        assert!(
            output_transport_config_to_endpoint_with_registry(
                &config,
                "output",
                false,
                secrets_dir.path(),
                &output_registry,
            )
            .unwrap()
            .is_none()
        )
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
