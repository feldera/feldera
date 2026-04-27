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

use anyhow::Result as AnyResult;
use feldera_adapterlib::connector_by_name;
use feldera_types::secret_resolver::resolve_secret_references_via_json;
use serde_json::Value as JsonValue;
pub mod adhoc;
mod file;
pub mod http;

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


#[cfg(test)]
pub use crate::transport::file::set_barrier;

pub use feldera_adapterlib::transport::*;

/// Extracts the inner config value from a `TransportConfig` as a `JsonValue`.
///
/// Serialises the whole `TransportConfig` (which has the form `{"name": "...", "config": {...}}`),
/// then returns the `"config"` field.  Unit variants (e.g. `HttpOutput`) have no `"config"` key;
/// those return `JsonValue::Null`.
pub(crate) fn transport_config_inner_as_json(config: &TransportConfig) -> AnyResult<JsonValue> {
    let json = serde_json::to_value(config)?;
    Ok(json.get("config").cloned().unwrap_or(JsonValue::Null))
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

    // Registry path: connectors registered via `ConnectorDescriptor` (file_input, clock, …).
    let name = config.name();
    if let Some(descriptor) = connector_by_name(&name) {
        return match descriptor.build_input {
            Some(build_fn) => {
                let config_value = transport_config_inner_as_json(&config)?;
                Ok(Some(build_fn(&config_value, endpoint_name, secrets_dir)?))
            }
            None => Ok(None),
        };
    }

    // Fallback match for connectors not yet migrated to the descriptor registry.
    match config {
        // All input connectors are now handled above via the descriptor registry.
        _ => Ok(None),
    }
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

    // Registry path: connectors registered via `ConnectorDescriptor` (file_output, …).
    let name = config.name();
    if let Some(descriptor) = connector_by_name(&name) {
        return match descriptor.build_output {
            Some(build_fn) => {
                let config_value = transport_config_inner_as_json(&config)?;
                Ok(Some(build_fn(
                    &config_value,
                    endpoint_name,
                    fault_tolerant,
                    secrets_dir,
                )?))
            }
            None => Ok(None),
        };
    }

    // Fallback match for connectors not yet migrated to the descriptor registry.
    match config {
        // file_output, redis_output, kafka_output are now handled above via the descriptor registry.
        _ => Ok(None),
    }
}
