use crate::controller::{ControllerInner, EndpointId};
use crate::transport::IntegratedInputEndpoint;
use crate::transport::transport_config_inner_as_json;
use crate::{ControllerError, InputConsumer};
use feldera_adapterlib::connector::{OutputControllerRef, connector_by_name};
use feldera_types::config::ConnectorConfig;
use feldera_types::program_schema::Relation;
use std::sync::{Arc, Weak};

#[cfg(feature = "with-deltalake")]
pub mod delta_table;
#[cfg(feature = "with-iceberg")]
mod iceberg;
mod postgres;

pub use crate::integrated::postgres::PostgresOutputEndpoint;

use feldera_adapterlib::transport::IntegratedOutputEndpoint;

/// Create an instance of an integrated output endpoint given its config
/// and output relation schema.
pub fn create_integrated_output_endpoint(
    endpoint_id: EndpointId,
    endpoint_name: &str,
    connector_config: &ConnectorConfig,
    key_schema: &Option<Relation>,
    schema: &Relation,
    controller: Weak<ControllerInner>,
    is_restart: bool,
) -> Result<Box<dyn IntegratedOutputEndpoint>, ControllerError> {
    // Registry path: connectors registered via `ConnectorDescriptor`.
    let name = connector_config.transport.name();
    if let Some(descriptor) = connector_by_name(&name) {
        if let Some(build_fn) = descriptor.build_integrated_output {
            if connector_config.format.is_some() {
                return Err(ControllerError::invalid_parser_configuration(
                    endpoint_name,
                    &format!("{name} transport does not allow 'format' specification"),
                ));
            }
            let config_value = transport_config_inner_as_json(&connector_config.transport)
                .map_err(|e| {
                    ControllerError::invalid_transport_configuration(
                        endpoint_name,
                        &e.to_string(),
                    )
                })?;
            let controller_arc = controller.upgrade().ok_or_else(|| {
                ControllerError::invalid_transport_configuration(
                    endpoint_name,
                    "controller has been dropped before endpoint construction",
                )
            })?;
            let controller_ref: Arc<dyn OutputControllerRef> = controller_arc;
            let ep = build_fn(
                endpoint_id,
                endpoint_name,
                &config_value,
                key_schema,
                schema,
                controller_ref,
                is_restart,
            )
            .map_err(|e| {
                ControllerError::invalid_transport_configuration(endpoint_name, &e.to_string())
            })?;
            return Ok(ep);
        }
        // Descriptor found but no build_integrated_output — not an integrated output connector.
    }

    // All integrated output connectors are now handled above via the descriptor registry.
    Err(ControllerError::unknown_output_transport(
        endpoint_name,
        &connector_config.transport.name(),
    ))
}

pub fn create_integrated_input_endpoint(
    endpoint_name: &str,
    config: &ConnectorConfig,
    consumer: Box<dyn InputConsumer>,
) -> Result<Box<dyn IntegratedInputEndpoint>, ControllerError> {
    // Registry path: connectors registered via `ConnectorDescriptor`.
    let name = config.transport.name();
    if let Some(descriptor) = connector_by_name(&name) {
        if let Some(build_fn) = descriptor.build_integrated_input {
            if config.format.is_some() {
                return Err(ControllerError::invalid_parser_configuration(
                    endpoint_name,
                    &format!("{name} transport does not allow 'format' specification"),
                ));
            }
            let config_value = transport_config_inner_as_json(&config.transport)
                .map_err(|e| {
                    ControllerError::invalid_transport_configuration(
                        endpoint_name,
                        &e.to_string(),
                    )
                })?;
            let ep = build_fn(&config_value, endpoint_name, consumer)
                .map_err(|e| {
                    ControllerError::invalid_transport_configuration(
                        endpoint_name,
                        &e.to_string(),
                    )
                })?;
            return Ok(ep);
        }
        // Descriptor found but no build_integrated_input — not an integrated input connector.
    }

    // All integrated input connectors are now handled above via the descriptor registry.
    Err(ControllerError::unknown_input_transport(
        endpoint_name,
        &config.transport.name(),
    ))
}
