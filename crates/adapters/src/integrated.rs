use crate::controller::{ControllerInner, EndpointId};
use crate::transport::IntegratedInputEndpoint;
use crate::transport::transport_config_inner_as_json;
use crate::{ControllerError, InputConsumer};
use feldera_adapterlib::connector::OutputControllerRef;
use feldera_types::config::ConnectorConfig;
use feldera_types::program_schema::Relation;
use std::sync::{Arc, Weak};

#[cfg(feature = "with-deltalake")]
mod delta_table;
#[cfg(feature = "with-iceberg")]
mod iceberg;
mod postgres;

use feldera_adapterlib::transport::IntegratedOutputEndpoint;

#[cfg(feature = "with-deltalake")]
pub use delta_table::{build_delta_table_input, build_delta_table_output};
pub use postgres::{build_postgres_input, build_postgres_output};
#[cfg(feature = "with-postgres-cdc")]
pub use postgres::build_postgres_cdc_input;
#[cfg(feature = "with-iceberg")]
pub use iceberg::build_iceberg_input;

/// Helper: reject a connector config that specifies a `format` field.
fn check_no_format(
    endpoint_name: &str,
    connector_config: &ConnectorConfig,
    transport_name: &str,
) -> Result<(), ControllerError> {
    if connector_config.format.is_some() {
        Err(ControllerError::invalid_parser_configuration(
            endpoint_name,
            &format!("{transport_name} transport does not allow 'format' specification"),
        ))
    } else {
        Ok(())
    }
}

/// Create an instance of an integrated output endpoint given its config
/// and output relation schema.
///
/// Dispatches through `CONNECTOR_DISPATCH_REGISTRY` (populated by
/// pipeline-manager's per-pipeline codegen). In-crate test contexts that
/// run without codegen instantiate integrated endpoints by calling the
/// per-builder fns (e.g. `crate::build_postgres_output`)
/// directly.
pub fn create_integrated_output_endpoint(
    endpoint_id: EndpointId,
    endpoint_name: &str,
    connector_config: &ConnectorConfig,
    key_schema: &Option<Relation>,
    schema: &Relation,
    controller: Weak<ControllerInner>,
    is_restart: bool,
) -> Result<Box<dyn IntegratedOutputEndpoint>, ControllerError> {
    let name = connector_config.transport.name();
    check_no_format(endpoint_name, connector_config, &name)?;

    let config_value = transport_config_inner_as_json(&connector_config.transport)
        .map_err(|e| {
            ControllerError::invalid_transport_configuration(endpoint_name, &e.to_string())
        })?;
    let controller_arc = controller.upgrade().ok_or_else(|| {
        ControllerError::invalid_transport_configuration(
            endpoint_name,
            "controller has been dropped before endpoint construction",
        )
    })?;
    let controller_ref: Arc<dyn OutputControllerRef> = controller_arc;

    match feldera_adapterlib::transport::dispatch_integrated_output(
        name.as_str(),
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
    })? {
        Some(ep) => Ok(ep),
        None => Err(ControllerError::unknown_output_transport(
            endpoint_name,
            &name,
        )),
    }
}

/// Create an instance of an integrated input endpoint given its config and
/// a consumer. Dispatches through `CONNECTOR_DISPATCH_REGISTRY`. Test
/// contexts call the per-builder fns directly.
pub fn create_integrated_input_endpoint(
    endpoint_name: &str,
    config: &ConnectorConfig,
    consumer: Box<dyn InputConsumer>,
) -> Result<Box<dyn IntegratedInputEndpoint>, ControllerError> {
    let name = config.transport.name();
    check_no_format(endpoint_name, config, &name)?;

    let config_value = transport_config_inner_as_json(&config.transport)
        .map_err(|e| {
            ControllerError::invalid_transport_configuration(endpoint_name, &e.to_string())
        })?;

    match feldera_adapterlib::transport::dispatch_integrated_input(
        name.as_str(),
        &config_value,
        endpoint_name,
        consumer,
    )
    .map_err(|e| {
        ControllerError::invalid_transport_configuration(endpoint_name, &e.to_string())
    })? {
        Some(ep) => Ok(ep),
        None => Err(ControllerError::unknown_input_transport(
            endpoint_name,
            &name,
        )),
    }
}
