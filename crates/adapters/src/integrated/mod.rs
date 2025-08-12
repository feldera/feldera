// Feature gating causes warnings
#![allow(unused_imports)]
#![allow(unused_variables)]
#![allow(unreachable_code)]

use crate::controller::{ControllerInner, EndpointId};
use crate::transport::IntegratedInputEndpoint;
use crate::{ControllerError, Encoder, InputConsumer, OutputEndpoint};
use feldera_types::config::{ConnectorConfig, TransportConfig};
use feldera_types::program_schema::Relation;
use postgres::PostgresOutputEndpoint;
use std::sync::Weak;

#[cfg(feature = "with-deltalake")]
mod delta_table;
mod postgres;

#[cfg(feature = "with-deltalake")]
pub use delta_table::{DeltaTableInputEndpoint, DeltaTableWriter};

#[cfg(feature = "with-deltalake")]
use feldera_types::config::TransportConfig::DeltaTableInput;

use crate::integrated::postgres::PostgresInputEndpoint;
#[cfg(feature = "with-iceberg")]
use feldera_types::config::TransportConfig::IcebergInput;
use feldera_types::config::TransportConfig::PostgresInput;

/// An integrated output connector implements both transport endpoint
/// (`OutputEndpoint`) and `Encoder` traits.  It is used to implement
/// connectors whose transport protocol and data format are tightly coupled.
pub trait IntegratedOutputEndpoint: OutputEndpoint + Encoder {
    fn into_encoder(self: Box<Self>) -> Box<dyn Encoder>;
    fn as_endpoint(&mut self) -> &mut dyn OutputEndpoint;
}

impl<EP> IntegratedOutputEndpoint for EP
where
    EP: OutputEndpoint + Encoder + 'static,
{
    fn into_encoder(self: Box<Self>) -> Box<dyn Encoder> {
        self
    }

    fn as_endpoint(&mut self) -> &mut dyn OutputEndpoint {
        self
    }
}

/// Create an instance of an integrated output endpoint given its config
/// and output relation schema.
pub fn create_integrated_output_endpoint(
    endpoint_id: EndpointId,
    endpoint_name: &str,

    connector_config: &ConnectorConfig,
    key_schema: &Option<Relation>,
    schema: &Relation,
    controller: Weak<ControllerInner>,
) -> Result<Box<dyn IntegratedOutputEndpoint>, ControllerError> {
    let ep: Box<dyn IntegratedOutputEndpoint> = match &connector_config.transport {
        #[cfg(feature = "with-deltalake")]
        TransportConfig::DeltaTableOutput(config) => Box::new(DeltaTableWriter::new(
            endpoint_id,
            endpoint_name,
            config,
            key_schema,
            schema,
            controller,
        )?),
        TransportConfig::PostgresOutput(config) => Box::new(PostgresOutputEndpoint::new(
            endpoint_id,
            endpoint_name,
            config,
            key_schema,
            schema,
            controller,
        )?),
        transport => {
            return Err(ControllerError::unknown_output_transport(
                endpoint_name,
                &transport.name(),
            ));
        }
    };

    if connector_config.format.is_some() {
        return Err(ControllerError::invalid_parser_configuration(
            endpoint_name,
            &format!(
                "{} transport does not allow 'format' specification",
                connector_config.transport.name()
            ),
        ));
    }

    Ok(ep)
}

pub fn create_integrated_input_endpoint(
    endpoint_name: &str,
    config: &ConnectorConfig,
    consumer: Box<dyn InputConsumer>,
) -> Result<Box<dyn IntegratedInputEndpoint>, ControllerError> {
    let ep: Box<dyn IntegratedInputEndpoint> = match &config.transport {
        #[cfg(feature = "with-deltalake")]
        DeltaTableInput(config) => Box::new(DeltaTableInputEndpoint::new(
            endpoint_name,
            config,
            consumer,
        )),
        #[cfg(feature = "with-iceberg")]
        IcebergInput(config) => Box::new(feldera_iceberg::IcebergInputEndpoint::new(
            endpoint_name,
            config,
            consumer,
        )),
        PostgresInput(config) => {
            Box::new(PostgresInputEndpoint::new(endpoint_name, config, consumer))
        }
        transport => {
            return Err(ControllerError::unknown_input_transport(
                endpoint_name,
                &transport.name(),
            ));
        }
    };

    if config.format.is_some() {
        return Err(ControllerError::invalid_parser_configuration(
            endpoint_name,
            &format!(
                "{} transport does not allow 'format' specification",
                config.transport.name()
            ),
        ));
    }

    Ok(ep)
}
