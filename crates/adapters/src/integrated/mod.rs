use crate::controller::{ControllerInner, EndpointId};
use crate::{ControllerError, Encoder, OutputEndpoint};
use pipeline_types::config::{OutputEndpointConfig, TransportConfig, TransportConfigVariant};
use pipeline_types::program_schema::Relation;
use std::sync::Weak;

#[cfg(feature = "with-deltalake")]
mod delta_table;

#[cfg(feature = "with-deltalake")]
pub use delta_table::DeltaTableWriter;

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
    config: &OutputEndpointConfig,
    schema: &Relation,
    controller: Weak<ControllerInner>,
) -> Result<Box<dyn IntegratedOutputEndpoint>, ControllerError> {
    match &config.connector_config.transport {
        #[cfg(feature = "with-deltalake")]
        TransportConfig::DeltaTableOutput(config) => Ok(Box::new(DeltaTableWriter::new(
            endpoint_id,
            endpoint_name,
            config,
            schema,
            controller,
        )?)),
        transport => Err(ControllerError::unknown_output_transport(
            endpoint_name,
            &transport.name(),
        )),
    }
}
