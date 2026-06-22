use crate::controller::{ControllerInner, EndpointId};
use crate::transport::IntegratedInputEndpoint;
use crate::{ControllerError, Encoder, InputConsumer, OutputEndpoint};
use datafusion::execution::runtime_env::RuntimeEnv;
use feldera_types::config::{ConnectorConfig, PipelineConfig, TransportConfig};
use feldera_types::program_schema::Relation;
use std::{
    collections::BTreeMap,
    sync::{Arc, Weak},
};

#[cfg(feature = "with-deltalake")]
pub mod delta_table;
#[cfg(feature = "with-dynamodb")]
mod dynamodb;
mod postgres;

#[cfg(feature = "with-dynamodb")]
pub use crate::integrated::dynamodb::DynamoDBOutputEndpoint;
#[cfg(feature = "with-postgres-cdc")]
use crate::integrated::postgres::PostgresCdcInputEndpoint;
use crate::integrated::postgres::PostgresInputEndpoint;
pub use crate::integrated::postgres::PostgresOutputEndpoint;

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

/// Factory for creating integrated output endpoints from connector configuration.
pub trait IntegratedOutputEndpointFactory: Send + Sync {
    #[allow(clippy::too_many_arguments)]
    fn create(
        &self,
        endpoint_id: EndpointId,
        endpoint_name: &str,
        connector_config: &ConnectorConfig,
        key_schema: &Option<Relation>,
        schema: &Relation,
        controller: Weak<ControllerInner>,
        continue_previous_state: bool,
        is_index: bool,
    ) -> Result<Option<Box<dyn IntegratedOutputEndpoint>>, ControllerError>;
}

/// Registry of integrated output endpoint factories keyed by transport name.
#[derive(Default)]
pub struct IntegratedOutputEndpointRegistry {
    registered: BTreeMap<&'static str, Arc<dyn IntegratedOutputEndpointFactory>>,
}

impl IntegratedOutputEndpointRegistry {
    pub fn new() -> Self {
        Self {
            registered: BTreeMap::new(),
        }
    }

    pub fn register(
        &mut self,
        name: &'static str,
        factory: Box<dyn IntegratedOutputEndpointFactory>,
    ) {
        self.registered.insert(name, Arc::from(factory));
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn IntegratedOutputEndpointFactory>> {
        self.registered.get(name).cloned()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create_endpoint(
        &self,
        endpoint_id: EndpointId,
        endpoint_name: &str,
        connector_config: &ConnectorConfig,
        key_schema: &Option<Relation>,
        schema: &Relation,
        controller: Weak<ControllerInner>,
        continue_previous_state: bool,
        is_index: bool,
    ) -> Result<Option<Box<dyn IntegratedOutputEndpoint>>, ControllerError> {
        let Some(factory) = self.get(&connector_config.transport.name()) else {
            return Ok(None);
        };
        factory.create(
            endpoint_id,
            endpoint_name,
            connector_config,
            key_schema,
            schema,
            controller,
            continue_previous_state,
            is_index,
        )
    }
}

/// Factory for creating integrated input endpoints from connector configuration.
pub trait IntegratedInputEndpointFactory: Send + Sync {
    fn create(
        &self,
        endpoint_name: &str,
        config: &ConnectorConfig,
        pipeline_config: &PipelineConfig,
        runtime_env: Arc<RuntimeEnv>,
        consumer: Box<dyn InputConsumer>,
    ) -> Result<Option<Box<dyn IntegratedInputEndpoint>>, ControllerError>;
}

/// Registry of integrated input endpoint factories keyed by transport name.
#[derive(Default)]
pub struct IntegratedInputEndpointRegistry {
    registered: BTreeMap<&'static str, Arc<dyn IntegratedInputEndpointFactory>>,
}

impl IntegratedInputEndpointRegistry {
    pub fn new() -> Self {
        Self {
            registered: BTreeMap::new(),
        }
    }

    pub fn register(
        &mut self,
        name: &'static str,
        factory: Box<dyn IntegratedInputEndpointFactory>,
    ) {
        self.registered.insert(name, Arc::from(factory));
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn IntegratedInputEndpointFactory>> {
        self.registered.get(name).cloned()
    }

    pub fn create_endpoint(
        &self,
        endpoint_name: &str,
        config: &ConnectorConfig,
        pipeline_config: &PipelineConfig,
        runtime_env: Arc<RuntimeEnv>,
        consumer: Box<dyn InputConsumer>,
    ) -> Result<Option<Box<dyn IntegratedInputEndpoint>>, ControllerError> {
        let Some(factory) = self.get(&config.transport.name()) else {
            return Ok(None);
        };
        factory.create(
            endpoint_name,
            config,
            pipeline_config,
            runtime_env,
            consumer,
        )
    }
}

pub fn builtin_integrated_output_endpoint_registry() -> IntegratedOutputEndpointRegistry {
    let mut registry = IntegratedOutputEndpointRegistry::new();
    #[cfg(feature = "with-deltalake")]
    registry.register("delta_table_output", Box::new(DeltaTableOutputFactory));
    registry.register("postgres_output", Box::new(PostgresOutputFactory));
    #[cfg(feature = "with-dynamodb")]
    registry.register("dynamodb_output", Box::new(DynamoDBOutputFactory));
    registry
}

pub fn builtin_integrated_input_endpoint_registry() -> IntegratedInputEndpointRegistry {
    let mut registry = IntegratedInputEndpointRegistry::new();
    #[cfg(feature = "with-deltalake")]
    registry.register("delta_table_input", Box::new(DeltaTableInputFactory));
    #[cfg(feature = "with-iceberg")]
    registry.register("iceberg_input", Box::new(IcebergInputFactory));
    registry.register("postgres_input", Box::new(PostgresInputFactory));
    #[cfg(feature = "with-postgres-cdc")]
    registry.register("postgres_cdc_input", Box::new(PostgresCdcInputFactory));
    registry
}

#[cfg(feature = "with-deltalake")]
struct DeltaTableOutputFactory;

#[cfg(feature = "with-deltalake")]
impl IntegratedOutputEndpointFactory for DeltaTableOutputFactory {
    #[allow(clippy::too_many_arguments)]
    fn create(
        &self,
        endpoint_id: EndpointId,
        endpoint_name: &str,
        connector_config: &ConnectorConfig,
        key_schema: &Option<Relation>,
        schema: &Relation,
        controller: Weak<ControllerInner>,
        continue_previous_state: bool,
        is_index: bool,
    ) -> Result<Option<Box<dyn IntegratedOutputEndpoint>>, ControllerError> {
        match &connector_config.transport {
            TransportConfig::DeltaTableOutput(config) => {
                Ok(Some(Box::new(delta_table::DeltaTableWriter::new(
                    endpoint_id,
                    endpoint_name,
                    config,
                    key_schema,
                    schema,
                    controller,
                    continue_previous_state,
                    is_index,
                )?)))
            }
            _ => Ok(None),
        }
    }
}

struct PostgresOutputFactory;

impl IntegratedOutputEndpointFactory for PostgresOutputFactory {
    #[allow(clippy::too_many_arguments)]
    fn create(
        &self,
        endpoint_id: EndpointId,
        endpoint_name: &str,
        connector_config: &ConnectorConfig,
        key_schema: &Option<Relation>,
        schema: &Relation,
        controller: Weak<ControllerInner>,
        _continue_previous_state: bool,
        is_index: bool,
    ) -> Result<Option<Box<dyn IntegratedOutputEndpoint>>, ControllerError> {
        match &connector_config.transport {
            TransportConfig::PostgresOutput(config) => {
                Ok(Some(Box::new(PostgresOutputEndpoint::new(
                    endpoint_id,
                    endpoint_name,
                    config,
                    key_schema,
                    schema,
                    controller,
                    is_index,
                )?)))
            }
            _ => Ok(None),
        }
    }
}

#[cfg(feature = "with-dynamodb")]
struct DynamoDBOutputFactory;

#[cfg(feature = "with-dynamodb")]
impl IntegratedOutputEndpointFactory for DynamoDBOutputFactory {
    #[allow(clippy::too_many_arguments)]
    fn create(
        &self,
        endpoint_id: EndpointId,
        endpoint_name: &str,
        connector_config: &ConnectorConfig,
        key_schema: &Option<Relation>,
        schema: &Relation,
        controller: Weak<ControllerInner>,
        _continue_previous_state: bool,
        is_index: bool,
    ) -> Result<Option<Box<dyn IntegratedOutputEndpoint>>, ControllerError> {
        match &connector_config.transport {
            TransportConfig::DynamoDBOutput(config) => {
                Ok(Some(Box::new(DynamoDBOutputEndpoint::new(
                    endpoint_id,
                    endpoint_name,
                    config,
                    key_schema,
                    schema,
                    controller,
                    is_index,
                )?)))
            }
            _ => Ok(None),
        }
    }
}

#[cfg(feature = "with-deltalake")]
struct DeltaTableInputFactory;

#[cfg(feature = "with-deltalake")]
impl IntegratedInputEndpointFactory for DeltaTableInputFactory {
    fn create(
        &self,
        endpoint_name: &str,
        config: &ConnectorConfig,
        pipeline_config: &PipelineConfig,
        runtime_env: Arc<RuntimeEnv>,
        consumer: Box<dyn InputConsumer>,
    ) -> Result<Option<Box<dyn IntegratedInputEndpoint>>, ControllerError> {
        match &config.transport {
            TransportConfig::DeltaTableInput(config) => {
                Ok(Some(Box::new(delta_table::DeltaTableInputEndpoint::new(
                    endpoint_name,
                    config,
                    pipeline_config,
                    runtime_env,
                    consumer,
                ))))
            }
            _ => Ok(None),
        }
    }
}

#[cfg(feature = "with-iceberg")]
struct IcebergInputFactory;

#[cfg(feature = "with-iceberg")]
impl IntegratedInputEndpointFactory for IcebergInputFactory {
    fn create(
        &self,
        endpoint_name: &str,
        config: &ConnectorConfig,
        pipeline_config: &PipelineConfig,
        runtime_env: Arc<RuntimeEnv>,
        consumer: Box<dyn InputConsumer>,
    ) -> Result<Option<Box<dyn IntegratedInputEndpoint>>, ControllerError> {
        match &config.transport {
            TransportConfig::IcebergInput(config) => {
                Ok(Some(Box::new(feldera_iceberg::IcebergInputEndpoint::new(
                    endpoint_name,
                    config,
                    pipeline_config,
                    runtime_env,
                    consumer,
                ))))
            }
            _ => Ok(None),
        }
    }
}

struct PostgresInputFactory;

impl IntegratedInputEndpointFactory for PostgresInputFactory {
    fn create(
        &self,
        endpoint_name: &str,
        config: &ConnectorConfig,
        _pipeline_config: &PipelineConfig,
        _runtime_env: Arc<RuntimeEnv>,
        consumer: Box<dyn InputConsumer>,
    ) -> Result<Option<Box<dyn IntegratedInputEndpoint>>, ControllerError> {
        match &config.transport {
            TransportConfig::PostgresInput(config) => Ok(Some(Box::new(
                PostgresInputEndpoint::new(endpoint_name, config, consumer),
            ))),
            _ => Ok(None),
        }
    }
}

#[cfg(feature = "with-postgres-cdc")]
struct PostgresCdcInputFactory;

#[cfg(feature = "with-postgres-cdc")]
impl IntegratedInputEndpointFactory for PostgresCdcInputFactory {
    fn create(
        &self,
        endpoint_name: &str,
        config: &ConnectorConfig,
        _pipeline_config: &PipelineConfig,
        _runtime_env: Arc<RuntimeEnv>,
        consumer: Box<dyn InputConsumer>,
    ) -> Result<Option<Box<dyn IntegratedInputEndpoint>>, ControllerError> {
        match &config.transport {
            TransportConfig::PostgresCdcInput(config) => Ok(Some(Box::new(
                PostgresCdcInputEndpoint::new(endpoint_name, config, consumer),
            ))),
            _ => Ok(None),
        }
    }
}

/// Create an instance of an integrated output endpoint given its config
/// and output relation schema.
#[allow(unused, clippy::too_many_arguments)]
pub fn create_integrated_output_endpoint(
    endpoint_id: EndpointId,
    endpoint_name: &str,
    connector_config: &ConnectorConfig,
    key_schema: &Option<Relation>,
    schema: &Relation,
    controller: Weak<ControllerInner>,
    continue_previous_state: bool,
    is_index: bool,
) -> Result<Box<dyn IntegratedOutputEndpoint>, ControllerError> {
    let registry = builtin_integrated_output_endpoint_registry();
    create_integrated_output_endpoint_with_registry(
        &registry,
        endpoint_id,
        endpoint_name,
        connector_config,
        key_schema,
        schema,
        controller,
        continue_previous_state,
        is_index,
    )
}

#[allow(unused, clippy::too_many_arguments)]
pub fn create_integrated_output_endpoint_with_registry(
    registry: &IntegratedOutputEndpointRegistry,
    endpoint_id: EndpointId,
    endpoint_name: &str,
    connector_config: &ConnectorConfig,
    key_schema: &Option<Relation>,
    schema: &Relation,
    controller: Weak<ControllerInner>,
    continue_previous_state: bool,
    is_index: bool,
) -> Result<Box<dyn IntegratedOutputEndpoint>, ControllerError> {
    let ep = registry
        .create_endpoint(
            endpoint_id,
            endpoint_name,
            connector_config,
            key_schema,
            schema,
            controller,
            continue_previous_state,
            is_index,
        )?
        .ok_or_else(|| {
            ControllerError::unknown_output_transport(
                endpoint_name,
                &connector_config.transport.name(),
            )
        })?;

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

#[allow(unused_variables)]
pub fn create_integrated_input_endpoint(
    endpoint_name: &str,
    config: &ConnectorConfig,
    pipeline_config: &PipelineConfig,
    runtime_env: Arc<RuntimeEnv>,
    consumer: Box<dyn InputConsumer>,
) -> Result<Box<dyn IntegratedInputEndpoint>, ControllerError> {
    let registry = builtin_integrated_input_endpoint_registry();
    create_integrated_input_endpoint_with_registry(
        &registry,
        endpoint_name,
        config,
        pipeline_config,
        runtime_env,
        consumer,
    )
}

#[allow(unused_variables)]
pub fn create_integrated_input_endpoint_with_registry(
    registry: &IntegratedInputEndpointRegistry,
    endpoint_name: &str,
    config: &ConnectorConfig,
    pipeline_config: &PipelineConfig,
    runtime_env: Arc<RuntimeEnv>,
    consumer: Box<dyn InputConsumer>,
) -> Result<Box<dyn IntegratedInputEndpoint>, ControllerError> {
    let ep = registry
        .create_endpoint(
            endpoint_name,
            config,
            pipeline_config,
            runtime_env,
            consumer,
        )?
        .ok_or_else(|| {
            ControllerError::unknown_input_transport(endpoint_name, &config.transport.name())
        })?;

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
