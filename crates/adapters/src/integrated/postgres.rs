mod error;
mod input;
mod output;
mod output_macros;
mod prepared_statements;
mod tls;

#[cfg(feature = "with-postgres-cdc")]
pub(crate) mod cdc_input;

#[cfg(test)]
mod test;

pub use input::PostgresInputEndpoint;
pub use output::PostgresOutputEndpoint;

#[cfg(feature = "with-postgres-cdc")]
pub use cdc_input::PostgresCdcInputEndpoint;

use anyhow::Result as AnyResult;
use feldera_adapterlib::connector::{
    ConnectorDescriptor, ConnectorFlags, ConnectorKind, Direction, OutputControllerRef,
};
use feldera_adapterlib::transport::{InputConsumer, IntegratedInputEndpoint, IntegratedOutputEndpoint};
use feldera_types::config::FtModel;
use feldera_types::program_schema::Relation;
use feldera_types::transport::postgres::{PostgresReaderConfig, PostgresWriterConfig};
use serde_json::Value as JsonValue;
use std::sync::Arc;

fn postgres_input_config_schema() -> JsonValue {
    JsonValue::Object(Default::default())
}

fn build_postgres_input(
    config: &JsonValue,
    endpoint_name: &str,
    consumer: Box<dyn InputConsumer>,
) -> AnyResult<Box<dyn IntegratedInputEndpoint>> {
    let config: PostgresReaderConfig = serde_json::from_value(config.clone())?;
    Ok(Box::new(PostgresInputEndpoint::new(endpoint_name, &config, consumer)))
}

static POSTGRES_INPUT_DESCRIPTOR: ConnectorDescriptor = ConnectorDescriptor {
    name: "postgres_input",
    direction: Direction::Input,
    kind: ConnectorKind::Integrated,
    fault_tolerance: None,
    config_schema: postgres_input_config_schema,
    default_format: None,
    flags: ConnectorFlags::EMPTY,
    build_input: None,
    build_output: None,
    build_integrated_input: Some(build_postgres_input),
    build_integrated_output: None,
};

inventory::submit! { &POSTGRES_INPUT_DESCRIPTOR }

fn postgres_output_config_schema() -> JsonValue {
    JsonValue::Object(Default::default())
}

fn build_postgres_output(
    endpoint_id: u64,
    endpoint_name: &str,
    config: &JsonValue,
    key_schema: &Option<Relation>,
    schema: &Relation,
    controller: Arc<dyn OutputControllerRef>,
    _is_restart: bool,
) -> AnyResult<Box<dyn IntegratedOutputEndpoint>> {
    let config: PostgresWriterConfig = serde_json::from_value(config.clone())?;
    Ok(Box::new(PostgresOutputEndpoint::new(
        endpoint_id,
        endpoint_name,
        &config,
        key_schema,
        schema,
        controller,
    )?))
}

static POSTGRES_OUTPUT_DESCRIPTOR: ConnectorDescriptor = ConnectorDescriptor {
    name: "postgres_output",
    direction: Direction::Output,
    kind: ConnectorKind::Integrated,
    fault_tolerance: None,
    config_schema: postgres_output_config_schema,
    default_format: None,
    flags: ConnectorFlags::EMPTY,
    build_input: None,
    build_output: None,
    build_integrated_input: None,
    build_integrated_output: Some(build_postgres_output),
};

inventory::submit! { &POSTGRES_OUTPUT_DESCRIPTOR }

#[cfg(feature = "with-postgres-cdc")]
mod postgres_cdc_descriptor {
    use super::*;
    use feldera_types::transport::postgres::PostgresCdcReaderConfig;

    fn postgres_cdc_input_config_schema() -> JsonValue {
        JsonValue::Object(Default::default())
    }

    fn build_postgres_cdc_input(
        config: &JsonValue,
        endpoint_name: &str,
        consumer: Box<dyn InputConsumer>,
    ) -> AnyResult<Box<dyn IntegratedInputEndpoint>> {
        let config: PostgresCdcReaderConfig = serde_json::from_value(config.clone())?;
        Ok(Box::new(PostgresCdcInputEndpoint::new(endpoint_name, &config, consumer)))
    }

    pub static POSTGRES_CDC_INPUT_DESCRIPTOR: ConnectorDescriptor = ConnectorDescriptor {
        name: "postgres_cdc_input",
        direction: Direction::Input,
        kind: ConnectorKind::Integrated,
        fault_tolerance: Some(FtModel::AtLeastOnce),
        config_schema: postgres_cdc_input_config_schema,
        default_format: None,
        flags: ConnectorFlags::EMPTY,
        build_input: None,
        build_output: None,
        build_integrated_input: Some(build_postgres_cdc_input),
        build_integrated_output: None,
    };

    inventory::submit! { &POSTGRES_CDC_INPUT_DESCRIPTOR }
}

#[cfg(test)]
mod registry_tests {
    #[test]
    fn postgres_input_descriptor() {
        let d = feldera_adapterlib::connector::connector_by_name("postgres_input")
            .expect("postgres_input descriptor not registered");
        assert!(d.build_integrated_input.is_some());
        assert!(d.build_integrated_output.is_none());
    }

    #[test]
    fn postgres_output_descriptor() {
        let d = feldera_adapterlib::connector::connector_by_name("postgres_output")
            .expect("postgres_output descriptor not registered");
        assert!(d.build_integrated_input.is_none());
        assert!(d.build_integrated_output.is_some());
    }

    #[cfg(feature = "with-postgres-cdc")]
    #[test]
    fn postgres_cdc_input_descriptor() {
        let d = feldera_adapterlib::connector::connector_by_name("postgres_cdc_input")
            .expect("postgres_cdc_input descriptor not registered");
        assert!(d.build_integrated_input.is_some());
        assert!(d.build_integrated_output.is_none());
    }
}
