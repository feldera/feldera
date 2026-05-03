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
use feldera_adapterlib::connector::OutputControllerRef;
use feldera_adapterlib::transport::{InputConsumer, IntegratedInputEndpoint, IntegratedOutputEndpoint};
use feldera_types::program_schema::Relation;
use feldera_types::transport::postgres::{PostgresReaderConfig, PostgresWriterConfig};
use serde_json::Value as JsonValue;
use std::sync::Arc;

fn postgres_input_config_schema() -> JsonValue {
    JsonValue::Object(Default::default())
}

pub fn build_postgres_input(
    config: &JsonValue,
    endpoint_name: &str,
    consumer: Box<dyn InputConsumer>,
) -> AnyResult<Box<dyn IntegratedInputEndpoint>> {
    let config: PostgresReaderConfig = serde_json::from_value(config.clone())?;
    Ok(Box::new(PostgresInputEndpoint::new(endpoint_name, &config, consumer)))
}

#[linkme::distributed_slice(feldera_adapterlib_meta::CONNECTOR_METADATA_REGISTRY)]
static POSTGRES_INPUT_META: feldera_adapterlib_meta::ConnectorDescriptor =
    feldera_adapterlib_meta::ConnectorDescriptor {
        name: "postgres_input",
        crate_name: env!("CARGO_CRATE_NAME"),
        direction: feldera_adapterlib_meta::Direction::Input,
        kind: feldera_adapterlib_meta::ConnectorKind::Integrated,
        fault_tolerance: None,
        config_schema: postgres_input_config_schema,
        default_format: None,
        flags: feldera_adapterlib_meta::ConnectorFlags::EMPTY,
    };

fn postgres_output_config_schema() -> JsonValue {
    JsonValue::Object(Default::default())
}

pub fn build_postgres_output(
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

#[linkme::distributed_slice(feldera_adapterlib_meta::CONNECTOR_METADATA_REGISTRY)]
static POSTGRES_OUTPUT_META: feldera_adapterlib_meta::ConnectorDescriptor =
    feldera_adapterlib_meta::ConnectorDescriptor {
        name: "postgres_output",
        crate_name: env!("CARGO_CRATE_NAME"),
        direction: feldera_adapterlib_meta::Direction::Output,
        kind: feldera_adapterlib_meta::ConnectorKind::Integrated,
        fault_tolerance: None,
        config_schema: postgres_output_config_schema,
        default_format: None,
        flags: feldera_adapterlib_meta::ConnectorFlags::EMPTY,
    };

#[cfg(feature = "with-postgres-cdc")]
mod postgres_cdc_descriptor {
    use super::*;
    use feldera_types::transport::postgres::PostgresCdcReaderConfig;

    fn postgres_cdc_input_config_schema() -> JsonValue {
        JsonValue::Object(Default::default())
    }

    pub fn build_postgres_cdc_input(
        config: &JsonValue,
        endpoint_name: &str,
        consumer: Box<dyn InputConsumer>,
    ) -> AnyResult<Box<dyn IntegratedInputEndpoint>> {
        let config: PostgresCdcReaderConfig = serde_json::from_value(config.clone())?;
        Ok(Box::new(PostgresCdcInputEndpoint::new(endpoint_name, &config, consumer)))
    }

    #[linkme::distributed_slice(feldera_adapterlib_meta::CONNECTOR_METADATA_REGISTRY)]
    pub(super) static POSTGRES_CDC_INPUT_META: feldera_adapterlib_meta::ConnectorDescriptor =
        feldera_adapterlib_meta::ConnectorDescriptor {
            name: "postgres_cdc_input",
            crate_name: env!("CARGO_CRATE_NAME"),
            direction: feldera_adapterlib_meta::Direction::Input,
            kind: feldera_adapterlib_meta::ConnectorKind::Integrated,
            fault_tolerance: Some(feldera_types::config::FtModel::AtLeastOnce),
            config_schema: postgres_cdc_input_config_schema,
            default_format: None,
            flags: feldera_adapterlib_meta::ConnectorFlags::EMPTY,
        };
}

#[cfg(feature = "with-postgres-cdc")]
pub use postgres_cdc_descriptor::build_postgres_cdc_input;

#[cfg(test)]
mod registry_tests {
    #[test]
    fn postgres_input_descriptor() {
        let d = feldera_adapterlib::meta::descriptor_by_name("postgres_input")
            .expect("postgres_input descriptor not registered");
        assert!(d.direction.allows_input());
    }

    #[test]
    fn postgres_output_descriptor() {
        let d = feldera_adapterlib::meta::descriptor_by_name("postgres_output")
            .expect("postgres_output descriptor not registered");
        assert!(d.direction.allows_output());
    }

    #[cfg(feature = "with-postgres-cdc")]
    #[test]
    fn postgres_cdc_input_descriptor() {
        let d = feldera_adapterlib::meta::descriptor_by_name("postgres_cdc_input")
            .expect("postgres_cdc_input descriptor not registered");
        assert!(d.direction.allows_input());
    }
}
