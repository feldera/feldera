use anyhow::Result as AnyResult;
use feldera_adapterlib::transport::{InputConsumer, IntegratedInputEndpoint};
use feldera_types::transport::iceberg::IcebergReaderConfig;
use serde_json::Value as JsonValue;

fn iceberg_input_config_schema() -> JsonValue {
    JsonValue::Object(Default::default())
}

pub fn build_iceberg_input(
    config: &JsonValue,
    endpoint_name: &str,
    consumer: Box<dyn InputConsumer>,
) -> AnyResult<Box<dyn IntegratedInputEndpoint>> {
    let config: IcebergReaderConfig = serde_json::from_value(config.clone())?;
    Ok(Box::new(feldera_iceberg::IcebergInputEndpoint::new(
        endpoint_name,
        &config,
        consumer,
    )))
}

#[linkme::distributed_slice(feldera_adapterlib_meta::CONNECTOR_METADATA_REGISTRY)]
static ICEBERG_INPUT_META: feldera_adapterlib_meta::ConnectorDescriptor =
    feldera_adapterlib_meta::ConnectorDescriptor {
        name: "iceberg_input",
        crate_name: env!("CARGO_CRATE_NAME"),
        direction: feldera_adapterlib_meta::Direction::Input,
        kind: feldera_adapterlib_meta::ConnectorKind::Integrated,
        fault_tolerance: None,
        config_schema: iceberg_input_config_schema,
        default_format: None,
        flags: feldera_adapterlib_meta::ConnectorFlags::EMPTY,
    };

#[cfg(test)]
mod registry_tests {
    #[test]
    fn iceberg_input_descriptor() {
        let d = feldera_adapterlib::meta::descriptor_by_name("iceberg_input")
            .expect("iceberg_input descriptor not registered");
        assert!(d.direction.allows_input());
    }
}
