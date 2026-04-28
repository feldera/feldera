use anyhow::Result as AnyResult;
use feldera_adapterlib::connector::{ConnectorDescriptor, ConnectorFlags, ConnectorKind, Direction};
use feldera_adapterlib::transport::{InputConsumer, IntegratedInputEndpoint};
use feldera_types::transport::iceberg::IcebergReaderConfig;
use serde_json::Value as JsonValue;

fn iceberg_input_config_schema() -> JsonValue {
    JsonValue::Object(Default::default())
}

fn build_iceberg_input(
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

static ICEBERG_INPUT_DESCRIPTOR: ConnectorDescriptor = ConnectorDescriptor {
    name: "iceberg_input",
    direction: Direction::Input,
    kind: ConnectorKind::Integrated,
    fault_tolerance: None,
    config_schema: iceberg_input_config_schema,
    default_format: None,
    flags: ConnectorFlags::EMPTY,
    build_input: None,
    build_output: None,
    build_integrated_input: Some(build_iceberg_input),
    build_integrated_output: None,
};

inventory::submit! { &ICEBERG_INPUT_DESCRIPTOR }

#[cfg(test)]
mod registry_tests {
    #[test]
    fn iceberg_input_descriptor() {
        let d = feldera_adapterlib::connector::connector_by_name("iceberg_input")
            .expect("iceberg_input descriptor not registered");
        assert!(d.build_integrated_input.is_some());
        assert!(d.build_integrated_output.is_none());
    }
}
