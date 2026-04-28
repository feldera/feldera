mod input;
mod output;

#[cfg(test)]
mod test;

use anyhow::Result as AnyResult;
use feldera_adapterlib::connector::{
    ConnectorDescriptor, ConnectorFlags, ConnectorKind, Direction, OutputControllerRef,
};
use feldera_adapterlib::transport::{InputConsumer, IntegratedInputEndpoint, IntegratedOutputEndpoint};
use feldera_types::config::FtModel;
use feldera_types::program_schema::Relation;
use feldera_types::transport::delta_table::{DeltaTableReaderConfig, DeltaTableWriterConfig};
use serde_json::Value as JsonValue;
use std::sync::Arc;

fn delta_table_input_config_schema() -> JsonValue {
    JsonValue::Object(Default::default())
}

fn build_delta_table_input(
    config: &JsonValue,
    endpoint_name: &str,
    consumer: Box<dyn InputConsumer>,
) -> AnyResult<Box<dyn IntegratedInputEndpoint>> {
    let config: DeltaTableReaderConfig = serde_json::from_value(config.clone())?;
    Ok(Box::new(DeltaTableInputEndpoint::new(endpoint_name, &config, consumer)))
}

static DELTA_TABLE_INPUT_DESCRIPTOR: ConnectorDescriptor = ConnectorDescriptor {
    name: "delta_table_input",
    direction: Direction::Input,
    kind: ConnectorKind::Integrated,
    fault_tolerance: Some(FtModel::AtLeastOnce),
    config_schema: delta_table_input_config_schema,
    default_format: None,
    flags: ConnectorFlags::EMPTY,
    build_input: None,
    build_output: None,
    build_integrated_input: Some(build_delta_table_input),
    build_integrated_output: None,
};

inventory::submit! { &DELTA_TABLE_INPUT_DESCRIPTOR }

fn delta_table_output_config_schema() -> JsonValue {
    JsonValue::Object(Default::default())
}

fn build_delta_table_output(
    endpoint_id: u64,
    endpoint_name: &str,
    config: &JsonValue,
    key_schema: &Option<Relation>,
    schema: &Relation,
    controller: Arc<dyn OutputControllerRef>,
    is_restart: bool,
) -> AnyResult<Box<dyn IntegratedOutputEndpoint>> {
    let config: DeltaTableWriterConfig = serde_json::from_value(config.clone())?;
    Ok(Box::new(DeltaTableWriter::new(
        endpoint_id,
        endpoint_name,
        &config,
        key_schema,
        schema,
        controller,
        is_restart,
    )?))
}

static DELTA_TABLE_OUTPUT_DESCRIPTOR: ConnectorDescriptor = ConnectorDescriptor {
    name: "delta_table_output",
    direction: Direction::Output,
    kind: ConnectorKind::Integrated,
    fault_tolerance: None,
    config_schema: delta_table_output_config_schema,
    default_format: None,
    flags: ConnectorFlags::EMPTY,
    build_input: None,
    build_output: None,
    build_integrated_input: None,
    build_integrated_output: Some(build_delta_table_output),
};

inventory::submit! { &DELTA_TABLE_OUTPUT_DESCRIPTOR }

#[cfg(test)]
mod registry_tests {
    #[test]
    fn delta_table_input_descriptor() {
        let d = feldera_adapterlib::connector::connector_by_name("delta_table_input")
            .expect("delta_table_input descriptor not registered");
        assert!(d.build_integrated_input.is_some());
        assert!(d.build_integrated_output.is_none());
    }

    #[test]
    fn delta_table_output_descriptor() {
        let d = feldera_adapterlib::connector::connector_by_name("delta_table_output")
            .expect("delta_table_output descriptor not registered");
        assert!(d.build_integrated_input.is_none());
        assert!(d.build_integrated_output.is_some());
    }
}

use feldera_types::serde_with_context::serde_config::{DecimalFormat, UuidFormat};
use feldera_types::serde_with_context::{DateFormat, SqlSerdeConfig, TimestampFormat};
pub use input::DeltaTableInputEndpoint;
pub use output::DeltaTableWriter;
use std::sync::Once;

static REGISTER_STORAGE_HANDLERS: Once = Once::new();

/// Register url handlers, so URL's like `s3://...` are recognized.
///
/// Runs initialization at most once per process.
pub fn register_storage_handlers() {
    REGISTER_STORAGE_HANDLERS.call_once(|| {
        deltalake::aws::register_handlers(None);
        deltalake::azure::register_handlers(None);
        deltalake::gcp::register_handlers(None);
        deltalake::unity_catalog::register_handlers(None);
    });
}

pub fn delta_input_serde_config() -> SqlSerdeConfig {
    SqlSerdeConfig::default()
        // Delta standard specifies that the timestamp type uses microseconds.
        // In reality, delta parquet files can represent timestamps as either
        // microseconds or nanoseconds.  Other options might be possible.
        // `serde_arrow` knows the correct type from the Arrow schema, and its
        // `Deserializer` implementation is nice enough to return the timestamp
        // formatted as string if the `Deserialize` implementation asks for it
        // (by calling `deserialize_str`), so we rely on that instead of trying
        // to deserialize the timestamp as an integer.  A better solution would
        // require a more flexible SqlSerdeConfig type that would specify a
        // schema per field.
        .with_timestamp_format(TimestampFormat::String("%Y-%m-%dT%H:%M:%S%.f%Z"))
        .with_date_format(DateFormat::DaysSinceEpoch)
        .with_decimal_format(DecimalFormat::String)
        // DeltaLake doesn't have a native UUID type. We assume that UUID
        // is represented as a string. If a different representation is used, the user
        // will have to deserialize into VARBINARY first. Alternatively, we can make
        // this configurable.
        .with_uuid_format(UuidFormat::String)
}
