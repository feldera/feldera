mod input;

pub use input::IcebergInputEndpoint;

use feldera_types::serde_with_context::{
    serde_config::DecimalFormat, DateFormat, SqlSerdeConfig, TimestampFormat,
};

pub fn iceberg_input_serde_config() -> SqlSerdeConfig {
    SqlSerdeConfig::default()
        // Iceberg supports microsecond or nanosecond timestamps.
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
}
