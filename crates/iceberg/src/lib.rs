mod input;
mod share;

pub use input::IcebergInputEndpoint;

use feldera_types::serde_with_context::{
    serde_config::{DecimalFormat, UuidFormat},
    DateFormat, SqlSerdeConfig, TimestampFormat,
};

pub fn iceberg_input_serde_config() -> SqlSerdeConfig {
    SqlSerdeConfig::default()
        // Iceberg supports microsecond or nanosecond timestamps, and
        // `serde_arrow` gives the raw integer without its unit. The connector
        // converts each batch with `timestamps_to_micros` and checks at startup
        // that every TIMESTAMP column is an Arrow timestamp. Formatting each
        // value to a string and parsing it back, as before, cost a large part of
        // the parse time.
        .with_timestamp_format(TimestampFormat::MicrosSinceEpoch)
        .with_date_format(DateFormat::DaysSinceEpoch)
        // `order_columns` gives a decimal as its significand when the scale
        // matches; otherwise `serde_arrow` gives it as a string.
        .with_decimal_format(DecimalFormat::I128)
        // Iceberg stores UUID as a physical 16-byte fixed binary, so the
        // deserializer must read it as bytes, not a string.
        .with_uuid_format(UuidFormat::Binary)
}
