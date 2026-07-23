mod arrow;
mod auth;
mod client;
mod input;
mod query;

pub use input::SnowflakeInputEndpoint;

use feldera_types::serde_with_context::{
    serde_config::{BinaryFormat, DecimalFormat, VariantFormat},
    DateFormat, SqlSerdeConfig, TimeFormat, TimestampFormat,
};

pub fn snowflake_input_serde_config() -> SqlSerdeConfig {
    let mut config = SqlSerdeConfig::default()
        .with_date_format(DateFormat::String("%Y-%m-%d"))
        .with_time_format(TimeFormat::NanosSigned)
        .with_timestamp_format(TimestampFormat::MicrosSinceEpoch)
        .with_decimal_format(DecimalFormat::String)
        .with_variant_format(VariantFormat::JsonString)
        .with_binary_format(BinaryFormat::Array);
    config.timestamp_tz_format = TimestampFormat::MicrosSinceEpoch;
    config
}
