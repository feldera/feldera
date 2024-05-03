mod input;
mod output;

#[cfg(test)]
mod test;

pub use input::DeltaTableInputEndpoint;
pub use output::DeltaTableWriter;
use pipeline_types::serde_with_context::serde_config::DecimalFormat;
use pipeline_types::serde_with_context::{DateFormat, SqlSerdeConfig, TimeFormat, TimestampFormat};
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
}
