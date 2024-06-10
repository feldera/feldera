//! (De)serializer configuration used for SQL records.
//!
//! The [`DeserializeWithContext`](`crate::serde_with_context::DeserializeWithContext`) and
//! [`SerializeWithContext`](`crate::serde_with_context::SerializeWithContext`) traits
//! provide a mechanism for building configurable deserializers and serializers.
//! This module defines the [`SqlSerdeConfig`] type used to specify the
//! encoding used for SQL types.  All input types used by a Feldera pipeline
//! must implement `DeserializeWithContext<SqlDeserializerConfig>`.
//! Likewise, all output types must implement
//! `SerializeWithContext<SqlSerializerConfig>`.

use crate::format::json::JsonFlavor;

/// Representation of the SQL `TIME` type.
#[derive(Clone, Debug)]
pub enum TimeFormat {
    // String formatted using the specified format string:
    // See [`chrono` documentation](https://docs.rs/chrono/0.4.31/chrono/format/strftime/)
    // for supported time formatting syntax.
    String(&'static str),
    /// Time specified in microseconds from the start of the day.
    Micros,
    /// Time specified in milliseconds from the start of the day.
    Millis,
    /// Time specified in nanoseconds from the start of the day.
    Nanos,
}

impl Default for TimeFormat {
    fn default() -> Self {
        Self::String("%H:%M:%S%.f")
    }
}

// Representation of the SQL `DATE` type.
#[derive(Clone, Debug)]
pub enum DateFormat {
    // String formatted using the specified format:
    // See [`chrono` documentation](https://docs.rs/chrono/0.4.31/chrono/format/strftime/)
    // for supported date formatting syntax.
    String(&'static str),
    /// Date specified as the number of days since UNIX epoch.
    DaysSinceEpoch,
}

impl Default for DateFormat {
    fn default() -> Self {
        Self::String("%Y-%m-%d")
    }
}

// Representation of the SQL `TIMESTAMP` type.
#[derive(Clone, Debug)]
pub enum TimestampFormat {
    /// String formatted using the specified format:
    /// See [`chrono` documentation](https://docs.rs/chrono/0.4.31/chrono/format/strftime/)
    /// for supported formatting syntax.
    String(&'static str),
    /// Time specified in milliseconds since UNIX epoch.
    MillisSinceEpoch,
    /// Time specified in microseconds since UNIX epoch.
    MicrosSinceEpoch,
}

impl Default for TimestampFormat {
    fn default() -> Self {
        Self::String("%F %T%.f")
    }
}

// Representation of the SQL `DECIMAL` type.
#[derive(Clone, Debug)]
pub enum DecimalFormat {
    String,
    U128,
}

impl Default for DecimalFormat {
    fn default() -> Self {
        Self::String
    }
}

/// Deserializer configuration for parsing SQL records.
#[derive(Clone, Default, Debug)]
pub struct SqlSerdeConfig {
    /// `TIME` format.
    pub time_format: TimeFormat,
    /// `DATE` format.
    pub date_format: DateFormat,
    /// `TIMESTAMP` format.
    pub timestamp_format: TimestampFormat,
    /// `DECIMAL` format.
    pub decimal_format: DecimalFormat,
}

impl SqlSerdeConfig {
    /// Create a `SqlSerdeConfig` for generating arrow data frames.
    /// We may want to make this configurable
    pub fn with_time_format(mut self, time_format: TimeFormat) -> Self {
        self.time_format = time_format;
        self
    }

    pub fn with_date_format(mut self, date_format: DateFormat) -> Self {
        self.date_format = date_format;
        self
    }

    pub fn with_timestamp_format(mut self, timestamp_format: TimestampFormat) -> Self {
        self.timestamp_format = timestamp_format;
        self
    }

    pub fn with_decimal_format(mut self, decimal_format: DecimalFormat) -> Self {
        self.decimal_format = decimal_format;
        self
    }
}

impl From<JsonFlavor> for SqlSerdeConfig {
    fn from(flavor: JsonFlavor) -> Self {
        match flavor {
            JsonFlavor::Default => Default::default(),
            JsonFlavor::KafkaConnectJsonConverter { .. } => Self {
                time_format: TimeFormat::Millis,
                date_format: DateFormat::DaysSinceEpoch,
                timestamp_format: TimestampFormat::MillisSinceEpoch,
                decimal_format: DecimalFormat::String,
            },
            JsonFlavor::DebeziumMySql => Self {
                time_format: TimeFormat::Micros,
                date_format: DateFormat::DaysSinceEpoch,
                timestamp_format: TimestampFormat::String("%Y-%m-%dT%H:%M:%S%Z"),
                decimal_format: DecimalFormat::String,
            },
            JsonFlavor::Snowflake => Self {
                time_format: TimeFormat::String("%H:%M:%S%.f"),
                date_format: DateFormat::String("%Y-%m-%d"),
                timestamp_format: TimestampFormat::String("%Y-%m-%dT%H:%M:%S%.f%:z"),
                decimal_format: DecimalFormat::String,
            },
            JsonFlavor::Pandas => Self {
                time_format: TimeFormat::String("%H:%M:%S%.f"),
                date_format: DateFormat::String("%Y-%m-%d"),
                timestamp_format: TimestampFormat::MillisSinceEpoch,
                decimal_format: DecimalFormat::String,
            },
            JsonFlavor::ParquetConverter => Self {
                time_format: TimeFormat::Nanos,
                date_format: DateFormat::String("%Y-%m-%d"),
                // TODO: This should become TimestampFormat::MillisSinceEpoch otherwise we lose the
                // millisecond precision that parquet stores e.g., because we call to_json_value on
                // the parquet row it calls this internally:
                // https://docs.rs/parquet/50.0.0/src/parquet/record/api.rs.html#858
                // the right way is probably to use serde_arrow for deserialization and serialization
                timestamp_format: TimestampFormat::String("%Y-%m-%d %H:%M:%S %:z"), // 2023-11-04 15:33:47 +00:00
                decimal_format: DecimalFormat::String,
            },
        }
    }
}
