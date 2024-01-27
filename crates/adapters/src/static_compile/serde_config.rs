//! (De)serializer configuration used for SQL records.
//!
//! The [`DeserializeWithContext`](`crate::DeserializeWithContext`) and
//! [`SerializeWithContext`](`crate::SerializeWithContext`) traits
//! provide a mechanism for building configurable deserializers and serializers.
//! This module defines the [`SqlSerdeConfig`] type used to specify the
//! encoding used for SQL types.  All input types used by a Feldera pipeline
//! must implement `DeserializeWithContext<SqlDeserializerConfig>`.
//! Likewise all output types must implement
//! `SerializeWithContext<SqlSerializerConfig>`.

use pipeline_types::format::json::JsonFlavor;

/// Representation of the SQL `TIME` type.
#[derive(Clone)]
pub enum TimeFormat {
    // String formatted using the specified format string:
    // See [`chrono` documentation](https://docs.rs/chrono/0.4.31/chrono/format/strftime/)
    // for supported time formatting syntax.
    String(&'static str),
    /// Time specified in microseconds from the start of the day.
    Micros,
    /// Time specified in milliseconds from the start of the day.
    Millis,
}

impl Default for TimeFormat {
    fn default() -> Self {
        Self::String("%H:%M:%S%.f")
    }
}

// Representation of the SQL `DATE` type.
#[derive(Clone)]
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
#[derive(Clone)]
pub enum TimestampFormat {
    /// String formatted using the specified format:
    /// See [`chrono` documentation](https://docs.rs/chrono/0.4.31/chrono/format/strftime/)
    /// for supported formatting syntax.
    String(&'static str),
    /// Time specified in milliseconds since UNIX epoch.
    MillisSinceEpoch,
}

impl Default for TimestampFormat {
    fn default() -> Self {
        Self::String("%F %T%.f")
    }
}

/// Deserializer configuration for parsing SQL records.
#[derive(Clone, Default)]
pub struct SqlSerdeConfig {
    /// `TIME` format.
    pub time_format: TimeFormat,
    /// `DATE` format.
    pub date_format: DateFormat,
    /// `TIMESTAMP` format.
    pub timestamp_format: TimestampFormat,
}

impl From<JsonFlavor> for SqlSerdeConfig {
    fn from(flavor: JsonFlavor) -> Self {
        match flavor {
            JsonFlavor::Default => Default::default(),
            JsonFlavor::KafkaConnectJsonConverter { .. } => Self {
                time_format: TimeFormat::Millis,
                date_format: DateFormat::DaysSinceEpoch,
                timestamp_format: TimestampFormat::MillisSinceEpoch,
            },
            JsonFlavor::DebeziumMySql => Self {
                time_format: TimeFormat::Micros,
                date_format: DateFormat::DaysSinceEpoch,
                timestamp_format: TimestampFormat::String("%Y-%m-%dT%H:%M:%S%Z"),
            },
            JsonFlavor::Snowflake => Self {
                time_format: TimeFormat::String("%H:%M:%S%.f"),
                date_format: DateFormat::String("%Y-%m-%d"),
                timestamp_format: TimestampFormat::String("%Y-%m-%dT%H:%M:%S%.f%:z"),
            },
        }
    }
}
