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
    /// Time specified in nanoseconds from the start of the day as an unsigned 64-bit integer.
    Nanos,
    /// Time specified in nanoseconds from the start of the day as a signed 64-bit integer.
    NanosSigned,
}

impl Default for TimeFormat {
    fn default() -> Self {
        Self::String("%H:%M:%S%.f")
    }
}

/// Representation of the SQL `DATE` type.
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

/// Representation of the SQL `TIMESTAMP` type.
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
    /// Time specified in the RFC 3339 format.
    Rfc3339,
}

impl Default for TimestampFormat {
    fn default() -> Self {
        Self::String("%F %T%.f")
    }
}

/// Representation of the SQL `DECIMAL` type.
#[derive(Clone, Debug)]
pub enum DecimalFormat {
    Numeric,
    String,
    I128,
}

impl Default for DecimalFormat {
    fn default() -> Self {
        Self::Numeric
    }
}

/// Representation of the SQL `VARIANT` type.
#[derive(Clone, Debug)]
pub enum VariantFormat {
    /// Serialize VARIANT to/from a JSON value.
    Json,
    /// Represent variant type as a JSON-formatted string.
    JsonString,
}

impl Default for VariantFormat {
    fn default() -> Self {
        Self::JsonString
    }
}

/// Representation of the SQL `BINARY` and `VARBINARY` types.
#[derive(Clone, Debug)]
pub enum BinaryFormat {
    /// Serialize as a sequence of bytes.
    Array,
    /// Serialize as base64-encoded string.
    Base64,
    /// Serialize as base58-encoded string.
    Base58,
    /// Serialize as a byte slice (`&[u8]``)
    Bytes,
    /// Serialize as a hexadecimal-encoded string, beginning with `\x`.
    PgHex,
}

impl Default for BinaryFormat {
    fn default() -> Self {
        Self::Array
    }
}

/// Representation of the SQL `BINARY` and `VARBINARY` types.
#[derive(Clone, Debug)]
pub enum UuidFormat {
    /// Serialize as string.
    String,
    /// Serialize as binary.
    Binary,
}

impl Default for UuidFormat {
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
    /// `VARIANT` format
    pub variant_format: VariantFormat,
    /// 'VARBINARY', 'BINARY' format.
    // TODO: do we want separate settings for binary and varbinary.
    pub binary_format: BinaryFormat,
    /// 'UUID' format.
    pub uuid_format: UuidFormat,
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

    pub fn with_variant_format(mut self, variant_format: VariantFormat) -> Self {
        self.variant_format = variant_format;
        self
    }

    pub fn with_binary_format(mut self, binary_format: BinaryFormat) -> Self {
        self.binary_format = binary_format;
        self
    }

    pub fn with_uuid_format(mut self, uuid_format: UuidFormat) -> Self {
        self.uuid_format = uuid_format;
        self
    }
}

impl From<JsonFlavor> for SqlSerdeConfig {
    fn from(flavor: JsonFlavor) -> Self {
        match flavor {
            JsonFlavor::Default => {
                SqlSerdeConfig::default().with_variant_format(VariantFormat::Json)
            }
            JsonFlavor::Datagen => SqlSerdeConfig::default()
                .with_variant_format(VariantFormat::Json)
                .with_timestamp_format(TimestampFormat::Rfc3339),
            JsonFlavor::KafkaConnectJsonConverter => Self {
                time_format: TimeFormat::Millis,
                date_format: DateFormat::DaysSinceEpoch,
                timestamp_format: TimestampFormat::MillisSinceEpoch,
                decimal_format: DecimalFormat::String,
                variant_format: VariantFormat::JsonString,
                binary_format: BinaryFormat::Array,
                uuid_format: UuidFormat::String,
            },
            JsonFlavor::DebeziumMySql => Self {
                time_format: TimeFormat::Micros,
                date_format: DateFormat::DaysSinceEpoch,
                timestamp_format: TimestampFormat::String("%Y-%m-%dT%H:%M:%S%Z"),
                decimal_format: DecimalFormat::String,
                variant_format: VariantFormat::JsonString,
                binary_format: BinaryFormat::Array,
                uuid_format: UuidFormat::String,
            },
            JsonFlavor::DebeziumPostgres => Self {
                time_format: TimeFormat::Micros,
                date_format: DateFormat::DaysSinceEpoch,
                timestamp_format: TimestampFormat::MillisSinceEpoch,
                decimal_format: DecimalFormat::String,
                variant_format: VariantFormat::JsonString,
                binary_format: BinaryFormat::Array,
                uuid_format: UuidFormat::String,
            },
            JsonFlavor::Snowflake => Self {
                time_format: TimeFormat::String("%H:%M:%S%.f"),
                date_format: DateFormat::String("%Y-%m-%d"),
                timestamp_format: TimestampFormat::String("%Y-%m-%dT%H:%M:%S%.f%:z"),
                decimal_format: DecimalFormat::String,
                variant_format: VariantFormat::JsonString,
                binary_format: BinaryFormat::Array,
                uuid_format: UuidFormat::String,
            },
            JsonFlavor::Pandas => Self {
                time_format: TimeFormat::String("%H:%M:%S%.f"),
                date_format: DateFormat::String("%Y-%m-%d"),
                timestamp_format: TimestampFormat::MillisSinceEpoch,
                decimal_format: DecimalFormat::String,
                variant_format: VariantFormat::JsonString,
                binary_format: BinaryFormat::Array,
                uuid_format: UuidFormat::String,
            },
            JsonFlavor::Blockchain => {
                SqlSerdeConfig::default().with_binary_format(BinaryFormat::Base58)
            }
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
                variant_format: VariantFormat::JsonString,
                binary_format: BinaryFormat::Base64,
                uuid_format: UuidFormat::String,
            },
            JsonFlavor::ClockInput => {
                SqlSerdeConfig::default().with_timestamp_format(TimestampFormat::MillisSinceEpoch)
            }
            JsonFlavor::Postgres => Self {
                time_format: TimeFormat::default(), // H-M-S
                date_format: DateFormat::default(), // Y-m-d
                timestamp_format: TimestampFormat::default(),
                decimal_format: DecimalFormat::String,
                variant_format: VariantFormat::Json,
                // We need [`BinaryFormat::PgHex`] only because we serialize
                // as JSON and let Postgres parse it.
                binary_format: BinaryFormat::PgHex,
                uuid_format: UuidFormat::String,
            },
        }
    }
}
