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
#[derive(Clone, Debug, Default)]
pub enum TimestampFormat {
    /// Default format:
    /// - Parsing: accepts RFC3339 ('YYYY-MM-DDTHH:MM:SS.fffZ') as well as 'YYYY-MM-DD HH:MM:SS.fff' formats.
    /// - Encoding: outputs 'YYYY-MM-DD HH:MM:SS.fff' format.
    #[default]
    Default,
    /// String formatted using the specified format:
    /// See [`chrono` documentation](https://docs.rs/chrono/0.4.31/chrono/format/strftime/)
    /// for supported formatting syntax.
    String(&'static str),
    /// Time specified in milliseconds since UNIX epoch.
    MillisSinceEpoch,
    /// Time specified in microseconds since UNIX epoch.
    MicrosSinceEpoch,
    /// Parsing: accepts microseconds since UNIX epoch, or a string in the
    /// specified `chrono` format. Encoding: outputs microseconds.
    MicrosSinceEpochOrString(&'static str),
    /// Time specified in the RFC 3339 format.
    Rfc3339,
}

/// Representation of the SQL `DECIMAL` type.
#[derive(Clone, Debug, Default)]
pub enum DecimalFormat {
    #[default]
    Numeric,
    String,
    /// The significand as a 128-bit integer.
    ///
    /// On input, a `DECIMAL(p, s)` value is its significand with the scale
    /// `s`, as a 128-bit integer or as the 16 bytes of one in native byte
    /// order. The other representations of a decimal are also accepted.
    I128,
}

/// Representation of the SQL `VARIANT` type.
#[derive(Clone, Debug, Default)]
pub enum VariantFormat {
    /// Serialize VARIANT to/from a JSON value.
    Json,
    /// Represent variant type as a JSON-formatted string.
    ///
    /// On input this also accepts the Parquet variant binary encoding, chosen
    /// per column from what the data holds.
    #[default]
    JsonString,
    /// Serialize VARIANT to the Parquet variant binary encoding: a struct of
    /// `metadata` and `value` binary buffers, as Delta Lake and Iceberg store
    /// the type.
    ///
    /// Deserialization behaves like [`VariantFormat::JsonString`], which reads
    /// that encoding anyway.
    ParquetVariant,
}

/// Representation of the SQL `BINARY` and `VARBINARY` types.
#[derive(Clone, Debug, Default)]
pub enum BinaryFormat {
    /// Serialize as a sequence of bytes.
    #[default]
    Array,
    /// Serialize as base64-encoded string.
    Base64,
    /// Serialize as base58-encoded string.
    Base58,
    /// Serialize as a byte slice (`&[u8]``)
    Bytes,
    /// Serialize as a hexadecimal-encoded string, beginning with `\x`.
    PgHex,
    /// Serialize as a hexadecimal-encoded string, beginning with `0x`.
    CHex,
}

/// Representation of the SQL `BINARY` and `VARBINARY` types.
#[derive(Clone, Debug, Default)]
pub enum UuidFormat {
    /// Serialize as string.
    #[default]
    String,
    /// Serialize as binary.
    Binary,
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
    /// `TIMESTAMP WITH TIME ZONE` format.
    pub timestamp_tz_format: TimestampFormat,
    /// `DECIMAL` format.
    pub decimal_format: DecimalFormat,
    /// `VARIANT` format
    pub variant_format: VariantFormat,
    /// 'VARBINARY', 'BINARY' format.
    // TODO: do we want separate settings for binary and varbinary.
    pub binary_format: BinaryFormat,
    /// 'UUID' format.
    pub uuid_format: UuidFormat,
    /// Read the fields of a record by position, not by name.
    ///
    /// The input must then hold every column of each record, and of each
    /// nested `ROW`, in the order of the SQL declaration, with no extra
    /// columns. Only an input that was put in this order sets this flag;
    /// see `order_columns` in `feldera_adapterlib`.
    pub positional_fields: bool,
    /// With `positional_fields`: the columns that the input does not have.
    pub absent_columns: Option<AbsentColumns>,
}

/// The columns of a table record that a positional input does not have.
///
/// The input then holds the other columns in the order of the SQL
/// declaration. An absent column gets the value that it gets when a record is
/// read by name: its default, `NULL`, or an error. This applies to the table
/// record only: each nested `ROW` holds all its fields.
///
/// A record type with another number of columns than the flags, such as one
/// without the unused columns of the table, reads the input by name.
#[derive(Clone, Debug)]
pub struct AbsentColumns {
    /// One flag for each column of the table, in declaration order.
    absent: Vec<bool>,
    num_present: usize,
    /// The configuration for the values of the columns.
    field_config: Box<SqlSerdeConfig>,
}

impl AbsentColumns {
    pub fn absent(&self) -> &[bool] {
        &self.absent
    }

    pub fn num_present(&self) -> usize {
        self.num_present
    }

    pub fn field_config(&self) -> &SqlSerdeConfig {
        &self.field_config
    }
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

    pub fn with_timestamp_tz_format(mut self, timestamp_tz_format: TimestampFormat) -> Self {
        self.timestamp_tz_format = timestamp_tz_format;
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

    pub fn with_positional_fields(mut self, positional_fields: bool) -> Self {
        self.positional_fields = positional_fields;
        self
    }

    /// Read the table record from a positional input that does not have the
    /// columns with a `true` flag in `absent`.
    pub fn with_absent_columns(mut self, absent: Vec<bool>) -> Self {
        self.positional_fields = true;
        self.absent_columns = None;
        let field_config = Box::new(self.clone());
        self.absent_columns = Some(AbsentColumns {
            num_present: absent.iter().filter(|absent| !**absent).count(),
            absent,
            field_config,
        });
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
                timestamp_tz_format: TimestampFormat::MillisSinceEpoch,
                decimal_format: DecimalFormat::String,
                variant_format: VariantFormat::JsonString,
                binary_format: BinaryFormat::Array,
                uuid_format: UuidFormat::String,
                positional_fields: false,
                absent_columns: None,
            },
            JsonFlavor::DebeziumMySql => Self {
                time_format: TimeFormat::Micros,
                date_format: DateFormat::DaysSinceEpoch,
                // `2018-06-20T13:37:03Z` and `2018-06-20T13:37:03.123456Z`.
                timestamp_format: TimestampFormat::String("%Y-%m-%dT%H:%M:%S%.f%Z"),
                timestamp_tz_format: TimestampFormat::String("%Y-%m-%dT%H:%M:%S%.f%Z"),
                decimal_format: DecimalFormat::String,
                variant_format: VariantFormat::JsonString,
                binary_format: BinaryFormat::Array,
                uuid_format: UuidFormat::String,
                positional_fields: false,
                absent_columns: None,
            },
            JsonFlavor::DebeziumPostgres => Self {
                time_format: TimeFormat::Micros,
                date_format: DateFormat::DaysSinceEpoch,
                timestamp_format: TimestampFormat::MillisSinceEpoch,
                timestamp_tz_format: TimestampFormat::MillisSinceEpoch,
                decimal_format: DecimalFormat::String,
                variant_format: VariantFormat::JsonString,
                binary_format: BinaryFormat::Array,
                uuid_format: UuidFormat::String,
                positional_fields: false,
                absent_columns: None,
            },
            JsonFlavor::Snowflake => Self {
                time_format: TimeFormat::String("%H:%M:%S%.f"),
                date_format: DateFormat::String("%Y-%m-%d"),
                timestamp_format: TimestampFormat::String("%Y-%m-%dT%H:%M:%S%.f%:z"),
                timestamp_tz_format: TimestampFormat::String("%Y-%m-%dT%H:%M:%S%.f%:z"),
                decimal_format: DecimalFormat::String,
                variant_format: VariantFormat::JsonString,
                binary_format: BinaryFormat::Array,
                uuid_format: UuidFormat::String,
                positional_fields: false,
                absent_columns: None,
            },
            JsonFlavor::Pandas => Self {
                time_format: TimeFormat::String("%H:%M:%S%.f"),
                date_format: DateFormat::String("%Y-%m-%d"),
                timestamp_format: TimestampFormat::MillisSinceEpoch,
                timestamp_tz_format: TimestampFormat::MillisSinceEpoch,
                decimal_format: DecimalFormat::String,
                variant_format: VariantFormat::JsonString,
                binary_format: BinaryFormat::Array,
                uuid_format: UuidFormat::String,
                positional_fields: false,
                absent_columns: None,
            },
            JsonFlavor::Blockchain => {
                SqlSerdeConfig::default().with_binary_format(BinaryFormat::Base58)
            }
            JsonFlavor::CHex => SqlSerdeConfig::default().with_binary_format(BinaryFormat::CHex),
            JsonFlavor::ParquetConverter => Self {
                time_format: TimeFormat::Nanos,
                date_format: DateFormat::String("%Y-%m-%d"),
                timestamp_format: TimestampFormat::String("%Y-%m-%d %H:%M:%S.%f %:z"), // 2023-11-04 15:33:47.123 +00:00
                timestamp_tz_format: TimestampFormat::String("%Y-%m-%d %H:%M:%S.%f %:z"), // 2023-11-04 15:33:47.123 +00:00
                decimal_format: DecimalFormat::String,
                variant_format: VariantFormat::JsonString,
                binary_format: BinaryFormat::Base64,
                uuid_format: UuidFormat::String,
                positional_fields: false,
                absent_columns: None,
            },
            JsonFlavor::ClockInput => {
                SqlSerdeConfig::default().with_timestamp_format(TimestampFormat::MillisSinceEpoch)
            }
            JsonFlavor::Postgres => Self {
                time_format: TimeFormat::default(), // H-M-S
                date_format: DateFormat::default(), // Y-m-d
                timestamp_format: TimestampFormat::default(),
                timestamp_tz_format: TimestampFormat::default(),
                decimal_format: DecimalFormat::String,
                variant_format: VariantFormat::Json,
                // We need [`BinaryFormat::PgHex`] only because we serialize
                // as JSON and let Postgres parse it.
                binary_format: BinaryFormat::PgHex,
                uuid_format: UuidFormat::String,
                positional_fields: false,
                absent_columns: None,
            },
        }
    }
}
