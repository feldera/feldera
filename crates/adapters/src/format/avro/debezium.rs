//! Debezium-specific value conversions used by the Avro input connector.
//!
//! Debezium does not use Avro's native `timestamp-*`/`time-*`/`decimal` logical
//! types. Instead it tags plain Avro types with a `connect.name` attribute that
//! names a Debezium semantic type, e.g.:
//!
//! ```json
//! { "type": "long", "connect.name": "io.debezium.time.MicroTimestamp" }
//! ```
//!
//! This module maps those semantic types to the representation Feldera's column
//! types expect. The generic machinery that recognizes annotations, drives the
//! deserializer, and validates schemas lives in [`super::coercion`]; this module
//! provides only the Debezium-specific pieces it delegates to.
//!
//! See the [Debezium temporal types documentation][docs] for the wire format of
//! each type.
//!
//! [docs]: https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-temporal-types

use apache_avro::{BigDecimal, types::Value};
use chrono::{DateTime, Timelike, Utc};
use num_bigint::BigInt;

/// Number of microseconds in a full day, used to normalize `ZonedTime`.
const MICROS_PER_DAY: i64 = 86_400 * 1_000_000;

/// A Debezium temporal semantic type carried by a `connect.name` attribute.
///
/// Covers the `TIME` and `TIMESTAMP` families. `io.debezium.time.Date` and
/// `org.apache.kafka.connect.data.Date` are intentionally omitted: they are
/// plain day counts that already deserialize correctly into a Feldera `DATE`
/// column without any conversion.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DebeziumTimeType {
    /// Milliseconds since midnight (`int`).
    ///
    /// `io.debezium.time.Time`, `org.apache.kafka.connect.data.Time`.
    TimeMillis,
    /// Microseconds since midnight (`long`). `io.debezium.time.MicroTime`.
    TimeMicros,
    /// Nanoseconds since midnight (`long`). `io.debezium.time.NanoTime`.
    TimeNanos,
    /// ISO-8601 time-with-offset string, e.g. `10:15:30+01:00`.
    /// `io.debezium.time.ZonedTime`.
    ZonedTime,
    /// Milliseconds since the Unix epoch (`long`).
    ///
    /// `io.debezium.time.Timestamp`, `org.apache.kafka.connect.data.Timestamp`.
    TimestampMillis,
    /// Microseconds since the Unix epoch (`long`).
    /// `io.debezium.time.MicroTimestamp`.
    TimestampMicros,
    /// Nanoseconds since the Unix epoch (`long`).
    /// `io.debezium.time.NanoTimestamp`.
    TimestampNanos,
    /// ISO-8601 timestamp-with-offset string,
    /// e.g. `2011-12-03T10:15:30.030431+01:00`. `io.debezium.time.ZonedTimestamp`.
    ZonedTimestamp,
}

impl DebeziumTimeType {
    /// Map a `connect.name` attribute value to a temporal type, or `None` if it
    /// is not a recognized temporal type.
    pub fn from_connect_name(connect_name: &str) -> Option<Self> {
        Some(match connect_name {
            "io.debezium.time.Time" | "org.apache.kafka.connect.data.Time" => Self::TimeMillis,
            "io.debezium.time.MicroTime" => Self::TimeMicros,
            "io.debezium.time.NanoTime" => Self::TimeNanos,
            "io.debezium.time.ZonedTime" => Self::ZonedTime,
            "io.debezium.time.Timestamp" | "org.apache.kafka.connect.data.Timestamp" => {
                Self::TimestampMillis
            }
            "io.debezium.time.MicroTimestamp" => Self::TimestampMicros,
            "io.debezium.time.NanoTimestamp" => Self::TimestampNanos,
            "io.debezium.time.ZonedTimestamp" => Self::ZonedTimestamp,
            _ => return None,
        })
    }

    /// A representative `connect.name` for this type, used in error messages.
    pub fn canonical_connect_name(self) -> &'static str {
        match self {
            Self::TimeMillis => "io.debezium.time.Time",
            Self::TimeMicros => "io.debezium.time.MicroTime",
            Self::TimeNanos => "io.debezium.time.NanoTime",
            Self::ZonedTime => "io.debezium.time.ZonedTime",
            Self::TimestampMillis => "io.debezium.time.Timestamp",
            Self::TimestampMicros => "io.debezium.time.MicroTimestamp",
            Self::TimestampNanos => "io.debezium.time.NanoTimestamp",
            Self::ZonedTimestamp => "io.debezium.time.ZonedTimestamp",
        }
    }

    /// `true` if this type populates a `TIMESTAMP` column, `false` for a `TIME`
    /// column.
    pub fn is_timestamp(self) -> bool {
        matches!(
            self,
            Self::TimestampMillis
                | Self::TimestampMicros
                | Self::TimestampNanos
                | Self::ZonedTimestamp
        )
    }

    /// The Avro primitive type name that carries this Debezium type on the wire.
    pub fn avro_primitive(self) -> &'static str {
        match self {
            Self::TimeMillis => "int",
            Self::TimeMicros | Self::TimeNanos => "long",
            Self::ZonedTime => "string",
            Self::TimestampMillis | Self::TimestampMicros | Self::TimestampNanos => "long",
            Self::ZonedTimestamp => "string",
        }
    }

    /// Convert a decoded Avro value into microseconds: since the Unix epoch for
    /// timestamp types, since midnight for time types.
    pub fn to_micros(self, value: &Value) -> Result<i64, String> {
        match self {
            Self::TimeMillis | Self::TimestampMillis => millis_to_micros(as_i64(value)?),
            Self::TimeMicros | Self::TimestampMicros => as_i64(value),
            Self::TimeNanos | Self::TimestampNanos => Ok(as_i64(value)? / 1_000),
            Self::ZonedTimestamp => parse_zoned_timestamp_micros(as_str(value)?),
            Self::ZonedTime => parse_zoned_time_micros(as_str(value)?),
        }
    }
}

/// Convert an `io.debezium.data.VariableScaleDecimal` record into a decimal
/// string.
///
/// The type is used for `NUMERIC`/`DECIMAL` columns without a fixed scale. On
/// the wire it is a record with a `scale` (`int`) and a `value` (`bytes`, the
/// big-endian two's-complement unscaled integer). The decimal string it yields
/// is parsed by the target `DECIMAL` column deserializer.
pub fn variable_scale_decimal_to_string(value: &Value) -> Result<String, String> {
    let Value::Record(fields) = value else {
        return Err(format!(
            "expected a VariableScaleDecimal record, but found {value:?}"
        ));
    };

    let mut scale = None;
    let mut unscaled = None;
    for (name, field) in fields {
        match name.as_str() {
            "scale" => scale = Some(record_int(field)?),
            "value" => unscaled = Some(record_bytes(field)?),
            _ => {}
        }
    }

    let scale = scale
        .ok_or_else(|| "VariableScaleDecimal record is missing the 'scale' field".to_string())?;
    let unscaled = unscaled
        .ok_or_else(|| "VariableScaleDecimal record is missing the 'value' field".to_string())?;

    let unscaled = BigInt::from_signed_bytes_be(unscaled);
    Ok(BigDecimal::new(unscaled, scale as i64).to_string())
}

/// Read an integer out of an Avro `int` or `long` value.
fn as_i64(value: &Value) -> Result<i64, String> {
    match value {
        Value::Int(i) => Ok(*i as i64),
        Value::Long(i) => Ok(*i),
        other => Err(format!(
            "expected an integer value for a Debezium temporal type, but found {other:?}"
        )),
    }
}

/// Read a string out of an Avro `string` value.
fn as_str(value: &Value) -> Result<&str, String> {
    match value {
        Value::String(s) => Ok(s.as_str()),
        other => Err(format!(
            "expected a string value for a Debezium zoned temporal type, but found {other:?}"
        )),
    }
}

/// Read the `scale` field of a VariableScaleDecimal record.
fn record_int(value: &Value) -> Result<i32, String> {
    match value {
        Value::Int(i) => Ok(*i),
        Value::Union(_, inner) => record_int(inner),
        other => Err(format!(
            "expected an int for the VariableScaleDecimal 'scale' field, but found {other:?}"
        )),
    }
}

/// Read the `value` field of a VariableScaleDecimal record.
fn record_bytes(value: &Value) -> Result<&[u8], String> {
    match value {
        Value::Bytes(bytes) | Value::Fixed(_, bytes) => Ok(bytes),
        Value::Union(_, inner) => record_bytes(inner),
        other => Err(format!(
            "expected bytes for the VariableScaleDecimal 'value' field, but found {other:?}"
        )),
    }
}

fn millis_to_micros(millis: i64) -> Result<i64, String> {
    millis.checked_mul(1_000).ok_or_else(|| {
        format!("millisecond value {millis} overflows when converted to microseconds")
    })
}

/// Parse an ISO-8601 `ZonedTimestamp` string into microseconds since the epoch.
fn parse_zoned_timestamp_micros(s: &str) -> Result<i64, String> {
    let dt = DateTime::parse_from_rfc3339(s.trim())
        .map_err(|e| format!("invalid Debezium ZonedTimestamp '{s}': {e}"))?;
    Ok(dt.timestamp_micros())
}

/// Parse an ISO-8601 `ZonedTime` string (e.g. `10:15:30+01:00`) into
/// microseconds since midnight, normalized to UTC.
///
/// A Feldera `TIME` has no timezone, so the offset is folded into the
/// wall-clock time modulo 24 hours.
fn parse_zoned_time_micros(s: &str) -> Result<i64, String> {
    let s = s.trim();
    // Reuse the RFC3339 parser by pinning the time to an arbitrary date. The
    // date is discarded after normalizing to UTC; only the time of day matters.
    let dt = DateTime::parse_from_rfc3339(&format!("1970-01-01T{s}"))
        .map_err(|e| format!("invalid Debezium ZonedTime '{s}': {e}"))?;
    let time = dt.with_timezone(&Utc).time();
    let micros =
        time.num_seconds_from_midnight() as i64 * 1_000_000 + time.nanosecond() as i64 / 1_000;
    Ok(micros.rem_euclid(MICROS_PER_DAY))
}

#[cfg(test)]
mod test {
    use super::*;
    use apache_avro::types::Value;

    #[test]
    fn connect_name_mapping() {
        assert_eq!(
            DebeziumTimeType::from_connect_name("io.debezium.time.MicroTimestamp"),
            Some(DebeziumTimeType::TimestampMicros)
        );
        assert_eq!(
            DebeziumTimeType::from_connect_name("org.apache.kafka.connect.data.Timestamp"),
            Some(DebeziumTimeType::TimestampMillis)
        );
        // Date is handled by the plain-int path, not as a coercion.
        assert_eq!(
            DebeziumTimeType::from_connect_name("io.debezium.time.Date"),
            None
        );
        assert_eq!(
            DebeziumTimeType::from_connect_name("io.debezium.data.Json"),
            None
        );
    }

    #[test]
    fn numeric_timestamps_to_micros() {
        // 2021-01-01T00:00:00Z = 1_609_459_200 s.
        let secs = 1_609_459_200i64;
        assert_eq!(
            DebeziumTimeType::TimestampMillis
                .to_micros(&Value::Long(secs * 1_000))
                .unwrap(),
            secs * 1_000_000
        );
        assert_eq!(
            DebeziumTimeType::TimestampMicros
                .to_micros(&Value::Long(secs * 1_000_000))
                .unwrap(),
            secs * 1_000_000
        );
        assert_eq!(
            DebeziumTimeType::TimestampNanos
                .to_micros(&Value::Long(secs * 1_000_000_000))
                .unwrap(),
            secs * 1_000_000
        );
    }

    #[test]
    fn numeric_times_to_micros() {
        // 01:02:03.004 = 3_723_004 ms since midnight.
        let millis = 3_723_004i64;
        let micros = millis * 1_000;
        assert_eq!(
            DebeziumTimeType::TimeMillis
                .to_micros(&Value::Int(millis as i32))
                .unwrap(),
            micros
        );
        assert_eq!(
            DebeziumTimeType::TimeMicros
                .to_micros(&Value::Long(micros))
                .unwrap(),
            micros
        );
        assert_eq!(
            DebeziumTimeType::TimeNanos
                .to_micros(&Value::Long(micros * 1_000))
                .unwrap(),
            micros
        );
    }

    #[test]
    fn zoned_timestamp_to_micros() {
        // Example from the Debezium docs, with a +01:00 offset.
        let micros = DebeziumTimeType::ZonedTimestamp
            .to_micros(&Value::String(
                "2011-12-03T10:15:30.030431+01:00".to_string(),
            ))
            .unwrap();
        // Equivalent UTC instant: 2011-12-03T09:15:30.030431Z.
        let expected = DateTime::parse_from_rfc3339("2011-12-03T09:15:30.030431Z")
            .unwrap()
            .timestamp_micros();
        assert_eq!(micros, expected);
    }

    #[test]
    fn zoned_time_to_micros() {
        // 10:15:30+01:00 normalizes to 09:15:30 UTC.
        let micros = DebeziumTimeType::ZonedTime
            .to_micros(&Value::String("10:15:30+01:00".to_string()))
            .unwrap();
        let expected = (9 * 3_600 + 15 * 60 + 30) * 1_000_000;
        assert_eq!(micros, expected);
    }

    #[test]
    fn zoned_time_wraps_across_midnight() {
        // 00:30:00+01:00 is 23:30:00 UTC on the previous day; only the time
        // of day survives.
        let micros = DebeziumTimeType::ZonedTime
            .to_micros(&Value::String("00:30:00+01:00".to_string()))
            .unwrap();
        let expected = (23i64 * 3_600 + 30 * 60) * 1_000_000;
        assert_eq!(micros, expected);
    }

    #[test]
    fn zoned_time_utc_with_fraction() {
        let micros = DebeziumTimeType::ZonedTime
            .to_micros(&Value::String("13:37:03.123456Z".to_string()))
            .unwrap();
        let expected = (13i64 * 3_600 + 37 * 60 + 3) * 1_000_000 + 123_456;
        assert_eq!(micros, expected);
    }

    #[test]
    fn invalid_zoned_values_error() {
        assert!(
            DebeziumTimeType::ZonedTimestamp
                .to_micros(&Value::String("not-a-timestamp".to_string()))
                .is_err()
        );
        assert!(
            DebeziumTimeType::ZonedTime
                .to_micros(&Value::String("99:99:99Z".to_string()))
                .is_err()
        );
    }

    #[test]
    fn variable_scale_decimal() {
        let record = |scale: i32, unscaled: i64| {
            Value::Record(vec![
                ("scale".to_string(), Value::Int(scale)),
                (
                    "value".to_string(),
                    Value::Bytes(BigInt::from(unscaled).to_signed_bytes_be()),
                ),
            ])
        };

        assert_eq!(
            variable_scale_decimal_to_string(&record(2, 12345)).unwrap(),
            "123.45"
        );
        assert_eq!(
            variable_scale_decimal_to_string(&record(3, -6789)).unwrap(),
            "-6.789"
        );
        assert_eq!(
            variable_scale_decimal_to_string(&record(0, 42)).unwrap(),
            "42"
        );
    }

    #[test]
    fn variable_scale_decimal_missing_field_errors() {
        let record = Value::Record(vec![("scale".to_string(), Value::Int(2))]);
        assert!(variable_scale_decimal_to_string(&record).is_err());
    }
}
