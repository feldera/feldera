//! Conversions between [`VariantValue`], the engine-neutral `VARIANT` value, and the two
//! `VARIANT` representations of this crate, [`Variant`] and [`FlatVariant`].
//!
//! - A `VariantValue` always converts, and three values change form on the way:
//!   - a nanosecond timestamp rounds toward the past to a whole microsecond, the unit
//!     [`crate::Timestamp`] and [`crate::TimestampTz`] hold;
//!   - a decimal takes the canonical form every other `VARIANT` decimal has, with no trailing
//!     zeros after the point, so `15.00` becomes `15`. The canonical form gives equal values
//!     identical bytes, which [`FlatVariant`] equality and hashing compare;
//!   - a leap second becomes the last nanosecond of the second before it, since a SQL `TIME`
//!     has no leap seconds.
//! - A [`Variant`] or [`FlatVariant`] converts unless it holds a type the encoding lacks (an
//!   interval or a geometry) or a map which does not have string or integer keys. An unsigned
//!   integer widens to the smallest signed type that holds it, or to a decimal of scale 0 when
//!   no signed type holds it, as for a `u64` above `i64::MAX`. A decimal takes the narrowest
//!   width that holds it, an integer map key becomes the string of its digits, and both SQL
//!   null and the variant null become [`VariantValue::Null`].

// The conversions to and from the enum `Variant` serve the code that still reads it, such as
// the record deserializer of connector metadata.
#![allow(deprecated)]

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::ops::Range;

use chrono::{NaiveTime, Timelike};
use feldera_fxp::DynamicDecimal;
use feldera_types::variant::{VariantDecimal4, VariantDecimal8, VariantDecimal16, VariantValue};

use crate::flat_variant::casts::{decimal_payload, type_string};
use crate::flat_variant::{
    Container, FlatVariant, TAG_ARRAY, TAG_BIGINT, TAG_BINARY, TAG_BOOLEAN, TAG_DATE, TAG_DECIMAL,
    TAG_DOUBLE, TAG_GEO_POINT, TAG_INT, TAG_LONG_INTERVAL, TAG_MAP, TAG_REAL, TAG_SHORT_INTERVAL,
    TAG_SMALLINT, TAG_SQL_NULL, TAG_STRING, TAG_TIME, TAG_TIMESTAMP, TAG_TIMESTAMP_TZ, TAG_TINYINT,
    TAG_UBIGINT, TAG_UINT, TAG_USMALLINT, TAG_UTINYINT, TAG_UUID, TAG_VARIANT_NULL, Writer,
    build_document_infallible, flat_variant_object_key, payload_array,
};
use crate::variant::Variant;
use crate::variant_binary::{
    MAX_VARIANT_DEPTH, unencodable_variant, variant_object_key, variant_too_deep,
};
use crate::{
    ByteArray, Date, F32, F64, SqlRuntimeError, SqlString, Time, Timestamp, TimestampTz, Uuid,
};

impl From<&VariantValue> for Variant {
    /// ```
    /// use feldera_sqllib::{SqlString, Variant, VariantValue};
    ///
    /// let topic = VariantValue::String("events".into());
    /// assert_eq!(Variant::from(&topic), Variant::String(SqlString::from_ref("events")));
    /// ```
    fn from(value: &VariantValue) -> Self {
        match value {
            VariantValue::Null => Variant::VariantNull,
            VariantValue::Boolean(v) => Variant::Boolean(*v),
            VariantValue::Int8(v) => Variant::TinyInt(*v),
            VariantValue::Int16(v) => Variant::SmallInt(*v),
            VariantValue::Int32(v) => Variant::Int(*v),
            VariantValue::Int64(v) => Variant::BigInt(*v),
            VariantValue::Float(v) => Variant::Real(F32::new(*v)),
            VariantValue::Double(v) => Variant::Double(F64::new(*v)),
            VariantValue::Decimal4(v) => {
                Variant::SqlDecimal(canonical_decimal(v.integer().into(), v.scale()))
            }
            VariantValue::Decimal8(v) => {
                Variant::SqlDecimal(canonical_decimal(v.integer().into(), v.scale()))
            }
            VariantValue::Decimal16(v) => {
                Variant::SqlDecimal(canonical_decimal(v.integer(), v.scale()))
            }
            VariantValue::String(v) => Variant::String(SqlString::from_ref(v)),
            VariantValue::Binary(v) => Variant::Binary(ByteArray::new(v)),
            VariantValue::Date(v) => Variant::Date(Date::from_date(*v)),
            VariantValue::Time(v) => Variant::Time(Time::from_time(without_leap_second(*v))),
            VariantValue::TimestampMicros(v) | VariantValue::TimestampNanos(v) => {
                Variant::TimestampTz(TimestampTz::from_microseconds(v.timestamp_micros()))
            }
            VariantValue::TimestampNtzMicros(v) | VariantValue::TimestampNtzNanos(v) => {
                Variant::Timestamp(Timestamp::from_microseconds(v.and_utc().timestamp_micros()))
            }
            VariantValue::Uuid(v) => Variant::Uuid(Uuid::from_bytes(*v.as_bytes())),
            VariantValue::List(elements) => Variant::Array(
                elements
                    .iter()
                    .map(Variant::from)
                    .collect::<Vec<_>>()
                    .into(),
            ),
            VariantValue::Object(fields) => Variant::Map(
                fields
                    .iter()
                    .map(|(name, value)| (Variant::String(SqlString::from_ref(name)), value.into()))
                    .collect::<BTreeMap<_, _>>()
                    .into(),
            ),
        }
    }
}

impl From<VariantValue> for Variant {
    fn from(value: VariantValue) -> Self {
        Variant::from(&value)
    }
}

impl TryFrom<&Variant> for VariantValue {
    type Error = Box<SqlRuntimeError>;

    /// ```
    /// use feldera_sqllib::{Variant, VariantValue};
    ///
    /// // The encoding has no unsigned types, so a `u8` widens to the next signed width.
    /// assert_eq!(VariantValue::try_from(&Variant::UTinyInt(200)).unwrap(), VariantValue::Int16(200));
    /// ```
    fn try_from(value: &Variant) -> Result<Self, Self::Error> {
        variant_value(value, 1).map_err(SqlRuntimeError::from_string)
    }
}

impl From<&VariantValue> for FlatVariant {
    /// ```
    /// use feldera_sqllib::{FlatVariant, VariantValue};
    ///
    /// let topic = FlatVariant::from(&VariantValue::String("events".into()));
    /// assert_eq!(topic.to_json_string().unwrap(), r#""events""#);
    /// ```
    fn from(value: &VariantValue) -> Self {
        build_document_infallible(|w| encode_flat_variant(w, value))
    }
}

impl From<VariantValue> for FlatVariant {
    fn from(value: VariantValue) -> Self {
        FlatVariant::from(&value)
    }
}

impl TryFrom<&FlatVariant> for VariantValue {
    type Error = Box<SqlRuntimeError>;

    /// ```
    /// use feldera_sqllib::{FlatVariant, VariantValue};
    ///
    /// // JSON reads `7` as unsigned, which widens to the next signed width.
    /// let document: FlatVariant = serde_json::from_str("[7]").unwrap();
    /// assert_eq!(
    ///     VariantValue::try_from(&document).unwrap(),
    ///     VariantValue::List(vec![VariantValue::Int64(7)])
    /// );
    /// ```
    fn try_from(value: &FlatVariant) -> Result<Self, Self::Error> {
        flat_variant_value(value.as_bytes(), 1).map_err(SqlRuntimeError::from_string)
    }
}

/// Append `value`'s flat encoding to `w` and return its range. An object's fields go in name
/// order, which is the order the encoding keeps string keys in.
fn encode_flat_variant(w: &mut Writer, value: &VariantValue) -> Range<usize> {
    match value {
        VariantValue::Null => w.scalar(TAG_VARIANT_NULL, &[]),
        VariantValue::Boolean(v) => w.scalar(TAG_BOOLEAN, &[u8::from(*v)]),
        VariantValue::Int8(v) => w.scalar(TAG_TINYINT, &v.to_le_bytes()),
        VariantValue::Int16(v) => w.scalar(TAG_SMALLINT, &v.to_le_bytes()),
        VariantValue::Int32(v) => w.scalar(TAG_INT, &v.to_le_bytes()),
        VariantValue::Int64(v) => w.scalar(TAG_BIGINT, &v.to_le_bytes()),
        VariantValue::Float(v) => w.scalar(TAG_REAL, &v.to_le_bytes()),
        VariantValue::Double(v) => w.scalar(TAG_DOUBLE, &v.to_le_bytes()),
        VariantValue::Decimal4(v) => encode_flat_decimal(w, v.integer().into(), v.scale()),
        VariantValue::Decimal8(v) => encode_flat_decimal(w, v.integer().into(), v.scale()),
        VariantValue::Decimal16(v) => encode_flat_decimal(w, v.integer(), v.scale()),
        VariantValue::String(v) => w.scalar(TAG_STRING, v.as_bytes()),
        VariantValue::Binary(v) => w.scalar(TAG_BINARY, v),
        VariantValue::Date(v) => w.scalar(TAG_DATE, &Date::from_date(*v).days().to_le_bytes()),
        VariantValue::Time(v) => {
            let time = Time::from_time(without_leap_second(*v));
            w.scalar(TAG_TIME, &time.nanoseconds().to_le_bytes())
        }
        VariantValue::TimestampMicros(v) | VariantValue::TimestampNanos(v) => {
            w.scalar(TAG_TIMESTAMP_TZ, &v.timestamp_micros().to_le_bytes())
        }
        VariantValue::TimestampNtzMicros(v) | VariantValue::TimestampNtzNanos(v) => {
            w.scalar(TAG_TIMESTAMP, &v.and_utc().timestamp_micros().to_le_bytes())
        }
        VariantValue::Uuid(v) => w.scalar(TAG_UUID, v.as_bytes()),
        VariantValue::List(elements) => w.array_in_place(elements.len(), |w, i| {
            encode_flat_variant(w, &elements[i]);
        }),
        VariantValue::Object(fields) => {
            let (start, mut key_ends, mut value_ends) = w.begin_map_in_place(fields.len());
            for name in fields.keys() {
                w.scalar(TAG_STRING, name.as_bytes());
                key_ends.record_end(w);
            }
            w.begin_map_values(&mut value_ends);
            for value in fields.values() {
                encode_flat_variant(w, value);
                value_ends.record_end(w);
            }
            start..w.out.len()
        }
    }
}

/// Append the flat encoding of the decimal `integer * 10^-scale`, in canonical form.
fn encode_flat_decimal(w: &mut Writer, integer: i128, scale: u8) -> Range<usize> {
    let (integer, scale) = canonical_decimal(integer, scale);
    w.scalar(TAG_DECIMAL, &decimal_payload(integer, scale))
}

/// The decimal `integer * 10^-scale` in the canonical form that every other `VARIANT` decimal
/// has: the smallest scale that holds it, so `(1500, 2)` becomes `(15, 0)`. Without it, equal
/// decimals would compare and hash as different values.
fn canonical_decimal(integer: i128, scale: u8) -> (i128, u8) {
    let decimal = DynamicDecimal::new(integer, scale);
    (decimal.significand(), decimal.exponent())
}

/// `time`, with a leap second moved back to the last nanosecond of the second before it. A
/// SQL `TIME` has no leap seconds: [`Time`] counts nanoseconds from midnight, so a leap second
/// would count past the end of the day.
fn without_leap_second(time: NaiveTime) -> NaiveTime {
    if time.nanosecond() < 1_000_000_000 {
        return time;
    }
    NaiveTime::from_num_seconds_from_midnight_opt(time.num_seconds_from_midnight(), 999_999_999)
        .expect("the second before a leap second is a time of day")
}

/// The flat-encoded value at `bytes` as a [`VariantValue`]. `depth` counts
/// the containers above it, so a value nested too deeply fails the way the Parquet encoder
/// fails it.
fn flat_variant_value(bytes: &[u8], depth: usize) -> Result<VariantValue, String> {
    if depth > MAX_VARIANT_DEPTH {
        return Err(variant_too_deep());
    }
    let p = &bytes[1..];
    Ok(match bytes[0] {
        TAG_SQL_NULL | TAG_VARIANT_NULL => VariantValue::Null,
        TAG_BOOLEAN => VariantValue::Boolean(p[0] != 0),
        TAG_TINYINT => VariantValue::Int8(p[0] as i8),
        TAG_SMALLINT => VariantValue::Int16(i16::from_le_bytes(payload_array(p))),
        TAG_INT => VariantValue::Int32(i32::from_le_bytes(payload_array(p))),
        TAG_BIGINT => VariantValue::Int64(i64::from_le_bytes(payload_array(p))),
        TAG_UTINYINT => VariantValue::Int16(p[0].into()),
        TAG_USMALLINT => VariantValue::Int32(u16::from_le_bytes(payload_array(p)).into()),
        TAG_UINT => VariantValue::Int64(u32::from_le_bytes(payload_array(p)).into()),
        TAG_UBIGINT => u64_value(u64::from_le_bytes(payload_array(p))),
        TAG_REAL => VariantValue::Float(f32::from_le_bytes(payload_array(p))),
        TAG_DOUBLE => VariantValue::Double(f64::from_le_bytes(payload_array(p))),
        TAG_DECIMAL => decimal_value(i128::from_le_bytes(payload_array(&p[..16])), p[16])?,
        TAG_STRING => VariantValue::String(
            std::str::from_utf8(p)
                .expect("encoded string must be UTF-8")
                .to_string(),
        ),
        TAG_BINARY => VariantValue::Binary(p.to_vec()),
        TAG_UUID => VariantValue::Uuid(uuid::Uuid::from_bytes(payload_array(p))),
        TAG_DATE => {
            VariantValue::Date(Date::from_days(i32::from_le_bytes(payload_array(p))).to_date())
        }
        TAG_TIME => VariantValue::Time(
            Time::from_nanoseconds(u64::from_le_bytes(payload_array(p))).to_time(),
        ),
        TAG_TIMESTAMP => VariantValue::TimestampNtzMicros(
            Timestamp::from_microseconds(i64::from_le_bytes(payload_array(p)))
                .to_dateTime()
                .naive_utc(),
        ),
        TAG_TIMESTAMP_TZ => VariantValue::TimestampMicros(
            TimestampTz::from_microseconds(i64::from_le_bytes(payload_array(p))).to_dateTime(),
        ),
        TAG_SHORT_INTERVAL | TAG_LONG_INTERVAL | TAG_GEO_POINT => {
            return Err(unencodable_variant(type_string(bytes)));
        }
        TAG_ARRAY => {
            let container = Container::new(bytes);
            VariantValue::List(
                (0..container.count)
                    .map(|i| flat_variant_value(&bytes[container.element(i)], depth + 1))
                    .collect::<Result<_, _>>()?,
            )
        }
        TAG_MAP => {
            let container = Container::new(bytes);
            let mut fields = BTreeMap::new();
            for i in 0..container.count {
                let name = flat_variant_object_key(&bytes[container.element(i)])?.into_owned();
                let value = flat_variant_value(&bytes[container.map_value(i)], depth + 1)?;
                insert_field(&mut fields, name, value)?;
            }
            VariantValue::Object(fields)
        }
        tag => unreachable!("invalid tag {tag}"),
    })
}

/// `value` as a [`VariantValue`]. `depth` counts the containers above
/// `value`, so a value nested too deeply fails the way the Parquet encoder fails it.
fn variant_value(value: &Variant, depth: usize) -> Result<VariantValue, String> {
    if depth > MAX_VARIANT_DEPTH {
        return Err(variant_too_deep());
    }
    Ok(match value {
        Variant::SqlNull | Variant::VariantNull => VariantValue::Null,
        Variant::Boolean(v) => VariantValue::Boolean(*v),
        Variant::TinyInt(v) => VariantValue::Int8(*v),
        Variant::SmallInt(v) => VariantValue::Int16(*v),
        Variant::Int(v) => VariantValue::Int32(*v),
        Variant::BigInt(v) => VariantValue::Int64(*v),
        Variant::UTinyInt(v) => VariantValue::Int16((*v).into()),
        Variant::USmallInt(v) => VariantValue::Int32((*v).into()),
        Variant::UInt(v) => VariantValue::Int64((*v).into()),
        Variant::UBigInt(v) => u64_value(*v),
        Variant::Real(v) => VariantValue::Float(v.into_inner()),
        Variant::Double(v) => VariantValue::Double(v.into_inner()),
        Variant::SqlDecimal((integer, scale)) => decimal_value(*integer, *scale)?,
        Variant::String(v) => VariantValue::String(v.str().to_string()),
        Variant::Binary(v) => VariantValue::Binary(v.as_slice().to_vec()),
        Variant::Uuid(v) => VariantValue::Uuid(uuid::Uuid::from_bytes(*v.to_bytes())),
        Variant::Date(v) => VariantValue::Date(v.to_date()),
        Variant::Time(v) => VariantValue::Time(v.to_time()),
        Variant::Timestamp(v) => VariantValue::TimestampNtzMicros(v.to_dateTime().naive_utc()),
        Variant::TimestampTz(v) => VariantValue::TimestampMicros(v.to_dateTime()),
        Variant::ShortInterval(_) | Variant::LongInterval(_) | Variant::Geometry(_) => {
            return Err(unencodable_variant(value.get_type_string()));
        }
        Variant::Array(elements) => VariantValue::List(
            elements
                .iter()
                .map(|element| variant_value(element, depth + 1))
                .collect::<Result<_, _>>()?,
        ),
        Variant::Map(entries) => {
            let mut fields = BTreeMap::new();
            for (key, value) in entries.iter() {
                let name = variant_object_key(key)?.into_owned();
                let value = variant_value(value, depth + 1)?;
                insert_field(&mut fields, name, value)?;
            }
            VariantValue::Object(fields)
        }
    })
}

/// A `u64` as the smallest signed type that holds it: an `Int64` when it fits, and a
/// decimal of scale 0 when it does not.
fn u64_value(value: u64) -> VariantValue {
    match i64::try_from(value) {
        Ok(value) => VariantValue::Int64(value),
        Err(_) => VariantValue::Decimal16(
            VariantDecimal16::try_new(value.into(), 0).expect("a u64 has at most 20 digits"),
        ),
    }
}

/// `integer * 10^-scale` in the narrowest decimal width that holds it. A width can take
/// `integer`'s type and still refuse its digits or its scale, so a refusal moves on to the
/// next width.
fn decimal_value(integer: i128, scale: u8) -> Result<VariantValue, String> {
    if let Ok(narrow) = i32::try_from(integer)
        && let Ok(decimal) = VariantDecimal4::try_new(narrow, scale)
    {
        return Ok(VariantValue::Decimal4(decimal));
    }
    if let Ok(narrow) = i64::try_from(integer)
        && let Ok(decimal) = VariantDecimal8::try_new(narrow, scale)
    {
        return Ok(VariantValue::Decimal8(decimal));
    }
    VariantDecimal16::try_new(integer, scale)
        .map(VariantValue::Decimal16)
        .map_err(|error| format!("cannot convert a VARIANT decimal to a VariantValue: {error}"))
}

/// Add the field `name` to `fields`. Fails if `fields` has it already: two map keys that
/// name one object field.
fn insert_field(
    fields: &mut BTreeMap<String, VariantValue>,
    name: String,
    value: VariantValue,
) -> Result<(), String> {
    match fields.entry(name) {
        Entry::Vacant(field) => {
            field.insert(value);
            Ok(())
        }
        Entry::Occupied(field) => Err(format!(
            "cannot convert a VARIANT map to a VariantValue: two keys name the object field \
             '{}'; an object's field names are strings, so an integer key and the string of \
             its digits collide",
            field.key()
        )),
    }
}

#[cfg(test)]
mod tests {
    //! The conversions against the Parquet variant encoding, which defines the mapping: a
    //! value that goes through `VariantValue` must come back as the value the encoding decodes
    //! it to, and fail where the encoding fails. The conversions share no code with the
    //! encoding, so the two agree only if each maps every type the same way.

    use std::collections::BTreeMap;
    use std::sync::Arc;

    use chrono::{NaiveDate, NaiveTime, TimeZone, Utc};
    use feldera_types::variant::{
        VariantDecimal4, VariantDecimal8, VariantDecimal16, VariantValue,
    };
    use parquet_variant::{Variant as ParquetVariant, VariantBuilder};

    use crate::flat_variant::casts::decimal_payload;
    use crate::flat_variant::{
        FlatVariant, TAG_DECIMAL, TAG_INT, TAG_STRING, build_document_infallible,
        flat_variant_to_parquet,
    };
    use crate::variant::Variant;
    use crate::variant_binary::{FromVariantEncoding, MAX_VARIANT_DEPTH, to_variant_binary};
    use crate::{
        ByteArray, Date, F32, F64, GeoPoint, LongInterval, ShortInterval, SqlString, Time,
        Timestamp, TimestampTz, Uuid,
    };

    /// The bounds `feldera_types::variant` checks are the Parquet variant's, so a decimal
    /// moves between the two decimal types without failing.
    const SAME_DECIMAL_BOUNDS: &str = "the two variant decimal types have the same bounds";

    /// A scalar `VariantValue` as the Parquet variant it mirrors.
    fn parquet_scalar(value: &VariantValue) -> ParquetVariant<'_, '_> {
        match value {
            VariantValue::Null => ParquetVariant::Null,
            VariantValue::Boolean(v) => ParquetVariant::from(*v),
            VariantValue::Int8(v) => ParquetVariant::Int8(*v),
            VariantValue::Int16(v) => ParquetVariant::Int16(*v),
            VariantValue::Int32(v) => ParquetVariant::Int32(*v),
            VariantValue::Int64(v) => ParquetVariant::Int64(*v),
            VariantValue::Float(v) => ParquetVariant::Float(*v),
            VariantValue::Double(v) => ParquetVariant::Double(*v),
            VariantValue::Decimal4(v) => ParquetVariant::Decimal4(
                parquet_variant::VariantDecimal4::try_new(v.integer(), v.scale())
                    .expect(SAME_DECIMAL_BOUNDS),
            ),
            VariantValue::Decimal8(v) => ParquetVariant::Decimal8(
                parquet_variant::VariantDecimal8::try_new(v.integer(), v.scale())
                    .expect(SAME_DECIMAL_BOUNDS),
            ),
            VariantValue::Decimal16(v) => ParquetVariant::Decimal16(
                parquet_variant::VariantDecimal16::try_new(v.integer(), v.scale())
                    .expect(SAME_DECIMAL_BOUNDS),
            ),
            VariantValue::String(v) => ParquetVariant::from(v.as_str()),
            VariantValue::Binary(v) => ParquetVariant::Binary(v),
            VariantValue::Date(v) => ParquetVariant::Date(*v),
            VariantValue::Time(v) => ParquetVariant::Time(*v),
            VariantValue::TimestampMicros(v) => ParquetVariant::TimestampMicros(*v),
            VariantValue::TimestampNtzMicros(v) => ParquetVariant::TimestampNtzMicros(*v),
            VariantValue::TimestampNanos(v) => ParquetVariant::TimestampNanos(*v),
            VariantValue::TimestampNtzNanos(v) => ParquetVariant::TimestampNtzNanos(*v),
            VariantValue::Uuid(v) => ParquetVariant::Uuid(*v),
            VariantValue::List(_) | VariantValue::Object(_) => {
                unreachable!("a container encodes element by element")
            }
        }
    }

    /// A scalar Parquet variant as the `VariantValue` that mirrors it.
    fn scalar_value(value: ParquetVariant<'_, '_>) -> VariantValue {
        match value {
            ParquetVariant::Null => VariantValue::Null,
            ParquetVariant::BooleanTrue => VariantValue::Boolean(true),
            ParquetVariant::BooleanFalse => VariantValue::Boolean(false),
            ParquetVariant::Int8(v) => VariantValue::Int8(v),
            ParquetVariant::Int16(v) => VariantValue::Int16(v),
            ParquetVariant::Int32(v) => VariantValue::Int32(v),
            ParquetVariant::Int64(v) => VariantValue::Int64(v),
            ParquetVariant::Float(v) => VariantValue::Float(v),
            ParquetVariant::Double(v) => VariantValue::Double(v),
            ParquetVariant::Decimal4(v) => VariantValue::Decimal4(
                VariantDecimal4::try_new(v.integer(), v.scale()).expect(SAME_DECIMAL_BOUNDS),
            ),
            ParquetVariant::Decimal8(v) => VariantValue::Decimal8(
                VariantDecimal8::try_new(v.integer(), v.scale()).expect(SAME_DECIMAL_BOUNDS),
            ),
            ParquetVariant::Decimal16(v) => VariantValue::Decimal16(
                VariantDecimal16::try_new(v.integer(), v.scale()).expect(SAME_DECIMAL_BOUNDS),
            ),
            ParquetVariant::String(v) => VariantValue::String(v.to_string()),
            ParquetVariant::ShortString(v) => VariantValue::String(v.as_str().to_string()),
            ParquetVariant::Binary(v) => VariantValue::Binary(v.to_vec()),
            ParquetVariant::Date(v) => VariantValue::Date(v),
            ParquetVariant::Time(v) => VariantValue::Time(v),
            ParquetVariant::TimestampMicros(v) => VariantValue::TimestampMicros(v),
            ParquetVariant::TimestampNtzMicros(v) => VariantValue::TimestampNtzMicros(v),
            ParquetVariant::TimestampNanos(v) => VariantValue::TimestampNanos(v),
            ParquetVariant::TimestampNtzNanos(v) => VariantValue::TimestampNtzNanos(v),
            ParquetVariant::Uuid(v) => VariantValue::Uuid(v),
            ParquetVariant::List(_) | ParquetVariant::Object(_) => {
                unreachable!("a container decodes element by element")
            }
        }
    }

    fn key(name: &str) -> Variant {
        Variant::String(SqlString::from_ref(name))
    }

    /// `value` written in the Parquet variant encoding and read back, or the encoder's
    /// error.
    fn through_the_encoding(value: &Variant) -> Result<Variant, String> {
        let (metadata, encoded) = to_variant_binary(value)?;
        Variant::from_binary(&metadata, &encoded)
    }

    /// `value` converted to a `VariantValue` and back.
    fn through_variant_value(value: &Variant) -> Result<Variant, String> {
        VariantValue::try_from(value)
            .map(|value| Variant::from(&value))
            .map_err(|error| error.to_string())
    }

    /// Every `VariantValue` arm, each paired with the `Variant` it converts to.
    fn every_arm() -> Vec<(VariantValue, Variant)> {
        let micros = 1_700_000_000_000_001;
        let date = NaiveDate::from_ymd_opt(2020, 1, 2).unwrap();
        let time = NaiveTime::from_hms_micro_opt(10, 20, 30, 400_500).unwrap();
        // Not a palindrome, so a swapped byte order shows.
        let uuid: [u8; 16] = std::array::from_fn(|i| i as u8);
        vec![
            (VariantValue::Null, Variant::VariantNull),
            (VariantValue::Boolean(true), Variant::Boolean(true)),
            (VariantValue::Int8(-8), Variant::TinyInt(-8)),
            (VariantValue::Int16(-16), Variant::SmallInt(-16)),
            (VariantValue::Int32(-32), Variant::Int(-32)),
            (VariantValue::Int64(-64), Variant::BigInt(-64)),
            (VariantValue::Float(1.5), Variant::Real(F32::new(1.5))),
            (VariantValue::Double(2.5), Variant::Double(F64::new(2.5))),
            (
                VariantValue::Decimal4(VariantDecimal4::try_new(1_234_567, 4).unwrap()),
                Variant::SqlDecimal((1_234_567, 4)),
            ),
            (
                VariantValue::Decimal8(VariantDecimal8::try_new(123_456_789_012, 6).unwrap()),
                Variant::SqlDecimal((123_456_789_012, 6)),
            ),
            (
                VariantValue::Decimal16(
                    VariantDecimal16::try_new(12_345_678_901_234_567_891, 8).unwrap(),
                ),
                Variant::SqlDecimal((12_345_678_901_234_567_891, 8)),
            ),
            (VariantValue::String("short".into()), key("short")),
            (
                VariantValue::String("a string long enough not to be short".repeat(3)),
                key(&"a string long enough not to be short".repeat(3)),
            ),
            (
                VariantValue::Binary(vec![0, 1, 255]),
                Variant::Binary(ByteArray::new(&[0, 1, 255])),
            ),
            (
                VariantValue::Date(date),
                Variant::Date(Date::from_date(date)),
            ),
            (
                VariantValue::Time(time),
                Variant::Time(Time::from_time(time)),
            ),
            (
                VariantValue::TimestampMicros(Utc.timestamp_micros(micros).unwrap()),
                Variant::TimestampTz(TimestampTz::from_microseconds(micros)),
            ),
            (
                VariantValue::TimestampNtzMicros(Utc.timestamp_micros(micros).unwrap().naive_utc()),
                Variant::Timestamp(Timestamp::from_microseconds(micros)),
            ),
            (
                VariantValue::Uuid(uuid::Uuid::from_bytes(uuid)),
                Variant::Uuid(Uuid::from_bytes(uuid)),
            ),
            (
                VariantValue::List(vec![VariantValue::Int64(1), VariantValue::Null]),
                Variant::Array(vec![Variant::BigInt(1), Variant::VariantNull].into()),
            ),
            (
                VariantValue::Object(BTreeMap::from([
                    ("a".to_string(), VariantValue::Int32(1)),
                    (
                        "b".to_string(),
                        VariantValue::List(vec![VariantValue::Binary(vec![7])]),
                    ),
                ])),
                Variant::Map(
                    BTreeMap::from([
                        (key("a"), Variant::Int(1)),
                        (
                            key("b"),
                            Variant::Array(vec![Variant::Binary(ByteArray::new(&[7]))].into()),
                        ),
                    ])
                    .into(),
                ),
            ),
            (
                VariantValue::List(Vec::new()),
                Variant::Array(Vec::<Variant>::new().into()),
            ),
            (
                VariantValue::Object(BTreeMap::new()),
                Variant::Map(BTreeMap::<Variant, Variant>::new().into()),
            ),
            // Field names in byte order: the empty name, then upper case before lower case,
            // then a name past ASCII, in an object in an object, as Kafka headers sit in the
            // connector metadata.
            (
                VariantValue::Object(BTreeMap::from([
                    ("".to_string(), VariantValue::Null),
                    ("B".to_string(), VariantValue::Int64(2)),
                    (
                        "a".to_string(),
                        VariantValue::Object(BTreeMap::from([(
                            "é".to_string(),
                            VariantValue::Binary(vec![1]),
                        )])),
                    ),
                ])),
                Variant::Map(
                    BTreeMap::from([
                        (key(""), Variant::VariantNull),
                        (key("B"), Variant::BigInt(2)),
                        (
                            key("a"),
                            Variant::Map(
                                BTreeMap::from([(key("é"), Variant::Binary(ByteArray::new(&[1])))])
                                    .into(),
                            ),
                        ),
                    ])
                    .into(),
                ),
            ),
        ]
    }

    /// Each arm converts to its `Variant`, which converts back to the same value, and the
    /// two directions agree with the Parquet encoding.
    #[test]
    fn every_arm_converts_both_ways() {
        for (value, variant) in every_arm() {
            assert_eq!(Variant::from(&value), variant, "{value:?}");
            assert_eq!(Variant::from(value.clone()), variant, "{value:?}");
            assert_eq!(
                VariantValue::try_from(&variant).unwrap(),
                value,
                "{value:?}"
            );
            assert_eq!(
                through_the_encoding(&variant).unwrap(),
                variant,
                "{value:?} through the encoding"
            );
        }
    }

    /// A nanosecond timestamp keeps its microseconds: the `Variant` timestamps and the flat
    /// encoding hold no finer unit, so the value reads back as the microsecond arm.
    #[test]
    fn nanosecond_timestamps_truncate_to_microseconds() {
        let nanos = Utc.timestamp_nanos(1_700_000_000_000_001_999);
        let micros = Utc.timestamp_micros(1_700_000_000_000_001).unwrap();
        // Before the epoch a timestamp rounds down: -1.5 microseconds becomes -2.
        let before_epoch = Utc.timestamp_nanos(-1_500);
        let before_epoch_micros = Utc.timestamp_micros(-2).unwrap();
        let cases = [
            (
                VariantValue::TimestampNanos(nanos),
                Variant::TimestampTz(TimestampTz::from_microseconds(1_700_000_000_000_001)),
                VariantValue::TimestampMicros(micros),
            ),
            (
                VariantValue::TimestampNtzNanos(nanos.naive_utc()),
                Variant::Timestamp(Timestamp::from_microseconds(1_700_000_000_000_001)),
                VariantValue::TimestampNtzMicros(micros.naive_utc()),
            ),
            (
                VariantValue::TimestampNanos(before_epoch),
                Variant::TimestampTz(TimestampTz::from_microseconds(-2)),
                VariantValue::TimestampMicros(before_epoch_micros),
            ),
            (
                VariantValue::TimestampNtzNanos(before_epoch.naive_utc()),
                Variant::Timestamp(Timestamp::from_microseconds(-2)),
                VariantValue::TimestampNtzMicros(before_epoch_micros.naive_utc()),
            ),
        ];
        for (value, variant, truncated) in cases {
            assert_eq!(Variant::from(&value), variant, "{value:?}");
            let flat = FlatVariant::from(&value);
            let (metadata, encoded) = parquet_encoding(&value);
            assert_eq!(
                flat,
                FlatVariant::from_binary(&metadata, &encoded).unwrap(),
                "{value:?}"
            );
            assert_eq!(
                VariantValue::try_from(&flat).unwrap(),
                truncated,
                "{value:?}"
            );
        }
    }

    /// A decimal takes the narrowest width that holds both its digits and its scale, and
    /// fails only past 38 of either.
    #[test]
    fn decimals_take_the_narrowest_width_that_holds_them() {
        let decimal4 = |integer, scale| {
            VariantValue::Decimal4(VariantDecimal4::try_new(integer, scale).unwrap())
        };
        let decimal8 = |integer, scale| {
            VariantValue::Decimal8(VariantDecimal8::try_new(integer, scale).unwrap())
        };
        let decimal16 = |integer, scale| {
            VariantValue::Decimal16(VariantDecimal16::try_new(integer, scale).unwrap())
        };
        let cases = [
            ((999_999_999, 9), Some(decimal4(999_999_999, 9))),
            // Fits an `i32`, but ten digits are too many for four bytes.
            ((1_500_000_000, 0), Some(decimal8(1_500_000_000, 0))),
            // Fits an `i32`, but a scale of 10 is too large for four bytes.
            ((1, 10), Some(decimal8(1, 10))),
            (
                (-5_000_000_000_000_000_000, 0),
                Some(decimal16(-5_000_000_000_000_000_000, 0)),
            ),
            ((1, 19), Some(decimal16(1, 19))),
            ((10i128.pow(38), 0), None),
            ((1, 39), None),
        ];
        for ((integer, scale), value) in cases {
            let variant = Variant::SqlDecimal((integer, scale));
            let flat = build_document_infallible(|w| {
                w.scalar(TAG_DECIMAL, &decimal_payload(integer, scale))
            });
            assert_eq!(VariantValue::try_from(&variant).ok(), value, "{variant:?}");
            assert_eq!(VariantValue::try_from(&flat).ok(), value, "{variant:?}");
        }
    }

    /// The `Variant` types the encoding lacks convert the way the encoding writes them.
    #[test]
    fn variant_only_types_convert_like_the_encoding() {
        let cases = [
            (Variant::SqlNull, VariantValue::Null),
            (Variant::UTinyInt(200), VariantValue::Int16(200)),
            (Variant::USmallInt(60_000), VariantValue::Int32(60_000)),
            (
                Variant::UInt(4_000_000_000),
                VariantValue::Int64(4_000_000_000),
            ),
            (Variant::UBigInt(5), VariantValue::Int64(5)),
            (
                Variant::UBigInt(i64::MAX as u64),
                VariantValue::Int64(i64::MAX),
            ),
            (
                Variant::UBigInt(i64::MAX as u64 + 1),
                VariantValue::Decimal16(
                    VariantDecimal16::try_new(i64::MAX as i128 + 1, 0).unwrap(),
                ),
            ),
            (
                Variant::UBigInt(u64::MAX),
                VariantValue::Decimal16(VariantDecimal16::try_new(u64::MAX as i128, 0).unwrap()),
            ),
            (
                Variant::SqlDecimal((12_345, 2)),
                VariantValue::Decimal4(VariantDecimal4::try_new(12_345, 2).unwrap()),
            ),
            (
                Variant::Map(BTreeMap::from([(Variant::Int(7), Variant::Boolean(false))]).into()),
                VariantValue::Object(BTreeMap::from([(
                    "7".to_string(),
                    VariantValue::Boolean(false),
                )])),
            ),
        ];
        for (variant, value) in cases {
            assert_eq!(
                VariantValue::try_from(&variant).unwrap(),
                value,
                "{variant:?}"
            );
            assert_eq!(
                through_variant_value(&variant),
                through_the_encoding(&variant),
                "{variant:?}"
            );
        }
    }

    /// A `Variant` the encoding cannot write has no `VariantValue` either.
    #[test]
    fn unencodable_variants_fail() {
        let in_a_map = |value: Variant| Variant::Map(BTreeMap::from([(key("x"), value)]).into());
        let cases = [
            (
                Variant::ShortInterval(ShortInterval::from_microseconds(1)),
                "cannot be encoded as a Parquet variant",
            ),
            (
                Variant::LongInterval(LongInterval::from_months(1)),
                "cannot be encoded as a Parquet variant",
            ),
            (
                in_a_map(Variant::Geometry(GeoPoint::new(1.0, 2.0))),
                "cannot be encoded as a Parquet variant",
            ),
            (
                Variant::Map(BTreeMap::from([(Variant::Boolean(true), Variant::Int(1))]).into()),
                "a variant object's field names are strings",
            ),
        ];
        for (variant, message) in cases {
            let error = VariantValue::try_from(&variant).unwrap_err().to_string();
            assert!(error.contains(message), "{variant:?}: {error}");
            assert!(through_the_encoding(&variant).is_err(), "{variant:?}");
        }
    }

    /// An integer key and the string of its digits name one object field, so a map that
    /// holds both has no `VariantValue`.
    #[test]
    fn keys_that_name_one_field_fail() {
        let variant = Variant::Map(
            BTreeMap::from([
                (Variant::Int(1), Variant::Int(10)),
                (key("1"), Variant::Int(11)),
            ])
            .into(),
        );
        let error = VariantValue::try_from(&variant).unwrap_err().to_string();
        assert!(
            error.contains("two keys name the object field '1'"),
            "{error}"
        );
        assert!(through_the_encoding(&variant).is_err());
    }

    /// A container nested deeper than the encoding allows fails like the encoding, and one
    /// level less converts.
    #[test]
    fn too_deep_a_variant_fails() {
        let nested = |levels: usize| {
            let mut variant = Variant::BigInt(1);
            for _ in 0..levels {
                variant = Variant::Array(vec![variant].into());
            }
            variant
        };
        let deepest = nested(MAX_VARIANT_DEPTH - 1);
        assert!(VariantValue::try_from(&deepest).is_ok());
        assert!(through_the_encoding(&deepest).is_ok());
        let variant = nested(MAX_VARIANT_DEPTH);
        let error = VariantValue::try_from(&variant).unwrap_err().to_string();
        assert!(error.contains("nested more than"), "{error}");
        assert!(through_the_encoding(&variant).is_err());
    }

    /// `value` written with the Parquet variant builder, a path that shares nothing with the
    /// flat encoder.
    fn parquet_encoding(value: &VariantValue) -> (Vec<u8>, Vec<u8>) {
        let mut builder = VariantBuilder::new();
        match value {
            VariantValue::List(elements) => {
                let mut list = builder.new_list();
                for element in elements {
                    let (metadata, encoded) = parquet_encoding(element);
                    list.append_value(ParquetVariant::try_new(&metadata, &encoded).unwrap());
                }
                list.finish();
            }
            VariantValue::Object(fields) => {
                let mut object = builder.new_object();
                for (name, field) in fields {
                    let (metadata, encoded) = parquet_encoding(field);
                    object.insert(name, ParquetVariant::try_new(&metadata, &encoded).unwrap());
                }
                object.finish();
            }
            scalar => builder.append_value(parquet_scalar(scalar)),
        }
        builder.finish()
    }

    /// A decoded Parquet variant as the `VariantValue` that mirrors it.
    fn parquet_value(value: ParquetVariant<'_, '_>) -> VariantValue {
        match value {
            ParquetVariant::List(list) => {
                VariantValue::List(list.iter().map(parquet_value).collect())
            }
            ParquetVariant::Object(object) => VariantValue::Object(
                object
                    .iter()
                    .map(|(name, field)| (name.to_string(), parquet_value(field)))
                    .collect(),
            ),
            scalar => scalar_value(scalar),
        }
    }

    /// `value` written in the Parquet variant encoding by the flat encoding's own encoder,
    /// read back as a `VariantValue`, or the encoder's error.
    fn flat_through_the_encoding(value: &FlatVariant) -> Result<VariantValue, String> {
        let (metadata, encoded) = flat_variant_to_parquet(value)?;
        Ok(parquet_value(
            ParquetVariant::try_new(&metadata, &encoded).unwrap(),
        ))
    }

    /// Each arm writes the flat encoding the Parquet path decodes to, reads back as itself,
    /// and agrees with the enum's path while the enum exists.
    #[test]
    fn every_arm_converts_to_a_flat_variant_and_back() {
        for (value, variant) in every_arm() {
            let flat = FlatVariant::from(&value);
            assert_eq!(FlatVariant::from(value.clone()), flat, "{value:?}");
            let (metadata, encoded) = parquet_encoding(&value);
            assert_eq!(
                flat,
                FlatVariant::from_binary(&metadata, &encoded).unwrap(),
                "{value:?}"
            );
            assert_eq!(VariantValue::try_from(&flat).unwrap(), value, "{value:?}");
            assert_eq!(
                flat_through_the_encoding(&flat).unwrap(),
                value,
                "{value:?}"
            );
            assert_eq!(flat, FlatVariant::from(&variant), "{value:?}");
        }
    }

    /// A document read from JSON converts to the attributes it holds, and back to the same
    /// JSON.
    #[test]
    fn a_json_flat_variant_converts_both_ways() {
        let document: FlatVariant =
            serde_json::from_str(r#"{"topic": "events", "offsets": [1, 2]}"#).unwrap();
        // JSON reads a non-negative integer as unsigned, which widens to a signed width.
        let value = VariantValue::try_from(&document).unwrap();
        assert_eq!(
            value,
            VariantValue::Object(BTreeMap::from([
                (
                    "offsets".to_string(),
                    VariantValue::List(vec![VariantValue::Int64(1), VariantValue::Int64(2)]),
                ),
                ("topic".to_string(), VariantValue::String("events".into())),
            ]))
        );
        assert_eq!(
            FlatVariant::from(&value).to_json_string().unwrap(),
            document.to_json_string().unwrap()
        );
    }

    /// The flat types the encoding lacks convert the way the encoding writes them.
    #[test]
    fn flat_only_types_convert_like_the_encoding() {
        let cases = [
            (FlatVariant::sql_null(), VariantValue::Null),
            (FlatVariant::from(200u8), VariantValue::Int16(200)),
            (FlatVariant::from(60_000u16), VariantValue::Int32(60_000)),
            (
                FlatVariant::from(4_000_000_000u32),
                VariantValue::Int64(4_000_000_000),
            ),
            (FlatVariant::from(5u64), VariantValue::Int64(5)),
            (
                FlatVariant::from(i64::MAX as u64),
                VariantValue::Int64(i64::MAX),
            ),
            (
                FlatVariant::from(i64::MAX as u64 + 1),
                VariantValue::Decimal16(
                    VariantDecimal16::try_new(i64::MAX as i128 + 1, 0).unwrap(),
                ),
            ),
            (
                FlatVariant::from(u64::MAX),
                VariantValue::Decimal16(VariantDecimal16::try_new(u64::MAX as i128, 0).unwrap()),
            ),
            (
                FlatVariant::from(Arc::new(BTreeMap::from([(7i32, false)]))),
                VariantValue::Object(BTreeMap::from([(
                    "7".to_string(),
                    VariantValue::Boolean(false),
                )])),
            ),
        ];
        for (flat, value) in cases {
            assert_eq!(VariantValue::try_from(&flat).unwrap(), value, "{flat:?}");
            assert_eq!(flat_through_the_encoding(&flat).unwrap(), value, "{flat:?}");
        }
    }

    /// A flat value the encoding cannot write has no `VariantValue` either.
    #[test]
    fn unencodable_flat_variants_fail() {
        let cases = [
            (
                FlatVariant::from(ShortInterval::from_microseconds(1)),
                "cannot be encoded as a Parquet variant",
            ),
            (
                FlatVariant::from(LongInterval::from_months(1)),
                "cannot be encoded as a Parquet variant",
            ),
            (
                FlatVariant::from(Arc::new(vec![GeoPoint::new(1.0, 2.0)])),
                "cannot be encoded as a Parquet variant",
            ),
            (
                FlatVariant::from(Arc::new(BTreeMap::from([(true, 1i32)]))),
                "a variant object's field names are strings",
            ),
        ];
        for (flat, message) in cases {
            let error = VariantValue::try_from(&flat).unwrap_err().to_string();
            assert!(error.contains(message), "{flat:?}: {error}");
            assert!(flat_through_the_encoding(&flat).is_err(), "{flat:?}");
        }
    }

    /// An integer key and the string of its digits name one object field, so a flat map
    /// that holds both has no `VariantValue`.
    #[test]
    fn flat_keys_that_name_one_field_fail() {
        // The keys sort integer first, as the encoding requires.
        let flat = build_document_infallible(|w| {
            let entries = vec![
                (
                    w.scalar(TAG_INT, &1i32.to_le_bytes()),
                    w.scalar(TAG_INT, &10i32.to_le_bytes()),
                ),
                (
                    w.scalar(TAG_STRING, b"1"),
                    w.scalar(TAG_INT, &11i32.to_le_bytes()),
                ),
            ];
            w.map(&entries)
        });
        let error = VariantValue::try_from(&flat).unwrap_err().to_string();
        assert!(
            error.contains("two keys name the object field '1'"),
            "{error}"
        );
        assert!(flat_through_the_encoding(&flat).is_err());
    }

    /// A flat value nested deeper than the encoding allows fails like the encoding, and one
    /// level less converts.
    #[test]
    fn too_deep_a_flat_variant_fails() {
        let nested = |levels: usize| {
            let mut flat = FlatVariant::from(1i64);
            for _ in 0..levels {
                flat = build_document_infallible(|w| {
                    w.array_in_place(1, |w, _| {
                        w.raw(flat.as_bytes());
                    })
                });
            }
            flat
        };
        let deepest = nested(MAX_VARIANT_DEPTH - 1);
        assert!(VariantValue::try_from(&deepest).is_ok());
        assert!(flat_through_the_encoding(&deepest).is_ok());
        let flat = nested(MAX_VARIANT_DEPTH);
        let error = VariantValue::try_from(&flat).unwrap_err().to_string();
        assert!(error.contains("nested more than"), "{error}");
        assert!(flat_through_the_encoding(&flat).is_err());
    }

    /// A decimal converts to the canonical form that JSON gives the same number, so the two
    /// compare equal.
    #[test]
    fn decimals_convert_in_canonical_form() {
        let cases = [
            (
                VariantValue::Decimal4(VariantDecimal4::try_new(1500, 2).unwrap()),
                "15.00",
                VariantValue::Decimal4(VariantDecimal4::try_new(15, 0).unwrap()),
            ),
            (
                VariantValue::Decimal8(VariantDecimal8::try_new(0, 5).unwrap()),
                "0.00000",
                VariantValue::Decimal4(VariantDecimal4::try_new(0, 0).unwrap()),
            ),
            (
                VariantValue::Decimal16(VariantDecimal16::try_new(-1200, 3).unwrap()),
                "-1.200",
                VariantValue::Decimal4(VariantDecimal4::try_new(-12, 1).unwrap()),
            ),
        ];
        for (value, json, canonical) in cases {
            let variant = Variant::from(&value);
            assert_eq!(
                variant,
                serde_json::from_str::<Variant>(json).unwrap(),
                "{value:?}"
            );
            let flat = FlatVariant::from(&value);
            assert_eq!(
                flat,
                serde_json::from_str::<FlatVariant>(json).unwrap(),
                "{value:?}"
            );
            assert_eq!(VariantValue::try_from(&variant).unwrap(), canonical);
            assert_eq!(VariantValue::try_from(&flat).unwrap(), canonical);
        }
    }

    /// A leap second becomes the last nanosecond of the second before it, a TIME that every
    /// later step can read.
    #[test]
    fn a_leap_second_becomes_the_last_nanosecond_before_it() {
        let leap = NaiveTime::from_hms_milli_opt(23, 59, 59, 1_500).unwrap();
        let last = NaiveTime::from_hms_nano_opt(23, 59, 59, 999_999_999).unwrap();
        let variant = Variant::from(&VariantValue::Time(leap));
        assert_eq!(variant, Variant::Time(Time::from_time(last)));
        assert_eq!(
            VariantValue::try_from(&variant).unwrap(),
            VariantValue::Time(last)
        );
        let flat = FlatVariant::from(&VariantValue::Time(leap));
        assert_eq!(
            VariantValue::try_from(&flat).unwrap(),
            VariantValue::Time(last)
        );
        assert!(flat.to_json_string().is_ok());
    }

    /// A TIME finer than a microsecond converts the same way to both representations.
    #[test]
    fn both_representations_agree_on_a_time_finer_than_a_microsecond() {
        let value =
            VariantValue::Time(NaiveTime::from_hms_nano_opt(10, 20, 30, 400_500_123).unwrap());
        let flat = FlatVariant::from(&value);
        assert_eq!(Variant::from(&flat), Variant::from(&value));
        assert_eq!(
            VariantValue::try_from(&flat).unwrap(),
            VariantValue::try_from(&Variant::from(&value)).unwrap()
        );
    }

    /// A nanosecond timestamp too far from the epoch for a 64-bit count of nanoseconds keeps
    /// its microseconds too.
    #[test]
    fn far_nanosecond_timestamps_keep_their_microseconds() {
        // 3000-01-01T00:00:00.000001001Z, past the year 2262.
        let micros = 32_503_680_000_000_001;
        let far = Utc.timestamp_micros(micros).unwrap() + chrono::TimeDelta::nanoseconds(1);
        let value = VariantValue::TimestampNanos(far);
        assert_eq!(
            Variant::from(&value),
            Variant::TimestampTz(TimestampTz::from_microseconds(micros))
        );
        assert_eq!(
            VariantValue::try_from(&FlatVariant::from(&value)).unwrap(),
            VariantValue::TimestampMicros(Utc.timestamp_micros(micros).unwrap())
        );
    }
}
