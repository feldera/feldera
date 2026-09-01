//! Reading `VARIANT` from the Parquet variant binary encoding.
//!
//! Delta Lake and Iceberg store a `VARIANT` column as a pair of binary buffers,
//! `metadata` and `value`, holding the encoding defined by the Parquet variant
//! specification. Table formats surface that pair as a two-field struct, so a
//! connector reading one hands the record deserializer a map rather than the
//! JSON string [`VariantFormat::JsonString`] historically expected.
//!
//! [`deserialize_variant`] accepts both shapes and picks between them from what
//! the column actually holds, so one column can be a JSON string and the next a
//! binary variant.
//!
//! [`VariantFormat::JsonString`]: feldera_types::serde_with_context::serde_config::VariantFormat::JsonString

use crate::{
    Date, SqlString, Time, Timestamp, TimestampTz, Uuid, binary::ByteArray,
    flat_variant::FlatVariant, variant::Variant,
};
use dbsp::algebra::{F32, F64};
use parquet_variant::Variant as ParquetVariant;
use serde::de::{Deserializer, Error as _, MapAccess, Visitor};
use std::fmt;

/// Field names of the Parquet variant's two binary buffers.
const METADATA_FIELD: &str = "metadata";
const VALUE_FIELD: &str = "value";

/// The extra field a shredded variant carries. We only read unshredded
/// variants, so its presence is worth naming in the error.
const SHREDDED_FIELD: &str = "typed_value";

/// serde_json's private token for arbitrary-precision numbers, which the JSON
/// deserializer presents as a single-entry map.
const JSON_NUMBER_TOKEN: &str = "$serde_json::private::Number";

/// A `VARIANT` representation that can be built from either encoding.
pub(crate) trait FromVariantEncoding: Sized {
    /// Parse JSON text, the encoding `VariantFormat::JsonString` names.
    fn from_json(text: &str) -> Result<Self, String>;

    /// Decode the Parquet variant binary encoding.
    fn from_binary(metadata: &[u8], value: &[u8]) -> Result<Self, String>;
}

impl FromVariantEncoding for Variant {
    fn from_json(text: &str) -> Result<Self, String> {
        serde_json::from_str(text).map_err(|e| e.to_string())
    }

    fn from_binary(metadata: &[u8], value: &[u8]) -> Result<Self, String> {
        let variant = ParquetVariant::try_new(metadata, value).map_err(|e| e.to_string())?;
        to_variant(variant)
    }
}

impl FromVariantEncoding for FlatVariant {
    fn from_json(text: &str) -> Result<Self, String> {
        serde_json::from_str(text).map_err(|e| e.to_string())
    }

    fn from_binary(metadata: &[u8], value: &[u8]) -> Result<Self, String> {
        let variant = ParquetVariant::try_new(metadata, value).map_err(|e| e.to_string())?;
        crate::flat_variant::flat_variant_from_parquet(variant)
    }
}

/// Deserialize a `VARIANT` written either as JSON text or as a Parquet binary
/// variant.
///
/// Self-describing formats such as CSV report a bare `123` or `true` as a
/// number or a boolean rather than as text; those go back through the JSON
/// parser so that a value reads the same whichever way the format hands it
/// over.
pub(crate) fn deserialize_variant<'de, T, D>(deserializer: D) -> Result<T, D::Error>
where
    T: FromVariantEncoding,
    D: Deserializer<'de>,
{
    deserializer.deserialize_any(VariantVisitor::<T>(std::marker::PhantomData))
}

struct VariantVisitor<T>(std::marker::PhantomData<T>);

impl<T: FromVariantEncoding> VariantVisitor<T> {
    fn from_json<E: serde::de::Error>(text: &str) -> Result<T, E> {
        T::from_json(text).map_err(|e| {
            E::custom(format!(
                "error deserializing VARIANT type from a JSON string: {e}"
            ))
        })
    }
}

impl<'de, T: FromVariantEncoding> Visitor<'de> for VariantVisitor<T> {
    type Value = T;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(
            formatter,
            "a JSON string or a Parquet variant (a struct with '{METADATA_FIELD}' and '{VALUE_FIELD}' binary fields)"
        )
    }

    fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<T, E> {
        Self::from_json(value)
    }

    fn visit_bool<E: serde::de::Error>(self, value: bool) -> Result<T, E> {
        Self::from_json(if value { "true" } else { "false" })
    }

    fn visit_i64<E: serde::de::Error>(self, value: i64) -> Result<T, E> {
        Self::from_json(&value.to_string())
    }

    fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<T, E> {
        Self::from_json(&value.to_string())
    }

    fn visit_i128<E: serde::de::Error>(self, value: i128) -> Result<T, E> {
        Self::from_json(&value.to_string())
    }

    fn visit_u128<E: serde::de::Error>(self, value: u128) -> Result<T, E> {
        Self::from_json(&value.to_string())
    }

    fn visit_f64<E: serde::de::Error>(self, value: f64) -> Result<T, E> {
        Self::from_json(&value.to_string())
    }

    fn visit_map<A>(self, mut map: A) -> Result<T, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut metadata: Option<Bytes> = None;
        let mut value: Option<Bytes> = None;

        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                METADATA_FIELD => metadata = Some(map.next_value()?),
                VALUE_FIELD => value = Some(map.next_value()?),
                // A JSON deserializer configured for arbitrary precision hands
                // over a number as this one-entry map.
                JSON_NUMBER_TOKEN if metadata.is_none() && value.is_none() => {
                    let number: String = map.next_value()?;
                    return Self::from_json(&number);
                }
                SHREDDED_FIELD => {
                    return Err(A::Error::custom(
                        "shredded Parquet variants are not supported: the VARIANT column has a \
                         'typed_value' field",
                    ));
                }
                other => {
                    return Err(A::Error::custom(format!(
                        "expected a VARIANT encoded as a JSON string or as a Parquet variant, \
                         found a struct with an unexpected field '{other}'"
                    )));
                }
            }
        }

        let metadata = metadata.ok_or_else(|| {
            A::Error::custom(format!(
                "Parquet variant is missing its '{METADATA_FIELD}' field"
            ))
        })?;
        let value = value.ok_or_else(|| {
            A::Error::custom(format!(
                "Parquet variant is missing its '{VALUE_FIELD}' field"
            ))
        })?;

        T::from_binary(&metadata.0, &value.0)
            .map_err(|e| A::Error::custom(format!("error deserializing Parquet variant: {e}")))
    }
}

/// One of the variant's binary buffers.
///
/// Arrow deserializers hand out a binary column as borrowed bytes; the copy
/// here is the buffer itself, which the decoder then reads without copying
/// again.
struct Bytes(Vec<u8>);

impl<'de> serde::Deserialize<'de> for Bytes {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct BytesVisitor;

        impl<'de> Visitor<'de> for BytesVisitor {
            type Value = Bytes;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                write!(formatter, "a byte array")
            }

            fn visit_bytes<E: serde::de::Error>(self, value: &[u8]) -> Result<Bytes, E> {
                Ok(Bytes(value.to_vec()))
            }

            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> Result<Bytes, A::Error> {
                let mut bytes = Vec::with_capacity(seq.size_hint().unwrap_or(0));
                while let Some(byte) = seq.next_element::<u8>()? {
                    bytes.push(byte);
                }
                Ok(Bytes(bytes))
            }
        }

        deserializer.deserialize_bytes(BytesVisitor)
    }
}

/// Map a decoded Parquet variant onto [`Variant`].
///
/// The two type systems line up almost exactly. The one lossy step is a
/// nanosecond timestamp, which [`Timestamp`] holds to microseconds.
fn to_variant(value: ParquetVariant<'_, '_>) -> Result<Variant, String> {
    Ok(match value {
        ParquetVariant::Null => Variant::VariantNull,
        ParquetVariant::BooleanTrue => Variant::Boolean(true),
        ParquetVariant::BooleanFalse => Variant::Boolean(false),
        ParquetVariant::Int8(v) => Variant::TinyInt(v),
        ParquetVariant::Int16(v) => Variant::SmallInt(v),
        ParquetVariant::Int32(v) => Variant::Int(v),
        ParquetVariant::Int64(v) => Variant::BigInt(v),
        ParquetVariant::Float(v) => Variant::Real(F32::new(v)),
        ParquetVariant::Double(v) => Variant::Double(F64::new(v)),
        ParquetVariant::Decimal4(v) => Variant::SqlDecimal((v.integer() as i128, v.scale())),
        ParquetVariant::Decimal8(v) => Variant::SqlDecimal((v.integer() as i128, v.scale())),
        ParquetVariant::Decimal16(v) => Variant::SqlDecimal((v.integer(), v.scale())),
        ParquetVariant::Date(v) => Variant::Date(Date::from_date(v)),
        ParquetVariant::Time(v) => Variant::Time(Time::from_time(v)),
        ParquetVariant::TimestampNtzMicros(v) => {
            Variant::Timestamp(Timestamp::from_microseconds(v.and_utc().timestamp_micros()))
        }
        ParquetVariant::TimestampNtzNanos(v) => {
            Variant::Timestamp(Timestamp::from_microseconds(v.and_utc().timestamp_micros()))
        }
        ParquetVariant::TimestampMicros(v) => {
            Variant::TimestampTz(TimestampTz::from_microseconds(v.timestamp_micros()))
        }
        ParquetVariant::TimestampNanos(v) => {
            Variant::TimestampTz(TimestampTz::from_microseconds(v.timestamp_micros()))
        }
        ParquetVariant::Binary(v) => Variant::Binary(ByteArray::new(v)),
        ParquetVariant::String(v) => Variant::String(SqlString::from_ref(v)),
        ParquetVariant::ShortString(v) => Variant::String(SqlString::from_ref(v.as_str())),
        ParquetVariant::Uuid(v) => Variant::Uuid(Uuid::from_bytes(*v.as_bytes())),
        ParquetVariant::List(list) => {
            let mut elements = Vec::with_capacity(list.len());
            for element in list.iter_try() {
                elements.push(to_variant(element.map_err(|e| e.to_string())?)?);
            }
            Variant::Array(elements.into())
        }
        ParquetVariant::Object(object) => {
            let mut fields = std::collections::BTreeMap::new();
            for field in object.iter_try() {
                let (key, value) = field.map_err(|e| e.to_string())?;
                fields.insert(
                    Variant::String(SqlString::from_ref(key)),
                    to_variant(value)?,
                );
            }
            Variant::Map(fields.into())
        }
    })
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::variant::Variant;
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
    use feldera_types::serde_with_context::{DeserializeWithContext, SqlSerdeConfig};
    use parquet_variant::{VariantBuilder, VariantDecimal4, VariantDecimal8, VariantDecimal16};
    use std::collections::BTreeMap;

    /// Encode one value with the Parquet variant builder.
    fn encode(build: impl FnOnce(&mut VariantBuilder)) -> (Vec<u8>, Vec<u8>) {
        let mut builder = VariantBuilder::new();
        build(&mut builder);
        builder.finish()
    }

    fn decode(metadata: &[u8], value: &[u8]) -> Variant {
        Variant::from_binary(metadata, value).unwrap()
    }

    fn scalar(value: ParquetVariant<'static, 'static>) -> Variant {
        let (metadata, encoded) = encode(|b| b.append_value(value));
        decode(&metadata, &encoded)
    }

    fn key(name: &str) -> Variant {
        Variant::String(SqlString::from_ref(name))
    }

    #[test]
    fn scalars_keep_their_types() {
        assert_eq!(scalar(ParquetVariant::Null), Variant::VariantNull);
        assert_eq!(scalar(ParquetVariant::BooleanTrue), Variant::Boolean(true));
        assert_eq!(
            scalar(ParquetVariant::BooleanFalse),
            Variant::Boolean(false)
        );
        assert_eq!(scalar(ParquetVariant::Int8(-5)), Variant::TinyInt(-5));
        assert_eq!(scalar(ParquetVariant::Int16(-300)), Variant::SmallInt(-300));
        assert_eq!(scalar(ParquetVariant::Int32(70_000)), Variant::Int(70_000));
        assert_eq!(
            scalar(ParquetVariant::Int64(9_000_000_000)),
            Variant::BigInt(9_000_000_000)
        );
        assert_eq!(
            scalar(ParquetVariant::Float(1.5f32)),
            Variant::Real(F32::new(1.5))
        );
        assert_eq!(
            scalar(ParquetVariant::Double(1.25f64)),
            Variant::Double(F64::new(1.25))
        );
        assert_eq!(
            scalar(ParquetVariant::String("hello")),
            Variant::String(SqlString::from_ref("hello"))
        );
        assert_eq!(
            scalar(ParquetVariant::Binary(&[1, 2, 3])),
            Variant::Binary(ByteArray::new(&[1, 2, 3]))
        );
        assert_eq!(
            scalar(ParquetVariant::Uuid(uuid::Uuid::from_bytes([7u8; 16]))),
            Variant::Uuid(Uuid::from_bytes([7u8; 16]))
        );
    }

    #[test]
    fn decimals_keep_their_scale() {
        assert_eq!(
            scalar(ParquetVariant::Decimal4(
                VariantDecimal4::try_new(12_345i32, 2u8).unwrap()
            )),
            Variant::SqlDecimal((12_345, 2))
        );
        assert_eq!(
            scalar(ParquetVariant::Decimal8(
                VariantDecimal8::try_new(-12_345_678_901i64, 4u8).unwrap()
            )),
            Variant::SqlDecimal((-12_345_678_901, 4))
        );
        assert_eq!(
            scalar(ParquetVariant::Decimal16(
                VariantDecimal16::try_new(123_456_789_012_345_678i128, 9u8).unwrap()
            )),
            Variant::SqlDecimal((123_456_789_012_345_678, 9))
        );
    }

    #[test]
    fn dates_and_times_keep_their_types() {
        let date = NaiveDate::from_ymd_opt(2026, 9, 1).unwrap();
        assert_eq!(
            scalar(ParquetVariant::Date(date)),
            Variant::Date(Date::from_date(date))
        );

        let time = NaiveTime::from_hms_micro_opt(13, 45, 6, 123_456).unwrap();
        assert_eq!(
            scalar(ParquetVariant::Time(time)),
            Variant::Time(Time::from_time(time))
        );

        let naive: NaiveDateTime = date.and_hms_micro_opt(13, 45, 6, 123_456).unwrap();
        assert_eq!(
            scalar(ParquetVariant::TimestampNtzMicros(naive)),
            Variant::Timestamp(Timestamp::from_microseconds(
                naive.and_utc().timestamp_micros()
            ))
        );
        assert_eq!(
            scalar(ParquetVariant::TimestampMicros(naive.and_utc())),
            Variant::TimestampTz(TimestampTz::from_microseconds(
                naive.and_utc().timestamp_micros()
            ))
        );
    }

    /// A nanosecond timestamp is the one lossy conversion: `Timestamp` holds
    /// microseconds.
    #[test]
    fn nanosecond_timestamps_truncate_to_microseconds() {
        let naive = NaiveDate::from_ymd_opt(2026, 9, 1)
            .unwrap()
            .and_hms_nano_opt(13, 45, 6, 123_456_789)
            .unwrap();
        assert_eq!(
            scalar(ParquetVariant::TimestampNtzNanos(naive)),
            Variant::Timestamp(Timestamp::from_microseconds(
                naive.and_utc().timestamp_micros()
            ))
        );
        assert_eq!(
            scalar(ParquetVariant::TimestampNanos(naive.and_utc())),
            Variant::TimestampTz(TimestampTz::from_microseconds(
                naive.and_utc().timestamp_micros()
            ))
        );
    }

    #[test]
    fn objects_and_lists_nest() {
        let (metadata, value) = encode(|b| {
            let mut object = b.new_object();
            object.insert("i", 42i64);
            object.insert("s", "hello");
            {
                let mut list = object.new_list("l");
                list.append_value(1i64);
                {
                    let mut inner = list.new_object();
                    inner.insert("deep", true);
                    inner.finish();
                }
                list.finish();
            }
            object.finish();
        });

        assert_eq!(
            decode(&metadata, &value),
            Variant::Map(
                BTreeMap::from([
                    (key("i"), Variant::BigInt(42)),
                    (key("s"), Variant::String(SqlString::from_ref("hello"))),
                    (
                        key("l"),
                        Variant::Array(
                            vec![
                                Variant::BigInt(1),
                                Variant::Map(
                                    BTreeMap::from([(key("deep"), Variant::Boolean(true))]).into()
                                ),
                            ]
                            .into()
                        )
                    ),
                ])
                .into()
            )
        );
    }

    #[test]
    fn empty_containers_decode() {
        let (metadata, value) = encode(|b| {
            b.new_object().finish();
        });
        assert_eq!(
            decode(&metadata, &value),
            Variant::Map(BTreeMap::new().into())
        );

        let (metadata, value) = encode(|b| {
            b.new_list().finish();
        });
        assert_eq!(decode(&metadata, &value), Variant::Array(Vec::new().into()));
    }

    #[test]
    fn malformed_buffers_are_rejected() {
        let (metadata, value) = encode(|b| b.append_value(42i64));

        assert!(Variant::from_binary(&[], &value).is_err());
        assert!(Variant::from_binary(&metadata, &[]).is_err());
        assert!(Variant::from_binary(&metadata, &[0xff, 0xff, 0xff]).is_err());
        // An object's field ids do not resolve against an empty dictionary.
        let (_, object) = encode(|b| {
            let mut object = b.new_object();
            object.insert("a", 1i64);
            object.finish();
        });
        let (empty_metadata, _) = encode(|b| b.append_value(1i64));
        assert!(Variant::from_binary(&empty_metadata, &object).is_err());
    }

    /// A JSON deserializer reaches the same visitor, and a `VARIANT` column
    /// written as a JSON string must still parse exactly as before.
    #[test]
    fn json_strings_still_parse() {
        fn parse(json: &str) -> Result<Variant, String> {
            let mut de = serde_json::Deserializer::from_str(json);
            <Variant as DeserializeWithContext<SqlSerdeConfig, Variant>>::deserialize_with_context(
                &mut de,
                &SqlSerdeConfig::default(),
            )
            .map_err(|e| e.to_string())
        }

        assert_eq!(
            parse(r#""{\"a\": 1}""#).unwrap(),
            Variant::Map(BTreeMap::from([(key("a"), Variant::UBigInt(1))]).into())
        );
        assert_eq!(parse(r#""true""#).unwrap(), Variant::Boolean(true));
        assert_eq!(parse(r#""null""#).unwrap(), Variant::VariantNull);

        // A JSON value that is not a string is still rejected; only the
        // wording of the error changed.
        let error = parse(r#"{"a": 1}"#).unwrap_err();
        assert!(error.contains("unexpected field 'a'"), "{error}");
        assert!(parse("[1]").is_err());
    }

    /// serde_json with `arbitrary_precision` reports a number outside `u64` as
    /// a one-entry map with a private key, which must not be mistaken for a
    /// Parquet variant.
    #[test]
    fn arbitrary_precision_numbers_are_not_parquet_variants() {
        fn parse(json: &str) -> Variant {
            let mut de = serde_json::Deserializer::from_str(json);
            <Variant as DeserializeWithContext<SqlSerdeConfig, Variant>>::deserialize_with_context(
                &mut de,
                &SqlSerdeConfig::default(),
            )
            .unwrap()
        }

        let expected = Variant::SqlDecimal((20_000_000_000_000_000_000i128, 0));
        // Inside a JSON string, which is what `JsonString` names.
        assert_eq!(parse(r#""20000000000000000000""#), expected);
        // As a bare JSON number, which the visitor accepts for the same reason
        // it accepts a bare `123`.
        assert_eq!(parse("20000000000000000000"), expected);
    }

    /// The two representations must agree: `FlatVariant`'s direct encoder is
    /// only worth having if it lands on the same value as the enum path.
    #[test]
    fn flat_variant_matches_variant() {
        let (metadata, value) = encode(|b| {
            let mut object = b.new_object();
            object.insert("i", 42i64);
            object.insert("s", "hello");
            object.insert("b", true);
            object.insert("n", ParquetVariant::Null);
            object.insert("d", 2.5f64);
            object.insert(
                "dec",
                ParquetVariant::Decimal4(VariantDecimal4::try_new(12_345i32, 2u8).unwrap()),
            );
            object.insert(
                "date",
                ParquetVariant::Date(NaiveDate::from_ymd_opt(2026, 9, 1).unwrap()),
            );
            object.insert("bin", ParquetVariant::Binary(&[9, 8, 7]));
            {
                let mut list = object.new_list("l");
                list.append_value(1i64);
                list.append_value("two");
                list.finish();
            }
            object.finish();
        });

        let variant = Variant::from_binary(&metadata, &value).unwrap();
        let flat = FlatVariant::from_binary(&metadata, &value).unwrap();
        assert_eq!(FlatVariant::from(&variant), flat);
    }
}
