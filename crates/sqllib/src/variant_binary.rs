//! `VARIANT` in the Parquet variant binary encoding.
//!
//! Delta Lake and Iceberg store a `VARIANT` column as a pair of binary buffers,
//! `metadata` and `value`, holding the encoding defined by the Parquet variant
//! specification. Table formats surface that pair as a two-field struct, so a
//! connector reading one hands the record deserializer a map rather than the
//! JSON string [`VariantFormat::JsonString`] historically expected.
//!
//! Reading and writing are not symmetric. [`deserialize_variant`] is handed the
//! data, so it tells a JSON string from a binary variant and decodes either;
//! one column can be a JSON string and the next a binary variant.
//! [`serialize_variant`] is handed a value, which knows nothing of the column
//! it will land in, so the encoding is chosen for it by
//! [`VariantFormat::ParquetVariant`].
//!
//! [`VariantFormat::JsonString`]: feldera_types::serde_with_context::serde_config::VariantFormat::JsonString
//! [`VariantFormat::ParquetVariant`]: feldera_types::serde_with_context::serde_config::VariantFormat::ParquetVariant

use crate::{
    Date, SqlString, Time, Timestamp, TimestampTz, Uuid, binary::ByteArray,
    flat_variant::FlatVariant, variant::Variant,
};
use dbsp::algebra::{F32, F64};
use parquet_variant::{
    ObjectBuilder, Variant as ParquetVariant, VariantBuilder, VariantBuilderExt,
};
use serde::de::{Deserializer, Error as _, MapAccess, Visitor};
use serde::ser::{Error as _, SerializeStruct, Serializer};
use std::borrow::Cow;
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

/// Maximum nesting depth of a `VARIANT` value.
///
/// Matches `serde_json`'s recursion limit, which bounded the JSON path this
/// encoding sits beside.
pub(crate) const MAX_VARIANT_DEPTH: usize = 128;

pub(crate) fn variant_too_deep() -> String {
    format!("VARIANT value is nested more than {MAX_VARIANT_DEPTH} levels deep")
}

/// Read `width` little-endian bytes at `at`.
fn packed_int(bytes: &[u8], at: usize, width: usize) -> Option<usize> {
    let raw = bytes.get(at..at.checked_add(width)?)?;
    Some(raw.iter().enumerate().fold(0usize, |value, (i, byte)| {
        value | (*byte as usize) << (8 * i)
    }))
}

/// Reject a value nested deeper than [`MAX_VARIANT_DEPTH`].
///
/// `parquet_variant` validates a value by recursing once per level with no
/// cap, so a deeply nested value overflows the stack and aborts the process
/// before any error can be returned: 1000 nested arrays fit in 6 KB. This
/// walks the encoding iteratively first, so every recursive walk that follows,
/// upstream's included, is bounded.
///
/// Structure the walk cannot read is left alone rather than reported: decoding
/// it is what produces the real error.
fn check_variant_depth(value: &[u8]) -> Result<(), String> {
    // Basic type, the low two bits of a value's header byte.
    const OBJECT: u8 = 2;
    const ARRAY: u8 = 3;

    let mut pending = vec![(0usize, value.len(), 1usize)];
    while let Some((start, len, depth)) = pending.pop() {
        if depth > MAX_VARIANT_DEPTH {
            return Err(variant_too_deep());
        }
        let Some(bytes) = value.get(start..start.saturating_add(len)) else {
            continue;
        };
        let Some(&header) = bytes.first() else {
            continue;
        };
        let basic = header & 0x03;
        if basic != OBJECT && basic != ARRAY {
            continue;
        }

        // Both containers end with an offset table and a data area; they
        // differ in the header bits and in the field ids an object carries
        // between the two.
        let value_header = header >> 2;
        let offset_size = (value_header & 0x03) as usize + 1;
        let (is_large, field_ids_size) = if basic == ARRAY {
            (value_header & 0x04 != 0, 0)
        } else {
            (
                value_header & 0x10 != 0,
                ((value_header >> 2) & 0x03) as usize + 1,
            )
        };
        let count_size = if is_large { 4 } else { 1 };

        let Some(count) = packed_int(bytes, 1, count_size) else {
            continue;
        };
        // A count larger than the buffer cannot be real; leave it to decoding.
        if count > bytes.len() {
            continue;
        }
        let Some(first_offset) = 1usize
            .checked_add(count_size)
            .and_then(|at| at.checked_add(count.checked_mul(field_ids_size)?))
        else {
            continue;
        };
        let Some(data) = count
            .checked_add(1)
            .and_then(|slots| slots.checked_mul(offset_size))
            .and_then(|table| first_offset.checked_add(table))
        else {
            continue;
        };

        for i in 0..count {
            let (Some(from), Some(to)) = (
                packed_int(bytes, first_offset + i * offset_size, offset_size),
                packed_int(bytes, first_offset + (i + 1) * offset_size, offset_size),
            ) else {
                continue;
            };
            if to < from {
                continue;
            }
            pending.push((start + data + from, to - from, depth + 1));
        }
    }
    Ok(())
}

/// Decode the two buffers, rejecting a value too deeply nested to walk.
fn parse_variant<'a>(
    metadata: &'a [u8],
    value: &'a [u8],
) -> Result<ParquetVariant<'a, 'a>, String> {
    check_variant_depth(value)?;
    ParquetVariant::try_new(metadata, value).map_err(|e| e.to_string())
}

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
        to_variant(parse_variant(metadata, value)?, 1)
    }
}

impl FromVariantEncoding for FlatVariant {
    fn from_json(text: &str) -> Result<Self, String> {
        serde_json::from_str(text).map_err(|e| e.to_string())
    }

    fn from_binary(metadata: &[u8], value: &[u8]) -> Result<Self, String> {
        crate::flat_variant::flat_variant_from_parquet(parse_variant(metadata, value)?)
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
        // `Display` renders 1.0 as "1", which JSON then reads as an integer,
        // and renders 1e300 in full; `Debug` keeps both the point and the
        // exponent, so the value reads back as the number it was.
        Self::from_json(&format!("{value:?}"))
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
                         'typed_value' field. Rewrite the column as an unshredded variant, or \
                         store it as a JSON string",
                    ));
                }
                other => {
                    return Err(A::Error::custom(format!(
                        "expected a VARIANT encoded as a JSON string or as a Parquet variant, \
                         found a struct with an unexpected field '{other}'. A Parquet variant \
                         is a struct of '{METADATA_FIELD}' and '{VALUE_FIELD}' binary fields"
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

/// One of the variant's binary buffers, in either direction.
///
/// Arrow deserializers hand out a binary column as borrowed bytes; the copy on
/// the way in is the buffer itself, which the decoder then reads without
/// copying again. On the way out it carries an encoded buffer to
/// `serialize_bytes`, which is how a binary Arrow column wants it.
struct Bytes(Vec<u8>);

impl serde::Serialize for Bytes {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.0)
    }
}

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

/// Serialize a `VARIANT` as a Parquet variant: a struct of the two binary
/// buffers, which is how Delta Lake and Iceberg store the type.
///
/// The reverse of [`deserialize_variant`]'s map branch. Deserialization can
/// choose its decoder from the data it is handed; serialization cannot, so the
/// encoding is a [`VariantFormat`] the caller picks.
///
/// [`VariantFormat`]: feldera_types::serde_with_context::serde_config::VariantFormat
pub(crate) fn serialize_variant<S>(value: &Variant, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let (metadata, value) = to_variant_binary(value).map_err(|e| {
        S::Error::custom(format!(
            "error serializing VARIANT to a Parquet variant: {e}"
        ))
    })?;
    serialize_variant_buffers(metadata, value, serializer)
}

/// Encode a `VARIANT` into the Parquet variant binary encoding, returning the
/// metadata and value buffers.
pub(crate) fn to_variant_binary(value: &Variant) -> Result<(Vec<u8>, Vec<u8>), String> {
    // Two VARIANT map keys can render to one object field name, and the
    // builder is otherwise last-write-wins, which drops an entry silently.
    let mut builder = VariantBuilder::new().with_validate_unique_fields(true);
    append_variant(&mut builder, value, 1)?;
    Ok(builder.finish())
}

/// Append `value` to any builder that takes an unnamed value: the top-level
/// builder or a list.
fn append_variant<B: VariantBuilderExt>(
    builder: &mut B,
    value: &Variant,
    depth: usize,
) -> Result<(), String> {
    if depth > MAX_VARIANT_DEPTH {
        return Err(variant_too_deep());
    }
    match value {
        Variant::Map(entries) => {
            let mut object = builder
                .try_new_object()
                .map_err(|e| format!("cannot start a variant object: {e}"))?;
            for (key, value) in entries.iter() {
                insert_variant(&mut object, key, value, depth + 1)?;
            }
            object.finish();
        }
        Variant::Array(elements) => {
            let mut list = builder
                .try_new_list()
                .map_err(|e| format!("cannot start a variant list: {e}"))?;
            for element in elements.iter() {
                append_variant(&mut list, element, depth + 1)?;
            }
            list.finish();
        }
        scalar => builder.append_value(to_parquet_scalar(scalar)?),
    }
    Ok(())
}

/// Insert one field into a variant object. Objects need the key alongside the
/// value, so they cannot share [`append_variant`]'s builder trait.
fn insert_variant<S: parquet_variant::BuilderSpecificState>(
    object: &mut ObjectBuilder<'_, S>,
    key: &Variant,
    value: &Variant,
    depth: usize,
) -> Result<(), String> {
    if depth > MAX_VARIANT_DEPTH {
        return Err(variant_too_deep());
    }
    let key = variant_object_key(key)?;
    let key = key.as_ref();

    match value {
        Variant::Map(entries) => {
            let mut nested = object
                .try_new_object(key)
                .map_err(|e| format!("cannot start a variant object: {e}"))?;
            for (key, value) in entries.iter() {
                insert_variant(&mut nested, key, value, depth + 1)?;
            }
            nested.finish();
        }
        Variant::Array(elements) => {
            let mut list = object
                .try_new_list(key)
                .map_err(|e| format!("cannot start a variant list: {e}"))?;
            for element in elements.iter() {
                append_variant(&mut list, element, depth + 1)?;
            }
            list.finish();
        }
        scalar => object
            .try_insert(key, to_parquet_scalar(scalar)?)
            .map_err(|e| duplicate_variant_key(key, e))?,
    }
    Ok(())
}

/// Render a map key as a Parquet variant object field name.
///
/// Variant objects are keyed by string. JSON is too, and the JSON encoding
/// renders an integer key as its digits, so this does the same rather than
/// rejecting a value the other encoding accepts. Keys that JSON also refuses
/// are refused here.
fn variant_object_key(key: &Variant) -> Result<Cow<'_, str>, String> {
    Ok(match key {
        Variant::String(key) => Cow::Borrowed(key.str()),
        Variant::TinyInt(v) => Cow::Owned(v.to_string()),
        Variant::SmallInt(v) => Cow::Owned(v.to_string()),
        Variant::Int(v) => Cow::Owned(v.to_string()),
        Variant::BigInt(v) => Cow::Owned(v.to_string()),
        Variant::UTinyInt(v) => Cow::Owned(v.to_string()),
        Variant::USmallInt(v) => Cow::Owned(v.to_string()),
        Variant::UInt(v) => Cow::Owned(v.to_string()),
        Variant::UBigInt(v) => Cow::Owned(v.to_string()),
        key => return Err(unencodable_variant_key(key.get_type_string())),
    })
}

/// The Parquet variant decimal that holds `significand` at `scale`, in the
/// narrowest width that fits.
pub(crate) fn parquet_decimal(
    significand: i128,
    scale: u8,
) -> Result<ParquetVariant<'static, 'static>, String> {
    use parquet_variant::{VariantDecimal4, VariantDecimal8, VariantDecimal16};
    let bad = |e| format!("cannot encode a variant decimal: {e}");
    Ok(if let Ok(v) = i32::try_from(significand) {
        ParquetVariant::Decimal4(VariantDecimal4::try_new(v, scale).map_err(bad)?)
    } else if let Ok(v) = i64::try_from(significand) {
        ParquetVariant::Decimal8(VariantDecimal8::try_new(v, scale).map_err(bad)?)
    } else {
        ParquetVariant::Decimal16(VariantDecimal16::try_new(significand, scale).map_err(bad)?)
    })
}

/// The encoding has no unsigned types, so a `u64` becomes an `Int64` when it
/// fits and a decimal when it does not.
pub(crate) fn parquet_u64(value: u64) -> Result<ParquetVariant<'static, 'static>, String> {
    match i64::try_from(value) {
        Ok(value) => Ok(ParquetVariant::Int64(value)),
        Err(_) => parquet_decimal(value as i128, 0),
    }
}

/// The error a type with no Parquet variant counterpart produces.
pub(crate) fn unencodable_variant(type_string: &str) -> String {
    format!(
        "a VARIANT holding {type_string} cannot be encoded as a Parquet variant, which has no such type"
    )
}

/// The error two map keys rendering to one object field name produce.
pub(crate) fn duplicate_variant_key(key: &str, error: impl std::fmt::Display) -> String {
    format!(
        "cannot encode a VARIANT map: two keys name the same variant object field \
         '{key}' ({error}); a variant object's field names are strings, so an \
         integer key and the string of its digits collide"
    )
}

/// The error a map key that cannot name an object field produces.
pub(crate) fn unencodable_variant_key(type_string: &str) -> String {
    format!(
        "a VARIANT map keyed by {type_string} cannot be encoded: a variant object's field names are strings"
    )
}

/// Serialize the two buffers as the struct a table format stores them in.
pub(crate) fn serialize_variant_buffers<S: Serializer>(
    metadata: Vec<u8>,
    value: Vec<u8>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    let mut variant = serializer.serialize_struct("Variant", 2)?;
    variant.serialize_field(METADATA_FIELD, &Bytes(metadata))?;
    variant.serialize_field(VALUE_FIELD, &Bytes(value))?;
    variant.end()
}

/// Serialize a [`FlatVariant`] as a Parquet variant, walking its buffer
/// directly rather than through the enum [`Variant`].
pub(crate) fn serialize_flat_variant<S>(
    value: &FlatVariant,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let (metadata, value) = crate::flat_variant::flat_variant_to_parquet(value).map_err(|e| {
        S::Error::custom(format!(
            "error serializing VARIANT to a Parquet variant: {e}"
        ))
    })?;
    serialize_variant_buffers(metadata, value, serializer)
}

/// Map a scalar `VARIANT` onto the Parquet variant type that holds it.
///
/// Three SQL types have no Parquet variant counterpart and are rejected rather
/// than silently reshaped: the two interval types and `GEOMETRY`. `SqlNull` and
/// `VariantNull` both encode as the variant null, since the encoding draws no
/// such distinction.
fn to_parquet_scalar(value: &Variant) -> Result<ParquetVariant<'_, '_>, String> {
    Ok(match value {
        Variant::SqlNull | Variant::VariantNull => ParquetVariant::Null,
        Variant::Boolean(v) => {
            if *v {
                ParquetVariant::BooleanTrue
            } else {
                ParquetVariant::BooleanFalse
            }
        }
        Variant::TinyInt(v) => ParquetVariant::Int8(*v),
        Variant::SmallInt(v) => ParquetVariant::Int16(*v),
        Variant::Int(v) => ParquetVariant::Int32(*v),
        Variant::BigInt(v) => ParquetVariant::Int64(*v),
        // The encoding has no unsigned types, so each widens to the smallest
        // signed type that holds it; `u64` needs a decimal to stay exact.
        Variant::UTinyInt(v) => ParquetVariant::Int16(*v as i16),
        Variant::USmallInt(v) => ParquetVariant::Int32(*v as i32),
        Variant::UInt(v) => ParquetVariant::Int64(*v as i64),
        Variant::UBigInt(v) => parquet_u64(*v)?,
        Variant::Real(v) => ParquetVariant::Float(v.into_inner()),
        Variant::Double(v) => ParquetVariant::Double(v.into_inner()),
        Variant::SqlDecimal((significand, scale)) => parquet_decimal(*significand, *scale)?,
        Variant::String(v) => ParquetVariant::String(v.str()),
        Variant::Binary(v) => ParquetVariant::Binary(v.as_slice()),
        Variant::Uuid(v) => ParquetVariant::Uuid(uuid::Uuid::from_bytes(*v.to_bytes())),
        Variant::Date(v) => ParquetVariant::Date(v.to_date()),
        Variant::Time(v) => ParquetVariant::Time(v.to_time()),
        Variant::Timestamp(v) => ParquetVariant::TimestampNtzMicros(v.to_dateTime().naive_utc()),
        Variant::TimestampTz(v) => ParquetVariant::TimestampMicros(v.to_dateTime()),
        Variant::ShortInterval(_) | Variant::LongInterval(_) | Variant::Geometry(_) => {
            return Err(unencodable_variant(value.get_type_string()));
        }
        // Containers are handled by the caller, which needs a builder.
        Variant::Map(_) | Variant::Array(_) => {
            return Err(
                "internal error: a variant container reached the scalar encoder".to_string(),
            );
        }
    })
}

/// Map a decoded Parquet variant onto [`Variant`].
///
/// The two type systems line up almost exactly. The one lossy step is a
/// nanosecond timestamp, which [`Timestamp`] holds to microseconds.
fn to_variant(value: ParquetVariant<'_, '_>, depth: usize) -> Result<Variant, String> {
    if depth > MAX_VARIANT_DEPTH {
        return Err(variant_too_deep());
    }
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
                elements.push(to_variant(element.map_err(|e| e.to_string())?, depth + 1)?);
            }
            Variant::Array(elements.into())
        }
        ParquetVariant::Object(object) => {
            let mut fields = std::collections::BTreeMap::new();
            for field in object.iter_try() {
                let (key, value) = field.map_err(|e| e.to_string())?;
                fields.insert(
                    Variant::String(SqlString::from_ref(key)),
                    to_variant(value, depth + 1)?,
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

    /// Every value the mapping covers, for tests that need a broad sample.
    fn sample_variants() -> Vec<Variant> {
        let date = NaiveDate::from_ymd_opt(2026, 9, 1).unwrap();
        let naive = date.and_hms_micro_opt(13, 45, 6, 123_456).unwrap();

        vec![
            Variant::VariantNull,
            Variant::Boolean(true),
            Variant::Boolean(false),
            Variant::TinyInt(-5),
            Variant::SmallInt(-300),
            Variant::Int(70_000),
            Variant::BigInt(9_000_000_000),
            Variant::Real(F32::new(1.5)),
            Variant::Double(F64::new(1.25)),
            Variant::SqlDecimal((12_345, 2)),
            Variant::SqlDecimal((-12_345_678_901, 4)),
            Variant::SqlDecimal((170_141_183_460_469_231_731_687_303_715_884_105_i128, 9)),
            Variant::String(SqlString::from_ref("hello")),
            Variant::Binary(ByteArray::new(&[1, 2, 3])),
            Variant::Uuid(Uuid::from_bytes([7u8; 16])),
            Variant::Date(Date::from_date(date)),
            Variant::Time(Time::from_time(
                NaiveTime::from_hms_micro_opt(13, 45, 6, 123_456).unwrap(),
            )),
            Variant::Timestamp(Timestamp::from_microseconds(
                naive.and_utc().timestamp_micros(),
            )),
            Variant::TimestampTz(TimestampTz::from_microseconds(
                naive.and_utc().timestamp_micros(),
            )),
            Variant::Array(vec![Variant::BigInt(1), Variant::VariantNull].into()),
            Variant::Map(
                BTreeMap::from([
                    (key("a"), Variant::BigInt(1)),
                    (
                        key("b"),
                        Variant::Map(BTreeMap::from([(key("c"), Variant::Boolean(true))]).into()),
                    ),
                ])
                .into(),
            ),
            Variant::Map(BTreeMap::new().into()),
            Variant::Array(Vec::new().into()),
        ]
    }

    /// Encoding and decoding are inverses for every type the Parquet variant
    /// encoding holds.
    #[test]
    fn values_round_trip_through_the_binary_encoding() {
        for value in sample_variants() {
            let (metadata, encoded) = to_variant_binary(&value).unwrap();
            assert_eq!(
                Variant::from_binary(&metadata, &encoded).unwrap(),
                value,
                "round trip changed {value:?}"
            );
        }
    }

    /// The encoding has no unsigned types and no separate SQL NULL, so those
    /// values land on the documented substitutes rather than round-tripping.
    #[test]
    fn values_without_a_counterpart_are_widened() {
        let widened = |value: Variant| {
            let (metadata, encoded) = to_variant_binary(&value).unwrap();
            Variant::from_binary(&metadata, &encoded).unwrap()
        };

        assert_eq!(widened(Variant::UTinyInt(200)), Variant::SmallInt(200));
        assert_eq!(widened(Variant::USmallInt(60_000)), Variant::Int(60_000));
        assert_eq!(
            widened(Variant::UInt(4_000_000_000)),
            Variant::BigInt(4_000_000_000)
        );
        assert_eq!(widened(Variant::UBigInt(7)), Variant::BigInt(7));
        assert_eq!(
            widened(Variant::UBigInt(u64::MAX)),
            Variant::SqlDecimal((u64::MAX as i128, 0))
        );
        // SQL NULL and the variant null share one encoding.
        assert_eq!(widened(Variant::SqlNull), Variant::VariantNull);
    }

    /// Types the encoding cannot express are rejected, not reshaped.
    #[test]
    fn values_with_no_encoding_are_rejected() {
        for value in [
            Variant::ShortInterval(crate::ShortInterval::from_microseconds(1)),
            Variant::LongInterval(crate::LongInterval::from_months(1)),
            Variant::Geometry(crate::GeoPoint::new(1.0, 2.0)),
        ] {
            let error = to_variant_binary(&value).unwrap_err();
            assert!(
                error.contains("cannot be encoded as a Parquet variant"),
                "unexpected error for {value:?}: {error}"
            );
        }

        // A key JSON also refuses.
        let error = to_variant_binary(&Variant::Map(
            BTreeMap::from([(Variant::Boolean(true), Variant::BigInt(2))]).into(),
        ))
        .unwrap_err();
        assert!(error.contains("field names are strings"), "{error}");
    }

    /// An integer map key renders as its digits, the way the JSON encoding
    /// renders it, so flipping a connector to the binary encoding does not
    /// start rejecting values it used to write.
    #[test]
    fn integer_map_keys_render_like_json() {
        let value = Variant::Map(
            BTreeMap::from([
                (
                    Variant::BigInt(1),
                    Variant::String(SqlString::from_ref("a")),
                ),
                (Variant::Int(-2), Variant::String(SqlString::from_ref("b"))),
            ])
            .into(),
        );

        let (metadata, encoded) = to_variant_binary(&value).unwrap();
        assert_eq!(
            Variant::from_binary(&metadata, &encoded).unwrap(),
            Variant::Map(
                BTreeMap::from([
                    (key("1"), Variant::String(SqlString::from_ref("a"))),
                    (key("-2"), Variant::String(SqlString::from_ref("b"))),
                ])
                .into()
            )
        );
        // Which is what the JSON encoding does with the same value.
        assert_eq!(value.to_json_string().unwrap(), r#"{"-2":"b","1":"a"}"#);
    }

    /// The two encoders are independent walks, one over the enum and one over
    /// the flat buffer, so each has to be checked against the other rather
    /// than only against its own decoder.
    #[test]
    fn flat_variant_encodes_like_variant() {
        for value in sample_variants() {
            let flat = FlatVariant::from(&value);
            assert_eq!(
                to_variant_binary(&value).unwrap(),
                crate::flat_variant::flat_variant_to_parquet(&flat).unwrap(),
                "the encoders disagree on {value:?}"
            );
        }
    }

    /// The flat encoder refuses the same values the enum encoder does.
    #[test]
    fn flat_variant_rejects_what_variant_rejects() {
        for value in [
            Variant::ShortInterval(crate::ShortInterval::from_microseconds(1)),
            Variant::LongInterval(crate::LongInterval::from_months(1)),
            Variant::Geometry(crate::GeoPoint::new(1.0, 2.0)),
            Variant::Map(BTreeMap::from([(Variant::Boolean(true), Variant::BigInt(2))]).into()),
        ] {
            let flat = FlatVariant::from(&value);
            assert_eq!(
                to_variant_binary(&value).unwrap_err(),
                crate::flat_variant::flat_variant_to_parquet(&flat).unwrap_err(),
                "the encoders disagree on {value:?}"
            );
        }
    }

    /// Integer map keys render the same way through either encoder.
    #[test]
    fn flat_variant_renders_integer_keys_alike() {
        let value = Variant::Map(
            BTreeMap::from([
                (
                    Variant::BigInt(1),
                    Variant::String(SqlString::from_ref("a")),
                ),
                (Variant::Int(-2), Variant::String(SqlString::from_ref("b"))),
            ])
            .into(),
        );

        assert_eq!(
            to_variant_binary(&value).unwrap(),
            crate::flat_variant::flat_variant_to_parquet(&FlatVariant::from(&value)).unwrap()
        );
    }

    /// Wrap `child` in a one-element variant array.
    fn wrap_array(child: &[u8]) -> Vec<u8> {
        let data_size = child.len();
        let offset_size: usize = match data_size {
            0..=0xFF => 1,
            0x100..=0xFFFF => 2,
            0x1_0000..=0xFF_FFFF => 3,
            _ => 4,
        };
        let mut out = vec![(((offset_size - 1) as u8) << 2) | 3, 1u8];
        out.extend_from_slice(&0u32.to_le_bytes()[..offset_size]);
        out.extend_from_slice(&(data_size as u32).to_le_bytes()[..offset_size]);
        out.extend_from_slice(child);
        out
    }

    /// `depth` nested single-element arrays around a variant null.
    fn nested_arrays(depth: usize) -> (Vec<u8>, Vec<u8>) {
        // Empty dictionary: version 1, unsorted, 1-byte offsets, no entries.
        let metadata = vec![0x01, 0x00, 0x00];
        let mut value = vec![0x00];
        for _ in 0..depth {
            value = wrap_array(&value);
        }
        (metadata, value)
    }

    /// Nesting within the limit decodes.
    #[test]
    fn nesting_within_the_limit_decodes() {
        let (metadata, value) = nested_arrays(MAX_VARIANT_DEPTH - 1);
        assert!(Variant::from_binary(&metadata, &value).is_ok());
        assert!(FlatVariant::from_binary(&metadata, &value).is_ok());
    }

    /// Nesting past the limit is an error, not a crash.
    ///
    /// `parquet_variant` validates by recursing once per level with no cap, so
    /// without the check 1000 levels - under 6 KB of input - overflowed the
    /// stack and aborted the process, which no error handler can catch.
    #[test]
    fn nesting_past_the_limit_is_rejected() {
        for depth in [MAX_VARIANT_DEPTH + 1, 1000] {
            let (metadata, value) = nested_arrays(depth);
            assert!(
                value.len() < 6_000,
                "{depth} levels is {} bytes",
                value.len()
            );
            for error in [
                Variant::from_binary(&metadata, &value).unwrap_err(),
                FlatVariant::from_binary(&metadata, &value).unwrap_err(),
            ] {
                assert!(error.contains("nested more than"), "{error}");
            }
        }
    }

    /// The encoders are bounded too, so a value built in memory cannot take
    /// the process down on the way out.
    #[test]
    fn encoding_past_the_limit_is_rejected() {
        let mut value = Variant::VariantNull;
        for _ in 0..MAX_VARIANT_DEPTH + 1 {
            value = Variant::Array(vec![value].into());
        }

        let error = to_variant_binary(&value).unwrap_err();
        assert!(error.contains("nested more than"), "{error}");
        let error =
            crate::flat_variant::flat_variant_to_parquet(&FlatVariant::from(&value)).unwrap_err();
        assert!(error.contains("nested more than"), "{error}");
    }

    /// A variant object's field names are strings, so an integer key and the
    /// string of its digits name the same field. The builder is otherwise
    /// last-write-wins, which drops one entry with no error.
    #[test]
    fn colliding_map_keys_are_rejected() {
        let value = Variant::Map(
            BTreeMap::from([
                (
                    Variant::BigInt(1),
                    Variant::String(SqlString::from_ref("int")),
                ),
                (key("1"), Variant::String(SqlString::from_ref("string"))),
            ])
            .into(),
        );

        for error in [
            to_variant_binary(&value).unwrap_err(),
            crate::flat_variant::flat_variant_to_parquet(&FlatVariant::from(&value)).unwrap_err(),
        ] {
            assert!(error.contains("two keys name the same"), "{error}");
        }
    }

    /// Distinct keys still encode.
    #[test]
    fn distinct_map_keys_encode() {
        let value = Variant::Map(
            BTreeMap::from([
                (
                    Variant::BigInt(1),
                    Variant::String(SqlString::from_ref("int")),
                ),
                (key("2"), Variant::String(SqlString::from_ref("string"))),
            ])
            .into(),
        );
        assert!(to_variant_binary(&value).is_ok());
    }

    /// Feed the visitor one map, the way an Arrow struct column reaches it.
    fn deserialize_struct(fields: &[(&str, &[u8])]) -> Result<Variant, String> {
        use serde::de::value::{Error, MapDeserializer};

        let entries = fields
            .iter()
            .map(|(name, bytes)| (*name, serde_bytes_value(bytes)));
        let deserializer = MapDeserializer::<_, Error>::new(entries);
        deserialize_variant::<Variant, _>(deserializer).map_err(|e| e.to_string())
    }

    /// A byte string that survives `MapDeserializer`.
    fn serde_bytes_value(
        bytes: &[u8],
    ) -> serde::de::value::SeqDeserializer<
        std::iter::Copied<std::slice::Iter<'_, u8>>,
        serde::de::value::Error,
    > {
        serde::de::value::SeqDeserializer::new(bytes.iter().copied())
    }

    /// The two branches a real third-party table is most likely to hit.
    #[test]
    fn shredded_and_unknown_fields_are_named() {
        let (metadata, value) = encode(|b| b.append_value(42i64));

        let error = deserialize_struct(&[
            ("metadata", &metadata),
            ("value", &value),
            ("typed_value", &[0u8]),
        ])
        .unwrap_err();
        assert!(error.contains("shredded"), "{error}");
        assert!(error.contains("unshredded variant"), "{error}");

        let error =
            deserialize_struct(&[("metadata", &metadata), ("surprise", &value)]).unwrap_err();
        assert!(error.contains("unexpected field 'surprise'"), "{error}");
        assert!(error.contains("binary fields"), "{error}");

        // A well-formed pair still decodes through the same path.
        assert_eq!(
            deserialize_struct(&[("metadata", &metadata), ("value", &value)]).unwrap(),
            Variant::BigInt(42)
        );

        // And a missing buffer is named.
        let error = deserialize_struct(&[("metadata", &metadata)]).unwrap_err();
        assert!(error.contains("missing its 'value' field"), "{error}");
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

/// Round trips a large volume of JSON through the binary encoding.
///
/// The other tests pin a fixed handful of values. Real JSON is where the
/// shapes that break an encoder live: documents past the 1-, 2- and 4-byte
/// offset widths, objects past 255 fields, keys that sort differently as text
/// than as bytes, numbers at the edge of every width.
///
/// The property is the user-visible one: what goes into a `VARIANT` comes back
/// out. It is checked on the JSON rendering rather than on `Variant` equality,
/// because the encoding has no unsigned types, so a JSON `1` legitimately
/// returns as `BigInt` rather than the `UBigInt` it parsed as.
#[cfg(test)]
mod json_round_trip {
    use super::*;
    use proptest::prelude::*;
    use serde_json::Value;

    /// `json` -> `Variant` -> binary -> `Variant` -> `json`, and the same
    /// through `FlatVariant`. Returns the two renderings for comparison.
    fn round_trip(json: &str) -> Result<(String, String, String), String> {
        let variant = Variant::from_json(json)?;
        let before = variant.to_json_string().map_err(|e| e.to_string())?;

        let (metadata, value) = to_variant_binary(&variant)?;
        let after = Variant::from_binary(&metadata, &value)?
            .to_json_string()
            .map_err(|e| e.to_string())?;

        let flat = FlatVariant::from_json(json)?;
        let (metadata, value) = crate::flat_variant::flat_variant_to_parquet(&flat)?;
        let flat_after = FlatVariant::from_binary(&metadata, &value)?
            .to_json_string()
            .map_err(|e| e.to_string())?;

        Ok((before, after, flat_after))
    }

    fn assert_round_trips(json: &str) {
        match round_trip(json) {
            Ok((before, after, flat_after)) => {
                assert_eq!(before, after, "Variant changed:\n{json}");
                assert_eq!(before, flat_after, "FlatVariant changed:\n{json}");
            }
            Err(e) => panic!("round trip failed: {e}\n{json}"),
        }
    }

    /// Arbitrary JSON, nested.
    fn json_value() -> impl Strategy<Value = Value> {
        let leaf = prop_oneof![
            Just(Value::Null),
            any::<bool>().prop_map(Value::Bool),
            any::<i64>().prop_map(|v| Value::Number(v.into())),
            any::<u64>().prop_map(|v| Value::Number(v.into())),
            (-1.0e12f64..1.0e12).prop_map(|v| serde_json::Number::from_f64(v)
                .map(Value::Number)
                .unwrap_or(Value::Null)),
            "[^\\p{Cc}]{0,24}".prop_map(Value::String),
        ];
        leaf.prop_recursive(5, 512, 8, |inner| {
            prop_oneof![
                prop::collection::vec(inner.clone(), 0..8).prop_map(Value::Array),
                prop::collection::btree_map("[^\\p{Cc}]{1,16}", inner, 0..8)
                    .prop_map(|m| Value::Object(m.into_iter().collect())),
            ]
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]

        #[test]
        fn generated_json_round_trips(value in json_value()) {
            assert_round_trips(&serde_json::to_string(&value).unwrap());
        }
    }

    /// Round trip a real corpus of newline-delimited JSON.
    ///
    /// Generated JSON explores shapes; a corpus explores what people actually
    /// write. Point `FELDERA_VARIANT_CORPUS` at a file or a directory of
    /// `.json`, `.jsonl` or `.json.gz` files, such as a JSONBench bluesky
    /// dump (`s3://clickhouse-public-datasets/bluesky`, which
    /// `python/tests/workloads/test_jsonbench.py` caches under
    /// `/mnt/data/jsonbench`) or a gharchive hour. `FELDERA_VARIANT_CORPUS_LINES`
    /// caps the number of records.
    ///
    /// Unset, the test does nothing: the data is far too large to check in,
    /// and CI should not depend on a download.
    ///
    /// Run against two gharchive hours either side of GitHub's schema
    /// changes (`2015-01-01-15` and `2024-06-01-12`): 251,253 records,
    /// 570 MiB of JSON, no mismatch.
    #[test]
    fn corpus_json_round_trips() {
        use std::io::BufRead;

        let Ok(path) = std::env::var("FELDERA_VARIANT_CORPUS") else {
            eprintln!(
                "set FELDERA_VARIANT_CORPUS to a file or directory of newline-delimited JSON \
                 to run this"
            );
            return;
        };
        let limit: usize = std::env::var("FELDERA_VARIANT_CORPUS_LINES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(usize::MAX);

        let path = std::path::Path::new(&path);
        let mut files: Vec<std::path::PathBuf> = if path.is_dir() {
            std::fs::read_dir(path)
                .unwrap()
                .filter_map(|entry| Some(entry.ok()?.path()))
                .filter(|p| {
                    let name = p.to_string_lossy();
                    name.ends_with(".json") || name.ends_with(".jsonl") || name.ends_with(".gz")
                })
                .collect()
        } else {
            vec![path.to_path_buf()]
        };
        files.sort();
        assert!(
            !files.is_empty(),
            "no corpus files under {}",
            path.display()
        );

        let started = std::time::Instant::now();
        let (mut records, mut bytes, mut rendered) = (0usize, 0usize, 0usize);

        for file in &files {
            let handle = std::fs::File::open(file).unwrap();
            let reader: Box<dyn std::io::Read> = if file.to_string_lossy().ends_with(".gz") {
                Box::new(flate2::read::MultiGzDecoder::new(handle))
            } else {
                Box::new(handle)
            };

            for (line_number, line) in std::io::BufReader::new(reader).lines().enumerate() {
                if records >= limit {
                    break;
                }
                let line = line.unwrap();
                if line.trim().is_empty() {
                    continue;
                }

                match round_trip(&line) {
                    Ok((before, after, flat_after)) => {
                        assert_eq!(
                            before,
                            after,
                            "Variant changed at {}:{}",
                            file.display(),
                            line_number + 1
                        );
                        assert_eq!(
                            before,
                            flat_after,
                            "FlatVariant changed at {}:{}",
                            file.display(),
                            line_number + 1
                        );
                        rendered += before.len();
                    }
                    Err(e) => panic!(
                        "round trip failed at {}:{}: {e}\n{}",
                        file.display(),
                        line_number + 1,
                        &line[..line.len().min(400)]
                    ),
                }
                records += 1;
                bytes += line.len();
            }
        }

        let elapsed = started.elapsed();
        eprintln!(
            "{records} records from {} file(s): {:.1} MiB in, {:.1} MiB re-rendered, \
             {:.1}s ({:.1} MiB/s)",
            files.len(),
            bytes as f64 / (1 << 20) as f64,
            rendered as f64 / (1 << 20) as f64,
            elapsed.as_secs_f64(),
            bytes as f64 / (1 << 20) as f64 / elapsed.as_secs_f64().max(f64::EPSILON),
        );
        assert!(records > 0, "corpus produced no records");
    }

    /// Documents that cross the encoding's width boundaries, which a small
    /// generated value never reaches: the offset table widens at 256 and
    /// 65536 bytes, and an object or array past 255 entries switches to a
    /// 4-byte count.
    #[test]
    fn wide_documents_round_trip() {
        for count in [1, 255, 256, 300, 4096] {
            let array: Vec<Value> = (0..count).map(|i| Value::from(i as i64)).collect();
            assert_round_trips(&serde_json::to_string(&Value::Array(array)).unwrap());

            let object: serde_json::Map<String, Value> = (0..count)
                .map(|i| (format!("field_{i:06}"), Value::from(i as i64)))
                .collect();
            assert_round_trips(&serde_json::to_string(&Value::Object(object)).unwrap());
        }

        // Past 64 KB, which widens the offsets again.
        let big = Value::Array(
            (0..2000)
                .map(|i| Value::String(format!("{i:0>40}")))
                .collect(),
        );
        assert_round_trips(&serde_json::to_string(&big).unwrap());
    }
}
