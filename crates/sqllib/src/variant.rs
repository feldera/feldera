//! Variant is a dynamically-typed object that can represent
//! the values in a SQL program.

use crate::{
    Date, GeoPoint, LongInterval, ShortInterval, SqlDecimal, SqlString, Time, Timestamp,
    TimestampTz, Uuid, array::Array, binary::ByteArray, casts::*, error::*, map::Map, tn, to_hex_,
    ttn,
};
use dbsp::algebra::{F32, F64};
use feldera_fxp::DynamicDecimal;
use feldera_macros::IsNone;
use feldera_types::serde_with_context::serde_config::VariantFormat;
use feldera_types::serde_with_context::{
    DeserializeWithContext, SerializeWithContext, SqlSerdeConfig,
};
use serde::de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor};
use serde::ser::{self, Error as _};
use serde::{Deserialize, Serialize};
use size_of::SizeOf;
use std::cmp::Ord;
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
use std::fmt::Display;
use std::sync::Arc;
use std::{fmt::Debug, hash::Hash};

/// Represents a Sql value with a VARIANT type.
#[derive(
    Debug,
    Default,
    Eq,
    Ord,
    Clone,
    Hash,
    PartialEq,
    PartialOrd,
    SizeOf,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    IsNone,
)]
#[archive(bound(
    serialize = "__S: rkyv::ser::ScratchSpace + rkyv::ser::Serializer + rkyv::ser::SharedSerializeRegistry",
    deserialize = "__D: rkyv::de::SharedDeserializeRegistry"
))]
#[archive_attr(derive(Eq, Ord, PartialEq, PartialOrd))]
pub enum Variant {
    /// A Variant with a `NULL` SQL value.
    #[default]
    SqlNull,
    /// A Variant with a Variant `null` value.
    VariantNull,
    Boolean(bool),
    TinyInt(i8),
    SmallInt(i16),
    Int(i32),
    BigInt(i64),
    UTinyInt(u8),
    USmallInt(u16),
    UInt(u32),
    UBigInt(u64),
    Real(F32),
    Double(F64),
    SqlDecimal((i128, u8)), // really a DynamicDecimal
    String(SqlString),
    Date(Date),
    Time(Time),
    Timestamp(Timestamp),
    ShortInterval(ShortInterval),
    LongInterval(LongInterval),
    Binary(ByteArray),
    // TODO: this should be called GeoPoint, not Geometry.
    Geometry(GeoPoint),
    Uuid(Uuid),
    #[size_of(skip, skip_bounds)]
    Array(#[omit_bounds] Array<Variant>),
    #[size_of(skip, skip_bounds)]
    Map(#[omit_bounds] Map<Variant, Variant>),
    TimestampTz(TimestampTz),
    // Note: if you extend this enum, add new labels at the end
    // This will hopefully preserve compatibility of the storage format.
}

/////////////// Variant index

// Return type is always Option<Variant>, but result is never None, always a Variant
#[doc(hidden)]
pub fn indexV__<T>(value: &Variant, index: T) -> Option<Variant>
where
    T: Into<Variant>,
{
    value.index(index.into())
}

#[doc(hidden)]
pub fn indexV_N<T>(value: &Variant, index: Option<T>) -> Option<Variant>
where
    T: Into<Variant>,
{
    let index = index?;
    indexV__(value, index)
}

#[doc(hidden)]
pub fn indexVN_<T>(value: &Option<Variant>, index: T) -> Option<Variant>
where
    T: Into<Variant>,
{
    match value {
        None => None,
        Some(value) => indexV__(value, index),
    }
}

#[doc(hidden)]
pub fn indexVNN<T>(value: &Option<Variant>, index: Option<T>) -> Option<Variant>
where
    T: Into<Variant>,
{
    match value {
        None => None,
        Some(value) => indexV_N(value, index),
    }
}

impl<'de> Deserialize<'de> for Variant {
    fn deserialize<D>(deserializer: D) -> Result<Variant, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct VariantVisitor;

        impl<'de> Visitor<'de> for VariantVisitor {
            type Value = Variant;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("any valid JSON value")
            }

            #[inline]
            fn visit_bool<E>(self, value: bool) -> Result<Variant, E> {
                Ok(Variant::Boolean(value))
            }

            #[inline]
            fn visit_i128<E>(self, value: i128) -> Result<Variant, E> {
                Ok(Variant::SqlDecimal((value, 0)))
            }

            #[inline]
            fn visit_i64<E>(self, value: i64) -> Result<Variant, E> {
                Ok(Variant::BigInt(value))
            }

            #[inline]
            fn visit_u64<E>(self, value: u64) -> Result<Variant, E> {
                Ok(Variant::UBigInt(value))
            }

            #[inline]
            fn visit_f64<E>(self, value: f64) -> Result<Variant, E> {
                Ok(Variant::Double(F64::new(value)))
            }

            #[inline]
            fn visit_str<E>(self, value: &str) -> Result<Variant, E>
            where
                E: serde::de::Error,
            {
                self.visit_string(String::from(value))
            }

            #[inline]
            fn visit_string<E>(self, value: String) -> Result<Variant, E> {
                Ok(Variant::String(SqlString::from(value)))
            }

            #[inline]
            fn visit_none<E>(self) -> Result<Variant, E> {
                Ok(Variant::VariantNull)
            }

            #[inline]
            fn visit_some<D>(self, deserializer: D) -> Result<Variant, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                Deserialize::deserialize(deserializer)
            }

            #[inline]
            fn visit_unit<E>(self) -> Result<Variant, E> {
                Ok(Variant::VariantNull)
            }

            #[inline]
            fn visit_seq<V>(self, mut visitor: V) -> Result<Variant, V::Error>
            where
                V: SeqAccess<'de>,
            {
                let mut vec = Vec::new();

                while let Some(elem) = visitor.next_element()? {
                    vec.push(elem);
                }

                Ok(Variant::Array(vec.into()))
            }

            #[inline]
            fn visit_map<V>(self, mut visitor: V) -> Result<Variant, V::Error>
            where
                V: MapAccess<'de>,
            {
                match visitor.next_key_seed(KeyClassifier)? {
                    Some(KeyClass::Number) => {
                        let number: DynamicDecimal = visitor.next_value()?;
                        Ok(Variant::SqlDecimal((
                            number.significand(),
                            number.exponent(),
                        )))
                    }
                    Some(KeyClass::Map(first_key)) => {
                        let mut values = BTreeMap::new();

                        values.insert(
                            Variant::String(SqlString::from(first_key)),
                            visitor.next_value()?,
                        );
                        while let Some((key, value)) = visitor.next_entry::<String, Variant>()? {
                            values.insert(Variant::String(SqlString::from(key)), value);
                        }

                        Ok(Variant::Map(values.into()))
                    }
                    None => Ok(Variant::Map(BTreeMap::new().into())),
                }
            }
        }

        deserializer.deserialize_any(VariantVisitor)
    }
}

impl<'de, AUX> DeserializeWithContext<'de, SqlSerdeConfig, AUX> for Variant {
    fn deserialize_with_context<D>(
        deserializer: D,
        context: &'de SqlSerdeConfig,
    ) -> Result<Variant, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        match context.variant_format {
            VariantFormat::Json => Variant::deserialize(deserializer),
            VariantFormat::JsonString => crate::variant_binary::deserialize_variant(deserializer),
        }
    }
}

#[doc(hidden)]
struct KeyClassifier;

#[doc(hidden)]
enum KeyClass {
    Map(String),
    Number,
}

#[doc(hidden)]
impl<'de> DeserializeSeed<'de> for KeyClassifier {
    type Value = KeyClass;

    #[doc(hidden)]
    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(self)
    }
}

// This is defined in serde_json, but not exported.
#[doc(hidden)]
const DECIMAL_KEY_TOKEN: &str = "$serde_json::private::Number";

#[doc(hidden)]
impl Visitor<'_> for KeyClassifier {
    type Value = KeyClass;

    #[doc(hidden)]
    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a string key")
    }

    #[doc(hidden)]
    fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        match s {
            DECIMAL_KEY_TOKEN => Ok(KeyClass::Number),
            _ => Ok(KeyClass::Map(s.to_owned())),
        }
    }

    #[doc(hidden)]
    fn visit_string<E>(self, s: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        match s.as_str() {
            DECIMAL_KEY_TOKEN => Ok(KeyClass::Number),
            _ => Ok(KeyClass::Map(s)),
        }
    }
}

impl Serialize for Variant {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.serialize_with_context(
            serializer,
            &SqlSerdeConfig::default().with_variant_format(VariantFormat::Json),
        )
    }
}

impl SerializeWithContext<SqlSerdeConfig> for Variant {
    fn serialize_with_context<S>(
        &self,
        serializer: S,
        context: &SqlSerdeConfig,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
        S::Error: ser::Error,
    {
        match context.variant_format {
            VariantFormat::JsonString => {
                serializer.serialize_str(&self.to_json_string().map_err(|e| {
                    S::Error::custom(format!("error serializing VARIANT to JSON string: {e}"))
                })?)
            }
            VariantFormat::Json => match self {
                Variant::SqlNull | Variant::VariantNull => serializer.serialize_none(),
                Variant::Boolean(v) => v.serialize_with_context(serializer, context),
                Variant::TinyInt(v) => v.serialize_with_context(serializer, context),
                Variant::SmallInt(v) => v.serialize_with_context(serializer, context),
                Variant::Int(v) => v.serialize_with_context(serializer, context),
                Variant::BigInt(v) => v.serialize_with_context(serializer, context),
                Variant::UTinyInt(v) => v.serialize_with_context(serializer, context),
                Variant::USmallInt(v) => v.serialize_with_context(serializer, context),
                Variant::UInt(v) => v.serialize_with_context(serializer, context),
                Variant::UBigInt(v) => v.serialize_with_context(serializer, context),
                Variant::Real(v) => v.serialize_with_context(serializer, context),
                Variant::Double(v) => v.serialize_with_context(serializer, context),
                Variant::SqlDecimal(v) => {
                    DynamicDecimal::new(v.0, v.1).serialize_with_context(serializer, context)
                }
                Variant::String(v) => v.serialize_with_context(serializer, context),
                Variant::Date(v) => v.serialize_with_context(serializer, context),
                Variant::Time(v) => v.serialize_with_context(serializer, context),
                Variant::Timestamp(v) => v.serialize_with_context(serializer, context),
                Variant::TimestampTz(v) => v.serialize_with_context(serializer, context),
                Variant::ShortInterval(v) => v.serialize_with_context(serializer, context),
                Variant::LongInterval(v) => v.serialize_with_context(serializer, context),
                Variant::Geometry(v) => v.serialize_with_context(serializer, context),
                Variant::Array(a) => a.serialize_with_context(serializer, context),
                Variant::Binary(a) => a.serialize_with_context(serializer, context),
                Variant::Map(m) => m.serialize_with_context(serializer, context),
                Variant::Uuid(u) => u.serialize_with_context(serializer, context),
            },
        }
    }
}

impl Variant {
    /// Get the runtime type of a Variant value
    fn get_type_string(&self) -> &'static str {
        match self {
            Variant::SqlNull => "NULL",
            Variant::VariantNull => "VARIANT",
            Variant::Boolean(_) => "BOOLEAN",
            Variant::TinyInt(_) => "TINYINT",
            Variant::SmallInt(_) => "SMALLINT",
            Variant::Int(_) => "INTEGER",
            Variant::BigInt(_) => "BIGINT",
            Variant::UTinyInt(_) => "TINYINT UNSIGNED",
            Variant::USmallInt(_) => "SMALLINT UNSIGNED",
            Variant::UInt(_) => "INTEGER UNSIGNED",
            Variant::UBigInt(_) => "BIGINT UNSIGNED",
            Variant::Real(_) => "REAL",
            Variant::Double(_) => "DOUBLE",
            Variant::SqlDecimal(_) => "DECIMAL",
            Variant::String(_) => "VARCHAR",
            Variant::Date(_) => "DATE",
            Variant::Time(_) => "TIME",
            Variant::Timestamp(_) => "TIMESTAMP",
            Variant::TimestampTz(_) => "TIMESTAMP WITH TIME ZONE",
            Variant::ShortInterval(_) => "SHORTINTERVAL",
            Variant::LongInterval(_) => "LONGINTERVAL",
            Variant::Geometry(_) => "GEOPOINT",
            Variant::Binary(_) => "BINARY",
            Variant::Array(_) => "ARRAY",
            Variant::Map(_) => "MAP",
            Variant::Uuid(_) => "UUID",
        }
    }

    #[doc(hidden)]
    pub fn index_string<I: AsRef<str>>(&self, index: I) -> Variant {
        match self {
            Variant::Map(value) => match value.get(&Variant::String(SqlString::from(
                index.as_ref().to_string(),
            ))) {
                None => Variant::SqlNull,
                Some(result) => result.clone(),
            },
            _ => Variant::SqlNull,
        }
    }

    #[doc(hidden)]
    pub fn index(&self, index: Variant) -> Option<Variant> {
        match self {
            Variant::Array(value) => {
                let index = match index {
                    Variant::TinyInt(index) => index as isize,
                    Variant::SmallInt(index) => index as isize,
                    Variant::Int(index) => index as isize,
                    Variant::BigInt(index) => index as isize,
                    Variant::UTinyInt(index) => index as isize,
                    Variant::USmallInt(index) => index as isize,
                    Variant::UInt(index) => index as isize,
                    Variant::UBigInt(index) => index as isize,
                    _ => 0, // out of bounds
                } - 1; // Array indexes in SQL start from 1!
                if (index < 0) || (index as usize >= value.len()) {
                    None
                } else {
                    Some(value[index as usize].clone())
                }
            }
            Variant::Map(value) => value.get(&index).cloned(),
            _ => None,
        }
    }

    /// Convert a Variant to a String representing a JSON encoding of
    /// the Variant value.
    pub fn to_json_string(&self) -> Result<String, Box<dyn std::error::Error>> {
        Ok(serde_json::to_string(self)?)
    }
}

// A macro for From<T> for Variant
macro_rules! from {
    ($variant:ident, $type:ty) => {
        #[doc(hidden)]
        impl From<$type> for Variant {
            #[doc(hidden)]
            fn from(value: $type) -> Self {
                Variant::$variant(value)
            }
        }

        #[doc(hidden)]
        impl From<Option<$type>> for Variant {
            #[doc(hidden)]
            fn from(value: Option<$type>) -> Self {
                match value {
                    None => Variant::SqlNull,
                    Some(value) => Variant::$variant(value),
                }
            }
        }
    };
}

from!(Boolean, bool);
from!(TinyInt, i8);
from!(SmallInt, i16);
from!(Int, i32);
from!(BigInt, i64);
from!(UTinyInt, u8);
from!(USmallInt, u16);
from!(UInt, u32);
from!(UBigInt, u64);
from!(Real, F32);
from!(Double, F64);
from!(String, SqlString);
from!(Date, Date);
from!(Time, Time);
from!(Timestamp, Timestamp);
from!(TimestampTz, TimestampTz);
from!(ShortInterval, ShortInterval);
from!(LongInterval, LongInterval);
from!(Geometry, GeoPoint);
from!(Binary, ByteArray);
from!(Uuid, Uuid);

impl<const P: usize, const S: usize> From<SqlDecimal<P, S>> for Variant {
    #[doc(hidden)]
    fn from(value: SqlDecimal<P, S>) -> Self {
        let dd: DynamicDecimal = value.into();
        Variant::SqlDecimal((dd.significand(), dd.exponent()))
    }
}

#[doc(hidden)]
impl<const P: usize, const S: usize> From<Option<SqlDecimal<P, S>>> for Variant {
    #[doc(hidden)]
    fn from(value: Option<SqlDecimal<P, S>>) -> Self {
        match value {
            None => Variant::SqlNull,
            Some(value) => {
                let dd: DynamicDecimal = value.into();
                Variant::SqlDecimal((dd.significand(), dd.exponent()))
            }
        }
    }
}

#[doc(hidden)]
impl From<Option<Variant>> for Variant {
    #[doc(hidden)]
    fn from(value: Option<Variant>) -> Self {
        match value {
            None => Variant::SqlNull,
            Some(value) => value,
        }
    }
}

#[doc(hidden)]
impl<T> From<Array<T>> for Variant
where
    Variant: From<T>,
    T: Clone,
{
    #[doc(hidden)]
    fn from(vec: Array<T>) -> Self {
        Variant::Array(
            (*vec)
                .iter()
                .map(|val| Variant::from(val.clone()))
                .collect::<Vec<Variant>>()
                .into(),
        )
    }
}

#[doc(hidden)]
impl<T> From<Option<Array<T>>> for Variant
where
    Variant: From<T>,
    T: Clone,
{
    #[doc(hidden)]
    fn from(vec: Option<Array<T>>) -> Self {
        match vec {
            None => Variant::SqlNull,
            Some(vec) => Variant::Array(Arc::new(
                (*vec)
                    .iter()
                    .map(|val| Variant::from(val.clone()))
                    .collect::<Vec<Variant>>(),
            )),
        }
    }
}

#[doc(hidden)]
impl<K, V> From<Map<K, V>> for Variant
where
    Variant: From<K> + From<V>,
    K: Clone + Ord,
    V: Clone,
{
    #[doc(hidden)]
    fn from(map: Map<K, V>) -> Self {
        let mut result = BTreeMap::<Variant, Variant>::new();
        for (key, value) in map.iter() {
            result.insert(key.clone().into(), value.clone().into());
        }
        Variant::Map(result.into())
    }
}

#[doc(hidden)]
impl<K, V> From<Option<Map<K, V>>> for Variant
where
    Variant: From<K> + From<V>,
    K: Clone + Ord,
    V: Clone,
{
    #[doc(hidden)]
    fn from(map: Option<Map<K, V>>) -> Self {
        match map {
            None => Variant::SqlNull,
            Some(map) => Variant::from(map),
        }
    }
}

//////////////////// Reverse conversions Variant -> T

// Conversion from Variant to a specific type
macro_rules! into {
    ($variant:ident, $type:ty, $type_name: ident) => {
        ::paste::paste! {
            #[doc(hidden)]
            impl TryFrom<Variant> for $type {
                type Error = Box<SqlRuntimeError>;

                #[doc(hidden)]
                fn try_from(value: Variant) -> Result<Self, Self::Error> {
                    match value {
                        Variant::String(x) => [< cast_to_ $type_name _s>](x),
                        Variant::$variant(x) => Ok(x),
                        _ => Err(SqlRuntimeError::from_string(format!(
                            "variant is {}, which cannot be converted to {}",
                            typeof_(value),
                            ttn!($type),
                        ))),
                    }
                }
            }

            #[doc(hidden)]
            impl TryFrom<Variant> for Option<$type> {
                type Error = Box<SqlRuntimeError>;

                #[doc(hidden)]
                fn try_from(value: Variant) -> Result<Self, Self::Error> {
                    match value {
                        Variant::SqlNull => Ok(None),
                        Variant::VariantNull => Ok(None),
                        _ => match <$type>::try_from(value) {
                            Ok(result) => Ok(Some(result)),
                            Err(e) => Err(SqlRuntimeError::from_string(e.to_string())),
                        },
                    }
                }
            }
        }
    };
}

// Like into! but no string conversion
macro_rules! into_no_string {
    ($variant:ident, $type:ty, $type_name: ident) => {
        #[doc(hidden)]
        impl TryFrom<Variant> for $type {
            type Error = Box<SqlRuntimeError>;

            #[doc(hidden)]
            fn try_from(value: Variant) -> Result<Self, Self::Error> {
                match value {
                    Variant::$variant(x) => Ok(x),
                    _ => Err(SqlRuntimeError::from_string(format!(
                        "variant is {}, which cannot be converted to {}",
                        typeof_(value),
                        ttn!($type),
                    ))),
                }
            }
        }

        #[doc(hidden)]
        impl TryFrom<Variant> for Option<$type> {
            type Error = Box<SqlRuntimeError>;

            #[doc(hidden)]
            fn try_from(value: Variant) -> Result<Self, Self::Error> {
                match value {
                    Variant::SqlNull => Ok(None),
                    Variant::VariantNull => Ok(None),
                    _ => match <$type>::try_from(value) {
                        Ok(result) => Ok(Some(result)),
                        Err(e) => Err(SqlRuntimeError::from_string(e.to_string())),
                    },
                }
            }
        }
    };
}

into!(Boolean, bool, b);
into!(Date, Date, Date);
into!(Time, Time, Time);
into!(Timestamp, Timestamp, Timestamp);
into!(TimestampTz, TimestampTz, TimestampTz);
into!(ShortInterval, ShortInterval, ShortInterval_DAYS_TO_MINUTES);
into!(LongInterval, LongInterval, LongInterval_YEARS_TO_MONTHS);
into!(Uuid, Uuid, Uuid);

into_no_string!(Geometry, GeoPoint, GeoPoint);
into_no_string!(Binary, ByteArray, bytes);

macro_rules! into_numeric {
    ($type:ty, $type_name: ident) => {
        #[doc(hidden)]
        impl TryFrom<Variant> for $type {
            type Error = Box<SqlRuntimeError>;

            ::paste::paste! {
                #[doc(hidden)]
                fn try_from(value: Variant) -> Result<Self, Self::Error> {
                    match value {
                        Variant::String(x) => [< cast_to_ $type_name _s>](x),
                        Variant::TinyInt(x) => [< cast_to_ $type_name _i8>](x),
                        Variant::SmallInt(x) => [< cast_to_ $type_name _i16>](x),
                        Variant::Int(x) => [< cast_to_ $type_name _i32 >](x),
                        Variant::BigInt(x) => [< cast_to_ $type_name _i64 >](x),
                        Variant::UTinyInt(x) => [< cast_to_ $type_name _u8>](x),
                        Variant::USmallInt(x) => [< cast_to_ $type_name _u16>](x),
                        Variant::UInt(x) => [< cast_to_ $type_name _u32 >](x),
                        Variant::UBigInt(x) => [< cast_to_ $type_name _u64 >](x),
                        Variant::Real(x) => [< cast_to_ $type_name _f >](x),
                        Variant::Double(x) => [< cast_to_ $type_name _d >](x),
                        Variant::SqlDecimal(d) => match i128::try_from(DynamicDecimal::new(d.0, d.1)) {
                            Ok(value) => [< cast_to_ $type_name _i128 >](value),
                            Err(_) => Err(SqlRuntimeError::from_string(format!(
                                "variant is {}, which cannot be converted to {}",
                                typeof_(value),
                                tn!($type),
                            ))),
                        },
                        _ => Err(SqlRuntimeError::from_string(format!(
                            "variant is {}, which cannot be converted to {}",
                            typeof_(value),
                            tn!($type),
                        ))),
                    }
                }
            }
        }

        #[doc(hidden)]
        impl TryFrom<Variant> for Option<$type> {
            type Error = Box<dyn Error>;

            #[doc(hidden)]
            fn try_from(value: Variant) -> Result<Self, Self::Error> {
                match value {
                    Variant::VariantNull => Ok(None),
                    Variant::SqlNull => Ok(None),
                    _ => match <$type>::try_from(value) {
                        Ok(result) => Ok(Some(result)),
                        Err(e) => Err(SqlRuntimeError::from_string(e.to_string())),
                    },
                }
            }
        }
    };
}

into_numeric!(i8, i8);
into_numeric!(i16, i16);
into_numeric!(i32, i32);
into_numeric!(i64, i64);
into_numeric!(u8, u8);
into_numeric!(u16, u16);
into_numeric!(u32, u32);
into_numeric!(u64, u64);
into_numeric!(F32, f);
into_numeric!(F64, d);

#[doc(hidden)]
impl TryFrom<Variant> for SqlString {
    type Error = Box<SqlRuntimeError>;

    #[doc(hidden)]
    fn try_from(value: Variant) -> Result<Self, Self::Error> {
        match value {
            Variant::Boolean(x) => Ok(SqlString::from(if x { "true" } else { "false" })),
            Variant::TinyInt(x) => Ok(SqlString::from(format!("{x}"))),
            Variant::SmallInt(x) => Ok(SqlString::from(format!("{x}"))),
            Variant::Int(x) => Ok(SqlString::from(format!("{x}"))),
            Variant::BigInt(x) => Ok(SqlString::from(format!("{x}"))),
            Variant::UTinyInt(x) => Ok(SqlString::from(format!("{x}"))),
            Variant::USmallInt(x) => Ok(SqlString::from(format!("{x}"))),
            Variant::UInt(x) => Ok(SqlString::from(format!("{x}"))),
            Variant::UBigInt(x) => Ok(SqlString::from(format!("{x}"))),
            Variant::SqlDecimal(x) => Ok(SqlString::from(format!(
                "{}",
                DynamicDecimal::new(x.0, x.1)
            ))),
            Variant::Real(x) => {
                let mut buffer = ryu::Buffer::new();
                let result = buffer.format(x.into_inner());
                Ok(SqlString::from(result))
            }
            Variant::Double(x) => {
                let mut buffer = ryu::Buffer::new();
                let result = buffer.format(x.into_inner());
                Ok(SqlString::from(result))
            }
            Variant::String(x) => Ok(x),
            Variant::Date(x) => Ok(SqlString::from(x.to_string())),
            Variant::Time(x) => Ok(SqlString::from(x.to_string())),
            Variant::Timestamp(x) => Ok(SqlString::from(x.to_string())),
            Variant::TimestampTz(x) => Ok(SqlString::from(x.to_string())),
            Variant::ShortInterval(x) => Ok(SqlString::from(x.to_string())),
            Variant::LongInterval(x) => Ok(SqlString::from(x.to_string())),
            Variant::Binary(x) => Ok(to_hex_(x)),
            Variant::Uuid(x) => Ok(SqlString::from(format!("{x}"))),
            // GeoPoint does not have a cast to string
            // Map does not have a cast to string
            // Array does not have a cast to string
            _ => Err(SqlRuntimeError::from_string(format!(
                "variant is {}, which cannot be converted to CHAR",
                typeof_(value),
            ))),
        }
    }
}

#[doc(hidden)]
impl TryFrom<Variant> for Option<SqlString> {
    type Error = Box<dyn Error>;

    #[doc(hidden)]
    fn try_from(value: Variant) -> Result<Self, Self::Error> {
        match value {
            Variant::SqlNull => Ok(None),
            Variant::VariantNull => Ok(None),
            _ => match SqlString::try_from(value) {
                Ok(result) => Ok(Some(result)),
                Err(e) => Err(SqlRuntimeError::from_string(e.to_string())),
            },
        }
    }
}

#[doc(hidden)]
impl<const P: usize, const S: usize> TryFrom<Variant> for SqlDecimal<P, S> {
    type Error = Box<SqlRuntimeError>;

    #[doc(hidden)]
    fn try_from(value: Variant) -> Result<Self, Self::Error> {
        match value {
            Variant::TinyInt(x) => convert_error(SqlDecimal::<P, S>::try_from(x)),
            Variant::SmallInt(x) => convert_error(SqlDecimal::<P, S>::try_from(x)),
            Variant::Int(x) => convert_error(SqlDecimal::<P, S>::try_from(x)),
            Variant::BigInt(x) => convert_error(SqlDecimal::<P, S>::try_from(x)),
            Variant::UTinyInt(x) => convert_error(SqlDecimal::<P, S>::try_from(x)),
            Variant::USmallInt(x) => convert_error(SqlDecimal::<P, S>::try_from(x)),
            Variant::UInt(x) => convert_error(SqlDecimal::<P, S>::try_from(x)),
            Variant::UBigInt(x) => convert_error(SqlDecimal::<P, S>::try_from(x)),
            Variant::Real(x) => convert_error(SqlDecimal::<P, S>::try_from(x.into_inner() as f64)),
            Variant::Double(x) => convert_error(SqlDecimal::<P, S>::try_from(x.into_inner())),
            Variant::SqlDecimal(d) => {
                let dd = DynamicDecimal::new(d.0, d.1);
                match SqlDecimal::<P, S>::try_from(dd) {
                    Err(_) => Err(SqlRuntimeError::from_string(format!(
                        "variant is {}, which cannot be converted to DECIMAL({P}, {S})",
                        typeof_(value),
                    ))),
                    Ok(value) => Ok(value),
                }
            }
            _ => Err(SqlRuntimeError::from_string(format!(
                "variant is {}, which cannot be converted to DECIMAL({P}, {S})",
                typeof_(value),
            ))),
        }
    }
}

#[doc(hidden)]
impl<const P: usize, const S: usize> TryFrom<Variant> for Option<SqlDecimal<P, S>> {
    type Error = Box<dyn Error>;

    #[doc(hidden)]
    fn try_from(value: Variant) -> Result<Self, Self::Error> {
        match value {
            Variant::VariantNull => Ok(None),
            Variant::SqlNull => Ok(None),
            _ => match SqlDecimal::<P, S>::try_from(value) {
                Ok(result) => Ok(Some(result)),
                Err(e) => Err(SqlRuntimeError::from_string(e.to_string())),
            },
        }
    }
}

#[doc(hidden)]
impl<T> TryFrom<Variant> for Array<T>
where
    T: TryFrom<Variant>,
    T::Error: Display,
{
    type Error = Box<dyn Error>;

    #[doc(hidden)]
    fn try_from(value: Variant) -> Result<Self, Self::Error> {
        match value {
            Variant::Array(a) => {
                let mut result = Vec::with_capacity(a.len());
                for e in &*a {
                    let converted = T::try_from(e.clone());
                    match converted {
                        Ok(value) => result.push(value),
                        Err(e) => return Err(SqlRuntimeError::from_string(e.to_string())),
                    }
                }
                Ok(result.into())
            }
            _ => Err(Box::new(SqlRuntimeError::CustomError(
                "not an array".to_string(),
            ))),
        }
    }
}

#[doc(hidden)]
impl<T> TryFrom<Variant> for Option<Array<T>>
where
    T: TryFrom<Variant>,
    T::Error: Display,
{
    type Error = Box<dyn Error>;

    #[doc(hidden)]
    fn try_from(value: Variant) -> Result<Self, Self::Error> {
        match value {
            Variant::VariantNull => Ok(None),
            Variant::SqlNull => Ok(None),
            _ => match Array::<T>::try_from(value) {
                Ok(result) => Ok(Some(result)),
                Err(e) => Err(SqlRuntimeError::from_string(e.to_string())),
            },
        }
    }
}

#[doc(hidden)]
impl<K, V> TryFrom<Variant> for Map<K, V>
where
    K: TryFrom<Variant> + Ord,
    K::Error: Display,
    V: TryFrom<Variant>,
    V::Error: Display,
{
    type Error = Box<dyn Error>;

    #[doc(hidden)]
    fn try_from(value: Variant) -> Result<Self, Self::Error> {
        match value {
            Variant::Map(map) => {
                let mut result = BTreeMap::<K, V>::new();
                for (key, value) in (*map).iter() {
                    let convertedKey = K::try_from(key.clone());
                    let convertedValue = V::try_from(value.clone());
                    let k = match convertedKey {
                        Ok(result) => result,
                        Err(e) => return Err(SqlRuntimeError::from_string(e.to_string())),
                    };
                    let v = match convertedValue {
                        Ok(result) => result,
                        Err(e) => return Err(SqlRuntimeError::from_string(e.to_string())),
                    };
                    result.insert(k, v);
                }
                Ok(result.into())
            }
            _ => Err(Box::new(SqlRuntimeError::CustomError(
                "not a map".to_string(),
            ))),
        }
    }
}

#[doc(hidden)]
impl<K, V> TryFrom<Variant> for Option<Map<K, V>>
where
    K: TryFrom<Variant> + Ord,
    K::Error: Display,
    V: TryFrom<Variant>,
    V::Error: Display,
{
    type Error = Box<dyn Error>;

    #[doc(hidden)]
    fn try_from(value: Variant) -> Result<Self, Self::Error> {
        match value {
            Variant::VariantNull => Ok(None),
            Variant::SqlNull => Ok(None),
            _ => match Map::<K, V>::try_from(value) {
                Ok(result) => Ok(Some(result)),
                Err(e) => Err(SqlRuntimeError::from_string(e.to_string())),
            },
        }
    }
}

#[doc(hidden)]
pub fn typeof_(value: Variant) -> SqlString {
    SqlString::from_ref(value.get_type_string())
}

#[doc(hidden)]
pub fn typeofN(value: Option<Variant>) -> SqlString {
    match value {
        None => SqlString::from_ref("NULL"),
        Some(value) => SqlString::from_ref(value.get_type_string()),
    }
}

#[doc(hidden)]
pub fn variantnull() -> Variant {
    Variant::VariantNull
}

/////////////// JSON_EACH_* functions

/// True if the variant holds a numeric value with no fractional part.
/// The integer extraction functions use this filter so that a field holding
/// e.g. 2.5 is not silently truncated to 2; such fields can still be
/// selected with `variant_filter_`.
fn is_integral_numeric_variant(value: &Variant) -> bool {
    match value {
        Variant::TinyInt(_)
        | Variant::SmallInt(_)
        | Variant::Int(_)
        | Variant::BigInt(_)
        | Variant::UTinyInt(_)
        | Variant::USmallInt(_)
        | Variant::UInt(_)
        | Variant::UBigInt(_) => true,
        Variant::Real(x) => x.into_inner().fract() == 0.0,
        Variant::Double(x) => x.into_inner().fract() == 0.0,
        Variant::SqlDecimal((sig, exp)) => match 10i128.checked_pow(*exp as u32) {
            Some(divisor) => sig % divisor == 0,
            None => *sig == 0,
        },
        _ => false,
    }
}

/// Extract from a variant holding a map all fields whose values have a runtime
/// type accepted by `keep` and convert to `T`.  The `keep` filter selects by
/// runtime type before conversion, so e.g. a string field never converts to a
/// number.  Fields that do not qualify or do not convert are omitted; so are
/// fields with non-string keys.  A variant that does not hold a map produces
/// an empty result.
fn json_each_typed<T>(value: Variant, keep: fn(&Variant) -> bool) -> Map<SqlString, Option<T>>
where
    T: TryFrom<Variant>,
{
    let mut result = BTreeMap::<SqlString, Option<T>>::new();
    if let Variant::Map(map) = value {
        for (key, val) in map.iter() {
            let Variant::String(key) = key else { continue };
            if !keep(val) {
                continue;
            }
            if let Ok(converted) = T::try_from(val.clone()) {
                result.insert(key.clone(), Some(converted));
            }
        }
    }
    result.into()
}

macro_rules! json_each {
    ($type_name:ident, $type:ty, $keep:expr) => {
        ::paste::paste! {
            #[doc(hidden)]
            pub fn [<json_each_ $type_name _V>](value: Variant) -> Map<SqlString, Option<$type>> {
                json_each_typed(value, $keep)
            }

            crate::some_polymorphic_function1!([<json_each_ $type_name>], V, Variant, Map<SqlString, Option<$type>>);
        }
    };
}

json_each!(bigint, i64, is_integral_numeric_variant);
json_each!(string, SqlString, |v| matches!(v, Variant::String(_)));
json_each!(boolean, bool, |v| matches!(v, Variant::Boolean(_)));
json_each!(date, Date, |v| matches!(
    v,
    Variant::Date(_) | Variant::String(_)
));
json_each!(time, Time, |v| matches!(
    v,
    Variant::Time(_) | Variant::String(_)
));
json_each!(timestamp, Timestamp, |v| matches!(
    v,
    Variant::Timestamp(_) | Variant::String(_)
));

/////////////// JSON_OBJECT_KEYS and JSON_KEYS

/// The top-level keys of a variant holding a map, sorted, following the
/// Postgres `json_object_keys` function.  Non-string keys are skipped.
/// A variant that does not hold a map produces an empty result.
#[doc(hidden)]
pub fn json_object_keys_V(value: Variant) -> Array<SqlString> {
    let mut result = Vec::new();
    if let Variant::Map(map) = value {
        for key in map.keys() {
            if let Variant::String(key) = key {
                result.push(key.clone());
            }
        }
    }
    result.into()
}

crate::some_polymorphic_function1!(json_object_keys, V, Variant, Array<SqlString>);

/// The keys of all nested objects in a variant, as dot-joined paths,
/// deduplicated and sorted, following the BigQuery `JSON_KEYS` function in
/// 'strict' mode: arrays are not traversed, and keys that are not
/// identifiers are double-quoted.  Non-string keys are skipped.
/// A variant that does not hold a map produces an empty result.
#[doc(hidden)]
pub fn json_keys_V(value: Variant) -> Array<SqlString> {
    fn collect(prefix: &str, map: &Map<Variant, Variant>, result: &mut BTreeSet<SqlString>) {
        for (key, val) in map.iter() {
            let Variant::String(key) = key else { continue };
            let path = append_path_component(prefix, key.str());
            if let Variant::Map(inner) = val {
                collect(&path, inner, result);
            }
            result.insert(SqlString::from(path));
        }
    }
    let mut result = BTreeSet::new();
    if let Variant::Map(map) = value {
        collect("", &map, &mut result);
    }
    result.into_iter().collect::<Vec<_>>().into()
}

crate::some_polymorphic_function1!(json_keys, V, Variant, Array<SqlString>);

/////////////// VARIANT_FILTER and VARIANT_DEEP_FILTER

/// An item is kept only when the predicate is 'true', like a SQL WHERE clause.
pub(crate) fn predicate_keeps<B: Into<Option<bool>>>(result: B) -> bool {
    result.into() == Some(true)
}

/// True if the key can appear in a path without quotes.
fn is_identifier_key(key: &str) -> bool {
    let mut chars = key.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Append a member-access component to a path.  A key that is not an
/// identifier is double-quoted, with backslashes escaping embedded quotes
/// and backslashes, so that paths are unambiguous: the key `a.b` produces
/// the path component `"a.b"`, distinct from the nested path `a.b`.
pub(crate) fn append_path_component(prefix: &str, key: &str) -> String {
    let mut result = String::with_capacity(prefix.len() + key.len() + 4);
    result.push_str(prefix);
    if !prefix.is_empty() {
        result.push('.');
    }
    if is_identifier_key(key) {
        result.push_str(key);
    } else {
        result.push('"');
        for c in key.chars() {
            if c == '"' || c == '\\' {
                result.push('\\');
            }
            result.push(c);
        }
        result.push('"');
    }
    result
}

/// Filter a variant with a predicate over (label, value) items.  A variant
/// holding a map contributes one item per field, labeled by its key; any
/// other variant is a single item with a `None` label, kept whole or dropped
/// entirely.  A dropped non-map variant produces `None` (SQL `NULL`).
#[doc(hidden)]
pub fn variant_filter_<F, B>(value: Variant, predicate: F) -> Option<Variant>
where
    F: Fn(&Option<Variant>, &Variant) -> B,
    B: Into<Option<bool>>,
{
    match value {
        Variant::Map(map) => {
            let mut result = BTreeMap::new();
            for (key, val) in map.iter() {
                if predicate_keeps(predicate(&Some(key.clone()), val)) {
                    result.insert(key.clone(), val.clone());
                }
            }
            Some(Variant::Map(result.into()))
        }
        other => {
            if predicate_keeps(predicate(&None, &other)) {
                Some(other)
            } else {
                None
            }
        }
    }
}

#[doc(hidden)]
pub fn variant_filterN<F, B>(value: Option<Variant>, predicate: F) -> Option<Variant>
where
    F: Fn(&Option<Variant>, &Variant) -> B,
    B: Into<Option<bool>>,
{
    variant_filter_(value?, predicate)
}

/// Recurse into a kept container; leaves are kept as they are.
fn deep_filter_value<F, B>(path: &str, value: &Variant, predicate: &F) -> Variant
where
    F: Fn(&Option<SqlString>, &Variant) -> B,
    B: Into<Option<bool>>,
{
    match value {
        Variant::Map(inner) => Variant::Map(deep_filter_map(path, inner, predicate)),
        Variant::Array(inner) => Variant::Array(deep_filter_array(path, inner, predicate)),
        other => other.clone(),
    }
}

fn deep_filter_map<F, B>(
    prefix: &str,
    map: &Map<Variant, Variant>,
    predicate: &F,
) -> Map<Variant, Variant>
where
    F: Fn(&Option<SqlString>, &Variant) -> B,
    B: Into<Option<bool>>,
{
    let mut result = BTreeMap::new();
    for (key, val) in map.iter() {
        let Variant::String(k) = key else {
            // A key that is not a string cannot appear in a path;
            // keep the field untouched rather than deleting data silently
            result.insert(key.clone(), val.clone());
            continue;
        };
        let path = append_path_component(prefix, k.str());
        let label = Some(SqlString::from_ref(&path));
        if predicate_keeps(predicate(&label, val)) {
            result.insert(key.clone(), deep_filter_value(&path, val, predicate));
        }
    }
    result.into()
}

fn deep_filter_array<F, B>(prefix: &str, array: &Array<Variant>, predicate: &F) -> Array<Variant>
where
    F: Fn(&Option<SqlString>, &Variant) -> B,
    B: Into<Option<bool>>,
{
    let mut result = Vec::new();
    for (index, val) in array.iter().enumerate() {
        // SQL array indexes start from 1
        let path = format!("{prefix}[{}]", index + 1);
        let label = Some(SqlString::from_ref(&path));
        if predicate_keeps(predicate(&label, val)) {
            result.push(deep_filter_value(&path, val, predicate));
        }
    }
    result.into()
}

/// Like `variant_filter_`, but recursive: the label is the dot-joined path of
/// the item, a string (array elements use 1-based bracket components, e.g.
/// "a[1].b"; keys that are not identifiers are double-quoted), and containers
/// kept by the predicate have their contents filtered recursively.  The
/// predicate receives the original, unfiltered value of each item.
#[doc(hidden)]
pub fn variant_deep_filter_<F, B>(value: Variant, predicate: F) -> Option<Variant>
where
    F: Fn(&Option<SqlString>, &Variant) -> B,
    B: Into<Option<bool>>,
{
    match value {
        Variant::Map(map) => Some(Variant::Map(deep_filter_map("", &map, &predicate))),
        Variant::Array(array) => Some(Variant::Array(deep_filter_array("", &array, &predicate))),
        other => {
            if predicate_keeps(predicate(&None, &other)) {
                Some(other)
            } else {
                None
            }
        }
    }
}

#[doc(hidden)]
pub fn variant_deep_filterN<F, B>(value: Option<Variant>, predicate: F) -> Option<Variant>
where
    F: Fn(&Option<SqlString>, &Variant) -> B,
    B: Into<Option<bool>>,
{
    variant_deep_filter_(value?, predicate)
}

/////////////// VARIANT_MAP

/// Map a variant with a function over (label, value) items, building an
/// isomorphic result: a variant holding a map produces a map with the same
/// keys and the mapped values; any other variant is a single item with a
/// `None` label whose mapped value is the result.  A SQL `NULL` produced by
/// the mapper becomes a variant `SqlNull` inside a map.  Top-level only.
#[doc(hidden)]
pub fn variant_map_<F, R>(value: Variant, mapper: F) -> Option<Variant>
where
    F: Fn(&Option<Variant>, &Variant) -> R,
    R: Into<Option<Variant>>,
{
    match value {
        Variant::Map(map) => {
            let mut result = BTreeMap::new();
            for (key, val) in map.iter() {
                let mapped = mapper(&Some(key.clone()), val)
                    .into()
                    .unwrap_or(Variant::SqlNull);
                result.insert(key.clone(), mapped);
            }
            Some(Variant::Map(result.into()))
        }
        other => mapper(&None, &other).into(),
    }
}

#[doc(hidden)]
pub fn variant_mapN<F, R>(value: Option<Variant>, mapper: F) -> Option<Variant>
where
    F: Fn(&Option<Variant>, &Variant) -> R,
    R: Into<Option<Variant>>,
{
    variant_map_(value?, mapper)
}

/////////////// VARIANT_DEEP_MAP

/// Map one node; containers recurse, leaves are transformed.
fn deep_map_value<F, R>(path: &str, value: &Variant, mapper: &F) -> Variant
where
    F: Fn(&Option<SqlString>, &Variant) -> R,
    R: Into<Option<Variant>>,
{
    match value {
        Variant::Map(inner) => Variant::Map(deep_map_map(path, inner, mapper)),
        Variant::Array(inner) => Variant::Array(deep_map_array(path, inner, mapper)),
        leaf => mapper(&Some(SqlString::from_ref(path)), leaf)
            .into()
            .unwrap_or(Variant::SqlNull),
    }
}

fn deep_map_map<F, R>(
    prefix: &str,
    map: &Map<Variant, Variant>,
    mapper: &F,
) -> Map<Variant, Variant>
where
    F: Fn(&Option<SqlString>, &Variant) -> R,
    R: Into<Option<Variant>>,
{
    let mut result = BTreeMap::new();
    for (key, val) in map.iter() {
        let Variant::String(k) = key else {
            // A key that is not a string cannot appear in a path;
            // keep the field untouched
            result.insert(key.clone(), val.clone());
            continue;
        };
        let path = append_path_component(prefix, k.str());
        result.insert(key.clone(), deep_map_value(&path, val, mapper));
    }
    result.into()
}

fn deep_map_array<F, R>(prefix: &str, array: &Array<Variant>, mapper: &F) -> Array<Variant>
where
    F: Fn(&Option<SqlString>, &Variant) -> R,
    R: Into<Option<Variant>>,
{
    let mut result = Vec::with_capacity(array.len());
    for (index, val) in array.iter().enumerate() {
        // SQL array indexes start from 1
        let path = format!("{prefix}[{}]", index + 1);
        result.push(deep_map_value(&path, val, mapper));
    }
    result.into()
}

/// Like `variant_map_`, but recursive, and applied only to leaves: the
/// structure of nested objects and arrays is preserved exactly, and the
/// mapper transforms every non-container value, labeled by its dot-joined
/// path (array elements use 1-based bracket components, e.g. "a[1].b";
/// keys that are not identifiers are double-quoted).  JSON nulls are
/// leaves too.
#[doc(hidden)]
pub fn variant_deep_map_<F, R>(value: Variant, mapper: F) -> Option<Variant>
where
    F: Fn(&Option<SqlString>, &Variant) -> R,
    R: Into<Option<Variant>>,
{
    match value {
        Variant::Map(map) => Some(Variant::Map(deep_map_map("", &map, &mapper))),
        Variant::Array(array) => Some(Variant::Array(deep_map_array("", &array, &mapper))),
        leaf => mapper(&None, &leaf).into(),
    }
}

#[doc(hidden)]
pub fn variant_deep_mapN<F, R>(value: Option<Variant>, mapper: F) -> Option<Variant>
where
    F: Fn(&Option<SqlString>, &Variant) -> R,
    R: Into<Option<Variant>>,
{
    variant_deep_map_(value?, mapper)
}

/////////////// VARIANT_MERGE

/// Merge two variants recursively, following the JSON Merge Patch algorithm
/// (RFC 7386) with one difference: JSON null values are ordinary values and
/// never delete fields.  When both arguments hold maps, their fields are
/// merged, recursing on common keys; in every other case, including two
/// arrays, the second argument wins.
#[doc(hidden)]
pub fn variant_merge_V_V(left: Variant, right: Variant) -> Variant {
    match (left, right) {
        (left @ Variant::Map(_), Variant::Map(right)) if right.is_empty() => left,
        (Variant::Map(left), Variant::Map(right)) => {
            let mut result = (*left).clone();
            for (key, value) in right.iter() {
                let merged = match result.remove(key) {
                    Some(existing) => variant_merge_V_V(existing, value.clone()),
                    None => value.clone(),
                };
                result.insert(key.clone(), merged);
            }
            Variant::Map(result.into())
        }
        (_, right) => right,
    }
}

crate::some_polymorphic_function2!(variant_merge, V, Variant, V, Variant, Variant);

pub fn from_json_string<T>(json: &str) -> Option<T>
where
    T: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>,
{
    T::deserialize_with_context(
        &mut serde_json::Deserializer::from_str(json),
        &SqlSerdeConfig::default(),
    )
    .ok()
}

#[cfg(test)]
mod test {
    use crate::{Date, SqlString, Time, Timestamp, binary::ByteArray};
    use std::sync::Arc;

    use super::Variant;
    use chrono::{DateTime, NaiveDate, NaiveTime};
    use dbsp::{
        RootCircuit,
        algebra::{F32, F64},
    };
    use std::collections::BTreeMap;

    #[test]
    fn circuit_accepts_variant() {
        let (_circuit, (_input_handle, _output_handle)) = RootCircuit::build(move |circuit| {
            let (stream, input_handle) = circuit.add_input_zset::<Variant>();
            Ok((input_handle, stream.output()))
        })
        .unwrap();
    }

    #[test]
    fn circuit_accepts_arc_variant() {
        let (_circuit, (_input_handle, _output_handle)) = RootCircuit::build(move |circuit| {
            let (stream, input_handle) = circuit.add_input_zset::<Arc<Variant>>();
            Ok((input_handle, stream.output()))
        })
        .unwrap();
    }

    #[test]
    fn deserialize_ints() {
        assert_eq!(
            serde_json::from_str::<Variant>("5").unwrap(),
            Variant::UBigInt(5)
        );

        assert_eq!(
            serde_json::from_str::<Variant>("-5").unwrap(),
            Variant::BigInt(-5)
        );

        assert_eq!(
            serde_json::from_str::<Variant>("18446744073709551615").unwrap(),
            Variant::UBigInt(u64::MAX)
        );

        // u64::MAX * 10
        assert_eq!(
            serde_json::from_str::<Variant>("184467440737095516150").unwrap(),
            Variant::SqlDecimal((184467440737095516150i128, 0))
        );

        // -u64::MAX * 10
        assert_eq!(
            serde_json::from_str::<Variant>("-184467440737095516150").unwrap(),
            Variant::SqlDecimal((-184467440737095516150i128, 0))
        );
    }

    #[test]
    fn deserialize_fractional() {
        assert_eq!(
            serde_json::from_str::<Variant>("5.0").unwrap(),
            Variant::SqlDecimal((5, 0))
        );

        assert_eq!(
            serde_json::from_str::<Variant>("-5.0").unwrap(),
            Variant::SqlDecimal((-5, 0))
        );

        assert_eq!(
            serde_json::from_str::<Variant>("0.1").unwrap(),
            Variant::SqlDecimal((1, 1))
        );

        assert_eq!(
            serde_json::from_str::<Variant>("123E-5").unwrap(),
            Variant::SqlDecimal((123, 5))
        );

        assert_eq!(
            serde_json::from_str::<Variant>("10e10").unwrap(),
            Variant::SqlDecimal((100000000000, 0))
        );
    }

    #[test]
    fn deserialize_map() {
        let v = serde_json::from_str::<Variant>(
            r#"{
                "b": true,
                "i": 12345,
                "f": 123e-5,
                "d": 123.45,
                "s": "foo\nbar",
                "n": null,
                "nested": {
                    "arr": [1, "foo", null]
                }
            }"#,
        )
        .unwrap();

        let expected = Variant::Map(
            [
                (
                    Variant::String(SqlString::from_ref("b")),
                    Variant::Boolean(true),
                ),
                (
                    Variant::String(SqlString::from_ref("i")),
                    Variant::UBigInt(12345),
                ),
                (
                    Variant::String(SqlString::from_ref("f")),
                    Variant::SqlDecimal((123, 5)),
                ),
                (
                    Variant::String(SqlString::from_ref("d")),
                    Variant::SqlDecimal((12345, 2)),
                ),
                (
                    Variant::String(SqlString::from_ref("s")),
                    Variant::String(SqlString::from_ref("foo\nbar")),
                ),
                (
                    Variant::String(SqlString::from_ref("n")),
                    Variant::VariantNull,
                ),
                (
                    Variant::String(SqlString::from_ref("nested")),
                    Variant::Map(
                        [(
                            Variant::String(SqlString::from_ref("arr")),
                            Variant::Array(Arc::new(vec![
                                Variant::UBigInt(1),
                                Variant::String(SqlString::from_ref("foo")),
                                Variant::VariantNull,
                            ])),
                        )]
                        .into_iter()
                        .collect::<BTreeMap<Variant, Variant>>()
                        .into(),
                    ),
                ),
            ]
            .into_iter()
            .collect::<BTreeMap<Variant, Variant>>()
            .into(),
        );
        assert_eq!(v, expected);
    }

    #[test]
    fn serialize_fractional() {
        assert_eq!(
            "5",
            &serde_json::to_string(&Variant::SqlDecimal((5, 0))).unwrap()
        );

        assert_eq!(
            "123.45",
            &serde_json::to_string(&Variant::SqlDecimal((12345, 2))).unwrap()
        );

        assert_eq!(
            "1.23",
            &serde_json::to_string(&Variant::SqlDecimal((123, 2))).unwrap()
        );

        assert_eq!(
            "0.00001",
            &serde_json::to_string(&Variant::Real(F32::new(1E-5))).unwrap()
        );

        assert_eq!(
            "-1e-20",
            &serde_json::to_string(&Variant::Double(F64::new(-1E-20))).unwrap()
        );
    }

    #[test]
    fn serialize_map() {
        let v = Variant::Map(
            [
                (
                    Variant::String(SqlString::from_ref("b")),
                    Variant::Boolean(true),
                ),
                (
                    Variant::String(SqlString::from_ref("i")),
                    Variant::SqlDecimal((12345, 0)),
                ),
                (
                    Variant::String(SqlString::from_ref("f")),
                    Variant::Double(F64::new(0.00123)),
                ),
                (
                    Variant::String(SqlString::from_ref("d")),
                    Variant::SqlDecimal((12345, 2)),
                ),
                (
                    Variant::String(SqlString::from_ref("s")),
                    Variant::String(SqlString::from_ref("foo\nbar")),
                ),
                (
                    Variant::String(SqlString::from_ref("bytes")),
                    Variant::Binary(ByteArray::new(b"hello")),
                ),
                (
                    Variant::String(SqlString::from_ref("n")),
                    Variant::VariantNull,
                ),
                (
                    Variant::String(SqlString::from_ref("nested")),
                    Variant::Map(
                        [
                            (
                                Variant::String(SqlString::from_ref("arr")),
                                Variant::Array(
                                    vec![
                                        Variant::SqlDecimal((1, 0)),
                                        Variant::String(SqlString::from_ref("foo")),
                                        Variant::VariantNull,
                                    ]
                                    .into(),
                                ),
                            ),
                            (
                                Variant::String(SqlString::from_ref("ts")),
                                Variant::Timestamp(Timestamp::from_dateTime(
                                    DateTime::parse_from_rfc3339("2024-12-19T16:39:57Z")
                                        .unwrap()
                                        .to_utc(),
                                )),
                            ),
                            (
                                Variant::String(SqlString::from_ref("d")),
                                Variant::Date(Date::from_date(
                                    NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
                                )),
                            ),
                            (
                                Variant::String(SqlString::from_ref("t")),
                                Variant::Time(Time::from_time(
                                    NaiveTime::from_hms_opt(17, 30, 40).unwrap(),
                                )),
                            ),
                        ]
                        .into_iter()
                        .collect::<BTreeMap<Variant, Variant>>()
                        .into(),
                    ),
                ),
            ]
            .into_iter()
            .collect::<BTreeMap<Variant, Variant>>()
            .into(),
        );

        let s = serde_json::to_string(&v).unwrap();

        let expected = serde_json::from_str::<serde_json::Value>(
            r#"{
                "b": true,
                "i": 12345,
                "f": 0.00123,
                "d": 123.45,
                "s": "foo\nbar",
                "n": null,
                "bytes": [104, 101, 108, 108, 111],
                "nested": {
                    "arr": [1, "foo", null],
                    "ts": "2024-12-19 16:39:57",
                    "d": "2024-01-01",
                    "t": "17:30:40"
                }
            }"#,
        )
        .unwrap();

        assert_eq!(
            expected,
            serde_json::from_str::<serde_json::Value>(&s).unwrap()
        );
    }

    /// A heterogeneous JSON object exercising every extraction function.
    fn each_test_object() -> Variant {
        serde_json::from_str::<Variant>(
            r#"{
                "i": 1,
                "neg": -5,
                "big": 5000000000,
                "huge": 18446744073709551615,
                "dec": 2.5,
                "s": "text",
                "snum": "7",
                "b": true,
                "n": null,
                "arr": [1, 2],
                "obj": {"x": 1},
                "date": "2024-01-01",
                "time": "17:30:40",
                "ts": "2024-12-19 16:39:57"
            }"#,
        )
        .unwrap()
    }

    fn keys<T>(map: &crate::Map<SqlString, Option<T>>) -> Vec<&str> {
        map.keys().map(|k| k.str()).collect()
    }

    #[test]
    fn json_each_bigint_extracts_only_numeric_fields_in_range() {
        use super::json_each_bigint_V;

        let bigints = json_each_bigint_V(each_test_object());
        // "huge" exceeds the i64 range; "dec" is fractional;
        // "snum" is a string, never parsed; "n" is a null
        assert_eq!(keys(&bigints), vec!["big", "i", "neg"]);
        assert_eq!(
            bigints.get(&SqlString::from_ref("big")),
            Some(&Some(5000000000i64))
        );
        assert_eq!(bigints.get(&SqlString::from_ref("i")), Some(&Some(1)));
        assert_eq!(bigints.get(&SqlString::from_ref("neg")), Some(&Some(-5)));
    }

    #[test]
    fn json_each_string_keeps_only_string_fields() {
        use super::json_each_string_V;

        let strings = json_each_string_V(each_test_object());
        // Only fields holding strings; numbers and booleans are not stringified
        assert_eq!(keys(&strings), vec!["date", "s", "snum", "time", "ts"]);
        assert_eq!(
            strings.get(&SqlString::from_ref("s")),
            Some(&Some(SqlString::from_ref("text")))
        );
    }

    #[test]
    fn json_each_datetime_parses_strings() {
        use super::{
            json_each_boolean_V, json_each_date_V, json_each_time_V, json_each_timestamp_V,
        };
        use chrono::{NaiveDate, NaiveTime};

        let bools = json_each_boolean_V(each_test_object());
        assert_eq!(keys(&bools), vec!["b"]);

        // JSON has no date or time types, so strings that parse using the
        // grammar of the corresponding SQL literal qualify; each grammar
        // accepts only its own fields of the test object, and strings such
        // as "text" or "7" parse as none of them
        let dates = json_each_date_V(each_test_object());
        assert_eq!(keys(&dates), vec!["date"]);
        assert_eq!(
            dates.get(&SqlString::from_ref("date")),
            Some(&Some(Date::from_date(
                NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()
            )))
        );
        assert_eq!(keys(&json_each_time_V(each_test_object())), vec!["time"]);
        // a date-only string is also a valid midnight timestamp
        assert_eq!(
            keys(&json_each_timestamp_V(each_test_object())),
            vec!["date", "ts"]
        );

        // Genuinely typed values, as produced by CAST(x AS VARIANT), also
        // qualify
        let date = Date::from_date(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap());
        let time = Time::from_time(NaiveTime::from_hms_opt(17, 30, 40).unwrap());
        let typed = Variant::Map(
            [
                (
                    Variant::String(SqlString::from_ref("d")),
                    Variant::Date(date),
                ),
                (
                    Variant::String(SqlString::from_ref("t")),
                    Variant::Time(time),
                ),
            ]
            .into_iter()
            .collect::<BTreeMap<Variant, Variant>>()
            .into(),
        );
        let dates = json_each_date_V(typed.clone());
        assert_eq!(keys(&dates), vec!["d"]);
        assert_eq!(dates.get(&SqlString::from_ref("d")), Some(&Some(date)));
        let times = json_each_time_V(typed);
        assert_eq!(keys(&times), vec!["t"]);
    }

    #[test]
    fn json_each_non_map_returns_empty() {
        use super::{json_each_bigint_V, json_each_bigint_VN, json_each_string_V};

        assert!(json_each_bigint_V(Variant::BigInt(5)).is_empty());
        assert!(json_each_string_V(serde_json::from_str::<Variant>("[1, 2]").unwrap()).is_empty());
        assert!(json_each_bigint_V(Variant::VariantNull).is_empty());
        assert!(json_each_bigint_V(Variant::SqlNull).is_empty());
        // SQL NULL argument propagates to a NULL result
        assert_eq!(json_each_bigint_VN(None), None);
        assert!(
            json_each_bigint_VN(Some(Variant::Boolean(true)))
                .unwrap()
                .is_empty()
        );

        // Non-string keys are skipped
        let int_keys = Variant::Map(
            [(Variant::BigInt(1), Variant::BigInt(2))]
                .into_iter()
                .collect::<BTreeMap<Variant, Variant>>()
                .into(),
        );
        assert!(json_each_bigint_V(int_keys).is_empty());
    }

    fn strs(array: &crate::Array<SqlString>) -> Vec<&str> {
        array.iter().map(|s| s.str()).collect()
    }

    #[test]
    fn json_object_keys_returns_top_level_keys() {
        use super::{json_object_keys_V, json_object_keys_VN};

        // All top-level keys sorted, including those holding nulls,
        // arrays and nested objects
        let keys = json_object_keys_V(each_test_object());
        assert_eq!(
            strs(&keys),
            vec![
                "arr", "b", "big", "date", "dec", "huge", "i", "n", "neg", "obj", "s", "snum",
                "time", "ts"
            ]
        );

        assert!(json_object_keys_V(Variant::BigInt(5)).is_empty());
        assert!(json_object_keys_V(serde_json::from_str::<Variant>("[1, 2]").unwrap()).is_empty());
        assert_eq!(json_object_keys_VN(None), None);
    }

    #[test]
    fn variant_filter_by_predicate() {
        use super::variant_filter_;

        // Keep only string-valued fields
        let filtered = variant_filter_(each_test_object(), |_k, v| matches!(v, Variant::String(_)));
        let Some(Variant::Map(map)) = &filtered else {
            panic!("expected a map")
        };
        let keys: Vec<&str> = map
            .keys()
            .map(|k| match k {
                Variant::String(s) => s.str(),
                _ => panic!("expected string keys"),
            })
            .collect();
        assert_eq!(keys, vec!["date", "s", "snum", "time", "ts"]);

        // Strip fields holding JSON nulls; "n" disappears
        let stripped = variant_filter_(each_test_object(), |_k, v| {
            !matches!(v, Variant::VariantNull)
        });
        let Some(Variant::Map(map)) = &stripped else {
            panic!("expected a map")
        };
        assert!(!map.contains_key(&Variant::String(SqlString::from_ref("n"))) && map.len() == 13);

        // A non-map variant is a single item with a None label
        let kept = variant_filter_(Variant::BigInt(5), |k, _v| k.is_none());
        assert_eq!(kept, Some(Variant::BigInt(5)));
        let dropped = variant_filter_(Variant::BigInt(5), |_k, v| matches!(v, Variant::String(_)));
        assert_eq!(dropped, None);

        // Arrays are kept whole or dropped, not filtered element-wise
        let arr = serde_json::from_str::<Variant>("[1, 2]").unwrap();
        assert_eq!(
            variant_filter_(arr.clone(), |k, _v| k.is_none()),
            Some(arr.clone())
        );
        assert_eq!(variant_filter_(arr, |k, _v| k.is_some()), None);

        // A NULL predicate result drops, like a SQL WHERE clause
        let dropped = variant_filter_(Variant::BigInt(5), |_k, _v| None::<bool>);
        assert_eq!(dropped, None);
    }

    #[test]
    fn variant_deep_filter_recurses_with_paths() {
        use super::variant_deep_filter_;

        fn nested() -> Variant {
            serde_json::from_str::<Variant>(
                r#"{"a": {"b": 1, "c": {"d": 2}}, "e": [{"f": 3}, 4], "g": 5}"#,
            )
            .unwrap()
        }
        fn path_str(k: &Option<SqlString>) -> String {
            match k {
                Some(s) => s.str().to_string(),
                None => String::new(),
            }
        }
        fn to_json(v: &Option<Variant>) -> String {
            v.as_ref().unwrap().to_json_string().unwrap()
        }

        // Dropping an inner path removes only that subtree
        let result = variant_deep_filter_(nested(), |k, _v| path_str(k) != "a.c");
        assert_eq!(to_json(&result), r#"{"a":{"b":1},"e":[{"f":3},4],"g":5}"#);

        // Array elements have 1-based bracket path components and recurse
        let result = variant_deep_filter_(nested(), |k, _v| path_str(k) != "e[1].f");
        assert_eq!(
            to_json(&result),
            r#"{"a":{"b":1,"c":{"d":2}},"e":[{},4],"g":5}"#
        );

        // Dropping an element shrinks the array
        let result = variant_deep_filter_(nested(), |k, _v| path_str(k) != "e[1]");
        assert_eq!(
            to_json(&result),
            r#"{"a":{"b":1,"c":{"d":2}},"e":[4],"g":5}"#
        );

        // A top-level array is filtered element-wise
        let array = serde_json::from_str::<Variant>(r#"[1, {"x": 2}, 3]"#).unwrap();
        let result = variant_deep_filter_(array, |k, _v| path_str(k) != "[2].x");
        assert_eq!(to_json(&result), r#"[1,{},3]"#);

        // A scalar is a single item with no label
        assert_eq!(
            variant_deep_filter_(Variant::BigInt(5), |k, _v| k.is_none()),
            Some(Variant::BigInt(5))
        );
        assert_eq!(
            variant_deep_filter_(Variant::BigInt(5), |k, _v| k.is_some()),
            None
        );
    }

    #[test]
    fn deep_path_components_are_quoted() {
        use super::{variant_deep_filter_, variant_deep_map_};

        // A key that is not an identifier is double-quoted in paths, so a
        // key containing a dot cannot be confused with nesting
        let v =
            serde_json::from_str::<Variant>(r#"{"example.com": {"a": 1}, "example": {"b": 2}}"#)
                .unwrap();
        let kept = variant_deep_filter_(v, |p, _v| {
            let path = p.as_ref().map(|s| s.str()).unwrap_or("");
            path == "example" || !path.starts_with("example.")
        });
        assert_eq!(
            kept.unwrap().to_json_string().unwrap(),
            r#"{"example":{},"example.com":{"a":1}}"#
        );

        // The mapper sees the quoted path, with embedded quotes escaped
        let v = serde_json::from_str::<Variant>(r#"{"a\"b": 1, "_ok9": 2}"#).unwrap();
        let mapped = variant_deep_map_(v, |p, _v| p.as_ref().map(|s| Variant::String(s.clone())));
        assert_eq!(
            mapped.unwrap().to_json_string().unwrap(),
            r#"{"_ok9":"_ok9","a\"b":"\"a\\\"b\""}"#
        );
    }

    #[test]
    fn variant_merge_merges_recursively() {
        use super::{variant_merge_V_V, variant_merge_VN_V};

        fn parse(s: &str) -> Variant {
            serde_json::from_str::<Variant>(s).unwrap()
        }
        fn merged_json(left: &str, right: &str) -> String {
            variant_merge_V_V(parse(left), parse(right))
                .to_json_string()
                .unwrap()
        }

        // Objects merge recursively; fields of the second win on common keys
        assert_eq!(
            merged_json(
                r#"{"a": {"x": 1, "y": 2}, "b": 1}"#,
                r#"{"a": {"x": 9, "z": 3}, "c": 4}"#
            ),
            r#"{"a":{"x":9,"y":2,"z":3},"b":1,"c":4}"#
        );

        // Arrays are replaced, not concatenated
        assert_eq!(
            merged_json(r#"{"a": [1, 2]}"#, r#"{"a": [3]}"#),
            r#"{"a":[3]}"#
        );

        // A JSON null is an ordinary value; it does not delete the field
        assert_eq!(
            merged_json(r#"{"a": 1, "b": 2}"#, r#"{"a": null}"#),
            r#"{"a":null,"b":2}"#
        );

        // When either argument is not an object, the second wins
        assert_eq!(merged_json("5", "6"), "6");
        assert_eq!(merged_json(r#"{"a": 1}"#, "[1]"), "[1]");
        assert_eq!(merged_json("[1]", r#"{"a": 1}"#), r#"{"a":1}"#);

        // A SQL NULL argument propagates
        assert_eq!(variant_merge_VN_V(None, parse(r#"{"a": 1}"#)), None);
    }

    #[test]
    fn variant_map_builds_isomorphic_object() {
        use super::variant_map_;

        // Replace every value by its key
        let mapped = variant_map_(each_test_object(), |k, _v| k.clone());
        let Some(Variant::Map(map)) = &mapped else {
            panic!("expected a map")
        };
        assert_eq!(map.len(), 14);
        assert_eq!(
            map.get(&Variant::String(SqlString::from_ref("i"))),
            Some(&Variant::String(SqlString::from_ref("i")))
        );

        // A SQL NULL mapper result becomes a SqlNull variant inside the map
        let nulled = variant_map_(each_test_object(), |_k, _v| None::<Variant>);
        let Some(Variant::Map(map)) = &nulled else {
            panic!("expected a map")
        };
        assert!(map.values().all(|v| matches!(v, Variant::SqlNull)));

        // A non-map variant is a single item with a None label
        let mapped = variant_map_(Variant::BigInt(5), |k, v| {
            assert!(k.is_none());
            match v {
                Variant::BigInt(x) => Some(Variant::BigInt(x + 1)),
                _ => None,
            }
        });
        assert_eq!(mapped, Some(Variant::BigInt(6)));
        // A NULL mapper result for a non-map variant is a SQL NULL
        assert_eq!(
            variant_map_(Variant::BigInt(5), |_k, _v| None::<Variant>),
            None
        );
    }

    #[test]
    fn variant_deep_map_transforms_leaves() {
        use super::variant_deep_map_;

        let nested =
            serde_json::from_str::<Variant>(r#"{"a": {"b": 1}, "e": [1, "x"], "n": null}"#)
                .unwrap();

        // Replace every leaf by its path: structure is preserved exactly,
        // and JSON nulls are leaves too
        let mapped = variant_deep_map_(nested.clone(), |p, _v| {
            p.as_ref().map(|s| Variant::String(s.clone()))
        });
        assert_eq!(
            mapped.unwrap().to_json_string().unwrap(),
            r#"{"a":{"b":"a.b"},"e":["e[1]","e[2]"],"n":"n"}"#
        );

        // A SQL NULL mapper result becomes a JSON null in place
        let nulled = variant_deep_map_(nested, |_p, _v| None::<Variant>);
        assert_eq!(
            nulled.unwrap().to_json_string().unwrap(),
            r#"{"a":{"b":null},"e":[null,null],"n":null}"#
        );

        // A top-level scalar is a single leaf with no label
        let mapped = variant_deep_map_(Variant::BigInt(5), |p, v| {
            assert!(p.is_none());
            match v {
                Variant::BigInt(x) => Some(Variant::BigInt(x + 1)),
                _ => None,
            }
        });
        assert_eq!(mapped, Some(Variant::BigInt(6)));
    }

    #[test]
    fn json_keys_returns_nested_paths() {
        use super::json_keys_V;

        // Every key at every level; arrays are not traversed ("e.f" is absent)
        let v = serde_json::from_str::<Variant>(
            r#"{"a": {"b": 1, "c": {"d": 2}}, "e": [{"f": 3}], "g": 4}"#,
        )
        .unwrap();
        assert_eq!(
            strs(&json_keys_V(v)),
            vec!["a", "a.b", "a.c", "a.c.d", "e", "g"]
        );

        // A key containing a dot is escaped using double quotes, like in
        // BigQuery, so it cannot collide with a nested path
        let v = serde_json::from_str::<Variant>(r#"{"a.b": 1, "a": {"b": 2}}"#).unwrap();
        assert_eq!(strs(&json_keys_V(v)), vec!["\"a.b\"", "a", "a.b"]);

        assert!(json_keys_V(Variant::BigInt(5)).is_empty());
    }
}
