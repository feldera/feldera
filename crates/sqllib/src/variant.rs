//! Variant is a dynamically-typed object that can represent
//! the values in a SQL program.

use crate::{
    array::Array, binary::ByteArray, casts::*, error::*, map::Map, tn, ttn, Date, GeoPoint,
    LongInterval, ShortInterval, SqlDecimal, SqlString, Time, Timestamp, Uuid,
};
use dbsp::algebra::{F32, F64};
use feldera_fxp::DynamicDecimal;
use feldera_types::serde_with_context::serde_config::VariantFormat;
use feldera_types::serde_with_context::{
    DeserializeWithContext, SerializeWithContext, SqlSerdeConfig,
};
use serde::de::{self, DeserializeSeed, Error as _, MapAccess, SeqAccess, Visitor};
use serde::ser::{self, Error as _};
use serde::{Deserialize, Serialize};
use size_of::SizeOf;
use std::borrow::Cow;
use std::cmp::Ord;
use std::collections::BTreeMap;
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
    Geometry(GeoPoint),
    Uuid(Uuid),
    #[size_of(skip, skip_bounds)]
    Array(#[omit_bounds] Array<Variant>),
    #[size_of(skip, skip_bounds)]
    Map(#[omit_bounds] Map<Variant, Variant>),
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
            VariantFormat::JsonString => {
                let s = Cow::<String>::deserialize(deserializer)?;
                serde_json::from_str::<Variant>(&s).map_err(|e| {
                    D::Error::custom(format!(
                        "error deserializing VARIANT type from a JSON string: {e}"
                    ))
                })
            }
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

macro_rules! into {
    ($variant:ident, $type:ty) => {
        #[doc(hidden)]
        impl TryFrom<Variant> for $type {
            type Error = Box<dyn Error>;

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
            type Error = Box<dyn Error>;

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

into!(Boolean, bool);
into!(String, SqlString);
into!(Date, Date);
into!(Time, Time);
into!(Timestamp, Timestamp);
into!(ShortInterval, ShortInterval);
into!(LongInterval, LongInterval);
into!(Geometry, GeoPoint);
into!(Binary, ByteArray);
into!(Uuid, Uuid);

macro_rules! into_numeric {
    ($type:ty, $type_name: ident) => {
        #[doc(hidden)]
        impl TryFrom<Variant> for $type {
            type Error = Box<SqlRuntimeError>;

            ::paste::paste! {
                #[doc(hidden)]
                fn try_from(value: Variant) -> Result<Self, Self::Error> {
                    match value {
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
    use crate::{binary::ByteArray, Date, SqlString, Time, Timestamp};
    use std::sync::Arc;

    use super::Variant;
    use chrono::{DateTime, NaiveDate, NaiveTime};
    use dbsp::{
        algebra::{F32, F64},
        RootCircuit,
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
}
