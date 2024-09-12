//! Variant is a dynamically-typed object that can represent
//! the values in a SQL program.

use crate::{casts::*, Date, GeoPoint, LongInterval, ShortInterval, Time, Timestamp};
use dbsp::algebra::{F32, F64};
use feldera_types::serde_with_context::{SerializeWithContext, SqlSerdeConfig};
use num::FromPrimitive;
use rkyv::collections::ArchivedBTreeMap;
use rkyv::string::ArchivedString;
use rkyv::Fallible;
use rust_decimal::Decimal;
use serde::de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Serialize};
use size_of::{Context, SizeOf};
use std::error::Error;
use std::fmt;
use std::str::FromStr;
use std::{collections::BTreeMap, fmt::Debug, hash::Hash};
use thiserror::Error;

pub trait StructVariant: Send + Sync + 'static {
    fn get(&self, key: &str) -> Option<Variant>;
    fn iter(&self) -> Box<dyn Iterator<Item = (&str, &Variant)> + '_>;
    fn values(&self) -> Box<dyn Iterator<Item = &Variant> + '_>;
    fn clone_box(&self) -> Box<dyn StructVariant>;
}

impl Eq for dyn StructVariant {}

impl PartialEq for dyn StructVariant {
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}

impl Clone for Box<dyn StructVariant> {
    fn clone(&self) -> Box<dyn StructVariant> {
        self.clone_box()
    }
}

impl Ord for dyn StructVariant {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.iter().cmp(other.iter())
    }
}

impl PartialOrd for dyn StructVariant {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Hash for dyn StructVariant {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        for (k, v) in self.iter() {
            k.hash(state);
            v.hash(state);
        }
    }
}

impl Serialize for dyn StructVariant {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        for (k, v) in self.iter() {
            map.serialize_entry(k, v)?;
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for Box<dyn StructVariant + 'static> {
    fn deserialize<D: serde::Deserializer<'de>>(_deserializer: D) -> Result<Self, D::Error> {
        todo!()
    }
}

impl Debug for dyn StructVariant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

impl SizeOf for dyn StructVariant {
    fn size_of_children(&self, _context: &mut Context) {
        todo!("don't understand how to use this crate")
    }
}

impl rkyv::Archive for Box<dyn StructVariant> {
    type Archived = ArchivedBTreeMap<ArchivedString, ArchivedVariant>;
    type Resolver = ();

    unsafe fn resolve(&self, _pos: usize, _resolver: Self::Resolver, _out: *mut Self::Archived) {
        todo!()
    }
}

impl<D: Fallible> rkyv::Deserialize<Box<dyn StructVariant>, D> for Box<dyn StructVariant> {
    fn deserialize(&self, _deserializer: &mut D) -> Result<Box<dyn StructVariant>, D::Error> {
        unimplemented!();
    }
}

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
#[archive(bound(serialize = "__S: rkyv::ser::ScratchSpace + rkyv::ser::Serializer"))]
#[archive_attr(derive(Eq, Ord, PartialEq, PartialOrd))]
pub enum Variant {
    #[default]
    SqlNull,
    VariantNull,
    Boolean(bool),
    TinyInt(i8),
    SmallInt(i16),
    Int(i32),
    BigInt(i64),
    Real(F32),
    Double(F64),
    Decimal(Decimal),
    String(String),
    Date(Date),
    Time(Time),
    Timestamp(Timestamp),
    ShortInterval(ShortInterval),
    LongInterval(LongInterval),
    Geometry(GeoPoint),
    #[size_of(skip, skip_bounds)]
    Array(#[omit_bounds] Vec<Variant>),
    #[size_of(skip, skip_bounds)]
    Map(#[omit_bounds] BTreeMap<Variant, Variant>),
    //Struct(Option<Box<dyn StructVariant>>),
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
            fn visit_i64<E>(self, value: i64) -> Result<Variant, E> {
                Ok(Variant::Decimal(Decimal::from(value)))
            }

            #[inline]
            fn visit_u64<E>(self, value: u64) -> Result<Variant, E> {
                Ok(Variant::Decimal(Decimal::from(value)))
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
                Ok(Variant::String(value))
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

                Ok(Variant::Array(vec))
            }

            #[inline]
            fn visit_map<V>(self, mut visitor: V) -> Result<Variant, V::Error>
            where
                V: MapAccess<'de>,
            {
                match visitor.next_key_seed(KeyClassifier)? {
                    Some(KeyClass::Number) => {
                        let number: NumberFromString = visitor.next_value()?;
                        Ok(Variant::Decimal(number.value))
                    }
                    Some(KeyClass::Map(first_key)) => {
                        let mut values = BTreeMap::new();

                        values.insert(Variant::String(first_key), visitor.next_value()?);
                        while let Some((key, value)) = visitor.next_entry::<String, Variant>()? {
                            values.insert(Variant::String(key), value);
                        }

                        Ok(Variant::Map(values))
                    }
                    None => Ok(Variant::Map(BTreeMap::new())),
                }
            }
        }

        deserializer.deserialize_any(VariantVisitor)
    }
}

struct KeyClassifier;

enum KeyClass {
    Map(String),
    Number,
}

impl<'de> DeserializeSeed<'de> for KeyClassifier {
    type Value = KeyClass;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(self)
    }
}

// This is defined in serde_json, but not exported.
const DECIMAL_KEY_TOKEN: &str = "$serde_json::private::Number";

impl<'de> Visitor<'de> for KeyClassifier {
    type Value = KeyClass;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a string key")
    }

    fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        match s {
            DECIMAL_KEY_TOKEN => Ok(KeyClass::Number),
            _ => Ok(KeyClass::Map(s.to_owned())),
        }
    }

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

pub struct NumberFromString {
    pub value: Decimal,
}

impl<'de> de::Deserialize<'de> for NumberFromString {
    fn deserialize<D>(deserializer: D) -> Result<NumberFromString, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        struct Visitor;

        impl<'de> de::Visitor<'de> for Visitor {
            type Value = NumberFromString;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("string containing a decimal number")
            }

            fn visit_str<E>(self, s: &str) -> Result<NumberFromString, E>
            where
                E: de::Error,
            {
                let d = Decimal::from_str(s)
                    .or_else(|_| Decimal::from_scientific(s))
                    .map_err(serde::de::Error::custom)?;
                Ok(NumberFromString { value: d })
            }
        }

        deserializer.deserialize_str(Visitor)
    }
}

impl Serialize for Variant {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.serialize_with_context(serializer, &SqlSerdeConfig::default())
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
    {
        match self {
            Variant::SqlNull | Variant::VariantNull => serializer.serialize_none(),
            Variant::Boolean(v) => v.serialize_with_context(serializer, context),
            Variant::TinyInt(v) => v.serialize_with_context(serializer, context),
            Variant::SmallInt(v) => v.serialize_with_context(serializer, context),
            Variant::Int(v) => v.serialize_with_context(serializer, context),
            Variant::BigInt(v) => v.serialize_with_context(serializer, context),
            Variant::Real(v) => v.serialize_with_context(serializer, context),
            Variant::Double(v) => v.serialize_with_context(serializer, context),
            Variant::Decimal(v) => v.serialize_with_context(serializer, context),
            Variant::String(v) => v.serialize_with_context(serializer, context),
            Variant::Date(v) => v.serialize_with_context(serializer, context),
            Variant::Time(v) => v.serialize_with_context(serializer, context),
            Variant::Timestamp(v) => v.serialize_with_context(serializer, context),
            Variant::ShortInterval(v) => v.serialize_with_context(serializer, context),
            Variant::LongInterval(v) => v.serialize_with_context(serializer, context),
            Variant::Geometry(v) => v.serialize_with_context(serializer, context),
            Variant::Array(a) => a.serialize_with_context(serializer, context),
            Variant::Map(m) => m.serialize_with_context(serializer, context),
        }
    }
}

impl Variant {
    fn get_type_string(&self) -> &'static str {
        match self {
            Variant::SqlNull => "NULL",
            Variant::VariantNull => "VARIANT",
            Variant::Boolean(_) => "BOOLEAN",
            Variant::TinyInt(_) => "TINYINT",
            Variant::SmallInt(_) => "SMALLINT",
            Variant::Int(_) => "INTEGER",
            Variant::BigInt(_) => "BIGINT",
            Variant::Real(_) => "REAL",
            Variant::Double(_) => "DOUBLE",
            Variant::Decimal(_) => "DECIMAL",
            Variant::String(_) => "VARCHAR",
            Variant::Date(_) => "DATE",
            Variant::Time(_) => "TIME",
            Variant::Timestamp(_) => "TIMESTAMP",
            Variant::ShortInterval(_) => "SHORTINTERVAL",
            Variant::LongInterval(_) => "LONGINTERVAL",
            Variant::Geometry(_) => "GEOPOINT",
            Variant::Array(_) => "ARRAY",
            Variant::Map(_) => "MAP",
        }
    }

    pub fn index_string<I: AsRef<str>>(&self, index: I) -> Variant {
        match self {
            Variant::Map(value) => match value.get(&Variant::String(index.as_ref().to_string())) {
                None => Variant::SqlNull,
                Some(result) => result.clone(),
            },
            _ => Variant::SqlNull,
        }
    }

    pub fn index(&self, index: Variant) -> Option<Variant> {
        match self {
            Variant::Array(value) => {
                let index = match index {
                    Variant::TinyInt(index) => index as isize,
                    Variant::SmallInt(index) => index as isize,
                    Variant::Int(index) => index as isize,
                    Variant::BigInt(index) => index as isize,
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

    pub fn to_json_string(&self) -> Result<String, Box<dyn std::error::Error>> {
        Ok(serde_json::to_string(self)?)
    }
}

// A macro for From<T> for Variant
macro_rules! from {
    ($variant:ident, $type:ty) => {
        impl From<$type> for Variant {
            fn from(value: $type) -> Self {
                Variant::$variant(value)
            }
        }
    };
}

from!(Boolean, bool);
from!(TinyInt, i8);
from!(SmallInt, i16);
from!(Int, i32);
from!(BigInt, i64);
from!(Real, F32);
from!(Double, F64);
from!(Decimal, Decimal);
from!(String, String);
from!(Date, Date);
from!(Time, Time);
from!(Timestamp, Timestamp);
from!(ShortInterval, ShortInterval);
from!(LongInterval, LongInterval);
from!(Geometry, GeoPoint);
//from!(Struct, Box<dyn StructVariant>);

impl<T> From<Vec<T>> for Variant
where
    Variant: From<T>,
{
    fn from(vec: Vec<T>) -> Self {
        Variant::Array(vec.into_iter().map(Variant::from).collect())
    }
}

impl<K, V> From<BTreeMap<K, V>> for Variant
where
    Variant: From<K> + From<V>,
    K: Clone,
    V: Clone,
{
    fn from(map: BTreeMap<K, V>) -> Self {
        let mut result = BTreeMap::new();
        for (key, value) in map.iter() {
            result.insert(key.clone().into(), value.clone().into());
        }
        Variant::Map(result)
    }
}

//////////////////// Reverse conversions Variant -> T

#[derive(Error, Debug)]
pub enum MyError {
    #[error("{0}")]
    CustomError(String),
}

impl MyError {
    pub fn from_string(message: String) -> Box<Self> {
        Box::new(MyError::CustomError(message))
    }

    pub fn from_strng(message: &str) -> Box<Self> {
        Box::new(MyError::CustomError(message.to_string()))
    }
}

macro_rules! into {
    ($variant:ident, $type:ty) => {
        impl TryFrom<Variant> for $type {
            type Error = Box<dyn Error>;

            fn try_from(value: Variant) -> Result<Self, Self::Error> {
                match value {
                    Variant::$variant(x) => Ok(x),
                    _ => Err(MyError::from_strng(concat!(
                        "Not an ",
                        stringify!($variant)
                    ))),
                }
            }
        }

        impl TryFrom<Variant> for Option<$type> {
            type Error = Box<dyn Error>;

            fn try_from(value: Variant) -> Result<Self, Self::Error> {
                match value {
                    Variant::SqlNull => Ok(None),
                    Variant::VariantNull => Ok(None),
                    _ => match <$type>::try_from(value) {
                        Ok(result) => Ok(Some(result)),
                        Err(e) => Err(e),
                    },
                }
            }
        }
    };
}

into!(Boolean, bool);
into!(String, String);
into!(Date, Date);
into!(Time, Time);
into!(Timestamp, Timestamp);
into!(ShortInterval, ShortInterval);
into!(LongInterval, LongInterval);
into!(Geometry, GeoPoint);

macro_rules! into_numeric {
    ($type:ty, $type_name: ident) => {
        impl TryFrom<Variant> for $type {
            type Error = Box<dyn Error>;

            ::paste::paste! {
                fn try_from(value: Variant) -> Result<Self, Self::Error> {
                    match value {
                        Variant::TinyInt(x) => Ok([< cast_to_ $type_name _i8>](x)),
                        Variant::SmallInt(x) => Ok([< cast_to_ $type_name _i16>](x)),
                        Variant::Int(x) => Ok([< cast_to_ $type_name _i32 >](x)),
                        Variant::BigInt(x) => Ok([< cast_to_ $type_name _i64 >](x)),
                        Variant::Real(x) => Ok([< cast_to_ $type_name _f >](x)),
                        Variant::Double(x) => Ok([< cast_to_ $type_name _d >](x)) ,
                        Variant::Decimal(x) => Ok([< cast_to_ $type_name _decimal >](x)),
                        _ => Err(MyError::from_strng(concat!("Not an ", stringify!($variant)))),
                    }
                }
            }
        }

        impl TryFrom<Variant> for Option<$type> {
            type Error = Box<dyn Error>;

            ::paste::paste! {
                fn try_from(value: Variant) -> Result<Self, Self::Error> {
                    match value {
                        Variant::VariantNull => Ok(None),
                        Variant::SqlNull => Ok(None),
                        _ => match <$type>::try_from(value) {
                            Ok(result) => Ok(Some(result)),
                            Err(e) => Err(e),
                        }
                    }
                }
            }
        }
    };
}

into_numeric!(i8, i8);
into_numeric!(i16, i16);
into_numeric!(i32, i32);
into_numeric!(i64, i64);
into_numeric!(F32, f);
into_numeric!(F64, d);

impl TryFrom<Variant> for Decimal {
    type Error = Box<dyn Error>;

    fn try_from(value: Variant) -> Result<Self, Self::Error> {
        match value {
            Variant::TinyInt(x) => Decimal::from_i8(x).ok_or(MyError::from_strng("Out of range")),
            Variant::SmallInt(x) => Decimal::from_i16(x).ok_or(MyError::from_strng("Out of range")),
            Variant::Int(x) => Decimal::from_i32(x).ok_or(MyError::from_strng("Out of range")),
            Variant::BigInt(x) => Decimal::from_i64(x).ok_or(MyError::from_strng("Out of range")),
            Variant::Real(x) => {
                Decimal::from_f32(x.into_inner()).ok_or(MyError::from_strng("Out of range"))
            }
            Variant::Double(x) => {
                Decimal::from_f64(x.into_inner()).ok_or(MyError::from_strng("Out of range"))
            }
            Variant::Decimal(x) => Ok(x),
            _ => Err(MyError::from_strng(concat!(
                "Not an ",
                stringify!($variant)
            ))),
        }
    }
}

impl TryFrom<Variant> for Option<Decimal> {
    type Error = Box<dyn Error>;

    fn try_from(value: Variant) -> Result<Self, Self::Error> {
        match value {
            Variant::VariantNull => Ok(None),
            Variant::SqlNull => Ok(None),
            _ => match Decimal::try_from(value) {
                Ok(result) => Ok(Some(result)),
                Err(e) => Err(e),
            },
        }
    }
}

impl<T> TryFrom<Variant> for Vec<T>
where
    T: TryFrom<Variant, Error = Box<dyn Error>>,
{
    type Error = Box<dyn Error>;

    fn try_from(value: Variant) -> Result<Self, Self::Error> {
        match value {
            Variant::Array(a) => {
                let mut result = vec![];
                for e in a {
                    let converted = T::try_from(e)?;
                    result.push(converted);
                }
                Ok(result)
            }
            _ => Err(Box::new(MyError::CustomError("Not an array".to_string()))),
        }
    }
}

impl<T> TryFrom<Variant> for Option<Vec<T>>
where
    T: TryFrom<Variant, Error = Box<dyn Error>>,
{
    type Error = Box<dyn Error>;

    fn try_from(value: Variant) -> Result<Self, Self::Error> {
        match value {
            Variant::VariantNull => Ok(None),
            Variant::SqlNull => Ok(None),
            _ => match Vec::<T>::try_from(value) {
                Ok(result) => Ok(Some(result)),
                Err(e) => Err(e),
            },
        }
    }
}

impl<K, V> TryFrom<Variant> for BTreeMap<K, V>
where
    K: TryFrom<Variant, Error = Box<dyn Error>> + Ord,
    V: TryFrom<Variant, Error = Box<dyn Error>>,
{
    type Error = Box<dyn Error>;

    fn try_from(value: Variant) -> Result<Self, Self::Error> {
        match value {
            Variant::Map(map) => {
                let mut result = BTreeMap::<K, V>::new();
                for (key, value) in map.into_iter() {
                    let convertedKey = K::try_from(key)?;
                    let convertedValue = V::try_from(value)?;
                    result.insert(convertedKey, convertedValue);
                }
                Ok(result)
            }
            _ => Err(Box::new(MyError::CustomError("Not a map".to_string()))),
        }
    }
}

impl<K, V> TryFrom<Variant> for Option<BTreeMap<K, V>>
where
    K: TryFrom<Variant, Error = Box<dyn Error>> + Ord,
    V: TryFrom<Variant, Error = Box<dyn Error>>,
{
    type Error = Box<dyn Error>;

    fn try_from(value: Variant) -> Result<Self, Self::Error> {
        match value {
            Variant::VariantNull => Ok(None),
            Variant::SqlNull => Ok(None),
            _ => match BTreeMap::<K, V>::try_from(value) {
                Ok(result) => Ok(Some(result)),
                Err(e) => Err(e),
            },
        }
    }
}

pub fn typeof_(value: Variant) -> String {
    value.get_type_string().to_string()
}

pub fn typeofN(value: Option<Variant>) -> String {
    match value {
        None => "NULL".to_string(),
        Some(value) => value.get_type_string().to_string(),
    }
}

pub fn variantnull() -> Variant {
    Variant::VariantNull
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use crate::{Date, Time, Timestamp};

    use super::Variant;
    use chrono::{DateTime, NaiveDate, NaiveTime};
    use dbsp::{
        algebra::{F32, F64},
        RootCircuit,
    };
    use num::FromPrimitive;
    use rust_decimal::Decimal;

    #[test]
    fn circuit_accepts_variant() {
        let (_circuit, _input_handle) = RootCircuit::build(move |circuit| {
            let (_stream, input_handle) = circuit.add_input_zset::<Variant>();
            Ok(input_handle)
        })
        .unwrap();
    }

    #[test]
    fn deserialize_ints() {
        assert_eq!(
            serde_json::from_str::<Variant>("5").unwrap(),
            Variant::Decimal(Decimal::from(5))
        );

        assert_eq!(
            serde_json::from_str::<Variant>("-5").unwrap(),
            Variant::Decimal(Decimal::from(-5))
        );

        assert_eq!(
            serde_json::from_str::<Variant>("18446744073709551615").unwrap(),
            Variant::Decimal(Decimal::from(u64::MAX))
        );

        // u64::MAX * 10
        assert_eq!(
            serde_json::from_str::<Variant>("184467440737095516150").unwrap(),
            Variant::Decimal(Decimal::from_str_exact("184467440737095516150").unwrap())
        );

        // -u64::MAX * 10
        assert_eq!(
            serde_json::from_str::<Variant>("-184467440737095516150").unwrap(),
            Variant::Decimal(Decimal::from_str_exact("-184467440737095516150").unwrap())
        );
    }

    #[test]
    fn deserialize_fractional() {
        assert_eq!(
            serde_json::from_str::<Variant>("5.0").unwrap(),
            Variant::Decimal(Decimal::from_f32(5.0).unwrap())
        );

        assert_eq!(
            serde_json::from_str::<Variant>("-5.0").unwrap(),
            Variant::Decimal(Decimal::from_f32(-5.0).unwrap())
        );

        assert_eq!(
            serde_json::from_str::<Variant>("0.1").unwrap(),
            Variant::Decimal(Decimal::from_str("0.1").unwrap())
        );

        assert_eq!(
            serde_json::from_str::<Variant>("123E-5").unwrap(),
            Variant::Decimal(Decimal::from_str("0.00123").unwrap())
        );

        assert_eq!(
            serde_json::from_str::<Variant>("10e10").unwrap(),
            Variant::Decimal(Decimal::from_str("100_000_000_000").unwrap())
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
                (Variant::String("b".to_string()), Variant::Boolean(true)),
                (
                    Variant::String("i".to_string()),
                    Variant::Decimal(Decimal::from(12345)),
                ),
                (
                    Variant::String("f".to_string()),
                    Variant::Decimal(Decimal::from_str("0.00123").unwrap()),
                ),
                (
                    Variant::String("d".to_string()),
                    Variant::Decimal(Decimal::from_str("123.45").unwrap()),
                ),
                (
                    Variant::String("s".to_string()),
                    Variant::String("foo\nbar".to_string()),
                ),
                (Variant::String("n".to_string()), Variant::VariantNull),
                (
                    Variant::String("nested".to_string()),
                    Variant::Map(
                        [(
                            Variant::String("arr".to_string()),
                            Variant::Array(vec![
                                Variant::Decimal(Decimal::from(1)),
                                Variant::String("foo".to_string()),
                                Variant::VariantNull,
                            ]),
                        )]
                        .into_iter()
                        .collect(),
                    ),
                ),
            ]
            .into_iter()
            .collect(),
        );
        assert_eq!(v, expected);
    }

    #[test]
    fn serialize_fractional() {
        assert_eq!(
            "5",
            &serde_json::to_string(&Variant::Decimal(Decimal::from(5))).unwrap()
        );

        assert_eq!(
            "123.45",
            &serde_json::to_string(&Variant::Decimal(Decimal::from_str("123.45").unwrap()))
                .unwrap()
        );

        assert_eq!(
            "1.23",
            &serde_json::to_string(&Variant::Decimal(
                Decimal::from_scientific("123E-2").unwrap()
            ))
            .unwrap()
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
                (Variant::String("b".to_string()), Variant::Boolean(true)),
                (
                    Variant::String("i".to_string()),
                    Variant::Decimal(Decimal::from(12345)),
                ),
                (
                    Variant::String("f".to_string()),
                    Variant::Double(F64::new(0.00123)),
                ),
                (
                    Variant::String("d".to_string()),
                    Variant::Decimal(Decimal::from_str("123.45").unwrap()),
                ),
                (
                    Variant::String("s".to_string()),
                    Variant::String("foo\nbar".to_string()),
                ),
                (Variant::String("n".to_string()), Variant::VariantNull),
                (
                    Variant::String("nested".to_string()),
                    Variant::Map(
                        [
                            (
                                Variant::String("arr".to_string()),
                                Variant::Array(vec![
                                    Variant::Decimal(Decimal::from(1)),
                                    Variant::String("foo".to_string()),
                                    Variant::VariantNull,
                                ]),
                            ),
                            (
                                Variant::String("ts".to_string()),
                                Variant::Timestamp(Timestamp::from_dateTime(
                                    DateTime::parse_from_rfc3339("2024-12-19T16:39:57Z")
                                        .unwrap()
                                        .to_utc(),
                                )),
                            ),
                            (
                                Variant::String("d".to_string()),
                                Variant::Date(Date::from_date(
                                    NaiveDate::from_ymd_opt(2024, 01, 01).unwrap(),
                                )),
                            ),
                            (
                                Variant::String("t".to_string()),
                                Variant::Time(Time::from_time(
                                    NaiveTime::from_hms_opt(17, 30, 40).unwrap(),
                                )),
                            ),
                        ]
                        .into_iter()
                        .collect(),
                    ),
                ),
            ]
            .into_iter()
            .collect(),
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
                "nested": {
                    "arr": [1, "foo", null],
                    "ts": "2024-12-19T16:39:57+00:00",
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
