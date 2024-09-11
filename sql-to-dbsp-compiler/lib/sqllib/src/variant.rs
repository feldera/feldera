//! Variant is a dynamically-typed object that can represent
//! the values in a SQL program.

use crate::{Date, GeoPoint, LongInterval, ShortInterval, Time, Timestamp};
use dbsp::algebra::{F32, F64};
use rkyv::collections::ArchivedBTreeMap;
use rkyv::string::ArchivedString;
use rkyv::Fallible;
use rust_decimal::Decimal;
use serde::ser::SerializeMap;
use serde::{Deserialize, Serialize};
use size_of::{Context, SizeOf};
use std::{
    collections::BTreeMap,
    fmt::{Debug, Write},
    hash::Hash,
    str::FromStr,
};

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
    Serialize,
    Deserialize,
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

fn json_string_escape(src: &str, buffer: &mut String) -> Result<(), Box<dyn std::error::Error>> {
    let mut utf16_buf = [0u16; 2];
    *buffer += "\"";
    for c in src.chars() {
        match c {
            '\x08' => *buffer += "\\b",
            '\x0c' => *buffer += "\\f",
            '\n' => *buffer += "\\n",
            '\r' => *buffer += "\\r",
            '\t' => *buffer += "\\t",
            '"' => *buffer += "\\\"",
            '\\' => *buffer += "\\",
            c if c.is_ascii_graphic() => buffer.push(c),
            c => {
                let encoded = c.encode_utf16(&mut utf16_buf);
                for utf16 in encoded {
                    write!(buffer, "\\u{:04X}", utf16)?;
                }
            }
        }
    }
    *buffer += "\"";
    Ok(())
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

    pub fn to_json_string(&self, buffer: &mut String) -> Result<(), Box<dyn std::error::Error>> {
        match self {
            Variant::VariantNull => {
                write!(buffer, "null")?;
                Ok(())
            }
            Variant::Boolean(b) => {
                write!(buffer, "{}", b)?;
                Ok(())
            }
            Variant::TinyInt(i) => {
                write!(buffer, "{}", i)?;
                Ok(())
            }
            Variant::SmallInt(i) => {
                write!(buffer, "{}", i)?;
                Ok(())
            }
            Variant::Int(i) => {
                write!(buffer, "{}", i)?;
                Ok(())
            }
            Variant::BigInt(i) => {
                write!(buffer, "{}", i)?;
                Ok(())
            }
            Variant::Real(f) => {
                write!(buffer, "{}", f)?;
                Ok(())
            }
            Variant::Double(f) => {
                write!(buffer, "{}", f)?;
                Ok(())
            }
            Variant::Decimal(d) => {
                write!(buffer, "{}", d)?;
                Ok(())
            }
            Variant::String(s) => {
                json_string_escape(s, buffer)?;
                Ok(())
            }
            Variant::Array(v) => {
                write!(buffer, "[")?;
                let mut first = true;
                for e in v {
                    if !first {
                        write!(buffer, ",")?;
                    }
                    first = false;
                    e.to_json_string(buffer)?;
                }
                write!(buffer, "]")?;
                Ok(())
            }
            Variant::Map(m) => {
                write!(buffer, "{{")?;
                let mut first = true;
                for (k, v) in m {
                    if !first {
                        write!(buffer, ",")?;
                    }
                    first = false;
                    match k {
                        Variant::String(_) => k.to_json_string(buffer),
                        _ => return Err(Box::from("Not a JSON value (label is not a string)")),
                    }?;
                    write!(buffer, ":")?;
                    v.to_json_string(buffer)?;
                }
                write!(buffer, "}}")?;
                Ok(())
            }
            _ => Err(Box::from("Not a JSON value (not a json type)")),
        }
    }
}

impl TryFrom<serde_json::Value> for Variant {
    type Error = rust_decimal::Error;

    fn try_from(value: serde_json::Value) -> Result<Self, rust_decimal::Error> {
        match value {
            serde_json::Value::Null => Ok(Variant::VariantNull),
            serde_json::Value::Bool(b) => Ok(Variant::Boolean(b)),
            serde_json::Value::String(s) => Ok(Variant::String(s)),
            serde_json::Value::Number(n) => {
                let decimal = Decimal::from_str(n.as_str())?;
                Ok(Variant::Decimal(decimal))
            }
            serde_json::Value::Array(v) => {
                let mut result = Vec::new();
                for e in v {
                    result.push(Variant::try_from(e)?);
                }
                Ok(Variant::Array(result))
            }
            serde_json::Value::Object(m) => {
                let mut result = BTreeMap::<Variant, Variant>::new();
                for (key, v) in m.iter() {
                    result.insert(Variant::String(key.clone()), Variant::try_from(v.clone())?);
                }
                Ok(Variant::Map(result))
            }
        }
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

#[cfg(test)]
mod test {
    use super::Variant;
    use dbsp::RootCircuit;

    #[test]
    fn circuit_accepts_variant() {
        let (_circuit, _input_handle) = RootCircuit::build(move |circuit| {
            let (_stream, input_handle) = circuit.add_input_zset::<Variant>();
            Ok(input_handle)
        })
        .unwrap();
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
