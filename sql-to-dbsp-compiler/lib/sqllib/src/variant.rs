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
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::hash::Hash;

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
    Boolean(Option<bool>),
    TinyInt(Option<i8>),
    SmallInt(Option<i16>),
    Int(Option<i32>),
    BigInt(Option<i64>),
    Real(Option<F32>),
    Double(Option<F64>),
    Decimal(Option<Decimal>),
    String(Option<String>),
    Date(Option<Date>),
    Time(Option<Time>),
    Timestamp(Option<Timestamp>),
    ShortInterval(Option<ShortInterval>),
    LongInterval(Option<LongInterval>),
    Geometry(Option<GeoPoint>),
    #[size_of(skip, skip_bounds)]
    Array(#[omit_bounds] Option<Vec<Variant>>),
    #[size_of(skip, skip_bounds)]
    Map(#[omit_bounds] Option<BTreeMap<String, Variant>>),
    //Struct(Option<Box<dyn StructVariant>>),
}

// A macro for TryInto<Option<T>> for Variant
macro_rules! try_into_option {
    ($variant:ident, $type:ty) => {
        impl TryInto<Option<$type>> for Variant {
            type Error = &'static str;

            fn try_into(self) -> Result<Option<$type>, Self::Error> {
                match self {
                    Variant::$variant(value) => Ok(value),
                    _ => Err(concat!("Variant is not a ", stringify!($variant))),
                }
            }
        }
    };
}

try_into_option!(Boolean, bool);
try_into_option!(TinyInt, i8);
try_into_option!(SmallInt, i16);
try_into_option!(Int, i32);
try_into_option!(BigInt, i64);
try_into_option!(Real, F32);
try_into_option!(Double, F64);
try_into_option!(Decimal, Decimal);
try_into_option!(String, String);
try_into_option!(Date, Date);
try_into_option!(Time, Time);
try_into_option!(Timestamp, Timestamp);
try_into_option!(ShortInterval, ShortInterval);
try_into_option!(LongInterval, LongInterval);
try_into_option!(Geometry, GeoPoint);
try_into_option!(Array, Vec<Variant>);
try_into_option!(Map, BTreeMap<String, Variant>);
//try_into_option!(Struct, Box<dyn StructVariant>);

// A macro for From<T> for Variant
macro_rules! from {
    ($variant:ident, $type:ty) => {
        impl From<$type> for Variant {
            fn from(value: $type) -> Self {
                Variant::$variant(Some(value))
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
from!(Array, Vec<Variant>);
from!(Map, BTreeMap<String, Variant>);
//from!(Struct, Box<dyn StructVariant>);

impl From<()> for Variant {
    fn from(_: ()) -> Self {
        Variant::SqlNull
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
