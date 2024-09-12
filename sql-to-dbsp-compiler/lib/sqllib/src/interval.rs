//! Support for SQL interval types.
//! Intervals are differences between dates and/or times
//! There are two interval types:
//! - Short intervals, representing differences between times. These are
//!   represented as milliseconds (positive or negative).
//! - Long intervals, representing differences between months. These are
//!   represented as days.

use crate::{
    operators::{eq, gt, gte, lt, lte, neq},
    some_existing_operator, some_operator,
};
use dbsp::num_entries_scalar;
use feldera_types::serde_with_context::{
    DeserializeWithContext, SerializeWithContext, SqlSerdeConfig,
};
use num::PrimInt;
use serde::{de::Error as _, ser::Error as _, Deserialize, Serialize};
use size_of::SizeOf;
use std::{fmt::Debug, ops::Mul};

#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    SizeOf,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    Serialize,
    Deserialize,
)]
#[archive_attr(derive(Clone, Ord, Eq, PartialEq, PartialOrd))]
#[archive(compare(PartialEq, PartialOrd))]
#[serde(transparent)]
pub struct ShortInterval {
    milliseconds: i64,
}

impl ShortInterval {
    pub const fn new(milliseconds: i64) -> Self {
        Self { milliseconds }
    }

    pub fn milliseconds(&self) -> i64 {
        self.milliseconds
    }

    pub fn nanoseconds(&self) -> i64 {
        self.milliseconds * 1_000_000_i64
    }
}

pub fn to_bound_ShortInterval_Date_u128(value: &ShortInterval) -> u128 {
    // express value in days
    (value.milliseconds / 1000 / 86400) as u128
}

pub fn to_bound_ShortInterval_Date_u64(value: &ShortInterval) -> u64 {
    // express value in days
    (value.milliseconds / 1000 / 86400) as u64
}

pub fn to_bound_ShortInterval_Timestamp_u128(value: &ShortInterval) -> u128 {
    // express value in milliseconds
    value.milliseconds as u128
}

pub fn to_bound_ShortInterval_Timestamp_u64(value: &ShortInterval) -> u64 {
    // express value in milliseconds
    value.milliseconds as u64
}

pub fn to_bound_ShortInterval_Time_u128(value: &ShortInterval) -> u128 {
    // express value in nanoseconds
    (value.milliseconds * 1_000_000) as u128
}

pub fn to_bound_ShortInterval_Time_u64(value: &ShortInterval) -> u64 {
    // express value in nanoseconds
    (value.milliseconds * 1_000_000) as u64
}

impl<T> Mul<T> for ShortInterval
where
    T: PrimInt,
    i64: Mul<T, Output = i64>,
{
    type Output = Self;

    fn mul(self, rhs: T) -> Self {
        Self {
            milliseconds: self.milliseconds * rhs,
        }
    }
}

impl<T> From<T> for ShortInterval
where
    i64: From<T>,
    T: PrimInt,
{
    fn from(value: T) -> Self {
        Self {
            milliseconds: i64::from(value),
        }
    }
}

impl SerializeWithContext<SqlSerdeConfig> for ShortInterval {
    fn serialize_with_context<S>(
        &self,
        _serializer: S,
        _context: &SqlSerdeConfig,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        Err(S::Error::custom(
            "serialization is not implemented for the INTERVAL type",
        ))
    }
}

impl<'de> DeserializeWithContext<'de, SqlSerdeConfig> for ShortInterval {
    fn deserialize_with_context<D>(
        _deserializer: D,
        _context: &'de SqlSerdeConfig,
    ) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Err(D::Error::custom(
            "deserialization is not implemented for the INTERVAL type",
        ))
    }
}

some_operator!(lt, ShortInterval, ShortInterval, bool);
some_operator!(gt, ShortInterval, ShortInterval, bool);
some_operator!(eq, ShortInterval, ShortInterval, bool);
some_operator!(neq, ShortInterval, ShortInterval, bool);
some_operator!(gte, ShortInterval, ShortInterval, bool);
some_operator!(lte, ShortInterval, ShortInterval, bool);

/////////////////////////

#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    SizeOf,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    Serialize,
    Deserialize,
)]
#[archive_attr(derive(Clone, Ord, Eq, PartialEq, PartialOrd))]
#[archive(compare(PartialEq, PartialOrd))]
#[serde(transparent)]
pub struct LongInterval {
    months: i32,
}

impl LongInterval {
    pub const fn new(months: i32) -> Self {
        Self { months }
    }

    pub fn months(&self) -> i32 {
        self.months
    }
}

impl<T> Mul<T> for LongInterval
where
    T: PrimInt,
    i32: Mul<T, Output = i32>,
{
    type Output = Self;

    fn mul(self, rhs: T) -> Self {
        Self {
            months: self.months * rhs,
        }
    }
}

impl<T> From<T> for LongInterval
where
    i32: From<T>,
    T: PrimInt,
{
    fn from(value: T) -> Self {
        Self {
            months: i32::from(value),
        }
    }
}

num_entries_scalar! {
    ShortInterval,
    LongInterval,
}

impl SerializeWithContext<SqlSerdeConfig> for LongInterval {
    fn serialize_with_context<S>(
        &self,
        _serializer: S,
        _context: &SqlSerdeConfig,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        Err(S::Error::custom(
            "serialization is not implemented for the INTERVAL type",
        ))
    }
}

impl<'de> DeserializeWithContext<'de, SqlSerdeConfig> for LongInterval {
    fn deserialize_with_context<D>(
        _deserializer: D,
        _context: &'de SqlSerdeConfig,
    ) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Err(D::Error::custom(
            "deserialization is not implemented for the INTERVAL type",
        ))
    }
}

some_operator!(lt, LongInterval, LongInterval, bool);
some_operator!(gt, LongInterval, LongInterval, bool);
some_operator!(eq, LongInterval, LongInterval, bool);
some_operator!(neq, LongInterval, LongInterval, bool);
some_operator!(gte, LongInterval, LongInterval, bool);
some_operator!(lte, LongInterval, LongInterval, bool);
