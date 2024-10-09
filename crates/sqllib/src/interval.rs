//! Support for SQL interval types.
//! Intervals are differences between dates and/or times

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

#[cfg(doc)]
use crate::{Date, Time, Timestamp};

/// A ShortInterval can express a difference between two [Time]
/// values, two [Date] values, or two [Timestamp] values.  The
/// representation is a (positive or negative) number of milliseconds.
/// While there are functions that can extract nanosecond-precision
/// values from a ShortInterval, the precision is always limited to
/// milliseconds.
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
    /// Create a ShortInterval with a length specified in milliseconds.
    pub const fn new(milliseconds: i64) -> Self {
        Self { milliseconds }
    }

    /// Extract the length of the interval in milliseconds.  The
    /// result can be negative.
    pub fn milliseconds(&self) -> i64 {
        self.milliseconds
    }

    /// Extract the length of the interval in nanoseconds.  The result
    /// can be negative.  The granularity of the representation is
    /// actually milliseconds, so this function will always return a
    /// number that is a multiple of 1 million.
    pub fn nanoseconds(&self) -> i64 {
        self.milliseconds * 1_000_000_i64
    }
}

#[doc(hidden)]
/// This function is used in rolling window computations, which require all
/// values to be expressed using unsigned types.
pub fn to_bound_ShortInterval_Date_u128(value: &ShortInterval) -> u128 {
    // express value in days
    (value.milliseconds / 1000 / 86400) as u128
}

#[doc(hidden)]
/// This function is used in rolling window computations, which require all
/// values to be expressed using unsigned types.
pub fn to_bound_ShortInterval_Date_u64(value: &ShortInterval) -> u64 {
    // express value in days
    (value.milliseconds / 1000 / 86400) as u64
}

#[doc(hidden)]
/// This function is used in rolling window computations, which require all
/// values to be expressed using unsigned types.
pub fn to_bound_ShortInterval_Timestamp_u128(value: &ShortInterval) -> u128 {
    // express value in milliseconds
    value.milliseconds as u128
}

#[doc(hidden)]
/// This function is used in rolling window computations, which require all
/// values to be expressed using unsigned types.
pub fn to_bound_ShortInterval_Timestamp_u64(value: &ShortInterval) -> u64 {
    // express value in milliseconds
    value.milliseconds as u64
}

#[doc(hidden)]
/// This function is used in rolling window computations, which require all
/// values to be expressed using unsigned types.
pub fn to_bound_ShortInterval_Time_u128(value: &ShortInterval) -> u128 {
    // express value in nanoseconds
    (value.milliseconds * 1_000_000) as u128
}

#[doc(hidden)]
/// This function is used in rolling window computations, which require all
/// values to be expressed using unsigned types.
pub fn to_bound_ShortInterval_Time_u64(value: &ShortInterval) -> u64 {
    // express value in nanoseconds
    (value.milliseconds * 1_000_000) as u64
}

/// Multiply a `ShortInterval` by a numeric value, producing a
/// `ShortInterval`.
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

/// Create a `ShortInterval` from a numeric value that is interpreted as
/// a number of milliseconds.
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

/// Serialize a `ShortInterval` with context.  See
/// [`SerializeWithContext`].
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

/// Deserialize a `ShortInterval` with context.  See
/// [`DeserializeWithContext`].
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

/// A difference between two [Date]s expressed as a (positive or
/// negative) number of months.  The value of a LongInterval is not
/// absolute, but it is relative to the date the interval is applied
/// to.  For example, an interval of 3 months starting from March 1st
/// has size of 92 days, but starting from April 1st it has a size of
/// 91 days.
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
    /// Create a new LongInterval from a number of months.
    pub const fn new(months: i32) -> Self {
        Self { months }
    }

    /// Retrieve the number of months in the long interval.
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

    /// Multiply a long interval by an integer.
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
    /// Convert a integer expressing a number of months into a
    /// [LongInterval]
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
