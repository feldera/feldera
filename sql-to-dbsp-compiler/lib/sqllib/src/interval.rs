//! Support for SQL interval types.
//! Intervals are differences between dates and/or times
//! There are two interval types:
//! - Short intervals, representing differences between times. These are
//!   represented as milliseconds (positive or negative).
//! - Long intervals, representing differences between months. These are
//!   represented as days.

use dbsp::num_entries_scalar;
use feldera_types::{deserialize_without_context, serialize_without_context};
use num::PrimInt;
use serde::{Deserialize, Serialize};
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

serialize_without_context!(ShortInterval);
deserialize_without_context!(ShortInterval);

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

serialize_without_context!(LongInterval);
deserialize_without_context!(LongInterval);
