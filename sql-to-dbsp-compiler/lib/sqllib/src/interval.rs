//! Support for SQL interval types.
//! Intervals are differences between dates and/or times
//! There are two interval types:
//! - Short intervals, representing differences between times. These are
//!   represented as milliseconds (positive or negative).
//! - Long intervals, representing differences between months. These are
//!   represented as days.

use crate::ToWindowBound;
use dbsp::num_entries_scalar;
use num::PrimInt;
use pipeline_types::{deserialize_without_context, serialize_without_context};
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

// This trait is used when converting to DATE offets
// In this case we convert the number to days
impl ToWindowBound<u64> for ShortInterval {
    fn to_bound(&self) -> u64 {
        (self.milliseconds / 1000 / 86400) as u64
    }
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

impl ToWindowBound<u64> for LongInterval {
    // TODO: this is not correct.  This expresses the interval in
    // days.
    fn to_bound(&self) -> u64 {
        (self.months * 30) as u64
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
