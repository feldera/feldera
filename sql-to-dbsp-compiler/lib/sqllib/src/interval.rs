//! Support for SQL interval types.
//! Intervals are differences between dates and/or times
//! There are two interval types:
//! - Short intervals, representing differences between times. These are
//!   represented as milliseconds (positive or negative).
//! - Long intervals, representing differences between months. These are
//!   represented as days.

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
    days: i32,
}

impl LongInterval {
    pub const fn new(days: i32) -> Self {
        Self { days }
    }

    pub fn days(&self) -> i32 {
        self.days
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
            days: self.days * rhs,
        }
    }
}

impl<T> From<T> for LongInterval
where
    i32: From<T>,
{
    fn from(value: T) -> Self {
        Self {
            days: i32::from(value),
        }
    }
}

num_entries_scalar! {
    ShortInterval,
    LongInterval,
}

serialize_without_context!(LongInterval);
deserialize_without_context!(LongInterval);
