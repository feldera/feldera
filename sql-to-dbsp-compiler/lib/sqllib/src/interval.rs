//! Support for SQL interval types.
//! Intervals are differences between dates and/or times
//! There are two interval types:
//! - Short intervals, representing differences between times. These are
//!   represented as milliseconds (positive or negative).
//! - Long intervals, representing differences between months. These are
//!   represented as days.

use dbsp::num_entries_scalar;
use dbsp_adapters::{DeserializeWithContext, SerializeWithContext, SqlSerdeConfig};
use num::PrimInt;
use serde::{de::Error as _, Deserialize, Deserializer, Serializer};
use size_of::SizeOf;
use std::{borrow::Cow, fmt::Debug, ops::Mul};

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
)]
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

impl SerializeWithContext<SqlSerdeConfig> for ShortInterval {
    fn serialize_with_context<S>(
        &self,
        serializer: S,
        _context: &SqlSerdeConfig,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // TODO: is this the right way to serialize ShortIntervals?
        serializer.serialize_str(&self.milliseconds.to_string())
    }
}

impl<'de> DeserializeWithContext<'de, SqlSerdeConfig> for ShortInterval {
    fn deserialize_with_context<D>(
        deserializer: D,
        _config: &'de SqlSerdeConfig,
    ) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let str: Cow<'de, str> = Deserialize::deserialize(deserializer)?;
        let milliseconds = str
            .trim()
            .parse::<i64>()
            .map_err(|e| D::Error::custom(format!("invalid ShortInterval '{str}': {e}")))?;

        Ok(Self::new(milliseconds))
    }
}

impl<'de> Deserialize<'de> for ShortInterval {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let str: Cow<'de, str> = Deserialize::deserialize(deserializer)?;
        let milliseconds = str
            .trim()
            .parse::<i64>()
            .map_err(|e| D::Error::custom(format!("invalid ShortInterval '{str}': {e}")))?;

        Ok(Self::new(milliseconds))
    }
}

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
)]
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

impl SerializeWithContext<SqlSerdeConfig> for LongInterval {
    fn serialize_with_context<S>(
        &self,
        serializer: S,
        _context: &SqlSerdeConfig,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // TODO: is this the right way to serialize ShortIntervals?
        serializer.serialize_str(&self.days.to_string())
    }
}

impl<'de> DeserializeWithContext<'de, SqlSerdeConfig> for LongInterval {
    fn deserialize_with_context<D>(
        deserializer: D,
        _config: &'de SqlSerdeConfig,
    ) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let str: Cow<'de, str> = Deserialize::deserialize(deserializer)?;
        let days = str
            .trim()
            .parse::<i32>()
            .map_err(|e| D::Error::custom(format!("invalid LongInterval '{str}': {e}")))?;

        Ok(Self::new(days))
    }
}

impl<'de> Deserialize<'de> for LongInterval {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let str: Cow<'de, str> = Deserialize::deserialize(deserializer)?;
        let days = str
            .trim()
            .parse::<i32>()
            .map_err(|e| D::Error::custom(format!("invalid ShortInterval '{str}': {e}")))?;

        Ok(Self::new(days))
    }
}
