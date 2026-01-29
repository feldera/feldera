//! Support for SQL interval types.
//! Intervals are differences between dates and/or times

use crate::{
    Date, SqlDecimal,
    operators::{eq, gt, gte, lt, lte, neq},
    plus_Date_Date_LongInterval__, some_existing_operator, some_function2, some_operator,
    some_polymorphic_function1, some_polymorphic_function2,
    timestamp::{extract_epoch_Date, extract_quarter_Date},
};
use dbsp::{algebra::F64, num_entries_scalar};
use feldera_macros::IsNone;
use feldera_types::serde_with_context::{
    DeserializeWithContext, SerializeWithContext, SqlSerdeConfig,
};
use serde::{Deserialize, Serialize, de::Error as _, ser::Error as _};
use size_of::SizeOf;
use std::{
    fmt::Debug,
    ops::{Add, Div, Mul, Neg, Sub},
};

#[cfg(doc)]
use crate::{Time, Timestamp};

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
    Serialize,
    Deserialize,
    IsNone,
)]
#[archive_attr(derive(Clone, Ord, Eq, PartialEq, PartialOrd))]
#[archive(compare(PartialEq, PartialOrd))]
#[serde(transparent)]
pub struct ShortInterval {
    microseconds: i64,
}

#[doc(hidden)]
impl<D> ::rkyv::Deserialize<ShortInterval, D> for ArchivedShortInterval
where
    D: ::rkyv::Fallible + ::core::any::Any,
{
    fn deserialize(&self, deserializer: &mut D) -> Result<ShortInterval, D::Error> {
        // The internal representation of ShortInterval changed from version 4 of the storage format
        const MILLISECOND_VERSION: u32 = 4;
        let version = (deserializer as &mut dyn ::core::any::Any)
            .downcast_mut::<::dbsp::storage::file::Deserializer>()
            .map(|deserializer| deserializer.version())
            .expect("Deserializer must be of type dbsp::storage::file::Deserializer");
        let value: i64 = self.microseconds.deserialize(deserializer)?;
        if version <= MILLISECOND_VERSION {
            Ok(ShortInterval::from_milliseconds(value))
        } else {
            Ok(ShortInterval::from_microseconds(value))
        }
    }
}

impl ShortInterval {
    /// Create a ShortInterval with a length specified in milliseconds.
    pub const fn from_milliseconds(milliseconds: i64) -> Self {
        Self {
            microseconds: milliseconds * 1000,
        }
    }

    /// Create a ShortInterval with a length specified in microseconds.
    pub const fn from_microseconds(microseconds: i64) -> Self {
        Self { microseconds }
    }

    /// Create a ShortInterval with a length specified in seconds.
    pub const fn from_seconds(seconds: i64) -> Self {
        Self::from_milliseconds(seconds * 1_000)
    }

    /// Extract the length of the interval in microseconds.  The
    /// result can be negative.
    pub fn microseconds(&self) -> i64 {
        self.microseconds
    }

    /// Extract the length of the interval in milliseconds.  The
    /// result can be negative.
    pub fn milliseconds(&self) -> i64 {
        self.microseconds / 1000
    }

    /// Extract the length of the interval in nanoseconds.  The result
    /// can be negative.  The granularity of the representation is
    /// actually milliseconds, so this function will always return a
    /// number that is a multiple of 1 million.
    pub fn nanoseconds(&self) -> i64 {
        self.microseconds * 1_000
    }
}

#[doc(hidden)]
pub fn abs_ShortInterval(value: ShortInterval) -> ShortInterval {
    ShortInterval::from_microseconds(num::abs(value.microseconds))
}

some_polymorphic_function1!(abs, ShortInterval, ShortInterval, ShortInterval);

#[doc(hidden)]
/// This function is used in rolling window computations, which require all
/// values to be expressed using unsigned types.
pub fn to_bound_ShortInterval_Date_u128(value: &ShortInterval) -> u128 {
    // express value in days
    (value.microseconds / 1_000_000_i64 / 86400) as u128
}

#[doc(hidden)]
/// This function is used in rolling window computations, which require all
/// values to be expressed using unsigned types.
pub fn to_bound_ShortInterval_Date_u64(value: &ShortInterval) -> u64 {
    // express value in days
    (value.microseconds / 1_000_000_i64 / 86400) as u64
}

#[doc(hidden)]
/// This function is used in rolling window computations, which require all
/// values to be expressed using unsigned types.
pub fn to_bound_ShortInterval_Timestamp_u128(value: &ShortInterval) -> u128 {
    // express value in milliseconds
    value.microseconds as u128
}

#[doc(hidden)]
/// This function is used in rolling window computations, which require all
/// values to be expressed using unsigned types.
pub fn to_bound_ShortInterval_Timestamp_u64(value: &ShortInterval) -> u64 {
    // express value in milliseconds
    value.microseconds as u64
}

#[doc(hidden)]
/// This function is used in rolling window computations, which require all
/// values to be expressed using unsigned types.
pub fn to_bound_ShortInterval_Time_u128(value: &ShortInterval) -> u128 {
    // express value in nanoseconds
    (value.microseconds * 1_000) as u128
}

#[doc(hidden)]
/// This function is used in rolling window computations, which require all
/// values to be expressed using unsigned types.
pub fn to_bound_ShortInterval_Time_u64(value: &ShortInterval) -> u64 {
    // express value in nanoseconds
    (value.microseconds * 1_000) as u64
}

/// Multiply a `ShortInterval` by a numeric value, producing a `ShortInterval`.
impl Mul<i64> for ShortInterval {
    type Output = Self;

    /// Multiply a short interval by an long integer
    fn mul(self, rhs: i64) -> Self {
        Self {
            microseconds: self.microseconds * rhs,
        }
    }
}

/// Multiply a `ShortInterval` by a numeric value, producing a `ShortInterval`.
impl Mul<F64> for ShortInterval {
    type Output = Self;

    fn mul(self, rhs: F64) -> Self {
        Self {
            microseconds: (F64::from(self.microseconds as f64) * rhs).into_inner() as i64,
        }
    }
}

/// Multiply a `ShortInterval` by a numeric value, producing a `ShortInterval`.
impl<const P: usize, const S: usize> Mul<SqlDecimal<P, S>> for ShortInterval {
    type Output = Self;

    fn mul(self, rhs: SqlDecimal<P, S>) -> Self {
        let us = SqlDecimal::<38, 0>::try_from(self.microseconds).expect(
            "overflow in short interval multiplication while converting microseconds to DECIMAL",
        );
        let mul = us
            .checked_mul_generic::<P, S, 38, 0>(rhs)
            .expect("overflow in short interval multiplication");
        Self {
            microseconds: mul
                .try_into()
                .expect("overflow in short interval multiplication: result too large"),
        }
    }
}

/// Divide a `ShortInterval` by a numeric value, producing a `ShortInterval`.
impl Div<i64> for ShortInterval {
    type Output = Self;

    fn div(self, rhs: i64) -> Self {
        Self {
            microseconds: self.microseconds / rhs,
        }
    }
}

/// Divide a `ShortInterval` by a numeric value, producing a `ShortInterval`.
impl Div<F64> for ShortInterval {
    type Output = Self;
    fn div(self, rhs: F64) -> Self {
        Self {
            microseconds: (F64::from(self.microseconds as f64) / rhs).into_inner() as i64,
        }
    }
}

/// Divide a `ShortInterval` by a numeric value, producing a `ShortInterval`.
impl<const P: usize, const S: usize> Div<SqlDecimal<P, S>> for ShortInterval {
    type Output = Self;

    fn div(self, rhs: SqlDecimal<P, S>) -> Self {
        let us = SqlDecimal::<38, 0>::try_from(self.microseconds)
            .expect("overflow in short interval division while converting microseconds to DECIMAL");
        let div = us
            .checked_div_generic::<P, S, 38, 0>(rhs)
            .expect("overflow in short interval division");
        Self {
            microseconds: div
                .try_into()
                .expect("overflow in short interval division: result too large"),
        }
    }
}

/// Add two `ShortInterval`s
impl Add<ShortInterval> for ShortInterval {
    type Output = Self;

    fn add(self, rhs: ShortInterval) -> Self {
        Self {
            microseconds: self
                .microseconds
                .checked_add(rhs.microseconds)
                .expect("Overflow during ShortInterval addition"),
        }
    }
}

/// Subtract two `ShortInterval`s
impl Sub<ShortInterval> for ShortInterval {
    type Output = Self;

    fn sub(self, rhs: ShortInterval) -> Self {
        Self {
            microseconds: self
                .microseconds
                .checked_sub(rhs.microseconds)
                .expect("Overflow during ShortInterval subtraction"),
        }
    }
}

/// Negate a `ShortInterval`
impl Neg for ShortInterval {
    type Output = Self;

    fn neg(self) -> Self {
        Self {
            microseconds: self
                .microseconds
                .checked_neg()
                .expect("Overflow during ShortInterval negation"),
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
impl<'de> DeserializeWithContext<'de, SqlSerdeConfig, ()> for ShortInterval {
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

#[doc(hidden)]
pub fn div_ShortInterval_SqlDecimal<const P: usize, const S: usize>(
    left: ShortInterval,
    right: SqlDecimal<P, S>,
) -> ShortInterval {
    left / right
}

some_polymorphic_function2!(
    div <const P: usize, const S: usize>,
    ShortInterval,
    ShortInterval,
    SqlDecimal,
    SqlDecimal<P, S>,
    ShortInterval
);

#[doc(hidden)]
pub fn times_ShortInterval_i64(left: ShortInterval, right: i64) -> ShortInterval {
    left * right
}

some_polymorphic_function2!(times, ShortInterval, ShortInterval, i64, i64, ShortInterval);

#[doc(hidden)]
pub fn times_ShortInterval_d(left: ShortInterval, right: F64) -> ShortInterval {
    left * right
}

some_polymorphic_function2!(times, ShortInterval, ShortInterval, d, F64, ShortInterval);

#[doc(hidden)]
pub fn times_ShortInterval_SqlDecimal<const P: usize, const S: usize>(
    left: ShortInterval,
    right: SqlDecimal<P, S>,
) -> ShortInterval {
    left * right
}

some_polymorphic_function2!(
    times <const P: usize, const S: usize>,
    ShortInterval,
    ShortInterval,
    SqlDecimal,
    SqlDecimal<P, S>,
    ShortInterval
);

#[doc(hidden)]
pub fn div_ShortInterval_d(left: ShortInterval, right: F64) -> ShortInterval {
    left / right
}

some_polymorphic_function2!(div, ShortInterval, ShortInterval, d, F64, ShortInterval);

#[doc(hidden)]
pub fn div_ShortInterval_i64(left: ShortInterval, right: i64) -> ShortInterval {
    left / right
}

some_polymorphic_function2!(div, ShortInterval, ShortInterval, i64, i64, ShortInterval);

#[doc(hidden)]
pub fn plus_ShortInterval_ShortInterval_ShortInterval__(
    left: ShortInterval,
    right: ShortInterval,
) -> ShortInterval {
    left + right
}

some_function2!(
    plus_ShortInterval_ShortInterval_ShortInterval,
    ShortInterval,
    ShortInterval,
    ShortInterval
);

#[doc(hidden)]
pub fn minus_ShortInterval_ShortInterval_ShortInterval__(
    left: ShortInterval,
    right: ShortInterval,
) -> ShortInterval {
    left - right
}

some_function2!(
    minus_ShortInterval_ShortInterval_ShortInterval,
    ShortInterval,
    ShortInterval,
    ShortInterval
);

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
    IsNone,
)]
#[archive_attr(derive(Clone, Ord, Eq, PartialEq, PartialOrd))]
#[archive(compare(PartialEq, PartialOrd))]
#[serde(transparent)]
pub struct LongInterval {
    months: i32,
}

impl LongInterval {
    /// Create a new LongInterval from a number of months.
    pub const fn from_months(months: i32) -> Self {
        Self { months }
    }

    /// Retrieve the number of months in the long interval.
    pub fn months(&self) -> i32 {
        self.months
    }

    /// Retrieve the number of years in the long interval.
    pub fn years(&self) -> i32 {
        let (mul, months) = if self.months() < 0 {
            (-1, -self.months())
        } else {
            (1, self.months())
        };
        (months / 12) * mul
    }
}

#[doc(hidden)]
pub fn abs_LongInterval(value: LongInterval) -> LongInterval {
    LongInterval::from_months(num::abs(value.months))
}

some_polymorphic_function1!(abs, LongInterval, LongInterval, LongInterval);

/// Multiply a `LongInterval` by an integer producing a `LongInterval`
impl Mul<i32> for LongInterval {
    type Output = Self;

    fn mul(self, rhs: i32) -> Self {
        Self {
            months: self.months * rhs,
        }
    }
}

/// Multiply a `LongInterval` by a numeric value producing a `LongInterval`
impl Mul<F64> for LongInterval {
    type Output = Self;

    fn mul(self, rhs: F64) -> Self {
        Self {
            months: (F64::from(self.months as f64) * rhs).into_inner() as i32,
        }
    }
}

/// Multiply a `LongInterval` by a numeric value producing a `LongInterval`
impl<const P: usize, const S: usize> Mul<SqlDecimal<P, S>> for LongInterval {
    type Output = Self;

    fn mul(self, rhs: SqlDecimal<P, S>) -> Self {
        let months = SqlDecimal::<10, 0>::try_from(self.months)
            .expect("overflow in long interval multiplication while converting months to DECIMAL");
        let mul = months
            .checked_mul_generic::<P, S, 10, 0>(rhs)
            .expect("overflow in long interval multiplication");
        Self {
            months: mul
                .try_into()
                .expect("overflow in long interval multiplication: result too large"),
        }
    }
}

/// Divide a `LongInterval` by an integer producing a `LongInterval`
impl Div<i32> for LongInterval {
    type Output = Self;

    fn div(self, rhs: i32) -> Self {
        Self {
            months: self.months / rhs,
        }
    }
}

/// Divide a `LongInterval` by a numeric value producing a `LongInterval`
impl Div<F64> for LongInterval {
    type Output = Self;

    fn div(self, rhs: F64) -> Self {
        Self {
            months: (F64::from(self.months as f64) / rhs).into_inner() as i32,
        }
    }
}

/// Divide a `LongInterval` by a numeric value producing a `LongInterval`
impl<const P: usize, const S: usize> Div<SqlDecimal<P, S>> for LongInterval {
    type Output = Self;

    fn div(self, rhs: SqlDecimal<P, S>) -> Self {
        let months = SqlDecimal::<10, 0>::try_from(self.months)
            .expect("overflow in long interval division while converting months to DECIMAL");
        let div = months
            .checked_div_generic::<P, S, 10, 0>(rhs)
            .expect("overflow in long interval division");
        Self {
            months: div
                .try_into()
                .expect("overflow in long interval division: result too large"),
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

impl<'de, AUX> DeserializeWithContext<'de, SqlSerdeConfig, AUX> for LongInterval {
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

/// Add two `LongInterval`s
impl Add<LongInterval> for LongInterval {
    type Output = Self;

    fn add(self, rhs: LongInterval) -> Self {
        Self {
            months: self
                .months
                .checked_add(rhs.months)
                .expect("Overflow during LongInterval addition"),
        }
    }
}

/// Subtract two `LongInterval`s
impl Sub<LongInterval> for LongInterval {
    type Output = Self;

    fn sub(self, rhs: LongInterval) -> Self {
        Self {
            months: self
                .months
                .checked_sub(rhs.months)
                .expect("Overflow during LongInterval subtraction"),
        }
    }
}

/// Negate a `LongInterval`
impl Neg for LongInterval {
    type Output = Self;

    fn neg(self) -> Self {
        Self {
            months: self
                .months
                .checked_neg()
                .expect("Overflow during LongInterval negation"),
        }
    }
}

some_operator!(lt, LongInterval, LongInterval, bool);
some_operator!(gt, LongInterval, LongInterval, bool);
some_operator!(eq, LongInterval, LongInterval, bool);
some_operator!(neq, LongInterval, LongInterval, bool);
some_operator!(gte, LongInterval, LongInterval, bool);
some_operator!(lte, LongInterval, LongInterval, bool);

#[doc(hidden)]
pub fn times_LongInterval_i32(left: LongInterval, right: i32) -> LongInterval {
    left * right
}

some_polymorphic_function2!(times, LongInterval, LongInterval, i32, i32, LongInterval);

#[doc(hidden)]
pub fn times_LongInterval_d(left: LongInterval, right: F64) -> LongInterval {
    left * right
}

some_polymorphic_function2!(times, LongInterval, LongInterval, d, F64, LongInterval);

#[doc(hidden)]
pub fn times_LongInterval_SqlDecimal<const P: usize, const S: usize>(
    left: LongInterval,
    right: SqlDecimal<P, S>,
) -> LongInterval {
    left * right
}

some_polymorphic_function2!(
    times <const P: usize, const S: usize>,
    LongInterval,
    LongInterval,
    SqlDecimal,
    SqlDecimal<P, S>,
    LongInterval
);

#[doc(hidden)]
pub fn div_LongInterval_i32(left: LongInterval, right: i32) -> LongInterval {
    left / right
}

some_polymorphic_function2!(div, LongInterval, LongInterval, i32, i32, LongInterval);

#[doc(hidden)]
pub fn div_LongInterval_d(left: LongInterval, right: F64) -> LongInterval {
    left / right
}

some_polymorphic_function2!(div, LongInterval, LongInterval, d, F64, LongInterval);

#[doc(hidden)]
pub fn div_LongInterval_SqlDecimal<const P: usize, const S: usize>(
    left: LongInterval,
    right: SqlDecimal<P, S>,
) -> LongInterval {
    left / right
}

some_polymorphic_function2!(
    div <const P: usize, const S: usize>,
    LongInterval,
    LongInterval,
    SqlDecimal,
    SqlDecimal<P, S>,
    LongInterval
);

#[doc(hidden)]
pub fn plus_LongInterval_LongInterval_LongInterval__(
    left: LongInterval,
    right: LongInterval,
) -> LongInterval {
    left + right
}

some_function2!(
    plus_LongInterval_LongInterval_LongInterval,
    LongInterval,
    LongInterval,
    LongInterval
);

#[doc(hidden)]
pub fn minus_LongInterval_LongInterval_LongInterval__(
    left: LongInterval,
    right: LongInterval,
) -> LongInterval {
    left - right
}

some_function2!(
    minus_LongInterval_LongInterval_LongInterval,
    LongInterval,
    LongInterval,
    LongInterval
);

#[doc(hidden)]
pub fn extract_year_LongInterval(value: LongInterval) -> i64 {
    (value.months / 12).into()
}

#[doc(hidden)]
pub fn extract_month_LongInterval(value: LongInterval) -> i64 {
    (value.months % 12).into()
}

#[doc(hidden)]
pub fn extract_quarter_LongInterval(value: LongInterval) -> i64 {
    let dt = Date::from_days(0);
    let dt = plus_Date_Date_LongInterval__(dt, value);
    extract_quarter_Date(dt)
}

#[doc(hidden)]
pub fn extract_decade_LongInterval(value: LongInterval) -> i64 {
    let year = extract_year_LongInterval(value);
    year / 10
}

#[doc(hidden)]
pub fn extract_millennium_LongInterval(value: LongInterval) -> i64 {
    let year = extract_year_LongInterval(value);
    year / 1000 // seems to be a different formula than dates
}

#[doc(hidden)]
pub fn extract_century_LongInterval(value: LongInterval) -> i64 {
    let year = extract_year_LongInterval(value);
    year / 100 // seems to be a different formula than dates
}

#[doc(hidden)]
pub fn extract_day_LongInterval(_value: LongInterval) -> i64 {
    0
}

#[doc(hidden)]
pub fn extract_epoch_LongInterval(value: LongInterval) -> i64 {
    let dt = Date::from_days(0);
    let dt = plus_Date_Date_LongInterval__(dt, value);
    // I think this is right and Postgres is wrong
    extract_epoch_Date(dt)
}

#[doc(hidden)]
pub fn extract_millisecond_LongInterval(_value: LongInterval) -> i64 {
    0
}

#[doc(hidden)]
pub fn extract_microsecond_LongInterval(_value: LongInterval) -> i64 {
    0
}

#[doc(hidden)]
pub fn extract_second_LongInterval(_value: LongInterval) -> i64 {
    0
}

#[doc(hidden)]
pub fn extract_minute_LongInterval(_value: LongInterval) -> i64 {
    0
}

#[doc(hidden)]
pub fn extract_hour_LongInterval(_value: LongInterval) -> i64 {
    0
}

///////////

#[doc(hidden)]
pub fn extract_day_ShortInterval(value: ShortInterval) -> i64 {
    value.microseconds() / 86_400_000_000_i64
}

#[doc(hidden)]
pub fn extract_epoch_ShortInterval(value: ShortInterval) -> i64 {
    value.microseconds() / 1_000_000
}

#[doc(hidden)]
pub fn extract_millisecond_ShortInterval(value: ShortInterval) -> i64 {
    (value.microseconds() % 60_000_000_i64) / 1000
}

#[doc(hidden)]
pub fn extract_microsecond_ShortInterval(value: ShortInterval) -> i64 {
    value.microseconds() % 60_000_000_i64
}

#[doc(hidden)]
pub fn extract_second_ShortInterval(value: ShortInterval) -> i64 {
    (value.microseconds() / 1_000_000) % 60
}

#[doc(hidden)]
pub fn extract_minute_ShortInterval(value: ShortInterval) -> i64 {
    (value.microseconds() / 60_000_000_i64) % 60
}

#[doc(hidden)]
pub fn extract_hour_ShortInterval(value: ShortInterval) -> i64 {
    (value.microseconds() / (60 * 60 * 1000 * 1000i64)) % 24
}

some_polymorphic_function1!(extract_year, LongInterval, LongInterval, i64);
some_polymorphic_function1!(extract_month, LongInterval, LongInterval, i64);
some_polymorphic_function1!(extract_quarter, LongInterval, LongInterval, i64);
some_polymorphic_function1!(extract_decade, LongInterval, LongInterval, i64);
some_polymorphic_function1!(extract_century, LongInterval, LongInterval, i64);
some_polymorphic_function1!(extract_millennium, LongInterval, LongInterval, i64);

some_polymorphic_function1!(extract_day, LongInterval, LongInterval, i64);
some_polymorphic_function1!(extract_epoch, LongInterval, LongInterval, i64);
some_polymorphic_function1!(extract_millisecond, LongInterval, LongInterval, i64);
some_polymorphic_function1!(extract_microsecond, LongInterval, LongInterval, i64);
some_polymorphic_function1!(extract_second, LongInterval, LongInterval, i64);
some_polymorphic_function1!(extract_minute, LongInterval, LongInterval, i64);
some_polymorphic_function1!(extract_hour, LongInterval, LongInterval, i64);

some_polymorphic_function1!(extract_day, ShortInterval, ShortInterval, i64);
some_polymorphic_function1!(extract_epoch, ShortInterval, ShortInterval, i64);
some_polymorphic_function1!(extract_millisecond, ShortInterval, ShortInterval, i64);
some_polymorphic_function1!(extract_microsecond, ShortInterval, ShortInterval, i64);
some_polymorphic_function1!(extract_second, ShortInterval, ShortInterval, i64);
some_polymorphic_function1!(extract_minute, ShortInterval, ShortInterval, i64);
some_polymorphic_function1!(extract_hour, ShortInterval, ShortInterval, i64);
