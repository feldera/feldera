//! Support for SQL interval types.
//! Intervals are differences between dates and/or times

use crate::{
    Date, MICROS_PER_DAY, MICROS_PER_HOUR, MICROS_PER_MINUTE, MICROS_PER_SECOND, SqlDecimal,
    operators::{eq, gt, gte, lt, lte, neq},
    plus_Date_Date_LongInterval__, sign, some_existing_operator, some_function1, some_function2,
    some_operator, some_polymorphic_function1, some_polymorphic_function2,
    some_polymorphic_null_function2,
    timestamp::{extract_epoch_Date, extract_quarter_Date},
};
use dbsp::{algebra::F64, num_entries_scalar};
use feldera_macros::{IsNone, OrdRepr};
use feldera_types::serde_with_context::{
    DeserializeWithContext, SerializeWithContext, SqlSerdeConfig,
};
use num_traits::cast::NumCast;
use serde::{Deserialize, Serialize, de::Error as _, ser::Error as _};
use size_of::SizeOf;
use std::fmt;
use std::ops::{Add, Div, Mul, Neg, Sub};

#[cfg(doc)]
use crate::{Time, Timestamp};

/// A ShortInterval can express a difference between two [Time]
/// values, two [Date] values, or two [Timestamp] values.  The
/// representation is a (positive or negative) number of microseconds.
/// While there are functions that can extract nanosecond-precision
/// values from a ShortInterval, the precision is always limited to
/// microseconds.
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
    OrdRepr,
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
    /// actually microseconds, so this function will always return a
    /// number that is a multiple of 1000.
    pub fn nanoseconds(&self) -> i64 {
        self.microseconds * 1_000
    }

    /// Extract the length of the interval in milliseconds.  The
    /// result can be negative.
    pub fn seconds(&self) -> i64 {
        self.microseconds / 1_000_000
    }

    /// Extract the length of the interval in minutes.  The
    /// result can be negative.
    pub fn minutes(&self) -> i64 {
        self.microseconds / (60 * 1_000_000)
    }

    /// Extract the length of the interval in minutes.  The
    /// result can be negative.
    pub fn hours(&self) -> i64 {
        self.microseconds / (60 * 60 * 1_000_000i64)
    }

    /// Extract the length of the interval in minutes.  The
    /// result can be negative.
    pub fn days(&self) -> i64 {
        self.microseconds / (24 * 60 * 60 * 1_000_000i64)
    }
}

/// Panic for an INTERVAL `ABS(value)` that overflows.
#[cold]
fn abs_failed(value: impl fmt::Display) -> ! {
    panic!("overflow in INTERVAL ABS({value})")
}

#[doc(hidden)]
pub fn abs_ShortInterval(value: ShortInterval) -> ShortInterval {
    let microseconds = value.microseconds.checked_abs();
    ShortInterval::from_microseconds(microseconds.unwrap_or_else(|| abs_failed(value)))
}

some_polymorphic_function1!(abs, ShortInterval, ShortInterval, ShortInterval);

#[doc(hidden)]
/// This function is used in rolling window computations, to compute a window bound.
/// Window bounds are always positive.
pub fn to_bound_ShortInterval_ShortInterval_u128(value: &ShortInterval) -> u128 {
    value.microseconds as u128
}

#[doc(hidden)]
/// This function is used in rolling window computations, to compute a window bound.
/// Window bounds are always positive.
pub fn to_bound_ShortInterval_Date_u128(value: &ShortInterval) -> u128 {
    // express value in days
    (value.microseconds / 1_000_000_i64 / 86400) as u128
}

#[doc(hidden)]
/// This function is used in rolling window computations, to compute a window bound.
/// Window bounds are always positive.
pub fn to_bound_ShortInterval_Date_u64(value: &ShortInterval) -> u64 {
    // express value in days
    (value.microseconds / 1_000_000_i64 / 86400) as u64
}

#[doc(hidden)]
/// This function is used in rolling window computations, to compute a window bound.
/// Window bounds are always positive.
pub fn to_bound_ShortInterval_Timestamp_u128(value: &ShortInterval) -> u128 {
    value.microseconds as u128
}

#[doc(hidden)]
/// This function is used in rolling window computations, to compute a window bound.
/// Window bounds are always positive.
pub fn to_bound_ShortInterval_Timestamp_u64(value: &ShortInterval) -> u64 {
    value.microseconds as u64
}

#[doc(hidden)]
/// This function is used in rolling window computations, to compute a window bound.
/// Window bounds are always positive.
pub fn to_bound_ShortInterval_Time_u128(value: &ShortInterval) -> u128 {
    // express value in nanoseconds
    (value.microseconds * 1_000) as u128
}

#[doc(hidden)]
/// This function is used in rolling window computations, to compute a window bound.
/// Window bounds are always positive.
pub fn to_bound_ShortInterval_Time_u64(value: &ShortInterval) -> u64 {
    // express value in nanoseconds
    (value.microseconds * 1_000) as u64
}

/// Panic for an INTERVAL multiplication `left * right` that overflows.
#[cold]
fn multiplication_failed(left: impl fmt::Display, right: impl fmt::Display) -> ! {
    panic!("overflow in INTERVAL multiplication {left} * {right}")
}

/// Panic for a `kind` INTERVAL `value` that does not fit in the DECIMAL that
/// `operation` uses.
#[cold]
fn decimal_conversion_failed(kind: &str, operation: &str, value: impl fmt::Display) -> ! {
    panic!("overflow in {kind} interval {operation} while converting {value} to DECIMAL")
}

/// Multiply a `ShortInterval` by a numeric value, producing a `ShortInterval`.
impl Mul<i64> for ShortInterval {
    type Output = Self;

    /// Multiply a short interval by an long integer
    fn mul(self, rhs: i64) -> Self {
        Self {
            microseconds: self
                .microseconds
                .checked_mul(rhs)
                .unwrap_or_else(|| multiplication_failed(self, rhs)),
        }
    }
}

/// Multiply a `ShortInterval` by a numeric value, producing a `ShortInterval`.
impl Mul<F64> for ShortInterval {
    type Output = Self;

    fn mul(self, rhs: F64) -> Self {
        let product = (F64::from(self.microseconds as f64) * rhs).into_inner();
        // Truncates; None for NaN, infinities and values out of range
        let microseconds = <i64 as NumCast>::from(product)
            .unwrap_or_else(|| multiplication_failed(self, rhs.into_inner()));
        Self { microseconds }
    }
}

/// Multiply a `ShortInterval` by a numeric value, producing a `ShortInterval`.
impl<const P: usize, const S: usize> Mul<SqlDecimal<P, S>> for ShortInterval {
    type Output = Self;

    fn mul(self, rhs: SqlDecimal<P, S>) -> Self {
        let us = SqlDecimal::<38, 0>::try_from(self.microseconds)
            .unwrap_or_else(|_| decimal_conversion_failed("short", "multiplication", self));
        let mul = us
            .checked_mul_generic::<P, S, 38, 0>(rhs)
            .unwrap_or_else(|| multiplication_failed(self, rhs));
        Self {
            microseconds: mul
                .try_into()
                .unwrap_or_else(|_| multiplication_failed(self, rhs)),
        }
    }
}

/// Panic for an INTERVAL division `left / right` that has no result.
#[cold]
fn division_failed(left: impl fmt::Display, right: impl fmt::Display, by_zero: bool) -> ! {
    if by_zero {
        panic!("division by zero in INTERVAL division {left} / {right}")
    }
    panic!("overflow in INTERVAL division {left} / {right}")
}

/// Panic for an INTERVAL addition `left + right` that overflows.
#[cold]
fn addition_failed(left: impl fmt::Display, right: impl fmt::Display) -> ! {
    panic!("overflow in INTERVAL addition {left} + {right}")
}

/// Panic for an INTERVAL subtraction `left - right` that overflows.
#[cold]
fn subtraction_failed(left: impl fmt::Display, right: impl fmt::Display) -> ! {
    panic!("overflow in INTERVAL subtraction {left} - {right}")
}

/// Panic for an INTERVAL negation `-value` that overflows.
#[cold]
fn negation_failed(value: impl fmt::Display) -> ! {
    panic!("overflow in INTERVAL negation -{value}")
}

/// Divide a `ShortInterval` by a numeric value, producing a `ShortInterval`.
impl Div<i64> for ShortInterval {
    type Output = Self;

    fn div(self, rhs: i64) -> Self {
        div_null_ShortInterval_i64(self, rhs)
            .unwrap_or_else(|| division_failed(self, rhs, rhs == 0))
    }
}

impl fmt::Display for ShortInterval {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (sign, magnitude) = self.into_sign_and_magnitude();
        let days = magnitude / MICROS_PER_DAY;
        let hours = (magnitude / MICROS_PER_HOUR) % 24;
        let minutes = (magnitude / MICROS_PER_MINUTE) % 60;
        let seconds = (magnitude / MICROS_PER_SECOND) % 60;
        let micro = magnitude % MICROS_PER_SECOND;
        if micro == 0 {
            write!(
                f,
                "{sign}{} {:02}:{:02}:{:02}",
                days, hours, minutes, seconds
            )
        } else {
            write!(
                f,
                "{sign}{} {:02}:{:02}:{:02}.{:06}",
                days, hours, minutes, seconds, micro
            )
        }
    }
}

/// Divide a `ShortInterval` by a numeric value, producing a `ShortInterval`.
impl Div<F64> for ShortInterval {
    type Output = Self;
    fn div(self, rhs: F64) -> Self {
        let divisor = rhs.into_inner();
        div_null_ShortInterval_d(self, rhs)
            .unwrap_or_else(|| division_failed(self, divisor, divisor == 0.0))
    }
}

/// Divide a `ShortInterval` by a numeric value, producing a `ShortInterval`.
impl<const P: usize, const S: usize> Div<SqlDecimal<P, S>> for ShortInterval {
    type Output = Self;

    fn div(self, rhs: SqlDecimal<P, S>) -> Self {
        let us = SqlDecimal::<38, 0>::try_from(self.microseconds)
            .unwrap_or_else(|_| decimal_conversion_failed("short", "division", self));
        let div = us
            .checked_div_generic::<P, S, 38, 0>(rhs)
            .unwrap_or_else(|| division_failed(self, rhs, rhs == SqlDecimal::<P, S>::ZERO));
        Self {
            microseconds: div
                .try_into()
                .unwrap_or_else(|_| division_failed(self, rhs, false)),
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
                .unwrap_or_else(|| addition_failed(self, rhs)),
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
                .unwrap_or_else(|| subtraction_failed(self, rhs)),
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
                .unwrap_or_else(|| negation_failed(self)),
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
    div [const P: usize, const S: usize],
    ShortInterval,
    ShortInterval,
    SqlDecimal,
    SqlDecimal<P, S>,
    ShortInterval
);

#[doc(hidden)]
pub fn div_null_ShortInterval_SqlDecimal<const P: usize, const S: usize>(
    left: ShortInterval,
    right: SqlDecimal<P, S>,
) -> Option<ShortInterval> {
    let us = SqlDecimal::<38, 0>::try_from(left.microseconds()).ok()?;
    let div = us.checked_div_generic::<P, S, 38, 0>(right)?;
    let micros: i64 = div.try_into().ok()?;
    Some(ShortInterval::from_microseconds(micros))
}

some_polymorphic_null_function2!(div_null [const P: usize, const S: usize], ShortInterval, ShortInterval, SqlDecimal, SqlDecimal<P, S>, ShortInterval);

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
    times [const P: usize, const S: usize],
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
pub fn div_null_ShortInterval_d(left: ShortInterval, right: F64) -> Option<ShortInterval> {
    let div = (left.microseconds() as f64) / right.into_inner();
    // Truncates; None for NaN, infinities and values out of range
    <i64 as NumCast>::from(div).map(ShortInterval::from_microseconds)
}

some_polymorphic_null_function2!(
    div_null,
    ShortInterval,
    ShortInterval,
    d,
    F64,
    ShortInterval
);

#[doc(hidden)]
pub fn div_ShortInterval_i64(left: ShortInterval, right: i64) -> ShortInterval {
    left / right
}

some_polymorphic_function2!(div, ShortInterval, ShortInterval, i64, i64, ShortInterval);

#[doc(hidden)]
pub fn div_null_ShortInterval_i64(left: ShortInterval, right: i64) -> Option<ShortInterval> {
    left.microseconds()
        .checked_div(right)
        .map(ShortInterval::from_microseconds)
}

some_polymorphic_null_function2!(
    div_null,
    ShortInterval,
    ShortInterval,
    i64,
    i64,
    ShortInterval
);

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
    OrdRepr,
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

    /// Retrieve the number of years in the long interval, rounded toward zero.
    pub fn years(&self) -> i32 {
        self.months / 12
    }
}

impl fmt::Display for LongInterval {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let months = self.months();
        let sign = sign(months < 0);
        let years = months.unsigned_abs() / 12;
        let months = months.unsigned_abs() % 12;
        write!(f, "{sign}{years}-{months:02}")
    }
}

#[doc(hidden)]
pub fn abs_LongInterval(value: LongInterval) -> LongInterval {
    let months = value.months.checked_abs();
    LongInterval::from_months(months.unwrap_or_else(|| abs_failed(value)))
}

some_polymorphic_function1!(abs, LongInterval, LongInterval, LongInterval);

/// Multiply a `LongInterval` by an integer producing a `LongInterval`
impl Mul<i32> for LongInterval {
    type Output = Self;

    fn mul(self, rhs: i32) -> Self {
        Self {
            months: self
                .months
                .checked_mul(rhs)
                .unwrap_or_else(|| multiplication_failed(self, rhs)),
        }
    }
}

/// Multiply a `LongInterval` by a numeric value producing a `LongInterval`
impl Mul<F64> for LongInterval {
    type Output = Self;

    fn mul(self, rhs: F64) -> Self {
        let product = (F64::from(self.months as f64) * rhs).into_inner();
        // Truncates; None for NaN, infinities and values out of range
        let months = <i32 as NumCast>::from(product)
            .unwrap_or_else(|| multiplication_failed(self, rhs.into_inner()));
        Self { months }
    }
}

/// Multiply a `LongInterval` by a numeric value producing a `LongInterval`
impl<const P: usize, const S: usize> Mul<SqlDecimal<P, S>> for LongInterval {
    type Output = Self;

    fn mul(self, rhs: SqlDecimal<P, S>) -> Self {
        let months = SqlDecimal::<10, 0>::try_from(self.months)
            .unwrap_or_else(|_| decimal_conversion_failed("long", "multiplication", self));
        let mul = months
            .checked_mul_generic::<P, S, 10, 0>(rhs)
            .unwrap_or_else(|| multiplication_failed(self, rhs));
        Self {
            months: mul
                .try_into()
                .unwrap_or_else(|_| multiplication_failed(self, rhs)),
        }
    }
}

/// Divide a `LongInterval` by an integer producing a `LongInterval`
impl Div<i32> for LongInterval {
    type Output = Self;

    fn div(self, rhs: i32) -> Self {
        div_null_LongInterval_i32(self, rhs).unwrap_or_else(|| division_failed(self, rhs, rhs == 0))
    }
}

/// Divide a `LongInterval` by a numeric value producing a `LongInterval`
impl Div<F64> for LongInterval {
    type Output = Self;

    fn div(self, rhs: F64) -> Self {
        let divisor = rhs.into_inner();
        div_null_LongInterval_d(self, rhs)
            .unwrap_or_else(|| division_failed(self, divisor, divisor == 0.0))
    }
}

/// Divide a `LongInterval` by a numeric value producing a `LongInterval`
impl<const P: usize, const S: usize> Div<SqlDecimal<P, S>> for LongInterval {
    type Output = Self;

    fn div(self, rhs: SqlDecimal<P, S>) -> Self {
        let months = SqlDecimal::<10, 0>::try_from(self.months)
            .unwrap_or_else(|_| decimal_conversion_failed("long", "division", self));
        let div = months
            .checked_div_generic::<P, S, 10, 0>(rhs)
            .unwrap_or_else(|| division_failed(self, rhs, rhs == SqlDecimal::<P, S>::ZERO));
        Self {
            months: div
                .try_into()
                .unwrap_or_else(|_| division_failed(self, rhs, false)),
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
                .unwrap_or_else(|| addition_failed(self, rhs)),
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
                .unwrap_or_else(|| subtraction_failed(self, rhs)),
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
                .unwrap_or_else(|| negation_failed(self)),
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
    times [const P: usize, const S: usize],
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
pub fn div_null_LongInterval_i32(left: LongInterval, right: i32) -> Option<LongInterval> {
    left.months
        .checked_div(right)
        .map(LongInterval::from_months)
}

some_polymorphic_null_function2!(div_null, LongInterval, LongInterval, i32, i32, LongInterval);

#[doc(hidden)]
pub fn div_LongInterval_d(left: LongInterval, right: F64) -> LongInterval {
    left / right
}

some_polymorphic_function2!(div, LongInterval, LongInterval, d, F64, LongInterval);

#[doc(hidden)]
pub fn div_null_LongInterval_d(left: LongInterval, right: F64) -> Option<LongInterval> {
    let div: f64 = (left.months() as f64) / right.into_inner();
    // Truncates; None for NaN, infinities and values out of range
    <i32 as NumCast>::from(div).map(LongInterval::from_months)
}

some_polymorphic_null_function2!(div_null, LongInterval, LongInterval, d, F64, LongInterval);

#[doc(hidden)]
pub fn div_LongInterval_SqlDecimal<const P: usize, const S: usize>(
    left: LongInterval,
    right: SqlDecimal<P, S>,
) -> LongInterval {
    left / right
}

some_polymorphic_function2!(
    div [const P: usize, const S: usize],
    LongInterval,
    LongInterval,
    SqlDecimal,
    SqlDecimal<P, S>,
    LongInterval
);

#[doc(hidden)]
pub fn div_null_LongInterval_SqlDecimal<const P: usize, const S: usize>(
    left: LongInterval,
    right: SqlDecimal<P, S>,
) -> Option<LongInterval> {
    let m = SqlDecimal::<38, 0>::try_from(left.months()).ok()?;
    let div = m.checked_div_generic::<P, S, 38, 0>(right)?;
    let months: i32 = div.try_into().ok()?;
    Some(LongInterval::from_months(months))
}

some_polymorphic_null_function2!(div_null [const P: usize, const S: usize], LongInterval, LongInterval, SqlDecimal, SqlDecimal<P, S>, LongInterval);

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

#[doc(hidden)]
pub fn long_interval_to_integer_(value: LongInterval) -> i32 {
    value.months
}

some_function1!(long_interval_to_integer, LongInterval, i32);

#[doc(hidden)]
pub fn integer_to_long_interval_(value: i32) -> LongInterval {
    LongInterval::from_months(value)
}

some_function1!(integer_to_long_interval, i32, LongInterval);

///////////

#[doc(hidden)]
pub fn short_interval_to_integer_(value: ShortInterval) -> i64 {
    value.microseconds
}

some_function1!(short_interval_to_integer, ShortInterval, i64);

#[doc(hidden)]
pub fn integer_to_short_interval_(value: i64) -> ShortInterval {
    ShortInterval::from_microseconds(value)
}

some_function1!(integer_to_short_interval, i64, ShortInterval);

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

// Hashing the archived form the way the decoded one hashes; see
// `crate::hash_repr`.
crate::hash_repr::hash_repr_struct! {
    ShortInterval => ArchivedShortInterval { microseconds: i64 },
    LongInterval => ArchivedLongInterval { months: i32 },
}

#[cfg(test)]
mod test {
    use super::{
        LongInterval, ShortInterval, abs_LongInterval, abs_ShortInterval, div_null_LongInterval_d,
        div_null_ShortInterval_d,
    };
    use crate::SqlDecimal;
    use dbsp::algebra::F64;

    #[test]
    #[should_panic(expected = "overflow in INTERVAL multiplication +106751991 04:00:54.775807 * 2")]
    fn short_interval_times_integer_overflow() {
        let _ = ShortInterval::from_microseconds(i64::MAX) * 2;
    }

    #[test]
    #[should_panic(expected = "overflow in INTERVAL multiplication +106751991 04:00:54.775807 * 2")]
    fn short_interval_times_double_overflow() {
        let _ = ShortInterval::from_microseconds(i64::MAX) * F64::new(2.0);
    }

    #[test]
    #[should_panic(expected = "overflow in INTERVAL multiplication +0 00:00:01 * NaN")]
    fn short_interval_times_nan() {
        let _ = ShortInterval::from_microseconds(1_000_000) * F64::new(f64::NAN);
    }

    #[test]
    #[should_panic(expected = "overflow in INTERVAL multiplication +178956970-07 * 2")]
    fn long_interval_times_integer_overflow() {
        let _ = LongInterval::from_months(i32::MAX) * 2;
    }

    #[test]
    #[should_panic(expected = "overflow in INTERVAL multiplication +178956970-07 * 2")]
    fn long_interval_times_double_overflow() {
        let _ = LongInterval::from_months(i32::MAX) * F64::new(2.0);
    }

    #[test]
    #[should_panic(
        expected = "overflow in INTERVAL addition +106751991 04:00:54.775807 + +0 00:00:00.000001"
    )]
    fn short_interval_addition_overflow() {
        let _ = ShortInterval::from_microseconds(i64::MAX) + ShortInterval::from_microseconds(1);
    }

    #[test]
    #[should_panic(expected = "overflow in INTERVAL ABS(-178956970-08)")]
    fn most_negative_long_interval_abs() {
        let _ = abs_LongInterval(LongInterval::from_months(i32::MIN));
    }

    #[test]
    #[should_panic(expected = "overflow in INTERVAL ABS(-106751991 04:00:54.775808)")]
    fn most_negative_short_interval_abs() {
        let _ = abs_ShortInterval(ShortInterval::from_microseconds(i64::MIN));
    }

    #[test]
    fn long_interval_years() {
        assert_eq!(LongInterval::from_months(13).years(), 1);
        assert_eq!(LongInterval::from_months(-13).years(), -1);
        assert_eq!(LongInterval::from_months(i32::MAX).years(), 178956970);
        assert_eq!(LongInterval::from_months(i32::MIN).years(), -178956970);
    }

    #[test]
    #[should_panic(expected = "overflow in INTERVAL subtraction -178956970-08 - +0-01")]
    fn long_interval_subtraction_overflow() {
        let _ = LongInterval::from_months(i32::MIN) - LongInterval::from_months(1);
    }

    #[test]
    #[should_panic(expected = "overflow in INTERVAL negation --106751991 04:00:54.775808")]
    fn most_negative_short_interval_negation() {
        let _ = -ShortInterval::from_microseconds(i64::MIN);
    }

    #[test]
    #[should_panic(expected = "division by zero in INTERVAL division +0 00:00:01 / 0")]
    fn short_interval_divided_by_zero() {
        let _ = ShortInterval::from_microseconds(1_000_000) / 0;
    }

    #[test]
    #[should_panic(expected = "division by zero in INTERVAL division +0 00:00:01 / 0")]
    fn short_interval_divided_by_double_zero() {
        let _ = ShortInterval::from_microseconds(1_000_000) / F64::new(0.0);
    }

    #[test]
    #[should_panic(expected = "overflow in INTERVAL division -106751991 04:00:54.775808 / -1")]
    fn most_negative_short_interval_divided_by_minus_one() {
        let _ = ShortInterval::from_microseconds(i64::MIN) / -1;
    }

    #[test]
    #[should_panic(expected = "overflow in INTERVAL division -106751991 04:00:54.775808 / -1")]
    fn most_negative_short_interval_divided_by_double_minus_one() {
        let _ = ShortInterval::from_microseconds(i64::MIN) / F64::new(-1.0);
    }

    #[test]
    #[should_panic(expected = "division by zero in INTERVAL division +1-00 / 0")]
    fn long_interval_divided_by_zero() {
        let _ = LongInterval::from_months(12) / 0;
    }

    #[test]
    #[should_panic(expected = "division by zero in INTERVAL division +1-00 / 0")]
    fn long_interval_divided_by_decimal_zero() {
        let _ = LongInterval::from_months(12) / SqlDecimal::<10, 0>::ZERO;
    }

    #[test]
    fn interval_times_double_at_the_limits() {
        let min = ShortInterval::from_microseconds(i64::MIN);
        assert_eq!(min * F64::new(1.0), min);
        let max = LongInterval::from_months(i32::MAX);
        assert_eq!(max * F64::new(1.0), max);
        // The product truncates towards zero
        assert_eq!(
            LongInterval::from_months(-3) * F64::new(0.5),
            LongInterval::from_months(-1)
        );
    }

    #[test]
    fn interval_divided_by_double_at_the_limits() {
        // 2^63 microseconds does not fit
        let min = ShortInterval::from_microseconds(i64::MIN);
        assert_eq!(div_null_ShortInterval_d(min, F64::new(-1.0)), None);
        assert_eq!(div_null_ShortInterval_d(min, F64::new(1.0)), Some(min));
        // The quotient truncates towards zero
        let max = LongInterval::from_months(i32::MAX);
        assert_eq!(
            div_null_LongInterval_d(max, F64::new(0.9999999999)),
            Some(max)
        );
        assert_eq!(div_null_LongInterval_d(max, F64::new(0.5)), None);
    }

    #[test]
    fn display_extreme_short_intervals() {
        assert_eq!(
            ShortInterval::from_microseconds(i64::MIN).to_string(),
            "-106751991 04:00:54.775808"
        );
        assert_eq!(
            ShortInterval::from_microseconds(i64::MAX).to_string(),
            "+106751991 04:00:54.775807"
        );
    }
}
