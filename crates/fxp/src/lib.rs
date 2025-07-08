use std::{
    cmp::Ordering,
    fmt::{Debug, Display},
    io::Write,
    num::IntErrorKind,
    ops::{Add, AddAssign, Div, DivAssign, Mul, MulAssign, Neg, Sub, SubAssign},
    str::FromStr,
};

use num_traits::{cast, CheckedAdd, CheckedDiv, CheckedMul, CheckedSub, One, Zero};
use smallvec::{Array, SmallVec};

use crate::u256::I256;

#[cfg(feature = "dbsp")]
mod dbsp_impl;

#[cfg(feature = "serde")]
mod serde_impl;
mod u256;

#[cfg(feature = "rkyv")]
mod rkyv_impl;

/// Fixed-point decimal with fixed precision and scale.
///
/// `Fixed<P, S>`, where `P` in `1..=38` is the "precision" and `S` in `0..=P`
/// is the "scale", represents a signed decimal number in which `S - P` digits
/// precede the decimal point and `S` digits follow it.  The table below shows
/// the maximum values for a few combinations of `P` and `S`.  For each type,
/// the minimum value is the negation of the maximum:
///
/// |          Type |                                              Maximum Value |
/// |:--------------|-----------------------------------------------------------:|
/// | `Fixed<5,5>`  | `                                                 0.99999` |
/// | `Fixed<5,4>`  | `                                                 9.9999 ` |
/// | `Fixed<5,3>`  | `                                                99.999  ` |
/// | `Fixed<5,2>`  | `                                               999.99   ` |
/// | `Fixed<5,1>`  | `                                             9,999.9    ` |
/// | `Fixed<5,0>`  | `                                            99,999      ` |
/// | `Fixed<38,0>` | `99,999,999,999,999,999,999,999,999,999,999,999,999      ` |
/// | `Fixed<38,5>` | `       999,999,999,999,999,999,999,999,999,999,999.99999` |
///
/// # Implementation
///
/// `Fixed<P, S>` internally contains a single `i128` that represents a value
/// `x` as `x * 10**P`.  This limits `S` to 38 because `10**38 ≤ 2**127 - 1 <
/// 10**39`.  A single `i64` would be sufficient for `S ≤ 18`, and a single
/// `i32` for `S ≤ 9`, but the implementation does not optimize for those cases.
#[derive(Copy, Clone, Default, Eq, Ord, Hash)]
#[cfg_attr(feature = "size_of", derive(size_of::SizeOf))]
pub struct Fixed<const P: usize, const S: usize>(i128);

impl<const P0: usize, const S0: usize, const P1: usize, const S1: usize> PartialEq<Fixed<P1, S1>>
    for Fixed<P0, S0>
{
    fn eq(&self, other: &Fixed<P1, S1>) -> bool {
        match S0.cmp(&S1) {
            Ordering::Less => I256::from_product(self.0, pow10(S1 - S0)) == I256::from(other.0),
            Ordering::Equal => self.0 == other.0,
            Ordering::Greater => I256::from(self.0) == I256::from_product(other.0, pow10(S0 - S1)),
        }
    }
}

macro_rules! partial_eq_int {
    ($type_name:ty) => {
        impl<const P0: usize, const S0: usize> PartialEq<$type_name> for Fixed<P0, S0> {
            fn eq(&self, other: &$type_name) -> bool {
                self.0 % Self::scale() == 0 && self.0 / Self::scale() == *other as i128
            }
        }
    };
}
partial_eq_int!(i8);
partial_eq_int!(i16);
partial_eq_int!(i32);
partial_eq_int!(i64);
partial_eq_int!(i128);
partial_eq_int!(isize);
partial_eq_int!(u8);
partial_eq_int!(u16);
partial_eq_int!(u32);
partial_eq_int!(u64);

impl<const P0: usize, const S0: usize> PartialEq<u128> for Fixed<P0, S0> {
    fn eq(&self, other: &u128) -> bool {
        self.0 >= 0
            && self.0 % Self::scale() == 0
            && (self.0 / Self::scale()).cast_unsigned() == *other
    }
}

impl<const P0: usize, const S0: usize> PartialEq<usize> for Fixed<P0, S0> {
    fn eq(&self, other: &usize) -> bool {
        self.0 >= 0
            && self.0 % Self::scale() == 0
            && (self.0 / Self::scale()).cast_unsigned() == *other as u128
    }
}

#[inline(never)]
pub fn eq_i32(f: Fixed<38, 0>, x: i32) -> bool {
    f == x
}

impl<const P0: usize, const S0: usize, const P1: usize, const S1: usize> PartialOrd<Fixed<P1, S1>>
    for Fixed<P0, S0>
{
    fn partial_cmp(&self, other: &Fixed<P1, S1>) -> Option<Ordering> {
        match S0.cmp(&S1) {
            Ordering::Less => {
                I256::from_product(self.0, pow10(S1 - S0)).partial_cmp(&I256::from(other.0))
            }
            Ordering::Equal => self.0.partial_cmp(&other.0),
            Ordering::Greater => {
                I256::from(self.0).partial_cmp(&I256::from_product(other.0, pow10(S0 - S1)))
            }
        }
    }
}

/// A maximum-precision `Fixed` with no decimal places.
pub type FixedInteger = Fixed<38, 0>;

impl<const P: usize, const S: usize> Debug for Fixed<P, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if S == 0 {
            write!(f, "{}", self.0)
        } else if self.0 % Self::scale() == 0 {
            write!(f, "{}", self.0 / Self::scale())
        } else {
            write!(
                f,
                "{}{}.{}",
                if self.0 < 0 { "-" } else { "" },
                self.0.abs() / Self::scale(),
                (self.0.abs() % Self::scale()).abs()
            )
        }
    }
}

impl<const P: usize, const S: usize> Display for Fixed<P, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut buf = SmallVec::<[u8; 64]>::new();
        write!(&mut buf, "{:01$}", self.0.abs(), S + 1).unwrap();
        debug_assert!(buf.len() > S);
        let decimals = if let Some(precision) = f.precision() {
            match precision.cmp(&S) {
                Ordering::Less => {
                    let new_len = buf.len() - (S - precision);
                    let mut discard = buf[new_len..].iter();
                    enum Rounding {
                        Up,
                        Down,
                        Even,
                    }
                    impl Rounding {
                        fn round<A>(&self, s: &mut SmallVec<A>)
                        where
                            A: Array<Item = u8>,
                        {
                            let round_up = match self {
                                Rounding::Down => false,
                                Rounding::Up => true,
                                Rounding::Even => s.last().unwrap() % 2 == 1,
                            };
                            if round_up {
                                let mut nines = 0;
                                let c = loop {
                                    match s.pop() {
                                        Some(b'9') => nines += 1,
                                        Some(c) => break c,
                                        None => break b'0',
                                    }
                                };
                                s.push(c + 1);
                                for _ in 0..nines {
                                    s.push(b'0');
                                }
                            }
                        }
                    }
                    let rounding = match discard.next().unwrap() {
                        b'0'..=b'4' => Rounding::Down,
                        b'5' => loop {
                            match discard.next() {
                                Some(b'0') => (),
                                Some(_) => break Rounding::Up,
                                None => break Rounding::Even,
                            }
                        },
                        b'6'..=b'9' => Rounding::Up,
                        _ => unreachable!(),
                    };
                    buf.truncate(new_len);
                    rounding.round(&mut buf);
                }
                Ordering::Equal => (),
                Ordering::Greater => {
                    for _ in S..precision {
                        buf.push(b'0');
                    }
                }
            }
            precision
        } else {
            let mut decimals = S;
            while decimals > 0 && buf.ends_with(b"0") {
                buf.pop();
                decimals -= 1;
            }
            decimals
        };
        if decimals > 0 {
            buf.insert(buf.len() - decimals, b'.');
        }

        // SAFETY: `buf` contains only ASCII characters.
        f.pad_integral(self.0 >= 0, "", unsafe { str::from_utf8_unchecked(&buf) })
    }
}

#[derive(Copy, Clone, Debug)]
pub struct OutOfRange;

/// Returns `10**exponent`, or `None` if `exponent > 38` (because the result
/// would be greater than `i128::MAX`).
const fn checked_pow10(exponent: u32) -> Option<i128> {
    10i128.checked_pow(exponent)
}

/// Returns `10**exponent`.
///
/// # Panic
///
/// Panics if `exponent > 38` (because the result would be greater than
/// `i128::MAX`).
const fn pow10(exponent: usize) -> i128 {
    10i128.checked_pow(exponent as u32).unwrap()
}

/// How to round values halfway between two integer.
#[derive(Copy, Clone, PartialEq, Eq)]
enum Halfway {
    /// Round away from zero.
    AwayFromZero,

    /// Round to even.
    Even,
}

fn round_inner(value: i128, scale: i32, n: i32, halfway: Halfway) -> Option<i128> {
    let position = scale.saturating_sub(n);
    if position <= 0 {
        Some(value)
    } else if position < scale {
        let divisor = pow10(position as usize);
        let quotient = value / divisor;
        let remainder = value % divisor;
        let round_away_from_zero = match remainder.abs().cmp(&(divisor / 2)) {
            Ordering::Less => false,
            Ordering::Equal => match halfway {
                Halfway::AwayFromZero => true,
                Halfway::Even => (quotient % 2) != 0,
            },
            Ordering::Greater => true,
        };
        let rounded_quotient = if round_away_from_zero {
            quotient + quotient.signum()
        } else {
            quotient
        };
        Some(divisor * rounded_quotient)
    } else if position > scale || value.abs() >= 5 * pow10(scale as usize - 1) {
        Some(0)
    } else {
        None
    }
}

/// Returns `floor(x / y)`.  This is copied out of `i128::div_floor` in the
/// standard library, which is not yet stable.
const fn div_floor(x: i128, y: i128) -> i128 {
    let d = x / y;
    let r = x % y;

    // If the remainder is non-zero, we need to subtract one if the
    // signs of lhs and rhs differ, as this means we rounded upwards
    // instead of downwards. We do this branchlessly by creating a mask
    // which is all-ones iff the signs differ, and 0 otherwise. Then by
    // adding this mask (which corresponds to the signed value -1), we
    // get our correction.
    let correction = (x ^ y) >> (i128::BITS - 1);
    if r != 0 {
        d + correction
    } else {
        d
    }
}

/// Returns `ceil(x / y)`.  This is copied out of `i128::div_ceil` in the
/// standard library, which is not yet stable.
const fn div_ceil(x: i128, y: i128) -> i128 {
    let d = x / y;
    let r = x % y;

    // When remainder is non-zero we have a.div_ceil(b) == 1 + a.div_floor(b),
    // so we can re-use the algorithm from div_floor, just adding 1.
    let correction = 1 + ((x ^ y) >> (i128::BITS - 1));
    if r != 0 {
        d + correction
    } else {
        d
    }
}

impl<const P: usize, const S: usize> Fixed<P, S> {
    /// Largest value for this type, e.g. 999.99 for `Fixed<5,2>`.
    pub const MAX: Self = Self(pow10(P) - 1);

    /// Smallest value for this type, e.g. -999.99 for `Fixed<5,2>`.
    ///
    /// `MIN` is always `-MAX`.
    pub const MIN: Self = Self(-Self::MAX.0);

    /// Zero in this type.
    pub const ZERO: Self = Self(0);

    /// 1 in this type.
    ///
    /// # Panic
    ///
    /// If `S == P`, this is undefined because 1 is not a value in this type,
    /// and referring to it yields a compile-time error.
    pub const ONE: Self = {
        if S < P {
            Self(pow10(S))
        } else {
            panic!("all values of Fixed::<S,P>::one() for S >= P have magnitude less than one");
        }
    };

    /// Returns `value` in this type.
    ///
    /// # Panic
    ///
    /// Panics if this type cannot hold every `i64` value.
    pub const fn for_i64(value: i64) -> Self {
        assert!(P.saturating_sub(S) >= 19);
        Self(value as i128 * Self::scale())
    }

    /// Returns `value` in this type.
    ///
    /// # Panic
    ///
    /// Panics if this type cannot hold every `u64` value.
    pub const fn for_u64(value: u64) -> Self {
        assert!(P.saturating_sub(S) >= 19);
        Self(value as i128 * Self::scale())
    }

    /// Returns `value` in this type.
    ///
    /// # Panic
    ///
    /// Panics if this type cannot hold every `i32` value.
    pub const fn for_i32(value: i32) -> Self {
        assert!(P.saturating_sub(S) >= 10);
        Self(value as i128 * Self::scale())
    }

    /// Returns `value` in this type.
    ///
    /// # Panic
    ///
    /// Panics if this type cannot hold every `u32` value.
    pub const fn for_u32(value: u32) -> Self {
        assert!(P.saturating_sub(S) >= 10);
        Self(value as i128 * Self::scale())
    }

    /// Returns `value` in this type.
    ///
    /// # Panic
    ///
    /// Panics if this type cannot hold every `i16` value.
    pub const fn for_i16(value: i16) -> Self {
        assert!(P.saturating_sub(S) >= 5);
        Self(value as i128 * Self::scale())
    }

    /// Returns `value` in this type.
    ///
    /// # Panic
    ///
    /// Panics if this type cannot hold every `u16` value.
    pub const fn for_u16(value: u16) -> Self {
        assert!(P.saturating_sub(S) >= 5);
        Self(value as i128 * Self::scale())
    }

    /// Returns `value` in this type.
    ///
    /// # Panic
    ///
    /// Panics if this type cannot hold every `i8` value.
    pub const fn for_i8(value: i8) -> Self {
        assert!(P.saturating_sub(S) >= 3);
        Self(value as i128 * Self::scale())
    }

    /// Returns `value` in this type.
    ///
    /// # Panic
    ///
    /// Panics if this type cannot hold every `u8` value.
    pub const fn for_u8(value: u8) -> Self {
        assert!(P.saturating_sub(S) >= 3);
        Self(value as i128 * Self::scale())
    }

    /// Returns `value` in this type.
    ///
    /// # Panic
    ///
    /// Panics if this type cannot hold every `isize` value.
    pub const fn for_isize(value: isize) -> Self {
        match isize::BITS {
            64 => Self::for_i64(value as i64),
            32 => Self::for_i32(value as i32),
            16 => Self::for_i16(value as i16),
            _ => panic!(),
        }
    }

    /// Returns `value` in this type.
    ///
    /// # Panic
    ///
    /// Panics if this type cannot hold every `usize` value.
    pub const fn for_usize(value: usize) -> Self {
        match usize::BITS {
            64 => Self::for_u64(value as u64),
            32 => Self::for_u32(value as u32),
            16 => Self::for_u16(value as u16),
            _ => panic!(),
        }
    }

    /// Returns `Self(value)`, if `value` is in the correct range for this type.
    fn try_new(value: i128) -> Option<Self> {
        const { Self::check_constraints() };
        (Self::MIN.0..=Self::MAX.0)
            .contains(&value)
            .then_some(Self(value))
    }

    /// Returns `Self(value * 10**exponent)`, rounding to even if `exponent` is
    /// negative, if the computed value is in the correct range for the type.
    fn try_new_with_exponent_round_even(value: i128, exponent: i32) -> Option<Self> {
        // Non-generic inner function to reduce monomorphization cost.
        fn inner(value: i128, exponent: i32) -> Option<i128> {
            Some(match exponent.cmp(&0) {
                Ordering::Less => {
                    // Divide by a negative exponent.
                    if let Some(divisor) = checked_pow10(exponent.unsigned_abs()) {
                        // Round toward even.
                        //
                        // For negative `x` and positive `y`, `x / y` rounds toward 0
                        // and `x % y` is zero or negative.
                        debug_assert!(divisor >= 2);
                        let quotient = value / divisor;
                        let remainder = value % divisor;
                        let round_away_from_zero = match remainder.abs().cmp(&(divisor / 2)) {
                            Ordering::Less => false,
                            Ordering::Equal => (quotient % 2) != 0,
                            Ordering::Greater => true,
                        };
                        if round_away_from_zero {
                            quotient + quotient.signum()
                        } else {
                            quotient
                        }
                    } else {
                        // `10**-exponent` is greater than `i128::MAX`.  The result
                        // must be zero.
                        0
                    }
                }
                Ordering::Equal => value,
                Ordering::Greater => {
                    // Multiply by a positive exponent.
                    value.checked_mul(checked_pow10(exponent.cast_unsigned())?)?
                }
            })
        }
        inner(value, exponent).and_then(Self::try_new)
    }

    /// Returns `Self(value * 10**exponent)`, rounding toward zero, if the
    /// computed value is in the correct range for the type.
    fn try_new_with_exponent(value: i128, exponent: i32) -> Option<Self> {
        // Non-generic inner function to reduce monomorphization cost.
        fn inner(value: i128, exponent: i32) -> Option<i128> {
            Some(match exponent.cmp(&0) {
                Ordering::Less => {
                    // Divide by a negative exponent.
                    if let Some(divisor) = checked_pow10(exponent.unsigned_abs()) {
                        value / divisor
                    } else {
                        // `10**-exponent` is greater than `i128::MAX`.  The result
                        // must be zero.
                        0
                    }
                }
                Ordering::Equal => value,
                Ordering::Greater => {
                    // Multiply by a positive exponent.
                    value.checked_mul(checked_pow10(exponent.cast_unsigned())?)?
                }
            })
        }
        inner(value, exponent).and_then(Self::try_new)
    }

    /// Validates the constraints on `S` and `P`.
    const fn check_constraints() {
        assert!(P >= 1 && P <= 38, "Fixed<S,P> must have 1 <= S <= 38");
        assert!(S <= P, "Fixed<S,P> must have S <= P");
    }

    /// Returns `pow10(S)`.
    const fn scale() -> i128 {
        Self::check_constraints();
        pow10(S)
    }

    /// Integer division, as defined for `divide-integer` in [General Decimal
    /// Arithmetic].  Returns `None` if `other` is zero or the result is greater
    /// than `i128::MAX`.
    ///
    /// [General Decimal Arithmetic]: https://speleotrove.com/decimal/decarith.pdf
    pub const fn checked_div_integer(self, other: Self) -> Option<i128> {
        self.0.checked_div(other.0)
    }

    /// Integer division like [checked_div_integer](Self::checked_div_integer),
    /// but panic on error.
    ///
    /// # Panic
    ///
    /// Panics if `other` is zero or the result is greater than `i128::MAX`.
    pub const fn strict_div_integer(self, other: Self) -> i128 {
        self.checked_div_integer(other).unwrap()
    }

    /// Remainder, as defined for `remainder` in [General Decimal Arithmetic].
    /// Returns `None` if `other` is zero.
    ///
    /// [General Decimal Arithmetic]: https://speleotrove.com/decimal/decarith.pdf
    pub const fn checked_rem_integer(self, _other: Self) -> Option<Self> {
        todo!()
    }

    /// Integer remainder like [checked_rem_integer](Self::checked_rem_integer),
    /// but panic on error.
    ///
    /// # Panic
    ///
    /// Panics if `other` is zero.
    pub const fn strict_rem_integer(self, other: Self) -> Self {
        self.checked_rem_integer(other).unwrap()
    }

    /// Returns the absolute value.  This is an exact calculation that cannot
    /// overflow.
    pub const fn abs(self) -> Self {
        Self(self.0.abs())
    }

    /// Returns true if this value is negative, false if it is zero or positive.
    pub const fn is_negative(self) -> bool {
        self.0.is_negative()
    }

    /// Returns the square root of this value, rounded down, or `None` if this
    /// value is negative.
    ///
    /// It probably makes more sense to convert to `f64` and take the
    /// floating-point square root.
    pub fn checked_sqrt(self) -> Option<Self> {
        Some(Self(
            I256::from_product(self.0, Self::scale()).checked_isqrt()?,
        ))
    }

    /// Returns the square root of this value, rounded down.
    ///
    /// It probably makes more sense to convert to `f64` and take the
    /// floating-point square root.
    ///
    /// # Panic
    ///
    /// Panics if this value is negative.
    pub fn sqrt(self) -> Self {
        self.checked_sqrt().unwrap()
    }

    /// Returns this value rounded to `n` digits after the decimal point, or
    /// `None` if rounding caused overflow, `n` may be negative.
    ///
    /// If the value is halfway between two integers, rounds away from zero.
    pub fn checked_round(&self, n: i32) -> Option<Self> {
        round_inner(self.0, S as i32, n, Halfway::AwayFromZero).and_then(Self::try_new)
    }

    /// Rounds to `n` digits after the decimal point, like [checked_round].
    /// If the value is halfway between two integers, rounds away from zero.
    ///
    /// # Panic
    ///
    /// Panics if rounding causes overflow.
    ///
    /// [checked_round]: Self::checked_round
    pub fn round(&self, n: i32) -> Self {
        self.checked_round(n).unwrap()
    }

    /// Returns this value rounded to `n` digits after the decimal point, or
    /// `None` if rounding caused overflow, `n` may be negative.  If the value
    /// is halfway between two integers, rounds toward an even least significant
    /// digit.
    pub fn checked_round_ties_even(&self, n: i32) -> Option<Self> {
        round_inner(self.0, S as i32, n, Halfway::Even).and_then(Self::try_new)
    }

    /// Rounds to `n` digits after the decimal point, like
    /// [checked_round_ties_even].  If the value is halfway between two
    /// integers, rounds toward an even least significant digit.
    ///
    /// # Panic
    ///
    /// Panics if rounding causes overflow.
    ///
    /// [checked_round_ties_even]: Self::checked_round_ties_even
    pub fn round_ties_even(&self, n: i32) -> Self {
        self.checked_round_ties_even(n).unwrap()
    }

    /// Returns this value rounded down to the nearest integer, or `None` if
    /// rounding caused overflow.
    pub fn checked_floor(&self) -> Option<Self> {
        if S > 0 {
            Self::try_new(div_floor(self.0, Self::scale()) * Self::scale())
        } else {
            Some(*self)
        }
    }

    /// Rounds down to the nearest integer, like [checked_floor].
    ///
    /// # Panic
    ///
    /// Panics if rounding causes overflow.
    ///
    /// [checked_floor]: Self::checked_floor
    pub fn floor(&self) -> Self {
        self.checked_floor().unwrap()
    }

    /// Returns the integer part of this value, truncating non-integers toward
    /// zero.  This is an exact calculation that cannot overflow.
    pub fn truncate(&self) -> Self {
        Self(self.0 / Self::scale() * Self::scale())
    }

    /// Returns this value rounded up to the nearest integer, or `None` if
    /// rounding caused overflow.
    pub fn checked_ceil(&self) -> Option<Self> {
        if S > 0 {
            Self::try_new(div_ceil(self.0, Self::scale()) * Self::scale())
        } else {
            Some(*self)
        }
    }

    /// Rounds up to the nearest integer, like [checked_ceil].
    ///
    /// # Panic
    ///
    /// Panics if rounding causes overflow.
    ///
    /// [checked_ceil]: Self::checked_ceil
    pub fn ceil(&self) -> Self {
        self.checked_ceil().unwrap()
    }

    /// Returns -1 if this value is less than zero, 0 if this value is zero, and
    /// 1 if this value is greater than zero, as `Fixed<1,0>`.
    pub fn sign(&self) -> Fixed<1, 0> {
        self.checked_sign_generic().unwrap()
    }

    /// Returns the reciprocal (inverse) of this value, `1/x`, or `None` if `x`
    /// is zero or `1/x` is out of range.
    pub fn checked_recip(&self) -> Option<Self> {
        if S < P {
            Self(Self::scale()).checked_div(self)
        } else {
            // `1` is out of range for this type, therefore `abs(self) < 1`,
            // therefore `abs(1/self) > 1`, therefore the result is out of
            // range.
            None
        }
    }

    /// Returns the reciprocal (inverse) of this value, `1/x`.  This works even
    /// if `1` is out of range for this type, as long as `1/x` is in range.
    ///
    /// # Panic
    ///
    /// Panics if `x` is zero or `1/x` is out of range.
    pub fn recip(&self) -> Self {
        self.checked_recip().unwrap()
    }

    /// Returns this value raised to `exp` power, rounding toward zero, or
    /// `None` if the result is out of range or if this value is 0 and `exp` is
    /// nonpositive.
    ///
    /// # Accuracy
    ///
    /// For `exp > 0`, this computes intermediate results with more than `S`
    /// digits of precision, if possible, to allow to better accuracy in the
    /// result.  For `exp < 0`, this isn't implemented yet.
    pub fn checked_powi(&self, exp: i32) -> Option<Self> {
        if self.is_zero() {
            (exp > 0).then_some(Self::ZERO)
        } else if exp == 0 {
            if S < P {
                Some(Self::ONE)
            } else {
                // 1 is not representable.
                None
            }
        } else if exp > 0 {
            let mut exp = exp.unsigned_abs();
            let mut base = self.0;
            let mut base_scale = S as i32;
            let mut acc = None;
            loop {
                if (exp & 1) == 1 {
                    acc = if let Some((acc, acc_scale)) = acc {
                        let (acc, shift) = I256::from_product(acc, base).reduce_to_i128();
                        Some((acc, (acc_scale + base_scale) - shift as i32))
                    } else {
                        Some((base, base_scale))
                    };
                }
                exp /= 2;
                if exp == 0 {
                    let (acc, acc_scale) = acc.unwrap();
                    return Self::try_new_with_exponent(acc, S as i32 - acc_scale);
                }

                let (next_base, shift) = I256::from_product(base, base).reduce_to_i128();
                base = next_base;
                base_scale = base_scale * 2 - shift as i32;
            }
        } else {
            let mut exp = exp.unsigned_abs();
            let mut base = *self;
            let mut acc: Option<Fixed<P, S>> = None;
            loop {
                if (exp & 1) == 1 {
                    acc = Some(if let Some(acc) = acc {
                        acc.checked_div(&base)
                    } else {
                        base.checked_recip()
                    }?)
                }
                exp /= 2;
                if exp == 0 {
                    return acc;
                }
                base *= base;
            }
        }
    }

    /// Returns this value raised to `exp` power, rounding toward zero.
    ///
    /// For `exp > 0`, this computes intermediate results with more than `S`
    /// digits of precision, if possible, to allow to better accuracy in the
    /// result.  For `exp < 0`, this isn't implemented yet.
    ///
    /// # Panics
    ///
    /// Panics if the result is out of range or if this value is 0 and `exp` is
    /// nonpositive.
    pub fn powi(&self, exp: i32) -> Self {
        self.checked_powi(exp).unwrap()
    }

    /// Returns the least number greater than `self`, or `None` if this is
    /// `Self::MAX`.
    pub fn next_up(&self) -> Option<Self> {
        if *self < Self::MAX {
            Some(Self(self.0 + 1))
        } else {
            None
        }
    }

    /// Returns the greatest number less than `self`, or `None` if this is
    /// `Self::MAX`.
    pub fn next_down(&self) -> Option<Self> {
        if *self > Self::MIN {
            Some(Self(self.0 - 1))
        } else {
            None
        }
    }

    /// Encodes this value in a form that can be later deserialized into a new
    /// `Fixed` with possibly a different precision and scale.
    pub fn encode(&self) -> (i128, u8) {
        (self.0, S as u8)
    }

    /// Returns a new value from the serialized form returned by [encode].  If
    /// successful, returns the original value from before serialization.  If `S
    /// < scale`, then some trailing decimals are lost, by rounding toward zero.
    /// Returns `None` if the value is out of range for this type.
    ///
    /// [encode]: Self::encode
    pub fn decode(value: i128, scale: u8) -> Option<Self> {
        Self::try_new_with_exponent(value, S as i32 - scale as i32)
    }
}

impl<const P0: usize, const S0: usize> Fixed<P0, S0> {
    /// Returns this value converted into another type `Fixed<P1, S1>`, or
    /// `None` if this value is outside the range of the target type.  If the
    /// conversion is successful, then the result is exactly the same as the
    /// original value if `S1 >= S0`, and rounded down otherwise.
    ///
    /// This should be implemented as `TryFrom` but that [conflicts with the
    /// standard library
    /// implementation](https://users.rust-lang.org/t/conflicting-implementations-of-trait-from/92994).
    pub fn convert<const P1: usize, const S1: usize>(&self) -> Option<Fixed<P1, S1>> {
        Fixed::try_new_with_exponent(self.0, S1 as i32 - S0 as i32)
    }

    /// Returns this value converted into another type `Fixed<P1, S1>`, or
    /// `None` if this value is outside the range of the target type.  If the
    /// conversion is successful, then the result is exactly the same as the
    /// original value if `S1 >= S0`, and rounded to even otherwise.
    pub fn convert_round_even<const P1: usize, const S1: usize>(&self) -> Option<Fixed<P1, S1>> {
        Fixed::try_new_with_exponent_round_even(self.0, S1 as i32 - S0 as i32)
    }

    /// Returns -1 if this value is less than zero, 0 if this value is zero, and
    /// 1 if this value is greater than zero, in an arbitrary `Fixed` type.
    /// Returns `None` on overflow (if this value is nonzero and `S1 >= P1`).
    pub fn checked_sign_generic<const P1: usize, const S1: usize>(&self) -> Option<Fixed<P1, S1>> {
        let one = Fixed::<P1, S1>::scale();
        match self.0.cmp(&0) {
            Ordering::Less if S1 < P1 => Some(Fixed(-one)),
            Ordering::Equal => Some(Fixed::ZERO),
            Ordering::Greater if S1 < P1 => Some(Fixed(one)),
            _ => None,
        }
    }

    /// Calculates `self + other`, for operands with scale and precision `(S0,P0)` and
    /// `(S1,P1)`, respectively, producing a result with scale and precision
    /// `(S2,P2)`.  The result is calculated exactly if possible and otherwise
    /// rounded toward zero.  Returns `None` if the result is not representable
    /// in the result type.
    pub fn checked_add_generic<
        const P1: usize,
        const S1: usize,
        const P2: usize,
        const S2: usize,
    >(
        self,
        other: Fixed<P1, S1>,
    ) -> Option<Fixed<P2, S2>> {
        match S0.cmp(&S1) {
            Ordering::Less => {
                let factor = pow10(S1 - S0);
                if self.0 <= i128::MAX / factor / 10 {
                    Fixed::try_new_with_exponent(
                        other.0.checked_add(self.0 * factor)?,
                        S2 as i32 - S1 as i32,
                    )
                } else {
                    let result = (I256::from_product(self.0, factor) + I256::from(other.0))
                        .narrowing_div(pow10(S2.saturating_sub(S1)))?;
                    Fixed::try_new_with_exponent(result, S1.saturating_sub(S2) as i32)
                }
            }
            Ordering::Equal => {
                Fixed::try_new_with_exponent(self.0.checked_add(other.0)?, (S2 - S0) as i32)
            }
            Ordering::Greater => {
                let factor = pow10(S0 - S1);
                if other.0 <= i128::MAX / factor / 10 {
                    Fixed::try_new_with_exponent(
                        self.0.checked_add(other.0 * factor)?,
                        S2 as i32 - S0 as i32,
                    )
                } else {
                    let result = (I256::from_product(other.0, factor) + I256::from(self.0))
                        .narrowing_div(pow10(S2.saturating_sub(S0)))?;
                    Fixed::try_new_with_exponent(result, S0.saturating_sub(S2) as i32)
                }
            }
        }
    }

    /// Calculates `self - other`, for operands with scale and precision
    /// `(S0,P0)` and `(S1,P1)`, respectively, producing a result with scale and
    /// precision `(S2,P2)`.  The result is calculated exactly if possible and
    /// otherwise rounded toward zero.  Returns `None` if the result is not
    /// representable in the result type.
    pub fn checked_sub_generic<
        const P1: usize,
        const S1: usize,
        const P2: usize,
        const S2: usize,
    >(
        self,
        other: Fixed<P1, S1>,
    ) -> Option<Fixed<P2, S2>> {
        self.checked_add_generic(-other)
    }

    /// Calculates `self * other`, for operands with scale and precision
    /// `(S0,P0)` and `(S1,P1)`, respectively, producing a result with scale and
    /// precision `(S2,P2)`.  The result is calculated exactly if possible and
    /// otherwise rounded toward zero.  Returns `None` if the result is not
    /// representable in the result type.
    pub fn checked_mul_generic<
        const P1: usize,
        const S1: usize,
        const P2: usize,
        const S2: usize,
    >(
        self,
        other: Fixed<P1, S1>,
    ) -> Option<Fixed<P2, S2>> {
        Fixed::<P2, S2>::try_new_with_exponent(
            I256::from_product(self.0, other.0)
                .narrowing_div(pow10((S0 + S1).saturating_sub(S2)))?,
            S2.saturating_sub(S0 + S1) as i32,
        )
    }

    /// Calculate `self / other`, for operands with scale and precision
    /// `(S0,P0)` and `(S1,P1)`, respectively, producing a result with scale and
    /// precision `(S2,P2)`.  The result is calculated exactly if possible and
    /// otherwise rounded toward zero.  Returns `None` if the result is not
    /// representable in the result type, or if `other` is zero.
    pub fn checked_div_generic<
        const P1: usize,
        const S1: usize,
        const P2: usize,
        const S2: usize,
    >(
        self,
        other: Fixed<P1, S1>,
    ) -> Option<Fixed<P2, S2>> {
        if other == 0 {
            None
        } else {
            let shift_left = (S1 + S2).saturating_sub(S0);
            if shift_left > 38 {
                // A shift this big would exceed the range of I256, so we can't
                // calculate it, but the ultimate result would also overflow, so
                // we don't have to.
                None
            } else {
                Fixed::try_new_with_exponent(
                    I256::from_product(self.0, pow10(shift_left)).narrowing_div(other.0)?,
                    S0.saturating_sub(S1 + S2) as i32,
                )
            }
        }
    }

    /// Compute `self + other`, rounding toward zero, panicking if overflow
    /// occurs.
    pub fn strict_add_generic<
        const P1: usize,
        const S1: usize,
        const P2: usize,
        const S2: usize,
    >(
        self,
        other: Fixed<P1, S1>,
    ) -> Fixed<P2, S2> {
        self.checked_add_generic(other).unwrap()
    }

    /// Compute `self - other`, rounding toward zero, panicking if overflow
    /// occurs.
    pub fn strict_sub_generic<
        const P1: usize,
        const S1: usize,
        const P2: usize,
        const S2: usize,
    >(
        self,
        other: Fixed<P1, S1>,
    ) -> Fixed<P2, S2> {
        self.checked_sub_generic(other).unwrap()
    }

    /// Compute `self * other`, rounding toward zero, panicking if overflow
    /// occurs.
    pub fn strict_mul_generic<
        const P1: usize,
        const S1: usize,
        const P2: usize,
        const S2: usize,
    >(
        self,
        other: Fixed<P1, S1>,
    ) -> Fixed<P2, S2> {
        self.checked_mul_generic(other).unwrap()
    }

    /// Compute `self / other`, rounding toward zero, panicking if overflow
    /// occurs or if `other` is zero.
    pub fn strict_div_generic<
        const P1: usize,
        const S1: usize,
        const P2: usize,
        const S2: usize,
    >(
        self,
        other: Fixed<P1, S1>,
    ) -> Fixed<P2, S2> {
        self.checked_div_generic(other).unwrap()
    }
}

impl<const P: usize, const S: usize> Zero for Fixed<P, S> {
    fn zero() -> Self {
        Self::ZERO
    }

    fn is_zero(&self) -> bool {
        *self == Self::ZERO
    }
}

impl<const P: usize, const S: usize> One for Fixed<P, S> {
    /// This will panic at compile time if 1 isn't in the range of this type.
    fn one() -> Self {
        Self::ONE
    }
}

impl<const P: usize, const S: usize> TryFrom<f64> for Fixed<P, S> {
    type Error = OutOfRange;

    /// Convert `value` to `Fixed`, rounding toward zero, reporting an error if
    /// `value` is out of range.
    fn try_from(value: f64) -> Result<Self, Self::Error> {
        cast(value * Self::scale() as f64)
            .and_then(Self::try_new)
            .ok_or(OutOfRange)
    }
}

impl<const P: usize, const S: usize> From<Fixed<P, S>> for f64 {
    fn from(value: Fixed<P, S>) -> Self {
        value.0 as f64 / Fixed::<P, S>::scale() as f64
    }
}

impl<const P: usize, const S: usize> TryFrom<i128> for Fixed<P, S> {
    type Error = OutOfRange;

    /// Convert `value` to `Fixed`, reporting an error if `value` is out of
    /// range.  This is an exact conversion that cannot lose precision if it
    /// succeeds.
    fn try_from(value: i128) -> Result<Self, Self::Error> {
        if value.unsigned_abs() <= Self::max_u128() {
            Ok(Self(value * Self::scale()))
        } else {
            Err(OutOfRange)
        }
    }
}

macro_rules! try_from_signed_int {
    ($type_name:ty) => {
        impl<const P: usize, const S: usize> TryFrom<$type_name> for Fixed<P, S> {
            type Error = OutOfRange;

            /// Convert `value` to `Fixed`, rounding toward zero, reporting an
            /// error if `value` is out of range.
            fn try_from(value: $type_name) -> Result<Self, Self::Error> {
                (value as i128).try_into()
            }
        }
    };
}

try_from_signed_int!(isize);
try_from_signed_int!(i64);
try_from_signed_int!(i32);
try_from_signed_int!(i16);
try_from_signed_int!(i8);

impl<const P: usize, const S: usize> TryFrom<u128> for Fixed<P, S> {
    type Error = OutOfRange;

    /// Convert `value` to `Fixed`, reporting an error if `value` is out of
    /// range.  This is an exact conversion that cannot lose precision if it
    /// succeeds.
    fn try_from(value: u128) -> Result<Self, Self::Error> {
        if value <= Self::max_i128() as u128 {
            Ok(Self(value as i128 * Self::scale()))
        } else {
            Err(OutOfRange)
        }
    }
}

macro_rules! try_from_unsigned_int {
    ($type_name:ty) => {
        impl<const P: usize, const S: usize> TryFrom<$type_name> for Fixed<P, S> {
            type Error = OutOfRange;

            /// Convert `value` to `Fixed`, reporting an error if `value` is out
            /// of range.  This is an exact conversion that cannot lose
            /// precision if it succeeds.
            fn try_from(value: $type_name) -> Result<Self, Self::Error> {
                (value as u128).try_into()
            }
        }
    };
}

try_from_unsigned_int!(usize);
try_from_unsigned_int!(u64);
try_from_unsigned_int!(u32);
try_from_unsigned_int!(u16);
try_from_unsigned_int!(u8);

macro_rules! min_max_int {
    ($signed_type:ty, $max_signed:ident, $min_signed:ident, $unsigned_type:ty, $max_unsigned:ident) => {
        #[doc = "Returns the maximum `"]
        #[doc = stringify!($signed_type)]
        #[doc = "` that can be converted to this type."]
        pub const fn $max_signed() -> $signed_type {
            if Self::max_i128() > <$signed_type>::MAX as i128 {
                <$signed_type>::MAX
            } else {
                Self::max_i128() as $signed_type
            }
        }

        #[doc = "Returns the minimum `"]
        #[doc = stringify!($signed_type)]
        #[doc = "` that can be converted to this type."]
        pub const fn $min_signed() -> $signed_type {
            -Self::$max_signed()
        }

        #[doc = "Returns the maximum `"]
        #[doc = stringify!($unsigned_type)]
        #[doc = "` that can be converted to this type.\n\nThe minimum is 0."]
        pub const fn $max_unsigned() -> $unsigned_type {
            if Self::max_u128() > <$unsigned_type>::MAX as u128 {
                <$unsigned_type>::MAX
            } else {
                Self::max_u128() as $unsigned_type
            }
        }
    };
}

impl<const P: usize, const S: usize> Fixed<P, S> {
    /// Returns the maximum `i128` that can be converted to this type.
    pub const fn max_i128() -> i128 {
        if P > S {
            pow10(P - S) - 1
        } else {
            0
        }
    }

    /// Returns the minimum `i128` that can be converted to this type.
    pub const fn min_i128() -> i128 {
        -Self::max_i128()
    }

    /// Returns the maximum `u128` that can be converted to this type.
    ///
    /// The minimum is 0.
    pub const fn max_u128() -> u128 {
        Self::max_i128().cast_unsigned()
    }

    min_max_int!(isize, max_isize, min_isize, usize, max_usize);
    min_max_int!(i64, max_i64, min_i64, u64, max_u64);
    min_max_int!(i32, max_i32, min_i32, u32, max_u32);
    min_max_int!(i16, max_i16, min_i16, u16, max_u16);
    min_max_int!(i8, max_i8, min_i8, u8, max_u8);
}

impl<const P: usize, const S: usize> From<Fixed<P, S>> for i128 {
    /// Convert from `Fixed` to integer, rounding toward zero (the same
    /// semantics as Rust casts from float to integer).
    fn from(value: Fixed<P, S>) -> Self {
        // Integer `/` rounds toward zero in Rust.
        value.0 / <Fixed<P, S>>::scale()
    }
}

macro_rules! try_to_signed_int {
    ($type_name:ty) => {
        impl<const P: usize, const S: usize> TryFrom<Fixed<P, S>> for $type_name {
            type Error = OutOfRange;

            /// Convert from `Fixed` to integer, rounding toward zero (the same
            /// semantics as Rust casts from float to integer).
            fn try_from(value: Fixed<P, S>) -> Result<Self, Self::Error> {
                i128::from(value).try_into().map_err(|_| OutOfRange)
            }
        }
    };
}

try_to_signed_int!(i64);
try_to_signed_int!(i32);
try_to_signed_int!(i16);
try_to_signed_int!(i8);
try_to_signed_int!(isize);

/// This is the same as [try_to_signed_int] except for the documentation
/// comment.
macro_rules! try_to_unsigned_int {
    ($type_name:ty) => {
        impl<const P: usize, const S: usize> TryFrom<Fixed<P, S>> for $type_name {
            type Error = OutOfRange;

            /// Convert from `Fixed` to integer, rounding toward zero (the same
            /// semantics as Rust casts from float to integer).
            ///
            /// Because this rounds toward zero, negative values greater than -1
            /// will convert to 0 instead of an out-of-range error.
            fn try_from(value: Fixed<P, S>) -> Result<Self, Self::Error> {
                i128::from(value).try_into().map_err(|_| OutOfRange)
            }
        }
    };
}

try_to_unsigned_int!(u128);
try_to_unsigned_int!(u64);
try_to_unsigned_int!(u32);
try_to_unsigned_int!(u16);
try_to_unsigned_int!(u8);
try_to_unsigned_int!(usize);

impl<const P: usize, const S: usize> Add for Fixed<P, S> {
    type Output = Self;

    /// Returns the sum, rounding toward zero.
    ///
    /// # Panic
    ///
    /// Panics if the result is out of range.
    fn add(self, other: Self) -> Self::Output {
        self.checked_add(&other).unwrap()
    }
}

impl<const P: usize, const S: usize> Add for &Fixed<P, S> {
    type Output = Fixed<P, S>;

    /// Returns the sum, which is exact if the result is in range.
    ///
    /// # Panic
    ///
    /// Panics if the result is out of range.
    fn add(self, other: Self) -> Self::Output {
        self.checked_add(other).unwrap()
    }
}

impl<const P: usize, const S: usize> CheckedAdd for Fixed<P, S> {
    /// Returns the sum, which is exact, or `None` if the result is out of
    /// range.
    fn checked_add(&self, other: &Self) -> Option<Self> {
        self.checked_add_generic(*other)
    }
}

impl<const P: usize, const S: usize> AddAssign for Fixed<P, S> {
    /// Adds `other` to `self`, which is an exact calculation.
    ///
    /// # Panic
    ///
    /// Panics if the result is out of range.
    fn add_assign(&mut self, other: Self) {
        *self = *self + other;
    }
}

impl<const P: usize, const S: usize> AddAssign<&Fixed<P, S>> for Fixed<P, S> {
    /// Adds `other` to `self`, which is an exact calculation.
    ///
    /// # Panic
    ///
    /// Panics if the result is out of range.
    fn add_assign(&mut self, other: &Fixed<P, S>) {
        *self = *self + *other;
    }
}

impl<const P: usize, const S: usize> Sub for Fixed<P, S> {
    type Output = Self;

    /// Returns the difference, which is exact if the result is in range.
    ///
    /// # Panic
    ///
    /// Panics if the result is out of range.
    fn sub(self, other: Self) -> Self::Output {
        self.checked_sub(&other).unwrap()
    }
}

impl<const P: usize, const S: usize> Sub for &Fixed<P, S> {
    type Output = Fixed<P, S>;

    /// Returns the difference, which is exact if the result is in range.
    ///
    /// # Panic
    ///
    /// Panics if the result is out of range.
    fn sub(self, other: Self) -> Self::Output {
        self.checked_sub(other).unwrap()
    }
}

impl<const P: usize, const S: usize> CheckedSub for Fixed<P, S> {
    /// Returns the difference, which is exact, or `None` if the result is out
    /// of range.
    fn checked_sub(&self, other: &Self) -> Option<Self> {
        self.checked_sub_generic(*other)
    }
}

impl<const P: usize, const S: usize> SubAssign for Fixed<P, S> {
    /// Subtracts `other` from `self`, which is an exact calculation.
    ///
    /// # Panic
    ///
    /// Panics if the result is out of range.
    fn sub_assign(&mut self, other: Self) {
        *self = *self - other;
    }
}

impl<const P: usize, const S: usize> Mul for Fixed<P, S> {
    type Output = Self;

    /// Returns the product, rounding toward zero.
    ///
    /// # Panic
    ///
    /// Panics if the result is out of range.
    fn mul(self, other: Self) -> Self::Output {
        self.checked_mul(&other).unwrap()
    }
}

impl<const P: usize, const S: usize> Mul for &Fixed<P, S> {
    type Output = Fixed<P, S>;

    /// Returns the product, rounding toward zero.
    ///
    /// # Panic
    ///
    /// Panics if the result is out of range.
    fn mul(self, other: Self) -> Self::Output {
        self.checked_mul(other).unwrap()
    }
}

impl<const P: usize, const S: usize> CheckedMul for Fixed<P, S> {
    /// Returns the product, rounding toward zero, or `None` if the result is
    /// out of range.
    fn checked_mul(&self, other: &Self) -> Option<Self> {
        Self::checked_mul_generic(*self, *other)
    }
}

impl<const P: usize, const S: usize> MulAssign for Fixed<P, S> {
    /// Multiplies `self` by `other`, rounding toward zero.
    ///
    /// # Panic
    ///
    /// Panics if the result is out of range.
    fn mul_assign(&mut self, other: Self) {
        *self = *self * other;
    }
}

impl<const P: usize, const S: usize> Div for Fixed<P, S> {
    type Output = Self;

    /// Returns the quotient, rounding toward zero.
    ///
    /// # Panic
    ///
    /// Panics if `other` is zero or the result is out of range.
    fn div(self, other: Self) -> Self::Output {
        self.checked_div(&other).unwrap()
    }
}

impl<const P: usize, const S: usize> Div for &Fixed<P, S> {
    type Output = Fixed<P, S>;

    /// Returns the quotient, rounding toward zero.
    ///
    /// # Panic
    ///
    /// Panics if `other` is zero or the result is out of range.
    fn div(self, other: Self) -> Self::Output {
        self.checked_div(other).unwrap()
    }
}

impl<const P: usize, const S: usize> CheckedDiv for Fixed<P, S> {
    /// Returns the quotient, rounding toward zero, or `None` if `other` is zero
    /// or the result is out of range.
    fn checked_div(&self, other: &Self) -> Option<Self> {
        Self::checked_div_generic(*self, *other)
    }
}

impl<const P: usize, const S: usize> DivAssign for Fixed<P, S> {
    /// Divides `self` by `other`, rounding toward zero.
    ///
    /// # Panic
    ///
    /// Panics if `other` is zero or the result is out of range.
    fn div_assign(&mut self, other: Self) {
        *self = *self / other;
    }
}

impl<const P: usize, const S: usize> Neg for Fixed<P, S> {
    type Output = Self;

    /// Returns `-self`.  This is an exact calculation that cannot overflow.
    fn neg(self) -> Self::Output {
        Self(-self.0)
    }
}

impl<const P: usize, const S: usize> Neg for &Fixed<P, S> {
    type Output = Fixed<P, S>;

    /// Returns `-self`.  This is an exact calculation that cannot overflow.
    fn neg(self) -> Self::Output {
        Fixed(-self.0)
    }
}

/// Error that can be returned when parsing a [Fixed].
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ParseFixedError {
    /// Invalid syntax.
    SyntaxError,

    /// Out of valid range.
    ///
    /// Underflow is rounded to zero, so this error is only returned when the
    /// absolute value exceeds the type's range.
    OutOfRange,
}

impl<const P: usize, const S: usize> FromStr for Fixed<P, S> {
    type Err = ParseFixedError;

    /// Parses `s` as `Fixed`.
    ///
    /// This accepts the same forms as [f64::from_str], except that it rejects
    /// infinities and NaNs (which `Fixed` does not support), as well as
    /// out-of-range values.  Rounds overprecise values to the nearest
    /// representable value, rounding halfway values to even.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Non-generic inner function to reduce monomorphization cost.
        fn inner(s: &str, scale: i32) -> Result<(i128, i32), ParseFixedError> {
            // Accumulate digits into `value`.  Adjust `exponent` such that the
            // parsed value is `value / 10**exponent`.
            let mut value = 0;
            let mut exponent = scale;

            let mut saw_dot = false;
            let mut saw_digit = false;

            let mut sign = None;
            enum Sign {
                Positive,
                Negative,
            }

            let mut iter = s.chars();
            while let Some(c) = iter.next() {
                match c {
                    '-' | '+' if sign.is_some() => return Err(ParseFixedError::SyntaxError),
                    '-' => {
                        sign = Some(Sign::Negative);
                    }
                    '+' => {
                        sign = Some(Sign::Positive);
                    }
                    '0'..='9' => {
                        saw_digit = true;
                        if value < i128::MAX / 10 {
                            value = value * 10 + (c as u8 - b'0') as i128;
                            if saw_dot {
                                exponent -= 1;
                            }
                        } else if !saw_dot {
                            exponent =
                                exponent.checked_add(1).ok_or(ParseFixedError::OutOfRange)?;
                        }
                    }
                    '.' => {
                        if saw_dot {
                            return Err(ParseFixedError::SyntaxError);
                        }
                        saw_dot = true;
                    }
                    'e' | 'E' => {
                        if !saw_digit {
                            return Err(ParseFixedError::SyntaxError);
                        }
                        let e: i32 = match iter.as_str().parse() {
                            Ok(e) => e,
                            Err(error) => {
                                return match error.kind() {
                                    IntErrorKind::Zero => unreachable!(),
                                    IntErrorKind::PosOverflow => {
                                        if value != 0 {
                                            Err(ParseFixedError::OutOfRange)
                                        } else {
                                            Ok((0, 0))
                                        }
                                    }
                                    IntErrorKind::NegOverflow => Ok((0, 0)),
                                    _ => Err(ParseFixedError::SyntaxError),
                                }
                            }
                        };
                        exponent = match exponent.checked_add(e) {
                            Some(exponent) => exponent,
                            None => {
                                if e > 0 {
                                    // Don't see any way that `value` can be
                                    // zero, since we only have a positive
                                    // `exponent` if `value` would otherwise
                                    // overflow.
                                    debug_assert_ne!(value, 0);
                                    return Err(ParseFixedError::OutOfRange);
                                } else {
                                    return Ok((0, 0));
                                }
                            }
                        };
                        break;
                    }
                    _ => return Err(ParseFixedError::SyntaxError),
                }
            }
            if !saw_digit {
                return Err(ParseFixedError::SyntaxError);
            }
            let value = match sign {
                Some(Sign::Negative) => -value,
                _ => value,
            };
            Ok((value, exponent))
        }

        let (value, exponent) = inner(s, S as i32)?;
        Self::try_new_with_exponent_round_even(value, exponent).ok_or(ParseFixedError::OutOfRange)
    }
}

/// This is a doc-test to check that trying to instantiate the value 1 for a
/// type that can't represent it properly fails, with an error like "all values
/// of Fixed::<S,P>::one() for S >= P have magnitude less than one"
///
/// ```compile_fail
/// use feldera_fxp::Fixed;
///
/// let _ = Fixed::<5,5>::ONE;
/// ```
fn _invalid_constant_test() {}

#[cfg(test)]
mod test {
    use num_traits::{CheckedAdd, CheckedDiv, CheckedMul, CheckedSub};

    use crate::{Fixed, ParseFixedError};
    use std::{fmt::Write, str::FromStr};

    #[test]
    fn from_str() {
        const VAR_NAME: f64 = 0.0;
        for (s, expect) in [
            ("0", Ok(0.0)),
            ("0.", Ok(0.0)),
            (".0", Ok(0.0)),
            ("-0", Ok(-0.0)),
            ("+0", Ok(-0.0)),
            ("--0", Err(ParseFixedError::SyntaxError)),
            ("-+0", Err(ParseFixedError::SyntaxError)),
            ("0x", Err(ParseFixedError::SyntaxError)),
            ("0e5x", Err(ParseFixedError::SyntaxError)),
            ("1.23", Ok(1.23)),
            ("-1.23", Ok(-1.23)),
            ("+1.23", Ok(1.23)),
            ("99999999", Ok(9999_9999.0)),
            ("999999999", Err(ParseFixedError::OutOfRange)),
            ("999999999E-1", Ok(9999_9999.9)),
            ("9999999999e-1", Err(ParseFixedError::OutOfRange)),
            ("9999999999E-2", Ok(9999_9999.99)),
            ("99999999999e-2", Err(ParseFixedError::OutOfRange)),
            // This fails to parse because `99999999.999` rounds up to
            // `100000000000`, which is out of range.
            ("99999999999e-3", Err(ParseFixedError::OutOfRange)),
            // But with a `1` at the end rounds down, so it stays in range.
            ("99999999991e-3", Ok(9999_9999.99)),
            // This value overflows the range of `i128` as an integer, so it
            // triggers the case where we stop accepting digits and simply
            // adjust the exponent instead.
            (
                "111111111111111111111111111111111111111111e-34",
                Ok(1111_1111.11),
            ),
            // This value overflows the range of `i128` in the fraction, so it
            // triggers the case where we stop accepting digits and simply
            // adjust the exponent instead.
            (
                "1.23456788901234567890123456789012345678890123456",
                Ok(1.23),
            ),
            // This value positively overflows the exponent.
            ("1e999999999999999", Err(ParseFixedError::OutOfRange)),
            // This value positively overflows the exponent but the value is 0.
            ("0e999999999999999", Ok(0.0)),
            // This value negatively overflows the exponent.
            ("1e-999999999999999", Ok(VAR_NAME)),
            // This value overflows the range of `i128` as an integer, which
            // starts adjusting the exponent, and then it overflows the exponent
            // with `e`.
            (
                "111111111111111111111111111111111111111111e2147483644",
                Err(ParseFixedError::OutOfRange),
            ),
            // This value adjusts the exponent downward, and then it negatively
            // overflows the exponent with `e`.
            (
                ".1111111111111111111111111111111111111111e-2147483648",
                Ok(0.0),
            ),
            ("123e5", Ok(12_300_000.0)),
            ("123E4", Ok(1_230_000.0)),
            ("123e3", Ok(123_000.0)),
            ("123e2", Ok(12_300.0)),
            ("123e1", Ok(1_230.0)),
            ("123e0", Ok(123.0)),
            ("123e-1", Ok(12.3)),
            ("123e-2", Ok(1.23)),
            (".123", Ok(0.12)),
            (".124", Ok(0.12)),
            (".125", Ok(0.12)),
            (".126", Ok(0.13)),
            (".133", Ok(0.13)),
            (".134", Ok(0.13)),
            (".135", Ok(0.14)),
            (".136", Ok(0.14)),
        ] {
            println!("{s}: {:?}", s.parse::<F>());
            assert_eq!(s.parse::<F>(), expect.map(f));
        }
    }

    fn multidisplay<const P: usize, const S: usize>(number: Fixed<P, S>) -> String {
        let mut s = String::new();
        write!(&mut s, "{number}").unwrap();
        for precision in 0..=S + 1 {
            write!(&mut s, " {number:.0$}", precision).unwrap();
        }
        s
    }

    #[test]
    fn display() {
        assert_eq!(
            multidisplay(Fixed::<20, 7>::try_from(1.682501).unwrap()),
            "1.682501 2 1.7 1.68 1.683 1.6825 1.68250 1.682501 1.6825010 1.68250100"
        );
        assert_eq!(
            multidisplay(Fixed::<20, 4>::try_from(1.6825).unwrap()),
            "1.6825 2 1.7 1.68 1.682 1.6825 1.68250"
        );
        assert_eq!(
            multidisplay(Fixed::<20, 6>::try_from(1.995670).unwrap()),
            "1.99567 2 2.0 2.00 1.996 1.9957 1.99567 1.995670 1.9956700"
        );
        assert_eq!(
            multidisplay(Fixed::<20, 6>::try_from(0.995670).unwrap()),
            "0.99567 1 1.0 1.00 0.996 0.9957 0.99567 0.995670 0.9956700"
        );
        assert_eq!(
            multidisplay(Fixed::<6, 6>::try_from(0.995670).unwrap()),
            "0.99567 1 1.0 1.00 0.996 0.9957 0.99567 0.995670 0.9956700"
        );

        assert_eq!(
            multidisplay(Fixed::<20, 7>::try_from(-1.682501).unwrap()),
            "-1.682501 -2 -1.7 -1.68 -1.683 -1.6825 -1.68250 -1.682501 -1.6825010 -1.68250100"
        );
        assert_eq!(
            multidisplay(Fixed::<20, 4>::try_from(-1.6825).unwrap()),
            "-1.6825 -2 -1.7 -1.68 -1.682 -1.6825 -1.68250"
        );
        assert_eq!(
            multidisplay(Fixed::<20, 6>::try_from(-1.995670).unwrap()),
            "-1.99567 -2 -2.0 -2.00 -1.996 -1.9957 -1.99567 -1.995670 -1.9956700"
        );
        assert_eq!(
            multidisplay(Fixed::<20, 6>::try_from(-0.995670).unwrap()),
            "-0.99567 -1 -1.0 -1.00 -0.996 -0.9957 -0.99567 -0.995670 -0.9956700"
        );
    }

    type F = Fixed<10, 2>;
    fn f(n: f64) -> F {
        Fixed::try_from(n).unwrap()
    }

    fn f38_0(n: f64) -> Fixed<38, 0> {
        Fixed::try_from(n).unwrap()
    }

    fn f38_38(s: &str) -> Fixed<38, 38> {
        Fixed::from_str(s).unwrap()
    }

    #[test]
    fn mul() {
        // A few specific handwritten cases.
        assert_eq!(f(1.23) * f(2.34), f(2.87));
        assert_eq!(f(-1.23) * f(2.34), f(-2.87));
        assert_eq!(f(1.23) * f(-2.34), f(-2.87));
        assert_eq!(f(-1.23) * f(-2.34), f(2.87));

        // General case.
        for a in -999..=999 {
            let af: Fixed<10, 2> = Fixed(a);
            for b in -999..=999 {
                let bf: Fixed<10, 2> = Fixed(b);
                assert_eq!(af * bf, Fixed::<10, 2>(a * b / 100));
            }
        }

        // General case with overflow.
        for a in -999..=999 {
            let af: Fixed<3, 2> = Fixed(a);
            for b in -999..=999 {
                let bf: Fixed<3, 2> = Fixed(b);
                let c = a * b / 100;
                let expected = (c.unsigned_abs() < 1000).then_some(Fixed(c));
                assert_eq!(af.checked_mul(&bf), expected);
            }
        }
    }

    #[test]
    fn mul_generic() {
        for a in -999..=999 {
            let af: Fixed<10, 2> = Fixed(a);
            for b in -999..=999 {
                let bf: Fixed<10, 3> = Fixed(b);
                let cf: Fixed<10, 5> = af.checked_mul_generic(bf).unwrap();
                assert_eq!(cf, Fixed::<10, 5>(a * b));
                let df: Fixed<10, 6> = af.checked_mul_generic(bf).unwrap();
                assert_eq!(df, Fixed::<10, 6>(a * b * 10));
                let ef: Fixed<10, 0> = af.checked_mul_generic(bf).unwrap();
                assert_eq!(ef, Fixed::<10, 0>(a * b / 100_000));
            }
        }
    }

    #[test]
    fn div() {
        // A few specific handwritten cases.
        assert_eq!(f(1.23) / f(2.34), f(0.52));
        assert_eq!(f(-1.23) / f(2.34), f(-0.52));
        assert_eq!(f(1.23) / f(-2.34), f(-0.52));
        assert_eq!(f(-1.23) / f(-2.34), f(0.52));
        assert_eq!(
            f38_0(1.0)
                .checked_div_generic::<38, 0, 38, 38>(f38_0(7.0))
                .unwrap(),
            f38_38("0.14285714285714285714285714285714285714")
        );

        assert_eq!(
            f38_0(123.0).checked_div_generic::<38, 38, 38, 38>(f38_38("0.456")),
            None
        );

        // General case.
        for a in -999..=999 {
            let af: Fixed<10, 2> = Fixed(a);
            for b in -999..=999 {
                let bf: Fixed<10, 2> = Fixed(b);
                assert_eq!(af.checked_div(&bf), (b != 0).then(|| Fixed(a * 100 / b)));
            }
        }

        // General case with overflow.
        for a in -999..=999 {
            let af: Fixed<3, 2> = Fixed(a);
            for b in -999..=999 {
                let bf: Fixed<3, 2> = Fixed(b);
                let expected = if b != 0 {
                    let result = a * 100 / b;
                    (result.unsigned_abs() <= 999).then_some(Fixed(result))
                } else {
                    None
                };
                assert_eq!(af.checked_div(&bf), expected);
            }
        }
    }

    #[test]
    fn div_generic() {
        for a in -999..=999 {
            let af: Fixed<10, 2> = Fixed(a);
            for b in -999..=999 {
                if b != 0 {
                    let bf: Fixed<10, 3> = Fixed(b);
                    let cf: Fixed<10, 5> = af.checked_div_generic(bf).unwrap();
                    assert_eq!(cf, Fixed::<10, 5>(a * 1_000_000 / b));
                    let df: Fixed<10, 6> = af.checked_div_generic(bf).unwrap();
                    assert_eq!(df, Fixed::<10, 6>(a * 10_000_000 / b));
                    let ef: Fixed<10, 0> = af.checked_div_generic(bf).unwrap();
                    assert_eq!(ef, Fixed::<10, 0>(a * 10 / b));
                }
            }
        }
    }

    #[test]
    fn add() {
        // A few specific handwritten cases.
        assert_eq!(f(1.23) + f(2.34), f(3.57));
        assert_eq!(f(-1.23) + f(2.34), f(1.11));
        assert_eq!(f(1.23) + f(-2.34), f(-1.11));
        assert_eq!(f(-1.23) + f(-2.34), f(-3.57));

        // General case.
        for a in -999..=999 {
            let af: Fixed<10, 2> = Fixed(a);
            for b in -999..=999 {
                let bf: Fixed<10, 2> = Fixed(b);
                assert_eq!(af + bf, Fixed::<10, 2>(a + b));
            }
        }

        // General case with overflow.
        for a in -999..=999 {
            let af: Fixed<3, 2> = Fixed(a);
            for b in -999..=999 {
                let bf: Fixed<3, 2> = Fixed(b);
                let c = a + b;
                let expected = (c.unsigned_abs() < 1000).then_some(Fixed(c));
                assert_eq!(af.checked_add(&bf), expected);
            }
        }

        // General case with type conversion.
        for a in -999..=999 {
            let af: Fixed<10, 2> = Fixed(a);
            for b in -999..=999 {
                let bf: Fixed<10, 3> = Fixed(b);
                let cf: Fixed<10, 5> = af.checked_add_generic(bf).unwrap();
                assert_eq!(
                    cf,
                    Fixed::<10, 5>(a * 1000 + b * 100),
                    "{af} + {bf} ?= {cf}"
                );
                let cf: Fixed<10, 5> = bf.checked_add_generic(af).unwrap();
                assert_eq!(
                    cf,
                    Fixed::<10, 5>(a * 1000 + b * 100),
                    "{bf} + {af} ?= {cf}"
                );
                let df: Fixed<10, 6> = af.checked_add_generic(bf).unwrap();
                assert_eq!(
                    df,
                    Fixed::<10, 6>(a * 10_000 + b * 1000),
                    "{af} + {bf} ?= {df}"
                );
                let ef: Fixed<10, 0> = af.checked_add_generic(bf).unwrap();
                assert_eq!(
                    ef,
                    Fixed::<10, 0>((a * 10 + b) / 1000),
                    "{af} + {bf} ?= {ef}"
                );
            }
        }
    }

    #[test]
    fn sub() {
        // A few specific handwritten cases.
        assert_eq!(f(1.23) - f(2.34), f(-1.11));
        assert_eq!(f(-1.23) - f(2.34), f(-3.57));
        assert_eq!(f(1.23) - f(-2.34), f(3.57));
        assert_eq!(f(-1.23) - f(-2.34), f(1.11));

        // General case.
        for a in -999..=999 {
            let af: Fixed<10, 2> = Fixed(a);
            for b in -999..=999 {
                let bf: Fixed<10, 2> = Fixed(b);
                assert_eq!(af - bf, Fixed::<10, 2>(a - b));
            }
        }

        // General case with overflow.
        for a in -999..=999 {
            let af: Fixed<3, 2> = Fixed(a);
            for b in -999..=999 {
                let bf: Fixed<3, 2> = Fixed(b);
                let c = a - b;
                let expected = (c.unsigned_abs() < 1000).then_some(Fixed(c));
                assert_eq!(af.checked_sub(&bf), expected);
            }
        }

        // General case with type conversion.
        for a in -999..=999 {
            let af: Fixed<10, 2> = Fixed(a);
            for b in -999..=999 {
                let bf: Fixed<10, 3> = Fixed(b);
                let cf: Fixed<10, 5> = af.checked_sub_generic(bf).unwrap();
                assert_eq!(
                    cf,
                    Fixed::<10, 5>(a * 1000 - b * 100),
                    "{af} - {bf} ?= {cf}"
                );
                let cf: Fixed<10, 5> = bf.checked_sub_generic(af).unwrap();
                assert_eq!(
                    cf,
                    Fixed::<10, 5>(b * 100 - a * 1000),
                    "{bf} - {af} ?= {cf}"
                );
                let df: Fixed<10, 6> = af.checked_sub_generic(bf).unwrap();
                assert_eq!(df, Fixed::<10, 6>(a * 10_000 - b * 1000));
            }
        }
    }

    #[test]
    fn powi() {
        assert_eq!(
            Fixed::<10, 8>::from_str("1.12345678")
                .unwrap()
                .powi(8)
                .to_string()
                .as_str(),
            "2.53776238"
        );
        assert_eq!(f(2.0).powi(3), f(8.0));
        assert_eq!(f(-2.0).powi(3), f(-8.0));
        assert_eq!(f(1.7).powi(8), f(69.75));
        assert_eq!(f(1.7).powi(-8), f(0.01));
        assert_eq!(f(0.0).powi(1), f(0.0));
        assert_eq!(f(0.0).checked_powi(0), None);
        assert_eq!(f(0.0).checked_powi(-1), None);
    }

    #[test]
    fn convert() {
        let a = Fixed::<10, 10>::from_str("0.0123456789").unwrap();
        assert_eq!(&a.convert::<10, 0>().unwrap().to_string(), "0");
        assert_eq!(&a.convert::<10, 1>().unwrap().to_string(), "0");
        assert_eq!(&a.convert::<10, 2>().unwrap().to_string(), "0.01");
        assert_eq!(&a.convert::<10, 3>().unwrap().to_string(), "0.012");
        assert_eq!(&a.convert::<10, 4>().unwrap().to_string(), "0.0123");
        assert_eq!(&a.convert::<10, 5>().unwrap().to_string(), "0.01234");
        assert_eq!(&a.convert::<10, 6>().unwrap().to_string(), "0.012345");
        assert_eq!(&a.convert::<10, 7>().unwrap().to_string(), "0.0123456");
        assert_eq!(&a.convert::<10, 8>().unwrap().to_string(), "0.01234567");
        assert_eq!(&a.convert::<10, 9>().unwrap().to_string(), "0.012345678");
        assert_eq!(&a.convert::<10, 10>().unwrap().to_string(), "0.0123456789");
        assert_eq!(&a.convert_round_even::<10, 0>().unwrap().to_string(), "0");
        assert_eq!(&a.convert_round_even::<10, 1>().unwrap().to_string(), "0");
        assert_eq!(
            &a.convert_round_even::<10, 2>().unwrap().to_string(),
            "0.01"
        );
        assert_eq!(
            &a.convert_round_even::<10, 3>().unwrap().to_string(),
            "0.012"
        );
        assert_eq!(
            &a.convert_round_even::<10, 4>().unwrap().to_string(),
            "0.0123"
        );
        assert_eq!(
            &a.convert_round_even::<10, 5>().unwrap().to_string(),
            "0.01235"
        );
        assert_eq!(
            &a.convert_round_even::<10, 6>().unwrap().to_string(),
            "0.012346"
        );
        assert_eq!(
            &a.convert_round_even::<10, 7>().unwrap().to_string(),
            "0.0123457"
        );
        assert_eq!(
            &a.convert_round_even::<10, 8>().unwrap().to_string(),
            "0.01234568"
        );
        assert_eq!(
            &a.convert_round_even::<10, 9>().unwrap().to_string(),
            "0.012345679"
        );
        assert_eq!(
            &a.convert_round_even::<10, 10>().unwrap().to_string(),
            "0.0123456789"
        );

        let b = Fixed::<10, 5>::from_str("12345.67895").unwrap();
        assert_eq!(&b.convert::<10, 0>().unwrap().to_string(), "12345");
        assert_eq!(&b.convert::<10, 1>().unwrap().to_string(), "12345.6");
        assert_eq!(&b.convert::<10, 2>().unwrap().to_string(), "12345.67");
        assert_eq!(&b.convert::<10, 3>().unwrap().to_string(), "12345.678");
        assert_eq!(&b.convert::<10, 4>().unwrap().to_string(), "12345.6789");
        assert_eq!(&b.convert::<10, 5>().unwrap().to_string(), "12345.67895");
        assert_eq!(b.convert::<10, 6>(), None);
        assert_eq!(b.convert::<10, 7>(), None);
        assert_eq!(b.convert::<10, 8>(), None);
        assert_eq!(b.convert::<10, 9>(), None);
        assert_eq!(b.convert::<10, 10>(), None);
        assert_eq!(
            &b.convert_round_even::<10, 0>().unwrap().to_string(),
            "12346"
        );
        assert_eq!(
            &b.convert_round_even::<10, 1>().unwrap().to_string(),
            "12345.7"
        );
        assert_eq!(
            &b.convert_round_even::<10, 2>().unwrap().to_string(),
            "12345.68"
        );
        assert_eq!(
            &b.convert_round_even::<10, 3>().unwrap().to_string(),
            "12345.679"
        );
        assert_eq!(
            &b.convert_round_even::<10, 4>().unwrap().to_string(),
            "12345.679"
        );
        assert_eq!(
            &b.convert_round_even::<10, 5>().unwrap().to_string(),
            "12345.67895"
        );
        assert_eq!(b.convert_round_even::<10, 6>(), None);
        assert_eq!(b.convert_round_even::<10, 7>(), None);
        assert_eq!(b.convert_round_even::<10, 8>(), None);
        assert_eq!(b.convert_round_even::<10, 9>(), None);
        assert_eq!(b.convert_round_even::<10, 10>(), None);
    }

    #[test]
    fn constants() {
        assert_eq!(Fixed::<5, 0>::MAX, Fixed::<5, 0>(99999));
        assert_eq!(Fixed::<5, 0>::MIN, Fixed::<5, 0>(-99999));
        assert_eq!(Fixed::<5, 0>::ZERO, Fixed::<5, 0>(0));
        assert_eq!(Fixed::<5, 0>::ONE, Fixed::<5, 0>(1));

        assert_eq!(Fixed::<5, 1>::MAX, Fixed::<5, 1>(99999));
        assert_eq!(Fixed::<5, 1>::MIN, Fixed::<5, 1>(-99999));
        assert_eq!(Fixed::<5, 1>::ZERO, Fixed::<5, 1>(0));
        assert_eq!(Fixed::<5, 1>::ONE, Fixed::<5, 1>(10));

        assert_eq!(Fixed::<5, 2>::MAX, Fixed::<5, 2>(99999));
        assert_eq!(Fixed::<5, 2>::MIN, Fixed::<5, 2>(-99999));
        assert_eq!(Fixed::<5, 2>::ZERO, Fixed::<5, 2>(0));
        assert_eq!(Fixed::<5, 2>::ONE, Fixed::<5, 2>(100));

        assert_eq!(Fixed::<5, 3>::MAX, Fixed::<5, 3>(99999));
        assert_eq!(Fixed::<5, 3>::MIN, Fixed::<5, 3>(-99999));
        assert_eq!(Fixed::<5, 3>::ZERO, Fixed::<5, 3>(0));
        assert_eq!(Fixed::<5, 3>::ONE, Fixed::<5, 3>(1000));

        assert_eq!(Fixed::<5, 4>::MAX, Fixed::<5, 4>(99999));
        assert_eq!(Fixed::<5, 4>::MIN, Fixed::<5, 4>(-99999));
        assert_eq!(Fixed::<5, 4>::ZERO, Fixed::<5, 4>(0));
        assert_eq!(Fixed::<5, 4>::ONE, Fixed::<5, 4>(10000));

        assert_eq!(Fixed::<5, 5>::MAX, Fixed::<5, 5>(99999));
        assert_eq!(Fixed::<5, 5>::MIN, Fixed::<5, 5>(-99999));
        assert_eq!(Fixed::<5, 5>::ZERO, Fixed::<5, 5>(0));
        // This would panic at compile time.  See [super::_invalid_constant_test].
        //let _ = Fixed::<5, 5>::ONE;
    }

    #[test]
    fn floor() {
        assert_eq!(f(5.0).floor(), f(5.0));
        assert_eq!(f(5.1).floor(), f(5.0));
        assert_eq!(f(5.5).floor(), f(5.0));
        assert_eq!(f(5.9).floor(), f(5.0));
        assert_eq!(f(-5.0).floor(), f(-5.0));
        assert_eq!(f(-5.1).floor(), f(-6.0));
        assert_eq!(f(-5.5).floor(), f(-6.0));
        assert_eq!(f(-5.6).floor(), f(-6.0));
        assert_eq!(f(4.0).floor(), f(4.0));
        assert_eq!(f(4.1).floor(), f(4.0));
        assert_eq!(f(4.5).floor(), f(4.0));
        assert_eq!(f(4.9).floor(), f(4.0));
        assert_eq!(f(-4.0).floor(), f(-4.0));
        assert_eq!(f(-4.1).floor(), f(-5.0));
        assert_eq!(f(-4.5).floor(), f(-5.0));
        assert_eq!(f(-4.6).floor(), f(-5.0));
        assert_eq!(f(-99_999_999.0).floor(), f(-99_999_999.0));
        assert_eq!(f(-99_999_999.1).checked_floor(), None);
        assert_eq!(f(-99_999_999.5).checked_floor(), None);
        assert_eq!(f(-99_999_999.6).checked_floor(), None);
    }

    #[test]
    fn ceil() {
        assert_eq!(f(5.0).ceil(), f(5.0));
        assert_eq!(f(5.1).ceil(), f(6.0));
        assert_eq!(f(5.5).ceil(), f(6.0));
        assert_eq!(f(5.9).ceil(), f(6.0));
        assert_eq!(f(-5.0).ceil(), f(-5.0));
        assert_eq!(f(-5.1).ceil(), f(-5.0));
        assert_eq!(f(-5.5).ceil(), f(-5.0));
        assert_eq!(f(-5.6).ceil(), f(-5.0));
        assert_eq!(f(4.0).ceil(), f(4.0));
        assert_eq!(f(4.1).ceil(), f(5.0));
        assert_eq!(f(4.5).ceil(), f(5.0));
        assert_eq!(f(4.9).ceil(), f(5.0));
        assert_eq!(f(-4.0).ceil(), f(-4.0));
        assert_eq!(f(-4.1).ceil(), f(-4.0));
        assert_eq!(f(-4.5).ceil(), f(-4.0));
        assert_eq!(f(-4.6).ceil(), f(-4.0));
        assert_eq!(f(99_999_999.0).ceil(), f(99_999_999.0));
        assert_eq!(f(99_999_999.1).checked_ceil(), None);
        assert_eq!(f(99_999_999.5).checked_ceil(), None);
        assert_eq!(f(99_999_999.6).checked_ceil(), None);
    }

    #[test]
    fn truncate() {
        assert_eq!(f(5.0).truncate(), f(5.0));
        assert_eq!(f(5.1).truncate(), f(5.0));
        assert_eq!(f(5.5).truncate(), f(5.0));
        assert_eq!(f(5.9).truncate(), f(5.0));
        assert_eq!(f(-5.0).truncate(), f(-5.0));
        assert_eq!(f(-5.1).truncate(), f(-5.0));
        assert_eq!(f(-5.5).truncate(), f(-5.0));
        assert_eq!(f(-5.6).truncate(), f(-5.0));
        assert_eq!(f(4.0).truncate(), f(4.0));
        assert_eq!(f(4.1).truncate(), f(4.0));
        assert_eq!(f(4.5).truncate(), f(4.0));
        assert_eq!(f(4.9).truncate(), f(4.0));
        assert_eq!(f(-4.0).truncate(), f(-4.0));
        assert_eq!(f(-4.1).truncate(), f(-4.0));
        assert_eq!(f(-4.5).truncate(), f(-4.0));
        assert_eq!(f(-4.6).truncate(), f(-4.0));
        assert_eq!(f(99_999_999.0).truncate(), f(99_999_999.0));
        assert_eq!(f(99_999_999.1).truncate(), f(99_999_999.0));
        assert_eq!(f(99_999_999.5).truncate(), f(99_999_999.0));
        assert_eq!(f(99_999_999.6).truncate(), f(99_999_999.0));
        assert_eq!(f(-99_999_999.0).truncate(), f(-99_999_999.0));
        assert_eq!(f(-99_999_999.1).truncate(), f(-99_999_999.0));
        assert_eq!(f(-99_999_999.5).truncate(), f(-99_999_999.0));
        assert_eq!(f(-99_999_999.6).truncate(), f(-99_999_999.0));
    }

    #[test]
    fn sign() {
        assert_eq!(f(-0.1).sign(), Fixed::<1, 0>::try_from(-1).unwrap());
        assert_eq!(f(0.0).sign(), Fixed::<1, 0>::try_from(0).unwrap());
        assert_eq!(f(0.5).sign(), Fixed::<1, 0>::try_from(1).unwrap());
    }

    #[test]
    fn sqrt() {
        // A few selected values.
        assert_eq!(f(0.0).sqrt(), f(0.0));
        assert_eq!(f(1.0).sqrt(), f(1.0));
        assert_eq!(f(2.0).sqrt(), f(1.41));
        assert_eq!(f(3.0).sqrt(), f(1.73));
        assert_eq!(f(4.0).sqrt(), f(2.0));
        assert_eq!(f(-1.0).checked_sqrt(), None);

        // General case.
        for a in 0..=999 {
            let af: Fixed<10, 2> = Fixed(a);
            assert_eq!(af.sqrt(), Fixed::<10, 2>((a * 100).isqrt()));
        }
    }

    #[test]
    fn nullable() {
        /// Adds `a` and `b` and returns the sum.  Return `None` if `a` or `b`
        /// is `None` or if their sum is out of range.
        fn nullable_checked_add_generic<
            const PA: usize,
            const SA: usize,
            const PB: usize,
            const SB: usize,
            const PC: usize,
            const SC: usize,
        >(
            a: Option<Fixed<PA, SA>>,
            b: Option<Fixed<PB, SB>>,
        ) -> Option<Fixed<PC, SC>> {
            a.zip(b).and_then(|(a, b)| a.checked_add_generic(b))
        }

        let a: Option<Fixed<10, 2>> = Some("1.23".parse().unwrap());
        let b: Option<Fixed<5, 4>> = Some("4.5678".parse().unwrap());
        let c: Option<Fixed<10, 4>> = nullable_checked_add_generic(a, b);
        assert_eq!(c, Some("5.7978".parse().unwrap()));
    }

    #[test]
    fn to_integer() {
        for x in -9999..=9999 {
            let f = Fixed::<4, 1>(x);
            assert_eq!(i128::from(f), x / 10);
            assert_eq!(i64::try_from(f).unwrap(), (x / 10) as i64);
            assert_eq!(i32::try_from(f).unwrap(), (x / 10) as i32);
            assert_eq!(i16::try_from(f).unwrap(), (x / 10) as i16);
            assert_eq!(
                i8::try_from(f).ok(),
                (-1289..=1279).contains(&x).then_some((x / 10) as i8)
            );
            assert_eq!(
                u128::try_from(f).ok(),
                (x > -10).then_some((x / 10) as u128)
            );
            assert_eq!(u64::try_from(f).ok(), (x > -10).then_some((x / 10) as u64));
            assert_eq!(u32::try_from(f).ok(), (x > -10).then_some((x / 10) as u32));
            assert_eq!(u16::try_from(f).ok(), (x > -10).then_some((x / 10) as u16));
            assert_eq!(
                u8::try_from(f).ok(),
                (-9..=2559).contains(&x).then_some((x / 10) as u8)
            );
        }
    }
}
