use std::{
    cmp::Ordering,
    fmt::{Debug, Display},
    io::Write,
    ops::{Add, AddAssign, Div, DivAssign, Mul, MulAssign, Neg, Range, Sub, SubAssign},
    str::FromStr,
};

use num_traits::{cast, CheckedAdd, CheckedDiv, CheckedMul, CheckedSub, One, Zero};
use size_of::SizeOf;
use smallvec::{Array, SmallVec};

use crate::u256::I256;

mod serde_impl;
mod u256;

/// Fixed-point decimal with fixed precision and scale.
///
/// `Fixed<P,S>`, with `1 <= P <= 38` and `S <= P`, represents a signed decimal
/// number with fixed `S - P` digits before the decimal point and `S` digits
/// after.  Some examples:
///
/// |         Type |Minimum Value | Maximum Value |
/// |:-------------|-------------:|--------------:|
/// | `Fixed<5,5>` |     -0.99999 |       0.99999 |
/// | `Fixed<5,4>` |      -9.9999 |        9.9999 |
/// | `Fixed<5,3>` |      -99.999 |        99.999 |
/// | `Fixed<5,2>` |      -999.99 |        999.99 |
/// | `Fixed<5,1>` |      -9999.9 |        9999.9 |
/// | `Fixed<5,0>` |       -99999 |         99999 |
///
/// # Representation
///
/// `Fixed` internally contains a single `i128`, which limits `S` to 38 because
/// `10**38 < 2**127 - 1 < 10**39`.  A single `i64` would be sufficient for `S
/// <= 18`, and a single `i32` for `S <= 9`, but the implementation does not
/// optimize for those cases.
#[derive(
    Copy,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    SizeOf,
)]
#[archive_attr(doc(hidden))]
pub struct Fixed<const P: usize, const S: usize>(i128);

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

    /// Returns `Self(value)`, if `value` is in the correct range for this type.
    fn try_new(value: i128) -> Option<Self> {
        Self::check_constraints();
        (Self::MIN.0..=Self::MAX.0)
            .contains(&value)
            .then_some(Self(value))
    }

    /// Returns `Self(value * 10**exponent)`, rounding to even if `exponent` is
    /// negative, if the computed value is in the correct range for the type.
    fn try_new_with_exponent(value: i128, exponent: i32) -> Option<Self> {
        let result = match exponent.cmp(&0) {
            Ordering::Less => {
                // Divide by a negative exponent.
                let Some(divisor) = checked_pow10(exponent.unsigned_abs()) else {
                    // `10**-exponent` is greater than `i128::MAX`.  The result
                    // must be zero.
                    return Some(Self::ZERO);
                };

                // Round toward even.
                let quotient = value.abs() / divisor;
                let remainder = value.abs() % divisor;
                let new_value = match remainder.cmp(&(divisor / 2)) {
                    Ordering::Less => quotient,
                    Ordering::Equal => quotient + quotient % 2,
                    Ordering::Greater => quotient + 1,
                };
                if value < 0 {
                    -new_value
                } else {
                    new_value
                }
            }
            Ordering::Equal => value,
            Ordering::Greater => value.checked_mul(checked_pow10(exponent.cast_unsigned())?)?,
        };
        Self::try_new(result)
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

    /// Returns the range of `i128`s that can be converted to this type.
    ///
    /// If `S == P`, the range contains only 0.
    pub const fn i128_range() -> Range<i128> {
        Self::min_i128()..Self::max_i128() + 1
    }

    /// Integer division, as defined for `divide-integer` in [General Decimal
    /// Arithmetic].  Returns `None` if `other` is zero or the result is out of
    /// range for the result type.
    ///
    /// [General Decimal Arithmetic]: https://speleotrove.com/decimal/decarith.pdf
    pub const fn checked_div_integer(self, other: Self) -> Option<i128> {
        self.0.checked_div(other.0)
    }

    /// Integer remainder, as defined for `remainder` in [General Decimal
    /// Arithmetic].  Returns `None` if `other` is zero or the result is out of
    /// range for the result type.
    ///
    /// [General Decimal Arithmetic]: https://speleotrove.com/decimal/decarith.pdf
    pub const fn checked_rem_integer(self, other: Self) -> Option<i128> {
        self.0.checked_rem(other.0)
    }

    /// Returns the absolute value.
    pub const fn abs(self) -> Self {
        Self(self.0.abs())
    }

    /// Returns true if this value is negative, false if it is zero or positive.
    pub const fn is_negative(self) -> bool {
        self.0.is_negative()
    }
}

impl<const P0: usize, const S0: usize> Fixed<P0, S0> {
    /// Returns this value converted into another type `Fixed<P1, S1>`, or
    /// `None` if this value is outside the range of the target type.  If the
    /// conversion is successful, then the result is exactly the same as the
    /// original value if `S1 >= S0`, and rounded to even otherwise.
    ///
    /// This should be implemented as `TryFrom` but that [conflicts with the
    /// standard library
    /// implementation](https://users.rust-lang.org/t/conflicting-implementations-of-trait-from/92994).
    pub fn convert<const P1: usize, const S1: usize>(&self) -> Option<Fixed<P1, S1>> {
        Fixed::try_new_with_exponent(self.0, (S1 - S0) as i32)
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
                if self.0 <= i128::MAX / factor {
                    Fixed::try_new_with_exponent(
                        other.0.checked_add(self.0 * factor)?,
                        (S1 - S0) as i32,
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
                if other.0 <= i128::MAX / factor {
                    Fixed::try_new_with_exponent(
                        self.0.checked_add(other.0 * factor)?,
                        (S0 - S1) as i32,
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
        if other == Fixed::ZERO {
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
        if Self::i128_range().contains(&value) {
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

impl<const P: usize, const S: usize> From<Fixed<P, S>> for i128 {
    /// Convert from `Fixed` to integer, rounding toward zero (the same
    /// semantics as Rust casts from float to integer).
    fn from(value: Fixed<P, S>) -> Self {
        // Integer `/` rounds toward zero in Rust.
        value.0 / <Fixed<P, S>>::scale()
    }
}

impl<const P: usize, const S: usize> Add for Fixed<P, S> {
    type Output = Self;

    fn add(self, other: Self) -> Self::Output {
        self.checked_add(&other).unwrap()
    }
}

impl<const P: usize, const S: usize> CheckedAdd for Fixed<P, S> {
    fn checked_add(&self, other: &Self) -> Option<Self> {
        self.checked_add_generic(*other)
    }
}

impl<const P: usize, const S: usize> AddAssign for Fixed<P, S> {
    fn add_assign(&mut self, other: Self) {
        *self = *self + other;
    }
}

impl<const P: usize, const S: usize> Sub for Fixed<P, S> {
    type Output = Self;

    fn sub(self, other: Self) -> Self::Output {
        self.checked_sub(&other).unwrap()
    }
}

impl<const P: usize, const S: usize> CheckedSub for Fixed<P, S> {
    fn checked_sub(&self, other: &Self) -> Option<Self> {
        self.checked_sub_generic(*other)
    }
}

impl<const P: usize, const S: usize> SubAssign for Fixed<P, S> {
    fn sub_assign(&mut self, other: Self) {
        *self = *self - other;
    }
}

impl<const P: usize, const S: usize> Mul for Fixed<P, S> {
    type Output = Self;

    fn mul(self, other: Self) -> Self::Output {
        self.checked_mul(&other).unwrap()
    }
}

impl<const P: usize, const S: usize> CheckedMul for Fixed<P, S> {
    fn checked_mul(&self, other: &Self) -> Option<Self> {
        Self::checked_mul_generic(*self, *other)
    }
}

impl<const P: usize, const S: usize> MulAssign for Fixed<P, S> {
    fn mul_assign(&mut self, other: Self) {
        *self = *self * other;
    }
}

impl<const P: usize, const S: usize> Div for Fixed<P, S> {
    type Output = Self;

    fn div(self, other: Self) -> Self::Output {
        self.checked_div(&other).unwrap()
    }
}

impl<const P: usize, const S: usize> CheckedDiv for Fixed<P, S> {
    fn checked_div(&self, other: &Self) -> Option<Self> {
        Self::checked_div_generic(*self, *other)
    }
}

impl<const P: usize, const S: usize> DivAssign for Fixed<P, S> {
    fn div_assign(&mut self, other: Self) {
        *self = *self / other;
    }
}

impl<const P: usize, const S: usize> Neg for Fixed<P, S> {
    type Output = Self;

    fn neg(self) -> Self::Output {
        Self(-self.0)
    }
}

#[derive(Copy, Clone, Debug)]
pub struct ParseFixedError;

impl<const P: usize, const S: usize> FromStr for Fixed<P, S> {
    type Err = ParseFixedError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Accumulate digits into `value`.  Adjust `exponent` such that the
        // parsed value is `value / 10**exponent`.
        let mut value = 0;
        let mut exponent = S as i32;

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
                '-' | '+' if sign.is_some() => return Err(ParseFixedError),
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
                        exponent += 1;
                    }
                }
                '.' => {
                    if saw_dot {
                        return Err(ParseFixedError);
                    }
                    saw_dot = true;
                }
                'e' | 'E' => {
                    exponent = exponent
                        .checked_add(iter.as_str().parse().map_err(|_| ParseFixedError)?)
                        .ok_or(ParseFixedError)?;
                    break;
                }
                _ => return Err(ParseFixedError),
            }
        }
        if !saw_digit {
            return Err(ParseFixedError);
        }
        let value = match sign {
            Some(Sign::Negative) => -value,
            _ => value,
        };
        Self::try_new_with_exponent(value, exponent).ok_or(ParseFixedError)
    }
}

pub fn add(x: Fixed<38, 10>, y: Fixed<38, 10>) -> Fixed<38, 10> {
    x + y
}

#[cfg(test)]
mod test {
    use num_traits::{CheckedDiv, CheckedMul};

    use crate::Fixed;
    use std::{fmt::Write, hint::black_box, str::FromStr, time::Instant};

    #[test]
    fn from_str() {
        for (s, f) in [
            ("0", Some(0.0)),
            ("0.", Some(0.0)),
            (".0", Some(0.0)),
            ("-0", Some(-0.0)),
            ("+0", Some(-0.0)),
            ("--0", None),
            ("-+0", None),
            ("0x", None),
            ("0e5x", None),
            ("1.23", Some(1.23)),
            ("-1.23", Some(-1.23)),
            ("+1.23", Some(1.23)),
            ("99999999", Some(9999_9999.0)),
            ("999999999", None),
            ("999999999E-1", Some(9999_9999.9)),
            ("9999999999e-1", None),
            ("9999999999E-2", Some(9999_9999.99)),
            ("99999999999e-2", None),
            // This fails to parse because `99999999.999` rounds up to
            // `100000000000`, which is out of range.
            ("99999999999e-3", None),
            // But with a `1` at the end rounds down, so it stays in range.
            ("99999999991e-3", Some(9999_9999.99)),
            // This value overflows the range of `i128` as an integer, so it
            // triggers the case where we stop accepting digits and simply
            // adjust the exponent instead.
            (
                "111111111111111111111111111111111111111111e-34",
                Some(1111_1111.11),
            ),
            // This value overflows the range of `i128` in the fraction, so it
            // triggers the case where we stop accepting digits and simply
            // adjust the exponent instead.
            (
                "1.23456788901234567890123456789012345678890123456",
                Some(1.23),
            ),
            ("123e5", Some(12_300_000.0)),
            ("123E4", Some(1_230_000.0)),
            ("123e3", Some(123_000.0)),
            ("123e2", Some(12_300.0)),
            ("123e1", Some(1_230.0)),
            ("123e0", Some(123.0)),
            ("123e-1", Some(12.3)),
            ("123e-2", Some(1.23)),
            (".123", Some(0.12)),
            (".124", Some(0.12)),
            (".125", Some(0.12)),
            (".126", Some(0.13)),
            (".133", Some(0.13)),
            (".134", Some(0.13)),
            (".135", Some(0.14)),
            (".136", Some(0.14)),
        ] {
            println!("{s}: {:?}", s.parse::<F>());
            assert_eq!(s.parse::<F>().ok(), f.map(|f| F::try_from(f).unwrap()));
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
        assert_eq!(f(1.23) * f(2.34), f(2.87));
        assert_eq!(f(-1.23) * f(2.34), f(-2.87));
        assert_eq!(f(1.23) * f(-2.34), f(-2.87));
        assert_eq!(f(-1.23) * f(-2.34), f(2.87));
    }

    #[test]
    fn div() {
        assert_eq!(f(1.23) / f(2.34), f(0.52));
        assert_eq!(f(-1.23) / f(2.34), f(-0.52));
        assert_eq!(f(1.23) / f(-2.34), f(-0.52));
        assert_eq!(f(-1.23) / f(-2.34), f(0.52));
        assert_eq!(
            f38_0(1.0).checked_div_generic(f38_0(7.0)).unwrap(),
            f38_38("0.14285714285714285714285714285714285714")
        );

        assert_eq!(
            f38_0(123.0).checked_div_generic::<38, 38, 38, 38>(f38_38("0.456")),
            None
        );
    }

    #[test]
    fn one() {
        println!("{}", Fixed::<5, 1>::max_i128());
        println!("{}", Fixed::<5, 1>::min_i128());
        println!("{}", Fixed::<5, 2>::max_i128());
        println!("{}", Fixed::<5, 2>::min_i128());
        println!("{}", Fixed::<5, 3>::max_i128());
        println!("{}", Fixed::<5, 3>::min_i128());
        println!("{}", Fixed::<5, 4>::max_i128());
        println!("{}", Fixed::<5, 4>::min_i128());
        println!("{}", Fixed::<5, 5>::max_i128());
        println!("{}", Fixed::<5, 5>::min_i128());

        type F10_2 = Fixed<10, 2>;
        let a = F10_2::try_from(-5).unwrap();
        let b = F10_2::try_from(6).unwrap();
        let c = F10_2::try_from(-1.23).unwrap();
        let d = F10_2::try_from(2).unwrap();
        println!("{a:?} {b:?} {c:?} {:?} {:?}", a + b, a + b + c);
        println!("{a} {b} {c} {} {}", a + b, a + b + c);
        println!("{a} {b} {c} {}", a * b);
        println!("{a} {b} {c} {}", b * c);
        println!(
            "{:?}",
            F10_2::MAX.checked_mul(&F10_2::try_from(0.5).unwrap())
        );
        println!("{:?}", a.checked_div(&b));

        println!(
            "{a} div {d}: {:?} {}",
            a.checked_div_integer(d).unwrap(),
            a.checked_rem_integer(d).unwrap()
        );
    }

    #[test]
    fn add_bench() {
        let start = Instant::now();
        let mut value = Fixed::<38, 10>::ZERO;
        for _ in 0..1_000_000 {
            value += black_box(Fixed::ONE);
        }
        println!(
            "fixed calculate {value} in {:.3}s",
            start.elapsed().as_secs_f64()
        );
    }
}
