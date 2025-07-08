// Warn about missing docs, but not for item declared with `#[cfg(test)]`.
#![cfg_attr(not(test), warn(missing_docs))]

//! Decimal arithmetic.
//!
//! This crate primarily provides the [Fixed] type for decimal arithmetic with
//! precision and scale supplied as type parameters.  It also provides
//! [DynamicDecimal], which has 38 digits of precision and a dynamic scale.  The
//! latter is mainly provided for serialization and does not include much in the
//! way of arithmetic.
//!
//! # Features
//!
//! The following `cargo` features are provided:
//!
//! * `serde`: Implements [serde] traits for serializing and deserializing
//! [Fixed] and [DynamicDecimal].
//!
//! * `rkyv`: Implements [rkyv] traits for serializing and deserializing [Fixed]
//! and [DynamicDecimal].
//!
//! * `validation` (depends on `rkyv`): Implements [rkyv] traits for validation.
//!
//! * `size_of`: Implements [size_of] traits for measuring data sizes.
//!
//! * `dbsp` (depends on `serde`, `rkyv`, and `size_of`): Implements [DBSP]
//! traits for [Fixed] and [DynamicDecimal].
//!
//! [rkyv]: https://rkyv.org/
//! [serde]: https://serde.rs/
//! [size_of]: https://docs.rs/size-of/latest/size_of/
//! [DBSP]: https://docs.rs/dbsp/latest/dbsp/

use std::{cmp::Ordering, io::Write, num::IntErrorKind};

use smallvec::{Array, SmallVec};

#[cfg(feature = "dbsp")]
mod dbsp_impl;

#[cfg(feature = "serde")]
mod serde_impl;
mod u256;

#[cfg(feature = "rkyv")]
mod rkyv_impl;

mod dynamic;
pub use dynamic::DynamicDecimal;

mod fixed;
pub use fixed::Fixed;

/// A maximum-precision `Fixed` with no decimal places.
pub type FixedInteger = Fixed<38, 0>;

fn debug_decimal(value: i128, s: usize, f: &mut std::fmt::Formatter) -> std::fmt::Result {
    let mut buf = SmallVec::<[u8; 64]>::new();
    write!(&mut buf, "{:01$}", value.unsigned_abs(), s + 1).unwrap();
    let d = buf.len() - s;
    while buf.len() > d && buf.ends_with(b"0") {
        buf.pop();
    }
    // SAFETY: `buf` contains only ASCII characters.
    let s = unsafe { str::from_utf8_unchecked(&buf) };
    let (integer, fraction) = s.split_at(d);
    let sign = if value < 0 { "-" } else { "" };
    write!(f, "{sign}{integer}")?;
    if !fraction.is_empty() {
        write!(f, ".{fraction}")?;
    }
    Ok(())
}

fn display_decimal(value: i128, s: usize, f: &mut std::fmt::Formatter) -> std::fmt::Result {
    let mut buf = SmallVec::<[u8; 64]>::new();
    write!(&mut buf, "{:01$}", value.abs(), s + 1).unwrap();
    debug_assert!(buf.len() > s);
    let decimals = if let Some(precision) = f.precision() {
        match precision.cmp(&s) {
            Ordering::Less => {
                let new_len = buf.len() - (s - precision);
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
                for _ in s..precision {
                    buf.push(b'0');
                }
            }
        }
        precision
    } else {
        let mut decimals = s;
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
    f.pad_integral(value >= 0, "", unsafe { str::from_utf8_unchecked(&buf) })
}

/// Parses decimal string `s` into `(sig,exp)`, representing `sig * 10**exp`.
///
/// Adds `scale` to the returned exponent, which is just a convenience.
fn parse_decimal(s: &str, scale: i32) -> Result<(i128, i32), ParseDecimalError> {
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
            '-' | '+' if sign.is_some() => return Err(ParseDecimalError::SyntaxError),
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
                    exponent = exponent
                        .checked_add(1)
                        .ok_or(ParseDecimalError::OutOfRange)?;
                }
            }
            '.' => {
                if saw_dot {
                    return Err(ParseDecimalError::SyntaxError);
                }
                saw_dot = true;
            }
            'e' | 'E' => {
                if !saw_digit {
                    return Err(ParseDecimalError::SyntaxError);
                }
                let e: i32 = match iter.as_str().parse() {
                    Ok(e) => e,
                    Err(error) => {
                        return match error.kind() {
                            IntErrorKind::Zero => unreachable!(),
                            IntErrorKind::PosOverflow => {
                                if value != 0 {
                                    Err(ParseDecimalError::OutOfRange)
                                } else {
                                    Ok((0, 0))
                                }
                            }
                            IntErrorKind::NegOverflow => Ok((0, 0)),
                            _ => Err(ParseDecimalError::SyntaxError),
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
                            return Err(ParseDecimalError::OutOfRange);
                        } else {
                            return Ok((0, 0));
                        }
                    }
                };
                break;
            }
            _ => return Err(ParseDecimalError::SyntaxError),
        }
    }
    if !saw_digit {
        return Err(ParseDecimalError::SyntaxError);
    }
    let value = match sign {
        Some(Sign::Negative) => -value,
        _ => value,
    };
    Ok((value, exponent))
}

/// Error that can be returned when parsing [Fixed] or [DynamicDecimal].
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ParseDecimalError {
    /// Invalid syntax.
    SyntaxError,

    /// Out of valid range.
    ///
    /// Underflow is rounded to zero, so this error is only returned when the
    /// absolute value exceeds the type's range.
    OutOfRange,
}

/// Error returned for operations that would produce an out-of-range result.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
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

/// Returns `value * 10**exponent`, rounding to even if `exponent` is negative,
/// or `None` if the result would be out of range for `i128`.
fn i128_mul_pow10_round_even(value: i128, exponent: i32) -> Option<i128> {
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
    use crate::{DynamicDecimal, Fixed, ParseDecimalError};
    use std::fmt::Write;

    #[test]
    fn from_str() {
        for (s, expect) in [
            ("0", Ok(0.0)),
            ("0.", Ok(0.0)),
            (".0", Ok(0.0)),
            ("-0", Ok(-0.0)),
            ("+0", Ok(-0.0)),
            ("--0", Err(ParseDecimalError::SyntaxError)),
            ("-+0", Err(ParseDecimalError::SyntaxError)),
            ("0x", Err(ParseDecimalError::SyntaxError)),
            ("0e5x", Err(ParseDecimalError::SyntaxError)),
            ("1.23", Ok(1.23)),
            ("-1.23", Ok(-1.23)),
            ("+1.23", Ok(1.23)),
            ("99999999", Ok(9999_9999.0)),
            ("999999999", Err(ParseDecimalError::OutOfRange)),
            ("999999999E-1", Ok(9999_9999.9)),
            ("9999999999e-1", Err(ParseDecimalError::OutOfRange)),
            ("9999999999E-2", Ok(9999_9999.99)),
            ("99999999999e-2", Err(ParseDecimalError::OutOfRange)),
            // This fails to parse because `99999999.999` rounds up to
            // `100000000000`, which is out of range.
            ("99999999999e-3", Err(ParseDecimalError::OutOfRange)),
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
            ("1e999999999999999", Err(ParseDecimalError::OutOfRange)),
            // This value positively overflows the exponent but the value is 0.
            ("0e999999999999999", Ok(0.0)),
            // This value negatively overflows the exponent.
            ("1e-999999999999999", Ok(0.0)),
            // This value overflows the range of `i128` as an integer, which
            // starts adjusting the exponent, and then it overflows the exponent
            // with `e`.
            (
                "111111111111111111111111111111111111111111e2147483644",
                Err(ParseDecimalError::OutOfRange),
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

    #[test]
    fn debug() {
        fn test<const P: usize, const S: usize>(fixed: Fixed<P, S>, expect: &str) {
            assert_eq!(format!("{fixed:?}"), expect);
            let dynamic = DynamicDecimal::from(fixed);
            assert_eq!(format!("{dynamic:?}"), expect);
        }
        test(Fixed::<20, 7>::try_from(0).unwrap(), "0");
        test(Fixed::<20, 7>::try_from(5).unwrap(), "5");
        test(Fixed::<20, 7>::try_from(-5).unwrap(), "-5");
        test(Fixed::<20, 7>::try_from(10).unwrap(), "10");
        test(Fixed::<20, 7>::try_from(0.0001).unwrap(), "0.0001");
        test(Fixed::<20, 7>::try_from(-0.0001).unwrap(), "-0.0001");
        test(Fixed::<20, 7>::try_from(1.0001).unwrap(), "1.0001");
        test(Fixed::<20, 7>::try_from(-1.0001).unwrap(), "-1.0001");
        test(Fixed::<20, 7>::try_from(1.682501).unwrap(), "1.682501");
        test(Fixed::<20, 4>::try_from(1.6825).unwrap(), "1.6825");
        test(Fixed::<20, 6>::try_from(1.995670).unwrap(), "1.99567");
        test(Fixed::<20, 6>::try_from(0.995670).unwrap(), "0.99567");
        test(Fixed::<6, 6>::try_from(0.995670).unwrap(), "0.99567");

        test(Fixed::<20, 7>::try_from(-1.682501).unwrap(), "-1.682501");
        test(Fixed::<20, 4>::try_from(-1.6825).unwrap(), "-1.6825");
        test(Fixed::<20, 6>::try_from(-1.995670).unwrap(), "-1.99567");
        test(Fixed::<20, 6>::try_from(-0.995670).unwrap(), "-0.99567");
    }

    #[test]
    fn display() {
        fn test<const P: usize, const S: usize>(fixed: Fixed<P, S>, expect: &str) {
            let mut s = String::new();
            write!(&mut s, "{fixed}").unwrap();
            for precision in 0..=S + 1 {
                write!(&mut s, " {fixed:.0$}", precision).unwrap();
            }
            assert_eq!(s, expect);

            let dynamic = DynamicDecimal::from(fixed);
            let mut s = String::new();
            write!(&mut s, "{dynamic}").unwrap();
            for precision in 0..=S + 1 {
                write!(&mut s, " {dynamic:.0$}", precision).unwrap();
            }
            assert_eq!(s, expect);
        }

        test(
            Fixed::<20, 7>::try_from(0.0001).unwrap(),
            "0.0001 0 0.0 0.00 0.000 0.0001 0.00010 0.000100 0.0001000 0.00010000",
        );
        test(
            Fixed::<20, 7>::try_from(-0.0001).unwrap(),
            "-0.0001 -0 -0.0 -0.00 -0.000 -0.0001 -0.00010 -0.000100 -0.0001000 -0.00010000",
        );
        test(
            Fixed::<20, 7>::try_from(1.0001).unwrap(),
            "1.0001 1 1.0 1.00 1.000 1.0001 1.00010 1.000100 1.0001000 1.00010000",
        );
        test(
            Fixed::<20, 7>::try_from(-1.0001).unwrap(),
            "-1.0001 -1 -1.0 -1.00 -1.000 -1.0001 -1.00010 -1.000100 -1.0001000 -1.00010000",
        );
        test(
            Fixed::<20, 7>::try_from(1.682501).unwrap(),
            "1.682501 2 1.7 1.68 1.683 1.6825 1.68250 1.682501 1.6825010 1.68250100",
        );
        test(
            Fixed::<20, 4>::try_from(1.6825).unwrap(),
            "1.6825 2 1.7 1.68 1.682 1.6825 1.68250",
        );
        test(
            Fixed::<20, 6>::try_from(1.995670).unwrap(),
            "1.99567 2 2.0 2.00 1.996 1.9957 1.99567 1.995670 1.9956700",
        );
        test(
            Fixed::<20, 6>::try_from(0.995670).unwrap(),
            "0.99567 1 1.0 1.00 0.996 0.9957 0.99567 0.995670 0.9956700",
        );
        test(
            Fixed::<6, 6>::try_from(0.995670).unwrap(),
            "0.99567 1 1.0 1.00 0.996 0.9957 0.99567 0.995670 0.9956700",
        );

        test(
            Fixed::<20, 7>::try_from(-1.682501).unwrap(),
            "-1.682501 -2 -1.7 -1.68 -1.683 -1.6825 -1.68250 -1.682501 -1.6825010 -1.68250100",
        );
        test(
            Fixed::<20, 4>::try_from(-1.6825).unwrap(),
            "-1.6825 -2 -1.7 -1.68 -1.682 -1.6825 -1.68250",
        );
        test(
            Fixed::<20, 6>::try_from(-1.995670).unwrap(),
            "-1.99567 -2 -2.0 -2.00 -1.996 -1.9957 -1.99567 -1.995670 -1.9956700",
        );
        test(
            Fixed::<20, 6>::try_from(-0.995670).unwrap(),
            "-0.99567 -1 -1.0 -1.00 -0.996 -0.9957 -0.99567 -0.995670 -0.9956700",
        );
    }

    type F = Fixed<10, 2>;
    fn f(n: f64) -> F {
        Fixed::try_from(n).unwrap()
    }
}
