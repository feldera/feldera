use std::{
    cmp::Ordering,
    fmt::{Debug, Display, Write},
    str::FromStr,
};

use smallstr::SmallString;

use crate::{
    checked_pow10, debug_decimal, display_decimal, parse_decimal, pow10, u256::I256, Fixed,
    OutOfRange, ParseDecimalError,
};

/// Decimal real number with 38 digits of precision and dynamic scale.
///
/// This type is primarily meant as a serialized form of `Fixed` that does not
/// require parameterization.  Any `Fixed` can be converted into
/// `DynamicDecimal` and then converted back into any other `Fixed`, possibly
/// with a different precision and scale, without loss of precision (beyond that
/// inherent in change of scale, if any).
///
/// # Representation
///
/// The number is represented as an `i128` significand and a `u8` scale, that
/// together express the value `significand * 10**-scale`.
///
/// # Invariants
///
/// These invariants on significand and scale ensure that the value is in
/// canonical form:
///
/// * If `significand != 0 && scale != 0`, then `significand` must not be a
///   multiple of 10.
///
/// * If `significand != 0 && scale == 0`, then `significand` may be a
///   multiple of 10.  (This is a "denormalized" representation with fewer
///   digits of precision than other forms.)
///
/// * If `significand == 0`, then `scale` must be zero.
#[derive(Copy, Clone, Default, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "size_of", derive(size_of::SizeOf))]
#[cfg_attr(
    feature = "rkyv",
    derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)
)]
pub struct DynamicDecimal {
    /// Significand.
    sig: i128,

    /// The number of digits of the value that follow the decimal point.
    exponent: u8,
}

impl DynamicDecimal {
    /// The largest `DynamicDecimal` value (`i128::MAX`).
    pub const MAX: Self = DynamicDecimal::new(i128::MAX, 0);

    /// The smallest `DynamicDecimal` value (`i128::MIN`).
    pub const MIN: Self = DynamicDecimal::new(i128::MIN, 0);

    /// 0 as `DynamicDecimal`.
    pub const ZERO: Self = DynamicDecimal::new(0, 0);

    /// 1 as `DynamicDecimal`.
    pub const ONE: Self = DynamicDecimal::new(1, 0);

    /// Constructs a new `DynamicDecimal` with value `sig * 10**-exponent`.
    pub const fn new(sig: i128, exponent: u8) -> Self {
        if exponent == 0 {
            Self { sig, exponent }
        } else if sig == 0 {
            Self { sig, exponent: 0 }
        } else {
            /// Returns `(sig, exponent)` divided by `10**D` as many times as
            /// possible without dropping nonzero decimal digits.
            const fn reduce<const D: u8>(mut sig: i128, mut exponent: u8) -> (i128, u8) {
                // `10**D`.
                let scale = pow10(D as usize);

                // A multiple of `10**D` has at least `D` trailing zeros, since
                // `10 == 2*5`, so if any of the `D` trailing bits is nonzero,
                // we can skip the expensive remainder operation.
                while exponent >= D
                    && sig.unsigned_abs() >= scale.cast_unsigned()
                    && (sig & ((1 << D) - 1)) == 0
                    && sig % scale == 0
                {
                    sig /= scale;
                    exponent -= D;
                }
                (sig, exponent)
            }

            // Canonicalize in a few stages, so that we don't have to divide as
            // many times as otherwise for numbers that have many trailing
            // decimal zeros.
            let (sig, exponent) = reduce::<8>(sig, exponent);
            let (sig, exponent) = reduce::<4>(sig, exponent);
            let (sig, exponent) = reduce::<1>(sig, exponent);
            debug_assert!(sig % 10 != 0 || exponent == 0);
            Self { sig, exponent }
        }
    }

    /// Returns the canonical [significand](Self#representation).
    pub const fn significand(&self) -> i128 {
        self.sig
    }

    /// Returns the canonical [scale](Self#representation).
    pub const fn exponent(&self) -> u8 {
        self.exponent
    }
}

impl<const P: usize, const S: usize> From<Fixed<P, S>> for DynamicDecimal {
    /// Encodes `value`, for later deserialization into a new `Fixed` with
    /// possibly a different precision and scale.
    fn from(value: Fixed<P, S>) -> Self {
        Self::new(value.0, S as u8)
    }
}

impl<const P: usize, const S: usize> TryFrom<DynamicDecimal> for Fixed<P, S> {
    type Error = OutOfRange;

    /// Deserializes [DynamicDecimal] into `Fixed`.  If successful, returns the
    /// original value from before serialization.  If `S < value.exponent`, then
    /// some trailing decimals are lost, by rounding toward zero.  Returns an
    /// error` if the value is out of range for this type.
    fn try_from(value: DynamicDecimal) -> Result<Self, Self::Error> {
        Self::try_new_with_exponent(value.sig, S as i32 - value.exponent as i32).ok_or(OutOfRange)
    }
}

impl Debug for DynamicDecimal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        debug_decimal(self.sig, self.exponent as usize, f)
    }
}

impl Display for DynamicDecimal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        display_decimal(self.sig, self.exponent as usize, f)
    }
}

impl FromStr for DynamicDecimal {
    type Err = ParseDecimalError;

    /// Parses `s` as `DynamicDecimal`.
    ///
    /// This accepts the same forms as [f64::from_str], except that it rejects
    /// infinities and NaNs (which `Fixed` does not support), as well as
    /// out-of-range values.  Rounds overprecise values to the nearest
    /// representable value, rounding halfway values to even.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (sig, exponent) = parse_decimal(s, 0)?;
        match (sig, exponent) {
            (0, _) => Ok(Self::ZERO),
            (_, 1..) => {
                let sig = checked_pow10(exponent.cast_unsigned())
                    .and_then(|m| m.checked_mul(sig))
                    .ok_or(ParseDecimalError::OutOfRange)?;
                Ok(Self::new(sig, 0))
            }
            (_, 0) => Ok(Self { sig, exponent: 0 }),
            (_, -255..0) => Ok(Self::new(sig, (-exponent) as u8)),
            (_, ..-255) => {
                // We could "denormalize" by dividing `sig` by a power of 10 and
                // adjusting `exponent`, but the value would inevitably be zero
                // when we convert to `Fixed`, which is what we really care
                // about.
                Ok(Self::ZERO)
            }
        }
    }
}

impl TryFrom<u128> for DynamicDecimal {
    type Error = OutOfRange;

    fn try_from(value: u128) -> Result<Self, Self::Error> {
        Ok(Self::new(value.try_into().map_err(|_| OutOfRange)?, 0))
    }
}

macro_rules! from_int {
    ($type_name:ty) => {
        impl From<$type_name> for DynamicDecimal {
            fn from(value: $type_name) -> Self {
                Self::new(value as i128, 0)
            }
        }
    };
}
from_int!(i128);
from_int!(i64);
from_int!(i32);
from_int!(i16);
from_int!(i8);
from_int!(isize);
from_int!(u64);
from_int!(u32);
from_int!(u16);
from_int!(u8);
from_int!(usize);

impl From<DynamicDecimal> for i128 {
    /// Convert from `Fixed` to integer, rounding toward zero (the same
    /// semantics as Rust casts from float to integer).
    fn from(value: DynamicDecimal) -> Self {
        checked_pow10(value.exponent.into()).map_or(0, |divisor| value.sig / divisor)
    }
}

macro_rules! try_to_signed_int {
    ($type_name:ty) => {
        impl TryFrom<DynamicDecimal> for $type_name {
            type Error = OutOfRange;

            /// Convert from `Fixed` to integer, rounding toward zero (the same
            /// semantics as Rust casts from float to integer).
            fn try_from(value: DynamicDecimal) -> Result<Self, Self::Error> {
                match checked_pow10(value.exponent.into()) {
                    Some(divisor) => (value.sig / divisor).try_into().map_err(|_| OutOfRange),
                    None => Ok(0),
                }
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
        impl TryFrom<DynamicDecimal> for $type_name {
            type Error = OutOfRange;

            /// Convert from `Fixed` to integer, rounding toward zero (the same
            /// semantics as Rust casts from float to integer).
            ///
            /// Because this rounds toward zero, negative values greater than -1
            /// will convert to 0 instead of an out-of-range error.
            fn try_from(value: DynamicDecimal) -> Result<Self, Self::Error> {
                match checked_pow10(value.exponent.into()) {
                    Some(divisor) => (value.sig / divisor).try_into().map_err(|_| OutOfRange),
                    None => Ok(0),
                }
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

impl From<DynamicDecimal> for f64 {
    fn from(value: DynamicDecimal) -> Self {
        value.significand() as f64 / pow10(value.exponent() as usize) as f64
    }
}

impl From<DynamicDecimal> for f32 {
    fn from(value: DynamicDecimal) -> Self {
        value.significand() as f32 / pow10(value.exponent() as usize) as f32
    }
}

impl TryFrom<f64> for DynamicDecimal {
    type Error = OutOfRange;

    /// Convert `value` to `DynamicDecimal`, reporting an error if `value` is
    /// out of range.
    fn try_from(value: f64) -> Result<Self, Self::Error> {
        // We need to convert binary to decimal.  We could do better, in theory,
        // than formatting to a string and parsing back, but possibly not much
        // better.  If this shows up as important in profiles, then we can
        // improve it, especially if there are important special cases
        // (e.g. integers).
        let mut buf = SmallString::<[u8; 64]>::new();
        write!(&mut buf, "{value:.15e}").unwrap();
        buf.parse().map_err(|_| OutOfRange)
    }
}

impl PartialOrd for DynamicDecimal {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DynamicDecimal {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.exponent.cmp(&other.exponent) {
            Ordering::Less => {
                if let Some(multiplier) =
                    checked_pow10(other.exponent as u32 - self.exponent as u32)
                {
                    I256::from_product(self.sig, multiplier).cmp(&I256::from(other.sig))
                } else {
                    match self.sig.cmp(&0) {
                        Ordering::Equal => 0.cmp(&other.sig),
                        ordering => ordering,
                    }
                }
            }
            Ordering::Equal => self.sig.cmp(&other.sig),
            Ordering::Greater => {
                if let Some(multiplier) =
                    checked_pow10(self.exponent as u32 - other.exponent as u32)
                {
                    I256::from(self.sig).cmp(&I256::from_product(other.sig, multiplier))
                } else {
                    match other.sig.cmp(&0) {
                        Ordering::Equal => self.sig.cmp(&0),
                        ordering => ordering.reverse(),
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{DynamicDecimal, OutOfRange, ParseDecimalError};

    #[test]
    fn eq() {
        assert_eq!(DynamicDecimal::new(123, 2), DynamicDecimal::new(1230, 3));
        assert_eq!(DynamicDecimal::new(1230, 3), DynamicDecimal::new(123, 2));
        assert_eq!(DynamicDecimal::new(123, 2), DynamicDecimal::new(123, 2));
        assert_ne!(DynamicDecimal::new(123, 2), DynamicDecimal::new(123, 3));
    }

    #[test]
    fn compare() {
        type DD = DynamicDecimal;
        fn check_comparisons(dx: DD, dy: DD, x: i128, y: i128) {
            assert_eq!(dx == dy, x == y);
            assert_eq!(dx != dy, x != y);
            assert_eq!(dx > dy, x > y);
            assert_eq!(dx >= dy, x >= y);
            assert_eq!(dx < dy, x < y);
            assert_eq!(dx <= dy, x <= y);
        }

        for x in -999..=999 {
            let fx = DD::new(x, 1);
            for y in -999..=999 {
                check_comparisons(fx, DD::new(y, 0), x, y * 10);
                check_comparisons(fx, DD::new(y, 1), x, y);
                check_comparisons(fx, DD::new(y, 2), x * 10, y);
            }
        }

        // Some handwritten overflow cases.
        check_comparisons(DD::new(0, 40), DD::new(0, 0), 0, 0);
        check_comparisons(DD::new(0, 0), DD::new(0, 40), 0, 0);

        check_comparisons(DD::new(1, 40), DD::new(1, 0), 0, 1);
        check_comparisons(DD::new(1, 40), DD::new(-1, 0), 1, 0);
        check_comparisons(DD::new(-1, 40), DD::new(1, 0), 0, 1);
        check_comparisons(DD::new(-1, 40), DD::new(-1, 0), 1, 0);

        check_comparisons(DD::new(1, 0), DD::new(1, 40), 1, 0);
        check_comparisons(DD::new(1, 0), DD::new(-1, 40), 1, 0);
        check_comparisons(DD::new(-1, 0), DD::new(1, 40), 0, 1);
        check_comparisons(DD::new(-1, 0), DD::new(-1, 40), 0, 1);
    }

    #[test]
    fn from_str() {
        for (s, expect) in [
            ("0", Ok("0")),
            ("0.", Ok("0")),
            (".0", Ok("0")),
            ("-0", Ok("0")),
            ("+0", Ok("0")),
            ("--0", Err(ParseDecimalError::SyntaxError)),
            ("-+0", Err(ParseDecimalError::SyntaxError)),
            ("0x", Err(ParseDecimalError::SyntaxError)),
            ("0e5x", Err(ParseDecimalError::SyntaxError)),
            ("1.23", Ok("1.23")),
            ("-1.23", Ok("-1.23")),
            ("+1.23", Ok("1.23")),
            ("99999999", Ok("99999999")),
            ("999999999", Ok("999999999")),
            ("999999999E-1", Ok("99999999.9")),
            ("9999999999e-1", Ok("999999999.9")),
            ("9999999999E-2", Ok("99999999.99")),
            ("99999999999e-2", Ok("999999999.99")),
            ("99999999999e-3", Ok("99999999.999")),
            ("99999999991e-3", Ok("99999999.991")),
            // This value overflows the range of `i128` as an integer, so it
            // triggers the case where we stop accepting digits and simply
            // adjust the exponent instead.
            (
                "111111111111111111111111111111111111111111e-34",
                Ok("11111111.1111111111111111111111111111111"),
            ),
            // This value overflows the range of `i128` in the fraction, so it
            // triggers the case where we stop accepting digits and simply
            // adjust the exponent instead.
            (
                "1.23456788901234567890123456789012345678890123456",
                Ok("1.23456788901234567890123456789012345678"),
            ),
            // This value positively overflows the exponent.
            ("1e999999999999999", Err(ParseDecimalError::OutOfRange)),
            // This value positively overflows the exponent but the value is 0.
            ("0e999999999999999", Ok("0")),
            // This value negatively overflows the exponent.
            ("1e-999999999999999", Ok("0")),
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
                Ok("0"),
            ),
            ("123e5", Ok("12300000")),
            ("123E4", Ok("1230000")),
            ("123e3", Ok("123000")),
            ("123e2", Ok("12300")),
            ("123e1", Ok("1230")),
            ("123e0", Ok("123")),
            ("123e-1", Ok("12.3")),
            ("123e-2", Ok("1.23")),
            (".123", Ok("0.123")),
            (".124", Ok("0.124")),
            (".125", Ok("0.125")),
            (".126", Ok("0.126")),
            (".133", Ok("0.133")),
            (".134", Ok("0.134")),
            (".135", Ok("0.135")),
            (".136", Ok("0.136")),
            ("1e38", Ok("100000000000000000000000000000000000000")),
            ("1e39", Err(ParseDecimalError::OutOfRange)),
            ("1e-255", Ok("0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001")),
            ("1e-256", Ok("0")),
        ] {
            println!("{s}: {:?}", s.parse::<DynamicDecimal>());
            assert_eq!(
                s.parse::<DynamicDecimal>().map(|d| d.to_string()),
                expect.map(|d| d.to_string())
            );
        }
    }

    #[test]
    fn to_integer() {
        for x in -9999..=9999 {
            let f = DynamicDecimal::new(x, 1);
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

        assert_eq!(i128::from(DynamicDecimal::new(1, 40)), 0);
        assert_eq!(i128::from(DynamicDecimal::new(i128::MAX, 0)), i128::MAX);
        assert_eq!(i64::try_from(DynamicDecimal::new(1, 40)), Ok(0));
        assert_eq!(
            i64::try_from(DynamicDecimal::new(i128::MAX, 0)),
            Err(OutOfRange)
        );
        assert_eq!(
            i64::try_from(DynamicDecimal::new(i128::MIN, 0)),
            Err(OutOfRange)
        );
    }
}
