use num::{FromPrimitive, Num, One, Signed, ToPrimitive, Zero};
use rust_decimal::{Decimal, MathematicalOps};
use size_of::SizeOf;
use std::hash::Hash;
use std::{
    fmt::Display,
    ops::{Add, AddAssign, Div, Mul, Neg, Rem, Sub},
    str::FromStr,
};

use crate::algebra::F64;

use super::precision::Precision;
use super::scale::Scale;

/// A wrapper type around [`rust_decimal::Decimal`].
#[derive(Debug, Default, Clone, Copy, Eq, PartialOrd, Ord, SizeOf)]
#[cfg_attr(feature = "with-serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "with-serde", serde(transparent))]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct SQLDecimal {
    inner: Decimal,
    #[cfg_attr(feature = "with-serde", serde(skip, default))]
    runtime_precision: Precision,
    #[cfg_attr(feature = "with-serde", serde(skip, default))]
    runtime_scale: Scale,
}

impl PartialEq for SQLDecimal {
    fn eq(&self, other: &Self) -> bool {
        self.inner.eq(&other.inner)
    }
}

impl Hash for SQLDecimal {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.inner.hash(state)
    }
}

impl SQLDecimal {
    /// Wrapper around [`rust_decimal::Decimal::new`] with precision
    /// and scale information
    pub fn new(num: i64, precision: Precision, scale: Scale) -> Self {
        let inner = Decimal::new(num, scale.inner());

        Self {
            inner,
            runtime_precision: precision,
            runtime_scale: scale,
        }
    }

    pub fn from_i8(num: i8, p: Precision, s: Scale) -> Option<Self> {
        let mut inner = Decimal::from_i8(num)?;
        inner.rescale(s.inner());

        Some(Self {
            inner,
            runtime_precision: p,
            runtime_scale: s,
        })
    }

    pub fn from_i16(num: i16, p: Precision, s: Scale) -> Option<Self> {
        let mut inner = Decimal::from_i16(num)?;
        inner.rescale(s.inner());

        Some(Self {
            inner,
            runtime_precision: p,
            runtime_scale: s,
        })
    }

    pub fn from_i32(num: i32, p: Precision, s: Scale) -> Option<Self> {
        let mut inner = Decimal::from_i32(num)?;
        inner.rescale(s.inner());

        Some(Self {
            inner,
            runtime_precision: p,
            runtime_scale: s,
        })
    }

    pub fn from_i64(num: i64, p: Precision, s: Scale) -> Option<Self> {
        let mut inner = Decimal::from_i64(num)?;
        inner.rescale(s.inner());

        Some(Self {
            inner,
            runtime_precision: p,
            runtime_scale: s,
        })
    }

    pub fn from_isize(num: isize, p: Precision, s: Scale) -> Option<Self> {
        let mut inner = Decimal::from_isize(num)?;
        inner.rescale(s.inner());

        Some(Self {
            inner,
            runtime_precision: p,
            runtime_scale: s,
        })
    }

    pub fn from_usize(num: usize, p: Precision, s: Scale) -> Option<Self> {
        let mut inner = Decimal::from_usize(num)?;
        inner.rescale(s.inner());

        Some(Self {
            inner,
            runtime_precision: p,
            runtime_scale: s,
        })
    }

    pub fn from_f64(num: f64, p: Precision, s: Scale) -> Option<Self> {
        let mut inner = Decimal::from_f64(num)?;
        inner.rescale(s.inner());

        Some(Self {
            inner,
            runtime_precision: p,
            runtime_scale: s,
        })
    }

    pub fn from_f32(num: f32, p: Precision, s: Scale) -> Option<Self> {
        let mut inner = Decimal::from_f32(num)?;
        inner.rescale(s.inner());

        Some(Self {
            inner,
            runtime_precision: p,
            runtime_scale: s,
        })
    }

    /// Creates [`SQLDecimal`] by parsing the String, and then applying the
    /// given precision and scale
    pub fn from_str_with_precision_scale(
        s: &str,
        precision: Precision,
        scale: Scale,
    ) -> Result<Self, String> {
        let mut inner = Decimal::from_str(s).map_err(|e| e.to_string())?;

        inner.rescale(scale.inner());

        let calc_precision = Precision::extract_precision_from_decimal(inner)?;
        if calc_precision > precision {
            return Err(format!(
                "cannot store the given value ({s}) with precision ({calc_precision}) in the desired precision ({precision})",
            ));
        }

        Ok(Self {
            inner,
            runtime_precision: precision,
            runtime_scale: scale,
        })
    }

    /// Rescales the decimal type to the given [`Scale`]
    pub fn rescale(&self, scale: Scale) -> Self {
        let mut out = *self;
        out.inner.rescale(scale.inner());
        out.runtime_scale = scale;

        out
    }

    /// Returns the precision
    pub fn precision(&self) -> Precision {
        self.runtime_precision.to_owned()
    }

    /// Sets the precision to the given value
    pub fn set_precision(&self, precision: Precision) -> Result<Self, String> {
        let mut out = *self;

        if Precision::extract_precision_from_decimal(out.inner)? > precision {
            return Err("cannot store the given value in the desired precision".to_owned());
        }

        out.runtime_precision = precision;

        Ok(out)
    }

    /// Returns the scale
    pub fn scale(&self) -> Scale {
        self.runtime_scale.to_owned()
    }

    /// Rescales the inner decimal type to the `runtime_scale`
    ///
    /// Also asserts that the precision after rescaling isn't higher than the
    /// specified run time precision
    fn apply_scaling_rules(&mut self) {
        self.inner.rescale(self.runtime_scale.inner());

        assert!(
            self.runtime_precision
                >= Precision::extract_precision_from_decimal(self.inner).unwrap()
        );
    }

    /// Calcuates the natural logarithm of the given value
    pub fn ln(&self) -> Option<F64> {
        self.inner.ln().to_f64().map(F64::new)
    }

    /// Calculates the base 10 logarithm of the given value
    pub fn log10(&self) -> Option<F64> {
        self.inner.log10().to_f64().map(F64::new)
    }

    /// Rounds the decimal value to the specified number of decimal places
    /// for the fractional portion.
    /// Rounding follows "Banker's Rounding".
    pub fn round_dp(&self, val: u32) -> Self {
        let mut out = *self;
        out.inner = out.inner.round_dp(val);
        out.apply_scaling_rules();
        out
    }

    /// Truncates the value.
    /// No rounding is performed.
    pub fn trunc(&self) -> Self {
        let mut out = *self;
        out.inner = out.inner.trunc();

        out
    }

    /// Truncates the value to the specified number of decimal places for the
    /// fractional portion.
    /// This is true truncation, no rounding is performed.
    pub fn trunc_to_dp(&self, dp: u32) -> Self {
        let mut out = *self;
        out.inner = out.inner.trunc_with_scale(dp);
        out.apply_scaling_rules();
        out
    }

    /// Calculate the square root of the value.
    pub fn sqrt(&self) -> Option<F64> {
        self.inner.sqrt()?.to_f64().map(F64::new)
    }

    /// Raises self to the given exponent.
    pub fn powd(&self, exp: SQLDecimal) -> Option<F64> {
        self.inner.powd(exp.inner).to_f64().map(F64::new)
    }

    /// Calculates the floor value of the given decimal.
    ///
    /// The decimal type is rescaled to [`Scale::min`].
    pub fn floor(&self) -> Self {
        let mut out = *self;
        out.inner = out.inner.floor();
        out.runtime_scale = Scale::min();
        out.apply_scaling_rules();
        out
    }

    /// Calculates the ceil value of the given decimal.
    ///
    /// The decimal type is rescaled to [`Scale::min`].
    pub fn ceil(&self) -> Self {
        let mut out = *self;
        out.inner = out.inner.ceil();
        out.runtime_scale = Scale::min();
        out.apply_scaling_rules();
        out
    }
}

/// Implementation required for [`Num`] trait
impl One for SQLDecimal {
    fn one() -> Self {
        Self {
            inner: Decimal::one(),
            runtime_precision: Precision::max(),
            runtime_scale: Scale::max(),
        }
    }
}

impl Num for SQLDecimal {
    type FromStrRadixErr = String;

    fn from_str_radix(str: &str, radix: u32) -> Result<Self, Self::FromStrRadixErr> {
        let inner = Decimal::from_str_radix(str, radix).map_err(|e| e.to_string())?;

        Ok(Self {
            inner,
            runtime_precision: Precision::max(),
            runtime_scale: Scale::max(),
        })
    }
}

impl Signed for SQLDecimal {
    fn abs(&self) -> Self {
        let mut out = *self;
        out.inner = out.inner.abs();
        out.apply_scaling_rules();
        out
    }

    fn abs_sub(&self, other: &Self) -> Self {
        let mut out = *self;
        out.inner = out.inner.abs_sub(&other.inner);
        out.apply_scaling_rules();
        out
    }

    fn signum(&self) -> Self {
        let mut out = *self;
        out.inner = out.inner.signum();
        out.apply_scaling_rules();
        out
    }

    fn is_positive(&self) -> bool {
        self.inner.is_sign_positive()
    }

    fn is_negative(&self) -> bool {
        self.inner.is_sign_negative()
    }
}

impl ToPrimitive for SQLDecimal {
    fn to_i64(&self) -> Option<i64> {
        self.inner.to_i64()
    }

    fn to_u64(&self) -> Option<u64> {
        self.inner.to_u64()
    }

    fn to_f32(&self) -> Option<f32> {
        self.inner.to_f32()
    }

    fn to_f64(&self) -> Option<f64> {
        self.inner.to_f64()
    }
}

impl Display for SQLDecimal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner)
    }
}

/// Implementation required for [`Num`] trait
impl Zero for SQLDecimal {
    fn zero() -> Self {
        let inner = Decimal::ZERO;

        Self {
            inner,
            runtime_precision: Precision::max(),
            runtime_scale: Scale::max(),
        }
    }

    // For zero, we only want to check if the inner value is zero
    fn is_zero(&self) -> bool {
        self.inner == Decimal::ZERO
    }
}

impl Add for SQLDecimal {
    type Output = SQLDecimal;

    fn add(self, rhs: Self) -> Self::Output {
        assert_eq!(
            self.runtime_precision, rhs.runtime_precision,
            "cannot add decimal types with unequal precision"
        );
        assert_eq!(
            self.runtime_scale, rhs.runtime_scale,
            "cannot add decimal types with unequal scale"
        );

        let mut out = self;
        out.inner = self.inner + rhs.inner;
        out.apply_scaling_rules();
        out
    }
}

impl<'a> Add for &'a SQLDecimal {
    type Output = SQLDecimal;

    fn add(self, rhs: Self) -> Self::Output {
        assert_eq!(
            self.runtime_precision, rhs.runtime_precision,
            "cannot add decimal types with unequal precision"
        );
        assert_eq!(
            self.runtime_scale, rhs.runtime_scale,
            "cannot add decimal types with unequal scale"
        );

        let mut out = *self;
        out.inner = self.inner + rhs.inner;
        out.apply_scaling_rules();
        out
    }
}

impl Sub for SQLDecimal {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        assert_eq!(
            self.runtime_precision, rhs.runtime_precision,
            "cannot subtract decimal types with unequal precision"
        );
        assert_eq!(
            self.runtime_scale, rhs.runtime_scale,
            "cannot subtract decimal types with unequal scale"
        );

        let mut out = self;
        out.inner = self.inner - rhs.inner;
        out.apply_scaling_rules();
        out
    }
}

impl Mul for SQLDecimal {
    type Output = Self;

    fn mul(self, rhs: Self) -> Self::Output {
        assert_eq!(
            self.runtime_precision, rhs.runtime_precision,
            "cannot multiply decimal types with unequal precision"
        );
        assert_eq!(
            self.runtime_scale, rhs.runtime_scale,
            "cannot multiply decimal types with unequal scale"
        );

        let mut out = self;
        out.inner = self.inner * rhs.inner;
        out.apply_scaling_rules();
        out
    }
}

impl Div for SQLDecimal {
    type Output = Self;

    fn div(self, rhs: Self) -> Self::Output {
        assert_eq!(
            self.runtime_precision, rhs.runtime_precision,
            "cannot divide decimal types with unequal precision"
        );
        assert_eq!(
            self.runtime_scale, rhs.runtime_scale,
            "cannot divide decimal types with unequal scale"
        );

        let mut out = self;
        out.inner = self.inner / rhs.inner;
        out.apply_scaling_rules();
        out
    }
}

impl Rem for SQLDecimal {
    type Output = Self;

    fn rem(self, rhs: Self) -> Self::Output {
        assert_eq!(
            self.runtime_precision, rhs.runtime_precision,
            "cannot perform modulo on decimal types with unequal precision"
        );
        assert_eq!(
            self.runtime_scale, rhs.runtime_scale,
            "cannot perform modulo on decimal types with unequal scale"
        );

        let mut out = self;
        out.inner = self.inner.rem(rhs.inner);

        out
    }
}

impl Neg for SQLDecimal {
    type Output = SQLDecimal;

    fn neg(self) -> Self::Output {
        let mut out = self;
        out.inner = self.inner.neg();
        out
    }
}

impl AddAssign for SQLDecimal {
    fn add_assign(&mut self, rhs: Self) {
        self.inner += rhs.inner;
    }
}

impl<'a> AddAssign<&'a SQLDecimal> for SQLDecimal {
    fn add_assign(&mut self, rhs: &'a SQLDecimal) {
        self.inner += rhs.inner;
    }
}

impl<'a> AddAssign<&'a SQLDecimal> for &'a mut SQLDecimal {
    fn add_assign(&mut self, rhs: &'a SQLDecimal) {
        self.inner += rhs.inner;
    }
}

#[cfg(test)]
mod tests {
    use rkyv::Deserialize;
    use std::str::FromStr;

    use super::{Precision, SQLDecimal, Scale};
    use rust_decimal::Decimal;

    #[test]
    fn decimal_deserialization() {
        let json = r###"{ "price": 10000 }"###;

        #[derive(Debug, PartialEq, Eq, serde::Deserialize)]
        struct Item {
            price: SQLDecimal,
        }

        let item: Item = serde_json::from_str(json).unwrap();

        let expected = Item {
            price: SQLDecimal {
                inner: Decimal::from_str("10000").unwrap(),
                runtime_precision: Precision::default(),
                runtime_scale: Scale::default(),
            },
        };

        assert_eq!(item, expected);
    }

    #[test]
    fn decimal_encode_decode() {
        for input in [
            SQLDecimal::from_str_with_precision_scale(
                "0.0",
                Precision::try_from(2).unwrap(),
                Scale::try_from(1).unwrap(),
            )
            .unwrap(),
            SQLDecimal::from_str_with_precision_scale(
                "10.0",
                Precision::try_from(3).unwrap(),
                Scale::try_from(1).unwrap(),
            )
            .unwrap(),
            SQLDecimal::from_str_with_precision_scale(
                "-10.0",
                Precision::try_from(3).unwrap(),
                Scale::try_from(1).unwrap(),
            )
            .unwrap(),
            SQLDecimal {
                inner: Decimal::MAX,
                runtime_precision: Precision::max(),
                runtime_scale: Scale::max(),
            },
            SQLDecimal {
                inner: Decimal::MIN,
                runtime_precision: Precision::max(),
                runtime_scale: Scale::max(),
            },
        ]
        .into_iter()
        {
            let encoded = rkyv::to_bytes::<_, 256>(&input).unwrap();
            let archived = unsafe { rkyv::archived_root::<SQLDecimal>(&encoded[..]) };
            let decoded: SQLDecimal = archived.deserialize(&mut rkyv::Infallible).unwrap();
            assert_eq!(decoded, input);
        }
    }
}
