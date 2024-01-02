use num::{FromPrimitive, Num, One, Signed, ToPrimitive, Zero};
use rust_decimal::{Decimal, MathematicalOps};
use size_of::SizeOf;
use std::hash::Hash;
use std::{
    fmt::Display,
    ops::{Add, AddAssign, Div, Mul, Neg, Rem, Sub},
    str::FromStr,
};

mod inner_types {
    use rust_decimal::Decimal;
    use size_of::SizeOf;
    use std::fmt::Display;

    const MIN_DECIMAL_PRECISION: u32 = 1;
    const MAX_DECIMAL_PRECISION: u32 = 38;

    const MIN_DECIMAL_SCALE: u32 = 0;
    const MAX_DECIMAL_SCALE: u32 = 28;

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
    #[cfg_attr(feature = "with-serde", derive(serde::Serialize, serde::Deserialize))]
    pub struct Precision(u32);

    impl Precision {
        pub fn new(val: Decimal) -> Result<Self, String> {
            let mut precision = val.mantissa().checked_ilog10().unwrap_or(0);

            if precision == 0 {
                precision = val.scale() + 1;
            }

            Self::try_from(precision)
        }

        pub fn inner(&self) -> u32 {
            self.0
        }

        pub fn max() -> Self {
            Self(MAX_DECIMAL_PRECISION)
        }
    }

    impl TryFrom<u32> for Precision {
        type Error = String;

        fn try_from(value: u32) -> Result<Self, Self::Error> {
            match value {
                x if (MIN_DECIMAL_PRECISION..=MAX_DECIMAL_PRECISION).contains(&x) => Ok(Precision(x)),
                _ => Err(format!(
                    "Numeric precision {value} must be between {MIN_DECIMAL_PRECISION} and {MAX_DECIMAL_PRECISION}"
                )),
            }
        }
    }

    impl Display for Precision {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    // In Postgres, the scale can be between [-1000, 1000]
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
    #[cfg_attr(feature = "with-serde", derive(serde::Serialize, serde::Deserialize))]
    pub struct Scale(u32);

    impl Scale {
        pub fn inner(&self) -> u32 {
            self.0
        }

        pub fn min() -> Self {
            Self(MIN_DECIMAL_SCALE)
        }

        pub const fn max() -> Self {
            Self(MAX_DECIMAL_SCALE)
        }

        pub fn apply(&self, val: &mut Decimal) {
            val.rescale(self.0)
        }
    }

    impl TryFrom<u32> for Scale {
        type Error = String;

        fn try_from(value: u32) -> Result<Self, Self::Error> {
            match value {
                x if (MIN_DECIMAL_SCALE..=MAX_DECIMAL_SCALE).contains(&x) => Ok(Scale(x)),
                _ => Err(format!(
                "Numeric scale {value} must be between {MIN_DECIMAL_SCALE} and {MAX_DECIMAL_SCALE}"
            )),
            }
        }
    }

    impl Display for Scale {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.0)
        }
    }
}

use inner_types::*;

/// A wrapper type around [`rust_decimal::Decimal`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, SizeOf)]
#[cfg_attr(feature = "with-serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct SQLDecimal {
    inner: Decimal,
    max_precision: Precision,
    max_scale: Scale,
}

impl FromStr for SQLDecimal {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let inner = Decimal::from_str(s).map_err(|e| e.to_string())?;

        Ok(Self {
            inner,
            max_precision: Precision::max(),
            max_scale: Scale::try_from(inner.scale())?,
        })
    }
}

impl TryFrom<Decimal> for SQLDecimal {
    type Error = String;

    fn try_from(mut inner: Decimal) -> Result<Self, Self::Error> {
        let scale = Scale::max();
        scale.apply(&mut inner);

        let precision = Precision::max();

        Ok(Self {
            inner,
            max_precision: precision,
            max_scale: scale,
        })
    }
}

impl SQLDecimal {
    pub fn new(precision: u32, scale: u32) -> Self {
        let inner = Default::default();

        let scale = Scale::try_from(scale).unwrap();
        let precision = Precision::try_from(precision).unwrap();

        Self {
            inner,
            max_precision: precision,
            max_scale: scale,
        }
    }

    pub fn from_str_with_precision_scale(
        s: &str,
        _precision: u32,
        scale: u32,
    ) -> Result<Self, String> {
        let mut inner = Decimal::from_str(s).map_err(|e| e.to_string())?;

        // TODO: use the precision value from the parameter
        let max_precision = Precision::max();

        let max_scale = Scale::try_from(scale)?;

        inner.rescale(max_scale.inner());

        Ok(Self {
            inner,
            max_precision,
            max_scale,
        })
    }

    pub fn rescale(mut self, val: u32) -> Result<Self, String> {
        self.max_scale = Scale::try_from(val)?;
        self.max_scale.apply(&mut self.inner);

        Ok(self)
    }

    pub fn precision(mut self, val: u32) -> Result<Self, String> {
        self.max_precision = Precision::try_from(val)?;

        Ok(self)
    }

    fn apply_scaling_rules(&mut self) {
        self.max_scale.apply(&mut self.inner)
    }

    pub fn trunc(&self) -> Self {
        let mut out = *self;
        out.inner = out.inner.trunc();
        out.apply_scaling_rules();
        out
    }

    pub fn rescale_with_prec(&mut self, precision: u32, scale: i32) {
        let scale = scale as u32;
        self.inner.rescale(scale);

        self.max_scale = Scale::try_from(self.inner.scale()).unwrap();
        self.max_precision = Precision::try_from(precision).unwrap();
    }

    pub fn ln(&self) -> Self {
        let mut out = *self;
        out.inner = out.inner.ln();
        out.apply_scaling_rules();
        out
    }

    pub fn log10(&self) -> Self {
        let mut out = *self;
        out.inner = out.inner.log10();
        out.apply_scaling_rules();
        out
    }

    pub fn round_dp(&self, val: u32) -> Self {
        let mut out = *self;
        out.inner = out.inner.round_dp(val);
        out.apply_scaling_rules();
        out
    }

    pub fn trunc_with_scale(&self, scale: u32) -> Self {
        let mut out = *self;
        out.inner = out.inner.trunc_with_scale(scale);
        out.apply_scaling_rules();
        out
    }

    pub fn sqrt(&self) -> Option<Self> {
        let mut out = *self;
        out.inner = out.inner.sqrt()?;
        out.inner.normalize_assign();
        out.max_scale = Scale::try_from(out.inner.scale()).unwrap_or(Scale::max());
        out.apply_scaling_rules();
        Some(out)
    }

    pub fn powd(&self, exp: SQLDecimal) -> Self {
        let mut out = *self;
        out.inner = out.inner.powd(exp.inner);
        out.apply_scaling_rules();
        out
    }

    pub fn floor(&self) -> Self {
        let mut out = *self;
        out.inner = out.inner.floor();
        out.max_scale = Scale::min();
        out.apply_scaling_rules();
        out
    }

    pub fn ceil(&self) -> Self {
        let mut out = *self;
        out.inner = out.inner.ceil();
        out.max_scale = Scale::min();
        out.apply_scaling_rules();
        out
    }
}

impl One for SQLDecimal {
    fn one() -> Self {
        Self::try_from(Decimal::ONE).unwrap()
    }
}

impl Rem for SQLDecimal {
    type Output = Self;

    fn rem(self, rhs: Self) -> Self::Output {
        let mut out = self;
        out.inner = self.inner.rem(rhs.inner);

        out
    }
}

impl Num for SQLDecimal {
    type FromStrRadixErr = String;

    fn from_str_radix(str: &str, radix: u32) -> Result<Self, Self::FromStrRadixErr> {
        let inner = Decimal::from_str_radix(str, radix).map_err(|e| e.to_string())?;

        Self::try_from(inner)
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

impl FromPrimitive for SQLDecimal {
    fn from_i64(n: i64) -> Option<Self> {
        let inner = Decimal::from_i64(n)?;

        Self::try_from(inner).ok()
    }

    fn from_u64(n: u64) -> Option<Self> {
        let inner = Decimal::from_u64(n)?;

        Self::try_from(inner).ok()
    }

    fn from_f32(n: f32) -> Option<Self> {
        let inner = Decimal::from_f32(n)?;

        Self::try_from(inner).ok()
    }

    fn from_f64(n: f64) -> Option<Self> {
        let inner = Decimal::from_f64(n)?;

        Self::try_from(inner).ok()
    }
}

impl Display for SQLDecimal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner)
    }
}

impl From<i32> for SQLDecimal {
    fn from(value: i32) -> Self {
        let inner = Decimal::from(value);

        Self::try_from(inner).unwrap()
    }
}

impl From<i64> for SQLDecimal {
    fn from(value: i64) -> Self {
        let inner = Decimal::from(value);
        let scale = inner.scale();

        SQLDecimal {
            inner,
            max_precision: Precision::max(),
            max_scale: Scale::try_from(scale).unwrap(),
        }
    }
}

impl From<isize> for SQLDecimal {
    fn from(value: isize) -> Self {
        let inner = Decimal::from(value);
        let scale = inner.scale();

        SQLDecimal {
            inner,
            max_precision: Precision::max(),
            max_scale: Scale::try_from(scale).unwrap(),
        }
    }
}

impl Zero for SQLDecimal {
    fn zero() -> Self {
        let inner = Decimal::ZERO;

        Self::try_from(inner).unwrap()
    }

    // For zero, we only want to check if the inner value is zero
    fn is_zero(&self) -> bool {
        self.inner == Decimal::ZERO
    }
}

impl Add for SQLDecimal {
    type Output = SQLDecimal;

    fn add(self, rhs: Self) -> Self::Output {
        let mut out = self;
        out.inner = self.inner + rhs.inner;
        out.apply_scaling_rules();
        out
    }
}

impl<'a> Add for &'a SQLDecimal {
    type Output = SQLDecimal;

    fn add(self, rhs: Self) -> Self::Output {
        let mut out = *self;
        out.inner = self.inner + rhs.inner;
        out.apply_scaling_rules();
        out
    }
}

impl Sub for SQLDecimal {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        let mut out = self;
        out.inner = self.inner - rhs.inner;
        out.apply_scaling_rules();
        out
    }
}

impl Mul for SQLDecimal {
    type Output = Self;

    fn mul(self, rhs: Self) -> Self::Output {
        let mut out = self;
        out.inner = self.inner * rhs.inner;
        out.apply_scaling_rules();
        out
    }
}

impl Div for SQLDecimal {
    type Output = Self;

    fn div(self, rhs: Self) -> Self::Output {
        let mut out = self;
        out.inner = self.inner / rhs.inner;
        out.apply_scaling_rules();
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
