// Newtype for rust_decimal::Decimal because it doesn't implement Encode and
// Decode

use ::serde::{Deserialize, Serialize};
use bincode::{Decode, Encode};
use num::{traits::Inv, Num, One, Zero};
use size_of::*;

#[cfg(feature = "decimal_maths")]
use num::traits::Pow;
#[cfg(feature = "decimal_proptest")]
use proptest_derive::Arbitrary;
#[cfg(feature = "decimal_maths")]
use rust_decimal::MathematicalOps;

use rust_decimal::{
    prelude::{FromPrimitive, Signed, ToPrimitive},
    Decimal as RustDecimal, Error as RustDecimalError, RoundingStrategy,
};
use std::{
    fmt::{Display, Formatter, LowerExp, UpperExp},
    iter::{Product, Sum},
    ops::{Add, AddAssign, Div, DivAssign, Mul, MulAssign, Neg, Rem, RemAssign, Sub, SubAssign},
    str::FromStr,
};

#[derive(
    Copy,
    Default,
    Eq,
    Ord,
    Clone,
    Hash,
    PartialEq,
    PartialOrd,
    SizeOf,
    Serialize,
    Deserialize,
    Debug,
    Encode,
    Decode,
)]
#[cfg_attr(feature = "decimal_proptest", derive(Arbitrary))]
pub struct Decimal(#[bincode(with_serde)] RustDecimal);

impl<T> From<T> for Decimal
where
    RustDecimal: From<T>,
{
    fn from(value: T) -> Self {
        Self(RustDecimal::from(value))
    }
}

impl Decimal {
    pub const MIN: Decimal = Self(RustDecimal::MIN);
    pub const MAX: Decimal = Self(RustDecimal::MAX);
    pub const ZERO: Decimal = Self(RustDecimal::ZERO);
    pub const ONE: Decimal = Self(RustDecimal::ONE);
    pub const NEGATIVE_ONE: Decimal = Self(RustDecimal::NEGATIVE_ONE);
    pub const TWO: Decimal = Self(RustDecimal::TWO);
    pub const TEN: Decimal = Self(RustDecimal::TEN);
    pub const ONE_HUNDRED: Decimal = Self(RustDecimal::ONE_HUNDRED);
    pub const ONE_THOUSAND: Decimal = Self(RustDecimal::ONE_THOUSAND);

    pub fn new(num: i64, scale: u32) -> Decimal {
        Self(RustDecimal::new(num, scale))
    }

    pub fn try_new(num: i64, scale: u32) -> Result<Decimal, RustDecimalError> {
        Ok(Self(RustDecimal::try_new(num, scale)?))
    }

    pub fn from_i128_with_scale(num: i128, scale: u32) -> Decimal {
        Self(RustDecimal::from_i128_with_scale(num, scale))
    }

    pub fn try_from_i128_with_scale(num: i128, scale: u32) -> Result<Decimal, RustDecimalError> {
        Ok(Self(RustDecimal::try_from_i128_with_scale(num, scale)?))
    }

    pub const fn from_parts(lo: u32, mid: u32, hi: u32, negative: bool, scale: u32) -> Decimal {
        Self(RustDecimal::from_parts(lo, mid, hi, negative, scale))
    }

    pub fn from_scientific(value: &str) -> Result<Decimal, RustDecimalError> {
        Ok(Self(RustDecimal::from_scientific(value)?))
    }

    pub fn from_str_radix(str: &str, radix: u32) -> Result<Self, RustDecimalError> {
        Ok(Self(RustDecimal::from_str_radix(str, radix)?))
    }

    pub fn from_str_exact(str: &str) -> Result<Self, RustDecimalError> {
        Ok(Self(RustDecimal::from_str_exact(str)?))
    }

    pub const fn scale(&self) -> u32 {
        self.0.scale()
    }
    pub const fn mantissa(&self) -> i128 {
        self.0.mantissa()
    }

    pub const fn is_zero(&self) -> bool {
        self.0.is_zero()
    }
    pub fn is_integer(&self) -> bool {
        self.0.is_integer()
    }

    pub fn set_sign_positive(&mut self, positive: bool) {
        self.0.set_sign_positive(positive)
    }
    pub fn set_sign_negative(&mut self, negative: bool) {
        self.0.set_sign_negative(negative)
    }
    pub fn set_scale(&mut self, scale: u32) -> Result<(), RustDecimalError> {
        self.0.set_scale(scale)
    }
    pub fn rescale(&mut self, scale: u32) {
        self.0.rescale(scale);
    }

    pub const fn serialize(&self) -> [u8; 16] {
        self.0.serialize()
    }
    pub fn deserialize(bytes: [u8; 16]) -> Decimal {
        Self(RustDecimal::deserialize(bytes))
    }
    pub const fn is_sign_negative(&self) -> bool {
        self.0.is_sign_negative()
    }
    pub const fn is_sign_positive(&self) -> bool {
        self.0.is_sign_positive()
    }

    pub fn trunc(&self) -> Decimal {
        Self(self.0.trunc())
    }

    pub fn trunc_with_scale(&self, scale: u32) -> Decimal {
        Self(self.0.trunc_with_scale(scale))
    }

    pub fn fract(&self) -> Decimal {
        Self(self.0.fract())
    }

    pub fn floor(&self) -> Self {
        Self(self.0.floor())
    }

    pub fn ceil(&self) -> Self {
        Self(self.0.ceil())
    }

    pub fn max(self, other: Decimal) -> Decimal {
        Self(self.0.max(other.0))
    }
    pub fn min(self, other: Decimal) -> Decimal {
        Self(self.0.min(other.0))
    }

    pub fn normalize(&self) -> Decimal {
        Self(self.0.normalize())
    }
    pub fn normalize_assign(&mut self) {
        self.0.normalize_assign()
    }

    pub fn round(&self) -> Decimal {
        Self(self.0.round())
    }

    pub fn round_dp_with_strategy(&self, dp: u32, strategy: RoundingStrategy) -> Decimal {
        Self(self.0.round_dp_with_strategy(dp, strategy))
    }

    pub fn round_dp(&self, dp: u32) -> Decimal {
        Self(self.0.round_dp(dp))
    }

    pub fn round_sf(&self, digits: u32) -> Option<Decimal> {
        self.0.round_sf(digits).map(Self)
    }

    pub fn round_sf_with_strategy(
        &self,
        digits: u32,
        strategy: RoundingStrategy,
    ) -> Option<Decimal> {
        self.0.round_sf_with_strategy(digits, strategy).map(Self)
    }

    pub fn from_f32_retain(n: f32) -> Option<Self> {
        RustDecimal::from_f32_retain(n).map(Self)
    }

    pub fn from_f64_retain(n: f64) -> Option<Self> {
        RustDecimal::from_f64_retain(n).map(Self)
    }

    pub fn checked_add(self, other: Decimal) -> Option<Decimal> {
        self.0.checked_add(other.0).map(Self)
    }
    pub fn checked_sub(self, other: Decimal) -> Option<Decimal> {
        self.0.checked_sub(other.0).map(Self)
    }
    pub fn checked_mul(self, other: Decimal) -> Option<Decimal> {
        self.0.checked_mul(other.0).map(Self)
    }
    pub fn checked_div(self, other: Decimal) -> Option<Decimal> {
        self.0.checked_div(other.0).map(Self)
    }
    pub fn checked_rem(self, other: Decimal) -> Option<Decimal> {
        self.0.checked_rem(other.0).map(Self)
    }

    pub fn saturating_add(self, other: Decimal) -> Decimal {
        Self(self.0.saturating_add(other.0))
    }
    pub fn saturating_sub(self, other: Decimal) -> Decimal {
        Self(self.0.saturating_sub(other.0))
    }
    pub fn saturating_mul(self, other: Decimal) -> Decimal {
        Self(self.0.saturating_mul(other.0))
    }
}

impl Signed for Decimal {
    fn abs(&self) -> Self {
        Self(self.0.abs())
    }

    fn abs_sub(&self, other: &Self) -> Self {
        Self(self.0.abs_sub(&other.0))
    }

    fn signum(&self) -> Self {
        Self(self.0.signum())
    }

    fn is_positive(&self) -> bool {
        self.0.is_sign_positive()
    }
    fn is_negative(&self) -> bool {
        self.0.is_sign_negative()
    }
}

impl Inv for Decimal {
    type Output = Decimal;
    fn inv(self) -> Self {
        Self(self.0.inv())
    }
}

impl Sum for Decimal {
    fn sum<I: Iterator<Item = Decimal>>(iter: I) -> Self {
        Self(iter.map(|d| d.0).sum())
    }
}

impl<'a> Sum<&'a Decimal> for Decimal {
    fn sum<I: Iterator<Item = &'a Decimal>>(iter: I) -> Self {
        Self(iter.map(|d| d.0).sum())
    }
}

impl Product for Decimal {
    fn product<I: Iterator<Item = Decimal>>(iter: I) -> Self {
        Self(iter.map(|d| d.0).product())
    }
}

impl<'a> Product<&'a Decimal> for Decimal {
    fn product<I: Iterator<Item = &'a Decimal>>(iter: I) -> Self {
        Self(iter.map(|d| d.0).product())
    }
}

#[cfg(feature = "decimal_maths")]
impl Decimal {
    pub const PI: Decimal = Self(RustDecimal::PI);
    pub const HALF_PI: Decimal = Self(RustDecimal::HALF_PI);
    pub const QUARTER_PI: Decimal = Self(RustDecimal::QUARTER_PI);
    pub const TWO_PI: Decimal = Self(RustDecimal::TWO_PI);
    pub const E: Decimal = Self(RustDecimal::E);
    pub const E_INVERSE: Decimal = Self(RustDecimal::E_INVERSE);

    pub fn exp(&self) -> Decimal {
        Self(self.0.exp())
    }

    pub fn checked_exp(&self) -> Option<Decimal> {
        self.0.checked_exp().map(Self)
    }

    pub fn exp_with_tolerance(&self, tolerance: Decimal) -> Decimal {
        Self(self.0.exp_with_tolerance(tolerance.0))
    }

    pub fn checked_exp_with_tolerance(&self, tolerance: Decimal) -> Option<Decimal> {
        self.0
            .checked_exp_with_tolerance(tolerance.0)
            .map(Self)
    }

    pub fn powi(&self, exp: i64) -> Decimal {
        Self(self.0.powi(exp))
    }

    pub fn checked_powi(&self, exp: i64) -> Option<Decimal> {
        self.0.checked_powi(exp).map(Self)
    }

    pub fn powu(&self, exp: u64) -> Decimal {
        Self(self.0.powu(exp))
    }

    pub fn checked_powu(&self, exp: u64) -> Option<Decimal> {
        self.0.checked_powu(exp).map(Self)
    }

    pub fn powf(&self, exp: f64) -> Decimal {
        Self(self.0.powf(exp))
    }

    pub fn checked_powf(&self, exp: f64) -> Option<Decimal> {
        self.0.checked_powf(exp).map(Self)
    }

    pub fn powd(&self, exp: Decimal) -> Decimal {
        Self(self.0.powd(exp.0))
    }

    pub fn checked_powd(&self, exp: Decimal) -> Option<Decimal> {
        self.0.checked_powd(exp.0).map(Self)
    }

    pub fn sqrt(&self) -> Option<Decimal> {
        self.0.sqrt().map(Self)
    }

    pub fn ln(&self) -> Decimal {
        Self(self.0.ln())
    }

    pub fn checked_ln(&self) -> Option<Decimal> {
        self.0.checked_ln().map(Self)
    }

    pub fn log10(&self) -> Decimal {
        Self(self.0.log10())
    }

    pub fn checked_log10(&self) -> Option<Decimal> {
        self.0.checked_log10().map(Self)
    }

    pub fn erf(&self) -> Decimal {
        Self(self.0.erf())
    }

    pub fn norm_cdf(&self) -> Decimal {
        Self(self.0.norm_cdf())
    }

    pub fn norm_pdf(&self) -> Decimal {
        Self(self.0.norm_pdf())
    }

    pub fn checked_norm_pdf(&self) -> Option<Decimal> {
        self.0.checked_norm_pdf().map(Self)
    }

    pub fn sin(&self) -> Decimal {
        Self(self.0.sin())
    }

    pub fn checked_sin(&self) -> Option<Decimal> {
        self.0.checked_sin().map(Self)
    }

    pub fn cos(&self) -> Decimal {
        Self(self.0.cos())
    }

    pub fn checked_cos(&self) -> Option<Decimal> {
        self.0.checked_cos().map(Self)
    }

    pub fn tan(&self) -> Decimal {
        Self(self.0.tan())
    }

    pub fn checked_tan(&self) -> Option<Decimal> {
        self.0.checked_tan().map(Self)
    }
}

impl Display for Decimal {
    fn fmt(&self, f: &mut Formatter) -> Result<(), std::fmt::Error> {
        Display::fmt(&self.0, f)
    }
}

impl LowerExp for Decimal {
    fn fmt(&self, f: &mut Formatter) -> Result<(), std::fmt::Error> {
        LowerExp::fmt(&self.0, f)
    }
}

impl UpperExp for Decimal {
    fn fmt(&self, f: &mut Formatter) -> Result<(), std::fmt::Error> {
        UpperExp::fmt(&self.0, f)
    }
}

impl FromPrimitive for Decimal {
    fn from_i64(n: i64) -> Option<Decimal> {
        RustDecimal::from_i64(n).map(Self)
    }

    fn from_u64(n: u64) -> Option<Decimal> {
        RustDecimal::from_u64(n).map(Self)
    }

    fn from_u128(n: u128) -> Option<Decimal> {
        RustDecimal::from_u128(n).map(Self)
    }

    fn from_i128(n: i128) -> Option<Decimal> {
        RustDecimal::from_i128(n).map(Self)
    }

    fn from_f64(n: f64) -> Option<Decimal> {
        RustDecimal::from_f64(n).map(Self)
    }
}

impl ToPrimitive for Decimal {
    fn to_i64(&self) -> Option<i64> {
        self.0.to_i64()
    }
    fn to_u64(&self) -> Option<u64> {
        self.0.to_u64()
    }
    fn to_i128(&self) -> Option<i128> {
        self.0.to_i128()
    }
    fn to_u128(&self) -> Option<u128> {
        self.0.to_u128()
    }
    fn to_f64(&self) -> Option<f64> {
        self.0.to_f64()
    }
}

impl FromStr for Decimal {
    type Err = <RustDecimal as FromStr>::Err;

    fn from_str(value: &str) -> Result<Decimal, <RustDecimal as FromStr>::Err> {
        Ok(Decimal(RustDecimal::from_str(value)?))
    }
}

impl Add for Decimal {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self(self.0.add(rhs.0))
    }
}

impl<'a, 'b> Add<&'b Decimal> for &'a Decimal {
    type Output = Decimal;

    fn add(self, rhs: &Decimal) -> Decimal {
        Decimal(self.0.add(rhs.0))
    }
}

impl<'a> AddAssign<&'a Decimal> for &'a mut Decimal {
    fn add_assign(&mut self, other: &'a Decimal) {
        self.0.add_assign(other.0)
    }
}

impl<'a> AddAssign<&'a Decimal> for Decimal {
    fn add_assign(&mut self, other: &'a Decimal) {
        self.0.add_assign(other.0)
    }
}

impl<'a> AddAssign<Decimal> for &'a mut Decimal {
    fn add_assign(&mut self, other: Decimal) {
        self.0.add_assign(other.0)
    }
}

impl AddAssign<Decimal> for Decimal {
    fn add_assign(&mut self, other: Decimal) {
        self.0.add_assign(other.0)
    }
}

impl Sub for Decimal {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        Self(self.0.sub(rhs.0))
    }
}

impl<'a, 'b> Sub<&'b Decimal> for &'a Decimal {
    type Output = Decimal;

    fn sub(self, rhs: &Decimal) -> Decimal {
        Decimal(self.0.sub(rhs.0))
    }
}

impl<'a> SubAssign<&'a Decimal> for &'a mut Decimal {
    fn sub_assign(&mut self, other: &'a Decimal) {
        self.0.sub_assign(other.0)
    }
}

impl<'a> SubAssign<&'a Decimal> for Decimal {
    fn sub_assign(&mut self, other: &'a Decimal) {
        self.0.sub_assign(other.0)
    }
}

impl<'a> SubAssign<Decimal> for &'a mut Decimal {
    fn sub_assign(&mut self, other: Decimal) {
        self.0.sub_assign(other.0)
    }
}

impl SubAssign<Decimal> for Decimal {
    fn sub_assign(&mut self, other: Decimal) {
        self.0.sub_assign(other.0)
    }
}

impl Mul for Decimal {
    type Output = Self;

    fn mul(self, rhs: Self) -> Self::Output {
        Self(self.0.mul(rhs.0))
    }
}

impl<'a, 'b> Mul<&'b Decimal> for &'a Decimal {
    type Output = Decimal;

    fn mul(self, rhs: &Decimal) -> Decimal {
        Decimal(self.0.mul(rhs.0))
    }
}

impl<'a> MulAssign<&'a Decimal> for &'a mut Decimal {
    fn mul_assign(&mut self, other: &'a Decimal) {
        self.0.mul_assign(other.0)
    }
}

impl<'a> MulAssign<&'a Decimal> for Decimal {
    fn mul_assign(&mut self, other: &'a Decimal) {
        self.0.mul_assign(other.0)
    }
}

impl<'a> MulAssign<Decimal> for &'a mut Decimal {
    fn mul_assign(&mut self, other: Decimal) {
        self.0.mul_assign(other.0)
    }
}

impl MulAssign<Decimal> for Decimal {
    fn mul_assign(&mut self, other: Decimal) {
        self.0.mul_assign(other.0)
    }
}

impl Div for Decimal {
    type Output = Self;

    fn div(self, rhs: Self) -> Self::Output {
        Self(self.0.div(rhs.0))
    }
}

impl<'a, 'b> Div<&'b Decimal> for &'a Decimal {
    type Output = Decimal;

    fn div(self, rhs: &Decimal) -> Decimal {
        Decimal(self.0.div(rhs.0))
    }
}

impl<'a> DivAssign<&'a Decimal> for &'a mut Decimal {
    fn div_assign(&mut self, other: &'a Decimal) {
        self.0.div_assign(other.0)
    }
}

impl<'a> DivAssign<&'a Decimal> for Decimal {
    fn div_assign(&mut self, other: &'a Decimal) {
        self.0.div_assign(other.0)
    }
}

impl<'a> DivAssign<Decimal> for &'a mut Decimal {
    fn div_assign(&mut self, other: Decimal) {
        self.0.div_assign(other.0)
    }
}

impl DivAssign<Decimal> for Decimal {
    fn div_assign(&mut self, other: Decimal) {
        self.0.div_assign(other.0)
    }
}

impl Rem for Decimal {
    type Output = Self;

    fn rem(self, rhs: Self) -> Self::Output {
        Self(self.0.rem(rhs.0))
    }
}

impl<'a, 'b> Rem<&'b Decimal> for &'a Decimal {
    type Output = Decimal;

    fn rem(self, rhs: &Decimal) -> Decimal {
        Decimal(self.0.rem(rhs.0))
    }
}

impl<'a> RemAssign<&'a Decimal> for &'a mut Decimal {
    fn rem_assign(&mut self, other: &'a Decimal) {
        self.0.rem_assign(other.0)
    }
}

impl<'a> RemAssign<&'a Decimal> for Decimal {
    fn rem_assign(&mut self, other: &'a Decimal) {
        self.0.rem_assign(other.0)
    }
}

impl<'a> RemAssign<Decimal> for &'a mut Decimal {
    fn rem_assign(&mut self, other: Decimal) {
        self.0.rem_assign(other.0)
    }
}

impl RemAssign<Decimal> for Decimal {
    fn rem_assign(&mut self, other: Decimal) {
        self.0.rem_assign(other.0)
    }
}

impl Neg for Decimal {
    type Output = Self;

    fn neg(self) -> Self::Output {
        Self(self.0.neg())
    }
}

impl Neg for &Decimal {
    type Output = Decimal;

    fn neg(self) -> Self::Output {
        Decimal(self.0.neg())
    }
}

impl Zero for Decimal {
    fn zero() -> Self {
        Decimal(RustDecimal::ZERO)
    }

    fn is_zero(&self) -> bool {
        self.0.is_zero()
    }
}

impl One for Decimal {
    fn one() -> Self {
        Decimal(RustDecimal::ONE)
    }
}

impl Num for Decimal {
    type FromStrRadixErr = RustDecimalError;
    fn from_str_radix(str: &str, radix: u32) -> Result<Self, Self::FromStrRadixErr> {
        Decimal::from_str_radix(str, radix)
    }
}

#[cfg(feature = "decimal_maths")]
impl Pow<Decimal> for Decimal {
    type Output = Decimal;

    fn pow(self, rhs: Decimal) -> Self::Output {
        Self::powd(&self, rhs)
    }
}
