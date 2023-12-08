use crate::algebra::{HasOne, HasZero};
use ordered_float::OrderedFloat;
use size_of::SizeOf;
use std::{
    fmt::{self, Debug, Display},
    iter::{Product, Sum},
    num::ParseFloatError,
    ops::{Add, AddAssign, Div, DivAssign, Mul, MulAssign, Neg, Sub, SubAssign},
    str::FromStr,
};

#[cfg(feature = "with-serde")]
use serde::{Deserialize, Serialize};

macro_rules! float {
    ($($outer:ident($inner:ident)),* $(,)?) => {
        $(
            #[doc = concat!("A wrapper around [`", stringify!($inner), "`] that allows using it in a DBSP stream.")]
            #[derive(Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, SizeOf)]
            #[repr(transparent)]
            #[size_of(skip_all)]
            #[cfg_attr(feature = "with-serde", derive(Serialize, Deserialize))]
            #[cfg_attr(feature = "with-serde", serde(transparent))]
            #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
            pub struct $outer(OrderedFloat<$inner>);

            impl $outer {
                pub const EPSILON: Self = Self::new($inner::EPSILON);

                #[inline]
                pub const fn new(float: $inner) -> Self {
                    Self(OrderedFloat(float))
                }

                #[inline]
                #[rustfmt::skip]
                pub const fn into_inner(self) -> $inner {
                    self.0.0
                }

                #[inline]
                pub fn abs(self) -> Self {
                    Self::new(self.0.abs())
                }
            }

            impl<T> From<T> for $outer
            where
                $inner: From<T>,
            {
                #[inline]
                fn from(x: T) -> Self {
                    Self::new($inner::from(x))
                }
            }

            impl PartialEq<$inner> for $outer {
                #[inline]
                fn eq(&self, other: &$inner) -> bool {
                    *self == $outer::new(*other)
                }
            }

            impl PartialEq<$outer> for $inner {
                #[inline]
                fn eq(&self, other: &$outer) -> bool {
                    $outer::new(*self) == *other
                }
            }

            impl Add for $outer {
                type Output = Self;

                #[inline]
                fn add(self, rhs: Self) -> Self::Output {
                    Self(self.0 + rhs.0)
                }
            }

            impl Add<&$outer> for $outer {
                type Output = Self;

                #[inline]
                fn add(self, rhs: &Self) -> Self::Output {
                    self + *rhs
                }
            }

            impl<'a> Add<&'a $outer> for &'a $outer {
                type Output = $outer;

                #[inline]
                fn add(self, rhs: Self) -> Self::Output {
                    *self + *rhs
                }
            }

            impl AddAssign for $outer {
                #[inline]
                fn add_assign(&mut self, rhs: Self) {
                    *self = *self + rhs;
                }
            }

            impl AddAssign<&'_ $outer> for $outer {
                #[inline]
                fn add_assign(&mut self, rhs: &Self) {
                    *self = *self + *rhs;
                }
            }

            impl Sub for $outer {
                type Output = Self;

                #[inline]
                fn sub(self, rhs: Self) -> Self::Output {
                    Self(self.0 - rhs.0)
                }
            }

            impl Sub<$inner> for $outer {
                type Output = Self;

                #[inline]
                fn sub(self, rhs: $inner) -> Self::Output {
                    self - Self::new(rhs)
                }
            }

            impl Sub<&$outer> for $outer {
                type Output = Self;

                #[inline]
                fn sub(self, rhs: &Self) -> Self::Output {
                    self - *rhs
                }
            }

            impl<'a> Sub<&'a $outer> for &'a $outer {
                type Output = $outer;

                #[inline]
                fn sub(self, rhs: Self) -> Self::Output {
                    *self - *rhs
                }
            }

            impl SubAssign for $outer {
                #[inline]
                fn sub_assign(&mut self, rhs: Self) {
                    *self = *self - rhs;
                }
            }

            impl SubAssign<&'_ $outer> for $outer {
                #[inline]
                fn sub_assign(&mut self, rhs: &Self) {
                    *self = *self - *rhs;
                }
            }

            impl Mul for $outer {
                type Output = Self;

                #[inline]
                fn mul(self, rhs: Self) -> Self::Output {
                    Self(self.0 * rhs.0)
                }
            }

            impl Mul<&$outer> for $outer {
                type Output = Self;

                #[inline]
                fn mul(self, rhs: &Self) -> Self::Output {
                    self * *rhs
                }
            }

            impl<'a> Mul<&'a $outer> for &'a $outer {
                type Output = $outer;

                #[inline]
                fn mul(self, rhs: Self) -> Self::Output {
                    *self * *rhs
                }
            }

            impl Mul<$inner> for $outer {
                type Output = Self;

                #[inline]
                fn mul(self, rhs: $inner) -> Self::Output {
                    self * Self::new(rhs)
                }
            }

            impl Mul<$outer> for $inner {
                type Output = $outer;

                #[inline]
                fn mul(self, rhs: $outer) -> Self::Output {
                    $outer::new(self) * rhs
                }
            }

            impl Mul<&$outer> for $inner {
                type Output = $outer;

                #[inline]
                fn mul(self, rhs: &$outer) -> Self::Output {
                    $outer::new(self) * *rhs
                }
            }

            impl MulAssign for $outer {
                #[inline]
                fn mul_assign(&mut self, rhs: Self) {
                    *self = *self * rhs;
                }
            }

            impl MulAssign<&'_ $outer> for $outer {
                #[inline]
                fn mul_assign(&mut self, rhs: &Self) {
                    *self = *self * *rhs;
                }
            }

            impl Div for $outer {
                type Output = Self;

                #[inline]
                fn div(self, rhs: Self) -> Self::Output {
                    Self(self.0 / rhs.0)
                }
            }

            impl Div<&$outer> for $outer {
                type Output = Self;

                #[inline]
                fn div(self, rhs: &Self) -> Self::Output {
                    self / *rhs
                }
            }

            impl Div<$inner> for $outer {
                type Output = Self;

                #[inline]
                fn div(self, rhs: $inner) -> Self::Output {
                    self / Self::new(rhs)
                }
            }

            impl Div<$outer> for $inner {
                type Output = $outer;

                #[inline]
                fn div(self, rhs: $outer) -> Self::Output {
                    $outer::new(self) / rhs
                }
            }

            impl Div<&$outer> for $inner {
                type Output = $outer;

                #[inline]
                fn div(self, rhs: &$outer) -> Self::Output {
                    $outer::new(self) / *rhs
                }
            }

            impl DivAssign for $outer {
                #[inline]
                fn div_assign(&mut self, rhs: Self) {
                    *self = *self / rhs;
                }
            }

            impl DivAssign<&'_ $outer> for $outer {
                #[inline]
                fn div_assign(&mut self, rhs: &Self) {
                    *self = *self / *rhs;
                }
            }

            impl Neg for $outer {
                type Output = Self;

                #[inline]
                fn neg(self) -> Self::Output {
                    Self(-self.0)
                }
            }

            impl Neg for &$outer {
                type Output = $outer;

                #[inline]
                fn neg(self) -> Self::Output {
                    $outer(-self.0)
                }
            }

            impl HasZero for $outer {
                #[inline]
                fn zero() -> Self {
                    Self::new(0.0)
                }

                #[inline]
                fn is_zero(&self) -> bool {
                    *self == 0.0
                }
            }

            impl HasOne for $outer {
                #[inline]
                fn one() -> Self {
                    Self::new(1.0)
                }
            }

            impl Sum for $outer {
                #[inline]
                fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
                    iter.fold(Self::zero(), |a, b| a + b)
                }
            }

            impl<'a> Sum<&'a $outer> for $outer {
                #[inline]
                fn sum<I: Iterator<Item = &'a Self>>(iter: I) -> Self {
                    iter.fold(Self::zero(), |a, b| a + b)
                }
            }

            impl Product for $outer {
                #[inline]
                fn product<I: Iterator<Item = Self>>(iter: I) -> Self {
                    iter.fold(Self::one(), |a, b| a * b)
                }
            }

            impl<'a> Product<&'a $outer> for $outer {
                #[inline]
                fn product<I: Iterator<Item = &'a Self>>(iter: I) -> Self {
                    iter.fold(Self::one(), |a, b| a * b)
                }
            }

            impl Debug for $outer {
                #[rustfmt::skip]
                fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    Debug::fmt(&self.0.0, f)
                }
            }

            impl Display for $outer {
                #[rustfmt::skip]
                fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    Display::fmt(&self.0.0, f)
                }
            }

            impl FromStr for $outer {
                type Err = ParseFloatError;

                #[inline]
                fn from_str(src: &str) -> Result<$outer, ParseFloatError> {
                    $inner::from_str(src).map($outer::new)
                }
            }
        )*
    };
}

// TODO: We could support halfs here too
float! {
    F32(f32),
    F64(f64),
}

impl From<F32> for F64 {
    #[inline]
    fn from(float: F32) -> Self {
        Self::new(float.into_inner() as f64)
    }
}

impl From<F64> for F32 {
    #[inline]
    fn from(float: F64) -> Self {
        Self::new(float.into_inner() as f32)
    }
}

#[cfg(test)]
mod tests {
    use rkyv::Deserialize;

    use super::{F32, F64};
    use std::str::FromStr;

    #[test]
    fn fromstr() {
        assert_eq!(Ok(F32::new(10.0)), F32::from_str("10"));
        assert_eq!(Ok(F64::new(-10.0)), F64::from_str("-10"));
        assert!(F32::from_str("what").is_err());
    }

    #[test]
    fn f64_decode_encode() {
        for input in [
            F64::new(-1.0),
            F64::new(0.0),
            F64::new(1.0),
            F64::new(f64::MAX),
            F64::new(f64::MIN),
            F64::new(f64::NAN),
            F64::new(f64::INFINITY),
        ]
        .into_iter()
        {
            let encoded = rkyv::to_bytes::<_, 256>(&input).unwrap();
            let archived = unsafe { rkyv::archived_root::<F64>(&encoded[..]) };
            let decoded: F64 = archived.deserialize(&mut rkyv::Infallible).unwrap();
            assert_eq!(decoded, input);
        }
    }

    #[test]
    fn f32_decode_encode() {
        for input in [
            F32::new(-1.0),
            F32::new(0.0),
            F32::new(1.0),
            F32::new(f32::MAX),
            F32::new(f32::MIN),
            F32::new(f32::NAN),
            F32::new(f32::INFINITY),
        ]
        .into_iter()
        {
            let encoded = rkyv::to_bytes::<_, 256>(&input).unwrap();
            let archived = unsafe { rkyv::archived_root::<F32>(&encoded[..]) };
            let decoded: F32 = archived.deserialize(&mut rkyv::Infallible).unwrap();
            assert_eq!(decoded, input);
        }
    }
}
