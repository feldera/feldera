use crate::algebra::{HasOne, HasZero};
use deepsize::DeepSizeOf;
use ordered_float::OrderedFloat;
use std::{
    fmt::{self, Debug, Display},
    iter::{Product, Sum},
    ops::{Add, AddAssign, Div, DivAssign, Mul, MulAssign, Neg, Sub, SubAssign},
};

macro_rules! float {
    ($($outer:ident($inner:ident)),* $(,)?) => {
        $(
            #[doc = concat!("A wrapper around [`", stringify!($inner), "`] that allows using it as a DBSP weight")]
            #[derive(Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
            #[repr(transparent)]
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

            impl From<$inner> for $outer {
                #[inline]
                fn from(float: $inner) -> Self {
                    Self::new(float)
                }
            }

            impl From<&$inner> for $outer {
                #[inline]
                fn from(float: &$inner) -> Self {
                    Self::new(*float)
                }
            }

            impl From<$outer> for $inner {
                #[inline]
                fn from(float: $outer) -> Self {
                    float.into_inner()
                }
            }

            impl From<&$outer> for $inner {
                #[inline]
                fn from(float: &$outer) -> Self {
                    float.into_inner()
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

            impl DeepSizeOf for $outer {
                #[inline]
                fn deep_size_of_children(&self, _context: &mut deepsize::Context) -> usize {
                    0
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
        )*
    };
}

// TODO: We could support halfs here too
float! {
    F32(f32),
    F64(f64),
}
