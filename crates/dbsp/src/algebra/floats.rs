//! Wrappers around floating point values.  Rust floating point values
//! do not implement Ord, which is needed by any data that must be
//! stored in a database.

use crate::algebra::{HasOne, HasZero};
use feldera_types::{deserialize_without_context, serialize_without_context};
use ordered_float::OrderedFloat;
use paste::paste;
use size_of::SizeOf;
use std::{
    fmt::{self, Debug, Display},
    iter::{Product, Sum},
    num::ParseFloatError,
    ops::{Add, AddAssign, Div, DivAssign, Mul, MulAssign, Neg, Sub, SubAssign},
    str::FromStr,
};

use serde::{Deserialize, Serialize};

macro_rules! float {
    ($($outer:ident($inner:ident)),* $(,)?) => {
        $(
            paste!{
            #[doc = concat!("A wrapper around [`", stringify!($inner), "`] that provides additional useful traits, such as [Ord].")]
            ///
            /// The wrapper holds one zero and one NaN: it stores -0.0 as +0.0 and
            /// every NaN as the same NaN.  `Eq` treats -0.0 and +0.0 as equal, and
            /// all NaNs as equal; storing one bit pattern for each such value
            /// ensures that every function gives the same result for equal values.
            #[derive(Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, SizeOf, rkyv::Archive, rkyv::Serialize, feldera_macros::IsNone, feldera_macros::OrdRepr)]
            #[repr(transparent)]
            #[size_of(skip_all)]
            #[derive(Serialize)]
            #[serde(transparent)]
            #[archive_attr(derive(Clone, Ord, Eq, PartialEq, PartialOrd))]
            #[archive(compare(PartialEq, PartialOrd))]
            pub struct $outer(OrderedFloat<$inner>);

            impl<D: rkyv::Fallible + ?Sized> rkyv::Deserialize<$outer, D> for [<Archived $outer>] {
                #[inline]
                fn deserialize(&self, deserializer: &mut D) -> Result<$outer, D::Error> {
                    let float: OrderedFloat<$inner> =
                        rkyv::Deserialize::deserialize(&self.0, deserializer)?;
                    Ok($outer::new(float.0))
                }
            }
            }

            impl<'de> Deserialize<'de> for $outer {
                #[inline]
                fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
                where
                    D: serde::Deserializer<'de>,
                {
                    $inner::deserialize(deserializer).map(Self::new)
                }
            }

            impl $outer {
                pub const EPSILON: Self = Self::new($inner::EPSILON);

                /// Wraps `float`, replacing -0.0 by +0.0 and any NaN by `NAN`.
                #[inline]
                pub const fn new(float: $inner) -> Self {
                    let canonical = if float.is_nan() {
                        $inner::NAN
                    } else if float == 0.0 {
                        0.0
                    } else {
                        float
                    };
                    Self(OrderedFloat(canonical))
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
                    Self::new(self.into_inner() + rhs.into_inner())
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
                    Self::new(self.into_inner() - rhs.into_inner())
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
                    Self::new(self.into_inner() * rhs.into_inner())
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
                    Self::new(self.into_inner() / rhs.into_inner())
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
                    Self::new(-self.into_inner())
                }
            }

            impl Neg for &$outer {
                type Output = $outer;

                #[inline]
                fn neg(self) -> Self::Output {
                    $outer::new(-self.into_inner())
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

serialize_without_context!(F32);
serialize_without_context!(F64);

/// A float wrapper archives to a struct holding the very same
/// `OrderedFloat`, so hashing the archived form writes what hashing the
/// decoded one writes, NaN and the two zeroes included.
macro_rules! impl_float_hash_repr {
    ($($outer:ident => $archived:ident),* $(,)?) => {$(
        impl $crate::dynamic::HashRepr for $outer {
            const FAITHFUL: bool = true;

            #[inline]
            fn hash_repr<H: ::std::hash::Hasher>(&self, state: &mut H) {
                ::std::hash::Hash::hash(self, state)
            }
        }

        impl $crate::dynamic::HashRepr for $archived {
            const FAITHFUL: bool = true;

            #[inline]
            fn hash_repr<H: ::std::hash::Hasher>(&self, state: &mut H) {
                ::std::hash::Hash::hash(&self.0, state)
            }
        }
    )*};
}

impl_float_hash_repr!(F32 => ArchivedF32, F64 => ArchivedF64);
deserialize_without_context!(F32);
deserialize_without_context!(F64);

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
    fn one_zero_and_one_nan() {
        use ordered_float::OrderedFloat;

        let zero = 0.0f64.to_bits();
        let nan = f64::NAN.to_bits();
        let negative_nan = f64::from_bits(nan | (1 << 63));
        // An archive written by a version that stored -0.0; the field bypasses F64::new
        let archive = rkyv::to_bytes::<_, 256>(&F64(OrderedFloat(-0.0))).unwrap();
        // SAFETY: `archive` holds the archive of an `F64` that `rkyv::to_bytes` just wrote.
        let archived = unsafe { rkyv::archived_root::<F64>(&archive[..]) };
        let decoded: F64 = archived.deserialize(&mut rkyv::Infallible).unwrap();

        let zeros = [
            F64::new(-0.0),
            -F64::new(0.0),
            -&F64::new(0.0),
            F64::new(0.0) * F64::new(-1.0),
            F64::new(0.0) / F64::new(-1.0),
            F64::new(-1e-300) * F64::new(1e-300),
            F64::from_str("-0").unwrap(),
            serde_json::from_str::<F64>("-0.0").unwrap(),
            decoded,
        ];
        for value in zeros {
            assert_eq!(value.into_inner().to_bits(), zero, "{value:?}");
        }

        let nans = [
            F64::new(negative_nan),
            F64::new(0.0) / F64::new(0.0),
            F64::new(f64::INFINITY) - F64::new(f64::INFINITY),
        ];
        for value in nans {
            assert_eq!(value.into_inner().to_bits(), nan, "{value:?}");
        }

        assert_eq!((-F32::new(0.0)).into_inner().to_bits(), 0.0f32.to_bits());
        let negative_nan32 = f32::from_bits(f32::NAN.to_bits() | (1 << 31));
        assert_eq!(
            F32::new(negative_nan32).into_inner().to_bits(),
            f32::NAN.to_bits()
        );
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
