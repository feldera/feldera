use crate::algebra::{HasOne, HasZero, MulByRef};
use rkyv::{Archive, Deserialize, Serialize};
use size_of::SizeOf;
use std::ops::{Add, AddAssign, Mul, MulAssign, Neg, Sub, SubAssign};

use super::{F32, F64};

/// A zero-sized weight that indicates a value is present
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    SizeOf,
    Default,
    Archive,
    Serialize,
    Deserialize,
)]
#[archive_attr(derive(Clone, Ord, Eq, PartialEq, PartialOrd))]
#[archive(compare(PartialEq, PartialOrd))]
pub struct Present;

impl HasZero for Present {
    fn zero() -> Self {
        // FIXME: This isn't correct
        Self
    }

    fn is_zero(&self) -> bool {
        false
    }
}

impl HasOne for Present {
    fn one() -> Self {
        Self
    }
}

impl<T> Add<T> for Present {
    type Output = T;

    #[inline]
    fn add(self, rhs: T) -> Self::Output {
        rhs
    }
}

impl Add<&Present> for &Present {
    type Output = Present;

    fn add(self, _rhs: &Present) -> Self::Output {
        Present
    }
}

impl AddAssign for Present {
    fn add_assign(&mut self, _rhs: Self) {}
}

impl AddAssign<&'_ Present> for Present {
    fn add_assign(&mut self, _rhs: &Self) {}
}

impl<T> Sub<T> for Present {
    type Output = T;

    #[inline]
    fn sub(self, rhs: T) -> Self::Output {
        rhs
    }
}

impl SubAssign for Present {
    fn sub_assign(&mut self, _rhs: Self) {}
}

impl SubAssign<&'_ Present> for Present {
    fn sub_assign(&mut self, _rhs: &Self) {}
}

impl<T> Mul<T> for Present {
    type Output = T;

    #[inline]
    fn mul(self, rhs: T) -> Self::Output {
        rhs
    }
}

impl<T: Copy> Mul<&T> for &Present {
    type Output = T;

    #[inline]
    fn mul(self, rhs: &T) -> Self::Output {
        *rhs
    }
}

impl MulAssign for Present {
    fn mul_assign(&mut self, _rhs: Self) {}
}

impl MulAssign<&'_ Present> for Present {
    fn mul_assign(&mut self, _rhs: &Self) {}
}

impl MulByRef<Present> for i32 {
    type Output = Self;

    #[inline]
    fn mul_by_ref(&self, _other: &Present) -> Self::Output {
        *self
    }
}

impl MulByRef<Present> for i64 {
    type Output = Self;

    #[inline]
    fn mul_by_ref(&self, _other: &Present) -> Self::Output {
        *self
    }
}

impl MulByRef<Present> for F32 {
    type Output = Self;

    #[inline]
    fn mul_by_ref(&self, _other: &Present) -> Self::Output {
        *self
    }
}

impl MulByRef<Present> for F64 {
    type Output = Self;

    #[inline]
    fn mul_by_ref(&self, _other: &Present) -> Self::Output {
        *self
    }
}

// FIXME: This doesn't really make sense and is wrong
impl Neg for Present {
    type Output = Self;

    #[inline]
    fn neg(self) -> Self::Output {
        Present
    }
}

// FIXME: This doesn't really make sense and is wrong
impl Neg for &Present {
    type Output = Present;

    #[inline]
    fn neg(self) -> Self::Output {
        Present
    }
}
