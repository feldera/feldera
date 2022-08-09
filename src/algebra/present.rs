use crate::algebra::{HasOne, HasZero, MulByRef};
use deepsize::DeepSizeOf;
use std::ops::{Add, AddAssign, Mul, MulAssign, Neg, Sub, SubAssign};

/// A zero-sized weight that indicates a value is present
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
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

impl DeepSizeOf for Present {
    fn deep_size_of_children(&self, _context: &mut deepsize::Context) -> usize {
        0
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

impl MulAssign for Present {
    fn mul_assign(&mut self, _rhs: Self) {}
}

impl MulAssign<&'_ Present> for Present {
    fn mul_assign(&mut self, _rhs: &Self) {}
}

impl<T> MulByRef<Present> for T
where
    T: Clone,
{
    type Output = T;

    #[inline]
    fn mul_by_ref(&self, _other: &Present) -> Self::Output {
        self.clone()
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
