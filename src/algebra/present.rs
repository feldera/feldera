use crate::algebra::HasOne;
use std::ops::{Add, AddAssign, Mul, MulAssign, Sub, SubAssign};

/// A zero-sized weight that indicates a value is present
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Present;

impl HasOne for Present {
    fn one() -> Self {
        Self
    }
}

impl Add for Present {
    type Output = Self;

    fn add(self, _rhs: Self) -> Self::Output {
        Self
    }
}

impl Add<&Present> for Present {
    type Output = Self;

    fn add(self, _rhs: &Self) -> Self::Output {
        Self
    }
}

impl<'a> Add<&'a Present> for &'a Present {
    type Output = Present;

    fn add(self, _rhs: Self) -> Self::Output {
        Present
    }
}

impl AddAssign for Present {
    fn add_assign(&mut self, _rhs: Self) {}
}

impl AddAssign<&'_ Present> for Present {
    fn add_assign(&mut self, _rhs: &Self) {}
}

impl Sub for Present {
    type Output = Self;

    fn sub(self, _rhs: Self) -> Self::Output {
        Self
    }
}

impl Sub<&Present> for Present {
    type Output = Self;

    fn sub(self, _rhs: &Self) -> Self::Output {
        Self
    }
}

impl Sub<Present> for &Present {
    type Output = Present;

    fn sub(self, _rhs: Present) -> Self::Output {
        Present
    }
}

impl<'a> Sub<&'a Present> for &'a Present {
    type Output = Present;

    fn sub(self, _rhs: Self) -> Self::Output {
        Present
    }
}

impl SubAssign for Present {
    fn sub_assign(&mut self, _rhs: Self) {}
}

impl SubAssign<&'_ Present> for Present {
    fn sub_assign(&mut self, _rhs: &Self) {}
}

impl Mul for Present {
    type Output = Self;

    fn mul(self, _rhs: Self) -> Self::Output {
        Self
    }
}

impl Mul<&Present> for Present {
    type Output = Self;

    fn mul(self, _rhs: &Self) -> Self::Output {
        Self
    }
}

impl<'a> Mul<&'a Present> for &'a Present {
    type Output = Present;

    fn mul(self, _rhs: Self) -> Self::Output {
        Present
    }
}

impl Mul<Present> for &Present {
    type Output = Present;

    fn mul(self, _rhs: Present) -> Self::Output {
        Present
    }
}

impl MulAssign for Present {
    fn mul_assign(&mut self, _rhs: Self) {}
}

impl MulAssign<&'_ Present> for Present {
    fn mul_assign(&mut self, _rhs: &Self) {}
}
