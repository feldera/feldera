/*
MIT License
SPDX-License-Identifier: MIT

Copyright (c) 2021 VMware, Inc
*/

//! This module contains declarations of abstract algebraic concepts:
//! monoids, groups, rings, etc.

pub mod zset;

use num::{CheckedAdd, CheckedMul, CheckedSub, One, Zero};
use std::fmt::{Display, Error, Formatter};
use std::ops::{Add, AddAssign, Mul, Neg};

/// A type with an associative addition and a zero.
/// We trust the implementation to have an associative addition.
/// This contains methods 'is_zero', 'zero' and 'add'
pub trait MonoidValue: Clone + Eq + 'static + Add<Output = Self> + Zero + AddAssign {}

/// Default implementation for all types that have an addition.
impl<T> MonoidValue for T where T: Clone + Eq + 'static + Add<Output = Self> + Zero + AddAssign {}

/// A `MonoidValue` with negation.
/// In addition we expect all our groups to be commutative.
/// This adds the 'neg' method to the MonoidValue methods.
pub trait GroupValue: MonoidValue + Neg<Output = Self> {}

/// Default implementation for all types that have the required traits.
impl<T> GroupValue for T where
    T: Clone + Eq + 'static + Add<Output = Self> + Zero + AddAssign + Neg<Output = Self>
{
}

/// A Group with a multiplication operation
/// This adds the 'mul' method
pub trait RingValue: GroupValue + Mul<Output = Self> + One {}

/// Default implementation for all types that have the required traits.
impl<T> RingValue for T where
    T: Clone
        + Eq
        + 'static
        + Add<Output = Self>
        + Zero
        + AddAssign
        + Neg<Output = Self>
        + Mul<Output = Self>
        + One
{
}

/// A ring where elements can be compared with zero
pub trait ZRingValue: RingValue {
    /// True if value is greater or equal to zero
    fn ge0(&self) -> bool;
}

/// Default implementation for all types that have the required traits.
impl<T> ZRingValue for T
where
    T: Clone
        + Eq
        + 'static
        + Add<Output = Self>
        + Zero
        + AddAssign
        + Neg<Output = Self>
        + Mul<Output = Self>
        + One
        + Ord,
{
    fn ge0(&self) -> bool {
        *self >= Self::zero()
    }
}

/////////////////////////////////////////////////////
/// Ring on i64 values

#[cfg(test)]
mod integer_ring_tests {
    use super::*;

    #[test]
    fn fixed_integer_tests() {
        assert_eq!(0, i64::zero());
        assert_eq!(1, i64::one());
        let two = i64::one().add(i64::one());
        assert_eq!(2, two);
        assert_eq!(-2, two.neg());
        assert_eq!(-4, two.mul(two.neg()));
    }
}

////////////////////////////////////////////////////////
/// Ring on numeric values that panics on overflow
/// Computes exactly like any signed numeric value, but panics on overflow
#[derive(PartialEq, Debug, Eq, Clone)]
pub struct CheckedInt<T> {
    value: T,
}

impl<T> CheckedInt<T> {
    fn new(value: T) -> Self {
        Self { value }
    }
}

impl<T> Add for CheckedInt<T>
where
    T: CheckedAdd,
{
    type Output = Self;

    fn add(self, other: Self) -> Self {
        // intentional panic on overflow
        Self {
            value: self.value.checked_add(&other.value).expect("overflow"),
        }
    }
}

impl<T> AddAssign for CheckedInt<T>
where
    T: CheckedAdd,
{
    fn add_assign(&mut self, other: Self) {
        self.value = self.value.checked_add(&other.value).expect("overflow")
    }
}

impl<T> Mul for CheckedInt<T>
where
    T: CheckedMul,
{
    type Output = Self;
    fn mul(self, rhs: Self) -> Self::Output {
        // intentional panic on overflow
        Self {
            value: self.value.checked_mul(&rhs.value).expect("overflow"),
        }
    }
}

impl<T> Neg for CheckedInt<T>
where
    T: CheckedSub + Zero,
{
    type Output = Self;

    fn neg(self) -> Self::Output {
        Self {
            // intentional panic on overflow
            value: T::zero().checked_sub(&self.value).expect("overflow"),
        }
    }
}

impl<T> Zero for CheckedInt<T>
where
    T: Zero + CheckedAdd,
{
    fn zero() -> Self {
        CheckedInt::new(T::zero())
    }
    fn is_zero(&self) -> bool {
        T::is_zero(&self.value)
    }
}

impl<T> One for CheckedInt<T>
where
    T: One + CheckedMul,
{
    fn one() -> Self {
        CheckedInt::new(T::one())
    }
}

impl<T> ZRingValue for CheckedInt<T>
where
    T: Ord + Zero + One + CheckedAdd + CheckedMul + CheckedSub + Clone + 'static,
{
    fn ge0(&self) -> bool {
        self.value >= T::zero()
    }
}

// Note: this should be generic in T, but the Rust compiler does not like it
// complaining that it conflicts with some implementation in core.
impl<T> From<CheckedInt<T>> for i64
where
    T: Into<i64>,
{
    fn from(value: CheckedInt<T>) -> Self {
        value.value.into()
    }
}

impl<T> From<T> for CheckedInt<T> {
    fn from(value: T) -> Self {
        Self { value }
    }
}

impl<T> Display for CheckedInt<T>
where
    T: Display,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        self.value.fmt(f)
    }
}

#[cfg(test)]
pub type CheckedI64 = CheckedInt<i64>;

#[cfg(test)]
mod checked_integer_ring_tests {
    use super::*;

    #[test]
    fn fixed_integer_tests() {
        assert_eq!(0i64, CheckedI64::zero().into());
        assert_eq!(1i64, CheckedI64::one().into());
        let two = CheckedI64::one().add(CheckedI64::one());
        assert_eq!(2i64, two.clone().into());
        assert_eq!(-2i64, two.clone().neg().into());
        assert_eq!(-4i64, two.clone().mul(two.clone().neg()).into());
        let mut three = two;
        three.add_assign(CheckedI64::from(1i64));
        assert_eq!(3i64, three.clone().into());
        assert_eq!(false, three.is_zero());
    }

    #[test]
    #[should_panic]
    fn overflow_test() {
        let max = CheckedI64::from(i64::MAX);
        let _ = max.add(CheckedI64::one());
    }
}
