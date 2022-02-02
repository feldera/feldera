/*
MIT License
SPDX-License-Identifier: MIT

Copyright (c) 2021 VMware, Inc
*/

//! This module contains declarations of abstract algebraic concepts:
//! monoids, groups, rings, etc.

pub mod finite_map;
pub mod zset;

use num::{CheckedAdd, CheckedMul, CheckedSub};
use std::{
    fmt::{Display, Error, Formatter},
    ops::{Add, AddAssign, Mul, Neg},
};

/// A trait for types that have a zero value.
/// This is simlar to the standard Zero trait, but that
/// trait depends on Add and HasZero doesn't.
pub trait HasZero {
    fn is_zero(&self) -> bool;
    fn zero() -> Self;
}

/// Implementation of HasZero for types that already implement Zero
impl<T> HasZero for T
where
    T: num::traits::Zero,
{
    fn is_zero(&self) -> bool {
        T::is_zero(self)
    }
    fn zero() -> Self {
        T::zero()
    }
}

/// A trait for types that have a one value.
/// This is similar to the standard One trait, but that
/// trait depends on Mul and HasOne doesn't.
pub trait HasOne {
    fn one() -> Self;
}

/// Implementation of HasOne for types that already implement One
impl<T> HasOne for T
where
    T: num::traits::One,
{
    fn one() -> Self {
        T::one()
    }
}

/// Like the Add trait, but with arguments by reference.
pub trait AddByRef {
    fn add_by_ref(&self, other: &Self) -> Self;
}

/// Implementation of AddByRef for types that have an Add.
impl<T> AddByRef for T
where
    for<'a> &'a T: Add<Output = T>,
{
    fn add_by_ref(&self, other: &Self) -> Self {
        self.add(other)
    }
}

/// Like the Neg trait, but with arguments by reference.
pub trait NegByRef {
    fn neg_by_ref(&self) -> Self;
}

/// Implementation of AddByRef for types that have an Add.
impl<T> NegByRef for T
where
    for<'a> &'a T: Neg<Output = T>,
{
    fn neg_by_ref(&self) -> Self {
        self.neg()
    }
}

/// Like the AddAsssign trait, but with arguments by reference
pub trait AddAssignByRef {
    fn add_assign_by_ref(&mut self, other: &Self);
}

/// Implemenation of AddAssignByRef for types that already have `AddAssign<&T>`.
impl<T> AddAssignByRef for T
where
    for<'a> T: AddAssign<&'a T>,
{
    fn add_assign_by_ref(&mut self, other: &Self) {
        self.add_assign(other)
    }
}

/// Like the Mul trait, but with arguments by reference
pub trait MulByRef {
    fn mul_by_ref(&self, other: &Self) -> Self;
}

/// Implementation of MulByRef for types that already have Mul.
impl<T> MulByRef for T
where
    for<'a> &'a T: Mul<Output = T>,
{
    fn mul_by_ref(&self, other: &Self) -> Self {
        self.mul(other)
    }
}

/// A type with an associative addition and a zero.
/// We trust the implementation to have an associative addition.
/// (this cannot be checked statically).
pub trait MonoidValue:
    Clone + Eq + 'static + HasZero + Add + AddByRef + AddAssign + AddAssignByRef
{
}

/// Default implementation for all types that have an addition and a zero.
impl<T> MonoidValue for T where
    T: Clone + Eq + 'static + HasZero + Add + AddByRef + AddAssign + AddAssignByRef
{
}

/// A Group is a Monoid with a with negation operation.
/// We expect all our groups to be commutative.
pub trait GroupValue: MonoidValue + NegByRef {}

/// Default implementation of GroupValue for all types that have the required traits.
impl<T> GroupValue for T where
    T: Clone + Eq + 'static + HasZero + Add + AddByRef + AddAssign + AddAssignByRef + NegByRef
{
}

/// A Group with a multiplication operation is a Ring.
pub trait RingValue: GroupValue + MulByRef + HasOne {}

/// Default implementation of RingValue for all types that have the required traits.
impl<T> RingValue for T where
    T: Clone
        + Eq
        + 'static
        + HasZero
        + Add
        + AddByRef
        + AddAssign
        + AddAssignByRef
        + NegByRef
        + MulByRef
        + HasOne
{
}

/// A ring where elements can be compared with zero
pub trait ZRingValue: RingValue {
    /// True if value is greater or equal to zero
    fn ge0(&self) -> bool;
}

/// Default implementation of ZRingValue for all types that have the required traits.
impl<T> ZRingValue for T
where
    T: Clone
        + Eq
        + 'static
        + HasZero
        + Add
        + AddByRef
        + AddAssign
        + AddAssignByRef
        + NegByRef
        + MulByRef
        + HasOne
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
        let two = i64::one().add_by_ref(&i64::one());
        assert_eq!(2, two);
        assert_eq!(-2, two.neg_by_ref());
        assert_eq!(-4, two.mul_by_ref(&two.neg_by_ref()));
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

impl<T> AddByRef for CheckedInt<T>
where
    T: CheckedAdd,
{
    fn add_by_ref(&self, other: &Self) -> Self {
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

impl<T> AddAssignByRef for CheckedInt<T>
where
    T: CheckedAdd,
{
    fn add_assign_by_ref(&mut self, other: &Self) {
        self.value = self.value.checked_add(&other.value).expect("overflow")
    }
}

impl<T> MulByRef for CheckedInt<T>
where
    T: CheckedMul,
{
    fn mul_by_ref(&self, rhs: &Self) -> Self {
        // intentional panic on overflow
        Self {
            value: self.value.checked_mul(&rhs.value).expect("overflow"),
        }
    }
}

impl<T> NegByRef for CheckedInt<T>
where
    T: CheckedSub + ::num::traits::Zero,
{
    fn neg_by_ref(&self) -> Self {
        Self {
            // intentional panic on overflow
            value: T::zero().checked_sub(&self.value).expect("overflow"),
        }
    }
}

impl<T> HasZero for CheckedInt<T>
where
    T: ::num::traits::Zero + CheckedAdd,
{
    fn is_zero(&self) -> bool {
        T::is_zero(&self.value)
    }
    fn zero() -> Self {
        CheckedInt::new(T::zero())
    }
}

impl<T> HasOne for CheckedInt<T>
where
    T: ::num::traits::One + CheckedMul,
{
    fn one() -> Self {
        CheckedInt::new(T::one())
    }
}

impl<T> ZRingValue for CheckedInt<T>
where
    T: Ord
        + ::num::traits::Zero
        + ::num::traits::One
        + CheckedAdd
        + CheckedMul
        + CheckedSub
        + Clone
        + 'static,
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
        let two = CheckedI64::one().add_by_ref(&CheckedI64::one());
        assert_eq!(2i64, two.clone().into());
        assert_eq!(-2i64, two.neg_by_ref().into());
        assert_eq!(-4i64, two.clone().mul_by_ref(&two.neg_by_ref()).into());
        let mut three = two;
        three.add_assign_by_ref(&CheckedI64::from(1i64));
        assert_eq!(3i64, three.clone().into());
        assert_eq!(false, three.is_zero());
    }

    #[test]
    #[should_panic]
    fn overflow_test() {
        let max = CheckedI64::from(i64::MAX);
        let _ = max.add_by_ref(&CheckedI64::one());
    }
}
