//! This module contains declarations of abstract algebraic concepts:
//! monoids, groups, rings, etc.

#[macro_use]
pub mod finite_map;
mod checked_int;
mod zset;

pub use checked_int::CheckedInt;
pub use finite_map::{FiniteHashMap, FiniteMap, MapBuilder, WithSupport};
pub use zset::{IndexedZSet, IndexedZSetHashMap, ZSet, ZSetHashMap};

use std::ops::{Add, AddAssign, Mul, Neg};

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

/// A trait for objects with size.
pub trait WithNumEntries {
    /// Returns the number of entries in `self`.
    ///
    /// Container types have a well-defined notion of size
    /// (the number of entries in the container).  Scalars have size 1.
    fn num_entries(&self) -> usize;
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
pub trait GroupValue: MonoidValue + Neg<Output = Self> + NegByRef {}

/// Default implementation of GroupValue for all types that have the required
/// traits.
impl<T> GroupValue for T where
    T: Clone
        + Eq
        + 'static
        + HasZero
        + Add
        + AddByRef
        + AddAssign
        + AddAssignByRef
        + Neg<Output = Self>
        + NegByRef
{
}

/// A Group with a multiplication operation is a Ring.
pub trait RingValue: GroupValue + MulByRef + HasOne {}

/// Default implementation of RingValue for all types that have the required
/// traits.
impl<T> RingValue for T where
    T: Clone
        + Eq
        + 'static
        + HasZero
        + Add
        + AddByRef
        + AddAssign
        + AddAssignByRef
        + Neg<Output = Self>
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

/// Default implementation of ZRingValue for all types that have the required
/// traits.
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
        + Neg<Output = Self>
        + NegByRef
        + MulByRef
        + HasOne
        + Ord,
{
    fn ge0(&self) -> bool {
        *self >= Self::zero()
    }
}

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

    #[test]
    fn fixed_isize_tests() {
        assert_eq!(0, isize::zero());
        assert_eq!(1, isize::one());
        let two = isize::one().add_by_ref(&isize::one());
        assert_eq!(2, two);
        assert_eq!(-2, two.neg_by_ref());
        assert_eq!(-4, two.mul_by_ref(&two.neg_by_ref()));
    }
}
