//! This module contains declarations of abstract algebraic concepts:
//! monoids, groups, rings, etc.

mod checked_int;
mod decimal;
mod floats;
mod lattice;
mod order;
mod present;

pub mod zset;

pub mod arcstr;
pub use self::arcstr::ArcStr;

pub use checked_int::CheckedInt;
pub use decimal::Decimal;
pub use floats::{F32, F64};
pub use lattice::Lattice;
pub use order::{PartialOrder, TotalOrder};
pub use present::Present;
pub use zset::{IndexedZSet, ZSet};

use size_of::SizeOf;
use std::{
    marker::PhantomData,
    num::Wrapping,
    ops::{Add, AddAssign, Mul, Neg},
    rc::Rc,
};

/// A trait for types that have a zero value.
///
/// This is similar to the standard Zero trait, but that
/// trait depends on Add and HasZero doesn't.
pub trait HasZero {
    fn is_zero(&self) -> bool;

    fn zero() -> Self;
}

/// Implement `HasZero` for types that already implement `Zero`.
macro_rules! impl_has_zero {
    ($($type:ty),* $(,)?) => {
        $(
            impl $crate::algebra::HasZero for $type {
                #[inline]
                fn is_zero(&self) -> bool {
                    *self == 0
                }

                #[inline]
                fn zero() -> Self {
                    0
                }
            }
        )*
    };
}

impl_has_zero! {
    u8,
    i8,
    u16,
    i16,
    u32,
    i32,
    u64,
    i64,
    u128,
    i128,
    usize,
    isize,
}

impl<T> HasZero for Option<T> {
    #[inline]
    fn is_zero(&self) -> bool {
        self.is_none()
    }

    #[inline]
    fn zero() -> Self {
        None
    }
}

// TODO: Implement for `std::num::Saturating` once stable
impl<T> HasZero for Wrapping<T>
where
    T: HasZero,
{
    #[inline]
    fn is_zero(&self) -> bool {
        self.0.is_zero()
    }

    #[inline]
    fn zero() -> Self {
        Self(T::zero())
    }
}

/// A trait for types that have a one value.
/// This is similar to the standard One trait, but that
/// trait depends on Mul and HasOne doesn't.
pub trait HasOne {
    fn one() -> Self;
}

/// Implement `HasOne` for types that already implement `One`.
macro_rules! impl_has_one {
    ($($type:ty),* $(,)?) => {
        $(
            impl $crate::algebra::HasOne for $type {
                #[inline]
                fn one() -> Self {
                    1
                }
            }
        )*
    };
}

impl_has_one! {
    u8,
    i8,
    u16,
    i16,
    u32,
    i32,
    u64,
    i64,
    u128,
    i128,
    usize,
    isize,
}

impl<T> HasOne for Rc<T>
where
    T: HasOne,
{
    #[inline]
    fn one() -> Self {
        Rc::new(<T as HasOne>::one())
    }
}

/// Like the Add trait, but with arguments by reference.
pub trait AddByRef<Rhs = Self> {
    fn add_by_ref(&self, other: &Rhs) -> Self;
}

/// Implementation of AddByRef for types that have an Add.
impl<T> AddByRef for T
where
    for<'a> &'a T: Add<Output = T>,
{
    #[inline]
    fn add_by_ref(&self, other: &Self) -> Self {
        self.add(other)
    }
}

/// Like the Neg trait, but with arguments by reference.
pub trait NegByRef {
    fn neg_by_ref(&self) -> Self;
}

/// Implementation of NegByRef for types that have a Neg.
impl<T> NegByRef for T
where
    for<'a> &'a T: Neg<Output = T>,
{
    #[inline]
    fn neg_by_ref(&self) -> Self {
        self.neg()
    }
}

/// Like the AddAsssign trait, but with arguments by reference
pub trait AddAssignByRef<Rhs = Self> {
    fn add_assign_by_ref(&mut self, other: &Rhs);
}

/// Implemenation of AddAssignByRef for types that already have `AddAssign<&T>`.
impl<T> AddAssignByRef for T
where
    for<'a> T: AddAssign<&'a T>,
{
    #[inline]
    fn add_assign_by_ref(&mut self, other: &Self) {
        self.add_assign(other)
    }
}

/// Like the Mul trait, but with arguments by reference
pub trait MulByRef<Rhs = Self> {
    type Output;

    fn mul_by_ref(&self, other: &Rhs) -> Self::Output;
}

/// Implementation of MulByRef for types that already have Mul.
impl<T> MulByRef<T> for T
where
    for<'a> &'a T: Mul<Output = Self>,
{
    type Output = Self;

    #[inline]
    fn mul_by_ref(&self, other: &Self) -> Self::Output {
        self.mul(other)
    }
}

/// A type with an associative addition.
/// We trust the implementation to have an associative addition.
/// (this cannot be checked statically).
// TODO: Add a `for<'a> Add<&'a Self, Output = Self>` bound for adding an owned
// and a referenced value together
pub trait SemigroupValue:
    Clone + Eq + SizeOf + Add<Output = Self> + AddByRef + AddAssign + AddAssignByRef + 'static
{
}

impl<T> SemigroupValue for T where
    T: Clone + Eq + SizeOf + Add<Output = Self> + AddByRef + AddAssign + AddAssignByRef + 'static
{
}

/// A type with an associative addition and a zero.
pub trait MonoidValue: SemigroupValue + HasZero {}

/// Default implementation for all types that have an addition and a zero.
impl<T> MonoidValue for T where T: SemigroupValue + HasZero {}

/// A Group is a Monoid with a with negation operation.
/// We expect all our groups to be commutative.
pub trait GroupValue: MonoidValue + Neg<Output = Self> + NegByRef {}

/// Default implementation of GroupValue for all types that have the required
/// traits.
impl<T> GroupValue for T where T: MonoidValue + Neg<Output = Self> + NegByRef {}

/// A Group with a multiplication operation is a Ring.
pub trait RingValue: GroupValue + Mul<Output = Self> + MulByRef<Output = Self> + HasOne {}

/// Default implementation of RingValue for all types that have the required
/// traits.
impl<T> RingValue for T where T: GroupValue + Mul<Output = Self> + MulByRef<Output = Self> + HasOne {}

/// A ring where elements can be compared with zero
pub trait ZRingValue: RingValue {
    /// True if value is greater or equal to zero.
    fn ge0(&self) -> bool;

    /// True if value is less than or equal to zero.
    fn le0(&self) -> bool;
}

/// Default implementation of `ZRingValue` for all types that have the required
/// traits.
impl<T> ZRingValue for T
where
    T: RingValue + Ord,
{
    #[inline]
    fn ge0(&self) -> bool {
        *self >= Self::zero()
    }

    #[inline]
    fn le0(&self) -> bool {
        *self <= Self::zero()
    }
}

impl MulByRef<isize> for i32 {
    type Output = Self;

    #[inline]
    fn mul_by_ref(&self, w: &isize) -> Self::Output {
        (*self as isize * w) as Self
    }
}

impl MulByRef<isize> for i64 {
    type Output = Self;

    #[inline]
    fn mul_by_ref(&self, w: &isize) -> Self::Output {
        (*self as isize * w) as Self
    }
}

impl MulByRef<isize> for f32 {
    type Output = Self;

    #[inline]
    fn mul_by_ref(&self, w: &isize) -> Self::Output {
        *self * ((*w) as f32)
    }
}

impl MulByRef<isize> for f64 {
    type Output = Self;

    #[inline]
    fn mul_by_ref(&self, w: &isize) -> Self::Output {
        *self * ((*w) as f64)
    }
}

impl MulByRef<isize> for F32 {
    type Output = Self;

    #[inline]
    fn mul_by_ref(&self, w: &isize) -> Self::Output {
        *self * ((*w) as f32)
    }
}

impl MulByRef<isize> for F64 {
    type Output = Self;

    #[inline]
    fn mul_by_ref(&self, w: &isize) -> Self::Output {
        *self * ((*w) as f64)
    }
}

impl MulByRef<isize> for Decimal {
    type Output = Self;

    #[inline]
    fn mul_by_ref(&self, w: &isize) -> Self::Output {
        *self * Decimal::from(*w)
    }
}

impl MulByRef<isize> for Option<Decimal> {
    type Output = Self;

    #[inline]
    fn mul_by_ref(&self, w: &isize) -> Self::Output {
        self.as_ref().map(|x| (*x * Decimal::from(*w)))
    }
}

impl MulByRef<isize> for Option<i32> {
    type Output = Self;

    #[inline]
    fn mul_by_ref(&self, w: &isize) -> Self::Output {
        self.as_ref().map(|x| (*x as isize * w) as i32)
    }
}

impl MulByRef<isize> for Option<i64> {
    type Output = Self;

    #[inline]
    fn mul_by_ref(&self, w: &isize) -> Self::Output {
        self.as_ref().map(|x| (*x as isize * w) as i64)
    }
}

impl MulByRef<isize> for Option<f32> {
    type Output = Self;

    #[inline]
    fn mul_by_ref(&self, w: &isize) -> Self::Output {
        self.as_ref().map(|x| *x * (*w as f32))
    }
}

impl MulByRef<isize> for Option<f64> {
    type Output = Self;

    #[inline]
    fn mul_by_ref(&self, w: &isize) -> Self::Output {
        self.as_ref().map(|x| *x * (*w as f64))
    }
}

impl MulByRef<isize> for Option<F32> {
    type Output = Self;

    #[inline]
    fn mul_by_ref(&self, w: &isize) -> Self::Output {
        self.as_ref().map(|x| *x * (*w as f32))
    }
}

impl MulByRef<isize> for Option<F64> {
    type Output = Self;

    #[inline]
    fn mul_by_ref(&self, w: &isize) -> Self::Output {
        self.as_ref().map(|x| *x * (*w as f64))
    }
}

/////////// same for i64

impl MulByRef<i64> for i32 {
    type Output = Self;

    #[inline]
    fn mul_by_ref(&self, w: &i64) -> Self::Output {
        (*self as i64 * w) as Self
    }
}

impl MulByRef<i64> for isize {
    type Output = Self;

    #[inline]
    fn mul_by_ref(&self, w: &i64) -> Self::Output {
        (*self as i64 * w) as Self
    }
}

impl MulByRef<i64> for f32 {
    type Output = Self;

    #[inline]
    fn mul_by_ref(&self, w: &i64) -> Self::Output {
        *self * ((*w) as f32)
    }
}

impl MulByRef<i64> for f64 {
    type Output = Self;

    #[inline]
    fn mul_by_ref(&self, w: &i64) -> Self::Output {
        *self * ((*w) as f64)
    }
}

impl MulByRef<i64> for F32 {
    type Output = Self;

    #[inline]
    fn mul_by_ref(&self, w: &i64) -> Self::Output {
        *self * ((*w) as f32)
    }
}

impl MulByRef<i64> for F64 {
    type Output = Self;

    #[inline]
    fn mul_by_ref(&self, w: &i64) -> Self::Output {
        *self * ((*w) as f64)
    }
}

impl MulByRef<i64> for Decimal {
    type Output = Self;

    #[inline]
    fn mul_by_ref(&self, w: &i64) -> Self::Output {
        *self * Decimal::from(*w)
    }
}

impl MulByRef<i64> for Option<Decimal> {
    type Output = Self;

    #[inline]
    fn mul_by_ref(&self, w: &i64) -> Self::Output {
        self.as_ref().map(|x| (*x * Decimal::from(*w)))
    }
}

impl MulByRef<i64> for Option<i32> {
    type Output = Self;

    #[inline]
    fn mul_by_ref(&self, w: &i64) -> Self::Output {
        self.as_ref().map(|x| (*x as i64 * w) as i32)
    }
}

impl MulByRef<i64> for Option<i64> {
    type Output = Self;

    #[inline]
    fn mul_by_ref(&self, w: &i64) -> Self::Output {
        self.as_ref().map(|x| *x * w)
    }
}

impl MulByRef<i64> for Option<f32> {
    type Output = Self;

    #[inline]
    fn mul_by_ref(&self, w: &i64) -> Self::Output {
        self.as_ref().map(|x| *x * (*w as f32))
    }
}

impl MulByRef<i64> for Option<f64> {
    type Output = Self;

    #[inline]
    fn mul_by_ref(&self, w: &i64) -> Self::Output {
        self.as_ref().map(|x| *x * (*w as f64))
    }
}

impl MulByRef<i64> for Option<F32> {
    type Output = Self;

    #[inline]
    fn mul_by_ref(&self, w: &i64) -> Self::Output {
        self.as_ref().map(|x| *x * (*w as f32))
    }
}

impl MulByRef<i64> for Option<F64> {
    type Output = Self;

    #[inline]
    fn mul_by_ref(&self, w: &i64) -> Self::Output {
        self.as_ref().map(|x| *x * (*w as f64))
    }
}

/// Semigroup over values of type `V`.
///
/// This trait defines an associative binary operation
/// over values of type `V`.  Unlike [`SemigroupValue`],
/// which can only be implemented once per type, it allows
/// creating multiple semigroup structures over the same type.
/// For example, integers form semigroups with addition,
/// multiplication, min, and max operations, among others.
// TODO: add optimized methods that take arguments by value,
// and that combine more than two elements.
pub trait Semigroup<V> {
    /// Apply the semigroup operation to `left` and `right`.
    fn combine(left: &V, right: &V) -> V;

    /// Apply the semigroup operation to values of type `Option<V>`.
    ///
    /// This method defines a monoid over values of type `Option<V>` with
    /// `None` as the neutral element.  It returns:
    /// * `None`, if both arguments are `None`,
    /// * `left` (`right`), if `right` (`left`) is `None`,
    /// * `combine(left.unwrap(), right.unwrap())` otherwise.
    fn combine_opt(left: &Option<V>, right: &Option<V>) -> Option<V>
    where
        V: Clone,
    {
        if let Some(left) = left {
            if let Some(right) = right {
                Some(Self::combine(left, right))
            } else {
                Some(left.clone())
            }
        } else {
            right.clone()
        }
    }
}

/// Trait [`Semigroup`] implementation for types that
/// implement [`SemigroupValue`].
///
/// Implements `Semigroup<V>` using `V`'s natural plus
/// operation.
#[derive(Clone)]
pub struct DefaultSemigroup<V>(PhantomData<V>);

impl<V> Semigroup<V> for DefaultSemigroup<V>
where
    V: SemigroupValue,
{
    fn combine(left: &V, right: &V) -> V {
        left.add_by_ref(right)
    }
}

/// [`Semigroup`] implementation that panics with "not implemented"
/// message.
// TODO: this is a temporary thing that can be used with aggregation operators,
// which currently don't invoke the `Semigroup` trait (but this will change
// in the future).
#[derive(Clone)]
pub struct UnimplementedSemigroup<V>(PhantomData<V>);

impl<V> Semigroup<V> for UnimplementedSemigroup<V> {
    fn combine(_left: &V, _right: &V) -> V {
        unimplemented!()
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

    #[test]
    fn fixed_i64_tests() {
        assert_eq!(0, i64::zero());
        assert_eq!(1, i64::one());
        let two = i64::one().add_by_ref(&i64::one());
        assert_eq!(2, two);
        assert_eq!(-2, two.neg_by_ref());
        assert_eq!(-4, two.mul_by_ref(&two.neg_by_ref()));
    }
}
