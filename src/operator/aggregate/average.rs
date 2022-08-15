use crate::{
    algebra::{AddAssignByRef, AddByRef, GroupValue, HasZero, IndexedZSet, MulByRef, NegByRef},
    operator::FilterMap,
    Circuit, OrdIndexedZSet, Stream, Timestamp,
};
use deepsize::DeepSizeOf;
use std::{
    hash::Hash,
    ops::{Add, AddAssign, Div, Neg},
};

/// Intermediate representation of an average as a `(sum, count)` pair.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, DeepSizeOf)]
pub struct Avg<T>(T, isize);

impl<T> HasZero for Avg<T>
where
    T: HasZero,
{
    fn is_zero(&self) -> bool {
        self.0.is_zero() && self.1.is_zero()
    }

    fn zero() -> Self {
        Self(T::zero(), 0)
    }
}

impl<T> Add for Avg<T>
where
    T: Add<Output = T>,
{
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self(self.0 + rhs.0, self.1 + rhs.1)
    }
}

impl<T> AddByRef for Avg<T>
where
    T: AddByRef,
{
    fn add_by_ref(&self, other: &Self) -> Self {
        Self(self.0.add_by_ref(&other.0), self.1 + other.1)
    }
}

impl<T> AddAssign for Avg<T>
where
    T: AddAssign,
{
    fn add_assign(&mut self, rhs: Self) {
        self.0 += rhs.0;
        self.1 += rhs.1;
    }
}

impl<T> AddAssignByRef for Avg<T>
where
    T: AddAssignByRef,
{
    fn add_assign_by_ref(&mut self, rhs: &Self) {
        self.0.add_assign_by_ref(&rhs.0);
        self.1 += rhs.1;
    }
}

impl<T> Neg for Avg<T>
where
    T: Neg<Output = T>,
{
    type Output = Self;

    fn neg(self) -> Self {
        Self(self.0.neg(), self.1.neg())
    }
}

impl<T> NegByRef for Avg<T>
where
    T: NegByRef,
{
    fn neg_by_ref(&self) -> Self {
        Self(self.0.neg_by_ref(), self.1.neg())
    }
}

impl<T, Rhs> MulByRef<Rhs> for Avg<T>
where
    T: MulByRef<Rhs, Output = T>,
    isize: MulByRef<Rhs, Output = isize>,
    // This bound is only here to prevent conflict with `MulByRef<Present>` :(
    Rhs: From<i8>,
{
    type Output = Avg<T>;

    fn mul_by_ref(&self, rhs: &Rhs) -> Avg<T> {
        Avg(self.0.mul_by_ref(rhs), self.1.mul_by_ref(rhs))
    }
}

impl<P, Z> Stream<Circuit<P>, Z>
where
    P: Clone + 'static,
    Z: Clone + 'static,
{
    /// Incremental average aggregate.
    ///
    /// This operator is a specialization of [`Stream::aggregate`] that for
    /// each key `k` in the input indexed Z-set computes the average value as:
    ///
    ///
    /// ```text
    ///    __                __
    ///    ╲                 ╲
    ///    ╱ v * w     /     ╱  w
    ///    ‾‾                ‾‾
    ///   (v,w) ∈ Z[k]      (v,w) ∈ Z[k]
    /// ```
    ///
    /// # Design
    ///
    /// Average is a quasi-linear aggregate, meaning that it can be efficiently
    /// computed as a compositon of two linear aggregates: sum and count.
    /// The `(sum, count)` pair with pair-wise operations is also a linear
    /// aggregate and can be computed with a single
    /// [`Stream::aggregate_linear`] operator. The actual average is
    /// computed by applying the `(sum, count) -> sum / count`
    /// transformation to its output.
    pub fn average<TS, A, F>(&self, f: F) -> Stream<Circuit<P>, OrdIndexedZSet<Z::Key, A, isize>>
    where
        TS: Timestamp + DeepSizeOf,
        Z: IndexedZSet,
        Z::Key: PartialEq + Ord + Hash + Clone + DeepSizeOf + Send,
        Z::Val: Ord + Clone,
        Avg<A>: MulByRef<Z::R, Output = Avg<A>>,
        A: GroupValue + DeepSizeOf + Ord + Send + Clone,
        A: Div<isize, Output = A>,
        isize: MulByRef<Z::R, Output = isize>,
        F: Fn(&Z::Key, &Z::Val) -> A + Clone + 'static,
    {
        self.aggregate_linear::<TS, _, _>(move |key, val| Avg(f(key, val), 1isize))
            .map_index(|(k, avg)| (k.clone(), (avg.0.clone()) / avg.1))
    }
}
