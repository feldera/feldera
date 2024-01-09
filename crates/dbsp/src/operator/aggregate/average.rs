use crate::{
    algebra::{
        AddAssignByRef, AddByRef, GroupValue, HasOne, HasZero, IndexedZSet, MulByRef, NegByRef,
        ZRingValue,
    },
    circuit::WithClock,
    operator::filter_map::FilterMap,
    Circuit, DBData, DBTimestamp, OrdIndexedZSet, Stream,
};
use rkyv::{Archive, Deserialize, Serialize};
use size_of::SizeOf;
use std::{
    hash::Hash,
    ops::{Add, AddAssign, Div, Neg},
};

/// Representation of a partially computed average aggregate as a `(sum, count)`
/// tuple.
///
/// This struct represents the result of the linear part of the average
/// aggregate as a `(sum, count)` tuple (see [`Stream::average`]).  The actual
/// average value can be obtained by dividing `sum` by `count`.  `Avg` forms a
/// commutative monoid with point-wise plus operation `(sum1, count1) + (sum2,
/// count2) = (sum1 + sum2, count1 + count2)`.
#[derive(
    Debug,
    Default,
    Clone,
    Eq,
    Hash,
    PartialEq,
    Ord,
    PartialOrd,
    SizeOf,
    Archive,
    Serialize,
    Deserialize,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
#[archive(bound(archive = "<T as Archive>::Archived: Ord, <R as Archive>::Archived: Ord"))]
#[archive(compare(PartialEq, PartialOrd))]
pub struct Avg<T, R> {
    sum: T,
    count: R,
}

impl<T, R> Avg<T, R> {
    /// Create a new `Avg` object with the given `sum` and `count`.
    pub const fn new(sum: T, count: R) -> Self {
        Self { sum, count }
    }

    /// Returns the `sum` component of the `(sum, count)` tuple.
    pub fn sum(&self) -> T
    where
        T: Clone,
    {
        self.sum.clone()
    }

    /// Returns the `count` component of the `(sum, count)` tuple.
    pub fn count(&self) -> R
    where
        R: Clone,
    {
        self.count.clone()
    }

    /// Returns `sum / count` or `None` if `count` is zero.
    pub fn compute_avg(&self) -> Option<T>
    where
        R: Clone + HasZero,
        T: From<R> + Div<Output = T> + Clone,
    {
        if self.count.is_zero() {
            None
        } else {
            Some(self.sum.clone() / T::from(self.count.clone()))
        }
    }
}

impl<T, R> HasZero for Avg<T, R>
where
    T: HasZero,
    R: HasZero,
{
    fn is_zero(&self) -> bool {
        self.sum.is_zero() && self.count.is_zero()
    }

    fn zero() -> Self {
        Self::new(T::zero(), R::zero())
    }
}

impl<T, R> Add for Avg<T, R>
where
    T: Add<Output = T>,
    R: Add<Output = R>,
{
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self::new(self.sum + rhs.sum, self.count + rhs.count)
    }
}

impl<T, R> AddByRef for Avg<T, R>
where
    T: AddByRef,
    R: AddByRef,
{
    fn add_by_ref(&self, other: &Self) -> Self {
        Self::new(
            self.sum.add_by_ref(&other.sum),
            self.count.add_by_ref(&other.count),
        )
    }
}

impl<T, R> AddAssign for Avg<T, R>
where
    T: AddAssign,
    R: AddAssign,
{
    fn add_assign(&mut self, rhs: Self) {
        self.sum += rhs.sum;
        self.count += rhs.count;
    }
}

impl<T, R> AddAssignByRef for Avg<T, R>
where
    T: AddAssignByRef,
    R: AddAssignByRef,
{
    fn add_assign_by_ref(&mut self, rhs: &Self) {
        self.sum.add_assign_by_ref(&rhs.sum);
        self.count.add_assign_by_ref(&rhs.count);
    }
}

impl<T, R> Neg for Avg<T, R>
where
    T: Neg<Output = T>,
    R: Neg<Output = R>,
{
    type Output = Self;

    fn neg(self) -> Self {
        Self::new(self.sum.neg(), self.count.neg())
    }
}

impl<T, R> NegByRef for Avg<T, R>
where
    T: NegByRef,
    R: NegByRef,
{
    fn neg_by_ref(&self) -> Self {
        Self::new(self.sum.neg_by_ref(), self.count.neg_by_ref())
    }
}

impl<T, R> MulByRef<R> for Avg<T, R>
where
    T: MulByRef<Output = T>,
    T: From<R>,
    R: MulByRef<Output = R> + Clone,
{
    type Output = Avg<T, R>;

    fn mul_by_ref(&self, rhs: &R) -> Avg<T, R> {
        Self::new(
            self.sum.mul_by_ref(&T::from(rhs.clone())),
            self.count.mul_by_ref(rhs),
        )
    }
}

impl<C, Z> Stream<C, Z>
where
    C: Circuit,
    <C as WithClock>::Time: DBTimestamp,
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
    /// computed as a composition of two linear aggregates: sum and count.
    /// The `(sum, count)` pair with pair-wise operations is also a linear
    /// aggregate and can be computed with a single
    /// [`Stream::aggregate_linear`] operator. The actual average is
    /// computed by applying the `(sum, count) -> sum / count`
    /// transformation to its output.
    #[track_caller]
    pub fn average<A, F>(&self, f: F) -> Stream<C, OrdIndexedZSet<Z::Key, A, Z::R>>
    where
        Z: IndexedZSet,
        Z::R: ZRingValue,
        Avg<A, Z::R>: MulByRef<Z::R, Output = Avg<A, Z::R>>,
        A: DBData + From<Z::R> + Div<Z::R, Output = A> + GroupValue,
        F: Fn(&Z::Val) -> A + Clone + 'static,
    {
        let aggregate = self.aggregate_linear(move |val| Avg::new(f(val), Z::R::one()));

        // We're the only possible consumer of the aggregated stream so we can use an
        // owned consumer to skip any cloning
        let average =
            aggregate.map_index(|(k, avg)| (k.clone(), avg.sum.clone() / avg.count.clone()));

        // Note: Currently `.aggregate_linear()` is always sharded, but we just do this
        // check so that we don't get any unpleasant surprises if that ever changes
        average.mark_sharded_if(&aggregate);

        average
    }
}

#[cfg(test)]
mod tests {
    use rkyv::Deserialize;

    use crate::operator::aggregate::average::Avg;

    #[test]
    fn avg_decode_encode() {
        type Type = Avg<u64, i64>;
        for input in [
            Avg::new(0, 0),
            Avg::new(u64::MAX, i64::MAX),
            Avg::new(u64::MIN, i64::MIN),
        ] {
            let input: Type = input;
            let encoded = rkyv::to_bytes::<_, 256>(&input).unwrap();
            let archived = unsafe { rkyv::archived_root::<Type>(&encoded[..]) };
            let decoded: Type = archived.deserialize(&mut rkyv::Infallible).unwrap();
            assert_eq!(decoded, input);
        }
    }
}
