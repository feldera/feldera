use crate::{
    algebra::{AddAssignByRef, AddByRef, GroupValue, HasZero, IndexedZSet, MulByRef, NegByRef},
    trace::layers::{column_leaf::OrderedColumnLeaf, ordered::OrderedLayer},
    utils::VecExt,
    Circuit, OrdIndexedZSet, Stream, Timestamp,
};
use size_of::SizeOf;
use std::{
    hash::Hash,
    ops::{Add, AddAssign, Div, Neg},
};

/// Intermediate representation of an average as a `(sum, count)` pair.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, SizeOf)]
pub struct Avg<T> {
    sum: T,
    count: isize,
}

impl<T> Avg<T> {
    pub const fn new(sum: T, count: isize) -> Self {
        Self { sum, count }
    }
}

impl<T> HasZero for Avg<T>
where
    T: HasZero,
{
    fn is_zero(&self) -> bool {
        self.sum.is_zero() && self.count.is_zero()
    }

    fn zero() -> Self {
        Self::new(T::zero(), 0)
    }
}

impl<T> Add for Avg<T>
where
    T: Add<Output = T>,
{
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self::new(self.sum + rhs.sum, self.count + rhs.count)
    }
}

impl<T> AddByRef for Avg<T>
where
    T: AddByRef,
{
    fn add_by_ref(&self, other: &Self) -> Self {
        Self::new(self.sum.add_by_ref(&other.sum), self.count + other.count)
    }
}

impl<T> AddAssign for Avg<T>
where
    T: AddAssign,
{
    fn add_assign(&mut self, rhs: Self) {
        self.sum += rhs.sum;
        self.count += rhs.count;
    }
}

impl<T> AddAssignByRef for Avg<T>
where
    T: AddAssignByRef,
{
    fn add_assign_by_ref(&mut self, rhs: &Self) {
        self.sum.add_assign_by_ref(&rhs.sum);
        self.count += rhs.count;
    }
}

impl<T> Neg for Avg<T>
where
    T: Neg<Output = T>,
{
    type Output = Self;

    fn neg(self) -> Self {
        Self::new(self.sum.neg(), self.count.neg())
    }
}

impl<T> NegByRef for Avg<T>
where
    T: NegByRef,
{
    fn neg_by_ref(&self) -> Self {
        Self::new(self.sum.neg_by_ref(), self.count.neg())
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
        Self::new(self.sum.mul_by_ref(rhs), self.count.mul_by_ref(rhs))
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
    /// computed as a composition of two linear aggregates: sum and count.
    /// The `(sum, count)` pair with pair-wise operations is also a linear
    /// aggregate and can be computed with a single
    /// [`Stream::aggregate_linear`] operator. The actual average is
    /// computed by applying the `(sum, count) -> sum / count`
    /// transformation to its output.
    #[track_caller]
    pub fn average<TS, A, F>(&self, f: F) -> Stream<Circuit<P>, OrdIndexedZSet<Z::Key, A, isize>>
    where
        TS: Timestamp + SizeOf,
        Z: IndexedZSet,
        Z::Key: PartialEq + Ord + SizeOf + Hash + Clone + SizeOf + Send,
        Z::Val: Ord + SizeOf + Clone,
        Avg<A>: MulByRef<Z::R, Output = Avg<A>>,
        A: GroupValue + SizeOf + Ord + Send + Clone,
        A: Div<isize, Output = A>,
        isize: MulByRef<Z::R, Output = isize>,
        F: Fn(&Z::Key, &Z::Val) -> A + Clone + 'static,
    {
        let aggregate =
            self.aggregate_linear::<TS, _, _>(move |key, val| Avg::new(f(key, val), 1isize));

        // We're the only possible consumer of the aggregated stream so we can use an
        // owned consumer to skip any cloning
        let average = aggregate.apply_owned_named("MapAverage", |aggregate| {
            /* The boring, semi-naive way to do this

            // Create a builder with the proper capacity, we don't alter the keys so we can
            // take advantage of a builder instead of a batcher
            let mut builder = <OrdIndexedZSet<Z::Key, A, isize> as Batch>::Builder::with_capacity(
                (),
                aggregate.len(),
            );

            // We use a consumer to avoid any clones
            let mut consumer = aggregate.consumer();
            while consumer.key_valid() {
                let (key, mut values) = consumer.next_key();

                // Each key should have a single value
                debug_assert_eq!(values.remaining_values(), 1);
                let (average, weight, ()) = values.next_value();

                builder.push(((key, average.sum / average.count), weight));
            }

            builder.done()
            */

            // Break the given `OrdIndexedZSet` into its components
            let OrderedLayer { keys, offs, vals } = aggregate.layer;
            let (aggregates, diffs) = vals.into_parts();

            // Average out the aggregated values
            // TODO: If we stored `Avg<A>` as two columns (one of `sum: A` and one of
            // `count: isize`) we could even reuse the `sum` vec by doing the division in
            // place (and we even could do it with simd depending on `A`'s type)
            let mut averages = Vec::with_capacity(aggregates.len());
            for Avg { sum, count } in aggregates {
                // Safety: We allocated the correct capacity for `aggregate_values`
                unsafe { averages.push_unchecked(sum / count) };
            }

            // Safety: `averages.len() == diffs.len()`
            let averages = unsafe { OrderedColumnLeaf::from_parts(averages, diffs) };

            // Create a new `OrdIndexedZSet` from our components, notably this doesn't touch
            // `keys`, `offs` or `diffs` which means we don't allocate new vectors for them
            // or even touch their memory
            OrdIndexedZSet {
                // Safety: `keys.len() + 1 == offs.len()`
                layer: unsafe { OrderedLayer::from_parts(keys, offs, averages) },
            }
        });

        // Note: Currently `.aggregate_linear()` is always sharded, but we just do this
        // check so that we don't get any unpleasant surprises if that ever changes
        average.mark_sharded_if(&aggregate);

        average
    }
}
