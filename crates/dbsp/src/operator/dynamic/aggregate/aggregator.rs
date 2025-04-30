use std::{marker::PhantomData, mem::take};

use dyn_clone::DynClone;

use crate::{
    algebra::Semigroup,
    dynamic::{
        DataTrait, DowncastTrait, DynOpt, DynUnit, Erase, Factory, WeightTrait, WithFactory,
    },
    trace::Cursor,
    DBData, DBWeight,
};

pub trait AggOutputFunc<A: ?Sized, O: ?Sized>: Fn(&mut A, &mut O) + DynClone {}
impl<A: ?Sized, O: ?Sized, F> AggOutputFunc<A, O> for F where F: Fn(&mut A, &mut O) + Clone {}

dyn_clone::clone_trait_object! {<A: ?Sized, O: ?Sized> AggOutputFunc<A, O>}

pub trait AggCombineFunc<A: ?Sized>: Fn(&mut A, &A) + DynClone {}
impl<A: ?Sized, F> AggCombineFunc<A> for F where F: Fn(&mut A, &A) + Clone {}

/// A trait for aggregator objects.  An aggregator summarizes the contents
/// of a Z-set into a single value.
///
/// This trait supports two aggregation methods that can be combined in
/// various ways to compute the aggregate efficiently.  First, the
/// [`aggregate`](`Self::aggregate`) method takes a cursor pointing to
/// the first key of a Z-set and scans the cursor to compute the aggregate
/// over all values in the cursor.  Second, the
/// [`Semigroup`](`Self::Semigroup`) associated type allows aggregating
/// partitioned Z-sets by combining aggregates computed over individual
/// partitions (e.g., computed by different worker threads).
///
/// This design requires aggregate values to form a semigroup with a `+`
/// operation such that `Agg(x + y) = Agg(x) + Agg(y)`, i.e., aggregating
/// a union of two Z-sets must produce the same result as the sum of
/// individual aggregates.  Not all aggregates have this property.  E.g.,
/// the average value of a Z-set cannot be computed by combining the
/// averages of its subsets.  We can get around this problem by representing
/// average as `(sum, count)` tuple with point-wise `+` operator
/// `(sum1, count1) + (sum2, count2) = (sum1+sum2, count1+count2)`.
/// The final result is converted to an actual average value by dividing
/// the first element of the tuple by the second.  This is a general technique
/// that works for all aggregators (although it may not always be optimal).
///
/// To support such aggregates, this trait distinguishes between the
/// `Accumulator` type returned by [`aggregate`](`Self::aggregate`),
/// which must implement
/// [`trait Semigroup`](`crate::algebra::Semigroup`), and the final
/// [`Output`](`Self::Output`) of the aggregator.  The latter is
/// computed by applying the [`finalize`](`Self::finalize`) method
/// to the final value of the accumulator.
///
/// This is a low-level trait that is mostly used to build libraries of
/// aggregators.  Users will typically work with ready-made implementations
/// like [`Min`](`crate::operator::Min`) and [`Fold`](`crate::operator::Fold`).
// TODO: Owned aggregation using `Consumer`
pub trait Aggregator<K, T, R>: Clone + 'static {
    /// Accumulator type returned by [`Self::finalize`].
    type Accumulator: DBData;

    /// Semigroup structure over aggregate values.
    ///
    /// Can be used to separately aggregate subsets of values (e.g., in
    /// different worker threads) and combine the results.  This
    /// `Semigroup` implementation must be consistent with `Self::aggregate`,
    /// meaning that computing the aggregate piecewise and combining
    /// the results using `Self::Semigroup` should yield the same value as
    /// aggregating the entire input using `Self::aggregate`.
    // TODO: We currently only use this with `radix_tree`, which only
    // requires the semigroup structure (i.e., associativity).  In the future
    // we will also use this in computing regular aggregates by combining
    // per-worker aggregates computes over arbitrary subsets of values,
    // which additionally requires commutativity.  Do we want to introduce
    // the `CommutativeSemigroup` trait?
    type Semigroup: Semigroup<Self::Accumulator>;

    /// Aggregate type produced by this aggregator.
    type Output: DBData;

    /// Takes a cursor pointing to the first key of a Z-set and outputs
    /// an aggregate of the Z-set.
    ///
    /// # Invariants
    ///
    /// This is a low-level API that relies on the implementer to maintain the
    /// following invariants:
    ///
    /// * The method must return `None` if the total weight of each key is zero.
    ///   It must return `Some` otherwise.
    fn aggregate<KTrait, RTrait>(
        &self,
        cursor: &mut dyn Cursor<KTrait, DynUnit, T, RTrait>,
    ) -> Option<Self::Accumulator>
    where
        KTrait: DataTrait + ?Sized,
        RTrait: WeightTrait + ?Sized,
        K: Erase<KTrait>,
        R: Erase<RTrait>;

    /// Compute the final value of the aggregate.
    fn finalize(&self, accumulator: Self::Accumulator) -> Self::Output;

    /// Applies `aggregate` to `cursor` followed by `finalize`.
    fn aggregate_and_finalize<KTrait, RTrait>(
        &self,
        cursor: &mut dyn Cursor<KTrait, DynUnit, T, RTrait>,
    ) -> Option<Self::Output>
    where
        KTrait: DataTrait + ?Sized,
        RTrait: WeightTrait + ?Sized,
        K: Erase<KTrait>,
        R: Erase<RTrait>,
    {
        self.aggregate(cursor).map(|x| self.finalize(x))
    }
}

/// Add postprocessing step to any aggregator.
#[derive(Clone)]
pub struct Postprocess<A, F> {
    aggregator: A,
    postprocess: F,
}

impl<A, F> Postprocess<A, F> {
    pub fn new(aggregator: A, postprocess: F) -> Self {
        Self {
            aggregator,
            postprocess,
        }
    }
}

impl<A, F, K, T, R, O> Aggregator<K, T, R> for Postprocess<A, F>
where
    A: Aggregator<K, T, R>,
    F: (Fn(&A::Output) -> O) + Clone + 'static,
    O: DBData,
{
    type Accumulator = A::Accumulator;
    type Semigroup = A::Semigroup;
    type Output = O;

    fn aggregate<KTrait, RTrait>(
        &self,
        cursor: &mut dyn Cursor<KTrait, DynUnit, T, RTrait>,
    ) -> Option<Self::Accumulator>
    where
        KTrait: DataTrait + ?Sized,
        RTrait: WeightTrait + ?Sized,
        K: Erase<KTrait>,
        R: Erase<RTrait>,
    {
        self.aggregator.aggregate(cursor)
    }

    fn finalize(&self, accumulator: Self::Accumulator) -> Self::Output {
        (self.postprocess)(&self.aggregator.finalize(accumulator))
    }
}

pub trait DynAggregator<K, T, R>: DynClone + 'static
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    type Accumulator: DataTrait + ?Sized;

    type Output: DataTrait + ?Sized;

    fn opt_accumulator_factory(&self) -> &'static dyn Factory<DynOpt<Self::Accumulator>>;
    fn output_factory(&self) -> &'static dyn Factory<Self::Output>;

    fn combine(&self) -> &dyn AggCombineFunc<Self::Accumulator>;

    fn aggregate(
        &self,
        cursor: &mut dyn Cursor<K, DynUnit, T, R>,
        accumulator: &mut DynOpt<Self::Accumulator>,
    );

    fn finalize(&self, accumulator: &mut Self::Accumulator, output: &mut Self::Output);

    fn aggregate_and_finalize(
        &self,
        cursor: &mut dyn Cursor<K, DynUnit, T, R>,
        output: &mut DynOpt<Self::Output>,
    );
}

pub struct DynAggregatorImpl<
    K: ?Sized,
    KType,
    T: 'static,
    R: ?Sized,
    RType,
    A,
    Acc: ?Sized,
    Out: ?Sized,
> {
    aggregator: A,
    phantom: PhantomData<fn(&Acc, &Out, &K, &KType, &T, &R, &RType)>,
}

impl<K: ?Sized, KType, T: 'static, R: ?Sized, RType, A: Clone, Acc: ?Sized, Out: ?Sized> Clone
    for DynAggregatorImpl<K, KType, T, R, RType, A, Acc, Out>
{
    fn clone(&self) -> Self {
        Self {
            aggregator: self.aggregator.clone(),
            phantom: PhantomData,
        }
    }
}

impl<K: ?Sized, KType, T: 'static, R: ?Sized, RType, A, Acc: ?Sized, Out: ?Sized>
    DynAggregatorImpl<K, KType, T, R, RType, A, Acc, Out>
{
    pub fn new(aggregator: A) -> Self {
        Self {
            aggregator,
            phantom: PhantomData,
        }
    }
}

impl<K, KType, T: 'static, R, RType, A, Acc, Out> DynAggregator<K, T, R>
    for DynAggregatorImpl<K, KType, T, R, RType, A, Acc, Out>
where
    A: Aggregator<KType, T, RType>,
    A::Accumulator: Erase<Acc>,
    A::Output: Erase<Out>,
    K: DataTrait + ?Sized,
    KType: DBData + Erase<K>,
    R: WeightTrait + ?Sized,
    RType: DBWeight + Erase<R>,
    Acc: DataTrait + ?Sized,
    Out: DataTrait + ?Sized,
{
    type Accumulator = Acc;
    type Output = Out;

    fn opt_accumulator_factory(&self) -> &'static dyn Factory<DynOpt<Self::Accumulator>> {
        WithFactory::<Option<A::Accumulator>>::FACTORY
    }

    fn output_factory(&self) -> &'static dyn Factory<Self::Output> {
        WithFactory::<A::Output>::FACTORY
    }

    fn combine(&self) -> &dyn AggCombineFunc<Self::Accumulator> {
        &|acc, val| {
            let acc: &mut A::Accumulator = unsafe { acc.downcast_mut::<A::Accumulator>() };
            let val: &A::Accumulator = unsafe { val.downcast::<A::Accumulator>() };
            *acc = A::Semigroup::combine(acc, val);
        }
    }

    fn aggregate(
        &self,
        cursor: &mut dyn Cursor<K, DynUnit, T, R>,
        acc: &mut DynOpt<Self::Accumulator>,
    ) {
        let acc = unsafe { acc.downcast_mut::<Option<A::Accumulator>>() };

        *acc = self.aggregator.aggregate(cursor);
    }

    fn finalize(&self, acc: &mut Self::Accumulator, output: &mut Self::Output) {
        let acc: &mut A::Accumulator = unsafe { acc.downcast_mut::<A::Accumulator>() };
        let output: &mut A::Output = unsafe { output.downcast_mut::<A::Output>() };

        *output = self.aggregator.finalize(take(acc))
    }

    fn aggregate_and_finalize(
        &self,
        cursor: &mut dyn Cursor<K, DynUnit, T, R>,
        output: &mut DynOpt<Self::Output>,
    ) {
        let output = unsafe { output.downcast_mut::<Option<A::Output>>() };

        let acc = self.aggregator.aggregate(cursor);
        *output = acc.map(|acc| self.aggregator.finalize(acc))
    }
}
