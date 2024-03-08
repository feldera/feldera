use super::{AggOutputFunc, IncAggregateLinearFactories};
use crate::{
    algebra::{
        AddAssignByRef, AddByRef, HasZero, IndexedZSet, IndexedZSetReader, MulByRef, NegByRef,
        OrdIndexedZSet,
    },
    declare_trait_object,
    dynamic::{ClonableTrait, DataTrait, Erase, Factory, Weight, WeightTrait, WithFactory},
    trace::Deserializable,
    Circuit, DBData, DBWeight, DynZWeight, Stream, Timestamp, ZWeight,
};
use rkyv::{Archive, Deserialize, Serialize};
use size_of::SizeOf;
use std::{
    fmt::Debug,
    hash::Hash,
    mem::take,
    ops::{Add, AddAssign, Div, Neg},
};

pub struct AvgFactories<Z, A, W, T>
where
    Z: IndexedZSetReader,
    A: DataTrait + ?Sized,
    W: DataTrait + ?Sized,
    T: Timestamp,
{
    aggregate_factories:
        IncAggregateLinearFactories<Z, DynAverage<W, DynZWeight>, OrdIndexedZSet<Z::Key, A>, T>,
    weight_factory: &'static dyn Factory<W>,
}

impl<Z, A, W, T> AvgFactories<Z, A, W, T>
where
    Z: IndexedZSet,
    A: DataTrait + ?Sized,
    W: WeightTrait + ?Sized,
    DynAverage<W, DynZWeight>: WeightTrait,
    T: Timestamp,
{
    pub fn new<KType, AType, WType>() -> Self
    where
        KType: DBData + Erase<Z::Key>,
        <KType as Deserializable>::ArchivedDeser: Ord,
        WType: DBWeight + Erase<W>,
        AType: DBWeight + Erase<A>,
        WType: From<ZWeight> + Div<Output = WType>,
    {
        Self {
            aggregate_factories: IncAggregateLinearFactories::new::<
                KType,
                Avg<WType, ZWeight>,
                AType,
            >(),
            weight_factory: WithFactory::<WType>::FACTORY,
        }
    }
}

/// Representation of a partially computed average aggregate as a `(sum, count)`
/// tuple.
///
/// This struct represents the result of the linear part of the average
/// aggregate as a `(sum, count)` tuple (see [`Stream::dyn_average`]).  The
/// actual average value can be obtained by dividing `sum` by `count`.  `Avg`
/// forms a commutative monoid with point-wise plus operation `(sum1, count1) +
/// (sum2, count2) = (sum1 + sum2, count1 + count2)`.
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

pub trait Average<T: DataTrait + ?Sized, R: WeightTrait + ?Sized>: Weight {
    fn sum(&self) -> &T;
    fn count(&self) -> &R;
    fn split_mut(&mut self) -> (&mut T, &mut R);

    #[allow(clippy::wrong_self_convention)]
    fn from_refs(&mut self, sum: &T, count: &R);

    #[allow(clippy::wrong_self_convention)]
    fn from_vals(&mut self, sum: &mut T, count: &mut R);
    fn compute_avg(&self, avg: &mut T);
}

impl<T1Type, T2Type, T1, T2> Average<T1, T2> for Avg<T1Type, T2Type>
where
    T1Type: DBWeight + Erase<T1>,
    T2Type: DBWeight + Erase<T2>,
    T1Type: From<T2Type> + Div<Output = T1Type>,
    T1: DataTrait + ?Sized,
    T2: WeightTrait + ?Sized,
{
    fn sum(&self) -> &T1 {
        self.sum.erase()
    }

    fn count(&self) -> &T2 {
        self.count.erase()
    }

    fn split_mut(&mut self) -> (&mut T1, &mut T2) {
        (self.sum.erase_mut(), self.count.erase_mut())
    }

    fn from_refs(&mut self, sum: &T1, count: &T2) {
        let sum: &T1Type = unsafe { sum.downcast::<T1Type>() };
        let count: &T2Type = unsafe { count.downcast::<T2Type>() };

        self.sum = sum.clone();
        self.count = count.clone();
    }

    fn from_vals(&mut self, sum: &mut T1, count: &mut T2) {
        let sum: &mut T1Type = unsafe { sum.downcast_mut::<T1Type>() };
        let count: &mut T2Type = unsafe { count.downcast_mut::<T2Type>() };

        self.sum = take(sum);
        self.count = take(count);
    }

    fn compute_avg(&self, avg: &mut T1) {
        let avg: &mut T1Type = unsafe { avg.downcast_mut::<T1Type>() };

        *avg = Avg::compute_avg(self).unwrap();
    }
}

declare_trait_object!(DynAverage<T, R> = dyn Average<T, R>
where
    T: DataTrait + ?Sized,
    R: WeightTrait + ?Sized
);

impl<C, Z> Stream<C, Z>
where
    C: Circuit,
    Z: Clone + 'static,
{
    /// See [`Stream::average`].
    #[track_caller]
    pub fn dyn_average<A, W>(
        &self,
        factories: &AvgFactories<Z, A, W, C::Time>,
        f: Box<dyn Fn(&Z::Key, &Z::Val, &DynZWeight, &mut W)>,
        out_func: Box<dyn AggOutputFunc<W, A>>,
    ) -> Stream<C, OrdIndexedZSet<Z::Key, A>>
    where
        A: DataTrait + ?Sized,
        W: DataTrait + ?Sized,
        Z: IndexedZSet,
    {
        let weight_factory = factories.weight_factory;
        self.dyn_aggregate_linear_generic(
            &factories.aggregate_factories,
            Box::new(
                move |k: &Z::Key, v: &Z::Val, w: &Z::R, avg: &mut DynAverage<W, Z::R>| {
                    let (sum, count) = avg.split_mut();
                    w.clone_to(count);
                    f(k, v, w, sum);
                },
            ),
            Box::new(move |avg, out| {
                weight_factory.with(&mut |w| {
                    avg.compute_avg(w);
                    out_func(w, out);
                })
            }),
        )

        //f: &dyn Fn(&Z::Key, &Z::Val, &Z::R, MutRef<A>),

        // We're the only possible consumer of the aggregated stream so we can
        // use an owned consumer to skip any cloning
        //let average = aggregate.apply_owned_named("ApplyAverage",
        // apply_average);

        /*let average = aggregate.dyn_map_index(
            &factories.output_factories,
            Box::new(|(k, avg), key_val| {
                let (key, val) = key_val.split_mut();
                k.clone_to(key);
                avg.compute_avg(val);
            }),
        );*/

        // Note: Currently `.aggregate_linear()` is always sharded, but we just
        // do this check so that we don't get any unpleasant surprises
        // if that ever changes average.mark_sharded_if(&aggregate);

        //average
    }
}

// /// The gist of what we're doing here is this:
// ///
// /// - We receive an owned `OrdIndexedZSet` so we can do whatever we want with
// it /// - `OrdIndexedZSet` consists of four discrete vectors of values, a
// `Vec<K>` ///   of keys, a `Vec<usize>` of value offsets, a `Vec<Avg<A>>` of
// average ///   aggregates and a `Vec<isize>` of differences
// /// - Of these only one vector needs to be changed in *any* way: The
// ///   `Vec<Avg<A>>` needs to be transformed into a `Vec<A>`
// /// - So following this fact, the other three vectors never need to be
// touched /// - Ergo, we simply reuse those three untouched vectors in our
// output ///   `OrdIndexedZSet`, requiring us to do the minimum of work:
// dividing all of ///   our sums by all of our counts within the `Vec<Avg<A>>`
// to produce our ///   output `Vec<A>`
// ///
// /// Note that unfortunately we can't reuse the `Vec<Avg<A>>`'s allocation
// here /// since an `Avg<A>` will never have the same size as an `A` due to
// `Avg<A>` /// containing an extra `isize` field
// fn apply_average<K, A, R, W>(aggregate: OrdIndexedZSet<K, Avg<A, R>, W>) ->
// OrdIndexedZSet<K, A, W> where
//     K: Ord,
//     A: DBData + From<R> + Div<Output = A>,
//     R: DBData + ZRingValue,
//     W: DBWeight,
// {
//     // Break the given `OrdIndexedZSet` into its components
//     let OrderedLayer {
//         keys,
//         offs,
//         vals,
//         lower_bound,
//     } = aggregate.layer;
//     let (aggregates, diffs, lower_bound_avg) = vals.into_parts();

//     // Average out the aggregated values
//     // TODO: If we stored `Avg<A>` as two columns (one of `sum: A` and one of
//     // `count: isize`) we could even reuse the `sum` vec by doing the
// division in     // place (and we even could do it with simd depending on
// `A`'s type)     let mut averages = Vec::with_capacity(aggregates.len());
//     for avg in aggregates {
//         // TODO: This can technically use an unchecked division (or an
//         // `assume(count != 0)` call since `.div_unchecked()` is unstable)
// since `count`         // should never be zero since zeroed elements are
// removed from zsets         debug_assert!(!avg.count().is_zero());

//         // Safety: We allocated the correct capacity for `aggregate_values`
//         unsafe { averages.push_unchecked(avg.compute_avg().unwrap()) };
//     }

//     // Safety: `averages.len() == diffs.len()`
//     let averages = unsafe { ColumnLayer::from_parts(averages, diffs,
// lower_bound_avg) };

//     // Create a new `OrdIndexedZSet` from our components, notably this
// doesn't touch     // `keys`, `offs` or `diffs` which means we don't allocate
// new vectors for them     // or even touch their memory
//     OrdIndexedZSet {
//         // Safety: `keys.len() + 1 == offs.len()`
//         layer: unsafe { OrderedLayer::from_parts(keys, offs, averages,
// lower_bound) },     }
// }

#[cfg(test)]
mod tests {
    use rkyv::Deserialize;

    use crate::operator::Avg;

    // #[test]
    // fn apply_average_smoke() {
    //     let input = indexed_zset! {
    //         0 => { Avg::new(1000, 10) => 1 },
    //         1 => { Avg::new(1, 1) => -12 },
    //         1000 => { Avg::new(200, 20) => 544 },
    //     };
    //     let expected = indexed_zset! {
    //         0 => { 100 => 1 },
    //         1 => { 1 => -12 },
    //         1000 => { 10 => 544 },
    //     };
    //
    //     let output = apply_average(input);
    //     assert_eq!(output, expected);
    // }

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
