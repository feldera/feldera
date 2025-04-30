use crate::{
    algebra::{HasZero, MonoidValue, Semigroup},
    dynamic::{DataTrait, DynUnit, Erase, WeightTrait},
    operator::Aggregator,
    trace::Cursor,
    utils::Tup1,
    DBData, DBWeight, Timestamp,
};
use std::{cmp::min, marker::PhantomData};

/// An [aggregator](`crate::operator::Aggregator`) that returns the
/// smallest value with non-zero weight.
///
/// This is a highly efficient aggregator, as it only scans the input
/// Z-set until hitting the first non-zero weight.
#[derive(Clone)]
pub struct Min;

#[derive(Clone)]
pub struct MinSemigroup<V>(PhantomData<V>);

impl<V> Semigroup<V> for MinSemigroup<V>
where
    V: Ord + Clone,
{
    fn combine(left: &V, right: &V) -> V {
        min(left, right).clone()
    }
}

impl<V, T, R> Aggregator<V, T, R> for Min
where
    V: DBData,
    T: Timestamp,
    R: DBWeight + MonoidValue,
{
    type Accumulator = V;
    type Output = V;
    type Semigroup = MinSemigroup<V>;

    fn aggregate<VTrait, RTrait>(
        &self,
        cursor: &mut dyn Cursor<VTrait, DynUnit, T, RTrait>,
    ) -> Option<Self::Output>
    where
        VTrait: DataTrait + ?Sized,
        RTrait: WeightTrait + ?Sized,
        V: Erase<VTrait>,
        R: Erase<RTrait>,
    {
        while cursor.key_valid() {
            // FIXME: This could be more succinct if we had an `Add<&Self, Output = Self>`
            // bound on `R`
            let mut weight: R = HasZero::zero();
            cursor.map_times(&mut |_, w| {
                weight.add_assign_by_ref(unsafe { w.downcast() });
            });
            if !weight.is_zero() {
                return Some(unsafe { cursor.key().downcast::<V>().clone() });
            }

            cursor.step_key();
        }

        None
    }

    fn finalize(&self, accumulator: Self::Accumulator) -> Self::Output {
        accumulator
    }
}

/// An aggregator that returns the smallest Some() value with non-zero weight
/// or None if no such value exists.
///
/// This is useful for implementing the SQL MIN operator.
#[derive(Clone)]
pub struct MinSome1;

#[derive(Clone)]
pub struct MinSome1Semigroup<V>(PhantomData<V>);

impl<V> Semigroup<Tup1<Option<V>>> for MinSome1Semigroup<V>
where
    V: DBData,
{
    fn combine(left: &Tup1<Option<V>>, right: &Tup1<Option<V>>) -> Tup1<Option<V>> {
        Tup1(match (&left.0, &right.0) {
            (None, None) => None,
            (Some(left), None) => Some(left.clone()),
            (None, Some(right)) => Some(right.clone()),
            (Some(left), Some(right)) => Some(min(left, right).clone()),
        })
    }
}

impl<V, T, R> Aggregator<Tup1<Option<V>>, T, R> for MinSome1
where
    V: DBData,
    T: Timestamp,
    R: DBWeight + MonoidValue,
{
    type Accumulator = Tup1<Option<V>>;
    type Output = Tup1<Option<V>>;
    type Semigroup = MinSome1Semigroup<V>;

    fn aggregate<VTrait, RTrait>(
        &self,
        cursor: &mut dyn Cursor<VTrait, DynUnit, T, RTrait>,
    ) -> Option<Self::Output>
    where
        VTrait: DataTrait + ?Sized,
        RTrait: WeightTrait + ?Sized,
        Tup1<Option<V>>: Erase<VTrait>,
        R: Erase<RTrait>,
    {
        let mut result = None;

        while cursor.key_valid() {
            // FIXME: This could be more succinct if we had an `Add<&Self, Output = Self>`
            // bound on `R`
            let mut weight: R = HasZero::zero();
            cursor.map_times(&mut |_, w| {
                weight.add_assign_by_ref(unsafe { w.downcast() });
            });
            if !weight.is_zero() {
                match unsafe { &cursor.key().downcast::<Tup1<Option<V>>>().0 } {
                    Some(v) => return Some(Tup1(Some(v.clone()))),
                    None => result = Some(Tup1(None)),
                }
            }

            cursor.step_key();
        }

        result
    }

    fn finalize(&self, accumulator: Self::Accumulator) -> Self::Output {
        accumulator
    }
}
