use crate::{
    algebra::{MonoidValue, Semigroup},
    operator::aggregate::Aggregator,
    trace::Cursor,
    DBData, Timestamp,
};
use std::{cmp::min, marker::PhantomData};

/// An [aggregator](`crate::operator::Aggregator`) that returns the
/// smallest value with non-zero weight.
///
/// This is a highly efficient aggregator, as it only scans the input
/// Z-set until hitting the first non-zero weight.
#[derive(Clone)]
pub struct Min;

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
    R: MonoidValue,
{
    type Output = V;
    type Semigroup = MinSemigroup<V>;

    fn aggregate<'s, C>(&self, cursor: &mut C) -> Option<Self::Output>
    where
        C: Cursor<'s, V, (), T, R>,
    {
        while cursor.key_valid() {
            // FIXME: This could be more succinct if we had an `Add<&Self, Output = Self>`
            // bound on `R`
            let weight = cursor.fold_times(R::zero(), |mut acc, _, weight| {
                acc.add_assign_by_ref(weight);
                acc
            });
            if !weight.is_zero() {
                return Some(cursor.key().clone());
            }

            cursor.step_key();
        }

        None
    }
}
