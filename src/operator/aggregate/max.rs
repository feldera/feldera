use crate::{
    algebra::{MonoidValue, Semigroup},
    operator::aggregate::Aggregator,
    trace::Cursor,
    DBData, Timestamp,
};
use std::{cmp::max, marker::PhantomData};

/// An [aggregator](`crate::operator::Aggregator`) that returns the
/// largest value with non-zero weight.
#[derive(Clone)]
pub struct Max;

pub struct MaxSemigroup<V>(PhantomData<V>);

impl<V> Semigroup<V> for MaxSemigroup<V>
where
    V: Ord + Clone,
{
    fn combine(left: &V, right: &V) -> V {
        max(left, right).clone()
    }
}

impl<V, T, R> Aggregator<V, T, R> for Max
where
    V: DBData,
    T: Timestamp,
    R: MonoidValue,
{
    type Output = V;
    type Semigroup = MaxSemigroup<V>;

    // TODO: this can be more efficient with reverse iterator.
    fn aggregate<'s, C>(&self, cursor: &mut C) -> Option<Self::Output>
    where
        C: Cursor<'s, V, (), T, R>,
    {
        let mut result = None;

        while cursor.key_valid() {
            let mut weight = R::zero();

            cursor.map_times(|_t, w| weight.add_assign_by_ref(w));

            if !weight.is_zero() {
                result = Some(cursor.key().clone());
            }

            cursor.step_key();
        }

        result
    }
}
