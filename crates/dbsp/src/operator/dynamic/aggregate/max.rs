use crate::{
    algebra::{HasZero, MonoidValue, Semigroup},
    dynamic::{DataTrait, DynUnit, Erase, WeightTrait},
    operator::Aggregator,
    trace::Cursor,
    DBData, DBWeight, Timestamp,
};
use std::{cmp::max, marker::PhantomData};

/// An [aggregator](`crate::operator::Aggregator`) that returns the
/// largest value with non-zero weight.
#[derive(Clone)]
pub struct Max;

#[derive(Clone)]
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
    R: DBWeight + MonoidValue,
{
    type Accumulator = V;
    type Output = V;
    type Semigroup = MaxSemigroup<V>;

    fn aggregate<VTrait, RTrait>(
        &self,
        cursor: &mut dyn Cursor<VTrait, DynUnit, T, RTrait>,
    ) -> Option<Self::Accumulator>
    where
        VTrait: DataTrait + ?Sized,
        RTrait: WeightTrait + ?Sized,
        V: Erase<VTrait>,
        R: Erase<RTrait>,
    {
        cursor.fast_forward_keys();

        while cursor.key_valid() {
            let mut weight: R = HasZero::zero();

            cursor.map_times(&mut |_t, w| weight.add_assign_by_ref(unsafe { w.downcast() }));

            if !weight.is_zero() {
                return Some(unsafe { cursor.key().downcast::<V>() }.clone());
            }

            cursor.step_key_reverse();
        }

        None
    }

    fn finalize(&self, accumulator: Self::Accumulator) -> Self::Output {
        accumulator
    }
}
