use crate::{algebra::MonoidValue, operator::aggregate::Aggregator, trace::Cursor, Timestamp};

/// An [aggregator](`crate::operator::Aggregator`) that returns the
/// largest value with non-zero weight.
pub struct Max;

impl<V, T, R> Aggregator<V, T, R> for Max
where
    V: Clone,
    T: Timestamp,
    R: MonoidValue,
{
    type Output = V;

    // TODO: this can be more efficient with reverse iterator.
    fn aggregate<'s, C>(&mut self, cursor: &mut C) -> Option<Self::Output>
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
