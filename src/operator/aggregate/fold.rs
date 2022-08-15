use crate::{algebra::MonoidValue, operator::aggregate::Aggregator, trace::Cursor, Timestamp};
use std::convert::identity;

/// An [aggregator](`crate::operator::Aggregator`) that can be expressed
/// as a fold of the input Z-set.
///
/// The `Fold` aggregator iterates over `(key, weight)` pairs with
/// non-zero weights updating the value of an accumulator with a
/// user-provided `step` function.  The final aggregate is produced by
/// applying a user-provided `output` function to the final value of
/// the accumulator.
///
/// # Type arguments
///
/// * `A` - accumulator
/// * `SF` - step function
/// * `OF` - output function
#[derive(Clone)]
pub struct Fold<A, SF, OF> {
    init: A,
    step: SF,
    output: OF,
}

impl<A, SF> Fold<A, SF, fn(A) -> A> {
    /// Create a `Fold` aggregator with initial accumulator value `init`,
    /// step function `step`, and identity output function.
    ///
    /// This constructor caters for the common case when the final value of
    /// the accumulator is the desired aggregate, i.e., no output processing
    /// is required.
    pub fn new(init: A, step: SF) -> Self {
        Self {
            init,
            step,
            output: identity,
        }
    }
}

impl<A, SF, OF> Fold<A, SF, OF> {
    /// Create a `Fold` aggregator with initial accumulator value `init`,
    /// step function `step`, and output function `output`.
    pub fn with_output(init: A, step: SF, output: OF) -> Self {
        Self { init, step, output }
    }
}

impl<V, T, R, A, O, SF, OF> Aggregator<V, T, R> for Fold<A, SF, OF>
where
    T: Timestamp,
    R: MonoidValue,
    A: Clone,
    SF: Fn(&mut A, &V, R),
    OF: Fn(A) -> O,
{
    type Output = O;

    fn aggregate<'s, C>(&mut self, cursor: &mut C) -> Option<Self::Output>
    where
        C: Cursor<'s, V, (), T, R>,
    {
        let mut acc = self.init.clone();
        let mut non_empty = false;

        while cursor.key_valid() {
            let mut weight = R::zero();

            cursor.map_times(|_t, w| weight.add_assign_by_ref(w));
            if !weight.is_zero() {
                non_empty = true;
                (self.step)(&mut acc, cursor.key(), weight);
            }

            cursor.step_key();
        }

        // Aggregator must return None iff the input cursor is empty (all keys have
        // weight 0).
        non_empty.then(|| (self.output)(acc))
    }
}
