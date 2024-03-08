use crate::{
    algebra::{HasZero, MonoidValue, Semigroup},
    dynamic::{DataTrait, DynUnit, Erase, WeightTrait},
    operator::Aggregator,
    trace::Cursor,
    DBData, DBWeight, Timestamp,
};
use std::{convert::identity, marker::PhantomData};

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
/// * `S` - semigroup structure used to compute aggregates piecewise
/// * `SF` - step function
/// * `OF` - output function
#[derive(Clone)]
pub struct Fold<V, A, S, SF, OF> {
    init: A,
    step: SF,
    output: OF,
    phantom: PhantomData<(V, S)>,
}

impl<V, A, S, SF> Fold<V, A, S, SF, fn(A) -> A> {
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
            phantom: PhantomData,
        }
    }
}

impl<V, A, S, SF, OF> Fold<V, A, S, SF, OF> {
    /// Create a `Fold` aggregator with initial accumulator value `init`,
    /// step function `step`, and output function `output`.
    pub fn with_output(init: A, step: SF, output: OF) -> Self {
        Self {
            init,
            step,
            output,
            phantom: PhantomData,
        }
    }
}

impl<V, T, R, A, S, O, SF, OF> Aggregator<V, T, R> for Fold<V, A, S, SF, OF>
where
    T: Timestamp,
    V: DBData,
    R: DBWeight + MonoidValue,
    A: DBData,
    SF: Fn(&mut A, &V, R) + Clone + 'static,
    OF: Fn(A) -> O + Clone + 'static,
    S: Semigroup<A> + Clone + 'static,
    O: DBData,
{
    type Accumulator = A;
    type Output = O;
    type Semigroup = S;

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
        let mut acc = self.init.clone();
        let mut non_empty = false;

        while cursor.key_valid() {
            let mut weight: R = HasZero::zero();

            cursor.map_times(&mut |_t, w| weight.add_assign_by_ref(unsafe { w.downcast() }));
            if !weight.is_zero() {
                non_empty = true;
                (self.step)(&mut acc, unsafe { cursor.key().downcast() }, weight);
            }

            cursor.step_key();
        }

        non_empty.then_some(acc)
    }

    fn finalize(&self, acc: Self::Accumulator) -> Self::Output {
        (self.output)(acc)
    }
}
