use crate::circuit::checkpointer::Checkpoint;
use crate::{
    circuit::OwnershipPreference,
    operator::{z1::DelayedId, Z1},
    Circuit, NumEntries, RootCircuit, Stream,
};
use size_of::SizeOf;

impl<T> Stream<RootCircuit, T>
where
    T: Clone + 'static,
{
    /// Folds every element in the input stream into an accumulator and outputs
    /// the current value of the accumulator at every clock cycle.
    ///
    /// # Arguments
    ///
    /// * `init` - initial value of the accumulator.
    /// * `fold_func` - closure that computes the new value of the accumulator
    ///   as a function of the previous value and the new input at each clock
    ///   cycle.
    #[track_caller]
    pub fn stream_fold<A, F>(&self, init: A, fold_func: F) -> Stream<RootCircuit, A>
    where
        F: Fn(A, &T) -> A + 'static,
        A: Checkpoint + Eq + Clone + SizeOf + NumEntries + 'static,
    {
        let (prev_accumulator, feedback) = self.circuit().add_feedback(Z1::new(init));
        let new_accumulator = prev_accumulator.apply2_owned(self, fold_func);

        feedback
            .connect_with_preference(&new_accumulator, OwnershipPreference::STRONGLY_PREFER_OWNED);
        self.circuit().cache_insert(
            DelayedId::new(new_accumulator.stream_id()),
            prev_accumulator,
        );

        new_accumulator
    }
}
