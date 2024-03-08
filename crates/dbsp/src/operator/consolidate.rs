use crate::{
    trace::BatchReaderFactories,
    typed_batch::{DynTrace, Trace, TypedBatch},
    Circuit, Stream,
};

impl<C, T> Stream<C, T>
where
    C: Circuit,
    T: Trace<Time = ()> + Clone,
    T::InnerTrace: Clone,
{
    // TODO: drop the `Time = ()` requirement?
    /// Consolidate a trace into a single batch.
    ///
    /// Each element in the input streams is a trace, consisting of multiple
    /// batches of updates.  This operator consolidates the trace into a
    /// single batch, which uses less memory and can be handled more
    /// efficiently by most operators than the trace.
    ///
    /// This operator is typically attached to the output of a nested circuit
    /// computed as the sum of deltas across all iterations of the circuit.
    /// Once the iteration has converged (e.g., reaching a fixed point) is a
    /// good time to consolidate the output.
    pub fn consolidate(
        &self,
    ) -> Stream<C, TypedBatch<T::Key, T::Val, T::R, <T::InnerTrace as DynTrace>::Batch>> {
        let factories = BatchReaderFactories::new::<T::Key, T::Val, T::R>();

        self.inner().dyn_consolidate(&factories).typed()
    }
}
