//! Operator that consolidates a trace into a single batch.

use std::{borrow::Cow, marker::PhantomData};

use crate::{
    circuit::{
        operator_traits::{Operator, UnaryOperator},
        Circuit, GlobalNodeId, OwnershipPreference, Scope, Stream,
    },
    circuit_cache_key,
    trace::{Batch, Trace},
};

circuit_cache_key!(ConsolidateId<C, D>(GlobalNodeId => Stream<C, D>));

impl<P, T> Stream<Circuit<P>, T>
where
    P: Clone + 'static,
    T: Clone + Trace<Time = ()> + 'static,
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
    pub fn consolidate(&self) -> Stream<Circuit<P>, T::Batch> {
        self.circuit()
            .cache_get_or_insert_with(ConsolidateId::new(self.origin_node_id().clone()), || {
                let consolidated = self.circuit().add_unary_operator_with_preference(
                    Consolidate::new(),
                    &self.try_sharded_version(),
                    OwnershipPreference::STRONGLY_PREFER_OWNED,
                );
                consolidated.mark_sharded_if(self);

                consolidated
            })
            .clone()
    }
}

pub struct Consolidate<T> {
    _type: PhantomData<T>,
}

impl<T> Consolidate<T> {
    pub fn new() -> Self {
        Self { _type: PhantomData }
    }
}

impl<T> Default for Consolidate<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Operator for Consolidate<T>
where
    T: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("Consolidate")
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<T> UnaryOperator<T, T::Batch> for Consolidate<T>
where
    T: Trace<Time = ()> + 'static,
{
    fn eval(&mut self, _i: &T) -> T::Batch {
        unimplemented!()
    }

    fn eval_owned(&mut self, i: T) -> T::Batch {
        i.consolidate().unwrap_or_else(|| T::Batch::empty(()))
    }
}
