//! Operator that consolidates a trace into a single batch.

use std::{borrow::Cow, convert::TryFrom, marker::PhantomData};

use crate::{
    circuit::{
        operator_traits::{Operator, UnaryOperator},
        Circuit, NodeId, OwnershipPreference, Scope, Stream,
    },
    circuit_cache_key,
    trace::{Batch, Trace},
};

circuit_cache_key!(ConsolidateId<C, D>(NodeId => Stream<C, D>));

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
    pub fn consolidate<B>(&self) -> Stream<Circuit<P>, B>
    where
        B: TryFrom<T::Batch> + Clone + 'static,
    {
        self.circuit()
            .cache_get_or_insert_with(ConsolidateId::new(self.local_node_id()), || {
                self.circuit().add_unary_operator_with_preference(
                    Consolidate::new(),
                    self,
                    OwnershipPreference::STRONGLY_PREFER_OWNED,
                )
            })
            .clone()
    }
}

pub struct Consolidate<T, B> {
    _type: PhantomData<(T, B)>,
}

impl<T, B> Consolidate<T, B> {
    pub fn new() -> Self {
        Self { _type: PhantomData }
    }
}

impl<T, B> Default for Consolidate<T, B> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T, B> Operator for Consolidate<T, B>
where
    T: 'static,
    B: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("Consolidate")
    }
    fn clock_start(&mut self, _scope: Scope) {}
    fn clock_end(&mut self, _scope: Scope) {}
    fn fixedpoint(&self) -> bool {
        true
    }
}

impl<T, B> UnaryOperator<T, B> for Consolidate<T, B>
where
    T: Trace<Time = ()> + 'static,
    B: TryFrom<T::Batch> + 'static,
{
    fn eval(&mut self, _i: &T) -> B {
        unimplemented!()
    }

    fn eval_owned(&mut self, i: T) -> B {
        match i.consolidate() {
            Some(batch) => TryFrom::try_from(batch),
            None => TryFrom::try_from(T::Batch::empty(())),
        }
        .unwrap_or_else(|_| panic!("Consolidate::eval_owned does not own its input"))
    }
}
