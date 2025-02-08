//! Differentiation operators.

use std::ops::Neg;

use crate::circuit::checkpointer::Checkpoint;
use crate::circuit::circuit_builder::StreamId;
use crate::{
    algebra::{AddAssignByRef, AddByRef, GroupValue, NegByRef},
    circuit::{Circuit, Stream},
    circuit_cache_key,
    operator::{integrate::IntegralId, Minus},
    NumEntries,
};
use size_of::SizeOf;

circuit_cache_key!(DifferentiateId<C, D>(StreamId => Stream<C, D>));
circuit_cache_key!(NestedDifferentiateId<C, D>(StreamId => Stream<C, D>));

impl<C, D> Stream<C, D>
where
    C: Circuit + 'static,
    D: Checkpoint + SizeOf + NumEntries + GroupValue,
{
    /// Stream differentiation.
    ///
    /// Computes the difference between current and previous value
    /// of `self`: `differentiate(a) = a - z^-1(a)`.
    /// The first output is the first input value, the second output is the
    /// second input value minus the first input value, and so on.
    ///
    /// You shouldn't ordinarily need this operator, at least not for streams of
    /// Z-sets, because most DBSP operators are fully incremental.
    #[track_caller]
    pub fn differentiate(&self) -> Stream<C, D> {
        self.differentiate_with_initial_value(D::zero())
    }

    /// Nested stream differentiation.
    #[track_caller]
    pub fn differentiate_nested(&self) -> Stream<C, D> {
        self.circuit()
            .cache_get_or_insert_with(NestedDifferentiateId::new(self.stream_id()), || {
                let differentiated = self.circuit().add_binary_operator(
                    Minus::new(),
                    &self.try_sharded_version(),
                    &self.try_sharded_version().delay_nested(),
                );
                differentiated.mark_sharded_if(self);
                differentiated
            })
            .clone()
    }
}

impl<C, D> Stream<C, D>
where
    C: Circuit + 'static,
    D: Checkpoint
        + SizeOf
        + NumEntries
        + Neg<Output = D>
        + Clone
        + AddByRef
        + AddAssignByRef
        + NegByRef
        + Eq
        + 'static,
{
    #[track_caller]
    pub fn differentiate_with_initial_value(&self, initial: D) -> Stream<C, D> {
        self.circuit()
            .cache_get_or_insert_with(DifferentiateId::new(self.stream_id()), || {
                let differentiated = self.circuit().add_binary_operator(
                    Minus::new(),
                    &self.try_sharded_version(),
                    &self
                        .try_sharded_version()
                        .delay_with_initial_value(initial.clone()),
                );
                differentiated.mark_sharded_if(self);

                self.circuit()
                    .cache_insert(IntegralId::new(differentiated.stream_id()), self.clone());
                differentiated
            })
            .clone()
    }
}
