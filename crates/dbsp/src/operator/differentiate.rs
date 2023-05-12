//! Differentiation operators.

use crate::{
    algebra::GroupValue,
    circuit::{Circuit, GlobalNodeId, Stream},
    circuit_cache_key,
    operator::Minus,
    NumEntries,
};
use size_of::SizeOf;

circuit_cache_key!(DifferentiateId<C, D>(GlobalNodeId => Stream<C, D>));
circuit_cache_key!(NestedDifferentiateId<C, D>(GlobalNodeId => Stream<C, D>));

impl<C, D> Stream<C, D>
where
    C: Circuit + 'static,
    D: SizeOf + NumEntries + GroupValue,
{
    /// Stream differentiation.
    ///
    /// Computes the difference between current and previous value
    /// of `self`: `differentiate(a) = a - z^-1(a)`.
    /// The first output is the first input value, the second output is the
    /// second input value minus the first input value, and so on.
    ///
    /// You shouldn't ordinarily need this operator, at least not for streams of
    /// Z-sets, because DBSP operators are fully incremental.
    pub fn differentiate(&self) -> Stream<C, D> {
        self.circuit()
            .cache_get_or_insert_with(DifferentiateId::new(self.origin_node_id().clone()), || {
                let differentiated = self.circuit().add_binary_operator(
                    Minus::new(),
                    &self.try_sharded_version(),
                    &self.try_sharded_version().delay(),
                );
                differentiated.mark_sharded_if(self);
                differentiated
            })
            .clone()
    }

    /// Nested stream differentiation.
    pub fn differentiate_nested(&self) -> Stream<C, D> {
        self.circuit()
            .cache_get_or_insert_with(
                NestedDifferentiateId::new(self.origin_node_id().clone()),
                || {
                    let differentiated = self.circuit().add_binary_operator(
                        Minus::new(),
                        &self.try_sharded_version(),
                        &self.try_sharded_version().delay_nested(),
                    );
                    differentiated.mark_sharded_if(self);
                    differentiated
                },
            )
            .clone()
    }
}
