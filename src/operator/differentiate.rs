//! Differentiation operators.

use crate::{
    algebra::GroupValue,
    circuit::{Circuit, GlobalNodeId, Stream},
    circuit_cache_key,
    operator::Minus,
    NumEntries,
};
use deepsize::DeepSizeOf;

circuit_cache_key!(DifferentiateId<C, D>(GlobalNodeId => Stream<C, D>));
circuit_cache_key!(NestedDifferentiateId<C, D>(GlobalNodeId => Stream<C, D>));

impl<P, D> Stream<Circuit<P>, D>
where
    P: Clone + 'static,
    D: DeepSizeOf + NumEntries + GroupValue,
{
    /// Stream differentiation.
    ///
    /// Computes the difference between current and previous value
    /// of `self`: `differentiate(a) = a - z^-1(a)`.
    pub fn differentiate(&self) -> Stream<Circuit<P>, D> {
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
    pub fn differentiate_nested(&self) -> Stream<Circuit<P>, D> {
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
