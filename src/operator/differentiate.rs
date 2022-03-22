//! Differentiation operators.

use crate::{
    algebra::GroupValue,
    circuit::{Circuit, NodeId, Stream},
    circuit_cache_key,
    operator::Minus,
    NumEntries,
};

circuit_cache_key!(DifferentiateId<C, D>(NodeId => Stream<C, D>));
circuit_cache_key!(NestedDifferentiateId<C, D>(NodeId => Stream<C, D>));

impl<P, D> Stream<Circuit<P>, D>
where
    P: Clone + 'static,
    D: NumEntries + GroupValue,
{
    /// Stream differentiation.
    ///
    /// Computes the difference between current and previous value
    /// of `self`: `differentiate(a) = a - z^-1(a)`.
    pub fn differentiate(&self) -> Stream<Circuit<P>, D> {
        self.circuit()
            .cache_get_or_insert_with(DifferentiateId::new(self.local_node_id()), || {
                self.circuit()
                    .add_binary_operator(Minus::new(), self, &self.delay())
            })
            .clone()
    }

    /// Nested stream differentiation.
    pub fn differentiate_nested(&self) -> Stream<Circuit<P>, D> {
        self.circuit()
            .cache_get_or_insert_with(NestedDifferentiateId::new(self.local_node_id()), || {
                self.circuit()
                    .add_binary_operator(Minus::new(), self, &self.delay_nested())
            })
            .clone()
    }
}
