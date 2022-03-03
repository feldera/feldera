//! Differentiator operator.

use crate::{
    algebra::GroupValue,
    circuit::{Circuit, NodeId, Stream},
    circuit_cache_key,
    operator::Minus,
};

circuit_cache_key!(DifferentiateId<C, D>(NodeId => Stream<C, D>));
circuit_cache_key!(NestedDifferentiateId<C, D>(NodeId => Stream<C, D>));

impl<P, D> Stream<Circuit<P>, D>
where
    P: Clone + 'static,
    D: GroupValue,
{
    /// Stream differentiation.
    ///
    /// Computes the difference between current and previous value
    /// of `self`: `differentiate(a) = a - z^-1(a)`.
    pub fn differentiate(&self) -> Stream<Circuit<P>, D> {
        if let Some(differential) = self
            .circuit()
            .cache()
            .get(&DifferentiateId::new(self.local_node_id()))
        {
            return differential.clone();
        }

        let differential = self
            .circuit()
            .add_binary_operator(Minus::new(), self, &self.delay());

        self.circuit().cache().insert(
            DifferentiateId::new(self.local_node_id()),
            differential.clone(),
        );

        differential
    }

    /// Nested stream differentiation.
    pub fn differentiate_nested(&self) -> Stream<Circuit<P>, D> {
        if let Some(differential) = self
            .circuit()
            .cache()
            .get(&NestedDifferentiateId::new(self.local_node_id()))
        {
            return differential.clone();
        }

        let differential =
            self.circuit()
                .add_binary_operator(Minus::new(), self, &self.delay_nested());

        self.circuit().cache().insert(
            NestedDifferentiateId::new(self.local_node_id()),
            differential.clone(),
        );

        differential
    }
}
