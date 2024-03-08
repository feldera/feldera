//! Defines an operator which negates the input value

use crate::{
    algebra::NegByRef,
    circuit::{
        operator_traits::{Operator, UnaryOperator},
        Circuit, OwnershipPreference, Scope, Stream,
    },
};
use std::{borrow::Cow, marker::PhantomData, ops::Neg};

impl<C, D> Stream<C, D>
where
    D: Clone + 'static + Neg<Output = D> + NegByRef,
    C: Circuit,
{
    /// Returns a stream with the same type as `self` in which each value is
    /// negated. Negating an indexed Z-set negates all the weights.
    pub fn neg(&self) -> Stream<C, D> {
        let negated = self
            .circuit()
            .add_unary_operator(UnaryMinus::new(), &self.try_sharded_version());

        // If the input stream is sharded then the negated stream is sharded
        negated.mark_sharded_if(self);
        negated
    }
}

pub struct UnaryMinus<T> {
    phantom: PhantomData<T>,
}

impl<T> UnaryMinus<T> {
    pub fn new() -> Self {
        Self {
            phantom: PhantomData,
        }
    }
}

impl<T> Operator for UnaryMinus<T>
where
    T: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("UnaryMinus")
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<T> Default for UnaryMinus<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> UnaryOperator<T, T> for UnaryMinus<T>
where
    T: 'static + NegByRef + Neg<Output = T> + Clone,
{
    fn eval(&mut self, input: &T) -> T {
        input.neg_by_ref()
    }

    fn eval_owned(&mut self, i: T) -> T {
        i.neg()
    }

    fn input_preference(&self) -> OwnershipPreference {
        OwnershipPreference::PREFER_OWNED
    }
}

#[cfg(test)]
mod test {
    use crate::{
        algebra::HasZero, operator::Generator, typed_batch::OrdZSet, zset, Circuit, RootCircuit,
    };

    #[test]
    fn zset_sum() {
        let build_circuit = |circuit: &RootCircuit| {
            let mut s = <OrdZSet<u64> as HasZero>::zero();
            let source = circuit.add_source(Generator::new(move || {
                let res = s.clone();
                s = s.merge(&zset! { 5 => 1, 6 => 2 });
                res
            }));
            source
                .neg()
                .plus(&source)
                .inspect(|s| assert_eq!(s, &<OrdZSet<_> as HasZero>::zero()));
            Ok(source)
        };

        let circuit = RootCircuit::build(move |circuit| build_circuit(circuit))
            .unwrap()
            .0;

        for _ in 0..100 {
            circuit.step().unwrap();
        }
    }
}
