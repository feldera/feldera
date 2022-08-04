//! Defines an operator which negates the input value

use crate::algebra::NegByRef;
use crate::circuit::operator_traits::UnaryOperator;
use crate::circuit::{operator_traits::Operator, Circuit, OwnershipPreference, Scope, Stream};
use std::borrow::Cow;
use std::marker::PhantomData;
use std::ops::Neg;

impl<P, D> Stream<Circuit<P>, D>
where
    D: Clone + 'static + Neg<Output = D> + NegByRef,
    P: Clone + 'static,
{
    pub fn neg(&self) -> Stream<Circuit<P>, D> {
        self.circuit().add_unary_operator(UnaryMinus::new(), self)
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
        algebra::HasZero,
        operator::Generator,
        trace::{ord::OrdZSet, Batch},
        zset, Circuit,
    };

    #[test]
    fn zset_sum() {
        let build_circuit = |circuit: &Circuit<()>| {
            let mut s = <OrdZSet<_, _> as HasZero>::zero();
            let source = circuit.add_source(Generator::new(move || {
                let res = s.clone();
                s = s.merge(&zset! { 5 => 1, 6 => 2 });
                res
            }));
            source
                .neg()
                .plus(&source)
                .inspect(|s| assert_eq!(s, &<OrdZSet<_, _> as HasZero>::zero()));
            source
        };

        let circuit = Circuit::build(move |circuit| {
            build_circuit(circuit);
        })
        .unwrap()
        .0;

        for _ in 0..100 {
            circuit.step().unwrap();
        }
    }
}
