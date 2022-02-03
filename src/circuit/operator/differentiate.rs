//! Differentiator operator: direct implementation, without an explicit z

use crate::algebra::GroupValue;
use crate::circuit::{
    operator_traits::{Operator, UnaryOperator},
    Circuit, Stream,
};
use std::borrow::Cow;

impl<P, V> Stream<Circuit<P>, V>
where
    P: Clone + 'static,
    V: GroupValue,
{
    pub fn differentiate(&self) -> Stream<Circuit<P>, V> {
        self.circuit()
            .add_unary_operator(Differentiate::new(), self)
    }
}

pub struct Differentiate<V>
where
    V: GroupValue,
{
    previous: V,
}

impl<V> Differentiate<V>
where
    V: GroupValue,
{
    pub fn new() -> Self {
        Self {
            previous: V::zero(),
        }
    }
}

impl<V> Default for Differentiate<V>
where
    V: GroupValue,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<V> Operator for Differentiate<V>
where
    V: GroupValue,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("Differentiator")
    }
    fn clock_start(&mut self) {}
    fn clock_end(&mut self) {
        self.previous = V::zero();
    }
}

impl<V> UnaryOperator<V, V> for Differentiate<V>
where
    V: GroupValue,
{
    fn eval(&mut self, i: &V) -> V {
        let result = i.add_by_ref(&self.previous.neg_by_ref());
        self.previous = i.clone();
        result
    }

    fn eval_owned(&mut self, i: V) -> V {
        let result = i.add_by_ref(&self.previous.neg_by_ref());
        self.previous = i;
        result
    }
}
