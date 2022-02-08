//! Differentiator operator.

use crate::{
    algebra::GroupValue,
    circuit::{
        operator_traits::{Operator, UnaryOperator},
        Circuit, Scope, Stream,
    },
};
use std::{borrow::Cow, mem::swap};

impl<P, V> Stream<Circuit<P>, V>
where
    P: Clone + 'static,
    V: GroupValue,
{
    /// Apply the `Differentiate` operator to `self`.
    pub fn differentiate(&self) -> Stream<Circuit<P>, V> {
        self.circuit()
            .add_unary_operator(Differentiate::new(), self)
    }
}

/// The differentiation operator computes the difference between the current and
/// the previous values of the stream.
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
        Cow::from("Differentiate")
    }
    fn clock_start(&mut self, _scope: Scope) {}
    fn clock_end(&mut self, _scope: Scope) {
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

    fn eval_owned(&mut self, mut i: V) -> V {
        swap(&mut self.previous, &mut i);
        i = i.neg();
        i.add_assign_by_ref(&self.previous);
        i
    }
}
