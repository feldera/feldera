//! Operator that applies an arbitrary function to its input.

use crate::circuit::{
    operator_traits::{Operator, UnaryOperator},
    Circuit, Stream,
};

use std::borrow::Cow;

impl<P, T1> Stream<Circuit<P>, T1>
where
    P: Clone + 'static,
    T1: Clone + 'static,
{
    /// Apply  the `Apply` operator to `self`.
    pub fn apply<F, T2>(&self, func: F) -> Stream<Circuit<P>, T2>
    where
        F: FnMut(&T1) -> T2 + 'static,
        T2: Clone + 'static,
    {
        self.circuit().add_unary_operator(Apply::new(func), self)
    }
}

/// Operator that applies a user provided function to its input at each
/// timestamp.
pub struct Apply<F> {
    func: F,
}

impl<F> Apply<F> {
    pub const fn new(func: F) -> Self
    where
        F: 'static,
    {
        Self { func }
    }
}

impl<F> Operator for Apply<F>
where
    F: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("Apply")
    }

    fn fixedpoint(&self) -> bool {
        // TODO: either change `F` type to `Fn` from `FnMut` or
        // parameterize the operator with custom fixed point check.
        unimplemented!();
    }
}

impl<T1, T2, F> UnaryOperator<T1, T2> for Apply<F>
where
    F: FnMut(&T1) -> T2 + 'static,
{
    fn eval(&mut self, i1: &T1) -> T2 {
        (self.func)(i1)
    }
}
