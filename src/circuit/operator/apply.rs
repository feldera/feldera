//! Operator that applies an arbitrary function to its input.

use crate::circuit::operator_traits::{Operator, UnaryOperator};
use std::borrow::Cow;

/// Operator that applies a user provided function to its input at each timestamp.
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

    fn clock_start(&mut self) {}
    fn clock_end(&mut self) {}
}

impl<T1, T2, F> UnaryOperator<T1, T2> for Apply<F>
where
    F: Fn(&T1) -> T2 + 'static,
{
    fn eval(&mut self, i1: &T1) -> T2 {
        (self.func)(i1)
    }
}
