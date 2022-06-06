//! Binary operator that applies an arbitrary binary function to its inputs.

use crate::circuit::{
    operator_traits::{BinaryOperator, Operator},
    Scope,
};
use std::borrow::Cow;

/// Applies a user-provided binary function to its inputs at each timestamp.
pub struct Apply2<F> {
    func: F,
}

impl<F> Apply2<F> {
    pub const fn new(func: F) -> Self
    where
        F: 'static,
    {
        Self { func }
    }
}

impl<F> Operator for Apply2<F>
where
    F: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("Apply2")
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        // TODO: either change `F` type to `Fn` from `FnMut` or
        // parameterize the operator with custom fixed point check.
        unimplemented!();
    }
}

impl<T1, T2, T3, F> BinaryOperator<T1, T2, T3> for Apply2<F>
where
    F: Fn(&T1, &T2) -> T3 + 'static,
{
    fn eval(&mut self, i1: &T1, i2: &T2) -> T3 {
        (self.func)(i1, i2)
    }
}
