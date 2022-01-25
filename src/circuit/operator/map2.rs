//! Binary operator that applies an arbitrary binary function to its inputs.

use crate::circuit::operator_traits::{BinaryOperator, Operator};
use std::borrow::Cow;

/// Binary map operator.
///
/// Applies a user-provided function to its inputs at each timestamp.
pub struct Map2<F> {
    map: F,
}

impl<F> Map2<F> {
    pub const fn new(map: F) -> Self
    where
        F: 'static,
    {
        Self { map }
    }
}

impl<F> Operator for Map2<F>
where
    F: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("Map2")
    }

    fn clock_start(&mut self) {}
    fn clock_end(&mut self) {}
}

impl<T1, T2, T3, F> BinaryOperator<T1, T2, T3> for Map2<F>
where
    F: Fn(&T1, &T2) -> T3 + 'static,
{
    fn eval(&mut self, i1: &T1, i2: &T2) -> T3 {
        (self.map)(i1, i2)
    }
}
