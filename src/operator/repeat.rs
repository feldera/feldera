//! Defines a source operator that generates an infinite stream of identical
//! values.

use crate::circuit::operator_traits::{Data, Operator, SourceOperator};
use std::borrow::Cow;

/// Source operator that yields the same value on each clock tick.
pub struct Repeat<T> {
    val: T,
}

impl<T> Repeat<T> {
    pub fn new(val: T) -> Self {
        Self { val }
    }
}
impl<T: Data> Operator for Repeat<T> {
    fn name(&self) -> Cow<'static, str> {
        Cow::from("Repeat")
    }

    fn clock_start(&mut self) {}
    fn clock_end(&mut self) {}
}

impl<T: Data> SourceOperator<T> for Repeat<T> {
    fn eval(&mut self) -> T {
        self.val.clone()
    }
}
