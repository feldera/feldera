//! Defines an operator that generates an infinite output stream from a single
//! seed value.

use crate::circuit::{
    operator_traits::{Data, Operator, SourceOperator},
    Scope,
};
use std::{borrow::Cow, marker::PhantomData};

/// A source operator that yields an infinite output stream
/// from a generator function.
pub struct Generator<T, F> {
    generator: F,
    _t: PhantomData<T>,
}

impl<T, F> Generator<T, F>
where
    T: Clone,
{
    /// Creates a generator
    pub fn new(g: F) -> Self {
        Self {
            generator: g,
            _t: Default::default(),
        }
    }
}

impl<T, F> Operator for Generator<T, F>
where
    T: Data,
    F: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("Generator")
    }
    fn clock_start(&mut self, _scope: Scope) {}
    fn clock_end(&mut self, _scope: Scope) {}
}

impl<T, F> SourceOperator<T> for Generator<T, F>
where
    F: FnMut() -> T + 'static,
    T: Data,
{
    fn eval(&mut self) -> T {
        (self.generator)()
    }
}
