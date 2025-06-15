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
    fn fixedpoint(&self, _scope: Scope) -> bool {
        false
    }
}

impl<T, F> SourceOperator<T> for Generator<T, F>
where
    F: FnMut() -> T + 'static,
    T: Data,
{
    async fn eval(&mut self) -> T {
        (self.generator)()
    }
}

/// Generator operator for nested circuits.
///
/// At each parent clock tick, invokes a user-provided reset closure, which
/// returns a generator closure, that yields a nested stream.
pub struct GeneratorNested<T> {
    reset: Box<dyn FnMut() -> Box<dyn FnMut() -> T>>,
    generator: Option<Box<dyn FnMut() -> T>>,
}

impl<T> GeneratorNested<T>
where
    T: Clone,
{
    /// Creates a nested generator with specified `reset` closure.
    pub fn new(reset: Box<dyn FnMut() -> Box<dyn FnMut() -> T>>) -> Self {
        Self {
            reset,
            generator: None,
        }
    }
}

impl<T> Operator for GeneratorNested<T>
where
    T: Data,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("GeneratorNested")
    }
    fn clock_start(&mut self, scope: Scope) {
        if scope == 0 {
            self.generator = Some((self.reset)());
        }
    }

    fn is_input(&self) -> bool {
        true
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        // TODO: do we want a version of `GeneratorNested` that
        // can inform the circuit that it's reached a fixedpoint?
        false
    }
}

impl<T> SourceOperator<T> for GeneratorNested<T>
where
    T: Data,
{
    async fn eval(&mut self) -> T {
        (self.generator.as_mut().unwrap())()
    }
}
