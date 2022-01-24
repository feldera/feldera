//! Defines a sink operator that inspects every element of its input stream by applying a
//! user-provided callback to it.

use crate::circuit::operator_traits::{Operator, SinkOperator};
use std::{borrow::Cow, marker::PhantomData};

/// Sink operator that consumes a stream of values of type `T` and
/// applies a user-provided callback to each input.
pub struct Inspect<T, F> {
    callback: F,
    phantom: PhantomData<T>,
}

impl<T, F> Inspect<T, F>
where
    F: FnMut(&T),
{
    /// Create a new instance of the `Inspect` operator that will apply `callback` to each value in
    /// the input stream.
    pub fn new(callback: F) -> Self {
        Self {
            callback,
            phantom: PhantomData,
        }
    }
}

impl<T, F> Operator for Inspect<T, F>
where
    T: 'static,
    F: FnMut(&T) + 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("Inspect")
    }

    fn stream_start(&mut self) {}
    fn stream_end(&mut self) {}
}

impl<T, F> SinkOperator<T> for Inspect<T, F>
where
    T: 'static,
    F: FnMut(&T) + 'static,
{
    fn eval(&mut self, i: &T) {
        (self.callback)(i)
    }
}
