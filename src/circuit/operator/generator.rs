//! Defines an operator that generates an infinite output stream from a single
//! seed value.

use crate::circuit::operator_traits::{Data, Operator, SourceOperator};
use std::borrow::Cow;

/// A source operator that yields an infinite output stream from an initial
/// seed and a transformer function.  On `clock_start`, the internal operator
/// state is reset to the seed value.  On each `eval`, the operator outputs
/// its current state before updating the state using the transformer function.
pub struct Generator<T, F> {
    seed: T,
    val: T,
    update: F,
}

impl<T, F> Generator<T, F>
where
    T: Clone,
{
    /// Creates a generator with initial `seed` and state transformer function.
    pub fn new(seed: T, update: F) -> Self {
        Self {
            seed: seed.clone(),
            val: seed,
            update,
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
    fn clock_start(&mut self) {}
    fn clock_end(&mut self) {
        self.val = self.seed.clone();
    }
}

impl<T, F> SourceOperator<T> for Generator<T, F>
where
    F: Fn(&mut T) + 'static,
    T: Data,
{
    fn eval(&mut self) -> T {
        let res = self.val.clone();
        (self.update)(&mut self.val);
        res
    }
}
