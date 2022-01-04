//! Binary plus operator

use std::{marker::PhantomData, ops::Add};

use crate::circuit::operator_traits::{BinaryRefRefOperator, Operator};

/// Binary plus operator.
///
/// Output the sum of its inputs for each timestamp.
pub struct Plus<T> {
    phantom: PhantomData<T>,
}

impl<T> Default for Plus<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Plus<T> {
    pub fn new() -> Self {
        Self {
            phantom: PhantomData,
        }
    }
}

impl<T> Operator for Plus<T>
where
    T: 'static,
{
    fn stream_start(&mut self) {}
    fn stream_end(&mut self) {}
}

impl<T> BinaryRefRefOperator<T, T, T> for Plus<T>
where
    T: 'static,
    for<'a> &'a T: Add<Output = T>,
{
    fn eval(&mut self, i1: &T, i2: &T) -> T {
        i1 + i2
    }
}
