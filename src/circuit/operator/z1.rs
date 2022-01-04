//! z^-1 operator delays its input by one timestamp.

use crate::circuit::operator_traits::{
    Operator, StrictOperator, StrictUnaryValOperator, UnaryValOperator,
};
use std::mem::swap;

use num::Zero;

/// z^-1 operator delays its input by one timestamp.  It outputs the default
/// value of `T` in the first timestamp after [stream_start](`Z1::stream_start`).
/// For all subsequent timestamps, it outputs the value received as input at the
/// previous timestamp.
///
/// # Examples
///
/// ```text
/// time | input | output
/// ---------------------
///   0  |   5   |   0
///   1  |   6   |   5
///   2  |   7   |   6
///   3  |   8   |   7
///         ...
/// ```
///
/// It is a strict operator
pub struct Z1<T> {
    val: T,
}

impl<T> Default for Z1<T>
where
    T: Zero,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Z1<T>
where
    T: Zero,
{
    pub fn new() -> Self {
        Self { val: Zero::zero() }
    }

    fn reset(&mut self) {
        self.val = Zero::zero()
    }
}

impl<T> Operator for Z1<T>
where
    T: Zero + 'static,
{
    fn stream_start(&mut self) {}
    fn stream_end(&mut self) {
        self.reset();
    }
}

impl<T> UnaryValOperator<T, T> for Z1<T>
where
    T: Zero + 'static,
{
    fn eval(&mut self, mut i: T) -> T {
        swap(&mut self.val, &mut i);
        i
    }
}

impl<T> StrictOperator<T> for Z1<T>
where
    T: Zero + 'static,
{
    fn get_output(&mut self) -> T {
        let mut old_val = Zero::zero();
        swap(&mut self.val, &mut old_val);
        old_val
    }
}

impl<T> StrictUnaryValOperator<T, T> for Z1<T>
where
    T: Zero + 'static,
{
    fn eval_strict(&mut self, i: T) {
        self.val = i
    }
}

#[test]
fn sum_circuit() {
    let mut z1 = Z1::new();

    let expected_result = vec![0, 1, 2, 0, 4, 5];

    // Test `UnaryValOperator` API.
    let mut res = Vec::new();
    z1.stream_start();
    res.push(z1.eval(1));
    res.push(z1.eval(2));
    res.push(z1.eval(3));
    z1.stream_end();

    z1.stream_start();
    res.push(z1.eval(4));
    res.push(z1.eval(5));
    res.push(z1.eval(6));
    z1.stream_end();

    assert_eq!(res, expected_result);

    // Test `StrictUnaryValOperator` API.
    let mut res = Vec::new();
    z1.stream_start();
    res.push(z1.get_output());
    z1.eval_strict(1);
    res.push(z1.get_output());
    z1.eval_strict(2);
    res.push(z1.get_output());
    z1.eval_strict(3);
    z1.stream_end();

    z1.stream_start();
    res.push(z1.get_output());
    z1.eval_strict(4);
    res.push(z1.get_output());
    z1.eval_strict(5);
    res.push(z1.get_output());
    z1.eval_strict(6);
    z1.stream_end();

    assert_eq!(res, expected_result);
}
