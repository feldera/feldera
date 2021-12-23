//! z^-1 operator delays its input by one timestamp.

use crate::circuit::operator_traits::{
    Operator, StrictOperator, StrictUnaryValOperator, UnaryValOperator,
};
use std::mem::swap;

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
    T: Default,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Z1<T>
where
    T: Default,
{
    pub fn new() -> Self {
        Self {
            val: Default::default(),
        }
    }

    fn reset(&mut self) {
        self.val = Default::default()
    }
}

impl<T> Operator for Z1<T>
where
    T: Default + 'static,
{
    fn stream_start(&mut self) {}
    fn stream_end(&mut self) {
        self.reset();
    }
}

impl<T> UnaryValOperator<T, T> for Z1<T>
where
    T: Default + 'static,
{
    fn eval(&mut self, mut i: T) -> T {
        swap(&mut self.val, &mut i);
        i
    }
}

impl<T> StrictOperator<T> for Z1<T>
where
    T: Default + 'static,
{
    fn get_output(&mut self) -> T {
        let mut old_val = Default::default();
        swap(&mut self.val, &mut old_val);
        old_val
    }
}

impl<T> StrictUnaryValOperator<T, T> for Z1<T>
where
    T: Default + 'static,
{
    fn eval_strict(&mut self, i: T) {
        self.val = i
    }
}

#[test]
fn sum_circuit() {
    let mut z1 = Z1::new();

    let expected_result = vec![
        "".to_string(),
        "foo".to_string(),
        "bar".to_string(),
        "".to_string(),
        "x".to_string(),
        "y".to_string(),
    ];

    // Test `UnaryValOperator` API.
    let mut res = Vec::new();
    z1.stream_start();
    res.push(z1.eval("foo".to_string()));
    res.push(z1.eval("bar".to_string()));
    res.push(z1.eval("buzz".to_string()));
    z1.stream_end();

    z1.stream_start();
    res.push(z1.eval("x".to_string()));
    res.push(z1.eval("y".to_string()));
    res.push(z1.eval("z".to_string()));
    z1.stream_end();

    assert_eq!(res, expected_result);

    // Test `StrictUnaryValOperator` API.
    let mut res = Vec::new();
    z1.stream_start();
    res.push(z1.get_output());
    z1.eval_strict("foo".to_string());
    res.push(z1.get_output());
    z1.eval_strict("bar".to_string());
    res.push(z1.get_output());
    z1.eval_strict("buzz".to_string());
    z1.stream_end();

    z1.stream_start();
    res.push(z1.get_output());
    z1.eval_strict("x".to_string());
    res.push(z1.get_output());
    z1.eval_strict("y".to_string());
    res.push(z1.get_output());
    z1.eval_strict("z".to_string());
    z1.stream_end();

    assert_eq!(res, expected_result);
}
