//! z^-1 operator delays its input by one timestamp.

use crate::circuit::operator_traits::{
    Operator, StrictOperator, StrictUnaryOperator, UnaryOperator,
};
use std::mem::swap;

/// z^-1 operator delays its input by one timestamp.  It outputs a user-defined
/// "zero" value in the first timestamp after [stream_start](`Z1::stream_start`).
/// For all subsequent timestamps, it outputs the value received as input at the
/// previous timestamp.  The zero value is typically the neutral element of
/// a monoid (e.g., 0 for addition or 1 for multiplication).
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
    zero: T,
    val: T,
}

impl<T> Z1<T>
where
    T: Clone,
{
    pub fn new(zero: T) -> Self {
        Self {
            zero: zero.clone(),
            val: zero,
        }
    }

    fn reset(&mut self) {
        self.val = self.zero.clone();
    }
}

impl<T> Operator for Z1<T>
where
    T: Clone + 'static,
{
    fn name(&self) -> &str {
        "Z^-1"
    }

    fn stream_start(&mut self) {}
    fn stream_end(&mut self) {
        self.reset();
    }
    fn prefer_owned_input(&self) -> bool {
        true
    }
}

impl<T> UnaryOperator<T, T> for Z1<T>
where
    T: Clone + 'static,
{
    fn eval(&mut self, i: &T) -> T {
        let mut res = i.clone();
        swap(&mut self.val, &mut res);
        res
    }

    fn eval_owned(&mut self, mut i: T) -> T {
        swap(&mut self.val, &mut i);
        i
    }
}

impl<T> StrictOperator<T> for Z1<T>
where
    T: Clone + 'static,
{
    fn get_output(&mut self) -> T {
        let mut old_val = self.zero.clone();
        swap(&mut self.val, &mut old_val);
        old_val
    }
}

impl<T> StrictUnaryOperator<T, T> for Z1<T>
where
    T: Clone + 'static,
{
    fn eval_strict(&mut self, i: &T) {
        self.val = i.clone();
    }

    fn eval_strict_owned(&mut self, i: T) {
        self.val = i;
    }
}

#[test]
fn sum_circuit() {
    let mut z1 = Z1::new(0);

    let expected_result = vec![0, 1, 2, 0, 4, 5];

    // Test `UnaryOperator` API.
    let mut res = Vec::new();
    z1.stream_start();
    res.push(z1.eval(&1));
    res.push(z1.eval(&2));
    res.push(z1.eval(&3));
    z1.stream_end();

    z1.stream_start();
    res.push(z1.eval_owned(4));
    res.push(z1.eval_owned(5));
    res.push(z1.eval_owned(6));
    z1.stream_end();

    assert_eq!(res, expected_result);

    // Test `StrictUnaryOperator` API.
    let mut res = Vec::new();
    z1.stream_start();
    res.push(z1.get_output());
    z1.eval_strict(&1);
    res.push(z1.get_output());
    z1.eval_strict(&2);
    res.push(z1.get_output());
    z1.eval_strict(&3);
    z1.stream_end();

    z1.stream_start();
    res.push(z1.get_output());
    z1.eval_strict_owned(4);
    res.push(z1.get_output());
    z1.eval_strict_owned(5);
    res.push(z1.get_output());
    z1.eval_strict_owned(6);
    z1.stream_end();

    assert_eq!(res, expected_result);
}
