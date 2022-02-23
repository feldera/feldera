//! z^-1 operator delays its input by one timestamp.

use crate::circuit::{
    operator_traits::{Operator, StrictOperator, StrictUnaryOperator, UnaryOperator},
    OwnershipPreference, Scope,
};
use std::{borrow::Cow, mem::swap};

/// z^-1 operator delays its input by one timestamp.
///
/// The operator outputs a user-defined "zero" value in the first timestamp
/// after [clock_start](`Z1::clock_start`).  For all subsequent timestamps, it
/// outputs the value received as input at the previous timestamp.  The zero
/// value is typically the neutral element of a monoid (e.g., 0 for addition
/// or 1 for multiplication).
///
/// It is a strict operator.
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
    fn name(&self) -> Cow<'static, str> {
        Cow::from("Z^-1")
    }

    fn clock_start(&mut self, _scope: Scope) {}
    fn clock_end(&mut self, _scope: Scope) {
        self.reset();
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

    fn input_preference(&self) -> OwnershipPreference {
        OwnershipPreference::PREFER_OWNED
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

    fn input_preference(&self) -> OwnershipPreference {
        OwnershipPreference::PREFER_OWNED
    }
}

/// z^-1 operator over streams of streams.
///
/// The operator stores a complete nested stream consumed at the last iteration
/// of the parent clock and outputs it at the next parent clock cycle
/// It outputs a stream of zeros value in the first parent clock tick.
///
/// It is a strict operator.
///
/// # Examples
///
/// Input (one row per parent timestamps):
///
/// ```text
/// 1 2 3 4
/// 1 1 1 1 1
/// 2 2 2 0 0
/// ```
///
/// Output:
///
/// ```text
/// 0 0 0 0
/// 1 2 3 4 0
/// 1 1 1 1 1
/// ```
pub struct Z1Nested<T> {
    zero: T,
    timestamp: usize,
    val: Vec<T>,
}

impl<T> Z1Nested<T>
where
    T: Clone,
{
    pub fn new(zero: T) -> Self {
        Self {
            zero,
            timestamp: 0,
            val: vec![],
        }
    }

    fn reset(&mut self) {
        self.timestamp = 0;
        self.val = vec![];
    }
}

impl<T> Operator for Z1Nested<T>
where
    T: Clone + 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("Z^-1 (nested)")
    }

    fn clock_start(&mut self, scope: Scope) {
        if scope == 0 {
            self.val.truncate(self.timestamp);
        }
        self.timestamp = 0;
    }

    fn clock_end(&mut self, scope: Scope) {
        if scope > 0 {
            self.reset();
        }
    }
}

impl<T> StrictOperator<T> for Z1Nested<T>
where
    T: Clone + 'static,
{
    fn get_output(&mut self) -> T {
        if self.timestamp >= self.val.len() {
            assert_eq!(self.timestamp, self.val.len());
            self.val.push(self.zero.clone());
        }

        let mut zero = self.zero.clone();
        swap(
            // Safe due to the check above.
            unsafe { self.val.get_unchecked_mut(self.timestamp) },
            &mut zero,
        );
        zero
    }
}

impl<T> StrictUnaryOperator<T, T> for Z1Nested<T>
where
    T: Clone + 'static,
{
    fn eval_strict(&mut self, i: &T) {
        assert!(self.timestamp < self.val.len());

        self.val[self.timestamp] = i.clone();
        self.timestamp += 1;
    }

    fn eval_strict_owned(&mut self, i: T) {
        assert!(self.timestamp < self.val.len());

        self.val[self.timestamp] = i;
        self.timestamp += 1;
    }

    fn input_preference(&self) -> OwnershipPreference {
        OwnershipPreference::PREFER_OWNED
    }
}

#[cfg(test)]
mod test {
    use crate::{
        circuit::operator_traits::{Operator, StrictOperator, StrictUnaryOperator, UnaryOperator},
        operator::{Z1Nested, Z1},
    };

    #[test]
    fn z1_test() {
        let mut z1 = Z1::new(0);

        let expected_result = vec![0, 1, 2, 0, 4, 5];

        // Test `UnaryOperator` API.
        let mut res = Vec::new();
        z1.clock_start(0);
        res.push(z1.eval(&1));
        res.push(z1.eval(&2));
        res.push(z1.eval(&3));
        z1.clock_end(0);

        z1.clock_start(0);
        res.push(z1.eval_owned(4));
        res.push(z1.eval_owned(5));
        res.push(z1.eval_owned(6));
        z1.clock_end(0);

        assert_eq!(res, expected_result);

        // Test `StrictUnaryOperator` API.
        let mut res = Vec::new();
        z1.clock_start(0);
        res.push(z1.get_output());
        z1.eval_strict(&1);
        res.push(z1.get_output());
        z1.eval_strict(&2);
        res.push(z1.get_output());
        z1.eval_strict(&3);
        z1.clock_end(0);

        z1.clock_start(0);
        res.push(z1.get_output());
        z1.eval_strict_owned(4);
        res.push(z1.get_output());
        z1.eval_strict_owned(5);
        res.push(z1.get_output());
        z1.eval_strict_owned(6);
        z1.clock_end(0);

        assert_eq!(res, expected_result);
    }

    #[test]
    fn z1_nested_test() {
        let mut z1 = Z1Nested::new(0);

        // Test `StrictUnaryOperator` API.
        let mut res = Vec::new();
        z1.clock_start(0);
        res.push(z1.get_output());
        z1.eval_strict(&1);
        res.push(z1.get_output());
        z1.eval_strict(&2);
        res.push(z1.get_output());
        z1.eval_strict(&3);
        z1.clock_end(0);
        assert_eq!(res.as_slice(), &[0, 0, 0]);

        let mut res = Vec::new();

        z1.clock_start(0);
        res.push(z1.get_output());
        z1.eval_strict_owned(4);
        res.push(z1.get_output());
        z1.eval_strict_owned(5);
        z1.clock_end(0);

        assert_eq!(res.as_slice(), &[1, 2]);

        let mut res = Vec::new();

        z1.clock_start(0);
        res.push(z1.get_output());
        z1.eval_strict_owned(6);
        res.push(z1.get_output());
        z1.eval_strict_owned(7);
        res.push(z1.get_output());
        z1.eval_strict_owned(8);
        res.push(z1.get_output());
        z1.eval_strict_owned(9);
        z1.clock_end(0);

        assert_eq!(res.as_slice(), &[4, 5, 0, 0]);
    }
}
