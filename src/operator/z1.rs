//! z^-1 operator delays its input by one timestamp.

use crate::{
    algebra::HasZero,
    circuit::{
        operator_traits::{Operator, StrictOperator, StrictUnaryOperator, UnaryOperator},
        Circuit, ExportId, ExportStream, FeedbackConnector, NodeId, OwnershipPreference, Scope,
        Stream,
    },
    circuit_cache_key,
};
use std::{borrow::Cow, mem::replace};

circuit_cache_key!(DelayedId<C, D>(NodeId => Stream<C, D>));
circuit_cache_key!(NestedDelayedId<C, D>(NodeId => Stream<C, D>));

/// Like [`FeedbackConnector`] but specialized for [`Z1`] feedback operator.
///
/// Use this API instead of the low-level [`Circuit::add_feedback`] API to
/// create feedback loops with `Z1` operator.  In addition to being more
/// concise, this API takes advantage of [caching](`crate::circuit::cache`).
pub struct DelayedFeedback<P, D> {
    feedback: FeedbackConnector<Circuit<P>, D, D, Z1<D>>,
    output: Stream<Circuit<P>, D>,
    export: Stream<P, D>,
}

impl<P, D> DelayedFeedback<P, D>
where
    P: Clone + 'static,
    D: Clone + HasZero + 'static,
{
    /// Create a feedback loop with `Z1` operator.  Use [`Self::connect`] to
    /// close the loop.
    pub fn new(circuit: &Circuit<P>) -> Self {
        let (ExportStream { local, export }, feedback) =
            circuit.add_feedback_with_export(Z1::new(D::zero()));

        Self {
            feedback,
            output: local,
            export,
        }
    }

    /// Output stream of the `Z1` operator.
    pub fn stream(&self) -> &Stream<Circuit<P>, D> {
        &self.output
    }

    /// Connect `input` stream to the input of the `Z1` operator.
    pub fn connect(self, input: &Stream<Circuit<P>, D>) {
        let Self {
            feedback,
            output,
            export,
        } = self;
        let circuit = output.circuit().clone();

        feedback.connect_with_preference(input, OwnershipPreference::STRONGLY_PREFER_OWNED);
        circuit
            .cache()
            .insert(DelayedId::new(input.local_node_id()), output);
        circuit
            .cache()
            .insert(ExportId::new(input.local_node_id()), export);
    }
}

/// Like [`FeedbackConnector`] but specialized for [`Z1Nested`] feedback
/// operator.
///
/// Use this API instead of the low-level [`Circuit::add_feedback`] API to
/// create feedback loops with `Z1Nested` operator.  In addition to being more
/// concise, this API takes advantage of [caching](`crate::circuit::cache`).
pub struct DelayedNestedFeedback<P, D> {
    feedback: FeedbackConnector<Circuit<P>, D, D, Z1Nested<D>>,
    output: Stream<Circuit<P>, D>,
}

impl<P, D> DelayedNestedFeedback<P, D>
where
    P: Clone + 'static,
    D: Clone + 'static,
{
    /// Create a feedback loop with `Z1` operator.  Use [`Self::connect`] to
    /// close the loop.
    pub fn new(circuit: &Circuit<P>) -> Self
    where
        D: HasZero,
    {
        let (output, feedback) = circuit.add_feedback(Z1Nested::new(D::zero()));
        Self { feedback, output }
    }

    /// Output stream of the `Z1Nested` operator.
    pub fn stream(&self) -> &Stream<Circuit<P>, D> {
        &self.output
    }

    /// Connect `input` stream to the input of the `Z1Nested` operator.
    pub fn connect(self, input: &Stream<Circuit<P>, D>) {
        let Self { feedback, output } = self;
        let circuit = output.circuit().clone();

        feedback.connect_with_preference(input, OwnershipPreference::STRONGLY_PREFER_OWNED);
        circuit
            .cache()
            .insert(NestedDelayedId::new(input.local_node_id()), output);
    }
}

impl<P, D> Stream<Circuit<P>, D> {
    /// Applies [`Z1`] operator to `self`.
    pub fn delay(&self) -> Stream<Circuit<P>, D>
    where
        P: Clone + 'static,
        D: Clone + HasZero + 'static,
    {
        if let Some(delayed) = self
            .circuit()
            .cache()
            .get(&DelayedId::new(self.local_node_id()))
        {
            return delayed.clone();
        }

        let delayed = self.circuit().add_unary_operator(Z1::new(D::zero()), self);
        self.circuit()
            .cache()
            .insert(DelayedId::new(self.local_node_id()), delayed.clone());

        delayed
    }

    /// Applies [`Z1Nested`] operator to `self`.
    pub fn delay_nested(&self) -> Stream<Circuit<P>, D>
    where
        P: Clone + 'static,
        D: Clone + HasZero + 'static,
    {
        if let Some(delayed) = self
            .circuit()
            .cache()
            .get(&NestedDelayedId::new(self.local_node_id()))
        {
            return delayed.clone();
        }

        let delayed = self
            .circuit()
            .add_unary_operator(Z1Nested::new(D::zero()), self);
        self.circuit()
            .cache()
            .insert(NestedDelayedId::new(self.local_node_id()), delayed.clone());

        delayed
    }
}

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
        replace(&mut self.val, i.clone())
    }

    fn eval_owned(&mut self, i: T) -> T {
        replace(&mut self.val, i)
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
        replace(&mut self.val, self.zero.clone())
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
/// of the parent clock and outputs it at the next parent clock cycle.
/// It outputs a stream of zeros value in the first parent clock tick.
///
/// One important subtlety is that mathematically speaking nested streams are
/// infinite, but we can only compute and store finite prefixes of such
/// streams.  When asked to produce output beyond the stored prefix, the z^-1
/// operator repeats the last value in the stored prefix.  The assumption
/// here is that the circuit was evaluated to a fixed point at the previous
/// parent timestamp, and hence the rest of the stream will contain the same
/// fixed point value.  This is not generally true for all circuits, and
/// users should keep this in mind when instantiating the operator.
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
/// 1 2 3 4 4
/// 1 1 1 1 1
/// ```
pub struct Z1Nested<T> {
    zero: T,
    timestamp: usize,
    val: Vec<T>,
}

impl<T> Z1Nested<T> {
    const fn new(zero: T) -> Self {
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

impl<T> UnaryOperator<T, T> for Z1Nested<T>
where
    T: Clone + 'static,
{
    fn eval(&mut self, i: &T) -> T {
        debug_assert!(self.timestamp <= self.val.len());

        if self.timestamp == self.val.len() {
            self.val.push(self.zero.clone());
        } else if self.timestamp == self.val.len() - 1 {
            self.val.push(self.val.last().unwrap().clone());
        }

        let result = replace(&mut self.val[self.timestamp], i.clone());

        self.timestamp += 1;
        result
    }

    fn eval_owned(&mut self, i: T) -> T {
        debug_assert!(self.timestamp <= self.val.len());

        if self.timestamp == self.val.len() {
            self.val.push(self.zero.clone());
        } else if self.timestamp == self.val.len() - 1 {
            self.val.push(self.val.last().unwrap().clone());
        }

        let result = replace(&mut self.val[self.timestamp], i);
        self.timestamp += 1;

        result
    }

    fn input_preference(&self) -> OwnershipPreference {
        OwnershipPreference::PREFER_OWNED
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
        } else if self.timestamp == self.val.len() - 1 {
            self.val.push(self.val.last().unwrap().clone())
        }

        replace(
            // Safe due to the check above.
            unsafe { self.val.get_unchecked_mut(self.timestamp) },
            self.zero.clone(),
        )
    }
}

impl<T> StrictUnaryOperator<T, T> for Z1Nested<T>
where
    T: Clone + 'static,
{
    fn eval_strict(&mut self, i: &T) {
        debug_assert!(self.timestamp < self.val.len());

        self.val[self.timestamp] = i.clone();
        self.timestamp += 1;
    }

    fn eval_strict_owned(&mut self, i: T) {
        debug_assert!(self.timestamp < self.val.len());

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

        // Test `UnaryOperator` API.

        z1.clock_start(1);

        let mut res = Vec::new();
        z1.clock_start(0);
        res.push(z1.eval_owned(1));
        res.push(z1.eval(&2));
        res.push(z1.eval(&3));
        z1.clock_end(0);
        assert_eq!(res.as_slice(), &[0, 0, 0]);

        let mut res = Vec::new();
        z1.clock_start(0);
        res.push(z1.eval_owned(4));
        res.push(z1.eval_owned(5));
        z1.clock_end(0);
        assert_eq!(res.as_slice(), &[1, 2]);

        let mut res = Vec::new();
        z1.clock_start(0);
        res.push(z1.eval_owned(6));
        res.push(z1.eval_owned(7));
        res.push(z1.eval(&8));
        res.push(z1.eval(&9));
        z1.clock_end(0);
        assert_eq!(res.as_slice(), &[4, 5, 5, 5]);

        z1.clock_end(1);

        // Test `StrictUnaryOperator` API.
        z1.clock_start(1);

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

        assert_eq!(res.as_slice(), &[4, 5, 5, 5]);

        z1.clock_end(1);
    }
}
