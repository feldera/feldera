//! Common traits for DBSP operators.
//!
//! Operators are the building blocks of DBSP circuits.  An operator
//! consumes one or more input streams and produces an output stream.

use crate::circuit::{
    metadata::{OperatorLocation, OperatorMeta},
    OwnershipPreference, Scope,
};
use crate::Error;
use std::borrow::Cow;

/// Minimal requirements for values exchanged by operators.
pub trait Data: Clone + 'static {}

impl<T: Clone + 'static> Data for T {}

/// Trait that must be implemented by all operators.
pub trait Operator: 'static {
    /// Human-readable operator name for debugging purposes.
    fn name(&self) -> Cow<'static, str>;

    /// The location the operator was created at
    fn location(&self) -> OperatorLocation {
        None
    }

    /// Collects metadata about the current operator
    fn metadata(&self, _meta: &mut OperatorMeta) {}

    /// Notify the operator about the start of a new clock epoch.
    ///
    /// `clock_start` and `clock_end` methods support the nested circuit
    /// architecture.  A nested circuit (or subcircuit) is a node in
    /// the parent circuit that contains another circuit.  The nested circuit
    /// has its own clock.  Each parent clock tick starts a new child clock
    /// epoch.  Each operator gets notified about start and end of a clock
    /// epoch in its local circuit and all of its ancestors.
    ///
    /// Formally, operators in a nested circuit operate over nested streams,
    /// or streams of streams, with each nested clock epoch starting a new
    /// stream.  Thus the `clock_start` and `clock_end` methods signal
    /// respectively the start and completion of a nested stream.
    ///
    /// # Examples
    ///
    /// For example, feeding the following matrix, where rows represent nested
    /// streams,
    ///
    /// ```text
    /// ┌       ┐
    /// │1 2    │
    /// │3 4 5 6│
    /// │7 8 9  |
    /// └       ┘
    /// ```
    ///
    /// to an operator requires the following sequence of invocations
    ///
    /// ```text
    /// clock_start(1) // Start outer clock.
    /// clock_start(0) // Start nested clock (first row of the matrix).
    /// eval(1)
    /// eval(2)
    /// clock_end(0)   // End nested clock.
    /// clock_start(0) // Start nested clock (second row).
    /// eval(3)
    /// eval(4)
    /// eval(5)
    /// eval(6)
    /// clock_end(0)   // End nested clock.
    /// clock_start(0) // Start nested clock (third row).
    /// eval(7)
    /// eval(8)
    /// eval(9)
    /// clock_end(0)   // End nested clock.
    /// clock_end(1)   // End outer clock.
    /// ```
    ///
    /// Note that the input and output of most operators belong to the same
    /// clock domain, i.e., an operator cannot consume a single value and
    /// produce a stream, or the other way around.  The only exception are
    /// [`ImportOperator`]s that make the contents of a stream in the parent
    /// circuit available inside a subcircuit.
    ///
    /// An operator can have multiple input streams, all of which belong to the
    /// same clock domain and therefore start and end at the same time.  Hence
    /// `clock_start` and `clock_end` apply to all input and output streams of
    /// the operator.
    ///
    /// # Arguments
    ///
    /// * `scope` - the scope whose clock is restarting.
    fn clock_start(&mut self, _scope: Scope) {}
    fn clock_end(&mut self, _scope: Scope) {}

    /// Returns `true` if `self` is an asynchronous operator.
    ///
    /// An asynchronous operator may need to wait for external inputs, i.e.,
    /// inputs from outside the circuit.  While a regular synchronous
    /// operator is ready to be triggered as soon as all of its input
    /// streams contain data, an async operator may require additional
    /// inputs that arrive asynchronously with respect to the operation of
    /// the circuit (e.g., from an I/O device or via an IPC channel).
    ///
    /// We do not allow operators to block, therefore the scheduler must not
    /// schedule an async operator until it has all external inputs
    /// available.  The scheduler checks that the operator is ready to
    /// execute using the [`ready`](`Self::ready`) method.
    fn is_async(&self) -> bool {
        false
    }

    /// Returns `true` if `self` has received all required external inputs and
    /// is ready to run.
    ///
    /// This method must always returns `true` for synchronous operators.  For
    /// an asynchronous operator, it returns `true` if the operator has all
    /// external inputs available (see [`is_async`](`Self::is_async`)
    /// documentation).  Once the operator is ready, it remains ready within
    /// the current clock cycle, thus the scheduler can safely evaluate the
    /// operator.
    fn ready(&self) -> bool {
        true
    }

    /// Register callback to be invoked when an asynchronous operator becomes
    /// ready.
    ///
    /// This method should only be used for asynchronous operators (see
    /// documentation for [`is_async`](`Self::is_async`) and
    /// [`ready`](`Self::ready`)) in order to enable dynamic schedulers to
    /// run async operators as they become ready without continuously
    /// polling them.  The operator need only support this method being
    /// called once, to set a single callback.
    ///
    /// Once the callback has been registered, the operator will invoke the
    /// callback at every clock cycle, when the operator becomes ready.
    /// The callback is invoked with at-least-once semantics, meaning that
    /// spurious invocations are possible.  The scheduler must always check
    /// if the operator is ready to run by calling [`ready`](`Self::ready`)
    /// and must be prepared to wait if it returns `false`.
    fn register_ready_callback<F>(&mut self, _cb: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
    }

    /// Check if the operator is in a stable state.
    ///
    /// This method is invoked as part of checking if the circuit has reached a
    /// fixed point state, i.e., a state where the outputs of all operators will
    /// remain constant until the end of the current clock epoch
    /// (see [`Circuit::fixedpoint`](`crate::circuit::Circuit::fixedpoint`)).
    ///
    /// It returns `true` if the operator's output is guaranteed to remain
    /// constant (i.e., all future outputs will be equal to the last output) as
    /// long as its inputs remain constant.
    ///
    /// The exact semantics depends on the value of the `scope` argument, which
    /// identifies the circuit whose fixed point state is being checked.
    /// Scope 0 is the local circuit.  The method is invoked with `scope=0`
    /// at the end of a clock cycle, and should return `true` if, assuming that
    /// it will see inputs identical to the last input during all future clock
    /// cycles in the current clock epoch, it will keep producing the same
    /// outputs.
    ///
    /// Scope 1 represents the parent of the local circuit.  The method is
    /// invoked with `scope=1` at the end of a clock _epoch_, and should
    /// return `true` if, assuming that it will see a sequence of inputs
    /// (aka the input stream) identical to the last epoch during all future
    /// epochs, it will keep producing the same output streams.
    ///
    /// Scope 2 represents the grandparent of the local circuit.  The method is
    /// invoked with `scope=2` at the end of the parent clock _epoch_, and
    /// checks that the operator's output will remain stable wrt to the
    /// nested input stream (i.e., stream of streams).
    ///
    /// And so on.
    ///
    /// The check must be precise. False positives (returning `true` when the
    /// output may change in the future) may lead to early termination before
    /// the circuit has reached a fixed point (and hence incorrect output).
    /// False negatives (returning `false` in a stable state) is only acceptable
    /// for a finite number of clock cycles and will otherwise prevent the
    /// fixedpoint computation from converging.
    ///
    /// # Warning
    ///
    /// Two operators currently violate this requirement:
    /// [`Z1`](`crate::operator::Z1`) and
    /// [`Z1Nested`](`crate::operator::Z1Nested`). The latter will get phased
    /// out soon.  The former is work-in-progress. It can be safely used inside
    /// nested circuits when carrying changes to collections across iterations
    /// of the fixed point computation, but not as part of an integrator circuit
    /// ([`Stream::integrate`](`crate::circuit::Stream::integrate`)).
    fn fixedpoint(&self, scope: Scope) -> bool;

    /// Instructors operator to checkpoint its state to persistent storage.
    ///
    /// In most cases (except for traces) this method is a no-op.
    fn commit(&self, _cid: u64) -> Result<(), Error> {
        Ok(())
    }
}

/// A source operator that injects data from the outside world or from the
/// parent circuit into the local circuit.  Consumes no input streams and emits
/// a single output stream.
pub trait SourceOperator<O>: Operator {
    /// Yield the next value.
    fn eval(&mut self) -> O;
}

/// A sink operator consumes an input stream, but does not produce an output
/// stream.  Such operators are used to send results of the computation
/// performed by the circuit to the outside world.
pub trait SinkOperator<I>: Operator {
    /// Consume input by reference.
    fn eval(&mut self, input: &I);

    /// Consume input by value.
    fn eval_owned(&mut self, input: I) {
        self.eval(&input);
    }

    /// Ownership preference on the operator's input stream
    /// (see [`OwnershipPreference`]).
    fn input_preference(&self) -> OwnershipPreference {
        OwnershipPreference::INDIFFERENT
    }
}

/// A sink operator that consumes two input streams, but does not produce
/// an output stream.  Such operators are used to send results of the
/// computation performed by the circuit to the outside world.
pub trait BinarySinkOperator<I1, I2>: Operator
where
    I1: Clone,
    I2: Clone,
{
    /// Consume inputs.
    ///
    /// The operator must be prepated to handle any combination of
    /// owned and borrowed inputs.
    fn eval<'a>(&mut self, lhs: Cow<'a, I1>, rhs: Cow<'a, I2>);

    /// Ownership preference on the operator's input streams
    /// (see [`OwnershipPreference`]).
    fn input_preference(&self) -> (OwnershipPreference, OwnershipPreference) {
        (
            OwnershipPreference::INDIFFERENT,
            OwnershipPreference::INDIFFERENT,
        )
    }
}

/// A unary operator that consumes a stream of inputs of type `I`
/// and produces a stream of outputs of type `O`.
pub trait UnaryOperator<I, O>: Operator {
    /// Consume input by reference.
    fn eval(&mut self, input: &I) -> O;

    /// Consume input by value.
    fn eval_owned(&mut self, input: I) -> O {
        self.eval(&input)
    }

    /// Ownership preference on the operator's input stream
    /// (see [`OwnershipPreference`]).
    fn input_preference(&self) -> OwnershipPreference {
        OwnershipPreference::INDIFFERENT
    }
}

/// A binary operator consumes two input streams carrying values
/// of types `I1` and `I2` and produces a stream of outputs of type `O`.
pub trait BinaryOperator<I1, I2, O>: Operator {
    /// Consume input by reference.
    fn eval(&mut self, lhs: &I1, rhs: &I2) -> O;

    /// Consume input by value.
    fn eval_owned(&mut self, lhs: I1, rhs: I2) -> O {
        self.eval(&lhs, &rhs)
    }

    /// Consume the first input by value and the second by reference.
    fn eval_owned_and_ref(&mut self, lhs: I1, rhs: &I2) -> O {
        self.eval(&lhs, rhs)
    }

    /// Consume the first input by reference and the second by value.
    fn eval_ref_and_owned(&mut self, lhs: &I1, rhs: I2) -> O {
        self.eval(lhs, &rhs)
    }

    /// Ownership preference on the operator's input streams
    /// (see [`OwnershipPreference`]).
    fn input_preference(&self) -> (OwnershipPreference, OwnershipPreference) {
        (
            OwnershipPreference::INDIFFERENT,
            OwnershipPreference::INDIFFERENT,
        )
    }
}

/// A ternary operator consumes three input streams carrying values
/// of types `I1`, `I2`, and `I3` and produces a stream of outputs of type `O`.
pub trait TernaryOperator<I1, I2, I3, O>: Operator
where
    I1: Clone,
    I2: Clone,
    I3: Clone,
{
    /// Consume inputs.
    ///
    /// The operator must be prepated to handle any combination of
    /// owned and borrowed inputs.
    fn eval<'a>(&mut self, i1: Cow<'a, I1>, i2: Cow<'a, I2>, i3: Cow<'a, I3>) -> O;

    fn input_preference(
        &self,
    ) -> (
        OwnershipPreference,
        OwnershipPreference,
        OwnershipPreference,
    ) {
        (
            OwnershipPreference::INDIFFERENT,
            OwnershipPreference::INDIFFERENT,
            OwnershipPreference::INDIFFERENT,
        )
    }
}

/// A quaternary operator consumes four input streams carrying values
/// of types `I1`, `I2`, `I3`, and `I4` and produces a stream of outputs of type
/// `O`.
pub trait QuaternaryOperator<I1, I2, I3, I4, O>: Operator
where
    I1: Clone,
    I2: Clone,
    I3: Clone,
    I4: Clone,
{
    /// Consume inputs.
    ///
    /// The operator must be prepated to handle any combination of
    /// owned and borrowed inputs.
    fn eval<'a>(&mut self, i1: Cow<'a, I1>, i2: Cow<'a, I2>, i3: Cow<'a, I3>, i4: Cow<'a, I4>)
        -> O;

    fn input_preference(
        &self,
    ) -> (
        OwnershipPreference,
        OwnershipPreference,
        OwnershipPreference,
        OwnershipPreference,
    ) {
        (
            OwnershipPreference::INDIFFERENT,
            OwnershipPreference::INDIFFERENT,
            OwnershipPreference::INDIFFERENT,
            OwnershipPreference::INDIFFERENT,
        )
    }
}

/// An operator that consumes any number of streams carrying values
/// of type `I` and produces a stream of outputs of type `O`.
pub trait NaryOperator<I, O>: Operator
where
    I: Clone + 'static,
{
    /// Consume inputs.
    ///
    /// The operator must be prepated to handle any combination of
    /// owned and borrowed inputs.
    fn eval<'a, Iter>(&'a mut self, inputs: Iter) -> O
    where
        Iter: Iterator<Item = Cow<'a, I>>;

    /// Ownership preference on the operator's input streams
    /// (see [`OwnershipPreference`]).
    fn input_preference(&self) -> OwnershipPreference {
        OwnershipPreference::INDIFFERENT
    }
}

/// A "strict operator" is one whose output only depends on inputs from previous
/// timestamps and hence can be produced before consuming new inputs.  This way
/// a strict operator can be used as part of a feedback loop where its output is
/// needed before input for the current timestamp is available.
///
/// The only strict operators that DBSP makes available are [Z1] and its variant
/// [Z1Nested].
///
/// [Z1]: crate::operator::Z1
/// [Z1Nested]: crate::operator::Z1Nested
/// [Z1Trace]: crate::operator::trace::Z1Trace
pub trait StrictOperator<O>: Operator {
    /// Returns the output value computed based on data consumed by the operator
    /// during previous timestamps.  This method is invoked **before**
    /// `eval_strict()` has been invoked for the current timestamp.  It can
    /// be invoked **at most once** for each timestamp,
    /// as the implementation may mutate or destroy the operator's internal
    /// state (for example [Z1](`crate::operator::Z1`) returns its inner
    /// value, leaving the operator empty).
    fn get_output(&mut self) -> O;

    fn get_final_output(&mut self) -> O;
}

/// A strict unary operator that consumes a stream of inputs of type `I`
/// by reference and produces a stream of outputs of type `O`.
pub trait StrictUnaryOperator<I, O>: StrictOperator<O> {
    /// Feed input for the current timestamp to the operator by reference.  The
    /// output will be consumed via
    /// [`get_output`](`StrictOperator::get_output`) during the
    /// next timestamp.
    fn eval_strict(&mut self, input: &I);

    /// Feed input for the current timestamp to the operator by value.  The
    /// output will be consumed via
    /// [`get_output`](`StrictOperator::get_output`) during the
    /// next timestamp.
    fn eval_strict_owned(&mut self, input: I) {
        self.eval_strict(&input);
    }

    /// Ownership preference on the operator's input stream
    /// (see [`OwnershipPreference`]).
    fn input_preference(&self) -> OwnershipPreference {
        OwnershipPreference::INDIFFERENT
    }
}

/// An import operator makes a stream from the parent circuit
/// available inside a subcircuit.
///
/// Import operators are the only kind of operator that span
/// two clock domains: an import operator reads a single
/// value from the parent stream per parent clock tick and produces
/// a stream of outputs in the nested circuit, one for each nested
/// clock tick.
///
/// See [`Delta0`](`crate::operator::Delta0`) for a concrete example
/// of an import operator.
pub trait ImportOperator<I, O>: Operator {
    /// Consumes a value from the parent stream by reference.
    ///
    /// Either `import` or [`Self::import_owned`] is invoked once per
    /// nested clock epoch, right after `clock_start(0)`.
    fn import(&mut self, val: &I);

    /// Consumes a value from the parent stream by value.
    fn import_owned(&mut self, val: I);

    /// Invoked once per nested clock cycle to write a value to
    /// the output stream.
    fn eval(&mut self) -> O;

    /// Ownership preference on the operator's input stream
    /// (see [`OwnershipPreference`]).
    fn input_preference(&self) -> OwnershipPreference {
        OwnershipPreference::INDIFFERENT
    }
}
