//! Common traits for DBSP operators.
//!
//! Operators are the building blocks of DBSP circuits.  An operator
//! consumes one or more input streams and produces an output stream.

use crate::circuit::OwnershipPreference;
use std::borrow::Cow;

/// Minimal requirements for values exchanged by operators.
pub trait Data: Clone + 'static {}

impl<T: Clone + 'static> Data for T {}

/// Trait that must be implemented by all operators regardless of their arity.
/// Methods in this trait are necessary to support operators over nested streams.
/// A nested stream is a stream whose elements are streams.  Since streams are
/// generated one value at a time, we need a separate operator invocation for each
/// element of the inner-most stream.  The `clock_start` and `clock_end` methods
/// signals respectively the start and completion of a nested stream.
///
/// For example, feeding the following matrix, where rows represent nested
/// streams
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
/// clock_start() // Start outer stream.
/// clock_start() // Start nested stream (first row of the matrix).
/// eval(1)
/// eval(2)
/// clock_end()   // End nested stream.
/// clock_start() // Start nested stream (second row).
/// eval(3)
/// eval(4)
/// eval(5)
/// eval(6)
/// clock_end()   // End nested stream.
/// clock_start() // Start nested strea (second row).
/// eval(7)
/// eval(8)
/// eval(9)
/// clock_end()   // End nested stream.
/// clock_end()   // End outer stream.
/// ```
///
/// Note that the input and output of an operator always belong to the same clock
/// domain, i.e., an operator cannot consume a single value and produce a stream,
/// or the other way around.  Nested clock domains are implemented by special
/// operators that wrap a nested circuit and, for each input value, run the nested
/// circuit to a fixed point (or some other termination condition).
///
/// An operator can have multiple input streams, all of which also belong to the
/// same clock domain and therefore start and end at the same time.  Hence
/// `start_stream` and `end_stream` apply to all input and output streams of the
/// operator.
pub trait Operator: 'static {
    fn name(&self) -> Cow<'static, str>;

    fn clock_start(&mut self);
    fn clock_end(&mut self);

    /// Returns `true` if `self` is an asynchronous operator.
    ///
    /// An asynchronous operator may need to wait for external inputs, i.e., inputs
    /// from outside the circuit.  While a regular synchronous operator is ready to
    /// be triggered as soon as all of its input streams contain data, an async
    /// operator may require additional inputs that arrive asynchronously with
    /// respect to the operation of the circuit (e.g., from an I/O device or via
    /// an IPC channel).
    ///
    /// We do not allow operators to block, therefore the scheduler must not schedule
    /// an async operator until it has all external inputs available.  The scheduler
    /// checks that the operator is ready to execute using the [`ready`](`Self::ready`) method.
    fn is_async(&self) -> bool {
        false
    }

    /// Returns `true` if `self` has received all required external inputs and is
    /// ready to run.
    ///
    /// This method must always returns `true` for synchronous operators.  For an
    /// asynchronous operator, it returns `true` if the operator has all external
    /// inputs available (see [`is_async`](`Self::is_async`) documentation).  Once the operator is
    /// ready, it remains ready within the current clock cycle, thus the scheduler
    /// can safely evaluate the operator.
    fn ready(&self) -> bool {
        true
    }

    /// Register callback to be invoked when an asynchronous operator becomes ready.
    ///
    /// This method should only be used for asynchronous operators (see documentation
    /// for [`is_async`](`Self::is_async`) and [`ready`](`Self::ready`)) in order to
    /// enable dynamic schedulers to run async operators as they become ready without
    /// continuously polling them.  It can be invoked at most once per operator;
    /// invoking it more than once is undefined behavior.
    ///
    /// Once the callback has been registered, the operator will invoke the callback
    /// at every clock cycle, when the operator becomes ready.   The callback in
    /// invoked with at-least-once semantics, meaning that spurious invocations are
    /// possible.  The scheduler must always check if the operator is ready ro run
    /// by calling [`ready`](`Self::ready`) and must be prepared to wait if it
    /// returns `false`.
    fn register_ready_callback<F>(&mut self, _cb: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
    }
}

/// A source operator that injects data from the outside world or from the parent
/// circuit into the local circuit.  Consumes no input streams and emits a single
/// output stream.
pub trait SourceOperator<O>: Operator {
    /// Yield the next value.
    fn eval(&mut self) -> O;
}

/// A sink operator consumes an input stream, but does not produce an output
/// stream.  Such operators are used to send results of the computation performed
/// by the circuit to the outside world.
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

/// The output of a strict operator only depends on inputs from previous timestamps and
/// hence can be produced before consuming new inputs.  This way a strict operator can
/// be used as part of a feedback loop where its output is needed before input for the
/// current timestamp is available.
pub trait StrictOperator<O>: Operator {
    /// Returns the output value computed based on data consumed by the operator during
    /// previous timestamps.  This method is invoked **before** `eval_strict()` has been invoked
    /// for the current timestamp.  It can be invoked **at most once** for each timestamp,
    /// as the implementation may mutate or destroy the operator's internal state
    /// (for example [Z1](`crate::operator::Z1`) returns its inner value, leaving
    /// the operator empty).
    fn get_output(&mut self) -> O;
}

/// A strict unary operator that consumes a stream of inputs of type `I`
/// by reference and produces a stream of outputs of type `O`.
pub trait StrictUnaryOperator<I, O>: StrictOperator<O> {
    /// Feed input for the current timestamp to the operator by reference.  The output
    /// will be consumed via [`get_output`](`StrictOperator::get_output`) during the
    /// next timestamp.
    fn eval_strict(&mut self, input: &I);

    /// Feed input for the current timestamp to the operator by value.  The output
    /// will be consumed via [`get_output`](`StrictOperator::get_output`) during the
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
