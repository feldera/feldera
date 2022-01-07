//! Common traits for DBSP operators.
//!
//! Operators are the building blocks of DBSP circuits.  An operator
//! consumes one or more input streams and produces an output stream.

/// Minimal requirements for values exchanged by operators.
pub trait Data: Clone + 'static {}

impl<T: Clone + 'static> Data for T {}

/// Trait that must be implemented by all operators regardless of their arity.
/// Methods in this trait are necessary to support operators over nested streams.
/// A nested stream is a stream whose elements are streams.  Since streams are
/// generated one value at a time, we need a separate operator invocation for each
/// element of the inner-most stream.  The `stream_start` and `stream_end` methods
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
/// stream_start() // Start outer stream.
/// stream_start() // Start nested stream (first row of the matrix).
/// eval(1)
/// eval(2)
/// stream_end()   // End nested stream.
/// stream_start() // Start nested stream (second row).
/// eval(3)
/// eval(4)
/// eval(5)
/// eval(6)
/// stream_end()   // End nested stream.
/// stream_start() // Start nested strea (second row).
/// eval(7)
/// eval(8)
/// eval(9)
/// stream_end()   // End nested stream.
/// stream_end()   // End outer stream.
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
    fn stream_start(&mut self);
    fn stream_end(&mut self);

    /// With the exception of [`SourceOperator`]s, all operators consume one or
    /// more input streams.  By default, operators consume values by reference;
    /// however some operators have alternative implementations optimized to work
    /// with owned values, available via operator's `eval_owned` method.  This
    /// method tells the scheduler that the operator has an optimized `eval_owned`
    /// implementation (the default implementation simply delegates to the `eval`
    /// method) and prefers to consume inputs by value.  This request is not
    /// guaranteed, but may be taken into account by the scheduler.
    fn prefer_owned_input(&self) -> bool {
        false
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
    fn eval(&mut self, i: &I);

    /// Consume input by value.
    fn eval_owned(&mut self, i: I) {
        self.eval(&i);
    }
}

/// A unary operator that consumes a stream of inputs of type `I`
/// and produces a stream of outputs of type `O`.
pub trait UnaryOperator<I, O>: Operator {
    /// Consume input by reference.
    fn eval(&mut self, i: &I) -> O;

    /// Consume input by value.
    fn eval_owned(&mut self, i: I) -> O {
        self.eval(&i)
    }
}

/// A binary operator consumes two input streams carrying values
/// of types `I1` and `I2` and produces a stream of outputs of type `O`.
pub trait BinaryOperator<I1, I2, O>: Operator {
    /// Consume input by reference.
    fn eval(&mut self, i1: &I1, i2: &I2) -> O;

    /// Consume input by value.
    fn eval_owned(&mut self, i1: I1, i2: I2) -> O {
        self.eval(&i1, &i2)
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
    /// (for example [Z1](`crate::circuit::operator::Z1`) returns its inner value, leaving
    /// the operator empty).
    fn get_output(&mut self) -> O;
}

/// A strict unary operator that consumes a stream of inputs of type `I`
/// by reference and produces a stream of outputs of type `O`.
pub trait StrictUnaryOperator<I, O>: StrictOperator<O> {
    /// Feed input for the current timestamp to the operator by reference.  The output
    /// will be consumed via [`get_output`](`StrictOperator::get_output`) during the
    /// next timestamp.
    fn eval_strict(&mut self, i: &I);

    /// Feed input for the current timestamp to the operator by value.  The output
    /// will be consumed via [`get_output`](`StrictOperator::get_output`) during the
    /// next timestamp.
    fn eval_strict_owned(&mut self, i: I);
}
