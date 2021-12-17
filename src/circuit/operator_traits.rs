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
/// ┌     ┐
/// │1 2 3│
/// │4 5 6│
/// │7 8 9│     
/// └     ┘
/// ```
///
/// to an operator requires the following sequence of invocations
///
/// ```text
/// stream_start() // Start outer stream.
/// stream_start() // Start nested stream (first row of the matrix).
/// eval(1)
/// eval(2)
/// eval(3)
/// stream_end()   // End nested stream.
/// stream_start() // Start nested stream (second row).
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
pub trait Operator: 'static {
    fn stream_start(&mut self);
    fn stream_end(&mut self);
}

/// A source operator that injects data from the outside world or from the parent
/// circuit into the local circuit.  Consumes no input streams and emits a single
/// output stream.
pub trait SourceOperator<O>: Operator {
    /// Yield the next value
    fn eval(&mut self) -> O;
}

/// A sink operator that consumes an input stream, but does not produce an output
/// stream.  Such operators are used to send results of the computation performed
/// by the circuit to the outside world.
pub trait SinkRefOperator<I>: Operator {
    fn eval(&mut self, i: &I);
}

/// A unary operator that consumes a stream of inputs of type `I`
/// by reference and produces a stream of outputs of type `O`.
pub trait UnaryRefOperator<I, O>: Operator {
    fn eval(&mut self, i: &I) -> O;
}

/// A unary operator that consumes a stream of inputs of type `I`
/// by value and produces a stream of outputs of type `O`.
pub trait UnaryValOperator<I, O>: Operator {
    fn eval(&mut self, i: I) -> O;
}

/// A binary operator that consumes two input streams carrying values
/// of types `I1` and `I2` by reference and produces a stream of
/// outputs of type `O`.
pub trait BinaryRefRefOperator<I1, I2, O>: Operator {
    fn eval(&mut self, i1: &I1, i2: &I2) -> O;
}
