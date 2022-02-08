//! Defines an operator that feeds data from the parent circuit to a nested
//! circuit.

use crate::circuit::{
    circuit_builder::{Circuit, Stream},
    operator_traits::{Data, Operator, SourceOperator},
};
use std::borrow::Cow;

/// An operator that feeds data from the parent circuit to a nested circuit with
/// a nested time domain (i.e., a circuit that performs multiple iterations for
/// each parent timestamp).  The operator consumes a stream from the parent
/// domain. On each parent clock tick it reads and stores internally an element
/// from the stream.  On each nested clock tick, it outputs the current stored
/// value and updates it using a user-provided transformer function.
pub struct NestedSource<P, T, F> {
    input_stream: Stream<Circuit<Circuit<P>>, T>,
    val: T,
    update: F,
}

impl<P, T, F> NestedSource<P, T, F>
where
    P: Clone + 'static,
    T: Default,
{
    /// Creates a new `NestedSource` operator and binds a parent stream to its
    /// input.
    ///
    /// `input_stream` - A stream from the parent circuit.  The operator
    /// consumes a value from this stream on each parent clock tick.
    ///
    /// `circuit` - Child circuit where the operator is instantiated.
    ///
    /// `update` - Transformer function applied to the stored value on each
    /// nested clock tick.
    pub fn new(
        input_stream: Stream<Circuit<P>, T>,
        circuit: &Circuit<Circuit<P>>,
        update: F,
    ) -> Self {
        Self {
            input_stream: input_stream.enter(circuit),
            val: Default::default(),
            update,
        }
    }
}

impl<P, T, F> Operator for NestedSource<P, T, F>
where
    P: 'static,
    F: 'static,
    T: Data + Default,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("NestedSource")
    }

    fn clock_start(&mut self) {
        self.val = match unsafe { self.input_stream.try_take() }.unwrap_or_default() {
            Cow::Owned(v) => v,
            Cow::Borrowed(v) => v.clone(),
        }
    }

    fn clock_end(&mut self) {
        self.val = T::default();
    }
}

impl<P, T, F> SourceOperator<T> for NestedSource<P, T, F>
where
    P: 'static,
    F: Fn(&mut T) + 'static,
    T: Data + Default,
{
    fn eval(&mut self) -> T {
        let res = self.val.clone();
        (self.update)(&mut self.val);
        res
    }
}
