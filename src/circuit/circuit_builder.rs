//! API to construct circuits.
//!
//! The API exposes two abstractions [`Scope`]s and [`Stream`]s.
//! A scope is a handle to a circuit.  It provides methods to add
//! operators to the circuit.  Adding an operator yields a handle
//! to its output stream that can be used as input to potentially
//! multiple other operators.
//!
//! ```
//! use dbsp::circuit::Scope;
//! use dbsp::circuit::operator::{Inspect, Repeat};
//!
//! // Create an empty circuit.
//! let scope = Scope::new();
//!
//! // Add a source operator.
//! let source_stream = scope.add_source(Repeat::new("Hello, world!".to_owned()));
//!
//! // Add a sink operator and wire the source directly to it.
//! let sinkid = scope.add_ref_sink(
//!     Inspect::new(|n| println!("New output: {}", n)),
//!     &source_stream
//! );
//! ```
//!
//! This module

use std::{
    cell::{RefCell, UnsafeCell},
    ops::Deref,
    rc::Rc,
};

use super::operator_traits::{
    BinaryRefRefOperator, Data, SinkRefOperator, SourceOperator, UnaryRefOperator, UnaryValOperator,
};

/// A stream stores the output of an operator.  Circuits are synchronous, meaning
/// that each value is produced and consumed in the same clock cycle, so there can
/// be at most one value in the stream at any time.
pub struct Stream<D> {
    // Id of the associated operator.
    id: NodeId,
    // Circuit that this stream belongs to.
    scope: Scope,
    // The value (there can be at most one since our circuits are synchronous).
    // We use `UnsafeCell` instead of `RefCell` to avoid runtime ownership tests.
    // We enforce unique ownership by making sure that at most one operator can
    // run (and access the stream) at any time.
    val: Rc<UnsafeCell<Option<D>>>,
}

impl<D> Clone for Stream<D> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            scope: self.scope.clone(),
            val: self.val.clone(),
        }
    }
}

impl<D> Stream<D> {
    /// Returns id of the operator that writes to this stream.
    pub fn id(&self) -> NodeId {
        self.id
    }
}

// Internal streams API only used inside this module.
impl<D> Stream<D> {
    // Create a new stream within the given scope and with the specified id.
    fn new(scope: Scope, id: NodeId) -> Self {
        Self {
            id,
            scope,
            val: Rc::new(UnsafeCell::new(None)),
        }
    }

    // Returns `Some` if the operator has produced output for the current timestamp and `None`
    // otherwise.
    unsafe fn get(&self) -> &Option<D> {
        &*self.val.get()
    }

    // Puts a value in the stream, overwriting the previous value if any.
    unsafe fn put(&self, val: D) {
        *self.val.get() = Some(val);
    }

    /*unsafe fn take(&self) -> Option<D> {
        let mut val = None;
        swap(&mut *self.val.get(), &mut val);
        val
    }*/

    // Remove the value in the stream, if any, leaving the stream empty.
    unsafe fn clear(&self) {
        *self.val.get() = None;
    }
}

// Node in a circuit.  A node wraps an operator with strongly typed
// input and output streams.
trait Node {
    // Evaluate the operator.  Reads one value from each input stream
    // and pushes a new value to the output stream (except for sink
    // operators, which don't have an output stream).
    unsafe fn eval(&mut self);

    fn stream_start(&mut self);
    unsafe fn stream_end(&mut self);
}

/// Id of an operator, guaranteed to be unique within a circuit.
pub type NodeId = usize;

// A circuit consists of nodes and edges.  And edge from
// node1 to node2 indicate that the output stream of node1
// is connected to an input of node2.
struct Circuit {
    nodes: Vec<Box<dyn Node>>,
    edges: Vec<(NodeId, NodeId)>,
}

impl Circuit {
    fn new() -> Self {
        Self {
            nodes: Vec::new(),
            edges: Vec::new(),
        }
    }
}

/// A handle to a circuit.
#[derive(Clone)]
pub struct Scope(Rc<RefCell<Circuit>>);

impl Default for Scope {
    fn default() -> Self {
        Self::new()
    }
}

impl Scope {
    /// Creates an empty scope.
    pub fn new() -> Self {
        Scope(Rc::new(RefCell::new(Circuit::new())))
    }

    /// Evaluate operator with the given id.  This method should only be used
    /// by schedulers.
    pub fn eval(&self, id: usize) {
        let mut circuit = self.0.borrow_mut();
        // This is safe because `eval` can never invoke the
        // `eval` method of another node.  To circumvent
        // this invariant the user would have to extract a
        // reference to a node and pass it to an operator,
        // but this module doesn't expose nodes, only
        // channels.
        unsafe { circuit.nodes[id].eval() };
    }

    /// Add a source operator to the scope.  See [`SourceOperator`].
    pub fn add_source<O, Op>(&self, operator: Op) -> Stream<O>
    where
        O: Data,
        Op: SourceOperator<O>,
    {
        let mut circuit = self.0.borrow_mut();
        let id = circuit.nodes.len();
        let node = Box::new(SourceNode::new(operator, self.clone(), id));
        let output_stream = node.output_stream();
        circuit.nodes.push(node as Box<dyn Node>);
        output_stream
    }

    /// Add a sink operator that consumes input values by reference.
    /// See [`SinkRefOperator`].
    pub fn add_ref_sink<I, Op>(&self, operator: Op, input_stream: &Stream<I>) -> NodeId
    where
        I: Data,
        Op: SinkRefOperator<I>,
    {
        let mut circuit = self.0.borrow_mut();
        let input_stream = input_stream.clone();
        let input_id = input_stream.id();
        let id = circuit.nodes.len();
        let node = Box::new(SinkRefNode::new(operator, input_stream, self.clone(), id));
        circuit.nodes.push(node as Box<dyn Node>);
        circuit.edges.push((input_id, id));
        id
    }

    /// Add a unary operator that consumes input values by reference.
    /// See [`UnaryRefOperator`].
    pub fn add_unary_ref_operator<I, O, Op>(
        &self,
        operator: Op,
        input_stream: &Stream<I>,
    ) -> Stream<O>
    where
        I: Data,
        O: Data,
        Op: UnaryRefOperator<I, O>,
    {
        let mut circuit = self.0.borrow_mut();
        let input_stream = input_stream.clone();
        let input_id = input_stream.id();
        let id = circuit.nodes.len();
        let node = Box::new(UnaryRefNode::new(operator, input_stream, self.clone(), id));
        let output_stream = node.output_stream();
        circuit.nodes.push(node as Box<dyn Node>);
        circuit.edges.push((input_id, id));
        output_stream
    }

    /// Add a unary operator that consumes inputs by value.
    /// See [`UnaryValOperator`].
    pub fn add_unary_val_operator<I, O, Op>(
        &self,
        operator: Op,
        input_stream: &Stream<I>,
    ) -> Stream<O>
    where
        I: Data,
        O: Data,
        Op: UnaryValOperator<I, O>,
    {
        let mut circuit = self.0.borrow_mut();
        let input_stream = input_stream.clone();
        let input_id = input_stream.id();
        let id = circuit.nodes.len();
        let node = Box::new(UnaryValNode::new(operator, input_stream, self.clone(), id));
        let output_stream = node.output_stream();
        circuit.nodes.push(node as Box<dyn Node>);
        circuit.edges.push((input_id, id));
        output_stream
    }

    /// Add a binary operator that consumes both inputs by reference.
    /// See [`BinaryRefRefOperator`].
    pub fn add_binary_refref_operator<I1, I2, O, Op>(
        &self,
        operator: Op,
        input_stream1: &Stream<I1>,
        input_stream2: &Stream<I2>,
    ) -> Stream<O>
    where
        I1: Data,
        I2: Data,
        O: Data,
        Op: BinaryRefRefOperator<I1, I2, O>,
    {
        let mut circuit = self.0.borrow_mut();
        let input_stream1 = input_stream1.clone();
        let input_stream2 = input_stream2.clone();
        let input_id1 = input_stream1.id();
        let input_id2 = input_stream2.id();
        let id = circuit.nodes.len();
        let node = Box::new(BinaryRefRefNode::new(
            operator,
            input_stream1,
            input_stream2,
            self.clone(),
            id,
        ));
        let output_stream = node.output_stream();
        circuit.nodes.push(node as Box<dyn Node>);
        circuit.edges.push((input_id1, id));
        circuit.edges.push((input_id2, id));
        output_stream
    }
}

struct SourceNode<O, Op>
where
    Op: SourceOperator<O>,
{
    operator: Op,
    output_stream: Stream<O>,
}

impl<O, Op> SourceNode<O, Op>
where
    Op: SourceOperator<O>,
{
    fn new(operator: Op, scope: Scope, id: NodeId) -> Self {
        Self {
            operator,
            output_stream: Stream::new(scope, id),
        }
    }

    fn output_stream(&self) -> Stream<O> {
        self.output_stream.clone()
    }
}

impl<O, Op> Node for SourceNode<O, Op>
where
    Op: SourceOperator<O>,
{
    unsafe fn eval(&mut self) {
        self.output_stream.put(self.operator.eval());
    }

    fn stream_start(&mut self) {
        self.operator.stream_start();
    }

    unsafe fn stream_end(&mut self) {
        self.operator.stream_end();
        self.output_stream.clear();
    }
}

struct UnaryRefNode<I, O, Op>
where
    Op: UnaryRefOperator<I, O>,
{
    operator: Op,
    input_stream: Stream<I>,
    output_stream: Stream<O>,
}

impl<I, O, Op> UnaryRefNode<I, O, Op>
where
    Op: UnaryRefOperator<I, O>,
{
    fn new(operator: Op, input_stream: Stream<I>, scope: Scope, id: NodeId) -> Self {
        Self {
            operator,
            input_stream,
            output_stream: Stream::new(scope, id),
        }
    }

    fn output_stream(&self) -> Stream<O> {
        self.output_stream.clone()
    }
}

impl<I, O, Op> Node for UnaryRefNode<I, O, Op>
where
    Op: UnaryRefOperator<I, O>,
{
    unsafe fn eval(&mut self) {
        self.output_stream.put(
            self.operator.eval(
                self.input_stream
                    .get()
                    .deref()
                    .as_ref()
                    .expect("operator scheduled before its input is ready"),
            ),
        );
    }

    fn stream_start(&mut self) {
        self.operator.stream_start();
    }

    unsafe fn stream_end(&mut self) {
        self.operator.stream_end();
        self.output_stream.clear();
    }
}

struct SinkRefNode<I, Op>
where
    Op: SinkRefOperator<I>,
{
    operator: Op,
    input_stream: Stream<I>,
}

impl<I, Op> SinkRefNode<I, Op>
where
    Op: SinkRefOperator<I>,
{
    fn new(operator: Op, input_stream: Stream<I>, _scope: Scope, _id: NodeId) -> Self {
        Self {
            operator,
            input_stream,
        }
    }
}

impl<I, Op> Node for SinkRefNode<I, Op>
where
    Op: SinkRefOperator<I>,
{
    unsafe fn eval(&mut self) {
        self.operator.eval(
            self.input_stream
                .get()
                .deref()
                .as_ref()
                .expect("operator scheduled before its input is ready"),
        );
    }

    fn stream_start(&mut self) {
        self.operator.stream_start();
    }

    unsafe fn stream_end(&mut self) {
        self.operator.stream_end();
    }
}

struct UnaryValNode<I, O, Op>
where
    Op: UnaryValOperator<I, O>,
{
    operator: Op,
    input_stream: Stream<I>,
    output_stream: Stream<O>,
}

impl<I, O, Op> UnaryValNode<I, O, Op>
where
    Op: UnaryValOperator<I, O>,
{
    fn new(operator: Op, input_stream: Stream<I>, scope: Scope, id: NodeId) -> Self {
        Self {
            operator,
            input_stream,
            output_stream: Stream::new(scope, id),
        }
    }

    fn output_stream(&self) -> Stream<O> {
        self.output_stream.clone()
    }
}

impl<I, O, Op> Node for UnaryValNode<I, O, Op>
where
    I: Data,
    Op: UnaryValOperator<I, O>,
{
    unsafe fn eval(&mut self) {
        self.output_stream.put(
            self.operator.eval(
                // TODO: avoid clone when we are the last consumer of the value.
                self.input_stream
                    .get()
                    .clone()
                    .expect("operator scheduled before its input is ready"),
            ),
        );
    }

    fn stream_start(&mut self) {
        self.operator.stream_start();
    }

    unsafe fn stream_end(&mut self) {
        self.operator.stream_end();
        self.output_stream.clear();
    }
}

struct BinaryRefRefNode<I1, I2, O, Op>
where
    Op: BinaryRefRefOperator<I1, I2, O>,
{
    operator: Op,
    input_stream1: Stream<I1>,
    input_stream2: Stream<I2>,
    output_stream: Stream<O>,
}

impl<I1, I2, O, Op> BinaryRefRefNode<I1, I2, O, Op>
where
    Op: BinaryRefRefOperator<I1, I2, O>,
{
    fn new(
        operator: Op,
        input_stream1: Stream<I1>,
        input_stream2: Stream<I2>,
        scope: Scope,
        id: NodeId,
    ) -> Self {
        Self {
            operator,
            input_stream1,
            input_stream2,
            output_stream: Stream::new(scope, id),
        }
    }

    fn output_stream(&self) -> Stream<O> {
        self.output_stream.clone()
    }
}

impl<I1, I2, O, Op> Node for BinaryRefRefNode<I1, I2, O, Op>
where
    Op: BinaryRefRefOperator<I1, I2, O>,
{
    unsafe fn eval(&mut self) {
        self.output_stream.put(
            self.operator.eval(
                self.input_stream1
                    .get()
                    .deref()
                    .as_ref()
                    .expect("operator scheduled before its input is ready"),
                self.input_stream2
                    .get()
                    .deref()
                    .as_ref()
                    .expect("operator scheduled before its input is ready"),
            ),
        );
    }

    fn stream_start(&mut self) {
        self.operator.stream_start();
    }

    unsafe fn stream_end(&mut self) {
        self.operator.stream_end();
        self.output_stream.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::Scope;
    use crate::circuit::operator::Inspect;
    use crate::circuit::operator_traits::{
        Operator, SinkRefOperator, SourceOperator, UnaryRefOperator,
    };
    use std::{cell::RefCell, fmt::Display, marker::PhantomData, ops::Deref, rc::Rc, vec::Vec};

    // Source operator that generates a stream of consecutive integers.
    struct Counter {
        n: usize,
    }
    impl Counter {
        fn new() -> Self {
            Self { n: 0 }
        }
    }
    impl Operator for Counter {
        fn stream_start(&mut self) {}
        fn stream_end(&mut self) {
            self.n = 0;
        }
    }
    impl SourceOperator<usize> for Counter {
        fn eval(&mut self) -> usize {
            let res = self.n;
            self.n += 1;
            res
        }
    }

    // Operator that integrates its input stream.
    struct Integrator {
        sum: usize,
    }
    impl Integrator {
        fn new() -> Self {
            Self { sum: 0 }
        }
    }
    impl Operator for Integrator {
        fn stream_start(&mut self) {}
        fn stream_end(&mut self) {
            self.sum = 0;
        }
    }
    impl UnaryRefOperator<usize, usize> for Integrator {
        fn eval(&mut self, i: &usize) -> usize {
            self.sum = self.sum + *i;
            self.sum
        }
    }

    // Sink operator that prints all elements in its input stream.
    struct Printer<T> {
        phantom: PhantomData<T>,
    }
    impl<T> Printer<T> {
        fn new() -> Self {
            Self {
                phantom: PhantomData,
            }
        }
    }
    impl<T: 'static> Operator for Printer<T> {
        fn stream_start(&mut self) {}
        fn stream_end(&mut self) {}
    }
    impl<T: Display + 'static> SinkRefOperator<T> for Printer<T> {
        fn eval(&mut self, i: &T) {
            println!("new output: {}", i);
        }
    }

    // Compute the sump of numbers from 0 to 99.
    #[test]
    fn sum_circuit() {
        let actual_output: Rc<RefCell<Vec<usize>>> = Rc::new(RefCell::new(Vec::with_capacity(100)));
        let actual_output_clone = actual_output.clone();
        let scope = Scope::new();
        let source = scope.add_source(Counter::new());
        let integrator = scope.add_unary_ref_operator(Integrator::new(), &source);
        let sinkid1 = scope.add_ref_sink(Printer::new(), &integrator);
        let sinkid2 = scope.add_ref_sink(
            Inspect::new(move |n| actual_output_clone.borrow_mut().push(*n)),
            &integrator,
        );

        for _ in 0..100 {
            scope.eval(source.id());
            scope.eval(integrator.id());
            scope.eval(sinkid1);
            scope.eval(sinkid2);
        }

        let mut sum = 0;
        let mut expected_output: Vec<usize> = Vec::with_capacity(100);
        for i in 0..100 {
            sum += i;
            expected_output.push(sum);
        }
        assert_eq!(&expected_output, actual_output.borrow().deref());
    }

    // TODO: Recursive circuit
}
