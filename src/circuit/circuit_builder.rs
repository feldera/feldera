//! API to construct circuits.
//!
//! The API exposes two abstractions [`Scope`]s and [`Stream`]s.
//! A scope is a handle to a circuit.  It provides methods to add
//! operators to the circuit.  Adding an operator yields a handle
//! to its output stream that can be used as input to potentially
//! multiple other operators.
//!
//! # Examples
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

use std::{
    cell::{RefCell, UnsafeCell},
    marker::PhantomData,
    ops::Deref,
    rc::Rc,
};

use super::operator_traits::{
    BinaryRefRefOperator, Data, SinkRefOperator, SourceOperator, StrictUnaryValOperator,
    UnaryRefOperator, UnaryValOperator,
};

/// A stream stores the output of an operator.  Circuits are synchronous, meaning
/// that each value is produced and consumed in the same clock cycle, so there can
/// be at most one value in the stream at any time.
pub struct Stream<S, D> {
    // Id of the associated operator.
    id: NodeId,
    // Circuit that this stream belongs to.
    scope: S,
    // The value (there can be at most one since our circuits are synchronous).
    // We use `UnsafeCell` instead of `RefCell` to avoid runtime ownership tests.
    // We enforce unique ownership by making sure that at most one operator can
    // run (and access the stream) at any time.
    val: Rc<UnsafeCell<Option<D>>>,
}

impl<S, D> Clone for Stream<S, D>
where
    S: Clone,
{
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            scope: self.scope.clone(),
            val: self.val.clone(),
        }
    }
}

impl<S, D> Stream<S, D> {
    /// Returns id of the operator that writes to this stream.
    pub fn node_id(&self) -> NodeId {
        self.id
    }
}

// Internal streams API only used inside this module.
impl<S, D> Stream<S, D> {
    // Create a new stream within the given scope and with the specified id.
    fn new(scope: S, id: NodeId) -> Self {
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

// A circuit consists of nodes and edges.  An edge from
// node1 to node2 indicates that the output stream of node1
// is connected to an input of node2.
struct Circuit<P> {
    parent: P,
    nodes: Vec<Box<dyn Node>>,
    edges: Vec<(NodeId, NodeId)>,
}

impl<P> Circuit<P> {
    fn new(parent: P) -> Self {
        Self {
            parent,
            nodes: Vec::new(),
            edges: Vec::new(),
        }
    }
}

/// A handle to a circuit.
pub struct Scope<P>(Rc<RefCell<Circuit<P>>>);

impl<P> Clone for Scope<P> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl Scope<()> {
    /// Create new top-level scope.
    pub fn new() -> Self {
        Self::with_parent(())
    }
}

impl Default for Scope<()> {
    fn default() -> Self {
        Self::new()
    }
}

impl<P> Scope<P>
where
    P: 'static + Clone,
{
    /// Creates an empty child scope of `parent`.
    pub fn with_parent(parent: P) -> Self {
        Scope(Rc::new(RefCell::new(Circuit::new(parent))))
    }

    pub fn parent(&self) -> P {
        self.0.borrow().parent.clone()
    }

    /// Evaluate operator with the given id.
    ///
    /// This method should only be used by schedulers.
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
    pub fn add_source<O, Op>(&self, operator: Op) -> Stream<Self, O>
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
    pub fn add_ref_sink<I, Op>(&self, operator: Op, input_stream: &Stream<Self, I>) -> NodeId
    where
        I: Data,
        Op: SinkRefOperator<I>,
    {
        let mut circuit = self.0.borrow_mut();
        let input_stream = input_stream.clone();
        let input_id = input_stream.node_id();
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
        input_stream: &Stream<Self, I>,
    ) -> Stream<Self, O>
    where
        I: Data,
        O: Data,
        Op: UnaryRefOperator<I, O>,
    {
        let mut circuit = self.0.borrow_mut();
        let input_stream = input_stream.clone();
        let input_id = input_stream.node_id();
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
        input_stream: &Stream<Self, I>,
    ) -> Stream<Self, O>
    where
        I: Data,
        O: Data,
        Op: UnaryValOperator<I, O>,
    {
        let mut circuit = self.0.borrow_mut();
        let input_stream = input_stream.clone();
        let input_id = input_stream.node_id();
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
        input_stream1: &Stream<Self, I1>,
        input_stream2: &Stream<Self, I2>,
    ) -> Stream<Self, O>
    where
        I1: Data,
        I2: Data,
        O: Data,
        Op: BinaryRefRefOperator<I1, I2, O>,
    {
        let mut circuit = self.0.borrow_mut();
        let input_stream1 = input_stream1.clone();
        let input_stream2 = input_stream2.clone();
        let input_id1 = input_stream1.node_id();
        let input_id2 = input_stream2.node_id();
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

    /// Add a feedback loop to the circuit.
    ///
    /// Other methods in this API only support the construction of acyclic graphs, as they require
    /// the input stream to exist before nodes that consumes it are created.  This method
    /// instantiates an operator whose input stream can be connected later, and thus may depend on
    /// the operator's output.  This enables the construction of feedback loops.  Since all loops
    /// in a well-formed circuit must include a [strict
    /// operator](`crate::circuit::operator_traits::StrictOperator`), `operator` must be strict.
    ///
    /// Returns the output stream of the operator and an object that can be used to later
    /// connect its input.
    ///
    /// # Examples
    /// We build the following circuit to compute the sum of input values received from `source`.
    /// `z1` stores the sum accumulated during previous timestamps.  At every timestamp,
    /// the [`crate::circuit::operator::Plus`] operator (`+`) computes the sum of the new value
    /// received from source with the value stored in `z1`.
    ///
    /// ```text
    ///                ┌─┐
    /// source ───────►│+├───┬─►
    ///           ▲    └─┘   │
    ///           │          │
    ///           │    ┌──┐  │
    ///           └────┤z1│◄─┘
    ///                └──┘
    /// ```
    ///
    /// ```
    /// # use dbsp::circuit::{
    /// #   Scope,
    /// #   operator::{Z1, Plus, Repeat},
    /// # };
    /// # let scope = Scope::new();
    /// // Create a data source.
    /// let source = scope.add_source(Repeat::new(10));
    /// // Create z1.  `z1_output` will contain the output stream of `z1`; `z1_feedback`
    /// // is a placeholder where we can later plug the input stream.
    /// let (z1_output, z1_feedback) = scope.add_feedback(Z1::new());
    /// // Connect outputs of `source` and `z1` to the plus operator.
    /// let plus = scope.add_binary_refref_operator(Plus::new(), &source, &z1_output);
    /// // Connect the output of `+` as input to `z1`.
    /// let z1_input_id = z1_feedback.connect(&plus);
    /// ```
    pub fn add_feedback<I, O, Op>(
        &self,
        operator: Op,
    ) -> (Stream<Self, O>, FeedbackConnector<Self, I, O, Op>)
    where
        I: Data,
        O: Data,
        Op: StrictUnaryValOperator<I, O>,
    {
        let mut circuit = self.0.borrow_mut();
        let operator = Rc::new(UnsafeCell::new(operator));
        let connector = FeedbackConnector::new(self.clone(), operator.clone());
        let id = circuit.nodes.len();
        let output_node = Box::new(FeedbackOutputNode::new(operator, self.clone(), id));
        let output_stream = output_node.output_stream();
        circuit.nodes.push(output_node as Box<dyn Node>);
        (output_stream, connector)
    }

    fn connect_feedback<I, O, Op>(
        &self,
        operator: Rc<UnsafeCell<Op>>,
        input_stream: &Stream<Self, I>,
    ) -> NodeId
    where
        I: Data,
        O: Data,
        Op: StrictUnaryValOperator<I, O>,
    {
        let mut circuit = self.0.borrow_mut();
        let input_id = input_stream.node_id();
        let id = circuit.nodes.len();
        let output_node = Box::new(FeedbackInputNode::new(operator, input_stream.clone()));
        circuit.nodes.push(output_node as Box<dyn Node>);
        circuit.edges.push((input_id, id));
        id
    }
}

struct SourceNode<S, O, Op>
where
    Op: SourceOperator<O>,
{
    operator: Op,
    output_stream: Stream<S, O>,
}

impl<S, O, Op> SourceNode<S, O, Op>
where
    Op: SourceOperator<O>,
    S: Clone,
{
    fn new(operator: Op, scope: S, id: NodeId) -> Self {
        Self {
            operator,
            output_stream: Stream::new(scope, id),
        }
    }

    fn output_stream(&self) -> Stream<S, O> {
        self.output_stream.clone()
    }
}

impl<S, O, Op> Node for SourceNode<S, O, Op>
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

struct UnaryRefNode<S, I, O, Op>
where
    Op: UnaryRefOperator<I, O>,
{
    operator: Op,
    input_stream: Stream<S, I>,
    output_stream: Stream<S, O>,
}

impl<S, I, O, Op> UnaryRefNode<S, I, O, Op>
where
    Op: UnaryRefOperator<I, O>,
    S: Clone,
{
    fn new(operator: Op, input_stream: Stream<S, I>, scope: S, id: NodeId) -> Self {
        Self {
            operator,
            input_stream,
            output_stream: Stream::new(scope, id),
        }
    }

    fn output_stream(&self) -> Stream<S, O> {
        self.output_stream.clone()
    }
}

impl<S, I, O, Op> Node for UnaryRefNode<S, I, O, Op>
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

struct SinkRefNode<S, I, Op>
where
    Op: SinkRefOperator<I>,
{
    operator: Op,
    input_stream: Stream<S, I>,
}

impl<S, I, Op> SinkRefNode<S, I, Op>
where
    Op: SinkRefOperator<I>,
{
    fn new(operator: Op, input_stream: Stream<S, I>, _scope: S, _id: NodeId) -> Self {
        Self {
            operator,
            input_stream,
        }
    }
}

impl<S, I, Op> Node for SinkRefNode<S, I, Op>
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

struct UnaryValNode<S, I, O, Op>
where
    Op: UnaryValOperator<I, O>,
{
    operator: Op,
    input_stream: Stream<S, I>,
    output_stream: Stream<S, O>,
}

impl<S, I, O, Op> UnaryValNode<S, I, O, Op>
where
    Op: UnaryValOperator<I, O>,
    S: Clone,
{
    fn new(operator: Op, input_stream: Stream<S, I>, scope: S, id: NodeId) -> Self {
        Self {
            operator,
            input_stream,
            output_stream: Stream::new(scope, id),
        }
    }

    fn output_stream(&self) -> Stream<S, O> {
        self.output_stream.clone()
    }
}

impl<S, I, O, Op> Node for UnaryValNode<S, I, O, Op>
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

struct BinaryRefRefNode<S, I1, I2, O, Op>
where
    Op: BinaryRefRefOperator<I1, I2, O>,
{
    operator: Op,
    input_stream1: Stream<S, I1>,
    input_stream2: Stream<S, I2>,
    output_stream: Stream<S, O>,
}

impl<S, I1, I2, O, Op> BinaryRefRefNode<S, I1, I2, O, Op>
where
    Op: BinaryRefRefOperator<I1, I2, O>,
    S: Clone,
{
    fn new(
        operator: Op,
        input_stream1: Stream<S, I1>,
        input_stream2: Stream<S, I2>,
        scope: S,
        id: NodeId,
    ) -> Self {
        Self {
            operator,
            input_stream1,
            input_stream2,
            output_stream: Stream::new(scope, id),
        }
    }

    fn output_stream(&self) -> Stream<S, O> {
        self.output_stream.clone()
    }
}

impl<S, I1, I2, O, Op> Node for BinaryRefRefNode<S, I1, I2, O, Op>
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

struct FeedbackOutputNode<S, I, O, Op>
where
    Op: StrictUnaryValOperator<I, O>,
{
    operator: Rc<UnsafeCell<Op>>,
    output_stream: Stream<S, O>,
    phantom_input: PhantomData<I>,
}

impl<S, I, O, Op> FeedbackOutputNode<S, I, O, Op>
where
    S: Clone,
    Op: StrictUnaryValOperator<I, O>,
{
    fn new(operator: Rc<UnsafeCell<Op>>, scope: S, id: NodeId) -> Self {
        Self {
            operator,
            output_stream: Stream::new(scope, id),
            phantom_input: PhantomData,
        }
    }

    fn output_stream(&self) -> Stream<S, O> {
        self.output_stream.clone()
    }
}

impl<S, I, O, Op> Node for FeedbackOutputNode<S, I, O, Op>
where
    I: Data,
    Op: StrictUnaryValOperator<I, O>,
{
    unsafe fn eval(&mut self) {
        self.output_stream
            .put((&mut *self.operator.get()).get_output());
    }

    fn stream_start(&mut self) {
        unsafe {
            (&mut *self.operator.get()).stream_start();
        }
    }

    unsafe fn stream_end(&mut self) {
        (&mut *self.operator.get()).stream_end();
        self.output_stream.clear();
    }
}

struct FeedbackInputNode<S, I, O, Op>
where
    Op: StrictUnaryValOperator<I, O>,
{
    operator: Rc<UnsafeCell<Op>>,
    input_stream: Stream<S, I>,
    phantom_output: PhantomData<O>,
}

impl<S, I, O, Op> FeedbackInputNode<S, I, O, Op>
where
    Op: StrictUnaryValOperator<I, O>,
{
    fn new(operator: Rc<UnsafeCell<Op>>, input_stream: Stream<S, I>) -> Self {
        Self {
            operator,
            input_stream,
            phantom_output: PhantomData,
        }
    }
}

impl<S, I, O, Op> Node for FeedbackInputNode<S, I, O, Op>
where
    Op: StrictUnaryValOperator<I, O>,
    I: Data,
{
    unsafe fn eval(&mut self) {
        (&mut *self.operator.get()).eval_strict(
            self.input_stream
                .get()
                .clone()
                .expect("operator scheduled before its input is ready"),
        );
    }

    // Don't call `stream_start`/`stream_end` on the operator.  `FeedbackOutputNode` will do that.
    fn stream_start(&mut self) {}

    unsafe fn stream_end(&mut self) {}
}

/// Input connector of a feedback operator.
///
/// This struct is part of the mechanism for constructing a feedback loop in a circuit.
/// It is returned by [`Scope::add_feedback`] and represents the input port of an operator
/// whose input stream does not exist yet.  Once the input stream has been created, it
/// can be connected to the operator using [`FeedbackConnector::connect`].
/// See [`Scope::add_feedback`] for details.
pub struct FeedbackConnector<S, I, O, Op>
where
    Op: StrictUnaryValOperator<I, O>,
{
    circuit: S,
    operator: Rc<UnsafeCell<Op>>,
    phantom_input: PhantomData<I>,
    phantom_output: PhantomData<O>,
}

impl<S, I, O, Op> FeedbackConnector<S, I, O, Op>
where
    Op: StrictUnaryValOperator<I, O>,
{
    fn new(circuit: S, operator: Rc<UnsafeCell<Op>>) -> Self {
        Self {
            circuit,
            operator,
            phantom_input: PhantomData,
            phantom_output: PhantomData,
        }
    }
}

impl<P, I, O, Op> FeedbackConnector<Scope<P>, I, O, Op>
where
    Op: StrictUnaryValOperator<I, O>,
    I: Data,
    O: Data,
    P: Clone + 'static,
{
    /// Connect `input_stream` as input to the operator.
    /// See [`Scope::add_feedback`] for details.
    /// Returns node id of the input node.
    // TODO: The return value won't be needed once we have schedulers.
    pub fn connect(self, input_stream: &Stream<Scope<P>, I>) -> NodeId {
        self.circuit.connect_feedback(self.operator, input_stream)
    }
}

#[cfg(test)]
mod tests {
    use super::Scope;
    use crate::circuit::operator::{Inspect, Plus, Z1};
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

    // Compute the sum of numbers from 0 to 99.
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
            scope.eval(source.node_id());
            scope.eval(integrator.node_id());
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

    // Recursive circuit
    #[test]
    fn recursive_sum_circuit() {
        let actual_output: Rc<RefCell<Vec<usize>>> = Rc::new(RefCell::new(Vec::with_capacity(100)));
        let actual_output_clone = actual_output.clone();
        let scope = Scope::new();
        let source = scope.add_source(Counter::new());
        let (z1_output, z1_feedback) = scope.add_feedback(Z1::new());
        let plus = scope.add_binary_refref_operator(Plus::new(), &source, &z1_output);
        let sinkid = scope.add_ref_sink(
            Inspect::new(move |n| actual_output_clone.borrow_mut().push(*n)),
            &plus,
        );
        let z1_input_id = z1_feedback.connect(&plus);

        for _ in 0..100 {
            scope.eval(z1_output.node_id());
            scope.eval(source.node_id());
            scope.eval(plus.node_id());
            scope.eval(z1_input_id);
            scope.eval(sinkid);
        }

        let mut sum = 0;
        let mut expected_output: Vec<usize> = Vec::with_capacity(100);
        for i in 0..100 {
            sum += i;
            expected_output.push(sum);
        }
        assert_eq!(&expected_output, actual_output.borrow().deref());
    }
}
