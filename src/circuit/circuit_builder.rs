//! API to construct circuits.
//!
//! The API exposes two abstractions [`Circuit`]s and [`Stream`]s.
//! A circuit is a dataflow graph that consists of operators connected
//! by streams.  It provides methods to add operators to the circuit.
//! Adding an operator yields a handle to its output stream that can be
//! used as input to potentially multiple other operators.
//!
//! # Examples
//!
//! ```
//! use dbsp::circuit::Circuit;
//! use dbsp::circuit::operator::{Inspect, Repeat};
//!
//! // Create an empty circuit.
//! let circuit = Circuit::new();
//!
//! // Add a source operator.
//! let source_stream = circuit.add_source(Repeat::new("Hello, world!".to_owned()));
//!
//! // Add a sink operator and wire the source directly to it.
//! let sinkid = circuit.add_ref_sink(
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
pub struct Stream<C, D> {
    // Id of the associated operator.
    id: NodeId,
    // Circuit that this stream belongs to.
    circuit: C,
    // The value (there can be at most one since our circuits are synchronous).
    // We use `UnsafeCell` instead of `RefCell` to avoid runtime ownership tests.
    // We enforce unique ownership by making sure that at most one operator can
    // run (and access the stream) at any time.
    val: Rc<UnsafeCell<Option<D>>>,
}

impl<C, D> Clone for Stream<C, D>
where
    C: Clone,
{
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            circuit: self.circuit.clone(),
            val: self.val.clone(),
        }
    }
}

impl<C, D> Stream<C, D> {
    /// Returns id of the operator that writes to this stream.
    pub fn node_id(&self) -> NodeId {
        self.id
    }
}

// Internal streams API only used inside this module.
impl<C, D> Stream<C, D> {
    // Create a new stream within the given circuit and with the specified id.
    fn new(circuit: C, id: NodeId) -> Self {
        Self {
            id,
            circuit,
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

    // Notify the node about start/end of input streams.  The node
    // should forward the notification to it inner operator.  In
    // addition, it should clear its output channel on `stream_end`.
    fn stream_start(&mut self);
    unsafe fn stream_end(&mut self);
}

/// Id of an operator, guaranteed to be unique within a circuit.
pub type NodeId = usize;

// A circuit consists of nodes and edges.  An edge from
// node1 to node2 indicates that the output stream of node1
// is connected to an input of node2.
struct CircuitInner<P> {
    parent: P,
    nodes: Vec<Box<dyn Node>>,
    edges: Vec<(NodeId, NodeId)>,
}

impl<P> CircuitInner<P> {
    fn new(parent: P) -> Self {
        Self {
            parent,
            nodes: Vec::new(),
            edges: Vec::new(),
        }
    }
}

/// A handle to a circuit.
pub struct Circuit<P>(Rc<RefCell<CircuitInner<P>>>);

impl<P> Clone for Circuit<P> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl Circuit<()> {
    /// Create new top-level circuit.
    pub fn new() -> Self {
        Self::with_parent(())
    }
}

impl Default for Circuit<()> {
    fn default() -> Self {
        Self::new()
    }
}

impl<P> Circuit<P>
where
    P: 'static + Clone,
{
    /// Creates an empty child circuit of `parent`.
    pub fn with_parent(parent: P) -> Self {
        Circuit(Rc::new(RefCell::new(CircuitInner::new(parent))))
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

    /// Add a source operator to the circuit.  See [`SourceOperator`].
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
    ///            ───►└─┘   │
    ///           │          │
    ///           │    ┌──┐  │
    ///           └────┤z1│◄─┘
    ///                └──┘
    /// ```
    ///
    /// ```
    /// # use dbsp::circuit::{
    /// #   Circuit,
    /// #   operator::{Z1, Plus, Repeat},
    /// # };
    /// # let circuit = Circuit::new();
    /// // Create a data source.
    /// let source = circuit.add_source(Repeat::new(10));
    /// // Create z1.  `z1_output` will contain the output stream of `z1`; `z1_feedback`
    /// // is a placeholder where we can later plug the input to `z1`.
    /// let (z1_output, z1_feedback) = circuit.add_feedback(Z1::new());
    /// // Connect outputs of `source` and `z1` to the plus operator.
    /// let plus = circuit.add_binary_refref_operator(Plus::new(), &source, &z1_output);
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

struct SourceNode<C, O, Op> {
    operator: Op,
    output_stream: Stream<C, O>,
}

impl<C, O, Op> SourceNode<C, O, Op>
where
    Op: SourceOperator<O>,
    C: Clone,
{
    fn new(operator: Op, circuit: C, id: NodeId) -> Self {
        Self {
            operator,
            output_stream: Stream::new(circuit, id),
        }
    }

    fn output_stream(&self) -> Stream<C, O> {
        self.output_stream.clone()
    }
}

impl<C, O, Op> Node for SourceNode<C, O, Op>
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

struct UnaryRefNode<C, I, O, Op> {
    operator: Op,
    input_stream: Stream<C, I>,
    output_stream: Stream<C, O>,
}

impl<C, I, O, Op> UnaryRefNode<C, I, O, Op>
where
    Op: UnaryRefOperator<I, O>,
    C: Clone,
{
    fn new(operator: Op, input_stream: Stream<C, I>, circuit: C, id: NodeId) -> Self {
        Self {
            operator,
            input_stream,
            output_stream: Stream::new(circuit, id),
        }
    }

    fn output_stream(&self) -> Stream<C, O> {
        self.output_stream.clone()
    }
}

impl<C, I, O, Op> Node for UnaryRefNode<C, I, O, Op>
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

struct SinkRefNode<C, I, Op> {
    operator: Op,
    input_stream: Stream<C, I>,
}

impl<C, I, Op> SinkRefNode<C, I, Op>
where
    Op: SinkRefOperator<I>,
{
    fn new(operator: Op, input_stream: Stream<C, I>, _circuit: C, _id: NodeId) -> Self {
        Self {
            operator,
            input_stream,
        }
    }
}

impl<C, I, Op> Node for SinkRefNode<C, I, Op>
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

struct UnaryValNode<C, I, O, Op> {
    operator: Op,
    input_stream: Stream<C, I>,
    output_stream: Stream<C, O>,
}

impl<C, I, O, Op> UnaryValNode<C, I, O, Op>
where
    Op: UnaryValOperator<I, O>,
    C: Clone,
{
    fn new(operator: Op, input_stream: Stream<C, I>, circuit: C, id: NodeId) -> Self {
        Self {
            operator,
            input_stream,
            output_stream: Stream::new(circuit, id),
        }
    }

    fn output_stream(&self) -> Stream<C, O> {
        self.output_stream.clone()
    }
}

impl<C, I, O, Op> Node for UnaryValNode<C, I, O, Op>
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

struct BinaryRefRefNode<C, I1, I2, O, Op> {
    operator: Op,
    input_stream1: Stream<C, I1>,
    input_stream2: Stream<C, I2>,
    output_stream: Stream<C, O>,
}

impl<C, I1, I2, O, Op> BinaryRefRefNode<C, I1, I2, O, Op>
where
    Op: BinaryRefRefOperator<I1, I2, O>,
    C: Clone,
{
    fn new(
        operator: Op,
        input_stream1: Stream<C, I1>,
        input_stream2: Stream<C, I2>,
        circuit: C,
        id: NodeId,
    ) -> Self {
        Self {
            operator,
            input_stream1,
            input_stream2,
            output_stream: Stream::new(circuit, id),
        }
    }

    fn output_stream(&self) -> Stream<C, O> {
        self.output_stream.clone()
    }
}

impl<C, I1, I2, O, Op> Node for BinaryRefRefNode<C, I1, I2, O, Op>
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

// The output half of a feedback node.  We implement a feedback node using a pair of nodes:
// `FeedbackOutputNode` is connected to the circuit as a source node (i.e., it does not have
// an input stream) and thus gets evaluated first in each time stamp.  `FeedbackInputNode`
// is a sink node.  This way the circuit graph remains acyclic and can be scheduled in a
// topological order.
struct FeedbackOutputNode<C, I, O, Op> {
    operator: Rc<UnsafeCell<Op>>,
    output_stream: Stream<C, O>,
    phantom_input: PhantomData<I>,
}

impl<C, I, O, Op> FeedbackOutputNode<C, I, O, Op>
where
    C: Clone,
    Op: StrictUnaryValOperator<I, O>,
{
    fn new(operator: Rc<UnsafeCell<Op>>, circuit: C, id: NodeId) -> Self {
        Self {
            operator,
            output_stream: Stream::new(circuit, id),
            phantom_input: PhantomData,
        }
    }

    fn output_stream(&self) -> Stream<C, O> {
        self.output_stream.clone()
    }
}

impl<C, I, O, Op> Node for FeedbackOutputNode<C, I, O, Op>
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

struct FeedbackInputNode<C, I, O, Op> {
    operator: Rc<UnsafeCell<Op>>,
    input_stream: Stream<C, I>,
    phantom_output: PhantomData<O>,
}

impl<C, I, O, Op> FeedbackInputNode<C, I, O, Op>
where
    Op: StrictUnaryValOperator<I, O>,
{
    fn new(operator: Rc<UnsafeCell<Op>>, input_stream: Stream<C, I>) -> Self {
        Self {
            operator,
            input_stream,
            phantom_output: PhantomData,
        }
    }
}

impl<C, I, O, Op> Node for FeedbackInputNode<C, I, O, Op>
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
/// It is returned by [`Circuit::add_feedback`] and represents the input port of an operator
/// whose input stream does not exist yet.  Once the input stream has been created, it
/// can be connected to the operator using [`FeedbackConnector::connect`].
/// See [`Circuit::add_feedback`] for details.
pub struct FeedbackConnector<C, I, O, Op> {
    circuit: C,
    operator: Rc<UnsafeCell<Op>>,
    phantom_input: PhantomData<I>,
    phantom_output: PhantomData<O>,
}

impl<C, I, O, Op> FeedbackConnector<C, I, O, Op>
where
    Op: StrictUnaryValOperator<I, O>,
{
    fn new(circuit: C, operator: Rc<UnsafeCell<Op>>) -> Self {
        Self {
            circuit,
            operator,
            phantom_input: PhantomData,
            phantom_output: PhantomData,
        }
    }
}

impl<P, I, O, Op> FeedbackConnector<Circuit<P>, I, O, Op>
where
    Op: StrictUnaryValOperator<I, O>,
    I: Data,
    O: Data,
    P: Clone + 'static,
{
    /// Connect `input_stream` as input to the operator.
    /// See [`Circuit::add_feedback`] for details.
    /// Returns node id of the input node.
    // TODO: The return value won't be needed once we have schedulers.
    pub fn connect(self, input_stream: &Stream<Circuit<P>, I>) -> NodeId {
        self.circuit.connect_feedback(self.operator, input_stream)
    }
}

#[cfg(test)]
mod tests {
    use super::Circuit;
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
        let circuit = Circuit::new();
        let source = circuit.add_source(Counter::new());
        let integrator = circuit.add_unary_ref_operator(Integrator::new(), &source);
        let sinkid1 = circuit.add_ref_sink(Printer::new(), &integrator);
        let sinkid2 = circuit.add_ref_sink(
            Inspect::new(move |n| actual_output_clone.borrow_mut().push(*n)),
            &integrator,
        );

        for _ in 0..100 {
            circuit.eval(source.node_id());
            circuit.eval(integrator.node_id());
            circuit.eval(sinkid1);
            circuit.eval(sinkid2);
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
        let circuit = Circuit::new();
        let source = circuit.add_source(Counter::new());
        let (z1_output, z1_feedback) = circuit.add_feedback(Z1::new());
        let plus = circuit.add_binary_refref_operator(Plus::new(), &source, &z1_output);
        let sinkid = circuit.add_ref_sink(
            Inspect::new(move |n| actual_output_clone.borrow_mut().push(*n)),
            &plus,
        );
        let z1_input_id = z1_feedback.connect(&plus);

        for _ in 0..100 {
            circuit.eval(z1_output.node_id());
            circuit.eval(source.node_id());
            circuit.eval(plus.node_id());
            circuit.eval(z1_input_id);
            circuit.eval(sinkid);
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
