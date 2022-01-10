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
//! use dbsp::circuit::{operator::{Inspect, Repeat}, Root};
//!
//! let root = Root::build(|circuit| {
//!     // Add a source operator.
//!     let source_stream = circuit.add_source(Repeat::new("Hello, world!".to_owned()));
//!
//!     // Add a sink operator and wire the source directly to it.
//!     circuit.add_sink(
//!         Inspect::new(|n| println!("New output: {}", n)),
//!         &source_stream
//!     );
//! });
//! ```

use std::{
    cell::{Ref, RefCell, RefMut, UnsafeCell},
    fmt,
    fmt::{Debug, Display, Write},
    marker::PhantomData,
    ops::Deref,
    rc::Rc,
};

use super::{
    operator_traits::{
        BinaryOperator, Data, SinkOperator, SourceOperator, StrictUnaryOperator, UnaryOperator,
    },
    schedule::{IterativeScheduler, OnceScheduler, Scheduler},
};

/// A stream stores the output of an operator.  Circuits are synchronous, meaning
/// that each value is produced and consumed in the same clock cycle, so there can
/// be at most one value in the stream at any time.
pub struct Stream<C, D> {
    /// Id of the operator that writes to the stream.
    /// `None` if this is a parent stream introduced to the child circuit using `enter`.
    node_id: Option<NodeId>,
    /// Circuit that this stream belongs to.
    circuit: C,
    /// The value (there can be at most one since our circuits are synchronous).
    /// We use `UnsafeCell` instead of `RefCell` to avoid runtime ownership tests.
    /// We enforce unique ownership by making sure that at most one operator can
    /// run (and access the stream) at any time.
    val: Rc<UnsafeCell<Option<D>>>,
}

impl<C, D> Clone for Stream<C, D>
where
    C: Clone,
{
    fn clone(&self) -> Self {
        Self {
            node_id: self.node_id,
            circuit: self.circuit.clone(),
            val: self.val.clone(),
        }
    }
}

impl<C, D> Stream<C, D> {
    /// Returns id of the operator that writes to this stream or `None`
    /// if the stream originates in the parent circuit.
    pub fn node_id(&self) -> Option<NodeId> {
        self.node_id
    }
}

impl<P, D> Stream<Circuit<P>, D>
where
    P: Clone + 'static,
{
    /// Imports stream from the parent circuit to a nested circuit.
    /// The stream will contain the same value throughout a parent
    /// clock tick, even if the nested circuit iterates multiple times
    /// for each parent clock cycle.
    pub fn enter(&self, circuit: &Circuit<Circuit<P>>) -> Stream<Circuit<Circuit<P>>, D> {
        self.circuit.connect_stream(self, circuit.node_id());
        Stream {
            node_id: None,
            circuit: circuit.clone(),
            val: self.val.clone(),
        }
    }

    /// Exports stream from a nested circuit to its parent circuit.
    /// The child can update the value in the stream multiple times
    /// per parent clock tick, but only the final value is visible
    /// to the parent.
    pub fn leave(&self) -> Stream<P, D> {
        Stream {
            node_id: Some(self.circuit.node_id()),
            circuit: self.circuit.parent(),
            val: self.val.clone(),
        }
    }
}

// Internal streams API only used inside this module.
impl<C, D> Stream<C, D> {
    // Create a new stream within the given circuit, connected to the specified node id.
    fn new(circuit: C, node_id: NodeId) -> Self {
        Self {
            node_id: Some(node_id),
            circuit,
            val: Rc::new(UnsafeCell::new(None)),
        }
    }

    /// Returns `Some` if the operator has produced output for the current timestamp and `None`
    /// otherwise.
    ///
    /// #Safety
    ///
    /// The caller must have exclusive access to the current stream.
    pub(super) unsafe fn get(&self) -> &Option<D> {
        &*self.val.get()
    }

    /// Puts a value in the stream, overwriting the previous value if any.
    ///
    /// #Safety
    ///
    /// The caller must have exclusive access to the current stream.
    unsafe fn put(&self, val: D) {
        *self.val.get() = Some(val);
    }

    /*unsafe fn take(&self) -> Option<D> {
        let mut val = None;
        swap(&mut *self.val.get(), &mut val);
        val
    }*/

    // Remove the value in the stream, if any, leaving the stream empty.
    /*unsafe fn clear(&self) {
        *self.val.get() = None;
    }*/
}

/// Node in a circuit.  A node wraps an operator with strongly typed
/// input and output streams.
pub(crate) trait Node {
    /// Node id unique within its parent circuit.
    fn id(&self) -> NodeId;

    /// Evaluate the operator.  Reads one value from each input stream
    /// and pushes a new value to the output stream (except for sink
    /// operators, which don't have an output stream).
    ///
    /// # Safety
    ///
    /// Only one node may be scheduled at any given time (a node cannot invoke
    /// another node).
    unsafe fn eval(&mut self);

    // TODO: we need to fix the terminology along with method names.
    // This is really the start/end of a clock epoch.
    /// Notify the node about start of input streams.  The node
    /// should forward the notification to its inner operator.
    fn stream_start(&mut self);

    /// Notify the node about end of input streams.  The node
    /// should forward the notification to its inner operator.
    ///
    /// # Safety
    ///
    /// Only one node may be scheduled at any given time (a node cannot invoke
    /// another node).
    unsafe fn stream_end(&mut self);
}

/// Id of an operator, guaranteed to be unique within a circuit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct NodeId(usize);

impl Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_char('n')?;
        Debug::fmt(&self.0, f)
    }
}

/// A circuit consists of nodes and edges.  An edge from
/// node1 to node2 indicates that the output stream of node1
/// is connected to an input of node2.
struct CircuitInner<P> {
    parent: P,
    // Circuit's node id within the parent circuit.
    node_id: NodeId,
    nodes: Vec<Box<dyn Node>>,
    edges: Vec<(NodeId, NodeId)>,
}

impl<P> CircuitInner<P> {
    fn new(parent: P, node_id: NodeId) -> Self {
        Self {
            node_id,
            parent,
            nodes: Vec::new(),
            edges: Vec::new(),
        }
    }

    fn add_edge(&mut self, from: Option<NodeId>, to: NodeId) {
        if let Some(from) = from {
            self.edges.push((from, to));
        }
    }

    fn add_node<N>(&mut self, node: N)
    where
        N: Node + 'static,
    {
        self.nodes.push(Box::new(node) as Box<dyn Node>);
    }

    fn clear(&mut self) {
        self.nodes.clear();
        self.edges.clear();
    }
}

/// A handle to a circuit.
#[repr(transparent)]
pub struct Circuit<P>(Rc<RefCell<CircuitInner<P>>>);

impl<P> Clone for Circuit<P> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl Circuit<()> {
    // Create new top-level circuit.  Clients invoke this via the [`Root::build`] API.
    fn new() -> Self {
        Self::with_parent((), NodeId(0))
    }
}

// Internal API.
impl<P> Circuit<P> {
    /// Create an empty nested circuit of `parent`.
    fn with_parent(parent: P, id: NodeId) -> Self {
        Circuit(Rc::new(RefCell::new(CircuitInner::new(parent, id))))
    }

    /// Mutably borrow the inner circuit.
    fn inner_mut(&self) -> RefMut<'_, CircuitInner<P>> {
        self.0.borrow_mut()
    }

    /// Immutably borrow the inner circuit.
    fn inner(&self) -> Ref<'_, CircuitInner<P>> {
        self.0.borrow()
    }

    /// Circuit's node id within the parent circuit.  Returns 0 for the root circuit.
    fn node_id(&self) -> NodeId {
        let circuit = self.inner();
        circuit.node_id
    }

    /// Connect `stream` as input to `to`.
    fn connect_stream<T>(&self, stream: &Stream<Self, T>, to: NodeId) {
        debug_assert_eq!(self.node_id(), stream.circuit.node_id());
        let mut circuit = self.inner_mut();
        circuit.add_edge(stream.node_id(), to);
    }

    /// Add a node to the circuit.  Allocates a new node id and invokes a user
    /// callback to create a new node instance.  The callback may use the node id,
    /// e.g., to add an edge to this node.
    fn add_node<F, N, T>(&self, f: F) -> T
    where
        F: FnOnce(NodeId) -> (N, T),
        N: Node + 'static,
    {
        let id = self.inner().nodes.len();

        // We don't hold a reference to `self.inner()` while calling `f`, so it can
        // safely modify the circuit, e.g., add edges.
        let (node, res) = f(NodeId(id));
        self.inner_mut().add_node(node);
        res
    }

    pub(super) fn edges(&self) -> Ref<'_, [(NodeId, NodeId)]> {
        let circuit = self.inner();
        Ref::map(circuit, |c| c.edges.as_slice())
    }

    // Deliver `stream_start` notification to all nodes in the circuit.
    pub(super) fn stream_start(&self) {
        let mut circuit = self.inner_mut();
        for node in circuit.nodes.iter_mut() {
            node.stream_start();
        }
    }

    // Deliver `stream_end` notification to all nodes in the circuit.
    pub(super) unsafe fn stream_end(&self) {
        let mut circuit = self.inner_mut();
        for node in circuit.nodes.iter_mut() {
            node.stream_end();
        }
    }

    fn clear(&mut self) {
        self.inner_mut().clear();
    }
}

impl<P> Circuit<P>
where
    P: 'static + Clone,
{
    /// Returns the parent circuit of `self`.
    pub fn parent(&self) -> P {
        self.inner().parent.clone()
    }

    /// Evaluate operator with the given id.
    ///
    /// This method should only be used by schedulers.
    pub(crate) fn eval_node(&self, id: NodeId) {
        let mut circuit = self.inner_mut();

        // Safety: `eval` cannot invoke the
        // `eval` method of another node.  To circumvent
        // this invariant the user would have to extract a
        // reference to a node and pass it to an operator,
        // but this module doesn't expose nodes, only
        // streams.
        unsafe { circuit.nodes[id.0].eval() };
    }

    /// Add a source operator to the circuit.  See [`SourceOperator`].
    pub fn add_source<O, Op>(&self, operator: Op) -> Stream<Self, O>
    where
        O: Data,
        Op: SourceOperator<O>,
    {
        self.add_node(|id| {
            let node = SourceNode::new(operator, self.clone(), id);
            let output_stream = node.output_stream();
            (node, output_stream)
        })
    }

    /// Add a sink operator that consumes input values by reference.
    /// See [`SinkOperator`].
    pub fn add_sink<I, Op>(&self, operator: Op, input_stream: &Stream<Self, I>)
    where
        I: Data,
        Op: SinkOperator<I>,
    {
        self.add_node(|id| {
            self.connect_stream(input_stream, id);
            (SinkNode::new(operator, input_stream.clone(), self.clone(), id), ())
        });
    }

    /// Add a unary operator that consumes input values by reference.
    /// See [`UnaryOperator`].
    pub fn add_unary_operator<I, O, Op>(
        &self,
        operator: Op,
        input_stream: &Stream<Self, I>,
    ) -> Stream<Self, O>
    where
        I: Data,
        O: Data,
        Op: UnaryOperator<I, O>,
    {
        self.add_node(|id| {
            let node = UnaryNode::new(operator, input_stream.clone(), self.clone(), id);
            let output_stream = node.output_stream();
            self.connect_stream(input_stream, id);
            (node, output_stream)
        })
    }

    /// Add a binary operator that consumes both inputs by reference.
    /// See [`BinaryOperator`].
    pub fn add_binary_operator<I1, I2, O, Op>(
        &self,
        operator: Op,
        input_stream1: &Stream<Self, I1>,
        input_stream2: &Stream<Self, I2>,
    ) -> Stream<Self, O>
    where
        I1: Data,
        I2: Data,
        O: Data,
        Op: BinaryOperator<I1, I2, O>,
    {
        self.add_node(|id| {
            let node = BinaryNode::new(operator, input_stream1.clone(), input_stream2.clone(), self.clone(), id);
            let output_stream = node.output_stream();
            self.connect_stream(input_stream1, id);
            self.connect_stream(input_stream2, id);
            (node, output_stream)
        })
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
    /// the (`+`) operator computes the sum of the new value received from source and the value
    /// stored in `z1`.
    ///
    /// ```text
    ///                ┌─┐
    /// source ───────►│+├───┬─►
    ///           ┌───►└─┘   │
    ///           │          │
    ///           │    ┌──┐  │
    ///           └────┤z1│◄─┘
    ///                └──┘
    /// ```
    ///
    /// ```
    /// # use dbsp::circuit::{
    /// #   operator::{Z1, Map2, Repeat},
    /// #   Root,
    /// # };
    /// # let root = Root::build(|circuit| {
    /// // Create a data source.
    /// let source = circuit.add_source(Repeat::new(10));
    /// // Create z1.  `z1_output` will contain the output stream of `z1`; `z1_feedback`
    /// // is a placeholder where we can later plug the input to `z1`.
    /// let (z1_output, z1_feedback) = circuit.add_feedback(Z1::new(0));
    /// // Connect outputs of `source` and `z1` to the plus operator.
    /// let plus = circuit.add_binary_operator(Map2::new(|n1: &usize, n2: &usize| n1 + n2), &source, &z1_output);
    /// // Connect the output of `+` as input to `z1`.
    /// z1_feedback.connect(&plus);
    /// # });
    /// ```
    pub fn add_feedback<I, O, Op>(
        &self,
        operator: Op,
    ) -> (Stream<Self, O>, FeedbackConnector<Self, I, O, Op>)
    where
        I: Data,
        O: Data,
        Op: StrictUnaryOperator<I, O>,
    {
        self.add_node(|id| {
            let operator = Rc::new(UnsafeCell::new(operator));
            let connector = FeedbackConnector::new(self.clone(), operator.clone());
            let output_node = FeedbackOutputNode::new(operator, self.clone(), id);
            let output_stream = output_node.output_stream();
            (output_node, (output_stream, connector))
        })
    }

    fn connect_feedback<I, O, Op>(
        &self,
        operator: Rc<UnsafeCell<Op>>,
        input_stream: &Stream<Self, I>,
    ) where
        I: Data,
        O: Data,
        Op: StrictUnaryOperator<I, O>,
    {
        self.add_node(|id| {
            let output_node = FeedbackInputNode::new(operator, input_stream.clone(), id);
            self.connect_stream(input_stream, id);
            (output_node, ())
        });
    }

    /// Add a child circuit.
    ///
    /// Creates an empty circuit with `self` as parent and invokes `child_constructor` to populate
    /// the circuit.  `child_constructor` typically captures some of the streams in `self` and
    /// connects them to source nodes of the child circuit.  It is also responsible for attaching
    /// a scheduler to the child circuit.  The return type `T` will typically contain output
    /// streams of the child.
    ///
    /// Most users should invoke higher-level APIs like [`iterate`] instead of using this method
    /// directly.
    pub(crate) fn subcircuit<F, T, S>(&self, child_constructor: F) -> T
    where
        F: FnOnce(&mut Circuit<Self>) -> (T, S),
        S: Scheduler<Self>,
    {
        self.add_node(|id| {
            let mut child_circuit = Circuit::with_parent(self.clone(), id);
            let (res, scheduler) = child_constructor(&mut child_circuit);
            let child = <ChildNode<Self, S>>::new(child_circuit, scheduler, id);
            (child, res)
        })
    }

    /// Add an iteratively scheduled child circuit.
    ///
    /// Add a child circuit with a nested clock.  The child will execute multiple times for each
    /// parent timestamp, until its termination condition is satisfied.  Specifically, this
    /// function attaches an iterative scheduler to the child.  Every time the child circuit is
    /// activated by the parent (once per parent timestamp), the scheduler calls `stream_start` on
    /// each child operator.  It then calls `eval` on all child operators in a causal order and
    /// checks if the termination condition is satisfied by reading from a user-specified
    /// "termination" stream.  If the value in the stream is `false`, the scheduler `eval`s all
    /// operators again.  Once the termination condition is `true`, the scheduler calls
    /// `stream_end` on all child operators and returns control back to the parent scheduler.
    ///
    /// The `constructor` closure populates the child circuit and returns the termination stream
    /// used by the scheduler to check termination condition on each iteration and an arbitrary
    /// user-defined return value that typically contains output streams of the child.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::{cell::RefCell, rc::Rc};
    /// use dbsp::circuit::{
    ///     Root,
    ///     operator::{Generator, Inspect, Map, Map2, NestedSource, Z1},
    /// };
    ///
    /// let root = Root::build(|circuit| {
    ///     let source = circuit.add_source(Generator::new(1, |n: &mut usize| *n = *n + 1));
    ///     let fact = circuit.iterate(|child| {
    ///         let counter = Rc::new(RefCell::new(0));
    ///         let counter_clone = counter.clone();
    ///         let countdown = child.add_source(NestedSource::new(source, child, move |n: &mut usize| {
    ///             *n = *n - 1;
    ///             *counter_clone.borrow_mut() = *n;
    ///         }));
    ///         let (z1_output, z1_feedback) = child.add_feedback(Z1::new(1));
    ///         let mul = child.add_binary_operator(
    ///             Map2::new(|n1: &usize, n2: &usize| n1 * n2),
    ///             &countdown,
    ///             &z1_output,
    ///         );
    ///         z1_feedback.connect(&mul);
    ///         (move || *counter.borrow() <= 1, mul.leave())
    ///     });
    ///     circuit.add_sink(
    ///         Inspect::new(move |n| eprintln!("Output: {}", n)),
    ///         &fact,
    ///     );
    /// });
    /// ```
    pub fn iterate<F, C, T>(&self, constructor: F) -> T
    where
        F: FnOnce(&mut Circuit<Self>) -> (C, T),
        C: Fn() -> bool + 'static,
    {
        self.subcircuit(|child| {
            let (termination_check, res) = constructor(child);
            let scheduler = IterativeScheduler::new(child, termination_check);
            (res, scheduler)
        })
    }
}

struct SourceNode<C, O, Op> {
    id: NodeId,
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
            id,
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
    fn id(&self) -> NodeId {
        self.id
    }

    unsafe fn eval(&mut self) {
        self.output_stream.put(self.operator.eval());
    }

    fn stream_start(&mut self) {
        self.operator.stream_start();
    }

    unsafe fn stream_end(&mut self) {
        self.operator.stream_end();
    }
}

struct UnaryNode<C, I, O, Op> {
    id: NodeId,
    operator: Op,
    input_stream: Stream<C, I>,
    output_stream: Stream<C, O>,
}

impl<C, I, O, Op> UnaryNode<C, I, O, Op>
where
    Op: UnaryOperator<I, O>,
    C: Clone,
{
    fn new(operator: Op, input_stream: Stream<C, I>, circuit: C, id: NodeId) -> Self {
        Self {
            id,
            operator,
            input_stream,
            output_stream: Stream::new(circuit, id),
        }
    }

    fn output_stream(&self) -> Stream<C, O> {
        self.output_stream.clone()
    }
}

impl<C, I, O, Op> Node for UnaryNode<C, I, O, Op>
where
    Op: UnaryOperator<I, O>,
{
    fn id(&self) -> NodeId {
        self.id
    }

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
    }
}

struct SinkNode<C, I, Op> {
    id: NodeId,
    operator: Op,
    input_stream: Stream<C, I>,
}

impl<C, I, Op> SinkNode<C, I, Op>
where
    Op: SinkOperator<I>,
{
    fn new(operator: Op, input_stream: Stream<C, I>, _circuit: C, id: NodeId) -> Self {
        Self {
            id,
            operator,
            input_stream,
        }
    }
}

impl<C, I, Op> Node for SinkNode<C, I, Op>
where
    Op: SinkOperator<I>,
{
    fn id(&self) -> NodeId {
        self.id
    }

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

struct BinaryNode<C, I1, I2, O, Op> {
    id: NodeId,
    operator: Op,
    input_stream1: Stream<C, I1>,
    input_stream2: Stream<C, I2>,
    output_stream: Stream<C, O>,
}

impl<C, I1, I2, O, Op> BinaryNode<C, I1, I2, O, Op>
where
    Op: BinaryOperator<I1, I2, O>,
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
            id,
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

impl<C, I1, I2, O, Op> Node for BinaryNode<C, I1, I2, O, Op>
where
    Op: BinaryOperator<I1, I2, O>,
{
    fn id(&self) -> NodeId {
        self.id
    }

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
    }
}

// The output half of a feedback node.  We implement a feedback node using a pair of nodes:
// `FeedbackOutputNode` is connected to the circuit as a source node (i.e., it does not have
// an input stream) and thus gets evaluated first in each time stamp.  `FeedbackInputNode`
// is a sink node.  This way the circuit graph remains acyclic and can be scheduled in a
// topological order.
struct FeedbackOutputNode<C, I, O, Op> {
    id: NodeId,
    operator: Rc<UnsafeCell<Op>>,
    output_stream: Stream<C, O>,
    phantom_input: PhantomData<I>,
}

impl<C, I, O, Op> FeedbackOutputNode<C, I, O, Op>
where
    C: Clone,
    Op: StrictUnaryOperator<I, O>,
{
    fn new(operator: Rc<UnsafeCell<Op>>, circuit: C, id: NodeId) -> Self {
        Self {
            id,
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
    Op: StrictUnaryOperator<I, O>,
{
    fn id(&self) -> NodeId {
        self.id
    }

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
    }
}

struct FeedbackInputNode<C, I, O, Op> {
    id: NodeId,
    operator: Rc<UnsafeCell<Op>>,
    input_stream: Stream<C, I>,
    phantom_output: PhantomData<O>,
}

impl<C, I, O, Op> FeedbackInputNode<C, I, O, Op>
where
    Op: StrictUnaryOperator<I, O>,
{
    fn new(operator: Rc<UnsafeCell<Op>>, input_stream: Stream<C, I>, id: NodeId) -> Self {
        Self {
            id,
            operator,
            input_stream,
            phantom_output: PhantomData,
        }
    }
}

impl<C, I, O, Op> Node for FeedbackInputNode<C, I, O, Op>
where
    Op: StrictUnaryOperator<I, O>,
    I: Data,
{
    fn id(&self) -> NodeId {
        self.id
    }

    unsafe fn eval(&mut self) {
        (&mut *self.operator.get()).eval_strict(
            self.input_stream
                .get()
                .deref()
                .as_ref()
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
    Op: StrictUnaryOperator<I, O>,
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
    Op: StrictUnaryOperator<I, O>,
    I: Data,
    O: Data,
    P: Clone + 'static,
{
    /// Connect `input_stream` as input to the operator.
    /// See [`Circuit::add_feedback`] for details.
    /// Returns node id of the input node.
    pub fn connect(self, input_stream: &Stream<Circuit<P>, I>) {
        self.circuit.connect_feedback(self.operator, input_stream);
    }
}

// A nested circuit instantiated as a node in a parent circuit.
struct ChildNode<P, S> {
    id: NodeId,
    circuit: Circuit<P>,
    scheduler: S,
}

impl<P, S> Drop for ChildNode<P, S> {
    fn drop(&mut self) {
        // Explicitly deallocate all nodes in the circuit to break
        // cyclic `Rc` references between circuits and streams.
        self.circuit.clear();
    }
}

impl<P, S> ChildNode<P, S> {
    fn new(circuit: Circuit<P>, scheduler: S, id: NodeId) -> Self {
        Self {
            id,
            circuit,
            scheduler,
        }
    }
}

impl<P, S> Node for ChildNode<P, S>
where
    S: Scheduler<P>,
{
    fn id(&self) -> NodeId {
        self.id
    }

    unsafe fn eval(&mut self) {
        self.scheduler.run(&self.circuit);
    }

    fn stream_start(&mut self) {
        self.circuit.stream_start();
    }

    unsafe fn stream_end(&mut self) {
        self.circuit.stream_end();
    }
}

/// Top-level circuit with scheduler.
pub struct Root {
    circuit: Circuit<()>,
    scheduler: OnceScheduler,
}

impl Drop for Root {
    fn drop(&mut self) {
        unsafe {
            self.circuit.stream_end();
        }
        // We must explicitly deallocate all nodes in the circuit to break
        // cyclic `Rc` references between circuits and streams.  Alternatively,
        // we could use weak references to break cycles, but we'd have to
        // pay the cost of upgrading weak references on each access.
        self.circuit.clear();
    }
}

impl Root {
    /// Create an empty circuit and populate it with operators by calling a user-provided
    /// constructor function.  Attach a scheduler to the resulting circuit.
    pub fn build<F>(constructor: F) -> Self
    where
        F: FnOnce(&mut Circuit<()>),
    {
        let mut circuit = Circuit::new();
        constructor(&mut circuit);
        let scheduler = OnceScheduler::new(&circuit);

        // Alternatively, `Root` should expose `stream_start` and `stream_end` APIs, so that the
        // user can reset the circuit at runtime and start evaluation from clean state without
        // having to rebuild it from scratch.
        circuit.stream_start();
        Self { circuit, scheduler }
    }

    /// This function drives the execution of the circuit.  Every call corresponds to one tick of
    /// the global logical clock and causes each operator in the circuit to get evaluated once,
    /// consuming one value from each of its input streams.
    pub fn step(&self) {
        // TODO: We need a protocol to make sure that all sources have data available before
        // running the circuit and to either block or fail if they don't.
        self.scheduler.run(&self.circuit);
    }
}

#[cfg(test)]
mod tests {
    use super::Root;
    use crate::circuit::{
        operator::{Generator, Inspect, Map2, NestedSource, Z1},
        operator_traits::{Operator, SinkOperator, UnaryOperator},
    };
    use std::{cell::RefCell, fmt::Display, marker::PhantomData, ops::Deref, rc::Rc, vec::Vec};

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
    impl UnaryOperator<usize, usize> for Integrator {
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
    impl<T: Display + 'static> SinkOperator<T> for Printer<T> {
        fn eval(&mut self, i: &T) {
            println!("new output: {}", i);
        }
    }

    // Compute the sum of numbers from 0 to 99.
    #[test]
    fn sum_circuit() {
        let actual_output: Rc<RefCell<Vec<usize>>> = Rc::new(RefCell::new(Vec::with_capacity(100)));
        let actual_output_clone = actual_output.clone();
        let root = Root::build(|circuit| {
            let source = circuit.add_source(Generator::new(0, |n: &mut usize| *n = *n + 1));
            let integrator = circuit.add_unary_operator(Integrator::new(), &source);
            circuit.add_sink(Printer::new(), &integrator);
            circuit.add_sink(
                Inspect::new(move |n| actual_output_clone.borrow_mut().push(*n)),
                &integrator,
            );
        });

        for _ in 0..100 {
            root.step()
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

        let root = Root::build(|circuit| {
            let source = circuit.add_source(Generator::new(0, |n: &mut usize| *n = *n + 1));
            let (z1_output, z1_feedback) = circuit.add_feedback(Z1::new(0));
            let plus = circuit.add_binary_operator(
                Map2::new(|n1: &usize, n2: &usize| *n1 + *n2),
                &source,
                &z1_output,
            );
            circuit.add_sink(
                Inspect::new(move |n| actual_output_clone.borrow_mut().push(*n)),
                &plus,
            );
            z1_feedback.connect(&plus);
        });

        for _ in 0..100 {
            root.step();
        }

        let mut sum = 0;
        let mut expected_output: Vec<usize> = Vec::with_capacity(100);
        for i in 0..100 {
            sum += i;
            expected_output.push(sum);
        }
        assert_eq!(&expected_output, actual_output.borrow().deref());
    }

    // Nested circuit.  The root circuit contains a source node that counts up from 1.  For
    // each `n` output by the source node, the nested circuit computes factorial(n) using a
    // `NestedSource` operator that counts from n down to `1` and a multiplier that multiplies
    // the next count by the product computed so far (stored in z-1).
    #[test]
    fn factorial() {
        let actual_output: Rc<RefCell<Vec<usize>>> = Rc::new(RefCell::new(Vec::with_capacity(100)));
        let actual_output_clone = actual_output.clone();

        let root = Root::build(|circuit| {
            let source = circuit.add_source(Generator::new(1, |n: &mut usize| *n = *n + 1));
            let fact = circuit.iterate(|child| {
                let counter = Rc::new(RefCell::new(0));
                let counter_clone = counter.clone();
                let countdown =
                    child.add_source(NestedSource::new(source, child, move |n: &mut usize| {
                        *n = *n - 1;
                        *counter_clone.borrow_mut() = *n;
                    }));
                let (z1_output, z1_feedback) = child.add_feedback(Z1::new(1));
                let mul = child.add_binary_operator(
                    Map2::new(|n1: &usize, n2: &usize| n1 * n2),
                    &countdown,
                    &z1_output,
                );
                z1_feedback.connect(&mul);
                (move || *counter.borrow() <= 1, mul.leave())
            });
            circuit.add_sink(
                Inspect::new(move |n| actual_output_clone.borrow_mut().push(*n)),
                &fact,
            );
        });

        for _ in 1..10 {
            root.step();
        }

        let mut expected_output: Vec<usize> = Vec::with_capacity(10);
        for i in 1..10 {
            expected_output.push(my_factorial(i));
        }
        assert_eq!(&expected_output, actual_output.borrow().deref());
    }

    fn my_factorial(n: usize) -> usize {
        if n == 1 {
            1
        } else {
            n * my_factorial(n - 1)
        }
    }
}
