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
    collections::HashMap,
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
    trace::{CircuitEvent, SchedulerEvent},
};

/// A stream stores the output of an operator.  Circuits are synchronous, meaning
/// that each value is produced and consumed in the same clock cycle, so there can
/// be at most one value in the stream at any time.
pub struct Stream<C, D> {
    /// Id of the operator within the local circuit that writes to the stream.
    /// `None` if this is a stream introduced to the child circuit from its parent
    /// circuit using `enter`, in which case the stream does not have a local source.
    local_node_id: Option<NodeId>,
    /// Global id of the node that writes to this stream.
    origin_node_id: GlobalNodeId,
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
            local_node_id: self.local_node_id,
            origin_node_id: self.origin_node_id.clone(),
            circuit: self.circuit.clone(),
            val: self.val.clone(),
        }
    }
}

impl<C, D> Stream<C, D> {
    /// Returns id of the operator that writes to this stream or `None`
    /// if the stream originates in the parent circuit.
    pub fn local_node_id(&self) -> Option<NodeId> {
        self.local_node_id
    }

    /// Returns global id of the operator that writes to this stream.
    pub fn origin_node_id(&self) -> &GlobalNodeId {
        &self.origin_node_id
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
            local_node_id: None,
            origin_node_id: self.origin_node_id.clone(),
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
            local_node_id: Some(self.circuit.node_id()),
            origin_node_id: self.origin_node_id.clone(),
            circuit: self.circuit.parent(),
            val: self.val.clone(),
        }
    }
}

// Internal streams API only used inside this module.
impl<P, D> Stream<Circuit<P>, D> {
    // Create a new stream within the given circuit, connected to the specified node id.
    fn new(circuit: Circuit<P>, node_id: NodeId) -> Self {
        Self {
            local_node_id: Some(node_id),
            origin_node_id: GlobalNodeId::child_of(&circuit, node_id),
            circuit,
            val: Rc::new(UnsafeCell::new(None)),
        }
    }
}

impl<C, D> Stream<C, D> {
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

    /// `true` if the node encapsulates an asynchronous operator (see [`Operator::is_async()`]).
    /// `false` for synchronous operators and subcircuits.
    fn is_async(&self) -> bool;

    /// `true` if the node is ready to execute (see [`Operator::ready()`]).
    /// Always returns `true` for synchronous operators and subcircuits.
    fn ready(&self) -> bool;

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
    fn clock_start(&mut self);

    /// Notify the node about end of input streams.  The node
    /// should forward the notification to its inner operator.
    ///
    /// # Safety
    ///
    /// Only one node may be scheduled at any given time (a node cannot invoke
    /// another node).
    unsafe fn clock_end(&mut self);
}

/// Id of an operator, guaranteed to be unique within a circuit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct NodeId(usize);

impl NodeId {
    pub fn new(id: usize) -> Self {
        Self(id)
    }

    pub(super) fn root() -> Self {
        Self(0)
    }
}

impl Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_char('n')?;
        Debug::fmt(&self.0, f)
    }
}

/// Globally unique id of a node (operator or subcircuit).
/// The identifier consists of a path from the top-level circuit to the node.
/// The top-level circuit has global id `[]`, an operator in the top-level
/// circuit or a sub-circuit nested inside the top-level circuit will have a
/// path of length 1, e.g., `[5]`, an operator inside the nested circuit
/// will have a path of length 2, e.g., `[5, 1]`, etc.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct GlobalNodeId(Vec<NodeId>);

impl Display for GlobalNodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[")?;
        let path = self.path();
        for i in 0..path.len() {
            f.write_str(&path[i].0.to_string())?;
            if i < path.len() - 1 {
                f.write_str(".")?;
            }
        }
        f.write_str("]")
    }
}

impl GlobalNodeId {
    /// Generate global node id from path.
    pub fn from_path(path: &[NodeId]) -> Self {
        Self(path.to_owned())
    }

    /// Generate global node id from path.
    pub fn from_path_vec(path: Vec<NodeId>) -> Self {
        Self(path)
    }

    /// Generate global node id by appending `child_id` to `self`.
    pub fn child(&self, child_id: NodeId) -> Self {
        let mut path = Vec::with_capacity(self.path().len() + 1);
        for id in self.path() {
            path.push(*id);
        }
        path.push(child_id);
        Self(path)
    }

    /// Generate global node id for a child node of `circuit`.
    pub fn child_of<P>(circuit: &Circuit<P>, node_id: NodeId) -> Self {
        let mut ids = circuit.global_node_id().path().to_owned();
        ids.push(node_id);
        Self(ids)
    }

    /// Get the path from global.
    pub fn path(&self) -> &[NodeId] {
        &self.0
    }
}

type CircuitEventHandler = Box<dyn Fn(&CircuitEvent)>;
type SchedulerEventHandler = Box<dyn Fn(&SchedulerEvent)>;
type CircuitEventHandlers = Rc<RefCell<HashMap<String, CircuitEventHandler>>>;
type SchedulerEventHandlers = Rc<RefCell<HashMap<String, SchedulerEventHandler>>>;

/// A circuit consists of nodes and edges.  An edge from
/// node1 to node2 indicates that the output stream of node1
/// is connected to an input of node2.
struct CircuitInner<P> {
    parent: P,
    // Circuit's node id within the parent circuit.
    node_id: NodeId,
    global_node_id: GlobalNodeId,
    nodes: Vec<Box<dyn Node>>,
    edges: Vec<(NodeId, NodeId)>,
    circuit_event_handlers: CircuitEventHandlers,
    scheduler_event_handlers: SchedulerEventHandlers,
}

impl<P> CircuitInner<P> {
    fn new(
        parent: P,
        node_id: NodeId,
        global_node_id: GlobalNodeId,
        circuit_event_handlers: CircuitEventHandlers,
        scheduler_event_handlers: SchedulerEventHandlers,
    ) -> Self {
        Self {
            node_id,
            global_node_id,
            parent,
            nodes: Vec::new(),
            edges: Vec::new(),
            circuit_event_handlers,
            scheduler_event_handlers,
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

    fn register_circuit_event_handler<F>(&mut self, name: &str, handler: F)
    where
        F: Fn(&CircuitEvent) + 'static,
    {
        self.circuit_event_handlers.borrow_mut().insert(
            name.to_string(),
            Box::new(handler) as Box<dyn Fn(&CircuitEvent)>,
        );
    }

    fn unregister_circuit_event_handler(&mut self, name: &str) -> bool {
        self.circuit_event_handlers
            .borrow_mut()
            .remove(name)
            .is_some()
    }

    fn register_scheduler_event_handler<F>(&mut self, name: &str, handler: F)
    where
        F: Fn(&SchedulerEvent) + 'static,
    {
        self.scheduler_event_handlers.borrow_mut().insert(
            name.to_string(),
            Box::new(handler) as Box<dyn Fn(&SchedulerEvent)>,
        );
    }

    fn unregister_scheduler_event_handler(&mut self, name: &str) -> bool {
        self.scheduler_event_handlers
            .borrow_mut()
            .remove(name)
            .is_some()
    }

    fn log_circuit_event(&self, event: &CircuitEvent) {
        for (_, handler) in self.circuit_event_handlers.borrow().iter() {
            handler(event)
        }
    }

    fn log_scheduler_event(&self, event: &SchedulerEvent) {
        for (_, handler) in self.scheduler_event_handlers.borrow().iter() {
            handler(event)
        }
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
        Self(Rc::new(RefCell::new(CircuitInner::new(
            (),
            NodeId::root(),
            GlobalNodeId::from_path(&[]),
            Rc::new(RefCell::new(HashMap::new())),
            Rc::new(RefCell::new(HashMap::new())),
        ))))
    }
}

impl Circuit<()> {
    /// Attach a circuit event handler to the top-level circuit (see [`super::trace::CircuitEvent`] for
    /// a description of circuit events).
    ///
    /// This method should normally be called inside the closure passed to [`Root::build`] before
    /// adding any operators to the circuit, so that the handler gets to observe all nodes, edges,
    /// and subcircuits added to the circuit.
    ///
    /// `name` - user-readable name assigned to the handler.  If a handler with the same name
    /// exists, it will be replaced by the new handler.
    ///
    /// `handler` - user callback invoked on each circuit event (see [`super::trace::CircuitEvent`]).
    ///
    /// # Examples
    ///
    /// ```text
    /// TODO
    /// ```
    pub fn register_circuit_event_handler<F>(&self, name: &str, handler: F)
    where
        F: Fn(&CircuitEvent) + 'static,
    {
        self.inner_mut()
            .register_circuit_event_handler(name, handler);
    }

    /// Remove a circuit event handler.  Returns `true` if a handler with the specified name had
    /// previously been registered and `false` otherwise.
    pub fn unregister_circuit_event_handler(&self, name: &str) -> bool {
        self.inner_mut().unregister_circuit_event_handler(name)
    }

    /// Attach a scheduler event handler to the top-level circuit (see [`super::trace::SchedulerEvent`] for
    /// a description of scheduler events).
    ///
    /// This method can be used during circuit construction, inside the closure provided to
    /// [`Root::build`].  Use [`Root::register_scheduler_event_handler`],
    /// [`Root::unregister_scheduler_event_handler`] to manipulate scheduler callbacks at runtime.
    ///
    /// `name` - user-readable name assigned to the handler.  If a handler with the same name
    /// exists, it will be replaced by the new handler.
    ///
    /// `handler` - user callback invoked on each scheduler event.
    pub fn register_scheduler_event_handler<F>(&self, name: &str, handler: F)
    where
        F: Fn(&SchedulerEvent) + 'static,
    {
        self.inner_mut()
            .register_scheduler_event_handler(name, handler);
    }

    /// Remove a scheduler event handler.  Returns `true` if a handler with the specified name had
    /// previously been registered and `false` otherwise.
    pub fn unregister_scheduler_event_handler(&self, name: &str) -> bool {
        self.inner_mut().unregister_scheduler_event_handler(name)
    }
}

impl<P> Circuit<Circuit<P>> {
    /// Create an empty nested circuit of `parent`.
    fn with_parent(parent: Circuit<P>, id: NodeId) -> Self {
        let global_node_id = GlobalNodeId::child_of(&parent, id);
        let circuit_handlers = parent.inner().circuit_event_handlers.clone();
        let sched_handlers = parent.inner().scheduler_event_handlers.clone();

        Circuit(Rc::new(RefCell::new(CircuitInner::new(
            parent,
            id,
            global_node_id,
            circuit_handlers,
            sched_handlers,
        ))))
    }
}

// Internal API.
impl<P> Circuit<P> {
    /// Mutably borrow the inner circuit.
    fn inner_mut(&self) -> RefMut<'_, CircuitInner<P>> {
        self.0.borrow_mut()
    }

    /// Immutably borrow the inner circuit.
    fn inner(&self) -> Ref<'_, CircuitInner<P>> {
        self.0.borrow()
    }

    /// Circuit's node id within the parent circuit.
    fn node_id(&self) -> NodeId {
        self.inner().node_id
    }

    /// Circuit's global node id.
    pub(super) fn global_node_id(&self) -> GlobalNodeId {
        self.inner().global_node_id.clone()
    }

    /// Connect `stream` as input to `to`.
    fn connect_stream<T>(&self, stream: &Stream<Self, T>, to: NodeId) {
        self.log_circuit_event(&CircuitEvent::stream(
            stream.origin_node_id().clone(),
            GlobalNodeId::child_of(self, to),
        ));

        debug_assert_eq!(self.global_node_id(), stream.circuit.global_node_id());
        self.inner_mut().add_edge(stream.local_node_id(), to);
    }

    /// Register a dependency between `from` and `to` nodes.  A dependency tells the
    /// scheduler that `from` must be evaluated before `to` in each clock cycle even
    /// though there may not be an edge or a path connecting them.
    fn add_dependency(&self, from: NodeId, to: NodeId) {
        self.log_circuit_event(&CircuitEvent::dependency(
            GlobalNodeId::child_of(self, from),
            GlobalNodeId::child_of(self, to),
        ));

        self.inner_mut().add_edge(Some(from), to);
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

    /// Deliver `clock_start` notification to all nodes in the circuit.
    pub(super) fn clock_start(&self) {
        for node in self.inner_mut().nodes.iter_mut() {
            node.clock_start();
        }
    }

    /// Deliver `clock_end` notification to all nodes in the circuit.
    pub(super) unsafe fn clock_end(&self) {
        for node in self.inner_mut().nodes.iter_mut() {
            node.clock_end();
        }
    }

    fn clear(&mut self) {
        self.inner_mut().clear();
    }

    /// Send the specified `CircuitEvent` to all handlers attached to the circuit.
    fn log_circuit_event(&self, event: &CircuitEvent) {
        self.inner().log_circuit_event(event);
    }

    /// Send the specified `SchedulerEvent` to all handlers attached to the circuit.
    pub(super) fn log_scheduler_event(&self, event: &SchedulerEvent) {
        self.inner().log_scheduler_event(event);
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

    pub(crate) fn ready(&self, id: NodeId) -> bool {
        self.inner().nodes[id.0].ready()
    }

    pub(crate) fn is_async_node(&self, id: NodeId) -> bool {
        self.inner().nodes[id.0].is_async()
    }

    /// Evaluate operator with the given id.
    ///
    /// This method should only be used by schedulers.
    pub(crate) fn eval_node(&self, id: NodeId) {
        let mut circuit = self.inner_mut();

        // Notify loggers while holding a reference to the inner circuit.
        // We normally avoid this, since a nested call from event handler
        // will panic in `self.inner()`, but we do it here as an
        // optimization.
        circuit.log_scheduler_event(&SchedulerEvent::eval_start(id));

        // Safety: `eval` cannot invoke the
        // `eval` method of another node.  To circumvent
        // this invariant the user would have to extract a
        // reference to a node and pass it to an operator,
        // but this module doesn't expose nodes, only
        // streams.
        unsafe { circuit.nodes[id.0].eval() };

        circuit.log_scheduler_event(&SchedulerEvent::eval_end(id));
    }

    /// Add a source operator to the circuit.  See [`SourceOperator`].
    pub fn add_source<O, Op>(&self, operator: Op) -> Stream<Self, O>
    where
        O: Data,
        Op: SourceOperator<O>,
    {
        self.add_node(|id| {
            self.log_circuit_event(&CircuitEvent::operator(
                GlobalNodeId::child_of(self, id),
                operator.name(),
            ));
            let node = SourceNode::new(operator, self.clone(), id);
            let output_stream = node.output_stream();
            (node, output_stream)
        })
    }

    /// Add a pair of operators that implement cross-worker communication.
    ///
    /// Operators that exchange data across workers are split into two operators:
    /// the **sender** responsible for partitioning values read from the input stream
    /// and distributing them across workers and the **receiver**, which receives and
    /// reassembles data received from its peers.  Splitting communication into two
    /// halves allows the scheduler to schedule useful work in between them instead of
    /// blocking to wait for the receiver.
    ///
    /// Exchange operators use some form of IPC or shared memory instead of streams to
    /// communicate.  Therefore, the sender must implement trait [`SinkOperator`], while
    /// the receiver implements [`SourceOperator`].
    ///
    /// This function adds both operators to the circuit and registers a dependency
    /// between them, making sure that the scheduler will evaluate the sender before
    /// the receiver even though there is no explicit stream connecting them.
    ///
    /// Returns the output stream produced by the receiver operator.
    ///
    /// # Arguments
    ///
    /// * `sender` - the sender half of the pair.  The sender must be a sink operator
    /// * `receiver` - the receiver half of the pair.  Must be a source
    /// * `input_stream` - stream to connect as input to the `sender`.
    pub fn add_exchange<I, SndOp, O, RcvOp>(
        &self,
        sender: SndOp,
        receiver: RcvOp,
        input_stream: &Stream<Self, I>,
    ) -> Stream<Self, O>
    where
        I: Data,
        O: Data,
        SndOp: SinkOperator<I>,
        RcvOp: SourceOperator<O>,
    {
        let sender_id = self.add_node(|id| {
            self.log_circuit_event(&CircuitEvent::operator(
                GlobalNodeId::child_of(self, id),
                sender.name(),
            ));
            let node = SinkNode::new(sender, input_stream.clone(), self.clone(), id);
            self.connect_stream(input_stream, id);
            (node, id)
        });

        let output_stream = self.add_node(|id| {
            self.log_circuit_event(&CircuitEvent::operator(
                GlobalNodeId::child_of(self, id),
                receiver.name(),
            ));
            let node = SourceNode::new(receiver, self.clone(), id);
            let output_stream = node.output_stream();
            (node, output_stream)
        });

        self.add_dependency(sender_id, output_stream.local_node_id().unwrap());
        output_stream
    }

    /// Add a sink operator that consumes input values by reference.
    /// See [`SinkOperator`].
    pub fn add_sink<I, Op>(&self, operator: Op, input_stream: &Stream<Self, I>)
    where
        I: Data,
        Op: SinkOperator<I>,
    {
        self.add_node(|id| {
            // Log the operator event before the connection event, so that handlers
            // don't observe edges that connect to nodes they haven't seen yet.
            self.log_circuit_event(&CircuitEvent::operator(
                GlobalNodeId::child_of(self, id),
                operator.name(),
            ));

            self.connect_stream(input_stream, id);
            (
                SinkNode::new(operator, input_stream.clone(), self.clone(), id),
                (),
            )
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
            self.log_circuit_event(&CircuitEvent::operator(
                GlobalNodeId::child_of(self, id),
                operator.name(),
            ));

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
            self.log_circuit_event(&CircuitEvent::operator(
                GlobalNodeId::child_of(self, id),
                operator.name(),
            ));

            let node = BinaryNode::new(
                operator,
                input_stream1.clone(),
                input_stream2.clone(),
                self.clone(),
                id,
            );
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
    /// #   operator::{Z1, Apply2, Repeat},
    /// #   Root,
    /// # };
    /// # let root = Root::build(|circuit| {
    /// // Create a data source.
    /// let source = circuit.add_source(Repeat::new(10));
    /// // Create z1.  `z1_output` will contain the output stream of `z1`; `z1_feedback`
    /// // is a placeholder where we can later plug the input to `z1`.
    /// let (z1_output, z1_feedback) = circuit.add_feedback(Z1::new(0));
    /// // Connect outputs of `source` and `z1` to the plus operator.
    /// let plus = circuit.add_binary_operator(Apply2::new(|n1: &usize, n2: &usize| n1 + n2), &source, &z1_output);
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
            self.log_circuit_event(&CircuitEvent::strict_operator_output(
                GlobalNodeId::child_of(self, id),
                operator.name(),
            ));

            let operator = Rc::new(UnsafeCell::new(operator));
            let connector = FeedbackConnector::new(id, self.clone(), operator.clone());
            let output_node = FeedbackOutputNode::new(operator, self.clone(), id);
            let output_stream = output_node.output_stream();
            (output_node, (output_stream, connector))
        })
    }

    fn connect_feedback<I, O, Op>(
        &self,
        output_node_id: NodeId,
        operator: Rc<UnsafeCell<Op>>,
        input_stream: &Stream<Self, I>,
    ) where
        I: Data,
        O: Data,
        Op: StrictUnaryOperator<I, O>,
    {
        self.add_node(|id| {
            self.log_circuit_event(&CircuitEvent::strict_operator_input(
                GlobalNodeId::child_of(self, id),
                output_node_id,
            ));

            let output_node = FeedbackInputNode::new(operator, input_stream.clone(), id);
            self.connect_stream(input_stream, id);
            self.add_dependency(output_node_id, id);
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
    pub(crate) fn subcircuit<F, T, S>(&self, iterative: bool, child_constructor: F) -> T
    where
        F: FnOnce(&mut Circuit<Self>) -> (T, S),
        S: Scheduler<Self>,
    {
        self.add_node(|id| {
            self.log_circuit_event(&CircuitEvent::subcircuit(
                GlobalNodeId::child_of(self, id),
                iterative,
            ));
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
    /// activated by the parent (once per parent timestamp), the scheduler calls `clock_start` on
    /// each child operator.  It then calls `eval` on all child operators in a causal order and
    /// checks if the termination condition is satisfied by reading from a user-specified
    /// "termination" stream.  If the value in the stream is `false`, the scheduler `eval`s all
    /// operators again.  Once the termination condition is `true`, the scheduler calls
    /// `clock_end` on all child operators and returns control back to the parent scheduler.
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
    ///     operator::{Generator, Inspect, Apply2, NestedSource, Z1},
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
    ///             Apply2::new(|n1: &usize, n2: &usize| n1 * n2),
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
        self.subcircuit(true, |child| {
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

impl<P, O, Op> SourceNode<Circuit<P>, O, Op>
where
    Op: SourceOperator<O>,
    P: Clone,
{
    fn new(operator: Op, circuit: Circuit<P>, id: NodeId) -> Self {
        Self {
            id,
            operator,
            output_stream: Stream::new(circuit, id),
        }
    }

    fn output_stream(&self) -> Stream<Circuit<P>, O> {
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

    fn is_async(&self) -> bool {
        self.operator.is_async()
    }

    fn ready(&self) -> bool {
        self.operator.ready()
    }

    unsafe fn eval(&mut self) {
        self.output_stream.put(self.operator.eval());
    }

    fn clock_start(&mut self) {
        self.operator.clock_start();
    }

    unsafe fn clock_end(&mut self) {
        self.operator.clock_end();
    }
}

struct UnaryNode<C, I, O, Op> {
    id: NodeId,
    operator: Op,
    input_stream: Stream<C, I>,
    output_stream: Stream<C, O>,
}

impl<P, I, O, Op> UnaryNode<Circuit<P>, I, O, Op>
where
    Op: UnaryOperator<I, O>,
    P: Clone,
{
    fn new(
        operator: Op,
        input_stream: Stream<Circuit<P>, I>,
        circuit: Circuit<P>,
        id: NodeId,
    ) -> Self {
        Self {
            id,
            operator,
            input_stream,
            output_stream: Stream::new(circuit, id),
        }
    }

    fn output_stream(&self) -> Stream<Circuit<P>, O> {
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

    fn is_async(&self) -> bool {
        self.operator.is_async()
    }

    fn ready(&self) -> bool {
        self.operator.ready()
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

    fn clock_start(&mut self) {
        self.operator.clock_start();
    }

    unsafe fn clock_end(&mut self) {
        self.operator.clock_end();
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

    fn is_async(&self) -> bool {
        self.operator.is_async()
    }

    fn ready(&self) -> bool {
        self.operator.ready()
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

    fn clock_start(&mut self) {
        self.operator.clock_start();
    }

    unsafe fn clock_end(&mut self) {
        self.operator.clock_end();
    }
}

struct BinaryNode<C, I1, I2, O, Op> {
    id: NodeId,
    operator: Op,
    input_stream1: Stream<C, I1>,
    input_stream2: Stream<C, I2>,
    output_stream: Stream<C, O>,
}

impl<P, I1, I2, O, Op> BinaryNode<Circuit<P>, I1, I2, O, Op>
where
    Op: BinaryOperator<I1, I2, O>,
    P: Clone,
{
    fn new(
        operator: Op,
        input_stream1: Stream<Circuit<P>, I1>,
        input_stream2: Stream<Circuit<P>, I2>,
        circuit: Circuit<P>,
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

    fn output_stream(&self) -> Stream<Circuit<P>, O> {
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

    fn is_async(&self) -> bool {
        self.operator.is_async()
    }

    fn ready(&self) -> bool {
        self.operator.ready()
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

    fn clock_start(&mut self) {
        self.operator.clock_start();
    }

    unsafe fn clock_end(&mut self) {
        self.operator.clock_end();
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

impl<P, I, O, Op> FeedbackOutputNode<Circuit<P>, I, O, Op>
where
    P: Clone,
    Op: StrictUnaryOperator<I, O>,
{
    fn new(operator: Rc<UnsafeCell<Op>>, circuit: Circuit<P>, id: NodeId) -> Self {
        Self {
            id,
            operator,
            output_stream: Stream::new(circuit, id),
            phantom_input: PhantomData,
        }
    }

    fn output_stream(&self) -> Stream<Circuit<P>, O> {
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

    fn is_async(&self) -> bool {
        unsafe { &*self.operator.get() }.is_async()
    }

    fn ready(&self) -> bool {
        unsafe { &*self.operator.get() }.ready()
    }

    unsafe fn eval(&mut self) {
        self.output_stream
            .put((&mut *self.operator.get()).get_output());
    }

    fn clock_start(&mut self) {
        unsafe {
            (&mut *self.operator.get()).clock_start();
        }
    }

    unsafe fn clock_end(&mut self) {
        (&mut *self.operator.get()).clock_end();
    }
}

/// The input half of a feedback node
struct FeedbackInputNode<C, I, O, Op> {
    // Id of this node (the input half).
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

    fn is_async(&self) -> bool {
        unsafe { &*self.operator.get() }.is_async()
    }

    fn ready(&self) -> bool {
        unsafe { &*self.operator.get() }.ready()
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

    // Don't call `clock_start`/`clock_end` on the operator.  `FeedbackOutputNode` will do that.
    fn clock_start(&mut self) {}

    unsafe fn clock_end(&mut self) {}
}

/// Input connector of a feedback operator.
///
/// This struct is part of the mechanism for constructing a feedback loop in a circuit.
/// It is returned by [`Circuit::add_feedback`] and represents the input port of an operator
/// whose input stream does not exist yet.  Once the input stream has been created, it
/// can be connected to the operator using [`FeedbackConnector::connect`].
/// See [`Circuit::add_feedback`] for details.
pub struct FeedbackConnector<C, I, O, Op> {
    output_node_id: NodeId,
    circuit: C,
    operator: Rc<UnsafeCell<Op>>,
    phantom_input: PhantomData<I>,
    phantom_output: PhantomData<O>,
}

impl<C, I, O, Op> FeedbackConnector<C, I, O, Op>
where
    Op: StrictUnaryOperator<I, O>,
{
    fn new(output_node_id: NodeId, circuit: C, operator: Rc<UnsafeCell<Op>>) -> Self {
        Self {
            output_node_id,
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
        self.circuit
            .connect_feedback(self.output_node_id, self.operator, input_stream);
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

    fn is_async(&self) -> bool {
        false
    }

    fn ready(&self) -> bool {
        true
    }

    unsafe fn eval(&mut self) {
        self.scheduler.run(&self.circuit);
    }

    fn clock_start(&mut self) {
        self.circuit.clock_start();
    }

    unsafe fn clock_end(&mut self) {
        self.circuit.clock_end();
    }
}

/// Top-level circuit with scheduler.
pub struct Root {
    circuit: Circuit<()>,
    scheduler: OnceScheduler,
}

impl Drop for Root {
    fn drop(&mut self) {
        self.circuit
            .log_scheduler_event(&SchedulerEvent::clock_end());
        unsafe {
            self.circuit.clock_end();
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

        // Alternatively, `Root` should expose `clock_start` and `clock_end` APIs, so that the
        // user can reset the circuit at runtime and start evaluation from clean state without
        // having to rebuild it from scratch.
        circuit.log_scheduler_event(&SchedulerEvent::clock_start());
        circuit.clock_start();
        Self { circuit, scheduler }
    }

    /// This function drives the execution of the circuit.  Every call corresponds to one tick of
    /// the global logical clock and causes each operator in the circuit to get evaluated once,
    /// consuming one value from each of its input streams.
    pub fn step(&self) {
        // TODO: Add a runtime check to prevent re-entering this method from an operator.

        // TODO: We need a protocol to make sure that all sources have data available before
        // running the circuit and to either block or fail if they don't.
        self.scheduler.run(&self.circuit);
    }

    /// Attach a scheduler event handler to the circuit.
    ///
    /// This method is identical to [`Circuit::register_scheduler_event_handler`], but it can be used at runtime,
    /// after the circuit has been fully constructed.
    ///
    /// Use [`Circuit::register_scheduler_event_handler`], [`Circuit::unregister_scheduler_event_handler`],
    /// to manipulate handlers during circuit construction.
    pub fn register_scheduler_event_handler<F>(&self, name: &str, handler: F)
    where
        F: Fn(&SchedulerEvent) + 'static,
    {
        self.circuit.register_scheduler_event_handler(name, handler);
    }

    /// Remove a scheduler event handler.
    ///
    /// This method is identical to [`Circuit::unregister_scheduler_event_handler`], but it can be used at runtime,
    /// after the circuit has been fully constructed.
    pub fn unregister_scheduler_event_handler(&self, name: &str) -> bool {
        self.circuit.unregister_scheduler_event_handler(name)
    }
}

#[cfg(test)]
mod tests {
    use super::Root;
    use crate::circuit::{
        operator::{Apply2, Generator, Inspect, NestedSource, Z1},
        operator_traits::{Operator, SinkOperator, UnaryOperator},
        trace::TraceMonitor,
    };
    use std::{
        borrow::Cow,
        cell::RefCell,
        fmt::Display,
        marker::PhantomData,
        ops::Deref,
        rc::Rc,
        sync::{Arc, Mutex},
        vec::Vec,
    };

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
        fn name(&self) -> Cow<'static, str> {
            Cow::from("Integrator")
        }
        fn clock_start(&mut self) {}
        fn clock_end(&mut self) {
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
        fn name(&self) -> Cow<'static, str> {
            Cow::from("Printer")
        }
        fn clock_start(&mut self) {}
        fn clock_end(&mut self) {}
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
            TraceMonitor::attach(
                Arc::new(Mutex::new(TraceMonitor::new_panic_on_error())),
                circuit,
                "monitor",
            );
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
            TraceMonitor::attach(
                Arc::new(Mutex::new(TraceMonitor::new_panic_on_error())),
                circuit,
                "monitor",
            );

            let source = circuit.add_source(Generator::new(0, |n: &mut usize| *n = *n + 1));
            let (z1_output, z1_feedback) = circuit.add_feedback(Z1::new(0));
            let plus = circuit.add_binary_operator(
                Apply2::new(|n1: &usize, n2: &usize| *n1 + *n2),
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
            TraceMonitor::attach(
                Arc::new(Mutex::new(TraceMonitor::new_panic_on_error())),
                circuit,
                "monitor",
            );

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
                    Apply2::new(|n1: &usize, n2: &usize| n1 * n2),
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
