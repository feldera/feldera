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
//! use dbsp::{
//!     circuit::Root,
//!     operator::{Generator, Inspect},
//! };
//!
//! let root = Root::build(|circuit| {
//!     // Add a source operator.
//!     let source_stream = circuit.add_source(Generator::new(|| "Hello, world!".to_owned()));
//!
//!     // Add a sink operator and wire the source directly to it.
//!     circuit.add_sink(
//!         Inspect::new(|n| println!("New output: {}", n)),
//!         &source_stream,
//!     );
//! });
//! ```

use std::{
    borrow::Cow,
    cell::{Ref, RefCell, RefMut, UnsafeCell},
    collections::HashMap,
    fmt,
    fmt::{Debug, Display, Write},
    marker::PhantomData,
    rc::Rc,
};

use crate::{
    circuit::{
        cache::{CircuitCache, CircuitStoreMarker},
        operator_traits::{
            BinaryOperator, Data, ImportOperator, NaryOperator, SinkOperator, SourceOperator,
            StrictUnaryOperator, TernaryOperator, UnaryOperator,
        },
        schedule::{
            DynamicScheduler, Error as SchedulerError, Executor, IterativeExecutor, OnceExecutor,
            Scheduler,
        },
        trace::{CircuitEvent, SchedulerEvent},
    },
    circuit_cache_key,
};
use typedmap::{TypedMap, TypedMapKey};

/// Value stored in the stream.
struct StreamValue<D> {
    /// Value written to the stream at the current clock cycle;
    /// `None` after the last consumer has retrieved the value from the stream.
    val: Option<D>,
    /// The number of consumers connected to the stream.  Each consumer
    /// reads from the stream exactly once at every clock cycle.
    consumers: usize,
    /// The number of remaining consumers still expected to read from the stream
    /// at the current clock cycle.  This value is reset to `consumers` when
    /// a new value is written to the stream.  It is decremented on each access.
    /// The last consumer to read from the stream (`tokens` drops to 0) obtains
    /// an owned value rather than a borrow.  See description of
    /// [ownership-aware scheduling](`OwnershipPreference`) for details.
    tokens: usize,
}

impl<D> StreamValue<D> {
    const fn empty() -> Self {
        Self {
            val: None,
            consumers: 0,
            tokens: 0,
        }
    }
}

/// A stream stores the output of an operator.  Circuits are synchronous,
/// meaning that each value is produced and consumed in the same clock cycle, so
/// there can be at most one value in the stream at any time.
pub struct Stream<C, D> {
    /// Id of the operator within the local circuit that writes to the stream.
    local_node_id: NodeId,
    /// Global id of the node that writes to this stream.
    origin_node_id: GlobalNodeId,
    /// Circuit that this stream belongs to.
    circuit: C,
    /// Value stored in the stream (there can be at most one since our
    /// circuits are synchronous).
    /// We use `UnsafeCell` instead of `RefCell` to avoid runtime ownership
    /// tests. We enforce unique ownership by making sure that at most one
    /// operator can run (and access the stream) at any time.
    val: Rc<UnsafeCell<StreamValue<D>>>,
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
    /// Returns local node id of the operator or subcircuit that writes to
    /// this stream.
    ///
    /// If the stream originates in a subcircuit, returns the id of the
    /// subcircuit node.
    pub fn local_node_id(&self) -> NodeId {
        self.local_node_id
    }

    /// Returns global id of the operator that writes to this stream.
    ///
    /// If the stream originates in a subcircuit, returns id of the operator
    /// inside the subcircuit (or one of its subcircuits) that produces the
    /// contents of the stream.
    pub fn origin_node_id(&self) -> &GlobalNodeId {
        &self.origin_node_id
    }

    /// Reference to the circuit the stream belongs to.
    pub fn circuit(&self) -> &C {
        &self.circuit
    }

    pub fn ptr_eq<D2>(&self, other: &Stream<C, D2>) -> bool {
        self.origin_node_id() == other.origin_node_id()
    }
}

// Internal streams API only used inside this module.
impl<P, D> Stream<Circuit<P>, D> {
    /// Create a new stream within the given circuit, connected to the specified
    /// node id.
    fn new(circuit: Circuit<P>, node_id: NodeId) -> Self {
        Self {
            local_node_id: node_id,
            origin_node_id: GlobalNodeId::child_of(&circuit, node_id),
            circuit,
            val: Rc::new(UnsafeCell::new(StreamValue::empty())),
        }
    }

    /// Export stream to the parent circuit.
    ///
    /// Creates a stream in the parent circuit that contains the last value in
    /// `self` when the child circuit terminates.
    ///
    /// This method currently only works for streams connected to a feedback
    /// `Z1` operator and will panic for other streams.
    pub fn export(&self) -> Stream<P, D>
    where
        P: Clone + 'static,
        D: 'static,
    {
        self.circuit()
            .cache_get_or_insert_with(ExportId::new(self.local_node_id()), || unimplemented!())
            .clone()
    }
}

impl<C, D> Stream<C, D> {
    /// Create a stream whose origin differs from its node id.
    fn with_origin(circuit: C, node_id: NodeId, origin_node_id: GlobalNodeId) -> Self {
        Self {
            local_node_id: node_id,
            origin_node_id,
            circuit,
            val: Rc::new(UnsafeCell::new(StreamValue::empty())),
        }
    }
}

impl<C, D> Stream<C, D>
where
    D: Clone,
{
    /// Consume a value from the stream.
    ///
    /// This function is invoked exactly once per clock cycle by each
    /// consumer connected to the stream.  The last consumer receives
    /// an owned value (`Cow::Owned`).
    ///
    /// # Safety
    ///
    /// The caller must have exclusive access to the current stream.
    pub(crate) unsafe fn take(&'_ self) -> Cow<'_, D> {
        let val = &mut *self.val.get();
        debug_assert_ne!(val.tokens, 0);
        val.tokens -= 1;
        if val.tokens == 0 {
            Cow::Owned(val.val.take().unwrap())
        } else {
            Cow::Borrowed(val.val.as_ref().unwrap())
        }
    }

    /// Get a reference to the content of the stream without
    /// decrementing the refcount.
    pub(crate) unsafe fn peek(&self) -> &D {
        let val = &*self.val.get();
        debug_assert_ne!(val.tokens, 0);

        val.val.as_ref().unwrap()
    }

    /// Puts a value in the stream, overwriting the previous value if any.
    ///
    /// #Safety
    ///
    /// The caller must have exclusive access to the current stream.
    unsafe fn put(&self, d: D) {
        let val = &mut *self.val.get();
        // If the stream is not connected to any consumers, drop the output
        // on the floor.
        if val.consumers > 0 {
            val.tokens = val.consumers;
            val.val = Some(d);
        }
    }
}

/// Stream whose final value is exported to the parent circuit.
///
/// The struct bundles a pair of streams emitted by a
/// [`StrictOperator`](`crate::circuit::operator_traits::StrictOperator`):
/// a `local` stream inside operator's local circuit and an
/// export stream available to the parent of the local circuit.
/// The export stream contains the final value computed by the
/// operator before `clock_end`.
pub struct ExportStream<P, D> {
    pub local: Stream<Circuit<P>, D>,
    pub export: Stream<P, D>,
}

/// Relative location of a circuit in the hierarchy of nested circuits.
///
/// `0` refers to the local circuit that a given node or operator belongs
/// to, `1` - to the parent of the local, circuit, `2` - to the parent's
/// parent, etc.
pub type Scope = u16;

/// Node in a circuit.  A node wraps an operator with strongly typed
/// input and output streams.
pub trait Node {
    /// Node id unique within its parent circuit.
    fn local_id(&self) -> NodeId;

    /// Global node id.
    fn global_id(&self) -> &GlobalNodeId;

    fn name(&self) -> Cow<'static, str>;

    /// `true` if the node encapsulates an asynchronous operator (see
    /// [`Operator::is_async()`](super::operator_traits::Operator::is_async)).
    /// `false` for synchronous operators and subcircuits.
    fn is_async(&self) -> bool;

    /// `true` if the node is ready to execute (see
    /// [`Operator::ready()`](super::operator_traits::Operator::ready)).
    /// Always returns `true` for synchronous operators and subcircuits.
    fn ready(&self) -> bool;

    /// Register callback to be invoked when an asynchronous operator becomes
    /// ready (see
    /// [`super::operator_traits::Operator::register_ready_callback`]).
    fn register_ready_callback(&mut self, _cb: Box<dyn Fn() + Send + Sync>) {}

    /// Evaluate the operator.  Reads one value from each input stream
    /// and pushes a new value to the output stream (except for sink
    /// operators, which don't have an output stream).
    ///
    /// # Safety
    ///
    /// Only one node may be scheduled at any given time (a node cannot invoke
    /// another node).
    unsafe fn eval(&mut self) -> Result<(), SchedulerError>;

    /// Notify the node about start of a clock epoch.
    ///
    /// The node should forward the notification to its inner operator.
    ///
    /// # Arguments
    ///
    /// * `scope` - the scope whose clock is restarting. A node gets notified
    ///   about clock events in its local circuit (scope 0) and all its
    ///   ancestors.
    fn clock_start(&mut self, scope: Scope);

    /// Notify the node about the end of a clock epoch.
    ///
    /// The node should forward the notification to its inner operator.
    ///
    /// # Arguments
    ///
    /// * `scope` - the scope whose clock is ending.
    ///
    /// # Safety
    ///
    /// Only one node may be scheduled at any given time (a node cannot invoke
    /// another node).
    unsafe fn clock_end(&mut self, scope: Scope);

    fn summary(&self, output: &mut String);

    fn fixedpoint(&self, scope: Scope) -> bool;
}

/// Id of an operator, guaranteed to be unique within a circuit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct NodeId(usize);

impl NodeId {
    pub fn new(id: usize) -> Self {
        Self(id)
    }

    /// Extracts numeric representation of the node id.
    pub fn id(&self) -> usize {
        self.0
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
///
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

    pub fn root() -> Self {
        Self(Vec::new())
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

    /// Returns local node id of `self` or `None` if `self` is the root node.
    pub fn local_node_id(&self) -> Option<NodeId> {
        self.0.last().cloned()
    }

    /// Returns parent id of `self` or `None` if `self` is the rooot node.
    pub fn parent_id(&self) -> Option<Self> {
        self.0
            .split_last()
            .map(|(_, prefix)| GlobalNodeId::from_path(prefix))
    }

    /// Get the path from global.
    pub fn path(&self) -> &[NodeId] {
        &self.0
    }
}

type CircuitEventHandler = Box<dyn Fn(&CircuitEvent)>;
type SchedulerEventHandler = Box<dyn FnMut(&SchedulerEvent<'_>)>;
type CircuitEventHandlers = Rc<RefCell<HashMap<String, CircuitEventHandler>>>;
type SchedulerEventHandlers = Rc<RefCell<HashMap<String, SchedulerEventHandler>>>;

/// Operator's preference to consume input data by value.
///
/// # Background
///
/// A stream in a circuit can be connected to multiple consumers.  It is
/// therefore generally impossible to provide each consumer with an owned copy
/// of the data without cloning it.  At the same time, many operators can be
/// more efficient when working with owned inputs.  For instance, when computing
/// a sum of two z-sets, if one of the input z-sets is owned we can just add
/// values from the other z-set to it without cloning the first z-set.  If both
/// inputs are owned then we additionally do not need to clone key/value pairs
/// when inserting them.  Furthermore, the implementation can choose to add the
/// contents of the smaller z-set to the larger one.
///
/// # Ownership-aware scheduling
///
/// To leverage such optimizations, we adopt the best-effort approach: operators
/// consume streaming data by-value when possible while falling back to
/// pass-by-reference otherwise.  In a synchronous circuit, each operator reads
/// its input stream precisely once in each clock cycle. It is therefore
/// possible to determine the last consumer at each clock cycle and give it the
/// owned value from the channel.  It is furthermore possible for the scheduler
/// to schedule operators that strongly prefer owned values last.
///
/// We capture ownership preferences at two levels.  First, each individual
/// operator that consumes one or more streams exposes its preferences on a
/// per-stream basis via an API method (e.g., `[UnaryOperator::
/// input_preference]`).  The [`Circuit`] API allows the circuit builder
/// to override these preferences when instantiating an operator, taking into
/// account circuit topology and workload.  We express preference as a numeric
/// value.
///
/// These preferences are associated with each edge in the circuit graph.  The
/// schedulers we have built so far implement a limited form of ownership-aware
/// scheduling.  They only consider strong preferences
/// (`OwnershipPreference::require_owned` and stronger) and model them
/// internally as hard constraints that must be satisfied for the circuit to be
/// schedulable.  Weaker preferences are ignored.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Hash)]
#[repr(transparent)]
pub struct OwnershipPreference(usize);

impl OwnershipPreference {
    /// Create a new instance with given numeric preference value (higher
    /// value means stronger preference).
    pub const fn new(val: usize) -> Self {
        Self(val)
    }

    /// The operator does not gain any speed up from consuming an owned value.
    pub const INDIFFERENT: Self = Self::new(0);

    /// The operator is likely to run faster provided an owned input.
    ///
    /// Preference levels above `prefer_owned` should not be used by operators
    /// and are reserved for use by circuit builders through the `Circuit`
    /// API.
    pub const PREFER_OWNED: Self = Self::new(50);

    /// The circuit will suffer a significant performance hit if the operator
    /// cannot consume data in the channel by-value.
    pub const STRONGLY_PREFER_OWNED: Self = Self::new(100);

    /// Returns the numeric value of the preference.
    pub const fn raw(&self) -> usize {
        self.0
    }
}

impl Display for OwnershipPreference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::INDIFFERENT => f.write_str("Indifferent"),
            Self::PREFER_OWNED => f.write_str("PreferOwned"),
            Self::STRONGLY_PREFER_OWNED => f.write_str("StronglyPreferOwned"),
            _ => write!(f, "Preference({})", self.raw()),
        }
    }
}

/// An edge in a circuit graph represents a stream connecting two
/// operators or a dependency (i.e., a requirement that one operator
/// must be evaluated before the other even if they are not connected
/// by a stream).
pub(super) struct Edge {
    /// Source node.
    pub from: NodeId,
    /// Destination node.
    pub to: NodeId,
    /// Origin node that generates the stream.  If the origin belongs
    /// to the local circuit, this is just the full path to the `from`
    /// node.
    pub origin: GlobalNodeId,
    /// Ownership preference associated with the consumer of this
    /// stream or `None` if this is a dependency edge.
    pub ownership_preference: Option<OwnershipPreference>,
}

#[allow(dead_code)]
impl Edge {
    /// `true` if `self` is a dependency edge.
    pub(super) fn is_dependency(&self) -> bool {
        self.ownership_preference.is_none()
    }

    /// `true` if `self` is a stream edge.
    pub(super) fn is_stream(&self) -> bool {
        self.ownership_preference.is_some()
    }
}

circuit_cache_key!(ExportId<C, D>(NodeId => Stream<C, D>));

/// A circuit consists of nodes and edges.  An edge from
/// node1 to node2 indicates that the output stream of node1
/// is connected to an input of node2.
struct CircuitInner<P> {
    parent: P,
    root_scope: Scope,
    // Circuit's node id within the parent circuit.
    node_id: NodeId,
    global_node_id: GlobalNodeId,
    nodes: Vec<Box<dyn Node>>,
    edges: Vec<Edge>,
    circuit_event_handlers: CircuitEventHandlers,
    scheduler_event_handlers: SchedulerEventHandlers,
    store: CircuitCache,
}

impl<P> CircuitInner<P> {
    fn new(
        parent: P,
        root_scope: Scope,
        node_id: NodeId,
        global_node_id: GlobalNodeId,
        circuit_event_handlers: CircuitEventHandlers,
        scheduler_event_handlers: SchedulerEventHandlers,
    ) -> Self {
        Self {
            parent,
            root_scope,
            node_id,
            global_node_id,
            nodes: Vec::new(),
            edges: Vec::new(),
            circuit_event_handlers,
            scheduler_event_handlers,
            store: TypedMap::new(),
        }
    }

    fn add_edge(&mut self, edge: Edge) {
        self.edges.push(edge);
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
        self.store.clear();
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
        F: FnMut(&SchedulerEvent<'_>) + 'static,
    {
        self.scheduler_event_handlers.borrow_mut().insert(
            name.to_string(),
            Box::new(handler) as Box<dyn FnMut(&SchedulerEvent<'_>)>,
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

    fn log_scheduler_event(&self, event: &SchedulerEvent<'_>) {
        for (_, handler) in self.scheduler_event_handlers.borrow_mut().iter_mut() {
            handler(event)
        }
    }

    fn fixedpoint(&self, scope: Scope) -> bool {
        self.nodes.iter().all(|node| {
            node.fixedpoint(scope)
            /*if !res {
                eprintln!("node {} ({})", node.global_id(), node.name());
            }*/
        })
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
    // Create new top-level circuit.  Clients invoke this via the [`Root::build`]
    // API.
    fn new() -> Self {
        Self(Rc::new(RefCell::new(CircuitInner::new(
            (),
            0,
            NodeId::root(),
            GlobalNodeId::root(),
            Rc::new(RefCell::new(HashMap::new())),
            Rc::new(RefCell::new(HashMap::new())),
        ))))
    }
}

impl Circuit<()> {
    /// Attach a circuit event handler to the top-level circuit (see
    /// [`super::trace::CircuitEvent`] for a description of circuit events).
    ///
    /// This method should normally be called inside the closure passed to
    /// [`Root::build`] before adding any operators to the circuit, so that
    /// the handler gets to observe all nodes, edges, and subcircuits added
    /// to the circuit.
    ///
    /// `name` - user-readable name assigned to the handler.  If a handler with
    /// the same name exists, it will be replaced by the new handler.
    ///
    /// `handler` - user callback invoked on each circuit event (see
    /// [`super::trace::CircuitEvent`]).
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

    /// Remove a circuit event handler.  Returns `true` if a handler with the
    /// specified name had previously been registered and `false` otherwise.
    pub fn unregister_circuit_event_handler(&self, name: &str) -> bool {
        self.inner_mut().unregister_circuit_event_handler(name)
    }

    /// Attach a scheduler event handler to the top-level circuit (see
    /// [`super::trace::SchedulerEvent`] for a description of scheduler
    /// events).
    ///
    /// This method can be used during circuit construction, inside the closure
    /// provided to [`Root::build`].  Use
    /// [`Root::register_scheduler_event_handler`],
    /// [`Root::unregister_scheduler_event_handler`] to manipulate scheduler
    /// callbacks at runtime.
    ///
    /// `name` - user-readable name assigned to the handler.  If a handler with
    /// the same name exists, it will be replaced by the new handler.
    ///
    /// `handler` - user callback invoked on each scheduler event.
    pub fn register_scheduler_event_handler<F>(&self, name: &str, handler: F)
    where
        F: FnMut(&SchedulerEvent<'_>) + 'static,
    {
        self.inner_mut()
            .register_scheduler_event_handler(name, handler);
    }

    /// Remove a scheduler event handler.  Returns `true` if a handler with the
    /// specified name had previously been registered and `false` otherwise.
    pub fn unregister_scheduler_event_handler(&self, name: &str) -> bool {
        self.inner_mut().unregister_scheduler_event_handler(name)
    }
}

impl<P> Circuit<Circuit<P>>
where
    P: Clone + 'static,
{
    /// Create an empty nested circuit of `parent`.
    fn with_parent(parent: Circuit<P>, id: NodeId) -> Self {
        let global_node_id = GlobalNodeId::child_of(&parent, id);
        let circuit_handlers = parent.inner().circuit_event_handlers.clone();
        let sched_handlers = parent.inner().scheduler_event_handlers.clone();
        let root_scope = parent.root_scope() + 1;

        Circuit(Rc::new(RefCell::new(CircuitInner::new(
            parent,
            root_scope,
            id,
            global_node_id,
            circuit_handlers,
            sched_handlers,
        ))))
    }

    /// `true` if `self` is a subcircuit of `other`.
    pub fn is_child_of(&self, other: &Circuit<P>) -> bool {
        Circuit::ptr_eq(&self.inner().parent, other)
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
    fn connect_stream<T>(
        &self,
        stream: &Stream<Self, T>,
        to: NodeId,
        ownership_preference: OwnershipPreference,
    ) {
        self.log_circuit_event(&CircuitEvent::stream(
            stream.origin_node_id().clone(),
            GlobalNodeId::child_of(self, to),
            ownership_preference,
        ));

        // Safe because the circuit isn't running yet.
        unsafe { (*stream.val.get()).consumers += 1 };

        debug_assert_eq!(self.global_node_id(), stream.circuit.global_node_id());
        self.inner_mut().add_edge(Edge {
            from: stream.local_node_id(),
            to,
            origin: stream.origin_node_id().clone(),
            ownership_preference: Some(ownership_preference),
        });
    }

    /// Register a dependency between `from` and `to` nodes.  A dependency tells
    /// the scheduler that `from` must be evaluated before `to` in each
    /// clock cycle even though there may not be an edge or a path
    /// connecting them.
    fn add_dependency(&self, from: NodeId, to: NodeId) {
        self.log_circuit_event(&CircuitEvent::dependency(
            GlobalNodeId::child_of(self, from),
            GlobalNodeId::child_of(self, to),
        ));

        let origin = GlobalNodeId::child_of(self, from);
        self.inner_mut().add_edge(Edge {
            from,
            to,
            origin,
            ownership_preference: None,
        });
    }

    /// Add a node to the circuit.
    ///
    /// Allocates a new node id and invokes a user callback to create a new node
    /// instance. The callback may use the node id, e.g., to add an edge to
    /// this node.
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

    /// Like `add_node`, but the node is not created if the closure fails.
    fn try_add_node<F, N, T, E>(&self, f: F) -> Result<T, E>
    where
        F: FnOnce(NodeId) -> Result<(N, T), E>,
        N: Node + 'static,
    {
        let id = self.inner().nodes.len();

        // We don't hold a reference to `self.inner()` while calling `f`, so it can
        // safely modify the circuit, e.g., add edges.
        let (node, res) = f(NodeId(id))?;
        self.inner_mut().add_node(node);
        Ok(res)
    }

    pub(super) fn edges(&self) -> Ref<'_, [Edge]> {
        let circuit = self.inner();
        Ref::map(circuit, |c| c.edges.as_slice())
    }

    /// Number of nodes in the circuit.
    pub(super) fn num_nodes(&self) -> usize {
        self.inner().nodes.len()
    }

    /// Returns vector of local node ids in the circuit.
    pub(super) fn node_ids(&self) -> Vec<NodeId> {
        self.inner()
            .nodes
            .iter()
            .map(|node| node.local_id())
            .collect()
    }

    /// Deliver `clock_start` notification to all nodes in the circuit.
    pub(super) fn clock_start(&self, scope: Scope) {
        for node in self.inner_mut().nodes.iter_mut() {
            node.clock_start(scope);
        }
    }

    /// Deliver `clock_end` notification to all nodes in the circuit.
    pub(super) unsafe fn clock_end(&self, scope: Scope) {
        for node in self.inner_mut().nodes.iter_mut() {
            node.clock_end(scope);
        }
    }

    fn clear(&mut self) {
        self.inner_mut().clear();
    }

    /// Send the specified `CircuitEvent` to all handlers attached to the
    /// circuit.
    fn log_circuit_event(&self, event: &CircuitEvent) {
        self.inner().log_circuit_event(event);
    }

    /// Send the specified `SchedulerEvent` to all handlers attached to the
    /// circuit.
    pub(super) fn log_scheduler_event(&self, event: &SchedulerEvent<'_>) {
        self.inner().log_scheduler_event(event);
    }

    /// Check if `this` and `other` refer to the same circuit instance.
    fn ptr_eq(this: &Self, other: &Self) -> bool {
        Rc::ptr_eq(&this.0, &other.0)
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

    /// Relative depth of `self` from the root circuit.
    ///
    /// Returns 0 if `self` is the root circuit, 1 if `self` is an immediate
    /// child of the root circuit, etc.
    pub fn root_scope(&self) -> Scope {
        self.inner().root_scope
    }

    /// Lookup a value in the circuit cache or create and insert a new value
    /// if it does not exist.
    ///
    /// See [`cache`] module documentation for details.
    pub(crate) fn cache_get_or_insert_with<K, F>(&self, key: K, mut f: F) -> RefMut<'_, K::Value>
    where
        K: 'static + TypedMapKey<CircuitStoreMarker>,
        F: FnMut() -> K::Value,
    {
        // Don't use `store.entry()`, since `f` may need to perform
        // its own cache lookup.
        if self.inner().store.contains_key(&key) {
            return RefMut::map(self.inner_mut(), |c| c.store.get_mut(&key).unwrap());
        }

        let new = f();

        // TODO: Use `RefMut::filter_map()` to only perform one lookup in the happy path
        //       https://github.com/rust-lang/rust/issues/81061
        RefMut::map(self.inner_mut(), |c| c.store.entry(key).or_insert(new))
    }

    pub(crate) fn ready(&self, id: NodeId) -> bool {
        self.inner().nodes[id.0].ready()
    }

    /// Insert a value to the circuit cache, overwriting any existing value.
    ///
    /// See [`cache`] module documentation for details.
    pub(crate) fn cache_insert<K>(&self, key: K, val: K::Value)
    where
        K: 'static + TypedMapKey<CircuitStoreMarker>,
    {
        self.inner_mut().store.insert(key, val);
    }

    pub(crate) fn register_ready_callback(&self, id: NodeId, cb: Box<dyn Fn() + Send + Sync>) {
        self.inner_mut().nodes[id.0].register_ready_callback(cb);
    }

    pub(crate) fn is_async_node(&self, id: NodeId) -> bool {
        self.inner().nodes[id.0].is_async()
    }

    /// Evaluate operator with the given id.
    ///
    /// This method should only be used by schedulers.
    pub(crate) fn eval_node(&self, id: NodeId) -> Result<(), SchedulerError> {
        let mut circuit = self.inner_mut();
        debug_assert!(id.0 < circuit.nodes.len());

        // Notify loggers while holding a reference to the inner circuit.
        // We normally avoid this, since a nested call from event handler
        // will panic in `self.inner()`, but we do it here as an
        // optimization.
        circuit.log_scheduler_event(&SchedulerEvent::eval_start(circuit.nodes[id.0].as_ref()));

        // Safety: `eval` cannot invoke the
        // `eval` method of another node.  To circumvent
        // this invariant the user would have to extract a
        // reference to a node and pass it to an operator,
        // but this module doesn't expose nodes, only
        // streams.
        unsafe { circuit.nodes[id.0].eval()? };

        circuit.log_scheduler_event(&SchedulerEvent::eval_end(circuit.nodes[id.0].as_ref()));

        Ok(())
    }

    /// Evaluate closure `f` inside a new circuit region.
    ///
    /// A region is a logical grouping of circuit nodes.  Regions are used
    /// exclusively for debugging and do not affect scheduling or evaluation
    /// of the circuit.  This function creates a new region and executes
    /// closure `f` inside it.  Any operators or subcircuits created by
    /// `f` will belong to the new region.
    pub fn region<F, T>(&self, name: &str, f: F) -> T
    where
        F: FnOnce() -> T,
    {
        self.log_circuit_event(&CircuitEvent::push_region(name));
        let res = f();
        self.log_circuit_event(&CircuitEvent::pop_region());
        res
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
    /// Operators that exchange data across workers are split into two
    /// operators: the **sender** responsible for partitioning values read
    /// from the input stream and distributing them across workers and the
    /// **receiver**, which receives and reassembles data received from its
    /// peers.  Splitting communication into two halves allows the scheduler
    /// to schedule useful work in between them instead of blocking to wait
    /// for the receiver.
    ///
    /// Exchange operators use some form of IPC or shared memory instead of
    /// streams to communicate.  Therefore, the sender must implement trait
    /// [`SinkOperator`], while the receiver implements [`SourceOperator`].
    ///
    /// This function adds both operators to the circuit and registers a
    /// dependency between them, making sure that the scheduler will
    /// evaluate the sender before the receiver even though there is no
    /// explicit stream connecting them.
    ///
    /// Returns the output stream produced by the receiver operator.
    ///
    /// # Arguments
    ///
    /// * `sender` - the sender half of the pair.  The sender must be a sink
    ///   operator
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
        let preference = sender.input_preference();
        self.add_exchange_with_preference(sender, receiver, input_stream, preference)
    }

    /// Like [`Self::add_exchange`], but overrides the ownership
    /// preference on the input stream with `input_preference`.
    pub fn add_exchange_with_preference<I, SndOp, O, RcvOp>(
        &self,
        sender: SndOp,
        receiver: RcvOp,
        input_stream: &Stream<Self, I>,
        input_preference: OwnershipPreference,
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
            self.connect_stream(input_stream, id, input_preference);
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

        self.add_dependency(sender_id, output_stream.local_node_id());
        output_stream
    }

    /// Add a sink operator (see [`SinkOperator`]).
    pub fn add_sink<I, Op>(&self, operator: Op, input_stream: &Stream<Self, I>)
    where
        I: Data,
        Op: SinkOperator<I>,
    {
        let preference = operator.input_preference();
        self.add_sink_with_preference(operator, input_stream, preference)
    }

    /// Like [`Self::add_sink`], but overrides the ownership preference on the
    /// input stream with `input_preference`.
    pub fn add_sink_with_preference<I, Op>(
        &self,
        operator: Op,
        input_stream: &Stream<Self, I>,
        input_preference: OwnershipPreference,
    ) where
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

            self.connect_stream(input_stream, id, input_preference);
            (
                SinkNode::new(operator, input_stream.clone(), self.clone(), id),
                (),
            )
        });
    }

    /// Add a unary operator (see [`UnaryOperator`]).
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
        let preference = operator.input_preference();
        self.add_unary_operator_with_preference(operator, input_stream, preference)
    }

    /// Like [`Self::add_unary_operator`], but overrides the ownership
    /// preference on the input stream with `input_preference`.
    pub fn add_unary_operator_with_preference<I, O, Op>(
        &self,
        operator: Op,
        input_stream: &Stream<Self, I>,
        input_preference: OwnershipPreference,
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
            self.connect_stream(input_stream, id, input_preference);
            (node, output_stream)
        })
    }

    /// Add a binary operator (see [`BinaryOperator`]).
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
        let (pref1, pref2) = operator.input_preference();
        self.add_binary_operator_with_preference(
            operator,
            input_stream1,
            input_stream2,
            pref1,
            pref2,
        )
    }

    /// Like [`Self::add_binary_operator`], but overrides the ownership
    /// preference on both input streams with `input_preference1` and
    /// `input_preference2` respectively.
    pub fn add_binary_operator_with_preference<I1, I2, O, Op>(
        &self,
        operator: Op,
        input_stream1: &Stream<Self, I1>,
        input_stream2: &Stream<Self, I2>,
        input_preference1: OwnershipPreference,
        input_preference2: OwnershipPreference,
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
            self.connect_stream(input_stream1, id, input_preference1);
            self.connect_stream(input_stream2, id, input_preference2);
            (node, output_stream)
        })
    }

    /// Add a ternary operator (see [`TernaryOperator`]).
    pub fn add_ternary_operator<I1, I2, I3, O, Op>(
        &self,
        operator: Op,
        input_stream1: &Stream<Self, I1>,
        input_stream2: &Stream<Self, I2>,
        input_stream3: &Stream<Self, I3>,
    ) -> Stream<Self, O>
    where
        I1: Data,
        I2: Data,
        I3: Data,
        O: Data,
        Op: TernaryOperator<I1, I2, I3, O>,
    {
        let (pref1, pref2, pref3) = operator.input_preference();
        self.add_ternary_operator_with_preference(
            operator,
            input_stream1,
            input_stream2,
            input_stream3,
            pref1,
            pref2,
            pref3,
        )
    }

    /// Like [`Self::add_ternary_operator`], but overrides the ownership
    /// preference on the input streams with specified values.
    #[allow(clippy::too_many_arguments)]
    pub fn add_ternary_operator_with_preference<I1, I2, I3, O, Op>(
        &self,
        operator: Op,
        input_stream1: &Stream<Self, I1>,
        input_stream2: &Stream<Self, I2>,
        input_stream3: &Stream<Self, I3>,
        input_preference1: OwnershipPreference,
        input_preference2: OwnershipPreference,
        input_preference3: OwnershipPreference,
    ) -> Stream<Self, O>
    where
        I1: Data,
        I2: Data,
        I3: Data,
        O: Data,
        Op: TernaryOperator<I1, I2, I3, O>,
    {
        self.add_node(|id| {
            self.log_circuit_event(&CircuitEvent::operator(
                GlobalNodeId::child_of(self, id),
                operator.name(),
            ));

            let node = TernaryNode::new(
                operator,
                input_stream1.clone(),
                input_stream2.clone(),
                input_stream3.clone(),
                self.clone(),
                id,
            );
            let output_stream = node.output_stream();
            self.connect_stream(input_stream1, id, input_preference1);
            self.connect_stream(input_stream2, id, input_preference2);
            self.connect_stream(input_stream3, id, input_preference3);
            (node, output_stream)
        })
    }

    /// Add a N-ary operator (see [`NaryOperator`]).
    pub fn add_nary_operator<'a, I, O, Op, Iter>(
        &'a self,
        operator: Op,
        input_streams: Iter,
    ) -> Stream<Self, O>
    where
        I: Data,
        O: Data,
        Op: NaryOperator<I, O>,
        Iter: IntoIterator<Item = &'a Stream<Self, I>>,
    {
        let pref = operator.input_preference();
        self.add_nary_operator_with_preference(operator, input_streams, pref)
    }

    /// Like [`Self::add_nary_operator`], but overrides the ownership
    /// preference with `input_preference`.
    pub fn add_nary_operator_with_preference<'a, I, O, Op, Iter>(
        &'a self,
        operator: Op,
        input_streams: Iter,
        input_preference: OwnershipPreference,
    ) -> Stream<Self, O>
    where
        I: Data,
        O: Data,
        Op: NaryOperator<I, O>,
        Iter: IntoIterator<Item = &'a Stream<Self, I>>,
    {
        let input_streams: Vec<Stream<_, _>> = input_streams.into_iter().cloned().collect();
        self.add_node(|id| {
            self.log_circuit_event(&CircuitEvent::operator(
                GlobalNodeId::child_of(self, id),
                operator.name(),
            ));

            let node = NaryNode::new(
                operator,
                input_streams.clone().into_iter(),
                self.clone(),
                id,
            );
            let output_stream = node.output_stream();
            for stream in input_streams.iter() {
                self.connect_stream(stream, id, input_preference);
            }
            (node, output_stream)
        })
    }

    /// Add a feedback loop to the circuit.
    ///
    /// Other methods in this API only support the construction of acyclic
    /// graphs, as they require the input stream to exist before nodes that
    /// consumes it are created.  This method instantiates an operator whose
    /// input stream can be connected later, and thus may depend on
    /// the operator's output.  This enables the construction of feedback loops.
    /// Since all loops in a well-formed circuit must include a [strict
    /// operator](`crate::circuit::operator_traits::StrictOperator`), `operator`
    /// must be strict.
    ///
    /// Returns the output stream of the operator and an object that can be used
    /// to later connect its input.
    ///
    /// # Examples
    /// We build the following circuit to compute the sum of input values
    /// received from `source`. `z1` stores the sum accumulated during
    /// previous timestamps.  At every timestamp, the (`+`) operator
    /// computes the sum of the new value received from source and the value
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
    /// # use dbsp::{
    /// #     circuit::Root,
    /// #     operator::{Z1, Apply2, Generator},
    /// # };
    /// # let root = Root::build(|circuit| {
    /// // Create a data source.
    /// let source = circuit.add_source(Generator::new(|| 10));
    /// // Create z1.  `z1_output` will contain the output stream of `z1`; `z1_feedback`
    /// // is a placeholder where we can later plug the input to `z1`.
    /// let (z1_output, z1_feedback) = circuit.add_feedback(Z1::new(0));
    /// // Connect outputs of `source` and `z1` to the plus operator.
    /// let plus = circuit.add_binary_operator(
    ///     Apply2::new(|n1: &usize, n2: &usize| n1 + n2),
    ///     &source,
    ///     &z1_output,
    /// );
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
        let (export_stream, connector) = self.add_feedback_with_export(operator);
        (export_stream.local, connector)
    }

    /// Like `add_feedback`, but additionally makes the output of the operator
    /// available to the parent circuit.
    ///
    /// Normally a strict operator writes a value computed based on inputs from
    /// previous clock cycles to its output stream at the start of each new
    /// clock cycle.  When the local clock epoch ends, the last value
    /// computed by the operator (that would otherwise be dropped) is
    /// written to the export stream instead.
    ///
    /// # Examples
    ///
    /// See example in the [`Self::iterate`] method.
    pub fn add_feedback_with_export<I, O, Op>(
        &self,
        operator: Op,
    ) -> (ExportStream<P, O>, FeedbackConnector<Self, I, O, Op>)
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
            let local = output_node.output_stream();
            let export = output_node.export_stream.clone();
            (output_node, (ExportStream { local, export }, connector))
        })
    }

    fn connect_feedback_with_preference<I, O, Op>(
        &self,
        output_node_id: NodeId,
        operator: Rc<UnsafeCell<Op>>,
        input_stream: &Stream<Self, I>,
        input_preference: OwnershipPreference,
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
            self.connect_stream(input_stream, id, input_preference);
            self.add_dependency(output_node_id, id);
            (output_node, ())
        });
    }

    /// Add a child circuit.
    ///
    /// Creates an empty circuit with `self` as parent and invokes
    /// `child_constructor` to populate the circuit.  `child_constructor`
    /// typically captures some of the streams in `self` and connects them
    /// to source nodes of the child circuit.  It is also responsible for
    /// attaching an executor to the child circuit.  The return type `T`
    /// will typically contain output streams of the child.
    ///
    /// Most users should invoke higher-level APIs like [`iterate`] instead of
    /// using this method directly.
    pub(crate) fn subcircuit<F, T, E>(
        &self,
        iterative: bool,
        child_constructor: F,
    ) -> Result<T, SchedulerError>
    where
        F: FnOnce(&mut Circuit<Self>) -> Result<(T, E), SchedulerError>,
        E: Executor<Self>,
    {
        self.try_add_node(|id| {
            let global_id = GlobalNodeId::child_of(self, id);
            self.log_circuit_event(&CircuitEvent::subcircuit(global_id.clone(), iterative));
            let mut child_circuit = Circuit::with_parent(self.clone(), id);
            let (res, executor) = child_constructor(&mut child_circuit)?;
            let child = <ChildNode<Self>>::new::<E>(child_circuit, executor);
            self.log_circuit_event(&CircuitEvent::subcircuit_complete(global_id));
            Ok((child, res))
        })
    }

    /// Add an iteratively scheduled child circuit.
    ///
    /// Add a child circuit with a nested clock.  The child will execute
    /// multiple times for each parent timestamp, until its termination
    /// condition is satisfied.  Every time the child circuit is activated
    /// by the parent (once per parent timestamp), the executor calls
    /// [`clock_start`](`super::operator_traits::Operator::clock_start`)
    /// on each child operator.  It then calls `eval` on all
    /// child operators in a causal order and checks if the termination
    /// condition is satisfied.  If the condition is `false`, the
    /// executor `eval`s all operators again.  Once the termination
    /// condition is `true`, the executor calls `clock_end` on all child
    /// operators and returns control back to the parent scheduler.
    ///
    /// The `constructor` closure populates the child circuit and returns a
    /// closure that checks the termination condition and an arbitrary
    /// user-defined return value that typically contains output streams
    /// of the child.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::{cell::RefCell, rc::Rc};
    /// use dbsp::{
    ///     circuit::Root,
    ///     operator::{Apply2, Generator, Inspect, Z1},
    /// };
    ///
    /// let root = Root::build(|circuit| {
    ///     // Generate sequence 0, 1, 2, ...
    ///     let mut n: usize = 0;
    ///     let source = circuit.add_source(Generator::new(move || {
    ///         let result = n;
    ///         n = n + 1;
    ///         result
    ///     }));
    ///     // Compute factorial of each number in the sequence.
    ///     let fact = circuit
    ///         .iterate(|child| {
    ///             let counter = Rc::new(RefCell::new(0));
    ///             let counter_clone = counter.clone();
    ///             let countdown = source.delta0(child).apply(move |parent_val| {
    ///                 if *parent_val > 0 {
    ///                     *counter_clone.borrow_mut() += *parent_val;
    ///                 }
    ///                 let res = *counter_clone.borrow();
    ///                 *counter_clone.borrow_mut() -= 1;
    ///                 res
    ///             });
    ///             let (z1_output, z1_feedback) = child.add_feedback_with_export(Z1::new(1));
    ///             let mul = child.add_binary_operator(
    ///                 Apply2::new(|n1: &usize, n2: &usize| n1 * n2),
    ///                 &countdown,
    ///                 &z1_output.local,
    ///             );
    ///             z1_feedback.connect(&mul);
    ///             Ok((move || *counter.borrow() <= 1, z1_output.export))
    ///         })
    ///         .unwrap();
    ///     circuit.add_sink(Inspect::new(move |n| eprintln!("Output: {}", n)), &fact);
    /// });
    /// ```
    pub fn iterate<F, C, T>(&self, constructor: F) -> Result<T, SchedulerError>
    where
        F: FnOnce(&mut Circuit<Self>) -> Result<(C, T), SchedulerError>,
        C: Fn() -> bool + 'static,
    {
        self.iterate_with_scheduler::<F, C, T, DynamicScheduler>(constructor)
    }

    /// Add an iteratively scheduled child circuit.
    ///
    /// Similar to [`iterate`](`Self::iterate`), but with a user-specified
    /// [`Scheduler`] implementation.
    pub fn iterate_with_scheduler<F, C, T, S>(&self, constructor: F) -> Result<T, SchedulerError>
    where
        F: FnOnce(&mut Circuit<Self>) -> Result<(C, T), SchedulerError>,
        C: Fn() -> bool + 'static,
        S: Scheduler + 'static,
    {
        self.subcircuit(true, |child| {
            let (termination_check, res) = constructor(child)?;
            let executor = <IterativeExecutor<_, S>>::new(child, termination_check)?;
            Ok((res, executor))
        })
    }

    /// Add a child circuit that will iterate to a fixed point.
    ///
    /// For each parent clock cycle, the child circuit will iterate until
    /// reaching a fixed point, i.e., a state where the outputs of all
    /// operators are guaranteed to remain the same, should the nested clock
    /// continue ticking.
    ///
    /// The fixed point check is implemented by checking the following
    /// condition:
    ///
    /// * All operators in the circuit are in such a state that, if their inputs
    ///   remain constant (i.e., all future inputs are identical to the last
    ///   input), then their outputs remain constant too.
    ///
    /// This is a necessary and sufficient condition that is also easy to check
    /// by asking each operator if it is in a stable state (via the
    /// [`Operator::fixedpoint`](`super::operator_traits::Operator::fixedpoint`)
    /// API.  
    ///
    /// # Warning
    ///
    /// The cost of checking this condition precisely can be high for some
    /// operators, which implement approximate checks instead.  For instance,
    /// delay operators ([`Z1`](`crate::operator::Z1`) and
    /// [`Z1Nested`](`crate::operator::Z1Nested`)) require storing the last
    /// two versions of the state instead of one and comparing them at each
    /// cycle.  Instead, they conservatively check for _specific_ fixed points,
    /// namely fixed points where both input and output of the operator are zero
    /// As a result, the circuit may fail to detect other fixed points and may
    /// iterate forever.
    ///
    /// The goal is to evolve the design so that circuits created using the
    /// high-level API (`Stream::xxx` methods) implement accurate fixed point
    /// checks, but there are currently no guardrails in the system against
    /// constructing non-compliant circuits.
    pub fn fixedpoint<F, T>(&self, constructor: F) -> Result<T, SchedulerError>
    where
        F: FnOnce(&mut Circuit<Self>) -> Result<T, SchedulerError>,
    {
        self.fixedpoint_with_scheduler::<F, T, DynamicScheduler>(constructor)
    }

    /// Add a child circuit that will iterate to a fixed point.
    ///
    /// Similar to [`fixedpoint`](`Self::fixedpoint`), but with a user-specified
    /// [`Scheduler`] implementation.
    pub fn fixedpoint_with_scheduler<F, T, S>(&self, constructor: F) -> Result<T, SchedulerError>
    where
        F: FnOnce(&mut Circuit<Self>) -> Result<T, SchedulerError>,
        S: Scheduler + 'static,
    {
        self.subcircuit(true, |child| {
            let res = constructor(child)?;
            let child_clone = child.clone();
            let termination_check = move || child_clone.inner().fixedpoint(0);
            let executor = <IterativeExecutor<_, S>>::new(child, termination_check)?;
            Ok((res, executor))
        })
    }
}

impl<P> Circuit<Circuit<P>>
where
    P: 'static + Clone,
{
    /// Make the contents of `parent_stream` available in the nested circuit
    /// via an [`ImportOperator`].
    ///
    /// Typically invoked via a convenience wrapper, e.g., [`Stream::delta0`].
    pub fn import_stream<I, O, Op>(
        &self,
        operator: Op,
        parent_stream: &Stream<Circuit<P>, I>,
    ) -> Stream<Self, O>
    where
        I: Data,
        O: Data,
        Op: ImportOperator<I, O>,
    {
        let preference = operator.input_preference();
        self.import_stream_with_preference(operator, parent_stream, preference)
    }

    /// Like [`Self::import_stream`] but overrides the ownership
    /// preference on the input stream with `input_preference.
    pub fn import_stream_with_preference<I, O, Op>(
        &self,
        operator: Op,
        parent_stream: &Stream<Circuit<P>, I>,
        input_preference: OwnershipPreference,
    ) -> Stream<Self, O>
    where
        I: Data,
        O: Data,
        Op: ImportOperator<I, O>,
    {
        assert!(self.is_child_of(parent_stream.circuit()));

        self.add_node(|id| {
            self.log_circuit_event(&CircuitEvent::operator(
                GlobalNodeId::child_of(self, id),
                operator.name(),
            ));
            let node = ImportNode::new(operator, self.clone(), parent_stream.clone(), id);
            self.parent()
                .connect_stream(parent_stream, self.node_id(), input_preference);
            let output_stream = node.output_stream();
            (node, output_stream)
        })
    }
}

struct ImportNode<P, I, O, Op> {
    id: GlobalNodeId,
    operator: Op,
    parent_stream: Stream<Circuit<P>, I>,
    output_stream: Stream<Circuit<Circuit<P>>, O>,
}

impl<P, I, O, Op> ImportNode<P, I, O, Op>
where
    P: Clone + 'static,
{
    fn new(
        operator: Op,
        circuit: Circuit<Circuit<P>>,
        parent_stream: Stream<Circuit<P>, I>,
        id: NodeId,
    ) -> Self {
        assert!(Circuit::ptr_eq(&circuit.parent(), parent_stream.circuit()));

        Self {
            id: circuit.global_node_id().child(id),
            operator,
            parent_stream,
            output_stream: Stream::new(circuit, id),
        }
    }

    fn output_stream(&self) -> Stream<Circuit<Circuit<P>>, O> {
        self.output_stream.clone()
    }
}

impl<P, I, O, Op> Node for ImportNode<P, I, O, Op>
where
    I: Clone,
    O: Clone,
    Op: ImportOperator<I, O>,
{
    fn name(&self) -> Cow<'static, str> {
        self.operator.name()
    }

    fn local_id(&self) -> NodeId {
        self.id.local_node_id().unwrap()
    }

    fn global_id(&self) -> &GlobalNodeId {
        &self.id
    }

    fn is_async(&self) -> bool {
        self.operator.is_async()
    }

    fn ready(&self) -> bool {
        self.operator.ready()
    }

    fn register_ready_callback(&mut self, cb: Box<dyn Fn() + Send + Sync>) {
        self.operator.register_ready_callback(cb);
    }

    unsafe fn eval(&mut self) -> Result<(), SchedulerError> {
        self.output_stream.put(self.operator.eval());
        Ok(())
    }

    fn clock_start(&mut self, scope: Scope) {
        self.operator.clock_start(scope);
        if scope == 0 {
            match unsafe { self.parent_stream.take() } {
                Cow::Borrowed(val) => self.operator.import(val),
                Cow::Owned(val) => self.operator.import_owned(val),
            }
        }
    }

    unsafe fn clock_end(&mut self, scope: Scope) {
        self.operator.clock_end(scope);
    }

    fn summary(&self, output: &mut String) {
        self.operator.summary(output);
    }

    fn fixedpoint(&self, scope: Scope) -> bool {
        self.operator.fixedpoint(scope)
    }
}

struct SourceNode<C, O, Op> {
    id: GlobalNodeId,
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
            id: circuit.global_node_id().child(id),
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
    O: Clone,
    Op: SourceOperator<O>,
{
    fn name(&self) -> Cow<'static, str> {
        self.operator.name()
    }

    fn local_id(&self) -> NodeId {
        self.id.local_node_id().unwrap()
    }

    fn global_id(&self) -> &GlobalNodeId {
        &self.id
    }

    fn is_async(&self) -> bool {
        self.operator.is_async()
    }

    fn ready(&self) -> bool {
        self.operator.ready()
    }

    fn register_ready_callback(&mut self, cb: Box<dyn Fn() + Send + Sync>) {
        self.operator.register_ready_callback(cb);
    }

    unsafe fn eval(&mut self) -> Result<(), SchedulerError> {
        self.output_stream.put(self.operator.eval());
        Ok(())
    }

    fn clock_start(&mut self, scope: Scope) {
        self.operator.clock_start(scope);
    }

    unsafe fn clock_end(&mut self, scope: Scope) {
        self.operator.clock_end(scope);
    }

    fn summary(&self, output: &mut String) {
        self.operator.summary(output);
    }

    fn fixedpoint(&self, scope: Scope) -> bool {
        self.operator.fixedpoint(scope)
    }
}

struct UnaryNode<C, I, O, Op> {
    id: GlobalNodeId,
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
            id: circuit.global_node_id().child(id),
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
    I: Clone,
    O: Clone,
    Op: UnaryOperator<I, O>,
{
    fn name(&self) -> Cow<'static, str> {
        self.operator.name()
    }

    fn local_id(&self) -> NodeId {
        self.id.local_node_id().unwrap()
    }

    fn global_id(&self) -> &GlobalNodeId {
        &self.id
    }

    fn is_async(&self) -> bool {
        self.operator.is_async()
    }

    fn ready(&self) -> bool {
        self.operator.ready()
    }

    fn register_ready_callback(&mut self, cb: Box<dyn Fn() + Send + Sync>) {
        self.operator.register_ready_callback(cb);
    }

    unsafe fn eval(&mut self) -> Result<(), SchedulerError> {
        self.output_stream.put(match self.input_stream.take() {
            Cow::Owned(v) => self.operator.eval_owned(v),
            Cow::Borrowed(v) => self.operator.eval(v),
        });
        Ok(())
    }

    fn clock_start(&mut self, scope: Scope) {
        self.operator.clock_start(scope);
    }

    unsafe fn clock_end(&mut self, scope: Scope) {
        self.operator.clock_end(scope);
    }

    fn summary(&self, output: &mut String) {
        self.operator.summary(output);
    }

    fn fixedpoint(&self, scope: Scope) -> bool {
        self.operator.fixedpoint(scope)
    }
}

struct SinkNode<C, I, Op> {
    id: GlobalNodeId,
    operator: Op,
    input_stream: Stream<C, I>,
}

impl<P, I, Op> SinkNode<Circuit<P>, I, Op>
where
    Op: SinkOperator<I>,
{
    fn new(
        operator: Op,
        input_stream: Stream<Circuit<P>, I>,
        circuit: Circuit<P>,
        id: NodeId,
    ) -> Self {
        Self {
            id: circuit.global_node_id().child(id),
            operator,
            input_stream,
        }
    }
}

impl<C, I, Op> Node for SinkNode<C, I, Op>
where
    I: Clone,
    Op: SinkOperator<I>,
{
    fn name(&self) -> Cow<'static, str> {
        self.operator.name()
    }

    fn local_id(&self) -> NodeId {
        self.id.local_node_id().unwrap()
    }

    fn global_id(&self) -> &GlobalNodeId {
        &self.id
    }

    fn is_async(&self) -> bool {
        self.operator.is_async()
    }

    fn ready(&self) -> bool {
        self.operator.ready()
    }

    fn register_ready_callback(&mut self, cb: Box<dyn Fn() + Send + Sync>) {
        self.operator.register_ready_callback(cb);
    }

    unsafe fn eval(&mut self) -> Result<(), SchedulerError> {
        match self.input_stream.take() {
            Cow::Owned(v) => self.operator.eval_owned(v),
            Cow::Borrowed(v) => self.operator.eval(v),
        };
        Ok(())
    }

    fn clock_start(&mut self, scope: Scope) {
        self.operator.clock_start(scope);
    }

    unsafe fn clock_end(&mut self, scope: Scope) {
        self.operator.clock_end(scope);
    }

    fn summary(&self, output: &mut String) {
        self.operator.summary(output);
    }

    fn fixedpoint(&self, scope: Scope) -> bool {
        self.operator.fixedpoint(scope)
    }
}

struct BinaryNode<C, I1, I2, O, Op> {
    id: GlobalNodeId,
    operator: Op,
    input_stream1: Stream<C, I1>,
    input_stream2: Stream<C, I2>,
    output_stream: Stream<C, O>,
    // `true` if both input streams are aliases of the same stream.
    is_alias: bool,
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
        let is_alias = input_stream1.ptr_eq(&input_stream2);
        Self {
            id: circuit.global_node_id().child(id),
            operator,
            input_stream1,
            input_stream2,
            is_alias,
            output_stream: Stream::new(circuit, id),
        }
    }

    fn output_stream(&self) -> Stream<Circuit<P>, O> {
        self.output_stream.clone()
    }
}

impl<C, I1, I2, O, Op> Node for BinaryNode<C, I1, I2, O, Op>
where
    I1: Clone,
    I2: Clone,
    O: Clone,
    Op: BinaryOperator<I1, I2, O>,
{
    fn name(&self) -> Cow<'static, str> {
        self.operator.name()
    }

    fn local_id(&self) -> NodeId {
        self.id.local_node_id().unwrap()
    }

    fn global_id(&self) -> &GlobalNodeId {
        &self.id
    }

    fn is_async(&self) -> bool {
        self.operator.is_async()
    }

    fn ready(&self) -> bool {
        self.operator.ready()
    }

    fn register_ready_callback(&mut self, cb: Box<dyn Fn() + Send + Sync>) {
        self.operator.register_ready_callback(cb);
    }

    unsafe fn eval(&mut self) -> Result<(), SchedulerError> {
        // If the two input streams are aliases, we cannot remove the owned
        // value from `input_stream2`, as this will invalidate the borrow
        // from `input_stream1`.  Instead use `peek` to obtain the value by
        // reference.
        if self.is_alias {
            self.output_stream.put(
                match (self.input_stream1.take(), self.input_stream2.peek()) {
                    (Cow::Borrowed(v1), v2) => self.operator.eval(v1, v2),
                    _ => unreachable!(),
                },
            );
            // It is now safe to call `take`, and we must do so to decrement
            // the ref counter.
            let _ = self.input_stream2.take();
        } else {
            self.output_stream.put(
                match (self.input_stream1.take(), self.input_stream2.take()) {
                    (Cow::Owned(v1), Cow::Owned(v2)) => self.operator.eval_owned(v1, v2),
                    (Cow::Owned(v1), Cow::Borrowed(v2)) => self.operator.eval_owned_and_ref(v1, v2),
                    (Cow::Borrowed(v1), Cow::Owned(v2)) => self.operator.eval_ref_and_owned(v1, v2),
                    (Cow::Borrowed(v1), Cow::Borrowed(v2)) => self.operator.eval(v1, v2),
                },
            );
        }
        Ok(())
    }

    fn clock_start(&mut self, scope: Scope) {
        self.operator.clock_start(scope);
    }

    unsafe fn clock_end(&mut self, scope: Scope) {
        self.operator.clock_end(scope);
    }

    fn summary(&self, output: &mut String) {
        self.operator.summary(output);
    }

    fn fixedpoint(&self, scope: Scope) -> bool {
        self.operator.fixedpoint(scope)
    }
}

struct TernaryNode<C, I1, I2, I3, O, Op> {
    id: GlobalNodeId,
    operator: Op,
    input_stream1: Stream<C, I1>,
    input_stream2: Stream<C, I2>,
    input_stream3: Stream<C, I3>,
    output_stream: Stream<C, O>,
    // `true` if `input_stream1` is an alias to `input_stream2` or `input_stream3`.
    is_alias1: bool,
    // `true` if `input_stream2` is an alias to `input_stream3`.
    is_alias2: bool,
}

impl<P, I1, I2, I3, O, Op> TernaryNode<Circuit<P>, I1, I2, I3, O, Op>
where
    I1: Clone,
    I2: Clone,
    I3: Clone,
    Op: TernaryOperator<I1, I2, I3, O>,
    P: Clone,
{
    fn new(
        operator: Op,
        input_stream1: Stream<Circuit<P>, I1>,
        input_stream2: Stream<Circuit<P>, I2>,
        input_stream3: Stream<Circuit<P>, I3>,
        circuit: Circuit<P>,
        id: NodeId,
    ) -> Self {
        let is_alias1 =
            input_stream1.ptr_eq(&input_stream2) || input_stream1.ptr_eq(&input_stream3);
        let is_alias2 = input_stream2.ptr_eq(&input_stream3);
        Self {
            id: circuit.global_node_id().child(id),
            operator,
            input_stream1,
            input_stream2,
            input_stream3,
            is_alias1,
            is_alias2,
            output_stream: Stream::new(circuit, id),
        }
    }

    fn output_stream(&self) -> Stream<Circuit<P>, O> {
        self.output_stream.clone()
    }
}

impl<C, I1, I2, I3, O, Op> Node for TernaryNode<C, I1, I2, I3, O, Op>
where
    I1: Clone,
    I2: Clone,
    I3: Clone,
    O: Clone,
    Op: TernaryOperator<I1, I2, I3, O>,
{
    fn name(&self) -> Cow<'static, str> {
        self.operator.name()
    }

    fn local_id(&self) -> NodeId {
        self.id.local_node_id().unwrap()
    }

    fn global_id(&self) -> &GlobalNodeId {
        &self.id
    }

    fn is_async(&self) -> bool {
        self.operator.is_async()
    }

    fn ready(&self) -> bool {
        self.operator.ready()
    }

    fn register_ready_callback(&mut self, cb: Box<dyn Fn() + Send + Sync>) {
        self.operator.register_ready_callback(cb);
    }

    unsafe fn eval(&mut self) -> Result<(), SchedulerError> {
        let input1 = if self.is_alias1 {
            Cow::Borrowed(self.input_stream1.peek())
        } else {
            self.input_stream1.take()
        };

        let input2 = if self.is_alias2 {
            Cow::Borrowed(self.input_stream2.peek())
        } else {
            self.input_stream2.take()
        };

        let input3 = self.input_stream3.take();

        self.output_stream
            .put(self.operator.eval(input1, input2, input3));
        Ok(())
    }

    fn clock_start(&mut self, scope: Scope) {
        self.operator.clock_start(scope);
    }

    unsafe fn clock_end(&mut self, scope: Scope) {
        self.operator.clock_end(scope);
    }

    fn summary(&self, output: &mut String) {
        self.operator.summary(output);
    }

    fn fixedpoint(&self, scope: Scope) -> bool {
        self.operator.fixedpoint(scope)
    }
}

struct NaryNode<C, I, O, Op>
where
    I: Clone + 'static,
{
    id: GlobalNodeId,
    operator: Op,
    // The second field of the tuple indicates if the stream is an
    // alias to an earlier stream.
    input_streams: Vec<(Stream<C, I>, bool)>,
    // Streams that are aliases.
    aliases: Vec<usize>,
    output_stream: Stream<C, O>,
}

impl<P, I, O, Op> NaryNode<Circuit<P>, I, O, Op>
where
    I: Clone + 'static,
    Op: NaryOperator<I, O>,
    P: Clone,
{
    fn new<Iter>(operator: Op, input_streams: Iter, circuit: Circuit<P>, id: NodeId) -> Self
    where
        Iter: IntoIterator<Item = Stream<Circuit<P>, I>>,
    {
        let mut input_streams: Vec<_> = input_streams
            .into_iter()
            .map(|stream| (stream, false))
            .collect();
        let mut aliases = Vec::new();
        for i in 0..input_streams.len() {
            for j in 0..i {
                if input_streams[i].0.ptr_eq(&input_streams[j].0) {
                    input_streams[i].1 = true;
                    aliases.push(i);
                    break;
                }
            }
        }
        aliases.shrink_to_fit();
        input_streams.shrink_to_fit();
        Self {
            id: circuit.global_node_id().child(id),
            operator,
            input_streams,
            aliases,
            output_stream: Stream::new(circuit, id),
        }
    }

    fn output_stream(&self) -> Stream<Circuit<P>, O> {
        self.output_stream.clone()
    }
}

impl<C, I, O, Op> Node for NaryNode<C, I, O, Op>
where
    I: Clone,
    O: Clone,
    Op: NaryOperator<I, O>,
{
    fn name(&self) -> Cow<'static, str> {
        self.operator.name()
    }

    fn local_id(&self) -> NodeId {
        self.id.local_node_id().unwrap()
    }

    fn global_id(&self) -> &GlobalNodeId {
        &self.id
    }

    fn is_async(&self) -> bool {
        self.operator.is_async()
    }

    fn ready(&self) -> bool {
        self.operator.ready()
    }

    fn register_ready_callback(&mut self, cb: Box<dyn Fn() + Send + Sync>) {
        self.operator.register_ready_callback(cb);
    }

    unsafe fn eval(&mut self) -> Result<(), SchedulerError> {
        self.output_stream
            .put(
                self.operator
                    .eval(self.input_streams.iter().map(|(stream, alias)| {
                        // Don't take owned value via an alias.
                        if *alias {
                            Cow::Borrowed(stream.peek())
                        } else {
                            stream.take()
                        }
                    })),
            );

        for i in self.aliases.iter() {
            let _ = self.input_streams[*i].0.take();
        }
        Ok(())
    }

    fn clock_start(&mut self, scope: Scope) {
        self.operator.clock_start(scope);
    }

    unsafe fn clock_end(&mut self, scope: Scope) {
        self.operator.clock_end(scope);
    }

    fn summary(&self, output: &mut String) {
        self.operator.summary(output);
    }

    fn fixedpoint(&self, scope: Scope) -> bool {
        self.operator.fixedpoint(scope)
    }
}

// The output half of a feedback node.  We implement a feedback node using a
// pair of nodes: `FeedbackOutputNode` is connected to the circuit as a source
// node (i.e., it does not have an input stream) and thus gets evaluated first
// in each time stamp.  `FeedbackInputNode` is a sink node.  This way the
// circuit graph remains acyclic and can be scheduled in a topological order.
struct FeedbackOutputNode<P, I, O, Op> {
    id: GlobalNodeId,
    operator: Rc<UnsafeCell<Op>>,
    output_stream: Stream<Circuit<P>, O>,
    export_stream: Stream<P, O>,
    phantom_input: PhantomData<I>,
}

impl<P, I, O, Op> FeedbackOutputNode<P, I, O, Op>
where
    P: Clone + 'static,
    Op: StrictUnaryOperator<I, O>,
{
    fn new(operator: Rc<UnsafeCell<Op>>, circuit: Circuit<P>, id: NodeId) -> Self {
        Self {
            id: circuit.global_node_id().child(id),
            operator,
            output_stream: Stream::new(circuit.clone(), id),
            export_stream: Stream::with_origin(
                circuit.parent(),
                circuit.node_id(),
                GlobalNodeId::child_of(&circuit, id),
            ),
            phantom_input: PhantomData,
        }
    }

    fn output_stream(&self) -> Stream<Circuit<P>, O> {
        self.output_stream.clone()
    }
}

impl<P, I, O, Op> Node for FeedbackOutputNode<P, I, O, Op>
where
    I: Data,
    O: Clone,
    Op: StrictUnaryOperator<I, O>,
{
    fn name(&self) -> Cow<'static, str> {
        unsafe { &*self.operator.get() }.name()
    }

    fn local_id(&self) -> NodeId {
        self.id.local_node_id().unwrap()
    }

    fn global_id(&self) -> &GlobalNodeId {
        &self.id
    }

    fn is_async(&self) -> bool {
        unsafe { &*self.operator.get() }.is_async()
    }

    fn ready(&self) -> bool {
        unsafe { &*self.operator.get() }.ready()
    }

    fn register_ready_callback(&mut self, cb: Box<dyn Fn() + Send + Sync>) {
        unsafe { &mut *self.operator.get() }.register_ready_callback(cb);
    }

    unsafe fn eval(&mut self) -> Result<(), SchedulerError> {
        self.output_stream
            .put((&mut *self.operator.get()).get_output());
        Ok(())
    }

    fn clock_start(&mut self, scope: Scope) {
        unsafe {
            (&mut *self.operator.get()).clock_start(scope);
        }
    }

    unsafe fn clock_end(&mut self, scope: Scope) {
        if scope == 0 {
            self.export_stream
                .put((&mut *self.operator.get()).get_final_output())
        }
        (&mut *self.operator.get()).clock_end(scope);
    }

    fn summary(&self, output: &mut String) {
        unsafe {
            (&*self.operator.get()).summary(output);
        }
    }

    fn fixedpoint(&self, scope: Scope) -> bool {
        unsafe { (&*self.operator.get()).fixedpoint(scope) }
    }
}

/// The input half of a feedback node
struct FeedbackInputNode<C, I, O, Op> {
    // Id of this node (the input half).
    id: GlobalNodeId,
    operator: Rc<UnsafeCell<Op>>,
    input_stream: Stream<C, I>,
    phantom_output: PhantomData<O>,
}

impl<P, I, O, Op> FeedbackInputNode<Circuit<P>, I, O, Op>
where
    Op: StrictUnaryOperator<I, O>,
{
    fn new(operator: Rc<UnsafeCell<Op>>, input_stream: Stream<Circuit<P>, I>, id: NodeId) -> Self {
        Self {
            id: input_stream.circuit().global_node_id().child(id),
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
    fn name(&self) -> Cow<'static, str> {
        unsafe { &*self.operator.get() }.name()
    }

    fn local_id(&self) -> NodeId {
        self.id.local_node_id().unwrap()
    }

    fn global_id(&self) -> &GlobalNodeId {
        &self.id
    }

    fn is_async(&self) -> bool {
        unsafe { &*self.operator.get() }.is_async()
    }

    fn ready(&self) -> bool {
        unsafe { &*self.operator.get() }.ready()
    }

    fn register_ready_callback(&mut self, cb: Box<dyn Fn() + Send + Sync>) {
        unsafe { &mut *self.operator.get() }.register_ready_callback(cb);
    }

    unsafe fn eval(&mut self) -> Result<(), SchedulerError> {
        match self.input_stream.take() {
            Cow::Owned(v) => (&mut *self.operator.get()).eval_strict_owned(v),
            Cow::Borrowed(v) => (&mut *self.operator.get()).eval_strict(v),
        };
        Ok(())
    }

    // Don't call `clock_start`/`clock_end` on the operator.  `FeedbackOutputNode`
    // will do that.
    fn clock_start(&mut self, _scope: Scope) {}

    unsafe fn clock_end(&mut self, _scope: Scope) {}

    fn summary(&self, output: &mut String) {
        unsafe {
            (&*self.operator.get()).summary(output);
        }
    }

    fn fixedpoint(&self, scope: Scope) -> bool {
        unsafe { (&*self.operator.get()).fixedpoint(scope) }
    }
}

/// Input connector of a feedback operator.
///
/// This struct is part of the mechanism for constructing a feedback loop in a
/// circuit. It is returned by [`Circuit::add_feedback`] and represents the
/// input port of an operator whose input stream does not exist yet.  Once the
/// input stream has been created, it can be connected to the operator using
/// [`FeedbackConnector::connect`]. See [`Circuit::add_feedback`] for details.
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
    ///
    /// See [`Circuit::add_feedback`] for details.
    /// Returns node id of the input node.
    pub fn connect(self, input_stream: &Stream<Circuit<P>, I>) {
        self.connect_with_preference(input_stream, OwnershipPreference::INDIFFERENT)
    }

    pub fn connect_with_preference(
        self,
        input_stream: &Stream<Circuit<P>, I>,
        input_preference: OwnershipPreference,
    ) {
        self.circuit.connect_feedback_with_preference(
            self.output_node_id,
            self.operator,
            input_stream,
            input_preference,
        );
    }
}

// A nested circuit instantiated as a node in a parent circuit.
struct ChildNode<P> {
    id: GlobalNodeId,
    circuit: Circuit<P>,
    executor: Box<dyn Executor<P>>,
}

impl<P> Drop for ChildNode<P> {
    fn drop(&mut self) {
        // Explicitly deallocate all nodes in the circuit to break
        // cyclic `Rc` references between circuits and streams.
        self.circuit.clear();
    }
}

impl<P> ChildNode<P> {
    fn new<E>(circuit: Circuit<P>, executor: E) -> Self
    where
        E: Executor<P>,
    {
        Self {
            id: circuit.global_node_id(),
            circuit,
            executor: Box::new(executor) as Box<dyn Executor<P>>,
        }
    }
}

impl<P> Node for ChildNode<P>
where
    P: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("Subcircuit")
    }

    fn local_id(&self) -> NodeId {
        self.id.local_node_id().unwrap()
    }

    fn global_id(&self) -> &GlobalNodeId {
        &self.id
    }

    fn is_async(&self) -> bool {
        false
    }

    fn ready(&self) -> bool {
        true
    }

    unsafe fn eval(&mut self) -> Result<(), SchedulerError> {
        self.executor.run(&self.circuit)
    }

    fn clock_start(&mut self, scope: Scope) {
        self.circuit.clock_start(scope + 1);
    }

    unsafe fn clock_end(&mut self, scope: Scope) {
        self.circuit.clock_end(scope + 1);
    }

    fn summary(&self, output: &mut String) {
        output.clear();
    }

    fn fixedpoint(&self, scope: Scope) -> bool {
        self.circuit.inner().fixedpoint(scope + 1)
    }
}

/// Top-level circuit with executor.
pub struct Root {
    circuit: Circuit<()>,
    executor: Box<dyn Executor<()>>,
}

impl Drop for Root {
    fn drop(&mut self) {
        self.circuit
            .log_scheduler_event(&SchedulerEvent::clock_end());
        unsafe {
            self.circuit.clock_end(0);
        }
        // We must explicitly deallocate all nodes in the circuit to break
        // cyclic `Rc` references between circuits and streams.  Alternatively,
        // we could use weak references to break cycles, but we'd have to
        // pay the cost of upgrading weak references on each access.
        self.circuit.clear();
    }
}

impl Root {
    /// Create a circuit and prepate it for execution.
    ///
    /// Creates an empty circuit and populates it with operators by calling a
    /// user-provided `constructor` function.  The circuit will be scheduled
    /// using the default scheduler (currently [`DynamicScheduler`]).
    pub fn build<F>(constructor: F) -> Result<Self, SchedulerError>
    where
        F: FnOnce(&mut Circuit<()>),
    {
        // TODO: use dynamic scheduler by default.
        Self::build_with_scheduler::<F, DynamicScheduler>(constructor)
    }

    /// Create a circuit and prepate it for execution.
    ///
    /// Similar to [`build`](`Self::build`), but with a user-specified
    /// [`Scheduler`] implementation.
    pub fn build_with_scheduler<F, S>(constructor: F) -> Result<Self, SchedulerError>
    where
        F: FnOnce(&mut Circuit<()>),
        S: Scheduler + 'static,
    {
        let mut circuit = Circuit::new();
        constructor(&mut circuit);
        let executor = Box::new(<OnceExecutor<S>>::new(&circuit)?) as Box<dyn Executor<()>>;

        // Alternatively, `Root` should expose `clock_start` and `clock_end` APIs, so
        // that the user can reset the circuit at runtime and start evaluation
        // from clean state without having to rebuild it from scratch.
        circuit.log_scheduler_event(&SchedulerEvent::clock_start());
        circuit.clock_start(0);
        Ok(Self { circuit, executor })
    }

    /// Function that drives the execution of the circuit.
    ///
    /// Every call to `step()` corresponds to one tick of the global logical
    /// clock and causes each operator in the circuit to get evaluated once,
    /// consuming one value from each of its input streams.
    pub fn step(&self) -> Result<(), SchedulerError> {
        // TODO: Add a runtime check to prevent re-entering this method from an
        // operator.

        self.executor.run(&self.circuit)
    }

    /// Attach a scheduler event handler to the circuit.
    ///
    /// This method is identical to
    /// [`Circuit::register_scheduler_event_handler`], but it can be used at
    /// runtime, after the circuit has been fully constructed.
    ///
    /// Use [`Circuit::register_scheduler_event_handler`],
    /// [`Circuit::unregister_scheduler_event_handler`], to manipulate
    /// handlers during circuit construction.
    pub fn register_scheduler_event_handler<F>(&self, name: &str, handler: F)
    where
        F: FnMut(&SchedulerEvent<'_>) + 'static,
    {
        self.circuit.register_scheduler_event_handler(name, handler);
    }

    /// Remove a scheduler event handler.
    ///
    /// This method is identical to
    /// [`Circuit::unregister_scheduler_event_handler`], but it can be used at
    /// runtime, after the circuit has been fully constructed.
    pub fn unregister_scheduler_event_handler(&self, name: &str) -> bool {
        self.circuit.unregister_scheduler_event_handler(name)
    }
}

#[cfg(test)]
mod tests {
    use super::Root;
    use crate::{
        circuit::schedule::{DynamicScheduler, Scheduler, StaticScheduler},
        monitor::TraceMonitor,
        operator::{Apply2, Generator, Inspect, Z1},
    };
    use std::{cell::RefCell, ops::Deref, rc::Rc, vec::Vec};

    // Compute the sum of numbers from 0 to 99.
    #[test]
    fn sum_circuit_static() {
        sum_circuit::<StaticScheduler>();
    }

    #[test]
    fn sum_circuit_dynamic() {
        sum_circuit::<DynamicScheduler>();
    }

    // Compute the sum of numbers from 0 to 99.
    fn sum_circuit<S>()
    where
        S: Scheduler + 'static,
    {
        let actual_output: Rc<RefCell<Vec<isize>>> = Rc::new(RefCell::new(Vec::with_capacity(100)));
        let actual_output_clone = actual_output.clone();
        let root = Root::build_with_scheduler::<_, S>(|circuit| {
            TraceMonitor::new_panic_on_error().attach(circuit, "monitor");
            let mut n: isize = 0;
            let source = circuit.add_source(Generator::new(move || {
                let result = n;
                n += 1;
                result
            }));
            let integrator = source.integrate();
            integrator.inspect(|n| println!("{}", n));
            integrator.inspect(move |n| actual_output_clone.borrow_mut().push(*n));
        })
        .unwrap();

        for _ in 0..100 {
            root.step().unwrap();
        }

        let mut sum = 0;
        let mut expected_output: Vec<isize> = Vec::with_capacity(100);
        for i in 0..100 {
            sum += i;
            expected_output.push(sum);
        }
        assert_eq!(&expected_output, actual_output.borrow().deref());
    }

    // Recursive circuit
    #[test]
    fn recursive_sum_circuit_static() {
        recursive_sum_circuit::<StaticScheduler>()
    }

    #[test]
    fn recursive_sum_circuit_dynamic() {
        recursive_sum_circuit::<DynamicScheduler>()
    }

    fn recursive_sum_circuit<S>()
    where
        S: Scheduler + 'static,
    {
        let actual_output: Rc<RefCell<Vec<usize>>> = Rc::new(RefCell::new(Vec::with_capacity(100)));
        let actual_output_clone = actual_output.clone();

        let root = Root::build_with_scheduler::<_, S>(|circuit| {
            TraceMonitor::new_panic_on_error().attach(circuit, "monitor");

            let mut n: usize = 0;
            let source = circuit.add_source(Generator::new(move || {
                let result = n;
                n += 1;
                result
            }));
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
        })
        .unwrap();

        for _ in 0..100 {
            root.step().unwrap();
        }

        let mut sum = 0;
        let mut expected_output: Vec<usize> = Vec::with_capacity(100);
        for i in 0..100 {
            sum += i;
            expected_output.push(sum);
        }
        assert_eq!(&expected_output, actual_output.borrow().deref());
    }

    #[test]
    fn factorial_static() {
        factorial::<StaticScheduler>();
    }

    #[test]
    fn factorial_dynamic() {
        factorial::<DynamicScheduler>();
    }

    // Nested circuit.  The root circuit contains a source node that counts up from
    // 1.  For each `n` output by the source node, the nested circuit computes
    // factorial(n) using a `NestedSource` operator that counts from n down to
    // `1` and a multiplier that multiplies the next count by the product
    // computed so far (stored in z-1).
    fn factorial<S>()
    where
        S: Scheduler + 'static,
    {
        let actual_output: Rc<RefCell<Vec<usize>>> = Rc::new(RefCell::new(Vec::with_capacity(100)));
        let actual_output_clone = actual_output.clone();

        let root = Root::build_with_scheduler::<_, S>(|circuit| {
            TraceMonitor::new_panic_on_error().attach(circuit, "monitor");

            let mut n: usize = 0;
            let source = circuit.add_source(Generator::new(move || {
                n += 1;
                n
            }));
            let fact = circuit
                .iterate_with_condition_and_scheduler::<_, _, S>(|child| {
                    let mut counter = 0;
                    let countdown = source.delta0(child).apply(move |parent_val| {
                        if *parent_val > 0 {
                            counter = *parent_val;
                        };
                        let res = counter;
                        counter -= 1;
                        res
                    });
                    let (z1_output, z1_feedback) = child.add_feedback_with_export(Z1::new(1));
                    let mul = child.add_binary_operator(
                        Apply2::new(|n1: &usize, n2: &usize| n1 * n2),
                        &countdown,
                        &z1_output.local,
                    );
                    z1_feedback.connect(&mul);
                    Ok((countdown.condition(|n| *n <= 1), z1_output.export))
                })
                .unwrap();
            circuit.add_sink(
                Inspect::new(move |n| actual_output_clone.borrow_mut().push(*n)),
                &fact,
            );
        })
        .unwrap();

        for _ in 1..10 {
            root.step().unwrap();
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
