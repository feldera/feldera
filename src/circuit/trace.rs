//! Definitions related to tracing the execution of a circuit.
//!
//! Circuit API users can register handlers for two types of events:
//! [`CircuitEvent`]s emitted during circuit construction and carrying
//! information about circuit topology (operators, subcircuits, and
//! streams connecting them),
//! and
//! [`SchedulerEvent`]s carrying information about circuit's runtime
//! behavior.
//!
//! See [`super::Circuit::register_circuit_event_handler`] and
//! [`super::Root::register_scheduler_event_handler`] APIs for attaching
//! event handlers to a circuit.
//!
//! Event handlers are invoked synchronously and therefore must complete
//! quickly, with any expensive processing completed asynchronously.

use std::{borrow::Cow, fmt, fmt::Display, hash::Hash};

use super::{GlobalNodeId, NodeId, OwnershipPreference};
pub use trace_monitor::TraceMonitor;

/// Type of edge in a circuit graph.
#[derive(Debug, Eq, PartialEq, Clone, Hash)]
pub enum EdgeKind {
    /// A stream edge indicates that there is a stream that connects two
    /// operators.
    Stream(OwnershipPreference),
    /// A dependency edge indicates that the source operator must be evaluated
    /// before the destination.
    Dependency,
}

/// Events related to circuit construction.  A handler listening to these
/// events should be able to reconstruct complete circuit topology,
/// including operators, nested circuits, and streams connecting them.
#[derive(Debug, Eq, PartialEq, Clone, Hash)]
pub enum CircuitEvent {
    /// A new regular (non-strict) operator is added to the circuit.
    Operator {
        /// Global id of the new operator.
        node_id: GlobalNodeId,
        /// Operator name.
        name: Cow<'static, str>,
    },
    /// The output half of a
    /// [`StrictOperator`](`crate::circuit::operator_traits::StrictOperator`).
    /// A strict operator is activated twice in each clock cycle: first, its
    /// output computed based on previous inputs is read; second, a new
    /// input for the current cycle is pushed to the operator.  These
    /// activations are modeled as separate circuit nodes.  The
    /// `StrictOperatorOutput` event is emitted first, when the output node is
    /// created.
    StrictOperatorOutput {
        node_id: GlobalNodeId,
        /// Operator name.
        name: Cow<'static, str>,
    },
    /// The input half of a strict operator is added to the circuit.  This event
    /// is triggered when the circuit builder connects an input stream to
    /// the strict operator.  The output node already exists at this point.
    StrictOperatorInput {
        /// Node id of the input half.
        node_id: GlobalNodeId,
        /// Node id of the associated output node, that already exists in the
        /// circuit.
        output_node_id: NodeId,
    },
    /// A new nested circuit is added to the circuit.
    Subcircuit {
        /// Global id of the nested circuit.
        node_id: GlobalNodeId,
        /// `true` is the subcircuit has its own clock and can iterate multiple
        /// times for each parent clock tick.
        iterative: bool,
    },
    /// A new edge between nodes connected as producer and consumer to the same
    /// stream. Producer and consumer nodes can be located in different
    /// subcircuits.
    Edge {
        kind: EdgeKind,
        /// Global id of the producer operator that writes to the stream.
        from: GlobalNodeId,
        /// Global id of an operator or subcircuit that reads values from the
        /// stream.
        to: GlobalNodeId,
    },
}

impl Display for CircuitEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Operator { node_id, name } => {
                write!(f, "Operator(\"{}\", {})", name, node_id)
            }
            Self::StrictOperatorOutput { node_id, name } => {
                write!(f, "StrictOperatorOutput(\"{}\", {})", name, node_id)
            }
            Self::StrictOperatorInput {
                node_id,
                output_node_id,
            } => {
                write!(f, "StrictOperatorInput({} -> {})", node_id, output_node_id)
            }
            Self::Subcircuit { node_id, iterative } => {
                write!(
                    f,
                    "{}Subcircuit({})",
                    if *iterative { "Iterative" } else { "" },
                    node_id,
                )
            }
            Self::Edge {
                kind: EdgeKind::Stream(preference),
                from,
                to,
            } => {
                write!(f, "Stream({} -> [{}]{})", from, preference, to)
            }
            Self::Edge {
                kind: EdgeKind::Dependency,
                from,
                to,
            } => {
                write!(f, "Dependency({} -> {})", from, to)
            }
        }
    }
}

impl CircuitEvent {
    /// Create a [`CircuitEvent::Operator`] event instance.
    pub fn operator(node_id: GlobalNodeId, name: Cow<'static, str>) -> Self {
        Self::Operator { node_id, name }
    }
    /// Create a [`CircuitEvent::StrictOperatorOutput`] event instance.
    pub fn strict_operator_output(node_id: GlobalNodeId, name: Cow<'static, str>) -> Self {
        Self::StrictOperatorOutput { node_id, name }
    }
    /// Create a [`CircuitEvent::StrictOperatorInput`] event instance.
    pub fn strict_operator_input(node_id: GlobalNodeId, output_node_id: NodeId) -> Self {
        Self::StrictOperatorInput {
            node_id,
            output_node_id,
        }
    }

    /// Create a [`CircuitEvent::Subcircuit`] event instance.
    pub fn subcircuit(node_id: GlobalNodeId, iterative: bool) -> Self {
        Self::Subcircuit { node_id, iterative }
    }

    /// Create a [`CircuitEvent::Edge`] event instance.
    pub fn stream(
        from: GlobalNodeId,
        to: GlobalNodeId,
        ownership_preference: OwnershipPreference,
    ) -> Self {
        Self::Edge {
            kind: EdgeKind::Stream(ownership_preference),
            from,
            to,
        }
    }

    /// Create a [`CircuitEvent::Edge`] event instance.
    pub fn dependency(from: GlobalNodeId, to: GlobalNodeId) -> Self {
        Self::Edge {
            kind: EdgeKind::Dependency,
            from,
            to,
        }
    }

    /// `true` if `self` is a [`CircuitEvent::StrictOperatorInput`]
    pub fn is_strict_input_event(&self) -> bool {
        matches!(self, Self::StrictOperatorInput { .. })
    }

    /// `true` if `self` is a [`CircuitEvent::StrictOperatorOutput`]
    pub fn is_strict_output_event(&self) -> bool {
        matches!(self, Self::StrictOperatorOutput { .. })
    }

    /// `true` if `self` is a [`CircuitEvent::Operator`]
    pub fn is_operator_event(&self) -> bool {
        matches!(self, Self::Operator { .. })
    }

    /// `true` if `self` is a [`CircuitEvent::Subcircuit`]
    pub fn is_subcircuit_event(&self) -> bool {
        matches!(self, Self::Subcircuit { .. })
    }

    /// `true` if `self` is a [`CircuitEvent::Subcircuit`] and `self.iterative`
    /// is `true`.
    pub fn is_iterative_subcircuit_event(&self) -> bool {
        matches!(
            self,
            Self::Subcircuit {
                iterative: true,
                ..
            }
        )
    }

    /// `true` if `self` is a [`CircuitEvent::Edge`]
    pub fn is_edge_event(&self) -> bool {
        matches!(self, Self::Edge { .. })
    }

    /// `true` if `self` is one of the node creation events:
    /// [`CircuitEvent::Operator`], [`CircuitEvent::StrictOperatorOutput`],
    /// [`CircuitEvent::StrictOperatorInput`], or [`CircuitEvent::Subcircuit`].
    pub fn is_node_event(&self) -> bool {
        matches!(
            self,
            Self::Operator { .. }
                | Self::StrictOperatorOutput { .. }
                | Self::StrictOperatorInput { .. }
                | Self::Subcircuit { .. }
        )
    }

    /// If `self` is one of the node creation events, returns `self.node_id`.
    pub fn node_id(&self) -> Option<&GlobalNodeId> {
        match self {
            Self::Operator { node_id, .. }
            | Self::StrictOperatorOutput { node_id, .. }
            | Self::StrictOperatorInput { node_id, .. }
            | Self::Subcircuit { node_id, .. } => Some(node_id),
            _ => None,
        }
    }

    /// If `self` is a [`CircuitEvent::Operator`] or
    /// [`CircuitEvent::StrictOperatorOutput`], returns `self.name`.
    pub fn node_name(&self) -> Option<&str> {
        match self {
            Self::Operator { name, .. } | Self::StrictOperatorOutput { name, .. } => Some(name),
            _ => None,
        }
    }

    /// If `self` is a `CircuitEvent::StrictOperatorInput`, returns
    /// `self.output_node_id`.
    pub fn output_node_id(&self) -> Option<NodeId> {
        if let Self::StrictOperatorInput { output_node_id, .. } = self {
            Some(*output_node_id)
        } else {
            None
        }
    }

    /// If `self` is a `CircuitEvent::Edge`, returns `self.from`.
    pub fn from(&self) -> Option<&GlobalNodeId> {
        if let Self::Edge { from, .. } = self {
            Some(from)
        } else {
            None
        }
    }

    /// If `self` is a `CircuitEvent::Edge`, returns `self.to`.
    pub fn to(&self) -> Option<&GlobalNodeId> {
        if let Self::Edge { to, .. } = self {
            Some(to)
        } else {
            None
        }
    }

    /// If `self` is a `CircuitEvent::Edge`, returns `self.kind`.
    pub fn edge_kind(&self) -> Option<&EdgeKind> {
        if let Self::Edge { kind, .. } = self {
            Some(kind)
        } else {
            None
        }
    }
}

/// Scheduler events carry information about circuit evaluation at runtime.
///
/// # Circuit automata
///
/// The following automaton describes valid sequences of events generated
/// by a circuit (the root circuit or a subcircuit) that has its own clock
/// domain:
///
/// ```text
///                ClockStart             StepStart     EvalStart(id)
/// ──────►idle─────────────────────┐ ┌──────────────┐ ┌───────────┐
///                                 ▼ │              ▼ │           ▼
///                              running            step          eval
///                                 │ ▲              │ ▲           │
///            ◄────────────────────┘ └──────────────┘ └───────────┘
///                ClockEnd               StepEnd       EvalEnd(id)
/// ```
///
/// The root circuit automaton is instantiated by the [`super::Root::build`]
/// function.  A subcircuit automaton is instantiated when its parent scheduler
/// evaluates the node that contains this subcircuit (see below).
///
/// In the initial state, the circuit issues a
/// [`ClockStart`](`SchedulerEvent::ClockStart`) event to reset its clock and
/// transitions to the `running` state.  In this state, the circuit performs
/// multiple **steps**.  A single step evaluates the circuit for one clock
/// cycle.  The circuit signals the start of a new clock cycle by the
/// [`StepStart`](`SchedulerEvent::StepStart`) event. In the `step` state the
/// circuit evaluates each operator exactly once in an order determined by
/// the circuit's dataflow graph.  In the process it issues one
/// [`EvalStart`](`SchedulerEvent::EvalStart`)/
/// [`EvalEnd`](`SchedulerEvent::EvalEnd`) event pair for each of its child
/// nodes before emitting the [`StepEnd`](`SchedulerEvent::StepEnd`) event.
///
/// Evaluating a child that contains another circuit pushes the entire automaton
/// on the stack, instantiates a fresh circuit automaton, and runs it to
/// completion.
///
/// An automaton for a subcircuit without a separate clock is simpler as it
/// doesn't have [`ClockStart`](`SchedulerEvent::ClockStart`)/
/// [`ClockEnd`](`SchedulerEvent::ClockEnd`) events and performs exactly one
/// step on each activation:
///
/// ```text
///              StepStart    EvalStart(id)
/// ──────►idle─────────────┐ ┌───────────┐
///                         ▼ │           ▼
///                         step         eval
///                         │ ▲           │
///         ◄───────────────┘ └───────────┘
///               StepEnd      EvalEnd(id)
/// ```
pub enum SchedulerEvent {
    EvalStart { node_id: NodeId },
    EvalEnd { node_id: NodeId },
    StepStart,
    StepEnd,
    ClockStart,
    ClockEnd,
}

impl Display for SchedulerEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EvalStart { node_id } => {
                write!(f, "EvalStart({})", node_id)
            }
            Self::EvalEnd { node_id } => {
                write!(f, "EvalEnd({})", node_id)
            }
            Self::StepStart => f.write_str("StepStart"),
            Self::StepEnd => f.write_str("StepEnd"),
            Self::ClockStart => f.write_str("ClockStart"),
            Self::ClockEnd => f.write_str("ClockEnd"),
        }
    }
}

impl SchedulerEvent {
    /// Create a [`SchedulerEvent::EvalStart`] event instance.
    pub fn eval_start(node_id: NodeId) -> Self {
        Self::EvalStart { node_id }
    }

    /// Create a [`SchedulerEvent::EvalEnd`] event instance.
    pub fn eval_end(node_id: NodeId) -> Self {
        Self::EvalEnd { node_id }
    }

    /// Create a [`SchedulerEvent::StepStart`] event instance.
    pub fn step_start() -> Self {
        Self::StepStart
    }

    /// Create a [`SchedulerEvent::StepEnd`] event instance.
    pub fn step_end() -> Self {
        Self::StepEnd
    }

    /// Create a [`SchedulerEvent::ClockStart`] event instance.
    pub fn clock_start() -> Self {
        Self::ClockStart
    }

    /// Create a [`SchedulerEvent::ClockEnd`] event instance.
    pub fn clock_end() -> Self {
        Self::ClockEnd
    }
}

// Don't gate this with `#[cfg(test)]`, since this can be used to find bugs
// in the wild, not just in the context of tests in this crate.
mod trace_monitor {
    ///! Trace monitor that validates event traces from a circuit.  It is
    ///! used to test both the tracing mechanism and the circuit engine
    ///! itself.
    use std::{
        borrow::Cow,
        collections::{hash_map::Entry, HashMap, HashSet},
        fmt,
        fmt::Display,
        slice,
        sync::{Arc, Mutex},
    };

    use crate::circuit::{
        trace::{CircuitEvent, EdgeKind, SchedulerEvent},
        Circuit, GlobalNodeId, NodeId,
    };

    enum NodeKind {
        /// Regular operator.
        Operator,
        /// Root circuit or subcircuit.
        Circuit {
            iterative: bool,
            children: HashMap<NodeId, Node>,
        },
        /// The input half of a strict operator.
        StrictInput { output: NodeId },
    }

    /// A node in a circuit graph represents an operator or a circuit.
    struct Node {
        #[allow(dead_code)]
        name: String,
        kind: NodeKind,
    }

    impl Node {
        fn new(name: &str, kind: NodeKind) -> Self {
            Self {
                name: name.to_string(),
                kind,
            }
        }

        /// Lookup node in the subtree with the root in `self` by path.
        fn node_ref<'a>(&self, mut path: slice::Iter<'a, NodeId>) -> Option<&Node> {
            match path.next() {
                None => Some(self),
                Some(node_id) => match &self.kind {
                    NodeKind::Circuit { children, .. } => children.get(node_id)?.node_ref(path),
                    _ => None,
                },
            }
        }

        /// Lookup node in the subtree with the root in `self` by path.
        fn node_mut<'a>(&mut self, mut path: slice::Iter<'a, NodeId>) -> Option<&mut Node> {
            match path.next() {
                None => Some(self),
                Some(node_id) => match &mut self.kind {
                    NodeKind::Circuit { children, .. } => children.get_mut(node_id)?.node_mut(path),
                    _ => None,
                },
            }
        }

        /// `true` if `self` is a circuit node.
        fn is_circuit(&self) -> bool {
            matches!(self.kind, NodeKind::Circuit { .. })
        }

        /// `true` if `self` is an iterative circuit (including the root
        /// circuit).
        fn is_iterative(&self) -> bool {
            matches!(
                self.kind,
                NodeKind::Circuit {
                    iterative: true,
                    ..
                }
            )
        }

        /// Returns children of `self` if `self` is a circuit.
        fn children(&self) -> Option<&HashMap<NodeId, Node>> {
            if let NodeKind::Circuit { children, .. } = &self.kind {
                Some(children)
            } else {
                None
            }
        }

        /// `true` if `self` is the input half of a circuit operaor.
        fn is_strict_input(&self) -> bool {
            matches!(self.kind, NodeKind::StrictInput { .. })
        }

        /// Returns `self.output` if `self` is a strict input operator.
        fn output_id(&self) -> Option<NodeId> {
            if let NodeKind::StrictInput { output } = &self.kind {
                Some(*output)
            } else {
                None
            }
        }
    }

    struct CircuitGraph {
        /// Tree of nodes.
        nodes: Node,
        /// Matches a node to the vector of nodes that read from its output
        /// stream or have a dependency on it.
        /// A node can occur in this vector multiple times.
        edges: HashMap<GlobalNodeId, Vec<(GlobalNodeId, EdgeKind)>>,
    }

    impl CircuitGraph {
        fn new() -> Self {
            Self {
                nodes: Node::new(
                    "root",
                    NodeKind::Circuit {
                        iterative: true,
                        children: HashMap::new(),
                    },
                ),
                edges: HashMap::new(),
            }
        }

        /// Locate node by its global id.
        fn node_ref(&self, id: &GlobalNodeId) -> Option<&Node> {
            self.nodes.node_ref(id.path().iter())
        }

        /// Locate node by its global id.
        fn node_mut(&mut self, id: &GlobalNodeId) -> Option<&mut Node> {
            self.nodes.node_mut(id.path().iter())
        }

        fn add_edge(&mut self, from: &GlobalNodeId, to: &GlobalNodeId, kind: &EdgeKind) {
            match self.edges.entry(from.clone()) {
                Entry::Occupied(mut oe) => {
                    oe.get_mut().push((to.clone(), kind.clone()));
                }
                Entry::Vacant(ve) => {
                    ve.insert(vec![(to.clone(), kind.clone())]);
                }
            }
        }
    }

    /// State of a circuit automaton (see
    /// [`SchedulerEvent`](`super::SchedulerEvent`) documentation).
    enum CircuitState {
        Idle,
        Running,
        /// We track the set of nodes evaluated at the current step in the
        /// `Step` state.
        Step(HashSet<NodeId>),
        /// In the `Eval` state, we track the set of previously evaluated nodes
        /// and the node being evaluated now.
        Eval(HashSet<NodeId>, NodeId),
    }

    impl CircuitState {
        fn new() -> Self {
            Self::Idle
        }

        fn name(&self) -> &str {
            match self {
                Self::Idle => "Idle",
                Self::Running => "Running",
                Self::Step(..) => "Step",
                Self::Eval(..) => "Eval",
            }
        }

        fn is_idle(&self) -> bool {
            matches!(self, Self::Idle)
        }

        fn is_running(&self) -> bool {
            matches!(self, Self::Running)
        }
    }

    /// Error type that describes invalid traces.
    #[derive(Debug)]
    pub enum TraceError {
        /// Attempt to create a node in the global scope.
        EmptyPath,
        /// Attempt to access node that does not exist.
        UnknownNode(GlobalNodeId),
        /// Attemp to create a node with id that already exists.
        NodeExists(GlobalNodeId),
        /// Attempt to add a node to a parent that is not a circuit node.
        NotACircuit(GlobalNodeId),
        /// Invalid event in the current state of the circuit automaton.
        InvalidEvent(Cow<'static, str>),
    }

    impl Display for TraceError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::EmptyPath => f.write_str("cannot create node at an empty path"),
                Self::UnknownNode(node) => write!(f, "unknown node {}", node),
                Self::NodeExists(node) => write!(f, "node already exists: {}", node),
                Self::NotACircuit(node) => write!(f, "not a circuit node {}", node),
                Self::InvalidEvent(descr) => f.write_str(descr),
            }
        }
    }

    /// `TraceMonitor` listens to and validates event traces emitted by a
    /// circuit.
    ///
    /// The monitor uses [`CircuitEvent`](`super::CircuitEvent`)s, to build up a
    /// model of the circuit graph and check that nodes aren't added twice,
    /// the parent circuit exists when a node is added, edges connect
    /// existing nodes, etc.
    ///
    /// During circuit evaluation, the monitor uses
    /// [`SchedulerEvent`](`super::SchedulerEvent`)s, to track the state of
    /// the circuit automaton (see [`SchedulerEvent`](`super::SchedulerEvent`)
    /// and detect invalid event sequences.  It uses the circuit graph
    /// constructed from `CircuitEvent`s to check that events relate to
    /// existing nodes and match the type of each node (e.g., the
    /// [`ClockStart`](`super::SchedulerEvent::ClockStart`) event
    /// can only be emitted by an iterative circuit).
    ///
    /// The monitor is initialized with a pair of callbacks to invoke when an
    /// invalid `CircuitEvent` or `SchedulerEvent` is observed.  These
    /// callbacks can simply panic when using the trace monitor in `cargo
    /// test`.
    pub struct TraceMonitor {
        /// Circuit graph constructed based on `CircuitEvent`s.
        circuit: CircuitGraph,
        /// The stack of circuit automata states.  We start with a single
        /// element for the root circuit.  Evaluating a nested circuit
        /// pushes a new state on the stack. The stack becomes empty
        /// when the root automaton terminates, at which point
        /// no further `SchedulerEvent`'s are allowed.
        state: Vec<CircuitState>,
        /// Callback to invoke on each invalid `CircuitEvent`.
        circuit_error_handler: Box<dyn Fn(&CircuitEvent, &TraceError)>,
        /// Callback to invoke on each invalid `SchedulerEvent`.
        scheduler_error_handler: Box<dyn Fn(&SchedulerEvent, &TraceError)>,
    }

    impl TraceMonitor {
        /// Attach trace monitor to a circuit.  The monitor will register for
        /// both `CircuitEvent`s and `SchedulerEvent`s.
        pub fn attach(this: Arc<Mutex<Self>>, circuit: &Circuit<()>, handler_name: &str) {
            let this_clone = this.clone();

            circuit.register_circuit_event_handler(handler_name, move |event| {
                let mut this = this.lock().unwrap();
                let _ = this.circuit_event(event).map_err(|e| {
                    (this.circuit_error_handler)(event, &e);
                });
            });
            circuit.register_scheduler_event_handler(handler_name, move |event| {
                // The `ClockEnd` event can be triggered by `Root::drop()` while the thread
                // is being terminated, at which point the lock may be poisoned.  Just ignore
                // the event in that case.
                if let Ok(mut this) = this_clone.lock() {
                    let _ = this.scheduler_event(event).map_err(|e| {
                        (this.scheduler_error_handler)(event, &e);
                    });
                };
            });
        }

        /// Create a new trace monitor with user-provided event handlers.
        pub fn new<CE, SE>(circuit_error_handler: CE, scheduler_error_handler: SE) -> Self
        where
            CE: Fn(&CircuitEvent, &TraceError) + 'static,
            SE: Fn(&SchedulerEvent, &TraceError) + 'static,
        {
            Self {
                circuit: CircuitGraph::new(),
                state: vec![CircuitState::new()],
                circuit_error_handler: Box::new(circuit_error_handler),
                scheduler_error_handler: Box::new(scheduler_error_handler),
            }
        }

        /// Create a trace monitor whose error handlers will panic on error.
        pub fn new_panic_on_error() -> Self {
            Self::new(
                |event, err| panic!("invalid circuit event {}: {}", event, err),
                |event, err| panic!("invalid scheduler event {}: {}", event, err),
            )
        }

        fn circuit_event(&mut self, event: &CircuitEvent) -> Result<(), TraceError> {
            if event.is_node_event() {
                let path = event.node_id().unwrap().path();
                let local_node_id = *path.last().ok_or(TraceError::EmptyPath)?;
                let parent_id = GlobalNodeId::from_path(path.split_last().unwrap().1);
                let parent_node = self
                    .circuit
                    .node_mut(&parent_id)
                    .ok_or_else(|| TraceError::UnknownNode(parent_id.clone()))?;
                match &mut parent_node.kind {
                    NodeKind::Circuit { children, .. } => {
                        if children.get(&local_node_id).is_some() {
                            return Err(TraceError::NodeExists(event.node_id().unwrap().clone()));
                        };
                        let new_node =
                            if event.is_operator_event() || event.is_strict_output_event() {
                                Node::new(event.node_name().unwrap(), NodeKind::Operator)
                            } else if event.is_strict_input_event() {
                                let output = event.output_node_id().unwrap();
                                if children.get(&output).is_none() {
                                    return Err(TraceError::UnknownNode(GlobalNodeId::child(
                                        &parent_id, output,
                                    )));
                                };

                                Node::new("", NodeKind::StrictInput { output })
                            } else {
                                Node::new(
                                    "",
                                    NodeKind::Circuit {
                                        iterative: event.is_iterative_subcircuit_event(),
                                        children: HashMap::new(),
                                    },
                                )
                            };
                        children.insert(local_node_id, new_node);
                        Ok(())
                    }
                    _ => Err(TraceError::NotACircuit(parent_id)),
                }
            } else if event.is_edge_event() {
                let from = event.from().unwrap();
                let to = event.to().unwrap();
                let kind = event.edge_kind().unwrap();
                self.circuit
                    .node_ref(from)
                    .ok_or_else(|| TraceError::UnknownNode(from.clone()))?;
                self.circuit
                    .node_ref(to)
                    .ok_or_else(|| TraceError::UnknownNode(to.clone()))?;
                self.circuit.add_edge(from, to, kind);
                Ok(())
            } else {
                panic!("unknown event")
            }
        }

        fn scheduler_event(&mut self, event: &SchedulerEvent) -> Result<(), TraceError> {
            //eprintln!("scheduler event: {}", event);
            if !self.running() {
                return Err(TraceError::InvalidEvent(Cow::from(
                    "scheduler event received after circuit execution has terminated",
                )));
            }
            let current_node_id = self.current_node_id();
            match event {
                SchedulerEvent::ClockStart => {
                    if !self
                        .circuit
                        .node_ref(&current_node_id)
                        .unwrap()
                        .is_iterative()
                    {
                        return Err(TraceError::InvalidEvent(Cow::from(
                            "received 'ClockStart' event for a non-iterative circuit",
                        )));
                    }

                    if !self.current_state().is_idle() {
                        return Err(TraceError::InvalidEvent(Cow::from(format!(
                            "received 'ClockStart' event in state {}",
                            self.current_state().name()
                        ))));
                    }

                    self.set_current_state(CircuitState::Running);
                    Ok(())
                }
                SchedulerEvent::ClockEnd => {
                    if !self.current_state().is_running() {
                        return Err(TraceError::InvalidEvent(Cow::from(format!(
                            "received 'ClockEnd' event in state {}",
                            self.current_state().name()
                        ))));
                    }

                    self.pop_state();
                    Ok(())
                }
                SchedulerEvent::StepStart => {
                    if self
                        .circuit
                        .node_ref(&current_node_id)
                        .unwrap()
                        .is_iterative()
                        && !self.current_state().is_running()
                    {
                        return Err(TraceError::InvalidEvent(Cow::from(format!(
                            "received 'StepStart' event in state {} of an iterative circuit",
                            self.current_state().name()
                        ))));
                    }
                    if !self
                        .circuit
                        .node_ref(&current_node_id)
                        .unwrap()
                        .is_iterative()
                        && !self.current_state().is_idle()
                    {
                        return Err(TraceError::InvalidEvent(Cow::from(format!(
                            "received 'StepStart' event in state {} of a non-iterative subcircuit",
                            self.current_state().name()
                        ))));
                    }

                    self.set_current_state(CircuitState::Step(HashSet::new()));
                    Ok(())
                }
                SchedulerEvent::StepEnd => {
                    match self.current_state() {
                        CircuitState::Step(visited_nodes) => {
                            let expected_len = self
                                .circuit
                                .node_ref(&current_node_id)
                                .unwrap()
                                .children()
                                .unwrap()
                                .len();
                            if visited_nodes.len() != expected_len {
                                return Err(TraceError::InvalidEvent(Cow::from(format!("received 'StepEnd' event after evaluating {} nodes instead of the expected {}", visited_nodes.len(), expected_len))));
                            }
                        }
                        state => {
                            return Err(TraceError::InvalidEvent(Cow::from(format!(
                                "received 'StepEnd' event in state {}",
                                state.name()
                            ))));
                        }
                    }

                    if self
                        .circuit
                        .node_ref(&current_node_id)
                        .unwrap()
                        .is_iterative()
                    {
                        self.set_current_state(CircuitState::Running);
                    } else {
                        self.pop_state();
                    }
                    Ok(())
                }
                SchedulerEvent::EvalStart { node_id } => {
                    let state = self.pop_state();
                    match state {
                        CircuitState::Step(visited_nodes) => {
                            if visited_nodes.contains(node_id) {
                                return Err(TraceError::InvalidEvent(Cow::from(format!(
                                    "node id {} evaluated twice in one clock cycle",
                                    node_id
                                ))));
                            }
                            let global_id = current_node_id.child(*node_id);
                            let node = match self.circuit.node_ref(&global_id) {
                                None => {
                                    return Err(TraceError::UnknownNode(global_id));
                                }
                                Some(node) => node,
                            };

                            if node.is_strict_input() {
                                let output_id = node.output_id().unwrap();
                                if !visited_nodes.contains(&output_id) {
                                    return Err(TraceError::InvalidEvent(Cow::from(format!("input node {} of a strict operator is evaluated before the output node {}", node_id, output_id))));
                                }
                            };

                            if node.is_circuit() {
                                self.push_state(CircuitState::Eval(visited_nodes, *node_id));
                                self.push_state(CircuitState::new());
                            } else {
                                self.push_state(CircuitState::Eval(visited_nodes, *node_id));
                            };

                            Ok(())
                        }
                        state => {
                            // Restore state on error.
                            let err = TraceError::InvalidEvent(Cow::from(format!(
                                "received 'EvalStart' event in state {}",
                                state.name()
                            )));
                            self.push_state(state);
                            Err(err)
                        }
                    }
                }
                SchedulerEvent::EvalEnd { node_id } => {
                    match self.current_state() {
                        CircuitState::Eval(visited_nodes, eval_node_id) => {
                            if eval_node_id != node_id {
                                return Err(TraceError::InvalidEvent(Cow::from(format!("received 'EvalEnd' event for node id {} while evaluating node {}", node_id, eval_node_id))));
                            }
                            let mut visited_nodes = visited_nodes.clone();
                            visited_nodes.insert(*eval_node_id);
                            self.set_current_state(CircuitState::Step(visited_nodes));
                            Ok(())
                        }
                        state => {
                            return Err(TraceError::InvalidEvent(Cow::from(format!(
                                "received 'EvalEnd' event in state {}",
                                state.name()
                            ))));
                        }
                    }
                }
            }
        }

        fn running(&self) -> bool {
            !self.state.is_empty()
        }

        fn current_state(&self) -> &CircuitState {
            self.state.last().unwrap()
        }

        fn set_current_state(&mut self, state: CircuitState) {
            *self.state.last_mut().unwrap() = state;
        }

        fn pop_state(&mut self) -> CircuitState {
            self.state.pop().unwrap()
        }

        fn push_state(&mut self, state: CircuitState) {
            self.state.push(state);
        }

        fn current_node_id(&self) -> GlobalNodeId {
            let mut path = Vec::with_capacity(self.state.len());

            for state in self.state.iter() {
                match state {
                    CircuitState::Eval(_, id) => {
                        path.push(*id);
                    }
                    _ => {
                        break;
                    }
                }
            }

            GlobalNodeId::from_path_vec(path)
        }
    }
}
