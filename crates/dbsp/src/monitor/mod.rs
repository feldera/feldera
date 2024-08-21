//! Trace monitor that validates event traces from a circuit.  It is
//! used to test both the tracing mechanism and the circuit engine
//! itself.
mod circuit_graph;

pub mod visual_graph;

use crate::circuit::{
    metadata::OperatorLocation,
    trace::{CircuitEvent, SchedulerEvent},
    GlobalNodeId, NodeId, RootCircuit,
};
use circuit_graph::{CircuitGraph, Node, NodeKind, Region, RegionId};
use std::{
    borrow::Cow,
    cell::RefCell,
    collections::{HashMap, HashSet},
    fmt,
    fmt::Display,
    rc::Rc,
};
use visual_graph::Graph as VisGraph;

/// Callback function type signature for reporting an invalid `CircuitEvent`.
type CircuitErrorHandler = dyn Fn(&CircuitEvent, &TraceError);

/// Callback function type signature for reporting an invalid `SchedulerEvent`.
type SchedulerErrorHandler = dyn Fn(&SchedulerEvent, &TraceError);

/// State of a circuit automaton (see [`SchedulerEvent`]).
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
    /// Node created outside of the current circuit.
    NodeOutsideCurrentScope(GlobalNodeId, GlobalNodeId),
    /// Event contains node id that is not valid for the given
    /// event type in the current state.
    UnexpectedNodeId,
    /// PopRegion event without matching PushRegion.
    NoRegion,
    /// Invalid event in the current state of the circuit automaton.
    InvalidEvent(Cow<'static, str>),
}

impl Display for TraceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyPath => f.write_str("cannot create node at an empty path"),
            Self::UnknownNode(node) => write!(f, "unknown node {node}"),
            Self::NodeExists(node) => write!(f, "node already exists: {node}"),
            Self::NotACircuit(node) => write!(f, "not a circuit node {node}"),
            Self::NodeOutsideCurrentScope(node, scope) => write!(
                f,
                "node id {node} does not belong to the current scope {scope}",
            ),
            Self::UnexpectedNodeId => {
                f.write_str("node id is not valid for this event type in the current state")
            }
            Self::NoRegion => f.write_str("PopRegion event without matching PushRegion"),
            Self::InvalidEvent(descr) => f.write_str(descr),
        }
    }
}

/// `TraceMonitor` listens to and validates event traces emitted by a
/// circuit.
///
/// The monitor uses [`CircuitEvent`](`crate::circuit::trace::CircuitEvent`)s,
/// to build up a model of the circuit graph and check that nodes aren't added
/// twice, the parent circuit exists when a node is added, edges connect
/// existing nodes, etc.
///
/// During circuit evaluation, the monitor uses
/// [`SchedulerEvent`](`crate::circuit::trace::SchedulerEvent`)s, to track the
/// state of the circuit automaton (see
/// [`SchedulerEvent`](`crate::circuit::trace::SchedulerEvent`) and detect
/// invalid event sequences.  It uses the circuit graph constructed from
/// `CircuitEvent`s to check that events relate to existing nodes and match the
/// type of each node (e.g., the
/// [`ClockStart`](`crate::circuit::trace::SchedulerEvent::ClockStart`) event
/// can only be emitted by an iterative circuit).
///
/// The monitor is initialized with a pair of callbacks to invoke when an
/// invalid `CircuitEvent` or `SchedulerEvent` is observed.  These
/// callbacks can simply panic when using the trace monitor in `cargo
/// test`.
#[repr(transparent)]
#[derive(Clone)]
pub struct TraceMonitor(Rc<RefCell<TraceMonitorInternal>>);

impl TraceMonitor {
    /// Attach trace monitor to a circuit.  The monitor will register for
    /// both `CircuitEvent`s and `SchedulerEvent`s.
    pub fn attach(&self, circuit: &RootCircuit, handler_name: &str) {
        TraceMonitorInternal::attach(self.0.clone(), circuit, true, handler_name);
    }

    /// Attach trace monitor to a circuit.  The monitor will register for
    /// `CircuitEvent`s only.  It will validate the circuit construction process
    /// and can be used to visualize the circuit (see
    /// [`Self::visualize_circuit`] and [`Self::visualize_circuit_annotate`],
    /// but it will not validate scheduler events, nor incur the associated
    /// overheads.
    pub fn attach_circuit_events(&self, circuit: &RootCircuit, handler_name: &str) {
        TraceMonitorInternal::attach(self.0.clone(), circuit, false, handler_name);
    }

    pub fn new<CE, SE>(circuit_error_handler: CE, scheduler_error_handler: SE) -> Self
    where
        CE: Fn(&CircuitEvent, &TraceError) + 'static,
        SE: Fn(&SchedulerEvent, &TraceError) + 'static,
    {
        Self(Rc::new(RefCell::new(TraceMonitorInternal::new(
            circuit_error_handler,
            scheduler_error_handler,
        ))))
    }

    pub fn new_panic_on_error() -> Self {
        Self(Rc::new(RefCell::new(
            TraceMonitorInternal::new_panic_on_error(),
        )))
    }

    pub fn visualize_circuit(&self) -> VisGraph {
        self.visualize_circuit_annotate(|_| ("".to_string(), 0f64))
    }

    pub fn visualize_circuit_annotate<F>(&self, annotate: F) -> VisGraph
    where
        F: Fn(&GlobalNodeId) -> (String, f64),
    {
        self.0.borrow().circuit.visualize(&annotate)
    }
}

pub struct TraceMonitorInternal {
    /// Circuit graph constructed based on `CircuitEvent`s.
    circuit: CircuitGraph,
    /// Subcircuit that is currently being populated.
    current_scope: GlobalNodeId,
    region_stack: Vec<RegionId>,
    /// The stack of circuit automata states.  We start with a single
    /// element for the root circuit.  Evaluating a nested circuit
    /// pushes a new state on the stack. The stack becomes empty
    /// when the root automaton terminates, at which point
    /// no further `SchedulerEvent`'s are allowed.
    state: Vec<CircuitState>,
    /// Callback to invoke on each invalid `CircuitEvent`.
    circuit_error_handler: Box<CircuitErrorHandler>,
    /// Callback to invoke on each invalid `SchedulerEvent`.
    scheduler_error_handler: Box<SchedulerErrorHandler>,
}

impl TraceMonitorInternal {
    fn attach(
        this: Rc<RefCell<Self>>,
        circuit: &RootCircuit,
        monitor_eval: bool,
        handler_name: &str,
    ) {
        let this_clone = this.clone();

        circuit.register_circuit_event_handler(handler_name, move |event| {
            let mut this = this.borrow_mut();
            let _ = this.circuit_event(event).map_err(|e| {
                (this.circuit_error_handler)(event, &e);
            });
        });
        if monitor_eval {
            circuit.register_scheduler_event_handler(handler_name, move |event| {
                // The `ClockEnd` event can be triggered by `CircuitHandle::drop()` while the
                // thread is being terminated, at which point the lock may be
                // poisoned.  Just ignore the event in that case.
                if let Ok(mut this) = this_clone.try_borrow_mut() {
                    let _ = this.scheduler_event(event).map_err(|e| {
                        (this.scheduler_error_handler)(event, &e);
                    });
                };
            });
        }
    }

    /// Create a new trace monitor with user-provided event handlers.
    fn new<CE, SE>(circuit_error_handler: CE, scheduler_error_handler: SE) -> Self
    where
        CE: Fn(&CircuitEvent, &TraceError) + 'static,
        SE: Fn(&SchedulerEvent, &TraceError) + 'static,
    {
        Self {
            circuit: CircuitGraph::new(),
            current_scope: GlobalNodeId::from_path(&[]),
            region_stack: vec![RegionId::root()],
            state: vec![CircuitState::new()],
            circuit_error_handler: Box::new(circuit_error_handler),
            scheduler_error_handler: Box::new(scheduler_error_handler),
        }
    }

    /// Create a trace monitor whose error handlers will panic on error.
    fn new_panic_on_error() -> Self {
        Self::new(
            |event, err| panic!("invalid circuit event {event}: {err}"),
            |event, err| panic!("invalid scheduler event {event}: {err}"),
        )
    }

    fn push_region(&mut self, name: Cow<'static, str>, location: OperatorLocation) {
        let mut current_region = self.current_region();
        let circuit_node = self.circuit.node_mut(&self.current_scope).unwrap();
        current_region =
            circuit_node
                .region_mut()
                .unwrap()
                .add_region(&current_region, name, location);
        self.set_current_region(current_region);
    }

    fn pop_region(&mut self) -> Result<(), TraceError> {
        let mut current_region = self.current_region();
        if current_region == RegionId::root() {
            Err(TraceError::NoRegion)
        } else {
            current_region.pop();
            self.set_current_region(current_region);
            Ok(())
        }
    }

    fn current_region(&mut self) -> RegionId {
        self.region_stack.last().unwrap().clone()
    }

    fn set_current_region(&mut self, region: RegionId) {
        *self.region_stack.last_mut().unwrap() = region;
    }

    fn circuit_event(&mut self, event: &CircuitEvent) -> Result<(), TraceError> {
        //println!("event: {}", event);
        if event.is_node_event() {
            let node_id = event.node_id().unwrap();
            let local_node_id = node_id.local_node_id().ok_or(TraceError::EmptyPath)?;
            let parent_id = node_id.parent_id().unwrap();
            let current_region = self.current_region();
            let parent_node = self
                .circuit
                .node_mut(&parent_id)
                .ok_or_else(|| TraceError::UnknownNode(parent_id.clone()))?;

            if event.is_new_node_event() {
                if parent_id != self.current_scope {
                    return Err(TraceError::NodeOutsideCurrentScope(
                        node_id.clone(),
                        self.current_scope.clone(),
                    ));
                }

                match &mut parent_node.kind {
                    NodeKind::Circuit {
                        children, region, ..
                    } => {
                        if children.get(&local_node_id).is_some() {
                            return Err(TraceError::NodeExists(node_id.clone()));
                        }

                        let new_node = if event.is_operator_event() {
                            Node::new(
                                node_id.clone(),
                                event.node_name().unwrap().clone(),
                                event.location(),
                                current_region.clone(),
                                NodeKind::Operator,
                            )
                        } else if event.is_strict_output_event() {
                            Node::new(
                                node_id.clone(),
                                event.node_name().unwrap().clone(),
                                event.location(),
                                current_region.clone(),
                                NodeKind::StrictOutput,
                            )
                        } else if event.is_strict_input_event() {
                            let output = event.output_node_id().unwrap();
                            let output_node = match children.get(&output) {
                                None => {
                                    return Err(TraceError::UnknownNode(GlobalNodeId::child(
                                        &parent_id, output,
                                    )));
                                }
                                Some(node) => node,
                            };

                            Node::new(
                                node_id.clone(),
                                output_node.name.clone(),
                                output_node.location,
                                current_region.clone(),
                                NodeKind::StrictInput { output },
                            )
                        } else {
                            self.current_scope = node_id.clone();
                            self.region_stack.push(RegionId::root());
                            Node::new(
                                node_id.clone(),
                                Cow::Borrowed(""),
                                None,
                                current_region.clone(),
                                NodeKind::Circuit {
                                    iterative: event.is_iterative_subcircuit_event(),
                                    children: HashMap::new(),
                                    region: Region::new(RegionId::root(), Cow::Borrowed(""), None),
                                },
                            )
                        };

                        children.insert(local_node_id, new_node);
                        region.get_region(&current_region).nodes.push(local_node_id);
                        Ok(())
                    }
                    _ => Err(TraceError::NotACircuit(parent_id)),
                }
            } else {
                if node_id != &self.current_scope {
                    return Err(TraceError::UnexpectedNodeId);
                }
                self.current_scope = parent_id;
                self.region_stack.pop();
                Ok(())
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
            match event {
                CircuitEvent::PushRegion { name, location } => {
                    self.push_region(name.clone(), *location);
                    Ok(())
                }

                CircuitEvent::PopRegion => self.pop_region(),
                _ => panic!("unknown event"),
            }
        }
    }

    fn scheduler_event(&mut self, event: &SchedulerEvent) -> Result<(), TraceError> {
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
            SchedulerEvent::StepStart { circuit_id } => {
                if **circuit_id != current_node_id {
                    return Err(TraceError::InvalidEvent(Cow::from(format!(
                        "received 'StepStart' event for node {circuit_id} while evaluating node {current_node_id}",
                    ))));
                }
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
            SchedulerEvent::StepEnd { circuit_id } => {
                if **circuit_id != current_node_id {
                    return Err(TraceError::InvalidEvent(Cow::from(format!(
                        "received 'StepEnd' event for node {circuit_id} while evaluating node {current_node_id}",
                    ))));
                }
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
                            return Err(TraceError::InvalidEvent(Cow::from(format!("received 'StepEnd' event after evaluating {} nodes instead of the expected {expected_len}", visited_nodes.len()))));
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
            SchedulerEvent::EvalStart { node } => {
                let state = self.pop_state();
                let local_node_id = node.local_id();
                match state {
                    CircuitState::Step(visited_nodes) => {
                        if visited_nodes.contains(&local_node_id) {
                            return Err(TraceError::InvalidEvent(Cow::from(format!(
                                "node id {local_node_id} evaluated twice in one clock cycle",
                            ))));
                        }
                        let global_id = node.global_id();
                        let cnode = match self.circuit.node_ref(global_id) {
                            None => {
                                return Err(TraceError::UnknownNode(global_id.clone()));
                            }
                            Some(cnode) => cnode,
                        };

                        if cnode.is_strict_input() {
                            let output_id = cnode.output_id().unwrap();
                            if !visited_nodes.contains(&output_id) {
                                return Err(TraceError::InvalidEvent(Cow::from(format!("input node {local_node_id} of a strict operator is evaluated before the output node {output_id}"))));
                            }
                        };

                        if cnode.is_circuit() {
                            self.push_state(CircuitState::Eval(visited_nodes, local_node_id));
                            self.push_state(CircuitState::new());
                        } else {
                            self.push_state(CircuitState::Eval(visited_nodes, local_node_id));
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
            SchedulerEvent::EvalEnd { node } => match self.current_state() {
                CircuitState::Eval(visited_nodes, eval_node_id) => {
                    if eval_node_id != &node.local_id() {
                        return Err(TraceError::InvalidEvent(Cow::from(format!(
                            "received 'EvalEnd' event for node id {} while evaluating node {}",
                            node.local_id(),
                            eval_node_id
                        ))));
                    }
                    let mut visited_nodes = visited_nodes.clone();
                    visited_nodes.insert(*eval_node_id);
                    self.set_current_state(CircuitState::Step(visited_nodes));
                    Ok(())
                }
                state => Err(TraceError::InvalidEvent(Cow::from(format!(
                    "received 'EvalEnd' event in state {}",
                    state.name()
                )))),
            },
            SchedulerEvent::WaitStart { circuit_id } => {
                if **circuit_id != current_node_id {
                    return Err(TraceError::InvalidEvent(Cow::from(format!(
                        "received 'WaitStart' event for node {circuit_id} while evaluating node {current_node_id}",
                    ))));
                }
                Ok(())
            }
            SchedulerEvent::WaitEnd { circuit_id } => {
                if **circuit_id != current_node_id {
                    return Err(TraceError::InvalidEvent(Cow::from(format!(
                        "received 'WaitEnd' event for node {circuit_id} while evaluating node {current_node_id}",
                    ))));
                }
                Ok(())
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
