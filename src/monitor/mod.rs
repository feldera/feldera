///! Trace monitor that validates event traces from a circuit.  It is
///! used to test both the tracing mechanism and the circuit engine
///! itself.
use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    fmt,
    fmt::Display,
    sync::{Arc, Mutex},
};

use crate::circuit::{
    trace::{CircuitEvent, SchedulerEvent},
    Circuit, GlobalNodeId, NodeId,
};

mod circuit_graph;
use circuit_graph::{CircuitGraph, Node, NodeKind};

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
                    let new_node = if event.is_operator_event() || event.is_strict_output_event() {
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
            SchedulerEvent::EvalEnd { node_id } => match self.current_state() {
                CircuitState::Eval(visited_nodes, eval_node_id) => {
                    if eval_node_id != node_id {
                        return Err(TraceError::InvalidEvent(Cow::from(format!(
                            "received 'EvalEnd' event for node id {} while evaluating node {}",
                            node_id, eval_node_id
                        ))));
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
            },
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
