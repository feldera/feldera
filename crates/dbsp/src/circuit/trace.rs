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
//! See
//! [`RootCircuit::register_circuit_event_handler`](`crate::RootCircuit::register_circuit_event_handler`)
//! and [`CircuitHandle::register_scheduler_event_handler`](`crate::CircuitHandle::register_scheduler_event_handler`)
//! APIs for attaching event handlers to a circuit.
//!
//! Event handlers are invoked synchronously and therefore must complete
//! quickly, with any expensive processing completed asynchronously.

use super::{circuit_builder::Node, GlobalNodeId, NodeId, OwnershipPreference};
use crate::circuit::metadata::OperatorLocation;
use std::{borrow::Cow, fmt, fmt::Display, hash::Hash};

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

impl EdgeKind {
    pub fn is_stream(&self) -> bool {
        matches!(self, Self::Stream(..))
    }
}

/// Events related to circuit construction.  A handler listening to these
/// events should be able to reconstruct complete circuit topology,
/// including operators, nested circuits, and streams connecting them.
#[derive(Debug, Eq, PartialEq, Clone, Hash)]
pub enum CircuitEvent {
    /// Create a sub-region.
    PushRegion {
        /// Sub-region name.
        name: Cow<'static, str>,
        /// The region's source location
        location: OperatorLocation,
    },

    /// Subregion complete.
    PopRegion,

    /// A new regular
    /// (non-[strict](`crate::circuit::operator_traits::StrictOperator`))
    /// operator is added to the circuit.
    Operator {
        /// Global id of the new operator.
        node_id: GlobalNodeId,
        /// Operator name.
        name: Cow<'static, str>,
        /// The operator's source location
        location: OperatorLocation,
    },

    /// The output half of a [strict
    /// operator](`crate::circuit::operator_traits::StrictOperator`).
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
        /// The operator's source location
        location: OperatorLocation,
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

    /// Nested circuit has been fully populated.
    SubcircuitComplete {
        /// Global id of the nested circuit.
        node_id: GlobalNodeId,
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

impl CircuitEvent {
    /// Create a [`CircuitEvent::PushRegion`] event instance.
    pub fn push_region_static(name: &'static str, location: OperatorLocation) -> Self {
        Self::PushRegion {
            name: Cow::Borrowed(name),
            location,
        }
    }

    /// Create a [`CircuitEvent::PushRegion`] event instance.
    pub fn push_region(name: &str, location: OperatorLocation) -> Self {
        Self::PushRegion {
            name: Cow::Owned(name.to_string()),
            location,
        }
    }

    /// Create a [`CircuitEvent::PopRegion`] event.
    pub fn pop_region() -> Self {
        Self::PopRegion
    }

    /// Create a [`CircuitEvent::Operator`] event instance.
    pub fn operator(
        node_id: GlobalNodeId,
        name: Cow<'static, str>,
        location: OperatorLocation,
    ) -> Self {
        Self::Operator {
            node_id,
            name,
            location,
        }
    }

    /// Create a [`CircuitEvent::StrictOperatorOutput`] event instance.
    pub fn strict_operator_output(
        node_id: GlobalNodeId,
        name: Cow<'static, str>,
        location: OperatorLocation,
    ) -> Self {
        Self::StrictOperatorOutput {
            node_id,
            name,
            location,
        }
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

    /// Create a [`CircuitEvent::SubcircuitComplete`] event instance.
    pub fn subcircuit_complete(node_id: GlobalNodeId) -> Self {
        Self::SubcircuitComplete { node_id }
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

    /// `true` if `self` is one of events related to nodes:
    /// [`CircuitEvent::Operator`], [`CircuitEvent::StrictOperatorOutput`],
    /// [`CircuitEvent::StrictOperatorInput`], [`CircuitEvent::Subcircuit`],
    /// or [`CircuitEvent::SubcircuitComplete`].
    pub fn is_node_event(&self) -> bool {
        matches!(
            self,
            Self::Operator { .. }
                | Self::StrictOperatorOutput { .. }
                | Self::StrictOperatorInput { .. }
                | Self::Subcircuit { .. }
                | Self::SubcircuitComplete { .. }
        )
    }

    /// `true` if `self` is one of the node creation events:
    /// [`CircuitEvent::Operator`], [`CircuitEvent::StrictOperatorOutput`],
    /// [`CircuitEvent::StrictOperatorInput`], or [`CircuitEvent::Subcircuit`].
    pub fn is_new_node_event(&self) -> bool {
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
            Self::SubcircuitComplete { node_id } => Some(node_id),
            _ => None,
        }
    }

    /// If `self` is a [`CircuitEvent::Operator`] or
    /// [`CircuitEvent::StrictOperatorOutput`], returns `self.name`.
    pub fn node_name(&self) -> Option<&Cow<'static, str>> {
        match self {
            Self::Operator { name, .. } | Self::StrictOperatorOutput { name, .. } => Some(name),
            _ => None,
        }
    }

    pub fn location(&self) -> OperatorLocation {
        match *self {
            Self::PushRegion { location, .. }
            | Self::Operator { location, .. }
            | Self::StrictOperatorOutput { location, .. } => location,
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

impl Display for CircuitEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PushRegion { name, location } => {
                write!(f, "PushRegion(\"{name}\"")?;
                if let Some(location) = location {
                    write!(
                        f,
                        " @ {}:{}:{}",
                        location.file(),
                        location.line(),
                        location.column()
                    )?;
                }

                write!(f, ")")
            }

            Self::PopRegion => f.write_str("PopRegion"),

            Self::Operator {
                node_id,
                name,
                location,
            } => {
                write!(f, "Operator(\"{name}\", {node_id}")?;
                if let Some(location) = location {
                    write!(
                        f,
                        " @ {}:{}:{}",
                        location.file(),
                        location.line(),
                        location.column()
                    )?;
                }

                write!(f, ")")
            }

            Self::StrictOperatorOutput {
                node_id,
                name,
                location,
            } => {
                write!(f, "StrictOperatorOutput(\"{name}\", {node_id}")?;
                if let Some(location) = location {
                    write!(
                        f,
                        " @ {}:{}:{}",
                        location.file(),
                        location.line(),
                        location.column()
                    )?;
                }

                write!(f, ")")
            }

            Self::StrictOperatorInput {
                node_id,
                output_node_id,
            } => {
                write!(f, "StrictOperatorInput({node_id} -> {output_node_id})")
            }

            Self::Subcircuit { node_id, iterative } => {
                write!(
                    f,
                    "{}Subcircuit({})",
                    if *iterative { "Iterative" } else { "" },
                    node_id,
                )
            }

            Self::SubcircuitComplete { node_id } => {
                write!(f, "SubcircuitComplete({node_id})")
            }

            Self::Edge {
                kind: EdgeKind::Stream(preference),
                from,
                to,
            } => {
                write!(f, "Stream({from} -> [{preference}]{to})")
            }

            Self::Edge {
                kind: EdgeKind::Dependency,
                from,
                to,
            } => {
                write!(f, "Dependency({from} -> {to})")
            }
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
///             ClockStart             StepStart     EvalStart(id) WaitStart
/// ───►idle─────────────────────┐ ┌──────────────┐ ┌───────────┐ ┌─────────┐
///                              ▼ │              ▼ │           ▼ │         ▼
///                           running            step          eval        wait
///                              │ ▲              │ ▲           │ ▲         │
///         ◄────────────────────┘ └──────────────┘ └───────────┘ └─────────┘
///             ClockEnd               StepEnd       EvalEnd(id)   WaitEnd
/// ```
///
/// The root circuit automaton is instantiated by the
/// [`RootCircuit::build`](`crate::RootCircuit::build`) function.  A subcircuit
/// automaton is instantiated when its parent scheduler evaluates the node that
/// contains this subcircuit (see below).
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
/// The scheduler may have to wait for one of async operators to become ready.
/// It brackets the wait period with
/// [`WaitStart`](`SchedulerEvent::WaitStart`)/
/// [`WaitEnd`](`SchedulerEvent::WaitEnd`) events.
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
///              StepStart    EvalStart(id)   WaitStart
/// ──────►idle─────────────┐ ┌───────────┐ ┌───────────┐
///                         ▼ │           ▼ │           ▼
///                         step         eval         wait
///                         │ ▲           │ ▲           │
///         ◄───────────────┘ └───────────┘ └───────────┘
///              StepEnd      EvalEnd(id)    WaitEnd
/// ```
pub enum SchedulerEvent<'a> {
    EvalStart { node: &'a dyn Node },
    EvalEnd { node: &'a dyn Node },
    WaitStart { circuit_id: &'a GlobalNodeId },
    WaitEnd { circuit_id: &'a GlobalNodeId },
    StepStart { circuit_id: &'a GlobalNodeId },
    StepEnd { circuit_id: &'a GlobalNodeId },
    ClockStart,
    ClockEnd,
}

impl<'a> SchedulerEvent<'a> {
    /// Create a [`SchedulerEvent::EvalStart`] event instance.
    pub fn eval_start(node: &'a dyn Node) -> Self {
        Self::EvalStart { node }
    }

    /// Create a [`SchedulerEvent::EvalEnd`] event instance.
    pub fn eval_end(node: &'a dyn Node) -> Self {
        Self::EvalEnd { node }
    }

    /// Create a [`SchedulerEvent::WaitStart`] event instance.
    pub fn wait_start(circuit_id: &'a GlobalNodeId) -> Self {
        Self::WaitStart { circuit_id }
    }

    /// Create a [`SchedulerEvent::WaitEnd`] event instance.
    pub fn wait_end(circuit_id: &'a GlobalNodeId) -> Self {
        Self::WaitEnd { circuit_id }
    }

    /// Create a [`SchedulerEvent::StepStart`] event instance.
    pub fn step_start(circuit_id: &'a GlobalNodeId) -> Self {
        Self::StepStart { circuit_id }
    }

    /// Create a [`SchedulerEvent::StepEnd`] event instance.
    pub fn step_end(circuit_id: &'a GlobalNodeId) -> Self {
        Self::StepEnd { circuit_id }
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

impl Display for SchedulerEvent<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EvalStart { node } => {
                write!(f, "EvalStart({})", node.global_id())
            }
            Self::EvalEnd { node } => {
                write!(f, "EvalEnd({})", node.global_id())
            }
            Self::WaitStart { circuit_id } => {
                write!(f, "WaitStart({circuit_id})")
            }
            Self::WaitEnd { circuit_id } => {
                write!(f, "WaitEnd({circuit_id})")
            }
            Self::StepStart { circuit_id } => {
                write!(f, "StepStart({circuit_id})")
            }
            Self::StepEnd { circuit_id } => {
                write!(f, "StepEnd({circuit_id})")
            }
            Self::ClockStart => f.write_str("ClockStart"),
            Self::ClockEnd => f.write_str("ClockEnd"),
        }
    }
}
