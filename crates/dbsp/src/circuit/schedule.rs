//! The scheduling framework controls the execution of a circuit at runtime.

#![allow(async_fn_in_trait)]

use super::{trace::SchedulerEvent, Circuit, GlobalNodeId, NodeId};
use crate::{DetailedError, Position};
use itertools::Itertools;
use serde::Serialize;
use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet},
    error::Error as StdError,
    fmt::{Display, Error as FmtError, Formatter},
    future::Future,
    pin::Pin,
    string::ToString,
};

mod dynamic_scheduler;
pub use dynamic_scheduler::DynamicScheduler;

/// Scheduler errors.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(untagged)]
pub enum Error {
    /// `origin` node has more than one strong successors who insist on
    /// consuming its output by value
    /// (`OwnershipPreference::STRONGLY_PREFER_OWNED` or higher).
    OwnershipConflict {
        origin: GlobalNodeId,
        consumers: Vec<GlobalNodeId>,
    },
    /// Ownership constraints introduce a cycle in the circuit graph.
    CyclicCircuit {
        node_id: GlobalNodeId,
    },
    CommitWithoutTransaction,
    StepWithoutTransaction,
    /// Execution of the circuit interrupted by the user (via
    /// [`RuntimeHandle::kill`](`crate::circuit::RuntimeHandle::kill`)).
    Killed,
    TokioError {
        error: String,
    },
    /// Replay info conflict.
    ///
    /// All workers should enter the replay mode with identical sets of replay and
    /// backfill nodes. If this is not the case, this indicates a bug or a corrupted
    /// checkpoint.
    ReplayInfoConflict {
        error: String,
    },
}

impl DetailedError for Error {
    fn error_code(&self) -> Cow<'static, str> {
        match self {
            Self::OwnershipConflict { .. } => Cow::from("OwnershipConflict"),
            Self::CyclicCircuit { .. } => Cow::from("CyclicCircuit"),
            Self::CommitWithoutTransaction => Cow::from("CommitWithoutTransaction"),
            Self::StepWithoutTransaction => Cow::from("StepWithoutTransaction"),
            Self::Killed => Cow::from("Killed"),
            Self::TokioError { .. } => Cow::from("TokioError"),
            Self::ReplayInfoConflict { .. } => Cow::from("ReplayInfoConflict"),
        }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), FmtError> {
        match self {
            Self::OwnershipConflict { origin, consumers } => {
                write!(f, "ownership conflict: output of node '{origin}' is consumed by value by the following nodes: [{}]",
                               consumers.iter().map(ToString::to_string).format(","))
            }
            Self::CyclicCircuit { node_id } => {
                write!(f, "unschedulable circuit due to a cyclic topology: cycle through node '{node_id}'")
            }
            Error::CommitWithoutTransaction => {
                f.write_str("commit invoked outside of a transaction")
            }
            Error::StepWithoutTransaction => f.write_str("step called outside of a transaction"),
            Self::Killed => f.write_str("circuit has been killed by the user"),
            Self::TokioError { error } => write!(f, "tokio error: {error}"),
            Self::ReplayInfoConflict { error } => {
                write!(f, "replay info conflict: {error}")
            }
        }
    }
}

impl StdError for Error {}

/// Progress toward committing a transaction.
#[derive(Debug)]
pub struct CommitProgress {
    /// Nodes that have been fully flushed along with the latest positions when flush completed.
    completed: BTreeMap<NodeId, Option<Position>>,

    /// Nodes that are currently being flushed, along with the current positions.
    in_progress: BTreeMap<NodeId, Option<Position>>,

    /// Nodes that are yet to be flushed.
    remaining: BTreeSet<NodeId>,
}

impl Default for CommitProgress {
    fn default() -> Self {
        Self::new()
    }
}

impl CommitProgress {
    pub fn new() -> Self {
        Self {
            completed: BTreeMap::new(),
            in_progress: BTreeMap::new(),
            remaining: BTreeSet::new(),
        }
    }

    pub fn add_remaining(&mut self, node_id: NodeId) {
        self.remaining.insert(node_id);
    }

    pub fn add_completed(&mut self, node_id: NodeId, progress: Option<Position>) {
        debug_assert!(!self.completed.contains_key(&node_id));
        debug_assert!(!self.in_progress.contains_key(&node_id));
        debug_assert!(!self.remaining.contains(&node_id));

        self.completed.insert(node_id, progress);
    }

    pub fn add_in_progress(&mut self, node_id: NodeId, progress: Option<Position>) {
        debug_assert!(!self.completed.contains_key(&node_id));
        debug_assert!(!self.in_progress.contains_key(&node_id));
        debug_assert!(!self.remaining.contains(&node_id));

        self.in_progress.insert(node_id, progress);
    }

    pub fn summary(&self) -> CommitProgressSummary {
        let completed = self.completed.len() as u64;
        let in_progress = self.in_progress.len() as u64;
        let remaining = self.remaining.len() as u64;
        let in_progress_processed_records = self
            .in_progress
            .values()
            .map(|progress| progress.as_ref().map(|p| p.offset).unwrap_or_default())
            .sum();

        let in_progress_total_records = self
            .in_progress
            .values()
            .map(|progress| progress.as_ref().map(|p| p.total).unwrap_or_default())
            .sum();

        CommitProgressSummary {
            completed,
            in_progress,
            remaining,
            in_progress_processed_records,
            in_progress_total_records,
        }
    }
}

/// Summary of the commit progress.
pub struct CommitProgressSummary {
    /// Number of operators that have been fully flushed.
    completed: u64,

    /// Number of operators that are currently being flushed.
    in_progress: u64,

    /// Number of operators that haven't started flushing.
    remaining: u64,

    /// Number of records processed by operators that are currently being flushed.
    in_progress_processed_records: u64,

    // Total number of records that operators that are currently being flushed need to process.
    in_progress_total_records: u64,
}

impl Default for CommitProgressSummary {
    fn default() -> Self {
        Self::new()
    }
}

impl CommitProgressSummary {
    pub fn new() -> Self {
        Self {
            completed: 0,
            in_progress: 0,
            remaining: 0,
            in_progress_processed_records: 0,
            in_progress_total_records: 0,
        }
    }

    pub fn merge(&mut self, other: &CommitProgressSummary) {
        self.completed += other.completed;
        self.in_progress += other.in_progress;
        self.remaining += other.remaining;
        self.in_progress_processed_records += other.in_progress_processed_records;
        self.in_progress_total_records += other.in_progress_total_records;
    }
}

impl Display for CommitProgressSummary {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "completed: {} operators, evaluating: {} operators [{}/{} changes processed], remaining: {} operators",
            self.completed, self.in_progress, self.in_progress_processed_records, self.in_progress_total_records, self.remaining
        )
    }
}

/// A scheduler defines the order in which nodes in a circuit are evaluated at
/// runtime.
///
/// Schedulers are per-[`Circuit`], not per-[`Runtime`](crate::Runtime).  They
/// decide the order in which a step through the circuit evaluates operators.
/// They do not affect process or thread scheduling.
///
/// This API supports two-level scheduling of circuits: **steps** and **transactions**.
///
/// ## Steps
///
/// During a step, the scheduler evaluates each node exactly once, after all of its
/// upstream nodes have been evaluated.  Note that this works for circuits with logical
/// cycles, as all such cycles must contain a [strict
/// operator](`crate::circuit::operator_traits::StrictOperator`), which maps
/// into a pair of source and sink nodes, so that the resulting circuit is still
/// acyclic and output of the strict operator is evaluated before feed input to
/// it.  In addition, the scheduler must wait for an async operator to be in a
/// ready state before evaluating it (see
/// [`Operator::is_async`](`crate::circuit::operator_traits::Operator`)).
///
/// ## Transactions
///
/// A transaction is a sequence of steps that evaluate a set of inputs for a single logical
/// timestamp to completion.
///
/// Transaction lifecycle:
///
/// ```text
///                              is_commit_complete() = true
///    ┌────────────────────────────────────────────────────────────────────────────────────┐
///    ▼                                                                                    │
/// ┌───────┐      start_transaction()      ┌───────────┐ start_commit_transaction()  ┌─────┴────┐
/// │ idle  ├──────────────────────────────►│  started  ├────────────────────────────►│committing│
/// └───────┘                               └────────┬──┘                             └─────────┬┘
///                                           ▲      │                                    ▲     │
///                                           └──────┘                                    └─────┘
///                                            step()                                      step()
/// ```
///
/// During the in-progress phase, each operator gets to decide how much of the input to process.
/// Some operators may accumulate inputs to process them later.
///
/// During the committing phase, the scheduler forces operators to process their inputs to completion
/// by invoking `flush` on each operator. Once all predecessor of an operator have finished processing
/// inputs for the current transaction, the scheduler invokes `flush` of the operator. It tracks the
/// frontier of flushed operators and reports `is_commit_complete()` as true once all operators have
/// been flushed.
pub trait Scheduler
where
    Self: Sized,
{
    fn new() -> Self;

    /// Initialize the scheduler for the circuit.
    ///
    /// Invoked before running the circuit to validate that the circuit is schedulable,
    /// e.g., doesn't contain circular scheduling dependencies, and to initialize any
    /// internal scheduler state.
    ///
    /// This function can be invoked multiple times, e.g., once before running the circuit
    /// in the backfill mode, and once before running the circuit in the normal mode.
    ///
    /// # Arguments
    ///
    /// * `nodes` - when specified, only the nodes in this set must be scheduled. All
    ///   other nodes remain idle.
    fn prepare<C>(&mut self, circuit: &C, nodes: Option<&BTreeSet<NodeId>>) -> Result<(), Error>
    where
        C: Circuit;

    /// Start a transaction.
    async fn start_transaction<C>(&self, circuit: &C) -> Result<(), Error>
    where
        C: Circuit;

    /// Start committing the current transaction.
    fn start_commit_transaction(&self) -> Result<(), Error>;

    /// Check if the current transaction is complete.
    ///
    /// Must be invoked after every `step` in the `committing` phase of the transaction.
    fn is_commit_complete(&self) -> bool;

    /// Estimated commit progress.
    fn commit_progress(&self) -> CommitProgress;

    /// Evaluate the circuit at runtime.
    ///
    /// Evaluates each node in the circuit exactly once in an order that
    /// respects (1) its dependency graph, and (2) the
    /// [`ready`](`crate::circuit::operator_traits::Operator::ready`) status
    /// of async operators.
    ///
    /// # Arguments
    ///
    /// * `circuit` - circuit to schedule, this must be the same circuit for
    ///   which the schedule was computed.
    async fn step<C>(&self, circuit: &C) -> Result<(), Error>
    where
        C: Circuit;

    async fn transaction<C>(&self, circuit: &C) -> Result<(), Error>
    where
        C: Circuit,
    {
        self.start_transaction(circuit).await?;
        self.start_commit_transaction()?;
        while !self.is_commit_complete() {
            self.step(circuit).await?;
        }

        Ok(())
    }
}

/// An executor executes a circuit by evaluating all of its operators using a
/// `Scheduler`. It can run the circuit exactly once or multiple times, until
/// some termination condition is reached.
pub trait Executor<C>: 'static {
    fn prepare(&mut self, circuit: &C, nodes: Option<&BTreeSet<NodeId>>) -> Result<(), Error>;

    fn start_commit_transaction(&self) -> Result<(), Error>;

    fn is_commit_complete(&self) -> bool;

    fn commit_progress(&self) -> CommitProgress;

    fn start_transaction<'a>(
        &'a self,
        circuit: &'a C,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + 'a>>;

    fn step<'a>(&'a self, circuit: &'a C) -> Pin<Box<dyn Future<Output = Result<(), Error>> + 'a>>;

    fn transaction<'a>(
        &'a self,
        circuit: &'a C,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + 'a>>;
}

/// An iterative executor evaluates the circuit until the `termination_check`
/// callback returns true.  Every time the executor is invoked, it first sends
/// the `clock_start` notification to all operators in the circuit. It then
/// evaluates the circuit until the termination condition is satisfied (but at
/// least once), and finally calls `clock_end` on it.
pub(crate) struct IterativeExecutor<F, S> {
    termination_check: F,
    scheduler: S,
}

impl<F, S> IterativeExecutor<F, S> {
    pub(crate) fn new(termination_check: F) -> Self
    where
        S: Scheduler,
    {
        Self {
            termination_check,
            scheduler: <S as Scheduler>::new(),
        }
    }
}

impl<C, F, S> Executor<C> for IterativeExecutor<F, S>
where
    F: AsyncFn() -> Result<bool, Error> + 'static,
    C: Circuit,
    S: Scheduler + 'static,
{
    fn start_transaction<'a>(
        &'a self,
        _circuit: &C,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + 'a>> {
        unimplemented!()
    }

    fn step<'a>(&'a self, circuit: &'a C) -> Pin<Box<dyn Future<Output = Result<(), Error>> + 'a>> {
        let circuit = circuit.clone();
        Box::pin(async move { self.scheduler.step(&circuit).await })
    }

    fn transaction<'a>(
        &'a self,
        circuit: &C,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + 'a>> {
        let circuit = circuit.clone();
        Box::pin(async move {
            circuit.log_scheduler_event(&SchedulerEvent::clock_start());
            circuit.clock_start(0);

            loop {
                self.scheduler.transaction(&circuit).await?;
                if (self.termination_check)().await? {
                    break;
                }
            }

            circuit.log_scheduler_event(&SchedulerEvent::clock_end());
            circuit.clock_end(0);
            Ok(())
        })
    }

    fn prepare(&mut self, circuit: &C, nodes: Option<&BTreeSet<NodeId>>) -> Result<(), Error> {
        self.scheduler.prepare(circuit, nodes)
    }

    fn start_commit_transaction(&self) -> Result<(), Error> {
        Ok(())
    }

    fn commit_progress(&self) -> CommitProgress {
        CommitProgress::new()
    }

    fn is_commit_complete(&self) -> bool {
        true
    }
}

/// An executor that evaluates the circuit exactly once every time it is
/// invoked.
pub(crate) struct OnceExecutor<S> {
    scheduler: S,
}

impl<S> OnceExecutor<S>
where
    S: Scheduler,
    Self: Sized,
{
    pub(crate) fn new() -> Self {
        Self {
            scheduler: <S as Scheduler>::new(),
        }
    }
}

impl<C, S> Executor<C> for OnceExecutor<S>
where
    C: Circuit,
    S: Scheduler + 'static,
{
    fn start_transaction<'a>(
        &'a self,
        circuit: &'a C,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + 'a>> {
        Box::pin(async { self.scheduler.start_transaction(circuit).await })
    }

    fn step<'a>(&'a self, circuit: &'a C) -> Pin<Box<dyn Future<Output = Result<(), Error>> + 'a>> {
        Box::pin(async { self.scheduler.step(circuit).await })
    }

    fn transaction<'a>(
        &'a self,
        circuit: &'a C,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + 'a>> {
        Box::pin(async { self.scheduler.transaction(circuit).await })
    }

    fn prepare(&mut self, circuit: &C, nodes: Option<&BTreeSet<NodeId>>) -> Result<(), Error> {
        self.scheduler.prepare(circuit, nodes)
    }

    fn start_commit_transaction(&self) -> Result<(), Error> {
        self.scheduler.start_commit_transaction()
    }

    fn is_commit_complete(&self) -> bool {
        self.scheduler.is_commit_complete()
    }

    fn commit_progress(&self) -> CommitProgress {
        self.scheduler.commit_progress()
    }
}

/// Some useful tools for developing schedulers.
mod util {

    use crate::circuit::{
        circuit_builder::StreamId, schedule::Error, Circuit, GlobalNodeId, NodeId,
        OwnershipPreference,
    };
    use petgraph::graphmap::DiGraphMap;
    use std::{collections::HashMap, ops::Deref};

    /// Dump circuit topology as a graph.
    pub(crate) fn circuit_graph<C>(circuit: &C) -> DiGraphMap<NodeId, ()>
    where
        C: Circuit,
    {
        let mut g = DiGraphMap::<NodeId, ()>::new();

        for node_id in circuit.node_ids().into_iter() {
            g.add_node(node_id);
        }

        for edge in circuit.edges().deref().iter() {
            g.add_edge(edge.from, edge.to, ());
        }

        g
    }

    /// Helper function used by schedulers to enforce ownership preferences.
    ///
    /// Individual schedulers can implement their own algorithms to enforce (or
    /// ignore) ownership preferences.  This helper function can optionally be
    /// used by schedulers that wish to implement one particular approach.
    /// The idea is to treat **strong** ownership preferences
    /// (`OwnershipPreference::STRONGLY_PREFER_OWNED` and above) as scheduling
    /// constraints: assuming an operator has exactly one successor with a
    /// strong ownership preference ("strong successor"), we can enforce
    /// this preference by scheduling this successor last. This scheduling
    /// constraint can in turn be enforced by adding a dependency edge from
    /// all other successors to the strong successor node to the circuit graph.
    /// This function computes the set of dependency edges needed to enforce
    /// all such constraints in the circuit.
    ///
    /// # Caveat
    ///
    /// The additional edges computed by this function can introduce cycles to
    /// the circuit graph making it unschedulable.  Schedulers must check
    /// for cycles before adding these constraints and either fail or drop
    /// some of the constraints to eliminate cycles.
    ///
    /// (Current scheduler implementations fail if there is a cycle).
    ///
    /// # Errors
    ///
    /// The function fails with [`Error::OwnershipConflict`] if the circuit has
    /// at least one node with multiple strong successors.
    pub(crate) fn ownership_constraints<C>(circuit: &C) -> Result<Vec<(NodeId, NodeId)>, Error>
    where
        C: Circuit,
    {
        // Compute successors of each node in the circuit.  Note: we index successors by
        // origin id, not local node id, since the former uniquely identifies a
        // stream, but the latter doesn't, since a subcircuit node can have
        // multiple output streams.
        let num_nodes = circuit.num_nodes();
        let mut successors: HashMap<
            (GlobalNodeId, StreamId),
            Vec<(NodeId, Option<OwnershipPreference>)>,
        > = HashMap::with_capacity(num_nodes);

        for edge in circuit.edges().deref().iter() {
            let Some(stream_id) = edge.stream_id() else {
                continue;
            };
            let origin = edge.origin.clone();

            successors
                .entry((origin, stream_id))
                .or_default()
                .push((edge.to, edge.ownership_preference));
        }

        let mut constraints = Vec::new();

        for ((origin, _), succ) in successors.into_iter() {
            // Find all strong successors of a node.
            let strong_successors: Vec<_> = succ
                .iter()
                .enumerate()
                .filter(|(_i, (_, pref))| {
                    pref.is_some() && pref.unwrap() >= OwnershipPreference::STRONGLY_PREFER_OWNED
                })
                .collect();

            // Declare conflict if there's more than one strong successor.
            if strong_successors.len() > 1 {
                return Err(Error::OwnershipConflict {
                    origin,
                    consumers: strong_successors
                        .into_iter()
                        .map(|(_, (suc, _))| GlobalNodeId::child_of(circuit, *suc))
                        .collect(),
                });
            };

            // No strong successors -- nothing to do for this node.
            if strong_successors.is_empty() {
                continue;
            }

            // A unique strong successor found; add edges from all other successors to it.
            let strong_successor_index = strong_successors[0].0;
            for (i, successor) in succ.iter().enumerate() {
                // Ignore dependency edges.
                if i != strong_successor_index && successor.1.is_some() {
                    constraints.push((successor.0, succ[strong_successor_index].0));
                }
            }
        }

        Ok(constraints)
    }
}
