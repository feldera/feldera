//! The scheduling framework controls the execution of a circuit at runtime.

use super::{trace::SchedulerEvent, Circuit, GlobalNodeId};

mod static_scheduler;
pub use static_scheduler::StaticScheduler;

mod dynamic_scheduler;
pub use dynamic_scheduler::DynamicScheduler;

/// Scheduler errors.
#[derive(Debug)]
pub enum Error {
    /// `origin` node has more than one strong successors who insist on
    /// consuming its output by value (`OwnershipPreference::
    /// STRONGLY_PREFER_OWNED` or higher).
    OwnershipConflict {
        origin: GlobalNodeId,
        consumers: Vec<GlobalNodeId>,
    },
    /// Ownership constraints introduce a cycle in the circuit graph.
    CyclicCircuit { node_id: GlobalNodeId },
    /// Execution of the circuit interrupted by the user (via
    /// [`RuntimeHandle::kill`](`crate::circuit::RuntimeHandle::kill`)).
    Killed,
}

/// A scheduler defines the order in which nodes in a circuit are evaluated at
/// runtime.
///
/// A valid schedule evaluates each node exactly once, after all of its upstream
/// nodes have been evaluated.  Note that this works for circuits with logical
/// cycles, as all such cycles must contain a strict operator, which maps into a
/// pair of source and sink nodes, so that the resulting circuit is still
/// acyclic and output of the strict operator is evaluated before feed input to
/// it.  In addition, the scheduler must wait for an async operator to be in a
/// ready state before evaluating it (see
/// [`Operator::is_async`](`crate::circuit::operator_traits::Operator`)).
pub trait Scheduler
where
    Self: Sized,
{
    /// Create a scheduler for a circuit.
    ///
    /// This method is invoked at circuit construction time to perform any
    /// required preparatory computation, e.g., compute a complete static
    /// schedule or build data structures needed for dynamic scheduling.
    fn prepare<P>(circuit: &Circuit<P>) -> Result<Self, Error>
    where
        P: Clone + 'static;

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
    fn step<P>(&self, circuit: &Circuit<P>) -> Result<(), Error>
    where
        P: Clone + 'static;
}

/// An executor executes a circuit by evaluating all of its operators using a
/// `Scheduler`. It can run the circuit exactly once or multiple times, until
/// some termination condition is reached.
pub(crate) trait Executor<P>: 'static {
    fn run(&self, circuit: &Circuit<P>) -> Result<(), Error>;
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
    pub(crate) fn new<P>(circuit: &Circuit<P>, termination_check: F) -> Result<Self, Error>
    where
        P: Clone + 'static,
        S: Scheduler,
    {
        Ok(Self {
            termination_check,
            scheduler: <S as Scheduler>::prepare(circuit)?,
        })
    }
}

impl<P, F, S> Executor<P> for IterativeExecutor<F, S>
where
    F: Fn() -> Result<bool, Error> + 'static,
    P: Clone + 'static,
    S: Scheduler + 'static,
{
    fn run(&self, circuit: &Circuit<P>) -> Result<(), Error> {
        circuit.log_scheduler_event(&SchedulerEvent::clock_start());
        circuit.clock_start(0);

        loop {
            self.scheduler.step(circuit)?;
            if (self.termination_check)()? {
                break;
            }
        }

        circuit.log_scheduler_event(&SchedulerEvent::clock_end());
        unsafe { circuit.clock_end(0) };
        Ok(())
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
    pub(crate) fn new<P>(circuit: &Circuit<P>) -> Result<Self, Error>
    where
        P: Clone + 'static,
    {
        Ok(Self {
            scheduler: <S as Scheduler>::prepare(circuit)?,
        })
    }
}

impl<P, S> Executor<P> for OnceExecutor<S>
where
    P: Clone + 'static,
    S: Scheduler + 'static,
{
    fn run(&self, circuit: &Circuit<P>) -> Result<(), Error> {
        self.scheduler.step(circuit)
    }
}

/// Some useful tools for developing schedulers.
mod util {

    use crate::circuit::{schedule::Error, Circuit, GlobalNodeId, NodeId, OwnershipPreference};
    use petgraph::graphmap::DiGraphMap;
    use std::{collections::HashMap, ops::Deref};

    /// Dump circuit topology as a graph.
    pub(crate) fn circuit_graph<P>(circuit: &Circuit<P>) -> DiGraphMap<NodeId, ()> {
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
    /// ignore) ownership preferences.  This helper function can optionally
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
    pub(crate) fn ownership_constraints<P>(
        circuit: &Circuit<P>,
    ) -> Result<Vec<(NodeId, NodeId)>, Error> {
        // Compute successors of each node in the circuit.  Note: we index successors by
        // origin id, not local node id, since the former uniquely identifies a
        // stream, but the latter doesn't, since a subcircuit node can have
        // multiple output streams.
        let num_nodes = circuit.num_nodes();
        let mut successors: HashMap<GlobalNodeId, Vec<(NodeId, Option<OwnershipPreference>)>> =
            HashMap::with_capacity(num_nodes);

        for edge in circuit.edges().deref().iter() {
            successors
                .entry(edge.origin.clone())
                .or_insert_with(Vec::new)
                .push((edge.to, edge.ownership_preference));
        }

        let mut constraints = Vec::new();

        for (origin, succ) in successors.into_iter() {
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
