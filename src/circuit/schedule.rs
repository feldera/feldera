//! A schedule controls the execution of a circuit.

use super::{trace::SchedulerEvent, Circuit, NodeId};

use petgraph::{algo::toposort, graphmap::DiGraphMap};
use std::{ops::Deref, thread::yield_now};

/// A scheduler defines the order in which nodes in a circuit are evaluated at runtime.
///
/// A valid schedule evaluates each node exactly once, after all of its upstream nodes have been
/// evaluated.  Note that this works for circuits with logical cycles, as all such cycles
/// must contain a strict operator, which maps into a pair of source and sink nodes, so
/// that the resulting circuit is still acyclic and output of the strict operator is
/// evaluated before feed input to it.  In addition, the scheduler must wait for an async
/// operator to be in a ready state before evaluating it
/// (see [`Operator::is_async`](`crate::circuit::operator_traits::Operator`)).
pub trait Scheduler {
    /// Create a scheduler for a circuit.
    ///
    /// This method is invoked at circuit construction time to perform any required
    /// preparatory computation, e.g., compute a complete static schedule or build
    /// data structures needed for dynamic scheduling.
    fn prepare<P>(circuit: &Circuit<P>) -> Self
    where
        P: Clone + 'static;

    /// Evaluate the circuit at runtime.
    ///
    /// Evaluates each node in the circuit exactly once in an order that respects
    /// (1) its dependency graph, and (2) the [`ready`](`crate::circuit::operator_traits::Operator::ready`)
    /// status of async operators.
    ///
    /// # Arguments
    ///
    /// * `circuit` - circuit to schedule, this must be the same circuit for which the schedule
    ///   was computed.
    fn step<P>(&self, circuit: &Circuit<P>)
    where
        P: Clone + 'static;
}

pub struct StaticScheduler {
    schedule: Vec<(NodeId, bool)>,
}

impl Scheduler for StaticScheduler {
    // Compute a schedule that respects the dependency graph by arranging
    // nodes in a topological order.
    // TODO: compute a schedule that takes into account operators that consume inputs by-value.
    fn prepare<P>(circuit: &Circuit<P>) -> Self
    where
        P: Clone + 'static,
    {
        let g = DiGraphMap::<NodeId, ()>::from_edges(circuit.edges().deref());
        // `toposort` fails if the graph contains cycles.
        // The circuit_builder API makes it impossible to construct such graphs.
        let schedule = toposort(&g, None)
            .unwrap_or_else(|e| panic!("cycle in the circuit graph: {:?}", e))
            .into_iter()
            .map(|node_id| (node_id, circuit.is_async_node(node_id)))
            .collect();

        Self { schedule }
    }

    fn step<P>(&self, circuit: &Circuit<P>)
    where
        P: Clone + 'static,
    {
        circuit.log_scheduler_event(&SchedulerEvent::step_start());

        for (node_id, is_async) in self.schedule.iter() {
            if !is_async {
                circuit.eval_node(*node_id);
            } else {
                loop {
                    if circuit.ready(*node_id) {
                        circuit.eval_node(*node_id);
                        break;
                    }
                    yield_now();
                }
            }
        }

        circuit.log_scheduler_event(&SchedulerEvent::step_end());
    }
}

/// An executor executes a circuit by evaluating all of its operators using a `Scheduler`.
/// It can run the circuit exactly once or multiple times, until some termination condition is
/// reached.
pub(crate) trait Executor<P>: 'static {
    fn run(&self, circuit: &Circuit<P>);
}

/// An iterative executor evaluates the circuit until the `termination_check` callback returns
/// true.  Every time the executor is invoked, it first sends the `clock_start` notification
/// to all operators in the circuit. It then evaluates the circuit until the termination condition
/// is satisfied (but at least once), and finally calls `clock_end` on it.
pub(crate) struct IterativeExecutor<F, S> {
    termination_check: F,
    scheduler: S,
}

impl<F, S> IterativeExecutor<F, S> {
    pub(crate) fn new<P>(circuit: &Circuit<P>, termination_check: F) -> Self
    where
        P: Clone + 'static,
        S: Scheduler,
    {
        Self {
            termination_check,
            scheduler: <S as Scheduler>::prepare(circuit),
        }
    }
}

impl<P, F, S> Executor<P> for IterativeExecutor<F, S>
where
    F: Fn() -> bool + 'static,
    P: Clone + 'static,
    S: Scheduler + 'static,
{
    fn run(&self, circuit: &Circuit<P>) {
        circuit.log_scheduler_event(&SchedulerEvent::clock_start());
        circuit.clock_start();

        loop {
            self.scheduler.step(circuit);
            if (self.termination_check)() {
                break;
            }
        }

        circuit.log_scheduler_event(&SchedulerEvent::clock_end());
        unsafe { circuit.clock_end() };
    }
}

/// An executor that evaluates the circuit exactly once every time it is invoked.
pub(crate) struct OnceExecutor<S> {
    scheduler: S,
}

impl<S> OnceExecutor<S>
where
    S: Scheduler,
{
    pub(crate) fn new<P>(circuit: &Circuit<P>) -> Self
    where
        P: Clone + 'static,
    {
        Self {
            scheduler: <S as Scheduler>::prepare(circuit),
        }
    }
}

impl<P, S> Executor<P> for OnceExecutor<S>
where
    P: Clone + 'static,
    S: Scheduler + 'static,
{
    fn run(&self, circuit: &Circuit<P>) {
        self.scheduler.step(circuit);
    }
}
