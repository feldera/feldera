//! A schedule controls the execution of a circuit.

use super::circuit_builder::{Circuit, NodeId, Stream};

use petgraph::{algo::toposort, graphmap::DiGraphMap};
use std::ops::Deref;

// A schedule defines the order in which operators in a circuit should be evaluated.  A valid
// schedule evals each operator exactly once, after all of its upstream operators have been
// evaluated.
struct Schedule {
    schedule: Vec<NodeId>,
}

impl Schedule {
    // Compute schedule for a circuit.  We may want to support multiple scheduling algorithms
    // in the future, but for now simple topological sorting seems good enough.
    // TODO: compute a schedule that takes into account operators that consume inputs by-value.
    fn schedule_circuit<P>(circuit: &Circuit<P>) -> Self {
        let g = DiGraphMap::<NodeId, ()>::from_edges(circuit.edges().deref());
        // `toposort` fails if the graph contains cycles.
        // The circuit_builder API should make it impossible to construct such graphs.
        let schedule =
            toposort(&g, None).unwrap_or_else(|e| panic!("cycle in the circuit graph: {:?}", e));
        Self { schedule }
    }

    // Run the schedule against a circuit.  `circuit` must be the same circuit for which the
    // schedule was computed.
    fn run<P>(&self, circuit: &Circuit<P>)
    where
        P: Clone + 'static,
    {
        for node_id in self.schedule.iter() {
            circuit.eval_node(*node_id);
        }
    }
}

/// A scheduler executes a circuit by evaluating all of its operators according to a `Schedule`.
/// It can run the circuit exactly once or multiple times, until some termimation condition is
/// reached.
pub(crate) trait Scheduler<P>: 'static {
    fn run(&self, circuit: &Circuit<P>);
}

/// An iterative scheduler evaluates the circuit until the circuit writes value `true` to
/// a designated termination stream.  Every time the scheduler is invoked, it first send
/// the `stream_start` notification to all operators in the circuit, it then evaluates the
/// circuit until the termination condition is satisfied (but at least once), and finally
/// calls `stream_end` on it.
pub(crate) struct IterativeScheduler<P> {
    termination_stream: Stream<Circuit<P>, bool>,
    schedule: Schedule,
}

impl<P> IterativeScheduler<P> {
    pub(crate) fn new(circuit: &Circuit<P>, termination_stream: Stream<Circuit<P>, bool>) -> Self {
        Self {
            termination_stream,
            schedule: Schedule::schedule_circuit(circuit),
        }
    }
}

impl<P> Scheduler<P> for IterativeScheduler<P>
where
    P: Clone + 'static,
{
    fn run(&self, circuit: &Circuit<P>) {
        circuit.stream_start();

        loop {
            self.schedule.run(circuit);
            if unsafe { *self.termination_stream.get() } == Some(true) {
                break;
            }
        }

        unsafe { circuit.stream_end() };
    }
}

/// A scheduler that evaluates the circuit exactly once every time it is invoked.
pub(crate) struct OnceScheduler {
    schedule: Schedule,
}

impl OnceScheduler {
    pub(crate) fn new<P>(circuit: &Circuit<P>) -> Self {
        Self {
            schedule: Schedule::schedule_circuit(circuit),
        }
    }
}

impl<P> Scheduler<P> for OnceScheduler
where
    P: Clone + 'static,
{
    fn run(&self, circuit: &Circuit<P>) {
        self.schedule.run(circuit);
    }
}
