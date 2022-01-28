//! Static scheduler.

use crate::circuit::{schedule::Scheduler, trace::SchedulerEvent, Circuit, NodeId};
use petgraph::{algo::toposort, graphmap::DiGraphMap};
use std::{ops::Deref, thread::yield_now};

/// Static scheduler evaluates nodes in the circuit in a fixed order computed
/// based on its dependency graph.
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
        let mut g = DiGraphMap::<NodeId, ()>::new();
        for node_id in circuit.node_ids().into_iter() {
            g.add_node(node_id);
        }
        for (from, to) in circuit.edges().deref().iter() {
            g.add_edge(*from, *to, ());
        }

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
