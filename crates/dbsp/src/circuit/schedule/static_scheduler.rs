//! Static scheduler.

use crate::circuit::{
    runtime::Runtime,
    schedule::{
        util::{circuit_graph, ownership_constraints},
        Error, Scheduler,
    },
    trace::SchedulerEvent,
    Circuit, GlobalNodeId, NodeId,
};
use petgraph::algo::toposort;
use std::thread::yield_now;

/// Static scheduler evaluates nodes in the circuit in a fixed order computed
/// based on its dependency graph.
pub struct StaticScheduler {
    schedule: Vec<(NodeId, bool)>,
}

impl Scheduler for StaticScheduler {
    // Compute a schedule that respects the dependency graph by arranging
    // nodes in a topological order.
    // TODO: compute a schedule that takes into account operators that consume
    // inputs by-value.
    fn prepare<C>(circuit: &C) -> Result<Self, Error>
    where
        C: Circuit,
    {
        let mut g = circuit_graph(circuit);

        // Add ownership constraints to the graph.
        let extra_constraints = ownership_constraints(circuit)?;

        for (from, to) in extra_constraints.into_iter() {
            g.add_edge(from, to, ());
        }

        // `toposort` fails if the graph contains cycles.
        // The circuit_builder API makes it impossible to construct such graphs.
        let schedule = toposort(&g, None)
            .map_err(|e| Error::CyclicCircuit {
                node_id: GlobalNodeId::child_of(circuit, e.node_id()),
            })?
            .into_iter()
            .map(|node_id| (node_id, circuit.is_async_node(node_id)))
            .collect();

        Ok(Self { schedule })
    }

    fn step<C>(&self, circuit: &C) -> Result<(), Error>
    where
        C: Circuit,
    {
        circuit.log_scheduler_event(&SchedulerEvent::step_start());

        for (node_id, is_async) in self.schedule.iter() {
            if !is_async {
                if Runtime::kill_in_progress() {
                    return Err(Error::Killed);
                }
                circuit.eval_node(*node_id)?;
            } else {
                loop {
                    if Runtime::kill_in_progress() {
                        return Err(Error::Killed);
                    }
                    if circuit.ready(*node_id) {
                        circuit.eval_node(*node_id)?;
                        break;
                    }
                    yield_now();
                }
            }
        }
        circuit.tick();

        circuit.log_scheduler_event(&SchedulerEvent::step_end());
        Ok(())
    }
}
