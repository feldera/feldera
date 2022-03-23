use dbsp::{
    algebra::{FiniteHashMap, HasZero, MapBuilder},
    circuit::{trace::SchedulerEvent, GlobalNodeId, Root, Stream},
    monitor::TraceMonitor,
    operator::{DelayedFeedback, Generator},
};
use std::{collections::HashMap, fs, vec};

fn main() {
    let monitor = TraceMonitor::new_panic_on_error();
    let root = Root::build(|circuit| {
        monitor.attach(circuit, "monitor");
        let mut metadata = <HashMap<GlobalNodeId, String>>::new();
        let mut nsteps = 0;
        let monitor_clone = monitor.clone();
        circuit.register_scheduler_event_handler("metadata", move |event: &SchedulerEvent| {
            match event {
                SchedulerEvent::EvalEnd { node } => {
                    let metadata_string = metadata
                        .entry(node.global_id().clone())
                        .or_insert_with(|| String::new());
                    metadata_string.clear();
                    node.summary(metadata_string);
                }
                SchedulerEvent::StepEnd => {
                    let graph = monitor_clone.visualize_circuit_annotate(&|node_id| {
                        metadata
                            .get(node_id)
                            .map(ToString::to_string)
                            .unwrap_or_else(|| "".to_string())
                    });
                    fs::write(format!("path.{}.dot", nsteps), graph.to_dot()).unwrap();
                    nsteps += 1;
                }
                _ => {}
            }
        });

        const LAYER: u32 = 200;

        let mut edges: FiniteHashMap<(u32, u32), i32> = FiniteHashMap::new();
        for layer in 0..5 {
            for from in 0..LAYER {
                for to in 0..LAYER {
                    edges.increment(&(from + (LAYER * layer), to + LAYER * (layer + 1)), 1);
                }
            }
        }

        let edges: Stream<_, FiniteHashMap<(u32, u32), i32>> =
            circuit.add_source(Generator::new(move || edges.clone()));

        let _paths = circuit
            .iterate_with_conditions(|child| {
                // ```text
                //                      distinct_incremental_nested
                //               ┌───┐          ┌───┐
                // edges         │   │          │   │  paths
                // ────┬────────►│ + ├──────────┤   ├────────┬───►
                //     │         │   │          │   │        │
                //     │         └───┘          └───┘        │
                //     │           ▲                         │
                //     │           │                         │
                //     │         ┌─┴─┐                       │
                //     │         │   │                       │
                //     └────────►│ X │ ◄─────────────────────┘
                //               │   │
                //               └───┘
                //      join_incremental_nested
                // ```
                let edges = edges.delta0(child);
                let paths_delayed = <DelayedFeedback<_, FiniteHashMap<_, _>>>::new(child);

                let paths_inverted: Stream<_, FiniteHashMap<(u32, u32), i32>> =
                    paths_delayed.stream().map_keys(|&(x, y)| (y, x));

                let paths_inverted_indexed: Stream<_, FiniteHashMap<u32, FiniteHashMap<u32, i32>>> =
                    paths_inverted.index();
                let edges_indexed: Stream<_, FiniteHashMap<u32, FiniteHashMap<u32, i32>>> =
                    edges.index();

                let paths = edges
                    .plus(
                        &paths_inverted_indexed
                            .join_incremental_nested(&edges_indexed, |_via, from, to| (*from, *to)),
                    )
                    .distinct_incremental_nested();
                paths_delayed.connect(&paths);
                let output = paths.integrate();
                Ok((
                    vec![
                        paths.condition(HasZero::is_zero),
                        paths.integrate_nested().condition(HasZero::is_zero),
                    ],
                    output.export(),
                ))
            })
            .unwrap();
    })
    .unwrap();

    let graph = monitor.visualize_circuit();
    fs::write("path.dot", graph.to_dot()).unwrap();

    root.step().unwrap();
}
