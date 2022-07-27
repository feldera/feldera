use dbsp::{
    operator::{FilterMap, Generator},
    time::NestedTimestamp32,
    trace::{ord::OrdZSet, Batch, BatchReader},
    Circuit, Runtime, Stream,
};

fn main() {
    let hruntime = Runtime::run(16, || {
        let circuit = Circuit::build(|circuit| {
            /*
            use dbsp::{
                circuit::{trace::SchedulerEvent, GlobalNodeId},
                monitor::TraceMonitor,
                profile::CPUProfiler,
            };
            use std::{collections::HashMap, fmt::Write, fs};

            let monitor = TraceMonitor::new_panic_on_error();
            let cpu_profiler = CPUProfiler::new();
            cpu_profiler.attach(circuit, "cpu profiler");

            monitor.attach(circuit, "monitor");
            let mut metadata = <HashMap<GlobalNodeId, String>>::new();
            let mut nsteps = 0;
            let monitor_clone = monitor.clone();
            circuit.register_scheduler_event_handler("metadata", move |event: &SchedulerEvent| {
                match event {
                    SchedulerEvent::EvalEnd { node } => {
                        let metadata_string = metadata.entry(node.global_id().clone()).or_default();
                        metadata_string.clear();
                        node.summary(metadata_string);
                    }
                    SchedulerEvent::StepEnd => {
                        let graph = monitor_clone.visualize_circuit_annotate(|node_id| {
                            let mut metadata_string = metadata
                                .get(node_id)
                                .map(ToString::to_string)
                                .unwrap_or_else(|| "".to_string());

                            if let Some(cpu_profile) = cpu_profiler.operator_profile(node_id) {
                                writeln!(
                                    metadata_string,
                                    "invocations: {}",
                                    cpu_profile.invocations()
                                )
                                .unwrap();
                                writeln!(metadata_string, "time: {:?}", cpu_profile.total_time())
                                    .unwrap();
                            };
                            metadata_string
                        });

                        fs::write(format!("path.{}.dot", nsteps), graph.to_dot()).unwrap();
                        nsteps += 1;
                    }
                    _ => {}
                }
            });
            */

            const LAYER: u32 = 200;

            let mut tuples = Vec::new();
            if Runtime::worker_index() == 0 {
                for layer in 0..5 {
                    for from in 0..LAYER {
                        for to in 0..LAYER {
                            tuples.push((
                                ((from + (LAYER * layer), to + LAYER * (layer + 1)), ()),
                                1,
                            ));
                        }
                    }
                }
            }

            let edges = <OrdZSet<(u32, u32), i32>>::from_tuples((), tuples);

            let edges: Stream<_, OrdZSet<(u32, u32), i32>> =
                circuit.add_source(Generator::new(move || edges.clone()));

            let paths = circuit
                .recursive(|child, paths: Stream<_, OrdZSet<(u32, u32), i32>>| {
                    // ```text
                    //                      distinct_trace
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
                    //               join
                    // ```
                    let edges = edges.delta0(child);

                    let paths_inverted = paths.map(|&(x, y)| (y, x));

                    let paths_inverted_indexed = paths_inverted.index();
                    let edges_indexed = edges.index();

                    Ok(edges.plus(
                        &paths_inverted_indexed.join::<NestedTimestamp32, _, _, _>(
                            &edges_indexed,
                            |_via, from, to| (*from, *to),
                        ),
                    ))
                })
                .unwrap();

            paths.gather(0).inspect(|zs: &OrdZSet<_, _>| {
                if Runtime::worker_index() == 0 {
                    println!("paths: {}", zs.len())
                }
            });
        })
        .unwrap();

        //let graph = monitor.visualize_circuit();
        //fs::write("path.dot", graph.to_dot()).unwrap();

        circuit.step().unwrap();
    });

    hruntime.join().unwrap();
}
