mod bfs;
mod data;

use crate::data::DataSet;
use clap::Parser;
use dbsp::{
    circuit::{trace::SchedulerEvent, Root, Runtime},
    monitor::TraceMonitor,
    operator::Generator,
    profile::CPUProfiler,
    zset_set,
};
use hashbrown::HashMap;
use std::{cell::RefCell, fmt::Write as _, io::Write, rc::Rc, time::Instant};

fn main() {
    let config = Config::parse();
    let dataset = config.dataset;

    println!("loading dataset {}...", dataset.name);
    let start = Instant::now();

    let (properties, edges, vertices, expected_output) = dataset.load().unwrap();
    let mut root_data = Some(zset_set! { properties.source_vertex });
    let mut vertex_data = Some(vertices);
    let mut edge_data = Some(edges);

    let elapsed = start.elapsed();
    println!("finished in {elapsed:#?}");

    println!(
        "using dataset {} with {} vertices and {} edges",
        dataset.name, properties.vertices, properties.edges,
    );
    Runtime::run(1, move |_runtime, _index| {
        print!("building dataflow... ");
        std::io::stdout().flush().unwrap();
        let start = Instant::now();

        let output = Rc::new(RefCell::new(None));

        let output_inner = output.clone();
        let root = Root::build(move |circuit| {
            if config.profile {
                let cpu_profiler = CPUProfiler::new();
                cpu_profiler.attach(circuit, "cpu profiler");

                let monitor = TraceMonitor::new_panic_on_error();
                monitor.attach(circuit, "monitor");

                let mut metadata = HashMap::<_, String>::new();
                let mut steps = 0;

                circuit.register_scheduler_event_handler("metadata", move |event| match event {
                    SchedulerEvent::EvalEnd { node } => {
                        let metadata_string = metadata.entry(node.global_id().clone()).or_default();
                        metadata_string.clear();
                        node.summary(metadata_string);
                    }

                    SchedulerEvent::StepEnd => {
                        let graph = monitor.visualize_circuit_annotate(|node_id| {
                            let mut metadata_string = metadata
                                .get(node_id)
                                .map(ToString::to_string)
                                .unwrap_or_else(|| "".to_string());

                            if let Some(cpu_profile) = cpu_profiler.operator_profile(node_id) {
                                writeln!(
                                    metadata_string,
                                    "invocations: {}\ntime: {:?}",
                                    cpu_profile.invocations(),
                                    cpu_profile.total_time(),
                                )
                                .unwrap();
                            };

                            metadata_string
                        });

                        std::fs::write(
                            dataset.path().join(format!("path.{}.dot", steps)),
                            graph.to_dot(),
                        )
                        .unwrap();

                        steps += 1;
                    }

                    _ => {}
                });
            }

            let roots = circuit.add_source(Generator::new(move || root_data.take().unwrap()));
            let vertices = circuit.add_source(Generator::new(move || vertex_data.take().unwrap()));
            let edges = circuit.add_source(Generator::new(move || edge_data.take().unwrap()));

            bfs::bfs(roots, vertices, edges)
                .inspect(move |results| *output_inner.borrow_mut() = Some(results.clone()));
        })
        .unwrap();

        let elapsed = start.elapsed();
        println!("finished in {elapsed:#?}");

        print!("running dfs benchmark... ");
        std::io::stdout().flush().unwrap();
        let start = Instant::now();

        root.step().unwrap();

        let elapsed = start.elapsed();
        println!("finished in {elapsed:#?}");

        assert_eq!(output.borrow_mut().take().unwrap(), expected_output);
    })
    .join()
    .unwrap();
}

#[derive(Debug, Clone, Parser)]
#[clap(allow_external_subcommands = true)]
struct Config {
    /// Select the dataset to benchmark
    #[clap(long, value_enum, default_value = "example-directed")]
    dataset: DataSet,

    /// Whether or not to profile the dataflow
    #[clap(long)]
    profile: bool,

    // When running with `cargo bench` the binary gets the `--bench` flag, so we
    // have to parse and ignore it so clap doesn't get angry
    #[doc(hidden)]
    #[clap(long = "bench", hide = true)]
    __bench: bool,
}
