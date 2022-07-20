mod bfs;
mod data;
mod pagerank;

use crate::{data::DataSet, pagerank::PageRankKind};
use clap::{Parser, ValueEnum};
use dbsp::{
    circuit::{trace::SchedulerEvent, Root, Runtime},
    monitor::TraceMonitor,
    operator::Generator,
    profile::CPUProfiler,
    trace::{BatchReader, Cursor},
    zset_set, Circuit,
};
use hashbrown::HashMap;
use std::{cell::RefCell, fmt::Write as _, io::Write, rc::Rc, time::Instant};

fn main() {
    let config = Config::parse();
    let dataset = config.dataset;

    print!("loading dataset {}...", dataset.name);
    std::io::stdout().flush().unwrap();
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
                attach_profiling(dataset, circuit);
            }

            let roots = circuit.add_source(Generator::new(move || root_data.take().unwrap()));
            let vertices = circuit.add_source(Generator::new(move || vertex_data.take().unwrap()));
            let edges = circuit.add_source(Generator::new(move || edge_data.take().unwrap()));

            match config.benchmark {
                Benchmark::Bfs => {
                    bfs::bfs(roots, vertices, edges)
                        .inspect(move |results| *output_inner.borrow_mut() = Some(results.clone()));
                }

                Benchmark::Pagerank => {
                    pagerank::pagerank(
                        properties.pagerank_iters.unwrap(),
                        properties.pagerank_damping_factor.unwrap(),
                        properties.directed,
                        vertices,
                        edges,
                        PageRankKind::Scoped,
                    )
                    .inspect(|ranks| {
                        let mut stdout = std::io::stdout().lock();

                        let mut cursor = ranks.cursor();
                        let mut sum = 0.0;
                        while cursor.key_valid() {
                            let key = *cursor.key();
                            while cursor.val_valid() {
                                let value = *cursor.val();
                                sum += value.inner();

                                let weight = cursor.weight();
                                writeln!(stdout, "output: {key}, {value} ({weight:+})").unwrap();
                                cursor.step_val();
                            }
                            cursor.step_key();
                        }

                        writeln!(stdout, "sum of all ranks: {sum}").unwrap();
                        stdout.flush().unwrap();
                    });
                }
            }
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

        // TODO: Is it correct to multiply edges by two for undirected graphs?
        let total_edges = properties.edges * (!properties.directed as u64 + 1);

        // Metrics calculation from https://arxiv.org/pdf/2011.15028v4.pdf#subsection.2.5.3
        let eps = total_edges as f64 / elapsed.as_secs_f64();
        let keps = eps / 1000.0;
        println!("achieved {keps:.02} kEPS ({eps:.02} EPS)");

        let elements = total_edges * properties.vertices;
        let evps = elements as f64 / elapsed.as_secs_f64();
        let kevps = evps / 1000.0;
        println!("achieved {kevps:.02} kEVPS ({evps:.02} EVPS)");

        if config.benchmark == Benchmark::Bfs {
            assert_eq!(output.borrow_mut().take().unwrap(), expected_output);
        }
    })
    .join()
    .unwrap();
}

fn attach_profiling(dataset: DataSet, circuit: &mut Circuit<()>) {
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
                    .cloned()
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

#[derive(Debug, Clone, Parser)]
struct Config {
    /// The benchmark to run
    #[clap(long, value_enum, default_value = "bfs")]
    benchmark: Benchmark,

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

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
enum Benchmark {
    Bfs,
    Pagerank,
}
