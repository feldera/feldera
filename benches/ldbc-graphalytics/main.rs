mod bfs;
mod data;
mod pagerank;

use crate::{
    data::{BfsResults, DataSet, DistanceSet, NoopResults, PageRankResults, RankMap},
    pagerank::PageRankKind,
};
use clap::Parser;
use dbsp::{
    algebra::NegByRef,
    circuit::{trace::SchedulerEvent, Root, Runtime},
    monitor::TraceMonitor,
    operator::Generator,
    profile::CPUProfiler,
    trace::{BatchReader, Cursor},
    zset_set, Circuit,
};
use hashbrown::HashMap;
use std::{
    cell::RefCell,
    fmt::Write as _,
    io::{self, Write},
    ops::Add,
    rc::Rc,
    time::Instant,
};

enum OutputData {
    None,
    Bfs(DistanceSet),
    PageRank(RankMap),
}

fn main() {
    let args = Args::parse();
    let config = match args {
        Args::Bfs { config } => config,
        Args::Pagerank { config, .. } => config,
    };
    let dataset = config.dataset;

    match args {
        Args::Bfs { .. } => println!("running breadth-first search"),
        Args::Pagerank { flavor, .. } => println!("running pagerank ({flavor})"),
    }

    print!("loading dataset {}...", dataset.name);
    io::stdout().flush().unwrap();
    let start = Instant::now();

    let (properties, edges, vertices, _) = dataset.load::<NoopResults>().unwrap();
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
        io::stdout().flush().unwrap();
        let start = Instant::now();

        let output = Rc::new(RefCell::new(OutputData::None));

        let output_inner = output.clone();
        let root = Root::build(move |circuit| {
            if config.profile {
                attach_profiling(dataset, circuit);
            }

            let roots = circuit.add_source(Generator::new(move || root_data.take().unwrap()));
            let vertices = circuit.add_source(Generator::new(move || vertex_data.take().unwrap()));
            let edges = circuit.add_source(Generator::new(move || edge_data.take().unwrap()));

            match args {
                Args::Bfs { .. } => {
                    bfs::bfs(roots, vertices, edges).inspect(move |results| {
                        *output_inner.borrow_mut() = OutputData::Bfs(results.clone());
                    });
                }

                Args::Pagerank { flavor, .. } => {
                    pagerank::pagerank(
                        properties.pagerank_iters.unwrap(),
                        properties.pagerank_damping_factor.unwrap(),
                        properties.directed,
                        vertices,
                        edges,
                        flavor,
                    )
                    .inspect(move |results| {
                        *output_inner.borrow_mut() = OutputData::PageRank(results.clone());
                    });
                }
            }
        })
        .unwrap();

        let elapsed = start.elapsed();
        println!("finished in {elapsed:#?}");

        print!("running benchmark... ");
        io::stdout().flush().unwrap();
        let start = Instant::now();

        root.step().unwrap();

        let elapsed = start.elapsed();
        println!("finished in {elapsed:#?}");

        // TODO: Is it correct to multiply edges by two for undirected graphs?
        let total_edges = properties.edges * (!properties.directed as u64 + 1);

        // Metrics calculations from https://arxiv.org/pdf/2011.15028v4.pdf#subsection.2.5.3
        let eps = total_edges as f64 / elapsed.as_secs_f64();
        let keps = eps / 1000.0;
        println!("achieved {keps:.02} kEPS ({eps:.02} EPS)");

        let elements = total_edges + properties.vertices;
        let evps = elements as f64 / elapsed.as_secs_f64();
        let kevps = evps / 1000.0;
        println!("achieved {kevps:.02} kEVPS ({evps:.02} EVPS)");

        let output = output.borrow();
        match &*output {
            OutputData::None => println!("no output was produced"),

            OutputData::Bfs(result) => {
                let expected = dataset.load_results::<BfsResults>().unwrap();
                // TODO: Better diff function
                assert_eq!(result, &expected);
            }

            OutputData::PageRank(result) => {
                let expected = dataset.load_results::<PageRankResults>().unwrap();
                let difference = expected.add(result.neg_by_ref());

                let mut incorrect = 0;
                if !difference.is_empty() {
                    let mut stdout = io::stdout().lock();
                    let mut cursor = difference.cursor();

                    while cursor.key_valid() {
                        let key = *cursor.key();

                        if let Some(first) = cursor.get_val().copied() {
                            let first_weight = cursor.weight();
                            cursor.step_val();

                            if let Some(second) = cursor.get_val().copied() {
                                let second_weight = cursor.weight();
                                cursor.step_val();

                                // Sometimes the values are only different because of floating point
                                // inaccuracies, e.g. 0.14776291666666666 vs. 0.1477629166666667
                                if (first.inner() - second.inner()).abs() > f64::EPSILON {
                                    let (first, first_weight, second, second_weight) = if first_weight.is_positive()
                                        && second_weight.is_negative()
                                    {
                                        (second, second_weight, first, first_weight)
                                    } else {
                                        (first, first_weight, second, second_weight)
                                    };

                                    writeln!(stdout, "{key}, expected {second} and got {first}  ({first_weight:+}, {second_weight:+})")
                                        .unwrap();
                                    incorrect += 1;
                                }
                            } else if first_weight.is_positive() {
                                writeln!(stdout, "{key}, missing: {first} ({first_weight:+})")
                                    .unwrap();
                            } else {
                                writeln!(stdout, "{key}, unexpected: {first} ({first_weight:+})")
                                    .unwrap();
                            }
                        }

                        cursor.step_key();
                    }

                    stdout.flush().unwrap();
                }

                println!(
                    "pagerank had {incorrect} incorrect result{}",
                    if incorrect == 1 { "" } else { "s" },
                );
            }
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
                let mut metadata_string = metadata.get(node_id).cloned().unwrap_or_default();

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

#[derive(Debug, Clone, Copy, Parser)]
enum Args {
    /// Run the breadth-first search benchmark
    Bfs {
        #[clap(flatten)]
        config: Config,
    },

    /// Run the pagerank benchmark
    Pagerank {
        #[clap(flatten)]
        config: Config,

        #[clap(long, value_enum, default_value = "scoped")]
        flavor: PageRankKind,
    },
}

#[derive(Debug, Clone, Copy, Parser)]
struct Config {
    /// Select the dataset to benchmark
    #[clap(value_enum, default_value = "example-directed")]
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
