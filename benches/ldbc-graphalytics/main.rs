mod bfs;
mod data;
mod pagerank;

use crate::data::{
    list_datasets, list_downloaded_benchmarks, BfsResults, DataSet, DistanceSet, Node, NoopResults,
    PageRankResults, Rank, RankMap, Weight,
};
use clap::Parser;
use dbsp::{
    algebra::{NegByRef, Present},
    circuit::{trace::SchedulerEvent, Runtime},
    monitor::TraceMonitor,
    operator::Generator,
    profile::CPUProfiler,
    trace::{BatchReader, Cursor},
    zset, Circuit,
};
use deepsize::DeepSizeOf;
use hashbrown::HashMap;
use indicatif::HumanBytes;
use std::{
    cell::RefCell,
    fmt::Write as _,
    io::{self, Write},
    mem::size_of,
    num::{NonZeroU8, NonZeroUsize},
    ops::Add,
    rc::Rc,
    thread,
    time::Instant,
};

#[global_allocator]
#[cfg(windows)]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

enum OutputData {
    None,
    Bfs(DistanceSet<Weight>),
    PageRank(RankMap),
}

fn main() {
    let args = Args::parse();
    let config = args.config();
    let threads = config
        .threads
        .or_else(|| thread::available_parallelism().ok())
        .unwrap_or_else(|| NonZeroUsize::new(8).unwrap());
    let dataset = config.dataset;

    match args {
        Args::Bfs { .. } => println!(
            "running breadth-first search with {threads} thread{}",
            if threads.get() == 1 { "" } else { "s" },
        ),

        Args::Pagerank { .. } => {
            println!(
                "running pagerank with {threads} thread{}",
                if threads.get() == 1 { "" } else { "s" },
            );
        }

        Args::ListDatasets { .. } => {
            list_datasets();
            return;
        }

        Args::ListDownloaded { .. } => {
            list_downloaded_benchmarks();
            return;
        }
    }

    print!("loading dataset {}...", dataset.name);
    io::stdout().flush().unwrap();
    let start = Instant::now();

    let (properties, edges, vertices, _) = dataset.load::<NoopResults>().unwrap();
    let elapsed = start.elapsed();

    // Calculate the amount of data that we've loaded in, including weights
    let actual_size = edges.deep_size_of() + vertices.deep_size_of();
    let theoretical_size = {
        let node = size_of::<Node>() as u64;
        let edges = edges.len() as u64;
        let vertices = vertices.len() as u64;

        let vertex_bytes = vertices * node;
        let edge_bytes = edges * node * 2;

        vertex_bytes + edge_bytes
    };

    println!(
        "finished in {elapsed:#?}, loaded {} of data (theoretical size of {})\n\
         using dataset {} which is {} graph with {} vertices and {} edges",
        HumanBytes(actual_size as u64),
        HumanBytes(theoretical_size),
        dataset.name,
        if properties.directed {
            "a directed"
        } else {
            "an undirected"
        },
        properties.vertices,
        properties.edges,
    );

    let mut root_data = Some(zset! { properties.source_vertex => Present });
    let mut vertex_data = Some(vertices);
    let mut edge_data = Some(edges);

    Runtime::run(threads.get(), move || {
        if Runtime::worker_index() == 0 {
            print!(
                "building dataflow{}... ",
                if config.profile { " with profiling" } else { "" },
            );
            io::stdout().flush().unwrap();
        }
        let start = Instant::now();

        let output = Rc::new(RefCell::new(OutputData::None));

        let output_inner = output.clone();
        let root = Circuit::build(move |circuit| {
            if config.profile && Runtime::worker_index() == 0 {
                attach_profiling(dataset, circuit);
            }

            let roots = circuit.region("roots", || {
                circuit.add_source(Generator::new(move || if Runtime::worker_index() == 0 {
                    root_data.take().unwrap()
                } else {
                    Default::default()
                }))
            });
            let vertices = circuit.region("vertices", || {
                circuit.add_source(Generator::new(move || if Runtime::worker_index() == 0 {
                    vertex_data.take().unwrap()
                } else {
                    Default::default()
                }))
            });
            let edges = circuit.region("edges", || {
                circuit.add_source(Generator::new(move || if Runtime::worker_index() == 0 {
                    edge_data.take().unwrap()
                } else {
                    Default::default()
                }))
            });

            match args {
                Args::Bfs { .. } => {
                    bfs::bfs(roots, vertices, edges)
                        .gather(0)
                        .inspect(move |results| {
                            if Runtime::worker_index() == 0 {
                                *output_inner.borrow_mut() = OutputData::Bfs(results.clone());
                            } else {
                                assert!(results.is_empty());
                            }
                        });
                }

                Args::Pagerank {  .. } => {
                    pagerank::pagerank(
                        properties.pagerank_iters.unwrap(),
                        properties.pagerank_damping_factor.unwrap(),
                        vertices,
                        edges,
                    )
                    .gather(0)
                    .inspect(move |results| {
                        if Runtime::worker_index() == 0 {
                            *output_inner.borrow_mut() = OutputData::PageRank(results.clone());
                        } else {
                            assert!(results.is_empty());
                        }
                    });
                }

                Args::ListDatasets { .. } | Args::ListDownloaded { .. } => unreachable!(),
            }
        })
        .unwrap().0;

        if Runtime::worker_index() == 0 {
            let elapsed = start.elapsed();
            print!("finished in {elapsed:#?}\nrunning benchmark... ");
            io::stdout().flush().unwrap();
        }
        let start = Instant::now();

        root.step().unwrap();

        if Runtime::worker_index() == 0 {
            let elapsed = start.elapsed();
            println!("finished in {elapsed:#?}");

            // TODO: Generalize this some more <https://stackoverflow.com/a/64166/9885253>
            #[cfg(windows)]
            {
                use winapi::um::{
                    processthreadsapi::GetCurrentProcess,
                    psapi::{GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS},
                };
                use std::mem::MaybeUninit;

                let mut info = MaybeUninit::<PROCESS_MEMORY_COUNTERS>::uninit();
                let status = unsafe {
                    GetProcessMemoryInfo(
                        GetCurrentProcess(),
                        info.as_mut_ptr(),
                        size_of::<PROCESS_MEMORY_COUNTERS>() as _,
                    )
                };

                if status != 0 {
                    let info = unsafe { info.assume_init() };
                    println!(
                        "peak memory usage: {}",
                        HumanBytes(info.PeakWorkingSetSize as u64),
                    );
                }
            }

            // Metrics calculations from https://arxiv.org/pdf/2011.15028v4.pdf#subsection.2.5.3
            let eps = properties.edges as f64 / elapsed.as_secs_f64();
            let keps = eps / 1000.0;
            println!("achieved {keps:.02} kEPS ({eps:.02} EPS)");

            let elements = properties.edges + properties.vertices;
            let evps = elements as f64 / elapsed.as_secs_f64();
            let kevps = evps / 1000.0;
            println!("achieved {kevps:.02} kEVPS ({evps:.02} EVPS)");

            let output = output.borrow();
            match &*output {
                OutputData::None => println!("no output was produced"),

                OutputData::Bfs(result) => {
                    let expected = dataset.load_results::<BfsResults>(&properties).unwrap();
                    // TODO: Better diff function
                    assert_eq!(result, &expected);
                }

                OutputData::PageRank(result) => {
                    let expected = dataset.load_results::<PageRankResults>(&properties).unwrap();
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
                                    if (first - second).abs() > Rank::EPSILON {
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

    let profile_path = dataset.path().join("profile");
    let _ = std::fs::remove_dir_all(&profile_path);
    std::fs::create_dir_all(&profile_path).expect("failed to create directory for profile");

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
                profile_path.join(format!("path.{}.dot", steps)),
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
    },

    /// List all available datasets
    ListDatasets {
        #[clap(flatten)]
        config: Config,
    },

    /// List the sizes of all downloaded benchmark datasets
    ListDownloaded {
        #[clap(flatten)]
        config: Config,
    },
}

impl Args {
    pub(crate) fn config(self) -> Config {
        match self {
            Self::Bfs { config }
            | Self::Pagerank { config }
            | Self::ListDownloaded { config }
            | Self::ListDatasets { config } => config,
        }
    }
}

#[derive(Debug, Clone, Copy, Parser)]
struct Config {
    /// Select the dataset to benchmark
    #[clap(value_enum, default_value = "example-directed")]
    dataset: DataSet,

    /// Whether or not to profile the dataflow
    #[clap(long)]
    profile: bool,

    /// The number of threads to use for the dataflow, defaults to the
    /// number of cores the current machine has
    #[clap(long)]
    threads: Option<NonZeroUsize>,

    /// The number of iterations to run
    #[clap(long, default_value = "5")]
    iters: NonZeroU8,

    // When running with `cargo bench` the binary gets the `--bench` flag, so we
    // have to parse and ignore it so clap doesn't get angry
    #[doc(hidden)]
    #[clap(long = "bench", hide = true)]
    __bench: bool,
}
