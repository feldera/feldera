// TODO: The main roadblock I ran into while porting this benchmark to dynamic dispatch is that
// we no longer support stream.distinct() with floating point weights.
fn main() {
    todo!()
}
/*
mod bfs;

mod data;
mod pagerank;

use crate::data::{
    list_datasets, list_downloaded_benchmarks, BfsResults, DataSet, DistanceSet, Node, NoopResults,
    PageRankResults, Rank, RankMap,
};
use clap::Parser;
use dbsp::{
    circuit::{
        metadata::{MetaItem, OperatorMeta},
        trace::SchedulerEvent,
        Runtime,
    },
    mimalloc::{AllocStats, MiMalloc},
    monitor::TraceMonitor,
    operator::Generator,
    profile::CPUProfiler,
    trace::{BatchReader, Cursor},
    utils::Tup2,
    Circuit, RootCircuit,
};
use hashbrown::HashMap;
use indicatif::HumanBytes;
use std::{
    borrow::Cow,
    cell::RefCell,
    fmt::Write as _,
    fs::OpenOptions,
    io::{self, Write},
    mem::{replace, size_of, take},
    num::{NonZeroU8, NonZeroUsize},
    ops::{Add, Neg},
    path::Path,
    rc::Rc,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread,
    time::{Duration, Instant},
};

#[global_allocator]
static ALLOC: MiMalloc = MiMalloc;

enum OutputData {
    None,
    Bfs(DistanceSet),
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

    match &args {
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

    print!("loading dataset {}... ", dataset.name);
    io::stdout().flush().unwrap();
    let start = Instant::now();

    let (properties, edge_data, vertex_data, _) =
        dataset.load::<NoopResults>(threads.get()).unwrap();
    let elapsed = start.elapsed();

    // Calculate the amount of data that we've loaded in, including weights
    let actual_size = size_of::size_of_values([&edge_data as _, &vertex_data as _]);
    let theoretical_size = {
        let node = size_of::<Node>() as u64;
        let edges = edge_data
            .iter()
            .map(|edges| edges.len() as u64)
            .sum::<u64>();
        let vertices = vertex_data
            .iter()
            .map(|vertices| vertices.len() as u64)
            .sum::<u64>();

        let vertex_bytes = vertices * node;
        let edge_bytes = edges * node * 2;

        vertex_bytes + edge_bytes
    };

    println!(
        "finished in {elapsed:#?}, loaded {} of data (allocated {}, theoretical size of {})\n\
         using dataset {} which is {} graph with {} vertices and {} edges",
        HumanBytes(actual_size.used_bytes() as u64),
        HumanBytes(actual_size.total_bytes() as u64),
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

    let edge_data = Arc::new(edge_data.into_iter().map(Mutex::new).collect::<Vec<_>>());
    let vertex_data = Arc::new(vertex_data.into_iter().map(Mutex::new).collect::<Vec<_>>());
    let incorrect_results_main = Arc::new(AtomicBool::new(false));
    let incorrect_results = incorrect_results_main.clone();

    Runtime::run(threads.get(), move || {
        let worker_idx = Runtime::worker_index();
        let is_leader = worker_idx == 0;

        let init_stats = if is_leader {
            print!(
                "building dataflow{}... ",
                if config.profile { " with profiling" } else { "" },
            );
            io::stdout().flush().unwrap();
            ALLOC.reset_stats();
            ALLOC.stats()
        } else {
            AllocStats::default()
        };
        let start = Instant::now();

        let output = Rc::new(RefCell::new(OutputData::None));

        let output_inner = output.clone();
        let args_for_circuit = args.clone();
        let root = RootCircuit::build(move |circuit| {
            if config.profile && is_leader {
                attach_profiling(dataset, circuit);
            }

            let roots = circuit.region("roots", || {
                let (roots, root_handle) = circuit.dyn_add_input_set();
                root_handle.dyn_push(properties.source_vertex, true);
                roots
            });

            let vertices = circuit.region("vertices", || {
                circuit.add_source(Generator::new(move || take(&mut *vertex_data[worker_idx].lock().unwrap())))
            })
            .mark_sharded();

            let edges = circuit.region("edges", || {
                circuit.add_source(Generator::new(move || take(&mut *edge_data[worker_idx].lock().unwrap())))
            })
            .mark_sharded();

            match args_for_circuit {
                Args::Bfs { .. } => {
                    bfs::bfs(roots, vertices, edges)
                        .gather(0)
                        .inspect(move |results| {
                            if is_leader {
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
                        if is_leader {
                            *output_inner.borrow_mut() = OutputData::PageRank(results.clone());
                        } else {
                            assert!(results.is_empty());
                        }
                    });
                }

                Args::ListDatasets { .. } | Args::ListDownloaded { .. } => unreachable!(),
            }
            Ok(())
        })
        .unwrap().0;

        if is_leader {
            let elapsed = start.elapsed();
            print!("finished in {elapsed:#?}\nrunning benchmark... ");
            io::stdout().flush().unwrap();
        }

        let start = Instant::now();
        root.step().unwrap();
        let elapsed = start.elapsed();

        if is_leader {
            let stats = ALLOC.stats();
            println!(
                "finished in {elapsed:#?}\ntime: {:#?}, user: {:#?}, system: {:#?}\nrss: {}, peak: {}\ncommit: {}, peak: {}\npage faults: {}",
                Duration::from_millis((stats.elapsed_ms - init_stats.elapsed_ms) as u64),
                Duration::from_millis((stats.user_ms - init_stats.user_ms) as u64),
                Duration::from_millis((stats.system_ms - init_stats.system_ms) as u64),
                HumanBytes(stats.current_rss as u64),
                HumanBytes(stats.peak_rss as u64),
                HumanBytes(stats.current_commit as u64),
                HumanBytes(stats.peak_commit as u64),
                stats.page_faults - init_stats.page_faults,
            );

            // Metrics calculations from https://arxiv.org/pdf/2011.15028v4.pdf#subsection.2.5.3
            let eps = properties.edges as f64 / elapsed.as_secs_f64();
            let keps = eps / 1000.0;
            println!("achieved {keps:.02} kEPS ({eps:.02} EPS)");

            let elements = properties.edges + properties.vertices;
            let evps = elements as f64 / elapsed.as_secs_f64();
            let kevps = evps / 1000.0;
            println!("achieved {kevps:.02} kEVPS ({evps:.02} EVPS)");

            if let Some(csv_file) = config.output_csv {
                let results_file_already_exists = Path::new(&csv_file).is_file();
                // If the file exists, we append another row, this is different
                // from other benchmarks that can run everything with a single
                // invocation:
                let file = OpenOptions::new()
                    .write(true)
                    .append(results_file_already_exists)
                    .create(!results_file_already_exists)
                    .open(&csv_file)
                    .expect("failed to open results csv file for writing");
                let mut csv_writer = csv::Writer::from_writer(file);

                if !results_file_already_exists {
                    // Write a header row if the file is newly created
                    csv_writer.write_record([
                        "name",
                        "algorithm",
                        "dataset",
                        "threads",
                        "elapsed",
                        "elements",
                        "evps",
                        "allocstats_before_elapsed_ms",
                        "allocstats_before_user_ms",
                        "allocstats_before_system_ms",
                        "allocstats_before_current_rss",
                        "allocstats_before_peak_rss",
                        "allocstats_before_current_commit",
                        "allocstats_before_peak_commit",
                        "allocstats_before_page_faults",
                        "allocstats_after_elapsed_ms",
                        "allocstats_after_user_ms",
                        "allocstats_after_system_ms",
                        "allocstats_after_current_rss",
                        "allocstats_after_peak_rss",
                        "allocstats_after_current_commit",
                        "allocstats_after_peak_commit",
                        "allocstats_after_page_faults"
                    ])
                    .expect("failed to write csv header");
                }
                csv_writer.write_record([
                    "ldbc",
                    args.algorithm(),
                    config.dataset.name,
                    threads.get().to_string().as_str(),
                    elapsed.as_secs_f64().to_string().as_str(),
                    elements.to_string().as_str(),
                    evps.to_string().as_str(),
                    init_stats.elapsed_ms.to_string().as_str(),
                    init_stats.user_ms.to_string().as_str(),
                    init_stats.system_ms.to_string().as_str(),
                    init_stats.current_rss.to_string().as_str(),
                    init_stats.peak_rss.to_string().as_str(),
                    init_stats.current_commit.to_string().as_str(),
                    init_stats.peak_commit.to_string().as_str(),
                    init_stats.page_faults.to_string().as_str(),
                    stats.elapsed_ms.to_string().as_str(),
                    stats.user_ms.to_string().as_str(),
                    stats.system_ms.to_string().as_str(),
                    stats.current_rss.to_string().as_str(),
                    stats.peak_rss.to_string().as_str(),
                    stats.current_commit.to_string().as_str(),
                    stats.peak_commit.to_string().as_str(),
                    stats.page_faults.to_string().as_str(),
                ])
                .expect("failed to write csv record");
            }

            const MAX_PRINT_COUNT: usize = 10;
            let output = replace(&mut *output.borrow_mut(), OutputData::None);
            match output {
                OutputData::None => println!("no output was produced"),

                OutputData::Bfs(result) => {
                    let expected = dataset.load_results::<BfsResults>(&properties).unwrap();
                    let difference = expected.add(result.neg());

                    if !difference.is_empty() {
                        let mut stdout = io::stdout().lock();
                        let mut cursor = difference.cursor();
                        let mut print_count = MAX_PRINT_COUNT;

                        while cursor.key_valid() && print_count > 0 {
                            let Tup2(node, distance) = *cursor.key();
                            let weight = cursor.weight();
                            writeln!(stdout, "{node}, {distance} ({weight:+})").unwrap();
                            cursor.step_key();
                            print_count -= 1;
                        }
                        if print_count == 0 && difference.len() - MAX_PRINT_COUNT > 0{
                            writeln!(stdout, "[stopped printing remaining {} incorrect results]", difference.len() - MAX_PRINT_COUNT).unwrap();
                        }
                    }

                    println!(
                        "bfs had {} incorrect result{}",
                        difference.len(),
                        if difference.len() == 1 { "" } else { "s" },
                    );

                    incorrect_results.store(!difference.is_empty(), Ordering::Relaxed);
                }

                OutputData::PageRank(result) => {
                    let expected = dataset.load_results::<PageRankResults>(&properties).unwrap();
                    let difference = expected.add(result.neg());

                    let mut incorrect = 0;
                    let mut print_count = MAX_PRINT_COUNT;
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
                                        incorrect += 1;

                                        if print_count > 0 {
                                            writeln!(stdout, "{key}, expected {second} and got {first}  ({first_weight:+}, {second_weight:+})")
                                                .unwrap();
                                            print_count -= 1;
                                        }
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
                        if incorrect > MAX_PRINT_COUNT {
                            writeln!(stdout, "[stopped printing remaining {} incorrect results]", difference.len() - MAX_PRINT_COUNT).unwrap();
                        }
                        stdout.flush().unwrap();
                    }
                    println!(
                        "pagerank had {incorrect} incorrect result{}",
                        if incorrect == 1 { "" } else { "s" },
                    );
                    incorrect_results.store(incorrect > 0, Ordering::Relaxed);
                }
            }
        }
    })
    .join()
    .unwrap();

    std::process::exit(incorrect_results_main.load(Ordering::Relaxed) as i32);
}

fn attach_profiling(dataset: DataSet, circuit: &mut RootCircuit) {
    let cpu_profiler = CPUProfiler::new();
    cpu_profiler.attach(circuit, "cpu profiler");

    let monitor = TraceMonitor::new_panic_on_error();
    monitor.attach(circuit, "monitor");

    let profile_path = dataset.path().join("profile");
    let _ = std::fs::remove_dir_all(&profile_path);
    std::fs::create_dir_all(&profile_path).expect("failed to create directory for profile");

    let mut metadata = HashMap::<_, OperatorMeta>::default();
    let mut steps = 0;

    circuit.register_scheduler_event_handler("metadata", move |event| match event {
        SchedulerEvent::EvalEnd { node } => {
            let meta = metadata.entry(node.global_id().clone()).or_default();
            meta.clear();
            node.metadata(meta);
        }

        SchedulerEvent::StepEnd { .. } => {
            let graph = monitor.visualize_circuit_annotate(|node_id| {
                let mut output = String::with_capacity(1024);
                let mut meta = metadata.get(node_id).cloned().unwrap_or_default();

                if let Some(profile) = cpu_profiler.operator_profile(node_id) {
                    let default_meta = [
                        (
                            Cow::Borrowed("invocations"),
                            MetaItem::Int(profile.invocations()),
                        ),
                        (
                            Cow::Borrowed("time"),
                            MetaItem::Duration(profile.total_time()),
                        ),
                        (
                            Cow::Borrowed("nodeid"),
                            MetaItem::String(node_id.to_string()),
                        ),
                    ];

                    for item in default_meta {
                        meta.insert(0, item);
                    }
                }

                for (label, item) in meta.iter() {
                    write!(output, "{label}: ").unwrap();
                    item.format(&mut output).unwrap();
                    output.push_str("\\l");
                }

                output
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

#[derive(Debug, Clone, Parser)]
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
    pub(crate) fn config(&self) -> Config {
        match self {
            Self::Bfs { config }
            | Self::Pagerank { config }
            | Self::ListDownloaded { config }
            | Self::ListDatasets { config } => config,
        }
        .clone()
    }

    fn algorithm(&self) -> &str {
        match Self::parse() {
            Self::Bfs { .. } => "bfs",
            Self::Pagerank { .. } => "pagerank",
            _ => unreachable!(),
        }
    }
}

#[derive(Debug, Clone, Parser)]
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

    /// Store results in a csv file in addition to printing on the command-line.
    #[clap(long = "csv", env = "DBSP_RESULTS_AS_CSV")]
    output_csv: Option<String>,

    // When running with `cargo bench` the binary gets the `--bench` flag, so we
    // have to parse and ignore it so clap doesn't get angry
    #[doc(hidden)]
    #[clap(long = "bench", hide = true)]
    __bench: bool,
}
*/
