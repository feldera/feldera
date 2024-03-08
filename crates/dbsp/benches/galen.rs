//! Galen benchmark from
//! `https://github.com/frankmcsherry/dynamic-datalog/tree/master/problems/galen`

use anyhow::{Context, Result};
use clap::Parser;
use csv::ReaderBuilder;
use dbsp::{
    mimalloc::MiMalloc,
    monitor::TraceMonitor,
    operator::CsvSource,
    utils::{Tup2, Tup3},
    Circuit, OrdZSet, RootCircuit, Runtime, Stream,
};
use std::{
    fs::{self, File, OpenOptions},
    io::BufReader,
    iter::once,
    path::Path,
    time::Instant,
};
use zip::ZipArchive;

#[global_allocator]
static ALLOC: MiMalloc = MiMalloc;

/*
.decl p(X: Number, Z: Number)
.decl q(X: Number, Y: Number, Z: Number)
.decl r(R: Number, P: Number, E: Number)
.decl c(Y: Number, Z: Number, W: Number)
.decl u(R: Number, Z: Number, W: Number)
.decl s(R: Number, P: Number)

.input p(IO="file", filename="p.txt", delimiter=",")
.input q(IO="file", filename="q.txt", delimiter=",")
.input r(IO="file", filename="r.txt", delimiter=",")
.input c(IO="file", filename="c.txt", delimiter=",")
.input u(IO="file", filename="u.txt", delimiter=",")
.input s(IO="file", filename="s.txt", delimiter=",")

p(?x,?z) :- p(?x,?y), p(?y,?z).
p(?x,?z) :- p(?y,?w), u(?w,?r,?z), q(?x,?r,?y).
p(?x,?z) :- c(?y,?w,?z),p(?x,?w), p(?x,?y).
q(?x,?r,?z) :- p(?x,?y), q(?y,?r,?z).
q(?x,?q,?z) :- q(?x,?r,?z),s(?r,?q).
q(?x,?e,?o) :- q(?x,?y,?z),r(?y,?u,?e),q(?z,?u,?o).
*/

type Number = u32;

const GALEN_DATA: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/benches/galen_data");
const GALEN_ARCHIVE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/benches/galen_data.zip");
const GALEN_GRAPH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/benches/galen_data/galen.dot");

fn csv_source<T>(file: &str) -> CsvSource<BufReader<File>, T>
where
    T: Clone + Ord,
{
    let path = Path::new(GALEN_DATA).join(file);
    let file = BufReader::new(File::open(&path).unwrap_or_else(|error| {
        panic!(
            "failed to open benchmark file '{}': {}",
            path.display(),
            error,
        )
    }));

    let reader = ReaderBuilder::new()
        .delimiter(b',')
        .has_headers(false)
        .from_reader(file);
    CsvSource::from_csv_reader(reader)
}

#[derive(Debug, Clone, Parser)]
struct Args {
    #[clap(long)]
    workers: usize,

    /// Store results in a csv file in addition to printing on the command-line.
    #[clap(long = "csv", env = "DBSP_RESULTS_AS_CSV")]
    output_csv: Option<String>,

    #[doc(hidden)]
    #[clap(long = "bench", hide = true)]
    __bench: bool,
}

// Disable the benchmark while under miri
#[cfg(not(all(windows, miri)))]
fn main() -> Result<()> {
    let args = Args::parse();
    println!("Running galen benchmark with {} workers", args.workers);

    unpack_galen_data()?;

    let hruntime = Runtime::run(args.workers, move || {
        let monitor = TraceMonitor::new_panic_on_error();
        let circuit = RootCircuit::build(|circuit| {
            /*
            use dbsp::{circuit::trace::SchedulerEvent, profile::CPUProfiler};
            use std::{collections::HashMap, fmt::Write};

            let cpu_profiler = CPUProfiler::new();
            cpu_profiler.attach(circuit, "cpu profiler");

            monitor.attach(circuit, "monitor");

            let mut metadata = HashMap::<_, String>::new();
            let mut steps = 0;
            let monitor_clone = monitor.clone();

            circuit.register_scheduler_event_handler("metadata", move |event| match event {
                SchedulerEvent::EvalEnd { node } => {
                    let metadata_string = metadata.entry(node.global_id().clone()).or_default();
                    metadata_string.clear();
                    node.summary(metadata_string);
                }

                SchedulerEvent::StepEnd => {
                    let graph = monitor_clone.visualize_circuit_annotate(|node_id| {
                        let mut metadata_string =
                            metadata.get(node_id).cloned().unwrap_or_default();

                        if let Some(cpu_profile) = cpu_profiler.operator_profile(node_id) {
                            writeln!(
                                metadata_string,
                                "invocations: {}\ntime: {:#?}",
                                cpu_profile.invocations(),
                                cpu_profile.total_time(),
                            )
                            .unwrap();
                        };

                        metadata_string
                    });

                    std::fs::write(format!("galen.{}.dot", steps), graph.to_dot()).unwrap();
                    steps += 1;
                }

                _ => {}
            });
            */

            let p_source = csv_source::<Tup2<Number, Number>>("p.txt");
            let q_source = csv_source::<Tup3<Number, Number, Number>>("q.txt");
            let r_source = csv_source::<Tup3<Number, Number, Number>>("r.txt");
            let c_source = csv_source::<Tup3<Number, Number, Number>>("c.txt");
            let u_source = csv_source::<Tup3<Number, Number, Number>>("u.txt");
            let s_source = csv_source::<Tup2<Number, Number>>("s.txt");

            let p: Stream<_, OrdZSet<_>> = circuit.region("p", || circuit.add_source(p_source));
            let q: Stream<_, OrdZSet<_>> = circuit.region("q", || circuit.add_source(q_source));
            let r: Stream<_, OrdZSet<_>> = circuit.region("r", || circuit.add_source(r_source));
            let c: Stream<_, OrdZSet<_>> = circuit.region("c", || circuit.add_source(c_source));
            let u: Stream<_, OrdZSet<_>> = circuit.region("u", || circuit.add_source(u_source));
            let s: Stream<_, OrdZSet<_>> = circuit.region("s", || circuit.add_source(s_source));

            type Pair = OrdZSet<Tup2<Number, Number>>;
            type Triple = OrdZSet<Tup3<Number, Number, Number>>;

            let (outp, outq) = circuit
                .recursive(
                    |child, (pvar, qvar): (Stream<_, Pair>, Stream<_, Triple>)| {
                        let p_by_1 = pvar.map_index(|Tup2(x, y)| (x.clone(), y.clone()));
                        let p_by_2 = pvar.map_index(|&Tup2(x, y)| (y, x));
                        let p_by_12 = pvar.map_index(|&Tup2(x, y)| (Tup2(x, y), ()));
                        let u_by_1 = u.delta0(child).map_index(|&Tup3(x, y, z)| (x, Tup2(y, z)));
                        let q_by_1 = qvar.map_index(|&Tup3(x, y, z)| (x, Tup2(y, z)));
                        let q_by_2 = qvar.map_index(|&Tup3(x, y, z)| (y, Tup2(x, z)));
                        let q_by_12 = qvar.map_index(|&Tup3(x, y, z)| (Tup2(x, y), z));
                        let q_by_23 = qvar.map_index(|&Tup3(x, y, z)| (Tup2(y, z), x));
                        let c_by_2 = c.delta0(child).map_index(|&Tup3(x, y, z)| (y, Tup2(x, z)));
                        let r_by_1 = r.delta0(child).map_index(|&Tup3(x, y, z)| (x, Tup2(y, z)));
                        let s_by_1 = s
                            .delta0(child)
                            .map_index(|Tup2(x, y)| (x.clone(), y.clone()));

                        // IR1: p(x,z) :- p(x,y), p(y,z).
                        let ir1 =
                            child.region("IR1", || p_by_2.join(&p_by_1, |&_y, &x, &z| Tup2(x, z)));
                        /*ir1.inspect(|zs: &OrdZSet<_, _>| {
                            println!("{}: ir1: {}", Runtime::worker_index(), zs.len())
                        });*/

                        // IR2: q(x,r,z) := p(x,y), q(y,r,z)
                        let ir2 = child.region("IR2", || {
                            p_by_2.join(&q_by_1, |&_y, &x, &Tup2(r, z)| Tup3(x, r, z))
                        });

                        /*ir2.inspect(|zs: &OrdZSet<_, _>| {
                            println!("{}: ir2: {}", Runtime::worker_index(), zs.len())
                        });*/

                        // IR3: p(x,z) := p(y,w), u(w,r,z), q(x,r,y)
                        let ir3 = child.region("IR3", || {
                            p_by_2
                                .join_index(&u_by_1, |&_w, &y, &Tup2(r, z)| once((Tup2(r, y), z)))
                                .join(&q_by_23, |&Tup2(_r, _y), &z, &x| Tup2(x, z))
                        });
                        /*ir3.inspect(|zs: &OrdZSet<_, _>| {
                            println!("{}: ir3: {}", Runtime::worker_index(), zs.len())
                        });*/

                        // IR4: p(x,z) := c(y,w,z), p(x,w), p(x,y)
                        let ir4_1 = child.region("IR4-1", || {
                            c_by_2.join_index(&p_by_2, |&_w, &Tup2(y, z), &x| once((Tup2(x, y), z)))
                        });
                        /*ir4_1.inspect(|zs: &OrdIndexedZSet<_, _, _>| {
                            println!("{}: ir4_1: {}", Runtime::worker_index(), zs.len())
                        });*/

                        let ir4 = child.region("IR4-2", || {
                            ir4_1.join(&p_by_12, |&Tup2(x, _y), &z, &()| Tup2(x, z))
                        });
                        /*ir4.inspect(|zs: &OrdZSet<_, _>| {
                            println!("{}: ir4: {}", Runtime::worker_index(), zs.len())
                        });*/

                        // IR5: q(x,q,z) := q(x,r,z), s(r,q)
                        let ir5 = child.region("IR5", || {
                            q_by_2.join(&s_by_1, |&_r, &Tup2(x, z), &q| Tup3(x, q, z))
                        });
                        /*ir5.inspect(|zs: &OrdZSet<_, _>| {
                            println!("{}: ir5: {}", Runtime::worker_index(), zs.len())
                        });*/

                        // IR6: q(x,e,o) := q(x,y,z), r(y,u,e), q(z,u,o)
                        let ir6_1 = child.region("IR6_1", || {
                            q_by_2.join_index(&r_by_1, |&_y, &Tup2(x, z), &Tup2(u, e)| {
                                once((Tup2(z, u), Tup2(x, e)))
                            })
                        });
                        let ir6 = child.region("IR6", || {
                            ir6_1.join(&q_by_12, |&Tup2(_z, _u), &Tup2(x, e), &o| Tup3(x, e, o))
                        });

                        /*ir6.inspect(|zs: &OrdZSet<_, _>| {
                            println!("{}: ir6: {}", Runtime::worker_index(), zs.len())
                        });*/

                        let p = p.delta0(child).sum([&ir1, &ir3, &ir4]);
                        let q = q.delta0(child).sum([&ir2, &ir5, &ir6]);

                        Ok((p, q))
                    },
                )
                .unwrap();
            outp.gather(0).inspect(|zs: &OrdZSet<_>| {
                if Runtime::worker_index() == 0 {
                    assert_eq!(zs.len(), 7560179);
                }
            });
            outq.gather(0).inspect(|zs: &OrdZSet<_>| {
                if Runtime::worker_index() == 0 {
                    assert_eq!(zs.len(), 16595494);
                }
            });
            Ok(())
        })
        .unwrap()
        .0;

        let graph = monitor.visualize_circuit();
        fs::write(GALEN_GRAPH, graph.to_dot()).unwrap();

        let start = Instant::now();
        circuit.step().unwrap();

        if Runtime::worker_index() == 0 {
            let elapsed = start.elapsed();
            println!("finished in {:#?}", elapsed);
            if let Some(csv_file) = args.output_csv {
                let results_file_already_exists = Path::new(&csv_file).is_file();
                let file = OpenOptions::new()
                    .write(true)
                    .append(results_file_already_exists)
                    .create(!results_file_already_exists)
                    .open(&csv_file)
                    .expect("failed to open results csv file for writing");
                let mut csv_writer = csv::WriterBuilder::new().from_writer(file);
                if !results_file_already_exists {
                    csv_writer
                        .write_record(["name", "workers", "elapsed"])
                        .expect("failed to write csv header");
                }
                csv_writer
                    .write_record([
                        "galen",
                        args.workers.to_string().as_str(),
                        elapsed.as_secs_f64().to_string().as_str(),
                    ])
                    .expect("failed to write csv record");
            }
        }
    });

    hruntime.join().map_err(|error| {
        if let Some(message) = error.downcast_ref::<&'static str>() {
            anyhow::anyhow!("failed to join runtime with main thread: {message}")
        } else if let Some(message) = error.downcast_ref::<String>() {
            anyhow::anyhow!("failed to join runtime with main thread: {message}")
        } else {
            anyhow::anyhow!("failed to join runtime with main thread")
        }
    })
}

#[cfg(all(windows, miri))]
fn main() {}

/// Unzips data for the galen benchmark if it doesn't already exist
fn unpack_galen_data() -> Result<()> {
    let galen_data = Path::new(GALEN_DATA);
    if !galen_data.exists() {
        fs::create_dir(galen_data).with_context(|| {
            format!(
                "failed to create directory '{}' for galen data",
                galen_data.display(),
            )
        })?;

        let archive_file = Path::new(GALEN_ARCHIVE);
        let mut archive = ZipArchive::new(BufReader::new(File::open(archive_file).with_context(
            || {
                format!(
                    "failed to open galen archive file '{}'",
                    archive_file.display(),
                )
            },
        )?))
        .with_context(|| format!("failed to read galen archive '{}'", archive_file.display()))?;

        archive.extract(galen_data).with_context(|| {
            format!(
                "failed to unzip '{}' into '{}'",
                archive_file.display(),
                galen_data.display(),
            )
        })?;
    }

    Ok(())
}
