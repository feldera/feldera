//! Measures how many steps it takes to commit a transaction through a chain of
//! joins, and how long, on one host or on several hosts in one process.  The
//! hosts exchange data over localhost TCP, as the hosts of a multihost
//! pipeline do.
//!
//! Each join keys its input anew, so each one passes it through a sharded
//! accumulator, and the commit has to flush them one after another.
//!
//! Modes:
//!
//! - `commit` loads facts and a dimension table, then commits `--commits`
//!   transactions of `--rows` new facts each.  With `--recursive`, the circuit
//!   also holds a transitive closure over a path graph, which runs again in
//!   every step of a commit although no transaction changes its input.
//! - `steps` runs `--steps` steps that each add `--rows` facts, without
//!   committing, to measure a step outside commits.
//! - `skewed` commits transactions whose facts all have the same join key, with
//!   joins that emit at most `--chunk-size` records per step.  One worker then
//!   emits each join's output over many steps while the others finish at once.
//!
//! Each run prints one line of results.  `--json` also writes every sample.

use anyhow::{Result, anyhow};
use clap::{Parser, ValueEnum};
use dbsp::{
    DBSPHandle, OrdZSet, RootCircuit, Runtime, Stream, ZSetHandle, ZWeight,
    circuit::{CircuitConfig, Layout},
    utils::Tup2,
};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use serde_json::json;
use std::{
    iter::zip,
    net::TcpListener,
    path::PathBuf,
    sync::mpsc::{Receiver, Sender, channel},
    thread::{self, JoinHandle},
    time::Instant,
};

/// The number of keys in the dimension table that every join joins with.
const KEYS: u64 = 64;

/// The number of nodes in the path graph whose transitive closure the
/// `--recursive` circuit computes.
const NODES: u64 = 64;

/// The number of bootstrap resamples behind each confidence interval.
const RESAMPLES: usize = 2000;

#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
enum Mode {
    Commit,
    Steps,
    Skewed,
}

#[derive(Parser, Debug)]
#[command(about = "Steps and time to commit through a chain of joins")]
struct Args {
    #[arg(long, value_enum, default_value_t = Mode::Commit)]
    mode: Mode,

    /// Workers on all hosts together.
    #[arg(long, default_value_t = 4)]
    workers: usize,

    /// Hosts, each a runtime of its own in this process.
    #[arg(long, default_value_t = 1)]
    hosts: usize,

    /// Joins in the chain.
    #[arg(long, default_value_t = 8)]
    depth: usize,

    /// Adds a transitive closure in a recursive sub-circuit.
    #[arg(long)]
    recursive: bool,

    /// Facts loaded before the measured transactions.
    #[arg(long, default_value_t = 100_000)]
    load: usize,

    /// Measured transactions (`commit`, `skewed`).
    #[arg(long, default_value_t = 100)]
    commits: usize,

    /// Measured steps (`steps`).
    #[arg(long, default_value_t = 2000)]
    steps: usize,

    /// Transactions or steps run before measuring.
    #[arg(long, default_value_t = 10)]
    warmup: usize,

    /// New facts in each transaction (`commit`, `skewed`) or step (`steps`).
    #[arg(long, default_value_t = 10)]
    rows: usize,

    /// Records that a join emits per step (`skewed`).
    #[arg(long, default_value_t = 10)]
    chunk_size: u64,

    /// Writes every sample to this file, as JSON.
    #[arg(long)]
    json: Option<PathBuf>,
}

/// A command to the thread that drives one host.
enum Command {
    StartTransaction,
    Append {
        facts: Vec<Tup2<Tup2<u64, u64>, ZWeight>>,
        dim: Vec<Tup2<Tup2<u64, u64>, ZWeight>>,
        edges: Vec<Tup2<Tup2<u64, u64>, ZWeight>>,
    },
    StartCommit,
    /// Runs a step, and answers whether it completed the commit.
    Step,
}

/// The input handles of one host's circuit.
struct Inputs {
    facts: ZSetHandle<Tup2<u64, u64>>,
    dim: ZSetHandle<Tup2<u64, u64>>,
    edges: Option<ZSetHandle<Tup2<u64, u64>>>,
}

/// Builds the circuit: `facts` through a chain of `depth` joins with `dim`,
/// and, if `recursive`, the transitive closure of `edges`.
fn build(circuit: &mut RootCircuit, depth: usize, recursive: bool) -> Result<Inputs> {
    let (facts, facts_handle) = circuit.add_input_zset::<Tup2<u64, u64>>();
    let (dim, dim_handle) = circuit.add_input_zset::<Tup2<u64, u64>>();
    let dim = dim.map_index(|Tup2(key, value)| (*key, *value));
    let mut chain = facts;
    for i in 0..depth as u64 {
        chain = chain
            .map_index(|Tup2(a, b)| (*b % KEYS, *a))
            .join(&dim, move |_key, a, d| Tup2(*a, *d + i));
    }
    chain.output();

    let edges_handle = if recursive {
        let (edges, edges_handle) = circuit.add_input_zset::<Tup2<u64, u64>>();
        let paths = circuit.recursive(|child, paths: Stream<_, OrdZSet<Tup2<u64, u64>>>| {
            let edges = edges.delta0(child);
            let by_target = paths.map_index(|Tup2(source, target)| (*target, *source));
            let by_source = edges.map_index(|Tup2(source, target)| (*source, *target));
            Ok(edges
                .plus(&by_target.join(&by_source, |_via, source, target| Tup2(*source, *target))))
        })?;
        paths.output();
        Some(edges_handle)
    } else {
        None
    };

    Ok(Inputs {
        facts: facts_handle,
        dim: dim_handle,
        edges: edges_handle,
    })
}

/// Returns a configuration for each of `hosts` hosts that run `workers`
/// workers in total, each listening on a localhost port, as in a multihost
/// pipeline.  For one host, a single-host configuration.
fn host_configs(workers: usize, hosts: usize) -> Result<Vec<CircuitConfig>> {
    if hosts == 0 || workers < hosts {
        return Err(anyhow!("{hosts} hosts cannot run {workers} workers"));
    }
    if hosts == 1 {
        return Ok(vec![CircuitConfig::with_workers(workers)]);
    }
    let listeners = (0..hosts)
        .map(|_| TcpListener::bind("127.0.0.1:0"))
        .collect::<Result<Vec<_>, _>>()?;
    let params = listeners
        .iter()
        .enumerate()
        .map(|(host, listener)| {
            Ok((
                listener.local_addr()?,
                workers / hosts + usize::from(host < workers % hosts),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    zip(&params, listeners)
        .map(|(&(address, _), listener)| {
            Ok(
                CircuitConfig::from(Layout::new_multihost(&params, address)?)
                    .with_exchange_listener(listener),
            )
        })
        .collect()
}

/// The hosts, each driven by a thread of its own, so that they step together.
struct Hosts {
    commands: Vec<Sender<Command>>,
    answers: Vec<Receiver<bool>>,
    threads: Vec<JoinHandle<()>>,
}

impl Hosts {
    fn start(args: &Args) -> Result<Self> {
        let mut hosts = Hosts {
            commands: Vec::new(),
            answers: Vec::new(),
            threads: Vec::new(),
        };
        for config in host_configs(args.workers, args.hosts)? {
            let config = if args.mode == Mode::Skewed {
                config.with_splitter_chunk_size_records(args.chunk_size)
            } else {
                config
            };
            let (depth, recursive) = (args.depth, args.recursive);
            let (command_sender, command_receiver) = channel();
            let (answer_sender, answer_receiver) = channel();
            hosts.threads.push(thread::spawn(move || {
                let (handle, inputs) =
                    Runtime::init_circuit(config, move |circuit| build(circuit, depth, recursive))
                        .expect("cannot start a host");
                drive(handle, inputs, command_receiver, answer_sender);
            }));
            hosts.commands.push(command_sender);
            hosts.answers.push(answer_receiver);
        }
        Ok(hosts)
    }

    /// Sends each host the command that `command` makes for it, and waits for
    /// all of them to answer.
    fn run(&self, command: impl Fn(usize) -> Command) -> Vec<bool> {
        for (host, sender) in self.commands.iter().enumerate() {
            sender.send(command(host)).expect("a host stopped");
        }
        self.answers
            .iter()
            .map(|answers| answers.recv().expect("a host stopped"))
            .collect()
    }

    /// Runs a step on every host, and returns whether it completed the commit.
    fn step(&self) -> bool {
        let committed = self.run(|_| Command::Step);
        assert!(
            committed.iter().all(|&host| host == committed[0]),
            "the hosts completed the commit in different steps"
        );
        committed[0]
    }

    /// Adds `facts`, `dim` and `edges`, spread over the hosts in turn.
    fn append(
        &self,
        facts: Vec<Tup2<u64, u64>>,
        dim: Vec<Tup2<u64, u64>>,
        edges: Vec<Tup2<u64, u64>>,
    ) {
        let n = self.commands.len();
        let share = |records: &Vec<Tup2<u64, u64>>, host: usize| {
            records
                .iter()
                .skip(host)
                .step_by(n)
                .map(|record| Tup2(*record, 1))
                .collect()
        };
        self.run(|host| Command::Append {
            facts: share(&facts, host),
            dim: share(&dim, host),
            edges: share(&edges, host),
        });
    }

    /// Commits the current transaction.
    ///
    /// # Returns
    ///
    /// The number of steps the commit took.
    fn commit(&self) -> usize {
        self.run(|_| Command::StartCommit);
        let mut steps = 1;
        while !self.step() {
            steps += 1;
        }
        steps
    }

    fn stop(self) {
        drop(self.commands);
        for thread in self.threads {
            thread.join().expect("a host panicked");
        }
    }
}

/// Runs the commands that the benchmark sends one host, until it stops
/// sending.
fn drive(
    mut handle: DBSPHandle,
    inputs: Inputs,
    commands: Receiver<Command>,
    answers: Sender<bool>,
) {
    for command in commands {
        let answer = match command {
            Command::StartTransaction => {
                handle.start_transaction().unwrap();
                true
            }
            Command::Append {
                mut facts,
                mut dim,
                mut edges,
            } => {
                inputs.facts.append(&mut facts);
                inputs.dim.append(&mut dim);
                if let Some(input) = &inputs.edges {
                    input.append(&mut edges);
                }
                true
            }
            Command::StartCommit => {
                handle.start_commit_transaction().unwrap();
                true
            }
            Command::Step => handle.step().unwrap(),
        };
        answers.send(answer).unwrap();
    }
    handle.kill().unwrap();
}

/// The CPU time that the process has used so far, in seconds.
fn cpu_seconds() -> f64 {
    // SAFETY: `getrusage` only writes the struct it is given.
    unsafe {
        let mut usage: libc::rusage = std::mem::zeroed();
        libc::getrusage(libc::RUSAGE_SELF, &mut usage);
        let seconds = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
        seconds(usage.ru_utime) + seconds(usage.ru_stime)
    }
}

/// One measured commit or step.
struct Sample {
    steps: usize,
    seconds: f64,
    cpu_seconds: f64,
}

/// Returns the `q` quantile of sorted `values`, by the nearest rank.
fn quantile(sorted: &[f64], q: f64) -> f64 {
    let rank = ((q * sorted.len() as f64).ceil() as usize).clamp(1, sorted.len());
    sorted[rank - 1]
}

/// Returns a 95% bootstrap confidence interval for the median of `values`.
fn median_interval(values: &[f64]) -> (f64, f64) {
    let mut rng = ChaCha8Rng::seed_from_u64(0x5eed);
    let mut medians = (0..RESAMPLES)
        .map(|_| {
            let mut resample = (0..values.len())
                .map(|_| values[rng.gen_range(0..values.len())])
                .collect::<Vec<_>>();
            resample.sort_by(f64::total_cmp);
            quantile(&resample, 0.5)
        })
        .collect::<Vec<_>>();
    medians.sort_by(f64::total_cmp);
    (quantile(&medians, 0.025), quantile(&medians, 0.975))
}

/// Prints the results of `samples` on one line, and writes them to the
/// `--json` file if there is one.
fn report(args: &Args, samples: &[Sample]) -> Result<()> {
    let mut millis = samples
        .iter()
        .map(|sample| sample.seconds * 1e3)
        .collect::<Vec<_>>();
    millis.sort_by(f64::total_cmp);
    let (low, high) = median_interval(&millis);
    let mut steps = samples
        .iter()
        .map(|sample| sample.steps)
        .collect::<Vec<_>>();
    steps.sort();
    let cpu_millis = samples
        .iter()
        .map(|sample| sample.cpu_seconds * 1e3)
        .sum::<f64>()
        / samples.len() as f64;
    let unit = if args.mode == Mode::Steps {
        "step"
    } else {
        "commit"
    };
    println!(
        "mode={:?} hosts={} workers={} depth={} recursive={} rows={} samples={} \
         steps(min/median/max)={}/{}/{} ms_per_{unit}(median [95% CI] p90 p99)={:.3} [{:.3}, {:.3}] {:.3} {:.3} \
         cpu_ms_per_{unit}(mean)={:.3}",
        args.mode,
        args.hosts,
        args.workers,
        args.depth,
        args.recursive,
        args.rows,
        samples.len(),
        steps[0],
        steps[steps.len() / 2],
        steps[steps.len() - 1],
        quantile(&millis, 0.5),
        low,
        high,
        quantile(&millis, 0.9),
        quantile(&millis, 0.99),
        cpu_millis,
    );
    if let Some(path) = &args.json {
        let samples = samples
            .iter()
            .map(|sample| json!([sample.steps, sample.seconds, sample.cpu_seconds]))
            .collect::<Vec<_>>();
        let document = json!({
            "args": format!("{args:?}"),
            "samples": ["steps", "seconds", "cpu_seconds"],
            "values": samples,
        });
        std::fs::write(path, serde_json::to_string_pretty(&document)?)?;
    }
    Ok(())
}

/// Facts `first..first + n` with the given join value.  Each joins with one
/// row of the dimension table at every hop of the chain.
fn facts(first: u64, n: usize, value: impl Fn(u64) -> u64) -> Vec<Tup2<u64, u64>> {
    (first..first + n as u64)
        .map(|id| Tup2(id, value(id)))
        .collect()
}

fn main() -> Result<()> {
    let args = Args::parse();
    let hosts = Hosts::start(&args)?;

    // Load the dimension table, the facts and, for `--recursive`, a path graph.
    let dim = (0..KEYS).map(|key| Tup2(key, key * 7 % KEYS)).collect();
    let edges = if args.recursive {
        (0..NODES).map(|node| Tup2(node, node + 1)).collect()
    } else {
        Vec::new()
    };
    hosts.run(|_| Command::StartTransaction);
    hosts.append(facts(0, args.load, |id| id * 3), dim, edges);
    hosts.commit();

    let mut next_fact = args.load as u64;
    let mut samples = Vec::new();
    match args.mode {
        Mode::Commit | Mode::Skewed => {
            for transaction in 0..args.warmup + args.commits {
                hosts.run(|_| Command::StartTransaction);
                let new_facts = if args.mode == Mode::Skewed {
                    facts(next_fact, args.rows, |_| 0)
                } else {
                    facts(next_fact, args.rows, |id| id * 3)
                };
                next_fact += args.rows as u64;
                hosts.append(new_facts, Vec::new(), Vec::new());

                let cpu = cpu_seconds();
                let start = Instant::now();
                let steps = hosts.commit();
                if transaction >= args.warmup {
                    samples.push(Sample {
                        steps,
                        seconds: start.elapsed().as_secs_f64(),
                        cpu_seconds: cpu_seconds() - cpu,
                    });
                }
            }
        }
        Mode::Steps => {
            hosts.run(|_| Command::StartTransaction);
            for step in 0..args.warmup + args.steps {
                hosts.append(
                    facts(next_fact, args.rows, |id| id * 3),
                    Vec::new(),
                    Vec::new(),
                );
                next_fact += args.rows as u64;

                let cpu = cpu_seconds();
                let start = Instant::now();
                let committed = hosts.step();
                assert!(!committed, "a step outside a commit completed a commit");
                if step >= args.warmup {
                    samples.push(Sample {
                        steps: 1,
                        seconds: start.elapsed().as_secs_f64(),
                        cpu_seconds: cpu_seconds() - cpu,
                    });
                }
            }
            hosts.commit();
        }
    }

    report(&args, &samples)?;
    hosts.stop();
    Ok(())
}
