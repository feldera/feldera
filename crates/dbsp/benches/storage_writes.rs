//! Counts the `writev` calls that the POSIX storage backend makes and measures
//! the CPU that writing costs, in one of two modes:
//!
//! - `raw` writes `--threads` files of `--file-mib` MiB each, in blocks of
//!   `--block-bytes` bytes, through the backend directly, one thread per file.
//!   This isolates the cost of the write path itself.
//! - `circuit` runs a circuit that integrates one indexed Z-set input, so the
//!   writes come from where a pipeline's do.  It ingests `--records` random
//!   records in steps of `--step-records`, during which the mergers spill
//!   merged batches to storage; waits for the mergers to go idle; takes a
//!   checkpoint, which writes the batches still in memory; and compacts every
//!   spine to one batch.
//!
//! Each phase prints one JSON line: its wall time; its CPU by thread class
//! (circuit workers, mergers, threads that exited during the phase, such as a
//! checkpoint's commit threads, and the rest); and the `writev` calls and
//! write-outs of queued blocks that the storage layer made, with the bytes they
//! wrote.  CPU by thread class needs Linux's `/proc/self/task`; elsewhere only
//! the process total is reported.
//!
//! `--dir` names the storage directory, which belongs on the device under test:
//!
//! ```text
//! cargo bench -p dbsp --bench storage_writes -- --dir /data/bench raw
//! cargo bench -p dbsp --bench storage_writes -- --dir /data/bench circuit
//! ```

use anyhow::{Context, Result, anyhow, bail};
use clap::{Parser, Subcommand};
use dbsp::{
    Runtime, ZWeight,
    circuit::{CircuitConfig, CircuitStorageConfig},
    storage::{backend::posixio_impl::PosixBackend, buffer_cache::FBuf},
    utils::Tup2,
};
use feldera_storage::{
    StorageBackend, StoragePath,
    histogram::ExponentialHistogram,
    metrics::{WRITE_BLOCKS_BYTES, WRITE_LATENCY_MICROSECONDS},
};
use feldera_types::config::{FileBackendConfig, StorageCacheConfig, StorageConfig, StorageOptions};
use nix::time::{ClockId, clock_gettime};
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
use serde_json::{Map, Value, json};
use std::{
    collections::{BTreeMap, HashMap},
    fs,
    path::PathBuf,
    thread,
    time::{Duration, Instant},
};

#[derive(Parser)]
struct Args {
    /// Directory for the storage files.  It is created if needed and must
    /// hold nothing else.
    #[arg(long)]
    dir: PathBuf,

    /// Label copied into every result line, to tell runs apart.
    #[arg(long, default_value = "")]
    label: String,

    #[command(subcommand)]
    mode: Mode,

    #[doc(hidden)]
    #[arg(long = "bench", hide = true, global = true)]
    __bench: bool,
}

#[derive(Subcommand)]
enum Mode {
    /// Writes files through the backend directly.
    Raw {
        /// Writer threads, one file each.
        #[arg(long, default_value_t = 16)]
        threads: usize,

        /// Size of each file, in MiB.
        #[arg(long, default_value_t = 256)]
        file_mib: u64,

        /// Size of each block, in bytes.
        #[arg(long, default_value_t = 12_800)]
        block_bytes: usize,
    },

    /// Ingests into a circuit, checkpoints it, and compacts it.
    Circuit {
        /// Circuit worker threads.
        #[arg(long, default_value_t = 16)]
        workers: usize,

        /// Records to ingest.
        #[arg(long, default_value_t = 200_000_000)]
        records: u64,

        /// Records per step, across all workers.
        #[arg(long, default_value_t = 160_000)]
        step_records: u64,
    },
}

/// Reads `clock`, in nanoseconds.
///
/// # Arguments
///
/// * `clock` - The clock to read.
///
/// # Returns
///
/// The clock's value in nanoseconds.
fn clock_ns(clock: ClockId) -> u64 {
    let time = clock_gettime(clock).expect("clock_gettime failed");
    time.tv_sec() as u64 * 1_000_000_000 + time.tv_nsec() as u64
}

/// The storage layer's write counters at one moment.
#[derive(Clone, Copy)]
struct WriteCounters {
    /// `writev` calls, one sample of `WRITE_BLOCKS_BYTES` each.
    writev_calls: u64,

    /// Bytes those calls wrote.
    writev_bytes: u64,

    /// `PosixWriter::flush` calls, one sample of `WRITE_LATENCY_MICROSECONDS`
    /// each.
    flushes: u64,

    /// Time spent in `PosixWriter::flush`, in microseconds, summed over threads.
    flush_micros: u64,
}

impl WriteCounters {
    /// Reads the counters.
    ///
    /// # Returns
    ///
    /// The counters' current values.
    fn now() -> Self {
        fn samples(histogram: &ExponentialHistogram) -> u64 {
            histogram
                .snapshot()
                .iter_buckets()
                .map(|bucket| bucket.count)
                .sum()
        }
        Self {
            writev_calls: samples(&WRITE_BLOCKS_BYTES),
            writev_bytes: WRITE_BLOCKS_BYTES.sum(),
            flushes: samples(&WRITE_LATENCY_MICROSECONDS),
            flush_micros: WRITE_LATENCY_MICROSECONDS.sum(),
        }
    }

    /// Reports what the counters gained since `before`.
    ///
    /// # Arguments
    ///
    /// * `before` - The counters at the start of the interval.
    ///
    /// # Returns
    ///
    /// The gains as JSON fields.
    fn json_since(&self, before: &Self) -> Map<String, Value> {
        let calls = self.writev_calls - before.writev_calls;
        let bytes = self.writev_bytes - before.writev_bytes;
        let fields = json!({
            "writev_calls": calls,
            "writev_bytes": bytes,
            "writev_mean_bytes": bytes.checked_div(calls).unwrap_or(0),
            "flushes": self.flushes - before.flushes,
            "flush_s": (self.flush_micros - before.flush_micros) as f64 / 1e6,
        });
        let Value::Object(fields) = fields else {
            unreachable!()
        };
        fields
    }
}

/// CPU time used so far by the process and by each of its live threads.
struct CpuSample {
    /// The whole process, threads that have exited included, in nanoseconds.
    process_ns: u64,

    /// Each live thread's name and CPU time in nanoseconds, by thread id.
    threads: HashMap<u32, (String, u64)>,
}

impl CpuSample {
    /// Takes a sample.
    ///
    /// # Returns
    ///
    /// The sample; without `/proc/self/task`, its thread map is empty.
    fn now() -> Self {
        let process_ns = clock_ns(ClockId::CLOCK_PROCESS_CPUTIME_ID);
        let mut threads = HashMap::new();
        if let Ok(entries) = fs::read_dir("/proc/self/task") {
            for entry in entries.flatten() {
                let Some(tid) = entry
                    .file_name()
                    .to_str()
                    .and_then(|name| name.parse().ok())
                else {
                    continue;
                };
                let dir = entry.path();
                let (Ok(name), Ok(schedstat)) = (
                    fs::read_to_string(dir.join("comm")),
                    fs::read_to_string(dir.join("schedstat")),
                ) else {
                    continue;
                };
                let Some(ns) = schedstat
                    .split_whitespace()
                    .next()
                    .and_then(|field| field.parse().ok())
                else {
                    continue;
                };
                threads.insert(tid, (name.trim().to_string(), ns));
            }
        }
        Self {
            process_ns,
            threads,
        }
    }

    /// Sums the CPU time of the merger threads.
    ///
    /// # Returns
    ///
    /// The merger threads' CPU time, in nanoseconds.
    fn merger_ns(&self) -> u64 {
        self.threads
            .values()
            .filter(|(name, _)| name.starts_with("merger-"))
            .map(|(_, ns)| ns)
            .sum()
    }

    /// Reports the CPU used since `before`, by thread class.
    ///
    /// A thread counts as `exited` if it did not live to this sample, which
    /// is how the checkpoint's commit threads show up.
    ///
    /// # Arguments
    ///
    /// * `before` - The sample at the start of the interval.
    ///
    /// # Returns
    ///
    /// CPU seconds by class, and in total.
    fn json_since(&self, before: &Self) -> Value {
        let total = self.process_ns.saturating_sub(before.process_ns);
        let mut classes: BTreeMap<&str, u64> = BTreeMap::new();
        if !self.threads.is_empty() {
            let main = std::process::id();
            let mut live = 0;
            for (tid, (name, ns)) in &self.threads {
                let delta = ns.saturating_sub(before.threads.get(tid).map_or(0, |(_, ns)| *ns));
                let class = if *tid == main {
                    "main"
                } else if name.starts_with("dbsp-worker") {
                    "worker"
                } else if name.starts_with("merger-") {
                    "merger"
                } else {
                    "other"
                };
                *classes.entry(class).or_default() += delta;
                live += delta;
            }
            classes.insert("exited", total.saturating_sub(live));
        }
        classes.insert("total", total);
        Value::Object(
            classes
                .into_iter()
                .map(|(class, ns)| (class.to_string(), json!(ns as f64 / 1e9)))
                .collect(),
        )
    }
}

/// Measures one phase of a run.
struct Phase {
    name: &'static str,
    start: Instant,
    cpu: CpuSample,
    writes: WriteCounters,
}

impl Phase {
    /// Starts measuring.
    ///
    /// # Arguments
    ///
    /// * `name` - The phase's name in its result line.
    ///
    /// # Returns
    ///
    /// The phase in progress.
    fn start(name: &'static str) -> Self {
        Self {
            name,
            cpu: CpuSample::now(),
            writes: WriteCounters::now(),
            start: Instant::now(),
        }
    }

    /// Stops measuring and prints the phase's result line.
    ///
    /// # Arguments
    ///
    /// * `header` - Fields that describe the run, copied into the line.
    /// * `extra` - Further fields for this phase.
    fn finish(self, header: &Value, extra: Value) {
        let wall = self.start.elapsed();
        let cpu = CpuSample::now();
        let writes = WriteCounters::now();
        let mut line = header.as_object().cloned().unwrap_or_default();
        line.insert("phase".into(), json!(self.name));
        line.insert("wall_s".into(), json!(wall.as_secs_f64()));
        line.insert("cpu_s".into(), cpu.json_since(&self.cpu));
        line.extend(writes.json_since(&self.writes));
        if let Value::Object(extra) = extra {
            line.extend(extra);
        }
        println!("{}", Value::Object(line));
    }
}

/// Waits until the merger threads have used no CPU for a second.
///
/// # Arguments
///
/// * `timeout` - How long to wait at most.
///
/// # Returns
///
/// An error if the mergers were still busy at the timeout.
fn wait_for_idle_mergers(timeout: Duration) -> Result<()> {
    const POLL: Duration = Duration::from_millis(250);
    const QUIET_POLLS: usize = 4;
    const QUIET_NS: u64 = 1_000_000;

    let deadline = Instant::now() + timeout;
    let mut last = CpuSample::now().merger_ns();
    let mut quiet = 0;
    while quiet < QUIET_POLLS {
        if Instant::now() > deadline {
            bail!("the mergers were still busy after {timeout:?}");
        }
        thread::sleep(POLL);
        let now = CpuSample::now().merger_ns();
        quiet = if now - last < QUIET_NS { quiet + 1 } else { 0 };
        last = now;
    }
    Ok(())
}

/// Writes `threads` files of `file_mib` MiB each, in blocks of `block_bytes`
/// bytes, through the backend directly, and prints the result line.
///
/// # Arguments
///
/// * `args` - The command line.
/// * `threads` - Writer threads, one file each.
/// * `file_mib` - Size of each file, in MiB.
/// * `block_bytes` - Size of each block, in bytes.
///
/// # Returns
///
/// An error if a write failed.
fn raw(args: &Args, threads: usize, file_mib: u64, block_bytes: usize) -> Result<()> {
    let backend = PosixBackend::new(
        &args.dir,
        StorageCacheConfig::default(),
        &FileBackendConfig::default(),
    );
    let blocks_per_file = file_mib * 1024 * 1024 / block_bytes as u64;
    let header = json!({
        "mode": "raw",
        "label": args.label,
        "threads": threads,
        "file_mib": file_mib,
        "block_bytes": block_bytes,
    });

    let phase = Phase::start("write");
    let writer_ns = thread::scope(|scope| {
        let handles: Vec<_> = (0..threads)
            .map(|index| {
                let backend = &backend;
                scope.spawn(move || -> Result<u64> {
                    let block = vec![index as u8; block_bytes];
                    let start = clock_ns(ClockId::CLOCK_THREAD_CPUTIME_ID);
                    let mut writer =
                        backend.create_named(&StoragePath::from(format!("raw-{index}")))?;
                    for _ in 0..blocks_per_file {
                        writer.write_block(FBuf::from_slice(&block))?;
                    }
                    let reader = writer.complete()?;
                    let cpu = clock_ns(ClockId::CLOCK_THREAD_CPUTIME_ID) - start;
                    // Not marked for a checkpoint, so this deletes the file.
                    drop(reader);
                    Ok(cpu)
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| {
                handle
                    .join()
                    .map_err(|_| anyhow!("a writer thread panicked"))?
            })
            .sum::<Result<u64>>()
    })?;
    phase.finish(
        &header,
        json!({ "writer_threads_cpu_s": writer_ns as f64 / 1e9 }),
    );
    Ok(())
}

/// Ingests `records` random records into a circuit with `workers` workers,
/// waits for its mergers to go idle, checkpoints it, and compacts it,
/// printing one result line per phase.
///
/// # Arguments
///
/// * `args` - The command line.
/// * `workers` - Circuit worker threads.
/// * `records` - Records to ingest.
/// * `step_records` - Records per step, across all workers.
///
/// # Returns
///
/// An error if the circuit failed.
fn circuit(args: &Args, workers: usize, records: u64, step_records: u64) -> Result<()> {
    let config = CircuitConfig::with_workers(workers).with_storage(Some(
        CircuitStorageConfig::for_config(
            StorageConfig {
                path: args.dir.to_string_lossy().into_owned(),
                cache: StorageCacheConfig::default(),
            },
            StorageOptions::default(),
        )
        .context("configuring storage")?,
    ));
    let (mut dbsp, input) = Runtime::init_circuit(config, |circuit| {
        let (stream, handle) = circuit.add_input_indexed_zset::<u64, u64>();
        stream.integrate_trace().apply(|_| ());
        Ok(handle)
    })
    .context("building the circuit")?;
    let header = json!({
        "mode": "circuit",
        "label": args.label,
        "workers": workers,
        "records": records,
        "step_records": step_records,
    });

    let mut rng = ChaCha8Rng::seed_from_u64(0);
    let phase = Phase::start("ingest");
    let mut step_time = Duration::ZERO;
    let mut steps = 0u64;
    let mut remaining = records;
    while remaining > 0 {
        let count = remaining.min(step_records);
        let mut batch: Vec<Tup2<u64, Tup2<u64, ZWeight>>> = (0..count)
            .map(|_| Tup2(rng.next_u64(), Tup2(rng.next_u64(), 1)))
            .collect();
        input.append(&mut batch);
        let start = Instant::now();
        dbsp.transaction().context("running a step")?;
        step_time += start.elapsed();
        steps += 1;
        remaining -= count;
    }
    phase.finish(
        &header,
        json!({ "steps": steps, "step_s": step_time.as_secs_f64() }),
    );

    let phase = Phase::start("settle");
    wait_for_idle_mergers(Duration::from_secs(1800))?;
    phase.finish(&header, json!({}));

    let phase = Phase::start("checkpoint");
    dbsp.checkpoint().run().context("checkpointing")?;
    phase.finish(&header, json!({}));

    let phase = Phase::start("compact");
    dbsp.start_compaction().context("starting compaction")?;
    dbsp.wait_for_compaction(Duration::from_secs(3600))?;
    phase.finish(&header, json!({}));

    dbsp.kill()
        .map_err(|_| anyhow!("stopping the circuit failed"))?;
    Ok(())
}

fn main() -> Result<()> {
    let args = Args::parse();
    fs::create_dir_all(&args.dir).with_context(|| format!("creating {}", args.dir.display()))?;
    if fs::read_dir(&args.dir)?.next().is_some() {
        bail!("{} is not empty", args.dir.display());
    }
    match &args.mode {
        Mode::Raw {
            threads,
            file_mib,
            block_bytes,
        } => raw(&args, *threads, *file_mib, *block_bytes),
        Mode::Circuit {
            workers,
            records,
            step_records,
        } => circuit(&args, *workers, *records, *step_records),
    }
}
