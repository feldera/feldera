//! What does the merge threshold do to a rolling aggregate, through a large
//! backfill and the small steps that follow it?
//!
//! A rolling aggregate reads a partition's recent history from its traces on
//! every update, with range seeks a membership filter cannot answer, so it is
//! the kind of operator that pays for every batch its spines carry.  Raising
//! the merge threshold leaves more batches in each level in exchange for less
//! merging; this measures both sides of that trade on one operator.
//!
//! The circuit is the one the SQL compiler builds for `SUM(v) OVER (PARTITION
//! BY p ORDER BY ts RANGE BETWEEN window PRECEDING AND CURRENT ROW)` without a
//! declared lateness: `partitioned_rolling_aggregate` with a summing `Fold`,
//! and no waterline, so every record stays in the traces.
//!
//! The run has three phases:
//!
//! * **ingest**: a backfill of `--records` records in one transaction, fed in
//!   steps of `--chunk` records;
//! * **commit**: committing that transaction, which is where the aggregate is
//!   computed for every record;
//! * **incremental**: `--steps` transactions of `--step-records` records each.
//!
//! Timestamps grow with every record, one second apart, so a new record only
//! ever starts a window and never falls inside one already computed: each
//! incremental step should produce exactly one output per record it adds,
//! and the run checks that it does.
//!
//! For each phase it reports wall time, the CPU time of the circuit's workers
//! (foreground) and of everything else (background, mostly the mergers), the
//! peak resident memory, and the storage used.
//!
//! ```text
//! cargo bench -p dbsp --bench rolling_aggregate_backfill -- --dir PATH
//!     [--records N] [--chunk N] [--steps N] [--step-records N]
//!     [--partitions N] [--window-secs N] [--workers N] [--max-rss-gib N]
//!     [--merge-threshold N]
//! ```
//!
//! `--merge-threshold` sets how many batches a level of an integral's spine
//! waits for before it merges; left out, the spine keeps its built-in minimum.
//! The storage goes under `--dir`, which has to be on a disk for the storage
//! to be read from one: the run says so when it is a file system held in
//! memory.

use dbsp::{
    Runtime, ZWeight,
    algebra::DefaultSemigroup,
    circuit::{
        CircuitConfig, CircuitStorageConfig, StorageCacheConfig, StorageConfig, StorageOptions,
    },
    operator::{
        Fold,
        time_series::{RelOffset, RelRange},
    },
    typed_batch::DynBatchReader,
    utils::Tup2,
};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

/// One input record: a timestamp in milliseconds, keying a partition and a
/// value, with its weight.
type Record = Tup2<u64, Tup2<Tup2<u32, i64>, ZWeight>>;

/// Milliseconds between consecutive records' timestamps.
const MILLIS_PER_RECORD: u64 = 1_000;

/// The phases of a run, in order.
const PHASES: [&str; 3] = ["ingest", "commit", "incremental"];

#[derive(Clone, Debug)]
struct Args {
    records: u64,
    chunk: u64,
    steps: usize,
    step_records: u64,
    partitions: u64,
    window_secs: u64,
    workers: usize,
    max_rss_gib: u64,
    merge_threshold: Option<u16>,
    dir: Option<PathBuf>,
}

impl Args {
    fn parse() -> Self {
        let mut args = Self {
            records: 1_000_000_000,
            chunk: 1_000_000,
            steps: 1_000,
            step_records: 10_000,
            partitions: 500_000,
            window_secs: 45 * 86_400,
            workers: 16,
            max_rss_gib: 64,
            merge_threshold: None,
            dir: None,
        };
        let argv: Vec<String> = std::env::args().collect();
        let mut i = 1;
        let value = |i: &mut usize| -> String {
            *i += 1;
            argv.get(*i)
                .cloned()
                .unwrap_or_else(|| panic!("{} wants a value", argv[*i - 1]))
        };
        while i < argv.len() {
            let flag = argv[i].clone();
            let number = |text: String| -> u64 {
                text.parse()
                    .unwrap_or_else(|_| panic!("{flag} wants a number, not {text}"))
            };
            match flag.as_str() {
                "--records" => args.records = number(value(&mut i)),
                "--chunk" => args.chunk = number(value(&mut i)),
                "--steps" => args.steps = number(value(&mut i)) as usize,
                "--step-records" => args.step_records = number(value(&mut i)),
                "--partitions" => args.partitions = number(value(&mut i)),
                "--window-secs" => args.window_secs = number(value(&mut i)),
                "--workers" => args.workers = number(value(&mut i)) as usize,
                "--max-rss-gib" => args.max_rss_gib = number(value(&mut i)),
                "--merge-threshold" => {
                    let threshold = number(value(&mut i));
                    args.merge_threshold =
                        Some(u16::try_from(threshold).unwrap_or_else(|_| {
                            panic!("--merge-threshold {threshold} is too large")
                        }));
                }
                "--dir" => args.dir = Some(PathBuf::from(value(&mut i))),
                "--bench" => {}
                other => panic!("unknown argument {other}"),
            }
            i += 1;
        }
        assert!(args.chunk > 0, "--chunk wants at least one record");
        assert!(args.partitions > 0, "--partitions wants at least one");
        assert!(
            args.partitions <= u32::MAX as u64,
            "--partitions must fit a 32-bit partition key"
        );
        args
    }
}

/// Mixes the bits of `x`, so that consecutive records land in partitions with
/// no relation to one another.  This is SplitMix64's finalizer.
fn mix(x: u64) -> u64 {
    let mut z = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Records `first..first + count`, in timestamp order.
///
/// Record `i` is stamped `i` seconds in, lands in a partition chosen by
/// mixing `i`, and carries a small value taken from the same mix.
///
/// # Arguments
///
/// * `first` - the index of the first record.
/// * `count` - how many records to make.
/// * `partitions` - how many partitions the records are spread over.
///
/// # Returns
///
/// The records, each with weight one.
fn records(first: u64, count: u64, partitions: u64) -> Vec<Record> {
    (first..first + count)
        .map(|i| {
            let mixed = mix(i);
            let partition = (mixed % partitions) as u32;
            let value = ((mixed >> 40) % 1_000) as i64;
            Tup2(i * MILLIS_PER_RECORD, Tup2(Tup2(partition, value), 1))
        })
        .collect()
}

/// CPU time the process has used, in seconds, split by who used it.
#[derive(Clone, Copy, Debug, Default)]
struct Cpu {
    /// Every thread of the process, including those that have exited.
    total: f64,
    /// The circuit's worker threads.  `None` where the system does not say
    /// how much each thread has used.
    workers: Option<f64>,
    /// The spine mergers' threads.
    mergers: Option<f64>,
    /// The thread that makes the backfill's records, which a pipeline would
    /// spend in its input connectors instead.
    datagen: Option<f64>,
}

impl Cpu {
    /// How much of each kind of CPU time was used between `earlier` and now.
    fn since(self, earlier: Self) -> Self {
        let delta = |now: Option<f64>, then: Option<f64>| Some(now? - then?);
        Self {
            total: self.total - earlier.total,
            workers: delta(self.workers, earlier.workers),
            mergers: delta(self.mergers, earlier.mergers),
            datagen: delta(self.datagen, earlier.datagen),
        }
    }

    /// CPU time used outside the workers, not counting the record generator,
    /// which is the benchmark's own and not the circuit's.
    fn background(self) -> Option<f64> {
        Some(self.total - self.workers? - self.datagen.unwrap_or(0.0))
    }
}

/// The CPU time the process has used so far.
fn cpu_now() -> Cpu {
    // SAFETY: `getrusage` only writes the struct it is given.
    let total = unsafe {
        let mut usage: libc::rusage = std::mem::zeroed();
        libc::getrusage(libc::RUSAGE_SELF, &mut usage);
        let seconds = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
        seconds(usage.ru_utime) + seconds(usage.ru_stime)
    };
    let (workers, mergers, datagen) = match thread_cpu_by_name() {
        Some(threads) => {
            let sum = |prefix: &str| {
                threads
                    .iter()
                    .filter(|(name, _)| name.starts_with(prefix))
                    .map(|(_, seconds)| seconds)
                    .sum::<f64>()
            };
            (
                Some(sum("dbsp-worker-")),
                Some(sum("merger-")),
                Some(sum("datagen")),
            )
        }
        None => (None, None, None),
    };
    Cpu {
        total,
        workers,
        mergers,
        datagen,
    }
}

/// The name and CPU time of every live thread of this process.
///
/// # Returns
///
/// One entry per thread, or `None` where the system does not say.
#[cfg(target_os = "linux")]
fn thread_cpu_by_name() -> Option<Vec<(String, f64)>> {
    // SAFETY: `sysconf` reads a system constant.
    let ticks_per_second = unsafe { libc::sysconf(libc::_SC_CLK_TCK) } as f64;
    let mut threads = Vec::new();
    for entry in std::fs::read_dir("/proc/self/task").ok()?.flatten() {
        let Ok(stat) = std::fs::read_to_string(entry.path().join("stat")) else {
            continue;
        };
        // The name sits between the first '(' and the last ')', and may
        // itself hold either.
        let (Some(open), Some(close)) = (stat.find('('), stat.rfind(')')) else {
            continue;
        };
        let name = stat[open + 1..close].to_string();
        let fields: Vec<&str> = stat[close + 1..].split_whitespace().collect();
        // After the name come the state and ten more fields, then user and
        // system time in clock ticks.
        let (Some(user), Some(system)) = (fields.get(11), fields.get(12)) else {
            continue;
        };
        let ticks = user.parse::<f64>().unwrap_or(0.0) + system.parse::<f64>().unwrap_or(0.0);
        threads.push((name, ticks / ticks_per_second));
    }
    Some(threads)
}

#[cfg(not(target_os = "linux"))]
fn thread_cpu_by_name() -> Option<Vec<(String, f64)>> {
    None
}

/// How many bytes of memory the process holds, where the system will say.
#[cfg(target_os = "linux")]
fn rss_bytes() -> Option<u64> {
    let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
    let pages: u64 = statm.split_whitespace().nth(1)?.parse().ok()?;
    // SAFETY: `sysconf` reads a system constant.
    Some(pages * unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as u64)
}

#[cfg(not(target_os = "linux"))]
fn rss_bytes() -> Option<u64> {
    None
}

/// Whether `dir` is on a file system held in memory, which would make every
/// read of the storage a read of memory.
#[cfg(target_os = "linux")]
fn on_tmpfs(dir: &Path) -> bool {
    use std::os::unix::ffi::OsStrExt;
    const TMPFS_MAGIC: i64 = 0x0102_1994;
    let Ok(path) = std::ffi::CString::new(dir.as_os_str().as_bytes()) else {
        return false;
    };
    // SAFETY: `statfs` only writes the struct it is given.
    unsafe {
        let mut fs: libc::statfs = std::mem::zeroed();
        libc::statfs(path.as_ptr(), &mut fs) == 0 && fs.f_type as i64 == TMPFS_MAGIC
    }
}

#[cfg(not(target_os = "linux"))]
fn on_tmpfs(_dir: &Path) -> bool {
    false
}

/// How many bytes `dir` holds, counting what is under it.
fn dir_bytes(dir: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .flatten()
        .map(|entry| match entry.metadata() {
            Ok(meta) if meta.is_dir() => dir_bytes(&entry.path()),
            Ok(meta) => meta.len(),
            Err(_) => 0,
        })
        .sum()
}

/// The most memory and storage a phase used, and the storage it left behind.
#[derive(Clone, Copy, Debug, Default)]
struct Footprint {
    peak_rss: u64,
    peak_storage: u64,
    end_storage: u64,
}

/// Watches the process's memory and storage from a thread of its own,
/// keeping the peak of each for whichever phase is running.
struct Sampler {
    phase: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
    footprints: Arc<Mutex<[Footprint; PHASES.len()]>>,
    thread: Option<thread::JoinHandle<()>>,
    storage: PathBuf,
}

impl Sampler {
    /// Starts watching, memory five times a second and the storage under
    /// `storage` once a second.
    fn start(storage: PathBuf) -> Self {
        let phase = Arc::new(AtomicUsize::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let footprints = Arc::new(Mutex::new([Footprint::default(); PHASES.len()]));
        let thread = {
            let (phase, stop, footprints, storage) = (
                phase.clone(),
                stop.clone(),
                footprints.clone(),
                storage.clone(),
            );
            thread::Builder::new()
                .name("sampler".into())
                .spawn(move || {
                    let mut tick = 0u64;
                    while !stop.load(Ordering::Relaxed) {
                        let rss = rss_bytes().unwrap_or(0);
                        let bytes = tick.is_multiple_of(5).then(|| dir_bytes(&storage));
                        let mut footprints = footprints.lock().unwrap();
                        let footprint = &mut footprints[phase.load(Ordering::Relaxed)];
                        footprint.peak_rss = footprint.peak_rss.max(rss);
                        if let Some(bytes) = bytes {
                            footprint.peak_storage = footprint.peak_storage.max(bytes);
                        }
                        drop(footprints);
                        tick += 1;
                        thread::sleep(Duration::from_millis(200));
                    }
                })
                .expect("failed to start the sampler")
        };
        Self {
            phase,
            stop,
            footprints,
            thread: Some(thread),
            storage,
        }
    }

    /// Closes the running phase, noting the storage it leaves, and opens the
    /// next one.
    fn end_phase(&self) {
        let ended = self.phase.load(Ordering::Relaxed);
        let bytes = dir_bytes(&self.storage);
        {
            let mut footprints = self.footprints.lock().unwrap();
            footprints[ended].end_storage = bytes;
            footprints[ended].peak_storage = footprints[ended].peak_storage.max(bytes);
        }
        if ended + 1 < PHASES.len() {
            self.phase.store(ended + 1, Ordering::Relaxed);
        }
    }

    /// Stops watching.
    ///
    /// # Returns
    ///
    /// The footprint of each phase.
    fn finish(mut self) -> [Footprint; PHASES.len()] {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            thread.join().expect("the sampler panicked");
        }
        *self.footprints.lock().unwrap()
    }
}

/// What one phase cost.
#[derive(Clone, Copy, Debug)]
struct PhaseCost {
    wall: f64,
    cpu: Cpu,
    outputs: u64,
}

/// The value at `quantile` of the sorted `values`.
fn percentile(sorted: &[f64], quantile: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let rank = ((sorted.len() - 1) as f64 * quantile).round() as usize;
    sorted[rank]
}

fn gib(bytes: u64) -> f64 {
    bytes as f64 / (1u64 << 30) as f64
}

/// Prints one phase's line of the report.
fn report_phase(name: &str, cost: &PhaseCost, footprint: &Footprint) {
    let seconds = |value: Option<f64>| value.map_or("n/a".to_string(), |v| format!("{v:.0}"));
    println!(
        "  {name:<12} {:>9.1} s wall   cpu: {:>7} fg {:>7} bg ({:>7} merger) {:>7.0} total   \
         rss peak {:>5.1} GiB   storage peak {:>6.1} GiB, end {:>6.1} GiB   {} outputs",
        cost.wall,
        seconds(cost.cpu.workers),
        seconds(cost.cpu.background()),
        seconds(cost.cpu.mergers),
        cost.cpu.total,
        gib(footprint.peak_rss),
        gib(footprint.peak_storage),
        gib(footprint.end_storage),
        cost.outputs,
    );
}

fn main() {
    let args = Args::parse();
    let base = args.dir.clone().unwrap_or_else(std::env::temp_dir);
    let temp = tempfile::tempdir_in(&base).expect("failed to create the storage directory");
    let storage_path = temp.path().to_path_buf();
    if on_tmpfs(&storage_path) {
        println!(
            "WARNING: {} is held in memory, so the storage is never read from a disk; \
             pass --dir on one",
            base.display()
        );
    }

    let storage = CircuitStorageConfig::for_config(
        StorageConfig {
            path: storage_path.to_string_lossy().into_owned(),
            cache: StorageCacheConfig::default(),
        },
        StorageOptions::default(),
    )
    .expect("failed to configure storage");
    let mut config = CircuitConfig::with_workers(args.workers)
        .with_storage(Some(storage))
        .with_max_rss_bytes(Some(args.max_rss_gib << 30));
    config.dev_tweaks.integral_merge_threshold_batches = args.merge_threshold;

    println!(
        "rolling aggregate backfill: {} records in steps of {}, then {} steps of {} records; \
         {} partitions, {}-second window, {} workers, memory limit {} GiB, merge threshold {}",
        args.records,
        args.chunk,
        args.steps,
        args.step_records,
        args.partitions,
        args.window_secs,
        args.workers,
        args.max_rss_gib,
        args.merge_threshold
            .map_or("built in".to_string(), |threshold| threshold.to_string()),
    );

    let outputs = Arc::new(AtomicU64::new(0));
    let window_millis = args.window_secs * 1_000;
    let (mut dbsp, input) = {
        let outputs = outputs.clone();
        Runtime::init_circuit(config, move |circuit| {
            let (stream, input) = circuit.add_input_indexed_zset::<u64, Tup2<u32, i64>>();
            let sum = <Fold<i64, i64, DefaultSemigroup<_>, _, _>>::new(
                0i64,
                |sum: &mut i64, value: &i64, weight: ZWeight| *sum += value * weight,
            );
            let outputs = outputs.clone();
            stream
                .partitioned_rolling_aggregate(
                    |Tup2(partition, value)| (*partition, *value),
                    sum,
                    RelRange::new(RelOffset::Before(window_millis), RelOffset::Before(0)),
                )
                .inspect(move |batch| {
                    outputs.fetch_add(batch.approximate_len() as u64, Ordering::Relaxed);
                });
            Ok(input)
        })
        .expect("failed to build the circuit")
    };

    let sampler = Sampler::start(storage_path.clone());
    let mut costs = Vec::with_capacity(PHASES.len());

    // Ingest: the backfill, in one transaction.  The records are made on a
    // thread of their own, a chunk or two ahead, as a pipeline's input
    // connectors would make them.
    let (sender, receiver) = mpsc::sync_channel::<Vec<Record>>(2);
    let datagen = {
        let (total, chunk, partitions) = (args.records, args.chunk, args.partitions);
        thread::Builder::new()
            .name("datagen".into())
            .spawn(move || {
                let mut first = 0;
                while first < total {
                    let count = chunk.min(total - first);
                    if sender.send(records(first, count, partitions)).is_err() {
                        return;
                    }
                    first += count;
                }
            })
            .expect("failed to start the record generator")
    };
    let cpu = cpu_now();
    let start = Instant::now();
    dbsp.start_transaction()
        .expect("failed to start the backfill");
    let mut ingested = 0u64;
    let mut reported = 0u64;
    for mut chunk in receiver {
        ingested += chunk.len() as u64;
        input.append(&mut chunk);
        dbsp.step().expect("an ingest step failed");
        if ingested - reported >= 100_000_000 || ingested == args.records {
            reported = ingested;
            let elapsed = start.elapsed().as_secs_f64();
            println!(
                "    ingested {ingested:>13} records in {elapsed:>8.1} s ({:.2} M/s), rss {:.1} GiB",
                ingested as f64 / elapsed / 1e6,
                gib(rss_bytes().unwrap_or(0)),
            );
        }
    }
    datagen.join().expect("the record generator panicked");
    costs.push(PhaseCost {
        wall: start.elapsed().as_secs_f64(),
        cpu: cpu_now().since(cpu),
        outputs: outputs.swap(0, Ordering::Relaxed),
    });
    sampler.end_phase();

    // Commit: the aggregate is computed for every record of the backfill.
    let cpu = cpu_now();
    let start = Instant::now();
    dbsp.commit_transaction()
        .expect("failed to commit the backfill");
    costs.push(PhaseCost {
        wall: start.elapsed().as_secs_f64(),
        cpu: cpu_now().since(cpu),
        outputs: outputs.swap(0, Ordering::Relaxed),
    });
    sampler.end_phase();

    // Incremental: small transactions that continue the timeline.
    let mut step_walls = Vec::with_capacity(args.steps);
    let mut step_outputs = Vec::with_capacity(args.steps);
    let cpu = cpu_now();
    let start = Instant::now();
    for step in 0..args.steps as u64 {
        let mut batch = records(
            args.records + step * args.step_records,
            args.step_records,
            args.partitions,
        );
        let begun = Instant::now();
        input.append(&mut batch);
        dbsp.transaction().expect("an incremental step failed");
        step_walls.push(begun.elapsed().as_secs_f64());
        step_outputs.push(outputs.swap(0, Ordering::Relaxed));
        // A line per tenth of the steps, to show which way they trend.
        let tenth = (args.steps / 10).max(1);
        if step_walls.len() % tenth == 0 {
            let block = &step_walls[step_walls.len() - tenth..];
            println!(
                "    steps {:>5}..{:<5} mean {:7.2} ms, max {:7.2} ms, rss {:.1} GiB",
                step_walls.len() - tenth,
                step_walls.len(),
                block.iter().sum::<f64>() / block.len() as f64 * 1e3,
                block.iter().cloned().fold(0.0, f64::max) * 1e3,
                gib(rss_bytes().unwrap_or(0)),
            );
        }
    }
    costs.push(PhaseCost {
        wall: start.elapsed().as_secs_f64(),
        cpu: cpu_now().since(cpu),
        outputs: step_outputs.iter().sum(),
    });
    sampler.end_phase();
    let footprints = sampler.finish();
    dbsp.kill().expect("failed to stop the circuit");

    println!();
    for ((name, cost), footprint) in PHASES.iter().zip(&costs).zip(&footprints) {
        report_phase(name, cost, footprint);
    }

    let mut sorted = step_walls.clone();
    sorted.sort_by(f64::total_cmp);
    let ms = |seconds: f64| seconds * 1e3;
    let mean = |walls: &[f64]| walls.iter().sum::<f64>() / walls.len().max(1) as f64;
    let window = (step_walls.len() / 10).max(1);
    println!(
        "  steps: mean {:.2} ms, p50 {:.2}, p90 {:.2}, p99 {:.2}, max {:.2}; \
         first tenth {:.2} ms, last tenth {:.2} ms",
        ms(mean(&step_walls)),
        ms(percentile(&sorted, 0.5)),
        ms(percentile(&sorted, 0.9)),
        ms(percentile(&sorted, 0.99)),
        ms(percentile(&sorted, 1.0)),
        ms(mean(&step_walls[..window.min(step_walls.len())])),
        ms(mean(&step_walls[step_walls.len().saturating_sub(window)..])),
    );

    // A step that recomputed what an earlier one produced would retract and
    // reinsert those outputs, and so produce more than one per record.
    let expected = args.step_records;
    let wrong = step_outputs.iter().filter(|&&n| n != expected).count();
    if costs[1].outputs != args.records || wrong > 0 {
        println!(
            "  WRONG: the commit produced {} outputs for {} records, and {wrong} of {} steps \
             produced other than {expected}",
            costs[1].outputs,
            args.records,
            step_outputs.len(),
        );
    } else {
        println!(
            "  outputs: one per record, in the commit and in every step, so no step recomputed \
             an earlier one"
        );
    }

    let phase_json = |cost: &PhaseCost, footprint: &Footprint| {
        serde_json::json!({
            "wall_s": cost.wall,
            "cpu_total_s": cost.cpu.total,
            "cpu_fg_s": cost.cpu.workers,
            "cpu_bg_s": cost.cpu.background(),
            "cpu_merger_s": cost.cpu.mergers,
            "cpu_datagen_s": cost.cpu.datagen,
            "outputs": cost.outputs,
            "peak_rss_bytes": footprint.peak_rss,
            "peak_storage_bytes": footprint.peak_storage,
            "end_storage_bytes": footprint.end_storage,
        })
    };
    let result = serde_json::json!({
        "records": args.records,
        "chunk": args.chunk,
        "steps": args.steps,
        "step_records": args.step_records,
        "partitions": args.partitions,
        "window_secs": args.window_secs,
        "workers": args.workers,
        "merge_threshold": args.merge_threshold,
        "ingest": phase_json(&costs[0], &footprints[0]),
        "commit": phase_json(&costs[1], &footprints[1]),
        "incremental": phase_json(&costs[2], &footprints[2]),
        "step_ms": {
            "mean": ms(mean(&step_walls)),
            "p50": ms(percentile(&sorted, 0.5)),
            "p90": ms(percentile(&sorted, 0.9)),
            "p99": ms(percentile(&sorted, 0.99)),
            "max": ms(percentile(&sorted, 1.0)),
            "first_tenth": ms(mean(&step_walls[..window.min(step_walls.len())])),
            "last_tenth": ms(mean(&step_walls[step_walls.len().saturating_sub(window)..])),
        },
        "wrong_steps": wrong,
    });
    println!("RESULT {result}");
}
