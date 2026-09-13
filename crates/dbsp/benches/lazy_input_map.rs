//! Compares the lazy input map with the eager one on the shape the lazy one
//! exists for: a transaction that writes far more keys than a step should
//! resolve at once, followed by transactions that rewrite a few of them.
//!
//! The eager map probes its integral once per key per step, at random.  The lazy
//! map stamps each update, accumulates the transaction, and resolves the whole
//! of it against the integral in one ordered scan when the transaction commits.
//! Which wins depends on how much of the integral a transaction touches and on
//! whether the integral fits in memory, so the benchmark runs with storage on
//! and over record sizes that put the integral well past it.
//!
//! # Running it
//!
//! Under a sampling profiler, always: the circuit profiles say where a stage's
//! CPU went between operators, and say nothing about the time a stage spent
//! waiting, which on the ingest stage is most of it.  `cargo bench` runs the
//! benchmark through a wrapper, so point samply at the binary itself:
//!
//! ```text
//! BIN=$(cargo bench --bench lazy_input_map --no-run --message-format=json \
//!        | jq -r 'select(.executable) | .executable' | tail -1)
//! samply record --save-only --presymbolicate -o ~/ingest.json.gz -- \
//!     "$BIN" --value-bytes 64 --maps lazy --load-batch 1000000 \
//!     --samply-output ~/ingest.json.gz
//! ```
//!
//! `--presymbolicate` is worth its cost: without it the profile carries bare
//! addresses until the front end resolves them, which makes it useless to
//! anything but the front end.  `--samply-output` tells the run where that
//! profile is going, so the summary it prints at the end names every profile
//! the run produced, circuit and samply alike.
//!
//! `--stop-after ingest` keeps the recording to the steps that feed a
//! transaction, with nothing after them.
//!
//! The load arrives `--load-batch` records at a time, 100K by default, so a
//! transaction of 1B records is a transaction of ten thousand steps.  That is
//! the shape the lazy map is for: it resolves the whole of it against the
//! integral once, where the eager map probes the integral in every step.
//!
//! The defaults size the load to the value: 1B records at 64 bytes, 100M at 256
//! and 1024.  `--records` overrides that.  Each map runs in a circuit of its own
//! over storage of its own, one after the other, so neither holds memory or disk
//! while the other is timed, and peak disk is the larger of the two rather than
//! the sum.

use anyhow::{Context, Result, anyhow};
use clap::Parser;
use crossbeam::channel::{Sender, bounded};
use dbsp::circuit::{CircuitConfig, CircuitStorageConfig};
use dbsp::operator::StagedBuffers;
use dbsp::{
    DBSPHandle, RootCircuit, Runtime,
    circuit::metadata::{
        CIRCUIT_CPU_TIME_SECONDS, CIRCUIT_IDLE_TIME_SECONDS, CIRCUIT_WAIT_BY_REASON_SECONDS,
        CIRCUIT_WAIT_TIME_SECONDS, MetaItem, MetricId, SPINE_ADD_BATCH_TIME_SECONDS,
        SPINE_FLUSH_BATCH_TIME_SECONDS,
    },
    mimalloc::MiMalloc,
    operator::{LazyMapHandle, MapHandle, Update},
    profile::DbspProfile,
    trace::{BatchReader as _, Cursor},
    typed_batch::{BatchReader as _, OrdIndexedZSet},
    utils::Tup2,
};
use feldera_macros::IsNone;
use feldera_types::config::{StorageCacheConfig, StorageConfig, StorageOptions};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use rkyv::{Archive, Deserialize, Serialize};
use size_of::SizeOf;
use std::{
    collections::{BTreeMap, HashSet, VecDeque},
    fmt::Debug,
    hash::Hash,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
use tempfile::TempDir;

#[global_allocator]
static ALLOC: MiMalloc = MiMalloc;

/// A value of a fixed size.
///
/// The first word counts the writes a key has taken, so a rewrite changes the
/// record; the rest is payload, there to put the integral past memory.
trait Payload: dbsp::DBData {
    const BYTES: usize;

    fn new(version: u64, seed: u64) -> Self;
}

/// Declares a payload of `8 * (1 + $words)` bytes.
///
/// `Default` is written out rather than derived because arrays implement it
/// only up to 32 elements.
macro_rules! payload {
    ($name:ident, $words:literal) => {
        #[derive(
            Clone,
            PartialEq,
            Eq,
            PartialOrd,
            Ord,
            Hash,
            Debug,
            SizeOf,
            Archive,
            Serialize,
            Deserialize,
            IsNone,
        )]
        #[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
        #[archive(compare(PartialEq, PartialOrd))]
        struct $name {
            version: u64,
            payload: [u64; $words],
        }

        impl Default for $name {
            fn default() -> Self {
                Self {
                    version: 0,
                    payload: [0; $words],
                }
            }
        }

        impl Payload for $name {
            const BYTES: usize = 8 * (1 + $words);

            fn new(version: u64, seed: u64) -> Self {
                // Storage compresses what it writes, and a payload of repeated
                // words compresses to nothing: the integral would then fit in
                // memory however many bytes a record claims to be, which is the
                // opposite of what a large-record run is for.  These bytes do
                // not compress.
                let mut payload = [0u64; $words];
                let mut state = seed ^ version.wrapping_mul(0x2545_F491_4F6C_DD1D);
                for word in payload.iter_mut() {
                    state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
                    *word = mix(state);
                }
                Self { version, payload }
            }
        }
    };
}

payload!(Value64, 7);
payload!(Value192, 23);
payload!(Value256, 31);
payload!(Value1024, 127);

/// The splitmix64 finalizer: a bijection on `u64` that decorrelates its input.
fn mix(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// A bijection on `[0, n)` that scatters its input.
///
/// The backfill writes every key exactly once, so the order it writes them in
/// has to be a permutation.  Shuffling a materialized one would cost 8 GiB at a
/// billion keys and would have to be shared between the generator threads; a
/// small Feistel network is a permutation by construction, needs no memory, and
/// leaves each thread able to compute its own batches alone.
///
/// Order matters because it decides what a batch's key range covers.  Ascending
/// keys give each batch on disk a narrow range, and an upsert of a later key is
/// then excluded by min and max alone, so no membership filter is ever
/// consulted and no read is ever issued.  Real keys do not arrive sorted, and a
/// benchmark whose lookups are all answered by a range check measures something
/// no workload will see.
#[derive(Clone, Copy)]
struct Shuffle {
    n: u64,
    half_bits: u32,
    half_mask: u64,
}

impl Shuffle {
    /// Four rounds is what makes a Feistel network a strong pseudorandom
    /// permutation rather than merely a bijection.
    const ROUNDS: u64 = 4;

    fn new(n: u64) -> Self {
        // Round the domain up to an even number of bits, so the two halves of
        // the network are the same width.
        let bits = (u64::BITS - n.saturating_sub(1).leading_zeros()).max(2);
        let bits = bits + (bits & 1);
        Self {
            n,
            half_bits: bits / 2,
            half_mask: (1 << (bits / 2)) - 1,
        }
    }

    /// One pass of the network: a bijection on the power-of-two domain.
    fn permute(&self, x: u64) -> u64 {
        let (mut left, mut right) = (x >> self.half_bits, x & self.half_mask);
        for round in 0..Self::ROUNDS {
            let next = left ^ (mix(right ^ (round << 40)) & self.half_mask);
            left = right;
            right = next;
        }
        (left << self.half_bits) | right
    }

    /// The key at `index` of the permutation, for `index` in `[0, n)`.
    fn key(&self, index: u64) -> u64 {
        // Cycle walking.  The network permutes a power-of-two domain larger
        // than `n`, so anything landing outside is fed back in.  This
        // terminates, and is itself a bijection on `[0, n)`, because a
        // bijection's cycles are closed: the orbit of a point inside `n`
        // returns to it.
        let mut x = index;
        loop {
            x = self.permute(x);
            if x < self.n {
                return x;
            }
        }
    }
}

#[derive(Parser, Debug, Clone)]
#[command(about = "Lazy versus eager input map, under a transaction that writes a whole table")]
struct Args {
    /// Bytes per value: 64, 192, 256 or 1024.
    #[arg(long, default_value_t = 64)]
    value_bytes: usize,

    /// Records in the initial transaction.  Defaults to what fits in 250 GiB
    /// for the chosen value size.
    #[arg(long)]
    records: Option<u64>,

    /// Records per step within the initial transaction.
    #[arg(long, default_value_t = 100_000)]
    load_batch: usize,

    /// Transactions that rewrite existing keys, after the load.
    #[arg(long, default_value_t = 1000)]
    transactions: usize,

    /// Records in each of those transactions.
    #[arg(long, default_value_t = 10_000)]
    transaction_batch: usize,

    #[arg(long, default_value_t = 8)]
    workers: usize,
    /// Merger threads.  One per worker by default, which is what leaves the
    /// mergers behind on a load whose steps arrive faster than they drain.
    #[arg(long)]
    merger_threads: Option<u16>,
    /// Partition each batch on the driver thread, between steps, rather than in
    /// the thread that generated it.
    ///
    /// Staging is the default because the driver sits on the circuit's critical
    /// path: whatever it does between steps, every worker waits through.  This
    /// turns it off, which is how to measure what it is worth.
    #[arg(long, default_value_t = false)]
    no_staging: bool,
    /// False-positive rate for the spines' Bloom filters.  Zero turns them off,
    /// which is what says how much of the eager map's speed they are carrying.
    #[arg(long)]
    bloom_rate: Option<f64>,
    /// Let file-backed batches carry roaring membership filters.
    ///
    /// On by default, and for an `i64` key it displaces the Bloom filter
    /// entirely, so turning it off is what puts `--bloom-rate` in play.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    roaring: bool,

    /// Where the circuits keep their storage.  A temporary directory by
    /// default, which lands wherever `TMPDIR` points.
    #[arg(long)]
    storage_dir: Option<PathBuf>,

    /// Which maps to run.  Both, one after the other, by default.
    #[arg(long, value_delimiter = ',', default_value = "lazy,eager")]
    maps: Vec<Map>,

    #[arg(long, default_value_t = 0)]
    seed: u64,
    /// The memory ceiling the circuit sizes its spilling against, in GiB.
    ///
    /// Without one there is no memory-pressure signal at all, so a spine holds
    /// what it has until the kernel intervenes: a billion 256-byte records
    /// reached 66.5 GiB resident and was killed on a 92 GiB machine.  Set this
    /// below what the machine can spare and the spine spills instead.
    #[arg(long)]
    max_rss_gib: Option<u64>,
    /// Write the backfill's keys in ascending order rather than shuffled.
    ///
    /// Shuffled is the default because ascending keys give every batch a narrow
    /// key range, which lets a range check answer every lookup the integral is
    /// asked for.  Real keys do not arrive sorted.  Either way each key is
    /// written exactly once.
    #[arg(long, default_value_t = false)]
    sequential_keys: bool,

    /// Stop once this stage is done, leaving the rest of the run unmeasured.
    ///
    /// `ingest` is what a sampling profiler wants pointed at the steps that
    /// feed a transaction, with nothing after them in the recording.
    #[arg(long, value_enum, default_value = "all")]
    stop_after: Stage,

    /// Where the run's samply profile is being written, if it was started
    /// under one.  The benchmark cannot know this by itself, and a run whose
    /// profiles it cannot name is a run nobody will find them for.
    #[arg(long)]
    samply_output: Option<PathBuf>,

    /// Where the per-stage circuit profiles are written.
    #[arg(long, default_value = "profile")]
    profile_dir: PathBuf,

    /// Count the collection each map leaves behind and print it, so a run can
    /// show the two did the same work.  Off by default: the count walks every
    /// delta the circuit emits, which is work the timings should not carry.
    #[arg(long, default_value_t = false)]
    verify: bool,

    /// Check the key permutation and exit, running no benchmark.
    ///
    /// This bench sets `harness = false`, so `cargo test` runs `main` rather
    /// than a test harness and a `#[cfg(test)]` module here would never run.
    #[arg(long, default_value_t = false)]
    self_check: bool,
    /// Accepted and ignored: `cargo bench` passes it to every benchmark.
    #[arg(long, hide = true)]
    bench: bool,
}

#[derive(clap::ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
enum Map {
    Lazy,
    Eager,
}

/// How far into a run to go.
#[derive(clap::ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
enum Stage {
    /// Feed the load in and stop, leaving the transaction open.
    Ingest,
    /// Commit it too, and stop before the rewriting transactions.
    Commit,
    /// The whole run.
    All,
}

/// What one map's run cost, in time and in bytes on disk.
struct Timing {
    /// Feeding the load in, a step at a time.  Both maps do their per-step work
    /// here; only the eager one resolves against the integral.
    ingest: Duration,

    /// Committing it.  The lazy map resolves the whole transaction here, so
    /// this is where its work moved to.
    commit: Duration,

    /// CPU seconds, summed over every operator and worker, for each stage.
    ingest_cpu: f64,
    commit_cpu: f64,
    transactions_cpu: f64,

    transactions: Vec<Duration>,

    /// Where each stage's circuit profile was written.
    profiles: Vec<(String, PathBuf)>,

    /// The storage directory once the load has committed.
    loaded_bytes: u64,

    /// And once the rewriting transactions have run.
    final_bytes: u64,

    /// The largest it was seen at, sampled while the run went on.  A merge
    /// writes its output before dropping its inputs, so this runs well above
    /// what the run settles at, and it is what decides whether a run fits.
    peak_bytes: u64,
}

impl Timing {
    fn report(&self, records: u64, batch: usize, samply: Option<&Path>) {
        let gib = |bytes: u64| bytes as f64 / (1 << 30) as f64;
        println!("  storage_loaded_gib={:.2}", gib(self.loaded_bytes));
        println!("  storage_final_gib={:.2}", gib(self.final_bytes));
        println!("  storage_peak_gib={:.2}", gib(self.peak_bytes));
        println!(
            "  storage_bytes_per_record={:.1}",
            self.loaded_bytes as f64 / records.max(1) as f64
        );

        let load = (self.ingest + self.commit).as_secs_f64();
        println!("  load_secs={load:.3}");
        println!("  load_ingest_secs={:.3}", self.ingest.as_secs_f64());
        println!("  load_commit_secs={:.3}", self.commit.as_secs_f64());
        println!("  load_ingest_cpu_secs={:.1}", self.ingest_cpu);
        println!("  load_commit_cpu_secs={:.1}", self.commit_cpu);
        println!(
            "  load_records_per_sec={:.0}",
            records as f64 / load.max(f64::MIN_POSITIVE)
        );

        if !self.transactions.is_empty() {
            self.report_transactions(batch);
        }

        println!("  profiles:");
        for (stage, path) in &self.profiles {
            println!("    circuit[{stage}]: {}", path.display());
        }
        match samply {
            Some(path) => println!("    samply:          {}", path.display()),
            None => println!("    samply:          not recorded (pass --samply-output)"),
        }
    }

    /// Only a run that got as far as the rewriting transactions has these.
    fn report_transactions(&self, batch: usize) {
        let mut sorted: Vec<f64> = self
            .transactions
            .iter()
            .map(Duration::as_secs_f64)
            .collect();
        let total: f64 = sorted.iter().sum();
        sorted.sort_by(f64::total_cmp);
        let mean = total / sorted.len() as f64;
        println!("  transaction_mean_secs={mean:.6}");
        println!("  transaction_median_secs={:.6}", sorted[sorted.len() / 2]);
        println!(
            "  transaction_p95_secs={:.6}",
            sorted[sorted.len() * 95 / 100]
        );
        println!("  transaction_max_secs={:.6}", sorted[sorted.len() - 1]);
        println!(
            "  transaction_records_per_sec={:.0}",
            batch as f64 / mean.max(f64::MIN_POSITIVE)
        );
        println!("  transactions_cpu_secs={:.1}", self.transactions_cpu);
    }
}

/// Checks that the key permutation is one, and that it scatters.
///
/// The backfill's contract is that every key is written exactly once: a
/// shuffle that dropped or repeated one would leave the maps holding a
/// collection `--verify` cannot explain.  Scattering is the point of shuffling
/// at all, since a batch covering a narrow key range is what lets a range check
/// answer a lookup without reading anything.
fn self_check() -> Result<()> {
    // Sizes that straddle the power-of-two boundaries the network is built on.
    for n in (1..=300u64).chain([511, 512, 513, 1023, 1024, 1025, 4096, 10_000]) {
        let shuffle = Shuffle::new(n);
        let keys: HashSet<u64> = (0..n).map(|index| shuffle.key(index)).collect();
        if keys.len() != n as usize {
            return Err(anyhow!(
                "n={n}: the shuffle wrote {} distinct keys, not {n}",
                keys.len()
            ));
        }
        if let Some(stray) = keys.iter().find(|key| **key >= n) {
            return Err(anyhow!("n={n}: the shuffle produced key {stray}"));
        }
    }
    println!("  permutation: every key written exactly once, 1..=300 and 8 boundary sizes");

    const N: u64 = 1_000_000;
    const BATCH: u64 = 1_000;
    let shuffle = Shuffle::new(N);
    for batch in [0, 1, 500, 999] {
        let first = batch * BATCH;
        let keys: Vec<u64> = (first..first + BATCH).map(|i| shuffle.key(i)).collect();
        let low = *keys.iter().min().unwrap();
        let high = *keys.iter().max().unwrap();
        // A thousand draws from a million leave the extremes within a percent
        // or so of the ends.  Ascending keys would span a thousandth.
        if low >= N / 50 || high <= N - N / 50 {
            return Err(anyhow!(
                "batch {batch} spans only {low}..{high} of 0..{N}; a batch has to \
                 cover the key space or a range check still answers every lookup"
            ));
        }
    }
    println!("  spread: a 1000-key batch spans the whole of a 1M key space");

    let near = (0..10_000)
        .filter(|index| shuffle.key(*index).abs_diff(shuffle.key(index + 1)) < N / 100)
        .count();
    // A uniform permutation leaves about 2% of neighbours within 1% of each
    // other; ascending keys would put every one of them there.
    if near >= 500 {
        return Err(anyhow!(
            "{near} of 10000 neighbouring indices produced keys within 1% of each other"
        ));
    }
    println!("  scatter: {near} of 10000 neighbouring indices stayed within 1%");

    // The generator threads compute disjoint slices with no coordination, so
    // the permutation has to be a pure function of the index.
    let wide = Shuffle::new(1_000_000_007);
    let sample: Vec<u64> = (0..100).map(|i| wide.key(i * 9_999_991)).collect();
    if sample
        != (0..100)
            .map(|i| wide.key(i * 9_999_991))
            .collect::<Vec<_>>()
    {
        return Err(anyhow!(
            "the permutation is not a pure function of the index"
        ));
    }
    println!("  reproducible: the same index gives the same key");

    Ok(())
}

fn main() -> Result<()> {
    let args = Args::parse();
    if args.self_check {
        println!("self-check:");
        return self_check();
    }
    let records = args.records.unwrap_or(match args.value_bytes {
        64 => 1_000_000_000,
        _ => 100_000_000,
    });

    println!(
        "value_bytes={} records={} load_batch={} transactions={}x{} workers={} merger_threads={} bloom_rate={} roaring={} staging={} keys={} max_rss={}",
        args.value_bytes,
        records,
        args.load_batch,
        args.transactions,
        args.transaction_batch,
        args.workers,
        match args.merger_threads {
            Some(threads) => threads.to_string(),
            None => format!("{} (one per worker)", args.workers),
        },
        match args.bloom_rate {
            Some(rate) if rate <= 0.0 => "0 (disabled)".to_string(),
            Some(rate) => rate.to_string(),
            None => "0.0001 (default)".to_string(),
        },
        args.roaring,
        if args.no_staging {
            "off (driver partitions)"
        } else {
            "on (generator partitions)"
        },
        if args.sequential_keys {
            "ascending"
        } else {
            "shuffled"
        },
        match args.max_rss_gib {
            Some(gib) => format!("{gib} GiB"),
            None => "unbounded".to_string(),
        }
    );

    for map in args.maps.clone() {
        println!("\n=== {map:?} ===");
        let timing = match args.value_bytes {
            64 => run::<Value64>(&args, records, map),
            192 => run::<Value192>(&args, records, map),
            256 => run::<Value256>(&args, records, map),
            1024 => run::<Value1024>(&args, records, map),
            other => Err(anyhow!(
                "--value-bytes must be 64, 192, 256 or 1024, not {other}"
            )),
        }?;
        timing.report(
            records,
            args.transaction_batch,
            args.samply_output.as_deref(),
        );
    }

    Ok(())
}

/// Dumps the circuit's profile and returns the CPU seconds it has spent.
///
/// The profile is cumulative, so a stage's own cost is the difference between
/// the reading at its end and the one before it.  The dump is per worker, in
/// graphviz, and carries the per-operator detail this total flattens away.
fn profile_stage(
    dbsp: &mut DBSPHandle,
    dir: &Path,
    stage: &str,
    written: &mut Vec<(String, PathBuf)>,
) -> Result<f64> {
    let into = dir.join(stage);
    match dbsp.dump_profile(&into) {
        Ok(path) => {
            println!("  profile[{stage}] written to {}", path.display());
            written.push((stage.to_string(), path));
        }
        Err(error) => println!("  profile[{stage}] could not be written: {error}"),
    }

    let profile = dbsp
        .retrieve_profile()
        .context("cannot retrieve the circuit profile")?;
    report_where_the_workers_went(stage, &profile);
    Ok(cpu_seconds(&profile))
}

/// Prints how the workers' wall clock divided up, summed over all of them.
///
/// The figures are cumulative, so a stage's own cost is the difference from the
/// stage before it.
fn report_where_the_workers_went(stage: &str, profile: &DbspProfile) {
    let total = |metric| seconds(profile, metric);
    println!(
        "  where[{stage}]: wait={:.1}s idle={:.1}s cpu={:.1}s",
        total(&CIRCUIT_WAIT_TIME_SECONDS),
        total(&CIRCUIT_IDLE_TIME_SECONDS),
        total(&CIRCUIT_CPU_TIME_SECONDS),
    );

    let mut by_reason = BTreeMap::new();
    for breakdown in profile
        .worker_profiles
        .iter()
        .flat_map(|worker| {
            worker
                .attribute_profile(&CIRCUIT_WAIT_BY_REASON_SECONDS)
                .into_values()
        })
        .filter_map(|item| match item {
            MetaItem::Map(breakdown) => Some(breakdown),
            _ => None,
        })
    {
        for (reason, parked) in breakdown {
            if let MetaItem::Duration(parked) = parked {
                *by_reason.entry(reason).or_insert(0.0) += parked.as_secs_f64();
            }
        }
    }
    let reasons = by_reason
        .iter()
        .map(|(reason, parked)| format!("{reason}={parked:.1}s"))
        .collect::<Vec<_>>();
    println!("    waiting for: {}", reasons.join(" "));
    println!(
        "    blocked in:  spine_flush_batch={:.1}s spine_add_batch={:.1}s",
        total(&SPINE_FLUSH_BATCH_TIME_SECONDS),
        total(&SPINE_ADD_BATCH_TIME_SECONDS),
    );
}

/// Sums a duration metric over every worker and every operator that reports it.
fn seconds(profile: &DbspProfile, metric: &MetricId) -> f64 {
    profile
        .worker_profiles
        .iter()
        .flat_map(|worker| worker.attribute_profile(metric).into_values())
        .filter_map(|item| match item {
            MetaItem::Duration(spent) => Some(spent.as_secs_f64()),
            _ => None,
        })
        .sum()
}

/// CPU seconds the circuit has spent, over every operator and every worker.
fn cpu_seconds(profile: &DbspProfile) -> f64 {
    profile
        .worker_profiles
        .iter()
        .flat_map(|worker| {
            worker
                .attribute_profile(&CIRCUIT_CPU_TIME_SECONDS)
                .into_values()
        })
        .filter_map(|item| match item {
            MetaItem::Duration(spent) => Some(spent.as_secs_f64()),
            _ => None,
        })
        .sum()
}

/// Bytes the storage directory holds, summed over its files.
///
/// This is what the files say they are, not what the filesystem charges for
/// them, which is close enough to compare two maps against each other.
fn dir_bytes(path: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(path) else {
        return 0;
    };
    entries
        .flatten()
        .map(|entry| match entry.file_type() {
            Ok(kind) if kind.is_dir() => dir_bytes(&entry.path()),
            Ok(_) => entry.metadata().map(|meta| meta.len()).unwrap_or(0),
            Err(_) => 0,
        })
        .sum()
}

/// Samples the storage directory while a run goes on, keeping the largest it
/// was seen at.
///
/// A spine merge writes its output before dropping the batches it merged, so
/// what a run needs on disk is well above what it settles at, and sampling is
/// the only way to see it: the peak is gone by the time the run ends.
struct DiskWatch {
    peak: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
    sampler: Option<JoinHandle<()>>,
}

impl DiskWatch {
    fn start(path: PathBuf) -> Self {
        let peak = Arc::new(AtomicU64::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let (sampled, stopped) = (peak.clone(), stop.clone());
        let sampler = thread::spawn(move || {
            while !stopped.load(Ordering::Relaxed) {
                sampled.fetch_max(dir_bytes(&path), Ordering::Relaxed);
                thread::sleep(Duration::from_millis(500));
            }
        });
        Self {
            peak,
            stop,
            sampler: Some(sampler),
        }
    }

    fn peak(&self) -> u64 {
        self.peak.load(Ordering::Relaxed)
    }
}

impl Drop for DiskWatch {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(sampler) = self.sampler.take() {
            let _ = sampler.join();
        }
    }
}

/// Builds a circuit holding one input map, loads it, then rewrites keys in it.
fn run<V>(args: &Args, records: u64, map: Map) -> Result<Timing>
where
    V: Payload,
    Tup2<i64, Option<V>>: dbsp::DBData,
    Tup2<i64, Update<V, V>>: dbsp::DBData,
{
    // The record is the size it claims to be, padding included.  A benchmark
    // whose 1024-byte value is really 1032 bytes measures something else.
    assert_eq!(
        std::mem::size_of::<V>(),
        V::BYTES,
        "the payload type is not the size it declares"
    );

    // What the run will want on disk, before it spends hours finding out.  A
    // record costs its key, its value and its weight.  The lazy map's
    // accumulator is not a second copy of the transaction: the batches it
    // gathers are the ones the integral takes, rewrapped rather than rewritten,
    // so the two share their files.  What it adds is the stamp each accumulated
    // record carries until the transaction commits.
    //
    // Neither figure counts spine merges, which write a batch before dropping
    // the ones they merged.  That is the larger term while a load runs: 100M
    // records of 1024 bytes settled at 114 GiB but peaked near 205 GiB.
    let per_record = (8 + V::BYTES + 8) as u64;
    let integral = records * per_record;
    let held = match map {
        Map::Lazy => integral + records * 4,
        Map::Eager => integral,
    };
    println!(
        "  storage: {:.1} GiB once loaded, up to about twice that while merges run",
        held as f64 / (1 << 30) as f64
    );

    // A directory of its own per run, so one map's files never sit on disk
    // while the other is timed.
    let temp = match &args.storage_dir {
        Some(dir) => {
            std::fs::create_dir_all(dir).context("cannot create the storage directory")?;
            tempfile::tempdir_in(dir).context("cannot create the storage directory")?
        }
        None => TempDir::new().context("cannot create the storage directory")?,
    };

    let mut config = CircuitConfig::with_workers(args.workers).with_storage(Some(
        CircuitStorageConfig::for_config(
            StorageConfig {
                path: temp.path().to_string_lossy().into_owned(),
                cache: StorageCacheConfig::default(),
            },
            StorageOptions {
                bloom_false_positive_rate: args.bloom_rate,
                ..StorageOptions::default()
            },
        )
        .context("cannot configure storage")?,
    ));
    config.dev_tweaks.merger_threads = args.merger_threads;
    config.dev_tweaks.enable_roaring = Some(args.roaring);
    config = config.with_max_rss_bytes(args.max_rss_gib.map(|gib| gib << 30));

    let weights = Arc::new(AtomicI64::new(0));
    let (verify, counter) = (args.verify, weights.clone());

    let timing = match map {
        Map::Lazy => {
            let (dbsp, handle) = Runtime::init_circuit(config, move |circuit: &mut RootCircuit| {
                let (stream, handle) = circuit.add_lazy_input_map::<i64, V>();
                consume(&stream, verify, counter.clone());
                Ok(handle)
            })
            .context("cannot build the circuit")?;
            drive(
                dbsp,
                LazyWriter(handle),
                args,
                records,
                temp.path(),
                &args.profile_dir,
                "lazy",
            )
        }
        Map::Eager => {
            let (dbsp, handle) = Runtime::init_circuit(config, move |circuit: &mut RootCircuit| {
                let (stream, handle) = circuit.add_input_map::<i64, V, V, _>(|v, u| *v = u.clone());
                consume(&stream, verify, counter.clone());
                Ok(handle)
            })
            .context("cannot build the circuit")?;
            drive(
                dbsp,
                EagerWriter(handle),
                args,
                records,
                temp.path(),
                &args.profile_dir,
                "eager",
            )
        }
    }?;

    if args.verify {
        let held = weights.load(Ordering::Relaxed);
        println!("  final_records={held}");
        if held != records as i64 {
            return Err(anyhow!(
                "the map holds {held} records, not the {records} it was written; \
                 the two maps are not doing the same work and the timings do not compare"
            ));
        }
    }

    Ok(timing)
}

/// Attaches whatever reads the delta stream.
///
/// Without `--verify` that is nothing but a sink, since a benchmark should
/// measure the map and not a consumer.  With it, every delta is walked and its
/// weights summed, which leaves the size of the collection the map holds.
fn consume<V: Payload>(
    stream: &dbsp::Stream<RootCircuit, OrdIndexedZSet<i64, V>>,
    verify: bool,
    weights: Arc<AtomicI64>,
) {
    if !verify {
        stream.inspect(|_| {});
        return;
    }
    stream.inspect(move |batch| {
        let mut sum = 0;
        let mut cursor = batch.inner().cursor();
        while cursor.key_valid() {
            while cursor.val_valid() {
                sum += **cursor.weight();
                cursor.step_val();
            }
            cursor.step_key();
        }
        weights.fetch_add(sum, Ordering::Relaxed);
    });
}

/// Writes a batch of key-value pairs to whichever map is under test.
///
/// The two handles do not take the same thing: the lazy map's cannot be sent an
/// `Update`, so it takes an option.
///
/// A batch reaches the circuit by one of two routes.  `write` does the whole
/// job on the caller's thread: build the tuples, hash every key, and copy each
/// one into its worker's buffer.  `stage` does the same work but hands back the
/// partitions instead of delivering them, so it can run wherever the batch was
/// generated and leave the driver only [`StagedBuffers::flush`], which moves
/// one pointer per worker.
trait Writer<V>: Clone + Send + 'static {
    fn write(&mut self, batch: Vec<(i64, V)>);

    fn stage(&self, batch: Vec<(i64, V)>) -> Box<dyn StagedBuffers + Send>;
}

struct LazyWriter<V: Payload>(LazyMapHandle<i64, V>);
struct EagerWriter<V: Payload>(MapHandle<i64, V, V>);

// Derived `Clone` would demand `V: Clone`, which the payloads satisfy but the
// handles do not need.
impl<V: Payload> Clone for LazyWriter<V> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<V: Payload> Clone for EagerWriter<V> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<V> Writer<V> for LazyWriter<V>
where
    V: Payload,
    Tup2<i64, Option<V>>: dbsp::DBData,
{
    fn write(&mut self, batch: Vec<(i64, V)>) {
        let mut pairs: Vec<Tup2<i64, Option<V>>> =
            batch.into_iter().map(|(k, v)| Tup2(k, Some(v))).collect();
        self.0.append(&mut pairs);
    }

    fn stage(&self, batch: Vec<(i64, V)>) -> Box<dyn StagedBuffers + Send> {
        let pairs: VecDeque<Tup2<i64, Option<V>>> =
            batch.into_iter().map(|(k, v)| Tup2(k, Some(v))).collect();
        Box::new(self.0.stage([pairs]))
    }
}

impl<V> Writer<V> for EagerWriter<V>
where
    V: Payload,
    Tup2<i64, Update<V, V>>: dbsp::DBData,
{
    fn write(&mut self, batch: Vec<(i64, V)>) {
        let mut pairs: Vec<Tup2<i64, Update<V, V>>> = batch
            .into_iter()
            .map(|(k, v)| Tup2(k, Update::Insert(v)))
            .collect();
        self.0.append(&mut pairs);
    }

    fn stage(&self, batch: Vec<(i64, V)>) -> Box<dyn StagedBuffers + Send> {
        let pairs: VecDeque<Tup2<i64, Update<V, V>>> = batch
            .into_iter()
            .map(|(k, v)| Tup2(k, Update::Insert(v)))
            .collect();
        Box::new(self.0.stage([pairs]))
    }
}

/// A batch on its way from a generator thread to the circuit.
enum Load<V> {
    /// Generated and nothing more: the driver turns it into tuples and
    /// partitions it, between steps, on one thread.
    Raw(Vec<(i64, V)>),
    /// Already partitioned by the thread that generated it, leaving the driver
    /// the handover alone.
    Staged(Box<dyn StagedBuffers + Send>),
}

/// One load transaction, then the rewriting transactions.
fn drive<V: Payload, W: Writer<V>>(
    mut dbsp: DBSPHandle,
    mut writer: W,
    args: &Args,
    records: u64,
    storage: &Path,
    profile_dir: &Path,
    stage_name: &str,
) -> Result<Timing> {
    let disk = DiskWatch::start(storage.to_path_buf());
    let mut profiles = Vec::new();
    let batches = records.div_ceil(args.load_batch as u64) as usize;
    if let Err(error) = dbsp.enable_cpu_profiler() {
        println!("  cpu profiling unavailable: {error}");
    }
    // Bound what the generator runs ahead by in bytes rather than batches, so
    // that raising `--load-batch` does not raise what is in flight with it: at
    // 1M records a batch is tens of megabytes.
    let batch_bytes = (args.load_batch * (8 + V::BYTES)) as u64;
    let in_flight = ((256 << 20) / batch_bytes.max(1)).clamp(2, 32) as usize;
    let (sender, receiver) = bounded::<Load<V>>(in_flight);
    let datagen = spawn_load_datagen(
        sender,
        records,
        args.load_batch,
        &writer,
        !args.no_staging,
        (!args.sequential_keys).then(|| Shuffle::new(records)),
    );

    // The whole load is one transaction, spread over as many steps as it takes.
    // Feeding it in and committing it are timed apart: the eager map resolves
    // against the integral as it goes, the lazy map when the transaction ends,
    // so the two stages are where the difference between them lives.
    let load_start = Instant::now();
    dbsp.start_transaction().context("cannot open the load")?;
    for batch_index in 0..batches {
        let batch = receiver
            .recv()
            .map_err(|_| anyhow!("the generator stopped early"))?;
        match batch {
            Load::Raw(batch) => writer.write(batch),
            Load::Staged(mut staged) => staged.flush(),
        }
        dbsp.step().context("a load step failed")?;

        if (batch_index + 1) % 100 == 0 {
            let done = ((batch_index + 1) as u64 * args.load_batch as u64).min(records);
            println!(
                "  loaded {done}/{records} ({:.1}%) in {:.1}s",
                done as f64 / records as f64 * 100.0,
                load_start.elapsed().as_secs_f64()
            );
        }
    }
    let ingest = load_start.elapsed();
    let ingest_cpu = profile_stage(
        &mut dbsp,
        profile_dir,
        &format!("{stage_name}-ingest"),
        &mut profiles,
    )?;
    println!(
        "  ingest done in {:.3}s, {:.2} GiB on disk, {ingest_cpu:.1} CPU-seconds",
        ingest.as_secs_f64(),
        dir_bytes(storage) as f64 / (1 << 30) as f64
    );

    if args.stop_after == Stage::Ingest {
        let final_bytes = dir_bytes(storage);
        let peak_bytes = disk.peak().max(final_bytes);
        drop(disk);
        dbsp.kill()
            .map_err(|_| anyhow!("cannot stop the runtime"))?;
        return Ok(Timing {
            ingest,
            commit: Duration::ZERO,
            ingest_cpu,
            commit_cpu: 0.0,
            transactions_cpu: 0.0,
            transactions: Vec::new(),
            profiles,
            loaded_bytes: final_bytes,
            final_bytes,
            peak_bytes,
        });
    }

    let commit_start = Instant::now();
    dbsp.commit_transaction()
        .context("cannot commit the load")?;
    let commit = commit_start.elapsed();
    let cumulative_cpu = profile_stage(
        &mut dbsp,
        profile_dir,
        &format!("{stage_name}-commit"),
        &mut profiles,
    )?;
    let commit_cpu = cumulative_cpu - ingest_cpu;
    let loaded_bytes = dir_bytes(storage);
    println!(
        "  commit done in {:.3}s, {:.2} GiB on disk, {commit_cpu:.1} CPU-seconds",
        commit.as_secs_f64(),
        loaded_bytes as f64 / (1 << 30) as f64
    );

    for handle in datagen {
        handle
            .join()
            .map_err(|_| anyhow!("a generator thread panicked"))??;
    }

    if args.stop_after == Stage::Commit {
        let peak_bytes = disk.peak().max(loaded_bytes);
        drop(disk);
        dbsp.kill()
            .map_err(|_| anyhow!("cannot stop the runtime"))?;
        return Ok(Timing {
            ingest,
            commit,
            ingest_cpu,
            commit_cpu,
            transactions_cpu: 0.0,
            transactions: Vec::new(),
            profiles,
            loaded_bytes,
            final_bytes: loaded_bytes,
            peak_bytes,
        });
    }

    // Then transactions that rewrite keys the load already wrote.
    let mut rng = ChaCha8Rng::seed_from_u64(args.seed ^ 0x5DEE_CE66);
    let mut transactions = Vec::with_capacity(args.transactions);
    for index in 0..args.transactions {
        let batch: Vec<(i64, V)> = (0..args.transaction_batch)
            .map(|_| {
                let key = rng.gen_range(0..records) as i64;
                (key, V::new(index as u64 + 1, key as u64))
            })
            .collect();

        writer.write(batch);
        let start = Instant::now();
        dbsp.transaction().context("a transaction failed")?;
        transactions.push(start.elapsed());

        if (index + 1) % 100 == 0 {
            println!("  {} transactions done", index + 1);
        }
    }

    // The run's last profile: what the whole thing cost, transactions included.
    let transactions_cpu = profile_stage(
        &mut dbsp,
        profile_dir,
        &format!("{stage_name}-final"),
        &mut profiles,
    )? - cumulative_cpu;
    println!("  transactions done, {transactions_cpu:.1} CPU-seconds");

    let final_bytes = dir_bytes(storage);
    let peak_bytes = disk.peak().max(final_bytes).max(loaded_bytes);
    drop(disk);

    dbsp.kill()
        .map_err(|_| anyhow!("cannot stop the runtime"))?;
    Ok(Timing {
        ingest,
        commit,
        ingest_cpu,
        commit_cpu,
        transactions_cpu,
        transactions,
        profiles,
        loaded_bytes,
        final_bytes,
        peak_bytes,
    })
}

/// Generates the load off the timed thread, so the measurement is the circuit's
/// and not the generator's.
///
/// Keys are dealt out one batch per thread in turn, so every key in the load is
/// distinct and no thread has to coordinate with another.
fn spawn_load_datagen<V: Payload, W: Writer<V>>(
    sender: Sender<Load<V>>,
    records: u64,
    batch_size: usize,
    writer: &W,
    staging: bool,
    shuffle: Option<Shuffle>,
) -> Vec<JoinHandle<Result<()>>> {
    let batches = records.div_ceil(batch_size as u64);
    let threads = (batches as usize).clamp(1, 8);
    let mut handles = Vec::with_capacity(threads);

    for thread in 0..threads {
        let sender = sender.clone();
        // Each thread partitions with a handle of its own.  Staging touches no
        // shared state: it reads the partition count and the hash function and
        // fills buffers it allocated itself.
        let writer = writer.clone();
        handles.push(thread::spawn(move || -> Result<()> {
            let mut index = thread as u64;
            while index < batches {
                let first = index * batch_size as u64;
                let last = (first + batch_size as u64).min(records);
                let batch: Vec<(i64, V)> = (first..last)
                    .map(|index| {
                        let key = shuffle.map_or(index, |shuffle| shuffle.key(index));
                        (key as i64, V::new(0, key))
                    })
                    .collect();
                let load = if staging {
                    Load::Staged(writer.stage(batch))
                } else {
                    Load::Raw(batch)
                };
                sender
                    .send(load)
                    .map_err(|_| anyhow!("the circuit stopped reading"))?;
                index += threads as u64;
            }
            Ok(())
        }));
    }

    drop(sender);
    handles
}
