//! What does a spine's merging cost when its batches do not fit in the cache?
//!
//! `splice_copy` measures one `ListMerger` call with a warm cache and no
//! competing reader, which is where copying bytes instead of re-encoding them
//! looks purely like a CPU saving.  A pipeline's spine is not that: it merges
//! across levels while the worker that feeds it also probes it, and it reads
//! its inputs from storage through a cache far smaller than the data.  There
//! the CPU a merge saves can turn into time waiting for a read instead, and
//! only wall time per merge step shows it.
//!
//! So this benchmark drives a real `Spine` with the runtime's merger threads,
//! pins the cache to a fraction of the data, and interleaves probes with the
//! inserts.
//!
//! The headline is what the probing costs the foreground thread: its wall time
//! less the CPU it used is the time it spent blocked, which is what a worker
//! reports as storage-read wait, and what a merge running alongside it
//! competes for.  The per-level merge figures come below it, because a merge
//! that has got cheaper is only worth having if the threads reading the spine
//! do not pay for it.
//!
//! A caveat on every wait here: it is wall time less thread CPU time, so it
//! counts being descheduled as well as blocking on a read.  Keep
//! `--merger-threads` at or below the spare core count, and compare against a
//! run whose cache holds all the data, to see what the scheduling floor is.
//!
//! # Making a read actually block
//!
//! Storage reads go through the operating system's page cache by default, so
//! on a machine with room to cache the whole run nothing blocks: a miss in the
//! spine's own cache costs a copy and a decompression, which is CPU, and the
//! blocked figure stays at zero however small `--cache-mib` is.  What the
//! pipeline sees is different only because its data is far larger than its
//! host's memory.
//!
//! `--direct` asks for `O_DIRECT`, which takes the page cache out of the way,
//! and is what makes the foreground blocked time meaningful.  It is a Linux
//! flag: elsewhere the option is accepted and does nothing, and the only other
//! route to a blocking read is data larger than memory.
//!
//! The shape is the one that regressed on a procore backfill: a key that is a
//! small tuple of integers, and a value that is a wide record of
//! variable-length strings.  Copying has the most to save on values like
//! these, and a merge of them moves the most bytes.
//!
//! The run is in three phases, because charging them together is what makes a
//! spine that merges harder look slower:
//!
//! * `insert`, which feeds the batches and probes between them.
//! * `quiesce`, which waits for the merging those inserts set going.  A
//!   cheaper merge step lets a spine get further through the hierarchy in the
//!   same insert time, and this is where that shows -- either as reaching a
//!   consolidated spine sooner, or as more merging to get there.
//! * `drain`, the final merge of whatever is left into one batch.
//!
//! `insert` and `quiesce` are the steady-state cost and are what to compare.
//! `drain` is an end-of-run artifact: it merges batches the spine would have
//! merged anyway, so an arm that had merged more already is charged less for
//! it, and an arm still merging is charged for the wait.  The spine's state at
//! quiescence is reported with the times, so that two arms can be compared at
//! the consolidation they actually reached rather than assumed to be equal.
//!
//! One other number decides whether the run means anything at all:
//! `cache/data` has to be small.  A run whose data fits in the cache measures
//! the CPU and nothing else, which is what `splice_copy` is for.
//!
//! # Reaching the levels that matter
//!
//! A spine assigns a batch to a level by its record count, on a decimal
//! scale: level 2 holds a hundred thousand records, level 3 a million, level 4
//! ten million, level 5 a hundred million.  This spine is an integral's, which
//! waits until ten batches have gathered at a level and then merges them, so
//! the merged batch is ten times the size and lands one level up; each level
//! fills at a tenth the rate of the one below.
//!
//! The merges worth measuring are the ones at level 4 and above, where a
//! backfill spends its time, and the arithmetic says what reaching them
//! costs.  At a million records an insert, ten inserts make a level-3 merge,
//! whose ten million records are a level-4 batch, and a hundred inserts make
//! the first level-4 merge, of a hundred million records.  The default of 150
//! inserts carries the run past it, so that the level-4 merge runs alongside
//! inserts and probes, as it would in a pipeline, rather than alone in the
//! quiesce phase.  `--merge-threshold-batches 3` merges three batches at a
//! time instead, which reaches level 4 sooner but merges each record more
//! often on the way.
//!
//! A level-5 merge wants ten batches of a hundred million records, a billion
//! records in the run, which is the pipeline's scale rather than a
//! benchmark's.  Level 4 is the ceiling for a run that finishes in minutes.
//!
//! Records decide the level and bytes decide the reading, and they are set
//! apart from each other.  The default value is narrow, to reach level 4 in
//! about eleven gigabytes of storage; `--value-bytes 300` is the width a join
//! index carries and is what to ask for when the question is what a merge
//! costs per record.
//!
//! ```text
//! cargo bench -p dbsp --bench spine_merge -- [--keys N] [--values N]
//!     [--batches N] [--value-bytes N] [--cache-mib N] [--merger-threads N]
//!     [--probes N] [--merge-threshold-batches N] [--quiesce-secs N]
//!     [--level0-records N] [--direct]
//! ```

use dbsp::circuit::metadata::{
    COMPLETED_MERGES, LOOSE_BATCHES_COUNT, LOOSE_MEMORY_RECORDS_COUNT, LOOSE_STORAGE_RECORDS_COUNT,
    MERGING_BATCHES_COUNT, MetaItem, OperatorMeta, SPINE_BATCHES_COUNT,
};
use dbsp::circuit::{CircuitConfig, CircuitStorageConfig, ElapsedTime};
use dbsp::dynamic::Erase;
use dbsp::{
    OrdIndexedZSet, Runtime, ZWeight,
    trace::{
        BatchReader as DynBatchReader, BatchReaderFactories, Cursor as DynCursor, Spine, Trace,
        TraceRole,
    },
    typed_batch::BatchReader as TypedBatchReader,
    utils::{Tup2, Tup3, Tup4},
};
use feldera_types::config::{StorageCacheConfig, StorageConfig, StorageOptions};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::tempdir;

/// A key of the shape a join index has: the identifiers a row is keyed by.
type Key = Tup3<i64, i64, i64>;

/// A value of the shape such an index carries: a wide record whose columns are
/// mostly strings, one of them absent for some rows.
type Val = Tup4<String, String, Option<String>, i64>;

type TypedBatchType = OrdIndexedZSet<Key, Val>;
type Inner = <TypedBatchType as TypedBatchReader>::Inner;

#[derive(Clone)]
struct Args {
    keys: usize,
    values: usize,
    batches: usize,
    value_bytes: usize,
    cache_mib: usize,
    merger_threads: usize,
    probes: usize,
    merge_threshold_batches: Option<usize>,
    quiesce_secs: u64,
    level0_records: Option<usize>,
    direct: bool,
}

impl Args {
    fn parse() -> Self {
        let mut args = Self {
            keys: 1_000_000,
            values: 1,
            batches: 150,
            value_bytes: 64,
            cache_mib: 128,
            merger_threads: 8,
            probes: 25_000,
            merge_threshold_batches: None,
            quiesce_secs: 600,
            level0_records: None,
            direct: false,
        };
        let argv: Vec<String> = std::env::args().collect();
        let mut i = 1;
        let number = |i: &mut usize| -> usize {
            *i += 1;
            argv[*i]
                .parse()
                .unwrap_or_else(|_| panic!("{} wants a number", argv[*i - 1]))
        };
        while i < argv.len() {
            match argv[i].as_str() {
                "--keys" => args.keys = number(&mut i),
                "--values" => args.values = number(&mut i),
                "--batches" => args.batches = number(&mut i),
                "--value-bytes" => args.value_bytes = number(&mut i),
                "--cache-mib" => args.cache_mib = number(&mut i),
                "--merger-threads" => args.merger_threads = number(&mut i),
                "--probes" => args.probes = number(&mut i),
                "--merge-threshold-batches" => args.merge_threshold_batches = Some(number(&mut i)),
                "--quiesce-secs" => args.quiesce_secs = number(&mut i) as u64,
                "--level0-records" => args.level0_records = Some(number(&mut i)),
                "--direct" => args.direct = true,
                "--bench" => {}
                other => panic!("unknown argument {other}"),
            }
            i += 1;
        }
        assert!(args.batches > 0, "--batches wants at least one");
        assert!(args.values > 0, "--values wants at least one");
        args
    }

    /// Records in one batch.
    fn records(&self) -> usize {
        self.keys * self.values
    }
}

/// The key of record `n`, spread over three identifier columns.
fn key(n: usize) -> Key {
    Tup3(
        (n % 97) as i64,
        (n % 1021) as i64,
        (n / 1021) as i64 + n as i64,
    )
}

/// The value of record `n`, about `bytes` long and of a length that varies
/// from one record to the next, so a run of them has no constant stride.
fn val(n: usize, bytes: usize) -> Val {
    let width = bytes / 3 + n % 17;
    Tup4(
        format!("{n:0width$}", width = width),
        format!("/{}/project/item/{}", n % 1021, n),
        // Absent for one row in five, as a nullable column is.
        (!n.is_multiple_of(5)).then(|| format!("{:0width$}", n, width = width / 2)),
        n as i64,
    )
}

/// The level a batch of `records` records lands at.
///
/// The spine decides this by record count on a decimal scale, so how far up
/// the levels a run reaches follows from how long its inserts are.
///
/// # Arguments
///
/// * `records` - how many records the batch holds.
///
/// # Returns
///
/// The level, on the scale the spine uses.
fn level_of(records: usize) -> usize {
    match records {
        0..=99_999 => 1,
        100_000..=999_999 => 2,
        1_000_000..=9_999_999 => 3,
        10_000_000..=99_999_999 => 4,
        100_000_000..=999_999_999 => 5,
        _ => 6,
    }
}

/// One batch of records, written to storage.
fn batch(args: &Args, seq: usize) -> Inner {
    let base = seq * args.records();
    let tuples: Vec<Tup2<Tup2<Key, Val>, ZWeight>> = (0..args.records())
        .map(|i| {
            let n = base + i;
            Tup2(Tup2(key(n), val(n, args.value_bytes)), 1)
        })
        .collect();
    TypedBatchType::from_tuples((), tuples).into_inner()
}

/// The merge statistics a spine reports, per level.
#[derive(Default, Clone, Copy)]
struct Level {
    merges: usize,
    batches: usize,
    steps: usize,
    cpu: Duration,
    wall: Duration,
}

/// Reads the per-level merge statistics out of a spine.
///
/// # Arguments
///
/// * `spine` - the spine to read.
///
/// # Returns
///
/// One entry per level the spine has merged at, indexed by level.
fn levels(spine: &Spine<Inner>) -> Vec<Level> {
    let mut meta = OperatorMeta::default();
    spine.metadata(&mut meta);
    let mut out = vec![Level::default(); 16];
    for (labels, item) in meta.readings(&COMPLETED_MERGES) {
        let slot: usize = labels
            .iter()
            .find(|(name, _)| name == "slot")
            .and_then(|(_, v)| v.parse().ok())
            .unwrap_or(0);
        let MetaItem::Map(map) = item else { continue };
        let count = |k: &str| match map.get(k) {
            Some(MetaItem::Count(n)) => *n,
            Some(MetaItem::Int(n)) => *n,
            _ => 0,
        };
        let dur = |k: &str| match map.get(k) {
            Some(MetaItem::Duration(d)) => *d,
            _ => Duration::ZERO,
        };
        if slot >= out.len() {
            out.resize(slot + 1, Level::default());
        }
        let steps = count("steps");
        out[slot] = Level {
            merges: count("merges"),
            batches: count("batches"),
            steps,
            // The spine reports averages per step; totals are what add up.
            cpu: dur("avg_step_cpu_seconds") * steps as u32,
            wall: dur("avg_step_seconds") * steps as u32,
        };
    }
    out
}

/// What a spine holds, so that two arms can be compared at the consolidation
/// they reached rather than assumed to have reached the same one.
struct State {
    batches: usize,
    merging: usize,
    records: usize,
    per_level: Vec<usize>,
}

/// Sums the readings of one metric over every level.
fn total(meta: &OperatorMeta, id: dbsp::circuit::metadata::MetricId) -> usize {
    meta.readings(&id)
        .filter_map(|(_, item)| match item {
            MetaItem::Count(n) | MetaItem::Int(n) => Some(*n),
            _ => None,
        })
        .sum()
}

/// Reads what the spine currently holds.
///
/// # Arguments
///
/// * `spine` - the spine to read.
///
/// # Returns
///
/// Its batch counts, records and bytes, and its batches by level.
fn state_of(spine: &Spine<Inner>) -> State {
    let mut meta = OperatorMeta::default();
    spine.metadata(&mut meta);
    let mut per_level = vec![0usize; 16];
    for (labels, item) in meta.readings(&LOOSE_BATCHES_COUNT) {
        let slot: usize = labels
            .iter()
            .find(|(name, _)| name == "slot")
            .and_then(|(_, v)| v.parse().ok())
            .unwrap_or(0);
        if let MetaItem::Count(n) = item
            && slot < per_level.len()
        {
            per_level[slot] = *n;
        }
    }
    State {
        batches: total(&meta, SPINE_BATCHES_COUNT),
        merging: total(&meta, MERGING_BATCHES_COUNT),
        records: total(&meta, LOOSE_STORAGE_RECORDS_COUNT)
            + total(&meta, LOOSE_MEMORY_RECORDS_COUNT),
        per_level,
    }
}

/// Waits for the merging the inserts set going to finish.
///
/// Measuring the final merge with a wait in front of it charges an arm that is
/// still merging for work the other has already done, so the wait is taken
/// here, on its own, and reported as its own phase.
///
/// # Arguments
///
/// * `spine` - the spine to wait on.
/// * `timeout` - how long to wait before giving up.
///
/// # Returns
///
/// Whether the spine went quiet before the timeout.
fn wait_until_quiet(spine: &Spine<Inner>, timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if state_of(spine).merging == 0 {
            return true;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    false
}

/// Probes the spine for `n` keys spread over what it holds.
///
/// The probes are what a worker does to a trace it also feeds, and they are
/// the reads whose waiting the merging competes with.
///
/// # Arguments
///
/// * `spine` - the spine to probe.
/// * `n` - how many keys to look up.
/// * `upto` - the highest record number written so far.
///
/// # Returns
///
/// How many of the probed keys were found, which the caller keeps so that the
/// probing cannot be optimized away.
fn probe(spine: &Spine<Inner>, n: usize, upto: usize) -> usize {
    if n == 0 || upto == 0 {
        return 0;
    }
    let mut found = 0;
    for i in 0..n {
        // Scattered, not strided: consecutive keys sit in the same block, so a
        // walk in key order is served from the cache and measures nothing.  A
        // fresh cursor per probe is what a lookup costs, rather than what
        // continuing an existing scan costs.
        let row = (i as u64).wrapping_mul(0x9E3779B97F4A7C15) % upto as u64;
        let mut k = key(row as usize);
        let mut cursor = spine.cursor();
        cursor.seek_key(k.erase_mut());
        if cursor.key_valid() {
            found += 1;
        }
    }
    found
}

fn main() {
    let args = Args::parse();
    let temp = tempdir().expect("failed to create temp directory");
    let mut config = CircuitConfig::with_workers(1).with_storage(Some(
        CircuitStorageConfig::for_config(
            StorageConfig {
                path: temp.path().to_string_lossy().into_owned(),
                cache: if args.direct {
                    // `O_DIRECT` on Linux, so a read reaches the device and
                    // the thread making it actually blocks.
                    StorageCacheConfig::FelderaCache
                } else {
                    StorageCacheConfig::PageCache
                },
            },
            StorageOptions {
                // Everything to storage, and a cache far smaller than the
                // data, so that a merge actually reads the device.
                min_storage_bytes: Some(0),
                min_step_storage_bytes: Some(0),
                cache_mib: Some(args.cache_mib),
                ..StorageOptions::default()
            },
        )
        .expect("failed to configure POSIX storage"),
    ));
    // The spine is built as an integral, so that is the threshold to set.
    config.dev_tweaks.integral_merge_threshold_batches =
        args.merge_threshold_batches.map(|n| n as u16);
    config.dev_tweaks.merger_threads = Some(args.merger_threads as u16);
    config.dev_tweaks.max_level0_batch_size_records = args.level0_records.map(|n| n as u16);

    let handle = Runtime::run(config, move |_parker| {
        let factories = BatchReaderFactories::new::<Key, Val, ZWeight>();
        let mut spine = Spine::<Inner>::new(
            &factories,
            Arc::new(String::from("spine_merge")),
            TraceRole::Integral,
        );

        let mut written = 0;
        let mut found = 0;
        // Timed apart, so that the probing -- the foreground cost this is for
        // -- is not hidden inside the building and the inserting.
        let mut building = ElapsedTime::default();
        let mut inserting = ElapsedTime::default();
        let mut probing = ElapsedTime::default();
        let start = Instant::now();
        for seq in 0..args.batches {
            let b = building.record(|| batch(&args, seq));
            inserting.record(|| {
                spine.insert_without_blocking(b);
            });
            written += args.records();
            found += probing.record(|| probe(&spine, args.probes, written));
        }
        let inserted = start.elapsed();

        // Wait out the merging the inserts set going before measuring the
        // final merge, so that an arm still merging is not charged for the
        // wait inside the drain.
        let quiesce = Instant::now();
        let quiet = wait_until_quiet(&spine, Duration::from_secs(args.quiesce_secs));
        let quiescing = quiesce.elapsed();

        // What each arm actually reached, which is what makes the times
        // comparable.
        let state = state_of(&spine);

        let drain = Instant::now();
        spine.complete_merges();
        let drained = drain.elapsed();

        let levels = levels(&spine);
        let records = args.batches * args.records();
        println!(
            "{} batches x {} keys x {} values = {} records ({} per insert, \
             level {}), ~{} B/value, cache {} MiB, {} merger threads, \
             {} probes/batch ({found} found)",
            args.batches,
            args.keys,
            args.values,
            records,
            args.records(),
            level_of(args.records()),
            args.value_bytes,
            args.cache_mib,
            args.merger_threads,
            args.probes,
        );
        // Building the data is the benchmark's own cost and belongs outside
        // any figure being compared.
        println!(
            "  phases: build {:.1}s  insert {:.1}s  probe {:.1}s  quiesce {:.1}s{}  \
             (drain {:.1}s, end-of-run only)",
            building.real.as_secs_f64(),
            inserting.real.as_secs_f64(),
            probing.real.as_secs_f64(),
            quiescing.as_secs_f64(),
            if quiet { "" } else { " QUIESCE TIMED OUT" },
            drained.as_secs_f64(),
        );
        let _ = inserted;
        let levels_held: Vec<String> = state
            .per_level
            .iter()
            .enumerate()
            .filter(|(_, n)| **n > 0)
            .map(|(level, n)| format!("L{level}:{n}"))
            .collect();
        // What the foreground thread paid.  The probes are the reads a worker
        // makes into a trace it also feeds, and the time they block is what a
        // merge running alongside them takes away.
        let probes = args.batches * args.probes;
        let blocked = probing.real.saturating_sub(probing.cpu);
        println!(
            "  foreground probes: {}  wall {:.1}s  cpu {:.1}s  blocked {:.1}s  \
             ({:.1} us/probe, {:.1} us blocked)",
            probes,
            probing.real.as_secs_f64(),
            probing.cpu.as_secs_f64(),
            blocked.as_secs_f64(),
            probing.real.as_secs_f64() * 1e6 / probes as f64,
            blocked.as_secs_f64() * 1e6 / probes as f64,
        );
        println!(
            "  foreground other: building {:.1}s  inserting {:.1}s (blocked {:.1}s)",
            building.real.as_secs_f64(),
            inserting.real.as_secs_f64(),
            inserting.real.saturating_sub(inserting.cpu).as_secs_f64(),
        );
        println!(
            "  at quiescence: {} batches ({}), {} records, {} still merging",
            state.batches,
            levels_held.join(" "),
            state.records,
            state.merging,
        );
        println!(
            "  {:<5}{:>9}{:>9}{:>10}{:>12}{:>12}{:>12}",
            "level", "merges", "steps", "batches", "cpu/step", "wall/step", "wait/step",
        );
        let mut cpu = Duration::ZERO;
        let mut wall = Duration::ZERO;
        for (level, l) in levels.iter().enumerate() {
            if l.steps == 0 {
                continue;
            }
            cpu += l.cpu;
            wall += l.wall;
            let per = |d: Duration| d.as_secs_f64() * 1e3 / l.steps as f64;
            println!(
                "  {:<5}{:>9}{:>9}{:>10}{:>9.1} ms{:>9.1} ms{:>9.1} ms",
                level,
                l.merges,
                l.steps,
                l.batches,
                per(l.cpu),
                per(l.wall),
                per(l.wall) - per(l.cpu),
            );
        }
        // The headline: merge CPU is what the copying saves, and merge wall is
        // what the pipeline actually waits for.  A change that moves the first
        // without the second has not made the spine faster.
        println!(
            "  merge totals: cpu {:.1}s  wall {:.1}s  wait {:.1}s",
            cpu.as_secs_f64(),
            wall.as_secs_f64(),
            (wall - cpu).as_secs_f64(),
        );
    })
    .expect("failed to start the runtime");
    handle.join().unwrap();
}
