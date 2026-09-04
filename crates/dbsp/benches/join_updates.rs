//! What do updates on both sides of a join do to the spines it keeps?
//!
//! An update retracts a record's old value and inserts its new one, so each
//! update reaches the join's output as negative weights for the old joined
//! records and positive ones for the new: the integral of the output
//! accumulates retractions that cancel only when a merge brings them together
//! with the records they retract.
//!
//! The join is on a key of three integers, and each left record matches
//! exactly one right record, each right record `--left / --right` left
//! records.  Every record carries a payload of about two hundred bytes of
//! numbers and text, and the join's output, a payload of the same size from
//! both sides, goes into an integral.  The run has three phases:
//!
//! 1. A backfill in one transaction: `--left` records on the left and
//!    `--right` on the right, both fed `--batch` at a time.
//! 2. `--right-updates` transactions, each updating `--right-update-size`
//!    right records.
//! 3. `--left-updates` transactions, each updating `--left-update-size` left
//!    records.
//!
//! Each phase reports its wall time, the CPU time of the circuit's workers
//! (foreground) and of everything else (background, mostly the mergers), peak
//! memory and storage.  After each phase, the run waits for the mergers to
//! finish the merging the phase left them, and reports the wait as a phase of
//! its own, so that each phase is charged only for the merging done while it
//! ran.  At each progress report, and before and after each wait, the run
//! takes a snapshot of how many batches, records and negative weights each
//! spine holds at each level.  The run checks that every phase's output holds
//! what its updates should produce.
//!
//! ```text
//! cargo bench -p dbsp --bench join_updates -- --dir PATH
//!     [--left N] [--right N] [--batch N]
//!     [--right-updates N] [--right-update-size N]
//!     [--left-updates N] [--left-update-size N]
//!     [--workers N] [--max-rss-gib N] [--datagen-threads N] [--dev-tweaks JSON]
//!     [--profiles DIR]
//! ```
//!
//! `--profiles` keeps the circuit profile of every snapshot in `DIR`.

#[allow(dead_code)]
#[path = "support/measure.rs"]
mod measure;

use dbsp::{
    Runtime, ZWeight,
    typed_batch::DynBatchReader,
    utils::{Tup2, Tup3, Tup4},
};
use measure::{Chunks, Mark, Payload, PhaseCost, Sampler, Snapshots, SpineLayout, mix, payload};
use serde_json::json;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// The join key: three integers.
type Key = Tup3<i64, i64, i64>;

/// One input record: its key, and its payload with a weight.
type Record = Tup2<Key, Tup2<Payload, ZWeight>>;

/// Right records' payloads are numbered from here, apart from the left's.
const RIGHT_IDS: u64 = 1 << 40;

/// A prime that steps through the records an update phase picks, so that no
/// record is picked twice.
const STRIDE: u64 = 2_147_483_647;

struct Args {
    left: u64,
    right: u64,
    batch: u64,
    right_updates: u64,
    right_update_size: u64,
    left_updates: u64,
    left_update_size: u64,
    workers: usize,
    max_rss_gib: u64,
    datagen_threads: usize,
    dev_tweaks: Option<String>,
    dir: Option<PathBuf>,
    profiles: Option<PathBuf>,
}

impl Args {
    fn parse() -> Self {
        let mut args = Self {
            left: 1_000_000_000,
            right: 100_000_000,
            batch: 100_000,
            right_updates: 1_000,
            right_update_size: 1_000,
            left_updates: 1_000,
            left_update_size: 100_000,
            workers: 16,
            max_rss_gib: 64,
            datagen_threads: 8,
            dev_tweaks: None,
            dir: None,
            profiles: None,
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
                "--left" => args.left = number(value(&mut i)),
                "--right" => args.right = number(value(&mut i)),
                "--batch" => args.batch = number(value(&mut i)),
                "--right-updates" => args.right_updates = number(value(&mut i)),
                "--right-update-size" => args.right_update_size = number(value(&mut i)),
                "--left-updates" => args.left_updates = number(value(&mut i)),
                "--left-update-size" => args.left_update_size = number(value(&mut i)),
                "--workers" => args.workers = number(value(&mut i)) as usize,
                "--max-rss-gib" => args.max_rss_gib = number(value(&mut i)),
                "--datagen-threads" => args.datagen_threads = number(value(&mut i)) as usize,
                "--dev-tweaks" => args.dev_tweaks = Some(value(&mut i)),
                "--dir" => args.dir = Some(PathBuf::from(value(&mut i))),
                "--profiles" => args.profiles = Some(PathBuf::from(value(&mut i))),
                "--bench" => {}
                other => panic!("unknown argument {other}"),
            }
            i += 1;
        }
        assert!(
            args.right > 0 && args.left.is_multiple_of(args.right),
            "--left has to be a whole number of times --right"
        );
        assert!(
            args.batch > 0 && args.right.is_multiple_of(args.batch),
            "--right, and so --left, has to be a whole number of batches"
        );
        assert!(
            args.right_updates * args.right_update_size <= args.right
                && args.left_updates * args.left_update_size <= args.left,
            "an update phase cannot update more records than there are"
        );
        assert!(
            gcd(STRIDE, args.left) == 1 && gcd(STRIDE, args.right) == 1,
            "the record counts must not share a factor with the stride that picks updates"
        );
        args
    }

    /// How many left records each right record matches.
    fn fanout(&self) -> u64 {
        self.left / self.right
    }
}

fn gcd(a: u64, b: u64) -> u64 {
    if b == 0 { a } else { gcd(b, a % b) }
}

/// The key of right record `right`, which the left records it matches share.
///
/// Its first integer is mixed, so that keys made together spread across the
/// whole key space, as they would from any real source.
fn key(right: u64) -> Key {
    Tup3(
        mix(right) as i64,
        right as i64,
        mix(right ^ 0xA5A5_A5A5) as i64,
    )
}

/// Left record `left` at `version`, with `weight`.
fn left_record(left: u64, fanout: u64, version: u64, weight: ZWeight) -> Record {
    Tup2(key(left / fanout), Tup2(payload(left, version), weight))
}

/// Right record `right` at `version`, with `weight`.
fn right_record(right: u64, version: u64, weight: ZWeight) -> Record {
    Tup2(
        key(right),
        Tup2(payload(RIGHT_IDS + right, version), weight),
    )
}

/// The `index`th record an update phase over `count` records picks.
fn picked(index: u64, count: u64) -> u64 {
    ((index as u128 * STRIDE as u128) % count as u128) as u64
}

/// The updates of transaction `transaction`: each of its records retracted at
/// its first version and inserted at its second.
fn updates(
    transaction: u64,
    size: u64,
    count: u64,
    record: impl Fn(u64, u64, ZWeight) -> Record,
) -> Vec<Record> {
    let mut records = Vec::with_capacity(2 * size as usize);
    for index in transaction * size..(transaction + 1) * size {
        let id = picked(index, count);
        records.push(record(id, 0, -1));
        records.push(record(id, 1, 1));
    }
    records
}

/// How a run of transactions' times were spread, in milliseconds.
fn step_times(walls: &[f64]) -> serde_json::Value {
    if walls.is_empty() {
        return json!(null);
    }
    let mut sorted = walls.to_vec();
    sorted.sort_by(f64::total_cmp);
    let at = |q: f64| sorted[((sorted.len() - 1) as f64 * q).round() as usize] * 1e3;
    json!({
        "mean": walls.iter().sum::<f64>() / walls.len() as f64 * 1e3,
        "p50": at(0.5), "p90": at(0.9), "p99": at(0.99), "max": at(1.0),
    })
}

/// The spread of a run of transactions' times, for the report.
fn describe(times: &serde_json::Value) -> String {
    let ms = |key: &str| times[key].as_f64().unwrap_or(f64::NAN);
    format!(
        "mean {:.1}, p50 {:.1}, p90 {:.1}, p99 {:.1}, max {:.1} ms",
        ms("mean"),
        ms("p50"),
        ms("p90"),
        ms("p99"),
        ms("max")
    )
}

/// Runs `transactions` transactions of updates on one side of the join.
///
/// Every 100 transactions it reports progress and takes a snapshot, except
/// after the last one, which the phase's own snapshot follows.
///
/// # Arguments
///
/// * `dbsp` - the circuit.
/// * `side` - the input the updates go to.
/// * `chunks` - each transaction's updates.
/// * `outputs` - counts the records the join outputs.
/// * `expected` - how many records each transaction should output.
/// * `label` - what to call the phase in progress reports and snapshots.
/// * `snapshots` - the run's snapshots.
///
/// # Returns
///
/// Each transaction's wall time, and how many output other than expected.
fn run_updates(
    dbsp: &mut dbsp::DBSPHandle,
    side: &dbsp::IndexedZSetHandle<Key, Payload>,
    chunks: Chunks<Vec<Record>>,
    outputs: &AtomicU64,
    expected: u64,
    label: &str,
    snapshots: &mut Snapshots,
) -> (Vec<f64>, usize) {
    let transactions = chunks.len();
    let mut walls = Vec::new();
    let mut wrong = 0;
    let started = Instant::now();
    for (index, mut records) in chunks.enumerate() {
        let begun = Instant::now();
        side.append(&mut records);
        dbsp.transaction().expect("a transaction failed");
        walls.push(begun.elapsed().as_secs_f64());
        if outputs.swap(0, Ordering::Relaxed) != expected {
            wrong += 1;
        }
        let done = index + 1;
        if done.is_multiple_of(100) {
            println!(
                "    {label}: {done:>5} transactions in {:>8.1} s, rss {:>5.1} GiB",
                started.elapsed().as_secs_f64(),
                measure::gib(measure::rss_bytes().unwrap_or(0)),
            );
            if done < transactions {
                snapshots.take(dbsp, &format!("{label}: {done} transactions"));
            }
        }
    }
    (walls, wrong)
}

/// Waits for the mergers to finish the merging a phase left them, then takes
/// a snapshot of the settled circuit.
///
/// The wait counts as a phase of its own, so that the phase before it is
/// charged only for the merging done while it ran.
///
/// # Arguments
///
/// * `dbsp` - the circuit.
/// * `sampler` - measures the wait.
/// * `snapshots` - the run's snapshots.
/// * `name` - what to call the wait, as a phase.
/// * `when` - what to call the snapshot.
///
/// # Returns
///
/// What the wait cost, and how the spines are laid out after it.
fn settle(
    dbsp: &mut dbsp::DBSPHandle,
    sampler: &Sampler,
    snapshots: &mut Snapshots,
    name: &str,
    when: &str,
) -> (PhaseCost, Vec<SpineLayout>) {
    let mark = Mark::now();
    measure::settle(dbsp);
    let cost = sampler.end_phase(name, mark);
    (cost, snapshots.take(dbsp, when))
}

fn main() {
    let args = Args::parse();
    let base = args.dir.clone().unwrap_or_else(std::env::temp_dir);
    let temp = tempfile::tempdir_in(&base).expect("failed to create the storage directory");
    let storage = temp.path().to_path_buf();
    if measure::on_tmpfs(&storage) {
        println!(
            "WARNING: {} is held in memory, so the storage is never read from a disk; \
             pass --dir on one",
            base.display()
        );
    }
    let config = measure::circuit_config(
        args.workers,
        &storage,
        args.max_rss_gib,
        args.dev_tweaks.as_deref(),
    );
    let fanout = args.fanout();
    println!(
        "join updates: {} left and {} right records ({fanout} left per right) in batches of {}; \
         then {} x {} right and {} x {} left updates; {} workers, memory limit {} GiB, \
         dev tweaks {}",
        args.left,
        args.right,
        args.batch,
        args.right_updates,
        args.right_update_size,
        args.left_updates,
        args.left_update_size,
        args.workers,
        args.max_rss_gib,
        args.dev_tweaks.as_deref().unwrap_or("default"),
    );

    let outputs = Arc::new(AtomicU64::new(0));
    let (mut dbsp, (left, right)) = {
        let outputs = outputs.clone();
        Runtime::init_circuit(config, move |circuit| {
            let (left, left_input) = circuit.add_input_indexed_zset::<Key, Payload>();
            let (right, right_input) = circuit.add_input_indexed_zset::<Key, Payload>();
            let joined = left.join(&right, |key, left, right| {
                Tup2(*key, Tup4(left.0, right.0, left.2.clone(), right.3))
            });
            let outputs = outputs.clone();
            joined.inspect(move |batch| {
                outputs.fetch_add(batch.approximate_len() as u64, Ordering::Relaxed);
            });
            joined.accumulate_integrate_trace();
            Ok((left_input, right_input))
        })
        .expect("failed to build the circuit")
    };

    let sampler = Sampler::start(storage.clone());
    let mut snapshots = Snapshots::new(args.profiles.clone());
    let mut phases = Vec::new();
    let mut layouts = Vec::new();
    let mut wrong = Vec::new();

    // Phase 1: the backfill, in one transaction.  A right batch goes in with
    // every `fanout`th left batch: the right records the left ones match.
    let batch = args.batch;
    let steps = (args.left / batch) as usize;
    let backfill = Chunks::new(args.datagen_threads, steps, move |index| {
        let first = index as u64 * batch;
        let left: Vec<Record> = (first..first + batch)
            .map(|left| left_record(left, fanout, 0, 1))
            .collect();
        let right: Vec<Record> = if (index as u64).is_multiple_of(fanout) {
            let first = index as u64 / fanout * batch;
            (first..first + batch)
                .map(|right| right_record(right, 0, 1))
                .collect()
        } else {
            Vec::new()
        };
        (left, right)
    });
    let started = Instant::now();
    let mark = Mark::now();
    dbsp.start_transaction()
        .expect("failed to start the backfill");
    for (index, (mut left_batch, mut right_batch)) in backfill.enumerate() {
        left.append(&mut left_batch);
        right.append(&mut right_batch);
        dbsp.step().expect("a backfill step failed");
        if (index + 1).is_multiple_of(1000) {
            let fed = (index as u64 + 1) * batch;
            println!(
                "    backfill: {fed:>11} left records in {:>8.1} s, rss {:>5.1} GiB",
                started.elapsed().as_secs_f64(),
                measure::gib(measure::rss_bytes().unwrap_or(0)),
            );
            snapshots.take(&mut dbsp, &format!("backfill: {fed} left records"));
        }
    }
    let ingest = sampler.end_phase("1 ingest", mark);
    let mark = Mark::now();
    dbsp.commit_transaction()
        .expect("failed to commit the backfill");
    let commit = sampler.end_phase("1 commit", mark);
    if outputs.swap(0, Ordering::Relaxed) != args.left {
        wrong.push("phase 1".to_string());
    }
    phases.push(PhaseCost::combined(
        "phase 1",
        &[ingest.clone(), commit.clone()],
    ));
    layouts.push(("after phase 1", snapshots.take(&mut dbsp, "after phase 1")));
    let (cost, spines) = settle(
        &mut dbsp,
        &sampler,
        &mut snapshots,
        "1 settle",
        "after phase 1 settled",
    );
    phases.push(cost);
    layouts.push(("after phase 1 settled", spines));

    // Phase 2: updates to right records, each matching `fanout` left ones.
    let (count, size) = (args.right, args.right_update_size);
    let chunks = Chunks::new(
        args.datagen_threads,
        args.right_updates as usize,
        move |t| updates(t as u64, size, count, right_record),
    );
    let mark = Mark::now();
    let (right_walls, right_wrong) = run_updates(
        &mut dbsp,
        &right,
        chunks,
        &outputs,
        2 * size * fanout,
        "right updates",
        &mut snapshots,
    );
    phases.push(sampler.end_phase("phase 2", mark));
    layouts.push(("after phase 2", snapshots.take(&mut dbsp, "after phase 2")));
    let (cost, spines) = settle(
        &mut dbsp,
        &sampler,
        &mut snapshots,
        "2 settle",
        "after phase 2 settled",
    );
    phases.push(cost);
    layouts.push(("after phase 2 settled", spines));
    if right_wrong > 0 {
        wrong.push(format!("{right_wrong} transactions of phase 2"));
    }

    // Phase 3: updates to left records, each matching one right record.
    let (count, size) = (args.left, args.left_update_size);
    let chunks = Chunks::new(args.datagen_threads, args.left_updates as usize, move |t| {
        updates(t as u64, size, count, |left, version, weight| {
            left_record(left, fanout, version, weight)
        })
    });
    let mark = Mark::now();
    let (left_walls, left_wrong) = run_updates(
        &mut dbsp,
        &left,
        chunks,
        &outputs,
        2 * size,
        "left updates",
        &mut snapshots,
    );
    phases.push(sampler.end_phase("phase 3", mark));
    layouts.push(("after phase 3", snapshots.take(&mut dbsp, "after phase 3")));
    let (cost, spines) = settle(
        &mut dbsp,
        &sampler,
        &mut snapshots,
        "3 settle",
        "after phase 3 settled",
    );
    phases.push(cost);
    layouts.push(("after phase 3 settled", spines));
    if left_wrong > 0 {
        wrong.push(format!("{left_wrong} transactions of phase 3"));
    }
    drop(sampler);
    dbsp.kill().expect("failed to stop the circuit");

    println!();
    for phase in [&ingest, &commit].into_iter().chain(phases.iter()) {
        phase.print();
    }
    snapshots.print();
    println!(
        "  phase 2 transactions: {}",
        describe(&step_times(&right_walls))
    );
    println!(
        "  phase 3 transactions: {}",
        describe(&step_times(&left_walls))
    );
    for (when, spines) in &layouts {
        measure::print_layouts(when, spines, args.workers);
    }
    if wrong.is_empty() {
        println!("  outputs: every phase output what its updates should produce");
    } else {
        println!("  WRONG: unexpected outputs in {}", wrong.join(", "));
    }

    let phase_costs: Vec<serde_json::Value> = [&ingest, &commit]
        .into_iter()
        .chain(phases.iter())
        .map(PhaseCost::to_json)
        .collect();
    let result = json!({
        "bench": "join_updates",
        "left": args.left,
        "right": args.right,
        "batch": args.batch,
        "right_updates": [args.right_updates, args.right_update_size],
        "left_updates": [args.left_updates, args.left_update_size],
        "workers": args.workers,
        "dev_tweaks": args.dev_tweaks,
        "phases": phase_costs,
        "transaction_ms": {"phase 2": step_times(&right_walls), "phase 3": step_times(&left_walls)},
        "spines": layouts.iter().map(|(when, spines)| json!({"when": when, "spines": measure::layouts_json(spines)})).collect::<Vec<_>>(),
        "snapshots": snapshots.to_json(),
        "wrong": wrong,
    });
    println!("RESULT {result}");
}
