//! What does a sliding window do to the spine that integrates its output?
//!
//! A window over time-ordered records retracts each record once it falls out
//! of the window, so the integral of the window's output takes a retraction
//! for every record it took, a window's width later.  A retraction cancels
//! only when a merge brings it together with the record it retracts, and by
//! then that record sits at one of the spine's highest levels, so the
//! integral accumulates negative weights.
//!
//! Every transaction adds `--batch` records, each stamped with a time one
//! past the last, and moves the window to the last `--window` of them.  Each
//! record carries a payload of about two hundred bytes of numbers and text.
//! The run reports, for each half of it and overall, its wall time, the CPU
//! time of the circuit's workers (foreground) and of everything else
//! (background, mostly the mergers), peak memory and storage.  Every 1,000
//! transactions, and at the middle and the end, it takes a snapshot of how
//! many batches, records and negative weights each spine holds at each
//! level.  Once the last transaction is done, it waits for the mergers to
//! finish the merging the run left them, reports the wait as a phase of its
//! own, and takes one more snapshot.  It checks that every transaction's
//! output holds what the window gained and lost.
//!
//! ```text
//! cargo bench -p dbsp --bench window_retractions -- --dir PATH
//!     [--records N] [--batch N] [--window N] [--workers N] [--max-rss-gib N]
//!     [--datagen-threads N] [--dev-tweaks JSON] [--profiles DIR]
//! ```
//!
//! `--dev-tweaks` takes the circuit's development settings as a JSON object,
//! such as `{"top_level_negative_weight_fraction": 1.0}`.  The storage goes
//! under `--dir`, which has to be on a disk for the storage to be read from
//! one.  `--profiles` keeps the circuit profile of every snapshot in `DIR`.

#[allow(dead_code)]
#[path = "support/measure.rs"]
mod measure;

use dbsp::{Runtime, ZWeight, typed_batch::DynBatchReader, typed_batch::TypedBox, utils::Tup2};
use measure::{Chunks, Mark, Payload, PhaseCost, Sampler, Snapshots, payload};
use serde_json::json;
use std::cell::Cell;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// One input record: its time, and its payload with weight one.
type Record = Tup2<u64, Tup2<Payload, ZWeight>>;

/// Transactions between progress reports, each with a snapshot.
const REPORT_EVERY: usize = 1000;

struct Args {
    records: u64,
    batch: u64,
    window: u64,
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
            records: 2_000_000_000,
            batch: 100_000,
            window: 500_000_000,
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
                "--records" => args.records = number(value(&mut i)),
                "--batch" => args.batch = number(value(&mut i)),
                "--window" => args.window = number(value(&mut i)),
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
        assert!(args.batch > 0, "--batch wants at least one record");
        assert!(
            args.records.is_multiple_of(args.batch),
            "--records has to be a whole number of batches"
        );
        args
    }
}

/// How a run of transactions' times were spread, in milliseconds.
fn step_times(walls: &[f64]) -> serde_json::Value {
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
    println!(
        "window retractions: {} records in transactions of {}, window of {} records, {} workers, \
         memory limit {} GiB, dev tweaks {}",
        args.records,
        args.batch,
        args.window,
        args.workers,
        args.max_rss_gib,
        args.dev_tweaks.as_deref().unwrap_or("default"),
    );

    let outputs = Arc::new(AtomicU64::new(0));
    let (mut dbsp, (input, bounds)) = {
        let outputs = outputs.clone();
        Runtime::init_circuit(config, move |circuit| {
            let (bounds, bounds_handle) = circuit.add_input_stream::<(u64, u64)>();
            let (stream, input) = circuit.add_input_indexed_zset::<u64, Payload>();
            // An input stream yields its default, `(0, 0)`, at every step it
            // was given nothing, such as the steps that commit a transaction.
            // The window's lower bound also garbage-collects the window's
            // input trace, and that bound may only grow, so the window gets
            // the last bounds the run set.
            let last = Cell::new((0, 0));
            let bounds = bounds.apply(move |&(start, end)| {
                if end > 0 {
                    last.set((start, end));
                }
                let (start, end) = last.get();
                (TypedBox::new(start), TypedBox::new(end))
            });
            let window = stream.window((true, false), &bounds);
            let outputs = outputs.clone();
            window.inspect(move |batch| {
                outputs.fetch_add(batch.len() as u64, Ordering::Relaxed);
            });
            window.accumulate_integrate_trace();
            Ok((input, bounds_handle))
        })
        .expect("failed to build the circuit")
    };

    let batch = args.batch;
    let transactions = (args.records / batch) as usize;
    let chunks = Chunks::new(args.datagen_threads, transactions, move |index| {
        let first = index as u64 * batch;
        (first..first + batch)
            .map(|time| Tup2(time, Tup2(payload(time, 0), 1)))
            .collect::<Vec<Record>>()
    });

    let sampler = Sampler::start(storage.clone());
    let mut snapshots = Snapshots::new(args.profiles.clone());
    let mut phases = Vec::new();
    let mut layouts = Vec::new();
    let mut walls = Vec::with_capacity(transactions);
    let mut wrong = 0usize;
    let mut previous_start = 0u64;
    let started = Instant::now();
    let mut mark = Mark::now();
    for (index, mut records) in chunks.enumerate() {
        let end = (index as u64 + 1) * batch;
        let start = end.saturating_sub(args.window);
        let begun = Instant::now();
        input.append(&mut records);
        bounds.set_for_all((start, end));
        dbsp.transaction().expect("a transaction failed");
        walls.push(begun.elapsed().as_secs_f64());

        // The window gained the batch and lost what slid out of it.
        if outputs.swap(0, Ordering::Relaxed) != batch + (start - previous_start) {
            wrong += 1;
        }
        previous_start = start;

        let done = index + 1;
        // Each half ends with a snapshot, named for where in the run it is.
        let half = if done == transactions {
            Some(("second half", "at the end"))
        } else if done == transactions / 2 {
            Some(("first half", "in the middle"))
        } else {
            None
        };
        // A half's costs end before the snapshot that follows it.
        if let Some((half, _)) = half {
            phases.push(sampler.end_phase(half, mark));
        }
        if half.is_some() || done.is_multiple_of(REPORT_EVERY) {
            println!(
                "    {done:>6} transactions, {:>11} records in {:>8.1} s, rss {:>5.1} GiB, \
                 storage {:>6.1} GiB",
                end,
                started.elapsed().as_secs_f64(),
                measure::gib(measure::rss_bytes().unwrap_or(0)),
                measure::gib(measure::dir_bytes(&storage)),
            );
            let spines = snapshots.take(&mut dbsp, &format!("after {done} transactions"));
            if let Some((_, when)) = half {
                layouts.push((when, spines));
            }
        }
        if half.is_some() {
            mark = Mark::now();
        }
    }
    let total = PhaseCost::combined("whole run", &phases);
    // The wait for the mergers began with the snapshot that ended the run.
    measure::settle(&mut dbsp);
    let settled = sampler.end_phase("settle", mark);
    layouts.push((
        "after settling",
        snapshots.take(&mut dbsp, "after settling"),
    ));
    drop(sampler);
    dbsp.kill().expect("failed to stop the circuit");

    println!();
    for phase in phases.iter().chain([&total, &settled]) {
        phase.print();
    }
    snapshots.print();
    let middle = transactions / 2;
    let (first, second) = walls.split_at(middle);
    println!(
        "  transactions, first half:  {}",
        describe(&step_times(first))
    );
    println!(
        "  transactions, second half: {}",
        describe(&step_times(second))
    );
    for (when, spines) in &layouts {
        measure::print_layouts(when, spines, args.workers);
    }
    if wrong > 0 {
        println!("  WRONG: {wrong} transactions' outputs were not what the window gained and lost");
    } else {
        println!("  outputs: every transaction's output was what the window gained and lost");
    }

    let result = json!({
        "bench": "window_retractions",
        "records": args.records,
        "batch": args.batch,
        "window": args.window,
        "workers": args.workers,
        "dev_tweaks": args.dev_tweaks,
        "phases": phases.iter().chain([&total, &settled]).map(PhaseCost::to_json).collect::<Vec<_>>(),
        "transaction_ms": {"first_half": step_times(first), "second_half": step_times(second)},
        "spines": layouts.iter().map(|(when, spines)| json!({"when": when, "spines": measure::layouts_json(spines)})).collect::<Vec<_>>(),
        "snapshots": snapshots.to_json(),
        "wrong_transactions": wrong,
    });
    println!("RESULT {result}");
}
