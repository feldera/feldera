//! What does a merge save by copying bytes instead of re-encoding them?
//!
//! A merge that finds a key supplied by one input alone can copy that key's
//! values, and then the key itself, instead of decoding each record out of the
//! input and encoding it again into the output.  This benchmark drives
//! `ListMerger` over file-backed batches -- the API a spine merges through --
//! so it runs unchanged on every commit of this branch and on the code before
//! it, and the saving appears as each copying commit lands.
//!
//! Two shapes, and the contrast between them is the measurement:
//!
//! * `disjoint`: each input covers its own stretch of the key space, so every
//!   key is supplied by one input and can be copied.  This is the shape a
//!   backfill's merges have, and the one the copying is for.
//! * `shared`: every input holds every key, so each key is merged from all of
//!   them and nothing can be copied.  This is the control -- it should not
//!   move as the copying commits land, and a run where it does is measuring
//!   the machine rather than the change.
//!
//! The key is a string.  An integer key is the kind a roaring filter is built
//! for, and that filter is fed the key itself, so an integer-keyed merge
//! writes its keys decoded however it reads them; a string is also where
//! decoding allocates and so where copying has something to save.
//!
//!     cargo bench -p dbsp --bench splice_copy -- [--keys N] [--values N]
//!                                               [--batches N] [--repeat N]
//!                                               [--no-compression]

use dbsp::circuit::{CircuitConfig, CircuitStorageConfig};
use dbsp::{
    OrdIndexedZSet, Runtime, ZWeight,
    trace::{Batch as DynBatch, BatchLocation, BatchReader as DynBatchReader, Builder, ListMerger},
    typed_batch::BatchReader as TypedBatchReader,
    utils::Tup2,
};
use feldera_types::config::{
    StorageCacheConfig, StorageCompression, StorageConfig, StorageOptions,
};
use std::hint::black_box;
use std::time::{Duration, Instant};
use tempfile::tempdir;

type Batch = OrdIndexedZSet<String, u64>;
type Inner = <Batch as TypedBatchReader>::Inner;

/// How the inputs divide the key space, which is what decides whether a merge
/// can copy anything.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Shape {
    /// One input per stretch of keys: every key comes from one input, so its
    /// values and then the key itself may be copied.
    Disjoint,
    /// Every input holds every key, so every key is merged and nothing is
    /// copied.  The control.
    Shared,
}

impl Shape {
    fn name(self) -> &'static str {
        match self {
            Shape::Disjoint => "disjoint",
            Shape::Shared => "shared",
        }
    }
}

#[derive(Clone)]
struct Args {
    keys: usize,
    values: usize,
    batches: usize,
    repeat: usize,
    /// Percentage of records that retract, as a weight of -1.
    negative: usize,
    compression: StorageCompression,
}

impl Args {
    fn parse() -> Self {
        let mut args = Self {
            keys: 200_000,
            values: 1,
            batches: 4,
            repeat: 3,
            negative: 0,
            compression: StorageCompression::Default,
        };
        let argv: Vec<String> = std::env::args().collect();
        let mut i = 1;
        while i < argv.len() {
            let flag = argv[i].clone();
            let number = |i: &mut usize| -> usize {
                *i += 1;
                argv.get(*i)
                    .and_then(|v| v.parse().ok())
                    .unwrap_or_else(|| panic!("{flag} needs a number"))
            };
            match argv[i].as_str() {
                "--keys" => args.keys = number(&mut i),
                "--values" => args.values = number(&mut i),
                "--batches" => args.batches = number(&mut i),
                "--repeat" => args.repeat = number(&mut i),
                "--negative" => args.negative = number(&mut i),
                "--no-compression" => args.compression = StorageCompression::None,
                _ => {}
            }
            i += 1;
        }
        assert!(args.batches > 0, "--batches wants at least one");
        assert!(args.negative <= 100, "--negative is a percentage");
        assert!(
            args.values >= args.batches,
            "--values must be at least --batches, so that the shared shape can \
             give every input a share of every key's values",
        );
        args
    }

    /// Records the merge reads, which is also what it writes: the shapes give
    /// every key the same number of values, so the two are comparable.
    fn tuples(&self) -> usize {
        self.keys * self.values
    }
}

/// The weight of record `n`: `-1` for the share `--negative` asks to retract,
/// `1` for the rest.
///
/// A merge can only copy a run whose weights it need not add up, and a batch
/// reports how many of its records carry a negative weight, so this is what
/// decides whether copying is open to the merge at all.
///
/// # Arguments
///
/// * `n` - which record.
/// * `args` - the run's command line, read for the share that retracts.
///
/// # Returns
///
/// The weight, `1` or `-1`.
fn weight(n: usize, args: &Args) -> ZWeight {
    if args.negative > 0 && n % 100 < args.negative {
        -1
    } else {
        1
    }
}

/// A key whose length varies, so items do not all encode to one size, and long
/// enough to hold its text out of line: that is what makes decoding one
/// allocate, and so what copying it saves.
///
/// # Arguments
///
/// * `row` - which key to build.
///
/// # Returns
///
/// The key, ordered the same way `row` is so that a stretch of rows is a
/// stretch of keys.
fn key(row: usize) -> String {
    format!("key-{row:08}-{}", "y".repeat(row % 29))
}

/// The inputs to merge, on storage, laid out as `shape` asks.
///
/// Every shape holds the same records in total, so the merge output is the
/// same collection either way and only the division between inputs differs.
///
/// # Arguments
///
/// * `args` - the run's command line, read for the key, value and batch
///   counts.
/// * `shape` - how to divide the keys between the inputs.
///
/// # Returns
///
/// One batch per input, each of which panics rather than measure anything if
/// it landed in memory.
fn inputs(args: &Args, shape: Shape) -> Vec<Inner> {
    (0..args.batches)
        .map(|batch| {
            let tuples: Vec<Tup2<Tup2<String, u64>, ZWeight>> = match shape {
                // This input's own stretch of keys, with all of each key's
                // values, so no other input holds the key.
                Shape::Disjoint => {
                    let from = args.keys * batch / args.batches;
                    let to = args.keys * (batch + 1) / args.batches;
                    (from..to)
                        .flat_map(|row| {
                            (0..args.values).map(move |i| {
                                let n = row * args.values + i;
                                Tup2(Tup2(key(row), (row * 64 + i) as u64), weight(n, args))
                            })
                        })
                        .collect()
                }
                // Every key, but only this input's share of each key's
                // values, so the shapes hold the same records in total and
                // every key still has to be merged from every input.
                Shape::Shared => (0..args.keys)
                    .flat_map(|row| {
                        (batch..args.values).step_by(args.batches).map(move |i| {
                            let n = row * args.values + i;
                            Tup2(Tup2(key(row), (row * 64 + i) as u64), weight(n, args))
                        })
                    })
                    .collect(),
            };
            let batch = Batch::from_tuples((), tuples).into_inner();
            assert_eq!(
                batch.location(),
                BatchLocation::Storage,
                "an input has to be on storage for a merge to have bytes to copy",
            );
            batch
        })
        .collect()
}

/// Merges `batches` into one batch on storage and returns how long it took.
///
/// This is `ListMerger::merge` over the cursors a spine hands it, so whatever
/// the merge does about copying is whatever it does in production.
///
/// # Arguments
///
/// * `batches` - the inputs, consumed by the merge as a spine's are.
/// * `args` - the run's command line, read for the capacities to build with.
///
/// # Returns
///
/// The elapsed time, including finishing the output's file.
fn merge(mut batches: Vec<Inner>, args: &Args) -> Duration {
    let factories = batches[0].factories();
    let start = Instant::now();
    let builder = <Inner as DynBatch>::Builder::with_capacity_in_location(
        &factories,
        args.keys,
        args.tuples(),
        Some(BatchLocation::Storage),
    );
    let output: Inner = ListMerger::merge(
        &factories,
        builder,
        batches
            .iter_mut()
            .map(|batch| batch.consuming_cursor(None, None))
            .collect(),
    );
    let elapsed = start.elapsed();
    assert_eq!(
        output.location(),
        BatchLocation::Storage,
        "the merge has to write a file for any of this to mean anything",
    );
    black_box(output);
    elapsed
}

fn main() {
    let args = Args::parse();
    let temp = tempdir().expect("failed to create temp directory");
    let config = CircuitConfig::with_workers(1).with_storage(Some(
        CircuitStorageConfig::for_config(
            StorageConfig {
                path: temp.path().to_string_lossy().into_owned(),
                cache: StorageCacheConfig::default(),
            },
            StorageOptions {
                min_storage_bytes: Some(0),
                min_step_storage_bytes: Some(0),
                compression: args.compression,
                ..StorageOptions::default()
            },
        )
        .expect("failed to configure POSIX storage"),
    ));

    let handle = Runtime::run(config, move |_parker| {
        println!(
            "merging {} batches of {} keys x {} values ({} tuples), \
             {}% retracting, compression {}",
            args.batches,
            args.keys,
            args.values,
            args.tuples(),
            args.negative,
            match args.compression {
                StorageCompression::None => "off",
                _ => "on",
            },
        );

        let mut best = [Duration::MAX; 2];
        for _ in 0..args.repeat {
            // Fresh inputs each time: a merge consumes its cursors.
            for (slot, shape) in [Shape::Disjoint, Shape::Shared].into_iter().enumerate() {
                let batches = inputs(&args, shape);
                best[slot] = best[slot].min(merge(batches, &args));
            }
        }

        let per = |d: Duration| d.as_secs_f64() * 1e9 / args.tuples() as f64;
        for (slot, shape) in [Shape::Disjoint, Shape::Shared].into_iter().enumerate() {
            println!(
                "  {:<10} {:>8.2} ns/tuple  ({:.3}s)",
                shape.name(),
                per(best[slot]),
                best[slot].as_secs_f64(),
            );
        }
        // What a key supplied by one input is worth against one that has to be
        // merged.  This is the number to watch commit by commit: `disjoint`
        // falls as copying lands, `shared` should not move.
        println!(
            "  disjoint is {:.2}x the cost of shared",
            best[0].as_secs_f64() / best[1].as_secs_f64(),
        );
    })
    .expect("failed to start the runtime");
    handle.join().unwrap();
}
