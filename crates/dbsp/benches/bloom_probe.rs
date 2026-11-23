//! What does a Bloom filter probe cost when the filters are hot, and when they
//! are too large for the caches to hold?
//!
//! Builds `--batches` file batches of `--keys` keys each, with a Bloom filter
//! and no roaring bitmap, and keeps one cursor open on each, as an operator
//! does over a step.  Then it asks each cursor, in a random order of batches,
//! for keys that no batch holds.  Every batch spans the whole key space, so
//! the range filter passes every key, and the Bloom filter answers almost
//! every lookup.  The time per lookup is therefore the cost of a probe.
//!
//! The defaults give 32 filters of 4M keys at 1e-4, about 300 MiB of filters,
//! which no cache holds.  `--batches 1 --keys 10000` keeps one small filter in
//! the L1 cache.
//!
//! ```text
//! cargo bench -p dbsp --bench bloom_probe -- [--batches N] [--keys N]
//!     [--probes N] [--rounds N] [--bloom-filter-eviction]
//! ```
//!
//! `--bloom-filter-eviction` turns on Bloom filter eviction, so large filters
//! take the code path that eviction needs.  Nothing is evicted, since the bench
//! puts no pressure on memory.

use dbsp::circuit::{CircuitConfig, CircuitStorageConfig};
use dbsp::dynamic::Erase;
use dbsp::trace::BatchLocation;
use dbsp::{
    OrdZSet, Runtime, ZWeight,
    trace::{Batch, BatchReader as DynBatchReader, BatchReaderFactories, Builder, Cursor},
    typed_batch::BatchReader as TypedBatchReader,
};
use feldera_types::config::{StorageCacheConfig, StorageConfig, StorageOptions};
use feldera_types::size::ConfigSize;
use std::time::Instant;
use tempfile::tempdir;

type Inner = <OrdZSet<u64> as TypedBatchReader>::Inner;

#[derive(Clone)]
struct Args {
    batches: u64,
    keys: u64,
    probes: u64,
    rounds: usize,
    bloom_filter_eviction: bool,
}

impl Args {
    fn parse() -> Self {
        let mut args = Self {
            batches: 32,
            keys: 4_000_000,
            probes: 4_000_000,
            rounds: 7,
            bloom_filter_eviction: false,
        };
        let argv: Vec<String> = std::env::args().skip(1).collect();
        let mut i = 0;
        let number = |i: &mut usize| -> u64 {
            *i += 1;
            argv[*i].parse().expect("a number")
        };
        while i < argv.len() {
            match argv[i].as_str() {
                "--batches" => args.batches = number(&mut i),
                "--keys" => args.keys = number(&mut i),
                "--probes" => args.probes = number(&mut i),
                "--rounds" => args.rounds = number(&mut i) as usize,
                "--bloom-filter-eviction" => args.bloom_filter_eviction = true,
                "--bench" => {}
                other => panic!("unknown argument {other}"),
            }
            i += 1;
        }
        args
    }
}

/// A fast, fixed pseudo-random sequence, so every build probes the same keys.
fn mix(mut x: u64) -> u64 {
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

/// Batch `index` of `batches` holds every even key whose half is `index`
/// modulo `batches`, so each batch spans the whole key space and odd keys are
/// in no batch.
fn batch(
    index: u64,
    batches: u64,
    keys: u64,
    factories: &<Inner as DynBatchReader>::Factories,
) -> Inner {
    let mut builder = <Inner as Batch>::Builder::with_capacity_in_location(
        factories,
        keys as usize,
        keys as usize,
        Some(BatchLocation::Storage),
    );
    let weight: ZWeight = 1;
    for i in 0..keys {
        let mut key = (i * batches + index) * 2;
        builder.push_val_diff(().erase(), weight.erase());
        builder.push_key(key.erase_mut());
    }
    builder.done()
}

/// One worker with storage in `path`, so every batch is a file with a Bloom
/// filter at a rate of 1e-4.
fn circuit_config(path: &std::path::Path, bloom_filter_eviction: bool) -> CircuitConfig {
    let storage = CircuitStorageConfig::for_config(
        StorageConfig {
            path: path.to_string_lossy().into_owned(),
            cache: StorageCacheConfig::default(),
        },
        StorageOptions {
            min_spill_size: Some(ConfigSize::ZERO),
            min_step_spill_size: Some(ConfigSize::ZERO),
            ..StorageOptions::default()
        },
    )
    .expect("failed to configure storage");
    let mut config = CircuitConfig::with_workers(1).with_storage(Some(storage));
    config.dev_tweaks.enable_roaring = Some(false);
    config.dev_tweaks.bloom_false_positive_rate = Some(1e-4);
    config.dev_tweaks.bloom_filter_eviction = Some(bloom_filter_eviction);
    config
}

/// Looks up `args.probes` absent keys per round in random batches, through one
/// cursor per batch held for every lookup, as an operator holds its cursors
/// over a step. Returns ns per lookup for each round, and the keys found.
fn time_lookups(batches: &[Inner], args: &Args) -> (Vec<f64>, u64) {
    let mut cursors: Vec<_> = batches.iter().map(|batch| batch.cursor()).collect();
    let span = args.batches * args.keys;
    let mut times = Vec::with_capacity(args.rounds);
    let mut found = 0u64;
    for round in 0..args.rounds as u64 {
        let start = Instant::now();
        for i in 0..args.probes {
            let r = mix(round * args.probes + i);
            let cursor = &mut cursors[(r % args.batches) as usize];
            let key = ((r >> 16) % span) * 2 + 1;
            found += u64::from(cursor.seek_key_exact(key.erase(), None));
        }
        times.push(start.elapsed().as_nanos() as f64 / args.probes as f64);
    }
    (times, found)
}

fn main() {
    let args = Args::parse();
    let temp = tempdir().expect("failed to create temp directory");
    let config = circuit_config(temp.path(), args.bloom_filter_eviction);

    let handle = Runtime::run(config, move |_parker| {
        let factories = BatchReaderFactories::new::<u64, (), ZWeight>();
        let build = Instant::now();
        let batches: Vec<Inner> = (0..args.batches)
            .map(|index| batch(index, args.batches, args.keys, &factories))
            .collect();
        let filter_bytes: usize = batches
            .iter()
            .map(|batch| batch.membership_filter_stats().size_byte)
            .sum();
        println!(
            "{} batches of {} keys, {:.1} MiB of Bloom filters, built in {:.0} s",
            args.batches,
            args.keys,
            filter_bytes as f64 / (1 << 20) as f64,
            build.elapsed().as_secs_f64()
        );

        let (mut times, found) = time_lookups(&batches, &args);
        times.sort_by(f64::total_cmp);
        println!(
            "absent: median {:.2} ns per lookup, min {:.2}, max {:.2}, over {} rounds of {} lookups ({found} found)",
            times[times.len() / 2],
            times[0],
            times[times.len() - 1],
            args.rounds,
            args.probes,
        );
    })
    .expect("failed to start the runtime");
    handle.join().unwrap();
}
