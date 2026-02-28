//! Benchmark for `ListMerger` over `OrdIndexedZSet` batches.
//!
//! Generates 100M `(key, value, +1)` records split across `N` input batches and
//! merges them into one batch using `ListMerger`.
//!
//! Runs both in-memory and file-backed modes.
//!
//! Run with: `cargo bench -p dbsp --bench list_merger`

use dbsp::circuit::{CircuitConfig, CircuitStorageConfig, DevTweaks, Layout, Mode};
use dbsp::{
    OrdIndexedZSet, Runtime, ZWeight,
    trace::{Batch as DynBatch, BatchLocation, BatchReader as DynBatchReader, Builder, ListMerger},
    typed_batch::BatchReader as TypedBatchReader,
    utils::{Tup2, Tup10},
};
use feldera_types::config::{StorageCacheConfig, StorageConfig, StorageOptions};
use rand::{Rng, RngCore, SeedableRng};
use rand_xoshiro::Xoshiro256StarStar;
use std::hint::black_box;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tempfile::tempdir;

const SEED: [u8; 32] = [
    0x7f, 0xc3, 0x59, 0x18, 0x45, 0x19, 0xc0, 0xaa, 0xd2, 0xec, 0x31, 0x26, 0xbb, 0x74, 0x2f, 0x8b,
    0x11, 0x7d, 0x0c, 0xe4, 0x64, 0xbf, 0x72, 0x17, 0x46, 0x28, 0x46, 0x42, 0xb2, 0x4b, 0x72, 0x18,
];

const NUM_RECORDS: usize = 20_000_000;
const BATCH_COUNTS: &[usize] = &[1, 8, 32, 64];
const KEY_RANGES: &[u64] = &[100, 100_000_000];

#[derive(Clone)]
struct BenchResult {
    num_batches: usize,
    key_range: u64,
    m_records_per_sec: f64,
}

type Value = Tup10<u64, u64, u64, u64, u64, u64, u64, u64, u64, u64>;

fn records_in_batch(batch_index: usize, num_batches: usize) -> usize {
    let base = NUM_RECORDS / num_batches;
    let remainder = NUM_RECORDS % num_batches;
    base + usize::from(batch_index < remainder)
}

fn generate_batches(num_batches: usize, key_range: u64) -> Vec<OrdIndexedZSet<u64, Value>> {
    let mut rng = Xoshiro256StarStar::from_seed(SEED);

    (0..num_batches)
        .map(|batch_index| {
            let n = records_in_batch(batch_index, num_batches);
            let tuples: Vec<Tup2<Tup2<u64, Value>, ZWeight>> = (0..n)
                .map(|_| {
                    let key = rng.gen_range(0..key_range);
                    let value = Tup10(
                        rng.next_u64(),
                        rng.next_u64(),
                        rng.next_u64(),
                        rng.next_u64(),
                        rng.next_u64(),
                        rng.next_u64(),
                        rng.next_u64(),
                        rng.next_u64(),
                        rng.next_u64(),
                        rng.next_u64(),
                    );
                    Tup2(Tup2(key, value), 1)
                })
                .collect();
            OrdIndexedZSet::from_tuples((), tuples)
        })
        .collect()
}

fn merge_with_list_merger(
    batches: Vec<OrdIndexedZSet<u64, Value>>,
) -> (
    <OrdIndexedZSet<u64, Value> as TypedBatchReader>::Inner,
    usize,
    BatchLocation,
) {
    type InnerBatch = <OrdIndexedZSet<u64, Value> as TypedBatchReader>::Inner;
    let mut inner_batches: Vec<InnerBatch> = batches
        .into_iter()
        .map(|batch| batch.into_inner())
        .collect();
    let factories = inner_batches[0].factories();
    let builder =
        <InnerBatch as DynBatch>::Builder::for_merge(&factories, inner_batches.iter(), None);

    let output: InnerBatch = ListMerger::merge(
        &factories,
        builder,
        inner_batches
            .iter_mut()
            .map(|batch| batch.consuming_cursor(None, None))
            .collect(),
    );
    let output_len = output.len();
    let actual_location = output.location();
    (output, output_len, actual_location)
}

fn bench(generate_on_storage: bool) {
    let temp = tempdir().expect("failed to create temp directory");
    let config = CircuitConfig {
        layout: Layout::new_solo(1),
        mode: Mode::Ephemeral,
        pin_cpus: Vec::new(),
        storage: Some(
            CircuitStorageConfig::for_config(
                StorageConfig {
                    path: temp.path().to_string_lossy().into_owned(),
                    cache: StorageCacheConfig::default(),
                },
                StorageOptions {
                    min_storage_bytes: Some(0),
                    min_step_storage_bytes: if generate_on_storage { Some(0) } else { None },
                    ..StorageOptions::default()
                },
            )
            .expect("failed to configure POSIX storage"),
        ),
        dev_tweaks: DevTweaks::default(),
    };

    let results: Arc<Mutex<Vec<BenchResult>>> = Arc::new(Mutex::new(Vec::new()));
    let results_clone = Arc::clone(&results);

    let handle = Runtime::run(config, move |_parker| {
        for &num_batches in BATCH_COUNTS {
            assert!(num_batches > 0 && num_batches <= 64);
            for &key_range in KEY_RANGES {
                println!(
                    "\nMerging {NUM_RECORDS} records across {num_batches} batches (key_range={key_range})..."
                );
                let batches = generate_batches(num_batches, key_range);
                let storage_batches = batches
                    .iter()
                    .filter(|batch| batch.location() == BatchLocation::Storage)
                    .count();
                println!("  Input batches on storage: {storage_batches}/{num_batches}");

                let start = Instant::now();
                let (output, output_len, actual_location) = merge_with_list_merger(batches);
                let elapsed = start.elapsed();
                black_box(output);
                black_box(output_len);

                let m_records_per_sec = NUM_RECORDS as f64 / elapsed.as_secs_f64() / 1_000_000.0;
                println!(
                    "  merged in {:?} ({:.1} M input records/s), output_location={:?}",
                    elapsed,
                    m_records_per_sec,
                    actual_location
                );

                results_clone.lock().unwrap().push(BenchResult {
                    num_batches,
                    key_range,
                    m_records_per_sec,
                });
            }
        }
    })
    .expect("failed to start DBSP runtime");

    handle.kill().expect("failed to kill runtime");

    let results = results.lock().unwrap().clone();
    let storage_label = if generate_on_storage {
        "file-backed (min_step_storage_bytes=Some(0))"
    } else {
        "in-memory (min_step_storage_bytes=None)"
    };
    println!("\nSummary ({storage_label}) – M input records/s");
    println!("\n┌─────────────┬────────────────────┬──────────────────────────┐");
    println!("│ # Batches   │ key range: 100     │ key range: 100000000     │");
    println!("├─────────────┼────────────────────┼──────────────────────────┤");
    for &num_batches in BATCH_COUNTS {
        let throughput_100 = results
            .iter()
            .find(|r| r.num_batches == num_batches && r.key_range == 100)
            .map(|r| r.m_records_per_sec)
            .unwrap_or(0.0);
        let throughput_100m = results
            .iter()
            .find(|r| r.num_batches == num_batches && r.key_range == 100_000_000)
            .map(|r| r.m_records_per_sec)
            .unwrap_or(0.0);
        println!(
            "│ {:>11} │ {:>18.1} │ {:>24.1} │",
            num_batches, throughput_100, throughput_100m
        );
    }
    println!("└─────────────┴────────────────────┴──────────────────────────┘");
}

fn main() {
    println!("Running ListMerger benchmark with in-memory batches...");
    bench(false);

    println!("\nRunning ListMerger benchmark with file-backed batches...");
    bench(true);
}
