//! Benchmark for CursorList iteration over multiple OrdIndexedZSet batches.
//!
//! Generates 100 batches with 1M records each. Times only the iteration phase.
//! Two flavors: few unique keys (many values per key) vs many unique keys.
//!
//! Runs within a single-threaded DBSP runtime with POSIX storage backend.
//!
//! Run with: cargo bench -p dbsp --bench cursor_list

use dbsp::circuit::{CircuitConfig, CircuitStorageConfig, Layout, Mode};
use dbsp::{
    OrdIndexedZSet, Runtime, ZWeight,
    trace::cursor::CursorList,
    trace::{BatchReader, BatchReaderFactories, Cursor},
    utils::Tup2,
};
use feldera_types::config::{StorageCacheConfig, StorageConfig, StorageOptions};
use rand::Rng;
use rand::SeedableRng;
use rand_xoshiro::Xoshiro256StarStar;
use std::hint::black_box;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tempfile::tempdir;

#[derive(Clone)]
struct BenchResult {
    num_batches: usize,
    key_type: &'static str,
    m_records_per_sec: f64,
}

const SEED: [u8; 32] = [
    0x7f, 0xc3, 0x59, 0x18, 0x45, 0x19, 0xc0, 0xaa, 0xd2, 0xec, 0x31, 0x26, 0xbb, 0x74, 0x2f, 0x8b,
    0x11, 0x7d, 0x0c, 0xe4, 0x64, 0xbf, 0x72, 0x17, 0x46, 0x28, 0x46, 0x42, 0xb2, 0x4b, 0x72, 0x18,
];

const MAX_BATCHES: usize = 20;
const NUM_RECORDS: usize = 100_000_000;

/// Generate batches with random (key, value) pairs.
/// Keys are drawn from [0, num_keys), values from [0, num_values).
fn generate_batches(
    num_batches: usize,
    records_per_batch: usize,
    num_keys: u64,
    num_values: u64,
) -> Vec<OrdIndexedZSet<u64, u64>> {
    let mut rng = Xoshiro256StarStar::from_seed(SEED);

    (0..num_batches)
        .map(|_| {
            let tuples: Vec<Tup2<Tup2<u64, u64>, ZWeight>> = (0..records_per_batch)
                .map(|_| {
                    let key = rng.gen_range(0..num_keys);
                    let value = rng.gen_range(0..num_values);
                    Tup2(Tup2(key, value), 1)
                })
                .collect();
            OrdIndexedZSet::from_tuples((), tuples)
        })
        .collect()
}

fn iterate_cursor_list(batches: &[OrdIndexedZSet<u64, u64>]) -> u64 {
    let cursors: Vec<_> = batches.iter().map(|b| b.cursor()).collect();
    let weight_factory = batches[0].factories().weight_factory();
    let mut cursor_list = CursorList::new(weight_factory, cursors);

    let mut count = 0u64;
    while cursor_list.key_valid() {
        while cursor_list.val_valid() {
            black_box(cursor_list.key());
            black_box(cursor_list.val());
            black_box(cursor_list.weight());
            count += 1;
            cursor_list.step_val();
        }
        cursor_list.step_key();
    }
    count
}

fn bench(storage: bool) {
    let temp = tempdir().expect("failed to create temp dir for storage");
    let config = CircuitConfig {
        layout: Layout::new_solo(1),
        max_rss_bytes: None,
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
                    min_step_storage_bytes: if storage { Some(0) } else { None },
                    ..StorageOptions::default()
                },
            )
            .expect("failed to configure POSIX storage"),
        ),
        dev_tweaks: Default::default(),
        exchange_listener: None,
    };

    let results: Arc<Mutex<Vec<BenchResult>>> = Arc::new(Mutex::new(Vec::new()));
    let results_clone = Arc::clone(&results);

    let handle = Runtime::run(config, move |_parker| {
        for num_batches in (1..=MAX_BATCHES).step_by(2) {
            let records_per_batch = NUM_RECORDS / num_batches;
            println!(
                "\nIterating over {num_batches} batches × {records_per_batch} records = {}M records...",
                (num_batches * records_per_batch) / 1_000_000
            );

            println!("\nFew keys (100)...");
            let batches = generate_batches(num_batches, records_per_batch, 100, u64::MAX);
            let start = Instant::now();
            let count = iterate_cursor_list(&batches);
            let elapsed = start.elapsed();
            let m_records_per_sec = count as f64 / elapsed.as_secs_f64() / 1_000_000.0;
            println!("  {} records in {:?} ({:.1} M/s)", count, elapsed, m_records_per_sec);
            results_clone.lock().unwrap().push(BenchResult {
                num_batches,
                key_type: "Few keys (100)",
                m_records_per_sec,
            });

            println!("\nMany keys (100M), many values...");
            let batches = generate_batches(
                num_batches,
                records_per_batch,
                100_000_000,
                u64::MAX,
            );
            let start = Instant::now();
            let count = iterate_cursor_list(&batches);
            let elapsed = start.elapsed();
            let m_records_per_sec = count as f64 / elapsed.as_secs_f64() / 1_000_000.0;
            println!("  {} records in {:?} ({:.1} M/s)", count, elapsed, m_records_per_sec);
            results_clone.lock().unwrap().push(BenchResult {
                num_batches,
                key_type: "Many keys (100M)",
                m_records_per_sec,
            });
        }
    })
    .expect("failed to start DBSP runtime");

    handle.kill().expect("failed to kill runtime");

    let results = results.lock().unwrap().clone();
    let storage_label = if storage { "file-backed" } else { "in-memory" };
    println!("Summary ({storage_label}) – M records/s");

    println!("\n┌─────────────┬──────────────────┬──────────────────┐");
    println!("│ # Batches   │ Few keys (100)   │ Many keys (100M) │");
    println!("├─────────────┼──────────────────┼──────────────────┤");
    for num_batches in (1..=MAX_BATCHES).step_by(2) {
        let few = results
            .iter()
            .find(|r| r.num_batches == num_batches && r.key_type == "Few keys (100)")
            .map(|r| r.m_records_per_sec)
            .unwrap_or(0.0);
        let many = results
            .iter()
            .find(|r| r.num_batches == num_batches && r.key_type == "Many keys (100M)")
            .map(|r| r.m_records_per_sec)
            .unwrap_or(0.0);
        println!("│ {:>11} │ {:>16.1} │ {:>16.1} │", num_batches, few, many);
    }
    println!("└─────────────┴──────────────────┴──────────────────┘");
}

fn main() {
    println!("Running CursorList benchmark with in-memory batches...");
    bench(false);

    println!("Running CursorList benchmark with file-backed batches...");
    bench(true);
}
