//! Benchmark for `Cursor::seek_key_exact` on file-backed and in-memory ZSets.
//!
//! Builds a single large ZSet of distinct random `u64` keys, then issues
//! `NUM_LOOKUPS` random lookups (mix of hits and misses). Reports ns/lookup so
//! the impact of deserializing-vs-direct-archived comparison is visible.
//!
//! Run with: cargo bench -p dbsp --bench seek_key_exact

use dbsp::circuit::{CircuitConfig, CircuitStorageConfig};
use dbsp::dynamic::{DowncastTrait, DynData, Erase};
use dbsp::typed_batch::BatchReader as TypedBatchReader;
use dbsp::{
    OrdZSet, Runtime, ZWeight,
    trace::{BatchReader, Cursor},
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

const SEED_KEYS: [u8; 32] = [
    0x7f, 0xc3, 0x59, 0x18, 0x45, 0x19, 0xc0, 0xaa, 0xd2, 0xec, 0x31, 0x26, 0xbb, 0x74, 0x2f, 0x8b,
    0x11, 0x7d, 0x0c, 0xe4, 0x64, 0xbf, 0x72, 0x17, 0x46, 0x28, 0x46, 0x42, 0xb2, 0x4b, 0x72, 0x18,
];
const SEED_LOOKUPS: [u8; 32] = [
    0xa1, 0x14, 0x9d, 0x5f, 0x22, 0x80, 0xb3, 0xc7, 0x66, 0x10, 0x4e, 0xab, 0x09, 0x77, 0x5e, 0xcc,
    0x3b, 0xd2, 0x44, 0x18, 0x91, 0xa8, 0x6c, 0x55, 0x77, 0xee, 0x4f, 0x12, 0x88, 0x33, 0xb5, 0x6d,
];

const NUM_KEYS: usize = 1_000_000;
const NUM_LOOKUPS: usize = 500_000;
const HIT_RATIO: f64 = 0.5;

fn build_zset() -> OrdZSet<u64> {
    let mut rng = Xoshiro256StarStar::from_seed(SEED_KEYS);
    let mut pairs: Vec<(u64, ZWeight)> = (0..NUM_KEYS)
        .map(|_| (rng.r#gen::<u64>() & ((1u64 << 48) - 1), 1))
        .collect();
    pairs.sort_by_key(|t| t.0);
    pairs.dedup_by_key(|t| t.0);
    let tuples: Vec<Tup2<Tup2<u64, ()>, ZWeight>> = pairs
        .into_iter()
        .map(|(k, w)| Tup2(Tup2(k, ()), w))
        .collect();
    OrdZSet::from_tuples((), tuples)
}

fn extract_keys(zset: &OrdZSet<u64>) -> Vec<u64> {
    let mut cursor = zset.inner().cursor();
    let mut acc = Vec::with_capacity(zset.key_count());
    while cursor.key_valid() {
        acc.push(unsafe { *cursor.key().downcast::<u64>() });
        cursor.step_key();
    }
    acc
}

fn build_lookups(present: &[u64]) -> Vec<u64> {
    let mut rng = Xoshiro256StarStar::from_seed(SEED_LOOKUPS);
    let hits = (NUM_LOOKUPS as f64 * HIT_RATIO) as usize;
    let mut result = Vec::with_capacity(NUM_LOOKUPS);
    for _ in 0..hits {
        let idx = rng.gen_range(0..present.len());
        result.push(present[idx]);
    }
    for _ in hits..NUM_LOOKUPS {
        // Random 48-bit value: misses the prepared key range with high probability.
        result.push(rng.r#gen::<u64>() & ((1u64 << 48) - 1));
    }
    for i in (1..result.len()).rev() {
        let j = rng.gen_range(0..=i);
        result.swap(i, j);
    }
    result
}

fn time_lookups(zset: &OrdZSet<u64>, lookups: &[u64]) -> (u64, u64) {
    let mut cursor = zset.inner().cursor();
    let start = Instant::now();
    let mut hit_count = 0u64;
    for key in lookups {
        cursor.rewind_keys();
        let key_dyn: &DynData = (*key).erase();
        if cursor.seek_key_exact(key_dyn, None) {
            hit_count += 1;
        }
        black_box(&cursor);
    }
    let elapsed_ns = start.elapsed().as_nanos() as u64;
    (hit_count, elapsed_ns)
}

fn bench(storage: bool) {
    let temp = tempdir().expect("failed to create temp dir for storage");
    let config = CircuitConfig::with_workers(1).with_storage(Some(
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
    ));

    let result_ns: Arc<Mutex<Option<(u64, u64, usize)>>> = Arc::new(Mutex::new(None));
    let result_clone = Arc::clone(&result_ns);

    let handle = Runtime::run(config, move |_parker| {
        let zset = build_zset();
        let present = extract_keys(&zset);
        let lookups = build_lookups(&present);
        let key_count = zset.key_count();
        let (hits, elapsed_ns) = time_lookups(&zset, &lookups);
        *result_clone.lock().unwrap() = Some((hits, elapsed_ns, key_count));
    })
    .expect("failed to start DBSP runtime");
    handle.kill().expect("failed to kill runtime");

    let (hits, elapsed_ns, keys) = result_ns
        .lock()
        .unwrap()
        .expect("bench did not produce a result");
    let storage_label = if storage { "file-backed" } else { "in-memory " };
    let per_lookup_ns = elapsed_ns as f64 / NUM_LOOKUPS as f64;
    println!(
        "{storage_label}: {NUM_LOOKUPS} lookups against {keys} keys in {:.2} ms = {:.1} ns/lookup, {} hits",
        elapsed_ns as f64 / 1e6,
        per_lookup_ns,
        hits
    );
}

fn main() {
    println!("seek_key_exact micro-benchmark");
    bench(false);
    bench(true);
}
