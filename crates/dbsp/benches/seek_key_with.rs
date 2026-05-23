//! Benchmark for `Cursor::seek_key_with` (predicate-based) on file-backed
//! and in-memory ZSets.
//!
//! Same data shapes as `seek_key_exact` but uses the predicate seek path,
//! which goes through `seek_forward_until` -> `Path::advance_to_first_ge`
//! with the user predicate wrapped in a scratch-buffer deserialize. Run
//! after a change to that path to see whether per-compare allocation cost
//! moved.
//!
//! Run with: cargo bench -p dbsp --bench seek_key_with

use dbsp::circuit::{CircuitConfig, CircuitStorageConfig};
use dbsp::dynamic::{DowncastTrait, DynData};
use dbsp::typed_batch::BatchReader as TypedBatchReader;
use dbsp::{
    OrdZSet, Runtime, ZWeight,
    trace::{BatchReader, Cursor},
    utils::Tup2,
};
use feldera_macros::IsNone;
use feldera_types::config::{StorageCacheConfig, StorageConfig, StorageOptions};
use rand::Rng;
use rand::SeedableRng;
use rand_xoshiro::Xoshiro256StarStar;
use rkyv::{Archive, Deserialize, Serialize};
use size_of::SizeOf;
use std::hash::Hash;
use std::hint::black_box;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tempfile::tempdir;

#[derive(
    Clone,
    Default,
    Debug,
    Eq,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
    SizeOf,
    Archive,
    Serialize,
    Deserialize,
    IsNone,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
#[archive(compare(PartialEq))]
pub struct WideRow {
    pub f0: String,
    pub f1: String,
    pub f2: String,
    pub f3: String,
    pub f4: String,
    pub f5: String,
    pub f6: String,
    pub f7: String,
    pub f8: String,
    pub f9: String,
    pub f10: String,
    pub f11: String,
    pub f12: String,
    pub f13: String,
    pub f14: String,
}

dbsp::impl_ord_repr_for_struct! {
    [] ArchivedWideRow as Repr<WideRow>,
    [f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14]
}

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

fn build_zset_u64() -> OrdZSet<u64> {
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

fn extract_keys_u64(zset: &OrdZSet<u64>) -> Vec<u64> {
    let mut cursor = zset.inner().cursor();
    let mut acc = Vec::with_capacity(zset.key_count());
    while cursor.key_valid() {
        acc.push(unsafe { *cursor.key().downcast::<u64>() });
        cursor.step_key();
    }
    acc
}

fn build_lookups_u64(present: &[u64]) -> Vec<u64> {
    let mut rng = Xoshiro256StarStar::from_seed(SEED_LOOKUPS);
    let hits = (NUM_LOOKUPS as f64 * HIT_RATIO) as usize;
    let mut result = Vec::with_capacity(NUM_LOOKUPS);
    for _ in 0..hits {
        let idx = rng.gen_range(0..present.len());
        result.push(present[idx]);
    }
    for _ in hits..NUM_LOOKUPS {
        result.push(rng.r#gen::<u64>() & ((1u64 << 48) - 1));
    }
    for i in (1..result.len()).rev() {
        let j = rng.gen_range(0..=i);
        result.swap(i, j);
    }
    result
}

fn time_lookups_u64(zset: &OrdZSet<u64>, lookups: &[u64]) -> (u64, u64) {
    let mut cursor = zset.inner().cursor();
    let start = Instant::now();
    let mut hit_count = 0u64;
    for key in lookups {
        cursor.rewind_keys();
        let target: u64 = *key;
        cursor.seek_key_with(&|k: &DynData| {
            // Advance past rows < target.
            let k = unsafe { *k.downcast::<u64>() };
            k >= target
        });
        if cursor.key_valid() {
            let landed = unsafe { *cursor.key().downcast::<u64>() };
            if landed == target {
                hit_count += 1;
            }
        }
        black_box(&cursor);
    }
    let elapsed_ns = start.elapsed().as_nanos() as u64;
    (hit_count, elapsed_ns)
}

fn build_zset_string() -> OrdZSet<String> {
    let mut rng = Xoshiro256StarStar::from_seed(SEED_KEYS);
    let mut pairs: Vec<(String, ZWeight)> = (0..NUM_KEYS)
        .map(|_| {
            let n = rng.r#gen::<u64>() & ((1u64 << 40) - 1);
            (format!("key-{n:020}"), 1)
        })
        .collect();
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
    pairs.dedup_by(|a, b| a.0 == b.0);
    let tuples: Vec<Tup2<Tup2<String, ()>, ZWeight>> = pairs
        .into_iter()
        .map(|(k, w)| Tup2(Tup2(k, ()), w))
        .collect();
    OrdZSet::from_tuples((), tuples)
}

fn extract_keys_string(zset: &OrdZSet<String>) -> Vec<String> {
    let mut cursor = zset.inner().cursor();
    let mut acc = Vec::with_capacity(zset.key_count());
    while cursor.key_valid() {
        acc.push(unsafe { cursor.key().downcast::<String>() }.clone());
        cursor.step_key();
    }
    acc
}

fn build_lookups_string(present: &[String]) -> Vec<String> {
    let mut rng = Xoshiro256StarStar::from_seed(SEED_LOOKUPS);
    let hits = (NUM_LOOKUPS as f64 * HIT_RATIO) as usize;
    let mut result = Vec::with_capacity(NUM_LOOKUPS);
    for _ in 0..hits {
        let idx = rng.gen_range(0..present.len());
        result.push(present[idx].clone());
    }
    for _ in hits..NUM_LOOKUPS {
        let n = rng.r#gen::<u64>() & ((1u64 << 40) - 1);
        result.push(format!("miss-{n:020}"));
    }
    for i in (1..result.len()).rev() {
        let j = rng.gen_range(0..=i);
        result.swap(i, j);
    }
    result
}

fn time_lookups_string(zset: &OrdZSet<String>, lookups: &[String]) -> (u64, u64) {
    let mut cursor = zset.inner().cursor();
    let start = Instant::now();
    let mut hit_count = 0u64;
    for key in lookups {
        cursor.rewind_keys();
        let target = key.clone();
        cursor.seek_key_with(&|k: &DynData| {
            let k = unsafe { k.downcast::<String>() };
            k.as_str() >= target.as_str()
        });
        if cursor.key_valid() {
            let landed = unsafe { cursor.key().downcast::<String>() };
            if landed.as_str() == target.as_str() {
                hit_count += 1;
            }
        }
        black_box(&cursor);
    }
    let elapsed_ns = start.elapsed().as_nanos() as u64;
    (hit_count, elapsed_ns)
}

fn random_string(rng: &mut Xoshiro256StarStar) -> String {
    let n = rng.r#gen::<u64>();
    format!("s-{n:020x}")
}

fn random_wide_row(rng: &mut Xoshiro256StarStar) -> WideRow {
    WideRow {
        f0: random_string(rng),
        f1: random_string(rng),
        f2: random_string(rng),
        f3: random_string(rng),
        f4: random_string(rng),
        f5: random_string(rng),
        f6: random_string(rng),
        f7: random_string(rng),
        f8: random_string(rng),
        f9: random_string(rng),
        f10: random_string(rng),
        f11: random_string(rng),
        f12: random_string(rng),
        f13: random_string(rng),
        f14: random_string(rng),
    }
}

fn build_zset_wide() -> OrdZSet<WideRow> {
    let mut rng = Xoshiro256StarStar::from_seed(SEED_KEYS);
    let mut pairs: Vec<(WideRow, ZWeight)> = (0..NUM_KEYS)
        .map(|_| (random_wide_row(&mut rng), 1))
        .collect();
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
    pairs.dedup_by(|a, b| a.0 == b.0);
    let tuples: Vec<Tup2<Tup2<WideRow, ()>, ZWeight>> = pairs
        .into_iter()
        .map(|(k, w)| Tup2(Tup2(k, ()), w))
        .collect();
    OrdZSet::from_tuples((), tuples)
}

fn extract_keys_wide(zset: &OrdZSet<WideRow>) -> Vec<WideRow> {
    let mut cursor = zset.inner().cursor();
    let mut acc = Vec::with_capacity(zset.key_count());
    while cursor.key_valid() {
        acc.push(unsafe { cursor.key().downcast::<WideRow>() }.clone());
        cursor.step_key();
    }
    acc
}

fn build_lookups_wide(present: &[WideRow]) -> Vec<WideRow> {
    let mut rng = Xoshiro256StarStar::from_seed(SEED_LOOKUPS);
    let hits = (NUM_LOOKUPS as f64 * HIT_RATIO) as usize;
    let mut result = Vec::with_capacity(NUM_LOOKUPS);
    for _ in 0..hits {
        let idx = rng.gen_range(0..present.len());
        result.push(present[idx].clone());
    }
    for _ in hits..NUM_LOOKUPS {
        result.push(random_wide_row(&mut rng));
    }
    for i in (1..result.len()).rev() {
        let j = rng.gen_range(0..=i);
        result.swap(i, j);
    }
    result
}

fn time_lookups_wide(zset: &OrdZSet<WideRow>, lookups: &[WideRow]) -> (u64, u64) {
    let mut cursor = zset.inner().cursor();
    let start = Instant::now();
    let mut hit_count = 0u64;
    for key in lookups {
        cursor.rewind_keys();
        let target = key.clone();
        cursor.seek_key_with(&|k: &DynData| {
            let k = unsafe { k.downcast::<WideRow>() };
            k.cmp(&target) != std::cmp::Ordering::Less
        });
        if cursor.key_valid() {
            let landed = unsafe { cursor.key().downcast::<WideRow>() };
            if landed == &target {
                hit_count += 1;
            }
        }
        black_box(&cursor);
    }
    let elapsed_ns = start.elapsed().as_nanos() as u64;
    (hit_count, elapsed_ns)
}

fn report(name: &str, storage: bool, hits: u64, elapsed_ns: u64, keys: usize) {
    let storage_label = if storage { "file-backed" } else { "in-memory " };
    let per_lookup_ns = elapsed_ns as f64 / NUM_LOOKUPS as f64;
    println!(
        "{name} {storage_label}: {NUM_LOOKUPS} lookups against {keys} keys in {:.2} ms = {:.1} ns/lookup, {} hits",
        elapsed_ns as f64 / 1e6,
        per_lookup_ns,
        hits
    );
}

fn bench_u64(storage: bool) {
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

    let result: Arc<Mutex<Option<(u64, u64, usize)>>> = Arc::new(Mutex::new(None));
    let result_clone = Arc::clone(&result);
    let handle = Runtime::run(config, move |_parker| {
        let zset = build_zset_u64();
        let present = extract_keys_u64(&zset);
        let lookups = build_lookups_u64(&present);
        let keys = zset.key_count();
        let (hits, elapsed) = time_lookups_u64(&zset, &lookups);
        *result_clone.lock().unwrap() = Some((hits, elapsed, keys));
    })
    .expect("failed to start DBSP runtime");
    handle.kill().expect("failed to kill runtime");
    let (hits, elapsed_ns, keys) = result.lock().unwrap().expect("no result");
    report("u64    ", storage, hits, elapsed_ns, keys);
}

fn bench_string(storage: bool) {
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

    let result: Arc<Mutex<Option<(u64, u64, usize)>>> = Arc::new(Mutex::new(None));
    let result_clone = Arc::clone(&result);
    let handle = Runtime::run(config, move |_parker| {
        let zset = build_zset_string();
        let present = extract_keys_string(&zset);
        let lookups = build_lookups_string(&present);
        let keys = zset.key_count();
        let (hits, elapsed) = time_lookups_string(&zset, &lookups);
        *result_clone.lock().unwrap() = Some((hits, elapsed, keys));
    })
    .expect("failed to start DBSP runtime");
    handle.kill().expect("failed to kill runtime");
    let (hits, elapsed_ns, keys) = result.lock().unwrap().expect("no result");
    report("String ", storage, hits, elapsed_ns, keys);
}

fn bench_wide(storage: bool) {
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

    let result: Arc<Mutex<Option<(u64, u64, usize)>>> = Arc::new(Mutex::new(None));
    let result_clone = Arc::clone(&result);
    let handle = Runtime::run(config, move |_parker| {
        let zset = build_zset_wide();
        let present = extract_keys_wide(&zset);
        let lookups = build_lookups_wide(&present);
        let keys = zset.key_count();
        let (hits, elapsed) = time_lookups_wide(&zset, &lookups);
        *result_clone.lock().unwrap() = Some((hits, elapsed, keys));
    })
    .expect("failed to start DBSP runtime");
    handle.kill().expect("failed to kill runtime");
    let (hits, elapsed_ns, keys) = result.lock().unwrap().expect("no result");
    report("WideRow", storage, hits, elapsed_ns, keys);
}

fn main() {
    println!("seek_key_with micro-benchmark");
    bench_u64(false);
    bench_u64(true);
    bench_string(false);
    bench_string(true);
    bench_wide(false);
    bench_wide(true);
}
