//! Micro-benchmark for point lookups in a Z-set: `Cursor::seek_key_exact`
//! and the predicate form `Cursor::seek_key_with`, against in-memory and
//! file-backed batches, with `u64`, `String`, and 15-`String` (`WideRow`)
//! keys.
//!
//! Builds one batch of `--keys` distinct random keys, then times `--lookups`
//! random lookups, half of which hit, and reports ns/lookup. The lookup keys
//! are a fixed pseudo-random sequence, so runs are comparable.
//!
//! ```text
//! cargo bench -p dbsp --bench seek_key_exact
//! ```
//!
//! To profile the hot path, restrict the run to one case and give it enough
//! lookups for the setup not to matter:
//!
//! ```text
//! samply record cargo bench -p dbsp --bench seek_key_exact -- \
//!     --key-type u64 --storage file --seek exact --lookups 10000000
//! ```

use clap::{Parser, ValueEnum};
use dbsp::circuit::{CircuitConfig, CircuitStorageConfig};
use dbsp::dynamic::{DowncastTrait, DynData, Erase};
use dbsp::typed_batch::BatchReader as TypedBatchReader;
use dbsp::{
    DBData, OrdZSet, Runtime, ZWeight,
    trace::{BatchReader, Cursor},
    utils::Tup2,
};
use feldera_macros::{HashRepr, IsNone, OrdRepr};
use feldera_types::config::{StorageCacheConfig, StorageConfig, StorageOptions};
use rand::{Rng, SeedableRng};
use rand_xoshiro::Xoshiro256StarStar;
use rkyv::{Archive, Deserialize, Serialize};
use size_of::SizeOf;
use std::hint::black_box;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tempfile::tempdir;

#[derive(Parser, Debug)]
#[command(name = "seek_key_exact")]
#[command(about = "Point-lookup throughput of Z-set cursors")]
struct Args {
    /// Distinct keys in the batch.
    #[arg(long, default_value_t = 1_000_000)]
    keys: usize,

    /// Lookups to time; half of them hit.
    #[arg(long, default_value_t = 500_000)]
    lookups: usize,

    /// Key types to run.
    #[arg(
        long,
        value_enum,
        value_delimiter = ',',
        default_value = "u64,string,wide"
    )]
    key_type: Vec<KeyType>,

    /// Where the batch lives.
    #[arg(long, value_enum, value_delimiter = ',', default_value = "memory,file")]
    storage: Vec<Storage>,

    /// Which seek to time.
    #[arg(
        long,
        value_enum,
        value_delimiter = ',',
        default_value = "exact,predicate"
    )]
    seek: Vec<Seek>,

    /// Ignored; cargo passes it to `harness = false` bench targets.
    #[doc(hidden)]
    #[arg(long = "bench", hide = true)]
    __bench: bool,
}

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
enum KeyType {
    U64,
    String,
    Wide,
}

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
enum Storage {
    Memory,
    File,
}

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
enum Seek {
    /// `seek_key_exact`: the key compares in its archived form.
    Exact,
    /// `seek_key_with` with a `>= target` predicate: every probed key is
    /// deserialized for the predicate.
    Predicate,
}

const SEED_KEYS: [u8; 32] = [
    0x7f, 0xc3, 0x59, 0x18, 0x45, 0x19, 0xc0, 0xaa, 0xd2, 0xec, 0x31, 0x26, 0xbb, 0x74, 0x2f, 0x8b,
    0x11, 0x7d, 0x0c, 0xe4, 0x64, 0xbf, 0x72, 0x17, 0x46, 0x28, 0x46, 0x42, 0xb2, 0x4b, 0x72, 0x18,
];
const SEED_LOOKUPS: [u8; 32] = [
    0xa1, 0x14, 0x9d, 0x5f, 0x22, 0x80, 0xb3, 0xc7, 0x66, 0x10, 0x4e, 0xab, 0x09, 0x77, 0x5e, 0xcc,
    0x3b, 0xd2, 0x44, 0x18, 0x91, 0xa8, 0x6c, 0x55, 0x77, 0xee, 0x4f, 0x12, 0x88, 0x33, 0xb5, 0x6d,
];

/// A key type the benchmark can generate.
trait BenchKey: DBData {
    const NAME: &'static str;

    fn random(rng: &mut Xoshiro256StarStar) -> Self;
}

impl BenchKey for u64 {
    const NAME: &'static str = "u64";

    fn random(rng: &mut Xoshiro256StarStar) -> Self {
        rng.r#gen::<u64>() & ((1 << 48) - 1)
    }
}

impl BenchKey for String {
    const NAME: &'static str = "String";

    fn random(rng: &mut Xoshiro256StarStar) -> Self {
        let n = rng.r#gen::<u64>() & ((1 << 40) - 1);
        format!("key-{n:020}")
    }
}

/// A row wide enough that deserializing it costs 15 allocations.
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
    HashRepr,
    IsNone,
    OrdRepr,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
struct WideRow {
    fields: [String; 15],
}

impl BenchKey for WideRow {
    const NAME: &'static str = "WideRow";

    fn random(rng: &mut Xoshiro256StarStar) -> Self {
        WideRow {
            fields: std::array::from_fn(|_| format!("s-{:020x}", rng.r#gen::<u64>())),
        }
    }
}

/// `n` distinct random keys, sorted.
fn random_keys<K: BenchKey>(n: usize) -> Vec<K> {
    let mut rng = Xoshiro256StarStar::from_seed(SEED_KEYS);
    let mut keys: Vec<K> = (0..n).map(|_| K::random(&mut rng)).collect();
    keys.sort();
    keys.dedup();
    keys
}

/// `n` lookup keys in random order: half drawn from `present`, half random
/// keys that are not in `present`.
fn random_lookups<K: BenchKey>(present: &[K], n: usize) -> Vec<K> {
    let mut rng = Xoshiro256StarStar::from_seed(SEED_LOOKUPS);
    let hits = n / 2;
    let mut lookups: Vec<K> = (0..hits)
        .map(|_| present[rng.gen_range(0..present.len())].clone())
        .collect();
    while lookups.len() < n {
        let key = K::random(&mut rng);
        if present.binary_search(&key).is_err() {
            lookups.push(key);
        }
    }
    for i in (1..lookups.len()).rev() {
        lookups.swap(i, rng.gen_range(0..=i));
    }
    lookups
}

/// Times `lookups` against `zset`, and returns the elapsed nanoseconds and
/// the number of hits.
fn time_lookups<K: BenchKey>(zset: &OrdZSet<K>, lookups: &[K], seek: Seek) -> (u64, usize) {
    let mut cursor = zset.inner().cursor();
    let mut hits = 0;
    let start = Instant::now();
    for target in lookups {
        cursor.rewind_keys();
        let hit = match seek {
            Seek::Exact => cursor.seek_key_exact(target.erase(), None),
            Seek::Predicate => {
                // SAFETY: the cursor walks an `OrdZSet<K>`, so every key it
                // hands out or lands on is a `K`.
                cursor.seek_key_with(&|key: &DynData| unsafe { key.downcast::<K>() } >= target);
                cursor.key_valid() && unsafe { cursor.key().downcast::<K>() } == target
            }
        };
        hits += hit as usize;
        black_box(&cursor);
    }
    (start.elapsed().as_nanos() as u64, hits)
}

fn bench<K: BenchKey>(args: &Args, storage: Storage, seek: Seek) {
    let temp = tempdir().expect("failed to create temp dir for storage");
    let storage_config = (storage == Storage::File).then(|| {
        CircuitStorageConfig::for_config(
            StorageConfig {
                path: temp.path().to_string_lossy().into_owned(),
                cache: StorageCacheConfig::default(),
            },
            StorageOptions {
                // Every batch with content goes to storage.
                min_step_storage_bytes: Some(0),
                ..StorageOptions::default()
            },
        )
        .expect("failed to configure storage")
    });
    let config = CircuitConfig::with_workers(1).with_storage(storage_config);

    // The batch must be built and searched on a runtime thread, which is
    // what decides between memory and storage.
    let result = Arc::new(Mutex::new(None));
    let handle = {
        let result = Arc::clone(&result);
        let (n_keys, n_lookups) = (args.keys, args.lookups);
        Runtime::run(config, move |_parker| {
            let present = random_keys::<K>(n_keys);
            let lookups = random_lookups(&present, n_lookups);
            let tuples = present
                .into_iter()
                .map(|key| Tup2(Tup2(key, ()), 1 as ZWeight))
                .collect();
            let zset = OrdZSet::<K>::from_tuples((), tuples);
            let (elapsed_ns, hits) = time_lookups(&zset, &lookups, seek);
            *result.lock().unwrap() = Some((elapsed_ns, hits, zset.key_count()));
        })
        .expect("failed to start DBSP runtime")
    };
    handle.join().expect("DBSP runtime failed");

    let (elapsed_ns, hits, keys) = result.lock().unwrap().expect("no result");
    println!(
        "{:<8} {:<10} {:<10} {} lookups against {keys} keys in {:.1} ms = {:.1} ns/lookup, {hits} hits",
        K::NAME,
        format!("{storage:?}").to_lowercase(),
        format!("{seek:?}").to_lowercase(),
        args.lookups,
        elapsed_ns as f64 / 1e6,
        elapsed_ns as f64 / args.lookups as f64,
    );
}

fn main() {
    let args = Args::parse();
    for &key_type in &args.key_type {
        for &storage in &args.storage {
            for &seek in &args.seek {
                match key_type {
                    KeyType::U64 => bench::<u64>(&args, storage, seek),
                    KeyType::String => bench::<String>(&args, storage, seek),
                    KeyType::Wide => bench::<WideRow>(&args, storage, seek),
                }
            }
        }
    }
}
