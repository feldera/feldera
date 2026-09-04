//! Benchmark for `ListMerger` over `OrdIndexedZSet` batches.
//!
//! Generates 20M `(key, value, +1)` records split across `N` input batches and
//! merges them into one batch using `ListMerger`.
//!
//! A value is ten fields, and `--value-strings` says how many of them are
//! 16-byte strings rather than `u64`.  That is what decides whether decoding
//! a value costs anything beyond a copy: sixteen bytes is one past what
//! `ArchivedString` keeps inline, so each string field is an allocation on
//! the way out and a copied value avoids it.  A value of ten `u64` has none,
//! which makes it the least favourable shape for copying values and the
//! fairest for measuring what copying keys is worth on its own.
//!
//! Runs both in-memory and file-backed modes, over two key types.  The key
//! type matters because a merge of file-backed batches copies a key rather
//! than decoding and re-encoding it, but only where nothing else needs the
//! key itself: an integer key is the kind a roaring filter is built for, and
//! that filter is fed the key, so integer keys are written decoded however
//! they are read.  A string key is copied, and a string is also where copying
//! has something to save, since decoding one allocates.
//!
//! Run with: `cargo bench -p dbsp --bench list_merger`
//!
//! Takes arguments, to hold one configuration still for an A/B:
//!
//!     --records N     records in total (default 20000000)
//!     --batches N     one batch count instead of 1, 8, 32, 64
//!     --key-range N   one key range instead of 100 and 100000000
//!     --keys u64|str  one key type instead of both
//!     --value-strings N   how many of a value's ten fields are 16-byte
//!                         strings rather than `u64`: 0, 2, 5 or 10
//!                         (default 0)
//!     --storage-only  only the file-backed pass
//!     --repeat N      run each configuration N times and keep the best

use dbsp::circuit::{CircuitConfig, CircuitStorageConfig};
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

/// A key type this benchmark runs over.
///
/// The two are the two sides of the merge's key handling: a merge of
/// file-backed batches copies a string key as it stands and writes an integer
/// key decoded, because an integer key is the kind a roaring filter is built
/// for and that filter is fed the key itself.
trait BenchKey: dbsp::DBData {
    /// What to call it in the results.
    const NAME: &'static str;

    /// The `n`th key, so that the two types see the same key sequence.
    fn nth(n: u64) -> Self;
}

impl BenchKey for u64 {
    const NAME: &'static str = "u64";

    fn nth(n: u64) -> Self {
        n
    }
}

impl BenchKey for String {
    const NAME: &'static str = "String";

    /// Long enough that the archived form keeps its text out of line, which
    /// is what makes decoding one allocate -- the cost a copy avoids.
    fn nth(n: u64) -> Self {
        format!("key-{n:016}-padding")
    }
}

/// What to run, for an A/B that wants one configuration held still.
#[derive(Clone)]
struct Args {
    records: usize,
    batch_counts: Vec<usize>,
    key_ranges: Vec<u64>,
    strings: bool,
    integers: bool,
    value_strings: usize,
    in_memory: bool,
    repeat: usize,
}

impl Args {
    fn parse() -> Self {
        let mut args = Self {
            records: NUM_RECORDS,
            batch_counts: BATCH_COUNTS.to_vec(),
            key_ranges: KEY_RANGES.to_vec(),
            strings: true,
            integers: true,
            value_strings: 0,
            in_memory: true,
            repeat: 1,
        };
        let argv: Vec<String> = std::env::args().collect();
        let mut i = 1;
        while i < argv.len() {
            let flag = argv[i].clone();
            let number = |i: &mut usize| -> u64 {
                *i += 1;
                argv.get(*i)
                    .and_then(|v| v.parse().ok())
                    .unwrap_or_else(|| panic!("{flag} needs a number"))
            };
            match argv[i].as_str() {
                "--records" => args.records = number(&mut i) as usize,
                "--batches" => args.batch_counts = vec![number(&mut i) as usize],
                "--key-range" => args.key_ranges = vec![number(&mut i)],
                "--repeat" => args.repeat = number(&mut i) as usize,
                "--value-strings" => args.value_strings = number(&mut i) as usize,
                "--storage-only" => args.in_memory = false,
                "--keys" => {
                    i += 1;
                    match argv.get(i).map(String::as_str) {
                        Some("u64") => args.strings = false,
                        Some("str") => args.integers = false,
                        other => panic!("--keys wants u64 or str, not {other:?}"),
                    }
                }
                _ => {}
            }
            i += 1;
        }
        args
    }
}

#[derive(Clone)]
struct BenchResult {
    key_type: &'static str,
    value_strings: usize,
    num_batches: usize,
    key_range: u64,
    m_records_per_sec: f64,
}

/// One field of a value.
///
/// A `u64` decodes with a copy; a string decodes with an allocation, because
/// sixteen bytes is one past what `ArchivedString` keeps inline.  That is the
/// difference a copied value avoids, and how much of a value is string is
/// what `--value-strings` sets.
trait Field: dbsp::DBData {
    /// Whether this field is one of the strings, for naming the mix.
    const IS_STRING: bool;

    /// A field's worth of the random stream, one `u64` either way, so that a
    /// run of records draws the same keys whatever the value mix is.
    fn next(rng: &mut Xoshiro256StarStar) -> Self;
}

impl Field for u64 {
    const IS_STRING: bool = false;

    fn next(rng: &mut Xoshiro256StarStar) -> Self {
        rng.next_u64()
    }
}

impl Field for String {
    const IS_STRING: bool = true;

    fn next(rng: &mut Xoshiro256StarStar) -> Self {
        format!("{:016x}", rng.next_u64())
    }
}

/// A value type this benchmark runs over: ten fields, some of them strings.
trait BenchValue: dbsp::DBData {
    /// How many of the ten fields are strings.
    const STRINGS: usize;

    fn next(rng: &mut Xoshiro256StarStar) -> Self;
}

impl<A, B, C, D, E, F, G, H, I, J> BenchValue for Tup10<A, B, C, D, E, F, G, H, I, J>
where
    A: Field,
    B: Field,
    C: Field,
    D: Field,
    E: Field,
    F: Field,
    G: Field,
    H: Field,
    I: Field,
    J: Field,
{
    const STRINGS: usize = A::IS_STRING as usize
        + B::IS_STRING as usize
        + C::IS_STRING as usize
        + D::IS_STRING as usize
        + E::IS_STRING as usize
        + F::IS_STRING as usize
        + G::IS_STRING as usize
        + H::IS_STRING as usize
        + I::IS_STRING as usize
        + J::IS_STRING as usize;

    fn next(rng: &mut Xoshiro256StarStar) -> Self {
        Tup10(
            A::next(rng),
            B::next(rng),
            C::next(rng),
            D::next(rng),
            E::next(rng),
            F::next(rng),
            G::next(rng),
            H::next(rng),
            I::next(rng),
            J::next(rng),
        )
    }
}

/// The mixes `--value-strings` offers: none, a fifth, half, all.
///
/// Each is a type of its own, and every one of them is compiled for every key
/// type, so the list is as short as a sweep can be and still show a trend.
type Values0 = Tup10<u64, u64, u64, u64, u64, u64, u64, u64, u64, u64>;
type Values2 = Tup10<String, String, u64, u64, u64, u64, u64, u64, u64, u64>;
type Values5 = Tup10<String, String, String, String, String, u64, u64, u64, u64, u64>;
type Values10 =
    Tup10<String, String, String, String, String, String, String, String, String, String>;

fn records_in_batch(batch_index: usize, num_batches: usize, num_records: usize) -> usize {
    let base = num_records / num_batches;
    let remainder = num_records % num_batches;
    base + usize::from(batch_index < remainder)
}

/// Kept out of line so that a profiler can tell the two phases apart: with
/// both inlined into their caller, a sample taken during the merge carries
/// this function's scope and reads as though it were still generating.
#[inline(never)]
fn generate_batches<K: BenchKey, V: BenchValue>(
    num_batches: usize,
    key_range: u64,
    num_records: usize,
) -> Vec<OrdIndexedZSet<K, V>> {
    let mut rng = Xoshiro256StarStar::from_seed(SEED);

    (0..num_batches)
        .map(|batch_index| {
            let n = records_in_batch(batch_index, num_batches, num_records);
            let tuples: Vec<Tup2<Tup2<K, V>, ZWeight>> = (0..n)
                .map(|_| {
                    let key = K::nth(rng.gen_range(0..key_range));
                    let value = V::next(&mut rng);
                    Tup2(Tup2(key, value), 1)
                })
                .collect();
            OrdIndexedZSet::from_tuples((), tuples)
        })
        .collect()
}

/// Out of line for the same reason as [`generate_batches`], and because this
/// is the frame a profile is read against.
#[inline(never)]
fn merge_with_list_merger<K: BenchKey, V: BenchValue>(
    batches: Vec<OrdIndexedZSet<K, V>>,
) -> (
    <OrdIndexedZSet<K, V> as TypedBatchReader>::Inner,
    usize,
    BatchLocation,
) {
    type InnerBatch<K, V> = <OrdIndexedZSet<K, V> as TypedBatchReader>::Inner;
    let mut inner_batches: Vec<InnerBatch<K, V>> = batches
        .into_iter()
        .map(|batch| batch.into_inner())
        .collect();
    let factories = inner_batches[0].factories();
    let builder =
        <InnerBatch<K, V> as DynBatch>::Builder::for_merge(&factories, inner_batches.iter(), None);

    let output: InnerBatch<K, V> = ListMerger::merge(
        &factories,
        builder,
        inner_batches
            .iter_mut()
            .map(|batch| batch.consuming_cursor(None, None))
            .collect(),
    );
    let output_len = output.approx_len();
    let actual_location = output.location();
    (output, output_len, actual_location)
}

/// Runs every configuration for one key type, appending to `results`.
fn bench_key_type<K: BenchKey, V: BenchValue>(args: &Args, results: &Mutex<Vec<BenchResult>>) {
    for &num_batches in &args.batch_counts {
        assert!(num_batches > 0 && num_batches <= 64);
        for &key_range in &args.key_ranges {
            println!(
                "\nMerging {} records across {num_batches} batches of {} keys \
                 with {} of ten value fields strings (key_range={key_range})...",
                args.records,
                K::NAME,
                V::STRINGS,
            );
            let mut best = f64::MIN;
            for _ in 0..args.repeat {
                let batches = generate_batches::<K, V>(num_batches, key_range, args.records);
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

                let m_records_per_sec = args.records as f64 / elapsed.as_secs_f64() / 1_000_000.0;
                println!(
                    "  merged in {elapsed:?} ({m_records_per_sec:.1} M input records/s), output_location={actual_location:?}",
                );
                best = best.max(m_records_per_sec);
            }
            results.lock().unwrap().push(BenchResult {
                key_type: K::NAME,
                value_strings: V::STRINGS,
                num_batches,
                key_range,
                m_records_per_sec: best,
            });
        }
    }
}

fn bench(generate_on_storage: bool, args: &Args) {
    let temp = tempdir().expect("failed to create temp directory");
    let config = CircuitConfig::with_workers(1).with_storage(Some(
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
    ));

    let results: Arc<Mutex<Vec<BenchResult>>> = Arc::new(Mutex::new(Vec::new()));
    let results_clone = Arc::clone(&results);

    let run_args = args.clone();
    let handle = Runtime::run(config, move |_parker| {
        // The value type is a type, and the flag is a number, so the choice
        // has to be made here rather than carried in `Args`.
        fn run<V: BenchValue>(args: &Args, results: &Mutex<Vec<BenchResult>>) {
            if args.integers {
                bench_key_type::<u64, V>(args, results);
            }
            if args.strings {
                bench_key_type::<String, V>(args, results);
            }
        }
        match run_args.value_strings {
            0 => run::<Values0>(&run_args, &results_clone),
            2 => run::<Values2>(&run_args, &results_clone),
            5 => run::<Values5>(&run_args, &results_clone),
            10 => run::<Values10>(&run_args, &results_clone),
            other => panic!("--value-strings wants 0, 2, 5 or 10, not {other}"),
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
    println!("┌────────┬─────────┬───────────┬─────────────┬─────────────┐");
    println!("│ keys   │ strings │ # batches │   key range │ M records/s │");
    println!("├────────┼─────────┼───────────┼─────────────┼─────────────┤");
    for result in &results {
        println!(
            "│ {:>6} │ {:>4}/10 │ {:>9} │ {:>11} │ {:>11.1} │",
            result.key_type,
            result.value_strings,
            result.num_batches,
            result.key_range,
            result.m_records_per_sec,
        );
    }
    println!("└────────┴─────────┴───────────┴─────────────┴─────────────┘");
}

fn main() {
    let args = Args::parse();
    if args.in_memory {
        println!("Running ListMerger benchmark with in-memory batches...");
        bench(false, &args);
    }

    println!("\nRunning ListMerger benchmark with file-backed batches...");
    bench(true, &args);
}
