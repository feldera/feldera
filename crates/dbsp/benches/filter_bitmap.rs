//! Membership benchmark for `fastbloom` vs `roaring`.
//!
//! Example:
//! `cargo bench -p dbsp --bench filter_bitmap -- --csv-output filter_bitmap.csv`

use clap::{Parser, ValueEnum};
use csv::Writer;
use dbsp::storage::file::BLOOM_FILTER_FALSE_POSITIVE_RATE;
use fastbloom::BloomFilter;
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
use roaring::RoaringBitmap;
use serde::Serialize;
use std::{
    fmt::{Display, Formatter},
    fs::File,
    mem::size_of,
    path::PathBuf,
    time::Instant,
};
use tikv_jemallocator::Jemalloc;

#[global_allocator]
static ALLOC: Jemalloc = Jemalloc;

const DEFAULT_BLOOM_SEED: u128 = 42;
const MIN_BLOOM_EXPECTED_ITEMS: u64 = 64;
const U32_KEY_SPACE_SIZE: u64 = u32::MAX as u64 + 1;
const DEFAULT_LOOKUP_LIMIT: u64 = 50_000_000;

// Mirror the spine_async size bands, capped to what fits in the u32 keyspace.
const DEFAULT_SPINE_LEVEL_SIZES: [u64; 6] = [
    14_999,
    99_999,
    999_999,
    9_999_999,
    99_999_999,
    999_999_999,
];

fn main() {
    let args = Args::parse();
    let num_elements_list = args.num_elements();
    args.validate(&num_elements_list);

    let csv_file = File::create(&args.csv_output)
        .unwrap_or_else(|error| panic!("failed to create {}: {error}", args.csv_output.display()));
    let mut csv_writer = Writer::from_writer(csv_file);

    println!("benchmark=filter_bitmap");
    println!(
        "num_elements={}",
        num_elements_list
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(",")
    );
    println!("repetitions={}", args.repetitions);
    println!("insert_order={}", args.insert_order);
    println!("lookup_order={}", args.lookup_order);
    println!("insert_seed={}", args.insert_seed);
    println!("lookup_seed={}", args.lookup_seed);
    println!(
        "structures={}",
        args.structures
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(",")
    );
    println!(
        "bloom_false_positive_rate={}",
        args.bloom_false_positive_rate
    );
    println!("bloom_seed={}", args.bloom_seed);
    println!("csv_output={}", args.csv_output.display());
    println!();

    for &num_elements in &num_elements_list {
        let lookup_count = args
            .lookup_count
            .unwrap_or(num_elements.min(DEFAULT_LOOKUP_LIMIT));
            let max_false_positive_lookup_count = U32_KEY_SPACE_SIZE - num_elements;
            let false_positive_lookup_count = args
                .false_positive_lookup_count
                .unwrap_or(lookup_count.min(max_false_positive_lookup_count));
            let bloom_expected_items = args
                .bloom_expected_items
                .unwrap_or(num_elements)
                .max(MIN_BLOOM_EXPECTED_ITEMS);

        for structure in &args.structures {
            let result = match structure {
                Structure::Bloom => benchmark_bloom(
                    &args,
                    num_elements,
                    lookup_count,
                    false_positive_lookup_count,
                    bloom_expected_items,
                ),
                Structure::Roaring => benchmark_roaring(&args, num_elements, lookup_count),
            };

            print_report(
                *structure,
                &result,
                num_elements,
                lookup_count,
                false_positive_lookup_count,
            );

            csv_writer
                .serialize(CsvRow::from_result(
                    *structure,
                    &args,
                    num_elements,
                    lookup_count,
                    false_positive_lookup_count,
                    bloom_expected_items,
                    &result,
                ))
                .expect("failed to write CSV row");
            csv_writer.flush().expect("failed to flush CSV writer");
        }
    }
}

#[derive(Parser, Debug, Clone)]
#[command(name = "filter_bitmap")]
#[command(about = "Benchmark fastbloom against roaring bitmap for u32 membership queries")]
struct Args {
    /// Comma-separated input sizes. Defaults to spine_async level-cap sizes that fit in u32.
    #[arg(long, value_name = "CSV")]
    num_elements: Option<String>,

    /// Number of successful lookups to benchmark for each input size.
    /// Defaults to min(num_elements, 50_000_000).
    #[arg(long)]
    lookup_count: Option<u64>,

    /// Number of negative lookups used to measure bloom false positives for each input size.
    #[arg(long)]
    false_positive_lookup_count: Option<u64>,

    /// Number of repeated benchmark runs used to compute min/avg/max/std.
    #[arg(long, default_value_t = 5)]
    repetitions: usize,

    /// Structures to benchmark.
    #[arg(long, value_delimiter = ',', default_value = "bloom,roaring")]
    structures: Vec<Structure>,

    /// Insert order over the chosen keyset.
    #[arg(long, default_value_t = Order::Random)]
    insert_order: Order,

    /// Lookup order over the chosen keyset or sampled subset.
    #[arg(long, default_value_t = Order::Random)]
    lookup_order: Order,

    /// Seed used when `insert-order=random`.
    #[arg(long, default_value_t = 0)]
    insert_seed: u64,

    /// Seed used when `lookup-order=random`.
    #[arg(long, default_value_t = 1)]
    lookup_seed: u64,

    /// Bloom filter false-positive rate. Defaults to DBSP storage default.
    #[arg(long, default_value_t = BLOOM_FILTER_FALSE_POSITIVE_RATE)]
    bloom_false_positive_rate: f64,

    /// Bloom filter seed. Defaults to DBSP storage seed.
    #[arg(long, default_value_t = DEFAULT_BLOOM_SEED)]
    bloom_seed: u128,

    /// Expected number of items passed to the bloom filter builder for each input size.
    #[arg(long)]
    bloom_expected_items: Option<u64>,

    /// Output CSV path.
    #[arg(long, default_value = "filter_bitmap.csv")]
    csv_output: PathBuf,

    // When running with `cargo bench` the binary gets the `--bench` flag, so we
    // have to parse and ignore it so clap doesn't reject it.
    #[doc(hidden)]
    #[arg(long = "bench", hide = true)]
    __bench: bool,
}

impl Args {
    fn num_elements(&self) -> Vec<u64> {
        match &self.num_elements {
            Some(csv) => parse_u64_csv(csv),
            None => DEFAULT_SPINE_LEVEL_SIZES.to_vec(),
        }
    }

    fn validate(&self, num_elements_list: &[u64]) {
        assert!(
            !num_elements_list.is_empty(),
            "--num-elements must select at least one size"
        );
        assert!(
            self.repetitions > 0,
            "--repetitions must be greater than zero"
        );
        assert!(
            !self.structures.is_empty(),
            "--structures must select at least one structure"
        );
        assert!(
            self.bloom_false_positive_rate > 0.0 && self.bloom_false_positive_rate < 1.0,
            "--bloom-false-positive-rate must be between 0 and 1"
        );

        for &num_elements in num_elements_list {
            assert!(
                num_elements > 0,
                "--num-elements values must be greater than zero"
            );
            assert!(
                num_elements <= u32::MAX as u64,
                "--num-elements values must be <= {}",
                u32::MAX
            );
            assert!(
                self.lookup_count
                    .unwrap_or(num_elements.min(DEFAULT_LOOKUP_LIMIT))
                    <= num_elements,
                "--lookup-count must be <= each --num-elements value"
            );
            let max_false_positive_lookup_count = U32_KEY_SPACE_SIZE - num_elements;
            assert!(
                self.false_positive_lookup_count
                    .unwrap_or(self.lookup_count.unwrap_or(num_elements).min(max_false_positive_lookup_count))
                    <= max_false_positive_lookup_count,
                "--false-positive-lookup-count must be <= {} for num_elements={num_elements}",
                max_false_positive_lookup_count
            );
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum Structure {
    Bloom,
    Roaring,
}

impl Display for Structure {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Bloom => f.write_str("bloom"),
            Self::Roaring => f.write_str("roaring"),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum Order {
    Sequential,
    Random,
}

impl Display for Order {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sequential => f.write_str("sequential"),
            Self::Random => f.write_str("random"),
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct AffinePermutation {
    len: u64,
    multiplier: u64,
    offset: u64,
}

impl AffinePermutation {
    fn sequential(len: u64) -> Self {
        Self {
            len,
            multiplier: 1,
            offset: 0,
        }
    }

    fn random(len: u64, seed: u64) -> Self {
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        let mut multiplier = (rng.next_u64() % len) | 1;
        while gcd(multiplier, len) != 1 {
            multiplier = (multiplier + 2) % len;
            if multiplier == 0 {
                multiplier = 1;
            }
        }
        let offset = rng.next_u64() % len;
        Self {
            len,
            multiplier,
            offset,
        }
    }

    fn for_order(len: u64, order: Order, seed: u64) -> Self {
        match order {
            Order::Sequential => Self::sequential(len),
            Order::Random => Self::random(len, seed),
        }
    }

    fn index_at(&self, position: u64) -> u64 {
        debug_assert!(position < self.len);
        (self
            .multiplier
            .wrapping_mul(position)
            .wrapping_add(self.offset))
            % self.len
    }

    fn key_at(&self, position: u64) -> u32 {
        self.index_at(position) as u32
    }

    fn absent_key_at(&self, position: u64, inserted_keys: u64) -> u32 {
        (inserted_keys + self.index_at(position)) as u32
    }
}

fn gcd(mut lhs: u64, mut rhs: u64) -> u64 {
    while rhs != 0 {
        let next = lhs % rhs;
        lhs = rhs;
        rhs = next;
    }
    lhs
}

fn parse_u64_csv(csv: &str) -> Vec<u64> {
    let mut out: Vec<u64> = csv
        .split(',')
        .filter(|entry| !entry.trim().is_empty())
        .map(|entry| {
            entry
                .trim()
                .parse::<u64>()
                .unwrap_or_else(|error| panic!("invalid u64 in --num-elements: {entry} ({error})"))
        })
        .collect();
    out.sort_unstable();
    out.dedup();
    out
}

#[derive(Debug, Clone, Copy)]
struct SummaryStats {
    min: f64,
    avg: f64,
    max: f64,
    stddev: f64,
}

impl SummaryStats {
    fn from_samples(samples: &[f64]) -> Self {
        let min = samples.iter().copied().fold(f64::INFINITY, f64::min);
        let max = samples.iter().copied().fold(f64::NEG_INFINITY, f64::max);
        let avg = samples.iter().sum::<f64>() / samples.len() as f64;
        let variance = samples
            .iter()
            .map(|sample| {
                let delta = *sample - avg;
                delta * delta
            })
            .sum::<f64>()
            / samples.len() as f64;
        Self {
            min,
            avg,
            max,
            stddev: variance.sqrt(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct BenchmarkResult {
    insert_ns_per_element: SummaryStats,
    lookup_ns_per_element: SummaryStats,
    bytes_used: usize,
    false_positive_rate_percent: Option<SummaryStats>,
}

#[derive(Debug, Serialize)]
struct CsvRow {
    structure: &'static str,
    num_elements: u64,
    lookup_count: u64,
    false_positive_lookup_count: u64,
    repetitions: usize,
    insert_order: &'static str,
    lookup_order: &'static str,
    insert_seed: u64,
    lookup_seed: u64,
    bloom_false_positive_rate_target_percent: f64,
    bloom_seed: u128,
    bloom_expected_items: u64,
    bytes_used: usize,
    bytes_per_element: f64,
    bits_per_element: Option<f64>,
    insert_ns_per_element_min: f64,
    insert_ns_per_element_avg: f64,
    insert_ns_per_element_max: f64,
    insert_ns_per_element_stddev: f64,
    lookup_ns_per_element_min: f64,
    lookup_ns_per_element_avg: f64,
    lookup_ns_per_element_max: f64,
    lookup_ns_per_element_stddev: f64,
    false_positive_rate_percent_min: Option<f64>,
    false_positive_rate_percent_avg: Option<f64>,
    false_positive_rate_percent_max: Option<f64>,
    false_positive_rate_percent_stddev: Option<f64>,
}

impl CsvRow {
    fn from_result(
        structure: Structure,
        args: &Args,
        num_elements: u64,
        lookup_count: u64,
        false_positive_lookup_count: u64,
        bloom_expected_items: u64,
        result: &BenchmarkResult,
    ) -> Self {
        let bits_per_element = (structure == Structure::Bloom)
            .then_some((result.bytes_used as f64 * 8.0) / num_elements as f64);
        let false_positive_stats = result.false_positive_rate_percent;

        Self {
            structure: structure.as_str(),
            num_elements,
            lookup_count,
            false_positive_lookup_count,
            repetitions: args.repetitions,
            insert_order: args.insert_order.as_str(),
            lookup_order: args.lookup_order.as_str(),
            insert_seed: args.insert_seed,
            lookup_seed: args.lookup_seed,
            bloom_false_positive_rate_target_percent: args.bloom_false_positive_rate * 100.0,
            bloom_seed: args.bloom_seed,
            bloom_expected_items,
            bytes_used: result.bytes_used,
            bytes_per_element: result.bytes_used as f64 / num_elements as f64,
            bits_per_element,
            insert_ns_per_element_min: result.insert_ns_per_element.min,
            insert_ns_per_element_avg: result.insert_ns_per_element.avg,
            insert_ns_per_element_max: result.insert_ns_per_element.max,
            insert_ns_per_element_stddev: result.insert_ns_per_element.stddev,
            lookup_ns_per_element_min: result.lookup_ns_per_element.min,
            lookup_ns_per_element_avg: result.lookup_ns_per_element.avg,
            lookup_ns_per_element_max: result.lookup_ns_per_element.max,
            lookup_ns_per_element_stddev: result.lookup_ns_per_element.stddev,
            false_positive_rate_percent_min: false_positive_stats.map(|stats| stats.min),
            false_positive_rate_percent_avg: false_positive_stats.map(|stats| stats.avg),
            false_positive_rate_percent_max: false_positive_stats.map(|stats| stats.max),
            false_positive_rate_percent_stddev: false_positive_stats.map(|stats| stats.stddev),
        }
    }
}

impl Structure {
    fn as_str(self) -> &'static str {
        match self {
            Self::Bloom => "bloom",
            Self::Roaring => "roaring",
        }
    }
}

impl Order {
    fn as_str(self) -> &'static str {
        match self {
            Self::Sequential => "sequential",
            Self::Random => "random",
        }
    }
}

fn benchmark_bloom(
    args: &Args,
    num_elements: u64,
    lookup_count: u64,
    false_positive_lookup_count: u64,
    bloom_expected_items: u64,
) -> BenchmarkResult {
    let mut insert_samples = Vec::with_capacity(args.repetitions);
    let mut lookup_samples = Vec::with_capacity(args.repetitions);
    let mut false_positive_rate_percent_samples = Vec::with_capacity(args.repetitions);
    let mut bytes_used = 0;
    let expected_items =
        usize::try_from(bloom_expected_items).expect("bloom expected items must fit in usize");

    for repetition in 0..args.repetitions {
        let insert_permutation = AffinePermutation::for_order(
            num_elements,
            args.insert_order,
            args.insert_seed.wrapping_add(repetition as u64),
        );
        let lookup_permutation = AffinePermutation::for_order(
            num_elements,
            args.lookup_order,
            args.lookup_seed.wrapping_add(repetition as u64),
        );
        let false_positive_permutation = AffinePermutation::for_order(
            U32_KEY_SPACE_SIZE - num_elements,
            args.lookup_order,
            args.lookup_seed.wrapping_add(repetition as u64),
        );

        let mut bloom = BloomFilter::with_false_pos(args.bloom_false_positive_rate)
            .seed(&args.bloom_seed)
            .expected_items(expected_items.max(MIN_BLOOM_EXPECTED_ITEMS as usize));

        let insert_started = Instant::now();
        for index in 0..num_elements {
            let key = insert_permutation.key_at(index);
            bloom.insert(&key);
        }
        let insert_elapsed = insert_started.elapsed();

        let lookup_started = Instant::now();
        let mut hits = 0u64;
        for index in 0..lookup_count {
            let key = lookup_permutation.key_at(index);
            hits += u64::from(bloom.contains(&key));
        }
        let lookup_elapsed = lookup_started.elapsed();

        assert_eq!(hits, lookup_count, "expected all lookup keys to be present");

        let mut false_positives = 0u64;
        for index in 0..false_positive_lookup_count {
            let key = false_positive_permutation.absent_key_at(index, num_elements);
            false_positives += u64::from(bloom.contains(&key));
        }

        bytes_used = bloom.as_slice().len() * size_of::<u64>();
        insert_samples.push(insert_elapsed.as_nanos() as f64 / num_elements as f64);
        lookup_samples.push(lookup_elapsed.as_nanos() as f64 / lookup_count as f64);
        false_positive_rate_percent_samples
            .push((false_positives as f64 / false_positive_lookup_count as f64) * 100.0);
    }

    BenchmarkResult {
        insert_ns_per_element: SummaryStats::from_samples(&insert_samples),
        lookup_ns_per_element: SummaryStats::from_samples(&lookup_samples),
        bytes_used,
        false_positive_rate_percent: Some(SummaryStats::from_samples(
            &false_positive_rate_percent_samples,
        )),
    }
}

fn benchmark_roaring(args: &Args, num_elements: u64, lookup_count: u64) -> BenchmarkResult {
    let mut insert_samples = Vec::with_capacity(args.repetitions);
    let mut lookup_samples = Vec::with_capacity(args.repetitions);
    let mut bytes_used = 0;

    for repetition in 0..args.repetitions {
        let insert_permutation = AffinePermutation::for_order(
            num_elements,
            args.insert_order,
            args.insert_seed.wrapping_add(repetition as u64),
        );
        let lookup_permutation = AffinePermutation::for_order(
            num_elements,
            args.lookup_order,
            args.lookup_seed.wrapping_add(repetition as u64),
        );

        let mut bitmap = RoaringBitmap::new();

        let insert_started = Instant::now();
        for index in 0..num_elements {
            let key = insert_permutation.key_at(index);
            let _ = bitmap.insert(key);
        }
        let insert_elapsed = insert_started.elapsed();

        let lookup_started = Instant::now();
        let mut hits = 0u64;
        for index in 0..lookup_count {
            let key = lookup_permutation.key_at(index);
            hits += u64::from(bitmap.contains(key));
        }
        let lookup_elapsed = lookup_started.elapsed();

        assert_eq!(hits, lookup_count, "expected all lookup keys to be present");
        bytes_used = bitmap.serialized_size();
        insert_samples.push(insert_elapsed.as_nanos() as f64 / num_elements as f64);
        lookup_samples.push(lookup_elapsed.as_nanos() as f64 / lookup_count as f64);
    }

    BenchmarkResult {
        insert_ns_per_element: SummaryStats::from_samples(&insert_samples),
        lookup_ns_per_element: SummaryStats::from_samples(&lookup_samples),
        bytes_used,
        false_positive_rate_percent: None,
    }
}

fn print_report(
    structure: Structure,
    result: &BenchmarkResult,
    num_elements: u64,
    lookup_count: u64,
    false_positive_lookup_count: u64,
) {
    println!("structure={structure}");
    println!("num_elements={num_elements}");
    println!("bytes_used={}", result.bytes_used);
    println!("bytes_used_human={}", format_bytes(result.bytes_used as f64));
    println!(
        "bytes_per_element={}",
        format_bytes(result.bytes_used as f64 / num_elements as f64)
    );
    if structure == Structure::Bloom {
        println!(
            "bits_per_element={:.6}",
            (result.bytes_used as f64 * 8.0) / num_elements as f64
        );
    }
    print_stats("insert_ns_per_element", result.insert_ns_per_element);
    print_stats("lookup_ns_per_element", result.lookup_ns_per_element);
    println!("lookup_count={lookup_count}");
    if let Some(stats) = result.false_positive_rate_percent {
        println!("false_positive_lookup_count={false_positive_lookup_count}");
        print_stats("false_positive_rate_percent", stats);
    }
    println!();
}

fn print_stats(label: &str, stats: SummaryStats) {
    println!("{label}.min={:.6}", stats.min);
    println!("{label}.avg={:.6}", stats.avg);
    println!("{label}.max={:.6}", stats.max);
    println!("{label}.stddev={:.6}", stats.stddev);
}

fn format_bytes(bytes: f64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];

    let mut value = bytes;
    let mut unit_index = 0;
    while value >= 1024.0 && unit_index + 1 < UNITS.len() {
        value /= 1024.0;
        unit_index += 1;
    }

    format!("{value:.6} {}", UNITS[unit_index])
}
