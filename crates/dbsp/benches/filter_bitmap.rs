//! Membership benchmark for `fastbloom` vs `roaring`.
//!
//! Examples:
//! `cargo bench -p dbsp --bench filter_bitmap -- --csv-output filter_bitmap.csv`
//! `cargo bench -p dbsp --bench filter_bitmap -- --key-types u32,u64 --key-spaces consecutive,full_range`

use clap::{Parser, ValueEnum};
use csv::Writer;
use dbsp::storage::file::BLOOM_FILTER_FALSE_POSITIVE_RATE;
use fastbloom::BloomFilter;
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
use rand_distr::{Distribution, Normal};
use roaring::{RoaringBitmap, RoaringTreemap};
use serde::Serialize;
use std::{
    fmt::{Display, Formatter},
    fs::File,
    mem::size_of_val,
    path::PathBuf,
    time::Instant,
};

const DEFAULT_BLOOM_SEED: u128 = 42;
const MIN_BLOOM_EXPECTED_ITEMS: u64 = 64;
const U32_KEY_SPACE_SIZE: u64 = u32::MAX as u64 + 1;
const DEFAULT_LOOKUP_LIMIT: u64 = 50_000_000;
const DEFAULT_KEY_EPS_VALUES: [f64; 6] = [1e-6, 1e-4, 1e-3, 1e-2, 1e-1, 5e-1];

// Mirror the spine_async size bands and include the near-full u32 domain case.
const DEFAULT_SPINE_LEVEL_SIZES: [u64; 6] =
    [14_999, 99_999, 999_999, 9_999_999, 99_999_999, 999_999_999];

fn main() {
    let args = Args::parse();
    let key_types = args.key_types();
    let key_spaces = args.key_spaces();
    let num_elements_list = args.num_elements();
    args.validate(&key_types, &key_spaces, &num_elements_list);

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
    println!("key_space_seed={}", args.key_space_seed);
    println!(
        "key_types={}",
        key_types
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(",")
    );
    println!(
        "key_spaces={}",
        key_spaces
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(",")
    );
    println!(
        "key_eps={}",
        args.key_eps()
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(",")
    );
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

    for &key_type in &key_types {
        for &key_space in &key_spaces {
            for key_eps in args.key_eps_for(key_space) {
                let config = BenchmarkConfig {
                    key_type,
                    key_space,
                    key_eps,
                };

                for &num_elements in &num_elements_list {
                    let lookup_count = args.lookup_count_for(num_elements);
                    let false_positive_lookup_count =
                        args.false_positive_lookup_count_for(config, num_elements, lookup_count);
                    let bloom_expected_items = args
                        .bloom_expected_items
                        .unwrap_or(num_elements)
                        .max(MIN_BLOOM_EXPECTED_ITEMS);

                    for structure in &args.structures {
                        let result = match structure {
                            Structure::Bloom => benchmark_bloom(
                                &args,
                                config,
                                num_elements,
                                lookup_count,
                                false_positive_lookup_count,
                                bloom_expected_items,
                            ),
                            Structure::Roaring => {
                                benchmark_roaring(&args, config, num_elements, lookup_count)
                            }
                        };

                        print_report(
                            *structure,
                            config,
                            &result,
                            num_elements,
                            lookup_count,
                            false_positive_lookup_count,
                        );

                        csv_writer
                            .serialize(CsvRow::from_result(
                                CsvRowContext {
                                    structure: *structure,
                                    config,
                                    args: &args,
                                    num_elements,
                                    lookup_count,
                                    false_positive_lookup_count,
                                    bloom_expected_items,
                                },
                                &result,
                            ))
                            .expect("failed to write CSV row");
                        csv_writer.flush().expect("failed to flush CSV writer");
                    }
                }
            }
        }
    }
}

#[derive(Parser, Debug, Clone)]
#[command(name = "filter_bitmap")]
#[command(about = "Benchmark fastbloom against roaring bitmap or treemap membership queries")]
struct Args {
    /// Comma-separated input sizes. Underscores and `u32::MAX` are accepted.
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
    #[arg(long, default_value_t = 3)]
    repetitions: usize,

    /// Structures to benchmark.
    #[arg(long, value_delimiter = ',', default_value = "bloom,roaring")]
    structures: Vec<Structure>,

    /// Key types to benchmark.
    #[arg(long, value_delimiter = ',', default_value = "u32")]
    key_types: Vec<KeyType>,

    /// Key-space models to benchmark.
    ///
    /// `consecutive` inserts keys from `0..n`.
    /// `full_range` samples `n` distinct keys from the full type domain.
    /// `half_normal` spreads `n` unique keys across `0..u32::MAX` with
    /// a half-normal offset distribution controlled by `--key-eps`.
    #[arg(long, value_delimiter = ',', default_value = "consecutive")]
    key_spaces: Vec<KeySpace>,

    /// Seed used by the full-range sampler and half-normal quantile phase.
    #[arg(long, default_value_t = 2)]
    key_space_seed: u64,

    /// Comma-separated epsilon values used by `--key-spaces half-normal`.
    #[arg(long, value_name = "CSV")]
    key_eps: Option<String>,

    /// Insert order over the chosen keyset.
    #[arg(long, default_value_t = Order::Sequential)]
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

    /// Backward-compatible alias for `--key-types u64`.
    #[doc(hidden)]
    #[arg(long, hide = true, default_value_t = false)]
    u64_keys: bool,

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
    fn key_types(&self) -> Vec<KeyType> {
        let raw = if self.u64_keys {
            vec![KeyType::U64]
        } else {
            self.key_types.clone()
        };
        dedup(raw)
    }

    fn key_spaces(&self) -> Vec<KeySpace> {
        dedup(self.key_spaces.clone())
    }

    fn key_eps(&self) -> Vec<f64> {
        match &self.key_eps {
            Some(csv) => parse_f64_csv(csv, "--key-eps"),
            None => DEFAULT_KEY_EPS_VALUES.to_vec(),
        }
    }

    fn key_eps_for(&self, key_space: KeySpace) -> Vec<Option<f64>> {
        match key_space {
            KeySpace::HalfNormal => self.key_eps().into_iter().map(Some).collect(),
            _ => vec![None],
        }
    }

    fn num_elements(&self) -> Vec<u64> {
        match &self.num_elements {
            Some(csv) => parse_u64_csv(csv),
            None => DEFAULT_SPINE_LEVEL_SIZES.to_vec(),
        }
    }

    fn lookup_count_for(&self, num_elements: u64) -> u64 {
        self.lookup_count
            .map(|lookup_count| lookup_count.min(num_elements))
            .unwrap_or(num_elements.min(DEFAULT_LOOKUP_LIMIT))
    }

    fn false_positive_lookup_count_for(
        &self,
        config: BenchmarkConfig,
        num_elements: u64,
        _lookup_count: u64,
    ) -> u64 {
        self.false_positive_lookup_count
            .map(|count| {
                let max_false_positive_lookup_count =
                    config.max_false_positive_lookup_count(num_elements);
                count.min(max_false_positive_lookup_count)
            })
            .unwrap_or(0)
    }

    fn validate(&self, key_types: &[KeyType], key_spaces: &[KeySpace], num_elements_list: &[u64]) {
        let key_eps = self.key_eps();

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
            !key_types.is_empty(),
            "--key-types must select at least one key type"
        );
        assert!(
            !key_spaces.is_empty(),
            "--key-spaces must select at least one key-space mode"
        );
        if key_spaces.contains(&KeySpace::HalfNormal) {
            assert!(
                !key_eps.is_empty(),
                "--key-eps must select at least one epsilon for key-space half_normal"
            );
            for eps in &key_eps {
                assert!(
                    eps.is_finite() && *eps > 0.0,
                    "--key-eps values must be finite and greater than zero"
                );
            }
        }
        assert!(
            self.bloom_false_positive_rate > 0.0 && self.bloom_false_positive_rate < 1.0,
            "--bloom-false-positive-rate must be between 0 and 1"
        );

        for &num_elements in num_elements_list {
            assert!(
                num_elements > 0,
                "--num-elements values must be greater than zero"
            );

            for &key_type in key_types {
                for &key_space in key_spaces {
                    let config = BenchmarkConfig {
                        key_type,
                        key_space,
                        key_eps: None,
                    };
                    config.validate_num_elements(num_elements);
                }
            }
        }
    }
}

fn dedup<T>(values: Vec<T>) -> Vec<T>
where
    T: PartialEq,
{
    let mut out = Vec::with_capacity(values.len());
    for value in values {
        if !out.contains(&value) {
            out.push(value);
        }
    }
    out
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum Structure {
    #[value(name = "bloom")]
    Bloom,
    #[value(name = "roaring")]
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
enum KeyType {
    #[value(name = "u32")]
    U32,
    #[value(name = "u64")]
    U64,
}

impl Display for KeyType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::U32 => f.write_str("u32"),
            Self::U64 => f.write_str("u64"),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum KeySpace {
    #[value(name = "consecutive")]
    Consecutive,
    #[value(name = "full_range", alias = "full-range")]
    FullRange,
    #[value(name = "half_normal", alias = "half-normal")]
    HalfNormal,
}

impl Display for KeySpace {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Consecutive => f.write_str("consecutive"),
            Self::FullRange => f.write_str("full_range"),
            Self::HalfNormal => f.write_str("half_normal"),
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct BenchmarkConfig {
    key_type: KeyType,
    key_space: KeySpace,
    key_eps: Option<f64>,
}

impl BenchmarkConfig {
    fn validate_num_elements(self, num_elements: u64) {
        match (self.key_type, self.key_space) {
            (KeyType::U32, _) | (_, KeySpace::HalfNormal) => assert!(
                num_elements <= U32_KEY_SPACE_SIZE,
                "--num-elements values must be <= {} for this key type/key space",
                U32_KEY_SPACE_SIZE
            ),
            (KeyType::U64, _) => {}
        }
    }

    fn max_false_positive_lookup_count(self, num_elements: u64) -> u64 {
        match (self.key_type, self.key_space) {
            (_, KeySpace::HalfNormal) => U32_KEY_SPACE_SIZE - num_elements,
            (KeyType::U32, _) => U32_KEY_SPACE_SIZE - num_elements,
            (KeyType::U64, _) => u64::MAX - num_elements + 1,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum Order {
    #[value(name = "sequential")]
    Sequential,
    #[value(name = "random")]
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
        if len <= 1 {
            return Self::sequential(len);
        }
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
}

#[derive(Clone, Copy, Debug)]
struct WrappingPermutation64 {
    multiplier: u64,
    offset: u64,
}

impl WrappingPermutation64 {
    fn sequential() -> Self {
        Self {
            multiplier: 1,
            offset: 0,
        }
    }

    fn random(seed: u64) -> Self {
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        Self {
            multiplier: rng.next_u64() | 1,
            offset: rng.next_u64(),
        }
    }

    fn for_order(order: Order, seed: u64) -> Self {
        match order {
            Order::Sequential => Self::sequential(),
            Order::Random => Self::random(seed),
        }
    }

    fn index_at(&self, position: u64) -> u64 {
        position
            .wrapping_mul(self.multiplier)
            .wrapping_add(self.offset)
    }
}

#[derive(Clone, Copy, Debug)]
struct HalfNormalKeySampler {
    eps: f64,
    seed: u64,
}

impl HalfNormalKeySampler {
    fn new(eps: f64, seed: u64) -> Self {
        Self { eps, seed }
    }

    fn present_keys_u32(&self, num_elements: u64) -> Vec<u32> {
        let len = usize::try_from(num_elements).expect("num_elements must fit in usize");
        let mut rng = ChaCha8Rng::seed_from_u64(self.seed);
        let sigma = self.eps * u32::MAX as f64;
        let distribution = Normal::new(0.0, sigma)
            .expect("half-normal epsilon must produce a positive standard deviation");
        let mut keys = Vec::with_capacity(len);

        for _ in 0..num_elements {
            let sampled = distribution.sample(&mut rng).abs().round();
            keys.push(sampled.clamp(0.0, u32::MAX as f64) as u32);
        }

        keys.sort_unstable();
        project_sorted_unique_u32_domain(&mut keys);
        keys
    }

    fn present_keys_u64(&self, num_elements: u64) -> Vec<u64> {
        self.present_keys_u32(num_elements)
            .into_iter()
            .map(u64::from)
            .collect()
    }
}

#[derive(Clone, Copy, Debug)]
enum U32KeySampler {
    Consecutive,
    FullRange(AffinePermutation),
    HalfNormal(HalfNormalKeySampler),
}

impl U32KeySampler {
    fn new(key_space: KeySpace, _num_elements: u64, key_eps: Option<f64>, seed: u64) -> Self {
        match key_space {
            KeySpace::Consecutive => Self::Consecutive,
            KeySpace::FullRange => Self::FullRange(AffinePermutation::for_order(
                U32_KEY_SPACE_SIZE,
                Order::Random,
                seed,
            )),
            KeySpace::HalfNormal => Self::HalfNormal(HalfNormalKeySampler::new(
                key_eps.expect("half_normal key space requires key_eps"),
                seed,
            )),
        }
    }

    fn present_key(&self, set_index: u64) -> u32 {
        match self {
            Self::Consecutive => set_index as u32,
            Self::FullRange(permutation) => permutation.index_at(set_index) as u32,
            Self::HalfNormal(_) => {
                panic!("half_normal key space requires pre-generated keys")
            }
        }
    }

    fn absent_key(&self, num_elements: u64, absent_index: u64) -> u32 {
        let domain_index = num_elements
            .checked_add(absent_index)
            .expect("u32 absent-key generation overflowed");
        match self {
            Self::Consecutive => domain_index as u32,
            Self::FullRange(permutation) => permutation.index_at(domain_index) as u32,
            Self::HalfNormal(_) => {
                panic!("half_normal key space requires prepared absent keys")
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum U64KeySampler {
    Consecutive,
    FullRange(WrappingPermutation64),
    HalfNormal(HalfNormalKeySampler),
}

impl U64KeySampler {
    fn new(key_space: KeySpace, _num_elements: u64, key_eps: Option<f64>, seed: u64) -> Self {
        match key_space {
            KeySpace::Consecutive => Self::Consecutive,
            KeySpace::FullRange => {
                Self::FullRange(WrappingPermutation64::for_order(Order::Random, seed))
            }
            KeySpace::HalfNormal => Self::HalfNormal(HalfNormalKeySampler::new(
                key_eps.expect("half_normal key space requires key_eps"),
                seed,
            )),
        }
    }

    fn present_key(&self, set_index: u64) -> u64 {
        match self {
            Self::Consecutive => set_index,
            Self::FullRange(permutation) => permutation.index_at(set_index),
            Self::HalfNormal(_) => {
                panic!("half_normal key space requires pre-generated keys")
            }
        }
    }

    fn absent_key(&self, num_elements: u64, absent_index: u64) -> u64 {
        let domain_index = num_elements
            .checked_add(absent_index)
            .expect("u64 absent-key generation overflowed");
        match self {
            Self::Consecutive => domain_index,
            Self::FullRange(permutation) => permutation.index_at(domain_index),
            Self::HalfNormal(_) => {
                panic!("half_normal key space requires prepared absent keys")
            }
        }
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
            parse_u64_token(entry.trim())
                .unwrap_or_else(|error| panic!("invalid u64 in --num-elements: {entry} ({error})"))
        })
        .collect();
    out.sort_unstable();
    out.dedup();
    out
}

fn parse_f64_csv(csv: &str, flag_name: &str) -> Vec<f64> {
    let mut out: Vec<f64> = csv
        .split(',')
        .filter(|entry| !entry.trim().is_empty())
        .map(|entry| {
            entry
                .trim()
                .parse::<f64>()
                .unwrap_or_else(|error| panic!("invalid f64 in {flag_name}: {entry} ({error})"))
        })
        .collect();
    out.sort_by(|lhs, rhs| lhs.partial_cmp(rhs).expect("NaN was already rejected"));
    out.dedup();
    out
}

fn parse_u64_token(token: &str) -> Result<u64, String> {
    match token {
        "u32::MAX" | "u32_max" | "max_u32" => Ok(u32::MAX as u64),
        _ => token
            .replace('_', "")
            .parse::<u64>()
            .map_err(|error| error.to_string()),
    }
}

fn project_sorted_unique_u32_domain(keys: &mut [u32]) {
    if keys.is_empty() {
        return;
    }

    for (index, key) in keys.iter_mut().enumerate() {
        let min_key = u32::try_from(index).expect("key count exceeded u32 domain");
        if *key < min_key {
            *key = min_key;
        }
    }

    for index in (0..keys.len()).rev() {
        let tail = keys.len() - 1 - index;
        let max_key = u32::MAX
            .checked_sub(u32::try_from(tail).expect("key count exceeded u32 domain"))
            .expect("tail adjustment underflowed");
        if keys[index] > max_key {
            keys[index] = max_key;
        }
        if index + 1 < keys.len() && keys[index] >= keys[index + 1] {
            keys[index] = keys[index + 1] - 1;
        }
    }

    debug_assert!(keys.windows(2).all(|window| window[0] < window[1]));
}

fn absent_keys_from_sorted_present_u32(present_keys: &[u32], count: u64) -> Vec<u32> {
    let target_len = usize::try_from(count).expect("false-positive lookup count must fit in usize");
    let mut absent_keys = Vec::with_capacity(target_len);
    let mut candidate = 0u64;

    for &present_key in present_keys {
        let present_key = present_key as u64;
        while candidate < present_key && absent_keys.len() < target_len {
            absent_keys.push(candidate as u32);
            candidate += 1;
        }
        if absent_keys.len() == target_len {
            return absent_keys;
        }
        candidate = present_key
            .checked_add(1)
            .expect("half-normal absent-key generation overflowed");
    }

    while absent_keys.len() < target_len {
        absent_keys.push(candidate as u32);
        candidate = candidate
            .checked_add(1)
            .expect("half-normal absent-key generation overflowed");
    }

    absent_keys
}

fn absent_keys_from_sorted_present_u64(present_keys: &[u64], count: u64) -> Vec<u64> {
    let target_len = usize::try_from(count).expect("false-positive lookup count must fit in usize");
    let mut absent_keys = Vec::with_capacity(target_len);
    let mut candidate = 0u64;

    for &present_key in present_keys {
        while candidate < present_key && absent_keys.len() < target_len {
            absent_keys.push(candidate);
            candidate += 1;
        }
        if absent_keys.len() == target_len {
            return absent_keys;
        }
        candidate = present_key
            .checked_add(1)
            .expect("half-normal absent-key generation overflowed");
    }

    while absent_keys.len() < target_len {
        absent_keys.push(candidate);
        candidate = candidate
            .checked_add(1)
            .expect("half-normal absent-key generation overflowed");
    }

    absent_keys
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
    key_type: &'static str,
    key_space: &'static str,
    key_eps: Option<f64>,
    num_elements: u64,
    lookup_count: u64,
    false_positive_lookup_count: u64,
    repetitions: usize,
    insert_order: &'static str,
    lookup_order: &'static str,
    insert_seed: u64,
    lookup_seed: u64,
    key_space_seed: u64,
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

#[derive(Clone, Copy)]
struct CsvRowContext<'a> {
    structure: Structure,
    config: BenchmarkConfig,
    args: &'a Args,
    num_elements: u64,
    lookup_count: u64,
    false_positive_lookup_count: u64,
    bloom_expected_items: u64,
}

impl CsvRow {
    fn from_result(context: CsvRowContext<'_>, result: &BenchmarkResult) -> Self {
        let bits_per_element = (context.structure == Structure::Bloom)
            .then_some((result.bytes_used as f64 * 8.0) / context.num_elements as f64);
        let false_positive_stats = result.false_positive_rate_percent;

        Self {
            structure: context.structure.as_str(),
            key_type: context.config.key_type.as_str(),
            key_space: context.config.key_space.as_str(),
            key_eps: context.config.key_eps,
            num_elements: context.num_elements,
            lookup_count: context.lookup_count,
            false_positive_lookup_count: context.false_positive_lookup_count,
            repetitions: context.args.repetitions,
            insert_order: context.args.insert_order.as_str(),
            lookup_order: context.args.lookup_order.as_str(),
            insert_seed: context.args.insert_seed,
            lookup_seed: context.args.lookup_seed,
            key_space_seed: context.args.key_space_seed,
            bloom_false_positive_rate_target_percent: context.args.bloom_false_positive_rate
                * 100.0,
            bloom_seed: context.args.bloom_seed,
            bloom_expected_items: context.bloom_expected_items,
            bytes_used: result.bytes_used,
            bytes_per_element: result.bytes_used as f64 / context.num_elements as f64,
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

impl KeyType {
    fn as_str(self) -> &'static str {
        match self {
            Self::U32 => "u32",
            Self::U64 => "u64",
        }
    }
}

impl KeySpace {
    fn as_str(self) -> &'static str {
        match self {
            Self::Consecutive => "consecutive",
            Self::FullRange => "full_range",
            Self::HalfNormal => "half_normal",
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
    config: BenchmarkConfig,
    num_elements: u64,
    lookup_count: u64,
    false_positive_lookup_count: u64,
    bloom_expected_items: u64,
) -> BenchmarkResult {
    match config.key_type {
        KeyType::U32 => benchmark_bloom_u32(
            args,
            config,
            num_elements,
            lookup_count,
            false_positive_lookup_count,
            bloom_expected_items,
        ),
        KeyType::U64 => benchmark_bloom_u64(
            args,
            config,
            num_elements,
            lookup_count,
            false_positive_lookup_count,
            bloom_expected_items,
        ),
    }
}

fn benchmark_bloom_u32(
    args: &Args,
    config: BenchmarkConfig,
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
        let sampler = U32KeySampler::new(
            config.key_space,
            num_elements,
            config.key_eps,
            args.key_space_seed.wrapping_add(repetition as u64),
        );
        let present_keys = sorted_present_keys_u32(sampler, num_elements);
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
        let false_positive_permutation = (false_positive_lookup_count > 0).then(|| {
            AffinePermutation::for_order(
                false_positive_lookup_count,
                args.lookup_order,
                args.lookup_seed.wrapping_add(repetition as u64),
            )
        });
        let absent_keys = matches!(sampler, U32KeySampler::HalfNormal(_)).then(|| {
            absent_keys_from_sorted_present_u32(&present_keys, false_positive_lookup_count)
        });

        let mut bloom = BloomFilter::with_false_pos(args.bloom_false_positive_rate)
            .seed(&args.bloom_seed)
            .expected_items(expected_items.max(MIN_BLOOM_EXPECTED_ITEMS as usize));

        let insert_started = Instant::now();
        for index in 0..num_elements {
            let key = present_keys[insert_permutation.index_at(index) as usize];
            bloom.insert(&key);
        }
        let insert_elapsed = insert_started.elapsed();

        let lookup_started = Instant::now();
        let mut hits = 0u64;
        for index in 0..lookup_count {
            let key = present_keys[lookup_permutation.index_at(index) as usize];
            hits += u64::from(bloom.contains(&key));
        }
        let lookup_elapsed = lookup_started.elapsed();

        assert_eq!(hits, lookup_count, "expected all lookup keys to be present");

        if let Some(false_positive_permutation) = false_positive_permutation {
            let mut false_positives = 0u64;
            for index in 0..false_positive_lookup_count {
                let absent_index = false_positive_permutation.index_at(index);
                let key = absent_keys.as_ref().map_or_else(
                    || sampler.absent_key(num_elements, absent_index),
                    |keys| keys[absent_index as usize],
                );
                false_positives += u64::from(bloom.contains(&key));
            }
            false_positive_rate_percent_samples
                .push((false_positives as f64 / false_positive_lookup_count as f64) * 100.0);
        }

        bytes_used = size_of_val(bloom.as_slice());
        insert_samples.push(insert_elapsed.as_nanos() as f64 / num_elements as f64);
        lookup_samples.push(lookup_elapsed.as_nanos() as f64 / lookup_count as f64);
    }

    BenchmarkResult {
        insert_ns_per_element: SummaryStats::from_samples(&insert_samples),
        lookup_ns_per_element: SummaryStats::from_samples(&lookup_samples),
        bytes_used,
        false_positive_rate_percent: (!false_positive_rate_percent_samples.is_empty())
            .then(|| SummaryStats::from_samples(&false_positive_rate_percent_samples)),
    }
}

fn benchmark_bloom_u64(
    args: &Args,
    config: BenchmarkConfig,
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
        let sampler = U64KeySampler::new(
            config.key_space,
            num_elements,
            config.key_eps,
            args.key_space_seed.wrapping_add(repetition as u64),
        );
        let present_keys = sorted_present_keys_u64(sampler, num_elements);
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
        let false_positive_permutation = (false_positive_lookup_count > 0).then(|| {
            AffinePermutation::for_order(
                false_positive_lookup_count,
                args.lookup_order,
                args.lookup_seed.wrapping_add(repetition as u64),
            )
        });
        let absent_keys = matches!(sampler, U64KeySampler::HalfNormal(_)).then(|| {
            absent_keys_from_sorted_present_u64(&present_keys, false_positive_lookup_count)
        });

        let mut bloom = BloomFilter::with_false_pos(args.bloom_false_positive_rate)
            .seed(&args.bloom_seed)
            .expected_items(expected_items.max(MIN_BLOOM_EXPECTED_ITEMS as usize));

        let insert_started = Instant::now();
        for index in 0..num_elements {
            let key = present_keys[insert_permutation.index_at(index) as usize];
            bloom.insert(&key);
        }
        let insert_elapsed = insert_started.elapsed();

        let lookup_started = Instant::now();
        let mut hits = 0u64;
        for index in 0..lookup_count {
            let key = present_keys[lookup_permutation.index_at(index) as usize];
            hits += u64::from(bloom.contains(&key));
        }
        let lookup_elapsed = lookup_started.elapsed();

        assert_eq!(hits, lookup_count, "expected all lookup keys to be present");

        if let Some(false_positive_permutation) = false_positive_permutation {
            let mut false_positives = 0u64;
            for index in 0..false_positive_lookup_count {
                let absent_index = false_positive_permutation.index_at(index);
                let key = absent_keys.as_ref().map_or_else(
                    || sampler.absent_key(num_elements, absent_index),
                    |keys| keys[absent_index as usize],
                );
                false_positives += u64::from(bloom.contains(&key));
            }
            false_positive_rate_percent_samples
                .push((false_positives as f64 / false_positive_lookup_count as f64) * 100.0);
        }

        bytes_used = size_of_val(bloom.as_slice());
        insert_samples.push(insert_elapsed.as_nanos() as f64 / num_elements as f64);
        lookup_samples.push(lookup_elapsed.as_nanos() as f64 / lookup_count as f64);
    }

    BenchmarkResult {
        insert_ns_per_element: SummaryStats::from_samples(&insert_samples),
        lookup_ns_per_element: SummaryStats::from_samples(&lookup_samples),
        bytes_used,
        false_positive_rate_percent: (!false_positive_rate_percent_samples.is_empty())
            .then(|| SummaryStats::from_samples(&false_positive_rate_percent_samples)),
    }
}

fn benchmark_roaring(
    args: &Args,
    config: BenchmarkConfig,
    num_elements: u64,
    lookup_count: u64,
) -> BenchmarkResult {
    match config.key_type {
        KeyType::U32 => benchmark_roaring_u32(args, config, num_elements, lookup_count),
        KeyType::U64 => benchmark_roaring_u64(args, config, num_elements, lookup_count),
    }
}

fn benchmark_roaring_u32(
    args: &Args,
    config: BenchmarkConfig,
    num_elements: u64,
    lookup_count: u64,
) -> BenchmarkResult {
    let mut insert_samples = Vec::with_capacity(args.repetitions);
    let mut lookup_samples = Vec::with_capacity(args.repetitions);
    let mut bytes_used = 0;

    for repetition in 0..args.repetitions {
        let sampler = U32KeySampler::new(
            config.key_space,
            num_elements,
            config.key_eps,
            args.key_space_seed.wrapping_add(repetition as u64),
        );
        let present_keys = sorted_present_keys_u32(sampler, num_elements);
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

        let insert_started = Instant::now();
        let mut bitmap = RoaringBitmap::new();
        for index in 0..num_elements {
            bitmap.insert(present_keys[insert_permutation.index_at(index) as usize]);
        }
        let insert_elapsed = insert_started.elapsed();
        let _ = bitmap.optimize();

        let lookup_started = Instant::now();
        let mut hits = 0u64;
        for index in 0..lookup_count {
            let key = present_keys[lookup_permutation.index_at(index) as usize];
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

fn benchmark_roaring_u64(
    args: &Args,
    config: BenchmarkConfig,
    num_elements: u64,
    lookup_count: u64,
) -> BenchmarkResult {
    let mut insert_samples = Vec::with_capacity(args.repetitions);
    let mut lookup_samples = Vec::with_capacity(args.repetitions);
    let mut bytes_used = 0;

    for repetition in 0..args.repetitions {
        let sampler = U64KeySampler::new(
            config.key_space,
            num_elements,
            config.key_eps,
            args.key_space_seed.wrapping_add(repetition as u64),
        );
        let present_keys = sorted_present_keys_u64(sampler, num_elements);
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

        let insert_started = Instant::now();
        let mut treemap = RoaringTreemap::new();
        for index in 0..num_elements {
            treemap.insert(present_keys[insert_permutation.index_at(index) as usize]);
        }
        let insert_elapsed = insert_started.elapsed();

        let lookup_started = Instant::now();
        let mut hits = 0u64;
        for index in 0..lookup_count {
            let key = present_keys[lookup_permutation.index_at(index) as usize];
            hits += u64::from(treemap.contains(key));
        }
        let lookup_elapsed = lookup_started.elapsed();

        assert_eq!(hits, lookup_count, "expected all lookup keys to be present");
        bytes_used = treemap.serialized_size();
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

fn sorted_present_keys_u32(sampler: U32KeySampler, num_elements: u64) -> Vec<u32> {
    match sampler {
        U32KeySampler::HalfNormal(sampler) => sampler.present_keys_u32(num_elements),
        _ => {
            let mut present_keys = Vec::with_capacity(
                usize::try_from(num_elements).expect("num_elements must fit in usize"),
            );
            for set_index in 0..num_elements {
                present_keys.push(sampler.present_key(set_index));
            }
            present_keys.sort_unstable();
            present_keys
        }
    }
}

fn sorted_present_keys_u64(sampler: U64KeySampler, num_elements: u64) -> Vec<u64> {
    match sampler {
        U64KeySampler::HalfNormal(sampler) => sampler.present_keys_u64(num_elements),
        _ => {
            let mut present_keys = Vec::with_capacity(
                usize::try_from(num_elements).expect("num_elements must fit in usize"),
            );
            for set_index in 0..num_elements {
                present_keys.push(sampler.present_key(set_index));
            }
            present_keys.sort_unstable();
            present_keys
        }
    }
}

fn print_report(
    structure: Structure,
    config: BenchmarkConfig,
    result: &BenchmarkResult,
    num_elements: u64,
    lookup_count: u64,
    false_positive_lookup_count: u64,
) {
    println!("structure={structure}");
    println!("key_type={}", config.key_type.as_str());
    println!("key_space={}", config.key_space.as_str());
    if let Some(key_eps) = config.key_eps {
        println!("key_eps={key_eps}");
    }
    println!("num_elements={num_elements}");
    println!("bytes_used={}", result.bytes_used);
    println!(
        "bytes_used_human={}",
        format_bytes(result.bytes_used as f64)
    );
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
