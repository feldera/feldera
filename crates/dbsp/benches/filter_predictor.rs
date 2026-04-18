//! Predictor benchmark for deciding between `fastbloom` and `roaring` on u32 keys
//! using per-batch `key_count/min/max/touched_window_count` metadata.
//!
//! Examples:
//! `cargo bench -p dbsp --bench filter_predictor -- --csv-output filter_predictor.csv`
//! `cargo bench -p dbsp --bench filter_predictor -- --num-keys 99_999,999_999 --distributions gaussian,bimodal,exponential --gaussian-means 0.1,0.5,0.9 --gaussian-stddevs 1e-6,1e-4,1e-2`
//! `cargo bench -p dbsp --bench filter_predictor -- --num-keys 499_999,1_999_999 --distributions holey_wide --batch-counts 8 --batch-layouts overlap`

use clap::{Parser, ValueEnum};
use csv::Writer;
use dbsp::storage::file::BLOOM_FILTER_FALSE_POSITIVE_RATE;
use fastbloom::BloomFilter;
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
use rand_distr::{Distribution, Exp, Normal};
use roaring::RoaringBitmap;
use serde::Serialize;
use std::{
    fmt::{Display, Formatter},
    fs::File,
    mem::size_of_val,
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::Instant,
};

const DEFAULT_BLOOM_SEED: u128 = 42;
const DEFAULT_GAUSSIAN_MEAN_FRACTIONS: [f64; 1] = [0.5];
const DEFAULT_GAUSSIAN_STDDEV_FRACTIONS: [f64; 5] = [1e-6, 1e-4, 1e-2, 1e-1, 5e-1];
const DEFAULT_LOOKUP_LIMIT: u64 = 5_000_000;
const DEFAULT_BATCH_COUNTS: [usize; 3] = [1, 8, 64];
const OVERLAP_FACTOR_DAMPING: f64 = 0.25;
const BIMODAL_LEFT_PEAK_FRAC: f64 = 0.25;
const BIMODAL_RIGHT_PEAK_FRAC: f64 = 0.75;
const HOLEY_WIDE_DOMAIN_MAX_EXCLUSIVE: u64 = 2_000_000_000;
const HOLEY_WIDE_TARGET_KEYS_PER_WINDOW: u64 = 5_000;
const HOLEY_WIDE_MIN_TOUCHED_WINDOWS: u64 = 192;
const MIN_BLOOM_EXPECTED_ITEMS: u64 = 64;
const U32_KEY_SPACE_SIZE: u64 = u32::MAX as u64 + 1;
const DEFAULT_NUM_KEYS: [u64; 5] = [14_999, 999_999, 9_999_999, 99_999_999, 999_999_999];

// Build and memory mostly care about how much work or storage Roaring pays per
// touched 16-bit container, so these predictors stay intentionally simple and
// depend primarily on estimated keys per touched container.
const BUILD_ROARING_ESTIMATED_KEYS_PER_CONTAINER_THRESHOLD: f64 = 4.0;
const MEMORY_ROARING_ESTIMATED_KEYS_PER_CONTAINER_THRESHOLD: f64 = 32.0;

// roaring-rs switches array containers to bitmap containers around 4096 keys.
// That transition materially changes lookup behavior, so the lookup predictor
// treats it as a first-class boundary.
const ROARING_BITMAP_CONTAINER_THRESHOLD: f64 = 4_096.0;

// Lookup prediction is framed as a coarse cost proxy. If the estimated cost of
// reaching and searching a Roaring container stays below this budget, predict
// Roaring; otherwise predict Bloom.
const LOOKUP_ROARING_CONTAINER_PROBABILITY_THRESHOLD: f64 = 0.1;
const LOOKUP_ROARING_BITMAP_CONTAINER_PROBABILITY_PENALTY: f64 = 0.1;
const LOOKUP_ROARING_ARRAY_CONTAINER_PROBABILITY_PENALTY_BASE: f64 = 0.20;
const LOOKUP_ROARING_ARRAY_CONTAINER_PROBABILITY_PENALTY_PER_LOG2_KEY: f64 = 0.10;

const U32_CONTAINER_COUNT: usize = 1 << 16;
const WINDOW_BITSET_WORDS: usize = U32_CONTAINER_COUNT / 64;

fn main() {
    let args = Args::parse();
    let distributions = args.distributions();
    let num_keys_list = args.num_keys();
    let gaussian_means = args.gaussian_means();
    let gaussian_stddevs = args.gaussian_stddevs();
    args.validate(
        &distributions,
        &num_keys_list,
        &gaussian_means,
        &gaussian_stddevs,
    );
    let run_configs = build_run_configs(
        &args,
        &distributions,
        &num_keys_list,
        &gaussian_means,
        &gaussian_stddevs,
    );
    let worker_threads = args.worker_threads(run_configs.len());

    let batch_counts = args.batch_counts();
    let batch_layouts = args.batch_layouts();

    println!("benchmark=filter_predictor");
    println!(
        "distributions={}",
        distributions
            .iter()
            .map(|distribution| distribution.as_str())
            .collect::<Vec<_>>()
            .join(",")
    );
    println!(
        "num_keys={}",
        num_keys_list
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(",")
    );
    println!(
        "gaussian_means={}",
        gaussian_means
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(",")
    );
    println!(
        "gaussian_stddevs={}",
        gaussian_stddevs
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(",")
    );
    println!(
        "batch_counts={}",
        batch_counts
            .iter()
            .map(usize::to_string)
            .collect::<Vec<_>>()
            .join(",")
    );
    println!(
        "batch_layouts={}",
        batch_layouts
            .iter()
            .map(|layout| layout.as_str())
            .collect::<Vec<_>>()
            .join(",")
    );
    println!("repetitions={}", args.repetitions);
    println!("distribution_seed={}", args.distribution_seed);
    println!("split_seed={}", args.split_seed);
    println!("lookup_seed={}", args.lookup_seed);
    println!("threads={}", worker_threads);
    println!("lookup_space={}", args.lookup_space.as_str());
    println!("lookup_count_override={}", option_u64(args.lookup_count));
    println!(
        "bloom_false_positive_rate={}",
        args.bloom_false_positive_rate
    );
    println!("bloom_seed={}", args.bloom_seed);
    println!(
        "bloom_expected_items_override={}",
        option_u64(args.bloom_expected_items)
    );
    println!("csv_output={}", args.csv_output.display());
    println!();

    let rows = execute_runs(&args, &run_configs, worker_threads);

    let csv_file = File::create(&args.csv_output)
        .unwrap_or_else(|error| panic!("failed to create {}: {error}", args.csv_output.display()));
    let mut csv_writer = Writer::from_writer(csv_file);
    for row in &rows {
        print_run_report(row);
        csv_writer
            .serialize(row)
            .expect("failed to write filter predictor CSV row");
    }
    csv_writer
        .flush()
        .expect("failed to flush filter predictor CSV");

    let accuracy = summarize_accuracy(&rows);
    print_summary(&rows, &accuracy);
}

#[derive(Parser, Debug, Clone)]
#[command(name = "filter_predictor")]
#[command(about = "Benchmark a merge-time roaring-vs-bloom predictor from batch metadata")]
struct Args {
    /// Comma-separated key counts. Underscores and `u32::MAX` are accepted.
    #[arg(long, value_name = "CSV")]
    num_keys: Option<String>,

    /// Comma-separated distribution families to run.
    /// Supported values: `gaussian`, `consecutive`, `round_robin_container`,
    /// `bimodal`, `exponential`, `holey_wide`.
    #[arg(long, value_name = "CSV")]
    distributions: Option<String>,

    /// Gaussian mean values expressed as fractions of `u32::MAX`.
    /// Only used by the `gaussian` distribution family.
    #[arg(long, value_name = "CSV")]
    gaussian_means: Option<String>,

    /// Spread parameters expressed as fractions of `u32::MAX`.
    /// Used as:
    /// - gaussian standard deviation for `gaussian`
    /// - per-peak standard deviation for `bimodal`
    /// - exponential scale for `exponential`
    #[arg(long, value_name = "CSV")]
    gaussian_stddevs: Option<String>,

    /// Comma-separated batch counts to simulate during merge.
    /// Defaults to `1,8,64`.
    #[arg(long, value_name = "CSV")]
    batch_counts: Option<String>,

    /// Comma-separated batch layouts to simulate.
    /// Supported values: `disjoint`, `overlap`.
    #[arg(long, value_name = "CSV")]
    batch_layouts: Option<String>,

    /// Number of repeated runs per `(num_keys, mean, stddev)` configuration.
    #[arg(long, default_value_t = 2)]
    repetitions: usize,

    /// Number of benchmark configurations to run concurrently.
    /// `1` keeps runs sequential.
    #[arg(long, default_value_t = 1)]
    threads: usize,

    /// Lookup workload.
    /// `present` samples only keys from the batch.
    /// `full_u32` samples random u32 keys from the full domain.
    #[arg(long, value_enum, default_value_t = LookupSpace::FullU32)]
    lookup_space: LookupSpace,

    /// Number of lookups to benchmark per run.
    /// Defaults to `min(num_keys, 5_000_000)` for `present` and `5_000_000`
    /// for `full_u32`.
    #[arg(long)]
    lookup_count: Option<u64>,

    /// Seed for gaussian key generation.
    #[arg(long, default_value_t = 0)]
    distribution_seed: u64,

    /// Seed for overlapping batch splits.
    #[arg(long, default_value_t = 1)]
    split_seed: u64,

    /// Seed for randomized successful lookups.
    #[arg(long, default_value_t = 2)]
    lookup_seed: u64,

    /// Bloom filter false-positive rate.
    #[arg(long, default_value_t = BLOOM_FILTER_FALSE_POSITIVE_RATE)]
    bloom_false_positive_rate: f64,

    /// Bloom filter seed.
    #[arg(long, default_value_t = DEFAULT_BLOOM_SEED)]
    bloom_seed: u128,

    /// Expected items passed to the bloom filter builder.
    #[arg(long)]
    bloom_expected_items: Option<u64>,

    /// Output CSV path.
    #[arg(long, default_value = "filter_predictor.csv")]
    csv_output: PathBuf,

    #[doc(hidden)]
    #[arg(long = "bench", hide = true)]
    __bench: bool,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, ValueEnum)]
enum LookupSpace {
    Present,
    FullU32,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, ValueEnum)]
enum DistributionKind {
    Gaussian,
    Consecutive,
    RoundRobinContainer,
    Bimodal,
    Exponential,
    HoleyWide,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum BatchLayout {
    Disjoint,
    Overlap,
}

impl BatchLayout {
    fn as_str(self) -> &'static str {
        match self {
            Self::Disjoint => "disjoint",
            Self::Overlap => "overlap",
        }
    }
}

impl DistributionKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Gaussian => "gaussian",
            Self::Consecutive => "consecutive",
            Self::RoundRobinContainer => "round_robin_container",
            Self::Bimodal => "bimodal",
            Self::Exponential => "exponential",
            Self::HoleyWide => "holey_wide",
        }
    }

    fn uses_gaussian_mean(self) -> bool {
        matches!(self, Self::Gaussian)
    }

    fn uses_spread_param(self) -> bool {
        matches!(self, Self::Gaussian | Self::Bimodal | Self::Exponential)
    }
}

const DEFAULT_DISTRIBUTIONS: [DistributionKind; 5] = [
    DistributionKind::Gaussian,
    DistributionKind::Consecutive,
    DistributionKind::RoundRobinContainer,
    DistributionKind::Bimodal,
    DistributionKind::Exponential,
];

impl LookupSpace {
    fn as_str(self) -> &'static str {
        match self {
            Self::Present => "present",
            Self::FullU32 => "full_u32",
        }
    }
}

impl Args {
    fn distributions(&self) -> Vec<DistributionKind> {
        match &self.distributions {
            Some(csv) => parse_distribution_csv(csv),
            None => DEFAULT_DISTRIBUTIONS.to_vec(),
        }
    }

    fn num_keys(&self) -> Vec<u64> {
        match &self.num_keys {
            Some(csv) => parse_u64_csv(csv),
            None => DEFAULT_NUM_KEYS.to_vec(),
        }
    }

    fn gaussian_means(&self) -> Vec<f64> {
        match &self.gaussian_means {
            Some(csv) => parse_f64_csv(csv, "--gaussian-means"),
            None => DEFAULT_GAUSSIAN_MEAN_FRACTIONS.to_vec(),
        }
    }

    fn gaussian_stddevs(&self) -> Vec<f64> {
        match &self.gaussian_stddevs {
            Some(csv) => parse_f64_csv(csv, "--gaussian-stddevs"),
            None => DEFAULT_GAUSSIAN_STDDEV_FRACTIONS.to_vec(),
        }
    }

    fn batch_counts(&self) -> Vec<usize> {
        match &self.batch_counts {
            Some(csv) => parse_usize_csv(csv),
            None => DEFAULT_BATCH_COUNTS.to_vec(),
        }
    }

    fn batch_layouts(&self) -> Vec<BatchLayout> {
        match &self.batch_layouts {
            Some(csv) => parse_batch_layout_csv(csv),
            None => vec![BatchLayout::Disjoint, BatchLayout::Overlap],
        }
    }

    fn lookup_count_for(&self, num_keys: u64) -> u64 {
        self.lookup_count
            .map(|lookup_count| match self.lookup_space {
                LookupSpace::Present => lookup_count.min(num_keys),
                LookupSpace::FullU32 => lookup_count,
            })
            .unwrap_or(match self.lookup_space {
                LookupSpace::Present => num_keys.min(DEFAULT_LOOKUP_LIMIT),
                LookupSpace::FullU32 => DEFAULT_LOOKUP_LIMIT,
            })
    }

    fn worker_threads(&self, run_count: usize) -> usize {
        self.threads.max(1).min(run_count.max(1))
    }

    fn validate(
        &self,
        distributions: &[DistributionKind],
        num_keys_list: &[u64],
        gaussian_means: &[f64],
        gaussian_stddevs: &[f64],
    ) {
        let batch_counts = self.batch_counts();
        let batch_layouts = self.batch_layouts();
        assert!(
            !distributions.is_empty(),
            "--distributions must select at least one family"
        );
        assert!(
            !num_keys_list.is_empty(),
            "--num-keys must select at least one size"
        );
        if distributions
            .iter()
            .copied()
            .any(DistributionKind::uses_gaussian_mean)
        {
            assert!(
                !gaussian_means.is_empty(),
                "--gaussian-means must select at least one value when gaussian is enabled"
            );
        }
        if distributions
            .iter()
            .copied()
            .any(DistributionKind::uses_spread_param)
        {
            assert!(
                !gaussian_stddevs.is_empty(),
                "--gaussian-stddevs must select at least one value when gaussian, bimodal, or exponential is enabled"
            );
        }
        assert!(
            self.repetitions > 0,
            "--repetitions must be greater than zero"
        );
        assert!(self.threads > 0, "--threads must be greater than zero");
        assert!(
            self.bloom_false_positive_rate > 0.0 && self.bloom_false_positive_rate < 1.0,
            "--bloom-false-positive-rate must be between 0 and 1"
        );

        for &num_keys in num_keys_list {
            assert!(num_keys > 0, "--num-keys values must be greater than zero");
            assert!(
                num_keys <= U32_KEY_SPACE_SIZE,
                "--num-keys values must be <= {}",
                U32_KEY_SPACE_SIZE
            );
        }
        for &gaussian_mean in gaussian_means {
            assert!(
                gaussian_mean.is_finite() && (0.0..=1.0).contains(&gaussian_mean),
                "--gaussian-means values must be finite fractions in [0, 1]"
            );
        }
        for &gaussian_stddev in gaussian_stddevs {
            assert!(
                gaussian_stddev.is_finite() && gaussian_stddev > 0.0,
                "--gaussian-stddevs values must be finite and greater than zero"
            );
        }
        assert!(
            !batch_counts.is_empty(),
            "--batch-counts must select at least one value"
        );
        assert!(
            !batch_layouts.is_empty(),
            "--batch-layouts must select at least one value"
        );
        for &batch_count in &batch_counts {
            assert!(
                (1..=64).contains(&batch_count),
                "--batch-counts values must be in [1, 64]"
            );
        }
        if let Some(lookup_count) = self.lookup_count {
            assert!(lookup_count > 0, "--lookup-count must be greater than zero");
        }
        if let Some(bloom_expected_items) = self.bloom_expected_items {
            assert!(
                bloom_expected_items > 0,
                "--bloom-expected-items must be greater than zero"
            );
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct GaussianDistribution {
    mean_frac: f64,
    stddev_frac: f64,
}

impl GaussianDistribution {
    fn mean_value(self) -> f64 {
        self.mean_frac * u32::MAX as f64
    }

    fn stddev_value(self) -> f64 {
        self.stddev_frac * u32::MAX as f64
    }
}

#[derive(Debug, Clone, Copy)]
enum DistributionSpec {
    Gaussian(GaussianDistribution),
    Consecutive,
    RoundRobinContainer,
    Bimodal { stddev_frac: f64 },
    Exponential { scale_frac: f64 },
    HoleyWide,
}

impl DistributionSpec {
    fn as_str(self) -> &'static str {
        match self {
            Self::Gaussian(_) => "gaussian",
            Self::Consecutive => "consecutive",
            Self::RoundRobinContainer => "round_robin_container",
            Self::Bimodal { .. } => "bimodal",
            Self::Exponential { .. } => "exponential",
            Self::HoleyWide => "holey_wide",
        }
    }

    fn parameter_name(self) -> &'static str {
        match self {
            Self::Gaussian(_) => "stddev_frac",
            Self::Bimodal { .. } => "stddev_frac",
            Self::Exponential { .. } => "scale_frac",
            Self::Consecutive | Self::RoundRobinContainer | Self::HoleyWide => "none",
        }
    }

    fn parameter_frac(self) -> Option<f64> {
        match self {
            Self::Gaussian(distribution) => Some(distribution.stddev_frac),
            Self::Bimodal { stddev_frac } => Some(stddev_frac),
            Self::Exponential { scale_frac } => Some(scale_frac),
            Self::Consecutive | Self::RoundRobinContainer | Self::HoleyWide => None,
        }
    }

    fn parameter_value(self) -> Option<f64> {
        match self {
            Self::Gaussian(distribution) => Some(distribution.stddev_value()),
            Self::Bimodal { stddev_frac } => Some(stddev_frac * u32::MAX as f64),
            Self::Exponential { scale_frac } => Some(scale_frac * u32::MAX as f64),
            Self::Consecutive | Self::RoundRobinContainer | Self::HoleyWide => None,
        }
    }

    fn gaussian_mean_frac(self) -> Option<f64> {
        match self {
            Self::Gaussian(distribution) => Some(distribution.mean_frac),
            Self::Consecutive
            | Self::RoundRobinContainer
            | Self::Bimodal { .. }
            | Self::Exponential { .. }
            | Self::HoleyWide => None,
        }
    }

    fn gaussian_mean_value(self) -> Option<f64> {
        match self {
            Self::Gaussian(distribution) => Some(distribution.mean_value()),
            Self::Consecutive
            | Self::RoundRobinContainer
            | Self::Bimodal { .. }
            | Self::Exponential { .. }
            | Self::HoleyWide => None,
        }
    }

    fn gaussian_stddev_frac(self) -> Option<f64> {
        match self {
            Self::Gaussian(distribution) => Some(distribution.stddev_frac),
            Self::Consecutive
            | Self::RoundRobinContainer
            | Self::Bimodal { .. }
            | Self::Exponential { .. }
            | Self::HoleyWide => None,
        }
    }

    fn gaussian_stddev_value(self) -> Option<f64> {
        match self {
            Self::Gaussian(distribution) => Some(distribution.stddev_value()),
            Self::Consecutive
            | Self::RoundRobinContainer
            | Self::Bimodal { .. }
            | Self::Exponential { .. }
            | Self::HoleyWide => None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct RunConfig {
    run_index: usize,
    num_keys: u64,
    distribution: DistributionSpec,
    repetition: usize,
    distribution_seed: u64,
    split_seed: u64,
    lookup_seed: u64,
}

fn build_run_configs(
    args: &Args,
    distributions: &[DistributionKind],
    num_keys_list: &[u64],
    gaussian_means: &[f64],
    gaussian_stddevs: &[f64],
) -> Vec<RunConfig> {
    let mut run_configs = Vec::new();

    for &num_keys in num_keys_list {
        for &distribution_kind in distributions {
            match distribution_kind {
                DistributionKind::Gaussian => {
                    for &gaussian_mean_frac in gaussian_means {
                        for &gaussian_stddev_frac in gaussian_stddevs {
                            let distribution = DistributionSpec::Gaussian(GaussianDistribution {
                                mean_frac: gaussian_mean_frac,
                                stddev_frac: gaussian_stddev_frac,
                            });
                            push_run_configs(&mut run_configs, args, num_keys, distribution);
                        }
                    }
                }
                DistributionKind::Consecutive => {
                    push_run_configs(
                        &mut run_configs,
                        args,
                        num_keys,
                        DistributionSpec::Consecutive,
                    );
                }
                DistributionKind::RoundRobinContainer => {
                    push_run_configs(
                        &mut run_configs,
                        args,
                        num_keys,
                        DistributionSpec::RoundRobinContainer,
                    );
                }
                DistributionKind::Bimodal => {
                    for &stddev_frac in gaussian_stddevs {
                        push_run_configs(
                            &mut run_configs,
                            args,
                            num_keys,
                            DistributionSpec::Bimodal { stddev_frac },
                        );
                    }
                }
                DistributionKind::Exponential => {
                    for &scale_frac in gaussian_stddevs {
                        push_run_configs(
                            &mut run_configs,
                            args,
                            num_keys,
                            DistributionSpec::Exponential { scale_frac },
                        );
                    }
                }
                DistributionKind::HoleyWide => {
                    push_run_configs(
                        &mut run_configs,
                        args,
                        num_keys,
                        DistributionSpec::HoleyWide,
                    );
                }
            }
        }
    }

    run_configs
}

fn push_run_configs(
    run_configs: &mut Vec<RunConfig>,
    args: &Args,
    num_keys: u64,
    distribution: DistributionSpec,
) {
    for repetition in 0..args.repetitions {
        run_configs.push(RunConfig {
            run_index: run_configs.len(),
            num_keys,
            distribution,
            repetition,
            distribution_seed: args.distribution_seed.wrapping_add(repetition as u64),
            split_seed: args.split_seed.wrapping_add(repetition as u64),
            lookup_seed: args.lookup_seed.wrapping_add(repetition as u64),
        });
    }
}

fn execute_runs(args: &Args, run_configs: &[RunConfig], worker_threads: usize) -> Vec<CsvRow> {
    if worker_threads <= 1 {
        return run_configs
            .iter()
            .copied()
            .flat_map(|run_config| run_single_config(args, run_config))
            .collect();
    }

    let next_index = AtomicUsize::new(0);
    let (tx, rx) = mpsc::channel::<(usize, Vec<CsvRow>)>();

    thread::scope(|scope| {
        for _ in 0..worker_threads {
            let tx = tx.clone();
            let next_index = &next_index;
            scope.spawn(move || {
                loop {
                    let task_index = next_index.fetch_add(1, Ordering::Relaxed);
                    if task_index >= run_configs.len() {
                        break;
                    }

                    let run_config = run_configs[task_index];
                    let rows = run_single_config(args, run_config);
                    tx.send((run_config.run_index, rows))
                        .expect("result receiver dropped unexpectedly");
                }
            });
        }

        drop(tx);

        let mut rows_by_index: Vec<Option<Vec<CsvRow>>> = std::iter::repeat_with(|| None)
            .take(run_configs.len())
            .collect();
        for (run_index, rows) in rx {
            rows_by_index[run_index] = Some(rows);
        }

        rows_by_index
            .into_iter()
            .flat_map(|rows| rows.expect("missing benchmark row"))
            .collect()
    })
}

fn run_single_config(args: &Args, run_config: RunConfig) -> Vec<CsvRow> {
    let generated_keys = generate_keys(
        run_config.num_keys,
        run_config.distribution,
        run_config.distribution_seed,
    );
    let batch = GeneratedBatch::new(generated_keys, run_config.split_seed);
    let actual_window_stats = actual_window_stats(batch.keys());
    let batch_counts = args.batch_counts();
    let batch_layouts = args.batch_layouts();
    let lookup_count = args.lookup_count_for(run_config.num_keys);
    let bloom_expected_items = args
        .bloom_expected_items
        .unwrap_or(run_config.num_keys)
        .max(MIN_BLOOM_EXPECTED_ITEMS);

    let bloom = benchmark_bloom(
        batch.keys(),
        lookup_count,
        run_config.lookup_seed,
        args.lookup_space,
        bloom_expected_items,
        args.bloom_false_positive_rate,
        args.bloom_seed,
    );
    let roaring = benchmark_roaring(
        batch.keys(),
        lookup_count,
        run_config.lookup_seed,
        args.lookup_space,
    );

    let build_actual = actual_winner(bloom.build_ns_per_element, roaring.build_ns_per_element);
    let lookup_actual = actual_winner(bloom.lookup_ns_per_element, roaring.lookup_ns_per_element);
    let memory_actual = actual_winner(bloom.bytes_used as f64, roaring.bytes_used as f64);
    let mut rows = Vec::with_capacity(batch_counts.len() * batch_layouts.len());
    for &batch_count in &batch_counts {
        for &batch_layout in &batch_layouts {
            let batch_summaries = split_batch_summaries(
                batch.keys(),
                batch_count,
                batch_layout,
                run_config.split_seed,
            );
            let predictor_stats = estimate_roaring_bounds_stats(&batch_summaries);
            let prediction = predict_filter_winner(&predictor_stats);

            rows.push(CsvRow {
                num_keys: run_config.num_keys,
                distribution: run_config.distribution.as_str(),
                distribution_param_name: run_config.distribution.parameter_name(),
                distribution_param_frac: run_config.distribution.parameter_frac(),
                distribution_param_value: run_config.distribution.parameter_value(),
                gaussian_mean_frac: run_config.distribution.gaussian_mean_frac(),
                gaussian_mean: run_config.distribution.gaussian_mean_value(),
                gaussian_stddev_frac: run_config.distribution.gaussian_stddev_frac(),
                gaussian_stddev: run_config.distribution.gaussian_stddev_value(),
                repetition: run_config.repetition,
                distribution_seed: run_config.distribution_seed,
                split_seed: run_config.split_seed,
                lookup_seed: run_config.lookup_seed,
                lookup_space: args.lookup_space.as_str(),
                lookup_count,
                batch_count,
                batch_layout: batch_layout.as_str(),
                bloom_false_positive_rate_target_percent: args.bloom_false_positive_rate * 100.0,
                bloom_seed: args.bloom_seed,
                bloom_expected_items,
                actual_touched_windows: actual_window_stats.touched_windows,
                actual_window_span: actual_window_stats.window_span,
                actual_keys_per_touched_window: actual_window_stats.keys_per_touched_window,
                predictor_total_keys: predictor_stats.batch_keys,
                predictor_global_min: predictor_stats.global_min,
                predictor_global_max: predictor_stats.global_max,
                predictor_span_windows_upper_bound: predictor_stats.span_windows_upper_bound,
                predictor_sum_batch_touched_windows: predictor_stats.sum_batch_touched_windows,
                predictor_touched_windows_estimate: predictor_stats.estimated_touched_containers,
                predictor_keys_per_window_estimate: predictor_stats.estimated_keys_per_container,
                predictor_overlap_factor: predictor_stats.overlap_factor,
                predictor_window_fill_ratio_estimate: predictor_stats
                    .estimated_container_fill_ratio,
                predictor_density_score: prediction.density_score,
                predictor_build_score: prediction.build_score,
                predictor_lookup_score: prediction.lookup_score,
                predictor_memory_score: prediction.memory_score,
                predicted_build_winner: prediction.build_winner.as_str(),
                predicted_lookup_winner: prediction.lookup_winner.as_str(),
                predicted_memory_winner: prediction.memory_winner.as_str(),
                bloom_build_ns_per_element: bloom.build_ns_per_element,
                roaring_build_ns_per_element: roaring.build_ns_per_element,
                build_ratio_bloom_over_roaring: bloom.build_ns_per_element
                    / roaring.build_ns_per_element,
                actual_build_winner: build_actual.as_str(),
                build_prediction_correct: prediction.build_winner == build_actual,
                bloom_lookup_ns_per_element: bloom.lookup_ns_per_element,
                bloom_lookup_hits: bloom.lookup_hits,
                bloom_lookup_hit_rate_percent: bloom.lookup_hits as f64 / lookup_count as f64
                    * 100.0,
                roaring_lookup_ns_per_element: roaring.lookup_ns_per_element,
                roaring_lookup_hits: roaring.lookup_hits,
                roaring_lookup_hit_rate_percent: roaring.lookup_hits as f64 / lookup_count as f64
                    * 100.0,
                lookup_ratio_bloom_over_roaring: bloom.lookup_ns_per_element
                    / roaring.lookup_ns_per_element,
                actual_lookup_winner: lookup_actual.as_str(),
                lookup_prediction_correct: prediction.lookup_winner == lookup_actual,
                bloom_bytes_used: bloom.bytes_used,
                roaring_bytes_used: roaring.bytes_used,
                memory_ratio_bloom_over_roaring: bloom.bytes_used as f64
                    / roaring.bytes_used as f64,
                actual_memory_winner: memory_actual.as_str(),
                memory_prediction_correct: prediction.memory_winner == memory_actual,
            });
        }
    }

    rows
}

#[derive(Debug, Clone)]
struct GeneratedBatch {
    keys: Vec<u32>,
}

impl GeneratedBatch {
    fn new(keys: Vec<u32>, _split_seed: u64) -> Self {
        Self { keys }
    }

    fn keys(&self) -> &[u32] {
        &self.keys
    }
}

#[derive(Debug, Clone)]
struct BatchSummary {
    key_count: usize,
    min_key: u32,
    max_key: u32,
    touched_windows: u16,
}

#[derive(Debug, Clone)]
pub struct RoaringSampleStats {
    /// Number of keys in the merged output batch.
    pub batch_keys: usize,

    /// Minimum key across the merged batches.
    pub global_min: u32,

    /// Maximum key across the merged batches.
    pub global_max: u32,

    /// Upper bound on distinct 16-bit containers touched by the merged batch,
    /// derived from the merged span.
    pub span_windows_upper_bound: usize,

    /// Exact sum of per-batch touched 16-bit containers.
    pub sum_batch_touched_windows: usize,

    /// Estimated distinct 16-bit containers touched by the merged batch.
    pub estimated_touched_containers: f64,

    /// Estimated real keys per touched 16-bit container.
    pub estimated_keys_per_container: f64,

    /// How much the batch span windows overlap. `1.0` means effectively
    /// disjoint span coverage; larger values mean many batches cover the same
    /// windows and the touched-window estimate should trust the summed batch
    /// counts less.
    pub overlap_factor: f64,

    /// Lower bound on occupancy of a touched container, normalized by 2^16.
    pub estimated_container_fill_ratio: f64,
}

#[derive(Debug, Clone, Copy)]
struct ActualWindowStats {
    touched_windows: usize,
    window_span: usize,
    keys_per_touched_window: f64,
}

#[derive(Debug, Clone)]
struct WindowBitset {
    words: [u64; WINDOW_BITSET_WORDS],
}

impl Default for WindowBitset {
    fn default() -> Self {
        Self {
            words: [0; WINDOW_BITSET_WORDS],
        }
    }
}

impl WindowBitset {
    fn insert_key(&mut self, key: u32) {
        let window = (key >> 16) as usize;
        let word_index = window / 64;
        let bit_index = window % 64;
        self.words[word_index] |= 1u64 << bit_index;
    }

    fn touched_window_count(&self) -> u16 {
        let count: u32 = self.words.iter().map(|word| word.count_ones()).sum();
        encode_touched_window_count(usize::try_from(count).expect("count fits in usize"))
    }
}

fn encode_touched_window_count(count: usize) -> u16 {
    assert!(count > 0, "batch summaries must be non-empty");
    assert!(
        count <= U32_CONTAINER_COUNT,
        "touched window count exceeded u32 window domain"
    );
    u16::try_from(count - 1).expect("encoded touched window count exceeded u16")
}

fn decode_touched_window_count(encoded: u16) -> usize {
    encoded as usize + 1
}

/// Split a merged keyset into `batch_count` simulated input batches.
///
/// `Disjoint` takes contiguous slices of the sorted keyset, so batch ranges are
/// mostly non-overlapping.
/// `Overlap` shuffles key positions before slicing, so batches contain random
/// subsets of the merged keyset and their min/max ranges usually overlap.
fn split_batch_summaries(
    keys: &[u32],
    batch_count: usize,
    batch_layout: BatchLayout,
    split_seed: u64,
) -> Vec<BatchSummary> {
    assert!(batch_count > 0);
    assert!(!keys.is_empty());

    match batch_layout {
        BatchLayout::Disjoint => (0..batch_count)
            .map(|batch_index| {
                let (start, end) = balanced_range(keys.len(), batch_count, batch_index);
                let slice = &keys[start..end];
                summarize_batch_keys(slice.iter().copied(), slice.len())
            })
            .collect(),
        BatchLayout::Overlap => {
            let permutation = AffinePermutation::random(
                keys.len() as u64,
                split_seed.wrapping_add(batch_count as u64),
            );
            (0..batch_count)
                .map(|batch_index| {
                    let (start, end) = balanced_range(keys.len(), batch_count, batch_index);
                    summarize_batch_keys(
                        (start..end)
                            .map(|position| keys[permutation.index_at(position as u64) as usize]),
                        end - start,
                    )
                })
                .collect()
        }
    }
}

fn balanced_range(total: usize, buckets: usize, bucket_index: usize) -> (usize, usize) {
    let start = total * bucket_index / buckets;
    let end = total * (bucket_index + 1) / buckets;
    (start, end)
}

fn summarize_batch_keys(keys: impl Iterator<Item = u32>, key_count: usize) -> BatchSummary {
    let mut min_key = u32::MAX;
    let mut max_key = u32::MIN;
    let mut touched_windows = WindowBitset::default();

    for key in keys {
        min_key = min_key.min(key);
        max_key = max_key.max(key);
        touched_windows.insert_key(key);
    }

    BatchSummary {
        key_count,
        min_key,
        max_key,
        touched_windows: touched_windows.touched_window_count(),
    }
}

/// Estimate Roaring lookup structure from merge-time batch metadata.
///
/// The estimator keeps two pieces of information separate:
/// - it knows the merged key count exactly as the sum of per-batch key counts
/// - it knows the global min/max exactly from batch mins/maxes
/// - it knows each batch's exact touched-window count
/// - it computes the exact union of 16-bit windows covered by the batch spans
///
/// From those we derive:
/// - a hard upper bound on touched windows from the merged span
/// - an estimated merged touched-window count from the summed per-batch counts
///   plus a damped span-overlap correction
/// - an estimated keys-per-window density
/// - an overlap factor from `sum(batch span windows) / union(batch span windows)`
///
/// For disjoint spans the merged touched-window estimate is just the sum of the
/// per-batch counts. For overlapping spans we still assume the batches are
/// mostly distinct, but we blend in overlap by dividing by a damped span
/// overlap factor. The merged span remains a hard cap.
fn estimate_roaring_bounds_stats(batch_summaries: &[BatchSummary]) -> RoaringSampleStats {
    let batch_keys = batch_summaries
        .iter()
        .map(|batch| batch.key_count)
        .sum::<usize>();
    let global_min = batch_summaries
        .iter()
        .map(|batch| batch.min_key)
        .min()
        .expect("batch summaries must be non-empty");
    let global_max = batch_summaries
        .iter()
        .map(|batch| batch.max_key)
        .max()
        .expect("batch summaries must be non-empty");
    let span_windows_upper_bound = covered_window_union(batch_summaries);
    let total_span_windows = batch_summaries
        .iter()
        .map(|batch| ((batch.max_key >> 16) - (batch.min_key >> 16) + 1) as usize)
        .sum::<usize>();
    let sum_batch_touched_windows = batch_summaries
        .iter()
        .map(|batch| decode_touched_window_count(batch.touched_windows))
        .sum::<usize>();
    let max_batch_touched_windows = batch_summaries
        .iter()
        .map(|batch| decode_touched_window_count(batch.touched_windows))
        .max()
        .expect("batch summaries must be non-empty");
    let overlap_factor = total_span_windows as f64 / span_windows_upper_bound as f64;
    let damped_overlap_factor = 1.0 + OVERLAP_FACTOR_DAMPING * (overlap_factor - 1.0);
    let estimated_touched_containers = (sum_batch_touched_windows as f64 / damped_overlap_factor)
        .clamp(
            max_batch_touched_windows as f64,
            span_windows_upper_bound as f64,
        )
        .min(U32_CONTAINER_COUNT as f64);
    let estimated_keys_per_container = batch_keys as f64 / estimated_touched_containers;
    let estimated_container_fill_ratio = estimated_keys_per_container / 65_536.0;

    RoaringSampleStats {
        batch_keys,
        global_min,
        global_max,
        span_windows_upper_bound,
        sum_batch_touched_windows,
        estimated_touched_containers,
        estimated_keys_per_container,
        overlap_factor: damped_overlap_factor,
        estimated_container_fill_ratio,
    }
}

fn covered_window_union(batch_summaries: &[BatchSummary]) -> usize {
    let mut intervals: Vec<(u32, u32)> = batch_summaries
        .iter()
        .map(|batch| (batch.min_key >> 16, batch.max_key >> 16))
        .collect();
    intervals.sort_unstable();

    let mut merged_windows = 0usize;
    let mut current = intervals[0];
    for interval in intervals.into_iter().skip(1) {
        if interval.0 <= current.1.saturating_add(1) {
            current.1 = current.1.max(interval.1);
        } else {
            merged_windows += (current.1 - current.0 + 1) as usize;
            current = interval;
        }
    }
    merged_windows + (current.1 - current.0 + 1) as usize
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum Winner {
    Bloom,
    Roaring,
}

impl Winner {
    fn as_str(self) -> &'static str {
        match self {
            Self::Bloom => "bloom",
            Self::Roaring => "roaring",
        }
    }
}

impl Display for Winner {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy)]
struct PredictorOutput {
    density_score: f64,
    build_score: f64,
    lookup_score: f64,
    memory_score: f64,
    build_winner: Winner,
    lookup_winner: Winner,
    memory_winner: Winner,
}

/// Convert conservative merge-time estimates into coarse Bloom-vs-Roaring winners.
///
/// The predictor intentionally uses different signals for different metrics:
/// - build: mostly "how many keys end up in each touched container?"
/// - memory: same question, but with a higher density threshold
/// - lookup: "how often does a random probe reach a touched container, and how
///   expensive is that container likely to be once it does?"
///
/// Example:
/// - Suppose a batch looks dense after sampling, with many keys per touched
///   container and only a small touched-container fraction. That usually pushes
///   all three metrics toward Roaring.
/// - Suppose instead the batch is spread across a large fraction of the 16-bit
///   containers and each container only has a modest number of keys. That is
///   the "many sparse array containers" regime where lookup can flip toward
///   Bloom.
///
/// The lookup path is where most of the iterations happened:
/// - using only keys/container missed sparse-wide cases
/// - using touched containers without normalizing them was the wrong shape
/// - using a flat array penalty missed that `ArrayStore::contains()` gets
///   slower as array containers grow
///
/// The current formula keeps the model simple while preserving those learned
/// corrections from the benchmark runs.
fn predict_filter_winner(stats: &RoaringSampleStats) -> PredictorOutput {
    let density_score = stats.estimated_container_fill_ratio;
    // Build and memory stay as simple density rules: if touched containers are
    // dense, Roaring tends to compress and build well; if they are sparse,
    // Bloom tends to be cheaper.
    let build_score =
        stats.estimated_keys_per_container / BUILD_ROARING_ESTIMATED_KEYS_PER_CONTAINER_THRESHOLD;
    // For lookups we need more than density. Random u32 probes only pay inner
    // container cost when they land in a touched 16-bit container, so the
    // touched-container estimate is normalized into a hit probability. If we
    // omit this term, the predictor cannot distinguish dense-in-a-few-containers
    // from equally dense batches spread across a large fraction of the domain.
    let lookup_container_probability =
        (stats.estimated_touched_containers / U32_CONTAINER_COUNT as f64).clamp(0.0, 1.0);
    // roaring-rs switches between array and bitmap containers around 4096
    // elements. Bitmap containers are close to a constant-time bit test, but
    // array containers use binary search and get meaningfully slower as they
    // grow. Without this size-dependent array penalty, medium-N wide Gaussians
    // with many sparse array containers were still over-predicted as Roaring.
    let lookup_container_penalty =
        if stats.estimated_keys_per_container >= ROARING_BITMAP_CONTAINER_THRESHOLD {
            LOOKUP_ROARING_BITMAP_CONTAINER_PROBABILITY_PENALTY
        } else {
            LOOKUP_ROARING_ARRAY_CONTAINER_PROBABILITY_PENALTY_BASE
                + LOOKUP_ROARING_ARRAY_CONTAINER_PROBABILITY_PENALTY_PER_LOG2_KEY
                    * (stats.estimated_keys_per_container + 1.0).log2()
        };
    // lookup_score >= 1.0 means the estimated Roaring lookup cost stays under
    // the current budget and we predict Roaring. The exact threshold is tuned
    // empirically from benchmark output; the important part is the shape above.
    let lookup_cost_proxy = lookup_container_probability * lookup_container_penalty;
    let lookup_score =
        LOOKUP_ROARING_CONTAINER_PROBABILITY_THRESHOLD / lookup_cost_proxy.max(f64::MIN_POSITIVE);
    let memory_score =
        stats.estimated_keys_per_container / MEMORY_ROARING_ESTIMATED_KEYS_PER_CONTAINER_THRESHOLD;

    PredictorOutput {
        density_score,
        build_score,
        lookup_score,
        memory_score,
        build_winner: predicted_winner(build_score),
        lookup_winner: predicted_winner(lookup_score),
        memory_winner: predicted_winner(memory_score),
    }
}

fn predicted_winner(score: f64) -> Winner {
    if score >= 1.0 {
        Winner::Roaring
    } else {
        Winner::Bloom
    }
}

#[derive(Debug, Clone, Copy)]
struct Measurement {
    build_ns_per_element: f64,
    lookup_ns_per_element: f64,
    lookup_hits: u64,
    bytes_used: usize,
}

fn benchmark_bloom(
    keys: &[u32],
    lookup_count: u64,
    lookup_seed: u64,
    lookup_space: LookupSpace,
    bloom_expected_items: u64,
    bloom_false_positive_rate: f64,
    bloom_seed: u128,
) -> Measurement {
    let expected_items =
        usize::try_from(bloom_expected_items).expect("bloom expected items must fit in usize");
    let mut bloom = BloomFilter::with_false_pos(bloom_false_positive_rate)
        .seed(&bloom_seed)
        .expected_items(expected_items.max(MIN_BLOOM_EXPECTED_ITEMS as usize));

    let build_started = Instant::now();
    for &key in keys {
        bloom.insert(&key);
    }
    let build_elapsed = build_started.elapsed();

    let (lookup_elapsed, hits) = benchmark_lookup(keys, lookup_count, lookup_seed, lookup_space, {
        |key| bloom.contains(&key)
    });

    Measurement {
        build_ns_per_element: build_elapsed.as_nanos() as f64 / keys.len() as f64,
        lookup_ns_per_element: lookup_elapsed.as_nanos() as f64 / lookup_count as f64,
        lookup_hits: hits,
        bytes_used: size_of_val(bloom.as_slice()),
    }
}

fn benchmark_roaring(
    keys: &[u32],
    lookup_count: u64,
    lookup_seed: u64,
    lookup_space: LookupSpace,
) -> Measurement {
    let build_started = Instant::now();
    let mut bitmap = RoaringBitmap::from_sorted_iter(keys.iter().copied())
        .expect("sorted gaussian keys should build a roaring bitmap");
    let build_elapsed = build_started.elapsed();
    let _ = bitmap.optimize();

    let (lookup_elapsed, hits) =
        benchmark_lookup(keys, lookup_count, lookup_seed, lookup_space, |key| {
            bitmap.contains(key)
        });

    Measurement {
        build_ns_per_element: build_elapsed.as_nanos() as f64 / keys.len() as f64,
        lookup_ns_per_element: lookup_elapsed.as_nanos() as f64 / lookup_count as f64,
        lookup_hits: hits,
        bytes_used: bitmap.serialized_size(),
    }
}

fn benchmark_lookup<F>(
    keys: &[u32],
    lookup_count: u64,
    lookup_seed: u64,
    lookup_space: LookupSpace,
    mut contains: F,
) -> (std::time::Duration, u64)
where
    F: FnMut(u32) -> bool,
{
    let lookup_started = Instant::now();
    let hits = match lookup_space {
        LookupSpace::Present => {
            let lookup_permutation = AffinePermutation::random(keys.len() as u64, lookup_seed);
            let mut hits = 0u64;
            for index in 0..lookup_count {
                let key = keys[lookup_permutation.index_at(index) as usize];
                hits += u64::from(contains(key));
            }
            assert_eq!(
                hits, lookup_count,
                "expected all present lookup keys to be present"
            );
            hits
        }
        LookupSpace::FullU32 => {
            let mut rng = ChaCha8Rng::seed_from_u64(lookup_seed);
            let mut hits = 0u64;
            for _ in 0..lookup_count {
                hits += u64::from(contains(rng.next_u32()));
            }
            hits
        }
    };
    (lookup_started.elapsed(), hits)
}

fn actual_winner(bloom_value: f64, roaring_value: f64) -> Winner {
    if roaring_value < bloom_value {
        Winner::Roaring
    } else {
        Winner::Bloom
    }
}

#[derive(Debug, Serialize)]
struct CsvRow {
    num_keys: u64,
    distribution: &'static str,
    distribution_param_name: &'static str,
    distribution_param_frac: Option<f64>,
    distribution_param_value: Option<f64>,
    gaussian_mean_frac: Option<f64>,
    gaussian_mean: Option<f64>,
    gaussian_stddev_frac: Option<f64>,
    gaussian_stddev: Option<f64>,
    repetition: usize,
    distribution_seed: u64,
    split_seed: u64,
    lookup_seed: u64,
    lookup_space: &'static str,
    lookup_count: u64,
    batch_count: usize,
    batch_layout: &'static str,
    bloom_false_positive_rate_target_percent: f64,
    bloom_seed: u128,
    bloom_expected_items: u64,
    actual_touched_windows: usize,
    actual_window_span: usize,
    actual_keys_per_touched_window: f64,
    predictor_total_keys: usize,
    predictor_global_min: u32,
    predictor_global_max: u32,
    predictor_span_windows_upper_bound: usize,
    predictor_sum_batch_touched_windows: usize,
    predictor_touched_windows_estimate: f64,
    predictor_keys_per_window_estimate: f64,
    predictor_overlap_factor: f64,
    predictor_window_fill_ratio_estimate: f64,
    predictor_density_score: f64,
    predictor_build_score: f64,
    predictor_lookup_score: f64,
    predictor_memory_score: f64,
    predicted_build_winner: &'static str,
    predicted_lookup_winner: &'static str,
    predicted_memory_winner: &'static str,
    bloom_build_ns_per_element: f64,
    roaring_build_ns_per_element: f64,
    build_ratio_bloom_over_roaring: f64,
    actual_build_winner: &'static str,
    build_prediction_correct: bool,
    bloom_lookup_ns_per_element: f64,
    bloom_lookup_hits: u64,
    bloom_lookup_hit_rate_percent: f64,
    roaring_lookup_ns_per_element: f64,
    roaring_lookup_hits: u64,
    roaring_lookup_hit_rate_percent: f64,
    lookup_ratio_bloom_over_roaring: f64,
    actual_lookup_winner: &'static str,
    lookup_prediction_correct: bool,
    bloom_bytes_used: usize,
    roaring_bytes_used: usize,
    memory_ratio_bloom_over_roaring: f64,
    actual_memory_winner: &'static str,
    memory_prediction_correct: bool,
}

#[derive(Debug, Default)]
struct AccuracySummary {
    runs: usize,
    build_correct: usize,
    lookup_correct: usize,
    memory_correct: usize,
}

fn summarize_accuracy(rows: &[CsvRow]) -> AccuracySummary {
    let mut accuracy = AccuracySummary::default();

    for row in rows {
        accuracy.runs += 1;
        accuracy.build_correct += usize::from(row.build_prediction_correct);
        accuracy.lookup_correct += usize::from(row.lookup_prediction_correct);
        accuracy.memory_correct += usize::from(row.memory_prediction_correct);
    }

    accuracy
}

fn print_summary(rows: &[CsvRow], accuracy: &AccuracySummary) {
    let wrong_rows: Vec<&CsvRow> = rows
        .iter()
        .filter(|row| {
            !row.build_prediction_correct
                || !row.lookup_prediction_correct
                || !row.memory_prediction_correct
        })
        .collect();
    let wrong_metric_predictions = wrong_rows
        .iter()
        .map(|row| {
            usize::from(!row.build_prediction_correct)
                + usize::from(!row.lookup_prediction_correct)
                + usize::from(!row.memory_prediction_correct)
        })
        .sum::<usize>();

    println!("summary.runs={}", accuracy.runs);
    println!(
        "accuracy.build={}/{}",
        accuracy.build_correct, accuracy.runs
    );
    println!(
        "accuracy.lookup={}/{}",
        accuracy.lookup_correct, accuracy.runs
    );
    println!(
        "accuracy.memory={}/{}",
        accuracy.memory_correct, accuracy.runs
    );
    println!("wrong_predictions.run_count={}", wrong_rows.len());
    println!(
        "wrong_predictions.metric_count={}",
        wrong_metric_predictions
    );

    for row in wrong_rows {
        println!(
            "wrong_prediction {} num_keys={} repetition={} batch_count={} batch_layout={}",
            distribution_summary_fields(row),
            row.num_keys,
            row.repetition,
            row.batch_count,
            row.batch_layout
        );
        println!(
            "wrong_prediction.predictor total_keys={} global_min={} global_max={} span_windows_upper_bound={} sum_batch_touched_windows={} touched_windows_estimate={:.6} keys_per_window_estimate={:.6} overlap_factor={:.6} window_fill_ratio_estimate={:.6}",
            row.predictor_total_keys,
            row.predictor_global_min,
            row.predictor_global_max,
            row.predictor_span_windows_upper_bound,
            row.predictor_sum_batch_touched_windows,
            row.predictor_touched_windows_estimate,
            row.predictor_keys_per_window_estimate,
            row.predictor_overlap_factor,
            row.predictor_window_fill_ratio_estimate
        );

        if !row.build_prediction_correct {
            println!(
                "wrong_prediction.build predicted={} actual={} score={:.6} bloom_over_roaring={:.6}",
                row.predicted_build_winner,
                row.actual_build_winner,
                row.predictor_build_score,
                row.build_ratio_bloom_over_roaring
            );
        }
        if !row.lookup_prediction_correct {
            println!(
                "wrong_prediction.lookup predicted={} actual={} score={:.6} bloom_over_roaring={:.6}",
                row.predicted_lookup_winner,
                row.actual_lookup_winner,
                row.predictor_lookup_score,
                row.lookup_ratio_bloom_over_roaring
            );
        }
        if !row.memory_prediction_correct {
            println!(
                "wrong_prediction.memory predicted={} actual={} score={:.6} bloom_over_roaring={:.6}",
                row.predicted_memory_winner,
                row.actual_memory_winner,
                row.predictor_memory_score,
                row.memory_ratio_bloom_over_roaring
            );
        }
    }
}

fn print_run_report(row: &CsvRow) {
    println!("distribution={}", row.distribution);
    println!("distribution_param_name={}", row.distribution_param_name);
    println!(
        "distribution_param_frac={}",
        option_f64(row.distribution_param_frac)
    );
    println!(
        "distribution_param_value={}",
        option_f64(row.distribution_param_value)
    );
    println!("num_keys={}", row.num_keys);
    println!("gaussian_mean_frac={}", option_f64(row.gaussian_mean_frac));
    println!("gaussian_mean={}", option_f64(row.gaussian_mean));
    println!(
        "gaussian_stddev_frac={}",
        option_f64(row.gaussian_stddev_frac)
    );
    println!("gaussian_stddev={}", option_f64(row.gaussian_stddev));
    println!("repetition={}", row.repetition);
    println!("batch_count={}", row.batch_count);
    println!("batch_layout={}", row.batch_layout);
    println!("lookup_space={}", row.lookup_space);
    println!("lookup_count={}", row.lookup_count);
    println!("actual.touched_windows={}", row.actual_touched_windows);
    println!("actual.window_span={}", row.actual_window_span);
    println!(
        "actual.keys_per_touched_window={:.6}",
        row.actual_keys_per_touched_window
    );
    println!("predictor.total_keys={}", row.predictor_total_keys);
    println!("predictor.global_min={}", row.predictor_global_min);
    println!("predictor.global_max={}", row.predictor_global_max);
    println!(
        "predictor.span_windows_upper_bound={}",
        row.predictor_span_windows_upper_bound
    );
    println!(
        "predictor.sum_batch_touched_windows={}",
        row.predictor_sum_batch_touched_windows
    );
    println!(
        "predictor.touched_windows_estimate={:.6}",
        row.predictor_touched_windows_estimate
    );
    println!(
        "predictor.keys_per_window_estimate={:.6}",
        row.predictor_keys_per_window_estimate
    );
    println!(
        "predictor.overlap_factor={:.6}",
        row.predictor_overlap_factor
    );
    println!(
        "predictor.window_fill_ratio_estimate={:.6}",
        row.predictor_window_fill_ratio_estimate
    );
    println!("predictor.build_score={:.6}", row.predictor_build_score);
    println!("predictor.lookup_score={:.6}", row.predictor_lookup_score);
    println!("predictor.memory_score={:.6}", row.predictor_memory_score);
    println!("predicted.build_winner={}", row.predicted_build_winner);
    println!("predicted.lookup_winner={}", row.predicted_lookup_winner);
    println!("predicted.memory_winner={}", row.predicted_memory_winner);
    println!(
        "bloom.build_ns_per_element={:.6}",
        row.bloom_build_ns_per_element
    );
    println!(
        "roaring.build_ns_per_element={:.6}",
        row.roaring_build_ns_per_element
    );
    println!(
        "build_ratio_bloom_over_roaring={:.6}",
        row.build_ratio_bloom_over_roaring
    );
    println!("actual.build_winner={}", row.actual_build_winner);
    println!("build_prediction_correct={}", row.build_prediction_correct);
    println!(
        "bloom.lookup_ns_per_element={:.6}",
        row.bloom_lookup_ns_per_element
    );
    println!("bloom.lookup_hits={}", row.bloom_lookup_hits);
    println!(
        "bloom.lookup_hit_rate_percent={:.6}",
        row.bloom_lookup_hit_rate_percent
    );
    println!(
        "roaring.lookup_ns_per_element={:.6}",
        row.roaring_lookup_ns_per_element
    );
    println!("roaring.lookup_hits={}", row.roaring_lookup_hits);
    println!(
        "roaring.lookup_hit_rate_percent={:.6}",
        row.roaring_lookup_hit_rate_percent
    );
    println!(
        "lookup_ratio_bloom_over_roaring={:.6}",
        row.lookup_ratio_bloom_over_roaring
    );
    println!("actual.lookup_winner={}", row.actual_lookup_winner);
    println!(
        "lookup_prediction_correct={}",
        row.lookup_prediction_correct
    );
    println!("bloom.bytes_used={}", row.bloom_bytes_used);
    println!("roaring.bytes_used={}", row.roaring_bytes_used);
    println!(
        "memory_ratio_bloom_over_roaring={:.6}",
        row.memory_ratio_bloom_over_roaring
    );
    println!("actual.memory_winner={}", row.actual_memory_winner);
    println!(
        "memory_prediction_correct={}",
        row.memory_prediction_correct
    );
    println!();
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

    fn index_at(&self, position: u64) -> u64 {
        debug_assert!(position < self.len);
        (self
            .multiplier
            .wrapping_mul(position)
            .wrapping_add(self.offset))
            % self.len
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

fn generate_keys(num_keys: u64, distribution: DistributionSpec, seed: u64) -> Vec<u32> {
    match distribution {
        DistributionSpec::Gaussian(distribution) => {
            generate_gaussian_keys(num_keys, distribution, seed)
        }
        DistributionSpec::Consecutive => generate_consecutive_keys(num_keys),
        DistributionSpec::RoundRobinContainer => generate_round_robin_container_keys(num_keys),
        DistributionSpec::Bimodal { stddev_frac } => {
            generate_bimodal_keys(num_keys, stddev_frac, seed)
        }
        DistributionSpec::Exponential { scale_frac } => {
            generate_exponential_keys(num_keys, scale_frac, seed)
        }
        DistributionSpec::HoleyWide => generate_holey_wide_keys(num_keys, seed),
    }
}

fn generate_gaussian_keys(
    num_keys: u64,
    distribution: GaussianDistribution,
    seed: u64,
) -> Vec<u32> {
    let len = usize::try_from(num_keys).expect("num_keys must fit in usize");
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let normal = Normal::new(distribution.mean_value(), distribution.stddev_value())
        .expect("gaussian distribution should have a positive standard deviation");
    let mut keys = Vec::with_capacity(len);

    for _ in 0..num_keys {
        let sampled = normal.sample(&mut rng).round();
        keys.push(sampled.clamp(0.0, u32::MAX as f64) as u32);
    }

    keys.sort_unstable();
    project_sorted_unique_u32_domain(&mut keys);
    keys
}

fn generate_consecutive_keys(num_keys: u64) -> Vec<u32> {
    let len = usize::try_from(num_keys).expect("num_keys must fit in usize");
    (0..len)
        .map(|index| u32::try_from(index).expect("consecutive key exceeded u32 domain"))
        .collect()
}

fn generate_round_robin_container_keys(num_keys: u64) -> Vec<u32> {
    let len = usize::try_from(num_keys).expect("num_keys must fit in usize");
    let mut keys = Vec::with_capacity(len);
    let full_layers = num_keys / U32_CONTAINER_COUNT as u64;
    let partial_containers = num_keys % U32_CONTAINER_COUNT as u64;

    for container in 0..U32_CONTAINER_COUNT as u64 {
        let keys_in_container = full_layers + u64::from(container < partial_containers);
        let container_base = container << 16;
        for low in 0..keys_in_container {
            keys.push(
                u32::try_from(container_base + low).expect("round-robin key exceeded u32 domain"),
            );
        }
    }

    debug_assert_eq!(keys.len(), len);
    keys
}

fn generate_bimodal_keys(num_keys: u64, stddev_frac: f64, seed: u64) -> Vec<u32> {
    let len = usize::try_from(num_keys).expect("num_keys must fit in usize");
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let left = Normal::new(
        BIMODAL_LEFT_PEAK_FRAC * u32::MAX as f64,
        stddev_frac * u32::MAX as f64,
    )
    .expect("bimodal distribution should have a positive standard deviation");
    let right = Normal::new(
        BIMODAL_RIGHT_PEAK_FRAC * u32::MAX as f64,
        stddev_frac * u32::MAX as f64,
    )
    .expect("bimodal distribution should have a positive standard deviation");
    let mut keys = Vec::with_capacity(len);

    for _ in 0..num_keys {
        let sampled = if rng.next_u32() & 1 == 0 {
            left.sample(&mut rng)
        } else {
            right.sample(&mut rng)
        }
        .round();
        keys.push(sampled.clamp(0.0, u32::MAX as f64) as u32);
    }

    keys.sort_unstable();
    project_sorted_unique_u32_domain(&mut keys);
    keys
}

fn generate_exponential_keys(num_keys: u64, scale_frac: f64, seed: u64) -> Vec<u32> {
    let len = usize::try_from(num_keys).expect("num_keys must fit in usize");
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let scale = (scale_frac * u32::MAX as f64).max(f64::MIN_POSITIVE);
    let distribution =
        Exp::new(1.0 / scale).expect("exponential distribution should have a positive scale");
    let mut keys = Vec::with_capacity(len);

    for _ in 0..num_keys {
        let sampled = distribution.sample(&mut rng).round();
        keys.push(sampled.clamp(0.0, u32::MAX as f64) as u32);
    }

    keys.sort_unstable();
    project_sorted_unique_u32_domain(&mut keys);
    keys
}

fn generate_holey_wide_keys(num_keys: u64, seed: u64) -> Vec<u32> {
    let len = usize::try_from(num_keys).expect("num_keys must fit in usize");
    let domain_window_count =
        usize::try_from(HOLEY_WIDE_DOMAIN_MAX_EXCLUSIVE / 65_536).expect("window count fits");
    assert!(
        domain_window_count >= 2,
        "holey-wide domain must span at least 2 windows"
    );

    let mut touched_window_count = HOLEY_WIDE_MIN_TOUCHED_WINDOWS
        .max(ceil_div_u64(num_keys, HOLEY_WIDE_TARGET_KEYS_PER_WINDOW));
    touched_window_count = touched_window_count.max(ceil_div_u64(num_keys, 65_536));
    touched_window_count = touched_window_count.min(domain_window_count as u64);
    let touched_window_count =
        usize::try_from(touched_window_count).expect("touched window count fits");

    // We saw this shape in the live Delta pipeline
    // `delta_bigint_sequence_keys`: batches span almost the full 2B-key
    // domain, but only a few hundred 16-bit Roaring windows are actually
    // populated inside that wide range.
    let mut touched_windows = Vec::with_capacity(touched_window_count);
    touched_windows.push(0u32);
    if touched_window_count > 1 {
        touched_windows.push((domain_window_count - 1) as u32);
    }
    if touched_window_count > 2 {
        let permutation = AffinePermutation::random((domain_window_count - 2) as u64, seed);
        while touched_windows.len() < touched_window_count {
            let window = 1 + permutation.index_at((touched_windows.len() - 2) as u64) as u32;
            touched_windows.push(window);
        }
    }
    touched_windows.sort_unstable();

    let mut keys = Vec::with_capacity(len);
    let base_keys_per_window = num_keys / touched_window_count as u64;
    let extra_windows = (num_keys % touched_window_count as u64) as usize;
    for (window_index, &window) in touched_windows.iter().enumerate() {
        let keys_in_window = base_keys_per_window + u64::from(window_index < extra_windows);
        let window_base = (window as u64) << 16;
        for low in 0..keys_in_window {
            keys.push(u32::try_from(window_base + low).expect("holey-wide key exceeded domain"));
        }
    }

    debug_assert_eq!(keys.len(), len);
    keys
}

fn actual_window_stats(keys: &[u32]) -> ActualWindowStats {
    let touched_windows = count_distinct_windows(keys);
    let window_span = ((keys[keys.len() - 1] >> 16) - (keys[0] >> 16) + 1) as usize;
    ActualWindowStats {
        touched_windows,
        window_span,
        keys_per_touched_window: keys.len() as f64 / touched_windows as f64,
    }
}

fn count_distinct_windows(keys: &[u32]) -> usize {
    let mut distinct_windows = 0usize;
    let mut previous_window = None;
    for &key in keys {
        let window = key >> 16;
        if previous_window != Some(window) {
            distinct_windows += 1;
            previous_window = Some(window);
        }
    }
    distinct_windows
}

fn ceil_div_u64(lhs: u64, rhs: u64) -> u64 {
    lhs.div_ceil(rhs)
}

fn parse_u64_csv(csv: &str) -> Vec<u64> {
    let mut out: Vec<u64> = csv
        .split(',')
        .filter(|entry| !entry.trim().is_empty())
        .map(|entry| {
            parse_u64_token(entry.trim())
                .unwrap_or_else(|error| panic!("invalid u64 in CSV: {entry} ({error})"))
        })
        .collect();
    out.sort_unstable();
    out.dedup();
    out
}

fn parse_usize_csv(csv: &str) -> Vec<usize> {
    let mut out: Vec<usize> = csv
        .split(',')
        .filter(|entry| !entry.trim().is_empty())
        .map(|entry| {
            entry
                .trim()
                .replace('_', "")
                .parse::<usize>()
                .unwrap_or_else(|error| panic!("invalid usize in CSV: {entry} ({error})"))
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

fn parse_distribution_csv(csv: &str) -> Vec<DistributionKind> {
    let mut out = Vec::new();

    for token in csv.split(',').filter(|entry| !entry.trim().is_empty()) {
        let normalized = token.trim().replace('_', "-");
        let distribution = DistributionKind::from_str(&normalized, true).unwrap_or_else(|error| {
            panic!("invalid distribution in --distributions: {token} ({error})")
        });
        if !out.contains(&distribution) {
            out.push(distribution);
        }
    }

    out
}

fn parse_batch_layout_csv(csv: &str) -> Vec<BatchLayout> {
    let mut out = Vec::new();

    for token in csv.split(',').filter(|entry| !entry.trim().is_empty()) {
        let normalized = token.trim().replace('_', "-");
        let layout = match normalized.as_str() {
            "disjoint" => BatchLayout::Disjoint,
            "overlap" => BatchLayout::Overlap,
            _ => panic!("invalid batch layout in --batch-layouts: {token}"),
        };
        if !out.contains(&layout) {
            out.push(layout);
        }
    }

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

fn option_u64(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "auto".to_string())
}

fn option_f64(value: Option<f64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "auto".to_string())
}

fn distribution_summary_fields(row: &CsvRow) -> String {
    let mut fields = format!("distribution={}", row.distribution);
    if let Some(gaussian_mean_frac) = row.gaussian_mean_frac {
        fields.push_str(&format!(" gaussian_mean_frac={gaussian_mean_frac}"));
    }
    if let Some(distribution_param_frac) = row.distribution_param_frac {
        fields.push_str(&format!(
            " {}={distribution_param_frac}",
            row.distribution_param_name
        ));
    }
    fields
}
