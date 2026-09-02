//! Shared fixtures for the modular Bloom filter tests.
//!
//! Every generator here is deterministic. A filter test that samples randomly
//! reports a different false positive rate on every run, which turns a real
//! accuracy regression into "that test is flaky".

#![allow(dead_code)]

use feldera_modular_bloom::{ModularBloomFilter, ModularBloomFilterBuilder, ModuleLayout};

/// The splitmix64 finalizer, a bijection on `u64`.
///
/// Bijectivity is what lets present and absent key streams be split by parity
/// and be provably disjoint, rather than disjoint by luck.
pub fn mix64(mut x: u64) -> u64 {
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

/// Hash of the `i`th key that is inserted into the filter.
pub fn present(i: u64) -> u64 {
    mix64(2 * i)
}

/// Hash of the `i`th key that is never inserted into any filter.
pub fn absent(i: u64) -> u64 {
    mix64(2 * i + 1)
}

/// A named point in the (layout, dataset) matrix.
#[derive(Clone, Copy, Debug)]
pub struct Config {
    pub name: &'static str,
    pub keys: u64,
    pub target_fp: f64,
    pub modules: u32,
}

/// The matrix every deterministic test walks.
///
/// `d1` is the legacy interop shape and `d16` is the only shape with one hash
/// per module, where `fill^d` and `fill^(d*hashes)` coincide and a dropped hash
/// count in the rate formula would otherwise hide.
pub const CONFIGS: &[Config] = &[
    Config {
        name: "d4_fp1e-4",
        keys: 20_000,
        target_fp: 1e-4,
        modules: 4,
    },
    Config {
        name: "d8_fp1e-4",
        keys: 20_000,
        target_fp: 1e-4,
        modules: 8,
    },
    Config {
        name: "d16_fp1e-2",
        keys: 20_000,
        target_fp: 1e-2,
        modules: 16,
    },
    Config {
        name: "d1_fp1e-4",
        keys: 20_000,
        target_fp: 1e-4,
        modules: 1,
    },
    Config {
        name: "d3_fp1e-6",
        keys: 5_000,
        target_fp: 1e-6,
        modules: 3,
    },
];

impl Config {
    pub fn layout(&self) -> ModuleLayout {
        ModuleLayout::for_keys(self.keys, self.target_fp, self.modules)
            .unwrap_or_else(|e| panic!("{}: layout: {e}", self.name))
    }

    /// Builds the filter with all modules resident and every key inserted.
    pub fn build(&self) -> ModularBloomFilter {
        build_with(self.layout(), self.keys)
    }

    /// Flattened words of the fully resident filter, as a caller would persist.
    pub fn words(&self) -> Vec<u64> {
        flatten(&self.build())
    }
}

pub fn build_with(layout: ModuleLayout, keys: u64) -> ModularBloomFilter {
    let mut builder = ModularBloomFilterBuilder::new(layout);
    for i in 0..keys {
        builder.insert_hash(present(i));
    }
    builder.finish()
}

/// Concatenates the resident modules into the single buffer a caller writes.
pub fn flatten(filter: &ModularBloomFilter) -> Vec<u64> {
    filter.modules().concat()
}

/// Counts admissions over `probes` keys that were never inserted.
pub fn count_false_positives(filter: &ModularBloomFilter, probes: u64) -> u64 {
    (0..probes)
        .filter(|&i| filter.contains_hash(absent(i)))
        .count() as u64
}

/// Counts inserted keys the filter fails to admit. Must always be zero.
pub fn count_false_negatives(filter: &ModularBloomFilter, keys: u64) -> u64 {
    (0..keys)
        .filter(|&i| !filter.contains_hash(present(i)))
        .count() as u64
}

/// Half-width of the band a count is accepted within.
///
/// The band is on the count rather than the rate: sampling noise on a rare
/// event is Poisson in the count, so a fixed relative band on the rate is far
/// too tight when the expected count is small and far too loose when it is
/// large.
///
/// # Arguments
///
/// - `expected`: the count the model predicts.
/// - `rel`: fraction of `expected` to widen the band by, covering error in the
///   model itself on top of the sampling noise.
fn band_half_width(expected: f64, rel: f64) -> f64 {
    5.0 * (expected + 1.0).sqrt() + rel * expected
}

/// Asserts that `observed` falls within the band around `expected`.
///
/// # Arguments
///
/// - `observed`: the count the test measured.
/// - `expected`: the count the model predicts.
/// - `rel`: fraction of `expected` to widen the band by, as in
///   [`band_half_width`].
/// - `what`: names the measurement in the failure message.
pub fn assert_within_band(observed: f64, expected: f64, rel: f64, what: &str) {
    let band = band_half_width(expected, rel);
    assert!(
        (observed - expected).abs() <= band,
        "{what}: observed {observed}, expected {expected}, band +/-{band}"
    );
}

/// Rebuilds a filter from `words` with module `zeroed` blanked out.
pub fn with_module_zeroed(layout: ModuleLayout, words: &[u64], zeroed: u32) -> ModularBloomFilter {
    let per = layout.words_per_module() as usize;
    let mut damaged = words.to_vec();
    let start = zeroed as usize * per;
    damaged[start..start + per].fill(0);
    ModularBloomFilter::from_modules(layout, &damaged).unwrap()
}

/// Rebuilds a filter from `words` with the modules rotated by one position.
///
/// Every module keeps its own bits but answers under a different module index,
/// so a filter whose modules are genuinely independent rejects almost every key
/// it contains. A filter whose modules are correlated still accepts them.
pub fn with_modules_rotated(layout: ModuleLayout, words: &[u64]) -> ModularBloomFilter {
    let per = layout.words_per_module() as usize;
    let mut rotated = words.to_vec();
    rotated.rotate_left(per);
    ModularBloomFilter::from_modules(layout, &rotated).unwrap()
}
