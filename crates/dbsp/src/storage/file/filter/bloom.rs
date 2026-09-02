//! Modular Bloom filters instantiated for Feldera batches.
//!
//! The modular-bloom crate supports filters with the arbitrary number of modules and
//! hashes per module. We instantiate it with up to 4 modules, with 3 hashes per module
//! corresponding to 4 fixed false positive rates:
//!
//! fp=1.0 - no filter, 0 bits/key
//! fp=0.1 - 1 module, 4.81 bits/key
//! fp=0.01 - 2 modules, 9.62 bits/key
//! fp=0.001 - 3 modules, 14.42 bits/key
//! fp=0.0001 - 4 modules, 19.23 bits/key

use crate::storage::tracking_bloom_filter::TrackingBloomFilter;
use feldera_modular_bloom::{ModularBloomFilter, ModuleDensity, ModuleLayout};

/// Maximum number of modules in a Bloom filter.
pub(crate) const MAX_MODULES: u32 = 4;

/// Number of modules that deliver `rate`.
///
/// Rounds to the nearest rate in the table above. Rounding is in log space
/// because the module count is linear in the exponent, not in the rate.
///
/// # Arguments
///
/// - `rate`: target false positive rate.
///
/// # Returns
///
/// A count from 0 to [`MAX_MODULES`]. Zero for any `rate` at or outside
/// `(0, 1)`, which is how a pipeline asks for no Bloom filter.
pub(crate) fn modules_for_rate(rate: f64) -> u32 {
    if !(rate > 0.0 && rate < 1.0) {
        return 0;
    }
    // Round in log space: the module count is linear in the exponent, not in
    // the rate.
    let modules = rate.recip().log10().round();
    if modules <= 0.0 {
        0
    } else {
        (modules as u32).min(MAX_MODULES)
    }
}

/// False positive rate that `modules` modules deliver.
///
/// # Arguments
///
/// - `modules`: a module count from 0 to [`MAX_MODULES`]. Larger counts return
///   a rate finer than any filter is built for.
///
/// # Returns
///
/// The rate, one decade per module.
pub(crate) fn rate_for_modules(modules: u32) -> f64 {
    10f64.powi(-(modules as i32))
}

/// Whether `rate` can drop a module from a filter written at the finest rate.
///
/// A reader asks this before reading anything, because at the finest rate no
/// module can be dropped and reading the header first would save it nothing.
///
/// # Arguments
///
/// - `rate`: the configured false positive rate.
///
/// # Returns
///
/// `true` when `rate` keeps fewer than [`MAX_MODULES`] modules.
pub(crate) fn rate_can_evict(rate: f64) -> bool {
    modules_for_rate(rate) < MAX_MODULES
}

/// Number of leading modules to keep resident when loading a stored filter.
///
/// # Arguments
///
/// - `layout`: shape of the stored filter, including how many modules it
///   holds.
/// - `density`: bits set in each of those modules, recorded when the filter was
///   written.
/// - `rate`: the configured false positive rate.
///
/// # Returns
///
/// The smallest number of leading modules whose combined false positive rate is
/// no worse than `rate`, limited both by what `rate` maps to in the table above
/// and by what the file holds. Zero when `rate` asks for no filter.
pub(crate) fn resident_modules(layout: &ModuleLayout, density: &ModuleDensity, rate: f64) -> u32 {
    let nominal = modules_for_rate(rate);
    if nominal == 0 {
        return 0;
    }
    // `density.modules_for` answers for the exact `rate`, while `rate` is only
    // as precise as the table it was rounded to. Capping at `nominal` keeps a
    // filter measuring slightly worse than its table entry from being given
    // another module, and twice the memory, to close that gap.
    density
        .modules_for(layout, rate)
        .min(nominal)
        .min(layout.total_modules())
}

/// Creates an empty filter for a batch that is about to be written.
///
/// # Arguments
///
/// - `estimated_keys`: keys the batch is expected to hold. Used as given,
///   including zero, which sizes every module to its one-word minimum.
/// - `bloom_false_positive_rate`: the configured false positive rate.
///
/// # Returns
///
/// - `None` when the rate asks for no filter, or when the batch needs more
///   words per module than the `u32` a layout counts them in. No filter is
///   better than one that cannot address all of its own bits.
/// - Otherwise a filter ready to accept keys.
pub(super) fn new_bloom_filter(
    estimated_keys: usize,
    bloom_false_positive_rate: f64,
) -> Option<TrackingBloomFilter> {
    let modules = modules_for_rate(bloom_false_positive_rate);
    if modules == 0 {
        return None;
    }
    let layout =
        ModuleLayout::for_keys(estimated_keys as u64, rate_for_modules(modules), modules).ok()?;
    Some(TrackingBloomFilter::building(layout))
}

/// Loads a filter written in the format that predates modular filters.
///
/// Such a filter is a single module, with whatever hash count its writer chose,
/// so it has no module to drop.
///
/// # Arguments
///
/// - `num_hashes`: hashes per key, from the file.
/// - `data`: the filter's bits, from the file.
/// - `bloom_false_positive_rate`: the configured false positive rate.
///
/// # Returns
///
/// - `None` when the rate asks for no filter, or when `data` holds more words
///   than a module is counted in.
/// - Otherwise the filter, with every module it was written with.
pub(super) fn deserialize_bloom_filter(
    num_hashes: u32,
    data: Vec<u64>,
    bloom_false_positive_rate: f64,
) -> Option<TrackingBloomFilter> {
    let layout = ModuleLayout::monolithic(num_hashes, data.len()).ok()?;
    if modules_for_rate(bloom_false_positive_rate) == 0 {
        // The rate asks for no filter, and a one-module filter has nothing to
        // shed short of the whole thing.
        return None;
    }
    ModularBloomFilter::from_modules(layout, &data)
        .ok()
        .map(TrackingBloomFilter::loaded)
}

/// Loads a modular filter from modules already read out of a file.
///
/// # Arguments
///
/// - `layout`: shape of the stored filter.
/// - `words`: its leading modules, as read from the file.
/// - `density`: bits set in every module the file holds, including any `words`
///   leaves out, which is what lets the filter report the rate other module
///   counts would give.
///
/// # Returns
///
/// - `None` when `words` is not a whole number of modules, or holds more
///   modules than `layout` declares.
/// - Otherwise the filter, holding the modules in `words`.
pub(super) fn load_modular_bloom_filter(
    layout: ModuleLayout,
    words: &[u64],
    density: ModuleDensity,
) -> Option<TrackingBloomFilter> {
    ModularBloomFilter::from_modules_with_density(layout, words, density)
        .ok()
        .map(TrackingBloomFilter::loaded)
}

#[cfg(test)]
mod tests {
    use super::{MAX_MODULES, modules_for_rate, rate_for_modules, resident_modules};
    use feldera_modular_bloom::{ModuleDensity, ModuleLayout};

    #[test]
    fn rates_map_to_modules_in_log_space() {
        assert_eq!(modules_for_rate(1e-4), 4);
        assert_eq!(modules_for_rate(1e-3), 3);
        assert_eq!(modules_for_rate(1e-2), 2);
        assert_eq!(modules_for_rate(1e-1), 1);
        assert_eq!(modules_for_rate(1.0), 0);

        // Rounding is by exponent, so 5e-4 goes to 1e-3 rather than to the
        // linearly closer 1e-4.
        assert_eq!(modules_for_rate(5e-4), 3);
        assert_eq!(modules_for_rate(3e-4), 4);
        assert_eq!(modules_for_rate(0.5), 0);
        assert_eq!(modules_for_rate(0.03), 2);

        // Every spelling of "no filter".
        assert_eq!(modules_for_rate(0.0), 0);
        assert_eq!(modules_for_rate(1.0), 0);
        assert_eq!(modules_for_rate(-1.0), 0);
        assert_eq!(modules_for_rate(2.0), 0);
        assert_eq!(modules_for_rate(f64::NAN), 0);

        // More accuracy than the ladder offers is capped, not extrapolated.
        assert_eq!(modules_for_rate(1e-9), MAX_MODULES);
    }

    #[test]
    fn module_counts_map_back_to_their_rates() {
        for modules in 1..=MAX_MODULES {
            assert_eq!(modules_for_rate(rate_for_modules(modules)), modules);
        }
    }

    /// Every supported rate produces the same module, which is what makes a
    /// filter written at one rate a prefix of one written at a better rate.
    #[test]
    fn every_rate_produces_the_same_module() {
        let keys = 1_000_000;
        let reference = ModuleLayout::for_keys(keys, rate_for_modules(1), 1).unwrap();
        for modules in 2..=MAX_MODULES {
            let layout = ModuleLayout::for_keys(keys, rate_for_modules(modules), modules).unwrap();
            assert_eq!(layout.words_per_module(), reference.words_per_module());
            assert_eq!(layout.hashes_per_module(), reference.hashes_per_module());
            assert_eq!(layout.total_modules(), modules);
        }
    }

    /// A density in which every module delivers exactly `rate`.
    ///
    /// A module holding a fraction `f` of its bits rejects all but `f^hashes`
    /// of absent keys, so `f = rate^(1/hashes)` is the fill that buys `rate`.
    fn density_delivering(layout: &ModuleLayout, rate: f64) -> ModuleDensity {
        let fill = rate.powf(1.0 / f64::from(layout.hashes_per_module()));
        let set = (layout.bits_per_module() as f64 * fill).round() as u64;
        ModuleDensity::new(vec![set; layout.total_modules() as usize])
    }

    /// What a correctly sized filter measures: one decade per module.
    fn uniform(layout: &ModuleLayout) -> ModuleDensity {
        density_delivering(layout, 0.1)
    }

    #[test]
    fn residency_never_exceeds_what_the_file_holds() {
        let layout = ModuleLayout::for_keys(100_000, 1e-2, 2).unwrap();
        let density = uniform(&layout);
        // A more accurate rate cannot conjure modules the file lacks.
        assert_eq!(resident_modules(&layout, &density, 1e-4), 2);
        assert_eq!(resident_modules(&layout, &density, 1e-2), 2);
        assert_eq!(resident_modules(&layout, &density, 1e-1), 1);
        assert_eq!(resident_modules(&layout, &density, 1.0), 0);
        assert_eq!(resident_modules(&layout, &density, 0.0), 0);
    }

    /// A filter that came out more accurate than its nominal rate keeps fewer
    /// modules than the nominal ladder would have kept.
    #[test]
    fn residency_follows_the_rate_the_modules_actually_give() {
        let layout = ModuleLayout::for_keys(100_000, 1e-4, 4).unwrap();
        // Far fewer keys than the layout was sized for, so every module is
        // sparse and one alone already beats the target.
        let sparse = density_delivering(&layout, 1e-3);
        let nominal = uniform(&layout);
        assert_eq!(resident_modules(&layout, &nominal, 1e-2), 2);
        assert_eq!(resident_modules(&layout, &sparse, 1e-2), 1);

        // The nominal count is a cap, so a filter that came out worse than
        // nominal is not given modules the ladder would not have kept.
        let dense = density_delivering(&layout, 0.5);
        assert_eq!(resident_modules(&layout, &dense, 1e-2), 2);
    }
}
