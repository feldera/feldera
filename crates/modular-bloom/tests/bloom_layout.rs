//! Geometry validation and sizing.
//!
//! These run on the constructor alone, with no filter and no probing, so they
//! cover cases a data-driven test cannot reach: a module too large to address,
//! a hash count of zero, and sizing rounding that only shows at small key
//! counts.

mod bloom_common;

use feldera_modular_bloom::{LayoutError, ModuleLayout};

/// A module may be as wide as its `u32` word count allows. There is no smaller
/// ceiling: the index reduction multiplies by the word count rather than the
/// bit count, so both factors stay below 2^32 and it addresses the whole
/// module however large it is.
#[test]
fn accepts_the_widest_module_the_layout_can_express() {
    assert!(ModuleLayout::new(1, u32::MAX, 4).is_ok());

    // The filter as a whole still has to be describable and allocatable.
    assert_eq!(
        ModuleLayout::new(u32::MAX, u32::MAX, 1),
        Err(LayoutError::Overflow {
            modules: u32::MAX,
            words_per_module: u32::MAX,
        })
    );
}

/// A module with no hashes sets and tests no bits, so the filter admits
/// everything while violating no membership property.
#[test]
fn rejects_empty_shapes() {
    assert_eq!(ModuleLayout::new(4, 1024, 0), Err(LayoutError::NoHashes));
    assert_eq!(ModuleLayout::new(0, 1024, 4), Err(LayoutError::NoModules));
    assert_eq!(ModuleLayout::new(4, 0, 4), Err(LayoutError::NoBits));
}

#[test]
fn rejects_impossible_false_positive_rates() {
    for rate in [0.0, 1.0, -0.5, 2.0, f64::NAN, f64::INFINITY] {
        assert!(
            matches!(
                ModuleLayout::for_keys(1000, rate, 4),
                Err(LayoutError::InvalidFalsePositiveRate { .. })
            ),
            "rate {rate} must be rejected"
        );
    }
}

/// A high target rate divided across many modules rounds the per-module hash
/// count toward zero. Clamping to one is what keeps the filter a filter.
#[test]
fn sizing_always_leaves_at_least_one_hash_per_module() {
    for (keys, rate, modules) in [
        (100_000u64, 0.5, 8u32),
        (100_000, 0.1, 16),
        (1_000, 0.9, 32),
        (10, 0.5, 64),
    ] {
        let layout = ModuleLayout::for_keys(keys, rate, modules).unwrap();
        assert!(
            layout.hashes_per_module() >= 1,
            "keys {keys} rate {rate} modules {modules} produced zero hashes per module"
        );
    }
}

/// Every layout `for_keys` returns must predict a rate at least as good as the
/// target it was asked for.
///
/// The rows with few keys are the ones that catch a sizing mistake. Rounding a
/// module's size down rather than up misses the target by up to a factor of
/// two when a module is only a word or two wide, and is invisible once a
/// module is large.
#[test]
fn sizing_meets_its_target_rate() {
    for (keys, target, modules) in [
        (10u64, 1e-2, 4u32),
        (100, 1e-3, 4),
        (100, 1e-2, 8),
        (1_000, 1e-3, 16),
        (1_000, 1e-2, 8),
        (10_000, 1e-2, 4),
        (100_000, 1e-4, 4),
        (1_000_000, 1e-6, 8),
    ] {
        let layout = ModuleLayout::for_keys(keys, target, modules)
            .unwrap_or_else(|e| panic!("keys {keys} target {target} modules {modules}: {e}"));

        let hashes = f64::from(layout.total_hashes());
        let bits = (layout.total_words() * 64) as f64;
        let fill = 1.0 - (-hashes * keys as f64 / bits).exp();
        let predicted = fill.powf(hashes);

        assert!(
            predicted <= target,
            "keys {keys} modules {modules}: predicted {predicted:e} exceeds target {target:e} \
             (bits/key {:.2}, hashes {hashes})",
            layout.bits_per_key(keys)
        );
    }
}

/// Feldera's default false positive rate is 1e-4, which is about 19 bits per
/// key. Every module count must land near that, or the ladder is being bought
/// with memory rather than with the rounding of the hash count.
#[test]
fn default_rate_costs_about_nineteen_bits_per_key() {
    for modules in [1u32, 2, 3, 4, 6, 8, 13, 16] {
        let keys = 1_000_000;
        let layout = ModuleLayout::for_keys(keys, 1e-4, modules).unwrap();
        let bits_per_key = layout.bits_per_key(keys);
        assert!(
            (18.5..20.5).contains(&bits_per_key),
            "modules {modules}: {bits_per_key:.2} bits/key is not near the 19.17 optimum \
             (hashes {})",
            layout.total_hashes()
        );
    }
}

#[test]
fn layout_arithmetic_is_consistent() {
    let layout = ModuleLayout::new(4, 1000, 3).unwrap();
    assert_eq!(layout.total_modules(), 4);
    assert_eq!(layout.words_per_module(), 1000);
    assert_eq!(layout.hashes_per_module(), 3);
    assert_eq!(layout.bits_per_module(), 64_000);
    assert_eq!(layout.bytes_per_module(), 8_000);
    assert_eq!(layout.total_words(), 4_000);
    assert_eq!(layout.total_bytes(), 32_000);
    assert_eq!(layout.total_hashes(), 12);
    assert_eq!(layout.bits_per_key(1_000), 256.0);
    assert_eq!(layout.bits_per_key(0), f64::INFINITY);

    let narrowed = layout.with_modules(2).unwrap();
    assert_eq!(narrowed.total_modules(), 2);
    assert_eq!(narrowed.words_per_module(), 1000);
    assert_eq!(narrowed.hashes_per_module(), 3);
}

/// A monolithic filter is a one-module layout, and its
/// hash count carries over unchanged.
#[test]
fn monolithic_layout_is_one_module() {
    let layout = ModuleLayout::monolithic(13, 29_954).unwrap();
    assert_eq!(layout.total_modules(), 1);
    assert_eq!(layout.words_per_module(), 29_954);
    assert_eq!(layout.hashes_per_module(), 13);
    assert_eq!(layout.total_hashes(), 13);

    assert_eq!(ModuleLayout::monolithic(0, 100), Err(LayoutError::NoHashes));
    assert_eq!(ModuleLayout::monolithic(13, 0), Err(LayoutError::NoBits));

    // A file claiming more words than a module is sized in is reported rather
    // than truncated: the loader turns this into no filter at all, which is
    // safe, while a silently narrowed module would misplace every bit.
    assert_eq!(
        ModuleLayout::monolithic(13, u32::MAX as usize + 1),
        Err(LayoutError::Overflow {
            modules: 1,
            words_per_module: u32::MAX,
        })
    );
}

#[test]
fn sizing_handles_a_key_count_of_zero() {
    let layout = ModuleLayout::for_keys(0, 1e-4, 4).unwrap();
    assert_eq!(layout.total_modules(), 4);
    assert!(layout.words_per_module() >= 1);
}
