//! Tests at the key counts production workloads actually reach.
//!
//! They matter because everything else in the suite runs on filters that fit in
//! cache and are far below the size at which `fastbloom`'s bit index overflows.
//!
//! All but one run in under a second on tens of megabytes, so they run with the
//! rest of the suite. `fifty_million_keys_default_rate` is ignored, being an
//! order of magnitude more of both. Run it with:
//!
//! ```text
//! cargo test -p feldera-modular-bloom --release --test bloom_large -- --ignored --nocapture
//! ```
//!
//! Run them in release: the set takes about two seconds there and twenty in
//! a debug build.

mod bloom_common;

use bloom_common::{absent, build_with, count_false_negatives, count_false_positives, flatten};
use feldera_modular_bloom::{ModularBloomFilter, ModuleLayout};

/// Feldera's default false positive rate.
const DEFAULT_FP: f64 = 1e-4;

/// Ten million keys at the default rate, the whole ladder.
#[test]
fn ten_million_keys_default_rate() {
    let keys = 10_000_000u64;
    let probes = 2_000_000u64;
    let layout = ModuleLayout::for_keys(keys, DEFAULT_FP, 4).unwrap();

    let bits_per_key = layout.bits_per_key(keys);
    assert!(
        (19.0..19.5).contains(&bits_per_key),
        "{bits_per_key:.2} bits/key is not the expected 19.23"
    );

    let full = build_with(layout, keys);
    let expected = [1e-1, 1e-2, 1e-3, 1e-4];

    for resident in 1..=4u32 {
        let mut filter = full.clone();
        filter.truncate(resident);

        assert_eq!(
            count_false_negatives(&filter, keys),
            0,
            "residency {resident} lost a key"
        );

        let observed = count_false_positives(&filter, probes) as f64 / probes as f64;
        let target = expected[resident as usize - 1];
        assert!(
            observed <= 2.0 * target,
            "residency {resident}: measured {observed:e} exceeds twice the target {target:e}"
        );
    }
}

/// Fifty million keys, the scale at which a single batch's filter starts
/// to dominate resident memory.
#[test]
#[ignore = "allocates ~115 MiB and runs for ~12 seconds"]
fn fifty_million_keys_default_rate() {
    let keys = 50_000_000u64;
    let probes = 2_000_000u64;
    let layout = ModuleLayout::for_keys(keys, DEFAULT_FP, 4).unwrap();

    let full = build_with(layout, keys);
    assert_eq!(count_false_negatives(&full, keys), 0);

    for resident in [4u32, 2, 1] {
        let mut filter = full.clone();
        filter.truncate(resident);
        assert_eq!(count_false_negatives(&filter, keys), 0);

        let observed = count_false_positives(&filter, probes) as f64 / probes as f64;
        let target = 10f64.powi(-(resident as i32));
        assert!(
            observed <= 2.0 * target,
            "residency {resident}: measured {observed:e} exceeds twice the \
             target {target:e}"
        );
    }
}

/// A large filter must survive the persist-and-reload path, not just the build.
#[test]
fn large_filter_round_trips_through_words() {
    let keys = 10_000_000u64;
    let layout = ModuleLayout::for_keys(keys, DEFAULT_FP, 4).unwrap();
    let full = build_with(layout, keys);
    let words = flatten(&full);
    let per = layout.words_per_module() as usize;

    let reloaded = ModularBloomFilter::from_modules(layout, &words).unwrap();
    assert_eq!(flatten(&reloaded), words);
    assert_eq!(count_false_negatives(&reloaded, keys), 0);

    // A reduced-accuracy load, which is how a filter comes back on a smaller node.
    let half = ModularBloomFilter::from_modules(layout, &words[..2 * per]).unwrap();
    assert_eq!(count_false_negatives(&half, keys), 0);
    assert_eq!(half.resident_bytes(), 2 * layout.bytes_per_module());

    let mut truncated = full.clone();
    truncated.truncate(2);
    assert_eq!(flatten(&truncated), flatten(&half));
}

/// Every bit of a large module is reachable.
///
/// `fastbloom` 0.14 reduces a hash with a 64-bit multiply by the module's bit
/// count, which overflows once that count passes 2^32, so the upper part of a
/// filter for more than about 224 million keys is never addressed (its issue
/// 22). This reduction multiplies by the word count instead, keeping both
/// factors below 2^32, and this asserts the reach that buys.
#[test]
fn every_bit_of_a_large_module_is_reachable() {
    let keys = 400_000_000u64;

    for modules in [1u32, 4, 16] {
        let layout = ModuleLayout::for_keys(keys, DEFAULT_FP, modules).unwrap();

        // Probe the reduction directly rather than building a 900 MiB filter:
        // the highest bit any hash reaches must be near the top of the module.
        let bits = layout.bits_per_module();
        let highest = (0..500_000u64)
            .map(|i| {
                let hash = crate::bloom_common::mix64(i);
                ((u128::from(hash >> 32) * u128::from(bits)) >> 32) as u64
            })
            .max()
            .unwrap();
        assert!(
            highest > bits - bits / 1000,
            "{modules} modules: reduction only reached {highest} of {bits} bits"
        );
    }
}

/// Retuning a large filter against a budget behaves as it does at small scale.
#[test]
fn retune_at_scale() {
    let keys = 10_000_000u64;
    let probes = 1_000_000u64;
    let layout = ModuleLayout::for_keys(keys, DEFAULT_FP, 4).unwrap();
    let mut filter = build_with(layout, keys);
    let before = filter.resident_bytes();

    let resident = filter.retune(1e-2);
    assert!(
        resident < layout.total_modules(),
        "retune to a coarser target kept every module"
    );
    assert_eq!(resident, filter.resident_modules());

    assert_eq!(count_false_negatives(&filter, keys), 0);
    let observed = count_false_positives(&filter, probes) as f64 / probes as f64;
    assert!(
        observed <= 1.25e-2,
        "retune left a filter measuring {observed:e} against a 1e-2 target"
    );
    assert!(filter.resident_bytes() < before);
}

/// A sanity check that absent keys really are absent at this scale, so the
/// false positive numbers above measure what they claim to.
#[test]
fn absent_keys_are_disjoint_from_present_keys_at_scale() {
    let keys = 10_000_000u64;
    let layout = ModuleLayout::for_keys(keys, DEFAULT_FP, 4).unwrap();
    let filter = build_with(layout, keys);

    // Every probe hash is odd-indexed under a bijection, so it cannot equal any
    // inserted hash. The measured rate must therefore track the target rather
    // than being inflated by accidental membership.
    let observed = count_false_positives(&filter, 5_000_000) as f64 / 5_000_000.0;
    assert!(
        observed <= 2.0 * DEFAULT_FP,
        "measured {observed:e} against target {DEFAULT_FP:e}"
    );
    let _ = absent(0);
}
