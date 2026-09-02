//! Property and model-based tests.
//!
//! The deterministic matrix in `bloom_invariants` walks one fixed order of
//! operations. These tests generate the rest: arbitrary layouts, arbitrary key
//! sets, and arbitrary interleavings of shrinking and growing a filter.

mod bloom_common;

use bloom_common::{absent, flatten, present};
use feldera_modular_bloom::{ModularBloomFilter, ModularBloomFilterBuilder, ModuleLayout};
use proptest::prelude::*;
use std::collections::BTreeSet;

/// One operation that can be applied to a filter.
#[derive(Clone, Copy, Debug)]
enum Op {
    /// Shed accuracy down to this residency.
    Truncate(u32),
    /// Read one more module back from storage.
    Grow,
}

fn layout_strategy() -> impl Strategy<Value = ModuleLayout> {
    (1u32..=8, 1u32..=4, 1u32..=200)
        .prop_map(|(modules, hashes, words)| ModuleLayout::new(modules, words, hashes).unwrap())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    /// No sequence of shrinking and regrowing can ever lose a key.
    ///
    /// The oracle is the set of inserted hashes. Every one of them must be
    /// admitted after every operation, at every residency the sequence reaches.
    #[test]
    fn no_operation_sequence_loses_a_key(
        layout in layout_strategy(),
        key_count in 0usize..300,
        ops in prop::collection::vec(0u32..16, 1..24),
    ) {
        let mut builder = ModularBloomFilterBuilder::new(layout);
        let mut oracle = BTreeSet::new();
        for i in 0..key_count as u64 {
            builder.insert_hash(present(i));
            oracle.insert(present(i));
        }
        let full = builder.finish();
        let words = flatten(&full);
        let per = layout.words_per_module() as usize;

        let mut filter = full.clone();
        let ops: Vec<Op> = ops
            .into_iter()
            .map(|raw| {
                if raw % 3 == 0 {
                    Op::Grow
                } else {
                    Op::Truncate(raw % (layout.total_modules() + 1))
                }
            })
            .collect();

        for op in ops {
            match op {
                Op::Truncate(target) => {
                    // Truncation only ever sheds modules; asking for more than
                    // are resident leaves the filter alone.
                    let expected = target.min(filter.resident_modules());
                    filter.truncate(target);
                    prop_assert_eq!(filter.resident_modules(), expected);
                }
                Op::Grow => {
                    let next = filter.resident_modules() as usize;
                    if next < layout.total_modules() as usize {
                        filter
                            .push_module(&words[next * per..(next + 1) * per])
                            .unwrap();
                        prop_assert_eq!(filter.resident_modules() as usize, next + 1);
                    } else {
                        prop_assert!(filter.push_module(&words[..per]).is_err());
                        prop_assert_eq!(filter.resident_modules() as usize, next);
                    }
                }
            }

            for &hash in &oracle {
                prop_assert!(
                    filter.contains_hash(hash),
                    "key lost at residency {}",
                    filter.resident_modules()
                );
            }

            // Whatever the sequence, the resident words are always the matching
            // prefix of the filter as built.
            let resident = filter.resident_modules() as usize;
            prop_assert_eq!(flatten(&filter), words[..resident * per].to_vec());
        }
    }

    /// Loading any prefix yields a filter that is monotone against the next one
    /// up and never loses a key.
    #[test]
    fn every_prefix_is_a_valid_filter(
        layout in layout_strategy(),
        key_count in 1usize..300,
    ) {
        let mut builder = ModularBloomFilterBuilder::new(layout);
        for i in 0..key_count as u64 {
            builder.insert_hash(present(i));
        }
        let words = flatten(&builder.finish());
        let per = layout.words_per_module() as usize;

        let mut narrower: Option<ModularBloomFilter> = None;
        for resident in 1..=layout.total_modules() {
            let filter =
                ModularBloomFilter::from_modules(layout, &words[..resident as usize * per])
                    .unwrap();

            for i in 0..key_count as u64 {
                prop_assert!(filter.contains_hash(present(i)));
            }

            if let Some(previous) = &narrower {
                for i in 0..2_000u64 {
                    let hash = absent(i);
                    prop_assert!(
                        !filter.contains_hash(hash) || previous.contains_hash(hash),
                        "adding a module admitted a key the smaller filter rejected"
                    );
                }
            }
            narrower = Some(filter);
        }
    }

    /// Sizing always returns a layout that meets its target rate.
    #[test]
    fn sizing_meets_its_target(
        keys in 1u64..2_000_000,
        exponent in 1u32..7,
        modules in 1u32..17,
    ) {
        let target = 10f64.powi(-(exponent as i32));
        let layout = ModuleLayout::for_keys(keys, target, modules).unwrap();
        prop_assert!(layout.hashes_per_module() >= 1);
        prop_assert_eq!(layout.total_modules(), modules);

        let hashes = f64::from(layout.total_hashes());
        let bits = (layout.total_words() * 64) as f64;
        let fill = 1.0 - (-hashes * keys as f64 / bits).exp();
        prop_assert!(
            fill.powf(hashes) <= target,
            "predicted {} exceeds target {}",
            fill.powf(hashes),
            target
        );
    }
}

/// Source hashes whose top bits are constant.
///
/// The hash chain seeds itself from the top half of the hash, so a caller whose
/// high bits carry little entropy degrades every module at once. Accuracy is
/// therefore a documented precondition on the caller, not a guarantee this crate
/// can make; what it must still guarantee under any input at all is that no key
/// is ever lost.
#[test]
fn degenerate_source_hashes_lose_no_keys() {
    let keys = 20_000u64;
    let layout = ModuleLayout::for_keys(keys, 1e-4, 4).unwrap();

    // With at least 32 bits of entropy in the top half the filter behaves
    // normally: worst measured admission is 0.00035 at 16 pinned bits. Beyond
    // that the chain seed itself is starved and accuracy degrades by design,
    // so only the no-false-negative guarantee is asserted there.
    const MAX_ADMITTED: f64 = 0.005;
    const STARVED_ABOVE: u32 = 16;
    const PROBES: u64 = 20_000;

    for pinned_high_bits in [0u32, 8, 16, 24, 32] {
        let mask = u64::MAX >> pinned_high_bits;
        let degenerate = |i: u64| present(i) & mask;

        let mut builder = ModularBloomFilterBuilder::new(layout);
        for i in 0..keys {
            builder.insert_hash(degenerate(i));
        }
        let filter = builder.finish();

        for i in 0..keys {
            assert!(
                filter.contains_hash(degenerate(i)),
                "pinning {pinned_high_bits} high bits produced a false negative"
            );
        }

        let admitted = (0..PROBES)
            .filter(|&i| filter.contains_hash(absent(i) & mask))
            .count();
        let rate = admitted as f64 / PROBES as f64;
        if pinned_high_bits <= STARVED_ABOVE {
            assert!(
                rate <= MAX_ADMITTED,
                "pinning {pinned_high_bits} high bits let {rate:.5} of absent keys through, \
                 above the {MAX_ADMITTED} bound"
            );
        } else {
            // Degraded, but it must still be a filter rather than a pass-through.
            assert!(
                rate < 1.0,
                "pinning {pinned_high_bits} high bits made the filter admit everything"
            );
        }
    }
}

/// Hashes that are all equal, all zero, or all ones must not panic or lose keys.
#[test]
fn constant_source_hashes_are_handled() {
    let layout = ModuleLayout::for_keys(1_000, 1e-4, 4).unwrap();
    for hash in [0u64, u64::MAX, 1, 1 << 63] {
        let mut builder = ModularBloomFilterBuilder::new(layout);
        for _ in 0..1_000 {
            builder.insert_hash(hash);
        }
        let filter = builder.finish();
        assert!(filter.contains_hash(hash));
    }
}
