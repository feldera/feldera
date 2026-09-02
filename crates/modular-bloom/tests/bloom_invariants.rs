//! Deterministic invariants, checked at every residency of every configuration.
//!
//! Membership tests alone are close to worthless here: a filter whose modules
//! are correlated, whose probe stops after the first module, or whose accuracy
//! is reported wrongly still admits every key it was given. The tests that do
//! the work are the corruption probes, which damage a module and assert that
//! the filter then rejects the keys it was built from.

mod bloom_common;

use bloom_common::{
    CONFIGS, Config, absent, count_false_negatives, flatten, present, with_module_zeroed,
    with_modules_rotated,
};
use fastbloom::BloomFilter;
use feldera_modular_bloom::{LoadError, ModularBloomFilter};

const PROBES: u64 = 200_000;

/// Every inserted key is admitted at every residency, on the filter as built.
#[test]
fn no_false_negatives_at_every_residency() {
    for config in CONFIGS {
        let full = config.build();
        for resident in 1..=config.modules {
            let mut filter = full.clone();
            filter.truncate(resident);
            let missing = count_false_negatives(&filter, config.keys);
            assert_eq!(
                missing, 0,
                "{} at residency {resident}: {missing} false negatives",
                config.name
            );
        }
    }
}

/// The same, on filters that came back through `from_modules`.
///
/// A loader that misplaced a module boundary by one word would leave the
/// filter it built untouched, so the build-time test above still reports zero
/// false negatives while the reloaded filter misses most of its keys.
#[test]
fn no_false_negatives_after_loading_every_prefix() {
    for config in CONFIGS {
        let layout = config.layout();
        let words = config.words();
        let per = layout.words_per_module() as usize;

        for resident in 1..=config.modules {
            let filter =
                ModularBloomFilter::from_modules(layout, &words[..resident as usize * per])
                    .unwrap();
            let missing = count_false_negatives(&filter, config.keys);
            assert_eq!(
                missing, 0,
                "{} loaded at residency {resident}: {missing} false negatives",
                config.name
            );
        }
    }
}

/// Dropping a module can only ever admit more keys, never fewer. A filter that
/// composes its modules with the wrong operator violates this while still
/// admitting everything it contains.
#[test]
fn truncation_is_monotone() {
    for config in CONFIGS {
        let full = config.build();
        for resident in 1..config.modules {
            let mut wider = full.clone();
            wider.truncate(resident + 1);
            let mut narrower = full.clone();
            narrower.truncate(resident);

            let violations = (0..PROBES)
                .filter(|&i| {
                    let hash = absent(i);
                    wider.contains_hash(hash) && !narrower.contains_hash(hash)
                })
                .count();
            assert_eq!(
                violations,
                0,
                "{}: {violations} keys admitted at residency {} but rejected at {resident}",
                config.name,
                resident + 1
            );
        }
    }
}

/// Loading a prefix and truncating a full filter must reach the same state,
/// word for word and answer for answer. The storage path does the first and the
/// runtime does the second, so a divergence would appear only after a restart.
#[test]
fn loading_a_prefix_equals_truncating() {
    for config in CONFIGS {
        let layout = config.layout();
        let words = config.words();
        let per = layout.words_per_module() as usize;
        let full = config.build();

        for resident in 1..=config.modules {
            let mut truncated = full.clone();
            truncated.truncate(resident);
            let loaded =
                ModularBloomFilter::from_modules(layout, &words[..resident as usize * per])
                    .unwrap();

            assert_eq!(
                flatten(&truncated),
                flatten(&loaded),
                "{} at residency {resident}: words differ",
                config.name
            );
            assert_eq!(truncated.resident_modules(), loaded.resident_modules());
            for i in 0..PROBES {
                assert_eq!(
                    truncated.contains_hash(absent(i)),
                    loaded.contains_hash(absent(i)),
                    "{} at residency {resident}: answers differ on absent key {i}",
                    config.name
                );
            }
        }
    }
}

/// Module 0 must be byte-identical to a plain `fastbloom` filter over the same
/// hashes.
///
/// This is what lets a monolithic filter be read as a one-module modular one,
/// and no other test in the suite would fail if it stopped holding.
#[test]
fn module_zero_is_bit_identical_to_a_bare_fastbloom_filter() {
    for config in CONFIGS {
        let layout = config.layout();
        let mut bare = BloomFilter::from_vec(vec![0u64; layout.words_per_module() as usize])
            .seed(&0)
            .hashes(layout.hashes_per_module());
        for i in 0..config.keys {
            bare.insert_hash(present(i));
        }

        let filter = config.build();
        assert_eq!(
            filter.module(0).unwrap(),
            bare.as_slice(),
            "{}: module 0 diverges from a bare fastbloom filter",
            config.name
        );
    }
}

/// A monolithic filter's words load through the one-module layout and answer
/// exactly as `fastbloom` does, which is what lets pre-existing filters be read
/// without conversion.
#[test]
fn monolithic_words_load_and_answer_identically() {
    let words_per_module = 2_000;
    let hashes = 13;
    let keys = 20_000;

    let mut bare = BloomFilter::from_vec(vec![0u64; words_per_module])
        .seed(&0)
        .hashes(hashes);
    for i in 0..keys {
        bare.insert_hash(present(i));
    }

    let layout = feldera_modular_bloom::ModuleLayout::monolithic(hashes, words_per_module).unwrap();
    let loaded = ModularBloomFilter::from_modules(layout, bare.as_slice()).unwrap();

    for i in 0..keys {
        assert!(
            loaded.contains_hash(present(i)),
            "false negative on key {i}"
        );
    }
    for i in 0..PROBES {
        assert_eq!(
            loaded.contains_hash(absent(i)),
            bare.contains_hash(absent(i)),
            "answers differ on absent key {i}"
        );
    }
}

/// Pins the upstream behaviour the design depends on: a `fastbloom` seed does
/// not reach `insert_hash`/`contains_hash`, so it cannot be used to decorrelate
/// modules. If a future version makes seeds effective, this fails and tells us
/// the assumption has moved.
#[test]
fn fastbloom_seeds_do_not_affect_the_hash_api() {
    let mut a = BloomFilter::from_vec(vec![0u64; 512]).seed(&1).hashes(4);
    let mut b = BloomFilter::from_vec(vec![0u64; 512]).seed(&999).hashes(4);
    for i in 0..1_000u64 {
        a.insert_hash(present(i));
        b.insert_hash(present(i));
    }
    assert_eq!(
        a.as_slice(),
        b.as_slice(),
        "seeds now affect insert_hash, so the per-module mix may be redundant"
    );
}

/// A full round trip through the words a caller would persist.
#[test]
fn words_round_trip() {
    for config in CONFIGS {
        let filter = config.build();
        let words = flatten(&filter);
        let reloaded = ModularBloomFilter::from_modules(config.layout(), &words).unwrap();

        assert_eq!(flatten(&reloaded), words, "{}: words differ", config.name);
        assert_eq!(reloaded.resident_modules(), config.modules);
        assert_eq!(count_false_negatives(&reloaded, config.keys), 0);
    }
}

/// Corruption probe. Zeroing any single module must make the filter reject
/// every key it contains, which is only true if every module is actually
/// consulted. A probe that stops early passes every membership test but fails
/// this one outright.
#[test]
fn zeroing_any_module_rejects_every_key() {
    for config in CONFIGS {
        let layout = config.layout();
        let words = config.words();
        for zeroed in 0..config.modules {
            let damaged = with_module_zeroed(layout, &words, zeroed);
            let admitted = (0..config.keys)
                .filter(|&i| damaged.contains_hash(present(i)))
                .count();
            assert_eq!(
                admitted, 0,
                "{}: {admitted} keys still admitted with module {zeroed} zeroed",
                config.name
            );
        }
    }
}

/// Corruption probe. Rotating the modules leaves every bit in place but moves
/// each module to a different index. Independent modules then reject almost
/// everything; correlated modules do not notice.
#[test]
fn rotating_the_modules_rejects_almost_every_key() {
    for config in CONFIGS.iter().filter(|c| c.modules > 1) {
        let layout = config.layout();
        let words = config.words();
        let filter = config.build();

        let rotated = with_modules_rotated(layout, &words);
        let admitted = (0..config.keys)
            .filter(|&i| rotated.contains_hash(present(i)))
            .count() as f64;

        // Ten times the filter's own rate, plus an absolute floor so a very
        // accurate configuration is not held to a threshold below one key.
        let budget = 10.0 * filter.false_positive_rate() * config.keys as f64 + 5.0;
        assert!(
            admitted <= budget,
            "{}: {admitted} of {} keys survive rotation, budget {budget:.1}. \
             Modules are not independent.",
            config.name,
            config.keys
        );
    }
}

#[test]
fn modules_are_pairwise_distinct() {
    for config in CONFIGS.iter().filter(|c| c.modules > 1) {
        let filter = config.build();
        for a in 0..config.modules {
            for b in (a + 1)..config.modules {
                assert_ne!(
                    filter.module(a).unwrap(),
                    filter.module(b).unwrap(),
                    "{}: modules {a} and {b} are identical",
                    config.name
                );
            }
        }
    }
}

#[test]
fn residency_bookkeeping_is_exact() {
    for config in CONFIGS {
        let full = config.build();
        assert_eq!(full.resident_modules(), config.modules);
        assert_eq!(
            full.resident_bytes(),
            config.modules as usize * config.layout().bytes_per_module()
        );

        for target in 0..=config.modules {
            let mut filter = full.clone();
            filter.truncate(target);
            assert_eq!(
                filter.resident_modules(),
                target,
                "{}: truncate({target}) left {} modules",
                config.name,
                filter.resident_modules()
            );
        }

        // Truncating to at or above the current residency changes nothing.
        let mut filter = full.clone();
        filter.truncate(config.modules + 10);
        assert_eq!(filter.resident_modules(), config.modules);
        assert_eq!(flatten(&filter), flatten(&full));
    }
}

/// With no modules resident the filter admits everything, which is the answer a
/// batch with no filter gives.
#[test]
fn truncating_to_zero_admits_everything() {
    for config in CONFIGS {
        let mut filter = config.build();
        filter.truncate(0);
        assert_eq!(filter.resident_modules(), 0);
        assert_eq!(filter.resident_bytes(), 0);
        assert_eq!(filter.false_positive_rate(), 1.0);
        for i in 0..1_000 {
            assert!(filter.contains_hash(absent(i)));
            assert!(filter.contains_hash(present(i)));
        }
    }
}

/// Growing accuracy back must restore the original bits exactly, or a filter
/// that was shrunk and later reloaded answers differently from one that never
/// moved.
#[test]
fn truncate_then_push_module_restores_the_filter() {
    for config in CONFIGS {
        let layout = config.layout();
        let words = config.words();
        let per = layout.words_per_module() as usize;
        let full = config.build();

        let mut filter = full.clone();
        filter.truncate(1);
        for module in 1..config.modules {
            let start = module as usize * per;
            filter.push_module(&words[start..start + per]).unwrap();
            assert_eq!(filter.resident_modules(), module + 1);
        }

        assert_eq!(flatten(&filter), flatten(&full), "{}", config.name);
        for i in 0..PROBES {
            assert_eq!(
                filter.contains_hash(absent(i)),
                full.contains_hash(absent(i)),
                "{}: answers differ after regrowth",
                config.name
            );
        }
    }
}

/// Error paths are only reachable by a caller doing something wrong, so a
/// happy-path suite never exercises them and a missing bounds check survives.
#[test]
fn load_and_growth_errors_are_reported() {
    let config: &Config = &CONFIGS[0];
    let layout = config.layout();
    let words = config.words();
    let per = layout.words_per_module() as usize;

    assert_eq!(
        ModularBloomFilter::from_modules(layout, &words[..per - 1]).unwrap_err(),
        LoadError::PartialModule {
            words: per - 1,
            words_per_module: layout.words_per_module(),
        },
        "a partial module must be rejected"
    );

    let mut oversized = words.clone();
    oversized.extend_from_slice(&words[..per]);
    assert!(matches!(
        ModularBloomFilter::from_modules(layout, &oversized),
        Err(LoadError::TooManyModules { .. })
    ));

    let mut filter = config.build();
    assert!(matches!(
        filter.push_module(&words[..per]),
        Err(LoadError::TooManyModules { .. })
    ));
    assert_eq!(
        filter.resident_modules(),
        config.modules,
        "a rejected push must not change residency"
    );

    filter.truncate(1);
    assert!(matches!(
        filter.push_module(&words[..per - 1]),
        Err(LoadError::PartialModule { .. })
    ));
    assert_eq!(filter.resident_modules(), 1);
}

/// An empty filter rejects everything, and a filter over a single key admits
/// exactly that key.
#[test]
fn degenerate_key_counts() {
    let layout = feldera_modular_bloom::ModuleLayout::for_keys(1_000, 1e-4, 4).unwrap();

    let empty = bloom_common::build_with(layout, 0);
    let admitted = (0..10_000)
        .filter(|&i| empty.contains_hash(absent(i)))
        .count();
    assert_eq!(admitted, 0, "an empty filter must reject everything");

    let single = bloom_common::build_with(layout, 1);
    assert!(single.contains_hash(present(0)));
    let admitted = (0..10_000)
        .filter(|&i| single.contains_hash(absent(i)))
        .count();
    assert_eq!(admitted, 0, "a one-key filter admitted an absent key");
}

/// Repeating a key must not change the filter.
#[test]
fn duplicate_inserts_are_idempotent() {
    let layout = feldera_modular_bloom::ModuleLayout::for_keys(1_000, 1e-4, 4).unwrap();
    let once = bloom_common::build_with(layout, 1_000);

    let mut builder = feldera_modular_bloom::ModularBloomFilterBuilder::new(layout);
    for _ in 0..3 {
        for i in 0..1_000 {
            builder.insert_hash(present(i));
        }
    }
    assert_eq!(flatten(&builder.finish()), flatten(&once));
}
