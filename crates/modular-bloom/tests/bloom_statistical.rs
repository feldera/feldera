//! Accuracy checks, the tier that catches bugs producing no false negatives.
//!
//! Every band is Poisson on the observed count rather than a fixed relative
//! error on the rate. Sampling noise on a rare event is Poisson in the count, so
//! a relative band is far too tight when the expected count is small and far too
//! loose when it is large.

mod bloom_common;

use bloom_common::{
    CONFIGS, assert_within_band, build_with, count_false_negatives, count_false_positives, flatten,
};
use feldera_modular_bloom::{ModularBloomFilter, ModuleLayout};

const PROBES: u64 = 200_000;

/// Product of the per-module rates, which is what the filter actually does.
///
/// Raising a mean fill to a power instead is biased by Jensen's inequality and
/// reports a rate the filter does not have.
fn predicted_rate(filter: &ModularBloomFilter, resident: u32) -> f64 {
    let hashes = filter.layout().hashes_per_module() as i32;
    let bits = filter.layout().bits_per_module() as f64;
    (0..resident)
        .map(|m| {
            let set: u32 = filter
                .module(m)
                .expect("module must be resident")
                .iter()
                .map(|w| w.count_ones())
                .sum();
            (f64::from(set) / bits).powi(hashes)
        })
        .product()
}

/// The measured rate at every rung matches the rate the filter's own bits imply.
#[test]
fn measured_rate_matches_the_bits_at_every_rung() {
    for config in CONFIGS {
        let full = config.build();
        for resident in 1..=config.modules {
            let mut filter = full.clone();
            filter.truncate(resident);

            let observed = count_false_positives(&filter, PROBES) as f64;
            let expected = predicted_rate(&full, resident) * PROBES as f64;
            assert_within_band(
                observed,
                expected,
                0.05,
                &format!("{} at residency {resident}", config.name),
            );
        }
    }
}

/// The rate the API reports for a hypothetical residency matches what that
/// residency actually measures. A method that ignores its argument, or that
/// drops the hash count from the exponent, is caught only here.
#[test]
fn reported_rate_matches_measurement_at_every_rung() {
    for config in CONFIGS {
        let full = config.build();
        for resident in 1..=config.modules {
            let mut filter = full.clone();
            filter.truncate(resident);

            let observed = count_false_positives(&filter, PROBES) as f64;
            let reported = full.false_positive_rate_with(resident) * PROBES as f64;
            assert_within_band(
                observed,
                reported,
                0.10,
                &format!(
                    "{} reported vs measured at residency {resident}",
                    config.name
                ),
            );
        }
    }
}

/// A filter at reduced residency must report the rate its resident modules
/// give, not the one its layout was sized for.
///
/// A rate computed against the layout's full size reports correctly on a full
/// filter and wrongly on a truncated one.
#[test]
fn a_truncated_filter_reports_its_own_rate() {
    for config in CONFIGS {
        let layout = config.layout();
        let words = config.words();
        let per = layout.words_per_module() as usize;

        for resident in 1..=config.modules {
            let filter =
                ModularBloomFilter::from_modules(layout, &words[..resident as usize * per])
                    .unwrap();
            let observed = count_false_positives(&filter, PROBES) as f64;
            let reported = filter.false_positive_rate() * PROBES as f64;
            assert_within_band(
                observed,
                reported,
                0.10,
                &format!("{} self-reported at residency {resident}", config.name),
            );
        }
    }
}

/// Accuracy improves monotonically as modules are added.
#[test]
fn rate_is_monotone_in_residency() {
    for config in CONFIGS {
        let full = config.build();
        let mut previous = f64::INFINITY;
        for resident in 1..=config.modules {
            let mut filter = full.clone();
            filter.truncate(resident);
            let rate = count_false_positives(&filter, PROBES) as f64 / PROBES as f64;
            assert!(
                rate <= previous,
                "{}: rate rose from {previous:e} to {rate:e} when residency reached {resident}",
                config.name
            );
            previous = rate;
        }
    }
}

/// Modules must be independent, not merely distinct. If two modules set
/// overlapping bits more often than chance, their false positives correlate and
/// the ladder stops being geometric.
#[test]
fn modules_do_not_share_bits_beyond_chance() {
    for config in CONFIGS.iter().filter(|c| c.modules > 1) {
        let filter = config.build();
        let bits = filter.layout().bits_per_module() as f64;

        for a in 0..config.modules {
            for b in (a + 1)..config.modules {
                let wa = filter.module(a).unwrap();
                let wb = filter.module(b).unwrap();
                let set_a: u32 = wa.iter().map(|w| w.count_ones()).sum();
                let set_b: u32 = wb.iter().map(|w| w.count_ones()).sum();
                let both: u32 = wa
                    .iter()
                    .zip(wb.iter())
                    .map(|(x, y)| (x & y).count_ones())
                    .sum();

                let fill_a = f64::from(set_a) / bits;
                let fill_b = f64::from(set_b) / bits;
                let expected = fill_a * fill_b;
                let lift = f64::from(both) / bits / expected;
                let band = 1.0 + 5.0 * ((1.0 - expected) / (expected * bits)).sqrt();
                assert!(
                    lift <= band,
                    "{}: modules {a} and {b} overlap {lift:.4}x more than chance, band {band:.4}",
                    config.name
                );
            }
        }
    }
}

/// Every module carries the same load. A build loop that misses a module leaves
/// it empty, which shows here and essentially nowhere else.
#[test]
fn per_module_fill_is_even() {
    for config in CONFIGS.iter().filter(|c| c.modules > 1) {
        let filter = config.build();
        let bits = filter.layout().bits_per_module() as f64;
        let fills: Vec<f64> = (0..config.modules)
            .map(|m| {
                let set: u32 = filter
                    .module(m)
                    .unwrap()
                    .iter()
                    .map(|w| w.count_ones())
                    .sum();
                f64::from(set) / bits
            })
            .collect();

        let mean = fills.iter().sum::<f64>() / fills.len() as f64;
        let spread =
            fills.iter().cloned().fold(0.0, f64::max) - fills.iter().cloned().fold(1.0, f64::min);
        let band = 8.0 * (mean * (1.0 - mean) / bits).sqrt();
        assert!(
            spread <= band,
            "{}: module fill spans {spread:.5} around {mean:.4}, band {band:.5}: {fills:?}",
            config.name
        );
    }
}

/// Retuning lands on the smallest residency that still meets the target, and
/// the filter it leaves behind really does meet it.
#[test]
fn retune_meets_its_target_and_stops_there() {
    let target = 1e-2;
    for config in CONFIGS {
        let mut filter = config.build();
        let resident = filter.retune(target);

        assert!(resident >= 1, "{}: retune emptied the filter", config.name);
        assert_eq!(resident, filter.resident_modules());

        let measured = count_false_positives(&filter, PROBES) as f64 / PROBES as f64;
        assert!(
            measured <= 1.25 * target,
            "{}: retune left {resident} modules measuring {measured:e} against target {target:e}",
            config.name
        );

        if resident > 1 {
            let mut smaller = config.build();
            smaller.truncate(resident - 1);
            assert!(
                smaller.false_positive_rate() > target,
                "{}: {} modules would also have met the target, so retune kept one too many",
                config.name,
                resident - 1
            );
        }
    }
}

/// The headline ladder at Feldera's default rate: four modules, about 19 bits
/// per key, and one decade of accuracy per module.
#[test]
fn default_rate_ladder_costs_one_decade_per_module() {
    let keys = 200_000;
    let probes = 400_000;
    let layout = ModuleLayout::for_keys(keys, 1e-4, 4).unwrap();
    assert_eq!(layout.total_hashes(), 12);

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
        let observed = count_false_positives(&filter, probes) as f64;
        let target = expected[resident as usize - 1] * probes as f64;
        assert_within_band(
            observed,
            target,
            0.35,
            &format!("default ladder at residency {resident}"),
        );
    }
}

/// A filter truncated to `d` modules holds exactly the bits a filter built for
/// `d` modules would, because a module's content depends on its own index and
/// the keys, never on how many other modules exist.
#[test]
fn a_truncated_filter_equals_one_built_at_that_size() {
    for config in CONFIGS {
        let layout = config.layout();
        let full = config.build();

        for resident in 1..=config.modules {
            let mut truncated = full.clone();
            truncated.truncate(resident);

            let narrower = layout.with_modules(resident).unwrap();
            let purpose_built = build_with(narrower, config.keys);

            assert_eq!(
                flatten(&truncated),
                flatten(&purpose_built),
                "{} at residency {resident}: a truncated filter differs from a purpose-built one",
                config.name
            );
        }
    }
}

/// Fill is a property of the bits that are resident, not of the layout the
/// filter was built with.
///
/// Every module carries the same load, so dropping modules must leave the fill
/// unchanged. A fill divided by the full size instead falls off in proportion to
/// what was dropped, and every rate derived from it is then wrong on exactly the
/// filters the eviction path produces.
#[test]
fn fill_is_measured_over_resident_modules_only() {
    for config in CONFIGS {
        let full = config.build();
        let bits = full.layout().bits_per_module() as f64;

        let full_fill = full.fill_ratio();
        let expected: f64 = {
            let set: u32 = (0..config.modules)
                .map(|m| {
                    full.module(m)
                        .unwrap()
                        .iter()
                        .map(|w| w.count_ones())
                        .sum::<u32>()
                })
                .sum();
            f64::from(set) / (bits * f64::from(config.modules))
        };
        assert!(
            (full_fill - expected).abs() < 1e-12,
            "{}: fill_ratio {full_fill} does not match the resident bits {expected}",
            config.name
        );

        for resident in 1..config.modules {
            let mut filter = full.clone();
            filter.truncate(resident);
            let fill = filter.fill_ratio();
            assert!(
                (fill - full_fill).abs() < 0.02,
                "{}: fill fell from {full_fill:.4} to {fill:.4} at residency {resident}, \
                 so it is being divided by the full size",
                config.name
            );
        }

        let mut empty = full.clone();
        empty.truncate(0);
        assert_eq!(empty.fill_ratio(), 0.0);
    }
}

/// A filter that holds no modules at all can still say how many it would need
/// for a given rate, if the density recorded at build time comes with it.
///
/// This is what lets a reader size its read before making it: it has the
/// layout and the density from the file's metadata, and no bits yet.
#[test]
fn residency_can_be_chosen_before_any_module_is_read() {
    for config in CONFIGS {
        let layout = config.layout();
        let full = config.build();
        let words = flatten(&full);
        let density = full.density().clone();
        let per = layout.words_per_module() as usize;

        // What a reader has before it has read anything.
        let unread =
            ModularBloomFilter::from_modules_with_density(layout, &[], density.clone()).unwrap();
        assert_eq!(unread.resident_modules(), 0);

        for target in [1e-1, 1e-2, 1e-3, 1e-4] {
            let wanted = unread.modules_for(target);
            assert_eq!(
                wanted,
                full.modules_for(target),
                "{}: an unread filter disagreed with a fully resident one at {target:e}",
                config.name
            );
            if wanted == 0 {
                continue;
            }

            // Read exactly that prefix and check the rate it really delivers.
            let loaded = ModularBloomFilter::from_modules_with_density(
                layout,
                &words[..wanted as usize * per],
                density.clone(),
            )
            .unwrap();
            assert_eq!(loaded.resident_modules(), wanted);
            assert_eq!(count_false_negatives(&loaded, config.keys), 0);

            let observed = count_false_positives(&loaded, PROBES) as f64;
            let predicted = unread.false_positive_rate_with(wanted) * PROBES as f64;
            assert_within_band(
                observed,
                predicted,
                0.10,
                &format!("{} at {target:e}, {wanted} modules read", config.name),
            );
        }
    }
}

/// Truncating keeps the recorded density of the dropped modules, so a shrunk
/// filter can still report what regrowing would buy it.
#[test]
fn a_truncated_filter_still_knows_its_full_ladder() {
    for config in CONFIGS {
        let full = config.build();
        let rates: Vec<f64> = (0..=config.modules)
            .map(|m| full.false_positive_rate_with(m))
            .collect();

        let mut shrunk = full.clone();
        shrunk.truncate(1);
        for modules in 0..=config.modules {
            assert_eq!(
                shrunk.false_positive_rate_with(modules),
                rates[modules as usize],
                "{}: rate for {modules} modules changed after truncating",
                config.name
            );
        }
    }
}
