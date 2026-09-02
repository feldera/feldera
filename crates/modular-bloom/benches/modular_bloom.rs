//! Modular Bloom filter built on top of the `fastbloom` crate.
//!
//! Every configuration targets Feldera's default false positive rate of 1e-4,
//! which is about 19 bits per key, so the variants differ in module count rather
//! than in memory.
//!
//! Absent and present keys are reported separately because they behave nothing
//! alike. An absent probe stops at the first module that rejects, so it touches
//! about two cache lines whatever the hash count; a present probe touches every
//! one. Feldera's `seek_key_exact` probes are overwhelmingly absent, so that is
//! the column that decides the design, but the present column is the worst case
//! and a regression there is still a regression.
//!
//! Two working-set sizes: one that fits in cache and one that does not. The
//! second is the honest one, because a filter for a real batch never fits.

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use fastbloom::BloomFilter;
use feldera_modular_bloom::{ModularBloomFilter, ModularBloomFilterBuilder, ModuleLayout};

const DEFAULT_FP: f64 = 1e-4;
/// Fits comfortably in cache: measures hashing, not memory.
const SMALL_KEYS: u64 = 1_000_000;
/// Exceeds any last-level cache: measures what production pays.
const LARGE_KEYS: u64 = 10_000_000;
const PROBES: u64 = 1 << 16;

// The same key streams the tests use, so a benchmark and a test that quote the
// same rate are measuring the same thing.
#[path = "../tests/bloom_common.rs"]
mod bloom_common;
use bloom_common::{absent, present};

/// The monolithic filter Feldera builds today, at the same target rate.
fn build_monolithic(keys: u64) -> BloomFilter {
    let mut filter = BloomFilter::with_false_pos(DEFAULT_FP)
        .seed(&42)
        .expected_items(keys as usize);
    for i in 0..keys {
        filter.insert_hash(present(i));
    }
    filter
}

fn build_modular(keys: u64, modules: u32) -> ModularBloomFilter {
    let layout = ModuleLayout::for_keys(keys, DEFAULT_FP, modules).unwrap();
    let mut builder = ModularBloomFilterBuilder::new(layout);
    for i in 0..keys {
        builder.insert_hash(present(i));
    }
    builder.finish()
}

/// Cost of building a filter, per key inserted.
fn construction(c: &mut Criterion) {
    let mut group = c.benchmark_group("construction");
    let keys = SMALL_KEYS;
    group.throughput(Throughput::Elements(keys));
    group.sample_size(10);

    group.bench_function("monolithic_k13", |b| {
        b.iter(|| black_box(build_monolithic(keys).num_bits()))
    });
    for modules in [1u32, 4, 8] {
        group.bench_with_input(
            BenchmarkId::new("modular", format!("d{modules}")),
            &modules,
            |b, &modules| b.iter(|| black_box(build_modular(keys, modules).resident_bytes())),
        );
    }
    group.finish();
}

/// Probe cost, split by whether the key is in the filter.
fn probe(c: &mut Criterion, keys: u64, label: &str) {
    let monolithic = build_monolithic(keys);
    let modular: Vec<(u32, ModularBloomFilter)> = [1u32, 4, 8]
        .into_iter()
        .map(|modules| (modules, build_modular(keys, modules)))
        .collect();

    for (case, hash) in [("absent", absent as fn(u64) -> u64), ("present", present)] {
        let mut group = c.benchmark_group(format!("probe_{case}_{label}"));
        group.throughput(Throughput::Elements(PROBES));

        group.bench_function("monolithic_k13", |b| {
            b.iter(|| {
                let mut hits = 0u64;
                for i in 0..PROBES {
                    hits += u64::from(monolithic.contains_hash(black_box(hash(i))));
                }
                hits
            })
        });

        for (modules, filter) in &modular {
            group.bench_with_input(
                BenchmarkId::new("modular", format!("d{modules}")),
                filter,
                |b, filter| {
                    b.iter(|| {
                        let mut hits = 0u64;
                        for i in 0..PROBES {
                            hits += u64::from(filter.contains_hash(black_box(hash(i))));
                        }
                        hits
                    })
                },
            );
        }
        group.finish();
    }
}

/// The truncation ladder: every rung's probe cost and the memory it holds.
fn ladder(c: &mut Criterion, keys: u64, label: &str) {
    let full = build_modular(keys, 4);

    for (case, hash) in [("absent", absent as fn(u64) -> u64), ("present", present)] {
        let mut group = c.benchmark_group(format!("ladder_{case}_{label}"));
        group.throughput(Throughput::Elements(PROBES));
        for resident in 1..=4u32 {
            let mut filter = full.clone();
            filter.truncate(resident);
            group.bench_with_input(
                BenchmarkId::from_parameter(format!(
                    "d4_resident{resident}_{}KiB",
                    filter.resident_bytes() / 1024
                )),
                &filter,
                |b, filter| {
                    b.iter(|| {
                        let mut hits = 0u64;
                        for i in 0..PROBES {
                            hits += u64::from(filter.contains_hash(black_box(hash(i))));
                        }
                        hits
                    })
                },
            );
        }
        group.finish();
    }
}

/// Mixed workloads, since real lookups are neither all absent nor all present.
fn mixed(c: &mut Criterion) {
    let keys = LARGE_KEYS;
    let monolithic = build_monolithic(keys);
    let modular = build_modular(keys, 4);

    let mut group = c.benchmark_group("probe_mixed_large");
    group.throughput(Throughput::Elements(PROBES));
    for hit_percent in [1u64, 10, 50] {
        let probe_hash = move |i: u64| {
            if i % 100 < hit_percent {
                present(i % keys)
            } else {
                absent(i)
            }
        };

        group.bench_with_input(
            BenchmarkId::new("monolithic_k13", format!("{hit_percent}pct_present")),
            &hit_percent,
            |b, _| {
                b.iter(|| {
                    let mut hits = 0u64;
                    for i in 0..PROBES {
                        hits += u64::from(monolithic.contains_hash(black_box(probe_hash(i))));
                    }
                    hits
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("modular_d4", format!("{hit_percent}pct_present")),
            &hit_percent,
            |b, _| {
                b.iter(|| {
                    let mut hits = 0u64;
                    for i in 0..PROBES {
                        hits += u64::from(modular.contains_hash(black_box(probe_hash(i))));
                    }
                    hits
                })
            },
        );
    }
    group.finish();
}

/// The operations that make the filter resizable, which a monolithic filter
/// cannot perform at all.
fn resize(c: &mut Criterion) {
    let keys = LARGE_KEYS;
    let layout = ModuleLayout::for_keys(keys, DEFAULT_FP, 4).unwrap();
    let full = build_modular(keys, 4);
    let words: Vec<u64> = full.modules().concat();
    let per = layout.words_per_module() as usize;

    let mut group = c.benchmark_group("resize_large");
    group.sample_size(20);

    group.bench_function("truncate_4_to_2", |b| {
        b.iter_batched(
            || full.clone(),
            |mut filter| {
                filter.truncate(2);
                black_box(filter.resident_bytes())
            },
            criterion::BatchSize::LargeInput,
        )
    });

    group.bench_function("load_prefix_2_of_4", |b| {
        b.iter(|| {
            let filter = ModularBloomFilter::from_modules(layout, &words[..2 * per]).unwrap();
            black_box(filter.resident_bytes())
        })
    });

    group.bench_function("load_all_4", |b| {
        b.iter(|| {
            let filter = ModularBloomFilter::from_modules(layout, &words).unwrap();
            black_box(filter.resident_bytes())
        })
    });

    group.bench_function("push_one_module", |b| {
        b.iter_batched(
            || {
                let mut filter = full.clone();
                filter.truncate(3);
                filter
            },
            |mut filter| {
                filter.push_module(&words[3 * per..4 * per]).unwrap();
                black_box(filter.resident_bytes())
            },
            criterion::BatchSize::LargeInput,
        )
    });

    group.finish();
}

fn all(c: &mut Criterion) {
    construction(c);
    probe(c, SMALL_KEYS, "small");
    probe(c, LARGE_KEYS, "large");
    ladder(c, LARGE_KEYS, "large");
    mixed(c);
    resize(c);
}

criterion_group!(benches, all);
criterion_main!(benches);
