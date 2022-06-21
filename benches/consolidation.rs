use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use dbsp::trace::consolidation::consolidate;
use rand::{prelude::SliceRandom, Rng, SeedableRng};
use rand_xoshiro::Xoshiro256StarStar;

/// The seed for our prng-generated benchmarks
const SEED: [u8; 32] = [
    0x7f, 0xc3, 0x59, 0x18, 0x45, 0x19, 0xc0, 0xaa, 0xd2, 0xec, 0x31, 0x26, 0xbb, 0x74, 0x2f, 0x8b,
    0x11, 0x7d, 0xc, 0xe4, 0x64, 0xbf, 0x72, 0x17, 0x46, 0x28, 0x46, 0x42, 0xb2, 0x4b, 0x72, 0x18,
];

fn consolidation_benches(c: &mut Criterion) {
    let mut rng = Xoshiro256StarStar::from_seed(SEED);

    c.benchmark_group("consolidate")
        .bench_function("0", |b| {
            let mut empty = Vec::<((usize, usize), isize)>::new();
            b.iter(|| consolidate(black_box(&mut empty)));
        })
        .bench_function("10", |b| {
            let mut unsorted: Vec<((usize, usize), isize)> = (0..10).map(|_| rng.gen()).collect();
            unsorted.shuffle(&mut rng);

            b.iter_batched(
                || unsorted.clone(),
                |mut unsorted| consolidate(black_box(&mut unsorted)),
                BatchSize::SmallInput,
            );
        })
        .bench_function("100", |b| {
            let mut unsorted: Vec<((usize, usize), isize)> = (0..100).map(|_| rng.gen()).collect();
            unsorted.shuffle(&mut rng);

            b.iter_batched(
                || unsorted.clone(),
                |mut unsorted| consolidate(black_box(&mut unsorted)),
                BatchSize::SmallInput,
            );
        })
        .bench_function("1,000", |b| {
            let mut unsorted: Vec<((usize, usize), isize)> = (0..1000).map(|_| rng.gen()).collect();
            unsorted.shuffle(&mut rng);

            b.iter_batched(
                || unsorted.clone(),
                |mut unsorted| consolidate(black_box(&mut unsorted)),
                BatchSize::SmallInput,
            );
        })
        .bench_function("10,000", |b| {
            let mut unsorted: Vec<((usize, usize), isize)> =
                (0..10_000).map(|_| rng.gen()).collect();
            unsorted.shuffle(&mut rng);

            b.iter_batched(
                || unsorted.clone(),
                |mut unsorted| consolidate(black_box(&mut unsorted)),
                BatchSize::SmallInput,
            );
        })
        .bench_function("100,000", |b| {
            let mut unsorted: Vec<((usize, usize), isize)> =
                (0..100_000).map(|_| rng.gen()).collect();
            unsorted.shuffle(&mut rng);

            b.iter_batched(
                || unsorted.clone(),
                |mut unsorted| consolidate(black_box(&mut unsorted)),
                BatchSize::SmallInput,
            );
        })
        .bench_function("1,000,000", |b| {
            let mut unsorted: Vec<((usize, usize), isize)> =
                (0..1_000_000).map(|_| rng.gen()).collect();
            unsorted.shuffle(&mut rng);

            b.iter_batched(
                || unsorted.clone(),
                |mut unsorted| consolidate(black_box(&mut unsorted)),
                BatchSize::SmallInput,
            );
        })
        .bench_function("10,000,000", |b| {
            let mut unsorted: Vec<((usize, usize), isize)> =
                (0..10_000_000).map(|_| rng.gen()).collect();
            unsorted.shuffle(&mut rng);

            b.iter_batched(
                || unsorted.clone(),
                |mut unsorted| consolidate(black_box(&mut unsorted)),
                BatchSize::SmallInput,
            );
        })
        .bench_function("100,000,000", |b| {
            let mut unsorted: Vec<((usize, usize), isize)> =
                (0..100_000_000).map(|_| rng.gen()).collect();
            unsorted.shuffle(&mut rng);

            b.iter_batched(
                || unsorted.clone(),
                |mut unsorted| consolidate(black_box(&mut unsorted)),
                BatchSize::SmallInput,
            );
        });
}

criterion_group!(benches, consolidation_benches);
criterion_main!(benches);
