// TODO: adapt these benchmarks for dynamic dispatch-based implementation.
fn main() {
    todo!()
}
/*
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use dbsp::algebra::{AddAssignByRef, HasZero};
use rand::{
    distributions::Standard,
    prelude::{Distribution, SliceRandom},
    Rng, SeedableRng,
};
use rand_xoshiro::Xoshiro256StarStar;
use std::{mem::replace, ops::AddAssign};

/// The seed for our prng-generated benchmarks
const SEED: [u8; 32] = [
    0x7f, 0xc3, 0x59, 0x18, 0x45, 0x19, 0xc0, 0xaa, 0xd2, 0xec, 0x31, 0x26, 0xbb, 0x74, 0x2f, 0x8b,
    0x11, 0x7d, 0xc, 0xe4, 0x64, 0xbf, 0x72, 0x17, 0x46, 0x28, 0x46, 0x42, 0xb2, 0x4b, 0x72, 0x18,
];

fn data<T>(length: usize) -> Vec<T>
where
    Standard: Distribution<T>,
{
    let mut rng = Xoshiro256StarStar::from_seed(SEED);

    let mut data: Vec<T> = (0..length).map(|_| rng.gen()).collect();
    data.shuffle(&mut rng);
    data
}

// Consolidation using stable sorting
fn consolidate_slice_stable<T, D>(slice: &mut [(T, D)]) -> usize
where
    T: Ord,
    D: AddAssignByRef + HasZero,
{
    slice.sort_by(|(key1, _), (key2, _)| key1.cmp(key2));
    consolidation::consolidate_slice_inner(
        slice,
        |(key1, _), (key2, _)| key1 == key2,
        |(_, diff1), (_, diff2)| diff1.add_assign_by_ref(diff2),
        |(_, diff)| diff.is_zero(),
    )
}

// Consolidation using stable sorting and native std functions
fn consolidate_naive_stable<T, D>(slice: &mut Vec<(T, D)>)
where
    T: Ord,
    D: AddAssign + HasZero,
{
    slice.sort_by(|(key1, _), (key2, _)| key1.cmp(key2));
    slice.dedup_by(|(key1, data1), (key2, data2)| {
        if key1 == key2 {
            data2.add_assign(replace(data1, D::zero()));
            true
        } else {
            false
        }
    });
    slice.retain(|(_, data)| !data.is_zero());
}

// Consolidation using unstable sorting and native std functions
fn consolidate_naive_unstable<T, D>(slice: &mut Vec<(T, D)>)
where
    T: Ord,
    D: AddAssign + HasZero,
{
    slice.sort_unstable_by(|(key1, _), (key2, _)| key1.cmp(key2));
    slice.dedup_by(|(key1, data1), (key2, data2)| {
        if key1 == key2 {
            data2.add_assign(replace(data1, D::zero()));
            true
        } else {
            false
        }
    });
    slice.retain(|(_, data)| !data.is_zero());
}

macro_rules! consolidation_benches {
    ($($name:literal = $size:literal),* $(,)?) => {
        fn consolidation_benches(c: &mut Criterion) {
            let mut group = c.benchmark_group("consolidate-unstable");
            $(
                group.bench_function($name, |b| {
                    let unsorted = data::<((usize, usize), isize)>($size);

                    b.iter_batched(
                        || unsorted.clone(),
                        |mut unsorted| consolidation::consolidate_slice(black_box(&mut unsorted)),
                        BatchSize::PerIteration,
                    );
                });
            )*
            group.finish();

            let mut group = c.benchmark_group("consolidate-stable");
            $(
                group.bench_function($name, |b| {
                    let unsorted = data::<((usize, usize), isize)>($size);

                    b.iter_batched(
                        || unsorted.clone(),
                        |mut unsorted| consolidate_slice_stable(black_box(&mut unsorted)),
                        BatchSize::PerIteration,
                    );
                });
            )*
            group.finish();

            let mut group = c.benchmark_group("consolidate-naive-unstable");
            $(
                group.bench_function($name, |b| {
                    let unsorted = data::<((usize, usize), isize)>($size);

                    b.iter_batched(
                        || unsorted.clone(),
                        |mut unsorted| consolidate_naive_unstable(black_box(&mut unsorted)),
                        BatchSize::PerIteration,
                    );
                });
            )*
            group.finish();

            let mut group = c.benchmark_group("consolidate-naive-stable");
            $(
                group.bench_function($name, |b| {
                    let unsorted = data::<((usize, usize), isize)>($size);

                    b.iter_batched(
                        || unsorted.clone(),
                        |mut unsorted| consolidate_naive_stable(black_box(&mut unsorted)),
                        BatchSize::PerIteration,
                    );
                });
            )*
            group.finish();

            let mut group = c.benchmark_group("unstable-sort");
            $(
                group.bench_function($name, |b| {
                    let unsorted = data::<((usize, usize), isize)>($size);

                    b.iter_batched(
                        || unsorted.clone(),
                        |mut unsorted| black_box(&mut unsorted).sort_unstable_by(|(key1, _), (key2, _)| key1.cmp(key2)),
                        BatchSize::PerIteration,
                    );
                });
            )*
            group.finish();

            let mut group = c.benchmark_group("stable-sort");
            $(
                group.bench_function($name, |b| {
                    let unsorted = data::<((usize, usize), isize)>($size);

                    b.iter_batched(
                        || unsorted.clone(),
                        |mut unsorted| black_box(&mut unsorted).sort_by(|(key1, _), (key2, _)| key1.cmp(key2)),
                        BatchSize::PerIteration,
                    );
                });
            )*
            group.finish();

            let mut group = c.benchmark_group("consolidate-paired-vecs");
            group.sample_size(10);
            $(
                group.bench_function($name, |b| {
                    let mut rng = Xoshiro256StarStar::from_seed(SEED);

                    let mut keys: Vec<usize> = (0..$size).map(|_| rng.gen()).collect();
                    keys.shuffle(&mut rng);
                    let mut diffs: Vec<isize> = (0..$size).map(|_| rng.gen()).collect();
                    diffs.shuffle(&mut rng);

                    b.iter_batched(
                        || (keys.clone(), diffs.clone()),
                        |(mut keys, mut diffs)| {
                            consolidation::consolidate_payload_from(&mut keys, &mut diffs, 0);
                        },
                        BatchSize::PerIteration,
                    );
                });
            )*
            group.finish();
        }
    };
}

consolidation_benches! {
    "0" = 0,
    "10" = 10,
    "100" = 100,
    "1000" = 1000,
    "10,000" = 10_000,
    "100,000" = 100_000,
    "1,000,000" = 1_000_000,
    "10,000,000" = 10_000_000,
    "100,000,000" = 100_000_000,
}

criterion_group!(benches, consolidation_benches);
criterion_main!(benches);
*/
