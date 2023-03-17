use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use dbsp::{
    algebra::{AddAssignByRef, AddByRef, MonoidValue, NegByRef},
    trace::{
        consolidation::consolidate,
        layers::{
            column_layer::{ColumnLayer, ColumnLayerBuilder},
            Builder, MergeBuilder, Trie, TupleBuilder,
        },
    },
};
use rand::{distributions::Standard, prelude::Distribution, Rng, SeedableRng};
use rand_xoshiro::Xoshiro256StarStar;

const SEED: [u8; 32] = [
    0x7f, 0xc3, 0x59, 0x18, 0x45, 0x19, 0xc0, 0xaa, 0xd2, 0xec, 0x31, 0x26, 0xbb, 0x74, 0x2f, 0x8b,
    0x11, 0x7d, 0xc, 0xe4, 0x64, 0xbf, 0x72, 0x17, 0x46, 0x28, 0x46, 0x42, 0xb2, 0x4b, 0x72, 0x18,
];

fn data_leaf<K, R>(length: usize) -> ColumnLayer<K, R>
where
    K: Ord + Clone,
    R: MonoidValue,
    Standard: Distribution<(K, R)>,
{
    let mut rng = Xoshiro256StarStar::from_seed(SEED);
    let mut data = Vec::with_capacity(length);

    for _ in 0..length {
        data.push(rng.gen());
    }
    consolidate(&mut data);

    let mut builder = <ColumnLayerBuilder<K, R> as TupleBuilder>::with_capacity(length);
    builder.extend_tuples(data.into_iter());
    builder.done()
}

fn data_leaves<K, R>(length: usize) -> (ColumnLayer<K, R>, ColumnLayer<K, R>)
where
    K: Ord + Clone,
    R: MonoidValue,
    Standard: Distribution<(K, R)>,
{
    let mut rng = Xoshiro256StarStar::from_seed(SEED);
    let mut left = Vec::with_capacity(length / 2);
    let mut right = Vec::with_capacity(length / 2);

    for _ in 0..length / 2 {
        left.push(rng.gen());
        right.push(rng.gen());
    }

    consolidate(&mut left);
    consolidate(&mut right);

    let (mut right_builder, mut left_builder) = (
        <ColumnLayerBuilder<K, R> as TupleBuilder>::with_capacity(length / 2),
        <ColumnLayerBuilder<K, R> as TupleBuilder>::with_capacity(length / 2),
    );

    left_builder.extend_tuples(left.into_iter());
    right_builder.extend_tuples(right.into_iter());

    (left_builder.done(), right_builder.done())
}

macro_rules! leaf_benches {
    ($($name:literal = $size:literal),* $(,)?) => {
        fn merge_ordered_column_leaf_builder(c: &mut Criterion) {
            let mut group = c.benchmark_group("ordered-builder-push-merge");
            $(
                group.bench_function($name, |b| {
                    let (left, right) = data_leaves::<usize, isize>($size);

                    b.iter_batched(
                        || (left.cursor(), right.cursor()),
                        |(left, right)| {
                            let mut builder = ColumnLayerBuilder::new();
                            builder.push_merge(left, right);
                        },
                        BatchSize::PerIteration,
                    );
                });
            )*
            group.finish();
        }

        fn column_leaf(c: &mut Criterion) {
            let mut group = c.benchmark_group("add");
            group.sample_size(10);
            $(
                group.bench_function($name, |b| {
                    let (left, right) = data_leaves::<usize, isize>($size);

                    b.iter_batched(
                        || (left.clone(), right.clone()),
                        |(left, right)| left + right,
                        BatchSize::PerIteration,
                    );
                });
            )*
            group.finish();

            let mut group = c.benchmark_group("add-by-ref");
            group.sample_size(10);
            $(
                group.bench_function($name, |b| {
                    let (left, right) = data_leaves::<usize, isize>($size);

                    b.iter_batched(
                        || (&left, &right),
                        |(left, right)| left.add_by_ref(right),
                        BatchSize::PerIteration,
                    );
                });
            )*
            group.finish();

            let mut group = c.benchmark_group("add-assign");
            group.sample_size(10);
            $(
                group.bench_function($name, |b| {
                    let (left, right) = data_leaves::<usize, isize>($size);

                    b.iter_batched(
                        || (left.clone(), right.clone()),
                        |(mut left, right)| left += right,
                        BatchSize::PerIteration,
                    );
                });
            )*
            group.finish();

            let mut group = c.benchmark_group("add-assign-by-ref");
            group.sample_size(10);
            $(
                group.bench_function($name, |b| {
                    let (left, right) = data_leaves::<usize, isize>($size);

                    b.iter_batched(
                        || (left.clone(), &right),
                        |(mut left, right)| left.add_assign_by_ref(right),
                        BatchSize::PerIteration,
                    );
                });
            )*
            group.finish();

            let mut group = c.benchmark_group("neg");
            group.sample_size(10);
            $(
                group.bench_function($name, |b| {
                    let leaf = data_leaf::<usize, isize>($size);

                    b.iter_batched(
                        || leaf.clone(),
                        |leaf| -leaf,
                        BatchSize::PerIteration,
                    );
                });
            )*
            group.finish();

            let mut group = c.benchmark_group("neg-by-ref");
            group.sample_size(10);
            $(
                group.bench_function($name, |b| {
                    let leaf = data_leaf::<usize, isize>($size);

                    b.iter_batched(
                        || &leaf,
                        |left| left.neg_by_ref(),
                        BatchSize::PerIteration,
                    );
                });
            )*
            group.finish();
        }
    };
}

leaf_benches! {
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

criterion_group!(benches, column_leaf, merge_ordered_column_leaf_builder,);
criterion_main!(benches);
