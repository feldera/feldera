use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use dbsp::{
    algebra::{AddAssignByRef, AddByRef, MonoidValue, NegByRef},
    trace::layers::{
        column_leaf::{OrderedColumnLeaf, OrderedColumnLeafBuilder, UnorderedColumnLeafBuilder},
        Builder, MergeBuilder, Trie, TupleBuilder,
    },
};
use rand::{distributions::Standard, prelude::Distribution, seq::SliceRandom, Rng, SeedableRng};
use rand_xoshiro::Xoshiro256StarStar;

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

fn data_builder<K, R>(length: usize) -> UnorderedColumnLeafBuilder<K, R>
where
    K: Ord + Clone,
    R: MonoidValue,
    Standard: Distribution<(K, R)>,
{
    let mut rng = Xoshiro256StarStar::from_seed(SEED);

    let mut builder = UnorderedColumnLeafBuilder::new();
    for _ in 0..length {
        builder.push_tuple(rng.gen());
    }
    builder
}

fn data_leaf<K, R>(length: usize) -> OrderedColumnLeaf<K, R>
where
    K: Ord + Clone,
    R: MonoidValue,
    Standard: Distribution<(K, R)>,
{
    let mut rng = Xoshiro256StarStar::from_seed(SEED);

    let mut builder = UnorderedColumnLeafBuilder::with_capacity(length);
    for _ in 0..length {
        builder.push_tuple(rng.gen());
    }
    builder.done()
}

fn data_leaves<K, R>(length: usize) -> (OrderedColumnLeaf<K, R>, OrderedColumnLeaf<K, R>)
where
    K: Ord + Clone,
    R: MonoidValue,
    Standard: Distribution<(K, R)>,
{
    let mut rng = Xoshiro256StarStar::from_seed(SEED);

    let (mut right, mut left) = (
        UnorderedColumnLeafBuilder::with_capacity(length / 2),
        UnorderedColumnLeafBuilder::with_capacity(length / 2),
    );
    for _ in 0..length / 2 {
        left.push_tuple(rng.gen());
        right.push_tuple(rng.gen());
    }

    (left.done(), right.done())
}

macro_rules! leaf_benches {
    ($($name:literal = $size:literal),* $(,)?) => {
        fn unordered_column_leaf_builder(c: &mut Criterion) {
            let mut group = c.benchmark_group("build-unordered-pushing");
            $(
                group.bench_function($name, |b| {
                    let unsorted = data::<(usize, isize)>($size);

                    b.iter_batched(
                        || unsorted.clone(),
                        |unsorted| {
                            let mut builder = UnorderedColumnLeafBuilder::new();
                            for tuple in unsorted {
                                builder.push_tuple(tuple);
                            }
                        },
                        BatchSize::PerIteration,
                    );
                });
            )*
            group.finish();


            let mut group = c.benchmark_group("build-unordered-boundary");
            group.sample_size(10);
            $(
                group.bench_function($name, |b| {
                    let builder = data_builder::<usize, isize>($size);

                    b.iter_batched(
                        || builder.clone(),
                        |builder| black_box(builder).boundary(),
                        BatchSize::PerIteration,
                    );
                });
            )*
            group.finish();

            let mut group = c.benchmark_group("build-unordered-done");
            group.sample_size(10);
            $(
                group.bench_function($name, |b| {
                    let builder = data_builder::<usize, isize>($size);

                    b.iter_batched(
                        || builder.clone(),
                        |builder| black_box(builder).done(),
                        BatchSize::PerIteration,
                    );
                });
            )*
            group.finish();
        }

        fn merge_ordered_column_leaf_builder(c: &mut Criterion) {
            let mut group = c.benchmark_group("ordered-builder-push-merge");
            $(
                group.bench_function($name, |b| {
                    let (left, right) = data_leaves::<usize, isize>($size);

                    b.iter_batched(
                        || (left.cursor(), right.cursor()),
                        |(left, right)| {
                            let mut builder = OrderedColumnLeafBuilder::new();
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

criterion_group!(
    benches,
    column_leaf,
    unordered_column_leaf_builder,
    merge_ordered_column_leaf_builder,
);
criterion_main!(benches);
