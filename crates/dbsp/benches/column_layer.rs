use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use dbsp::{
    algebra::{AddAssignByRef, AddByRef, MonoidValue, NegByRef},
    trace::{
        consolidation::consolidate,
        layers::{
            column_layer::ColumnLayer,
            erased::{TypedErasedKeyLeaf, TypedErasedLeaf},
            Builder, MergeBuilder, Trie, TupleBuilder,
        },
    },
};
use pprof::criterion::{Output, PProfProfiler};
use rand::{distributions::Standard, prelude::Distribution, Rng, SeedableRng};
use rand_xoshiro::Xoshiro256StarStar;

const SEED: [u8; 32] = [
    0x7f, 0xc3, 0x59, 0x18, 0x45, 0x19, 0xc0, 0xaa, 0xd2, 0xec, 0x31, 0x26, 0xbb, 0x74, 0x2f, 0x8b,
    0x11, 0x7d, 0xc, 0xe4, 0x64, 0xbf, 0x72, 0x17, 0x46, 0x28, 0x46, 0x42, 0xb2, 0x4b, 0x72, 0x18,
];

fn data_leaf<K, R, L>(length: usize) -> L
where
    K: Ord + Clone,
    R: MonoidValue,
    L: Trie<Item = (K, R)>,
    Standard: Distribution<(K, R)>,
{
    let mut rng = Xoshiro256StarStar::from_seed(SEED);
    let mut data = Vec::with_capacity(length);

    for _ in 0..length {
        data.push(rng.gen());
    }
    consolidate(&mut data);

    let mut builder = <L::TupleBuilder>::with_capacity(length);
    builder.extend_tuples(data.into_iter());
    builder.done()
}

fn data_leaves<K, R, L>(length: usize) -> (L, L)
where
    K: Ord + Clone,
    R: MonoidValue,
    L: Trie<Item = (K, R)>,
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
        <L::TupleBuilder>::with_capacity(length / 2),
        <L::TupleBuilder>::with_capacity(length / 2),
    );

    left_builder.extend_tuples(left.into_iter());
    right_builder.extend_tuples(right.into_iter());

    (left_builder.done(), right_builder.done())
}

macro_rules! leaf_benches {
    ($($name:literal = [$layer:ident]$size:literal),* $(,)?) => {
        fn merge_ordered_column_leaf_builder(c: &mut Criterion) {
            let mut group = c.benchmark_group("ordered-builder-push-merge");
            $(
                group.bench_function($name, |b| {
                    let (left, right) = data_leaves::<usize, isize, $layer<_,_>>($size);

                    b.iter_batched(
                        || (left.cursor(), right.cursor()),
                        |(left, right)| {
                            let mut builder = <$layer<_,_> as Trie>::MergeBuilder::new();
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
                    let (left, right) = data_leaves::<usize, isize, $layer<_,_>>($size);

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
                    let (left, right) = data_leaves::<usize, isize, $layer<_,_>>($size);

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
                    let (left, right) = data_leaves::<usize, isize, $layer<_,_>>($size);

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
                    let (left, right) = data_leaves::<usize, isize, $layer<_,_>>($size);

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
                    let leaf = data_leaf::<usize, isize, $layer<usize, isize>>($size);

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
                    let leaf = data_leaf::<usize, isize, $layer<usize, isize>>($size);

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
    "0-static" = [ColumnLayer]0,
    "0-erased" = [TypedErasedLeaf]0,
    "0-erased-key" = [TypedErasedKeyLeaf]0,
    "10-static" = [ColumnLayer]10,
    "10-erased" = [TypedErasedLeaf]10,
    "10-erased-key" = [TypedErasedKeyLeaf]10,
    "100-static" = [ColumnLayer]100,
    "100-erased" = [TypedErasedLeaf]100,
    "100-erased-key" = [TypedErasedKeyLeaf]100,
    "1000-static" = [ColumnLayer]1000,
    "1000-erased" = [TypedErasedLeaf]1000,
    "1000-erased-key" = [TypedErasedKeyLeaf]1000,
    "10,000-static" = [ColumnLayer]10_000,
    "10,000-erased" = [TypedErasedLeaf]10_000,
    "10,000-erased-key" = [TypedErasedKeyLeaf]10_000,
    "100,000-static" = [ColumnLayer]100_000,
    "100,000-erased" = [TypedErasedLeaf]100_000,
    "100,000-erased-key" = [TypedErasedKeyLeaf]100_000,
    "1,000,000-static" = [ColumnLayer]1_000_000,
    "1,000,000-erased" = [TypedErasedLeaf]1_000_000,
    "1,000,000-erased-key" = [TypedErasedKeyLeaf]1_000_000,
    "10,000,000-static" = [ColumnLayer]10_000_000,
    "10,000,000-erased" = [TypedErasedLeaf]10_000_000,
    "10,000,000-erased-key" = [TypedErasedKeyLeaf]10_000_000,
    "100,000,000-static" = [ColumnLayer]100_000_000,
    "100,000,000-erased" = [TypedErasedLeaf]100_000_000,
    "100,000,000-erased-key" = [TypedErasedKeyLeaf]100_000_000,
}

criterion_group!(
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(300, Output::Flamegraph(None)));
    targets = column_leaf, merge_ordered_column_leaf_builder
);
criterion_main!(benches);
