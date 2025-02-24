use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use dbsp::dynamic::{DataTraitTyped, DynDataTyped, Erase, LeanVec, WeightTraitTyped};
use dbsp::utils::Tup2;
use dbsp::{
    algebra::{AddAssignByRef, AddByRef, NegByRef},
    trace::{
        layers::{Builder, Leaf, MergeBuilder, TupleBuilder},
        Trie,
    },
    utils::consolidate,
    DBData, DBWeight, DynZWeight, ZWeight,
};
use pprof::criterion::{Output, PProfProfiler};
use rand::{distributions::Standard, prelude::Distribution, Rng, SeedableRng};
use rand_xoshiro::Xoshiro256StarStar;

const SEED: [u8; 32] = [
    0x7f, 0xc3, 0x59, 0x18, 0x45, 0x19, 0xc0, 0xaa, 0xd2, 0xec, 0x31, 0x26, 0xbb, 0x74, 0x2f, 0x8b,
    0x11, 0x7d, 0xc, 0xe4, 0x64, 0xbf, 0x72, 0x17, 0x46, 0x28, 0x46, 0x42, 0xb2, 0x4b, 0x72, 0x18,
];

fn data_leaf<K: DataTraitTyped + ?Sized, R: WeightTraitTyped + ?Sized, L>(
    factories: &L::Factories,
    length: usize,
) -> L
where
    K::Type: DBData + Erase<K>,
    R::Type: DBWeight + Erase<R>,
    L: for<'a> Trie<Item<'a> = (&'a mut K, &'a mut R)>,
    Standard: Distribution<(K::Type, R::Type)>,
{
    let mut rng = Xoshiro256StarStar::from_seed(SEED);
    let mut data = LeanVec::with_capacity(length);

    for _ in 0..length {
        let (k, w) = rng.gen();
        data.push(Tup2::new(k, w));
    }
    consolidate(&mut data);

    let data = Vec::from(data);

    let mut builder = <L::TupleBuilder>::with_capacity(factories, length);

    for t in data.into_iter() {
        let (mut k, mut r) = t.into();
        builder.push_tuple((k.erase_mut(), r.erase_mut()))
    }

    builder.done()
}

fn data_leaves<K: DataTraitTyped + ?Sized, R: DataTraitTyped + ?Sized, L>(
    factories: &L::Factories,
    length: usize,
) -> (L, L)
where
    K::Type: DBData + Erase<K>,
    R::Type: DBWeight + Erase<R>,
    L: for<'a> Trie<Item<'a> = (&'a mut K, &'a mut R)>,
    Standard: Distribution<(K::Type, R::Type)>,
{
    let mut rng = Xoshiro256StarStar::from_seed(SEED);
    let mut left = LeanVec::with_capacity(length / 2);
    let mut right = LeanVec::with_capacity(length / 2);

    for _ in 0..length / 2 {
        let (k, w) = rng.gen();
        left.push(Tup2::new(k, w));

        let (k, w) = rng.gen();
        right.push(Tup2::new(k, w));
    }

    consolidate(&mut left);
    consolidate(&mut right);

    let left = Vec::from(left);
    let right = Vec::from(right);

    let (mut right_builder, mut left_builder) = (
        <L::TupleBuilder>::with_capacity(factories, length / 2),
        <L::TupleBuilder>::with_capacity(factories, length / 2),
    );

    for t in left.into_iter() {
        let (mut k, mut r) = t.into();
        left_builder.push_tuple((k.erase_mut(), r.erase_mut()))
    }

    for t in right.into_iter() {
        let (mut k, mut r) = t.into();
        right_builder.push_tuple((k.erase_mut(), r.erase_mut()))
    }

    (left_builder.done(), right_builder.done())
}

macro_rules! leaf_benches {
    ($($name:literal = [$layer:ident]$size:literal),* $(,)?) => {
        fn merge_ordered_column_leaf_builder(c: &mut Criterion) {
            let mut group = c.benchmark_group("ordered-builder-push-merge");
            $(
                group.bench_function($name, |b| {
                    let factories = <$layer<_,_> as Trie>::Factories::new::<u64, ZWeight>();
                    let (left, right) = data_leaves::<DynDataTyped<u64>, DynZWeight, $layer<_,_>>(&factories, $size);

                    b.iter_batched(
                        || (left.cursor(), right.cursor()),
                        |(left, right)| {
                            let mut builder = <$layer<_,_> as Trie>::MergeBuilder::new(&factories);
                            builder.push_merge(left, right, None);
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
                    let factories = <$layer<_,_> as Trie>::Factories::new::<u64, ZWeight>();

                    let (left, right) = data_leaves::<DynDataTyped<u64>, DynZWeight, $layer<_,_>>(&factories, $size);

                    b.iter_batched(
                        || (left.clone(), right.clone()),
                        |(left, right)| left.add_by_ref(&right),
                        BatchSize::PerIteration,
                    );
                });
            )*
            group.finish();

            let mut group = c.benchmark_group("add-by-ref");
            group.sample_size(10);
            $(
                group.bench_function($name, |b| {
                    let factories = <$layer<_,_> as Trie>::Factories::new::<u64, ZWeight>();

                    let (left, right) = data_leaves::<DynDataTyped<u64>, DynZWeight, $layer<_,_>>(&factories, $size);

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
                    let factories = <$layer<_,_> as Trie>::Factories::new::<u64, ZWeight>();

                    let (left, right) = data_leaves::<DynDataTyped<u64>, DynZWeight, $layer<_,_>>(&factories, $size);

                    b.iter_batched(
                        || (left.clone(), right.clone()),
                        |(mut left, right)| left.add_assign_by_ref(&right),
                        BatchSize::PerIteration,
                    );
                });
            )*
            group.finish();

            let mut group = c.benchmark_group("add-assign-by-ref");
            group.sample_size(10);
            $(
                group.bench_function($name, |b| {
                    let factories = <$layer<_,_> as Trie>::Factories::new::<u64, ZWeight>();

                    let (left, right) = data_leaves::<DynDataTyped<u64>, DynZWeight, $layer<_,_>>(&factories, $size);

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
                    let factories = <$layer<_,_> as Trie>::Factories::new::<u64, ZWeight>();

                    let leaf = data_leaf::<DynDataTyped<u64>, DynZWeight, $layer<DynDataTyped<u64>, DynZWeight>>(&factories, $size);

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
                    let factories = <$layer<_,_> as Trie>::Factories::new::<u64, ZWeight>();

                    let leaf = data_leaf::<DynDataTyped<u64>, DynZWeight, $layer<DynDataTyped<u64>, DynZWeight>>(&factories, $size);

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
    "0-erased" = [Leaf]0,
    "10-erased" = [Leaf]10,
    "100-erased" = [Leaf]100,
    "1000-erased" = [Leaf]1000,
    "10,000-erased" = [Leaf]10_000,
    "100,000-erased" = [Leaf]100_000,
    "1,000,000-erased" = [Leaf]1_000_000,
    "10,000,000-erased" = [Leaf]10_000_000,
    "100,000,000-erased" = [Leaf]100_000_000,
}

criterion_group!(
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(300, Output::Flamegraph(None)));
    targets = column_leaf, merge_ordered_column_leaf_builder
);
criterion_main!(benches);
