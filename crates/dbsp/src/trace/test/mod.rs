use std::{
    cmp::max,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use proptest::{collection::vec, prelude::*, strategy::BoxedStrategy};
use size_of::SizeOf;

use crate::{
    algebra::{
        IndexedZSet, OrdIndexedZSet, OrdIndexedZSetFactories, OrdZSet, OrdZSetFactories, ZBatch,
        ZSet,
    },
    circuit::mkconfig,
    dynamic::{pair::DynPair, DowncastTrait, DynData, DynUnit, DynWeightedPairs, Erase, LeanVec},
    trace::{
        cursor::CursorPair,
        ord::{
            FallbackIndexedWSet, FallbackIndexedWSetFactories, FallbackWSet, FallbackWSetFactories,
            FileKeyBatch, FileKeyBatchFactories, FileValBatch, FileValBatchFactories, OrdKeyBatch,
            OrdKeyBatchFactories, OrdValBatch, OrdValBatchFactories,
        },
        test::test_batch::{
            assert_batch_cursors_eq, assert_batch_eq, assert_trace_eq, test_batch_sampling,
            test_trace_sampling, TestBatch, TestBatchFactories,
        },
        Batch, BatchReader, BatchReaderFactories, Builder, Spine, Trace,
    },
    utils::{Tup2, Tup3, Tup4},
    DynZWeight, Runtime, ZWeight,
};

use super::Filter;
use itertools::Itertools;

pub mod test_batch;

type DynI32 = DynData;

fn kvtr_batch(
    max_key: i32,
    max_val: i32,
    max_time: u32,
    max_weight: ZWeight,
    max_tuples: usize,
) -> BoxedStrategy<Vec<Tup4<i32, i32, u32, ZWeight>>> {
    vec(
        (0..max_key, 0..max_val, 0..max_time, -max_weight..max_weight)
            .prop_map(|(k, v, t, r)| Tup4(k, v, t, r)),
        max_tuples,
    )
    .boxed()
}

fn ktr_batch(
    max_key: i32,
    max_time: u32,
    max_weight: ZWeight,
    max_tuples: usize,
) -> BoxedStrategy<Vec<Tup3<i32, u32, ZWeight>>> {
    vec(
        (0..max_key, 0..max_time, -max_weight..max_weight).prop_map(|(k, t, r)| Tup3(k, t, r)),
        max_tuples,
    )
    .boxed()
}

fn kr_batches(
    max_key: i32,
    max_weight: ZWeight,
    max_tuples: usize,
    max_batches: usize,
) -> BoxedStrategy<Vec<(Vec<Tup2<i32, ZWeight>>, i32)>> {
    vec(
        (
            vec(
                (0..max_key, -max_weight..max_weight).prop_map(|(x, y)| Tup2(x, y)),
                0..max_tuples,
            ),
            (0..max_key),
        ),
        0..max_batches,
    )
    .boxed()
}

fn kvr_batches(
    max_key: i32,
    max_val: i32,
    max_weight: ZWeight,
    max_tuples: usize,
    max_batches: usize,
) -> BoxedStrategy<Vec<(Vec<Tup2<Tup2<i32, i32>, ZWeight>>, i32, i32)>> {
    vec(
        (
            vec(
                (
                    (0..max_key, 0..max_val).prop_map(|(x, y)| Tup2(x, y)),
                    -max_weight..max_weight,
                )
                    .prop_map(|(x, y)| Tup2(x, y)),
                0..max_tuples,
            ),
            (0..max_key),
            (0..max_val),
        ),
        0..max_batches,
    )
    .boxed()
}

fn kvr_batches_monotone_keys(
    window_size: i32,
    window_step: i32,
    max_value: i32,
    max_tuples: usize,
    batches: usize,
) -> BoxedStrategy<Vec<Vec<Tup2<Tup2<i32, i32>, ZWeight>>>> {
    (0..batches)
        .map(|i| {
            vec(
                (
                    (
                        i as i32 * window_step..i as i32 * window_step + window_size,
                        0..max_value,
                    )
                        .prop_map(|(x, y)| Tup2(x, y)),
                    1..2i64,
                )
                    .prop_map(|(x, y)| Tup2(x, y)),
                0..max_tuples,
            )
        })
        .collect::<Vec<_>>()
        .boxed()
}

fn kvr_batches_monotone_values(
    max_key: i32,
    window_size: i32,
    window_step: i32,
    max_tuples: usize,
    batches: usize,
) -> BoxedStrategy<Vec<Vec<Tup2<Tup2<i32, i32>, ZWeight>>>> {
    (0..batches)
        .map(|i| {
            vec(
                (
                    (
                        0..max_key,
                        i as i32 * window_step..i as i32 * window_step + window_size,
                    )
                        .prop_map(|(x, y)| Tup2(x, y)),
                    1..2i64,
                )
                    .prop_map(|(x, y)| Tup2(x, y)),
                0..max_tuples,
            )
        })
        .collect::<Vec<_>>()
        .boxed()
}

fn indexed_zset_tuples(
    tuples: Vec<Tup2<Tup2<i32, i32>, ZWeight>>,
) -> Box<DynWeightedPairs<DynPair<DynI32, DynI32>, DynZWeight>> {
    Box::new(LeanVec::from(tuples)).erase_box()
}

pub fn zset_tuples(
    tuples: Vec<Tup2<i32, ZWeight>>,
) -> Box<DynWeightedPairs<DynPair<DynI32, DynUnit>, DynZWeight>> {
    Box::new(LeanVec::from(
        tuples
            .into_iter()
            .map(|Tup2(k, w)| Tup2(Tup2(k, ()), w))
            .collect::<Vec<_>>(),
    ))
    .erase_box()
}

fn test_zset_spine<B: ZSet<Key = DynI32>>(
    factories: &B::Factories,
    batches: Vec<(Vec<Tup2<i32, ZWeight>>, i32)>,
    seed: u64,
) {
    let mut trace: Spine<B> = Spine::new(factories);

    let mut ref_trace: TestBatch<DynI32, DynUnit /* <()> */, (), DynZWeight> =
        TestBatch::new(&TestBatchFactories::new());

    let mut kbound = 0;
    for (tuples, bound) in batches.into_iter() {
        let mut erased_tuples = zset_tuples(tuples.clone());

        let batch = B::dyn_from_tuples(factories, (), &mut erased_tuples.clone());
        let ref_batch: TestBatch<DynData, DynUnit, (), DynZWeight> =
            TestBatch::dyn_from_tuples(&TestBatchFactories::new(), (), &mut erased_tuples);

        test_batch_sampling(&batch);

        assert_batch_eq(&batch, &ref_batch);

        ref_trace.insert(ref_batch);
        assert_batch_cursors_eq(
            CursorPair::new(&mut batch.cursor(), &mut trace.cursor()),
            &ref_trace,
            seed,
        );

        trace.insert(batch);
        test_trace_sampling(&trace);

        assert_trace_eq(&trace, &ref_trace);

        kbound = max(kbound, bound);
        trace.retain_keys(Filter::new(Box::new(move |key| {
            *key.downcast_checked::<i32>() >= kbound
        })));
        ref_trace.retain_keys(Filter::new(Box::new(move |key| {
            *key.downcast_checked::<i32>() >= kbound
        })));

        test_trace_sampling(&trace);

        assert_trace_eq(&trace, &ref_trace);
    }
}

fn test_indexed_zset_spine<B: IndexedZSet<Key = DynI32, Val = DynI32>>(
    factories: &B::Factories,
    batches: Vec<(Vec<Tup2<Tup2<i32, i32>, ZWeight>>, i32, i32)>,
    seed: u64,
) {
    let mut trace: Spine<B> = Spine::new(factories);

    let mut ref_trace: TestBatch<DynI32, DynI32, (), DynZWeight> =
        TestBatch::new(&TestBatchFactories::new());

    let mut bound = 0;
    let mut kbound = 0;
    for (tuples, key_bound, val_bound) in batches.into_iter() {
        let mut erased_tuples = indexed_zset_tuples(tuples);

        let batch = B::dyn_from_tuples(factories, (), &mut erased_tuples.clone());
        let ref_batch =
            TestBatch::dyn_from_tuples(&TestBatchFactories::new(), (), &mut erased_tuples);

        test_batch_sampling(&batch);

        assert_batch_eq(&batch, &ref_batch);

        assert_batch_cursors_eq(batch.cursor(), &ref_batch, seed);

        ref_trace.insert(ref_batch);
        assert_batch_cursors_eq(
            CursorPair::new(&mut batch.cursor(), &mut trace.cursor()),
            &ref_trace,
            seed,
        );

        trace.insert(batch);
        test_trace_sampling(&trace);

        assert_trace_eq(&trace, &ref_trace);
        assert_batch_cursors_eq(trace.cursor(), &ref_trace, seed);

        kbound = max(kbound, key_bound);
        trace.retain_keys(Filter::new(Box::new(move |key| {
            *key.downcast_checked::<i32>() >= kbound
        })));

        ref_trace.retain_keys(Filter::new(Box::new(move |key| {
            *key.downcast_checked::<i32>() >= kbound
        })));
        test_trace_sampling(&trace);

        bound = max(bound, val_bound);
        trace.retain_values(Filter::new(Box::new(move |val| {
            *val.downcast_checked::<i32>() >= bound
        })));
        ref_trace.retain_values(Filter::new(Box::new(move |val| {
            *val.downcast_checked::<i32>() >= bound
        })));
        test_trace_sampling(&trace);

        assert_trace_eq(&trace, &ref_trace);
        assert_batch_cursors_eq(trace.cursor(), &ref_trace, seed);
    }
}

fn test_indexed_zset_trace_spine<B: ZBatch<Key = DynI32, Val = DynI32, Time = u32>>(
    factories: &B::Factories,
    batches: Vec<(Vec<Tup2<Tup2<i32, i32>, ZWeight>>, i32, i32)>,
    seed: u64,
) {
    // `trace1` uses `truncate_keys_below`.
    // `trace2` uses `retain_keys`.
    let mut trace: Spine<B> = Spine::new(factories);
    let mut ref_trace: TestBatch<DynI32, DynI32, u32, DynZWeight> =
        TestBatch::new(&TestBatchFactories::new());

    let mut bound = 0;
    let mut kbound = 0;
    for (time, (tuples, key_bound, val_bound)) in batches.into_iter().enumerate() {
        let mut erased_tuples = indexed_zset_tuples(tuples);

        let batch = B::dyn_from_tuples(factories, time as u32, &mut erased_tuples.clone());
        let ref_batch =
            TestBatch::dyn_from_tuples(&TestBatchFactories::new(), time as u32, &mut erased_tuples);

        assert_batch_eq(&batch, &ref_batch);
        assert_batch_cursors_eq(batch.cursor(), &ref_batch, seed);

        ref_trace.insert(ref_batch);
        assert_batch_cursors_eq(
            CursorPair::new(&mut trace.cursor(), &mut batch.cursor()),
            &ref_trace,
            seed,
        );

        trace.insert(batch);

        assert_trace_eq(&trace, &ref_trace);
        assert_batch_cursors_eq(trace.cursor(), &ref_trace, seed);

        kbound = max(kbound, key_bound);
        trace.retain_keys(Filter::new(Box::new(move |key| {
            *key.downcast_checked::<i32>() >= kbound
        })));
        ref_trace.retain_keys(Filter::new(Box::new(move |key| {
            *key.downcast_checked::<i32>() >= kbound
        })));

        bound = max(bound, val_bound);
        trace.retain_values(Filter::new(Box::new(move |val| {
            *val.downcast_checked::<i32>() >= bound
        })));
        ref_trace.retain_values(Filter::new(Box::new(move |val| {
            *val.downcast_checked::<i32>() >= bound
        })));

        assert_trace_eq(&trace, &ref_trace);
        assert_batch_cursors_eq(trace.cursor(), &ref_trace, seed);
    }
}

fn timed_indexed_batch_from_tuples<B>(
    factories: &B::Factories,
    tuples: &[Tup4<i32, i32, u32, ZWeight>],
) -> B
where
    B: ZBatch<Key = DynI32, Val = DynI32, Time = u32>,
{
    let mut builder = B::Builder::with_capacity(factories, tuples.len());
    #[allow(clippy::into_iter_on_ref)]
    for (key, vtds) in &tuples.into_iter().chunk_by(|Tup4(key, _, _, _)| key) {
        for (val, tds) in &vtds.into_iter().chunk_by(|Tup4(_, val, _, _)| val) {
            for Tup4(_, _, time, diff) in tds {
                builder.push_time_diff(time, diff);
            }
            builder.push_val(val);
        }
        builder.push_key(key);
    }
    builder.done()
}

fn test_indexed_zset_trace_builder<B>(
    factories: &B::Factories,
    mut tuples: Vec<Tup4<i32, i32, u32, ZWeight>>,
    seed: u64,
) where
    B: ZBatch<Key = DynI32, Val = DynI32, Time = u32>,
{
    tuples.sort_unstable();
    tuples.retain(|Tup4(_k, _v, _t, r)| *r != 0);
    tuples.dedup_by_key(|Tup4(k, v, t, _r)| (*k, *v, *t));

    let ref_batch = timed_indexed_batch_from_tuples::<TestBatch<DynI32, DynI32, u32, DynZWeight>>(
        &TestBatchFactories::new(),
        &tuples,
    );

    let batch = timed_indexed_batch_from_tuples::<B>(factories, &tuples);

    assert_batch_eq(&batch, &ref_batch);
    assert_batch_cursors_eq(batch.cursor(), &ref_batch, seed);
}

fn timed_batch_from_tuples<B>(factories: &B::Factories, tuples: &[Tup3<i32, u32, ZWeight>]) -> B
where
    B: ZBatch<Key = DynI32, Val = DynUnit, Time = u32>,
{
    let mut builder = B::Builder::with_capacity(factories, tuples.len());
    #[allow(clippy::into_iter_on_ref)]
    for (key, tds) in &tuples.into_iter().chunk_by(|Tup3(key, _time, _diff)| key) {
        for Tup3(_key, time, diff) in tds {
            builder.push_time_diff(time, diff);
        }
        builder.push_val(&());
        builder.push_key(key);
    }
    builder.done()
}

fn test_zset_trace_builder<B>(
    factories: &B::Factories,
    mut tuples: Vec<Tup3<i32, u32, ZWeight>>,
    seed: u64,
) where
    B: ZBatch<Key = DynI32, Val = DynUnit, Time = u32>,
{
    tuples.sort_unstable();
    tuples.retain(|Tup3(_k, _t, r)| *r != 0);
    tuples.dedup_by_key(|Tup3(k, t, _r)| (*k, *t));

    let ref_batch = timed_batch_from_tuples::<TestBatch<DynI32, DynUnit, u32, DynZWeight>>(
        &TestBatchFactories::new(),
        &tuples,
    );

    let batch = timed_batch_from_tuples::<B>(factories, &tuples);

    assert_batch_eq(&batch, &ref_batch);
    assert_batch_cursors_eq(batch.cursor(), &ref_batch, seed);
}

fn test_zset_trace_spine<B: ZBatch<Key = DynI32, Val = DynUnit, Time = u32>>(
    factories: &B::Factories,
    batches: Vec<(Vec<Tup2<i32, ZWeight>>, i32)>,
    seed: u64,
) {
    let mut trace: Spine<B> = Spine::new(factories);
    let mut ref_trace: TestBatch<DynI32, DynUnit /* <()> */, u32, DynZWeight> =
        TestBatch::new(&TestBatchFactories::new());

    let mut kbound = 0;
    for (time, (tuples, bound)) in batches.into_iter().enumerate() {
        let mut erased_tuples = zset_tuples(tuples.clone());

        let batch = B::dyn_from_tuples(factories, time as u32, &mut erased_tuples.clone());
        let ref_batch =
            TestBatch::dyn_from_tuples(&TestBatchFactories::new(), time as u32, &mut erased_tuples);

        assert_batch_eq(&batch, &ref_batch);

        ref_trace.insert(ref_batch);
        assert_batch_cursors_eq(
            CursorPair::new(&mut trace.cursor(), &mut batch.cursor()),
            &ref_trace,
            seed,
        );

        trace.insert(batch);

        assert_trace_eq(&trace, &ref_trace);

        kbound = max(bound, kbound);
        trace.retain_keys(Filter::new(Box::new(move |key| {
            *key.downcast_checked::<i32>() >= kbound
        })));
        ref_trace.retain_keys(Filter::new(Box::new(move |key| {
            *key.downcast_checked::<i32>() >= kbound
        })));

        assert_trace_eq(&trace, &ref_trace);
    }
}

proptest! {
    #[test]
    fn test_truncate_key_bounded_memory(batches in kvr_batches_monotone_keys(100, 20, 50, 20, 500)) {
        let factories = <OrdIndexedZSetFactories<DynI32, DynI32>>::new::<i32, i32, ZWeight>();

        let mut trace: Spine<OrdIndexedZSet<DynI32, DynI32>> = Spine::new(&factories);

        for (i, tuples) in batches.into_iter().enumerate() {
            let mut erased_tuples = indexed_zset_tuples(tuples);

            let batch = <OrdIndexedZSet<DynI32, DynI32>>::dyn_from_tuples(&factories, (), &mut erased_tuples);

            test_batch_sampling(&batch);

            trace.insert(batch);
            trace.retain_keys(Filter::new(Box::new(move |x| *x.downcast_checked::<i32>() >= ((i * 20) as i32))));

            trace.complete_merges();
            // FIXME: Change to 20000 after changing vtable types to pointers.
            let trace_total_bytes = trace.size_of().total_bytes();
            assert!(trace_total_bytes < /*20000*/ 200000, "total bytes={}", trace_total_bytes);
        }
    }

    #[test]
    fn test_truncate_value_bounded_memory(batches in kvr_batches_monotone_values(50, 100, 20, 20, 500)) {
        let factories = <OrdIndexedZSetFactories<DynI32, DynI32>>::new::<i32, i32, ZWeight>();

        let mut trace: Spine<OrdIndexedZSet<DynI32, DynI32>> = Spine::new(&factories);

        for (i, tuples) in batches.into_iter().enumerate() {
            let mut erased_tuples = indexed_zset_tuples(tuples);

            let batch = <OrdIndexedZSet<DynI32, DynI32>>::dyn_from_tuples(&factories, (), &mut erased_tuples);

            test_batch_sampling(&batch);

            trace.retain_values(Filter::new(Box::new(move |x| *x.downcast_checked::<i32>() >= ((i * 20) as i32))));
            trace.insert(batch);
            trace.complete_merges();
            // FIXME: Change to 20000 after changing vtable types to pointers.
            let trace_total_bytes = trace.size_of().total_bytes();
            assert!(trace_total_bytes < /*20000*/ 200000, "total bytes={}", trace_total_bytes);
        }
    }

    #[test]
    fn test_vec_zset_spine(batches in kr_batches(50, 2, 100, 20), seed in 0..u64::MAX) {
        let factories = <OrdZSetFactories<DynI32>>::new::<i32, (), ZWeight>();

        test_zset_spine::<OrdZSet<DynI32>>(&factories, batches, seed)
    }

    #[test]
    fn test_file_zset_spine(batches in kr_batches(50, 2, 50, 10), seed in 0..u64::MAX) {
        let factories = <FallbackWSetFactories<DynI32, DynZWeight>>::new::<i32, (), ZWeight>();

        test_zset_spine::<FallbackWSet<DynI32, DynZWeight>>(&factories, batches, seed)
    }

    #[test]
    fn test_vec_indexed_zset_spine(batches in kvr_batches(100, 5, 2, 500, 20), seed in 0..u64::MAX) {
        let factories = <OrdIndexedZSetFactories<DynI32, DynI32>>::new::<i32, i32, ZWeight>();
        test_indexed_zset_spine::<OrdIndexedZSet<DynI32, DynI32>>(&factories, batches, seed)
    }

    #[test]
    fn test_file_indexed_zset_spine(batches in kvr_batches(100, 5, 2, 200, 10), seed in 0..u64::MAX) {
        let factories = <FallbackIndexedWSetFactories<DynI32, DynI32, DynZWeight>>::new::<i32, i32, ZWeight>();
        test_indexed_zset_spine::<FallbackIndexedWSet<DynI32, DynI32, DynZWeight>>(&factories, batches, seed)
    }

    // Like `test_indexed_zset_spine` but keeps even values only.
    #[test]
    fn test_indexed_zset_spine_even_values(batches in kvr_batches(100, 5, 2, 500, 10), seed in 0..u64::MAX) {
        let factories = <OrdIndexedZSetFactories<DynI32, DynI32>>::new::<i32, i32, ZWeight>();

        let mut trace: Spine<OrdIndexedZSet<DynI32, DynI32>> = Spine::new(&factories);
        let mut ref_trace: TestBatch<DynI32, DynI32, (), DynZWeight> = TestBatch::new(&TestBatchFactories::new());

        trace.retain_values(Filter::new(Box::new(move |val| *val.downcast_checked::<i32>() % 2 == 0)));
        ref_trace.retain_values(Filter::new(Box::new(move |val| *val.downcast_checked::<i32>() % 2 == 0)));

        for (tuples, _key_bound, _val_bound) in batches.into_iter() {
            let mut erased_tuples = indexed_zset_tuples(tuples);

            let batch = OrdIndexedZSet::dyn_from_tuples(&factories, (), &mut erased_tuples.clone());
            let ref_batch = TestBatch::dyn_from_tuples(&TestBatchFactories::new(), (), &mut erased_tuples);

            test_batch_sampling(&batch);

            assert_batch_eq(&batch, &ref_batch);
            assert_batch_cursors_eq(batch.cursor(), &ref_batch, seed);

            ref_trace.insert(ref_batch);
            assert_batch_cursors_eq(CursorPair::new(&mut batch.cursor(), &mut trace.cursor()), &ref_trace, seed);

            trace.insert(batch);
            test_trace_sampling(&trace);

            assert_trace_eq(&trace, &ref_trace);
            assert_batch_cursors_eq(trace.cursor(), &ref_trace, seed);
        }
    }

    #[test]
    fn test_indexed_zset_spine_even_keys(batches in kvr_batches(100, 5, 2, 500, 10), seed in 0..u64::MAX) {
        let factories = <OrdIndexedZSetFactories<DynI32, DynI32>>::new::<i32, i32, ZWeight>();

        let mut trace: Spine<OrdIndexedZSet<DynI32, DynI32>> = Spine::new(&factories);
        let mut ref_trace: TestBatch<DynI32, DynI32, (), DynZWeight> = TestBatch::new(&TestBatchFactories::new());

        trace.retain_keys(Filter::new(Box::new(move |val| *val.downcast_checked::<i32>() % 2 == 0)));
        ref_trace.retain_keys(Filter::new(Box::new(move |val| *val.downcast_checked::<i32>() % 2 == 0)));

        for (tuples, _key_bound, _val_bound) in batches.into_iter() {
            let mut erased_tuples = indexed_zset_tuples(tuples);

            let batch = OrdIndexedZSet::dyn_from_tuples(&factories, (), &mut erased_tuples.clone());
            let ref_batch = TestBatch::dyn_from_tuples(&TestBatchFactories::new(), (), &mut erased_tuples);

            test_batch_sampling(&batch);

            assert_batch_eq(&batch, &ref_batch);
            assert_batch_cursors_eq(batch.cursor(), &ref_batch, seed);

            ref_trace.insert(ref_batch);
            assert_batch_cursors_eq(CursorPair::new(&mut batch.cursor(), &mut trace.cursor()), &ref_trace, seed);

            trace.insert(batch);
            test_trace_sampling(&trace);

            assert_trace_eq(&trace, &ref_trace);
            assert_batch_cursors_eq(trace.cursor(), &ref_trace, seed);
        }
    }

    #[test]
    fn test_vec_zset_trace_spine(batches in kr_batches(100, 2, 500, 20), seed in 0..u64::MAX) {
        let factories = <OrdKeyBatchFactories<DynI32, u32, DynZWeight>>::new::<i32, (), ZWeight>();
        test_zset_trace_spine::<OrdKeyBatch<DynI32, u32, DynZWeight>>(&factories, batches, seed)
    }

    #[test]
    fn test_file_zset_trace_spine(batches in kr_batches(100, 2, 200, 10), seed in 0..u64::MAX) {
        run_in_circuit_with_storage(move || {
            let factories = <FileKeyBatchFactories<DynI32, u32, DynZWeight>>::new::<i32, (), ZWeight>();
            test_zset_trace_spine::<FileKeyBatch<DynI32, u32, DynZWeight>>(&factories, batches, seed);
        });
    }

    #[test]
    fn test_vec_indexed_zset_trace_spine(batches in kvr_batches(100, 5, 2, 300, 20), seed in 0..u64::MAX) {
        let factories = <OrdValBatchFactories<DynI32, DynI32, u32, DynZWeight>>::new::<i32, i32, ZWeight>();

        test_indexed_zset_trace_spine::<OrdValBatch<DynI32, DynI32, u32, DynZWeight>>(&factories, batches, seed)
    }

    #[test]
    fn test_file_indexed_zset_trace_spine(batches in kvr_batches(100, 5, 2, 100, 10), seed in 0..u64::MAX) {
        run_in_circuit_with_storage(move || {
            let factories =
                <FileValBatchFactories<DynI32, DynI32, u32, DynZWeight>>::new::<i32, i32, ZWeight>();
            test_indexed_zset_trace_spine::<FileValBatch<DynI32, DynI32, u32, DynZWeight>>(&factories, batches, seed);
        });
    }

    // Like `test_indexed_zset_trace_spine` but keeps even values only.
    #[test]
    fn test_indexed_zset_trace_spine_retain_even_values(batches in kvr_batches(100, 5, 2, 300, 20), seed in 0..u64::MAX) {
        let factories = <OrdValBatchFactories<DynI32, DynI32, u32, DynZWeight>>::new::<i32, i32, ZWeight>();

        // `trace1` uses `truncate_keys_below`.
        // `trace2` uses `retain_keys`.
        let mut trace: Spine<OrdValBatch<DynI32, DynI32, u32, DynZWeight>> = Spine::new(&factories);
        let mut ref_trace: TestBatch<DynI32, DynI32, u32, DynZWeight> = TestBatch::new(&TestBatchFactories::new());

        trace.retain_values(Filter::new(Box::new(move |val| *val.downcast_checked::<i32>() % 2 == 0)));
        ref_trace.retain_values(Filter::new(Box::new(move |val| *val.downcast_checked::<i32>() % 2 == 0)));

        for (time, (tuples, _key_bound, _val_bound)) in batches.into_iter().enumerate() {
            let mut erased_tuples = indexed_zset_tuples(tuples);

            let batch = OrdValBatch::dyn_from_tuples(&factories, time as u32, &mut erased_tuples.clone());
            let ref_batch = TestBatch::dyn_from_tuples(&TestBatchFactories::new(), time as u32, &mut erased_tuples);

            assert_batch_eq(&batch, &ref_batch);
            assert_batch_cursors_eq(batch.cursor(), &ref_batch, seed);

            ref_trace.insert(ref_batch);
            assert_batch_cursors_eq(CursorPair::new(&mut trace.cursor(), &mut batch.cursor()), &ref_trace, seed);

            trace.insert(batch);

            assert_trace_eq(&trace, &ref_trace);
            assert_batch_cursors_eq(trace.cursor(), &ref_trace, seed);
        }
    }

    #[test]
    fn test_indexed_zset_trace_spine_retain_even_keys(batches in kvr_batches(100, 5, 2, 300, 10), seed in 0..u64::MAX) {
        let factories = <OrdValBatchFactories<DynI32, DynI32, u32, DynZWeight>>::new::<i32, i32, ZWeight>();

        // `trace1` uses `truncate_keys_below`.
        // `trace2` uses `retain_keys`.
        let mut trace: Spine<OrdValBatch<DynI32, DynI32, u32, DynZWeight>> = Spine::new(&factories);
        let mut ref_trace: TestBatch<DynI32, DynI32, u32, DynZWeight> = TestBatch::new(&TestBatchFactories::new());

        trace.retain_keys(Filter::new(Box::new(move |key| *key.downcast_checked::<i32>() % 2 == 0)));
        ref_trace.retain_keys(Filter::new(Box::new(move |key| *key.downcast_checked::<i32>() % 2 == 0)));

        for (time, (tuples, _key_bound, _val_bound)) in batches.into_iter().enumerate() {
            let mut erased_tuples = indexed_zset_tuples(tuples);

            let batch = OrdValBatch::dyn_from_tuples(&factories, time as u32, &mut erased_tuples.clone());
            let ref_batch = TestBatch::dyn_from_tuples(&TestBatchFactories::new(), time as u32, &mut erased_tuples);

            assert_batch_eq(&batch, &ref_batch);
            assert_batch_cursors_eq(batch.cursor(), &ref_batch, seed);

            ref_trace.insert(ref_batch);
            assert_batch_cursors_eq(CursorPair::new(&mut trace.cursor(), &mut batch.cursor()), &ref_trace, seed);

            trace.insert(batch);

            assert_trace_eq(&trace, &ref_trace);
            assert_batch_cursors_eq(trace.cursor(), &ref_trace, seed);
        }
    }

}

/// Executes `f`, once, inside a circuit initialized so that it has access to
/// storage.
///
/// This is necessary because the batches in `crate::trace::ord::file` require
/// access to storage, which they get per-thread from a [Runtime], and which is
/// only available within a circuit initialized with storage.
fn run_in_circuit_with_storage<F>(f: F)
where
    F: FnOnce() + Clone + Send + 'static,
{
    let (_temp_dir, config) = mkconfig();
    let count = Arc::new(AtomicUsize::new(0));
    Runtime::init_circuit(config, {
        let count = count.clone();
        move |_| {
            count.fetch_add(1, Ordering::Relaxed);
            f();
            Ok(())
        }
    })
    .unwrap();

    // Make sure that the callback executes exactly once.
    assert_eq!(count.load(Ordering::Relaxed), 1);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1000))]

    #[test]
    fn test_vec_indexed_zset_builder(batch in kvtr_batch(5, 5, 5, 5, 20), seed in 0..u64::MAX) {
        let factories = <OrdValBatchFactories<DynI32, DynI32, u32, DynZWeight>>::new::<i32, i32, ZWeight>();

        test_indexed_zset_trace_builder::<OrdValBatch<DynI32, DynI32, u32, DynZWeight>>(&factories, batch, seed)
    }

    #[test]
    fn test_file_indexed_zset_builder(batch in kvtr_batch(5, 5, 5, 5, 20), seed in 0..u64::MAX) {
        run_in_circuit_with_storage(move || {
            let factories = <FileValBatchFactories<DynI32, DynI32, u32, DynZWeight>>::new::<i32, i32, ZWeight>();
            test_indexed_zset_trace_builder::<FileValBatch<DynI32, DynI32, u32, DynZWeight>>(&factories, batch, seed);
        });
    }

    #[test]
    fn test_vec_zset_builder(batch in ktr_batch(5, 5, 5, 20), seed in 0..u64::MAX) {
        let factories = <OrdKeyBatchFactories<DynI32, u32, DynZWeight>>::new::<i32, (), ZWeight>();

        test_zset_trace_builder::<OrdKeyBatch<DynI32, u32, DynZWeight>>(&factories, batch, seed)
    }

    #[test]
    fn test_file_zset_builder(batch in ktr_batch(5, 5, 5, 20), seed in 0..u64::MAX) {
        run_in_circuit_with_storage(move || {
            let factories = <FileKeyBatchFactories<DynI32, u32, DynZWeight>>::new::<i32, (), ZWeight>();
            test_zset_trace_builder::<FileKeyBatch<DynI32, u32, DynZWeight>>(&factories, batch, seed);
        });
    }
}
