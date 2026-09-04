use crate::trace::StoragePath;
use crate::utils::test::CIRCUIT_CASES;
use std::{
    cmp::max,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    thread::sleep,
    time::{Duration, Instant},
};

use feldera_storage::tokio::TOKIO;
use feldera_types::memory_pressure::MemoryPressure;
use proptest::{collection::vec, prelude::*, strategy::BoxedStrategy};
use size_of::SizeOf;
use tempfile::tempdir;

use crate::{
    DynZWeight, Runtime, ZWeight,
    algebra::{
        AddByRef, IndexedZSet, NegByRef, OrdIndexedZSet, OrdIndexedZSetFactories, OrdZSet,
        OrdZSetFactories, ZBatch, ZSet,
    },
    circuit::{CircuitConfig, mkconfig},
    dynamic::{DowncastTrait, DynData, DynUnit, DynWeightedPairs, Erase, LeanVec, pair::DynPair},
    storage::{buffer_cache::CacheStats, file::FilterKind},
    trace::{
        Batch, BatchLocation, BatchReader, BatchReaderFactories, Builder, FileIndexedWSetFactories,
        FileWSetFactories, GroupFilter, ListMerger, Spine, Trace, TraceRole, VecIndexedWSet,
        VecIndexedWSetFactories, VecKeyBatch, VecKeyBatchFactories, VecValBatch,
        VecValBatchFactories, VecWSet, VecWSetFactories,
        cursor::{Cursor, CursorPair},
        ord::{
            FileIndexedWSet, FileKeyBatch, FileKeyBatchFactories, FileValBatch,
            FileValBatchFactories, FileWSet, OrdKeyBatch, OrdKeyBatchFactories, OrdValBatch,
            OrdValBatchFactories,
        },
        test::test_batch::{
            TestBatch, TestBatchFactories, assert_batch_cursors_eq, assert_batch_eq,
            assert_trace_eq, test_batch_sampling, test_trace_sampling,
        },
    },
    utils::{Tup1, Tup2, Tup3, Tup4},
};

use super::Filter;
use crate::circuit::runtime::tests::with_mock_process_rss;
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
    let mut trace: Spine<B> = Spine::new(
        factories,
        Arc::new(String::from("Test")),
        TraceRole::Integral,
    );

    let mut ref_trace: TestBatch<DynI32, DynUnit /* <()> */, (), DynZWeight> = TestBatch::new(
        &TestBatchFactories::new(),
        Arc::new(String::from("Test")),
        TraceRole::Integral,
    );

    let mut kbound = 0;
    for (tuples, bound) in batches.into_iter() {
        let mut erased_tuples = zset_tuples(tuples.clone());

        let batch = B::dyn_from_tuples(factories, (), &mut erased_tuples.clone());
        let ref_batch: TestBatch<DynData, DynUnit, (), DynZWeight> =
            TestBatch::dyn_from_tuples(&TestBatchFactories::new(), (), &mut erased_tuples);

        test_batch_sampling(&batch);

        assert_batch_eq(&batch, &ref_batch);

        TOKIO.block_on(ref_trace.insert(ref_batch));
        assert_batch_cursors_eq(
            CursorPair::new(&mut batch.cursor(), &mut trace.cursor()),
            &ref_trace,
            seed,
        );

        TOKIO.block_on(trace.insert(batch));
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
    let mut trace: Spine<B> = Spine::new(
        factories,
        Arc::new(String::from("Test")),
        TraceRole::Integral,
    );

    let mut ref_trace: TestBatch<DynI32, DynI32, (), DynZWeight> = TestBatch::new(
        &TestBatchFactories::new(),
        Arc::new(String::from("Test")),
        TraceRole::Integral,
    );

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

        TOKIO.block_on(ref_trace.insert(ref_batch));
        assert_batch_cursors_eq(
            CursorPair::new(&mut batch.cursor(), &mut trace.cursor()),
            &ref_trace,
            seed,
        );

        TOKIO.block_on(trace.insert(batch));
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
        trace.retain_values(GroupFilter::Simple(Filter::new(Box::new(
            move |val: &DynI32| *val.downcast_checked::<i32>() >= bound,
        ))));
        ref_trace.retain_values(GroupFilter::Simple(Filter::new(Box::new(
            move |val: &DynI32| *val.downcast_checked::<i32>() >= bound,
        ))));
        test_trace_sampling(&trace);

        assert_trace_eq(&trace, &ref_trace);
        assert_batch_cursors_eq(trace.cursor(), &ref_trace, seed);
    }
}

/// Tests [`Trace::fork`]: inserts `batches[..fork_at]` into a spine, forks
/// it, and checks that (1) the fork starts out equal to the source, (2) the
/// fork is frozen while the source receives `batches[fork_at..]`, and (3)
/// retracting the fork's entire contents empties the fork without affecting
/// the source.  Step (3) also exercises `consolidate` on a spine whose
/// batches are `Arc`-shared with another spine.
fn test_fork_spine<B: IndexedZSet<Key = DynI32, Val = DynI32>>(
    factories: &B::Factories,
    batches: Vec<Vec<Tup2<Tup2<i32, i32>, ZWeight>>>,
    fork_at: usize,
) {
    let fork_at = fork_at.min(batches.len());

    let mut trace: Spine<B> = Spine::new(
        factories,
        Arc::new(String::from("Test")),
        TraceRole::Integral,
    );
    let mut ref_trace: TestBatch<DynI32, DynI32, (), DynZWeight> = TestBatch::new(
        &TestBatchFactories::new(),
        Arc::new(String::from("Test")),
        TraceRole::Integral,
    );

    for tuples in &batches[..fork_at] {
        let mut erased_tuples = indexed_zset_tuples(tuples.clone());

        let batch = B::dyn_from_tuples(factories, (), &mut erased_tuples.clone());
        let ref_batch =
            TestBatch::dyn_from_tuples(&TestBatchFactories::new(), (), &mut erased_tuples);

        TOKIO.block_on(ref_trace.insert(ref_batch));
        TOKIO.block_on(trace.insert(batch));
    }

    let mut fork = trace.fork();
    let ref_fork = ref_trace.clone();

    // The fork starts out equal to the source.
    assert_trace_eq(&fork, &ref_fork);

    // Insert the remaining batches into the source only.
    for tuples in &batches[fork_at..] {
        let mut erased_tuples = indexed_zset_tuples(tuples.clone());

        let batch = B::dyn_from_tuples(factories, (), &mut erased_tuples.clone());
        let ref_batch =
            TestBatch::dyn_from_tuples(&TestBatchFactories::new(), (), &mut erased_tuples);

        TOKIO.block_on(ref_trace.insert(ref_batch));
        TOKIO.block_on(trace.insert(batch));
    }

    // The source evolved past the fork point; the fork stayed frozen.
    assert_trace_eq(&trace, &ref_trace);
    assert_trace_eq(&fork, &ref_fork);

    // Retract the fork's entire contents from the fork only: the fork
    // consolidates to nothing, the source is unaffected.
    for tuples in &batches[..fork_at] {
        let negated = tuples
            .iter()
            .map(|Tup2(kv, w)| Tup2(*kv, -*w))
            .collect::<Vec<_>>();
        let mut erased_tuples = indexed_zset_tuples(negated);

        let batch = B::dyn_from_tuples(factories, (), &mut erased_tuples);
        TOKIO.block_on(fork.insert(batch));
    }
    assert!(fork.consolidate().is_none());
    assert_trace_eq(&trace, &ref_trace);
}

fn test_val_batch_trace_spine<B: ZBatch<Key = DynI32, Val = DynI32, Time = u32>>(
    factories: &B::Factories,
    batches: Vec<(Vec<Tup2<Tup2<i32, i32>, ZWeight>>, i32, i32)>,
    seed: u64,
) {
    // `trace1` uses `truncate_keys_below`.
    // `trace2` uses `retain_keys`.
    let mut trace: Spine<B> = Spine::new(
        factories,
        Arc::new(String::from("Test")),
        TraceRole::Integral,
    );
    let mut ref_trace: TestBatch<DynI32, DynI32, u32, DynZWeight> = TestBatch::new(
        &TestBatchFactories::new(),
        Arc::new(String::from("Test")),
        TraceRole::Integral,
    );

    let mut bound = 0;
    let mut kbound = 0;
    for (time, (tuples, key_bound, val_bound)) in batches.into_iter().enumerate() {
        let mut erased_tuples = indexed_zset_tuples(tuples);

        let batch = B::dyn_from_tuples(factories, time as u32, &mut erased_tuples.clone());
        let ref_batch =
            TestBatch::dyn_from_tuples(&TestBatchFactories::new(), time as u32, &mut erased_tuples);

        assert_batch_eq(&batch, &ref_batch);
        assert_batch_cursors_eq(batch.cursor(), &ref_batch, seed);

        TOKIO.block_on(ref_trace.insert(ref_batch));
        assert_batch_cursors_eq(
            CursorPair::new(&mut trace.cursor(), &mut batch.cursor()),
            &ref_trace,
            seed,
        );

        TOKIO.block_on(trace.insert(batch));

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
        trace.retain_values(GroupFilter::Simple(Filter::new(Box::new(
            move |val: &DynI32| *val.downcast_checked::<i32>() >= bound,
        ))));
        ref_trace.retain_values(GroupFilter::Simple(Filter::new(Box::new(
            move |val: &DynI32| *val.downcast_checked::<i32>() >= bound,
        ))));

        assert_trace_eq(&trace, &ref_trace);
        assert_batch_cursors_eq(trace.cursor(), &ref_trace, seed);
    }
}

fn val_batch_from_tuples<B>(factories: &B::Factories, tuples: &[Tup4<i32, i32, u32, ZWeight>]) -> B
where
    B: ZBatch<Key = DynI32, Val = DynI32, Time = u32>,
{
    let mut builder = B::Builder::with_capacity(factories, tuples.len(), tuples.len());
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

fn test_val_batch_trace_builder<B>(
    factories: &B::Factories,
    mut tuples: Vec<Tup4<i32, i32, u32, ZWeight>>,
    seed: u64,
) where
    B: ZBatch<Key = DynI32, Val = DynI32, Time = u32>,
{
    tuples.sort_unstable();
    tuples.retain(|Tup4(_k, _v, _t, r)| *r != 0);
    tuples.dedup_by_key(|Tup4(k, v, t, _r)| (*k, *v, *t));

    let ref_batch = val_batch_from_tuples::<TestBatch<DynI32, DynI32, u32, DynZWeight>>(
        &TestBatchFactories::new(),
        &tuples,
    );

    let batch = val_batch_from_tuples::<B>(factories, &tuples);

    assert_batch_eq(&batch, &ref_batch);
    assert_batch_cursors_eq(batch.cursor(), &ref_batch, seed);
}

fn timed_batch_from_tuples<B>(factories: &B::Factories, tuples: &[Tup3<i32, u32, ZWeight>]) -> B
where
    B: ZBatch<Key = DynI32, Val = DynUnit, Time = u32>,
{
    let mut builder = B::Builder::with_capacity(factories, tuples.len(), tuples.len());
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

fn test_key_batch_builder<B>(
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

fn test_key_batch_spine<B: ZBatch<Key = DynI32, Val = DynUnit, Time = u32>>(
    factories: &B::Factories,
    batches: Vec<(Vec<Tup2<i32, ZWeight>>, i32)>,
    seed: u64,
) {
    let mut trace: Spine<B> = Spine::new(
        factories,
        Arc::new(String::from("Test")),
        TraceRole::Integral,
    );
    let mut ref_trace: TestBatch<DynI32, DynUnit /* <()> */, u32, DynZWeight> = TestBatch::new(
        &TestBatchFactories::new(),
        Arc::new(String::from("Test")),
        TraceRole::Integral,
    );

    let mut kbound = 0;
    for (time, (tuples, bound)) in batches.into_iter().enumerate() {
        let mut erased_tuples = zset_tuples(tuples.clone());

        let batch = B::dyn_from_tuples(factories, time as u32, &mut erased_tuples.clone());
        let ref_batch =
            TestBatch::dyn_from_tuples(&TestBatchFactories::new(), time as u32, &mut erased_tuples);

        assert_batch_eq(&batch, &ref_batch);

        TOKIO.block_on(ref_trace.insert(ref_batch));
        assert_batch_cursors_eq(
            CursorPair::new(&mut trace.cursor(), &mut batch.cursor()),
            &ref_trace,
            seed,
        );

        TOKIO.block_on(trace.insert(batch));

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

fn assert_out_of_range_seek_uses_range_filter<B>(batch: &B, low: i32, high: i32)
where
    B: BatchReader<Key = DynI32, Time = ()>,
{
    let range_before = batch.range_filter_stats();
    let membership_before = batch.membership_filter_stats();
    assert!(range_before.size_byte > 0);

    let low: Box<DynI32> = Box::new(low).erase_box();
    let high: Box<DynI32> = Box::new(high).erase_box();
    let mut cursor = batch.cursor();
    assert!(!cursor.seek_key_exact(low.as_ref(), None));
    assert!(!cursor.seek_key_exact(high.as_ref(), None));

    let range_after = batch.range_filter_stats();
    let membership_after = batch.membership_filter_stats();

    assert_eq!(range_after.size_byte, range_before.size_byte);
    assert_eq!(range_after.hits, range_before.hits);
    assert_eq!(range_after.misses, range_before.misses + 2);
    assert_eq!(membership_after, membership_before);
}

#[test]
fn test_file_wset_neg_by_ref_preserves_key_bounds() {
    run_in_circuit_with_storage(|| {
        let factories = <FileWSetFactories<DynI32, DynZWeight>>::new::<i32, (), ZWeight>();
        let tuples = vec![Tup2(-10, 1), Tup2(0, -2), Tup2(25, 3)];

        let mut erased_tuples = zset_tuples(tuples.clone());
        let batch =
            FileWSet::<DynI32, DynZWeight>::dyn_from_tuples(&factories, (), &mut erased_tuples);
        let negated = batch.neg_by_ref();

        let mut expected_tuples = zset_tuples(
            tuples
                .into_iter()
                .map(|Tup2(key, weight)| Tup2(key, -weight))
                .collect(),
        );
        let expected =
            TestBatch::dyn_from_tuples(&TestBatchFactories::new(), (), &mut expected_tuples);

        assert_batch_eq(&negated, &expected);
        assert_out_of_range_seek_uses_range_filter(&negated, -20, 40);
    });
}

/// A layer file records whether its value column carries a hidden trailing
/// column, and opening one through factories that disagree is a loud error
/// rather than a misdecode.
///
/// This is the guard that keeps a projected batch from being read as though its
/// values were plain, which would deserialize `Tup2<V, u32>` bytes as `V`
/// through unchecked rkyv.
#[test]
fn test_file_indexed_wset_value_stamp_guards_from_path() {
    run_in_circuit_with_storage(|| {
        let stamped =
            <FileIndexedWSetFactories<DynI32, DynI32, DynZWeight>>::stamped::<i32, i32, ZWeight>();
        let plain =
            <FileIndexedWSetFactories<DynI32, DynI32, DynZWeight>>::new::<i32, i32, ZWeight>();

        let mut tuples = indexed_zset_tuples(vec![Tup2(Tup2(1, 10), 1), Tup2(Tup2(2, 20), 1)]);
        let batch = FileIndexedWSet::<DynI32, DynI32, DynZWeight>::dyn_from_tuples(
            &stamped,
            (),
            &mut tuples,
        );

        let path = batch.file_reader().unwrap().path().to_string().into();

        // Same factories: reopens, and the contents survive. This also proves
        // the builder wrote the stamp, since the guard compares the two.
        let reopened = FileIndexedWSet::<DynI32, DynI32, DynZWeight>::from_path(&stamped, &path)
            .expect("a stamped file reopens through stamped factories");
        assert_eq!(reopened.approx_len(), batch.approx_len());

        // Plain factories: refused, not misdecoded.
        let err = FileIndexedWSet::<DynI32, DynI32, DynZWeight>::from_path(&plain, &path)
            .expect_err("plain factories must refuse a stamped file");
        assert!(
            format!("{err}").contains("stamped"),
            "expected a value-stamp mismatch, got: {err}"
        );
    });
}

#[test]
fn test_file_indexed_wset_neg_by_ref_preserves_key_bounds() {
    run_in_circuit_with_storage(|| {
        let factories =
            <FileIndexedWSetFactories<DynI32, DynI32, DynZWeight>>::new::<i32, i32, ZWeight>();
        let tuples = vec![
            Tup2(Tup2(-10, 1), 1),
            Tup2(Tup2(0, 5), -2),
            Tup2(Tup2(25, 7), 3),
        ];

        let mut erased_tuples = indexed_zset_tuples(tuples.clone());
        let batch = FileIndexedWSet::<DynI32, DynI32, DynZWeight>::dyn_from_tuples(
            &factories,
            (),
            &mut erased_tuples,
        );
        let negated = batch.neg_by_ref();

        let mut expected_tuples = indexed_zset_tuples(
            tuples
                .into_iter()
                .map(|Tup2(Tup2(key, val), weight)| Tup2(Tup2(key, val), -weight))
                .collect(),
        );
        let expected =
            TestBatch::dyn_from_tuples(&TestBatchFactories::new(), (), &mut expected_tuples);

        assert_batch_eq(&negated, &expected);
        assert_out_of_range_seek_uses_range_filter(&negated, -20, 40);
    });
}

#[test]
fn test_file_indexed_wset_key_bounds() {
    run_in_circuit_with_storage(|| {
        let factories =
            <FileIndexedWSetFactories<DynI32, DynI32, DynZWeight>>::new::<i32, i32, ZWeight>();
        let tuples = vec![
            Tup2(Tup2(-10, 1), 1),
            Tup2(Tup2(0, 5), -2),
            Tup2(Tup2(25, 7), 3),
        ];

        let mut erased_tuples = indexed_zset_tuples(tuples);
        let batch = FileIndexedWSet::<DynI32, DynI32, DynZWeight>::dyn_from_tuples(
            &factories,
            (),
            &mut erased_tuples,
        );

        let Some((min, max)) = batch.key_bounds() else {
            panic!("expected non-empty key bounds");
        };
        assert_eq!(*min.downcast_checked::<i32>(), -10);
        assert_eq!(*max.downcast_checked::<i32>(), 25);

        let empty_batch =
            <FileIndexedWSet<DynI32, DynI32, DynZWeight> as Batch>::Builder::with_capacity(
                &factories, 0, 0,
            )
            .done();
        assert!(empty_batch.key_bounds().is_none());
    });
}

#[test]
fn test_file_key_batch_key_bounds() {
    run_in_circuit_with_storage(|| {
        let factories =
            <FileKeyBatchFactories<DynData, u32, DynZWeight>>::new::<i16, (), ZWeight>();
        let mut batch = <FileKeyBatch<DynData, u32, DynZWeight> as Batch>::Builder::with_capacity(
            &factories, 3, 3,
        );
        for (key, time, weight) in [(-7i16, 1u32, 1i64), (3, 2, -2), (12, 4, 3)] {
            let weight: ZWeight = weight;
            batch.push_time_diff(&time, weight.erase());
            batch.push_val(&());
            batch.push_key(key.erase());
        }
        let batch = batch.done();

        let Some((min, max)) = batch.key_bounds() else {
            panic!("expected non-empty key bounds");
        };
        assert_eq!(*min.downcast_checked::<i16>(), -7);
        assert_eq!(*max.downcast_checked::<i16>(), 12);

        let empty_batch =
            <FileKeyBatch<DynData, u32, DynZWeight> as Batch>::Builder::with_capacity(
                &factories, 0, 0,
            )
            .done();
        assert!(empty_batch.key_bounds().is_none());
    });
}

#[test]
fn test_file_val_batch_key_bounds() {
    run_in_circuit_with_storage(|| {
        let factories =
            <FileValBatchFactories<DynData, DynData, u32, DynZWeight>>::new::<u64, i16, ZWeight>();
        let mut batch =
            <FileValBatch<DynData, DynData, u32, DynZWeight> as Batch>::Builder::with_capacity(
                &factories, 3, 3,
            );
        for (key, val, time, weight) in [(5u64, -1i16, 1u32, 1i64), (11, 2, 2, -1), (42, 9, 7, 4)] {
            let weight: ZWeight = weight;
            batch.push_time_diff(&time, weight.erase());
            batch.push_val(val.erase());
            batch.push_key(key.erase());
        }
        let batch = batch.done();

        let Some((min, max)) = batch.key_bounds() else {
            panic!("expected non-empty key bounds");
        };
        assert_eq!(*min.downcast_checked::<u64>(), 5);
        assert_eq!(*max.downcast_checked::<u64>(), 42);

        let empty_batch =
            <FileValBatch<DynData, DynData, u32, DynZWeight> as Batch>::Builder::with_capacity(
                &factories, 0, 0,
            )
            .done();
        assert!(empty_batch.key_bounds().is_none());
    });
}

#[test]
fn test_file_wset_key_bounds() {
    run_in_circuit_with_storage(|| {
        let batch = build_file_wset_tup1_i32(&[-4, 6, 15]);

        let Some((min, max)) = batch.key_bounds() else {
            panic!("expected non-empty key bounds");
        };
        assert_eq!(min.downcast_checked::<Tup1<i32>>(), &Tup1(-4));
        assert_eq!(max.downcast_checked::<Tup1<i32>>(), &Tup1(15));

        let factories = <FileWSetFactories<DynData, DynZWeight>>::new::<Tup1<i32>, (), ZWeight>();
        let empty_batch =
            <FileWSet<DynData, DynZWeight> as Batch>::Builder::with_capacity(&factories, 0, 0)
                .done();
        assert!(empty_batch.key_bounds().is_none());
    });
}

#[test]
fn test_vec_indexed_wset_key_bounds() {
    let factories =
        <VecIndexedWSetFactories<DynData, DynData, DynZWeight>>::new::<i64, u8, ZWeight>();
    let mut batch = <VecIndexedWSet<DynData, DynData, DynZWeight> as Batch>::Builder::with_capacity(
        &factories, 3, 3,
    );
    for (key, val, weight) in [(-50i64, 1u8, 1i64), (4, 2, -1), (999, 3, 5)] {
        let weight: ZWeight = weight;
        batch.push_val_diff(val.erase(), weight.erase());
        batch.push_key(key.erase());
    }
    let batch = batch.done();

    let Some((min, max)) = batch.key_bounds() else {
        panic!("expected non-empty key bounds");
    };
    assert_eq!(*min.downcast_checked::<i64>(), -50);
    assert_eq!(*max.downcast_checked::<i64>(), 999);

    let empty_batch =
        <VecIndexedWSet<DynData, DynData, DynZWeight> as Batch>::Builder::with_capacity(
            &factories, 0, 0,
        )
        .done();
    assert!(empty_batch.key_bounds().is_none());
}

#[test]
fn test_vec_key_batch_key_bounds() {
    let factories = <VecKeyBatchFactories<DynData, u32, DynZWeight>>::new::<String, (), ZWeight>();
    let mut batch =
        <VecKeyBatch<DynData, u32, DynZWeight> as Batch>::Builder::with_capacity(&factories, 3, 3);
    for (key, time, weight) in [
        (String::from("ant"), 1u32, 1i64),
        (String::from("bee"), 2, -3),
        (String::from("cat"), 7, 4),
    ] {
        let weight: ZWeight = weight;
        batch.push_time_diff(&time, weight.erase());
        batch.push_val(&());
        batch.push_key(key.erase());
    }
    let batch = batch.done();

    let Some((min, max)) = batch.key_bounds() else {
        panic!("expected non-empty key bounds");
    };
    assert_eq!(min.downcast_checked::<String>(), "ant");
    assert_eq!(max.downcast_checked::<String>(), "cat");

    let empty_batch =
        <VecKeyBatch<DynData, u32, DynZWeight> as Batch>::Builder::with_capacity(&factories, 0, 0)
            .done();
    assert!(empty_batch.key_bounds().is_none());
}

#[test]
fn test_vec_val_batch_key_bounds() {
    let factories =
        <VecValBatchFactories<DynData, DynData, u32, DynZWeight>>::new::<u16, i32, ZWeight>();
    let mut batch =
        <VecValBatch<DynData, DynData, u32, DynZWeight> as Batch>::Builder::with_capacity(
            &factories, 3, 3,
        );
    for (key, val, time, weight) in [(2u16, -8i32, 1u32, 1i64), (10, 4, 2, -2), (70, 9, 9, 3)] {
        let weight: ZWeight = weight;
        batch.push_time_diff(&time, weight.erase());
        batch.push_val(val.erase());
        batch.push_key(key.erase());
    }
    let batch = batch.done();

    let Some((min, max)) = batch.key_bounds() else {
        panic!("expected non-empty key bounds");
    };
    assert_eq!(*min.downcast_checked::<u16>(), 2);
    assert_eq!(*max.downcast_checked::<u16>(), 70);

    let empty_batch =
        <VecValBatch<DynData, DynData, u32, DynZWeight> as Batch>::Builder::with_capacity(
            &factories, 0, 0,
        )
        .done();
    assert!(empty_batch.key_bounds().is_none());
}

#[test]
fn test_vec_wset_key_bounds() {
    let factories = <VecWSetFactories<DynData, DynZWeight>>::new::<u8, (), ZWeight>();
    let mut batch =
        <VecWSet<DynData, DynZWeight> as Batch>::Builder::with_capacity(&factories, 3, 0);
    for key in [1u8, 4, 9] {
        let weight: ZWeight = 1;
        batch.push_val_diff(&(), weight.erase());
        batch.push_key(key.erase());
    }
    let batch = batch.done();

    let Some((min, max)) = batch.key_bounds() else {
        panic!("expected non-empty key bounds");
    };
    assert_eq!(*min.downcast_checked::<u8>(), 1);
    assert_eq!(*max.downcast_checked::<u8>(), 9);

    let empty_batch =
        <VecWSet<DynData, DynZWeight> as Batch>::Builder::with_capacity(&factories, 0, 0).done();
    assert!(empty_batch.key_bounds().is_none());
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(CIRCUIT_CASES))]

    #[test]
    fn test_truncate_key_bounded_memory(batches in kvr_batches_monotone_keys(100, 20, 50, 20, 500)) {
        Runtime::run(CircuitConfig::with_workers(1), move |_parker| {
            let factories = <OrdIndexedZSetFactories<DynI32, DynI32>>::new::<i32, i32, ZWeight>();

            let mut trace: Spine<OrdIndexedZSet<DynI32, DynI32>> = Spine::new(&factories, Arc::new(String::from("Test")), TraceRole::Integral);

            for (i, tuples) in batches.into_iter().enumerate() {
                let mut erased_tuples = indexed_zset_tuples(tuples);

                let batch = <OrdIndexedZSet<DynI32, DynI32>>::dyn_from_tuples(&factories, (), &mut erased_tuples);

                test_batch_sampling(&batch);

                TOKIO.block_on(trace.insert(batch));
                trace.retain_keys(Filter::new(Box::new(move |x| *x.downcast_checked::<i32>() >= ((i * 20) as i32))));

                trace.complete_merges();
                let trace_total_bytes = trace.size_of().total_bytes();
                assert!(trace_total_bytes < /*20000*/ 200000, "total bytes={}", trace_total_bytes);
            }
        }).unwrap().join().unwrap();
    }

    #[test]
    fn test_truncate_value_bounded_memory(batches in kvr_batches_monotone_values(50, 100, 20, 20, 500)) {
        Runtime::run(CircuitConfig::with_workers(1), move |_parker| {
            let factories = <OrdIndexedZSetFactories<DynI32, DynI32>>::new::<i32, i32, ZWeight>();

            let mut trace: Spine<OrdIndexedZSet<DynI32, DynI32>> = Spine::new(&factories, Arc::new(String::from("Test")), TraceRole::Integral);

            for (i, tuples) in batches.into_iter().enumerate() {
                let mut erased_tuples = indexed_zset_tuples(tuples);

                let batch = <OrdIndexedZSet<DynI32, DynI32>>::dyn_from_tuples(&factories, (), &mut erased_tuples);

                test_batch_sampling(&batch);

                trace.retain_values(GroupFilter::Simple(Filter::new(Box::new(
                    move |x: &DynI32| *x.downcast_checked::<i32>() >= ((i * 20) as i32),
                ))));
                TOKIO.block_on(trace.insert(batch));
                trace.complete_merges();
                // FIXME: Change to 20000 after changing vtable types to pointers.
                let trace_total_bytes = trace.size_of().total_bytes();
                assert!(trace_total_bytes < /*20000*/ 200000, "total bytes={}", trace_total_bytes);
            }
        }).unwrap().join().unwrap();
    }

    #[test]
    fn test_vec_zset_spine(batches in kr_batches(50, 2, 100, 20), seed in 0..u64::MAX) {
        let factories = <OrdZSetFactories<DynI32>>::new::<i32, (), ZWeight>();

        Runtime::run(CircuitConfig::with_workers(1), move |_parker| {
            test_zset_spine::<OrdZSet<DynI32>>(&factories, batches, seed)
        }).unwrap().join().unwrap();
    }

    #[test]
    fn test_file_zset_spine(batches in kr_batches(50, 2, 50, 10), seed in 0..u64::MAX) {

        let tempdir = tempfile::tempdir().unwrap();
        Runtime::run(CircuitConfig::with_workers(1).with_temporary_storage(tempdir.path()), move |_parker| {
            let factories = <FileWSetFactories<DynI32, DynZWeight>>::new::<i32, (), ZWeight>();
            test_zset_spine::<FileWSet<DynI32, DynZWeight>>(&factories, batches, seed)
        }).unwrap().join().unwrap();
    }

    #[test]
    fn test_vec_indexed_zset_spine(batches in kvr_batches(100, 5, 2, 500, 20), seed in 0..u64::MAX) {
        Runtime::run(CircuitConfig::with_workers(1), move |_parker| {
            let factories = <OrdIndexedZSetFactories<DynI32, DynI32>>::new::<i32, i32, ZWeight>();
            test_indexed_zset_spine::<OrdIndexedZSet<DynI32, DynI32>>(&factories, batches, seed)
        }).unwrap().join().unwrap();
    }

    #[test]
    fn test_file_indexed_zset_spine(batches in kvr_batches(100, 5, 2, 200, 10), seed in 0..u64::MAX) {
        let tempdir = tempfile::tempdir().unwrap();
        Runtime::run(CircuitConfig::with_workers(1).with_temporary_storage(tempdir.path()), move |_parker| {
            let factories = <FileIndexedWSetFactories<DynI32, DynI32, DynZWeight>>::new::<i32, i32, ZWeight>();
            test_indexed_zset_spine::<FileIndexedWSet<DynI32, DynI32, DynZWeight>>(&factories, batches, seed)
        }).unwrap().join().unwrap();
    }

    // Like `test_indexed_zset_spine` but keeps even values only.
    #[test]
    fn test_indexed_zset_spine_even_values(batches in kvr_batches(100, 5, 2, 500, 10), seed in 0..u64::MAX) {
        Runtime::run(CircuitConfig::with_workers(1), move |_parker| {
            let factories = <OrdIndexedZSetFactories<DynI32, DynI32>>::new::<i32, i32, ZWeight>();

            let mut trace: Spine<OrdIndexedZSet<DynI32, DynI32>> = Spine::new(&factories, Arc::new(String::from("Test")), TraceRole::Integral);
            let mut ref_trace: TestBatch<DynI32, DynI32, (), DynZWeight> = TestBatch::new(&TestBatchFactories::new(), Arc::new(String::from("Test")), TraceRole::Integral);

            trace.retain_values(GroupFilter::Simple(Filter::new(Box::new(
                move |val: &DynI32| *val.downcast_checked::<i32>() % 2 == 0,
            ))));
            ref_trace.retain_values(GroupFilter::Simple(Filter::new(Box::new(
                move |val: &DynI32| *val.downcast_checked::<i32>() % 2 == 0,
            ))));

            for (tuples, _key_bound, _val_bound) in batches.into_iter() {
                let mut erased_tuples = indexed_zset_tuples(tuples);

                let batch = OrdIndexedZSet::dyn_from_tuples(&factories, (), &mut erased_tuples.clone());
                let ref_batch = TestBatch::dyn_from_tuples(&TestBatchFactories::new(), (), &mut erased_tuples);

                test_batch_sampling(&batch);

                assert_batch_eq(&batch, &ref_batch);
                assert_batch_cursors_eq(batch.cursor(), &ref_batch, seed);

                TOKIO.block_on(ref_trace.insert(ref_batch));
                assert_batch_cursors_eq(CursorPair::new(&mut batch.cursor(), &mut trace.cursor()), &ref_trace, seed);

                TOKIO.block_on(trace.insert(batch));
                test_trace_sampling(&trace);

                assert_trace_eq(&trace, &ref_trace);
                assert_batch_cursors_eq(trace.cursor(), &ref_trace, seed);
            }
        }).unwrap().join().unwrap();
    }

    #[test]
    fn test_indexed_zset_spine_even_keys(batches in kvr_batches(100, 5, 2, 500, 10), seed in 0..u64::MAX) {
        Runtime::run(CircuitConfig::with_workers(1), move |_parker| {
            let factories = <OrdIndexedZSetFactories<DynI32, DynI32>>::new::<i32, i32, ZWeight>();

            let mut trace: Spine<OrdIndexedZSet<DynI32, DynI32>> = Spine::new(&factories, Arc::new(String::from("Test")), TraceRole::Integral);
            let mut ref_trace: TestBatch<DynI32, DynI32, (), DynZWeight> = TestBatch::new(&TestBatchFactories::new(), Arc::new(String::from("Test")), TraceRole::Integral);

            trace.retain_keys(Filter::new(Box::new(move |val| *val.downcast_checked::<i32>() % 2 == 0)));
            ref_trace.retain_keys(Filter::new(Box::new(move |val| *val.downcast_checked::<i32>() % 2 == 0)));

            for (tuples, _key_bound, _val_bound) in batches.into_iter() {
                let mut erased_tuples = indexed_zset_tuples(tuples);

                let batch = OrdIndexedZSet::dyn_from_tuples(&factories, (), &mut erased_tuples.clone());
                let ref_batch = TestBatch::dyn_from_tuples(&TestBatchFactories::new(), (), &mut erased_tuples);

                test_batch_sampling(&batch);

                assert_batch_eq(&batch, &ref_batch);
                assert_batch_cursors_eq(batch.cursor(), &ref_batch, seed);

                TOKIO.block_on(ref_trace.insert(ref_batch));
                assert_batch_cursors_eq(CursorPair::new(&mut batch.cursor(), &mut trace.cursor()), &ref_trace, seed);

                TOKIO.block_on(trace.insert(batch));
                test_trace_sampling(&trace);

                assert_trace_eq(&trace, &ref_trace);
                assert_batch_cursors_eq(trace.cursor(), &ref_trace, seed);
            }
        }).unwrap().join().unwrap();
    }

    #[test]
    fn test_fork_vec_indexed_zset_spine(batches in kvr_batches(100, 5, 2, 500, 20), fork_at in 0..20usize) {
        Runtime::run(CircuitConfig::with_workers(1), move |_parker| {
            let factories = <OrdIndexedZSetFactories<DynI32, DynI32>>::new::<i32, i32, ZWeight>();
            let batches = batches.into_iter().map(|(tuples, _, _)| tuples).collect();
            test_fork_spine::<OrdIndexedZSet<DynI32, DynI32>>(&factories, batches, fork_at)
        }).unwrap().join().unwrap();
    }

    #[test]
    fn test_fork_file_indexed_zset_spine(batches in kvr_batches(100, 5, 2, 200, 10), fork_at in 0..10usize) {
        run_in_circuit_with_storage(move || {
            let factories = <FileIndexedWSetFactories<DynI32, DynI32, DynZWeight>>::new::<i32, i32, ZWeight>();
            let batches = batches.clone().into_iter().map(|(tuples, _, _)| tuples).collect();
            test_fork_spine::<FileIndexedWSet<DynI32, DynI32, DynZWeight>>(&factories, batches, fork_at)
        });
    }

    #[test]
    fn test_vec_key_batch_trace_spine(batches in kr_batches(100, 2, 500, 20), seed in 0..u64::MAX) {
        Runtime::run(CircuitConfig::with_workers(1), move |_parker| {
        let factories = <OrdKeyBatchFactories<DynI32, u32, DynZWeight>>::new::<i32, (), ZWeight>();
            test_key_batch_spine::<OrdKeyBatch<DynI32, u32, DynZWeight>>(&factories, batches, seed)
        }).unwrap().join().unwrap();
    }

    #[test]
    fn test_file_key_batch_spine(batches in kr_batches(100, 2, 200, 10), seed in 0..u64::MAX) {
        run_in_circuit_with_storage(move || {
            let factories = <FileKeyBatchFactories<DynI32, u32, DynZWeight>>::new::<i32, (), ZWeight>();
            test_key_batch_spine::<FileKeyBatch<DynI32, u32, DynZWeight>>(&factories, batches, seed);
        });
    }

    #[test]
    fn test_vec_val_batch_spine(batches in kvr_batches(100, 5, 2, 300, 20), seed in 0..u64::MAX) {
        let factories = <OrdValBatchFactories<DynI32, DynI32, u32, DynZWeight>>::new::<i32, i32, ZWeight>();

        Runtime::run(CircuitConfig::with_workers(1), move |_parker| {
            test_val_batch_trace_spine::<OrdValBatch<DynI32, DynI32, u32, DynZWeight>>(&factories, batches, seed)
        }).unwrap().join().unwrap();
    }

    #[test]
    fn test_file_val_batch_spine(batches in kvr_batches(100, 5, 2, 100, 10), seed in 0..u64::MAX) {
        run_in_circuit_with_storage(move || {
            let factories =
                <FileValBatchFactories<DynI32, DynI32, u32, DynZWeight>>::new::<i32, i32, ZWeight>();
            test_val_batch_trace_spine::<FileValBatch<DynI32, DynI32, u32, DynZWeight>>(&factories, batches, seed);
        });
    }

    // Like `test_val_batch_trace_spine` but keeps even values only.
    #[test]
    fn test_val_batch_spine_retain_even_values(batches in kvr_batches(100, 5, 2, 300, 20), seed in 0..u64::MAX) {
        Runtime::run(CircuitConfig::with_workers(1), move |_parker| {
            let factories = <OrdValBatchFactories<DynI32, DynI32, u32, DynZWeight>>::new::<i32, i32, ZWeight>();

            // `trace1` uses `truncate_keys_below`.
            // `trace2` uses `retain_keys`.
            let mut trace: Spine<OrdValBatch<DynI32, DynI32, u32, DynZWeight>> = Spine::new(&factories, Arc::new(String::from("Test")), TraceRole::Integral);
            let mut ref_trace: TestBatch<DynI32, DynI32, u32, DynZWeight> = TestBatch::new(&TestBatchFactories::new(), Arc::new(String::from("Test")), TraceRole::Integral);

            trace.retain_values(GroupFilter::Simple(Filter::new(Box::new(
                move |val: &DynI32| *val.downcast_checked::<i32>() % 2 == 0,
            ))));
            ref_trace.retain_values(GroupFilter::Simple(Filter::new(Box::new(
                move |val: &DynI32| *val.downcast_checked::<i32>() % 2 == 0,
            ))));

            for (time, (tuples, _key_bound, _val_bound)) in batches.into_iter().enumerate() {
                let mut erased_tuples = indexed_zset_tuples(tuples);

                let batch = OrdValBatch::dyn_from_tuples(&factories, time as u32, &mut erased_tuples.clone());
                let ref_batch = TestBatch::dyn_from_tuples(&TestBatchFactories::new(), time as u32, &mut erased_tuples);

                assert_batch_eq(&batch, &ref_batch);
                assert_batch_cursors_eq(batch.cursor(), &ref_batch, seed);

                TOKIO.block_on(ref_trace.insert(ref_batch));
                assert_batch_cursors_eq(CursorPair::new(&mut trace.cursor(), &mut batch.cursor()), &ref_trace, seed);

                TOKIO.block_on(trace.insert(batch));

                assert_trace_eq(&trace, &ref_trace);
                assert_batch_cursors_eq(trace.cursor(), &ref_trace, seed);
            }
        }).unwrap().join().unwrap();
    }

    #[test]
    fn test_val_batch_spine_retain_even_keys(batches in kvr_batches(100, 5, 2, 300, 10), seed in 0..u64::MAX) {
        Runtime::run(CircuitConfig::with_workers(1), move |_parker| {
            let factories = <OrdValBatchFactories<DynI32, DynI32, u32, DynZWeight>>::new::<i32, i32, ZWeight>();

            // `trace1` uses `truncate_keys_below`.
            // `trace2` uses `retain_keys`.
            let mut trace: Spine<OrdValBatch<DynI32, DynI32, u32, DynZWeight>> = Spine::new(&factories, Arc::new(String::from("Test")), TraceRole::Integral);
            let mut ref_trace: TestBatch<DynI32, DynI32, u32, DynZWeight> = TestBatch::new(&TestBatchFactories::new(), Arc::new(String::from("Test")), TraceRole::Integral);

            trace.retain_keys(Filter::new(Box::new(move |key| *key.downcast_checked::<i32>() % 2 == 0)));
            ref_trace.retain_keys(Filter::new(Box::new(move |key| *key.downcast_checked::<i32>() % 2 == 0)));

            for (time, (tuples, _key_bound, _val_bound)) in batches.into_iter().enumerate() {
                let mut erased_tuples = indexed_zset_tuples(tuples);

                let batch = OrdValBatch::dyn_from_tuples(&factories, time as u32, &mut erased_tuples.clone());
                let ref_batch = TestBatch::dyn_from_tuples(&TestBatchFactories::new(), time as u32, &mut erased_tuples);

                assert_batch_eq(&batch, &ref_batch);
                assert_batch_cursors_eq(batch.cursor(), &ref_batch, seed);

                TOKIO.block_on(ref_trace.insert(ref_batch));
                assert_batch_cursors_eq(CursorPair::new(&mut trace.cursor(), &mut batch.cursor()), &ref_trace, seed);

                TOKIO.block_on(trace.insert(batch));

                assert_trace_eq(&trace, &ref_trace);
                assert_batch_cursors_eq(trace.cursor(), &ref_trace, seed);
            }
        }).unwrap().join().unwrap();
    }

}

/// Checks that [`Trace::fork`] carries over the source's metadata — the
/// compaction frontier, retention filters, and dirty flag — and that
/// [`Trace::frontier`] reports the value most recently set with
/// `set_frontier`.
#[test]
fn test_fork_spine_metadata() {
    Runtime::run(CircuitConfig::with_workers(1), move |_parker| {
        let factories =
            <OrdValBatchFactories<DynI32, DynI32, u32, DynZWeight>>::new::<i32, i32, ZWeight>();

        let mut trace: Spine<OrdValBatch<DynI32, DynI32, u32, DynZWeight>> = Spine::new(
            &factories,
            Arc::new(String::from("Test")),
            TraceRole::Integral,
        );

        // A fresh spine has a minimal frontier, and the fork of an empty
        // spine is empty.
        assert_eq!(trace.frontier(), 0);
        assert!(trace.fork().consolidate().is_none());

        let mut erased_tuples = indexed_zset_tuples(vec![Tup2(Tup2(1, 2), 1)]);
        let batch = OrdValBatch::dyn_from_tuples(&factories, 0, &mut erased_tuples);
        TOKIO.block_on(trace.insert(batch));

        trace.set_frontier(&5);
        trace.retain_keys(Filter::new(Box::new(|key| {
            *key.downcast_checked::<i32>() >= 0
        })));
        trace.retain_values(GroupFilter::Simple(Filter::new(Box::new(
            |val: &DynI32| *val.downcast_checked::<i32>() >= 0,
        ))));

        let fork = trace.fork();
        assert_eq!(fork.frontier(), 5);
        assert!(fork.dirty());
        assert!(fork.key_filter().is_some());
        assert!(fork.value_filter().is_some());

        // The dirty flag is copied, not hardwired: a clean source produces a
        // clean fork.
        trace.clear_dirty_flag();
        assert!(!trace.fork().dirty());

        // The reference model honors the same frontier round-trip contract,
        // including across fork.
        let mut ref_trace: TestBatch<DynI32, DynI32, u32, DynZWeight> = TestBatch::new(
            &TestBatchFactories::new(),
            Arc::new(String::from("Test")),
            TraceRole::Integral,
        );
        assert_eq!(ref_trace.frontier(), 0);
        ref_trace.set_frontier(&5);
        assert_eq!(ref_trace.frontier(), 5);
        assert_eq!(ref_trace.fork().frontier(), 5);
    })
    .unwrap()
    .join()
    .unwrap();
}

/// Executes `f`, once, inside a circuit initialized so that it has access to
/// storage.
///
/// This is necessary because the batches in `crate::trace::ord::file` require
/// access to storage, which they get per-thread from a [Runtime], and which is
/// only available within a circuit initialized with storage.
pub(crate) fn run_in_circuit_with_storage<F>(f: F)
where
    F: FnOnce() + Clone + Send + 'static,
{
    let _temp_dir = tempdir().expect("Can't create temp dir for storage");
    run_in_circuit_with_storage_config(mkconfig(_temp_dir.path()), f);
}

/// Like [`run_in_circuit_with_storage`], but lets the caller supply the
/// `CircuitConfig` (e.g. to flip `dev_tweaks` flags). Storage-backing for
/// the config is the caller's responsibility.
pub(crate) fn run_in_circuit_with_storage_config<F>(config: CircuitConfig, f: F)
where
    F: FnOnce() + Clone + Send + 'static,
{
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

fn total_cache_accesses(stats: CacheStats) -> u64 {
    stats
        .0
        .iter()
        .map(|(_, accesses)| accesses.iter().map(|(_, counts)| counts.count).sum::<u64>())
        .sum()
}

fn build_file_wset_u32(keys: &[u32]) -> FileWSet<DynData, DynZWeight> {
    let factories = <FileWSetFactories<DynData, DynZWeight>>::new::<u32, (), ZWeight>();
    let mut builder =
        <FileWSet<DynData, DynZWeight> as Batch>::Builder::with_capacity(&factories, keys.len(), 0);

    for key in keys {
        let weight: ZWeight = 1;
        builder.push_time_diff(&(), weight.erase());
        builder.push_key(key.erase());
    }

    builder.done()
}

fn build_file_wset_tup1_i32(keys: &[i32]) -> FileWSet<DynData, DynZWeight> {
    let factories = <FileWSetFactories<DynData, DynZWeight>>::new::<Tup1<i32>, (), ZWeight>();
    let mut builder =
        <FileWSet<DynData, DynZWeight> as Batch>::Builder::with_capacity(&factories, keys.len(), 0);

    for key in keys {
        let weight: ZWeight = 1;
        builder.push_time_diff(&(), weight.erase());
        builder.push_key(Tup1(*key).erase());
    }

    builder.done()
}

fn build_fallback_wset_i32(keys: &[i32]) -> crate::trace::FallbackWSet<DynI32, DynZWeight> {
    let factories =
        <crate::trace::FallbackWSetFactories<DynI32, DynZWeight>>::new::<i32, (), ZWeight>();
    let mut erased_tuples = zset_tuples(keys.iter().copied().map(|key| Tup2(key, 1)).collect());
    crate::trace::FallbackWSet::<DynI32, DynZWeight>::dyn_from_tuples(
        &factories,
        (),
        &mut erased_tuples,
    )
}

#[test]
fn test_file_wset_roaring_u32_seek_key_exact_skips_absent_reads() {
    let _temp_dir = tempdir().expect("Can't create temp dir for storage");
    let mut config = mkconfig(_temp_dir.path());
    config.dev_tweaks.enable_roaring = Some(true);

    run_in_circuit_with_storage_config(config, move || {
        let batch = build_file_wset_u32(&[1, 3, 7]);
        let mut cursor = batch.cursor();
        let before = total_cache_accesses(batch.cache_stats());

        let missing = 2u32;
        assert!(!cursor.seek_key_exact(missing.erase(), None));
        assert_eq!(total_cache_accesses(batch.cache_stats()), before);

        let present = 3u32;
        assert!(cursor.seek_key_exact(present.erase(), None));
    });
}

#[test]
fn test_file_wset_tup1_i32_roaring_seek_key_exact_skips_absent_reads() {
    let _temp_dir = tempdir().expect("Can't create temp dir for storage");
    let mut config = mkconfig(_temp_dir.path());
    config.dev_tweaks.enable_roaring = Some(true);

    run_in_circuit_with_storage_config(config, move || {
        let batch = build_file_wset_tup1_i32(&[-7, 1, 3]);
        let mut cursor = batch.cursor();
        let before = total_cache_accesses(batch.cache_stats());

        let missing = Tup1(2i32);
        assert!(!cursor.seek_key_exact(missing.erase(), None));
        assert_eq!(total_cache_accesses(batch.cache_stats()), before);

        let present = Tup1(3i32);
        assert!(cursor.seek_key_exact(present.erase(), None));
    });
}

#[test]
fn test_file_wset_roaring_filter_rebuilt_after_merge() {
    let _temp_dir = tempdir().expect("Can't create temp dir for storage");
    let mut config = mkconfig(_temp_dir.path());
    config.dev_tweaks.enable_roaring = Some(true);

    run_in_circuit_with_storage_config(config, move || {
        let lhs = build_file_wset_u32(&[1, 5]);
        let rhs = build_file_wset_u32(&[3, 7]);
        let merged = lhs.add_by_ref(&rhs);

        let mut cursor = merged.cursor();
        let before = total_cache_accesses(merged.cache_stats());

        let missing = 4u32;
        assert!(!cursor.seek_key_exact(missing.erase(), None));
        assert_eq!(total_cache_accesses(merged.cache_stats()), before);

        let present = 7u32;
        assert!(cursor.seek_key_exact(present.erase(), None));
    });
}

#[test]
fn test_fallback_wset_roaring_filter_rebuilt_after_storage_merge() {
    let _temp_dir = tempdir().expect("Can't create temp dir for storage");
    let mut config = mkconfig(_temp_dir.path());
    config.dev_tweaks.enable_roaring = Some(true);
    config.storage.as_mut().unwrap().options.min_storage_bytes = Some(0);

    run_in_circuit_with_storage_config(config, move || {
        let lhs = build_fallback_wset_i32(&[1, 5]);
        let rhs = build_fallback_wset_i32(&[3, 7]);
        let factories =
            <crate::trace::FallbackWSetFactories<DynI32, DynZWeight>>::new::<i32, (), ZWeight>();
        let merged: crate::trace::FallbackWSet<DynI32, DynZWeight> = ListMerger::merge(
            &factories,
            <crate::trace::FallbackWSet<DynI32, DynZWeight> as Batch>::Builder::for_merge(
                &factories,
                [&lhs, &rhs],
                Some(BatchLocation::Storage),
            ),
            vec![lhs.merge_cursor(None, None), rhs.merge_cursor(None, None)],
        );

        assert_eq!(merged.membership_filter_kind(), FilterKind::Roaring);

        let mut cursor = merged.cursor();
        let before = total_cache_accesses(merged.cache_stats());

        let missing = 4i32;
        assert!(!cursor.seek_key_exact(missing.erase(), None));
        assert_eq!(total_cache_accesses(merged.cache_stats()), before);
    });
}

/// Builds a `FallbackIndexedWSet` over `(key, value, weight)` triples.
fn build_fallback_indexed_wset_i32(
    tuples: Vec<Tup2<Tup2<i32, i32>, ZWeight>>,
) -> crate::trace::FallbackIndexedWSet<DynI32, DynI32, DynZWeight> {
    let factories = <crate::trace::FallbackIndexedWSetFactories<DynI32, DynI32, DynZWeight>>::new::<
        i32,
        i32,
        ZWeight,
    >();
    let mut erased = indexed_zset_tuples(tuples);
    crate::trace::FallbackIndexedWSet::<DynI32, DynI32, DynZWeight>::dyn_from_tuples(
        &factories,
        (),
        &mut erased,
    )
}

/// Builds a `FallbackIndexedWSet` and re-routes it to the requested storage
/// tier, regardless of the runtime's `min_step_storage_bytes` default
/// that gives more control over batch location.
fn build_fallback_indexed_wset_i32_at(
    tuples: Vec<Tup2<Tup2<i32, i32>, ZWeight>>,
    location: BatchLocation,
) -> crate::trace::FallbackIndexedWSet<DynI32, DynI32, DynZWeight> {
    let factories = <crate::trace::FallbackIndexedWSetFactories<DynI32, DynI32, DynZWeight>>::new::<
        i32,
        i32,
        ZWeight,
    >();
    let initial = build_fallback_indexed_wset_i32(tuples);
    let builder = <crate::trace::FallbackIndexedWSet<
        DynI32,
        DynI32,
        DynZWeight,
    > as Batch>::Builder::for_merge(&factories, [&initial], Some(location));
    ListMerger::merge(&factories, builder, vec![initial.merge_cursor(None, None)])
}

/// A worker's storage system calls are charged to their own reasons, and only
/// for the part the thread spends off the CPU: a buffered write or a cached
/// read runs in the kernel on this thread, where the step's CPU time already
/// counts it, so charging the whole call would put one interval in two numbers
/// that divide the step's wall clock between them.
///
/// The backend's simulated device latency stands in for the part that really
/// does block, which is what makes the read and the write measurable here; a
/// real fsync blocks without any help.
#[test]
fn storage_calls_are_charged_to_the_worker_for_their_time_off_the_cpu() {
    /// Blocks per layer file, enough to pass the writer's 1 MiB buffer and so
    /// to spill before the file is finished.
    const BLOCKS: usize = 1024 * 1024 / BLOCK_SIZE + 1;
    const BLOCK_SIZE: usize = 4096;
    const IO_DELAY: std::time::Duration = std::time::Duration::from_millis(10);

    let temp_dir = tempdir().expect("Can't create temp dir for storage");
    run_in_circuit_with_storage_config(slow_read_storage_config(temp_dir.path(), IO_DELAY), || {
        use crate::circuit::ThreadCpuTime;
        use std::time::Instant;

        let idle = crate::profile::current_runtime_idle()
            .expect("a worker thread carries its runtime's accumulator");
        let read = crate::profile::ParkReason::StorageRead as usize;
        let write = crate::profile::ParkReason::StorageWrite as usize;
        let sync = crate::profile::ParkReason::StorageSync as usize;

        /// What a call may be charged: no more than its thread spent off the
        /// CPU, plus a slack for the clock reads around it.
        ///
        /// The CPU clock is read first, so that a thread descheduled between
        /// the two reads widens this bound rather than narrowing it.
        fn off_cpu(wall: Instant, cpu: ThreadCpuTime) -> std::time::Duration {
            let cpu = cpu.elapsed();
            wall.elapsed().saturating_sub(cpu) + std::time::Duration::from_millis(1)
        }

        // Straight to the backend: the buffer cache sits above it, and a
        // cached block would never reach `pread`.
        let backend = crate::Runtime::storage_backend().expect("storage is configured");
        let before = idle.by_reason();

        let mut writer = backend.create().expect("create a layer file");
        let wall = Instant::now();
        let cpu = ThreadCpuTime::now();
        for _ in 0..BLOCKS {
            let mut block = crate::storage::buffer_cache::FBuf::with_capacity(BLOCK_SIZE);
            block.resize(BLOCK_SIZE, 7);
            writer.write_block(block).expect("write a block");
        }
        let after_write = idle.by_reason();
        let written = after_write[write] - before[write];
        assert!(
            written >= IO_DELAY / 2,
            "spilling blocks waited {written:?}"
        );
        assert!(
            written <= off_cpu(wall, cpu),
            "spilling blocks charged {written:?} of its time off the CPU"
        );
        assert_eq!(
            after_write[sync], before[sync],
            "spilling blocks fsyncs nothing"
        );

        // Finishing a file renames it and no longer syncs it: durability is
        // the committer's job, which is what keeps the fsync off the threads
        // that write files.
        let reader = writer.complete().expect("finish the file");
        let after_complete = idle.by_reason();
        assert_eq!(
            after_complete[sync], after_write[sync],
            "finishing a file fsyncs nothing"
        );

        let wall = Instant::now();
        let cpu = ThreadCpuTime::now();
        reader.commit().expect("commit the file");
        let after_sync = idle.by_reason();
        let synced = after_sync[sync] - after_complete[sync];
        assert!(synced > Duration::ZERO, "committing a file fsyncs it");
        assert!(
            synced <= off_cpu(wall, cpu),
            "committing a file charged {synced:?} of its time off the CPU"
        );
        assert_eq!(
            after_sync[read], before[read],
            "finishing and committing a file reads nothing"
        );

        let wall = Instant::now();
        let cpu = ThreadCpuTime::now();
        reader
            .read_block(feldera_storage::block::BlockLocation::new(0, BLOCK_SIZE).unwrap())
            .expect("read the block back");
        let after_read = idle.by_reason();
        let waited = after_read[read] - after_sync[read];
        assert!(waited >= IO_DELAY / 2, "a block read waited {waited:?}");
        assert!(
            waited <= off_cpu(wall, cpu),
            "a block read charged {waited:?} of its time off the CPU"
        );
        assert_eq!(
            after_read[sync], after_sync[sync],
            "a block read fsyncs nothing"
        );

        assert_eq!(
            idle.by_reason().iter().sum::<std::time::Duration>(),
            idle.total(),
            "the breakdown adds up to the total it splits"
        );
    });
}

/// A buffered write is not idle time: it runs in the kernel on this thread,
/// where the step's CPU time already counts it.  Writing enough to make that
/// CPU time dominate pins the difference, which a call that also blocks, or one
/// the backend delays on purpose, would hide.
#[test]
fn a_buffered_write_is_charged_only_for_its_time_off_the_cpu() {
    const BLOCK_SIZE: usize = 4096;
    const BLOCKS: usize = 8 * 1024; // 32 MiB, enough for the kernel's copy to dominate

    run_in_circuit_with_storage(|| {
        use crate::circuit::ThreadCpuTime;
        use std::time::Instant;

        let idle = crate::profile::current_runtime_idle()
            .expect("a worker thread carries its runtime's accumulator");
        let write = crate::profile::ParkReason::StorageWrite as usize;

        let backend = crate::Runtime::storage_backend().expect("storage is configured");
        let mut writer = backend.create().expect("create a layer file");
        let before = idle.by_reason();
        let wall = Instant::now();
        let cpu = ThreadCpuTime::now();
        for _ in 0..BLOCKS {
            let mut block = crate::storage::buffer_cache::FBuf::with_capacity(BLOCK_SIZE);
            block.resize(BLOCK_SIZE, 7);
            writer.write_block(block).expect("write a block");
        }
        // The CPU clock is read first, so that a thread descheduled between the
        // two reads widens the bound rather than narrowing it.
        let cpu = cpu.elapsed();
        let wall = wall.elapsed();
        let charged = idle.by_reason()[write] - before[write];

        // On an idle machine the kernel's copy accounts for nearly all of this,
        // which is what makes the bound tight enough to catch the whole call
        // being charged.  A loaded node deschedules the thread instead, which
        // is time the guard may charge, so the bound loosens rather than the
        // test failing.
        assert!(
            charged <= wall.saturating_sub(cpu) + Duration::from_millis(1),
            "writing {BLOCKS} blocks charged {charged:?} as wait, of {wall:?} with {cpu:?} on the CPU"
        );
    });
}

/// Creating, finishing and dropping a layer file costs a system call apiece
/// that moves no data: `open`, `rename`, `unlink`.  Locally each of those runs
/// on the CPU and so is charged almost nothing, but on storage that blocks
/// they are exactly the calls whose time would otherwise go missing.  What is
/// pinned here is that they never charge more than their thread spent off the
/// CPU.
#[test]
fn storage_metadata_calls_charge_no_more_than_their_time_off_the_cpu() {
    run_in_circuit_with_storage(|| {
        use crate::circuit::ThreadCpuTime;
        use std::time::Instant;

        let idle = crate::profile::current_runtime_idle()
            .expect("a worker thread carries its runtime's accumulator");
        let metadata = crate::profile::ParkReason::StorageMetadata as usize;

        let backend = crate::Runtime::storage_backend().expect("storage is configured");
        let before = idle.by_reason();
        let wall = Instant::now();
        let cpu = ThreadCpuTime::now();

        let writer = backend.create().expect("create a layer file");
        let reader = writer.complete().expect("finish the file");
        // The file was never marked to keep, so the last reference unlinks it.
        drop(reader);

        let charged = idle.by_reason()[metadata] - before[metadata];
        // The CPU clock is read first; see `off_cpu` above.
        let cpu = cpu.elapsed();
        let off_cpu = wall.elapsed().saturating_sub(cpu) + Duration::from_millis(1);
        assert!(
            charged <= off_cpu,
            "opening, renaming and unlinking a file charged {charged:?} of {off_cpu:?} off the CPU"
        );
    });
}

/// Storage whose reads sleep, so that a test observes a wait rather than
/// racing a read that has already finished.
fn slow_read_storage_config(
    path: &std::path::Path,
    read_delay: std::time::Duration,
) -> CircuitConfig {
    use feldera_types::config::{FileBackendConfig, StorageBackendConfig};

    let mut config = CircuitConfig::with_workers(1).with_storage(Some(
        crate::circuit::CircuitStorageConfig::for_config(
            crate::circuit::StorageConfig {
                path: path.to_string_lossy().into_owned(),
                cache: crate::circuit::StorageCacheConfig::default(),
            },
            crate::circuit::StorageOptions {
                min_storage_bytes: Some(0),
                backend: StorageBackendConfig::File(Box::new(FileBackendConfig {
                    ioop_delay: Some(read_delay.as_millis() as u64),
                    ..Default::default()
                })),
                ..Default::default()
            },
        )
        .unwrap(),
    ));
    // Writing a file leaves its blocks in the buffer cache, which sits above
    // the backend, so a reader has to be able to evict them to reach storage.
    config.dev_tweaks.eager_evict = Some(true);
    config
}

/// `fetch` hands its block reads to the blocking pool, where no accumulator is
/// installed, and awaits them.  The worker's runtime parks with no system call
/// for the park hook to look at, so the wait has to name the read itself or it
/// lands in the breakdown's catch-all.
#[test]
fn a_fetch_waiting_for_a_block_read_declares_the_read() {
    use std::{
        future::Future,
        task::{Context, Waker},
    };

    // Long enough that the read cannot finish between issuing it and parking
    // for it, even on a node loaded enough to deschedule this thread in
    // between.
    const READ_DELAY: std::time::Duration = std::time::Duration::from_millis(200);

    let temp_dir = tempdir().expect("Can't create temp dir for storage");
    run_in_circuit_with_storage_config(
        slow_read_storage_config(temp_dir.path(), READ_DELAY),
        || {
            let keys = (0..4096u32).collect::<Vec<_>>();
            let batch = build_file_wset_u32(&keys);
            batch.evict();
            // Left in the cache: the fetch walks it before it issues any read,
            // and the test is about the wait for the read.
            let wanted = build_file_wset_u32(&keys);

            let mut fetch = Box::pin(batch.fetch(&wanted));
            let mut context = Context::from_waker(Waker::noop());

            assert!(
                fetch.as_mut().poll(&mut context).is_pending(),
                "the fetch finished before it had to wait for a block"
            );
            assert_eq!(
                crate::profile::current_park_reason(),
                crate::profile::ParkReason::StorageRead
            );
        },
    );
}
/// Under a zero step threshold, which is what Critical memory pressure
/// imposes, a builder that receives nothing must finish in memory.  It has
/// nothing to spill, and a layer file costs two fsyncs on the way out, which a
/// backfill paid for every step whose output was empty.  A builder that
/// receives anything still spills, at its first item.
#[test]
fn an_empty_builder_stays_in_memory_under_a_zero_threshold() {
    let dir = tempdir().expect("temp dir");
    let config = CircuitConfig::with_workers(1).with_storage(Some(
        crate::circuit::CircuitStorageConfig::for_config(
            crate::circuit::StorageConfig {
                path: dir.path().to_string_lossy().into_owned(),
                cache: crate::circuit::StorageCacheConfig::default(),
            },
            crate::circuit::StorageOptions {
                min_storage_bytes: Some(0),
                min_step_storage_bytes: Some(0),
                ..Default::default()
            },
        )
        .unwrap(),
    ));
    run_in_circuit_with_storage_config(config, || {
        // The capacity is what the lazy input map's per-step builder asks for,
        // and at 32 estimated bytes per slot it clears any positive threshold.
        const CAPACITY: usize = 10_000;

        let factories =
            <crate::trace::FallbackIndexedWSetFactories<DynI32, DynI32, DynZWeight>>::new::<
                i32,
                i32,
                ZWeight,
            >();
        type Indexed =
            <crate::trace::FallbackIndexedWSet<DynI32, DynI32, DynZWeight> as Batch>::Builder;
        let empty = Indexed::with_capacity(&factories, CAPACITY, CAPACITY).done();
        assert_eq!(
            empty.location(),
            BatchLocation::Memory,
            "an empty indexed batch went to storage"
        );

        let (key, val, weight): (i32, i32, ZWeight) = (1, 7, 1);
        let mut one = Indexed::with_capacity(&factories, CAPACITY, CAPACITY);
        one.push_time_diff(&(), weight.erase());
        one.push_val(val.erase());
        one.push_key(key.erase());
        assert_eq!(
            one.done().location(),
            BatchLocation::Storage,
            "an indexed batch with content stayed in memory under a zero threshold"
        );

        let factories =
            <crate::trace::FallbackWSetFactories<DynI32, DynZWeight>>::new::<i32, (), ZWeight>();
        type Plain = <crate::trace::FallbackWSet<DynI32, DynZWeight> as Batch>::Builder;
        let empty = Plain::with_capacity(&factories, CAPACITY, CAPACITY).done();
        assert_eq!(
            empty.location(),
            BatchLocation::Memory,
            "an empty batch went to storage"
        );

        let mut one = Plain::with_capacity(&factories, CAPACITY, CAPACITY);
        one.push_time_diff(&(), weight.erase());
        one.push_val(().erase());
        one.push_key(key.erase());
        assert_eq!(
            one.done().location(),
            BatchLocation::Storage,
            "a batch with content stayed in memory under a zero threshold"
        );
    });
}

/// A builder that starts in memory under a threshold reserves room for what
/// it can hold before it spills, not for the caller's whole capacity guess.
/// Under a zero threshold every item spills at once, so a guess of a million
/// rows must not cost a million slots of memory first.
#[test]
fn a_threshold_builder_reserves_no_more_than_the_threshold() {
    let dir = tempdir().expect("temp dir");
    let config = CircuitConfig::with_workers(1).with_storage(Some(
        crate::circuit::CircuitStorageConfig::for_config(
            crate::circuit::StorageConfig {
                path: dir.path().to_string_lossy().into_owned(),
                cache: crate::circuit::StorageCacheConfig::default(),
            },
            crate::circuit::StorageOptions {
                min_storage_bytes: Some(0),
                min_step_storage_bytes: Some(0),
                ..Default::default()
            },
        )
        .unwrap(),
    ));
    run_in_circuit_with_storage_config(config, || {
        const CAPACITY: usize = 1_000_000;
        // Room for the bookkeeping of an empty builder, far below what a
        // million slots of keys, values and weights would take.
        const ALLOWANCE: usize = 64 * 1024;

        let factories =
            <crate::trace::FallbackIndexedWSetFactories<DynI32, DynI32, DynZWeight>>::new::<
                i32,
                i32,
                ZWeight,
            >();
        type Indexed =
            <crate::trace::FallbackIndexedWSet<DynI32, DynI32, DynZWeight> as Batch>::Builder;
        let reserved = Indexed::with_capacity(&factories, CAPACITY, CAPACITY)
            .size_of()
            .total_bytes();
        assert!(
            reserved < ALLOWANCE,
            "an indexed builder under a zero threshold holds {reserved} bytes for a guess of {CAPACITY} rows"
        );

        let factories =
            <crate::trace::FallbackWSetFactories<DynI32, DynZWeight>>::new::<i32, (), ZWeight>();
        type Plain = <crate::trace::FallbackWSet<DynI32, DynZWeight> as Batch>::Builder;
        let reserved = Plain::with_capacity(&factories, CAPACITY, CAPACITY)
            .size_of()
            .total_bytes();
        assert!(
            reserved < ALLOWANCE,
            "a builder under a zero threshold holds {reserved} bytes for a guess of {CAPACITY} rows"
        );
    });
}

/// Critical memory pressure is what imposes the zero threshold in practice:
/// `Runtime::min_step_storage_bytes()` answers zero under it whatever the
/// configured value.  An empty builder still finishes in memory, one that
/// receives an item spills, and neither reserves room for its capacity guess.
#[test]
fn critical_memory_pressure_keeps_an_empty_builder_in_memory() {
    const GIB: u64 = 1 << 30;
    // 9.6 GiB of a 10 GiB limit is past the 95% where Critical starts.
    with_mock_process_rss(96 * GIB / 10, || {
        let dir = tempdir().expect("temp dir");
        let config = CircuitConfig::with_workers(1)
            .with_temporary_storage(dir.path())
            .with_max_rss_bytes(Some(10 * GIB));
        run_in_circuit_with_storage_config(config, || {
            // The runtime samples the process size once a second.
            let deadline = Instant::now() + Duration::from_secs(30);
            while Runtime::memory_pressure() != Some(MemoryPressure::Critical) {
                assert!(
                    Instant::now() < deadline,
                    "memory pressure stayed at {:?}",
                    Runtime::memory_pressure()
                );
                sleep(Duration::from_millis(100));
            }
            assert_eq!(Runtime::min_step_storage_bytes(), Some(0));

            const CAPACITY: usize = 1_000_000;
            const ALLOWANCE: usize = 64 * 1024;
            let factories =
                <crate::trace::FallbackIndexedWSetFactories<DynI32, DynI32, DynZWeight>>::new::<
                    i32,
                    i32,
                    ZWeight,
                >();
            type Indexed =
                <crate::trace::FallbackIndexedWSet<DynI32, DynI32, DynZWeight> as Batch>::Builder;
            let empty = Indexed::with_capacity(&factories, CAPACITY, CAPACITY);
            let reserved = empty.size_of().total_bytes();
            assert!(
                reserved < ALLOWANCE,
                "a builder under Critical pressure holds {reserved} bytes for a guess of {CAPACITY} rows"
            );
            assert_eq!(
                empty.done().location(),
                BatchLocation::Memory,
                "an empty batch went to storage under Critical pressure"
            );

            let (key, val, weight): (i32, i32, ZWeight) = (1, 7, 1);
            let mut one = Indexed::with_capacity(&factories, CAPACITY, CAPACITY);
            one.push_time_diff(&(), weight.erase());
            one.push_val(val.erase());
            one.push_key(key.erase());
            assert_eq!(
                one.done().location(),
                BatchLocation::Storage,
                "a batch with content stayed in memory under Critical pressure"
            );
        });
    });
}

/// Strategy for the storage tier of a single proptest input batch.
///
/// Each batch independently picks `Memory` or `Storage` so a single test
/// case typically runs the merger on a mix of cursor types.
/// Bias toward `Storage`.
fn batch_location_strategy() -> impl Strategy<Value = BatchLocation> {
    prop_oneof![
        2 => Just(BatchLocation::Storage),
        1 => Just(BatchLocation::Memory),
    ]
}

/// Strategy that produces a single batch's worth of `(key, value, weight)`
/// triples with i32 keys spanning negative and positive ranges.
fn merge_proptest_batch(max_tuples: usize) -> BoxedStrategy<Vec<Tup2<Tup2<i32, i32>, ZWeight>>> {
    vec(
        (-150_000..150_000i32, 0..32i32, -3..=3i64).prop_map(|(k, v, w)| Tup2(Tup2(k, v), w)),
        0..=max_tuples,
    )
    .boxed()
}

/// Dense strategy: keys in `0..200` with up to 80 tuples per batch. Designed
/// to maximize per-batch overlap and weight cancellation between merge inputs.
fn merge_proptest_batch_dense(
    max_tuples: usize,
) -> BoxedStrategy<Vec<Tup2<Tup2<i32, i32>, ZWeight>>> {
    vec(
        (0..200i32, 0..16i32, -3..=3i64).prop_map(|(k, v, w)| Tup2(Tup2(k, v), w)),
        0..=max_tuples,
    )
    .boxed()
}

/// Which membership-filter strategies the runtime is allowed to choose from.
/// The merger picks one of these per output batch via `FilterPlan::decide_filter`.
#[derive(Copy, Clone, Debug)]
enum FilterConfig {
    /// Bloom only: roaring disabled.
    BloomOnly,
    /// Roaring only: bloom disabled (false-positive rate forced to zero).
    RoaringOnly,
    /// Both enabled: the lookup predictor picks per output batch.
    Both,
    /// Neither enabled: file batches are written without a membership filter,
    /// so `seek_key_exact` always reads the data block.
    Neither,
}

impl FilterConfig {
    /// Sets the dev tweaks that select this filter configuration.
    fn apply(self, config: &mut CircuitConfig) {
        match self {
            Self::BloomOnly => {
                config.dev_tweaks.enable_roaring = Some(false);
            }
            Self::RoaringOnly => {
                config.dev_tweaks.enable_roaring = Some(true);
                config.dev_tweaks.bloom_false_positive_rate = Some(0.0);
            }
            Self::Both => {
                config.dev_tweaks.enable_roaring = Some(true);
            }
            Self::Neither => {
                config.dev_tweaks.enable_roaring = Some(false);
                config.dev_tweaks.bloom_false_positive_rate = Some(0.0);
            }
        }
    }

    /// Filter kinds the merged batch is allowed to end up with for this
    /// configuration. `None` is always allowed because empty merges or merges
    /// without a usable bounds plan get no filter.
    fn allowed_filter_kinds(self) -> &'static [FilterKind] {
        match self {
            Self::BloomOnly => &[FilterKind::None, FilterKind::Bloom],
            Self::RoaringOnly => &[FilterKind::None, FilterKind::Roaring],
            Self::Both => &[FilterKind::None, FilterKind::Bloom, FilterKind::Roaring],
            Self::Neither => &[FilterKind::None],
        }
    }
}

/// Per-input batch shape: the tuples to feed in plus the storage tier the
/// input batch should reside in before the merger reads it.
type MergeInputBatches = Vec<(Vec<Tup2<Tup2<i32, i32>, ZWeight>>, BatchLocation)>;

/// Asserts that `merged.cursor().seek_key_exact` agrees with `expected` for
/// every key present in `expected` and for every probe key in `extra_probes`.
///
/// Each probe runs on a fresh cursor in an order shuffled with a fixed
/// seed, so we exercise the membership-filter and data-block lookup paths
/// from a clean cursor state rather than reusing a single forward-walking
/// cursor.
fn assert_seek_key_exact_matches<B>(
    merged: &B,
    expected: &TestBatch<DynI32, DynI32, (), DynZWeight>,
    extra_probes: impl IntoIterator<Item = i32>,
    seed: u64,
) where
    B: BatchReader<Key = DynI32, Val = DynI32, Time = (), R = DynZWeight>,
{
    use rand::SeedableRng;
    use rand::seq::SliceRandom;
    use rand_chacha::ChaChaRng;
    use std::collections::BTreeSet;

    let mut present: BTreeSet<i32> = BTreeSet::new();
    let mut probe = expected.cursor();
    while probe.key_valid() {
        present.insert(*probe.key().downcast_checked::<i32>());
        probe.step_key();
    }

    // Deduplicate keys, then collect into a vector we can shuffle.
    let probe_set: BTreeSet<i32> = present.iter().copied().chain(extra_probes).collect();
    let mut probes: Vec<i32> = probe_set.into_iter().collect();
    let mut rng = ChaChaRng::seed_from_u64(seed);
    probes.shuffle(&mut rng);

    for &k in &probes {
        let mut cursor = merged.cursor();
        let want = present.contains(&k);
        let got = cursor.seek_key_exact(k.erase(), None);
        assert_eq!(
            got, want,
            "seek_key_exact({k}) on a fresh cursor: expected {want}, got {got}",
        );
    }
}

/// A cursor over a file-backed batch offers its key as it is stored, and one
/// over a vec-backed batch does not.
///
/// This is what lets a merge compare and copy keys without decoding them, so
/// the archived key has to be the same key: the merge would otherwise order
/// its output by something other than what it wrote.
#[test]
fn a_file_cursor_offers_the_same_key_archived() {
    use std::cmp::Ordering;

    let _temp_dir = tempdir().expect("Can't create temp dir for storage");
    let mut config = mkconfig(_temp_dir.path());
    config.storage.as_mut().unwrap().options.min_storage_bytes = Some(0);

    run_in_circuit_with_storage_config(config, move || {
        // Keys that share bytes and differ in length once archived, so that a
        // comparison has to read past the first of them.
        let tuples: Vec<Tup2<Tup2<i32, i32>, ZWeight>> =
            (0..400i32).map(|i| Tup2(Tup2(i, i * 3), 1)).collect();

        for location in [BatchLocation::Memory, BatchLocation::Storage] {
            let batch = build_fallback_indexed_wset_i32_at(tuples.clone(), location);
            let mut cursor = batch.merge_cursor(None, None);
            let mut keys = 0;
            while cursor.key_valid() {
                match cursor.archived_key() {
                    Some(archived) => {
                        assert_eq!(
                            location,
                            BatchLocation::Storage,
                            "a vec-backed batch has nothing archived to offer",
                        );
                        assert_eq!(
                            archived.cmp_target(cursor.key()),
                            Ordering::Equal,
                            "key {keys}: the archived key is a different key from the decoded one",
                        );
                    }
                    None => assert_eq!(
                        location,
                        BatchLocation::Memory,
                        "a file-backed batch should offer its key archived",
                    ),
                }
                keys += 1;
                cursor.step_key();
            }
            assert_eq!(keys, tuples.len(), "at {location:?}");
        }
    });
}

/// A merge of file-backed batches copies values instead of rewriting them.
///
/// The correctness of what it writes is the business of the
/// `indexed_wset_storage_merges_*` proptests, which pass either way. This
/// says the fast path is the one taken, which they cannot: a splice that
/// stopped engaging would leave them green and every merge slow.
#[test]
fn a_merge_splices_values_it_does_not_have_to_decode() {
    use crate::trace::ord::file::indexed_wset_batch::SPLICED_VALUES;
    use std::sync::atomic::Ordering;

    let _temp_dir = tempdir().expect("Can't create temp dir for storage");
    let mut config = mkconfig(_temp_dir.path());
    config.storage.as_mut().unwrap().options.min_storage_bytes = Some(0);

    run_in_circuit_with_storage_config(config, move || {
        // Several values a key, since a run of one is the case where copying
        // has the least to save and the most to prove.
        // One value a key, which is what a table with a primary key has and
        // the shape a merge sees most. `splice_copy` measures copying at 24%
        // ahead of rewriting even here, so this is the case worth guarding.
        let left: Vec<Tup2<Tup2<i32, i32>, ZWeight>> =
            (0..400i32).map(|k| Tup2(Tup2(k * 2, k), 1)).collect();
        let right: Vec<Tup2<Tup2<i32, i32>, ZWeight>> =
            (0..400i32).map(|k| Tup2(Tup2(k * 2 + 1, k), 1)).collect();

        let factories =
            <crate::trace::FallbackIndexedWSetFactories<DynI32, DynI32, DynZWeight>>::new::<
                i32,
                i32,
                ZWeight,
            >();
        let inputs: Vec<_> = [left, right]
            .into_iter()
            .map(|t| build_fallback_indexed_wset_i32_at(t, BatchLocation::Storage))
            .collect();
        let input_refs: Vec<&_> = inputs.iter().collect();
        let builder = <crate::trace::FallbackIndexedWSet<DynI32, DynI32, DynZWeight> as Batch>::Builder::for_merge(
            &factories,
            input_refs,
            Some(BatchLocation::Storage),
        );
        let cursors: Vec<_> = inputs.iter().map(|b| b.merge_cursor(None, None)).collect();

        let before = SPLICED_VALUES.load(Ordering::Relaxed);
        let merged: crate::trace::FallbackIndexedWSet<DynI32, DynI32, DynZWeight> =
            ListMerger::merge(&factories, builder, cursors);
        let spliced = SPLICED_VALUES.load(Ordering::Relaxed) - before;

        assert_eq!(merged.approx_key_count(), 800);
        assert_eq!(merged.approx_len(), 800);
        assert!(
            spliced > 0,
            "the merge decoded and rewrote every value; nothing was copied",
        );
    });
}

/// A key long enough to hold its text out of line, so that decoding one
/// allocates: that is what copying a key saves, and a key that fitted inline
/// would understate it.
fn long_string_key(k: i32) -> String {
    format!("key-{k:08}-{}", "y".repeat((k % 19) as usize))
}

/// A merge of file-backed batches copies keys instead of rewriting them.
///
/// The same guard as `a_merge_splices_values_it_does_not_have_to_decode`, for
/// the key column, and it needs a key type of its own: the keys here are
/// strings, which is where copying a key pays, since decoding one allocates.
/// An integer key is the kind a roaring filter is built for, and that filter
/// is fed the key itself, so an integer-keyed merge writes its keys the
/// ordinary way and would leave this test green while proving nothing.
#[test]
fn a_merge_splices_keys_it_does_not_have_to_decode() {
    use crate::trace::ord::file::indexed_wset_batch::SPLICED_KEYS;
    use std::sync::atomic::Ordering;

    type Keys = crate::trace::FallbackIndexedWSet<DynData, DynI32, DynZWeight>;
    type KeyFactories = crate::trace::FallbackIndexedWSetFactories<DynData, DynI32, DynZWeight>;

    let _temp_dir = tempdir().expect("Can't create temp dir for storage");
    let mut config = mkconfig(_temp_dir.path());
    config.storage.as_mut().unwrap().options.min_storage_bytes = Some(0);

    run_in_circuit_with_storage_config(config, move || {
        let factories = <KeyFactories>::new::<String, i32, ZWeight>();
        let build = |tuples: Vec<Tup2<Tup2<String, i32>, ZWeight>>| {
            let mut erased: Box<DynWeightedPairs<DynPair<DynData, DynI32>, DynZWeight>> =
                Box::new(LeanVec::from(tuples)).erase_box();
            let initial = Keys::dyn_from_tuples(&factories, (), &mut erased);
            let builder = <Keys as Batch>::Builder::for_merge(
                &factories,
                [&initial],
                Some(BatchLocation::Storage),
            );
            ListMerger::merge(&factories, builder, vec![initial.merge_cursor(None, None)])
        };

        let key = long_string_key;
        let inputs: Vec<Keys> = [0, 1]
            .into_iter()
            .map(|half| {
                build(
                    (0..400i32)
                        .map(|k| Tup2(Tup2(key(k * 2 + half), k), 1))
                        .collect(),
                )
            })
            .collect();

        let input_refs: Vec<&Keys> = inputs.iter().collect();
        let builder = <Keys as Batch>::Builder::for_merge(
            &factories,
            input_refs,
            Some(BatchLocation::Storage),
        );
        let cursors: Vec<_> = inputs.iter().map(|b| b.merge_cursor(None, None)).collect();

        let before = SPLICED_KEYS.load(Ordering::Relaxed);
        let merged: Keys = ListMerger::merge(&factories, builder, cursors);
        let spliced = SPLICED_KEYS.load(Ordering::Relaxed) - before;

        assert_eq!(merged.approx_key_count(), 800);
        assert_eq!(merged.approx_len(), 800);
        assert!(
            spliced > 0,
            "the merge decoded and rewrote every key; nothing was copied",
        );

        // What was written has to be what the inputs held, key for key.
        let mut cursor = merged.cursor();
        for k in 0..800i32 {
            let want = key(k);
            assert_eq!(
                unsafe { cursor.key().downcast::<String>() },
                &want,
                "key {k} came back changed",
            );
            cursor.step_key();
        }
    });
}

/// Two string-keyed batches to merge, overlapping often enough that a key is
/// as likely to be shared as held alone.
///
/// The weights are mostly positive, because a batch holding a negative weight
/// offers no values to copy -- the count of them is metadata a copy never
/// reads -- and a generator that reached for negatives often would rarely
/// exercise the copy at all.  The few that appear are there to make keys
/// cancel, which is the case where a merge writes values decoded and so must
/// write its key decoded too.
fn string_keyed_tuples() -> impl Strategy<Value = Vec<Tup2<Tup2<String, i32>, ZWeight>>> {
    prop::collection::vec(
        (
            0..40i32,
            0..3i32,
            prop_oneof![9 => Just(1 as ZWeight), 3 => Just(2), 1 => Just(-1)],
        ),
        0..60,
    )
    .prop_map(|tuples| {
        tuples
            .into_iter()
            .map(|(k, v, w)| Tup2(Tup2(long_string_key(k), v), w))
            .collect()
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// A merge of string-keyed batches says what summing their tuples says.
    ///
    /// The `indexed_wset_storage_merges_*` proptests check the same thing for
    /// integer keys, which a merge always writes decoded.  A string key is
    /// one a merge copies, and a copied key names the rows its values
    /// occupied in the batch it came from: this is what says those are the
    /// rows that went in, over inputs that mix keys one batch holds alone
    /// with keys both hold, whose values are summed and written decoded.
    #[test]
    fn string_keyed_storage_merges_match_their_tuples(
        left in string_keyed_tuples(),
        right in string_keyed_tuples(),
    ) {
        type Keys = crate::trace::FallbackIndexedWSet<DynData, DynI32, DynZWeight>;
        type KeyFactories = crate::trace::FallbackIndexedWSetFactories<DynData, DynI32, DynZWeight>;

        let _temp_dir = tempdir().expect("Can't create temp dir for storage");
        let mut config = mkconfig(_temp_dir.path());
        config.storage.as_mut().unwrap().options.min_storage_bytes = Some(0);

        run_in_circuit_with_storage_config(config, move || {
            let factories = <KeyFactories>::new::<String, i32, ZWeight>();
            let build = |tuples: Vec<Tup2<Tup2<String, i32>, ZWeight>>| {
                let mut erased: Box<DynWeightedPairs<DynPair<DynData, DynI32>, DynZWeight>> =
                    Box::new(LeanVec::from(tuples)).erase_box();
                let initial = Keys::dyn_from_tuples(&factories, (), &mut erased);
                let builder = <Keys as Batch>::Builder::for_merge(
                    &factories,
                    [&initial],
                    Some(BatchLocation::Storage),
                );
                ListMerger::merge(&factories, builder, vec![initial.merge_cursor(None, None)])
            };

            let inputs: Vec<Keys> = [left.clone(), right.clone()].into_iter().map(build).collect();
            let input_refs: Vec<&Keys> = inputs.iter().collect();
            let builder = <Keys as Batch>::Builder::for_merge(
                &factories,
                input_refs,
                Some(BatchLocation::Storage),
            );
            let cursors: Vec<_> = inputs.iter().map(|b| b.merge_cursor(None, None)).collect();
            let merged: Keys = ListMerger::merge(&factories, builder, cursors);

            let mut union: Vec<Tup2<Tup2<String, i32>, ZWeight>> =
                left.iter().chain(right.iter()).cloned().collect();
            let mut expected_erased: Box<
                DynWeightedPairs<DynPair<DynData, DynI32>, DynZWeight>,
            > = Box::new(LeanVec::from(std::mem::take(&mut union))).erase_box();
            let expected: TestBatch<DynData, DynI32, (), DynZWeight> =
                TestBatch::dyn_from_tuples(&TestBatchFactories::new(), (), &mut expected_erased);

            assert_eq!(merged.location(), BatchLocation::Storage);
            assert_batch_eq(&merged, &expected);
        });
    }
}

/// Shared body for `indexed_wset_storage_merges_*` proptests. Generates
/// inputs as a vec/file mix, runs `ListMerger::merge` to file storage, and
/// validates the merged batch against a `TestBatch` reference. The input
/// batches are reduced by an optional key filter `retain_above`.
fn run_indexed_wset_storage_merges(
    batches: MergeInputBatches,
    retain_above: Option<i32>,
    fc: FilterConfig,
) {
    let _temp_dir = tempdir().expect("Can't create temp dir for storage");
    let mut config = mkconfig(_temp_dir.path());
    fc.apply(&mut config);
    // `min_storage_bytes` forces the merged batch to file; per-input tiers
    // come from `build_fallback_indexed_wset_i32_at`.
    config.storage.as_mut().unwrap().options.min_storage_bytes = Some(0);

    run_in_circuit_with_storage_config(config, move || {
        let factories =
            <crate::trace::FallbackIndexedWSetFactories<DynI32, DynI32, DynZWeight>>::new::<
                i32,
                i32,
                ZWeight,
            >();

        // Build inputs as a mix of vec-backed and file-backed fallback
        // batches, as picked by `batch_location_strategy` per case.
        let inputs: Vec<crate::trace::FallbackIndexedWSet<DynI32, DynI32, DynZWeight>> = batches
            .iter()
            .map(|(tuples, loc)| build_fallback_indexed_wset_i32_at(tuples.clone(), *loc))
            .collect();

        // Sanity check that each input batch resides where we requested it to be.
        for (input, (_tuples, requested_loc)) in inputs.iter().zip(batches.iter()) {
            if input.approx_key_count() > 0 {
                assert_eq!(input.location(), *requested_loc);
            }
        }

        // Build a `TestBatch` reference from the union of input tuples,
        // applying the same key filter.
        let key_filter: Option<Filter<DynI32>> = retain_above.map(|threshold| {
            Filter::new(Box::new(move |k: &DynI32| {
                *k.downcast_checked::<i32>() > threshold
            }))
        });
        let filtered_tuples: Vec<Tup2<Tup2<i32, i32>, ZWeight>> = batches
            .iter()
            .flat_map(|(tuples, _loc)| tuples.iter().copied())
            .filter(|Tup2(Tup2(k, _), _)| retain_above.is_none_or(|threshold| *k > threshold))
            .collect();
        let mut expected_erased = indexed_zset_tuples(filtered_tuples);
        let expected: TestBatch<DynI32, DynI32, (), DynZWeight> =
            TestBatch::dyn_from_tuples(&TestBatchFactories::new(), (), &mut expected_erased);

        // Force the merge through the file-backed builder.
        let input_refs: Vec<&_> = inputs.iter().collect();
        let builder = <crate::trace::FallbackIndexedWSet<
            DynI32,
            DynI32,
            DynZWeight,
        > as Batch>::Builder::for_merge(
            &factories,
            input_refs,
            Some(BatchLocation::Storage),
        );
        let cursors: Vec<_> = inputs
            .iter()
            .map(|b| b.merge_cursor(key_filter.clone(), None))
            .collect();
        let merged: crate::trace::FallbackIndexedWSet<DynI32, DynI32, DynZWeight> =
            ListMerger::merge(&factories, builder, cursors);

        // Sanity check that destination forces file storage and that the
        // merged batch's filter kind is consistent with the chosen config.
        assert_eq!(merged.location(), BatchLocation::Storage);
        let kind = merged.membership_filter_kind();
        assert!(
            fc.allowed_filter_kinds().contains(&kind),
            "filter kind {kind:?} is not allowed under {fc:?}",
        );
        assert_batch_eq(&merged, &expected);

        // Check some absent probes across the input range.
        let absent_probes = (-150_000i32..=150_000).step_by(7_500);
        assert_seek_key_exact_matches(&merged, &expected, absent_probes, 42);
    });
}

/// Dense-key sibling of `run_indexed_wset_storage_merges`. Same merge plumbing
/// but with the `0..200` key domain so we probe every possible key in
/// `seek_key_exact` and so per-batch overlap is high.
fn run_indexed_wset_storage_merges_dense(batches: MergeInputBatches, fc: FilterConfig) {
    let _temp_dir = tempdir().expect("Can't create temp dir for storage");
    let mut config = mkconfig(_temp_dir.path());
    fc.apply(&mut config);
    config.storage.as_mut().unwrap().options.min_storage_bytes = Some(0);

    run_in_circuit_with_storage_config(config, move || {
        let factories =
            <crate::trace::FallbackIndexedWSetFactories<DynI32, DynI32, DynZWeight>>::new::<
                i32,
                i32,
                ZWeight,
            >();

        let inputs: Vec<crate::trace::FallbackIndexedWSet<DynI32, DynI32, DynZWeight>> = batches
            .iter()
            .map(|(tuples, loc)| build_fallback_indexed_wset_i32_at(tuples.clone(), *loc))
            .collect();

        for (input, (_tuples, requested_loc)) in inputs.iter().zip(batches.iter()) {
            if input.approx_key_count() > 0 {
                assert_eq!(input.location(), *requested_loc);
            }
        }

        let mut expected_erased = indexed_zset_tuples(
            batches
                .iter()
                .flat_map(|(t, _l)| t.iter().copied())
                .collect(),
        );
        let expected: TestBatch<DynI32, DynI32, (), DynZWeight> =
            TestBatch::dyn_from_tuples(&TestBatchFactories::new(), (), &mut expected_erased);

        let input_refs: Vec<&_> = inputs.iter().collect();
        let builder = <crate::trace::FallbackIndexedWSet<
            DynI32,
            DynI32,
            DynZWeight,
        > as Batch>::Builder::for_merge(
            &factories,
            input_refs,
            Some(BatchLocation::Storage),
        );
        let cursors: Vec<_> = inputs.iter().map(|b| b.merge_cursor(None, None)).collect();
        let merged: crate::trace::FallbackIndexedWSet<DynI32, DynI32, DynZWeight> =
            ListMerger::merge(&factories, builder, cursors);

        assert_eq!(merged.location(), BatchLocation::Storage);
        let kind = merged.membership_filter_kind();
        assert!(
            fc.allowed_filter_kinds().contains(&kind),
            "filter kind {kind:?} is not allowed under {fc:?}",
        );

        assert_batch_eq(&merged, &expected);

        // Key domain is `0..200`; probe every value to cover present and
        // absent paths exhaustively.
        assert_seek_key_exact_matches(&merged, &expected, 0..200i32, 42);
    });
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(CIRCUIT_CASES))]

    #[test]
    fn indexed_wset_storage_merges_bloom_only(
        batches in vec(
            (merge_proptest_batch(1_000), batch_location_strategy()),
            1..=24usize,
        ),
        retain_above in proptest::option::of(-150_000..150_000i32),
    ) {
        run_indexed_wset_storage_merges(batches, retain_above, FilterConfig::BloomOnly);
    }

    #[test]
    fn indexed_wset_storage_merges_roaring_only(
        batches in vec(
            (merge_proptest_batch(1_000), batch_location_strategy()),
            1..=24usize,
        ),
        retain_above in proptest::option::of(-150_000..150_000i32),
    ) {
        run_indexed_wset_storage_merges(batches, retain_above, FilterConfig::RoaringOnly);
    }

    #[test]
    fn indexed_wset_storage_merges_both(
        batches in vec(
            (merge_proptest_batch(1_000), batch_location_strategy()),
            1..=24usize,
        ),
        retain_above in proptest::option::of(-150_000..150_000i32),
    ) {
        run_indexed_wset_storage_merges(batches, retain_above, FilterConfig::Both);
    }

    #[test]
    fn indexed_wset_storage_merges_neither(
        batches in vec(
            (merge_proptest_batch(1_000), batch_location_strategy()),
            1..=24usize,
        ),
        retain_above in proptest::option::of(-150_000..150_000i32),
    ) {
        run_indexed_wset_storage_merges(batches, retain_above, FilterConfig::Neither);
    }

    #[test]
    fn indexed_wset_storage_merges_dense_bloom_only(
        batches in vec(
            (merge_proptest_batch_dense(100), batch_location_strategy()),
            1..=10usize,
        ),
    ) {
        run_indexed_wset_storage_merges_dense(batches, FilterConfig::BloomOnly);
    }

    #[test]
    fn indexed_wset_storage_merges_dense_roaring_only(
        batches in vec(
            (merge_proptest_batch_dense(100), batch_location_strategy()),
            1..=10usize,
        ),
    ) {
        run_indexed_wset_storage_merges_dense(batches, FilterConfig::RoaringOnly);
    }

    #[test]
    fn indexed_wset_storage_merges_dense_both(
        batches in vec(
            (merge_proptest_batch_dense(100), batch_location_strategy()),
            1..=10usize,
        ),
    ) {
        run_indexed_wset_storage_merges_dense(batches, FilterConfig::Both);
    }

    #[test]
    fn indexed_wset_storage_merges_dense_neither(
        batches in vec(
            (merge_proptest_batch_dense(100), batch_location_strategy()),
            1..=10usize,
        ),
    ) {
        run_indexed_wset_storage_merges_dense(batches, FilterConfig::Neither);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1000))]

    #[test]
    fn test_vec_val_batch_builder(batch in kvtr_batch(5, 5, 5, 5, 20), seed in 0..u64::MAX) {
        let factories = <OrdValBatchFactories<DynI32, DynI32, u32, DynZWeight>>::new::<i32, i32, ZWeight>();

        test_val_batch_trace_builder::<OrdValBatch<DynI32, DynI32, u32, DynZWeight>>(&factories, batch, seed)
    }

    #[test]
    fn test_file_val_batch_builder(batch in kvtr_batch(5, 5, 5, 5, 20), seed in 0..u64::MAX) {
        run_in_circuit_with_storage(move || {
            let factories = <FileValBatchFactories<DynI32, DynI32, u32, DynZWeight>>::new::<i32, i32, ZWeight>();
            test_val_batch_trace_builder::<FileValBatch<DynI32, DynI32, u32, DynZWeight>>(&factories, batch, seed);
        });
    }

    #[test]
    fn test_vec_key_batch_builder(batch in ktr_batch(5, 5, 5, 20), seed in 0..u64::MAX) {
        let factories = <OrdKeyBatchFactories<DynI32, u32, DynZWeight>>::new::<i32, (), ZWeight>();

        test_key_batch_builder::<OrdKeyBatch<DynI32, u32, DynZWeight>>(&factories, batch, seed)
    }

    #[test]
    fn test_file_key_batch_builder(batch in ktr_batch(5, 5, 5, 20), seed in 0..u64::MAX) {
        run_in_circuit_with_storage(move || {
            let factories = <FileKeyBatchFactories<DynI32, u32, DynZWeight>>::new::<i32, (), ZWeight>();
            test_key_batch_builder::<FileKeyBatch<DynI32, u32, DynZWeight>>(&factories, batch, seed);
        });
    }
}

/// Sets the false positive rate `config`'s circuit will report.
fn set_false_positive_rate(config: &mut CircuitConfig, rate: f64) {
    config
        .storage
        .as_mut()
        .expect("config must be storage-backed")
        .options
        .bloom_false_positive_rate = Some(rate);
}

/// Builds a file batch at the most accurate rate, then reloads it at each
/// coarser rate and checks that the filter shrinks without ever losing a key.
///
/// The filter lives below every batch type, in the layer file reader, but each
/// of the four file batch types reaches it through its own `from_path`. This
/// runs the same ladder through all of them so a divergence in any one shows up.
fn assert_rate_ladder<B>(
    label: &str,
    dir: &std::path::Path,
    factories: fn() -> B::Factories,
    build: fn(&B::Factories) -> B,
    keys: &'static [u32],
) where
    B: Batch<Key = DynData> + Send + 'static,
    B::Factories: Send + 'static,
{
    // A circuit deletes its storage directory when it shuts down, so the batch
    // file is stashed outside any of them and copied back in for each reload.
    let stash = dir.join("stashed-batch.feldera");
    let name: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));

    {
        let write_dir = dir.join("write");
        std::fs::create_dir_all(&write_dir).unwrap();
        let mut config = mkconfig(&write_dir);
        set_false_positive_rate(&mut config, 1e-4);
        let (name, stash, write_dir) = (name.clone(), stash.clone(), write_dir.clone());
        run_in_circuit_with_storage_config(config, move || {
            let batch = build(&factories());
            let file = batch
                .file_reader()
                .expect("batch under test must be file backed");
            let source = file.path().to_string();
            std::fs::copy(write_dir.join(source.trim_start_matches('/')), &stash).unwrap();
            *name.lock().unwrap() = Some("ladder-batch.feldera".to_string());
            drop(batch);
        });
    }
    let name = name.lock().unwrap().clone().unwrap();
    let path = StoragePath::from(name.clone());

    // Coarsening the rate never costs more memory, and going the whole way
    // from the finest rate to the coarsest costs strictly less. Adjacent rungs
    // can tie: a batch small enough to sit on the layout's floor is more
    // accurate than nominal, so one module can already satisfy two of them.
    let mut previous_bytes = usize::MAX;
    let mut finest_bytes = 0;
    for rate in [1e-4, 1e-3, 1e-2, 1e-1] {
        let read_dir = dir.join(format!("read-{rate:e}"));
        std::fs::create_dir_all(&read_dir).unwrap();
        std::fs::copy(&stash, read_dir.join(&name)).unwrap();
        let mut config = mkconfig(&read_dir);
        set_false_positive_rate(&mut config, rate);
        let bytes = Arc::new(Mutex::new(0));
        {
            let (bytes, path) = (bytes.clone(), path.clone());
            let label = label.to_string();
            run_in_circuit_with_storage_config(config, move || {
                let batch = B::from_path(&factories(), &path)
                    .unwrap_or_else(|e| panic!("{label} at {rate:e}: {e}"));

                assert_eq!(
                    batch.membership_filter_kind(),
                    FilterKind::Bloom,
                    "{label} at {rate:e} lost its Bloom filter"
                );
                for key in keys {
                    let mut cursor = batch.cursor();
                    assert!(
                        cursor.seek_key_exact(key.erase(), None),
                        "{label} at {rate:e}: eviction lost key {key}"
                    );
                }
                *bytes.lock().unwrap() = batch.membership_filter_stats().size_byte;
            });
        }
        let bytes = *bytes.lock().unwrap();
        assert!(
            bytes <= previous_bytes,
            "{label} at {rate:e} kept {bytes} bytes, more than the {previous_bytes} before it"
        );
        if previous_bytes == usize::MAX {
            finest_bytes = bytes;
        }
        previous_bytes = bytes;
    }
    assert!(
        previous_bytes < finest_bytes,
        "{label} kept {previous_bytes} bytes at the coarsest rate, no less than \
         the {finest_bytes} it kept at the finest"
    );
}

/// Keys wide enough that roaring is not preferred, so every batch type really
/// exercises the Bloom path.
const LADDER_KEYS: &[u32] = &[
    1,
    5_000,
    90_000,
    1_000_000,
    7_000_000,
    40_000_000,
    300_000_000,
    4_000_000_000,
];

#[test]
fn file_wset_filter_follows_the_rate() {
    let temp_dir = tempdir().expect("Can't create temp dir for storage");
    assert_rate_ladder::<FileWSet<DynData, DynZWeight>>(
        "FileWSet",
        temp_dir.path(),
        <FileWSetFactories<DynData, DynZWeight>>::new::<u32, (), ZWeight>,
        |factories| {
            let mut builder =
                <FileWSet<DynData, DynZWeight> as Batch>::Builder::with_capacity(factories, 8, 0);
            for key in LADDER_KEYS {
                let weight: ZWeight = 1;
                builder.push_time_diff(&(), weight.erase());
                builder.push_key(key.erase());
            }
            builder.done()
        },
        LADDER_KEYS,
    );
}

#[test]
fn file_indexed_wset_filter_follows_the_rate() {
    let temp_dir = tempdir().expect("Can't create temp dir for storage");
    assert_rate_ladder::<FileIndexedWSet<DynData, DynData, DynZWeight>>(
        "FileIndexedWSet",
        temp_dir.path(),
        <FileIndexedWSetFactories<DynData, DynData, DynZWeight>>::new::<u32, u32, ZWeight>,
        |factories| {
            let mut builder =
                <FileIndexedWSet<DynData, DynData, DynZWeight> as Batch>::Builder::with_capacity(
                    factories, 8, 0,
                );
            for key in LADDER_KEYS {
                let weight: ZWeight = 1;
                builder.push_time_diff(&(), weight.erase());
                builder.push_val(7u32.erase());
                builder.push_key(key.erase());
            }
            builder.done()
        },
        LADDER_KEYS,
    );
}

#[test]
fn file_key_batch_filter_follows_the_rate() {
    let temp_dir = tempdir().expect("Can't create temp dir for storage");
    assert_rate_ladder::<FileKeyBatch<DynData, u32, DynZWeight>>(
        "FileKeyBatch",
        temp_dir.path(),
        <FileKeyBatchFactories<DynData, u32, DynZWeight>>::new::<u32, (), ZWeight>,
        |factories| {
            let mut builder =
                <FileKeyBatch<DynData, u32, DynZWeight> as Batch>::Builder::with_capacity(
                    factories, 8, 0,
                );
            for key in LADDER_KEYS {
                let weight: ZWeight = 1;
                builder.push_time_diff(&0u32, weight.erase());
                builder.push_val(().erase());
                builder.push_key(key.erase());
            }
            builder.done()
        },
        LADDER_KEYS,
    );
}

#[test]
fn file_val_batch_filter_follows_the_rate() {
    let temp_dir = tempdir().expect("Can't create temp dir for storage");
    assert_rate_ladder::<FileValBatch<DynData, DynData, u32, DynZWeight>>(
        "FileValBatch",
        temp_dir.path(),
        <FileValBatchFactories<DynData, DynData, u32, DynZWeight>>::new::<u32, u32, ZWeight>,
        |factories| {
            let mut builder =
                <FileValBatch<DynData, DynData, u32, DynZWeight> as Batch>::Builder::with_capacity(
                    factories, 8, 0,
                );
            for key in LADDER_KEYS {
                let weight: ZWeight = 1;
                builder.push_time_diff(&0u32, weight.erase());
                builder.push_val(7u32.erase());
                builder.push_key(key.erase());
            }
            builder.done()
        },
        LADDER_KEYS,
    );
}

/// Retention by `LastN`, `TopN` and `BottomN`: every value satisfying the filter
/// survives, along with a bounded number that do not.
///
/// `TopN` and `BottomN` accept a predicate that is not monotone, and most of
/// what these tests pin came from treating one as monotone. `TopN` used to reach
/// the first value to keep with `Cursor::seek_val_with`, a galloping search that
/// holds only for a predicate staying true once it turns true, so it skipped
/// values it had to keep. `BottomN` never positioned the batch cursor, so it
/// kept values it had to collect, and, because the cursors of one merge then
/// disagreed over a value, resurrected deleted rows. The snapshot key gate was a
/// separate defect that needed no predicate at all.
mod non_monotone_retention {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use proptest::collection::vec as prop_vec;
    use proptest::prelude::*;

    use super::{CircuitConfig, DynI32, Filter, indexed_zset_tuples};
    use crate::algebra::{OrdIndexedZSet, OrdIndexedZSetFactories};
    use crate::dynamic::{DowncastTrait, DynData, WithFactory};
    use crate::trace::{
        Batch, BatchLocation, BatchReader, BatchReaderFactories, Builder, GroupFilter, ListMerger,
        cursor::Cursor,
    };
    use crate::utils::Tup2;
    use crate::{Runtime, ZWeight};

    type Batched = OrdIndexedZSet<DynI32, DynI32>;

    /// Every `(key, value, weight)` a batch holds.
    fn contents_of(batch: &Batched) -> Vec<(i32, i32, ZWeight)> {
        let mut out = Vec::new();
        let mut cursor = batch.cursor();
        while cursor.key_valid() {
            while cursor.val_valid() {
                out.push((
                    *unsafe { cursor.key().downcast::<i32>() },
                    *unsafe { cursor.val().downcast::<i32>() },
                    **cursor.weight(),
                ));
                cursor.step_val();
            }
            cursor.step_key();
        }
        out
    }

    /// `retain_within_spine` for a spine that holds nothing but the merged
    /// batches.
    fn retain(
        filter: GroupFilter<DynI32>,
        batches: &[&[(i32, ZWeight)]],
    ) -> Vec<(i32, i32, ZWeight)> {
        retain_within_spine(filter, batches, &[])
    }

    /// Runs the filtered merge that performs retention, and reports what
    /// survived.
    ///
    /// `elsewhere` is what the spine holds in batches this merge does not cover.
    /// That is the usual case: a merge takes some of the spine's batches, while
    /// the snapshot it reads spans all of them.
    ///
    /// Retention only happens in the merge that reads a snapshot.
    /// `Spine::complete_merges` does not read one, so it applies no `TopN` or
    /// `BottomN` retention at all. The spine's background merger does, through
    /// `merge_cursor_with_snapshot`, and that is the path this mirrors, with one
    /// batch of every value standing in for the snapshot.
    fn retain_within_spine(
        filter: GroupFilter<DynI32>,
        batches: &[&[(i32, ZWeight)]],
        elsewhere: &[(i32, ZWeight)],
    ) -> Vec<(i32, i32, ZWeight)> {
        let batches: Vec<Vec<(i32, ZWeight)>> = batches.iter().map(|b| b.to_vec()).collect();
        // The snapshot is the whole spine, so weights sum and anything that
        // cancels becomes invisible to it, exactly as `CursorList` makes it.
        let values: Vec<(i32, ZWeight)> = {
            let mut v: Vec<(i32, ZWeight)> = batches
                .iter()
                .flatten()
                .chain(elsewhere.iter())
                .copied()
                .collect();
            v.sort_unstable();
            v
        };
        let result = Arc::new(std::sync::Mutex::new(Vec::new()));
        let out = result.clone();

        Runtime::run(CircuitConfig::with_workers(1), move |_parker| {
            let factories = <OrdIndexedZSetFactories<DynI32, DynI32>>::new::<i32, i32, ZWeight>();
            let build_key = |key: i32, vals: &[(i32, ZWeight)]| {
                let mut tuples =
                    indexed_zset_tuples(vals.iter().map(|&(v, w)| Tup2(Tup2(key, v), w)).collect());
                Batched::dyn_from_tuples(&factories, (), &mut tuples)
            };

            // How key 1's values are split across batches matters, so each
            // caller chooses. The extra key keeps a single-batch merge from
            // being the identity, and, since the snapshot never holds it, puts
            // every run through `retain_whole_key`.
            let mut inputs: Vec<Batched> = batches.iter().map(|b| build_key(1, b)).collect();
            inputs.push(build_key(ABSENT_KEY, &ABSENT_KEY_VALUES));

            // Everything under key 1, as the spine snapshot would see it.
            let snapshot = Some(Arc::new(build_key(1, &values)));

            let builder = <Batched as Batch>::Builder::for_merge(
                &factories,
                inputs.iter(),
                Some(BatchLocation::Memory),
            );
            let cursors = inputs
                .iter()
                .map(|b| b.merge_cursor_with_snapshot(None, Some(filter.clone()), &snapshot))
                .collect();

            let merged: Batched = ListMerger::merge(&factories, builder, cursors);
            *out.lock().unwrap() = contents_of(&merged);
        })
        .unwrap()
        .join()
        .unwrap();

        let contents = result.lock().unwrap().clone();

        // The snapshot holds key 1 alone, so `ABSENT_KEY` reaches
        // `retain_whole_key` on every run and must come back whole: nothing
        // measures it, so nothing may be dropped from it. It carries several
        // values, so a filter still holding the previous key's mark shows up.
        let absent: Vec<(i32, i32, ZWeight)> = contents
            .iter()
            .copied()
            .filter(|&(key, _value, _weight)| key == ABSENT_KEY)
            .collect();
        assert_eq!(
            absent,
            ABSENT_KEY_VALUES
                .iter()
                .map(|&(value, weight)| (ABSENT_KEY, value, weight))
                .collect::<Vec<_>>(),
            "a key the snapshot does not hold must survive the merge whole"
        );

        contents
            .into_iter()
            .filter(|&(key, _value, _weight)| key == 1)
            .collect()
    }

    /// A key the snapshot never holds, present in every merge.
    const ABSENT_KEY: i32 = 2;
    const ABSENT_KEY_VALUES: [(i32, ZWeight); 4] = [(0, 1), (5, 1), (9, 1), (14, 1)];

    /// A monotone predicate, which is all the in-tree tests use. The filters
    /// behave correctly here, so this pins the baseline the bug is measured
    /// against.
    #[test]
    fn top_n_is_correct_for_a_monotone_predicate() {
        // Retain everything >= 8, plus the 2 largest below it.
        let filter = GroupFilter::TopN(
            2,
            Filter::new(Box::new(|v: &DynI32| *unsafe { v.downcast::<i32>() } >= 8)),
            <DynData as WithFactory<i32>>::FACTORY,
        );
        let all: Vec<(i32, ZWeight)> = (0..12).map(|v| (v, 1)).collect();
        let survived = retain(filter, &[&all]);
        let values: Vec<i32> = survived.iter().map(|&(_, v, _)| v).collect();
        assert_eq!(values, vec![6, 7, 8, 9, 10, 11]);
    }

    /// The same shape of filter, but not monotone: it holds for value 3 and for
    /// nothing else below the top. Value 3 satisfies the filter, so it survives.
    /// The galloping seek used to skip it.
    #[test]
    fn top_n_keeps_a_retained_value_for_a_non_monotone_predicate() {
        let filter = GroupFilter::TopN(
            2,
            Filter::new(Box::new(|v: &DynI32| *unsafe { v.downcast::<i32>() } == 3)),
            <DynData as WithFactory<i32>>::FACTORY,
        );
        // One batch, so the seek runs over more than 8 values and `advance`
        // takes its galloping path.
        let all: Vec<(i32, ZWeight)> = (0..12).map(|v| (v, 1)).collect();
        let survived = retain(filter, &[&all]);
        let values: Vec<i32> = survived.iter().map(|&(_, v, _)| v).collect();

        // Documented semantics: every value satisfying the filter, which is {3},
        // plus the 2 largest that do not, which are {10, 11}.
        assert_eq!(
            values,
            vec![3, 10, 11],
            "value 3 satisfies the filter and must be retained"
        );
    }

    /// `BottomN` resurrects a deleted row.
    ///
    /// This is the worst of the three, and it needs no non-monotonicity: the
    /// filter here is a plain threshold. It follows purely from
    /// `BottomN::on_step_key` never touching the batch cursor, so each batch's
    /// FIRST value is emitted with no filter check at all.
    ///
    /// A value that cancels across the spine is invisible to the snapshot, so
    /// the filter never reasons about it: `CursorList::seek_key_exact` skips
    /// values whose weights sum to zero. This is what the `GroupFilter` docs
    /// warn about. Here value 5 leads one batch and trails the
    /// other. Both copies must reach the same verdict, or they stop cancelling
    /// and the deleted row comes back; that is what happened while `BottomN`
    /// left the leading copy unexamined.
    #[test]
    fn bottom_n_does_not_resurrect_a_deleted_value() {
        // Nothing satisfies the filter, so retention keeps only the n smallest.
        let filter = GroupFilter::BottomN(
            1,
            Filter::new(Box::new(|v: &DynI32| {
                *unsafe { v.downcast::<i32>() } >= 100
            })),
            <DynData as WithFactory<i32>>::FACTORY,
        );

        // Spine total for key 1: {1: +1, 5: 0, 20: +1}. Value 5 is deleted.
        // With n = 1 the only value to retain is 1, the smallest failing one.
        let survived = retain(
            filter,
            &[
                &[(5, 1), (20, 1)], // 5 leads this batch
                &[(1, 1), (5, -1)], // and trails this one
            ],
        );

        assert_eq!(
            survived,
            vec![(1, 1, 1)],
            "value 5 was deleted; retention must not bring it back"
        );
    }

    /// `BottomN` retains everything satisfying the filter plus the `n` smallest
    /// that do not. Anything else is collected; it used to be kept.
    #[test]
    fn bottom_n_collects_a_collectable_value_for_a_non_monotone_predicate() {
        let filter = GroupFilter::BottomN(
            2,
            Filter::new(Box::new(|v: &DynI32| *unsafe { v.downcast::<i32>() } == 8)),
            <DynData as WithFactory<i32>>::FACTORY,
        );
        // Split so the second batch starts at 6. `BottomN`'s `on_step_key` does
        // no seek, unlike `TopN`'s, so nothing ever skips the values leading
        // each batch.
        let survived = retain(
            filter,
            &[
                &[(0, 1), (1, 1), (2, 1), (3, 1), (4, 1), (5, 1)],
                &[(6, 1), (7, 1), (8, 1), (9, 1), (10, 1), (11, 1)],
            ],
        );
        let values: Vec<i32> = survived.iter().map(|&(_, v, _)| v).collect();

        assert_eq!(
            values,
            vec![0, 1, 8],
            "6 fails the filter and is not among the 2 smallest that do"
        );
    }

    /// Values come from a small domain so that a random bitmask ranges over
    /// every shape of predicate, and so that one batch can hold more than the
    /// eight values `advance` probes before it starts skipping.
    const VALUES: i32 = 16;
    const BATCHES: usize = 3;

    /// `(value, batch, weight)` triples. Repeating a value across batches with
    /// opposing weights cancels it, which is how a value becomes invisible to
    /// the snapshot while still present in the batches being merged.
    fn entries() -> impl Strategy<Value = Vec<(i32, usize, ZWeight)>> {
        prop_vec(
            (
                0..VALUES,
                0..BATCHES,
                prop::sample::select(vec![-2 as ZWeight, -1, 1, 2]),
            ),
            1..32usize,
        )
    }

    /// How the records outside the merge cancel the merged ones. Cancelling a
    /// key's whole group is what empties the snapshot for it, and a free draw
    /// essentially never does that, so it has to be constructed.
    #[derive(Clone, Copy, Debug)]
    enum Cancel {
        Nothing,
        Subset,
        Everything,
    }

    fn cancellation() -> impl Strategy<Value = (Cancel, Vec<bool>)> {
        (
            prop::sample::select(vec![Cancel::Nothing, Cancel::Subset, Cancel::Everything]),
            prop_vec(any::<bool>(), 0..32usize),
        )
    }

    /// What the spine holds outside the merge: `extra`, drawn freely, plus exact
    /// negations of some merged records.
    fn spine_only(
        entries: &[(i32, usize, ZWeight)],
        extra: &[(i32, ZWeight)],
        (cancel, mask): &(Cancel, Vec<bool>),
    ) -> Vec<(i32, ZWeight)> {
        let mut out = extra.to_vec();
        for (i, &(value, _batch, weight)) in entries.iter().enumerate() {
            let cancelled = match cancel {
                Cancel::Nothing => false,
                Cancel::Everything => true,
                Cancel::Subset => mask.get(i).copied().unwrap_or(false),
            };
            if cancelled {
                out.push((value, -weight));
            }
        }
        out
    }

    /// Records the spine holds in batches the merge does not cover. They decide
    /// retention without being emitted, and can cancel a merged record so that
    /// the snapshot never sees it.
    fn elsewhere() -> impl Strategy<Value = Vec<(i32, ZWeight)>> {
        prop_vec(
            (
                0..VALUES,
                prop::sample::select(vec![-2 as ZWeight, -1, 1, 2]),
            ),
            0..8usize,
        )
    }

    /// `fold` collapses the batch assignment onto fewer batches. Whether a key's
    /// values sit in one batch or are spread over several decides how far the
    /// cursor has to skip, and skipping is where the bugs are, so the property
    /// tests need both shapes.
    fn split(entries: &[(i32, usize, ZWeight)], fold: usize) -> Vec<Vec<(i32, ZWeight)>> {
        let mut batches = vec![Vec::new(); fold];
        for &(value, batch, weight) in entries {
            batches[batch % fold].push((value, weight));
        }
        batches.retain(|batch| !batch.is_empty());
        batches
    }

    /// The spine as the snapshot sees it: weights summed over every batch,
    /// including the ones outside the merge, with anything that cancels gone.
    fn spine_of(
        merged: &[(i32, usize, ZWeight)],
        elsewhere: &[(i32, ZWeight)],
    ) -> BTreeMap<i32, ZWeight> {
        let mut spine = BTreeMap::new();
        for &(value, _batch, weight) in merged {
            *spine.entry(value).or_insert(0) += weight;
        }
        for &(value, weight) in elsewhere {
            *spine.entry(value).or_insert(0) += weight;
        }
        spine.retain(|_value, weight| *weight != 0);
        spine
    }

    /// What the merge holds, and so all that it can emit. Retention is decided
    /// against the whole spine, but only these records pass through the merge.
    fn merged_of(merged: &[(i32, usize, ZWeight)]) -> BTreeMap<i32, ZWeight> {
        let mut batch = BTreeMap::new();
        for &(value, _batch, weight) in merged {
            *batch.entry(value).or_insert(0) += weight;
        }
        batch.retain(|_value, weight| *weight != 0);
        batch
    }

    fn expected(
        merged: &BTreeMap<i32, ZWeight>,
        keep: impl Fn(i32) -> bool,
    ) -> Vec<(i32, i32, ZWeight)> {
        merged
            .iter()
            .filter(|&(&value, _weight)| keep(value))
            .map(|(&value, &weight)| (1, value, weight))
            .collect()
    }

    /// Which values `TopN` and `BottomN` must retain, from the semantics stated
    /// in filter.rs rather than from the cursor: every value satisfying the
    /// filter, plus the `n` failing values nearest the chosen end of the group.
    fn keep_n(
        spine: &BTreeMap<i32, ZWeight>,
        mask: u32,
        n: usize,
        keep_largest: bool,
    ) -> Box<dyn Fn(i32) -> bool> {
        // The spine holds nothing for this key, so nothing measures it and all
        // of it stays.
        if spine.is_empty() {
            return Box::new(|_value| true);
        }

        let failing: Vec<i32> = spine
            .keys()
            .copied()
            .filter(|&value| !satisfies(mask, value))
            .collect();
        let bound = if keep_largest {
            failing.iter().rev().nth(n - 1).copied()
        } else {
            failing.get(n - 1).copied()
        };

        match bound {
            // Fewer than `n` values fail the filter, so all of them stay.
            None => Box::new(|_value| true),
            Some(bound) if keep_largest => {
                Box::new(move |value| satisfies(mask, value) || value >= bound)
            }
            Some(bound) => Box::new(move |value| satisfies(mask, value) || value <= bound),
        }
    }

    /// Which values `LastN` must retain: every one from the `n`'th before the
    /// first satisfying value onwards, or the last `n` when none satisfies it.
    fn keep_last_n(
        spine: &BTreeMap<i32, ZWeight>,
        threshold: i32,
        n: usize,
    ) -> Box<dyn Fn(i32) -> bool> {
        if spine.is_empty() {
            return Box::new(|_value| true);
        }

        let values: Vec<i32> = spine.keys().copied().collect();
        let first = values
            .iter()
            .position(|&value| value >= threshold)
            .unwrap_or(values.len());
        // Fewer than `n` values precede the first satisfying one, so all of them
        // are retained and nothing bounds the group from below.
        let bound = (first >= n).then(|| values[first - n]);
        Box::new(move |value| bound.is_none_or(|bound| value >= bound))
    }

    fn satisfies(mask: u32, value: i32) -> bool {
        mask >> value & 1 == 1
    }

    fn bitmask_filter(mask: u32) -> Filter<DynI32> {
        Filter::new(Box::new(move |v: &DynI32| {
            satisfies(mask, *unsafe { v.downcast::<i32>() })
        }))
    }

    /// A monotone filter, the only kind `LastN` supports.
    fn threshold_filter(threshold: i32) -> Filter<DynI32> {
        Filter::new(Box::new(move |v: &DynI32| {
            *unsafe { v.downcast::<i32>() } >= threshold
        }))
    }

    fn run(
        filter: GroupFilter<DynI32>,
        batches: &[Vec<(i32, ZWeight)>],
        elsewhere: &[(i32, ZWeight)],
    ) -> Vec<(i32, i32, ZWeight)> {
        let refs: Vec<&[(i32, ZWeight)]> = batches.iter().map(|b| b.as_slice()).collect();
        retain_within_spine(filter, &refs, elsewhere)
    }

    /// A merge covers some of the spine's batches. When the spine's weights for a
    /// key cancel out, `on_step_key` cannot find the key in the snapshot and
    /// drops every record it holds for that key. The records that cancel it live
    /// in batches this merge does not cover, so they survive, and the retraction
    /// they used to balance is gone.
    #[test]
    fn a_key_cancelled_elsewhere_in_the_spine_keeps_its_records() {
        let filter = GroupFilter::TopN(
            2,
            Filter::new(Box::new(|v: &DynI32| {
                *unsafe { v.downcast::<i32>() } >= 100
            })),
            <DynData as WithFactory<i32>>::FACTORY,
        );
        // 150 satisfies the filter, so retention must keep it and the operator
        // will read it. The merge sees the insertion; the matching deletion sits
        // in a batch outside the merge, so the spine nets to zero for key 1.
        let survived = retain_within_spine(filter, &[&[(150, 1)]], &[(150, -1)]);

        assert_eq!(
            survived,
            vec![(1, 150, 1)],
            "the deletion that balances this record is in a batch the merge does \
             not cover, so dropping the record leaves that deletion unbalanced"
        );
    }

    /// The mirror of the case above: the merge holds the deletion and a batch
    /// outside it holds the insertion. Dropping the deletion resurrects a row.
    #[test]
    fn a_key_cancelled_elsewhere_keeps_its_deletion() {
        let filter = GroupFilter::TopN(
            2,
            Filter::new(Box::new(|v: &DynI32| {
                *unsafe { v.downcast::<i32>() } >= 100
            })),
            <DynData as WithFactory<i32>>::FACTORY,
        );
        let survived = retain_within_spine(filter, &[&[(150, -1)]], &[(150, 1)]);

        assert_eq!(survived, vec![(1, 150, -1)]);
    }

    /// The same for `BottomN`, which is what MIN and ARG_MIN install.
    #[test]
    fn bottom_n_keeps_a_key_cancelled_elsewhere() {
        let filter = GroupFilter::BottomN(
            1,
            Filter::new(Box::new(|v: &DynI32| {
                *unsafe { v.downcast::<i32>() } >= 100
            })),
            <DynData as WithFactory<i32>>::FACTORY,
        );
        let survived = retain_within_spine(filter, &[&[(150, 1)]], &[(150, -1)]);

        assert_eq!(survived, vec![(1, 150, 1)]);
    }

    /// And for `LastN`. The gate runs before any arm, so its monotone predicate
    /// makes no difference here.
    #[test]
    fn last_n_keeps_a_key_cancelled_elsewhere() {
        let filter = GroupFilter::LastN(2, threshold_filter(100));
        let survived = retain_within_spine(filter, &[&[(150, 1)]], &[(150, -1)]);

        assert_eq!(survived, vec![(1, 150, 1)]);
    }

    /// `n = 0` denotes `Simple`, but the marking these filters rely on cannot
    /// express it, and the compiler never emits it. Rejecting it turns a silent
    /// wrong answer into a loud one.
    ///
    /// The rejection panics on a worker thread, and the runtime re-raises it as
    /// its own panic, so the message does not survive to be matched on. Running
    /// the same call with a limit of one instead pins the panic to the zero
    /// rather than to the surrounding machinery.
    fn assert_rejects_zero_n(zero: GroupFilter<DynI32>, one: GroupFilter<DynI32>) {
        let values: [(i32, ZWeight); 2] = [(0, 1), (8, 1)];

        retain(one, &[&values]);

        let panicked =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| retain(zero, &[&values])));
        assert!(panicked.is_err(), "a group filter with n = 0 was accepted");
    }

    #[test]
    fn top_n_rejects_a_zero_limit() {
        assert_rejects_zero_n(
            GroupFilter::TopN(
                0,
                threshold_filter(8),
                <DynData as WithFactory<i32>>::FACTORY,
            ),
            GroupFilter::TopN(
                1,
                threshold_filter(8),
                <DynData as WithFactory<i32>>::FACTORY,
            ),
        );
    }

    #[test]
    fn bottom_n_rejects_a_zero_limit() {
        assert_rejects_zero_n(
            GroupFilter::BottomN(
                0,
                threshold_filter(8),
                <DynData as WithFactory<i32>>::FACTORY,
            ),
            GroupFilter::BottomN(
                1,
                threshold_filter(8),
                <DynData as WithFactory<i32>>::FACTORY,
            ),
        );
    }

    #[test]
    fn last_n_rejects_a_zero_limit() {
        assert_rejects_zero_n(
            GroupFilter::LastN(0, threshold_filter(8)),
            GroupFilter::LastN(1, threshold_filter(8)),
        );
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]

        #[test]
        fn top_n_matches_the_documented_semantics(
            mask in any::<u32>(),
            n in 1usize..4,
            fold in 1usize..=BATCHES,
            entries in entries(),
            extra in elsewhere(),
            cancellation in cancellation(),
        ) {
            let elsewhere = spine_only(&entries, &extra, &cancellation);
            let filter = GroupFilter::TopN(n, bitmask_filter(mask), <DynData as WithFactory<i32>>::FACTORY);
            let spine = spine_of(&entries, &elsewhere);
            prop_assert_eq!(
                run(filter, &split(&entries, fold), &elsewhere),
                expected(&merged_of(&entries), keep_n(&spine, mask, n, true)));
        }

        #[test]
        fn bottom_n_matches_the_documented_semantics(
            mask in any::<u32>(),
            n in 1usize..4,
            fold in 1usize..=BATCHES,
            entries in entries(),
            extra in elsewhere(),
            cancellation in cancellation(),
        ) {
            let elsewhere = spine_only(&entries, &extra, &cancellation);
            let filter = GroupFilter::BottomN(n, bitmask_filter(mask), <DynData as WithFactory<i32>>::FACTORY);
            let spine = spine_of(&entries, &elsewhere);
            prop_assert_eq!(
                run(filter, &split(&entries, fold), &elsewhere),
                expected(&merged_of(&entries), keep_n(&spine, mask, n, false)));
        }

        /// `LastN` is only defined for a monotone filter, so this drives it with
        /// a threshold rather than a bitmask.
        #[test]
        fn last_n_matches_the_documented_semantics(
            threshold in 0i32..=VALUES,
            n in 1usize..4,
            fold in 1usize..=BATCHES,
            entries in entries(),
            extra in elsewhere(),
            cancellation in cancellation(),
        ) {
            let elsewhere = spine_only(&entries, &extra, &cancellation);
            let filter = GroupFilter::LastN(n, threshold_filter(threshold));
            let spine = spine_of(&entries, &elsewhere);
            prop_assert_eq!(
                run(filter, &split(&entries, fold), &elsewhere),
                expected(&merged_of(&entries), keep_last_n(&spine, threshold, n)));
        }
    }
}
