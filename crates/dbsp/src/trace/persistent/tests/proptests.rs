//! Property based testing that ensure correctness of the persistent trace
//! implementation.
//!
//! We define "correct" by testing if it behaves the same as Spine.

use crate::{
    algebra::{HasZero, MonoidValue},
    time::NestedTimestamp32,
    trace::{
        cursor::Cursor,
        ord::{OrdIndexedZSet, OrdKeyBatch, OrdValBatch, OrdZSet},
        persistent::{cursor::PersistentTraceCursor, PersistentTrace},
        spine_fueled::{Spine, SpineCursor},
        Batch, BatchReader, Builder, Trace,
    },
};
use bincode::{Decode, Encode};
use proptest::prelude::*;
use proptest_derive::Arbitrary;
use size_of::SizeOf;
use std::{
    cmp::Ordering,
    fmt::{self, Debug, Display},
    hash::{Hash, Hasher},
    ops::Range,
};

/// Maximum number of actions to perform on a trace.
const TRACE_ACTIONS_RANGE: Range<usize> = 0..8;

/// Range for possible numbers of entries in batches.
const ENTRIES_RANGE: Range<usize> = 0..24;

/// How many actions to execute on the cursors.
const ACTIONS_RANGE: Range<usize> = 0..64;

/// The sample range for `R` weights.
///
/// The bound would ideally be unconstrained
/// - but neither Spine nor PersistentTrace handles integer overflows.
/// - and by having a very small range we force more "interesting cases" where
///   weights cancel each other out.
const WEIGHT_RANGE: Range<isize> = -10..10;

/// This is a "complex" key for testing.
///
/// It's "complex" because it defines a custom ordering logic & has a heap
/// allocated [`String`] inside of it.
///
/// The tests ensure that the RocksDB based data-structure adhere to the same
/// ordering as the DRAM based version which is defined through the [`Ord`]
/// trait.
#[derive(Clone, Debug, Encode, Decode, Arbitrary, SizeOf)]
pub(super) struct ComplexKey {
    /// We ignore this type for ordering.
    pub(super) _a: isize,
    /// We use this to define the order of `Self`.
    pub(super) ord: String,
}

impl Display for ComplexKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.ord)
    }
}

impl Hash for ComplexKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.ord.hash(state);
    }
}

impl PartialEq for ComplexKey {
    fn eq(&self, other: &Self) -> bool {
        self.ord.eq(&other.ord)
    }
}

impl Eq for ComplexKey {}

impl PartialOrd for ComplexKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.ord.partial_cmp(&other.ord)
    }
}

impl Ord for ComplexKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.ord.cmp(&other.ord)
    }
}

/// Makes sure that everything we find in `spine` is also in `ptrace` (and not
/// more).
pub(super) fn spine_ptrace_are_equal<B>(spine: &Spine<B>, ptrace: &PersistentTrace<B>) -> bool
where
    B: Batch + 'static,
    B::Key: Clone + Debug + Eq + Ord + Decode + Encode,
    B::Val: Clone + Debug + Eq + Ord + Decode + Encode,
    B::R: Clone + Debug + Eq + Ord + Decode + Encode,
    B::Time: Encode + Decode + Ord,
{
    let mut spine_cursor = spine.cursor();
    let mut ptcursor = ptrace.cursor();
    while spine_cursor.key_valid() {
        if !ptcursor.key_valid() {
            eprintln!("spine_cursor key_valid(true) != ptcursor key_valid(false)");
            return false;
        }

        if spine_cursor.key() != ptcursor.key() {
            eprintln!(
                "spine_cursor key({:?}) != ptcursor key({:?})",
                spine_cursor.key(),
                ptcursor.key()
            );
            return false;
        }

        while spine_cursor.val_valid() {
            if !ptcursor.val_valid() {
                eprintln!(
                    "spine_cursor val_valid({}, val={:?}) != ptcursor val_valid({})",
                    spine_cursor.val_valid(),
                    spine_cursor.val(),
                    ptcursor.val_valid()
                );
                return false;
            }

            if spine_cursor.val() != ptcursor.val() {
                eprintln!(
                    "spine_cursor val({:?}) != ptcursor val({:?})",
                    spine_cursor.val(),
                    ptcursor.val()
                );
                return false;
            }

            let mut model_invocations = Vec::new();
            let mut test_invocation = Vec::new();
            spine_cursor.map_times(|v, t| {
                model_invocations.push((v.clone(), t.clone()));
            });
            ptcursor.map_times(|v, t| {
                test_invocation.push((v.clone(), t.clone()));
            });
            assert_eq!(model_invocations, test_invocation, "time->weights mismatch");

            spine_cursor.step_val();
            ptcursor.step_val();
        }

        debug_assert!(!spine_cursor.val_valid());
        if ptcursor.val_valid() {
            eprintln!("spine_cursor !val_valid != ptcursor !val_valid");
            return false;
        }

        spine_cursor.step_key();
        ptcursor.step_key();
    }

    debug_assert!(!spine_cursor.key_valid());
    if ptcursor.key_valid() {
        eprintln!(
            "spine_cursor !key_valid but ptcursor key_valid(key = {:?})",
            ptcursor.key()
        );
        return false;
    }

    true
}

/// Models calls we can make on the data-structure once we implement the
/// [`Cursor`] trait.
#[derive(Debug, Clone)]
enum CursorAction<
    K: Arbitrary + Clone + 'static,
    V: Arbitrary + Clone + 'static,
    T: Arbitrary + Clone + 'static,
> {
    StepKey,
    StepVal,
    SeekKey(K),
    SeekVal(V),
    SeekValWith(V),
    RewindKeys,
    RewindVals,
    Key,
    Val,
    MapTimes,
    MapTimesThrough(T),
}

/// Generates a random action to perform on a cursor.
fn action<
    K: Arbitrary + Clone + 'static,
    V: Arbitrary + Clone + 'static,
    T: Arbitrary + Clone + 'static,
>() -> impl Strategy<Value = CursorAction<K, V, T>> {
    prop_oneof![
        Just(CursorAction::StepKey),
        Just(CursorAction::StepVal),
        Just(CursorAction::RewindKeys),
        Just(CursorAction::RewindVals),
        Just(CursorAction::Key),
        Just(CursorAction::Val),
        Just(CursorAction::MapTimes),
        any::<T>().prop_map(CursorAction::MapTimesThrough),
        any::<K>().prop_map(CursorAction::SeekKey),
        any::<V>().prop_map(CursorAction::SeekVal),
        any::<V>().prop_map(CursorAction::SeekValWith),
    ]
}

/// Generates a sequence of actions to perform on a cursor.
fn actions<
    K: Arbitrary + Clone + 'static,
    V: Arbitrary + Clone + 'static,
    T: Arbitrary + Clone + 'static,
>() -> impl Strategy<Value = Vec<CursorAction<K, V, T>>> {
    prop::collection::vec(action::<K, V, T>(), ACTIONS_RANGE)
}

/// Performs a random series of actions on a [`Spine`] cursor and a
/// [`PersistentTrace`] cursor, and makes sure they produce the same results.
fn cursor_trait<B, I>(
    mut data: Vec<(B::Item, B::R)>,
    ops: Vec<CursorAction<B::Key, B::Val, B::Time>>,
) where
    I: Ord + Clone + 'static,
    B: Batch<Item = I> + 'static,
    B::Key: Arbitrary + Ord + Clone + Encode + Decode,
    B::Val: Arbitrary + Ord + Clone + Encode + Decode,
    B::R: Arbitrary + Ord + Clone + MonoidValue + Encode + Decode,
    B::Time: Arbitrary + Clone + Encode + Decode + Default,
{
    // Builder interface wants sorted, unique(?) keys:
    data.sort_unstable();
    data.dedup_by(|a, b| a.0.eq(&b.0));

    // Instantiate a Batch
    let mut batch_builder = B::Builder::new_builder(B::Time::default());
    for data_tuple in data.into_iter() {
        batch_builder.push(data_tuple);
    }
    let batch = batch_builder.done();

    let mut model = Spine::<B>::new(None);
    model.insert(batch.clone());
    let mut model_cursor = model.cursor();

    let mut ptrace = PersistentTrace::<B>::new(None);
    ptrace.insert(batch);
    let mut totest_cursor = ptrace.cursor();

    // We check the non-mutating cursor interface after every
    // command/mutation.
    fn check_eq_invariants<B: Batch>(
        step: usize,
        model_cursor: &SpineCursor<B>,
        totest_cursor: &PersistentTraceCursor<B>,
    ) where
        B::Key: Ord + Clone + Encode + Decode,
        B::Val: Ord + Clone + Encode + Decode,
        B::Time: Encode + Decode,
        B::R: MonoidValue + Encode + Decode,
    {
        assert_eq!(
            model_cursor.key_valid(),
            totest_cursor.key_valid(),
            "key_valid() mismatch in step {}",
            step
        );
        assert_eq!(
            model_cursor.val_valid(),
            totest_cursor.val_valid(),
            "val_valid() mismatch in step {}",
            step
        );
    }

    //assert_eq!(ptrace.len(), model.len());
    for (i, action) in ops.iter().enumerate() {
        match action {
            CursorAction::StepKey => {
                model_cursor.step_key();
                totest_cursor.step_key();
                check_eq_invariants(i, &model_cursor, &totest_cursor);
            }
            CursorAction::StepVal => {
                model_cursor.step_val();
                totest_cursor.step_val();
                check_eq_invariants(i, &model_cursor, &totest_cursor);
            }
            CursorAction::SeekKey(k) => {
                model_cursor.seek_key(k);
                totest_cursor.seek_key(k);
                check_eq_invariants(i, &model_cursor, &totest_cursor);
            }
            CursorAction::SeekVal(v) => {
                model_cursor.seek_val(v);
                totest_cursor.seek_val(v);
                check_eq_invariants(i, &model_cursor, &totest_cursor);
            }
            CursorAction::SeekValWith(v) => {
                model_cursor.seek_val_with(|cmp_with| cmp_with >= v);
                totest_cursor.seek_val_with(|cmp_with| cmp_with >= v);
                check_eq_invariants(i, &model_cursor, &totest_cursor);
            }
            CursorAction::RewindKeys => {
                model_cursor.rewind_keys();
                totest_cursor.rewind_keys();
                check_eq_invariants(i, &model_cursor, &totest_cursor);
            }
            CursorAction::RewindVals => {
                model_cursor.rewind_vals();
                totest_cursor.rewind_vals();
                check_eq_invariants(i, &model_cursor, &totest_cursor);
            }
            CursorAction::Key => {
                if model_cursor.key_valid() {
                    assert_eq!(model_cursor.key(), totest_cursor.key());
                }
                check_eq_invariants(i, &model_cursor, &totest_cursor);
            }
            CursorAction::Val => {
                if model_cursor.val_valid() {
                    assert_eq!(model_cursor.val(), totest_cursor.val());
                }
                check_eq_invariants(i, &model_cursor, &totest_cursor);
            }
            CursorAction::MapTimes => {
                let mut model_invocations = Vec::new();
                let mut test_invocation = Vec::new();
                model_cursor.map_times(|v, t| {
                    model_invocations.push((v.clone(), t.clone()));
                });
                totest_cursor.map_times(|v, t| {
                    test_invocation.push((v.clone(), t.clone()));
                });
                assert_eq!(model_invocations, test_invocation);
            }
            CursorAction::MapTimesThrough(upper) => {
                let mut model_invocations = Vec::new();
                let mut test_invocation = Vec::new();
                model_cursor.map_times_through(upper, |v, t| {
                    model_invocations.push((v.clone(), t.clone()));
                });
                totest_cursor
                    .map_times_through(upper, |v, t| test_invocation.push((v.clone(), t.clone())));
                assert_eq!(model_invocations, test_invocation);
            }
        }
    }
}

/// Generates a `(K, V)` tuple for insertion into batches that store values.
fn kv_tuple<K: Arbitrary + Clone, V: Arbitrary + Clone>() -> impl Strategy<Value = (K, V)> {
    (any::<K>(), any::<V>())
}

/// Generates a vector of `((K, V), R)` tuples for insertion into batches that
/// store values and weights.
fn kvr_tuples<K: Arbitrary + Clone, V: Arbitrary + Clone, R: Arbitrary + Clone>(
    rs: impl Strategy<Value = R>,
) -> impl Strategy<Value = Vec<((K, V), R)>> {
    prop::collection::vec(((any::<K>(), any::<V>()), rs), ENTRIES_RANGE)
}

/// Generates a vector of `(K, R)` tuples for insertion into batches that don't
/// store values.
fn kr_tuples<K: Arbitrary + Clone, R: Default + Arbitrary + Clone>(
    rs: impl Strategy<Value = R>,
) -> impl Strategy<Value = Vec<(K, R)>> {
    prop::collection::vec((any::<K>(), rs), ENTRIES_RANGE)
}

/// Mutable calls we can perform on something that implements the [`Trace`]
/// trait.
#[derive(Clone, Debug)]
enum TraceAction<T, I, R>
where
    I: Ord + Clone + 'static,
    R: Ord + Clone + MonoidValue + Encode + Decode,
    T: Clone + 'static,
{
    ClearDirtyFlag,
    RecedeTo(T),
    Insert((T, Vec<(I, R)>)),
    // Exert -- ignored because it's a nop in the PersistentTrace
    // Consolidate -- consumes self so we should just do it once at the end of the test
}

/// Generate one action to perform on a [`Trace`].
fn trace_action<T, I, R>(
    is: impl Strategy<Value = I>,
    rs: impl Strategy<Value = R>,
) -> impl Strategy<Value = TraceAction<T, I, R>>
where
    T: Arbitrary + Debug + Clone + 'static,
    I: Debug + Ord + Clone + 'static,
    R: Debug + Ord + Clone + MonoidValue + Encode + Decode,
{
    prop_oneof![
        Just(TraceAction::ClearDirtyFlag),
        any::<T>().prop_map(TraceAction::RecedeTo),
        (any::<T>(), prop::collection::vec((is, rs), ENTRIES_RANGE)).prop_map(TraceAction::Insert),
    ]
}

/// Generate a sequence of actions to perform on a [`Trace`].
fn trace_actions<T, I, R>(
    is: impl Strategy<Value = I>,
    rs: impl Strategy<Value = R>,
) -> impl Strategy<Value = Vec<TraceAction<T, I, R>>>
where
    T: Arbitrary + Debug + Clone + 'static,
    I: Arbitrary + Debug + Ord + Clone + 'static,
    R: Arbitrary + Debug + Ord + Clone + MonoidValue + Encode + Decode,
{
    prop::collection::vec(trace_action::<T, I, R>(is, rs), TRACE_ACTIONS_RANGE)
}

/// Performs a random series of actions on a [`Spine`] and a
/// [`PersistentTrace`], and makes sure they produce the same results.
fn trace_trait<B, I>(actions: Vec<TraceAction<B::Time, I, B::R>>)
where
    I: Ord + Clone + 'static,
    B: Batch<Item = I> + 'static,
    B::Key: Arbitrary + Ord + Clone + Encode + Decode,
    B::Val: Arbitrary + Ord + Clone + Encode + Decode,
    B::R: Arbitrary + Ord + Clone + MonoidValue + Encode + Decode,
    B::Time: Arbitrary + Clone + Encode + Decode + Default,
{
    let mut model = Spine::<B>::new(None);
    let mut ptrace = PersistentTrace::<B>::new(None);

    // We check the non-mutating functions after every
    // command/mutation.
    fn check_eq_invariants<B: Batch + 'static>(
        step: usize,
        model: &Spine<B>,
        totest: &PersistentTrace<B>,
    ) where
        B::Key: Debug + Ord + Clone + Encode + Decode,
        B::Val: Debug + Ord + Clone + Encode + Decode,
        B::Time: Encode + Decode,
        B::R: Debug + MonoidValue + Encode + Decode,
    {
        assert_eq!(
            model.dirty(),
            totest.dirty(),
            "dirty() mismatch in step {}",
            step
        );
        // Not comparable, no accurate counts with rocksdb:
        //assert_eq!(
        //    model.key_count(),
        //    totest.key_count(),
        //    "key_count() mismatch in step {}",
        //    step
        //);
        //assert_eq!(model.len(), totest.len(), "len() mismatch in step {}", step);
        //assert_eq!(
        //model.is_empty(),
        //totest.is_empty(),
        //"is_empty() mismatch in step {}",
        //step
        //);
        assert_eq!(
            model.lower(),
            totest.lower(),
            "lower() mismatch in step {}",
            step
        );
        assert_eq!(
            model.upper(),
            totest.upper(),
            "upper() mismatch in step {}",
            step
        );
    }

    for (step, action) in actions.into_iter().enumerate() {
        match action {
            TraceAction::RecedeTo(t) => {
                // TODO: implement recede_to
                model.recede_to(&t);
                ptrace.recede_to(&t);
                check_eq_invariants(step, &model, &ptrace);
                assert!(spine_ptrace_are_equal(&model, &ptrace));
            }
            TraceAction::ClearDirtyFlag => {
                ptrace.clear_dirty_flag();
                model.clear_dirty_flag();
                check_eq_invariants(step, &model, &ptrace);
                assert!(spine_ptrace_are_equal(&model, &ptrace));
            }
            TraceAction::Insert((time, mut batch_tuples)) => {
                // Builder interface wants sorted, unique(?) keys:
                batch_tuples.sort_unstable();
                batch_tuples.dedup_by(|a, b| a.0.eq(&b.0));

                // Instantiate a Batch
                let mut batch_builder = B::Builder::new_builder(time);
                for tuple in batch_tuples.into_iter() {
                    if !tuple.1.is_zero() {
                        batch_builder.push(tuple);
                    }
                }
                let batch = batch_builder.done();

                model.insert(batch.clone());
                let mut fuel = isize::MAX;
                model.exert(&mut fuel);

                ptrace.insert(batch);

                check_eq_invariants(step, &model, &ptrace);
                assert!(spine_ptrace_are_equal(&model, &ptrace));
            }
        }
    }
}

proptest! {
    // Verify that our [`Cursor`] implementation for the persistent trace
    // behaves the same as spine (non-persistent traces):

    #[test]
    fn cursor_val_batch_ptrace_behaves_like_spine(ks in kvr_tuples::<ComplexKey, String, isize>(WEIGHT_RANGE), ops in actions::<ComplexKey, String, u32>()) {
        cursor_trait::<OrdValBatch<ComplexKey, String, u32, isize>, (ComplexKey, String)>(ks, ops);
    }

    #[test]
    fn cursor_key_batch_ptrace_behaves_like_spine(ks in kr_tuples::<ComplexKey, isize>(WEIGHT_RANGE), ops in actions::<ComplexKey, (), u32>()) {
        cursor_trait::<OrdKeyBatch<ComplexKey, u32, isize>, ComplexKey>(ks, ops);
    }

    #[test]
    fn cursor_zset_ptrace_behaves_like_spine(ks in kr_tuples::<ComplexKey, isize>(WEIGHT_RANGE), ops in actions::<ComplexKey, (), ()>()) {
        cursor_trait::<OrdZSet<ComplexKey, isize>, ComplexKey>(ks, ops);
    }

    #[test]
    fn cursor_indexed_zset_ptrace_behaves_like_spine(ks in kvr_tuples::<ComplexKey, usize, isize>(WEIGHT_RANGE), ops in actions::<ComplexKey, usize, ()>()) {
        cursor_trait::<OrdIndexedZSet<ComplexKey, usize, isize>, (ComplexKey, usize)>(ks, ops);
    }

    // Verify that our [`Trace`] implementation behaves the same as the spine
    // trace implementation:

    #[test]
    fn val_batch_trace_behaves_like_spine(actions in trace_actions::<NestedTimestamp32, (ComplexKey, String), isize>(kv_tuple::<ComplexKey, String>(), WEIGHT_RANGE)) {
        trace_trait::<OrdValBatch<ComplexKey, String, NestedTimestamp32, isize>, (ComplexKey, String)>(actions);
    }

    #[test]
    fn key_batch_trace_behaves_like_spine(actions in trace_actions::<NestedTimestamp32, ComplexKey, isize>(any::<ComplexKey>(), WEIGHT_RANGE)) {
        trace_trait::<OrdKeyBatch<ComplexKey, NestedTimestamp32, isize>, ComplexKey>(actions);
    }

    #[test]
    fn zset_trace_behaves_like_spine(actions in trace_actions::<(), ComplexKey, isize>(any::<ComplexKey>(), WEIGHT_RANGE)) {
        trace_trait::<OrdZSet<ComplexKey, isize>, ComplexKey>(actions);
    }

    #[test]
    fn indexed_zset_trace_behaves_like_spine(actions in trace_actions::<(), (ComplexKey, usize), isize>(kv_tuple::<ComplexKey, usize>(), WEIGHT_RANGE)) {
        trace_trait::<OrdIndexedZSet<ComplexKey, usize, isize>, (ComplexKey, usize)>(actions);
    }
}
