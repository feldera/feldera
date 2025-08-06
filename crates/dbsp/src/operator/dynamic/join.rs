use crate::algebra::{ZBatch, ZBatchReader};
use crate::circuit::circuit_builder::StreamId;
use crate::circuit::metadata::{BatchSizeStats, OUTPUT_BATCHES_LABEL};
use crate::circuit::splitter_output_chunk_size;
use crate::dynamic::DynData;
use crate::operator::async_stream_operators::{StreamingBinaryOperator, StreamingBinaryWrapper};
use crate::trace::spine_async::WithSnapshot;
use crate::trace::{Spine, Trace};
use crate::{
    algebra::{
        IndexedZSet, IndexedZSetReader, Lattice, MulByRef, OrdIndexedZSet, OrdZSet, PartialOrder,
        ZSet,
    },
    circuit::{
        metadata::{
            MetaItem, OperatorLocation, OperatorMeta, NUM_ENTRIES_LABEL, SHARED_BYTES_LABEL,
            USED_BYTES_LABEL,
        },
        operator_traits::{BinaryOperator, Operator},
        Circuit, RootCircuit, Scope, Stream, WithClock,
    },
    circuit_cache_key,
    dynamic::{
        ClonableTrait, DataTrait, DynDataTyped, DynPair, DynPairs, DynUnit, Erase, Factory,
        LeanVec, WithFactory,
    },
    operator::dynamic::{distinct::DistinctFactories, filter_map::DynFilterMap},
    time::Timestamp,
    trace::{
        BatchFactories, BatchReader, BatchReaderFactories, Batcher, Builder, Cursor, WeightedItem,
    },
    utils::Tup2,
    DBData, ZWeight,
};
use crate::{NestedCircuit, Position, Runtime};
use async_stream::stream;
use minitrace::trace;
use size_of::{Context, SizeOf};
use std::cell::RefCell;
use std::rc::Rc;
use std::{
    borrow::Cow,
    cmp::{min, Ordering},
    collections::HashMap,
    marker::PhantomData,
    ops::Deref,
    panic::Location,
};

use super::{MonoIndexedZSet, MonoZSet};
circuit_cache_key!(AntijoinId<C, D>((StreamId, StreamId) => Stream<C, D>));

pub trait TraceJoinFuncTrait<K: ?Sized, V1: ?Sized, V2: ?Sized, OK: ?Sized, OV: ?Sized>:
    FnMut(&K, &V1, &V2, &mut dyn FnMut(&mut OK, &mut OV))
{
    fn fork(&self) -> TraceJoinFunc<K, V1, V2, OK, OV>;
}

impl<K: ?Sized, V1: ?Sized, V2: ?Sized, OK: ?Sized, OV: ?Sized, F>
    TraceJoinFuncTrait<K, V1, V2, OK, OV> for F
where
    F: FnMut(&K, &V1, &V2, &mut dyn FnMut(&mut OK, &mut OV)) + Clone + 'static,
{
    fn fork(&self) -> TraceJoinFunc<K, V1, V2, OK, OV> {
        Box::new(self.clone())
    }
}

pub type TraceJoinFunc<K, V1, V2, OK, OV> = Box<dyn TraceJoinFuncTrait<K, V1, V2, OK, OV>>;

pub struct TraceJoinFuncs<K: ?Sized, V1: ?Sized, V2: ?Sized, OK: ?Sized, OV: ?Sized> {
    pub left: TraceJoinFunc<K, V1, V2, OK, OV>,
    pub right: TraceJoinFunc<K, V2, V1, OK, OV>,
}

impl<K: ?Sized, V1: ?Sized, V2: ?Sized, OK: ?Sized, OV: ?Sized> TraceJoinFuncs<K, V1, V2, OK, OV> {
    fn new<F>(mut f: F) -> Self
    where
        F: TraceJoinFuncTrait<K, V1, V2, OK, OV> + Clone + 'static,
    {
        let mut f_clone = f.clone();
        Self {
            left: Box::new(move |k, v1, v2, cb| f_clone(k, v1, v2, cb)),
            right: Box::new(move |k, v1, v2, cb| f(k, v2, v1, cb)),
        }
    }
}
pub trait JoinFuncTrait<K: ?Sized, V1: ?Sized, V2: ?Sized, O: ?Sized>:
    Fn(&K, &V1, &V2, &mut O)
{
    fn fork(&self) -> JoinFunc<K, V1, V2, O>;
}

impl<K: ?Sized, V1: ?Sized, V2: ?Sized, O: ?Sized, F> JoinFuncTrait<K, V1, V2, O> for F
where
    F: Fn(&K, &V1, &V2, &mut O) + Clone + 'static,
{
    fn fork(&self) -> JoinFunc<K, V1, V2, O> {
        Box::new(self.clone())
    }
}

pub type JoinFunc<K, V1, V2, O> = Box<dyn JoinFuncTrait<K, V1, V2, O>>;

pub struct StreamJoinFactories<I1, I2, O>
where
    I1: IndexedZSetReader,
    I2: IndexedZSetReader,
    O: ZSet,
{
    pub left_factories: I1::Factories,
    pub right_factories: I2::Factories,
    pub output_factories: O::Factories,
}

impl<I1, I2, O> StreamJoinFactories<I1, I2, O>
where
    I1: IndexedZSetReader,
    I2: IndexedZSetReader<Key = I1::Key>,
    O: ZSet,
{
    pub fn new<KType, V1Type, V2Type, VType>() -> Self
    where
        KType: DBData + Erase<I1::Key>,
        V1Type: DBData + Erase<I1::Val>,
        V2Type: DBData + Erase<I2::Val>,
        VType: DBData + Erase<O::Key>,
    {
        Self {
            left_factories: BatchReaderFactories::new::<KType, V1Type, ZWeight>(),
            right_factories: BatchReaderFactories::new::<KType, V2Type, ZWeight>(),
            output_factories: BatchReaderFactories::new::<VType, (), ZWeight>(),
        }
    }
}

pub struct JoinFactories<I1, I2, T, O>
where
    I1: IndexedZSet,
    I2: IndexedZSet,
    O: IndexedZSet,
    T: Timestamp,
{
    pub left_factories: I1::Factories,
    pub right_factories: I2::Factories,
    pub left_trace_factories: <T::TimedBatch<I1> as BatchReader>::Factories,
    pub right_trace_factories: <T::TimedBatch<I2> as BatchReader>::Factories,
    pub output_factories: O::Factories,
    pub timed_item_factory:
        &'static dyn Factory<DynPair<DynDataTyped<T>, WeightedItem<O::Key, O::Val, O::R>>>,
    pub timed_items_factory:
        &'static dyn Factory<DynPairs<DynDataTyped<T>, WeightedItem<O::Key, O::Val, O::R>>>,
}

impl<I1, I2, T, O> JoinFactories<I1, I2, T, O>
where
    I1: IndexedZSet,
    I2: IndexedZSet<Key = I1::Key>,
    O: IndexedZSet,
    T: Timestamp,
{
    pub fn new<KType, V1Type, V2Type, OKType, OVType>() -> Self
    where
        KType: DBData + Erase<I1::Key>,
        V1Type: DBData + Erase<I1::Val>,
        V2Type: DBData + Erase<I2::Val>,
        OKType: DBData + Erase<O::Key>,
        OVType: DBData + Erase<O::Val>,
    {
        Self {
            left_factories: BatchReaderFactories::new::<KType, V1Type, ZWeight>(),
            right_factories: BatchReaderFactories::new::<KType, V2Type, ZWeight>(),
            left_trace_factories: BatchReaderFactories::new::<KType, V1Type, ZWeight>(),
            right_trace_factories: BatchReaderFactories::new::<KType, V2Type, ZWeight>(),
            output_factories: BatchReaderFactories::new::<OKType, OVType, ZWeight>(),
            timed_item_factory:
                WithFactory::<Tup2<T, Tup2<Tup2<OKType, OVType>, ZWeight>>>::FACTORY,
            timed_items_factory:
                WithFactory::<LeanVec<Tup2<T, Tup2<Tup2<OKType, OVType>, ZWeight>>>>::FACTORY,
        }
    }
}

impl<I1, I2, T, O> Clone for JoinFactories<I1, I2, T, O>
where
    I1: IndexedZSet,
    I2: IndexedZSet,
    O: IndexedZSet,
    T: Timestamp,
{
    fn clone(&self) -> Self {
        Self {
            left_factories: self.left_factories.clone(),
            right_factories: self.right_factories.clone(),
            left_trace_factories: self.left_trace_factories.clone(),
            right_trace_factories: self.right_trace_factories.clone(),
            output_factories: self.output_factories.clone(),
            timed_item_factory: self.timed_item_factory,
            timed_items_factory: self.timed_items_factory,
        }
    }
}

pub struct AntijoinFactories<I1, I2, T>
where
    I1: IndexedZSet,
    I2: ZSet,
    T: Timestamp,
{
    pub join_factories: JoinFactories<I1, I2, T, I1>,
    pub distinct_factories: DistinctFactories<I2, T>,
}

impl<I1, I2, T> AntijoinFactories<I1, I2, T>
where
    I1: IndexedZSet,
    I2: ZSet<Key = I1::Key>,
    T: Timestamp,
{
    pub fn new<KType, V1Type>() -> Self
    where
        KType: DBData + Erase<I1::Key>,
        V1Type: DBData + Erase<I1::Val>,
    {
        Self {
            join_factories: JoinFactories::new::<KType, V1Type, (), KType, V1Type>(),
            distinct_factories: DistinctFactories::new::<KType, ()>(),
        }
    }
}

impl<I1, I2, T> Clone for AntijoinFactories<I1, I2, T>
where
    I1: IndexedZSet,
    I2: ZSet,
    T: Timestamp,
{
    fn clone(&self) -> Self {
        Self {
            join_factories: self.join_factories.clone(),
            distinct_factories: self.distinct_factories.clone(),
        }
    }
}

pub struct OuterJoinFactories<I1, I2, T, O>
where
    I1: IndexedZSet,
    I2: IndexedZSet,
    T: Timestamp,
    O: DataTrait + ?Sized,
{
    pub join_factories: JoinFactories<I1, I2, T, OrdZSet<O>>,
    pub left_join_factories: JoinFactories<I1, OrdZSet<I2::Key>, T, I1>,
    pub right_join_factories: JoinFactories<I2, OrdZSet<I1::Key>, T, I2>,

    pub left_distinct_factories: DistinctFactories<OrdZSet<I1::Key>, T>,
    pub right_distinct_factories: DistinctFactories<OrdZSet<I2::Key>, T>,
    pub output_factories: <OrdZSet<O> as BatchReader>::Factories,
}

impl<I1, I2, T, O> OuterJoinFactories<I1, I2, T, O>
where
    I1: IndexedZSet,
    I2: IndexedZSet<Key = I1::Key>,
    T: Timestamp,
    O: DataTrait + ?Sized,
{
    pub fn new<KType, V1Type, V2Type, OType>() -> Self
    where
        KType: DBData + Erase<I1::Key>,
        V1Type: DBData + Erase<I1::Val>,
        V2Type: DBData + Erase<I2::Val>,
        OType: DBData + Erase<O>,
    {
        Self {
            join_factories: JoinFactories::new::<KType, V1Type, V2Type, OType, ()>(),
            left_join_factories: JoinFactories::new::<KType, V1Type, (), KType, V1Type>(),
            right_join_factories: JoinFactories::new::<KType, V2Type, (), KType, V2Type>(),
            left_distinct_factories: DistinctFactories::new::<KType, ()>(),
            right_distinct_factories: DistinctFactories::new::<KType, ()>(),
            output_factories: BatchReaderFactories::new::<OType, (), ZWeight>(),
        }
    }
}

impl<I1, I2, T, O> Clone for OuterJoinFactories<I1, I2, T, O>
where
    I1: IndexedZSet,
    I2: IndexedZSet,
    T: Timestamp,
    O: DataTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            join_factories: self.join_factories.clone(),
            left_join_factories: self.left_join_factories.clone(),
            right_join_factories: self.right_join_factories.clone(),
            left_distinct_factories: self.left_distinct_factories.clone(),
            right_distinct_factories: self.right_distinct_factories.clone(),
            output_factories: self.output_factories.clone(),
        }
    }
}

impl<C, I1> Stream<C, I1>
where
    C: Circuit,
{
    /// See [`Stream::stream_join`].
    #[track_caller]
    #[allow(clippy::type_complexity)]
    pub fn dyn_stream_join<I2, V>(
        &self,
        factories: &StreamJoinFactories<I1, I2, OrdZSet<V>>,
        other: &Stream<C, I2>,
        join: JoinFunc<I1::Key, I1::Val, I2::Val, V>,
    ) -> Stream<C, OrdZSet<V>>
    where
        I1: IndexedZSet,
        I2: IndexedZSet<Key = I1::Key>,
        V: DataTrait + ?Sized,
    {
        self.dyn_stream_join_generic(factories, other, join)
    }

    /// Like [`Self::dyn_stream_join`], but can return any batch type.
    #[track_caller]
    pub fn dyn_stream_join_generic<I2, Z>(
        &self,
        factories: &StreamJoinFactories<I1, I2, Z>,
        other: &Stream<C, I2>,
        join: JoinFunc<I1::Key, I1::Val, I2::Val, Z::Key>,
    ) -> Stream<C, Z>
    where
        I1: IndexedZSet,
        I2: IndexedZSet<Key = I1::Key>,
        Z: ZSet,
    {
        self.circuit().add_binary_operator(
            Join::new(&factories.output_factories, join, Location::caller()),
            &self.dyn_shard(&factories.left_factories),
            &other.dyn_shard(&factories.right_factories),
        )
    }

    /// See [`Stream::monotonic_stream_join`].
    #[track_caller]
    pub fn dyn_monotonic_stream_join<I2, Z>(
        &self,
        factories: &StreamJoinFactories<I1, I2, Z>,
        other: &Stream<C, I2>,
        join: JoinFunc<I1::Key, I1::Val, I2::Val, Z::Key>,
    ) -> Stream<C, Z>
    where
        I1: IndexedZSet,
        I2: IndexedZSet<Key = I1::Key>,
        Z: ZSet,
    {
        self.circuit().add_binary_operator(
            MonotonicJoin::new(&factories.output_factories, join, Location::caller()),
            &self.dyn_shard(&factories.left_factories),
            &other.dyn_shard(&factories.right_factories),
        )
    }

    fn dyn_stream_join_inner<I2, Z>(
        &self,
        output_factories: &Z::Factories,
        other: &Stream<C, I2>,
        join: JoinFunc<I1::Key, I1::Val, I2::Val, Z::Key>,
        location: &'static Location<'static>,
    ) -> Stream<C, Z>
    where
        I1: IndexedZSetReader + Clone,
        I2: IndexedZSetReader<Key = I1::Key> + Clone,
        Z: ZSet,
    {
        self.circuit()
            .add_binary_operator(Join::new(output_factories, join, location), self, other)
    }
}

impl<I1> Stream<RootCircuit, I1> {
    /// Incremental join of two streams of batches.
    ///
    /// Given streams `a` and `b` of changes to relations `A` and `B`
    /// respectively, computes a stream of changes to `A ⋈ B` (where `⋈`
    /// is the join operator):
    ///
    /// ```text
    /// delta(A ⋈ B) = A ⋈ B - z^-1(A) ⋈ z^-1(B) = a ⋈ z^-1(B) + z^-1(A) ⋈ b + a ⋈ b
    /// ```
    ///
    /// This method only works in the top-level scope.  It is superseded by
    /// [`join`](`crate::circuit::Stream::join`), which works in arbitrary
    /// nested scopes.  We keep this implementation for testing and
    /// benchmarking purposes.
    #[track_caller]
    #[doc(hidden)]
    pub fn dyn_join_incremental<F, I2, Z>(
        &self,
        self_factories: &I1::Factories,
        other_factories: &I2::Factories,
        output_factories: &Z::Factories,
        other: &Stream<RootCircuit, I2>,
        join_func: JoinFunc<I1::Key, I1::Val, I2::Val, Z::Key>,
    ) -> Stream<RootCircuit, Z>
    where
        I1: IndexedZSet,
        I2: IndexedZSet<Key = I1::Key>,
        F: Clone + Fn(&I1::Key, &I1::Val, &I2::Val) -> Z::Key + 'static,
        Z: ZSet,
    {
        let left = self.dyn_shard(self_factories);
        let right = other.dyn_shard(other_factories);

        left.dyn_integrate_trace(self_factories)
            .delay_trace()
            .dyn_stream_join_inner(
                output_factories,
                &right,
                join_func.fork(),
                Location::caller(),
            )
            .plus(&left.dyn_stream_join_inner(
                output_factories,
                &right.dyn_integrate_trace(other_factories),
                join_func,
                Location::caller(),
            ))
    }
}

impl Stream<RootCircuit, MonoIndexedZSet> {
    #[track_caller]
    pub fn dyn_join_mono(
        &self,
        factories: &JoinFactories<MonoIndexedZSet, MonoIndexedZSet, (), OrdZSet<DynData>>,
        other: &Stream<RootCircuit, MonoIndexedZSet>,
        join_funcs: TraceJoinFuncs<DynData, DynData, DynData, DynData, DynUnit>,
    ) -> Stream<RootCircuit, MonoZSet> {
        self.dyn_join(factories, other, join_funcs)
    }

    #[track_caller]
    pub fn dyn_join_index_mono(
        &self,
        factories: &JoinFactories<MonoIndexedZSet, MonoIndexedZSet, (), MonoIndexedZSet>,
        other: &Stream<RootCircuit, MonoIndexedZSet>,
        join_funcs: TraceJoinFuncs<DynData, DynData, DynData, DynData, DynData>,
    ) -> Stream<RootCircuit, MonoIndexedZSet> {
        self.dyn_join_generic(factories, other, join_funcs)
    }

    pub fn dyn_antijoin_mono(
        &self,
        factories: &AntijoinFactories<MonoIndexedZSet, MonoZSet, ()>,
        other: &Stream<RootCircuit, MonoIndexedZSet>,
    ) -> Stream<RootCircuit, MonoIndexedZSet> {
        self.dyn_antijoin(factories, other)
    }
}

impl Stream<NestedCircuit, MonoIndexedZSet> {
    #[track_caller]
    pub fn dyn_join_mono(
        &self,
        factories: &JoinFactories<
            MonoIndexedZSet,
            MonoIndexedZSet,
            <NestedCircuit as WithClock>::Time,
            OrdZSet<DynData>,
        >,
        other: &Stream<NestedCircuit, MonoIndexedZSet>,
        join_funcs: TraceJoinFuncs<DynData, DynData, DynData, DynData, DynUnit>,
    ) -> Stream<NestedCircuit, MonoZSet> {
        self.dyn_join(factories, other, join_funcs)
    }

    #[track_caller]
    pub fn dyn_join_index_mono(
        &self,
        factories: &JoinFactories<
            MonoIndexedZSet,
            MonoIndexedZSet,
            <NestedCircuit as WithClock>::Time,
            MonoIndexedZSet,
        >,
        other: &Stream<NestedCircuit, MonoIndexedZSet>,
        join_funcs: TraceJoinFuncs<DynData, DynData, DynData, DynData, DynData>,
    ) -> Stream<NestedCircuit, MonoIndexedZSet> {
        self.dyn_join_generic(factories, other, join_funcs)
    }

    pub fn dyn_antijoin_mono(
        &self,
        factories: &AntijoinFactories<
            MonoIndexedZSet,
            MonoZSet,
            <NestedCircuit as WithClock>::Time,
        >,
        other: &Stream<NestedCircuit, MonoIndexedZSet>,
    ) -> Stream<NestedCircuit, MonoIndexedZSet> {
        self.dyn_antijoin(factories, other)
    }
}

impl<C, I1> Stream<C, I1>
where
    C: Circuit,
    I1: IndexedZSet,
{
    /// See [`Stream::join`].
    #[track_caller]
    pub fn dyn_join<I2, V>(
        &self,
        factories: &JoinFactories<I1, I2, C::Time, OrdZSet<V>>,
        other: &Stream<C, I2>,
        join_funcs: TraceJoinFuncs<I1::Key, I1::Val, I2::Val, V, DynUnit>,
    ) -> Stream<C, OrdZSet<V>>
    where
        I2: IndexedZSet<Key = I1::Key>,
        V: DataTrait + ?Sized,
    {
        self.dyn_join_generic(factories, other, join_funcs)
    }

    /// See [`Stream::join_index`].
    #[track_caller]
    pub fn dyn_join_index<I2, K, V>(
        &self,
        factories: &JoinFactories<I1, I2, C::Time, OrdIndexedZSet<K, V>>,
        other: &Stream<C, I2>,
        join_funcs: TraceJoinFuncs<I1::Key, I1::Val, I2::Val, K, V>,
    ) -> Stream<C, OrdIndexedZSet<K, V>>
    where
        I2: IndexedZSet<Key = I1::Key>,
        K: DataTrait + ?Sized,
        V: DataTrait + ?Sized,
    {
        self.dyn_join_generic(factories, other, join_funcs)
    }

    /// Like [`Self::dyn_join_index`], but can return any indexed Z-set type.
    #[track_caller]
    pub fn dyn_join_generic<I2, Z>(
        &self,
        factories: &JoinFactories<I1, I2, C::Time, Z>,
        other: &Stream<C, I2>,
        join_funcs: TraceJoinFuncs<I1::Key, I1::Val, I2::Val, Z::Key, Z::Val>,
    ) -> Stream<C, Z>
    where
        I2: IndexedZSet<Key = I1::Key>,
        Z: IndexedZSet,
    {
        // TODO: I think this is correct, but we need a proper proof.

        // We use the following formula for nested incremental join with arbitrary
        // nesting depth:
        //
        // ```
        // (↑(a ⋈ b)∆)[t] =
        //      __         __            __
        //      ╲          ╲             ╲
        //      ╱          ╱             ╱  {f(k,v1,v2), w1*w2}
        //      ‾‾         ‾‾            ‾‾
        //     k∈K  (t1,t2).t1\/t2=t  (k,v1,w1)∈a[t1]
        //                            (k,v2,w2)∈b[t2]
        // ```
        // where `t1\/t2 = t1.join(t2)` is the least upper bound of logical timestamps
        // t1 and t2, `f` is the join function that combines values from input streams
        // `a` and `b`.  This sum can be split into two terms `left + right`:
        //
        // ```
        //           __         __            __
        //           ╲          ╲             ╲
        // left=     ╱          ╱             ╱  {f(k,v1,v2), w1*w2}
        //           ‾‾         ‾‾            ‾‾
        //          k∈K  (t1,t2).t1\/t2=t  (k,v1,w1)∈a[t1]
        //                 and t2<t1       (k,v2,w2)∈b[t2]
        //           __         __            __
        //           ╲          ╲             ╲
        // right=    ╱          ╱             ╱  {f(k,v1,v2), w1*w2}
        //           ‾‾         ‾‾            ‾‾
        //          k∈K  (t1,t2).t1\/t2=t  (k,v1,w1)∈a[t1]
        //                 and t2>=t1      (k,v2,w2)∈b[t2]
        // ```
        // where `t2<t1` and `t2>=t1` refer to the total order in which timestamps are
        // observed during the execution of the circuit, not their logical partial
        // order.  In particular, all iterations of an earlier clock epoch precede the
        // first iteration of a newer epoch.
        //
        // The advantage of this representation is that each term can be computed
        // as a join of one of the input streams with the trace of the other stream,
        // implemented by the `JoinTrace` operator.
        self.circuit().region("join", || {
            let left = self.dyn_shard(&factories.left_factories);
            let right = other.dyn_shard(&factories.right_factories);

            let left_trace = left
                .dyn_accumulate_trace(&factories.left_trace_factories, &factories.left_factories);
            let right_trace = right
                .dyn_accumulate_trace(&factories.right_trace_factories, &factories.right_factories);

            let left = self.circuit().add_binary_operator(
                StreamingBinaryWrapper::new(JoinTrace::new(
                    &factories.right_trace_factories,
                    &factories.output_factories,
                    factories.timed_item_factory,
                    factories.timed_items_factory,
                    join_funcs.left,
                    Location::caller(),
                    self.circuit().clone(),
                )),
                &left.dyn_accumulate(&factories.left_factories),
                &right_trace,
            );

            let right = self.circuit().add_binary_operator(
                StreamingBinaryWrapper::new(JoinTrace::new(
                    &factories.left_trace_factories,
                    &factories.output_factories,
                    factories.timed_item_factory,
                    factories.timed_items_factory,
                    join_funcs.right,
                    Location::caller(),
                    self.circuit().clone(),
                )),
                &right.dyn_accumulate(&factories.right_factories),
                &left_trace.accumulate_delay_trace(),
            );

            left.plus(&right)
        })
    }

    /// See [`Stream::antijoin`].
    pub fn dyn_antijoin<I2>(
        &self,
        factories: &AntijoinFactories<I1, OrdZSet<I2::Key>, C::Time>,
        other: &Stream<C, I2>,
    ) -> Stream<C, I1>
    where
        I2: IndexedZSet<Key = I1::Key> + DynFilterMap + Send,
        Box<I1::Key>: Clone,
        Box<I1::Val>: Clone,
    {
        self.circuit()
            .cache_get_or_insert_with(
                AntijoinId::new((self.stream_id(), other.stream_id())),
                move || {
                    self.circuit().region("antijoin", || {
                        // Used to assign persistent ids to intermediate operators below.
                        let antijoin_pid = if let (Some(pid1), Some(pid2)) =
                            (self.get_persistent_id(), other.get_persistent_id())
                        {
                            Some(format!("{}.antijoin.{}", pid1, pid2))
                        } else {
                            None
                        };

                        let stream1 = self.dyn_shard(&factories.join_factories.left_factories);

                        // Project away values, leave keys only.
                        let other_keys = other
                            .try_sharded_version()
                            .dyn_map(
                                &factories.join_factories.right_factories,
                                Box::new(|item: <I2 as DynFilterMap>::DynItemRef<'_>, output| {
                                    <I2 as DynFilterMap>::item_ref_keyval(item)
                                        .0
                                        .clone_to(output.fst_mut())
                                }),
                            )
                            .set_persistent_id(
                                antijoin_pid
                                    .as_deref()
                                    .map(|pid| format!("{pid}.keys"))
                                    .as_deref(),
                            );

                        // `dyn_map` above preserves keys.
                        other_keys.mark_sharded_if(other);

                        let stream2 = other_keys
                            .dyn_distinct(&factories.distinct_factories)
                            .set_persistent_id(
                                antijoin_pid
                                    .as_deref()
                                    .map(|pid| format!("{pid}.distinct"))
                                    .as_deref(),
                            )
                            .dyn_shard(&factories.join_factories.right_factories);

                        //map_func: Box<dyn Fn(B::DynItemRef<'_>, &mut DynPair<K, DynUnit>)>,

                        let mut key = factories
                            .join_factories
                            .output_factories
                            .key_factory()
                            .default_box();
                        let mut val = factories
                            .join_factories
                            .output_factories
                            .val_factory()
                            .default_box();
                        stream1
                            .minus(&stream1.dyn_join_generic(
                                &factories.join_factories,
                                &stream2,
                                TraceJoinFuncs::new(move |k: &I1::Key, v1: &I1::Val, _v2, cb| {
                                    k.clone_to(&mut key);
                                    v1.clone_to(&mut val);
                                    cb(key.as_mut(), val.as_mut())
                                }),
                            ))
                            .mark_sharded()
                    })
                },
            )
            .clone()
    }
}

impl<C, Z> Stream<C, Z>
where
    C: Circuit,
    Z: IndexedZSet,
{
    /// See [Stream::outer_join].
    pub fn dyn_outer_join<Z2, O>(
        &self,
        factories: &OuterJoinFactories<Z, Z2, C::Time, O>,
        other: &Stream<C, Z2>,
        join_funcs: TraceJoinFuncs<Z::Key, Z::Val, Z2::Val, O, DynUnit>,
        left_func: Box<
            dyn FnMut(<Z as DynFilterMap>::DynItemRef<'_>, &mut dyn FnMut(&mut O, &mut DynUnit)),
        >,
        right_func: Box<
            dyn FnMut(<Z2 as DynFilterMap>::DynItemRef<'_>, &mut dyn FnMut(&mut O, &mut DynUnit)),
        >,
    ) -> Stream<C, OrdZSet<O>>
    where
        Z: DynFilterMap + Send,
        Z2: IndexedZSet<Key = Z::Key> + Send,
        Z2: DynFilterMap,
        O: DataTrait + ?Sized,
        Box<Z::Key>: Clone,
        Box<Z::Val>: Clone,
        Box<Z2::Val>: Clone,
    {
        let center: Stream<C, OrdZSet<O>> =
            self.dyn_join_generic(&factories.join_factories, other, join_funcs);

        let factories1 = AntijoinFactories {
            join_factories: factories.left_join_factories.clone(),
            distinct_factories: factories.right_distinct_factories.clone(),
        };
        let factories2 = AntijoinFactories {
            join_factories: factories.right_join_factories.clone(),
            distinct_factories: factories.left_distinct_factories.clone(),
        };

        let left = self
            .dyn_antijoin(&factories1, other)
            .dyn_flat_map_generic(&factories.output_factories, left_func);
        let right = other
            .dyn_antijoin(&factories2, self)
            .dyn_flat_map_generic(&factories.output_factories, right_func);
        center.sum(&[left, right])
    }

    /// Like [`Stream::dyn_outer_join`], but uses default value for the missing side of the
    /// join.
    pub fn dyn_outer_join_default<Z2, O>(
        &self,
        factories: &OuterJoinFactories<Z, Z2, C::Time, O>,
        other: &Stream<C, Z2>,
        join_funcs: TraceJoinFuncs<Z::Key, Z::Val, Z2::Val, O, DynUnit>,
    ) -> Stream<C, OrdZSet<O>>
    where
        Z: for<'a> DynFilterMap<
                DynItemRef<'a> = (&'a <Z as BatchReader>::Key, &'a <Z as BatchReader>::Val),
            > + Send,
        Z2: IndexedZSet<Key = Z::Key> + Send,
        Z2: for<'a> DynFilterMap<
            DynItemRef<'a> = (&'a <Z2 as BatchReader>::Key, &'a <Z2 as BatchReader>::Val),
        >,
        O: DataTrait + ?Sized,
        Box<Z::Key>: Clone,
        Box<Z::Val>: Clone,
        Box<Z2::Val>: Clone,
    {
        let mut join_func_left = join_funcs.left.fork();
        let mut join_func_right = join_funcs.left.fork();
        let default1 = factories
            .join_factories
            .left_factories
            .val_factory()
            .default_box();
        let default2 = factories
            .join_factories
            .right_factories
            .val_factory()
            .default_box();

        self.dyn_outer_join(
            factories,
            other,
            join_funcs,
            Box::new(move |(k, v1), cb| join_func_left(k, v1, &default2, &mut |x, u| cb(x, u))),
            Box::new(move |(k, v2), cb| join_func_right(k, &default1, v2, &mut |x, u| cb(x, u))),
        )
    }
}

/// Join two streams of batches.
///
/// See [`Stream::join`](`crate::circuit::Stream::join`).
pub struct Join<I1, I2, Z>
where
    I1: IndexedZSetReader,
    I2: IndexedZSetReader,
    Z: ZSet,
{
    output_factories: Z::Factories,
    join_func: JoinFunc<I1::Key, I1::Val, I2::Val, Z::Key>,
    location: &'static Location<'static>,
    _types: PhantomData<(I1, I2, Z)>,
}

impl<I1, I2, Z> Join<I1, I2, Z>
where
    I1: IndexedZSetReader,
    I2: IndexedZSetReader,
    Z: ZSet,
{
    pub fn new(
        output_factories: &Z::Factories,
        join_func: JoinFunc<I1::Key, I1::Val, I2::Val, Z::Key>,
        location: &'static Location<'static>,
    ) -> Self {
        Self {
            output_factories: output_factories.clone(),
            join_func,
            location,
            _types: PhantomData,
        }
    }
}

impl<I1, I2, Z> Operator for Join<I1, I2, Z>
where
    I1: IndexedZSetReader,
    I2: IndexedZSetReader,
    Z: ZSet,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("Join")
    }

    fn location(&self) -> OperatorLocation {
        Some(self.location)
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<I1, I2, Z> BinaryOperator<I1, I2, Z> for Join<I1, I2, Z>
where
    I1: IndexedZSetReader,
    I2: IndexedZSetReader<Key = I1::Key>,
    Z: ZSet,
{
    #[trace]
    async fn eval(&mut self, i1: &I1, i2: &I2) -> Z {
        let mut cursor1 = i1.cursor();
        let mut cursor2 = i2.cursor();

        let mut output = self.output_factories.weighted_item_factory().default_box();

        // Choose capacity heuristically.
        let mut batch = self.output_factories.weighted_items_factory().default_box();
        batch.reserve(min(i1.len(), i2.len()));

        while cursor1.key_valid() && cursor2.key_valid() {
            match cursor1.key().cmp(cursor2.key()) {
                Ordering::Less => cursor1.seek_key(cursor2.key()),
                Ordering::Greater => cursor2.seek_key(cursor1.key()),
                Ordering::Equal => {
                    while cursor1.val_valid() {
                        let w1 = **cursor1.weight();
                        let v1 = cursor1.val();
                        while cursor2.val_valid() {
                            let w2 = **cursor2.weight();
                            let v2 = cursor2.val();

                            let (kv, w) = output.split_mut();
                            let (k, _v) = kv.split_mut();

                            (self.join_func)(cursor1.key(), v1, v2, k);
                            **w = w1.mul_by_ref(&w2);

                            batch.push_val(output.as_mut());
                            cursor2.step_val();
                        }

                        cursor2.rewind_vals();
                        cursor1.step_val();
                    }

                    cursor1.step_key();
                    cursor2.step_key();
                }
            }
        }

        Z::dyn_from_tuples(&self.output_factories, (), &mut batch)
    }

    // TODO: Impls using consumers
}

pub struct MonotonicJoin<I1, I2, Z>
where
    I1: IndexedZSetReader,
    I2: IndexedZSetReader,
    Z: ZSet,
{
    output_factories: Z::Factories,
    join_func: JoinFunc<I1::Key, I1::Val, I2::Val, Z::Key>,
    location: &'static Location<'static>,
    _types: PhantomData<(I1, I2, Z)>,
}

impl<I1, I2, Z> MonotonicJoin<I1, I2, Z>
where
    I1: IndexedZSetReader,
    I2: IndexedZSetReader,
    Z: ZSet,
{
    pub fn new(
        output_factories: &Z::Factories,
        join_func: JoinFunc<I1::Key, I1::Val, I2::Val, Z::Key>,
        location: &'static Location<'static>,
    ) -> Self {
        Self {
            output_factories: output_factories.clone(),
            join_func,
            location,
            _types: PhantomData,
        }
    }
}

impl<I1, I2, Z> Operator for MonotonicJoin<I1, I2, Z>
where
    I1: IndexedZSetReader,
    I2: IndexedZSetReader,
    Z: ZSet,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("MonotonicJoin")
    }

    fn location(&self) -> OperatorLocation {
        Some(self.location)
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<I1, I2, Z> BinaryOperator<I1, I2, Z> for MonotonicJoin<I1, I2, Z>
where
    I1: IndexedZSetReader,
    I2: IndexedZSetReader<Key = I1::Key>,
    Z: ZSet,
{
    #[trace]
    async fn eval(&mut self, i1: &I1, i2: &I2) -> Z {
        let mut cursor1 = i1.cursor();
        let mut cursor2 = i2.cursor();

        let output_key_factory = self.output_factories.key_factory();

        // Choose capacity heuristically.
        let mut builder =
            Z::Builder::with_capacity(&self.output_factories, min(i1.len(), i2.len()));

        let mut output = output_key_factory.default_box();

        while cursor1.key_valid() && cursor2.key_valid() {
            match cursor1.key().cmp(cursor2.key()) {
                Ordering::Less => cursor1.seek_key(cursor2.key()),
                Ordering::Greater => cursor2.seek_key(cursor1.key()),
                Ordering::Equal => {
                    while cursor1.val_valid() {
                        let w1 = **cursor1.weight();
                        let v1 = cursor1.val();
                        while cursor2.val_valid() {
                            let w2 = **cursor2.weight();
                            let v2 = cursor2.val();

                            (self.join_func)(cursor1.key(), v1, v2, output.as_mut());

                            builder
                                .push_val_diff_mut(().erase_mut(), w1.mul_by_ref(&w2).erase_mut());
                            builder.push_key_mut(output.as_mut());
                            cursor2.step_val();
                        }

                        cursor2.rewind_vals();
                        cursor1.step_val();
                    }

                    cursor1.step_key();
                    cursor2.step_key();
                }
            }
        }

        builder.done()
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
struct JoinStats {
    lhs_tuples: usize,
    rhs_tuples: usize,
    output_tuples: usize,
    output_batch_stats: BatchSizeStats,
}

impl JoinStats {
    pub const fn new() -> Self {
        Self {
            lhs_tuples: 0,
            rhs_tuples: 0,
            output_tuples: 0,
            output_batch_stats: BatchSizeStats::new(),
        }
    }

    pub fn add_output_batch<Z: ZBatch>(&mut self, batch: &Z) {
        self.output_batch_stats.add_batch(batch.len())
    }
}

pub struct JoinTrace<I, B, T, Z, Clk>
where
    I: ZBatch,
    B: ZBatch,
    T: ZBatchReader,
    Z: IndexedZSet,
{
    right_factories: T::Factories,
    output_factories: Z::Factories,
    timed_item_factory:
        &'static dyn Factory<DynPair<DynDataTyped<T::Time>, WeightedItem<Z::Key, Z::Val, Z::R>>>,
    clock: Clk,
    timed_items_factory:
        &'static dyn Factory<DynPairs<DynDataTyped<T::Time>, WeightedItem<Z::Key, Z::Val, Z::R>>>,
    join_func: RefCell<TraceJoinFunc<I::Key, I::Val, T::Val, Z::Key, Z::Val>>,
    location: &'static Location<'static>,
    // Future updates computed ahead of time, indexed by time
    // when each set of updates should be output.
    future_outputs: RefCell<HashMap<T::Time, Spine<Z>>>,
    // True if empty input batch was received at the current clock cycle.
    empty_input: RefCell<bool>,
    // True if empty output was produced at the current clock cycle.
    empty_output: RefCell<bool>,
    stats: RefCell<JoinStats>,
    _types: PhantomData<(I, B, T, Z)>,
}

impl<I, B, T, Z, Clk> JoinTrace<I, B, T, Z, Clk>
where
    I: ZBatch,
    B: ZBatch,
    T: ZBatchReader,
    Z: IndexedZSet,
{
    pub fn new(
        right_factories: &T::Factories,
        output_factories: &Z::Factories,
        timed_item_factory: &'static dyn Factory<
            DynPair<DynDataTyped<T::Time>, WeightedItem<Z::Key, Z::Val, Z::R>>,
        >,
        timed_items_factory: &'static dyn Factory<
            DynPairs<DynDataTyped<T::Time>, WeightedItem<Z::Key, Z::Val, Z::R>>,
        >,
        join_func: TraceJoinFunc<I::Key, I::Val, T::Val, Z::Key, Z::Val>,
        location: &'static Location<'static>,
        clock: Clk,
    ) -> Self {
        Self {
            right_factories: right_factories.clone(),
            output_factories: output_factories.clone(),
            timed_item_factory,
            timed_items_factory,
            clock,
            join_func: RefCell::new(join_func),
            location,
            future_outputs: RefCell::new(HashMap::new()),
            empty_input: RefCell::new(false),
            empty_output: RefCell::new(false),
            stats: RefCell::new(JoinStats::new()),
            _types: PhantomData,
        }
    }
}

impl<I, B, T, Z, Clk> Operator for JoinTrace<I, B, T, Z, Clk>
where
    I: ZBatch,
    B: ZBatch,
    T: ZBatchReader,
    Z: IndexedZSet,
    Clk: WithClock<Time = T::Time> + 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("JoinTrace")
    }

    fn location(&self) -> OperatorLocation {
        Some(self.location)
    }

    fn clock_start(&mut self, scope: Scope) {
        if scope == 0 {
            *self.empty_input.borrow_mut() = false;
            *self.empty_output.borrow_mut() = false;
        }
    }

    fn clock_end(&mut self, _scope: Scope) {
        debug_assert!(self
            .future_outputs
            .borrow()
            .keys()
            .all(|time| !time.less_equal(&self.clock.time())));
    }

    fn metadata(&self, meta: &mut OperatorMeta) {
        let stats = self.stats.borrow();
        let total_size: usize = self
            .future_outputs
            .borrow()
            .values()
            .map(|spine| spine.len())
            .sum();

        let batch_sizes = MetaItem::Array(
            self.future_outputs
                .borrow()
                .values()
                .map(|batcher| {
                    let size = batcher.size_of();

                    MetaItem::Map(
                        metadata! {
                            "allocated" => MetaItem::bytes(size.total_bytes()),
                            "used" => MetaItem::bytes(size.used_bytes()),
                        }
                        .into(),
                    )
                })
                .collect(),
        );

        let bytes = {
            let mut context = Context::new();
            for batcher in self.future_outputs.borrow().values() {
                batcher.size_of_with_context(&mut context);
            }

            context.size_of()
        };

        // Find the percentage of consolidated outputs
        let output_redundancy = MetaItem::Percent {
            numerator: (stats.output_tuples - stats.output_batch_stats.total_size()) as u64,
            denominator: stats.output_tuples as u64,
        };

        meta.extend(metadata! {
            NUM_ENTRIES_LABEL => MetaItem::Count(total_size),
            "batch sizes" => batch_sizes,
            USED_BYTES_LABEL => MetaItem::bytes(bytes.used_bytes()),
            "allocations" => MetaItem::Count(bytes.distinct_allocations()),
            SHARED_BYTES_LABEL => MetaItem::bytes(bytes.shared_bytes()),
            "left inputs" => stats.lhs_tuples,
            "right inputs" => stats.rhs_tuples,
            "computed outputs" => stats.output_tuples,
            OUTPUT_BATCHES_LABEL => stats.output_batch_stats.metadata(),
            "output redundancy" => output_redundancy,
        });
    }

    fn fixedpoint(&self, scope: Scope) -> bool {
        let epoch_end = self.clock.time().epoch_end(scope);
        // We're in a stable state if input and output at the current clock cycle are
        // both empty, and there are no precomputed outputs before the end of the
        // clock epoch.
        *self.empty_input.borrow()
            && *self.empty_output.borrow()
            && self
                .future_outputs
                .borrow()
                .keys()
                .all(|time| !time.less_equal(&epoch_end))
    }
}

impl<I, B, T, Z, Clk> StreamingBinaryOperator<Option<Spine<I>>, T, Z> for JoinTrace<I, B, T, Z, Clk>
where
    I: ZBatch<Time = ()>,
    B: ZBatch<Key = I::Key>,
    T: ZBatchReader<Key = B::Key, Val = B::Val, Time = B::Time> + WithSnapshot<Batch = B>,
    Z: IndexedZSet,
    Clk: WithClock<Time = T::Time> + 'static,
{
    #[trace]
    fn eval(
        self: Rc<Self>,
        delta: &Option<Spine<I>>,
        trace: &T,
    ) -> impl futures::Stream<Item = (Z, bool, Option<Position>)> + 'static {
        let chunk_size = splitter_output_chunk_size();

        let delta = delta.as_ref().map(|b| b.ro_snapshot());

        let trace = if delta.is_some() {
            Some(trace.ro_snapshot())
        } else {
            None
        };

        stream! {
            let Some(delta) = delta else {
                // println!("yield empty");
                yield(Z::dyn_empty(&self.output_factories), true, None);
                return;
            };
            let trace = trace.unwrap();

            *self.empty_input.borrow_mut() = delta.is_empty();
            *self.empty_output.borrow_mut() = true;
            self.stats.borrow_mut().lhs_tuples += delta.len();
            self.stats.borrow_mut().rhs_tuples = trace.len();

            let fetched = if Runtime::with_dev_tweaks(|dev_tweaks| dev_tweaks.fetch_join) {
                trace.fetch(&delta).await
            } else {
                None
            };

            let mut delta_cursor = delta.cursor();

            let mut trace_cursor = if let Some(fetched) = fetched.as_ref() {
                fetched.get_cursor()
            } else {
                Box::new(trace.cursor())
            };

            let mut val = self.right_factories.val_factory().default_box();

            let batch = if size_of::<T::Time>() != 0 {
                let time = self.clock.time();

                // Buffer to collect output tuples.
                // One allocation per clock tick is acceptable; however the actual output can be
                // larger than `index.len()`.  If re-allocations becomes a problem, we
                // may need to do something smarter, like a chain of buffers.
                // TODO: Sub-scopes can cause a lot of inner clock cycles to be set off, so this
                //       actually could be significant
                let mut output_tuples = self.timed_items_factory.default_box();
                output_tuples.reserve(chunk_size);

                let mut timed_item = self.timed_item_factory.default_box();

                while delta_cursor.key_valid() {
                    if trace_cursor.seek_key_exact(delta_cursor.key()) {
                        //println!("key: {}", index_cursor.key(index));

                        while delta_cursor.val_valid() {
                            let w1 = **delta_cursor.weight();
                            let v1 = delta_cursor.val();
                            //println!("v1: {}, w1: {}", v1, w1);

                            while trace_cursor.val_valid() {
                                // FIXME: this clone is only needed to avoid borrow checker error due to
                                // borrowing `trace_cursor` below.
                                trace_cursor.val().clone_to(val.as_mut());

                                (self.join_func.borrow_mut())(delta_cursor.key(), v1, &val, &mut |k, v| {
                                        trace_cursor
                                        .map_times(&mut |ts: &T::Time, w2: &T::R| {
                                            let (time_ref, item) = timed_item.split_mut();
                                            let (kv, w) = item.split_mut();
                                            let (key, val) = kv.split_mut();

                                            **w = w1.mul_by_ref(&**w2);
                                            k.clone_to(key);
                                            v.clone_to(val);
                                            **time_ref = ts.join(&time);
                                            output_tuples.push_val(timed_item.as_mut());
                                        });
                                });
                                trace_cursor.step_val();
                            }
                            trace_cursor.rewind_vals();
                            delta_cursor.step_val();
                        }
                    }
                    delta_cursor.step_key();
                }

                self.stats.borrow_mut().output_tuples += output_tuples.len();

                // Sort `output_tuples` by timestamp and push all tuples for each unique
                // timestamp to the appropriate spine.
                output_tuples.sort_by_key();

                let mut batch = self.output_factories.weighted_items_factory().default_box();
                let mut start: usize = 0;

                while start < output_tuples.len() {
                    let batch_time = output_tuples[start].fst().deref().clone();

                    let run_length =
                        output_tuples.advance_while(start, output_tuples.len(), &|tuple| {
                            tuple.fst().deref() == &batch_time
                        });
                    batch.reserve(run_length);

                    for i in start..start + run_length {
                        batch.push_val(unsafe { output_tuples.index_mut_unchecked(i) }.snd_mut());
                    }

                    start += run_length;

                    self.future_outputs.borrow_mut().entry(batch_time).or_insert_with(|| {
                        let mut spine = <Spine<Z> as Trace>::new(&self.output_factories);
                        spine.insert(Z::dyn_from_tuples(&self.output_factories, (), &mut batch));
                        spine
                    });
                    batch.clear();
                }

                // Consolidate the spine for the current timestamp and return it.
                let output = self.future_outputs.borrow_mut()
                    .remove(&time)
                    .and_then(|spine| spine.consolidate())
                    .unwrap_or_else(|| Z::dyn_empty(&self.output_factories));

                output
            } else {
                let mut output_tuples = self.output_factories.weighted_items_factory().default_box();
                output_tuples.reserve(chunk_size);
                let mut batcher = Z::Batcher::new_batcher(&self.output_factories, ());

                let mut output_tuple = self.output_factories.weighted_item_factory().default_box();

                while delta_cursor.key_valid() {
                    if trace_cursor.seek_key_exact(delta_cursor.key()) {
                        //println!("key: {}", index_cursor.key(index));

                        while delta_cursor.val_valid() {
                            let w1 = **delta_cursor.weight();
                            let v1 = delta_cursor.val();
                            //println!("v1: {}, w1: {}", v1, w1);

                            while trace_cursor.val_valid() {
                                // FIXME: this clone is only needed to avoid borrow checker error due to
                                // borrowing `trace_cursor` below.
                                trace_cursor.val().clone_to(val.as_mut());

                                (self.join_func.borrow_mut())(delta_cursor.key(), v1, &val, &mut |k, v| {
                                    trace_cursor
                                        .map_times(&mut |_ts: &T::Time, w2: &T::R| {
                                            let (kv, w) = output_tuple.split_mut();
                                            let (key, val) = kv.split_mut();

                                            **w = w1.mul_by_ref(&**w2);
                                            k.clone_to(key);
                                            v.clone_to(val);
                                            output_tuples.push_val(output_tuple.as_mut());
                                        });
                                });

                                // Push a sufficiently large chunk of update to the batcher. The batcher
                                // will consolidate the updates and possibly merge them with previous updates.
                                // Yield if the batcher has accumulated enough tuples. The divisor of 3 guarantees that
                                // the output batch won't exceed `chunk_size` by more than 33%. Alternatively we could
                                // push every individual output tuple to the batcher, but that's probably inefficient.
                                if output_tuples.len() >= chunk_size / 3 {
                                    self.stats.borrow_mut().output_tuples += output_tuples.len();
                                    batcher.push_batch(&mut output_tuples);

                                    if batcher.tuples() >= chunk_size {
                                        *self.empty_output.borrow_mut() = false;
                                        let batch = batcher.seal();
                                        self.stats.borrow_mut().add_output_batch(&batch);

                                        yield (batch, false, delta_cursor.position());
                                        batcher = Z::Batcher::new_batcher(&self.output_factories, ());
                                    }
                                }

                                trace_cursor.step_val();
                            }

                            trace_cursor.rewind_vals();
                            delta_cursor.step_val();
                        }
                    }
                    delta_cursor.step_key();
                }

                self.stats.borrow_mut().output_tuples += output_tuples.len();
                batcher.push_batch(&mut output_tuples);
                batcher.seal()
            };

            // println!(
            //     "{}: join produces {} outputs (final = {:?}):{:?}",
            //     Runtime::worker_index(),
            //     batch.len(),
            //     self.state.is_none(),
            //     batch
            // );

            self.stats.borrow_mut().add_output_batch(&batch);

            if !batch.is_empty() {
                *self.empty_output.borrow_mut() = false;
            }

            yield (batch, true, delta_cursor.position())
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{
        circuit::CircuitConfig,
        indexed_zset,
        operator::Generator,
        typed_batch::{OrdZSet, TypedBatch},
        utils::Tup2,
        zset, Circuit, RootCircuit, Runtime, Stream,
    };
    use std::vec;

    fn do_join_test(workers: usize, transaction: bool) {
        let mut input1 = vec![
            vec![
                Tup2(Tup2(1, "a".to_string()), 1i64),
                Tup2(Tup2(1, "b".to_string()), 2),
                Tup2(Tup2(2, "c".to_string()), 3),
                Tup2(Tup2(2, "d".to_string()), 4),
                Tup2(Tup2(3, "e".to_string()), 5),
                Tup2(Tup2(3, "f".to_string()), -2),
            ],
            vec![Tup2(Tup2(1, "a".to_string()), 1)],
            vec![Tup2(Tup2(1, "a".to_string()), 1)],
            vec![Tup2(Tup2(4, "n".to_string()), 2)],
            vec![Tup2(Tup2(1, "a".to_string()), 0)],
        ]
        .into_iter();
        let mut input2 = vec![
            vec![
                Tup2(Tup2(2, "g".to_string()), 3i64),
                Tup2(Tup2(2, "h".to_string()), 4),
                Tup2(Tup2(3, "i".to_string()), 5),
                Tup2(Tup2(3, "j".to_string()), -2),
                Tup2(Tup2(4, "k".to_string()), 5),
                Tup2(Tup2(4, "l".to_string()), -2),
            ],
            vec![Tup2(Tup2(1, "b".to_string()), 1)],
            vec![Tup2(Tup2(4, "m".to_string()), 1)],
            vec![],
            vec![],
        ]
        .into_iter();

        let inc_outputs_vec = vec![
            zset! {
                Tup2(2, "c g".to_string()) => 9,
                Tup2(2, "c h".to_string()) => 12,
                Tup2(2, "d g".to_string()) => 12,
                Tup2(2, "d h".to_string()) => 16,
                Tup2(3, "e i".to_string()) => 25,
                Tup2(3, "e j".to_string()) => -10,
                Tup2(3, "f i".to_string()) => -10,
                Tup2(3, "f j".to_string()) => 4
            },
            zset! {
                Tup2(1, "a b".to_string()) => 2,
                Tup2(1, "b b".to_string()) => 2,
            },
            zset! {
                Tup2(1, "a b".to_string()) => 1,
            },
            zset! {
                Tup2(4, "n k".to_string()) => 10,
                Tup2(4, "n l".to_string()) => -4,
                Tup2(4, "n m".to_string()) => 2,
            },
            zset! {},
        ];
        let inc_filtered_outputs_vec = vec![
            zset! {
                Tup2(2, "c g".to_string()) => 9,
                Tup2(2, "c h".to_string()) => 12,
                Tup2(2, "d g".to_string()) => 12,
                Tup2(2, "d h".to_string()) => 16,
                Tup2(3, "e i".to_string()) => 25,
                Tup2(3, "e j".to_string()) => -10,
                Tup2(3, "f i".to_string()) => -10,
                Tup2(3, "f j".to_string()) => 4
            },
            zset! {
                Tup2(1, "b b".to_string()) => 2,
            },
            zset! {},
            zset! {
                Tup2(4, "n k".to_string()) => 10,
                Tup2(4, "n l".to_string()) => -4,
                Tup2(4, "n m".to_string()) => 2,
            },
            zset! {},
        ];

        let outputs = vec![
            zset! {
                Tup2(2, "c g".to_string()) => 9i64,
                Tup2(2, "c h".to_string()) => 12,
                Tup2(2, "d g".to_string()) => 12,
                Tup2(2, "d h".to_string()) => 16,
                Tup2(3, "e i".to_string()) => 25,
                Tup2(3, "e j".to_string()) => -10,
                Tup2(3, "f i".to_string()) => -10,
                Tup2(3, "f j".to_string()) => 4
            },
            zset! {
                Tup2(1, "a b".to_string()) => 1,
            },
            zset! {},
            zset! {},
            zset! {},
        ];

        let (
            mut circuit,
            (input_handle1, input_handle2, join_output, join_flatmap_output, stream_join_output),
        ) = Runtime::init_circuit(
            CircuitConfig::from(workers).with_splitter_chunk_size_records(2),
            move |circuit| {
                let (input1, input_handle1) = circuit.add_input_zset::<Tup2<u64, String>>();
                let index1 = input1.map_index(|Tup2(k, v)| (*k, v.clone()));

                let (input2, input_handle2) = circuit.add_input_zset::<Tup2<u64, String>>();
                let index2 = input2.map_index(|Tup2(k, v)| (*k, v.clone()));

                let stream_join_output = circuit
                    .non_incremental(
                        &(index1.clone(), index2.clone()),
                        |_child, (index1, index2)| {
                            Ok(index1.stream_join(index2, |&k: &u64, s1, s2| {
                                Tup2(k, format!("{} {}", s1, s2))
                            }))
                        },
                    )
                    .unwrap()
                    .accumulate_output();

                /*index1
                .join_incremental(&index2, |&k: &u64, s1, s2| Tup2(k, format!("{} {}", s1, s2)))
                .gather(0)
                .inspect(move |fm: &OrdZSet<Tup2<u64, String>>| {
                    if Runtime::worker_index() == 0 {
                        assert_eq!(fm, &inc_outputs.next().unwrap())
                    }
                });*/

                let join_output = index1
                    .join(&index2, |&k: &u64, s1, s2| {
                        Tup2(k, format!("{} {}", s1, s2))
                    })
                    .accumulate_output();

                let join_flatmap_output = index1
                    .join_flatmap(&index2, |&k: &u64, s1, s2| {
                        if s1.as_str() == "a" {
                            None
                        } else {
                            Some(Tup2(k, format!("{} {}", s1, s2)))
                        }
                    })
                    .accumulate_output();

                Ok((
                    input_handle1,
                    input_handle2,
                    join_output,
                    join_flatmap_output,
                    stream_join_output,
                ))
            },
        )
        .unwrap();

        if transaction {
            let inc_output = TypedBatch::merge_batches(inc_outputs_vec.clone());
            let inc_filtered_output = TypedBatch::merge_batches(inc_filtered_outputs_vec.clone());

            circuit.start_transaction().unwrap();

            for _ in 0..5 {
                input_handle1.append(&mut input1.next().unwrap());
                input_handle2.append(&mut input2.next().unwrap());
                circuit.step().unwrap();
            }

            circuit.commit_transaction().unwrap();

            assert_eq!(join_output.concat().consolidate(), inc_output);
            assert_eq!(
                join_flatmap_output.concat().consolidate(),
                inc_filtered_output
            );
        } else {
            let mut inc_output_vec = inc_outputs_vec.clone().into_iter();
            let mut inc_filtered_outputs_vec = inc_filtered_outputs_vec.clone().into_iter();
            let mut output = outputs.clone().into_iter();

            for _ in 0..5 {
                input_handle1.append(&mut input1.next().unwrap());
                input_handle2.append(&mut input2.next().unwrap());
                circuit.transaction().unwrap();

                assert_eq!(
                    join_output.concat().consolidate(),
                    inc_output_vec.next().unwrap()
                );
                assert_eq!(
                    join_flatmap_output.concat().consolidate(),
                    inc_filtered_outputs_vec.next().unwrap()
                );
                assert_eq!(
                    stream_join_output.concat().consolidate(),
                    output.next().unwrap()
                );
            }
        }

        circuit.kill().unwrap()
    }

    #[test]
    fn join_test_mt() {
        do_join_test(1, false);
        do_join_test(2, false);
        do_join_test(4, false);
        do_join_test(16, false);
    }

    #[test]
    fn join_test_mt_one_transaction() {
        do_join_test(1, true);
        do_join_test(2, true);
        do_join_test(4, true);
        do_join_test(16, true);
    }

    // Compute pairwise reachability relation between graph nodes as the
    // transitive closure of the edge relation.
    #[test]
    fn join_trace_test() {
        let circuit = RootCircuit::build(move |circuit| {
            // Changes to the edges relation.
            let mut edges: vec::IntoIter<OrdZSet<Tup2<u64, u64>>> = vec![
                zset! { Tup2(1, 2) => 1 },
                zset! { Tup2(2, 3) => 1},
                zset! { Tup2(1, 3) => 1},
                zset! { Tup2(3, 1) => 1},
                zset! { Tup2(3, 1) => -1},
                zset! { Tup2(1, 2) => -1},
                zset! { Tup2(2, 4) => 1, Tup2(4, 1) => 1 },
                zset! {Tup2 (2, 3) => -1, Tup2(3, 2) => 1 },
            ]
            .into_iter();

            // Expected content of the reachability relation.
            let mut outputs: vec::IntoIter<OrdZSet<Tup2<u64, u64>>> = vec![
                zset! { Tup2(1, 2) => 1 },
                zset! { Tup2(1, 2) => 1, Tup2(2, 3) => 1, Tup2(1, 3) => 1 },
                zset! { Tup2(1, 2) => 1, Tup2(2, 3) => 1, Tup2(1, 3) => 1 },
                zset! { Tup2(1, 1) => 1, Tup2(2, 2) => 1, Tup2(3, 3) => 1, Tup2(1, 2) => 1, Tup2(1, 3) => 1, Tup2(2, 3) => 1, Tup2(2, 1) => 1, Tup2(3, 1) => 1, Tup2(3, 2) => 1},
                zset! { Tup2(1, 2) => 1, Tup2(2, 3) => 1, Tup2(1, 3) => 1 },
                zset! { Tup2(2, 3) => 1, Tup2(1, 3) => 1 },
                zset! { Tup2(1, 3) => 1, Tup2(2, 3) => 1, Tup2(2, 4) => 1, Tup2(2, 1) => 1, Tup2(4, 1) => 1, Tup2(4, 3) => 1 },
                zset! { Tup2(1, 1) => 1, Tup2(2, 2) => 1, Tup2(3, 3) => 1, Tup2(4, 4) => 1,
                              Tup2(1, 2) => 1, Tup2(1, 3) => 1, Tup2(1, 4) => 1,
                              Tup2(2, 1) => 1, Tup2(2, 3) => 1, Tup2(2, 4) => 1,
                              Tup2(3, 1) => 1, Tup2(3, 2) => 1, Tup2(3, 4) => 1,
                              Tup2(4, 1) => 1, Tup2(4, 2) => 1, Tup2(4, 3) => 1 },
            ]
            .into_iter();

            let edges: Stream<_, OrdZSet<Tup2<u64, u64>>> =
                circuit
                    .add_source(Generator::new(move || edges.next().unwrap()));

            let paths = circuit.recursive(|child, paths_delayed: Stream<_, OrdZSet<Tup2<u64, u64>>>| {
                // ```text
                //                             distinct
                //               ┌───┐          ┌───┐
                // edges         │   │          │   │  paths
                // ────┬────────►│ + ├──────────┤   ├────────┬───►
                //     │         │   │          │   │        │
                //     │         └───┘          └───┘        │
                //     │           ▲                         │
                //     │           │                         │
                //     │         ┌─┴─┐                       │
                //     │         │   │                       │
                //     └────────►│ X │ ◄─────────────────────┘
                //               │   │
                //               └───┘
                //               join
                // ```
                let edges = edges.delta0(child);

                let paths_inverted: Stream<_, OrdZSet<Tup2<u64, u64>>> = paths_delayed
                    .map(|&Tup2(x, y)| Tup2(y, x));

                let paths_inverted_indexed = paths_inverted.map_index(|Tup2(k,v)| (*k, *v));
                let edges_indexed = edges.map_index(|Tup2(k, v)| (*k, *v));

                Ok(edges.plus(&paths_inverted_indexed.join(&edges_indexed, |_via, from, to| Tup2(*from, *to))))
            })
            .unwrap();

            paths.integrate().stream_distinct().inspect(move |ps| {
                assert_eq!(*ps, outputs.next().unwrap());
            });
            Ok(())
        })
        .unwrap().0;

        for _ in 0..8 {
            //eprintln!("{}", i);
            circuit.step().unwrap();
        }
    }

    fn antijoin_test(transaction: bool) {
        let inputs1 = [
            vec![
                Tup2(1, Tup2(0, 1)),
                Tup2(1, Tup2(1, 2)),
                Tup2(2, Tup2(0, 1)),
                Tup2(2, Tup2(1, 1)),
            ],
            vec![Tup2(3, Tup2(1, 1))],
            vec![],
            vec![Tup2(2, Tup2(2, 1)), Tup2(4, Tup2(1, 1))],
            vec![],
        ];

        let inputs2 = vec![
            vec![],
            vec![],
            vec![Tup2(1, Tup2(1, 3))],
            vec![Tup2(2, Tup2(5, 1))],
            // Issue https://github.com/feldera/feldera/issues/3365. Multiple values per key in the right-hand input shouldn't
            // produce outputs with negative weights.
            vec![Tup2(2, Tup2(6, 1))],
        ];

        let expected_outputs = vec![
            indexed_zset! {1 => {0 => 1, 1 => 2}, 2 => {0 => 1, 1 => 1}},
            indexed_zset! {3 => {1 => 1}},
            indexed_zset! { 1 => {0 => -1, 1 => -2}},
            indexed_zset! {2 => { 0 => -1, 1 => -1}, 4 => {1 => 1}},
            indexed_zset! {},
        ];

        let (mut circuit, (input1, input2, output)) = Runtime::init_circuit(
            CircuitConfig::from(4).with_splitter_chunk_size_records(2),
            move |circuit| {
                let (input1, input_handle1) = circuit.add_input_indexed_zset::<u64, u64>();
                let (input2, input_handle2) = circuit.add_input_indexed_zset::<u64, u64>();

                let output = input1.antijoin(&input2).accumulate_output();

                Ok((input_handle1, input_handle2, output))
            },
        )
        .unwrap();

        if transaction {
            circuit.start_transaction().unwrap();
            for i in 0..inputs1.len() {
                input1.append(&mut inputs1[i].clone());
                input2.append(&mut inputs2[i].clone());
                circuit.step().unwrap();
            }

            circuit.commit_transaction().unwrap();

            assert_eq!(
                output.concat().consolidate(),
                TypedBatch::merge_batches(expected_outputs)
            );
        } else {
            for i in 0..inputs1.len() {
                input1.append(&mut inputs1[i].clone());
                input2.append(&mut inputs2[i].clone());
                circuit.transaction().unwrap();
                assert_eq!(output.concat().consolidate(), expected_outputs[i]);
            }
        }

        circuit.kill().unwrap();
    }

    #[test]
    fn antijoin_test_small_steps() {
        antijoin_test(false);
    }

    #[test]
    fn antijoin_test_big_step() {
        antijoin_test(true);
    }
}

#[cfg(all(test, not(feature = "backend-mode")))]
mod propagate_test {
    use crate::{
        circuit::WithClock,
        operator::{DelayedFeedback, Generator},
        typed_batch::{OrdZSet, Spine},
        zset, Circuit, FallbackZSet, RootCircuit, Stream, Timestamp,
    };
    use rkyv::{Archive, Deserialize, Serialize};
    use size_of::SizeOf;
    use std::{
        fmt::{Display, Formatter},
        hash::Hash,
        vec,
    };

    #[derive(
        Clone,
        Debug,
        Default,
        Hash,
        Ord,
        PartialOrd,
        Eq,
        PartialEq,
        SizeOf,
        Archive,
        Serialize,
        Deserialize,
    )]
    #[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
    #[archive(compare(PartialEq, PartialOrd))]
    struct Label(pub u64, pub u16);

    impl Display for Label {
        fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
            write!(f, "L({},{})", self.0, self.1)
        }
    }

    #[derive(
        Clone,
        Debug,
        Default,
        Ord,
        PartialOrd,
        Hash,
        Eq,
        PartialEq,
        SizeOf,
        Archive,
        Serialize,
        Deserialize,
    )]
    #[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
    #[archive(compare(PartialEq, PartialOrd))]
    struct Edge(pub u64, pub u64);

    impl Display for Edge {
        fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
            write!(f, "E({},{})", self.0, self.1)
        }
    }
    // Recursively propagate node labels in an acyclic graph.
    // The reason for supporting acyclic graphs only is that we use this to test
    // the join operator in isolation, so we don't want to use `distinct`.
    fn propagate<C>(
        circuit: &C,
        edges: &Stream<C, OrdZSet<Edge>>,
        labels: &Stream<C, OrdZSet<Label>>,
    ) -> Stream<C, OrdZSet<Label>>
    where
        C: Circuit,
        <<C as WithClock>::Time as Timestamp>::Nested: Timestamp,
    {
        let computed_labels = circuit
            .fixedpoint(|child| {
                let edges = edges.delta0(child);
                let labels = labels.delta0(child);

                let computed_labels = <DelayedFeedback<_, OrdZSet<_>>>::new(child);
                let result: Stream<_, OrdZSet<Label>> = labels.plus(computed_labels.stream());

                computed_labels.connect(&result.map_index(|label| (label.0, label.1)).join(
                    &edges.map_index(|edge| (edge.0, edge.1)),
                    |_from, label, to| Label(*to, *label),
                ));

                // FIXME: make sure the `export` API works on typed streams correctly,
                // so that the `inner`/`typed` calls below are not needed.
                Ok(result
                    .integrate_trace()
                    .inner()
                    .export()
                    .typed::<Spine<FallbackZSet<Label>>>())
            })
            .unwrap();

        computed_labels.consolidate()
    }

    #[test]
    fn propagate_test() {
        let circuit = RootCircuit::build(move |circuit| {
            let mut edges: vec::IntoIter<OrdZSet<Edge>> = vec![
                zset! { Edge(1, 2) => 1, Edge(1, 3) => 1, Edge(2, 4) => 1, Edge(3, 4) => 1 },
                zset! { Edge(5, 7) => 1, Edge(6, 7) => 1 },
                zset! { Edge(4, 5) => 1, Edge(4, 6) => 1, },
                zset! { Edge(3, 8) => 1, Edge(8, 9) => 1 },
                zset! { Edge(2, 4) => -1, Edge(7, 10) => 1 },
                zset! { Edge(3, 4) => -1 },
                zset! { Edge(1, 4) => 1 },
                zset! { Edge(9, 7) => 1 },
            ]
            .into_iter();

            let mut labels: vec::IntoIter<OrdZSet<Label>> = vec![
                zset! { Label(1, 0) => 1 },
                zset! { Label(4, 1) => 1 },
                zset! { },
                zset! { Label(1, 0) => -1, Label(1, 2) => 1 },
                zset! { },
                zset! { Label(8, 3) => 1 },
                zset! { Label(4, 1) => -1 },
                zset! { },
            ]
            .into_iter();

            let mut outputs: vec::IntoIter<OrdZSet<Label>> = vec![
                zset! { Label(1, 0) => 1, Label(2, 0) => 1, Label(3, 0) => 1, Label(4, 0) => 2 },
                zset! { Label(4, 1) => 1 },
                zset! { Label(5, 0) => 2, Label(5, 1) => 1, Label(6, 0) => 2, Label(6, 1) => 1, Label(7, 0) => 4, Label(7, 1) => 2 },
                zset! { Label(1, 0) => -1, Label(1, 2) => 1, Label(2, 0) => -1, Label(2, 2) => 1, Label(3, 0) => -1, Label(3, 2) => 1, Label(4, 0) => -2, Label(4, 2) => 2, Label(5, 0) => -2, Label(5, 2) => 2, Label(6, 0) => -2, Label(6, 2) => 2, Label(7, 0) => -4, Label(7, 2) => 4, Label(8, 2) => 1, Label(9, 2) => 1 },
                zset! { Label(4, 2) => -1, Label(5, 2) => -1, Label(6, 2) => -1, Label(7, 2) => -2, Label(10, 1) => 2, Label(10, 2) => 2 },
                zset! { Label(4, 2) => -1, Label(5, 2) => -1, Label(6, 2) => -1, Label(7, 2) => -2, Label(8, 3) => 1, Label(9, 3) => 1, Label(10, 2) => -2 },
                zset! { Label(4, 1) => -1, Label(4, 2) => 1, Label(5, 1) => -1, Label(5, 2) => 1, Label(6, 1) => -1, Label(6, 2) => 1, Label(7, 1) => -2, Label(7, 2) => 2, Label(10, 1) => -2, Label(10, 2) => 2 },
                zset! { Label(7, 2) => 1, Label(7, 3) => 1, Label(10, 2) => 1, Label(10, 3) => 1 },
            ]
            .into_iter();

            let edges: Stream<_, OrdZSet<Edge>> =
                circuit
                    .add_source(Generator::new(move || edges.next().unwrap()));

            let labels: Stream<_, OrdZSet<Label>> =
                circuit
                    .add_source(Generator::new(move || labels.next().unwrap()));

            propagate(circuit, &edges, &labels).inspect(move |labeled| {
                assert_eq!(*labeled, outputs.next().unwrap());
            });
            Ok(())
        })
        .unwrap().0;

        for _ in 0..8 {
            //eprintln!("{}", i);
            circuit.step().unwrap();
        }
    }

    #[test]
    fn propagate_nested_test() {
        let circuit = RootCircuit::build(move |circuit| {
            let mut edges: vec::IntoIter<OrdZSet<Edge>> = vec![
                zset! { Edge(1, 2) => 1, Edge(1, 3) => 1, Edge(2, 4) => 1, Edge(3, 4) => 1 },
                zset! { Edge(5, 7) => 1, Edge(6, 7) => 1 },
                zset! { Edge(4, 5) => 1, Edge(4, 6) => 1 },
                zset! { Edge(3, 8) => 1, Edge(8, 9) => 1 },
                zset! { Edge(2, 4) => -1, Edge(7, 10) => 1 },
                zset! { Edge(3, 4) => -1 },
                zset! { Edge(1, 4) => 1 },
                zset! { Edge(9, 7) => 1 },
            ]
            .into_iter();

            let mut labels: vec::IntoIter<OrdZSet<Label>> = vec![
                zset! { Label(1, 0) => 1 },
                zset! { Label(4, 1) => 1 },
                zset! { },
                zset! { Label(1, 0) => -1, Label(1, 2) => 1 },
                zset! { },
                zset! { Label(8, 3) => 1 },
                zset! { Label(4, 1) => -1 },
                zset! { },
            ]
            .into_iter();

            let mut outputs: vec::IntoIter<OrdZSet<Label>> = vec![
                zset!{ Label(1,0) => 2, Label(2,0) => 3, Label(3,0) => 3, Label(4,0) => 8 },
                zset!{ Label(4,1) => 2 },
                zset!{ Label(5,0) => 10, Label(5,1) => 3, Label(6,0) => 10, Label(6,1) => 3, Label(7,0) => 24, Label(7,1) => 8 },
                zset!{ Label(1,0) => -2, Label(1,2) => 2, Label(2,0) => -3, Label(2,2) => 3, Label(3,0) => -3, Label(3,2) => 3,
                       Label(4,0) => -8, Label(4,2) => 8, Label(5,0) => -10, Label(5,2) => 10, Label(6,0) => -10, Label(6,2) => 10,
                       Label(7,0) => -24, Label(7,2) => 24, Label(8,2) => 4, Label(9,2) => 5 },
                zset!{ Label(4,2) => -4, Label(5,2) => -5, Label(6,2) => -5, Label(7,2) => -12, Label(10,1) => 10, Label(10,2) => 14 },
                zset!{ Label(4,2) => -4, Label(5,2) => -5, Label(6,2) => -5, Label(7,2) => -12, Label(8,3) => 2,
                       Label(9,3) => 3, Label(10,2) => -14 },
                zset!{ Label(4,1) => -2, Label(4,2) => 3, Label(5,1) => -3, Label(5,2) => 4, Label(6,1) => -3, Label(6,2) => 4,
                       Label(7,1) => -8, Label(7,2) => 10, Label(10,1) => -10, Label(10,2) => 12 },
                zset!{ Label(7,2) => 6, Label(7,3) => 4, Label(10,2) => 7, Label(10,3) => 5 },
            ].into_iter();

            let edges: Stream<_, OrdZSet<Edge>> =
                circuit
                    .add_source(Generator::new(move || edges.next().unwrap()));

            let labels: Stream<_, OrdZSet<Label>> =
                circuit
                    .add_source(Generator::new(move || labels.next().unwrap()));

            let result = circuit.iterate(|child| {

                let counter = std::cell::RefCell::new(0);
                let edges = edges.delta0(child);
                let labels = labels.delta0(child);

                let computed_labels = <DelayedFeedback<_, OrdZSet<_>>>::new(child);
                let result = propagate(child, &edges, &labels.plus(computed_labels.stream()));
                computed_labels.connect(&result);

                //result.inspect(|res: &OrdZSet<Label, isize>| println!("delta: {}", res));
                Ok((move || {
                    let mut counter = counter.borrow_mut();
                    // reset to 0 on each outer loop iteration.
                    if *counter == 2 {
                        *counter = 0;
                    }
                    *counter += 1;
                    //println!("counter: {}", *counter);
                    Ok(*counter == 2)
                },
                    // FIXME: make sure the `export` API works on typed streams correctly,
                    // so that the `inner`/`typed` calls below are not needed.
                result.integrate_trace().inner().export().typed::<Spine<FallbackZSet<Label>>>()))
            }).unwrap();

            result.consolidate().inspect(move |res: &FallbackZSet<Label>| {
                assert_eq!(*res, outputs.next().unwrap());
            });
            Ok(())
        })
        .unwrap().0;

        for _ in 0..8 {
            //eprintln!("{}", i);
            circuit.step().unwrap();
        }
    }
}
