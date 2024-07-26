use crate::{
    algebra::{
        IndexedZSet, IndexedZSetReader, Lattice, MulByRef, OrdIndexedZSet, OrdZSet, PartialOrder,
        ZSet, ZTrace,
    },
    circuit::{
        metadata::{
            MetaItem, OperatorLocation, OperatorMeta, NUM_ENTRIES_LABEL, SHARED_BYTES_LABEL,
            USED_BYTES_LABEL,
        },
        operator_traits::{BinaryOperator, Operator},
        Circuit, GlobalNodeId, RootCircuit, Scope, Stream, WithClock,
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
use minitrace::trace;
use size_of::{Context, SizeOf};
use std::{
    borrow::Cow,
    cmp::{min, Ordering},
    collections::HashMap,
    marker::PhantomData,
    ops::Deref,
    panic::Location,
};

circuit_cache_key!(AntijoinId<C, D>((GlobalNodeId, GlobalNodeId) => Stream<C, D>));

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
    I1: IndexedZSetReader,
    I2: IndexedZSetReader,
    O: IndexedZSet,
    T: Timestamp,
{
    pub left_factories: I1::Factories,
    pub right_factories: I2::Factories,
    pub left_trace_factories: <T::FileValBatch<I1::Key, I1::Val, I1::R> as BatchReader>::Factories,
    pub right_trace_factories: <T::FileValBatch<I1::Key, I2::Val, I1::R> as BatchReader>::Factories,
    pub output_factories: O::Factories,
    pub timed_item_factory:
        &'static dyn Factory<DynPair<DynDataTyped<T>, WeightedItem<O::Key, O::Val, O::R>>>,
    pub timed_items_factory:
        &'static dyn Factory<DynPairs<DynDataTyped<T>, WeightedItem<O::Key, O::Val, O::R>>>,
}

impl<I1, I2, T, O> JoinFactories<I1, I2, T, O>
where
    I1: IndexedZSetReader,
    I2: IndexedZSetReader<Key = I1::Key>,
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
    I1: IndexedZSetReader,
    I2: IndexedZSetReader,
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
    I2: IndexedZSet,
    T: Timestamp,
{
    pub join_factories: JoinFactories<I1, I2, T, I1>,
    pub distinct_factories: DistinctFactories<I2, T>,
}

impl<I1, I2, T> AntijoinFactories<I1, I2, T>
where
    I1: IndexedZSet,
    I2: IndexedZSet<Key = I1::Key>,
    T: Timestamp,
{
    pub fn new<KType, V1Type, V2Type>() -> Self
    where
        KType: DBData + Erase<I1::Key>,
        V1Type: DBData + Erase<I1::Val>,
        V2Type: DBData + Erase<I2::Val>,
    {
        Self {
            join_factories: JoinFactories::new::<KType, V1Type, V2Type, KType, V1Type>(),
            distinct_factories: DistinctFactories::new::<KType, V2Type>(),
        }
    }
}

impl<I1, I2, T> Clone for AntijoinFactories<I1, I2, T>
where
    I1: IndexedZSet,
    I2: IndexedZSet,
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
    pub left_join_factories: JoinFactories<I1, I2, T, I1>,
    pub right_join_factories: JoinFactories<I2, I1, T, I2>,

    pub left_distinct_factories: DistinctFactories<I1, T>,
    pub right_distinct_factories: DistinctFactories<I2, T>,
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
            left_join_factories: JoinFactories::new::<KType, V1Type, V2Type, KType, V1Type>(),
            right_join_factories: JoinFactories::new::<KType, V2Type, V1Type, KType, V2Type>(),
            left_distinct_factories: DistinctFactories::new::<KType, V1Type>(),
            right_distinct_factories: DistinctFactories::new::<KType, V2Type>(),
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

            let left_trace = left.dyn_trace(&factories.left_trace_factories);
            let right_trace = right.dyn_trace(&factories.right_trace_factories);

            let left = self.circuit().add_binary_operator(
                JoinTrace::new(
                    &factories.right_trace_factories,
                    &factories.output_factories,
                    factories.timed_item_factory,
                    factories.timed_items_factory,
                    join_funcs.left,
                    Location::caller(),
                    self.circuit().clone(),
                ),
                &left,
                &right_trace,
            );

            let right = self.circuit().add_binary_operator(
                JoinTrace::new(
                    &factories.left_trace_factories,
                    &factories.output_factories,
                    factories.timed_item_factory,
                    factories.timed_items_factory,
                    join_funcs.right,
                    Location::caller(),
                    self.circuit().clone(),
                ),
                &right,
                &left_trace.delay_trace(),
            );

            left.plus(&right)
        })
    }

    /// See [`Stream::antijoin`].
    pub fn dyn_antijoin<I2>(
        &self,
        factories: &AntijoinFactories<I1, I2, C::Time>,
        other: &Stream<C, I2>,
    ) -> Stream<C, I1>
    where
        I2: IndexedZSet<Key = I1::Key> + Send,
        Box<I1::Key>: Clone,
        Box<I1::Val>: Clone,
    {
        self.circuit()
            .cache_get_or_insert_with(
                AntijoinId::new((
                    self.origin_node_id().clone(),
                    other.origin_node_id().clone(),
                )),
                move || {
                    self.circuit().region("antijoin", || {
                        let stream1 = self.dyn_shard(&factories.join_factories.left_factories);
                        let stream2 = other
                            .dyn_distinct(&factories.distinct_factories)
                            .dyn_shard(&factories.join_factories.right_factories);

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
    fn eval(&mut self, i1: &I1, i2: &I2) -> Z {
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
    fn eval(&mut self, i1: &I1, i2: &I2) -> Z {
        let mut cursor1 = i1.cursor();
        let mut cursor2 = i2.cursor();

        let output_key_factory = self.output_factories.key_factory();

        // Choose capacity heuristically.
        let mut builder =
            Z::Builder::with_capacity(&self.output_factories, (), min(i1.len(), i2.len()));

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

                            Builder::push_vals(
                                &mut builder,
                                output.as_mut(),
                                ().erase_mut(),
                                w1.mul_by_ref(&w2).erase_mut(),
                            );
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
    produced_tuples: usize,
}

impl JoinStats {
    pub const fn new() -> Self {
        Self {
            lhs_tuples: 0,
            rhs_tuples: 0,
            output_tuples: 0,
            produced_tuples: 0,
        }
    }
}

pub struct JoinTrace<I, T, Z, Clk>
where
    I: IndexedZSet,
    T: ZTrace,
    Z: IndexedZSet,
{
    right_factories: T::Factories,
    output_factories: Z::Factories,
    timed_item_factory:
        &'static dyn Factory<DynPair<DynDataTyped<T::Time>, WeightedItem<Z::Key, Z::Val, Z::R>>>,
    clock: Clk,
    timed_items_factory:
        &'static dyn Factory<DynPairs<DynDataTyped<T::Time>, WeightedItem<Z::Key, Z::Val, Z::R>>>,
    join_func: TraceJoinFunc<I::Key, I::Val, T::Val, Z::Key, Z::Val>,
    location: &'static Location<'static>,
    // Future update batches computed ahead of time, indexed by time
    // when each batch should be output.
    output_batchers: HashMap<T::Time, Z::Batcher>,
    // True if empty input batch was received at the current clock cycle.
    empty_input: bool,
    // True if empty output was produced at the current clock cycle.
    empty_output: bool,
    stats: JoinStats,
    _types: PhantomData<(I, T, Z)>,
}

impl<I, T, Z, Clk> JoinTrace<I, T, Z, Clk>
where
    I: IndexedZSet,
    T: ZTrace,
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
            join_func,
            location,
            output_batchers: HashMap::new(),
            empty_input: false,
            empty_output: false,
            stats: JoinStats::new(),
            _types: PhantomData,
        }
    }
}

impl<I, T, Z, Clk> Operator for JoinTrace<I, T, Z, Clk>
where
    I: IndexedZSet,
    T: ZTrace,
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
            self.empty_input = false;
            self.empty_output = false;
        }
    }

    fn clock_end(&mut self, _scope: Scope) {
        debug_assert!(self
            .output_batchers
            .keys()
            .all(|time| !time.less_equal(&self.clock.time())));
    }

    fn metadata(&self, meta: &mut OperatorMeta) {
        let total_size: usize = self
            .output_batchers
            .values()
            .map(|batcher| batcher.tuples())
            .sum();

        let batch_sizes = MetaItem::Array(
            self.output_batchers
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
            for batcher in self.output_batchers.values() {
                batcher.size_of_with_context(&mut context);
            }

            context.size_of()
        };

        // Find the percentage of consolidated outputs
        let mut output_redundancy = ((self.stats.output_tuples as f64
            - self.stats.produced_tuples as f64)
            / self.stats.output_tuples as f64)
            * 100.0;
        if output_redundancy.is_nan() {
            output_redundancy = 0.0;
        } else if output_redundancy.is_infinite() {
            output_redundancy = 100.0;
        }

        meta.extend(metadata! {
            NUM_ENTRIES_LABEL => total_size,
            "batch sizes" => batch_sizes,
            USED_BYTES_LABEL => MetaItem::bytes(bytes.used_bytes()),
            "allocations" => bytes.distinct_allocations(),
            SHARED_BYTES_LABEL => MetaItem::bytes(bytes.shared_bytes()),
            "left inputs" => self.stats.lhs_tuples,
            "right inputs" => self.stats.rhs_tuples,
            "computed outputs" => self.stats.output_tuples,
            "produced outputs" => self.stats.produced_tuples,
            "output redundancy" => MetaItem::Percent(output_redundancy),
        });
    }

    fn fixedpoint(&self, scope: Scope) -> bool {
        let epoch_end = self.clock.time().epoch_end(scope);
        // We're in a stable state if input and output at the current clock cycle are
        // both empty, and there are no precomputed outputs before the end of the
        // clock epoch.
        self.empty_input
            && self.empty_output
            && self
                .output_batchers
                .keys()
                .all(|time| !time.less_equal(&epoch_end))
    }
}

impl<I, T, Z, Clk> BinaryOperator<I, T, Z> for JoinTrace<I, T, Z, Clk>
where
    I: IndexedZSet,
    T: ZTrace<Key = I::Key>,
    Z: IndexedZSet,
    Clk: WithClock<Time = T::Time> + 'static,
{
    #[trace]
    fn eval(&mut self, index: &I, trace: &T) -> Z {
        self.stats.lhs_tuples += index.len();
        self.stats.rhs_tuples = trace.len();

        self.empty_input = index.is_empty();

        // Buffer to collect output tuples.
        // One allocation per clock tick is acceptable; however the actual output can be
        // larger than `index.len()`.  If re-allocations becomes a problem, we
        // may need to do something smarter, like a chain of buffers.
        // TODO: Sub-scopes can cause a lot of inner clock cycles to be set off, so this
        //       actually could be significant
        let mut output_tuples = self.timed_items_factory.default_box();

        let mut index_cursor = index.cursor();
        let mut trace_cursor = trace.cursor();

        let time = self.clock.time();

        let mut val = self.right_factories.val_factory().default_box();

        let mut timed_item = self.timed_item_factory.default_box();

        while index_cursor.key_valid() && trace_cursor.key_valid() {
            match index_cursor.key().cmp(trace_cursor.key()) {
                Ordering::Less => index_cursor.seek_key(trace_cursor.key()),
                Ordering::Greater => trace_cursor.seek_key(index_cursor.key()),
                Ordering::Equal => {
                    //println!("key: {}", index_cursor.key(index));

                    while index_cursor.val_valid() {
                        let w1 = **index_cursor.weight();
                        let v1 = index_cursor.val();
                        //println!("v1: {}, w1: {}", v1, w1);

                        while trace_cursor.val_valid() {
                            // FIXME: this clone is only needed to avoid borrow checker error due to
                            // borrowing `trace_cursor` below.
                            trace_cursor.val().clone_to(val.as_mut());

                            (self.join_func)(index_cursor.key(), v1, &val, &mut |k, v| {
                                trace_cursor.map_times(&mut |ts: &T::Time, w2: &T::R| {
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
                        index_cursor.step_val();
                    }

                    index_cursor.step_key();
                    trace_cursor.step_key();
                }
            }
        }

        self.stats.output_tuples += output_tuples.len();
        // Sort `output_tuples` by timestamp and push all tuples for each unique
        // timestamp to the appropriate batcher.
        output_tuples.sort_by_key();

        let mut batch = self.output_factories.weighted_items_factory().default_box();
        let mut start: usize = 0;

        while start < output_tuples.len() {
            let batch_time = output_tuples[start].fst().deref().clone();

            let run_length = output_tuples.advance_while(start, output_tuples.len(), &|tuple| {
                tuple.fst().deref() == &batch_time
            });
            batch.reserve(run_length);

            for i in start..start + run_length {
                batch.push_val(unsafe { output_tuples.index_mut_unchecked(i) }.snd_mut());
            }

            start += run_length;

            self.output_batchers
                .entry(batch_time)
                .or_insert_with(|| Z::Batcher::new_batcher(&self.output_factories, ()))
                .push_batch(&mut batch);
            batch.clear();
        }

        // Finalize the batch for the current timestamp and return it.
        let batcher = self
            .output_batchers
            .remove(&time)
            .unwrap_or_else(|| Z::Batcher::new_batcher(&self.output_factories, ()));

        let result = batcher.seal();
        self.stats.produced_tuples += result.len();
        self.empty_output = result.is_empty();

        result
    }
}

#[cfg(test)]
mod test {
    use crate::{
        circuit::WithClock,
        indexed_zset,
        operator::{DelayedFeedback, Generator},
        typed_batch::{OrdIndexedZSet, OrdZSet, Spine},
        utils::Tup2,
        zset, Circuit, FallbackZSet, RootCircuit, Runtime, Stream, Timestamp,
    };
    use rkyv::{Archive, Deserialize, Serialize};
    use size_of::SizeOf;
    use std::{
        fmt::{Display, Formatter},
        hash::Hash,
        sync::{Arc, Mutex},
        vec,
    };

    #[test]
    fn join_test() {
        let circuit = RootCircuit::build(move |circuit| {
            let mut input1 = vec![
                zset! {
                    Tup2(1, "a".to_string()) => 1i64,
                    Tup2(1, "b".to_string()) => 2,
                    Tup2(2, "c".to_string()) => 3,
                    Tup2(2, "d".to_string()) => 4,
                    Tup2(3, "e".to_string()) => 5,
                    Tup2(3, "f".to_string()) => -2,
                },
                zset! {Tup2(1, "a".to_string()) => 1},
                zset! {Tup2(1, "a".to_string()) => 1},
                zset! {Tup2(4, "n".to_string()) => 2},
                zset! {Tup2(1, "a".to_string()) => 0},
            ]
            .into_iter();
            let mut input2 = vec![
                zset! {
                    Tup2(2, "g".to_string()) => 3i64,
                    Tup2(2, "h".to_string()) => 4,
                    Tup2(3, "i".to_string()) => 5,
                    Tup2(3, "j".to_string()) => -2,
                    Tup2(4, "k".to_string()) => 5,
                    Tup2(4, "l".to_string()) => -2,
                },
                zset! {Tup2(1, "b".to_string()) => 1},
                zset! {Tup2(4, "m".to_string()) => 1},
                zset! {},
                zset! {},
            ]
            .into_iter();
            let mut outputs = vec![
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

            //let mut inc_outputs = inc_outputs_vec.clone().into_iter();
            let mut inc_outputs2 = inc_outputs_vec.into_iter();
            let mut inc_filtered_outputs = inc_filtered_outputs_vec.into_iter();

            let index1: Stream<_, OrdIndexedZSet<u64, String>> = circuit
                .add_source(Generator::new(move || {
                    if Runtime::worker_index() == 0 {
                        input1.next().unwrap()
                    } else {
                        <OrdZSet<_>>::empty()
                    }
                }))
                .map_index(|Tup2(k, v)| (*k, v.clone()));
            let index2: Stream<_, OrdIndexedZSet<u64, String>> = circuit
                .add_source(Generator::new(move || {
                    if Runtime::worker_index() == 0 {
                        input2.next().unwrap()
                    } else {
                        <OrdZSet<_>>::empty()
                    }
                }))
                .map_index(|Tup2(k, v)| (*k, v.clone()));
            index1
                .stream_join(&index2, |&k: &u64, s1, s2| {
                    Tup2(k, format!("{} {}", s1, s2))
                })
                .gather(0)
                .inspect(move |fm: &OrdZSet<Tup2<u64, String>>| {
                    if Runtime::worker_index() == 0 {
                        assert_eq!(fm, &outputs.next().unwrap())
                    }
                });
            /*index1
            .join_incremental(&index2, |&k: &u64, s1, s2| Tup2(k, format!("{} {}", s1, s2)))
            .gather(0)
            .inspect(move |fm: &OrdZSet<Tup2<u64, String>>| {
                if Runtime::worker_index() == 0 {
                    assert_eq!(fm, &inc_outputs.next().unwrap())
                }
            });*/

            index1
                .join(&index2, |&k: &u64, s1, s2| {
                    Tup2(k, format!("{} {}", s1, s2))
                })
                .gather(0)
                .inspect(move |fm: &OrdZSet<Tup2<u64, String>>| {
                    if Runtime::worker_index() == 0 {
                        assert_eq!(fm, &inc_outputs2.next().unwrap())
                    }
                });

            index1
                .join_flatmap(&index2, |&k: &u64, s1, s2| {
                    if s1.as_str() == "a" {
                        None
                    } else {
                        Some(Tup2(k, format!("{} {}", s1, s2)))
                    }
                })
                .gather(0)
                .inspect(move |fm: &OrdZSet<Tup2<u64, String>>| {
                    if Runtime::worker_index() == 0 {
                        assert_eq!(fm, &inc_filtered_outputs.next().unwrap())
                    }
                });

            Ok(())
        })
        .unwrap()
        .0;

        for _ in 0..5 {
            circuit.step().unwrap();
        }
    }

    fn do_join_test_mt(workers: usize) {
        let hruntime = Runtime::run(workers, || {
            join_test();
        })
        .expect("failed to run test");

        hruntime.join().unwrap();
    }

    #[test]
    fn join_test_mt() {
        do_join_test_mt(1);
        do_join_test_mt(2);
        do_join_test_mt(4);
        do_join_test_mt(16);
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

    #[test]
    fn antijoin_test() {
        let output = Arc::new(Mutex::new(OrdIndexedZSet::empty()));
        let output_clone = output.clone();

        let (mut circuit, (input1, input2)) = Runtime::init_circuit(4, move |circuit| {
            let (input1, input_handle1) = circuit.add_input_indexed_zset::<u64, u64>();
            let (input2, input_handle2) = circuit.add_input_indexed_zset::<u64, u64>();

            input1.antijoin(&input2).gather(0).inspect(move |batch| {
                if Runtime::worker_index() == 0 {
                    *output_clone.lock().unwrap() = batch.clone();
                }
            });

            Ok((input_handle1, input_handle2))
        })
        .unwrap();

        input1.append(&mut vec![
            Tup2(1, Tup2(0, 1)),
            Tup2(1, Tup2(1, 2)),
            Tup2(2, Tup2(0, 1)),
            Tup2(2, Tup2(1, 1)),
        ]);
        circuit.step().unwrap();
        assert_eq!(
            &*output.lock().unwrap(),
            &indexed_zset! { 1 => { 0 => 1, 1 => 2}, 2 => { 0 => 1, 1 => 1 } }
        );

        input1.append(&mut vec![Tup2(3, Tup2(1, 1))]);
        circuit.step().unwrap();
        assert_eq!(&*output.lock().unwrap(), &indexed_zset! { 3 => { 1 => 1 } });

        input2.append(&mut vec![Tup2(1, Tup2(1, 3))]);
        circuit.step().unwrap();
        assert_eq!(
            &*output.lock().unwrap(),
            &indexed_zset! { 1 => { 0 => -1, 1 => -2 } }
        );

        input2.append(&mut vec![Tup2(2, Tup2(5, 1))]);
        input1.append(&mut vec![Tup2(2, Tup2(2, 1)), Tup2(4, Tup2(1, 1))]);
        circuit.step().unwrap();
        assert_eq!(
            &*output.lock().unwrap(),
            &indexed_zset! { 2 => { 0 => -1, 1 => -1 }, 4 => { 1 => 1 } }
        );

        circuit.kill().unwrap();
    }
}
