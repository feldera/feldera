use crate::{
    algebra::{HasOne, HasZero, IndexedZSet, ZRingValue},
    circuit::{
        operator_traits::{Operator, QuaternaryOperator},
        Scope,
    },
    dynamic::{
        ClonableTrait, DataTrait, DynDataTyped, DynOpt, DynPair, DynUnit, Erase, Factory,
        WeightTrait, WithFactory,
    },
    operator::{
        dynamic::{
            aggregate::{AggCombineFunc, AggOutputFunc, DynAggregator, DynAverage},
            filter_map::DynFilterMap,
            time_series::{
                radix_tree::{
                    OrdPartitionedTreeAggregateFactories, PartitionedRadixTreeReader,
                    RadixTreeCursor, TreeNode,
                },
                range::{Range, RangeCursor, Ranges, RelRange},
                OrdPartitionedIndexedZSet, PartitionCursor, PartitionedBatchReader,
                PartitionedIndexedZSet, RelOffset,
            },
            trace::{TraceBound, TraceBounds, TraceFeedback},
        },
        Avg,
    },
    trace::{
        Batch, BatchFactories, BatchReader, BatchReaderFactories, Builder, Cursor, Spillable, Spine,
    },
    utils::Tup2,
    Circuit, DBData, DBWeight, DynZWeight, RootCircuit, Stream, ZWeight,
};
use dyn_clone::{clone_box, DynClone};
use num::{Bounded, PrimInt};
use std::{
    borrow::Cow,
    marker::PhantomData,
    ops::{Deref, Div, Neg},
};

use super::radix_tree::{FilePartitionedRadixTreeFactories, Prefix};

pub trait WeighFunc<V: ?Sized, R: ?Sized, A: ?Sized>: Fn(&V, &R, &mut A) + DynClone {}

impl<V: ?Sized, R: ?Sized, A: ?Sized, F> WeighFunc<V, R, A> for F where F: Fn(&V, &R, &mut A) + Clone
{}

dyn_clone::clone_trait_object! {<V: ?Sized, R: ?Sized, A: ?Sized> WeighFunc<V, R, A>}

pub trait PartitionFunc<IV: ?Sized, PK: ?Sized, OV: ?Sized>:
    Fn(&IV, &mut PK, &mut OV) + DynClone
{
}

impl<IV: ?Sized, PK: ?Sized, OV: ?Sized, F> PartitionFunc<IV, PK, OV> for F where
    F: Fn(&IV, &mut PK, &mut OV) + Clone
{
}

pub type OrdPartitionedOverStream<PK, TS, A> =
    Stream<RootCircuit, OrdPartitionedIndexedZSet<PK, TS, DynOpt<A>>>;

pub struct PartitionedRollingAggregateFactories<TS, V, Acc, Out, B, O>
where
    B: PartitionedIndexedZSet<DynDataTyped<TS>, V> + Spillable,
    O: IndexedZSet<Key = B::Key>,
    Acc: DataTrait + ?Sized,
    Out: DataTrait + ?Sized,
    TS: DBData + PrimInt,
    V: DataTrait + ?Sized,
{
    input_factories: B::Factories,
    stored_factories: <B::Spilled as BatchReader>::Factories,
    radix_tree_factories: FilePartitionedRadixTreeFactories<B::Key, TS, Acc>,
    partitioned_tree_aggregate_factories: OrdPartitionedTreeAggregateFactories<TS, V, B, Acc>,
    output_factories: O::Factories,
    phantom: PhantomData<fn(&Out)>,
}

impl<TS, V, Acc, Out, B, O> PartitionedRollingAggregateFactories<TS, V, Acc, Out, B, O>
where
    B: PartitionedIndexedZSet<DynDataTyped<TS>, V> + Spillable,
    O: PartitionedIndexedZSet<DynDataTyped<TS>, DynOpt<Out>, Key = B::Key>,
    Acc: DataTrait + ?Sized,
    Out: DataTrait + ?Sized,
    TS: DBData + PrimInt,
    V: DataTrait + ?Sized,
{
    pub fn new<KType, VType, AType, OType>() -> Self
    where
        KType: DBData + Erase<B::Key>,
        VType: DBData + Erase<V>,
        AType: DBData + Erase<Acc>,
        OType: DBData + Erase<Out>,
    {
        Self {
            input_factories: BatchReaderFactories::new::<KType, Tup2<TS, VType>, ZWeight>(),
            stored_factories: BatchReaderFactories::new::<KType, Tup2<TS, VType>, ZWeight>(),
            radix_tree_factories: BatchReaderFactories::new::<
                KType,
                Tup2<Prefix<TS>, TreeNode<TS, AType>>,
                ZWeight,
            >(),
            partitioned_tree_aggregate_factories: OrdPartitionedTreeAggregateFactories::new::<
                KType,
                VType,
                AType,
            >(),
            output_factories: BatchReaderFactories::new::<KType, Tup2<TS, Option<OType>>, ZWeight>(
            ),
            phantom: PhantomData,
        }
    }
}

pub struct PartitionedRollingAggregateWithWaterlineFactories<PK, TS, V, Acc, Out, B>
where
    PK: DataTrait + ?Sized,
    B: IndexedZSet<Key = DynDataTyped<TS>> + Spillable,
    TS: DBData + PrimInt,
    Acc: DataTrait + ?Sized,
    Out: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
{
    input_factories: B::Factories,
    stored_factories: <B::Spilled as BatchReader>::Factories,
    rolling_aggregate_factories: PartitionedRollingAggregateFactories<
        TS,
        V,
        Acc,
        Out,
        OrdPartitionedIndexedZSet<PK, DynDataTyped<TS>, V>,
        OrdPartitionedIndexedZSet<PK, DynDataTyped<TS>, DynOpt<Out>>,
    >,
}

impl<PK, TS, V, Acc, Out, B>
    PartitionedRollingAggregateWithWaterlineFactories<PK, TS, V, Acc, Out, B>
where
    PK: DataTrait + ?Sized,
    B: IndexedZSet<Key = DynDataTyped<TS>> + Spillable,
    TS: DBData + PrimInt,
    Acc: DataTrait + ?Sized,
    Out: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
{
    pub fn new<KType, VType, PKType, PVType, AType, OType>() -> Self
    where
        KType: DBData + Erase<B::Key>,
        VType: DBData + Erase<B::Val>,
        PKType: DBData + Erase<PK>,
        PVType: DBData + Erase<V>,
        AType: DBData + Erase<Acc>,
        OType: DBData + Erase<Out>,
    {
        Self {
            input_factories: BatchReaderFactories::new::<KType, VType, ZWeight>(),
            stored_factories: BatchReaderFactories::new::<KType, VType, ZWeight>(),
            rolling_aggregate_factories: PartitionedRollingAggregateFactories::new::<
                PKType,
                PVType,
                AType,
                OType,
            >(),
        }
    }
}

pub struct PartitionedRollingAggregateLinearFactories<TS, V, OV, A, B, O>
where
    B: PartitionedIndexedZSet<DynDataTyped<TS>, V> + Spillable,
    O: IndexedZSet<Key = B::Key>,
    TS: DBData + PrimInt,
    V: DataTrait + ?Sized,
    OV: DataTrait + ?Sized,
    A: WeightTrait + ?Sized,
{
    aggregate_factory: &'static dyn Factory<A>,
    opt_accumulator_factory: &'static dyn Factory<DynOpt<A>>,
    output_factory: &'static dyn Factory<OV>,
    rolling_aggregate_factories: PartitionedRollingAggregateFactories<TS, V, A, OV, B, O>,
}

impl<TS, V, OV, A, B, O> PartitionedRollingAggregateLinearFactories<TS, V, OV, A, B, O>
where
    B: PartitionedIndexedZSet<DynDataTyped<TS>, V> + Spillable,
    O: PartitionedIndexedZSet<DynDataTyped<TS>, DynOpt<OV>, Key = B::Key>,
    B::Key: DataTrait,
    TS: DBData + PrimInt,
    V: DataTrait + ?Sized,
    OV: DataTrait + ?Sized,
    A: WeightTrait + ?Sized,
{
    pub fn new<KType, VType, AType, OVType>() -> Self
    where
        KType: DBData + Erase<B::Key>,
        VType: DBData + Erase<V>,
        AType: DBWeight + Erase<A>,
        OVType: DBData + Erase<OV>,
    {
        Self {
            aggregate_factory: WithFactory::<AType>::FACTORY,
            opt_accumulator_factory: WithFactory::<Option<AType>>::FACTORY,
            output_factory: WithFactory::<OVType>::FACTORY,
            rolling_aggregate_factories: PartitionedRollingAggregateFactories::new::<
                KType,
                VType,
                AType,
                OVType,
            >(),
        }
    }
}

pub struct PartitionedRollingAverageFactories<TS, V, W, B, O>
where
    B: PartitionedIndexedZSet<DynDataTyped<TS>, V> + Spillable,
    O: IndexedZSet<Key = B::Key>,
    TS: DBData + PrimInt,
    V: DataTrait + ?Sized,
    W: WeightTrait + ?Sized,
{
    aggregate_factories:
        PartitionedRollingAggregateLinearFactories<TS, V, V, DynAverage<W, B::R>, B, O>,
    weight_factory: &'static dyn Factory<W>,
}

impl<TS, V, W, B, O> PartitionedRollingAverageFactories<TS, V, W, B, O>
where
    B: PartitionedIndexedZSet<DynDataTyped<TS>, V> + Spillable,
    O: PartitionedIndexedZSet<DynDataTyped<TS>, DynOpt<V>, Key = B::Key>,
    TS: DBData + PrimInt,
    V: DataTrait + ?Sized,
    W: WeightTrait + ?Sized,
{
    pub fn new<KType, VType, WType>() -> Self
    where
        KType: DBData + Erase<B::Key>,
        VType: DBData + Erase<V>,
        WType: DBWeight + From<ZWeight> + Div<Output = WType> + Erase<W>,
    {
        Self {
            aggregate_factories: PartitionedRollingAggregateLinearFactories::new::<
                KType,
                VType,
                Avg<WType, ZWeight>,
                VType,
            >(),
            weight_factory: WithFactory::<WType>::FACTORY,
        }
    }
}

/// `Aggregator` object that computes a linear aggregation function.
// TODO: we need this because we currently compute linear aggregates
// using the same algorithm as general aggregates.  Additional performance
// gains can be obtained with an optimized implementation of radix trees
// for linear aggregates (specifically, updating a node when only
// some of its children have changed can be done without computing
// the sum of all children from scratch).
struct LinearAggregator<V, A, O>
where
    V: DataTrait + ?Sized,
    A: WeightTrait + ?Sized,
    O: DataTrait + ?Sized,
{
    acc_factory: &'static dyn Factory<A>,
    opt_accumulator_factory: &'static dyn Factory<DynOpt<A>>,
    output_factory: &'static dyn Factory<O>,
    f: Box<dyn WeighFunc<V, DynZWeight, A>>,
    output_func: Box<dyn AggOutputFunc<A, O>>,
    combine: Box<dyn AggCombineFunc<A>>,
}

impl<V, A, O> Clone for LinearAggregator<V, A, O>
where
    V: DataTrait + ?Sized,
    A: WeightTrait + ?Sized,
    O: DataTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            acc_factory: self.acc_factory,
            opt_accumulator_factory: self.opt_accumulator_factory,
            output_factory: self.output_factory,
            f: clone_box(self.f.as_ref()),
            output_func: clone_box(self.output_func.as_ref()),
            combine: clone_box(self.combine.as_ref()),
        }
    }
}

impl<V, A, O> LinearAggregator<V, A, O>
where
    V: DataTrait + ?Sized,
    A: WeightTrait + ?Sized,
    O: DataTrait + ?Sized,
{
    fn new(
        acc_factory: &'static dyn Factory<A>,
        opt_accumulator_factory: &'static dyn Factory<DynOpt<A>>,
        output_factory: &'static dyn Factory<O>,
        f: Box<dyn WeighFunc<V, DynZWeight, A>>,
        output_func: Box<dyn AggOutputFunc<A, O>>,
    ) -> Self {
        Self {
            acc_factory,
            opt_accumulator_factory,
            output_factory,
            f,
            output_func,
            combine: Box::new(|acc, v| acc.add_assign(v)),
        }
    }
}

impl<V, A, O> DynAggregator<V, (), DynZWeight> for LinearAggregator<V, A, O>
where
    V: DataTrait + ?Sized,
    A: WeightTrait + ?Sized,
    O: DataTrait + ?Sized,
{
    type Accumulator = A;
    type Output = O;

    fn combine(&self) -> &dyn AggCombineFunc<A> {
        self.combine.as_ref()
    }

    fn aggregate(&self, cursor: &mut dyn Cursor<V, DynUnit, (), DynZWeight>, agg: &mut DynOpt<A>) {
        agg.set_none();
        while cursor.key_valid() {
            self.acc_factory.with(&mut |tmp_agg| {
                let w = *cursor.weight().deref();
                (self.f)(cursor.key(), w.erase(), tmp_agg);
                match agg.get_mut() {
                    None => agg.from_val(tmp_agg),
                    Some(old) => old.add_assign(tmp_agg),
                };
            });
            cursor.step_key();
        }
    }

    fn finalize(&self, accumulator: &mut A, output: &mut O) {
        (self.output_func)(accumulator, output)
    }

    fn aggregate_and_finalize(
        &self,
        _cursor: &mut dyn Cursor<V, DynUnit, (), DynZWeight>,
        _output: &mut DynOpt<Self::Output>,
    ) {
        todo!()
    }

    fn opt_accumulator_factory(&self) -> &'static dyn Factory<DynOpt<Self::Accumulator>> {
        self.opt_accumulator_factory
    }

    fn output_factory(&self) -> &'static dyn Factory<Self::Output> {
        self.output_factory
    }
}

impl<B> Stream<RootCircuit, B>
where
    B: IndexedZSet,
{
    /// See [`Stream::partitioned_rolling_aggregate_with_waterline`].
    pub fn dyn_partitioned_rolling_aggregate_with_waterline<PK, TS, V, Acc, Out>(
        &self,
        factories: &PartitionedRollingAggregateWithWaterlineFactories<PK, TS, V, Acc, Out, B>,
        waterline: &Stream<RootCircuit, Box<DynDataTyped<TS>>>,
        partition_func: Box<dyn PartitionFunc<B::Val, PK, V>>,
        aggregator: &dyn DynAggregator<V, (), B::R, Accumulator = Acc, Output = Out>,
        range: RelRange<TS>,
    ) -> OrdPartitionedOverStream<PK, DynDataTyped<TS>, Out>
    where
        B: IndexedZSet<Key = DynDataTyped<TS>> + Spillable,
        B::Spilled: for<'a> DynFilterMap<
            DynItemRef<'a> = (&'a DynDataTyped<TS>, &'a <B as BatchReader>::Val),
        >,
        B: for<'a> DynFilterMap<
            DynItemRef<'a> = (&'a DynDataTyped<TS>, &'a <B as BatchReader>::Val),
        >,
        PK: DataTrait + ?Sized,
        TS: DBData + PrimInt,
        V: DataTrait + ?Sized,
        Acc: DataTrait + ?Sized,
        Out: DataTrait + ?Sized,
    {
        self.circuit()
            .region("partitioned_rolling_aggregate_with_waterline", || {
                // Shift the aggregation window so that its right end is at 0.
                let shifted_range =
                    RelRange::new(range.from - range.to, RelOffset::Before(TS::zero()));

                // Trace bound used inside `partitioned_rolling_aggregate_inner` to
                // bound its output trace.  This is the same bound we use to construct
                // the input window here.
                let bound: TraceBound<DynPair<DynDataTyped<TS>, DynOpt<Out>>> = TraceBound::new();
                let bound_clone = bound.clone();

                let mut bound_box = factories
                    .rolling_aggregate_factories
                    .output_factories
                    .val_factory()
                    .default_box();

                // Restrict the input stream to the `[lb -> ∞)` time window,
                // where `lb = waterline - (range.to - range.from)` is the lower
                // bound on input timestamps that may be used to compute
                // changes to the rolling aggregate operator.
                let bounds = waterline.apply_mut(move |wm| {
                    let lower = shifted_range
                        .range_of(wm.as_ref().deref())
                        .map(|range| range.from)
                        .unwrap_or_else(|| Bounded::min_value());
                    **bound_box.fst_mut() = lower;
                    bound_box.snd_mut().set_none();
                    bound_clone.set(clone_box(bound_box.as_ref()));
                    (
                        Box::new(lower).erase_box(),
                        Box::new(<TS as Bounded>::max_value()).erase_box(),
                    )
                });
                let window = self.dyn_window(
                    &factories.input_factories,
                    &factories.stored_factories,
                    &bounds,
                );

                // Now that we've truncated old inputs, which required the
                // input stream to be indexed by time, we can re-index it
                // by partition id.
                let partition_func_clone = clone_box(partition_func.as_ref());

                let partitioned_window = window.dyn_map_index(
                    &factories.rolling_aggregate_factories.input_factories,
                    Box::new(move |(ts, v), res| {
                        let (partition_key, ts_val) = res.split_mut();
                        let (res_ts, val) = ts_val.split_mut();
                        partition_func_clone(v, partition_key, val);
                        ts.clone_to(res_ts);
                    }),
                );
                let partitioned_self = self.dyn_map_index(
                    &factories.rolling_aggregate_factories.input_factories,
                    Box::new(move |(ts, v), res| {
                        let (partition_key, ts_val) = res.split_mut();
                        let (res_ts, val) = ts_val.split_mut();
                        partition_func(v, partition_key, val);
                        ts.clone_to(res_ts);
                    }),
                );

                partitioned_self.dyn_partitioned_rolling_aggregate_inner(
                    &factories.rolling_aggregate_factories,
                    &partitioned_window,
                    aggregator,
                    range,
                    bound,
                )
            })
    }
}

impl<B> Stream<RootCircuit, B> {
    /// See [`Stream::partitioned_rolling_aggregate`].
    pub fn dyn_partitioned_rolling_aggregate<TS, V, Acc, Out>(
        &self,
        factories: &PartitionedRollingAggregateFactories<
            TS,
            V,
            Acc,
            Out,
            B,
            OrdPartitionedIndexedZSet<B::Key, DynDataTyped<TS>, DynOpt<Out>>,
        >,
        aggregator: &dyn DynAggregator<V, (), B::R, Accumulator = Acc, Output = Out>,
        range: RelRange<TS>,
    ) -> OrdPartitionedOverStream<B::Key, DynDataTyped<TS>, Out>
    where
        B: PartitionedIndexedZSet<DynDataTyped<TS>, V> + Spillable + Send,
        Acc: DataTrait + ?Sized,
        Out: DataTrait + ?Sized,
        TS: DBData + PrimInt,
        V: DataTrait + ?Sized,
    {
        self.dyn_partitioned_rolling_aggregate_generic::<TS, V, Acc, Out, _>(
            factories, aggregator, range,
        )
    }

    /// Like [`Self::dyn_partitioned_rolling_aggregate`], but can return any
    /// batch type.
    pub fn dyn_partitioned_rolling_aggregate_generic<TS, V, Acc, Out, O>(
        &self,
        factories: &PartitionedRollingAggregateFactories<TS, V, Acc, Out, B, O>,
        aggregator: &dyn DynAggregator<V, (), B::R, Accumulator = Acc, Output = Out>,
        range: RelRange<TS>,
    ) -> Stream<RootCircuit, O>
    where
        B: PartitionedIndexedZSet<DynDataTyped<TS>, V> + Spillable + Send,
        Acc: DataTrait + ?Sized,
        Out: DataTrait + ?Sized,
        O: PartitionedIndexedZSet<DynDataTyped<TS>, DynOpt<Out>, Key = B::Key>,
        TS: DBData + PrimInt,
        V: DataTrait + ?Sized,
    {
        // ```
        //                  ┌───────────────┐   input_trace
        //      ┌──────────►│integrate_trace├──────────────┐                              output
        //      │           └───────────────┘              │                           ┌────────────────────────────────────►
        //      │                                          ▼                           │
        // self │    ┌──────────────────────────┐  tree  ┌───────────────────────────┐ │  ┌──────────────────┐ output_trace
        // ─────┼───►│partitioned_tree_aggregate├───────►│PartitionedRollingAggregate├─┴──┤UntimedTraceAppend├────────┐
        //      │    └──────────────────────────┘        └───────────────────────────┘    └──────────────────┘        │
        //      │                                          ▲               ▲                 ▲                        │
        //      └──────────────────────────────────────────┘               │                 │                        │
        //                                                                 │               ┌─┴──┐                     │
        //                                                                 └───────────────┤Z^-1│◄────────────────────┘
        //                                                                   delayed_trace └────┘
        // ```
        self.circuit().region("partitioned_rolling_aggregate", || {
            self.dyn_partitioned_rolling_aggregate_inner(
                factories,
                self,
                aggregator,
                range,
                TraceBound::new(),
            )
        })
    }

    #[doc(hidden)]
    pub fn dyn_partitioned_rolling_aggregate_inner<TS, V, Acc, Out, O>(
        &self,
        factories: &PartitionedRollingAggregateFactories<TS, V, Acc, Out, B, O>,
        self_window: &Self,
        aggregator: &dyn DynAggregator<V, (), DynZWeight, Accumulator = Acc, Output = Out>,
        range: RelRange<TS>,
        bound: TraceBound<DynPair<DynDataTyped<TS>, DynOpt<Out>>>,
    ) -> Stream<RootCircuit, O>
    where
        B: PartitionedIndexedZSet<DynDataTyped<TS>, V> + Spillable + Send,
        O: PartitionedIndexedZSet<DynDataTyped<TS>, DynOpt<Out>, Key = B::Key>,
        Acc: DataTrait + ?Sized,
        Out: DataTrait + ?Sized,
        TS: DBData + PrimInt,
        V: DataTrait + ?Sized,
    {
        let circuit = self.circuit();
        let stream = self.dyn_shard(&factories.input_factories);
        let stream_window = self_window.dyn_shard(&factories.input_factories);

        // Build the radix tree over the bounded window.
        let tree = stream_window
            .partitioned_tree_aggregate::<TS, V, Acc, Out>(
                &factories.partitioned_tree_aggregate_factories,
                aggregator,
            )
            .dyn_spill(&factories.radix_tree_factories)
            .dyn_integrate_trace(&factories.radix_tree_factories);
        let input_trace = stream_window
            .dyn_spill(&factories.stored_factories)
            .dyn_integrate_trace(&factories.stored_factories);

        // Truncate timestamps `< bound` in the output trace.
        let bounds = TraceBounds::new();
        bounds.add_key_bound(TraceBound::new());
        bounds.add_val_bound(bound);

        let feedback =
            circuit.add_integrate_trace_feedback::<Spine<O>>(&factories.output_factories, bounds);

        let output = circuit
            .add_quaternary_operator(
                <PartitionedRollingAggregate<TS, V, Acc, Out, _>>::new(
                    &factories.output_factories,
                    range,
                    aggregator,
                ),
                &stream,
                &input_trace,
                &tree,
                &feedback.delayed_trace,
            )
            .mark_distinct()
            .mark_sharded();

        feedback.connect(&output);

        output
    }

    /// See [`Stream::partitioned_rolling_aggregate_linear`].
    pub fn dyn_partitioned_rolling_aggregate_linear<TS, V, A, O>(
        &self,
        factories: &PartitionedRollingAggregateLinearFactories<
            TS,
            V,
            O,
            A,
            B,
            OrdPartitionedIndexedZSet<B::Key, DynDataTyped<TS>, DynOpt<O>>,
        >,
        f: Box<dyn WeighFunc<V, B::R, A>>,
        output_func: Box<dyn AggOutputFunc<A, O>>,
        range: RelRange<TS>,
    ) -> OrdPartitionedOverStream<B::Key, DynDataTyped<TS>, O>
    where
        B: PartitionedIndexedZSet<DynDataTyped<TS>, V> + Spillable + Send,
        A: WeightTrait + ?Sized,
        TS: DBData + PrimInt,
        V: DataTrait + ?Sized,
        O: DataTrait + ?Sized,
    {
        let aggregator = LinearAggregator::new(
            factories.aggregate_factory,
            factories.opt_accumulator_factory,
            factories.output_factory,
            f,
            output_func,
        );
        self.dyn_partitioned_rolling_aggregate_generic::<TS, V, _, _, _>(
            &factories.rolling_aggregate_factories,
            &aggregator,
            range,
        )
    }

    /// Like [`Self::dyn_partitioned_rolling_aggregate_linear`], but can return
    /// any batch type.
    pub fn dyn_partitioned_rolling_aggregate_linear_generic<TS, V, A, O, Out>(
        &self,
        factories: &PartitionedRollingAggregateLinearFactories<TS, V, O, A, B, Out>,
        f: Box<dyn WeighFunc<V, B::R, A>>,
        output_func: Box<dyn AggOutputFunc<A, O>>,
        range: RelRange<TS>,
    ) -> Stream<RootCircuit, Out>
    where
        B: PartitionedIndexedZSet<DynDataTyped<TS>, V> + Spillable + Send,
        A: WeightTrait + ?Sized,
        TS: DBData + PrimInt,
        V: DataTrait + ?Sized,
        O: DataTrait + ?Sized,
        Out: PartitionedIndexedZSet<DynDataTyped<TS>, DynOpt<O>, Key = B::Key, R = B::R>,
    {
        let aggregator = LinearAggregator::new(
            factories.aggregate_factory,
            factories.opt_accumulator_factory,
            factories.output_factory,
            f,
            output_func,
        );
        self.dyn_partitioned_rolling_aggregate_generic::<TS, V, _, _, _>(
            &factories.rolling_aggregate_factories,
            &aggregator,
            range,
        )
    }

    /// See [`Stream::partitioned_rolling_average`].
    pub fn dyn_partitioned_rolling_average<TS, V, W>(
        &self,
        factories: &PartitionedRollingAverageFactories<
            TS,
            V,
            W,
            B,
            OrdPartitionedIndexedZSet<B::Key, DynDataTyped<TS>, DynOpt<V>>,
        >,
        f: Box<dyn WeighFunc<V, B::R, W>>,
        out_func: Box<dyn AggOutputFunc<W, V>>,
        range: RelRange<TS>,
    ) -> OrdPartitionedOverStream<B::Key, DynDataTyped<TS>, V>
    where
        B: PartitionedIndexedZSet<DynDataTyped<TS>, V> + Spillable + Send,
        W: WeightTrait + ?Sized,
        TS: DBData + PrimInt,
        V: DataTrait + ?Sized,
    {
        self.dyn_partitioned_rolling_average_generic(factories, f, out_func, range)
    }

    pub fn dyn_partitioned_rolling_average_generic<TS, V, W, Out>(
        &self,
        factories: &PartitionedRollingAverageFactories<TS, V, W, B, Out>,
        f: Box<dyn WeighFunc<V, B::R, W>>,
        out_func: Box<dyn AggOutputFunc<W, V>>,
        range: RelRange<TS>,
    ) -> Stream<RootCircuit, Out>
    where
        B: PartitionedIndexedZSet<DynDataTyped<TS>, V> + Spillable + Send,
        TS: DBData + PrimInt,
        V: DataTrait + ?Sized,
        W: WeightTrait + ?Sized,
        Out: PartitionedIndexedZSet<DynDataTyped<TS>, DynOpt<V>, Key = B::Key>,
    {
        let weight_factory = factories.weight_factory;
        self.dyn_partitioned_rolling_aggregate_linear_generic(
            &factories.aggregate_factories,
            Box::new(move |v: &V, w: &B::R, avg: &mut DynAverage<W, B::R>| {
                let (sum, count) = avg.split_mut();
                w.clone_to(count);
                f(v, w, sum);
            }),
            Box::new(move |avg, out| {
                weight_factory.with(&mut |avg_val| {
                    avg.compute_avg(avg_val);
                    out_func(avg_val, out)
                })
            }),
            range,
        )
    }
}

/// Quaternary operator that implements the internals of
/// `partitioned_rolling_aggregate`.
///
/// * Input stream 1: updates to the time series.  Used to identify affected
///   partitions and times.
/// * Input stream 2: trace containing the accumulated time series data.
/// * Input stream 3: trace containing the partitioned radix tree over the input
///   time series.
/// * Input stream 4: trace of previously produced outputs.  Used to compute
///   retractions.
struct PartitionedRollingAggregate<
    TS,
    V: DataTrait + ?Sized,
    Acc: DataTrait + ?Sized,
    Out: DataTrait + ?Sized,
    O: Batch,
> {
    output_factories: O::Factories,
    range: RelRange<TS>,
    aggregator: Box<dyn DynAggregator<V, (), DynZWeight, Accumulator = Acc, Output = Out>>,
    phantom: PhantomData<fn(&V, &O)>,
}

impl<TS, V, Acc, Out, O> PartitionedRollingAggregate<TS, V, Acc, Out, O>
where
    V: DataTrait + ?Sized,
    Acc: DataTrait + ?Sized,
    Out: DataTrait + ?Sized,
    O: Batch,
{
    fn new(
        output_factories: &O::Factories,
        range: RelRange<TS>,
        aggregator: &dyn DynAggregator<V, (), DynZWeight, Accumulator = Acc, Output = Out>,
    ) -> Self {
        Self {
            output_factories: output_factories.clone(),
            range,
            aggregator: clone_box(aggregator),
            phantom: PhantomData,
        }
    }

    fn affected_ranges<R, C>(&self, delta_cursor: &mut C) -> Ranges<TS>
    where
        C: Cursor<DynDataTyped<TS>, V, (), R>,
        TS: DBData + PrimInt,
        R: ?Sized,
    {
        let mut affected_ranges = Ranges::new();
        let mut delta_ranges = Ranges::new();

        while delta_cursor.key_valid() {
            if let Some(range) = self.range.affected_range_of(delta_cursor.key().deref()) {
                affected_ranges.push_monotonic(range);
            }
            // If `delta_cursor.key()` is a new key that doesn't yet occur in the input
            // z-set, we need to compute its aggregate even if it is outside
            // affected range.
            delta_ranges.push_monotonic(Range::new(**delta_cursor.key(), **delta_cursor.key()));
            delta_cursor.step_key();
        }

        affected_ranges.merge(&delta_ranges)
    }
}

impl<TS, V, Acc, Out, O> Operator for PartitionedRollingAggregate<TS, V, Acc, Out, O>
where
    TS: 'static,
    V: DataTrait + ?Sized,
    Acc: DataTrait + ?Sized,
    Out: DataTrait + ?Sized,
    O: Batch,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("PartitionedRollingAggregate")
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<TS, V, Acc, Out, B, T, RT, OT, O> QuaternaryOperator<B, T, RT, OT, O>
    for PartitionedRollingAggregate<TS, V, Acc, Out, O>
where
    TS: DBData + PrimInt,
    V: DataTrait + ?Sized,
    Acc: DataTrait + ?Sized,
    Out: DataTrait + ?Sized,
    B: PartitionedBatchReader<DynDataTyped<TS>, V, R = DynZWeight> + Clone,
    T: PartitionedBatchReader<DynDataTyped<TS>, V, Key = B::Key, R = B::R> + Clone,
    RT: PartitionedRadixTreeReader<TS, Acc, Key = B::Key> + Clone,
    OT: PartitionedBatchReader<DynDataTyped<TS>, DynOpt<Out>, Key = B::Key, R = B::R> + Clone,
    O: IndexedZSet<Key = B::Key, Val = DynPair<DynDataTyped<TS>, DynOpt<Out>>>,
{
    fn eval<'a>(
        &mut self,
        input_delta: Cow<'a, B>,
        input_trace: Cow<'a, T>,
        radix_tree: Cow<'a, RT>,
        output_trace: Cow<'a, OT>,
    ) -> O {
        let mut delta_cursor = input_delta.cursor();
        let mut output_trace_cursor = output_trace.cursor();
        let mut input_trace_cursor = input_trace.cursor();
        let mut tree_cursor = radix_tree.cursor();

        let mut retraction_builder = O::Builder::new_builder(&self.output_factories, ());
        let mut insertion_builder =
            O::Builder::with_capacity(&self.output_factories, (), input_delta.len());

        // println!("delta: {input_delta:#x?}");
        // println!("radix tree: {radix_tree:#x?}");
        // println!("aggregate_range({range:x?})");
        // let mut treestr = String::new();
        // radix_tree.cursor().format_tree(&mut treestr).unwrap();
        // println!("tree: {treestr}");
        // tree_partition_cursor.rewind_keys();

        let mut item = self.output_factories.weighted_item_factory().default_box();
        let mut acc = self.aggregator.opt_accumulator_factory().default_box();
        let mut agg = self.aggregator.output_factory().default_box();

        // Iterate over affected partitions.
        while delta_cursor.key_valid() {
            // Compute affected intervals using `input_delta`.
            let ranges = self.affected_ranges(&mut PartitionCursor::new(&mut delta_cursor));
            // println!("affected_ranges: {ranges:?}");

            // Clear old outputs.
            output_trace_cursor.seek_key(delta_cursor.key());
            if output_trace_cursor.key_valid() && output_trace_cursor.key() == delta_cursor.key() {
                let mut range_cursor = RangeCursor::new(
                    PartitionCursor::new(&mut output_trace_cursor),
                    ranges.clone(),
                );
                while range_cursor.key_valid() {
                    while range_cursor.val_valid() {
                        let weight = **range_cursor.weight();
                        if !weight.is_zero() {
                            let (kv, w) = item.split_mut();
                            let (k, v) = kv.split_mut();
                            delta_cursor.key().clone_to(k);
                            v.from_refs(range_cursor.key(), range_cursor.val());
                            **w = weight.neg();
                            // println!("retract ({item:?})");
                            retraction_builder.push(&mut *item);
                        }
                        range_cursor.step_val();
                    }
                    range_cursor.step_key();
                }
            };

            // Compute new outputs.
            input_trace_cursor.seek_key(delta_cursor.key());
            tree_cursor.seek_key(delta_cursor.key());

            if input_trace_cursor.key_valid() && input_trace_cursor.key() == delta_cursor.key() {
                debug_assert!(tree_cursor.key_valid());
                debug_assert_eq!(tree_cursor.key(), delta_cursor.key());

                let mut tree_partition_cursor = PartitionCursor::new(&mut tree_cursor);
                let mut input_range_cursor =
                    RangeCursor::new(PartitionCursor::new(&mut input_trace_cursor), ranges);

                // For all affected times, seek them in `input_trace`, compute aggregates using
                // using radix_tree.
                while input_range_cursor.key_valid() {
                    let range = self.range.range_of(input_range_cursor.key());
                    tree_partition_cursor.rewind_keys();

                    // println!("aggregate_range({range:x?})");
                    // let mut treestr = String::new();
                    // tree_partition_cursor.format_tree(&mut treestr).unwrap();
                    // println!("tree: {treestr}");
                    // tree_partition_cursor.rewind_keys();

                    while input_range_cursor.val_valid() {
                        // Generate output update.
                        if !input_range_cursor.weight().le0() {
                            let (kv, w) = item.split_mut();
                            let (k, v) = kv.split_mut();

                            delta_cursor.key().clone_to(k);
                            **v.fst_mut() = **input_range_cursor.key();
                            **w = HasOne::one();
                            if let Some(range) = range {
                                tree_partition_cursor.aggregate_range(
                                    &range,
                                    self.aggregator.combine(),
                                    acc.as_mut(),
                                );
                                if let Some(acc) = acc.get_mut() {
                                    self.aggregator.finalize(acc, agg.as_mut());
                                    v.snd_mut().from_val(agg.as_mut());
                                } else {
                                    v.snd_mut().set_none();
                                }
                            } else {
                                v.snd_mut().set_none();
                            }

                            // println!("insert({item:?})");

                            insertion_builder.push(item.as_mut());
                            break;
                        }

                        input_range_cursor.step_val();
                    }

                    input_range_cursor.step_key();
                }
            }

            delta_cursor.step_key();
        }

        let retractions = retraction_builder.done();
        let insertions = insertion_builder.done();
        retractions.add(insertions)
    }
}

#[cfg(test)]
mod test {
    use crate::{
        algebra::DefaultSemigroup,
        dynamic::{ClonableTrait, DowncastTrait, DynData, DynDataTyped, DynOpt, DynPair, Erase},
        lean_vec,
        operator::{
            dynamic::{
                input::AddInputIndexedZSetFactories,
                time_series::{
                    range::{Range, RelOffset, RelRange},
                    PartitionCursor,
                },
                trace::TraceBound,
            },
            time_series::OrdPartitionedIndexedZSet,
            Fold,
        },
        trace::{BatchReaderFactories, Cursor},
        typed_batch::{BatchReader, DynBatchReader, DynOrdIndexedZSet, TypedBatch},
        utils::Tup2,
        DBSPHandle, IndexedZSetHandle, RootCircuit, Runtime, Stream, ZWeight,
    };
    use proptest::{collection, prelude::*};
    use size_of::SizeOf;

    type DataBatch = DynOrdIndexedZSet<
        DynData, /* <u64> */
        DynPair<DynDataTyped<u64>, DynData /* <i64> */>,
    >;
    type DataStream = Stream<RootCircuit, DataBatch>;
    type OutputBatch = TypedBatch<
        u64,
        Tup2<u64, Option<i64>>,
        ZWeight,
        DynOrdIndexedZSet<
            DynData, /* <u64> */
            DynPair<DynDataTyped<u64>, DynOpt<DynData /* <i64> */>>,
        >,
    >;
    type OutputStream = Stream<RootCircuit, OutputBatch>;

    // Reference implementation of `aggregate_range` for testing.
    fn aggregate_range_slow(batch: &DataBatch, partition: u64, range: Range<u64>) -> Option<i64> {
        let mut cursor = batch.cursor();

        cursor.seek_key(&partition);
        assert!(cursor.key_valid());
        assert!(*cursor.key().downcast_checked::<u64>() == partition);
        let mut partition_cursor = PartitionCursor::new(&mut cursor);

        let mut agg = None;
        partition_cursor.seek_key(&range.from);
        while partition_cursor.key_valid()
            && *partition_cursor.key().downcast_checked::<u64>() <= range.to
        {
            while partition_cursor.val_valid() {
                let w = *partition_cursor.weight().downcast_checked::<ZWeight>();
                agg = if let Some(a) = agg {
                    Some(a + *partition_cursor.val().downcast_checked::<i64>() * w)
                } else {
                    Some(*partition_cursor.val().downcast_checked::<i64>() * w)
                };
                partition_cursor.step_val();
            }
            partition_cursor.step_key();
        }

        agg
    }

    // Reference implementation of `partitioned_rolling_aggregate` for testing.
    fn partitioned_rolling_aggregate_slow(
        stream: &DataStream,
        range_spec: RelRange<u64>,
    ) -> OutputStream {
        stream
            .typed::<TypedBatch<u64, Tup2<u64, i64>, ZWeight, _>>()
            .gather(0)
            .integrate()
            .apply(move |batch: &TypedBatch<_, _, _, DataBatch>| {
                let mut tuples = Vec::with_capacity(batch.len());

                let mut cursor = batch.cursor();

                while cursor.key_valid() {
                    while cursor.val_valid() {
                        let partition = *cursor.key().downcast_checked::<u64>();
                        let Tup2(ts, _val) = *cursor.val().downcast_checked::<Tup2<u64, i64>>();
                        let agg = range_spec
                            .range_of(&ts)
                            .and_then(|range| aggregate_range_slow(batch, partition, range));
                        tuples.push(Tup2(Tup2(partition, Tup2(ts, agg)), 1));
                        cursor.step_val();
                    }
                    cursor.step_key();
                }

                OutputBatch::from_tuples((), tuples)
            })
            .stream_distinct()
            .gather(0)
    }

    fn partition_rolling_aggregate_circuit(
        lateness: u64,
        size_bound: Option<usize>,
    ) -> (DBSPHandle, IndexedZSetHandle<u64, Tup2<u64, i64>>) {
        Runtime::init_circuit(4, move |circuit| {
            let (input_stream, input_handle) =
                circuit.add_input_indexed_zset::<u64, Tup2<u64, i64>>();

            let input_stream = input_stream.as_partitioned_zset();

            let input_by_time: Stream<_, TypedBatch<u64,Tup2<u64, i64>, ZWeight, _>> = input_stream
                .inner()
                .dyn_map_index::<DynDataTyped<u64>, _>(
                    &BatchReaderFactories::new::<u64, Tup2<u64, i64>, ZWeight>(),
                    Box::new(|(partition, ts_val): (&DynData, &DynPair<DynDataTyped<u64>, DynData>), kv: &mut DynPair<DynDataTyped<u64>, DynData>| {
                        let (k, v) = kv.split_mut();
                        let (ts, val) = ts_val.split();
                        ts.clone_to(k);
                        *v.downcast_mut_checked() = Tup2(*partition.downcast_checked::<u64>(), *val.downcast_checked::<i64>());
                    }),
                )
                .typed();

            let waterline =
                input_by_time.waterline_monotonic(|| 0, move |ts| ts.saturating_sub(lateness));

            let aggregator = <Fold<i64, i64, DefaultSemigroup<_>, _, _>>::new(
                0i64,
                |agg: &mut i64, val: &i64, w: ZWeight| *agg += val * w,
            );

            let range_spec = RelRange::new(RelOffset::Before(1000), RelOffset::Before(0));
            let expected_1000_0 =
                partitioned_rolling_aggregate_slow(&input_stream.inner(), range_spec);
            let output_1000_0 = input_stream
                .partitioned_rolling_aggregate(aggregator.clone(), range_spec)
                .gather(0)
                .integrate();
            expected_1000_0.apply2(&output_1000_0, |expected, actual| {
                assert_eq!(expected, actual)
            });

            let output_1000_0_waterline = Stream::partitioned_rolling_aggregate_with_waterline(
                &input_by_time,
                &waterline,
                |Tup2(partition, val)| (*partition, *val),
                aggregator.clone(),
                range_spec,
            )
            .gather(0)
            .integrate();

            expected_1000_0.apply2(&output_1000_0_waterline, |expected, actual| {
                assert_eq!(expected, actual)
            });

            let output_1000_0_linear = input_stream
                .partitioned_rolling_aggregate_linear(|v| *v, |v| v, range_spec)
                .gather(0)
                .integrate();
            expected_1000_0.apply2(&output_1000_0_linear, |expected, actual| {
                assert_eq!(expected, actual)
            });

            let range_spec = RelRange::new(RelOffset::Before(500), RelOffset::After(500));
            let expected_500_500 =
                partitioned_rolling_aggregate_slow(&input_stream.inner(), range_spec);
            let aggregate_500_500 =
                input_stream.partitioned_rolling_aggregate(aggregator.clone(), range_spec);
            let output_500_500 = aggregate_500_500.gather(0).integrate();
            expected_500_500.apply2(&output_500_500, |expected, actual| {
                assert_eq!(expected, actual)
            });

            let aggregate_500_500_waterline = input_by_time
                .partitioned_rolling_aggregate_with_waterline(
                    &waterline,
                    |Tup2(partition, val)| (*partition, *val),
                    aggregator.clone(),
                    range_spec,
                );
            let output_500_500_waterline = aggregate_500_500_waterline.gather(0).integrate();

            let bound: TraceBound<DynPair<DynDataTyped<u64>, DynOpt<DynData>>> = TraceBound::new();
            let b: Tup2<u64, Option<i64>> = Tup2(u64::max_value(), None::<i64>);

            bound.set(Box::new(b).erase_box());

            aggregate_500_500_waterline
                .spill()
                .integrate_trace_with_bound(TraceBound::new(), bound)
                .apply(move |trace| {
                    if let Some(bound) = size_bound {
                        assert!(trace.size_of().total_bytes() <= bound);
                    }
                });

            expected_500_500.apply2(&output_500_500_waterline, |expected, actual| {
                assert_eq!(expected, actual)
            });

            let output_500_500_linear = input_stream
                .partitioned_rolling_aggregate_linear(|v| *v, |v| v, range_spec)
                .gather(0)
                .integrate();
            expected_500_500.apply2(&output_500_500_linear, |expected, actual| {
                assert_eq!(expected, actual)
            });

            let range_spec = RelRange::new(RelOffset::Before(500), RelOffset::Before(100));
            let expected_500_100 =
                partitioned_rolling_aggregate_slow(&input_stream.inner(), range_spec);
            let output_500_100 = input_stream
                .partitioned_rolling_aggregate(aggregator, range_spec)
                .gather(0)
                .integrate();
            expected_500_100.apply2(&output_500_100, |expected, actual| {
                assert_eq!(expected, actual)
            });

            Ok(input_handle)
        })
        .unwrap()
    }

    #[test]
    fn test_partitioned_over_range_2() {
        let (mut circuit, input) = partition_rolling_aggregate_circuit(u64::max_value(), None);

        circuit.step().unwrap();

        input.append(&mut vec![Tup2(2u64, Tup2(Tup2(110271u64, 100i64), 1i64))]);
        circuit.step().unwrap();

        input.append(&mut vec![Tup2(2u64, Tup2(Tup2(0u64, 100i64), 1i64))]);
        circuit.step().unwrap();

        circuit.kill().unwrap();
    }

    #[test]
    fn test_partitioned_over_range() {
        let (mut circuit, input) = partition_rolling_aggregate_circuit(u64::max_value(), None);

        circuit.step().unwrap();

        input.append(&mut vec![
            Tup2(0u64, Tup2(Tup2(1u64, 100i64), 1)),
            Tup2(0, Tup2(Tup2(10, 100), 1)),
            Tup2(0, Tup2(Tup2(20, 100), 1)),
            Tup2(0, Tup2(Tup2(30, 100), 1)),
        ]);
        circuit.step().unwrap();

        input.append(&mut vec![
            Tup2(0u64, Tup2(Tup2(5u64, 100i64), 1)),
            Tup2(0, Tup2(Tup2(15, 100), 1)),
            Tup2(0, Tup2(Tup2(25, 100), 1)),
            Tup2(0, Tup2(Tup2(35, 100), 1)),
        ]);
        circuit.step().unwrap();

        input.append(&mut vec![
            Tup2(0u64, Tup2(Tup2(1u64, 100i64), -1)),
            Tup2(0, Tup2(Tup2(10, 100), -1)),
            Tup2(0, Tup2(Tup2(20, 100), -1)),
            Tup2(0, Tup2(Tup2(30, 100), -1)),
        ]);
        input.append(&mut vec![
            Tup2(1u64, Tup2(Tup2(1u64, 100i64), 1)),
            Tup2(1, Tup2(Tup2(1000, 100), 1)),
            Tup2(1, Tup2(Tup2(2000, 100), 1)),
            Tup2(1, Tup2(Tup2(3000, 100), 1)),
        ]);
        circuit.step().unwrap();

        circuit.kill().unwrap();
    }

    // Test derived from issue #199 (https://github.com/feldera/feldera/issues/199).
    #[test]
    fn test_partitioned_rolling_aggregate2() {
        let (circuit, (input, expected)) = RootCircuit::build(move |circuit| {
            let (input, input_handle) = circuit
                .dyn_add_input_indexed_zset::<DynData/*<u64>*/, DynPair<DynDataTyped<u64>, DynData/*<i64>*/>>(
                    &AddInputIndexedZSetFactories::new::<u64, Tup2<u64, i64>>(),
                );
            let (expected, expected_handle) =
                circuit.dyn_add_input_indexed_zset::<DynData/*<u64>*/, DynPair<DynDataTyped<u64>, DynOpt<DynData/*<i64>*/>>>(&AddInputIndexedZSetFactories::new::<u64, Tup2<u64, Option<i64>>>());

            let input = input.typed::<TypedBatch<u64,Tup2<u64,i64>,ZWeight,_>>();

            input.inspect(|f| {
                for (p, Tup2(ts, v), w) in f.iter() {
                    println!(" input {p} {ts} {v:6} {w:+}");
                }
            });
            let range_spec = RelRange::new(RelOffset::Before(3), RelOffset::Before(2));
            let sum = input.partitioned_rolling_aggregate_linear(|&f| f, |x| x, range_spec);
            sum.inspect(|f| {
                for (p, Tup2(ts, sum), w) in f.iter() {
                    println!("output {p} {ts} {:6} {w:+}", sum.unwrap_or_default());
                }
            });
            expected.apply2(&sum, |expected, actual| assert_eq!(expected, actual.inner()));
            Ok((input_handle, expected_handle))
        })
        .unwrap();

        input.dyn_append(
            &mut Box::new(lean_vec![
                Tup2(1u64, Tup2(Tup2(0u64, 1i64), 1)),
                Tup2(1, Tup2(Tup2(1, 10), 1)),
                Tup2(1, Tup2(Tup2(2, 100), 1)),
                Tup2(1, Tup2(Tup2(3, 1000), 1)),
                Tup2(1, Tup2(Tup2(4, 10000), 1)),
                Tup2(1, Tup2(Tup2(5, 100000), 1)),
                Tup2(1, Tup2(Tup2(9, 123456), 1)),
            ])
            .erase_box(),
        );
        expected.dyn_append(
            &mut Box::new(lean_vec![
                Tup2(1u64, Tup2(Tup2(0u64, None::<i64>), 1)),
                Tup2(1, Tup2(Tup2(1, None), 1)),
                Tup2(1, Tup2(Tup2(2, Some(1)), 1)),
                Tup2(1, Tup2(Tup2(3, Some(11)), 1)),
                Tup2(1, Tup2(Tup2(4, Some(110)), 1)),
                Tup2(1, Tup2(Tup2(5, Some(1100)), 1)),
                Tup2(1, Tup2(Tup2(9, None), 1)),
            ])
            .erase_box(),
        );
        circuit.step().unwrap();
    }

    #[test]
    fn test_partitioned_rolling_average() {
        let (circuit, (input, expected)) = RootCircuit::build(move |circuit| {
            let (input_stream, input_handle) = circuit
                .dyn_add_input_indexed_zset::<DynData/*<u64>*/, DynPair<DynDataTyped<u64>, DynData/*<i64>*/>>(&AddInputIndexedZSetFactories::new::<u64, Tup2<u64, i64>>());
            let (expected_stream, expected_handle) =
                circuit.dyn_add_input_indexed_zset::<DynData/*<u64>*/, DynPair<DynDataTyped<u64>, DynOpt<DynData/*<i64>*/>>>(&AddInputIndexedZSetFactories::new::<u64, Tup2<u64, Option<i64>>>());

            let range_spec = RelRange::new(RelOffset::Before(3), RelOffset::Before(1));
            input_stream
                .typed()
                .partitioned_rolling_average(range_spec)
                .apply2(&expected_stream, |avg: &OrdPartitionedIndexedZSet<u64, u64, _, Option<i64>, _>, expected| assert_eq!(avg.inner(), expected));
            Ok((input_handle, expected_handle))
        })
        .unwrap();

        circuit.step().unwrap();

        input.dyn_append(
            &mut Box::new(lean_vec![
                Tup2(0u64, Tup2(Tup2(10u64, 10i64), 1)),
                Tup2(0, Tup2(Tup2(11, 20), 1)),
                Tup2(0, Tup2(Tup2(12, 30), 1)),
                Tup2(0, Tup2(Tup2(13, 40), 1)),
                Tup2(0, Tup2(Tup2(14, 50), 1)),
                Tup2(0, Tup2(Tup2(15, 60), 1)),
            ])
            .erase_box(),
        );
        expected.dyn_append(
            &mut Box::new(lean_vec![
                Tup2(0u64, Tup2(Tup2(10u64, None::<i64>), 1)),
                Tup2(0, Tup2(Tup2(11, Some(10)), 1)),
                Tup2(0, Tup2(Tup2(12, Some(15)), 1)),
                Tup2(0, Tup2(Tup2(13, Some(20)), 1)),
                Tup2(0, Tup2(Tup2(14, Some(30)), 1)),
                Tup2(0, Tup2(Tup2(15, Some(40)), 1)),
            ])
            .erase_box(),
        );
        circuit.step().unwrap();
    }

    #[test]
    fn test_partitioned_rolling_aggregate() {
        let (circuit, input) = RootCircuit::build(move |circuit| {
            let (input_stream, input_handle) = circuit
                .dyn_add_input_indexed_zset::<DynData/*<u64>*/, DynPair<DynDataTyped<u64>, DynData/*<i64>*/>>(
                    &AddInputIndexedZSetFactories::new::<u64, Tup2<u64, i64>>(),
                );

            let input_stream = input_stream.typed::<TypedBatch<u64, Tup2<u64, i64>, ZWeight, _>>();

            input_stream.inspect(|f| {
                for (p, Tup2(ts, v), w) in f.iter() {
                    println!(" input {p} {ts} {v:6} {w:+}");
                }
            });
            let range_spec = RelRange::new(RelOffset::Before(3), RelOffset::Before(2));
            let sum = input_stream.partitioned_rolling_aggregate_linear(|&f| f, |x| x, range_spec);
            sum.inspect(|f| {
                for (p, Tup2(ts, sum), w) in f.iter() {
                    println!("output {p} {ts} {:6} {w:+}", sum.unwrap_or_default());
                }
            });
            Ok(input_handle)
        })
        .unwrap();

        input.dyn_append(
            &mut Box::new(lean_vec![
                Tup2(1u64, Tup2(Tup2(0u64, 1i64), 1)),
                Tup2(1, Tup2(Tup2(1, 10), 1)),
                Tup2(1, Tup2(Tup2(2, 100), 1)),
                Tup2(1, Tup2(Tup2(3, 1000), 1)),
                Tup2(1, Tup2(Tup2(4, 10000), 1)),
                Tup2(1, Tup2(Tup2(5, 100000), 1)),
                Tup2(1, Tup2(Tup2(9, 123456), 1)),
            ])
            .erase_box(),
        );
        circuit.step().unwrap();
    }

    type InputTuple = Tup2<u64, Tup2<Tup2<u64, i64>, ZWeight>>;
    type InputBatch = Vec<InputTuple>;

    fn input_tuple(partitions: u64, window: (u64, u64)) -> impl Strategy<Value = InputTuple> {
        (
            (0..partitions),
            (
                (window.0..window.1, 100..101i64).prop_map(|(x, y)| Tup2(x, y)),
                1..2i64,
            )
                .prop_map(|(x, y)| Tup2(x, y)),
        )
            .prop_map(|(x, y)| Tup2(x, y))
    }

    fn input_batch(
        partitions: u64,
        window: (u64, u64),
        max_batch_size: usize,
    ) -> impl Strategy<Value = InputBatch> {
        collection::vec(input_tuple(partitions, window), 0..max_batch_size)
    }

    fn input_trace(
        partitions: u64,
        epoch: u64,
        max_batch_size: usize,
        max_batches: usize,
    ) -> impl Strategy<Value = Vec<InputBatch>> {
        collection::vec(
            input_batch(partitions, (0, epoch), max_batch_size),
            0..max_batches,
        )
    }

    fn input_trace_quasi_monotone(
        partitions: u64,
        window_size: u64,
        window_step: u64,
        max_batch_size: usize,
        batches: usize,
    ) -> impl Strategy<Value = Vec<InputBatch>> {
        (0..batches)
            .map(|i| {
                input_batch(
                    partitions,
                    (i as u64 * window_step, i as u64 * window_step + window_size),
                    max_batch_size,
                )
                .boxed()
            })
            .collect::<Vec<_>>()
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(5))]

        #[test]
        #[cfg_attr(feature = "persistence", ignore = "takes a long time?")]
        fn proptest_partitioned_rolling_aggregate_quasi_monotone(trace in input_trace_quasi_monotone(5, 10_000, 2_000, 20, 200)) {
            // 10_000 is an empirically established bound: without GC this test needs >10KB.
            let (mut circuit, input) = partition_rolling_aggregate_circuit(10000, Some(20_000));

            for mut batch in trace {
                input.append(&mut batch);
                circuit.step().unwrap();
            }

            circuit.kill().unwrap();
        }
    }

    proptest! {
        #[test]
        #[cfg_attr(feature = "persistence", ignore = "takes a long time?")]
        fn proptest_partitioned_over_range_sparse(trace in input_trace(5, 1_000_000, 20, 20)) {
            let (mut circuit, input) = partition_rolling_aggregate_circuit(u64::max_value(), None);

            for mut batch in trace {
                input.append(&mut batch);
                circuit.step().unwrap();
            }

            circuit.kill().unwrap();
        }

        #[test]
        #[cfg_attr(feature = "persistence", ignore = "takes a long time?")]
        fn proptest_partitioned_over_range_dense(trace in input_trace(5, 1_000, 50, 20)) {
            let (mut circuit, input) = partition_rolling_aggregate_circuit(u64::max_value(), None);

            for mut batch in trace {
                input.append(&mut batch);
                circuit.step().unwrap();
            }

            circuit.kill().unwrap();
        }
    }
}
