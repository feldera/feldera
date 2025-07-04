//! Aggregation operators.

use dyn_clone::{clone_box, DynClone};
use minitrace::trace;
use std::{
    any::TypeId,
    borrow::Cow,
    cmp::{min, Ordering},
    collections::BTreeMap,
    marker::PhantomData,
};

use crate::{
    algebra::{HasOne, IndexedZSet, IndexedZSetReader, Lattice, OrdIndexedZSet, PartialOrder},
    circuit::{
        operator_traits::{BinaryOperator, Operator, UnaryOperator},
        Circuit, Scope, Stream, WithClock,
    },
    dynamic::{
        DataTrait, DynData, DynOpt, DynPair, DynPairs, DynSet, DynUnit, DynWeight, Erase, Factory,
        LeanVec, Weight, WeightTrait, WithFactory,
    },
    operator::dynamic::upsert::UpsertFactories,
    time::Timestamp,
    trace::{
        cursor::CursorGroup, Batch, BatchReader, BatchReaderFactories, Builder, Cursor, Filter,
        OrdWSet, OrdWSetFactories,
    },
    DBData, DBWeight, DynZWeight, NestedCircuit, RootCircuit, ZWeight,
};

mod aggregator;
// Some standard aggregators.
mod average;
mod chain_aggregate;
mod fold;
mod max;
mod min;

use crate::{
    dynamic::{BSet, ClonableTrait},
    storage::file::Deserializable,
    utils::Tup2,
};
pub use aggregator::{
    AggCombineFunc, AggOutputFunc, Aggregator, DynAggregator, DynAggregatorImpl, Postprocess,
};
pub use average::{Avg, AvgFactories, DynAverage};
pub use fold::Fold;
pub use max::{Max, MaxSemigroup};
pub use min::{Min, MinSemigroup, MinSome1, MinSome1Semigroup};

use super::MonoIndexedZSet;

pub struct IncAggregateFactories<I: BatchReader, O: IndexedZSet, T: Timestamp> {
    pub input_factories: I::Factories,
    pub trace_factories: <T::ValBatch<I::Key, I::Val, I::R> as BatchReader>::Factories,
    pub upsert_factories: UpsertFactories<T, O>,
    keys_factory: &'static dyn Factory<DynSet<I::Key>>,
    output_pair_factory: &'static dyn Factory<DynPair<I::Key, DynOpt<O::Val>>>,
    output_pairs_factory: &'static dyn Factory<DynPairs<I::Key, DynOpt<O::Val>>>,
}

impl<I, O, T> IncAggregateFactories<I, O, T>
where
    I: BatchReader,
    O: IndexedZSet<Key = I::Key>,
    T: Timestamp,
{
    pub fn new<KType, VType, RType, OType>() -> Self
    where
        KType: DBData + Erase<I::Key>,
        <KType as Deserializable>::ArchivedDeser: Ord,
        VType: DBData + Erase<I::Val>,
        RType: DBWeight + Erase<I::R>,
        OType: DBData + Erase<O::Val>,
    {
        Self {
            input_factories: BatchReaderFactories::new::<KType, VType, RType>(),
            trace_factories: BatchReaderFactories::new::<KType, VType, RType>(),
            upsert_factories: UpsertFactories::new::<KType, OType>(),
            keys_factory: WithFactory::<BSet<KType>>::FACTORY,
            output_pair_factory: WithFactory::<Tup2<KType, Option<OType>>>::FACTORY,
            output_pairs_factory: WithFactory::<LeanVec<Tup2<KType, Option<OType>>>>::FACTORY,
        }
    }
}

pub struct IncAggregateLinearFactories<
    I: BatchReader,
    R: WeightTrait + ?Sized,
    O: IndexedZSet,
    T: Timestamp,
> {
    pub out_factory: &'static dyn Factory<O::Val>,
    pub agg_factory: &'static dyn Factory<R>,
    pub option_agg_factory: &'static dyn Factory<DynOpt<R>>,
    pub aggregate_factories: IncAggregateFactories<OrdWSet<I::Key, R>, O, T>,
}

impl<I, R, O, T> IncAggregateLinearFactories<I, R, O, T>
where
    I: BatchReader,
    O: IndexedZSet<Key = I::Key>,
    R: WeightTrait + ?Sized,
    T: Timestamp,
{
    pub fn new<KType, RType, OType>() -> Self
    where
        KType: DBData + Erase<I::Key>,
        <KType as Deserializable>::ArchivedDeser: Ord,
        RType: DBWeight + Erase<R>,
        OType: DBData + Erase<O::Val>,
    {
        Self {
            out_factory: WithFactory::<OType>::FACTORY,
            agg_factory: WithFactory::<RType>::FACTORY,
            option_agg_factory: WithFactory::<Option<RType>>::FACTORY,
            aggregate_factories: IncAggregateFactories::new::<KType, (), RType, OType>(),
        }
    }
}

pub struct StreamAggregateFactories<I: BatchReader, O: IndexedZSet> {
    pub input_factories: I::Factories,
    pub output_factories: O::Factories,
    pub option_output_factory: &'static dyn Factory<DynOpt<O::Val>>,
}

impl<I, O> StreamAggregateFactories<I, O>
where
    I: BatchReader,
    O: IndexedZSet<Key = I::Key>,
{
    pub fn new<KType, VType, RType, OType>() -> Self
    where
        KType: DBData + Erase<I::Key>,
        VType: DBData + Erase<I::Val>,
        RType: DBWeight + Erase<I::R>,
        OType: DBData + Erase<O::Val>,
    {
        Self {
            input_factories: BatchReaderFactories::new::<KType, VType, RType>(),
            output_factories: BatchReaderFactories::new::<KType, OType, ZWeight>(),
            option_output_factory: WithFactory::<Option<OType>>::FACTORY,
        }
    }
}

pub struct StreamLinearAggregateFactories<I: IndexedZSetReader, R, O: IndexedZSet>
where
    R: WeightTrait + ?Sized,
{
    aggregate_factories: StreamAggregateFactories<OrdWSet<I::Key, R>, O>,
    out_factory: &'static dyn Factory<O::Val>,
    agg_factory: &'static dyn Factory<R>,
    option_agg_factory: &'static dyn Factory<DynOpt<R>>,
}

impl<I, R, O> StreamLinearAggregateFactories<I, R, O>
where
    I: IndexedZSetReader,
    O: IndexedZSet<Key = I::Key>,
    R: WeightTrait + ?Sized,
{
    pub fn new<KType, VType, RType, OType>() -> Self
    where
        KType: DBData + Erase<I::Key>,
        VType: DBData + Erase<I::Val>,
        RType: DBWeight + Erase<R>,
        OType: DBData + Erase<O::Val>,
    {
        Self {
            aggregate_factories: StreamAggregateFactories::new::<KType, (), RType, OType>(),
            out_factory: WithFactory::<OType>::FACTORY,
            agg_factory: WithFactory::<RType>::FACTORY,
            option_agg_factory: WithFactory::<Option<RType>>::FACTORY,
        }
    }
}

pub trait WeightedCountOutFunc<R: ?Sized, O: ?Sized>: Fn(&mut R, &mut O) + DynClone {}
impl<R: ?Sized, O: ?Sized, F> WeightedCountOutFunc<R, O> for F where F: Fn(&mut R, &mut O) + DynClone
{}

/// Aggregator used internally by [`Stream::dyn_aggregate_linear`].  Computes
/// the total sum of weights.
struct WeightedCount<R: WeightTrait + ?Sized, O: DataTrait + ?Sized> {
    out_factory: &'static dyn Factory<O>,
    weight_factory: &'static dyn Factory<R>,
    option_weight_factory: &'static dyn Factory<DynOpt<R>>,
    out_func: Box<dyn WeightedCountOutFunc<R, O>>,
}

impl<R: WeightTrait + ?Sized, O: DataTrait + ?Sized> Clone for WeightedCount<R, O> {
    fn clone(&self) -> Self {
        Self {
            out_factory: self.out_factory,
            weight_factory: self.weight_factory,
            option_weight_factory: self.option_weight_factory,
            out_func: clone_box(self.out_func.as_ref()),
        }
    }
}

impl<R: WeightTrait + ?Sized, O: DataTrait + ?Sized> WeightedCount<R, O> {
    fn new(
        out_factory: &'static dyn Factory<O>,
        weight_factory: &'static dyn Factory<R>,
        option_weight_factory: &'static dyn Factory<DynOpt<R>>,
        out_func: Box<dyn WeightedCountOutFunc<R, O>>,
    ) -> Self {
        Self {
            out_factory,
            weight_factory,
            option_weight_factory,
            out_func,
        }
    }
}

impl<T, R, O> DynAggregator<DynUnit, T, R> for WeightedCount<R, O>
where
    T: Timestamp,
    R: WeightTrait + ?Sized,
    O: DataTrait + ?Sized,
{
    type Accumulator = R;
    type Output = O;

    fn opt_accumulator_factory(&self) -> &'static dyn Factory<DynOpt<Self::Accumulator>> {
        self.option_weight_factory
    }

    fn output_factory(&self) -> &'static dyn Factory<Self::Output> {
        self.out_factory
    }

    fn combine(&self) -> &dyn AggCombineFunc<R> {
        &|left: &mut R, right: &R| left.add_assign(right)
    }

    fn aggregate(
        &self,
        cursor: &mut dyn Cursor<DynUnit, DynUnit, T, R>,
        aggregate: &mut DynOpt<R>,
    ) {
        let mut weight = self.weight_factory.default_box();

        cursor.map_times(&mut |_t, w| weight.add_assign(w));

        if weight.is_zero() {
            aggregate.set_none();
        } else {
            aggregate.from_val(&mut weight)
        }
    }

    fn finalize(&self, accumulator: &mut R, output: &mut O) {
        (self.out_func)(accumulator, output);
    }

    fn aggregate_and_finalize(
        &self,
        cursor: &mut dyn Cursor<DynUnit, DynUnit, T, R>,
        output: &mut DynOpt<Self::Output>,
    ) {
        self.option_weight_factory.with(&mut |w| {
            self.aggregate(cursor, w);
            match w.get_mut() {
                None => output.set_none(),
                Some(w) => {
                    output.set_some_with(&mut |o| DynAggregator::<_, T, _>::finalize(self, w, o))
                }
            }
        })
    }
}

impl Stream<RootCircuit, MonoIndexedZSet> {
    #[allow(clippy::type_complexity)]
    pub fn dyn_aggregate_mono(
        &self,
        persistent_id: Option<&str>,
        factories: &IncAggregateFactories<MonoIndexedZSet, MonoIndexedZSet, ()>,
        aggregator: &dyn DynAggregator<
            DynData,
            (),
            DynZWeight,
            Accumulator = DynData,
            Output = DynData,
        >,
    ) -> Stream<RootCircuit, MonoIndexedZSet> {
        self.dyn_aggregate(persistent_id, factories, aggregator)
    }

    pub fn dyn_aggregate_linear_mono(
        &self,
        persisent_id: Option<&str>,
        factories: &IncAggregateLinearFactories<MonoIndexedZSet, DynWeight, MonoIndexedZSet, ()>,
        agg_func: Box<dyn Fn(&DynData, &DynData, &DynZWeight, &mut DynWeight)>,
        out_func: Box<dyn WeightedCountOutFunc<DynWeight, DynData>>,
    ) -> Stream<RootCircuit, MonoIndexedZSet> {
        self.dyn_aggregate_linear_generic(persisent_id, factories, agg_func, out_func)
    }
}

impl Stream<NestedCircuit, MonoIndexedZSet> {
    #[allow(clippy::type_complexity)]
    pub fn dyn_aggregate_mono(
        &self,
        persistent_id: Option<&str>,
        factories: &IncAggregateFactories<
            MonoIndexedZSet,
            MonoIndexedZSet,
            <NestedCircuit as WithClock>::Time,
        >,
        aggregator: &dyn DynAggregator<
            DynData,
            <NestedCircuit as WithClock>::Time,
            DynZWeight,
            Accumulator = DynData,
            Output = DynData,
        >,
    ) -> Stream<NestedCircuit, MonoIndexedZSet> {
        self.dyn_aggregate(persistent_id, factories, aggregator)
    }

    pub fn dyn_aggregate_linear_mono(
        &self,
        persistent_id: Option<&str>,
        factories: &IncAggregateLinearFactories<
            MonoIndexedZSet,
            DynWeight,
            MonoIndexedZSet,
            <NestedCircuit as WithClock>::Time,
        >,
        agg_func: Box<dyn Fn(&DynData, &DynData, &DynZWeight, &mut DynWeight)>,
        out_func: Box<dyn WeightedCountOutFunc<DynWeight, DynData>>,
    ) -> Stream<NestedCircuit, MonoIndexedZSet> {
        self.dyn_aggregate_linear_generic(persistent_id, factories, agg_func, out_func)
    }
}

impl<C, Z> Stream<C, Z>
where
    C: Circuit,
    Z: Clone + 'static,
{
    /// See [`Stream::stream_aggregate`].
    #[allow(clippy::type_complexity)]
    pub fn dyn_stream_aggregate<Acc, Out>(
        &self,
        factories: &StreamAggregateFactories<Z, OrdIndexedZSet<Z::Key, Out>>,
        aggregator: &dyn DynAggregator<Z::Val, (), Z::R, Accumulator = Acc, Output = Out>,
    ) -> Stream<C, OrdIndexedZSet<Z::Key, Out>>
    where
        Z: IndexedZSet,
        Acc: DataTrait + ?Sized,
        Out: DataTrait + ?Sized,
    {
        self.dyn_stream_aggregate_generic(factories, aggregator)
    }

    /// Like [`Self::dyn_stream_aggregate`], but can return any batch type.
    pub fn dyn_stream_aggregate_generic<Acc, Out, O>(
        &self,
        factories: &StreamAggregateFactories<Z, O>,
        aggregator: &dyn DynAggregator<Z::Val, (), Z::R, Accumulator = Acc, Output = Out>,
    ) -> Stream<C, O>
    where
        Acc: DataTrait + ?Sized,
        Out: DataTrait + ?Sized,
        Z: Batch<Time = ()>,
        O: IndexedZSet<Key = Z::Key, Val = Out>,
    {
        self.circuit()
            .add_unary_operator(
                Aggregate::new(
                    &factories.output_factories,
                    factories.option_output_factory,
                    aggregator,
                ),
                &self.dyn_shard(&factories.input_factories),
            )
            .mark_sharded()
    }

    /// See [`Stream::stream_aggregate_linear`].
    pub fn dyn_stream_aggregate_linear<A>(
        &self,
        factories: &StreamLinearAggregateFactories<Z, A, OrdIndexedZSet<Z::Key, A>>,
        f: Box<dyn Fn(&Z::Key, &Z::Val, &Z::R, &mut A)>,
    ) -> Stream<C, OrdIndexedZSet<Z::Key, A>>
    where
        Z: IndexedZSet,
        A: WeightTrait + ?Sized,
    {
        self.dyn_stream_aggregate_linear_generic(factories, f, Box::new(|w, out| w.move_to(out)))
    }

    /// Like [`Self::dyn_stream_aggregate_linear`], but can return any batch
    /// type.
    pub fn dyn_stream_aggregate_linear_generic<A, O>(
        &self,
        factories: &StreamLinearAggregateFactories<Z, A, O>,
        agg_func: Box<dyn Fn(&Z::Key, &Z::Val, &Z::R, &mut A)>,
        out_func: Box<dyn WeightedCountOutFunc<A, O::Val>>,
    ) -> Stream<C, O>
    where
        Z: IndexedZSet,
        O: IndexedZSet<Key = Z::Key>,
        A: WeightTrait + ?Sized,
    {
        self.dyn_weigh(&factories.aggregate_factories.input_factories, agg_func)
            .dyn_stream_aggregate_generic(
                &factories.aggregate_factories,
                &WeightedCount::new(
                    factories.out_factory,
                    factories.agg_factory,
                    factories.option_agg_factory,
                    out_func,
                ),
            )
    }

    /// See [`Stream::aggregate`].
    #[allow(clippy::type_complexity)]
    pub fn dyn_aggregate<Acc, Out>(
        &self,
        persistent_id: Option<&str>,
        factories: &IncAggregateFactories<Z, OrdIndexedZSet<Z::Key, Out>, C::Time>,
        aggregator: &dyn DynAggregator<
            Z::Val,
            <C as WithClock>::Time,
            Z::R,
            Accumulator = Acc,
            Output = Out,
        >,
    ) -> Stream<C, OrdIndexedZSet<Z::Key, Out>>
    where
        Acc: DataTrait + ?Sized,
        Out: DataTrait + ?Sized,
        Z: IndexedZSet,
    {
        self.dyn_aggregate_generic::<Acc, Out, OrdIndexedZSet<Z::Key, Out>>(
            persistent_id,
            factories,
            aggregator,
        )
    }

    /// Like [`Self::dyn_aggregate`], but can return any batch type.
    pub fn dyn_aggregate_generic<Acc, Out, O>(
        &self,
        persistent_id: Option<&str>,
        factories: &IncAggregateFactories<Z, O, C::Time>,
        aggregator: &dyn DynAggregator<
            Z::Val,
            <C as WithClock>::Time,
            Z::R,
            Accumulator = Acc,
            Output = Out,
        >,
    ) -> Stream<C, O>
    where
        Acc: DataTrait + ?Sized,
        Out: DataTrait + ?Sized,
        Z: Batch<Time = ()>,
        O: IndexedZSet<Key = Z::Key, Val = Out>,
    {
        let circuit = self.circuit();
        let stream = self.dyn_shard(&factories.input_factories);

        // We construct the following circuit.  See `AggregateIncremental` documentation
        // for details.
        //
        // ```
        //          ┌────────────────────────────────────────┐
        //          │                                        │
        //          │                                        ▼
        //  stream  │     ┌─────┐  stream.trace()  ┌────────────────────┐      ┌──────┐
        // ─────────┴─────┤trace├─────────────────►│AggregateIncremental├─────►│upsert├──────►
        //                └─────┘                  └────────────────────┘      └──────┘
        // ```

        circuit
            .add_binary_operator(
                AggregateIncremental::new(
                    factories.keys_factory,
                    factories.output_pair_factory,
                    factories.output_pairs_factory,
                    aggregator,
                    circuit.clone(),
                ),
                &stream,
                &stream.dyn_trace(&factories.trace_factories, &factories.input_factories),
            )
            .mark_sharded()
            .upsert::<O>(persistent_id, &factories.upsert_factories)
            .mark_sharded()
    }

    /// See [`Stream::aggregate_linear`].
    pub fn dyn_aggregate_linear<A>(
        &self,
        persistent_id: Option<&str>,
        factories: &IncAggregateLinearFactories<Z, A, OrdIndexedZSet<Z::Key, A>, C::Time>,
        f: Box<dyn Fn(&Z::Key, &Z::Val, &Z::R, &mut A)>,
    ) -> Stream<C, OrdIndexedZSet<Z::Key, A>>
    where
        Z: IndexedZSet,
        A: WeightTrait + ?Sized,
    {
        self.dyn_aggregate_linear_generic(
            persistent_id,
            factories,
            f,
            Box::new(|w, out| w.move_to(out)),
        )
    }

    /// Like [`Self::dyn_aggregate_linear`], but can return any batch type.
    pub fn dyn_aggregate_linear_generic<A, O>(
        &self,
        persistent_id: Option<&str>,
        factories: &IncAggregateLinearFactories<Z, A, O, C::Time>,
        agg_func: Box<dyn Fn(&Z::Key, &Z::Val, &Z::R, &mut A)>,
        out_func: Box<dyn WeightedCountOutFunc<A, O::Val>>,
    ) -> Stream<C, O>
    where
        Z: IndexedZSet,
        O: IndexedZSet<Key = Z::Key>,
        A: WeightTrait + ?Sized,
    {
        self.dyn_weigh(&factories.aggregate_factories.input_factories, agg_func)
            .set_persistent_id(
                persistent_id
                    .map(|name| format!("{name}-weighted"))
                    .as_deref(),
            )
            .dyn_aggregate_generic(
                persistent_id,
                &factories.aggregate_factories,
                &WeightedCount::new(
                    factories.out_factory,
                    factories.agg_factory,
                    factories.option_agg_factory,
                    out_func,
                ),
            )
    }

    /// See [`Stream::weigh`].
    pub fn dyn_weigh<T>(
        &self,
        output_factories: &OrdWSetFactories<Z::Key, T>,
        f: Box<dyn Fn(&Z::Key, &Z::Val, &Z::R, &mut T)>,
    ) -> Stream<C, OrdWSet<Z::Key, T>>
    where
        T: WeightTrait + ?Sized,
        Z: IndexedZSet,
    {
        self.dyn_weigh_generic::<OrdWSet<_, _>>(output_factories, f)
    }

    /// Like [`Self::dyn_weigh`], but can return any batch type.
    pub fn dyn_weigh_generic<O>(
        &self,
        output_factories: &O::Factories,
        f: Box<dyn Fn(&Z::Key, &Z::Val, &Z::R, &mut O::R)>,
    ) -> Stream<C, O>
    where
        Z: IndexedZSet,
        O: Batch<Key = Z::Key, Val = DynUnit, Time = ()>,
    {
        let output_factories = output_factories.clone();

        let output = self
            .try_sharded_version()
            .apply_named("Weigh", move |batch| {
                let mut agg = output_factories.weight_factory().default_box();
                let mut agg_delta = output_factories.weight_factory().default_box();
                let mut input_weight = batch.factories().weight_factory().default_box();

                let mut delta = <O::Builder>::with_capacity(&output_factories, batch.key_count());
                let mut cursor = batch.cursor();
                while cursor.key_valid() {
                    agg.set_zero();
                    while cursor.val_valid() {
                        **input_weight = **cursor.weight();
                        f(
                            cursor.key(),
                            cursor.val(),
                            &*input_weight,
                            agg_delta.as_mut(),
                        );

                        agg.add_assign(&agg_delta);
                        cursor.step_val();
                    }
                    if !agg.is_zero() {
                        delta.push_val_diff_mut(().erase_mut(), &mut agg);
                        delta.push_key(cursor.key());
                    }
                    cursor.step_key();
                }
                delta.done()
            });

        output.mark_sharded_if(self);
        output
    }
}

impl Stream<RootCircuit, MonoIndexedZSet> {
    pub fn dyn_aggregate_linear_retain_keys_mono(
        &self,
        persistent_id: Option<&str>,
        factories: &IncAggregateLinearFactories<MonoIndexedZSet, DynWeight, MonoIndexedZSet, ()>,
        waterline: &Stream<RootCircuit, Box<DynData>>,
        retain_key_func: Box<dyn Fn(&DynData) -> Filter<DynData>>,
        agg_func: Box<dyn Fn(&DynData, &DynData, &DynZWeight, &mut DynWeight)>,
        out_func: Box<dyn WeightedCountOutFunc<DynWeight, DynData>>,
    ) -> Stream<RootCircuit, MonoIndexedZSet>
where {
        self.dyn_aggregate_linear_retain_keys_generic(
            persistent_id,
            factories,
            waterline,
            retain_key_func,
            agg_func,
            out_func,
        )
    }
}

impl<Z> Stream<RootCircuit, Z>
where
    Z: Clone + 'static,
{
    pub fn dyn_aggregate_linear_retain_keys_generic<A, O, TS>(
        &self,
        persistent_id: Option<&str>,
        factories: &IncAggregateLinearFactories<Z, A, O, ()>,
        waterline: &Stream<RootCircuit, Box<TS>>,
        retain_key_func: Box<dyn Fn(&TS) -> Filter<Z::Key>>,
        agg_func: Box<dyn Fn(&Z::Key, &Z::Val, &Z::R, &mut A)>,
        out_func: Box<dyn WeightedCountOutFunc<A, O::Val>>,
    ) -> Stream<RootCircuit, O>
    where
        Z: IndexedZSet<Time = ()>,
        O: IndexedZSet<Key = Z::Key>,
        A: WeightTrait + ?Sized,
        TS: DataTrait + ?Sized,
        Box<TS>: Clone,
    {
        let weighted = self.dyn_weigh(&factories.aggregate_factories.input_factories, agg_func);
        weighted.dyn_integrate_trace_retain_keys(waterline, retain_key_func);

        weighted
            .set_persistent_id(
                persistent_id
                    .map(|name| format!("{name}-weighted"))
                    .as_deref(),
            )
            .dyn_aggregate_generic(
                persistent_id,
                &factories.aggregate_factories,
                &WeightedCount::new(
                    factories.out_factory,
                    factories.agg_factory,
                    factories.option_agg_factory,
                    out_func,
                ),
            )
    }
}

/// Non-incremental aggregation operator.
struct Aggregate<Z, Acc, O>
where
    Z: BatchReader,
    O: IndexedZSet,
    Acc: DataTrait + ?Sized,
{
    factories: O::Factories,
    option_output_factory: &'static dyn Factory<DynOpt<O::Val>>,
    aggregator: Box<dyn DynAggregator<Z::Val, (), Z::R, Accumulator = Acc, Output = O::Val>>,
    _type: PhantomData<(Z, O)>,
}

impl<Z, Acc, O: Batch> Aggregate<Z, Acc, O>
where
    Z: BatchReader,
    O: IndexedZSet,
    Acc: DataTrait + ?Sized,
{
    pub fn new(
        factories: &O::Factories,
        option_output_factory: &'static dyn Factory<DynOpt<O::Val>>,
        aggregator: &dyn DynAggregator<Z::Val, (), Z::R, Accumulator = Acc, Output = O::Val>,
    ) -> Self {
        Self {
            factories: factories.clone(),
            option_output_factory,
            aggregator: clone_box(aggregator),
            _type: PhantomData,
        }
    }
}

impl<Z, Acc, O: Batch> Operator for Aggregate<Z, Acc, O>
where
    Z: BatchReader,
    O: IndexedZSet,
    Acc: DataTrait + ?Sized,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("Aggregate")
    }
    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<Z, Acc, O> UnaryOperator<Z, O> for Aggregate<Z, Acc, O>
where
    Z: BatchReader<Time = ()>,
    O: IndexedZSet<Key = Z::Key>,
    Acc: DataTrait + ?Sized,
{
    #[trace]
    async fn eval(&mut self, i: &Z) -> O {
        let mut builder = O::Builder::with_capacity(&self.factories, i.len());
        let mut agg = self.option_output_factory.default_box();

        let mut cursor = i.cursor();

        while cursor.key_valid() {
            self.aggregator
                .aggregate_and_finalize(&mut CursorGroup::new(&mut cursor, ()), agg.as_mut());

            if let Some(agg) = agg.get_mut() {
                builder.push_val_diff_mut(agg, ZWeight::one().erase_mut());
                builder.push_key(cursor.key());
            }
            cursor.step_key();
        }
        builder.done()
    }
}

/// Incremental version of the `Aggregate` operator that works
/// in arbitrarily nested scopes.
///
/// This is a binary operator with the following inputs:
/// * `delta` - stream of changes to the input indexed Z-set, only used to
///   compute the set of affected keys.
/// * `input_trace` - a trace of the input indexed Z-set.
///
/// # Type arguments
///
/// * `Z` - input batch type in the `delta` stream.
/// * `IT` - input trace type.
/// * `A` - aggregator to apply to each input group.
/// * `Clk` - clock that keeps track of the current logical time.
///
/// # Design
///
/// There are two possible strategies for incremental implementation of
/// non-linear operators like `distinct` and `aggregate`: (1) compute
/// the value to retract for each updated key using the input trace,
/// (2) compute new values of updated keys only and extract the old values
/// to retract from the trace of the output collection. We adopt the
/// second approach here, which avoids re-computation by using more
/// memory.  This is based on two considerations.  First, computing
/// an aggregate can be a relatively expensive operation, as it
/// typically requires scanning all values associated with each key.
/// Second, the aggregated output trace will typically use less memory
/// than the input trace, as it summarizes all values per key in one
/// aggregate value.  Of course these are not always true, and we may
/// want to one day build an alternative implementation using the
/// other approach.
struct AggregateIncremental<Z, IT, Acc, Out, Clk>
where
    Acc: DataTrait + ?Sized,
    Out: DataTrait + ?Sized,
    Z: BatchReader,
    IT: BatchReader,
{
    keys_factory: &'static dyn Factory<DynSet<IT::Key>>,
    output_pair_factory: &'static dyn Factory<DynPair<IT::Key, DynOpt<Out>>>,
    output_pairs_factory: &'static dyn Factory<DynPairs<IT::Key, DynOpt<Out>>>,
    clock: Clk,
    aggregator: Box<dyn DynAggregator<Z::Val, IT::Time, Z::R, Accumulator = Acc, Output = Out>>,
    // The last input batch was empty - used in fixedpoint computation.
    empty_input: bool,
    // The last output batch was empty - used in fixedpoint computation.
    empty_output: bool,
    // Keys that may need updating at future times.
    keys_of_interest: BTreeMap<IT::Time, Box<DynSet<IT::Key>>>,
    // Buffer used in computing per-key outputs.
    // Keep it here to reuse allocation across multiple operations.
    _type: PhantomData<fn(&Z, &IT)>,
}

impl<Z, IT, Acc, Out, Clk> AggregateIncremental<Z, IT, Acc, Out, Clk>
where
    Clk: WithClock<Time = IT::Time>,
    Z: Batch<Time = ()>,
    IT: BatchReader<Key = Z::Key, Val = Z::Val, R = Z::R>,
    Acc: DataTrait + ?Sized,
    Out: DataTrait + ?Sized,
{
    pub fn new(
        keys_factory: &'static dyn Factory<DynSet<IT::Key>>,
        output_pair_factory: &'static dyn Factory<DynPair<IT::Key, DynOpt<Out>>>,
        output_pairs_factory: &'static dyn Factory<DynPairs<IT::Key, DynOpt<Out>>>,
        aggregator: &dyn DynAggregator<Z::Val, IT::Time, Z::R, Accumulator = Acc, Output = Out>,
        clock: Clk,
    ) -> Self {
        Self {
            keys_factory,
            output_pair_factory,
            output_pairs_factory,
            clock,
            aggregator: clone_box(aggregator),
            empty_input: false,
            empty_output: false,
            keys_of_interest: BTreeMap::new(),
            _type: PhantomData,
        }
    }

    /// Compute output of the operator for `key`.
    ///
    /// # Arguments
    ///
    /// * `input_cursor` - cursor over the input trace that contains all updates
    ///   to the indexed Z-set that we are aggregating, up to and including the
    ///   current timestamp.
    /// * `builder` - builder that accumulates output tuples for the current
    ///   evalution of the operator.
    ///
    /// # Computing output
    ///
    /// We use the `input_cursor` to compute the current value of the
    /// aggregate as:
    ///
    /// ```text
    /// (1) agg = aggregate({(v, w) | (v, t, w) ∈ input_cursor[key], t <= time})
    /// ```
    ///
    /// # Updating `keys_of_interest`
    ///
    /// For each `(v, t, w)` tuple in `input_cursor`, such that `t <= time`
    /// does not hold, the tuple can affect the value of the aggregate at
    /// time `time.join(t)`.  We compute the smallest (according to the
    /// global lexicographic ordering of timestamps that reflects the order in
    /// which DBSP processes timestamps) such `t` and insert `key` in
    /// `keys_of_interest` for that time.
    ///
    /// Note that this implementation may end up running `input_cursor` twice,
    /// once when computing the aggregate and once when updating
    /// `keys_of_interest`. This allows cleanly encapsulating the
    /// aggregation logic in the `Aggregator` trait.  The second iteration
    /// is only needed inside nested scopes and can in the future be
    /// optimized to terminate early.
    #[trace]
    fn eval_key(
        &mut self,
        key: &Z::Key,
        input_cursor: &mut IT::Cursor<'_>,
        output: &mut DynPairs<Z::Key, DynOpt<Out>>,
        time: &Clk::Time,
        // Temporary variable.
        key_aggregate: &mut DynPair<Z::Key, DynOpt<Out>>,
    ) {
        // println!(
        //     "{}: eval_key({key}) @ {:?}",
        //     Runtime::worker_index(),
        //     time
        // );

        let (output_key, aggregate) = key_aggregate.split_mut();
        key.clone_to(output_key);

        // If found, compute `agg` using formula (1) above; otherwise the aggregate is
        // `0`.
        if input_cursor.seek_key_exact(key) {
            // Apply aggregator to a `CursorGroup` that iterates over the nested
            // Z-set associated with `input_cursor.key()` at time `time`.
            self.aggregator.aggregate_and_finalize(
                &mut CursorGroup::new(input_cursor, time.clone()),
                aggregate,
            );

            output.push_val(key_aggregate);

            // Compute the closest future timestamp when we may need to reevaluate
            // this key (See 'Updating keys of interest' section above).
            //
            // Skip this relatively expensive computation when running in the root
            // scope using unit timestamps (`IT::Time = ()`).
            if TypeId::of::<IT::Time>() != TypeId::of::<()>() {
                input_cursor.rewind_vals();

                let mut time_of_interest = None;
                while input_cursor.val_valid() {
                    // TODO: More efficient lookup of the smallest timestamp exceeding
                    // `time`, without scanning everything.
                    input_cursor.map_times(&mut |ts, _| {
                        time_of_interest = if !ts.less_equal(time) {
                            match &time_of_interest {
                                None => Some(time.join(ts)),
                                Some(time_of_interest) => {
                                    Some(min(time_of_interest, &time.join(ts)).clone())
                                }
                            }
                        } else {
                            time_of_interest.clone()
                        }
                    });
                    input_cursor.step_val();
                }

                if let Some(t) = time_of_interest {
                    self.keys_of_interest
                        .entry(t)
                        .or_insert_with(|| self.keys_factory.default_box())
                        .insert_ref(key);
                }
            }
        } else {
            // No need to set `aggregate` to None, since `None` is the default value.
            // aggregate.none();
            output.push_val(key_aggregate);
        }
    }
}

impl<Z, IT, Acc, Out, Clk> Operator for AggregateIncremental<Z, IT, Acc, Out, Clk>
where
    Clk: WithClock<Time = IT::Time> + 'static,
    Z: BatchReader,
    IT: BatchReader,
    Acc: DataTrait + ?Sized,
    Out: DataTrait + ?Sized,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("AggregateIncremental")
    }

    fn clock_start(&mut self, scope: Scope) {
        if scope == 0 {
            self.empty_input = false;
            self.empty_output = false;
        }
    }

    fn clock_end(&mut self, scope: Scope) {
        debug_assert!(self.keys_of_interest.keys().all(|ts| {
            if ts.less_equal(&self.clock.time().epoch_end(scope)) {
                println!(
                    "ts: {ts:?}, epoch_end: {:?}",
                    self.clock.time().epoch_end(scope)
                );
            }
            !ts.less_equal(&self.clock.time().epoch_end(scope))
        }));
    }

    fn fixedpoint(&self, scope: Scope) -> bool {
        let epoch_end = self.clock.time().epoch_end(scope);

        self.empty_input
            && self.empty_output
            && self
                .keys_of_interest
                .keys()
                .all(|ts| !ts.less_equal(&epoch_end))
    }
}

impl<Z, IT, Acc, Out, Clk> BinaryOperator<Z, IT, Box<DynPairs<Z::Key, DynOpt<Out>>>>
    for AggregateIncremental<Z, IT, Acc, Out, Clk>
where
    Clk: WithClock<Time = IT::Time> + 'static,
    Z: Batch<Time = ()>,
    IT: BatchReader<Key = Z::Key, Val = Z::Val, R = Z::R> + Clone,
    Acc: DataTrait + ?Sized,
    Out: DataTrait + ?Sized,
{
    #[trace]
    async fn eval(&mut self, delta: &Z, input_trace: &IT) -> Box<DynPairs<Z::Key, DynOpt<Out>>> {
        // println!(
        //     "{}: AggregateIncremental::eval @{:?}\ndelta:{delta}",
        //     Runtime::worker_index(),
        //     self.time
        // );
        self.empty_input = delta.is_empty();

        let mut result = self.output_pairs_factory.default_box();
        result.reserve(delta.key_count());

        let mut delta_cursor = delta.cursor();
        let mut input_trace_cursor = input_trace.cursor();

        let time = self.clock.time();

        // Previously encountered keys that may affect output at the
        // current time.
        let keys_of_interest = self
            .keys_of_interest
            .remove(&time)
            .unwrap_or_else(|| self.keys_factory.default_box());

        let mut keys_of_interest = keys_of_interest.dyn_iter();

        let mut key_of_interest = keys_of_interest.next();
        let mut key_aggregate = self.output_pair_factory.default_box();

        // Iterate over all keys in `delta_cursor` and `keys_of_interest`.
        while delta_cursor.key_valid() && key_of_interest.is_some() {
            let key_of_interest_ref = key_of_interest.unwrap();

            match delta_cursor.key().cmp(key_of_interest_ref) {
                // Key only appears in `delta`.
                Ordering::Less => {
                    self.eval_key(
                        delta_cursor.key(),
                        &mut input_trace_cursor,
                        result.as_mut(),
                        &time,
                        key_aggregate.as_mut(),
                    );
                    delta_cursor.step_key();
                }
                // Key only appears in `keys_of_interest`.
                Ordering::Greater => {
                    self.eval_key(
                        key_of_interest_ref,
                        &mut input_trace_cursor,
                        result.as_mut(),
                        &time,
                        key_aggregate.as_mut(),
                    );
                    key_of_interest = keys_of_interest.next();
                }
                // Key appears in both `delta` and `keys_of_interest`.
                Ordering::Equal => {
                    self.eval_key(
                        delta_cursor.key(),
                        &mut input_trace_cursor,
                        result.as_mut(),
                        &time,
                        key_aggregate.as_mut(),
                    );
                    delta_cursor.step_key();
                    key_of_interest = keys_of_interest.next();
                }
            }
        }

        while delta_cursor.key_valid() {
            self.eval_key(
                delta_cursor.key(),
                &mut input_trace_cursor,
                result.as_mut(),
                &time,
                key_aggregate.as_mut(),
            );
            delta_cursor.step_key();
        }

        while key_of_interest.is_some() {
            self.eval_key(
                key_of_interest.unwrap(),
                &mut input_trace_cursor,
                result.as_mut(),
                &time,
                key_aggregate.as_mut(),
            );
            key_of_interest = keys_of_interest.next();
        }

        self.empty_output = result.is_empty();
        result
    }
}

#[cfg(test)]
pub mod test {
    use anyhow::Result as AnyResult;

    use std::{
        cell::RefCell,
        rc::Rc,
        sync::{Arc, Mutex},
    };

    use crate::{
        algebra::DefaultSemigroup,
        indexed_zset,
        operator::{
            dynamic::aggregate::{MinSome1, Postprocess},
            Fold, GeneratorNested, Min,
        },
        trace::{BatchReader, Cursor},
        typed_batch::{OrdIndexedZSet, OrdZSet},
        utils::{Tup1, Tup3},
        zset, Circuit, RootCircuit, Runtime, Stream, ZWeight,
    };

    type TestZSet = OrdZSet<Tup2<u64, i64>>;

    fn aggregate_test_circuit(
        circuit: &mut RootCircuit,
        inputs: Vec<Vec<TestZSet>>,
    ) -> AnyResult<()> {
        let mut inputs = inputs.into_iter();

        circuit
            .iterate(|child| {
                let counter = Rc::new(RefCell::new(0));
                let counter_clone = counter.clone();

                let input: Stream<_, OrdIndexedZSet<u64, i64>> = child
                    .add_source(GeneratorNested::new(Box::new(move || {
                        *counter_clone.borrow_mut() = 0;
                        if Runtime::worker_index() == 0 {
                            let mut deltas = inputs.next().unwrap_or_default().into_iter();
                            Box::new(move || deltas.next().unwrap_or_else(|| zset! {}))
                        } else {
                            Box::new(|| zset! {})
                        }
                    })))
                    .map_index(|Tup2(k, v)| (*k, *v));

                // Weighted sum aggregate.
                let sum = <Fold<i64, i64, DefaultSemigroup<_>, _, _>>::new(
                    0,
                    |acc: &mut i64, v: &i64, w: i64| *acc += *v * w,
                );

                // Weighted sum aggregate that returns only the weighted sum
                // value and is therefore linear.
                let sum_linear = |val: &i64| -> i64 { *val };

                let sum_inc: Stream<_, OrdIndexedZSet<u64, i64>> =
                    input.aggregate(sum.clone()).gather(0);
                let sum_inc_linear: Stream<_, OrdIndexedZSet<u64, i64>> =
                    input.aggregate_linear(sum_linear).gather(0);
                let sum_noninc = input
                    .integrate_nested()
                    .integrate()
                    .stream_aggregate(sum)
                    .differentiate()
                    .differentiate_nested()
                    .gather(0);
                let sum_noninc_linear = input
                    .integrate_nested()
                    .integrate()
                    .stream_aggregate_linear(sum_linear)
                    .differentiate()
                    .differentiate_nested()
                    .gather(0);

                // Compare outputs of all four implementations.
                sum_inc
                    .apply2(
                        &sum_noninc,
                        |d1: &OrdIndexedZSet<u64, i64>, d2: &OrdIndexedZSet<u64, i64>| {
                            (d1.clone(), d2.clone())
                        },
                    )
                    .inspect(|(d1, d2)| {
                        //println!("{}: incremental: {:?}", Runtime::worker_index(), d1);
                        //println!("{}: non-incremental: {:?}", Runtime::worker_index(), d2);
                        assert_eq!(d1, d2);
                    });

                sum_inc.apply2(
                    &sum_inc_linear,
                    |d1: &OrdIndexedZSet<u64, i64>, d2: &OrdIndexedZSet<u64, i64>| {
                        // println!("{}: incremental: {:?}", Runtime::worker_index(), d1);
                        // println!("{}: linear: {:?}", Runtime::worker_index(), d2);

                        // Compare d1 and d2 modulo 0 values (linear aggregation removes them
                        // from the collection).
                        let mut cursor1 = d1.cursor();
                        let mut cursor2 = d2.cursor();

                        while cursor1.key_valid() {
                            while cursor1.val_valid() {
                                if *cursor1.val().downcast_checked::<i64>() != 0 {
                                    assert!(cursor2.key_valid());
                                    assert_eq!(cursor2.key(), cursor1.key());
                                    assert!(cursor2.val_valid());
                                    assert_eq!(cursor2.val(), cursor1.val());
                                    assert_eq!(cursor2.weight(), cursor1.weight());
                                    cursor2.step_val();
                                }

                                cursor1.step_val();
                            }

                            if cursor2.key_valid() && cursor2.key() == cursor1.key() {
                                cursor2.step_key();
                            }

                            cursor1.step_key();
                        }
                        assert!(!cursor2.key_valid());
                    },
                );

                sum_inc_linear
                    .apply2(
                        &sum_noninc_linear,
                        |d1: &OrdIndexedZSet<u64, i64>, d2: &OrdIndexedZSet<u64, i64>| {
                            (d1.clone(), d2.clone())
                        },
                    )
                    .inspect(|(d1, d2)| {
                        //println!("{}: incremental: {:?}", Runtime::worker_index(), d1);
                        //println!("{}: non-incremental: {:?}", Runtime::worker_index(), d2);
                        assert_eq!(d1, d2);
                    });

                let min_inc = input.aggregate(Min).gather(0);
                let min_noninc = input
                    .integrate_nested()
                    .integrate()
                    .stream_aggregate(Min)
                    .differentiate()
                    .differentiate_nested()
                    .gather(0);

                min_inc
                    .apply2(
                        &min_noninc,
                        |d1: &OrdIndexedZSet<u64, i64>, d2: &OrdIndexedZSet<u64, i64>| {
                            (d1.clone(), d2.clone())
                        },
                    )
                    .inspect(|(d1, d2)| {
                        assert_eq!(d1, d2);
                    });

                Ok((
                    async move || {
                        *counter.borrow_mut() += 1;
                        Ok(*counter.borrow() == MAX_ITERATIONS)
                    },
                    (),
                ))
            })
            .unwrap();
        Ok(())
    }

    use crate::{dynamic::DowncastTrait, utils::Tup2};
    use proptest::{collection, prelude::*};

    const MAX_ROUNDS: usize = 10;
    const MAX_ITERATIONS: usize = 10;
    const NUM_KEYS: u64 = 3;
    const MAX_VAL: i64 = 3;
    const MAX_TUPLES: usize = 8;

    pub fn test_zset() -> impl Strategy<Value = TestZSet> {
        collection::vec(
            (
                (0..NUM_KEYS, -MAX_VAL..MAX_VAL).prop_map(|(x, y)| Tup2(x, y)),
                -1..=1i64,
            ),
            0..MAX_TUPLES,
        )
        .prop_map(|tuples| {
            OrdZSet::from_tuples(
                (),
                tuples
                    .into_iter()
                    .map(|(k, w)| Tup2(Tup2(k, ()), w))
                    .collect(),
            )
        })
    }
    pub fn test_input() -> impl Strategy<Value = Vec<Vec<TestZSet>>> {
        collection::vec(
            collection::vec(test_zset(), 0..MAX_ITERATIONS),
            0..MAX_ROUNDS,
        )
    }

    proptest! {
        #[test]
        fn proptest_aggregate_test_st(inputs in test_input()) {
            let iterations = inputs.len();
            let circuit = RootCircuit::build(|circuit| aggregate_test_circuit(circuit, inputs)).unwrap().0;

            for _ in 0..iterations {
                circuit.step().unwrap();
            }
        }

        #[test]
        fn proptest_aggregate_test_mt(inputs in test_input(), log_workers in (1..=4)) {
            let workers = 1usize << log_workers;
            let iterations = inputs.len();
            let mut circuit = Runtime::init_circuit(workers, |circuit| aggregate_test_circuit(circuit, inputs)).unwrap().0;

            for _ in 0..iterations {
                circuit.step().unwrap();
            }

            circuit.kill().unwrap();
        }
    }

    fn count_test(workers: usize) {
        let count_weighted_output: Arc<Mutex<OrdIndexedZSet<u64, ZWeight>>> =
            Arc::new(Mutex::new(indexed_zset! {}));
        let sum_weighted_output: Arc<Mutex<OrdIndexedZSet<u64, i64>>> =
            Arc::new(Mutex::new(indexed_zset! {}));
        let count_distinct_output: Arc<Mutex<OrdIndexedZSet<u64, u64>>> =
            Arc::new(Mutex::new(indexed_zset! {}));
        let sum_distinct_output: Arc<Mutex<OrdIndexedZSet<u64, u64>>> =
            Arc::new(Mutex::new(indexed_zset! {}));

        let count_weighted_output_clone = count_weighted_output.clone();
        let count_distinct_output_clone = count_distinct_output.clone();
        let sum_weighted_output_clone = sum_weighted_output.clone();
        let sum_distinct_output_clone = sum_distinct_output.clone();

        let (mut dbsp, input_handle) = Runtime::init_circuit(workers, move |circuit| {
            let (input_stream, input_handle) = circuit.add_input_indexed_zset::<u64, u64>();
            input_stream
                .weighted_count()
                .gather(0)
                .inspect(move |batch| {
                    if Runtime::worker_index() == 0 {
                        *count_weighted_output.lock().unwrap() = batch.clone();
                    }
                });

            input_stream
                .aggregate_linear(|value: &u64| *value as i64)
                .gather(0)
                .inspect(move |batch| {
                    if Runtime::worker_index() == 0 {
                        *sum_weighted_output.lock().unwrap() = batch.clone();
                    }
                });

            input_stream
                .aggregate(<Fold<_, _, DefaultSemigroup<_>, _, _>>::new(
                    0,
                    |sum: &mut u64, _v: &u64, _w| *sum += 1,
                ))
                .gather(0)
                .inspect(move |batch| {
                    if Runtime::worker_index() == 0 {
                        *count_distinct_output.lock().unwrap() = batch.clone();
                    }
                });

            input_stream
                .aggregate(<Fold<_, _, DefaultSemigroup<_>, _, _>>::new(
                    0,
                    |sum: &mut u64, v: &u64, _w| *sum += v,
                ))
                .gather(0)
                .inspect(move |batch| {
                    if Runtime::worker_index() == 0 {
                        *sum_distinct_output.lock().unwrap() = batch.clone();
                    }
                });

            Ok(input_handle)
        })
        .unwrap();

        input_handle.append(&mut vec![Tup2(1u64, Tup2(1u64, 1)), Tup2(1, Tup2(2, 2))]);
        dbsp.step().unwrap();
        assert_eq!(
            &*count_distinct_output_clone.lock().unwrap(),
            &indexed_zset! {1 => {2 => 1}}
        );
        assert_eq!(
            &*sum_distinct_output_clone.lock().unwrap(),
            &indexed_zset! {1 => {3 => 1}}
        );
        assert_eq!(
            &*count_weighted_output_clone.lock().unwrap(),
            &indexed_zset! {1 => {3 => 1}}
        );
        assert_eq!(
            &*sum_weighted_output_clone.lock().unwrap(),
            &indexed_zset! {1 => {5 => 1}}
        );

        input_handle.append(&mut vec![
            Tup2(2, Tup2(2, 1)),
            Tup2(2, Tup2(4, 1)),
            Tup2(1, Tup2(2, -1)),
        ]);
        dbsp.step().unwrap();
        assert_eq!(
            &*count_distinct_output_clone.lock().unwrap(),
            &indexed_zset! {2 => {2 => 1}}
        );
        assert_eq!(
            &*sum_distinct_output_clone.lock().unwrap(),
            &indexed_zset! {2 => {6 => 1}}
        );
        assert_eq!(
            &*count_weighted_output_clone.lock().unwrap(),
            &indexed_zset! {1 => {3 => -1, 2 => 1}, 2 => {2 => 1}}
        );
        assert_eq!(
            &*sum_weighted_output_clone.lock().unwrap(),
            &indexed_zset! {2 => {6 => 1}, 1 => {5 => -1, 3 => 1}}
        );

        input_handle.append(&mut vec![Tup2(1, Tup2(3, 1)), Tup2(1, Tup2(2, -1))]);
        dbsp.step().unwrap();
        assert_eq!(
            &*count_distinct_output_clone.lock().unwrap(),
            &indexed_zset! {}
        );
        assert_eq!(
            &*sum_distinct_output_clone.lock().unwrap(),
            &indexed_zset! {1 => {3 => -1, 4 => 1}}
        );
        assert_eq!(
            &*count_weighted_output_clone.lock().unwrap(),
            &indexed_zset! {}
        );
        assert_eq!(
            &*sum_weighted_output_clone.lock().unwrap(),
            &indexed_zset! {1 => {3 => -1, 4 => 1}}
        );

        dbsp.kill().unwrap();
    }

    #[test]
    fn count_test1() {
        count_test(1);
    }

    #[test]
    fn count_test4() {
        count_test(4);
    }

    #[test]
    fn min_some_test() {
        let (mut dbsp, (input_handle, output_handle)) = Runtime::init_circuit(4, move |circuit| {
            let (input_stream, input_handle) =
                circuit.add_input_indexed_zset::<u64, Tup1<Option<u64>>>();
            let output_handle = input_stream.aggregate(MinSome1).integrate().output();

            Ok((input_handle, output_handle))
        })
        .unwrap();

        // min({None}) = None
        // min({5}) = 5
        input_handle.append(&mut vec![
            Tup2(1u64, Tup2(Tup1(None), 1)),
            Tup2(2u64, Tup2(Tup1(Some(5)), 1)),
        ]);
        dbsp.step().unwrap();
        let output = output_handle.consolidate();
        assert_eq!(
            &output,
            &indexed_zset! {1 => {Tup1(None) => 1}, 2 => { Tup1(Some(5)) => 1 }}
        );

        // min({None, 3}) = 3
        // min({None, 5}) = 5
        input_handle.append(&mut vec![
            Tup2(1u64, Tup2(Tup1(Some(3)), 1)),
            Tup2(2u64, Tup2(Tup1(None), 1)),
        ]);
        dbsp.step().unwrap();
        let output = output_handle.consolidate();
        assert_eq!(
            &output,
            &indexed_zset! {1 => {Tup1(Some(3)) => 1}, 2 => { Tup1(Some(5)) => 1 }}
        );

        // min({3}) = 3
        // min({None}) = None
        input_handle.append(&mut vec![
            Tup2(1u64, Tup2(Tup1(None), -1)),
            Tup2(2u64, Tup2(Tup1(Some(5)), -1)),
        ]);
        dbsp.step().unwrap();
        let output = output_handle.consolidate();
        assert_eq!(
            &output,
            &indexed_zset! {1 => {Tup1(Some(3)) => 1}, 2 => { Tup1(None) => 1 }}
        );

        dbsp.kill().unwrap();
    }

    // Test `Postprocess` wrapper.
    #[test]
    fn postprocess_test() {
        let (mut dbsp, (input_handle, output_handle)) = Runtime::init_circuit(4, move |circuit| {
            let (input_stream, input_handle) = circuit.add_input_indexed_zset::<u64, Tup1<u64>>();
            // Postprocessing ofren used in SQL: wrap result in Option.
            let output_handle = input_stream
                .aggregate(Postprocess::new(Min, |x: &Tup1<u64>| Tup1(Some(x.0))))
                .integrate()
                .output();

            Ok((input_handle, output_handle))
        })
        .unwrap();

        input_handle.append(&mut vec![
            Tup2(1u64, Tup2(Tup1(1), 1)),
            Tup2(2u64, Tup2(Tup1(5), 1)),
        ]);
        dbsp.step().unwrap();
        let output = output_handle.consolidate();
        assert_eq!(
            &output,
            &indexed_zset! {1 => {Tup1(Some(1)) => 1}, 2 => { Tup1(Some(5)) => 1 }}
        );

        input_handle.append(&mut vec![
            Tup2(1u64, Tup2(Tup1(3), 1)),
            Tup2(2u64, Tup2(Tup1(2), 1)),
        ]);
        dbsp.step().unwrap();
        let output = output_handle.consolidate();
        assert_eq!(
            &output,
            &indexed_zset! {1 => {Tup1(Some(1)) => 1}, 2 => { Tup1(Some(2)) => 1 }}
        );

        input_handle.append(&mut vec![
            Tup2(1u64, Tup2(Tup1(1), -1)),
            Tup2(2u64, Tup2(Tup1(5), -1)),
        ]);
        dbsp.step().unwrap();
        let output = output_handle.consolidate();
        assert_eq!(
            &output,
            &indexed_zset! {1 => {Tup1(Some(3)) => 1}, 2 => { Tup1(Some(2)) => 1 }}
        );

        dbsp.kill().unwrap();
    }

    /// Uses `aggregate_linear_postprocess` to implement SQL-style linear aggregation for nullable values:
    ///
    /// - All elements in a group are NULL -> sum = NULL
    /// - Otherwise it's the sum of all non-null elements.
    #[test]
    fn aggregate_linear_postprocess_test() {
        // Aggregation function that returns a 3-tuple including:
        // 1. Number of elements in the group.
        // 2. Number of non-null elements
        // 3. Sum of non-null elements
        // Note we only compute 1 to make sure that the aggregate is not
        // zero unless the group is actualy empty.
        fn agg_func(x: &Option<i32>) -> Tup3<i32, i32, i32> {
            let v = x.unwrap_or_default();

            Tup3(1, if x.is_some() { 1 } else { 0 }, v)
        }

        // Postprocessing: sum of non-null elements or NULL if
        // all elements are NULL.
        fn postprocess_func(Tup3(_count, non_nulls, sum): Tup3<i32, i32, i32>) -> Option<i32> {
            if non_nulls > 0 {
                Some(sum)
            } else {
                None
            }
        }

        let (mut dbsp, (input_handle, _waterline_handle, output_handle)) =
            Runtime::init_circuit(4, |circuit| {
                let (input, input_handle) = circuit.add_input_zset::<Tup2<i32, Option<i32>>>();
                let (waterline, waterline_handle) = circuit.add_input_stream::<i32>();

                let indexed_input = input.map_index(|Tup2(k, v)| (*k, *v));

                let sum = indexed_input.aggregate_linear_postprocess(agg_func, postprocess_func);
                let sum_retain = indexed_input.aggregate_linear_postprocess_retain_keys(
                    &waterline.typed_box(),
                    |k, ts| k >= ts,
                    agg_func,
                    postprocess_func,
                );

                // aggregate_linear_postprocess must be equivalent to aggregate_linear().map_index().
                let sum_slow = indexed_input
                    .aggregate_linear(agg_func)
                    .map_index(|(k, v)| (*k, postprocess_func(*v)));
                let sum_slow_retain = indexed_input
                    .aggregate_linear_retain_keys(&waterline.typed_box(), |k, ts| k >= ts, agg_func)
                    .map_index(|(k, v)| (*k, postprocess_func(*v)));

                sum.apply2(&sum_slow, |sum, sum_slow| assert_eq!(sum, sum_slow));
                sum_retain.apply2(&sum_slow, |sum, sum_slow| assert_eq!(sum, sum_slow));
                sum_slow_retain.apply2(&sum_slow, |sum, sum_slow| assert_eq!(sum, sum_slow));

                let output_handle = sum.integrate().output();

                Ok((input_handle, waterline_handle, output_handle))
            })
            .unwrap();

        // A single NULL element -> aggregate is NULL
        input_handle.append(&mut vec![Tup2(Tup2(1i32, None), 2)]);
        dbsp.step().unwrap();
        assert_eq!(
            output_handle.consolidate(),
            indexed_zset! {1 => {None => 1}}
        );

        // +5 -> aggregate = 5
        input_handle.append(&mut vec![Tup2(Tup2(1i32, Some(5)), 1)]);
        dbsp.step().unwrap();
        assert_eq!(
            output_handle.consolidate(),
            indexed_zset! {1 => {Some(5) => 1}}
        );

        // +3 * 2 -> aggrregate = 11
        input_handle.append(&mut vec![Tup2(Tup2(1i32, Some(3)), 2)]);
        dbsp.step().unwrap();
        assert_eq!(
            output_handle.consolidate(),
            indexed_zset! {1 => {Some(11) => 1}}
        );

        // + (-11) -> aggregate = 0
        input_handle.append(&mut vec![Tup2(Tup2(1i32, Some(-11)), 1)]);
        dbsp.step().unwrap();
        assert_eq!(
            output_handle.consolidate(),
            indexed_zset! {1 => {Some(0) => 1}}
        );

        // Remove all non-NULL eleements -> aggregate = NULL
        input_handle.append(&mut vec![
            Tup2(Tup2(1i32, Some(-11)), -1),
            Tup2(Tup2(1i32, Some(5)), -1),
            Tup2(Tup2(1i32, Some(3)), -2),
        ]);
        dbsp.step().unwrap();
        assert_eq!(
            output_handle.consolidate(),
            indexed_zset! {1 => {None => 1}}
        );

        // Remove the remaining NULL -> the whole group disappears.
        input_handle.append(&mut vec![Tup2(Tup2(1i32, None), -2)]);
        dbsp.step().unwrap();
        assert_eq!(output_handle.consolidate(), indexed_zset! {});
    }

    // Like previous test but uses i8
    #[test]
    fn aggregate_linear_postprocess_test_i8() {
        // Aggregation function that returns a 3-tuple including:
        // 1. Number of elements in the group.
        // 2. Number of non-null elements
        // 3. Sum of non-null elements
        // Note we only compute 1 to make sure that the aggregate is not
        // zero unless the group is actualy empty.
        fn agg_func(x: &Option<i8>) -> Tup3<i8, i8, i8> {
            let v = x.unwrap_or_default();

            Tup3(1, if x.is_some() { 1 } else { 0 }, v)
        }

        // Postprocessing: sum of non-null elements or NULL if
        // all elements are NULL.
        fn postprocess_func(Tup3(_count, non_nulls, sum): Tup3<i8, i8, i8>) -> Option<i8> {
            if non_nulls > 0 {
                Some(sum)
            } else {
                None
            }
        }

        let (mut dbsp, (input_handle, output_handle)) = Runtime::init_circuit(4, |circuit| {
            let (input, input_handle) = circuit.add_input_zset::<Tup2<i8, Option<i8>>>();

            let indexed_input = input.map_index(|Tup2(k, v)| (*k, *v));

            let sum = indexed_input.aggregate_linear_postprocess(agg_func, postprocess_func);

            // aggregate_linear_postprocess must be equivalent to aggregate_linear().map_index().
            let sum_slow = indexed_input
                .aggregate_linear(agg_func)
                .map_index(|(k, v)| (*k, postprocess_func(*v)));

            sum.apply2(&sum_slow, |sum, sum_slow| assert_eq!(sum, sum_slow));

            let output_handle = sum.integrate().output();

            Ok((input_handle, output_handle))
        })
        .unwrap();

        // A single NULL element -> aggregate is NULL
        input_handle.append(&mut vec![Tup2(Tup2(1i8, None), 2)]);
        dbsp.step().unwrap();
        assert_eq!(
            output_handle.consolidate(),
            indexed_zset! {1 => {None => 1}}
        );

        // +5 -> aggregate = 5
        input_handle.append(&mut vec![Tup2(Tup2(1i8, Some(5)), 1)]);
        dbsp.step().unwrap();
        assert_eq!(
            output_handle.consolidate(),
            indexed_zset! {1 => {Some(5) => 1}}
        );

        // +3 * 2 -> aggrregate = 11
        input_handle.append(&mut vec![Tup2(Tup2(1i8, Some(3)), 2)]);
        dbsp.step().unwrap();
        assert_eq!(
            output_handle.consolidate(),
            indexed_zset! {1 => {Some(11) => 1}}
        );

        // + (-11) -> aggregate = 0
        input_handle.append(&mut vec![Tup2(Tup2(1i8, Some(-11)), 1)]);
        dbsp.step().unwrap();
        assert_eq!(
            output_handle.consolidate(),
            indexed_zset! {1 => {Some(0) => 1}}
        );

        // Remove all non-NULL eleements -> aggregate = NULL
        input_handle.append(&mut vec![
            Tup2(Tup2(1i8, Some(-11)), -1),
            Tup2(Tup2(1i8, Some(5)), -1),
            Tup2(Tup2(1i8, Some(3)), -2),
        ]);
        dbsp.step().unwrap();
        assert_eq!(
            output_handle.consolidate(),
            indexed_zset! {1 => {None => 1}}
        );

        // Remove the remaining NULL -> the whole group disappears.
        input_handle.append(&mut vec![Tup2(Tup2(1i8, None), -2)]);
        dbsp.step().unwrap();
        assert_eq!(output_handle.consolidate(), indexed_zset! {});
    }
}
