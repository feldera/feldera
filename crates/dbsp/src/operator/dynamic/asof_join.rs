use std::{borrow::Cow, cmp::Ordering, marker::PhantomData, panic::Location};

use crate::{
    algebra::{IndexedZSet, IndexedZSetReader, OrdIndexedZSet, OrdZSet, ZCursor, ZTrace},
    circuit::{
        metadata::OperatorLocation,
        operator_traits::{Operator, QuaternaryOperator},
    },
    dynamic::{
        ClonableTrait, DataTrait, DowncastTrait, DynPair, DynUnit, DynVec, DynWeightedPairs, Erase,
        Factory, LeanVec, WithFactory,
    },
    trace::{cursor::CursorPair, BatchFactories, BatchReaderFactories, Cursor},
    Circuit, DBData, DynZWeight, RootCircuit, Scope, Stream, ZWeight,
};

pub struct AsofJoinFactories<TS, I1, I2, O>
where
    TS: DataTrait + ?Sized,
    I1: IndexedZSetReader,
    I2: IndexedZSetReader,
    O: IndexedZSet,
{
    pub timestamp_factory: &'static dyn Factory<TS>,
    pub timestamps_factory: &'static dyn Factory<DynVec<TS>>,
    pub left_factories: I1::Factories,
    pub right_factories: I2::Factories,
    pub output_factories: O::Factories,
}

impl<TS, I1, I2, O> AsofJoinFactories<TS, I1, I2, O>
where
    TS: DataTrait + ?Sized,
    I1: IndexedZSetReader,
    I2: IndexedZSetReader<Key = I1::Key>,
    O: IndexedZSet,
{
    pub fn new<TSType, KType, V1Type, V2Type, OKType, OVType>() -> Self
    where
        TSType: DBData + Erase<TS>,
        KType: DBData + Erase<I1::Key>,
        V1Type: DBData + Erase<I1::Val>,
        V2Type: DBData + Erase<I2::Val>,
        OKType: DBData + Erase<O::Key>,
        OVType: DBData + Erase<O::Val>,
    {
        Self {
            timestamp_factory: WithFactory::<TSType>::FACTORY,
            timestamps_factory: WithFactory::<LeanVec<TSType>>::FACTORY,
            left_factories: BatchReaderFactories::new::<KType, V1Type, ZWeight>(),
            right_factories: BatchReaderFactories::new::<KType, V2Type, ZWeight>(),
            output_factories: BatchReaderFactories::new::<OKType, OVType, ZWeight>(),
        }
    }
}

impl<TS, I1, I2, O> Clone for AsofJoinFactories<TS, I1, I2, O>
where
    TS: DataTrait + ?Sized,
    I1: IndexedZSetReader,
    I2: IndexedZSetReader,
    O: IndexedZSet,
{
    fn clone(&self) -> Self {
        Self {
            timestamp_factory: self.timestamp_factory,
            timestamps_factory: self.timestamps_factory,
            left_factories: self.left_factories.clone(),
            right_factories: self.right_factories.clone(),
            output_factories: self.output_factories.clone(),
        }
    }
}

impl<I1> Stream<RootCircuit, I1>
where
    I1: IndexedZSet + Send,
{
    /// See [`Stream::asof_join`].
    #[track_caller]
    pub fn dyn_asof_join<TS, I2, V>(
        &self,
        factories: &AsofJoinFactories<TS, I1, I2, OrdZSet<V>>,
        other: &Stream<RootCircuit, I2>,
        ts_func1: Box<dyn Fn(&I1::Val, &mut TS)>,
        ts_func2: Box<dyn Fn(&I2::Val, &mut TS)>,
        tscmp_func: Box<dyn Fn(&I1::Val, &I2::Val) -> Ordering>,
        valts_cmp_func: Box<dyn Fn(&I1::Val, &TS) -> Ordering>,
        join_func: Box<AsofJoinFunc<I1::Key, I1::Val, I2::Val, V, DynUnit>>,
    ) -> Stream<RootCircuit, OrdZSet<V>>
    where
        TS: DataTrait + ?Sized,
        I2: IndexedZSet<Key = I1::Key>,
        V: DataTrait + ?Sized,
    {
        self.dyn_asof_join_generic(
            factories,
            other,
            ts_func1,
            ts_func2,
            tscmp_func,
            valts_cmp_func,
            join_func,
        )
    }

    /// See [`Stream::asof_join_index`].
    #[track_caller]
    pub fn dyn_asof_join_index<TS, I2, K, V>(
        &self,
        factories: &AsofJoinFactories<TS, I1, I2, OrdIndexedZSet<K, V>>,
        other: &Stream<RootCircuit, I2>,
        ts_func1: Box<dyn Fn(&I1::Val, &mut TS)>,
        ts_func2: Box<dyn Fn(&I2::Val, &mut TS)>,
        tscmp_func: Box<dyn Fn(&I1::Val, &I2::Val) -> Ordering>,
        valts_cmp_func: Box<dyn Fn(&I1::Val, &TS) -> Ordering>,
        join_func: Box<AsofJoinFunc<I1::Key, I1::Val, I2::Val, K, V>>,
    ) -> Stream<RootCircuit, OrdIndexedZSet<K, V>>
    where
        TS: DataTrait + ?Sized,
        I2: IndexedZSet<Key = I1::Key>,
        K: DataTrait + ?Sized,
        V: DataTrait + ?Sized,
    {
        self.dyn_asof_join_generic(
            factories,
            other,
            ts_func1,
            ts_func2,
            tscmp_func,
            valts_cmp_func,
            join_func,
        )
    }

    /// Like [`Self::dyn_asof_join_index`], but can return any indexed Z-set type.
    #[track_caller]
    pub fn dyn_asof_join_generic<TS, I2, Z>(
        &self,
        factories: &AsofJoinFactories<TS, I1, I2, Z>,
        other: &Stream<RootCircuit, I2>,
        ts_func1: Box<dyn Fn(&I1::Val, &mut TS)>,
        ts_func2: Box<dyn Fn(&I2::Val, &mut TS)>,
        tscmp_func: Box<dyn Fn(&I1::Val, &I2::Val) -> Ordering>,
        valts_cmp_func: Box<dyn Fn(&I1::Val, &TS) -> Ordering>,
        join_func: Box<AsofJoinFunc<I1::Key, I1::Val, I2::Val, Z::Key, Z::Val>>,
    ) -> Stream<RootCircuit, Z>
    where
        TS: DataTrait + ?Sized,
        I2: IndexedZSet<Key = I1::Key>,
        Z: IndexedZSet,
    {
        self.circuit().region("asof_join", || {
            let left = self.dyn_shard(&factories.left_factories);
            let right = other.dyn_shard(&factories.right_factories);

            let left_trace = left
                .dyn_integrate_trace(&factories.left_factories)
                .delay_trace();
            let right_trace = right
                .dyn_integrate_trace(&factories.right_factories)
                .delay_trace();

            self.circuit().add_quaternary_operator(
                AsofJoin::new(
                    factories.clone(),
                    ts_func1,
                    tscmp_func,
                    valts_cmp_func,
                    join_func,
                    Location::caller(),
                ),
                &left,
                &left_trace,
                &right,
                &right_trace,
            )
        })
    }
}

pub type AsofJoinFunc<K, V1, V2, OK, OV> =
    dyn Fn(&K, &V1, Option<&V2>, &dyn FnMut(&mut OK, &mut OV));

pub struct AsofJoin<TS, I1, T1, I2, T2, Z>
where
    TS: DataTrait + ?Sized,
    I1: IndexedZSet,
    T1: ZTrace,
    I2: IndexedZSet,
    T2: ZTrace,
    Z: IndexedZSet,
{
    factories: AsofJoinFactories<TS, I1, I2, Z>,
    ts_func1: Box<dyn Fn(&I1::Val, &mut TS)>,
    tscmp_func: Box<dyn Fn(&I1::Val, &I2::Val) -> Ordering>,
    valts_cmp_func: Box<dyn Fn(&I1::Val, &TS) -> Ordering>,
    join_func: Box<AsofJoinFunc<I1::Key, I1::Val, I2::Val, Z::Key, Z::Val>>,
    location: &'static Location<'static>,
    ts: Box<TS>,
    phantom: PhantomData<(I1, T1, I2, T2, Z)>,
}

impl<TS, I1, T1, I2, T2, Z> AsofJoin<TS, I1, T1, I2, T2, Z>
where
    TS: DataTrait + ?Sized,
    I1: IndexedZSet,
    T1: ZTrace,
    I2: IndexedZSet,
    T2: ZTrace,
    Z: IndexedZSet,
{
    pub fn new(
        factories: AsofJoinFactories<TS, I1, I2, Z>,
        ts_func1: Box<dyn Fn(&I1::Val, &mut TS)>,
        tscmp_func: Box<dyn Fn(&I1::Val, &I2::Val) -> Ordering>,
        valts_cmp_func: Box<dyn Fn(&I1::Val, &TS) -> Ordering>,
        join_func: Box<AsofJoinFunc<I1::Key, I1::Val, I2::Val, Z::Key, Z::Val>>,
        location: &'static Location<'static>,
    ) -> Self {
        let ts = factories.timestamp_factory.default_box();

        Self {
            factories,
            ts_func1,
            tscmp_func,
            valts_cmp_func,
            join_func,
            location,
            ts,
            phantom: PhantomData,
        }
    }

    fn compute_affected_times<DC1, C2>(
        &mut self,
        delta1: &mut I1::Cursor<'_>,
        delta2: &mut I2::Cursor<'_>,
        delayed_cursor1: &mut DC1,
        cursor2: &mut C2,
        affected_times: &mut DynVec<TS>,
    ) where
        DC1: ZCursor<I1::Key, I1::Val, ()>,
        C2: ZCursor<I2::Key, I2::Val, ()>,
    {
        affected_times.clear();

        // Update all timestamps in `delta1`.
        while delta1.val_valid() {
            affected_times.push_with(&mut |ts| (self.ts_func1)(delta1.val(), ts));
            delta1.step_val();
        }

        debug_assert!(affected_times.is_sorted_by(&|ts1, ts2| ts1.cmp(ts2)));

        while delta2.val_valid() {
            delayed_cursor1
                .seek_val_with(&|v| (self.tscmp_func)(v, delta2.val()) != Ordering::Less);

            cursor2.seek_val_with(&|v| v > delta2.val());

            while delayed_cursor1.val_valid()
                && (!cursor2.val_valid()
                    || (self.tscmp_func)(delayed_cursor1.val(), cursor2.val()) == Ordering::Less)
            {
                affected_times.push_with(&mut |ts| (self.ts_func1)(delayed_cursor1.val(), ts));
                delayed_cursor1.step_val();
            }

            if !cursor2.val_valid() {
                break;
            }
            if !delayed_cursor1.val_valid() {
                break;
            }
            delta2.seek_val(cursor2.val());
        }

        affected_times.sort();
        affected_times.dedup();
    }

    fn eval_val<C1, C2>(
        &mut self,
        ts: &TS,
        cursor1: &mut C1,
        cursor2: &mut C2,
        multiplier: ZWeight,
        output_tuples: &mut DynWeightedPairs<DynPair<Z::Key, Z::Val>, DynZWeight>,
    ) where
        C1: ZCursor<I1::Key, I1::Val, ()>,
        C2: ZCursor<I2::Key, I2::Val, ()>,
    {
        cursor1.seek_val_with_reverse(&|v| (self.valts_cmp_func)(v, ts) != Ordering::Less);

        if !cursor1.val_valid() || ts != self.ts.as_ref() {
            return;
        }

        cursor2
            .seek_val_with_reverse(&|v| (self.tscmp_func)(cursor1.val(), v) != Ordering::Greater);

        let w1 = **cursor1.weight();
        let w2 = if cursor2.val_valid() {
            **cursor2.weight()
        } else {
            1
        };

        let w = w1 * w2 * multiplier;

        (self.join_func)(cursor1.key(), cursor1.val(), cursor2.get_val(), &|k, v| {
            output_tuples.push_with(&mut move |tup| {
                let (kv, neww) = tup.split_mut();
                let (newk, newv) = kv.split_mut();
                k.move_to(newk);
                v.move_to(newv);
                *unsafe { neww.downcast_mut() } = w;
            });
        });
    }

    fn eval_key<DC1, DC2, C1, C2>(
        &mut self,
        delta1: &mut I1::Cursor<'_>,
        delta2: &mut I2::Cursor<'_>,
        delayed_cursor1: &mut DC1,
        delayed_cursor2: &mut DC2,
        cursor1: &mut C1,
        cursor2: &mut C2,
        affected_times: &mut DynVec<TS>,
        output_tuples: &mut DynWeightedPairs<DynPair<Z::Key, Z::Val>, DynZWeight>,
    ) where
        DC1: ZCursor<I1::Key, I1::Val, ()>,
        DC2: ZCursor<I2::Key, I2::Val, ()>,
        C1: ZCursor<I1::Key, I1::Val, ()>,
        C2: ZCursor<I2::Key, I2::Val, ()>,
    {
        self.compute_affected_times(delta1, delta2, delayed_cursor1, cursor2, affected_times);

        cursor1.fast_forward_vals();
        cursor2.fast_forward_vals();
        delayed_cursor1.fast_forward_vals();
        delayed_cursor2.fast_forward_vals();

        for i in (0..affected_times.len()).rev() {
            let ts = unsafe { affected_times.index_unchecked(i) };

            // Retract old values.
            self.eval_val(ts, delayed_cursor1, delayed_cursor2, -1, output_tuples);

            // Insert new values.
            self.eval_val(ts, cursor1, cursor2, 1, output_tuples);
        }

        // Affected values:
        // - All values in delta1.
        // - For each value in delta2, all following values in delayed_trace1
        //   until the next timestamp present in delayed_trace2.

        // For each affected value x
        // - Find x in delayed_trace1: w1.
        // - Find matching value y, in delayed_trace2: w2.
        // - Retract: f(x, y) with weight -w1*w2.
        // - Find x in trace1: w1.
        // - Find matching value y in trace2: w2.
        // - Insert f(x, y) with weight w1*w2.
    }
}

impl<TS, I1, T1, I2, T2, Z> Operator for AsofJoin<TS, I1, T1, I2, T2, Z>
where
    TS: DataTrait + ?Sized,
    I1: IndexedZSet,
    T1: ZTrace,
    I2: IndexedZSet,
    T2: ZTrace,
    Z: IndexedZSet,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("AsofJoin")
    }

    fn location(&self) -> OperatorLocation {
        Some(self.location)
    }

    /*fn metadata(&self, meta: &mut OperatorMeta) {
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
    }*/

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<TS, I1, T1, I2, T2, Z> QuaternaryOperator<I1, T1, I2, T2, Z>
    for AsofJoin<TS, I1, T1, I2, T2, Z>
where
    TS: DataTrait + ?Sized,
    I1: IndexedZSet,
    T1: ZTrace<Key = I1::Key, Val = I1::Val, Time = ()> + Clone,
    I2: IndexedZSet<Key = I1::Key>,
    T2: ZTrace<Key = I2::Key, Val = I2::Val, Time = ()> + Clone,
    Z: IndexedZSet,
{
    fn eval<'a>(
        &mut self,
        delta1: Cow<'a, I1>,
        delayed_trace1: Cow<'a, T1>,
        delta2: Cow<'a, I2>,
        delayed_trace2: Cow<'a, T2>,
    ) -> Z {
        let mut delta1_cursor = delta1.cursor();
        let mut delta2_cursor = delta2.cursor();

        let mut delayed_trace1_cursor = delayed_trace1.cursor();
        let mut delayed_trace2_cursor = delayed_trace2.cursor();

        let mut trace1_cursor = CursorPair::new(&mut delta1_cursor, &mut delayed_trace1_cursor);
        let mut trace2_cursor = CursorPair::new(&mut delta2_cursor, &mut delayed_trace2_cursor);

        let mut delta1_cursor = delta1.cursor();
        let mut delta2_cursor = delta2.cursor();

        let mut delayed_trace1_cursor = delayed_trace1.cursor();
        let mut delayed_trace2_cursor = delayed_trace2.cursor();

        let mut output_tuples = self
            .factories
            .output_factories
            .weighted_items_factory()
            .default_box();

        // Timestamps that need to be recomputed, kept here for allocation reuse.
        let mut affected_times = self.factories.timestamps_factory.default_box();

        // Iterate over keys in delta1 and delta2.
        while delta1_cursor.key_valid() && delta2_cursor.key_valid() {
            match delta1_cursor.key().cmp(delta2_cursor.key()) {
                Ordering::Less => {
                    self.eval_key(
                        &mut delta1_cursor,
                        &mut delta2_cursor,
                        &mut delayed_trace1_cursor,
                        &mut delayed_trace2_cursor,
                        &mut trace1_cursor,
                        &mut trace2_cursor,
                        affected_times.as_mut(),
                        output_tuples.as_mut(),
                    );
                    delta1_cursor.step_key();
                }
                Ordering::Equal => {
                    self.eval_key(
                        &mut delta1_cursor,
                        &mut delta2_cursor,
                        &mut delayed_trace1_cursor,
                        &mut delayed_trace2_cursor,
                        &mut trace1_cursor,
                        &mut trace2_cursor,
                        affected_times.as_mut(),
                        output_tuples.as_mut(),
                    );
                    delta1_cursor.step_key();
                    delta2_cursor.step_key();
                }
                Ordering::Greater => {
                    self.eval_key(
                        &mut delta1_cursor,
                        &mut delta2_cursor,
                        &mut delayed_trace1_cursor,
                        &mut delayed_trace2_cursor,
                        &mut trace1_cursor,
                        &mut trace2_cursor,
                        affected_times.as_mut(),
                        output_tuples.as_mut(),
                    );
                    delta2_cursor.step_key();
                }
            }
        }

        while delta1_cursor.key_valid() {
            self.eval_key(
                &mut delta1_cursor,
                &mut delta2_cursor,
                &mut delayed_trace1_cursor,
                &mut delayed_trace2_cursor,
                &mut trace1_cursor,
                &mut trace2_cursor,
                affected_times.as_mut(),
                output_tuples.as_mut(),
            );
            delta1_cursor.step_key();
        }

        while delta2_cursor.key_valid() {
            self.eval_key(
                &mut delta1_cursor,
                &mut delta2_cursor,
                &mut delayed_trace1_cursor,
                &mut delayed_trace2_cursor,
                &mut trace1_cursor,
                &mut trace2_cursor,
                affected_times.as_mut(),
                output_tuples.as_mut(),
            );
            delta2_cursor.step_key();
        }

        Z::dyn_from_tuples(&self.factories.output_factories, (), &mut output_tuples)
    }
}
