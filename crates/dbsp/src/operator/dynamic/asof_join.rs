use std::{borrow::Cow, cmp::Ordering, marker::PhantomData, panic::Location};

use crate::{
    algebra::{IndexedZSet, IndexedZSetReader, OrdIndexedZSet, OrdZSet, ZBatchReader, ZCursor},
    circuit::{
        metadata::OperatorLocation,
        operator_traits::{Operator, QuaternaryOperator},
    },
    dynamic::{
        ClonableTrait, DataTrait, DowncastTrait, DynData, DynPair, DynUnit, DynVec,
        DynWeightedPairs, Erase, Factory, LeanVec, WeightTrait, WithFactory,
    },
    trace::{
        cursor::{CursorEmpty, CursorPair},
        BatchFactories, BatchReaderFactories, Cursor,
    },
    Circuit, DBData, DynZWeight, RootCircuit, Scope, Stream, ZWeight,
};

use super::{MonoIndexedZSet, MonoZSet};

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

impl Stream<RootCircuit, MonoIndexedZSet> {
    #[track_caller]
    pub fn dyn_asof_join_mono(
        &self,
        factories: &AsofJoinFactories<DynData, MonoIndexedZSet, MonoIndexedZSet, MonoZSet>,
        other: &Stream<RootCircuit, MonoIndexedZSet>,
        ts_func1: Box<dyn Fn(&DynData, &mut DynData)>,
        tscmp_func: Box<dyn Fn(&DynData, &DynData) -> Ordering>,
        valts_cmp_func: Box<dyn Fn(&DynData, &DynData) -> Ordering>,
        join_func: Box<AsofJoinFunc<DynData, DynData, DynData, DynData, DynUnit>>,
    ) -> Stream<RootCircuit, MonoZSet> {
        self.dyn_asof_join(
            factories,
            other,
            ts_func1,
            tscmp_func,
            valts_cmp_func,
            join_func,
        )
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
            tscmp_func,
            valts_cmp_func,
            join_func,
        )
    }

    #[track_caller]
    pub fn dyn_asof_join_index<TS, I2, K, V>(
        &self,
        factories: &AsofJoinFactories<TS, I1, I2, OrdIndexedZSet<K, V>>,
        other: &Stream<RootCircuit, I2>,
        ts_func1: Box<dyn Fn(&I1::Val, &mut TS)>,
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
    dyn Fn(&K, &V1, Option<&V2>, &mut dyn FnMut(&mut OK, &mut OV));

pub struct AsofJoin<TS, I1, T1, I2, T2, Z>
where
    TS: DataTrait + ?Sized,
    I1: IndexedZSet,
    T1: ZBatchReader,
    I2: IndexedZSet,
    T2: ZBatchReader,
    Z: IndexedZSet,
{
    factories: AsofJoinFactories<TS, I1, I2, Z>,
    ts_func1: Box<dyn Fn(&I1::Val, &mut TS)>,
    tscmp_func: Box<dyn Fn(&I1::Val, &I2::Val) -> Ordering>,
    valts_cmp_func: Box<dyn Fn(&I1::Val, &TS) -> Ordering>,
    join_func: Box<AsofJoinFunc<I1::Key, I1::Val, I2::Val, Z::Key, Z::Val>>,
    location: &'static Location<'static>,
    phantom: PhantomData<(I1, T1, I2, T2, Z)>,
}

impl<TS, I1, T1, I2, T2, Z> AsofJoin<TS, I1, T1, I2, T2, Z>
where
    TS: DataTrait + ?Sized,
    I1: IndexedZSet,
    T1: ZBatchReader,
    I2: IndexedZSet<Key = I1::Key>,
    T2: ZBatchReader,
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
        Self {
            factories,
            ts_func1,
            tscmp_func,
            valts_cmp_func,
            join_func,
            location,
            phantom: PhantomData,
        }
    }

    /// Seek to a key; return `None` if exact match was not found.
    fn try_seek<'a, C, K, V, T, R>(cursor: &'a mut C, key: &K) -> Option<&'a mut C>
    where
        K: DataTrait + ?Sized,
        V: DataTrait + ?Sized,
        R: WeightTrait + ?Sized,
        C: Cursor<K, V, T, R>,
    {
        cursor.seek_key(key);
        if cursor.get_key() == Some(key) {
            Some(cursor)
        } else {
            None
        }
    }

    /// Compute all timestamps affected by the changes.  We will
    /// update the value of the asof-join for these timestamps.
    fn compute_affected_times<DC1, DC2, ZC1, C2>(
        &mut self,
        delta1: &mut DC1,
        delta2: &mut DC2,
        delayed_cursor1: &mut Option<&mut ZC1>,
        cursor2: &mut C2,
        affected_times: &mut DynVec<TS>,
    ) where
        DC1: ZCursor<I1::Key, I1::Val, ()>,
        DC2: ZCursor<I2::Key, I2::Val, ()>,
        ZC1: ZCursor<I1::Key, I1::Val, ()>,
        C2: ZCursor<I2::Key, I2::Val, ()>,
    {
        affected_times.clear();

        // Update all timestamps in `delta1`.
        while delta1.val_valid() {
            affected_times.push_with(&mut |ts| (self.ts_func1)(delta1.val(), ts));
            delta1.step_val();
        }

        debug_assert!(affected_times.is_sorted_by(&|ts1, ts2| ts1.cmp(ts2)));

        // For `delta2`, we want to determine all timestamps in `delayed_cursor1`
        // that could potentially be affected by changes in `delta2` (we don't care
        // about timestamps in `cursor1` that are not in `delayed_cursor1`, since
        // they are already accounted for in `delta1`).
        //
        // For each value in `delta2`, this includes timestamps in `delayed_cursor1`
        // between the `delta2.val` (inclusive) and the next bigger value in `cursor2`
        // (exclusive).
        if let Some(delayed_cursor1) = delayed_cursor1 {
            while delta2.val_valid() {
                // Find the first value in `delayed_cursor1` following the current
                // timestamp in `delta2`.
                delayed_cursor1
                    .seek_val_with(&|v| (self.tscmp_func)(v, delta2.val()) != Ordering::Less);

                // Find the next timestamp in `cursor2` with non-zero weight.
                cursor2.seek_val_with(&|v| v > delta2.val());
                while cursor2.val_valid() && **cursor2.weight() == 0 {
                    cursor2.step_val()
                }

                // Enumerate all timestamps in `delayed_cursor1` preceding the current
                // position of `cursor2`.
                while delayed_cursor1.val_valid()
                    && (!cursor2.val_valid()
                        || (self.tscmp_func)(delayed_cursor1.val(), cursor2.val())
                            == Ordering::Less)
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
        }

        affected_times.dedup();
    }

    /// Compute asof-join for the given timestamp.
    ///
    /// Assumes that, if `ts` is present in `cursor1`, then `cursor1` can reverse-seek
    /// to it (i.e., it hasn't passed the timestamp yet).  Assumes the same for `cursor2`.
    ///
    /// By setting `multiplier` to +1 or -1, we get this function to produce
    /// insertions and retractions respectively.
    fn eval_val<C1, C2>(
        &mut self,
        ts: &TS,
        cursor1: &mut Option<&mut C1>,
        cursor2: &mut C2,
        multiplier: ZWeight,
        output_tuples: &mut DynWeightedPairs<DynPair<Z::Key, Z::Val>, DynZWeight>,
    ) where
        C1: ZCursor<I1::Key, I1::Val, ()>,
        C2: ZCursor<I2::Key, I2::Val, ()>,
    {
        let Some(cursor1) = cursor1 else {
            return;
        };

        cursor1.seek_val_with_reverse(&|v| (self.valts_cmp_func)(v, ts) != Ordering::Greater);

        // if !cursor1.val_valid() {
        //     println!("timestamp not found");
        // } else if (self.valts_cmp_func)(cursor1.val(), ts) != Ordering::Equal {
        //     println!("missed timestamp, current value: {:?}", cursor1.val());
        // }

        // Iterate over all values with the same timestamp in `cursor1`.
        while cursor1.val_valid() && (self.valts_cmp_func)(cursor1.val(), ts) == Ordering::Equal {
            // println!("Processing value {:?}", cursor1.val());

            cursor2
                .seek_val_with_reverse(&|v| (self.tscmp_func)(cursor1.val(), v) != Ordering::Less);

            // Ignore zero weights.
            while cursor2.val_valid() && **cursor2.weight() == 0 {
                cursor2.step_val_reverse();
            }

            // The weight of the result is the product of input weights.
            // If there is no matching RHS value, then asof-join behaves like
            // the left join: we pass `None` to the join function with weight 1.
            let w1 = **cursor1.weight();
            let w2 = if cursor2.val_valid() {
                **cursor2.weight()
            } else {
                1
            };

            let w = w1 * w2 * multiplier;

            (self.join_func)(
                cursor1.key(),
                cursor1.val(),
                cursor2.get_val(),
                &mut |k, v| {
                    // println!("  output: k={k:?}, w={w}");
                    output_tuples.push_with(&mut move |tup| {
                        let (kv, neww) = tup.split_mut();
                        let (newk, newv) = kv.split_mut();
                        k.move_to(newk);
                        v.move_to(newv);
                        *unsafe { neww.downcast_mut() } = w;
                    });
                },
            );

            cursor1.step_val_reverse();
        }
    }

    /// Evaluate operator for the current key.
    #[allow(clippy::too_many_arguments)]
    fn eval_key<DC1, DC2, ZC1, ZC2, C1, C2>(
        &mut self,
        delta1: &mut DC1,
        delta2: &mut DC2,
        delayed_cursor1: &mut ZC1,
        delayed_cursor2: &mut ZC2,
        cursor1: &mut C1,
        cursor2: &mut C2,
        affected_times: &mut DynVec<TS>,
        output_tuples: &mut DynWeightedPairs<DynPair<Z::Key, Z::Val>, DynZWeight>,
    ) where
        DC1: ZCursor<I1::Key, I1::Val, ()>,
        DC2: ZCursor<I2::Key, I2::Val, ()>,
        ZC1: ZCursor<I1::Key, I1::Val, ()>,
        ZC2: ZCursor<I2::Key, I2::Val, ()>,
        C1: ZCursor<I1::Key, I1::Val, ()>,
        C2: ZCursor<I2::Key, I2::Val, ()>,
    {
        let key = if delta1.key_valid() {
            delta1.key()
        } else {
            delta2.key()
        };

        // println!("key: {key:?}");

        // Make sure that all cursors point to the same key or are None.
        let mut delayed_cursor1 = Self::try_seek(delayed_cursor1, key);
        let mut delayed_cursor2 = Self::try_seek(delayed_cursor2, key);
        let mut cursor1 = Self::try_seek(cursor1, key);
        let mut cursor2 = Self::try_seek(cursor2, key);

        let mut empty_cursor = CursorEmpty::new(WithFactory::<ZWeight>::FACTORY);

        if let Some(cursor2) = &mut cursor2 {
            self.compute_affected_times(
                delta1,
                delta2,
                &mut delayed_cursor1,
                *cursor2,
                affected_times,
            );
        } else {
            self.compute_affected_times(
                delta1,
                delta2,
                &mut delayed_cursor1,
                &mut empty_cursor,
                affected_times,
            );
        }

        // println!("Affected times: {affected_times:?}");

        // We iterate in reverse when computing asof join, because we
        // cannot use forward iteration to get cursor2 to stop at the last
        // value <=cursor1.
        if let Some(c) = cursor1.as_mut() {
            c.fast_forward_vals()
        }
        if let Some(c) = cursor2.as_mut() {
            c.fast_forward_vals()
        }
        if let Some(c) = delayed_cursor1.as_mut() {
            c.fast_forward_vals()
        }
        if let Some(c) = delayed_cursor2.as_mut() {
            c.fast_forward_vals()
        }

        // if let Some(delayed_cursor1) = &mut delayed_cursor1 {
        //     println!("Enumerating previous values:");
        //     while delayed_cursor1.val_valid() {
        //         println!("  {:?}", delayed_cursor1.val());
        //         delayed_cursor1.step_val_reverse();
        //     }
        //     delayed_cursor1.fast_forward_vals();
        // }

        for i in (0..affected_times.len()).rev() {
            let ts = unsafe { affected_times.index_unchecked(i) };
            // println!("Processing timestamp {ts:?}");

            // Retract old values.
            if let Some(delayed_cursor2) = &mut delayed_cursor2 {
                self.eval_val(
                    ts,
                    &mut delayed_cursor1,
                    *delayed_cursor2,
                    -1,
                    output_tuples,
                );
            } else {
                self.eval_val(
                    ts,
                    &mut delayed_cursor1,
                    &mut empty_cursor,
                    -1,
                    output_tuples,
                );
            }

            // Insert new values.
            if let Some(cursor2) = &mut cursor2 {
                self.eval_val(ts, &mut cursor1, *cursor2, 1, output_tuples);
            } else {
                self.eval_val(ts, &mut cursor1, &mut empty_cursor, 1, output_tuples);
            }
        }
    }
}

impl<TS, I1, T1, I2, T2, Z> Operator for AsofJoin<TS, I1, T1, I2, T2, Z>
where
    TS: DataTrait + ?Sized,
    I1: IndexedZSet,
    T1: ZBatchReader,
    I2: IndexedZSet,
    T2: ZBatchReader,
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
    T1: ZBatchReader<Key = I1::Key, Val = I1::Val, Time = ()> + Clone,
    I2: IndexedZSet<Key = I1::Key>,
    T2: ZBatchReader<Key = I2::Key, Val = I2::Val, Time = ()> + Clone,
    Z: IndexedZSet,
{
    async fn eval(
        &mut self,
        delta1: Cow<'_, I1>,
        delayed_trace1: Cow<'_, T1>,
        delta2: Cow<'_, I2>,
        delayed_trace2: Cow<'_, T2>,
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

        // Timestamps that need to be recomputed for each key, created here for allocation
        // reuse across keys.
        let mut affected_times = self.factories.timestamps_factory.default_box();

        // Iterate over keys in delta1 and delta2.
        while delta1_cursor.key_valid() && delta2_cursor.key_valid() {
            match delta1_cursor.key().cmp(delta2_cursor.key()) {
                Ordering::Less => {
                    self.eval_key(
                        &mut delta1_cursor,
                        &mut CursorEmpty::new(WithFactory::<ZWeight>::FACTORY),
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
                        &mut CursorEmpty::new(WithFactory::<ZWeight>::FACTORY),
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
                &mut CursorEmpty::new(WithFactory::<ZWeight>::FACTORY),
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
                &mut CursorEmpty::new(WithFactory::<ZWeight>::FACTORY),
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

#[cfg(test)]
mod test {
    use crate::{
        algebra::F32,
        circuit::CircuitConfig,
        utils::{Tup2, Tup3, Tup4},
        zset, DBData, DBSPHandle, OrdIndexedZSet, OrdZSet, OutputHandle, Runtime, ZSetHandle,
        ZWeight,
    };
    use proptest::{collection::vec, prelude::*};

    type Time = u64;
    type CCNum = u64;
    type Amt = F32;
    type Transaction = Tup3<Time, CCNum, Amt>;
    type User = Tup3<Time, CCNum, String>;
    type Output = Tup4<Time, CCNum, Amt, Option<String>>;

    fn test_circuit() -> (
        DBSPHandle,
        (
            ZSetHandle<Transaction>,
            ZSetHandle<User>,
            OutputHandle<OrdZSet<Output>>,
        ),
    ) {
        Runtime::init_circuit(CircuitConfig::with_workers(2), |circuit| {
            let (transactions, transactions_handle) = circuit.add_input_zset::<Transaction>();
            let (users, users_handle) = circuit.add_input_zset::<User>();

            let transactions = transactions.map_index(|transaction| (transaction.1, *transaction));
            let users = users.map_index(|user| (user.1, user.clone()));

            let join = |_key: &CCNum, transaction: &Transaction, user: Option<&User>| {
                Tup4(
                    transaction.0,
                    transaction.1,
                    transaction.2,
                    user.map(|u| u.2.clone()),
                )
            };
            let ts_func1 = |transaction: &Transaction| transaction.0;
            let ts_func2 = |user: &User| user.0;

            let result = transactions.asof_join(&users, join, ts_func1, ts_func2);

            let expected_result = transactions
                .shard()
                .integrate()
                .apply2(&users.shard().integrate(), move |t, u| {
                    asof_join_reference(t, u, join, ts_func1, ts_func2)
                });

            result
                .integrate()
                .apply2(&expected_result, |actual, expected| {
                    assert_eq!(actual, expected)
                });

            let output_handle = result.output();

            Ok((transactions_handle, users_handle, output_handle))
        })
        .unwrap()
    }

    #[test]
    fn asof_join_test() {
        let (mut dbsp, (transactions, users, result)) = test_circuit();

        // Step 1. Add some transactions without users.
        transactions.append(&mut vec![
            // CCNum = 1
            Tup2(Tup3(100, 1, F32::new(10.0)), 1),
            Tup2(Tup3(200, 1, F32::new(10.0)), 1),
            Tup2(Tup3(300, 1, F32::new(10.0)), 1),
            // CCNum = 2
            Tup2(Tup3(100, 2, F32::new(20.0)), 1),
            // CCNum = 3
            Tup2(Tup3(100, 3, F32::new(30.0)), 1),
        ]);
        dbsp.step().unwrap();

        assert_eq!(
            result.consolidate(),
            zset! {
               Tup4(100, 1, F32::new(10.0), None) => 1,
               Tup4(200, 1, F32::new(10.0), None) => 1,
               Tup4(300, 1, F32::new(10.0), None) => 1,
               Tup4(100, 2, F32::new(20.0), None) => 1,
               Tup4(100, 3, F32::new(30.0), None) => 1,
            }
        );

        // Step 2. Add matching users.
        users.append(&mut vec![
            // CCNum = 1
            Tup2(Tup3(50, 1, "A50".to_string()), 1),
            // CCNum = 2
            Tup2(Tup3(100, 2, "B100".to_string()), 1),
            // CCNum = 3
            Tup2(Tup3(110, 3, "C110".to_string()), 1),
        ]);
        dbsp.step().unwrap();

        assert_eq!(
            result.consolidate(),
            zset! {
                Tup4(100, 1, F32::new(10.0), None) => -1,
                Tup4(200, 1, F32::new(10.0), None) => -1,
                Tup4(300, 1, F32::new(10.0), None) => -1,
                Tup4(100, 2, F32::new(20.0), None) => -1,
                Tup4(100, 1, F32::new(10.0), Some("A50".to_string())) => 1,
                Tup4(200, 1, F32::new(10.0), Some("A50".to_string())) => 1,
                Tup4(300, 1, F32::new(10.0), Some("A50".to_string())) => 1,
                Tup4(100, 2, F32::new(20.0), Some("B100".to_string())) => 1,
            }
        );

        // Step 3.
        // - Add a more recent version of user 1.
        // - Add a "future" version of user 2.
        // - Add an older version of user 3.
        users.append(&mut vec![
            // CCNum = 1
            Tup2(Tup3(60, 1, "A60".to_string()), 1),
            Tup2(Tup3(120, 2, "B120".to_string()), 1),
            Tup2(Tup3(50, 3, "C50".to_string()), 1),
        ]);

        transactions.append(&mut vec![Tup2(Tup3(200, 3, F32::new(30.0)), 1)]);

        dbsp.step().unwrap();

        assert_eq!(
            result.consolidate(),
            zset! {
                Tup4(100, 1, F32::new(10.0), Some("A50".to_string())) => -1,
                Tup4(100, 1, F32::new(10.0), Some("A60".to_string())) => 1,
                Tup4(200, 1, F32::new(10.0), Some("A50".to_string())) => -1,
                Tup4(200, 1, F32::new(10.0), Some("A60".to_string())) => 1,
                Tup4(300, 1, F32::new(10.0), Some("A50".to_string())) => -1,
                Tup4(300, 1, F32::new(10.0), Some("A60".to_string())) => 1,
                Tup4(100, 3, F32::new(30.0), None) => -1,
                Tup4(100, 3, F32::new(30.0), Some("C50".to_string())) => 1,
                Tup4(200, 3, F32::new(30.0), Some("C110".to_string())) => 1,
            }
        );

        // Step 4. Add users with old timestamps.
        users.append(&mut vec![
            // CCNum = 1
            Tup2(Tup3(10, 1, "A10".to_string()), 1),
            Tup2(Tup3(10, 2, "B10".to_string()), 1),
            Tup2(Tup3(10, 3, "C10".to_string()), 1),
            Tup2(Tup3(110, 3, "C105".to_string()), 1),
        ]);

        dbsp.step().unwrap();

        assert_eq!(result.consolidate(), zset! {});

        // Step 5. Add multiple transactions per timestamp.
        transactions.append(&mut vec![
            // CCNum = 1
            Tup2(Tup3(100, 1, F32::new(100.0)), 1),
            Tup2(Tup3(200, 1, F32::new(100.0)), 1),
            Tup2(Tup3(300, 1, F32::new(100.0)), 1),
            // CCNum = 2
            Tup2(Tup3(100, 2, F32::new(200.0)), 1),
            // CCNum = 3
            Tup2(Tup3(100, 3, F32::new(300.0)), 1),
        ]);
        dbsp.step().unwrap();

        assert_eq!(
            result.consolidate(),
            zset! {
                Tup4(100, 1, F32::new(100.0), Some("A60".to_string())) => 1,
                Tup4(200, 1, F32::new(100.0), Some("A60".to_string())) => 1,
                Tup4(300, 1, F32::new(100.0), Some("A60".to_string())) => 1,
                Tup4(100, 2, F32::new(200.0), Some("B100".to_string())) => 1,
                Tup4(100, 3, F32::new(300.0), Some("C50".to_string())) => 1,
            }
        );

        // Step 6. Delete users.
        users.append(&mut vec![
            Tup2(Tup3(10, 1, "A10".to_string()), -1),
            Tup2(Tup3(10, 2, "B10".to_string()), -1),
            Tup2(Tup3(10, 3, "C10".to_string()), -1),
        ]);

        dbsp.step().unwrap();

        assert_eq!(result.consolidate(), zset! {});

        // Step 7. Delete more users.
        users.append(&mut vec![
            // CCNum = 1
            // Tup2(Tup3(50, 1, "A50".to_string()), 1),
            Tup2(Tup3(60, 1, "A60".to_string()), -1),
            // CCNum = 2
            // Tup2(Tup3(100, 2, "B100".to_string()), 1),
            Tup2(Tup3(120, 2, "B120".to_string()), -1),
            // CCNum = 3
            // Tup2(Tup3(50, 3, "C50".to_string()), 1),
            // Tup2(Tup3(110, 3, "C105".to_string()), 1),
            Tup2(Tup3(110, 3, "C110".to_string()), -1),
        ]);

        dbsp.step().unwrap();

        assert_eq!(
            result.consolidate(),
            zset! {
                Tup4(100, 1, F32::new(100.0), Some("A60".to_string())) => -1,
                Tup4(200, 1, F32::new(100.0), Some("A60".to_string())) => -1,
                Tup4(300, 1, F32::new(100.0), Some("A60".to_string())) => -1,
                Tup4(100, 1, F32::new(10.0), Some("A60".to_string())) => -1,
                Tup4(200, 1, F32::new(10.0), Some("A60".to_string())) => -1,
                Tup4(300, 1, F32::new(10.0), Some("A60".to_string())) => -1,
                Tup4(100, 1, F32::new(100.0), Some("A50".to_string())) => 1,
                Tup4(200, 1, F32::new(100.0), Some("A50".to_string())) => 1,
                Tup4(300, 1, F32::new(100.0), Some("A50".to_string())) => 1,
                Tup4(100, 1, F32::new(10.0), Some("A50".to_string())) => 1,
                Tup4(200, 1, F32::new(10.0), Some("A50".to_string())) => 1,
                Tup4(300, 1, F32::new(10.0), Some("A50".to_string())) => 1,
                Tup4(200, 3, F32::new(30.0), Some("C110".to_string())) => -1,
                Tup4(200, 3, F32::new(30.0), Some("C105".to_string())) => 1,
            }
        );
    }

    #[test]
    fn asof_join_regressions() {
        let (mut dbsp, (transactions, users, _result)) = test_circuit();

        users.append(&mut vec![
            Tup2(Tup3(37, 0, "L".to_string()), 1),
            Tup2(Tup3(0, 0, "A".to_string()), 1),
        ]);
        dbsp.step().unwrap();

        transactions.append(&mut vec![Tup2(Tup3(37, 0, F32::new(0.0)), 1)]);
        dbsp.step().unwrap();

        users.append(&mut vec![Tup2(Tup3(37, 0, "L".to_string()), -1)]);
        dbsp.step().unwrap();

        users.append(&mut vec![Tup2(Tup3(0, 0, "A".to_string()), -1)]);
        dbsp.step().unwrap();
    }

    /// Reference implementaton of asof-join for testing.
    fn asof_join_reference<TS, F, TSF1, TSF2, K, V1, V2, OV>(
        left: &OrdIndexedZSet<K, V1>,
        right: &OrdIndexedZSet<K, V2>,
        join: F,
        ts_func1: TSF1,
        ts_func2: TSF2,
    ) -> OrdZSet<OV>
    where
        TS: DBData,
        K: DBData,
        V1: DBData,
        V2: DBData,
        OV: DBData,
        F: Fn(&K, &V1, Option<&V2>) -> OV + Clone + 'static,
        TSF1: Fn(&V1) -> TS + Clone + 'static,
        TSF2: Fn(&V2) -> TS + 'static,
    {
        let left = left.iter().collect::<Vec<_>>();
        let right = right.iter().collect::<Vec<_>>();

        let mut result = Vec::new();

        for (k, v1, w1) in left.iter() {
            let (ov, ow) = right
                .iter()
                .rev()
                .find_map(|(k2, v2, w2)| {
                    if k2 == k && ts_func2(v2) <= ts_func1(v1) {
                        Some((join(k, v1, Some(v2)), w1 * w2))
                    } else {
                        None
                    }
                })
                .unwrap_or_else(|| (join(k, v1, None), *w1));
            result.push(Tup2(ov, ow));
        }

        OrdZSet::from_keys((), result)
    }

    prop_compose! {
        fn transaction()
            (time in 0..100u64,
            cc_num in 0..10u64,
            amt in 0..100i32,
            w in 1..=2 as ZWeight)
            -> Tup2<Transaction, ZWeight> {
            Tup2(Tup3(time, cc_num, F32::new(amt as f32)), w)
        }
    }

    prop_compose! {
        fn user()
            (time in 0..100u64,
            cc_num in 0..5u64,
            name in "[A-Z][a-z]{5}",
            w in 1..=2 as ZWeight)
            -> Tup2<User, ZWeight> {
            Tup2(Tup3(time, cc_num, name), w)
        }
    }

    prop_compose! {
        fn input()
            (transactions in vec(transaction(), 0..20),
            users in vec(user(), 0..10))
            -> (Vec<Tup2<Transaction, ZWeight>>, Vec<Tup2<User, ZWeight>>) {
            (transactions, users)
        }
    }

    prop_compose! {
        fn inputs(steps: usize)
            (inputs in vec(input(), 0..=steps))
        -> Vec<(Vec<Tup2<Transaction,ZWeight>>, Vec<Tup2<User, ZWeight>>)> {
            inputs
        }
    }

    proptest! {
        #[test]
        fn asof_join_proptest(inputs in inputs(50)) {
            let (mut dbsp, (htransactions, husers, _hresult)) = test_circuit();

            let mut deletions = inputs.clone();
            for (ts, us) in deletions.iter_mut() {
                for Tup2(_t, w) in ts.iter_mut(){
                    *w = -*w;
                }
                for Tup2(_u, w) in us.iter_mut() {
                     *w = -*w;
                }
            }

            for (mut transactions, mut users) in inputs {
                htransactions.append(&mut transactions);
                husers.append(&mut users);

                dbsp.step().unwrap();
            }

            for (mut transactions, mut users) in deletions {
                htransactions.append(&mut transactions);
                husers.append(&mut users);

                dbsp.step().unwrap();
            }
        }
    }
}
