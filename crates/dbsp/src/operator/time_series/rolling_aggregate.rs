use crate::dynamic::Erase;
use crate::trace::BatchReaderFactories;
use crate::{
    algebra::MulByRef,
    dynamic::{DowncastTrait, DynData, DynDataTyped, DynOpt, DynPair, DynWeight},
    operator::{
        dynamic::{
            aggregate::DynAggregatorImpl,
            time_series::{
                PartitionedRollingAggregateFactories, PartitionedRollingAggregateLinearFactories,
                PartitionedRollingAggregateWithWaterlineFactories,
                PartitionedRollingAverageFactories, RelRange,
            },
        },
        Aggregator,
    },
    typed_batch::{DynOrdIndexedZSet, TypedBatch, TypedBox},
    utils::Tup2,
    DBData, DBWeight, OrdIndexedZSet, RootCircuit, Stream, ZWeight,
};
use num::PrimInt;
use std::{mem::take, ops::Div};

// TODO: This should be `OrdIndexedZSet<PK, (TS, V)>`.
pub type OrdPartitionedIndexedZSet<PK, TS, DynTS, V, DynV> =
    TypedBatch<PK, Tup2<TS, V>, ZWeight, DynOrdIndexedZSet<DynData, DynPair<DynTS, DynV>>>;

pub type OrdPartitionedOverStream<PK, TS, A> = Stream<
    RootCircuit,
    OrdPartitionedIndexedZSet<PK, TS, DynDataTyped<TS>, Option<A>, DynOpt<DynData>>,
>;

impl<TS, V>
    Stream<RootCircuit, TypedBatch<TS, V, ZWeight, DynOrdIndexedZSet<DynDataTyped<TS>, DynData>>>
where
    TS: DBData + PrimInt,
    V: DBData,
{
    /// Similar to
    /// [`partitioned_rolling_aggregate`](`Stream::partitioned_rolling_aggregate`),
    /// but uses `waterline` to bound its memory footprint.
    ///
    /// Splits the input stream into non-overlapping
    /// partitions using `partition_func` and for each input record
    /// computes an aggregate over a relative time range (e.g., the
    /// last three months) within its partition.  Outputs the contents
    /// of the input stream extended with the value of the aggregate.
    ///
    /// This operator is incremental and will update previously
    /// computed outputs affected by new data.  For example,
    /// a data point arriving out-of-order may affect previously
    /// computed rolling aggregate values at future times.
    ///
    /// The `waterline` stream bounds the out-of-ordedness of the input
    /// data by providing a monotonically growing lower bound on
    /// timestamps that can appear in the input stream.  The operator
    /// does not expect inputs with timestamps smaller than the current
    /// waterline.  The `waterline` value is used to bound the amount of
    /// state maintained by the operator.
    ///
    /// # Background
    ///
    /// The rolling aggregate operator is typically applied to time series data
    /// with bounded out-of-ordedness, i.e, having seen a timestamp `ts` in
    /// the input stream, the operator will never observe a timestamp
    /// smaller than `ts - b` for some bound `b`.  This in turn means that
    /// the value of the aggregate will remain constant for timestamps that
    /// only depend on times `< ts - b`.  Hence, we do not need to maintain
    /// the state needed to recompute these aggregates, which allows us to
    /// bound the amount of state maintained by this operator.
    ///
    /// The bound `ts - b` is known as "waterline" and can be computed, e.g., by
    /// the [`waterline_monotonic`](`Stream::waterline_monotonic`) operator.
    ///
    /// # Arguments
    ///
    /// * `self` - time series data indexed by time.
    /// * `waterline` - monotonically growing lower bound on timestamps in the
    ///   input stream.
    /// * `partition_func` - function used to split inputs into non-overlapping
    ///   partitions indexed by partition key of type `PK`.
    /// * `aggregator` - aggregator used to summarize values within the relative
    ///   time range `range` of each input timestamp.
    /// * `range` - relative time range to aggregate over.
    pub fn partitioned_rolling_aggregate_with_waterline<PK, OV, Agg, PF>(
        &self,
        waterline: &Stream<RootCircuit, TypedBox<TS, DynDataTyped<TS>>>,
        partition_func: PF,
        aggregator: Agg,
        range: RelRange<TS>,
    ) -> OrdPartitionedOverStream<PK, TS, Agg::Output>
    where
        PK: DBData,
        OV: DBData,
        Agg: Aggregator<OV, (), ZWeight>,
        PF: Fn(&V) -> (PK, OV) + Clone + 'static,
    {
        let factories = PartitionedRollingAggregateWithWaterlineFactories::<
            DynData,
            TS,
            DynData,
            DynData,
            DynData,
            DynOrdIndexedZSet<DynDataTyped<TS>, DynData>,
        >::new::<TS, V, PK, OV, Agg::Accumulator, Agg::Output>();

        self.inner()
            .dyn_partitioned_rolling_aggregate_with_waterline::<_, TS, _, DynData, DynData>(
                &factories,
                &waterline.inner_data(),
                Box::new(
                    move |v, pk: &mut DynData /* <PK> */, ov: &mut DynData /* <OV> */| unsafe {
                        let (tmp_pk, tmp_ov) = partition_func(v.downcast());
                        *pk.downcast_mut() = tmp_pk;
                        *ov.downcast_mut() = tmp_ov;
                    },
                ),
                &DynAggregatorImpl::new(aggregator),
                range,
            )
            .typed()
    }
}

impl<PK, TS, V> Stream<RootCircuit, OrdIndexedZSet<PK, Tup2<TS, V>>>
where
    PK: DBData,
    TS: DBData + PrimInt,
    V: DBData,
{
    pub fn as_partitioned_zset(
        &self,
    ) -> Stream<RootCircuit, OrdPartitionedIndexedZSet<PK, TS, DynDataTyped<TS>, V, DynData>> {
        let factories = BatchReaderFactories::new::<PK, Tup2<TS, V>, ZWeight>();

        self.inner()
            .dyn_map_index(
                &factories,
                Box::new(|(k, v), kv| {
                    kv.from_refs(k, unsafe { v.downcast::<Tup2<TS, V>>().erase() })
                }),
            )
            .typed()
    }
}

impl<PK, TS, V> Stream<RootCircuit, OrdPartitionedIndexedZSet<PK, TS, DynDataTyped<TS>, V, DynData>>
where
    PK: DBData,
    TS: DBData + PrimInt,
    V: DBData,
{
    /// Rolling aggregate of a partitioned stream over time range.
    ///
    /// For each record in the input stream, computes an aggregate
    /// over a relative time range (e.g., the last three months).
    /// Outputs the contents of the input stream extended with the
    /// value of the aggregate.
    ///
    /// For each input record `(p, (ts, v))`, rolling aggregation finds all the
    /// records `(p, (ts2, x))` such that `ts2` is in `range(ts)`, applies
    /// `aggregator` across these records to obtain a finalized value `f`,
    /// and outputs `(p, (ts, f))`.
    ///
    /// This operator is incremental and will update previously
    /// computed outputs affected by new data.  For example,
    /// a data point arriving out-of-order may affect previously
    /// computed rolling aggregate value at future times.
    pub fn partitioned_rolling_aggregate<Agg>(
        &self,
        aggregator: Agg,
        range: RelRange<TS>,
    ) -> OrdPartitionedOverStream<PK, TS, Agg::Output>
    where
        Agg: Aggregator<V, (), ZWeight>,
    {
        let factories =
            PartitionedRollingAggregateFactories::new::<PK, V, Agg::Accumulator, Agg::Output>();

        self.inner()
            .dyn_partitioned_rolling_aggregate::<_, _, DynData, _>(
                &factories,
                &DynAggregatorImpl::new(aggregator),
                range,
            )
            .typed()
    }

    /// A version of [`Self::partitioned_rolling_aggregate`] optimized for
    /// linear aggregation functions.  For each input record `(p, (ts, v))`,
    /// it finds all the records `(p, (ts2, x))` such that `ts2` is in
    /// `range.range_of(ts)`, computes the sum `s` of `f(x)` across these
    /// records, and outputs `(p, (ts, Some(output_func(s))))`.
    ///
    /// Output records from linear aggregation contain an `Option` type because
    /// there might be no records matching `range.range_of(ts)`.  If `range`
    /// contains (relative) time 0, this never happens (because the record
    /// containing `ts` itself is always a match), so in that case the
    /// caller can safely `unwrap()` the `Option`.
    ///
    /// In rolling aggregation, the number of output records matches the number
    /// of input records.
    ///
    /// This method only works for linear aggregation functions `f`, i.e.,
    /// functions that satisfy `f(a+b) = f(a) + f(b)`.  It will produce
    /// incorrect results if `f` is not linear.
    pub fn partitioned_rolling_aggregate_linear<A, O, WF, OF>(
        &self,
        weigh_func: WF,
        output_func: OF,
        range: RelRange<TS>,
    ) -> OrdPartitionedOverStream<PK, TS, O>
    where
        A: DBWeight + MulByRef<ZWeight, Output = A>,
        O: DBData,
        WF: Fn(&V) -> A + Clone + 'static,
        OF: Fn(A) -> O + Clone + 'static,
    {
        let factories = PartitionedRollingAggregateLinearFactories::new::<PK, V, A, O>();

        self.inner()
            .dyn_partitioned_rolling_aggregate_linear(
                &factories,
                Box::new(move |v, r, a: &mut DynWeight /* <A> */| unsafe {
                    *a.downcast_mut() = weigh_func(v.downcast()).mul_by_ref(r.downcast())
                }),
                Box::new(move |a, o: &mut DynData| unsafe {
                    *o.downcast_mut() = output_func(take(a.downcast_mut()))
                }),
                range,
            )
            .typed()
    }

    /// Incremental rolling average.
    ///
    /// For each input record, it computes the average of the values in records
    /// in the same partition in the time range specified by `range`.
    pub fn partitioned_rolling_average(
        &self,
        range: RelRange<TS>,
    ) -> OrdPartitionedOverStream<PK, TS, V>
    where
        V: DBWeight + From<ZWeight> + MulByRef<Output = V> + Div<Output = V>,
    {
        let factories = PartitionedRollingAverageFactories::new::<PK, V, V>();

        self.inner()
            .dyn_partitioned_rolling_average(
                &factories,
                Box::new(move |v, r, a: &mut DynWeight /* <V> */| unsafe {
                    *a.downcast_mut() = v.downcast::<V>().mul_by_ref(&V::from(*r.downcast()))
                }),
                Box::new(|avg, v| unsafe { *v.downcast_mut() = take(avg.downcast_mut::<V>()) }),
                range,
            )
            .typed()
    }
}
