use crate::{
    algebra::{MulByRef, UnsignedPrimInt},
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
use std::{mem::take, ops::Div};

// TODO: This should be `OrdIndexedZSet<PK, (TS, V)>`.
pub type OrdPartitionedIndexedZSet<PK, TS, DynTS, V, DynV> =
    TypedBatch<PK, Tup2<TS, V>, ZWeight, DynOrdIndexedZSet<DynData, DynPair<DynTS, DynV>>>;

pub type OrdPartitionedOverStream<PK, TS, A> = Stream<
    RootCircuit,
    OrdPartitionedIndexedZSet<PK, TS, DynDataTyped<TS>, Option<A>, DynOpt<DynData>>,
>;

impl<TS, V> Stream<RootCircuit, OrdIndexedZSet<TS, V>>
where
    TS: DBData + UnsignedPrimInt,
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
        self.partitioned_rolling_aggregate_with_waterline_persistent::<PK, OV, Agg, PF>(
            None,
            waterline,
            partition_func,
            aggregator,
            range,
        )
    }

    pub fn partitioned_rolling_aggregate_with_waterline_persistent<PK, OV, Agg, PF>(
        &self,
        persistent_id: Option<&str>,
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
            DynOrdIndexedZSet<DynData, DynData>,
        >::new::<V, PK, OV, Agg::Accumulator, Agg::Output>();

        self.inner()
            .dyn_partitioned_rolling_aggregate_with_waterline::<_, TS, _, DynData, DynData>(
                persistent_id,
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
    pub fn partitioned_rolling_aggregate<PK, OV, Agg, PF>(
        &self,
        partition_func: PF,
        aggregator: Agg,
        range: RelRange<TS>,
    ) -> OrdPartitionedOverStream<PK, TS, Agg::Output>
    where
        Agg: Aggregator<OV, (), ZWeight>,
        OV: DBData,
        PK: DBData,
        PF: Fn(&V) -> (PK, OV) + Clone + 'static,
    {
        self.partitioned_rolling_aggregate_persistent::<PK, OV, Agg, PF>(
            None,
            partition_func,
            aggregator,
            range,
        )
    }

    pub fn partitioned_rolling_aggregate_persistent<PK, OV, Agg, PF>(
        &self,
        persistent_id: Option<&str>,
        partition_func: PF,
        aggregator: Agg,
        range: RelRange<TS>,
    ) -> OrdPartitionedOverStream<PK, TS, Agg::Output>
    where
        Agg: Aggregator<OV, (), ZWeight>,
        OV: DBData,
        PK: DBData,
        PF: Fn(&V) -> (PK, OV) + Clone + 'static,
    {
        let factories =
            PartitionedRollingAggregateFactories::new::<PK, OV, Agg::Accumulator, Agg::Output>();

        self.inner()
            .dyn_partitioned_rolling_aggregate::<_, _, _, DynData, _>(
                persistent_id,
                &factories,
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
    pub fn partitioned_rolling_aggregate_linear<PK, OV, A, O, PF, WF, OF>(
        &self,
        partition_func: PF,
        weigh_func: WF,
        output_func: OF,
        range: RelRange<TS>,
    ) -> OrdPartitionedOverStream<PK, TS, O>
    where
        PK: DBData,
        OV: DBData,
        A: DBWeight + MulByRef<ZWeight, Output = A>,
        O: DBData,
        PF: Fn(&V) -> (PK, OV) + Clone + 'static,
        WF: Fn(&OV) -> A + Clone + 'static,
        OF: Fn(A) -> O + Clone + 'static,
    {
        let factories = PartitionedRollingAggregateLinearFactories::new::<PK, OV, A, O>();

        self.inner()
            .dyn_partitioned_rolling_aggregate_linear(
                None,
                &factories,
                Box::new(
                    move |v, pk: &mut DynData /* <PK> */, ov: &mut DynData /* <OV> */| unsafe {
                        let (tmp_pk, tmp_ov) = partition_func(v.downcast());
                        *pk.downcast_mut() = tmp_pk;
                        *ov.downcast_mut() = tmp_ov;
                    },
                ),
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
    pub fn partitioned_rolling_average<PK, OV, PF>(
        &self,
        partition_func: PF,
        range: RelRange<TS>,
    ) -> OrdPartitionedOverStream<PK, TS, OV>
    where
        OV: DBWeight + From<ZWeight> + MulByRef<Output = OV> + Div<Output = OV>,
        PK: DBData,
        PF: Fn(&V) -> (PK, OV) + Clone + 'static,
    {
        let factories = PartitionedRollingAverageFactories::new::<PK, OV, OV>();

        self.inner()
            .dyn_partitioned_rolling_average(
                None,
                &factories,
                Box::new(
                    move |v, pk: &mut DynData /* <PK> */, ov: &mut DynData /* <OV> */| unsafe {
                        let (tmp_pk, tmp_ov) = partition_func(v.downcast());
                        *pk.downcast_mut() = tmp_pk;
                        *ov.downcast_mut() = tmp_ov;
                    },
                ),
                Box::new(move |v: &DynData, r, a: &mut DynWeight /* <V> */| unsafe {
                    *a.downcast_mut() = v.downcast::<OV>().mul_by_ref(&OV::from(*r.downcast()))
                }),
                Box::new(|avg, v| unsafe { *v.downcast_mut() = take(avg.downcast_mut::<OV>()) }),
                range,
            )
            .typed()
    }
}
