use crate::operator::dynamic::group::LagCustomOrdFactories;
use crate::operator::group::custom_ord::WithCustomOrd;
use crate::{
    CmpFunc, DBData, OrdIndexedZSet, RootCircuit, Stream, ZWeight,
    circuit::metadata::MetaItem,
    dynamic::{DowncastTrait, DynData, DynPair, Erase},
    operator::dynamic::group::LagFactories,
    trace::Filter,
    typed_batch::{DynOrdIndexedZSet, IndexedZSet, TypedBatch, TypedBox},
    utils::Tup2,
};
use dyn_clone::clone_box;

impl<B> Stream<RootCircuit, B>
where
    B: IndexedZSet,
    B::InnerBatch: Send,
{
    /// Lag operator matches each row in a group with a value at the given offset
    /// in the same group.
    ///
    /// For each key in the input stream, it matches each associated value with
    /// another value in the same group with a smaller (`offset > 0`) or greater
    /// (`offset < 0`) index according to ascending order of values, applies
    /// projection function `project` to it and outputs the input value along
    /// with this projection.
    ///
    /// The offset it computed as if each value occurred as many times as its
    /// weight.  Values with negative weights are ignored; hence the output Z-set
    /// will only contain positive weights.
    ///
    /// # Arguments
    ///
    /// * `offset` - offset to the previous value.
    /// * `project` - projection function to apply to the delayed row.
    // TODO: for this to return `stat::OrdIndexedZSet`, the implementation of `Lag`
    // must change to take a pair of closures that assemble (V, OV) into an output
    // value and reverse and split it back.
    #[allow(clippy::type_complexity)]
    pub fn lag<VL, PF>(
        &self,
        offset: isize,
        project: PF,
    ) -> Stream<
        RootCircuit,
        TypedBatch<
            B::Key,
            Tup2<B::Val, VL>,
            ZWeight,
            DynOrdIndexedZSet<B::DynK, DynPair<B::DynV, DynData>>,
        >,
    >
    where
        VL: DBData,
        PF: Fn(Option<&B::Val>) -> VL + 'static,
    {
        let factories = LagFactories::<B::Inner, DynData>::new::<B::Key, B::Val, VL>();

        self.inner()
            .dyn_lag(
                None,
                &factories,
                offset,
                Box::new(move |v, ov: &mut DynData| unsafe {
                    *ov.downcast_mut::<VL>() = project(v.map(|v| v.downcast::<B::Val>()))
                }),
            )
            .typed()
    }
}

impl<K, V> Stream<RootCircuit, OrdIndexedZSet<K, V>>
where
    K: DBData,
    V: DBData,
{
    /// Like [`Stream::lag`], but uses a custom ordering of values within the group
    /// defined by the comparison function `CF`.
    ///
    /// # Arguments
    ///
    /// * `offset` - offset to the previous or next value.
    /// * `project` - projection function to apply to the delayed row.
    /// * `output` - output function that constructs the output value from
    ///   the value of the current row and the projection of the delayed
    ///   row.
    #[allow(clippy::type_complexity)]
    pub fn lag_custom_order<VL, OV, PF, CF, OF>(
        &self,
        offset: isize,
        project: PF,
        output: OF,
    ) -> Stream<RootCircuit, OrdIndexedZSet<K, OV>>
    where
        VL: DBData,
        OV: DBData,
        CF: CmpFunc<V>,
        PF: Fn(Option<&V>) -> VL + 'static,
        OF: Fn(&V, &VL) -> OV + 'static,
    {
        self.lag_custom_order_persistent::<VL, OV, PF, CF, OF>(None, offset, project, output)
    }

    #[allow(clippy::type_complexity)]
    pub fn lag_custom_order_persistent<VL, OV, PF, CF, OF>(
        &self,
        persistent_id: Option<&str>,
        offset: isize,
        project: PF,
        output: OF,
    ) -> Stream<RootCircuit, OrdIndexedZSet<K, OV>>
    where
        VL: DBData,
        OV: DBData,
        CF: CmpFunc<V>,
        PF: Fn(Option<&V>) -> VL + 'static,
        OF: Fn(&V, &VL) -> OV + 'static,
    {
        let factories = LagCustomOrdFactories::<
            DynOrdIndexedZSet<DynData, DynData>,
            DynData,
            DynData,
            DynData,
        >::new::<K, V, WithCustomOrd<V, CF>, VL, OV>();

        self.inner()
            .dyn_lag_custom_order_mono(
                persistent_id,
                &factories,
                offset,
                encode_custom_ord::<V, CF>(),
                project_custom_ord::<V, VL, CF, PF>(project),
                decode_custom_ord::<V, VL, OV, CF, OF>(output),
            )
            .typed()
    }

    /// Like [`Stream::lag_custom_order`], but garbage-collects the state of
    /// the operator below a waterline.
    ///
    /// The operator keeps the input rows and its output in integrals of its
    /// own.  Only the input rows at or above the waterline can still change,
    /// and changing one changes the outputs of at most `|offset|` other rows,
    /// so the operator needs only a few rows below the waterline in each
    /// group.  Its integrals keep the rows at or above the waterline and, in
    /// each group, the last few rows below it: `|offset|` input rows, and for
    /// LEAD `2 * |offset|` output records.
    ///
    /// # Correctness
    ///
    /// The operator returns the same output as [`Stream::lag_custom_order`]
    /// as long as its input meets the following conditions:
    ///
    /// * Every row that a transaction inserts or deletes has
    ///   `above_waterline(row, w)` for each value `w` of `waterline` during
    ///   the transaction.  The operator processes the changes of a
    ///   transaction when it commits, while garbage collection applies the
    ///   waterline at every step, so a waterline that changes only between
    ///   transactions, such as a waterline delayed by `transaction_delay`,
    ///   is the simplest to bound the input with.  The operator may process
    ///   a change below the waterline incorrectly.
    ///
    /// * `waterline` never decreases, and `above_waterline` is monotone in
    ///   both arguments: once it holds for a row in a group, it holds for
    ///   every later row of the group in the order defined by `CF`, and once
    ///   it fails for a row, it fails for every later waterline.  For
    ///   example, a predicate `row.ts >= waterline` qualifies when `ts` is
    ///   the first column that `CF` compares, in ascending order.
    ///
    /// * The integral of the input has no negative weights.
    ///
    /// # Arguments
    ///
    /// * `waterline` - a lower bound on the input rows.
    /// * `above_waterline` - returns `true` if a row is at or above a
    ///   waterline, i.e., if it can still change.
    /// * `offset` - offset to the previous (`offset > 0`) or next
    ///   (`offset < 0`) value.
    /// * `project` - projection function to apply to the delayed row.
    /// * `output` - output function that constructs the output value from
    ///   the value of the current row and the projection of the delayed
    ///   row.
    #[allow(clippy::type_complexity)]
    pub fn lag_custom_order_with_waterline<VL, OV, PF, CF, OF, TS, RF>(
        &self,
        waterline: &Stream<RootCircuit, TypedBox<TS, DynData>>,
        above_waterline: RF,
        offset: isize,
        project: PF,
        output: OF,
    ) -> Stream<RootCircuit, OrdIndexedZSet<K, OV>>
    where
        VL: DBData,
        OV: DBData,
        CF: CmpFunc<V>,
        PF: Fn(Option<&V>) -> VL + 'static,
        OF: Fn(&V, &VL) -> OV + 'static,
        TS: DBData + Erase<DynData>,
        RF: Fn(&V, &TS) -> bool + Clone + Send + Sync + 'static,
    {
        self.lag_custom_order_with_waterline_persistent::<VL, OV, PF, CF, OF, TS, RF>(
            None,
            waterline,
            above_waterline,
            offset,
            project,
            output,
        )
    }

    /// Like [`Stream::lag_custom_order_with_waterline`], with a persistent
    /// id for the state of the operator.
    #[allow(clippy::type_complexity, clippy::too_many_arguments)]
    pub fn lag_custom_order_with_waterline_persistent<VL, OV, PF, CF, OF, TS, RF>(
        &self,
        persistent_id: Option<&str>,
        waterline: &Stream<RootCircuit, TypedBox<TS, DynData>>,
        above_waterline: RF,
        offset: isize,
        project: PF,
        output: OF,
    ) -> Stream<RootCircuit, OrdIndexedZSet<K, OV>>
    where
        VL: DBData,
        OV: DBData,
        CF: CmpFunc<V>,
        PF: Fn(Option<&V>) -> VL + 'static,
        OF: Fn(&V, &VL) -> OV + 'static,
        TS: DBData + Erase<DynData>,
        RF: Fn(&V, &TS) -> bool + Clone + Send + Sync + 'static,
    {
        let factories = LagCustomOrdFactories::<
            DynOrdIndexedZSet<DynData, DynData>,
            DynData,
            DynData,
            DynData,
        >::new::<K, V, WithCustomOrd<V, CF>, VL, OV>();

        self.inner()
            .dyn_lag_custom_order_with_waterline_mono(
                persistent_id,
                &factories,
                &waterline.inner_data(),
                Box::new(move |ts: &DynData| {
                    let metadata = MetaItem::String(format!("{ts:?}"));
                    let ts = clone_box(ts);
                    let above_waterline = above_waterline.clone();
                    Filter::new(Box::new(move |row: &DynData| {
                        // SAFETY: the operator applies the filter only to encoded
                        // rows, which `factories` creates as `WithCustomOrd<V, CF>`,
                        // and `ts` is a value of `waterline`, of type `TS`.
                        above_waterline(
                            &unsafe { row.downcast::<WithCustomOrd<V, CF>>() }.val,
                            unsafe { ts.as_ref().downcast::<TS>() },
                        )
                    }))
                    .with_metadata(metadata)
                }),
                offset,
                encode_custom_ord::<V, CF>(),
                project_custom_ord::<V, VL, CF, PF>(project),
                decode_custom_ord::<V, VL, OV, CF, OF>(output),
            )
            .typed()
    }
}

/// Returns the function that encodes an input row of a LAG operator in the
/// custom order `CF`.
fn encode_custom_ord<V, CF>() -> Box<dyn Fn(&DynData, &mut DynData)>
where
    V: DBData,
    CF: CmpFunc<V>,
{
    // SAFETY: the callers apply this function to input rows of type `V`, with
    // factories that create encoded rows as `WithCustomOrd<V, CF>`.
    Box::new(move |v1, v2: &mut DynData| unsafe {
        *v2.downcast_mut::<WithCustomOrd<V, CF>>() = WithCustomOrd::new(v1.downcast::<V>().clone())
    })
}

/// Returns the function that applies `project` to an encoded row of a LAG
/// operator.
fn project_custom_ord<V, VL, CF, PF>(project: PF) -> Box<dyn Fn(Option<&DynData>, &mut DynData)>
where
    V: DBData,
    VL: DBData,
    CF: CmpFunc<V>,
    PF: Fn(Option<&V>) -> VL + 'static,
{
    // SAFETY: the callers build the factories that create encoded rows as
    // `WithCustomOrd<V, CF>` and projections as `VL`.
    Box::new(move |v, ov: &mut DynData| unsafe {
        *ov.downcast_mut::<VL>() = project(v.map(|v| &v.downcast::<WithCustomOrd<V, CF>>().val))
    })
}

/// Returns the function that applies `output` to an encoded row of a LAG
/// operator and the projection of its delayed row.
fn decode_custom_ord<V, VL, OV, CF, OF>(output: OF) -> Box<dyn Fn(&DynData, &DynData, &mut DynData)>
where
    V: DBData,
    VL: DBData,
    OV: DBData,
    CF: CmpFunc<V>,
    OF: Fn(&V, &VL) -> OV + 'static,
{
    // SAFETY: the callers build the factories that create encoded rows as
    // `WithCustomOrd<V, CF>`, projections as `VL`, and outputs as `OV`.
    Box::new(move |v2, vl, ov| {
        *unsafe { ov.downcast_mut::<OV>() } = output(
            &unsafe { v2.downcast::<WithCustomOrd<V, CF>>() }.val,
            unsafe { vl.downcast::<VL>() },
        )
    })
}
