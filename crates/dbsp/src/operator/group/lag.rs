use crate::operator::dynamic::group::LagCustomOrdFactories;
use crate::operator::group::custom_ord::WithCustomOrd;
use crate::{
    dynamic::{DowncastTrait, DynData, DynPair},
    operator::dynamic::group::LagFactories,
    typed_batch::{DynOrdIndexedZSet, IndexedZSet, TypedBatch},
    utils::Tup2,
    CmpFunc, DBData, OrdIndexedZSet, RootCircuit, Stream, ZWeight,
};

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
                Box::new(move |v1, v2: &mut DynData| unsafe {
                    *v2.downcast_mut::<WithCustomOrd<V, CF>>() =
                        WithCustomOrd::new(v1.downcast::<V>().clone())
                }),
                Box::new(move |v, ov: &mut DynData| unsafe {
                    *ov.downcast_mut::<VL>() =
                        project(v.map(|v| &v.downcast::<WithCustomOrd<V, CF>>().val))
                }),
                Box::new(move |v2, vl, ov| {
                    *unsafe { ov.downcast_mut::<OV>() } = output(
                        &unsafe { v2.downcast::<WithCustomOrd<V, CF>>() }.val,
                        unsafe { vl.downcast::<VL>() },
                    )
                }),
            )
            .typed()
    }
}
