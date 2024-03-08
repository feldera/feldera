use crate::{
    dynamic::{DowncastTrait, DynData, DynPair},
    operator::dynamic::group::LagFactories,
    typed_batch::{DynOrdIndexedZSet, IndexedZSet, TypedBatch},
    utils::Tup2,
    DBData, RootCircuit, Stream, ZWeight,
};

impl<B> Stream<RootCircuit, B>
where
    B: IndexedZSet,
    B::InnerBatch: Send,
{
    /// Lag operator matches each row in a group with a previous row in the
    /// same group.
    ///
    /// For each key in the input stream, it matches each associated value with
    /// a previous value (i.e., value with a smaller index according to
    /// ascending order of values), applies projection function `project` to
    /// it and outputs the input value along with this projection.
    ///
    /// # Arguments
    ///
    /// * `offset` - offset to the previous value.
    /// * `project` - projection function to apply to the delayed row.
    // TODO: for this to return `stat::OrdIndexedZSet`, the implementation of `Lag`
    // must change to take a pair of closures that assemble (V, OV) into an output
    // value and reverse and split it back.
    #[allow(clippy::type_complexity)]
    pub fn lag<OV, PF>(
        &self,
        offset: usize,
        project: PF,
    ) -> Stream<
        RootCircuit,
        TypedBatch<
            B::Key,
            Tup2<B::Val, OV>,
            ZWeight,
            DynOrdIndexedZSet<B::DynK, DynPair<B::DynV, DynData>>,
        >,
    >
    where
        OV: DBData,
        PF: Fn(Option<&B::Val>) -> OV + 'static,
    {
        let factories = LagFactories::<B::Inner, DynData>::new::<B::Key, B::Val, OV>();

        self.inner()
            .dyn_lag(
                &factories,
                offset,
                Box::new(move |v, ov: &mut DynData| unsafe {
                    *ov.downcast_mut::<OV>() = project(v.map(|v| v.downcast::<B::Val>()))
                }),
            )
            .typed()
    }

    /// Lead operator matches each row in a group with a subsequent row in the
    /// same group.
    ///
    /// For each key in the input stream, matches each associated value with
    /// a subsequent value (i.e., value with a larger index according to
    /// ascending order of values), applies projection function `project` to
    /// it and outputs the input value along with this projection.
    ///
    /// # Arguments
    ///
    /// * `offset` - offset to the subsequent value.
    /// * `project` - projection function to apply to the subsequent row. The
    ///   argument is `None` for out-of-range values.
    #[allow(clippy::type_complexity)]
    pub fn lead<OV, PF>(
        &self,
        offset: usize,
        project: PF,
    ) -> Stream<
        RootCircuit,
        TypedBatch<
            B::Key,
            Tup2<B::Val, OV>,
            ZWeight,
            DynOrdIndexedZSet<B::DynK, DynPair<B::DynV, DynData>>,
        >,
    >
    where
        OV: DBData,
        PF: Fn(Option<&B::Val>) -> OV + 'static,
    {
        let factories = LagFactories::<B::Inner, DynData>::new::<B::Key, B::Val, OV>();

        self.inner()
            .dyn_lead(
                &factories,
                offset,
                Box::new(move |v, ov: &mut DynData| unsafe {
                    *ov.downcast_mut::<OV>() = project(v.map(|v| v.downcast::<B::Val>()))
                }),
            )
            .typed()
    }
}
