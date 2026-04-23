use std::cmp::Ordering;

use crate::{
    CmpFunc, DBData, OrdIndexedZSet, RootCircuit, Stream,
    dynamic::{DowncastTrait, DynData},
    operator::{dynamic::group::RankCustomOrdFactories, group::WithCustomOrd},
};

impl<K, V> Stream<RootCircuit, OrdIndexedZSet<K, V>>
where
    K: DBData,
    V: DBData,
{
    /// Rank elements in the group.
    ///
    /// This operator implements the behavior of the following SQL pattern:
    /// ```text
    /// SELECT
    ///     ...,
    ///     RANK() OVER (PARTITION BY .. ORDER BY ...) AS rank
    /// FROM table
    /// ```
    ///
    /// The `CF` type, `projection_func`, and `rank_cmp_func` function together establish the
    /// ranking of values in the group:
    /// * `CF` establishes a _total_ ordering of elements such that `v1 <= v2 =>
    ///   rank(v1) <= rank(v2)`.
    /// * `projection_func` projects the value to a value that is used for the ranking
    ///   (e.g., the ORDER BY columns).
    /// * `rank_cmp_func` compares the rank of two elements, one specified by its full value, the other by its projected value.
    ///   Must respect the following equivalence:
    ///   `rank_cmp_func(v1, v2) <=> rank(v1).cmp(rank(v2))`.
    ///
    /// The `output_func` closure takes a value and its rank and produces an
    /// output value.
    ///
    /// ## Correctness
    ///
    /// * `CF` must establish a total order over `V`, consistent with `impl E
    ///   for V`, i.e., `CF::cmp(v1, v2) == Equal <=> v1.eq(v2)`.
    /// * `CF` must be consistent with `rank_cmp_func`, i.e.,
    ///   - `CF::cmp(v1, v2) == Less => rank_cmp_func(v1, v2) != Greater`.
    ///   - `CF::cmp(v1, v2) == Equal => rank_cmp_func(v1, v2) == Equal`.
    ///   - `CF::cmp(v1, v2) == Greater => rank_cmp_func(v1, v2) != Less`.
    pub fn rank_custom_order<CF, RV, PF, EF, OF, OV>(
        &self,
        projection_func: PF,
        rank_cmp_func: EF,
        output_func: OF,
    ) -> Stream<RootCircuit, OrdIndexedZSet<K, OV>>
    where
        CF: CmpFunc<V>,
        OV: DBData,
        RV: DBData,
        PF: Fn(&V) -> RV + 'static,
        EF: Fn(&V, &RV) -> Ordering + 'static,
        OF: Fn(i64, &V) -> OV + 'static,
    {
        self.rank_custom_order_persistent::<CF, RV, PF, EF, OF, OV>(
            None,
            projection_func,
            rank_cmp_func,
            output_func,
        )
    }

    pub fn rank_custom_order_persistent<CF, RV, PF, EF, OF, OV>(
        &self,
        persistent_id: Option<&str>,
        projection_func: PF,
        rank_cmp_func: EF,
        output_func: OF,
    ) -> Stream<RootCircuit, OrdIndexedZSet<K, OV>>
    where
        CF: CmpFunc<V>,
        RV: DBData,
        OV: DBData,
        PF: Fn(&V) -> RV + 'static,
        EF: Fn(&V, &RV) -> Ordering + 'static,
        OF: Fn(i64, &V) -> OV + 'static,
    {
        let factories = RankCustomOrdFactories::<DynData, DynData, DynData, DynData>::new::<
            K,
            WithCustomOrd<V, CF>,
            RV,
            OV,
        >();

        self.inner()
            .dyn_rank_custom_order_mono(
                persistent_id,
                &factories,
                Box::new(
                    move |v1, v2: &mut DynData /* <WithCustomOrd<V, CF>> */| unsafe {
                        *v2.downcast_mut::<WithCustomOrd<V, CF>>() =
                            WithCustomOrd::new(v1.downcast::<V>().clone())
                    },
                ),
                Box::new(move |v, rv| unsafe {
                    *rv.downcast_mut::<RV>() =
                        (projection_func)(&v.downcast::<WithCustomOrd<V, CF>>().val)
                }),
                Box::new(move |x, y| unsafe {
                    rank_cmp_func(
                        &x.downcast::<WithCustomOrd<V, CF>>().val,
                        y.downcast::<RV>(),
                    )
                }),
                Box::new(move |rank, v2, ov| unsafe {
                    *ov.downcast_mut() =
                        output_func(rank, &v2.downcast::<WithCustomOrd<V, CF>>().val)
                }),
            )
            .typed()
    }

    /// Rank elements in the group.
    ///
    /// This operator implements the behavior of the following SQL pattern:
    /// ```text
    /// SELECT
    ///     ...,
    ///     DENSE_RANK() OVER (PARTITION BY .. ORDER BY ...) AS rank
    /// FROM table
    /// ```
    ///
    /// The `CF` type, `projection_func`, and `rank_cmp_func` function together establish the
    /// ranking of values in the group:
    /// * `CF` establishes a _total_ ordering of elements such that `v1 < v2 =>
    ///   rank(v1) <= rank(v2)`.
    /// * `projection_func` projects the value to a value that is used for the ranking
    ///   (e.g., the ORDER BY columns).
    /// * `rank_cmp_func` compares the rank of two elements, one specified by its full value, the other by its projected value.
    ///   Must respect the following equivalence:
    ///   `rank_cmp_func(v1, v2) <=> rank(v1).cmp(rank(v2))`.
    ///
    /// The `output_func` closure takes a value and its rank and produces an
    /// output value.
    ///
    /// ## Correctness
    ///
    /// * `CF` must establish a total order over `V`, consistent with `impl Eq
    ///   for V`, i.e., `CF::cmp(v1, v2) == Equal <=> v1.eq(v2)`.
    /// * `CF` must be consistent with `rank_cmp_func`, i.e.,
    ///   - `CF::cmp(v1, v2) == Less => rank_cmp_func(v1, v2) != Greater`.
    ///   - `CF::cmp(v1, v2) == Equal => rank_cmp_func(v1, v2) == Equal`.
    ///   - `CF::cmp(v1, v2) == Greater => rank_cmp_func(v1, v2) != Less`.
    pub fn dense_rank_custom_order<CF, RV, PF, EF, OF, OV>(
        &self,
        projection_func: PF,
        rank_cmp_func: EF,
        output_func: OF,
    ) -> Stream<RootCircuit, OrdIndexedZSet<K, OV>>
    where
        CF: CmpFunc<V>,
        OV: DBData,
        RV: DBData,
        PF: Fn(&V) -> RV + 'static,
        EF: Fn(&V, &RV) -> Ordering + 'static,
        OF: Fn(i64, &V) -> OV + 'static,
    {
        self.dense_rank_custom_order_persistent::<CF, RV, PF, EF, OF, OV>(
            None,
            projection_func,
            rank_cmp_func,
            output_func,
        )
    }

    pub fn dense_rank_custom_order_persistent<CF, RV, PF, EF, OF, OV>(
        &self,
        persistent_id: Option<&str>,
        projection_func: PF,
        rank_cmp_func: EF,
        output_func: OF,
    ) -> Stream<RootCircuit, OrdIndexedZSet<K, OV>>
    where
        CF: CmpFunc<V>,
        OV: DBData,
        RV: DBData,
        PF: Fn(&V) -> RV + 'static,
        EF: Fn(&V, &RV) -> Ordering + 'static,
        OF: Fn(i64, &V) -> OV + 'static,
    {
        let factories = RankCustomOrdFactories::<DynData, DynData, DynData, DynData>::new::<
            K,
            WithCustomOrd<V, CF>,
            RV,
            OV,
        >();

        self.inner()
            .dyn_dense_rank_custom_order_mono(
                persistent_id,
                &factories,
                Box::new(
                    move |v1, v2: &mut DynData /* <WithCustomOrd<V, CF>> */| unsafe {
                        *v2.downcast_mut::<WithCustomOrd<V, CF>>() =
                            WithCustomOrd::new(v1.downcast::<V>().clone())
                    },
                ),
                Box::new(move |v, rv| unsafe {
                    *rv.downcast_mut::<RV>() =
                        projection_func(&v.downcast::<WithCustomOrd<V, CF>>().val)
                }),
                Box::new(move |x, y| unsafe {
                    rank_cmp_func(
                        &x.downcast::<WithCustomOrd<V, CF>>().val,
                        y.downcast::<RV>(),
                    )
                }),
                Box::new(move |rank, v2, ov| unsafe {
                    *ov.downcast_mut() =
                        output_func(rank, &v2.downcast::<WithCustomOrd<V, CF>>().val)
                }),
            )
            .typed()
    }

    // /// Number the rows in the group according to the order defined by `CF`.
    // ///
    // /// This operator implements the behavior of the following SQL pattern:
    // ///
    // /// ```text
    // /// SELECT
    // ///     ...,
    // ///     ROW_NUMBER() OVER (PARTITION BY .. ORDER BY ...) AS row_number
    // /// FROM table
    // /// ```
    // ///
    // /// ## Correctness
    // ///
    // /// * `CF` must establish a _total_ order over `V`, consistent with `impl Eq
    // ///   for V`, i.e., `CF::cmp(v1, v2) == Equal <=> v1.eq(v2)`.
    // pub fn row_number_custom_order<CF, OF, OV>(
    //     &self,
    //     output_func: OF,
    // ) -> Stream<RootCircuit, OrdIndexedZSet<K, OV>>
    // where
    //     CF: CmpFunc<V>,
    //     OV: DBData,
    //     OF: Fn(i64, &V) -> OV + 'static,
    // {
    //     self.row_number_custom_order_persistent::<CF, OF, OV>(None, output_func)
    // }

    // pub fn row_number_custom_order_persistent<CF, OF, OV>(
    //     &self,
    //     persistent_id: Option<&str>,
    //     output_func: OF,
    // ) -> Stream<RootCircuit, OrdIndexedZSet<K, OV>>
    // where
    //     CF: CmpFunc<V>,
    //     OV: DBData,
    //     OF: Fn(i64, &V) -> OV + 'static,
    // {
    //     todo!()
    //     // let factories = TopKRankCustomOrdFactories::<DynData, DynData, DynData>::new::<
    //     //     K,
    //     //     WithCustomOrd<V, CF>,
    //     //     OV,
    //     // >();

    //     // self.inner()
    //     //     .dyn_topk_row_number_custom_order_mono(
    //     //         persistent_id,
    //     //         &factories,
    //     //         k,
    //     //         Box::new(
    //     //             move |v1, v2: &mut DynData /* <WithCustomOrd<V, CF>> */| unsafe {
    //     //                 *v2.downcast_mut::<WithCustomOrd<V, CF>>() =
    //     //                     WithCustomOrd::new(v1.downcast::<V>().clone())
    //     //             },
    //     //         ),
    //     //         Box::new(move |rank, v2, ov| unsafe {
    //     //             *ov.downcast_mut() =
    //     //                 output_func(rank, &v2.downcast::<WithCustomOrd<V, CF>>().val)
    //     //         }),
    //     //     )
    //     //     .typed()
    // }
}
