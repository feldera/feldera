use crate::{
    dynamic::{DowncastTrait, DynData},
    operator::{
        dynamic::group::{TopKCustomOrdFactories, TopKFactories, TopKRankCustomOrdFactories},
        group::custom_ord::{CmpFunc, WithCustomOrd},
    },
    trace::Spillable,
    typed_batch::{IndexedZSet, OrdIndexedZSet},
    DBData, DynZWeight, RootCircuit, Stream, ZWeight,
};

impl<B> Stream<RootCircuit, B>
where
    B: IndexedZSet<DynK = DynData, DynV = DynData>,
    B::InnerBatch: Spillable + Send,
{
    /// Pick `k` smallest values in each group.
    ///
    /// For each key in the input stream, removes all but `k` smallest values.
    #[allow(clippy::type_complexity)]
    pub fn topk_asc(&self, k: usize) -> Stream<RootCircuit, OrdIndexedZSet<B::Key, B::Val>> {
        let factories = TopKFactories::new::<B::Key, B::Val>();
        self.inner().dyn_topk_asc(&factories, k).typed()
    }

    /// Pick `k` largest values in each group.
    ///
    /// For each key in the input stream, removes all but `k` largest values.
    #[allow(clippy::type_complexity)]
    pub fn topk_desc(&self, k: usize) -> Stream<RootCircuit, OrdIndexedZSet<B::Key, B::Val>> {
        let factories = TopKFactories::new::<B::Key, B::Val>();

        self.inner().dyn_topk_desc(&factories, k).typed()
    }
}

impl<K, V> Stream<RootCircuit, OrdIndexedZSet<K, V>>
where
    K: DBData,
    V: DBData,
{
    /// Pick `k` smallest values in each group based on a custom comparison
    /// function.
    ///
    /// This method is similar to [`topk_asc`](`Stream::topk_asc`), but instead
    /// of ordering elements according to `trait Ord for V`, it uses a
    /// user-defined comparison function `F`.
    ///
    /// ## Correctness
    ///
    /// * `CF` must establish a _total_ order over `V`, consistent with `impl Eq
    ///   for V`, i.e., `CF::cmp(v1, v2) == Equal <=> v1.eq(v2)`.
    pub fn topk_custom_order<F>(&self, k: usize) -> Self
    where
        F: CmpFunc<V>,
    {
        let factories = TopKCustomOrdFactories::<DynData, DynData, DynData, DynZWeight>::new::<
            K,
            V,
            WithCustomOrd<V, F>,
            ZWeight,
        >();

        self.inner()
            .dyn_topk_custom_order(
                &factories,
                k,
                Box::new(
                    move |v1, v2: &mut DynData /* <WithCustomOrd<V, F>> */| unsafe {
                        *v2.downcast_mut::<WithCustomOrd<V, F>>() =
                            WithCustomOrd::new(v1.downcast::<V>().clone())
                    },
                ),
                Box::new(move |v2| unsafe { &v2.downcast::<WithCustomOrd<V, F>>().val }),
            )
            .typed()
    }

    /// Rank elements in the group and output all elements with `rank <= k`.
    ///
    /// This operator implements the behavior of the following SQL pattern:
    /// ```text
    /// SELECT
    ///     ...,
    ///     RANK() OVER (PARTITION BY .. ORDER BY ...) AS rank
    /// FROM table
    /// WHERE rank <= K
    /// ```
    ///
    /// The `CF` type and the `rank_eq_func` function together establish the
    /// ranking of values in the group:
    /// * `CF` establishes a _total_ ordering of elements such that `v1 < v2 =>
    ///   rank(v1) <= rank(v2)`.
    /// * `rank_eq_func` checks that two elements have equal rank, i.e., have
    ///   equal values in all the ORDER BY columns in the SQL query above:
    ///   `rank_eq_func(v1, v2) <=> rank(v1) == rank(v2)`.
    ///
    /// The `output_func` closure takes a value and its rank and produces an
    /// output value.
    ///
    /// ## Correctness
    ///
    /// * `CF` must establish a total order over `V`, consistent with `impl Eq
    ///   for V`, i.e., `CF::cmp(v1, v2) == Equal <=> v1.eq(v2)`.
    /// * `CF` must be consistent with `rank_eq_func`, i.e., `CF::cmp(v1, v2) ==
    ///   Equal => rank_eq_func(v1, v2)`.
    pub fn topk_rank_custom_order<CF, EF, OF, OV>(
        &self,
        k: usize,
        rank_eq_func: EF,
        output_func: OF,
    ) -> Stream<RootCircuit, OrdIndexedZSet<K, OV>>
    where
        CF: CmpFunc<V>,
        OV: DBData,
        EF: Fn(&V, &V) -> bool + 'static,
        OF: Fn(i64, &V) -> OV + 'static,
    {
        let factories = TopKRankCustomOrdFactories::<DynData, DynData, DynData>::new::<
            K,
            WithCustomOrd<V, CF>,
            OV,
        >();

        self.inner()
            .dyn_topk_rank_custom_order(
                &factories,
                k,
                Box::new(
                    move |v1, v2: &mut DynData /* <WithCustomOrd<V, CF>> */| unsafe {
                        *v2.downcast_mut::<WithCustomOrd<V, CF>>() =
                            WithCustomOrd::new(v1.downcast::<V>().clone())
                    },
                ),
                Box::new(move |x, y| unsafe {
                    rank_eq_func(
                        &x.downcast::<WithCustomOrd<V, CF>>().val,
                        &y.downcast::<WithCustomOrd<V, CF>>().val,
                    )
                }),
                Box::new(move |rank, v2, ov| unsafe {
                    *ov.downcast_mut() =
                        output_func(rank, &v2.downcast::<WithCustomOrd<V, CF>>().val)
                }),
            )
            .typed()
    }

    /// Rank elements in the group using dense ranking and output all elements
    /// with `rank <= k`.
    ///
    /// Similar to [`topk_rank_custom_order`](`Self::topk_rank_custom_order`),
    /// but computes a dense ranking of elements in the group.
    pub fn topk_dense_rank_custom_order<CF, EF, OF, OV>(
        &self,
        k: usize,
        rank_eq_func: EF,
        output_func: OF,
    ) -> Stream<RootCircuit, OrdIndexedZSet<K, OV>>
    where
        CF: CmpFunc<V>,
        OV: DBData,
        EF: Fn(&V, &V) -> bool + 'static,
        OF: Fn(i64, &V) -> OV + 'static,
    {
        let factories = TopKRankCustomOrdFactories::<DynData, DynData, DynData>::new::<
            K,
            WithCustomOrd<V, CF>,
            OV,
        >();

        self.inner()
            .dyn_topk_dense_rank_custom_order(
                &factories,
                k,
                Box::new(
                    move |v1, v2: &mut DynData /* <WithCustomOrd<V, CF>> */| unsafe {
                        *v2.downcast_mut::<WithCustomOrd<V, CF>>() =
                            WithCustomOrd::new(v1.downcast::<V>().clone())
                    },
                ),
                Box::new(move |x, y| unsafe {
                    rank_eq_func(
                        &x.downcast::<WithCustomOrd<V, CF>>().val,
                        &y.downcast::<WithCustomOrd<V, CF>>().val,
                    )
                }),
                Box::new(move |rank, v2, ov| unsafe {
                    *ov.downcast_mut() =
                        output_func(rank, &v2.downcast::<WithCustomOrd<V, CF>>().val)
                }),
            )
            .typed()
    }

    /// Pick `k` smallest values in each group based on a custom comparison
    /// function.  Return the `k` elements along with their row numbers.
    ///
    /// This operator implements the behavior of the following SQL pattern:
    ///
    /// ```text
    /// SELECT
    ///     ...,
    ///     ROW_NUMBER() OVER (PARTITION BY .. ORDER BY ...) AS row_number
    /// FROM table
    /// WHERE row_number <= K
    /// ```
    ///
    /// ## Correctness
    ///
    /// * `CF` must establish a _total_ order over `V`, consistent with `impl Eq
    ///   for V`, i.e., `CF::cmp(v1, v2) == Equal <=> v1.eq(v2)`.
    pub fn topk_row_number_custom_order<CF, OF, OV>(
        &self,
        k: usize,
        output_func: OF,
    ) -> Stream<RootCircuit, OrdIndexedZSet<K, OV>>
    where
        CF: CmpFunc<V>,
        OV: DBData,
        OF: Fn(i64, &V) -> OV + 'static,
    {
        let factories = TopKRankCustomOrdFactories::<DynData, DynData, DynData>::new::<
            K,
            WithCustomOrd<V, CF>,
            OV,
        >();

        self.inner()
            .dyn_topk_row_number_custom_order(
                &factories,
                k,
                Box::new(
                    move |v1, v2: &mut DynData /* <WithCustomOrd<V, CF>> */| unsafe {
                        *v2.downcast_mut::<WithCustomOrd<V, CF>>() =
                            WithCustomOrd::new(v1.downcast::<V>().clone())
                    },
                ),
                Box::new(move |rank, v2, ov| unsafe {
                    *ov.downcast_mut() =
                        output_func(rank, &v2.downcast::<WithCustomOrd<V, CF>>().val)
                }),
            )
            .typed()
    }
}
