use crate::{
    CmpFunc, DBData, OrdIndexedZSet, RootCircuit, Stream,
    dynamic::{DowncastTrait, DynData},
    operator::{dynamic::group::RowNumberCustomOrdFactories, group::WithCustomOrd},
};

impl<K, V> Stream<RootCircuit, OrdIndexedZSet<K, V>>
where
    K: DBData,
    V: DBData,
{
    /// Row number operator.
    ///
    /// This operator implements the behavior of the following SQL pattern:
    /// ```text
    /// SELECT
    ///     ...,
    ///     ROW_NUMBER() OVER (PARTITION BY .. ORDER BY ...) AS rank
    /// FROM table
    /// ```
    ///
    /// The `CF` type establishes the ordering of values in the group.
    ///
    /// The `output_func` closure takes a value and its row number and produces an
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
    pub fn row_number_custom_order<CF, OF, OV>(
        &self,
        output_func: OF,
    ) -> Stream<RootCircuit, OrdIndexedZSet<K, OV>>
    where
        CF: CmpFunc<V>,
        OV: DBData,
        OF: Fn(i64, &V) -> OV + 'static,
    {
        self.row_number_custom_order_persistent::<CF, OF, OV>(None, output_func)
    }

    pub fn row_number_custom_order_persistent<CF, OF, OV>(
        &self,
        persistent_id: Option<&str>,
        output_func: OF,
    ) -> Stream<RootCircuit, OrdIndexedZSet<K, OV>>
    where
        CF: CmpFunc<V>,
        OV: DBData,
        OF: Fn(i64, &V) -> OV + 'static,
    {
        let factories = RowNumberCustomOrdFactories::<DynData, DynData, DynData>::new::<
            K,
            WithCustomOrd<V, CF>,
            OV,
        >();

        self.inner()
            .dyn_row_number_custom_order_mono(
                persistent_id,
                &factories,
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
