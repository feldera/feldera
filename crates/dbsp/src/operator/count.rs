use crate::{
    dynamic::{DowncastTrait, DynData},
    operator::dynamic::{
        aggregate::{IncAggregateLinearFactories, StreamLinearAggregateFactories},
        count::{DistinctCountFactories, StreamDistinctCountFactories},
    },
    storage::file::Deserializable,
    typed_batch::{DynOrdIndexedZSet, IndexedZSet, OrdIndexedZSet},
    Circuit, DynZWeight, Stream, ZWeight,
};

impl<C, Z> Stream<C, Z>
where
    C: Circuit,
    Z: IndexedZSet<DynK = DynData>,
    Z::InnerBatch: Send,
    <Z::Key as Deserializable>::ArchivedDeser: Ord,
{
    /// Incrementally sums the weights for each key `self` into an indexed Z-set
    /// that maps from the original keys to the weights.  Both the input and
    /// output are streams of updates.
    #[allow(clippy::type_complexity)]
    pub fn weighted_count(&self) -> Stream<C, OrdIndexedZSet<Z::Key, ZWeight>> {
        let factories: IncAggregateLinearFactories<
            Z::Inner,
            DynZWeight,
            DynOrdIndexedZSet<DynData, DynData>,
            C::Time,
        > = IncAggregateLinearFactories::new::<Z::Key, ZWeight, ZWeight>();

        self.inner()
            .dyn_weighted_count_generic(
                &factories,
                Box::new(|w, out| *unsafe { out.downcast_mut() } = **w),
            )
            .typed()
    }

    /// Like [`Self::dyn_weighted_count`], but can return any batch type.
    pub fn weighted_count_generic<O>(&self) -> Stream<C, O>
    where
        O: IndexedZSet<Key = Z::Key, DynK = Z::DynK, DynV = DynData>,
    {
        let factories: IncAggregateLinearFactories<Z::Inner, DynZWeight, O::Inner, C::Time> =
            IncAggregateLinearFactories::new::<Z::Key, ZWeight, O::Val>();

        self.inner()
            .dyn_weighted_count_generic(
                &factories,
                Box::new(|w, out| *unsafe { out.downcast_mut() } = **w),
            )
            .typed()
    }

    /// Incrementally, for each key in `self`, counts the number of unique
    /// values having positive weights, and outputs it as an indexed Z-set
    /// that maps from the original keys to the unique value counts.  Both
    /// the input and output are streams of updates.
    #[allow(clippy::type_complexity)]
    pub fn distinct_count(&self) -> Stream<C, OrdIndexedZSet<Z::Key, ZWeight>> {
        let factories: DistinctCountFactories<
            Z::Inner,
            DynOrdIndexedZSet<DynData, DynData>,
            C::Time,
        > = DistinctCountFactories::new::<Z::Key, Z::Val, ZWeight>();

        self.inner()
            .dyn_distinct_count_generic(
                &factories,
                Box::new(|w, out| *unsafe { out.downcast_mut() } = **w),
            )
            .typed()
    }

    /// Like [`Self::dyn_distinct_count`], but can return any batch type.
    pub fn distinct_count_generic<O>(&self) -> Stream<C, O>
    where
        O: IndexedZSet<Key = Z::Key, DynK = DynData>,
    {
        let factories: DistinctCountFactories<Z::Inner, O::Inner, C::Time> =
            DistinctCountFactories::new::<Z::Key, Z::Val, O::Val>();

        self.inner()
            .dyn_distinct_count_generic(
                &factories,
                Box::new(|w, out| *unsafe { out.downcast_mut() } = **w),
            )
            .typed()
    }

    /// Non-incrementally sums the weights for each key `self` into an indexed
    /// Z-set that maps from the original keys to the weights.  Both the
    /// input and output are streams of data (not updates).
    #[allow(clippy::type_complexity)]
    pub fn stream_weighted_count(&self) -> Stream<C, OrdIndexedZSet<Z::Key, ZWeight>> {
        let factories: StreamLinearAggregateFactories<
            Z::Inner,
            Z::DynR,
            DynOrdIndexedZSet<DynData, DynData>,
        > = StreamLinearAggregateFactories::new::<Z::Key, Z::Val, ZWeight, ZWeight>();

        self.inner()
            .dyn_stream_weighted_count_generic(
                &factories,
                Box::new(|w, out| *unsafe { out.downcast_mut() } = **w),
            )
            .typed()
    }

    /// Like [`Self::dyn_stream_weighted_count`], but can return any batch type.
    pub fn stream_weighted_count_generic<O>(&self) -> Stream<C, O>
    where
        O: IndexedZSet<Key = Z::Key, DynK = Z::DynK, Val = ZWeight, DynV = DynData>,
    {
        let factories: StreamLinearAggregateFactories<Z::Inner, Z::DynR, O::Inner> =
            StreamLinearAggregateFactories::new::<Z::Key, Z::Val, ZWeight, ZWeight>();

        self.inner()
            .dyn_stream_weighted_count_generic(
                &factories,
                Box::new(|w, out| *unsafe { out.downcast_mut() } = **w),
            )
            .typed()
    }

    /// Incrementally, for each key in `self`, counts the number of unique
    /// values having positive weights, and outputs it as an indexed Z-set
    /// that maps from the original keys to the unique value counts.  Both
    /// the input and output are streams of data (not updates).
    #[allow(clippy::type_complexity)]
    pub fn stream_distinct_count(&self) -> Stream<C, OrdIndexedZSet<Z::Key, ZWeight>> {
        let factories: StreamDistinctCountFactories<Z::Inner, DynOrdIndexedZSet<DynData, DynData>> =
            StreamDistinctCountFactories::new::<Z::Key, Z::Val, ZWeight>();

        self.inner()
            .dyn_stream_distinct_count_generic(
                &factories,
                Box::new(|w, out| *unsafe { out.downcast_mut() } = **w),
            )
            .typed()
    }

    /// Like [`Self::dyn_distinct_count`], but can return any batch type.
    pub fn stream_distinct_count_generic<O>(&self) -> Stream<C, O>
    where
        O: IndexedZSet<Key = Z::Key, DynK = Z::DynK, Val = ZWeight, DynV = DynData>,
    {
        let factories: StreamDistinctCountFactories<Z::Inner, O::Inner> =
            StreamDistinctCountFactories::new::<Z::Key, Z::Val, ZWeight>();

        self.inner()
            .dyn_stream_distinct_count_generic(
                &factories,
                Box::new(|w, out| *unsafe { out.downcast_mut() } = **w),
            )
            .typed()
    }
}
