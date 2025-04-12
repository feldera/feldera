use std::mem;

use crate::{
    dynamic::{DowncastTrait, DynData},
    trace::BatchReaderFactories,
    DBData, OrdIndexedZSet, RootCircuit, Stream, ZWeight,
};

impl<K, V> Stream<RootCircuit, OrdIndexedZSet<K, V>>
where
    K: DBData,
    V: DBData,
{
    /// Aggregate whose value depends on the previous value of the aggregate
    /// and changes to the input collection.
    ///
    /// Unlike general aggregates, such aggregates don't require storing the integral
    /// of the input collection and hence require only O(1) memory per key.
    ///
    /// Examples include min and max over append-only collections.
    ///
    /// # Arguments
    ///
    /// * `finit` - returns the initial value of the aggregate.
    /// * `fupdate` - updates the aggregate given its previous value and a
    ///   new element of the group.
    #[track_caller]
    pub fn chain_aggregate<A, FInit, FUpdate>(
        &self,
        finit: FInit,
        fupdate: FUpdate,
    ) -> Stream<RootCircuit, OrdIndexedZSet<K, A>>
    where
        A: DBData,
        FInit: Fn(&V, ZWeight) -> A + 'static,
        FUpdate: Fn(A, &V, ZWeight) -> A + 'static,
    {
        self.chain_aggregate_persistent::<A, FInit, FUpdate>(None, finit, fupdate)
    }

    #[track_caller]
    pub fn chain_aggregate_persistent<A, FInit, FUpdate>(
        &self,
        persistent_id: Option<&str>,
        finit: FInit,
        fupdate: FUpdate,
    ) -> Stream<RootCircuit, OrdIndexedZSet<K, A>>
    where
        A: DBData,
        FInit: Fn(&V, ZWeight) -> A + 'static,
        FUpdate: Fn(A, &V, ZWeight) -> A + 'static,
    {
        let input_factories = BatchReaderFactories::new::<K, V, ZWeight>();
        let output_factories = BatchReaderFactories::new::<K, A, ZWeight>();

        self.inner()
            .dyn_chain_aggregate_mono(
                persistent_id,
                &input_factories,
                &output_factories,
                Box::new(move |acc: &mut DynData, v: &DynData, w: ZWeight| unsafe {
                    *acc.downcast_mut() = finit(v.downcast(), w)
                }),
                Box::new(move |acc: &mut DynData, v: &DynData, w: ZWeight| unsafe {
                    *acc.downcast_mut() =
                        fupdate(mem::take(acc.downcast_mut::<A>()), v.downcast(), w)
                }),
            )
            .typed()
    }
}
