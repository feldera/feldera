use crate::{
    dynamic::{DowncastTrait, DynData, Erase},
    operator::dynamic::join_range::StreamJoinRangeFactories,
    typed_batch::{
        DynOrdIndexedZSet, DynOrdZSet, IndexedZSet, IndexedZSetReader, OrdIndexedZSet, OrdZSet,
    },
    Circuit, DBData, Stream,
};

impl<C, I1> Stream<C, I1>
where
    I1: IndexedZSetReader,
    I1::Inner: Clone,
    C: Circuit,
{
    /// Range-join two streams into an `OrdZSet`.
    ///
    /// This operator is non-incremental, i.e., it joins the pair of batches it
    /// receives at each timestamp ignoring previous inputs.
    pub fn stream_join_range<I2, RF, JF, It>(
        &self,
        other: &Stream<C, I2>,
        range_func: RF,
        join_func: JF,
    ) -> Stream<C, OrdZSet<It::Item>>
    where
        I2: IndexedZSetReader,
        I2::Inner: Clone,
        RF: Fn(&I1::Key) -> (I2::Key, I2::Key) + 'static,
        JF: Fn(&I1::Key, &I1::Val, &I2::Key, &I2::Val) -> It + 'static,
        It: IntoIterator + 'static,
        It::Item: DBData,
    {
        let factories = StreamJoinRangeFactories::<I2::Inner, DynOrdZSet<DynData>>::new::<
            I2::Key,
            I2::Val,
            It::Item,
            (),
        >();

        self.inner()
            .dyn_stream_join_range(
                &factories,
                &other.inner(),
                Box::new(move |k1, from, to| unsafe {
                    let (from_tmp, to_tmp) = range_func(k1.downcast());
                    *from.downcast_mut() = from_tmp;
                    *to.downcast_mut() = to_tmp;
                }),
                Box::new(move |k1, v1, k2, v2, cb| {
                    for mut v in unsafe {
                        join_func(k1.downcast(), v1.downcast(), k2.downcast(), v2.downcast())
                    }
                    .into_iter()
                    {
                        cb(v.erase_mut(), ().erase_mut());
                    }
                }),
            )
            .typed()
    }

    /// Range-join two streams into an `OrdIndexedZSet`.
    ///
    /// See module documentation for the definition of the range-join operator
    /// and its arguments.
    ///
    /// In this version of the operator, the `join_func` closure returns
    /// an iterator over `(key, value)` pairs used to assemble the output
    /// indexed Z-set.
    ///
    /// This operator is non-incremental, i.e., it joins the pair of batches it
    /// receives at each timestamp ignoring previous inputs.
    pub fn stream_join_range_index<I2, K, V, RF, JF, It>(
        &self,
        other: &Stream<C, I2>,
        range_func: RF,
        join_func: JF,
    ) -> Stream<C, OrdIndexedZSet<K, V>>
    where
        I2: IndexedZSetReader,
        I2::Inner: Clone,
        K: DBData,
        V: DBData,
        RF: Fn(&I1::Key) -> (I2::Key, I2::Key) + 'static,
        JF: Fn(&I1::Key, &I1::Val, &I2::Key, &I2::Val) -> It + 'static,
        It: IntoIterator<Item = (K, V)> + 'static,
    {
        let factories =
            StreamJoinRangeFactories::<I2::Inner, DynOrdIndexedZSet<DynData, DynData>>::new::<
                I2::Key,
                I2::Val,
                K,
                V,
            >();

        self.inner()
            .dyn_stream_join_range_index(
                &factories,
                &other.inner(),
                Box::new(move |k1, from, to| unsafe {
                    let (from_tmp, to_tmp) = range_func(k1.downcast());
                    *from.downcast_mut() = from_tmp;
                    *to.downcast_mut() = to_tmp;
                }),
                Box::new(move |k1, v1, k2, v2, cb| {
                    for (mut k, mut v) in unsafe {
                        join_func(k1.downcast(), v1.downcast(), k2.downcast(), v2.downcast())
                    }
                    .into_iter()
                    {
                        cb(k.erase_mut(), v.erase_mut());
                    }
                }),
            )
            .typed()
    }

    /// Like [`Self::dyn_stream_join_range`], but can return any indexed Z-set
    /// type.
    pub fn stream_join_range_generic<I2, K, V, RF, JF, It, O>(
        &self,
        other: &Stream<C, I2>,
        range_func: RF,
        join_func: JF,
    ) -> Stream<C, O>
    where
        I2: IndexedZSetReader,
        I2::Inner: Clone,
        K: DBData + Erase<O::DynK>,
        V: DBData + Erase<O::DynV>,
        RF: Fn(&I1::Key) -> (I2::Key, I2::Key) + 'static,
        JF: Fn(&I1::Key, &I1::Val, &I2::Key, &I2::Val) -> It + 'static,
        It: IntoIterator<Item = (K, V)> + 'static,
        O: IndexedZSet<Key = K, Val = V>,
    {
        let factories =
            StreamJoinRangeFactories::<I2::Inner, O::Inner>::new::<I2::Key, I2::Val, K, V>();

        self.inner()
            .dyn_stream_join_range_generic(
                &factories,
                &other.inner(),
                Box::new(move |k1, from, to| unsafe {
                    let (from_tmp, to_tmp) = range_func(k1.downcast());
                    *from.downcast_mut() = from_tmp;
                    *to.downcast_mut() = to_tmp;
                }),
                Box::new(move |k1, v1, k2, v2, cb| {
                    for (mut k, mut v) in unsafe {
                        join_func(k1.downcast(), v1.downcast(), k2.downcast(), v2.downcast())
                    }
                    .into_iter()
                    {
                        cb(k.erase_mut(), v.erase_mut());
                    }
                }),
            )
            .typed()
    }
}
