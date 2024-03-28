use crate::typed_batch::DynFileZSet;
use crate::{
    dynamic::{DataTrait, Erase, WeightTrait},
    trace::BatchReaderFactories,
    typed_batch::{
        Batch, BatchReader, DynOrdIndexedWSet, DynOrdWSet, OrdIndexedWSet, OrdWSet, TypedBatch,
    },
    Circuit, DBData, DBWeight, Stream,
};

/// This trait abstracts away a stream of records that can be filtered
/// and transformed on a record-by-record basis.
///
/// The notion of a record is determined by each implementer and can be either
/// a `(key, value)` tuple for a stream of key/value pairs or just `key`
/// for a stream of singleton values.
///
/// # Background
///
/// DBSP represents relational data using the [`Batch`](`crate::Batch`)
/// trait. A batch is conceptually a set of `(key, value, weight)` tuples.  When
/// processing batches, we often need to filter and/or transform tuples one at a
/// time with a user-provided closure, e.g., we may want to create a batch with
/// only the tuples that satisfy a predicate of the form `Fn(K, V) -> bool`.
///
/// In practice we often work with specialized implementations of `Batch` where
/// the value type is `()`, e.g., [`OrdZSet`](`crate::OrdZSet`).  We call such
/// batches **non-indexed batches**, in contrast to **indexed batches** like
/// [`OrdIndexedZSet`](`crate::OrdIndexedZSet`), which support arbitrary `value`
/// types.  When filtering or transforming non-indexed batches we want to ignore
/// the value and work with keys only, e.g., to write filtering predicates of
/// the form `Fn(K) -> bool` rather than `Fn(K, ()) -> bool`.
///
/// This trait enables both use cases by allowing the implementer to define
/// their own record type, which can be either `(K, V)` or `K`.
///
/// These methods are equally suitable for [streams of data or streams of
/// deltas](Stream#data-streams-versus-delta-streams).
///
/// This trait uses the same [naming
/// convention](Stream#operator-naming-convention) as [`Stream`], for `_index`
/// and `_generic` suffixes.
pub trait FilterMap: BatchReader + Sized {
    /// A borrowed version of the record type, e.g., `(&K, &V)` for a stream of
    /// `(key, value, weight)` tuples or `&K` if the value type is `()`.
    type ItemRef<'a>;

    fn filter<C: Circuit, F>(stream: &Stream<C, Self>, filter_func: F) -> Stream<C, Self>
    where
        F: Fn(Self::ItemRef<'_>) -> bool + 'static;

    fn map_generic<C: Circuit, F, K, V, O>(stream: &Stream<C, Self>, map_func: F) -> Stream<C, O>
    where
        K: DBData + Erase<O::DynK>,
        V: DBData + Erase<O::DynV>,
        F: Fn(Self::ItemRef<'_>) -> (K, V) + 'static,
        O: Batch<Key = K, Val = V, Time = (), R = Self::R, DynR = Self::DynR>;

    fn flat_map_generic<C: Circuit, F, K, V, I, O>(
        stream: &Stream<C, Self>,
        func: F,
    ) -> Stream<C, O>
    where
        K: DBData + Erase<O::DynK>,
        V: DBData + Erase<O::DynV>,
        F: FnMut(Self::ItemRef<'_>) -> I + 'static,
        I: IntoIterator<Item = (K, V)> + 'static,
        O: Batch<Key = K, Val = V, Time = (), R = Self::R, DynR = Self::DynR> + Clone + 'static;
}

impl<C: Circuit, B: FilterMap> Stream<C, B> {
    /// Filter input stream only retaining records that satisfy the
    /// `filter_func` predicate.
    pub fn filter<F>(&self, filter_func: F) -> Self
    where
        F: Fn(B::ItemRef<'_>) -> bool + 'static,
    {
        FilterMap::filter(self, filter_func)
    }

    /// Applies `map_func` to each record in the input stream.  Assembles output
    /// record into `OrdZSet` batches.
    pub fn map<F, K>(&self, map_func: F) -> Stream<C, OrdWSet<K, B::R, B::DynR>>
    where
        K: DBData,
        F: Fn(B::ItemRef<'_>) -> K + Clone + 'static,
    {
        FilterMap::map_generic(self, move |item| (map_func(item), ()))
    }

    /// Behaves as [`Self::map`] followed by [`index`](`crate::Stream::index`),
    /// but is more efficient.  Assembles output records into
    /// `OrdIndexedZSet` batches.
    pub fn map_index<F, K, V>(&self, map_func: F) -> Stream<C, OrdIndexedWSet<K, V, B::R, B::DynR>>
    where
        K: DBData,
        V: DBData,
        F: Fn(B::ItemRef<'_>) -> (K, V) + 'static,
    {
        FilterMap::map_generic(self, map_func)
    }

    /// Like [`Self::map_index`], but can return any batch type.
    pub fn map_generic<F, K, V, O>(&self, map_func: F) -> Stream<C, O>
    where
        K: DBData + Erase<O::DynK>,
        V: DBData + Erase<O::DynV>,
        F: Fn(B::ItemRef<'_>) -> (K, V) + 'static,
        O: Batch<Key = K, Val = V, Time = (), R = B::R, DynR = B::DynR>,
    {
        FilterMap::map_generic(self, map_func)
    }

    /// Applies `func` to each record in the input stream.  Assembles output
    /// records into `OrdZSet` batches.
    ///
    /// The output of `func` can be any type that implements `trait
    /// IntoIterator`, e.g., `Option<>` or `Vec<>`.
    pub fn flat_map<F, I>(&self, mut func: F) -> Stream<C, OrdWSet<I::Item, B::R, B::DynR>>
    where
        F: FnMut(B::ItemRef<'_>) -> I + 'static,
        I: IntoIterator + 'static,
        I::Item: DBData,
    {
        FilterMap::flat_map_generic(self, move |item| func(item).into_iter().map(|x| (x, ())))
    }

    /// Behaves as [`Self::flat_map`] followed by
    /// [`index`](`crate::Stream::index`), but is more efficient.  Assembles
    /// output records into `OrdIndexedZSet` batches.
    pub fn flat_map_index<F, K, V, I>(
        &self,
        func: F,
    ) -> Stream<C, OrdIndexedWSet<K, V, B::R, B::DynR>>
    where
        F: FnMut(B::ItemRef<'_>) -> I + 'static,
        I: IntoIterator<Item = (K, V)> + 'static,
        K: DBData,
        V: DBData,
    {
        FilterMap::flat_map_generic(self, func)
    }

    /// Like [`Self::flat_map_index`], but can return any batch type.
    pub fn flat_map_generic<F, K, V, I, O>(&self, func: F) -> Stream<C, O>
    where
        K: DBData + Erase<O::DynK>,
        V: DBData + Erase<O::DynV>,
        F: FnMut(B::ItemRef<'_>) -> I + 'static,
        I: IntoIterator<Item = (K, V)> + 'static,
        O: Batch<Key = K, Val = V, Time = (), R = B::R, DynR = B::DynR> + Clone + 'static,
    {
        FilterMap::flat_map_generic(self, func)
    }
}

impl<K, DynK, R, DynR> FilterMap for TypedBatch<K, (), R, DynOrdWSet<DynK, DynR>>
where
    K: DBData + Erase<DynK>,
    DynK: DataTrait + ?Sized,
    R: DBWeight + Erase<DynR>,
    DynR: WeightTrait + ?Sized,
{
    type ItemRef<'a> = &'a K;

    fn filter<C: Circuit, F>(stream: &Stream<C, Self>, filter_func: F) -> Stream<C, Self>
    where
        F: Fn(Self::ItemRef<'_>) -> bool + 'static,
    {
        let filter_func: Box<dyn Fn(&Self::DynK) -> bool> =
            Box::new(move |k| filter_func(unsafe { k.downcast() }));

        stream.inner().dyn_filter(filter_func).typed()
    }

    fn map_generic<C: Circuit, F, KT, V, O>(stream: &Stream<C, Self>, map_func: F) -> Stream<C, O>
    where
        KT: DBData + Erase<O::DynK>,
        V: DBData + Erase<O::DynV>,
        F: Fn(Self::ItemRef<'_>) -> (KT, V) + 'static,
        O: Batch<Key = KT, Val = V, Time = (), R = R, DynR = DynR>,
    {
        let factories = BatchReaderFactories::new::<KT, V, R>();

        stream
            .inner()
            .dyn_map_generic(
                &factories,
                Box::new(move |item, pair| {
                    let (mut key, mut val) = map_func(unsafe { item.downcast() });
                    pair.from_vals(key.erase_mut(), val.erase_mut());
                }),
            )
            .typed()
    }

    fn flat_map_generic<C: Circuit, F, KT, VT, I, O>(
        stream: &Stream<C, Self>,
        mut func: F,
    ) -> Stream<C, O>
    where
        KT: DBData + Erase<O::DynK>,
        VT: DBData + Erase<O::DynV>,
        F: FnMut(Self::ItemRef<'_>) -> I + 'static,
        I: IntoIterator<Item = (KT, VT)> + 'static,
        O: Batch<Key = KT, Val = VT, Time = (), R = R, DynR = DynR>,
    {
        let factories = BatchReaderFactories::new::<O::Key, O::Val, R>();

        stream
            .inner()
            .dyn_flat_map_generic(
                &factories,
                Box::new(move |item: &Self::DynK, cb| {
                    for (mut k, mut v) in func(unsafe { item.downcast() }) {
                        cb(k.erase_mut(), v.erase_mut());
                    }
                }),
            )
            .typed()
    }
}

impl<K, DynK, V, DynV, R, DynR> FilterMap
    for TypedBatch<K, V, R, DynOrdIndexedWSet<DynK, DynV, DynR>>
where
    K: DBData + Erase<DynK>,
    DynK: DataTrait + ?Sized,
    V: DBData + Erase<DynV>,
    DynV: DataTrait + ?Sized,
    R: DBWeight + Erase<DynR>,
    DynR: WeightTrait + ?Sized,
{
    type ItemRef<'a> = (&'a K, &'a V);

    fn filter<C: Circuit, F>(stream: &Stream<C, Self>, filter_func: F) -> Stream<C, Self>
    where
        F: Fn(Self::ItemRef<'_>) -> bool + 'static,
    {
        stream
            .inner()
            .dyn_filter(Box::new(move |(k, v)| unsafe {
                filter_func((k.downcast(), v.downcast()))
            }))
            .typed()
    }

    fn map_generic<C: Circuit, F, KT, VT, O>(stream: &Stream<C, Self>, map_func: F) -> Stream<C, O>
    where
        KT: DBData + Erase<O::DynK>,
        VT: DBData + Erase<O::DynV>,
        F: Fn(Self::ItemRef<'_>) -> (KT, VT) + 'static,
        O: Batch<Key = KT, Val = VT, Time = (), R = R, DynR = DynR>,
    {
        let factories = BatchReaderFactories::new::<KT, VT, R>();

        stream
            .inner()
            .dyn_map_generic(
                &factories,
                Box::new(move |(k, v), pair| {
                    let (mut key, mut val) = unsafe { map_func((k.downcast(), v.downcast())) };
                    pair.from_vals(key.erase_mut(), val.erase_mut());
                }),
            )
            .typed()
    }

    fn flat_map_generic<C: Circuit, F, KT, VT, I, O>(
        stream: &Stream<C, Self>,
        mut func: F,
    ) -> Stream<C, O>
    where
        KT: DBData + Erase<O::DynK>,
        VT: DBData + Erase<O::DynV>,
        F: FnMut(Self::ItemRef<'_>) -> I + 'static,
        I: IntoIterator<Item = (KT, VT)> + 'static,
        O: Batch<Key = KT, Val = VT, Time = (), R = R, DynR = DynR>,
    {
        let factories = BatchReaderFactories::new::<KT, VT, R>();

        stream
            .inner()
            .dyn_flat_map_generic(
                &factories,
                Box::new(move |(k, v), cb| {
                    for (mut k, mut v) in unsafe { func((k.downcast(), v.downcast())) } {
                        cb(k.erase_mut(), v.erase_mut());
                    }
                }),
            )
            .typed()
    }
}

impl<K, DynK, R, DynR> FilterMap for TypedBatch<K, (), R, DynFileZSet<DynK, DynR>>
where
    K: DBData + Erase<DynK>,
    DynK: DataTrait + ?Sized,
    R: DBWeight + Erase<DynR>,
    DynR: WeightTrait + ?Sized,
{
    type ItemRef<'a> = &'a K;

    fn filter<C: Circuit, F>(stream: &Stream<C, Self>, filter_func: F) -> Stream<C, Self>
    where
        F: Fn(Self::ItemRef<'_>) -> bool + 'static,
    {
        let filter_func: Box<dyn Fn(&Self::DynK) -> bool> =
            Box::new(move |k| filter_func(unsafe { k.downcast() }));

        stream.inner().dyn_filter(filter_func).typed()
    }

    fn map_generic<C: Circuit, F, KT, V, O>(stream: &Stream<C, Self>, map_func: F) -> Stream<C, O>
    where
        KT: DBData + Erase<O::DynK>,
        V: DBData + Erase<O::DynV>,
        F: Fn(Self::ItemRef<'_>) -> (KT, V) + 'static,
        O: Batch<Key = KT, Val = V, Time = (), R = R, DynR = DynR>,
    {
        let factories = BatchReaderFactories::new::<KT, V, R>();

        stream
            .inner()
            .dyn_map_generic(
                &factories,
                Box::new(move |item, pair| {
                    let (mut key, mut val) = map_func(unsafe { item.downcast() });
                    pair.from_vals(key.erase_mut(), val.erase_mut());
                }),
            )
            .typed()
    }

    fn flat_map_generic<C: Circuit, F, KT, VT, I, O>(
        stream: &Stream<C, Self>,
        mut func: F,
    ) -> Stream<C, O>
    where
        KT: DBData + Erase<O::DynK>,
        VT: DBData + Erase<O::DynV>,
        F: FnMut(Self::ItemRef<'_>) -> I + 'static,
        I: IntoIterator<Item = (KT, VT)> + 'static,
        O: Batch<Key = KT, Val = VT, Time = (), R = R, DynR = DynR>,
    {
        let factories = BatchReaderFactories::new::<O::Key, O::Val, R>();

        stream
            .inner()
            .dyn_flat_map_generic(
                &factories,
                Box::new(move |item: &Self::DynK, cb| {
                    for (mut k, mut v) in unsafe { func(item.downcast()) } {
                        cb(k.erase_mut(), v.erase_mut());
                    }
                }),
            )
            .typed()
    }
}
