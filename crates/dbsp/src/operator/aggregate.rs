use std::mem::take;

use dyn_clone::clone_box;

use crate::{
    algebra::MulByRef,
    circuit::{metadata::MetaItem, WithClock},
    dynamic::{ClonableTrait, DowncastTrait, DynData, DynUnit, DynWeight, Erase},
    operator::dynamic::aggregate::{
        Aggregator, DynAggregatorImpl, IncAggregateFactories, IncAggregateLinearFactories,
        StreamAggregateFactories, StreamLinearAggregateFactories,
    },
    storage::file::Deserializable,
    trace::{BatchReaderFactories, Filter},
    typed_batch::{Batch, BatchReader, DynOrdIndexedZSet, IndexedZSet, OrdIndexedZSet, OrdWSet},
    Circuit, DBData, DBWeight, DynZWeight, RootCircuit, Stream, TypedBox, ZWeight,
};

impl<C, K, V> Stream<C, OrdIndexedZSet<K, V>>
where
    C: Circuit,
    K: DBData,
    <K as Deserializable>::ArchivedDeser: Ord,
    V: DBData,
{
    /// Incremental aggregation operator.
    ///
    /// This operator is an incremental version of
    /// [`Self::stream_aggregate`]. It transforms a stream of changes to
    /// an indexed Z-set to a stream of changes to its aggregate computed by
    /// applying `aggregator` to each key in the input.
    ///
    /// [`Min`](`crate::operator::Min`), [`crate::operator::Max`], and
    /// [`Fold`](`crate::operator::Fold`) are provided as example `Aggregator`s.
    #[cfg(not(feature = "backend-mode"))]
    #[allow(clippy::type_complexity)]
    pub fn aggregate<A>(&self, aggregator: A) -> Stream<C, OrdIndexedZSet<K, A::Output>>
    where
        A: Aggregator<V, <C as WithClock>::Time, ZWeight>,
    {
        let aggregate_factories = IncAggregateFactories::new::<K, V, ZWeight, A::Output>();

        let dyn_aggregator = DynAggregatorImpl::<
            DynData,
            V,
            C::Time,
            DynZWeight,
            ZWeight,
            A,
            DynData,
            DynData,
        >::new(aggregator);

        self.inner()
            .dyn_aggregate(None, &aggregate_factories, &dyn_aggregator)
            .typed()
    }
}

impl<C, Z> Stream<C, Z>
where
    C: Circuit,
{
    /// Aggregate values associated with each key in an indexed Z-set.
    ///
    /// An indexed Z-set `IndexedZSet<K, V, R>` maps each key into a
    /// set of `(value, weight)` tuples `(V, R)`.  These tuples form
    /// a nested Z-set `ZSet<V, R>`.  This method applies `aggregator`
    /// to each such Z-set and adds it to the output indexed Z-set with
    /// weight `+1`.
    ///
    /// [`Min`](`crate::operator::Min`), [`Max`](`crate::operator::Max`),
    /// and [`Fold`](`crate::operator::Fold`) are provided as example
    /// `Aggregator`s.
    #[allow(clippy::type_complexity)]
    pub fn stream_aggregate<A>(&self, aggregator: A) -> Stream<C, OrdIndexedZSet<Z::Key, A::Output>>
    where
        Z: IndexedZSet<DynK = DynData>,
        Z::InnerBatch: Send,
        A: Aggregator<Z::Val, (), ZWeight>,
    {
        let factories: StreamAggregateFactories<
            <Z as BatchReader>::Inner,
            <OrdIndexedZSet<Z::Key, A::Output> as BatchReader>::Inner,
        > = StreamAggregateFactories::new::<Z::Key, Z::Val, ZWeight, A::Output>();

        self.inner()
            .dyn_stream_aggregate(
                &factories,
                &DynAggregatorImpl::<Z::DynV, Z::Val, (), DynZWeight, ZWeight, A, DynData, DynData>::new(aggregator),
            )
            .typed()
    }

    /// Like [`Self::stream_aggregate`], but can return any batch type.
    pub fn stream_aggregate_generic<A, O>(&self, aggregator: A) -> Stream<C, O>
    where
        Z: Batch<Time = ()>,
        Z::InnerBatch: Send,
        A: Aggregator<Z::Val, (), Z::R>,
        A::Output: Erase<O::DynV>,
        O: IndexedZSet<Key = Z::Key, DynK = Z::DynK, Val = A::Output>,
    {
        let factories: StreamAggregateFactories<
            <Z as BatchReader>::Inner,
            <O as BatchReader>::Inner,
        > = StreamAggregateFactories::new::<Z::Key, Z::Val, Z::R, A::Output>();

        self.inner()
            .dyn_stream_aggregate_generic(
                &factories,
                &DynAggregatorImpl::<Z::DynV, Z::Val, (), Z::DynR, Z::R, A, DynData, O::DynV>::new(
                    aggregator,
                ),
            )
            .typed()
    }

    /// A version of [`Self::dyn_stream_aggregate`] optimized for linear
    /// aggregation functions.
    ///
    /// This method only works for linear aggregation functions `f`, i.e.,
    /// functions that satisfy `f(a+b) = f(a) + f(b)`, where the first "+"
    /// is the zset union of zsets composed of tuples a and b.
    /// This function will will produce incorrect results if `f` is not linear.
    /// The input stream is ZSet of (key, value) pairs, but the function
    /// only receives the "value" part as an input.
    pub fn stream_aggregate_linear<F, A>(&self, f: F) -> Stream<C, OrdIndexedZSet<Z::Key, A>>
    where
        Z: IndexedZSet<DynK = DynData>,
        A: DBWeight + MulByRef<ZWeight, Output = A>,
        F: Fn(&Z::Val) -> A + Clone + 'static,
    {
        let factories: StreamLinearAggregateFactories<
            Z::Inner,
            DynWeight,
            DynOrdIndexedZSet<DynData, DynData>,
        > = StreamLinearAggregateFactories::new::<Z::Key, Z::Val, A, A>();

        self.inner()
            .dyn_stream_aggregate_linear_generic(
                &factories,
                Box::new(move |_k, v, r, acc| unsafe {
                    *acc.downcast_mut() = f(v.downcast()).mul_by_ref(&**r)
                }),
                Box::new(|w, out| w.as_data_mut().move_to(out)),
            )
            .typed()
    }

    /// Like [`Self::stream_aggregate_linear`], but can return any batch
    /// type.
    pub fn stream_aggregate_linear_generic<F, A, O>(&self, f: F) -> Stream<C, O>
    where
        Z: IndexedZSet,
        O: IndexedZSet<Key = Z::Key, DynK = Z::DynK, Val = A, DynV = DynData>,
        A: DBWeight + MulByRef<ZWeight, Output = A>,
        F: Fn(&Z::Val) -> A + Clone + 'static,
    {
        let factories: StreamLinearAggregateFactories<Z::Inner, DynWeight, O::Inner> =
            StreamLinearAggregateFactories::new::<Z::Key, Z::Val, A, A>();

        self.inner()
            .dyn_stream_aggregate_linear_generic(
                &factories,
                Box::new(move |_k, v, r, acc| unsafe {
                    *acc.downcast_mut() = f(v.downcast()).mul_by_ref(&**r)
                }),
                Box::new(|w, out| w.as_data_mut().move_to(out)),
            )
            .typed()
    }

    /// Like [`Self::dyn_aggregate`], but can return any batch type.
    pub fn aggregate_generic<A, O>(&self, aggregator: A) -> Stream<C, O>
    where
        Z: Batch<Time = ()> + std::fmt::Debug,
        Z::InnerBatch: Send,
        A: Aggregator<Z::Val, <C as WithClock>::Time, Z::R>,
        O: IndexedZSet<Key = Z::Key, DynK = Z::DynK, Val = DynData>,
        A::Output: Erase<O::DynV>,
        <Z::Key as Deserializable>::ArchivedDeser: Ord,
    {
        let factories: IncAggregateFactories<Z::Inner, O::Inner, C::Time> =
            IncAggregateFactories::new::<Z::Key, Z::Val, Z::R, A::Output>();

        self.inner()
            .dyn_aggregate_generic(
                None,
                &factories,
                &DynAggregatorImpl::<Z::DynV, Z::Val, _, Z::DynR, Z::R, _, DynData, O::DynV>::new(
                    aggregator,
                ),
            )
            .typed()
    }

    /// A version of [`Self::aggregate`] optimized for linear
    /// aggregation functions.
    ///
    /// This method only works for linear aggregation functions `f`, i.e.,
    /// functions that satisfy `f(a+b) = f(a) + f(b)`, where the first "+"
    /// is zset union of the zsets composed of tuples a and b.
    /// This function will produce
    /// incorrect results if `f` is not linear.  The input of
    /// `aggregate_linear` is an indexed Zset, but the function `f` is only
    /// applied to the values, ignoring the keys.
    #[track_caller]
    pub fn aggregate_linear<F, A>(&self, f: F) -> Stream<C, OrdIndexedZSet<Z::Key, A>>
    where
        Z: IndexedZSet<DynK = DynData>,
        A: DBWeight + MulByRef<ZWeight, Output = A>,
        F: Fn(&Z::Val) -> A + Clone + 'static,
        <Z::Key as Deserializable>::ArchivedDeser: Ord,
    {
        let factories: IncAggregateLinearFactories<
            Z::Inner,
            DynWeight,
            DynOrdIndexedZSet<DynData, DynData>,
            C::Time,
        > = IncAggregateLinearFactories::new::<Z::Key, A, A>();

        self.inner()
            .dyn_aggregate_linear_generic(
                None,
                &factories,
                Box::new(move |_k, v, r, acc| unsafe {
                    *acc.downcast_mut::<A>() = f(v.downcast::<Z::Val>()).mul_by_ref(&**r)
                }),
                Box::new(|w, out| w.as_data_mut().move_to(out)),
            )
            .typed()
    }

    /// Linear aggregation with a post-processing step.
    ///
    /// Equivalent to `self.aggregate_linear(f).map_index(|(k,v)|(k, of(v)))`,
    /// but more efficient.  This operator is incremental.
    #[cfg(not(feature = "backend-mode"))]
    #[track_caller]
    pub fn aggregate_linear_postprocess<F, A, OF, OV>(
        &self,
        f: F,
        of: OF,
    ) -> Stream<C, OrdIndexedZSet<Z::Key, OV>>
    where
        Z: IndexedZSet<DynK = DynData>,
        A: DBWeight + MulByRef<ZWeight, Output = A>,
        OV: DBData,
        F: Fn(&Z::Val) -> A + Clone + 'static,
        OF: Fn(A) -> OV + Clone + 'static,
        <Z::Key as Deserializable>::ArchivedDeser: Ord,
    {
        let factories: IncAggregateLinearFactories<
            Z::Inner,
            DynWeight,
            DynOrdIndexedZSet<DynData, DynData>,
            C::Time,
        > = IncAggregateLinearFactories::new::<Z::Key, A, OV>();

        self.inner()
            .dyn_aggregate_linear_generic(
                None,
                &factories,
                Box::new(move |_k, v, r, acc| unsafe {
                    *acc.downcast_mut::<A>() = f(v.downcast::<Z::Val>()).mul_by_ref(&**r)
                }),
                Box::new(move |w, out| unsafe {
                    *out.downcast_mut::<OV>() = of(take(w.downcast_mut::<A>()))
                }),
            )
            .typed()
    }

    /// Convert indexed Z-set `Z` into a Z-set where the weight of each key
    /// is computed as:
    ///
    /// ```text
    ///    __
    ///    ╲
    ///    ╱ f(k,v) * w
    ///    ‾‾
    /// (k,v,w) ∈ Z
    /// ```
    ///
    /// Discards the values from the input.
    ///
    /// This is a linear operator.
    pub fn weigh<F, T>(&self, f: F) -> Stream<C, OrdWSet<Z::Key, T, DynWeight>>
    where
        Z: IndexedZSet<DynK = DynData>,
        F: Fn(&Z::Key, &Z::Val) -> T + 'static,
        T: DBWeight + MulByRef<ZWeight, Output = T>,
    {
        self.inner()
            .dyn_weigh(
                &BatchReaderFactories::new::<Z::Key, (), T>(),
                Box::new(move |k, v, r, acc: &mut DynWeight| unsafe {
                    *acc.downcast_mut() = f(k.downcast(), v.downcast()).mul_by_ref(r.downcast())
                }),
            )
            .typed()
    }

    /// Like [`Self::weigh`], but can return any batch type.
    pub fn weigh_generic<F, T, O>(&self, f: F) -> Stream<C, O>
    where
        Z: IndexedZSet,
        F: Fn(&Z::Key, &Z::Val) -> T + 'static,
        O: Batch<
            Key = Z::Key,
            DynK = Z::DynK,
            Val = (),
            DynV = DynUnit,
            Time = (),
            DynR = DynWeight,
        >,
        T: DBWeight + MulByRef<ZWeight, Output = T>,
    {
        self.inner()
            .dyn_weigh_generic(
                &BatchReaderFactories::new::<Z::Key, (), O::R>(),
                Box::new(move |k, v, r, acc: &mut DynWeight| unsafe {
                    *acc.downcast_mut() = f(k.downcast(), v.downcast()).mul_by_ref(r.downcast())
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
    /// Like `aggregate_linear_postprocess`, but additionally applies `waterline` to the internal integral
    ///
    /// See `aggregate_linear_retain_keys` for details.
    #[track_caller]
    pub fn aggregate_linear_postprocess_retain_keys<F, A, OF, OV, TS, RK>(
        &self,
        waterline: &Stream<RootCircuit, TypedBox<TS, DynData>>,
        retain_key_func: RK,
        f: F,
        of: OF,
    ) -> Stream<RootCircuit, OrdIndexedZSet<K, OV>>
    where
        A: DBWeight + MulByRef<ZWeight, Output = A>,
        OV: DBData,
        TS: DBData,
        RK: Fn(&K, &TS) -> bool + Clone + Send + Sync + 'static,
        F: Fn(&V) -> A + Clone + 'static,
        OF: Fn(A) -> OV + Clone + 'static,
    {
        self.aggregate_linear_postprocess_retain_keys_persistent::<F, A, OF, OV, TS, RK>(
            None,
            waterline,
            retain_key_func,
            f,
            of,
        )
    }

    #[track_caller]
    pub fn aggregate_linear_postprocess_retain_keys_persistent<F, A, OF, OV, TS, RK>(
        &self,
        persistent_id: Option<&str>,
        waterline: &Stream<RootCircuit, TypedBox<TS, DynData>>,
        retain_key_func: RK,
        f: F,
        of: OF,
    ) -> Stream<RootCircuit, OrdIndexedZSet<K, OV>>
    where
        A: DBWeight + MulByRef<ZWeight, Output = A>,
        OV: DBData,
        TS: DBData,
        RK: Fn(&K, &TS) -> bool + Clone + Send + Sync + 'static,
        F: Fn(&V) -> A + Clone + 'static,
        OF: Fn(A) -> OV + Clone + 'static,
    {
        let factories: IncAggregateLinearFactories<
            DynOrdIndexedZSet<DynData, DynData>,
            DynWeight,
            DynOrdIndexedZSet<DynData, DynData>,
            (),
        > = IncAggregateLinearFactories::new::<K, A, OV>();

        self.inner()
            .dyn_aggregate_linear_retain_keys_mono(
                persistent_id,
                &factories,
                &waterline.inner_data(),
                Box::new(move |ts| {
                    let metadata = MetaItem::String(format!("{ts:?}"));
                    let ts = clone_box(ts);
                    let retain_key_func = retain_key_func.clone();
                    Filter::new(Box::new(move |k: &DynData| {
                        retain_key_func(unsafe { k.downcast::<K>() }, unsafe {
                            ts.as_ref().downcast::<TS>()
                        })
                    }))
                    .with_metadata(metadata)
                }),
                Box::new(move |_k, v, r, acc| unsafe {
                    *acc.downcast_mut::<A>() = f(v.downcast::<V>()).mul_by_ref(&**r)
                }),
                Box::new(move |w, out| unsafe {
                    *out.downcast_mut::<OV>() = of(take(w.downcast_mut::<A>()))
                }),
            )
            .typed()
    }

    /// Like `aggregate_linear`, but additionally applies `waterline` to the internal integral
    ///
    /// The linear aggregate operator internally invokes the regular aggregation operator, which
    /// creates two integrals: for input and output streams.  The latter stream is returned as
    /// output of this operator and can be GC'd using regular means (integrate_trace_retain_keys),
    /// but the former integral is internal to this operator.  When aggregating a stream that has
    /// a waterline, use this function to GC keys in the internal stream that fall below the waterline.
    #[track_caller]
    pub fn aggregate_linear_retain_keys<F, A, TS, RK>(
        &self,
        waterline: &Stream<RootCircuit, TypedBox<TS, DynData>>,
        retain_key_func: RK,
        f: F,
    ) -> Stream<RootCircuit, OrdIndexedZSet<K, A>>
    where
        A: DBWeight + MulByRef<ZWeight, Output = A>,
        TS: DBData,
        RK: Fn(&K, &TS) -> bool + Clone + Send + Sync + 'static,
        F: Fn(&V) -> A + Clone + 'static,
    {
        let factories: IncAggregateLinearFactories<
            DynOrdIndexedZSet<DynData, DynData>,
            DynWeight,
            DynOrdIndexedZSet<DynData, DynData>,
            (),
        > = IncAggregateLinearFactories::new::<K, A, A>();

        self.inner()
            .dyn_aggregate_linear_retain_keys_mono(
                None,
                &factories,
                &waterline.inner_data(),
                Box::new(move |ts| {
                    let metadata = MetaItem::String(format!("{ts:?}"));
                    let ts = clone_box(ts);
                    let retain_key_func = retain_key_func.clone();
                    Filter::new(Box::new(move |k: &DynData| {
                        retain_key_func(unsafe { k.downcast::<K>() }, unsafe {
                            ts.as_ref().downcast::<TS>()
                        })
                    }))
                    .with_metadata(metadata)
                }),
                Box::new(move |_k, v, r, acc| unsafe {
                    *acc.downcast_mut::<A>() = f(v.downcast::<V>()).mul_by_ref(&**r)
                }),
                Box::new(|w, out| w.as_data_mut().move_to(out)),
            )
            .typed()
    }
}
