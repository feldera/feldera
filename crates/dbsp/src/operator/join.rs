use crate::{
    dynamic::{DowncastTrait, DynData, DynUnit, Erase},
    operator::dynamic::{
        filter_map::DynFilterMap,
        join::{
            AntijoinFactories, JoinFactories, OuterJoinFactories, StreamJoinFactories,
            TraceJoinFuncs,
        },
    },
    trace::Spillable,
    typed_batch::{IndexedZSet, OrdIndexedZSet, OrdZSet, ZSet},
    Circuit, DBData, Stream,
};

fn mk_trace_join_funcs<I1, I2, Z, F>(
    join: F,
) -> TraceJoinFuncs<I1::DynK, I1::DynV, I2::DynV, Z::DynK, DynUnit>
where
    I1: IndexedZSet,
    I2: IndexedZSet<Key = I1::Key, DynK = I1::DynK>,
    Z: ZSet,
    Box<Z::DynK>: Clone,
    F: Fn(&I1::Key, &I1::Val, &I2::Val) -> Z::Key + Clone + 'static,
{
    let mut key1: Box<Z::DynK> = Box::<Z::Key>::default().erase_box();
    let mut val1: Box<DynUnit> = Box::new(()).erase_box();

    let mut key2: Box<Z::DynK> = Box::<Z::Key>::default().erase_box();
    let mut val2: Box<DynUnit> = Box::new(()).erase_box();

    let join_clone = join.clone();

    TraceJoinFuncs {
        left: Box::new(move |k, v1, v2, cb| unsafe {
            *key1.downcast_mut() = join(k.downcast(), v1.downcast(), v2.downcast());
            cb(key1.as_mut(), val1.as_mut());
        }),
        right: Box::new(move |k, v2, v1, cb| unsafe {
            *key2.downcast_mut() = join_clone(k.downcast(), v1.downcast(), v2.downcast());
            cb(key2.as_mut(), val2.as_mut());
        }),
    }
}

fn mk_trace_join_generic_funcs<I1, I2, Z, F, It>(
    join: F,
) -> TraceJoinFuncs<I1::DynK, I1::DynV, I2::DynV, Z::DynK, Z::DynV>
where
    I1: IndexedZSet,
    I2: IndexedZSet<Key = I1::Key, DynK = I1::DynK>,
    Z: IndexedZSet,
    Box<Z::DynK>: Clone,
    Box<Z::DynV>: Clone,
    F: Fn(&I1::Key, &I1::Val, &I2::Val) -> It + Clone + 'static,
    It: IntoIterator<Item = (Z::Key, Z::Val)> + 'static,
{
    let mut key1: Box<Z::DynK> = Box::<Z::Key>::default().erase_box();
    let mut val1: Box<Z::DynV> = Box::<Z::Val>::default().erase_box();

    let mut key2: Box<Z::DynK> = Box::<Z::Key>::default().erase_box();
    let mut val2: Box<Z::DynV> = Box::<Z::Val>::default().erase_box();

    let join_clone = join.clone();

    TraceJoinFuncs {
        left: Box::new(move |k, v1, v2, cb| unsafe {
            for (key, val) in join(k.downcast(), v1.downcast(), v2.downcast()) {
                *key1.downcast_mut() = key;
                *val1.downcast_mut() = val;
                cb(key1.as_mut(), val1.as_mut());
            }
        }),
        right: Box::new(move |k, v2, v1, cb| unsafe {
            for (key, val) in join_clone(k.downcast(), v1.downcast(), v2.downcast()) {
                *key2.downcast_mut() = key;
                *val2.downcast_mut() = val;
                cb(key2.as_mut(), val2.as_mut());
            }
        }),
    }
}

impl<C, K1, V1> Stream<C, OrdIndexedZSet<K1, V1>>
where
    C: Circuit,
    K1: DBData,
    V1: DBData,
{
    /// Incrementally join two streams of batches.
    ///
    /// Given streams `self` and `other` of batches that represent changes to
    /// relations `A` and `B` respectively, computes a stream of changes to
    /// `A ⋈ B` (where `⋈` is the join operator):
    ///
    /// # Type arguments
    ///
    /// * `I1` - batch type in the first input stream.
    /// * `I2` - batch type in the second input stream.
    /// * `F` - join function type: maps key and a pair of values from input
    ///   batches to an output value.
    /// * `V` - output value type.
    pub fn join<F, V2, V>(
        &self,
        other: &Stream<C, OrdIndexedZSet<K1, V2>>,
        join: F,
    ) -> Stream<C, OrdZSet<V>>
    where
        V2: DBData,
        V: DBData,
        F: Fn(&K1, &V1, &V2) -> V + Clone + 'static,
    {
        let join_funcs =
            mk_trace_join_funcs::<OrdIndexedZSet<K1, V1>, OrdIndexedZSet<K1, V2>, OrdZSet<V>, _>(
                join,
            );

        let join_factories = JoinFactories::new::<K1, V1, V2, V, ()>();

        self.inner()
            .dyn_join(&join_factories, &other.inner(), join_funcs)
            .typed()
    }

    /// Incrementally join two streams of batches, producing an indexed output
    /// stream.
    ///
    /// This method generalizes [`Self::join`].  It takes a join function that
    /// returns an iterable collection of `(key, value)` pairs, used to
    /// construct an indexed output Z-set.
    pub fn join_index<F, V2, K, V, It>(
        &self,
        other: &Stream<C, OrdIndexedZSet<K1, V2>>,
        join: F,
    ) -> Stream<C, OrdIndexedZSet<K, V>>
    where
        V2: DBData,
        K: DBData,
        V: DBData,
        F: Fn(&K1, &V1, &V2) -> It + Clone + 'static,
        It: IntoIterator<Item = (K, V)> + 'static,
    {
        let join_funcs = mk_trace_join_generic_funcs::<
            OrdIndexedZSet<K1, V1>,
            OrdIndexedZSet<K1, V2>,
            OrdIndexedZSet<K, V>,
            _,
            _,
        >(join);

        let join_factories = JoinFactories::new::<K1, V1, V2, K, V>();

        self.inner()
            .dyn_join_index(&join_factories, &other.inner(), join_funcs)
            .typed()
    }

    /// Like [`Stream::outer_join`], but uses default value for the missing side of the
    /// join.
    pub fn outer_join_default<F, V2, O>(
        &self,
        other: &Stream<C, OrdIndexedZSet<K1, V2>>,
        join_func: F,
    ) -> Stream<C, OrdZSet<O>>
    where
        V2: DBData,
        O: DBData,
        F: Fn(&K1, &V1, &V2) -> O + Clone + 'static,
    {
        let join_funcs =
            mk_trace_join_funcs::<OrdIndexedZSet<K1, V1>, OrdIndexedZSet<K1, V2>, OrdZSet<O>, _>(
                join_func,
            );

        let factories = OuterJoinFactories::new::<K1, V1, V2, O>();

        self.inner()
            .dyn_outer_join_default(&factories, &other.inner(), join_funcs)
            .typed()
    }
}

impl<C, I1> Stream<C, I1>
where
    C: Circuit,
    I1: IndexedZSet,
    I1::InnerBatch: Send,
{
    /// Join two streams of batches.
    ///
    /// The operator takes two streams of batches indexed with the same key type
    /// (`I1::Key = I2::Key`) and outputs a stream obtained by joining each pair
    /// of inputs.
    ///
    /// Input streams will typically be produced by the
    /// [`map_index`](`crate::circuit::Stream::index`) operator.
    ///
    /// # Type arguments
    ///
    /// * `F` - join function type: maps key and a pair of values from input
    ///   batches to an output value.
    /// * `I1` - batch type in the first input stream.
    /// * `I2` - batch type in the second input stream.
    /// * `V` - output value type.
    pub fn stream_join<F, I2, V>(&self, other: &Stream<C, I2>, join: F) -> Stream<C, OrdZSet<V>>
    where
        I2: IndexedZSet<Key = I1::Key, DynK = I1::DynK>,
        I2::InnerBatch: Send,
        V: DBData,
        F: Fn(&I1::Key, &I1::Val, &I2::Val) -> V + Clone + 'static,
    {
        let factories = StreamJoinFactories::new::<I1::Key, I1::Val, I2::Val, V>();

        self.inner()
            .dyn_stream_join(
                &factories,
                &other.inner(),
                Box::new(move |k, v1, v2, res: &mut DynData| unsafe {
                    *res.downcast_mut() = join(k.downcast(), v1.downcast(), v2.downcast())
                }),
            )
            .typed()
    }

    /// Like [`Self::stream_join`], but can return any batch type.
    pub fn stream_join_generic<F, I2, Z>(&self, other: &Stream<C, I2>, join: F) -> Stream<C, Z>
    where
        I2: IndexedZSet<Key = I1::Key, DynK = I1::DynK>,
        I2::InnerBatch: Send,
        Z: ZSet,
        F: Fn(&I1::Key, &I1::Val, &I2::Val) -> Z::Key + Clone + 'static,
    {
        let factories = StreamJoinFactories::new::<I1::Key, I1::Val, I2::Val, Z::Key>();

        self.inner()
            .dyn_stream_join_generic(
                &factories,
                &other.inner(),
                Box::new(move |k, v1, v2, res: &mut Z::DynK| unsafe {
                    *res.downcast_mut() = join(k.downcast(), v1.downcast(), v2.downcast())
                }),
            )
            .typed()
    }

    /// More efficient than [`Self::stream_join`], but the output of the join
    /// function must grow monotonically as `(k, v1, v2)` tuples are fed to it
    /// in lexicographic order.
    ///
    /// One such monotonic function is a join function that returns `(k, v1,
    /// v2)` itself.
    pub fn monotonic_stream_join<F, I2, Z>(&self, other: &Stream<C, I2>, join: F) -> Stream<C, Z>
    where
        I1: IndexedZSet,
        I2: IndexedZSet<Key = I1::Key, DynK = I1::DynK>,
        I2::InnerBatch: Send,
        Z: ZSet,
        F: Fn(&I1::Key, &I1::Val, &I2::Val) -> Z::Key + Clone + 'static,
    {
        let factories = StreamJoinFactories::new::<I1::Key, I1::Val, I2::Val, Z::Key>();

        self.inner()
            .dyn_monotonic_stream_join(
                &factories,
                &other.inner(),
                Box::new(move |k, v1, v2, res: &mut Z::DynK| unsafe {
                    *res.downcast_mut() = join(k.downcast(), v1.downcast(), v2.downcast())
                }),
            )
            .typed()
    }

    pub fn join_generic<I2, F, Z, It>(&self, other: &Stream<C, I2>, join: F) -> Stream<C, Z>
    where
        I1::InnerBatch: Spillable,
        I2: IndexedZSet<Key = I1::Key, DynK = I1::DynK>,
        I2::InnerBatch: Spillable + Send,
        Z: IndexedZSet,
        Box<Z::DynK>: Clone,
        Box<Z::DynV>: Clone,
        F: Fn(&I1::Key, &I1::Val, &I2::Val) -> It + Clone + 'static,
        It: IntoIterator<Item = (Z::Key, Z::Val)> + 'static,
    {
        let factories = JoinFactories::new::<I1::Key, I1::Val, I2::Val, Z::Key, Z::Val>();
        let join_funcs = mk_trace_join_generic_funcs::<I1, I2, Z, _, _>(join);

        self.inner()
            .dyn_join_generic(&factories, &other.inner(), join_funcs)
            .typed()
    }

    /// Incremental anti-join operator.
    ///
    /// Returns indexed Z-set consisting of the contents of `self`,
    /// excluding keys that are present in `other`.
    pub fn antijoin<I2>(&self, other: &Stream<C, I2>) -> Stream<C, I1>
    where
        I1::InnerBatch: Spillable,
        I2: IndexedZSet<Key = I1::Key, DynK = I1::DynK>,
        I2::InnerBatch: Spillable + Send,
        Box<I1::DynK>: Clone,
        Box<I1::DynV>: Clone,
    {
        let factories = AntijoinFactories::new::<I1::Key, I1::Val, I2::Val>();

        self.inner()
            .dyn_antijoin(&factories, &other.inner())
            .typed()
    }

    /// Outer join:
    /// - returns the output of `join_func` for common keys.
    /// - returns the output of `left_func` for keys only found in `self`, but
    ///   not `other`.
    /// - returns the output of `right_func` for keys only found in `other`, but
    ///   not `self`.
    pub fn outer_join<I2, F, FL, FR, O>(
        &self,
        other: &Stream<C, I2>,
        join_func: F,
        left_func: FL,
        right_func: FR,
    ) -> Stream<C, OrdZSet<O>>
    where
        I2: IndexedZSet<Key = I1::Key, DynK = I1::DynK>,
        I1::Inner: DynFilterMap,
        I2::Inner: DynFilterMap,
        I1::InnerBatch: Spillable,
        I2::InnerBatch: Spillable + Send,
        O: DBData,
        Box<I1::DynK>: Clone,
        Box<I1::DynV>: Clone,
        Box<I2::DynV>: Clone,
        F: Fn(&I1::Key, &I1::Val, &I2::Val) -> O + Clone + 'static,
        for<'a> FL: Fn(&I1::Key, &I1::Val) -> O + Clone + 'static,
        for<'a> FR: Fn(&I2::Key, &I2::Val) -> O + Clone + 'static,
    {
        let factories = OuterJoinFactories::new::<I1::Key, I1::Val, I2::Val, O>();
        let join_funcs = mk_trace_join_funcs::<I1, I2, OrdZSet<O>, _>(join_func);

        self.inner()
            .dyn_outer_join(
                &factories,
                &other.inner(),
                join_funcs,
                Box::new(move |item, cb| {
                    let (k, v) = I1::Inner::item_ref_keyval(item);
                    let mut out = unsafe { left_func(k.downcast(), v.downcast()) };
                    cb(out.erase_mut(), ().erase_mut());
                }),
                Box::new(move |item, cb| {
                    let (k, v) = I2::Inner::item_ref_keyval(item);
                    let mut out = unsafe { right_func(k.downcast(), v.downcast()) };
                    cb(out.erase_mut(), ().erase_mut());
                }),
            )
            .typed()
    }
}
