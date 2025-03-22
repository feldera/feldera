//! Monomorphic versions of DBSP operators for use by the SQL compiler.
//!
//! This module contains versions of several DBSP operators that force the implementation
//! of these operators to get compiled as part of the DBSP crate instead of the client crate
//! that uses DBSP. Using these operators instead of the equivalents in `crate::operators`
//! can significantly speed up Rust compilation when the client code is decomposed into many
//! crates.
//!
//! To understand how this works, consider for example the `Stream::join` operator. This operator
//! is a wrapper on top of `Stream::dyn_join`, which is generic over circuit type, key and
//! value types. In practice it is always instantiated with key=`DynData`, value=`DynData` and
//! circuit being either `RootCircuit` or `NestedCircuit`.  As such, it needs to be instantiated
//! at most twice in any program. However, since this instantiation happens when compiling the
//! client crate, it ends up being instantiated once in each client crate of a multi-crate project.
//!
//! The API in this module solves the problem by forcing the instantiation of `dyn_join` in the
//! `dbsp` crate. It provides a less generic API: all methods take and return `OrdIndexedZSet`/`OrdZSet`
//! streams and are specialized for a specific type of circuit, `RootCircuit` or `NestedCircuit` (which
//! means that they don't work for arbitrary levels of nesting).  Internally they call monomorphized
//! versions of `dyn_` operators (e.g., `dyn_join_mono`).
//!
//! This API is enabled by the `backend-mode` feature, which also disables the matching polymorphic
//! API in `crate::operators`. Enabling this feature should be transparent for most client code.

use std::mem::take;

use crate::{
    algebra::MulByRef,
    circuit::WithClock,
    dynamic::{DowncastTrait, DynData, DynUnit, DynWeight, Erase},
    operator::{
        dynamic::{
            aggregate::{DynAggregatorImpl, IncAggregateFactories, IncAggregateLinearFactories},
            controlled_filter::ControlledFilterFactories,
            distinct::{DistinctFactories, HashDistinctFactories},
            join::{AntijoinFactories, JoinFactories},
            MonoIndexedZSet,
        },
        join::{mk_trace_join_flatmap_funcs, mk_trace_join_funcs, mk_trace_join_generic_funcs},
        time_series::OrdPartitionedOverStream,
        Aggregator,
    },
    trace::BatchReaderFactories,
    utils::Tup2,
    DBData, DBWeight, DynZWeight, NestedCircuit, OrdIndexedZSet, OrdZSet, RootCircuit, Stream,
    TypedBox, ZWeight,
};

impl<K, V> Stream<RootCircuit, OrdIndexedZSet<K, V>>
where
    K: DBData,
    V: DBData,
{
    #[allow(clippy::type_complexity)]
    #[track_caller]
    pub fn aggregate<A>(&self, aggregator: A) -> Stream<RootCircuit, OrdIndexedZSet<K, A::Output>>
    where
        A: Aggregator<V, (), ZWeight>,
    {
        let aggregate_factories = IncAggregateFactories::new::<K, V, ZWeight, A::Output>();

        let dyn_aggregator =
            DynAggregatorImpl::<DynData, V, (), DynZWeight, ZWeight, A, DynData, DynData>::new(
                aggregator,
            );

        self.inner()
            .dyn_aggregate_mono(&aggregate_factories, &dyn_aggregator)
            .typed()
    }

    #[track_caller]
    pub fn aggregate_linear_postprocess<F, A, OF, OV>(
        &self,
        f: F,
        of: OF,
    ) -> Stream<RootCircuit, OrdIndexedZSet<K, OV>>
    where
        A: DBWeight + MulByRef<ZWeight, Output = A>,
        OV: DBData,
        F: Fn(&V) -> A + Clone + 'static,
        OF: Fn(A) -> OV + Clone + 'static,
    {
        let factories: IncAggregateLinearFactories<
            MonoIndexedZSet,
            DynWeight,
            MonoIndexedZSet,
            (),
        > = IncAggregateLinearFactories::new::<K, A, OV>();

        self.inner()
            .dyn_aggregate_linear_mono(
                &factories,
                Box::new(move |_k, v, r, acc| unsafe {
                    *acc.downcast_mut::<A>() = f(v.downcast::<V>()).mul_by_ref(&**r)
                }),
                Box::new(move |w, out| unsafe {
                    *out.downcast_mut::<OV>() = of(take(w.downcast_mut::<A>()))
                }),
            )
            .typed()
    }

    #[track_caller]
    pub fn join<F, V2, OV>(
        &self,
        other: &Stream<RootCircuit, OrdIndexedZSet<K, V2>>,
        join: F,
    ) -> Stream<RootCircuit, OrdZSet<OV>>
    where
        V2: DBData,
        OV: DBData,
        F: Fn(&K, &V, &V2) -> OV + Clone + 'static,
    {
        let join_funcs =
            mk_trace_join_funcs::<OrdIndexedZSet<K, V>, OrdIndexedZSet<K, V2>, OrdZSet<OV>, _>(
                join,
            );

        let join_factories = JoinFactories::new::<K, V, V2, OV, ()>();

        self.inner()
            .dyn_join_mono(&join_factories, &other.inner(), join_funcs)
            .typed()
    }

    #[track_caller]
    pub fn join_flatmap<F, V2, OV, It>(
        &self,
        other: &Stream<RootCircuit, OrdIndexedZSet<K, V2>>,
        join: F,
    ) -> Stream<RootCircuit, OrdZSet<OV>>
    where
        V2: DBData,
        OV: DBData,
        F: Fn(&K, &V, &V2) -> It + Clone + 'static,
        It: IntoIterator<Item = OV> + 'static,
    {
        let join_funcs = mk_trace_join_flatmap_funcs::<
            OrdIndexedZSet<K, V>,
            OrdIndexedZSet<K, V2>,
            OrdZSet<OV>,
            _,
            It,
        >(join);

        let join_factories = JoinFactories::new::<K, V, V2, OV, ()>();

        self.inner()
            .dyn_join_mono(&join_factories, &other.inner(), join_funcs)
            .typed()
    }

    #[track_caller]
    pub fn join_index<F, V2, OK, OV, It>(
        &self,
        other: &Stream<RootCircuit, OrdIndexedZSet<K, V2>>,
        join: F,
    ) -> Stream<RootCircuit, OrdIndexedZSet<OK, OV>>
    where
        V2: DBData,
        OK: DBData,
        OV: DBData,
        F: Fn(&K, &V, &V2) -> It + Clone + 'static,
        It: IntoIterator<Item = (OK, OV)> + 'static,
    {
        let join_funcs = mk_trace_join_generic_funcs::<
            OrdIndexedZSet<K, V>,
            OrdIndexedZSet<K, V2>,
            OrdIndexedZSet<OK, OV>,
            _,
            _,
        >(join);

        let join_factories = JoinFactories::new::<K, V, V2, OK, OV>();

        self.inner()
            .dyn_join_index_mono(&join_factories, &other.inner(), join_funcs)
            .typed()
    }

    #[track_caller]
    pub fn antijoin<K2, V2>(&self, other: &Stream<RootCircuit, OrdIndexedZSet<K2, V2>>) -> Self
    where
        K2: DBData,
        V2: DBData,
    {
        let factories = AntijoinFactories::new::<K, V>();

        self.inner()
            .dyn_antijoin_mono(&factories, &other.inner())
            .typed()
    }

    #[track_caller]
    pub fn distinct(&self) -> Self {
        let factories = DistinctFactories::new::<K, V>();

        self.inner().dyn_distinct_mono(&factories).typed()
    }

    #[track_caller]
    pub fn hash_distinct(&self) -> Self {
        let factories = HashDistinctFactories::new::<K, V>();

        self.inner().dyn_hash_distinct_mono(&factories).typed()
    }

    #[track_caller]
    pub fn waterline<TS, WF, IF, LB>(
        &self,
        init: IF,
        extract_ts: WF,
        least_upper_bound: LB,
    ) -> Stream<RootCircuit, TypedBox<TS, DynData>>
    where
        TS: DBData,
        IF: Fn() -> TS + 'static,
        WF: Fn(&K, &V) -> TS + 'static,
        LB: Fn(&TS, &TS) -> TS + Clone + 'static,
    {
        let result = self.inner().dyn_waterline_mono(
            Box::new(move || Box::new(init()).erase_box()),
            Box::new(move |k, v, ts: &mut DynData| unsafe {
                *ts.downcast_mut::<TS>() = extract_ts(k.downcast(), v.downcast())
            }),
            Box::new(move |l, r, ts| unsafe {
                *ts.downcast_mut() = least_upper_bound(l.downcast(), r.downcast())
            }),
        );

        unsafe { result.typed_data() }
    }

    #[track_caller]
    pub fn filter<F>(&self, filter_func: F) -> Self
    where
        F: Fn((&K, &V)) -> bool + 'static,
    {
        self.inner()
            .dyn_filter_mono(Box::new(move |(k, v)| unsafe {
                filter_func((k.downcast(), v.downcast()))
            }))
            .typed()
    }

    #[track_caller]
    pub fn map<F, OK>(&self, map_func: F) -> Stream<RootCircuit, OrdZSet<OK>>
    where
        OK: DBData,
        F: Fn((&K, &V)) -> OK + Clone + 'static,
    {
        let factories = BatchReaderFactories::new::<OK, (), ZWeight>();

        self.inner()
            .dyn_map_mono(
                &factories,
                Box::new(move |(k, v), pair| {
                    let mut key = map_func(unsafe { (k.downcast(), v.downcast()) });
                    pair.from_vals(key.erase_mut(), ().erase_mut());
                }),
            )
            .typed()
    }

    #[track_caller]
    pub fn map_index<F, OK, OV>(&self, map_func: F) -> Stream<RootCircuit, OrdIndexedZSet<OK, OV>>
    where
        OK: DBData,
        OV: DBData,
        F: Fn((&K, &V)) -> (OK, OV) + 'static,
    {
        let factories = BatchReaderFactories::new::<OK, OV, ZWeight>();

        self.inner()
            .dyn_map_index_mono(
                &factories,
                Box::new(move |(k, v), pair| {
                    let (mut key, mut val) = map_func(unsafe { (k.downcast(), v.downcast()) });
                    pair.from_vals(key.erase_mut(), val.erase_mut());
                }),
            )
            .typed()
    }

    #[track_caller]
    pub fn flat_map<F, I>(&self, mut func: F) -> Stream<RootCircuit, OrdZSet<I::Item>>
    where
        F: FnMut((&K, &V)) -> I + 'static,
        I: IntoIterator + 'static,
        I::Item: DBData,
    {
        let factories = BatchReaderFactories::new::<I::Item, (), ZWeight>();

        self.inner()
            .dyn_flat_map_mono(
                &factories,
                Box::new(move |(k, v), cb| {
                    for mut key in func(unsafe { (k.downcast(), v.downcast()) }) {
                        cb(key.erase_mut(), ().erase_mut());
                    }
                }),
            )
            .typed()
    }

    #[track_caller]
    pub fn flat_map_index<F, OK, OV, I>(
        &self,
        mut func: F,
    ) -> Stream<RootCircuit, OrdIndexedZSet<OK, OV>>
    where
        F: FnMut((&K, &V)) -> I + 'static,
        I: IntoIterator<Item = (OK, OV)> + 'static,
        OK: DBData,
        OV: DBData,
    {
        let factories = BatchReaderFactories::new::<OK, OV, ZWeight>();

        self.inner()
            .dyn_flat_map_index_mono(
                &factories,
                Box::new(move |(k, v), cb| {
                    for (mut key, mut val) in func(unsafe { (k.downcast(), v.downcast()) }) {
                        cb(key.erase_mut(), val.erase_mut());
                    }
                }),
            )
            .typed()
    }
}

impl<K> Stream<RootCircuit, OrdZSet<K>>
where
    K: DBData,
{
    #[track_caller]
    pub fn distinct(&self) -> Self {
        let factories = DistinctFactories::new::<K, ()>();

        self.inner().dyn_distinct_mono(&factories).typed()
    }

    #[track_caller]
    pub fn hash_distinct(&self) -> Self {
        let factories = HashDistinctFactories::new::<K, ()>();

        self.inner().dyn_hash_distinct_mono(&factories).typed()
    }

    #[track_caller]
    pub fn waterline<TS, WF, IF, LB>(
        &self,
        init: IF,
        extract_ts: WF,
        least_upper_bound: LB,
    ) -> Stream<RootCircuit, TypedBox<TS, DynData>>
    where
        TS: DBData,
        IF: Fn() -> TS + 'static,
        WF: Fn(&K, &()) -> TS + 'static,
        LB: Fn(&TS, &TS) -> TS + Clone + 'static,
    {
        let result = self.inner().dyn_waterline_mono(
            Box::new(move || Box::new(init()).erase_box()),
            Box::new(move |k, v, ts: &mut DynData| unsafe {
                *ts.downcast_mut::<TS>() = extract_ts(k.downcast(), v.downcast())
            }),
            Box::new(move |l, r, ts| unsafe {
                *ts.downcast_mut() = least_upper_bound(l.downcast(), r.downcast())
            }),
        );

        unsafe { result.typed_data() }
    }

    #[track_caller]
    pub fn filter<F>(&self, filter_func: F) -> Self
    where
        F: Fn(&K) -> bool + 'static,
    {
        self.inner()
            .dyn_filter_mono(Box::new(move |k| unsafe { filter_func(k.downcast()) }))
            .typed()
    }

    #[track_caller]
    pub fn map<F, OK>(&self, map_func: F) -> Stream<RootCircuit, OrdZSet<OK>>
    where
        OK: DBData,
        F: Fn(&K) -> OK + Clone + 'static,
    {
        let factories = BatchReaderFactories::new::<OK, (), ZWeight>();

        self.inner()
            .dyn_map_mono(
                &factories,
                Box::new(move |k, pair| {
                    let mut key = map_func(unsafe { k.downcast() });
                    pair.from_vals(key.erase_mut(), ().erase_mut());
                }),
            )
            .typed()
    }

    #[track_caller]
    pub fn map_index<F, OK, OV>(&self, map_func: F) -> Stream<RootCircuit, OrdIndexedZSet<OK, OV>>
    where
        OK: DBData,
        OV: DBData,
        F: Fn(&K) -> (OK, OV) + 'static,
    {
        let factories = BatchReaderFactories::new::<OK, OV, ZWeight>();

        self.inner()
            .dyn_map_index_mono(
                &factories,
                Box::new(move |k, pair| {
                    let (mut key, mut val) = map_func(unsafe { k.downcast() });
                    pair.from_vals(key.erase_mut(), val.erase_mut());
                }),
            )
            .typed()
    }

    #[track_caller]
    pub fn flat_map<F, I>(&self, mut func: F) -> Stream<RootCircuit, OrdZSet<I::Item>>
    where
        F: FnMut(&K) -> I + 'static,
        I: IntoIterator + 'static,
        I::Item: DBData,
    {
        let factories = BatchReaderFactories::new::<I::Item, (), ZWeight>();

        self.inner()
            .dyn_flat_map_mono(
                &factories,
                Box::new(move |k, cb| {
                    for mut key in func(unsafe { k.downcast() }) {
                        cb(key.erase_mut(), ().erase_mut());
                    }
                }),
            )
            .typed()
    }

    #[track_caller]
    pub fn flat_map_index<F, OK, OV, I>(
        &self,
        mut func: F,
    ) -> Stream<RootCircuit, OrdIndexedZSet<OK, OV>>
    where
        F: FnMut(&K) -> I + 'static,
        I: IntoIterator<Item = (OK, OV)> + 'static,
        OK: DBData,
        OV: DBData,
    {
        let factories = BatchReaderFactories::new::<OK, OV, ZWeight>();

        self.inner()
            .dyn_flat_map_index_mono(
                &factories,
                Box::new(move |k, cb| {
                    for (mut key, mut val) in func(unsafe { k.downcast() }) {
                        cb(key.erase_mut(), val.erase_mut());
                    }
                }),
            )
            .typed()
    }

    pub fn controlled_key_filter_typed<T, E, F, RF>(
        &self,
        threshold: &Stream<RootCircuit, T>,
        filter_func: F,
        report_func: RF,
    ) -> (Self, Stream<RootCircuit, OrdZSet<E>>)
    where
        E: DBData,
        T: DBData,
        F: Fn(&T, &K) -> bool + 'static,
        RF: Fn(&T, &K, &(), ZWeight) -> E + 'static,
    {
        let factories = ControlledFilterFactories::new::<K, (), E>();

        let (output, error_stream) = self.inner().dyn_controlled_key_filter_mono(
            factories,
            &threshold.typed_box::<DynData>().inner_data(),
            Box::new(move |t: &DynData, k: &DynData| unsafe {
                filter_func(t.downcast(), k.downcast())
            }),
            Box::new(
                move |t: &DynData, k: &DynData, v: &DynUnit, w: ZWeight, e: &mut DynData| unsafe {
                    *e.downcast_mut() = report_func(t.downcast(), k.downcast(), v.downcast(), w)
                },
            ),
        );

        (output.typed(), error_stream.typed())
    }
}

impl<K, V> Stream<NestedCircuit, OrdIndexedZSet<K, V>>
where
    K: DBData,
    V: DBData,
{
    #[allow(clippy::type_complexity)]
    #[track_caller]
    pub fn aggregate<A>(&self, aggregator: A) -> Stream<NestedCircuit, OrdIndexedZSet<K, A::Output>>
    where
        A: Aggregator<V, <NestedCircuit as WithClock>::Time, ZWeight>,
    {
        let aggregate_factories = IncAggregateFactories::new::<K, V, ZWeight, A::Output>();

        let dyn_aggregator = DynAggregatorImpl::<
            DynData,
            V,
            <NestedCircuit as WithClock>::Time,
            DynZWeight,
            ZWeight,
            A,
            DynData,
            DynData,
        >::new(aggregator);

        self.inner()
            .dyn_aggregate_mono(&aggregate_factories, &dyn_aggregator)
            .typed()
    }

    #[track_caller]
    pub fn aggregate_linear_postprocess<F, A, OF, OV>(
        &self,
        f: F,
        of: OF,
    ) -> Stream<NestedCircuit, OrdIndexedZSet<K, OV>>
    where
        A: DBWeight + MulByRef<ZWeight, Output = A>,
        OV: DBData,
        F: Fn(&V) -> A + Clone + 'static,
        OF: Fn(A) -> OV + Clone + 'static,
    {
        let factories: IncAggregateLinearFactories<
            MonoIndexedZSet,
            DynWeight,
            MonoIndexedZSet,
            <NestedCircuit as WithClock>::Time,
        > = IncAggregateLinearFactories::new::<K, A, OV>();

        self.inner()
            .dyn_aggregate_linear_mono(
                &factories,
                Box::new(move |_k, v, r, acc| unsafe {
                    *acc.downcast_mut::<A>() = f(v.downcast::<V>()).mul_by_ref(&**r)
                }),
                Box::new(move |w, out| unsafe {
                    *out.downcast_mut::<OV>() = of(take(w.downcast_mut::<A>()))
                }),
            )
            .typed()
    }

    #[track_caller]
    pub fn join<F, V2, OV>(
        &self,
        other: &Stream<NestedCircuit, OrdIndexedZSet<K, V2>>,
        join: F,
    ) -> Stream<NestedCircuit, OrdZSet<OV>>
    where
        V2: DBData,
        OV: DBData,
        F: Fn(&K, &V, &V2) -> OV + Clone + 'static,
    {
        let join_funcs =
            mk_trace_join_funcs::<OrdIndexedZSet<K, V>, OrdIndexedZSet<K, V2>, OrdZSet<OV>, _>(
                join,
            );

        let join_factories = JoinFactories::new::<K, V, V2, OV, ()>();

        self.inner()
            .dyn_join_mono(&join_factories, &other.inner(), join_funcs)
            .typed()
    }

    #[track_caller]
    pub fn join_flatmap<F, V2, OV, It>(
        &self,
        other: &Stream<NestedCircuit, OrdIndexedZSet<K, V2>>,
        join: F,
    ) -> Stream<NestedCircuit, OrdZSet<OV>>
    where
        V2: DBData,
        OV: DBData,
        F: Fn(&K, &V, &V2) -> It + Clone + 'static,
        It: IntoIterator<Item = OV> + 'static,
    {
        let join_funcs = mk_trace_join_flatmap_funcs::<
            OrdIndexedZSet<K, V>,
            OrdIndexedZSet<K, V2>,
            OrdZSet<OV>,
            _,
            It,
        >(join);

        let join_factories = JoinFactories::new::<K, V, V2, OV, ()>();

        self.inner()
            .dyn_join_mono(&join_factories, &other.inner(), join_funcs)
            .typed()
    }

    #[track_caller]
    pub fn join_index<F, V2, OK, OV, It>(
        &self,
        other: &Stream<NestedCircuit, OrdIndexedZSet<K, V2>>,
        join: F,
    ) -> Stream<NestedCircuit, OrdIndexedZSet<OK, OV>>
    where
        V2: DBData,
        OK: DBData,
        OV: DBData,
        F: Fn(&K, &V, &V2) -> It + Clone + 'static,
        It: IntoIterator<Item = (OK, OV)> + 'static,
    {
        let join_funcs = mk_trace_join_generic_funcs::<
            OrdIndexedZSet<K, V>,
            OrdIndexedZSet<K, V2>,
            OrdIndexedZSet<OK, OV>,
            _,
            _,
        >(join);

        let join_factories = JoinFactories::new::<K, V, V2, OK, OV>();

        self.inner()
            .dyn_join_index_mono(&join_factories, &other.inner(), join_funcs)
            .typed()
    }

    #[track_caller]
    pub fn antijoin<K2, V2>(&self, other: &Stream<NestedCircuit, OrdIndexedZSet<K2, V2>>) -> Self
    where
        K2: DBData,
        V2: DBData,
    {
        let factories = AntijoinFactories::new::<K, V>();

        self.inner()
            .dyn_antijoin_mono(&factories, &other.inner())
            .typed()
    }

    #[track_caller]
    pub fn distinct(&self) -> Self {
        let factories = DistinctFactories::new::<K, V>();

        self.inner().dyn_distinct_mono(&factories).typed()
    }

    #[track_caller]
    pub fn hash_distinct(&self) -> Self {
        let factories = HashDistinctFactories::new::<K, V>();

        self.inner().dyn_hash_distinct_mono(&factories).typed()
    }

    #[track_caller]
    pub fn filter<F>(&self, filter_func: F) -> Self
    where
        F: Fn((&K, &V)) -> bool + 'static,
    {
        self.inner()
            .dyn_filter_mono(Box::new(move |(k, v)| unsafe {
                filter_func((k.downcast(), v.downcast()))
            }))
            .typed()
    }

    #[track_caller]
    pub fn map<F, OK>(&self, map_func: F) -> Stream<NestedCircuit, OrdZSet<OK>>
    where
        OK: DBData,
        F: Fn((&K, &V)) -> OK + Clone + 'static,
    {
        let factories = BatchReaderFactories::new::<OK, (), ZWeight>();

        self.inner()
            .dyn_map_mono(
                &factories,
                Box::new(move |(k, v), pair| {
                    let mut key = map_func(unsafe { (k.downcast(), v.downcast()) });
                    pair.from_vals(key.erase_mut(), ().erase_mut());
                }),
            )
            .typed()
    }

    #[track_caller]
    pub fn map_index<F, OK, OV>(&self, map_func: F) -> Stream<NestedCircuit, OrdIndexedZSet<OK, OV>>
    where
        OK: DBData,
        OV: DBData,
        F: Fn((&K, &V)) -> (OK, OV) + 'static,
    {
        let factories = BatchReaderFactories::new::<OK, OV, ZWeight>();

        self.inner()
            .dyn_map_index_mono(
                &factories,
                Box::new(move |(k, v), pair| {
                    let (mut key, mut val) = map_func(unsafe { (k.downcast(), v.downcast()) });
                    pair.from_vals(key.erase_mut(), val.erase_mut());
                }),
            )
            .typed()
    }

    #[track_caller]
    pub fn flat_map<F, I>(&self, mut func: F) -> Stream<NestedCircuit, OrdZSet<I::Item>>
    where
        F: FnMut((&K, &V)) -> I + 'static,
        I: IntoIterator + 'static,
        I::Item: DBData,
    {
        let factories = BatchReaderFactories::new::<I::Item, (), ZWeight>();

        self.inner()
            .dyn_flat_map_mono(
                &factories,
                Box::new(move |(k, v), cb| {
                    for mut key in func(unsafe { (k.downcast(), v.downcast()) }) {
                        cb(key.erase_mut(), ().erase_mut());
                    }
                }),
            )
            .typed()
    }

    #[track_caller]
    pub fn flat_map_index<F, OK, OV, I>(
        &self,
        mut func: F,
    ) -> Stream<NestedCircuit, OrdIndexedZSet<OK, OV>>
    where
        F: FnMut((&K, &V)) -> I + 'static,
        I: IntoIterator<Item = (OK, OV)> + 'static,
        OK: DBData,
        OV: DBData,
    {
        let factories = BatchReaderFactories::new::<OK, OV, ZWeight>();

        self.inner()
            .dyn_flat_map_index_mono(
                &factories,
                Box::new(move |(k, v), cb| {
                    for (mut key, mut val) in func(unsafe { (k.downcast(), v.downcast()) }) {
                        cb(key.erase_mut(), val.erase_mut());
                    }
                }),
            )
            .typed()
    }
}

impl<K> Stream<NestedCircuit, OrdZSet<K>>
where
    K: DBData,
{
    #[track_caller]
    pub fn distinct(&self) -> Self {
        let factories = DistinctFactories::new::<K, ()>();

        self.inner().dyn_distinct_mono(&factories).typed()
    }

    #[track_caller]
    pub fn hash_distinct(&self) -> Self {
        let factories = HashDistinctFactories::new::<K, ()>();

        self.inner().dyn_hash_distinct_mono(&factories).typed()
    }

    #[track_caller]
    pub fn filter<F>(&self, filter_func: F) -> Self
    where
        F: Fn(&K) -> bool + 'static,
    {
        self.inner()
            .dyn_filter_mono(Box::new(move |k| unsafe { filter_func(k.downcast()) }))
            .typed()
    }

    #[track_caller]
    pub fn map<F, OK>(&self, map_func: F) -> Stream<NestedCircuit, OrdZSet<OK>>
    where
        OK: DBData,
        F: Fn(&K) -> OK + Clone + 'static,
    {
        let factories = BatchReaderFactories::new::<OK, (), ZWeight>();

        self.inner()
            .dyn_map_mono(
                &factories,
                Box::new(move |k, pair| {
                    let mut key = map_func(unsafe { k.downcast() });
                    pair.from_vals(key.erase_mut(), ().erase_mut());
                }),
            )
            .typed()
    }

    #[track_caller]
    pub fn map_index<F, OK, OV>(&self, map_func: F) -> Stream<NestedCircuit, OrdIndexedZSet<OK, OV>>
    where
        OK: DBData,
        OV: DBData,
        F: Fn(&K) -> (OK, OV) + 'static,
    {
        let factories = BatchReaderFactories::new::<OK, OV, ZWeight>();

        self.inner()
            .dyn_map_index_mono(
                &factories,
                Box::new(move |k, pair| {
                    let (mut key, mut val) = map_func(unsafe { k.downcast() });
                    pair.from_vals(key.erase_mut(), val.erase_mut());
                }),
            )
            .typed()
    }

    #[track_caller]
    pub fn flat_map<F, I>(&self, mut func: F) -> Stream<NestedCircuit, OrdZSet<I::Item>>
    where
        F: FnMut(&K) -> I + 'static,
        I: IntoIterator + 'static,
        I::Item: DBData,
    {
        let factories = BatchReaderFactories::new::<I::Item, (), ZWeight>();

        self.inner()
            .dyn_flat_map_mono(
                &factories,
                Box::new(move |k, cb| {
                    for mut key in func(unsafe { k.downcast() }) {
                        cb(key.erase_mut(), ().erase_mut());
                    }
                }),
            )
            .typed()
    }

    #[track_caller]
    pub fn flat_map_index<F, OK, OV, I>(
        &self,
        mut func: F,
    ) -> Stream<NestedCircuit, OrdIndexedZSet<OK, OV>>
    where
        F: FnMut(&K) -> I + 'static,
        I: IntoIterator<Item = (OK, OV)> + 'static,
        OK: DBData,
        OV: DBData,
    {
        let factories = BatchReaderFactories::new::<OK, OV, ZWeight>();

        self.inner()
            .dyn_flat_map_index_mono(
                &factories,
                Box::new(move |k, cb| {
                    for (mut key, mut val) in func(unsafe { k.downcast() }) {
                        cb(key.erase_mut(), val.erase_mut());
                    }
                }),
            )
            .typed()
    }
}

// For use with rolling aggregates only.
impl<PK, TS, V> OrdPartitionedOverStream<PK, TS, V>
where
    PK: DBData,
    V: DBData,
    TS: DBData,
{
    #[track_caller]
    pub fn map_index<F, OK, OV>(&self, map_func: F) -> Stream<RootCircuit, OrdIndexedZSet<OK, OV>>
    where
        OK: DBData,
        OV: DBData,
        F: Fn((&PK, &Tup2<TS, Option<V>>)) -> (OK, OV) + 'static,
    {
        let factories = BatchReaderFactories::new::<OK, OV, ZWeight>();

        self.inner()
            .dyn_map_index(
                &factories,
                Box::new(move |(k, v), pair| {
                    let (mut key, mut val) = map_func(unsafe { (k.downcast(), v.downcast()) });
                    pair.from_vals(key.erase_mut(), val.erase_mut());
                }),
            )
            .typed()
    }
}
