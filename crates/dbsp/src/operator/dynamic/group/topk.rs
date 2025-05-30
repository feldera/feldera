use dyn_clone::clone_box;

use super::{DiffGroupTransformer, Monotonicity, NonIncrementalGroupTransformer};
use crate::{
    algebra::{
        AddAssignByRef, HasOne, HasZero, IndexedZSet, OrdIndexedZSet, OrdIndexedZSetFactories,
        ZCursor, ZRingValue,
    },
    dynamic::{DataTrait, DynData, DynUnit, Erase, Factory, WeightTrait},
    operator::dynamic::MonoIndexedZSet,
    trace::{BatchReaderFactories, OrdIndexedWSetFactories},
    DBData, DBWeight, DynZWeight, RootCircuit, Stream, ZWeight,
};
use std::{marker::PhantomData, ops::Neg};

pub struct TopKFactories<B: IndexedZSet> {
    input_factories: B::Factories,
    output_factories: OrdIndexedWSetFactories<B::Key, B::Val, B::R>,
}

impl<B> TopKFactories<B>
where
    B: IndexedZSet,
{
    pub fn new<KType, VType>() -> Self
    where
        KType: DBData + Erase<B::Key>,
        VType: DBData + Erase<B::Val>,
    {
        Self {
            input_factories: BatchReaderFactories::new::<KType, VType, ZWeight>(),
            output_factories: BatchReaderFactories::new::<KType, VType, ZWeight>(),
        }
    }
}

pub struct TopKCustomOrdFactories<K, V, V2, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    V2: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    input_factories: OrdIndexedWSetFactories<K, V, R>,
    inner_factories: OrdIndexedWSetFactories<K, V2, R>,
}

impl<K, V, V2, R> TopKCustomOrdFactories<K, V, V2, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    V2: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    pub fn new<KType, VType, V2Type, RType>() -> Self
    where
        KType: DBData + Erase<K>,
        VType: DBData + Erase<V>,
        V2Type: DBData + Erase<V2>,
        RType: DBWeight + Erase<R>,
    {
        Self {
            input_factories: BatchReaderFactories::new::<KType, VType, RType>(),
            inner_factories: BatchReaderFactories::new::<KType, V2Type, RType>(),
        }
    }
}

pub struct TopKRankCustomOrdFactories<K, V2, OV>
where
    K: DataTrait + ?Sized,
    V2: DataTrait + ?Sized,
    OV: DataTrait + ?Sized,
{
    inner_factories: OrdIndexedZSetFactories<K, V2>,
    output_factories: OrdIndexedZSetFactories<K, OV>,
}

impl<K, V2, OV> TopKRankCustomOrdFactories<K, V2, OV>
where
    K: DataTrait + ?Sized,
    OV: DataTrait + ?Sized,
    V2: DataTrait + ?Sized,
{
    pub fn new<KType, V2Type, OVType>() -> Self
    where
        KType: DBData + Erase<K>,
        V2Type: DBData + Erase<V2>,
        OVType: DBData + Erase<OV>,
    {
        Self {
            inner_factories: BatchReaderFactories::new::<KType, V2Type, ZWeight>(),
            output_factories: BatchReaderFactories::new::<KType, OVType, ZWeight>(),
        }
    }
}

impl<B> Stream<RootCircuit, B>
where
    B: IndexedZSet + Send,
{
    /// See [`Stream::topk_asc`].
    #[allow(clippy::type_complexity)]
    pub fn dyn_topk_asc(
        &self,
        persistent_id: Option<&str>,
        factories: &TopKFactories<B>,
        k: usize,
    ) -> Stream<RootCircuit, OrdIndexedZSet<B::Key, B::Val>> {
        self.dyn_group_transform(
            persistent_id,
            &factories.input_factories,
            &factories.output_factories,
            Box::new(DiffGroupTransformer::new(
                factories.output_factories.val_factory(),
                TopK::asc(factories.input_factories.val_factory(), k),
            )),
        )
    }

    /// See [`Stream::topk_desc`].
    #[allow(clippy::type_complexity)]
    pub fn dyn_topk_desc(
        &self,
        persistent_id: Option<&str>,
        factories: &TopKFactories<B>,
        k: usize,
    ) -> Stream<RootCircuit, OrdIndexedZSet<B::Key, B::Val>> {
        self.dyn_group_transform(
            persistent_id,
            &factories.input_factories,
            &factories.output_factories,
            Box::new(DiffGroupTransformer::new(
                factories.output_factories.val_factory(),
                TopK::desc(factories.input_factories.val_factory(), k),
            )),
        )
    }
}

impl Stream<RootCircuit, MonoIndexedZSet> {
    pub fn dyn_topk_custom_order_mono(
        &self,
        persistent_id: Option<&str>,
        factories: &TopKCustomOrdFactories<DynData, DynData, DynData, DynZWeight>,
        k: usize,
        encode: Box<dyn Fn(&DynData, &mut DynData)>,
        decode: Box<dyn Fn(&DynData) -> &DynData>,
    ) -> Self {
        self.dyn_topk_custom_order(persistent_id, factories, k, encode, decode)
    }

    pub fn dyn_topk_rank_custom_order_mono(
        &self,
        persistent_id: Option<&str>,
        factories: &TopKRankCustomOrdFactories<DynData, DynData, DynData>,
        k: usize,
        encode: Box<dyn Fn(&DynData, &mut DynData)>,
        rank_eq_func: Box<dyn Fn(&DynData, &DynData) -> bool>,
        output_func: Box<dyn Fn(i64, &DynData, &mut DynData)>,
    ) -> Stream<RootCircuit, MonoIndexedZSet> {
        self.dyn_topk_rank_custom_order(
            persistent_id,
            factories,
            k,
            encode,
            rank_eq_func,
            output_func,
        )
    }

    pub fn dyn_topk_dense_rank_custom_order_mono(
        &self,
        persistent_id: Option<&str>,
        factories: &TopKRankCustomOrdFactories<DynData, DynData, DynData>,
        k: usize,
        encode: Box<dyn Fn(&DynData, &mut DynData)>,
        rank_eq_func: Box<dyn Fn(&DynData, &DynData) -> bool>,
        output_func: Box<dyn Fn(i64, &DynData, &mut DynData)>,
    ) -> Stream<RootCircuit, MonoIndexedZSet> {
        self.dyn_topk_dense_rank_custom_order(
            persistent_id,
            factories,
            k,
            encode,
            rank_eq_func,
            output_func,
        )
    }

    pub fn dyn_topk_row_number_custom_order_mono(
        &self,
        persistent_id: Option<&str>,
        factories: &TopKRankCustomOrdFactories<DynData, DynData, DynData>,
        k: usize,
        encode: Box<dyn Fn(&DynData, &mut DynData)>,
        output_func: Box<dyn Fn(i64, &DynData, &mut DynData)>,
    ) -> Stream<RootCircuit, MonoIndexedZSet> {
        self.dyn_topk_row_number_custom_order(persistent_id, factories, k, encode, output_func)
    }
}

impl<K, V> Stream<RootCircuit, OrdIndexedZSet<K, V>>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
{
    /// See [`Stream::topk_custom_order`].
    pub fn dyn_topk_custom_order<V2>(
        &self,
        persistent_id: Option<&str>,
        factories: &TopKCustomOrdFactories<K, V, V2, DynZWeight>,
        k: usize,
        encode: Box<dyn Fn(&V, &mut V2)>,
        decode: Box<dyn Fn(&V2) -> &V>,
    ) -> Self
    where
        V2: DataTrait + ?Sized,
    {
        self.dyn_map_index(
            &factories.inner_factories,
            Box::new(move |(k, v), kv| {
                let (out_k, out_v) = kv.split_mut();
                k.clone_to(out_k);
                encode(v, out_v);
            }),
        )
        .set_persistent_id(
            persistent_id
                .map(|name| format!("{name}-ordered"))
                .as_deref(),
        )
        .dyn_group_transform(
            persistent_id,
            &factories.inner_factories,
            &factories.inner_factories,
            Box::new(DiffGroupTransformer::new(
                factories.inner_factories.val_factory(),
                TopK::asc(factories.inner_factories.val_factory(), k),
            )),
        )
        .dyn_map_index(
            &factories.input_factories,
            Box::new(move |(k, v), kv| {
                let (out_k, out_v) = kv.split_mut();
                k.clone_to(out_k);
                decode(v).clone_to(out_v);
            }),
        )
    }

    /// See [`Stream::topk_rank_custom_order`].
    pub fn dyn_topk_rank_custom_order<V2, OV>(
        &self,
        persistent_id: Option<&str>,
        factories: &TopKRankCustomOrdFactories<K, V2, OV>,
        k: usize,
        encode: Box<dyn Fn(&V, &mut V2)>,
        rank_eq_func: Box<dyn Fn(&V2, &V2) -> bool>,
        output_func: Box<dyn Fn(i64, &V2, &mut OV)>,
    ) -> Stream<RootCircuit, OrdIndexedZSet<K, OV>>
    where
        V2: DataTrait + ?Sized,
        OV: DataTrait + ?Sized,
    {
        self.dyn_map_index(
            &factories.inner_factories,
            Box::new(move |(k, v), kv| {
                let (out_k, out_v) = kv.split_mut();
                k.clone_to(out_k);
                encode(v, out_v);
            }),
        )
        .set_persistent_id(
            persistent_id
                .map(|name| format!("{name}-ordered"))
                .as_deref(),
        )
        .dyn_group_transform(
            persistent_id,
            &factories.inner_factories,
            &factories.output_factories,
            Box::new(DiffGroupTransformer::new(
                factories.output_factories.val_factory(),
                TopKRank::sparse(
                    factories.output_factories.val_factory(),
                    k,
                    rank_eq_func,
                    output_func,
                ),
            )),
        )
    }

    /// See [`Stream::topk_dense_rank_custom_order`].
    pub fn dyn_topk_dense_rank_custom_order<V2, OV>(
        &self,
        persistent_id: Option<&str>,
        factories: &TopKRankCustomOrdFactories<K, V2, OV>,
        k: usize,
        encode: Box<dyn Fn(&V, &mut V2)>,
        rank_eq_func: Box<dyn Fn(&V2, &V2) -> bool>,
        output_func: Box<dyn Fn(i64, &V2, &mut OV)>,
    ) -> Stream<RootCircuit, OrdIndexedZSet<K, OV>>
    where
        V2: DataTrait + ?Sized,
        OV: DataTrait + ?Sized,
    {
        self.dyn_map_index(
            &factories.inner_factories,
            Box::new(move |(k, v), kv| {
                let (out_k, out_v) = kv.split_mut();
                k.clone_to(out_k);
                encode(v, out_v);
            }),
        )
        .set_persistent_id(
            persistent_id
                .map(|name| format!("{name}-ordered"))
                .as_deref(),
        )
        .dyn_group_transform(
            persistent_id,
            &factories.inner_factories,
            &factories.output_factories,
            Box::new(DiffGroupTransformer::new(
                factories.output_factories.val_factory(),
                TopKRank::dense(
                    factories.output_factories.val_factory(),
                    k,
                    rank_eq_func,
                    output_func,
                ),
            )),
        )
    }

    /// See [`Stream::topk_row_number_custom_order`].
    pub fn dyn_topk_row_number_custom_order<V2, OV>(
        &self,
        persistent_id: Option<&str>,
        factories: &TopKRankCustomOrdFactories<K, V2, OV>,
        k: usize,
        encode: Box<dyn Fn(&V, &mut V2)>,
        output_func: Box<dyn Fn(i64, &V2, &mut OV)>,
    ) -> Stream<RootCircuit, OrdIndexedZSet<K, OV>>
    where
        V2: DataTrait + ?Sized,
        OV: DataTrait + ?Sized,
    {
        self.dyn_map_index(
            &factories.inner_factories,
            Box::new(move |(k, v), kv| {
                let (out_k, out_v) = kv.split_mut();
                k.clone_to(out_k);
                encode(v, out_v);
            }),
        )
        .set_persistent_id(
            persistent_id
                .map(|name| format!("{name}-ordered"))
                .as_deref(),
        )
        .dyn_group_transform(
            persistent_id,
            &factories.inner_factories,
            &factories.output_factories,
            Box::new(DiffGroupTransformer::new(
                factories.output_factories.val_factory(),
                TopKRowNumber::new(factories.output_factories.val_factory(), k, output_func),
            )),
        )
    }
}

struct TopK<I, const ASCENDING: bool>
where
    I: DataTrait + ?Sized,
{
    key_factory: &'static dyn Factory<I>,
    k: usize,
    name: String,
    // asc: bool,
    _phantom: PhantomData<fn(&I)>,
}

impl<I: DataTrait + ?Sized> TopK<I, true> {
    fn asc(key_factory: &'static dyn Factory<I>, k: usize) -> Self {
        Self {
            key_factory,
            k,
            name: format!("top-{k}-asc"),
            _phantom: PhantomData,
        }
    }
}

impl<I: DataTrait + ?Sized> TopK<I, false> {
    fn desc(key_factory: &'static dyn Factory<I>, k: usize) -> Self {
        Self {
            key_factory,
            k,
            name: format!("top-{k}-desc"),
            _phantom: PhantomData,
        }
    }
}

impl<I, const ASCENDING: bool> NonIncrementalGroupTransformer<I, I> for TopK<I, ASCENDING>
where
    I: DataTrait + ?Sized,
{
    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn monotonicity(&self) -> Monotonicity {
        if ASCENDING {
            Monotonicity::Ascending
        } else {
            Monotonicity::Descending
        }
    }

    fn transform<C, CB>(&mut self, cursor: &mut C, mut output_cb: CB)
    where
        C: ZCursor<I, DynUnit, ()>,
        CB: FnMut(&mut I, &mut DynZWeight),
    {
        let mut count = 0usize;
        let mut key = self.key_factory.default_box();

        if ASCENDING {
            while cursor.key_valid() && count < self.k {
                let mut w = **cursor.weight();
                debug_assert!(w != 0);
                cursor.key().clone_to(&mut key);
                output_cb(&mut key, w.erase_mut());
                count += 1;
                cursor.step_key();
            }
        } else {
            cursor.fast_forward_keys();

            while cursor.key_valid() && count < self.k {
                let mut w = **cursor.weight();
                debug_assert!(w != 0);

                cursor.key().clone_to(&mut key);
                output_cb(&mut key, w.erase_mut());
                count += 1;
                cursor.step_key_reverse();
            }
        }
    }
}

struct TopKRank<I: ?Sized, O: DataTrait + ?Sized> {
    output_factory: &'static dyn Factory<O>,
    k: usize,
    dense: bool,
    name: String,
    rank_eq_func: Box<dyn Fn(&I, &I) -> bool>,
    output_func: Box<dyn Fn(i64, &I, &mut O)>,
    _phantom: PhantomData<fn(&I)>,
}

impl<I: ?Sized, O: DataTrait + ?Sized> TopKRank<I, O> {
    fn sparse(
        output_factory: &'static dyn Factory<O>,
        k: usize,
        rank_eq_func: Box<dyn Fn(&I, &I) -> bool>,
        output_func: Box<dyn Fn(i64, &I, &mut O)>,
    ) -> Self {
        Self {
            output_factory,
            k,
            dense: false,
            name: format!("top-{k}-rank"),
            rank_eq_func,
            output_func,
            _phantom: PhantomData,
        }
    }

    fn dense(
        output_factory: &'static dyn Factory<O>,
        k: usize,
        rank_eq_func: Box<dyn Fn(&I, &I) -> bool>,
        output_func: Box<dyn Fn(i64, &I, &mut O)>,
    ) -> Self {
        Self {
            output_factory,
            k,
            dense: true,
            name: format!("top-{k}-dense-rank"),
            rank_eq_func,
            output_func,
            _phantom: PhantomData,
        }
    }
}

impl<I, OV> NonIncrementalGroupTransformer<I, OV> for TopKRank<I, OV>
where
    I: DataTrait + ?Sized,
    OV: DataTrait + ?Sized,
{
    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn monotonicity(&self) -> Monotonicity {
        // We don't assume that `OF` preserves ordering.
        Monotonicity::Unordered
    }

    fn transform<C, CB>(&mut self, cursor: &mut C, mut output_cb: CB)
    where
        C: ZCursor<I, DynUnit, ()>,
        CB: FnMut(&mut OV, &mut DynZWeight),
    {
        let mut count = 0i64;

        let mut output_val = self.output_factory.default_box();

        let mut rank = 1;
        let mut prev_key: Option<Box<I>> = None;
        while cursor.key_valid() {
            let mut w = **cursor.weight();
            debug_assert!(w != 0);

            if w > 0 {
                count += w;
                let key = cursor.key();
                if let Some(prev_key) = &prev_key {
                    if !(self.rank_eq_func)(key, prev_key.as_ref()) {
                        // Rank stays the same while iterating over equal-ranked elements,
                        // and then increases by one when computing dense ranking or skips
                        // to `count` otherwise.
                        if self.dense {
                            rank += 1;
                        } else {
                            rank = count;
                        }
                        if rank as usize > self.k {
                            break;
                        }
                    }
                };

                (self.output_func)(rank, key, &mut output_val);
                output_cb(&mut output_val, w.erase_mut());
                prev_key = Some(clone_box(key));
            }
            cursor.step_key();
        }
    }
}

struct TopKRowNumber<I: ?Sized, OV: DataTrait + ?Sized> {
    output_factory: &'static dyn Factory<OV>,
    k: usize,
    name: String,
    output_func: Box<dyn Fn(i64, &I, &mut OV)>,
    _phantom: PhantomData<fn(&I, &OV)>,
}

impl<I: ?Sized, OV: DataTrait + ?Sized> TopKRowNumber<I, OV> {
    fn new(
        output_factory: &'static dyn Factory<OV>,
        k: usize,
        output_func: Box<dyn Fn(i64, &I, &mut OV)>,
    ) -> Self {
        Self {
            output_factory,
            k,
            name: format!("top-{k}-row_number"),
            output_func,
            _phantom: PhantomData,
        }
    }
}

impl<I, OV> NonIncrementalGroupTransformer<I, OV> for TopKRowNumber<I, OV>
where
    I: DataTrait + ?Sized,
    OV: DataTrait + ?Sized,
{
    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn monotonicity(&self) -> Monotonicity {
        // We don't assume that `OF` preserves ordering.
        Monotonicity::Unordered
    }

    fn transform<C, CB>(&mut self, cursor: &mut C, mut output_cb: CB)
    where
        C: ZCursor<I, DynUnit, ()>,
        CB: FnMut(&mut OV, &mut DynZWeight),
    {
        let mut count = 0usize;
        let mut output_val = self.output_factory.default_box();

        while cursor.key_valid() && count < self.k {
            let mut w = **cursor.weight();
            debug_assert!(w != 0);

            while w.ge0() && !w.is_zero() {
                count += 1;
                if count > self.k {
                    break;
                }
                (self.output_func)(count as i64, cursor.key(), &mut output_val);
                output_cb(&mut output_val, ZWeight::one().erase_mut());
                AddAssignByRef::add_assign_by_ref(&mut w, &ZWeight::one().neg());
            }
            cursor.step_key();
        }
    }
}
