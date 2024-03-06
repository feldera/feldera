//! Filter and transform data record-by-record.

use crate::trace::ord::file::indexed_zset_batch::FileIndexedZSet;
use crate::trace::ord::file::zset_batch::FileZSet;
use crate::{
    circuit::{
        operator_traits::{Operator, UnaryOperator},
        Circuit, OwnershipPreference, Scope, Stream,
    },
    dynamic::{ClonableTrait, DataTrait, DynPair, DynUnit, WeightTrait},
    trace::{
        Batch, BatchFactories, BatchReader, BatchReaderFactories, Builder, Cursor, OrdIndexedWSet,
        OrdIndexedWSetFactories, OrdWSet, OrdWSetFactories,
    },
};
use std::{borrow::Cow, marker::PhantomData};

/// See [`crate::operator::FilterMap`].
pub trait DynFilterMap: BatchReader {
    /// A borrowed version of the record type, e.g., `(&K, &V)` for a stream of
    /// `(key, value, weight)` tuples or `&K` if the value type is `()`.
    type DynItemRef<'a>;

    fn item_ref<'a>(key: &'a Self::Key, val: &'a Self::Val) -> Self::DynItemRef<'a>;
    fn item_ref_keyval(item_ref: Self::DynItemRef<'_>) -> (&Self::Key, &Self::Val);

    fn dyn_filter<C: Circuit>(
        stream: &Stream<C, Self>,
        filter_func: Box<dyn Fn(Self::DynItemRef<'_>) -> bool>,
    ) -> Stream<C, Self>;

    fn dyn_map_generic<C: Circuit, O>(
        stream: &Stream<C, Self>,
        output_factories: &O::Factories,
        map_func: Box<dyn Fn(Self::DynItemRef<'_>, &mut DynPair<O::Key, O::Val>)>,
    ) -> Stream<C, O>
    where
        O: Batch<Time = (), R = Self::R>;

    fn dyn_flat_map_generic<C: Circuit, O>(
        stream: &Stream<C, Self>,
        output_factories: &O::Factories,
        func: Box<dyn FnMut(Self::DynItemRef<'_>, &mut dyn FnMut(&mut O::Key, &mut O::Val))>,
    ) -> Stream<C, O>
    where
        O: Batch<Time = (), R = Self::R> + Clone + 'static;
}

impl<C: Circuit, B: DynFilterMap> Stream<C, B> {
    /// See [`Stream::filter`].
    pub fn dyn_filter(&self, filter_func: Box<dyn Fn(B::DynItemRef<'_>) -> bool>) -> Self {
        DynFilterMap::dyn_filter(self, filter_func)
    }

    /// See [`Stream::map`].
    pub fn dyn_map<K: DataTrait + ?Sized>(
        &self,
        output_factories: &OrdWSetFactories<K, B::R>,
        map_func: Box<dyn Fn(B::DynItemRef<'_>, &mut DynPair<K, DynUnit>)>,
    ) -> Stream<C, OrdWSet<K, B::R>> {
        DynFilterMap::dyn_map_generic(self, output_factories, map_func)
    }

    /// Behaves as [`Self::dyn_map`] followed by
    /// [`index`](`crate::Stream::index`), but is more efficient.  Assembles
    /// output records into `OrdIndexedZSet` batches.
    pub fn dyn_map_index<K: DataTrait + ?Sized, V: DataTrait + ?Sized>(
        &self,
        output_factories: &OrdIndexedWSetFactories<K, V, B::R>,
        map_func: Box<dyn Fn(B::DynItemRef<'_>, &mut DynPair<K, V>)>,
    ) -> Stream<C, OrdIndexedWSet<K, V, B::R>> {
        DynFilterMap::dyn_map_generic(self, output_factories, map_func)
    }

    /// Like [`Self::dyn_map_index`], but can return any batch type.
    pub fn dyn_map_generic<O>(
        &self,
        output_factories: &O::Factories,
        map_func: Box<dyn Fn(B::DynItemRef<'_>, &mut DynPair<O::Key, O::Val>)>,
    ) -> Stream<C, O>
    where
        O: Batch<Time = (), R = B::R>,
    {
        DynFilterMap::dyn_map_generic(self, output_factories, map_func)
    }

    /// See [`Stream::flat_map`].
    pub fn dyn_flat_map<K: DataTrait + ?Sized>(
        &self,
        output_factories: &OrdWSetFactories<K, B::R>,
        func: Box<dyn FnMut(B::DynItemRef<'_>, &mut dyn FnMut(&mut K, &mut DynUnit))>,
    ) -> Stream<C, OrdWSet<K, B::R>> {
        DynFilterMap::dyn_flat_map_generic(self, output_factories, func)
    }

    /// See [`Stream::flat_map_index`].
    pub fn dyn_flat_map_index<K: DataTrait + ?Sized, V: DataTrait + ?Sized>(
        &self,
        output_factories: &OrdIndexedWSetFactories<K, V, B::R>,
        func: Box<dyn FnMut(B::DynItemRef<'_>, &mut dyn FnMut(&mut K, &mut V))>,
    ) -> Stream<C, OrdIndexedWSet<K, V, B::R>> {
        DynFilterMap::dyn_flat_map_generic(self, output_factories, func)
    }

    /// Like [`Self::dyn_flat_map_index`], but can return any batch type.
    pub fn dyn_flat_map_generic<O>(
        &self,
        output_factories: &O::Factories,
        func: Box<dyn FnMut(B::DynItemRef<'_>, &mut dyn FnMut(&mut O::Key, &mut O::Val))>,
    ) -> Stream<C, O>
    where
        O: Batch<Time = (), R = B::R> + Clone + 'static,
    {
        DynFilterMap::dyn_flat_map_generic(self, output_factories, func)
    }
}

// This impl for VecZSet is identical to the one for FileZSet below.  There
// doesn't seem to be a good way to avoid the code duplication short of a macro.
impl<K, R> DynFilterMap for OrdWSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    // type Item = K;
    type DynItemRef<'a> = &'a K;

    fn item_ref<'a>(key: &'a Self::Key, _val: &'a Self::Val) -> Self::DynItemRef<'a> {
        key
    }

    fn item_ref_keyval(item_ref: Self::DynItemRef<'_>) -> (&Self::Key, &Self::Val) {
        (item_ref, &())
    }

    fn dyn_filter<C: Circuit>(
        stream: &Stream<C, Self>,
        filter_func: Box<dyn Fn(Self::DynItemRef<'_>) -> bool>,
    ) -> Stream<C, Self> {
        let filtered = stream
            .circuit()
            .add_unary_operator(FilterZSet::new(filter_func), &stream.try_sharded_version());
        filtered.mark_sharded_if(stream);
        if stream.is_distinct() {
            filtered.mark_distinct();
        }
        filtered
    }

    fn dyn_map_generic<C: Circuit, O>(
        stream: &Stream<C, Self>,
        output_factories: &O::Factories,
        map_func: Box<dyn Fn(Self::DynItemRef<'_>, &mut DynPair<O::Key, O::Val>)>,
    ) -> Stream<C, O>
    where
        O: Batch<Time = (), R = Self::R>,
    {
        stream
            .circuit()
            .add_unary_operator(MapZSet::new(output_factories, map_func), stream)
    }

    fn dyn_flat_map_generic<C: Circuit, O>(
        stream: &Stream<C, Self>,
        output_factories: &O::Factories,
        func: Box<dyn FnMut(Self::DynItemRef<'_>, &mut dyn FnMut(&mut O::Key, &mut O::Val))>,
    ) -> Stream<C, O>
    where
        O: Batch<Time = (), R = Self::R>,
    {
        stream
            .circuit()
            .add_unary_operator(FlatMapZSet::new(output_factories, func), stream)
    }
}

impl<K, V, R> DynFilterMap for OrdIndexedWSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    // type Item = (K, V);
    type DynItemRef<'a> = (&'a K, &'a V);

    fn item_ref<'a>(key: &'a Self::Key, val: &'a Self::Val) -> Self::DynItemRef<'a> {
        (key, val)
    }

    fn item_ref_keyval(item_ref: Self::DynItemRef<'_>) -> (&Self::Key, &Self::Val) {
        item_ref
    }

    fn dyn_filter<C: Circuit>(
        stream: &Stream<C, Self>,
        filter_func: Box<dyn Fn(Self::DynItemRef<'_>) -> bool>,
    ) -> Stream<C, Self> {
        let filtered = stream.circuit().add_unary_operator(
            FilterIndexedZSet::new(filter_func),
            &stream.try_sharded_version(),
        );
        filtered.mark_sharded_if(stream);
        if stream.is_distinct() {
            filtered.mark_distinct();
        }
        filtered
    }

    fn dyn_map_generic<C: Circuit, O>(
        stream: &Stream<C, Self>,
        output_factories: &O::Factories,
        map_func: Box<dyn Fn(Self::DynItemRef<'_>, &mut DynPair<O::Key, O::Val>)>,
    ) -> Stream<C, O>
    where
        O: Batch<Time = (), R = Self::R>,
    {
        stream
            .circuit()
            .add_unary_operator(MapIndexedZSet::new(output_factories, map_func), stream)
    }

    fn dyn_flat_map_generic<C: Circuit, O>(
        stream: &Stream<C, Self>,
        output_factories: &O::Factories,
        func: Box<dyn FnMut(Self::DynItemRef<'_>, &mut dyn FnMut(&mut O::Key, &mut O::Val))>,
    ) -> Stream<C, O>
    where
        O: Batch<Time = (), R = Self::R>,
    {
        stream
            .circuit()
            .add_unary_operator(FlatMapIndexedZSet::new(output_factories, func), stream)
    }
}

impl<K, R> DynFilterMap for FileZSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    // type Item = K;
    type DynItemRef<'a> = &'a K;

    fn item_ref<'a>(key: &'a Self::Key, _val: &'a Self::Val) -> Self::DynItemRef<'a> {
        key
    }

    fn item_ref_keyval(item_ref: Self::DynItemRef<'_>) -> (&Self::Key, &Self::Val) {
        (item_ref, &())
    }

    fn dyn_filter<C: Circuit>(
        stream: &Stream<C, Self>,
        filter_func: Box<dyn Fn(Self::DynItemRef<'_>) -> bool>,
    ) -> Stream<C, Self> {
        let filtered = stream
            .circuit()
            .add_unary_operator(FilterZSet::new(filter_func), &stream.try_sharded_version());
        filtered.mark_sharded_if(stream);
        if stream.is_distinct() {
            filtered.mark_distinct();
        }
        filtered
    }

    fn dyn_map_generic<C: Circuit, O>(
        stream: &Stream<C, Self>,
        output_factories: &O::Factories,
        map_func: Box<dyn Fn(Self::DynItemRef<'_>, &mut DynPair<O::Key, O::Val>)>,
    ) -> Stream<C, O>
    where
        O: Batch<Time = (), R = Self::R>,
    {
        stream
            .circuit()
            .add_unary_operator(MapZSet::new(output_factories, map_func), stream)
    }

    fn dyn_flat_map_generic<C: Circuit, O>(
        stream: &Stream<C, Self>,
        output_factories: &O::Factories,
        func: Box<dyn FnMut(Self::DynItemRef<'_>, &mut dyn FnMut(&mut O::Key, &mut O::Val))>,
    ) -> Stream<C, O>
    where
        O: Batch<Time = (), R = Self::R>,
    {
        stream
            .circuit()
            .add_unary_operator(FlatMapZSet::new(output_factories, func), stream)
    }
}

impl<K, V, R> DynFilterMap for FileIndexedZSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    // type Item = (K, V);
    type DynItemRef<'a> = (&'a K, &'a V);

    fn item_ref<'a>(key: &'a Self::Key, val: &'a Self::Val) -> Self::DynItemRef<'a> {
        (key, val)
    }

    fn item_ref_keyval(item_ref: Self::DynItemRef<'_>) -> (&Self::Key, &Self::Val) {
        item_ref
    }

    fn dyn_filter<C: Circuit>(
        stream: &Stream<C, Self>,
        filter_func: Box<dyn Fn(Self::DynItemRef<'_>) -> bool>,
    ) -> Stream<C, Self> {
        let filtered = stream.circuit().add_unary_operator(
            FilterIndexedZSet::new(filter_func),
            &stream.try_sharded_version(),
        );
        filtered.mark_sharded_if(stream);
        if stream.is_distinct() {
            filtered.mark_distinct();
        }
        filtered
    }

    fn dyn_map_generic<C: Circuit, O>(
        stream: &Stream<C, Self>,
        output_factories: &O::Factories,
        map_func: Box<dyn Fn(Self::DynItemRef<'_>, &mut DynPair<O::Key, O::Val>)>,
    ) -> Stream<C, O>
    where
        O: Batch<Time = (), R = Self::R>,
    {
        stream
            .circuit()
            .add_unary_operator(MapIndexedZSet::new(output_factories, map_func), stream)
    }

    fn dyn_flat_map_generic<C: Circuit, O>(
        stream: &Stream<C, Self>,
        output_factories: &O::Factories,
        func: Box<dyn FnMut(Self::DynItemRef<'_>, &mut dyn FnMut(&mut O::Key, &mut O::Val))>,
    ) -> Stream<C, O>
    where
        O: Batch<Time = (), R = Self::R>,
    {
        stream
            .circuit()
            .add_unary_operator(FlatMapIndexedZSet::new(output_factories, func), stream)
    }
}

/// Internal implementation for filtering [`BatchReader`]s
pub struct FilterZSet<B: Batch> {
    filter: Box<dyn Fn(&B::Key) -> bool>,
    _type: PhantomData<*const B>,
}

impl<B: Batch> FilterZSet<B> {
    pub fn new(filter: Box<dyn Fn(&B::Key) -> bool>) -> Self {
        Self {
            filter,
            _type: PhantomData,
        }
    }

    // fn filter_owned_generic(&mut self, input: CI) -> CO
    // where
    //     CI: BatchReader<Time = ()>,
    //     CO: Batch<Key = CI::Key, Val = CI::Val, Time = (), R = CI::R>,
    //     F: Fn(&CI::Key) -> bool + 'static,
    // {
    //     // We can use Builder because cursor yields ordered values.  This
    //     // is a nice property of the filter operation.
    //     //
    //     // Pre-allocating will create waste if most tuples get filtered out,
    // since     // the buffers allocated here can make it all the way to the
    // output batch.     // This is probably ok, because the batch will either
    // get freed at the end     // of the current clock tick or get added to the
    // trace, where it will likely     // get merged with other batches soon, at
    // which point the waste is gone.     let mut builder =
    // CO::Builder::with_capacity((), input.len());

    //     let mut consumer = input.consumer();
    //     while consumer.key_valid() {
    //         let (key, mut values) = consumer.next_key();

    //         // FIXME: We assume that this operator will only be used with
    // `OrdZSet` and that         // it has either zero or one values, meaning
    // that it'll skip any additional         // values
    //         if (self.filter)(&key) && values.value_valid() {
    //             let (value, weight, ()) = values.next_value();
    //             builder.push((CO::item_from(key, value), weight));
    //         }
    //     }
    //     builder.done()
    // }
}

impl<B: Batch> Operator for FilterZSet<B> {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("FilterKeys")
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<B> UnaryOperator<B, B> for FilterZSet<B>
where
    B: Batch<Time = ()>,
{
    fn eval(&mut self, input: &B) -> B {
        // We can use Builder because cursor yields ordered values.  This
        // is a nice property of the filter operation.
        //
        // Pre-allocating will create waste if most tuples get filtered out, since
        // the buffers allocated here can make it all the way to the output batch.
        // This is probably ok, because the batch will either get freed at the end
        // of the current clock tick or get added to the trace, where it will likely
        // get merged with other batches soon, at which point the waste is gone.
        let mut builder = B::Builder::with_capacity(&input.factories(), (), input.len());
        let mut weight = input.factories().weight_factory().default_box();

        let mut cursor = input.cursor();
        while cursor.key_valid() {
            if (self.filter)(cursor.key()) {
                while cursor.val_valid() {
                    cursor.weight().clone_to(&mut *weight);
                    Builder::push_refs(&mut builder, cursor.key(), cursor.val(), &*weight);
                    cursor.step_val();
                }
            }
            cursor.step_key();
        }

        builder.done()
    }

    // Enable this optimization when we have `retain`.
    /*fn eval_owned(&mut self, input: CI) -> CO {
        // Bootleg specialization, we can do filtering in-place when the input and
        // output types are `OrdZSet`. I'd prefer to do this with "real" specialization
        // of some kind, but this'll work for now
        if TypeId::of::<CI>() == TypeId::of::<OrdZSet<CI::Key, CI::R>>()
            && TypeId::of::<CO>() == TypeId::of::<OrdZSet<CI::Key, CI::R>>()
        {
            // Safety: We've ensured that `CI` is an `OrdZSet`
            let mut output = unsafe {
                let input = ManuallyDrop::new(input);
                transmute_copy::<CI, OrdZSet<CI::Key, CI::R>>(&input)
            };

            // Filter the values within the `OrdZSet`
            output.retain(|key, _| (self.filter)(key));

            // Safety: We've ensured that `CO` is an `OrdZSet`
            unsafe {
                let output = ManuallyDrop::new(output);
                transmute_copy::<OrdZSet<CI::Key, CI::R>, CO>(&output)
            }
        } else {
            // Use a generic filter implementation
            self.filter_owned_generic(input)
        }
    }*/

    // Filtering *wants* owned values, but it's not critical to performance
    fn input_preference(&self) -> OwnershipPreference {
        /*if TypeId::of::<CI>() == TypeId::of::<OrdZSet<CI::Key, CI::R>>()
            && TypeId::of::<CO>() == TypeId::of::<OrdZSet<CI::Key, CI::R>>()
        {
            // Owned values matter a lot more when the input and output types are both
            // OrdZSet
            OwnershipPreference::PREFER_OWNED
        } else {
            OwnershipPreference::WEAKLY_PREFER_OWNED
        }*/
        OwnershipPreference::INDIFFERENT
    }
}

/// Internal implementation for filtering [`BatchReader`]s
pub struct FilterIndexedZSet<B: Batch> {
    filter: Box<dyn for<'a> Fn((&'a B::Key, &'a B::Val)) -> bool>,
    _type: PhantomData<B>,
}

impl<B: Batch> FilterIndexedZSet<B> {
    pub fn new(filter: Box<dyn for<'a> Fn((&'a B::Key, &'a B::Val)) -> bool>) -> Self {
        Self {
            filter,
            _type: PhantomData,
        }
    }
}

impl<B: Batch> Operator for FilterIndexedZSet<B> {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("FilterVals")
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<B> UnaryOperator<B, B> for FilterIndexedZSet<B>
where
    B: Batch<Time = ()>,
{
    fn eval(&mut self, input: &B) -> B {
        // We can use Builder because cursor yields ordered values.  This
        // is a nice property of the filter operation.

        // This will create waste if most tuples get filtered out, since
        // the buffers allocated here can make it all the way to the output batch.
        // This is probably ok, because the batch will either get freed at the end
        // of the current clock tick or get added to the trace, where it will likely
        // get merged with other batches soon, at which point the waste is gone.
        let mut builder = B::Builder::with_capacity(&input.factories(), (), input.len());
        let mut weight = input.factories().weight_factory().default_box();

        let mut cursor = input.cursor();
        while cursor.key_valid() {
            while cursor.val_valid() {
                if (self.filter)((cursor.key(), cursor.val())) {
                    cursor.weight().clone_to(&mut *weight);
                    Builder::push_refs(&mut builder, cursor.key(), cursor.val(), &*weight);
                }
                cursor.step_val();
            }
            cursor.step_key();
        }

        builder.done()
    }

    /*fn eval_owned(&mut self, input: CI) -> CO {
        let mut builder = CO::Builder::with_capacity((), input.len());

        let mut consumer = input.consumer();
        while consumer.key_valid() {
            let (key, mut values) = consumer.next_key();

            while values.value_valid() {
                let (value, diff, ()) = values.next_value();

                if (self.filter)((&key, &value)) {
                    builder.push((CO::item_from(key.clone(), value), diff));
                }
            }
        }

        builder.done()
    }*/

    fn input_preference(&self) -> OwnershipPreference {
        OwnershipPreference::INDIFFERENT
    }
}

pub struct MapZSet<CI, CO>
where
    CI: BatchReader,
    CO: Batch,
{
    output_factories: CO::Factories,
    map: Box<dyn for<'a> Fn(&'a CI::Key, &mut DynPair<CO::Key, CO::Val>)>,
    _type: PhantomData<(CI, CO)>,
}

impl<CI, CO> MapZSet<CI, CO>
where
    CI: BatchReader,
    CO: Batch,
{
    pub fn new(
        output_factories: &CO::Factories,
        map: Box<dyn for<'a> Fn(&'a CI::Key, &mut DynPair<CO::Key, CO::Val>)>,
    ) -> Self {
        Self {
            output_factories: output_factories.clone(),
            map,
            _type: PhantomData,
        }
    }
}

impl<CI, CO> Operator for MapZSet<CI, CO>
where
    CI: BatchReader,
    CO: Batch,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("Map")
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<CI, CO> UnaryOperator<CI, CO> for MapZSet<CI, CO>
where
    CI: BatchReader<Time = ()>,
    CO: Batch<Time = (), R = CI::R>,
{
    fn eval(&mut self, i: &CI) -> CO {
        let mut batch = self.output_factories.weighted_items_factory().default_box();
        batch.reserve(i.len());
        let mut item = self.output_factories.weighted_item_factory().default_box();

        let mut cursor = i.cursor();
        while cursor.key_valid() {
            while cursor.val_valid() {
                let (kv, weight) = item.split_mut();

                (self.map)(cursor.key(), kv);
                cursor.weight().clone_to(weight);

                batch.push_val(&mut *item);
                cursor.step_val();
            }
            cursor.step_key();
        }

        CO::dyn_from_tuples(&self.output_factories, (), &mut batch)
    }
}

pub struct MapIndexedZSet<CI, CO>
where
    CI: BatchReader,
    CO: Batch,
{
    output_factories: CO::Factories,
    map: Box<dyn for<'a> Fn((&'a CI::Key, &'a CI::Val), &mut DynPair<CO::Key, CO::Val>)>,
    _type: PhantomData<(CI, CO)>,
}

impl<CI, CO> MapIndexedZSet<CI, CO>
where
    CI: BatchReader,
    CO: Batch,
{
    pub fn new(
        output_factories: &CO::Factories,
        map: Box<dyn for<'a> Fn((&'a CI::Key, &'a CI::Val), &mut DynPair<CO::Key, CO::Val>)>,
    ) -> Self {
        Self {
            output_factories: output_factories.clone(),
            map,
            _type: PhantomData,
        }
    }
}

impl<CI, CO> Operator for MapIndexedZSet<CI, CO>
where
    CI: BatchReader,
    CO: Batch,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("Map")
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<CI, CO> UnaryOperator<CI, CO> for MapIndexedZSet<CI, CO>
where
    CI: BatchReader<Time = ()>,
    CO: Batch<Time = (), R = CI::R>,
{
    fn eval(&mut self, i: &CI) -> CO {
        let mut batch = self.output_factories.weighted_items_factory().default_box();
        batch.reserve(i.len());
        let mut item = self.output_factories.weighted_item_factory().default_box();

        let mut cursor = i.cursor();
        while cursor.key_valid() {
            while cursor.val_valid() {
                let (kv, weight) = item.split_mut();

                (self.map)((cursor.key(), cursor.val()), kv);
                cursor.weight().clone_to(weight);

                batch.push_val(&mut *item);
                cursor.step_val();
            }
            cursor.step_key();
        }

        CO::dyn_from_tuples(&self.output_factories, (), &mut batch)
    }
}

pub struct FlatMapZSet<CI: BatchReader, CO: Batch> {
    output_factories: CO::Factories,
    func: Box<dyn for<'a> FnMut(&'a CI::Key, &mut dyn FnMut(&mut CO::Key, &mut CO::Val))>,
    _type: PhantomData<(CI, CO)>,
}

impl<CI: BatchReader, CO: Batch> FlatMapZSet<CI, CO> {
    pub fn new(
        output_factories: &CO::Factories,
        func: Box<dyn for<'a> FnMut(&'a CI::Key, &mut dyn FnMut(&mut CO::Key, &mut CO::Val))>,
    ) -> Self {
        Self {
            output_factories: output_factories.clone(),
            func,
            _type: PhantomData,
        }
    }
}

impl<CI, CO> Operator for FlatMapZSet<CI, CO>
where
    CI: BatchReader,
    CO: Batch,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("FlatMap")
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<CI, CO> UnaryOperator<CI, CO> for FlatMapZSet<CI, CO>
where
    CI: BatchReader<Time = ()>,
    CO: Batch<Time = (), R = CI::R>,
{
    fn eval(&mut self, i: &CI) -> CO {
        let mut batch = self.output_factories.weighted_items_factory().default_box();
        batch.reserve(i.len());

        let mut weight = self.output_factories.weight_factory().default_box();

        let mut weighted_item = self.output_factories.weighted_item_factory().default_box();

        let mut cursor = i.cursor();

        while cursor.key_valid() {
            while cursor.val_valid() {
                cursor.weight().clone_to(&mut weight);

                (self.func)(cursor.key(), &mut |k, v| {
                    let (output_item, output_weight) = weighted_item.split_mut();
                    weight.clone_to(output_weight);
                    let (key, val) = output_item.split_mut();
                    k.clone_to(key);
                    v.clone_to(val);
                    batch.push_val(&mut *weighted_item);
                });

                // // Reserve capacity for the given elements
                // let (low, high) = values.size_hint();
                // batch.reserve(high.unwrap_or(low));

                cursor.step_val();
            }

            cursor.step_key();
        }

        CO::dyn_from_tuples(&self.output_factories, (), &mut batch)
    }
}

pub struct FlatMapIndexedZSet<CI: BatchReader, CO: Batch> {
    output_factories: CO::Factories,
    func: Box<
        dyn for<'a> FnMut((&'a CI::Key, &'a CI::Val), &mut dyn FnMut(&mut CO::Key, &mut CO::Val)),
    >,
    _type: PhantomData<(CI, CO)>,
}

impl<CI: BatchReader, CO: Batch> FlatMapIndexedZSet<CI, CO> {
    pub fn new(
        output_factories: &CO::Factories,
        func: Box<
            dyn for<'a> FnMut(
                (&'a CI::Key, &'a CI::Val),
                &mut dyn FnMut(&mut CO::Key, &mut CO::Val),
            ),
        >,
    ) -> Self {
        Self {
            output_factories: output_factories.clone(),
            func,
            _type: PhantomData,
        }
    }
}

impl<CI, CO> Operator for FlatMapIndexedZSet<CI, CO>
where
    CI: BatchReader,
    CO: Batch,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("FlatMap")
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<CI, CO> UnaryOperator<CI, CO> for FlatMapIndexedZSet<CI, CO>
where
    CI: BatchReader<Time = ()>,
    CO: Batch<Time = (), R = CI::R>,
{
    fn eval(&mut self, i: &CI) -> CO {
        let mut batch = self.output_factories.weighted_items_factory().default_box();
        batch.reserve(i.len());

        let mut weight = self.output_factories.weight_factory().default_box();

        let mut weighted_item = self.output_factories.weighted_item_factory().default_box();

        let mut cursor = i.cursor();

        while cursor.key_valid() {
            while cursor.val_valid() {
                cursor.weight().clone_to(&mut weight);

                (self.func)((cursor.key(), cursor.val()), &mut |k, v| {
                    let (output_item, output_weight) = weighted_item.split_mut();
                    let (key, val) = output_item.split_mut();
                    weight.clone_to(output_weight);
                    k.clone_to(key);
                    v.clone_to(val);
                    batch.push_val(&mut *weighted_item);
                });

                // // Reserve capacity for the given elements
                // let (low, high) = values.size_hint();
                // batch.reserve(high.unwrap_or(low));

                cursor.step_val();
            }

            cursor.step_key();
        }

        CO::dyn_from_tuples(&self.output_factories, (), &mut batch)
    }
}

#[cfg(test)]
mod test {
    use crate::{
        indexed_zset, operator::Generator, typed_batch::OrdZSet, utils::Tup2, zset, Circuit,
        RootCircuit,
    };
    use std::vec;

    #[test]
    fn filter_map_test() {
        let circuit = RootCircuit::build(move |circuit| {
            let mut input: vec::IntoIter<OrdZSet<Tup2<i64, String>>> =
                vec![zset! { Tup2(1, "1".to_string()) => 1, Tup2(-1, "-1".to_string()) => 1, Tup2(5, "5 foo".to_string()) => 1, Tup2(-2, "-2".to_string()) => 1 }].into_iter();

            let mut filter_output =
                vec![zset! { 1 => 1, 5 => 1 }].into_iter();
            let mut i_filter_output =
                vec![indexed_zset! { 5 => {"5 foo".to_string() => 1} }].into_iter();
            let mut indexed_output =
                vec![indexed_zset! { 1 => {1 => 1}, -1 => {-1 => 1}, 5 => {5 => 1}, -2 => {-2 => 1} }].into_iter();
            let mut i_indexed_output =
                vec![indexed_zset! { 2 => {"1".to_string() => 1}, -2 => {"-1".to_string() => 1}, 10 => {"5 foo".to_string() => 1}, -4 => {"-2".to_string() => 1} }].into_iter();
            let mut times2_output =
                vec![zset! { 2 => 1, -2 => 1, 10 => 1, -4 => 1 }].into_iter();
            let mut i_times2_output =
                vec![zset! { 2 => 1, -2 => 1, 10 => 1, -4 => 1 }].into_iter();
            let mut times2_pos_output =
                vec![zset! { 2 => 1, 10 => 1 }].into_iter();
            let mut i_times2_pos_output =
                vec![zset! { 10 => 1 }].into_iter();
            let mut neg_output =
                vec![zset! { -1 => 1, 1 => 1, -5 => 1, 2 => 1}].into_iter();
            let mut i_neg_output =
                vec![zset! { -1 => 1, 1 => 1, -5 => 1, 2 => 1}].into_iter();
            let mut neg_pos_output =
                vec![zset! { -1 => 1, -5 => 1}].into_iter();
            let mut i_neg_pos_output =
                vec![zset! {-5 => 1}].into_iter();
            let mut abs_output =
                vec![zset! { 1 => 2, 5 => 1, 2 => 1 }].into_iter();
            let mut i_abs_output =
                vec![zset! { Tup2(1, "1".to_string()) => 1, Tup2(1, "-1".to_string()) => 1, Tup2(5, "5 foo".to_string()) => 1, Tup2(2, "-2".to_string()) => 1 }].into_iter();
            let mut abs_pos_output =
                vec![zset! { 1 => 1, 5 => 1 }].into_iter();
            let mut i_abs_pos_output =
                vec![zset! { Tup2(1, "1".to_string()) => 1, Tup2(5, "5 foo".to_string()) => 1 }].into_iter();
            let mut sqr_output =
                vec![zset! { 1 => 2, 25 => 1, 4 => 1 }].into_iter();
            let mut i_sqr_output =
                vec![zset! { 1 => 2, 25 => 1, 4 => 1 }].into_iter();
            let mut sqr_pos_output =
                vec![zset! { 1 => 1, 25 => 1 }].into_iter();
            let mut i_sqr_pos_output =
                vec![zset! { 25 => 1 }].into_iter();
            let mut sqr_pos_indexed_output =
                vec![indexed_zset! { 1 => {1 => 1}, 25 => {5 => 1} }].into_iter();
            let mut i_sqr_pos_indexed_output =
                vec![indexed_zset! { 1 => {"1".to_string() => 1}, 25 => {"5 foo".to_string() => 1} }].into_iter();

            let input =
                circuit.add_source(Generator::new(move || input.next().unwrap()));
            let input_indexed = input.map_index(|Tup2(k, v)| (*k, v.clone()));
            let input_ints = input_indexed.map(|(&x, _)| x);

            let filter_pos = input_ints.filter(|&n| n > 0);
            let indexed = input_ints.map_index(|&n| (n, n));
            let times2 = input_ints.map(|&n| n * 2);
            let times2_pos = input_ints.flat_map(|&n| if n > 0 { Some(n * 2) } else { None });
            let neg = input_ints.map(|n| -n);
            let neg_pos = input_ints.flat_map(|n| if *n > 0 { Some(-n) } else { None });
            let abs = input_ints.map(|n| n.abs());
            let abs_pos = input_ints.flat_map(|n| if *n > 0 { Some(n.abs()) } else { None });
            let sqr = input_ints.map(|n| n * n);
            let sqr_pos = input_ints.flat_map(|n| if *n > 0 { Some(n * n) } else { None });
            let sqr_pos_indexed = input_ints.flat_map_index(|&n| if n > 0 { Some((n * n, n)) } else { None });

            let i_filter_pos = input_indexed.filter(|(&n, s)| n > 0 && s.contains("foo"));
            let i_indexed = input_indexed.map_index(move |(&n, s)| (2 * n, s.clone()));
            let i_times2 = input_indexed.map(|(&n, _)| n * 2);
            let i_times2_pos = input_indexed.flat_map(|(&n, s)| if n > 0 && s.contains("foo") { Some(n * 2) } else { None });
            let i_neg = input_indexed.map(|(n, _)| -n);
            let i_neg_pos = input_indexed.flat_map(|(&n, s)| if n > 0 && s.contains("foo") { Some(-n) } else { None });
            let i_abs = input_indexed.map(|(n, s)| Tup2(n.abs(), s.clone()));
            let i_abs_pos = input_indexed.flat_map(|(&n, s)| if n > 0 { Some(Tup2(n.abs(), s.clone())) } else { None });
            let i_sqr = input_indexed.map(|(n, _)| n * n);
            let i_sqr_pos = input_indexed.flat_map(|(&n, s)| if n > 0 && s.contains("foo") { Some(n * n) } else { None });
            let i_sqr_pos_indexed = input_indexed.flat_map_index(|(&n, s)| if n > 0 { Some((n * n, s.clone())) } else { None });

            filter_pos.inspect(move |n| {
                assert_eq!(*n, filter_output.next().unwrap());
            });
            indexed.inspect(move |n| {
                assert_eq!(*n, indexed_output.next().unwrap());
            });
            times2.inspect(move |n| {
                assert_eq!(*n, times2_output.next().unwrap());
            });
            times2_pos.inspect(move |n| {
                assert_eq!(*n, times2_pos_output.next().unwrap());
            });
            neg.inspect(move |n| {
                assert_eq!(*n, neg_output.next().unwrap());
            });
            neg_pos.inspect(move |n| {
                assert_eq!(*n, neg_pos_output.next().unwrap());
            });
            abs.inspect(move |n| {
                assert_eq!(*n, abs_output.next().unwrap());
            });
            abs_pos.inspect(move |n| {
                assert_eq!(*n, abs_pos_output.next().unwrap());
            });
            sqr.inspect(move |n| {
                assert_eq!(*n, sqr_output.next().unwrap());
            });
            sqr_pos.inspect(move |n| {
                assert_eq!(*n, sqr_pos_output.next().unwrap());
            });
            sqr_pos_indexed.inspect(move |n| {
                assert_eq!(*n, sqr_pos_indexed_output.next().unwrap());
            });
            i_filter_pos.inspect(move |n| {
                assert_eq!(*n, i_filter_output.next().unwrap());
            });
            i_indexed.inspect(move |n| {
                assert_eq!(*n, i_indexed_output.next().unwrap());
            });
            i_times2.inspect(move |n| {
                assert_eq!(*n, i_times2_output.next().unwrap());
            });
            i_times2_pos.inspect(move |n| {
                assert_eq!(*n, i_times2_pos_output.next().unwrap());
            });
            i_neg.inspect(move |n| {
                assert_eq!(*n, i_neg_output.next().unwrap());
            });
            i_neg_pos.inspect(move |n| {
                assert_eq!(*n, i_neg_pos_output.next().unwrap());
            });
            i_abs.inspect(move |n| {
                assert_eq!(*n, i_abs_output.next().unwrap());
            });
            i_abs_pos.inspect(move |n| {
                assert_eq!(*n, i_abs_pos_output.next().unwrap());
            });
            i_sqr.inspect(move |n| {
                assert_eq!(*n, i_sqr_output.next().unwrap());
            });
            i_sqr_pos.inspect(move |n| {
                assert_eq!(*n, i_sqr_pos_output.next().unwrap());
            });
            i_sqr_pos_indexed.inspect(move |n| {
                assert_eq!(*n, i_sqr_pos_indexed_output.next().unwrap());
            });
            Ok(())
        })
        .unwrap().0;

        for _ in 0..1 {
            circuit.step().unwrap();
        }
    }

    /// Checks for regression against a bug such that if a distinct version of a
    /// stream existed, `filter` marked the filtered version of the stream as
    /// distinct.
    #[test]
    fn distinct_filter_test() {
        let (circuit, (input_handle, output_handle)) = RootCircuit::build(|circuit| {
            let (input_stream, input_handle) = circuit.add_input_indexed_zset::<i32, ()>();
            let sharded = input_stream.shard();
            assert!(!sharded.inner().is_distinct());

            // Make a distinct version of the stream.  We don't filter this
            // version, but rather the original version, so this should have no
            // effect on whether the filtered version is considered distinct.
            let distinct = sharded.distinct();
            assert!(distinct.inner().is_distinct());

            let filtered = sharded.filter(|key| *key.0 > 0);
            // The bug caused the following assertion to fail.
            assert!(!filtered.inner().is_distinct());

            Ok((input_handle, filtered.distinct_count().output()))
        })
        .unwrap();

        input_handle.append(&mut vec![
            Tup2(-1, ((), 1).into()),
            Tup2(0, ((), 1).into()),
            Tup2(1, ((), 1).into()),
            Tup2(2, ((), 2).into()),
            Tup2(3, ((), 3).into()),
            Tup2(4, ((), 4).into()),
        ]);
        circuit.step().unwrap();
        let output = output_handle.consolidate();
        // The bug caused the weights below to be 1,2,3,4 instead of 1,1,1,1.
        assert_eq!(
            output,
            indexed_zset! {1 => {1 => 1}, 2 => {1 => 1}, 3 => {1 => 1}, 4 => {1 => 1}}
        );
    }
}
