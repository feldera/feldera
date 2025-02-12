use crate::{
    dynamic::{
        DataTrait, DynDataTyped, DynPair, DynUnit, DynVec, DynWeightedPairs, Erase, Factory,
        LeanVec, WeightTrait, WithFactory,
    },
    time::{Antichain, AntichainRef},
    trace::{
        layers::{
            Builder as _, Cursor as _, Layer, LayerCursor, LayerFactories, Leaf, LeafFactories,
            MergeBuilder, OrdOffset, Trie,
        },
        Batch, BatchFactories, BatchLocation, BatchReader, BatchReaderFactories, Builder, Cursor,
        Deserializer, Filter, Merger, Serializer, WeightedItem,
    },
    utils::{ConsolidatePairedSlices, Tup2},
    DBData, DBWeight, NumEntries, Timestamp,
};
use rand::Rng;
use rkyv::{Archive, Deserialize, Serialize};
use size_of::SizeOf;
use std::fmt::{self, Debug, Display};
use std::path::PathBuf;

use crate::trace::ord::merge_batcher::MergeBatcher;

pub struct VecKeyBatchFactories<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    layer_factories: LayerFactories<K, LeafFactories<DynDataTyped<T>, R>>,
    consolidate_weights: &'static dyn ConsolidatePairedSlices<DynDataTyped<T>, R>,
    item_factory: &'static dyn Factory<DynPair<K, DynUnit>>,
    weighted_item_factory: &'static dyn Factory<WeightedItem<K, DynUnit, R>>,
    weighted_items_factory: &'static dyn Factory<DynWeightedPairs<DynPair<K, DynUnit>, R>>,
    weighted_vals_factory: &'static dyn Factory<DynWeightedPairs<DynUnit, R>>,
    time_diffs_factory: &'static dyn Factory<DynWeightedPairs<DynDataTyped<T>, R>>,
}

unsafe impl<K, T, R> Send for VecKeyBatchFactories<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
}

impl<K, T, R> Clone for VecKeyBatchFactories<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            layer_factories: self.layer_factories.clone(),
            consolidate_weights: self.consolidate_weights,
            item_factory: self.item_factory,
            weighted_item_factory: self.weighted_item_factory,
            weighted_items_factory: self.weighted_items_factory,
            weighted_vals_factory: self.weighted_vals_factory,
            time_diffs_factory: self.time_diffs_factory,
        }
    }
}

impl<K, T, R> BatchReaderFactories<K, DynUnit, T, R> for VecKeyBatchFactories<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn new<KType, VType, RType>() -> Self
    where
        KType: DBData + Erase<K>,
        VType: DBData + Erase<DynUnit>,
        RType: DBWeight + Erase<R>,
    {
        Self {
            layer_factories: LayerFactories::new::<KType>(
                <LeafFactories<DynDataTyped<T>, R>>::new::<T, RType>(),
            ),
            consolidate_weights: <dyn ConsolidatePairedSlices<_, _>>::factory::<T, RType>(),
            item_factory: WithFactory::<Tup2<KType, ()>>::FACTORY,
            weighted_item_factory: WithFactory::<Tup2<Tup2<KType, ()>, RType>>::FACTORY,
            weighted_items_factory: WithFactory::<LeanVec<Tup2<Tup2<KType, ()>, RType>>>::FACTORY,
            weighted_vals_factory: WithFactory::<LeanVec<Tup2<(), RType>>>::FACTORY,
            time_diffs_factory: WithFactory::<LeanVec<Tup2<T, RType>>>::FACTORY,
        }
    }

    fn key_factory(&self) -> &'static dyn Factory<K> {
        self.layer_factories.key
    }

    fn keys_factory(&self) -> &'static dyn Factory<DynVec<K>> {
        self.layer_factories.keys
    }

    fn val_factory(&self) -> &'static dyn Factory<DynUnit> {
        WithFactory::<()>::FACTORY
    }

    fn weight_factory(&self) -> &'static dyn Factory<R> {
        self.layer_factories.child.diff
    }
}

impl<K, R, T> BatchFactories<K, DynUnit, T, R> for VecKeyBatchFactories<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    // type BatchItemFactory = BatchItemFactory<K, (), K, R>;

    fn item_factory(&self) -> &'static dyn Factory<DynPair<K, DynUnit>> {
        self.item_factory
    }

    fn weighted_item_factory(&self) -> &'static dyn Factory<WeightedItem<K, DynUnit, R>> {
        self.weighted_item_factory
    }

    fn weighted_items_factory(
        &self,
    ) -> &'static dyn Factory<DynWeightedPairs<DynPair<K, DynUnit>, R>> {
        self.weighted_items_factory
    }

    fn weighted_vals_factory(&self) -> &'static dyn Factory<DynWeightedPairs<DynUnit, R>> {
        self.weighted_vals_factory
    }

    fn time_diffs_factory(
        &self,
    ) -> Option<&'static dyn Factory<DynWeightedPairs<DynDataTyped<T>, R>>> {
        Some(self.time_diffs_factory)
    }
}

pub type VecKeyBatchLayer<K, T, R, O> = Layer<K, Leaf<DynDataTyped<T>, R>, O>;

/// An immutable collection of update tuples, from a contiguous interval of
/// logical times.
#[derive(SizeOf)]
pub struct VecKeyBatch<K, T, R, O = usize>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    /// Where all the dataz is.
    pub layer: VecKeyBatchLayer<K, T, R, O>,
    pub lower: Antichain<T>,
    pub upper: Antichain<T>,
    #[size_of(skip)]
    factories: VecKeyBatchFactories<K, T, R>,
}

impl<K, T, R, O> Debug for VecKeyBatch<K, T, R, O>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    T: Timestamp,
    O: OrdOffset,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VecKeyBatch")
            .field("layer", &self.layer)
            .field("lower", &self.lower)
            .field("upper", &self.upper)
            .finish()
    }
}

impl<K, T, R, O: OrdOffset> Deserialize<VecKeyBatch<K, T, R, O>, Deserializer> for ()
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    fn deserialize(
        &self,
        _deserializer: &mut Deserializer,
    ) -> Result<VecKeyBatch<K, T, R, O>, <Deserializer as rkyv::Fallible>::Error> {
        todo!()
    }
}

impl<K, T, R, O> Archive for VecKeyBatch<K, T, R, O>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    type Archived = ();
    type Resolver = ();

    unsafe fn resolve(&self, _pos: usize, _resolver: Self::Resolver, _out: *mut Self::Archived) {
        todo!()
    }
}
impl<K, T, R, O: OrdOffset> Serialize<Serializer> for VecKeyBatch<K, T, R, O>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    fn serialize(
        &self,
        _serializer: &mut Serializer,
    ) -> Result<Self::Resolver, <Serializer as rkyv::Fallible>::Error> {
        todo!()
    }
}

impl<K, T, R, O> Clone for VecKeyBatch<K, T, R, O>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    fn clone(&self) -> Self {
        Self {
            layer: self.layer.clone(),
            lower: self.lower.clone(),
            upper: self.upper.clone(),
            factories: self.factories.clone(),
        }
    }
}

impl<K, T, R, O> Display for VecKeyBatch<K, T, R, O>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "layer:\n{}",
            textwrap::indent(&self.layer.to_string(), "    ")
        )
    }
}

impl<K, T, R, O> NumEntries for VecKeyBatch<K, T, R, O>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    const CONST_NUM_ENTRIES: Option<usize> = <VecKeyBatchLayer<K, T, R, O>>::CONST_NUM_ENTRIES;

    #[inline]
    fn num_entries_shallow(&self) -> usize {
        self.layer.num_entries_shallow()
    }

    #[inline]
    fn num_entries_deep(&self) -> usize {
        self.layer.num_entries_deep()
    }
}

impl<K, T, R, O> BatchReader for VecKeyBatch<K, T, R, O>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    type Key = K;
    type Val = DynUnit;
    type Time = T;
    type R = R;
    type Cursor<'s>
        = ValKeyCursor<'s, K, T, R, O>
    where
        O: 's;
    type Factories = VecKeyBatchFactories<K, T, R>;
    // type Consumer = VecKeyConsumer<K, T, R, O>;

    fn factories(&self) -> Self::Factories {
        self.factories.clone()
    }

    fn cursor(&self) -> Self::Cursor<'_> {
        ValKeyCursor {
            valid: true,
            cursor: self.layer.cursor(),
        }
    }

    /*fn consumer(self) -> Self::Consumer {
        todo!()
    }*/

    fn key_count(&self) -> usize {
        <VecKeyBatchLayer<K, T, R, O> as Trie>::keys(&self.layer)
    }

    fn len(&self) -> usize {
        <VecKeyBatchLayer<K, T, R, O> as Trie>::tuples(&self.layer)
    }

    fn approximate_byte_size(&self) -> usize {
        self.size_of().total_bytes()
    }

    fn lower(&self) -> AntichainRef<'_, T> {
        self.lower.as_ref()
    }

    fn upper(&self) -> AntichainRef<'_, T> {
        self.upper.as_ref()
    }

    fn sample_keys<RG>(&self, rng: &mut RG, sample_size: usize, sample: &mut DynVec<Self::Key>)
    where
        Self::Time: PartialEq<()>,
        RG: Rng,
    {
        self.layer.sample_keys(rng, sample_size, sample);
    }
}

impl<K, T, R, O> Batch for VecKeyBatch<K, T, R, O>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    type Batcher = MergeBatcher<Self>;
    type Builder = VecKeyBuilder<K, T, R, O>;
    type Merger = VecKeyMerger<K, T, R, O>;

    fn checkpoint_path(&self) -> Option<PathBuf> {
        unimplemented!()
    }

    /*fn from_keys(time: Self::Time, keys: Vec<(Self::Key, Self::R)>) -> Self {
        Self::from_tuples(time, keys)
    }*/

    fn begin_merge(&self, other: &Self, dst_hint: Option<BatchLocation>) -> Self::Merger {
        Self::Merger::new_merger(self, other, dst_hint)
    }
}

/// State for an in-progress merge.
#[derive(SizeOf)]
pub struct VecKeyMerger<K, T, R, O = usize>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    // first batch, and position therein.
    lower1: usize,
    upper1: usize,
    // second batch, and position therein.
    lower2: usize,
    upper2: usize,
    // result that we are currently assembling.
    result: <VecKeyBatchLayer<K, T, R, O> as Trie>::MergeBuilder,
    lower: Antichain<T>,
    upper: Antichain<T>,
    #[size_of(skip)]
    factories: VecKeyBatchFactories<K, T, R>,
}

impl<K, T, R, O> Merger<K, DynUnit, T, R, VecKeyBatch<K, T, R, O>> for VecKeyMerger<K, T, R, O>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    fn new_merger(
        batch1: &VecKeyBatch<K, T, R, O>,
        batch2: &VecKeyBatch<K, T, R, O>,
        _dst_hint: Option<BatchLocation>,
    ) -> Self {
        // Leonid: we do not require batch bounds to grow monotonically.
        //assert!(batch1.upper() == batch2.lower());

        VecKeyMerger {
            lower1: 0,
            upper1: batch1.layer.keys(),
            lower2: 0,
            upper2: batch2.layer.keys(),
            result: <<VecKeyBatchLayer<K, T, R, O> as Trie>::MergeBuilder as MergeBuilder>::with_capacity(&batch1.layer, &batch2.layer),
            lower: batch1.lower().meet(batch2.lower()),
            upper: batch1.upper().join(batch2.upper()),
            factories: batch1.factories.clone(),
        }
    }

    fn done(self) -> VecKeyBatch<K, T, R, O> {
        assert!(self.lower1 == self.upper1);
        assert!(self.lower2 == self.upper2);

        VecKeyBatch {
            layer: self.result.done(),
            lower: self.lower,
            upper: self.upper,
            factories: self.factories,
        }
    }

    fn work(
        &mut self,
        source1: &VecKeyBatch<K, T, R, O>,
        source2: &VecKeyBatch<K, T, R, O>,
        key_filter: &Option<Filter<K>>,
        _value_filter: &Option<Filter<DynUnit>>,
        frontier: &T,
        fuel: &mut isize,
    ) {
        let advance_func = |t: &mut DynDataTyped<T>| t.join_assign(frontier);

        let time_map_func = if frontier == &T::minimum() {
            None
        } else {
            Some(&advance_func as &dyn Fn(&mut DynDataTyped<T>))
        };

        if let Some(key_filter) = key_filter {
            self.result.push_merge_retain_keys_fueled(
                (&source1.layer, &mut self.lower1, self.upper1),
                (&source2.layer, &mut self.lower2, self.upper2),
                &key_filter.filter_func,
                time_map_func,
                fuel,
            );
        } else {
            self.result.push_merge_fueled(
                (&source1.layer, &mut self.lower1, self.upper1),
                (&source2.layer, &mut self.lower2, self.upper2),
                time_map_func,
                fuel,
            );
        }
    }
}

/// A cursor for navigating a single layer.
#[derive(Debug, SizeOf)]
pub struct ValKeyCursor<'s, K, T, R, O = usize>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    valid: bool,
    cursor: LayerCursor<'s, K, Leaf<DynDataTyped<T>, R>, O>,
}

impl<K, T, R, O> Clone for ValKeyCursor<'_, K, T, R, O>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    fn clone(&self) -> Self {
        Self {
            valid: self.valid,
            cursor: self.cursor.clone(),
        }
    }
}

impl<K, T, R, O> Cursor<K, DynUnit, T, R> for ValKeyCursor<'_, K, T, R, O>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    // fn key_factory(&self) -> &'static Factory<K> {
    //     self.cursor.storage.factories.key
    // }

    // fn val_factory(&self) -> &'static Factory<()> {
    //     todo!()
    // }

    fn weight_factory(&self) -> &'static dyn Factory<R> {
        self.cursor.child.storage.factories.diff
    }

    fn key(&self) -> &K {
        self.cursor.item()
    }

    fn val(&self) -> &DynUnit {
        &()
    }

    fn map_times(&mut self, logic: &mut dyn FnMut(&T, &R)) {
        self.cursor.child.rewind();
        while self.cursor.child.valid() {
            logic(
                self.cursor.child.current_key(),
                self.cursor.child.current_diff(),
            );
            self.cursor.child.step();
        }
    }

    fn map_times_through(&mut self, upper: &T, logic: &mut dyn FnMut(&T, &R)) {
        self.cursor.child.rewind();
        while self.cursor.child.valid() {
            if self.cursor.child.item().0.less_equal(upper) {
                logic(
                    self.cursor.child.current_key(),
                    self.cursor.child.current_diff(),
                );
            }
            self.cursor.child.step();
        }
    }

    fn weight(&mut self) -> &R
    where
        T: PartialEq<()>,
    {
        debug_assert!(&self.cursor.child.valid());
        self.cursor.child.current_diff()
    }

    fn map_values(&mut self, logic: &mut dyn FnMut(&DynUnit, &R))
    where
        T: PartialEq<()>,
    {
        if self.val_valid() {
            logic(self.val(), self.cursor.child.current_diff());
        }
    }

    fn key_valid(&self) -> bool {
        self.cursor.valid()
    }

    fn val_valid(&self) -> bool {
        self.valid
    }

    fn step_key(&mut self) {
        self.cursor.step();
        self.valid = true;
    }

    fn step_key_reverse(&mut self) {
        self.cursor.step_reverse();
        self.valid = true;
    }

    fn seek_key(&mut self, key: &K) {
        self.cursor.seek(key);
        self.valid = true;
    }

    fn seek_key_exact(&mut self, key: &K) -> bool {
        self.seek_key(key);
        self.key_valid() && self.key().eq(key)
    }

    fn seek_key_with(&mut self, predicate: &dyn Fn(&K) -> bool) {
        self.cursor.seek_with(predicate);
        self.valid = true;
    }

    fn seek_key_with_reverse(&mut self, predicate: &dyn Fn(&K) -> bool) {
        self.cursor.seek_with_reverse(predicate);
        self.valid = true;
    }

    fn seek_key_reverse(&mut self, key: &K) {
        self.cursor.seek_reverse(key);
        self.valid = true;
    }

    fn step_val(&mut self) {
        self.valid = false;
    }

    fn seek_val(&mut self, _val: &DynUnit) {}

    fn seek_val_with(&mut self, predicate: &dyn Fn(&DynUnit) -> bool) {
        if !predicate(&()) {
            self.valid = false;
        }
    }

    fn rewind_keys(&mut self) {
        self.cursor.rewind();
        self.valid = true;
    }

    fn fast_forward_keys(&mut self) {
        self.cursor.fast_forward();
        self.valid = true;
    }

    fn rewind_vals(&mut self) {
        self.valid = true;
    }

    fn step_val_reverse(&mut self) {
        self.valid = false;
    }

    fn seek_val_reverse(&mut self, _val: &DynUnit) {}

    fn seek_val_with_reverse(&mut self, predicate: &dyn Fn(&DynUnit) -> bool) {
        if !predicate(&()) {
            self.valid = false;
        }
    }

    fn fast_forward_vals(&mut self) {
        self.valid = true;
    }
}

/// A builder for creating layers from unsorted update tuples.
#[derive(SizeOf)]
pub struct VecKeyBuilder<K, T, R, O = usize>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    #[size_of(skip)]
    factories: VecKeyBatchFactories<K, T, R>,
    keys: Box<DynVec<K>>,
    offs: Vec<O>,
    times: Box<DynVec<DynDataTyped<T>>>,
    diffs: Box<DynVec<R>>,
}

impl<K, T, R, O> VecKeyBuilder<K, T, R, O>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    fn pushed_key(&mut self) {
        let off = O::from_usize(self.times.len());
        debug_assert!(off > *self.offs.last().unwrap());
        self.offs.push(off);
    }
}

impl<K, T, R, O> Builder<VecKeyBatch<K, T, R, O>> for VecKeyBuilder<K, T, R, O>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    fn with_capacity(factories: &VecKeyBatchFactories<K, T, R>, capacity: usize) -> Self {
        let mut keys = factories.layer_factories.keys.default_box();
        keys.reserve_exact(capacity);

        let mut offs = Vec::with_capacity(capacity + 1);
        offs.push(O::zero());

        let mut times = factories.layer_factories.child.keys.default_box();
        times.reserve_exact(capacity);

        let mut diffs = factories.layer_factories.child.diffs.default_box();
        diffs.reserve_exact(capacity);
        Self {
            factories: factories.clone(),
            keys,
            offs,
            times,
            diffs,
        }
    }

    fn reserve(&mut self, additional: usize) {
        self.keys.reserve(additional);
        self.offs.reserve(additional);
        self.times.reserve(additional);
        self.diffs.reserve(additional);
    }

    fn push_key(&mut self, key: &K) {
        self.keys.push_ref(key);
        self.pushed_key();
    }

    fn push_key_mut(&mut self, key: &mut K) {
        self.keys.push_val(key);
        self.pushed_key();
    }

    fn push_val(&mut self, _val: &DynUnit) {}

    fn push_time_diff(&mut self, time: &T, weight: &R) {
        self.times.push(time.clone());
        self.diffs.push_ref(weight);
    }

    fn push_time_diff_mut(&mut self, time: &mut T, weight: &mut R) {
        self.times.push(time.clone());
        self.diffs.push_val(weight);
    }

    fn done_with_bounds(
        self,
        (lower, upper): (Antichain<T>, Antichain<T>),
    ) -> VecKeyBatch<K, T, R, O> {
        VecKeyBatch {
            layer: Layer::from_parts(
                &self.factories.layer_factories,
                self.keys,
                self.offs,
                Leaf::from_parts(
                    &self.factories.layer_factories.child,
                    self.times,
                    self.diffs,
                ),
            ),
            factories: self.factories,
            lower,
            upper,
        }
    }
}

/*pub struct VecKeyConsumer<K, T, R, O>
where
    K: 'static,
    T: 'static,
    R: 'static,
    O: OrdOffset,
{
    consumer: OrderedLayerConsumer<K, T, R, O>,
}

impl<K, T, R, O> Consumer<K, (), R, T> for VecKeyConsumer<K, T, R, O>
where
    O: OrdOffset,
{
    type ValueConsumer<'a> = VecKeyValueConsumer<'a, K, T, R, O>
    where
        Self: 'a;

    fn key_valid(&self) -> bool {
        self.consumer.key_valid()
    }

    fn peek_key(&self) -> &K {
        self.consumer.peek_key()
    }

    fn next_key(&mut self) -> (K, Self::ValueConsumer<'_>) {
        let (key, values) = self.consumer.next_key();
        (key, VecKeyValueConsumer::new(values))
    }

    fn seek_key(&mut self, key: &K)
    where
        K: Ord,
    {
        self.consumer.seek_key(key);
    }
}

pub struct VecKeyValueConsumer<'a, K, T, R, O>
where
    T: 'static,
    R: 'static,
{
    consumer: OrderedLayerValues<'a, T, R>,
    __type: PhantomData<(K, O)>,
}

impl<'a, K, T, R, O> VecKeyValueConsumer<'a, K, T, R, O> {
    const fn new(consumer: OrderedLayerValues<'a, T, R>) -> Self {
        Self {
            consumer,
            __type: PhantomData,
        }
    }
}

impl<'a, K, T, R, O> ValueConsumer<'a, (), R, T> for VecKeyValueConsumer<'a, K, T, R, O> {
    fn value_valid(&self) -> bool {
        self.consumer.value_valid()
    }

    fn next_value(&mut self) -> ((), R, T) {
        let (time, diff, ()) = self.consumer.next_value();
        ((), diff, time)
    }

    fn remaining_values(&self) -> usize {
        self.consumer.remaining_values()
    }
}
*/
