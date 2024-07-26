use crate::{
    algebra::{AddAssignByRef, AddByRef, NegByRef, ZRingValue},
    dynamic::{
        DataTrait, DynPair, DynUnit, DynVec, DynWeightedPairs, Erase, Factory, LeanVec,
        WeightTrait, WeightTraitTyped, WithFactory,
    },
    time::{Antichain, AntichainRef},
    trace::{
        cursor::{HasTimeDiffCursor, SingletonTimeDiffCursor},
        layers::{
            Builder as _, Cursor as _, Leaf, LeafBuilder, LeafCursor, LeafFactories, MergeBuilder,
            Trie, TupleBuilder,
        },
        ord::merge_batcher::MergeBatcher,
        Batch, BatchFactories, BatchLocation, BatchReader, BatchReaderFactories, Builder, Cursor,
        Deserializer, Filter, Merger, Serializer, TimedBuilder, WeightedItem,
    },
    utils::Tup2,
    DBData, DBWeight, NumEntries,
};
use rand::Rng;
use rkyv::{Archive, Deserialize, Serialize};
use size_of::SizeOf;
use std::{
    cmp::max,
    fmt::{self, Debug, Display},
    ops::Neg,
};

pub struct VecWSetFactories<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> {
    layer_factories: LeafFactories<K, R>,
    item_factory: &'static dyn Factory<DynPair<K, DynUnit>>,
    weighted_item_factory: &'static dyn Factory<WeightedItem<K, DynUnit, R>>,
    weighted_items_factory: &'static dyn Factory<DynWeightedPairs<DynPair<K, DynUnit>, R>>,
    //pub batch_item_factory: &'static BatchItemFactory<K, (), K, R>,
}

impl<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> Clone for VecWSetFactories<K, R> {
    fn clone(&self) -> Self {
        Self {
            layer_factories: self.layer_factories.clone(),
            item_factory: self.item_factory,
            weighted_item_factory: self.weighted_item_factory,
            weighted_items_factory: self.weighted_items_factory,
        }
    }
}

impl<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> BatchReaderFactories<K, DynUnit, (), R>
    for VecWSetFactories<K, R>
{
    fn new<KType, VType, RType>() -> Self
    where
        KType: DBData + Erase<K>,
        VType: DBData + Erase<DynUnit>,
        RType: DBWeight + Erase<R>,
    {
        Self {
            layer_factories: LeafFactories::new::<KType, RType>(),
            item_factory: WithFactory::<Tup2<KType, ()>>::FACTORY,
            weighted_item_factory: WithFactory::<Tup2<Tup2<KType, ()>, RType>>::FACTORY,
            weighted_items_factory: WithFactory::<LeanVec<Tup2<Tup2<KType, ()>, RType>>>::FACTORY,
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
        self.layer_factories.diff
    }
}

impl<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> BatchFactories<K, DynUnit, (), R>
    for VecWSetFactories<K, R>
{
    //type BatchItemFactory = BatchItemFactory<K, (), K, R>;

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
}

/// An immutable collection of `(key, weight)` pairs without timing information.
#[derive(SizeOf)]
pub struct VecWSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    #[doc(hidden)]
    pub layer: Leaf<K, R>,
    #[size_of(skip)]
    factories: VecWSetFactories<K, R>,
    // #[size_of(skip)]
    // weighted_item_factory: &'static WeightedFactory<K, R>,
    // #[size_of(skip)]
    // batch_item_factory: &'static BatchItemFactory<K, (), K, R>,
}

impl<K, R> PartialEq for VecWSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn eq(&self, other: &Self) -> bool {
        self.layer == other.layer
    }
}

impl<K, R> Eq for VecWSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
}

impl<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> Debug for VecWSet<K, R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VecWSet")
            .field("layer", &self.layer)
            .finish()
    }
}

impl<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> Clone for VecWSet<K, R> {
    fn clone(&self) -> Self {
        Self {
            layer: self.layer.clone(),
            factories: self.factories.clone(),
            //weighted_item_factory: self.weighted_item_factory,
            //batch_item_factory: self.batch_item_factory,
        }
    }
}

impl<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> VecWSet<K, R> {
    #[inline]
    pub fn len(&self) -> usize {
        self.layer.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.layer.is_empty()
    }

    /*#[inline]
    pub fn retain(&mut self, retain: &dyn Fn(&K, &R) -> bool) {
        self.layer.retain(retain);
    }*/

    /*
    #[doc(hidden)]
    #[inline]
    pub fn from_columns(mut keys: ErasedVec<K>, mut diffs: ErasedVec<R>) -> Self {
        consolidate_payload_from(&mut keys, &mut diffs, 0);

        Self {
            // Safety: We've ensured that keys and diffs are the same length
            // and are sorted & consolidated
            layer: unsafe { Leaf::from_parts(keys, diffs, 0) },
        }
    }*/
}

impl<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> Deserialize<VecWSet<K, R>, Deserializer>
    for ()
{
    fn deserialize(
        &self,
        _deserializer: &mut Deserializer,
    ) -> Result<VecWSet<K, R>, <Deserializer as rkyv::Fallible>::Error> {
        todo!()
    }
}

impl<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> Archive for VecWSet<K, R> {
    type Archived = ();
    type Resolver = ();

    unsafe fn resolve(&self, _pos: usize, _resolver: Self::Resolver, _out: *mut Self::Archived) {
        todo!()
    }
}
impl<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> Serialize<Serializer> for VecWSet<K, R> {
    fn serialize(
        &self,
        _serializer: &mut Serializer,
    ) -> Result<Self::Resolver, <Serializer as rkyv::Fallible>::Error> {
        todo!()
    }
}

impl<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> Display for VecWSet<K, R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "layer:\n{}",
            textwrap::indent(&self.layer.to_string(), "    ")
        )
    }
}

/*impl<K, R, KR> From<Leaf<K, R, KR>> for VecZSet<K, R, KR> {
    fn from(layer: Leaf<K, R, KR>) -> Self {
        Self { layer }
    }
}

impl<K, R, KR> From<Leaf<K, R, KR>> for Rc<VecZSet<K, R, KR>> {
    fn from(layer: Leaf<K, R, KR>) -> Self {
        Rc::new(From::from(layer))
    }
}*/

impl<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> NumEntries for VecWSet<K, R> {
    const CONST_NUM_ENTRIES: Option<usize> = <Leaf<K, R>>::CONST_NUM_ENTRIES;

    fn num_entries_shallow(&self) -> usize {
        self.layer.num_entries_shallow()
    }

    fn num_entries_deep(&self) -> usize {
        self.layer.num_entries_deep()
    }
}

impl<K: DataTrait + ?Sized, R: WeightTraitTyped + ?Sized> NegByRef for VecWSet<K, R>
where
    R::Type: DBWeight + ZRingValue + Erase<R>,
{
    fn neg_by_ref(&self) -> Self {
        Self {
            layer: self.layer.neg_by_ref(),
            factories: self.factories.clone(),
            //weighted_item_factory: self.weighted_item_factory,
            //batch_item_factory: self.batch_item_factory,
        }
    }
}

impl<K: DataTrait + ?Sized, R: WeightTraitTyped + ?Sized> Neg for VecWSet<K, R>
where
    R::Type: DBWeight + ZRingValue,
{
    type Output = Self;

    fn neg(self) -> Self {
        Self {
            layer: self.layer.neg(),
            factories: self.factories,
            // weighted_item_factory: self.weighted_item_factory,
            // batch_item_factory: self.batch_item_factory,
        }
    }
}

impl<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> AddAssignByRef for VecWSet<K, R> {
    fn add_assign_by_ref(&mut self, rhs: &Self) {
        self.layer.add_assign_by_ref(&rhs.layer);
    }
}

impl<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> AddByRef for VecWSet<K, R> {
    fn add_by_ref(&self, rhs: &Self) -> Self {
        Self {
            layer: self.layer.add_by_ref(&rhs.layer),
            factories: self.factories.clone(),
            // weighted_item_factory: self.weighted_item_factory,
            // batch_item_factory: self.batch_item_factory,
        }
    }
}

impl<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> BatchReader for VecWSet<K, R> {
    type Key = K;
    type Val = DynUnit;
    type Time = ();
    type R = R;
    type Cursor<'s> = VecWSetCursor<'s, K, R>;
    type Factories = VecWSetFactories<K, R>;
    // type Consumer = VecZSetConsumer<K, R>;

    #[inline]
    fn factories(&self) -> Self::Factories {
        self.factories.clone()
    }

    #[inline]
    fn cursor(&self) -> Self::Cursor<'_> {
        VecWSetCursor {
            valid: true,
            cursor: self.layer.cursor(),
        }
    }

    /*
    #[inline]
    fn consumer(self) -> Self::Consumer {
        VecZSetConsumer {
            consumer: ColumnLayerConsumer::from(self.layer),
        }
    }*/

    #[inline]
    fn key_count(&self) -> usize {
        Trie::keys(&self.layer)
    }

    #[inline]
    fn len(&self) -> usize {
        self.layer.tuples()
    }

    #[inline]
    fn approximate_byte_size(&self) -> usize {
        self.size_of().total_bytes()
    }

    #[inline]
    fn lower(&self) -> AntichainRef<'_, ()> {
        AntichainRef::new(&[()])
    }

    #[inline]
    fn upper(&self) -> AntichainRef<'_, ()> {
        AntichainRef::empty()
    }

    fn truncate_keys_below(&mut self, lower_bound: &Self::Key) {
        self.layer.truncate_keys_below(lower_bound);
    }

    fn sample_keys<RG>(&self, rng: &mut RG, sample_size: usize, sample: &mut DynVec<Self::Key>)
    where
        RG: Rng,
    {
        self.layer.sample_keys(rng, sample_size, sample);
    }
}

impl<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> Batch for VecWSet<K, R> {
    type Batcher = MergeBatcher<Self>;
    type Builder = VecWSetBuilder<K, R>;
    type Merger = VecWSetMerger<K, R>;

    /*fn from_keys(time: Self::Time, keys: Vec<(Self::Key, Self::R)>) -> Self {
        Self::from_tuples(time, keys)
    }*/

    fn begin_merge(&self, other: &Self, dst_hint: Option<BatchLocation>) -> Self::Merger {
        VecWSetMerger::new_merger(self, other, dst_hint)
    }

    fn recede_to(&mut self, _frontier: &()) {}
}

/// State for an in-progress merge.
#[derive(SizeOf)]
pub struct VecWSetMerger<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> {
    // result that we are currently assembling.
    result: <Leaf<K, R> as Trie>::MergeBuilder,
    #[size_of(skip)]
    factories: VecWSetFactories<K, R>,
    // item_factory: &'static WeightedFactory<K, R>,
    // batch_item_factory: &'static BatchItemFactory<K, (), K, R>,
}

impl<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> Merger<K, DynUnit, (), R, VecWSet<K, R>>
    for VecWSetMerger<K, R>
where
    Self: SizeOf,
{
    fn new_merger(
        batch1: &VecWSet<K, R>,
        batch2: &VecWSet<K, R>,
        _dst_hint: Option<BatchLocation>,
    ) -> Self {
        Self {
            result: <<Leaf<K, R> as Trie>::MergeBuilder as MergeBuilder>::with_capacity(
                &batch1.layer,
                &batch2.layer,
            ),
            factories: batch1.factories.clone(),
            // item_factory: batch1.weighted_item_factory,
            // batch_item_factory: batch1.batch_item_factory,
        }
    }

    fn done(self) -> VecWSet<K, R> {
        VecWSet {
            layer: self.result.done(),
            factories: self.factories,
            // weighted_item_factory: self.item_factory,
            // batch_item_factory: self.batch_item_factory,
        }
    }

    fn work(
        &mut self,
        source1: &VecWSet<K, R>,
        source2: &VecWSet<K, R>,
        key_filter: &Option<Filter<K>>,
        _value_filter: &Option<Filter<DynUnit>>,
        fuel: &mut isize,
    ) {
        let initial_size = self.result.keys();

        if let Some(key_filter) = key_filter {
            self.result.push_merge_retain_keys(
                source1.layer.cursor(),
                source2.layer.cursor(),
                key_filter,
            );
        } else {
            self.result
                .push_merge(source1.layer.cursor(), source2.layer.cursor());
        }
        let effort = self.result.keys() - initial_size;
        *fuel -= effort as isize;

        *fuel = max(*fuel, 1);
    }
}

/// A cursor for navigating a single layer.
#[derive(Debug, SizeOf)]
pub struct VecWSetCursor<'s, K: DataTrait + ?Sized, R: WeightTrait + ?Sized> {
    valid: bool,
    pub(crate) cursor: LeafCursor<'s, K, R>,
}

impl<'s, K, R> Clone for VecWSetCursor<'s, K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            valid: self.valid,
            cursor: self.cursor.clone(),
        }
    }
}

impl<'s, K: DataTrait + ?Sized, R: WeightTrait + ?Sized> Cursor<K, DynUnit, (), R>
    for VecWSetCursor<'s, K, R>
{
    // fn key_factory(&self) -> &'static Factory<K> {
    //     self.cursor.storage.vtables.key
    // }

    // fn val_factory(&self) -> &'static Factory<()> {
    //     todo!()
    // }

    fn weight_factory(&self) -> &'static dyn Factory<R> {
        self.cursor.storage.factories.diff
    }

    fn key(&self) -> &K {
        self.cursor.current_key()
    }

    fn val(&self) -> &DynUnit {
        &()
    }

    fn map_times(&mut self, logic: &mut dyn FnMut(&(), &R)) {
        if self.cursor.valid() {
            logic(&(), self.cursor.current_diff())
        }
    }

    fn map_times_through(&mut self, _upper: &(), logic: &mut dyn FnMut(&(), &R)) {
        self.map_times(logic)
    }

    fn weight(&mut self) -> &R {
        debug_assert!(&self.cursor.valid());
        self.cursor.current_diff()
    }

    fn map_values(&mut self, logic: &mut dyn FnMut(&DynUnit, &R)) {
        if self.val_valid() {
            logic(&(), self.cursor.current_diff())
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

    fn seek_key_with(&mut self, predicate: &dyn Fn(&K) -> bool) {
        self.cursor.seek_key_with(predicate);
        self.valid = true;
    }

    fn seek_key_with_reverse(&mut self, predicate: &dyn Fn(&K) -> bool) {
        self.cursor.seek_key_with_reverse(predicate);
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

impl<'s, K, R> HasTimeDiffCursor<K, DynUnit, (), R> for VecWSetCursor<'s, K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    type TimeDiffCursor<'a> = SingletonTimeDiffCursor<'a, R>
    where
        Self: 'a;

    fn time_diff_cursor(&self) -> Self::TimeDiffCursor<'_> {
        SingletonTimeDiffCursor::new(self.val_valid().then(|| self.cursor.current_diff()))
    }
}

/// A builder for creating layers from unsorted update tuples.
#[derive(SizeOf)]
pub struct VecWSetBuilder<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> {
    builder: LeafBuilder<K, R>,
    #[size_of(skip)]
    factories: VecWSetFactories<K, R>,
    // item_factory: &'static WeightedFactory<K, R>,
    // batch_item_factory: &'static BatchItemFactory<K, (), K, R>,
}

impl<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> Builder<VecWSet<K, R>> for VecWSetBuilder<K, R>
where
    Self: SizeOf,
{
    #[inline]
    fn new_builder(factories: &VecWSetFactories<K, R>, _time: ()) -> Self {
        Self {
            builder: LeafBuilder::new(&factories.layer_factories),
            factories: factories.clone(),
            // item_factory: vtables.weighted_item_factory,
            // batch_item_factory: vtables.batch_item_factory,
        }
    }

    #[inline]
    fn with_capacity(factories: &VecWSetFactories<K, R>, _time: (), capacity: usize) -> Self {
        Self {
            builder: <LeafBuilder<K, R> as TupleBuilder>::with_capacity(
                &factories.layer_factories,
                capacity,
            ),
            factories: factories.clone(),
            // item_factory: vtables.weighted_item_factory,
            // batch_item_factory: vtables.batch_item_factory,
        }
    }

    #[inline]
    fn reserve(&mut self, additional: usize) {
        self.builder.reserve(additional);
    }

    #[inline]
    fn push(&mut self, element: &mut WeightedItem<K, DynUnit, R>) {
        let (kv, weight) = element.split_mut();
        let k = kv.fst_mut();
        self.builder.push_tuple((k, weight));
    }

    #[inline]
    fn push_refs(&mut self, key: &K, _val: &DynUnit, weight: &R) {
        self.builder.push_refs((key, weight))
    }

    #[inline]
    fn push_vals(&mut self, key: &mut K, _val: &mut DynUnit, weight: &mut R) {
        self.builder.push_tuple((key, weight))
    }

    #[inline(never)]
    fn done(self) -> VecWSet<K, R> {
        VecWSet {
            layer: self.builder.done(),
            factories: self.factories,
        }
    }
}

impl<K, R> TimedBuilder<VecWSet<K, R>> for VecWSetBuilder<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn push_time(&mut self, key: &K, val: &DynUnit, _time: &(), weight: &R) {
        self.push_refs(key, val, weight);
    }

    fn done_with_bounds(self, _lower: Antichain<()>, _upper: Antichain<()>) -> VecWSet<K, R> {
        self.done()
    }
}

/*
#[derive(Debug, SizeOf)]
pub struct VecZSetConsumer<K, R>
where
    K: 'static,
    R: 'static,
{
    consumer: ColumnLayerConsumer<K, R>,
}

impl<K, R> Consumer<K, (), R, ()> for VecZSetConsumer<K, R> {
    type ValueConsumer<'a> = VecZSetValueConsumer<'a, K, R>
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
        (key, VecZSetValueConsumer { values })
    }

    fn seek_key(&mut self, key: &K) {
        self.consumer.seek_key(key);
    }
}

#[derive(Debug)]
pub struct VecZSetValueConsumer<'a, K, R>
where
    K: 'static,
    R: 'static,
{
    values: ColumnLayerValues<'a, K, R>,
}

impl<'a, K, R> ValueConsumer<'a, (), R, ()> for VecZSetValueConsumer<'a, K, R> {
    fn value_valid(&self) -> bool {
        self.values.value_valid()
    }

    fn next_value(&mut self) -> ((), R, ()) {
        self.values.next_value()
    }

    fn remaining_values(&self) -> usize {
        self.values.remaining_values()
    }
}
*/
