use crate::{
    algebra::{AddAssignByRef, AddByRef, NegByRef},
    dynamic::{
        DataTrait, DynPair, DynUnit, DynVec, DynWeightedPairs, Erase, Factory, LeanVec,
        WeightTrait, WeightTraitTyped, WithFactory,
    },
    time::AntichainRef,
    trace::{
        layers::{
            Builder as TrieBuilder, Cursor as TrieCursor, FileColumnLayer, FileColumnLayerBuilder,
            FileColumnLayerCursor, FileLeafFactories, MergeBuilder, Trie, TupleBuilder,
        },
        ord::merge_batcher::MergeBatcher,
        Batch, BatchFactories, BatchReader, BatchReaderFactories, Builder, Cursor, Deserializer,
        Filter, Merger, Serializer, WeightedItem,
    },
    utils::Tup2,
    DBData, DBWeight, NumEntries,
};
use rand::Rng;
use rkyv::{Archive, Deserialize, Serialize};
use size_of::SizeOf;
use std::path::PathBuf;
use std::{
    cmp::max,
    fmt::{self, Debug, Display},
    ops::{Add, AddAssign, Neg},
};

pub struct FileZSetFactories<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    layer_factories: FileLeafFactories<K, R>,
    keys_factory: &'static dyn Factory<DynVec<K>>,
    item_factory: &'static dyn Factory<DynPair<K, DynUnit>>,
    weighted_item_factory: &'static dyn Factory<WeightedItem<K, DynUnit, R>>,
    weighted_items_factory: &'static dyn Factory<DynWeightedPairs<DynPair<K, DynUnit>, R>>,
}

impl<K, R> Clone for FileZSetFactories<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            layer_factories: self.layer_factories.clone(),
            keys_factory: self.keys_factory,
            item_factory: self.item_factory,
            weighted_item_factory: self.weighted_item_factory,
            weighted_items_factory: self.weighted_items_factory,
        }
    }
}

impl<K, R> BatchReaderFactories<K, DynUnit, (), R> for FileZSetFactories<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn new<KType, VType, RType>() -> Self
    where
        KType: DBData + Erase<K>,
        VType: DBData + Erase<DynUnit>,
        RType: DBWeight + Erase<R>,
    {
        Self {
            layer_factories: FileLeafFactories::new::<KType, RType>(),
            keys_factory: WithFactory::<LeanVec<KType>>::FACTORY,
            item_factory: WithFactory::<Tup2<KType, ()>>::FACTORY,
            weighted_item_factory: WithFactory::<Tup2<Tup2<KType, ()>, RType>>::FACTORY,
            weighted_items_factory: WithFactory::<LeanVec<Tup2<Tup2<KType, ()>, RType>>>::FACTORY,
        }
    }

    fn key_factory(&self) -> &'static dyn Factory<K> {
        self.layer_factories.file_factories.key_factory
    }

    fn keys_factory(&self) -> &'static dyn Factory<DynVec<K>> {
        self.keys_factory
    }

    fn val_factory(&self) -> &'static dyn Factory<DynUnit> {
        WithFactory::<()>::FACTORY
    }

    fn weight_factory(&self) -> &'static dyn Factory<R> {
        self.layer_factories.diff_factory
    }
}

impl<K, R> BatchFactories<K, DynUnit, (), R> for FileZSetFactories<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
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

/// A batch of weighted tuples without values or times.
///
/// Each tuple in `FileZSet<K, R>` has key type `K`, value type `()`, weight
/// type `R`, and time type `()`.
#[derive(SizeOf)]
pub struct FileZSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    #[size_of(skip)]
    factories: FileZSetFactories<K, R>,
    #[doc(hidden)]
    pub layer: FileColumnLayer<K, R>,
}

impl<K, R> Debug for FileZSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileZSet")
            .field("layer", &self.layer)
            .finish()
    }
}

impl<K, R> Clone for FileZSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            factories: self.factories.clone(),
            layer: self.layer.clone(),
        }
    }
}

impl<K, R> FileZSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    #[inline]
    pub fn len(&self) -> usize {
        self.layer.len() as usize
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.layer.is_empty()
    }

    /*
    #[inline]
    pub fn retain<F>(&mut self, retain: F)
    where
        F: Fn(&K, &R) -> bool,
    {
        let mut cursor = self.cursor();
        let mut builder = FileColumnLayerBuilder::new(&self.layer.factories);
        while cursor.key_valid() {
            while cursor.val_valid() {
                let weight = cursor.weight();
                let key = cursor.key();
                if retain(key, &weight) {
                    builder.push_refs((key, weight));
                }
                cursor.step_val();
            }
            cursor.step_key();
        }
    }*/
}

// This is `#[cfg(test)]` only because it would be surprisingly expensive in
// production.
impl<Other, K, R> PartialEq<Other> for FileZSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    Other: BatchReader<Key = K, Val = DynUnit, R = R, Time = ()>,
{
    fn eq(&self, other: &Other) -> bool {
        let mut c1 = self.cursor();
        let mut c2 = other.cursor();
        while c1.key_valid() && c2.key_valid() {
            if c1.key() != c2.key() || c1.weight() != c2.weight() {
                return false;
            }
            c1.step_key();
            c2.step_key();
        }
        !c1.key_valid() && !c2.key_valid()
    }
}

impl<K, R> Eq for FileZSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
}

impl<K, R> Display for FileZSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "layer:\n{}",
            textwrap::indent(&self.layer.to_string(), "    ")
        )
    }
}

impl<K, R> NumEntries for FileZSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    const CONST_NUM_ENTRIES: Option<usize> = <FileColumnLayer<K, R>>::CONST_NUM_ENTRIES;

    fn num_entries_shallow(&self) -> usize {
        self.layer.num_entries_shallow()
    }

    fn num_entries_deep(&self) -> usize {
        self.layer.num_entries_deep()
    }
}

impl<K, R> NegByRef for FileZSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTraitTyped + ?Sized,
    R::Type: DBWeight + NegByRef + Erase<R>,
{
    fn neg_by_ref(&self) -> Self {
        Self {
            factories: self.factories.clone(),
            layer: self.layer.neg_by_ref(),
        }
    }
}

impl<K, R> Neg for FileZSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTraitTyped + ?Sized,
    R::Type: DBWeight + NegByRef + Erase<R>,
{
    type Output = Self;

    fn neg(self) -> Self {
        Self {
            factories: self.factories.clone(),
            layer: self.layer.neg(),
        }
    }
}

// TODO: by-value merge
impl<K, R> Add<Self> for FileZSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self {
            factories: self.factories.clone(),
            layer: self.layer.add(rhs.layer),
        }
    }
}

impl<K, R> AddAssign<Self> for FileZSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn add_assign(&mut self, rhs: Self) {
        self.layer.add_assign(rhs.layer);
    }
}

impl<K, R> AddAssignByRef for FileZSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn add_assign_by_ref(&mut self, rhs: &Self) {
        self.layer.add_assign_by_ref(&rhs.layer);
    }
}

impl<K, R> AddByRef for FileZSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn add_by_ref(&self, rhs: &Self) -> Self {
        Self {
            factories: self.factories.clone(),
            layer: self.layer.add_by_ref(&rhs.layer),
        }
    }
}

impl<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> Deserialize<FileZSet<K, R>, Deserializer>
    for ()
{
    fn deserialize(
        &self,
        _deserializer: &mut Deserializer,
    ) -> Result<FileZSet<K, R>, <Deserializer as rkyv::Fallible>::Error> {
        todo!()
    }
}

impl<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> Archive for FileZSet<K, R> {
    type Archived = ();
    type Resolver = ();

    unsafe fn resolve(&self, _pos: usize, _resolver: Self::Resolver, _out: *mut Self::Archived) {
        todo!()
    }
}
impl<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> Serialize<Serializer> for FileZSet<K, R> {
    fn serialize(
        &self,
        _serializer: &mut Serializer,
    ) -> Result<Self::Resolver, <Serializer as rkyv::Fallible>::Error> {
        todo!()
    }
}

impl<K, R> BatchReader for FileZSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    type Factories = FileZSetFactories<K, R>;
    type Key = K;
    type Val = DynUnit;
    type Time = ();
    type R = R;
    type Cursor<'s> = FileZSetCursor<'s, K, R>;

    fn factories(&self) -> Self::Factories {
        self.factories.clone()
    }

    #[inline]
    fn cursor(&self) -> Self::Cursor<'_> {
        FileZSetCursor {
            valid: true,
            cursor: self.layer.cursor(),
        }
    }

    #[inline]
    fn key_count(&self) -> usize {
        Trie::keys(&self.layer)
    }

    #[inline]
    fn len(&self) -> usize {
        self.layer.tuples()
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

impl<K, R> Batch for FileZSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    type Batcher = MergeBatcher<Self>;
    type Builder = FileZSetBuilder<K, R>;
    type Merger = FileZSetMerger<K, R>;

    fn begin_merge(&self, other: &Self) -> Self::Merger {
        FileZSetMerger::new_merger(self, other)
    }

    fn recede_to(&mut self, _frontier: &()) {}

    fn dyn_empty(factories: &Self::Factories, _time: Self::Time) -> Self {
        Self {
            factories: factories.clone(),
            layer: FileColumnLayer::empty(&factories.layer_factories),
        }
    }

    fn persistent_id(&self) -> Option<PathBuf> {
        Some(self.layer.path())
    }
}

/// State for an in-progress merge.
#[derive(SizeOf)]
pub struct FileZSetMerger<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    #[size_of(skip)]
    factories: FileZSetFactories<K, R>,
    // result that we are currently assembling.
    result: <FileColumnLayer<K, R> as Trie>::MergeBuilder,
}

impl<K, R> Merger<K, DynUnit, (), R, FileZSet<K, R>> for FileZSetMerger<K, R>
where
    Self: SizeOf,
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn new_merger(batch1: &FileZSet<K, R>, batch2: &FileZSet<K, R>) -> Self {
        Self {
            factories: batch1.factories().clone(),
            result: <<FileColumnLayer<K, R> as Trie>::MergeBuilder as MergeBuilder>::with_capacity(
                &batch1.layer,
                &batch2.layer,
            ),
        }
    }

    fn done(self) -> FileZSet<K, R> {
        FileZSet {
            factories: self.factories,
            layer: self.result.done(),
        }
    }

    fn work(
        &mut self,
        source1: &FileZSet<K, R>,
        source2: &FileZSet<K, R>,
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
pub struct FileZSetCursor<'s, K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    valid: bool,
    cursor: FileColumnLayerCursor<'s, K, R>,
}

impl<'s, K, R> Clone for FileZSetCursor<'s, K, R>
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

impl<'s, K, R> Cursor<K, DynUnit, (), R> for FileZSetCursor<'s, K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn key(&self) -> &K {
        self.cursor.current_key()
    }

    fn val(&self) -> &DynUnit {
        &()
    }

    fn map_times(&mut self, logic: &mut dyn FnMut(&(), &R)) {
        if self.cursor.valid() {
            logic(&(), self.cursor.current_diff());
        }
    }

    fn map_times_through(&mut self, _upper: &(), logic: &mut dyn FnMut(&(), &R)) {
        self.map_times(logic)
    }

    fn weight(&mut self) -> &R {
        debug_assert!(&self.cursor.valid());
        self.cursor.current_diff()
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

    fn weight_factory(&self) -> &'static dyn Factory<R> {
        self.cursor.storage.factories.diff_factory
    }

    fn map_values(&mut self, logic: &mut dyn FnMut(&DynUnit, &R)) {
        if self.val_valid() {
            logic(&(), self.cursor.current_diff())
        }
    }
}

/// A builder for creating layers from unsorted update tuples.
#[derive(SizeOf)]
pub struct FileZSetBuilder<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    #[size_of(skip)]
    factories: FileZSetFactories<K, R>,
    builder: FileColumnLayerBuilder<K, R>,
}

impl<K, R> Builder<FileZSet<K, R>> for FileZSetBuilder<K, R>
where
    Self: SizeOf,
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    #[inline]
    fn new_builder(factories: &<FileZSet<K, R> as BatchReader>::Factories, _time: ()) -> Self {
        Self {
            factories: factories.clone(),
            builder: FileColumnLayerBuilder::new(&factories.layer_factories),
        }
    }

    #[inline]
    fn with_capacity(
        factories: &<FileZSet<K, R> as BatchReader>::Factories,
        _time: (),
        capacity: usize,
    ) -> Self {
        Self {
            factories: factories.clone(),
            builder: <FileColumnLayerBuilder<K, R> as TupleBuilder>::with_capacity(
                &factories.layer_factories,
                capacity,
            ),
        }
    }

    #[inline]
    fn reserve(&mut self, additional: usize) {
        self.builder.reserve(additional);
    }

    #[inline]
    fn push(&mut self, item: &mut WeightedItem<K, DynUnit, R>) {
        let (kv, weight) = item.split();
        let k = kv.fst();
        self.builder.push_refs((k, weight));
    }

    #[inline(never)]
    fn done(self) -> FileZSet<K, R> {
        FileZSet {
            factories: self.factories,
            layer: self.builder.done(),
        }
    }

    fn push_refs(&mut self, key: &K, _val: &DynUnit, weight: &R) {
        self.builder.push_refs((key, weight))
    }

    fn push_vals(&mut self, key: &mut K, _val: &mut DynUnit, weight: &mut R) {
        self.builder.push_tuple((key, weight))
    }
}

/*
#[derive(Debug, SizeOf)]
pub struct FileZSetConsumer<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    consumer: FileColumnLayerConsumer<K, R>,
}

impl<K, R> Consumer<K, (), R, ()> for FileZSetConsumer<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    type ValueConsumer<'a> = FileZSetValueConsumer<R>
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
        (key, FileZSetValueConsumer { values })
    }

    fn seek_key(&mut self, key: &K)
    where
        K: Ord,
    {
        self.consumer.seek_key(key);
    }
}

#[derive(Debug)]
pub struct FileZSetValueConsumer<R>
where
    R: WeightTrait + ?Sized,
{
    values: FileColumnLayerValues<R>,
}

impl<'a, R> ValueConsumer<'a, (), R, ()> for FileZSetValueConsumer<R>
where
    R: WeightTrait + ?Sized,
{
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
