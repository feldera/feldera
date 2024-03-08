use crate::{
    algebra::{AddAssignByRef, AddByRef, NegByRef},
    dynamic::{
        DataTrait, DynPair, DynVec, DynWeightedPairs, Erase, Factory, LeanVec, WeightTrait,
        WeightTraitTyped, WithFactory,
    },
    time::AntichainRef,
    trace::{
        layers::{
            Builder as TrieBuilder, Cursor as TrieCursor, FileOrderedCursor, FileOrderedLayer,
            FileOrderedLayerFactories, FileOrderedTupleBuilder, FileOrderedValueCursor,
            MergeBuilder, Trie, TupleBuilder,
        },
        ord::merge_batcher::MergeBatcher,
        Batch, BatchFactories, BatchReader, BatchReaderFactories, Builder, Cursor, Filter, Merger,
        WeightedItem,
    },
    utils::Tup2,
    DBData, DBWeight, NumEntries,
};
use rand::Rng;
use rkyv::{ser::Serializer, Archive, Archived, Deserialize, Fallible, Serialize};
use size_of::SizeOf;
use std::path::PathBuf;
use std::{
    fmt::{self, Debug, Display},
    ops::{Add, AddAssign, Neg},
};

pub struct FileIndexedZSetFactories<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    layer_factories: FileOrderedLayerFactories<K, V, R>,
    keys_factory: &'static dyn Factory<DynVec<K>>,
    item_factory: &'static dyn Factory<DynPair<K, V>>,
    weighted_item_factory: &'static dyn Factory<WeightedItem<K, V, R>>,
    weighted_items_factory: &'static dyn Factory<DynWeightedPairs<DynPair<K, V>, R>>,
}

impl<K, V, R> Clone for FileIndexedZSetFactories<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
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

impl<K, V, R> BatchReaderFactories<K, V, (), R> for FileIndexedZSetFactories<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn new<KType, VType, RType>() -> Self
    where
        KType: DBData + Erase<K>,
        VType: DBData + Erase<V>,
        RType: DBWeight + Erase<R>,
    {
        Self {
            layer_factories: FileOrderedLayerFactories::new::<KType, VType, RType>(),
            keys_factory: WithFactory::<LeanVec<KType>>::FACTORY,
            item_factory: WithFactory::<Tup2<KType, VType>>::FACTORY,
            weighted_item_factory: WithFactory::<Tup2<Tup2<KType, VType>, RType>>::FACTORY,
            weighted_items_factory:
                WithFactory::<LeanVec<Tup2<Tup2<KType, VType>, RType>>>::FACTORY,
        }
    }

    fn key_factory(&self) -> &'static dyn Factory<K> {
        self.layer_factories.factories0.key_factory
    }

    fn keys_factory(&self) -> &'static dyn Factory<DynVec<K>> {
        self.keys_factory
    }

    fn val_factory(&self) -> &'static dyn Factory<V> {
        self.layer_factories.factories1.key_factory
    }

    fn weight_factory(&self) -> &'static dyn Factory<R> {
        self.layer_factories.diff_factory
    }
}

impl<K, V, R> BatchFactories<K, V, (), R> for FileIndexedZSetFactories<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn item_factory(&self) -> &'static dyn Factory<DynPair<K, V>> {
        self.item_factory
    }

    fn weighted_item_factory(&self) -> &'static dyn Factory<WeightedItem<K, V, R>> {
        self.weighted_item_factory
    }

    fn weighted_items_factory(&self) -> &'static dyn Factory<DynWeightedPairs<DynPair<K, V>, R>> {
        self.weighted_items_factory
    }
}

/// A batch of key-value weighted tuples without timing information.
///
/// Each tuple in `FileIndexedZSet<K, V, R>` has key type `K`, value type `V`,
/// weight type `R`, and time `()`.
pub struct FileIndexedZSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    factories: FileIndexedZSetFactories<K, V, R>,
    #[doc(hidden)]
    pub layer: FileOrderedLayer<K, V, R>,
}

impl<K, V, R> Debug for FileIndexedZSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileIndexedZSet")
            .field("layer", &self.layer)
            .finish()
    }
}

impl<K, V, R> Clone for FileIndexedZSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            factories: self.factories.clone(),
            layer: self.layer.clone(),
        }
    }
}

// This is `#[cfg(test)]` only because it would be surprisingly expensive in
// production.
#[cfg(test)]
impl<Other, K, V, R> PartialEq<Other> for FileIndexedZSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    Other: BatchReader<Key = K, Val = V, R = R, Time = ()>,
{
    fn eq(&self, other: &Other) -> bool {
        let mut c1 = self.cursor();
        let mut c2 = other.cursor();
        while c1.key_valid() && c2.key_valid() {
            if c1.key() != c2.key() {
                return false;
            }
            while c1.val_valid() && c2.val_valid() {
                if c1.val() != c2.val() || c1.weight() != c2.weight() {
                    return false;
                }
                c1.step_val();
                c2.step_val();
            }
            if c1.val_valid() || c2.val_valid() {
                return false;
            }
            c1.step_key();
            c2.step_key();
        }
        !c1.key_valid() && !c2.key_valid()
    }
}

#[cfg(test)]
impl<K, V, R> Eq for FileIndexedZSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
}

impl<K, V, R> Display for FileIndexedZSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    FileOrderedLayer<K, V, R>: Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "layer:\n{}",
            textwrap::indent(&self.layer.to_string(), "    ")
        )
    }
}

impl<K, V, R> NumEntries for FileIndexedZSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    const CONST_NUM_ENTRIES: Option<usize> = FileOrderedLayer::<K, V, R>::CONST_NUM_ENTRIES;

    #[inline]
    fn num_entries_shallow(&self) -> usize {
        self.layer.num_entries_shallow()
    }

    #[inline]
    fn num_entries_deep(&self) -> usize {
        self.layer.num_entries_deep()
    }
}

impl<K, V, R> NegByRef for FileIndexedZSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTraitTyped + ?Sized,
    R::Type: DBWeight + NegByRef + Erase<R>,
{
    #[inline]
    fn neg_by_ref(&self) -> Self {
        Self {
            factories: self.factories.clone(),
            layer: self.layer.neg_by_ref(),
        }
    }
}

impl<K, V, R> Neg for FileIndexedZSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTraitTyped + ?Sized,
    R::Type: DBWeight + NegByRef + Erase<R>,
{
    type Output = Self;

    #[inline]
    fn neg(self) -> Self {
        Self {
            factories: self.factories.clone(),
            layer: self.layer.neg(),
        }
    }
}

impl<K, V, R> Add<Self> for FileIndexedZSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    type Output = Self;
    #[inline]

    fn add(self, rhs: Self) -> Self::Output {
        Self {
            factories: self.factories.clone(),
            layer: self.layer.add(rhs.layer),
        }
    }
}

impl<K, V, R> AddAssign<Self> for FileIndexedZSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    #[inline]
    fn add_assign(&mut self, rhs: Self) {
        self.layer.add_assign(rhs.layer);
    }
}

impl<K, V, R> AddAssignByRef for FileIndexedZSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    #[inline]
    fn add_assign_by_ref(&mut self, rhs: &Self) {
        self.layer.add_assign_by_ref(&rhs.layer);
    }
}

impl<K, V, R> AddByRef for FileIndexedZSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    #[inline]
    fn add_by_ref(&self, rhs: &Self) -> Self {
        Self {
            factories: self.factories.clone(),
            layer: self.layer.add_by_ref(&rhs.layer),
        }
    }
}

impl<K, V, R> BatchReader for FileIndexedZSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    type Factories = FileIndexedZSetFactories<K, V, R>;
    type Key = K;
    type Val = V;
    type Time = ();
    type R = R;
    type Cursor<'s> = FileIndexedZSetCursor<'s, K, V, R>
    where
        V: 's;

    fn factories(&self) -> Self::Factories {
        self.factories.clone()
    }

    #[inline]
    fn cursor(&self) -> Self::Cursor<'_> {
        FileIndexedZSetCursor::new(self)
    }

    #[inline]
    fn key_count(&self) -> usize {
        self.layer.keys()
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

impl<K, V, R> Batch for FileIndexedZSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    type Batcher = MergeBatcher<Self>;
    type Builder = FileIndexedZSetBuilder<K, V, R>;
    type Merger = FileIndexedZSetMerger<K, V, R>;

    fn begin_merge(&self, other: &Self) -> Self::Merger {
        FileIndexedZSetMerger::new_merger(self, other)
    }

    fn recede_to(&mut self, _frontier: &()) {}

    fn dyn_empty(factories: &Self::Factories, _time: Self::Time) -> Self {
        Self {
            factories: factories.clone(),
            layer: FileOrderedLayer::empty(&factories.layer_factories),
        }
    }
    fn persistent_id(&self) -> Option<PathBuf> {
        Some(self.layer.path())
    }
}

/// State for an in-progress merge.
pub struct FileIndexedZSetMerger<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    factories: FileIndexedZSetFactories<K, V, R>,
    result: Option<FileOrderedLayer<K, V, R>>,
}

impl<K, V, R> Merger<K, V, (), R, FileIndexedZSet<K, V, R>> for FileIndexedZSetMerger<K, V, R>
where
    Self: SizeOf,
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    #[inline]
    fn new_merger(batch1: &FileIndexedZSet<K, V, R>, _batch2: &FileIndexedZSet<K, V, R>) -> Self {
        Self {
            factories: batch1.factories.clone(),
            result: None,
        }
    }

    #[inline]
    fn done(mut self) -> FileIndexedZSet<K, V, R> {
        FileIndexedZSet {
            factories: self.factories.clone(),
            layer: self
                .result
                .take()
                .unwrap_or_else(|| FileOrderedLayer::empty(&self.factories.layer_factories)),
        }
    }

    fn work(
        &mut self,
        source1: &FileIndexedZSet<K, V, R>,
        source2: &FileIndexedZSet<K, V, R>,
        key_filter: &Option<Filter<K>>,
        value_filter: &Option<Filter<V>>,
        fuel: &mut isize,
    ) {
        debug_assert!(*fuel > 0);
        if self.result.is_none() {
            let mut builder =
                <<FileOrderedLayer<K, V, R> as Trie>::MergeBuilder as MergeBuilder>::with_capacity(
                    &source1.layer,
                    &source2.layer,
                );
            let cursor1 = source1.layer.cursor();
            let cursor2 = source2.layer.cursor();
            match (key_filter, value_filter) {
                (None, None) => builder.push_merge(cursor1, cursor2),
                (Some(key_filter), None) => {
                    builder.push_merge_retain_keys(cursor1, cursor2, key_filter)
                }
                (Some(key_filter), Some(value_filter)) => {
                    builder.push_merge_retain_values(cursor1, cursor2, key_filter, value_filter)
                }
                (None, Some(value_filter)) => {
                    builder.push_merge_retain_values(cursor1, cursor2, &|_| true, value_filter)
                }
            };
            self.result = Some(builder.done());
        }
    }
}

impl<K, V, R> SizeOf for FileIndexedZSetMerger<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn size_of_children(&self, _context: &mut size_of::Context) {
        // XXX
    }
}

/// A cursor for navigating a single layer.
#[derive(Debug, SizeOf)]
pub struct FileIndexedZSetCursor<'s, K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    key_cursor: FileOrderedCursor<'s, K, V, R>,
    val_cursor: FileOrderedValueCursor<'s, K, V, R>,
}

impl<'s, K, V, R> Clone for FileIndexedZSetCursor<'s, K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            key_cursor: self.key_cursor.clone(),
            val_cursor: self.val_cursor.clone(),
        }
    }
}

impl<'s, K, V, R> FileIndexedZSetCursor<'s, K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn new(zset: &'s FileIndexedZSet<K, V, R>) -> Self {
        let key_cursor = zset.layer.cursor();
        let val_cursor = key_cursor.values();
        Self {
            key_cursor,
            val_cursor,
        }
    }

    fn move_key<F>(&mut self, op: F)
    where
        F: Fn(&mut FileOrderedCursor<'s, K, V, R>),
    {
        op(&mut self.key_cursor);
        self.val_cursor = self.key_cursor.values();
    }
}

impl<'s, K, V, R> Cursor<K, V, (), R> for FileIndexedZSetCursor<'s, K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn weight_factory(&self) -> &'static dyn Factory<R> {
        self.key_cursor.storage.factories.diff_factory
    }

    fn key(&self) -> &K {
        self.key_cursor.current_key()
    }

    fn val(&self) -> &V {
        self.val_cursor.current_value()
    }

    fn map_times(&mut self, logic: &mut dyn FnMut(&(), &R)) {
        if self.val_cursor.valid() {
            logic(&(), self.val_cursor.current_diff())
        }
    }

    fn map_times_through(&mut self, _upper: &(), logic: &mut dyn FnMut(&(), &R)) {
        self.map_times(logic)
    }

    fn map_values(&mut self, logic: &mut dyn FnMut(&V, &R)) {
        while self.val_valid() {
            logic(self.val(), self.val_cursor.current_diff());
            self.step_val();
        }
    }

    fn weight(&mut self) -> &R {
        debug_assert!(self.val_cursor.valid());
        self.val_cursor.current_diff()
    }

    fn key_valid(&self) -> bool {
        self.key_cursor.valid()
    }

    fn val_valid(&self) -> bool {
        self.val_cursor.valid()
    }

    fn step_key(&mut self) {
        self.move_key(|key_cursor| key_cursor.step());
    }

    fn step_key_reverse(&mut self) {
        self.move_key(|key_cursor| key_cursor.step_reverse());
    }

    fn seek_key(&mut self, key: &K) {
        self.move_key(|key_cursor| key_cursor.seek(key));
    }

    fn seek_key_with(&mut self, predicate: &dyn Fn(&K) -> bool) {
        self.move_key(|key_cursor| key_cursor.seek_with(&predicate));
    }

    fn seek_key_with_reverse(&mut self, predicate: &dyn Fn(&K) -> bool) {
        self.move_key(|key_cursor| key_cursor.seek_with_reverse(&predicate));
    }

    fn seek_key_reverse(&mut self, key: &K) {
        self.move_key(|key_cursor| key_cursor.seek_reverse(key));
    }

    fn step_val(&mut self) {
        self.val_cursor.step();
    }

    fn seek_val(&mut self, val: &V) {
        self.val_cursor.seek(val);
    }

    fn seek_val_with(&mut self, predicate: &dyn Fn(&V) -> bool) {
        self.val_cursor.seek_val_with(predicate);
    }

    fn rewind_keys(&mut self) {
        self.move_key(|key_cursor| key_cursor.rewind());
    }

    fn fast_forward_keys(&mut self) {
        self.move_key(|key_cursor| key_cursor.fast_forward());
    }

    fn rewind_vals(&mut self) {
        self.val_cursor.rewind();
    }

    fn step_val_reverse(&mut self) {
        self.val_cursor.step_reverse();
    }

    fn seek_val_reverse(&mut self, val: &V) {
        self.val_cursor.seek_reverse(val);
    }

    fn seek_val_with_reverse(&mut self, predicate: &dyn Fn(&V) -> bool) {
        self.val_cursor.seek_val_with_reverse(predicate);
    }

    fn fast_forward_vals(&mut self) {
        self.val_cursor.fast_forward();
    }
}

/// A builder for creating layers from unsorted update tuples.
pub struct FileIndexedZSetBuilder<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    factories: FileIndexedZSetFactories<K, V, R>,
    builder: FileOrderedTupleBuilder<K, V, R>,
}

impl<K, V, R> Builder<FileIndexedZSet<K, V, R>> for FileIndexedZSetBuilder<K, V, R>
where
    Self: SizeOf,
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    #[inline]
    fn new_builder(factories: &FileIndexedZSetFactories<K, V, R>, _time: ()) -> Self {
        Self {
            factories: factories.clone(),
            builder: FileOrderedTupleBuilder::<K, V, R>::new(&factories.layer_factories),
        }
    }

    #[inline]
    fn with_capacity(
        factories: &FileIndexedZSetFactories<K, V, R>,
        _time: (),
        capacity: usize,
    ) -> Self {
        Self {
            factories: factories.clone(),
            builder: <FileOrderedTupleBuilder<K, V, R> as TupleBuilder>::with_capacity(
                &factories.layer_factories,
                capacity,
            ),
        }
    }

    #[inline]
    fn reserve(&mut self, _additional: usize) {}

    #[inline]
    fn push(&mut self, item: &mut DynPair<DynPair<K, V>, R>) {
        let (kv, r) = item.split();
        let (k, v) = kv.split();

        self.builder.push_refs((k, (v, r)));
    }

    #[inline]
    fn push_refs(&mut self, key: &K, val: &V, weight: &R) {
        self.builder.push_refs((key, (val, weight)))
    }

    #[inline]
    fn push_vals(&mut self, key: &mut K, val: &mut V, weight: &mut R) {
        self.builder.push_refs((key, (val, weight)))
    }

    #[inline(never)]
    fn done(self) -> FileIndexedZSet<K, V, R> {
        FileIndexedZSet {
            factories: self.factories,
            layer: self.builder.done(),
        }
    }
}

impl<K, V, R> SizeOf for FileIndexedZSetBuilder<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn size_of_children(&self, _context: &mut size_of::Context) {
        // XXX
    }
}

/*
pub struct FileIndexedZSetConsumer<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    consumer: FileOrderedLayerConsumer<K, V, R>,
}

impl<K, V, R> Consumer<K, V, R, ()> for FileIndexedZSetConsumer<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    type ValueConsumer<'a> = FileIndexedZSetValueConsumer<'a, K, V,  R>
    where
        Self: 'a;

    fn key_valid(&self) -> bool {
        self.consumer.key_valid()
    }

    fn peek_key(&self) -> &K {
        self.consumer.peek_key()
    }

    fn next_key(&mut self) -> (K, Self::ValueConsumer<'_>) {
        let (key, consumer) = self.consumer.next_key();
        (key, FileIndexedZSetValueConsumer::new(consumer))
    }

    fn seek_key(&mut self, key: &K)
    where
        K: Ord,
    {
        self.consumer.seek_key(key)
    }
}

pub struct FileIndexedZSetValueConsumer<'a, K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    consumer: FileOrderedLayerValues<'a, K, V, R>,
}

impl<'a, K, V, R> FileIndexedZSetValueConsumer<'a, K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    #[inline]
    const fn new(consumer: FileOrderedLayerValues<'a, K, V, R>) -> Self {
        Self { consumer }
    }
}

impl<'a, K, V, R> ValueConsumer<'a, V, R, ()> for FileIndexedZSetValueConsumer<'a, K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn value_valid(&self) -> bool {
        self.consumer.value_valid()
    }

    fn next_value(&mut self) -> (V, R, ()) {
        self.consumer.next_value()
    }

    fn remaining_values(&self) -> usize {
        self.consumer.remaining_values()
    }
}

 */

impl<K, V, R> SizeOf for FileIndexedZSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn size_of_children(&self, _context: &mut size_of::Context) {
        // XXX
    }
}

impl<K, V, R> Archive for FileIndexedZSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    type Archived = ();
    type Resolver = ();

    unsafe fn resolve(&self, _pos: usize, _resolver: Self::Resolver, _out: *mut Self::Archived) {
        unimplemented!();
    }
}

impl<K, V, R, S> Serialize<S> for FileIndexedZSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    S: Serializer + ?Sized,
{
    fn serialize(&self, _serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        unimplemented!();
    }
}

impl<K, V, R, D> Deserialize<FileIndexedZSet<K, V, R>, D> for Archived<FileIndexedZSet<K, V, R>>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    D: Fallible,
{
    fn deserialize(&self, _deserializer: &mut D) -> Result<FileIndexedZSet<K, V, R>, D::Error> {
        unimplemented!();
    }
}
