use crate::{
    dynamic::{
        DataTrait, DynDataTyped, DynPair, DynUnit, DynVec, DynWeightedPairs, Erase, Factory,
        LeanVec, WeightTrait, WithFactory,
    },
    time::{Antichain, AntichainRef},
    trace::{
        layers::{
            Builder as TrieBuilder, Cursor as TrieCursor, FileOrderedCursor, FileOrderedLayer,
            FileOrderedLayerFactories, FileOrderedTupleBuilder, MergeBuilder, Trie, TupleBuilder,
        },
        ord::merge_batcher::MergeBatcher,
        Batch, BatchFactories, BatchReader, BatchReaderFactories, Builder, Cursor, Filter, Merger,
        WeightedItem,
    },
    utils::Tup2,
    DBData, DBWeight, NumEntries, Timestamp,
};
use dyn_clone::clone_box;
use rand::Rng;
use rkyv::{ser::Serializer, Archive, Archived, Deserialize, Fallible, Serialize};
use size_of::SizeOf;
use std::fmt::{self, Debug, Display};
use std::path::PathBuf;

pub struct FileKeyBatchFactories<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    layer_factories: FileOrderedLayerFactories<K, DynDataTyped<T>, R>,
    keys_factory: &'static dyn Factory<DynVec<K>>,
    item_factory: &'static dyn Factory<DynPair<K, DynUnit>>,
    weighted_item_factory: &'static dyn Factory<WeightedItem<K, DynUnit, R>>,
    weighted_items_factory: &'static dyn Factory<DynWeightedPairs<DynPair<K, DynUnit>, R>>,
}

impl<K, T, R> Clone for FileKeyBatchFactories<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
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

impl<K, T, R> BatchReaderFactories<K, DynUnit, T, R> for FileKeyBatchFactories<K, T, R>
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
            layer_factories: FileOrderedLayerFactories::new::<KType, T, RType>(),
            keys_factory: WithFactory::<LeanVec<KType>>::FACTORY,
            item_factory: WithFactory::<Tup2<KType, ()>>::FACTORY,
            weighted_item_factory: WithFactory::<Tup2<Tup2<KType, ()>, RType>>::FACTORY,
            weighted_items_factory: WithFactory::<LeanVec<Tup2<Tup2<KType, ()>, RType>>>::FACTORY,
        }
    }

    fn key_factory(&self) -> &'static dyn Factory<K> {
        self.layer_factories.factories0.key_factory
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

impl<K, T, R> BatchFactories<K, DynUnit, T, R> for FileKeyBatchFactories<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
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

/// A batch of keys with weights and times.
///
/// Each tuple in `FileKeyBatch<K, T, R>` has key type `K`, value type `()`,
/// weight type `R`, and time type `R`.
pub struct FileKeyBatch<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    factories: FileKeyBatchFactories<K, T, R>,
    pub layer: FileOrderedLayer<K, DynDataTyped<T>, R>,
    pub lower: Antichain<T>,
    pub upper: Antichain<T>,
}

impl<K, T, R> Debug for FileKeyBatch<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileKeyBatch")
            .field("layer", &self.layer)
            .field("lower", &self.lower)
            .field("upper", &self.upper)
            .finish()
    }
}

impl<K, T, R> Clone for FileKeyBatch<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            factories: self.factories.clone(),
            layer: self.layer.clone(),
            lower: self.lower.clone(),
            upper: self.upper.clone(),
        }
    }
}

impl<K, T, R> Display for FileKeyBatch<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    FileOrderedLayer<K, DynDataTyped<T>, R>: Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "layer:\n{}",
            textwrap::indent(&self.layer.to_string(), "    ")
        )
    }
}

impl<K, T, R> NumEntries for FileKeyBatch<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    const CONST_NUM_ENTRIES: Option<usize> =
        <FileOrderedLayer<K, DynDataTyped<T>, R>>::CONST_NUM_ENTRIES;

    #[inline]
    fn num_entries_shallow(&self) -> usize {
        self.layer.num_entries_shallow()
    }

    #[inline]
    fn num_entries_deep(&self) -> usize {
        self.layer.num_entries_deep()
    }
}

impl<K, T, R> BatchReader for FileKeyBatch<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    type Factories = FileKeyBatchFactories<K, T, R>;
    type Key = K;
    type Val = DynUnit;
    type Time = T;
    type R = R;
    type Cursor<'s> = FileKeyCursor<'s, K, T, R>;

    fn factories(&self) -> Self::Factories {
        self.factories.clone()
    }

    fn cursor(&self) -> Self::Cursor<'_> {
        FileKeyCursor::new(self)
    }

    fn key_count(&self) -> usize {
        <FileOrderedLayer<K, DynDataTyped<T>, R> as Trie>::keys(&self.layer)
    }

    fn len(&self) -> usize {
        <FileOrderedLayer<K, DynDataTyped<T>, R> as Trie>::tuples(&self.layer)
    }

    fn lower(&self) -> AntichainRef<'_, T> {
        self.lower.as_ref()
    }

    fn upper(&self) -> AntichainRef<'_, T> {
        self.upper.as_ref()
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

impl<K, T, R> Batch for FileKeyBatch<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    type Batcher = MergeBatcher<Self>;
    type Builder = FileKeyBuilder<K, T, R>;
    type Merger = FileKeyMerger<K, T, R>;

    fn begin_merge(&self, other: &Self) -> Self::Merger {
        Self::Merger::new_merger(self, other)
    }

    fn recede_to(&mut self, frontier: &T) {
        // Nothing to do if the batch is entirely before the frontier.
        if !self.upper().less_equal(frontier) {
            // TODO: Optimize case where self.upper()==self.lower().
            self.do_recede_to(frontier);
        }
    }

    fn persistent_id(&self) -> Option<PathBuf> {
        Some(self.layer.path())
    }
}

impl<K, T, R> FileKeyBatch<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn do_recede_to(&mut self, _frontier: &T) {
        todo!()
    }
}

/// State for an in-progress merge.
pub struct FileKeyMerger<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    factories: FileKeyBatchFactories<K, T, R>,
    result: Option<FileOrderedLayer<K, DynDataTyped<T>, R>>,
    lower: Antichain<T>,
    upper: Antichain<T>,
}

impl<K, T, R> Merger<K, DynUnit, T, R, FileKeyBatch<K, T, R>> for FileKeyMerger<K, T, R>
where
    Self: SizeOf,
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn new_merger(batch1: &FileKeyBatch<K, T, R>, batch2: &FileKeyBatch<K, T, R>) -> Self {
        FileKeyMerger {
            factories: batch1.factories.clone(),
            result: None,
            lower: batch1.lower().meet(batch2.lower()),
            upper: batch1.upper().join(batch2.upper()),
        }
    }

    fn done(mut self) -> FileKeyBatch<K, T, R> {
        FileKeyBatch {
            factories: self.factories.clone(),
            layer: self
                .result
                .take()
                .unwrap_or_else(|| FileOrderedLayer::empty(&self.factories.layer_factories)),
            lower: self.lower,
            upper: self.upper,
        }
    }

    fn work(
        &mut self,
        source1: &FileKeyBatch<K, T, R>,
        source2: &FileKeyBatch<K, T, R>,
        key_filter: &Option<Filter<K>>,
        _value_filter: &Option<Filter<DynUnit>>,
        fuel: &mut isize,
    ) {
        debug_assert!(*fuel > 0);
        if self.result.is_none() {
            let mut builder =
                <<FileOrderedLayer<K, DynDataTyped<T>, R> as Trie>::MergeBuilder as MergeBuilder>::with_capacity(
                    &source1.layer,
                    &source2.layer,
                );
            let cursor1 = source1.layer.cursor();
            let cursor2 = source2.layer.cursor();
            if let Some(key_filter) = key_filter {
                builder.push_merge_retain_keys(cursor1, cursor2, key_filter)
            } else {
                builder.push_merge(cursor1, cursor2);
            }
            self.result = Some(builder.done());
        }
    }
}

impl<K, T, R> SizeOf for FileKeyMerger<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn size_of_children(&self, _context: &mut size_of::Context) {
        // XXX
    }
}

/// A cursor for navigating a single layer.
#[derive(Debug, SizeOf)]
pub struct FileKeyCursor<'s, K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    weight: Box<R>,
    val_valid: bool,
    cursor: FileOrderedCursor<'s, K, DynDataTyped<T>, R>,
}

impl<'s, K, T, R> Clone for FileKeyCursor<'s, K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            weight: clone_box(&self.weight),
            val_valid: self.val_valid,
            cursor: self.cursor.clone(),
        }
    }
}

impl<'s, K, T, R> FileKeyCursor<'s, K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn new(batch: &'s FileKeyBatch<K, T, R>) -> Self {
        Self {
            weight: batch.factories.weight_factory().default_box(),
            cursor: batch.layer.cursor(),
            val_valid: true,
        }
    }
}

impl<'s, K, T, R> Cursor<K, DynUnit, T, R> for FileKeyCursor<'s, K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn weight_factory(&self) -> &'static dyn Factory<R> {
        self.cursor.storage.factories.diff_factory
    }

    fn key(&self) -> &K {
        self.cursor.item()
    }

    fn val(&self) -> &DynUnit {
        &()
    }

    fn map_times(&mut self, logic: &mut dyn FnMut(&T, &R)) {
        let mut subcursor = self.cursor.values();
        while let Some((time, diff)) = subcursor.get_current_item() {
            logic(time, diff);
            subcursor.step();
        }
    }

    fn map_times_through(&mut self, upper: &T, logic: &mut dyn FnMut(&T, &R)) {
        let mut subcursor = self.cursor.values();
        while let Some((time, diff)) = subcursor.get_current_item() {
            if time.less_equal(upper) {
                logic(time, diff);
            }
            subcursor.step();
        }
    }

    fn map_values(&mut self, logic: &mut dyn FnMut(&DynUnit, &R))
    where
        T: PartialEq<()>,
    {
        if self.val_valid() {
            logic(&(), self.cursor.values().get_current_item().unwrap().1)
        }
    }

    fn weight(&mut self) -> &R
    where
        T: PartialEq<()>,
    {
        self.cursor
            .values()
            .get_current_item()
            .unwrap()
            .1
            .clone_to(&mut self.weight);
        self.weight.as_ref()
    }

    fn key_valid(&self) -> bool {
        self.cursor.valid()
    }

    fn val_valid(&self) -> bool {
        self.val_valid
    }

    fn step_key(&mut self) {
        self.cursor.step();
        self.val_valid = true;
    }

    fn step_key_reverse(&mut self) {
        self.cursor.step_reverse();
        self.val_valid = true;
    }

    fn seek_key(&mut self, key: &K) {
        self.cursor.seek(key);
        self.val_valid = true;
    }

    fn seek_key_with(&mut self, predicate: &dyn Fn(&K) -> bool) {
        self.cursor.seek_with(predicate);
        self.val_valid = true;
    }

    fn seek_key_with_reverse(&mut self, predicate: &dyn Fn(&K) -> bool) {
        self.cursor.seek_with_reverse(predicate);
        self.val_valid = true;
    }

    fn seek_key_reverse(&mut self, key: &K) {
        self.cursor.seek_reverse(key);
        self.val_valid = true;
    }

    fn step_val(&mut self) {
        self.val_valid = false;
    }

    fn seek_val(&mut self, _val: &DynUnit) {}

    fn seek_val_with(&mut self, predicate: &dyn Fn(&DynUnit) -> bool) {
        if !predicate(&()) {
            self.val_valid = false;
        }
    }

    fn rewind_keys(&mut self) {
        self.cursor.rewind();
        self.val_valid = true;
    }

    fn fast_forward_keys(&mut self) {
        self.cursor.fast_forward();
        self.val_valid = true;
    }

    fn rewind_vals(&mut self) {
        self.val_valid = true;
    }

    fn step_val_reverse(&mut self) {
        self.val_valid = false;
    }

    fn seek_val_reverse(&mut self, _val: &DynUnit) {}

    fn seek_val_with_reverse(&mut self, predicate: &dyn Fn(&DynUnit) -> bool) {
        if !predicate(&()) {
            self.val_valid = false;
        }
    }

    fn fast_forward_vals(&mut self) {
        self.val_valid = true;
    }
}

/// A builder for creating layers from unsorted update tuples.
pub struct FileKeyBuilder<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    factories: FileKeyBatchFactories<K, T, R>,
    time: T,
    builder: FileOrderedTupleBuilder<K, DynDataTyped<T>, R>,
}

impl<K, T, R> Builder<FileKeyBatch<K, T, R>> for FileKeyBuilder<K, T, R>
where
    Self: SizeOf,
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    #[inline]
    fn new_builder(factories: &<FileKeyBatch<K, T, R> as BatchReader>::Factories, time: T) -> Self {
        Self {
            factories: factories.clone(),
            time,
            builder: <FileOrderedTupleBuilder<K, DynDataTyped<T>, R> as TupleBuilder>::new(
                &factories.layer_factories,
            ),
        }
    }

    #[inline]
    fn with_capacity(
        factories: &<FileKeyBatch<K, T, R> as BatchReader>::Factories,
        time: T,
        cap: usize,
    ) -> Self {
        Self {
            factories: factories.clone(),
            time,
            builder:
                <FileOrderedTupleBuilder<K, DynDataTyped<T>, R> as TupleBuilder>::with_capacity(
                    &factories.layer_factories,
                    cap,
                ),
        }
    }

    #[inline]
    fn reserve(&mut self, _additional: usize) {}

    #[inline]
    fn push(&mut self, item: &mut WeightedItem<K, DynUnit, R>) {
        let (kv, weight) = item.split();
        let k = kv.fst();
        self.builder.push_refs((k, (&self.time, weight)));
    }

    fn push_refs(&mut self, key: &K, _val: &DynUnit, weight: &R) {
        self.builder.push_refs((key, (&self.time, weight)));
    }

    fn push_vals(&mut self, key: &mut K, _val: &mut DynUnit, weight: &mut R) {
        self.builder.push_refs((key, (&self.time, weight)));
    }

    #[inline(never)]
    fn done(self) -> FileKeyBatch<K, T, R> {
        let time_next = self.time.advance(0);
        let upper = if time_next <= self.time {
            Antichain::new()
        } else {
            Antichain::from_elem(time_next)
        };

        FileKeyBatch {
            factories: self.factories,
            layer: self.builder.done(),
            lower: Antichain::from_elem(self.time),
            upper,
        }
    }
}

impl<K, T, R> SizeOf for FileKeyBuilder<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn size_of_children(&self, _context: &mut size_of::Context) {
        // XXX
    }
}

/*
pub struct FileKeyConsumer<K, T, R>

where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    consumer: FileOrderedLayerConsumer<K, T, R>,
}

impl<K, T, R> Consumer<K, (), R, T> for FileKeyConsumer<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    type ValueConsumer<'a> = FileKeyValueConsumer<'a, K, T, R>
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
        (key, FileKeyValueConsumer::new(values))
    }

    fn seek_key(&mut self, key: &K)
    where
        K: Ord,
    {
        self.consumer.seek_key(key);
    }
}

pub struct FileKeyValueConsumer<'a, K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    consumer: FileOrderedLayerValues<'a, K, T, R>,
}

impl<'a, K, T, R> FileKeyValueConsumer<'a, K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    const fn new(consumer: FileOrderedLayerValues<'a, K, T, R>) -> Self {
        Self { consumer }
    }
}

impl<'a, K, T, R> ValueConsumer<'a, (), R, T> for FileKeyValueConsumer<'a, K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
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
}*/

impl<K, T, R> SizeOf for FileKeyBatch<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn size_of_children(&self, _context: &mut size_of::Context) {
        // XXX
    }
}

impl<K, T, R> Archive for FileKeyBatch<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    type Archived = ();
    type Resolver = ();

    unsafe fn resolve(&self, _pos: usize, _resolver: Self::Resolver, _out: *mut Self::Archived) {
        unimplemented!();
    }
}

impl<K, T, R, S> Serialize<S> for FileKeyBatch<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    S: Serializer + ?Sized,
{
    fn serialize(&self, _serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        unimplemented!();
    }
}

impl<K, T, R, D> Deserialize<FileKeyBatch<K, T, R>, D> for Archived<FileKeyBatch<K, T, R>>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    D: Fallible,
{
    fn deserialize(&self, _deserializer: &mut D) -> Result<FileKeyBatch<K, T, R>, D::Error> {
        unimplemented!();
    }
}
