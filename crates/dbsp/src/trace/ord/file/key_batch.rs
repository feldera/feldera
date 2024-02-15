use crate::{
    time::{Antichain, AntichainRef},
    trace::{
        layers::{
            file::ordered::{
                FileOrderedCursor, FileOrderedLayer, FileOrderedLayerConsumer,
                FileOrderedLayerValues, FileOrderedTupleBuilder,
            },
            Builder as TrieBuilder, Cursor as TrieCursor, MergeBuilder, Trie, TupleBuilder,
        },
        ord::merge_batcher::MergeBatcher,
        Batch, BatchReader, Builder, Consumer, Cursor, Filter, Merger, ValueConsumer,
    },
    DBData, DBTimestamp, DBWeight, NumEntries,
};
use rand::Rng;
use rkyv::{ser::Serializer, Archive, Archived, Deserialize, Fallible, Serialize};
use size_of::SizeOf;
use std::fmt::{self, Debug, Display};
use std::path::PathBuf;

/// A batch of keys with weights and times.
///
/// Each tuple in `FileKeyBatch<K, T, R>` has key type `K`, value type `()`,
/// weight type `R`, and time type `R`.
#[derive(Debug, Clone)]
pub struct FileKeyBatch<K, T, R>
where
    K: DBData,
    T: DBTimestamp,
    R: DBWeight,
{
    pub layer: FileOrderedLayer<K, T, R>,
    pub lower: Antichain<T>,
    pub upper: Antichain<T>,
}

impl<K, T, R> Display for FileKeyBatch<K, T, R>
where
    K: DBData,
    T: DBTimestamp,
    R: DBWeight,
    FileOrderedLayer<K, T, R>: Display,
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
    K: DBData,
    T: DBTimestamp,
    R: DBWeight,
{
    const CONST_NUM_ENTRIES: Option<usize> = <FileOrderedLayer<K, T, R>>::CONST_NUM_ENTRIES;

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
    K: DBData,
    T: DBTimestamp,
    R: DBWeight,
{
    type Key = K;
    type Val = ();
    type Time = T;
    type R = R;
    type Cursor<'s> = FileKeyCursor<'s, K, T, R>;
    type Consumer = FileKeyConsumer<K, T, R>;

    fn cursor(&self) -> Self::Cursor<'_> {
        FileKeyCursor::new(self)
    }

    fn consumer(self) -> Self::Consumer {
        todo!()
    }

    fn key_count(&self) -> usize {
        <FileOrderedLayer<K, T, R> as Trie>::keys(&self.layer)
    }

    fn len(&self) -> usize {
        <FileOrderedLayer<K, T, R> as Trie>::tuples(&self.layer)
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

    fn sample_keys<RG>(&self, rng: &mut RG, sample_size: usize, sample: &mut Vec<Self::Key>)
    where
        RG: Rng,
    {
        self.layer.sample_keys(rng, sample_size, sample);
    }
}

impl<K, T, R> Batch for FileKeyBatch<K, T, R>
where
    K: DBData,
    T: DBTimestamp,
    R: DBWeight,
{
    type Item = K;
    type Batcher = MergeBatcher<K, T, R, Self>;
    type Builder = FileKeyBuilder<K, T, R>;
    type Merger = FileKeyMerger<K, T, R>;

    fn item_from(key: K, _val: ()) -> Self::Item {
        key
    }

    fn from_keys(time: Self::Time, keys: Vec<(Self::Key, Self::R)>) -> Self {
        Self::from_tuples(time, keys)
    }

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
    K: DBData,
    T: DBTimestamp,
    R: DBWeight,
{
    fn do_recede_to(&mut self, _frontier: &T) {
        todo!()
    }
}

/// State for an in-progress merge.
pub struct FileKeyMerger<K, T, R>
where
    K: DBData,
    T: DBTimestamp,
    R: DBWeight,
{
    result: Option<FileOrderedLayer<K, T, R>>,
    lower: Antichain<T>,
    upper: Antichain<T>,
}

impl<K, T, R> Merger<K, (), T, R, FileKeyBatch<K, T, R>> for FileKeyMerger<K, T, R>
where
    Self: SizeOf,
    K: DBData,
    T: DBTimestamp,
    R: DBWeight,
{
    fn new_merger(batch1: &FileKeyBatch<K, T, R>, batch2: &FileKeyBatch<K, T, R>) -> Self {
        FileKeyMerger {
            result: None,
            lower: batch1.lower().meet(batch2.lower()),
            upper: batch1.upper().join(batch2.upper()),
        }
    }

    fn done(mut self) -> FileKeyBatch<K, T, R> {
        FileKeyBatch {
            layer: self.result.take().unwrap_or_default(),
            lower: self.lower,
            upper: self.upper,
        }
    }

    fn work(
        &mut self,
        source1: &FileKeyBatch<K, T, R>,
        source2: &FileKeyBatch<K, T, R>,
        key_filter: &Option<Filter<K>>,
        _value_filter: &Option<Filter<()>>,
        fuel: &mut isize,
    ) {
        debug_assert!(*fuel > 0);
        if self.result.is_none() {
            let mut builder =
                <<FileOrderedLayer<K, T, R> as Trie>::MergeBuilder as MergeBuilder>::with_capacity(
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
    K: DBData,
    T: DBTimestamp,
    R: DBWeight,
{
    fn size_of_children(&self, _context: &mut size_of::Context) {
        // XXX
    }
}

/// A cursor for navigating a single layer.
#[derive(Debug, SizeOf, Clone)]
pub struct FileKeyCursor<'s, K, T, R>
where
    K: DBData,
    T: DBTimestamp,
    R: DBWeight,
{
    val_valid: bool,
    cursor: FileOrderedCursor<'s, K, T, R>,
}

impl<'s, K, T, R> FileKeyCursor<'s, K, T, R>
where
    K: DBData,
    T: DBTimestamp,
    R: DBWeight,
{
    fn new(batch: &'s FileKeyBatch<K, T, R>) -> Self {
        Self {
            cursor: batch.layer.cursor(),
            val_valid: true,
        }
    }
}

impl<'s, K, T, R> Cursor<K, (), T, R> for FileKeyCursor<'s, K, T, R>
where
    K: DBData,
    T: DBTimestamp,
    R: DBWeight,
{
    fn key(&self) -> &K {
        self.cursor.item()
    }

    fn val(&self) -> &() {
        &()
    }

    fn fold_times<F, U>(&mut self, mut init: U, mut fold: F) -> U
    where
        F: FnMut(U, &T, &R) -> U,
    {
        let mut subcursor = self.cursor.values();
        while let Some((time, diff)) = subcursor.take_current_item() {
            init = fold(init, &time, &diff);
        }

        init
    }

    fn fold_times_through<F, U>(&mut self, upper: &T, mut init: U, mut fold: F) -> U
    where
        F: FnMut(U, &T, &R) -> U,
    {
        let mut subcursor = self.cursor.values();
        while let Some((time, diff)) = subcursor.take_current_item() {
            if time.less_equal(upper) {
                init = fold(init, &time, &diff);
            }
        }

        init
    }

    fn weight(&mut self) -> R
    where
        T: PartialEq<()>,
    {
        self.cursor.values().take_current_item().unwrap().1.clone()
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

    fn seek_key_with<P>(&mut self, predicate: P)
    where
        P: Fn(&K) -> bool + Clone,
    {
        self.cursor.seek_with(|k| !predicate(k));
        self.val_valid = true;
    }

    fn seek_key_with_reverse<P>(&mut self, predicate: P)
    where
        P: Fn(&K) -> bool + Clone,
    {
        self.cursor.seek_with_reverse(|k| !predicate(k));
        self.val_valid = true;
    }

    fn seek_key_reverse(&mut self, key: &K) {
        self.cursor.seek_reverse(key);
        self.val_valid = true;
    }

    fn step_val(&mut self) {
        self.val_valid = false;
    }

    fn seek_val(&mut self, _val: &()) {}
    fn seek_val_with<P>(&mut self, predicate: P)
    where
        P: Fn(&()) -> bool + Clone,
    {
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

    fn seek_val_reverse(&mut self, _val: &()) {}

    fn seek_val_with_reverse<P>(&mut self, predicate: P)
    where
        P: Fn(&()) -> bool + Clone,
    {
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
    K: DBData,
    T: DBTimestamp,
    R: DBWeight,
{
    time: T,
    builder: FileOrderedTupleBuilder<K, T, R>,
}

impl<K, T, R> Builder<K, T, R, FileKeyBatch<K, T, R>> for FileKeyBuilder<K, T, R>
where
    Self: SizeOf,
    K: DBData,
    T: DBTimestamp,
    R: DBWeight,
{
    #[inline]
    fn new_builder(time: T) -> Self {
        Self {
            time,
            builder: <FileOrderedTupleBuilder<K, T, R> as TupleBuilder>::new(),
        }
    }

    #[inline]
    fn with_capacity(time: T, cap: usize) -> Self {
        Self {
            time,
            builder: <FileOrderedTupleBuilder<K, T, R> as TupleBuilder>::with_capacity(cap),
        }
    }

    #[inline]
    fn reserve(&mut self, _additional: usize) {}

    #[inline]
    fn push(&mut self, (key, diff): (K, R)) {
        self.builder.push_tuple((key, (self.time.clone(), diff)));
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
            layer: self.builder.done(),
            lower: Antichain::from_elem(self.time),
            upper,
        }
    }
}

impl<K, T, R> SizeOf for FileKeyBuilder<K, T, R>
where
    K: DBData,
    T: DBTimestamp,
    R: DBWeight,
{
    fn size_of_children(&self, _context: &mut size_of::Context) {
        // XXX
    }
}

pub struct FileKeyConsumer<K, T, R>
where
    K: DBData,
    T: DBTimestamp,
    R: DBWeight,
{
    consumer: FileOrderedLayerConsumer<K, T, R>,
}

impl<K, T, R> Consumer<K, (), R, T> for FileKeyConsumer<K, T, R>
where
    K: DBData,
    T: DBTimestamp,
    R: DBWeight,
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
    K: DBData,
    T: DBTimestamp,
    R: DBWeight,
{
    consumer: FileOrderedLayerValues<'a, K, T, R>,
}

impl<'a, K, T, R> FileKeyValueConsumer<'a, K, T, R>
where
    K: DBData,
    T: DBTimestamp,
    R: DBWeight,
{
    const fn new(consumer: FileOrderedLayerValues<'a, K, T, R>) -> Self {
        Self { consumer }
    }
}

impl<'a, K, T, R> ValueConsumer<'a, (), R, T> for FileKeyValueConsumer<'a, K, T, R>
where
    K: DBData,
    T: DBTimestamp,
    R: DBWeight,
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
}

impl<K, T, R> SizeOf for FileKeyBatch<K, T, R>
where
    K: DBData,
    T: DBTimestamp,
    R: DBWeight,
{
    fn size_of_children(&self, _context: &mut size_of::Context) {
        // XXX
    }
}

impl<K, T, R> Archive for FileKeyBatch<K, T, R>
where
    K: DBData,
    T: DBTimestamp,
    R: DBWeight,
{
    type Archived = ();
    type Resolver = ();

    unsafe fn resolve(&self, _pos: usize, _resolver: Self::Resolver, _out: *mut Self::Archived) {
        unimplemented!();
    }
}

impl<K, T, R, S> Serialize<S> for FileKeyBatch<K, T, R>
where
    K: DBData,
    T: DBTimestamp,
    R: DBWeight,
    S: Serializer + ?Sized,
{
    fn serialize(&self, _serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        unimplemented!();
    }
}

impl<K, T, R, D> Deserialize<FileKeyBatch<K, T, R>, D> for Archived<FileKeyBatch<K, T, R>>
where
    K: DBData,
    T: DBTimestamp,
    R: DBWeight,
    D: Fallible,
{
    fn deserialize(&self, _deserializer: &mut D) -> Result<FileKeyBatch<K, T, R>, D::Error> {
        unimplemented!();
    }
}
