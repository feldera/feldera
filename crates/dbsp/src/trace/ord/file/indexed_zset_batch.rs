use crate::{
    algebra::{AddAssignByRef, AddByRef, NegByRef},
    time::AntichainRef,
    trace::{
        layers::{
            file::ordered::{
                FileOrderedCursor, FileOrderedLayer, FileOrderedLayerConsumer,
                FileOrderedLayerValues, FileOrderedTupleBuilder, FileOrderedValueCursor,
            },
            Builder as TrieBuilder, Cursor as TrieCursor, MergeBuilder, Trie, TupleBuilder,
        },
        ord::merge_batcher::MergeBatcher,
        Batch, BatchReader, Builder, Consumer, Cursor, Filter, Merger, ValueConsumer,
    },
    DBData, DBWeight, NumEntries,
};
use rand::Rng;
use rkyv::{ser::Serializer, Archive, Archived, Deserialize, Fallible, Serialize};
use size_of::SizeOf;
use std::path::PathBuf;
use std::{
    fmt::{self, Debug, Display},
    ops::{Add, AddAssign, Neg},
    rc::Rc,
};

/// A batch of key-value weighted tuples without timing information.
///
/// Each tuple in `FileIndexedZSet<K, V, R>` has key type `K`, value type `V`,
/// weight type `R`, and time `()`.
#[derive(Debug, Clone)]
#[cfg_attr(test, derive(Eq))]
pub struct FileIndexedZSet<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    #[doc(hidden)]
    pub layer: FileOrderedLayer<K, V, R>,
}

// This is `#[cfg(test)]` only because it would be surprisingly expensive in
// production.
#[cfg(test)]
impl<Other, K, V, R> PartialEq<Other> for FileIndexedZSet<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
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

impl<K, V, R> Display for FileIndexedZSet<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
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

impl<K, V, R> Default for FileIndexedZSet<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    #[inline]
    fn default() -> Self {
        Self::empty(())
    }
}

impl<K, V, R> From<FileOrderedLayer<K, V, R>> for FileIndexedZSet<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    #[inline]
    fn from(layer: FileOrderedLayer<K, V, R>) -> Self {
        Self { layer }
    }
}

impl<K, V, R> From<FileOrderedLayer<K, V, R>> for Rc<FileIndexedZSet<K, V, R>>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    #[inline]
    fn from(layer: FileOrderedLayer<K, V, R>) -> Self {
        Rc::new(From::from(layer))
    }
}

impl<K, V, R> NumEntries for FileIndexedZSet<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
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
    K: DBData,
    V: DBData,
    R: DBWeight + NegByRef,
{
    #[inline]
    fn neg_by_ref(&self) -> Self {
        Self {
            layer: self.layer.neg_by_ref(),
        }
    }
}

impl<K, V, R> Neg for FileIndexedZSet<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight + Neg<Output = R>,
{
    type Output = Self;

    #[inline]
    fn neg(self) -> Self {
        Self {
            layer: self.layer.neg(),
        }
    }
}

impl<K, V, R> Add<Self> for FileIndexedZSet<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    type Output = Self;
    #[inline]

    fn add(self, rhs: Self) -> Self::Output {
        Self {
            layer: self.layer.add(rhs.layer),
        }
    }
}

impl<K, V, R> AddAssign<Self> for FileIndexedZSet<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    #[inline]
    fn add_assign(&mut self, rhs: Self) {
        self.layer.add_assign(rhs.layer);
    }
}

impl<K, V, R> AddAssignByRef for FileIndexedZSet<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    #[inline]
    fn add_assign_by_ref(&mut self, rhs: &Self) {
        self.layer.add_assign_by_ref(&rhs.layer);
    }
}

impl<K, V, R> AddByRef for FileIndexedZSet<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    #[inline]
    fn add_by_ref(&self, rhs: &Self) -> Self {
        Self {
            layer: self.layer.add_by_ref(&rhs.layer),
        }
    }
}

impl<K, V, R> BatchReader for FileIndexedZSet<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    type Key = K;
    type Val = V;
    type Time = ();
    type R = R;
    type Cursor<'s> = FileIndexedZSetCursor<'s, K, V, R>
    where
        V: 's;
    type Consumer = FileIndexedZSetConsumer<K, V, R>;

    #[inline]
    fn cursor(&self) -> Self::Cursor<'_> {
        FileIndexedZSetCursor::new(self)
    }

    #[inline]
    fn consumer(self) -> Self::Consumer {
        FileIndexedZSetConsumer {
            consumer: FileOrderedLayerConsumer::from(self.layer),
        }
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

    fn sample_keys<RG>(&self, rng: &mut RG, sample_size: usize, sample: &mut Vec<Self::Key>)
    where
        RG: Rng,
    {
        self.layer.sample_keys(rng, sample_size, sample);
    }
}

impl<K, V, R> Batch for FileIndexedZSet<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    type Item = (K, V);
    type Batcher = MergeBatcher<(K, V), (), R, Self>;
    type Builder = FileIndexedZSetBuilder<K, V, R>;
    type Merger = FileIndexedZSetMerger<K, V, R>;

    fn item_from(key: K, val: V) -> Self::Item {
        (key, val)
    }

    fn from_keys(time: Self::Time, keys: Vec<(Self::Key, Self::R)>) -> Self
    where
        Self::Val: From<()>,
    {
        Self::from_tuples(
            time,
            keys.into_iter()
                .map(|(k, w)| ((k, From::from(())), w))
                .collect(),
        )
    }

    fn begin_merge(&self, other: &Self) -> Self::Merger {
        FileIndexedZSetMerger::new_merger(self, other)
    }

    fn recede_to(&mut self, _frontier: &()) {}

    fn empty(_time: Self::Time) -> Self {
        Self {
            layer: FileOrderedLayer::empty(),
        }
    }
    fn persistent_id(&self) -> Option<PathBuf> {
        Some(self.layer.path())
    }
}

/// State for an in-progress merge.
pub struct FileIndexedZSetMerger<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    result: Option<FileOrderedLayer<K, V, R>>,
}

impl<K, V, R> Merger<K, V, (), R, FileIndexedZSet<K, V, R>> for FileIndexedZSetMerger<K, V, R>
where
    Self: SizeOf,
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    #[inline]
    fn new_merger(_batch1: &FileIndexedZSet<K, V, R>, _batch2: &FileIndexedZSet<K, V, R>) -> Self {
        Self { result: None }
    }

    #[inline]
    fn done(mut self) -> FileIndexedZSet<K, V, R> {
        FileIndexedZSet {
            layer: self.result.take().unwrap_or_default(),
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
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    fn size_of_children(&self, _context: &mut size_of::Context) {
        // XXX
    }
}

/// A cursor for navigating a single layer.
#[derive(Debug, SizeOf, Clone)]
pub struct FileIndexedZSetCursor<'s, K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    key_cursor: FileOrderedCursor<'s, K, V, R>,
    val_cursor: FileOrderedValueCursor<'s, K, V, R>,
}

impl<'s, K, V, R> FileIndexedZSetCursor<'s, K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
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
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    fn key(&self) -> &K {
        self.key_cursor.current_key()
    }

    fn val(&self) -> &V {
        self.val_cursor.current_value()
    }

    fn fold_times<F, U>(&mut self, init: U, mut fold: F) -> U
    where
        F: FnMut(U, &(), &R) -> U,
    {
        if self.val_cursor.valid() {
            fold(init, &(), self.val_cursor.current_diff())
        } else {
            init
        }
    }

    fn fold_times_through<F, U>(&mut self, _upper: &(), init: U, fold: F) -> U
    where
        F: FnMut(U, &(), &R) -> U,
    {
        self.fold_times(init, fold)
    }

    fn weight(&mut self) -> R {
        debug_assert!(self.val_cursor.valid());
        self.val_cursor.current_diff().clone()
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

    fn seek_key_with<P>(&mut self, predicate: P)
    where
        P: Fn(&K) -> bool + Clone,
    {
        self.move_key(|key_cursor| key_cursor.seek_with(&predicate));
    }

    fn seek_key_with_reverse<P>(&mut self, predicate: P)
    where
        P: Fn(&K) -> bool + Clone,
    {
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

    fn seek_val_with<P>(&mut self, predicate: P)
    where
        P: Fn(&V) -> bool + Clone,
    {
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

    fn seek_val_with_reverse<P>(&mut self, predicate: P)
    where
        P: Fn(&V) -> bool + Clone,
    {
        self.val_cursor.seek_val_with_reverse(predicate);
    }

    fn fast_forward_vals(&mut self) {
        self.val_cursor.fast_forward();
    }
}

/// A builder for creating layers from unsorted update tuples.
pub struct FileIndexedZSetBuilder<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    builder: FileOrderedTupleBuilder<K, V, R>,
}

impl<K, V, R> Builder<(K, V), (), R, FileIndexedZSet<K, V, R>> for FileIndexedZSetBuilder<K, V, R>
where
    Self: SizeOf,
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    #[inline]
    fn new_builder(_time: ()) -> Self {
        Self {
            builder: FileOrderedTupleBuilder::<K, V, R>::new(),
        }
    }

    #[inline]
    fn with_capacity(_time: (), capacity: usize) -> Self {
        Self {
            builder: <FileOrderedTupleBuilder<K, V, R> as TupleBuilder>::with_capacity(capacity),
        }
    }

    #[inline]
    fn reserve(&mut self, _additional: usize) {}

    #[inline]
    fn push(&mut self, ((key, val), diff): ((K, V), R)) {
        self.builder.push_tuple((key, (val, diff)));
    }

    #[inline(never)]
    fn done(self) -> FileIndexedZSet<K, V, R> {
        FileIndexedZSet {
            layer: self.builder.done(),
        }
    }
}

impl<K, V, R> SizeOf for FileIndexedZSetBuilder<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    fn size_of_children(&self, _context: &mut size_of::Context) {
        // XXX
    }
}

pub struct FileIndexedZSetConsumer<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    consumer: FileOrderedLayerConsumer<K, V, R>,
}

impl<K, V, R> Consumer<K, V, R, ()> for FileIndexedZSetConsumer<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
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
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    consumer: FileOrderedLayerValues<'a, K, V, R>,
}

impl<'a, K, V, R> FileIndexedZSetValueConsumer<'a, K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    #[inline]
    const fn new(consumer: FileOrderedLayerValues<'a, K, V, R>) -> Self {
        Self { consumer }
    }
}

impl<'a, K, V, R> ValueConsumer<'a, V, R, ()> for FileIndexedZSetValueConsumer<'a, K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
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

impl<K, V, R> SizeOf for FileIndexedZSet<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    fn size_of_children(&self, _context: &mut size_of::Context) {
        // XXX
    }
}

impl<K, V, R> Archive for FileIndexedZSet<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    type Archived = ();
    type Resolver = ();

    unsafe fn resolve(&self, _pos: usize, _resolver: Self::Resolver, _out: *mut Self::Archived) {
        unimplemented!();
    }
}

impl<K, V, R, S> Serialize<S> for FileIndexedZSet<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
    S: Serializer + ?Sized,
{
    fn serialize(&self, _serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        unimplemented!();
    }
}

impl<K, V, R, D> Deserialize<FileIndexedZSet<K, V, R>, D> for Archived<FileIndexedZSet<K, V, R>>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
    D: Fallible,
{
    fn deserialize(&self, _deserializer: &mut D) -> Result<FileIndexedZSet<K, V, R>, D::Error> {
        unimplemented!();
    }
}
