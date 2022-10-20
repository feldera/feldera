//! Blanket implementations for reference counted batches.

use crate::{
    time::AntichainRef,
    trace::{Batch, BatchReader, Batcher, Builder, Consumer, Cursor, Merger, ValueConsumer},
};
use size_of::SizeOf;
use std::{
    fmt::{self, Debug},
    rc::Rc,
};

impl<B> BatchReader for Rc<B>
where
    B: BatchReader,
{
    type Key = B::Key;
    type Val = B::Val;
    type Time = B::Time;
    type R = B::R;

    type Cursor<'s> = RcBatchCursor<'s, B> where B: 's;

    type Consumer = RcBatchConsumer<B>;

    fn cursor(&self) -> Self::Cursor<'_> {
        RcBatchCursor::new(B::cursor(self))
    }

    fn consumer(self) -> Self::Consumer {
        todo!()
    }

    fn key_count(&self) -> usize {
        B::key_count(self)
    }

    fn len(&self) -> usize {
        B::len(self)
    }

    fn is_empty(&self) -> bool {
        B::is_empty(self)
    }

    fn lower(&self) -> AntichainRef<'_, Self::Time> {
        B::lower(self)
    }

    fn upper(&self) -> AntichainRef<'_, Self::Time> {
        B::upper(self)
    }
}

/// Wrapper to provide cursor to nested scope.
pub struct RcBatchCursor<'s, B>
where
    B: BatchReader + 's,
{
    cursor: B::Cursor<'s>,
}

impl<'s, B: BatchReader> RcBatchCursor<'s, B> {
    const fn new(cursor: B::Cursor<'s>) -> Self {
        RcBatchCursor { cursor }
    }
}

impl<'s, B> Cursor<'s, B::Key, B::Val, B::Time, B::R> for RcBatchCursor<'s, B>
where
    B: BatchReader,
{
    fn key_valid(&self) -> bool {
        self.cursor.key_valid()
    }

    fn val_valid(&self) -> bool {
        self.cursor.val_valid()
    }

    fn key(&self) -> &B::Key {
        self.cursor.key()
    }

    fn val(&self) -> &B::Val {
        self.cursor.val()
    }

    fn get_key(&self) -> Option<&B::Key> {
        self.cursor.get_key()
    }

    fn get_val(&self) -> Option<&B::Val> {
        self.cursor.get_val()
    }

    fn map_times<L: FnMut(&B::Time, &B::R)>(&mut self, logic: L) {
        self.cursor.map_times(logic)
    }

    fn fold_times<F, U>(&mut self, init: U, fold: F) -> U
    where
        F: FnMut(U, &B::Time, &B::R) -> U,
    {
        self.cursor.fold_times(init, fold)
    }

    fn map_times_through<L: FnMut(&B::Time, &B::R)>(&mut self, upper: &B::Time, logic: L) {
        self.cursor.map_times_through(upper, logic)
    }

    fn fold_times_through<F, U>(&mut self, upper: &B::Time, init: U, fold: F) -> U
    where
        F: FnMut(U, &B::Time, &B::R) -> U,
    {
        self.cursor.fold_times_through(upper, init, fold)
    }

    fn weight(&mut self) -> B::R
    where
        B::Time: PartialEq<()>,
    {
        self.cursor.weight()
    }

    fn map_values<L>(&mut self, logic: L)
    where
        B::Time: PartialEq<()>,
        L: FnMut(&B::Val, &B::R),
    {
        self.cursor.map_values(logic)
    }

    fn step_key(&mut self) {
        self.cursor.step_key()
    }

    fn seek_key(&mut self, key: &B::Key) {
        self.cursor.seek_key(key)
    }

    fn last_key(&mut self) -> Option<&B::Key> {
        self.cursor.last_key()
    }

    fn step_val(&mut self) {
        self.cursor.step_val()
    }

    fn seek_val(&mut self, val: &B::Val) {
        self.cursor.seek_val(val)
    }

    fn seek_val_with<P>(&mut self, predicate: P)
    where
        P: Fn(&B::Val) -> bool + Clone,
    {
        self.cursor.seek_val_with(predicate)
    }

    fn rewind_keys(&mut self) {
        self.cursor.rewind_keys()
    }

    fn rewind_vals(&mut self) {
        self.cursor.rewind_vals()
    }
}

pub struct RcBatchConsumer<B>
where
    B: BatchReader,
{
    consumer: B::Consumer,
}

impl<B> Consumer<B::Key, B::Val, B::R, B::Time> for RcBatchConsumer<B>
where
    B: BatchReader,
{
    type ValueConsumer<'a> = RcBatchValueConsumer<'a, B>
    where
        Self: 'a;

    fn key_valid(&self) -> bool {
        self.consumer.key_valid()
    }

    fn peek_key(&self) -> &B::Key {
        self.consumer.peek_key()
    }

    fn next_key(&mut self) -> (B::Key, Self::ValueConsumer<'_>) {
        let (key, values) = self.consumer.next_key();
        (key, RcBatchValueConsumer { values })
    }

    fn seek_key(&mut self, key: &B::Key)
    where
        B::Key: Ord,
    {
        self.consumer.seek_key(key);
    }
}

pub struct RcBatchValueConsumer<'a, B>
where
    B: BatchReader + 'a,
{
    #[allow(clippy::type_complexity)]
    values: <B::Consumer as Consumer<B::Key, B::Val, B::R, B::Time>>::ValueConsumer<'a>,
}

impl<'a, B> ValueConsumer<'a, B::Val, B::R, B::Time> for RcBatchValueConsumer<'a, B>
where
    B: BatchReader + 'a,
{
    fn value_valid(&self) -> bool {
        self.values.value_valid()
    }

    fn next_value(&mut self) -> (B::Val, B::R, B::Time) {
        self.values.next_value()
    }

    fn remaining_values(&self) -> usize {
        self.values.remaining_values()
    }
}

/// An immutable collection of updates.
impl<B> Batch for Rc<B>
where
    B: Batch,
{
    type Item = B::Item;
    type Batcher = RcBatcher<B>;
    type Builder = RcBuilder<B>;
    type Merger = RcMerger<B>;

    fn item_from(key: Self::Key, val: Self::Val) -> Self::Item {
        B::item_from(key, val)
    }

    fn from_tuples(time: Self::Time, tuples: Vec<(Self::Item, Self::R)>) -> Self {
        Rc::new(B::from_tuples(time, tuples))
    }

    fn from_keys(time: Self::Time, keys: Vec<(Self::Key, Self::R)>) -> Self
    where
        Self::Val: From<()>,
    {
        Rc::new(B::from_keys(time, keys))
    }

    fn begin_merge(&self, other: &Self) -> Self::Merger {
        RcMerger::new(B::begin_merge(self, other))
    }

    fn merge(&self, other: &Self) -> Self {
        Rc::new(B::merge(self, other))
    }

    fn empty(time: Self::Time) -> Self {
        Rc::new(B::empty(time))
    }

    fn recede_to(&mut self, frontier: &B::Time) {
        Rc::get_mut(self).unwrap().recede_to(frontier);
    }
}

/// Wrapper type for batching reference counted batches.
#[derive(SizeOf)]
pub struct RcBatcher<B>
where
    B: Batch,
{
    batcher: B::Batcher,
}

/// Functionality for collecting and batching updates.
impl<B> Batcher<B::Item, B::Time, B::R, Rc<B>> for RcBatcher<B>
where
    B: Batch,
{
    fn new_batcher(time: B::Time) -> Self {
        Self {
            batcher: B::Batcher::new_batcher(time),
        }
    }

    fn push_batch(&mut self, batch: &mut Vec<(B::Item, B::R)>) {
        self.batcher.push_batch(batch)
    }

    fn push_consolidated_batch(&mut self, batch: &mut Vec<(B::Item, B::R)>) {
        self.batcher.push_consolidated_batch(batch)
    }

    fn tuples(&self) -> usize {
        self.batcher.tuples()
    }

    fn seal(self) -> Rc<B> {
        Rc::new(self.batcher.seal())
    }
}

/// Wrapper type for building reference counted batches.
#[derive(SizeOf)]
pub struct RcBuilder<B>
where
    B: Batch,
{
    builder: B::Builder,
}

/// Functionality for building batches from ordered update sequences.
impl<B> Builder<B::Item, B::Time, B::R, Rc<B>> for RcBuilder<B>
where
    B: Batch,
{
    fn new_builder(time: B::Time) -> Self {
        Self {
            builder: B::Builder::new_builder(time),
        }
    }

    fn with_capacity(time: B::Time, cap: usize) -> Self {
        Self {
            builder: <B::Builder as Builder<B::Item, B::Time, B::R, B>>::with_capacity(time, cap),
        }
    }

    fn push(&mut self, element: (B::Item, B::R)) {
        self.builder.push(element)
    }

    fn reserve(&mut self, additional: usize) {
        self.builder.reserve(additional);
    }

    fn extend<It>(&mut self, iter: It)
    where
        It: Iterator<Item = (B::Item, B::R)>,
    {
        self.builder.extend(iter);
    }

    fn done(self) -> Rc<B> {
        Rc::new(self.builder.done())
    }
}

/// Wrapper type for merging reference counted batches.
#[derive(SizeOf)]
pub struct RcMerger<B>
where
    B: Batch,
{
    merger: B::Merger,
}

impl<B> RcMerger<B>
where
    B: Batch,
{
    const fn new(merger: B::Merger) -> Self {
        Self { merger }
    }
}

/// Represents a merge in progress.
impl<B> Merger<B::Key, B::Val, B::Time, B::R, Rc<B>> for RcMerger<B>
where
    B: Batch,
{
    fn new_merger(source1: &Rc<B>, source2: &Rc<B>) -> Self {
        Self {
            merger: B::begin_merge(source1, source2),
        }
    }

    fn work(&mut self, source1: &Rc<B>, source2: &Rc<B>, fuel: &mut isize) {
        self.merger.work(source1, source2, fuel)
    }

    fn done(self) -> Rc<B> {
        Rc::new(self.merger.done())
    }
}

impl<B> Debug for RcMerger<B>
where
    B: Batch,
    B::Merger: Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RcMerger")
            .field("merger", &self.merger)
            .finish()
    }
}
