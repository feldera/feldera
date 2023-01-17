use dbsp::{
    time::AntichainRef,
    trace::{
        layers::{erased::ErasedLayer, MergeBuilder, Trie},
        Batch, BatchReader, Builder, Consumer, Merger, ValueConsumer,
    },
};
use size_of::SizeOf;
use std::cmp::max;

/// An immutable collection of `(key, weight)` pairs without timing information
#[derive(Debug, Clone, Eq, PartialEq, SizeOf)]
pub struct ErasedZSet {
    pub layer: ErasedLayer,
}

/*
impl BatchReader for ErasedZSet {
    type Key = *const u8;
    type Val = ();
    type Time = ();
    type R = *const u8;
    type Cursor<'s> = ErasedZSetCursor<'s>;
    type Consumer = ErasedZSetConsumer;

    #[inline]
    fn cursor(&self) -> Self::Cursor<'_> {
        ErasedZSetCursor {
            valid: true,
            cursor: self.layer.cursor(),
        }
    }

    #[inline]
    fn consumer(self) -> Self::Consumer {
        ErasedZSetConsumer {
            consumer: TypedLayerConsumer::from(self.layer),
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
}

impl Batch for ErasedZSet {
    type Item = K;
    type Batcher = MergeBatcher<K, (), R, Self>;
    type Builder = ErasedZSetBuilder<K, R>;
    type Merger = ErasedZSetMerger<K, R>;

    fn item_from(key: K, _val: ()) -> Self::Item {
        key
    }

    fn from_keys(time: Self::Time, keys: Vec<(Self::Key, Self::R)>) -> Self {
        Self::from_tuples(time, keys)
    }

    fn begin_merge(&self, other: &Self) -> Self::Merger {
        ErasedZSetMerger::new_merger(self, other)
    }

    fn recede_to(&mut self, _frontier: &()) {}

    fn empty(_time: Self::Time) -> Self {
        Self {
            layer: ErasedLayer::empty(),
        }
    }
}

/// State for an in-progress merge.
#[derive(SizeOf)]
pub struct ErasedZSetMerger {
    // result that we are currently assembling.
    result: <ErasedLayer as Trie>::MergeBuilder,
}

impl Merger<K, (), (), R, ErasedZSet> for ErasedZSetMerger
where
    Self: SizeOf,
{
    fn new_merger(batch1: &ErasedZSet, batch2: &ErasedZSet) -> Self {
        Self {
            result: <<ErasedLayer as Trie>::MergeBuilder as MergeBuilder>::with_capacity(
                &batch1.layer,
                &batch2.layer,
            ),
        }
    }

    fn done(self) -> ErasedZSet {
        ErasedZSet {
            layer: self.result.done(),
        }
    }

    fn work(&mut self, source1: &ErasedZSet, source2: &ErasedZSet, fuel: &mut isize) {
        *fuel -= self
            .result
            .push_merge(source1.layer.cursor(), source2.layer.cursor()) as isize;
        *fuel = max(*fuel, 1);
    }
}

/// A cursor for navigating a single layer.
#[derive(Debug, SizeOf)]
pub struct ErasedZSetCursor<'s> {
    valid: bool,
    cursor: ColumnLayerCursor<'s>,
}

impl<'s> Cursor<'s, K, (), (), R> for ErasedZSetCursor<'s> {
    fn key(&self) -> &K {
        self.cursor.current_key()
    }

    fn val(&self) -> &() {
        &()
    }

    fn fold_times<F, U>(&mut self, init: U, mut fold: F) -> U
    where
        F: FnMut(U, &(), &R) -> U,
    {
        if self.cursor.valid() {
            fold(init, &(), self.cursor.current_diff())
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
        debug_assert!(&self.cursor.valid());
        self.cursor.current_diff().clone()
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

    fn seek_key(&mut self, key: &K) {
        self.cursor.seek_key(key);
        self.valid = true;
    }

    fn last_key(&mut self) -> Option<&K> {
        self.cursor.last_key().map(|(k, _)| k)
    }

    fn step_val(&mut self) {
        self.valid = false;
    }

    fn seek_val(&mut self, _val: &()) {}

    fn seek_val_with<P>(&mut self, predicate: P)
    where
        P: Fn(&()) -> bool + Clone,
    {
        if !predicate(&()) {
            self.valid = false;
        }
    }

    fn rewind_keys(&mut self) {
        self.cursor.rewind();
        self.valid = true;
    }

    fn rewind_vals(&mut self) {
        self.valid = true;
    }
}

/// A builder for creating layers from unsorted update tuples.
#[derive(SizeOf)]
pub struct ErasedZSetBuilder<K, R>
where
    K: Ord,
    R: DBWeight,
{
    builder: ColumnLayerBuilder<K, R>,
}

impl<K, R> Builder<K, (), R, ErasedZSet> for ErasedZSetBuilder<K, R>
where
    Self: SizeOf,
    K: DBData,
    R: DBWeight,
{
    #[inline]
    fn new_builder(_time: ()) -> Self {
        Self {
            builder: ColumnLayerBuilder::new(),
        }
    }

    #[inline]
    fn with_capacity(_time: (), capacity: usize) -> Self {
        Self {
            builder: <ColumnLayerBuilder<K, R> as TupleBuilder>::with_capacity(capacity),
        }
    }

    #[inline]
    fn reserve(&mut self, additional: usize) {
        self.builder.reserve(additional);
    }

    #[inline]
    fn push(&mut self, (key, diff): (K, R)) {
        self.builder.push_tuple((key, diff));
    }

    #[inline(never)]
    fn done(self) -> ErasedZSet {
        ErasedZSet {
            layer: self.builder.done(),
        }
    }
}

#[derive(Debug, SizeOf)]
pub struct ErasedZSetConsumer<K, R> {
    consumer: ColumnLayerConsumer<K, R>,
}

impl<K, R> Consumer<K, (), R, ()> for ErasedZSetConsumer<K, R> {
    type ValueConsumer<'a> = ErasedZSetValueConsumer<'a, K, R>
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
        (key, ErasedZSetValueConsumer { values })
    }

    fn seek_key(&mut self, key: &K)
    where
        K: Ord,
    {
        self.consumer.seek_key(key);
    }
}

#[derive(Debug)]
pub struct ErasedZSetValueConsumer<'a, K, R> {
    values: ColumnLayerValues<'a, K, R>,
}

impl<'a, K, R> ValueConsumer<'a, (), R, ()> for ErasedZSetValueConsumer<'a, K, R> {
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
