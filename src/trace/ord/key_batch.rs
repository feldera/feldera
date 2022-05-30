use std::{
    convert::{TryFrom, TryInto},
    fmt::Debug,
    marker::PhantomData,
};

use timely::progress::Antichain;

use crate::{
    algebra::MonoidValue,
    lattice::Lattice,
    trace::{
        description::Description,
        layers::{
            ordered::{OrdOffset, OrderedBuilder, OrderedCursor, OrderedLayer},
            ordered_leaf::{OrderedLeaf, OrderedLeafBuilder},
            Builder as TrieBuilder, Cursor as TrieCursor, MergeBuilder, Trie, TupleBuilder,
        },
        ord::merge_batcher::MergeBatcher,
        Batch, BatchReader, Builder, Cursor, Merger,
    },
    Timestamp,
};

use deepsize::DeepSizeOf;

/// An immutable collection of update tuples, from a contiguous interval of
/// logical times.
#[derive(Debug)]
pub struct OrdKeyBatch<K, T, R, O = usize>
where
    K: Ord,
    T: Lattice,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    /// Where all the dataz is.
    pub layer: OrderedLayer<K, OrderedLeaf<T, R>, O>,
    /// Description of the update times this layer represents.
    pub desc: Description<T>,
}

impl<K, T, R, O> DeepSizeOf for OrdKeyBatch<K, T, R, O>
where
    K: DeepSizeOf + Ord,
    T: DeepSizeOf + Lattice,
    R: DeepSizeOf,
    O: DeepSizeOf + OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    fn deep_size_of_children(&self, _context: &mut deepsize::Context) -> usize {
        self.layer.deep_size_of()
    }
}

impl<K, T, R, O> BatchReader for OrdKeyBatch<K, T, R, O>
where
    K: Ord + Clone + 'static,
    T: Timestamp + Lattice,
    R: MonoidValue,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    type Key = K;
    type Val = ();
    type Time = T;
    type R = R;
    type Cursor = OrdKeyCursor<T, R, O>;

    fn cursor(&self) -> Self::Cursor {
        OrdKeyCursor {
            empty: (),
            valid: true,
            cursor: self.layer.cursor(),
            phantom: PhantomData,
        }
    }
    fn len(&self) -> usize {
        <OrderedLayer<K, OrderedLeaf<T, R>, O> as Trie>::tuples(&self.layer)
    }
    fn description(&self) -> &Description<T> {
        &self.desc
    }
}

impl<K, T, R, O> Batch for OrdKeyBatch<K, T, R, O>
where
    K: Ord + Clone + 'static,
    T: Lattice + Timestamp + Ord + Clone + 'static,
    R: MonoidValue,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    type Batcher = MergeBatcher<K, (), T, R, Self>;
    type Builder = OrdKeyBuilder<K, T, R, O>;
    type Merger = OrdKeyMerger<K, T, R, O>;

    fn begin_merge(&self, other: &Self) -> Self::Merger {
        OrdKeyMerger::new(self, other)
    }

    fn recede_to(&mut self, frontier: &T) {
        // Nothing to do if the batch is entirely before the frontier.
        if !self.upper().less_equal(frontier) {
            // TODO: Optimize case where self.upper()==self.lower().
            self.do_recede_to(frontier);
        }
    }
}

impl<K, T, R, O> OrdKeyBatch<K, T, R, O>
where
    K: Ord + Clone + 'static,
    T: Lattice + Ord + Clone + 'static,
    R: MonoidValue,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    fn do_recede_to(&mut self, frontier: &T) {
        // We will zip through the time leaves, calling advance on each,
        //    then zip through the value layer, sorting and collapsing each,
        //    then zip through the key layer, collapsing each .. ?

        // 1. For each (time, diff) pair, advance the time.
        for i in 0..self.layer.vals.vals.len() {
            self.layer.vals.vals[i].0.meet_assign(frontier);
        }
        // for time_diff in self.layer.vals.vals.iter_mut() {
        //     time_diff.0 = time_diff.0.advance_by(frontier);
        // }

        // 2. For each `(val, off)` pair, sort the range, compact, and rewrite `off`.
        //    This may leave `val` with an empty range; filtering happens in step 3.
        let mut write_position = 0;
        for i in 0..self.layer.keys.len() {
            // NB: batch.layer.vals.offs[i+1] will be used next iteration, and should not be
            // changed.     we will change batch.layer.vals.offs[i] in this
            // iteration, from `write_position`'s     initial value.

            let lower: usize = self.layer.offs[i].try_into().unwrap();
            let upper: usize = self.layer.offs[i + 1].try_into().unwrap();

            self.layer.offs[i] = O::try_from(write_position).unwrap();

            let updates = &mut self.layer.vals.vals[..];

            // sort the range by the times (ignore the diffs; they will collapse).
            let count = crate::trace::consolidation::consolidate_slice(&mut updates[lower..upper]);

            for index in lower..(lower + count) {
                updates.swap(write_position, index);
                write_position += 1;
            }
        }
        self.layer.vals.vals.truncate(write_position);
        self.layer.offs[self.layer.keys.len()] = O::try_from(write_position).unwrap();

        // 4. Remove empty keys.
        let mut write_position = 0;
        for i in 0..self.layer.keys.len() {
            let lower: usize = self.layer.offs[i].try_into().unwrap();
            let upper: usize = self.layer.offs[i + 1].try_into().unwrap();

            if lower < upper {
                self.layer.keys.swap(write_position, i);
                // batch.layer.offs updated via `dedup` below; keeps me sane.
                write_position += 1;
            }
        }
        self.layer.offs.dedup();
        self.layer.keys.truncate(write_position);
        self.layer.offs.truncate(write_position + 1);
    }
}

/// State for an in-progress merge.
pub struct OrdKeyMerger<K, T, R, O = usize>
where
    K: Ord + Clone + 'static,
    T: Lattice + Ord + Clone + 'static,
    R: MonoidValue,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    // first batch, and position therein.
    lower1: usize,
    upper1: usize,
    // second batch, and position therein.
    lower2: usize,
    upper2: usize,
    // result that we are currently assembling.
    result: <OrderedLayer<K, OrderedLeaf<T, R>, O> as Trie>::MergeBuilder,
    description: Description<T>,
}

impl<K, T, R, O> Merger<K, (), T, R, OrdKeyBatch<K, T, R, O>> for OrdKeyMerger<K, T, R, O>
where
    K: Ord + Clone + 'static,
    T: Lattice + Timestamp + Ord + Clone + 'static,
    R: MonoidValue,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    fn new(batch1: &OrdKeyBatch<K, T, R, O>, batch2: &OrdKeyBatch<K, T, R, O>) -> Self {
        // Leonid: we do not require batch bounds to grow monotonically.
        //assert!(batch1.upper() == batch2.lower());

        // Leonid: we do not require batch bounds to grow monotonically.
        let description = Description::new(
            batch1.lower().meet(batch2.lower()),
            batch2.upper().join(batch2.upper()),
        );

        OrdKeyMerger {
            lower1: 0,
            upper1: batch1.layer.keys(),
            lower2: 0,
            upper2: batch2.layer.keys(),
            result: <<OrderedLayer<K, OrderedLeaf<T, R>, O> as Trie>::MergeBuilder as MergeBuilder>::with_capacity(&batch1.layer, &batch2.layer),
            description,
        }
    }
    fn done(self) -> OrdKeyBatch<K, T, R, O> {
        assert!(self.lower1 == self.upper1);
        assert!(self.lower2 == self.upper2);

        OrdKeyBatch {
            layer: self.result.done(),
            desc: self.description,
        }
    }
    fn work(
        &mut self,
        source1: &OrdKeyBatch<K, T, R, O>,
        source2: &OrdKeyBatch<K, T, R, O>,
        fuel: &mut isize,
    ) {
        let starting_updates = self.result.vals.vals.len();
        let mut effort = 0isize;

        // while both mergees are still active
        while self.lower1 < self.upper1 && self.lower2 < self.upper2 && effort < *fuel {
            self.result.merge_step(
                (&source1.layer, &mut self.lower1, self.upper1),
                (&source2.layer, &mut self.lower2, self.upper2),
            );
            effort = (self.result.vals.vals.len() - starting_updates) as isize;
        }

        // if self.lower1 == self.upper1 || self.lower2 == self.upper2 {
        //     // these are just copies, so let's bite the bullet and just do them.
        //     if self.lower1 < self.upper1 { self.result.copy_range(&source1.layer,
        // self.lower1, self.upper1); self.lower1 = self.upper1; }     if self.
        // lower2 < self.upper2 { self.result.copy_range(&source2.layer, self.lower2,
        // self.upper2); self.lower2 = self.upper2; } }
        // Merging is complete; only copying remains. Copying is probably faster than
        // merging, so could take some liberties here.
        if self.lower1 == self.upper1 || self.lower2 == self.upper2 {
            // Limit merging by remaining fuel.
            let remaining_fuel = *fuel - effort;
            if remaining_fuel > 0 {
                if self.lower1 < self.upper1 {
                    let mut to_copy = remaining_fuel as usize;
                    if to_copy < 1_000 {
                        to_copy = 1_000;
                    }
                    if to_copy > (self.upper1 - self.lower1) {
                        to_copy = self.upper1 - self.lower1;
                    }
                    self.result
                        .copy_range(&source1.layer, self.lower1, self.lower1 + to_copy);
                    self.lower1 += to_copy;
                }
                if self.lower2 < self.upper2 {
                    let mut to_copy = remaining_fuel as usize;
                    if to_copy < 1_000 {
                        to_copy = 1_000;
                    }
                    if to_copy > (self.upper2 - self.lower2) {
                        to_copy = self.upper2 - self.lower2;
                    }
                    self.result
                        .copy_range(&source2.layer, self.lower2, self.lower2 + to_copy);
                    self.lower2 += to_copy;
                }
            }
        }

        effort = (self.result.vals.vals.len() - starting_updates) as isize;

        *fuel -= effort;

        // if *fuel < -1_000_000 {
        //     eprintln!("Massive deficit OrdKey::work: {}", fuel);
        // }
    }
}

/// A cursor for navigating a single layer.
#[derive(Debug)]
pub struct OrdKeyCursor<T: Lattice + Ord + Clone, R: MonoidValue, O = usize> {
    valid: bool,
    empty: (),
    cursor: OrderedCursor<OrderedLeaf<T, R>>,
    phantom: PhantomData<O>,
}

impl<K, T, R, O> Cursor<K, (), T, R> for OrdKeyCursor<T, R, O>
where
    K: Ord + Clone,
    T: Lattice + Ord + Clone,
    R: MonoidValue,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    type Storage = OrdKeyBatch<K, T, R, O>;

    fn key<'a>(&self, storage: &'a Self::Storage) -> &'a K {
        self.cursor.key(&storage.layer)
    }
    fn val<'a>(&self, _storage: &'a Self::Storage) -> &'a () {
        unsafe { ::std::mem::transmute(&self.empty) }
    }
    fn map_times<L: FnMut(&T, &R)>(&mut self, storage: &Self::Storage, mut logic: L) {
        self.cursor.child.rewind(&storage.layer.vals);
        while self.cursor.child.valid(&storage.layer.vals) {
            logic(
                &self.cursor.child.key(&storage.layer.vals).0,
                &self.cursor.child.key(&storage.layer.vals).1,
            );
            self.cursor.child.step(&storage.layer.vals);
        }
    }
    fn weight<'a>(&self, storage: &'a Self::Storage) -> &'a R
    where
        T: PartialEq<()>,
    {
        debug_assert!(self.cursor.child.valid(&storage.layer.vals));
        &self.cursor.child.key(&storage.layer.vals).1
    }
    fn key_valid(&self, storage: &Self::Storage) -> bool {
        self.cursor.valid(&storage.layer)
    }
    fn val_valid(&self, _storage: &Self::Storage) -> bool {
        self.valid
    }
    fn step_key(&mut self, storage: &Self::Storage) {
        self.cursor.step(&storage.layer);
        self.valid = true;
    }
    fn seek_key(&mut self, storage: &Self::Storage, key: &K) {
        self.cursor.seek(&storage.layer, key);
        self.valid = true;
    }
    fn step_val(&mut self, _storage: &Self::Storage) {
        self.valid = false;
    }
    fn seek_val(&mut self, _storage: &Self::Storage, _val: &()) {}
    fn rewind_keys(&mut self, storage: &Self::Storage) {
        self.cursor.rewind(&storage.layer);
        self.valid = true;
    }
    fn rewind_vals(&mut self, _storage: &Self::Storage) {
        self.valid = true;
    }
}

/// A builder for creating layers from unsorted update tuples.
pub struct OrdKeyBuilder<K, T, R, O = usize>
where
    K: Ord,
    T: Ord + Lattice,
    R: MonoidValue,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    time: T,
    builder: OrderedBuilder<K, OrderedLeafBuilder<T, R>, O>,
}

impl<K, T, R, O> Builder<K, (), T, R, OrdKeyBatch<K, T, R, O>> for OrdKeyBuilder<K, T, R, O>
where
    K: Ord + Clone + 'static,
    T: Lattice + Timestamp + Ord + Clone + 'static,
    R: MonoidValue,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    fn new(time: T) -> Self {
        OrdKeyBuilder {
            time,
            builder: OrderedBuilder::<K, OrderedLeafBuilder<T, R>, O>::new(),
        }
    }

    fn with_capacity(time: T, cap: usize) -> Self {
        OrdKeyBuilder {
            time,
            builder:
                <OrderedBuilder<K, OrderedLeafBuilder<T, R>, O> as TupleBuilder>::with_capacity(cap),
        }
    }

    #[inline]
    fn push(&mut self, (key, _, diff): (K, (), R)) {
        self.builder.push_tuple((key, (self.time.clone(), diff)));
    }

    #[inline(never)]
    fn done(self) -> OrdKeyBatch<K, T, R, O> {
        let time_next = self.time.advance(0);
        let upper = if time_next <= self.time {
            Antichain::new()
        } else {
            Antichain::from_elem(time_next)
        };
        OrdKeyBatch {
            layer: self.builder.done(),
            desc: Description::new(Antichain::from_elem(self.time), upper),
        }
    }
}
