use crate::{
    algebra::{AddAssignByRef, HasZero, MonoidValue},
    lattice::Lattice,
    trace::{
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
use std::{
    convert::{TryFrom, TryInto},
    fmt::{Debug, Display, Formatter},
};
use timely::progress::Antichain;

pub type OrdValBatchLayer<K, V, T, R, O> =
    OrderedLayer<K, OrderedLayer<V, OrderedLeaf<T, R>, O>, O>;

/// An immutable collection of update tuples, from a contiguous interval of
/// logical times.
#[derive(Debug)]
pub struct OrdValBatch<K, V, T, R, O = usize>
where
    K: Ord,
    V: Ord,
    T: Lattice,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    /// Where all the dataz is.
    pub layer: OrdValBatchLayer<K, V, T, R, O>,
    pub lower: Antichain<T>,
    pub upper: Antichain<T>,
}

impl<K, V, T, R, O> Display for OrdValBatch<K, V, T, R, O>
where
    K: Ord + Clone + Display,
    V: Ord + Clone + Display + 'static,
    T: Lattice + Clone + Ord + Display + Debug + 'static,
    R: Eq + HasZero + AddAssignByRef + Clone + Display + 'static,
    O: OrdOffset + 'static,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        writeln!(
            f,
            "lower: {:?}, upper: {:?}\nlayer:\n{}",
            self.lower,
            self.upper,
            textwrap::indent(&self.layer.to_string(), "    ")
        )
    }
}

impl<K, V, T, R, O> DeepSizeOf for OrdValBatch<K, V, T, R, O>
where
    K: DeepSizeOf + Ord,
    V: DeepSizeOf + Ord,
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

impl<K, V, T, R, O> BatchReader for OrdValBatch<K, V, T, R, O>
where
    K: Ord + Clone + 'static,
    V: Ord + Clone + 'static,
    T: Timestamp + Lattice,
    R: MonoidValue,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    type Key = K;
    type Val = V;
    type Time = T;
    type R = R;

    type Cursor<'s> = OrdValCursor<'s, K, V, T, R, O> where O: 's;
    fn cursor(&self) -> Self::Cursor<'_> {
        OrdValCursor {
            cursor: self.layer.cursor(),
        }
    }
    fn len(&self) -> usize {
        <OrdValBatchLayer<K, V, T, R, O> as Trie>::tuples(&self.layer)
    }
    fn lower(&self) -> &Antichain<T> {
        &self.lower
    }
    fn upper(&self) -> &Antichain<T> {
        &self.upper
    }
}

impl<K, V, T, R, O> Batch for OrdValBatch<K, V, T, R, O>
where
    K: Ord + Clone + 'static,
    V: Ord + Clone + 'static,
    T: Lattice + Timestamp + Ord + Clone + ::std::fmt::Debug + 'static,
    R: MonoidValue,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    type Batcher = MergeBatcher<K, V, T, R, Self>;
    type Builder = OrdValBuilder<K, V, T, R, O>;
    type Merger = OrdValMerger<K, V, T, R, O>;

    fn begin_merge(&self, other: &Self) -> Self::Merger {
        OrdValMerger::new(self, other)
    }

    fn recede_to(&mut self, frontier: &T) {
        // Nothing to do if the batch is entirely before the frontier.
        if !self.upper().less_equal(frontier) {
            self.do_recede_to(frontier);
        }
    }
}

impl<K, V, T, R, O> OrdValBatch<K, V, T, R, O>
where
    K: Ord + Clone + 'static,
    V: Ord + Clone + 'static,
    T: Lattice + Ord + Clone + ::std::fmt::Debug + 'static,
    R: MonoidValue,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    fn do_recede_to(&mut self, frontier: &T) {
        // We have unique ownership of the batch, and can advance times in place.
        // We must still sort, collapse, and remove empty updates.

        // We will zip throught the time leaves, calling advance on each,
        //    then zip through the value layer, sorting and collapsing each,
        //    then zip through the key layer, collapsing each .. ?

        // 1. For each (time, diff) pair, advance the time.
        for i in 0..self.layer.vals.vals.vals.len() {
            self.layer.vals.vals.vals[i].0.meet_assign(frontier);
        }

        // 2. For each `(val, off)` pair, sort the range, compact, and rewrite `off`.
        //    This may leave `val` with an empty range; filtering happens in step 3.
        let mut write_position = 0;
        for i in 0..self.layer.vals.keys.len() {
            // NB: batch.layer.vals.offs[i+1] will be used next iteration, and should not be
            // changed.     we will change batch.layer.vals.offs[i] in this
            // iteration, from `write_position`'s     initial value.

            let lower: usize = self.layer.vals.offs[i].try_into().unwrap();
            let upper: usize = self.layer.vals.offs[i + 1].try_into().unwrap();

            self.layer.vals.offs[i] = O::try_from(write_position).unwrap();

            let updates = &mut self.layer.vals.vals.vals[..];

            // sort the range by the times (ignore the diffs; they will collapse).
            let count = crate::trace::consolidation::consolidate_slice(&mut updates[lower..upper]);

            for index in lower..(lower + count) {
                updates.swap(write_position, index);
                write_position += 1;
            }
        }
        self.layer.vals.vals.vals.truncate(write_position);
        self.layer.vals.offs[self.layer.vals.keys.len()] = O::try_from(write_position).unwrap();

        // 3. For each `(key, off)` pair, (values already sorted), filter vals, and
        // rewrite `off`.    This may leave `key` with an empty range. Filtering
        // happens in step 4.
        let mut write_position = 0;
        for i in 0..self.layer.keys.len() {
            // NB: batch.layer.offs[i+1] must remain as is for the next iteration.
            //     instead, we update batch.layer.offs[i]

            let lower: usize = self.layer.offs[i].try_into().unwrap();
            let upper: usize = self.layer.offs[i + 1].try_into().unwrap();

            self.layer.offs[i] = O::try_from(write_position).unwrap();

            // values should already be sorted, but some might now be empty.
            for index in lower..upper {
                let val_lower: usize = self.layer.vals.offs[index].try_into().unwrap();
                let val_upper: usize = self.layer.vals.offs[index + 1].try_into().unwrap();
                if val_lower < val_upper {
                    self.layer.vals.keys.swap(write_position, index);
                    self.layer.vals.offs[write_position + 1] = self.layer.vals.offs[index + 1];
                    write_position += 1;
                }
            }
            // batch.layer.offs[i+1] = write_position;
        }
        self.layer.vals.keys.truncate(write_position);
        self.layer.vals.offs.truncate(write_position + 1);
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
pub struct OrdValMerger<K, V, T, R, O = usize>
where
    K: Ord + Clone + 'static,
    V: Ord + Clone + 'static,
    T: Lattice + Ord + Clone + Debug + 'static,
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
    result: <OrdValBatchLayer<K, V, T, R, O> as Trie>::MergeBuilder,
    lower: Antichain<T>,
    upper: Antichain<T>,
}

impl<K, V, T, R, O> Merger<K, V, T, R, OrdValBatch<K, V, T, R, O>> for OrdValMerger<K, V, T, R, O>
where
    K: Ord + Clone + 'static,
    V: Ord + Clone + 'static,
    T: Lattice + Timestamp + Ord + Clone + Debug + 'static,
    R: MonoidValue,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    fn new(batch1: &OrdValBatch<K, V, T, R, O>, batch2: &OrdValBatch<K, V, T, R, O>) -> Self {
        // Leonid: we do not require batch bounds to grow monotonically.
        // assert!(batch1.upper() == batch2.lower());

        OrdValMerger {
            lower1: 0,
            upper1: batch1.layer.keys(),
            lower2: 0,
            upper2: batch2.layer.keys(),
            result: <<OrdValBatchLayer<K, V, T, R, O> as Trie>::MergeBuilder as MergeBuilder>::with_capacity(&batch1.layer, &batch2.layer),
            lower: batch1.lower().meet(batch2.lower()),
            upper: batch1.upper().join(batch2.upper()),
        }
    }
    fn done(self) -> OrdValBatch<K, V, T, R, O> {
        assert!(self.lower1 == self.upper1);
        assert!(self.lower2 == self.upper2);

        OrdValBatch {
            layer: self.result.done(),
            lower: self.lower,
            upper: self.upper,
        }
    }
    fn work(
        &mut self,
        source1: &OrdValBatch<K, V, T, R, O>,
        source2: &OrdValBatch<K, V, T, R, O>,
        fuel: &mut isize,
    ) {
        let starting_updates = self.result.vals.vals.vals.len();
        let mut effort = 0isize;

        // while both mergees are still active
        while self.lower1 < self.upper1 && self.lower2 < self.upper2 && effort < *fuel {
            self.result.merge_step(
                (&source1.layer, &mut self.lower1, self.upper1),
                (&source2.layer, &mut self.lower2, self.upper2),
            );
            effort = (self.result.vals.vals.vals.len() - starting_updates) as isize;
        }

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

        effort = (self.result.vals.vals.vals.len() - starting_updates) as isize;

        *fuel -= effort;

        // if *fuel < -1_000_000 {
        //     eprintln!("Massive deficit OrdVal::work: {}", fuel);
        // }
    }
}

/// A cursor for navigating a single layer.
#[derive(Debug)]
pub struct OrdValCursor<'s, K, V, T, R, O = usize>
where
    K: Ord + Clone,
    V: Ord + Clone,
    T: Lattice + Ord + Clone,
    R: MonoidValue,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    cursor: OrderedCursor<'s, K, O, OrderedLayer<V, OrderedLeaf<T, R>, O>>,
}

impl<'s, K, V, T, R, O> Cursor<'s, K, V, T, R> for OrdValCursor<'s, K, V, T, R, O>
where
    K: Ord + Clone,
    V: Ord + Clone,
    T: Lattice + Ord + Clone,
    R: MonoidValue,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    type Storage = OrdValBatch<K, V, T, R, O>;

    fn key(&self) -> &K {
        self.cursor.key()
    }
    fn val(&self) -> &V {
        self.cursor.child.key()
    }
    fn map_times<L: FnMut(&T, &R)>(&mut self, mut logic: L) {
        self.cursor.child.child.rewind();
        while self.cursor.child.child.valid() {
            logic(
                &self.cursor.child.child.key().0,
                &self.cursor.child.child.key().1,
            );
            self.cursor.child.child.step();
        }
    }
    fn weight(&mut self) -> R
    where
        T: PartialEq<()>,
    {
        debug_assert!(self.cursor.child.child.valid());
        self.cursor.child.child.key().1.clone()
    }

    fn key_valid(&self) -> bool {
        self.cursor.valid()
    }
    fn val_valid(&self) -> bool {
        self.cursor.child.valid()
    }
    fn step_key(&mut self) {
        self.cursor.step();
    }
    fn seek_key(&mut self, key: &K) {
        self.cursor.seek(key);
    }
    fn step_val(&mut self) {
        self.cursor.child.step();
    }
    fn seek_val(&mut self, val: &V) {
        self.cursor.child.seek(val);
    }

    fn values<'a>(&mut self, _vals: &mut Vec<(&'a V, R)>)
    where
        's: 'a,
    {
        // We currently don't use this method on timed batches.
        // When we do, the API is likely to change, so just
        // leave it unimplemented for now.
        unimplemented!()
    }

    fn rewind_keys(&mut self) {
        self.cursor.rewind();
    }
    fn rewind_vals(&mut self) {
        self.cursor.child.rewind();
    }
}

/// A builder for creating layers from unsorted update tuples.
pub struct OrdValBuilder<K, V, T, R, O = usize>
where
    K: Ord,
    V: Ord,
    T: Ord + Lattice,
    R: MonoidValue,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    time: T,
    builder: OrderedBuilder<K, OrderedBuilder<V, OrderedLeafBuilder<T, R>, O>, O>,
}

impl<K, V, T, R, O> Builder<K, V, T, R, OrdValBatch<K, V, T, R, O>> for OrdValBuilder<K, V, T, R, O>
where
    K: Ord + Clone + 'static,
    V: Ord + Clone + 'static,
    T: Lattice + Timestamp + Ord + Clone + ::std::fmt::Debug + 'static,
    R: MonoidValue,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    fn new(time: T) -> Self {
        OrdValBuilder {
            time,
            builder: OrderedBuilder::<K, OrderedBuilder<V, OrderedLeafBuilder<T, R>, O>, O>::new(),
        }
    }
    fn with_capacity(time: T, cap: usize) -> Self {
        OrdValBuilder {
            time,
            builder: <OrderedBuilder<K, OrderedBuilder<V, OrderedLeafBuilder<T, R>, O>, O> as TupleBuilder>::with_capacity(cap)
        }
    }

    #[inline]
    fn push(&mut self, (key, val, diff): (K, V, R)) {
        self.builder
            .push_tuple((key, (val, (self.time.clone(), diff))));
    }

    #[inline(never)]
    fn done(self) -> OrdValBatch<K, V, T, R, O> {
        let time_next = self.time.advance(0);
        let upper = if time_next <= self.time {
            Antichain::new()
        } else {
            Antichain::from_elem(time_next)
        };
        OrdValBatch {
            layer: self.builder.done(),
            lower: Antichain::from_elem(self.time),
            upper,
        }
    }
}
