//! Traits and datastructures representing a collection trace.
//!
//! A collection trace is a set of updates of the form `(key, val, time, diff)`,
//! which determine the contents of a collection at given times by accumulating
//! updates whose time field is less or equal to the target field.
//!
//! The `Trace` trait describes those types and methods that a data structure
//! must implement to be viewed as a collection trace. This trait allows
//! operator implementations to be generic with respect to the type of trace,
//! and allows various data structures to be interpretable as multiple different
//! types of trace.

pub mod consolidation;
pub mod cursor;
pub mod description;
pub mod layers;
pub mod ord;
pub mod spine_fueled;

use crate::{algebra::MonoidValue, lattice::Lattice, time::Timestamp};
use timely::progress::Antichain;

pub use cursor::Cursor;
pub use description::Description;

/// A trace whose contents may be read.
///
/// This is a restricted interface to the more general `Trace` trait, which
/// extends this trait with further methods to update the contents of the trace.
/// These methods are used to examine the contents, and to update the reader's
/// capabilities (which may release restrictions on the mutations to the
/// underlying trace and cause work to happen).
pub trait TraceReader {
    /// Key by which updates are indexed.
    type Key;
    /// Values associated with keys.
    type Val;
    /// Timestamps associated with updates
    type Time: Timestamp + Lattice;
    /// Associated update.
    type R;

    /// The type of an immutable collection of updates.
    type Batch: Batch<Key = Self::Key, Val = Self::Val, Time = Self::Time, R = Self::R>
        + Clone
        + 'static;

    /// The type used to enumerate the collections contents.
    type Cursor: Cursor<Self::Key, Self::Val, Self::Time, Self::R>;

    /// Provides a cursor over updates contained in the trace.
    #[allow(clippy::type_complexity)]
    fn cursor(
        &self,
    ) -> (
        Self::Cursor,
        <Self::Cursor as Cursor<Self::Key, Self::Val, Self::Time, Self::R>>::Storage,
    );

    // TODO: Do we want a version of `cursor` with an upper bound on time?  E.g., it
    // could help in `distinct` to avoid iterating into the future (and then
    // drop future timestamps anyway).
    /*
    /// Acquires a cursor to the restriction of the collection's contents to updates at times not greater or
    /// equal to an element of `upper`.
    ///
    /// This method is expected to work if called with an `upper` that (i) was an observed bound in batches from
    /// the trace, and (ii) the trace has not been advanced beyond `upper`. Practically, the implementation should
    /// be expected to look for a "clean cut" using `upper`, and if it finds such a cut can return a cursor. This
    /// should allow `upper` such as `&[]` as used by `self.cursor()`, though it is difficult to imagine other uses.
    fn cursor_through(&self, upper: AntichainRef<Self::Time>) -> Option<(Self::Cursor, <Self::Cursor as Cursor<Self::Key, Self::Val, Self::Time, Self::R>>::Storage)>;
    */

    /// Maps logic across the non-empty sequence of batches in the trace.
    fn map_batches<F: FnMut(&Self::Batch)>(&self, f: F);
}

/// An append-only collection of `(key, val, time, diff)` tuples.
///
/// The trace must be constructable from, and navigable by the `Key`, `Val`,
/// `Time` types, but does not need to return them.
pub trait Trace: TraceReader {
    /// Allocates a new empty trace.
    fn new(activator: Option<timely::scheduling::activate::Activator>) -> Self;

    /// Push all timestamps in the trace back to `frontier`.
    ///
    /// Modifies all timestamps `t` that are not less than or equal to
    /// `frontier` to `t.meet(frontier)`.  As a result, the trace can no
    /// longer distinguish between timestamps that map to the same value,
    /// but it will contain fewer different timestamps, thus reducing its
    /// memory footprint.
    ///
    /// This also enables us to use fewer bits to represent timestamps.
    /// In DBSP, computations inside a nested circuit only need to distinguish
    /// between updates added during the current run of the nested circuit
    /// vs all previous runs.  Thus, we only need a single bit for the outer
    /// time stamp.  When the nested clock epoch completes, all tuples with
    /// outer timestamp `1` are demoted to `0`, so they appear as old
    /// updates during the next run of the circuit.
    ///
    /// The downside of the 1-bit clock is that it requires rewriting timestamps
    /// and rearranging batches in the trace at the end of every clock epoch.
    /// Unlike merging of batches, which can be done in the backgound, this work
    /// must be completed synchronously before the start of the next epoch.
    /// This cost should be roughly proportional to the number of updates
    /// added to the trace during the last epoch.
    ///
    /// See [`NestedTimestamp32`](`crate::time::NestedTimestamp32`) for an
    /// example of a timestamp type that takes advantage of the 1-bit
    /// timestamp representation.
    fn recede_to(&mut self, frontier: &Self::Time);

    /// Exert merge effort, even without updates.
    fn exert(&mut self, effort: &mut isize);

    /// Merge all updates in a trace into a single batch.
    fn consolidate(self) -> Option<Self::Batch>;

    /// Introduces a batch of updates to the trace.
    ///
    /// Batches describe the time intervals they contain, and they should be
    /// added to the trace in contiguous intervals. If a batch arrives with
    /// a lower bound that does not equal the upper bound of the most recent
    /// addition, the trace will add an empty batch. It is an error to then try
    /// to populate that region of time.
    ///
    /// This restriction could be relaxed, especially if we discover ways in
    /// which batch interval order could commute. For now, the trace should
    /// complain, to the extent that it cares about contiguous intervals.
    fn insert(&mut self, batch: Self::Batch);

    /// Clears the value of the "dirty" flag to `false`.
    ///
    /// The "dirty" flag is used to efficiently track changes to the trace,
    /// e.g., as part of checking whether a circuit has reached a fixed point.
    /// Pushing a non-empty batch to the trace sets the flag to `true`. The
    /// [`Self::dirty`] method returns true iff the trace has changed since the
    /// last call to `clear_dirty_flag`.
    fn clear_dirty_flag(&mut self);

    /// Returns the value of the dirty flag.
    fn dirty(&self) -> bool;
}

/// A batch of updates whose contents may be read.
///
/// This is a restricted interface to batches of updates, which support the
/// reading of the batch's contents, but do not expose ways to construct the
/// batches. This trait is appropriate for views of the batch, and is especially
/// useful for views derived from other sources in ways that prevent the
/// construction of batches from the type of data in the view (for example,
/// filtered views, or views with extended time coordinates).
pub trait BatchReader
where
    Self: ::std::marker::Sized,
{
    type Key;
    type Val;
    type Time: Timestamp + Lattice;
    type R: MonoidValue;

    /// The type used to enumerate the batch's contents.
    type Cursor: Cursor<Self::Key, Self::Val, Self::Time, Self::R, Storage = Self>;
    /// Acquires a cursor to the batch's contents.
    fn cursor(&self) -> Self::Cursor;
    /// The number of updates in the batch.
    fn len(&self) -> usize;
    /// True if the batch is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Describes the times of the updates in the batch.
    fn description(&self) -> &Description<Self::Time>;

    /// All times in the batch are greater or equal to an element of `lower`.
    fn lower(&self) -> &Antichain<Self::Time> {
        self.description().lower()
    }
    /// All times in the batch are not greater or equal to any element of
    /// `upper`.
    fn upper(&self) -> &Antichain<Self::Time> {
        self.description().upper()
    }
}

/// An immutable collection of updates.
pub trait Batch: BatchReader
where
    Self: ::std::marker::Sized,
{
    /// A type used to assemble batches from disordered updates.
    type Batcher: Batcher<Self::Key, Self::Val, Self::Time, Self::R, Self>;

    /// A type used to assemble batches from ordered update sequences.
    type Builder: Builder<Self::Key, Self::Val, Self::Time, Self::R, Self>;

    /// A type used to progressively merge batches.
    type Merger: Merger<Self::Key, Self::Val, Self::Time, Self::R, Self>;

    #[allow(clippy::type_complexity)]
    fn from_tuples(time: Self::Time, mut tuples: Vec<((Self::Key, Self::Val), Self::R)>) -> Self {
        let mut batcher = Self::Batcher::new(time);
        batcher.push_batch(&mut tuples);
        batcher.seal()
    }

    /// Initiates the merging of consecutive batches.
    ///
    /// The result of this method can be exercised to eventually produce the
    /// same result that a call to `self.merge(other)` would produce, but it
    /// can be done in a measured fashion. This can help to avoid latency
    /// spikes where a large merge needs to happen.
    fn begin_merge(&self, other: &Self) -> Self::Merger {
        Self::Merger::new(self, other)
    }

    /// Merges `self` with `other` by running merger to completion.
    fn merge(&self, other: &Self) -> Self {
        let mut fuel = isize::max_value();
        let mut merger = Self::Merger::new(self, other);
        merger.work(self, other, &mut fuel);
        merger.done()
    }

    /// Creates an empty batch with timestamp `time`.
    fn empty(time: Self::Time) -> Self {
        <Self::Builder>::new(time).done()
    }

    /// Push all timestamps in the batch back to `frontier`.
    ///
    /// Modifies all timestamps `t` that are not less than or equal to
    /// `frontier` to `t.meet(frontier)`.  See [`Trace::recede_to`].
    fn recede_to(&mut self, frontier: &Self::Time);
}

/// Functionality for collecting and batching updates.
pub trait Batcher<K, V, T, R, Output: Batch<Key = K, Val = V, Time = T, R = R>> {
    /// Allocates a new empty batcher.  All tuples in the batcher (and its
    /// output batch) will have timestamp `time`.
    fn new(time: T) -> Self;
    /// Adds an unordered batch of elements to the batcher.
    fn push_batch(&mut self, batch: &mut Vec<((K, V), R)>);
    /// Returns the number of tuples in the batcher.
    fn tuples(&self) -> usize;
    /// Returns all updates not greater or equal to an element of `upper`.
    fn seal(self) -> Output;
}

/// Functionality for building batches from ordered update sequences.
pub trait Builder<K, V, T, R, Output: Batch<Key = K, Val = V, Time = T, R = R>> {
    /// Allocates an empty builder.  All tuples in the builder (and its output
    /// batch) will have timestamp `time`.
    fn new(time: T) -> Self;
    /// Allocates an empty builder with some capacity.  All tuples in the
    /// builder (and its output batch) will have timestamp `time`.
    fn with_capacity(time: T, cap: usize) -> Self;
    /// Adds an element to the batch.
    fn push(&mut self, element: (K, V, R));
    /// Adds an ordered sequence of elements to the batch.
    fn extend<I: Iterator<Item = (K, V, R)>>(&mut self, iter: I) {
        for item in iter {
            self.push(item);
        }
    }
    /// Completes building and returns the batch.
    fn done(self) -> Output;
}

/// Represents a merge in progress.
pub trait Merger<K, V, T, R, Output: Batch<Key = K, Val = V, Time = T, R = R>> {
    /// Creates a new merger to merge the supplied batches, optionally
    /// compacting up to the supplied frontier.
    fn new(source1: &Output, source2: &Output) -> Self;
    /// Perform some amount of work, decrementing `fuel`.
    ///
    /// If `fuel` is non-zero after the call, the merging is complete and
    /// one should call `done` to extract the merged results.
    fn work(&mut self, source1: &Output, source2: &Output, fuel: &mut isize);
    /// Extracts merged results.
    ///
    /// This method should only be called after `work` has been called and
    /// has not brought `fuel` to zero. Otherwise, the merge is still in
    /// progress.
    fn done(self) -> Output;
}

/// Blanket implementations for reference counted batches.
pub mod rc_blanket_impls {

    use std::rc::Rc;

    use super::{Batch, BatchReader, Batcher, Builder, Cursor, Description, Merger};

    impl<B: BatchReader> BatchReader for Rc<B> {
        type Key = B::Key;
        type Val = B::Val;
        type Time = B::Time;
        type R = B::R;

        /// The type used to enumerate the batch's contents.
        type Cursor = RcBatchCursor<B>;
        /// Acquires a cursor to the batch's contents.
        fn cursor(&self) -> Self::Cursor {
            RcBatchCursor::new((&**self).cursor())
        }

        /// The number of updates in the batch.
        fn len(&self) -> usize {
            (&**self).len()
        }
        /// Describes the times of the updates in the batch.
        fn description(&self) -> &Description<B::Time> {
            (&**self).description()
        }
    }

    /// Wrapper to provide cursor to nested scope.
    pub struct RcBatchCursor<B: BatchReader> {
        phantom: ::std::marker::PhantomData<B>,
        cursor: B::Cursor,
    }

    impl<B: BatchReader> RcBatchCursor<B> {
        fn new(cursor: B::Cursor) -> Self {
            RcBatchCursor {
                cursor,
                phantom: ::std::marker::PhantomData,
            }
        }
    }

    impl<B: BatchReader> Cursor<B::Key, B::Val, B::Time, B::R> for RcBatchCursor<B> {
        type Storage = Rc<B>;

        #[inline]
        fn key_valid(&self, storage: &Self::Storage) -> bool {
            self.cursor.key_valid(storage)
        }
        #[inline]
        fn val_valid(&self, storage: &Self::Storage) -> bool {
            self.cursor.val_valid(storage)
        }

        #[inline]
        fn key<'a>(&self, storage: &'a Self::Storage) -> &'a B::Key {
            self.cursor.key(storage)
        }
        #[inline]
        fn val<'a>(&self, storage: &'a Self::Storage) -> &'a B::Val {
            self.cursor.val(storage)
        }

        #[inline]
        fn map_times<L: FnMut(&B::Time, &B::R)>(&mut self, storage: &Self::Storage, logic: L) {
            self.cursor.map_times(storage, logic)
        }

        #[inline]
        fn weight<'a>(&self, storage: &'a Self::Storage) -> &'a B::R
        where
            B::Time: PartialEq<()>,
        {
            self.cursor.weight(storage)
        }

        #[inline]
        fn step_key(&mut self, storage: &Self::Storage) {
            self.cursor.step_key(storage)
        }
        #[inline]
        fn seek_key(&mut self, storage: &Self::Storage, key: &B::Key) {
            self.cursor.seek_key(storage, key)
        }

        #[inline]
        fn step_val(&mut self, storage: &Self::Storage) {
            self.cursor.step_val(storage)
        }
        #[inline]
        fn seek_val(&mut self, storage: &Self::Storage, val: &B::Val) {
            self.cursor.seek_val(storage, val)
        }

        #[inline]
        fn rewind_keys(&mut self, storage: &Self::Storage) {
            self.cursor.rewind_keys(storage)
        }
        #[inline]
        fn rewind_vals(&mut self, storage: &Self::Storage) {
            self.cursor.rewind_vals(storage)
        }
    }

    /// An immutable collection of updates.
    impl<B: Batch> Batch for Rc<B> {
        type Batcher = RcBatcher<B>;
        type Builder = RcBuilder<B>;
        type Merger = RcMerger<B>;

        fn recede_to(&mut self, frontier: &B::Time) {
            Rc::get_mut(self).unwrap().recede_to(frontier);
        }
    }

    /// Wrapper type for batching reference counted batches.
    pub struct RcBatcher<B: Batch> {
        batcher: B::Batcher,
    }

    /// Functionality for collecting and batching updates.
    impl<B: Batch> Batcher<B::Key, B::Val, B::Time, B::R, Rc<B>> for RcBatcher<B> {
        fn new(time: B::Time) -> Self {
            RcBatcher {
                batcher: <B::Batcher as Batcher<B::Key, B::Val, B::Time, B::R, B>>::new(time),
            }
        }
        fn push_batch(&mut self, batch: &mut Vec<((B::Key, B::Val), B::R)>) {
            self.batcher.push_batch(batch)
        }
        fn tuples(&self) -> usize {
            self.batcher.tuples()
        }
        fn seal(self) -> Rc<B> {
            Rc::new(self.batcher.seal())
        }
    }

    /// Wrapper type for building reference counted batches.
    pub struct RcBuilder<B: Batch> {
        builder: B::Builder,
    }

    /// Functionality for building batches from ordered update sequences.
    impl<B: Batch> Builder<B::Key, B::Val, B::Time, B::R, Rc<B>> for RcBuilder<B> {
        fn new(time: B::Time) -> Self {
            RcBuilder {
                builder: <B::Builder as Builder<B::Key, B::Val, B::Time, B::R, B>>::new(time),
            }
        }
        fn with_capacity(time: B::Time, cap: usize) -> Self {
            RcBuilder {
                builder: <B::Builder as Builder<B::Key, B::Val, B::Time, B::R, B>>::with_capacity(
                    time, cap,
                ),
            }
        }
        fn push(&mut self, element: (B::Key, B::Val, B::R)) {
            self.builder.push(element)
        }
        fn done(self) -> Rc<B> {
            Rc::new(self.builder.done())
        }
    }

    /// Wrapper type for merging reference counted batches.
    pub struct RcMerger<B: Batch> {
        merger: B::Merger,
    }

    /// Represents a merge in progress.
    impl<B: Batch> Merger<B::Key, B::Val, B::Time, B::R, Rc<B>> for RcMerger<B> {
        fn new(source1: &Rc<B>, source2: &Rc<B>) -> Self {
            RcMerger {
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
}
