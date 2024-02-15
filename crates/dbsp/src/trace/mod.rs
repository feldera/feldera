//! # Traces
//!
//! A "trace" describes how a collection of key-value pairs changes over time.
//! A "batch" is a mostly immutable trace.  This module provides traits and
//! structures for expressing traces in DBSP as collections of `(key, val, time,
//! diff)` tuples.
//!
//! The base trait for a trace is [`BatchReader`], which allows a trace to be
//! read in sorted order by key and value.  `BatchReader` provides [`Cursor`] to
//! step through a batch's tuples without modifying them, and [`Consumer`] to
//! read tuples while taking ownership.
//!
//! The [`Batch`] trait extends [`BatchReader`] with types and methods for
//! creating new traces from ordered tuples ([`Batch::Builder`]) or unordered
//! tuples ([`Batch::Batcher`]), or by merging traces of like types
//! ([`Batch::Merger`]).
//!
//! The [`Trace`] trait, which also extends [`BatchReader`], adds methods to
//! append new batches.  New tuples must not have times earlier than any of the
//! tuples already in the trace.
//!
//! # Time within traces
//!
//! See the [time](crate::time) module documentation for a description of
//! logical times.
//!
//! Traces are sorted by key and value.  They are not sorted with respect to
//! time: reading a trace might obtain out of order and duplicate times among
//! the `(time, diff)` pairs associated with a key and value.
//!
//! Traces keep track of the lower and upper bounds among their tuples' times.
//! If the trace contains incomparable times, then it will have multiple lower
//! and upper bounds, one for each category of incomparable time, in an
//! [`Antichain`](crate::time::Antichain).

pub mod consolidation;
pub mod cursor;
pub mod layers;
pub mod ord;
#[cfg(feature = "persistence")]
pub mod persistent;
pub mod spine_fueled;

pub use cursor::{Consumer, Cursor, ValueConsumer};
use feldera_storage::buffer_cache::FBuf;
pub use feldera_storage::file::{Deserializable, Deserializer, Rkyv, Serializer};

#[cfg(feature = "persistence")]
pub use persistent::PersistentTrace as Spine;
#[cfg(not(feature = "persistence"))]
pub use spine_fueled::Spine;

#[cfg(test)]
pub mod test_batch;

use crate::{
    algebra::{HasZero, MonoidValue},
    circuit::Activator,
    time::{AntichainRef, Timestamp},
    Error, NumEntries,
};
use rand::Rng;
use rkyv::{archived_root, Archive, Archived, Deserialize, Infallible, Serialize};
use size_of::SizeOf;
use std::path::PathBuf;
use std::{fmt::Debug, hash::Hash};

/// Trait for data stored in batches.
///
/// This trait is used as a bound on `BatchReader::Key` and `BatchReader::Val`
/// associated types (see [`trait BatchReader`]).  Hence when writing code that
/// must be generic over any relational data, it is sufficient to impose
/// `DBData` as a trait bound on types.  Conversely, a trait bound of the form
/// `B: BatchReader` implies `B::Key: DBData` and `B::Val: DBData`.
pub trait DBData:
    Clone
    + Eq
    + Ord
    + Hash
    + SizeOf
    + Send
    + Debug
    + Archive
    + Serialize<Serializer>
    + ArchivedDBData
    + 'static
where
    // We want to be able Clone the serialized version and compare it with the
    // original as well as serialized version.
    <Self as ArchivedDBData>::Repr: Ord + PartialOrd<Self>,
{
}

/// Automatically implement DBData for everything that satisfied the bounds.
impl<T> DBData for T
where
    T: Clone
        + Eq
        + Ord
        + Hash
        + SizeOf
        + Send
        + Debug
        + Archive
        + Serialize<Serializer>
        + ArchivedDBData
        + 'static,
    <T as ArchivedDBData>::Repr: Ord + PartialOrd<T>,
{
}

/// Trait for DBData that can be deserialized with [`rkyv`].
///
/// The associated type `Repr` with the bound + connecting it to Archived
/// seems to be the key for rust to know the bounds exist globally in the code
/// without having to specify the bounds everywhere.
pub trait ArchivedDBData: Archive<Archived = Self::Repr> + Sized {
    type Repr: Deserialize<Self, Deserializer> + Ord + PartialOrd<Self>;
}

/// We also automatically implement this bound for everything that satisfies it.
impl<T: Archive> ArchivedDBData for T
where
    Archived<T>: Deserialize<T, Deserializer> + Ord + PartialOrd<T>,
{
    type Repr = Archived<T>;
}

/// Trait for data that can be serialized and deserialized with [`rkyv`].
///
/// This trait doesn't have any extra bounds on Deserializable.

/// Deserializes `bytes` as type `T` using `rkyv`, tolerating `bytes` being
/// misaligned.
pub fn unaligned_deserialize<T: Deserializable>(bytes: &[u8]) -> T {
    let mut aligned_bytes = FBuf::new();
    aligned_bytes.extend_from_slice(bytes);
    unsafe { archived_root::<T>(&aligned_bytes[..]) }
        .deserialize(&mut Infallible)
        .unwrap()
}

/// Trait for data types used as weights.
///
/// A type used for weights in a batch (i.e., as `BatchReader::R`) must behave
/// as a monoid, i.e., a set with an associative `+` operation and a neutral
/// element (zero).
///
/// Some applications use a weight as a ring, that is, require it to support
/// multiplication too.
///
/// Finally, some applications require it to have `<` and `>` operations, in
/// particular to distinguish whether something is an insertion or deletion.
///
/// Signed integer types such as `i32` and `i64` are suitable as weights,
/// although if there is overflow then the results will be wrong.
///
/// When writing code generic over any weight type, it is sufficient to impose
/// `DBWeight` as a trait bound on types.  Conversely, a trait bound of the form
/// `B: BatchReader` implies `B::R: DBWeight`.
pub trait DBWeight: DBData + MonoidValue {}

impl<T> DBWeight for T where T: DBData + MonoidValue {}

/// Trait for data types used as logical timestamps.
pub trait DBTimestamp: DBData + Timestamp {}

impl<T> DBTimestamp for T where T: DBData + Timestamp {}

pub trait FilterFunc<V>: Fn(&V) -> bool {
    fn fork(&self) -> Filter<V>;
}

impl<V, F> FilterFunc<V> for F
where
    F: Fn(&V) -> bool + Clone + 'static,
{
    fn fork(&self) -> Filter<V> {
        Box::new(self.clone())
    }
}

pub type Filter<V> = Box<dyn FilterFunc<V>>;

/// A set of `(key, val, time, diff)` tuples that can be read and extended.
///
/// `Trace` extends [`BatchReader`], most notably with [`insert`][Self::insert]
/// for adding new batches of tuples.
///
/// See [crate documentation](crate::trace) for more information on batches and
/// traces.
pub trait Trace: BatchReader {
    /// The type of an immutable collection of updates.
    type Batch: Batch<Key = Self::Key, Val = Self::Val, Time = Self::Time, R = Self::R>;

    /// Allocates a new empty trace.
    fn new<S: AsRef<str>>(activator: Option<Activator>, persistent_id: S) -> Self;

    /// Pushes all timestamps in the trace back to `frontier` or less, by
    /// replacing each timestamp `t` in the trace by `t.meet(frontier)`.  This
    /// has no effect on timestamps that are already less than or equal to
    /// `frontier`.  For later timestamps, it reduces the number of distinct
    /// timestamps in the trace, which in turn allows the trace to combine
    /// tuples that now have the same key, value, and time by adding their
    /// weights, reducing memory consumption.
    ///
    /// See [`NestedTimestamp`](crate::time::NestedTimestamp32) for information
    /// on how DBSP can take advantage of its usage of time.
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

    /// Informs the trace that keys that don't pass the filter are no longer
    /// used and can be removed from the trace.
    ///
    /// The implementation is not required to remove truncated keys instantly
    /// or at all.  This method is just a hint that keys that don't pass the
    /// filter are no longer of interest to the consumer of the trace and
    /// can be garbage collected.
    // This API is similar to `BatchReader::truncate_keys_below`, however we make
    // it a method of `trait Trace` rather than `trait BatchReader`.  The difference
    // is that a batch can truncate its keys instanly by simply moving an internal
    // pointer to the first remaining key.  However, there is no similar way to
    // retain keys based on arbitrary predicates, this can only be done efficiently
    // as part of trace maintenance when either merging or compacting batches.
    fn retain_keys(&mut self, filter: Filter<Self::Key>);

    /// Informs the trace that values that don't pass the filter are no longer
    /// used and can be removed from the trace.
    ///
    /// The implementation is not required to remove truncated values instantly
    /// or at all.  This method is just a hint that values that don't pass the
    /// filter are no longer of interest to the consumer of the trace and
    /// can be garbage collected.
    fn retain_values(&mut self, filter: Filter<Self::Val>);

    fn key_filter(&self) -> &Option<Filter<Self::Key>>;
    fn value_filter(&self) -> &Option<Filter<Self::Val>>;
    fn commit(&self, _cid: u64) -> Result<(), Error> {
        Ok(())
    }
}

/// A set of `(key, value, time, diff)` tuples whose contents may be read in
/// order by key and value.
///
/// A `BatchReader` is a mostly read-only interface.  This is especially useful
/// for views derived from other sources in ways that prevent the construction
/// of batches from the type of data in the view (for example, filtered views,
/// or views with extended time coordinates).
///
/// See [crate documentation](crate::trace) for more information on batches and
/// traces.
pub trait BatchReader: NumEntries + Rkyv + SizeOf + 'static
where
    Self: Sized,
{
    /// Key by which updates are indexed.
    type Key: DBData;

    /// Values associated with keys.
    type Val: DBData;

    /// Timestamps associated with updates
    type Time: DBTimestamp;

    /// Associated update.
    type R: DBWeight;

    /// The type used to enumerate the batch's contents.
    type Cursor<'s>: Cursor<Self::Key, Self::Val, Self::Time, Self::R> + Clone
    where
        Self: 's;

    type Consumer: Consumer<Self::Key, Self::Val, Self::R, Self::Time>;

    /// Acquires a cursor to the batch's contents.
    fn cursor(&self) -> Self::Cursor<'_>;

    fn consumer(self) -> Self::Consumer;

    /// The number of keys in the batch.
    // TODO: return `(usize, Option<usize>)`, similar to
    // `Iterator::size_hint`, since not all implementations
    // can compute the number of keys precisely.  Same for
    // `len()`.
    fn key_count(&self) -> usize;

    /// The number of updates in the batch.
    fn len(&self) -> usize;

    /// True if the batch is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// All times in the batch are greater or equal to an element of `lower`.
    fn lower(&self) -> AntichainRef<'_, Self::Time>;

    /// All times in the batch are not greater or equal to any element of
    /// `upper`.
    fn upper(&self) -> AntichainRef<'_, Self::Time>;

    /// Remove keys smaller than `lower_bound` from the batch.
    ///
    /// The removed tuples may not get deallocated instantly but they won't
    /// appear when iterating over the batch.
    fn truncate_keys_below(&mut self, lower_bound: &Self::Key);

    /// Returns a uniform random sample of distincts keys from the batch.
    ///
    /// Does not take into account the number values associated with each
    /// key and their weights, i.e., a key that has few values is as likely
    /// to appear in the output sample as a key with many values.
    ///
    /// # Arguments
    ///
    /// * `rng` - random number generator used to generate the sample.
    ///
    /// * `sample_size` - requested sample size.
    ///
    /// * `sample` - output
    ///
    /// # Invariants
    ///
    /// The actual sample computed by the method can be smaller than
    /// `sample_size` even if `self` contains `>sample_size` keys.
    ///
    /// A correct implementation must enforce the following invariants:
    ///
    /// * The output sample size cannot exceed `sample_size`.
    ///
    /// * The output sample can only contain keys present in `self` (with
    ///   non-zero weights).
    ///
    /// * If `sample_size` is greater than or equal to the number of keys
    ///   present in `self` (with non-zero weights), the resulting sample must
    ///   contain all such keys.
    ///
    /// * The output sample contains keys sorted in ascending order.
    fn sample_keys<RG>(&self, rng: &mut RG, sample_size: usize, sample: &mut Vec<Self::Key>)
    where
        Self::Time: PartialEq<()>,
        RG: Rng;
}

/// A [`BatchReader`] plus features for constructing new batches.
///
/// [`Batch`] extends [`BatchReader`] with types for constructing new batches
/// from ordered tuples ([`Self::Builder`]) or unordered tuples
/// ([`Self::Batcher`]), or by merging traces of like types ([`Self::Merger`]),
/// plus some convenient methods for using those types.
///
/// A `Batch` is mostly immutable, with the exception of [`recede_to`].
///
/// See [crate documentation](crate::trace) for more information on batches and
/// traces.
///
/// [`recede_to`]: Self::recede_to
pub trait Batch: BatchReader + Clone
where
    Self: Sized,
{
    /// Items used to assemble the batch.  Must be one of `Self::Key`
    /// (when `Self::Val = ()`) or `(Self::Key, Self::Val)`.
    type Item;

    /// A type used to assemble batches from disordered updates.
    type Batcher: Batcher<Self::Item, Self::Time, Self::R, Self>;

    /// A type used to assemble batches from ordered update sequences.
    type Builder: Builder<Self::Item, Self::Time, Self::R, Self>;

    /// A type used to progressively merge batches.
    type Merger: Merger<Self::Key, Self::Val, Self::Time, Self::R, Self>;

    /// Create an item from a `(key, value)` pair.
    fn item_from(key: Self::Key, val: Self::Val) -> Self::Item;

    /// Assemble an unordered vector of weighted items into a batch.
    #[allow(clippy::type_complexity)]
    fn from_tuples(time: Self::Time, mut tuples: Vec<(Self::Item, Self::R)>) -> Self {
        let mut batcher = Self::Batcher::new_batcher(time);
        batcher.push_batch(&mut tuples);
        batcher.seal()
    }

    /// Assemble an unordered vector of keys into a batch.
    ///
    /// This method is only defined for batches whose `Val` type is `()`.
    fn from_keys(time: Self::Time, keys: Vec<(Self::Key, Self::R)>) -> Self
    where
        Self::Val: From<()>;

    /// Initiates the merging of consecutive batches.
    ///
    /// The result of this method can be exercised to eventually produce the
    /// same result that a call to `self.merge(other)` would produce, but it
    /// can be done in a measured fashion. This can help to avoid latency
    /// spikes where a large merge needs to happen.
    fn begin_merge(&self, other: &Self) -> Self::Merger {
        Self::Merger::new_merger(self, other)
    }

    /// Merges `self` with `other` by running merger to completion.
    fn merge(&self, other: &Self) -> Self {
        let mut fuel = isize::max_value();
        let mut merger = Self::Merger::new_merger(self, other);
        merger.work(self, other, &None, &None, &mut fuel);
        merger.done()
    }

    /// Creates an empty batch.
    fn empty(time: Self::Time) -> Self {
        Self::Builder::new_builder(time).done()
    }

    /// Pushes all timestamps in the trace back to `frontier` or less, by
    /// replacing each timestamp `t` in the trace by `t.meet(frontier)`.  See
    /// [`Trace::recede_to`].
    fn recede_to(&mut self, frontier: &Self::Time);

    fn persistent_id(&self) -> Option<PathBuf> {
        None
    }
}

impl<B> HasZero for B
where
    B: Batch<Time = ()>,
{
    fn zero() -> Self {
        Self::empty(())
    }

    fn is_zero(&self) -> bool {
        self.is_empty()
    }
}

/// Functionality for collecting and batching updates.
pub trait Batcher<I, T, R, Output>: SizeOf
where
    Output: Batch<Item = I, Time = T, R = R>,
{
    /// Allocates a new empty batcher.  All tuples in the batcher (and its
    /// output batch) will have timestamp `time`.
    fn new_batcher(time: T) -> Self;

    /// Adds an unordered batch of elements to the batcher.
    fn push_batch(&mut self, batch: &mut Vec<(I, R)>);

    /// Adds a consolidated batch of elements to the batcher
    fn push_consolidated_batch(&mut self, batch: &mut Vec<(I, R)>);

    /// Returns the number of tuples in the batcher.
    fn tuples(&self) -> usize;

    /// Returns all updates not greater or equal to an element of `upper`.
    fn seal(self) -> Output;
}

/// Functionality for building batches from ordered update sequences.
pub trait Builder<I, T, R, Output>: SizeOf
where
    Output: Batch<Item = I, Time = T, R = R>,
{
    /// Allocates an empty builder.  All tuples in the builder (and its output
    /// batch) will have timestamp `time`.
    fn new_builder(time: T) -> Self;

    /// Allocates an empty builder with some capacity.  All tuples in the
    /// builder (and its output batch) will have timestamp `time`.
    fn with_capacity(time: T, cap: usize) -> Self;

    /// Adds an element to the batch.
    fn push(&mut self, element: (I, R));

    fn reserve(&mut self, additional: usize);

    /// Adds an ordered sequence of elements to the batch.
    #[inline]
    fn extend<It: Iterator<Item = (I, R)>>(&mut self, iter: It) {
        let (lower, upper) = iter.size_hint();
        self.reserve(upper.unwrap_or(lower));

        for item in iter {
            self.push(item);
        }
    }

    /// Completes building and returns the batch.
    fn done(self) -> Output;
}

/// Represents a merge in progress.
pub trait Merger<K, V, T, R, Output>: SizeOf
where
    Output: Batch<Key = K, Val = V, Time = T, R = R>,
{
    /// Creates a new merger to merge the supplied batches.
    fn new_merger(source1: &Output, source2: &Output) -> Self;

    /// Perform some amount of work, decrementing `fuel`.
    ///
    /// If `fuel` is greater than zero after the call, the merging is complete
    /// and one should call `done` to extract the merged results.
    fn work(
        &mut self,
        source1: &Output,
        source2: &Output,
        key_filter: &Option<Filter<K>>,
        value_filter: &Option<Filter<V>>,
        fuel: &mut isize,
    );

    /// Extracts merged results.
    ///
    /// This method should only be called after `work` has been called and
    /// has not brought `fuel` to zero. Otherwise, the merge is still in
    /// progress.
    fn done(self) -> Output;
}

/// Merges all of the batches in `batches` and returns the merged result.
pub fn merge_batches<B, T>(batches: T) -> B
where
    T: IntoIterator<Item = B>,
    B: Batch<Time = ()>,
    B::Key: Clone,
    B::Val: Clone,
{
    // Repeatedly merge the smallest two batches until no more than one batch is
    // left.
    //
    // Because weights can add to zero, merging two non-empty batches can yield
    // an empty batch.
    let mut batches: Vec<_> = batches.into_iter().filter(|b| !b.is_empty()).collect();
    batches.sort_unstable_by(|a, b| a.len().cmp(&b.len()).reverse());
    while batches.len() > 1 {
        let a = batches.pop().unwrap();
        let b = batches.pop().unwrap();
        let new = a.merge(&b);
        if !new.is_empty() {
            let new_len = new.len();
            let position = batches
                .binary_search_by(|element| element.len().cmp(&new_len).reverse())
                .map_or_else(|err| err, |ok| ok);
            batches.insert(position, new);
        }
    }
    batches.pop().unwrap_or(B::empty(()))
}
