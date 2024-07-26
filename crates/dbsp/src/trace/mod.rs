//! # Traces
//!
//! A "trace" describes how a collection of key-value pairs changes over time.
//! A "batch" is a mostly immutable trace.  This module provides traits and
//! structures for expressing traces in DBSP as collections of `(key, val, time,
//! diff)` tuples.
//!
//! The base trait for a trace is [`BatchReader`], which allows a trace to be
//! read in sorted order by key and value.  `BatchReader` provides [`Cursor`] to
//! step through a batch's tuples without modifying them.
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
//! [`Antichain`].

use crate::circuit::metadata::OperatorMeta;
use crate::dynamic::ClonableTrait;
pub use crate::storage::file::{Deserializable, Deserializer, Rkyv, Serializer};
use crate::time::Antichain;
use crate::{dynamic::ArchivedDBData, storage::buffer_cache::FBuf};
use dyn_clone::DynClone;
use rand::Rng;
use size_of::SizeOf;
use std::path::Path;
use std::{fmt::Debug, hash::Hash, path::PathBuf};

pub mod cursor;
pub mod layers;
pub mod ord;
pub mod spine_async;
pub use spine_async::Spine;

mod spine_fueled;
pub use spine_fueled::Spine as OldSpine;
#[cfg(test)]
pub mod test;

pub use ord::{
    FallbackIndexedWSet, FallbackIndexedWSetBuilder, FallbackIndexedWSetFactories,
    FallbackIndexedWSetMerger, FallbackKeyBatch, FallbackKeyBatchFactories, FallbackValBatch,
    FallbackValBatchFactories, FallbackWSet, FallbackWSetBuilder, FallbackWSetFactories,
    FallbackWSetMerger, FileIndexedWSet, FileIndexedWSetFactories, FileKeyBatch,
    FileKeyBatchFactories, FileValBatch, FileValBatchFactories, FileWSet, FileWSetFactories,
    OrdIndexedWSet, OrdIndexedWSetBuilder, OrdIndexedWSetFactories, OrdIndexedWSetMerger,
    OrdKeyBatch, OrdKeyBatchFactories, OrdValBatch, OrdValBatchFactories, OrdWSet, OrdWSetBuilder,
    OrdWSetFactories, OrdWSetMerger, VecIndexedWSet, VecIndexedWSetFactories, VecKeyBatch,
    VecKeyBatchFactories, VecValBatch, VecValBatchFactories, VecWSet, VecWSetFactories,
};

use rkyv::{archived_root, Deserialize, Infallible};
use uuid::Uuid;

use crate::{
    algebra::MonoidValue,
    dynamic::{DataTrait, DynPair, DynVec, DynWeightedPairs, Erase, Factory, WeightTrait},
    storage::file::reader::Error as ReaderError,
    time::AntichainRef,
    Error, NumEntries, Timestamp,
};
pub use cursor::Cursor;
pub use layers::Trie;

/// Trait for data stored in batches.
///
/// This trait is used as a bound on `BatchReader::Key` and `BatchReader::Val`
/// associated types (see [`trait BatchReader`]).  Hence when writing code that
/// must be generic over any relational data, it is sufficient to impose
/// `DBData` as a trait bound on types.  Conversely, a trait bound of the form
/// `B: BatchReader` implies `B::Key: DBData` and `B::Val: DBData`.
pub trait DBData:
    Default + Clone + Eq + Ord + Hash + SizeOf + Send + Sync + Debug + ArchivedDBData + 'static
{
}

/// Automatically implement DBData for everything that satisfied the bounds.
impl<T> DBData for T where
    T: Default + Clone + Eq + Ord + Hash + SizeOf + Send + Sync + Debug + ArchivedDBData + 'static /* as ArchivedDBData>::Repr: Ord + PartialOrd<T>, */
{
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

pub trait FilterFunc<V: ?Sized>: Fn(&V) -> bool + DynClone + Send + Sync {}

impl<V: ?Sized, F> FilterFunc<V> for F where F: Fn(&V) -> bool + Clone + Send + Sync + 'static {}

dyn_clone::clone_trait_object! {<V: ?Sized> FilterFunc<V>}
pub type Filter<V> = Box<dyn FilterFunc<V>>;

pub trait BatchReaderFactories<
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T,
    R: WeightTrait + ?Sized,
>: Clone + Send
{
    // type BatchItemVTable: BatchItemTypeDescr<Key = K, Val = V, Item = I, R = R>;
    fn new<KType, VType, RType>() -> Self
    where
        KType: DBData + Erase<K>,
        VType: DBData + Erase<V>,
        RType: DBWeight + Erase<R>;

    fn key_factory(&self) -> &'static dyn Factory<K>;
    fn keys_factory(&self) -> &'static dyn Factory<DynVec<K>>;
    fn val_factory(&self) -> &'static dyn Factory<V>;
    fn weight_factory(&self) -> &'static dyn Factory<R>;
}

// TODO: use Tuple3 instead
pub type WeightedItem<K, V, R> = DynPair<DynPair<K, V>, R>;

pub trait BatchFactories<K: DataTrait + ?Sized, V: DataTrait + ?Sized, T, R: WeightTrait + ?Sized>:
    BatchReaderFactories<K, V, T, R>
{
    fn item_factory(&self) -> &'static dyn Factory<DynPair<K, V>>;

    fn weighted_items_factory(&self) -> &'static dyn Factory<DynWeightedPairs<DynPair<K, V>, R>>;
    fn weighted_item_factory(&self) -> &'static dyn Factory<WeightedItem<K, V, R>>;
}

/// A set of `(key, val, time, diff)` tuples that can be read and extended.
///
/// `Trace` extends [`BatchReader`], most notably with [`insert`][Self::insert]
/// for adding new batches of tuples.
///
/// See [crate documentation](crate::trace) for more information on batches and
/// traces.
pub trait Trace: BatchReader {
    /// The type of an immutable collection of updates.
    type Batch: Batch<
        Key = Self::Key,
        Val = Self::Val,
        Time = Self::Time,
        R = Self::R,
        Factories = Self::Factories,
    >;

    /// Allocates a new empty trace.
    fn new(factories: &Self::Factories) -> Self;

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
    fn commit<P: AsRef<str>>(&mut self, _cid: Uuid, _pid: P) -> Result<(), Error> {
        Ok(())
    }
    fn restore<P: AsRef<str>>(&mut self, _cid: Uuid, _pid: P) -> Result<(), Error> {
        Ok(())
    }

    /// Allows the trace to report additional metadata.
    fn metadata(&self, _meta: &mut OperatorMeta) {}
}

/// Where a batch is stored.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum BatchLocation {
    /// In RAM.
    Memory,

    /// On disk.
    Storage,
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
    type Factories: BatchFactories<Self::Key, Self::Val, Self::Time, Self::R>;

    /// Key by which updates are indexed.
    type Key: DataTrait + ?Sized;

    /// Values associated with keys.
    type Val: DataTrait + ?Sized;

    /// Timestamps associated with updates
    type Time: Timestamp;

    /// Associated update.
    type R: WeightTrait + ?Sized;

    /// The type used to enumerate the batch's contents.
    type Cursor<'s>: Cursor<Self::Key, Self::Val, Self::Time, Self::R> + Clone
    where
        Self: 's;

    // type Consumer: Consumer<Self::Key, Self::Val, Self::R, Self::Time>;

    fn factories(&self) -> Self::Factories;

    /// Acquires a cursor to the batch's contents.
    fn cursor(&self) -> Self::Cursor<'_>;

    //fn consumer(self) -> Self::Consumer;

    /// The number of keys in the batch.
    // TODO: return `(usize, Option<usize>)`, similar to
    // `Iterator::size_hint`, since not all implementations
    // can compute the number of keys precisely.  Same for
    // `len()`.
    fn key_count(&self) -> usize;

    /// The number of updates in the batch.
    fn len(&self) -> usize;

    /// The memory or storage size of the batch in bytes.
    ///
    /// This can be an approximation, such as the size of an on-disk file for a
    /// stored batch.
    ///
    /// Implementations of this function can be expensive because they might
    /// require iterating through all the data in a batch.  Currently this is
    /// only used to decide whether to keep the result of a merge in memory or
    /// on storage.  For this case, the merge will visit and copy all the data
    /// in the batch. The batch will be discarded afterward, which means that
    /// the implementation need not attempt to cache the return value.
    fn approximate_byte_size(&self) -> usize;

    /// Where the batch's data is stored.
    fn location(&self) -> BatchLocation {
        BatchLocation::Memory
    }

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
    fn sample_keys<RG>(&self, rng: &mut RG, sample_size: usize, sample: &mut DynVec<Self::Key>)
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
pub trait Batch: BatchReader + Clone + Send + Sync
where
    Self: Sized,
{
    /// A type used to assemble batches from disordered updates.
    type Batcher: Batcher<Self>;

    /// A type used to assemble batches from ordered update sequences.
    type Builder: Builder<Self>;

    /// A type used to progressively merge batches.
    type Merger: Merger<Self::Key, Self::Val, Self::Time, Self::R, Self> + Send + Sync;

    /// Assemble an unordered vector of weighted items into a batch.
    #[allow(clippy::type_complexity)]
    fn dyn_from_tuples(
        factories: &Self::Factories,
        time: Self::Time,
        tuples: &mut Box<DynWeightedPairs<DynPair<Self::Key, Self::Val>, Self::R>>,
    ) -> Self {
        let mut batcher = Self::Batcher::new_batcher(factories, time);
        batcher.push_batch(tuples);
        batcher.seal()
    }

    /*
    /// Assemble an unordered vector of keys into a batch.
    ///
    /// This method is only defined for batches whose `Val` type is `()`.
    fn from_keys(time: Self::Time, keys: Vec<(Self::Key, Self::R)>) -> Self
    where
        Self::Val: From<()>;
    */

    /// Initiates the merging of consecutive batches.
    ///
    /// The result of this method can be exercised to eventually produce the
    /// same result that a call to `self.merge(other)` would produce, but it
    /// can be done in a measured fashion. This can help to avoid latency
    /// spikes where a large merge needs to happen.
    ///
    /// If `dst_hint` is set, then the merger should prefer to write the output
    /// batch there, if it can.
    fn begin_merge(&self, other: &Self, dst_hint: Option<BatchLocation>) -> Self::Merger {
        Self::Merger::new_merger(self, other, dst_hint)
    }

    /// Merges `self` with `other` by running merger to completion.
    ///
    /// We keep the merge output in memory on the assumption that it's primarily
    /// spine merges that should be kept on storage, under the theory that other
    /// merges are likely to be large if large batches are passing through the
    /// pipeline.
    fn merge(&self, other: &Self) -> Self {
        let mut fuel = isize::MAX;
        let mut merger = Self::Merger::new_merger(self, other, Some(BatchLocation::Memory));
        merger.work(self, other, &None, &None, &mut fuel);
        merger.done()
    }

    /// Creates an empty batch.
    fn dyn_empty(factories: &Self::Factories) -> Self {
        Self::Builder::new_builder(factories, Self::Time::default()).done()
    }

    /// Pushes all timestamps in the trace back to `frontier` or less, by
    /// replacing each timestamp `t` in the trace by `t.meet(frontier)`.  See
    /// [`Batch::recede_to`].
    fn recede_to(&mut self, frontier: &Self::Time);

    /// If this batch is not on storage, but supports writing itself to storage,
    /// this method writes it to storage and returns the stored version.
    fn persisted(&self) -> Option<Self> {
        None
    }

    fn persistent_id(&self) -> Option<PathBuf> {
        None
    }

    fn from_path(_factories: &Self::Factories, _path: &Path) -> Result<Self, ReaderError> {
        Err(ReaderError::Unsupported)
    }
}

/// Functionality for collecting and batching updates.
pub trait Batcher<Output>: SizeOf
where
    Output: Batch,
{
    /// Allocates a new empty batcher.  All tuples in the batcher (and its
    /// output batch) will have timestamp `time`.
    fn new_batcher(vtables: &Output::Factories, time: Output::Time) -> Self;

    /// Adds an unordered batch of elements to the batcher.
    fn push_batch(
        &mut self,
        batch: &mut Box<DynWeightedPairs<DynPair<Output::Key, Output::Val>, Output::R>>,
    );

    /// Adds a consolidated batch of elements to the batcher
    fn push_consolidated_batch(
        &mut self,
        batch: &mut Box<DynWeightedPairs<DynPair<Output::Key, Output::Val>, Output::R>>,
    );

    /// Returns the number of tuples in the batcher.
    fn tuples(&self) -> usize;

    /// Returns all updates not greater or equal to an element of `upper`.
    fn seal(self) -> Output;
}

/// Functionality for building batches from ordered update sequences.
pub trait Builder<Output>: SizeOf
where
    Output: Batch,
{
    /// Allocates an empty builder.  All tuples in the builder (and its output
    /// batch) will have timestamp `time`.
    fn new_builder(factories: &Output::Factories, time: Output::Time) -> Self;

    /// Allocates an empty builder with some capacity.  All tuples in the
    /// builder (and its output batch) will have timestamp `time`.
    fn with_capacity(factories: &Output::Factories, time: Output::Time, cap: usize) -> Self;

    /// Adds an element to the batch.
    fn push(&mut self, element: &mut DynPair<DynPair<Output::Key, Output::Val>, Output::R>);

    fn push_refs(&mut self, key: &Output::Key, val: &Output::Val, weight: &Output::R);

    fn push_vals(&mut self, key: &mut Output::Key, val: &mut Output::Val, weight: &mut Output::R);

    fn reserve(&mut self, additional: usize);

    /// Adds an ordered sequence of elements to the batch.
    #[inline]
    fn extend<
        'a,
        It: Iterator<Item = &'a mut WeightedItem<Output::Key, Output::Val, Output::R>>,
    >(
        &mut self,
        iter: It,
    ) {
        let (lower, upper) = iter.size_hint();
        self.reserve(upper.unwrap_or(lower));

        for item in iter {
            self.push(item);
        }
    }

    /// Completes building and returns the batch.
    fn done(self) -> Output;
}

/// Functionality for building timed batches from ordered update sequences.
///
/// The [`Builder`] trait builds a batch in which every tuple has the same time.
/// This trait builds a batch in which each tuple's time can be individually
/// specified.
pub trait TimedBuilder<Output>: Builder<Output>
where
    Output: Batch,
{
    /// Allocates an empty builder with some capacity.
    fn timed_with_capacity(factories: &Output::Factories, cap: usize) -> Self
    where
        Self: Sized,
    {
        <Self as Builder<Output>>::with_capacity(factories, Output::Time::default(), cap)
    }

    /// Adds an element to the batch.
    fn push_time(
        &mut self,
        key: &Output::Key,
        val: &Output::Val,
        time: &Output::Time,
        weight: &Output::R,
    );

    /// Completes building and returns the batch with lower bound `lower` and
    /// upper bound `upper`.  The bounds must be correct to avoid violating
    /// invariants in the output type.
    fn done_with_bounds(
        self,
        lower: Antichain<Output::Time>,
        upper: Antichain<Output::Time>,
    ) -> Output;
}

/// Represents a merge in progress.
pub trait Merger<K: ?Sized, V: ?Sized, T, R: ?Sized, Output>: SizeOf
where
    Output: Batch<Key = K, Val = V, Time = T, R = R>,
{
    /// Creates a new merger to merge the supplied batches.
    fn new_merger(source1: &Output, source2: &Output, dst_hint: Option<BatchLocation>) -> Self;

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
pub fn merge_batches<B, T>(factories: &B::Factories, batches: T) -> B
where
    T: IntoIterator<Item = B>,
    B: Batch,
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
                .unwrap_or_else(|err| err);
            batches.insert(position, new);
        }
    }
    batches.pop().unwrap_or(B::dyn_empty(factories))
}

/// Copies a batch.  This uses the [`Builder`] interface to produce the output
/// batch, which only supports producing batches with a single fixed timestamp,
/// which must be supplied as `timestamp`.  To avoid discarding meaningful
/// timestamps in the input, the input batch's time type must be `()`.
///
/// This is useful for adding a timestamp to a batch, or for converting between
/// different batch implementations (e.g. writing an in-memory batch to disk).
///
/// TODO: for adding a timestamp to a batch, this could be implemented more
/// efficiently by having a special batch type where all updates have the same
/// timestamp, as this is the only kind of batch that we ever create directly in
/// DBSP; batches with multiple timestamps are only created as a result of
/// merging.  The main complication is that we will need to extend the trace
/// implementation to work with batches of multiple types.  This shouldn't be
/// too hard and is on the todo list.
pub fn copy_batch<BI, TS, BO>(batch: &BI, timestamp: &TS, factories: &BO::Factories) -> BO
where
    TS: Timestamp,
    BI: BatchReader<Time = ()>,
    BO: Batch<Key = BI::Key, Val = BI::Val, Time = TS, R = BI::R>,
{
    let mut builder = BO::Builder::with_capacity(factories, timestamp.clone(), batch.len());
    let mut cursor = batch.cursor();
    let mut weight = batch.factories().weight_factory().default_box();
    while cursor.key_valid() {
        while cursor.val_valid() {
            cursor.weight().clone_to(&mut weight);
            builder.push_refs(cursor.key(), cursor.val(), &weight);
            cursor.step_val();
        }
        cursor.step_key();
    }
    builder.done()
}

/// Compares two batches for equality.  This works regardless of whether the
/// batches are the same type, as long as their key, value, and weight types can
/// be compared for equality.
///
/// This can't be implemented as `PartialEq` because that is specialized for
/// comparing particular batch types (often in faster ways than this generic
/// function).  This function is mainly useful for testing in any case.
pub fn eq_batch<A, B, KA, VA, RA, KB, VB, RB>(a: &A, b: &B) -> bool
where
    A: BatchReader<Key = KA, Val = VA, Time = (), R = RA>,
    B: BatchReader<Key = KB, Val = VB, Time = (), R = RB>,
    KA: PartialEq<KB> + ?Sized,
    VA: PartialEq<VB> + ?Sized,
    RA: PartialEq<RB> + ?Sized,
    KB: ?Sized,
    VB: ?Sized,
    RB: ?Sized,
{
    let mut c1 = a.cursor();
    let mut c2 = b.cursor();
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
