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

use crate::circuit::metadata::{MetaItem, OperatorMeta};
use crate::circuit::GlobalNodeId;
use crate::dynamic::{ClonableTrait, DynUnit, Weight};
pub use crate::storage::file::{Deserializable, Deserializer, Rkyv, Serializer};
use crate::time::Antichain;
use crate::{dynamic::ArchivedDBData, storage::buffer_cache::FBuf};
use cursor::CursorList;
use dyn_clone::DynClone;
use rand::Rng;
use rkyv::ser::Serializer as _;
use size_of::SizeOf;
use std::path::Path;
use std::{fmt::Debug, hash::Hash, path::PathBuf};

pub mod cursor;
pub mod layers;
pub mod ord;
pub mod spine_async;
pub use spine_async::{Spine, SpineSnapshot};

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

use rkyv::{archived_root, de::deserializers::SharedDeserializeMap, Deserialize};
use uuid::Uuid;

use crate::dynamic::arrow::HasArrowBuilder;
use crate::{
    algebra::MonoidValue,
    dynamic::{
        arrow::ArrowSupportDyn, DataTrait, DynPair, DynVec, DynWeightedPairs, Erase, Factory,
        WeightTrait,
    },
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
    Default
    + Clone
    + Eq
    + Ord
    + Hash
    + SizeOf
    + Send
    + Sync
    + Debug
    + ArchivedDBData
    + ArrowSupportDyn
    + 'static
{
}

/// Automatically implement DBData for everything that satisfied the bounds.
impl<T> DBData for T where
    T: Default
        + Clone
        + Eq
        + Ord
        + Hash
        + SizeOf
        + Send
        + Sync
        + Debug
        + ArchivedDBData
        + ArrowSupportDyn
        + 'static /* as ArchivedDBData>::Repr: Ord + PartialOrd<T>, */
{
}

/// A spine that is serialized to a file.
#[derive(rkyv::Serialize, rkyv::Deserialize, rkyv::Archive)]
pub(crate) struct CommittedSpine<B: Batch + Send + Sync> {
    pub batches: Vec<String>,
    pub merged: Vec<(String, String)>,
    pub lower: Vec<B::Time>,
    pub upper: Vec<B::Time>,
    pub effort: u64,
    pub dirty: bool,
}

/// Trait for data that can be serialized and deserialized with [`rkyv`].
///
/// This trait doesn't have any extra bounds on Deserializable.
///
/// Deserializes `bytes` as type `T` using `rkyv`, tolerating `bytes` being
/// misaligned.
pub fn unaligned_deserialize<T: Deserializable>(bytes: &[u8]) -> T {
    let mut aligned_bytes = FBuf::new();
    aligned_bytes.extend_from_slice(bytes);
    unsafe { archived_root::<T>(&aligned_bytes[..]) }
        .deserialize(&mut SharedDeserializeMap::new())
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

pub struct Filter<V: ?Sized> {
    filter_func: Box<dyn FilterFunc<V>>,
    metadata: MetaItem,
}

impl<V: ?Sized> Filter<V> {
    pub fn new(filter_func: Box<dyn FilterFunc<V>>) -> Self {
        Self {
            filter_func,
            metadata: MetaItem::String(String::new()),
        }
    }

    pub fn with_metadata(mut self, metadata: MetaItem) -> Self {
        self.metadata = metadata;
        self
    }

    pub fn filter_func(&self) -> &dyn FilterFunc<V> {
        self.filter_func.as_ref()
    }

    pub fn metadata(&self) -> &MetaItem {
        &self.metadata
    }
}

impl<V: ?Sized> Clone for Filter<V> {
    fn clone(&self) -> Self {
        Self {
            filter_func: self.filter_func.clone(),
            metadata: self.metadata.clone(),
        }
    }
}

pub trait BatchReaderFactories<
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T,
    R: WeightTrait + ?Sized,
>: Clone + Send + Sync
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

    /// Sets a compaction frontier, i.e., a timestamp such that timestamps
    /// below the frontier are indistinguishable to DBSP, therefore any `ts`
    /// in the trace can be safely replaced with `ts.join(frontier)` without
    /// affecting the output of the circuit.  By applying this replacement,
    /// updates to the same (key, value) pairs applied during different steps
    /// can be merged or discarded.
    ///
    /// The compaction is performed lazily at merge time.
    fn set_frontier(&mut self, frontier: &Self::Time);

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
    fn init(&mut self, _gid: &GlobalNodeId) {}
    fn metrics(&self) {}

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
pub trait BatchReader: Debug + NumEntries + Rkyv + SizeOf + 'static
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
    type Cursor<'s>: Cursor<Self::Key, Self::Val, Self::Time, Self::R> + Clone + Send
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
/// See [crate documentation](crate::trace) for more information on batches and
/// traces.
pub trait Batch: BatchReader + Clone + Send + Sync
where
    Self: Sized,
{
    /// A type used to assemble batches from disordered updates.
    type Batcher: Batcher<Self>;

    /// A type used to assemble batches from ordered update sequences.
    type Builder: Builder<Self>;

    /// A type used to progressively merge batches.
    type Merger: Merger<Self::Key, Self::Val, Self::Time, Self::R, Self>;

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

    /// Creates a new batch as a copy of `batch`, using `timestamp` for all of
    /// the new batch's timestamps This is useful for adding a timestamp to a
    /// batch, or for converting between different batch implementations
    /// (e.g. writing an in-memory batch to disk).
    ///
    /// TODO: for adding a timestamp to a batch, this could be implemented more
    /// efficiently by having a special batch type where all updates have the same
    /// timestamp, as this is the only kind of batch that we ever create directly in
    /// DBSP; batches with multiple timestamps are only created as a result of
    /// merging.  The main complication is that we will need to extend the trace
    /// implementation to work with batches of multiple types.  This shouldn't be
    /// too hard and is on the todo list.
    fn from_batch<BI>(batch: &BI, timestamp: &Self::Time, factories: &Self::Factories) -> Self
    where
        BI: BatchReader<Key = Self::Key, Val = Self::Val, Time = (), R = Self::R>,
    {
        Self::from_cursor(batch.cursor(), timestamp, factories, batch.len())
    }

    /// Creates a new batch as a copy of the tuples accessible via `cursor``,
    /// using `timestamp` for all of the new batch's timestamps.
    fn from_cursor<C>(
        mut cursor: C,
        timestamp: &Self::Time,
        factories: &Self::Factories,
        capacity: usize,
    ) -> Self
    where
        C: Cursor<Self::Key, Self::Val, (), Self::R>,
    {
        let mut builder = Self::Builder::with_capacity(factories, timestamp.clone(), capacity);
        let mut weight = cursor.weight_factory().default_box();
        while cursor.key_valid() {
            while cursor.val_valid() {
                cursor.weight().clone_to(&mut weight);
                if !weight.is_zero() {
                    builder.push_refs(cursor.key(), cursor.val(), &weight);
                }
                cursor.step_val();
            }
            cursor.step_key();
        }
        builder.done()
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

    /// Merges `self` with `other` by running merger to completion, applying
    /// `key_filter` and `value_filter`.
    ///
    /// We keep the merge output in memory on the assumption that it's primarily
    /// spine merges that should be kept on storage, under the theory that other
    /// merges are likely to be large if large batches are passing through the
    /// pipeline.
    fn merge(
        &self,
        other: &Self,
        key_filter: &Option<Filter<Self::Key>>,
        value_filter: &Option<Filter<Self::Val>>,
    ) -> Self {
        let mut fuel = isize::MAX;
        let mut merger = Self::Merger::new_merger(self, other, Some(BatchLocation::Memory));
        merger.work(
            self,
            other,
            key_filter,
            value_filter,
            &Self::Time::minimum(),
            &mut fuel,
        );
        merger.done()
    }

    /// Creates an empty batch.
    fn dyn_empty(factories: &Self::Factories) -> Self {
        Self::Builder::new_builder(factories, Self::Time::default()).done()
    }

    /// Returns elements from `self` that satisfy a predicate.
    fn filter(&self, predicate: &dyn Fn(&Self::Key, &Self::Val) -> bool) -> Self
    where
        Self::Time: PartialEq<()> + From<()>,
    {
        let factories = self.factories();
        let mut builder = Self::Builder::new_builder(&factories, Self::Time::from(()));
        let mut cursor = self.cursor();
        let mut w = factories.weight_factory().default_box();

        while cursor.key_valid() {
            while cursor.val_valid() {
                if predicate(cursor.key(), cursor.val()) {
                    cursor.weight().clone_to(&mut w);
                    builder.push_refs(cursor.key(), cursor.val(), &w);
                }
                cursor.step_val();
            }
            cursor.step_key();
        }

        builder.done()
    }

    /// If this batch is not on storage, but supports writing itself to storage,
    /// this method writes it to storage and returns the stored version.
    fn persisted(&self) -> Option<Self> {
        None
    }

    /// This functions returns a path to a file that can be used by the checkpoint
    /// mechanism to find the batch again on re-start.
    ///
    /// If the batch can not be persisted, this function returns None.
    fn checkpoint_path(&self) -> Option<PathBuf> {
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
        frontier: &T,
        fuel: &mut isize,
    );

    /// Extracts merged results.
    ///
    /// This method should only be called after `work` has been called and
    /// has not brought `fuel` to zero. Otherwise, the merge is still in
    /// progress.
    fn done(self) -> Output;
}

/// Merges all of the batches in `batches`, applying `key_filter` and
/// `value_filter`, and returns the merged result.
///
/// The filters won't be applied to batches that don't get merged at all, that
/// is, if `batches` contains only one non-empty batch, or if it contains two
/// small batches that merge to become an empty batch alongside a third larger
/// batch, etc.
pub fn merge_batches<B, T>(
    factories: &B::Factories,
    batches: T,
    key_filter: &Option<Filter<B::Key>>,
    value_filter: &Option<Filter<B::Val>>,
) -> B
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
        let new = a.merge(&b, key_filter, value_filter);
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

/// Merges all of the batches in `batches` and returns the merged result.
///
/// This implements a multiway merge rather than iterating a 2-way merge.  This
/// is theoretically and asymptotically more efficient, but it is practically
/// much more efficient for the case where cloning items in the batch is
/// expensive, because 2-way merges will clone each data item `lg n` times
/// whereas an N-way merge will only clone them once each. For a 16-way merge,
/// that's a 4x reduction.
pub fn merge_untimed_batches<B, T>(factories: &B::Factories, batches: T) -> B
where
    T: IntoIterator<Item = B>,
    B: Batch<Time = ()>,
{
    let mut batches: Vec<_> = batches.into_iter().filter(|b| !b.is_empty()).collect();
    match batches.len() {
        0 => B::dyn_empty(factories),
        1 => batches.pop().unwrap(),
        2 => {
            // Presumably the specialized merge implementation for the batch
            // type can do better than our general algorithm.
            merge_batches(factories, batches, &None, &None)
        }
        _ => B::from_cursor(
            CursorList::new(
                factories.weight_factory(),
                batches.iter().map(|b| b.cursor()).collect(),
            ),
            &(),
            factories,
            batches.iter().map(|b| b.len()).sum(),
        ),
    }
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

fn serialize_wset<B, K, R>(batch: &B) -> Vec<u8>
where
    B: BatchReader<Key = K, Val = DynUnit, Time = (), R = R>,
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    let mut s = Serializer::default();
    let mut offsets = Vec::with_capacity(2 * batch.len());
    let mut cursor = batch.cursor();
    while cursor.key_valid() {
        offsets.push(cursor.key().serialize(&mut s).unwrap());
        offsets.push(cursor.weight().serialize(&mut s).unwrap());
        cursor.step_key();
    }
    let _offset = s.serialize_value(&offsets).unwrap();
    s.into_serializer().into_inner().into_vec()
}

fn deserialize_wset<B, K, R>(factories: &B::Factories, data: &[u8]) -> B
where
    B: Batch<Key = K, Val = DynUnit, Time = (), R = R>,
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    let offsets = unsafe { archived_root::<Vec<usize>>(data) };
    assert!(offsets.len() % 2 == 0);
    let n = offsets.len() / 2;
    let mut builder = B::Builder::with_capacity(factories, (), n);
    let mut key = factories.key_factory().default_box();
    let mut diff = factories.weight_factory().default_box();
    for i in 0..n {
        unsafe { key.deserialize_from_bytes(data, offsets[i * 2] as usize) };
        unsafe { diff.deserialize_from_bytes(data, offsets[i * 2 + 1] as usize) };
        builder.push_refs(&key, &(), &diff);
    }
    builder.done()
}
