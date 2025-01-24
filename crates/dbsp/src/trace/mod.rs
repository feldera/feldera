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
use crate::dynamic::{ClonableTrait, DynDataTyped, DynUnit, Weight};
use crate::storage::buffer_cache::CacheStats;
pub use crate::storage::file::{Deserializable, Deserializer, Rkyv, Serializer};
use crate::time::Antichain;
use crate::{dynamic::ArchivedDBData, storage::buffer_cache::FBuf};
use cursor::{CursorFactory, CursorList};
use dyn_clone::DynClone;
use rand::Rng;
use rkyv::ser::Serializer as _;
use size_of::SizeOf;
use std::ops::Deref;
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
    fn weighted_vals_factory(&self) -> &'static dyn Factory<DynWeightedPairs<V, R>>;
    fn weighted_item_factory(&self) -> &'static dyn Factory<WeightedItem<K, V, R>>;

    /// Factory for a vector of (T, R) or `None` if `T` is `()`.
    fn time_diffs_factory(
        &self,
    ) -> Option<&'static dyn Factory<DynWeightedPairs<DynDataTyped<T>, R>>>;
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
    ///
    /// # Rationale
    ///
    /// This API is similar to the old API `BatchReader::truncate_keys_below`,
    /// but in [Trace] instead of [BatchReader].  The difference is that a batch
    /// can truncate its keys instanly by simply moving an internal pointer to
    /// the first remaining key.  However, there is no similar way to retain
    /// keys based on arbitrary predicates, this can only be done efficiently as
    /// part of trace maintenance when either merging or compacting batches.
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
    fn commit(&mut self, _base: &Path, _pid: &str) -> Result<(), Error> {
        Ok(())
    }
    fn restore(&mut self, _base: &Path, _pid: &str) -> Result<(), Error> {
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
///
/// # Object safety
///
/// `BatchReader` is not object safe (it cannot be used as `dyn BatchReader`),
/// but [Cursor] is, which can often be a useful substitute.
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

    /// Storage cache access statistics for this batch only.
    ///
    /// Most batches are in-memory, so they don't have any statistics.
    fn cache_stats(&self) -> CacheStats {
        CacheStats::default()
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

    /// A method that returns either true (possibly in the batch) or false
    /// (definitely not in the batch).
    fn maybe_contains_key(&self, _key: &Self::Key) -> bool {
        true
    }

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

    /// Creates and returns a new batch that is a subset of this one, containing
    /// only the key-value pairs whose keys are in `keys`. May also return
    /// `None`, the default implementation, if the batch doesn't want to
    /// implement this method.  In particular, a batch for which access through
    /// a cursor is fast should return `None` to avoid the expense of copying
    /// data.
    ///
    /// # Rationale
    ///
    /// This method enables performance optimizations for the case where these
    /// assumptions hold:
    ///
    /// 1. Individual [Batch]es flowing through a circuit are small enough to
    ///    fit comfortably in memory.
    ///
    /// 2. [Trace]s accumulated over time as a circuit executes may become large
    ///    enough that they must be maintained in external storage.
    ///
    /// If an operator needs to fetch all of the data from a `trace` that
    /// corresponds to some set of `keys`, then, given these assumptions, doing
    /// so one key at a time with a cursor will be slow because every key fetch
    /// potentially incurs a round trip to the storage, with total latency O(n)
    /// in the number of keys. This method gives the batch implementation the
    /// opportunity to implement parallel fetch for `trace.fetch(key)`, with
    /// total latency O(1) in the number of keys.
    #[allow(async_fn_in_trait)]
    async fn fetch<B>(
        &self,
        keys: &B,
    ) -> Option<Box<dyn CursorFactory<Self::Key, Self::Val, Self::Time, Self::R>>>
    where
        B: Batch<Key = Self::Key, Time = ()>,
    {
        let _ = keys;
        None
    }
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
        let mut builder = Self::Builder::with_capacity(factories, capacity);
        while cursor.key_valid() {
            let mut any_values = false;
            while cursor.val_valid() {
                let weight = cursor.weight();
                if !weight.is_zero() {
                    builder.push_time_diff(timestamp, weight);
                    builder.push_val(cursor.val());
                    any_values = true;
                }
                cursor.step_val();
            }
            if any_values {
                builder.push_key(cursor.key());
            }
            cursor.step_key();
        }
        builder.done_with_bounds(bounds_for_fixed_time(timestamp))
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
        Self::Builder::new_builder(factories)
            .done_with_bounds(bounds_for_fixed_time(&Self::Time::default()))
    }

    /// Returns elements from `self` that satisfy a predicate.
    fn filter(&self, predicate: &dyn Fn(&Self::Key, &Self::Val) -> bool) -> Self
    where
        Self::Time: PartialEq<()> + From<()>,
    {
        let factories = self.factories();
        let mut builder = Self::Builder::new_builder(&factories);
        let mut cursor = self.cursor();

        while cursor.key_valid() {
            let mut any_values = false;
            while cursor.val_valid() {
                if predicate(cursor.key(), cursor.val()) {
                    builder.push_diff(cursor.weight());
                    builder.push_val(cursor.val());
                    any_values = true;
                }
                cursor.step_val();
            }
            if any_values {
                builder.push_key(cursor.key());
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

    /// Adds a consolidated batch of elements to the batcher.
    ///
    /// A consolidated batch is sorted and contains no duplicates or zero
    /// weights.
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
///
/// This interface requires the client to push all of the time-diff pairs
/// associated with a value, then the value, then all the time-diff pairs
/// associated with the next value, then that value, and so on. Once all of the
/// values associated with the current key have been pushed, the client pushes
/// the key.
///
/// If this interface is too low-level for the client, consider wrapping it in a
/// [TupleBuilder].
///
/// # Example
///
/// To push the following tuples:
///
/// ```text
/// (k1, v1, t1, r1)
/// (k1, v1, t2, r2)
/// (k1, v2, t1, r1)
/// (k1, v3, t2, r2)
/// (k2, v1, t1, r1)
/// (k2, v1, t2, r2)
/// (k3, v1, t1, r1)
/// (k4, v2, t2, r2)
/// ```
///
/// the client would use:
///
/// ```ignore
/// builder.push_time_diff(t1, r1);
/// builder.push_time_diff(t2, r2);
/// builder.push_val(v1);
/// builder.push_time_diff(t1, r1);
/// builder.push_val(v2);
/// builder.push_time_diff(t2, r2);
/// builder.push_val(v3);
/// builder.push_key(k1);
/// builder.push_time_diff(t1, r1);
/// builder.push_time_diff(t2, r2);
/// builder.push_val(v1);
/// builder.push_key(k2);
/// builder.push_time_diff(t1, r1);
/// builder.push_val(v1);
/// builder.push_key(k3);
/// builder.push_time_diff(t2, r2);
/// builder.push_val(v2);
/// builder.push_key(k4);
/// ```
pub trait Builder<Output>: SizeOf
where
    Self: Sized,
    Output: Batch,
{
    /// Creates a new builder with an initial capacity of 0.
    fn new_builder(factories: &Output::Factories) -> Self {
        Self::with_capacity(factories, 0)
    }

    /// Creates an empty builder with some initial `capacity` for keys.
    fn with_capacity(factories: &Output::Factories, capacity: usize) -> Self;

    /// Creates an empty builder to hold the result of merging `batches`.
    fn for_merge<B, AR>(factories: &Output::Factories, batches: &[AR]) -> Self
    where
        B: BatchReader,
        AR: Deref<Target = B>,
    {
        let cap = batches.iter().map(|b| b.len()).sum();
        Self::with_capacity(factories, cap)
    }

    /// Adds time-diff pair `(time, weight)`.
    fn push_time_diff(&mut self, time: &Output::Time, weight: &Output::R);

    /// Adds time-diff pair `(time, weight)`.
    fn push_time_diff_mut(&mut self, time: &mut Output::Time, weight: &mut Output::R) {
        self.push_time_diff(time, weight);
    }

    /// Adds value `val`.
    fn push_val(&mut self, val: &Output::Val);

    /// Adds value `val`.
    fn push_val_mut(&mut self, val: &mut Output::Val) {
        self.push_val(val);
    }

    /// Adds key `key`.
    fn push_key(&mut self, key: &Output::Key);

    /// Adds key `key`.
    fn push_key_mut(&mut self, key: &mut Output::Key) {
        self.push_key(key);
    }

    /// Adds time-diff pair `(), weight`.
    fn push_diff(&mut self, weight: &Output::R)
    where
        Output::Time: PartialEq<()>,
    {
        self.push_time_diff(&Output::Time::default(), weight);
    }

    /// Adds time-diff pair `(), weight`.
    fn push_diff_mut(&mut self, weight: &mut Output::R)
    where
        Output::Time: PartialEq<()>,
    {
        self.push_diff(weight);
    }

    /// Adds time-diff pair `(), weight` and value `val`.
    fn push_val_diff(&mut self, val: &Output::Val, weight: &Output::R)
    where
        Output::Time: PartialEq<()>,
    {
        self.push_time_diff(&Output::Time::default(), weight);
        self.push_val(val);
    }

    /// Adds time-diff pair `(), weight` and value `val`.
    fn push_val_diff_mut(&mut self, val: &mut Output::Val, weight: &mut Output::R)
    where
        Output::Time: PartialEq<()>,
    {
        self.push_val_diff(val, weight);
    }

    /// Allocates room for `additional` keys.
    fn reserve(&mut self, additional: usize) {
        let _ = additional;
    }

    /// Completes building and returns the batch.
    fn done(self) -> Output
    where
        Output::Time: PartialEq<()>,
    {
        self.done_with_bounds(bounds_for_fixed_time(&Output::Time::default()))
    }

    /// Completes building and returns the batch with lower bound `bounds.0` and
    /// upper bound `bounds.1`.  The bounds must be correct to avoid violating
    /// invariants in the output type.
    fn done_with_bounds(self, bounds: (Antichain<Output::Time>, Antichain<Output::Time>))
        -> Output;
}

/// Returns appropriate bounds for a batch in which all tuples have the given
/// `time`.
pub fn bounds_for_fixed_time<T>(time: &T) -> (Antichain<T>, Antichain<T>)
where
    T: Timestamp,
{
    let lower = Antichain::from_elem(time.clone());
    let time_next = time.advance(0);
    let upper = if time_next <= *time {
        Antichain::new()
    } else {
        Antichain::from_elem(time_next)
    };
    (lower, upper)
}

/// Batch builder that accepts a full tuple at a time.
///
/// This wrapper for [Builder] allows a full tuple to be added at a time.
pub struct TupleBuilder<B, Output>
where
    B: Builder<Output>,
    Output: Batch,
{
    builder: B,
    kv: Box<DynPair<Output::Key, Output::Val>>,
    has_kv: bool,
}

impl<B, Output> TupleBuilder<B, Output>
where
    B: Builder<Output>,
    Output: Batch,
{
    pub fn new(factories: &Output::Factories, builder: B) -> Self {
        Self {
            builder,
            kv: factories.item_factory().default_box(),
            has_kv: false,
        }
    }

    /// Adds `element` to the batch.
    pub fn push(&mut self, element: &mut DynPair<DynPair<Output::Key, Output::Val>, Output::R>)
    where
        Output::Time: PartialEq<()>,
    {
        let (kv, w) = element.split_mut();
        let (k, v) = kv.split_mut();
        self.push_vals(k, v, &mut Output::Time::default(), w);
    }

    /// Adds tuple `(key, val, time, weight)` to the batch.
    pub fn push_refs(
        &mut self,
        key: &Output::Key,
        val: &Output::Val,
        time: &Output::Time,
        weight: &Output::R,
    ) {
        if self.has_kv {
            let (k, v) = self.kv.split_mut();
            if k != key {
                self.builder.push_val_mut(v);
                self.builder.push_key_mut(k);
                self.kv.from_refs(key, val);
            } else if v != val {
                self.builder.push_val_mut(v);
                val.clone_to(v);
            }
        } else {
            self.has_kv = true;
            self.kv.from_refs(key, val);
        }
        self.builder.push_time_diff(time, weight);
    }

    /// Adds tuple `(key, val, time, weight)` to the batch.
    pub fn push_vals(
        &mut self,
        key: &mut Output::Key,
        val: &mut Output::Val,
        time: &mut Output::Time,
        weight: &mut Output::R,
    ) {
        if self.has_kv {
            let (k, v) = self.kv.split_mut();
            if k != key {
                self.builder.push_val_mut(v);
                self.builder.push_key_mut(k);
                self.kv.from_vals(key, val);
            } else if v != val {
                self.builder.push_val_mut(v);
                val.move_to(v);
            }
        } else {
            self.has_kv = true;
            self.kv.from_vals(key, val);
        }
        self.builder.push_time_diff_mut(time, weight);
    }

    pub fn reserve(&mut self, additional: usize) {
        self.builder.reserve(additional)
    }

    /// Adds all of the tuples in `iter` to the batch.
    pub fn extend<'a, I>(&mut self, iter: I)
    where
        Output::Time: PartialEq<()>,
        I: Iterator<Item = &'a mut WeightedItem<Output::Key, Output::Val, Output::R>>,
    {
        let (lower, upper) = iter.size_hint();
        self.reserve(upper.unwrap_or(lower));

        for item in iter {
            let (kv, w) = item.split_mut();
            let (k, v) = kv.split_mut();

            self.push_vals(k, v, &mut Output::Time::default(), w);
        }
    }

    /// Completes building and returns the batch.
    pub fn done(self) -> Output
    where
        Output::Time: PartialEq<()>,
    {
        self.done_with_bounds(bounds_for_fixed_time(&Output::Time::default()))
    }

    /// Completes building and returns the batch with lower bound `bounds.0` and
    /// upper bound `bounds.1`.  The bounds must be correct to avoid violating
    /// invariants in the output type.
    pub fn done_with_bounds(
        mut self,
        bounds: (Antichain<Output::Time>, Antichain<Output::Time>),
    ) -> Output {
        if self.has_kv {
            let (k, v) = self.kv.split_mut();
            self.builder.push_val_mut(v);
            self.builder.push_key_mut(k);
        }
        self.builder.done_with_bounds(bounds)
    }
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
    let mut builder = B::Builder::with_capacity(factories, n);
    let mut key = factories.key_factory().default_box();
    let mut diff = factories.weight_factory().default_box();
    for i in 0..n {
        unsafe { key.deserialize_from_bytes(data, offsets[i * 2] as usize) };
        unsafe { diff.deserialize_from_bytes(data, offsets[i * 2 + 1] as usize) };
        builder.push_val_diff(&(), &diff);
        builder.push_key(&key);
    }
    builder.done()
}
