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
//! tuples ([`Batch::Batcher`]), or by merging traces of like types.
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

use crate::circuit::metadata::{MetaItem, OperatorMeta};
use crate::dynamic::{ClonableTrait, DynDataTyped, DynUnit, Weight};
use crate::storage::buffer_cache::CacheStats;
pub use crate::storage::file::{Deserializable, Deserializer, Rkyv, Serializer};
use crate::trace::cursor::{
    DefaultPushCursor, FilteredMergeCursor, PushCursor, UnfilteredMergeCursor,
};
use crate::{dynamic::ArchivedDBData, storage::buffer_cache::FBuf};
use cursor::CursorFactory;
use dyn_clone::DynClone;
use enum_map::Enum;
use feldera_storage::{FileCommitter, FileReader, StoragePath};
use rand::Rng;
use rkyv::ser::Serializer as _;
use size_of::SizeOf;
use std::any::TypeId;
use std::sync::Arc;
use std::{fmt::Debug, hash::Hash};

pub mod cursor;
pub mod layers;
pub mod ord;
pub mod spine_async;
pub use spine_async::{
    BatchReaderWithSnapshot, ListMerger, MergerType, Spine, SpineSnapshot, WithSnapshot,
};

#[cfg(test)]
pub mod test;

pub use ord::{
    FallbackIndexedWSet, FallbackIndexedWSetBuilder, FallbackIndexedWSetFactories,
    FallbackKeyBatch, FallbackKeyBatchFactories, FallbackValBatch, FallbackValBatchFactories,
    FallbackWSet, FallbackWSetBuilder, FallbackWSetFactories, FileIndexedWSet,
    FileIndexedWSetFactories, FileKeyBatch, FileKeyBatchFactories, FileValBatch,
    FileValBatchFactories, FileWSet, FileWSetFactories, OrdIndexedWSet, OrdIndexedWSetBuilder,
    OrdIndexedWSetFactories, OrdKeyBatch, OrdKeyBatchFactories, OrdValBatch, OrdValBatchFactories,
    OrdWSet, OrdWSetBuilder, OrdWSetFactories, VecIndexedWSet, VecIndexedWSetFactories,
    VecKeyBatch, VecKeyBatchFactories, VecValBatch, VecValBatchFactories, VecWSet,
    VecWSetFactories,
};

use rkyv::{archived_root, de::deserializers::SharedDeserializeMap, Deserialize};

use crate::{
    algebra::MonoidValue,
    dynamic::{DataTrait, DynPair, DynVec, DynWeightedPairs, Erase, Factory, WeightTrait},
    storage::file::reader::Error as ReaderError,
    Error, NumEntries, Timestamp,
};
pub use cursor::{Cursor, MergeCursor};
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
pub(crate) struct CommittedSpine {
    pub batches: Vec<String>,
    pub merged: Vec<(String, String)>,
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

    pub fn include(this: &Option<Filter<V>>, value: &V) -> bool {
        this.as_ref().is_none_or(|f| (f.filter_func)(value))
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
    fn insert(&mut self, batch: Self::Batch);

    /// Introduces a batch of updates to the trace. More efficient that cloning
    /// a batch and calling `insert`.
    fn insert_arc(&mut self, batch: Arc<Self::Batch>);

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

    /// Writes this trace to storage beneath `base`, using `pid` as a file name
    /// prefix.  Adds the files that were written to `files` so that they can be
    /// committed later.
    fn save(
        &mut self,
        base: &StoragePath,
        pid: &str,
        files: &mut Vec<Arc<dyn FileCommitter>>,
    ) -> Result<(), Error>;

    /// Reads this trace back from storage under `base` with `pid` as the
    /// prefix.
    fn restore(&mut self, base: &StoragePath, pid: &str) -> Result<(), Error>;

    /// Allows the trace to report additional metadata.
    fn metadata(&self, _meta: &mut OperatorMeta) {}
}

/// Where a batch is stored.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Enum)]
pub enum BatchLocation {
    /// In RAM.
    Memory,

    /// On disk.
    Storage,
}

impl BatchLocation {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::Storage => "storage",
        }
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

    /// Acquires a [PushCursor] for the batch's contents.
    fn push_cursor(
        &self,
    ) -> Box<dyn PushCursor<Self::Key, Self::Val, Self::Time, Self::R> + Send + '_> {
        Box::new(DefaultPushCursor::new(self.cursor()))
    }

    /// Acquires a [MergeCursor] for the batch's contents.
    fn merge_cursor(
        &self,
        key_filter: Option<Filter<Self::Key>>,
        value_filter: Option<Filter<Self::Val>>,
    ) -> Box<dyn MergeCursor<Self::Key, Self::Val, Self::Time, Self::R> + Send + '_> {
        if key_filter.is_none() && value_filter.is_none() {
            Box::new(UnfilteredMergeCursor::new(self.cursor()))
        } else {
            Box::new(FilteredMergeCursor::new(
                UnfilteredMergeCursor::new(self.cursor()),
                key_filter,
                value_filter,
            ))
        }
    }

    /// Acquires a merge cursor for the batch's contents.
    fn consuming_cursor(
        &mut self,
        key_filter: Option<Filter<Self::Key>>,
        value_filter: Option<Filter<Self::Val>>,
    ) -> Box<dyn MergeCursor<Self::Key, Self::Val, Self::Time, Self::R> + Send + '_> {
        self.merge_cursor(key_filter, value_filter)
    }
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

    /// A method that returns either true (possibly in the batch) or false
    /// (definitely not in the batch).
    fn maybe_contains_key(&self, _hash: u64) -> bool {
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
        B: BatchReader<Key = Self::Key, Time = ()>,
    {
        let _ = keys;
        None
    }

    fn keys(&self) -> Option<&DynVec<Self::Key>> {
        None
    }
}

impl<B> BatchReader for Arc<B>
where
    B: BatchReader,
{
    type Factories = B::Factories;
    type Key = B::Key;
    type Val = B::Val;
    type Time = B::Time;
    type R = B::R;
    type Cursor<'s> = B::Cursor<'s>;
    fn factories(&self) -> Self::Factories {
        (**self).factories()
    }
    fn cursor(&self) -> Self::Cursor<'_> {
        (**self).cursor()
    }
    fn merge_cursor(
        &self,
        key_filter: Option<Filter<Self::Key>>,
        value_filter: Option<Filter<Self::Val>>,
    ) -> Box<dyn MergeCursor<Self::Key, Self::Val, Self::Time, Self::R> + Send + '_> {
        (**self).merge_cursor(key_filter, value_filter)
    }
    fn key_count(&self) -> usize {
        (**self).key_count()
    }
    fn len(&self) -> usize {
        (**self).len()
    }
    fn approximate_byte_size(&self) -> usize {
        (**self).approximate_byte_size()
    }
    fn location(&self) -> BatchLocation {
        (**self).location()
    }
    fn cache_stats(&self) -> CacheStats {
        (**self).cache_stats()
    }
    fn is_empty(&self) -> bool {
        (**self).is_empty()
    }
    fn maybe_contains_key(&self, hash: u64) -> bool {
        (**self).maybe_contains_key(hash)
    }
    fn sample_keys<RG>(&self, rng: &mut RG, sample_size: usize, sample: &mut DynVec<Self::Key>)
    where
        Self::Time: PartialEq<()>,
        RG: Rng,
    {
        (**self).sample_keys(rng, sample_size, sample)
    }
    fn consuming_cursor(
        &mut self,
        key_filter: Option<Filter<Self::Key>>,
        value_filter: Option<Filter<Self::Val>>,
    ) -> Box<dyn MergeCursor<Self::Key, Self::Val, Self::Time, Self::R> + Send + '_> {
        (**self).merge_cursor(key_filter, value_filter)
    }
    async fn fetch<KB>(
        &self,
        keys: &KB,
    ) -> Option<Box<dyn CursorFactory<Self::Key, Self::Val, Self::Time, Self::R>>>
    where
        KB: BatchReader<Key = Self::Key, Time = ()>,
    {
        (**self).fetch(keys).await
    }
    fn keys(&self) -> Option<&DynVec<Self::Key>> {
        (**self).keys()
    }
}

/// A [`BatchReader`] plus features for constructing new batches.
///
/// [`Batch`] extends [`BatchReader`] with types for constructing new batches
/// from ordered tuples ([`Self::Builder`]) or unordered tuples
/// ([`Self::Batcher`]), or by merging traces of like types, plus some
/// convenient methods for using those types.
///
/// See [crate documentation](crate::trace) for more information on batches and
/// traces.
pub trait Batch: BatchReader + Clone + Send + Sync
where
    Self: Sized,
{
    /// A batch type equivalent to `Self`, but with timestamp type `T` instead of `Self::Time`.
    type Timed<T: Timestamp>: Batch<
        Key = <Self as BatchReader>::Key,
        Val = <Self as BatchReader>::Val,
        Time = T,
        R = <Self as BatchReader>::R,
    >;

    /// A type used to assemble batches from disordered updates.
    type Batcher: Batcher<Self>;

    /// A type used to assemble batches from ordered update sequences.
    type Builder: Builder<Self>;

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
        // Source and destination types are usually the same in the top-level scope.
        // Optimize for this case by simply cloning the source batch. If the batch is
        // implemented as `Arc` internally, this is essentially zero cost.
        if TypeId::of::<BI>() == TypeId::of::<Self>() {
            unsafe { std::mem::transmute::<&BI, &Self>(batch).clone() }
        } else {
            Self::from_cursor(batch.cursor(), timestamp, factories, batch.len())
        }
    }

    /// Like `from_batch`, but avoids cloning the batch if the output type is identical to the input type.
    fn from_arc_batch<BI>(
        batch: &Arc<BI>,
        timestamp: &Self::Time,
        factories: &Self::Factories,
    ) -> Arc<Self>
    where
        BI: BatchReader<Key = Self::Key, Val = Self::Val, Time = (), R = Self::R>,
    {
        // Source and destination types are usually the same in the top-level scope.
        // Optimize for this case by simply cloning the source batch. If the batch is
        // implemented as `Arc` internally, this is essentially zero cost.
        if TypeId::of::<BI>() == TypeId::of::<Self>() {
            unsafe { std::mem::transmute::<&Arc<BI>, &Arc<Self>>(batch).clone() }
        } else {
            Arc::new(Self::from_cursor(
                batch.cursor(),
                timestamp,
                factories,
                batch.len(),
            ))
        }
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
                debug_assert!(!weight.is_zero());
                builder.push_time_diff(timestamp, weight);
                builder.push_val(cursor.val());
                any_values = true;
                cursor.step_val();
            }
            if any_values {
                builder.push_key(cursor.key());
            }
            cursor.step_key();
        }
        builder.done()
    }

    /// Creates an empty batch.
    fn dyn_empty(factories: &Self::Factories) -> Self {
        Self::Builder::new_builder(factories).done()
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

    /// This functions returns the file that can be used to restore the batch's
    /// contents.
    ///
    /// If the batch can not be persisted, this function returns None.
    fn file_reader(&self) -> Option<Arc<dyn FileReader>> {
        None
    }

    fn from_path(_factories: &Self::Factories, _path: &StoragePath) -> Result<Self, ReaderError> {
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

    /// Creates an empty builder to hold the result of merging
    /// `batches`. Optionally, `location` can specify the preferred location for
    /// the result of the merge.
    fn for_merge<'a, B, I>(
        factories: &Output::Factories,
        batches: I,
        location: Option<BatchLocation>,
    ) -> Self
    where
        B: BatchReader,
        I: IntoIterator<Item = &'a B> + Clone,
    {
        let _ = location;
        let cap = batches.into_iter().map(|b| b.len()).sum();
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

    fn num_tuples(&self) -> usize;

    /// Completes building and returns the batch.
    fn done(self) -> Output;
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
    num_tuples: usize,
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
            num_tuples: 0,
        }
    }

    pub fn num_tuples(&self) -> usize {
        self.num_tuples
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
        self.num_tuples += 1;
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
        self.num_tuples += 1;
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
    pub fn done(mut self) -> Output {
        if self.has_kv {
            let (k, v) = self.kv.split_mut();
            self.builder.push_val_mut(v);
            self.builder.push_key_mut(k);
        }
        self.builder.done()
    }
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
    // Collect input batches, discarding empty batches.
    let mut batches = batches
        .into_iter()
        .filter(|b| !b.is_empty())
        .collect::<Vec<_>>();

    // Merge groups of up to 64 input batches to one output batch each.
    //
    // In practice, there are <= 64 input batches and 1 output batch (or 0 if
    // the inputs cancel each other out).
    while batches.len() > 1 {
        let mut inputs = batches.split_off(batches.len().saturating_sub(64));
        let result: B = ListMerger::merge(
            factories,
            B::Builder::for_merge(factories, &inputs, Some(BatchLocation::Memory)),
            inputs
                .iter_mut()
                .map(|b| b.consuming_cursor(key_filter.clone(), value_filter.clone()))
                .collect(),
        );
        if !result.is_empty() {
            batches.push(result);
        }
    }

    // Take the final output batch, or synthesize an empty one if all the
    // batches added up to nothing.
    batches.pop().unwrap_or_else(|| B::dyn_empty(factories))
}

/// Merges all of the batches in `batches`, applying `key_filter` and
/// `value_filter`, and returns the merged result.
///
/// Every tuple will be passed through the filters.
pub fn merge_batches_by_reference<'a, B, T>(
    factories: &B::Factories,
    batches: T,
    key_filter: &Option<Filter<B::Key>>,
    value_filter: &Option<Filter<B::Val>>,
) -> B
where
    T: IntoIterator<Item = &'a B> + Clone,
    B: Batch,
{
    // Collect input batches, discarding empty batches.
    let mut batches = batches
        .into_iter()
        .filter(|b| !b.is_empty())
        .collect::<Vec<_>>();

    // Merge groups of up to 64 input batches to one output batch each. This
    // also transforms `&B` in `batches` into `B` in `outputs`.
    //
    // In practice, there are <= 64 input batches and 1 output batch (or 0 if
    // the inputs cancel each other out).
    let mut outputs = Vec::with_capacity(batches.len().div_ceil(64));
    while !batches.is_empty() {
        let inputs = batches.split_off(batches.len().saturating_sub(64));
        let result: B = ListMerger::merge(
            factories,
            B::Builder::for_merge(
                factories,
                inputs.iter().cloned(),
                Some(BatchLocation::Memory),
            ),
            inputs
                .into_iter()
                .map(|b| b.merge_cursor(key_filter.clone(), value_filter.clone()))
                .collect(),
        );
        if !result.is_empty() {
            outputs.push(result);
        }
    }

    // Merge the output batches (in practice, either 0 or 1 of them).
    merge_batches(factories, outputs, key_filter, value_filter)
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

/// Separator that identifies the end of values for a key.
const SEPARATOR: u64 = u64::MAX;

pub fn serialize_indexed_wset<B, K, V, R>(batch: &B) -> Vec<u8>
where
    B: BatchReader<Key = K, Val = V, Time = (), R = R>,
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    let mut s = Serializer::default();
    let mut offsets = Vec::with_capacity(2 * batch.len());
    let mut cursor = batch.cursor();
    offsets.push(batch.len());

    while cursor.key_valid() {
        offsets.push(cursor.key().serialize(&mut s).unwrap());
        while cursor.val_valid() {
            offsets.push(cursor.val().serialize(&mut s).unwrap());
            offsets.push(cursor.weight().serialize(&mut s).unwrap());

            cursor.step_val();
        }
        cursor.step_key();
        offsets.push(SEPARATOR as usize);
    }
    let _offset = s.serialize_value(&offsets).unwrap();
    s.into_serializer().into_inner().into_vec()
}

pub fn deserialize_indexed_wset<B, K, V, R>(factories: &B::Factories, data: &[u8]) -> B
where
    B: Batch<Key = K, Val = V, Time = (), R = R>,
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    let offsets = unsafe { archived_root::<Vec<usize>>(data) };
    let len = offsets[0];

    let mut builder = B::Builder::with_capacity(factories, len as usize);
    let mut key = factories.key_factory().default_box();
    let mut val = factories.val_factory().default_box();
    let mut diff = factories.weight_factory().default_box();

    let mut current_offset = 1;

    while current_offset < offsets.len() {
        unsafe { key.deserialize_from_bytes(data, offsets[current_offset] as usize) };
        current_offset += 1;
        while offsets[current_offset] != SEPARATOR {
            unsafe { val.deserialize_from_bytes(data, offsets[current_offset] as usize) };
            current_offset += 1;
            unsafe { diff.deserialize_from_bytes(data, offsets[current_offset] as usize) };

            builder.push_val_diff(&val, &diff);
            current_offset += 1;
        }

        current_offset += 1;
        builder.push_key(&key);
    }
    builder.done()
}

#[cfg(test)]
mod serialize_test {
    use crate::{
        algebra::OrdIndexedZSet as DynOrdIndexedZSet,
        dynamic::DynData,
        indexed_zset,
        trace::{deserialize_indexed_wset, serialize_indexed_wset, BatchReader},
        DynZWeight, OrdIndexedZSet,
    };

    #[test]
    fn test_serialize_indexed_wset() {
        let test1: OrdIndexedZSet<u64, u64> = indexed_zset! {};
        let test2 = indexed_zset! { 1 => { 1 => 1 } };
        let test3 =
            indexed_zset! { 1 => { 1 => 1, 2 => 2, 3 => 3 }, 2 => { 1 => 1, 2 => 2, 3 => 3 } };

        for test in [test1, test2, test3] {
            let serialized = serialize_indexed_wset(&*test);
            let deserialized = deserialize_indexed_wset::<
                DynOrdIndexedZSet<DynData, DynData>,
                DynData,
                DynData,
                DynZWeight,
            >(&test.factories(), &serialized);

            assert_eq!(&*test, &deserialized);
        }
    }

    #[test]
    fn test_serialize_indexed_wset_tup0_key() {
        let test1: OrdIndexedZSet<(), u64> = indexed_zset! {};
        let test2 = indexed_zset! { () => { 1 => 1 } };

        for test in [test1, test2] {
            let serialized = serialize_indexed_wset(&*test);
            let deserialized = deserialize_indexed_wset::<
                DynOrdIndexedZSet<DynData, DynData>,
                DynData,
                DynData,
                DynZWeight,
            >(&test.factories(), &serialized);

            assert_eq!(&*test, &deserialized);
        }
    }

    #[test]
    fn test_serialize_indexed_wset_tup0_val() {
        let test1: OrdIndexedZSet<u64, ()> = indexed_zset! {};
        let test2 = indexed_zset! { 1 => { () => 1 } };
        let test3 = indexed_zset! { 1 => { () => 1 }, 2 => { () => 1 } };

        for test in [test1, test2, test3] {
            let serialized = serialize_indexed_wset(&*test);
            let deserialized = deserialize_indexed_wset::<
                DynOrdIndexedZSet<DynData, DynData>,
                DynData,
                DynData,
                DynZWeight,
            >(&test.factories(), &serialized);

            assert_eq!(&*test, &deserialized);
        }
    }
}
