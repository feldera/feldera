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
//! [`Antichain`](crate::time::Antichain).

use crate::dynamic::ClonableTrait;
pub use crate::storage::file::{Deserializable, Deserializer, Rkyv, Serializer};
use crate::{dynamic::ArchivedDBData, storage::buffer_cache::FBuf};
use dyn_clone::DynClone;
use rand::Rng;
use size_of::SizeOf;
use std::{fmt::Debug, hash::Hash, path::PathBuf};

pub mod cursor;
pub mod layers;
pub mod ord;
pub mod spine_fueled;
#[cfg(test)]
pub mod test;

pub use ord::{
    FileIndexedZSet, FileIndexedZSetFactories, FileKeyBatch, FileKeyBatchFactories, FileValBatch,
    FileValBatchFactories, FileZSet, FileZSetFactories,
};
pub use ord::{
    OrdIndexedWSet, OrdIndexedWSetFactories, OrdKeyBatch, OrdKeyBatchFactories, OrdValBatch,
    OrdValBatchFactories, OrdWSet, OrdWSetFactories,
};

use rkyv::{archived_root, Deserialize, Infallible};

use crate::{
    algebra::MonoidValue,
    dynamic::{DataTrait, DynPair, DynVec, DynWeightedPairs, Erase, Factory, WeightTrait},
    time::AntichainRef,
    Error, NumEntries, Timestamp,
};
pub use cursor::Cursor;
pub use layers::Trie;
pub use spine_fueled::Spine;

/// Trait for data stored in batches.
///
/// This trait is used as a bound on `BatchReader::Key` and `BatchReader::Val`
/// associated types (see [`trait BatchReader`]).  Hence when writing code that
/// must be generic over any relational data, it is sufficient to impose
/// `DBData` as a trait bound on types.  Conversely, a trait bound of the form
/// `B: BatchReader` implies `B::Key: DBData` and `B::Val: DBData`.
pub trait DBData:
    Default + Clone + Eq + Ord + Hash + SizeOf + Send + Debug + ArchivedDBData + 'static
{
}

/// Automatically implement DBData for everything that satisfied the bounds.
impl<T> DBData for T where
    T: Default + Clone + Eq + Ord + Hash + SizeOf + Send + Debug + ArchivedDBData + 'static /* as ArchivedDBData>::Repr: Ord + PartialOrd<T>, */
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

pub trait FilterFunc<V: ?Sized>: Fn(&V) -> bool + DynClone {}

impl<V: ?Sized, F> FilterFunc<V> for F where F: Fn(&V) -> bool + Clone + 'static {}

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
    fn new<S: AsRef<str>>(factories: &Self::Factories, persistent_id: S) -> Self;

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

pub trait Spillable: BatchReader<Time = ()> {
    type Spilled: Batch<Key = Self::Key, Val = Self::Val, R = Self::R, Time = ()> + Stored;

    fn spill(&self, output_factories: &<Self::Spilled as BatchReader>::Factories) -> Self::Spilled {
        copy_batch(self, &(), output_factories)
    }
}

impl<K, V, R> Spillable for OrdIndexedWSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    type Spilled = FileIndexedZSet<K, V, R>;
}

impl<K, R> Spillable for OrdWSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    type Spilled = FileZSet<K, R>;
}

pub trait Stored: BatchReader<Time = ()> {
    type Unspilled: Batch<Key = Self::Key, Val = Self::Val, R = Self::R, Time = ()> + Spillable;

    fn unspill(
        &self,
        output_factories: &<Self::Unspilled as BatchReader>::Factories,
    ) -> Self::Unspilled {
        copy_batch(self, &(), output_factories)
    }
}

impl<K, V, R> Stored for FileIndexedZSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    type Unspilled = OrdIndexedWSet<K, V, R>;
}

impl<K, R> Stored for FileZSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    type Unspilled = OrdWSet<K, R>;
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
    fn dyn_empty(factories: &Self::Factories, time: Self::Time) -> Self {
        Self::Builder::new_builder(factories, time).done()
    }

    /// Pushes all timestamps in the trace back to `frontier` or less, by
    /// replacing each timestamp `t` in the trace by `t.meet(frontier)`.  See
    /// [`Trace::recede_to`].
    fn recede_to(&mut self, frontier: &Self::Time);

    fn persistent_id(&self) -> Option<PathBuf> {
        None
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

/// Represents a merge in progress.
pub trait Merger<K: ?Sized, V: ?Sized, T, R: ?Sized, Output>: SizeOf
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
pub fn merge_batches<B, T>(factories: &B::Factories, batches: T) -> B
where
    T: IntoIterator<Item = B>,
    B: Batch<Time = ()>,
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
    batches.pop().unwrap_or(B::dyn_empty(factories, ()))
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
