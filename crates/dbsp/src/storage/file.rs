//! File based data format for Feldera.
//!
//! A "layer file" stores `n > 0` columns of data, each of which has a key type
//! `K[i]` and an auxiliary data type `A[i]`.  Each column is arranged into
//! groups of rows, where column 0 forms a single group and each row in column
//! `i` is associated with a group of one or more rows in column `i + 1` (for
//! `i + 1 < n`).  A group contains sorted, unique values. A group cursor for
//! column `i` can move forward and backward by rows, seek forward and backward
//! by the key type `K[i]` or using a predicate based on `K[i]`, and (when `i +
//! 1 < n`) move to the row group in column `i + 1` associated with the cursor's
//! row.
//!
//! Thus, ignoring the auxiliary data in each column, a 1-column layer file is
//! analogous to `BTreeSet<K[0]>`, a 2-column layer file is analogous to
//! `BTreeMap<K[0], BTreeSet<K[1]>>`, and a 3-column layer file is analogous to
//! `BTreeMap<K[0], BTreeMap<K[1], BTreeSet<K[2]>>>`.
//!
//! For DBSP, it is likely that only 1-, 2, and 3-column layer files matter, and
//! maybe not even 3-column.
//!
//! Layer files are written once in their entirety and immutable thereafter.
//! Therefore, there are APIs for reading and writing layer files, but no API
//! for modifying them.
//!
//! Layer files use [`rkyv`] for serialization and deserialization.
//!
//! The "layer file" name comes from the `ColumnLayer` and `OrderedLayer` data
//! structures used in DBSP and inherited from Differential Dataflow.
//!
//! # Goals
//!
//! Layer files aim to balance read and write performance.  That is, neither
//! should be sacrificed to benefit the other.
//!
//! Row groups should implement indexing efficiently for `O(lg n)` seek by data
//! value and for sequential reads.  It should be possible to disable indexing
//! by data value for workloads that don't require it.
//!
//! Layer files support cheap key-membership tests using a per-batch filter
//! block. The default filter is Bloom-based; key types whose per-batch span
//! fits in `u32` can alternatively use an exact roaring bitmap filter by
//! storing keys relative to the batch minimum.
//!
//! Layer files should support 1 TB data size.
//!
//! Layer files should include data checksums to detect accidental corruption.
//!
//! # Design
//!
//! Layer files are stored as on-disk trees, one tree per column, with data
//! blocks as leaf nodes and index blocks as interior nodes.  Each tree's
//! branching factor is the number of values per data block and the number of
//! index entries per index block.  Block sizes and the branching factor can be
//! set as [parameters](`writer::Parameters`) at write time.
//!
//! Layer files support variable-length data in all columns.  The layer file
//! writer automatically detects fixed-length data and stores it slightly more
//! efficiently.
//!
//! Layer files index and compare data using [`Ord`] and [`Eq`], unlike many
//! data storage libraries that compare data lexicographically as byte arrays.
//! This convenience does prevent layer files from usefully storing only a
//! prefix of large data items plus a pointer to their full content.  In turn,
//! that means that, while layer files don't limit the size of data items, they
//! are always stored in full in index and data blocks, limiting performance for
//! large data.  This could be ameliorated if the layer file's clients were
//! permitted to provide a way to summarize data for comparisons.  The need for
//! this improvement is not yet clear, so it is not yet implemented.

// Warn about missing docs, but not for item declared with `#[cfg(test)]`.
#![cfg_attr(not(test), warn(missing_docs))]

use crate::{
    dynamic::{ArchivedDBData, DynVec, LeanVec},
    storage::buffer_cache::{FBuf, FBufSerializer},
};
use rkyv::{
    AlignedBytes,
    de::deserializers::SharedDeserializeMap,
    ser::{
        ScratchSpace,
        serializers::{
            AllocScratchError, BufferScratch, CompositeSerializerError, SharedSerializeMapError,
        },
    },
};
use rkyv::{
    Archive, Archived, Deserialize, Fallible, Serialize,
    ser::{Serializer as _, serializers::AllocScratch},
};
use rkyv::{
    de::{SharedDeserializeRegistry, SharedPointer},
    ser::SharedSerializeRegistry,
};
use std::{
    alloc::{Layout, alloc_zeroed},
    cell::RefCell,
    collections::{HashMap, hash_map::Entry},
};
use std::{any::Any, sync::Arc};
use std::{fmt::Debug, ptr::NonNull};

mod filter;
pub mod format;
mod item;
pub mod reader;
pub mod writer;

use crate::{
    DBData,
    dynamic::{DataTrait, Erase, Factory, WithFactory},
    storage::file::item::RefTup2Factory,
};
pub use filter::BatchKeyFilter;
pub use filter::FilterPlan;
pub use filter::TrackingRoaringBitmap;
pub use filter::{FilterKind, FilterStats, TrackingFilterStats};
pub use item::{ArchivedItem, Item, ItemFactory, WithItemFactory};

const BLOOM_FILTER_SEED: u128 = 42;

/// Default false-positive rate for Bloom filters in newly created files.
///
/// This can be adjusted via `DevTweaks`.
pub const BLOOM_FILTER_FALSE_POSITIVE_RATE: f64 = 0.0001;

/// Factory objects used by file reader and writer.
pub struct Factories<K, A>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    /// Factory for creating instances of `K`.
    pub key_factory: &'static dyn Factory<K>,

    /// Factory for creating instances of `Item<K, A>`.
    pub item_factory: &'static dyn ItemFactory<K, A>,

    /// Factory for creating instances of `Vector<K>`.
    pub keys_factory: &'static dyn Factory<DynVec<K>>,

    /// Factory for creating instances of `Vector<A>`.
    pub auxes_factory: &'static dyn Factory<DynVec<A>>,
}

impl<K, A> Clone for Factories<K, A>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            key_factory: self.key_factory,
            item_factory: self.item_factory,
            keys_factory: self.keys_factory,
            auxes_factory: self.auxes_factory,
        }
    }
}

impl<K, A> Factories<K, A>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    /// Create an instance of `Factories<K, A>` backed by concrete types
    /// `KType` and `AType`.
    pub fn new<KType, AType>() -> Self
    where
        KType: DBData + Erase<K>,
        AType: DBData + Erase<A>,
    {
        Self {
            key_factory: WithFactory::<KType>::FACTORY,
            item_factory: <RefTup2Factory<KType, AType> as WithItemFactory<K, A>>::ITEM_FACTORY,
            keys_factory: WithFactory::<LeanVec<KType>>::FACTORY,
            auxes_factory: WithFactory::<LeanVec<AType>>::FACTORY,
        }
    }

    /// Convert `self` into an instance of [`AnyFactories`],
    /// which can be used in contexts where `K` and `A` traits
    /// are unknown.
    ///
    /// See documentation of [`AnyFactories`].
    pub fn any_factories(&self) -> AnyFactories {
        AnyFactories {
            key_factory: Arc::new(self.key_factory),
            item_factory: Arc::new(self.item_factory),
            keys_factory: Arc::new(self.keys_factory),
            auxes_factory: Arc::new(self.auxes_factory),
        }
    }
}

/// A type-erased version of [`Factories`].
///
/// File reader and writer objects have types that don't include their key and
/// aux types as type arguments, which allows us to have arrays of readers and
/// writers with different data types. This requires a type erased
/// representation of factory objects, which can be downcast to concrete factory
/// types on demand. This struct offers such a representation by casting key and
/// item factories to `dyn Any`.
#[derive(Clone)]
pub struct AnyFactories {
    key_factory: Arc<dyn Any + Send + Sync + 'static>,
    item_factory: Arc<dyn Any + Send + Sync + 'static>,
    keys_factory: Arc<dyn Any + Send + Sync + 'static>,
    auxes_factory: Arc<dyn Any + Send + Sync + 'static>,
}

impl Debug for AnyFactories {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AnyFactories").finish()
    }
}

impl AnyFactories {
    fn key_factory<K>(&self) -> &'static dyn Factory<K>
    where
        K: DataTrait + ?Sized,
    {
        *self
            .key_factory
            .as_ref()
            .downcast_ref::<&'static dyn Factory<K>>()
            .unwrap()
    }

    fn item_factory<K, A>(&self) -> &'static dyn ItemFactory<K, A>
    where
        K: DataTrait + ?Sized,
        A: DataTrait + ?Sized,
    {
        *self
            .item_factory
            .as_ref()
            .downcast_ref::<&'static dyn ItemFactory<K, A>>()
            .unwrap()
    }

    fn keys_factory<K>(&self) -> &'static dyn Factory<DynVec<K>>
    where
        K: DataTrait + ?Sized,
    {
        *self
            .keys_factory
            .as_ref()
            .downcast_ref::<&'static dyn Factory<DynVec<K>>>()
            .unwrap()
    }

    fn auxes_factory<K>(&self) -> &'static dyn Factory<DynVec<K>>
    where
        K: DataTrait + ?Sized,
    {
        *self
            .auxes_factory
            .as_ref()
            .downcast_ref::<&'static dyn Factory<DynVec<K>>>()
            .unwrap()
    }

    fn factories<K, A>(&self) -> Factories<K, A>
    where
        K: DataTrait + ?Sized,
        A: DataTrait + ?Sized,
    {
        Factories {
            key_factory: self.key_factory(),
            item_factory: self.item_factory(),
            keys_factory: self.keys_factory(),
            auxes_factory: self.auxes_factory(),
        }
    }
}

/// Trait for data that can be serialized and deserialized with [`rkyv`].
pub trait Rkyv: Archive + for<'a> Serialize<DbspSerializer<'a>> + Deserializable {}
impl<T> Rkyv for T where T: Archive + for<'a> Serialize<DbspSerializer<'a>> + Deserializable {}

/// Trait for data that can be deserialized with [`rkyv`].
pub trait Deserializable: Archive<Archived = Self::ArchivedDeser> + Sized {
    /// Deserialized type.
    type ArchivedDeser: Deserialize<Self, Deserializer>;
}
impl<T: Archive> Deserializable for T
where
    Archived<T>: Deserialize<T, Deserializer>,
{
    type ArchivedDeser = Archived<T>;
}

/// Temporary storage needed during serialization.
///
/// We have this as a separate structure because it is a bit expensive to
/// allocate it for the purpose of serializing a single small data item.
pub struct SerializerInner {
    scratch: DbspScratch,
    shared_resolvers: HashMap<*const u8, usize>,
}

impl SerializerInner {
    /// Constructs a new `SerializerInner`.
    pub fn new() -> Self {
        Self {
            scratch: DbspScratch::default(),
            shared_resolvers: HashMap::new(),
        }
    }

    /// Constructs a `DbspSerializer` with this `SerializerInner` and
    /// `serializer`, and calls `f` on it, passing along its return value.
    pub fn with<F, R>(&mut self, serializer: FBufSerializer<&mut FBuf>, f: F) -> R
    where
        F: FnOnce(&mut DbspSerializer) -> R,
    {
        self.scratch.clear();
        self.shared_resolvers.clear();

        let mut serializer = DbspSerializer {
            serializer,
            inner: self,
        };
        f(&mut serializer)
    }

    /// Constructs a `DbspSerializer` with a thread-local `SerializerInner` and
    /// `serializer`, and calls `f` on it, passing along its return value.
    pub fn with_thread_local<F, R>(serializer: FBufSerializer<&mut FBuf>, f: F) -> R
    where
        F: FnOnce(&mut DbspSerializer) -> R,
    {
        thread_local! {
            static INNER: RefCell<SerializerInner> = RefCell::new(SerializerInner::new());
        }
        INNER.with_borrow_mut(|inner| inner.with(serializer, f))
    }

    /// Constructs a `DbspSerializer` with a thread-local `SerializerInner` and
    /// a new [FBuf], and calls `f` on it, returning the [FBuf].
    ///
    /// # Panic
    ///
    /// Panics if `f` returns an error.
    pub fn to_fbuf_with_thread_local<F, E, R>(f: F) -> FBuf
    where
        F: FnOnce(&mut DbspSerializer) -> Result<R, E>,
        E: Debug,
    {
        let mut fbuf = FBuf::default();
        Self::with_thread_local(FBufSerializer::new(&mut fbuf), f).unwrap();
        fbuf
    }
}

impl Default for SerializerInner {
    fn default() -> Self {
        Self::new()
    }
}

// SAFETY: SerializerInner is safe to send to another thread
// This trait is not automatically implemented because the struct contains a pointer
unsafe impl Send for SerializerInner {}

// SAFETY: SerializerInner is safe to share between threads
// This trait is not automatically implemented because the struct contains a pointer
unsafe impl Sync for SerializerInner {}

/// The [rkyv::ser::Serializer] used by DBSP.
pub struct DbspSerializer<'a> {
    /// The actual serializer.
    serializer: FBufSerializer<&'a mut FBuf>,

    /// The extra data we need temporarily during serialization.
    inner: &'a mut SerializerInner,
}

impl Fallible for DbspSerializer<'_> {
    type Error = CompositeSerializerError<
        <FBufSerializer<FBuf> as Fallible>::Error,
        <DbspScratch as Fallible>::Error,
        SharedSerializeMapError,
    >;
}

impl rkyv::ser::Serializer for DbspSerializer<'_> {
    fn pos(&self) -> usize {
        self.serializer.pos()
    }

    fn write(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
        self.serializer
            .write(bytes)
            .map_err(CompositeSerializerError::SerializerError)
    }
}

impl ScratchSpace for DbspSerializer<'_> {
    unsafe fn push_scratch(&mut self, layout: Layout) -> Result<NonNull<[u8]>, Self::Error> {
        unsafe { self.inner.scratch.push_scratch(layout) }
            .map_err(CompositeSerializerError::ScratchSpaceError)
    }

    unsafe fn pop_scratch(&mut self, ptr: NonNull<u8>, layout: Layout) -> Result<(), Self::Error> {
        unsafe { self.inner.scratch.pop_scratch(ptr, layout) }
            .map_err(CompositeSerializerError::ScratchSpaceError)
    }
}

impl SharedSerializeRegistry for DbspSerializer<'_> {
    fn get_shared_ptr(&self, value: *const u8) -> Option<usize> {
        self.inner.shared_resolvers.get(&value).copied()
    }

    fn add_shared_ptr(&mut self, value: *const u8, pos: usize) -> Result<(), Self::Error> {
        match self.inner.shared_resolvers.entry(value) {
            Entry::Occupied(_) => Err(CompositeSerializerError::SharedError(
                SharedSerializeMapError::DuplicateSharedPointer(value),
            )),
            Entry::Vacant(e) => {
                e.insert(pos);
                Ok(())
            }
        }
    }
}

/// Scratch space type used by DBSP.
///
/// This is `FallbackScratch<HeapScratch<SCRATCH_SIZE>, AllocScratch>` with the
/// added ability to clear it.
pub struct DbspScratch {
    main: BufferScratch<Box<AlignedBytes<SCRATCH_SIZE>>>,
    fallback: ReusableAllocScratch,
}

impl Default for DbspScratch {
    fn default() -> Self {
        Self::new()
    }
}

impl DbspScratch {
    fn new() -> Self {
        Self {
            main: {
                // This code is copied from `rkyv::ser::serializers::HeapScratch::new`.
                let layout = Layout::new::<AlignedBytes<SCRATCH_SIZE>>();
                unsafe {
                    // SAFETY: `layout` does have nonzero size.
                    let ptr = alloc_zeroed(layout).cast::<AlignedBytes<SCRATCH_SIZE>>();
                    assert!(!ptr.is_null());
                    // SAFETY: We are using the raw pointer only once and the memory is
                    // allocated by the global allocator using a layout for the correct type.
                    BufferScratch::new(Box::from_raw(ptr))
                }
            },
            fallback: ReusableAllocScratch::new(),
        }
    }

    fn clear(&mut self) {
        self.main.clear();
        self.fallback.clear();
    }
}

impl Fallible for DbspScratch {
    type Error = <AllocScratch as Fallible>::Error;
}

impl ScratchSpace for DbspScratch {
    #[inline]
    unsafe fn push_scratch(&mut self, layout: Layout) -> Result<NonNull<[u8]>, Self::Error> {
        unsafe {
            // SAFETY: This passes the safety constraints to the inner implementations.
            self.main
                .push_scratch(layout)
                .or_else(|_| self.fallback.push_scratch(layout))
        }
    }

    #[inline]
    unsafe fn pop_scratch(&mut self, ptr: NonNull<u8>, layout: Layout) -> Result<(), Self::Error> {
        unsafe {
            // SAFETY: This passes the safety constraints to the inner implementations.
            self.main
                .pop_scratch(ptr, layout)
                .or_else(|_| self.fallback.pop_scratch(ptr, layout))
        }
    }
}

/// Scratch space that always uses the global allocator.
///
/// This allocator will panic if scratch is popped that it did not allocate. For this reason, it
/// should only ever be used as a fallback allocator.
#[derive(Debug, Default)]
struct ReusableAllocScratch {
    allocations: Vec<(*mut u8, Layout)>,
}

// SAFETY: ReusableAllocScratch is safe to send to another thread
// This trait is not automatically implemented because the struct contains a pointer
unsafe impl Send for ReusableAllocScratch {}

// SAFETY: ReusableAllocScratch is safe to share between threads
// This trait is not automatically implemented because the struct contains a pointer
unsafe impl Sync for ReusableAllocScratch {}

impl ReusableAllocScratch {
    /// Creates a new scratch allocator.
    fn new() -> Self {
        Self::default()
    }

    fn clear(&mut self) {
        for (ptr, layout) in self.allocations.drain(..).rev() {
            unsafe {
                std::alloc::dealloc(ptr, layout);
            }
        }
    }
}

impl Drop for ReusableAllocScratch {
    fn drop(&mut self) {
        self.clear();
    }
}

impl Fallible for ReusableAllocScratch {
    type Error = AllocScratchError;
}

impl ScratchSpace for ReusableAllocScratch {
    #[inline]
    unsafe fn push_scratch(&mut self, layout: Layout) -> Result<NonNull<[u8]>, Self::Error> {
        // SAFETY: The trait definition's safety comments require that `layout`
        // be nonzero size.
        let result_ptr = unsafe { std::alloc::alloc(layout) };
        assert!(!result_ptr.is_null());
        self.allocations.push((result_ptr, layout));
        let result_slice = ptr_meta::from_raw_parts_mut(result_ptr.cast(), layout.size());
        // SAFETY: We asserted that the pointer is nonnull.
        let result = unsafe { NonNull::new_unchecked(result_slice) };
        Ok(result)
    }

    #[inline]
    unsafe fn pop_scratch(&mut self, ptr: NonNull<u8>, layout: Layout) -> Result<(), Self::Error> {
        if let Some(&(last_ptr, last_layout)) = self.allocations.last() {
            if ptr.as_ptr() == last_ptr && layout == last_layout {
                // SAFETY: The `if` condition ensures that the callee's safety
                // constraints are met.
                unsafe { std::alloc::dealloc(ptr.as_ptr(), layout) };
                self.allocations.pop();
                Ok(())
            } else {
                Err(AllocScratchError::NotPoppedInReverseOrder {
                    expected: last_ptr,
                    expected_layout: last_layout,
                    actual: ptr.as_ptr(),
                    actual_layout: layout,
                })
            }
        } else {
            Err(AllocScratchError::NoAllocationsToPop)
        }
    }
}

/// The particular [`rkyv`] deserializer that we use.
#[derive(Debug)]
pub struct Deserializer {
    version: u32,
    inner: SharedDeserializeMap,
}

impl Deserializer {
    /// Create a deserializer configured for the given file format version.
    pub fn new(version: u32) -> Self {
        // Proper error is returned in reader.rs, this is a sanity check.
        assert!(
            version >= format::VERSION_NUMBER,
            "Unable to read old (pre-v{}) checkpoint data on this feldera version, pipeline needs to backfilled to start.",
            format::VERSION_NUMBER
        );
        Self {
            version,
            inner: SharedDeserializeMap::new(),
        }
    }

    /// Create a deserializer with a preallocated shared pointer map.
    pub fn with_capacity(version: u32, capacity: usize) -> Self {
        Self {
            version,
            inner: SharedDeserializeMap::with_capacity(capacity),
        }
    }

    /// Return the file format version this deserializer targets.
    pub fn version(&self) -> u32 {
        self.version
    }
}

impl Default for Deserializer {
    fn default() -> Self {
        Self::new(format::VERSION_NUMBER)
    }
}

impl Fallible for Deserializer {
    type Error = <SharedDeserializeMap as Fallible>::Error;
}

impl SharedDeserializeRegistry for Deserializer {
    fn get_shared_ptr(&mut self, ptr: *const u8) -> Option<&dyn SharedPointer> {
        self.inner.get_shared_ptr(ptr)
    }

    fn add_shared_ptr(
        &mut self,
        ptr: *const u8,
        shared: Box<dyn SharedPointer>,
    ) -> Result<(), Self::Error> {
        self.inner.add_shared_ptr(ptr, shared)
    }
}

/// Scratch space size.
///
/// This is the amount of space we allocate as base scratch space for rkyv
/// serialization.  If more is needed for a particular serialization, then we
/// fall back to [AllocScratch].
pub const SCRATCH_SIZE: usize = 65536;

/// Serializes the given value and returns the resulting bytes.
///
/// This is like [`rkyv::to_bytes`], but that function only works with one
/// particular serializer whereas this function works with [`DbspSerializer`].
pub fn to_bytes<T>(value: &T) -> Result<FBuf, <DbspSerializer<'_> as Fallible>::Error>
where
    T: for<'a> Serialize<DbspSerializer<'a>>,
{
    Ok(SerializerInner::to_fbuf_with_thread_local(|serializer| {
        serializer.serialize_value(value)
    }))
}

/// Serializes the given value and returns the resulting bytes.
///
/// This is like [`rkyv::to_bytes`], but that function only works with one
/// particular serializer whereas this function works with [`DbspSerializer`].
pub fn to_bytes_dyn<T>(value: &T) -> Result<FBuf, <DbspSerializer<'_> as Fallible>::Error>
where
    T: ArchivedDBData,
{
    Ok(SerializerInner::to_fbuf_with_thread_local(|serializer| {
        serializer.serialize_value(value)
    }))
}

#[cfg(test)]
mod test;
