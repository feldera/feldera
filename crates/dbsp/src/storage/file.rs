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
//! Layer files support approximate set membership query in `~O(1)` time using
//! [a filter block](format::FilterBlock).
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
use rkyv::de::deserializers::SharedDeserializeMap;
use rkyv::{
    ser::{
        serializers::{
            AllocScratch, CompositeSerializer, FallbackScratch, HeapScratch, SharedSerializeMap,
        },
        Serializer as _,
    },
    Archive, Archived, Deserialize, Fallible, Serialize,
};
use std::cell::RefCell;
use std::fmt::Debug;
use std::{any::Any, sync::Arc};

pub mod format;
mod item;
pub mod reader;
pub mod writer;

use crate::{
    dynamic::{DataTrait, Erase, Factory, WithFactory},
    storage::file::item::RefTup2Factory,
    DBData,
};
pub use item::{ArchivedItem, Item, ItemFactory, WithItemFactory};

const BLOOM_FILTER_SEED: u128 = 42;
const BLOOM_FILTER_FALSE_POSITIVE_RATE: f64 = 0.0001;

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
    pub(crate) fn any_factories(&self) -> AnyFactories {
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
pub trait Rkyv: Archive + Serialize<Serializer> + Deserializable {}
impl<T> Rkyv for T where T: Archive + Serialize<Serializer> + Deserializable {}

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

/// The particular [`rkyv::ser::Serializer`] that we use.
pub type Serializer = CompositeSerializer<FBufSerializer<FBuf>, DbspScratch, SharedSerializeMap>;

/// The particular [`rkyv::ser::ScratchSpace`] that we use.
pub type DbspScratch = FallbackScratch<HeapScratch<65536>, AllocScratch>;

/// The particular [`rkyv`] deserializer that we use.
pub type Deserializer = SharedDeserializeMap;

/// Creates an instance of [Serializer] that will serialize to `serializer` and
/// passes it to `f`. Returns a tuple of the `FBuf` from the [Serializer] and
/// the return value of `f`.
///
/// This is useful because it reuses the scratch space from one serializer to
/// the next, which is valuable because it saves an allocation and free per
/// serialization.
pub fn with_serializer<F, R>(serializer: FBufSerializer<FBuf>, f: F) -> (FBuf, R)
where
    F: FnOnce(&mut Serializer) -> R,
{
    thread_local! {
        static SCRATCH: RefCell<Option<DbspScratch>> = RefCell::new(Some(Default::default()));
    }

    let mut serializer = Serializer::new(serializer, SCRATCH.take().unwrap(), Default::default());
    let result = f(&mut serializer);
    let (serializer, scratch, _shared) = serializer.into_components();
    SCRATCH.replace(Some(scratch));
    (serializer.into_inner(), result)
}

/// Serializes the given value and returns the resulting bytes.
///
/// This is like [`rkyv::to_bytes`], but that function only works with one
/// particular serializer whereas this function works with our [`Serializer`].
pub fn to_bytes<T>(value: &T) -> Result<FBuf, <Serializer as Fallible>::Error>
where
    T: Serialize<Serializer>,
{
    let (bytes, result) = with_serializer(FBufSerializer::default(), |serializer| {
        serializer.serialize_value(value)
    });
    result?;
    Ok(bytes)
}

/// Serializes the given value and returns the resulting bytes.
///
/// This is like [`rkyv::to_bytes`], but that function only works with one
/// particular serializer whereas this function works with our [`Serializer`].
pub fn to_bytes_dyn<T>(value: &T) -> Result<FBuf, <Serializer as Fallible>::Error>
where
    T: ArchivedDBData,
{
    let (bytes, result) = with_serializer(FBufSerializer::default(), |serializer| {
        serializer.serialize_value(value)
    });
    result?;
    Ok(bytes)
}

#[cfg(test)]
mod test;
