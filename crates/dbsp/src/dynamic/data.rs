use size_of::SizeOf;
use std::{fmt::Debug, ops::DerefMut};

use crate::dynamic::arrow::{ArrowSupportDyn, HasArrowBuilder};
use crate::{
    declare_trait_object, declare_typed_trait_object,
    dynamic::{
        rkyv::SerializeDyn, ArchiveTrait, AsAny, Clonable, ClonableTrait, Comparable,
        DeserializableDyn, DowncastTrait,
    },
    hash::default_hash,
    DBData, NumEntries,
};

/// Defines the minimal set of operations that must be supported by
/// all data types stored in DBSP batches.
///
/// This trait is object safe and can be invoked via dynamic dispatch.
pub trait Data:
    Comparable
    + Clonable
    + SerializeDyn
    + DeserializableDyn
    + ArrowSupportDyn
    + Send
    + Sync
    + Debug
    + AsAny
    + SizeOf
{
    /// Compute a hash of the object using default hasher and seed.
    fn default_hash(&self) -> u64;

    /// Cast any type that implements this trait to `&dyn Data`.
    ///
    /// This method will not be needed once trait downcasting has been stabilized.
    fn as_data(&self) -> &dyn Data;

    /// Cast any type that implements this trait to `&mut dyn Data`.
    ///
    /// This method will not be needed once trait downcasting has been stabilized.
    fn as_data_mut(&mut self) -> &mut dyn Data;
}

/// The base trait for trait objects that DBSP can compute on.
pub trait DataTrait: Data + DowncastTrait + ClonableTrait + ArchiveTrait + Eq + Ord {}

impl<Trait: ?Sized> DataTrait for Trait where
    Trait: Data + DowncastTrait + ClonableTrait + ArchiveTrait + Eq + Ord
{
}

// `DynData` is a type alias for `dyn Data`.
//
// It is the minimal implementation of trait [`DataTrait`] that only provides the
// methods required by this trait and nothing else.
declare_trait_object!(DynData<> = dyn Data<>
where
);

// Fixme: we always treat `DynData` as a scalar.
impl NumEntries for DynData {
    const CONST_NUM_ENTRIES: Option<usize> = Some(1);

    #[inline]
    fn num_entries_shallow(&self) -> usize {
        1
    }

    #[inline]
    fn num_entries_deep(&self) -> usize {
        1
    }
}

impl<T: DBData> Data for T {
    fn default_hash(&self) -> u64 {
        default_hash(self)
    }

    fn as_data(&self) -> &DynData {
        self
    }

    fn as_data_mut(&mut self) -> &mut DynData {
        self
    }
}

/// This test ensures that the underlying hash function stays the same from one
/// build to the next. If the hash function changes, then restoring from a
/// checkpoint will fail because data hashes will change.
///
/// We only test this on little-endian platforms. Big-endian platforms will
/// compute a different hash value, which means that checkpoints won't be
/// exchangeable between big- and little-endian platforms.
#[cfg(all(test, target_endian = "little"))]
#[test]
fn test_default_hash() {
    assert_eq!(1.default_hash(), 15781232456890734344);
}

/// Strongly typed data.
///
/// A trait that includes the concrete type behind the given trait object in its type signature.
/// See [`DataTraitTyped`].
pub trait DataTyped: Data {
    /// Concrete inner type.
    type Type: DBData;
}

/// A trait for trait objects that include the concrete type behind the trait object in
/// their type signatures.
///
/// Such trait objects can be dereferenced into their concrete types.
///
/// See [`DynZWeight`](`crate::DynZWeight`) for an example of such a trait object.
pub trait DataTraitTyped: DataTyped + DataTrait + DerefMut<Target = Self::Type> {}

impl<Trait: ?Sized> DataTraitTyped for Trait where
    Trait: DataTyped + DataTrait + DerefMut<Target = Self::Type>
{
}

impl<T: DBData> DataTyped for T {
    type Type = T;
}

declare_typed_trait_object!(DynDataTyped<T> = dyn DataTyped<Type = T>
    [T]
where
    T: DBData,
);

// Fixme: we always treat `DynDataTyped` as a scalar.
impl<T: DBData> NumEntries for DynDataTyped<T> {
    const CONST_NUM_ENTRIES: Option<usize> = Some(1);

    #[inline]
    fn num_entries_shallow(&self) -> usize {
        1
    }

    #[inline]
    fn num_entries_deep(&self) -> usize {
        1
    }
}

/// A trait object backed by the unit type `()`.
pub type DynUnit = DynDataTyped<()>;

/// A trait object backed by `bool`.
pub type DynBool = DynDataTyped<bool>;
