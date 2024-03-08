use super::{AsAny, Comparable, DowncastTrait};
use crate::{
    derive_comparison_traits,
    storage::file::{Deserializer, Serializer},
};
use rkyv::{archived_value, Archive, Archived, Deserialize, Fallible, Infallible, Serialize};
use std::{cmp::Ordering, marker::PhantomData, mem::transmute};

/// Trait for DBData that can be deserialized with [`rkyv`].
///
/// The associated type `Repr` with the bound + connecting it to Archived
/// seems to be the key for rust to know the bounds exist globally in the code
/// without having to specify the bounds everywhere.
pub trait ArchivedDBData: Serialize<Serializer> + Archive<Archived = Self::Repr> + Sized {
    type Repr: Deserialize<Self, Deserializer> + Ord;
}

/// We also automatically implement this bound for everything that satisfies it.
impl<T> ArchivedDBData for T
where
    T: Archive + Serialize<Serializer>,
    Archived<T>: Deserialize<T, Deserializer> + Ord,
{
    type Repr = Archived<T>;
}

/// Object-safe version of the `Serialize` trait.
pub trait SerializeDyn {
    fn serialize(
        &self,
        serializer: &mut Serializer,
    ) -> Result<usize, <Serializer as Fallible>::Error>;
}

pub trait DeserializableDyn {
    /// Deserialize `self` from the given slice and offset.
    ///
    /// # Safety
    ///
    /// The offset must store a valid serialized value of the
    /// concrete type that `self` points to.
    unsafe fn deserialize_from_bytes(&mut self, bytes: &[u8], pos: usize);
}

impl<T> SerializeDyn for T
where
    T: ArchivedDBData,
{
    fn serialize(
        &self,
        serializer: &mut Serializer,
    ) -> Result<usize, <Serializer as Fallible>::Error> {
        rkyv::ser::Serializer::serialize_value(serializer, self)
    }
}
impl<T> DeserializableDyn for T
where
    T: ArchivedDBData,
{
    unsafe fn deserialize_from_bytes(&mut self, bytes: &[u8], pos: usize) {
        let archived: &<Self as Archive>::Archived = archived_value::<Self>(bytes, pos);

        *self = archived.deserialize(&mut Infallible).unwrap();
    }
}

/// Object-safe version of the `Deserialize` trait.
pub trait DeserializeDyn<Trait: ?Sized>: AsAny + Comparable {
    fn deserialize(&self, target: &mut Trait);

    fn eq_target(&self, target: &Trait) -> bool;
    fn cmp_target(&self, target: &Trait) -> Option<Ordering>;
}

#[repr(transparent)]
pub struct DeserializeImpl<T: Archive, Trait: ?Sized> {
    pub archived: T::Archived,
    phantom: PhantomData<fn(T, Box<Trait>)>,
}

impl<T: Archive, Trait: ?Sized> AsRef<T::Archived> for DeserializeImpl<T, Trait> {
    fn as_ref(&self) -> &T::Archived {
        &self.archived
    }
}

impl<T: Archive, Trait: ?Sized> DeserializeImpl<T, Trait> {
    pub fn new(archived: &T::Archived) -> &Self {
        unsafe { transmute(archived) }
    }
}

impl<T, Trait> PartialEq for DeserializeImpl<T, Trait>
where
    T: Archive,
    T::Archived: PartialEq,
    Trait: ?Sized,
{
    fn eq(&self, other: &Self) -> bool {
        self.archived.eq(&other.archived)
    }
}

impl<T, Trait> Eq for DeserializeImpl<T, Trait>
where
    T: Archive,
    T::Archived: Eq,
    Trait: ?Sized,
{
}

impl<T, Trait> PartialOrd for DeserializeImpl<T, Trait>
where
    T: Archive,
    T::Archived: PartialOrd,
    Trait: ?Sized,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.archived.partial_cmp(&other.archived)
    }
}

impl<T, Trait> Ord for DeserializeImpl<T, Trait>
where
    T: Archive,
    T::Archived: Ord,
    Trait: ?Sized,
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.archived.cmp(&other.archived)
    }
}

impl<T, Trait> DeserializeDyn<Trait> for DeserializeImpl<T, Trait>
where
    T: ArchivedDBData + Eq + Ord + 'static,
    Trait: DowncastTrait + ?Sized + 'static,
{
    fn deserialize(&self, target: &mut Trait) {
        *unsafe { target.downcast_mut::<T>() } = self.archived.deserialize(&mut Infallible).unwrap()
    }

    fn eq_target(&self, other: &Trait) -> bool {
        self.archived
            .deserialize(&mut Infallible)
            .unwrap()
            .eq(unsafe { other.downcast::<T>() })
    }

    fn cmp_target(&self, other: &Trait) -> Option<Ordering> {
        self.archived
            .deserialize(&mut Infallible)
            .unwrap()
            .partial_cmp(unsafe { other.downcast::<T>() })
    }
}

/// Trait for trait objects that can be deserialized into a trait object of
/// type `TargetTrait`.
pub trait DeserializeTrait<TargetTrait: ?Sized>:
    DeserializeDyn<TargetTrait> + DowncastTrait + Eq + Ord
{
}

impl<Trait, TargetTrait: ?Sized> DeserializeTrait<TargetTrait> for Trait where
    Trait: DeserializeDyn<TargetTrait> + DowncastTrait + Eq + Ord + ?Sized
{
}

pub type DynDeserialize<Trait> = dyn DeserializeDyn<Trait>;

derive_comparison_traits!(DynDeserialize<Trait> where Trait: DowncastTrait + ?Sized + 'static);

impl<Trait: ?Sized + 'static> DowncastTrait for DynDeserialize<Trait> {}

/// Trait for trait objects that can be serialized and deserialized with `rkyv`.
pub trait ArchiveTrait: SerializeDyn {
    type Archived: DeserializeTrait<Self> + ?Sized;
}
