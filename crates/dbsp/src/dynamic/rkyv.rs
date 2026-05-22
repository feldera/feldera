use super::{AsAny, Comparable, DowncastTrait};
use crate::{
    derive_comparison_traits,
    storage::file::{DbspSerializer, Deserializer},
};
use rkyv::{Archive, Archived, Deserialize, Fallible, Serialize, archived_value, option::ArchivedOption};
use std::{cmp::Ordering, marker::PhantomData, mem::transmute};

/// Cross-type ordered comparison between an archived value and the original.
///
/// Mirrors [`PartialOrd<T>`] but is a local trait so we can implement it for
/// types where `PartialOrd<T>` would be blocked by Rust's orphan rules
/// (e.g., `rkyv::ArchivedOption<U>` vs `Option<T>`).
pub trait OrdRepr<T: ?Sized> {
    fn ord_cmp(&self, other: &T) -> Ordering;
}

impl<T, U> OrdRepr<Option<T>> for ArchivedOption<U>
where
    U: PartialOrd<T>,
{
    fn ord_cmp(&self, other: &Option<T>) -> Ordering {
        match (self, other) {
            (ArchivedOption::None, None) => Ordering::Equal,
            (ArchivedOption::None, Some(_)) => Ordering::Less,
            (ArchivedOption::Some(_), None) => Ordering::Greater,
            (ArchivedOption::Some(a), Some(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
        }
    }
}

/// Forwards `OrdRepr<T>` to `PartialOrd<T>` for the named types.
///
/// We cannot blanket-impl `OrdRepr<T> for U where U: PartialOrd<T>` because
/// that would conflict with the manual impl for `ArchivedOption<U>` (Rust's
/// coherence checker treats them as potentially overlapping even though
/// `ArchivedOption<U>: PartialOrd<Option<T>>` is blocked by orphan rules).
#[macro_export]
macro_rules! impl_ord_repr_via_partial_ord {
    ($([$($generics:tt)*] $archived:ty as Repr<$orig:ty> $(where $($wh:tt)*)?),* $(,)?) => {$(
        impl<$($generics)*> $crate::dynamic::OrdRepr<$orig> for $archived
        where
            $archived: ::core::cmp::PartialOrd<$orig>,
            $($($wh)*)?
        {
            #[inline]
            fn ord_cmp(&self, other: &$orig) -> ::core::cmp::Ordering {
                <$archived as ::core::cmp::PartialOrd<$orig>>::partial_cmp(self, other)
                    .unwrap_or(::core::cmp::Ordering::Equal)
            }
        }
    )*};
}

/// Trait for DBData that can be deserialized with [`rkyv`].
///
/// The associated type `Repr` with the bound + connecting it to Archived
/// seems to be the key for rust to know the bounds exist globally in the code
/// without having to specify the bounds everywhere.
pub trait ArchivedDBData:
    for<'a> Serialize<DbspSerializer<'a>> + Archive<Archived = Self::Repr> + Sized
{
    type Repr: Deserialize<Self, Deserializer> + Ord + PartialEq<Self>;
}

/// We also automatically implement this bound for everything that satisfies it.
impl<T> ArchivedDBData for T
where
    T: Archive + for<'a> Serialize<DbspSerializer<'a>>,
    Archived<T>: Deserialize<T, Deserializer> + Ord + PartialEq<T>,
{
    type Repr = Archived<T>;
}

/// Object-safe version of the `Serialize` trait.
pub trait SerializeDyn {
    fn serialize(
        &self,
        serializer: &mut DbspSerializer,
    ) -> Result<usize, <DbspSerializer<'_> as Fallible>::Error>;
}

pub trait DeserializableDyn {
    /// Deserialize `self` from the given slice and offset.
    ///
    /// # Safety
    ///
    /// The offset must store a valid serialized value of the
    /// concrete type that `self` points to.
    unsafe fn deserialize_from_bytes_with(
        &mut self,
        bytes: &[u8],
        pos: usize,
        deserializer: &mut Deserializer,
    );

    /// Deserialize `self` from the given slice and offset using the default
    /// deserializer configuration.
    ///
    /// # Safety
    ///
    /// The offset must store a valid serialized value of the
    /// concrete type that `self` points to.
    unsafe fn deserialize_from_bytes(&mut self, bytes: &[u8], pos: usize) {
        let mut deserializer = Deserializer::default();
        unsafe { self.deserialize_from_bytes_with(bytes, pos, &mut deserializer) };
    }
}

impl<T> SerializeDyn for T
where
    T: ArchivedDBData,
{
    fn serialize(
        &self,
        serializer: &mut DbspSerializer,
    ) -> Result<usize, <DbspSerializer<'_> as Fallible>::Error> {
        rkyv::ser::Serializer::serialize_value(serializer, self)
    }
}
impl<T> DeserializableDyn for T
where
    T: ArchivedDBData,
{
    unsafe fn deserialize_from_bytes_with(
        &mut self,
        bytes: &[u8],
        pos: usize,
        deserializer: &mut Deserializer,
    ) {
        unsafe {
            let archived: &<Self as Archive>::Archived = archived_value::<Self>(bytes, pos);

            *self = archived.deserialize(deserializer).unwrap();
        }
    }
}

/// Object-safe version of the `Deserialize` trait.
pub trait DeserializeDyn<Trait: ?Sized>: AsAny + Comparable {
    fn deserialize_with(&self, target: &mut Trait, deserializer: &mut Deserializer);

    fn deserialize(&self, target: &mut Trait) {
        let mut deserializer = Deserializer::default();
        self.deserialize_with(target, &mut deserializer);
    }

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
    fn deserialize_with(&self, target: &mut Trait, deserializer: &mut Deserializer) {
        *unsafe { target.downcast_mut::<T>() } = self.archived.deserialize(deserializer).unwrap()
    }

    fn eq_target(&self, other: &Trait) -> bool {
        let mut deserializer = Deserializer::default();
        self.archived
            .deserialize(&mut deserializer)
            .unwrap()
            .eq(unsafe { other.downcast::<T>() })
    }

    fn cmp_target(&self, other: &Trait) -> Option<Ordering> {
        let mut deserializer = Deserializer::default();
        self.archived
            .deserialize(&mut deserializer)
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
