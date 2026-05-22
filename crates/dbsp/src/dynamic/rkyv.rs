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
    U: OrdRepr<T>,
{
    fn ord_cmp(&self, other: &Option<T>) -> Ordering {
        match (self, other) {
            (ArchivedOption::None, None) => Ordering::Equal,
            (ArchivedOption::None, Some(_)) => Ordering::Less,
            (ArchivedOption::Some(_), None) => Ordering::Greater,
            (ArchivedOption::Some(a), Some(b)) => a.ord_cmp(b),
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

// Forward `OrdRepr<T>` to `PartialOrd<T>` for the rkyv primitives.
//
// rkyv's default (no `archive_le` / `archive_be` feature) keeps the archived
// form of every primitive equal to the primitive itself, so a single set of
// `OrdRepr<T> for T` impls covers them. We don't use a blanket impl because
// that would conflict with the manual `ArchivedOption<U>: OrdRepr<Option<T>>`
// impl above (Rust's coherence checker considers them potentially overlapping).
impl_ord_repr_via_partial_ord! {
    [] () as Repr<()>,
    [] bool as Repr<bool>,
    [] i8 as Repr<i8>,
    [] u8 as Repr<u8>,
    [] i16 as Repr<i16>,
    [] i32 as Repr<i32>,
    [] i64 as Repr<i64>,
    [] i128 as Repr<i128>,
    [] u16 as Repr<u16>,
    [] u32 as Repr<u32>,
    [] u64 as Repr<u64>,
    [] u128 as Repr<u128>,
    [] f32 as Repr<f32>,
    [] f64 as Repr<f64>,
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
        // Compare archived against target without deserializing.
        // `Self::Repr: PartialEq<Self>` is part of ArchivedDBData.
        self.archived.eq(unsafe { other.downcast::<T>() })
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

#[cfg(test)]
mod tests {
    use super::{ArchivedDBData, ArchivedOption, OrdRepr};
    use crate::storage::file::to_bytes;
    use crate::utils::{Tup1, Tup10};
    use rkyv::archived_root;
    use std::cmp::Ordering;

    fn archived_of<T: ArchivedDBData>(value: &T) -> rkyv::util::AlignedVec {
        let buf = to_bytes(value).unwrap();
        let mut aligned = rkyv::util::AlignedVec::new();
        aligned.extend_from_slice(&buf);
        aligned
    }

    fn with_archived<T: ArchivedDBData, F: FnOnce(&T::Repr)>(value: &T, f: F) {
        let bytes = archived_of(value);
        let archived = unsafe { archived_root::<T>(&bytes[..]) };
        f(archived);
    }

    #[test]
    fn cross_eq_primitives() {
        with_archived(&42i64, |arch| {
            assert!(arch.eq(&42i64));
            assert!(!arch.eq(&41i64));
        });
        with_archived(&7u32, |arch| {
            assert!(arch.eq(&7u32));
            assert!(!arch.eq(&8u32));
        });
        let s = String::from("hello");
        with_archived(&s, |arch| {
            assert!(arch.eq(s.as_str()));
            assert!(!arch.eq("world"));
        });
    }

    #[test]
    fn cross_ord_primitives() {
        with_archived(&42i64, |arch| {
            assert_eq!(arch.partial_cmp(&42i64), Some(Ordering::Equal));
            assert_eq!(arch.partial_cmp(&100i64), Some(Ordering::Less));
            assert_eq!(arch.partial_cmp(&0i64), Some(Ordering::Greater));
        });
        with_archived(&7u32, |arch| {
            assert_eq!(arch.partial_cmp(&7u32), Some(Ordering::Equal));
            assert_eq!(arch.partial_cmp(&8u32), Some(Ordering::Less));
        });
    }

    #[test]
    fn cross_eq_option_via_rkyv() {
        let some: Option<i64> = Some(5);
        with_archived(&some, |arch| {
            assert!(arch.eq(&Some(5i64)));
            assert!(!arch.eq(&Some(6i64)));
            assert!(!arch.eq(&Option::<i64>::None));
        });
        let none: Option<i64> = None;
        with_archived(&none, |arch| {
            assert!(arch.eq(&Option::<i64>::None));
            assert!(!arch.eq(&Some(0i64)));
        });
    }

    #[test]
    fn ord_repr_option_fills_rkyv_gap() {
        let some: Option<i64> = Some(5);
        with_archived(&some, |arch| {
            assert_eq!(OrdRepr::ord_cmp(arch, &Some(5i64)), Ordering::Equal);
            assert_eq!(OrdRepr::ord_cmp(arch, &Some(10i64)), Ordering::Less);
            assert_eq!(OrdRepr::ord_cmp(arch, &Some(1i64)), Ordering::Greater);
            assert_eq!(OrdRepr::ord_cmp(arch, &Option::<i64>::None), Ordering::Greater);
        });
        let none: Option<i64> = None;
        with_archived(&none, |arch| {
            assert_eq!(OrdRepr::ord_cmp(arch, &Option::<i64>::None), Ordering::Equal);
            assert_eq!(OrdRepr::ord_cmp(arch, &Some(0i64)), Ordering::Less);
        });
    }

    #[test]
    fn ord_repr_only_for_archived_option() {
        // Sanity check that the OrdRepr impl applies to the rkyv ArchivedOption,
        // not to bare ArchivedOption-shaped values. Uses a fresh archive.
        let some: Option<u32> = Some(99);
        let bytes = archived_of(&some);
        let arch: &ArchivedOption<rkyv::Archived<u32>> =
            unsafe { archived_root::<Option<u32>>(&bytes[..]) };
        assert_eq!(OrdRepr::ord_cmp(arch, &Some(100u32)), Ordering::Less);
    }

    #[test]
    fn cross_eq_ord_tup1_legacy_layout() {
        // Tup1 uses the legacy macro layout (size <= 8).
        let value = Tup1::new(123i64);
        with_archived(&value, |arch| {
            assert!(arch.eq(&Tup1::new(123i64)));
            assert!(!arch.eq(&Tup1::new(124i64)));
            assert_eq!(arch.partial_cmp(&Tup1::new(123i64)), Some(Ordering::Equal));
            assert_eq!(arch.partial_cmp(&Tup1::new(200i64)), Some(Ordering::Less));
            assert_eq!(arch.partial_cmp(&Tup1::new(0i64)), Some(Ordering::Greater));
        });
    }

    #[test]
    fn cross_eq_ord_tup10_v4_layout() {
        // Tup10 uses the v4 macro layout (size > 8); fields go through IsNone semantics.
        let base = Tup10::new(
            1i64, 2u32, 3i16, 4u8, 5i8, 6u16, 7i32, 8u64, 9i128, 10u128,
        );
        with_archived(&base, |arch| {
            let same = Tup10::new(
                1i64, 2u32, 3i16, 4u8, 5i8, 6u16, 7i32, 8u64, 9i128, 10u128,
            );
            let bigger_last = Tup10::new(
                1i64, 2u32, 3i16, 4u8, 5i8, 6u16, 7i32, 8u64, 9i128, 11u128,
            );
            let smaller_first = Tup10::new(
                0i64, 2u32, 3i16, 4u8, 5i8, 6u16, 7i32, 8u64, 9i128, 10u128,
            );
            assert!(arch.eq(&same));
            assert!(!arch.eq(&bigger_last));
            assert_eq!(arch.partial_cmp(&same), Some(Ordering::Equal));
            assert_eq!(arch.partial_cmp(&bigger_last), Some(Ordering::Less));
            assert_eq!(arch.partial_cmp(&smaller_first), Some(Ordering::Greater));
        });
    }
}
