use crate::{
    dynamic::{DataTrait, DowncastTrait, Erase, SerializeDyn},
    trace::Serializer,
    DBData,
};
use rkyv::{archived_value, with::Inline, Archive, Fallible, Serialize};
use std::{marker::PhantomData, mem::transmute};

/// An object-safe interface to types that represent (key, auxiliary data) pair.
///
/// This type is serialized with `rkyv` for each value in a data block.
///
/// Key and aux data are stored by reference, so they don't need to be cloned
/// in order to get serialized.
///
/// Note 1: `rkyv` can serialize arrays, slices, and vectors perfectly well on
/// its own. This crate serializes each value in a data block separately because
/// there is no way to accurately predict how big a serialized vector will be
/// without doing it.
///
/// Note 2: We could serialize key and value separately. This design reduces the
/// amount of metadata maintained in storage by only storing one offset per
/// item.
pub trait Item<'a, K, A>: SerializeDyn
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    /// Populate `self` with references to key and aux values.
    #[allow(clippy::wrong_self_convention)]
    fn from_refs(&mut self, key: &'a K, aux: &'a A);
}

/// An object-safe interface to an archived [`Item`].
pub trait ArchivedItem<'a, K, A>: 'a
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    /// Returns a reference to the archived key.
    fn fst(&self) -> &K::Archived;

    /// Returns a reference to the archived aux value.
    fn snd(&self) -> &A::Archived;

    /// Returns references to archived key and aux fields of the item.
    ///
    /// This is slightly more efficient than getting them individually,
    /// as this only requires one vtable access.
    fn split(&self) -> (&K::Archived, &A::Archived);

    /// Compare two archived items for equality.
    fn equal(&self, other: &dyn ArchivedItem<'a, K, A>) -> bool;
}

impl<'a, K, A> PartialEq for dyn ArchivedItem<'a, K, A>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    fn eq(&self, other: &Self) -> bool {
        self.equal(other)
    }
}

impl<'a, K, A> Eq for dyn ArchivedItem<'a, K, A>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
}

/// A factory trait that instantiates [`Item`]'s from either deserialized
/// or serialized representation of key/value pairs.
pub trait ItemFactory<K, A>: Send + Sync
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    /// Create a heap-allocated item from key and aux references.
    fn new_pair<'a>(&self, key: &'a K, aux: &'a A) -> Box<dyn Item<'a, K, A> + 'a>;

    /// Create an item from key and aux references on the stack and pass it as
    /// an argument to `f`.
    fn with<'a>(&self, key: &'a K, aux: &'a A, f: &mut dyn FnMut(&dyn Item<'a, K, A>));

    /// Casts an archived item from the given byte slice at the given position.
    ///
    /// # Safety
    ///
    /// The specified offset must contain an archived instance of [`Item`]
    /// backed by the same concrete key and aux types as `self`.
    unsafe fn archived_value<'a>(
        &self,
        bytes: &'a [u8],
        pos: usize,
    ) -> &'a dyn ArchivedItem<'a, K, A>;
}

/// Struct that implements the [`Item`] trait.
///
/// The definition of this struct makes it appear that the key precedes the
/// auxiliary data, but the Rust compiler can reorder struct members (in both
/// `Item` and [`ArchivedItem`]), so the serialized form might be in the reverse
/// order.
#[derive(Archive, Serialize)]
pub struct RefTup2<'a, K, A>(#[with(Inline)] pub &'a K, #[with(Inline)] pub &'a A)
where
    K: DBData,
    A: DBData;

impl<'a, K, A> SerializeDyn for RefTup2<'a, K, A>
where
    K: DBData,
    A: DBData,
{
    fn serialize(
        &self,
        serializer: &mut Serializer,
    ) -> Result<usize, <Serializer as Fallible>::Error> {
        rkyv::ser::Serializer::serialize_value(serializer, self)
    }
}

/*impl<'a, K, A> DeserializableDyn for RefTup2<'a, K, A>
where
    K: DBData,
    A: DBData,
{
    unsafe fn deserialize_from_bytes(&mut self, bytes: &[u8], pos: usize) {
        let archived: &<Self as Archive>::Archived = archived_value::<Self>(bytes, pos);

        *self.0 = archived.0.deserialize(&mut Infallible).unwrap();
        *self.1 = archived.1.deserialize(&mut Infallible).unwrap();
    }
}*/

impl<'a, K, A, KTrait, ATrait> Item<'a, KTrait, ATrait> for RefTup2<'a, K, A>
where
    K: DBData + Erase<KTrait>,
    A: DBData + Erase<ATrait>,
    KTrait: DataTrait + ?Sized,
    ATrait: DataTrait + ?Sized,
{
    fn from_refs(&mut self, key: &'a KTrait, aux: &'a ATrait) {
        self.0 = unsafe { key.downcast::<K>() };
        self.1 = unsafe { aux.downcast::<A>() };
    }
}

/// A struct that implements trait [`ArchivedItem`] for concrete
/// types `K` and `A` and trait object types `KTrait` and `ATrait`.
#[repr(transparent)]
struct Tup2Deserialize<'a, K, A, KTrait, ATrait>
where
    K: DBData + Erase<KTrait>,
    A: DBData + Erase<ATrait>,
    KTrait: DataTrait + ?Sized,
    ATrait: DataTrait + ?Sized,
{
    tup: ArchivedRefTup2<'a, K, A>,
    phantom: PhantomData<fn(&KTrait, &ATrait)>,
}

impl<'a, K, A, KTrait, ATrait> Tup2Deserialize<'a, K, A, KTrait, ATrait>
where
    K: DBData + Erase<KTrait>,
    A: DBData + Erase<ATrait>,
    KTrait: DataTrait + ?Sized,
    ATrait: DataTrait + ?Sized,
{
    fn new<'b>(tup: &'b ArchivedRefTup2<'a, K, A>) -> &'b Self {
        unsafe { transmute(tup) }
    }
}

impl<'a, K, A, KTrait, ATrait> ArchivedItem<'a, KTrait, ATrait>
    for Tup2Deserialize<'a, K, A, KTrait, ATrait>
where
    K: DBData + Erase<KTrait>,
    A: DBData + Erase<ATrait>,
    KTrait: DataTrait + ?Sized,
    ATrait: DataTrait + ?Sized,
{
    fn fst(&self) -> &KTrait::Archived {
        <K as Erase<KTrait>>::erase_archived(&self.tup.0)
    }

    fn snd(&self) -> &ATrait::Archived {
        <A as Erase<ATrait>>::erase_archived(&self.tup.1)
    }

    fn split(&self) -> (&KTrait::Archived, &ATrait::Archived) {
        (
            <K as Erase<KTrait>>::erase_archived(&self.tup.0),
            <A as Erase<ATrait>>::erase_archived(&self.tup.1),
        )
    }

    fn equal(&self, other: &dyn ArchivedItem<'a, KTrait, ATrait>) -> bool {
        let (other_k, other_a) = other.split();

        unsafe {
            &self.tup.0 == other_k.downcast::<K::Archived>()
                && &self.tup.1 == other_a.downcast::<A::Archived>()
        }
    }
}

/// An implementation of the [`ItemFactory`] trait that creates items
/// backed by the given concrete key and aux types.
pub struct RefTup2Factory<K, A>
where
    K: DBData,
    A: DBData,
{
    phantom: PhantomData<(K, A)>,
}

impl<K, A> RefTup2Factory<K, A>
where
    K: DBData,
    A: DBData,
{
}

unsafe impl<K, A> Send for RefTup2Factory<K, A>
where
    K: DBData,
    A: DBData,
{
}

unsafe impl<K, A> Sync for RefTup2Factory<K, A>
where
    K: DBData,
    A: DBData,
{
}

impl<K, A, KTrait, ATrait> ItemFactory<KTrait, ATrait> for RefTup2Factory<K, A>
where
    K: DBData + Erase<KTrait>,
    A: DBData + Erase<ATrait>,
    KTrait: DataTrait + ?Sized,
    ATrait: DataTrait + ?Sized,
{
    fn new_pair<'a>(
        &self,
        key: &'a KTrait,
        aux: &'a ATrait,
    ) -> Box<dyn Item<'a, KTrait, ATrait> + 'a> {
        unsafe { Box::new(RefTup2(key.downcast::<K>(), aux.downcast::<A>())) }
    }
    fn with<'a>(
        &self,
        key: &'a KTrait,
        aux: &'a ATrait,
        f: &mut dyn FnMut(&dyn Item<'a, KTrait, ATrait>),
    ) {
        unsafe { f(&RefTup2(key.downcast::<K>(), aux.downcast::<A>())) }
    }
    unsafe fn archived_value<'a>(
        &self,
        bytes: &'a [u8],
        pos: usize,
    ) -> &'a dyn ArchivedItem<'a, KTrait, ATrait> {
        let archived: &ArchivedRefTup2<'a, K, A> = archived_value::<RefTup2<'a, K, A>>(bytes, pos);
        Tup2Deserialize::new(archived)
    }
}

/// Trait for item factories that define a static instance.
pub trait WithItemFactory<KTrait, ATrait>: 'static
where
    KTrait: DataTrait + ?Sized,
    ATrait: DataTrait + ?Sized,
{
    /// Static instance of a factory that generates items with the
    /// given key and aux trait objects.
    const ITEM_FACTORY: &'static dyn ItemFactory<KTrait, ATrait>;
}

impl<K, A, KTrait, ATrait> WithItemFactory<KTrait, ATrait> for RefTup2Factory<K, A>
where
    KTrait: DataTrait + ?Sized,
    ATrait: DataTrait + ?Sized,
    K: DBData + Erase<KTrait>,
    A: DBData + Erase<ATrait>,
{
    const ITEM_FACTORY: &'static dyn ItemFactory<KTrait, ATrait> = &RefTup2Factory::<K, A> {
        phantom: PhantomData,
    };
}
