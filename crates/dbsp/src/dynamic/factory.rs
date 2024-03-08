use crate::{
    dynamic::{erase::Erase, ArchiveTrait},
    DBData,
};
use rkyv::archived_value;
use std::{marker::PhantomData, mem};

/// Create instances of a concrete type wrapped in a trait object.
pub trait Factory<Trait: ArchiveTrait + ?Sized>: Send + Sync {
    /// Size of the underlying concrete type.
    fn size_of(&self) -> usize;

    /// True iff the underlying concrete type is a zero-sized type.
    fn is_zst(&self) -> bool {
        self.size_of() == 0
    }

    /// Create an instance of the underlying concrete type with the default value on the heap.
    fn default_box(&self) -> Box<Trait>;

    /// Creates an instance of the underlying concrete type with the default value on the stack
    /// and passes it as an argument to the provided closure.
    fn with(&self, f: &mut dyn FnMut(&mut Trait));

    /// Casts an archived value from the given byte slice at the given position.
    ///
    /// # Safety
    ///
    /// The specified offset must contain an archived instance of the concrete
    /// type that this factory manages.
    unsafe fn archived_value<'a>(&self, bytes: &'a [u8], pos: usize) -> &'a Trait::Archived;
}

struct FactoryImpl<T, Trait: ?Sized> {
    phantom: PhantomData<fn(&T, &Trait)>,
}

/// Trait for trait objects that can be created from instances of a concrete type `T`.
pub trait WithFactory<T>: 'static {
    /// A factory that creates trait objects of type `Self`, backed by concrete values
    /// of type `T`.
    const FACTORY: &'static dyn Factory<Self>;
}

impl<T, Trait> Factory<Trait> for FactoryImpl<T, Trait>
where
    T: DBData + Erase<Trait> + 'static,
    Trait: ArchiveTrait + ?Sized,
{
    fn default_box(&self) -> Box<Trait> {
        Box::<T>::default().erase_box()
    }

    fn size_of(&self) -> usize {
        mem::size_of::<T>()
    }

    fn with(&self, f: &mut dyn FnMut(&mut Trait)) {
        f(T::default().erase_mut())
    }

    unsafe fn archived_value<'a>(&self, bytes: &'a [u8], pos: usize) -> &'a Trait::Archived {
        let archived: &T::Archived = archived_value::<T>(bytes, pos);
        <T as Erase<Trait>>::erase_archived(archived)
    }
}

pub trait AnyFactory {
    fn downcast<Trait: ArchiveTrait + ?Sized>(&self) -> Option<&dyn Factory<Trait>>;
}

impl<T, Trait> WithFactory<T> for Trait
where
    Trait: ArchiveTrait + ?Sized + 'static,
    T: DBData + Erase<Trait>,
{
    const FACTORY: &'static dyn Factory<Self> = &FactoryImpl::<T, Self> {
        phantom: PhantomData,
    };
}
