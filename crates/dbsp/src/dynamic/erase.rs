use crate::{dynamic::rkyv::ArchiveTrait, DBData};

/// Type erasure operations.
///
/// Given a trait object type `Trait` such that `Self: Trait`, converts
/// references to self into references to `Trait`.  The `Self: Trait` constraint
/// cannot be captured explicitly in the Rust type system, hence the need for this
/// trait.
pub trait Erase<Trait: ArchiveTrait + ?Sized> {
    /// Convert a reference to `self` into a reference to `Trait`.
    fn erase(&self) -> &Trait;

    /// Convert a mutable reference to `self` into a mutable reference to `Trait`.
    fn erase_mut(&mut self) -> &mut Trait;

    /// Convert `Box<Self>` into `Box<Trait>`.
    fn erase_box(self: Box<Self>) -> Box<Trait>;

    /// Convert a reference to an archived representation of `Self` into a reference
    /// to a trait object of type `Trait::Archived`.
    fn erase_archived(archived: &Self::Archived) -> &<Trait as ArchiveTrait>::Archived
    where
        Self: DBData;
}

#[macro_export]
macro_rules! derive_erase {
    (<$($generic:ident),*> $trait_bound:path
    { type Archived = dyn $archived_type:path }
    where
        $($bound:tt)*
    ) =>
    {
        impl<V, $($generic),*> $crate::dynamic::Erase<dyn $trait_bound> for V
        where
            V: $trait_bound + $crate::dynamic::ArchivedDBData,
            $crate::dynamic::DeserializeImpl::<V, dyn $trait_bound>: $archived_type,
            $($bound)*
        {
            fn erase(&self) -> &dyn $trait_bound {
                self
            }
            fn erase_mut(&mut self) -> &mut dyn $trait_bound {
                self
            }
            fn erase_box(self: Box<Self>) -> Box<dyn $trait_bound> {
                self
            }

            fn erase_archived(
                archived: &<Self as rkyv::Archive>::Archived,
            ) -> &<dyn $trait_bound as $crate::dynamic::ArchiveTrait>::Archived {
                $crate::dynamic::DeserializeImpl::<V, dyn $trait_bound>::new(archived)
            }
        }
    };
}
