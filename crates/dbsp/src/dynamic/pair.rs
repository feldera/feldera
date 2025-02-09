use std::mem::take;

use crate::{
    declare_trait_object_with_archived, derive_comparison_traits, dynamic::erase::Erase,
    utils::Tup2, DBData,
};

use super::{Data, DataTrait, DowncastTrait};
use crate::utils::ArchivedTup2;

use crate::dynamic::rkyv::{ArchivedDBData, DeserializeDyn, DeserializeImpl};

/// A dynamically typed interface to `Tup2`.
pub trait Pair<T1: DataTrait + ?Sized, T2: DataTrait + ?Sized>: Data {
    /// Return a reference to the first component of the tuple.
    fn fst(&self) -> &T1;

    /// Return a reference to the second component of the tuple.
    fn snd(&self) -> &T2;

    /// Return a mutable reference to the first component of the tuple.
    fn fst_mut(&mut self) -> &mut T1;

    /// Return a mutable reference to the second component of the tuple.
    fn snd_mut(&mut self) -> &mut T2;

    /// Return a pair of references to the values stored in the tuple.
    fn split(&self) -> (&T1, &T2);

    /// Return a pair of mutable references to the values stored in the tuple.
    fn split_mut(&mut self) -> (&mut T1, &mut T2);

    /// Clone the values of `fst` anf `snd` to the first and second components of the tuple.
    #[allow(clippy::wrong_self_convention)]
    fn from_refs(&mut self, fst: &T1, snd: &T2);

    /// Move `fst` and `snd` into the first and second components of the tuple without cloning.
    /// Sets the contents of `fst` and `snd` to default values.
    #[allow(clippy::wrong_self_convention)]
    fn from_vals(&mut self, fst: &mut T1, snd: &mut T2);
}

impl<T1, T2, Trait1, Trait2> Pair<Trait1, Trait2> for Tup2<T1, T2>
where
    T1: DBData + Erase<Trait1>,
    T2: DBData + Erase<Trait2>,
    Trait1: DataTrait + ?Sized,
    Trait2: DataTrait + ?Sized,
{
    fn fst(&self) -> &Trait1 {
        self.fst().erase()
    }

    fn fst_mut(&mut self) -> &mut Trait1 {
        self.fst_mut().erase_mut()
    }

    fn snd(&self) -> &Trait2 {
        self.snd().erase()
    }

    fn snd_mut(&mut self) -> &mut Trait2 {
        self.snd_mut().erase_mut()
    }

    fn split(&self) -> (&Trait1, &Trait2) {
        (self.fst().erase(), self.snd().erase())
    }

    fn split_mut(&mut self) -> (&mut Trait1, &mut Trait2) {
        let (fst, snd) = self.as_mut();
        (fst.erase_mut(), snd.erase_mut())
    }

    fn from_refs(&mut self, fst: &Trait1, snd: &Trait2) {
        unsafe {
            *self.fst_mut() = fst.downcast::<T1>().clone();
            *self.snd_mut() = snd.downcast::<T2>().clone();
        }
    }

    fn from_vals(&mut self, fst: &mut Trait1, snd: &mut Trait2) {
        unsafe {
            *self.fst_mut() = take(fst.downcast_mut::<T1>());
            *self.snd_mut() = take(snd.downcast_mut::<T2>());
        }
    }
}

pub trait ArchivedPair<T1: DataTrait + ?Sized, T2: DataTrait + ?Sized> {
    fn fst(&self) -> &T1::Archived;
    fn snd(&self) -> &T2::Archived;
    fn split(&self) -> (&T1::Archived, &T2::Archived);
}

impl<T1, T2, Trait1, Trait2> ArchivedPair<Trait1, Trait2> for ArchivedTup2<T1, T2>
where
    T1: DBData + Erase<Trait1>,
    T2: DBData + Erase<Trait2>,
    Trait1: DataTrait + ?Sized,
    Trait2: DataTrait + ?Sized,
{
    fn fst(&self) -> &Trait1::Archived {
        <T1 as Erase<Trait1>>::erase_archived(self.fst())
    }

    fn snd(&self) -> &Trait2::Archived {
        <T2 as Erase<Trait2>>::erase_archived(self.snd())
    }

    fn split(&self) -> (&Trait1::Archived, &Trait2::Archived) {
        (
            <T1 as Erase<Trait1>>::erase_archived(self.fst()),
            <T2 as Erase<Trait2>>::erase_archived(self.snd()),
        )
    }
}

impl<T, Trait, Trait1, Trait2> ArchivedPair<Trait1, Trait2> for DeserializeImpl<T, Trait>
where
    T: ArchivedDBData + 'static,
    T::Archived: ArchivedPair<Trait1, Trait2> + Ord + Eq,
    Trait: Pair<Trait1, Trait2> + DowncastTrait + ?Sized + 'static,
    Trait1: DataTrait + ?Sized,
    Trait2: DataTrait + ?Sized,
{
    fn fst(&self) -> &Trait1::Archived {
        self.archived.fst()
    }

    fn snd(&self) -> &Trait2::Archived {
        self.archived.snd()
    }

    fn split(&self) -> (&Trait1::Archived, &Trait2::Archived) {
        (self.archived.fst(), self.archived.snd())
    }
}

pub trait ArchivedPairTrait<T1: DataTrait + ?Sized, T2: DataTrait + ?Sized>:
    ArchivedPair<T1, T2> + DeserializeDyn<DynPair<T1, T2>>
{
}

impl<Trait, T1, T2> ArchivedPairTrait<T1, T2> for Trait
where
    Trait: ArchivedPair<T1, T2> + DeserializeDyn<DynPair<T1, T2>> + ?Sized,
    T1: DataTrait + ?Sized,
    T2: DataTrait + ?Sized,
{
}

type DynArchivedPair<Trait1, Trait2> = dyn ArchivedPairTrait<Trait1, Trait2>;

derive_comparison_traits!(DynArchivedPair<Trait1, Trait2> where Trait1: DataTrait + ?Sized + 'static, Trait2: DataTrait + ?Sized + 'static);
impl<Trait1: ?Sized + 'static, Trait2: ?Sized + 'static> DowncastTrait
    for DynArchivedPair<Trait1, Trait2>
{
}

declare_trait_object_with_archived!(DynPair<T1, T2> = dyn Pair<T1, T2>
    { type Archived = dyn ArchivedPairTrait<T1, T2>}
    where
    T1: DataTrait + ?Sized,
    T2: DataTrait + ?Sized,
);
