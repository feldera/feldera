use std::mem::take;

use crate::{
    DBData, declare_trait_object,
    dynamic::erase::Erase,
    utils::Tup2,
};

use super::{Data, DataTrait};

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
        self.0.erase()
    }

    fn fst_mut(&mut self) -> &mut Trait1 {
        self.0.erase_mut()
    }

    fn snd(&self) -> &Trait2 {
        self.1.erase()
    }

    fn snd_mut(&mut self) -> &mut Trait2 {
        self.1.erase_mut()
    }

    fn split(&self) -> (&Trait1, &Trait2) {
        (self.0.erase(), self.1.erase())
    }

    fn split_mut(&mut self) -> (&mut Trait1, &mut Trait2) {
        (self.0.erase_mut(), self.1.erase_mut())
    }

    fn from_refs(&mut self, fst: &Trait1, snd: &Trait2) {
        unsafe {
            self.0 = fst.downcast::<T1>().clone();
            self.1 = snd.downcast::<T2>().clone();
        }
    }

    fn from_vals(&mut self, fst: &mut Trait1, snd: &mut Trait2) {
        unsafe {
            self.0 = take(fst.downcast_mut::<T1>());
            self.1 = take(snd.downcast_mut::<T2>());
        }
    }
}

declare_trait_object!(DynPair<T1, T2> = dyn Pair<T1, T2>
where
    T1: DataTrait + ?Sized,
    T2: DataTrait + ?Sized,
);
