use std::{
    mem::swap,
    ops::{Index, IndexMut},
};

use super::{
    DataTrait, DowncastTrait, DynPair, DynPairs, Erase, LeanVec, Pairs, Vector, WeightTrait,
};
use crate::{
    declare_trait_object,
    utils::{consolidate_from, Tup2},
    DBData, DBWeight,
};

/// A vector of (key, value) pairs, where the value implements `WeightTrait`,
/// meaning that tuples with the same key can be consolidated by adding their weights.
/// Tuples that have zero weights after consolidation are removed from the vector.
pub trait WeightedPairs<T1: DataTrait + ?Sized, T2: WeightTrait + ?Sized>: Pairs<T1, T2> {
    fn consolidate(&mut self) {
        self.consolidate_from(0);
    }

    fn consolidate_from(&mut self, from: usize);

    #[allow(clippy::wrong_self_convention)]
    fn from_pairs(&mut self, pairs: &mut DynPairs<T1, T2>);
}

pub trait WeightedPairsTrait<T1: DataTrait + ?Sized, T2: WeightTrait + ?Sized>:
    WeightedPairs<T1, T2> + DataTrait
{
}

impl<V, T1: DataTrait + ?Sized, T2: WeightTrait + ?Sized> WeightedPairsTrait<T1, T2> for V where
    V: WeightedPairs<T1, T2> + DataTrait
{
}

declare_trait_object!(DynWeightedPairs<T1, T2> = dyn WeightedPairs<T1, T2>
where
    T1: DataTrait + ?Sized,
    T2: WeightTrait + ?Sized);

impl<T1: DataTrait + ?Sized, T2: WeightTrait + ?Sized> Index<usize> for DynWeightedPairs<T1, T2> {
    type Output = DynPair<T1, T2>;

    fn index(&self, index: usize) -> &Self::Output {
        Vector::index(self, index)
    }
}

impl<T1: DataTrait + ?Sized, T2: WeightTrait + ?Sized> IndexMut<usize>
    for DynWeightedPairs<T1, T2>
{
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        Vector::index_mut(self, index)
    }
}

impl<T1, T2, Trait1, Trait2> WeightedPairs<Trait1, Trait2> for LeanVec<Tup2<T1, T2>>
where
    Trait1: DataTrait + ?Sized,
    Trait2: WeightTrait + ?Sized,
    T1: DBData + Erase<Trait1>,
    T2: DBWeight + Erase<Trait2>,
{
    fn consolidate_from(&mut self, from: usize) {
        consolidate_from(self, from);
    }

    fn from_pairs(&mut self, pairs: &mut DynPairs<Trait1, Trait2>) {
        let pairs: &mut Self = unsafe { pairs.downcast_mut::<Self>() };

        swap(self, pairs);
    }
}
