use std::{
    mem::swap,
    ops::{Index, IndexMut},
};

use crate::{
    DBData, NumEntries, declare_trait_object,
    dynamic::erase::Erase,
    utils::{Tup2, stable_sort_by},
};

use super::{DataTrait, DynPair, LeanVec, Vector};

// TODO: `trait Slice`, move vector methods that operate on slices there.

/// A dynamically typed interface to `LeanVec<Tup2<_,_>>`
pub trait Pairs<T1: DataTrait + ?Sized, T2: DataTrait + ?Sized>: Vector<DynPair<T1, T2>> {
    /// Sort `self` by key.
    fn sort_by_key(&mut self);

    /// Removes all but the last of consecutive elements in the vector for each key.
    fn dedup_by_key_keep_last(&mut self);

    /// Push a new element; clone its first and second components from `item`.
    fn push_refs(&mut self, item: (&T1, &T2));

    /// Cast any trait object that implements this trait to `&DynPairs`.
    ///
    /// This method will not be needed once trait downcasting has been stabilized.
    fn as_pairs(&self) -> &DynPairs<T1, T2>;

    /// Cast any trait object that implements this trait to `&mut DynPairs`.
    ///
    /// This method will not be needed once trait downcasting has been stabilized.
    fn as_pairs_mut(&mut self) -> &mut DynPairs<T1, T2>;
}

pub trait PairsTrait<T1: DataTrait + ?Sized, T2: DataTrait + ?Sized>:
    Pairs<T1, T2> + DataTrait
{
}

impl<V, T1: DataTrait + ?Sized, T2: DataTrait + ?Sized> PairsTrait<T1, T2> for V where
    V: Pairs<T1, T2> + DataTrait
{
}

declare_trait_object!(DynPairs<T1, T2> = dyn Pairs<T1, T2>
where
    T1: DataTrait + ?Sized,
    T2: DataTrait + ?Sized
);

impl<T1: DataTrait + ?Sized, T2: DataTrait + ?Sized> Index<usize> for DynPairs<T1, T2> {
    type Output = DynPair<T1, T2>;

    fn index(&self, index: usize) -> &Self::Output {
        Vector::index(self, index)
    }
}

impl<T1: DataTrait + ?Sized, T2: DataTrait + ?Sized> IndexMut<usize> for DynPairs<T1, T2> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        Vector::index_mut(self, index)
    }
}

impl<T1: DataTrait + ?Sized, T2: DataTrait + ?Sized> NumEntries for DynPairs<T1, T2> {
    const CONST_NUM_ENTRIES: Option<usize> = None;

    fn num_entries_shallow(&self) -> usize {
        self.len()
    }

    fn num_entries_deep(&self) -> usize {
        self.len()
    }
}

/// Private interface
impl<T1, T2, Trait1, Trait2> Pairs<Trait1, Trait2> for LeanVec<Tup2<T1, T2>>
where
    T1: DBData + Erase<Trait1>,
    T2: DBData + Erase<Trait2>,
    Trait1: DataTrait + ?Sized,
    Trait2: DataTrait + ?Sized,
{
    fn dedup_by_key_keep_last(&mut self) {
        self.dedup_by(|Tup2(k1, v1), Tup2(k2, v2)| {
            if k1 == k2 {
                swap(v1, v2);
                true
            } else {
                false
            }
        });
    }

    fn sort_by_key(&mut self) {
        stable_sort_by(self.as_mut_slice(), |Tup2(k1, _v1), Tup2(k2, _v2)| {
            k1.cmp(k2)
        });
    }

    fn push_refs(&mut self, item: (&Trait1, &Trait2)) {
        unsafe {
            self.push(Tup2(
                item.0.downcast::<T1>().clone(),
                item.1.downcast::<T2>().clone(),
            ))
        }
    }

    fn as_pairs(&self) -> &DynPairs<Trait1, Trait2> {
        self
    }

    fn as_pairs_mut(&mut self) -> &mut DynPairs<Trait1, Trait2> {
        self
    }
}

#[cfg(test)]
mod test {
    use super::Pairs;
    use crate::{
        dynamic::{DynData, LeanVec},
        utils::Tup2,
    };

    type Keyed = LeanVec<Tup2<i32, i32>>;

    fn sort_and_dedup(pairs: Vec<Tup2<i32, i32>>) -> Vec<Tup2<i32, i32>> {
        let mut pairs = Keyed::from(pairs);
        <Keyed as Pairs<DynData, DynData>>::sort_by_key(&mut pairs);
        <Keyed as Pairs<DynData, DynData>>::dedup_by_key_keep_last(&mut pairs);
        pairs.as_slice().to_vec()
    }

    /// Sorting leaves a key's pairs adjacent in the order they were pushed, and
    /// deduping then keeps the last of them.
    ///
    /// Callers pair the two to reduce a run of updates to a key down to the one
    /// that survives, so which of the run is kept is the whole point.  The value
    /// counts the pair's position among its key's, so a kept pair says which.
    #[test]
    fn sorting_then_deduping_keeps_a_key_s_last_pair() {
        assert_eq!(
            sort_and_dedup(vec![
                Tup2(2, 0),
                Tup2(1, 0),
                Tup2(2, 1),
                Tup2(1, 1),
                Tup2(3, 0),
                Tup2(1, 2),
            ]),
            vec![Tup2(1, 2), Tup2(2, 1), Tup2(3, 0)]
        );
    }

    /// Deduping an empty vector, and one whose keys are already distinct, leaves
    /// them as they are.
    #[test]
    fn deduping_distinct_keys_changes_nothing() {
        assert_eq!(sort_and_dedup(Vec::new()), Vec::new());
        assert_eq!(
            sort_and_dedup(vec![Tup2(1, 10), Tup2(2, 20), Tup2(3, 30)]),
            vec![Tup2(1, 10), Tup2(2, 20), Tup2(3, 30)]
        );
    }

    /// Every pair sharing one key collapses to the last.
    #[test]
    fn deduping_one_repeated_key_keeps_its_last_pair() {
        assert_eq!(
            sort_and_dedup(vec![Tup2(7, 0), Tup2(7, 1), Tup2(7, 2), Tup2(7, 3)]),
            vec![Tup2(7, 3)]
        );
    }
}
