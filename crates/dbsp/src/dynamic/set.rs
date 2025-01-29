use super::{Data, DataTrait, Erase};
use crate::dynamic::arrow::ArrowSupportDyn;
use crate::utils::Tup2;
use crate::{declare_trait_object, trace::Deserializable, DBData};
use arrow::array::{ArrayBuilder, ArrayRef};
use rkyv::Archive;
use size_of::SizeOf;
use std::{
    collections::{btree_set::Iter as BTreeSetIter, BTreeSet},
    marker::PhantomData,
    mem::take,
    ops::{Deref, DerefMut},
};

/// A dynamically typed interface to `BTreeSet`.
pub trait Set<T: DataTrait + ?Sized>: Data {
    /// Return the number of elements in `self`.
    fn len(&self) -> usize;

    /// Check if the set is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clear the set, removing all elements.
    fn clear(&mut self);

    /// Add a value to the set without cloning.
    ///
    /// Sets `val` to the default value.
    fn insert_val(&mut self, val: &mut T);

    /// Add a value to the set, cloning it from `val`.
    fn insert_ref(&mut self, val: &T);

    /// Return an iterator over items in `self`.
    fn dyn_iter<'a>(&'a self) -> Box<dyn Iterator<Item = &'a T> + 'a>;

    /// Cast any trait object that implements this trait to `&DynSet`.
    ///
    /// This method will not be needed once trait downcasting has been stabilized.
    fn as_set(&self) -> &DynSet<T>;

    /// Cast any trait object that implements this trait to `&mut DynSet`.
    ///
    /// This method will not be needed once trait downcasting has been stabilized.
    fn as_set_mut(&mut self) -> &mut DynSet<T>;
}

pub trait SetTrait<T: DataTrait + ?Sized>: Set<T> + DataTrait {}

impl<V, T: DataTrait + ?Sized> SetTrait<T> for V where V: Set<T> + DataTrait {}

declare_trait_object!(DynSet<T> = dyn Set<T>
where
    T: DataTrait + ?Sized
);

/// Wrapper around BTreeSet whose archived representation implements the
/// required ordering traits.
#[derive(
    Debug,
    Hash,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Clone,
    Default,
    SizeOf,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub struct BSet<T>(BTreeSet<T>);

impl<T: ArrowSupportDyn> ArrowSupportDyn for BSet<T> {
    fn serialize_into_arrow_builder(&self, builder: &mut dyn ArrayBuilder) {
        unimplemented!()
    }

    fn deserialize_from_arrow(&mut self, array: &ArrayRef, index: usize) {
        unimplemented!()
    }
}

impl<T> Deref for BSet<T> {
    type Target = BTreeSet<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for BSet<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T> PartialEq for ArchivedBSet<T>
where
    T: Archive + Clone + Ord,
    <T as Archive>::Archived: Ord,
{
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}

impl<T> Eq for ArchivedBSet<T>
where
    T: Archive + Clone + Ord,
    <T as Archive>::Archived: Ord,
{
}

impl<T> PartialOrd for ArchivedBSet<T>
where
    T: Archive + Clone + Ord,
    <T as Archive>::Archived: Ord,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for ArchivedBSet<T>
where
    T: Archive + Clone + Ord,
    <T as Archive>::Archived: Ord,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

impl<T, Trait> Set<Trait> for BSet<T>
where
    Trait: DataTrait + ?Sized,
    T: DBData + Erase<Trait>,
    <T as Deserializable>::ArchivedDeser: Ord,
{
    fn len(&self) -> usize {
        BTreeSet::len(self)
    }

    fn clear(&mut self) {
        BTreeSet::clear(self)
    }

    fn insert_val(&mut self, val: &mut Trait) {
        let val = take(unsafe { val.downcast_mut::<T>() });

        self.insert(val);
    }

    fn insert_ref(&mut self, val: &Trait) {
        let val = unsafe { val.downcast::<T>() }.clone();

        self.insert(val);
    }

    fn dyn_iter<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Trait> + 'a> {
        Box::new(SetIter::new(self.iter()))
    }

    fn as_set(&self) -> &DynSet<Trait> {
        self
    }

    fn as_set_mut(&mut self) -> &mut DynSet<Trait> {
        self
    }
}

struct SetIter<'a, T, Trait: ?Sized> {
    iter: BTreeSetIter<'a, T>,
    phantom: PhantomData<&'a mut Trait>,
}

impl<'a, T, Trait: ?Sized> SetIter<'a, T, Trait> {
    fn new(iter: BTreeSetIter<'a, T>) -> Self {
        Self {
            iter,
            phantom: PhantomData,
        }
    }
}

impl<'a, T, Trait> Iterator for SetIter<'a, T, Trait>
where
    Trait: DataTrait + ?Sized,
    T: DBData + Erase<Trait>,
{
    type Item = &'a Trait;

    fn next(&mut self) -> Option<&'a Trait> {
        self.iter.next().map(|x| x.erase())
    }
}
