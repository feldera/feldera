use std::{
    cmp::Ordering,
    marker::PhantomData,
    mem::take,
    ops::{Deref, DerefMut, Index, IndexMut},
};

use rand::RngCore;

use crate::{
    declare_trait_object,
    dynamic::DataTraitTyped,
    utils::{
        dyn_advance, dyn_retreat, stable_sort, stable_sort_by, {self},
    },
    DBData,
};

use super::{Data, DataTrait, DowncastTrait, Erase, LeanVec, RawIter};

// TODO: `trait Slice`, move vector methods that operate on slices there.

/// A dynamically typed interface to `LeanVec`
pub trait Vector<T: DataTrait + ?Sized>: Data {
    /// Return the length of the vector.
    fn len(&self) -> usize;

    /// Return the total number of elements the vector can hold without reallocating.s
    fn capacity(&self) -> usize;

    /// Return the remaining spare capacity of the vector.
    fn spare_capacity(&self) -> usize {
        self.capacity() - self.len()
    }

    /// Check whether `self` has spare capacity for at least one element.
    fn has_spare_capacity(&self) -> bool {
        self.spare_capacity() > 0
    }

    /// Return `true` if the vector contains no elements.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clear the vector, removing all values.
    fn clear(&mut self);

    /// Shrink the capacity of the vector as much as possible.
    fn shrink_to_fit(&mut self);

    /// Append an element to the back of the vector without cloning.
    ///
    /// Sets `val` to the default value
    fn push_val(&mut self, val: &mut T);

    /// Append an element to the back of the vector.
    fn push_ref(&mut self, val: &T);

    /// Append an element to the back of the vector; call `f` to initialize the new element.
    fn push_with(&mut self, f: &mut dyn FnMut(&mut T));

    /// Return a reference to the element at `index`; panic if `index >= self.len()`.
    fn index(&self, index: usize) -> &T;

    /// Return a reference to the element at `index`, eliding bounds checks.
    ///
    /// # Safety
    ///
    /// `index` must be within bounds.
    unsafe fn index_unchecked(&self, index: usize) -> &T;

    /// Return a mutable reference to the element at `index`; panic if `index >= self.len()`.
    fn index_mut(&mut self, index: usize) -> &mut T;

    /// Return a mutable reference to an element at `index`, eliding bounds
    /// checks.
    ///
    /// # Safety
    ///
    /// `index` must be within bounds.
    unsafe fn index_mut_unchecked(&mut self, index: usize) -> &mut T;

    /// Swap value at indexes `a` and `b`.
    fn swap(&mut self, a: usize, b: usize);

    /// Reserve capacity for at least `additional` more elements.
    ///
    /// May reserve more space to speculatively avoid frequent reallocations.  After calling
    /// `reserve`, capacity will be greater than or equal to `self.len() + additional`.
    /// Does nothing if capacity is already sufficient.
    fn reserve(&mut self, additional: usize);

    /// Append values in the range `from..to` in `other` to `self`.
    fn extend_from_range(&mut self, other: &DynVec<T>, from: usize, to: usize);

    /// Append the entire contents of `other` to `self`.
    ///
    /// Values in `other` are moved rather than cloned into `self`, leaving `other`
    /// empty.  The `capacity` of `other` is not affected by this function.
    fn append(&mut self, other: &mut DynVec<T>);

    /// Append the values in the range `from..to` in `other` to `self`.
    ///
    /// Values are moved rather than cloned, leaving default values in `other`.
    fn append_range(&mut self, other: &mut DynVec<T>, from: usize, to: usize);

    /// Return the number of elements in the `from..to` range where the predicate is true.
    ///
    /// Assumes that once the predicate doesn't hold for a value in the range, it
    /// also doesn't hold for all subsequent values.
    fn advance_while(&self, from: usize, to: usize, predicate: &dyn Fn(&T) -> bool) -> usize;

    /// Return the number of elements in the `from..to` range where the predicate is false.
    ///
    /// Assumes that once the predicate is `true` for a value in the range, it
    /// also holds for all subsequent values.
    fn advance_until(&self, from: usize, to: usize, predicate: &dyn Fn(&T) -> bool) -> usize;

    /// Return the number of elements in the `from..to` range that are smaller than `val`.
    ///
    /// Assumes that the vector is sorted in the ascending order.
    fn advance_to(&self, from: usize, to: usize, val: &T) -> usize;

    /// Returns the number of elements in the `from..=to` range where the predicate evaluates to
    /// true.
    ///
    /// Assumes that once the predicate doesn't hold for an index `i`, it also doesn't hold for
    /// any `j < i`.
    fn retreat_while(&self, from: usize, to: usize, predicate: &dyn Fn(&T) -> bool) -> usize;

    /// Returns the number of elements in the `from..=to` range where the predicate evaluates to
    /// false.
    ///
    /// Assumes that once the predicate holds for an index `i`, it also holds for
    /// any `j < i`.
    fn retreat_until(&self, from: usize, to: usize, predicate: &dyn Fn(&T) -> bool) -> usize;

    /// Returns the number of elements in the `from..=to` range that are greater than `val`.
    ///
    /// Assumes that the vector is sorted in the ascending order.
    fn retreat_to(&self, from: usize, to: usize, val: &T) -> usize;

    /// Forces the length of the vector to `new_len`.
    ///
    /// This is a low-level operation that maintains none of the normal
    /// invariants of the type. Normally changing the length of a vector is
    /// done using one of the safe operations instead, such as truncate,
    /// resize, extend, or clear.
    ///
    /// # Safety
    ///
    /// `new_len` must be less than or equal to `capacity()`.
    /// The elements at `old_len..new_len` must be initialized.
    unsafe fn set_len(&mut self, len: usize);

    /// Shortens the vector, keeping the first len elements and dropping the rest.
    ///
    /// If len is greater or equal to the vector’s current length, this has no effect.
    /// Note that this method has no effect on the allocated capacity of the vector.
    fn truncate(&mut self, len: usize);

    /// Sort the vector using a stable sorting algorithm.
    fn sort(&mut self) {
        self.sort_slice(0, self.len());
    }

    /// Sort a range of the vector using a stable sorting algorithm.
    fn sort_slice(&mut self, from: usize, to: usize);

    /// Sort the vector using an unstable sorting algorithm.
    fn sort_unstable(&mut self) {
        self.sort_slice_unstable(0, self.len())
    }

    /// Sort a range of the vector using an unstable sort algorithm.
    fn sort_slice_unstable(&mut self, from: usize, to: usize);

    /// Sort a range of the vector with a comparator function using a stable sort algorithm.
    fn sort_slice_by(&mut self, from: usize, to: usize, cmp: &dyn Fn(&T, &T) -> Ordering);

    /// Sort the vector with a comparator function using an unstable sort algorithm.
    fn sort_unstable_by(&mut self, cmp: &dyn Fn(&T, &T) -> Ordering) {
        self.sort_slice_unstable_by(0, self.len(), cmp)
    }

    /// Sort a range of the vector with a comparator function using an unstable sort algorithm.
    fn sort_slice_unstable_by(&mut self, from: usize, to: usize, cmp: &dyn Fn(&T, &T) -> Ordering);

    /// Check if the vector is sorted according to the ordering induced by `compare`.
    fn is_sorted_by(&self, compare: &dyn Fn(&T, &T) -> Ordering) -> bool;

    /// Remove all but the first of equal consecutive elements in the vector.
    fn dedup(&mut self);

    /// Return a read-only iterator over the vector.
    fn dyn_iter<'a>(&'a self) -> Box<dyn DoubleEndedIterator<Item = &T> + 'a>;

    /// Return a mutable iterator over the vector.
    fn dyn_iter_mut<'a>(&'a mut self) -> Box<dyn DoubleEndedIterator<Item = &mut T> + 'a>;

    /// Compute a uniform random sample of a range of the vector.
    fn sample_slice(
        &self,
        from: usize,
        to: usize,
        rng: &mut dyn RngCore,
        sample_size: usize,
        sample: &mut DynVec<T>,
    );

    /// Cast any trait object that implements this trait to `&DynVec`.
    ///
    /// This method will not be needed once trait downcasting has been stabilized.
    fn as_vec(&self) -> &DynVec<T>;

    /// Cast any trait object that implements this trait to `&mut DynVec`.
    ///
    /// This method will not be needed once trait downcasting has been stabilized.
    fn as_vec_mut(&mut self) -> &mut DynVec<T>;
}

pub trait VecTrait<T: DataTrait + ?Sized>: Vector<T> + DataTrait {}

impl<V, T: DataTrait + ?Sized> VecTrait<T> for V where V: Vector<T> + DataTrait {}

declare_trait_object!(DynVec<T> = dyn Vector<T>
where
    T: DataTrait + ?Sized
);

impl<T: DataTraitTyped + ?Sized> Deref for DynVec<T> {
    type Target = LeanVec<T::Type>;

    fn deref(&self) -> &Self::Target {
        // Safety: this is safe assuming `LeanVec` is the only type that implements
        // `trait Vector`.
        unsafe { self.downcast::<Self::Target>() }
    }
}

impl<T: DataTraitTyped + ?Sized> DerefMut for DynVec<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // Safety: this is safe assuming `LeanVec` is the only type that implements
        // `trait Vector`.
        unsafe { self.downcast_mut::<Self::Target>() }
    }
}

impl<T: DataTrait + ?Sized> Index<usize> for DynVec<T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        Vector::index(self, index)
    }
}

impl<T: DataTrait + ?Sized> IndexMut<usize> for DynVec<T> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        Vector::index_mut(self, index)
    }
}

impl<T, Trait> Vector<Trait> for LeanVec<T>
where
    Trait: DataTrait + ?Sized,
    T: DBData + Erase<Trait>,
{
    fn len(&self) -> usize {
        LeanVec::len(self)
    }

    fn capacity(&self) -> usize {
        LeanVec::capacity(self)
    }

    fn clear(&mut self) {
        LeanVec::clear(self)
    }

    fn shrink_to_fit(&mut self) {
        todo!()
    }

    fn push_val(&mut self, val: &mut Trait) {
        let val = take(unsafe { val.downcast_mut::<T>() });

        LeanVec::push(self, val);
    }

    fn push_ref(&mut self, val: &Trait) {
        let val = unsafe { val.downcast::<T>() }.clone();

        LeanVec::push(self, val);
    }

    fn push_with(&mut self, f: &mut dyn FnMut(&mut Trait)) {
        let mut val: T = Default::default();
        f(val.erase_mut());

        LeanVec::push(self, val);
    }

    fn index(&self, index: usize) -> &Trait {
        LeanVec::get(self, index).erase()
    }

    fn index_mut(&mut self, index: usize) -> &mut Trait {
        LeanVec::get_mut(self, index).erase_mut()
    }

    unsafe fn index_unchecked(&self, index: usize) -> &Trait {
        LeanVec::get_unchecked(self, index).erase()
    }

    unsafe fn index_mut_unchecked(&mut self, index: usize) -> &mut Trait {
        LeanVec::get_mut_unchecked(self, index).erase_mut()
    }

    fn swap(&mut self, a: usize, b: usize) {
        // TODO: implement `swap` as a method of `LeanVec` to reduce code size.
        self.as_mut_slice().swap(a, b)
    }

    fn reserve(&mut self, reservation: usize) {
        LeanVec::reserve(self, reservation)
    }

    fn extend_from_range(&mut self, other: &DynVec<Trait>, from: usize, to: usize) {
        let other = unsafe { other.downcast::<Self>() };

        LeanVec::extend_from_slice(self, &other[from..to])
    }

    fn append(&mut self, other: &mut DynVec<Trait>) {
        let other = unsafe { other.downcast_mut::<Self>() };

        LeanVec::append(self, other);
    }

    fn append_range(&mut self, other: &mut DynVec<Trait>, from: usize, to: usize) {
        let other = unsafe { other.downcast_mut::<Self>() };

        LeanVec::append_from_slice(self, &mut other[from..to])
    }

    fn advance_while(&self, from: usize, to: usize, predicate: &dyn Fn(&Trait) -> bool) -> usize {
        dyn_advance(&self[from..to], &|val| {
            predicate(unsafe { &*(val as *const T) }.erase())
        })
    }

    fn advance_until(&self, from: usize, to: usize, predicate: &dyn Fn(&Trait) -> bool) -> usize {
        dyn_advance(&self[from..to], &|val| {
            !predicate(unsafe { &*(val as *const T) }.erase())
        })
    }

    fn advance_to(&self, from: usize, to: usize, val: &Trait) -> usize {
        let val = unsafe { val.downcast::<T>() };

        dyn_advance(&self[from..to], &|x| unsafe { &*(x as *const T) } < val)
    }

    fn retreat_while(&self, from: usize, to: usize, predicate: &dyn Fn(&Trait) -> bool) -> usize {
        dyn_retreat(&self[from..=to], &|val| {
            predicate(unsafe { &*(val as *const T) }.erase())
        })
    }

    fn retreat_until(&self, from: usize, to: usize, predicate: &dyn Fn(&Trait) -> bool) -> usize {
        dyn_retreat(&self[from..=to], &|val| {
            !predicate(unsafe { &*(val as *const T) }.erase())
        })
    }

    fn retreat_to(&self, from: usize, to: usize, val: &Trait) -> usize {
        let val = unsafe { val.downcast::<T>() };

        dyn_retreat(&self[from..=to], &|x| unsafe { &*(x as *const T) } > val)
    }

    unsafe fn set_len(&mut self, len: usize) {
        LeanVec::set_len(self, len)
    }

    fn truncate(&mut self, len: usize) {
        LeanVec::truncate(self, len)
    }

    fn sort_slice(&mut self, from: usize, to: usize) {
        //self[from..to].sort()
        stable_sort(&mut self[from..to]);
    }

    fn sort_slice_by(&mut self, from: usize, to: usize, cmp: &dyn Fn(&Trait, &Trait) -> Ordering) {
        stable_sort_by(&mut self[from..to], |x, y| cmp(x.erase(), y.erase()))
    }

    fn sort_slice_unstable(&mut self, from: usize, to: usize) {
        // FIXME: The unstable sort implementation in `utils/sort.rs` requires additional testing
        // and benchmarking.  Use stable sort for now.
        stable_sort(&mut self[from..to]);

        //self[from..to].sort_unstable()
    }

    fn sort_slice_unstable_by(
        &mut self,
        from: usize,
        to: usize,
        cmp: &dyn Fn(&Trait, &Trait) -> Ordering,
    ) {
        // FIXME: The unstable sort implementation in `utils/sort.rs` requires additional testing
        // and benchmarking.  Use stable sort for now.
        stable_sort_by(&mut self[from..to], |x, y| cmp(x.erase(), y.erase()))
    }

    fn dedup(&mut self) {
        LeanVec::dedup(self);
    }
    fn dyn_iter<'a>(&'a self) -> Box<dyn DoubleEndedIterator<Item = &Trait> + 'a> {
        Box::new(VecIter::new(self))
    }

    fn dyn_iter_mut<'a>(&'a mut self) -> Box<dyn DoubleEndedIterator<Item = &mut Trait> + 'a> {
        Box::new(VecIterMut::new(self))
    }

    fn sample_slice(
        &self,
        from: usize,
        to: usize,
        rng: &mut dyn RngCore,
        sample_size: usize,
        sample: &mut DynVec<Trait>,
    ) {
        let sample: &mut Self = unsafe { sample.downcast_mut::<Self>() };

        utils::sample_slice(&self[from..to], rng, sample_size, sample);
    }

    fn as_vec(&self) -> &DynVec<Trait> {
        self
    }

    fn as_vec_mut(&mut self) -> &mut DynVec<Trait> {
        self
    }

    fn is_sorted_by(&self, compare: &dyn Fn(&Trait, &Trait) -> Ordering) -> bool {
        LeanVec::is_sorted_by(self, |x, y| compare(x.erase(), y.erase()))
    }
}

struct VecIter<'a, T, Trait: ?Sized> {
    iter: RawIter<'a>,
    phantom: PhantomData<(&'a T, &'a Trait)>,
}

impl<'a, T, Trait: ?Sized> VecIter<'a, T, Trait> {
    fn new(vec: &'a LeanVec<T>) -> Self {
        Self {
            iter: vec.raw_iter(),
            phantom: PhantomData,
        }
    }
}

impl<'a, T, Trait: DataTrait + ?Sized> Iterator for VecIter<'a, T, Trait>
where
    T: Erase<Trait>,
{
    type Item = &'a Trait;

    fn next(&mut self) -> Option<&'a Trait> {
        self.iter
            .next()
            .map(|x| unsafe { &*(x as *const T) }.erase())
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }

    fn count(self) -> usize
    where
        Self: Sized,
    {
        self.iter.count()
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        self.iter
            .nth(n)
            .map(|x| unsafe { &*(x as *const T) }.erase())
    }

    fn last(self) -> Option<Self::Item>
    where
        Self: Sized,
    {
        self.iter
            .last()
            .map(|x| unsafe { &*(x as *const T) }.erase())
    }
}

impl<'a, T, Trait: DataTrait + ?Sized> DoubleEndedIterator for VecIter<'a, T, Trait>
where
    T: Erase<Trait>,
{
    fn next_back(&mut self) -> Option<&'a Trait> {
        self.iter
            .next_back()
            .map(|x| unsafe { &*(x as *const T) }.erase())
    }

    fn nth_back(&mut self, n: usize) -> Option<Self::Item> {
        self.iter
            .nth_back(n)
            .map(|x| unsafe { &*(x as *const T) }.erase())
    }
}

struct VecIterMut<'a, T, Trait: ?Sized> {
    iter: RawIter<'a>,
    phantom: PhantomData<(&'a mut T, &'a Trait)>,
}

impl<'a, T, Trait: ?Sized> VecIterMut<'a, T, Trait> {
    fn new(vec: &'a mut LeanVec<T>) -> Self {
        Self {
            iter: vec.raw_iter(),
            phantom: PhantomData,
        }
    }
}

impl<'a, T, Trait: DataTrait + ?Sized> Iterator for VecIterMut<'a, T, Trait>
where
    T: Erase<Trait>,
{
    type Item = &'a mut Trait;

    fn next(&mut self) -> Option<&'a mut Trait> {
        self.iter
            .next()
            .map(|x| unsafe { &mut *(x as *mut T) }.erase_mut())
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }

    fn count(self) -> usize
    where
        Self: Sized,
    {
        self.iter.count()
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        self.iter
            .nth(n)
            .map(|x| unsafe { &mut *(x as *mut T) }.erase_mut())
    }

    fn last(self) -> Option<Self::Item>
    where
        Self: Sized,
    {
        self.iter
            .last()
            .map(|x| unsafe { &mut *(x as *mut T) }.erase_mut())
    }
}

impl<'a, T, Trait: DataTrait + ?Sized> DoubleEndedIterator for VecIterMut<'a, T, Trait>
where
    T: Erase<Trait>,
{
    fn next_back(&mut self) -> Option<&'a mut Trait> {
        self.iter
            .next_back()
            .map(|x| unsafe { &mut *(x as *mut T) }.erase_mut())
    }

    fn nth_back(&mut self, n: usize) -> Option<Self::Item> {
        self.iter
            .nth_back(n)
            .map(|x| unsafe { &mut *(x as *mut T) }.erase_mut())
    }
}

#[cfg(test)]
mod test {
    use crate::{
        dynamic::{DowncastTrait, DynData, DynVec, LeanVec, WithFactory},
        lean_vec,
    };

    #[test]
    fn dyn_vec_test() {
        let factory = <DynVec<DynData> as WithFactory<LeanVec<String>>>::FACTORY;

        let mut vec = factory.default_box();

        vec.push_val(&mut "foo".to_string());
        vec.push_ref(&"bar".to_string());
        let contents = vec
            .dyn_iter()
            .map(|x| x.downcast_checked::<String>().clone())
            .collect::<Vec<String>>();

        assert_eq!(
            LeanVec::from(contents),
            lean_vec!["foo".to_string(), "bar".to_string()]
        );
    }
}
