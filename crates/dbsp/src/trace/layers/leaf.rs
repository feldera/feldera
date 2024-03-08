use crate::{
    algebra::{NegByRef, ZRingValue},
    dynamic::{DataTrait, DynPair, DynVec, Factory, WeightTrait, WeightTraitTyped, WithFactory},
    utils::cursor_position_oob,
    DBData, DBWeight, NumEntries,
};
use rand::Rng;

use crate::{
    algebra::{AddAssignByRef, AddByRef},
    dynamic::{Erase, LeanVec},
    utils::Tup2,
};
use size_of::SizeOf;
use std::{
    cmp::{min, Ordering},
    fmt::{self, Debug, Display},
    ops::{Add, AddAssign, Deref, DerefMut, Neg},
};

use super::{Builder, Cursor, MergeBuilder, Trie, TupleBuilder};

// TODO: the `diff` type is fixed in most use cases (the `weighted` operator
// being the only exception I can think of, so it will probably pay off to have
// a verson of this with a statically typed diff type).
pub struct LeafFactories<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> {
    pub key: &'static dyn Factory<K>,
    pub keys: &'static dyn Factory<DynVec<K>>,
    pub diff: &'static dyn Factory<R>,
    pub diffs: &'static dyn Factory<DynVec<R>>,
    pub paired: &'static dyn Factory<DynPair<K, R>>,
}

impl<K, R> LeafFactories<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    pub fn new<KType, RType>() -> Self
    where
        KType: DBData + Erase<K>,
        RType: DBWeight + Erase<R>,
    {
        Self {
            key: WithFactory::<KType>::FACTORY,
            keys: WithFactory::<LeanVec<KType>>::FACTORY,
            diff: WithFactory::<RType>::FACTORY,
            diffs: WithFactory::<LeanVec<RType>>::FACTORY,
            paired: WithFactory::<Tup2<KType, RType>>::FACTORY,
        }
    }
}

impl<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> Clone for LeafFactories<K, R> {
    fn clone(&self) -> Self {
        Self {
            key: self.key,
            keys: self.keys,
            diff: self.diff,
            diffs: self.diffs,
            paired: self.paired,
        }
    }
}

#[derive(SizeOf)]
pub struct Leaf<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> {
    #[size_of(skip)]
    pub(crate) factories: LeafFactories<K, R>,
    pub(crate) keys: Box<DynVec<K>>,
    pub(crate) diffs: Box<DynVec<R>>,
    lower_bound: usize,
}

impl<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> Clone for Leaf<K, R> {
    fn clone(&self) -> Self {
        Self {
            factories: self.factories.clone(),
            keys: self.keys.clone(),
            diffs: self.diffs.clone(),
            lower_bound: self.lower_bound,
        }
    }
}

impl<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> Debug for Leaf<K, R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut map = f.debug_map();
        for idx in 0..self.len() {
            map.entry(&&self.keys[idx], &&self.diffs[idx]);
        }

        map.finish()
    }
}

impl<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> Display for Leaf<K, R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.cursor(), f)
    }
}

impl<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> Leaf<K, R> {
    pub fn new(factories: &LeafFactories<K, R>) -> Self {
        Self {
            keys: factories.keys.default_box(),
            diffs: factories.diffs.default_box(),
            factories: factories.clone(),
            lower_bound: 0,
        }
    }

    pub fn with_capacity(factories: &LeafFactories<K, R>, capacity: usize) -> Self {
        let mut result = Self {
            keys: factories.keys.default_box(),
            diffs: factories.diffs.default_box(),
            factories: factories.clone(),
            lower_bound: 0,
        };

        result.keys.reserve(capacity);
        result.diffs.reserve(capacity);

        result
    }

    pub(crate) fn columns_mut(&mut self) -> (&mut DynVec<K>, &mut DynVec<R>) {
        (self.keys.as_mut(), self.diffs.as_mut())
    }

    // FIXME: We need to do some extra stuff for zsts
    pub fn len(&self) -> usize {
        debug_assert_eq!(self.keys.len(), self.diffs.len());
        self.keys.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn reserve(&mut self, additional: usize) {
        self.keys.reserve(additional);
        self.diffs.reserve(additional);
    }

    /// Extends the current layer with elements between lower and upper from the
    /// supplied source layer
    ///
    /// # Safety
    ///
    /// The key and diff types of both layers must be the same
    unsafe fn extend_from_range(&mut self, source: &Self, lower: usize, upper: usize) {
        debug_assert!(lower <= source.len() && upper <= source.len());
        if lower == upper {
            return;
        }

        // Extend with the given keys
        self.keys
            .extend_from_range(source.keys.as_ref(), lower, upper);

        // Extend with the given diffs
        self.diffs
            .extend_from_range(source.diffs.as_ref(), lower, upper);
    }

    /*fn extend<'a, I>(&mut self, tuples: I)
    where
        I: IntoIterator<Item = (&'a K, &'a R)>,
    {
        let tuples = tuples.into_iter();

        let (min, max) = tuples.size_hint();
        let extra = max.unwrap_or(min);
        self.reserve(extra);

        for tuple in tuples {
            // Safety: We checked that the types are correct earlier
            let (key, diff) = tuple;
            self.keys.push_ref(key);
            self.diffs.push_ref(diff);
        }
    }*/

    fn push_vals(&mut self, key: &mut K, diff: &mut R) {
        debug_assert_eq!(self.keys.len(), self.diffs.len());
        self.keys.push_val(key);
        self.diffs.push_val(diff);
    }

    fn push_refs(&mut self, key: &K, diff: &R) {
        debug_assert_eq!(self.keys.len(), self.diffs.len());
        self.keys.push_ref(key);
        self.diffs.push_ref(diff);
    }

    fn push_merge(
        &mut self,
        lhs: &Self,
        (mut lower1, upper1): (usize, usize),
        rhs: &Self,
        (mut lower2, upper2): (usize, usize),
    ) -> usize {
        let reserved = (upper1 - lower1) + (upper2 - lower2);
        self.reserve(reserved);

        // Create a buffer to hold any intermediate values we have to create
        let mut diff = self.factories.diff.default_box();

        // while both mergees are still active
        while lower1 < upper1 && lower2 < upper2 {
            // Safety: All involved types are the same
            match lhs.keys[lower1].cmp(&rhs.keys[lower2]) {
                Ordering::Less => {
                    // determine how far we can advance lower1 until we reach/pass lower2
                    // TODO: have a specialized `advance_to_val` method, which can be more efficient
                    // for erased vectors.
                    let bound = &rhs.keys[lower2];
                    let step = 1 + lhs.keys.advance_to(lower1 + 1, upper1, bound);

                    unsafe { self.extend_from_range(lhs, lower1, lower1 + step) };

                    lower1 += step;
                }

                Ordering::Equal => {
                    // Add `lhs[lower1]` and `rhs[lower2]`, storing the result in `diff_buf`

                    lhs.diffs[lower1].add(&rhs.diffs[lower2], diff.deref_mut());

                    // If the produced diff is not zero, push the key and its merged diff
                    if !diff.is_zero() {
                        // Push the raw values to the layer
                        self.push_refs(&lhs.keys[lower1], diff.deref());
                    }

                    lower1 += 1;
                    lower2 += 1;
                }

                Ordering::Greater => {
                    // determine how far we can advance lower2 until we reach/pass lower1
                    let bound = &lhs.keys[lower1];
                    let step = 1 + rhs.keys.advance_to(lower2 + 1, upper2, bound);

                    unsafe { self.extend_from_range(rhs, lower2, lower2 + step) };

                    lower2 += step;
                }
            }
        }

        unsafe {
            if lower1 < upper1 {
                self.extend_from_range(lhs, lower1, upper1);
            }

            if lower2 < upper2 {
                self.extend_from_range(rhs, lower2, upper2);
            }
        }

        self.len()
    }

    /// Remove keys smaller than `lower_bound` from the batch.
    pub fn truncate_keys_below(&mut self, lower_bound: &K) {
        let index = self.keys.advance_to(0, self.keys.len(), lower_bound);
        self.truncate_below(index);
    }

    /// Compute a random sample of size `sample_size` of keys in `self.keys`.
    ///
    /// Pushes the random sample of keys to the `output` vector in ascending
    /// order.
    pub fn sample_keys<RG>(&self, rng: &mut RG, sample_size: usize, output: &mut DynVec<K>)
    where
        RG: Rng,
    {
        self.keys
            .sample_slice(self.lower_bound, self.len(), rng, sample_size, output);
    }

    pub(crate) fn truncate(&mut self, length: usize) {
        self.keys.truncate(length);
        self.diffs.truncate(length);
        self.lower_bound = min(self.lower_bound, length);
    }
}

impl<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> PartialEq for Leaf<K, R> {
    fn eq(&self, other: &Self) -> bool {
        self.keys.eq(&other.keys) && self.diffs.eq(&other.diffs)
    }
}

impl<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> Eq for Leaf<K, R> {}

impl<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> Trie for Leaf<K, R> {
    type Item<'a> = (&'a mut K, &'a mut R);
    type ItemRef<'a> = (&'a K, &'a R);
    type Cursor<'s> = LeafCursor<'s, K, R>;
    type Factories = LeafFactories<K, R>;

    type MergeBuilder = LeafBuilder<K, R>;
    type TupleBuilder = LeafBuilder<K, R>;

    fn keys(&self) -> usize {
        self.len() - self.lower_bound
    }

    fn tuples(&self) -> usize {
        self.len() - self.lower_bound
    }

    fn cursor_from(&self, lower: usize, upper: usize) -> Self::Cursor<'_> {
        LeafCursor::new(lower, self, (lower, upper))
    }

    fn truncate_below(&mut self, lower_bound: usize) {
        if lower_bound > self.lower_bound {
            self.lower_bound = min(lower_bound, self.len());
        }
    }

    fn lower_bound(&self) -> usize {
        self.lower_bound
    }
}

impl<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> Add<Self> for Leaf<K, R> {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        if self.is_empty() {
            rhs
        } else if rhs.is_empty() {
            self
        } else {
            // FIXME: We want to reuse allocations if at all possible
            self.merge(&rhs)
        }
    }
}

impl<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> AddAssign<Self> for Leaf<K, R> {
    fn add_assign(&mut self, rhs: Self) {
        if !rhs.is_empty() {
            // FIXME: We want to reuse allocations if at all possible
            *self = self.merge(&rhs);
        }
    }
}

impl<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> AddAssignByRef for Leaf<K, R> {
    fn add_assign_by_ref(&mut self, other: &Self) {
        if !other.is_empty() {
            // FIXME: We want to reuse allocations if at all possible
            *self = self.merge(other);
        }
    }
}

impl<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> AddByRef for Leaf<K, R> {
    fn add_by_ref(&self, rhs: &Self) -> Self {
        self.merge(rhs)
    }
}

impl<K: DataTrait + ?Sized, R: WeightTraitTyped + ?Sized> NegByRef for Leaf<K, R>
where
    R::Type: DBWeight + ZRingValue + Erase<R>,
{
    fn neg_by_ref(&self) -> Self {
        let mut diffs: LeanVec<R::Type> = LeanVec::with_capacity(self.diffs.len());

        for diff in self.diffs.as_slice().iter() {
            diffs.push(diff.neg_by_ref())
        }
        // TODO: We can eliminate elements from `0..lower_bound` when creating the
        // negated layer
        Self {
            keys: self.keys.clone(),
            diffs: Box::new(diffs).erase_box(),
            lower_bound: self.lower_bound,
            factories: self.factories.clone(),
        }
    }
}

impl<K: DataTrait + ?Sized, R: WeightTraitTyped + ?Sized> Neg for Leaf<K, R>
where
    R::Type: DBWeight + ZRingValue,
{
    type Output = Self;

    fn neg(mut self) -> Self {
        for diff in self.diffs.as_mut_slice().iter_mut() {
            *diff = diff.neg_by_ref();
        }

        self
    }
}

impl<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> NumEntries for Leaf<K, R> {
    const CONST_NUM_ENTRIES: Option<usize> = None;

    fn num_entries_shallow(&self) -> usize {
        self.len()
    }

    fn num_entries_deep(&self) -> usize {
        // FIXME: Doesn't take element sizes into account
        self.len()
    }
}

#[derive(Debug)]
pub struct LeafCursor<'a, K: DataTrait + ?Sized, R: WeightTrait + ?Sized> {
    pos: isize,
    pub(crate) storage: &'a Leaf<K, R>,
    bounds: (usize, usize),
}

impl<'a, K: DataTrait + ?Sized, R: WeightTrait + ?Sized> Clone for LeafCursor<'a, K, R> {
    fn clone(&self) -> Self {
        Self {
            pos: self.pos,
            storage: self.storage,
            bounds: self.bounds,
        }
    }
}

impl<'a, K: DataTrait + ?Sized, R: WeightTrait + ?Sized> Display for LeafCursor<'a, K, R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut cursor: LeafCursor<K, R> = self.clone();

        while cursor.valid() {
            let (key, val) = cursor.item();
            write!(f, "{key:?}->{val:?}")?;
            cursor.step();
        }

        Ok(())
    }
}

impl<'a, K: DataTrait + ?Sized, R: WeightTrait + ?Sized> LeafCursor<'a, K, R> {
    pub const fn new(pos: usize, storage: &'a Leaf<K, R>, bounds: (usize, usize)) -> Self {
        Self {
            pos: pos as isize,
            storage,
            bounds,
        }
    }

    pub(crate) const fn storage(&self) -> &'a Leaf<K, R> {
        self.storage
    }

    pub(crate) const fn bounds(&self) -> (usize, usize) {
        self.bounds
    }

    pub fn seek_key_with(&mut self, predicate: &dyn Fn(&K) -> bool) {
        if self.valid() {
            self.pos += self
                .storage
                .keys
                .advance_until(self.pos as usize, self.bounds.1, predicate)
                as isize;
        }
    }

    pub fn seek_key_with_reverse(&mut self, predicate: &dyn Fn(&K) -> bool) {
        if self.valid() {
            self.pos -= self
                .storage
                .keys
                .retreat_until(self.bounds.0, self.pos as usize, predicate)
                as isize;
        }
    }

    pub fn current_key(&self) -> &'a K {
        debug_assert!(self.pos >= 0);
        &self.storage.keys[self.pos as usize]
    }

    pub fn current_diff(&self) -> &'a R {
        debug_assert!(self.pos >= 0);
        &self.storage.diffs[self.pos as usize]
    }
}

impl<'s, K: DataTrait + ?Sized, R: WeightTrait + ?Sized> Cursor<'s> for LeafCursor<'s, K, R> {
    /// The type revealed by the cursor.
    type Item<'k> = (&'k K, &'k R)
    where
        Self: 'k;

    /// Key used to search the contents of the cursor.
    type Key = K;

    type ValueCursor = ();

    fn keys(&self) -> usize {
        self.bounds.1 - self.bounds.0
    }

    /// Reveals the current item.
    fn item(&self) -> Self::Item<'s> {
        if self.pos as usize >= self.storage.keys.len() || self.pos < 0 {
            cursor_position_oob(self.pos, self.storage.keys.len());
        }

        (self.current_key(), self.current_diff())
    }

    /// Returns cursor over values associted with the current key.
    fn values(&self) {}

    /// Advances the cursor by one element.
    fn step(&mut self) {
        self.pos += 1;

        if !self.valid() {
            self.pos = self.bounds.1 as isize;
        }
    }

    /// Move cursor back by one element.
    fn step_reverse(&mut self) {
        self.pos -= 1;

        if self.pos < self.bounds.0 as isize {
            self.pos = self.bounds.0 as isize - 1;
        }
    }

    /// Advances the cursor until the location where `key` would be expected.
    fn seek(&mut self, key: &K) {
        if self.valid() {
            self.pos += self
                .storage
                .keys
                .advance_to(self.pos as usize, self.bounds.1, key) as isize;
        }
    }

    /// Move the cursor back until the location where `key` would be expected.
    fn seek_reverse(&mut self, key: &K) {
        if self.valid() {
            self.pos -= self
                .storage
                .keys
                .retreat_to(self.bounds.0, self.pos as usize, key) as isize;
        }
    }

    /// Returns `true` if the cursor points at valid data. Returns `false` if
    /// the cursor is exhausted.
    fn valid(&self) -> bool {
        self.pos >= self.bounds.0 as isize && self.pos < self.bounds.1 as isize
    }

    /// Rewinds the cursor to its initial state.
    fn rewind(&mut self) {
        self.pos = self.bounds.0 as isize;
    }

    /// Moves the cursor to the last position.
    fn fast_forward(&mut self) {
        self.pos = self.bounds.1 as isize - 1;
    }

    /// Current position of the cursor.
    fn position(&self) -> usize {
        self.pos as usize
    }

    /// Repositions the cursor to a different range of values.
    fn reposition(&mut self, lower: usize, upper: usize) {
        self.pos = lower as isize;
        self.bounds = (lower, upper);
    }
}

/// A builder for ordered values
#[derive(SizeOf)]
pub struct LeafBuilder<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> {
    layer: Leaf<K, R>,
}

impl<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> LeafBuilder<K, R> {
    fn with_key_capacity(factories: &LeafFactories<K, R>, capacity: usize) -> Self {
        Self {
            layer: Leaf::with_capacity(factories, capacity),
        }
    }
}

impl<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> Builder for LeafBuilder<K, R> {
    type Trie = Leaf<K, R>;

    fn boundary(&mut self) -> usize {
        self.layer.len()
    }

    fn done(self) -> Self::Trie {
        self.layer
    }
}

impl<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> MergeBuilder for LeafBuilder<K, R> {
    fn with_capacity(left: &Self::Trie, right: &Self::Trie) -> Self {
        let capacity = left.keys() + right.keys();
        Self::with_key_capacity(&left.factories, capacity)
    }

    fn reserve(&mut self, additional: usize) {
        self.layer.reserve(additional);
    }

    fn keys(&self) -> usize {
        self.layer.keys.len()
    }

    fn copy_range(&mut self, other: &Self::Trie, lower: usize, upper: usize) {
        // Safety: The current builder's layer and the trie layer types are the same
        unsafe { self.layer.extend_from_range(other, lower, upper) };
    }

    fn copy_range_retain_keys<'a, F>(
        &mut self,
        other: &'a Self::Trie,
        lower: usize,
        upper: usize,
        filter: &F,
    ) where
        F: Fn(&<<Self::Trie as Trie>::Cursor<'a> as Cursor<'a>>::Key) -> bool,
    {
        assert!(lower <= other.keys.len() && upper <= other.keys.len());

        self.layer.keys.reserve(upper - lower);
        self.layer.diffs.reserve(upper - lower);

        for index in lower..upper {
            if filter(&other.keys[index]) {
                self.layer.keys.push_ref(&other.keys[index]);
                self.layer.diffs.push_ref(&other.diffs[index]);
            }
        }
    }

    fn push_merge<'a>(
        &'a mut self,
        lhs_cursor: <Self::Trie as Trie>::Cursor<'a>,
        rhs_cursor: <Self::Trie as Trie>::Cursor<'a>,
    ) {
        let (lhs, rhs) = (lhs_cursor.storage(), rhs_cursor.storage());
        let lhs_bounds = (lhs_cursor.position(), lhs_cursor.bounds().1);
        let rhs_bounds = (rhs_cursor.position(), rhs_cursor.bounds().1);
        self.layer.push_merge(lhs, lhs_bounds, rhs, rhs_bounds);
    }

    fn push_merge_retain_keys<'a, F>(
        &'a mut self,
        cursor1: <Self::Trie as Trie>::Cursor<'a>,
        cursor2: <Self::Trie as Trie>::Cursor<'a>,
        filter: &F,
    ) where
        F: Fn(&<<Self::Trie as Trie>::Cursor<'a> as Cursor<'a>>::Key) -> bool,
    {
        //unsafe { self.assume_invariants() }

        let (trie1, trie2) = (cursor1.storage(), cursor2.storage());
        /*unsafe {
            trie1.assume_invariants();
            trie2.assume_invariants();
        }*/
        let mut diff = self.layer.factories.diff.default_box();

        let (_, upper1) = cursor1.bounds();
        let mut lower1 = cursor1.position();
        let (_, upper2) = cursor2.bounds();
        let mut lower2 = cursor2.position();

        let reserved = (upper1 - lower1) + (upper2 - lower2);
        self.reserve(reserved);

        // while both mergees are still active
        while lower1 < upper1 && lower2 < upper2 {
            match &trie1.keys[lower1].cmp(&trie2.keys[lower2]) {
                Ordering::Less => {
                    // determine how far we can advance lower1 until we reach/pass lower2
                    let bound = &trie2.keys[lower2];
                    let step = 1 + trie1.keys.advance_to(1 + lower1, upper1, bound);

                    let step = min(step, 1000);
                    self.copy_range_retain_keys(trie1, lower1, lower1 + step, filter);

                    lower1 += step;
                }

                Ordering::Equal => {
                    if filter(&trie1.keys[lower1]) {
                        trie1.diffs[lower1].add(&trie2.diffs[lower2], &mut *diff);

                        if !diff.is_zero() {
                            self.layer.push_refs(&trie1.keys[lower1], &*diff);
                        }
                    }

                    lower1 += 1;
                    lower2 += 1;
                }

                Ordering::Greater => {
                    // determine how far we can advance lower2 until we reach/pass lower1
                    let bound = &trie1.keys[lower1];
                    let step = 1 + trie2.keys.advance_to(1 + lower2, upper2, bound);

                    let step = min(step, 1000);
                    self.copy_range_retain_keys(trie2, lower2, lower2 + step, filter);

                    lower2 += step;
                }
            }
        }

        if lower1 < upper1 {
            self.copy_range_retain_keys(trie1, lower1, upper1, filter);
        }
        if lower2 < upper2 {
            self.copy_range_retain_keys(trie2, lower2, upper2, filter);
        }

        // unsafe { self.assume_invariants() }
    }
}

impl<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> TupleBuilder for LeafBuilder<K, R> {
    fn new(factories: &<Self::Trie as Trie>::Factories) -> Self {
        Self {
            layer: Leaf::new(factories),
        }
    }

    fn with_capacity(factories: &<Self::Trie as Trie>::Factories, capacity: usize) -> Self {
        Self {
            layer: Leaf::with_capacity(factories, capacity),
        }
    }

    fn reserve_tuples(&mut self, additional: usize) {
        self.layer.reserve(additional);
    }

    fn tuples(&self) -> usize {
        self.layer.len()
    }

    fn push_tuple(&mut self, tuple: <Self::Trie as Trie>::Item<'_>) {
        let (key, w) = tuple;
        self.layer.push_vals(key, w);
    }

    fn push_refs<'a>(&mut self, (key, w): (&K, &R)) {
        self.layer.push_refs(key, w);
    }
}
