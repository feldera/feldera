mod builders;
mod cursor;

use super::{DataVTable, IntoErasedData};
pub use builders::{TypedErasedKeyLeafBuilder, UnorderedTypedLayerBuilder};
pub use cursor::TypedKeyLeafCursor;

use crate::{
    algebra::{AddAssignByRef, AddByRef, HasZero, NegByRef},
    trace::layers::{advance_erased, Trie},
    utils::{uninit_vec, DynVec, DynVecVTable},
    NumEntries,
};
use size_of::SizeOf;
use std::{
    any::TypeId,
    cmp::{min, Ordering},
    fmt::{self, Debug},
    marker::PhantomData,
    ops::{Add, AddAssign, Index, Neg},
};

// TODO: the `diff` type is fixed in most use cases (the `weighted` operator
// being the only exception I can think of, so it will probably pay off to have
// a verson of this with a statically typed diff type).

#[derive(Clone, SizeOf)]
pub struct ErasedKeyLeaf<R> {
    keys: DynVec<DataVTable>,
    diffs: Vec<R>,
    lower_bound: usize,
}

impl<R: Debug> Debug for ErasedKeyLeaf<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        struct DebugPtr(
            *const u8,
            unsafe extern "C" fn(*const u8, *mut fmt::Formatter<'_>) -> bool,
        );

        impl Debug for DebugPtr {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                if unsafe { (self.1)(self.0, f) } {
                    Ok(())
                } else {
                    Err(fmt::Error)
                }
            }
        }

        let mut map = f.debug_map();
        for idx in 0..self.len() {
            let key = DebugPtr(self.keys.index(idx), self.keys.vtable().common.debug);
            let diff = self.diffs.index(idx);
            map.entry(&key, &diff);
        }

        map.finish()
    }
}

impl<R> ErasedKeyLeaf<R> {
    pub fn new(key_vtable: &'static DataVTable) -> ErasedKeyLeaf<R> {
        Self {
            keys: DynVec::new(key_vtable),
            diffs: Vec::new(),
            lower_bound: 0,
        }
    }

    pub fn with_capacity(key_vtable: &'static DataVTable, capacity: usize) -> ErasedKeyLeaf<R> {
        Self {
            keys: DynVec::with_capacity(key_vtable, capacity),
            diffs: Vec::with_capacity(capacity),
            lower_bound: 0,
        }
    }

    // FIXME: We need to do some extra stuff for zsts
    pub fn len(&self) -> usize {
        debug_assert_eq!(self.keys.len(), self.diffs.len());
        self.keys.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    const fn key_size(&self) -> usize {
        self.keys.vtable().common.size_of
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
    unsafe fn extend_from_range(&mut self, source: &ErasedKeyLeaf<R>, lower: usize, upper: usize)
    where
        R: Clone,
    {
        debug_assert!(lower <= source.len() && upper <= source.len());
        if lower == upper {
            return;
        }

        // Extend with the given keys
        self.keys.clone_from_range(&source.keys, lower..upper);

        // Extend with the given diffs
        self.diffs.extend_from_slice(&source.diffs[lower..upper]);
    }

    fn push<K: 'static>(&mut self, (key, diff): (K, R)) {
        debug_assert_eq!(self.keys.len(), self.diffs.len());
        self.keys.push(key);
        self.diffs.push(diff);
    }

    fn extend<K, I>(&mut self, tuples: I)
    where
        K: 'static,
        R: 'static,
        I: IntoIterator<Item = (K, R)>,
    {
        let tuples = tuples.into_iter();

        let (min, max) = tuples.size_hint();
        let extra = max.unwrap_or(min);
        self.reserve(extra);

        assert!(self.keys.contains_type::<K>());
        for (key, diff) in tuples {
            // Safety: We checked that the types are correct earlier
            unsafe {
                self.keys.push_untypechecked(key);
                self.diffs.push(diff);
            }
        }
    }

    unsafe fn push_raw(&mut self, key: *const u8, diff: R) {
        debug_assert_eq!(self.keys.len(), self.diffs.len());
        self.keys.push_raw(key);
        self.diffs.push(diff);
    }

    /// Returns the [`TypeId`]s of the current layer's key.
    fn key_type(&self) -> TypeId {
        self.keys.vtable().type_id()
    }

    fn push_merge(
        &mut self,
        lhs: &Self,
        (mut lower1, upper1): (usize, usize),
        rhs: &Self,
        (mut lower2, upper2): (usize, usize),
    ) -> usize
    where
        R: Eq + HasZero + AddByRef + Clone,
    {
        // Ensure all the vtables are for the same type
        debug_assert_eq!(self.key_type(), lhs.key_type());

        let key_common = self.keys.vtable().common;

        let reserved = (upper1 - lower1) + (upper2 - lower2);
        self.reserve(reserved);

        // Create a buffer to hold any intermediate values we have to create
        let mut key_buf = uninit_vec::<u8>(self.key_size()).into_boxed_slice();

        // while both mergees are still active
        while lower1 < upper1 && lower2 < upper2 {
            // Safety: All involved types are the same
            let order = unsafe { (key_common.cmp)(lhs.keys.index(lower1), rhs.keys.index(lower2)) };

            match order {
                Ordering::Less => {
                    // determine how far we can advance lower1 until we reach/pass lower2
                    let step = 1 + advance_erased(
                        lhs.keys.range(lower1 + 1..upper1),
                        self.key_size(),
                        |x| unsafe { (key_common.lt)(x, rhs.keys.index(lower2)) },
                    );

                    unsafe { self.extend_from_range(lhs, lower1, lower1 + step) };

                    lower1 += step;
                }

                Ordering::Equal => {
                    // Safety: All involved types are the same
                    unsafe {
                        // Add `lhs[lower1]` and `rhs[lower2]`, storing the result in `diff_buf`
                        let w = lhs.diffs.index(lower1).add_by_ref(rhs.diffs.index(lower2));

                        // If the produced diff is not zero, push the key and its merged diff
                        if !w.is_zero() {
                            // Clone the element at `lhs[lower1]` into `key_buf`
                            (key_common.clone)(lhs.keys.index(lower1), key_buf.as_mut_ptr().cast());

                            // Push the raw values to the layer
                            self.push_raw(key_buf.as_ptr().cast(), w);
                        }
                    }

                    lower1 += 1;
                    lower2 += 1;
                }

                Ordering::Greater => {
                    // determine how far we can advance lower2 until we reach/pass lower1
                    let step = 1 + advance_erased(
                        rhs.keys.range(lower2 + 1..upper2),
                        self.key_size(),
                        |x| unsafe { (key_common.lt)(x, lhs.keys.index(lower1)) },
                    );

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

    /*
    unsafe fn drop_range<Rg>(&mut self, range: Rg)
    where
        Rg: RangeBounds<usize> + Clone,
    {
        let (ptr, len) = self.keys.range_mut(range.clone());
        unsafe { self.keys.vtable().drop_slice_in_place(ptr, len) }

        let (ptr, len) = self.diffs.range_mut(range);
        unsafe { self.diffs.vtable().drop_slice_in_place(ptr, len) }
    }*/
}

impl<R: PartialEq> PartialEq for ErasedKeyLeaf<R> {
    fn eq(&self, other: &Self) -> bool {
        self.key_type() == other.key_type()
            && self.len() == other.len()
            && self
                .keys
                .iter()
                .zip(other.keys.iter())
                .all(|(lhs, rhs)| unsafe { (self.keys.vtable().common.eq)(lhs, rhs) })
            && (self.diffs == other.diffs)
    }
}

impl<R: Eq> Eq for ErasedKeyLeaf<R> {}

impl<R> NegByRef for ErasedKeyLeaf<R>
where
    R: NegByRef,
{
    fn neg_by_ref(&self) -> Self {
        Self {
            keys: self.keys.clone(),
            diffs: self.diffs.iter().map(NegByRef::neg_by_ref).collect(),
            lower_bound: self.lower_bound,
        }
    }
}

impl<R> Neg for ErasedKeyLeaf<R>
where
    R: Neg<Output = R>,
{
    type Output = Self;

    fn neg(self) -> Self {
        Self {
            keys: self.keys,
            diffs: self.diffs.into_iter().map(Neg::neg).collect(),
            lower_bound: self.lower_bound,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, SizeOf)]
pub struct TypedErasedKeyLeaf<K, R> {
    layer: ErasedKeyLeaf<R>,
    __type: PhantomData<(K, R)>,
}

impl<K, R> TypedErasedKeyLeaf<K, R> {
    pub fn new() -> Self
    where
        K: IntoErasedData,
    {
        Self {
            layer: ErasedKeyLeaf::new(&<K as IntoErasedData>::DATA_VTABLE),
            __type: PhantomData,
        }
    }

    pub fn with_capacity(capacity: usize) -> TypedErasedKeyLeaf<K, R>
    where
        K: IntoErasedData,
    {
        Self {
            layer: ErasedKeyLeaf::with_capacity(&<K as IntoErasedData>::DATA_VTABLE, capacity),
            __type: PhantomData,
        }
    }

    pub fn len(&self) -> usize {
        self.layer.len()
    }

    pub fn is_empty(&self) -> bool {
        self.layer.is_empty()
    }
}

impl<K, R> Trie for TypedErasedKeyLeaf<K, R>
where
    K: IntoErasedData,
    R: Clone + Eq + AddAssign + AddByRef + HasZero + 'static,
{
    type Item = (K, R);
    type Cursor<'s> = TypedKeyLeafCursor<'s, K, R>
    where
        K: 's,
        R: 's;
    type MergeBuilder = TypedErasedKeyLeafBuilder<K, R>;
    type TupleBuilder = UnorderedTypedLayerBuilder<K, R>;

    fn keys(&self) -> usize {
        self.len() - self.layer.lower_bound
    }

    fn tuples(&self) -> usize {
        self.len() - self.layer.lower_bound
    }

    fn cursor_from(&self, lower: usize, upper: usize) -> Self::Cursor<'_> {
        TypedKeyLeafCursor::new(lower, self, (lower, upper))
    }

    fn truncate_below(&mut self, lower_bound: usize) {
        if lower_bound > self.layer.lower_bound {
            self.layer.lower_bound = min(lower_bound, self.len());
        }
    }

    fn lower_bound(&self) -> usize {
        self.layer.lower_bound
    }
}

// TODO: by-value merge
impl<K, R> Add<Self> for TypedErasedKeyLeaf<K, R>
where
    K: IntoErasedData,
    R: Clone + Eq + HasZero + AddAssign + AddByRef + 'static,
{
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

impl<K, R> AddAssign<Self> for TypedErasedKeyLeaf<K, R>
where
    K: IntoErasedData,
    R: Clone + Eq + HasZero + AddAssign + AddByRef + 'static,
{
    fn add_assign(&mut self, rhs: Self) {
        if !rhs.is_empty() {
            // FIXME: We want to reuse allocations if at all possible
            *self = self.merge(&rhs);
        }
    }
}

impl<K, R> AddAssignByRef for TypedErasedKeyLeaf<K, R>
where
    K: IntoErasedData,
    R: Clone + Eq + HasZero + AddAssign + AddByRef + 'static,
{
    fn add_assign_by_ref(&mut self, other: &Self) {
        if !other.is_empty() {
            // FIXME: We want to reuse allocations if at all possible
            *self = self.merge(other);
        }
    }
}

impl<K, R> AddByRef for TypedErasedKeyLeaf<K, R>
where
    K: IntoErasedData,
    R: Clone + Eq + HasZero + AddAssign + AddByRef + 'static,
{
    fn add_by_ref(&self, rhs: &Self) -> Self {
        self.merge(rhs)
    }
}

impl<K, R> NegByRef for TypedErasedKeyLeaf<K, R>
where
    K: IntoErasedData,
    R: NegByRef,
{
    fn neg_by_ref(&self) -> Self {
        Self {
            layer: self.layer.neg_by_ref(),
            __type: PhantomData,
        }
    }
}

impl<K, R> Neg for TypedErasedKeyLeaf<K, R>
where
    K: IntoErasedData,
    R: Neg<Output = R>,
{
    type Output = Self;

    fn neg(self) -> Self {
        Self {
            layer: self.layer.neg(),
            __type: PhantomData,
        }
    }
}

impl<K, R> NumEntries for TypedErasedKeyLeaf<K, R> {
    const CONST_NUM_ENTRIES: Option<usize> = None;

    fn num_entries_shallow(&self) -> usize {
        self.len()
    }

    fn num_entries_deep(&self) -> usize {
        // FIXME: Doesn't take element sizes into account
        self.len()
    }
}

impl<K, R> Default for TypedErasedKeyLeaf<K, R>
where
    K: IntoErasedData,
{
    fn default() -> Self {
        Self::new()
    }
}
