mod builders;
mod consumer;
mod cursor;
mod vtable;

pub use builders::{TypedLayerBuilder, UnorderedTypedLayerBuilder};
pub use consumer::{TypedLayerConsumer, TypedLayerValues};
pub use cursor::TypedLayerCursor;
pub use vtable::{DataVTable, DiffVTable, ErasedVTable, IntoErasedData, IntoErasedDiff};

use crate::{
    algebra::{AddAssignByRef, AddByRef, NegByRef},
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
    ops::{Add, AddAssign, Neg, RangeBounds},
};

#[derive(Clone, SizeOf)]
pub struct ErasedLayer {
    keys: DynVec<DataVTable>,
    diffs: DynVec<DiffVTable>,
    lower_bound: usize,
}

impl Debug for ErasedLayer {
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
            let key = DebugPtr(self.keys.index(idx), self.diffs.vtable().common.debug);
            let diff = DebugPtr(self.diffs.index(idx), self.diffs.vtable().common.debug);
            map.entry(&key, &diff);
        }

        map.finish()
    }
}

impl ErasedLayer {
    pub fn new(key_vtable: &'static DataVTable, diff_vtable: &'static DiffVTable) -> ErasedLayer {
        Self {
            keys: DynVec::new(key_vtable),
            diffs: DynVec::new(diff_vtable),
            lower_bound: 0,
        }
    }

    pub fn with_capacity(
        key_vtable: &'static DataVTable,
        diff_vtable: &'static DiffVTable,
        capacity: usize,
    ) -> ErasedLayer {
        Self {
            keys: DynVec::with_capacity(key_vtable, capacity),
            diffs: DynVec::with_capacity(diff_vtable, capacity),
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

    const fn diff_size(&self) -> usize {
        self.diffs.vtable().common.size_of
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
    unsafe fn extend_from_range(&mut self, source: &ErasedLayer, lower: usize, upper: usize) {
        debug_assert!(lower <= source.len() && upper <= source.len());
        if lower == upper {
            return;
        }

        // Extend with the given keys
        self.keys.clone_from_range(&source.keys, lower..upper);

        // Extend with the given diffs
        self.diffs.clone_from_range(&source.diffs, lower..upper);
    }

    fn push<K: 'static, R: 'static>(&mut self, (key, diff): (K, R)) {
        debug_assert_eq!(self.keys.len(), self.diffs.len());
        self.keys.push(key);
        self.diffs.push(diff);
    }

    fn extend<K, R, I>(&mut self, tuples: I)
    where
        K: 'static,
        R: 'static,
        I: IntoIterator<Item = (K, R)>,
    {
        let tuples = tuples.into_iter();

        let (min, max) = tuples.size_hint();
        let extra = max.unwrap_or(min);
        self.reserve(extra);

        assert!(self.keys.contains_type::<K>() && self.diffs.contains_type::<R>());
        for (key, diff) in tuples {
            // Safety: We checked that the types are correct earlier
            unsafe {
                self.keys.push_untypechecked(key);
                self.diffs.push_untypechecked(diff);
            }
        }
    }

    unsafe fn push_raw(&mut self, key: *const u8, diff: *const u8) {
        debug_assert_eq!(self.keys.len(), self.diffs.len());
        self.keys.push_raw(key);
        self.diffs.push_raw(diff);
    }

    /// Returns the [`TypeId`]s of the current layer's key and difference
    fn value_types(&self) -> (TypeId, TypeId) {
        (self.keys.vtable().type_id(), self.diffs.vtable().type_id())
    }

    fn push_merge(
        &mut self,
        lhs: &Self,
        (mut lower1, upper1): (usize, usize),
        rhs: &Self,
        (mut lower2, upper2): (usize, usize),
    ) -> usize {
        // Ensure all the vtables are for the same type
        debug_assert_eq!(self.value_types(), lhs.value_types());
        debug_assert_eq!(self.value_types(), rhs.value_types());

        let (key_common, diff_common) = (self.keys.vtable().common, self.diffs.vtable().common);

        let reserved = (upper1 - lower1) + (upper2 - lower2);
        self.reserve(reserved);

        // Create a buffer to hold any intermediate values we have to create
        let mut key_buf = uninit_vec::<u8>(self.key_size()).into_boxed_slice();
        let mut diff_buf = uninit_vec::<u8>(self.diff_size()).into_boxed_slice();

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
                        (self.diffs.vtable().add_by_ref)(
                            lhs.diffs.index(lower1),
                            rhs.diffs.index(lower2),
                            diff_buf.as_mut_ptr().cast(),
                        );

                        // If the produced diff is not zero, push the key and its merged diff
                        if !(self.diffs.vtable().is_zero)(diff_buf.as_ptr().cast()) {
                            // Clone the element at `lhs[lower1]` into `key_buf`
                            (key_common.clone)(lhs.keys.index(lower1), key_buf.as_mut_ptr().cast());

                            // Push the raw values to the layer
                            self.push_raw(key_buf.as_ptr().cast(), diff_buf.as_ptr().cast());

                        // Otherwise, drop the difference value
                        } else {
                            // FIXME: If `is_zero` or `clone` panic, the value within `diff_buf` can
                            // potentially leak
                            (diff_common.drop_in_place)(diff_buf.as_mut_ptr().cast());
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

    unsafe fn drop_range<R>(&mut self, range: R)
    where
        R: RangeBounds<usize> + Clone,
    {
        let (ptr, len) = self.keys.range_mut(range.clone());
        unsafe { self.keys.vtable().drop_slice_in_place(ptr, len) }

        let (ptr, len) = self.diffs.range_mut(range);
        unsafe { self.diffs.vtable().drop_slice_in_place(ptr, len) }
    }

    fn neg(mut self) -> Self {
        unsafe {
            (self.diffs.vtable().neg_slice)(self.diffs.as_mut_ptr(), self.diffs.len());
        }

        self
    }

    fn neg_by_ref(&self) -> Self {
        let mut diffs = DynVec::with_capacity(self.diffs.vtable(), self.diffs.len());

        unsafe {
            (self.diffs.vtable().neg_slice_by_ref)(
                self.diffs.as_ptr(),
                diffs.as_mut_ptr(),
                self.diffs.len(),
            );

            diffs.set_len(self.diffs.len());
        }

        // TODO: We can eliminate elements from `0..lower_bound` when creating the
        // negated layer
        Self {
            keys: self.keys.clone(),
            diffs,
            lower_bound: self.lower_bound,
        }
    }
}

impl PartialEq for ErasedLayer {
    fn eq(&self, other: &Self) -> bool {
        self.value_types() == other.value_types()
            && self.len() == other.len()
            && self
                .keys
                .iter()
                .zip(other.keys.iter())
                .all(|(lhs, rhs)| unsafe { (self.keys.vtable().common.eq)(lhs, rhs) })
            && self
                .diffs
                .iter()
                .zip(other.diffs.iter())
                .all(|(lhs, rhs)| unsafe { (self.diffs.vtable().common.eq)(lhs, rhs) })
    }
}

impl Eq for ErasedLayer {}

#[derive(Debug, Clone, PartialEq, Eq, SizeOf)]
pub struct TypedLayer<K, R> {
    layer: ErasedLayer,
    __type: PhantomData<(K, R)>,
}

impl<K, R> TypedLayer<K, R> {
    pub fn new() -> Self
    where
        K: IntoErasedData,
        R: IntoErasedDiff,
    {
        Self {
            layer: ErasedLayer::new(
                &<K as IntoErasedData>::DATA_VTABLE,
                &<R as IntoErasedDiff>::DIFF_VTABLE,
            ),
            __type: PhantomData,
        }
    }

    pub fn with_capacity(capacity: usize) -> TypedLayer<K, R>
    where
        K: IntoErasedData,
        R: IntoErasedDiff,
    {
        Self {
            layer: ErasedLayer::with_capacity(
                &<K as IntoErasedData>::DATA_VTABLE,
                &<R as IntoErasedDiff>::DIFF_VTABLE,
                capacity,
            ),
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

impl<K, R> Trie for TypedLayer<K, R>
where
    K: IntoErasedData,
    R: IntoErasedDiff,
{
    type Item = (K, R);
    type Cursor<'s> = TypedLayerCursor<'s, K, R>
    where
        K: 's,
        R: 's;
    type MergeBuilder = TypedLayerBuilder<K, R>;
    type TupleBuilder = UnorderedTypedLayerBuilder<K, R>;

    fn keys(&self) -> usize {
        self.len()
    }

    fn tuples(&self) -> usize {
        self.len()
    }

    fn cursor_from(&self, lower: usize, upper: usize) -> Self::Cursor<'_> {
        TypedLayerCursor::new(lower, self, (lower, upper))
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
impl<K, R> Add<Self> for TypedLayer<K, R>
where
    K: IntoErasedData,
    R: IntoErasedDiff,
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

impl<K, R> AddAssign<Self> for TypedLayer<K, R>
where
    K: IntoErasedData,
    R: IntoErasedDiff,
{
    fn add_assign(&mut self, rhs: Self) {
        if !rhs.is_empty() {
            // FIXME: We want to reuse allocations if at all possible
            *self = self.merge(&rhs);
        }
    }
}

impl<K, R> AddAssignByRef for TypedLayer<K, R>
where
    K: IntoErasedData,
    R: IntoErasedDiff,
{
    fn add_assign_by_ref(&mut self, other: &Self) {
        if !other.is_empty() {
            // FIXME: We want to reuse allocations if at all possible
            *self = self.merge(other);
        }
    }
}

impl<K, R> AddByRef for TypedLayer<K, R>
where
    K: IntoErasedData,
    R: IntoErasedDiff,
{
    fn add_by_ref(&self, rhs: &Self) -> Self {
        self.merge(rhs)
    }
}

impl<K, R> NegByRef for TypedLayer<K, R>
where
    K: IntoErasedData,
    R: IntoErasedDiff,
{
    fn neg_by_ref(&self) -> Self {
        Self {
            layer: self.layer.neg_by_ref(),
            __type: PhantomData,
        }
    }
}

impl<K, R> Neg for TypedLayer<K, R>
where
    K: IntoErasedData,
    R: IntoErasedDiff,
{
    type Output = Self;

    fn neg(self) -> Self {
        Self {
            layer: self.layer.neg(),
            __type: PhantomData,
        }
    }
}

impl<K, R> NumEntries for TypedLayer<K, R> {
    const CONST_NUM_ENTRIES: Option<usize> = None;

    fn num_entries_shallow(&self) -> usize {
        self.len()
    }

    fn num_entries_deep(&self) -> usize {
        // FIXME: Doesn't take element sizes into account
        self.len()
    }
}

impl<K, R> Default for TypedLayer<K, R>
where
    K: IntoErasedData,
    R: IntoErasedDiff,
{
    fn default() -> Self {
        Self::new()
    }
}
