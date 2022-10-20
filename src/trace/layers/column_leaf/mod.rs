//! Implementation using ordered keys and exponential search over a
//! struct-of-array container

mod builders;
mod consumer;
mod cursor;
mod tests;

pub use builders::{OrderedColumnLeafBuilder, UnorderedColumnLeafBuilder};
pub use consumer::{ColumnLeafConsumer, ColumnLeafValues};
pub use cursor::ColumnLeafCursor;

use crate::{
    algebra::{AddAssignByRef, AddByRef, HasZero, NegByRef},
    trace::layers::Trie,
    utils::{assume, cast_uninit_vec},
    DBData, DBWeight, NumEntries,
};
use size_of::SizeOf;
use std::{
    fmt::{self, Display},
    mem::MaybeUninit,
    ops::{Add, AddAssign, Neg},
    ptr,
    slice::SliceIndex,
};

/// A layer of ordered values
#[derive(Debug, Clone, Eq, PartialEq, SizeOf)]
pub struct OrderedColumnLeaf<K, R> {
    // Invariant: keys.len == diffs.len
    pub(super) keys: Vec<K>,
    pub(super) diffs: Vec<R>,
}

impl<K, R> OrderedColumnLeaf<K, R> {
    /// Create an empty `OrderedColumnLeaf`
    pub const fn empty() -> Self {
        Self {
            keys: Vec::new(),
            diffs: Vec::new(),
        }
    }

    /// Breaks an `OrderedColumnLeaf` into its component parts
    pub fn into_parts(self) -> (Vec<K>, Vec<R>) {
        (self.keys, self.diffs)
    }

    /// Creates a new `OrderedColumnLeaf` from the given keys and diffs
    ///
    /// # Safety
    ///
    /// `keys` and `diffs` must have the same length
    pub unsafe fn from_parts(keys: Vec<K>, diffs: Vec<R>) -> Self {
        debug_assert_eq!(keys.len(), diffs.len());
        Self { keys, diffs }
    }

    /// Get the length of the current leaf
    pub fn len(&self) -> usize {
        unsafe { self.assume_invariants() }
        self.keys.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    unsafe fn set_len(&mut self, length: usize) {
        self.keys.set_len(length);
        self.diffs.set_len(length);
    }

    /// Get mutable references to the current leaf's keys and differences
    pub(crate) fn columns_mut(&mut self) -> (&mut [K], &mut [R]) {
        unsafe { self.assume_invariants() }
        (&mut self.keys, &mut self.diffs)
    }

    /// Get a reference to the current leaf's key values
    pub fn keys(&self) -> &[K] {
        unsafe { self.assume_invariants() }
        &self.keys
    }

    /// Get a mutable reference to the current leaf's key values
    pub fn keys_mut(&mut self) -> &mut [K] {
        unsafe { self.assume_invariants() }
        &mut self.keys
    }

    /// Get a reference to the current leaf's key values
    pub fn diffs(&self) -> &[R] {
        unsafe { self.assume_invariants() }
        &self.diffs
    }

    /// Get a mutable reference to the current leaf's difference values
    pub fn diffs_mut(&mut self) -> &mut [R] {
        unsafe { self.assume_invariants() }
        &mut self.diffs
    }

    /// Truncate the elements of the current leaf
    pub(crate) fn truncate(&mut self, length: usize) {
        unsafe { self.assume_invariants() }
        self.keys.truncate(length);
        self.diffs.truncate(length);
        unsafe { self.assume_invariants() }
    }

    /// Assume the invariants of the current leaf
    ///
    /// # Safety
    ///
    /// Requires that `keys` and `diffs` have the exact same length
    pub(in crate::trace::layers) unsafe fn assume_invariants(&self) {
        assume(self.keys.len() == self.diffs.len())
    }

    /// Turns the current `OrderedColumnLeaf<K, V>` into a leaf of
    /// [`MaybeUninit`] values
    pub(in crate::trace::layers) fn into_uninit(
        self,
    ) -> OrderedColumnLeaf<MaybeUninit<K>, MaybeUninit<R>> {
        unsafe { self.assume_invariants() }

        OrderedColumnLeaf {
            keys: cast_uninit_vec(self.keys),
            diffs: cast_uninit_vec(self.diffs),
        }
    }

    pub fn retain<F>(&mut self, mut retain: F)
    where
        F: FnMut(&K, &R) -> bool,
    {
        let original_len = self.len();
        // Avoid double drop if the drop guard is not executed,
        // since we may make some holes during the process.
        unsafe { self.set_len(0) };

        // Vec: [Kept, Kept, Hole, Hole, Hole, Hole, Unchecked, Unchecked]
        //      |<-              processed len   ->| ^- next to check
        //                  |<-  deleted cnt     ->|
        //      |<-              original_len                          ->|
        // Kept: Elements which predicate returns true on.
        // Hole: Moved or dropped element slot.
        // Unchecked: Unchecked valid elements.
        //
        // This drop guard will be invoked when predicate or `drop` of element panicked.
        // It shifts unchecked elements to cover holes and `set_len` to the correct
        // length. In cases when predicate and `drop` never panick, it will be
        // optimized out.
        struct BackshiftOnDrop<'a, K, R> {
            keys: &'a mut Vec<K>,
            diffs: &'a mut Vec<R>,
            processed_len: usize,
            deleted_cnt: usize,
            original_len: usize,
        }

        impl<K, R> Drop for BackshiftOnDrop<'_, K, R> {
            fn drop(&mut self) {
                if self.deleted_cnt > 0 {
                    let trailing = self.original_len - self.processed_len;
                    let processed = self.processed_len - self.deleted_cnt;

                    // SAFETY: Trailing unchecked items must be valid since we never touch them.
                    unsafe {
                        ptr::copy(
                            self.keys.as_ptr().add(self.processed_len),
                            self.keys.as_mut_ptr().add(processed),
                            trailing,
                        );

                        ptr::copy(
                            self.diffs.as_ptr().add(self.processed_len),
                            self.diffs.as_mut_ptr().add(processed),
                            trailing,
                        );
                    }
                }

                // SAFETY: After filling holes, all items are in contiguous memory.
                unsafe {
                    let final_len = self.original_len - self.deleted_cnt;
                    self.keys.set_len(final_len);
                    self.diffs.set_len(final_len);
                }
            }
        }

        let mut shifter = BackshiftOnDrop {
            keys: &mut self.keys,
            diffs: &mut self.diffs,
            processed_len: 0,
            deleted_cnt: 0,
            original_len,
        };

        fn process_loop<F, K, R, const DELETED: bool>(
            original_len: usize,
            retain: &mut F,
            shifter: &mut BackshiftOnDrop<'_, K, R>,
        ) where
            F: FnMut(&K, &R) -> bool,
        {
            while shifter.processed_len != original_len {
                // SAFETY: Unchecked element must be valid.
                let current_key =
                    unsafe { &mut *shifter.keys.as_mut_ptr().add(shifter.processed_len) };
                let current_diff =
                    unsafe { &mut *shifter.diffs.as_mut_ptr().add(shifter.processed_len) };

                if !retain(current_key, current_diff) {
                    // Advance early to avoid double drop if `drop_in_place` panicked.
                    shifter.processed_len += 1;
                    shifter.deleted_cnt += 1;

                    // SAFETY: We never touch these elements again after they're dropped.
                    unsafe {
                        ptr::drop_in_place(current_key);
                        ptr::drop_in_place(current_diff);
                    }

                    // We already advanced the counter.
                    if DELETED {
                        continue;
                    } else {
                        break;
                    }
                }

                if DELETED {
                    // SAFETY: `deleted_cnt` > 0, so the hole slot must not overlap with current
                    // element. We use copy for move, and never touch this
                    // element again.
                    unsafe {
                        let hole_offset = shifter.processed_len - shifter.deleted_cnt;

                        let key_hole = shifter.keys.as_mut_ptr().add(hole_offset);
                        ptr::copy_nonoverlapping(current_key, key_hole, 1);

                        let diff_hole = shifter.diffs.as_mut_ptr().add(hole_offset);
                        ptr::copy_nonoverlapping(current_diff, diff_hole, 1);
                    }
                }

                shifter.processed_len += 1;
            }
        }

        // Stage 1: Nothing was deleted.
        process_loop::<F, K, R, false>(original_len, &mut retain, &mut shifter);

        // Stage 2: Some elements were deleted.
        process_loop::<F, K, R, true>(original_len, &mut retain, &mut shifter);

        // All item are processed. This can be optimized to `set_len` by LLVM.
        drop(shifter);
    }
}

impl<K, R> OrderedColumnLeaf<MaybeUninit<K>, MaybeUninit<R>> {
    /// Drops all keys and diffs within the given range
    ///
    /// # Safety
    ///
    /// `range` must be a valid index into `self.keys` and `self.values` and all
    /// values within that range must be valid, initialized and have not been
    /// previously dropped
    pub(crate) unsafe fn drop_range<T>(&mut self, range: T)
    where
        T: SliceIndex<[MaybeUninit<K>], Output = [MaybeUninit<K>]>
            + SliceIndex<[MaybeUninit<R>], Output = [MaybeUninit<R>]>
            + Clone,
    {
        self.assume_invariants();
        if cfg!(debug_assertions) {
            let _ = &self.keys[range.clone()];
            let _ = &self.diffs[range.clone()];
        }

        // Drop keys within the given range
        ptr::drop_in_place(
            self.keys.get_unchecked_mut(range.clone()) as *mut [MaybeUninit<K>] as *mut [K],
        );

        // Drop diffs within the given range
        ptr::drop_in_place(self.diffs.get_unchecked_mut(range) as *mut [MaybeUninit<R>] as *mut [R]);
    }
}

impl<K, R> Trie for OrderedColumnLeaf<K, R>
where
    K: Ord + Clone,
    R: Eq + HasZero + AddAssign + AddAssignByRef + Clone,
{
    type Item = (K, R);
    type Cursor<'s> = ColumnLeafCursor<'s, K, R> where K: 's, R: 's;
    type MergeBuilder = OrderedColumnLeafBuilder<K, R>;
    type TupleBuilder = UnorderedColumnLeafBuilder<K, R>;

    fn keys(&self) -> usize {
        self.len()
    }

    fn tuples(&self) -> usize {
        self.len()
    }

    fn cursor_from(&self, lower: usize, upper: usize) -> Self::Cursor<'_> {
        unsafe { self.assume_invariants() }
        ColumnLeafCursor::new(lower, self, (lower, upper))
    }
}

impl<K, R> Display for OrderedColumnLeaf<K, R>
where
    K: DBData,
    R: DBWeight,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.cursor().fmt(f)
    }
}

// TODO: by-value merge
impl<K, R> Add<Self> for OrderedColumnLeaf<K, R>
where
    K: Ord + Clone,
    R: Eq + HasZero + AddAssign + AddAssignByRef + Clone,
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

impl<K, R> AddAssign<Self> for OrderedColumnLeaf<K, R>
where
    K: Ord + Clone,
    R: Eq + HasZero + AddAssign + AddAssignByRef + Clone,
{
    fn add_assign(&mut self, rhs: Self) {
        if !rhs.is_empty() {
            // FIXME: We want to reuse allocations if at all possible
            *self = self.merge(&rhs);
        }
    }
}

impl<K, R> AddAssignByRef for OrderedColumnLeaf<K, R>
where
    K: Ord + Clone,
    R: Eq + HasZero + AddAssign + AddAssignByRef + Clone,
{
    fn add_assign_by_ref(&mut self, other: &Self) {
        if !other.is_empty() {
            // FIXME: We want to reuse allocations if at all possible
            *self = self.merge(other);
        }
    }
}

impl<K, R> AddByRef for OrderedColumnLeaf<K, R>
where
    K: Ord + Clone,
    R: Eq + HasZero + AddAssign + AddAssignByRef + Clone,
{
    fn add_by_ref(&self, rhs: &Self) -> Self {
        self.merge(rhs)
    }
}

impl<K, R> NegByRef for OrderedColumnLeaf<K, R>
where
    K: Ord + Clone,
    R: NegByRef,
{
    fn neg_by_ref(&self) -> Self {
        Self {
            keys: self.keys.clone(),
            diffs: self.diffs.iter().map(NegByRef::neg_by_ref).collect(),
        }
    }
}

impl<K, R> Neg for OrderedColumnLeaf<K, R>
where
    K: Ord + Clone,
    R: Neg<Output = R>,
{
    type Output = Self;

    fn neg(self) -> Self {
        Self {
            keys: self.keys,
            diffs: self.diffs.into_iter().map(Neg::neg).collect(),
        }
    }
}

impl<K, R> NumEntries for OrderedColumnLeaf<K, R>
where
    K: Ord + Clone,
    R: Eq + HasZero + AddAssign + AddAssignByRef + Clone,
{
    const CONST_NUM_ENTRIES: Option<usize> = None;

    #[inline]
    fn num_entries_shallow(&self) -> usize {
        self.len()
    }

    #[inline]
    fn num_entries_deep(&self) -> usize {
        // FIXME: Doesn't take element sizes into account
        self.len()
    }
}

impl<K, R> Default for OrderedColumnLeaf<K, R> {
    #[inline]
    fn default() -> Self {
        Self::empty()
    }
}
