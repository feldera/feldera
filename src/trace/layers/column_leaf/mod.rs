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
    trace::{MonoidValue, layers::Trie},
    utils::{assume, cast_uninit_vec},
    DBData, NumEntries,
};
use size_of::SizeOf;
use std::{
    fmt::{self, Display},
    mem::MaybeUninit,
    ops::{Add, AddAssign, Neg},
    ptr,
    slice::SliceIndex,
};

/// A layer of unordered values.
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
    #[allow(dead_code)]
    pub(crate) fn len(&self) -> usize {
        unsafe { self.assume_invariants() }
        self.keys.len()
    }

    /// Get mutable references to the current leaf's keys and differences
    pub(crate) fn columns_mut(&mut self) -> (&mut [K], &mut [R]) {
        unsafe { self.assume_invariants() }
        (&mut self.keys, &mut self.diffs)
    }

    /// Get a mutable reference to the current leaf's key values
    pub(crate) fn keys_mut(&mut self) -> &mut [K] {
        unsafe { self.assume_invariants() }
        &mut self.keys
    }

    /// Get a mutable reference to the current leaf's difference values
    #[doc(hidden)]
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

    /// Assume the invariants of the current builder
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

    #[inline]
    fn keys(&self) -> usize {
        self.len()
    }

    #[inline]
    fn tuples(&self) -> usize {
        self.len()
    }

    #[inline]
    fn cursor_from(&self, lower: usize, upper: usize) -> Self::Cursor<'_> {
        unsafe { self.assume_invariants() }
        ColumnLeafCursor::new(lower, self, (lower, upper))
    }
}

impl<K, R> Display for OrderedColumnLeaf<K, R>
where
    K: DBData,
    R: DBData + MonoidValue,
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
        self.keys()
    }

    #[inline]
    fn num_entries_deep(&self) -> usize {
        // FIXME: Doesn't take element sizes into account
        self.keys()
    }
}

impl<K, R> Default for OrderedColumnLeaf<K, R> {
    #[inline]
    fn default() -> Self {
        Self::empty()
    }
}
