//! Implementation using ordered keys and exponential search over a
//! struct-of-array container

mod builders;
mod cursor;

pub use builders::{OrderedColumnLeafBuilder, UnorderedColumnLeafBuilder};
pub use cursor::ColumnLeafCursor;

use crate::{
    algebra::{AddAssignByRef, AddByRef, HasZero, NegByRef},
    trace::layers::Trie,
    utils::assume,
    NumEntries,
};
use deepsize::DeepSizeOf;
use std::{
    fmt::{self, Display},
    ops::{Add, AddAssign, Neg},
};

/// A layer of unordered values.
#[derive(Debug, Clone, Eq, PartialEq, Default, DeepSizeOf)]
pub struct OrderedColumnLeaf<K, R> {
    // Invariant: keys.len == diffs.len
    keys: Vec<K>,
    diffs: Vec<R>,
}

impl<K, R> OrderedColumnLeaf<K, R> {
    pub const fn empty() -> Self {
        Self {
            keys: Vec::new(),
            diffs: Vec::new(),
        }
    }

    #[inline]
    #[doc(hidden)]
    pub fn diffs_mut(&mut self) -> &mut [R] {
        unsafe { self.assume_invariants() }
        &mut self.diffs
    }

    // fn len(&self) -> usize {
    //     unsafe { self.assume_invariants() }
    //     self.keys.len()
    // }

    /// Assume the invariants of the current builder
    ///
    /// # Safety
    ///
    /// Requires that `keys` and `diffs` have the exact same length
    #[inline]
    unsafe fn assume_invariants(&self) {
        unsafe { assume(self.keys.len() == self.diffs.len()) }
    }

    // fn into_uninit(self) -> OrderedColumnLeaf<MaybeUninit<K>, MaybeUninit<R>> {
    //     unsafe { self.assume_invariants() }
    //
    //     let mut keys = ManuallyDrop::new(self.keys);
    //     let (len, cap, ptr) = (keys.len(), keys.capacity(), keys.as_mut_ptr());
    //     let keys = unsafe { Vec::from_raw_parts(ptr.cast(), len, cap) };
    //
    //     let mut diffs = ManuallyDrop::new(self.diffs);
    //     let (len, cap, ptr) = (diffs.len(), diffs.capacity(),
    // diffs.as_mut_ptr());     let diffs = unsafe {
    // Vec::from_raw_parts(ptr.cast(), len, cap) };
    //
    //     OrderedColumnLeaf { keys, diffs }
    // }
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
        unsafe { self.assume_invariants() }
        self.keys.len()
    }

    #[inline]
    fn tuples(&self) -> usize {
        unsafe { self.assume_invariants() }
        self.keys.len()
    }

    #[inline]
    fn cursor_from(&self, lower: usize, upper: usize) -> Self::Cursor<'_> {
        unsafe { self.assume_invariants() }
        ColumnLeafCursor::new(lower, self, (lower, upper))
    }
}

impl<K, R> Display for OrderedColumnLeaf<K, R>
where
    K: Ord + Clone + Display,
    R: Eq + HasZero + AddAssign + AddAssignByRef + Clone + Display,
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
