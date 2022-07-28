//! Implementation using ordered keys and exponential search.

use crate::{
    algebra::{AddAssignByRef, AddByRef, HasZero, NegByRef},
    trace::{
        consolidation::consolidate_slice,
        layers::{advance, Builder, Cursor, MergeBuilder, Trie, TupleBuilder},
    },
    NumEntries, SharedRef,
};
use deepsize::DeepSizeOf;
use std::{
    cmp::{min, Ordering},
    fmt::{self, Display},
    hint::unreachable_unchecked,
    ops::{Add, AddAssign, Neg},
};

/// A layer of unordered values.
#[derive(Debug, Eq, PartialEq, Clone, Default, DeepSizeOf)]
pub struct OrderedSetLeaf<K, R> {
    // Invariant: keys.len == diffs.len
    keys: Vec<K>,
    diffs: Vec<R>,
}

impl<K: Ord + Clone, R: Eq + HasZero + AddAssignByRef + Clone> Trie for OrderedSetLeaf<K, R> {
    type Item = (K, R);
    type Cursor<'s> = OrderedSetLeafCursor<'s, K, R> where K: 's, R: 's;
    type MergeBuilder = OrderedSetLeafBuilder<K, R>;
    type TupleBuilder = UnorderedSetLeafBuilder<K, R>;

    #[inline]
    fn keys(&self) -> usize {
        unsafe { assume(self.keys.len() == self.diffs.len()) }
        self.keys.len()
    }

    #[inline]
    fn tuples(&self) -> usize {
        unsafe { assume(self.keys.len() == self.diffs.len()) }
        self.keys.len()
    }

    #[inline]
    fn cursor_from(&self, lower: usize, upper: usize) -> Self::Cursor<'_> {
        unsafe { assume(self.keys.len() == self.diffs.len()) }

        OrderedSetLeafCursor {
            storage: self,
            bounds: (lower, upper),
            pos: lower,
        }
    }
}

impl<K, R> Display for OrderedSetLeaf<K, R>
where
    K: Ord + Clone + Display,
    R: Eq + HasZero + AddAssignByRef + Clone + Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.cursor().fmt(f)
    }
}

// TODO: by-value merge
impl<K, R> Add<Self> for OrderedSetLeaf<K, R>
where
    K: Ord + Clone,
    R: Eq + HasZero + AddAssignByRef + Clone,
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

impl<K, R> AddAssign<Self> for OrderedSetLeaf<K, R>
where
    K: Ord + Clone,
    R: Eq + HasZero + AddAssignByRef + Clone,
{
    fn add_assign(&mut self, rhs: Self) {
        if !rhs.is_empty() {
            // FIXME: We want to reuse allocations if at all possible
            *self = self.merge(&rhs);
        }
    }
}

impl<K, R> AddAssignByRef for OrderedSetLeaf<K, R>
where
    K: Ord + Clone,
    R: Eq + HasZero + AddAssignByRef + Clone,
{
    fn add_assign_by_ref(&mut self, other: &Self) {
        if !other.is_empty() {
            // FIXME: We want to reuse allocations if at all possible
            *self = self.merge(other);
        }
    }
}

impl<K, R> AddByRef for OrderedSetLeaf<K, R>
where
    K: Ord + Clone,
    R: Eq + HasZero + AddAssignByRef + Clone,
{
    fn add_by_ref(&self, rhs: &Self) -> Self {
        self.merge(rhs)
    }
}

impl<K, R> NegByRef for OrderedSetLeaf<K, R>
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

impl<K, R> Neg for OrderedSetLeaf<K, R>
where
    K: Ord + Clone,
    R: Neg<Output = R>,
{
    type Output = Self;

    fn neg(self) -> Self {
        Self {
            keys: self.keys,
            // FIXME: I'd rather use an explicit loop that operates
            //        over mutable refs instead of relying on optimizations
            //        to remove the extra allocation
            diffs: self.diffs.into_iter().map(Neg::neg).collect(),
        }
    }
}

impl<K, R> NumEntries for OrderedSetLeaf<K, R>
where
    K: Ord + Clone,
    R: Eq + HasZero + AddAssignByRef + Clone,
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

impl<K, R> SharedRef for OrderedSetLeaf<K, R>
where
    K: Clone,
    R: Clone,
{
    type Target = Self;

    fn try_into_owned(self) -> Result<Self::Target, Self> {
        Ok(self)
    }
}

/// A builder for unordered values.
pub struct OrderedSetLeafBuilder<K, R> {
    // Invariant: `keys.len() == diffs.len`
    keys: Vec<K>,
    diffs: Vec<R>,
}

impl<K: Ord + Clone, R: Eq + HasZero + AddAssignByRef + Clone> Builder
    for OrderedSetLeafBuilder<K, R>
{
    type Trie = OrderedSetLeaf<K, R>;

    #[inline]
    fn boundary(&mut self) -> usize {
        unsafe { assume(self.keys.len() == self.diffs.len()) }
        self.keys.len()
    }

    #[inline]
    fn done(self) -> Self::Trie {
        unsafe { assume(self.keys.len() == self.diffs.len()) }

        OrderedSetLeaf {
            keys: self.keys,
            diffs: self.diffs,
        }
    }
}

impl<K: Ord + Clone, R: Eq + HasZero + AddAssignByRef + Clone> MergeBuilder
    for OrderedSetLeafBuilder<K, R>
{
    #[inline]
    fn with_capacity(left: &Self::Trie, right: &Self::Trie) -> Self {
        let capacity = left.keys() + right.keys();
        Self::with_key_capacity(capacity)
    }

    #[inline]
    fn with_key_capacity(capacity: usize) -> Self {
        Self {
            keys: Vec::with_capacity(capacity),
            diffs: Vec::with_capacity(capacity),
        }
    }

    #[inline]
    fn copy_range(&mut self, other: &Self::Trie, lower: usize, upper: usize) {
        unsafe {
            assume(self.keys.len() == self.diffs.len());
            assume(other.keys.len() == other.diffs.len());
        }

        self.keys.extend_from_slice(&other.keys[lower..upper]);
        self.diffs.extend_from_slice(&other.diffs[lower..upper]);
    }

    fn push_merge<'a>(
        &'a mut self,
        cursor1: <Self::Trie as Trie>::Cursor<'a>,
        cursor2: <Self::Trie as Trie>::Cursor<'a>,
    ) -> usize {
        unsafe { assume(self.keys.len() == self.diffs.len()) }

        let (trie1, trie2) = (cursor1.storage, cursor2.storage);
        unsafe {
            assume(trie1.keys.len() == trie1.diffs.len());
            assume(trie2.keys.len() == trie2.diffs.len());
        }

        let (mut lower1, upper1) = cursor1.bounds;
        let (mut lower2, upper2) = cursor2.bounds;

        let reserved = (upper1 - lower1) + (upper2 - lower2);
        self.keys.reserve(reserved);
        self.diffs.reserve(reserved);
        unsafe { assume(self.keys.len() == self.diffs.len()) }

        // while both mergees are still active
        while lower1 < upper1 && lower2 < upper2 {
            match trie1.keys[lower1].cmp(&trie2.keys[lower2]) {
                Ordering::Less => {
                    // determine how far we can advance lower1 until we reach/pass lower2
                    let step = 1 + advance(&trie1.keys[(1 + lower1)..upper1], |x| {
                        x < &trie2.keys[lower2]
                    });

                    let step = min(step, 1000);
                    <OrderedSetLeafBuilder<K, R> as MergeBuilder>::copy_range(
                        self,
                        trie1,
                        lower1,
                        lower1 + step,
                    );

                    lower1 += step;
                }

                Ordering::Equal => {
                    let mut sum = trie1.diffs[lower1].clone();
                    sum.add_assign_by_ref(&trie2.diffs[lower2]);

                    if !sum.is_zero() {
                        self.keys.push(trie1.keys[lower1].clone());
                        self.diffs.push(sum);
                    }

                    lower1 += 1;
                    lower2 += 1;
                }

                Ordering::Greater => {
                    // determine how far we can advance lower2 until we reach/pass lower1
                    let step = 1 + advance(&trie2.keys[(1 + lower2)..upper2], |x| {
                        x < &trie1.keys[lower1]
                    });

                    let step = min(step, 1000);
                    <OrderedSetLeafBuilder<K, R> as MergeBuilder>::copy_range(
                        self,
                        trie2,
                        lower2,
                        lower2 + step,
                    );

                    lower2 += step;
                }
            }
        }

        if lower1 < upper1 {
            <OrderedSetLeafBuilder<K, R> as MergeBuilder>::copy_range(self, trie1, lower1, upper1);
        }
        if lower2 < upper2 {
            <OrderedSetLeafBuilder<K, R> as MergeBuilder>::copy_range(self, trie2, lower2, upper2);
        }

        unsafe { assume(self.keys.len() == self.diffs.len()) }
        self.keys.len()
    }
}

impl<K: Ord + Clone, R: Eq + HasZero + AddAssignByRef + Clone> TupleBuilder
    for OrderedSetLeafBuilder<K, R>
{
    type Item = (K, R);

    #[inline]
    fn new() -> Self {
        Self {
            keys: Vec::new(),
            diffs: Vec::new(),
        }
    }

    #[inline]
    fn with_capacity(capacity: usize) -> Self {
        Self {
            keys: Vec::with_capacity(capacity),
            diffs: Vec::with_capacity(capacity),
        }
    }

    #[inline]
    fn tuples(&self) -> usize {
        unsafe { assume(self.keys.len() == self.diffs.len()) }
        self.keys.len()
    }

    #[inline]
    fn push_tuple(&mut self, (key, diff): (K, R)) {
        unsafe { assume(self.keys.len() == self.diffs.len()) }

        self.keys.push(key);
        self.diffs.push(diff);
    }
}

#[derive(DeepSizeOf)]
pub struct UnorderedSetLeafBuilder<K, R> {
    pub vals: Vec<(K, R)>,
    boundary: usize,
}

impl<K: Ord + Clone, R: Eq + HasZero + AddAssignByRef + Clone> Builder
    for UnorderedSetLeafBuilder<K, R>
{
    type Trie = OrderedSetLeaf<K, R>;

    fn boundary(&mut self) -> usize {
        let consolidated_len = consolidate_slice(&mut self.vals[self.boundary..]);
        self.boundary += consolidated_len;
        self.vals.truncate(self.boundary);
        self.boundary
    }

    fn done(mut self) -> Self::Trie {
        self.boundary();

        let (keys, diffs) = self.vals.into_iter().unzip();
        OrderedSetLeaf { keys, diffs }
    }
}

impl<K: Ord + Clone, R: Eq + HasZero + AddAssignByRef + Clone> TupleBuilder
    for UnorderedSetLeafBuilder<K, R>
{
    type Item = (K, R);

    #[inline]
    fn new() -> Self {
        Self {
            vals: Vec::new(),
            boundary: 0,
        }
    }

    #[inline]
    fn with_capacity(capacity: usize) -> Self {
        Self {
            vals: Vec::with_capacity(capacity),
            boundary: 0,
        }
    }

    #[inline]
    fn tuples(&self) -> usize {
        self.vals.len()
    }

    #[inline]
    fn push_tuple(&mut self, tuple: (K, R)) {
        self.vals.push(tuple)
    }
}

/// A cursor for walking through an unordered sequence of values.
///
/// This cursor does not support `seek`, though I'm not certain how to expose
/// this.
#[derive(Clone, Debug)]
pub struct OrderedSetLeafCursor<'s, K, R>
where
    K: Ord + Clone,
    R: Clone,
{
    pos: usize,
    storage: &'s OrderedSetLeaf<K, R>,
    bounds: (usize, usize),
}

impl<'s, K, R> OrderedSetLeafCursor<'s, K, R>
where
    K: Ord + Clone,
    R: Clone,
{
    #[inline]
    pub fn seek_key(&mut self, key: &K) {
        self.pos += advance(&self.storage.keys[self.pos..self.bounds.1], |k| k.lt(key));
    }

    #[inline]
    pub fn current_key(&self) -> &K {
        &self.storage.keys[self.pos]
    }

    #[inline]
    pub fn current_diff(&self) -> &R {
        &self.storage.diffs[self.pos]
    }
}

impl<'s, K, R> Cursor<'s> for OrderedSetLeafCursor<'s, K, R>
where
    K: Ord + Clone,
    R: Clone,
{
    type Key<'k> = (&'k K, &'k R)
    where
        Self: 'k;

    type ValueStorage = ();

    #[inline]
    fn keys(&self) -> usize {
        self.bounds.1 - self.bounds.0
    }

    // FIXME: If we could return our key value and our weight separately we'd be
    //        more efficient and cache-friendly. Maybe implement `trace::Cursor`
    //        directly?
    #[inline]
    fn key(&self) -> Self::Key<'s> {
        // Elide extra bounds checking
        unsafe { assume(self.storage.keys.len() == self.storage.diffs.len()) }
        if self.pos >= self.storage.keys.len() {
            cursor_position_oob(self.pos, self.storage.keys.len());
        }

        (&self.storage.keys[self.pos], &self.storage.diffs[self.pos])
    }

    #[inline]
    fn values(&self) {}

    #[inline]
    fn step(&mut self) {
        self.pos += 1;

        if !self.valid() {
            self.pos = self.bounds.1;
        }
    }

    #[inline]
    fn seek<'a>(&mut self, key: Self::Key<'a>)
    where
        's: 'a,
    {
        self.seek_key(key.0);
    }

    #[inline]
    fn valid(&self) -> bool {
        self.pos < self.bounds.1
    }

    #[inline]
    fn rewind(&mut self) {
        self.pos = self.bounds.0;
    }

    #[inline]
    fn reposition(&mut self, lower: usize, upper: usize) {
        self.pos = lower;
        self.bounds = (lower, upper);
    }
}

impl<'a, K, R> Display for OrderedSetLeafCursor<'a, K, R>
where
    K: Ord + Clone + Display,
    R: Eq + HasZero + AddAssignByRef + Clone + Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut cursor: OrderedSetLeafCursor<K, R> = self.clone();

        while cursor.valid() {
            let (key, val) = cursor.key();
            writeln!(f, "{} -> {}", key, val)?;
            cursor.step();
        }

        Ok(())
    }
}

#[cold]
#[inline(never)]
fn cursor_position_oob(position: usize, length: usize) -> ! {
    panic!("the cursor was at the invalid position {position} while the leaf was only {length} elements long")
}

/// Tells the optimizer that a condition is always true
///
/// # Safety
///
/// It's UB to call this function with `false` as the condition
#[inline(always)]
#[deny(unsafe_op_in_unsafe_fn)]
unsafe fn assume(cond: bool) {
    debug_assert!(cond, "called `assume()` on a false condition");

    if !cond {
        // Safety: It's UB for `cond` to be false
        unsafe { unreachable_unchecked() };
    }
}
