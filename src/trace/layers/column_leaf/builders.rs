use crate::{
    algebra::{AddAssignByRef, HasZero},
    trace::{
        consolidation::consolidate_from,
        layers::{
            advance, column_leaf::OrderedColumnLeaf, Builder, MergeBuilder, Trie, TupleBuilder,
        },
    },
    utils::assume,
};
use size_of::SizeOf;
use std::{
    cmp::{min, Ordering},
    ops::AddAssign,
};

/// A builder for ordered values
#[derive(SizeOf)]
pub struct OrderedColumnLeafBuilder<K, R> {
    // Invariant: `keys.len() == diffs.len()`
    keys: Vec<K>,
    diffs: Vec<R>,
}

impl<K, R> OrderedColumnLeafBuilder<K, R> {
    /// Get the length of the current builder
    pub(crate) fn len(&self) -> usize {
        unsafe { self.assume_invariants() }
        self.keys.len()
    }

    /// Assume the invariants of the current builder
    ///
    /// # Safety
    ///
    /// Requires that `keys` and `diffs` have the exact same length
    unsafe fn assume_invariants(&self) {
        assume(self.keys.len() == self.diffs.len())
    }
}

impl<K, R> Builder for OrderedColumnLeafBuilder<K, R>
where
    K: Ord + Clone,
    R: Eq + HasZero + AddAssign + AddAssignByRef + Clone,
{
    type Trie = OrderedColumnLeaf<K, R>;

    fn boundary(&mut self) -> usize {
        unsafe { self.assume_invariants() }
        self.keys.len()
    }

    fn done(self) -> Self::Trie {
        unsafe { self.assume_invariants() }

        // TODO: Should we call `.shrink_to_fit()` here?
        OrderedColumnLeaf {
            keys: self.keys,
            diffs: self.diffs,
        }
    }
}

impl<K, R> MergeBuilder for OrderedColumnLeafBuilder<K, R>
where
    K: Ord + Clone,
    R: Eq + HasZero + AddAssign + AddAssignByRef + Clone,
{
    fn with_capacity(left: &Self::Trie, right: &Self::Trie) -> Self {
        let capacity = Trie::keys(left) + Trie::keys(right);
        Self::with_key_capacity(capacity)
    }

    fn with_key_capacity(capacity: usize) -> Self {
        Self {
            keys: Vec::with_capacity(capacity),
            diffs: Vec::with_capacity(capacity),
        }
    }

    fn reserve(&mut self, additional: usize) {
        unsafe { self.assume_invariants() }
        self.keys.reserve(additional);
        self.diffs.reserve(additional);
        unsafe { self.assume_invariants() }
    }

    fn copy_range(&mut self, other: &Self::Trie, lower: usize, upper: usize) {
        unsafe {
            self.assume_invariants();
            other.assume_invariants();
        }

        assert!(lower <= other.keys.len() && upper <= other.keys.len());
        self.keys.extend_from_slice(&other.keys[lower..upper]);
        self.diffs.extend_from_slice(&other.diffs[lower..upper]);

        unsafe { self.assume_invariants() }
    }

    fn push_merge<'a>(
        &'a mut self,
        cursor1: <Self::Trie as Trie>::Cursor<'a>,
        cursor2: <Self::Trie as Trie>::Cursor<'a>,
    ) -> usize {
        unsafe { self.assume_invariants() }

        let (trie1, trie2) = (cursor1.storage(), cursor2.storage());
        unsafe {
            trie1.assume_invariants();
            trie2.assume_invariants();
        }

        let (mut lower1, upper1) = cursor1.bounds();
        let (mut lower2, upper2) = cursor2.bounds();

        let reserved = (upper1 - lower1) + (upper2 - lower2);
        self.reserve(reserved);

        // while both mergees are still active
        while lower1 < upper1 && lower2 < upper2 {
            match trie1.keys[lower1].cmp(&trie2.keys[lower2]) {
                Ordering::Less => {
                    // determine how far we can advance lower1 until we reach/pass lower2
                    let step = 1 + advance(&trie1.keys[(1 + lower1)..upper1], |x| {
                        x < &trie2.keys[lower2]
                    });

                    let step = min(step, 1000);
                    self.copy_range(trie1, lower1, lower1 + step);

                    lower1 += step;
                }

                Ordering::Equal => {
                    let mut sum = trie1.diffs[lower1].clone();
                    sum.add_assign_by_ref(&trie2.diffs[lower2]);

                    if !sum.is_zero() {
                        self.push_tuple((trie1.keys[lower1].clone(), sum));
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
                    self.copy_range(trie2, lower2, lower2 + step);

                    lower2 += step;
                }
            }
        }

        if lower1 < upper1 {
            self.copy_range(trie1, lower1, upper1);
        }
        if lower2 < upper2 {
            self.copy_range(trie2, lower2, upper2);
        }

        unsafe { self.assume_invariants() }
        self.keys.len()
    }
}

impl<K, R> TupleBuilder for OrderedColumnLeafBuilder<K, R>
where
    K: Ord + Clone,
    R: Eq + HasZero + AddAssign + AddAssignByRef + Clone,
{
    type Item = (K, R);

    fn new() -> Self {
        Self {
            keys: Vec::new(),
            diffs: Vec::new(),
        }
    }

    fn with_capacity(capacity: usize) -> Self {
        Self {
            keys: Vec::with_capacity(capacity),
            diffs: Vec::with_capacity(capacity),
        }
    }

    fn reserve_tuples(&mut self, additional: usize) {
        self.keys.reserve(additional);
        self.diffs.reserve(additional);
    }

    fn tuples(&self) -> usize {
        unsafe { self.assume_invariants() }
        self.keys.len()
    }

    fn push_tuple(&mut self, (key, diff): (K, R)) {
        // if cfg!(debug_assertions) && !self.keys.is_empty() {
        //     debug_assert!(
        //         self.keys.last().unwrap() <= &key,
        //         "OrderedSetLeafBuilder expects sorted values to be passed to \
        //          `TupleBuilder::push_tuple()`",
        //      );
        // }

        unsafe { self.assume_invariants() }
        self.keys.push(key);
        self.diffs.push(diff);
        unsafe { self.assume_invariants() }
    }
}

/// A builder for unordered values
#[derive(Debug, Clone, SizeOf)]
pub struct UnorderedColumnLeafBuilder<K, R> {
    tuples: Vec<(K, R)>,
    boundary: usize,
}

impl<K, R> UnorderedColumnLeafBuilder<K, R> {
    /// Create a new `UnorderedColumnLeafBuilder`
    pub const fn new() -> Self {
        Self {
            tuples: Vec::new(),
            boundary: 0,
        }
    }

    /// Get the length of the current builder
    pub(crate) fn len(&self) -> usize {
        self.tuples.len()
    }
}

impl<K, R> Builder for UnorderedColumnLeafBuilder<K, R>
where
    K: Ord + Clone,
    R: HasZero + AddAssign + AddAssignByRef + Eq + Clone,
{
    type Trie = OrderedColumnLeaf<K, R>;

    fn boundary(&mut self) -> usize {
        consolidate_from(&mut self.tuples, self.boundary);
        self.boundary = self.len();
        self.boundary
    }

    fn done(mut self) -> Self::Trie {
        self.boundary();

        let (keys, diffs) = self.tuples.into_iter().unzip();
        // TODO: The indices buffer is dropped here, can we reuse it for other builders?
        OrderedColumnLeaf { keys, diffs }
    }
}

impl<K, R> TupleBuilder for UnorderedColumnLeafBuilder<K, R>
where
    K: Ord + Clone,
    R: HasZero + AddAssign + AddAssignByRef + Eq + Clone,
{
    type Item = (K, R);

    fn new() -> Self {
        Self {
            tuples: Vec::new(),
            boundary: 0,
        }
    }

    fn with_capacity(capacity: usize) -> Self {
        Self {
            tuples: Vec::with_capacity(capacity),
            boundary: 0,
        }
    }

    fn reserve_tuples(&mut self, additional: usize) {
        self.tuples.reserve(additional);
    }

    fn tuples(&self) -> usize {
        self.len()
    }

    fn push_tuple(&mut self, tuple: (K, R)) {
        self.tuples.push(tuple);
    }

    fn extend_tuples<I>(&mut self, tuples: I)
    where
        I: IntoIterator<Item = Self::Item>,
    {
        self.tuples.extend(tuples);
    }
}

impl<K, R> Default for UnorderedColumnLeafBuilder<K, R> {
    fn default() -> Self {
        Self::new()
    }
}
