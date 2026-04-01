//! A binary heap implementation optimized for DBSP.
//!
//! This implementation was shamelessly stole from the standard library and extended with
//! several operations necessary to efficiently implement DBSP-style merge operations. In DBSP,
//! when merging sorted batches, we need to simultaneously peek all cursors the the max key or
//! value. The standard heap implementation only allows peeking a single max element at a time;
//! the next max can only be observed by removing or updating the current max first.
//!
//! We add:
//! - `peek_all` operation, which peeks all max elements at once by performing
//!   `num_max * 2` comparisons.
//! - `remove` operation, which removes an element from the heap by index.
//! - `update_pos_sift_down` operation, which updates an element at a given
//!   index to a smaller value and sifting it down.

// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright (c) 2026 Feldera Inc.
// Copyright (c) The Rust Project Developers

use std::cmp::Ordering;
use std::iter::FusedIterator;
use std::mem::{self, ManuallyDrop, swap};
use std::num::NonZero;
use std::ops::{Deref, DerefMut};
use std::{fmt, ptr};

use std::collections::TryReserveError;
use std::slice;
use std::vec::{self, Vec};

pub struct BinaryHeap<T, C> {
    data: Vec<T>,
    cmp: C,
}

/// Structure wrapping a mutable reference to the greatest item on a
/// `BinaryHeap`.
///
/// This `struct` is created by the [`peek_mut`] method on [`BinaryHeap`]. See
/// its documentation for more.
///
/// [`peek_mut`]: BinaryHeap::peek_mut
pub struct PeekMut<'a, T: 'a, C>
where
    C: Fn(&T, &T) -> Ordering,
{
    heap: &'a mut BinaryHeap<T, C>,
    // If a set_len + sift_down are required, this is Some. If a &mut T has not
    // yet been exposed to peek_mut()'s caller, it's None.
    original_len: Option<NonZero<usize>>,
}

impl<T: fmt::Debug, C> fmt::Debug for PeekMut<'_, T, C>
where
    C: Fn(&T, &T) -> Ordering,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("PeekMut").field(&self.heap.data[0]).finish()
    }
}

impl<T, C> Drop for PeekMut<'_, T, C>
where
    C: Fn(&T, &T) -> Ordering,
{
    fn drop(&mut self) {
        if let Some(original_len) = self.original_len {
            // SAFETY: That's how many elements were in the Vec at the time of
            // the PeekMut::deref_mut call, and therefore also at the time of
            // the BinaryHeap::peek_mut call. Since the PeekMut did not end up
            // getting leaked, we are now undoing the leak amplification that
            // the DerefMut prepared for.
            unsafe { self.heap.data.set_len(original_len.get()) };

            // SAFETY: PeekMut is only instantiated for non-empty heaps.
            unsafe { self.heap.sift_down(0) };
        }
    }
}

impl<T, C> Deref for PeekMut<'_, T, C>
where
    C: Fn(&T, &T) -> Ordering,
{
    type Target = T;
    fn deref(&self) -> &T {
        debug_assert!(!self.heap.is_empty());
        // SAFE: PeekMut is only instantiated for non-empty heaps
        unsafe { self.heap.data.get_unchecked(0) }
    }
}

impl<T, C> DerefMut for PeekMut<'_, T, C>
where
    C: Fn(&T, &T) -> Ordering,
{
    fn deref_mut(&mut self) -> &mut T {
        debug_assert!(!self.heap.is_empty());

        let len = self.heap.len();
        if len > 1 {
            // Here we preemptively leak all the rest of the underlying vector
            // after the currently max element. If the caller mutates the &mut T
            // we're about to give them, and then leaks the PeekMut, all these
            // elements will remain leaked. If they don't leak the PeekMut, then
            // either Drop or PeekMut::pop will un-leak the vector elements.
            //
            // This is technique is described throughout several other places in
            // the standard library as "leak amplification".
            unsafe {
                // SAFETY: len > 1 so len != 0.
                self.original_len = Some(NonZero::new_unchecked(len));
                // SAFETY: len > 1 so all this does for now is leak elements,
                // which is safe.
                self.heap.data.set_len(1);
            }
        }

        // SAFE: PeekMut is only instantiated for non-empty heaps
        unsafe { self.heap.data.get_unchecked_mut(0) }
    }
}

impl<'a, T, C> PeekMut<'a, T, C>
where
    C: Fn(&T, &T) -> Ordering,
{
    /// Sifts the current element to its new position.
    ///
    /// Afterwards refers to the new element. Returns if the element changed.
    #[must_use = "is equivalent to dropping and getting a new PeekMut except for return information"]
    pub fn refresh(&mut self) -> bool {
        // The length of the underlying heap is unchanged by sifting down. The value stored for leak
        // amplification thus remains accurate. We erase the leak amplification firstly because the
        // operation is then equivalent to constructing a new PeekMut and secondly this avoids any
        // future complication where original_len being non-empty would be interpreted as the heap
        // having been leak amplified instead of checking the heap itself.
        if let Some(original_len) = self.original_len.take() {
            // SAFETY: This is how many elements were in the Vec at the time of
            // the BinaryHeap::peek_mut call.
            unsafe { self.heap.data.set_len(original_len.get()) };

            // The length of the heap did not change by sifting, upholding our own invariants.

            // SAFETY: PeekMut is only instantiated for non-empty heaps.
            (unsafe { self.heap.sift_down(0) }) != 0
        } else {
            // The element was not modified.
            false
        }
    }

    /// Removes the peeked value from the heap and returns it.
    pub fn pop(mut this: PeekMut<'a, T, C>) -> T {
        if let Some(original_len) = this.original_len.take() {
            // SAFETY: This is how many elements were in the Vec at the time of
            // the BinaryHeap::peek_mut call.
            unsafe { this.heap.data.set_len(original_len.get()) };

            // Unlike in Drop, here we don't also need to do a sift_down even if
            // the caller could've mutated the element. It is removed from the
            // heap on the next line and pop() is not sensitive to its value.
        }

        // SAFETY: Have a `PeekMut` element proves that the associated binary heap being non-empty,
        // so the `pop` operation will not fail.
        unsafe { this.heap.pop().unwrap_unchecked() }
    }
}

impl<T: Clone, C: Clone> Clone for BinaryHeap<T, C> {
    fn clone(&self) -> Self {
        BinaryHeap {
            data: self.data.clone(),
            cmp: self.cmp.clone(),
        }
    }

    /// Overwrites the contents of `self` with a clone of the contents of `source`.
    ///
    /// This method is preferred over simply assigning `source.clone()` to `self`,
    /// as it avoids reallocation if possible.
    ///
    /// See [`Vec::clone_from()`] for more details.
    fn clone_from(&mut self, source: &Self) {
        self.data.clone_from(&source.data);
    }
}

impl<T: fmt::Debug, C> fmt::Debug for BinaryHeap<T, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

struct RebuildOnDrop<'a, T, C>
where
    C: Fn(&T, &T) -> Ordering,
{
    heap: &'a mut BinaryHeap<T, C>,
    rebuild_from: usize,
}

impl<T, C> Drop for RebuildOnDrop<'_, T, C>
where
    C: Fn(&T, &T) -> Ordering,
{
    fn drop(&mut self) {
        self.heap.rebuild_tail(self.rebuild_from);
    }
}

impl<T, C> BinaryHeap<T, C>
where
    C: Fn(&T, &T) -> Ordering,
{
    pub const fn new(cmp: C) -> BinaryHeap<T, C> {
        BinaryHeap { data: vec![], cmp }
    }

    pub fn from_vec(vec: Vec<T>, cmp: C) -> BinaryHeap<T, C> {
        let mut heap = BinaryHeap { data: vec, cmp };
        heap.rebuild();
        heap
    }

    /// Constructs a binary heap from a vector without checking if it's a valid heap.
    ///
    /// # Safety
    ///
    /// `vec` must be a valid binary heap built using `cmp`.
    pub unsafe fn from_vec_unchecked(vec: Vec<T>, cmp: C) -> BinaryHeap<T, C> {
        BinaryHeap { data: vec, cmp }
    }

    #[must_use]
    pub fn with_capacity(capacity: usize, cmp: C) -> BinaryHeap<T, C> {
        BinaryHeap {
            data: Vec::with_capacity(capacity),
            cmp,
        }
    }
}

impl<T, C> BinaryHeap<T, C>
where
    C: Fn(&T, &T) -> Ordering,
{
    /// Returns a mutable reference to the greatest item in the binary heap, or
    /// `None` if it is empty.
    ///
    /// Note: If the `PeekMut` value is leaked, some heap elements might get
    /// leaked along with it, but the remaining elements will remain a valid
    /// heap.
    ///
    /// # Time complexity
    ///
    /// If the item is modified then the worst case time complexity is *O*(log(*n*)),
    /// otherwise it's *O*(1).
    pub fn peek_mut(&mut self) -> Option<PeekMut<'_, T, C>> {
        if self.is_empty() {
            None
        } else {
            Some(PeekMut {
                heap: self,
                original_len: None,
            })
        }
    }

    /// Removes the greatest item from the binary heap and returns it, or `None` if it
    /// is empty.
    ///
    /// # Time complexity
    ///
    /// The worst case cost of `pop` on a heap containing *n* elements is *O*(log(*n*)).
    pub fn pop(&mut self) -> Option<T> {
        self.data.pop().map(|mut item| {
            if !self.is_empty() {
                swap(&mut item, &mut self.data[0]);
                // SAFETY: !self.is_empty() means that self.len() > 0
                unsafe { self.sift_down_to_bottom(0) };
            }
            item
        })
    }

    /// Replaces the element at position `pos` with `item` and sifts it down.
    ///
    /// # Safety
    ///
    /// `item` must be <= the element at position `pos`.
    pub unsafe fn update_pos_sift_down(&mut self, pos: usize, item: T) {
        debug_assert!((self.cmp)(&item, &self.data[pos]) != Ordering::Greater);
        self.data[pos] = item;
        unsafe { self.sift_down(pos) };
    }

    /// Peek all max elements.
    ///
    /// Invokes `cb` for every element equal to the current maximum value.
    /// The first argument to `cb` is the index of the element in the heap's internal vector.
    /// The second argument is a reference to the element.
    ///
    /// The traversal prunes subtrees as soon as a node is smaller than the
    /// maximum, so work is proportional to the number of maximal elements.
    ///
    /// Elements are visited in breadth-first order: the root is visited first,
    /// followed by its children, then its grandchildren, etc.
    ///
    /// The `queue` argument is used as scratch space for the traversal
    /// to avoid a dynamic allocation.
    pub fn peek_all(&self, mut cb: impl FnMut(usize, &T), queue: &mut Vec<usize>) {
        let Some(max) = self.peek() else {
            return;
        };

        // Root is known to be equal to `max`; avoid a redundant self-compare.
        cb(0, max);

        let mut head = 0usize;
        queue.clear();

        let len = self.data.len();

        if len > 1 {
            queue.push(1);
        }
        if len > 2 {
            queue.push(2);
        }

        while let Some(&index) = queue.get(head) {
            head += 1;

            // SAFETY: values written to `queue` are valid indices into `self.data`.
            let node = unsafe { self.data.get_unchecked(index) };
            if (self.cmp)(node, max) != Ordering::Equal {
                continue;
            }

            cb(index, node);

            let left = index * 2 + 1;
            if left < len {
                queue.push(left);
            }

            let right = left + 1;
            if right < len {
                queue.push(right);
            }
        }
    }

    /// Removes the element at position `index` from the binary heap.
    ///
    /// # Panics
    ///
    /// Panics if `index` is out of bounds.
    pub fn remove(&mut self, index: usize) {
        let last = self.data.pop().expect("index is in bounds");

        // Removing the last element needs no reheapification.
        if index == self.data.len() {
            return;
        }

        self.data[index] = last;

        // SAFETY: `index < self.len()` after the previous line.
        unsafe {
            self.sift_down_to_bottom(index);
        }
    }

    /// Pushes an item onto the binary heap.
    ///
    /// # Time complexity
    ///
    /// The expected cost of `push`, averaged over every possible ordering of
    /// the elements being pushed, and over a sufficiently large number of
    /// pushes, is *O*(1). This is the most meaningful cost metric when pushing
    /// elements that are *not* already in any sorted pattern.
    ///
    /// The time complexity degrades if elements are pushed in predominantly
    /// ascending order. In the worst case, elements are pushed in ascending
    /// sorted order and the amortized cost per push is *O*(log(*n*)) against a heap
    /// containing *n* elements.
    ///
    /// The worst case cost of a *single* call to `push` is *O*(*n*). The worst case
    /// occurs when capacity is exhausted and needs a resize. The resize cost
    /// has been amortized in the previous figures.
    pub fn push(&mut self, item: T) {
        let old_len = self.len();
        self.data.push(item);
        // SAFETY: Since we pushed a new item it means that
        //  old_len = self.len() - 1 < self.len()
        unsafe { self.sift_up(0, old_len) };
    }

    /// Consumes the `BinaryHeap` and returns a vector in sorted
    /// (ascending) order.
    ///
    pub fn into_sorted_vec(mut self) -> Vec<T> {
        let mut end = self.len();
        while end > 1 {
            end -= 1;
            // SAFETY: `end` goes from `self.len() - 1` to 1 (both included),
            //  so it's always a valid index to access.
            //  It is safe to access index 0 (i.e. `ptr`), because
            //  1 <= end < self.len(), which means self.len() >= 2.
            unsafe {
                let ptr = self.data.as_mut_ptr();
                ptr::swap(ptr, ptr.add(end));
            }
            // SAFETY: `end` goes from `self.len() - 1` to 1 (both included) so:
            //  0 < 1 <= end <= self.len() - 1 < self.len()
            //  Which means 0 < end and end < self.len().
            unsafe { self.sift_down_range(0, end) };
        }
        self.into_vec()
    }

    // The implementations of sift_up and sift_down use unsafe blocks in
    // order to move an element out of the vector (leaving behind a
    // hole), shift along the others and move the removed element back into the
    // vector at the final location of the hole.
    // The `Hole` type is used to represent this, and make sure
    // the hole is filled back at the end of its scope, even on panic.
    // Using a hole reduces the constant factor compared to using swaps,
    // which involves twice as many moves.

    /// # Safety
    ///
    /// The caller must guarantee that `pos < self.len()`.
    ///
    /// Returns the new position of the element.
    unsafe fn sift_up(&mut self, start: usize, pos: usize) -> usize {
        // Take out the value at `pos` and create a hole.
        // SAFETY: The caller guarantees that pos < self.len()
        let mut hole = unsafe { Hole::new(&mut self.data, pos) };

        while hole.pos() > start {
            let parent = (hole.pos() - 1) / 2;

            // SAFETY: hole.pos() > start >= 0, which means hole.pos() > 0
            //  and so hole.pos() - 1 can't underflow.
            //  This guarantees that parent < hole.pos() so
            //  it's a valid index and also != hole.pos().
            if (self.cmp)(hole.element(), unsafe { hole.get(parent) }) != Ordering::Greater {
                break;
            }

            // SAFETY: Same as above
            unsafe { hole.move_to(parent) };
        }

        hole.pos()
    }

    /// Take an element at `pos` and move it down the heap,
    /// while its children are larger.
    ///
    /// Returns the new position of the element.
    ///
    /// # Safety
    ///
    /// The caller must guarantee that `pos < end <= self.len()`.
    unsafe fn sift_down_range(&mut self, pos: usize, end: usize) -> usize {
        // SAFETY: The caller guarantees that pos < end <= self.len().
        let mut hole = unsafe { Hole::new(&mut self.data, pos) };
        let mut child = 2 * hole.pos() + 1;

        // Loop invariant: child == 2 * hole.pos() + 1.
        while child <= end.saturating_sub(2) {
            // compare with the greater of the two children
            // SAFETY: child < end - 1 < self.len() and
            //  child + 1 < end <= self.len(), so they're valid indexes.
            //  child == 2 * hole.pos() + 1 != hole.pos() and
            //  child + 1 == 2 * hole.pos() + 2 != hole.pos().
            // FIXME: 2 * hole.pos() + 1 or 2 * hole.pos() + 2 could overflow
            //  if T is a ZST
            child += ((self.cmp)(unsafe { hole.get(child) }, unsafe { hole.get(child + 1) })
                != Ordering::Greater) as usize;

            // if we are already in order, stop.
            // SAFETY: child is now either the old child or the old child+1
            //  We already proven that both are < self.len() and != hole.pos()
            if (self.cmp)(hole.element(), unsafe { hole.get(child) }) != Ordering::Less {
                return hole.pos();
            }

            // SAFETY: same as above.
            unsafe { hole.move_to(child) };
            child = 2 * hole.pos() + 1;
        }

        // SAFETY: && short circuit, which means that in the
        //  second condition it's already true that child == end - 1 < self.len().
        if child == end - 1
            && (self.cmp)(hole.element(), unsafe { hole.get(child) }) == Ordering::Less
        {
            // SAFETY: child is already proven to be a valid index and
            //  child == 2 * hole.pos() + 1 != hole.pos().
            unsafe { hole.move_to(child) };
        }

        hole.pos()
    }

    /// # Safety
    ///
    /// The caller must guarantee that `pos < self.len()`.
    pub unsafe fn sift_down(&mut self, pos: usize) -> usize {
        let len = self.len();
        // SAFETY: pos < len is guaranteed by the caller and
        //  obviously len = self.len() <= self.len().
        unsafe { self.sift_down_range(pos, len) }
    }

    /// Take an element at `pos` and move it all the way down the heap,
    /// then sift it up to its position.
    ///
    /// Note: This is faster when the element is known to be large / should
    /// be closer to the bottom.
    ///
    /// # Safety
    ///
    /// The caller must guarantee that `pos < self.len()`.
    pub unsafe fn sift_down_to_bottom(&mut self, mut pos: usize) {
        let end = self.len();
        let start = pos;

        // SAFETY: The caller guarantees that pos < self.len().
        let mut hole = unsafe { Hole::new(&mut self.data, pos) };
        let mut child = 2 * hole.pos() + 1;

        // Loop invariant: child == 2 * hole.pos() + 1.
        while child <= end.saturating_sub(2) {
            // SAFETY: child < end - 1 < self.len() and
            //  child + 1 < end <= self.len(), so they're valid indexes.
            //  child == 2 * hole.pos() + 1 != hole.pos() and
            //  child + 1 == 2 * hole.pos() + 2 != hole.pos().
            // FIXME: 2 * hole.pos() + 1 or 2 * hole.pos() + 2 could overflow
            //  if T is a ZST
            child += ((self.cmp)(unsafe { hole.get(child) }, unsafe { hole.get(child + 1) })
                != Ordering::Greater) as usize;

            // SAFETY: Same as above
            unsafe { hole.move_to(child) };
            child = 2 * hole.pos() + 1;
        }

        if child == end - 1 {
            // SAFETY: child == end - 1 < self.len(), so it's a valid index
            //  and child == 2 * hole.pos() + 1 != hole.pos().
            unsafe { hole.move_to(child) };
        }
        pos = hole.pos();
        drop(hole);

        // SAFETY: pos is the position in the hole and was already proven
        //  to be a valid index.
        unsafe { self.sift_up(start, pos) };
    }

    /// Rebuild assuming data[0..start] is still a proper heap.
    fn rebuild_tail(&mut self, start: usize) {
        if start == self.len() {
            return;
        }

        let tail_len = self.len() - start;

        #[inline(always)]
        fn log2_fast(x: usize) -> usize {
            (usize::BITS - x.leading_zeros() - 1) as usize
        }

        // `rebuild` takes O(self.len()) operations
        // and about 2 * self.len() comparisons in the worst case
        // while repeating `sift_up` takes O(tail_len * log(start)) operations
        // and about 1 * tail_len * log_2(start) comparisons in the worst case,
        // assuming start >= tail_len. For larger heaps, the crossover point
        // no longer follows this reasoning and was determined empirically.
        let better_to_rebuild = if start < tail_len {
            true
        } else if self.len() <= 2048 {
            2 * self.len() < tail_len * log2_fast(start)
        } else {
            2 * self.len() < tail_len * 11
        };

        if better_to_rebuild {
            self.rebuild();
        } else {
            for i in start..self.len() {
                // SAFETY: The index `i` is always less than self.len().
                unsafe { self.sift_up(0, i) };
            }
        }
    }

    fn rebuild(&mut self) {
        let mut n = self.len() / 2;
        while n > 0 {
            n -= 1;
            // SAFETY: n starts from self.len() / 2 and goes down to 0.
            //  The only case when !(n < self.len()) is if
            //  self.len() == 0, but it's ruled out by the loop condition.
            unsafe { self.sift_down(n) };
        }
    }

    /// Moves all the elements of `other` into `self`, leaving `other` empty.
    pub fn append(&mut self, other: &mut Self) {
        if self.len() < other.len() {
            swap(self, other);
        }

        let start = self.data.len();

        self.data.append(&mut other.data);

        self.rebuild_tail(start);
    }

    /// Clears the binary heap, returning an iterator over the removed elements
    /// in heap order. If the iterator is dropped before being fully consumed,
    /// it drops the remaining elements in heap order.
    ///
    /// The returned iterator keeps a mutable borrow on the heap to optimize
    /// its implementation.
    ///
    /// Note:
    /// * `.drain_sorted()` is *O*(*n* \* log(*n*)); much slower than `.drain()`.
    ///   You should use the latter for most cases.
    #[inline]
    pub fn drain_sorted(&mut self) -> DrainSorted<'_, T, C> {
        DrainSorted { inner: self }
    }

    /// Retains only the elements specified by the predicate.
    ///
    /// In other words, remove all elements `e` for which `f(&e)` returns
    /// `false`. The elements are visited in unsorted (and unspecified) order.
    pub fn retain<F>(&mut self, mut f: F)
    where
        F: FnMut(&T) -> bool,
    {
        // rebuild_start will be updated to the first touched element below, and the rebuild will
        // only be done for the tail.
        let mut guard = RebuildOnDrop {
            rebuild_from: self.len(),
            heap: self,
        };
        let mut i = 0;

        guard.heap.data.retain(|e| {
            let keep = f(e);
            if !keep && i < guard.rebuild_from {
                guard.rebuild_from = i;
            }
            i += 1;
            keep
        });
    }
}

impl<T, C> BinaryHeap<T, C> {
    /// Returns an iterator visiting all values in the underlying vector, in
    /// arbitrary order.
    pub fn iter(&self) -> Iter<'_, T> {
        Iter {
            iter: self.data.iter(),
        }
    }

    /// Returns an iterator which retrieves elements in heap order.
    ///
    /// This method consumes the original heap.
    pub fn into_iter_sorted(self) -> IntoIterSorted<T, C> {
        IntoIterSorted { inner: self }
    }

    /// Returns the greatest item in the binary heap, or `None` if it is empty.
    ///
    /// # Time complexity
    ///
    /// Cost is *O*(1) in the worst case.
    #[must_use]
    pub fn peek(&self) -> Option<&T> {
        self.data.first()
    }

    /// Returns the number of elements the binary heap can hold without reallocating.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.data.capacity()
    }

    /// Reserves the minimum capacity for at least `additional` elements more than
    /// the current length. Unlike [`reserve`], this will not
    /// deliberately over-allocate to speculatively avoid frequent allocations.
    /// After calling `reserve_exact`, capacity will be greater than or equal to
    /// `self.len() + additional`. Does nothing if the capacity is already
    /// sufficient.
    ///
    /// [`reserve`]: BinaryHeap::reserve
    ///
    /// # Panics
    ///
    /// Panics if the new capacity overflows [`usize`].
    pub fn reserve_exact(&mut self, additional: usize) {
        self.data.reserve_exact(additional);
    }

    /// Reserves capacity for at least `additional` elements more than the
    /// current length. The allocator may reserve more space to speculatively
    /// avoid frequent allocations. After calling `reserve`,
    /// capacity will be greater than or equal to `self.len() + additional`.
    /// Does nothing if capacity is already sufficient.
    ///
    /// # Panics
    ///
    /// Panics if the new capacity overflows [`usize`].
    pub fn reserve(&mut self, additional: usize) {
        self.data.reserve(additional);
    }

    /// Tries to reserve the minimum capacity for at least `additional` elements
    /// more than the current length. Unlike [`try_reserve`], this will not
    /// deliberately over-allocate to speculatively avoid frequent allocations.
    /// After calling `try_reserve_exact`, capacity will be greater than or
    /// equal to `self.len() + additional` if it returns `Ok(())`.
    /// Does nothing if the capacity is already sufficient.
    ///
    /// Note that the allocator may give the collection more space than it
    /// requests. Therefore, capacity can not be relied upon to be precisely
    /// minimal. Prefer [`try_reserve`] if future insertions are expected.
    ///
    /// [`try_reserve`]: BinaryHeap::try_reserve
    ///
    /// # Errors
    ///
    /// If the capacity overflows, or the allocator reports a failure, then an error
    /// is returned.
    pub fn try_reserve_exact(&mut self, additional: usize) -> Result<(), TryReserveError> {
        self.data.try_reserve_exact(additional)
    }

    /// Tries to reserve capacity for at least `additional` elements more than the
    /// current length. The allocator may reserve more space to speculatively
    /// avoid frequent allocations. After calling `try_reserve`, capacity will be
    /// greater than or equal to `self.len() + additional` if it returns
    /// `Ok(())`. Does nothing if capacity is already sufficient. This method
    /// preserves the contents even if an error occurs.
    ///
    /// # Errors
    ///
    /// If the capacity overflows, or the allocator reports a failure, then an error
    /// is returned.
    pub fn try_reserve(&mut self, additional: usize) -> Result<(), TryReserveError> {
        self.data.try_reserve(additional)
    }

    /// Discards as much additional capacity as possible.
    pub fn shrink_to_fit(&mut self) {
        self.data.shrink_to_fit();
    }

    /// Discards capacity with a lower bound.
    ///
    /// The capacity will remain at least as large as both the length
    /// and the supplied value.
    ///
    /// If the current capacity is less than the lower limit, this is a no-op.
    #[inline]
    pub fn shrink_to(&mut self, min_capacity: usize) {
        self.data.shrink_to(min_capacity)
    }

    /// Returns a slice of all values in the underlying vector, in arbitrary
    /// order.
    #[must_use]
    pub fn as_slice(&self) -> &[T] {
        self.data.as_slice()
    }

    /// Consumes the `BinaryHeap` and returns the underlying vector
    /// in arbitrary order.
    #[must_use = "`self` will be dropped if the result is not used"]
    pub fn into_vec(self) -> Vec<T> {
        self.into()
    }

    /// Returns the length of the binary heap.
    #[must_use]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Checks if the binary heap is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clears the binary heap, returning an iterator over the removed elements
    /// in arbitrary order. If the iterator is dropped before being fully
    /// consumed, it drops the remaining elements in arbitrary order.
    ///
    /// The returned iterator keeps a mutable borrow on the heap to optimize
    /// its implementation.
    #[inline]
    pub fn drain(&mut self) -> Drain<'_, T> {
        Drain {
            iter: self.data.drain(..),
        }
    }

    /// Drops all items from the binary heap.
    pub fn clear(&mut self) {
        self.drain();
    }
}

/// Hole represents a hole in a slice i.e., an index without valid value
/// (because it was moved from or duplicated).
/// In drop, `Hole` will restore the slice by filling the hole
/// position with the value that was originally removed.
struct Hole<'a, T: 'a> {
    data: &'a mut [T],
    elt: ManuallyDrop<T>,
    pos: usize,
}

impl<'a, T> Hole<'a, T> {
    /// Creates a new `Hole` at index `pos`.
    ///
    /// Unsafe because pos must be within the data slice.
    #[inline]
    unsafe fn new(data: &'a mut [T], pos: usize) -> Self {
        debug_assert!(pos < data.len());
        // SAFE: pos should be inside the slice
        let elt = unsafe { ptr::read(data.get_unchecked(pos)) };
        Hole {
            data,
            elt: ManuallyDrop::new(elt),
            pos,
        }
    }

    #[inline]
    fn pos(&self) -> usize {
        self.pos
    }

    /// Returns a reference to the element removed.
    #[inline]
    fn element(&self) -> &T {
        &self.elt
    }

    /// Returns a reference to the element at `index`.
    ///
    /// Unsafe because index must be within the data slice and not equal to pos.
    #[inline]
    unsafe fn get(&self, index: usize) -> &T {
        debug_assert!(index != self.pos);
        debug_assert!(index < self.data.len());
        unsafe { self.data.get_unchecked(index) }
    }

    /// Move hole to new location
    ///
    /// Unsafe because index must be within the data slice and not equal to pos.
    #[inline]
    unsafe fn move_to(&mut self, index: usize) {
        debug_assert!(index != self.pos);
        debug_assert!(index < self.data.len());
        unsafe {
            let ptr = self.data.as_mut_ptr();
            let index_ptr: *const _ = ptr.add(index);
            let hole_ptr = ptr.add(self.pos);
            ptr::copy_nonoverlapping(index_ptr, hole_ptr, 1);
        }
        self.pos = index;
    }
}

impl<T> Drop for Hole<'_, T> {
    #[inline]
    fn drop(&mut self) {
        // fill the hole again
        unsafe {
            let pos = self.pos;
            ptr::copy_nonoverlapping(&*self.elt, self.data.get_unchecked_mut(pos), 1);
        }
    }
}

/// An iterator over the elements of a `BinaryHeap`.
///
/// This `struct` is created by [`BinaryHeap::iter()`]. See its
/// documentation for more.
///
/// [`iter`]: BinaryHeap::iter
#[must_use = "iterators are lazy and do nothing unless consumed"]
pub struct Iter<'a, T: 'a> {
    iter: slice::Iter<'a, T>,
}

impl<T> Default for Iter<'_, T> {
    /// Creates an empty `binary_heap::Iter`.
    ///
    /// ```
    /// # use std::collections::binary_heap;
    /// let iter: binary_heap::Iter<'_, u8> = Default::default();
    /// assert_eq!(iter.len(), 0);
    /// ```
    fn default() -> Self {
        Iter {
            iter: Default::default(),
        }
    }
}

impl<T: fmt::Debug> fmt::Debug for Iter<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Iter").field(&self.iter.as_slice()).finish()
    }
}

// FIXME(#26925) Remove in favor of `#[derive(Clone)]`
impl<T> Clone for Iter<'_, T> {
    fn clone(&self) -> Self {
        Iter {
            iter: self.iter.clone(),
        }
    }
}

impl<'a, T> Iterator for Iter<'a, T> {
    type Item = &'a T;

    #[inline]
    fn next(&mut self) -> Option<&'a T> {
        self.iter.next()
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }

    #[inline]
    fn last(self) -> Option<&'a T> {
        self.iter.last()
    }
}

impl<'a, T> DoubleEndedIterator for Iter<'a, T> {
    #[inline]
    fn next_back(&mut self) -> Option<&'a T> {
        self.iter.next_back()
    }
}

impl<T> FusedIterator for Iter<'_, T> {}

/// An owning iterator over the elements of a `BinaryHeap`.
///
/// This `struct` is created by [`BinaryHeap::into_iter()`]
/// (provided by the [`IntoIterator`] trait). See its documentation for more.
///
/// [`into_iter`]: BinaryHeap::into_iter
#[derive(Clone)]
pub struct IntoIter<T> {
    iter: vec::IntoIter<T>,
}

impl<T: fmt::Debug> fmt::Debug for IntoIter<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("IntoIter")
            .field(&self.iter.as_slice())
            .finish()
    }
}

impl<T> Iterator for IntoIter<T> {
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<T> {
        self.iter.next()
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

impl<T> DoubleEndedIterator for IntoIter<T> {
    #[inline]
    fn next_back(&mut self) -> Option<T> {
        self.iter.next_back()
    }
}

impl<T> FusedIterator for IntoIter<T> {}

impl<T> Default for IntoIter<T> {
    /// Creates an empty `binary_heap::IntoIter`.
    ///
    /// ```
    /// # use std::collections::binary_heap;
    /// let iter: binary_heap::IntoIter<u8> = Default::default();
    /// assert_eq!(iter.len(), 0);
    /// ```
    fn default() -> Self {
        IntoIter {
            iter: Default::default(),
        }
    }
}

#[must_use = "iterators are lazy and do nothing unless consumed"]
#[derive(Clone, Debug)]
pub struct IntoIterSorted<T, C> {
    inner: BinaryHeap<T, C>,
}

impl<T, C> Iterator for IntoIterSorted<T, C>
where
    C: Fn(&T, &T) -> Ordering,
{
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<T> {
        self.inner.pop()
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let exact = self.inner.len();
        (exact, Some(exact))
    }
}

/// A draining iterator over the elements of a `BinaryHeap`.
///
/// This `struct` is created by [`BinaryHeap::drain()`]. See its
/// documentation for more.
///
/// [`drain`]: BinaryHeap::drain
#[derive(Debug)]
pub struct Drain<'a, T: 'a> {
    iter: vec::Drain<'a, T>,
}

impl<T> Iterator for Drain<'_, T> {
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<T> {
        self.iter.next()
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

impl<T> DoubleEndedIterator for Drain<'_, T> {
    #[inline]
    fn next_back(&mut self) -> Option<T> {
        self.iter.next_back()
    }
}

impl<T> ExactSizeIterator for Drain<'_, T> {}

impl<T> FusedIterator for Drain<'_, T> {}

/// A draining iterator over the elements of a `BinaryHeap`.
///
/// This `struct` is created by [`BinaryHeap::drain_sorted()`]. See its
/// documentation for more.
///
/// [`drain_sorted`]: BinaryHeap::drain_sorted
#[derive(Debug)]
pub struct DrainSorted<'a, T, C>
where
    C: Fn(&T, &T) -> Ordering,
{
    inner: &'a mut BinaryHeap<T, C>,
}

impl<'a, T, C> Drop for DrainSorted<'a, T, C>
where
    C: Fn(&T, &T) -> Ordering,
{
    /// Removes heap elements in heap order.
    fn drop(&mut self) {
        struct DropGuard<'r, 'a, T, C>(&'r mut DrainSorted<'a, T, C>)
        where
            C: Fn(&T, &T) -> Ordering;

        impl<'r, 'a, T, C> Drop for DropGuard<'r, 'a, T, C>
        where
            C: Fn(&T, &T) -> Ordering,
        {
            fn drop(&mut self) {
                while self.0.inner.pop().is_some() {}
            }
        }

        while let Some(item) = self.inner.pop() {
            let guard = DropGuard(self);
            drop(item);
            mem::forget(guard);
        }
    }
}

impl<T, C> Iterator for DrainSorted<'_, T, C>
where
    C: Fn(&T, &T) -> Ordering,
{
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<T> {
        self.inner.pop()
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let exact = self.inner.len();
        (exact, Some(exact))
    }
}

impl<T, C> ExactSizeIterator for DrainSorted<'_, T, C> where C: Fn(&T, &T) -> Ordering {}

impl<T, C> FusedIterator for DrainSorted<'_, T, C> where C: Fn(&T, &T) -> Ordering {}

impl<T, C> From<BinaryHeap<T, C>> for Vec<T> {
    /// Converts a `BinaryHeap<T>` into a `Vec<T>`.
    ///
    /// This conversion requires no data movement or allocation, and has
    /// constant time complexity.
    fn from(heap: BinaryHeap<T, C>) -> Vec<T> {
        heap.data
    }
}

impl<T, C> IntoIterator for BinaryHeap<T, C> {
    type Item = T;
    type IntoIter = IntoIter<T>;

    /// Creates a consuming iterator, that is, one that moves each value out of
    /// the binary heap in arbitrary order. The binary heap cannot be used
    /// after calling this.
    fn into_iter(self) -> IntoIter<T> {
        IntoIter {
            iter: self.data.into_iter(),
        }
    }
}

impl<'a, T, C> IntoIterator for &'a BinaryHeap<T, C> {
    type Item = &'a T;
    type IntoIter = Iter<'a, T>;

    fn into_iter(self) -> Iter<'a, T> {
        self.iter()
    }
}

impl<T, C> Extend<T> for BinaryHeap<T, C>
where
    C: Fn(&T, &T) -> Ordering,
{
    #[inline]
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        let guard = RebuildOnDrop {
            rebuild_from: self.len(),
            heap: self,
        };
        guard.heap.data.extend(iter);
    }
}

impl<'a, T, C> Extend<&'a T> for BinaryHeap<T, C>
where
    T: Clone,
    C: Fn(&T, &T) -> Ordering,
{
    fn extend<I: IntoIterator<Item = &'a T>>(&mut self, iter: I) {
        self.extend(iter.into_iter().cloned());
    }
}

#[cfg(test)]
mod tests {
    use super::BinaryHeap;

    #[test]
    fn peek_all_returns_all_max_elements() {
        let heap = BinaryHeap::from_vec(vec![3, 5, 5, 2, 5, 4], |a, b| a.cmp(b));

        let mut values = Vec::new();
        heap.peek_all(|_, x| values.push(*x), &mut Vec::new());

        assert_eq!(values.len(), 3);
        assert!(values.into_iter().all(|x| x == 5));
    }

    #[test]
    fn peek_all_on_empty_heap_invokes_nothing() {
        let heap = BinaryHeap::<i32, _>::new(|a, b| a.cmp(b));
        let mut called = false;

        heap.peek_all(|__, _| called = true, &mut Vec::new());

        assert!(!called);
    }

    #[test]
    fn peek_all_on_single_element_heap_returns_that_element() {
        let heap = BinaryHeap::<i32, _>::from_vec(vec![42], |a, b| a.cmp(b));
        let mut values = Vec::new();

        heap.peek_all(|_, x| values.push(*x), &mut Vec::new());

        assert_eq!(values, vec![42]);
    }

    #[test]
    fn peek_all_returns_every_element_when_all_are_equal() {
        let heap = BinaryHeap::<i32, _>::from_vec(vec![7, 7, 7, 7, 7, 7, 7, 7], |a, b| a.cmp(b));
        let mut values = Vec::new();

        heap.peek_all(|_, x| values.push(*x), &mut Vec::new());

        assert_eq!(values.len(), heap.len());
        assert!(values.into_iter().all(|x| x == 7));
    }

    #[test]
    fn peek_all_returns_only_one_when_maximum_is_unique() {
        let heap = BinaryHeap::<i32, _>::from_vec(vec![9, 8, 7, 6, 5, 4, 3, 2, 1], |a, b| a.cmp(b));
        let mut values = Vec::new();

        heap.peek_all(|_, x| values.push(*x), &mut Vec::new());

        assert_eq!(values, vec![9]);
    }
}
