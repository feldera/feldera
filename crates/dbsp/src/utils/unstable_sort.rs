//! This module contains a dynamically typed re-implementation of the unstable
//! sorting algorithm from the Rust standard library.  DBSP stores data in sorted vectors.
//! Using the `std` implementation of sorting, we ended up compiling specialized
//! implementations of the sorting algorithm for hundreds of concrete types, which slowed down
//! compilation significantly.  The dynamic implementation in this module works for all typed by
//! taking the comparison function as `&dyn Fn`, along with the size and alignment of a vector
//! element.

// TODO: The unstable sort implementation is currently broken and not used by DBSP.
// - Tests are currently failing.
// - I did some initial benchmarking enabling unstable sort with nexmark, but did not see
//   any measurable improvement. Not sure if this is because unstable sort is not faster
//   than stable for our use case or because it's not on the critical path in this benchmark,
//   or perhaps my dynamically typed implementation of unstable sort loses too much performance,
//   e.g., there are places where the original code allocates local temporary variables, but
//   we have to pass them as raw pointers (`scratch`).
//   In any case, we need to add a standalone benchmark for unstable vs stable sort.

use std::ptr;
use std::{
    cmp,
    cmp::Ordering,
    fmt::Debug,
    mem::{size_of, MaybeUninit},
    ops::Range,
};

unsafe fn swap_if_different(x: *mut u8, y: *mut u8, count: usize) {
    if x != y {
        ptr::swap_nonoverlapping(x, y, count);
    }
}

#[inline]
fn vec_len(v: &[u8], val_size: usize) -> usize {
    v.len() / val_size
}

#[inline]
unsafe fn vec_index_unchecked(v: &mut [u8], val_size: usize, index: usize) -> *mut u8 {
    let v = v.as_mut_ptr();
    v.add(index * val_size)
}

#[inline]
fn vec_suffix(v: &mut [u8], val_size: usize, start: usize) -> &mut [u8] {
    &mut v[start * val_size..]
}

#[inline]
fn vec_slice(v: &mut [u8], val_size: usize, start: usize, end: usize) -> &mut [u8] {
    &mut v[start * val_size..end * val_size]
}

#[inline]
fn vec_split_at(v: &mut [u8], val_size: usize, start: usize) -> (&mut [u8], &mut [u8]) {
    v.split_at_mut(start * val_size)
}

#[inline]
fn vec_swap(v: &mut [u8], val_size: usize, index1: usize, index2: usize) {
    let v = v.as_mut_ptr();

    unsafe { swap_if_different(v.add(index1 * val_size), v.add(index2 * val_size), val_size) };
}

fn vec_reverse(v: &mut [u8], val_size: usize) {
    let len = v.len() / val_size;

    let half_len = len / 2;
    let Range { start, end } = v.as_mut_ptr_range();

    let mut i = 0;
    while i < half_len {
        unsafe {
            ptr::swap_nonoverlapping(
                start.add(i * val_size),
                end.sub((1 + i) * val_size),
                val_size,
            );
        }

        i += 1;
    }
}

// When dropped, copies from `src` into `dest`.
struct InsertionHole {
    src: *const u8,
    dest: *mut u8,
    val_size: usize,
}

impl Drop for InsertionHole {
    fn drop(&mut self) {
        // SAFETY: This is a helper class. Please refer to its usage for correctness.
        // Namely, one must be sure that `src` and `dst` does not overlap as
        // required by `ptr::copy_nonoverlapping` and are both valid for writes.
        unsafe {
            ptr::copy_nonoverlapping(self.src, self.dest, self.val_size);
        }
    }
}

/// Inserts `v[v.len() - 1]` into pre-sorted sequence `v[..v.len() - 1]` so that
/// whole `v[..]` becomes sorted.
unsafe fn insert_tail(
    v: &mut [u8],
    val_size: usize,
    is_less: &dyn Fn(*const u8, *const u8) -> bool,
    scratch: *mut u8,
) {
    debug_assert!(v.len() / val_size >= 2);

    let arr_ptr = v.as_mut_ptr();
    let i = v.len() / val_size - 1;

    // SAFETY: caller must ensure v is at least len 2.
    unsafe {
        // See insert_head which talks about why this approach is beneficial.
        let i_ptr = arr_ptr.add(i * val_size);

        // It's important that we use i_ptr here. If this check is positive and we
        // continue, We want to make sure that no other copy of the value was
        // seen by is_less. Otherwise we would have to copy it back.
        if is_less(i_ptr, i_ptr.sub(val_size)) {
            // It's important, that we use tmp for comparison from now on. As it is the
            // value that will be copied back. And notionally we could have
            // created a divergence if we copy back the wrong value.
            ptr::copy_nonoverlapping(i_ptr, scratch, val_size);
            //let tmp = mem::ManuallyDrop::new(ptr::read(i_ptr));

            // Intermediate state of the insertion process is always tracked by `hole`,
            // which serves two purposes:
            // 1. Protects integrity of `v` from panics in `is_less`.
            // 2. Fills the remaining hole in `v` in the end.
            //
            // Panic safety:
            //
            // If `is_less` panics at any point during the process, `hole` will get dropped
            // and fill the hole in `v` with `tmp`, thus ensuring that `v` still
            // holds every object it initially held exactly once.
            let mut hole = InsertionHole {
                src: scratch,
                dest: i_ptr.sub(val_size),
                val_size,
            };
            ptr::copy_nonoverlapping(hole.dest, i_ptr, val_size);

            // SAFETY: We know i is at least 1.
            for j in (0..(i - 1)).rev() {
                let j_ptr = arr_ptr.add(j * val_size);
                if !is_less(scratch, j_ptr) {
                    break;
                }

                ptr::copy_nonoverlapping(j_ptr, hole.dest, val_size);
                hole.dest = j_ptr;
            }
            // `hole` gets dropped and thus copies `tmp` into the remaining hole
            // in `v`.
        }
    }
}

/// Inserts `v[0]` into pre-sorted sequence `v[1..]` so that whole `v[..]` becomes sorted.
///
/// This is the integral subroutine of insertion sort.
unsafe fn insert_head(
    v: &mut [u8],
    val_size: usize,
    is_less: &dyn Fn(*const u8, *const u8) -> bool,
    scratch: *mut u8,
) {
    debug_assert!(vec_len(v, val_size) >= 2);

    // SAFETY: caller must ensure v is at least len 2.
    unsafe {
        if is_less(
            vec_index_unchecked(v, val_size, 1),
            vec_index_unchecked(v, val_size, 0),
        ) {
            let arr_ptr = v.as_mut_ptr();

            // There are three ways to implement insertion here:
            //
            // 1. Swap adjacent elements until the first one gets to its final destination.
            //    However, this way we copy data around more than is necessary. If elements are big
            //    structures (costly to copy), this method will be slow.
            //
            // 2. Iterate until the right place for the first element is found. Then shift the
            //    elements succeeding it to make room for it and finally place it into the
            //    remaining hole. This is a good method.
            //
            // 3. Copy the first element into a temporary variable. Iterate until the right place
            //    for it is found. As we go along, copy every traversed element into the slot
            //    preceding it. Finally, copy data from the temporary variable into the remaining
            //    hole. This method is very good. Benchmarks demonstrated slightly better
            //    performance than with the 2nd method.
            //
            // All methods were benchmarked, and the 3rd showed best results. So we chose that one.
            ptr::copy_nonoverlapping(arr_ptr, scratch, val_size);

            // Intermediate state of the insertion process is always tracked by `hole`, which
            // serves two purposes:
            // 1. Protects integrity of `v` from panics in `is_less`.
            // 2. Fills the remaining hole in `v` in the end.
            //
            // Panic safety:
            //
            // If `is_less` panics at any point during the process, `hole` will get dropped and
            // fill the hole in `v` with `tmp`, thus ensuring that `v` still holds every object it
            // initially held exactly once.
            let mut hole = InsertionHole {
                src: scratch,
                dest: arr_ptr.add(val_size),
                val_size,
            };
            ptr::copy_nonoverlapping(arr_ptr.add(val_size), arr_ptr.add(0), val_size);

            for i in 2..vec_len(v, val_size) {
                if !is_less(vec_index_unchecked(v, val_size, i), scratch) {
                    break;
                }
                ptr::copy_nonoverlapping(
                    arr_ptr.add(val_size),
                    arr_ptr.add(val_size * (i - 1)),
                    val_size,
                );
                hole.dest = arr_ptr.add(val_size * i);
            }
            // `hole` gets dropped and thus copies `tmp` into the remaining hole in `v`.
        }
    }
}

/// Sort `v` assuming `v[..offset]` is already sorted.
///
/// Never inline this function to avoid code bloat. It still optimizes nicely
/// and has practically no performance impact. Even improving performance in
/// some cases.
#[inline(never)]
pub(super) fn insertion_sort_shift_left(
    v: &mut [u8],
    val_size: usize,
    offset: usize,
    is_less: &dyn Fn(*const u8, *const u8) -> bool,
    scratch: *mut u8,
) {
    let len = v.len() / val_size;

    // Using assert here improves performance.
    assert!(offset != 0 && offset <= len);

    // Shift each element of the unsorted region v[i..] as far left as is needed to
    // make v sorted.
    for i in offset..len {
        // SAFETY: we tested that `offset` must be at least 1, so this loop is only
        // entered if len >= 2. The range is exclusive and we know `i` must be
        // at least 1 so this slice has at >least len 2.
        unsafe {
            insert_tail(
                &mut v[..=(i + 1) * val_size - 1],
                val_size,
                is_less,
                scratch,
            );
        }
    }
}

/// Sort `v` assuming `v[offset..]` is already sorted.
///
/// Never inline this function to avoid code bloat. It still optimizes nicely and has practically no
/// performance impact. Even improving performance in some cases.
#[inline(never)]
fn insertion_sort_shift_right(
    v: &mut [u8],
    val_size: usize,
    offset: usize,
    is_less: &dyn Fn(*const u8, *const u8) -> bool,
    scratch: *mut u8,
) {
    let len = vec_len(v, val_size);

    // Using assert here improves performance.
    assert!(offset != 0 && offset <= len && len >= 2);

    // Shift each element of the unsorted region v[..i] as far left as is needed to make v sorted.
    for i in (0..offset).rev() {
        // SAFETY: we tested that `offset` must be at least 1, so this loop is only entered if len
        // >= 2.We ensured that the slice length is always at least 2 long. We know that start_found
        // will be at least one less than end, and the range is exclusive. Which gives us i always
        // <= (end - 2).
        unsafe {
            insert_head(vec_slice(v, val_size, i, len), val_size, is_less, scratch);
        }
    }
}

/// Partially sorts a slice by shifting several out-of-order elements around.
///
/// Returns `true` if the slice is sorted at the end. This function is *O*(*n*) worst-case.
#[cold]
fn partial_insertion_sort(
    v: &mut [u8],
    val_size: usize,
    is_less: &dyn Fn(*const u8, *const u8) -> bool,
    scratch: *mut u8,
) -> bool {
    // Maximum number of adjacent out-of-order pairs that will get shifted.
    const MAX_STEPS: usize = 5;
    // If the slice is shorter than this, don't shift any elements.
    const SHORTEST_SHIFTING: usize = 50;

    let len = vec_len(v, val_size);
    let mut i = 1;

    for _ in 0..MAX_STEPS {
        // SAFETY: We already explicitly did the bound checking with `i < len`.
        // All our subsequent indexing is only in the range `0 <= index < len`
        unsafe {
            // Find the next pair of adjacent out-of-order elements.
            while i < len
                && !is_less(
                    vec_index_unchecked(v, val_size, i),
                    vec_index_unchecked(v, val_size, i - 1),
                )
            {
                i += 1;
            }
        }

        // Are we done?
        if i == len {
            return true;
        }

        // Don't shift elements on short arrays, that has a performance cost.
        if len < SHORTEST_SHIFTING {
            return false;
        }

        // Swap the found pair of elements. This puts them in correct order.
        vec_swap(v, val_size, i - 1, i);

        if i >= 2 {
            // Shift the smaller element to the left.
            insertion_sort_shift_left(
                vec_slice(v, val_size, 0, i),
                val_size,
                i - 1,
                is_less,
                scratch,
            );

            // Shift the greater element to the right.
            insertion_sort_shift_right(vec_slice(v, val_size, 0, i), val_size, 1, is_less, scratch);
        }
    }

    // Didn't manage to sort the slice in the limited number of steps.
    false
}

/// Sorts `v` using heapsort, which guarantees *O*(*n* \* log(*n*)) worst-case.
#[cold]
pub fn heapsort(v: &mut [u8], val_size: usize, is_less: &dyn Fn(*const u8, *const u8) -> bool) {
    // This binary heap respects the invariant `parent >= child`.
    let sift_down = |v: &mut [u8], mut node| {
        let len = vec_len(v, val_size);
        loop {
            // Children of `node`.
            let mut child = 2 * node + 1;
            if child >= len {
                break;
            }

            // Choose the greater child.
            if child + 1 < len {
                // We need a branch to be sure not to out-of-bounds index,
                // but it's highly predictable.  The comparison, however,
                // is better done branchless, especially for primitives.
                unsafe {
                    child += is_less(
                        vec_index_unchecked(v, val_size, child),
                        vec_index_unchecked(v, val_size, child + 1),
                    ) as usize
                };
            }

            // Stop if the invariant holds at `node`.
            if !unsafe {
                is_less(
                    vec_index_unchecked(v, val_size, node),
                    vec_index_unchecked(v, val_size, child),
                )
            } {
                break;
            }

            // Swap `node` with the greater child, move one step down, and continue sifting.
            vec_swap(v, val_size, node, child);
            node = child;
        }
    };

    // Build the heap in linear time.
    for i in (0..vec_len(v, val_size) / 2).rev() {
        sift_down(v, i);
    }

    // Pop maximal elements from the heap.
    for i in (1..vec_len(v, val_size)).rev() {
        vec_swap(v, val_size, 0, i);
        sift_down(vec_slice(v, val_size, 0, i), 0);
    }
}

/// Partitions `v` into elements smaller than `pivot`, followed by elements greater than or equal
/// to `pivot`.
///
/// Returns the number of elements smaller than `pivot`.
///
/// Partitioning is performed block-by-block in order to minimize the cost of branching operations.
/// This idea is presented in the [BlockQuicksort][pdf] paper.
///
/// [pdf]: https://drops.dagstuhl.de/opus/volltexte/2016/6389/pdf/LIPIcs-ESA-2016-38.pdf
fn partition_in_blocks(
    v: &mut [u8],
    val_size: usize,
    pivot: *const u8,
    is_less: &dyn Fn(*const u8, *const u8) -> bool,
    scratch: *mut u8,
) -> usize {
    // Number of elements in a typical block.
    const BLOCK: usize = 128;

    // The partitioning algorithm repeats the following steps until completion:
    //
    // 1. Trace a block from the left side to identify elements greater than or equal to the pivot.
    // 2. Trace a block from the right side to identify elements smaller than the pivot.
    // 3. Exchange the identified elements between the left and right side.
    //
    // We keep the following variables for a block of elements:
    //
    // 1. `block` - Number of elements in the block.
    // 2. `start` - Start pointer into the `offsets` array.
    // 3. `end` - End pointer into the `offsets` array.
    // 4. `offsets` - Indices of out-of-order elements within the block.

    // The current block on the left side (from `l` to `l.add(block_l)`).
    let mut l = v.as_mut_ptr();
    let mut block_l = BLOCK;
    let mut start_l = ptr::null_mut();
    let mut end_l = ptr::null_mut();
    let mut offsets_l = [MaybeUninit::<u8>::uninit(); BLOCK];

    // The current block on the right side (from `r.sub(block_r)` to `r`).
    // SAFETY: The documentation for .add() specifically mention that `vec.as_ptr().add(vec.len())` is always safe
    let mut r = unsafe { l.add(v.len()) };
    let mut block_r = BLOCK;
    let mut start_r = ptr::null_mut();
    let mut end_r = ptr::null_mut();
    let mut offsets_r = [MaybeUninit::<u8>::uninit(); BLOCK];

    // FIXME: When we get VLAs, try creating one array of length `min(v.len(), 2 * BLOCK)` rather
    // than two fixed-size arrays of length `BLOCK`. VLAs might be more cache-efficient.

    // Returns the number of elements between pointers `l` (inclusive) and `r` (exclusive).
    fn width(l: *mut u8, r: *mut u8, type_size: usize) -> usize {
        assert!(type_size > 0);
        // FIXME: this should *likely* use `offset_from`, but more
        // investigation is needed (including running tests in miri).
        (r as usize - l as usize) / type_size
    }

    loop {
        // We are done with partitioning block-by-block when `l` and `r` get very close. Then we do
        // some patch-up work in order to partition the remaining elements in between.
        let is_done = width(l, r, val_size) <= 2 * BLOCK;

        if is_done {
            // Number of remaining elements (still not compared to the pivot).
            let mut rem = width(l, r, val_size);
            if start_l < end_l || start_r < end_r {
                rem -= BLOCK;
            }

            // Adjust block sizes so that the left and right block don't overlap, but get perfectly
            // aligned to cover the whole remaining gap.
            if start_l < end_l {
                block_r = rem;
            } else if start_r < end_r {
                block_l = rem;
            } else {
                // There were the same number of elements to switch on both blocks during the last
                // iteration, so there are no remaining elements on either block. Cover the remaining
                // items with roughly equally-sized blocks.
                block_l = rem / 2;
                block_r = rem - block_l;
            }
            debug_assert!(block_l <= BLOCK && block_r <= BLOCK);
            debug_assert!(width(l, r, val_size) == block_l + block_r);
        }

        if start_l == end_l {
            // Trace `block_l` elements from the left side.
            start_l = offsets_l.as_mut_ptr() as *mut u8;
            end_l = start_l;
            let mut elem = l;

            for i in 0..block_l {
                // SAFETY: The unsafety operations below involve the usage of the `offset`.
                //         According to the conditions required by the function, we satisfy them because:
                //         1. `offsets_l` is stack-allocated, and thus considered separate allocated object.
                //         2. The function `is_less` returns a `bool`.
                //            Casting a `bool` will never overflow `isize`.
                //         3. We have guaranteed that `block_l` will be `<= BLOCK`.
                //            Plus, `end_l` was initially set to the begin pointer of `offsets_` which was declared on the stack.
                //            Thus, we know that even in the worst case (all invocations of `is_less` returns false) we will only be at most 1 byte pass the end.
                //        Another unsafety operation here is dereferencing `elem`.
                //        However, `elem` was initially the begin pointer to the slice which is always valid.
                unsafe {
                    // Branchless comparison.
                    *end_l = i as u8;
                    end_l = end_l.add(!is_less(&*elem, pivot) as usize);
                    elem = elem.add(val_size);
                }
            }
        }

        if start_r == end_r {
            // Trace `block_r` elements from the right side.
            start_r = offsets_r.as_mut_ptr() as *mut u8;
            end_r = start_r;
            let mut elem = r;

            for i in 0..block_r {
                // SAFETY: The unsafety operations below involve the usage of the `offset`.
                //         According to the conditions required by the function, we satisfy them because:
                //         1. `offsets_r` is stack-allocated, and thus considered separate allocated object.
                //         2. The function `is_less` returns a `bool`.
                //            Casting a `bool` will never overflow `isize`.
                //         3. We have guaranteed that `block_r` will be `<= BLOCK`.
                //            Plus, `end_r` was initially set to the begin pointer of `offsets_` which was declared on the stack.
                //            Thus, we know that even in the worst case (all invocations of `is_less` returns true) we will only be at most 1 byte pass the end.
                //        Another unsafety operation here is dereferencing `elem`.
                //        However, `elem` was initially `1 * sizeof(T)` past the end and we decrement it by `1 * sizeof(T)` before accessing it.
                //        Plus, `block_r` was asserted to be less than `BLOCK` and `elem` will therefore at most be pointing to the beginning of the slice.
                unsafe {
                    // Branchless comparison.
                    elem = elem.sub(val_size);
                    *end_r = i as u8;
                    end_r = end_r.add(is_less(&*elem, pivot) as usize);
                }
            }
        }

        // Number of out-of-order elements to swap between the left and right side.
        let count = cmp::min(width(start_l, end_l, 1), width(start_r, end_r, 1));

        if count > 0 {
            macro_rules! left {
                () => {
                    l.add(usize::from(*start_l) * val_size)
                };
            }
            macro_rules! right {
                () => {
                    r.sub((usize::from(*start_r) + 1) * val_size)
                };
            }

            // Instead of swapping one pair at the time, it is more efficient to perform a cyclic
            // permutation. This is not strictly equivalent to swapping, but produces a similar
            // result using fewer memory operations.

            // SAFETY: The use of `ptr::read` is valid because there is at least one element in
            // both `offsets_l` and `offsets_r`, so `left!` is a valid pointer to read from.
            //
            // The uses of `left!` involve calls to `offset` on `l`, which points to the
            // beginning of `v`. All the offsets pointed-to by `start_l` are at most `block_l`, so
            // these `offset` calls are safe as all reads are within the block. The same argument
            // applies for the uses of `right!`.
            //
            // The calls to `start_l.offset` are valid because there are at most `count-1` of them,
            // plus the final one at the end of the unsafe block, where `count` is the minimum number
            // of collected offsets in `offsets_l` and `offsets_r`, so there is no risk of there not
            // being enough elements. The same reasoning applies to the calls to `start_r.offset`.
            //
            // The calls to `copy_nonoverlapping` are safe because `left!` and `right!` are guaranteed
            // not to overlap, and are valid because of the reasoning above.
            unsafe {
                ptr::copy_nonoverlapping(left!(), scratch, val_size);

                ptr::copy_nonoverlapping(right!(), left!(), val_size);

                for _ in 1..count {
                    start_l = start_l.add(1);
                    ptr::copy_nonoverlapping(left!(), right!(), val_size);
                    start_r = start_r.add(1);
                    ptr::copy_nonoverlapping(right!(), left!(), val_size);
                }

                ptr::copy_nonoverlapping(scratch, right!(), val_size);

                start_l = start_l.add(1);
                start_r = start_r.add(1);
            }
        }

        if start_l == end_l {
            // All out-of-order elements in the left block were moved. Move to the next block.

            // block-width-guarantee
            // SAFETY: if `!is_done` then the slice width is guaranteed to be at least `2*BLOCK` wide. There
            // are at most `BLOCK` elements in `offsets_l` because of its size, so the `offset` operation is
            // safe. Otherwise, the debug assertions in the `is_done` case guarantee that
            // `width(l, r) == block_l + block_r`, namely, that the block sizes have been adjusted to account
            // for the smaller number of remaining elements.
            l = unsafe { l.add(block_l * val_size) };
        }

        if start_r == end_r {
            // All out-of-order elements in the right block were moved. Move to the previous block.

            // SAFETY: Same argument as [block-width-guarantee]. Either this is a full block `2*BLOCK`-wide,
            // or `block_r` has been adjusted for the last handful of elements.
            r = unsafe { r.sub(block_r * val_size) };
        }

        if is_done {
            break;
        }
    }

    // All that remains now is at most one block (either the left or the right) with out-of-order
    // elements that need to be moved. Such remaining elements can be simply shifted to the end
    // within their block.

    if start_l < end_l {
        // The left block remains.
        // Move its remaining out-of-order elements to the far right.
        debug_assert_eq!(width(l, r, val_size), block_l);
        while start_l < end_l {
            // remaining-elements-safety
            // SAFETY: while the loop condition holds there are still elements in `offsets_l`, so it
            // is safe to point `end_l` to the previous element.
            //
            // The `ptr::swap` is safe if both its arguments are valid for reads and writes:
            //  - Per the debug assert above, the distance between `l` and `r` is `block_l`
            //    elements, so there can be at most `block_l` remaining offsets between `start_l`
            //    and `end_l`. This means `r` will be moved at most `block_l` steps back, which
            //    makes the `r.offset` calls valid (at that point `l == r`).
            //  - `offsets_l` contains valid offsets into `v` collected during the partitioning of
            //    the last block, so the `l.offset` calls are valid.
            unsafe {
                end_l = end_l.sub(val_size);
                swap_if_different(
                    l.add(usize::from(*end_l) * val_size),
                    r.sub(val_size),
                    val_size,
                );
                r = r.sub(val_size);
            }
        }
        width(v.as_mut_ptr(), r, val_size)
    } else if start_r < end_r {
        // The right block remains.
        // Move its remaining out-of-order elements to the far left.
        debug_assert_eq!(width(l, r, val_size), block_r);
        while start_r < end_r {
            // SAFETY: See the reasoning in [remaining-elements-safety].
            unsafe {
                end_r = end_r.sub(1);
                /*println!(
                    "start_r: {start_r:?}, *start_r: {}, end_r: {end_r:?}, *end_r: {}, l: {l:?}, r: {r:?}, r.sub: {:?}, val_size: {val_size}", *start_r, *end_r, r.sub(val_size * (usize::from(*end_r) + 1))
                );*/

                swap_if_different(l, r.sub(val_size * (usize::from(*end_r) + 1)), val_size);
                l = l.add(val_size);
            }
        }
        width(v.as_mut_ptr(), l, val_size)
    } else {
        // Nothing else to do, we're done.
        width(v.as_mut_ptr(), l, val_size)
    }
}

/// Partitions `v` into elements smaller than `v[pivot]`, followed by elements greater than or
/// equal to `v[pivot]`.
///
/// Returns a tuple of:
///
/// 1. Number of elements smaller than `v[pivot]`.
/// 2. True if `v` was already partitioned.
pub(super) fn partition(
    v: &mut [u8],
    val_size: usize,
    pivot: usize,
    is_less: &dyn Fn(*const u8, *const u8) -> bool,
    scratch: *mut u8,
) -> (usize, bool) {
    let (mid, was_partitioned) = {
        // Place the pivot at the beginning of slice.
        vec_swap(v, val_size, 0, pivot);
        let (pivot, v) = vec_split_at(v, val_size, 1);
        let pivot = pivot.as_mut_ptr();

        // Find the first pair of out-of-order elements.
        let mut l = 0;
        let mut r = vec_len(v, val_size);

        // SAFETY: The unsafety below involves indexing an array.
        // For the first one: We already do the bounds checking here with `l < r`.
        // For the second one: We initially have `l == 0` and `r == v.len()` and we checked that `l < r` at every indexing operation.
        //                     From here we know that `r` must be at least `r == l` which was shown to be valid from the first one.
        unsafe {
            // Find the first element greater than or equal to the pivot.
            while l < r && is_less(vec_index_unchecked(v, val_size, l), pivot) {
                l += 1;
            }

            // Find the last element smaller that the pivot.
            while l < r && !is_less(vec_index_unchecked(v, val_size, r - 1), pivot) {
                r -= 1;
            }
        }

        (
            l + partition_in_blocks(
                vec_slice(v, val_size, l, r),
                val_size,
                pivot,
                is_less,
                scratch,
            ),
            l >= r,
        )

        // `_pivot_guard` goes out of scope and writes the pivot (which is a stack-allocated
        // variable) back into the slice where it originally was. This step is critical in ensuring
        // safety!
    };

    // Place the pivot between the two partitions.
    vec_swap(v, val_size, 0, mid);

    (mid, was_partitioned)
}

/// Partitions `v` into elements equal to `v[pivot]` followed by elements greater than `v[pivot]`.
///
/// Returns the number of elements equal to the pivot. It is assumed that `v` does not contain
/// elements smaller than the pivot.
pub(super) fn partition_equal(
    v: &mut [u8],
    val_size: usize,
    pivot: usize,
    is_less: &dyn Fn(*const u8, *const u8) -> bool,
) -> usize {
    // Place the pivot at the beginning of slice.
    vec_swap(v, val_size, 0, pivot);
    let (pivot, v) = vec_split_at(v, val_size, 1);
    let pivot = pivot.as_mut_ptr();

    let len = vec_len(v, val_size);
    if len == 0 {
        return 0;
    }

    // Now partition the slice.
    let mut l = 0;
    let mut r = len;
    loop {
        // SAFETY: The unsafety below involves indexing an array.
        // For the first one: We already do the bounds checking here with `l < r`.
        // For the second one: We initially have `l == 0` and `r == v.len()` and we checked that `l < r` at every indexing operation.
        //                     From here we know that `r` must be at least `r == l` which was shown to be valid from the first one.
        unsafe {
            // Find the first element greater than the pivot.
            while l < r && !is_less(pivot, vec_index_unchecked(v, val_size, l)) {
                l += 1;
            }

            // Find the last element equal to the pivot.
            loop {
                r -= 1;
                if l >= r || !is_less(pivot, vec_index_unchecked(v, val_size, r)) {
                    break;
                }
            }

            // Are we done?
            if l >= r {
                break;
            }

            // Swap the found pair of out-of-order elements.
            vec_swap(v, val_size, l, r);
            l += 1;
        }
    }

    // We found `l` elements equal to the pivot. Add 1 to account for the pivot itself.
    l + 1

    // `_pivot_guard` goes out of scope and writes the pivot (which is a stack-allocated variable)
    // back into the slice where it originally was. This step is critical in ensuring safety!
}

/// Scatters some elements around in an attempt to break patterns that might cause imbalanced
/// partitions in quicksort.
#[cold]
pub(super) fn break_patterns(v: &mut [u8], val_size: usize) {
    let len = vec_len(v, val_size);

    if len >= 8 {
        let mut seed = len;
        let mut gen_usize = || {
            // Pseudorandom number generator from the "Xorshift RNGs" paper by George Marsaglia.
            if usize::BITS <= 32 {
                let mut r = seed as u32;
                r ^= r << 13;
                r ^= r >> 17;
                r ^= r << 5;
                seed = r as usize;
                seed
            } else {
                let mut r = seed as u64;
                r ^= r << 13;
                r ^= r >> 7;
                r ^= r << 17;
                seed = r as usize;
                seed
            }
        };

        // Take random numbers modulo this number.
        // The number fits into `usize` because `len` is not greater than `isize::MAX`.
        let modulus = len.next_power_of_two();

        // Some pivot candidates will be in the nearby of this index. Let's randomize them.
        let pos = len / 4 * 2;

        for i in 0..3 {
            // Generate a random number modulo `len`. However, in order to avoid costly operations
            // we first take it modulo a power of two, and then decrease by `len` until it fits
            // into the range `[0, len - 1]`.
            let mut other = gen_usize() & (modulus - 1);

            // `other` is guaranteed to be less than `2 * len`.
            if other >= len {
                other -= len;
            }

            vec_swap(v, val_size, pos - 1 + i, other);
        }
    }
}

/// Chooses a pivot in `v` and returns the index and `true` if the slice is likely already sorted.
///
/// Elements in `v` might be reordered in the process.
pub(super) fn choose_pivot(
    v: &mut [u8],
    val_size: usize,
    is_less: &dyn Fn(*const u8, *const u8) -> bool,
) -> (usize, bool) {
    // Minimum length to choose the median-of-medians method.
    // Shorter slices use the simple median-of-three method.
    const SHORTEST_MEDIAN_OF_MEDIANS: usize = 50;
    // Maximum number of swaps that can be performed in this function.
    const MAX_SWAPS: usize = 4 * 3;

    let len = vec_len(v, val_size);

    // Three indices near which we are going to choose a pivot.
    let mut a = len / 4;
    let mut b = len / 4 * 2;
    let mut c = len / 4 * 3;

    // Counts the total number of swaps we are about to perform while sorting indices.
    let mut swaps = 0;

    if len >= 8 {
        // Swaps indices so that `v[a] <= v[b]`.
        // SAFETY: `len >= 8` so there are at least two elements in the neighborhoods of
        // `a`, `b` and `c`. This means the three calls to `sort_adjacent` result in
        // corresponding calls to `sort3` with valid 3-item neighborhoods around each
        // pointer, which in turn means the calls to `sort2` are done with valid
        // references. Thus the `v.get_unchecked` calls are safe, as is the `ptr::swap`
        // call.
        let mut sort2 = |a: &mut usize, b: &mut usize| unsafe {
            if is_less(
                vec_index_unchecked(v, val_size, *b),
                vec_index_unchecked(v, val_size, *a),
            ) {
                ptr::swap(a, b);
                swaps += 1;
            }
        };

        // Swaps indices so that `v[a] <= v[b] <= v[c]`.
        let mut sort3 = |a: &mut usize, b: &mut usize, c: &mut usize| {
            sort2(a, b);
            sort2(b, c);
            sort2(a, b);
        };

        if len >= SHORTEST_MEDIAN_OF_MEDIANS {
            // Finds the median of `v[a - 1], v[a], v[a + 1]` and stores the index into `a`.
            let mut sort_adjacent = |a: &mut usize| {
                let tmp = *a;
                sort3(&mut (tmp - 1), a, &mut (tmp + 1));
            };

            // Find medians in the neighborhoods of `a`, `b`, and `c`.
            sort_adjacent(&mut a);
            sort_adjacent(&mut b);
            sort_adjacent(&mut c);
        }

        // Find the median among `a`, `b`, and `c`.
        sort3(&mut a, &mut b, &mut c);
    }

    if swaps < MAX_SWAPS {
        (b, swaps == 0)
    } else {
        // The maximum number of swaps was performed. Chances are the slice is descending or mostly
        // descending, so reversing will probably help sort it faster.
        vec_reverse(v, val_size);
        (len - 1 - b, true)
    }
}

/// Sorts `v` recursively.
///
/// If the slice had a predecessor in the original array, it is specified as `pred`.
///
/// `limit` is the number of allowed imbalanced partitions before switching to `heapsort`. If zero,
/// this function will immediately switch to heapsort.
fn recurse(
    mut v: &mut [u8],
    val_size: usize,
    is_less: &dyn Fn(*const u8, *const u8) -> bool,
    mut pred: Option<*const u8>,
    mut limit: u32,
    scratch: *mut u8,
) {
    // Slices of up to this length get sorted using insertion sort.
    const MAX_INSERTION: usize = 20;

    // True if the last partitioning was reasonably balanced.
    let mut was_balanced = true;
    // True if the last partitioning didn't shuffle elements (the slice was already partitioned).
    let mut was_partitioned = true;

    loop {
        let len = vec_len(v, val_size);

        // Very short slices get sorted using insertion sort.
        if len <= MAX_INSERTION {
            if len >= 2 {
                insertion_sort_shift_left(v, val_size, 1, is_less, scratch);
            }
            return;
        }

        // If too many bad pivot choices were made, simply fall back to heapsort in order to
        // guarantee `O(n * log(n))` worst-case.
        if limit == 0 {
            heapsort(v, val_size, is_less);
            return;
        }

        // If the last partitioning was imbalanced, try breaking patterns in the slice by shuffling
        // some elements around. Hopefully we'll choose a better pivot this time.
        if !was_balanced {
            break_patterns(v, val_size);
            limit -= 1;
        }

        // Choose a pivot and try guessing whether the slice is already sorted.
        let (pivot, likely_sorted) = choose_pivot(v, val_size, is_less);

        // If the last partitioning was decently balanced and didn't shuffle elements, and if pivot
        // selection predicts the slice is likely already sorted...
        if was_balanced && was_partitioned && likely_sorted {
            // Try identifying several out-of-order elements and shifting them to correct
            // positions. If the slice ends up being completely sorted, we're done.
            if partial_insertion_sort(v, val_size, is_less, scratch) {
                return;
            }
        }

        // If the chosen pivot is equal to the predecessor, then it's the smallest element in the
        // slice. Partition the slice into elements equal to and elements greater than the pivot.
        // This case is usually hit when the slice contains many duplicate elements.
        if let Some(p) = pred {
            if !unsafe { is_less(p, vec_index_unchecked(v, val_size, pivot)) } {
                let mid = partition_equal(v, val_size, pivot, is_less);

                // Continue sorting elements greater than the pivot.
                v = vec_suffix(v, val_size, mid);
                continue;
            }
        }

        // Partition the slice.
        let (mid, was_p) = partition(v, val_size, pivot, is_less, scratch);
        was_balanced = cmp::min(mid, len - mid) >= len / 8;
        was_partitioned = was_p;

        // Split the slice into `left`, `pivot`, and `right`.
        let (left, right) = vec_split_at(v, val_size, mid);
        let (pivot, right) = vec_split_at(right, val_size, 1);
        let pivot = pivot.as_ptr();

        // Recurse into the shorter side only in order to minimize the total number of recursive
        // calls and consume less stack space. Then just continue with the longer side (this is
        // akin to tail recursion).
        if left.len() < right.len() {
            recurse(left, val_size, is_less, pred, limit, scratch);
            v = right;
            pred = Some(pivot);
        } else {
            recurse(right, val_size, is_less, Some(pivot), limit, scratch);
            v = left;
        }
    }
}

/// Sorts `v` using pattern-defeating quicksort, which is *O*(*n* \* log(*n*)) worst-case.
pub fn quicksort(
    v: &mut [u8],
    val_size: usize,
    is_less: &dyn Fn(*const u8, *const u8) -> bool,
    scratch: *mut u8,
) {
    // Sorting has no meaningful behavior on zero-sized types.
    if val_size == 0 {
        return;
    }

    // Limit the number of imbalanced partitions to `floor(log2(len)) + 1`.
    let limit = usize::BITS - vec_len(v, val_size).leading_zeros();

    recurse(v, val_size, is_less, None, limit, scratch);
}

pub fn unstable_sort<T: Ord + Debug>(slice: &mut [T]) {
    unstable_sort_by(slice, |x, y| x.cmp(y))
}

pub fn unstable_sort_by<T, F>(slice: &mut [T], cmp: F)
where
    F: Fn(&T, &T) -> Ordering,
{
    let byte_slice = unsafe {
        std::slice::from_raw_parts_mut(slice.as_mut_ptr() as *mut u8, std::mem::size_of_val(slice))
    };
    let is_less = (&|x: *const u8, y: *const u8| {
        let x = unsafe { &*(x as *const T) };
        let y = unsafe { &*(y as *const T) };
        cmp(x, y) == Ordering::Less
    }) as &dyn Fn(*const u8, *const u8) -> bool;

    quicksort(
        byte_slice,
        size_of::<T>(),
        is_less,
        <MaybeUninit<T>>::uninit().as_mut_ptr() as *mut u8,
    );
}

#[cfg(test)]
mod test {
    use super::unstable_sort;
    use proptest::{collection::vec, prelude::*};

    #[test]
    fn test_unstable_sort() {
        let corpus: Vec<Vec<i32>> = vec![
            vec![],
            vec![10],
            vec![5, 4],
            vec![1, 2, 3, 4, 5],
            vec![5, 3, 4, 1, 2],
            vec![
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1560281088, 234560113, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            ],
        ];

        for mut vec in corpus.into_iter() {
            let mut expected = vec.clone();
            expected.sort();

            unstable_sort(&mut vec);

            assert_eq!(vec, expected);
        }
    }

    prop_compose! {
        fn short_vec()(batch in vec(any::<u32>(), 0..30)) -> Vec<u32> {
            batch
        }
    }

    prop_compose! {
        fn long_vec()(batch in vec(any::<u32>(), 0..300)) -> Vec<u32> {
            batch
        }
    }

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
    struct TestStruct {
        f1: u16,
        f2: String,
        f3: i64,
        f4: i8,
        f5: String,
        f6: u8,
        f7: Vec<u8>,
    }

    fn test_struct() -> impl Strategy<Value = TestStruct> {
        (
            any::<u16>(),
            any::<String>(),
            any::<i64>(),
            any::<i8>(),
            any::<String>(),
            any::<u8>(),
            vec(any::<u8>(), 0..20),
        )
            .prop_map(|(f1, f2, f3, f4, f5, f6, f7)| TestStruct {
                f1,
                f2,
                f3,
                f4,
                f5,
                f6,
                f7,
            })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(10000))]

        #[test]
        fn unstable_sort_small_proptest(mut v in short_vec()) {
            let mut expected = v.clone();
            expected.sort();

            unstable_sort(&mut v);
            assert_eq!(v, expected);
        }

    }

    proptest! {
        #[test]
        fn unstable_sort_small_structs_proptest(mut v in vec(test_struct(), 0..25)) {
            let mut expected = v.clone();
            expected.sort();

            unstable_sort(&mut v);
            assert_eq!(v, expected);
        }

    }

    proptest! {
        #[test]
        fn unstable_sort_large_proptest(mut v in long_vec()) {
            let mut expected = v.clone();
            expected.sort();

            unstable_sort(&mut v);
            assert_eq!(v, expected);
        }
    }
}
