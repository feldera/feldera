//! Common logic for the consolidation of vectors of MonoidValues.
//!
//! Often we find ourselves with collections of records with associated weights
//! (often integers) where we want to reduce the collection to the point that
//! each record occurs at most once, with the accumulated weights. These methods
//! supply that functionality.

use crate::{
    algebra::{AddAssignByRef, HasZero, MonoidValue},
    utils::assume,
};
use bitvec::slice::BitSlice;
use std::{
    mem::{forget, replace},
    ops::AddAssign,
    ptr,
};

/// Sorts and consolidates `vec`.
///
/// This method will sort `vec` and then consolidate runs of more than one entry
/// with identical first elements by accumulating the second elements of the
/// pairs. Should the final accumulation be zero, the element is discarded.
pub fn consolidate<T, R>(vec: &mut Vec<(T, R)>)
where
    T: Ord,
    R: MonoidValue,
{
    // Benchmarks found that using `consolidate_slice()` (used by
    // `consolidate_from()`) is slower than just using `.dedup_by()` + `.retain()`
    // consolidate_from(vec, 0);

    vec.sort_unstable_by(|(key1, _), (key2, _)| key1.cmp(key2));

    // TODO: Combine the `.dedup_by()` and `.retain()` calls together
    vec.dedup_by(|(key1, data1), (key2, data2)| {
        if key1 == key2 {
            data2.add_assign(replace(data1, R::zero()));
            true
        } else {
            false
        }
    });
    vec.retain(|(_, data)| !data.is_zero());
}

/// Sorts and consolidate `vec[offset..]`.
///
/// This method will sort `vec[offset..]` and then consolidate runs of more than
/// one entry with identical first elements by accumulating the second elements
/// of the pairs. Should the final accumulation be zero, the element is
/// discarded.
pub fn consolidate_from<T, R>(vec: &mut Vec<(T, R)>, offset: usize)
where
    T: Ord,
    R: HasZero + AddAssign,
{
    // let length = consolidate_slice(&mut vec[offset..]);
    // vec.truncate(offset + length);

    vec[offset..].sort_unstable_by(|(key1, _), (key2, _)| key1.cmp(key2));
    dedup_starting_at(vec, offset, |(key1, data1), (key2, data2)| {
        if key1 == key2 {
            data2.add_assign(replace(data1, R::zero()));
            true
        } else {
            false
        }
    });
    retain_starting_at(vec, offset, |(_, data)| !data.is_zero());
}

/// Sorts and consolidate `vec[offset..]`.
///
/// This method will sort `vec[offset..]` and then consolidate runs of more than
/// one entry with identical first elements by accumulating the second elements
/// of the pairs. Should the final accumulation be zero, the element is
/// discarded.
pub fn consolidate_paired_vecs_from<T, R>(
    keys: &mut Vec<T>,
    diffs: &mut Vec<R>,
    indices: &mut Vec<usize>,
    offset: usize,
) where
    T: Ord,
    R: HasZero + AddAssign,
{
    // Ensure that the paired slices are the same length
    assert_eq!(keys.len(), diffs.len());

    // Clear and pre-allocate the indices buffer
    indices.clear();
    indices.reserve(keys.len());
    // TODO: We can do this in a vectorized manner, the assembly isn't ideal https://godbolt.org/z/4TbK6Mzec
    indices.extend(0..keys.len());

    // Ideally we'd combine the sorting and value merging portions
    // This line right here is literally the hottest code within the entirety of the
    // program. It makes up 90% of the work done while joining or merging anything
    indices.sort_unstable_by(|&idx1, &idx2| {
        // Safety: All indices within `indices` are in-bounds of `keys` and `diffs`
        unsafe { keys.get_unchecked(idx1).cmp(keys.get_unchecked(idx2)) }
    });

    // Safety: All indices within `indices` are valid
    let diffs_ptr = diffs.as_mut_ptr();
    dedup_starting_at(indices, offset, |&mut idx1, &mut idx2| unsafe {
        debug_assert!(idx1 < keys.len() && idx2 < keys.len());

        if keys.get_unchecked(idx1) == keys.get_unchecked(idx2) {
            debug_assert_ne!(idx1, idx2);
            let data1 = replace(&mut *diffs_ptr.add(idx1), R::zero());
            let data2 = &mut *diffs_ptr.add(idx2);
            data2.add_assign(data1);

            true
        } else {
            false
        }
    });
    retain_starting_at(indices, offset, |&mut idx| unsafe {
        !diffs.get_unchecked(idx).is_zero()
    });
}

/// Sorts and consolidates a slice, returning the valid prefix length.
// TODO: I'm pretty sure there's some improvements to be made here.
//       We don't really need (pure) slice consolidation from what I've
//       seen, we only actually care about consolidating vectors and
//       portions *of* vectors, so taking a starting index and a vector
//       would allow us to operate over the vec with the ability to discard
//       elements, meaning that we could drop elements instead of swapping
//       them once their diff hits zero. Is that significant? I don't really
//       know, but ~1 second to consolidate 10 million elements is
//       nearly intolerable, combining the sorting and compacting processes
//       could help alleviate that though.
pub fn consolidate_slice<T, R>(slice: &mut [(T, R)]) -> usize
where
    T: Ord,
    R: AddAssignByRef + HasZero,
{
    // Ideally we'd combine the sorting and value merging portions
    // This line right here is literally the hottest code within the entirety of the
    // program. It makes up 90% of the work done while joining or merging anything
    slice.sort_unstable_by(|(key1, _), (key2, _)| key1.cmp(key2));
    consolidate_slice_inner(
        slice,
        |(key1, _), (key2, _)| key1 == key2,
        |(_, diff1), (_, diff2)| diff1.add_assign_by_ref(diff2),
        |(_, diff)| diff.is_zero(),
    )
}

pub fn consolidate_paired_slices<T, R>(
    keys: &mut [T],
    diffs: &mut [R],
    indices: &mut Vec<usize>,
) -> usize
where
    T: Ord,
    R: AddAssignByRef + HasZero,
{
    // Ensure that the paired slices are the same length
    assert_eq!(keys.len(), diffs.len());

    // Clear and pre-allocate the indices buffer
    fill_indices::fill_indices(keys.len(), indices);
    debug_assert_eq!(indices.len(), keys.len());

    // Ideally we'd combine the sorting and value merging portions
    // These lines right here are literally the hottest code within the entirety of
    // the program. They make up 90% of the work done while joining or merging
    // anything
    indices.sort_unstable_by(|&idx1, &idx2| {
        // Safety: All indices within `indices` are in-bounds of `keys` and `diffs`
        unsafe { keys.get_unchecked(idx1).cmp(keys.get_unchecked(idx2)) }
    });

    // Safety: All indices within `indices` are in-bounds of `keys` and `diffs`
    let valid_prefix = unsafe {
        let diffs_ptr = diffs.as_mut_ptr();

        consolidate_slice_inner(
            indices,
            |&idx1, &idx2| keys.get_unchecked(idx1) == keys.get_unchecked(idx2),
            |&mut idx1, &idx2| (*diffs_ptr.add(idx1)).add_assign_by_ref(&*diffs_ptr.add(idx2)),
            |&idx| (*diffs_ptr.add(idx)).is_zero(),
        )
    };

    // Safety: All indices within `indices` are valid and `keys`, `diffs` and
    // `indices` all have the same length
    unsafe { shuffle_by_indices(diffs, keys, indices) };

    valid_prefix
}

/// Shuffles all values within `values` to the position prescribed by `indices`
/// while using `indices` as scratch space
///
/// The contents of `indices` are unspecified after the function is called
///
/// # Safety
///
/// - `values` and `diffs` must have the same length
/// - `indices` must be the same length or longer than `values` and `diffs`
///   (allows for over-alignment)
/// - Every index within `indices` must be a valid index into `values` and
///   `diffs`
/// - Every index within `indices` must be unique
/// - `indices` should contain the offset of every value/diff in the form of
///   `0..length` (the most likely scenario is that `indices` is the product of
///   `(0..len).collect()` or a similar invocation)
#[doc(hidden)]
pub unsafe fn shuffle_by_indices<T, R>(values: &mut [T], diffs: &mut [R], indices: &mut [usize]) {
    // Validate our preconditions
    if cfg!(debug_assertions) {
        for &index in &indices[..values.len()] {
            // Indices should be valid indices
            assert!(index < diffs.len());
            assert!(index < values.len());
            assert!(index < indices.len());

            // All indices are unique
            assert_eq!(indices.iter().filter(|&&idx| idx == index).count(), 1);
        }
    }

    assume(
        values.len() == diffs.len()
            && values.len() <= indices.len()
            && diffs.len() <= indices.len(),
    );

    let (values_ptr, diffs_ptr) = (values.as_mut_ptr(), diffs.as_mut_ptr());
    for i in 0..values.len() {
        debug_assert!(i < indices.len());
        debug_assert!(indices[i] < values.len() && indices[i] < diffs.len());

        if i != *indices.get_unchecked(i) {
            let (temp_val, temp_diff) = (values_ptr.add(i).read(), diffs_ptr.add(i).read());
            let mut j = i;

            loop {
                debug_assert!(j < indices.len());
                let k = *indices.get_unchecked(j);
                if i == k {
                    break;
                }

                debug_assert!(k < indices.len());
                values_ptr.add(j).write(values_ptr.add(k).read());
                diffs_ptr.add(j).write(diffs_ptr.add(k).read());
                *indices.get_unchecked_mut(j) = j;
                j = k;
            }

            debug_assert!(j < indices.len());
            values_ptr.add(j).write(temp_val);
            diffs_ptr.add(j).write(temp_diff);
            *indices.get_unchecked_mut(j) = j;
        }
    }
}

/// Shuffles all values within `values` to the position prescribed by `indices`
/// while using `indices` as scratch space
///
/// The contents of `indices` are unspecified after the function is called
///
/// # Safety
///
/// - `values`, `diffs` and `indices` must have the same length
/// - Every index within `indices` must be a valid index into `values` and
///   `diffs`
/// - Every index within `indices` must be unique
/// - `indices` should contain the offset of every value/diff in the form of
///   `0..length` (the most likely scenario is that `indices` is the product of
///   `(0..len).collect()` or a similar invocation)
#[doc(hidden)]
pub unsafe fn shuffle_by_indices_bitvec<T, R>(
    values: &mut [T],
    diffs: &mut [R],
    indices: &mut [usize],
    visited: &mut BitSlice,
) {
    assume(
        values.len() == diffs.len()
            && values.len() <= indices.len()
            && diffs.len() <= indices.len()
            // Every bit within `visited` should be false
            && visited.not_any(),
    );

    let (values_ptr, diffs_ptr) = (values.as_mut_ptr(), diffs.as_mut_ptr());
    for idx in 0..values.len() {
        debug_assert!(idx < visited.len());
        if *visited.get_unchecked(idx) {
            continue;
        }
        visited.set_unchecked(idx, true);

        let mut previous = idx;
        let mut current = *indices.get_unchecked(idx);

        while current != idx {
            debug_assert!(current < visited.len());
            visited.set_unchecked(current, true);

            ptr::swap(values_ptr.add(current), values_ptr.add(previous));
            ptr::swap(diffs_ptr.add(current), diffs_ptr.add(previous));

            previous = current;
            current = *indices.get_unchecked(current);
        }
    }
}

/// The innards of `consolidate_slice()`, not meant to be used directly
///
/// Expects `slice` to be pre-sorted
#[doc(hidden)]
pub fn consolidate_slice_inner<T, E, M, Z>(
    slice: &mut [T],
    mut are_equal: E,
    mut merge: M,
    mut is_zero: Z,
) -> usize
where
    E: FnMut(&T, &T) -> bool,
    M: FnMut(&mut T, &T),
    Z: FnMut(&T) -> bool,
{
    let slice_len = slice.len();
    let slice_ptr = slice.as_mut_ptr();

    // Counts the number of distinct known-non-zero accumulations. Indexes the write
    // location.
    let mut offset = 0;
    for index in 1..slice_len {
        // The following unsafe block elides various bounds checks, using the reasoning
        // that `offset` is always strictly less than `index` at the beginning
        // of each iteration. This is initially true, and in each iteration
        // `offset` can increase by at most one (whereas `index` always
        // increases by one). As `index` is always in bounds, and `offset` starts at
        // zero, it too is always in bounds.
        //
        // LLVM appears to struggle to optimize out Rust's split_at_mut, which would
        // prove disjointness using run-time tests.
        unsafe {
            debug_assert!(offset < index);
            debug_assert!(index < slice_len);
            debug_assert!(offset < slice_len);

            // LOOP INVARIANT: offset < index
            let ptr1 = slice_ptr.add(offset);
            let ptr2 = slice_ptr.add(index);

            // If the values are equal, merge them
            if are_equal(&*ptr1, &*ptr2) {
                merge(&mut *ptr1, &*ptr2)

            // Otherwise continue
            } else {
                if !is_zero(&*ptr1) {
                    offset += 1;
                }

                let ptr1 = slice_ptr.add(offset);
                ptr::swap(ptr1, ptr2);
            }
        }
    }

    if offset < slice_len && unsafe { !is_zero(&*slice_ptr.add(offset)) } {
        offset += 1;
    }

    offset
}

/// An implementation of `Vec::dedup_by()` that takes a starting point
///
/// Modified from [the stdlib](https://doc.rust-lang.org/std/vec/struct.Vec.html#method.dedup_by)
fn dedup_starting_at<T, F>(vec: &mut Vec<T>, starting_point: usize, mut same_bucket: F)
where
    F: FnMut(&mut T, &mut T) -> bool,
{
    let len = vec.len();
    if len <= 1 || starting_point + 1 >= vec.len() {
        return;
    }

    // INVARIANT: vec.len() > read >= write > write-1 >= 0
    struct FillGapOnDrop<'a, T> {
        // Offset of the element we want to check if it is duplicate
        read: usize,

        // Offset of the place where we want to place the non-duplicate
        // when we find it.
        write: usize,

        // The Vec that would need correction if `same_bucket` panicked
        vec: &'a mut Vec<T>,
    }

    impl<'a, T> Drop for FillGapOnDrop<'a, T> {
        fn drop(&mut self) {
            // This code gets executed when `same_bucket` panics

            // SAFETY: invariant guarantees that `read - write`
            // and `len - read` never overflow and that the copy is always
            // in-bounds.
            unsafe {
                let ptr = self.vec.as_mut_ptr();
                let len = self.vec.len();

                // How many items were left when `same_bucket` panicked.
                // Basically vec[read..].len()
                let items_left = len.wrapping_sub(self.read);

                // Pointer to first item in vec[write..write+items_left] slice
                let dropped_ptr = ptr.add(self.write);
                // Pointer to first item in vec[read..] slice
                let valid_ptr = ptr.add(self.read);

                // Copy `vec[read..]` to `vec[write..write+items_left]`.
                // The slices can overlap, so `copy_nonoverlapping` cannot be used
                ptr::copy(valid_ptr, dropped_ptr, items_left);

                // How many items have been already dropped
                // Basically vec[read..write].len()
                let dropped = self.read.wrapping_sub(self.write);

                self.vec.set_len(len - dropped);
            }
        }
    }

    let mut gap = FillGapOnDrop {
        read: starting_point + 1,
        write: starting_point + 1,
        vec,
    };
    let ptr = gap.vec.as_mut_ptr();

    // Drop items while going through Vec, it should be more efficient than
    // doing slice partition_dedup + truncate

    // SAFETY: Because of the invariant, read_ptr, prev_ptr and write_ptr
    // are always in-bounds and read_ptr never aliases prev_ptr
    unsafe {
        while gap.read < len {
            let read_ptr = ptr.add(gap.read);
            let prev_ptr = ptr.add(gap.write.wrapping_sub(1));

            if same_bucket(&mut *read_ptr, &mut *prev_ptr) {
                // Increase `gap.read` now since the drop may panic.
                gap.read += 1;
                // We have found duplicate, drop it in-place
                ptr::drop_in_place(read_ptr);
            } else {
                let write_ptr = ptr.add(gap.write);

                // Because `read_ptr` can be equal to `write_ptr`, we either
                // have to use `copy` or conditional `copy_nonoverlapping`.
                // Looks like the first option is faster.
                ptr::copy(read_ptr, write_ptr, 1);

                // We have filled that place, so go further
                gap.write += 1;
                gap.read += 1;
            }
        }

        // Technically we could let `gap` clean up with its Drop, but
        // when `same_bucket` is guaranteed to not panic, this bloats a little
        // the codegen, so we just do it manually
        gap.vec.set_len(gap.write);
        forget(gap);
    }
}

/// An implementation of `Vec::retain()` that takes a starting point
///
/// Modified from [the stdlib](https://doc.rust-lang.org/std/vec/struct.Vec.html#method.retain)
fn retain_starting_at<T, F>(vec: &mut Vec<T>, starting_point: usize, mut retain: F)
where
    F: FnMut(&mut T) -> bool,
{
    if vec.is_empty() || starting_point >= vec.len() {
        return;
    }

    let original_len = vec.len();
    // Avoid double drop if the drop guard is not executed,
    // since we may make some holes during the process.
    unsafe { vec.set_len(0) };

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
    struct BackshiftOnDrop<'a, T> {
        v: &'a mut Vec<T>,
        processed_len: usize,
        deleted_cnt: usize,
        original_len: usize,
    }

    impl<T> Drop for BackshiftOnDrop<'_, T> {
        fn drop(&mut self) {
            if self.deleted_cnt > 0 {
                // SAFETY: Trailing unchecked items must be valid since we never touch them.
                unsafe {
                    ptr::copy(
                        self.v.as_ptr().add(self.processed_len),
                        self.v
                            .as_mut_ptr()
                            .add(self.processed_len - self.deleted_cnt),
                        self.original_len - self.processed_len,
                    );
                }
            }

            // SAFETY: After filling holes, all items are in contiguous memory.
            unsafe {
                self.v.set_len(self.original_len - self.deleted_cnt);
            }
        }
    }

    let mut g = BackshiftOnDrop {
        v: vec,
        processed_len: starting_point,
        deleted_cnt: 0,
        original_len,
    };

    fn process_loop<F, T, const DELETED: bool>(
        original_len: usize,
        f: &mut F,
        g: &mut BackshiftOnDrop<'_, T>,
    ) where
        F: FnMut(&mut T) -> bool,
    {
        while g.processed_len != original_len {
            // SAFETY: Unchecked element must be valid.
            let cur = unsafe { &mut *g.v.as_mut_ptr().add(g.processed_len) };
            if !f(cur) {
                // Advance early to avoid double drop if `drop_in_place` panicked.
                g.processed_len += 1;
                g.deleted_cnt += 1;

                // SAFETY: We never touch this element again after dropped.
                unsafe { ptr::drop_in_place(cur) };

                // We already advanced the counter.
                if DELETED {
                    continue;
                } else {
                    break;
                }
            }

            if DELETED {
                // SAFETY: `deleted_cnt` > 0, so the hole slot must not overlap with current
                // element. We use copy for move, and never touch this element
                // again.
                unsafe {
                    let hole_slot = g.v.as_mut_ptr().add(g.processed_len - g.deleted_cnt);
                    ptr::copy_nonoverlapping(cur, hole_slot, 1);
                }
            }

            g.processed_len += 1;
        }
    }

    // Stage 1: Nothing was deleted.
    process_loop::<F, T, false>(original_len, &mut retain, &mut g);

    // Stage 2: Some elements were deleted.
    process_loop::<F, T, true>(original_len, &mut retain, &mut g);

    // All item are processed. This can be optimized to `set_len` by LLVM.
    drop(g);
}

// TODO: More arch support (mainly arm and aarch64)
mod fill_indices {
    use crate::utils::next_multiple_of;
    use std::{
        mem::transmute,
        sync::atomic::{AtomicPtr, Ordering},
    };

    #[cfg(target_arch = "x86")]
    use std::arch::x86;
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64 as x86;

    /// Memoizes feature selection so that it's not repeated on every invocation
    static FILL_INDICES: AtomicPtr<()> = AtomicPtr::new(select_impl as *mut ());

    pub fn fill_indices(length: usize, indices: &mut Vec<usize>) {
        let implementation = unsafe {
            transmute::<*mut (), fn(usize, &mut Vec<usize>)>(FILL_INDICES.load(Ordering::Relaxed))
        };

        implementation(length, indices);
    }

    /// Selects the filling implementation based on the current machine's
    /// available features
    fn select_impl(length: usize, indices: &mut Vec<usize>) {
        let mut selected: unsafe fn(usize, &mut Vec<usize>) = fill_indices_naive;

        // x86 and x86-64 feature selection
        #[cfg(all(
            any(target_arch = "x86", target_arch = "x86_64"),
            any(target_pointer_width = "32", target_pointer_width = "64")
        ))]
        {
            if is_x86_feature_detected!("avx2") {
                selected = fill_indices_x86_avx2;
            } else if is_x86_feature_detected!("sse2") {
                selected = fill_indices_x86_sse2;
            }
        }

        // wasm32 feature selection
        #[cfg(all(target_arch = "wasm32", target_feature = "simd128"))]
        {
            selected = fill_indices_wasm32_simd128;
        }

        FILL_INDICES.store(selected as *mut (), Ordering::Relaxed);

        unsafe { selected(length, indices) }
    }

    /// Naive version of that uses `Vec::extend()`
    unsafe fn fill_indices_naive(length: usize, indices: &mut Vec<usize>) {
        if length == 0 {
            return;
        }

        indices.clear();
        indices.reserve(length);
        indices.extend(0..length);
    }

    /// Fills indices using sse2 simd
    #[target_feature(enable = "sse2")]
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    unsafe fn fill_indices_x86_sse2(original_length: usize, indices: &mut Vec<usize>) {
        use x86::{
            __m128i, _mm_add_epi32, _mm_add_epi64, _mm_set1_epi32, _mm_set1_epi64x, _mm_set_epi32,
            _mm_set_epi64x, _mm_storeu_si128,
        };

        if original_length == 0 {
            return;
        }
        indices.clear();

        // If usize is 32 bits
        if cfg!(target_pointer_width = "32") {
            // Round up the next multiple of four since four u32s fit into a __m128i
            let length = next_multiple_of(original_length, 4);
            indices.reserve(length);

            let mut index = _mm_set_epi32(3, 2, 1, 0);
            let four = _mm_set1_epi32(4);

            let mut indices_ptr = indices.as_mut_ptr().cast::<__m128i>();
            let indices_end = indices_ptr.add(length / 4);

            while indices_ptr < indices_end {
                // Store the indices into the vec
                _mm_storeu_si128(indices_ptr, index);

                // Increment the indices
                index = _mm_add_epi32(index, four);
                // Increment the indices pointer
                indices_ptr = indices_ptr.add(1);
            }

        // If usize is 64 bits
        } else if cfg!(target_pointer_width = "64") {
            // Round up the next multiple of two since two u64s fit into a __m128i
            let length = next_multiple_of(original_length, 2);
            indices.reserve(length);

            let mut index = _mm_set_epi64x(1, 0);
            let two = _mm_set1_epi64x(2);

            let mut indices_ptr = indices.as_mut_ptr().cast::<__m128i>();
            let indices_end = indices_ptr.add(length / 2);

            while indices_ptr < indices_end {
                // Store the indices into the vec
                _mm_storeu_si128(indices_ptr, index);

                // Increment the indices
                index = _mm_add_epi64(index, two);
                // Increment the indices pointer
                indices_ptr = indices_ptr.add(1);
            }

        // Only 32bit and 64bit targets are supported for this function
        } else {
            unreachable!()
        }

        indices.set_len(original_length);
    }

    /// Fills indices using avx2 simd
    #[target_feature(enable = "avx2")]
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    unsafe fn fill_indices_x86_avx2(original_length: usize, indices: &mut Vec<usize>) {
        use x86::{
            __m256i, _mm256_add_epi32, _mm256_add_epi64, _mm256_set1_epi32, _mm256_set1_epi64x,
            _mm256_set_epi32, _mm256_set_epi64x, _mm256_storeu_si256,
        };

        if original_length == 0 {
            return;
        }
        indices.clear();

        // If usize is 32 bits
        if cfg!(target_pointer_width = "32") {
            // Round up the next multiple of eight since eight u32s fit into a __m256i
            let length = next_multiple_of(original_length, 8);
            indices.reserve(length);

            let mut index = _mm256_set_epi32(7, 6, 5, 4, 3, 2, 1, 0);
            let eight = _mm256_set1_epi32(8);

            let mut indices_ptr = indices.as_mut_ptr().cast::<__m256i>();
            let indices_end = indices_ptr.add(length / 8);

            while indices_ptr < indices_end {
                // Store the indices into the vec
                _mm256_storeu_si256(indices_ptr, index);

                // Increment the indices
                index = _mm256_add_epi32(index, eight);
                // Increment the indices pointer
                indices_ptr = indices_ptr.add(1);
            }

        // If usize is 64 bits
        } else if cfg!(target_pointer_width = "64") {
            // Round up the next multiple of four since four u64s fit into a __m256i
            let length = next_multiple_of(original_length, 4);
            indices.reserve(length);

            let mut index = _mm256_set_epi64x(3, 2, 1, 0);
            let four = _mm256_set1_epi64x(4);

            let mut indices_ptr = indices.as_mut_ptr().cast::<__m256i>();
            let indices_end = indices_ptr.add(length / 4);

            while indices_ptr < indices_end {
                // Store the indices into the vec
                _mm256_storeu_si256(indices_ptr, index);

                // Increment the indices
                index = _mm256_add_epi64(index, four);
                // Increment the indices pointer
                indices_ptr = indices_ptr.add(1);
            }

        // Only 32bit and 64bit targets are supported for this function
        } else {
            unreachable!()
        }

        indices.set_len(original_length);
    }

    /// Fills indices using wasm simd
    // FIXME: Add `target_family = "wasm"`/`target_arch = "wasm64"` support
    #[cfg(target_family = "wasm32")]
    #[target_feature(enable = "simd128")]
    unsafe fn fill_indices_wasm32_simd128(original_length: usize, indices: &mut Vec<usize>) {
        use std::arch::wasm32::{i32x4, i32x4_add, i32x4_splat, v128, v128_store};

        if original_length == 0 {
            return;
        }
        indices.clear();

        // Round up the next multiple of four since four u32s fit into a v128
        let length = next_multiple_of(original_length, 4);
        indices.reserve(length);

        let mut index = i32x4(3, 2, 1, 0);
        let four = i32x4_splat(4);

        let mut indices_ptr = indices.as_mut_ptr().cast::<v128>();
        let indices_end = indices_ptr.add(length / 4);

        while indices_ptr < indices_end {
            // Store the indices into the vec
            v128_store(indices_ptr, index);

            // Increment the indices
            index = i32x4_add(index, four);

            // Increment the indices pointer
            indices_ptr = indices_ptr.add(1);
        }

        indices.set_len(original_length);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_consolidate() {
        let test_cases = vec![
            (vec![("a", -1), ("b", -2), ("a", 1)], vec![("b", -2)]),
            (vec![("a", -1), ("b", 0), ("a", 1)], vec![]),
            (vec![("a", 0)], vec![]),
            (vec![("a", 0), ("b", 0)], vec![]),
            (vec![("a", 1), ("b", 1)], vec![("a", 1), ("b", 1)]),
        ];

        for (mut input, output) in test_cases {
            consolidate(&mut input);
            assert_eq!(input, output);
        }
    }

    #[test]
    fn test_consolidate_from_start() {
        let test_cases = vec![
            (vec![("a", -1), ("b", -2), ("a", 1)], vec![("b", -2)]),
            (vec![("a", -1), ("b", 0), ("a", 1)], vec![]),
            (vec![("a", 0)], vec![]),
            (vec![("a", 0), ("b", 0)], vec![]),
            (vec![("a", 1), ("b", 1)], vec![("a", 1), ("b", 1)]),
        ];

        for (mut input, output) in test_cases {
            consolidate_from(&mut input, 0);
            assert_eq!(input, output);
        }
    }

    #[test]
    fn test_consolidate_from() {
        let test_cases = vec![
            (
                vec![("a", -1), ("b", -2), ("a", 1)],
                vec![("a", -1), ("a", 1), ("b", -2)],
            ),
            (
                vec![("a", -1), ("b", 0), ("a", 1)],
                vec![("a", -1), ("a", 1)],
            ),
            (vec![("a", 0)], vec![("a", 0)]),
            (vec![("a", 0), ("b", 0)], vec![("a", 0)]),
            (vec![("a", 1), ("b", 1)], vec![("a", 1), ("b", 1)]),
        ];

        for (mut input, output) in test_cases {
            consolidate_from(&mut input, 1);
            assert_eq!(input, output);
        }
    }

    #[test]
    fn test_consolidate_slice() {
        let test_cases = vec![
            (vec![("a", -1), ("b", -2), ("a", 1)], vec![("b", -2)]),
            (vec![("a", -1), ("b", 0), ("a", 1)], vec![]),
            (vec![("a", 0)], vec![]),
            (vec![("a", 0), ("b", 0)], vec![]),
            (vec![("a", 1), ("b", 1)], vec![("a", 1), ("b", 1)]),
        ];

        for (mut input, output) in test_cases {
            let length = consolidate_slice(&mut input);
            assert_eq!(input[..length], output);
        }
    }

    #[test]
    fn test_consolidate_paired_slices() {
        let test_cases = vec![
            (
                (vec!["a", "b", "a"], vec![-1, -2, 1]),
                (vec!["b"], vec![-2]),
            ),
            ((vec!["a", "b", "a"], vec![-1, 0, 1]), (vec![], vec![])),
            ((vec!["a"], vec![0]), (vec![], vec![])),
            ((vec!["a", "b"], vec![0, 0]), (vec![], vec![])),
            ((vec!["a", "b"], vec![1, 1]), (vec!["a", "b"], vec![1, 1])),
        ];

        let mut indices = Vec::with_capacity(10);
        for ((mut keys, mut values), (output_keys, output_values)) in test_cases {
            let length = consolidate_paired_slices(&mut keys, &mut values, &mut indices);
            assert_eq!(keys[..length], output_keys);
            assert_eq!(values[..length], output_values);
        }
    }

    #[test]
    fn offset_dedup() {
        let test_cases = vec![
            (vec![], 0, vec![]),
            (vec![1, 2, 3, 4], 0, vec![1, 2, 3, 4]),
            (vec![1, 2, 3, 4], 2, vec![1, 2, 3, 4]),
            (vec![1, 2, 3, 4, 4, 4, 4, 4, 4], 3, vec![1, 2, 3, 4]),
            (
                vec![1, 2, 3, 4, 4, 4, 4, 4, 4, 6, 5, 4, 4, 6, 6, 6, 6, 7, 2, 3],
                3,
                vec![1, 2, 3, 4, 6, 5, 4, 6, 7, 2, 3],
            ),
            (
                vec![1, 2, 3, 4, 4, 4, 4, 4, 4, 6, 5, 4, 4, 6, 6, 6, 6, 7, 2, 3],
                5,
                vec![1, 2, 3, 4, 4, 4, 6, 5, 4, 6, 7, 2, 3],
            ),
        ];

        for (mut input, starting_point, output) in test_cases {
            dedup_starting_at(&mut input, starting_point, |a, b| *a == *b);
            assert_eq!(input, output);
        }
    }

    #[test]
    fn offset_retain() {
        let test_cases = vec![
            (vec![], 0, vec![]),
            (
                vec![(1, true), (2, true), (3, true), (4, true)],
                0,
                vec![(1, true), (2, true), (3, true), (4, true)],
            ),
            (
                vec![(1, false), (2, true), (3, true), (4, true)],
                2,
                vec![(1, false), (2, true), (3, true), (4, true)],
            ),
            (
                vec![
                    (1, true),
                    (2, false),
                    (3, false),
                    (4, true),
                    (5, true),
                    (6, false),
                    (7, true),
                    (8, false),
                    (9, false),
                ],
                3,
                vec![
                    (1, true),
                    (2, false),
                    (3, false),
                    (4, true),
                    (5, true),
                    (7, true),
                ],
            ),
        ];

        for (mut input, starting_point, output) in test_cases {
            retain_starting_at(&mut input, starting_point, |(_, cond)| *cond);
            assert_eq!(input, output);
        }
    }

    #[cfg_attr(miri, ignore)]
    mod proptests {
        use crate::{
            trace::consolidation::{
                consolidate, consolidate_from, consolidate_slice, dedup_starting_at,
                retain_starting_at, shuffle_by_indices, shuffle_by_indices_bitvec,
            },
            utils::VecExt,
        };
        use bitvec::vec::BitVec;
        use proptest::{collection::vec, prelude::*};
        use std::collections::BTreeMap;

        prop_compose! {
            /// Create a batch data tuple
            fn tuple()(key in 0..10_000usize, value in 0..10_000usize, diff in -10_000..=10_000isize) -> ((usize, usize), isize) {
                ((key, value), diff)
            }
        }

        prop_compose! {
            /// Generate a random batch of data
            fn batch()(batch in vec(tuple(), 0..50_000)) -> Vec<((usize, usize), isize)> {
                batch
            }
        }

        prop_compose! {
            fn random_vec()(batch in vec(any::<u16>(), 0..5000)) -> Vec<u16> {
                batch
            }
        }

        prop_compose! {
            fn random_paired_vecs()
                (len in 0..5000usize)
                (left in vec(any::<u16>(), len), right in vec(any::<i16>(), len))
            -> (Vec<u16>, Vec<i16>) {
                assert_eq!(left.len(), right.len());
                (left, right)
            }
        }

        fn batch_data(batch: &[((usize, usize), isize)]) -> BTreeMap<(usize, usize), i64> {
            let mut values = BTreeMap::new();
            for &(tuple, diff) in batch {
                values
                    .entry(tuple)
                    .and_modify(|acc| *acc += diff as i64)
                    .or_insert(diff as i64);
            }

            // Elements with a value of zero are removed in consolidation
            values.retain(|_, &mut diff| diff != 0);
            values
        }

        proptest! {
            #[test]
            fn consolidate_batch(mut batch in batch()) {
                let expected = batch_data(&batch);
                consolidate(&mut batch);
                let output = batch_data(&batch);

                // Ensure the batch is sorted
                prop_assert!(batch.is_sorted_by(|(a, _), (b, _)| a.partial_cmp(b)));
                // Ensure no diff values are zero
                prop_assert!(batch.iter().all(|&(_, diff)| diff != 0));
                // Ensure the aggregated data is the same
                prop_assert_eq!(expected, output);
            }

            #[test]
            fn consolidate_impls_are_equivalent(batch in batch()) {
                let expected = batch_data(&batch);

                let mut vec = batch.clone();
                consolidate(&mut vec);
                prop_assert!(vec.iter().all(|&(_, diff)| diff != 0));
                prop_assert!(vec.is_sorted_by(|(a, _), (b, _)| a.partial_cmp(b)));
                prop_assert!(vec.iter().all(|&(_, diff)| diff != 0));
                prop_assert_eq!(&expected, &batch_data(&vec));

                let mut vec_offset = batch.clone();
                consolidate_from(&mut vec_offset, 0);
                prop_assert!(vec_offset.iter().all(|&(_, diff)| diff != 0));
                prop_assert!(vec_offset.is_sorted_by(|(a, _), (b, _)| a.partial_cmp(b)));
                prop_assert!(vec_offset.iter().all(|&(_, diff)| diff != 0));
                prop_assert_eq!(&expected, &batch_data(&vec));
                prop_assert_eq!(&vec, &vec_offset);

                let mut slice = batch;
                let len = consolidate_slice(&mut slice);
                prop_assert!(slice[..len].iter().all(|&(_, diff)| diff != 0));
                // prop_assert!(slice[..len].is_sorted_by(|(a, _), (b, _)| a.partial_cmp(b)));
                prop_assert!(slice[..len].iter().all(|&(_, diff)| diff != 0));
                prop_assert_eq!(&expected, &batch_data(&slice[..len]));
                prop_assert_eq!(&vec, &slice[..len]);
            }

            #[test]
            fn retain_equivalence(mut expected in random_vec()) {
                let mut output = expected.clone();
                retain_starting_at(&mut output, 0, |a| *a % 5 == 0);
                expected.retain(|a| *a % 5 == 0);
                prop_assert_eq!(output, expected);
            }

            #[test]
            fn dedup_equivalence(mut expected in random_vec()) {
                let mut output = expected.clone();
                dedup_starting_at(&mut output, 0, |a, b| *a == *b);
                expected.dedup_by(|a, b| *a == *b);
                prop_assert_eq!(output, expected);
            }

            #[test]
            fn shuffle_by_indices_equivalence((mut values, mut diffs) in random_paired_vecs()) {
                let mut expected_indices: Vec<_> = (0..values.len()).collect();
                expected_indices.sort_by_key(|&idx| values[idx]);

                let (mut output_values, mut output_diffs) = (vec![0; values.len()], vec![0; values.len()]);
                for (current, &idx) in expected_indices.iter().enumerate() {
                    output_values[current] = values[idx];
                    output_diffs[current] = diffs[idx];
                }

                unsafe { shuffle_by_indices(&mut values, &mut diffs, &mut expected_indices) };
                prop_assert_eq!(values, output_values);
                prop_assert_eq!(diffs, output_diffs);
            }

            #[test]
            fn shuffle_by_indices_bitvec_equivalence((mut values, mut diffs) in random_paired_vecs()) {
                let mut visited = BitVec::repeat(false, values.len());
                let mut expected_indices: Vec<_> = (0..values.len()).collect();
                expected_indices.sort_by_key(|&idx| values[idx]);

                let (mut output_values, mut output_diffs) = (vec![0; values.len()], vec![0; values.len()]);
                for (current, &idx) in expected_indices.iter().enumerate() {
                    output_values[current] = values[idx];
                    output_diffs[current] = diffs[idx];
                }

                unsafe { shuffle_by_indices_bitvec(&mut values, &mut diffs, &mut expected_indices, &mut visited) };
                prop_assert_eq!(values, output_values);
                prop_assert_eq!(diffs, output_diffs);
            }
        }
    }
}
