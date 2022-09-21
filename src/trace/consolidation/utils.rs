use crate::utils::assume;
use bitvec::slice::BitSlice;
use std::{mem::forget, ptr};

/// An implementation of `Vec::dedup_by()` that takes a starting point
///
/// Modified from [the stdlib](https://doc.rust-lang.org/std/vec/struct.Vec.html#method.dedup_by)
pub(super) fn dedup_starting_at<T, F>(vec: &mut Vec<T>, starting_point: usize, mut same_bucket: F)
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
pub(super) fn retain_starting_at<T, F>(vec: &mut Vec<T>, starting_point: usize, mut retain: F)
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
pub unsafe fn shuffle_by_indices<T, R>(values: &mut [T], diffs: &mut [R], indices: &mut [usize]) {
    if cfg!(debug_assertions) {
        assert_shuffle_preconditions(values, diffs, indices);
    }

    assume(
        values.len() == diffs.len()
            && values.len() == indices.len()
            && diffs.len() == indices.len(),
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
    if cfg!(debug_assertions) {
        assert_shuffle_preconditions(values, diffs, indices);
    }

    assume(
        values.len() == diffs.len()
            && values.len() == indices.len()
            && diffs.len() == indices.len()
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

fn assert_shuffle_preconditions<T, R>(values: &[T], diffs: &[R], indices: &[usize]) {
    // All vectors should have the same length
    debug_assert_eq!(values.len(), diffs.len());
    debug_assert_eq!(values.len(), indices.len());
    debug_assert_eq!(diffs.len(), indices.len());

    for &index in &indices[..values.len()] {
        // Indices should be valid indices
        debug_assert!(index < diffs.len());
        debug_assert!(index < values.len());
        debug_assert!(index < indices.len());

        // All indices are unique
        debug_assert_eq!(indices.iter().filter(|&&idx| idx == index).count(), 1);
    }
}
