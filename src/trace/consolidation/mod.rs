//! Common logic for the consolidation of vectors of MonoidValues.
//!
//! Often we find ourselves with collections of records with associated weights
//! (often integers) where we want to reduce the collection to the point that
//! each record occurs at most once, with the accumulated weights. These methods
//! supply that functionality.

mod fill_indices;
mod tests;

// Public for benchmarks
// FIXME: Add a benchmarking feature
#[doc(hidden)]
pub mod utils;

use crate::algebra::{AddAssignByRef, HasZero, MonoidValue};
use std::{mem::replace, ops::AddAssign, ptr};
use utils::{dedup_starting_at, retain_starting_at, shuffle_by_indices};

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
    if vec.is_empty() {
        return;
    }

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
    if vec[offset..].is_empty() {
        return;
    }

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
    if keys[offset..].is_empty() {
        return;
    }

    // Clear and pre-allocate the indices buffer
    fill_indices::fill_indices(keys.len() - offset, indices);
    debug_assert_eq!(indices.len(), keys.len() - offset);

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
    if slice.is_empty() {
        return 0;
    }

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
    if keys.is_empty() {
        return 0;
    }

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
    unsafe { shuffle_by_indices(keys, diffs, indices) };

    valid_prefix
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
