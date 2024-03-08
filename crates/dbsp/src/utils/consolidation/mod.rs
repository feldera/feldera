//! Common logic for the consolidation of vectors of MonoidValues.
//!
//! Often we find ourselves with collections of records with associated weights
//! (often integers) where we want to reduce the collection to the point that
//! each record occurs at most once, with the accumulated weights. These methods
//! supply that functionality.

mod quicksort;
mod tests;

// Public for benchmarks
// FIXME: Add a benchmarking feature
#[doc(hidden)]
pub mod utils;

use crate::{
    algebra::{AddAssignByRef, HasZero, MonoidValue},
    dynamic::{DataTrait, DowncastTrait, DynVec, Erase, LeanVec, WeightTrait},
    utils::{assume, unstable_sort_by, Tup2},
    DBData, DBWeight,
};
use std::{
    marker::PhantomData,
    mem::{replace, size_of},
    ops::AddAssign,
    ptr,
};
use utils::{dedup_payload_starting_at, retain_payload_starting_at};

#[cfg(test)]
pub fn consolidate_pairs<T, R>(vec: &mut LeanVec<(T, R)>)
where
    T: Ord,
    R: MonoidValue,
{
    if vec.is_empty() {
        return;
    }

    unstable_sort_by(vec.as_mut_slice(), |(key1, _), (key2, _)| key1.cmp(key2));
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

/// Sorts and consolidates `vec`.
///
/// This method will sort `vec` and then consolidate runs of more than one entry
/// with identical first elements by accumulating the second elements of the
/// pairs. Should the final accumulation be zero, the element is discarded.
pub fn consolidate<T, R>(vec: &mut LeanVec<Tup2<T, R>>)
where
    T: Ord,
    R: MonoidValue,
{
    if vec.is_empty() {
        return;
    }

    unstable_sort_by(vec.as_mut_slice(), |Tup2(key1, _), Tup2(key2, _)| {
        key1.cmp(key2)
    });
    // TODO: Combine the `.dedup_by()` and `.retain()` calls together
    vec.dedup_by(|Tup2(key1, data1), Tup2(key2, data2)| {
        if key1 == key2 {
            data2.add_assign(replace(data1, R::zero()));
            true
        } else {
            false
        }
    });
    vec.retain(|Tup2(_, data)| !data.is_zero());
}

/// Sorts and consolidate `vec[offset..]`.
///
/// This method will sort `vec[offset..]` and then consolidate runs of more than
/// one entry with identical first elements by accumulating the second elements
/// of the pairs. Should the final accumulation be zero, the element is
/// discarded.
pub fn consolidate_from<T, R>(vec: &mut LeanVec<Tup2<T, R>>, offset: usize)
where
    T: Ord,
    R: HasZero + AddAssign,
{
    if vec[offset..].is_empty() {
        return;
    }

    unstable_sort_by(&mut vec[offset..], |Tup2(key1, _), Tup2(key2, _)| {
        key1.cmp(key2)
    });
    vec.dedup_by_starting_at(offset, |Tup2(key1, data1), Tup2(key2, data2)| {
        if key1 == key2 {
            data2.add_assign(replace(data1, R::zero()));
            true
        } else {
            false
        }
    });
    vec.retain_starting_at(offset, |Tup2(_, data)| !data.is_zero());
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
pub fn consolidate_slice<T, R>(slice: &mut [Tup2<T, R>]) -> usize
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
    unstable_sort_by(slice, |Tup2(key1, _), Tup2(key2, _)| key1.cmp(key2));
    consolidate_slice_inner(
        slice,
        |Tup2(key1, _), Tup2(key2, _)| key1 == key2,
        |Tup2(_, diff1), Tup2(_, diff2)| diff1.add_assign_by_ref(diff2),
        |Tup2(_, diff)| diff.is_zero(),
    )
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

/// Sorts and consolidate `vec[offset..]`.
///
/// This method will sort `vec[offset..]` and then consolidate runs of more than
/// one entry with identical first elements by accumulating the second elements
/// of the pairs. Should the final accumulation be zero, the element is
/// discarded.
pub fn consolidate_payload_from<K, R>(keys: &mut Vec<K>, diffs: &mut Vec<R>, offset: usize)
where
    K: Ord,
    R: HasZero + AddAssign,
{
    // Ensure that the paired slices are the same length
    assert_eq!(keys.len(), diffs.len());
    if keys[offset..].is_empty() {
        return;
    }

    // Ideally we'd combine the sorting and value merging portions
    // This line right here is literally the hottest code within the entirety of the
    // program. It makes up 90% of the work done while joining or merging anything
    quicksort::quicksort(&mut keys[offset..], &mut diffs[offset..]);

    // Deduplicate all difference values
    dedup_payload_starting_at(keys, &mut *diffs, offset, |key1, diff1, key2, diff2| {
        if key1 == key2 {
            diff2.add_assign(replace(diff1, R::zero()));
            true
        } else {
            false
        }
    });

    // Remove any keys with zeroed diffs
    retain_payload_starting_at(keys, diffs, offset, |_key, diff| !diff.is_zero());
}

pub fn consolidate_paired_slices<K, R>(keys: &mut [K], diffs: &mut [R]) -> usize
where
    K: Ord,
    R: AddAssignByRef + HasZero,
{
    // Ensure that the paired slices are the same length
    assert_eq!(keys.len(), diffs.len());
    if keys.is_empty() {
        return 0;
    }

    // Ideally we'd combine the sorting and value merging portions
    // These lines right here are literally the hottest code within the entirety of
    // the program. They make up 90% of the work done while joining or merging
    // anything
    quicksort::quicksort(keys, diffs);

    // Safety: the keys & diffs slices are the same length and are non-empty
    unsafe { compact_paired_slices(keys, diffs) }
}

/// Compacts already-sorted values and their diffs, returning the compacted
/// prefix length
///
/// # Safety
///
/// - `keys` and `diffs` must have the same length
/// - `keys` and `diffs` must both be non-empty
unsafe fn compact_paired_slices<T, R>(keys: &mut [T], diffs: &mut [R]) -> usize
where
    T: Eq,
    R: AddAssignByRef + HasZero,
{
    unsafe {
        assume(!keys.is_empty());
        assume(keys.len() == diffs.len());
    }

    // If the key type is a zst then all keys are identical, so we sum up all diffs
    if size_of::<T>() == 0 {
        debug_assert!(!diffs.is_empty());
        let (sum, diffs) = diffs.split_at_mut(1);
        debug_assert_eq!(sum.len(), 1);
        let sum = &mut sum[0];

        // Add all diffs to the first diff in the slice
        for diff in diffs {
            sum.add_assign_by_ref(diff);
        }

        // If the diff that contains the sum of all diffs is zero, return 0.
        // Otherwise if it's non-zero, return 1 as our prefix length
        return !sum.is_zero() as usize;

    // If the diff type is a zst we check if the diff is always zero or always
    // non-zero
    } else if size_of::<R>() == 0 {
        return !diffs[0].is_zero() as usize;
    }

    let len = keys.len();
    let key_ptr = keys.as_mut_ptr();
    let diff_ptr = diffs.as_mut_ptr();

    // Counts the number of distinct known-non-zero accumulations. Indexes the write
    // location.
    let mut offset = 0;
    for index in 1..len {
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
            assume(offset < index);
            assume(index < len);
            assume(offset < len);

            // LOOP INVARIANT: offset < index
            let key1 = key_ptr.add(offset);
            let key2 = key_ptr.add(index);
            let diff1 = diff_ptr.add(offset);
            let diff2 = diff_ptr.add(index);

            // If the values are equal, merge them
            if *key1 == *key2 {
                (*diff1).add_assign_by_ref(&*diff2);

            // Otherwise continue
            } else {
                if !(*diff1).is_zero() {
                    offset += 1;
                }

                debug_assert!(offset < len);
                ptr::swap(key_ptr.add(offset), key2);
                ptr::swap(diff_ptr.add(offset), diff2);
            }
        }
    }

    if offset < len && unsafe { !(*diff_ptr.add(offset)).is_zero() } {
        offset += 1;
    }

    offset
}

pub trait ConsolidatePairedSlices<T1: DataTrait + ?Sized, T2: WeightTrait + ?Sized> {
    fn consolidate_paired_slices(
        &self,
        keys: (&mut DynVec<T1>, usize, usize),
        weights: (&mut DynVec<T2>, usize, usize),
    ) -> usize;
}

pub struct ConsolidatePairedSlicesImpl<T1Type, T2Type, T1, T2>
where
    T1: DataTrait + ?Sized,
    T2: WeightTrait + ?Sized,
    T1Type: DBData + Erase<T1>,
    T2Type: DBWeight + Erase<T2>,
{
    phantom: PhantomData<fn(&T1Type, &T2Type, &T1, &T2)>,
}

impl<T1Type, T2Type, T1, T2> ConsolidatePairedSlices<T1, T2>
    for ConsolidatePairedSlicesImpl<T1Type, T2Type, T1, T2>
where
    T1: DataTrait + ?Sized,
    T2: WeightTrait + ?Sized,
    T1Type: DBData + Erase<T1>,
    T2Type: DBWeight + Erase<T2>,
{
    fn consolidate_paired_slices(
        &self,
        (keys, from1, to1): (&mut DynVec<T1>, usize, usize),
        (weights, from2, to2): (&mut DynVec<T2>, usize, usize),
    ) -> usize {
        let keys: &mut LeanVec<T1Type> = unsafe { keys.downcast_mut::<LeanVec<T1Type>>() };
        let weights: &mut LeanVec<T2Type> = unsafe { weights.downcast_mut::<LeanVec<T2Type>>() };

        consolidate_paired_slices(&mut keys[from1..to1], &mut weights[from2..to2])
    }
}

impl<T1, T2> dyn ConsolidatePairedSlices<T1, T2>
where
    T1: DataTrait + ?Sized,
    T2: WeightTrait + ?Sized,
{
    pub const fn factory<T1Type: DBData + Erase<T1>, T2Type: DBWeight + Erase<T2>>() -> &'static Self
    {
        &ConsolidatePairedSlicesImpl::<T1Type, T2Type, T1, T2> {
            phantom: PhantomData,
        }
    }
}
