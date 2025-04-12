mod advance_retreat;
mod sample;
//pub(crate) mod tests;
mod consolidation;
mod sort;
mod tuple;

#[cfg(test)]
mod vec_ext;

#[cfg(test)]
mod dot;

use std::cmp::Ordering;
use std::fmt::Debug;
use std::{fmt::Display, hint::unreachable_unchecked};

pub use advance_retreat::{
    advance, advance_erased, dyn_advance, dyn_retreat, retreat, retreat_erased,
};
pub use consolidation::{
    consolidate, consolidate_from, consolidate_paired_slices, consolidate_payload_from,
    consolidate_slice, ConsolidatePairedSlices,
};

#[cfg(test)]
pub use dot::{DotEdgeAttributes, DotNodeAttributes};

#[cfg(test)]
pub use consolidation::consolidate_pairs;

pub use sample::sample_slice;
pub use sort::{stable_sort, stable_sort_by};
pub use tuple::{
    ArchivedTup0, ArchivedTup1, ArchivedTup10, ArchivedTup2, ArchivedTup3, ArchivedTup4,
    ArchivedTup5, ArchivedTup6, ArchivedTup7, ArchivedTup8, ArchivedTup9, Tup0, Tup1, Tup10, Tup2,
    Tup3, Tup4, Tup5, Tup6, Tup7, Tup8, Tup9,
};

// mod unstable_sort;

// FIXME: unstable sort implementation is currently broken; use stable sorting instead.
pub fn unstable_sort<T: Ord + Debug>(slice: &mut [T]) {
    stable_sort(slice)
}

// FIXME: unstable sort implementation is currently broken; use stable sorting instead.
pub fn unstable_sort_by<T, F>(slice: &mut [T], cmp: F)
where
    F: Fn(&T, &T) -> Ordering,
{
    stable_sort_by(slice, cmp)
}

#[cfg(test)]
pub(crate) use vec_ext::VecExt;

/// Tells the optimizer that a condition is always true
///
/// # Safety
///
/// It's UB to call this function with `false` as the condition
#[inline(always)]
#[deny(unsafe_op_in_unsafe_fn)]
#[cfg_attr(debug_assertions, track_caller)]
pub(crate) unsafe fn assume(cond: bool) {
    debug_assert!(cond, "called `assume()` on a false condition");

    if !cond {
        // Safety: It's UB for `cond` to be false
        unsafe { unreachable_unchecked() };
    }
}

#[cold]
#[inline(never)]
pub(crate) fn cursor_position_oob<P: Display>(position: P, length: usize) -> ! {
    panic!("the cursor was at the invalid position {position} while the leaf was only {length} elements long")
}

#[inline]
pub(crate) fn bytes_of<T>(slice: &[T]) -> &[std::mem::MaybeUninit<u8>] {
    // Safety: It's always sound to interpret possibly uninitialized bytes as
    // `MaybeUninit<u8>`
    unsafe { std::slice::from_raw_parts(slice.as_ptr().cast(), std::mem::size_of_val(slice)) }
}
