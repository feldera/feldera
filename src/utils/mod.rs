pub(crate) mod tests;
mod vec_ext;

pub(crate) use vec_ext::VecExt;

use std::{
    hint::unreachable_unchecked,
    mem::{ManuallyDrop, MaybeUninit},
};

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

// FIXME: Replace with `usize::next_multiple_of()` via rust/#88581
#[inline]
pub(crate) const fn next_multiple_of(n: usize, rhs: usize) -> usize {
    match n % rhs {
        0 => n,
        r => n + (rhs - r),
    }
}

#[cold]
#[inline(never)]
pub(crate) fn cursor_position_oob(position: usize, length: usize) -> ! {
    panic!("the cursor was at the invalid position {position} while the leaf was only {length} elements long")
}

/// Casts a `Vec<T>` into a `Vec<MaybeUninit<T>>`
#[inline]
pub(crate) fn cast_uninit_vec<T>(vec: Vec<T>) -> Vec<MaybeUninit<T>> {
    // Make sure we don't drop the old vec
    let mut vec = ManuallyDrop::new(vec);

    // Get the length, capacity and pointer of the vec (we get the pointer last as a
    // rather nitpicky thing irt stacked borrows since the `.len()` and
    // `.capacity()` calls technically reborrow the vec). Ideally we'd use
    // `Vec::into_raw_parts()` but it's currently unstable via rust/#65816
    let (len, cap, ptr) = (vec.len(), vec.capacity(), vec.as_mut_ptr());

    // Create a new vec with the different type
    unsafe { Vec::from_raw_parts(ptr.cast::<MaybeUninit<T>>(), len, cap) }
}
