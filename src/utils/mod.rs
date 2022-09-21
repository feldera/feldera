mod vec_ext;

pub(crate) use vec_ext::VecExt;

use std::hint::unreachable_unchecked;

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
