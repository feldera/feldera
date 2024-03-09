mod dyn_vec;
mod sample;
pub(crate) mod tests;
mod tuple;
mod vec_ext;

pub use dyn_vec::{DynIter, DynVec, DynVecVTable};

pub use sample::sample_slice;
pub use tuple::{Tup0, Tup1, Tup10, Tup2, Tup3, Tup4, Tup5, Tup6, Tup7, Tup8, Tup9};
pub(crate) use vec_ext::VecExt;

use std::{
    fmt::Display,
    hint::unreachable_unchecked,
    mem::{forget, ManuallyDrop, MaybeUninit},
    ptr,
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

#[cold]
#[inline(never)]
pub(crate) fn cursor_position_oob<P: Display>(position: P, length: usize) -> ! {
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

/// Creates a `Vec<MaybeUninit<T>>` with the given length
#[inline]
pub(crate) fn uninit_vec<T>(length: usize) -> Vec<MaybeUninit<T>> {
    let mut buf: Vec<MaybeUninit<T>> = Vec::with_capacity(length);
    // Safety: `buf` is initialized with uninitalized elements up to `length`
    unsafe { buf.set_len(length) };
    buf
}

// TODO: Replace with `MaybeUninit::write_slice_cloned()` via rust/#79995
#[inline]
pub(crate) fn write_uninit_slice_cloned<'a, T>(
    this: &'a mut [MaybeUninit<T>],
    src: &[T],
) -> &'a mut [T]
where
    T: Clone,
{
    // unlike copy_from_slice this does not call clone_from_slice on the slice
    // this is because `MaybeUninit<T: Clone>` does not implement Clone.

    struct Guard<'a, T> {
        slice: &'a mut [MaybeUninit<T>],
        initialized: usize,
    }

    impl<'a, T> Drop for Guard<'a, T> {
        fn drop(&mut self) {
            let initialized_part = &mut self.slice[..self.initialized];
            // SAFETY: this raw slice will contain only initialized objects
            // that's why, it is allowed to drop it.
            unsafe {
                ptr::drop_in_place(&mut *(initialized_part as *mut [MaybeUninit<T>] as *mut [T]));
            }
        }
    }

    assert_eq!(
        this.len(),
        src.len(),
        "destination and source slices have different lengths"
    );
    // NOTE: We need to explicitly slice them to the same length
    // for bounds checking to be elided, and the optimizer will
    // generate memcpy for simple cases (for example T = u8).
    let len = this.len();
    let src = &src[..len];

    // guard is needed b/c panic might happen during a clone
    let mut guard = Guard {
        slice: this,
        initialized: 0,
    };

    #[allow(clippy::needless_range_loop)]
    for i in 0..len {
        guard.slice[i].write(src[i].clone());
        guard.initialized += 1;
    }

    forget(guard);

    // SAFETY: Valid elements have just been written into `this` so it is
    // initialized
    unsafe { &mut *(this as *mut [MaybeUninit<T>] as *mut [T]) }
}

#[inline]
#[cfg(test)]
pub(crate) fn bytes_of<T>(slice: &[T]) -> &[MaybeUninit<u8>] {
    // Safety: It's always sound to interpret possibly uninitialized bytes as
    // `MaybeUninit<u8>`
    unsafe { std::slice::from_raw_parts(slice.as_ptr().cast(), std::mem::size_of_val(slice)) }
}
