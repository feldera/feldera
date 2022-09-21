// TODO: More arch support (mainly arm and aarch64, maybe even avx512)

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

    let mut index = i32x4(0, 1, 2, 3);
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
