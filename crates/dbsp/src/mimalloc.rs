use mimalloc_rust_sys::{
    aligned_allocation::{mi_malloc_aligned, mi_realloc_aligned, mi_zalloc_aligned},
    basic_allocation::{mi_free, mi_malloc, mi_realloc, mi_zalloc},
    extended_functions::{mi_process_info, mi_stats_reset},
};
use serde::Serialize;
use std::alloc::{GlobalAlloc, Layout};

/// `MI_MAX_ALIGN_SIZE` is 16 unless manually overridden:
/// https://github.com/microsoft/mimalloc/blob/15220c68/include/mimalloc-types.h#L22
const MI_MAX_ALIGN_SIZE: usize = 16;

/// Allocation and process statistics
#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct AllocStats {
    pub elapsed_ms: usize,
    pub user_ms: usize,
    pub system_ms: usize,
    pub current_rss: usize,
    pub peak_rss: usize,
    pub current_commit: usize,
    pub peak_commit: usize,
    pub page_faults: usize,
}

/// A [`GlobalAlloc`] implementation that uses `mimalloc`
#[derive(Debug, Clone, Copy, Default)]
pub struct MiMalloc;

// The dead code lint triggers if the function is unused within any benchmark,
// not if it's unused in all of them
#[allow(dead_code)]
impl MiMalloc {
    /// Collect allocation stats
    pub fn stats(&self) -> AllocStats {
        let mut stats = AllocStats::default();
        unsafe {
            mi_process_info(
                &mut stats.elapsed_ms,
                &mut stats.user_ms,
                &mut stats.system_ms,
                &mut stats.current_rss,
                &mut stats.peak_rss,
                &mut stats.current_commit,
                &mut stats.peak_commit,
                &mut stats.page_faults,
            );
        }

        stats
    }

    /// Reset allocation statistics
    pub fn reset_stats(&self) {
        unsafe { mi_stats_reset() };
    }
}

// Using the normal (unaligned) mimalloc apis is apparently marginally faster
// and better supported than using the aligned api for everything, so we do a
// check to see if the allocation falls within the range where it's better to
// use the unaligned api
//
// See https://github.com/microsoft/mimalloc/issues/314
unsafe impl GlobalAlloc for MiMalloc {
    #[inline]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if use_unaligned_api(layout.size(), layout.align()) {
            mi_malloc(layout.size())
        } else {
            mi_malloc_aligned(layout.size(), layout.align())
        }
        .cast()
    }

    #[inline]
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if use_unaligned_api(layout.size(), layout.align()) {
            mi_zalloc(layout.size())
        } else {
            mi_zalloc_aligned(layout.size(), layout.align())
        }
        .cast()
    }

    #[inline]
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let ptr = ptr.cast();
        if use_unaligned_api(layout.size(), layout.align()) {
            mi_realloc(ptr, new_size)
        } else {
            mi_realloc_aligned(ptr, new_size, layout.align())
        }
        .cast()
    }

    #[inline]
    unsafe fn dealloc(&self, ptr: *mut u8, _layout: Layout) {
        // Deallocation doesn't care about alignment which is nice
        mi_free(ptr.cast());
    }
}

#[inline(always)]
const fn use_unaligned_api(size: usize, alignment: usize) -> bool {
    // This logic is based on the discussion [here]. We don't bother with the
    // 3rd suggested test due to it being high cost (calling `mi_good_size`)
    // compared to the other checks, and also feeling like it relies on too much
    // implementation-specific behavior.
    //
    // [here]: https://github.com/microsoft/mimalloc/issues/314#issuecomment-708541845
    (alignment <= MI_MAX_ALIGN_SIZE && size >= alignment)
        || (alignment == size && alignment <= 4096)
}
