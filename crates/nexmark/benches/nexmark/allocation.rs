use mimalloc_rust_sys::{
    aligned_allocation::{mi_malloc_aligned, mi_realloc_aligned, mi_zalloc_aligned},
    basic_allocation::mi_free,
    extended_functions::mi_process_info,
};
use std::alloc::{GlobalAlloc, Layout};

pub struct MiMalloc;

impl MiMalloc {
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
}

unsafe impl GlobalAlloc for MiMalloc {
    #[inline]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        mi_malloc_aligned(layout.size(), layout.align()).cast()
    }

    #[inline]
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        mi_zalloc_aligned(layout.size(), layout.align()).cast()
    }

    #[inline]
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        mi_realloc_aligned(ptr.cast(), new_size, layout.align()).cast()
    }

    #[inline]
    unsafe fn dealloc(&self, ptr: *mut u8, _layout: Layout) {
        mi_free(ptr.cast());
    }
}

#[derive(Debug, Default)]
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
