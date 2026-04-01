//! Slab-backed allocation and recycling for [`FBuf`].
//!
//! This module keeps a small set of per-size-class freelists for power-of-two
//! `FBuf` capacities. Allocations that do not fit a slab size class fall back
//! to direct allocation and deallocation.

use std::{
    alloc,
    array::from_fn,
    cell::RefCell,
    ops::{Add, AddAssign},
    ptr::NonNull,
    sync::{
        Arc, LazyLock,
        atomic::{AtomicU64, Ordering as AtomicOrdering},
    },
};

use crossbeam::queue::ArrayQueue;

use super::FBuf;

thread_local! {
    /// Per-thread override for the active `FBuf` slab set.
    ///
    /// When unset, allocation and recycle operations fall back to the
    /// process-wide slab set in [`FALLBACK_SLAB_POOL`].
    static THREAD_SLAB_POOL: RefCell<Option<Arc<FBufSlabs>>> = const { RefCell::new(None) };
}

tokio::task_local! {
    /// Per-task override for the active `FBuf` slab set.
    ///
    /// This is used by async merger tasks, which can move across worker
    /// threads and therefore cannot rely on thread-local slab state.
    pub static TOKIO_FBUF_SLABS: Arc<FBufSlabs>;
}

/// Process-wide fallback `FBuf` slab set used when a thread has not installed
/// its own slab set via [`set_thread_slab_pool`].
///
/// This is for stuff that doesn't use a runtime. If we get it wrong
/// somehow, we'll know because we will probably contend on this horribly.
static FALLBACK_SLAB_POOL: LazyLock<Arc<FBufSlabs>> =
    LazyLock::new(|| Arc::new(FBufSlabs::default()));

// Locking in some constraints for my sanity
const _: () = {
    assert!(FBuf::ALIGNMENT.is_power_of_two());
    assert!(FBufSlabs::HIGHEST_SLAB_CLASS_CAPACITY.is_power_of_two());
    assert!(FBufSlabs::HIGHEST_SLAB_CLASS_CAPACITY >= FBuf::ALIGNMENT);
};

/// One power-of-two slab class that caches recycled `FBuf` allocations of a
/// single capacity.
#[derive(Debug)]
struct FBufSlab {
    /// Fixed-capacity free-list queue for recycled raw buffer pointers.
    buffers: ArrayQueue<CachedAllocation>,
    /// Counters for requests and hits observed by this slab class.
    stats: FBufSlabStats,
}

/// A recycled `FBuf` allocation stored in a slab free list.
#[derive(Clone, Copy, Debug)]
struct CachedAllocation {
    /// Pointer to an owned heap allocation that can be reused for a future
    /// `FBuf`.
    ptr: NonNull<u8>,
}

// SAFETY: These pointers represent owned heap allocations. Access is
// synchronized by the enclosing `ArrayQueue`, and moving the pointer value
// itself across threads does not transfer aliasing guarantees beyond raw
// ownership.
unsafe impl Send for CachedAllocation {}

/// Mutable counters for requests and hits recorded by a single slab class.
#[derive(Debug, Default)]
struct FBufSlabStats {
    /// Total allocation attempts routed to this slab class.
    alloc_requests: AtomicU64,
    /// Allocation attempts satisfied from a cached buffer.
    alloc_hits: AtomicU64,
    /// Total recycle attempts routed to this slab class.
    recycle_requests: AtomicU64,
    /// Recycle attempts that successfully returned a buffer to the cache.
    recycle_hits: AtomicU64,
}

impl FBufSlabStats {
    fn snapshot(&self, size: usize, available_buffers: usize) -> FBufSlabStatsSnapshot {
        FBufSlabStatsSnapshot {
            size,
            available_buffers,
            alloc_requests: self.alloc_requests.load(AtomicOrdering::Relaxed),
            alloc_hits: self.alloc_hits.load(AtomicOrdering::Relaxed),
            recycle_requests: self.recycle_requests.load(AtomicOrdering::Relaxed),
            recycle_hits: self.recycle_hits.load(AtomicOrdering::Relaxed),
        }
    }
}

/// Snapshot of one slab size class.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FBufSlabStatsSnapshot {
    /// Buffer capacity in bytes for this slab class.
    pub size: usize,
    /// Number of available buffers currently retained in this slab class.
    pub available_buffers: usize,
    /// Total allocation requests routed to this slab class.
    pub alloc_requests: u64,
    /// Allocation requests served from cached buffers in this slab class.
    pub alloc_hits: u64,
    /// Total recycle requests routed to this slab class.
    pub recycle_requests: u64,
    /// Recycle requests accepted into this slab class.
    pub recycle_hits: u64,
}

impl FBufSlabStatsSnapshot {
    /// Returns the number of allocation requests that missed this slab class
    /// and fell back to direct allocation.
    pub fn malloc_fallbacks(&self) -> u64 {
        self.alloc_requests.saturating_sub(self.alloc_hits)
    }

    /// Returns the number of recycle requests that missed this slab class and
    /// fell back to direct deallocation.
    pub fn free_fallbacks(&self) -> u64 {
        self.recycle_requests.saturating_sub(self.recycle_hits)
    }
}

/// Snapshot of an [`FBufSlabs`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FBufSlabsStats {
    /// Number of cached buffers currently retained across all slab classes.
    pub cached_buffers: usize,
    /// Allocation requests that bypassed slab reuse because the requested
    /// capacity does not map to any slab size class.
    pub fallback_alloc_requests: u64,
    /// Recycle requests that bypassed slab reuse because the buffer capacity
    /// does not map to any slab size class.
    pub fallback_recycle_requests: u64,
    /// Per-size-class request counters and free-list occupancy information.
    pub classes: Vec<FBufSlabStatsSnapshot>,
}

impl FBufSlabsStats {
    /// Returns the total number of allocation requests observed across the
    /// slab set, including fallback-only requests.
    pub fn alloc_requests(&self) -> u64 {
        self.fallback_alloc_requests
            + self
                .classes
                .iter()
                .map(|class| class.alloc_requests)
                .sum::<u64>()
    }

    /// Returns the total number of recycle requests observed across the slab
    /// set, including fallback-only requests.
    pub fn recycle_requests(&self) -> u64 {
        self.fallback_recycle_requests
            + self
                .classes
                .iter()
                .map(|class| class.recycle_requests)
                .sum::<u64>()
    }

    /// Returns the number of allocations that reused an already cached buffer.
    pub fn mallocs_saved(&self) -> u64 {
        self.classes.iter().map(|class| class.alloc_hits).sum()
    }

    /// Returns the number of allocation requests that fell back to direct
    /// allocation, either because no cached buffer was available or because
    /// the request was outside the slabbed range.
    pub fn malloc_fallbacks(&self) -> u64 {
        self.fallback_alloc_requests
            + self
                .classes
                .iter()
                .map(FBufSlabStatsSnapshot::malloc_fallbacks)
                .sum::<u64>()
    }

    /// Returns the number of frees that were converted into slab reuse by
    /// caching the buffer instead of immediately deallocating it.
    pub fn frees_saved(&self) -> u64 {
        self.classes.iter().map(|class| class.recycle_hits).sum()
    }

    /// Returns the number of recycle requests that fell back to direct
    /// deallocation, either because a slab class was full or because the
    /// buffer capacity was outside the slabbed range.
    pub fn free_fallbacks(&self) -> u64 {
        self.fallback_recycle_requests
            + self
                .classes
                .iter()
                .map(FBufSlabStatsSnapshot::free_fallbacks)
                .sum::<u64>()
    }
}

impl Add for FBufSlabsStats {
    type Output = Self;

    fn add(mut self, rhs: Self) -> Self::Output {
        self += rhs;
        self
    }
}

impl AddAssign for FBufSlabsStats {
    fn add_assign(&mut self, rhs: Self) {
        self.cached_buffers += rhs.cached_buffers;
        self.fallback_alloc_requests += rhs.fallback_alloc_requests;
        self.fallback_recycle_requests += rhs.fallback_recycle_requests;

        if self.classes.is_empty() {
            self.classes = rhs.classes;
            return;
        }
        if rhs.classes.is_empty() {
            return;
        }

        assert_eq!(self.classes.len(), rhs.classes.len());
        for (lhs, rhs) in self.classes.iter_mut().zip(rhs.classes) {
            assert_eq!(lhs.size, rhs.size);
            lhs.available_buffers += rhs.available_buffers;
            lhs.alloc_requests += rhs.alloc_requests;
            lhs.alloc_hits += rhs.alloc_hits;
            lhs.recycle_requests += rhs.recycle_requests;
            lhs.recycle_hits += rhs.recycle_hits;
        }
    }
}

/// Power-of-two free lists for recycling [`FBuf`] allocations.
#[derive(Debug)]
pub struct FBufSlabs {
    /// Slab classes indexed by capacity bucket.
    slabs: [FBufSlab; Self::NUM_CLASSES],
    /// Allocation requests that never entered a slab class because the
    /// requested capacity was not slab-eligible.
    fallback_alloc_requests: AtomicU64,
    /// Recycle requests that never entered a slab class because the buffer
    /// capacity was not slab-eligible.
    fallback_recycle_requests: AtomicU64,
}

impl FBufSlabs {
    /// Largest allocation capacity that maps to a dedicated slab size class.
    /// Requests above this bypass slab reuse and fall back to malloc/free.
    pub const HIGHEST_SLAB_CLASS_CAPACITY: usize = 64 * 1024;

    /// Default byte budget retained per slab size class when callers do not
    /// supply an explicit setting.
    pub const DEFAULT_BYTES_PER_CLASS: usize = 16 * 1024 * 1024;

    const MIN_SHIFT: u32 = FBuf::ALIGNMENT.trailing_zeros();
    const MAX_SHIFT: u32 = Self::HIGHEST_SLAB_CLASS_CAPACITY.trailing_zeros();
    const NUM_CLASSES: usize = (Self::MAX_SHIFT - Self::MIN_SHIFT + 1) as usize;

    fn slab_size(index: usize) -> usize {
        1usize << (Self::MIN_SHIFT as usize + index)
    }

    /// Returns a stable identifier for this slab allocator instance.
    pub fn backend_id(&self) -> usize {
        self as *const Self as usize
    }

    /// Creates a new slab set with a fixed byte budget per size class.
    ///
    /// The effective cached buffer count for each class is
    /// `max(bytes_per_class, class_size) / class_size`, which guarantees that
    /// every slab class can cache at least one buffer.
    pub fn new(bytes_per_class: usize) -> Self {
        Self {
            slabs: from_fn(|index| {
                let size = Self::slab_size(index);
                let max_buffers = bytes_per_class.max(size) / size;
                FBufSlab {
                    buffers: ArrayQueue::new(max_buffers),
                    stats: FBufSlabStats::default(),
                }
            }),
            fallback_alloc_requests: AtomicU64::new(0),
            fallback_recycle_requests: AtomicU64::new(0),
        }
    }

    fn try_take(&self, requested_capacity: usize) -> Option<(NonNull<u8>, usize)> {
        let Some(index) = Self::request_index(requested_capacity) else {
            self.fallback_alloc_requests
                .fetch_add(1, AtomicOrdering::Relaxed);
            return None;
        };
        let slab = &self.slabs[index];
        let size = Self::slab_size(index);
        slab.stats
            .alloc_requests
            .fetch_add(1, AtomicOrdering::Relaxed);

        let ptr = slab.buffers.pop();
        if ptr.is_some() {
            slab.stats.alloc_hits.fetch_add(1, AtomicOrdering::Relaxed);
        }
        ptr.map(|ptr| (ptr.ptr, size))
    }

    fn try_put(&self, ptr: NonNull<u8>, capacity: usize) -> bool {
        let slab = match Self::capacity_index(capacity) {
            Some(index) => &self.slabs[index],
            None => {
                self.fallback_recycle_requests
                    .fetch_add(1, AtomicOrdering::Relaxed);
                return false;
            }
        };
        slab.stats
            .recycle_requests
            .fetch_add(1, AtomicOrdering::Relaxed);
        if slab.buffers.push(CachedAllocation { ptr }).is_err() {
            return false;
        }
        slab.stats
            .recycle_hits
            .fetch_add(1, AtomicOrdering::Relaxed);
        true
    }

    /// Returns a snapshot of slab occupancy and request counters for this slab
    /// set.
    pub fn stats(&self) -> FBufSlabsStats {
        let classes = self
            .slabs
            .iter()
            .enumerate()
            .map(|(index, slab)| {
                let size = Self::slab_size(index);
                let available_buffers = slab.buffers.len();
                slab.stats.snapshot(size, available_buffers)
            })
            .collect::<Vec<_>>();

        FBufSlabsStats {
            cached_buffers: classes.iter().map(|class| class.available_buffers).sum(),
            fallback_alloc_requests: self.fallback_alloc_requests.load(AtomicOrdering::Relaxed),
            fallback_recycle_requests: self.fallback_recycle_requests.load(AtomicOrdering::Relaxed),
            classes,
        }
    }

    fn request_index(requested_capacity: usize) -> Option<usize> {
        if requested_capacity == 0 || requested_capacity > Self::HIGHEST_SLAB_CLASS_CAPACITY {
            return None;
        }
        let rounded = requested_capacity.max(FBuf::ALIGNMENT).next_power_of_two();
        Some((rounded.trailing_zeros() - Self::MIN_SHIFT) as usize)
    }

    fn capacity_index(capacity: usize) -> Option<usize> {
        if !(FBuf::ALIGNMENT..=Self::HIGHEST_SLAB_CLASS_CAPACITY).contains(&capacity)
            || !capacity.is_power_of_two()
        {
            return None;
        }
        Some((capacity.trailing_zeros() - Self::MIN_SHIFT) as usize)
    }

    #[cfg(test)]
    fn cached_buffers(&self, capacity: usize) -> usize {
        Self::capacity_index(capacity)
            .map(|index| self.slabs[index].buffers.len())
            .unwrap_or_default()
    }
}

impl Default for FBufSlabs {
    fn default() -> Self {
        Self::new(Self::DEFAULT_BYTES_PER_CLASS)
    }
}

impl Drop for FBufSlabs {
    fn drop(&mut self) {
        for (index, slab) in self.slabs.iter_mut().enumerate() {
            let size = Self::slab_size(index);
            while let Some(ptr) = slab.buffers.pop() {
                // SAFETY: Each pointer in `buffers` came from an allocation for
                // this slab class's `size`, so deallocating it with the
                // matching layout is correct.
                unsafe {
                    alloc::dealloc(ptr.ptr.as_ptr(), layout_for(size));
                }
            }
        }
    }
}

/// Installs a slab pool for the current thread and returns the previous one.
///
/// Passing `None` clears the thread-local override and restores use of the
/// process-wide fallback slab set.
pub fn set_thread_slab_pool(pool: Option<Arc<FBufSlabs>>) -> Option<Arc<FBufSlabs>> {
    THREAD_SLAB_POOL.with(|current| current.replace(pool))
}

fn with_active_slab_pool<R>(f: impl FnOnce(&FBufSlabs) -> R) -> R {
    let mut f = Some(f);

    if let Ok(result) = TOKIO_FBUF_SLABS.try_with(|pool| f.take().unwrap()(pool)) {
        return result;
    }

    if let Ok(Some(result)) = THREAD_SLAB_POOL.try_with(|pool| {
        let pool = pool.borrow();
        pool.as_ref().map(|pool| f.take().unwrap()(pool))
    }) {
        return result;
    }

    f.take().unwrap()(FALLBACK_SLAB_POOL.as_ref())
}

/// Attempts to satisfy an `FBuf` allocation request from the active slab set,
/// falling back to a fresh allocation when the request is not slab-eligible or
/// no cached buffer is available.
pub(super) fn acquire_allocation(capacity: usize) -> (NonNull<u8>, usize) {
    with_active_slab_pool(|pool| pool.try_take(capacity)).unwrap_or_else(|| {
        let ptr = allocate_exact(capacity);
        (ptr, capacity)
    })
}

/// Attempts to recycle an `FBuf` allocation into the active slab set, falling
/// back to immediate deallocation when the capacity is not slab-eligible or
/// the target slab class is already full.
pub(super) fn recycle_or_dealloc(ptr: NonNull<u8>, capacity: usize) {
    if !with_active_slab_pool(|pool| pool.try_put(ptr, capacity)) {
        // SAFETY: `ptr` is an owned allocation for `capacity` bytes produced
        // by this module or by `FBuf`, and the fallback path deallocates it
        // with the same layout that was used to allocate it.
        unsafe {
            alloc::dealloc(ptr.as_ptr(), layout_for(capacity));
        }
    }
}

fn allocate_exact(capacity: usize) -> NonNull<u8> {
    let layout = layout_for(capacity);
    // SAFETY: `layout` is constructed by `layout_for`, which enforces the
    // invariants required by the allocator API.
    let ptr = unsafe { alloc::alloc(layout) };
    if ptr.is_null() {
        alloc::handle_alloc_error(layout);
    }
    // SAFETY: The pointer was checked for null just above.
    unsafe { NonNull::new_unchecked(ptr) }
}

pub(super) fn layout_for(capacity: usize) -> alloc::Layout {
    // SAFETY: `FBuf::ALIGNMENT` is non-zero and power-of-two by the module
    // const assertion, all callers pass capacities produced or
    // validated by the surrounding `FBuf` allocation logic.
    unsafe { alloc::Layout::from_size_align_unchecked(capacity, FBuf::ALIGNMENT) }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::fbuf::FBuf;

    use super::{FBufSlabs, TOKIO_FBUF_SLABS, set_thread_slab_pool};

    #[test]
    fn slab_pool_reuses_buffers_across_matching_size_classes() {
        let pool = Arc::new(FBufSlabs::default());
        let previous = set_thread_slab_pool(Some(pool));

        let first = FBuf::with_capacity(4096);
        let first_ptr = first.as_ptr();
        drop(first);

        let second = FBuf::with_capacity(3000);
        assert_eq!(second.capacity(), 4096);
        assert_eq!(second.as_ptr(), first_ptr);

        set_thread_slab_pool(previous);
    }

    #[test]
    fn slab_pool_has_fixed_capacity_per_size_class() {
        let pool = Arc::new(FBufSlabs::new(4096));
        let previous = set_thread_slab_pool(Some(pool.clone()));

        let first = FBuf::with_capacity(4096);
        let second = FBuf::with_capacity(4096);
        drop(first);
        drop(second);

        assert_eq!(pool.cached_buffers(4096), 1);

        set_thread_slab_pool(previous);
    }

    #[test]
    fn slab_pool_stats_track_reuse_and_fallbacks() {
        let pool = Arc::new(FBufSlabs::new(4096));
        let previous = set_thread_slab_pool(Some(pool.clone()));

        let first = FBuf::with_capacity(4096);
        drop(first);

        let second = FBuf::with_capacity(3000);
        drop(second);

        let big = FBuf::with_capacity(FBufSlabs::HIGHEST_SLAB_CLASS_CAPACITY + FBuf::ALIGNMENT);
        drop(big);

        let stats = pool.stats();
        let class = stats
            .classes
            .iter()
            .find(|class| class.size == 4096)
            .unwrap();

        assert_eq!(stats.alloc_requests(), 3);
        assert_eq!(stats.mallocs_saved(), 1);
        assert_eq!(stats.malloc_fallbacks(), 2);
        assert_eq!(stats.recycle_requests(), 3);
        assert_eq!(stats.frees_saved(), 2);
        assert_eq!(stats.free_fallbacks(), 1);
        assert_eq!(stats.cached_buffers, 1);
        assert_eq!(stats.fallback_alloc_requests, 1);
        assert_eq!(stats.fallback_recycle_requests, 1);

        assert_eq!(class.alloc_requests, 2);
        assert_eq!(class.alloc_hits, 1);
        assert_eq!(class.malloc_fallbacks(), 1);
        assert_eq!(class.recycle_requests, 2);
        assert_eq!(class.recycle_hits, 2);
        assert_eq!(class.free_fallbacks(), 0);
        assert_eq!(class.available_buffers, 1);
        set_thread_slab_pool(previous);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn task_local_slab_takes_precedence() {
        let thread_pool = Arc::new(FBufSlabs::default());
        let task_pool = Arc::new(FBufSlabs::default());
        let previous = set_thread_slab_pool(Some(thread_pool.clone()));

        TOKIO_FBUF_SLABS
            .scope(task_pool.clone(), async {
                let first = FBuf::with_capacity(4096);
                let first_ptr = first.as_ptr();
                drop(first);

                tokio::task::yield_now().await;

                let second = FBuf::with_capacity(3000);
                assert_eq!(second.capacity(), 4096);
                assert_eq!(second.as_ptr(), first_ptr);
            })
            .await;

        assert_eq!(task_pool.cached_buffers(4096), 1);
        assert_eq!(thread_pool.cached_buffers(4096), 0);

        let stats = task_pool.stats();
        let class = stats
            .classes
            .iter()
            .find(|class| class.size == 4096)
            .unwrap();

        assert_eq!(class.alloc_requests, 2);
        assert_eq!(class.alloc_hits, 1);
        assert_eq!(class.recycle_requests, 2);
        assert_eq!(class.recycle_hits, 2);

        assert_eq!(thread_pool.stats().alloc_requests(), 0);
        assert_eq!(thread_pool.stats().recycle_requests(), 0);

        set_thread_slab_pool(previous);
    }
}
