use crossbeam::utils::CachePadded;
use enum_map::Enum;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Kind of filter attached to a batch.
#[derive(Clone, Copy, Debug, Default, Enum, Eq, PartialEq, Ord, PartialOrd)]
pub enum FilterKind {
    /// No filter is present.
    #[default]
    None,
    /// Bloom membership filter.
    Bloom,
    /// Roaring bitmap membership filter.
    Roaring,
    /// Min/max range filter.
    Range,
}
/// Statistics about an in-memory key filter.
///
/// The statistics implement addition such that they can be summed across
/// batches. Their addition loses information about individual sizes, hits,
/// misses and by extension, hit rates.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    derive_more::Add,
    derive_more::AddAssign,
    derive_more::Sum,
)]
pub struct FilterStats {
    /// Filter size in bytes.
    pub size_byte: usize,
    /// Number of hits.
    pub hits: usize,
    /// Number of misses.
    pub misses: usize,
}

/// Shared hit/miss accounting for key filters.
#[derive(Debug)]
pub struct TrackingFilterStats {
    size_byte: usize,
    counts: CachePadded<FilterCounts>,
}

#[derive(Debug)]
struct FilterCounts {
    hits: AtomicUsize,
    misses: AtomicUsize,
}

impl TrackingFilterStats {
    /// Creates tracking state for a filter of the given size.
    pub fn new(size_byte: usize) -> Self {
        Self {
            size_byte,
            counts: CachePadded::new(FilterCounts {
                hits: AtomicUsize::new(0),
                misses: AtomicUsize::new(0),
            }),
        }
    }

    /// Retrieves statistics.
    pub fn stats(&self) -> FilterStats {
        FilterStats {
            size_byte: self.size_byte,
            hits: self.counts.hits.load(Ordering::Relaxed),
            misses: self.counts.misses.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn set_size_byte(&mut self, size_byte: usize) {
        self.size_byte = size_byte;
    }

    /// Records the result of one filter probe.
    pub fn record(&self, is_hit: bool) {
        if is_hit {
            self.counts.hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.counts.misses.fetch_add(1, Ordering::Relaxed);
        }
    }
}
