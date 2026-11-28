use crossbeam::utils::CachePadded;
use enum_map::Enum;
use std::cell::Cell;
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
    /// Lookups the filter skipped because it held no module. They are not hits,
    /// so they do not raise the hit rate of a filter that rejects nothing.
    skipped: AtomicUsize,
}

impl TrackingFilterStats {
    /// Creates tracking state for a filter of the given size.
    pub fn new(size_byte: usize) -> Self {
        Self {
            size_byte,
            counts: CachePadded::new(FilterCounts {
                hits: AtomicUsize::new(0),
                misses: AtomicUsize::new(0),
                skipped: AtomicUsize::new(0),
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

    /// Adds outcomes counted elsewhere, for example by a cursor.
    pub fn add_counts(&self, hits: usize, misses: usize) {
        if hits > 0 {
            self.counts.hits.fetch_add(hits, Ordering::Relaxed);
        }
        if misses > 0 {
            self.counts.misses.fetch_add(misses, Ordering::Relaxed);
        }
    }

    /// Adds lookups the filter skipped, counted elsewhere.
    pub(crate) fn add_skipped(&self, lookups: usize) {
        if lookups > 0 {
            self.counts.skipped.fetch_add(lookups, Ordering::Relaxed);
        }
    }

    /// Lookups the filter answered, including those it skipped.
    pub(crate) fn lookups(&self) -> usize {
        let counts = &self.counts;
        counts.hits.load(Ordering::Relaxed)
            + counts.misses.load(Ordering::Relaxed)
            + counts.skipped.load(Ordering::Relaxed)
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

/// Hits and misses that one reader, such as a cursor, counts without atomic
/// operations.
///
/// Adds them to its [`TrackingFilterStats`] every [`Self::FLUSH_EVERY`]
/// outcomes of a kind, so a long-lived reader does not hide its counts, and
/// the rest when it drops. A clone starts its own counts, so no outcome is
/// added twice.
#[derive(Debug)]
pub(crate) struct LocalFilterCounts<'a> {
    tracking: &'a TrackingFilterStats,
    hits: Cell<usize>,
    misses: Cell<usize>,
    skipped: Cell<usize>,
}

impl<'a> LocalFilterCounts<'a> {
    /// Outcomes of one kind counted before they are added to the shared stats.
    const FLUSH_EVERY: usize = 1 << 12;

    pub(crate) fn new(tracking: &'a TrackingFilterStats) -> Self {
        Self {
            tracking,
            hits: Cell::new(0),
            misses: Cell::new(0),
            skipped: Cell::new(0),
        }
    }

    /// Records the result of one filter probe.
    #[inline]
    pub(crate) fn record(&self, is_hit: bool) {
        if is_hit {
            Self::count(&self.hits, |n| self.tracking.add_counts(n, 0));
        } else {
            Self::count(&self.misses, |n| self.tracking.add_counts(0, n));
        }
    }

    /// Records one lookup that the filter skipped, because it held no module.
    #[inline]
    pub(crate) fn record_skipped(&self) {
        Self::count(&self.skipped, |n| self.tracking.add_skipped(n));
    }

    /// Adds one to `count`, and passes the total to `flush` every
    /// [`Self::FLUSH_EVERY`].
    #[inline]
    fn count(count: &Cell<usize>, flush: impl FnOnce(usize)) {
        let n = count.get() + 1;
        if n < Self::FLUSH_EVERY {
            count.set(n);
        } else {
            count.set(0);
            flush(n);
        }
    }
}

impl Clone for LocalFilterCounts<'_> {
    fn clone(&self) -> Self {
        Self::new(self.tracking)
    }
}

impl Drop for LocalFilterCounts<'_> {
    fn drop(&mut self) {
        self.tracking.add_counts(self.hits.get(), self.misses.get());
        self.tracking.add_skipped(self.skipped.get());
    }
}

#[cfg(test)]
mod tests {
    use super::{LocalFilterCounts, TrackingFilterStats};

    /// Local counts reach the shared stats in batches while the reader lives,
    /// and the rest when it drops.
    #[test]
    fn local_counts_are_added_while_the_reader_lives() {
        let tracking = TrackingFilterStats::new(0);
        let counts = LocalFilterCounts::new(&tracking);
        counts.record(true);
        for _ in 0..LocalFilterCounts::FLUSH_EVERY {
            counts.record(false);
        }
        let stats = tracking.stats();
        assert_eq!(
            (stats.hits, stats.misses),
            (0, LocalFilterCounts::FLUSH_EVERY)
        );
        drop(counts);
        let stats = tracking.stats();
        assert_eq!(
            (stats.hits, stats.misses),
            (1, LocalFilterCounts::FLUSH_EVERY)
        );
    }

    /// Lookups a filter did not test count as lookups, not as hits.
    #[test]
    fn skipped_lookups_are_not_hits() {
        let tracking = TrackingFilterStats::new(0);
        let counts = LocalFilterCounts::new(&tracking);
        counts.record(false);
        for _ in 0..LocalFilterCounts::FLUSH_EVERY {
            counts.record_skipped();
        }
        assert_eq!(tracking.lookups(), LocalFilterCounts::FLUSH_EVERY);
        counts.record_skipped();
        drop(counts);
        let stats = tracking.stats();
        assert_eq!((stats.hits, stats.misses), (0, 1));
        assert_eq!(tracking.lookups(), LocalFilterCounts::FLUSH_EVERY + 2);
    }
}
