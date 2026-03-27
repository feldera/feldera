//! Key filters for exact seeks.
//!
//! These filters only answer "definitely not present" versus "might still be
//! present", so callers can reject cheap cases before doing the exact lookup.

use crate::{
    dynamic::DataTrait,
    storage::{
        file::reader::{ColumnSpec, Reader},
        filter_stats::{FilterStats, TrackingFilterStats},
    },
    trace::ord::key_range::KeyRange,
};
use dyn_clone::{DynClone, clone_box};
use size_of::SizeOf;
use std::sync::Arc;

/// A cheap, in-memory precheck used by `seek_key_exact`.
///
/// Each filter may only reject a key early. Returning `true` means "the key
/// might still be present", so the caller must continue with the next filter or
/// the exact lookup.
pub(crate) trait BatchFilter<K>: DynClone + Send + Sync
where
    K: DataTrait + ?Sized,
{
    /// Returns `false` only when the key is definitely absent.
    ///
    /// Filters that need a hash compute it through `hash`, so a chain of
    /// filters pays that cost at most once.
    fn maybe_contains_key(&self, key: &K, hash: &mut Option<u64>) -> bool;

    /// Statistics for this filter.
    fn stats(&self) -> FilterStats;
}

/// Ordered key filters for file-backed batches.
///
/// The key range is kept separately from the trait objects because batches
/// thread typed bounds through file-writer finalization and rebuild the range
/// when they create a new reader. All other filters are stored behind a trait
/// object so we can swap bloom for bitmap or another single post-range filter
/// without changing the batch structs again.
#[derive(SizeOf)]
pub(crate) struct BatchFilters<K>
where
    K: DataTrait + ?Sized,
{
    range_filter: Arc<TrackedRangeFilter<K>>,
    #[size_of(skip)]
    membership_filter: Box<dyn BatchFilter<K>>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub(crate) struct BatchFilterStats {
    pub(crate) range_filter: FilterStats,
    pub(crate) membership_filter: FilterStats,
}

/// A range filter with statistics.
#[derive(SizeOf)]
struct TrackedRangeFilter<K>
where
    K: DataTrait + ?Sized,
{
    range: Option<KeyRange<K>>,
    #[size_of(skip)]
    tracking: TrackingFilterStats,
}

impl<K> TrackedRangeFilter<K>
where
    K: DataTrait + ?Sized,
{
    fn new(range: Option<KeyRange<K>>) -> Self {
        let size_byte = range
            .as_ref()
            .map(|range| range.size_of().total_bytes())
            .unwrap_or_default();
        Self {
            range,
            tracking: TrackingFilterStats::new(size_byte),
        }
    }

    fn stats(&self) -> FilterStats {
        self.tracking.stats()
    }
}

impl<K> BatchFilters<K>
where
    K: DataTrait + ?Sized,
{
    pub(crate) fn new(
        range_filter: Option<KeyRange<K>>,
        membership_filter: Box<dyn BatchFilter<K>>,
    ) -> Self {
        Self {
            range_filter: Arc::new(TrackedRangeFilter::new(range_filter)),
            membership_filter,
        }
    }

    /// Builds the current file-backed filter chain.
    ///
    /// The range check runs before the bloom filter so out-of-range keys never
    /// pay the hash or bloom lookup cost.
    pub(crate) fn from_file<A, N>(
        key_range: Option<KeyRange<K>>,
        file: Arc<Reader<(&'static K, &'static A, N)>>,
    ) -> Self
    where
        A: DataTrait + ?Sized,
        N: 'static,
        (&'static K, &'static A, N): ColumnSpec,
    {
        Self::new(key_range, Box::new(file))
    }

    pub(crate) fn stats(&self) -> BatchFilterStats {
        BatchFilterStats {
            range_filter: self.range_filter.stats(),
            membership_filter: self.membership_filter.stats(),
        }
    }

    pub(crate) fn maybe_contains_key(&self, key: &K, mut hash: Option<u64>) -> bool {
        if !self.range_filter.maybe_contains_key(key, &mut hash) {
            return false;
        }

        self.membership_filter.maybe_contains_key(key, &mut hash)
    }
}

impl<K> Clone for BatchFilters<K>
where
    K: DataTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            range_filter: self.range_filter.clone(),
            membership_filter: clone_box(self.membership_filter.as_ref()),
        }
    }
}

impl<K> BatchFilter<K> for Arc<TrackedRangeFilter<K>>
where
    K: DataTrait + ?Sized,
{
    fn maybe_contains_key(&self, key: &K, _hash: &mut Option<u64>) -> bool {
        let is_hit = self.range.as_ref().is_some_and(|range| range.contains(key));
        self.tracking.record(is_hit);
        is_hit
    }

    fn stats(&self) -> FilterStats {
        self.as_ref().stats()
    }
}

impl<K, A, N> BatchFilter<K> for Arc<Reader<(&'static K, &'static A, N)>>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
    N: 'static,
    (&'static K, &'static A, N): ColumnSpec,
{
    fn maybe_contains_key(&self, key: &K, hash: &mut Option<u64>) -> bool {
        let hash = hash.get_or_insert_with(|| key.default_hash());
        self.as_ref().maybe_contains_key(*hash)
    }

    fn stats(&self) -> FilterStats {
        self.as_ref().membership_filter_stats()
    }
}

#[cfg(test)]
mod tests {
    use super::{BatchFilter, TrackedRangeFilter};
    use crate::{
        dynamic::DynData, storage::filter_stats::FilterStats, trace::ord::key_range::KeyRange,
    };
    use std::sync::Arc;

    #[test]
    fn tracked_range_filter_stats() {
        let filter = Arc::new(TrackedRangeFilter::new(Some(KeyRange::from_refs(
            (&1i32) as &DynData,
            (&10i32) as &DynData,
        ))));

        assert!(filter.maybe_contains_key((&5i32) as &DynData, &mut None));
        assert!(!filter.maybe_contains_key((&11i32) as &DynData, &mut None));

        let stats = filter.stats();
        assert!(stats.size_byte > 0);
        assert_eq!(
            stats,
            FilterStats {
                size_byte: stats.size_byte,
                hits: 1,
                misses: 1,
            }
        );
    }
}
