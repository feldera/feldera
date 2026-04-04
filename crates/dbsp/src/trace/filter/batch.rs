//! Key filters for exact seeks.
//!
//! These filters only answer "definitely not present" versus "might still be
//! present", so callers can reject cheap cases before doing the exact lookup.

use crate::{
    dynamic::{DataTrait, DynVec},
    storage::{
        file::{
            BatchKeyFilter, FilterKind, FilterStats, TrackingFilterStats, TrackingRoaringBitmap,
            reader::FilteredKeys,
        },
        tracking_bloom_filter::TrackingBloomFilter,
    },
    trace::filter::key_range::KeyRange,
};
use size_of::SizeOf;
use std::sync::Arc;

/// A cheap, in-memory precheck used by `seek_key_exact`.
///
/// Each filter may only reject a key early. Returning `true` means "the key
/// might still be present", so the caller must continue with the next filter or
/// the exact lookup.
pub(crate) trait BatchFilter<K>: Send + Sync
where
    K: DataTrait + ?Sized,
{
    /// Returns `false` only when the key is definitely absent.
    ///
    /// Filters that need a hash compute it through `hash`, so a chain of
    /// filters pays that cost at most once.
    fn maybe_contains_key(&self, key: &K, hash: &mut Option<u64>) -> bool;

    /// Filter kind for observability.
    fn kind(&self) -> FilterKind;

    /// Statistics for this filter.
    fn stats(&self) -> FilterStats;
}

/// Ordered key filters for exact-key probes against file-backed batches.
///
/// The key range is kept separately from the trait objects because batches
/// thread typed bounds through file-writer finalization and rebuild the range
/// when they create a new reader. All other filters are stored behind a trait
/// object so we can swap bloom for bitmap or another single post-range filter
/// without changing the batch structs again.
pub struct BatchFilters<K>
where
    K: DataTrait + ?Sized,
{
    range_filter: Arc<TrackedRangeFilter<K>>,
    membership_filter: Option<Arc<dyn BatchFilter<K>>>,
}

/// Runtime statistics for the range and membership filters inside
/// [`BatchFilters`].
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct BatchFilterStats {
    pub range_filter: FilterStats,
    pub membership_filter: FilterStats,
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
        membership_filter: Option<Arc<dyn BatchFilter<K>>>,
    ) -> Self {
        Self {
            range_filter: Arc::new(TrackedRangeFilter::new(range_filter)),
            membership_filter,
        }
    }

    /// Rebuilds the filter chain from persisted key bounds and an optional
    /// membership filter.
    ///
    /// This is the public constructor for callers that read filter state back
    /// from disk and want the same range-then-membership behavior that trace
    /// batches use for `seek_key_exact`.
    pub fn from_parts(
        key_bounds: Option<(Box<K>, Box<K>)>,
        membership_filter: Option<BatchKeyFilter>,
    ) -> Self {
        Self::from_file(key_bounds.map(KeyRange::from), membership_filter)
    }

    /// Builds the current file-backed filter chain.
    ///
    /// The range check runs before the bloom filter so out-of-range keys never
    /// pay the hash or bloom lookup cost.
    pub(crate) fn from_file(
        key_range: Option<KeyRange<K>>,
        membership_filter: Option<BatchKeyFilter>,
    ) -> Self {
        let membership_filter = membership_filter.map(Arc::<dyn BatchFilter<K>>::from);
        Self::new(key_range, membership_filter)
    }

    /// Returns cumulative statistics for the range and membership filters.
    pub fn stats(&self) -> BatchFilterStats {
        BatchFilterStats {
            range_filter: self.range_filter.stats(),
            membership_filter: self
                .membership_filter
                .as_ref()
                .map(|filter| filter.stats())
                .unwrap_or_default(),
        }
    }

    pub fn membership_filter_kind(&self) -> FilterKind {
        self.membership_filter
            .as_ref()
            .map(|filter| filter.kind())
            .unwrap_or(FilterKind::None)
    }

    /// Returns the cached key bounds, when available.
    pub fn key_bounds(&self) -> Option<(&K, &K)> {
        self.range_filter.range.as_ref().map(|range| range.bounds())
    }

    /// Returns `keys`, optionally narrowed to the indexes that pass the filter
    /// chain when that is cheap enough to be worthwhile.
    pub(crate) fn filtered_keys<'a>(&self, keys: &'a DynVec<K>) -> FilteredKeys<'a, K> {
        debug_assert!(keys.is_sorted_by(&|a, b| a.cmp(b)));

        let mut filter_pass_keys = Vec::with_capacity(keys.len().min(50));
        for (index, key) in keys.dyn_iter().enumerate() {
            if self.maybe_contains_key(key, None) {
                filter_pass_keys.push(index);
                if filter_pass_keys.len() >= keys.len() / 300 {
                    return FilteredKeys::all(keys);
                }
            }
        }

        FilteredKeys::with_filter_pass_keys(keys, Some(filter_pass_keys))
    }

    /// Returns `false` only when `key` is definitely not present.
    ///
    /// Passing a cached `hash` avoids recomputing it when the caller already
    /// has one available.
    pub fn maybe_contains_key(&self, key: &K, mut hash: Option<u64>) -> bool {
        // The range filter runs first so later filters only see keys inside
        // the batch bounds. That matters for roaring-backed batches: once the
        // range check has accepted the key, min-offset conversion is known to
        // fit in `u32` because roaring filters are only built for batches
        // whose full key range fits in `u32`.
        if !self.range_filter.maybe_contains_key(key, &mut hash) {
            return false;
        }

        self.membership_filter
            .as_ref()
            .is_none_or(|filter| filter.maybe_contains_key(key, &mut hash))
    }
}

impl<K> Clone for BatchFilters<K>
where
    K: DataTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            range_filter: self.range_filter.clone(),
            membership_filter: self.membership_filter.clone(),
        }
    }
}

impl<K> SizeOf for BatchFilters<K>
where
    K: DataTrait + ?Sized,
{
    fn size_of_children(&self, context: &mut size_of::Context) {
        self.range_filter.size_of_with_context(context);
        context.add(
            self.membership_filter
                .as_ref()
                .map(|filter| filter.stats().size_byte)
                .unwrap_or_default(),
        );
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

    fn kind(&self) -> FilterKind {
        FilterKind::Range
    }

    fn stats(&self) -> FilterStats {
        self.as_ref().stats()
    }
}

impl<K> BatchFilter<K> for TrackingBloomFilter
where
    K: DataTrait + ?Sized,
{
    fn maybe_contains_key(&self, key: &K, hash: &mut Option<u64>) -> bool {
        let hash = hash.get_or_insert_with(|| key.default_hash());
        self.contains_hash(*hash)
    }

    fn kind(&self) -> FilterKind {
        FilterKind::Bloom
    }

    fn stats(&self) -> FilterStats {
        TrackingBloomFilter::stats(self)
    }
}

impl<K> BatchFilter<K> for TrackingRoaringBitmap
where
    K: DataTrait + ?Sized,
{
    fn maybe_contains_key(&self, key: &K, _hash: &mut Option<u64>) -> bool {
        self.maybe_contains_key(key)
    }

    fn kind(&self) -> FilterKind {
        FilterKind::Roaring
    }

    fn stats(&self) -> FilterStats {
        TrackingRoaringBitmap::stats(self)
    }
}

impl<K> From<BatchKeyFilter> for Arc<dyn BatchFilter<K>>
where
    K: DataTrait + ?Sized,
{
    fn from(filter: BatchKeyFilter) -> Self {
        match filter {
            BatchKeyFilter::Bloom(filter) => Arc::new(filter),
            BatchKeyFilter::RoaringU32(filter) => Arc::new(filter),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{BatchFilter, TrackedRangeFilter};
    use crate::{dynamic::DynData, storage::file::FilterStats, trace::filter::key_range::KeyRange};
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
