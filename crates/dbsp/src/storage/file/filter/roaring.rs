use crate::{
    dynamic::{DataTrait, DynData},
    storage::file::{FilterStats, TrackingFilterStats, format::TouchedWindowCount},
};
use dyn_clone::clone_box;
use roaring::RoaringBitmap;
use size_of::SizeOf;
use std::{io, mem::size_of_val};

/// Roaring bitmap wrapper that tracks hit/miss counts during membership probes.
#[derive(Debug)]
pub struct TrackingRoaringBitmap {
    bitmap: RoaringBitmap,
    min: Box<DynData>,
    tracking: TrackingFilterStats,
}

impl TrackingRoaringBitmap {
    pub(crate) fn new<K>(bitmap: RoaringBitmap, min: &K) -> Self
    where
        K: DataTrait + ?Sized,
    {
        let mut filter = Self {
            bitmap,
            min: clone_box(min.as_data()),
            tracking: TrackingFilterStats::new(0),
        };
        filter.refresh_stats_size();
        filter
    }

    pub(crate) fn with_min<K>(min: &K) -> Self
    where
        K: DataTrait + ?Sized,
    {
        Self::new(RoaringBitmap::new(), min)
    }

    /// Appends a value that is greater than or equal to the current maximum.
    ///
    /// Uses `try_push` which is O(1) amortized, unlike `insert` which does a
    /// binary search over containers. Duplicates are silently ignored.
    ///
    /// # Panics
    ///
    /// Panics if `value` is strictly less than the previously pushed value
    /// (i.e., keys arrived out of order). This indicates a bug in the caller.
    pub(crate) fn push(&mut self, value: u32) {
        if self.bitmap.try_push(value).is_err() {
            assert_eq!(
                self.bitmap.max(),
                Some(value),
                "roaring push received out-of-order value {value} \
                 (current max: {:?})",
                self.bitmap.max(),
            );
        }
    }

    /// Appends a key assuming keys arrive in sorted order (as in the batch
    /// writer). Uses the O(1) push path; duplicates are silently ignored.
    pub(crate) fn push_key<K>(&mut self, key: &K)
    where
        K: DataTrait + ?Sized,
    {
        self.push(self.roaring_u32(key));
    }

    pub(crate) fn finalize(&mut self) {
        self.bitmap.optimize();
        self.refresh_stats_size();
    }

    /// Updates the size of the filter for metrics.
    ///
    /// Bloom filters allocate their backing bitset up front, so their tracked
    /// size is stable after construction. Roaring bitmaps grow as keys are
    /// inserted and can shrink again after `optimize()`, so refresh the tracked
    /// size once the batch is finalized instead of trying to maintain it on
    /// every insert.
    fn refresh_stats_size(&mut self) {
        let min_size = self.min.size_of().total_bytes();
        self.tracking
            .set_size_byte(size_of_val(&self.bitmap) + self.bitmap.serialized_size() + min_size);
    }

    pub(crate) fn contains(&self, value: u32) -> bool {
        let is_hit = self.bitmap.contains(value);
        self.tracking.record(is_hit);
        is_hit
    }

    fn roaring_u32<K>(&self, key: &K) -> u32
    where
        K: DataTrait + ?Sized,
    {
        key.roaring_u32_offset_dyn_checked(self.min.as_ref())
    }

    pub(crate) fn maybe_contains_key<K>(&self, key: &K) -> bool
    where
        K: DataTrait + ?Sized,
    {
        self.contains(self.roaring_u32(key))
    }

    pub(crate) fn stats(&self) -> FilterStats {
        self.tracking.stats()
    }

    pub(crate) fn serialized_size(&self) -> usize {
        self.bitmap.serialized_size()
    }

    pub(crate) fn serialize_into<W: io::Write>(&self, writer: W) -> io::Result<()> {
        self.bitmap.serialize_into(writer)
    }

    pub(crate) fn deserialize_from<R, K>(reader: R, min: &K) -> io::Result<Self>
    where
        R: io::Read,
        K: DataTrait + ?Sized,
    {
        Ok(Self::new(RoaringBitmap::deserialize_from(reader)?, min))
    }
}

/// Tracks the exact number of 16-bit roaring windows touched by a batch while
/// keys are appended in sorted order.
#[derive(Debug)]
pub(crate) struct TouchedWindowCounter {
    min: Option<Box<DynData>>,
    windows: RoaringBitmap,
}

impl Default for TouchedWindowCounter {
    fn default() -> Self {
        Self {
            min: None,
            windows: RoaringBitmap::new(),
        }
    }
}

impl TouchedWindowCounter {
    /// Records `key` and returns `true` while the exact touched-window count
    /// remains representable in the roaring `u32` offset domain.
    ///
    /// Returns `false` once the key type is not roaring-compatible or the
    /// batch span no longer fits in `u32`.
    pub(crate) fn push_key<K>(&mut self, key: &K) -> bool
    where
        K: DataTrait + ?Sized,
    {
        match self.min.as_deref() {
            Some(min) => match key.roaring_u32_offset_dyn(min) {
                Some(offset) => {
                    self.windows.insert(offset >> 16);
                    true
                }
                None => false,
            },
            None => {
                if !key.supports_roaring32() {
                    return false;
                }
                self.min = Some(clone_box(key.as_data()));
                self.windows.insert(0);
                true
            }
        }
    }

    /// Finishes tracking and returns the exact touched-window count.
    pub(crate) fn finish(self) -> TouchedWindowCount {
        TouchedWindowCount::new(
            usize::try_from(self.windows.len()).expect("touched-window count fits in usize"),
        )
    }
}

/// Summary of how a batch likely maps onto Roaring's 16-bit container layout
/// for lookup prediction.
///
/// This exists because Roaring is not uniformly "better than Bloom":
/// - keys are partitioned by their high 16 offset bits, so a `u32` offset
///   domain is split into up to `2^16` containers;
/// - roaring-rs keeps container payloads in arrays until they reach about 4096
///   entries, then upgrades them to bitmaps;
/// - sparse batches therefore tend to pay binary-search costs in many small
///   array containers, while dense batches benefit from cheap bitmap probes.
///
/// The predictor estimates two things from the merged key count and the
/// per-input-batch container spans:
/// - how many 16-bit containers the batch likely touches
/// - how many keys each touched container likely holds
#[derive(Debug, Clone, Copy)]
pub(crate) struct RoaringLookupStats {
    /// Conservative estimate of real keys per touched 16-bit container.
    estimated_keys_per_container: f64,
    /// Conservative estimate of touched 16-bit containers after overlap
    /// damping.
    estimated_touched_containers: f64,
}

impl RoaringLookupStats {
    const ROARING_CONTAINER_CAPACITY: f64 = 65_536.0;
    const ROARING_BITMAP_UPGRADE_THRESHOLD: f64 = 4_096.0;
    const LOOKUP_ROARING_CONTAINER_PROBABILITY_THRESHOLD: f64 = 0.1;
    const LOOKUP_ROARING_BITMAP_CONTAINER_PROBABILITY_PENALTY: f64 = 0.1;
    const LOOKUP_ROARING_ARRAY_CONTAINER_PROBABILITY_PENALTY_BASE: f64 = 0.20;
    const LOOKUP_ROARING_ARRAY_CONTAINER_PROBABILITY_PENALTY_PER_LOG2_KEY: f64 = 0.10;
    const OVERLAP_FACTOR_DAMPING: f64 = 0.25;
    const U32_CONTAINER_COUNT: usize = 1 << 16;

    /// Estimate roaring friendliness for lookups from batch metadata.
    ///
    /// When exact per-batch touched-window counts are available, combine them
    /// with the span-overlap heuristic from the benchmark. Otherwise fall back
    /// to the span-only estimate.
    pub(crate) fn from_metadata(
        batch_keys: usize,
        container_spans: &[(u32, u32)],
        touched_window_counts: &[TouchedWindowCount],
    ) -> Option<Self> {
        let span_stats = Self::from_bounds(batch_keys, container_spans)?;
        if touched_window_counts.is_empty()
            || touched_window_counts.len() != container_spans.len()
            || touched_window_counts
                .iter()
                .any(|count| !count.is_available())
        {
            return Some(span_stats);
        }

        let span_windows_upper_bound = covered_container_union(container_spans);
        let total_span_containers = container_spans
            .iter()
            .map(|(start, end)| (end - start + 1) as usize)
            .sum::<usize>();
        let overlap_factor = total_span_containers as f64 / span_windows_upper_bound as f64;
        let damped_overlap_factor = 1.0 + Self::OVERLAP_FACTOR_DAMPING * (overlap_factor - 1.0);
        let sum_batch_touched_windows = touched_window_counts
            .iter()
            .map(|count| count.get())
            .sum::<usize>();
        let max_batch_touched_windows = touched_window_counts
            .iter()
            .map(|count| count.get())
            .max()
            .expect("checked above");
        let estimated_touched_containers = (sum_batch_touched_windows as f64
            / damped_overlap_factor)
            .clamp(
                max_batch_touched_windows as f64,
                span_windows_upper_bound as f64,
            )
            .min(Self::U32_CONTAINER_COUNT as f64);
        let estimated_keys_per_container = batch_keys as f64 / estimated_touched_containers;

        Some(Self {
            estimated_keys_per_container,
            estimated_touched_containers,
        })
    }

    /// Estimate roaring friendliness for lookups from batch-span metadata.
    ///
    /// The estimator follows the `benches/filter_predictor.rs` benchmark:
    /// 1. Compute the exact union of 16-bit containers covered by the input
    ///    batch spans.
    /// 2. Compute an overlap factor from
    ///    `sum(batch span containers) / union(batch span containers)`.
    /// 3. Damp only the *excess* overlap so disjoint layouts stay at `1.0`,
    ///    but heavily overlapping layouts remain conservative.
    /// 4. Divide the merged key count by this effective touched-container
    ///    count to estimate keys per container.
    ///
    /// Each span is inclusive and already expressed in Roaring's 16-bit
    /// container space, relative to the merged batch minimum.
    pub(crate) fn from_bounds(batch_keys: usize, container_spans: &[(u32, u32)]) -> Option<Self> {
        if batch_keys == 0 || container_spans.is_empty() {
            return None;
        }

        let distinct_containers = covered_container_union(container_spans);
        let total_span_containers = container_spans
            .iter()
            .map(|(start, end)| (end - start + 1) as usize)
            .sum::<usize>();
        let overlap_factor = total_span_containers as f64 / distinct_containers as f64;
        let damped_overlap_factor = 1.0 + Self::OVERLAP_FACTOR_DAMPING * (overlap_factor - 1.0);
        let estimated_touched_containers = (distinct_containers as f64 * damped_overlap_factor)
            .min(Self::U32_CONTAINER_COUNT as f64);
        let estimated_keys_per_container = (batch_keys as f64 / estimated_touched_containers)
            .min(Self::ROARING_CONTAINER_CAPACITY);

        Some(Self {
            estimated_keys_per_container,
            estimated_touched_containers,
        })
    }

    /// Predict whether lookup-heavy workloads should prefer Roaring.
    ///
    /// Random probes only pay container cost when they land in a touched
    /// 16-bit container, so the touched-container count is normalized into a
    /// probability. Array containers get a size-dependent penalty because
    /// `ArrayStore::contains()` gets slower as they grow, while bitmap
    /// containers are treated as near-constant-time once the estimated
    /// keys/container crosses Roaring's array-to-bitmap threshold.
    pub(crate) fn lookup_prefers_roaring(&self) -> bool {
        let lookup_container_probability =
            (self.estimated_touched_containers / Self::U32_CONTAINER_COUNT as f64).clamp(0.0, 1.0);
        // roaring-rs switches between array and bitmap containers around 4096
        // elements. Bitmap containers are close to a constant-time bit test,
        // but array containers use binary search and get meaningfully slower
        // as they grow.
        let lookup_container_penalty =
            if self.estimated_keys_per_container >= Self::ROARING_BITMAP_UPGRADE_THRESHOLD {
                Self::LOOKUP_ROARING_BITMAP_CONTAINER_PROBABILITY_PENALTY
            } else {
                Self::LOOKUP_ROARING_ARRAY_CONTAINER_PROBABILITY_PENALTY_BASE
                    + Self::LOOKUP_ROARING_ARRAY_CONTAINER_PROBABILITY_PENALTY_PER_LOG2_KEY
                        * (self.estimated_keys_per_container + 1.0).log2()
            };
        let lookup_cost_proxy = lookup_container_probability * lookup_container_penalty;
        let lookup_score = Self::LOOKUP_ROARING_CONTAINER_PROBABILITY_THRESHOLD
            / lookup_cost_proxy.max(f64::MIN_POSITIVE);
        lookup_score >= 1.0
    }
}

fn covered_container_union(container_spans: &[(u32, u32)]) -> usize {
    let mut intervals = container_spans.to_vec();
    intervals.sort_unstable();

    let mut merged_containers = 0usize;
    let mut current = intervals[0];
    for interval in intervals.into_iter().skip(1) {
        if interval.0 <= current.1.saturating_add(1) {
            current.1 = current.1.max(interval.1);
        } else {
            merged_containers += (current.1 - current.0 + 1) as usize;
            current = interval;
        }
    }

    merged_containers + (current.1 - current.0 + 1) as usize
}

#[cfg(test)]
mod tests {
    use super::TrackingRoaringBitmap;
    use crate::dynamic::DynData;
    use crate::storage::file::FilterStats;
    use proptest::{collection::vec, prelude::*};
    use std::collections::BTreeSet;

    /// Simple test makes sure filter works.
    #[test]
    fn tracking_roaring_bitmap_stats() {
        let mut filter = TrackingRoaringBitmap::with_min((&0u32) as &crate::dynamic::DynData);
        filter.push(1);
        filter.push(3);

        assert!(filter.contains(1));
        assert!(!filter.contains(2));
        assert_eq!(
            filter.stats(),
            FilterStats {
                size_byte: filter.stats().size_byte,
                hits: 1,
                misses: 1,
            }
        );
    }

    #[test]
    fn push_tolerates_duplicate_max() {
        let mut filter = TrackingRoaringBitmap::with_min((&0u32) as &crate::dynamic::DynData);
        filter.push(5);
        filter.push(5);
        filter.push(10);
        filter.push(10);
        filter.push(10);
        assert!(filter.contains(5));
        assert!(filter.contains(10));
        assert!(!filter.contains(7));
    }

    #[test]
    #[should_panic(expected = "roaring push received out-of-order value")]
    fn push_panics_on_out_of_order() {
        let mut filter = TrackingRoaringBitmap::with_min((&0u32) as &crate::dynamic::DynData);
        filter.push(10);
        filter.push(5);
    }

    /// Filter test covering special cases.
    #[test]
    fn tracking_roaring_bitmap_handles_signed_offsets_and_duplicates() {
        let min = -10i64;
        let second_container_value = min + 65_536;
        let max_offset_value = min + i64::from(u32::MAX);
        let inserted_values = vec![
            min,
            min + 3,
            min + 3,
            0,
            second_container_value,
            max_offset_value,
        ];
        let absent_values = vec![min + 1, 1, second_container_value - 1, max_offset_value - 1];

        let mut filter = TrackingRoaringBitmap::with_min((&min) as &crate::dynamic::DynData);
        for value in &inserted_values {
            filter.push_key(value as &DynData);
        }
        filter.finalize();

        for value in &inserted_values {
            assert!(
                filter.maybe_contains_key(value as &DynData),
                "expected inserted value {value} to be present"
            );
        }
        for value in &absent_values {
            assert!(
                !filter.maybe_contains_key(value as &DynData),
                "expected absent value {value} to be missing"
            );
        }
    }

    /// Round-trip serialize/deserialize keeps the same membership behavior.
    #[test]
    fn tracking_roaring_bitmap_roundtrip_serialization() {
        let min = -123i64;
        const CONTAINER_COUNT: i64 = 1 << 7;
        const CONTAINER_WIDTH: i64 = 1 << 16;

        // Build a bitmap that touches containers `0..128`, leaving container 0
        // empty and making later containers denser with `i^2` inserted keys
        // up through container 127, which contains `127^2 = 16129` keys
        // covering offsets `[127 << 16, (127 << 16) + 16129)`.
        let inserted_values: Vec<i64> = (0..CONTAINER_COUNT)
            .flat_map(|container| {
                let container_base = container << 16;
                let keys_in_container = container * container;
                (0..keys_in_container).map(move |offset| min + container_base + offset)
            })
            .collect();

        let mut original = TrackingRoaringBitmap::with_min((&min) as &crate::dynamic::DynData);
        for value in &inserted_values {
            original.push_key(value as &DynData);
        }
        original.finalize();

        let mut serialized = Vec::new();
        original
            .serialize_into(&mut serialized)
            .expect("roaring filter should serialize");
        let restored = TrackingRoaringBitmap::deserialize_from(
            serialized.as_slice(),
            (&min) as &crate::dynamic::DynData,
        )
        .expect("roaring filter should deserialize");

        assert_eq!(restored.serialized_size(), original.serialized_size());
        assert_eq!(
            restored.stats(),
            FilterStats {
                size_byte: restored.stats().size_byte,
                hits: 0,
                misses: 0,
            }
        );

        for value in &inserted_values {
            assert!(
                restored.maybe_contains_key(value as &DynData),
                "expected inserted value {value} to survive round-trip"
            );
        }
        for container in 0..CONTAINER_COUNT {
            let container_base = min + (container << 16);
            let keys_in_container = container * container;
            for offset in keys_in_container..CONTAINER_WIDTH {
                let value = container_base + offset;
                assert!(
                    !restored.maybe_contains_key((&value) as &DynData),
                    "expected absent value {value} to stay absent after round-trip"
                );
            }
        }

        let untouched_container_value = min + (CONTAINER_COUNT << 16);
        assert!(
            !restored.maybe_contains_key((&untouched_container_value) as &DynData),
            "expected untouched container value {untouched_container_value} to stay absent after round-trip"
        );
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]
        /// Randomized inserts and probes against a reference set.
        /// Makes sure filter fulfills its contract.
        #[test]
        fn tracking_roaring_bitmap_matches_reference_set(
            min in -1_000_000i64..=1_000_000,
            inserted_offsets in vec(any::<u32>(), 0..256),
            probe_offsets in vec(any::<u32>(), 0..256),
        ) {
            // The absolute values can be large and span many Roaring containers,
            // but every generated key still fits this filter because its
            // batch-relative offset from `min` is a `u32`.
            let mut sorted_offsets = inserted_offsets.clone();
            sorted_offsets.sort();
            let inserted_values: Vec<i64> = sorted_offsets
                .iter()
                .map(|offset| min + i64::from(*offset))
                .collect();
            let mut expected = BTreeSet::new();
            let mut filter = TrackingRoaringBitmap::with_min((&min) as &crate::dynamic::DynData);

            for value in &inserted_values {
                filter.push_key(value as &DynData);
                expected.insert(*value);
            }
            filter.finalize();

            for value in &inserted_values {
                prop_assert!(
                    filter.maybe_contains_key(value as &DynData),
                    "inserted value {value} should be present"
                );
            }

            let probed_values: Vec<i64> = probe_offsets
                .iter()
                .map(|offset| min + i64::from(*offset))
                .collect();
            let mut probe_hits = 0usize;
            for value in &probed_values {
                let expected_hit = expected.contains(value);
                probe_hits += usize::from(expected_hit);
                prop_assert_eq!(
                    filter.maybe_contains_key(value as &DynData),
                    expected_hit,
                    "membership mismatch for probed value {}",
                    value,
                );
            }

            let stats = filter.stats();
            let probe_misses = probed_values.len() - probe_hits;
            prop_assert!(stats.size_byte > 0);
            prop_assert_eq!(stats.hits, inserted_values.len() + probe_hits);
            prop_assert_eq!(stats.misses, probe_misses);
        }
    }
}
