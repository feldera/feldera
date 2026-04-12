use crate::{
    dynamic::{DataTrait, DynData},
    storage::file::{FilterStats, TrackingFilterStats},
};
use dyn_clone::clone_box;
use roaring::RoaringBitmap;
use size_of::SizeOf;
use std::{collections::HashMap, io, mem::size_of_val};

/// Sample 0.1% of keys per batch when building a merge-time filter plan.
pub(crate) const FILTER_PLAN_SAMPLE_PERCENT: f64 = 0.1;
/// Never sample fewer than this many keys from a batch for the filter plan.
pub(crate) const FILTER_PLAN_MIN_SAMPLE_SIZE: usize = 1_024;

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

    pub(crate) fn insert(&mut self, value: u32) {
        self.bitmap.insert(value);
    }

    pub(crate) fn insert_key<K>(&mut self, key: &K)
    where
        K: DataTrait + ?Sized,
    {
        self.insert(self.roaring_u32(key));
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

/// Sample-derived summary of how a batch's key distribution maps onto
/// Roaring's container layout for lookup prediction.
///
/// This exists because Roaring is not uniformly "better than Bloom":
/// - keys are first partitioned by their high 16 bits, so a `u32` domain is
///   split into up to `2^16` containers;
/// - within each touched container, roaring-rs keeps values in an array until
///   the container reaches about 4096 entries, then upgrades it to a bitmap;
/// - sparse batches therefore tend to pay binary-search costs in many small
///   array containers, while dense batches benefit from cheap bitmap probes.
///
/// The predictor estimates two things from a sample:
/// - how many 16-bit containers the batch likely touches
/// - how many keys each touched container likely holds
#[derive(Debug, Clone, Copy)]
pub(crate) struct RoaringLookupSampleStats {
    /// Estimated number of real keys per touched 16-bit container after
    /// rescaling the sampled keys/container by the sample fraction.
    estimated_keys_per_container: f64,
    /// Estimated number of distinct 16-bit containers touched by the full batch.
    estimated_touched_containers: f64,
}

impl RoaringLookupSampleStats {
    const ROARING_CONTAINER_CAPACITY: f64 = 65_536.0;
    const ROARING_BITMAP_UPGRADE_THRESHOLD: f64 = 4_096.0;
    const LOOKUP_ROARING_CONTAINER_PROBABILITY_THRESHOLD: f64 = 0.1;
    const LOOKUP_ROARING_BITMAP_CONTAINER_PROBABILITY_PENALTY: f64 = 0.1;
    const LOOKUP_ROARING_ARRAY_CONTAINER_PROBABILITY_PENALTY_BASE: f64 = 0.25;
    const LOOKUP_ROARING_ARRAY_CONTAINER_PROBABILITY_PENALTY_PER_LOG2_KEY: f64 = 0.15;
    const TOUCHED_CONTAINERS_CHAO1_DAMPING: f64 = 0.25;
    const U32_CONTAINER_COUNT: usize = 1 << 16;

    /// Estimate roaring friendliness for lookups from a small sample of keys.
    ///
    /// The estimator works is based on `crates/dbsp/benches/filter_predictor.rs`:
    /// 1. Bucket sampled keys by their high 16 bits, which matches Roaring's
    ///    top-level `u32` container layout.
    /// 2. Rescale sampled keys/container by the sample fraction so large &
    ///    dense batches do not look artificially sparse.
    /// 3. Estimate the full-batch touched-container count by combining a
    ///    uniform occupancy model with a Chao1 unseen-container correction.
    ///
    /// Example:
    /// - If the batch has `1_000_000` keys and the sample contains `1_000`,
    ///   the sample fraction is `0.001` (`0.1%`).
    /// - If those `1_000` sampled keys touch `50` containers, then the sampled
    ///   average is `20` keys/container and the rescaled estimate is
    ///   `20 / 0.001 = 20_000` real keys/container.
    /// - If many sampled containers are singletons, the Chao1 correction
    ///   pushes the touched-container estimate upward because the sample
    ///   likely missed many containers entirely.
    pub(crate) fn from_sample(batch_keys: usize, sampled_keys: &[u32]) -> Option<Self> {
        if batch_keys == 0 || sampled_keys.is_empty() {
            return None;
        }

        let sampled_key_count = sampled_keys.len();
        let mut per_container: HashMap<u16, usize> = HashMap::new();
        for &key in sampled_keys {
            let container = (key >> 16) as u16;
            *per_container.entry(container).or_insert(0) += 1;
        }

        let distinct_containers = per_container.len();
        debug_assert_ne!(
            distinct_containers, 0,
            "non-empty sample must touch at least one container"
        );

        let sample_fraction = sampled_key_count as f64 / batch_keys as f64;
        if sample_fraction <= 0.0 {
            return None;
        }

        let avg_sample_keys_per_container = sampled_key_count as f64 / distinct_containers as f64;
        // Without this rescaling, large but dense batches look artificially
        // sparse and the predictor drifts toward Bloom.
        let estimated_keys_per_container =
            (avg_sample_keys_per_container / sample_fraction).min(Self::ROARING_CONTAINER_CAPACITY);
        // Sparse, wide samples often show up as many singleton containers and
        // very few doubletons. Those are exactly the signals the Chao1
        // correction uses to estimate how many containers the sample likely
        // missed entirely.
        let sample_singleton_containers =
            per_container.values().filter(|&&count| count == 1).count();
        let sample_doubleton_containers =
            per_container.values().filter(|&&count| count == 2).count();
        let estimated_touched_containers = estimate_touched_containers(
            batch_keys,
            sampled_key_count,
            distinct_containers,
            sample_singleton_containers,
            sample_doubleton_containers,
        );

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

/// Estimate how many distinct 16-bit Roaring containers the full batch touches.
///
/// This combines:
/// 1. A uniform occupancy estimate that works well when containers are
///    populated fairly evenly.
/// 2. A Chao1-style unseen-container estimate that reacts when the sample is
///    full of singleton containers and is therefore likely missing many
///    containers.
///
/// The blend exists because just doing a uniform-only estimate under-counts
/// touched containers on sparse, wide distributions and makes random Roaring
/// lookups look cheaper than they are.
fn estimate_touched_containers(
    batch_keys: usize,
    sampled_keys: usize,
    distinct_containers: usize,
    sample_singleton_containers: usize,
    sample_doubleton_containers: usize,
) -> f64 {
    if batch_keys == 0 || sampled_keys == 0 || distinct_containers == 0 {
        return 0.0;
    }
    if sampled_keys >= batch_keys {
        return distinct_containers as f64;
    }

    let uniform_estimate =
        estimate_uniform_touched_containers(batch_keys, sampled_keys, distinct_containers);
    let chao1_estimate = estimate_chao1_touched_containers(
        distinct_containers,
        sample_singleton_containers,
        sample_doubleton_containers,
    );
    blend_touched_container_estimates(
        uniform_estimate,
        chao1_estimate,
        RoaringLookupSampleStats::TOUCHED_CONTAINERS_CHAO1_DAMPING,
    )
}

/// Estimate touched containers under a "roughly uniform occupancy" assumption.
///
/// Intuition:
/// - assume the full batch touches `W` containers and spreads keys across
///   them fairly evenly;
/// - given the sample fraction, solve for the `W` that would yield the
///   observed sampled distinct-container count.
///
/// This is the baseline estimate because it behaves sensibly on compact or
/// moderately regular distributions. It falls apart on sparse wide batches,
/// where many containers are touched so rarely that the sample never sees
/// them.
fn estimate_uniform_touched_containers(
    batch_keys: usize,
    sampled_keys: usize,
    distinct_containers: usize,
) -> f64 {
    if batch_keys == 0 || sampled_keys == 0 || distinct_containers == 0 {
        return 0.0;
    }
    if sampled_keys >= batch_keys {
        return distinct_containers as f64;
    }

    let sample_fraction = sampled_keys as f64 / batch_keys as f64;
    let mut low = distinct_containers as f64;
    let mut high = batch_keys.min(RoaringLookupSampleStats::U32_CONTAINER_COUNT) as f64;

    if low >= high {
        return low;
    }

    let log_unseen = (-sample_fraction).ln_1p();
    for _ in 0..100 {
        let mid = (low + high) * 0.5;
        let avg_keys_per_container = batch_keys as f64 / mid;
        let observed_containers = mid * (1.0 - (avg_keys_per_container * log_unseen).exp());

        if observed_containers < distinct_containers as f64 {
            low = mid;
        } else {
            high = mid;
        }
    }

    high
}

/// Estimate touched containers with a Chao1-style unseen-species correction.
///
/// Here the "species" are touched 16-bit containers:
/// - `distinct_containers` is how many containers the sample observed
/// - `sample_singleton_containers` counts containers seen exactly once
/// - `sample_doubleton_containers` counts containers seen exactly twice
///
/// Raw Chao1 is intentionally not used directly in the final predictor because
/// it can overreact when `f2` is tiny. We still compute it because it pushes
/// the estimate upward in the cases we care about here: batches that touch
/// many 16-bit containers, but only a few sampled keys land in each one.
fn estimate_chao1_touched_containers(
    distinct_containers: usize,
    sample_singleton_containers: usize,
    sample_doubleton_containers: usize,
) -> f64 {
    let chao1_estimate = if sample_doubleton_containers > 0 {
        distinct_containers as f64
            + (sample_singleton_containers * sample_singleton_containers) as f64
                / (2.0 * sample_doubleton_containers as f64)
    } else {
        distinct_containers as f64
            + (sample_singleton_containers
                .saturating_mul(sample_singleton_containers.saturating_sub(1))
                / 2) as f64
    };

    chao1_estimate
        .max(distinct_containers as f64)
        .min(RoaringLookupSampleStats::U32_CONTAINER_COUNT as f64)
}

/// Raw Chao1 reacts strongly to singleton-heavy samples, which is useful
/// for sparse wide batches but too aggressive to use directly. Blend it
/// toward the uniform estimate so the unseen-container correction only nudges
/// the final estimate in the right direction.
fn blend_touched_container_estimates(
    uniform_estimate: f64,
    chao1_estimate: f64,
    alpha: f64,
) -> f64 {
    uniform_estimate + alpha * (chao1_estimate - uniform_estimate)
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
        filter.insert(1);
        filter.insert(3);

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
            filter.insert_key(value as &DynData);
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
            original.insert_key(value as &DynData);
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
            let inserted_values: Vec<i64> = inserted_offsets
                .iter()
                .map(|offset| min + i64::from(*offset))
                .collect();
            let mut expected = BTreeSet::new();
            let mut filter = TrackingRoaringBitmap::with_min((&min) as &crate::dynamic::DynData);

            for value in &inserted_values {
                filter.insert_key(value as &DynData);
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
