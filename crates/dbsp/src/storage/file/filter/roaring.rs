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

    // Bloom filters allocate their backing bitset up front, so their tracked
    // size is stable after construction. Roaring bitmaps grow as keys are
    // inserted and can shrink again after `optimize()`, so refresh the tracked
    // size once the batch is finalized instead of trying to maintain it on
    // every insert.
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
        key.into_roaring_u32_checked(self.min.as_ref())
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
///   split into `2^16` containers;
/// - within each touched container, roaring-rs keeps values in an array until
///   the container reaches about 4096 entries, then upgrades it to a bitmap;
/// - sparse batches therefore tend to pay binary-search costs in many small
///   array containers, while dense batches benefit from cheap bitmap probes.
///
/// The predictor estimates those two things from a sample:
/// - how many 16-bit containers the batch likely touches
/// - how many keys each touched container likely holds
#[derive(Debug, Clone, Copy)]
pub(crate) struct RoaringLookupSampleStats {
    // Estimated number of real keys per touched 16-bit window after rescaling
    // the sampled keys/window by the sample fraction.
    estimated_keys_per_window: f64,
    // Estimated number of distinct 16-bit windows touched by the full batch.
    estimated_touched_windows: f64,
}

impl RoaringLookupSampleStats {
    const ROARING_WINDOW_CAPACITY: f64 = 65_536.0;
    const ROARING_BITMAP_CONTAINER_THRESHOLD: f64 = 4_096.0;
    const LOOKUP_ROARING_WINDOW_PROBABILITY_THRESHOLD: f64 = 0.1;
    const LOOKUP_ROARING_BITMAP_WINDOW_PROBABILITY_PENALTY: f64 = 0.1;
    const LOOKUP_ROARING_ARRAY_WINDOW_PROBABILITY_PENALTY_BASE: f64 = 0.25;
    const LOOKUP_ROARING_ARRAY_WINDOW_PROBABILITY_PENALTY_PER_LOG2_KEY: f64 = 0.15;
    const TOUCHED_WINDOWS_CHAO1_DAMPING: f64 = 0.25;
    const U32_WINDOW_COUNT: usize = 1 << 16;

    /// Estimate roaring friendliness for lookups from a small sample of keys.
    ///
    /// The estimator works is based on `crates/dbsp/benches/filter_predictor.rs`:
    /// 1. Bucket sampled keys by their high 16 bits, which matches Roaring's
    ///    top-level `u32` container layout.
    /// 2. Rescale sampled keys/window by the sample fraction so large & dense
    ///    batches do not look artificially sparse.
    /// 3. Estimate the full-batch touched-window count by combining a uniform
    ///    occupancy model with a Chao1 unseen-window correction.
    ///
    /// Example:
    /// - If the batch has `1_000_000` keys and the sample contains `1_000`,
    ///   the sample fraction is `0.001` (`0.1%`).
    /// - If those `1_000` sampled keys touch `50` windows, then the sampled
    ///   average is `20` keys/window and the rescaled estimate is
    ///   `20 / 0.001 = 20_000` real keys/window.
    /// - If many sampled windows are singletons, the Chao1 correction pushes
    ///   the touched-window estimate upward because the sample likely missed
    ///   many windows entirely.
    pub(crate) fn from_sample(batch_keys: usize, sampled_keys: &[u32]) -> Option<Self> {
        if batch_keys == 0 || sampled_keys.is_empty() {
            return None;
        }

        let sampled_key_count = sampled_keys.len();
        let mut per_window: HashMap<u16, usize> = HashMap::new();
        for &key in sampled_keys {
            let window = (key >> 16) as u16;
            *per_window.entry(window).or_insert(0) += 1;
        }

        let distinct_windows = per_window.len();
        if distinct_windows == 0 {
            return None;
        }

        let sample_fraction = sampled_key_count as f64 / batch_keys as f64;
        if sample_fraction <= 0.0 {
            return None;
        }

        let avg_sample_keys_per_window = sampled_key_count as f64 / distinct_windows as f64;
        // Without this rescaling, large but dense batches look artificially
        // sparse and the predictor drifts toward Bloom.
        let estimated_keys_per_window =
            (avg_sample_keys_per_window / sample_fraction).min(Self::ROARING_WINDOW_CAPACITY);
        // Sparse, wide samples often show up as many singleton windows and very
        // few doubletons. Those are exactly the signals the Chao1 correction
        // uses to estimate how many windows the sample likely missed entirely.
        let sample_singleton_windows = per_window.values().filter(|&&count| count == 1).count();
        let sample_doubleton_windows = per_window.values().filter(|&&count| count == 2).count();
        let estimated_touched_windows = estimate_touched_windows(
            batch_keys,
            sampled_key_count,
            distinct_windows,
            sample_singleton_windows,
            sample_doubleton_windows,
        );

        Some(Self {
            estimated_keys_per_window,
            estimated_touched_windows,
        })
    }

    /// Predict whether lookup-heavy workloads should prefer Roaring.
    ///
    /// Random probes only pay container cost when they land in a touched
    /// 16-bit window, so touched-window count is normalized into a
    /// probability. Array containers get a size-dependent penalty because
    /// `ArrayStore::contains()` gets slower as they grow, while bitmap
    /// containers are treated as near-constant-time once the estimated
    /// keys/window crosses Roaring's array-to-bitmap threshold.
    pub(crate) fn lookup_prefers_roaring(&self) -> bool {
        let lookup_window_probability =
            (self.estimated_touched_windows / Self::U32_WINDOW_COUNT as f64).clamp(0.0, 1.0);
        // roaring-rs switches between array and bitmap containers around 4096
        // elements. Bitmap containers are close to a constant-time bit test,
        // but array containers use binary search and get meaningfully slower
        // as they grow.
        let lookup_container_penalty =
            if self.estimated_keys_per_window >= Self::ROARING_BITMAP_CONTAINER_THRESHOLD {
                Self::LOOKUP_ROARING_BITMAP_WINDOW_PROBABILITY_PENALTY
            } else {
                Self::LOOKUP_ROARING_ARRAY_WINDOW_PROBABILITY_PENALTY_BASE
                    + Self::LOOKUP_ROARING_ARRAY_WINDOW_PROBABILITY_PENALTY_PER_LOG2_KEY
                        * (self.estimated_keys_per_window + 1.0).log2()
            };
        let lookup_cost_proxy = lookup_window_probability * lookup_container_penalty;
        let lookup_score = Self::LOOKUP_ROARING_WINDOW_PROBABILITY_THRESHOLD
            / lookup_cost_proxy.max(f64::MIN_POSITIVE);
        lookup_score >= 1.0
    }
}

/// Estimate how many distinct 16-bit Roaring windows the full batch touches.
///
/// This combines:
/// 1. A uniform occupancy estimate that works well when windows are populated
///    fairly evenly.
/// 2. A Chao1-style unseen-window estimate that reacts when the sample is full
///    of singleton windows and is therefore likely missing many windows.
///
/// The blend exists because just doing a uniform-only estimate under-counts
/// touched windows on sparse, wide distributions and makes random Roaring
/// lookups look cheaper than they are.
fn estimate_touched_windows(
    batch_keys: usize,
    sampled_keys: usize,
    distinct_windows: usize,
    sample_singleton_windows: usize,
    sample_doubleton_windows: usize,
) -> f64 {
    if batch_keys == 0 || sampled_keys == 0 || distinct_windows == 0 {
        return 0.0;
    }
    if sampled_keys >= batch_keys {
        return distinct_windows as f64;
    }

    let uniform_estimate =
        estimate_uniform_touched_windows(batch_keys, sampled_keys, distinct_windows);
    let chao1_estimate = estimate_chao1_touched_windows(
        distinct_windows,
        sample_singleton_windows,
        sample_doubleton_windows,
    );
    blend_touched_window_estimates(
        uniform_estimate,
        chao1_estimate,
        RoaringLookupSampleStats::TOUCHED_WINDOWS_CHAO1_DAMPING,
    )
}

/// Estimate touched windows under a "roughly uniform occupancy" assumption.
///
/// Intuition:
/// - assume the full batch touches `W` windows and spreads keys across them
///   fairly evenly;
/// - given the sample fraction, solve for the `W` that would yield the
///   observed sampled distinct-window count.
///
/// This is the baseline estimate because it behaves sensibly on compact or
/// moderately regular distributions. It falls apart on sparse wide batches,
/// where many windows are touched so rarely that the sample never sees them.
fn estimate_uniform_touched_windows(
    batch_keys: usize,
    sampled_keys: usize,
    distinct_windows: usize,
) -> f64 {
    if batch_keys == 0 || sampled_keys == 0 || distinct_windows == 0 {
        return 0.0;
    }
    if sampled_keys >= batch_keys {
        return distinct_windows as f64;
    }

    let sample_fraction = sampled_keys as f64 / batch_keys as f64;
    let mut low = distinct_windows as f64;
    let mut high = batch_keys.min(RoaringLookupSampleStats::U32_WINDOW_COUNT) as f64;

    if low >= high {
        return low;
    }

    let log_unseen = (-sample_fraction).ln_1p();
    for _ in 0..100 {
        let mid = (low + high) * 0.5;
        let avg_keys_per_window = batch_keys as f64 / mid;
        let observed_windows = mid * (1.0 - (avg_keys_per_window * log_unseen).exp());

        if observed_windows < distinct_windows as f64 {
            low = mid;
        } else {
            high = mid;
        }
    }

    high
}

/// Estimate touched windows with a Chao1-style unseen-species correction.
///
/// Here the "species" are touched 16-bit windows:
/// - `distinct_windows` is how many windows the sample observed
/// - `sample_singleton_windows` counts windows seen exactly once
/// - `sample_doubleton_windows` counts windows seen exactly twice
///
/// Raw Chao1 is intentionally not used directly in the final predictor because
/// it can overreact when `f2` is tiny. We still compute it because it pushes
/// the estimate upward in the cases we care about here: batches that touch
/// many 16-bit windows, but only a few sampled keys land in each window.
fn estimate_chao1_touched_windows(
    distinct_windows: usize,
    sample_singleton_windows: usize,
    sample_doubleton_windows: usize,
) -> f64 {
    let chao1_estimate = if sample_doubleton_windows > 0 {
        distinct_windows as f64
            + (sample_singleton_windows * sample_singleton_windows) as f64
                / (2.0 * sample_doubleton_windows as f64)
    } else {
        distinct_windows as f64
            + (sample_singleton_windows.saturating_mul(sample_singleton_windows.saturating_sub(1))
                / 2) as f64
    };

    chao1_estimate
        .max(distinct_windows as f64)
        .min(RoaringLookupSampleStats::U32_WINDOW_COUNT as f64)
}

fn blend_touched_window_estimates(uniform_estimate: f64, chao1_estimate: f64, alpha: f64) -> f64 {
    // Raw Chao1 reacts strongly to singleton-heavy samples, which is useful
    // for sparse wide batches but too aggressive to use directly. Blend it
    // toward the uniform estimate so the unseen-window correction only nudges
    // the final estimate in the right direction.
    uniform_estimate + alpha * (chao1_estimate - uniform_estimate)
}

#[cfg(test)]
mod tests {
    use super::TrackingRoaringBitmap;
    use crate::storage::file::FilterStats;

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
}
