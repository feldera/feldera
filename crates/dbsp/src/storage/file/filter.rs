mod bloom;
mod roaring;
mod stats;

use crate::{
    Runtime,
    dynamic::{DataTrait, DynVec},
    storage::tracking_bloom_filter::TrackingBloomFilter,
    trace::{BatchReader, BatchReaderFactories, sample_keys_from_batches},
};
use dyn_clone::clone_box;
use rand::thread_rng;
use std::io;

pub use roaring::TrackingRoaringBitmap;
pub(crate) use roaring::{
    FILTER_PLAN_MIN_SAMPLE_SIZE, FILTER_PLAN_SAMPLE_PERCENT, RoaringLookupSampleStats,
};
pub use stats::{FilterKind, FilterStats, TrackingFilterStats};

/// In-memory representation of the per-batch key filter.
#[derive(Debug)]
pub enum BatchKeyFilter {
    /// Probabilistic Bloom filter over key hashes.
    Bloom(TrackingBloomFilter),

    /// Exact roaring bitmap for key types whose batch's range fits in `u32`.
    RoaringU32(TrackingRoaringBitmap),
}

impl BatchKeyFilter {
    pub(crate) fn new_bloom(estimated_keys: usize, bloom_false_positive_rate: f64) -> Self {
        Self::Bloom(bloom::new_bloom_filter(
            estimated_keys,
            bloom_false_positive_rate,
        ))
    }

    pub(crate) fn new_roaring_u32<K>(min: &K) -> Self
    where
        K: DataTrait + ?Sized,
    {
        Self::RoaringU32(TrackingRoaringBitmap::with_min(min))
    }

    pub(crate) fn deserialize_bloom(num_hashes: u32, data: Vec<u64>) -> Self {
        Self::Bloom(bloom::deserialize_bloom_filter(num_hashes, data))
    }

    pub(crate) fn deserialize_roaring_u32<K>(data: &[u8], min: &K) -> io::Result<Self>
    where
        K: DataTrait + ?Sized,
    {
        TrackingRoaringBitmap::deserialize_from(data, min).map(Self::RoaringU32)
    }

    pub(crate) fn insert_key<K>(&mut self, key: &K)
    where
        K: DataTrait + ?Sized,
    {
        match self {
            Self::Bloom(filter) => {
                filter.insert_hash(key.default_hash());
            }
            Self::RoaringU32(filter) => {
                filter.insert_key(key);
            }
        }
    }
    pub(crate) fn finalize(&mut self) {
        match self {
            Self::Bloom(_) => {}
            Self::RoaringU32(filter) => filter.finalize(),
        }
    }
}

/// Merge-time input used to choose the batch membership filter before writing.
///
/// The writer must know upfront whether it is building Bloom or bitmap state,
/// because it cannot switch filters after the first key is written. The plan
/// therefore bundles:
/// - the merged batch bounds, which tell us whether min-offset roaring fits;
/// - a sampled subset of input keys, which lets us predict lookup behavior
///   when Bloom and roaring are both enabled.
pub struct FilterPlan<K>
where
    K: DataTrait + ?Sized,
{
    min: Box<K>,
    max: Box<K>,
    sampled_keys: Option<Box<DynVec<K>>>,
}

impl<K> FilterPlan<K>
where
    K: DataTrait + ?Sized,
{
    fn sample_count_for_filter_plan(num_keys: usize) -> usize {
        let scaled = ((num_keys as f64) * (FILTER_PLAN_SAMPLE_PERCENT / 100.0)).ceil() as usize;
        scaled.max(FILTER_PLAN_MIN_SAMPLE_SIZE).min(num_keys)
    }

    /// Builds a filter plan from the known minimum and maximum batch keys.
    pub fn from_bounds(min: &K, max: &K) -> Self {
        Self {
            min: clone_box(min),
            max: clone_box(max),
            sampled_keys: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_sampled_keys(mut self, sampled_keys: Box<DynVec<K>>) -> Self {
        self.sampled_keys = Some(sampled_keys);
        self
    }

    pub(crate) fn from_batches<'a, B, I>(batches: I) -> Option<Self>
    where
        B: BatchReader<Key = K>,
        I: IntoIterator<Item = &'a B>,
    {
        let batches: Vec<&'a B> = batches.into_iter().collect();
        let mut bounds: Option<(Box<K>, Box<K>)> = None;
        for batch in &batches {
            let (batch_min, batch_max) = batch.key_bounds()?;
            match bounds.as_mut() {
                Some((min, max)) => {
                    if batch_min < min.as_ref() {
                        *min = clone_box(batch_min);
                    }
                    if batch_max > max.as_ref() {
                        *max = clone_box(batch_max);
                    }
                }
                None => bounds = Some((clone_box(batch_min), clone_box(batch_max))),
            }
        }

        bounds.map(|(min, max)| {
            let mut plan = Self {
                min,
                max,
                sampled_keys: None,
            };
            if plan.roaring_range_fits() {
                plan.sampled_keys = Self::collect_sampled_keys_from_batches(&batches);
            }
            plan
        })
    }

    fn collect_sampled_keys_from_batches<B>(batches: &[&B]) -> Option<Box<DynVec<K>>>
    where
        B: BatchReader<Key = K>,
    {
        let first_batch = batches.first()?;
        let mut sampled_keys = first_batch.factories().keys_factory().default_box();
        let total_sample_size = batches
            .iter()
            .map(|batch| Self::sample_count_for_filter_plan(batch.key_count()))
            .sum::<usize>();
        sampled_keys.reserve(total_sample_size);

        let mut rng = thread_rng();
        sample_keys_from_batches(
            &first_batch.factories(),
            batches,
            &mut rng,
            |batch| Self::sample_count_for_filter_plan(batch.key_count()),
            sampled_keys.as_mut(),
        );

        (!sampled_keys.is_empty()).then_some(sampled_keys)
    }

    fn roaring_range_fits(&self) -> bool {
        self.min.supports_roaring32() && self.max.into_roaring_u32(self.min.as_data()).is_some()
    }

    fn can_use_roaring(&self, enable_roaring: bool) -> bool {
        enable_roaring && self.roaring_range_fits()
    }

    fn predict_lookup_prefers_roaring(&self, estimated_keys: usize) -> bool {
        let sampled_keys = match self.sampled_keys.as_ref() {
            Some(sampled_keys) => sampled_keys,
            None => return false,
        };

        let mut roaring_keys = Vec::with_capacity(sampled_keys.len());
        for index in 0..sampled_keys.len() {
            let roaring_key = match sampled_keys
                .index(index)
                .into_roaring_u32(self.min.as_data())
            {
                Some(roaring_key) => roaring_key,
                None => return false,
            };
            roaring_keys.push(roaring_key);
        }
        roaring_keys.sort_unstable();
        roaring_keys.dedup();

        RoaringLookupSampleStats::from_sample(estimated_keys, &roaring_keys)
            .map(|stats| stats.lookup_prefers_roaring())
            .unwrap_or(false)
    }

    fn preferred_filter(
        &self,
        estimated_keys: usize,
        enable_roaring: bool,
        bloom_false_positive_rate: f64,
    ) -> BatchKeyFilter {
        if self.can_use_roaring(enable_roaring)
            && self.predict_lookup_prefers_roaring(estimated_keys)
        {
            BatchKeyFilter::new_roaring_u32(self.min.as_ref())
        } else {
            BatchKeyFilter::new_bloom(estimated_keys, bloom_false_positive_rate)
        }
    }

    /// Chooses the membership filter to build for a batch with `estimated_keys`
    /// rows, using the enabled Bloom/roaring settings and an optional batch
    /// bounds plan.
    pub fn decide_filter(
        filter_plan: Option<&Self>,
        estimated_keys: usize,
    ) -> Option<BatchKeyFilter> {
        // Choose between Bloom, roaring, or no membership filter using the
        // following rules:
        //
        // - If Bloom and roaring are both enabled, prefer roaring when the
        //   plan proves the batch range fits in `u32` and the sampled-key
        //   lookup predictor says roaring should beat Bloom. If sampling is
        //   unavailable or the predictor cannot run, fall back to Bloom.
        // - If only Bloom is enabled, always build Bloom.
        // - If only roaring is enabled, build roaring only when the plan
        //   proves the batch range fits in `u32`; otherwise build no
        //   membership filter.
        // - If both are disabled, build no membership filter.
        //
        // The "no plan => no roaring" rule is intentional: without known
        // batch bounds we cannot safely decide that min-offset roaring
        // encoding will fit, and we do not allow switching filters after
        // writing has started.
        let enable_roaring = Runtime::with_dev_tweaks(|dev_tweaks| dev_tweaks.enable_roaring());
        let bloom_false_positive_rate = Runtime::with_dev_tweaks(|dev_tweaks| {
            let rate = dev_tweaks.bloom_false_positive_rate();
            (rate > 0.0 && rate < 1.0).then_some(rate)
        });
        match (bloom_false_positive_rate, filter_plan) {
            (Some(rate), Some(filter_plan)) => {
                Some(filter_plan.preferred_filter(estimated_keys, enable_roaring, rate))
            }
            (Some(rate), None) => Some(BatchKeyFilter::new_bloom(estimated_keys, rate)),
            (None, Some(filter_plan)) if filter_plan.can_use_roaring(enable_roaring) => {
                Some(BatchKeyFilter::new_roaring_u32(filter_plan.min.as_ref()))
            }
            (None, _) => None,
        }
    }
}
