mod bloom;
mod roaring;
mod stats;

use crate::{
    Runtime,
    dynamic::{DataTrait, DynData, DynVec},
    storage::tracking_bloom_filter::TrackingBloomFilter,
    trace::{Batch, BatchReaderFactories, sample_keys_from_batches},
};
use dyn_clone::clone_box;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use std::io;
use std::ops::Not;
use std::sync::Once;
use tracing::info;

pub use roaring::TrackingRoaringBitmap;
pub(crate) use roaring::{
    FILTER_PLAN_MIN_SAMPLE_SIZE, FILTER_PLAN_SAMPLE_PERCENT, RoaringLookupSampleStats,
};
pub use stats::{FilterKind, FilterStats, TrackingFilterStats};

const FILTER_PLAN_SAMPLE_SEED: u64 = 0x67;

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

    /// Adds a key to the filter.
    ///
    /// Keys must be pushed in sorted order. The roaring path uses an O(1)
    /// append; the Bloom path is order-independent.
    pub(crate) fn push_key<K>(&mut self, key: &K)
    where
        K: DataTrait + ?Sized,
    {
        match self {
            Self::Bloom(filter) => {
                filter.insert_hash(key.default_hash());
            }
            Self::RoaringU32(filter) => {
                filter.push_key(key);
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
    /// Pre-computed roaring u32 offsets (relative to `min`) from a key sample,
    /// sorted and deduplicated. Used by the lookup predictor to decide between
    /// Bloom and roaring filters.
    sampled_offsets: Option<Vec<u32>>,
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
            sampled_offsets: None,
        }
    }

    /// Converts a key sample into roaring u32 offsets and attaches them to the
    /// plan.
    ///
    /// This is useful for callers that already have a representative set of
    /// keys and want to reuse the same Bloom-vs-roaring decision logic as the
    /// merge path without rebuilding the sample internally.
    pub fn with_sampled_keys(mut self, sampled_keys: &DynVec<K>) -> Self {
        self.sampled_offsets = Self::keys_to_offsets(self.min.as_data(), sampled_keys);
        self
    }

    pub(crate) fn from_batches<'a, B, I>(batches: I) -> Option<Self>
    where
        B: Batch<Key = K>,
        I: IntoIterator<Item = &'a B>,
    {
        let batches: Vec<&'a B> = batches.into_iter().collect();
        let mut bounds: Option<(Box<K>, Box<K>)> = None;
        for batch in &batches {
            let (batch_min, batch_max) = batch.key_bounds()?;
            match bounds.as_mut() {
                Some((min, max)) => {
                    if batch_min < min.as_ref() {
                        batch_min.clone_to(min);
                    }
                    if batch_max > max.as_ref() {
                        batch_max.clone_to(max);
                    }
                }
                None => bounds = Some((clone_box(batch_min), clone_box(batch_max))),
            }
        }

        bounds.map(|(min, max)| {
            let mut plan = Self {
                min,
                max,
                sampled_offsets: None,
            };
            if plan.roaring_range_fits() {
                plan.sampled_offsets =
                    Self::collect_sampled_offsets_from_batches(plan.min.as_data(), &batches);
            }
            plan
        })
    }

    /// Converts a dynamic key vector into sorted, deduplicated `u32` offsets
    /// relative to `min`.
    ///
    /// Accepts unsorted input: the result is always sorted and deduplicated.
    /// When the input is already sorted the offsets are monotonic (subtraction
    /// from a fixed `min` preserves order), so the sort is a no-op.
    ///
    /// Returns `None` if any key cannot be mapped to a `u32` offset or if the
    /// resulting vector is empty.
    fn keys_to_offsets(min: &DynData, keys: &DynVec<K>) -> Option<Vec<u32>> {
        let mut offsets = Vec::with_capacity(keys.len());
        for i in 0..keys.len() {
            offsets.push(keys.index(i).roaring_u32_offset_dyn(min)?);
        }
        offsets.sort_unstable();
        offsets.dedup();
        offsets.is_empty().not().then_some(offsets)
    }

    fn collect_sampled_offsets_from_batches<B>(min: &DynData, batches: &[&B]) -> Option<Vec<u32>>
    where
        B: Batch<Key = K>,
    {
        let first_batch = batches.first()?;
        let factories = first_batch.factories();
        let mut sampled_keys = factories.keys_factory().default_box();
        let total_sample_size = batches
            .iter()
            .map(|batch| Self::sample_count_for_filter_plan(batch.key_count()))
            .sum::<usize>();
        sampled_keys.reserve(total_sample_size);

        // Use a deterministic sampler here so rerunning the same workload on
        // the same data does not flip batches between Bloom and roaring.
        let mut rng = ChaCha8Rng::seed_from_u64(FILTER_PLAN_SAMPLE_SEED);
        sample_keys_from_batches(
            &factories,
            batches,
            &mut rng,
            |batch| Self::sample_count_for_filter_plan(batch.key_count()),
            sampled_keys.as_mut(),
        );

        Self::keys_to_offsets(min, sampled_keys.as_ref())
    }

    fn roaring_range_fits(&self) -> bool {
        self.min.supports_roaring32()
            && self
                .max
                .roaring_u32_offset_dyn(self.min.as_data())
                .is_some()
    }

    fn can_use_roaring(&self, enable_roaring: bool) -> bool {
        enable_roaring && self.roaring_range_fits()
    }

    fn predict_lookup_prefers_roaring(&self, estimated_keys: usize) -> bool {
        let offsets = match self.sampled_offsets.as_ref() {
            Some(offsets) => offsets,
            None => return false,
        };

        RoaringLookupSampleStats::from_sample(estimated_keys, offsets)
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
        let (enable_roaring, bloom_false_positive_rate) = Runtime::with_dev_tweaks(|dev_tweaks| {
            let rate = dev_tweaks.bloom_false_positive_rate();
            (
                dev_tweaks.enable_roaring(),
                (rate > 0.0 && rate < 1.0).then_some(rate),
            )
        });
        static LOG_FILTER_CONFIG: Once = Once::new();
        LOG_FILTER_CONFIG.call_once(|| {
            if let Some(rate) = bloom_false_positive_rate {
                if enable_roaring {
                    info!("Using Bloom filter false positive rate {rate}; \
                           preferring bitmap filters when favorable");
                } else {
                    info!("Using Bloom filter false positive rate {rate}; roaring bitmap filters disabled");
                }
            } else if enable_roaring {
                info!("Bloom filters disabled; bitmap filters enabled unconditionally");
            } else {
                info!("Bloom filters disabled and roaring bitmap filters disabled; \
                       batches will have no membership filter");
            }
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

#[cfg(test)]
mod tests {
    use super::{BatchKeyFilter, FILTER_PLAN_SAMPLE_PERCENT, FilterPlan};
    use crate::{
        DBData, DynZWeight, Runtime, ZWeight,
        circuit::CircuitConfig,
        dynamic::{DynData, Erase},
        storage::file::Factories,
        trace::{Batch, BatchReader, BatchReaderFactories, Builder, VecWSet, VecWSetFactories},
    };

    fn build_vec_wset_u32(keys: std::ops::Range<u32>) -> VecWSet<DynData, DynZWeight> {
        let factories = VecWSetFactories::new::<u32, (), ZWeight>();
        let key_count = keys.len();
        let mut builder = <VecWSet<DynData, DynZWeight> as Batch>::Builder::with_capacity(
            &factories, key_count, 0,
        );

        for key in keys {
            let weight: ZWeight = 1;
            builder.push_val_diff(&(), weight.erase());
            builder.push_key(key.erase());
        }

        builder.done()
    }

    fn sampled_filter_plan<K>(
        factories: &Factories<DynData, DynData>,
        keys: &[K],
    ) -> FilterPlan<DynData>
    where
        K: DBData + Erase<DynData>,
    {
        let mut sampled_keys = factories.keys_factory.default_box();
        sampled_keys.reserve(keys.len());
        for key in keys {
            sampled_keys.push_ref(key.erase());
        }

        FilterPlan::from_bounds(keys.first().unwrap().erase(), keys.last().unwrap().erase())
            .with_sampled_keys(sampled_keys.as_ref())
    }

    fn with_roaring_enabled<F>(f: F)
    where
        F: FnOnce() + Clone + Send + 'static,
    {
        let mut config = CircuitConfig::default();
        config.dev_tweaks.enable_roaring = Some(true);
        let (handle, ()) = Runtime::init_circuit(config, move |_| {
            f();
            Ok(())
        })
        .unwrap();
        handle.kill().unwrap();
    }

    /// Covers the interesting boundary cases for whether a batch range can be
    /// encoded into a `u32` roaring offset domain.
    #[test]
    fn roaring_range_fits_boundaries() {
        let exact_u32_span_max = i64::from(u32::MAX);
        let over_u32_span_max = exact_u32_span_max + 1;

        let exact_fit_i64 = FilterPlan::from_bounds(
            (&-5i64) as &DynData,
            (&(-5i64 + exact_u32_span_max)) as &DynData,
        );
        assert!(exact_fit_i64.roaring_range_fits());

        let too_wide_i64 = FilterPlan::from_bounds(
            (&-5i64) as &DynData,
            (&(-5i64 + over_u32_span_max)) as &DynData,
        );
        assert!(!too_wide_i64.roaring_range_fits());

        let exact_fit_u64 = FilterPlan::from_bounds(
            (&7u64) as &DynData,
            (&(7u64 + u64::from(u32::MAX))) as &DynData,
        );
        assert!(exact_fit_u64.roaring_range_fits());

        let too_wide_u64 = FilterPlan::from_bounds(
            (&7u64) as &DynData,
            (&(7u64 + u64::from(u32::MAX) + 1)) as &DynData,
        );
        assert!(!too_wide_u64.roaring_range_fits());

        let exact_fit_i32 =
            FilterPlan::from_bounds((&-10i32) as &DynData, (&(-10i32 + i32::MAX)) as &DynData);
        assert!(exact_fit_i32.roaring_range_fits());

        let full_u32_domain = FilterPlan::from_bounds((&0u32) as &DynData, (&u32::MAX) as &DynData);
        assert!(full_u32_domain.roaring_range_fits());

        let unsupported_string = FilterPlan::from_bounds(
            (&String::from("a")) as &DynData,
            (&String::from("z")) as &DynData,
        );
        assert!(!unsupported_string.roaring_range_fits());
    }

    /// Collecting sampled keys from disjoint positive-weight batches keeps
    /// approximately `0.1%` of the total keys, and for exact integer-friendly
    /// batch sizes here that means exactly `1024` samples per batch.
    /// This is also the minimum of samples we'd collect in any case
    /// so we avoid failures due to randomness.
    #[test]
    fn collect_sampled_keys_from_batches_samples_point_one_percent() {
        const KEYS_PER_BATCH: u32 = 1_024_000;
        const EXPECTED_SAMPLES_PER_BATCH: usize = 1_024;
        const EXPECTED_TOTAL_SAMPLES: usize = EXPECTED_SAMPLES_PER_BATCH * 2;

        let batch1 = build_vec_wset_u32(0..KEYS_PER_BATCH);
        let batch2 = build_vec_wset_u32(2_000_000..(2_000_000 + KEYS_PER_BATCH));
        let plan = FilterPlan::from_batches([&batch1, &batch2]).expect("non-empty batches");
        let sampled_offsets = plan
            .sampled_offsets
            .as_ref()
            .expect("roaring-compatible batches should collect a sample");

        assert_eq!(sampled_offsets.len(), EXPECTED_TOTAL_SAMPLES);

        let total_keys = (batch1.key_count() + batch2.key_count()) as f64;
        let sampled_fraction = sampled_offsets.len() as f64 / total_keys;
        assert_eq!(sampled_fraction, FILTER_PLAN_SAMPLE_PERCENT / 100.0);
    }

    #[test]
    fn filter_plan_without_sample_falls_back_to_bloom() {
        with_roaring_enabled(|| {
            let filter_plan = FilterPlan::from_bounds((&1u32) as &DynData, (&7u32) as &DynData);
            assert!(matches!(
                FilterPlan::decide_filter(Some(&filter_plan), 3),
                Some(BatchKeyFilter::Bloom(_))
            ));
        });
    }

    #[test]
    fn filter_plan_predictor_prefers_roaring_for_dense_sample() {
        with_roaring_enabled(|| {
            let factories = Factories::<DynData, DynData>::new::<u32, ()>();
            let keys: Vec<u32> = (0..50_000).collect();
            let filter_plan = sampled_filter_plan(&factories, keys.as_slice());

            assert!(matches!(
                FilterPlan::decide_filter(Some(&filter_plan), keys.len()),
                Some(BatchKeyFilter::RoaringU32(_))
            ));
        });
    }

    #[test]
    fn filter_plan_predictor_prefers_bloom_for_sparse_wide_sample() {
        with_roaring_enabled(|| {
            let factories = Factories::<DynData, DynData>::new::<u32, ()>();
            let keys: Vec<u32> = (0..50_000).map(|index| index << 16).collect();
            let filter_plan = sampled_filter_plan(&factories, keys.as_slice());

            assert!(matches!(
                FilterPlan::decide_filter(Some(&filter_plan), keys.len()),
                Some(BatchKeyFilter::Bloom(_))
            ));
        });
    }
}
