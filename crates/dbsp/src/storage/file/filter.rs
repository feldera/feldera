mod bloom;
mod roaring;
mod stats;

use super::format::TouchedWindowCount;
use crate::{
    Runtime, dynamic::DataTrait, storage::tracking_bloom_filter::TrackingBloomFilter, trace::Batch,
};
use dyn_clone::clone_box;
use std::io;
use std::ops::Not;
use std::sync::Once;
use tracing::{debug, info};

use roaring::RoaringLookupStats;
pub(crate) use roaring::TouchedWindowCounter;
pub use roaring::TrackingRoaringBitmap;
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
/// - one 16-bit Roaring container span per input batch, relative to the merged
///   minimum;
/// - one exact touched-window count per input batch, relative to that batch's
///   own minimum.
pub struct FilterPlan<K>
where
    K: DataTrait + ?Sized,
{
    min: Box<K>,
    max: Box<K>,
    /// Inclusive 16-bit Roaring container spans, relative to `min`, for each
    /// input batch that contributes to the merged batch.
    container_spans: Option<Vec<(u32, u32)>>,
    /// Exact touched-window counts for each input batch.
    ///
    /// A count of `0` means the exact value was unavailable for that input
    /// batch, so lookup prediction must fall back to spans only.
    touched_window_counts: Vec<TouchedWindowCount>,
}

impl<K> FilterPlan<K>
where
    K: DataTrait + ?Sized,
{
    /// Builds a filter plan from the known minimum and maximum batch keys.
    pub fn from_bounds(min: &K, max: &K) -> Self {
        let mut plan = Self {
            min: clone_box(min),
            max: clone_box(max),
            container_spans: None,
            touched_window_counts: Vec::new(),
        };
        if plan.roaring_range_fits() {
            plan.container_spans = plan
                .container_span(min, max)
                .map(|container_span| vec![container_span]);
        }
        plan
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
                container_spans: None,
                touched_window_counts: Vec::new(),
            };
            if plan.roaring_range_fits() {
                let container_spans = batches
                    .iter()
                    .map(|batch| {
                        let (batch_min, batch_max) =
                            batch.key_bounds().expect("bounds were checked above");
                        plan.container_span(batch_min, batch_max)
                    })
                    .collect::<Option<Vec<_>>>();
                plan.container_spans = container_spans.filter(|spans| spans.is_empty().not());
                plan.touched_window_counts = batches
                    .iter()
                    .map(|batch| batch.touched_window_count())
                    .collect();
            }
            plan
        })
    }

    fn container_span(&self, batch_min: &K, batch_max: &K) -> Option<(u32, u32)> {
        let start = batch_min.roaring_u32_offset_dyn(self.min.as_data())? >> 16;
        let end = batch_max.roaring_u32_offset_dyn(self.min.as_data())? >> 16;
        Some((start, end))
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
        let container_spans = match self.container_spans.as_ref() {
            Some(container_spans) => container_spans,
            None => return false,
        };

        RoaringLookupStats::from_metadata(
            estimated_keys,
            container_spans,
            &self.touched_window_counts,
        )
        .map(|stats| stats.lookup_prefers_roaring())
        .unwrap_or(false)
    }

    fn preferred_filter(
        &self,
        estimated_keys: usize,
        enable_roaring: bool,
        bloom_false_positive_rate: f64,
    ) -> BatchKeyFilter {
        if !self.can_use_roaring(enable_roaring) {
            debug!(
                enable_roaring,
                roaring_range_fits = self.roaring_range_fits(),
                min = ?self.min.as_ref(),
                max = ?self.max.as_ref(),
                estimated_keys,
                "filter predictor: roaring not available, using Bloom",
            );
            return BatchKeyFilter::new_bloom(estimated_keys, bloom_false_positive_rate);
        }
        if self.predict_lookup_prefers_roaring(estimated_keys) {
            debug!(estimated_keys, "filter predictor: chose roaring bitmap");
            BatchKeyFilter::new_roaring_u32(self.min.as_ref())
        } else {
            debug!(
                estimated_keys,
                "filter predictor: predictor prefers Bloom over roaring",
            );
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
        //   plan proves the batch range fits in `u32` and the lookup
        //   predictor says roaring should beat Bloom. If the predictor cannot
        //   run, fall back to Bloom.
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
                debug!(
                    "filter predictor: Bloom disabled, roaring available — using roaring bitmap",
                );
                Some(BatchKeyFilter::new_roaring_u32(filter_plan.min.as_ref()))
            }
            (None, _) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{BatchKeyFilter, FilterPlan};
    use crate::{
        DynZWeight, Runtime, ZWeight,
        circuit::CircuitConfig,
        dynamic::{DynData, Erase},
        trace::{Batch, BatchReader, BatchReaderFactories, Builder, VecWSet, VecWSetFactories},
    };

    fn build_vec_wset_u32_keys(
        keys: impl IntoIterator<Item = u32>,
    ) -> VecWSet<DynData, DynZWeight> {
        let keys: Vec<u32> = keys.into_iter().collect();
        let factories = VecWSetFactories::new::<u32, (), ZWeight>();
        let mut builder = <VecWSet<DynData, DynZWeight> as Batch>::Builder::with_capacity(
            &factories,
            keys.len(),
            0,
        );

        for key in keys {
            let weight: ZWeight = 1;
            builder.push_val_diff(&(), weight.erase());
            builder.push_key(key.erase());
        }

        builder.done()
    }

    fn build_vec_wset_u32(keys: std::ops::Range<u32>) -> VecWSet<DynData, DynZWeight> {
        build_vec_wset_u32_keys(keys)
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

    #[test]
    fn filter_plan_prefers_roaring_for_dense_range() {
        with_roaring_enabled(|| {
            let filter_plan =
                FilterPlan::from_bounds((&0u32) as &DynData, (&49_999u32) as &DynData);
            assert!(matches!(
                FilterPlan::decide_filter(Some(&filter_plan), 50_000),
                Some(BatchKeyFilter::RoaringU32(_))
            ));
        });
    }

    #[test]
    fn filter_plan_prefers_bloom_for_sparse_wide_range() {
        with_roaring_enabled(|| {
            let max = (49_999u32) << 16;
            let filter_plan = FilterPlan::from_bounds((&0u32) as &DynData, (&max) as &DynData);

            assert!(matches!(
                FilterPlan::decide_filter(Some(&filter_plan), 50_000),
                Some(BatchKeyFilter::Bloom(_))
            ));
        });
    }

    #[test]
    fn filter_plan_from_batches_prefers_roaring() {
        with_roaring_enabled(|| {
            let batch1 = build_vec_wset_u32(0..25_000);
            let batch2 = build_vec_wset_u32(25_000..50_000);
            let filter_plan = FilterPlan::from_batches([&batch1, &batch2])
                .expect("dense roaring-compatible batches should build a plan");

            assert!(matches!(
                FilterPlan::decide_filter(
                    Some(&filter_plan),
                    batch1.key_count() + batch2.key_count()
                ),
                Some(BatchKeyFilter::RoaringU32(_))
            ));
        });
    }

    #[test]
    fn batches_prefer_roaring_for_dense_sparse_batches() {
        with_roaring_enabled(|| {
            let batch1 = build_vec_wset_u32_keys(
                [0u32, 30_000u32]
                    .into_iter()
                    .flat_map(|window| (0..5_000).map(move |low| (window << 16) + low)),
            );
            let batch2 = build_vec_wset_u32_keys(
                [10_000u32, 20_000u32]
                    .into_iter()
                    .flat_map(|window| (0..5_000).map(move |low| (window << 16) + low)),
            );
            let max = (30_000u32 << 16) + 4_999;
            let span_only_plan = FilterPlan::from_bounds((&0u32) as &DynData, (&max) as &DynData);
            assert!(matches!(
                FilterPlan::decide_filter(
                    Some(&span_only_plan),
                    batch1.key_count() + batch2.key_count()
                ),
                Some(BatchKeyFilter::Bloom(_))
            ));

            let filter_plan = FilterPlan::from_batches([&batch1, &batch2])
                .expect("wide-but-holey roaring-compatible batches should build a plan");

            assert!(matches!(
                FilterPlan::decide_filter(
                    Some(&filter_plan),
                    batch1.key_count() + batch2.key_count()
                ),
                Some(BatchKeyFilter::RoaringU32(_))
            ));
        });
    }
}
