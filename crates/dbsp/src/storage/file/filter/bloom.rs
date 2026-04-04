use crate::storage::tracking_bloom_filter::TrackingBloomFilter;
use fastbloom::BloomFilter;

use super::super::BLOOM_FILTER_SEED;

pub(super) fn new_bloom_filter(
    estimated_keys: usize,
    bloom_false_positive_rate: f64,
) -> TrackingBloomFilter {
    TrackingBloomFilter::new(
        BloomFilter::with_false_pos(bloom_false_positive_rate)
            .seed(&BLOOM_FILTER_SEED)
            .expected_items({
                // `.max(64)` works around a fastbloom bug that hangs when the
                // expected number of items is zero (see
                // <https://github.com/tomtomwombat/fastbloom/issues/17>).
                estimated_keys.max(64)
            }),
    )
}

pub(super) fn deserialize_bloom_filter(num_hashes: u32, data: Vec<u64>) -> TrackingBloomFilter {
    TrackingBloomFilter::new(
        BloomFilter::from_vec(data)
            .seed(&BLOOM_FILTER_SEED)
            .hashes(num_hashes),
    )
}
