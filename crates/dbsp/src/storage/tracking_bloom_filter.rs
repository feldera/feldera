use crate::storage::filter_stats::{FilterStats, TrackingFilterStats};
use fastbloom::BloomFilter;

/// Bloom filter which tracks the number of hits and misses when lookups are performed.
/// It implements the subset of [`BloomFilter`] functions that are used by Feldera storage.
#[derive(Debug)]
pub struct TrackingBloomFilter {
    /// Underlying Bloom filter.
    bloom_filter: BloomFilter,
    tracking: TrackingFilterStats,
}

impl TrackingBloomFilter {
    /// Constructs a tracking Bloom filter which wraps a regular Bloom filter instance.
    /// It is assumed the underlying Bloom filter has not yet been used.
    pub fn new(bloom_filter: BloomFilter) -> Self {
        let size_byte = size_of_val(&bloom_filter) + bloom_filter.num_bits() / 8;
        Self {
            tracking: TrackingFilterStats::new(size_byte),
            bloom_filter,
        }
    }

    /// Retrieves statistics.
    pub fn stats(&self) -> FilterStats {
        self.tracking.stats()
    }

    /// See [`BloomFilter::num_hashes`].
    pub fn num_hashes(&self) -> u32 {
        self.bloom_filter.num_hashes()
    }

    /// See [`BloomFilter::insert_hash`].
    pub fn insert_hash(&mut self, hash: u64) -> bool {
        self.bloom_filter.insert_hash(hash)
    }

    /// See [`BloomFilter::as_slice`].
    pub fn as_slice(&self) -> &[u64] {
        self.bloom_filter.as_slice()
    }

    /// See [`BloomFilter::contains_hash`].
    /// It additionally counts the hits or misses, before returning.
    pub fn contains_hash(&self, hash: u64) -> bool {
        let is_hit = self.bloom_filter.contains_hash(hash);
        self.tracking.record(is_hit);
        is_hit
    }
}

#[cfg(test)]
mod tests {
    use super::TrackingBloomFilter;
    use crate::storage::filter_stats::FilterStats;
    use fastbloom::BloomFilter;

    #[test]
    fn tracking_bloom_filter_stats() {
        let mut filter =
            TrackingBloomFilter::new(BloomFilter::with_num_bits(8192).expected_items(100));
        assert_eq!(filter.as_slice(), &[0; 8192 / 64]);
        assert!(filter.num_hashes() >= 1);
        assert_eq!(
            filter.stats(),
            FilterStats {
                size_byte: 96 + 8192 / 8,
                hits: 0,
                misses: 0,
            }
        );
        filter.insert_hash(123);
        assert!(filter.contains_hash(123));
        assert!(!filter.contains_hash(456));
        assert!(!filter.contains_hash(789));
        assert_eq!(
            filter.stats(),
            FilterStats {
                size_byte: 96 + 8192 / 8,
                hits: 1,
                misses: 2,
            }
        );
    }

    #[test]
    fn tracking_bloom_filter_stats_default() {
        assert_eq!(
            FilterStats::default(),
            FilterStats {
                size_byte: 0,
                hits: 0,
                misses: 0,
            }
        );
    }

    #[test]
    fn tracking_bloom_filter_stats_addition() {
        let stats1 = FilterStats {
            size_byte: 123,
            hits: 456,
            misses: 789,
        };
        let stats2 = FilterStats {
            size_byte: 100,
            hits: 200,
            misses: 300,
        };
        let stats3 = FilterStats {
            size_byte: 223,
            hits: 656,
            misses: 1089,
        };
        assert_eq!(stats1 + stats2, stats3);
        assert_eq!(
            vec![stats1, stats2].into_iter().sum::<FilterStats>(),
            stats3
        );
    }
}
