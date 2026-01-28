use fastbloom::BloomFilter;
use std::iter::Sum;
use std::ops::{Add, AddAssign};
use std::sync::atomic::{AtomicUsize, Ordering};

/// Bloom filter which tracks the number of hits and misses when lookups are performed.
/// It implements the subset of [`BloomFilter`] functions that are used by Feldera storage.
#[derive(Debug)]
pub struct TrackingBloomFilter {
    /// Underlying Bloom filter.
    bloom_filter: BloomFilter,
    /// Number of hits.
    hits: AtomicUsize,
    /// Number of misses.
    misses: AtomicUsize,
}

/// Statistics about the Bloom filter.
///
/// The statistics implement addition such that they can be summed (e.g., when you have a spine
/// consisting out of multiple batches, each with their own Bloom filter). However, their addition
/// will lose information about individual sizes, hits, misses and by extension, hit rates.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct BloomFilterStats {
    /// Bloom filter size in bytes.
    pub size_byte: usize,
    /// Number of hits.
    pub hits: usize,
    /// Number of misses.
    pub misses: usize,
}

impl Add for BloomFilterStats {
    type Output = Self;

    fn add(mut self, rhs: Self) -> Self::Output {
        self.add_assign(rhs);
        self
    }
}

impl AddAssign for BloomFilterStats {
    fn add_assign(&mut self, rhs: Self) {
        self.size_byte += rhs.size_byte;
        self.hits += rhs.hits;
        self.misses += rhs.misses;
    }
}

impl Sum for BloomFilterStats {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.fold(Self::default(), Add::add)
    }
}

impl TrackingBloomFilter {
    /// Constructs a tracking Bloom filter which wraps a regular Bloom filter instance.
    /// It is assumed the underlying Bloom filter has not yet been used.
    pub fn new(bloom_filter: BloomFilter) -> Self {
        Self {
            bloom_filter,
            hits: AtomicUsize::new(0),
            misses: AtomicUsize::new(0),
        }
    }

    /// Retrieves statistics.
    pub fn stats(&self) -> BloomFilterStats {
        BloomFilterStats {
            size_byte: size_of_val(&self.bloom_filter) + self.bloom_filter.num_bits() / 8,
            hits: self.hits.load(Ordering::Acquire),
            misses: self.misses.load(Ordering::Acquire),
        }
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
        if is_hit {
            self.hits.fetch_add(1, Ordering::Release);
        } else {
            self.misses.fetch_add(1, Ordering::Release);
        }
        is_hit
    }
}

#[cfg(test)]
mod tests {
    use super::{BloomFilterStats, TrackingBloomFilter};
    use fastbloom::BloomFilter;

    #[test]
    fn tracking_bloom_filter_stats() {
        let mut filter =
            TrackingBloomFilter::new(BloomFilter::with_num_bits(8192).expected_items(100));
        assert_eq!(filter.as_slice(), &[0; 8192 / 64]);
        assert!(filter.num_hashes() >= 1);
        assert_eq!(
            filter.stats(),
            BloomFilterStats {
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
            BloomFilterStats {
                size_byte: 96 + 8192 / 8,
                hits: 1,
                misses: 2,
            }
        );
    }

    #[test]
    fn tracking_bloom_filter_stats_default() {
        assert_eq!(
            BloomFilterStats::default(),
            BloomFilterStats {
                size_byte: 0,
                hits: 0,
                misses: 0,
            }
        );
    }

    #[test]
    fn tracking_bloom_filter_stats_addition() {
        let stats1 = BloomFilterStats {
            size_byte: 123,
            hits: 456,
            misses: 789,
        };
        let stats2 = BloomFilterStats {
            size_byte: 100,
            hits: 200,
            misses: 300,
        };
        let stats3 = BloomFilterStats {
            size_byte: 223,
            hits: 656,
            misses: 1089,
        };
        assert_eq!(stats1 + stats2, stats3);
        assert_eq!(
            vec![stats1, stats2].into_iter().sum::<BloomFilterStats>(),
            stats3
        );
    }
}
