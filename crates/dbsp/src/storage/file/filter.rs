use super::BLOOM_FILTER_SEED;
use crate::{
    dynamic::DataTrait,
    storage::{
        filter_stats::{FilterStats, TrackingFilterStats},
        tracking_bloom_filter::TrackingBloomFilter,
    },
};
use fastbloom::BloomFilter;
use roaring::RoaringBitmap;
use std::{io, mem::size_of_val};

/// In-memory representation of the per-batch key filter.
#[derive(Debug)]
pub enum BatchKeyFilter {
    /// Probabilistic Bloom filter over key hashes.
    Bloom(TrackingBloomFilter),

    /// Exact roaring bitmap for key types that support exact `u32` encoding.
    RoaringU32(TrackingRoaringBitmap),
}

impl BatchKeyFilter {
    pub(crate) fn new_bloom(
        estimated_keys: usize,
        bloom_false_positive_rate: Option<f64>,
    ) -> Option<Self> {
        bloom_false_positive_rate.map(|rate| {
            Self::Bloom(TrackingBloomFilter::new(
                BloomFilter::with_false_pos(rate)
                    .seed(&BLOOM_FILTER_SEED)
                    .expected_items({
                        // `.max(64)` works around a fastbloom bug that hangs when the
                        // expected number of items is zero (see
                        // <https://github.com/tomtomwombat/fastbloom/issues/17>).
                        estimated_keys.max(64)
                    }),
            ))
        })
    }

    pub(crate) fn new_roaring_u32() -> Self {
        Self::RoaringU32(TrackingRoaringBitmap::default())
    }

    pub(crate) fn from_bloom_parts(num_hashes: u32, data: Vec<u64>) -> Self {
        Self::Bloom(TrackingBloomFilter::new(
            BloomFilter::from_vec(data)
                .seed(&BLOOM_FILTER_SEED)
                .hashes(num_hashes),
        ))
    }

    pub(crate) fn from_roaring_u32_bytes(data: &[u8]) -> io::Result<Self> {
        TrackingRoaringBitmap::deserialize_from(data).map(Self::RoaringU32)
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
                if let Some(key) = key.roaring_u32_value() {
                    filter.insert(key);
                } else {
                    debug_assert!(
                        false,
                        "roaring-u32 filter was selected for an unsupported key type"
                    );
                }
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

/// Roaring bitmap wrapper that tracks hit/miss counts during membership probes.
#[derive(Debug)]
pub struct TrackingRoaringBitmap {
    bitmap: RoaringBitmap,
    tracking: TrackingFilterStats,
}

impl Default for TrackingRoaringBitmap {
    fn default() -> Self {
        Self::new(RoaringBitmap::new())
    }
}

impl TrackingRoaringBitmap {
    pub(crate) fn new(bitmap: RoaringBitmap) -> Self {
        let size_byte = size_of_val(&bitmap) + bitmap.serialized_size();
        Self {
            bitmap,
            tracking: TrackingFilterStats::new(size_byte),
        }
    }

    pub(crate) fn insert(&mut self, value: u32) {
        self.bitmap.insert(value);
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
        self.tracking
            .set_size_byte(size_of_val(&self.bitmap) + self.bitmap.serialized_size());
    }

    pub(crate) fn contains(&self, value: u32) -> bool {
        let is_hit = self.bitmap.contains(value);
        self.tracking.record(is_hit);
        is_hit
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

    pub(crate) fn deserialize_from<R: io::Read>(reader: R) -> io::Result<Self> {
        Ok(Self::new(RoaringBitmap::deserialize_from(reader)?))
    }
}

#[cfg(test)]
mod tests {
    use super::TrackingRoaringBitmap;
    use crate::storage::filter_stats::FilterStats;

    #[test]
    fn tracking_roaring_bitmap_stats() {
        let mut filter = TrackingRoaringBitmap::default();
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
