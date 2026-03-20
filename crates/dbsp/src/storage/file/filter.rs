use super::{AnyFactories, BLOOM_FILTER_SEED};
use crate::{
    dynamic::DataTrait,
    storage::tracking_bloom_filter::{BloomFilterStats, TrackingBloomFilter},
};
use fastbloom::BloomFilter;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use std::{
    io,
    mem::size_of_val,
    sync::atomic::{AtomicUsize, Ordering},
};

/// Configures which per-batch key filter implementation should be used.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BatchKeyFilterKind {
    /// Use the existing Bloom filter path.
    #[default]
    Bloom,

    /// Use an exact roaring bitmap when the batch key type can be mapped
    /// injectively into a `u32` filter domain.
    ///
    /// Unsupported key types fall back to the existing Bloom filter behavior.
    RoaringU32,
}

/// Result of probing a batch key filter.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BatchKeyFilterProbe {
    /// The key is definitely not present.
    Absent,

    /// The key may be present.
    MaybePresent,

    /// The key is definitely present.
    Present,
}

impl BatchKeyFilterProbe {
    /// Returns true if the filter result requires checking the batch contents.
    pub(crate) fn may_contain(self) -> bool {
        !matches!(self, Self::Absent)
    }
}

/// In-memory representation of the per-batch key filter.
#[derive(Debug)]
pub enum BatchKeyFilter {
    /// Probabilistic Bloom filter over key hashes.
    Bloom(TrackingBloomFilter),

    /// Exact roaring bitmap for key types that support exact `u32` encoding.
    RoaringU32(TrackingRoaringBitmap),
}

impl BatchKeyFilter {
    pub(crate) fn new(
        requested_kind: BatchKeyFilterKind,
        estimated_keys: usize,
        key_factories: &AnyFactories,
        bloom_false_positive_rate: Option<f64>,
    ) -> Option<Self> {
        match requested_kind {
            BatchKeyFilterKind::Bloom => bloom_false_positive_rate.map(|rate| {
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
            }),
            BatchKeyFilterKind::RoaringU32 if key_factories.supports_roaring_u32() => {
                Some(Self::RoaringU32(TrackingRoaringBitmap::default()))
            }
            BatchKeyFilterKind::RoaringU32 => bloom_false_positive_rate.map(|rate| {
                Self::Bloom(TrackingBloomFilter::new(
                    BloomFilter::with_false_pos(rate)
                        .seed(&BLOOM_FILTER_SEED)
                        .expected_items(estimated_keys.max(64)),
                ))
            }),
        }
    }

    pub(crate) fn kind(&self) -> BatchKeyFilterKind {
        match self {
            Self::Bloom(_) => BatchKeyFilterKind::Bloom,
            Self::RoaringU32(_) => BatchKeyFilterKind::RoaringU32,
        }
    }

    pub(crate) fn from_bloom_parts(num_hashes: u32, data: Vec<u64>) -> Self {
        Self::Bloom(TrackingBloomFilter::new(
            BloomFilter::from_vec(data)
                .seed(&BLOOM_FILTER_SEED)
                .hashes(num_hashes),
        ))
    }

    pub(crate) fn from_roaring_bytes(data: &[u8]) -> io::Result<Self> {
        TrackingRoaringBitmap::deserialize_from(data).map(Self::RoaringU32)
    }

    pub(crate) fn is_exact(&self) -> bool {
        matches!(self, Self::RoaringU32(_))
    }

    pub(crate) fn stats(&self) -> BloomFilterStats {
        match self {
            Self::Bloom(filter) => filter.stats(),
            Self::RoaringU32(filter) => filter.stats(),
        }
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
                let key = key.as_roaring_u32();
                debug_assert!(
                    key.is_some(),
                    "roaring-u32 filter was selected for an unsupported key type"
                );
                if let Some(key) = key {
                    filter.insert(key);
                }
            }
        }
    }

    pub(crate) fn probe_key<K>(&self, key: &K, hash: Option<u64>) -> BatchKeyFilterProbe
    where
        K: DataTrait + ?Sized,
    {
        match self {
            Self::Bloom(filter) => {
                if filter.contains_hash(hash.unwrap_or_else(|| key.default_hash())) {
                    BatchKeyFilterProbe::MaybePresent
                } else {
                    BatchKeyFilterProbe::Absent
                }
            }
            Self::RoaringU32(filter) => match key.as_roaring_u32() {
                Some(key) => {
                    if filter.contains(key) {
                        BatchKeyFilterProbe::Present
                    } else {
                        BatchKeyFilterProbe::Absent
                    }
                }
                None => BatchKeyFilterProbe::MaybePresent,
            },
        }
    }

    /// Hash-only membership checks are only meaningful for Bloom filters.
    ///
    /// Exact filters need the original key value, so they conservatively report
    /// "maybe present" here.
    pub(crate) fn maybe_contains_hash(&self, hash: u64) -> bool {
        match self {
            Self::Bloom(filter) => filter.contains_hash(hash),
            Self::RoaringU32(_) => true,
        }
    }
}

/// Roaring bitmap wrapper that tracks hit/miss counts during membership probes.
#[derive(Debug, Default)]
pub struct TrackingRoaringBitmap {
    bitmap: RoaringBitmap,
    hits: AtomicUsize,
    misses: AtomicUsize,
}

impl TrackingRoaringBitmap {
    pub(crate) fn new(bitmap: RoaringBitmap) -> Self {
        Self {
            bitmap,
            hits: AtomicUsize::new(0),
            misses: AtomicUsize::new(0),
        }
    }

    pub(crate) fn insert(&mut self, value: u32) {
        self.bitmap.insert(value);
    }

    pub(crate) fn contains(&self, value: u32) -> bool {
        let is_hit = self.bitmap.contains(value);
        if is_hit {
            self.hits.fetch_add(1, Ordering::Release);
        } else {
            self.misses.fetch_add(1, Ordering::Release);
        }
        is_hit
    }

    pub(crate) fn stats(&self) -> BloomFilterStats {
        BloomFilterStats {
            size_byte: size_of_val(&self.bitmap) + self.bitmap.serialized_size(),
            hits: self.hits.load(Ordering::Acquire),
            misses: self.misses.load(Ordering::Acquire),
        }
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
    use super::{BatchKeyFilterProbe, TrackingRoaringBitmap};
    use crate::storage::tracking_bloom_filter::BloomFilterStats;

    #[test]
    fn tracking_roaring_bitmap_stats() {
        let mut filter = TrackingRoaringBitmap::default();
        filter.insert(1);
        filter.insert(3);

        assert!(filter.contains(1));
        assert!(!filter.contains(2));
        assert_eq!(
            filter.stats(),
            BloomFilterStats {
                size_byte: std::mem::size_of::<roaring::RoaringBitmap>() + filter.serialized_size(),
                hits: 1,
                misses: 1,
            }
        );
    }

    #[test]
    fn exact_probe_semantics() {
        let mut filter = TrackingRoaringBitmap::default();
        filter.insert(7);

        assert!(matches!(
            if filter.contains(7) {
                BatchKeyFilterProbe::Present
            } else {
                BatchKeyFilterProbe::Absent
            },
            BatchKeyFilterProbe::Present
        ));
    }
}
