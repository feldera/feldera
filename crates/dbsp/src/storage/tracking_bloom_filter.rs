use crate::storage::file::{FilterStats, TrackingFilterStats};
use feldera_modular_bloom::{
    ModularBloomFilter, ModularBloomFilterBuilder, ModuleDensity, ModuleLayout,
};

/// A filter is accumulating keys or answering queries, never both.
///
/// Inserting into a filter that has dropped modules would leave the dropped
/// ones missing keys the resident ones hold, so reloading them later would turn
/// that gap into a false negative. The writer builds and then finalizes; the
/// reader only ever loads.
#[derive(Debug)]
enum State {
    Building(ModularBloomFilterBuilder),
    Built(ModularBloomFilter),
}

/// Modular Bloom filter which tracks the number of hits and misses when lookups
/// are performed.
///
/// A filter written before modular filters existed loads as a single module, so
/// it shares this code path entirely. It has only one rung and therefore cannot
/// shed accuracy, which is the one thing a multi-module filter can do.
#[derive(Debug)]
pub struct TrackingBloomFilter {
    state: State,
    layout: ModuleLayout,
    tracking: TrackingFilterStats,
}

impl TrackingBloomFilter {
    /// Starts a filter that will accept keys.
    pub fn building(layout: ModuleLayout) -> Self {
        Self {
            state: State::Building(ModularBloomFilterBuilder::new(layout)),
            layout,
            tracking: TrackingFilterStats::new(Self::size_byte(layout.total_bytes())),
        }
    }

    /// Wraps a filter read back from storage, which may hold only a prefix of
    /// the modules the layout describes.
    pub fn loaded(filter: ModularBloomFilter) -> Self {
        let layout = *filter.layout();
        Self {
            tracking: TrackingFilterStats::new(Self::size_byte(filter.resident_bytes())),
            state: State::Built(filter),
            layout,
        }
    }

    fn size_byte(bits_bytes: usize) -> usize {
        size_of::<Self>() + bits_bytes
    }

    /// Shape the filter was written with, whatever is resident.
    pub fn layout(&self) -> &ModuleLayout {
        &self.layout
    }

    /// Bits set per module, for the writer to record alongside the words.
    ///
    /// Panics before [`Self::finalize`], which is when the counts settle.
    pub fn density(&self) -> &ModuleDensity {
        match &self.state {
            State::Building(_) => panic!("Bloom filter must be finalized before its density"),
            State::Built(filter) => filter.density(),
        }
    }

    /// Modules held in memory, which may be fewer than the layout describes.
    pub fn resident_modules(&self) -> u32 {
        match &self.state {
            State::Building(_) => self.layout.total_modules(),
            State::Built(filter) => filter.resident_modules(),
        }
    }

    /// Retrieves statistics.
    pub fn stats(&self) -> FilterStats {
        self.tracking.stats()
    }

    /// Stops accepting keys, so the filter can be queried and serialized.
    pub fn finalize(&mut self) {
        if let State::Building(builder) = &mut self.state {
            let builder = std::mem::replace(builder, ModularBloomFilterBuilder::new(self.layout));
            self.state = State::Built(builder.finish());
        }
    }

    /// Adds a key hash. Only valid before [`Self::finalize`].
    pub fn insert_hash(&mut self, hash: u64) {
        match &mut self.state {
            State::Building(builder) => builder.insert_hash(hash),
            State::Built(_) => panic!("cannot insert into a finalized Bloom filter"),
        }
    }

    /// Words of each resident module, in order, for serialization.
    pub fn module_words(&self) -> Vec<&[u64]> {
        match &self.state {
            State::Building(_) => panic!("Bloom filter must be finalized before serialization"),
            State::Built(filter) => filter.modules(),
        }
    }

    /// Returns whether `hash` might have been inserted, counting the outcome.
    pub fn contains_hash(&self, hash: u64) -> bool {
        let is_hit = match &self.state {
            State::Building(_) => true,
            State::Built(filter) => filter.contains_hash(hash),
        };
        self.tracking.record(is_hit);
        is_hit
    }
}

#[cfg(test)]
mod tests {
    use super::TrackingBloomFilter;
    use crate::storage::file::FilterStats;
    use feldera_modular_bloom::ModuleLayout;

    fn layout() -> ModuleLayout {
        ModuleLayout::for_keys(100, 1e-4, 4).unwrap()
    }

    #[test]
    fn tracking_bloom_filter_stats() {
        let mut filter = TrackingBloomFilter::building(layout());
        filter.insert_hash(123);
        filter.finalize();

        assert!(filter.contains_hash(123));
        assert!(!filter.contains_hash(456));
        assert!(!filter.contains_hash(789));

        let stats = filter.stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 2);
        assert!(stats.size_byte >= layout().total_bytes());
    }

    /// A filter loaded with fewer modules reports the memory it actually holds,
    /// not the memory its layout describes.
    #[test]
    fn loaded_filter_reports_resident_size() {
        let layout = layout();
        let mut builder = TrackingBloomFilter::building(layout);
        for i in 0..100u64 {
            builder.insert_hash(i.wrapping_mul(0x9E37_79B9_7F4A_7C15));
        }
        builder.finalize();
        let words: Vec<u64> = builder.module_words().concat();

        let per = layout.words_per_module() as usize;
        let half =
            feldera_modular_bloom::ModularBloomFilter::from_modules(layout, &words[..2 * per])
                .unwrap();
        let half = TrackingBloomFilter::loaded(half);

        assert_eq!(half.resident_modules(), 2);
        assert!(half.stats().size_byte < builder.stats().size_byte);
        for i in 0..100u64 {
            assert!(half.contains_hash(i.wrapping_mul(0x9E37_79B9_7F4A_7C15)));
        }
    }

    #[test]
    fn tracking_bloom_filter_stats_default() {
        assert_eq!(
            FilterStats::default(),
            FilterStats {
                size_byte: 0,
                hits: 0,
                misses: 0
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
