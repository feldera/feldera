use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use crate::storage::backend::{BlockLocation, FileReader};
use crate::storage::file::reader::{Error, read_bloom_filter_module};
use crate::storage::file::{
    FilterStats, TrackingFilterStats, modules_for_rate, rate_for_modules, resident_modules,
};
use arc_swap::ArcSwap;
use feldera_modular_bloom::{
    LoadError, ModularBloomFilter, ModularBloomFilterBuilder, ModuleLayout,
};
use thiserror::Error as ThisError;

/// A handle to a Bloom filter's modules on storage, for reading dropped
/// modules back.
#[derive(Debug)]
pub(crate) enum BloomFilterModuleHandle {
    /// The filter block at `location` in a batch file, whose module 0 starts
    /// `data_offset` bytes into the block.
    File {
        file: Arc<dyn FileReader>,
        location: BlockLocation,
        data_offset: usize,
    },
    /// Modules held in memory, so tests need no file. A failing handle fails
    /// every read. Each read counts in `reads`.
    #[cfg(test)]
    Memory {
        modules: Vec<Arc<[u64]>>,
        fail: bool,
        reads: Arc<std::sync::atomic::AtomicUsize>,
    },
}

impl BloomFilterModuleHandle {
    /// Reads the words of module `index`.
    fn read_module(&self, index: u32, words_per_module: u32) -> Result<Arc<[u64]>, Error> {
        match self {
            Self::File {
                file,
                location,
                data_offset,
            } => {
                read_bloom_filter_module(&**file, *location, *data_offset, words_per_module, index)
            }
            #[cfg(test)]
            Self::Memory {
                modules,
                fail,
                reads,
            } => {
                reads.fetch_add(1, Ordering::Relaxed);
                if *fail {
                    return Err(Error::Storage(
                        crate::storage::backend::StorageError::StdIo {
                            kind: std::io::ErrorKind::Other,
                            operation: "read",
                            path: None,
                        },
                    ));
                }
                Ok(modules[index as usize].clone())
            }
        }
    }
}

/// Attempts to restore a module that may fail before a filter stops trying.
const MAX_RESTORE_FAILURES: u8 = 3;

/// Why a module could not be read back.
#[derive(Debug, ThisError)]
pub(crate) enum RestoreError {
    #[error("reading the module failed: {0}")]
    Read(#[from] Error),
    #[error("the module read back is not the one written: {0}")]
    Module(#[from] LoadError),
}

/// A filter is accumulating keys or answering queries, never both.
///
/// Inserting into a filter that has dropped modules would leave the dropped
/// ones missing keys the resident ones hold, so reloading them later would turn
/// that gap into a false negative. The writer builds and then finalizes; the
/// reader only ever loads.
#[derive(Debug)]
enum State {
    Building(ModularBloomFilterBuilder),
    /// Behind an [`ArcSwap`] so that modules can be dropped and added back
    /// while lookups run on other threads.
    Built(ArcSwap<ModularBloomFilter>),
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
    /// Hits and misses. The size is read live in [`Self::stats`].
    tracking: TrackingFilterStats,
    /// Where dropped modules can be read back from, once the filter is on
    /// storage.
    module_handle: Option<BloomFilterModuleHandle>,
    /// Failed attempts to restore a module. At [`MAX_RESTORE_FAILURES`] the
    /// filter stops trying, so a broken handle is not retried forever.
    restore_failures: AtomicU8,
}

impl TrackingBloomFilter {
    /// Starts a filter that will accept keys.
    pub fn building(layout: ModuleLayout) -> Self {
        Self::new(
            State::Building(ModularBloomFilterBuilder::new(layout)),
            layout,
        )
    }

    /// Wraps a filter read back from storage, which may hold only a prefix of
    /// the modules the layout describes.
    pub fn loaded(filter: ModularBloomFilter) -> Self {
        let layout = *filter.layout();
        Self::new(State::Built(ArcSwap::from_pointee(filter)), layout)
    }

    fn new(state: State, layout: ModuleLayout) -> Self {
        Self {
            state,
            layout,
            tracking: TrackingFilterStats::new(0),
            module_handle: None,
            restore_failures: AtomicU8::new(0),
        }
    }

    /// Shape the filter was written with, whatever is resident.
    pub fn layout(&self) -> &ModuleLayout {
        &self.layout
    }

    /// The filter as it is now, for the writer to serialize.
    ///
    /// Panics before [`Self::finalize`].
    pub fn built(&self) -> Arc<ModularBloomFilter> {
        match &self.state {
            State::Building(_) => panic!("Bloom filter must be finalized before serialization"),
            State::Built(filter) => filter.load_full(),
        }
    }

    /// Modules held in memory, which may be fewer than the layout describes.
    pub fn resident_modules(&self) -> u32 {
        match &self.state {
            State::Building(_) => self.layout.total_modules(),
            State::Built(filter) => filter.load().resident_modules(),
        }
    }

    /// Bytes the modules held in memory occupy.
    pub fn resident_bytes(&self) -> usize {
        match &self.state {
            State::Building(_) => self.layout.total_bytes(),
            State::Built(filter) => filter.load().resident_bytes(),
        }
    }

    /// Retrieves statistics.
    ///
    /// The size is read live, because modules can be dropped and added back at
    /// any time.
    pub fn stats(&self) -> FilterStats {
        let mut stats = self.tracking.stats();
        stats.size_byte = size_of::<Self>() + self.resident_bytes();
        stats
    }

    /// Keeps at most `modules` modules.
    ///
    /// Safe while lookups run on other threads. The kept modules are shared
    /// with the old filter, not copied, and the dropped ones are freed when the
    /// last lookup that still holds the old filter finishes. Does nothing while
    /// the filter is still building.
    ///
    /// Call this only once the filter is written to storage, since the file
    /// must hold every module.
    pub fn evict_to(&self, modules: u32) {
        if let State::Built(filter) = &self.state {
            filter.rcu(|current| {
                if current.resident_modules() > modules {
                    let mut kept = (**current).clone();
                    kept.truncate(modules);
                    Arc::new(kept)
                } else {
                    current.clone()
                }
            });
        }
    }

    /// Lets the filter read its modules back through `handle` after dropping
    /// them.
    pub(crate) fn set_module_handle(&mut self, handle: BloomFilterModuleHandle) {
        self.module_handle = Some(handle);
    }

    /// Whether dropped modules can be read back.
    pub(crate) fn has_module_handle(&self) -> bool {
        self.module_handle.is_some()
    }

    /// Lookups this filter has answered.
    pub(crate) fn probes(&self) -> usize {
        let stats = self.tracking.stats();
        stats.hits + stats.misses
    }

    /// Modules this filter should hold at `configured_rate` when at most
    /// `module_max` are allowed.
    ///
    /// This is what loading the filter from storage would keep: at the
    /// configured rate while `module_max` allows every module that rate keeps,
    /// otherwise at the rate `module_max` modules give. So a filter that
    /// measures more accurate than nominal holds fewer modules, exactly as it
    /// would after a restart.
    pub(crate) fn target_modules(&self, configured_rate: f64, module_max: u32) -> u32 {
        let rate = if module_max >= modules_for_rate(configured_rate) {
            configured_rate
        } else {
            rate_for_modules(module_max)
        };
        match &self.state {
            State::Building(_) => self.layout.total_modules(),
            State::Built(filter) => resident_modules(&self.layout, filter.load().density(), rate),
        }
    }

    /// Reads the next dropped module back from storage and adds it.
    ///
    /// Safe while lookups run on other threads.
    ///
    /// # Returns
    ///
    /// - The bytes added.
    /// - `Ok(0)` when there is nothing to add: the filter holds every module,
    ///   has no module handle, or changed while the module was read.
    /// - An error when the read fails or the module does not match the bits
    ///   recorded for it. A failed read is tried again on later calls, up to
    ///   [`MAX_RESTORE_FAILURES`] attempts in all, since storage errors can be
    ///   transient. A module that does not match will not match next time
    ///   either, so after one, later calls return `Ok(0)`.
    pub(crate) fn restore_one(&self) -> Result<usize, RestoreError> {
        let (State::Built(filter), Some(handle)) = (&self.state, &self.module_handle) else {
            return Ok(0);
        };
        if self.restore_failures.load(Ordering::Relaxed) >= MAX_RESTORE_FAILURES {
            return Ok(0);
        }
        let current = filter.load_full();
        let index = current.resident_modules();
        if index >= self.layout.total_modules() {
            return Ok(0);
        }

        let restored = (|| -> Result<_, RestoreError> {
            let words = handle.read_module(index, self.layout.words_per_module())?;
            let mut restored = (*current).clone();
            restored.push_module(words)?;
            Ok(restored)
        })();
        let restored = restored.inspect_err(|error| {
            let failures = match error {
                RestoreError::Read(_) => 1,
                RestoreError::Module(_) => MAX_RESTORE_FAILURES,
            };
            self.restore_failures.fetch_add(failures, Ordering::Relaxed);
        })?;

        // Eviction may have run while the module was read. Then the module
        // read no longer follows the last resident one, so keep what is there.
        let previous = filter.compare_and_swap(&current, Arc::new(restored));
        Ok(if Arc::ptr_eq(&previous, &current) {
            self.layout.bytes_per_module()
        } else {
            0
        })
    }

    /// Stops accepting keys, so the filter can be queried and serialized.
    pub fn finalize(&mut self) {
        if let State::Building(builder) = &mut self.state {
            let builder = std::mem::replace(builder, ModularBloomFilterBuilder::new(self.layout));
            self.state = State::Built(ArcSwap::from_pointee(builder.finish()));
        }
    }

    /// Adds a key hash. Only valid before [`Self::finalize`].
    pub fn insert_hash(&mut self, hash: u64) {
        match &mut self.state {
            State::Building(builder) => builder.insert_hash(hash),
            State::Built(_) => panic!("cannot insert into a finalized Bloom filter"),
        }
    }

    /// Returns whether `hash` might have been inserted, counting the outcome.
    pub fn contains_hash(&self, hash: u64) -> bool {
        let is_hit = match &self.state {
            State::Building(_) => true,
            State::Built(filter) => filter.load().contains_hash(hash),
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
        let words: Vec<u64> = builder.built().modules().concat();

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
