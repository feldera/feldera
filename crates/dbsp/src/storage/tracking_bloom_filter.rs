use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex, Weak};

use crate::storage::backend::{BlockLocation, FileReader};
use crate::storage::file::reader::{Error, read_bloom_filter_module};
use crate::storage::file::{FilterStats, LocalFilterCounts, TrackingFilterStats, target_modules};
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

#[cfg(test)]
thread_local! {
    /// Runs inside the next in-memory module read on this thread, so a test
    /// can change the filter while a module is read.
    pub(crate) static DURING_MEMORY_READ: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
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
                if let Some(during) = DURING_MEMORY_READ.with(|hook| hook.borrow_mut().take()) {
                    during();
                }
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

/// Attempts in a row to restore a module that may fail before a filter stops
/// trying.
pub(crate) const MAX_RESTORE_FAILURES: u8 = 3;

/// Smallest filter worth enrolling, in bytes at the configured rate.
///
/// Filters below this hold too little memory to be worth the accuracy they
/// would lose. The floor is low on purpose: a spine can hold much of its filter
/// memory in many mid-size batches, for example 6 batches of 320,000 keys,
/// each with about 750 KiB of filter at a rate of 1e-4, and less at coarser
/// rates.
pub(crate) const MIN_EVICTABLE_BYTES: usize = 64 << 10;

/// Whether a filter that holds `full_bytes` at the configured rate is large
/// enough to enroll. Loading a filter under pressure uses this too, because
/// only an enrolled filter gets its modules back.
pub(crate) fn is_evictable_size(full_bytes: usize) -> bool {
    full_bytes >= MIN_EVICTABLE_BYTES
}

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
    /// Built, and never evicted, so a probe reads it with no atomic load.
    Fixed(Arc<ModularBloomFilter>),
    /// Built and enrolled for eviction. Behind an [`ArcSwap`] so that modules
    /// can be dropped and added back while lookups run on other threads.
    Evictable(ArcSwap<ModularBloomFilter>),
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
    /// Failed attempts in a row to restore a module. A successful restore
    /// resets it. At [`MAX_RESTORE_FAILURES`] the filter stops trying, so a
    /// broken handle is not retried forever.
    restore_failures: AtomicU8,
    /// The filter as it was before the oldest eviction whose dropped modules
    /// a snapshot still holds, so the manager can tell how much memory open
    /// cursors keep. See [`Self::pinned_bytes`].
    released: Mutex<Weak<ModularBloomFilter>>,
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
        Self::new(State::Fixed(Arc::new(filter)), layout)
    }

    fn new(state: State, layout: ModuleLayout) -> Self {
        Self {
            state,
            layout,
            tracking: TrackingFilterStats::new(0),
            module_handle: None,
            restore_failures: AtomicU8::new(0),
            released: Mutex::new(Weak::new()),
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
            State::Fixed(filter) => filter.clone(),
            State::Evictable(filter) => filter.load_full(),
        }
    }

    /// Runs `f` on the built filter, or returns `None` while building.
    fn with_built<R>(&self, f: impl FnOnce(&ModularBloomFilter) -> R) -> Option<R> {
        match &self.state {
            State::Building(_) => None,
            State::Fixed(filter) => Some(f(filter)),
            State::Evictable(filter) => Some(f(&filter.load())),
        }
    }

    /// Modules held in memory, which may be fewer than the layout describes.
    pub fn resident_modules(&self) -> u32 {
        self.with_built(ModularBloomFilter::resident_modules)
            .unwrap_or(self.layout.total_modules())
    }

    /// Bytes the modules held in memory occupy.
    pub fn resident_bytes(&self) -> usize {
        self.with_built(ModularBloomFilter::resident_bytes)
            .unwrap_or(self.layout.total_bytes())
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

    /// Stops accepting keys, so the filter can be queried and serialized.
    pub fn finalize(&mut self) {
        if matches!(self.state, State::Building(_)) {
            // A filter with no module holds the place while the builder moves
            // out, so finishing allocates nothing more.
            let empty = ModularBloomFilter::from_modules(self.layout, &[])
                .expect("a filter with no module always loads");
            let State::Building(builder) =
                std::mem::replace(&mut self.state, State::Fixed(Arc::new(empty)))
            else {
                unreachable!()
            };
            self.state = State::Fixed(Arc::new(builder.finish()));
        }
    }

    /// Adds a key hash. Only valid before [`Self::finalize`].
    pub fn insert_hash(&mut self, hash: u64) {
        match &mut self.state {
            State::Building(builder) => builder.insert_hash(hash),
            State::Fixed(_) | State::Evictable(_) => {
                panic!("cannot insert into a finalized Bloom filter")
            }
        }
    }

    /// The filter as it is now, for one reader to probe many times.
    ///
    /// A probe through the snapshot needs no atomic operation: an evictable
    /// filter pays one [`ArcSwap`] load here, and the snapshot counts hits and
    /// misses itself. Modules evicted after this call stay in memory until the
    /// snapshot drops, and modules read back after it are not seen, so a
    /// snapshot must live only as long as a cursor.
    ///
    /// Returns `None` while the filter is building, when it admits every key.
    pub fn snapshot(&self) -> Option<BloomFilterSnapshot<'_>> {
        let filter = match &self.state {
            State::Building(_) => return None,
            State::Fixed(filter) => SnapshotFilter::Fixed(filter),
            State::Evictable(filter) => SnapshotFilter::Evictable(filter.load_full()),
        };
        Some(BloomFilterSnapshot {
            filter,
            counts: LocalFilterCounts::new(&self.tracking),
        })
    }

    /// Returns whether `hash` might have been inserted, counting the outcome.
    pub fn contains_hash(&self, hash: u64) -> bool {
        let is_hit = self
            .with_built(|filter| filter.contains_hash(hash))
            .unwrap_or(true);
        self.tracking.record(is_hit);
        is_hit
    }
}

// Eviction and restore, for filters the Bloom filter manager enrolls.
impl TrackingBloomFilter {
    /// Lets [`Self::evict_to`] and [`Self::restore_one`] change the filter
    /// while lookups run on other threads.
    ///
    /// Only an evictable filter pays for an [`ArcSwap`] load when a cursor
    /// takes a snapshot of it, so call this only for filters the manager
    /// enrolls. Does nothing while the filter is building.
    pub(crate) fn make_evictable(&mut self) {
        if let State::Fixed(filter) = &self.state {
            let filter = Arc::clone(filter);
            self.state = State::Evictable(ArcSwap::new(filter));
        }
    }

    /// Modules this filter should hold at `configured_rate` when at most
    /// `max_modules` are allowed.
    ///
    /// This is what loading the filter from storage would keep: at the
    /// configured rate while `max_modules` allows every module that rate keeps,
    /// otherwise at the rate `max_modules` modules give. So a filter that
    /// measures more accurate than nominal holds fewer modules, exactly as it
    /// would after a restart.
    pub(crate) fn target_modules(&self, configured_rate: f64, max_modules: u32) -> u32 {
        self.with_built(|filter| {
            target_modules(&self.layout, filter.density(), configured_rate, max_modules)
        })
        .unwrap_or(self.layout.total_modules())
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
        if let State::Evictable(filter) = &self.state {
            let previous = filter.rcu(|current| {
                if current.resident_modules() > modules {
                    let mut kept = (**current).clone();
                    kept.truncate(modules);
                    Arc::new(kept)
                } else {
                    current.clone()
                }
            });
            if previous.resident_modules() > modules {
                let mut released = self.released.lock().unwrap();
                // Keep the oldest filter a snapshot still holds, since it holds
                // the most modules.
                if released.strong_count() == 0 {
                    *released = Arc::downgrade(&previous);
                }
            }
        }
    }

    /// Bytes of modules this filter dropped that open snapshots, and so open
    /// cursors, still hold. They are freed when those cursors close.
    pub(crate) fn pinned_bytes(&self) -> usize {
        let Some(old) = self.released.lock().unwrap().upgrade() else {
            return 0;
        };
        old.resident_bytes().saturating_sub(self.resident_bytes())
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

    /// Whether [`Self::restore_one`] may still read modules back: the filter
    /// is evictable, has a module handle, and has not given up after
    /// [`MAX_RESTORE_FAILURES`].
    pub(crate) fn can_restore(&self) -> bool {
        matches!(self.state, State::Evictable(_))
            && self.module_handle.is_some()
            && self.restore_failures.load(Ordering::Relaxed) < MAX_RESTORE_FAILURES
    }

    /// Lookups this filter has answered, including those it answered with no
    /// module resident.
    pub(crate) fn probes(&self) -> usize {
        self.tracking.lookups()
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
    ///   recorded for it. A failed read is tried again on later calls, until
    ///   [`MAX_RESTORE_FAILURES`] attempts in a row fail, since storage errors
    ///   can be transient. A module that does not match will not match next time
    ///   either, so after one, later calls return `Ok(0)`.
    pub(crate) fn restore_one(&self) -> Result<usize, RestoreError> {
        let (true, State::Evictable(filter), Some(handle)) =
            (self.can_restore(), &self.state, &self.module_handle)
        else {
            return Ok(0);
        };
        let current = filter.load_full();
        let index = current.resident_modules();
        if index >= self.layout.total_modules() {
            return Ok(0);
        }

        let mut restored = (*current).clone();
        let pushed = handle
            .read_module(index, self.layout.words_per_module())
            .map_err(RestoreError::from)
            .and_then(|words| Ok(restored.push_module(words)?));
        if let Err(error) = pushed {
            self.count_restore_failure(&error);
            return Err(error);
        }
        // The read worked, so the earlier failures were transient. A module
        // that did not match stops all restores, so it cannot get here.
        self.restore_failures.store(0, Ordering::Relaxed);

        // Eviction may have run while the module was read. Then the module
        // read no longer follows the last resident one, so keep what is there.
        let previous = filter.compare_and_swap(&current, Arc::new(restored));
        Ok(if Arc::ptr_eq(&previous, &current) {
            self.layout.bytes_per_module()
        } else {
            0
        })
    }

    /// Counts a failed restore toward [`MAX_RESTORE_FAILURES`]. A module that
    /// does not match will not match next time either, so it uses them all.
    fn count_restore_failure(&self, error: &RestoreError) {
        let failures = match error {
            RestoreError::Read(_) => 1,
            RestoreError::Module(_) => MAX_RESTORE_FAILURES,
        };
        self.restore_failures.fetch_add(failures, Ordering::Relaxed);
    }
}

/// The filter a [`BloomFilterSnapshot`] probes.
#[derive(Clone, Debug)]
enum SnapshotFilter<'a> {
    Fixed(&'a ModularBloomFilter),
    Evictable(Arc<ModularBloomFilter>),
}

impl SnapshotFilter<'_> {
    #[inline]
    fn get(&self) -> &ModularBloomFilter {
        match self {
            Self::Fixed(filter) => filter,
            Self::Evictable(filter) => filter,
        }
    }
}

/// A [`TrackingBloomFilter`] as one reader probes it. See
/// [`TrackingBloomFilter::snapshot`]. A clone starts its own counts.
#[derive(Clone, Debug)]
pub struct BloomFilterSnapshot<'a> {
    filter: SnapshotFilter<'a>,
    counts: LocalFilterCounts<'a>,
}

impl BloomFilterSnapshot<'_> {
    /// Returns whether the key whose hash `hash` gives might have been
    /// inserted, counting the outcome.
    ///
    /// Calls `hash` only when the filter has a module left, so a filter that
    /// admits every key costs no key hash. Such a lookup is not counted as a
    /// hit, so the hit rate covers only lookups the filter tested.
    #[inline]
    pub fn contains_hash_with(&self, hash: impl FnOnce() -> u64) -> bool {
        let filter = self.filter.get();
        if filter.resident_modules() == 0 {
            self.counts.record_skipped();
            return true;
        }
        let is_hit = filter.contains_hash(hash());
        self.counts.record(is_hit);
        is_hit
    }

    /// Returns whether `hash` might have been inserted, counting the outcome.
    #[inline]
    pub fn contains_hash(&self, hash: u64) -> bool {
        self.contains_hash_with(|| hash)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BloomFilterModuleHandle, DURING_MEMORY_READ, MAX_RESTORE_FAILURES, RestoreError,
        TrackingBloomFilter,
    };
    use crate::storage::file::FilterStats;
    use feldera_modular_bloom::ModuleLayout;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    const KEYS: u64 = 100;

    fn layout() -> ModuleLayout {
        ModuleLayout::for_keys(KEYS, 1e-4, 4).unwrap()
    }

    fn key(i: u64) -> u64 {
        i.wrapping_mul(0x9E37_79B9_7F4A_7C15)
    }

    /// An evictable filter holding [`KEYS`] keys in every module. It reads
    /// modules back from memory, from a copy of its own that `edit` may
    /// change, and every read fails when `fail` is set. Returns the filter and
    /// the count of reads.
    fn evictable(
        fail: bool,
        edit: impl FnOnce(&mut [Arc<[u64]>]),
    ) -> (Arc<TrackingBloomFilter>, Arc<AtomicUsize>) {
        let mut filter = TrackingBloomFilter::building(layout());
        for i in 0..KEYS {
            filter.insert_hash(key(i));
        }
        filter.finalize();
        let mut modules: Vec<Arc<[u64]>> = filter
            .built()
            .modules()
            .into_iter()
            .map(Arc::from)
            .collect();
        edit(&mut modules);
        let reads = Arc::new(AtomicUsize::new(0));
        filter.set_module_handle(BloomFilterModuleHandle::Memory {
            modules,
            fail,
            reads: reads.clone(),
        });
        filter.make_evictable();
        (Arc::new(filter), reads)
    }

    /// Dropped modules read back in order, one per call, until the filter
    /// holds every module again.
    #[test]
    fn dropped_modules_read_back_in_order() {
        let (filter, reads) = evictable(false, |_| {});
        let total = layout().total_modules();
        filter.evict_to(1);
        assert_eq!(filter.restore_one().unwrap(), layout().bytes_per_module());
        assert_eq!(filter.resident_modules(), 2);

        while filter.restore_one().unwrap() > 0 {}
        assert_eq!(filter.resident_modules(), total);
        assert_eq!(reads.load(Ordering::Relaxed), total as usize - 1);
        assert_eq!(filter.restore_one().unwrap(), 0, "nothing left to read");
        assert_eq!(reads.load(Ordering::Relaxed), total as usize - 1);
        assert!((0..KEYS).all(|i| filter.contains_hash(key(i))));
    }

    /// A failed read is tried again, until [`MAX_RESTORE_FAILURES`] reads in
    /// a row fail. A module that does not match the bits recorded for it is refused
    /// at once, since it would not match on a later read either.
    #[test]
    fn a_restore_gives_up_after_too_many_failures() {
        let (failing, reads) = evictable(true, |_| {});
        failing.evict_to(1);
        for _ in 0..MAX_RESTORE_FAILURES {
            assert!(matches!(failing.restore_one(), Err(RestoreError::Read(_))));
        }
        assert!(!failing.can_restore());
        assert_eq!(failing.restore_one().unwrap(), 0);
        assert_eq!(reads.load(Ordering::Relaxed), MAX_RESTORE_FAILURES as usize);

        let (wrong, reads) = evictable(false, |modules| {
            let mut words = modules[1].to_vec();
            words[0] ^= 1;
            modules[1] = Arc::from(words);
        });
        wrong.evict_to(1);
        assert!(matches!(wrong.restore_one(), Err(RestoreError::Module(_))));
        assert!(!wrong.can_restore());
        assert_eq!(wrong.restore_one().unwrap(), 0);
        assert_eq!(reads.load(Ordering::Relaxed), 1);
        assert_eq!(wrong.resident_modules(), 1);
    }

    /// A successful restore resets the count of failed reads, so failures
    /// with successes between them never stop restores.
    #[test]
    fn a_successful_restore_resets_the_failures() {
        let mut modules = Vec::new();
        let (mut filter, _) = evictable(true, |m| modules = m.to_vec());
        let set_failing = |filter: &mut Arc<TrackingBloomFilter>, fail: bool| {
            Arc::get_mut(filter)
                .unwrap()
                .set_module_handle(BloomFilterModuleHandle::Memory {
                    modules: modules.clone(),
                    fail,
                    reads: Arc::new(AtomicUsize::new(0)),
                });
        };
        filter.evict_to(1);
        for _ in 0..3 {
            for _ in 1..MAX_RESTORE_FAILURES {
                assert!(matches!(filter.restore_one(), Err(RestoreError::Read(_))));
            }
            set_failing(&mut filter, false);
            assert!(filter.restore_one().unwrap() > 0);
            filter.evict_to(1);
            set_failing(&mut filter, true);
        }
        assert!(filter.can_restore());
    }

    /// Modules that an open snapshot still holds count as pinned until it
    /// drops. Over two evictions, the oldest filter it holds counts.
    #[test]
    fn a_snapshot_pins_the_modules_it_holds() {
        let (filter, _) = evictable(false, |_| {});
        let module = layout().bytes_per_module();
        let snapshot = filter.snapshot();
        filter.evict_to(3);
        filter.evict_to(2);
        assert_eq!(filter.pinned_bytes(), 2 * module);
        drop(snapshot);
        assert_eq!(filter.pinned_bytes(), 0);
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

    /// A snapshot counts its own probes and adds them to the filter's stats
    /// when it drops, and a clone does not count them again.
    #[test]
    fn a_snapshot_adds_its_counts_on_drop() {
        let mut filter = TrackingBloomFilter::building(layout());
        filter.insert_hash(123);
        filter.finalize();

        let snapshot = filter.snapshot().unwrap();
        assert!(snapshot.contains_hash(123));
        assert!(!snapshot.contains_hash(456));
        let clone = snapshot.clone();
        assert!(!clone.contains_hash(789));
        assert_eq!(filter.stats().hits + filter.stats().misses, 0);

        drop(snapshot);
        drop(clone);
        let stats = filter.stats();
        assert_eq!((stats.hits, stats.misses), (1, 2));
    }

    /// A building filter gives no snapshot. A filter with no module left
    /// admits every key without hashing it, and counts the lookup but not as a
    /// hit.
    #[test]
    fn a_filter_with_no_module_counts_without_hashing() {
        let mut filter = TrackingBloomFilter::building(layout());
        assert!(filter.snapshot().is_none(), "a building filter");
        filter.insert_hash(123);
        filter.finalize();
        filter.make_evictable();
        filter.evict_to(0);
        let snapshot = filter.snapshot().unwrap();
        assert!(snapshot.contains_hash_with(|| panic!("the key was hashed")));
        drop(snapshot);
        let stats = filter.stats();
        assert_eq!((stats.hits, stats.misses), (0, 0), "counted as tested");
        assert_eq!(filter.probes(), 1, "the lookup was not counted");
    }

    /// Only an evictable filter can lose modules.
    #[test]
    fn a_fixed_filter_keeps_its_modules() {
        let mut filter = TrackingBloomFilter::building(layout());
        filter.insert_hash(123);
        filter.finalize();
        filter.evict_to(0);
        assert_eq!(filter.resident_modules(), layout().total_modules());
        filter.make_evictable();
        filter.evict_to(1);
        assert_eq!(filter.resident_modules(), 1);
    }

    /// An eviction that runs while a module is read wins: the module read no
    /// longer follows the last kept one, so the restore keeps what is there.
    #[test]
    fn a_restore_that_races_an_eviction_keeps_the_eviction() {
        let (filter, reads) = evictable(false, |_| {});
        filter.evict_to(2);
        let evicting = filter.clone();
        DURING_MEMORY_READ.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || evicting.evict_to(1)));
        });
        assert_eq!(filter.restore_one().unwrap(), 0);
        assert_eq!(reads.load(Ordering::Relaxed), 1);
        assert_eq!(filter.resident_modules(), 1);

        // Later restores start from the module after the last kept one.
        while filter.restore_one().unwrap() > 0 {}
        assert_eq!(filter.resident_modules(), 4);
        assert!((0..KEYS).all(|i| filter.contains_hash(key(i))));
    }

    /// Lookups through snapshots on other threads never miss a stored key
    /// while modules are evicted and restored.
    #[test]
    fn lookups_during_evict_and_restore_have_no_false_negatives() {
        let (filter, _) = evictable(false, |_| {});
        let stop = Arc::new(AtomicBool::new(false));
        let readers: Vec<_> = (0..4)
            .map(|_| {
                let (filter, stop) = (filter.clone(), stop.clone());
                std::thread::spawn(move || {
                    let mut i = 0u64;
                    while !stop.load(Ordering::Relaxed) {
                        let snapshot = filter.snapshot();
                        for _ in 0..1_000 {
                            assert!(
                                snapshot
                                    .as_ref()
                                    .is_none_or(|s| s.contains_hash(key(i % KEYS))),
                                "false negative"
                            );
                            i += 1;
                        }
                    }
                })
            })
            .collect();
        for round in 0..200u32 {
            filter.evict_to(round % 4);
            while filter.restore_one().unwrap() > 0 {}
            assert_eq!(filter.resident_modules(), 4);
        }
        stop.store(true, Ordering::Relaxed);
        readers.into_iter().for_each(|r| r.join().unwrap());
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
