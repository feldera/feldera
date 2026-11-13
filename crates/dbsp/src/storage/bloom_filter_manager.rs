//! Manages how many modules each large Bloom filter keeps in memory.
//!
//! This is experimental and off unless the `evict_bloom_filter` dev tweak
//! is set. When it is on, every large Bloom filter that a batch takes over
//! is enrolled here. The memory monitor calls [`BloomFilterManager::tick`]
//! once a second:
//!
//! - At high memory pressure it lowers the module max, which is shared by the
//!   whole runtime, and trims every enrolled filter to it.
//! - After a period of low pressure it raises the module max again.
//! - At low memory pressure it reads dropped modules back from storage, up to
//!   the module max. The filters that answer the most lookups go first, and
//!   each tick reads a bounded number of bytes that never takes the process
//!   past the moderate pressure threshold.
//!
//! Files on storage are never changed. They keep every module, which is what
//! lets a filter restore its modules.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use feldera_types::memory_pressure::MemoryPressure;
use tracing::warn;

use crate::storage::file::modules_for_rate;
use crate::storage::tracking_bloom_filter::TrackingBloomFilter;

/// Smallest filter worth enrolling, in resident bytes.
///
/// Filters below this hold too little memory to be worth the accuracy they
/// would lose. The floor is low on purpose: a spine can hold much of its filter
/// memory in many mid-size batches, for example 6 batches of 320,000 keys,
/// each with about 750 KiB of filter at a rate of 1e-4, and less at coarser
/// rates.
pub(crate) const MIN_EVICTABLE_BYTES: usize = 64 << 10;

/// Shortest time between two changes of the module max.
///
/// jemalloc returns freed pages to the OS over about 10 seconds by default,
/// so a shorter interval would lower the module max again before RSS shows the
/// effect of the last step.
const STEP_INTERVAL: Duration = Duration::from_secs(10);

/// Time memory pressure must stay low before the module max goes up one step.
const LOW_PRESSURE_PERIOD: Duration = Duration::from_secs(60);

/// Most module bytes read back from storage in one tick, unless a single
/// module is larger, in which case that one module is read.
///
/// This spreads restoring over time, so its reads and its memory do not arrive
/// in one burst.
const RESTORE_BYTES_PER_TICK: usize = 16 << 20;

/// What one tick did.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Tick {
    /// The old and new module max, when it changed.
    pub(crate) step: Option<(u32, u32)>,
    /// Module bytes read back from storage.
    pub(crate) restored_bytes: usize,
}

/// When the module max last changed, and since when pressure has been low.
#[derive(Debug, Default)]
struct Policy {
    last_step: Option<Instant>,
    low_since: Option<Instant>,
}

/// Decides, for one runtime, how many modules each large Bloom filter keeps in
/// memory.
#[derive(Debug)]
pub(crate) struct BloomFilterManager {
    enabled: bool,
    /// The configured false positive rate, which filters return to when
    /// pressure is low.
    configured_rate: f64,
    /// Modules the configured rate keeps, which the module max never exceeds.
    configured_modules: u32,
    /// Max number of modules an enrolled filter may keep.
    module_max: AtomicU32,
    /// Every enrolled filter. Entries for dropped filters are pruned on each
    /// tick.
    filters: Mutex<Vec<Weak<TrackingBloomFilter>>>,
    policy: Mutex<Policy>,
}

impl BloomFilterManager {
    pub(crate) fn new(enabled: bool, configured_rate: f64) -> Self {
        let configured_modules = modules_for_rate(configured_rate);
        Self {
            enabled,
            configured_rate,
            configured_modules,
            module_max: AtomicU32::new(configured_modules),
            filters: Mutex::new(Vec::new()),
            policy: Mutex::new(Policy::default()),
        }
    }

    /// Current module max, or `None` when eviction is disabled.
    pub(crate) fn module_max(&self) -> Option<u32> {
        self.enabled
            .then(|| self.module_max.load(Ordering::Relaxed))
    }

    /// Starts managing `filter`, so that later ticks can trim it and restore
    /// it, and trims it to the current module max.
    ///
    /// Does nothing when eviction is disabled, when the filter cannot read its
    /// modules back from storage, or when it is smaller than
    /// [`MIN_EVICTABLE_BYTES`].
    pub(crate) fn enroll(&self, filter: &Arc<TrackingBloomFilter>) {
        if !self.enabled
            || !filter.has_module_handle()
            || filter.resident_bytes() < MIN_EVICTABLE_BYTES
        {
            return;
        }
        self.filters.lock().unwrap().push(Arc::downgrade(filter));
        // Trim after enrolling, so a tick that lowers the module max in between
        // still reaches this filter.
        let module_max = self.module_max.load(Ordering::Relaxed);
        filter.evict_to(filter.target_modules(self.configured_rate, module_max));
    }

    /// Runs the policy once.
    ///
    /// # Arguments
    ///
    /// - `pressure`: the memory pressure the monitor just computed.
    /// - `headroom`: bytes the process can still grow by before it reaches
    ///   moderate memory pressure, or `None` when there is no memory limit.
    /// - `now`: the current time.
    pub(crate) fn tick(
        &self,
        pressure: MemoryPressure,
        headroom: Option<usize>,
        now: Instant,
    ) -> Tick {
        if !self.enabled {
            return Tick::default();
        }
        let mut policy = self.policy.lock().unwrap();

        if pressure == MemoryPressure::Low {
            policy.low_since.get_or_insert(now);
        } else {
            policy.low_since = None;
        }
        let cooled = policy.last_step.is_none_or(|t| now - t >= STEP_INTERVAL);
        let low_long_enough = policy
            .low_since
            .is_some_and(|t| now - t >= LOW_PRESSURE_PERIOD);

        let old = self.module_max.load(Ordering::Relaxed);
        let new = if pressure >= MemoryPressure::High && old > 0 && cooled {
            old - 1
        } else if low_long_enough && old < self.configured_modules && cooled {
            // Each further step up needs another low-pressure period.
            policy.low_since = Some(now);
            old + 1
        } else {
            old
        };
        if new != old {
            self.module_max.store(new, Ordering::Relaxed);
            policy.last_step = Some(now);
        }

        let live = self.live_filters();
        if new < old {
            for filter in &live {
                filter.evict_to(filter.target_modules(self.configured_rate, new));
            }
        }
        let restored_bytes = if pressure == MemoryPressure::Low && new >= old {
            self.restore(&live, new, headroom.unwrap_or(usize::MAX))
        } else {
            0
        };
        Tick {
            step: (new != old).then_some((old, new)),
            restored_bytes,
        }
    }

    /// Reads dropped modules back into `filters`, up to `module_max`.
    ///
    /// Filters that answer the most lookups go first, since they gain the most
    /// from accuracy. Reads stop at [`RESTORE_BYTES_PER_TICK`], except that one
    /// module is always allowed, and never go past `headroom`, counting each
    /// module twice while it is read.
    ///
    /// Returns the bytes read.
    fn restore(
        &self,
        filters: &[Arc<TrackingBloomFilter>],
        module_max: u32,
        headroom: usize,
    ) -> usize {
        let mut wanting: Vec<_> = filters
            .iter()
            .map(|f| (f, f.target_modules(self.configured_rate, module_max)))
            .filter(|(f, target)| f.resident_modules() < *target)
            .collect();
        wanting.sort_by_cached_key(|(f, _)| std::cmp::Reverse(f.probes()));

        let mut read = 0;
        'filters: for (filter, target) in wanting {
            let bytes = filter.layout().bytes_per_module();
            while filter.resident_modules() < target {
                let total = read + bytes;
                // A read holds the module twice for a moment: as read from
                // storage, and as the copy the filter keeps.
                if read + 2 * bytes > headroom || (read > 0 && total > RESTORE_BYTES_PER_TICK) {
                    break 'filters;
                }
                match filter.restore_one() {
                    Ok(0) => break,
                    Ok(added) => read += added,
                    Err(error) => {
                        warn!("Bloom filter keeps its current accuracy: {error}");
                        break;
                    }
                }
            }
        }
        read
    }

    /// Prunes entries for dropped filters and returns the rest.
    pub(crate) fn live_filters(&self) -> Vec<Arc<TrackingBloomFilter>> {
        let mut filters = self.filters.lock().unwrap();
        filters.retain(|weak| weak.strong_count() > 0);
        filters.iter().filter_map(Weak::upgrade).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::{BloomFilterManager, LOW_PRESSURE_PERIOD, MIN_EVICTABLE_BYTES, STEP_INTERVAL};
    use crate::storage::file::resident_modules;
    use crate::storage::tracking_bloom_filter::{BloomFilterModuleHandle, TrackingBloomFilter};
    use feldera_modular_bloom::{ModularBloomFilterBuilder, ModuleLayout};
    use feldera_types::memory_pressure::MemoryPressure::{self, *};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::time::{Duration, Instant};

    /// Keys a four-module filter of just over 1 MiB holds at its design rate.
    const KEYS: u64 = 450_000;

    /// Keys that leave a filter sized for [`KEYS`] needing three modules for
    /// 5e-4 but only two for 1e-3.
    const PARTIAL_KEYS: u64 = 260_000;

    fn key(i: u64) -> u64 {
        i.wrapping_mul(0x9E37_79B9_7F4A_7C15)
    }

    /// A four-module filter filled with `keys` keys to its design rate of
    /// 1e-4, which reads its modules back from memory, or fails to with
    /// `fail`. Also returns the count of those reads.
    fn filled(keys: u64, fail: bool) -> (Arc<TrackingBloomFilter>, Arc<AtomicUsize>) {
        let layout = ModuleLayout::for_keys(keys, 1e-4, 4).unwrap();
        let mut builder = ModularBloomFilterBuilder::new(layout);
        for i in 0..keys {
            builder.insert_hash(key(i));
        }
        let full = builder.finish();
        let reads = Arc::new(AtomicUsize::new(0));
        let mut filter = TrackingBloomFilter::loaded(full.clone());
        filter.set_module_handle(BloomFilterModuleHandle::Memory {
            modules: full.modules().into_iter().map(Arc::from).collect(),
            fail,
            reads: reads.clone(),
        });
        (Arc::new(filter), reads)
    }

    fn enrolled(
        e: &BloomFilterManager,
        fail: bool,
    ) -> (Arc<TrackingBloomFilter>, Arc<AtomicUsize>) {
        let (filter, reads) = filled(KEYS, fail);
        assert!(filter.resident_bytes() >= MIN_EVICTABLE_BYTES);
        e.enroll(&filter);
        (filter, reads)
    }

    /// Runs `ticks` ticks of `pressure`, one second apart, from `*now`.
    fn run(e: &BloomFilterManager, pressure: MemoryPressure, ticks: u32, now: &mut Instant) {
        for _ in 0..ticks {
            *now += Duration::from_secs(1);
            e.tick(pressure, None, *now);
        }
    }

    /// Steps the module max down from 4 to 2 with high pressure, then waits
    /// out one low-pressure period with no room to restore, so the next low tick raises
    /// it to 3.
    fn down_to_two_then_low_pressure(e: &BloomFilterManager, now: &mut Instant) {
        e.tick(High, None, *now);
        run(e, High, STEP_INTERVAL.as_secs() as u32, now);
        assert_eq!(e.module_max(), Some(2));
        for _ in 0..LOW_PRESSURE_PERIOD.as_secs() {
            *now += Duration::from_secs(1);
            assert_eq!(e.tick(Low, Some(0), *now).restored_bytes, 0);
        }
        assert_eq!(e.module_max(), Some(2));
    }

    #[test]
    fn disabled_manager_enrolls_nothing() {
        let e = BloomFilterManager::new(false, 1e-4);
        enrolled(&e, false);
        assert!(e.live_filters().is_empty());
        assert_eq!(e.tick(Critical, None, Instant::now()).step, None);
        assert_eq!(e.module_max(), None);
    }

    /// Only large filters that can read their modules back are enrolled.
    #[test]
    fn only_large_filters_with_a_module_handle_are_enrolled() {
        let e = BloomFilterManager::new(true, 1e-4);
        let (small, _) = filled(10_000, false);
        assert!(small.resident_bytes() < MIN_EVICTABLE_BYTES);
        e.enroll(&small);
        let (large, _) = filled(KEYS, false);
        let no_handle = Arc::new(TrackingBloomFilter::loaded(large.built().as_ref().clone()));
        e.enroll(&no_handle);
        assert!(e.live_filters().is_empty());

        e.enroll(&large);
        assert_eq!(e.live_filters().len(), 1);
    }

    /// High pressure steps the module max down once per interval, trims enrolled
    /// filters without false negatives, and frees their modules.
    #[test]
    fn high_pressure_steps_down_and_frees_modules() {
        let e = BloomFilterManager::new(true, 1e-4);
        let (filter, _) = enrolled(&e, false);
        let full = filter.resident_bytes();
        let mut now = Instant::now();

        assert_eq!(e.tick(Low, None, now).step, None);
        assert_eq!(e.module_max(), Some(4));

        now += Duration::from_secs(1);
        assert_eq!(e.tick(High, None, now).step, Some((4, 3)));
        assert_eq!(filter.resident_modules(), 3);
        assert_eq!(filter.resident_bytes(), full / 4 * 3);

        // No second step before the interval passes.
        run(&e, High, STEP_INTERVAL.as_secs() as u32 - 1, &mut now);
        assert_eq!(e.module_max(), Some(3));
        run(&e, High, 1, &mut now);
        assert_eq!(e.module_max(), Some(2));

        run(&e, Critical, 2 * STEP_INTERVAL.as_secs() as u32, &mut now);
        assert_eq!(e.module_max(), Some(0));
        assert_eq!(filter.resident_bytes(), 0);
        assert!((0..KEYS).all(|i| filter.contains_hash(key(i))));

        // A filter enrolled now starts at the module max.
        let (late, _) = enrolled(&e, false);
        assert_eq!(late.resident_modules(), 0);
    }

    #[test]
    fn module_max_starts_at_the_configured_modules() {
        let e = BloomFilterManager::new(true, 1e-1);
        assert_eq!(e.module_max(), Some(1));
        assert_eq!(e.tick(High, None, Instant::now()).step, Some((1, 0)));
    }

    /// Low pressure raises the module max one step per low-pressure period, never
    /// above the configured count, and filters are restored to it.
    #[test]
    fn low_pressure_raises_the_module_max_slowly() {
        let e = BloomFilterManager::new(true, 1e-4);
        let (filter, _) = enrolled(&e, false);
        let mut now = Instant::now();
        e.tick(Critical, None, now);
        assert_eq!(e.module_max(), Some(3));

        // Moderate pressure does not count as low.
        run(
            &e,
            Moderate,
            2 * LOW_PRESSURE_PERIOD.as_secs() as u32,
            &mut now,
        );
        assert_eq!(e.module_max(), Some(3));

        // The first low-pressure tick starts the period, so it ends one tick later.
        run(&e, Low, LOW_PRESSURE_PERIOD.as_secs() as u32, &mut now);
        assert_eq!(e.module_max(), Some(3));
        run(&e, Low, 1, &mut now);
        assert_eq!(e.module_max(), Some(4));
        assert_eq!(filter.resident_modules(), 4);

        run(&e, Low, 2 * LOW_PRESSURE_PERIOD.as_secs() as u32, &mut now);
        assert_eq!(e.module_max(), Some(4));
    }

    /// A filter holding far fewer keys than it was sized for meets the rate
    /// with fewer modules, and keeps only what loading it would keep.
    #[test]
    fn a_sparse_filter_keeps_what_a_load_would_keep() {
        let e = BloomFilterManager::new(true, 1e-4);
        let layout = ModuleLayout::for_keys(KEYS, 1e-4, 4).unwrap();
        let mut builder = ModularBloomFilterBuilder::new(layout);
        for i in 0..1_000 {
            builder.insert_hash(key(i));
        }
        let full = builder.finish();
        let on_load = resident_modules(&layout, full.density(), 1e-4);
        assert!(on_load < 4, "{on_load} modules is not sparse");

        let mut filter = TrackingBloomFilter::loaded(full.clone());
        filter.set_module_handle(BloomFilterModuleHandle::Memory {
            modules: full.modules().into_iter().map(Arc::from).collect(),
            fail: false,
            reads: Arc::new(AtomicUsize::new(0)),
        });
        let filter = Arc::new(filter);
        e.enroll(&filter);
        assert_eq!(filter.resident_modules(), on_load);
        assert!((0..1_000).all(|i| filter.contains_hash(key(i))));
    }

    /// Dropped filters leave the registry on the next tick.
    #[test]
    fn dropped_filters_are_pruned() {
        let e = BloomFilterManager::new(true, 1e-4);
        let (filter, _) = enrolled(&e, false);
        drop(filter);
        e.tick(Low, None, Instant::now());
        assert!(e.filters.lock().unwrap().is_empty());
    }

    /// At low pressure, filters read modules back up to the module max, the
    /// busiest first, and never past the headroom.
    #[test]
    fn low_pressure_restores_the_busiest_filter_first() {
        let e = BloomFilterManager::new(true, 1e-4);
        let (quiet, _) = enrolled(&e, false);
        let (busy, _) = enrolled(&e, false);
        for i in 0..100 {
            busy.contains_hash(key(i));
        }
        let module = busy.layout().bytes_per_module();
        let mut now = Instant::now();
        down_to_two_then_low_pressure(&e, &mut now);

        // A read holds the module twice for a moment, so room for one module
        // is not enough.
        now += Duration::from_secs(1);
        let tick = e.tick(Low, Some(module), now);
        assert_eq!(tick.step, Some((2, 3)));
        assert_eq!(tick.restored_bytes, 0);

        // Room for one read: only the busy filter gets it.
        now += Duration::from_secs(1);
        assert_eq!(e.tick(Low, Some(2 * module), now).restored_bytes, module);
        assert_eq!(busy.resident_modules(), 3);
        assert_eq!(quiet.resident_modules(), 2);

        now += Duration::from_secs(1);
        assert_eq!(e.tick(Low, None, now).restored_bytes, module);
        assert_eq!(quiet.resident_modules(), 3);
        assert!((0..KEYS).all(|i| quiet.contains_hash(key(i))));
    }

    #[test]
    fn moderate_pressure_does_not_restore() {
        let e = BloomFilterManager::new(true, 1e-4);
        let (filter, reads) = enrolled(&e, false);
        let mut now = Instant::now();
        down_to_two_then_low_pressure(&e, &mut now);
        now += Duration::from_secs(1);
        e.tick(Low, Some(0), now);
        assert_eq!(e.module_max(), Some(3));

        run(&e, Moderate, 5, &mut now);
        assert_eq!(filter.resident_modules(), 2);
        assert_eq!(reads.load(AtomicOrdering::Relaxed), 0);
    }

    /// A filter whose module reads fail keeps its accuracy, and stops trying
    /// after three attempts.
    #[test]
    fn a_failing_read_is_tried_three_times() {
        let e = BloomFilterManager::new(true, 1e-4);
        let (filter, reads) = enrolled(&e, true);
        let mut now = Instant::now();
        down_to_two_then_low_pressure(&e, &mut now);
        run(&e, Low, 10, &mut now);
        assert_eq!(filter.resident_modules(), 2);
        assert_eq!(reads.load(AtomicOrdering::Relaxed), 3);
    }

    /// A module that does not match the bits recorded for it is refused at
    /// once, since it would not match on a later read either.
    #[test]
    fn a_module_that_does_not_match_is_not_tried_again() {
        let e = BloomFilterManager::new(true, 1e-4);
        let layout = ModuleLayout::for_keys(KEYS, 1e-4, 4).unwrap();
        let mut builder = ModularBloomFilterBuilder::new(layout);
        for i in 0..KEYS {
            builder.insert_hash(key(i));
        }
        let full = builder.finish();
        let mut modules: Vec<Arc<[u64]>> = full.modules().into_iter().map(Arc::from).collect();
        let mut wrong = modules[2].to_vec();
        wrong[0] ^= 1;
        modules[2] = Arc::from(wrong);
        let reads = Arc::new(AtomicUsize::new(0));
        let mut filter = TrackingBloomFilter::loaded(full);
        filter.set_module_handle(BloomFilterModuleHandle::Memory {
            modules,
            fail: false,
            reads: reads.clone(),
        });
        let filter = Arc::new(filter);
        e.enroll(&filter);

        let mut now = Instant::now();
        down_to_two_then_low_pressure(&e, &mut now);
        run(&e, Low, 10, &mut now);
        assert_eq!(filter.resident_modules(), 2);
        assert_eq!(reads.load(AtomicOrdering::Relaxed), 1);
    }

    /// With a configured rate between two table entries, a filter keeps what
    /// loading it at that rate keeps, not what the coarser entry would keep.
    #[test]
    fn a_rate_between_table_entries_keeps_what_a_load_would_keep() {
        const RATE: f64 = 5e-4;
        let e = BloomFilterManager::new(true, RATE);
        let layout = ModuleLayout::for_keys(KEYS, 1e-4, 4).unwrap();
        let mut builder = ModularBloomFilterBuilder::new(layout);
        for i in 0..PARTIAL_KEYS {
            builder.insert_hash(key(i));
        }
        let full = builder.finish();
        let on_load = resident_modules(&layout, full.density(), RATE);
        assert!(
            resident_modules(&layout, full.density(), 1e-3) < on_load,
            "this filter keeps as much at 1e-3 as at {RATE:e}, so it tests nothing"
        );

        let mut filter = TrackingBloomFilter::loaded(full.clone());
        filter.set_module_handle(BloomFilterModuleHandle::Memory {
            modules: full.modules().into_iter().map(Arc::from).collect(),
            fail: false,
            reads: Arc::new(AtomicUsize::new(0)),
        });
        let filter = Arc::new(filter);
        e.enroll(&filter);
        assert_eq!(filter.resident_modules(), on_load);
    }
}
