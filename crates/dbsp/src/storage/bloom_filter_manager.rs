//! Manages how many modules large Bloom filters keep in memory, and how many
//! modules new Bloom filters are built with.
//!
//! This is experimental and off unless the `evict_bloom_filter` dev tweak
//! is set. When it is on, every Bloom filter of [`MIN_EVICTABLE_BYTES`] or
//! more that a batch takes over is enrolled here. The memory monitor calls
//! [`BloomFilterManager::tick`] once a second:
//!
//! - At high memory pressure it lowers the module max, which is shared by the
//!   whole runtime, and trims every enrolled filter to it.
//! - After a period of low pressure it raises the module max again.
//!
//! New batches build only as many modules as the module max allows, which also
//! saves the time to build the others. A filter never gets a dropped module
//! back: accuracy returns as merges replace batches with new ones, built at the
//! module max of that time. A restart loads each filter with every module its
//! file holds, up to what the configured rate keeps.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use feldera_types::memory_pressure::MemoryPressure;

use crate::storage::file::{modules_for_rate, rate_for_modules};
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

/// When the module max last changed, and since when pressure has been low.
#[derive(Debug, Default)]
struct Policy {
    last_step: Option<Instant>,
    low_since: Option<Instant>,
}

/// Decides, for one runtime, how many modules large Bloom filters keep in
/// memory and how many modules new Bloom filters are built with.
#[derive(Debug)]
pub(crate) struct BloomFilterManager {
    enabled: bool,
    /// The configured false positive rate, which applies again once the module
    /// max is back to its configured value.
    configured_rate: f64,
    /// Modules the configured rate keeps, which the module max never exceeds.
    configured_modules: u32,
    /// Max number of modules a filter may keep or be built with.
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

    /// The false positive rate a filter is held to when at most `module_max`
    /// modules are allowed.
    ///
    /// This is the configured rate while `module_max` allows every module that
    /// rate keeps, otherwise the rate `module_max` modules give.
    fn rate(&self, module_max: u32) -> f64 {
        if module_max >= self.configured_modules {
            self.configured_rate
        } else {
            rate_for_modules(module_max)
        }
    }

    /// The false positive rate new Bloom filters are built for.
    ///
    /// The configured rate when eviction is disabled. Otherwise the rate of the
    /// current module max, so that a new batch builds no modules that eviction
    /// would drop at once.
    pub(crate) fn build_rate(&self) -> f64 {
        if self.enabled {
            self.rate(self.module_max.load(Ordering::Relaxed))
        } else {
            self.configured_rate
        }
    }

    /// Starts managing `filter`, so that later ticks can trim it, and trims it
    /// to the current module max.
    ///
    /// Does nothing when eviction is disabled or the filter is smaller than
    /// [`MIN_EVICTABLE_BYTES`].
    pub(crate) fn enroll(&self, filter: &Arc<TrackingBloomFilter>) {
        if !self.enabled || filter.resident_bytes() < MIN_EVICTABLE_BYTES {
            return;
        }
        self.filters.lock().unwrap().push(Arc::downgrade(filter));
        // Trim after enrolling, so a tick that lowers the module max in between
        // still reaches this filter.
        let module_max = self.module_max.load(Ordering::Relaxed);
        filter.evict_to(filter.target_modules(self.rate(module_max)));
    }

    /// Runs the policy once, given the memory pressure the monitor just
    /// computed.
    ///
    /// # Returns
    ///
    /// The old and new module max when it changed, otherwise `None`.
    pub(crate) fn tick(&self, pressure: MemoryPressure, now: Instant) -> Option<(u32, u32)> {
        if !self.enabled {
            return None;
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
            let rate = self.rate(new);
            for filter in &live {
                filter.evict_to(filter.target_modules(rate));
            }
        }
        (new != old).then_some((old, new))
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
    use crate::storage::tracking_bloom_filter::TrackingBloomFilter;
    use feldera_modular_bloom::{ModularBloomFilterBuilder, ModuleLayout};
    use feldera_types::memory_pressure::MemoryPressure::{self, *};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    /// Keys a four-module filter of just over 1 MiB holds at its design rate.
    const KEYS: u64 = 450_000;

    /// Keys that leave a filter sized for [`KEYS`] needing three modules for
    /// 5e-4 but only two for 1e-3.
    const PARTIAL_KEYS: u64 = 260_000;

    fn key(i: u64) -> u64 {
        i.wrapping_mul(0x9E37_79B9_7F4A_7C15)
    }

    /// A four-module filter sized for [`KEYS`] at 1e-4, holding `keys` keys.
    fn filled(keys: u64) -> Arc<TrackingBloomFilter> {
        let layout = ModuleLayout::for_keys(KEYS, 1e-4, 4).unwrap();
        let mut builder = ModularBloomFilterBuilder::new(layout);
        for i in 0..keys {
            builder.insert_hash(key(i));
        }
        Arc::new(TrackingBloomFilter::loaded(builder.finish()))
    }

    fn enrolled(e: &BloomFilterManager) -> Arc<TrackingBloomFilter> {
        let filter = filled(KEYS);
        assert!(filter.resident_bytes() >= MIN_EVICTABLE_BYTES);
        e.enroll(&filter);
        filter
    }

    /// Runs `ticks` ticks of `pressure`, one second apart, from `*now`.
    fn run(e: &BloomFilterManager, pressure: MemoryPressure, ticks: u32, now: &mut Instant) {
        for _ in 0..ticks {
            *now += Duration::from_secs(1);
            e.tick(pressure, *now);
        }
    }

    #[test]
    fn disabled_manager_enrolls_nothing() {
        let e = BloomFilterManager::new(false, 1e-4);
        enrolled(&e);
        assert!(e.live_filters().is_empty());
        assert_eq!(e.tick(Critical, Instant::now()), None);
        assert_eq!(e.module_max(), None);
        assert_eq!(e.build_rate(), 1e-4);
    }

    /// Only filters of at least the floor are enrolled.
    #[test]
    fn only_large_filters_are_enrolled() {
        let e = BloomFilterManager::new(true, 1e-4);
        let layout = ModuleLayout::for_keys(10_000, 1e-4, 4).unwrap();
        let mut builder = ModularBloomFilterBuilder::new(layout);
        for i in 0..10_000 {
            builder.insert_hash(key(i));
        }
        let small = Arc::new(TrackingBloomFilter::loaded(builder.finish()));
        assert!(small.resident_bytes() < MIN_EVICTABLE_BYTES);
        e.enroll(&small);
        assert!(e.live_filters().is_empty());

        let _large = enrolled(&e);
        assert_eq!(e.live_filters().len(), 1);
    }

    /// High pressure steps the module max down once per interval, trims enrolled
    /// filters without false negatives, frees their modules, and lowers the rate
    /// new filters are built for.
    #[test]
    fn high_pressure_steps_down_and_frees_modules() {
        let e = BloomFilterManager::new(true, 1e-4);
        let filter = enrolled(&e);
        let full = filter.resident_bytes();
        let mut now = Instant::now();

        assert_eq!(e.tick(Low, now), None);
        assert_eq!(e.module_max(), Some(4));
        assert_eq!(e.build_rate(), 1e-4);

        now += Duration::from_secs(1);
        assert_eq!(e.tick(High, now), Some((4, 3)));
        assert_eq!(filter.resident_modules(), 3);
        assert_eq!(filter.resident_bytes(), full / 4 * 3);
        assert_eq!(e.build_rate(), 1e-3);

        // No second step before the interval passes.
        run(&e, High, STEP_INTERVAL.as_secs() as u32 - 1, &mut now);
        assert_eq!(e.module_max(), Some(3));
        run(&e, High, 1, &mut now);
        assert_eq!(e.module_max(), Some(2));

        run(&e, Critical, 2 * STEP_INTERVAL.as_secs() as u32, &mut now);
        assert_eq!(e.module_max(), Some(0));
        assert_eq!(filter.resident_bytes(), 0);
        assert!((0..KEYS).all(|i| filter.contains_hash(key(i))));
        assert_eq!(e.build_rate(), 1.0);

        // A filter enrolled now starts at the module max.
        let late = enrolled(&e);
        assert_eq!(late.resident_modules(), 0);
    }

    #[test]
    fn module_max_starts_at_the_configured_modules() {
        let e = BloomFilterManager::new(true, 1e-1);
        assert_eq!(e.module_max(), Some(1));
        assert_eq!(e.tick(High, Instant::now()), Some((1, 0)));
    }

    /// Low pressure raises the module max one step per low-pressure period,
    /// never above the configured count. Filters already trimmed keep what they
    /// have; only new filters are built with more modules.
    #[test]
    fn low_pressure_raises_the_module_max_slowly() {
        let e = BloomFilterManager::new(true, 1e-4);
        let filter = enrolled(&e);
        let mut now = Instant::now();
        e.tick(Critical, now);
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
        assert_eq!(e.build_rate(), 1e-4);
        assert_eq!(filter.resident_modules(), 3);

        run(&e, Low, 2 * LOW_PRESSURE_PERIOD.as_secs() as u32, &mut now);
        assert_eq!(e.module_max(), Some(4));
    }

    /// A filter holding far fewer keys than it was sized for meets the rate
    /// with fewer modules, and keeps only what loading it would keep.
    #[test]
    fn a_sparse_filter_keeps_what_a_load_would_keep() {
        let e = BloomFilterManager::new(true, 1e-4);
        let filter = filled(1_000);
        let on_load = resident_modules(filter.layout(), filter.built().density(), 1e-4);
        assert!(on_load < 4, "{on_load} modules is not sparse");

        e.enroll(&filter);
        assert_eq!(filter.resident_modules(), on_load);
        assert!((0..1_000).all(|i| filter.contains_hash(key(i))));
    }

    /// With a configured rate between two table entries, a filter keeps what
    /// loading it at that rate keeps, and new filters are built for that rate,
    /// not for the coarser table entry.
    #[test]
    fn a_rate_between_table_entries_keeps_what_a_load_would_keep() {
        const RATE: f64 = 5e-4;
        let e = BloomFilterManager::new(true, RATE);
        let filter = filled(PARTIAL_KEYS);
        let density = filter.built().density().clone();
        let on_load = resident_modules(filter.layout(), &density, RATE);
        assert!(
            resident_modules(filter.layout(), &density, 1e-3) < on_load,
            "this filter keeps as much at 1e-3 as at {RATE:e}, so it tests nothing"
        );

        e.enroll(&filter);
        assert_eq!(filter.resident_modules(), on_load);
        assert_eq!(e.build_rate(), RATE);
    }

    /// Dropped filters leave the registry on the next tick.
    #[test]
    fn dropped_filters_are_pruned() {
        let e = BloomFilterManager::new(true, 1e-4);
        let filter = enrolled(&e);
        drop(filter);
        e.tick(Low, Instant::now());
        assert!(e.filters.lock().unwrap().is_empty());
    }
}
