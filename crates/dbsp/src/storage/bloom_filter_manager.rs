//! Manages how many modules each large Bloom filter keeps in memory.
//!
//! This is experimental and off unless the `bloom_filter_eviction` dev tweak
//! is set. When it is on, every large Bloom filter that a batch takes over
//! is enrolled here. The memory monitor calls [`BloomFilterManager::tick`]
//! once a second:
//!
//! - At high memory pressure it lowers the module limit, which is shared by the
//!   whole runtime, and trims every enrolled filter to it. It does so only
//!   when freeing the enrolled filters could end the pressure, and it removes
//!   the last module only at critical pressure. Below critical pressure, a
//!   step goes ahead only if the last one lowered RSS.
//! - After a period of low pressure it raises the module limit again, if the
//!   modules of that step fit below the moderate pressure threshold.
//! - At low memory pressure it reads dropped modules back from storage, up to
//!   the module limit. The filters that answer the most lookups go first, and
//!   each tick reads a bounded number of bytes that never takes the process
//!   past the moderate pressure threshold.
//!
//! Files on storage are never changed. They keep every module, which is what
//! lets a filter restore its modules.

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use feldera_types::memory_pressure::{MODERATE_MEMORY_PRESSURE_THRESHOLD, MemoryPressure};
use tracing::warn;

use crate::storage::file::modules_for_rate;
use crate::storage::tracking_bloom_filter::{TrackingBloomFilter, is_evictable_size};

#[cfg(test)]
thread_local! {
    /// Runs in the next tick on this thread right after it collects the live
    /// filters, so a test can enroll a filter while the tick runs.
    static AFTER_COLLECT: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
}

/// Shortest time between two changes of the module limit.
///
/// jemalloc returns freed pages to the OS over about 10 seconds by default,
/// so a shorter interval would lower the module limit again before RSS shows
/// the effect of the last step.
const STEP_INTERVAL: Duration = Duration::from_secs(10);

/// Time memory pressure must stay low before the module limit goes up one step.
const LOW_PRESSURE_PERIOD: Duration = Duration::from_secs(60);

/// Most module bytes read back from storage in one tick, unless a single
/// module is larger, in which case that one module is read.
///
/// This spreads restoring over time, so its reads and its memory do not arrive
/// in one burst.
const RESTORE_BYTES_PER_TICK: usize = 16 << 20;

/// Part of the bytes a step down freed that RSS must have dropped by before
/// the next step down, below critical pressure.
///
/// A quarter allows for noise and for pages the allocator has not returned
/// yet, but still catches a step that freed nothing RSS can see.
const STEP_HELPED_DIVISOR: usize = 4;

/// Process memory, as the memory monitor measured it.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Memory {
    /// Resident set size of the process, in bytes.
    pub(crate) rss: usize,
    /// The memory limit, in bytes.
    pub(crate) max_rss: usize,
}

impl Memory {
    /// Where moderate pressure starts, in bytes. High pressure ends only below
    /// it.
    fn moderate(&self) -> usize {
        (MODERATE_MEMORY_PRESSURE_THRESHOLD * self.max_rss as f64) as usize
    }

    /// Bytes RSS can grow by before moderate pressure starts.
    fn headroom(&self) -> usize {
        self.moderate().saturating_sub(self.rss)
    }

    /// Bytes RSS must drop by to get below moderate pressure.
    fn excess(&self) -> usize {
        self.rss.saturating_sub(self.moderate())
    }
}

/// Why a tick left the module limit where it was, although pressure asked for a
/// step.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Hold {
    /// Freeing every module that eviction can still free, down to the fewest
    /// modules this pressure allows, with the modules that open cursors still
    /// hold, would still leave RSS above where moderate pressure starts. So
    /// the pressure does not come from the filters, and evicting would cost
    /// lookups for nothing.
    FiltersTooSmall { filter_bytes: usize, excess: usize },
    /// Modules that earlier steps dropped, and that open cursors still hold,
    /// will end the pressure when those cursors close.
    Releasing { pinned: usize, excess: usize },
    /// The step would remove the last module, which costs far more lookups per
    /// byte than any other, and pressure is not critical.
    LastModule,
    /// The modules of the next step do not fit below the moderate pressure
    /// threshold, so reading them back would bring the pressure back.
    NoRoom { needed: usize, headroom: usize },
    /// The last step down freed `freed` bytes that no cursor holds, but RSS
    /// dropped by only `dropped` since. Either the allocator kept the pages,
    /// or other memory grew, so another step would cost lookups and not end
    /// the pressure.
    StepDidNotHelp { freed: usize, dropped: usize },
}

/// What one tick did.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) enum Tick {
    /// The module limit stayed, and pressure asked for no step.
    #[default]
    Idle,
    /// The module limit changed from `from` to `to`.
    Step { from: u32, to: u32 },
    /// The module limit stayed, although pressure asked for a step.
    Held(Hold),
}

/// RSS when the last step down was taken, and the module bytes it freed.
#[derive(Clone, Copy, Debug)]
struct StepDown {
    rss: usize,
    freed: usize,
}

/// When the module limit last changed, since when pressure has been low, and
/// what the last step down did while pressure has been high.
#[derive(Debug, Default)]
struct Policy {
    last_step: Option<Instant>,
    low_since: Option<Instant>,
    /// Cleared when pressure falls below high, so each period of high
    /// pressure starts with no record.
    last_step_down: Option<StepDown>,
}

impl Policy {
    /// Starts or stops the low-pressure timer, and forgets the last step down
    /// once pressure falls below high.
    fn observe(&mut self, pressure: MemoryPressure, now: Instant) {
        if pressure == MemoryPressure::Low {
            self.low_since.get_or_insert(now);
        } else {
            self.low_since = None;
        }
        if pressure < MemoryPressure::High {
            self.last_step_down = None;
        }
    }

    /// Whether [`STEP_INTERVAL`] has passed since the last step.
    fn step_interval_elapsed(&self, now: Instant) -> bool {
        self.last_step.is_none_or(|t| now - t >= STEP_INTERVAL)
    }

    /// Whether pressure has been low for [`LOW_PRESSURE_PERIOD`].
    fn low_pressure_period_elapsed(&self, now: Instant) -> bool {
        self.low_since
            .is_some_and(|t| now - t >= LOW_PRESSURE_PERIOD)
    }
}

/// Decides, for one runtime, how many modules each large Bloom filter keeps in
/// memory.
#[derive(Debug)]
pub(crate) struct BloomFilterManager {
    enabled: bool,
    /// The configured false positive rate, which filters return to when
    /// pressure is low.
    configured_rate: f64,
    /// Modules the configured rate keeps, which the module limit never exceeds.
    configured_modules: u32,
    /// Max number of modules an enrolled filter may keep.
    max_modules: AtomicU32,
    /// Every enrolled filter. Entries for dropped filters are pruned on each
    /// tick.
    filters: Mutex<Vec<Weak<TrackingBloomFilter>>>,
    /// Set when the module limit goes up or a filter enrolls below its target,
    /// and cleared when a restore pass finds no filter that can read modules
    /// back. While it is clear, a tick at low pressure only prunes the
    /// registry.
    restore_pending: AtomicBool,
    policy: Mutex<Policy>,
}

impl BloomFilterManager {
    pub(crate) fn new(enabled: bool, configured_rate: f64) -> Self {
        let configured_modules = modules_for_rate(configured_rate);
        Self {
            enabled,
            configured_rate,
            configured_modules,
            max_modules: AtomicU32::new(configured_modules),
            filters: Mutex::new(Vec::new()),
            restore_pending: AtomicBool::new(false),
            policy: Mutex::new(Policy::default()),
        }
    }

    /// Current module limit, or `None` when eviction is disabled.
    pub(crate) fn max_modules(&self) -> Option<u32> {
        self.enabled
            .then(|| self.max_modules.load(Ordering::Relaxed))
    }

    /// Wraps `filter` for a batch to share, and starts managing it, so that
    /// later ticks can trim it and restore it. Trims it to the current module
    /// limit.
    ///
    /// Leaves the filter unmanaged, and free of the cost of eviction, when
    /// eviction is disabled, when the filter cannot read its modules back from
    /// storage, or when it is too small for [`is_evictable_size`] at the
    /// configured rate. The size at the configured rate counts, not the
    /// resident size, because a filter loaded under pressure holds fewer
    /// modules and must still get them back.
    pub(crate) fn enroll(&self, mut filter: TrackingBloomFilter) -> Arc<TrackingBloomFilter> {
        if !self.enabled || !filter.has_module_handle() {
            return Arc::new(filter);
        }
        let full_bytes = filter.layout().bytes_per_module()
            * self.target(&filter, self.configured_modules) as usize;
        if !is_evictable_size(full_bytes) {
            return Arc::new(filter);
        }
        filter.make_evictable();
        let filter = Arc::new(filter);
        self.filters.lock().unwrap().push(Arc::downgrade(&filter));
        // Trim after enrolling, so a tick that lowers the module limit in
        // between still reaches this filter: it collects the filters to trim
        // after it stores the new limit.
        let target = self.target(&filter, self.max_modules.load(Ordering::Relaxed));
        filter.evict_to(target);
        // A filter loaded at a lower module limit holds fewer modules than the
        // current one allows, and a restore pass may have finished before it
        // enrolled.
        if filter.resident_modules() < target {
            self.restore_pending.store(true, Ordering::Release);
        }
        filter
    }

    /// Runs the policy once.
    ///
    /// # Arguments
    ///
    /// - `pressure`: the memory pressure the monitor just computed.
    /// - `memory`: RSS and the memory limit, or `None` when there is no memory
    ///   limit. Then every step that pressure asks for is taken.
    /// - `now`: the current time.
    pub(crate) fn tick(
        &self,
        pressure: MemoryPressure,
        memory: Option<Memory>,
        now: Instant,
    ) -> Tick {
        if !self.enabled {
            return Tick::default();
        }
        let mut policy = self.policy.lock().unwrap();
        policy.observe(pressure, now);

        let old = self.max_modules.load(Ordering::Relaxed);
        let interval_elapsed = policy.step_interval_elapsed(now);
        let step_down = pressure >= MemoryPressure::High && old > 0 && interval_elapsed;
        let step_up = !step_down
            && old < self.configured_modules
            && interval_elapsed
            && policy.low_pressure_period_elapsed(now);
        // Clear the flag before collecting the live filters, so a filter that
        // enrolls below its target after that sets it again for the next tick.
        let restore =
            pressure == MemoryPressure::Low && self.restore_pending.swap(false, Ordering::AcqRel);
        if !(step_down || step_up || restore) {
            // Nothing to decide, trim or restore, so only drop the entries of
            // dropped filters, without collecting the live ones.
            self.prune();
            return Tick::default();
        }
        let live = self.live_filters();
        #[cfg(test)]
        if let Some(hook) = AFTER_COLLECT.with(|hook| hook.borrow_mut().take()) {
            hook();
        }

        // Decide the new limit. A held step up is tried again on the next
        // tick, without waiting for another low-pressure period.
        let (new, held) = if step_down {
            match self.hold_step_down(&live, pressure, old, memory, policy.last_step_down) {
                Some(hold) => (old, Some(hold)),
                None => (old - 1, None),
            }
        } else if step_up {
            match self.hold_step_up(&live, old + 1, memory) {
                Some(hold) => (old, Some(hold)),
                None => (old + 1, None),
            }
        } else {
            (old, None)
        };
        if new != old {
            self.max_modules.store(new, Ordering::Relaxed);
            policy.last_step = Some(now);
        }
        if new > old {
            // Each further step up needs another low-pressure period.
            policy.low_since = Some(now);
        }

        // Apply it. A step up happens only at low pressure, so it restores.
        if new < old {
            policy.last_step_down = self.trim(new, memory);
        } else if restore || new > old {
            let headroom = memory.map_or(usize::MAX, |memory| memory.headroom());
            if self.restore(&live, new, headroom).is_some() {
                self.restore_pending.store(true, Ordering::Release);
            }
        }
        match held {
            Some(hold) => Tick::Held(hold),
            None if new != old => Tick::Step { from: old, to: new },
            None => Tick::Idle,
        }
    }

    /// Trims every enrolled filter to `max_modules`, and returns what the step
    /// down did, for [`Hold::StepDidNotHelp`].
    fn trim(&self, max_modules: u32, memory: Option<Memory>) -> Option<StepDown> {
        // Collect the filters after the new limit is stored, so a filter that
        // enrolled after the tick first collected them, and read the old limit,
        // is trimmed too.
        let live = self.live_filters();
        let resident = || live.iter().map(|f| f.resident_bytes()).sum::<usize>();
        let before = resident();
        for filter in &live {
            filter.evict_to(self.target(filter, max_modules));
        }
        let freed = before.saturating_sub(resident());
        memory.map(|memory| StepDown {
            rss: memory.rss,
            freed,
        })
    }

    /// Modules `filter` should hold when at most `max_modules` are allowed.
    fn target(&self, filter: &TrackingBloomFilter, max_modules: u32) -> u32 {
        filter.target_modules(self.configured_rate, max_modules)
    }

    /// Bytes `filter` holds above what it keeps at `max_modules`.
    fn freeable_bytes(&self, filter: &TrackingBloomFilter, max_modules: u32) -> usize {
        let kept = self.target(filter, max_modules) as usize * filter.layout().bytes_per_module();
        filter.resident_bytes().saturating_sub(kept)
    }

    /// Why the module limit should not go down from `max_modules` at
    /// `pressure`, or `None` to go down. `last_step_down` is the last step down
    /// of this period of high pressure.
    fn hold_step_down(
        &self,
        filters: &[Arc<TrackingBloomFilter>],
        pressure: MemoryPressure,
        max_modules: u32,
        memory: Option<Memory>,
        last_step_down: Option<StepDown>,
    ) -> Option<Hold> {
        // The last module costs the most lookups per byte: dropping it takes a
        // filter from a false positive rate of 0.1 to 1, while dropping the
        // fourth takes it from 1e-4 to 1e-3, for the same memory.
        if max_modules == 1 && pressure < MemoryPressure::Critical {
            return Some(Hold::LastModule);
        }
        let memory = memory?;
        let excess = memory.excess();
        // Open cursors keep the modules that earlier steps dropped until they
        // close. That memory goes without another step.
        let pinned: usize = filters.iter().map(|filter| filter.pinned_bytes()).sum();
        if pinned >= excess {
            return Some(Hold::Releasing { pinned, excess });
        }
        // Only bytes above the fewest modules this pressure allows can be
        // freed: a filter that needs few modules for its rate, or that already
        // holds fewer, frees nothing more.
        let floor = if pressure < MemoryPressure::Critical {
            1
        } else {
            0
        };
        let freeable: usize = filters.iter().map(|f| self.freeable_bytes(f, floor)).sum();
        let filter_bytes = pinned + freeable;
        if filter_bytes <= excess {
            return Some(Hold::FiltersTooSmall {
                filter_bytes,
                excess,
            });
        }
        // At critical pressure the process is close to running out of memory,
        // and evicting still helps even if other memory grew, so only below it
        // does a step wait for the last one to show in RSS. Modules that open
        // cursors hold are not expected to show yet.
        if pressure < MemoryPressure::Critical
            && let Some(last) = last_step_down
        {
            let freed = last.freed.saturating_sub(pinned);
            let dropped = last.rss.saturating_sub(memory.rss);
            if dropped < freed / STEP_HELPED_DIVISOR {
                return Some(Hold::StepDidNotHelp { freed, dropped });
            }
        }
        None
    }

    /// Why the module limit should not go up to `max_modules`, or `None` to go
    /// up.
    fn hold_step_up(
        &self,
        filters: &[Arc<TrackingBloomFilter>],
        max_modules: u32,
        memory: Option<Memory>,
    ) -> Option<Hold> {
        let memory = memory?;
        let mut needed = 0;
        let mut largest = 0;
        for filter in filters.iter().filter(|filter| filter.can_restore()) {
            let target = self.target(filter, max_modules);
            let missing = target.saturating_sub(filter.resident_modules()) as usize;
            if missing > 0 {
                let bytes = filter.layout().bytes_per_module();
                needed += missing * bytes;
                largest = largest.max(bytes);
            }
        }
        // A read holds one module twice for a moment, as read and as kept.
        let needed = needed + largest;
        let headroom = memory.headroom();
        (needed > headroom).then_some(Hold::NoRoom { needed, headroom })
    }

    /// Reads dropped modules back into `filters`, up to `max_modules`.
    ///
    /// Filters that answer the most lookups go first, since they gain the most
    /// from accuracy. Reads stop at [`RESTORE_BYTES_PER_TICK`], except that one
    /// module is always allowed, and never go past `headroom`, counting each
    /// module twice while it is read.
    ///
    /// Returns the bytes read, or `None` when no filter that can read modules
    /// back is below its target.
    fn restore(
        &self,
        filters: &[Arc<TrackingBloomFilter>],
        max_modules: u32,
        headroom: usize,
    ) -> Option<usize> {
        let mut wanting: Vec<_> = filters
            .iter()
            .filter(|f| f.can_restore())
            .map(|f| (f, self.target(f, max_modules)))
            .filter(|(f, target)| f.resident_modules() < *target)
            .collect();
        if wanting.is_empty() {
            return None;
        }
        wanting.sort_by_cached_key(|(f, _)| std::cmp::Reverse(f.probes()));

        let mut read = 0;
        'filters: for (filter, target) in wanting {
            let bytes = filter.layout().bytes_per_module();
            while filter.resident_modules() < target {
                // A read holds the module twice for a moment: as read from
                // storage, and as the copy the filter keeps. The first module
                // is read whatever its size.
                let no_room = read + 2 * bytes > headroom;
                let over_budget = read > 0 && read + bytes > RESTORE_BYTES_PER_TICK;
                if no_room || over_budget {
                    break 'filters;
                }
                match filter.restore_one() {
                    Ok(0) => break,
                    Ok(added) => read += added,
                    Err(error) => {
                        // Storage may be failing for every filter, so stop here
                        // and not use up the retries of every filter at once.
                        warn!("Bloom filter keeps its current accuracy: {error}");
                        break 'filters;
                    }
                }
            }
        }
        Some(read)
    }

    /// Drops the entries of dropped filters.
    fn prune(&self) {
        retain_live(&mut self.filters.lock().unwrap());
    }

    /// Prunes entries for dropped filters and returns the rest.
    pub(crate) fn live_filters(&self) -> Vec<Arc<TrackingBloomFilter>> {
        let mut filters = self.filters.lock().unwrap();
        retain_live(&mut filters);
        filters.iter().filter_map(Weak::upgrade).collect()
    }
}

/// Drops the entries of `filters` whose filter is gone.
fn retain_live(filters: &mut Vec<Weak<TrackingBloomFilter>>) {
    filters.retain(|weak| weak.strong_count() > 0);
}

#[cfg(test)]
mod tests {
    use super::AFTER_COLLECT;
    use super::{BloomFilterManager, Hold, LOW_PRESSURE_PERIOD, Memory, STEP_INTERVAL, Tick};
    use crate::storage::file::resident_modules;
    use crate::storage::tracking_bloom_filter::{
        BloomFilterModuleHandle, MAX_RESTORE_FAILURES, MIN_EVICTABLE_BYTES, TrackingBloomFilter,
    };
    use feldera_modular_bloom::{ModularBloomFilter, ModularBloomFilterBuilder, ModuleLayout};
    use feldera_types::memory_pressure::MemoryPressure::{self, *};
    use std::cell::RefCell;
    use std::rc::Rc;
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
    fn filled(keys: u64, fail: bool) -> (TrackingBloomFilter, Arc<AtomicUsize>) {
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
        (filter, reads)
    }

    fn enrolled(
        e: &BloomFilterManager,
        fail: bool,
    ) -> (Arc<TrackingBloomFilter>, Arc<AtomicUsize>) {
        let (filter, reads) = filled(KEYS, fail);
        assert!(filter.resident_bytes() >= MIN_EVICTABLE_BYTES);
        (e.enroll(filter), reads)
    }

    /// A memory limit large enough that no test filter matters to it.
    const LIMIT: usize = 1 << 40;

    /// Memory with `headroom` bytes left below moderate pressure.
    fn room(headroom: usize) -> Option<Memory> {
        let moderate = Memory {
            rss: 0,
            max_rss: LIMIT,
        }
        .moderate();
        Some(Memory {
            rss: moderate - headroom,
            max_rss: LIMIT,
        })
    }

    /// Memory `excess` bytes above where moderate pressure starts.
    fn over(excess: usize) -> Option<Memory> {
        let moderate = room(0).unwrap().rss;
        Some(Memory {
            rss: moderate + excess,
            max_rss: LIMIT,
        })
    }

    /// Runs `ticks` ticks of `pressure`, one second apart, from `*now`.
    fn run(e: &BloomFilterManager, pressure: MemoryPressure, ticks: u32, now: &mut Instant) {
        for _ in 0..ticks {
            *now += Duration::from_secs(1);
            e.tick(pressure, None, *now);
        }
    }

    /// Steps the module limit down from 4 to 2 with high pressure, then waits
    /// out one low-pressure period with no room to restore, so the next low
    /// tick with room raises it to 3.
    fn down_to_two_then_low_pressure(e: &BloomFilterManager, now: &mut Instant) {
        e.tick(High, None, *now);
        run(e, High, STEP_INTERVAL.as_secs() as u32, now);
        assert_eq!(e.max_modules(), Some(2));
        for _ in 0..LOW_PRESSURE_PERIOD.as_secs() {
            *now += Duration::from_secs(1);
            assert_eq!(e.tick(Low, room(0), *now), Tick::Idle);
        }
        assert_eq!(e.max_modules(), Some(2));
    }

    #[test]
    fn disabled_manager_enrolls_nothing() {
        let e = BloomFilterManager::new(false, 1e-4);
        enrolled(&e, false);
        assert!(e.live_filters().is_empty());
        assert_eq!(e.tick(Critical, None, Instant::now()), Tick::Idle);
        assert_eq!(e.max_modules(), None);
    }

    /// Only large filters that can read their modules back are enrolled.
    #[test]
    fn only_large_filters_with_a_module_handle_are_enrolled() {
        let e = BloomFilterManager::new(true, 1e-4);
        let (small, _) = filled(10_000, false);
        assert!(small.resident_bytes() < MIN_EVICTABLE_BYTES);
        let modules = small.resident_modules();
        let small = e.enroll(small);
        small.evict_to(0);
        assert_eq!(
            small.resident_modules(),
            modules,
            "a filter the manager left alone lost modules"
        );
        let (large, _) = filled(KEYS, false);
        let no_handle = TrackingBloomFilter::loaded(large.built().as_ref().clone());
        e.enroll(no_handle);
        assert!(e.live_filters().is_empty());

        let _large = e.enroll(large);
        assert_eq!(e.live_filters().len(), 1);
    }

    /// High pressure steps the module limit down once per interval, trims
    /// enrolled filters without false negatives, and frees their modules.
    #[test]
    fn high_pressure_steps_down_and_frees_modules() {
        let e = BloomFilterManager::new(true, 1e-4);
        let (filter, _) = enrolled(&e, false);
        let full = filter.resident_bytes();
        let mut now = Instant::now();

        assert_eq!(e.tick(Low, None, now), Tick::Idle);
        assert_eq!(e.max_modules(), Some(4));

        now += Duration::from_secs(1);
        assert_eq!(e.tick(High, None, now), Tick::Step { from: 4, to: 3 });
        assert_eq!(filter.resident_modules(), 3);
        assert_eq!(filter.resident_bytes(), full / 4 * 3);

        // No second step before the interval passes.
        run(&e, High, STEP_INTERVAL.as_secs() as u32 - 1, &mut now);
        assert_eq!(e.max_modules(), Some(3));
        run(&e, High, 1, &mut now);
        assert_eq!(e.max_modules(), Some(2));

        run(&e, Critical, 2 * STEP_INTERVAL.as_secs() as u32, &mut now);
        assert_eq!(e.max_modules(), Some(0));
        assert_eq!(filter.resident_bytes(), 0);
        assert!((0..KEYS).all(|i| filter.contains_hash(key(i))));

        // A filter enrolled now starts at the module limit.
        let (late, _) = enrolled(&e, false);
        assert_eq!(late.resident_modules(), 0);
    }

    /// The module limit starts at what the configured rate keeps, and the last
    /// module goes only at critical pressure.
    #[test]
    fn the_last_module_goes_only_at_critical_pressure() {
        let e = BloomFilterManager::new(true, 1e-1);
        assert_eq!(e.max_modules(), Some(1));
        let now = Instant::now();
        let tick = e.tick(High, None, now);
        assert_eq!(tick, Tick::Held(Hold::LastModule));
        assert_eq!(e.tick(Critical, None, now), Tick::Step { from: 1, to: 0 });
    }

    /// High pressure that the filters could not end by themselves evicts
    /// nothing, and pressure they could end evicts as usual. Below critical
    /// pressure the last module cannot be freed, so it does not count.
    #[test]
    fn filters_that_cannot_end_the_pressure_are_not_evicted() {
        let e = BloomFilterManager::new(true, 1e-4);
        let (filter, _) = enrolled(&e, false);
        let full = filter.resident_bytes();
        let module = filter.layout().bytes_per_module();
        let now = Instant::now();

        let tick = e.tick(Critical, over(full), now);
        assert_eq!(
            tick,
            Tick::Held(Hold::FiltersTooSmall {
                filter_bytes: full,
                excess: full
            })
        );
        assert_eq!(filter.resident_bytes(), full);

        let freeable = full - module;
        let tick = e.tick(High, over(freeable), now);
        assert_eq!(
            tick,
            Tick::Held(Hold::FiltersTooSmall {
                filter_bytes: freeable,
                excess: freeable
            })
        );

        assert_eq!(
            e.tick(High, over(freeable - 1), now),
            Tick::Step { from: 4, to: 3 }
        );
        assert_eq!(filter.resident_modules(), 3);
    }

    /// The module limit goes up only when every module of the step, plus one
    /// module in flight, fits below moderate pressure.
    #[test]
    fn a_step_up_waits_until_its_modules_fit() {
        let e = BloomFilterManager::new(true, 1e-4);
        let (first, _) = enrolled(&e, false);
        let (second, _) = enrolled(&e, false);
        let module = first.layout().bytes_per_module();
        let mut now = Instant::now();
        down_to_two_then_low_pressure(&e, &mut now);

        // Two filters each want one module back, and one more is in flight.
        now += Duration::from_secs(1);
        let tick = e.tick(Low, room(3 * module - 1), now);
        assert_eq!(
            tick,
            Tick::Held(Hold::NoRoom {
                needed: 3 * module,
                headroom: 3 * module - 1
            })
        );

        // The next tick tries again, without another low-pressure period.
        now += Duration::from_secs(1);
        let tick = e.tick(Low, room(3 * module), now);
        assert_eq!(tick, Tick::Step { from: 2, to: 3 });
        assert_eq!(first.resident_modules(), 3);
        assert_eq!(second.resident_modules(), 3);
    }

    /// Low pressure raises the module limit one step per low-pressure period,
    /// never above the configured count, and filters are restored to it.
    #[test]
    fn low_pressure_raises_the_module_limit_slowly() {
        let e = BloomFilterManager::new(true, 1e-4);
        let (filter, _) = enrolled(&e, false);
        let mut now = Instant::now();
        e.tick(Critical, None, now);
        assert_eq!(e.max_modules(), Some(3));

        // Moderate pressure does not count as low.
        run(
            &e,
            Moderate,
            2 * LOW_PRESSURE_PERIOD.as_secs() as u32,
            &mut now,
        );
        assert_eq!(e.max_modules(), Some(3));

        // The first low-pressure tick starts the period, so it ends one tick later.
        run(&e, Low, LOW_PRESSURE_PERIOD.as_secs() as u32, &mut now);
        assert_eq!(e.max_modules(), Some(3));
        run(&e, Low, 1, &mut now);
        assert_eq!(e.max_modules(), Some(4));
        assert_eq!(filter.resident_modules(), 4);

        run(&e, Low, 2 * LOW_PRESSURE_PERIOD.as_secs() as u32, &mut now);
        assert_eq!(e.max_modules(), Some(4));
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
        let filter = e.enroll(filter);
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

    /// Filters read modules back the busiest first, and never past the
    /// headroom.
    #[test]
    fn restoring_reads_the_busiest_filter_first() {
        let e = BloomFilterManager::new(true, 1e-4);
        let (quiet, _) = enrolled(&e, false);
        let (busy, _) = enrolled(&e, false);
        for i in 0..100 {
            busy.contains_hash(key(i));
        }
        let module = busy.layout().bytes_per_module();
        quiet.evict_to(2);
        busy.evict_to(2);
        let filters = e.live_filters();

        // A read holds the module twice for a moment, so room for one module
        // is not enough.
        assert_eq!(e.restore(&filters, 3, module), Some(0));

        // Room for one read: only the busy filter gets it.
        assert_eq!(e.restore(&filters, 3, 2 * module), Some(module));
        assert_eq!(busy.resident_modules(), 3);
        assert_eq!(quiet.resident_modules(), 2);

        assert_eq!(e.restore(&filters, 3, usize::MAX), Some(module));
        assert_eq!(quiet.resident_modules(), 3);
        assert!((0..KEYS).all(|i| quiet.contains_hash(key(i))));
        assert_eq!(e.restore(&filters, 3, usize::MAX), None);
    }

    /// Restoring stops at moderate pressure, even with modules still missing.
    #[test]
    fn moderate_pressure_does_not_restore() {
        let e = BloomFilterManager::new(true, 1e-4);
        let (filter, reads) = enrolled(&e, false);
        let mut now = Instant::now();
        down_to_two_then_low_pressure(&e, &mut now);
        now += Duration::from_secs(1);
        e.tick(Low, None, now);
        assert_eq!(e.max_modules(), Some(3));
        assert_eq!(filter.resident_modules(), 3);
        let read = reads.load(AtomicOrdering::Relaxed);

        // Lose the module again, as if the restore had not finished.
        filter.evict_to(2);
        run(&e, Moderate, 5, &mut now);
        assert_eq!(filter.resident_modules(), 2);
        assert_eq!(reads.load(AtomicOrdering::Relaxed), read);
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
        let filter = e.enroll(filter);

        let mut now = Instant::now();
        down_to_two_then_low_pressure(&e, &mut now);
        run(&e, Low, 10, &mut now);
        assert_eq!(filter.resident_modules(), 2);
        assert_eq!(reads.load(AtomicOrdering::Relaxed), 1);
    }

    /// A filter loaded with only the first `modules` modules of a full one, as
    /// a load under pressure gives, which reads the rest back from memory.
    fn loaded_with(modules: usize) -> TrackingBloomFilter {
        loaded_failing(modules, false)
    }

    /// Like [`loaded_with`], but every read of a module back fails with
    /// `fail`.
    fn loaded_failing(modules: usize, fail: bool) -> TrackingBloomFilter {
        let (full, _) = filled(KEYS, false);
        let built = full.built();
        let words: Vec<u64> = built
            .modules()
            .into_iter()
            .take(modules)
            .flat_map(|module| module.to_vec())
            .collect();
        let mut filter = TrackingBloomFilter::loaded(
            ModularBloomFilter::from_modules_with_density(
                *built.layout(),
                &words,
                built.density().clone(),
            )
            .unwrap(),
        );
        filter.set_module_handle(BloomFilterModuleHandle::Memory {
            modules: built.modules().into_iter().map(Arc::from).collect(),
            fail,
            reads: Arc::new(AtomicUsize::new(0)),
        });
        filter
    }

    /// A filter loaded under pressure with no module still enrolls, because its
    /// size at the configured rate counts, so it can read its modules back.
    #[test]
    fn a_filter_loaded_with_no_module_still_enrolls() {
        let e = BloomFilterManager::new(true, 1e-4);
        let empty = loaded_with(0);
        assert_eq!(empty.resident_modules(), 0);
        let empty = e.enroll(empty);
        assert_eq!(e.live_filters().len(), 1, "the filter did not enroll");
        while empty.restore_one().unwrap() > 0 {}
        assert_eq!(empty.resident_modules(), 4);
        assert!((0..KEYS).all(|i| empty.contains_hash(key(i))));
    }

    /// A filter that enrolls with fewer modules than the module limit allows,
    /// because it loaded at a lower one, reads them back although the module
    /// limit does not go up again.
    #[test]
    fn a_filter_enrolled_below_its_target_is_restored() {
        let e = BloomFilterManager::new(true, 1e-4);
        let filter = e.enroll(loaded_with(2));
        assert_eq!(filter.resident_modules(), 2);
        e.tick(Low, None, Instant::now());
        assert_eq!(filter.resident_modules(), 4);
    }

    /// A filter that gave up reading its modules back is not waited for: it
    /// holds no step up, and leaves no restore pending.
    #[test]
    fn a_filter_that_gave_up_is_not_waited_for() {
        let e = BloomFilterManager::new(true, 1e-4);
        let (filter, reads) = enrolled(&e, true);
        filter.evict_to(2);
        let live = e.live_filters();
        assert!(e.hold_step_up(&live, 3, room(0)).is_some());
        for _ in 0..3 {
            assert!(filter.restore_one().is_err());
        }
        assert!(!filter.can_restore());
        assert_eq!(e.hold_step_up(&live, 3, room(0)), None);
        assert_eq!(e.restore(&live, 3, usize::MAX), None);
        assert_eq!(reads.load(AtomicOrdering::Relaxed), 3);
    }

    /// Modules that open cursors still hold count as freed, so while they
    /// would end the pressure by themselves, the module limit stays.
    #[test]
    fn modules_that_open_cursors_hold_are_waited_for() {
        let e = BloomFilterManager::new(true, 1e-4);
        let (filter, _) = enrolled(&e, false);
        let module = filter.layout().bytes_per_module();
        let mut now = Instant::now();
        let snapshot = filter.snapshot();
        assert_eq!(
            e.tick(High, over(module), now),
            Tick::Step { from: 4, to: 3 }
        );
        assert_eq!(filter.pinned_bytes(), module);

        now += STEP_INTERVAL;
        let tick = e.tick(High, over(module), now);
        assert_eq!(
            tick,
            Tick::Held(Hold::Releasing {
                pinned: module,
                excess: module
            })
        );

        // Closing the cursor frees the module, and RSS falls.
        drop(snapshot);
        assert_eq!(filter.pinned_bytes(), 0);
        assert_eq!(
            e.tick(High, over(module / 2), now),
            Tick::Step { from: 3, to: 2 }
        );
    }

    /// Below critical pressure, a step down waits until RSS has dropped by a
    /// quarter of what the last step freed. Critical pressure does not wait,
    /// and pressure below high forgets the last step.
    #[test]
    fn a_step_that_did_not_help_holds_the_next_one() {
        let e = BloomFilterManager::new(true, 1e-4);
        let (filter, _) = enrolled(&e, false);
        let module = filter.layout().bytes_per_module();
        let mut now = Instant::now();
        let before = filter.resident_bytes();
        assert_eq!(
            e.tick(High, over(module), now),
            Tick::Step { from: 4, to: 3 }
        );
        assert_eq!(before - filter.resident_bytes(), module);

        // RSS did not move, as if other memory grew by what the step freed.
        now += STEP_INTERVAL;
        let tick = e.tick(High, over(module), now);
        assert_eq!(
            tick,
            Tick::Held(Hold::StepDidNotHelp {
                freed: module,
                dropped: 0
            })
        );
        assert_eq!(
            e.tick(High, over(module - module / 4 + 1), now),
            Tick::Held(Hold::StepDidNotHelp {
                freed: module,
                dropped: module / 4 - 1
            })
        );

        // A drop of a quarter is enough.
        let three_quarters = over(module - module / 4);
        assert_eq!(
            e.tick(High, three_quarters, now),
            Tick::Step { from: 3, to: 2 }
        );

        // The step from 3 to 2 did not help either, but pressure fell below
        // high, so the next period starts with no record.
        now += STEP_INTERVAL;
        assert_eq!(
            e.tick(High, three_quarters, now),
            Tick::Held(Hold::StepDidNotHelp {
                freed: module,
                dropped: 0
            })
        );
        e.tick(Moderate, three_quarters, now);
        assert_eq!(
            e.tick(High, three_quarters, now),
            Tick::Step { from: 2, to: 1 }
        );

        // Critical pressure does not wait for the last step.
        now += STEP_INTERVAL;
        assert_eq!(
            e.tick(Critical, three_quarters, now),
            Tick::Step { from: 1, to: 0 }
        );
    }

    /// Modules that a cursor still holds are not expected to show in RSS
    /// yet, so they do not count against the last step.
    #[test]
    fn modules_cursors_hold_do_not_count_against_a_step() {
        let e = BloomFilterManager::new(true, 1e-4);
        let (filter, _) = enrolled(&e, false);
        let module = filter.layout().bytes_per_module();
        let mut now = Instant::now();
        let _snapshot = filter.snapshot();
        assert_eq!(
            e.tick(High, over(2 * module), now),
            Tick::Step { from: 4, to: 3 }
        );
        assert_eq!(filter.pinned_bytes(), module);

        now += STEP_INTERVAL;
        assert_eq!(
            e.tick(High, over(2 * module), now),
            Tick::Step { from: 3, to: 2 }
        );
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
        let filter = e.enroll(filter);
        assert_eq!(filter.resident_modules(), on_load);
    }

    /// Runs `hook` in the next tick on this thread, right after the tick
    /// collects the live filters.
    fn after_collect(hook: impl FnOnce() + 'static) {
        AFTER_COLLECT.with(|slot| *slot.borrow_mut() = Some(Box::new(hook)));
    }

    /// A filter that enrolls while a tick lowers the module limit, after the
    /// tick collected the filters, is trimmed to the new limit.
    #[test]
    fn a_filter_enrolled_during_a_step_down_is_trimmed() {
        let e = Arc::new(BloomFilterManager::new(true, 1e-4));
        let late = Rc::new(RefCell::new(None));
        after_collect({
            let (e, late) = (e.clone(), late.clone());
            move || *late.borrow_mut() = Some(enrolled(&e, false).0)
        });
        assert_eq!(
            e.tick(High, None, Instant::now()),
            Tick::Step { from: 4, to: 3 }
        );
        let late = late.borrow_mut().take().unwrap();
        assert_eq!(late.resident_modules(), 3);
    }

    /// A filter that enrolls below its target while a tick restores, after the
    /// tick collected the filters, is restored by the next tick, although the
    /// first found nothing to restore.
    #[test]
    fn a_filter_enrolled_during_a_restore_is_restored_later() {
        let e = Arc::new(BloomFilterManager::new(true, 1e-4));
        // Leaves a restore pending for a filter that is gone by the next tick.
        drop(e.enroll(loaded_with(2)));
        let late = Rc::new(RefCell::new(None));
        after_collect({
            let (e, late) = (e.clone(), late.clone());
            move || *late.borrow_mut() = Some(e.enroll(loaded_with(2)))
        });
        let now = Instant::now();
        e.tick(Low, None, now);
        let late = late.borrow_mut().take().unwrap();
        assert_eq!(late.resident_modules(), 2);
        e.tick(Low, None, now + Duration::from_secs(1));
        assert_eq!(late.resident_modules(), 4);
    }

    /// A read error ends the restore pass, so storage that fails for a few
    /// ticks uses up the retries of one filter, not of every filter.
    #[test]
    fn a_read_error_ends_the_restore_pass() {
        let e = BloomFilterManager::new(true, 1e-4);
        let filters = [
            e.enroll(loaded_failing(2, true)),
            e.enroll(loaded_failing(2, true)),
        ];
        let mut now = Instant::now();
        for _ in 0..MAX_RESTORE_FAILURES {
            now += Duration::from_secs(1);
            e.tick(Low, None, now);
        }
        let retrying = filters.iter().filter(|f| f.can_restore()).count();
        assert_eq!(retrying, 1);
    }
}
