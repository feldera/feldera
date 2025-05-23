//! Implementation of a [`Trace`] which merges batches in the background.
//!
//! This is a "spine", a [`Trace`] that internally consists of a vector of
//! batches. Inserting a new batch appends to the vector, and iterating or
//! searching a spine iterates or searches all of the batches in the vector.
//! The cost of these operations grows with the number of batches in the vector,
//! so it is beneficial to reduce the number by merging batches.

use crate::{
    circuit::{
        metadata::{MetaItem, OperatorMeta},
        metrics::COMPACTION_STALL_TIME_NANOSECONDS,
    },
    dynamic::{DynVec, Factory, Weight},
    storage::buffer_cache::CacheStats,
    time::Timestamp,
    trace::{
        cursor::CursorList,
        merge_batches,
        ord::fallback::pick_merge_destination,
        spine_async::{
            list_merger::ArcListMerger, push_merger::ArcPushMerger, snapshot::FetchList,
        },
        Batch, BatchReader, BatchReaderFactories, Builder, Cursor, Filter, Trace,
    },
    Error, NumEntries, Runtime,
};

use crate::storage::file::to_bytes;
pub use crate::trace::spine_async::snapshot::SpineSnapshot;
use crate::trace::CommittedSpine;
use enum_map::EnumMap;
use feldera_storage::StoragePath;
use feldera_types::checkpoint::PSpineBatches;
use ouroboros::self_referencing;
use rand::Rng;
use rkyv::{
    de::deserializers::SharedDeserializeMap, ser::Serializer, Archive, Archived, Deserialize,
    Fallible, Serialize,
};
use size_of::{Context, SizeOf};
use std::sync::{Arc, MutexGuard};
use std::time::{Duration, Instant};
use std::{collections::VecDeque, sync::atomic::Ordering};
use std::{
    fmt::{self, Debug, Display, Formatter},
    ops::DerefMut,
    sync::Condvar,
};
use std::{ops::RangeInclusive, sync::Mutex};
use textwrap::indent;

mod index_set;
mod list_merger;
mod push_merger;
mod snapshot;
use self::thread::{BackgroundThread, WorkerStatus};

use super::{cursor::CursorFactory, BatchLocation};

mod thread;

pub use list_merger::ListMerger;

/// Maximum amount of levels in the spine.
pub(crate) const MAX_LEVELS: usize = 9;

impl<B: Batch + Send + Sync> From<(Vec<String>, &Spine<B>)> for CommittedSpine {
    fn from((batches, spine): (Vec<String>, &Spine<B>)) -> Self {
        CommittedSpine {
            batches,
            merged: Vec::new(),
            effort: 0,
            dirty: spine.dirty,
        }
    }
}

/// A group of batches with similar sizes (as determined by [size_from_level]).
#[derive(Clone, SizeOf)]
struct Slot<B>
where
    B: Batch,
{
    /// Optionally, a list of batches that are currently being merged.  These
    /// batches are not in `loose_batches`.
    ///
    /// Invariant: the batches (if present) must be non-empty.
    merging_batches: Option<Vec<Arc<B>>>,

    /// Zero or more batches not currently being merged.
    ///
    /// Invariant: the batches must be non-empty.
    loose_batches: VecDeque<Arc<B>>,

    /// Amount of time spent merging batches at this level.
    elapsed: Duration,

    /// Number of completed merges at this level.
    n_merged: usize,

    /// Number of batches input to the completed merges.
    n_merged_batches: usize,

    /// Number of merge steps required to complete the merges so far.
    n_steps: usize,
}

impl<B> Default for Slot<B>
where
    B: Batch,
{
    fn default() -> Self {
        Self {
            merging_batches: None,
            loose_batches: VecDeque::new(),
            elapsed: Duration::ZERO,
            n_merged: 0,
            n_merged_batches: 0,
            n_steps: 0,
        }
    }
}

impl<B> Slot<B>
where
    B: Batch,
{
    /// If this slot doesn't currently have an ongoing merge, and it does have
    /// at least two loose batches, picks an upper limit of the loose batches and makes
    /// them into merging batches, and returns those batches. Otherwise, returns
    /// `None` without changing anything.
    ///
    /// We merge the least recently added batches (ensuring that batches
    /// eventually get merged).
    fn try_start_merge(&mut self, level: usize) -> Option<Vec<Arc<B>>> {
        /// Minimum and maximum numbers of batches to merge at each level.
        ///
        /// The minimum number of batches to merge is key to performance.  The
        /// maximum number seems much less important.
        const MERGE_COUNTS: [RangeInclusive<usize>; MAX_LEVELS] = [
            8..=64,
            8..=64,
            3..=64,
            3..=64,
            3..=64,
            3..=64,
            2..=64,
            2..=64,
            2..=64,
        ];

        let merge_counts = &MERGE_COUNTS[level];
        if self.merging_batches.is_none() && self.loose_batches.len() >= *merge_counts.start() {
            let n = std::cmp::min(*merge_counts.end(), self.loose_batches.len());
            let batches = self.loose_batches.drain(..n).collect::<Vec<_>>();
            self.merging_batches = Some(batches.clone());
            Some(batches)
        } else {
            None
        }
    }

    /// Returns the number of batches in the slot, whether loose or merging.
    fn n_batches(&self) -> usize {
        self.all_batches().count()
    }

    /// Returns an iterator over all batches in the slot, whether loose or
    /// merging.
    fn all_batches(&self) -> impl Iterator<Item = &Arc<B>> {
        self.loose_batches
            .iter()
            .chain(self.merging_batches.iter().flatten())
    }
}

/// State shared between the merger thread and the main thread.
///
/// This shared state is accessed through a `Mutex`, which we try to hold for as
/// short a time as possible.
#[derive(SizeOf)]
struct SharedState<B>
where
    B: Batch,
{
    #[size_of(skip)]
    key_filter: Option<Filter<B::Key>>,
    #[size_of(skip)]
    value_filter: Option<Filter<B::Val>>,
    #[size_of(skip)]
    frontier: B::Time,
    slots: [Slot<B>; MAX_LEVELS],
    #[size_of(skip)]
    request_exit: bool,
    #[size_of(skip)]
    spine_stats: SpineStats,
}

impl<B> SharedState<B>
where
    B: Batch,
{
    pub fn new() -> Self {
        Self {
            key_filter: None,
            value_filter: None,
            frontier: B::Time::minimum(),
            slots: std::array::from_fn(|_| Slot::default()),
            request_exit: false,
            spine_stats: SpineStats::default(),
        }
    }

    /// Adds all of `batches` as (initially) loose batches.  They will be merged
    /// when the merger thread has a chance (although it might not be awake).
    fn add_batches(&mut self, batches: impl IntoIterator<Item = Arc<B>>) {
        for batch in batches {
            if !batch.is_empty() {
                self.add_batch(batch);
            }
        }
    }

    /// Add `batch` as an (initially) loose batch, which will be merged when
    /// the merger thread has a chance (although it might not be awake).
    fn add_batch(&mut self, batch: Arc<B>) {
        debug_assert!(!batch.is_empty());
        let level = Spine::<B>::size_to_level(batch.len());
        self.slots[level].loose_batches.push_back(batch);
    }

    fn should_apply_backpressure(&self) -> bool {
        const HIGH_THRESHOLD: usize = 128;
        self.slots
            .iter()
            .map(|s| s.loose_batches.len())
            .sum::<usize>()
            >= HIGH_THRESHOLD
    }

    fn should_relieve_backpressure(&self) -> bool {
        const LOWER_THRESHOLD: usize = 127;
        self.slots
            .iter()
            .map(|s| s.loose_batches.len())
            .sum::<usize>()
            <= LOWER_THRESHOLD
    }

    fn get_filters(&self) -> (Option<Filter<B::Key>>, Option<Filter<B::Val>>) {
        (self.key_filter.clone(), self.value_filter.clone())
    }

    /// Gets a copy of all of the batches (whether loose or being merged).
    fn get_batches(&self) -> Vec<Arc<B>> {
        let mut batches = Vec::with_capacity(self.slots.iter().map(Slot::n_batches).sum());
        for slot in &self.slots {
            batches.extend(slot.all_batches().cloned());
        }
        batches
    }

    /// Removes the loose batches and returns them.  This ensures that the
    /// merger thread will not initiate any more merges.
    fn take_loose_batches(&mut self) -> Vec<Arc<B>> {
        let mut loose_batches =
            Vec::with_capacity(self.slots.iter().map(|slot| slot.loose_batches.len()).sum());
        for slot in &mut self.slots {
            loose_batches.extend(slot.loose_batches.drain(..));
        }
        loose_batches
    }

    /// Returns true if any merging work is currently going on.
    ///
    /// If this returns false, a new merge might still start without any further
    /// batches being submitted if there are enough loose batches.
    fn is_merging(&self) -> bool {
        self.slots.iter().any(|slot| slot.merging_batches.is_some())
    }

    /// Finishes up the ongoing merge at the given `level`, which completed in
    /// `elapsed` time over `n_steps` steps, with `new_batch` as the result.
    fn merge_complete(
        &mut self,
        level: usize,
        new_batch: Arc<B>,
        elapsed: Duration,
        n_steps: usize,
    ) {
        let slot = &mut self.slots[level];
        let batches = slot.merging_batches.take().unwrap();
        slot.n_merged += 1;
        slot.n_merged_batches += batches.len();
        slot.elapsed += elapsed;
        slot.n_steps += n_steps;
        let cache_stats = batches.iter().fold(CacheStats::default(), |stats, batch| {
            stats + batch.cache_stats()
        });
        self.spine_stats.report_merge(
            batches.iter().map(|b| b.len()).sum(),
            new_batch.len(),
            cache_stats,
        );
        self.add_batches([new_batch]);
    }

    /// Returns a copy of the data that the caller can use to construct a
    /// metadata report.
    ///
    /// This is better than constructing the report here directly, because part
    /// of that is measuring the size of the batches, which can require I/O.
    fn metadata_snapshot(&self) -> ([Slot<B>; MAX_LEVELS], SpineStats) {
        (self.slots.clone(), self.spine_stats.clone())
    }
}

/// A fully asynchronous merger.
///
/// Merging has two benefits:
///
/// 1. To make iteration and searching cheaper because we have fewer batches to
///    iterate and search in our CursorLists.
///
/// 2. In some cases only, to reduce the total amount of data, because weights
///    summarize multiple items or cancel each other out or because filters drop
///    data.
///
/// We only get these benefits when we complete a merge. An ongoing but
/// incomplete merge only has a cost (the CPU we invested in it, plus memory or
/// storage), with no benefits. Therefore, it is to our benefit to both complete
/// as many merges as possible and to have as few ongoing merges as possible.
///
/// Since the smallest merges are cheapest, one might conclude that we should
/// only do one merge at a time and that that should be the two smallest
/// batches. In a static scenario, that would indeed be the right choice. But we
/// tend to be getting a new smallest batch on every step. With that, the
/// strategy of always doing the smallest merge piles up larger batches that
/// never get merged. For example, suppose that we add a new batch with size 1
/// in every step, we start merging the smallest runs whenever we first a merge,
/// and that a merge takes two steps. Then we end up with something like this,
/// where each line is a step that adds a new batch of size 1,` ()` designates
/// that batches are being merged, and `->` shows that a merge was finished or
/// started within the step:
///
/// ```text
/// 1
/// 1 1 -> (1 1)
/// 1 (1 1)
/// 1 1 (1 1) -> 1 1 2 -> 2 (1 1)
/// 1 2 (1 1)
/// 1 1 2 (1 1) -> 1 1 2 2 -> 2 2 (1 1)
/// 1 2 2 (1 1)
/// 1 1 2 2 (1 1) -> 1 1 2 2 2 -> 2 2 2 (1 1)
/// ````
///
/// The result is that we pile up runs that are slightly longer than the
/// shortest and they never get merged.
///
/// So, we still want to complete as many merges as possible and to have as few
/// ongoing merges as possible, but "as few ongoing merges as possible" needs to
/// be more than one and needs to include merges that are bigger than the
/// smallest possible merge.
///
/// The design of this merger does both. We divide batches into categories based
/// on their "level", which is roughly the base-10 log of their size (see
/// [Spine::size_to_level]). We run one merge per level at a time, only merging
/// batches in the same level with each.  When a level contains more than two
/// batches, we merge the least recently added batch (ensuring that batches
/// eventually get merged) with the one closest in size (ensuring that the merge
/// is as efficient as it can be). The result of a merge might be in the next
/// higher level, ensuring that larger merges eventually happen.
struct AsyncMerger<B>
where
    B: Batch,
{
    /// State shared with the background thread.
    state: Arc<Mutex<SharedState<B>>>,

    /// Allows us to wait for the background worker until we no longer want to
    /// block for backpressure.
    no_backpressure: Arc<Condvar>,

    /// Allows us to wait for the background worker to become idle.
    idle: Arc<Condvar>,
}

impl<B> AsyncMerger<B>
where
    B: Batch,
{
    fn new() -> Self {
        let idle = Arc::new(Condvar::new());
        let no_backpressure = Arc::new(Condvar::new());
        let state = Arc::new(Mutex::new(SharedState::new()));
        BackgroundThread::add_worker({
            let state = Arc::clone(&state);
            let idle = Arc::clone(&idle);
            let no_backpressure = Arc::clone(&no_backpressure);
            Box::new(|| {
                let mut mergers = std::array::from_fn(|_| None);
                let merger_type = Runtime::with_dev_tweaks(|tweaks| tweaks.merger);
                Box::new(move |worker_state| {
                    Self::run(
                        worker_state,
                        &mut mergers,
                        merger_type,
                        &state,
                        &idle,
                        &no_backpressure,
                    )
                })
            })
        });
        Self {
            state,
            idle,
            no_backpressure,
        }
    }
    fn set_key_filter(&self, key_filter: &Filter<B::Key>) {
        self.state.lock().unwrap().key_filter = Some(key_filter.clone());
    }
    fn set_value_filter(&self, value_filter: &Filter<B::Val>) {
        self.state.lock().unwrap().value_filter = Some(value_filter.clone());
    }

    fn set_frontier(&self, frontier: &B::Time) {
        self.state.lock().unwrap().frontier = frontier.clone();
    }

    /// Adds `batch` to the shared merging state and wakes up the merger.
    fn add_batch(&self, batch: Arc<B>) {
        debug_assert!(!batch.is_empty());
        let mut state = self.state.lock().unwrap();
        state.add_batch(batch);
        BackgroundThread::wake();
        if state.should_apply_backpressure() {
            let start = Instant::now();
            let mut state = self.no_backpressure.wait(state).unwrap();
            state.spine_stats.backpressure_wait += start.elapsed();
            COMPACTION_STALL_TIME_NANOSECONDS
                .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
        }
    }

    /// Adds `batches` to the shared merging state and wakes up the merger.
    fn add_batches(&self, batches: impl IntoIterator<Item = Arc<B>>) {
        self.state.lock().unwrap().add_batches(batches);
        BackgroundThread::wake();
    }

    /// Gets the complete set of batches to include in the spine.
    fn get_batches(&self) -> Vec<Arc<B>> {
        self.state.lock().unwrap().get_batches()
    }

    /// Pauses merging, by stopping the initiation of new merges and waiting for
    /// ongoing merges to finish.  Removes all of the batches from the merger
    /// and returns them. The caller can resume merging by passing those batches
    /// back to [Self::resume].
    fn pause(&self) -> Vec<Arc<B>> {
        let mut state = self.state.lock().unwrap();
        let mut batches = state.take_loose_batches();
        let mut state = self
            .idle
            .wait_while(state, |state| state.is_merging())
            .unwrap();
        batches.extend(state.take_loose_batches());
        batches
    }

    /// Stops the initiation of new merges.  Returns `(not_merging, merging)`,
    /// where `not_merging` is the batches that were not being merged and
    /// `merging` is the ones that were. Merging of batches in `merging` will
    /// continue, and further merges of the results of those merges could happen
    /// as well. The caller can re-insert `not_merging` later by passing it to
    /// [Self::resume].
    fn pause_new_merges(&self) -> (Vec<Arc<B>>, Vec<Arc<B>>) {
        let mut state = self.state.lock().unwrap();
        let not_merging = state.take_loose_batches();
        let merging = state.get_batches();
        (not_merging, merging)
    }

    /// Starts merging again with `batches`, which are presumably what
    /// [Self::pause] or [Self::pause_new_merges] returned.
    fn resume(&self, batches: impl IntoIterator<Item = Arc<B>>) {
        self.add_batches(batches);
    }

    fn metadata(&self, meta: &mut OperatorMeta) {
        let (mut slots, spine_stats) = self.state.lock().unwrap().metadata_snapshot();

        // Construct per-slot occupancy description.
        for (index, slot) in slots.iter_mut().enumerate() {
            for (class, batches) in [
                ("loose", slot.loose_batches.make_contiguous() as &_),
                (
                    "merging",
                    slot.merging_batches
                        .as_ref()
                        .unwrap_or(&Vec::new())
                        .as_slice(),
                ),
            ] {
                if !batches.is_empty() {
                    let mut tuple_counts = EnumMap::<BatchLocation, usize>::default();
                    for batch in batches {
                        tuple_counts[batch.location()] += batch.len();
                    }

                    let mut facts = Vec::with_capacity(3);
                    facts.push(format!("{} batches", batches.len()));
                    for (location, count) in tuple_counts {
                        if count > 0 {
                            facts.push(format!("{count} {} tuples", location.as_str()));
                        }
                    }
                    meta.extend([(
                        format!("slot {index} {class}").into(),
                        MetaItem::String(facts.join(", ")),
                    )]);
                }
            }
            if slot.n_merged > 0 {
                meta.extend([(
                    format!("slot {index} completed").into(),
                    MetaItem::String(format!(
                        "{} merges of {} batches over {} steps (avg {:.1} ms/step)",
                        slot.n_merged,
                        slot.n_merged_batches,
                        slot.n_steps,
                        (slot.elapsed / slot.n_steps as u32).as_secs_f64() * 1000.0
                    )),
                )]);
            }
        }

        // Extract all the batches from `slots`, annotating with whether they're
        // merging.
        let mut batches = Vec::new();
        for slot in slots {
            batches.extend(slot.loose_batches.into_iter().map(|b| (b, false)));
            if let Some(merging_batches) = slot.merging_batches {
                batches.extend(merging_batches.into_iter().map(|b| (b, true)));
            }
        }

        // Then summarize the batches.
        let n_batches = batches.len();
        let n_merging = batches.iter().filter(|(_batch, merging)| *merging).count();
        let mut cache_stats = spine_stats.cache_stats;
        let mut storage_size = 0;
        let mut merging_size = 0;
        for (batch, merging) in batches {
            cache_stats += batch.cache_stats();
            let on_storage = batch.location() == BatchLocation::Storage;
            if on_storage || merging {
                let size = batch.approximate_byte_size();
                if on_storage {
                    storage_size += size;
                }
                if merging {
                    merging_size += size;
                }
            }
        }

        meta.extend(metadata! {
            // Number of batches currently in the spine.
            "batches" => MetaItem::Count(n_batches),

            // The amount of data in the spine currently stored on disk (not
            // including any in-progress merges).
            "storage size" => MetaItem::bytes(storage_size),

            // The number of batches currently being merged (currently this
            // is always an even number because batches are merged in
            // pairs).
            "merging batches" => MetaItem::Count(n_merging),

            // The number of bytes of batches being merged.
            "merging size" => MetaItem::bytes(merging_size),

            // For merges already completed, the percentage of the updates input
            // to merges that merging eliminated, whether by weights adding to
            // zero or through key or value filters.
            "merge reduction" => spine_stats.merge_reduction(),

            // The amount of time waiting for backpressure.
            "merge backpressure wait" => MetaItem::Duration(spine_stats.backpressure_wait),
        });

        cache_stats.metadata(meta);
    }

    fn maybe_relieve_backpressure(
        no_backpressure: &Arc<Condvar>,
        state: &MutexGuard<SharedState<B>>,
    ) {
        if state.should_relieve_backpressure() {
            Self::relieve_backpressure(no_backpressure);
        }
    }

    fn relieve_backpressure(no_backpressure: &Arc<Condvar>) {
        no_backpressure.notify_all();
    }

    fn run(
        worker_state: &mut WorkerState,
        mergers: &mut [Option<Merge<B>>; MAX_LEVELS],
        merger_type: MergerType,
        state: &Arc<Mutex<SharedState<B>>>,
        idle: &Arc<Condvar>,
        no_backpressure: &Arc<Condvar>,
    ) -> WorkerStatus {
        // Run in-progress merges.
        let ((key_filter, value_filter), frontier) = {
            let shared = state.lock().unwrap();
            (shared.get_filters(), shared.frontier.clone())
        };

        for (level, m) in mergers.iter_mut().enumerate() {
            if let Some(merger) = m.as_mut() {
                // Run level-0 merges to completion.  For other levels, we
                // supply as much fuel as the average level-0 merge.  Along with
                // round-robinning between levels, this means that we invest
                // about the same amount of effort into merges at each level,
                // which should ensure that the higher-level merges complete in
                // time to keep batches from piling up.
                let fuel = if level == 0 {
                    isize::MAX
                } else {
                    worker_state.avg_slot0_merge_fuel()
                };
                merger.merge(&frontier, fuel);
                if merger.done {
                    if level == 0 {
                        worker_state.report_slot0_merge(merger.fuel);
                    }
                    let merger = m.take().unwrap();
                    let new_batch = Arc::new(merger.builder.done());
                    state.lock().unwrap().merge_complete(
                        level,
                        new_batch,
                        merger.elapsed,
                        merger.n_steps,
                    );
                }
            }
        }

        // Start new merges out of loose batches.
        //
        // Figuring out what merges to start requires the lock. Then we drop
        // the lock to actually start them, in case that's expensive (it
        // might require creating a file, for example).
        let start_merges = state
            .lock()
            .unwrap()
            .slots
            .iter_mut()
            .enumerate()
            .filter_map(|(level, slot)| slot.try_start_merge(level).map(|batches| (level, batches)))
            .collect::<Vec<_>>();
        for (level, batches) in start_merges {
            mergers[level] = Some(Merge::new(merger_type, batches, &key_filter, &value_filter));
        }

        let state = state.lock().unwrap();
        if state.request_exit {
            Self::relieve_backpressure(no_backpressure);
            WorkerStatus::Done
        } else if mergers.iter().all(|m| m.is_none()) {
            Self::relieve_backpressure(no_backpressure);
            idle.notify_all(); // XXX is there a race here?
            WorkerStatus::Idle
        } else {
            Self::maybe_relieve_backpressure(no_backpressure, &state);
            WorkerStatus::Busy
        }
    }
}

impl<B> Drop for AsyncMerger<B>
where
    B: Batch,
{
    fn drop(&mut self) {
        self.state.lock().unwrap().request_exit = true;
        BackgroundThread::wake();
    }
}

/// State shared among all of the workers in a background thread.
#[derive(Debug)]
struct WorkerState {
    /// Average amount of fuel used for slot-0 merges.
    avg_slot0_merge_fuel: isize,
}

impl Default for WorkerState {
    fn default() -> Self {
        Self {
            avg_slot0_merge_fuel: 10_000,
        }
    }
}

impl WorkerState {
    fn report_slot0_merge(&mut self, fuel: isize) {
        // Maintains an exponentially weighted moving average of the amount of
        // fuel used to merge batches in slot 0.
        self.avg_slot0_merge_fuel = ((127 * self.avg_slot0_merge_fuel + fuel + 64) / 128).max(1);
    }

    fn avg_slot0_merge_fuel(&self) -> isize {
        self.avg_slot0_merge_fuel
    }
}

/// Which merger to use.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MergerType {
    /// Newer merger, which should be faster for high-latency storage, such as
    /// object storage, but it likely needs tuning.
    PushMerger,

    /// The old standby, with known performance.
    #[default]
    ListMerger,
}

/// A single merge in progress in an [AsyncMerger].
struct Merge<B>
where
    B: Batch,
{
    /// Builder for merge output.
    builder: B::Builder,

    /// Total fuel consumed by this merge.
    fuel: isize,

    /// Done?
    done: bool,

    elapsed: Duration,

    n_steps: usize,

    /// The merger itself.
    inner: MergeInner<B>,
}

enum MergeInner<B>
where
    B: Batch,
{
    ListMerger(ArcListMerger<B>),
    PushMerger(ArcPushMerger<B>),
}

impl<B> Merge<B>
where
    B: Batch,
{
    fn new(
        merger_type: MergerType,
        batches: Vec<Arc<B>>,
        key_filter: &Option<Filter<B::Key>>,
        value_filter: &Option<Filter<B::Val>>,
    ) -> Self {
        let factories = batches[0].factories();
        let builder = B::Builder::for_merge(&factories, &batches, None);
        Self {
            builder,
            fuel: 0,
            elapsed: Duration::ZERO,
            n_steps: 0,
            done: false,
            inner: match merger_type {
                MergerType::ListMerger => MergeInner::ListMerger(ArcListMerger::new(
                    &factories,
                    batches,
                    key_filter,
                    value_filter,
                )),
                MergerType::PushMerger => {
                    let mut inner =
                        ArcPushMerger::new(&factories, batches, key_filter, value_filter);
                    inner.run();
                    MergeInner::PushMerger(inner)
                }
            },
        }
    }

    fn merge(&mut self, frontier: &B::Time, mut fuel: isize) -> isize {
        debug_assert!(fuel > 0);
        let supplied_fuel = fuel;
        let start = Instant::now();
        match &mut self.inner {
            MergeInner::ListMerger(merger) => {
                merger.work(&mut self.builder, frontier, &mut fuel);
                self.done = fuel > 0;
            }
            MergeInner::PushMerger(merger) => {
                self.done =
                    merger.merge(&mut self.builder, frontier, &mut fuel).is_ok() && fuel > 0;
                if !self.done {
                    merger.run();
                }
            }
        };
        self.elapsed += start.elapsed();
        self.n_steps += 1;
        let consumed_fuel = supplied_fuel - fuel;
        self.fuel += consumed_fuel;
        consumed_fuel
    }
}

/// Statistics about merges that a [Spine] has performed.
///
/// The difference between `post_len` and `pre_len` reflects updates that were
/// dropped because weights added to zero or because of key or value filters.
#[derive(Clone, Default)]
struct SpineStats {
    /// Number of updates before merging.
    pre_len: u64,
    /// Number of updates after merging.
    post_len: u64,
    /// Cache statistics, only for the batches that have already been merged and
    /// discarded.
    cache_stats: CacheStats,
    /// Time spent waiting for backpressure.
    backpressure_wait: Duration,
}

impl SpineStats {
    /// Adds `pre_len`, `post_len`, and `cache_stats` to the statistics.
    fn report_merge(&mut self, pre_len: usize, post_len: usize, cache_stats: CacheStats) {
        self.pre_len += pre_len as u64;
        self.post_len += post_len as u64;
        self.cache_stats += cache_stats;
    }

    /// Reports the percentage (in range `0..=100`) of updates that merging
    /// eliminated.
    fn merge_reduction(&self) -> MetaItem {
        MetaItem::Percent {
            numerator: self.pre_len - self.post_len,
            denominator: self.pre_len,
        }
    }
}

/// Persistence optimized [trace][crate::trace::Trace] implementation based on
/// collection and merging immutable batches of updates.
///
/// This spine works asynchronously.  The batches exposed to cursors are
/// maintained separately from the batches currently being merged by an
/// asynchronous thread. When one or more merges complete, the spine fetches the
/// new (smaller) collection of batches from the thread in the next step. (It
/// could fetch them earlier, but it might be unfriendly to expose potentially
/// one form of data to a given cursor and then a different form to the next one
/// within a single step.)
pub struct Spine<B>
where
    B: Batch,
{
    factories: B::Factories,

    dirty: bool,
    key_filter: Option<Filter<B::Key>>,
    value_filter: Option<Filter<B::Val>>,

    /// The asynchronous merger.
    merger: AsyncMerger<B>,
}

impl<B> SizeOf for Spine<B>
where
    B: Batch,
{
    fn size_of_children(&self, context: &mut Context) {
        self.merger.get_batches().size_of_with_context(context);
    }
}

impl<B> Display for Spine<B>
where
    B: Batch + Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for batch in self.merger.get_batches() {
            writeln!(f, "batch:\n{}", indent(&batch.to_string(), "    "))?
        }
        Ok(())
    }
}

impl<B> Debug for Spine<B>
where
    B: Batch,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        let mut cursor = self.cursor();
        writeln!(f, "spine:")?;
        while cursor.key_valid() {
            writeln!(f, "{:?}:", cursor.key())?;
            while cursor.val_valid() {
                writeln!(f, "    {:?}:", cursor.val())?;

                cursor.map_times(&mut |t, w| {
                    writeln!(f, "        {t:?} -> {w:?}").unwrap();
                });

                cursor.step_val();
            }
            cursor.step_key();
        }
        writeln!(f)?;
        Ok(())
    }
}

// TODO.
impl<B> Clone for Spine<B>
where
    B: Batch,
{
    fn clone(&self) -> Self {
        unimplemented!()
    }
}

impl<B> Archive for Spine<B>
where
    B: Batch,
{
    type Archived = ();
    type Resolver = ();

    unsafe fn resolve(&self, _pos: usize, _resolver: Self::Resolver, _out: *mut Self::Archived) {
        unimplemented!();
    }
}

impl<B: Batch, S: Serializer + ?Sized> Serialize<S> for Spine<B> {
    fn serialize(&self, _serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        unimplemented!();
    }
}

impl<B: Batch, D: Fallible> Deserialize<Spine<B>, D> for Archived<Spine<B>> {
    fn deserialize(&self, _deserializer: &mut D) -> Result<Spine<B>, D::Error> {
        unimplemented!();
    }
}

impl<B> NumEntries for Spine<B>
where
    B: Batch,
{
    const CONST_NUM_ENTRIES: Option<usize> = None;

    fn num_entries_shallow(&self) -> usize {
        self.merger
            .get_batches()
            .iter()
            .map(|batch| batch.len())
            .sum()
    }

    fn num_entries_deep(&self) -> usize {
        self.num_entries_shallow()
    }
}

impl<B> BatchReader for Spine<B>
where
    B: Batch,
{
    type Key = B::Key;
    type Val = B::Val;
    type Time = B::Time;
    type R = B::R;
    type Factories = B::Factories;

    type Cursor<'s> = SpineCursor<B>;

    fn factories(&self) -> Self::Factories {
        self.factories.clone()
    }

    fn key_count(&self) -> usize {
        self.merger
            .get_batches()
            .iter()
            .map(|batch| batch.key_count())
            .sum()
    }

    fn len(&self) -> usize {
        self.merger
            .get_batches()
            .iter()
            .map(|batch| batch.len())
            .sum()
    }

    fn approximate_byte_size(&self) -> usize {
        self.merger
            .get_batches()
            .iter()
            .map(|batch| batch.approximate_byte_size())
            .sum()
    }

    fn cursor(&self) -> Self::Cursor<'_> {
        SpineCursor::new_cursor(&self.factories, self.merger.get_batches())
    }

    fn sample_keys<RG>(&self, rng: &mut RG, sample_size: usize, sample: &mut DynVec<Self::Key>)
    where
        Self::Time: PartialEq<()>,
        RG: Rng,
    {
        let batches = self.merger.get_batches();
        let total_keys = batches.iter().map(|batch| batch.key_count()).sum::<usize>();

        if sample_size == 0 || total_keys == 0 {
            // Avoid division by zero.
            return;
        }

        // Sample each batch, picking the number of keys proportional to
        // batch size.
        let mut intermediate = self.factories.keys_factory().default_box();
        intermediate.reserve(sample_size);

        for batch in &batches {
            batch.sample_keys(
                rng,
                ((batch.key_count() as u128) * (sample_size as u128) / (total_keys as u128))
                    as usize,
                intermediate.as_mut(),
            );
        }

        // Drop duplicate keys and keys that appear with 0 weight, i.e.,
        // get canceled out across multiple batches.
        intermediate.deref_mut().sort_unstable();
        intermediate.dedup();

        let mut cursor = SpineCursor::new_cursor(&self.factories, batches);
        for key in intermediate.dyn_iter_mut() {
            cursor.seek_key(key);
            if let Some(current_key) = cursor.get_key() {
                if current_key == key {
                    debug_assert!(cursor.val_valid() && !cursor.weight().is_zero());
                    sample.push_ref(key);
                }
            }
        }
    }

    async fn fetch<KR>(
        &self,
        keys: &KR,
    ) -> Option<Box<dyn CursorFactory<Self::Key, Self::Val, Self::Time, Self::R>>>
    where
        KR: BatchReader<Key = Self::Key, Time = ()>,
    {
        Some(Box::new(
            FetchList::new(
                self.merger.get_batches(),
                keys,
                self.factories.weight_factory(),
            )
            .await,
        ))
    }
}

impl<B> Spine<B>
where
    B: Batch,
{
    /// Return the absolute path of the file for this Spine checkpoint.
    ///
    /// # Arguments
    /// - `cid`: The checkpoint id.
    /// - `persistent_id`: The persistent id that identifies the spine within
    ///   the circuit for a given checkpoint.
    fn checkpoint_file(base: &StoragePath, persistent_id: &str) -> StoragePath {
        base.child(format!("pspine-{}.dat", persistent_id))
    }

    /// Return the absolute path of the file for this Spine's batchlist.
    fn batchlist_file(&self, base: &StoragePath, persistent_id: &str) -> StoragePath {
        base.child(format!("pspine-batches-{}.dat", persistent_id))
    }
}

#[self_referencing]
pub struct SpineCursor<B: Batch> {
    batches: Vec<Arc<B>>,
    #[borrows(batches)]
    #[not_covariant]
    cursor: CursorList<B::Key, B::Val, B::Time, B::R, B::Cursor<'this>>,
}

impl<B: Batch> Clone for SpineCursor<B> {
    fn clone(&self) -> Self {
        let batches = self.borrow_batches().clone();
        let weight_factory = self.with_cursor(|cursor| cursor.weight_factory());
        SpineCursorBuilder {
            batches,
            cursor_builder: |batches| {
                CursorList::new(
                    weight_factory,
                    batches.iter().map(|batch| batch.cursor()).collect(),
                )
            },
        }
        .build()
    }
}

impl<B: Batch> SpineCursor<B> {
    fn new_cursor(factories: &B::Factories, batches: Vec<Arc<B>>) -> Self {
        SpineCursorBuilder {
            batches,
            cursor_builder: |batches| {
                CursorList::new(
                    factories.weight_factory(),
                    batches.iter().map(|batch| batch.cursor()).collect(),
                )
            },
        }
        .build()
    }
}

impl<B: Batch> Cursor<B::Key, B::Val, B::Time, B::R> for SpineCursor<B> {
    // fn key_vtable(&self) -> &'static VTable<B::Key> {
    //     self.cursor.key_vtable()
    // }

    // fn val_vtable(&self) -> &'static VTable<B::Val> {
    //     self.cursor.val_vtable()
    // }

    fn weight_factory(&self) -> &'static dyn Factory<B::R> {
        self.with_cursor(|cursor| cursor.weight_factory())
    }

    fn key_valid(&self) -> bool {
        self.with_cursor(|cursor| cursor.key_valid())
    }

    fn val_valid(&self) -> bool {
        self.with_cursor(|cursor| cursor.val_valid())
    }

    fn key(&self) -> &B::Key {
        self.with_cursor(|cursor| cursor.key())
    }

    fn val(&self) -> &B::Val {
        self.with_cursor(|cursor| cursor.val())
    }

    fn map_times(&mut self, logic: &mut dyn FnMut(&B::Time, &B::R)) {
        self.with_cursor_mut(|cursor| cursor.map_times(logic));
    }

    fn map_times_through(&mut self, upper: &B::Time, logic: &mut dyn FnMut(&B::Time, &B::R)) {
        self.with_cursor_mut(|cursor| cursor.map_times_through(upper, logic));
    }

    fn weight(&mut self) -> &B::R
    where
        B::Time: PartialEq<()>,
    {
        self.with_cursor_mut(|cursor| cursor.weight())
    }

    fn map_values(&mut self, logic: &mut dyn FnMut(&B::Val, &B::R))
    where
        B::Time: PartialEq<()>,
    {
        self.with_cursor_mut(|cursor| cursor.map_values(logic))
    }

    fn step_key(&mut self) {
        self.with_cursor_mut(|cursor| cursor.step_key());
    }

    fn step_key_reverse(&mut self) {
        self.with_cursor_mut(|cursor| cursor.step_key_reverse());
    }

    fn seek_key(&mut self, key: &B::Key) {
        self.with_cursor_mut(|cursor| cursor.seek_key(key));
    }

    fn seek_key_exact(&mut self, key: &B::Key) -> bool {
        self.with_cursor_mut(|cursor| cursor.seek_key_exact(key))
    }

    fn seek_key_with(&mut self, predicate: &dyn Fn(&B::Key) -> bool) {
        self.with_cursor_mut(|cursor| cursor.seek_key_with(predicate));
    }

    fn seek_key_with_reverse(&mut self, predicate: &dyn Fn(&B::Key) -> bool) {
        self.with_cursor_mut(|cursor| cursor.seek_key_with_reverse(predicate));
    }

    fn seek_key_reverse(&mut self, key: &B::Key) {
        self.with_cursor_mut(|cursor| cursor.seek_key_reverse(key));
    }

    fn step_val(&mut self) {
        self.with_cursor_mut(|cursor| cursor.step_val());
    }

    fn seek_val(&mut self, val: &B::Val) {
        self.with_cursor_mut(|cursor| cursor.seek_val(val));
    }

    fn seek_val_with(&mut self, predicate: &dyn Fn(&B::Val) -> bool) {
        self.with_cursor_mut(|cursor| cursor.seek_val_with(predicate));
    }

    fn rewind_keys(&mut self) {
        self.with_cursor_mut(|cursor| cursor.rewind_keys());
    }

    fn fast_forward_keys(&mut self) {
        self.with_cursor_mut(|cursor| cursor.fast_forward_keys());
    }

    fn rewind_vals(&mut self) {
        self.with_cursor_mut(|cursor| cursor.rewind_vals());
    }

    fn step_val_reverse(&mut self) {
        self.with_cursor_mut(|cursor| cursor.step_val_reverse());
    }

    fn seek_val_reverse(&mut self, val: &B::Val) {
        self.with_cursor_mut(|cursor| cursor.seek_val_reverse(val));
    }

    fn seek_val_with_reverse(&mut self, predicate: &dyn Fn(&B::Val) -> bool) {
        self.with_cursor_mut(|cursor| cursor.seek_val_with_reverse(predicate));
    }

    fn fast_forward_vals(&mut self) {
        self.with_cursor_mut(|cursor| cursor.fast_forward_vals());
    }
}

impl<B> Trace for Spine<B>
where
    B: Batch,
{
    type Batch = B;

    fn new(factories: &B::Factories) -> Self {
        Self::with_effort(factories, 1)
    }

    fn set_frontier(&mut self, frontier: &B::Time) {
        self.merger.set_frontier(frontier)
    }

    fn exert(&mut self, _effort: &mut isize) {}

    fn consolidate(self) -> Option<B> {
        let batches = self
            .merger
            .pause()
            .into_iter()
            .map(|batch| Arc::into_inner(batch).unwrap());
        let result = merge_batches(
            &self.factories,
            batches,
            &self.key_filter,
            &self.value_filter,
        );
        if result.is_empty() {
            None
        } else {
            Some(result)
        }
    }

    fn insert(&mut self, mut batch: Self::Batch) {
        if !batch.is_empty() {
            // If `batch` is in memory and it's got a fair number of records
            // (level 2 or higher), and we'll write it to storage on first
            // merge, then write it to storage right away.
            //
            // This addresses a problem with very large in-memory batches being
            // added to a spine and using too much memory.  This should not
            // happen in normal operation, because we do not feed such very
            // large batches into the circuit.  But intermediate joins, etc. can
            // sometimes produce them, and in that case we want to get them out
            // of memory as quickly as we can.
            //
            // This approach is a stopgap.  It is still a problem to generate
            // very large in-memory batches, because if they every exist at all
            // then they can OOM the process.  There are better ways to avoid
            // them that we should implement instead or in addition:
            //
            // - One of the sources of these large batches is the Batcher, which
            //   is currently in-memory.  It could fall back to an external sort
            //   if the result or the inputs are large.
            //
            // - We can allow an operator to split its output across many steps
            //   (see the huge step RFC).
            //
            // - We can have operators output mini-spines instead of batches.
            //
            // We already have the ability to write very large batches to
            // storage at build time, using `min_step_storage_bytes`.  This only
            // addresses individual large batches; it does not help with the
            // Batcher, which should be separately addressed.
            let batch = if batch.location() == BatchLocation::Memory
                && Spine::<B>::size_to_level(batch.len()) >= 2
                && pick_merge_destination([&batch], None) == BatchLocation::Storage
            {
                let factories = batch.factories();
                let builder =
                    B::Builder::for_merge(&factories, [&batch], Some(BatchLocation::Storage));
                let (key_filter, value_filter) = self.merger.state.lock().unwrap().get_filters();
                ListMerger::merge(
                    &factories,
                    builder,
                    vec![batch.consuming_cursor(key_filter, value_filter)],
                )
            } else {
                batch
            };

            self.dirty = true;
            self.merger.add_batch(Arc::new(batch));
        }
    }

    fn clear_dirty_flag(&mut self) {
        self.dirty = false;
    }

    fn dirty(&self) -> bool {
        self.dirty
    }

    fn retain_keys(&mut self, filter: Filter<Self::Key>) {
        self.merger.set_key_filter(&filter);
        self.key_filter = Some(filter);
    }

    fn retain_values(&mut self, filter: Filter<Self::Val>) {
        self.merger.set_value_filter(&filter);
        self.value_filter = Some(filter);
    }

    fn key_filter(&self) -> &Option<Filter<Self::Key>> {
        &self.key_filter
    }

    fn value_filter(&self) -> &Option<Filter<Self::Val>> {
        &self.value_filter
    }

    fn commit(&mut self, base: &StoragePath, persistent_id: &str) -> Result<(), Error> {
        fn persist_batches<B>(batches: Vec<Arc<B>>) -> Vec<Arc<B>>
        where
            B: Batch,
        {
            batches
                .into_iter()
                .map(|batch| {
                    if let Some(persisted) = batch.persisted() {
                        Arc::new(persisted)
                    } else {
                        batch
                    }
                })
                .collect::<Vec<_>>()
        }

        // Persist all the batches, and stick the not-merging batches back into
        // the merger.  (Putting the persisted batches into the merger means
        // that we don't have to persist them again for the next checkpoint,
        // saving time then. On the other hand, we do have to read them back
        // from disk to use them: no free lunch.)
        let (not_merging, merging) = self.merger.pause_new_merges();
        let not_merging = persist_batches(not_merging);
        self.merger.resume(not_merging.iter().cloned());
        let merging = persist_batches(merging);

        // Get the persistent IDs.
        let ids = not_merging
            .iter()
            .chain(merging.iter())
            .map(|batch| {
                batch
                    .checkpoint_path()
                    .expect("The batch should have been persisted")
                    .as_ref()
                    .to_string()
            })
            .collect::<Vec<_>>();

        let backend = Runtime::storage_backend().unwrap();
        let committed: CommittedSpine = (ids, self as &Self).into();
        let as_bytes = to_bytes(&committed).expect("Serializing CommittedSpine should work.");
        backend.write(&Self::checkpoint_file(base, persistent_id), as_bytes)?;

        // Write the batches as a separate file, this allows to parse it
        // in `Checkpointer` without the need to know the exact Spine type.
        let pspine_batches = PSpineBatches {
            files: committed.batches,
        };
        backend.write_json(&self.batchlist_file(base, persistent_id), &pspine_batches)?;

        Ok(())
    }

    fn restore(&mut self, base: &StoragePath, persistent_id: &str) -> Result<(), Error> {
        let pspine_path = Self::checkpoint_file(base, persistent_id);

        let content = Runtime::storage_backend().unwrap().read(&pspine_path)?;
        let archived = unsafe { rkyv::archived_root::<CommittedSpine>(&content) };

        let committed: CommittedSpine = archived
            .deserialize(&mut SharedDeserializeMap::new())
            .unwrap();
        self.dirty = committed.dirty;
        self.key_filter = None;
        self.value_filter = None;
        for batch in committed.batches {
            let batch = B::from_path(&self.factories.clone(), &batch.clone().into())
                .unwrap_or_else(|error| {
                    panic!("Failed to read batch {batch} for checkpoint ({error}).")
                });
            self.insert(batch);
        }

        Ok(())
    }

    fn metadata(&self, meta: &mut OperatorMeta) {
        self.merger.metadata(meta);
    }
}

impl<B> Spine<B>
where
    B: Batch,
{
    /// Given a batch size figure out which level it should reside in.
    fn size_to_level(len: usize) -> usize {
        debug_assert_eq!(MAX_LEVELS, 9);
        match len {
            0..=9999 => 0,
            10_000..=99_999 => 1,
            100_000..=999_999 => 2,
            1_000_000..=9_999_999 => 3,
            10_000_000..=99_999_999 => 4,
            100_000_000..=999_999_999 => 5,
            1_000_000_000..=9_999_999_999 => 6,
            10_000_000_000..=99_999_999_999 => 7,
            _ => 8, // For reference: 100 bln * 8 bytes ~= 750 GiB
        }
    }

    /// Allocates a fueled `Spine` with a specified effort multiplier.
    ///
    /// This trace will merge batches progressively, with each inserted batch
    /// applying a multiple of the batch's length in effort to each merge.
    /// The `effort` parameter is that multiplier. This value should be at
    /// least one for the merging to happen; a value of zero is not helpful.
    pub fn with_effort(factories: &B::Factories, _effort: usize) -> Self {
        Spine {
            factories: factories.clone(),
            dirty: false,
            key_filter: None,
            value_filter: None,
            merger: AsyncMerger::new(),
        }
    }

    pub fn complete_merges(&mut self) {
        let batches = self.merger.pause();
        let batch = merge_batches(
            &self.factories,
            batches.into_iter().map(|b| Arc::unwrap_or_clone(b)),
            &self.key_filter,
            &self.value_filter,
        );
        self.merger.resume(vec![Arc::new(batch)]);
    }

    /// Returns a read-only, non-merging snapshot of the current trace
    /// state.
    pub fn ro_snapshot(&self) -> SpineSnapshot<B> {
        self.into()
    }
}
