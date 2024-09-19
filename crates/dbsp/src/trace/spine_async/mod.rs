//! Implementation of a [`Trace`] which merges batches in the background.
//!
//! This is a "spine", a [`Trace`] that internally consists of a vector of
//! batches. Inserting a new batch appends to the vector, and iterating or
//! searching a spine iterates or searches all of the batches in the vector.
//! The cost of these operations grows with the number of batches in the vector,
//! so it is beneficial to reduce the number by merging batches.

use crate::{
    circuit::metadata::{MetaItem, OperatorMeta},
    dynamic::{DynVec, Factory, Weight},
    time::{Antichain, AntichainRef, Timestamp},
    trace::{
        cursor::CursorList, merge_batches, Batch, BatchReader, BatchReaderFactories, Cursor,
        Filter, Trace,
    },
    Error, NumEntries,
};

use crate::dynamic::{ClonableTrait, DeserializableDyn};
use crate::storage::file::to_bytes;
use crate::storage::{checkpoint_path, write_commit_metadata};
use crate::trace::spine_async::snapshot::SpineSnapshot;
use crate::trace::spine_fueled::CommittedSpine;
use crate::trace::Merger;
use ouroboros::self_referencing;
use rand::Rng;
use rkyv::{ser::Serializer, Archive, Archived, Deserialize, Fallible, Serialize};
use size_of::{Context, SizeOf};
use std::sync::Arc;
use std::sync::Mutex;
use std::{
    collections::VecDeque,
    path::{Path, PathBuf},
};
use std::{
    fmt::{self, Debug, Display, Formatter},
    fs,
    ops::DerefMut,
    sync::Condvar,
};
use textwrap::indent;
use uuid::Uuid;

mod snapshot;
use self::thread::{BackgroundThread, WorkerStatus};

use super::BatchLocation;

mod thread;

/// Maximum amount of levels in the spine.
pub(crate) const MAX_LEVELS: usize = 9;

impl<B: Batch + Send + Sync> From<(Vec<String>, &Spine<B>)> for CommittedSpine<B> {
    fn from((batches, spine): (Vec<String>, &Spine<B>)) -> Self {
        // Transform the lower key bound into a serialized form and store it as a byte vector.
        // This is necessary because the key type is not sized.
        use crate::dynamic::rkyv::SerializeDyn;
        let lower_key_bound_ser = spine.lower_key_bound.as_ref().map(|b| {
            let mut s: crate::trace::Serializer = crate::trace::Serializer::default();
            b.serialize(&mut s).unwrap();
            s.into_serializer().into_inner().to_vec()
        });

        CommittedSpine {
            batches,
            merged: Vec::new(),
            lower: spine.lower.clone().into(),
            upper: spine.upper.clone().into(),
            effort: 0,
            dirty: spine.dirty,
            lower_key_bound: lower_key_bound_ser,
        }
    }
}

/// A group of batches with similar sizes (as determined by [size_from_level]).
#[derive(SizeOf)]
struct Slot<B>
where
    B: Batch,
{
    /// Optionally, a pair of batches that are currently being merged.  These
    /// batches are not in `loose_batches`.
    ///
    /// Invariant: the batches (if present) must be non-empty.
    merging_batches: Option<[Arc<B>; 2]>,

    /// Zero or more batches not currently being merged.
    ///
    /// Invariant: the batches must be non-empty.
    loose_batches: VecDeque<Arc<B>>,
}

impl<B> Default for Slot<B>
where
    B: Batch,
{
    fn default() -> Self {
        Self {
            merging_batches: None,
            loose_batches: VecDeque::new(),
        }
    }
}

impl<B> Slot<B>
where
    B: Batch,
{
    /// If this slot doesn't currently have an ongoing merge, and it does have
    /// at least two loose batches, picks two of the loose batches and makes
    /// them into merging batches, and returns those batches. Otherwise, returns
    /// `None` without changing anything.
    ///
    /// We merge the least recently added batch (ensuring that batches
    /// eventually get merged) with the one closest in size (ensuring that the
    /// merge is as efficient as it can be).
    fn try_start_merge(&mut self) -> Option<[Arc<B>; 2]> {
        if self.merging_batches.is_none() && self.loose_batches.len() >= 2 {
            let a = self.loose_batches.pop_front().unwrap();
            let a_len = a.len();
            let mut best_idx = 0;
            let mut best_distance = self.loose_batches[0].len().abs_diff(a_len);
            for idx in 1..self.loose_batches.len() {
                let distance = self.loose_batches[idx].len().abs_diff(a_len);
                if distance < best_distance {
                    best_idx = idx;
                    best_distance = distance;
                }
            }
            let b = self.loose_batches.remove(best_idx).unwrap();
            self.merging_batches = Some([Arc::clone(&a), Arc::clone(&b)]);
            Some([a, b])
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
    slots: [Slot<B>; MAX_LEVELS],
    #[size_of(skip)]
    request_exit: bool,
    #[size_of(skip)]
    merge_stats: MergeStats,
}

impl<B> SharedState<B>
where
    B: Batch,
{
    pub fn new() -> Self {
        Self {
            key_filter: None,
            value_filter: None,
            slots: std::array::from_fn(|_| Slot::default()),
            request_exit: false,
            merge_stats: MergeStats::default(),
        }
    }

    /// Adds all of `batches` as (initially) loose batches.  They will be merged
    /// when the merger thread has a chance (although it might not be awake).
    fn add_batches(&mut self, batches: impl IntoIterator<Item = Arc<B>>) {
        for batch in batches {
            if !batch.is_empty() {
                self.slots[Spine::<B>::size_to_level(batch.len())]
                    .loose_batches
                    .push_back(batch);
            }
        }
    }

    /// Add `batches` as an (initially) loose batch, which will be merged when
    /// the merger thread has a chance (although it might not be awake).
    fn add_batch(&mut self, batch: Arc<B>) {
        debug_assert!(!batch.is_empty());
        let level = Spine::<B>::size_to_level(batch.len());
        self.slots[level].loose_batches.push_back(batch);
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

    /// Returns true if the merger is empty: it is not doing any merging work
    /// and there are no loose batches.
    fn is_empty(&self) -> bool {
        self.slots.iter().all(|slot| slot.n_batches() == 0)
    }

    /// Finishes up the ongoing merge at the given `level`, which has completed
    /// with `new_batch` as the result.
    fn merge_complete(&mut self, level: usize, new_batch: Arc<B>) {
        let [a, b] = self.slots[level].merging_batches.take().unwrap();
        self.merge_stats
            .report_merge(a.len() + b.len(), new_batch.len());
        self.add_batches([new_batch]);
    }

    /// Returns information that the caller can use to construct a metadata
    /// report. The vector consists of each of the batches and a bool that
    /// indicates whether it is now being merged.
    ///
    /// This is better than getting the full metadata here because part of that
    /// is measuring the size of the batches, which can require I/O.
    fn metadata_snapshot(&self) -> (Vec<(Arc<B>, bool)>, MergeStats) {
        let mut batches = Vec::new();
        for slot in &self.slots {
            batches.extend(slot.loose_batches.iter().map(|b| (Arc::clone(b), false)));
            if let Some(merging_batches) = &slot.merging_batches {
                batches.extend(merging_batches.iter().map(|b| (Arc::clone(b), true)));
            }
        }
        (batches, self.merge_stats.clone())
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

    /// Allows us to wait for the background worker to become idle.
    idle: Arc<Condvar>,
}

impl<B> AsyncMerger<B>
where
    B: Batch,
{
    fn new() -> Self {
        let idle = Arc::new(Condvar::new());
        let state = Arc::new(Mutex::new(SharedState::new()));
        BackgroundThread::add_worker({
            let state = Arc::clone(&state);
            let idle = Arc::clone(&idle);
            let mut mergers = std::array::from_fn(|_| None);
            Box::new(move || Self::run(&mut mergers, &state, &idle))
        });
        Self { state, idle }
    }
    fn set_key_filter(&self, key_filter: &Filter<B::Key>) {
        self.state.lock().unwrap().key_filter = Some(key_filter.clone());
    }
    fn set_value_filter(&self, value_filter: &Filter<B::Val>) {
        self.state.lock().unwrap().value_filter = Some(value_filter.clone());
    }

    /// Adds `batch` to the shared merging state and wakes up the merger.
    fn add_batch(&self, batch: Arc<B>) {
        debug_assert!(!batch.is_empty());
        self.state.lock().unwrap().add_batch(batch);
        BackgroundThread::wake();
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

    /// Starts merging again with `batches`, which are presumably what
    /// [Self::pause] returned.
    fn resume(&self, batches: Vec<Arc<B>>) {
        debug_assert!(self.is_empty());
        self.add_batches(batches);
    }

    /// Returns true if the merger is empty: it is not doing any merging work
    /// and there are no loose batches.
    fn is_empty(&self) -> bool {
        self.state.lock().unwrap().is_empty()
    }

    fn metadata(&self, meta: &mut OperatorMeta) {
        let (batches, merge_stats) = self.state.lock().unwrap().metadata_snapshot();

        let n_batches = batches.len();
        let n_merging = batches.iter().filter(|(_batch, merging)| *merging).count();
        let mut storage_size = 0;
        let mut merging_size = 0;
        for (batch, merging) in batches {
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
            "merge reduction" => merge_stats.merge_reduction()
        });
    }
    fn run(
        mergers: &mut [Option<(B::Merger, [Arc<B>; 2])>; MAX_LEVELS],
        state: &Arc<Mutex<SharedState<B>>>,
        idle: &Arc<Condvar>,
    ) -> WorkerStatus {
        // Run in-progress merges.
        let (key_filter, value_filter) = state.lock().unwrap().get_filters();
        for (level, m) in mergers.iter_mut().enumerate() {
            if let Some((merger, [a, b])) = m.as_mut() {
                let mut fuel = 10_000;
                merger.work(a, b, &key_filter, &value_filter, &mut fuel);
                if fuel > 0 {
                    let (merger, _batches) = m.take().unwrap();
                    let new_batch = Arc::new(merger.done());
                    state.lock().unwrap().merge_complete(level, new_batch);
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
            .filter_map(|(level, slot)| slot.try_start_merge().map(|batches| (level, batches)))
            .collect::<Vec<_>>();
        for (level, [a, b]) in start_merges {
            let merger = B::Merger::new_merger(&a, &b, None);
            mergers[level] = Some((merger, [a, b]));
        }

        if state.lock().unwrap().request_exit {
            WorkerStatus::Done
        } else if mergers.iter().all(|m| m.is_none()) {
            idle.notify_all(); // XXX is there a race here?
            WorkerStatus::Idle
        } else {
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

/// Statistics about merges that a [Spine] has performed.
///
/// The difference between `post_len` and `pre_len` reflects updates that were
/// dropped because weights added to zero or because of key or value filters.
#[derive(Clone, Default)]
struct MergeStats {
    /// Number of updates before merging.
    pre_len: u64,
    /// Number of updates after merging.
    post_len: u64,
}

impl MergeStats {
    /// Adds `pre_len` and `post_len` to the statistics.
    fn report_merge(&mut self, pre_len: usize, post_len: usize) {
        self.pre_len += pre_len as u64;
        self.post_len += post_len as u64;
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

    lower: Antichain<B::Time>,
    upper: Antichain<B::Time>,
    dirty: bool,
    lower_key_bound: Option<Box<B::Key>>,
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
        self.lower.size_of_children(context);
        self.upper.size_of_children(context);
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
    // type Consumer = SpineConsumer<B>;

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

    fn lower(&self) -> AntichainRef<'_, Self::Time> {
        self.lower.as_ref()
    }

    fn upper(&self) -> AntichainRef<'_, Self::Time> {
        self.upper.as_ref()
    }

    fn cursor(&self) -> Self::Cursor<'_> {
        SpineCursor::new_cursor(&self.factories, self.merger.get_batches())
    }

    fn truncate_keys_below(&mut self, lower_bound: &Self::Key) {
        if let Some(bound) = &mut self.lower_key_bound {
            if bound.as_ref() < lower_bound {
                lower_bound.clone_to(&mut *bound);
            }
        } else {
            let mut bound = self.key_factory().default_box();
            lower_bound.clone_to(&mut *bound);
            self.lower_key_bound = Some(bound);
        };
        let mut bound = self.factories.key_factory().default_box();
        self.lower_key_bound.as_ref().unwrap().clone_to(&mut *bound);

        let batches = self
            .merger
            .pause()
            .into_iter()
            .map(|batch| {
                let mut batch = Arc::into_inner(batch).unwrap();
                batch.truncate_keys_below(&bound);
                Arc::new(batch)
            })
            .collect();

        self.merger.resume(batches);
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
                    while cursor.val_valid() {
                        let weight = cursor.weight();
                        if !weight.is_zero() {
                            sample.push_ref(key);
                            break;
                        }
                        cursor.step_val();
                    }
                }
            }
        }
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
    fn checkpoint_file<P: AsRef<str>>(cid: Uuid, persistent_id: P) -> PathBuf {
        let mut path = checkpoint_path(cid);
        path.push(format!("pspine-{}.dat", persistent_id.as_ref()));
        path
    }

    /// Return the absolute path of the file for this Spine's batchlist.
    ///
    /// # Arguments
    /// - `sid`: The step id of the checkpoint.
    fn batchlist_file<P: AsRef<str>>(&self, cid: Uuid, persistent_id: P) -> PathBuf {
        let mut path = checkpoint_path(cid);
        path.push(format!("pspine-batches-{}.dat", persistent_id.as_ref()));
        path
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

    fn recede_to(&mut self, frontier: &B::Time) {
        let batches = self
            .merger
            .pause()
            .into_iter()
            .map(|batch| {
                let mut batch = Arc::into_inner(batch).unwrap();
                batch.recede_to(frontier);
                Arc::new(batch)
            })
            .collect();
        self.merger.resume(batches);
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
        assert!(batch.lower() != batch.upper());
        if let Some(bound) = &self.lower_key_bound {
            batch.truncate_keys_below(bound);
        }

        if !batch.is_empty() {
            self.dirty = true;
            self.lower = self.lower.as_ref().meet(batch.lower());
            self.upper = self.upper.as_ref().join(batch.upper());
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

    fn commit<P: AsRef<str>>(&mut self, cid: Uuid, persistent_id: P) -> Result<(), Error> {
        // Persist all the batches.
        let batches = self
            .merger
            .pause()
            .into_iter()
            .map(|batch| {
                if let Some(persisted) = batch.persisted() {
                    Arc::new(persisted)
                } else {
                    batch
                }
            })
            .collect::<Vec<_>>();
        let ids = batches
            .iter()
            .map(|batch| {
                batch
                    .persistent_id()
                    .expect("The batch should have been persisted")
                    .to_string_lossy()
                    .to_string()
            })
            .collect::<Vec<_>>();
        self.merger.resume(batches);

        let committed: CommittedSpine<B> = (ids, self as &Self).into();
        let as_bytes = to_bytes(&committed).expect("Serializing CommittedSpine should work.");
        write_commit_metadata(
            Self::checkpoint_file(cid, &persistent_id),
            as_bytes.as_slice(),
        )?;

        // Write the batches as a separate file, this allows to parse it
        // in `Checkpointer` without the need to know the exact Spine type.
        let batches = committed.batches;
        let as_bytes = to_bytes(&batches).expect("Serializing batches to Vec<String> should work.");
        write_commit_metadata(
            self.batchlist_file(cid, &persistent_id),
            as_bytes.as_slice(),
        )?;

        Ok(())
    }

    fn restore<P: AsRef<str>>(&mut self, cid: Uuid, persistent_id: P) -> Result<(), Error> {
        let pspine_path = Self::checkpoint_file(cid, persistent_id);
        let content = fs::read(pspine_path)?;
        let archived = unsafe { rkyv::archived_root::<CommittedSpine<B>>(&content) };

        let committed: CommittedSpine<B> = archived.deserialize(&mut rkyv::Infallible).unwrap();
        self.lower = Antichain::from(committed.lower);
        self.upper = Antichain::from(committed.upper);
        self.dirty = committed.dirty;
        if let Some(bytes) = committed.lower_key_bound {
            let mut default_box = self.factories.key_factory().default_box();
            unsafe { default_box.deserialize_from_bytes(&bytes, 0) };
            self.lower_key_bound = Some(default_box);
        }
        self.key_filter = None;
        self.value_filter = None;
        for batch in committed.batches {
            let batch = B::from_path(&self.factories.clone(), Path::new(batch.as_str()))
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

    #[inline]
    fn key_factory(&self) -> &'static dyn Factory<B::Key> {
        self.factories.key_factory()
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
            lower: Antichain::from_elem(B::Time::minimum()),
            upper: Antichain::new(),
            dirty: false,
            lower_key_bound: None,
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
