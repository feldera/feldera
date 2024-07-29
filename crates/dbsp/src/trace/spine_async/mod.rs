//! Implementation of a trace which merges batches in the background.

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
use crate::trace::spine_fueled::CommittedSpine;
use crate::trace::Merger;
use rand::Rng;
use rkyv::{ser::Serializer, Archive, Archived, Deserialize, Fallible, Serialize};
use size_of::SizeOf;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::{
    fmt::{self, Debug, Display, Formatter},
    fs,
    ops::DerefMut,
    sync::Condvar,
};
use textwrap::indent;
use uuid::Uuid;

use self::thread::{BackgroundThread, WorkerStatus};

use super::BatchLocation;

mod thread;

/// Maximum amount of levels in the spine.
pub(crate) const MAX_LEVELS: usize = 9;

impl<B: Batch + Send + Sync> From<&Spine<B>> for CommittedSpine<B> {
    fn from(value: &Spine<B>) -> Self {
        let mut batches = vec![];
        for b in &value.batches {
            if b.persistent_id().is_none() {
                eprintln!("Batch is missing a persistent id has len: {:?}", b.len());
            }
            batches.push(
                b.persistent_id()
                    .expect("Persistent spine needs an identifier")
                    .to_string_lossy()
                    .to_string(),
            );
        }

        // Transform the lower key bound into a serialized form and store it as a byte vector.
        // This is necessary because the key type is not sized.
        use crate::dynamic::rkyv::SerializeDyn;
        let lower_key_bound_ser = value.lower_key_bound.as_ref().map(|b| {
            let mut s: crate::trace::Serializer = crate::trace::Serializer::default();
            b.serialize(&mut s).unwrap();
            s.into_serializer().into_inner().to_vec()
        });

        CommittedSpine {
            batches,
            merged: Vec::new(),
            lower: value.lower.clone().into(),
            upper: value.upper.clone().into(),
            effort: 0,
            dirty: value.dirty,
            lower_key_bound: lower_key_bound_ser,
        }
    }
}

/// A group of batches with similar sizes (as determined by [size_from_level]).
struct Slot<B>
where
    B: Batch,
{
    /// Optionally, a pair of batches that are currently being merged.  These
    /// batches are not in `loose_batches`.
    merging_batches: Option<[Arc<B>; 2]>,

    /// Zero or more batches not currently being merged.
    loose_batches: Vec<Arc<B>>,
}

impl<B> Default for Slot<B>
where
    B: Batch,
{
    fn default() -> Self {
        Self {
            merging_batches: None,
            loose_batches: Vec::new(),
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
    fn try_start_merge(&mut self) -> Option<[Arc<B>; 2]> {
        if self.merging_batches.is_none() && self.loose_batches.len() >= 2 {
            let a = self.loose_batches.pop().unwrap();
            let b = self.loose_batches.pop().unwrap();
            self.merging_batches = Some([Arc::clone(&a), Arc::clone(&b)]);
            Some([a, b])
        } else {
            None
        }
    }
}

/// State shared between the merger thread and the main thread.
///
/// This shared state is accessed through a `Mutex`, which we try to hold for as
/// short a time as possible.
struct SharedState<B>
where
    B: Batch,
{
    key_filter: Option<Filter<B::Key>>,
    value_filter: Option<Filter<B::Val>>,
    slots: [Slot<B>; MAX_LEVELS],
    request_exit: bool,
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
            self.slots[Spine::<B>::size_to_level(batch.len())]
                .loose_batches
                .push(batch);
        }
    }

    /// Add `batches` as an (initially) loose batch, which will be merged when
    /// the merger thread has a chance (although it might not be awake).
    ///
    /// Returns a copy of all of the batches (whether loose or being merged).
    fn add_batch(&mut self, batch: Arc<B>) -> Vec<Arc<B>> {
        self.add_batches([batch]);
        self.get_batches()
    }

    fn get_filters(&self) -> (Option<Filter<B::Key>>, Option<Filter<B::Val>>) {
        (self.key_filter.clone(), self.value_filter.clone())
    }

    /// Gets a copy of all of the batches (whether loose or being merged).
    fn get_batches(&self) -> Vec<Arc<B>> {
        let mut batches = Vec::new();
        for slot in &self.slots {
            batches.extend(slot.loose_batches.iter().map(Arc::clone));
            if let Some(merging_batches) = &slot.merging_batches {
                batches.extend(merging_batches.iter().map(Arc::clone));
            }
        }
        batches
    }

    /// Removes the loose batches and returns them.  This ensures that the
    /// merger thread will not initiate any more merges.
    fn take_loose_batches(&mut self) -> Vec<Arc<B>> {
        let mut loose_batches = Vec::new();
        for slot in &mut self.slots {
            loose_batches.append(&mut slot.loose_batches);
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
        self.slots
            .iter()
            .all(|slot| slot.merging_batches.is_none() && slot.loose_batches.is_empty())
    }

    /// Finishes up the ongoing merge at the given `level`, which has completed
    /// with `new_batch` as the result.
    fn merge_complete(&mut self, level: usize, new_batch: Arc<B>) {
        let [a, b] = self.slots[level].merging_batches.take().unwrap();
        self.merge_stats
            .report_merge(a.len() + b.len(), new_batch.len());
        self.slots[Spine::<B>::size_to_level(new_batch.len())]
            .loose_batches
            .push(new_batch);
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
    /// Returns the new complete set of batches to include in the spine.
    fn add_batch(&self, batch: Arc<B>) -> Vec<Arc<B>> {
        let batches = self.state.lock().unwrap().add_batch(batch);
        BackgroundThread::wake();
        batches
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
    fn resume(&self, batches: impl IntoIterator<Item = Arc<B>>) {
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
            "batches" => n_batches,

            // The amount of data in the spine currently stored on disk (not
            // including any in-progress merges).
            "storage size" => MetaItem::bytes(storage_size),

            // The number of batches currently being merged (currently this
            // is always an even number because batches are merged in
            // pairs).
            "merging batches" => n_merging,

            // The number of bytes of batches being merged.
            "merging size" => MetaItem::bytes(merging_size),

            // For merges already completed, the percentage of the updates input
            // to merges that merging eliminated, whether by weights adding to
            // zero or through key or value filters.
            "merge reduction" => MetaItem::Percent(merge_stats.reduction_percent())
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
                if fuel >= 0 {
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
    fn reduction_percent(&self) -> f64 {
        if self.pre_len > self.post_len {
            let pre = self.pre_len as f64;
            let post = self.post_len as f64;
            (pre - post) / pre * 100.0
        } else {
            0.0
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
#[derive(SizeOf)]
pub struct Spine<B>
where
    B: Batch,
{
    #[size_of(skip)]
    factories: B::Factories,

    /// All the batches in the spine, in no particular order.
    batches: Vec<Arc<B>>,

    lower: Antichain<B::Time>,
    upper: Antichain<B::Time>,
    #[size_of(skip)]
    dirty: bool,
    #[size_of(skip)]
    lower_key_bound: Option<Box<B::Key>>,
    #[size_of(skip)]
    key_filter: Option<Filter<B::Key>>,
    #[size_of(skip)]
    value_filter: Option<Filter<B::Val>>,

    /// The asynchronous merger.
    #[size_of(skip)]
    merger: AsyncMerger<B>,
}

impl<B> Display for Spine<B>
where
    B: Batch + Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for batch in &self.batches {
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
        self.batches.iter().map(|batch| batch.len()).sum()
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

    type Cursor<'s> = SpineCursor<'s, B>;
    // type Consumer = SpineConsumer<B>;

    fn factories(&self) -> Self::Factories {
        self.factories.clone()
    }

    fn key_count(&self) -> usize {
        self.batches.iter().map(|batch| batch.key_count()).sum()
    }

    fn len(&self) -> usize {
        self.batches.iter().map(|batch| batch.len()).sum()
    }

    fn approximate_byte_size(&self) -> usize {
        self.batches
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
        SpineCursor::new(
            &self.factories,
            self.batches
                .iter()
                .filter(|batch| !batch.is_empty())
                .map(|batch| batch.cursor())
                .collect(),
        )
    }

    fn truncate_keys_below(&mut self, lower_bound: &Self::Key) {
        self.pause();

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

        self.batches = self
            .batches
            .drain(..)
            .map(|batch| {
                let mut batch = Arc::into_inner(batch).unwrap();
                batch.truncate_keys_below(&bound);
                Arc::new(batch)
            })
            .collect();

        self.resume();
    }

    fn sample_keys<RG>(&self, rng: &mut RG, sample_size: usize, sample: &mut DynVec<Self::Key>)
    where
        Self::Time: PartialEq<()>,
        RG: Rng,
    {
        let total_keys = self.key_count();

        if sample_size == 0 || total_keys == 0 {
            // Avoid division by zero.
            return;
        }

        // Sample each batch, picking the number of keys proportional to
        // batch size.
        let mut intermediate = self.factories.keys_factory().default_box();
        intermediate.reserve(sample_size);

        for batch in self.batches.iter() {
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

        let mut cursor = self.cursor();
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

pub struct SpineCursor<'s, B: Batch + 's> {
    #[allow(clippy::type_complexity)]
    cursor: CursorList<B::Key, B::Val, B::Time, B::R, B::Cursor<'s>>,
}

impl<'s, B: Batch + 's> Clone for SpineCursor<'s, B> {
    fn clone(&self) -> Self {
        Self {
            cursor: self.cursor.clone(),
        }
    }
}

impl<'s, B: Batch> SpineCursor<'s, B> {
    fn new(factories: &B::Factories, cursors: Vec<B::Cursor<'s>>) -> Self {
        Self {
            cursor: CursorList::new(factories.weight_factory(), cursors),
        }
    }
}

impl<'s, B: Batch> Cursor<B::Key, B::Val, B::Time, B::R> for SpineCursor<'s, B> {
    // fn key_vtable(&self) -> &'static VTable<B::Key> {
    //     self.cursor.key_vtable()
    // }

    // fn val_vtable(&self) -> &'static VTable<B::Val> {
    //     self.cursor.val_vtable()
    // }

    fn weight_factory(&self) -> &'static dyn Factory<B::R> {
        self.cursor.weight_factory()
    }

    fn key_valid(&self) -> bool {
        self.cursor.key_valid()
    }

    fn val_valid(&self) -> bool {
        self.cursor.val_valid()
    }

    fn key(&self) -> &B::Key {
        self.cursor.key()
    }

    fn val(&self) -> &B::Val {
        self.cursor.val()
    }

    fn map_times(&mut self, logic: &mut dyn FnMut(&B::Time, &B::R)) {
        self.cursor.map_times(logic);
    }

    fn map_times_through(&mut self, upper: &B::Time, logic: &mut dyn FnMut(&B::Time, &B::R)) {
        self.cursor.map_times_through(upper, logic);
    }

    fn weight(&mut self) -> &B::R
    where
        B::Time: PartialEq<()>,
    {
        self.cursor.weight()
    }

    fn map_values(&mut self, logic: &mut dyn FnMut(&B::Val, &B::R))
    where
        B::Time: PartialEq<()>,
    {
        self.cursor.map_values(logic)
    }

    fn step_key(&mut self) {
        self.cursor.step_key();
    }

    fn step_key_reverse(&mut self) {
        self.cursor.step_key_reverse();
    }

    fn seek_key(&mut self, key: &B::Key) {
        self.cursor.seek_key(key);
    }

    fn seek_key_with(&mut self, predicate: &dyn Fn(&B::Key) -> bool) {
        self.cursor.seek_key_with(predicate);
    }

    fn seek_key_with_reverse(&mut self, predicate: &dyn Fn(&B::Key) -> bool) {
        self.cursor.seek_key_with_reverse(predicate);
    }

    fn seek_key_reverse(&mut self, key: &B::Key) {
        self.cursor.seek_key_reverse(key);
    }

    fn step_val(&mut self) {
        self.cursor.step_val();
    }

    fn seek_val(&mut self, val: &B::Val) {
        self.cursor.seek_val(val);
    }

    fn seek_val_with(&mut self, predicate: &dyn Fn(&B::Val) -> bool) {
        self.cursor.seek_val_with(predicate);
    }

    fn rewind_keys(&mut self) {
        self.cursor.rewind_keys();
    }

    fn fast_forward_keys(&mut self) {
        self.cursor.fast_forward_keys();
    }

    fn rewind_vals(&mut self) {
        self.cursor.rewind_vals();
    }

    fn step_val_reverse(&mut self) {
        self.cursor.step_val_reverse();
    }

    fn seek_val_reverse(&mut self, val: &B::Val) {
        self.cursor.seek_val_reverse(val);
    }

    fn seek_val_with_reverse(&mut self, predicate: &dyn Fn(&B::Val) -> bool) {
        self.cursor.seek_val_with_reverse(predicate);
    }

    fn fast_forward_vals(&mut self) {
        self.cursor.fast_forward_vals();
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
        // Complete all in-progress merges, as we don't have an easy way to update
        // timestamps in an ongoing merge.
        self.pause();

        self.batches = self
            .batches
            .drain(..)
            .map(|batch| {
                let mut batch = Arc::into_inner(batch).unwrap();
                batch.recede_to(frontier);
                Arc::new(batch)
            })
            .collect();

        self.resume();
    }

    fn exert(&mut self, _effort: &mut isize) {}

    fn consolidate(mut self) -> Option<B> {
        self.pause();
        let result = merge_batches(
            &self.factories,
            self.batches
                .drain(..)
                .map(|batch| Arc::into_inner(batch).unwrap()),
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

        if batch.is_empty() {
            // Refresh the set of batches from the merger, in case some merges
            // completed.
            self.batches = self.merger.get_batches();
        } else {
            // XXX This always inserts the new batch asynchronously. This is
            // usually what we want to do, but it means that in theory we could
            // continue building up batches until we run out of memory (or disk
            // space), if merging is slow. Thus, we should figure out at what
            // point it makes sense to wait until the backlog is reduced. We
            // wouldn't have to just block; instead, we could grab some of the
            // loose batches and do some merging ourselves while we wait,
            // thereby actually speeding up the merges by devoting two threads
            // instead of just one.
            self.dirty = true;
            self.lower = self.lower.as_ref().meet(batch.lower());
            self.upper = self.upper.as_ref().join(batch.upper());
            self.batches = self.merger.add_batch(Arc::new(batch));
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
        for batch in self.batches.iter_mut() {
            if let Some(persisted) = batch.persisted() {
                *batch = Arc::new(persisted);
            }
        }

        let committed: CommittedSpine<B> = (self as &Self).into();
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
                .expect("Batch file for checkpoint must exist.");
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
            batches: Vec::new(),
            dirty: false,
            lower_key_bound: None,
            key_filter: None,
            value_filter: None,
            merger: AsyncMerger::new(),
        }
    }

    pub fn complete_merges(&mut self) {
        self.pause();
        self.resume();
    }

    fn pause(&mut self) {
        self.batches = self.merger.pause();
    }

    fn resume(&mut self) {
        self.merger.resume(self.batches.iter().map(Arc::clone));
    }
}
