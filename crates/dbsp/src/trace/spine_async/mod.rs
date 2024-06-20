//! Implementation of a trace which merges batches in the background.

use crate::{
    dynamic::{DynVec, Factory, Weight},
    time::{Antichain, AntichainRef, Timestamp},
    trace::{cursor::CursorList, Batch, BatchReader, BatchReaderFactories, Cursor, Filter, Trace},
    Error, NumEntries, Runtime,
};

use crate::dynamic::{ClonableTrait, DeserializableDyn};
use crate::storage::backend::metrics::{
    COMPACTION_DURATION, COMPACTION_SIZE, COMPACTION_SIZE_SAVINGS, COMPACTION_STALL_TIME,
    TOTAL_COMPACTIONS,
};
use crate::storage::backend::StorageError;
use crate::storage::file::to_bytes;
use crate::storage::{checkpoint_path, write_commit_metadata};
use crate::trace::spine_async::merger::{BackgroundOperation, MergeResult};
use crate::trace::spine_fueled::CommittedSpine;
use crate::trace::Merger;
use crossbeam::channel::{unbounded, Receiver, Sender, TryRecvError};
use metrics::{counter, histogram};
use rand::Rng;
use rkyv::{ser::Serializer, Archive, Archived, Deserialize, Fallible, Serialize};
use size_of::SizeOf;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use std::{
    fmt::{self, Debug, Display, Formatter, Write},
    fs,
    ops::DerefMut,
};
use textwrap::indent;
use uuid::Uuid;

pub(crate) mod merger;

#[cfg(test)]
mod tests;

/// Maximum amount of levels in the spine.
pub(crate) const MAX_LEVELS: usize = 9;

impl<B: Batch + Send + Sync> From<&Spine<B>> for CommittedSpine<B> {
    fn from(value: &Spine<B>) -> Self {
        let mut batches = vec![];
        value.map_batches(|b| {
            if b.persistent_id().is_none() {
                eprintln!("Batch is missing a persistent id has len: {:?}", b.len());
            }
            batches.push(
                b.persistent_id()
                    .expect("Persistent spine needs an identifier")
                    .to_string_lossy()
                    .to_string(),
            );
        });

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

/// The type of value that the given key stores.
///
/// - `Merging` implies `BatchState::Merging(a,b)` as a value -- this also means a merge of these two
///    has been initiated.
/// - `Single` implies `BatchState::Single(a)` as a value -- this batch isn't currently being
///    merged with another batch.
///
/// This ensures a sort order in the corresponding level where `Single` entries
/// come first when iterating over the tree, and the things we are merged are pushed
/// to the back of the tree.
#[derive(Debug, Ord, PartialOrd, Copy, Clone, Eq, PartialEq)]
enum BatchKind {
    Single,
    Merging,
}

/// A unique identifier for batches we hold within levels.
#[derive(SizeOf, Ord, PartialOrd, Debug, Copy, Clone, Eq, PartialEq)]
struct BatchIdent {
    /// What type of value we're dealing with (e.g., two batches
    /// being currently merged or a single batch).
    #[size_of(skip)]
    kind: BatchKind,
    /// Which level this batch is from.
    ///
    /// We use this to find the two batches we want to remove when
    /// we finished a merge.
    level: usize,
    /// A key that is unique within the level.
    ///
    /// We use this to find the two batches we want to remove when
    /// we finished a merge.
    #[size_of(skip)]
    key: u64,
}

impl BatchIdent {
    fn single(level: usize, key: u64) -> Self {
        BatchIdent {
            kind: BatchKind::Single,
            level,
            key,
        }
    }

    fn merging(level: usize, key: u64) -> Self {
        BatchIdent {
            kind: BatchKind::Merging,
            level,
            key,
        }
    }
}

/// Describes the state of a layer.
///
/// A layer can be empty, contain a single batch, or contain a pair of batches
/// that are in the process of merging into a new batch.
#[derive(SizeOf)]
pub enum BatchState<B>
where
    B: Batch,
{
    /// An entry containing a single batch.
    Single(B),
    /// An entry containing two batches which are in the process of merging.
    Merging(Vec<Arc<B>>),
}

impl<B> BatchState<B>
where
    B: Batch,
{
    /// The number of actual updates contained in the level.
    fn len(&self) -> usize {
        match self {
            BatchState::Single(b) => b.len(),
            BatchState::Merging(bs) => bs.iter().map(|b| b.len()).sum(),
        }
    }

    fn batch_count(&self) -> usize {
        match self {
            BatchState::Single(_) => 1,
            BatchState::Merging(bs) => bs.len(),
        }
    }

    fn is_merging(&self) -> bool {
        matches!(self, BatchState::Merging(_))
    }
}

struct CompactStrategy {
    /// Minimum number of batches that need to belong to the same size
    /// bucket before compaction is triggered on that bucket.
    min_threshold: usize,
    /// Maximum number of batches that will be
    /// compacted together in one compaction step.
    max_threshold: usize,
}

impl Default for CompactStrategy {
    fn default() -> Self {
        CompactStrategy {
            min_threshold: 2,
            max_threshold: 64,
        }
    }
}

/// Persistence optimized [trace][crate::trace::Trace] implementation based on
/// collection and merging immutable batches of updates.
#[derive(SizeOf)]
pub struct Spine<B>
where
    B: Batch,
{
    #[size_of(skip)]
    factories: B::Factories,
    /// `levels` holds `MAX_LEVEL` number of BTrees which contain batches.
    levels: Vec<BTreeMap<BatchIdent, BatchState<B>>>,
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
    /// The channel where we send merge requests to the compactor thread.
    #[size_of(skip)]
    merger_tx: Arc<Sender<BackgroundOperation>>,
    /// The closure of the compactor thread uses this channel to send completed
    /// merges back to us.
    #[size_of(skip)]
    completion_tx: Arc<Sender<MergeResult<B>>>,
    /// The endpoint where we receive completed merges sent to us.
    #[size_of(skip)]
    completion_rx: Receiver<MergeResult<B>>,
    next_batch_key: u64,
    /// How many batch merges are outstanding.
    ///
    /// This is a shortcut of summing everything in `levels` that's `BatchState::Merging`.
    #[size_of(skip)]
    outstanding: Vec<usize>,
    /// Decision logic for compaction.
    #[size_of(skip)]
    strategy: CompactStrategy,
}

impl<B> Display for Spine<B>
where
    B: Batch + Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.try_fold_batches((), |_, batch| {
            writeln!(f, "batch:\n{}", indent(&batch.to_string(), "    "),)
        })
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
        self.fold_batches(0, |acc, batch| acc + batch.len())
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
        self.fold_batches(0, |acc, batch| acc + batch.key_count())
    }

    fn len(&self) -> usize {
        self.fold_batches(0, |acc, batch| acc + batch.len())
    }

    fn lower(&self) -> AntichainRef<'_, Self::Time> {
        self.lower.as_ref()
    }

    fn upper(&self) -> AntichainRef<'_, Self::Time> {
        self.upper.as_ref()
    }

    fn cursor(&self) -> Self::Cursor<'_> {
        let mut cursors = Vec::with_capacity(
            self.levels
                .iter()
                .flat_map(|level| level.values())
                .map(|l| l.batch_count())
                .sum(),
        );
        for level in self.levels.iter().rev() {
            for merge_state in level.values() {
                match merge_state {
                    BatchState::Merging(bs) => {
                        for batch in bs.iter() {
                            if !batch.is_empty() {
                                cursors.push(batch.cursor());
                            }
                        }
                    }
                    BatchState::Single(batch) => {
                        if !batch.is_empty() {
                            cursors.push(batch.cursor());
                        }
                    }
                }
            }
        }
        SpineCursor::new(&self.factories, cursors)
    }

    fn truncate_keys_below(&mut self, lower_bound: &Self::Key) {
        self.complete_merges();

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

        self.map_batches_mut(|batch| batch.truncate_keys_below(&bound));
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

        self.map_batches(|batch| {
            batch.sample_keys(
                rng,
                ((batch.key_count() as u128) * (sample_size as u128) / (total_keys as u128))
                    as usize,
                intermediate.as_mut(),
            );
        });

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
    /// Display the structure of the spine, including the type of each bin and
    /// the sizes of batches.
    pub fn sketch(&self) -> String {
        let mut s = String::new();

        for level in self.levels.iter() {
            for batch in level.values() {
                match batch {
                    BatchState::Merging(bs) => {
                        s.write_char('[').unwrap();
                        for b in bs.iter() {
                            s.write_fmt(format_args!("{},", b.num_entries_deep()))
                                .unwrap();
                        }
                        s.write_char(']').unwrap();
                    }
                    BatchState::Single(batch) => {
                        s.write_fmt(format_args!("{},", batch.num_entries_deep()))
                            .unwrap();
                    }
                }
            }
        }

        s
    }

    #[allow(dead_code)]
    fn map_batches<F>(&self, mut map: F)
    where
        F: FnMut(&B),
    {
        for level in self.levels.iter().rev() {
            for batch in level.values() {
                match batch {
                    BatchState::Merging(bs) => {
                        for b in bs.iter() {
                            map(b);
                        }
                    }
                    BatchState::Single(batch) => map(batch),
                }
            }
        }
    }

    fn fold_batches<T, F>(&self, init: T, mut fold: F) -> T
    where
        F: FnMut(T, &B) -> T,
    {
        self.levels
            .iter()
            .rev()
            .flat_map(|inner| inner.values())
            .fold(init, |mut acc, batch| match batch {
                BatchState::Merging(bs) => {
                    for b in bs.iter() {
                        acc = fold(acc, b);
                    }
                    acc
                }
                BatchState::Single(batch) => fold(acc, batch),
            })
    }

    // TODO: Use the `Try` trait when stable
    fn try_fold_batches<T, E, F>(&self, init: T, mut fold: F) -> Result<T, E>
    where
        F: FnMut(T, &B) -> Result<T, E>,
    {
        self.levels
            .iter()
            .rev()
            .flat_map(|innser| innser.values())
            .try_fold(init, |mut acc, batch| match batch {
                BatchState::Merging(bs) => {
                    for b in bs.iter() {
                        acc = fold(acc, b)?;
                    }
                    Ok(acc)
                }
                BatchState::Single(batch) => fold(acc, batch),
            })
    }

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

    /// Dequeue a completed merge if there is one available.
    fn try_dequeue_merge(&mut self) -> Result<(BatchIdent, B), StorageError> {
        match self.completion_rx.try_recv()? {
            MergeResult::MergeCompleted(r) => r,
        }
    }

    /// Waits until there is a completed merge, then return it.
    fn dequeue_merge(&mut self) -> Result<(BatchIdent, B), StorageError> {
        match self.completion_rx.recv()? {
            MergeResult::MergeCompleted(r) => r,
        }
    }

    /// Starts a new merge.
    fn enqueue(
        sender: &Arc<Sender<BackgroundOperation>>,
        key: BatchIdent,
        mut batches: Vec<Arc<B>>,
        key_filter: Option<Filter<B::Key>>,
        value_filter: Option<Filter<B::Val>>,
        completed_merge_sender: Arc<Sender<MergeResult<B>>>,
    ) {
        assert_eq!(batches.len(), 2);
        let a = batches.pop().unwrap();
        let b = batches.pop().unwrap();
        let mut merger = None;

        if let Ok(()) = sender.send(BackgroundOperation::Merge(Box::new(
            move |fuel: &mut isize| {
                let start = Instant::now();
                if merger.is_none() {
                    // We initialize this here because to ensure we create the new file we're
                    // writing to on the background thread that's running the closure.
                    merger = Some(<B as Batch>::Merger::new_merger(&a, &b));
                }

                merger
                    .as_mut()
                    .unwrap()
                    .work(&a, &b, &key_filter, &value_filter, fuel);

                if *fuel > 0 {
                    let old_length = a.len() + b.len();
                    let done_merger = merger.take().unwrap();
                    let current = Arc::new(done_merger.done());
                    counter!(TOTAL_COMPACTIONS).increment(1);
                    histogram!(COMPACTION_SIZE).record(current.len() as f64);
                    counter!(COMPACTION_SIZE_SAVINGS)
                        .increment((old_length - current.len()) as u64);
                    histogram!(COMPACTION_DURATION).record(start.elapsed().as_secs_f64());
                    match completed_merge_sender.send(MergeResult::MergeCompleted(Ok((
                        key,
                        // unwrap is fine, we haven't shared the batch with anyone yet.
                        Arc::into_inner(current).unwrap(),
                    )))) {
                        Ok(()) => {
                            // The merge was sent back successfully.
                        }
                        Err(_e) => {
                            // The receiver has been dropped, so the spine is no longer interested in this,
                            // because it already exited.
                        }
                    }
                }
            },
        ))) {}
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
        self.complete_merges();
        self.map_batches_mut(|b| b.recede_to(frontier));
    }

    fn exert(&mut self, _effort: &mut isize) {}

    fn consolidate(mut self) -> Option<B> {
        self.complete_merges();

        let mut batches: Vec<Arc<B>> = self
            .levels
            .into_iter()
            .flat_map(|inner| inner.into_values())
            .map(|batch| match batch {
                BatchState::Single(b) => Arc::new(b),
                BatchState::Merging(_bs) => {
                    // This shouldn't happen because we call complete_merges first.
                    // we could also just add them to `batches and don't wait for
                    // `complete_merges`
                    panic!("In-progress Merge op found during consolidation");
                }
            })
            .collect();

        match batches.len() {
            0 => None,
            1 => Arc::into_inner(batches.pop().unwrap()),
            _ => {
                // We send everything to the merge thread
                // for consolidation into a single batch.
                // Currently self.enqueue needs `batches.len() == 2`
                // so this happens in a loop until there is one batch left.
                while batches.len() > 1 {
                    let to_merge = vec![batches.pop().unwrap(), batches.pop().unwrap()];
                    Self::enqueue(
                        &self.merger_tx,
                        BatchIdent::single(0, 0),
                        to_merge,
                        self.key_filter.clone(),
                        self.value_filter.clone(),
                        self.completion_tx.clone(),
                    );
                    let MergeResult::MergeCompleted(r) = self.completion_rx.recv().unwrap();
                    batches.push(Arc::new(r.unwrap().1));
                }
                assert!(batches.len() == 1);
                Arc::into_inner(batches.pop().unwrap())
            }
        }
    }

    fn insert(&mut self, mut batch: Self::Batch) {
        assert!(batch.lower() != batch.upper());
        if batch.is_empty() {
            return;
        }

        self.try_complete_merges()
            .expect("Failed to complete merges");

        if let Some(bound) = &self.lower_key_bound {
            batch.truncate_keys_below(bound);
        }

        self.dirty = true;
        self.lower = self.lower.as_ref().meet(batch.lower());
        self.upper = self.upper.as_ref().join(batch.upper());

        self.introduce_batch(batch);
    }

    fn clear_dirty_flag(&mut self) {
        self.dirty = false;
    }

    fn dirty(&self) -> bool {
        self.dirty
    }

    fn retain_keys(&mut self, filter: Filter<Self::Key>) {
        self.key_filter = Some(filter);
    }

    fn retain_values(&mut self, filter: Filter<Self::Val>) {
        self.value_filter = Some(filter);
    }

    fn key_filter(&self) -> &Option<Filter<Self::Key>> {
        &self.key_filter
    }

    fn value_filter(&self) -> &Option<Filter<Self::Val>> {
        &self.value_filter
    }

    fn commit<P: AsRef<str>>(&self, cid: Uuid, persistent_id: P) -> Result<(), Error> {
        let committed: CommittedSpine<B> = self.into();
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

    /// Describes the merge progress of layers in the trace.
    ///
    /// Intended for diagnostics rather than public consumption.
    #[allow(dead_code)]
    fn describe(&self) -> Vec<(usize, usize)> {
        self.levels
            .iter()
            .flat_map(|inner| inner.values())
            .map(|b| match b {
                x @ BatchState::Single(_) => (1, x.len()),
                x @ BatchState::Merging(bs) => (bs.len(), x.len()),
            })
            .collect()
    }

    /// Allocates a fueled `Spine` with a specified effort multiplier.
    ///
    /// This trace will merge batches progressively, with each inserted batch
    /// applying a multiple of the batch's length in effort to each merge.
    /// The `effort` parameter is that multiplier. This value should be at
    /// least one for the merging to happen; a value of zero is not helpful.
    pub fn with_effort(factories: &B::Factories, _effort: usize) -> Self {
        let (tx, rx) = unbounded();
        Spine {
            factories: factories.clone(),
            lower: Antichain::from_elem(B::Time::minimum()),
            upper: Antichain::new(),
            levels: (0..MAX_LEVELS).map(|_| BTreeMap::new()).collect(),
            dirty: false,
            lower_key_bound: None,
            key_filter: None,
            value_filter: None,
            next_batch_key: 0,
            merger_tx: Runtime::background_channel(),
            completion_tx: Arc::new(tx),
            completion_rx: rx,
            outstanding: (0..MAX_LEVELS).map(|_| 0).collect(),
            strategy: CompactStrategy::default(),
        }
    }

    /// Introduces a batch at an indicated level.
    fn introduce_batch(&mut self, batch: B) {
        if batch.is_empty() {
            return;
        }
        let level = Self::size_to_level(batch.len());
        self.insert_batch_at(level, BatchState::Single(batch));
        self.maybe_initiate_merges(level);
    }

    /// Dequeue any completed merges and update the trace.
    fn try_complete_merges(&mut self) -> Result<(), Error> {
        loop {
            let dequeued = self.try_complete_merge()?;
            if !dequeued {
                break;
            }
        }
        Ok(())
    }

    /// We remove the old two batches and insert the new one.
    fn handle_completed_batch(&mut self, r: (BatchIdent, B)) {
        let (old_key, new_batch) = r;
        let ret = self.levels[old_key.level].remove(&old_key).unwrap();
        assert!(ret.is_merging(), "We should have found a double batch");
        self.introduce_batch(new_batch);

        self.outstanding[old_key.level] -= 1;
    }

    /// Check if the RX queue from the merger thread has any completed merges,
    /// dequeue the new batch and update the trace by inserting it and removing
    /// the older two.
    ///
    /// Returns true if it dequeued something, or false if the queue was empty.
    fn try_complete_merge(&mut self) -> Result<bool, Error> {
        match self.try_dequeue_merge() {
            Ok(completion) => {
                self.handle_completed_batch(completion);
                Ok(true)
            }
            Err(StorageError::TryRx(TryRecvError::Empty)) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    pub fn apply_fuel(&mut self, _fuel: &mut isize) {}

    /// Inserts the `bs` at `level` and returns the key used to insert it.
    fn insert_batch_at(&mut self, level: usize, bs: BatchState<B>) -> BatchIdent {
        let key = match bs {
            BatchState::Single(_) => BatchIdent::single(level, self.next_batch_key),
            BatchState::Merging(_) => BatchIdent::merging(level, self.next_batch_key),
        };

        let r = self.levels[level].insert(key, bs);
        assert!(r.is_none(), "We will never overwrite an existing entry");
        self.next_batch_key += 1;
        key
    }

    /// Checks if the current level needs compaction and if
    /// so initiates it.
    fn maybe_initiate_merges(&mut self, level: usize) {
        if self.levels[level]
            .keys()
            .filter(|id| id.kind != BatchKind::Merging)
            .count()
            >= self.strategy.min_threshold
        {
            self.initiate_merges_at(level, self.strategy.max_threshold);
        }
    }

    /// Initiates a round of merging at a specified `level`.
    ///
    /// # Arguments
    /// - `level`: At which level to merge things
    /// - `max_initiations`: An upper bound of new merges we initiate.
    ///
    /// # Returns
    /// - The number of new merges initiated.
    fn initiate_merges_at(&mut self, level: usize, max_initiations: usize) -> usize {
        let mut merges_initiated = 0;
        while merges_initiated < max_initiations {
            // We try to get two Single batch entries from the level,
            // the sort order is such that they are in the front of the tree
            match (
                self.levels[level].pop_first(),
                self.levels[level].pop_first(),
            ) {
                (Some((_key1, BatchState::Single(b1))), Some((_key2, BatchState::Single(b2)))) => {
                    let b1_arc = Arc::new(b1);
                    let b2_arc = Arc::new(b2);
                    // We found two single batches, merge them
                    let key = self.insert_batch_at(
                        level,
                        BatchState::Merging(vec![b1_arc.clone(), b2_arc.clone()]),
                    );
                    let key_filter = self.key_filter.clone();
                    let value_filter = self.value_filter.clone();
                    let start = Instant::now();
                    Self::enqueue(
                        &self.merger_tx,
                        key,
                        vec![b1_arc.clone(), b2_arc.clone()],
                        key_filter,
                        value_filter,
                        self.completion_tx.clone(),
                    );
                    counter!(COMPACTION_STALL_TIME).increment(start.elapsed().as_millis() as u64);
                    self.outstanding[level] += 1;

                    merges_initiated += 1;
                }
                // The rest are handling the cases where we don't have two single batches,
                // we re-insert what we popped and exit the loop:
                (Some((key, val)), None) => {
                    self.levels[level].insert(key, val);
                    break;
                }
                (None, Some((key, val))) => {
                    self.levels[level].insert(key, val);
                    break;
                }
                (Some((key1, val1)), Some((key2, val2))) => {
                    self.levels[level].insert(key1, val1);
                    self.levels[level].insert(key2, val2);
                    break;
                }
                (None, None) => {
                    break;
                }
            }
        }

        merges_initiated
    }

    /// Waits for completion of all outstanding merges at a specified `level`.
    fn complete_merges_at(&mut self, level: usize) -> Result<(), Error> {
        while self.outstanding[level] > 0 {
            let r = self.dequeue_merge()?;
            self.handle_completed_batch(r);
        }
        Ok(())
    }

    /// Complete all in-progress merges (without starting any new ones).
    pub(crate) fn complete_merges(&mut self) {
        for level in 0..MAX_LEVELS {
            let r = self.complete_merges_at(level);
            assert!(r.is_ok(), "We should not fail to complete merges");
        }
        debug_assert!(self.outstanding.iter().all(|&x| x == 0));
        debug_assert!(!self
            .levels
            .iter()
            .flat_map(|bs| bs.values())
            .any(|b| b.is_merging()));
    }

    /// Mutate all batches.
    ///
    /// Can only be invoked when there are no in-progress batches in the trace.
    fn map_batches_mut<F: FnMut(&mut <Self as Trace>::Batch)>(&mut self, mut f: F) {
        for batch in self
            .levels
            .iter_mut()
            .flat_map(|level| level.values_mut())
            .rev()
        {
            match batch {
                BatchState::Merging(_) => {
                    panic!("map_batches_mut called on an in-progress batch")
                }
                BatchState::Single(batch) => f(batch),
            }
        }
    }
}
