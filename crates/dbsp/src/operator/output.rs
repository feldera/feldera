use super::{Mailbox, require_persistent_id};
use crate::{
    Batch, BatchReader, Circuit, Error, Runtime, Stream,
    circuit::{
        GlobalNodeId, LocalStoreMarker, OwnershipPreference, RootCircuit, Scope,
        circuit_builder::CircuitBase,
        metadata::{BatchSizeStats, OUTPUT_BATCHES_STATS, OperatorMeta},
        operator_traits::{BinarySinkOperator, Operator, SinkOperator},
    },
    operator::dynamic::accumulator::EnableCount,
    storage::file::to_bytes,
    trace::{
        BatchReader as DynBatchReader, BatchReaderFactories, SpineSnapshot as DynSpineSnapshot,
    },
    typed_batch::{Spine, SpineSnapshot, TypedBatch},
};
use feldera_storage::{FileCommitter, StoragePath};
use std::{
    borrow::Cow,
    fmt::Debug,
    hash::{Hash, Hasher},
    marker::PhantomData,
    mem::transmute,
    ops::Range,
    sync::{Arc, Mutex},
};
use typedmap::TypedMapKey;

impl<T> Stream<RootCircuit, T>
where
    T: Debug + Clone + Send + 'static,
{
    /// Create an output handle that makes the contents of `self` available
    /// outside the circuit.
    ///
    /// This API makes the result of the computation performed by the circuit
    /// available to the outside world.  At each clock cycle, the contents
    /// of the stream is buffered inside the handle and can be read using
    /// the [`OutputHandle`] API.
    #[track_caller]
    pub fn output(&self) -> OutputHandle<T> {
        self.output_persistent(None)
    }

    #[track_caller]
    pub fn output_persistent(&self, persistent_id: Option<&str>) -> OutputHandle<T> {
        self.output_persistent_with_gid(persistent_id).0
    }

    #[track_caller]
    pub fn output_persistent_with_gid(
        &self,
        persistent_id: Option<&str>,
    ) -> (OutputHandle<T>, GlobalNodeId) {
        let (output, output_handle) = Output::new();
        let gid = self.circuit().add_sink(output, self);
        self.circuit().set_persistent_node_id(&gid, persistent_id);

        (output_handle, gid)
    }

    /// Create an output handle that makes the contents of `self` available
    /// outside the circuit on demand.
    ///
    /// This operator is similar to [`output`](`Self::output`), but it only
    /// produces the output conditionally, when the value in the `guard` stream
    /// is `true`.  When `guard` is false, the output mailbox remains empty at
    /// the end of the clock cycle, and [`OutputHandle::take_from_worker`] will
    /// return `None`.  This operator can be used to output a large collection,
    /// such as an integral of a stream, on demand.
    #[track_caller]
    pub fn output_guarded(&self, guard: &Stream<RootCircuit, bool>) -> OutputHandle<T> {
        let (output, output_handle) = OutputGuarded::new();
        self.circuit().add_binary_sink(output, self, guard);
        output_handle
    }
}

impl<B> Stream<RootCircuit, B>
where
    B: Batch + Send,
{
    /// Output operator that produces a single accumulated output per clock cycle.
    #[track_caller]
    pub fn accumulate_output(&self) -> OutputHandle<SpineSnapshot<B>> {
        self.accumulate_output_persistent(None)
    }

    #[track_caller]
    pub fn accumulate_output_persistent(
        &self,
        persistent_id: Option<&str>,
    ) -> OutputHandle<SpineSnapshot<B>> {
        let (handle, enable_count, _) = self.accumulate_output_persistent_with_gid(persistent_id);
        enable_count.enable();
        handle
    }

    /// Accumulate `self` and create an output handle for the accumulated stream.
    ///
    /// Returns:
    /// - The output handle.
    /// - The enable count of the accumulator. Can be used to enable/disable the accumulator.
    /// - The global node ID of the output operator.
    #[track_caller]
    pub fn accumulate_output_persistent_with_gid(
        &self,
        persistent_id: Option<&str>,
    ) -> (OutputHandle<SpineSnapshot<B>>, EnableCount, GlobalNodeId) {
        let (accumulated, enable_count) = self.accumulate().into_parts();
        let (output_handle, gid) = self
            .circuit()
            .output_accumulated_stream_persistent_with_gid::<B>(
                &accumulated,
                enable_count.clone(),
                persistent_id,
            );

        (output_handle, enable_count, gid)
    }
}

impl RootCircuit {
    /// Create an output handle for an accumulated stream `stream`.
    ///
    /// Returns:
    /// - The output handle.
    /// - The enable count of the accumulator. Can be used to enable/disable the accumulator.
    /// - The global node ID of the output operator.
    #[track_caller]
    pub fn output_accumulated_stream_persistent_with_gid<B>(
        &self,
        stream: &Stream<Self, Option<Spine<B>>>,
        enable_count: EnableCount,
        persistent_id: Option<&str>,
    ) -> (OutputHandle<SpineSnapshot<B>>, GlobalNodeId)
    where
        B: Batch + Send,
    {
        let (output, output_handle) = AccumulateOutput::<B>::new(enable_count);

        let gid = self.add_sink(output, stream);
        self.set_persistent_node_id(&gid, persistent_id);

        (output_handle, gid)
    }
}

impl<T> Stream<RootCircuit, Option<T>>
where
    T: Debug + Clone + Send + 'static,
{
    /// Output operator that produces the latest non-`None` output per clock
    /// cycle.
    #[track_caller]
    pub fn latest_output(&self) -> OutputHandle<T> {
        let (output, output_handle) = LatestOutput::<T>::new();

        self.circuit().add_sink(output, self);

        output_handle
    }
}

/// `TypedMapKey` entry used to share `OutputHandle` objects across workers in a
/// runtime. The first worker to create the handle will store it in the map,
/// subsequent workers will get a clone of the same handle.
struct OutputId<T> {
    id: usize,
    _marker: PhantomData<T>,
}

unsafe impl<T> Sync for OutputId<T> {}

// Implement `Hash`, `Eq` manually to avoid `T: Hash` type bound.
impl<T> Hash for OutputId<T> {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.id.hash(state);
    }
}

impl<T> PartialEq for OutputId<T> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl<T> Eq for OutputId<T> {}

impl<T> OutputId<T> {
    fn new(id: usize) -> Self {
        Self {
            id,
            _marker: PhantomData,
        }
    }
}

impl<T> TypedMapKey<LocalStoreMarker> for OutputId<T>
where
    T: 'static,
{
    type Value = OutputHandle<T>;
}

/// `TypedMapKey` entry used to share the pending-cohort slots of an
/// [`AccumulateOutput`] operator across the workers of one host.
struct CohortId<T> {
    id: usize,
    // `fn() -> T` keeps the key `Send + Sync` regardless of `T`: the key
    // names a type but never holds a value of it.
    _marker: PhantomData<fn() -> T>,
}

// Implement `Hash`, `Eq` manually to avoid `T: Hash` type bound.
impl<T> Hash for CohortId<T> {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.id.hash(state);
    }
}

impl<T> PartialEq for CohortId<T> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl<T> Eq for CohortId<T> {}

impl<T> CohortId<T> {
    fn new(id: usize) -> Self {
        Self {
            id,
            _marker: PhantomData,
        }
    }
}

impl<T> TypedMapKey<LocalStoreMarker> for CohortId<T>
where
    T: 'static,
{
    type Value = CohortSlots<T>;
}

/// Pending per-worker outputs for the transaction in progress.
///
/// Slot `i` stores the output of the host's `i`-th worker until every worker
/// on this host has produced its output for the transaction. Remote workers
/// have no slots: they cannot reach this host's store and publish through
/// their own host's instance.
struct CohortSlots<T>(Arc<Mutex<Vec<Option<T>>>>);

impl<T> Clone for CohortSlots<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> CohortSlots<T> {
    fn new(num_workers: usize) -> Self {
        assert_ne!(num_workers, 0);
        Self(Arc::new(Mutex::new(
            (0..num_workers).map(|_| None).collect(),
        )))
    }

    /// Store `value` in `slot` as its worker's output for the transaction in
    /// progress.
    ///
    /// Returns the complete cohort, one value per slot in slot order, when
    /// `value` fills the last empty slot; returns `None` otherwise.
    fn put(&self, slot: usize, value: T) -> Option<Vec<T>> {
        let mut pending = self.0.lock().unwrap();

        // The upstream accumulator emits exactly once per transaction per
        // worker, so the slot must be free.
        debug_assert!(pending[slot].is_none());
        pending[slot] = Some(value);

        if pending.iter().all(Option::is_some) {
            Some(
                pending
                    .iter_mut()
                    .map(|slot| slot.take().unwrap())
                    .collect(),
            )
        } else {
            None
        }
    }
}

struct OutputHandleInternal<T> {
    mailbox: Vec<Mailbox<Option<T>>>,
}

impl<T: Clone> OutputHandleInternal<T> {
    fn new(num_workers: usize) -> Self {
        assert_ne!(num_workers, 0);

        let mut mailbox = Vec::with_capacity(num_workers);
        for _ in 0..num_workers {
            mailbox.push(Mailbox::new(Arc::new(|| None)));
        }

        Self { mailbox }
    }

    fn take_from_worker(&self, worker: usize) -> Option<T> {
        self.mailbox[worker].take()
    }

    fn peek_from_worker<F, O: 'static>(&self, worker: usize, func: F) -> O
    where
        F: Fn(&Option<T>) -> O,
    {
        self.mailbox[worker].map(func)
    }

    fn mailbox(&self, worker: usize) -> &Mailbox<Option<T>> {
        &self.mailbox[worker]
    }
}

/// A handle used to read data from a stream from outside the circuit.
///
/// Internally, the handle manages an array of mailboxes, one for
/// each worker thread.  At the end of each clock cycle, the worker
/// writes the current contents of the stream to the mailbox.
///
/// Once the clock cycle ends (i.e., the
/// [`DBSPHandle::step`](`crate::DBSPHandle::step`) method
/// returns), each mailbox contains a single value -- a copy of
/// stream contents at the current clock cycle.
///
/// The client retrieves values produced by individual workers using
/// the [`take_from_worker`](`OutputHandle::take_from_worker`) method.
/// Alternatively they can retrieve values from all mailboxes at once
/// using [`take_from_all`](`OutputHandle::take_from_all`).
/// If the stream carries relational data, the
/// [`consolidate`](`OutputHandle::consolidate`) method can be used
/// to combine output batches produced by all workers into a single
/// batch.
///
/// Reading from a mailbox using any of these methods removes the value
/// leaving the mailbox empty.  If the value is not read, it gets
/// overwritten at the next clock cycle (i.e., during the next call to
/// `step`).
#[derive(Clone)]
pub struct OutputHandle<T>(Arc<OutputHandleInternal<T>>);

impl<T> OutputHandle<T>
where
    T: Send + Clone + 'static,
{
    fn new() -> Self {
        match Runtime::runtime() {
            None => Self(Arc::new(OutputHandleInternal::new(1))),
            Some(runtime) => {
                let output_id = runtime.sequence_next();

                runtime
                    .local_store()
                    .entry(OutputId::new(output_id))
                    .or_insert_with(|| {
                        Self(Arc::new(OutputHandleInternal::new(Runtime::num_workers())))
                    })
                    .value()
                    .clone()
            }
        }
    }

    fn mailbox(&self, worker: usize) -> &Mailbox<Option<T>> {
        self.0.mailbox(worker)
    }

    /// The number of mailboxes that contain values that haven't been retrieved
    /// yet.
    pub fn num_nonempty_mailboxes(&self) -> usize {
        let num_workers = self.0.mailbox.len();
        let mut non_empty = 0;

        for worker in 0..num_workers {
            non_empty += self.peek_from_worker(worker, Option::is_some) as usize;
        }

        non_empty
    }

    pub fn peek_from_worker<F, O: 'static>(&self, worker: usize, func: F) -> O
    where
        F: Fn(&Option<T>) -> O,
    {
        self.0.peek_from_worker(worker, func)
    }

    /// Read the value produced by `worker` worker thread during the last
    /// clock cycle.
    ///
    /// This method is invoked between two consecutive
    /// [`DBSPHandle::step`](`crate::DBSPHandle::step`)
    /// calls to retrieve the value written to the stream during the last
    /// clock cycle, if any.  It removes the value from the
    /// mailbox, so subsequent calls will return `None`.
    ///
    /// Invoking this method in the middle of a clock cycle, i.e., during
    /// the execution of `DBSPHandle::step`, may return either `None` or
    /// `Some`, depending of whether a value has already been pushed to
    /// the stream.
    pub fn take_from_worker(&self, worker: usize) -> Option<T> {
        self.0.take_from_worker(worker)
    }

    /// Read values produced by all worker threads during the last
    /// clock cycle.
    ///
    /// This is equivalent to calling
    /// [`take_from_worker`](`Self::take_from_worker`) for each
    /// worker thread in order and storing all none-`None`
    /// results in a vector.
    pub fn take_from_all(&self) -> Vec<T> {
        let num_workers = self.0.mailbox.len();
        let mut res = Vec::with_capacity(num_workers);

        for worker in 0..num_workers {
            if let Some(v) = self.take_from_worker(worker) {
                res.push(v);
            }
        }
        res
    }
}

impl<T> OutputHandle<T>
where
    T: Batch<Time = ()>,
    T::InnerBatch: Send,
{
    /// Read batches produced by all worker threads during the last
    /// clock cycle and consolidate them into a single batch.
    ///
    /// This method is used in the common case when the `OutputHandle` is
    /// attached to a stream that carries [`Batch`](`crate::trace::Batch`)es
    /// of updates to relational data.  Semantically, each `Batch` consists
    /// of `(key, value, weight)` tuples.  Depending on the structure of the
    /// circuit, the same `key` or `(key, value)` pair can occur in batches
    /// produced by multiple workers.  This method retrieves batches
    /// produced by all workers and consolidates them into a single batch
    /// where each `(key, value)` pair occurs exactly once.
    ///
    /// Internally, `consolidate` calls `take_from_worker` to retrieve batches
    /// from individual worker threads.  See
    /// [`take_from_worker`](`Self::take_from_worker`) documentation for the
    /// exact semantics of this method.  In particular, note that repeated calls
    /// to `take_from_worker` return `None`. `consolidate` skips `None` results
    /// when computing the consolidated batch.
    pub fn consolidate(&self) -> T {
        let factories = BatchReaderFactories::new::<T::Key, T::Val, T::R>();
        let handle: &OutputHandle<T::Inner> = unsafe { transmute(self) };
        T::from_inner(handle.dyn_consolidate(&factories))
    }
}

impl<T> OutputHandle<T>
where
    T: BatchReader<Time = ()> + Send + Clone,
    T::Inner: Send,
{
    /// Concatenate outputs produced by all worker threads.
    pub fn concat(&self) -> TypedBatch<T::Key, T::Val, T::R, DynSpineSnapshot<T::IntoBatch>> {
        TypedBatch::new(DynSpineSnapshot::concat(
            <T::IntoBatch as DynBatchReader>::Factories::new::<T::Key, T::Val, T::R>(),
            self.take_from_all()
                .into_iter()
                .map(|b| b.into_dyn_snapshot())
                .collect::<Vec<_>>()
                .iter(),
        ))
    }
}

/// Sink operator that stores the contents of its input stream in
/// an `OutputHandle`.
struct Output<T> {
    global_id: GlobalNodeId,
    mailbox: Mailbox<Option<T>>,
}

impl<T> Output<T>
where
    T: Clone + Send + 'static,
{
    fn new() -> (Self, OutputHandle<T>) {
        let handle = OutputHandle::new();
        let mailbox = handle.mailbox(Runtime::worker_index()).clone();

        let output = Self {
            global_id: GlobalNodeId::root(),
            mailbox,
        };

        (output, handle)
    }

    fn checkpoint_file(base: &StoragePath, persistent_id: &str) -> StoragePath {
        base.child(format!("output-{}.dat", persistent_id))
    }
}

impl<T> Operator for Output<T>
where
    T: Clone + Send + 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("Output")
    }

    fn init(&mut self, global_id: &GlobalNodeId) {
        self.global_id = global_id.clone();
    }

    fn checkpoint(
        &mut self,
        base: &StoragePath,
        pid: Option<&str>,
        files: &mut Vec<Arc<dyn FileCommitter>>,
    ) -> Result<(), Error> {
        let pid = require_persistent_id(pid, &self.global_id)?;
        let as_bytes = to_bytes(&()).expect("Serializing () should work.");

        files.push(
            Runtime::storage_backend()
                .unwrap()
                .write(&Self::checkpoint_file(base, pid), as_bytes)?,
        );

        Ok(())
    }

    fn restore(&mut self, base: &StoragePath, pid: Option<&str>) -> Result<(), Error> {
        let pid = require_persistent_id(pid, &self.global_id)?;

        let path = Self::checkpoint_file(base, pid);
        let _content = Runtime::storage_backend().unwrap().read(&path)?;

        Ok(())
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<T> SinkOperator<T> for Output<T>
where
    T: Debug + Clone + Send + 'static,
{
    async fn eval(&mut self, val: &T) {
        self.mailbox.set(Some(val.clone()));
    }

    async fn eval_owned(&mut self, val: T) {
        self.mailbox.set(Some(val));
    }

    fn input_preference(&self) -> OwnershipPreference {
        OwnershipPreference::PREFER_OWNED
    }
}

/// Sink operator that publishes an accumulated stream through an
/// [`OutputHandle`].
///
/// Every worker's accumulator emits exactly once per transaction, but when a
/// transaction commit spans several steps, workers can emit in different
/// steps. Writing each emission to its mailbox immediately would let a reader
/// observe a partial cross-worker output for a transaction. Instead, the
/// operator parks emissions in [`CohortSlots`] shared by the workers of one
/// host and publishes the whole cohort to the mailboxes when the last of
/// these workers emits, so a reader sees either all of a transaction's
/// outputs or none.
///
/// In a multihost runtime, the cohort covers one host: each host's workers
/// share host-local [`CohortSlots`] and publish to host-local mailboxes,
/// which remote workers cannot reach. A reader observes a host's outputs
/// atomically; distinct hosts publish independently.
pub struct AccumulateOutput<B>
where
    B: Batch,
{
    global_id: GlobalNodeId,
    handle: OutputHandle<SpineSnapshot<B>>,
    /// Global indices of this host's workers: cohort slot `i` belongs to
    /// worker `local_workers.start + i`.
    local_workers: Range<usize>,
    /// This worker's cohort slot: its offset within `local_workers`.
    slot: usize,
    pending: CohortSlots<SpineSnapshot<B>>,
    output_batch_stats: BatchSizeStats,

    /// Enable count of the paired upstream [`Accumulator`](crate::operator::dynamic::accumulator::Accumulator).
    ///
    /// A concurrent bootstrap circuit (copy 2) has no output connector
    /// attached, so its accumulators would be disabled and would discard the
    /// view's contents.  Entering caching mode force-enables the accumulator
    /// through this handle (see [`Self::start_bootstrap_output_caching`]).
    enable_count: EnableCount,

    /// `true` in a concurrent bootstrap circuit (copy 2): the view's
    /// accumulated output is cached for transfer to the live circuit at
    /// cutover instead of being published to the mailboxes (the bootstrap
    /// circuit has no connector reading them).
    caching: bool,

    /// `true` once [`Self::start_bootstrap_output_caching`] has bumped
    /// [`Self::enable_count`], so the bump happens exactly once.
    bootstrap_enabled: bool,

    /// The view's accumulated output, as a single snapshot.
    ///
    /// In a bootstrap circuit (copy 2) this collects the deltas flushed during
    /// the backfill and synchronization transactions.  At cutover it is
    /// swapped into the live circuit's operator (see [`Self::swap_state`]),
    /// where the next committed transaction combines it with that
    /// transaction's output and publishes the result.
    cache: Option<SpineSnapshot<B>>,
}

impl<B> AccumulateOutput<B>
where
    B: Batch + Send,
{
    pub fn new(enable_count: EnableCount) -> (Self, OutputHandle<SpineSnapshot<B>>) {
        let handle = OutputHandle::new();

        // The slots live in the host-local store, out of reach of remote
        // workers, so the cohort covers only this host's workers.
        let (local_workers, pending) = match Runtime::runtime() {
            None => (0..1, CohortSlots::new(1)),
            Some(runtime) => {
                let local_workers = runtime.layout().local_workers();
                let cohort_id = runtime.sequence_next();

                let pending = runtime
                    .local_store()
                    .entry(CohortId::new(cohort_id))
                    .or_insert_with(|| CohortSlots::new(local_workers.len()))
                    .value()
                    .clone();

                (local_workers, pending)
            }
        };

        let output = Self {
            global_id: GlobalNodeId::root(),
            handle: handle.clone(),
            local_workers,
            slot: Runtime::local_worker_offset(),
            pending,
            output_batch_stats: BatchSizeStats::new(),
            enable_count,
            caching: false,
            bootstrap_enabled: false,
            cache: None,
        };

        (output, handle)
    }

    /// Write this worker's output; publish the cohort if it is now complete.
    ///
    /// Only the worker that completes a cohort publishes it, so there is
    /// never more than one publisher per cohort. Mailboxes are indexed by
    /// global worker index, slots by local offset.
    fn put_and_publish(&self, snapshot: SpineSnapshot<B>) {
        if let Some(cohort) = self.pending.put(self.slot, snapshot) {
            for (worker, snapshot) in self.local_workers.clone().zip(cohort) {
                self.handle.mailbox(worker).set(Some(snapshot));
            }
        }
    }

    fn checkpoint_file(base: &StoragePath, persistent_id: &str) -> StoragePath {
        base.child(format!("accumulate-output-{}.dat", persistent_id))
    }

    /// Merge `snapshot` into the cached accumulated output.
    fn merge_into_cache(&mut self, snapshot: SpineSnapshot<B>) {
        self.cache = Some(match self.cache.take() {
            None => snapshot,
            Some(cached) => SpineSnapshot::<B>::concat([&cached, &snapshot]),
        });
    }

    /// Route the transaction's output `snapshot`.
    fn deliver(&mut self, snapshot: SpineSnapshot<B>) {
        if self.caching {
            // Bootstrap circuit: accumulate the view's output across the
            // backfill and synchronization transactions for transfer at
            // cutover instead of publishing it.
            self.merge_into_cache(snapshot);
        } else if let Some(cached) = self.cache.take() {
            // First committed transaction after a cutover swapped in the
            // backfilled output: combine it with this transaction's output so
            // the connector observes the full view as its first batch.
            self.put_and_publish(SpineSnapshot::<B>::concat([&cached, &snapshot]));
        } else {
            self.put_and_publish(snapshot);
        }
    }
}

impl<B> Operator for AccumulateOutput<B>
where
    B: Batch + Send,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("AccumulateOutput")
    }

    fn init(&mut self, global_id: &GlobalNodeId) {
        self.global_id = global_id.clone();
    }

    fn checkpoint(
        &mut self,
        base: &StoragePath,
        pid: Option<&str>,
        files: &mut Vec<Arc<dyn FileCommitter>>,
    ) -> Result<(), Error> {
        let pid = require_persistent_id(pid, &self.global_id)?;
        let as_bytes = to_bytes(&()).expect("Serializing () should work.");

        files.push(
            Runtime::storage_backend()
                .unwrap()
                .write(&Self::checkpoint_file(base, pid), as_bytes)?,
        );

        Ok(())
    }

    fn restore(&mut self, base: &StoragePath, pid: Option<&str>) -> Result<(), Error> {
        let pid = require_persistent_id(pid, &self.global_id)?;

        let path = Self::checkpoint_file(base, pid);
        let _content = Runtime::storage_backend().unwrap().read(&path)?;

        Ok(())
    }

    fn metadata(&self, meta: &mut OperatorMeta) {
        meta.extend(metadata! {
            OUTPUT_BATCHES_STATS => self.output_batch_stats.metadata(),
        });
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }

    fn start_bootstrap_output_caching(&mut self) {
        self.caching = true;

        // The bootstrap circuit has no connector reading this view, so its
        // paired accumulator would otherwise stay disabled and discard the
        // view's contents.  Force-enable it once. The bump is local to the
        // bootstrap circuit's enable count and is harmless if the accumulator
        // was already enabled.
        if !self.bootstrap_enabled {
            self.bootstrap_enabled = true;
            self.enable_count.enable();
        }
    }

    fn swap_state(&mut self, other: &mut Self) -> Result<(), Error> {
        // Transfer the cached output from the bootstrap circuit (`other`) to
        // this live operator.  Only the bootstrap circuit caches, so `self`'s
        // cache is empty going in; afterwards this operator holds the
        // backfilled output and `other`'s state is irrelevant (it is dropped).
        std::mem::swap(&mut self.cache, &mut other.cache);
        Ok(())
    }
}

impl<B> SinkOperator<Option<Spine<B>>> for AccumulateOutput<B>
where
    B: Batch + Send,
{
    async fn eval(&mut self, val: &Option<Spine<B>>) {
        if let Some(val) = val {
            self.output_batch_stats.add_batch(val.len());
            // Deliver even empty outputs: cohort completion requires one
            // emission per worker per transaction.
            self.deliver(val.ro_snapshot());
        }
    }

    async fn eval_owned(&mut self, val: Option<Spine<B>>) {
        if let Some(val) = val {
            self.output_batch_stats.add_batch(val.len());
            self.deliver(val.ro_snapshot());
        }
    }

    fn input_preference(&self) -> OwnershipPreference {
        OwnershipPreference::PREFER_OWNED
    }
}

pub struct LatestOutput<T>
where
    T: Debug + Clone + Send + 'static,
{
    mailbox: Mailbox<Option<T>>,
}

impl<T> LatestOutput<T>
where
    T: Debug + Clone + Send + 'static,
{
    pub fn new() -> (Self, OutputHandle<T>) {
        let handle = OutputHandle::new();
        let mailbox = handle.mailbox(Runtime::worker_index()).clone();

        let output = Self { mailbox };

        (output, handle)
    }
}

impl<T> Operator for LatestOutput<T>
where
    T: Debug + Clone + Send + 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("LatestOutput")
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<T> SinkOperator<Option<T>> for LatestOutput<T>
where
    T: Debug + Clone + Send + 'static,
{
    async fn eval(&mut self, val: &Option<T>) {
        self.eval_owned(val.clone()).await;
    }

    async fn eval_owned(&mut self, val: Option<T>) {
        if val.is_some() {
            self.mailbox.set(val);
        }
    }

    fn input_preference(&self) -> OwnershipPreference {
        OwnershipPreference::PREFER_OWNED
    }
}

struct OutputGuarded<T> {
    mailbox: Mailbox<Option<T>>,
}

impl<T> OutputGuarded<T>
where
    T: Clone + Send + 'static,
{
    fn new() -> (Self, OutputHandle<T>) {
        let handle = OutputHandle::new();
        let mailbox = handle.mailbox(Runtime::worker_index()).clone();

        let output = Self { mailbox };

        (output, handle)
    }
}

impl<T> Operator for OutputGuarded<T>
where
    T: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("OutputGuarded")
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<T> BinarySinkOperator<T, bool> for OutputGuarded<T>
where
    T: Clone + 'static,
{
    async fn eval<'a>(&mut self, val: Cow<'a, T>, guard: Cow<'a, bool>) {
        if *guard {
            self.mailbox.set(Some(val.into_owned()));
        }
    }

    fn input_preference(&self) -> (OwnershipPreference, OwnershipPreference) {
        (
            OwnershipPreference::PREFER_OWNED,
            OwnershipPreference::INDIFFERENT,
        )
    }
}

#[cfg(test)]
mod test {
    use super::CohortSlots;
    use crate::{Runtime, typed_batch::OrdZSet, utils::Tup2};

    /// Parking releases a cohort only when every worker has contributed, and
    /// the slots are reusable for the next cohort.
    #[test]
    fn test_cohort_slots() {
        let slots = CohortSlots::new(3);

        // Partial cohort: nothing is released.
        assert_eq!(slots.put(0, "a0"), None);
        assert_eq!(slots.put(2, "a2"), None);

        // The last worker's output releases the whole cohort.
        assert_eq!(slots.put(1, "a1"), Some(vec!["a0", "a1", "a2"]));

        // The slots are empty again and serve the next cohort.
        assert_eq!(slots.put(1, "b1"), None);
        assert_eq!(slots.put(2, "b2"), None);
        assert_eq!(slots.put(0, "b0"), Some(vec!["b0", "b1", "b2"]));
    }

    /// End-to-end: the accumulated output surfaces exactly once per
    /// transaction, as a complete cohort with one output per worker, and
    /// never mid-transaction or as a partial cohort mid-commit.
    ///
    /// Drives transactions with the manual `start_transaction` ->
    /// `start_commit_transaction` -> `step` API so the handle can be
    /// inspected after every step, including each step of a multi-step
    /// commit; `DBSPHandle::transaction` would only return after the commit
    /// completed, when the cohort is trivially complete.
    #[test]
    fn test_accumulate_output() {
        use crate::trace::BatchReader;

        const WORKERS: usize = 8;

        let (mut dbsp, (input, output)) = Runtime::init_circuit(WORKERS, |circuit| {
            let (zset, zset_handle) = circuit.add_input_zset::<u64>();
            let output = zset.accumulate_output();

            Ok((zset_handle, output))
        })
        .unwrap();

        for round in 0..20u64 {
            // Odd rounds run an empty transaction, which must still publish
            // a complete, empty cohort.
            let expected = if round % 2 == 0 { 20_000 } else { 0 };

            dbsp.start_transaction().unwrap();

            if expected > 0 {
                let mut tuples = (round * 100_000..round * 100_000 + 20_000)
                    .map(|k| Tup2(k, 1i64))
                    .collect::<Vec<_>>();
                input.append(&mut tuples);
            }

            // Steps inside an open transaction publish nothing.
            dbsp.step().unwrap();
            assert!(output.take_from_all().is_empty());
            dbsp.step().unwrap();
            assert!(output.take_from_all().is_empty());

            dbsp.start_commit_transaction().unwrap();

            // Every commit step yields either nothing or one complete
            // cohort, and the whole transaction yields exactly one.
            let mut cohorts = 0;
            loop {
                let committed = dbsp.step().unwrap();

                let batches = output.take_from_all();
                if !batches.is_empty() {
                    assert_eq!(batches.len(), WORKERS);
                    assert_eq!(batches.iter().map(|b| b.len()).sum::<usize>(), expected);
                    cohorts += 1;
                }

                if committed {
                    break;
                }
            }
            assert_eq!(cohorts, 1);
            assert!(output.take_from_all().is_empty());
        }

        dbsp.kill().unwrap();
    }

    #[test]
    fn test_output_handle() {
        let (mut dbsp, (input, output)) = Runtime::init_circuit(4, |circuit| {
            let (zset, zset_handle) = circuit.add_input_zset::<u64>();
            let zset_output = zset.output();

            Ok((zset_handle, zset_output))
        })
        .unwrap();

        let inputs = vec![
            vec![Tup2(1, 1), Tup2(2, 1), Tup2(3, 1), Tup2(4, 1), Tup2(5, 1)],
            vec![
                Tup2(1, -1),
                Tup2(2, -1),
                Tup2(3, -1),
                Tup2(4, -1),
                Tup2(5, -1),
            ],
        ];

        for mut input_vec in inputs {
            let input_tuples = input_vec
                .iter()
                .map(|Tup2(k, w)| Tup2(Tup2(*k, ()), *w))
                .collect::<Vec<_>>();

            let expected_output = OrdZSet::from_tuples((), input_tuples);

            input.append(&mut input_vec);
            dbsp.transaction().unwrap();
            let output = output.consolidate();
            assert_eq!(output, expected_output);
        }

        dbsp.kill().unwrap();
    }

    #[test]
    fn test_guarded_output_handle() {
        let (mut dbsp, (input, guard, output)) = Runtime::init_circuit(4, |circuit| {
            let (zset, zset_handle) = circuit.add_input_zset::<u64>();
            let (guard, guard_handle) = circuit.add_input_stream::<bool>();
            let zset_output = zset.output_guarded(&guard);

            Ok((zset_handle, guard_handle, zset_output))
        })
        .unwrap();

        let inputs = vec![
            vec![Tup2(1, 1), Tup2(2, 1), Tup2(3, 1), Tup2(4, 1), Tup2(5, 1)],
            vec![
                Tup2(1, -1),
                Tup2(2, -1),
                Tup2(3, -1),
                Tup2(4, -1),
                Tup2(5, -1),
            ],
        ];

        for mut input_vec in inputs {
            let input_tuples = input_vec
                .iter()
                .map(|Tup2(k, w)| Tup2(Tup2(*k, ()), *w))
                .collect::<Vec<_>>();

            let expected_output = OrdZSet::from_tuples((), input_tuples);

            input.append(&mut input_vec.clone());
            guard.set_for_all(false);
            dbsp.transaction().unwrap();
            let output1 = output.consolidate();
            assert_eq!(output1, OrdZSet::empty());

            input.append(&mut input_vec);
            guard.set_for_all(true);
            dbsp.transaction().unwrap();
            let output2 = output.consolidate();

            assert_eq!(output2, expected_output);
        }

        dbsp.kill().unwrap();
    }
}
