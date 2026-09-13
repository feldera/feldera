//! Input upsert that defers its integral lookups to the end of a transaction.
//!
//! [`RootCircuit::dyn_add_lazy_input_map`] offers the same interface as
//! [`RootCircuit::dyn_add_input_map`](crate::RootCircuit::dyn_add_input_map) and
//! the same semantics, but pays for the integral differently.  The eager
//! operator resolves every update against the integral in the step the update
//! arrives, one random probe per key per step.  This one stamps each update with
//! the step it arrived in, accumulates the stamped updates for the whole
//! transaction, and resolves them all in a single ordered scan when the
//! transaction commits.
//!
//! The stamp is what makes that possible.  A Z-set has no order, so without it
//! two updates to one key inside a transaction would be an unordered pair and
//! "which update wins" would have no answer.  The stamp is the last sort
//! component of the value, so records sharing a value stay contiguous within a
//! key, which is what lets the projection that removes it consolidate a run in
//! one pass.

use crate::{
    Circuit, DBData, Error, NumEntries, Position, RootCircuit, Runtime, Stream, ZWeight,
    algebra::{IndexedZSet, OrdIndexedZSet, OrdIndexedZSetFactories},
    circuit::{
        GlobalNodeId, OwnershipPreference, Scope,
        circuit_builder::{CircuitBase, RefStreamValue},
        lazy_input_map_keys_per_step,
        metadata::{
            ALLOCATED_MEMORY_BYTES, BatchSizeStats, CONFLICTING_UPDATES_COUNT, DurationHistogram,
            INPUT_BATCHES_STATS, MEMORY_ALLOCATIONS_COUNT, MetaItem, OUTPUT_ADJUSTMENT_STATS,
            OperatorMeta, SHARED_MEMORY_BYTES, STATE_RECORDS_COUNT, STEP_DURATION_HISTOGRAM,
            USED_MEMORY_BYTES,
        },
        operator_traits::{Operator, OperatorName, UnaryOperator},
        splitter_output_chunk_size, splitter_output_first_chunk_size,
    },
    dynamic::{
        DataTrait, DowncastTrait, DynData, DynOpt, DynPair, DynPairs, Erase, Factory, WithFactory,
    },
    operator::{
        Input,
        async_stream_operators::{StreamingBinaryOperator, StreamingBinaryWrapper},
        dynamic::{
            accumulate_trace::{
                AccumulateTraceId, AccumulateUntimedTraceAppend, AccumulateZ1Trace,
                ShardedAccumulateTraceId,
            },
            accumulator::{Accumulation, AccumulatorId, EnableCount},
            input::{IndexedZSetStream, UpsertHandle},
            sharded_accumulator::ShardedAccumulatorId,
            trace::TraceBounds,
        },
    },
    trace::{
        BatchFactories, BatchReader, BatchReaderFactories, Builder, Cursor, Spine, Trace,
        TraceRole, WithSnapshot, merge_batches_by_reference,
    },
    utils::Tup2,
};
use async_stream::stream;
use futures::Stream as AsyncStream;
use size_of::SizeOf;
use std::{
    borrow::Cow,
    cell::{Cell, RefCell},
    marker::PhantomData,
    panic::Location,
    rc::Rc,
    sync::Arc,
    time::Instant,
};
use tracing::warn;

/// The stamped form of `B`: the same keys, with each value paired with the
/// `u32` step it arrived in.
pub type Stamped<B> =
    OrdIndexedZSet<<B as BatchReader>::Key, DynPair<<B as BatchReader>::Val, DynData>>;

/// Factories for [`RootCircuit::dyn_add_lazy_input_map`].
pub struct AddLazyInputMapFactories<B>
where
    B: IndexedZSet,
{
    /// The unstamped batches the operator emits.
    pub batch_factories: B::Factories,

    /// The stamped batches the accumulator holds.  Built with a projection, so
    /// a stamped batch can present itself as an unstamped one.
    pub stamped_factories: <Stamped<B> as BatchReader>::Factories,

    input_pair_factory: &'static dyn Factory<DynPair<B::Key, DynOpt<B::Val>>>,
    input_pairs_factory: &'static dyn Factory<DynPairs<B::Key, DynOpt<B::Val>>>,

    /// A `(value, stamp)` pair, built once per surviving update.
    stamped_val_factory: &'static dyn Factory<DynPair<B::Val, DynData>>,
}

impl<K, V> AddLazyInputMapFactories<OrdIndexedZSet<K, V>>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
{
    pub fn new<KType, VType>() -> Self
    where
        KType: DBData + Erase<K>,
        VType: DBData + Erase<V>,
        Tup2<KType, Option<VType>>: DBData,
        Tup2<VType, u32>: DBData + Erase<DynPair<V, DynData>>,
    {
        Self {
            // Built with a projection, so the batches this operator emits can
            // be stamped batches presenting themselves as unstamped ones.
            batch_factories: OrdIndexedZSetFactories::with_projection::<KType, VType, ZWeight>(),
            stamped_factories: BatchReaderFactories::new::<KType, Tup2<VType, u32>, ZWeight>(),
            input_pair_factory: WithFactory::<Tup2<KType, Option<VType>>>::FACTORY,
            input_pairs_factory:
                WithFactory::<crate::dynamic::LeanVec<Tup2<KType, Option<VType>>>>::FACTORY,
            stamped_val_factory: WithFactory::<Tup2<VType, u32>>::FACTORY,
        }
    }
}

impl<B> Clone for AddLazyInputMapFactories<B>
where
    B: IndexedZSet,
{
    fn clone(&self) -> Self {
        Self {
            batch_factories: self.batch_factories.clone(),
            stamped_factories: self.stamped_factories.clone(),
            input_pair_factory: self.input_pair_factory,
            input_pairs_factory: self.input_pairs_factory,
            stamped_val_factory: self.stamped_val_factory,
        }
    }
}

/// Presents a spine of stamped batches as a spine of unstamped ones.
///
/// Each batch is rewrapped rather than copied: the stamped and unstamped
/// spellings describe the same records, and the projection happens in the
/// cursor.  The resulting spine starts from the batch list alone, so whatever
/// merges the stamped spine had in flight are discarded; preserving them is a
/// separate change.
pub async fn remove_stamp<K, V>(
    factories: &OrdIndexedZSetFactories<K, V>,
    stamped: Vec<Arc<Stamped<OrdIndexedZSet<K, V>>>>,
    name: Arc<String>,
) -> Spine<OrdIndexedZSet<K, V>>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
{
    let mut spine =
        <Spine<OrdIndexedZSet<K, V>> as Trace>::new(factories, name, TraceRole::Integral);
    for batch in stamped {
        spine
            .insert(OrdIndexedZSet::project_batch(factories, &batch))
            .await;
    }
    spine
}

/// Turns each step's updates into a stamped indexed Z-set.
///
/// Per step and per key exactly one update survives -- the last one, in arrival
/// order -- and it becomes one record:
///
/// * `Insert(v)` becomes `(key, (v, stamp))` with weight `+1`.
/// * `Delete` becomes `(key, (v, stamp))` with weight `-1`.  The value is the
///   value factory's default, and which value it is does not matter: the
///   downstream scan retracts the record it belongs to, so the two annihilate
///   and only the retraction of the integral's value survives.  Within a
///   transaction the delta therefore carries a record no reader should read on
///   its own, which is sound because only the committed total is observable.
///
/// The stamp is the step's index within the transaction, so it orders a key's
/// updates across the whole transaction.
pub struct Stamp<K, V, B>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    B: IndexedZSet<Key = K, Val = V>,
{
    factories: AddLazyInputMapFactories<B>,

    /// Steps elapsed in this transaction, and so the stamp the next step's
    /// updates carry.
    stamp: u32,

    input_batch_stats: BatchSizeStats,
    phantom: PhantomData<fn(&K, &V)>,
}

impl<K, V, B> Stamp<K, V, B>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    B: IndexedZSet<Key = K, Val = V>,
{
    pub fn new(factories: &AddLazyInputMapFactories<B>) -> Self {
        Self {
            factories: factories.clone(),
            stamp: 0,
            input_batch_stats: BatchSizeStats::new(),
            phantom: PhantomData,
        }
    }
}

impl<K, V, B> Operator for Stamp<K, V, B>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    B: IndexedZSet<Key = K, Val = V>,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("Stamp")
    }

    /// The stamp orders updates within one transaction and nothing beyond it,
    /// so it starts over with each transaction.
    fn start_transaction(&mut self) {
        self.stamp = 0;
    }

    fn metadata(&self, meta: &mut OperatorMeta) {
        meta.extend(metadata! {
            INPUT_BATCHES_STATS => self.input_batch_stats.metadata(),
        });
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<K, V, B> UnaryOperator<Vec<Box<DynPairs<K, DynOpt<V>>>>, Stamped<B>> for Stamp<K, V, B>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    B: IndexedZSet<Key = K, Val = V>,
{
    async fn eval(&mut self, _updates: &Vec<Box<DynPairs<K, DynOpt<V>>>>) -> Stamped<B> {
        // The operator sorts its input in place, so it cannot take it by
        // reference.  Nothing else reads the input stream, so a correctly built
        // circuit never asks it to.
        panic!("Stamp::eval(): cannot accept updates by reference")
    }

    fn input_preference(&self) -> OwnershipPreference {
        OwnershipPreference::STRONGLY_PREFER_OWNED
    }

    async fn eval_owned(&mut self, mut updates: Vec<Box<DynPairs<K, DynOpt<V>>>>) -> Stamped<B> {
        let stamp = self.stamp;
        // A transaction of four billion steps is not reachable in practice, but
        // wrapping here would silently reorder a key's updates.
        self.stamp = self
            .stamp
            .checked_add(1)
            .expect("a transaction cannot hold more steps than a stamp can count");

        // The merge below walks the vectors in key order and needs a key's
        // updates in the order the client wrote them, and nothing upstream
        // promises either: the client appends in whatever order it likes, a key
        // as often as it likes.  The sort establishes both, being stable and
        // comparing keys alone.
        //
        // The dedup only trims work.  A key's updates within one step share a
        // stamp, so the merge keeps the last of them either way; dropping the
        // rest here keeps them out of it.
        updates.retain_mut(|pairs| {
            pairs.sort_by_key();
            pairs.dedup_by_key_keep_last();
            !pairs.is_empty()
        });

        // The vectors are in append order, so an n-way merge visits a key's
        // updates in that order too: `min_by` returns the first among equal
        // keys.
        let mut sources: Vec<(&DynPairs<K, DynOpt<V>>, usize)> =
            updates.iter().map(|pairs| (&**pairs, 0)).collect();
        let n_updates: usize = sources.iter().map(|(pairs, _)| pairs.len()).sum();
        self.input_batch_stats.add_batch(n_updates);

        let mut builder = <Stamped<B> as crate::trace::Batch>::Builder::with_capacity(
            &self.factories.stamped_factories,
            n_updates,
            n_updates,
        );

        // The surviving update for the key under construction.  The key is
        // cloned into one box that lasts the whole call, since the merge only
        // ever needs the key it is currently on.
        let mut cur_key = self.factories.batch_factories.key_factory().default_box();
        let mut have_key = false;
        let mut surviving_val = self.factories.batch_factories.val_factory().default_box();
        let mut surviving_weight: ZWeight = 0;

        let mut stamped_val = self.factories.stamped_val_factory.default_box();
        let default_val = self.factories.batch_factories.val_factory().default_box();

        loop {
            let next = sources
                .iter()
                .map(|(pairs, index)| pairs.index(*index))
                .enumerate()
                .min_by(|(_, a), (_, b)| a.fst().cmp(b.fst()));

            let Some((source, pair)) = next else { break };
            sources[source].1 += 1;
            if sources[source].1 >= sources[source].0.len() {
                sources.remove(source);
            }

            let (key, update) = pair.split();

            if !have_key || &*cur_key != key {
                if have_key {
                    push_stamped::<K, V, B>(
                        &mut builder,
                        &mut stamped_val,
                        &cur_key,
                        &mut surviving_val,
                        surviving_weight,
                        stamp,
                    );
                }
                key.clone_to(&mut cur_key);
                have_key = true;
            }

            // The last update for a key wins, so each one simply overwrites the
            // one before it.
            match update.get() {
                Some(val) => {
                    val.clone_to(&mut surviving_val);
                    surviving_weight = 1;
                }
                None => {
                    // Any value works here; see the type's documentation.
                    default_val.clone_to(&mut surviving_val);
                    surviving_weight = -1;
                }
            }
        }

        if have_key {
            push_stamped::<K, V, B>(
                &mut builder,
                &mut stamped_val,
                &cur_key,
                &mut surviving_val,
                surviving_weight,
                stamp,
            );
        }

        builder.done()
    }
}

/// Writes one `(key, (value, stamp))` record with the given weight.
fn push_stamped<K, V, B>(
    builder: &mut <Stamped<B> as crate::trace::Batch>::Builder,
    stamped_val: &mut Box<DynPair<V, DynData>>,
    key: &K,
    val: &mut Box<V>,
    weight: ZWeight,
    stamp: u32,
) where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    B: IndexedZSet<Key = K, Val = V>,
{
    let (value_half, stamp_half) = stamped_val.split_mut();
    val.move_to(value_half);
    *unsafe { stamp_half.downcast_mut::<u32>() } = stamp;

    let mut weight = weight;
    builder.push_val_diff_mut(&mut **stamped_val, weight.erase_mut());
    builder.push_key(key);
}

/// Resolves a transaction's accumulated updates against the integral.
///
/// The eager operator probes the integral once per key per step.  This one is
/// handed every update the transaction made, already ordered by the stamp, and
/// the integral as it stood when the transaction began, and walks both in key
/// order: the probes become one sequential pass.
///
/// It computes the adjustments `A` such that `project(U) + I + A` is the
/// collection's new state.  For each key the accumulator holds:
///
/// * the integral's current value is retracted, since the key is being written;
/// * every update but the one with the largest stamp is retracted, since only
///   the last one survives;
/// * if the survivor is a delete, it is retracted too, which cancels the record
///   `project(U)` contributes and leaves the key with nothing.
///
/// The main output is the accumulator: `project(U)` with the adjustments added.
/// The adjustments also leave on a second stream, which the circuit concatenates
/// with the projected updates to form the delta.
pub struct LazyUpsert<K, V, B>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    B: IndexedZSet<Key = K, Val = V>,
{
    factories: AddLazyInputMapFactories<B>,

    /// The adjustments of the step that just ran, for the delta stream.
    adjustments: RefStreamValue<Arc<OrdIndexedZSet<K, V>>>,

    /// The accumulator under construction.
    ///
    /// It holds the transaction's updates with their stamps dropped, and grows
    /// by a chunk of adjustments per step until the commit finishes, so it is
    /// state the operator carries between steps and worth reporting as such.
    state: RefCell<Option<Spine<OrdIndexedZSet<K, V>>>>,

    output_adjustment_stats: RefCell<BatchSizeStats>,

    /// How long the operator held each step it ran in.
    ///
    /// Every worker meets its peers at a barrier at the end of a step, so a
    /// step costs each of them what the slowest spent in it.  The total says
    /// nothing about that; the spread does.
    step_durations: RefCell<DurationHistogram>,

    /// Updates that arrived at the same step as another update to their key.
    ///
    /// Each host stamps its own steps from zero and `UpsertHandle` shards only
    /// within a host, so two hosts ingesting one key in a transaction give it
    /// two updates at the same stamp.  The resolution still has to pick one.
    conflicting_updates: Cell<u64>,

    name: OperatorName,
    phantom: PhantomData<fn(&K, &V)>,
}

impl<K, V, B> LazyUpsert<K, V, B>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    B: IndexedZSet<Key = K, Val = V>,
{
    pub fn new(
        factories: &AddLazyInputMapFactories<B>,
        adjustments: RefStreamValue<Arc<OrdIndexedZSet<K, V>>>,
    ) -> Self {
        Self {
            factories: factories.clone(),
            adjustments,
            state: RefCell::new(None),
            output_adjustment_stats: RefCell::new(BatchSizeStats::new()),
            step_durations: RefCell::new(DurationHistogram::new()),
            conflicting_updates: Cell::new(0),
            name: OperatorName::new("LazyUpsert"),
            phantom: PhantomData,
        }
    }
}

impl<K, V, B> Operator for LazyUpsert<K, V, B>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    B: IndexedZSet<Key = K, Val = V>,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("LazyUpsert")
    }

    fn init(&mut self, global_id: &GlobalNodeId) {
        self.name.init(global_id);
    }

    fn metadata(&self, meta: &mut OperatorMeta) {
        meta.extend(metadata! {
            OUTPUT_ADJUSTMENT_STATS => self.output_adjustment_stats.borrow().metadata(),
            STEP_DURATION_HISTOGRAM => self.step_durations.borrow().metadata(),
            CONFLICTING_UPDATES_COUNT => MetaItem::Count(self.conflicting_updates.get() as usize),
        });

        // The accumulator exists only while a transaction commits, and is out of
        // the cell while a chunk of it is being inserted.
        let state = self.state.borrow();
        let Some(state) = state.as_ref() else {
            return;
        };

        let bytes = state.size_of();
        meta.extend(metadata! {
            STATE_RECORDS_COUNT => MetaItem::Count(state.num_entries_deep()),
            ALLOCATED_MEMORY_BYTES => MetaItem::bytes(bytes.total_bytes()),
            USED_MEMORY_BYTES => MetaItem::bytes(bytes.used_bytes()),
            MEMORY_ALLOCATIONS_COUNT => MetaItem::Count(bytes.distinct_allocations()),
            SHARED_MEMORY_BYTES => MetaItem::bytes(bytes.shared_bytes()),
        });
        state.metadata(meta);
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }

    fn clear_state(&mut self) -> Result<(), Error> {
        *self.state.borrow_mut() = None;
        Ok(())
    }
}

impl<K, V> LazyUpsert<K, V, OrdIndexedZSet<K, V>>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
{
    fn builder(&self, capacity: usize) -> <OrdIndexedZSet<K, V> as crate::trace::Batch>::Builder {
        <OrdIndexedZSet<K, V> as crate::trace::Batch>::Builder::with_capacity(
            &self.factories.batch_factories,
            capacity,
            capacity,
        )
    }

    /// Records an update that collided with another at its key's stamp, and
    /// says so once, since the count alone does not say where to look.
    fn count_conflict(&self) {
        let seen = self.conflicting_updates.get();
        if seen == 0 {
            warn!(
                "{}: a key was written by more than one host in one transaction. \
                 The update whose value sorts first wins, the same way on every \
                 replay. `{CONFLICTING_UPDATES_COUNT}` counts how often this happens.",
                self.name.get()
            );
        }
        self.conflicting_updates.set(seen + 1);
    }

    /// Hands one chunk of adjustments to the delta stream and to the
    /// accumulator, which are the two places every adjustment has to reach.
    async fn emit(&self, batch: Arc<OrdIndexedZSet<K, V>>) {
        self.output_adjustment_stats
            .borrow_mut()
            .add_batch(batch.approx_len());
        self.adjustments.put(Arc::clone(&batch));

        // Taken out for the insert rather than borrowed across it, so no borrow
        // of the accumulator outlives a call that can suspend.
        let mut accumulator = self
            .state
            .borrow_mut()
            .take()
            .expect("the accumulator outlives the step that fills it");
        accumulator.insert(batch).await;
        *self.state.borrow_mut() = Some(accumulator);
    }
}

impl<K, V>
    StreamingBinaryOperator<
        Spine<OrdIndexedZSet<K, V>>,
        Option<Spine<Stamped<OrdIndexedZSet<K, V>>>>,
        Option<Spine<OrdIndexedZSet<K, V>>>,
    > for LazyUpsert<K, V, OrdIndexedZSet<K, V>>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
{
    fn eval(
        self: Rc<Self>,
        integral: &Spine<OrdIndexedZSet<K, V>>,
        updates: &Option<Spine<Stamped<OrdIndexedZSet<K, V>>>>,
    ) -> impl AsyncStream<Item = (Option<Spine<OrdIndexedZSet<K, V>>>, bool, Option<Position>)> + 'static
    {
        // The stream outlives this call, so everything it reads is taken now:
        // the updates as batches for the accumulator and as a snapshot to walk,
        // and the integral as a snapshot to probe.
        let stamped = updates.as_ref().map(|updates| updates.get_batches());
        let updates = updates.as_ref().map(|updates| updates.ro_snapshot());
        let integral = updates.is_some().then(|| integral.ro_snapshot());

        stream! {
            // The accumulator delivers its spine when the transaction commits
            // and nothing in between, so this is the only step with work to do.
            let Some(updates) = updates else {
                self.adjustments.put(Arc::new(self.builder(0).done()));
                yield (None, true, None);
                return;
            };
            let integral = integral.unwrap();

            let factories = &self.factories.batch_factories;

            // The accumulator starts as the transaction's updates with their
            // stamps dropped; the adjustments join it as they are resolved.
            *self.state.borrow_mut() = Some(
                remove_stamp(
                    factories,
                    stamped.unwrap(),
                    Arc::new(String::from("lazy_input_upsert.accumulator")),
                )
                .await,
            );

            let chunk_size = splitter_output_chunk_size();
            let keys_per_step = lazy_input_map_keys_per_step();
            let capacity = splitter_output_first_chunk_size();

            let mut updates_cursor = updates.cursor();
            let mut integral_cursor = integral.cursor();

            // One key's adjustments, consolidated before they reach the builder:
            // they arrive in no particular value order, and a builder takes
            // values in order and once each.
            let mut key_adjustments = factories.weighted_vals_factory().default_box();

            let mut key = factories.key_factory().default_box();
            let mut max_val = factories.val_factory().default_box();

            let mut builder = self.builder(capacity);
            let mut in_chunk = 0usize;
            let mut walked = 0usize;

            // One clock read per step rather than per key: the loop below runs
            // millions of times and a step ends only at a yield.
            let mut step_start = Instant::now();

            while updates_cursor.key_valid() {
                updates_cursor.key().clone_to(&mut key);
                key_adjustments.clear();

                // The key is being written, so whatever the integral holds for
                // it goes.  The integral is a map this operator alone writes, so
                // a key it holds has one value and that value has weight one.
                if integral_cursor.seek_key_exact(&key, None) {
                    debug_assert!(integral_cursor.val_valid());
                    assert_eq!(
                        **integral_cursor.weight(),
                        1,
                        "the integral holds a key at a weight other than one"
                    );
                    let val = integral_cursor.val();
                    key_adjustments.push_with(&mut |item| {
                        let (v, w) = item.split_mut();
                        val.clone_to(v);
                        **w = -1;
                    });
                    integral_cursor.step_val();
                    debug_assert!(
                        !integral_cursor.val_valid(),
                        "the integral holds a key at more than one value"
                    );
                }

                // Only the largest stamp survives.  The cursor walks values in
                // value order rather than stamp order, so the running maximum is
                // retracted whenever a later stamp displaces it.
                let mut max_stamp: Option<u32> = None;
                let mut max_weight: ZWeight = 0;

                while updates_cursor.val_valid() {
                    // The weight is read first: it needs the cursor mutably, and
                    // the value borrows it for the rest of the iteration.
                    let weight = **updates_cursor.weight();
                    let (val, stamp) = updates_cursor.val().split();
                    let stamp = *unsafe { stamp.downcast::<u32>() };

                    // Two hosts ingesting one key in a transaction give it two
                    // updates at the same stamp, since each host stamps its own
                    // steps.  The cursor walks values in value order, so the one
                    // whose value sorts first wins: arbitrary, but the same on
                    // every replay of the same input.
                    if max_stamp.is_some_and(|max| stamp == max) {
                        self.count_conflict();
                    }

                    if max_stamp.is_none_or(|max| stamp > max) {
                        if max_stamp.is_some() {
                            // The displaced value is about to be overwritten,
                            // so it moves out rather than being copied.
                            key_adjustments.push_with(&mut |item| {
                                let (v, w) = item.split_mut();
                                max_val.move_to(v);
                                **w = -max_weight;
                            });
                        }
                        max_stamp = Some(stamp);
                        val.clone_to(&mut max_val);
                        max_weight = weight;
                    } else {
                        key_adjustments.push_with(&mut |item| {
                            let (v, w) = item.split_mut();
                            val.clone_to(v);
                            **w = -weight;
                        });
                    }

                    updates_cursor.step_val();
                }

                // A surviving delete cancels the record `project(U)` contributes
                // for it, which is what leaves the key with nothing.
                if max_weight < 0 {
                    // The key is done, so this value moves out too.
                    key_adjustments.push_with(&mut |item| {
                        let (v, w) = item.split_mut();
                        max_val.move_to(v);
                        **w = -max_weight;
                    });
                }

                key_adjustments.consolidate();
                if !key_adjustments.is_empty() {
                    in_chunk += key_adjustments.len();
                    for pair in key_adjustments.dyn_iter_mut() {
                        let (v, w) = pair.split_mut();
                        builder.push_val_diff_mut(v, w);
                    }
                    builder.push_key(&key);
                }

                updates_cursor.step_key();
                walked += 1;

                // A step ends on a chunk of adjustments, or on a run of keys
                // that produced too few of them to end it that way.  Either way
                // it ends at a key boundary, which is where a builder can be
                // finished.
                if in_chunk >= chunk_size || walked >= keys_per_step {
                    let position = updates_cursor.position();
                    self.emit(Arc::new(builder.done())).await;
                    self.step_durations.borrow_mut().add(step_start.elapsed());
                    yield (None, false, position);
                    step_start = Instant::now();
                    builder = self.builder(capacity);
                    in_chunk = 0;
                    walked = 0;
                }
            }

            let position = updates_cursor.position();
            self.emit(Arc::new(builder.done())).await;
            self.step_durations.borrow_mut().add(step_start.elapsed());
            let accumulator = self.state.borrow_mut().take();
            yield (accumulator, true, position);
        }
    }
}

impl RootCircuit {
    /// [`dyn_add_lazy_input_map`](Self::dyn_add_lazy_input_map) with its type
    /// parameters pinned to the only types the typed wrapper instantiates it
    /// with.
    ///
    /// Going through this monomorphizes the operator once here rather than
    /// again for every `K`, `V` and `U` a caller names, which is what the
    /// generic form would otherwise cost at each call site.
    pub fn dyn_add_lazy_input_map_mono(
        &self,
        persistent_id: Option<&str>,
        factories: &AddLazyInputMapFactories<OrdIndexedZSet<DynData, DynData>>,
    ) -> (
        IndexedZSetStream<DynData, DynData>,
        UpsertHandle<DynData, DynOpt<DynData>>,
    ) {
        self.dyn_add_lazy_input_map(persistent_id, factories)
    }

    /// An input map that resolves its updates when the transaction commits.
    ///
    /// Same interface and semantics as
    /// [`dyn_add_input_map`](RootCircuit::dyn_add_input_map), minus `Update`
    /// commands, but the integral is read once per transaction in key order
    /// rather than once per key per step at random.
    ///
    /// ```text
    ///                                          ┌──────────── integral ───────────┐
    ///                                          ▼                                 │
    ///  updates ──► Stamp ──┬──► shard_accumulate ──► LazyUpsert ──┬──► integrate ─┘
    ///                      │                            │         │
    ///                      │                      adjustments   accumulator
    ///                      │                            │
    ///                      └────────► project ──► concat ──► delta
    /// ```
    ///
    /// `Stamp` gives each update the step it arrived in; the accumulator gathers
    /// the transaction's updates; `LazyUpsert` resolves them against the
    /// integral in one scan and emits both the accumulator that feeds the
    /// integral and the adjustments that, with the projected updates, make the
    /// delta.
    #[track_caller]
    pub fn dyn_add_lazy_input_map<K, V>(
        &self,
        persistent_id: Option<&str>,
        factories: &AddLazyInputMapFactories<OrdIndexedZSet<K, V>>,
    ) -> (IndexedZSetStream<K, V>, UpsertHandle<K, DynOpt<V>>)
    where
        K: DataTrait + ?Sized,
        V: DataTrait + ?Sized,
    {
        self.region("lazy_input_map", || {
            let (input, input_handle) = Input::new(
                Location::caller(),
                |tuples: Vec<Box<DynPairs<K, DynOpt<V>>>>| tuples,
                Arc::new(|| vec![factories.input_pairs_factory.default_box()]),
            );
            let input_stream = self.add_source(input);
            let zset_handle = <UpsertHandle<K, DynOpt<V>>>::new(
                factories.input_pair_factory,
                factories.input_pairs_factory,
                input_handle,
            );

            // No exchange.  `UpsertHandle` hashes each key to a worker as the
            // client appends, so a key's updates all reach one worker on their
            // host, which is everything `Stamp` needs to order them.
            let stamped = self.add_unary_operator(
                <Stamp<K, V, OrdIndexedZSet<K, V>>>::new(factories),
                &input_stream,
            );

            // On one host those local shards are the global ones, so say so and
            // `dyn_shard_accumulate` moves nothing.  Across hosts they are not,
            // and leaving the stream unmarked is what lets the accumulator do
            // the sharding: it exchanges a transaction's batches rather than
            // every step's loose pairs.
            if Runtime::runtime().is_none_or(|runtime| runtime.layout().is_solo()) {
                stamped.mark_sharded();
            }

            let accumulated = stamped
                .dyn_shard_accumulate(&factories.stamped_factories)
                .into_enabled_stream();

            let bounds = <TraceBounds<K, V>>::unbounded();
            let (delayed_integral, z1feedback) =
                self.add_feedback_persistent(
                    persistent_id
                        .map(|name| format!("{name}.accintegral"))
                        .as_deref(),
                    <AccumulateZ1Trace<
                        RootCircuit,
                        OrdIndexedZSet<K, V>,
                        Spine<OrdIndexedZSet<K, V>>,
                    >>::new(
                        &factories.batch_factories,
                        &factories.batch_factories,
                        false,
                        self.root_scope(),
                        bounds.clone(),
                    ),
                );
            delayed_integral.mark_sharded();

            let adjustments_value = RefStreamValue::empty();
            let accumulator = self.add_binary_operator(
                StreamingBinaryWrapper::new(<LazyUpsert<K, V, OrdIndexedZSet<K, V>>>::new(
                    factories,
                    adjustments_value.clone(),
                )),
                &delayed_integral,
                &accumulated,
            );
            accumulator.mark_sharded();

            let integral = self.add_binary_operator_with_preference(
                AccumulateUntimedTraceAppend::<Spine<OrdIndexedZSet<K, V>>>::new(),
                (
                    &delayed_integral,
                    OwnershipPreference::STRONGLY_PREFER_OWNED,
                ),
                (&accumulator, OwnershipPreference::PREFER_OWNED),
            );
            integral.mark_sharded();
            z1feedback
                .connect_with_preference(&integral, OwnershipPreference::STRONGLY_PREFER_OWNED);

            let adjustments =
                Stream::with_value(self.clone(), accumulator.local_node_id(), adjustments_value);

            // `project` and `concat` in one step: the projection is free, since
            // a stamped batch can present itself as an unstamped one, so a step
            // whose adjustments are empty -- every step but a transaction's last
            // -- hands the projected batch straight through without copying it.
            let batch_factories = factories.batch_factories.clone();
            let delta = stamped.apply2(&adjustments, move |stamped, adjustments| {
                let projected = OrdIndexedZSet::project_batch(&batch_factories, stamped);
                if adjustments.is_empty() {
                    projected
                } else if projected.is_empty() {
                    (**adjustments).clone()
                } else {
                    merge_batches_by_reference(
                        &batch_factories,
                        [&projected, &**adjustments],
                        &None,
                        &None,
                    )
                }
            });
            let enable_count = EnableCount::new();
            enable_count.enable();

            // The updates reach the map sharded within a host only, and the
            // adjustments come back from the accumulator sharded across all of
            // them, so the two halves of the delta agree on one host and on no
            // more than that.
            if Runtime::runtime().is_none_or(|runtime| runtime.layout().is_solo()) {
                delta.mark_sharded();

                self.cache_insert(
                    AccumulatorId::new(delta.stream_id()),
                    Accumulation {
                        stream: accumulator,
                        enable_count,
                    },
                );
                self.cache_insert(AccumulateTraceId::new(delta.stream_id()), integral);
            } else {
                self.cache_insert(
                    ShardedAccumulatorId::new((delta.stream_id(), 0..Runtime::num_workers())),
                    Accumulation {
                        stream: accumulator,
                        enable_count,
                    },
                );
                self.cache_insert(ShardedAccumulateTraceId::new(delta.stream_id()), integral);
            }

            (delta, zset_handle)
        })
    }
}

#[cfg(test)]
mod stamp_tests {
    use super::*;
    use crate::dynamic::LeanVec;
    use crate::trace::{Cursor, test::run_in_circuit_with_storage};
    use feldera_storage::tokio::TOKIO;

    type Key = DynData;
    type Val = DynData;
    type Batched = OrdIndexedZSet<Key, Val>;

    /// `Some(v)` is a write, `None` a delete.
    type Command = (i32, Option<i32>);

    fn factories() -> AddLazyInputMapFactories<Batched> {
        AddLazyInputMapFactories::new::<i32, i32>()
    }

    /// One worker's commands for a step, in the order the client wrote them.
    /// The operator sorts them, so they need no order here.
    fn commands(commands: &[Command]) -> Box<DynPairs<Key, DynOpt<Val>>> {
        let pairs: Vec<Tup2<i32, Option<i32>>> = commands
            .iter()
            .map(|&(key, value)| Tup2(key, value))
            .collect();
        Box::new(LeanVec::from(pairs)).erase_box()
    }

    /// `(key, value, stamp, weight)` for every record a step emits.
    fn step(
        operator: &mut Stamp<Key, Val, Batched>,
        vectors: &[&[Command]],
    ) -> Vec<(i32, i32, u32, ZWeight)> {
        let input: Vec<Box<DynPairs<Key, DynOpt<Val>>>> =
            vectors.iter().map(|v| commands(v)).collect();
        let batch = TOKIO.block_on(operator.eval_owned(input));

        let mut out = Vec::new();
        let mut cursor = batch.cursor();
        while cursor.key_valid() {
            while cursor.val_valid() {
                let Tup2(value, stamp) = *unsafe { cursor.val().downcast::<Tup2<i32, u32>>() };
                out.push((
                    *unsafe { cursor.key().downcast::<i32>() },
                    value,
                    stamp,
                    **cursor.weight(),
                ));
                cursor.step_val();
            }
            cursor.step_key();
        }
        out
    }

    /// An insert becomes a `+1` record and a delete a `-1` record, both carrying
    /// the step's stamp.
    #[test]
    fn an_insert_and_a_delete_become_one_record_each() {
        run_in_circuit_with_storage(|| {
            let factories = factories();
            let mut operator = Stamp::new(&factories);
            let records = step(&mut operator, &[&[(1, Some(10)), (2, None)]]);

            assert_eq!(records.len(), 2, "one record per key: {records:?}");
            assert_eq!(records[0], (1, 10, 0, 1));
            // The delete's value is a default and carries no meaning; only its
            // key, stamp and weight do.
            let (key, _value, stamp, weight) = records[1];
            assert_eq!((key, stamp, weight), (2, 0, -1));
        });
    }

    /// The map a set of vectors describes: the last update to a key across all
    /// of them wins, and a delete travels as the value type's default.
    fn merged(vectors: &[&[Command]]) -> Vec<(i32, i32, u32, ZWeight)> {
        let mut map: std::collections::BTreeMap<i32, (i32, ZWeight)> =
            std::collections::BTreeMap::new();
        for vector in vectors {
            for &(key, value) in vector.iter() {
                map.insert(key, value.map_or((0, -1), |value| (value, 1)));
            }
        }
        map.into_iter()
            .map(|(key, (value, weight))| (key, value, 0, weight))
            .collect()
    }

    /// Checks the merge against the model on vectors written out by hand.
    fn merges_to_the_model(vectors: &[&[Command]]) {
        let factories = factories();
        let mut operator = Stamp::new(&factories);
        assert_eq!(
            step(&mut operator, vectors),
            merged(vectors),
            "the merge disagrees with the model on {vectors:?}"
        );
    }

    /// A key written by every vector takes the last vector's update.
    #[test]
    fn every_vector_writes_one_key() {
        run_in_circuit_with_storage(|| {
            merges_to_the_model(&[&[(1, Some(10))], &[(1, Some(20))], &[(1, Some(30))]]);
            merges_to_the_model(&[&[(1, Some(10))], &[(1, None)], &[(1, Some(30))]]);
            merges_to_the_model(&[&[(1, Some(10))], &[(1, Some(20))], &[(1, None)]]);
            merges_to_the_model(&[&[(1, None)], &[(1, None)], &[(1, Some(30))]]);
        });
    }

    /// A key skipped by a middle vector still takes the last update written to
    /// it, not the one from the vector before the gap.
    #[test]
    fn a_key_skips_a_vector() {
        run_in_circuit_with_storage(|| {
            merges_to_the_model(&[&[(1, Some(10))], &[(2, Some(20))], &[(1, Some(30))]]);
            merges_to_the_model(&[
                &[(1, Some(10)), (2, Some(11))],
                &[(2, Some(20))],
                &[(1, Some(30))],
            ]);
        });
    }

    /// Vectors of different lengths, so the merge runs out of one while others
    /// still have keys.
    ///
    /// Exhausting a source drops it from the list the merge scans, which shifts
    /// the positions of every source after it.
    #[test]
    fn a_vector_runs_out_before_the_others() {
        run_in_circuit_with_storage(|| {
            // The first source empties first, then the second.
            merges_to_the_model(&[
                &[(1, Some(10))],
                &[(1, Some(20)), (2, Some(21))],
                &[(1, Some(30)), (2, Some(31)), (3, Some(32))],
            ]);
            // The last source empties first.
            merges_to_the_model(&[
                &[(1, Some(10)), (2, Some(11)), (3, Some(12))],
                &[(1, Some(20)), (2, Some(21))],
                &[(1, Some(30))],
            ]);
            // A middle source empties first, so the survivors close the gap it
            // leaves behind.
            merges_to_the_model(&[
                &[(1, Some(10)), (5, Some(11))],
                &[(1, Some(20))],
                &[(1, Some(30)), (5, Some(31))],
            ]);
        });
    }

    /// Keys interleaved across vectors, so the merge alternates sources.
    #[test]
    fn keys_interleave_across_vectors() {
        run_in_circuit_with_storage(|| {
            merges_to_the_model(&[
                &[(1, Some(10)), (4, Some(13))],
                &[(2, Some(21)), (3, Some(22))],
            ]);
            merges_to_the_model(&[
                &[(1, Some(10)), (3, Some(12))],
                &[(2, Some(21)), (4, Some(23))],
                &[(1, Some(30)), (2, Some(31)), (3, Some(32)), (4, Some(33))],
            ]);
        });
    }

    /// Empty vectors among the others, which the merge drops before it starts.
    #[test]
    fn empty_vectors_among_the_others() {
        run_in_circuit_with_storage(|| {
            merges_to_the_model(&[&[], &[(1, Some(10))], &[]]);
            merges_to_the_model(&[&[], &[], &[]]);
            merges_to_the_model(&[&[(1, Some(10))], &[], &[(1, Some(20))]]);
        });
    }

    /// More vectors than a client would send, all writing the same two keys.
    #[test]
    fn many_vectors_write_the_same_keys() {
        run_in_circuit_with_storage(|| {
            let vectors: Vec<Vec<Command>> = (0..16)
                .map(|round| vec![(1, Some(round)), (2, Some(100 + round))])
                .collect();
            let borrowed: Vec<&[Command]> = vectors.iter().map(|v| v.as_slice()).collect();
            merges_to_the_model(&borrowed);
        });
    }

    /// Vectors as `Stamp` requires them: sorted by key, each key once.
    ///
    /// Keys are drawn from a small range so that vectors overlap heavily, which
    /// is what makes the merge choose between them.
    fn a_vector() -> impl proptest::strategy::Strategy<Value = Vec<Command>> {
        use proptest::prelude::*;
        proptest::collection::btree_map(
            0i32..6,
            prop_oneof![Just(None), (0i32..4).prop_map(Some)],
            0..6,
        )
        .prop_map(|map| map.into_iter().collect())
    }

    proptest::proptest! {
        #![proptest_config(proptest::test_runner::Config::with_cases(256))]

        /// The merge keeps the last update to each key across every vector.
        #[test]
        fn a_merge_of_random_vectors_agrees_with_the_model(
            vectors in proptest::collection::vec(a_vector(), 0..6)
        ) {
            run_in_circuit_with_storage(move || {
                let borrowed: Vec<&[Command]> = vectors.iter().map(|v| v.as_slice()).collect();
                merges_to_the_model(&borrowed);
            });
        }
    }

    /// The operator sorts what it is handed, so a client can append in any order
    /// and write a key as often as it likes.
    ///
    /// Two updates to one key within a step share a stamp, which cannot tell
    /// them apart, so the order the client wrote them in is what decides: the
    /// last one wins.
    #[test]
    fn a_vector_is_sorted_before_the_merge() {
        run_in_circuit_with_storage(|| {
            let factories = factories();

            // Out of key order, with key 1 written three times.
            let mut operator = Stamp::new(&factories);
            assert_eq!(
                step(
                    &mut operator,
                    &[&[
                        (3, Some(30)),
                        (1, Some(10)),
                        (2, Some(20)),
                        (1, Some(11)),
                        (1, Some(12)),
                    ]]
                ),
                vec![(1, 12, 0, 1), (2, 20, 0, 1), (3, 30, 0, 1)]
            );

            // A delete after inserts is the write that survives.
            let mut operator = Stamp::new(&factories);
            let records = step(&mut operator, &[&[(1, Some(10)), (1, None)]]);
            assert_eq!(records.len(), 1);
            let (key, _value, stamp, weight) = records[0];
            assert_eq!((key, stamp, weight), (1, 0, -1));

            // An insert after a delete likewise.
            let mut operator = Stamp::new(&factories);
            assert_eq!(
                step(&mut operator, &[&[(1, None), (1, Some(10))]]),
                vec![(1, 10, 0, 1)]
            );

            // An empty vector drops out of the merge.
            let mut operator = Stamp::new(&factories);
            assert_eq!(
                step(&mut operator, &[&[], &[(1, Some(10))]]),
                vec![(1, 10, 0, 1)]
            );
        });
    }

    /// Only the last update for a key survives a step.
    ///
    /// Each vector holds a key once, so the earlier updates are in earlier
    /// vectors, and the merge is what has to put them in order.
    #[test]
    fn the_last_update_for_a_key_wins() {
        run_in_circuit_with_storage(|| {
            let factories = factories();

            // The later vector's update is the later arrival.
            let mut operator = Stamp::new(&factories);
            let records = step(&mut operator, &[&[(1, Some(10))], &[(1, Some(20))]]);
            assert_eq!(records, vec![(1, 20, 0, 1)]);

            // A delete after an insert leaves the delete.
            let mut operator = Stamp::new(&factories);
            let records = step(&mut operator, &[&[(1, Some(10))], &[(1, None)]]);
            assert_eq!(records.len(), 1);
            let (key, _value, stamp, weight) = records[0];
            assert_eq!((key, stamp, weight), (1, 0, -1));
        });
    }

    /// The stamp counts steps within a transaction and starts over with each
    /// transaction, so it orders a key's updates across the transaction and
    /// nothing beyond it.
    #[test]
    fn the_stamp_counts_steps_and_resets_per_transaction() {
        run_in_circuit_with_storage(|| {
            let factories = factories();
            let mut operator = Stamp::new(&factories);

            assert_eq!(
                step(&mut operator, &[&[(1, Some(10))]]),
                vec![(1, 10, 0, 1)]
            );
            assert_eq!(
                step(&mut operator, &[&[(1, Some(11))]]),
                vec![(1, 11, 1, 1)]
            );
            assert_eq!(
                step(&mut operator, &[&[(1, Some(12))]]),
                vec![(1, 12, 2, 1)]
            );

            operator.start_transaction();
            assert_eq!(
                step(&mut operator, &[&[(1, Some(13))]]),
                vec![(1, 13, 0, 1)]
            );
        });
    }

    /// A step with nothing in it emits nothing.
    #[test]
    fn an_empty_step_emits_nothing() {
        run_in_circuit_with_storage(|| {
            let factories = factories();
            let mut operator = Stamp::new(&factories);
            assert_eq!(step(&mut operator, &[]), vec![]);
            assert_eq!(step(&mut operator, &[&[]]), vec![]);
        });
    }

    /// A transaction longer than the stamp can count is refused rather than
    /// wrapped.
    ///
    /// Wrapping would hand a later step a smaller stamp than an earlier one, and
    /// the resolution keeps the largest, so the transaction would silently take
    /// an update the client had already replaced.
    #[test]
    #[should_panic(expected = "more steps than a stamp can count")]
    fn a_transaction_longer_than_the_stamp_can_count_is_refused() {
        run_in_circuit_with_storage(|| {
            let factories = factories();
            let mut operator = Stamp::new(&factories);
            operator.stamp = u32::MAX;
            let _ = TOKIO.block_on(operator.eval_owned(Vec::new()));
        });
    }
}

#[cfg(test)]
mod remove_stamp_tests {
    use super::*;
    use crate::dynamic::LeanVec;
    use crate::trace::{Batch, BatchLocation, Cursor, test::run_in_circuit_with_storage};
    use feldera_storage::tokio::TOKIO;

    type Key = DynData;
    type Val = DynData;
    type Batched = OrdIndexedZSet<Key, Val>;

    /// `(key, value, stamp, weight)`.
    type Row = (i32, i32, u32, ZWeight);

    fn factories() -> AddLazyInputMapFactories<Batched> {
        AddLazyInputMapFactories::new::<i32, i32>()
    }

    fn stamped_batch(
        factories: &AddLazyInputMapFactories<Batched>,
        rows: &[Row],
    ) -> Stamped<Batched> {
        let tuples: Vec<Tup2<Tup2<i32, Tup2<i32, u32>>, ZWeight>> = rows
            .iter()
            .map(|&(key, value, stamp, weight)| Tup2(Tup2(key, Tup2(value, stamp)), weight))
            .collect();
        let mut tuples = Box::new(LeanVec::from(tuples)).erase_box();
        <Stamped<Batched> as crate::trace::Batch>::dyn_from_tuples(
            &factories.stamped_factories,
            (),
            &mut tuples,
        )
    }

    /// Every `(key, value, weight)` a batch or trace exposes.
    fn contents<B>(batch: &B) -> Vec<(i32, i32, ZWeight)>
    where
        B: BatchReader<Key = Key, Val = Val, Time = (), R = crate::DynZWeight>,
    {
        let mut out = Vec::new();
        let mut cursor = batch.cursor();
        while cursor.key_valid() {
            while cursor.val_valid() {
                out.push((
                    *unsafe { cursor.key().downcast::<i32>() },
                    *unsafe { cursor.val().downcast::<i32>() },
                    **cursor.weight(),
                ));
                cursor.step_val();
            }
            cursor.step_key();
        }
        out
    }

    /// A stamped batch presents itself as an unstamped one: the stamp is
    /// invisible, records differing only in it consolidate, and a record left
    /// with no weight disappears along with a key left with nothing.
    #[test]
    fn a_stamped_batch_projects_to_its_values() {
        run_in_circuit_with_storage(|| {
            let factories = factories();
            let rows: &[Row] = &[
                (1, 10, 0, 1),
                (1, 10, 3, 2),
                (1, 20, 1, 1),
                (2, 30, 0, 1),
                (2, 30, 4, -1),
            ];
            let projected = Batched::project_batch(
                &factories.batch_factories,
                &stamped_batch(&factories, rows),
            );
            assert_eq!(contents(&projected), vec![(1, 10, 3), (1, 20, 1)]);
        });
    }

    /// A stamped batch that has spilled projects the same way.
    #[test]
    fn a_spilled_stamped_batch_projects_the_same() {
        run_in_circuit_with_storage(|| {
            let factories = factories();
            let rows: &[Row] = &[(1, 10, 0, 1), (1, 10, 3, 2), (1, 20, 1, 1)];

            let stamped = stamped_batch(&factories, rows);
            let spilled = stamped
                .persisted()
                .expect("an in-memory batch persists to storage");
            assert_eq!(spilled.location(), BatchLocation::Storage);

            let projected = Batched::project_batch(&factories.batch_factories, &spilled);
            assert_eq!(contents(&projected), vec![(1, 10, 3), (1, 20, 1)]);
        });
    }

    /// A spine of stamped batches reads as a spine of unstamped ones, across
    /// batch boundaries: a value split over two batches consolidates once the
    /// spine merges them.
    #[test]
    fn a_stamped_spine_reads_as_an_unstamped_one() {
        run_in_circuit_with_storage(|| {
            let factories = factories();

            let mut stamped: Spine<Stamped<Batched>> = <Spine<_> as Trace>::new(
                &factories.stamped_factories,
                Arc::new(String::from("stamped")),
                TraceRole::Integral,
            );
            TOKIO.block_on(
                stamped.insert(stamped_batch(&factories, &[(1, 10, 0, 1), (2, 30, 0, 1)])),
            );
            TOKIO.block_on(
                stamped.insert(stamped_batch(&factories, &[(1, 10, 1, 2), (2, 30, 1, -1)])),
            );

            let unstamped = TOKIO.block_on(remove_stamp(
                &factories.batch_factories,
                stamped.get_batches(),
                Arc::new(String::from("unstamped")),
            ));

            // Key 1 keeps value 10 with the summed weight; key 2's two stamps
            // cancel, so both the value and the key are gone.
            assert_eq!(contents(&unstamped), vec![(1, 10, 3)]);
        });
    }
}

#[cfg(test)]
mod lazy_upsert_tests {
    use super::*;
    use crate::circuit::{CircuitConfig, mkconfig};
    use crate::dynamic::LeanVec;
    use crate::trace::{
        Batch, Cursor,
        test::{run_in_circuit_with_storage, run_in_circuit_with_storage_config},
    };
    use feldera_storage::tokio::TOKIO;
    use futures_util::StreamExt;
    use tempfile::tempdir;

    type Key = DynData;
    type Val = DynData;
    type Batched = OrdIndexedZSet<Key, Val>;
    type Factories = AddLazyInputMapFactories<Batched>;

    fn factories() -> Factories {
        AddLazyInputMapFactories::new::<i32, i32>()
    }

    /// The collection as it stood when the transaction began.
    fn integral(factories: &Factories, rows: &[(i32, i32, ZWeight)]) -> Spine<Batched> {
        let tuples: Vec<Tup2<Tup2<i32, i32>, ZWeight>> = rows
            .iter()
            .map(|&(key, value, weight)| Tup2(Tup2(key, value), weight))
            .collect();
        let mut tuples = Box::new(LeanVec::from(tuples)).erase_box();
        let batch = Batched::dyn_from_tuples(&factories.batch_factories, (), &mut tuples);

        let mut spine = <Spine<Batched> as Trace>::new(
            &factories.batch_factories,
            Arc::new("I".to_string()),
            TraceRole::Integral,
        );
        TOKIO.block_on(spine.insert(batch));
        spine
    }

    /// Everything the transaction did, stamped with the step it happened in.
    fn updates(
        factories: &Factories,
        rows: &[(i32, i32, u32, ZWeight)],
    ) -> Spine<Stamped<Batched>> {
        let tuples: Vec<Tup2<Tup2<i32, Tup2<i32, u32>>, ZWeight>> = rows
            .iter()
            .map(|&(key, value, stamp, weight)| Tup2(Tup2(key, Tup2(value, stamp)), weight))
            .collect();
        let mut tuples = Box::new(LeanVec::from(tuples)).erase_box();
        let batch =
            Stamped::<Batched>::dyn_from_tuples(&factories.stamped_factories, (), &mut tuples);

        let mut spine = <Spine<Stamped<Batched>> as Trace>::new(
            &factories.stamped_factories,
            Arc::new("U".to_string()),
            TraceRole::Integral,
        );
        TOKIO.block_on(spine.insert(batch));
        spine
    }

    fn contents<B>(batch: &B) -> Vec<(i32, i32, ZWeight)>
    where
        B: BatchReader<Key = Key, Val = Val, Time = (), R = crate::DynZWeight>,
    {
        let mut out = Vec::new();
        let mut cursor = batch.cursor();
        while cursor.key_valid() {
            while cursor.val_valid() {
                out.push((
                    *unsafe { cursor.key().downcast::<i32>() },
                    *unsafe { cursor.val().downcast::<i32>() },
                    **cursor.weight(),
                ));
                cursor.step_val();
            }
            cursor.step_key();
        }
        out
    }

    /// What one call to the operator produced.
    ///
    /// The operator spreads its work over as many steps as the chunking asks
    /// for, so how many it took is part of what a test can check.  The
    /// adjustments themselves are not: a stream value with no consumer attached
    /// drops what is put on it, so only a circuit can observe them.
    struct Resolution {
        steps: usize,
        accumulator: Option<Vec<(i32, i32, ZWeight)>>,
        conflicts: u64,
    }

    /// Runs the operator to completion, one yield at a time, as the circuit
    /// would over as many steps.
    fn run(
        factories: &Factories,
        integral_rows: &[(i32, i32, ZWeight)],
        update_rows: Option<&[(i32, i32, u32, ZWeight)]>,
    ) -> Resolution {
        let operator = Rc::new(<LazyUpsert<Key, Val, Batched>>::new(
            factories,
            RefStreamValue::empty(),
        ));
        let integral = integral(factories, integral_rows);
        let updates = update_rows.map(|rows| updates(factories, rows));

        let mut steps = 0;
        let mut accumulator = None;
        TOKIO.block_on(async {
            let mut stream: std::pin::Pin<
                Box<dyn AsyncStream<Item = (Option<Spine<Batched>>, bool, Option<Position>)>>,
            > = Box::pin(Rc::clone(&operator).eval(&integral, &updates));
            loop {
                let (output, complete, _position) = stream
                    .next()
                    .await
                    .expect("the operator yields until it reports completion");
                steps += 1;
                if let Some(output) = output {
                    accumulator = Some(contents(&output));
                }
                if complete {
                    break;
                }
            }
        });
        Resolution {
            steps,
            accumulator,
            conflicts: operator.conflicting_updates.get(),
        }
    }

    /// The accumulator the operator produces: `project(U)` with the adjustments
    /// added, which is the delta the transaction applies to the integral.
    fn resolve(
        factories: &Factories,
        integral_rows: &[(i32, i32, ZWeight)],
        update_rows: &[(i32, i32, u32, ZWeight)],
    ) -> Vec<(i32, i32, ZWeight)> {
        run(factories, integral_rows, Some(update_rows))
            .accumulator
            .expect("a delivered accumulation produces an accumulator")
    }

    /// A key the integral does not hold takes the inserted value, with nothing
    /// to retract.
    #[test]
    fn a_new_key_is_inserted() {
        run_in_circuit_with_storage(|| {
            let factories = factories();
            assert_eq!(resolve(&factories, &[], &[(1, 10, 0, 1)]), vec![(1, 10, 1)]);
        });
    }

    /// Writing a key the integral holds retracts the old value and inserts the
    /// new one.
    #[test]
    fn an_existing_key_is_overwritten() {
        run_in_circuit_with_storage(|| {
            let factories = factories();
            assert_eq!(
                resolve(&factories, &[(1, 7, 1)], &[(1, 10, 0, 1)]),
                vec![(1, 7, -1), (1, 10, 1)]
            );
        });
    }

    /// Deleting a key the integral holds leaves only the retraction: the record
    /// the delete contributed cancels against the adjustment for it.
    #[test]
    fn deleting_an_existing_key_leaves_only_the_retraction() {
        run_in_circuit_with_storage(|| {
            let factories = factories();
            // The value a delete carries is a default and means nothing; here it
            // is 0, and it must not appear in the result.
            assert_eq!(
                resolve(&factories, &[(1, 7, 1)], &[(1, 0, 0, -1)]),
                vec![(1, 7, -1)]
            );
        });
    }

    /// Deleting a key the integral does not hold changes nothing.
    #[test]
    fn deleting_an_absent_key_changes_nothing() {
        run_in_circuit_with_storage(|| {
            let factories = factories();
            assert_eq!(resolve(&factories, &[], &[(1, 0, 0, -1)]), vec![]);
        });
    }

    /// Two updates to one key at the same stamp resolve to the one whose value
    /// sorts first, and are counted.
    ///
    /// A stamp orders the steps of one host, and `UpsertHandle` shards only
    /// within a host, so two hosts ingesting a key in one transaction give it
    /// two updates the stamp cannot order.  One of them still has to win, and
    /// the choice has to fall the same way on every replay or a restart would
    /// reach a different collection.
    #[test]
    fn updates_at_the_same_stamp_resolve_to_the_first_value() {
        run_in_circuit_with_storage(|| {
            let factories = factories();

            // 10 and 20 at stamp 0, over a key the integral holds at 7.
            let resolved = run(
                &factories,
                &[(1, 7, 1)],
                Some(&[(1, 10, 0, 1), (1, 20, 0, 1)]),
            );
            assert_eq!(
                resolved.accumulator,
                Some(vec![(1, 7, -1), (1, 10, 1)]),
                "the value that sorts first wins"
            );
            assert_eq!(resolved.conflicts, 1);

            // The same updates in the other order reach the same collection,
            // because the cursor presents them in value order either way.
            let resolved = run(
                &factories,
                &[(1, 7, 1)],
                Some(&[(1, 20, 0, 1), (1, 10, 0, 1)]),
            );
            assert_eq!(resolved.accumulator, Some(vec![(1, 7, -1), (1, 10, 1)]));
            assert_eq!(resolved.conflicts, 1);

            // Three at one stamp count twice: every update but the winner.
            let resolved = run(
                &factories,
                &[],
                Some(&[(1, 10, 0, 1), (1, 20, 0, 1), (1, 30, 0, 1)]),
            );
            assert_eq!(resolved.accumulator, Some(vec![(1, 10, 1)]));
            assert_eq!(resolved.conflicts, 2);

            // Distinct stamps are not conflicts, whatever the values do.
            let resolved = run(&factories, &[], Some(&[(1, 30, 0, 1), (1, 10, 1, 1)]));
            assert_eq!(resolved.accumulator, Some(vec![(1, 10, 1)]));
            assert_eq!(resolved.conflicts, 0);
        });
    }

    /// Only the largest stamp survives a transaction: the updates it displaced
    /// are retracted.
    #[test]
    fn only_the_last_update_of_a_transaction_survives() {
        run_in_circuit_with_storage(|| {
            let factories = factories();
            assert_eq!(
                resolve(
                    &factories,
                    &[(1, 7, 1)],
                    &[(1, 10, 0, 1), (1, 20, 1, 1), (1, 30, 2, 1)]
                ),
                vec![(1, 7, -1), (1, 30, 1)]
            );
        });
    }

    /// An insert followed by a delete inside one transaction leaves the key
    /// gone, whichever order the values happen to sort in.
    #[test]
    fn an_insert_then_a_delete_leaves_the_key_gone() {
        run_in_circuit_with_storage(|| {
            let factories = factories();
            // The delete's value sorts after the insert's.
            assert_eq!(
                resolve(&factories, &[(1, 7, 1)], &[(1, 10, 0, 1), (1, 99, 1, -1)]),
                vec![(1, 7, -1)]
            );
            // ... and before it, which reaches the other branch of the scan.
            assert_eq!(
                resolve(&factories, &[(1, 7, 1)], &[(1, 10, 0, 1), (1, 2, 1, -1)]),
                vec![(1, 7, -1)]
            );
        });
    }

    /// A delete followed by an insert leaves the inserted value.
    #[test]
    fn a_delete_then_an_insert_leaves_the_insert() {
        run_in_circuit_with_storage(|| {
            let factories = factories();
            assert_eq!(
                resolve(&factories, &[(1, 7, 1)], &[(1, 0, 0, -1), (1, 20, 1, 1)]),
                vec![(1, 7, -1), (1, 20, 1)]
            );
        });
    }

    /// Keys are independent, and a key the transaction never mentions is left
    /// alone.
    #[test]
    fn untouched_keys_are_left_alone() {
        run_in_circuit_with_storage(|| {
            let factories = factories();
            assert_eq!(
                resolve(
                    &factories,
                    &[(1, 7, 1), (2, 8, 1), (3, 9, 1)],
                    &[(1, 10, 0, 1), (3, 0, 0, -1)]
                ),
                vec![(1, 7, -1), (1, 10, 1), (3, 9, -1)]
            );
        });
    }

    /// No accumulation means no work and no accumulator, which is every step of
    /// a transaction but its last.
    #[test]
    fn a_step_without_an_accumulation_produces_nothing() {
        run_in_circuit_with_storage(|| {
            let factories = factories();
            let resolved = run(&factories, &[(1, 7, 1)], None);
            assert!(resolved.accumulator.is_none());
            assert_eq!(
                resolved.steps, 1,
                "a step with no work to do takes one step"
            );
        });
    }

    /// Cutting the adjustments into chunks spreads the work over steps without
    /// changing it.  A chunk of one record ends a chunk at every key, so each of
    /// the three keys takes a step, and the operator takes one more to report
    /// that it has finished.
    #[test]
    fn chunking_spreads_the_work_over_steps() {
        let dir = tempdir().expect("temp dir");
        let config: CircuitConfig = mkconfig(dir.path()).with_splitter_chunk_size_records(1);
        run_in_circuit_with_storage_config(config, || {
            let factories = factories();
            let resolved = run(
                &factories,
                &[(1, 7, 1), (2, 8, 1), (3, 9, 1)],
                Some(&[(1, 10, 0, 1), (2, 20, 0, 1), (3, 0, 0, -1)]),
            );
            assert_eq!(
                resolved.accumulator,
                Some(vec![
                    (1, 7, -1),
                    (1, 10, 1),
                    (2, 8, -1),
                    (2, 20, 1),
                    (3, 9, -1)
                ])
            );
            assert_eq!(resolved.steps, 4, "a step per key, then one to finish");
        });
    }

    /// A run of keys that produces no adjustments still ends a step.
    ///
    /// Rewriting a key with the value it already holds cancels out, so the chunk
    /// never fills and only the bound on keys walked can end the step.  Without
    /// it a transaction of such writes would resolve in a single step however
    /// long it ran.
    #[test]
    fn a_run_of_keys_without_adjustments_still_ends_a_step() {
        let dir = tempdir().expect("temp dir");
        let config: CircuitConfig = mkconfig(dir.path()).with_lazy_input_map_keys_per_step(2);
        run_in_circuit_with_storage_config(config, || {
            let factories = factories();

            // Six keys, each written the value it already holds, so every key
            // cancels and the chunk stays empty.
            let held: Vec<(i32, i32, ZWeight)> = (1..=6).map(|key| (key, key * 10, 1)).collect();
            let rewritten: Vec<(i32, i32, u32, ZWeight)> =
                (1..=6).map(|key| (key, key * 10, 0, 1)).collect();

            let resolved = run(&factories, &held, Some(&rewritten));
            assert_eq!(
                resolved.accumulator,
                Some(Vec::new()),
                "rewriting a key with its own value leaves the integral alone"
            );
            assert_eq!(
                resolved.steps, 4,
                "three steps of two keys each, then one to finish"
            );
        });
    }
}

#[cfg(test)]
mod test;
