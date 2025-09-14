use crate::{
    algebra::{HasOne, HasZero, IndexedZSet, OrdZSet, ZTrace},
    circuit::{
        checkpointer::Checkpoint,
        circuit_builder::{register_replay_stream, CircuitBase, RefStreamValue},
        metadata::{BatchSizeStats, OperatorMeta, INPUT_BATCHES_LABEL, OUTPUT_BATCHES_LABEL},
        operator_traits::{BinaryOperator, Operator, TernaryOperator},
        OwnershipPreference, Scope,
    },
    declare_trait_object,
    dynamic::{ClonableTrait, Data, DataTrait, DynOpt, DynPairs, Erase, Factory, WithFactory},
    operator::{
        dynamic::{
            time_series::LeastUpperBoundFunc,
            trace::{DelayedTraceId, TraceBounds, TraceId, UntimedTraceAppend, Z1Trace},
        },
        Z1,
    },
    trace::{
        cursor::{Cursor, CursorFactory, CursorList},
        ord::vec::wset_batch::VecWSetBuilder,
        spine_async::FetchList,
        Batch, BatchFactories, BatchLocation, BatchReader, BatchReaderFactories, Builder,
        ListMerger, MergeCursor, Rkyv, Spine, VecWSetFactories,
    },
    Circuit, DBData, DynZWeight, NumEntries, RootCircuit, Stream, ZWeight,
};
use minitrace::trace;
use rkyv::{Archive, Deserialize, Serialize};
use size_of::SizeOf;
use std::{
    borrow::Cow,
    fmt::Debug,
    marker::PhantomData,
    mem::take,
    ops::{Deref, Neg},
};

use super::trace::BoundsId;

#[derive(
    Clone,
    Debug,
    Default,
    SizeOf,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Archive,
    Serialize,
    Deserialize,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
#[archive(compare(PartialEq, PartialOrd))]
pub enum Update<V: DBData, U: DBData> {
    Insert(V),
    #[default]
    Delete,
    Update(U),
}

pub enum UpdateRef<'a, V: DataTrait + ?Sized, U: DataTrait + ?Sized> {
    Insert(&'a V),
    Delete,
    Update(&'a U),
}

impl<V: DBData, U: DBData> NumEntries for Update<V, U> {
    const CONST_NUM_ENTRIES: Option<usize> = Some(1);

    fn num_entries_shallow(&self) -> usize {
        1
    }

    fn num_entries_deep(&self) -> usize {
        1
    }
}

pub trait UpdateTrait<V: DataTrait + ?Sized, U: DataTrait + ?Sized>: Data {
    fn get(&self) -> UpdateRef<V, U>;
    fn insert_ref(&mut self, val: &V);
    fn insert_val(&mut self, val: &mut V);
    fn delete(&mut self);
    fn update_ref(&mut self, upd: &U);
    fn update_val(&mut self, upd: &mut U);
}

impl<V, U, VType, UType> UpdateTrait<V, U> for Update<VType, UType>
where
    V: DataTrait + ?Sized,
    U: DataTrait + ?Sized,
    VType: DBData + Erase<V>,
    UType: DBData + Erase<U>,
{
    fn get(&self) -> UpdateRef<V, U> {
        match self {
            Update::Insert(v) => UpdateRef::Insert(v.erase()),
            Update::Delete => UpdateRef::Delete,
            Update::Update(u) => UpdateRef::Update(u.erase()),
        }
    }

    fn insert_ref(&mut self, val: &V) {
        *self = Update::Insert(unsafe { val.downcast::<VType>().clone() })
    }

    fn insert_val(&mut self, val: &mut V) {
        *self = Update::Insert(take(unsafe { val.downcast_mut::<VType>() }))
    }

    fn delete(&mut self) {
        *self = Update::Delete;
    }

    fn update_ref(&mut self, upd: &U) {
        *self = Update::Update(unsafe { upd.downcast::<UType>().clone() })
    }

    fn update_val(&mut self, upd: &mut U) {
        *self = Update::Update(take(unsafe { upd.downcast_mut::<UType>() }))
    }
}

declare_trait_object!(DynUpdate<VTrait, UTrait> = dyn UpdateTrait<VTrait, UTrait>
where
    VTrait: DataTrait + ?Sized,
    UTrait: DataTrait + ?Sized,
);

pub type PatchFunc<V, U> = Box<dyn Fn(&mut V, &U)>;

pub struct InputUpsertFactories<B: IndexedZSet> {
    pub batch_factories: B::Factories,
    pub keys_factories: VecWSetFactories<B::Key, DynZWeight>,
    pub opt_key_factory: &'static dyn Factory<DynOpt<B::Key>>,
    pub opt_val_factory: &'static dyn Factory<DynOpt<B::Val>>,
}

impl<B: IndexedZSet> Clone for InputUpsertFactories<B> {
    fn clone(&self) -> Self {
        Self {
            batch_factories: self.batch_factories.clone(),
            keys_factories: self.keys_factories.clone(),
            opt_key_factory: self.opt_key_factory,
            opt_val_factory: self.opt_val_factory,
        }
    }
}

impl<B> InputUpsertFactories<B>
where
    B: Batch + IndexedZSet,
{
    pub fn new<KType, VType>() -> Self
    where
        KType: DBData + Erase<B::Key>,
        VType: DBData + Erase<B::Val>,
    {
        Self {
            batch_factories: BatchReaderFactories::new::<KType, VType, ZWeight>(),
            keys_factories: VecWSetFactories::new::<KType, (), ZWeight>(),
            opt_key_factory: WithFactory::<Option<KType>>::FACTORY,
            opt_val_factory: WithFactory::<Option<VType>>::FACTORY,
        }
    }
}

pub struct InputUpsertWithWaterlineFactories<B: IndexedZSet, E: DataTrait + ?Sized> {
    pub batch_factories: B::Factories,
    pub opt_key_factory: &'static dyn Factory<DynOpt<B::Key>>,
    pub opt_val_factory: &'static dyn Factory<DynOpt<B::Val>>,
    pub val_factory: &'static dyn Factory<B::Val>,
    errors_factory: <OrdZSet<E> as BatchReader>::Factories,
}

impl<B: IndexedZSet, E: DataTrait + ?Sized> Clone for InputUpsertWithWaterlineFactories<B, E> {
    fn clone(&self) -> Self {
        Self {
            batch_factories: self.batch_factories.clone(),
            opt_key_factory: self.opt_key_factory,
            opt_val_factory: self.opt_val_factory,
            val_factory: self.val_factory,
            errors_factory: self.errors_factory.clone(),
        }
    }
}

impl<B, E> InputUpsertWithWaterlineFactories<B, E>
where
    B: Batch + IndexedZSet,
    E: DataTrait + ?Sized,
{
    pub fn new<KType, VType, EType>() -> Self
    where
        KType: DBData + Erase<B::Key>,
        VType: DBData + Erase<B::Val>,
        EType: DBData + Erase<E>,
    {
        Self {
            batch_factories: BatchReaderFactories::new::<KType, VType, ZWeight>(),
            opt_key_factory: WithFactory::<Option<KType>>::FACTORY,
            opt_val_factory: WithFactory::<Option<VType>>::FACTORY,
            val_factory: WithFactory::<VType>::FACTORY,
            errors_factory: BatchReaderFactories::new::<EType, (), ZWeight>(),
        }
    }
}

impl<K, V, U> Stream<RootCircuit, Vec<Box<DynPairs<K, DynUpdate<V, U>>>>>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    U: DataTrait + ?Sized,
{
    /// Convert an input stream of upserts into a stream of updates to a
    /// relation.
    ///
    /// The input stream carries changes to a key/value map in the form of
    /// _upserts_.  An upsert assigns a new value to a key (or deletes the key
    /// from the map) without explicitly removing the old value, if any.  The
    /// operator converts upserts into batches of updates, which is the input
    /// format of most DBSP operators.
    ///
    /// The operator assumes that the input vector is sorted by key; however,
    /// unlike the [`Stream::upsert`] operator it allows the vector to
    /// contain multiple updates per key.  Updates are applied one by one in
    /// order, and the output of the operator reflects cumulative effect of
    /// the updates.  Additionally, unlike the [`Stream::upsert`] operator,
    /// which only supports inserts, which overwrite the entire value with a
    /// new value, and deletions, this operator also supports updates that
    /// modify the contents of a value, e.g., overwriting some of its
    /// fields.  Type argument `U` defines the format of modifications,
    /// and the `patch_func` function applies update of type `U` to a value of
    /// type `V`.
    ///
    /// This is a stateful operator that internally maintains the trace of the
    /// collection.
    pub fn input_upsert<B>(
        &self,
        persistent_id: Option<&str>,
        factories: &InputUpsertFactories<B>,
        patch_func: PatchFunc<V, U>,
    ) -> Stream<RootCircuit, B>
    where
        B: IndexedZSet<Key = K, Val = V>,
    {
        let circuit = self.circuit();

        assert!(
            self.is_sharded(),
            "input_upsert operator applied to a non-sharded collection"
        );

        // We build the following circuit to implement the upsert semantics.
        // The collection is accumulated into a trace using integrator
        // (UntimedTraceAppend + Z1Trace = integrator).  The `InputUpsert`
        // operator evaluates each upsert command in the input stream against
        // the trace and computes a batch of updates to be added to the trace.
        //
        // ```text
        //                               ┌────────────────────────────►
        //                               │
        //                               │
        //  self        ┌───────────┐    │        ┌──────────────────┐  trace
        // ────────────►│InputUpsert├────┴───────►│UntimedTraceAppend├────┐
        //              └───────────┘   delta     └──────────────────┘    │
        //                      ▲                  ▲                      │
        //                      │                  │                      │
        //                      │                  │   ┌───────┐          │
        //                      └──────────────────┴───┤Z1Trace│◄─────────┘
        //                         z1trace             └───────┘
        // ```
        circuit.region("input_upsert", || {
            let bounds = <TraceBounds<K, V>>::unbounded();

            let z1 = Z1Trace::new(
                &factories.batch_factories,
                &factories.batch_factories,
                false,
                circuit.root_scope(),
                bounds.clone(),
            );

            let (delayed_trace, z1feedback) = circuit.add_feedback_persistent(
                persistent_id
                    .map(|name| format!("{name}.integral"))
                    .as_deref(),
                z1,
            );

            delayed_trace.mark_sharded();

            let delta = circuit
                .add_binary_operator(
                    <InputUpsert<Spine<B>, U, B>>::new(
                        factories.batch_factories.clone(),
                        factories.opt_key_factory,
                        factories.opt_val_factory,
                        factories.keys_factories.clone(),
                        patch_func,
                    ),
                    &delayed_trace,
                    self,
                )
                .mark_distinct();
            delta.mark_sharded();
            let replay_stream = z1feedback.operator_mut().prepare_replay_stream(&delta);

            let trace = circuit.add_binary_operator_with_preference(
                UntimedTraceAppend::<Spine<B>>::new(),
                (&delayed_trace, OwnershipPreference::STRONGLY_PREFER_OWNED),
                (&delta, OwnershipPreference::PREFER_OWNED),
            );
            trace.mark_sharded();

            z1feedback.connect_with_preference(&trace, OwnershipPreference::STRONGLY_PREFER_OWNED);

            register_replay_stream(circuit, &delta, &replay_stream);

            circuit.cache_insert(DelayedTraceId::new(trace.stream_id()), delayed_trace);
            circuit.cache_insert(TraceId::new(delta.stream_id()), trace);
            circuit.cache_insert(BoundsId::<B>::new(delta.stream_id()), bounds);
            delta
        })
    }

    // Like `input_upsert`, but additionally tracks a waterline of the input collection and
    // rejects inputs that are below the waterline.  An input is rejected if the input record
    // itself is below the waterline or if the existing record it replaces is below the waterline.
    #[allow(clippy::too_many_arguments)]
    pub fn input_upsert_with_waterline<B, W, E>(
        &self,
        persistent_id: Option<&str>,
        factories: &InputUpsertWithWaterlineFactories<B, E>,
        patch_func: PatchFunc<V, U>,
        init_waterline: Box<dyn Fn() -> Box<W>>,
        extract_ts: Box<dyn Fn(&B::Key, &B::Val, &mut W)>,
        least_upper_bound: LeastUpperBoundFunc<W>,
        filter_func: Box<dyn Fn(&W, &B::Key, &B::Val) -> bool>,
        report_func: Box<dyn Fn(&W, &B::Key, &B::Val, ZWeight, &mut E)>,
    ) -> (
        Stream<RootCircuit, B>,
        Stream<RootCircuit, OrdZSet<E>>,
        Stream<RootCircuit, Box<W>>,
    )
    where
        B: IndexedZSet<Key = K, Val = V>,
        W: DataTrait + Checkpoint + ?Sized,
        E: DataTrait + ?Sized,
        Box<W>: Checkpoint + Clone + NumEntries + Rkyv,
    {
        let circuit = self.circuit();

        assert!(
            self.is_sharded(),
            "input_upsert_with_waterline operator applied to a non-sharded collection"
        );

        // ```text
        //                   ┌─────────────────────────────────────────────►
        //                   │ waterline
        //             ┌─────┴─────┐
        //             │ waterline │◄─────────┬────────────────────────────►
        //             └──────┬────┘          │
        //                    │ waterline     │
        //                   Z1               │
        //  delayed_waterline │               │
        //                    ▼               │
        //         ┌─────────────────────┐    │        ┌──────────────────┐  trace
        // ───────►│InputUpsertWaterline ├────┴───────►│UntimedTraceAppend├────┐
        //         └──────┬──────────────┘   delta     └──────────────────┘    │
        //                │          ▲                  ▲                      │
        //                │          │                  │                      │
        //                │          │                  │   ┌───────┐          │
        //                │          └──────────────────┴───┤Z1Trace│◄─────────┘
        //                │             delayed_trace       └───────┘
        //                │
        //                │error stream
        //                └────────────────────────────────────────────────►
        // ```

        circuit.region("input_upsert_waterline", || {
            let bounds = <TraceBounds<K, V>>::unbounded();

            let z1 = Z1Trace::new(
                &factories.batch_factories,
                &factories.batch_factories,
                false,
                circuit.root_scope(),
                bounds.clone(),
            );

            let (delayed_trace, z1feedback) = circuit.add_feedback_persistent(
                persistent_id
                    .map(|name| format!("{name}.integral"))
                    .as_deref(),
                z1,
            );

            delayed_trace.mark_sharded();

            let waterline_z1 = Z1::new((init_waterline)());

            let (delayed_waterline, waterline_feedback) = circuit.add_feedback_persistent(
                persistent_id
                    .map(|name| format!("{name}.delayed_waterline"))
                    .as_deref(),
                waterline_z1,
            );

            let error_stream_val = RefStreamValue::empty();

            let delta = circuit
                .add_ternary_operator(
                    <InputUpsertWithWaterline<Spine<B>, U, B, W, E>>::new(
                        factories.clone(),
                        patch_func,
                        filter_func,
                        report_func,
                        error_stream_val.clone(),
                    ),
                    &delayed_trace,
                    self,
                    &delayed_waterline,
                )
                .mark_distinct();
            delta.mark_sharded();
            let replay_stream = z1feedback.operator_mut().prepare_replay_stream(&delta);

            let waterline_id = persistent_id.map(|name| format!("{name}.input_waterline"));

            let waterline = delta.dyn_waterline(
                waterline_id.as_deref(),
                init_waterline,
                extract_ts,
                least_upper_bound,
            );

            waterline_feedback.connect(&waterline);

            let trace = circuit.add_binary_operator_with_preference(
                UntimedTraceAppend::<Spine<B>>::new(),
                (&delayed_trace, OwnershipPreference::STRONGLY_PREFER_OWNED),
                (&delta, OwnershipPreference::PREFER_OWNED),
            );
            trace.mark_sharded();

            z1feedback.connect_with_preference(&trace, OwnershipPreference::STRONGLY_PREFER_OWNED);

            register_replay_stream(circuit, &delta, &replay_stream);

            let error_stream = Stream::with_value(
                self.circuit().clone(),
                delta.local_node_id(),
                error_stream_val,
            );

            circuit.cache_insert(DelayedTraceId::new(trace.stream_id()), delayed_trace);
            circuit.cache_insert(TraceId::new(delta.stream_id()), trace);
            circuit.cache_insert(BoundsId::<B>::new(delta.stream_id()), bounds);

            (delta, error_stream, waterline)
        })
    }
}

pub struct InputUpsert<T, U, B>
where
    T: BatchReader,
    B: Batch,
    U: DataTrait + ?Sized,
{
    batch_factories: B::Factories,
    opt_key_factory: &'static dyn Factory<DynOpt<B::Key>>,
    opt_val_factory: &'static dyn Factory<DynOpt<B::Val>>,
    keys_factories: VecWSetFactories<B::Key, DynZWeight>,
    patch_func: PatchFunc<T::Val, U>,

    // Input batch sizes.
    input_batch_stats: BatchSizeStats,

    // Output batch sizes.
    output_batch_stats: BatchSizeStats,

    phantom: PhantomData<B>,
}

impl<T, U, B> InputUpsert<T, U, B>
where
    T: BatchReader,
    B: Batch,
    U: DataTrait + ?Sized,
{
    pub fn new(
        batch_factories: B::Factories,
        opt_key_factory: &'static dyn Factory<DynOpt<B::Key>>,
        opt_val_factory: &'static dyn Factory<DynOpt<B::Val>>,
        keys_factories: VecWSetFactories<B::Key, DynZWeight>,
        patch_func: PatchFunc<T::Val, U>,
    ) -> Self {
        Self {
            batch_factories,
            opt_key_factory,
            opt_val_factory,
            keys_factories,
            patch_func,
            input_batch_stats: BatchSizeStats::new(),
            output_batch_stats: BatchSizeStats::new(),
            phantom: PhantomData,
        }
    }
}

impl<T, U, B> Operator for InputUpsert<T, U, B>
where
    T: BatchReader,
    U: DataTrait + ?Sized,
    B: Batch,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("InputUpsert")
    }

    fn metadata(&self, meta: &mut OperatorMeta) {
        meta.extend(metadata! {
            INPUT_BATCHES_LABEL => self.input_batch_stats.metadata(),
            OUTPUT_BATCHES_LABEL => self.output_batch_stats.metadata(),
        });
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<T, U, B> BinaryOperator<T, Vec<Box<DynPairs<T::Key, DynUpdate<T::Val, U>>>>, B>
    for InputUpsert<T, U, B>
where
    T: ZTrace<Time = ()>,
    U: DataTrait + ?Sized,
    B: IndexedZSet<Key = T::Key, Val = T::Val>,
{
    #[trace]
    async fn eval(
        &mut self,
        trace: &T,
        updates: &Vec<Box<DynPairs<T::Key, DynUpdate<T::Val, U>>>>,
    ) -> B {
        // Inputs must be sorted by key
        let mut updates = updates
            .iter()
            .filter_map(|updates| {
                if !updates.is_empty() {
                    Some((&**updates, 0))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        let n_updates = updates.iter().map(|updates| updates.0.len()).sum();
        debug_assert!(updates
            .iter()
            .all(|updates| updates.0.is_sorted_by(&|u1, u2| u1.fst().cmp(u2.fst()))));

        self.input_batch_stats.add_batch(n_updates);

        // # Overview
        //
        // We divide processing upserts for a given key in our input stream into
        // two parts:
        //
        // 1. Inserting the key's new value.
        //
        // 2. Deleting the key's old value (a retraction).
        //
        // A truly new key has only #1, a key that existed and is being deleted
        // has only #2, a key that existed and is being inserted has both #1 and
        // #2.  A key that existed and is being inserted with the same value is
        // a no-op and we have to make it cancel out one way or another, as
        // described below.
        //
        // # Strategy
        //
        // The original and simplest approach here is to generate retractions at
        // the same time we process the updates.  For each key, we find in the
        // trace the original value of the key, if there is one, and add it,
        // negated, to `key_updates`.  We also add the final value of the key to
        // `key_updates`.  Then we consolidate to get the overall update (which
        // will be empty if the old and new value are the same).
        //
        // A different strategy is to add the keys that are being updated to a
        // collection of keys (`keys_builder`), and then later look them all up
        // in the trace in batch using [BatchReader::fetch].  If we find any (we
        // often will not), then we can negate the results of the lookup to form
        // the retractions, and merge it with the insertions.  Batch lookup is
        // optimized such that this yields up to about 15% better performance.
        //
        // # Problem
        //
        // However, [BatchReader::fetch] introduces another complication, in
        // that it only does the lookup for on-storage batches; for in-memory
        // batches, it does nothing.  [Spine::fetch] makes this a bigger problem
        // because it mixes full original in-memory batches with fetched
        // on-storage batches.  That means that if we use [Spine::fetch] to
        // generate our retractions, we will retract all of the keys in the
        // in-memory batches.
        //
        // We solve this problem by breaking the spine into `memory_batches` and
        // `storage_batches`.  If a key appears in the former, then we generate
        // a retraction for it immediately[^1]; otherwise, we add the key to
        // `keys_builder` and later attempt fetch it from the on-storage
        // batches.  This also improves performance in the common case.
        //
        // [^1]: We do have to look it up in the on-storage batches in that case
        //   since it's possible that there's a canceling tuple there.
        //
        // [Spine::fetch]: crate::trace::spine_async::Spine

        let mut key_updates = self.batch_factories.weighted_vals_factory().default_box();

        let mut trace_cursor = trace.cursor();

        // Get batches from the trace and break them into in-memory and
        // on-storage batches.
        let (memory_batches, storage_batches) = trace
            .batches()
            .into_iter()
            .partition::<Vec<_>, _>(|b| b.location() == BatchLocation::Memory);

        // Create a cursor for looking up just the in-memory batches.
        let mut memory_cursor = CursorList::new(
            trace_cursor.weight_factory(),
            memory_batches.iter().map(|b| b.cursor()).collect(),
        );

        // Create a builder for accumulating the keys to fetch from
        // `storage_batches` later to generate retractions.
        let mut keys_builder = VecWSetBuilder::with_capacity(&self.keys_factories, n_updates);

        let mut builder = B::Builder::with_capacity(&self.batch_factories, n_updates);

        // Current key for which we are processing updates.
        let mut cur_key: Box<DynOpt<T::Key>> = self.opt_key_factory.default_box();

        // Current value associated with the key after applying all processed updates
        // to it.
        let mut cur_val: Box<DynOpt<T::Val>> = self.opt_val_factory.default_box();

        while !updates.is_empty() {
            let (index, key_upd) = updates
                .iter()
                .map(|(updates, index)| updates.index(*index))
                .enumerate()
                .min_by(|(_a_index, a), (_b_index, b)| a.cmp(b))
                .unwrap();
            updates[index].1 += 1;
            if updates[index].1 >= updates[index].0.len() {
                updates.remove(index);
            }

            let (key, upd) = key_upd.split();

            // We finished processing updates for the previous key. Push them to the
            // builder and generate a retraction for the new key.
            if cur_key.get() != Some(key) {
                // Push updates for the previous key to the builder.
                if let Some(cur_key) = cur_key.get_mut() {
                    if let Some(val) = cur_val.get_mut() {
                        key_updates.push_with(&mut |item| {
                            let (v, w) = item.split_mut();

                            val.move_to(v);
                            **w = HasOne::one();
                        });
                    }
                    key_updates.consolidate();
                    if !key_updates.is_empty() {
                        for pair in key_updates.dyn_iter_mut() {
                            let (v, d) = pair.split_mut();
                            builder.push_val_diff_mut(v, d);
                        }
                        builder.push_key(cur_key);
                    }
                    key_updates.clear();
                }

                cur_key.from_ref(key);
                cur_val.set_none();

                // If `key` is in the in-memory trace, then get it from the
                // overall trace and generate a retraction.
                //
                // Otherwise, it might be in the on-disk trace, so append it to
                // `keys_builder`.
                enum Action {
                    GenerateRetraction,
                    FetchFromStorage,
                }
                let action = match (memory_batches.is_empty(), storage_batches.is_empty()) {
                    (true, true) => None,
                    (false, true) => trace_cursor
                        .seek_key_exact(key, None)
                        .then_some(Action::GenerateRetraction),
                    (true, false) => Some(Action::FetchFromStorage),
                    (false, false) => {
                        let hash = key.default_hash();
                        if !memory_cursor.seek_key_exact(key, Some(hash)) {
                            // `key` is not in memory but it could be in storage.
                            Some(Action::FetchFromStorage)
                        } else if trace_cursor.seek_key_exact(key, Some(hash)) {
                            // `key` is in memory and it is not cancelled out by
                            // keys in storage.
                            Some(Action::GenerateRetraction)
                        } else {
                            // `key` is in memory but it is cancelled out by
                            // on-storage keys.
                            None
                        }
                    }
                };
                match action {
                    Some(Action::FetchFromStorage) => {
                        keys_builder.push_val_diff(&(), &1);
                        keys_builder.push_key(key);
                    }
                    Some(Action::GenerateRetraction) => {
                        while trace_cursor.val_valid() {
                            let weight = **trace_cursor.weight();

                            if !weight.is_zero() {
                                let val = trace_cursor.val();

                                key_updates.push_with(&mut |item| {
                                    let (v, w) = item.split_mut();

                                    val.clone_to(v);
                                    **w = weight.neg()
                                });
                                cur_val.from_ref(val);
                            }

                            trace_cursor.step_val();
                        }
                    }
                    None => (),
                }
            }

            match upd.get() {
                UpdateRef::Delete => {
                    // TODO: if cur_val.is_none(), report missing key.
                    cur_val.set_none();
                }
                UpdateRef::Insert(val) => {
                    cur_val.from_ref(val);
                }
                UpdateRef::Update(upd) => {
                    if let Some(val) = cur_val.get_mut() {
                        (self.patch_func)(val, upd);
                    } else {
                        // TODO: report missing key.
                    }
                }
            }
        }

        // Push updates for the last key.
        if let Some(cur_key) = cur_key.get_mut() {
            if let Some(val) = cur_val.get_mut() {
                key_updates.push_with(&mut |item| {
                    let (v, w) = item.split_mut();

                    val.move_to(v);
                    **w = HasOne::one();
                });
            }

            key_updates.consolidate();
            if !key_updates.is_empty() {
                for pair in key_updates.dyn_iter_mut() {
                    let (v, d) = pair.split_mut();
                    builder.push_val_diff_mut(v, d);
                }
                builder.push_key(cur_key);
            }
            key_updates.clear();
        }

        let mut insertions = builder.done();

        // Fetch retraction values from the on-storage batches.
        let retractions = FetchList::new(
            storage_batches,
            &keys_builder.done(),
            trace_cursor.weight_factory(),
        )
        .await;

        // If there are any values to retract, then generate retractions and
        // merge them with insertions.  Otherwise, the result is just the
        // insertions.
        let retraction_cursor = retractions.get_cursor();
        if retraction_cursor.key_valid() {
            let builder = B::Builder::with_capacity(&self.batch_factories, n_updates * 2);
            let insertion_cursor = insertions.consuming_cursor(None, None);
            let retraction_cursor = Box::new(NegatedMergeCursor::new(retraction_cursor));
            let vec: Vec<Box<dyn MergeCursor<_, _, _, _>>> =
                vec![insertion_cursor, retraction_cursor];
            ListMerger::merge(&self.batch_factories, builder, vec)
        } else {
            insertions
        }
    }

    fn input_preference(&self) -> (OwnershipPreference, OwnershipPreference) {
        (
            OwnershipPreference::PREFER_OWNED,
            OwnershipPreference::PREFER_OWNED,
        )
    }
}

pub struct InputUpsertWithWaterline<T, U, B, W, E>
where
    T: BatchReader,
    B: IndexedZSet,
    U: DataTrait + ?Sized,
    W: DataTrait + ?Sized,
    E: DataTrait + ?Sized,
{
    factories: InputUpsertWithWaterlineFactories<B, E>,
    patch_func: PatchFunc<T::Val, U>,
    filter_func: Box<dyn Fn(&W, &B::Key, &B::Val) -> bool>,
    report_func: Box<dyn Fn(&W, &B::Key, &B::Val, ZWeight, &mut E)>,
    error_stream_val: RefStreamValue<OrdZSet<E>>,

    // Input batch sizes.
    input_batch_stats: BatchSizeStats,

    // Output batch sizes.
    output_batch_stats: BatchSizeStats,

    phantom: PhantomData<B>,
}

impl<T, U, B, W, E> InputUpsertWithWaterline<T, U, B, W, E>
where
    T: BatchReader,
    B: IndexedZSet,
    U: DataTrait + ?Sized,
    W: DataTrait + ?Sized,
    E: DataTrait + ?Sized,
{
    pub fn new(
        factories: InputUpsertWithWaterlineFactories<B, E>,
        patch_func: PatchFunc<T::Val, U>,
        filter_func: Box<dyn Fn(&W, &B::Key, &B::Val) -> bool>,
        report_func: Box<dyn Fn(&W, &B::Key, &B::Val, ZWeight, &mut E)>,
        error_stream_val: RefStreamValue<OrdZSet<E>>,
    ) -> Self {
        Self {
            factories,
            patch_func,
            filter_func,
            report_func,
            error_stream_val,
            input_batch_stats: BatchSizeStats::new(),
            output_batch_stats: BatchSizeStats::new(),
            phantom: PhantomData,
        }
    }

    fn passes_filter(&self, waterline: &W, key: &B::Key, val: &B::Val) -> bool {
        (self.filter_func)(waterline, key, val)
    }
}

impl<T, U, B, W, E> Operator for InputUpsertWithWaterline<T, U, B, W, E>
where
    T: BatchReader,
    U: DataTrait + ?Sized,
    B: IndexedZSet,
    W: DataTrait + ?Sized,
    E: DataTrait + ?Sized,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("InputUpsertWithWaterline")
    }

    fn metadata(&self, meta: &mut OperatorMeta) {
        meta.extend(metadata! {
            INPUT_BATCHES_LABEL => self.input_batch_stats.metadata(),
            OUTPUT_BATCHES_LABEL => self.output_batch_stats.metadata(),
        });
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<T, U, B, W, E> TernaryOperator<T, Vec<Box<DynPairs<T::Key, DynUpdate<T::Val, U>>>>, Box<W>, B>
    for InputUpsertWithWaterline<T, U, B, W, E>
where
    T: ZTrace<Time = ()> + Clone,
    U: DataTrait + ?Sized,
    B: IndexedZSet<Key = T::Key, Val = T::Val>,
    W: DataTrait + ?Sized,
    Box<W>: Clone,
    E: DataTrait + ?Sized,
{
    #[trace]
    async fn eval(
        &mut self,
        trace: Cow<'_, T>,
        updates: Cow<'_, Vec<Box<DynPairs<T::Key, DynUpdate<T::Val, U>>>>>,
        waterline: Cow<'_, Box<W>>,
    ) -> B {
        // Inputs must be sorted by key
        let mut updates = updates
            .iter()
            .filter_map(|updates| {
                if !updates.is_empty() {
                    Some((updates, 0))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        let n_updates = updates.iter().map(|updates| updates.0.len()).sum();
        debug_assert!(updates
            .iter()
            .all(|updates| updates.0.is_sorted_by(&|u1, u2| u1.fst().cmp(u2.fst()))));

        self.input_batch_stats.add_batch(n_updates);

        let mut errors = self
            .factories
            .errors_factory
            .weighted_items_factory()
            .default_box();

        let waterline = waterline.deref();
        let mut key_updates = self
            .factories
            .batch_factories
            .weighted_vals_factory()
            .default_box();

        let mut trace_cursor = trace.deref().cursor();

        let mut builder = B::Builder::with_capacity(&self.factories.batch_factories, n_updates * 2);

        // Current key for which we are processing updates.
        let mut cur_key: Box<DynOpt<T::Key>> = self.factories.opt_key_factory.default_box();

        // Current value associated with the key after applying all processed updates
        // to it.
        let mut cur_val: Box<DynOpt<T::Val>> = self.factories.opt_val_factory.default_box();
        let mut tmp_val: Box<T::Val> = self.factories.val_factory.default_box();

        // Set to true when the value associated with the current key doesn't
        // satisfy `val_filter`, hence refuse to remove this value and process
        // all updates for this key.
        let mut skip_key = false;

        while !updates.is_empty() {
            let (index, key_upd) = updates
                .iter()
                .map(|(updates, index)| updates.index(*index))
                .enumerate()
                .min_by(|(_a_index, a), (_b_index, b)| a.cmp(b))
                .unwrap();
            updates[index].1 += 1;
            if updates[index].1 >= updates[index].0.len() {
                updates.remove(index);
            }

            let (key, upd) = key_upd.split();

            // We finished processing updates for the previous key. Push them to the
            // builder and generate a retraction for the new key.
            if cur_key.get() != Some(key) {
                // Push updates for the previous key to the builder.
                if let Some(cur_key) = cur_key.get_mut() {
                    if let Some(val) = cur_val.get_mut() {
                        key_updates.push_with(&mut |item| {
                            let (v, w) = item.split_mut();

                            val.move_to(v);
                            **w = HasOne::one();
                        });
                    }
                    key_updates.consolidate();
                    if !key_updates.is_empty() {
                        for pair in key_updates.dyn_iter_mut() {
                            let (v, d) = pair.split_mut();
                            builder.push_val_diff_mut(v, d);
                        }
                        builder.push_key(cur_key);
                    }
                    key_updates.clear();
                }

                skip_key = false;
                cur_key.from_ref(key);
                cur_val.set_none();

                // Generate retraction if `key` is present in the trace.
                if trace_cursor.seek_key_exact(key, None) {
                    // println!("{}: found key in trace_cursor", Runtime::worker_index());
                    while trace_cursor.val_valid() {
                        let weight = **trace_cursor.weight();

                        if !weight.is_zero() {
                            let val = trace_cursor.val();

                            if self.passes_filter(waterline, key, val) {
                                key_updates.push_with(&mut |item| {
                                    let (v, w) = item.split_mut();

                                    val.clone_to(v);
                                    **w = weight.neg()
                                });
                                cur_val.from_ref(val);
                            } else {
                                skip_key = true;
                                errors.push_with(&mut |item| {
                                    let (kv, err_weight) = item.split_mut();
                                    **err_weight = HasOne::one();
                                    (self.report_func)(
                                        waterline,
                                        key,
                                        val,
                                        weight.neg(),
                                        kv.fst_mut(),
                                    );
                                });
                            }
                        }

                        trace_cursor.step_val();
                    }
                }
            }

            if !skip_key {
                match upd.get() {
                    UpdateRef::Delete => {
                        // TODO: if cur_val.is_none(), report missing key.
                        cur_val.set_none();
                    }
                    UpdateRef::Insert(val) => {
                        if self.passes_filter(waterline, key, val) {
                            cur_val.from_ref(val);
                        } else {
                            errors.push_with(&mut |item| {
                                let (kv, err_weight) = item.split_mut();
                                **err_weight = HasOne::one();
                                (self.report_func)(waterline, key, val, 1, kv.fst_mut());
                            });
                        }
                    }
                    UpdateRef::Update(upd) => {
                        if let Some(val) = cur_val.get_mut() {
                            val.clone_to(&mut tmp_val);
                            (self.patch_func)(&mut tmp_val, upd);
                            if !self.passes_filter(waterline, key, &tmp_val) {
                                errors.push_with(&mut |item| {
                                    let (kv, err_weight) = item.split_mut();
                                    **err_weight = HasOne::one();
                                    (self.report_func)(waterline, key, &tmp_val, 1, kv.fst_mut());
                                });
                            } else {
                                tmp_val.clone_to(val);
                            }
                        } else {
                            // TODO: report missing key.
                        }
                    }
                }
            }
        }

        // Push updates for the last key.
        if let Some(cur_key) = cur_key.get_mut() {
            if let Some(val) = cur_val.get_mut() {
                key_updates.push_with(&mut |item| {
                    let (v, w) = item.split_mut();

                    val.move_to(v);
                    **w = HasOne::one();
                });
            }

            key_updates.consolidate();
            if !key_updates.is_empty() {
                for pair in key_updates.dyn_iter_mut() {
                    let (v, d) = pair.split_mut();
                    builder.push_val_diff_mut(v, d);
                }
                builder.push_key(cur_key);
            }
            key_updates.clear();
        }

        let errors = <OrdZSet<E>>::dyn_from_tuples(&self.factories.errors_factory, (), &mut errors);
        self.error_stream_val.put(errors);

        let result = builder.done();
        self.output_batch_stats.add_batch(result.len());
        result
    }

    fn input_preference(
        &self,
    ) -> (
        OwnershipPreference,
        OwnershipPreference,
        OwnershipPreference,
    ) {
        (
            OwnershipPreference::PREFER_OWNED,
            OwnershipPreference::PREFER_OWNED,
            OwnershipPreference::INDIFFERENT,
        )
    }
}

/// Wraps any [Cursor] with a [MergeCursor] that negates its weights.
pub struct NegatedMergeCursor<K, V, T, C>
where
    K: ?Sized,
    V: ?Sized,
    C: Cursor<K, V, T, DynZWeight>,
{
    cursor: C,
    weight: Box<DynZWeight>,
    phantom: PhantomData<(Box<K>, Box<V>, T)>,
}

impl<K, V, T, C> NegatedMergeCursor<K, V, T, C>
where
    K: ?Sized,
    V: ?Sized,
    C: Cursor<K, V, T, DynZWeight>,
{
    pub fn new(cursor: C) -> Self {
        Self {
            weight: cursor.weight_factory().default_box(),
            cursor,
            phantom: PhantomData,
        }
    }
}

impl<K, V, T, C> MergeCursor<K, V, T, DynZWeight> for NegatedMergeCursor<K, V, T, C>
where
    K: ?Sized + Debug,
    V: ?Sized + Debug,
    C: Cursor<K, V, T, DynZWeight>,
{
    fn key_valid(&self) -> bool {
        self.cursor.key_valid()
    }
    fn val_valid(&self) -> bool {
        self.cursor.val_valid()
    }
    fn val(&self) -> &V {
        self.cursor.val()
    }
    fn key(&self) -> &K {
        self.cursor.key()
    }
    fn step_key(&mut self) {
        self.cursor.step_key()
    }
    fn step_val(&mut self) {
        self.cursor.step_val()
    }
    fn map_times(&mut self, logic: &mut dyn FnMut(&T, &DynZWeight)) {
        self.cursor.map_times(&mut |t, r| {
            logic(t, &r.neg());
        });
    }
    fn weight(&mut self) -> &DynZWeight
    where
        T: PartialEq<()>,
    {
        **self.weight = self.cursor.weight().neg();
        &*self.weight
    }
}
