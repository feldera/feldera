use crate::{
    algebra::{AddAssignByRef, HasOne, HasZero, IndexedZSet, PartialOrder, ZSet, ZTrace},
    circuit::{
        circuit_builder::register_replay_stream,
        operator_traits::{BinaryOperator, Operator},
        OwnershipPreference, Scope, WithClock,
    },
    dynamic::{ClonableTrait, DataTrait, DynOpt, DynPairs, DynUnit, Erase},
    operator::dynamic::trace::{
        DelayedTraceId, TraceAppend, TraceBounds, TraceId, ValSpine, Z1Trace,
    },
    trace::{
        Batch, BatchFactories, BatchReader, BatchReaderFactories, Builder, Cursor, TupleBuilder,
    },
    Circuit, DBData, Stream, Timestamp, ZWeight,
};
use minitrace::trace;
use std::{borrow::Cow, marker::PhantomData, ops::Neg};

use super::trace::{BoundsId, KeySpine};

pub struct UpdateSetFactories<T: Timestamp, B: ZSet> {
    pub batch_factories: B::Factories,
    pub trace_factories: <T::KeyBatch<B::Key, B::R> as BatchReader>::Factories,
}

impl<T: Timestamp, B: ZSet> Clone for UpdateSetFactories<T, B> {
    fn clone(&self) -> Self {
        Self {
            batch_factories: self.batch_factories.clone(),
            trace_factories: self.trace_factories.clone(),
        }
    }
}

impl<T, B> UpdateSetFactories<T, B>
where
    T: Timestamp,
    B: ZSet,
{
    pub fn new<KType>() -> Self
    where
        KType: DBData + Erase<B::Key>,
    {
        Self {
            batch_factories: BatchReaderFactories::new::<KType, (), ZWeight>(),
            trace_factories: BatchReaderFactories::new::<KType, (), ZWeight>(),
        }
    }
}

pub struct UpsertFactories<T: Timestamp, B: IndexedZSet> {
    pub batch_factories: B::Factories,
    pub trace_factories: <T::ValBatch<B::Key, B::Val, B::R> as BatchReader>::Factories,
}

impl<T: Timestamp, B: IndexedZSet> Clone for UpsertFactories<T, B> {
    fn clone(&self) -> Self {
        Self {
            batch_factories: self.batch_factories.clone(),
            trace_factories: self.trace_factories.clone(),
        }
    }
}

impl<T, B> UpsertFactories<T, B>
where
    T: Timestamp,
    B: Batch + IndexedZSet,
{
    pub fn new<KType, VType>() -> Self
    where
        KType: DBData + Erase<B::Key>,
        VType: DBData + Erase<B::Val>,
    {
        Self {
            batch_factories: BatchReaderFactories::new::<KType, VType, ZWeight>(),
            trace_factories: BatchReaderFactories::new::<KType, VType, ZWeight>(),
        }
    }
}

impl<C, K> Stream<C, Box<DynPairs<K, DynOpt<DynUnit>>>>
where
    K: DataTrait + ?Sized,
    C: Circuit,
{
    /// Convert a stream of inserts and deletes into a stream of Z-set updates.
    ///
    /// The input stream carries changes to a set in the form of
    /// insert and delete commands.  The set semantics implies that inserting an
    /// element that already exists in the set is a no-op.  Likewise, deleting
    /// an element that is not in the set is a no-op.  This operator converts
    /// these commands into batches of updates to a Z-set, which is the input
    /// format of most DBSP operators.
    ///
    /// The operator assumes that the input vector is sorted by key.
    ///
    /// This is a stateful operator that internally maintains the trace of the
    /// collection.
    pub fn update_set<B>(
        &self,
        persistent_id: Option<&str>,
        factories: &UpdateSetFactories<<C as WithClock>::Time, B>,
    ) -> Stream<C, B>
    where
        B: ZSet<Key = K>,
    {
        let circuit = self.circuit();

        assert!(
            self.is_sharded(),
            "update_set operator applied to a non-sharded collection"
        );

        // We build the following circuit to implement the set update semantics.
        // The collection is accumulated into a trace using integrator
        // (TraceAppend + Z1Trace = integrator).  The `Upsert` operator
        // evaluates each command in the input stream against the trace
        // and computes a batch of updates to be added to the trace.
        //
        // ```text
        //                          ┌────────────────────────────►
        //                          │
        //                          │
        //  self        ┌──────┐    │        ┌───────────┐  trace
        // ────────────►│Upsert├────┴───────►│TraceAppend├────┐
        //              └──────┘   delta     └───────────┘    │
        //                 ▲                  ▲               │
        //                 │                  │               │
        //                 │                  │   ┌───────┐   │
        //                 └──────────────────┴───┤Z1Trace│◄──┘
        //                    z1trace             └───────┘
        // ```
        circuit.region("update_set", || {
            let bounds = <TraceBounds<K, DynUnit>>::unbounded();

            let (delayed_trace, z1feedback) = circuit.add_feedback_persistent(
                persistent_id
                    .map(|name| format!("{name}.integral"))
                    .as_deref(),
                Z1Trace::new(
                    &factories.trace_factories,
                    &factories.batch_factories,
                    false,
                    circuit.root_scope(),
                    bounds.clone(),
                ),
            );
            delayed_trace.mark_sharded();

            let delta = circuit
                .add_binary_operator(
                    <Upsert<KeySpine<B, C>, B>>::new(&factories.batch_factories, bounds.clone()),
                    &delayed_trace,
                    self,
                )
                .mark_distinct();
            delta.mark_sharded();
            let replay_stream = z1feedback.operator_mut().prepare_replay_stream(&delta);

            let trace = circuit.add_binary_operator_with_preference(
                <TraceAppend<KeySpine<B, C>, B, C>>::new(
                    &factories.trace_factories,
                    circuit.clone(),
                ),
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
}

impl<C, K, V> Stream<C, Box<DynPairs<K, DynOpt<V>>>>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    C: Circuit,
{
    /// Convert a stream of upserts into a stream of updates.
    ///
    /// The input stream carries changes to a key/value map in the form of
    /// _upserts_.  An upsert assigns a new value to a key (or `None` to
    /// remove the key from the map) without explicitly removing the old
    /// value, if any.  Upserts are produced by some operators, including
    /// [`Stream::aggregate`].  The operator converts upserts
    /// into batches of updates, which is the input format of most DBSP
    /// operators.
    ///
    /// The operator assumes that the input vector is sorted by key and contains
    /// exactly one value per key.
    ///
    /// This is a stateful operator that internally maintains the trace of the
    /// collection.
    pub fn upsert<B>(
        &self,
        persistent_id: Option<&str>,
        factories: &UpsertFactories<<C as WithClock>::Time, B>,
    ) -> Stream<C, B>
    where
        B: IndexedZSet<Key = K, Val = V>,
    {
        let circuit = self.circuit();

        assert!(
            self.is_sharded(),
            "upsert operator applied to a non-sharded collection"
        );

        // We build the following circuit to implement the upsert semantics.
        // The collection is accumulated into a trace using integrator
        // (UntimedTraceAppend + Z1Trace = integrator).  The `Upsert` operator
        // evaluates each upsert command in the input stream against the trace
        // and computes a batch of updates to be added to the trace.
        //
        // ```text
        //                          ┌────────────────────────────►
        //                          │
        //                          │
        //  self        ┌──────┐    │        ┌───────────┐  trace
        // ────────────►│Upsert├────┴───────►│TraceAppend├────┐
        //              └──────┘   delta     └───────────┘    │
        //                 ▲                  ▲               │
        //                 │                  │               │
        //                 │                  │   ┌───────┐   │
        //                 └──────────────────┴───┤Z1Trace│◄──┘
        //                    z1trace             └───────┘
        // ```
        circuit.region("upsert", || {
            let bounds = <TraceBounds<K, V>>::unbounded();

            let (delayed_trace, z1feedback) = circuit.add_feedback_persistent(
                persistent_id
                    .map(|name| format!("{name}.integral"))
                    .as_deref(),
                Z1Trace::new(
                    &factories.trace_factories,
                    &factories.batch_factories,
                    false,
                    circuit.root_scope(),
                    bounds.clone(),
                ),
            );
            delayed_trace.mark_sharded();

            let delta = circuit
                .add_binary_operator(
                    <Upsert<ValSpine<B, C>, B>>::new(&factories.batch_factories, bounds.clone()),
                    &delayed_trace,
                    self,
                )
                .mark_distinct();
            delta.mark_sharded();
            let replay_stream = z1feedback.operator_mut().prepare_replay_stream(&delta);

            let trace = circuit.add_binary_operator_with_preference(
                <TraceAppend<ValSpine<B, C>, B, C>>::new(
                    &factories.trace_factories,
                    circuit.clone(),
                ),
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
}

pub struct Upsert<T, B>
where
    B: Batch,
    T: BatchReader,
{
    batch_factories: B::Factories,
    time: T::Time,
    bounds: TraceBounds<T::Key, T::Val>,
    phantom: PhantomData<B>,
}

impl<T, B> Upsert<T, B>
where
    B: Batch,
    T: BatchReader,
{
    pub fn new(batch_factories: &B::Factories, bounds: TraceBounds<T::Key, T::Val>) -> Self {
        Self {
            batch_factories: batch_factories.clone(),
            time: <T::Time as Timestamp>::clock_start(),
            bounds,
            phantom: PhantomData,
        }
    }
}

impl<T, B> Operator for Upsert<T, B>
where
    T: BatchReader,
    B: Batch,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("Upsert")
    }
    fn clock_end(&mut self, scope: Scope) {
        self.time = self.time.advance(scope + 1);
    }
    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<T, B> BinaryOperator<T, Box<DynPairs<T::Key, DynOpt<T::Val>>>, B> for Upsert<T, B>
where
    T: ZTrace,
    B: IndexedZSet<Key = T::Key, Val = T::Val>,
{
    #[trace]
    async fn eval(&mut self, trace: &T, updates: &Box<DynPairs<T::Key, DynOpt<T::Val>>>) -> B {
        // Inputs must be sorted by key
        debug_assert!(updates.is_sorted_by(&|u1, u2| u1.fst().cmp(u2.fst())));

        // ... and contain a single update per key.
        // TODO: implement this check.
        // debug_assert!(updates
        //     .windows(2)
        //     .all(|upserts| upserts[0].0 != upserts[1].0));

        let mut key_updates = self.batch_factories.weighted_items_factory().default_box();
        let mut item = self.batch_factories.weighted_item_factory().default_box();

        let mut trace_cursor = trace.cursor();

        let builder = B::Builder::with_capacity(&self.batch_factories, updates.len() * 2);
        let mut builder = TupleBuilder::new(&self.batch_factories, builder);

        let val_filter = self.bounds.effective_val_filter();
        let key_filter = self.bounds.effective_key_filter();

        for kv in updates.dyn_iter() {
            let (key, val) = kv.split();

            if let Some(key_filter) = &key_filter {
                if !(key_filter.filter_func())(key) {
                    continue;
                }
            }

            if let Some(val) = val.get() {
                if let Some(val_filter) = &val_filter {
                    if !(val_filter.filter_func())(val) {
                        continue;
                    }
                }

                let (kv, weight) = item.split_mut();
                let (k, v) = kv.split_mut();

                key.clone_to(k);
                val.clone_to(v);
                **weight = HasOne::one();

                key_updates.push_val(&mut *item);
            }

            if trace_cursor.seek_key_exact(key) {
                // println!("{}: found key in trace_cursor", Runtime::worker_index());
                while trace_cursor.val_valid() {
                    let mut weight = ZWeight::zero();
                    trace_cursor.map_times(&mut |t, w| {
                        if t.less_equal(&self.time) {
                            weight.add_assign_by_ref(w);
                        };
                    });

                    if !weight.is_zero() {
                        let (kv, w) = item.split_mut();
                        let (k, v) = kv.split_mut();

                        key.clone_to(k);
                        trace_cursor.val().clone_to(v);
                        **w = weight.neg();

                        key_updates.push_val(&mut *item);
                    }

                    trace_cursor.step_val();
                }
            }

            key_updates.consolidate();
            builder.extend(key_updates.dyn_iter_mut());
            key_updates.clear();
        }

        self.time = self.time.advance(0);
        builder.done()
    }

    fn input_preference(&self) -> (OwnershipPreference, OwnershipPreference) {
        (
            OwnershipPreference::PREFER_OWNED,
            OwnershipPreference::PREFER_OWNED,
        )
    }
}
