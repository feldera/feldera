use crate::{
    algebra::{AddAssignByRef, HasOne, HasZero, PartialOrder, ZRingValue},
    circuit::{
        operator_traits::{BinaryOperator, Operator},
        OwnershipPreference, Scope, WithClock,
    },
    operator::trace::{
        DelayedTraceId, KeySpine, TraceAppend, TraceBounds, TraceId, ValSpine, Z1Trace,
    },
    trace::{consolidation::consolidate, cursor::Cursor, Batch, BatchReader, Builder, Trace},
    utils::VecExt,
    Circuit, DBData, DBTimestamp, Stream, Timestamp,
};
use std::{borrow::Cow, marker::PhantomData, ops::Neg};

impl<C, K> Stream<C, Vec<(K, Option<()>)>>
where
    C: Circuit,
    <C as WithClock>::Time: DBTimestamp,
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
    pub fn update_set<B>(&self) -> Stream<C, B>
    where
        K: DBData,
        B::R: DBData + ZRingValue,
        B: Batch<Key = K, Val = (), Time = ()>,
    {
        let circuit = self.circuit();

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
            let bounds = <TraceBounds<K, ()>>::unbounded();

            let (local, z1feedback) = circuit.add_feedback(Z1Trace::new(
                false,
                circuit.root_scope(),
                bounds.clone(),
                self.origin_node_id().persistent_id(),
            ));
            local.mark_sharded_if(self);

            let delta = circuit
                .add_binary_operator(
                    <Upsert<KeySpine<B, C>, B>>::new(bounds.clone()),
                    &local,
                    &self.try_sharded_version(),
                )
                .mark_distinct();
            delta.mark_sharded_if(self);

            let trace = circuit.add_binary_operator_with_preference(
                <TraceAppend<KeySpine<B, C>, B, C>>::new(circuit.clone()),
                (&local, OwnershipPreference::STRONGLY_PREFER_OWNED),
                (
                    &delta.try_sharded_version(),
                    OwnershipPreference::PREFER_OWNED,
                ),
            );
            trace.mark_sharded_if(self);

            z1feedback.connect_with_preference(&trace, OwnershipPreference::STRONGLY_PREFER_OWNED);
            circuit.cache_insert(DelayedTraceId::new(trace.origin_node_id().clone()), local);
            circuit.cache_insert(
                TraceId::new(delta.origin_node_id().clone()),
                (trace, bounds),
            );
            delta
        })
    }
}

impl<C, K, V> Stream<C, Vec<(K, Option<V>)>>
where
    C: Circuit,
    <C as WithClock>::Time: DBTimestamp,
{
    /// Convert a stream of upserts into a stream of updates.
    ///
    /// The input stream carries changes to a key/value map in the form of
    /// _upserts_.  An upsert assigns a new value to a key (or `None` to
    /// remove the key from the map) without explicitly removing the old
    /// value, if any.  Upserts are produced by some operators
    /// or arrive from external data sources via
    /// [`UpsertHandle`](`crate::UpsertHandle`)s.  The operator converts upserts
    /// into batches of updates, which is the input format of most DBSP
    /// operators.
    ///
    /// The operator assumes that the input vector is sorted by key and contains
    /// exactly one value per key.
    ///
    /// This is a stateful operator that internally maintains the trace of the
    /// collection.
    pub fn upsert<B>(&self) -> Stream<C, B>
    where
        K: DBData,
        V: DBData,
        B::R: DBData + ZRingValue,
        B: Batch<Key = K, Val = V, Time = ()>,
    {
        let circuit = self.circuit();

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

            let (local, z1feedback) = circuit.add_feedback(Z1Trace::new(
                false,
                circuit.root_scope(),
                bounds.clone(),
                self.origin_node_id().persistent_id(),
            ));
            local.mark_sharded_if(self);

            let delta = circuit
                .add_binary_operator(
                    <Upsert<ValSpine<B, C>, B>>::new(bounds.clone()),
                    &local,
                    &self.try_sharded_version(),
                )
                .mark_distinct();
            delta.mark_sharded_if(self);

            let trace = circuit.add_binary_operator_with_preference(
                <TraceAppend<ValSpine<B, C>, B, C>>::new(circuit.clone()),
                (&local, OwnershipPreference::STRONGLY_PREFER_OWNED),
                (
                    &delta.try_sharded_version(),
                    OwnershipPreference::PREFER_OWNED,
                ),
            );
            trace.mark_sharded_if(self);

            z1feedback.connect_with_preference(&trace, OwnershipPreference::STRONGLY_PREFER_OWNED);
            circuit.cache_insert(DelayedTraceId::new(trace.origin_node_id().clone()), local);
            circuit.cache_insert(
                TraceId::new(delta.origin_node_id().clone()),
                (trace, bounds),
            );
            delta
        })
    }
}

pub struct Upsert<T, B>
where
    T: BatchReader,
{
    time: T::Time,
    bounds: TraceBounds<T::Key, T::Val>,
    phantom: PhantomData<B>,
}

impl<T, B> Upsert<T, B>
where
    T: BatchReader,
{
    pub fn new(bounds: TraceBounds<T::Key, T::Val>) -> Self {
        Self {
            time: T::Time::clock_start(),
            bounds,
            phantom: PhantomData,
        }
    }
}

impl<T, B> Operator for Upsert<T, B>
where
    T: BatchReader,
    B: 'static,
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

impl<T, B> BinaryOperator<T, Vec<(T::Key, Option<T::Val>)>, B> for Upsert<T, B>
where
    T: Trace,
    T::R: ZRingValue,
    B: Batch<Key = T::Key, Val = T::Val, Time = (), R = T::R>,
{
    fn eval(&mut self, trace: &T, updates: &Vec<(T::Key, Option<T::Val>)>) -> B {
        // Inputs must be sorted by key
        debug_assert!(updates.is_sorted_by(|(k1, _), (k2, _)| k1.partial_cmp(k2)));
        // ... and contain a single update per key.
        debug_assert!(updates
            .windows(2)
            .all(|upserts| upserts[0].0 != upserts[1].0));
        let mut trace_cursor = trace.cursor();

        let mut builder = B::Builder::with_capacity((), updates.len() * 2);
        let mut key_updates: Vec<(T::Val, T::R)> = Vec::new();

        let val_filter = self.bounds.effective_val_filter();
        let key_filter = self.bounds.key_filter();
        let key_bound = self.bounds.effective_key_bound();

        for (key, val) in updates {
            if let Some(key_bound) = &key_bound {
                if key < key_bound {
                    continue;
                }
            }

            if let Some(key_filter) = &key_filter {
                if !key_filter(key) {
                    continue;
                }
            }

            if let Some(val) = val {
                if let Some(val_filter) = &val_filter {
                    if !val_filter(val) {
                        continue;
                    }
                }

                key_updates.push((val.clone(), HasOne::one()));
            }

            trace_cursor.seek_key(key);

            if trace_cursor.key_valid() && trace_cursor.key() == key {
                // println!("{}: found key in trace_cursor", Runtime::worker_index());
                while trace_cursor.val_valid() {
                    let mut weight = T::R::zero();
                    trace_cursor.map_times(|t, w| {
                        if t.less_equal(&self.time) {
                            weight.add_assign_by_ref(w);
                        };
                    });

                    if !weight.is_zero() {
                        key_updates.push((trace_cursor.val().clone(), weight.neg()));
                    }

                    trace_cursor.step_val();
                }
            }

            consolidate(&mut key_updates);
            builder.extend(
                key_updates
                    .drain(..)
                    .map(|(val, w)| (B::item_from(key.clone(), val), w)),
            )
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
