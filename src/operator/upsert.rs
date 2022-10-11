use crate::{
    algebra::{AddAssignByRef, HasOne, HasZero, PartialOrder, ZRingValue},
    circuit::{
        operator_traits::{BinaryOperator, Operator},
        ExportId, ExportStream, OwnershipPreference, Scope,
    },
    operator::trace::{DelayedTraceId, TraceAppend, TraceId, Z1Trace},
    trace::{
        consolidation::consolidate, cursor::Cursor, spine_fueled::Spine, Batch, BatchReader,
        Builder, Trace,
    },
    utils::VecExt,
    Circuit, DBData, Stream, Timestamp,
};
use std::{borrow::Cow, marker::PhantomData, ops::Neg};

impl<P, K, V> Stream<Circuit<P>, Vec<(K, Option<V>)>>
where
    P: Clone + 'static,
{
    /// Convert a stream of upserts into a stream of updates.
    ///
    /// The input stream carries changes to a key/value map in the form of
    /// _upserts_.  An upsert assigns a new value to a key (or `None` to
    /// remove the key fro the map) without explicitly removing the old
    /// value, if any.  Upserts are produced by some operators
    /// or arrive from external data sources via
    /// [`UpsertHandle`](`crate::UpsertHandle`)s.  The operator converts upserts
    /// into batches of updates, which is the input format of most DBSP
    /// operators.
    ///
    /// The operator assumes that the input vector is sorted by key and contains
    /// exactly one value per key.
    ///
    /// This is a stateful operator that internaly maintains the trace of the
    /// collection.
    // TODO: Derive TS from circuit.
    pub fn upsert<TS, B>(&self) -> Stream<Circuit<P>, B>
    where
        K: DBData,
        V: DBData,
        B::R: DBData + ZRingValue,
        TS: DBData + Timestamp,
        B: Batch<Key = K, Val = V, Time = ()> + 'static,
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
            let (ExportStream { local, export }, z1feedback) =
                circuit.add_feedback_with_export(Z1Trace::new(false, circuit.root_scope()));
            local.mark_sharded_if(self);

            let delta = circuit.add_binary_operator(
                <Upsert<Spine<TS::OrdValBatch<K, V, B::R>>, B>>::new(),
                &local,
                &self.try_sharded_version(),
            );
            delta.mark_sharded_if(self);

            let trace = circuit.add_binary_operator_with_preference(
                <TraceAppend<Spine<TS::OrdValBatch<K, V, B::R>>, B>>::new(),
                (&local, OwnershipPreference::STRONGLY_PREFER_OWNED),
                (
                    &delta.try_sharded_version(),
                    OwnershipPreference::PREFER_OWNED,
                ),
            );
            trace.mark_sharded_if(self);

            z1feedback.connect_with_preference(&trace, OwnershipPreference::STRONGLY_PREFER_OWNED);
            circuit.cache_insert(DelayedTraceId::new(trace.origin_node_id().clone()), local);
            circuit.cache_insert(ExportId::new(trace.origin_node_id().clone()), export);
            circuit.cache_insert(TraceId::new(delta.origin_node_id().clone()), trace);
            delta
        })
    }
}

pub struct Upsert<T, B>
where
    T: BatchReader,
{
    time: T::Time,
    phantom: PhantomData<B>,
}

impl<T, B> Upsert<T, B>
where
    T: BatchReader,
{
    pub fn new() -> Self {
        Self {
            time: T::Time::clock_start(),
            phantom: PhantomData,
        }
    }
}

impl<T, B> Default for Upsert<T, B>
where
    T: BatchReader,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T, B> Operator for Upsert<T, B>
where
    T: BatchReader + 'static,
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
    T: Trace + 'static,
    T::R: ZRingValue,
    B: Batch<Key = T::Key, Val = T::Val, Time = (), R = T::R> + 'static,
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

        for (key, val) in updates {
            if let Some(val) = val {
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
