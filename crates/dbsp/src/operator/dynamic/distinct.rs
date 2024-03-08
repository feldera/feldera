//! Distinct operator.

use crate::{
    algebra::{
        AddByRef, HasOne, HasZero, IndexedZSet, IndexedZSetReader, Lattice, OrdIndexedZSet,
        OrdIndexedZSetFactories, PartialOrder, ZRingValue, ZTrace,
    },
    circuit::{
        metadata::{MetaItem, OperatorMeta, SHARED_BYTES_LABEL, USED_BYTES_LABEL},
        operator_traits::{BinaryOperator, Operator, UnaryOperator},
        Circuit, GlobalNodeId, Scope, Stream, WithClock,
    },
    circuit_cache_key,
    dynamic::{DynPair, DynWeightedPairs, Erase},
    trace::{Batch, BatchFactories, BatchReader, BatchReaderFactories, Builder, Cursor},
    DBData, Timestamp, ZWeight,
};
use size_of::SizeOf;
use std::{
    borrow::Cow,
    cmp::{min, Ordering},
    collections::BTreeMap,
    marker::PhantomData,
    ops::Neg,
};

circuit_cache_key!(DistinctId<C, D>(GlobalNodeId => Stream<C, D>));
circuit_cache_key!(DistinctIncrementalId<C, D>(GlobalNodeId => Stream<C, D>));

pub struct DistinctFactories<Z: IndexedZSet, T: Timestamp> {
    pub input_factories: Z::Factories,
    trace_factories: <T::OrdValBatch<Z::Key, Z::Val, Z::R> as BatchReader>::Factories,
    aux_factories: OrdIndexedZSetFactories<Z::Key, Z::Val>,
}

impl<Z: IndexedZSet, T: Timestamp> Clone for DistinctFactories<Z, T> {
    fn clone(&self) -> Self {
        Self {
            input_factories: self.input_factories.clone(),
            trace_factories: self.trace_factories.clone(),
            aux_factories: self.aux_factories.clone(),
        }
    }
}

impl<Z, T> DistinctFactories<Z, T>
where
    Z: IndexedZSet,
    T: Timestamp,
{
    pub fn new<KType, VType>() -> Self
    where
        KType: DBData + Erase<Z::Key>,
        VType: DBData + Erase<Z::Val>,
    {
        Self {
            input_factories: BatchReaderFactories::new::<KType, VType, ZWeight>(),
            trace_factories: BatchReaderFactories::new::<KType, VType, ZWeight>(),
            aux_factories: BatchReaderFactories::new::<KType, VType, ZWeight>(),
        }
    }
}

impl<C, D> Stream<C, D>
where
    C: Circuit,
    D: 'static,
{
    /// Marks the data within the current stream as distinct, meaning that all
    /// further calls to `.distinct()` will have no effect.
    ///
    /// This must only be used on streams whose integral contain elements with
    /// unit weights only, otherwise this will cause the dataflow to yield
    /// incorrect results
    pub fn mark_distinct(&self) -> Self {
        self.circuit().cache_insert(
            DistinctIncrementalId::new(self.origin_node_id().clone()),
            self.clone(),
        );
        self.clone()
    }

    /// Returns `true` if a distinct version of the current stream exists
    pub fn has_distinct_version(&self) -> bool {
        self.circuit()
            .cache_contains(&DistinctIncrementalId::<C, D>::new(
                self.origin_node_id().clone(),
            ))
    }

    /// Returns the distinct version of the stream if it exists
    /// Otherwise, returns `self`.
    pub fn try_distinct_version(&self) -> Self {
        self.circuit()
            .cache_get(&DistinctIncrementalId::new(self.origin_node_id().clone()))
            .unwrap_or_else(|| self.clone())
    }

    /// Marks `self` as distinct if `input` has a distinct version of itself
    pub fn mark_distinct_if<C2, D2>(&self, input: &Stream<C2, D2>)
    where
        C2: Circuit,
        D2: 'static,
    {
        if input.has_distinct_version() {
            self.mark_distinct();
        }
    }
}

impl<C, Z> Stream<C, Z>
where
    C: Circuit,
{
    /// See [`Stream::stream_distinct`].
    pub fn dyn_stream_distinct(&self, input_factories: &Z::Factories) -> Stream<C, Z>
    where
        Z: IndexedZSet + Send,
    {
        let stream = self.dyn_shard(input_factories);

        self.circuit()
            .cache_get_or_insert_with(DistinctId::new(stream.origin_node_id().clone()), || {
                self.circuit()
                    .add_unary_operator(Distinct::new(), &stream)
                    .mark_sharded()
            })
            .clone()
    }

    /// See [`Stream::distinct`].
    pub fn dyn_distinct(&self, factories: &DistinctFactories<Z, C::Time>) -> Stream<C, Z>
    where
        Z: IndexedZSet + Send,
    {
        let circuit = self.circuit();
        let stream = self.dyn_shard(&factories.input_factories);

        circuit
            .cache_get_or_insert_with(
                DistinctIncrementalId::new(stream.origin_node_id().clone()),
                || {
                    circuit.region("distinct", || {
                        if circuit.root_scope() == 0 {
                            // Use an implementation optimized to work in the root scope.
                            circuit.add_binary_operator(
                                DistinctIncrementalTotal::new(&factories.input_factories),
                                &stream,
                                &stream
                                    .dyn_integrate_trace(&factories.input_factories)
                                    .delay_trace(),
                            )
                        } else {
                            // ```
                            //          ┌────────────────────────────────────┐
                            //          │                                    │
                            //          │                                    ▼
                            //  stream  │     ┌─────┐  stream.trace()  ┌───────────────────┐
                            // ─────────┴─────┤trace├─────────────────►│DistinctIncremental├─────►
                            //                └─────┘                  └───────────────────┘
                            // ```
                            circuit.add_binary_operator(
                                DistinctIncremental::new(
                                    &factories.input_factories,
                                    &factories.aux_factories,
                                    circuit.clone(),
                                ),
                                &stream,
                                // TODO use OrdIndexedZSetSpine if `Z::Val = ()`
                                &stream.dyn_trace(&factories.trace_factories),
                            )
                        }
                        .mark_sharded()
                    })
                },
            )
            .clone()
    }
}

/// `Distinct` operator changes all weights in the support of a Z-set to 1.
pub struct Distinct<Z> {
    _type: PhantomData<Z>,
}

impl<Z> Distinct<Z> {
    pub fn new() -> Self {
        Self { _type: PhantomData }
    }
}

impl<Z> Default for Distinct<Z> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Z> Operator for Distinct<Z>
where
    Z: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("Distinct")
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<Z> UnaryOperator<Z, Z> for Distinct<Z>
where
    Z: IndexedZSet,
{
    fn eval(&mut self, input: &Z) -> Z {
        input.distinct()
    }

    fn eval_owned(&mut self, input: Z) -> Z {
        input.distinct_owned()
    }
}

/// Incremental version of the distinct operator that only works in the
/// top-level scope (i.e., for totally ordered timestamps).
///
/// Takes a stream `a` of changes to relation `A` and a stream with delayed
/// value of `A`: `z^-1(A) = a.integrate().delay()` and computes
/// `distinct(A) - distinct(z^-1(A))` incrementally, by only considering
/// values in the support of `a`.
struct DistinctIncrementalTotal<Z: IndexedZSet, I> {
    input_factories: Z::Factories,
    _type: PhantomData<(Z, I)>,
}

impl<Z: IndexedZSet, I> DistinctIncrementalTotal<Z, I> {
    pub fn new(input_factories: &Z::Factories) -> Self {
        Self {
            input_factories: input_factories.clone(),
            _type: PhantomData,
        }
    }
}

impl<Z, I> Operator for DistinctIncrementalTotal<Z, I>
where
    Z: IndexedZSet,
    I: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("DistinctIncrementalTotal")
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<Z, I> BinaryOperator<Z, I, Z> for DistinctIncrementalTotal<Z, I>
where
    Z: IndexedZSet,
    I: IndexedZSetReader<Key = Z::Key, Val = Z::Val>,
{
    fn eval(&mut self, delta: &Z, delayed_integral: &I) -> Z {
        let mut builder = Z::Builder::with_capacity(&self.input_factories, (), delta.len());
        let mut delta_cursor = delta.cursor();
        let mut integral_cursor = delayed_integral.cursor();

        while delta_cursor.key_valid() {
            integral_cursor.seek_key(delta_cursor.key());

            if integral_cursor.key_valid() && integral_cursor.key() == delta_cursor.key() {
                while delta_cursor.val_valid() {
                    let w = **delta_cursor.weight();
                    let v = delta_cursor.val();

                    integral_cursor.seek_val(v);
                    let old_weight = if integral_cursor.val_valid() && integral_cursor.val() == v {
                        **integral_cursor.weight()
                    } else {
                        HasZero::zero()
                    };

                    let new_weight = old_weight.add_by_ref(&w);

                    if old_weight.le0() {
                        // Weight changes from non-positive to positive.
                        if new_weight.ge0() && !new_weight.is_zero() {
                            builder.push_refs(delta_cursor.key(), v, ZWeight::one().erase());
                        }
                    } else if new_weight.le0() {
                        // Weight changes from positive to non-positive.
                        builder.push_refs(delta_cursor.key(), v, ZWeight::one().neg().erase());
                    }

                    delta_cursor.step_val();
                }
            } else {
                while delta_cursor.val_valid() {
                    let new_weight = **delta_cursor.weight();

                    if new_weight.ge0() && !new_weight.is_zero() {
                        builder.push_refs(
                            delta_cursor.key(),
                            delta_cursor.val(),
                            ZWeight::one().erase(),
                        );
                    }
                    delta_cursor.step_val();
                }
            };

            delta_cursor.step_key();
        }

        builder.done()
    }

    // TODO: owned implementation.
    fn eval_owned_and_ref(&mut self, delta: Z, delayed_integral: &I) -> Z {
        self.eval(&delta, delayed_integral)
    }

    fn eval_owned(&mut self, delta: Z, delayed_integral: I) -> Z {
        self.eval_owned_and_ref(delta, &delayed_integral)
    }
}

/// Track key/value pairs that must be recomputed at
/// future times.  Represent them as `((key, value), Present)`
/// tuples so we can use the `Batcher` API to compile them
/// into a batch.
type KeysOfInterest<TS, K, V, R> = BTreeMap<TS, Box<DynWeightedPairs<DynPair<K, V>, R>>>;

#[derive(SizeOf)]
struct DistinctIncremental<Z, T, Clk>
where
    Z: IndexedZSet,
    T: ZTrace<Key = Z::Key, Val = Z::Val>,
{
    #[size_of(skip)]
    input_factories: Z::Factories,
    #[size_of(skip)]
    aux_factories: OrdIndexedZSetFactories<Z::Key, Z::Val>,
    #[size_of(skip)]
    clock: Clk,
    // Keys that may need updating at future times.
    keys_of_interest: KeysOfInterest<T::Time, Z::Key, Z::Val, Z::R>,
    // True if the operator received empty input during the last clock
    // tick.
    empty_input: bool,
    // True if the operator produced empty output at the last clock tick.
    empty_output: bool,
    // Used in computing partial derivatives
    // (we keep it here to reuse allocations across `eval_keyval` calls).
    distinct_vals: Vec<(Option<T::Time>, ZWeight)>,
    _type: PhantomData<(Z, T)>,
}

impl<Z, T, Clk> DistinctIncremental<Z, T, Clk>
where
    Z: IndexedZSet,
    T: ZTrace<Key = Z::Key, Val = Z::Val>,
    Clk: WithClock<Time = T::Time>,
{
    fn new(
        input_factories: &Z::Factories,
        aux_factories: &OrdIndexedZSetFactories<Z::Key, Z::Val>,
        clock: Clk,
    ) -> Self {
        let depth = clock.nesting_depth();

        Self {
            input_factories: input_factories.clone(),
            aux_factories: aux_factories.clone(),
            clock,
            keys_of_interest: BTreeMap::new(),
            empty_input: false,
            empty_output: false,
            distinct_vals: vec![(None, HasZero::zero()); 2 << depth],
            _type: PhantomData,
        }
    }

    /// Compute the output of the operator for a single key/value pair.
    ///
    /// # Theory
    ///
    /// According to the definition of a lifted incremental operator, we are
    /// computing a partial derivative:
    ///
    /// ```text
    ///     ∂^n                      ∂^(n-1)                      ∂^(n-1)
    ///  ------------f(t1,..,tn) = ------------f(t1,..,tn)  -  ------------f(t1-1,..,tn).
    ///  ∂_t1 .. ∂_tn              ∂_t2 .. ∂_tn                ∂_t2 .. ∂_tn
    /// ```
    ///
    /// where `n` is the nesting level of the circuit, i.e., 1 for the top-level
    /// circuit, 2 for its subcircuit, etc., `ti` is the i'th component of
    /// the n-dimensional timestamp, and `f(t1..tn)` is the value of the
    /// function we want to compute (in this case `distinct`) at time
    /// `t1..tn`.
    ///
    /// For example, for a twice nested circuit, we get:
    ///
    /// ```text
    ///      ∂^3                        ∂^2                         ∂^2
    ///  --------------f(t1,t2,t3) = -----------f(t1,t2,t3)  -  -----------f(t1-1,t2,t3) =
    ///  ∂_t1 ∂_t2 ∂_t3               ∂_t2 ∂_t3                  ∂_t2 ∂_t3
    ///
    ///   ┌─                                     ─┐   ┌─                                          ─┐
    ///   │   ∂                   ∂               │   │   ∂                    ∂                   │
    ///   │ -----f(t1,t2,t3)  - -----f(t1,t2-1,t3)│ - │ -----f(t1-1,t2,t3)  - -----f(t1-1,t2-1,t3) │ =
    /// = │ ∂_t3                 ∂_t3             │   │ ∂_t3                  ∂_t3                 │
    ///   └─                                     ─┘   └─                                          ─┘
    ///
    /// = ((f(t1,t2,t3) - f(t1,t2,t3-1)) - (f(t1,t2-1,t2) - f(t1,t2-1,t3-1))) -
    ///   ((f(t1-1,t2,t3) - f(t1-1,t2,t3-1)) - (f(t1-1,t2-1,t2) - f(t1-1,t2-1,t3-1))).
    /// ```
    ///
    /// In general, in order to compute the partial derivative, we need to know
    /// the values of `f` for all times within the n-dimensional cube with
    /// edge 1 of the current time `t1..tn` (i.e., all `2^n` possible
    /// combinations of `ti`'s and `ti-1`'s).
    ///
    /// Computing the value of `f` at time `t`, in turn, requires first
    /// computing the sum of all updates with time `t' < t` in the input
    /// stream:
    ///
    /// ```text
    ///              __
    ///              \
    /// f(t) =dist(  / stream[t'][k][v].weight )
    ///              --
    ///              t'<=t
    /// ```
    ///
    /// where `stream[t'][k][v].weight` is pseudocode for selecting the weight
    /// of the update at time `t'` for key `k` and value `v`, and
    ///
    /// ```text
    ///            ┌
    ///            │  1, if w > 0
    /// dist(w) = <
    ///            │  0, otherwise
    ///            └
    /// ```
    ///
    /// We compute `f(t)` for all `2^n` times `t` of interest simultaneously in
    /// a single run of the cursor by maintaining an array of `2^n`
    /// accumulators and updating a subset of them at each step.
    ///
    /// # `keys_of_interest` map.
    ///
    /// The `distinct` operator does not have nice properties like linearity
    /// that help with incremental evaluation: computing an update for each
    /// key/value pair requires inspecting the entire history of updates for
    /// this pair.  Fortunately, we do not need to look at all key/value pairs
    /// in the trace.  Specifically, a key/value pair `(k, v)` can only
    /// appear in the output of the operator at time `t` if it satisfies one
    /// of the following conditions:
    /// * `(k,v)` occurs in the current input `delta`
    /// * there exist `t1<t` and `t2<t` such that `t = t1 /\ t2` and `(k,v)`
    ///   occurs in the inputs of the operator at times `t1` and `t2`.
    ///
    /// To efficiently compute tuples that satisfy the second condition, we use
    /// the `keys_of_interest` map.  For each `(k,v)` observed at time `t1`
    /// we lookup the smallest time `t'` such that `t' = t1 /\ t2` for some
    /// `t2` such that `not (t2 <= t1)` at which we've encountered `(k,v)`
    /// and record `(k,v)` in `keys_of_interest[t']`.  When evaluating the
    /// operator at time `t'` we simply scan all tuples in
    /// `keys_of_interest[t2]` for candidates.
    fn eval_keyval(
        &mut self,
        time: &Clk::Time,
        key: &Z::Key,
        val: &Z::Val,
        trace_cursor: &mut T::Cursor<'_>,
        output: &mut Z::Builder,
        item: &mut DynPair<DynPair<Z::Key, Z::Val>, Z::R>,
    ) {
        if trace_cursor.key_valid() && trace_cursor.key() == key {
            trace_cursor.seek_val(val);

            if trace_cursor.val_valid() && trace_cursor.val() == val {
                // The nearest future timestamp when we need to update this
                // key/value pair.
                let mut time_of_interest = None;

                // Reset all counters to 0.
                self.clear_distinct_vals();

                trace_cursor.map_times(&mut |trace_ts, weight| {
                    // Update weights in `distinct_vals`.
                    for (ts, total_weight) in self.distinct_vals.iter_mut() {
                        if let Some(ts) = ts {
                            if trace_ts.less_equal(ts) {
                                *total_weight += **weight;
                            }
                        }
                    }
                    // Timestamp in the future - update `time_of_interest`.
                    if !trace_ts.less_equal(time) {
                        time_of_interest = match time_of_interest.take() {
                            None => Some(time.join(trace_ts)),
                            Some(time_of_interest) => {
                                Some(min(time_of_interest, time.join(trace_ts)))
                            }
                        }
                    }
                });

                // Compute `dist` for each entry in `distinct_vals`.
                for (_time, weight) in self.distinct_vals.iter_mut() {
                    if weight.le0() {
                        *weight = HasZero::zero();
                    } else {
                        *weight = HasOne::one();
                    }
                }

                // We have computed `f` at all the relevant points in times; we can now
                // compute the partial derivative.
                let output_weight = Self::partial_derivative(&self.distinct_vals);
                if !output_weight.is_zero() {
                    output.push_refs(key, val, output_weight.erase());
                }

                let (kv, weight) = item.split_mut();
                **weight = HasOne::one();
                kv.from_refs(key, val);

                if let Some(t) = time_of_interest {
                    self.keys_of_interest
                        .entry(t)
                        .or_insert_with(|| {
                            self.aux_factories.weighted_items_factory().default_box()
                        })
                        .push_val(&mut *item);
                }
            }
        }
    }

    fn init_distinct_vals(vals: &mut [(Option<T::Time>, ZWeight)], ts: Option<T::Time>) {
        if vals.len() == 1 {
            vals[0] = (ts, HasZero::zero());
        } else {
            let half_len = vals.len() >> 1;
            Self::init_distinct_vals(
                &mut vals[0..half_len],
                ts.as_ref()
                    .and_then(|ts| ts.checked_recede(half_len.ilog2() as Scope)),
            );
            Self::init_distinct_vals(&mut vals[half_len..], ts);
        }
    }

    fn clear_distinct_vals(&mut self) {
        for (_time, val) in self.distinct_vals.iter_mut() {
            *val = HasZero::zero();
        }
    }

    /// Compute partial derivative.
    fn partial_derivative(vals: &[(Option<T::Time>, ZWeight)]) -> ZWeight {
        // Split vals in two halves.  Compute `partial_derivative` recursively
        // for each half, return the difference.
        if vals.len() == 1 {
            vals[0].1
        } else {
            Self::partial_derivative(&vals[vals.len() >> 1..])
                + Self::partial_derivative(&vals[0..vals.len() >> 1]).neg()
        }
    }
}

impl<Z, T, Clk> Operator for DistinctIncremental<Z, T, Clk>
where
    Z: IndexedZSet,
    T: ZTrace<Key = Z::Key, Val = Z::Val>,
    Clk: WithClock<Time = T::Time> + 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("DistinctIncremental")
    }

    fn metadata(&self, meta: &mut OperatorMeta) {
        let size: usize = self.keys_of_interest.values().map(|v| v.len()).sum();
        let bytes = self.size_of();

        meta.extend(metadata! {
            "total updates" => MetaItem::bytes(size),
            USED_BYTES_LABEL => MetaItem::bytes(bytes.used_bytes()),
            "allocations" => bytes.distinct_allocations(),
            SHARED_BYTES_LABEL => MetaItem::bytes(bytes.shared_bytes()),
        });
    }

    fn clock_start(&mut self, scope: Scope) {
        if scope == 0 {
            self.empty_input = false;
            self.empty_output = false;
        }
    }

    fn clock_end(&mut self, scope: Scope) {
        debug_assert!(self.keys_of_interest.keys().all(|ts| {
            if ts.less_equal(&self.clock.time().epoch_end(scope)) {
                eprintln!(
                    "ts: {ts:?}, epoch_end: {:?}",
                    self.clock.time().epoch_end(scope)
                );
            }
            !ts.less_equal(&self.clock.time().epoch_end(scope))
        }));
    }

    fn fixedpoint(&self, scope: Scope) -> bool {
        let epoch_end = self.clock.time().epoch_end(scope);

        self.empty_input
            && self.empty_output
            && self
                .keys_of_interest
                .keys()
                .all(|ts| !ts.less_equal(&epoch_end))
    }
}

impl<Z, T, Clk> BinaryOperator<Z, T, Z> for DistinctIncremental<Z, T, Clk>
where
    Z: IndexedZSet,
    T: ZTrace<Key = Z::Key, Val = Z::Val>,
    Clk: WithClock<Time = T::Time> + 'static,
{
    // TODO: add eval_owned, so we can use keys and values from `delta` without
    // cloning.
    fn eval(&mut self, delta: &Z, trace: &T) -> Z {
        let time = self.clock.time();

        Self::init_distinct_vals(&mut self.distinct_vals, Some(time.clone()));
        self.empty_input = delta.is_empty();

        // We iterate over keys and values in order, so it is safe to use `Builder`.
        let mut result_builder = Z::Builder::with_capacity(&self.input_factories, (), delta.len());

        let mut delta_cursor = delta.cursor();
        let mut trace_cursor = trace.cursor();

        // Previously encountered keys that may affect output at the
        // current time.
        let mut keys_of_interest = self
            .keys_of_interest
            .remove(&time)
            .unwrap_or_else(|| self.aux_factories.weighted_items_factory().default_box());

        let keys_of_interest = <OrdIndexedZSet<Z::Key, Z::Val>>::dyn_from_tuples(
            &self.aux_factories,
            (),
            &mut keys_of_interest,
        );
        let mut keys_of_interest_cursor = keys_of_interest.cursor();

        let mut item = self.aux_factories.weighted_item_factory().default_box();

        // Iterate over all keys in `delta_cursor` and `keys_of_interest`.
        while delta_cursor.key_valid() && keys_of_interest_cursor.key_valid() {
            match delta_cursor.key().cmp(keys_of_interest_cursor.key()) {
                // Key only appears in `delta`.
                Ordering::Less => {
                    trace_cursor.seek_key(delta_cursor.key());

                    while delta_cursor.val_valid() {
                        self.eval_keyval(
                            &time,
                            delta_cursor.key(),
                            delta_cursor.val(),
                            &mut trace_cursor,
                            &mut result_builder,
                            &mut *item,
                        );
                        delta_cursor.step_val();
                    }
                    delta_cursor.step_key();
                }
                // Key only appears in `keys_of_interest`.
                Ordering::Greater => {
                    trace_cursor.seek_key(keys_of_interest_cursor.key());

                    while keys_of_interest_cursor.val_valid() {
                        self.eval_keyval(
                            &time,
                            keys_of_interest_cursor.key(),
                            keys_of_interest_cursor.val(),
                            &mut trace_cursor,
                            &mut result_builder,
                            &mut *item,
                        );
                        keys_of_interest_cursor.step_val();
                    }
                    keys_of_interest_cursor.step_key();
                }
                // Key appears in both `delta` and `keys_of_interest`:
                // Iterate over all values in both cursors.
                Ordering::Equal => {
                    trace_cursor.seek_key(keys_of_interest_cursor.key());

                    while delta_cursor.val_valid() && keys_of_interest_cursor.val_valid() {
                        match delta_cursor.val().cmp(keys_of_interest_cursor.val()) {
                            Ordering::Less => {
                                self.eval_keyval(
                                    &time,
                                    delta_cursor.key(),
                                    delta_cursor.val(),
                                    &mut trace_cursor,
                                    &mut result_builder,
                                    &mut *item,
                                );
                                delta_cursor.step_val();
                            }
                            Ordering::Greater => {
                                self.eval_keyval(
                                    &time,
                                    keys_of_interest_cursor.key(),
                                    keys_of_interest_cursor.val(),
                                    &mut trace_cursor,
                                    &mut result_builder,
                                    &mut *item,
                                );
                                keys_of_interest_cursor.step_val();
                            }
                            Ordering::Equal => {
                                self.eval_keyval(
                                    &time,
                                    delta_cursor.key(),
                                    delta_cursor.val(),
                                    &mut trace_cursor,
                                    &mut result_builder,
                                    &mut *item,
                                );
                                delta_cursor.step_val();
                                keys_of_interest_cursor.step_val();
                            }
                        }
                    }

                    // Iterate over remaining `delta_cursor` values.
                    while delta_cursor.val_valid() {
                        self.eval_keyval(
                            &time,
                            delta_cursor.key(),
                            delta_cursor.val(),
                            &mut trace_cursor,
                            &mut result_builder,
                            &mut *item,
                        );
                        delta_cursor.step_val();
                    }

                    // Iterate over remaining `keys_of_interest` values.
                    while keys_of_interest_cursor.val_valid() {
                        self.eval_keyval(
                            &time,
                            keys_of_interest_cursor.key(),
                            keys_of_interest_cursor.val(),
                            &mut trace_cursor,
                            &mut result_builder,
                            &mut *item,
                        );
                        keys_of_interest_cursor.step_val();
                    }

                    delta_cursor.step_key();
                    keys_of_interest_cursor.step_key();
                }
            }
        }

        // Iterate over remaining `delta_cursor` keys.
        while delta_cursor.key_valid() {
            trace_cursor.seek_key(delta_cursor.key());

            while delta_cursor.val_valid() {
                self.eval_keyval(
                    &time,
                    delta_cursor.key(),
                    delta_cursor.val(),
                    &mut trace_cursor,
                    &mut result_builder,
                    &mut *item,
                );
                delta_cursor.step_val();
            }
            delta_cursor.step_key();
        }

        // Iterate over remaining `keys_of_interest_cursor` keys.
        while keys_of_interest_cursor.key_valid() {
            trace_cursor.seek_key(keys_of_interest_cursor.key());

            while keys_of_interest_cursor.val_valid() {
                self.eval_keyval(
                    &time,
                    keys_of_interest_cursor.key(),
                    keys_of_interest_cursor.val(),
                    &mut trace_cursor,
                    &mut result_builder,
                    &mut *item,
                );
                keys_of_interest_cursor.step_val();
            }
            keys_of_interest_cursor.step_key();
        }

        let result = result_builder.done();
        self.empty_output = result.is_empty();

        result
    }
}

#[cfg(test)]
mod test {
    use anyhow::Result as AnyResult;

    use std::{
        cell::RefCell,
        rc::Rc,
        sync::{Arc, Mutex},
    };

    use crate::{
        indexed_zset,
        operator::{Generator, GeneratorNested, OutputHandle},
        typed_batch::{OrdIndexedZSet, OrdZSet},
        utils::Tup2,
        zset, Circuit, RootCircuit, Runtime,
    };
    use proptest::{collection, prelude::*};

    fn do_distinct_inc_test_mt(workers: usize) {
        let hruntime = Runtime::run(workers, || {
            distinct_inc_test();
        });

        hruntime.join().unwrap();
    }

    #[test]
    fn distinct_inc_test_mt() {
        do_distinct_inc_test_mt(1);
        do_distinct_inc_test_mt(2);
        do_distinct_inc_test_mt(4);
        do_distinct_inc_test_mt(16);
    }

    #[test]
    fn distinct_inc_test() {
        let circuit = RootCircuit::build(move |circuit| {
            let mut inputs = vec![
                vec![zset! { 1 => 1, 2 => 1 }, zset! { 2 => -1, 3 => 2, 4 => 2 }],
                vec![zset! { 2 => 1, 3 => 1 }, zset! { 3 => -2, 4 => -1 }],
                vec![
                    zset! { 5 => 1, 6 => 1 },
                    zset! { 2 => -1, 7 => 1 },
                    zset! { 2 => 1, 7 => -1, 8 => 2, 9 => 1 },
                ],
            ]
            .into_iter();

            circuit
                .iterate(|child| {
                    let counter = Rc::new(RefCell::new(0));
                    let counter_clone = counter.clone();

                    let input = child.add_source(GeneratorNested::new(Box::new(move || {
                        *counter_clone.borrow_mut() = 0;
                        if Runtime::worker_index() == 0 {
                            let mut deltas = inputs.next().unwrap_or_default().into_iter();
                            Box::new(move || deltas.next().unwrap_or_else(|| zset! {}))
                        } else {
                            Box::new(|| zset! {})
                        }
                    })));

                    let distinct_inc = input.distinct().gather(0);
                    let distinct_noninc = input
                        // Non-incremental implementation of distinct_nested_incremental.
                        .integrate()
                        .integrate_nested()
                        .stream_distinct()
                        .differentiate()
                        .differentiate_nested()
                        .gather(0);

                    distinct_inc
                        .apply2(&distinct_noninc, |d1: &OrdZSet<u64>, d2: &OrdZSet<u64>| {
                            (d1.clone(), d2.clone())
                        })
                        .inspect(|(d1, d2)| assert_eq!(d1, d2));

                    Ok((
                        move || {
                            *counter.borrow_mut() += 1;
                            Ok(*counter.borrow() == 4)
                        },
                        (),
                    ))
                })
                .unwrap();
            Ok(())
        })
        .unwrap()
        .0;

        for _ in 0..3 {
            circuit.step().unwrap();
        }
    }

    #[test]
    fn distinct_indexed_test() {
        let output1 = Arc::new(Mutex::new(OrdIndexedZSet::empty(())));
        let output1_clone = output1.clone();

        let output2 = Arc::new(Mutex::new(OrdIndexedZSet::empty(())));
        let output2_clone = output2.clone();

        let (mut circuit, input) = Runtime::init_circuit(4, move |circuit| {
            let (input, input_handle) = circuit.add_input_indexed_zset::<u64, u64>();

            input
                .integrate()
                .stream_distinct()
                .gather(0)
                .inspect(move |batch| {
                    if Runtime::worker_index() == 0 {
                        *output2_clone.lock().unwrap() = batch.clone();
                    }
                });

            input
                .distinct()
                .integrate()
                .gather(0)
                .inspect(move |batch| {
                    if Runtime::worker_index() == 0 {
                        *output1_clone.lock().unwrap() = batch.clone();
                    }
                });

            Ok(input_handle)
        })
        .unwrap();

        input.append(&mut vec![
            Tup2(1, Tup2(0, 1)),
            Tup2(1, Tup2(1, 2)),
            Tup2(2, Tup2(0, 1)),
            Tup2(2, Tup2(1, 1)),
        ]);
        circuit.step().unwrap();
        assert_eq!(
            &*output1.lock().unwrap(),
            &indexed_zset! { 1 => { 0 => 1, 1 => 1}, 2 => { 0 => 1, 1 => 1 } }
        );
        assert_eq!(&*output1.lock().unwrap(), &*output2.lock().unwrap(),);

        input.append(&mut vec![Tup2(3, Tup2(1, 1)), Tup2(2, Tup2(1, 1))]);
        circuit.step().unwrap();
        assert_eq!(
            &*output1.lock().unwrap(),
            &indexed_zset! { 1 => { 0 => 1, 1 => 1}, 2 => { 0 => 1, 1 => 1 }, 3 => { 1 => 1 } }
        );
        assert_eq!(&*output1.lock().unwrap(), &*output2.lock().unwrap(),);

        input.append(&mut vec![Tup2(1, Tup2(1, 3)), Tup2(2, Tup2(1, -3))]);
        circuit.step().unwrap();
        assert_eq!(
            &*output1.lock().unwrap(),
            &indexed_zset! { 1 => { 0 => 1, 1 => 1}, 2 => { 0 => 1 }, 3 => { 1 => 1 } }
        );
        assert_eq!(&*output1.lock().unwrap(), &*output2.lock().unwrap(),);

        circuit.kill().unwrap();
    }

    type TestZSet = OrdZSet<u64>;
    type TestIndexedZSet = OrdIndexedZSet<u64, i64>;

    const MAX_ROUNDS: usize = 15;
    const MAX_ITERATIONS: usize = 15;
    const NUM_KEYS: u64 = 10;
    const MAX_VAL: i64 = 3;
    const MAX_TUPLES: usize = 10;

    fn test_zset() -> impl Strategy<Value = TestZSet> {
        collection::vec((0..NUM_KEYS, -1..=1i64), 0..MAX_TUPLES).prop_map(|tuples| {
            OrdZSet::from_tuples(
                (),
                tuples
                    .into_iter()
                    .map(|(k, w)| Tup2(Tup2(k, ()), w))
                    .collect(),
            )
        })
    }

    fn test_input() -> impl Strategy<Value = Vec<TestZSet>> {
        collection::vec(test_zset(), 0..MAX_ROUNDS * MAX_ITERATIONS)
    }

    fn test_indexed_zset() -> impl Strategy<Value = TestIndexedZSet> {
        collection::vec(
            (
                (0..NUM_KEYS, -MAX_VAL..MAX_VAL).prop_map(|(x, y)| Tup2(x, y)),
                -1..=1i64,
            )
                .prop_map(|(x, y)| Tup2(x, y)),
            0..MAX_TUPLES,
        )
        .prop_map(|tuples| OrdIndexedZSet::from_tuples((), tuples))
    }

    fn test_indexed_input() -> impl Strategy<Value = Vec<TestIndexedZSet>> {
        collection::vec(test_indexed_zset(), 0..MAX_ROUNDS * MAX_ITERATIONS)
    }

    fn test_indexed_nested_input() -> impl Strategy<Value = Vec<Vec<TestIndexedZSet>>> {
        collection::vec(
            collection::vec(test_indexed_zset(), 0..MAX_ITERATIONS),
            0..MAX_ROUNDS,
        )
    }

    fn distinct_test_circuit(
        circuit: &mut RootCircuit,
        inputs: Vec<TestZSet>,
    ) -> AnyResult<(OutputHandle<TestZSet>, OutputHandle<TestZSet>)> {
        let mut inputs = inputs.into_iter();

        let input = circuit.add_source(Generator::new(Box::new(move || {
            if Runtime::worker_index() == 0 {
                inputs.next().unwrap_or_default()
            } else {
                zset! {}
            }
        })));

        let distinct_inc = input.distinct().output();
        let distinct_noninc = input.integrate().stream_distinct().differentiate().output();

        Ok((distinct_inc, distinct_noninc))
    }

    fn distinct_indexed_test_circuit(
        circuit: &mut RootCircuit,
        inputs: Vec<TestIndexedZSet>,
    ) -> AnyResult<(OutputHandle<TestIndexedZSet>, OutputHandle<TestIndexedZSet>)> {
        let mut inputs = inputs.into_iter();

        let input = circuit.add_source(Generator::new(Box::new(move || {
            if Runtime::worker_index() == 0 {
                inputs.next().unwrap_or_default()
            } else {
                indexed_zset! {}
            }
        })));

        let distinct_inc = input.distinct().output();
        let distinct_noninc = input.integrate().stream_distinct().differentiate().output();

        Ok((distinct_inc, distinct_noninc))
    }

    fn distinct_indexed_nested_test_circuit(
        circuit: &mut RootCircuit,
        inputs: Vec<Vec<TestIndexedZSet>>,
    ) -> AnyResult<()> {
        let mut inputs = inputs.into_iter();

        circuit
            .iterate(|child| {
                let counter = Rc::new(RefCell::new(0));
                let counter_clone = counter.clone();

                let input = child.add_source(GeneratorNested::new(Box::new(move || {
                    *counter_clone.borrow_mut() = 0;
                    if Runtime::worker_index() == 0 {
                        let mut deltas = inputs.next().unwrap_or_default().into_iter();
                        Box::new(move || deltas.next().unwrap_or_else(|| indexed_zset! {}))
                    } else {
                        Box::new(|| indexed_zset! {})
                    }
                })));

                let distinct_inc = input.distinct().gather(0);
                let distinct_noninc = input
                    .integrate_nested()
                    .integrate()
                    .stream_distinct()
                    .differentiate()
                    .differentiate_nested()
                    .gather(0);

                // Compare outputs of all three implementations.
                distinct_inc
                    .apply2(
                        &distinct_noninc,
                        |d1: &TestIndexedZSet, d2: &TestIndexedZSet| (d1.clone(), d2.clone()),
                    )
                    .inspect(|(d1, d2)| {
                        // if d1 != d2 {
                        //     println!("{}: incremental: {d1}", Runtime::worker_index());
                        //     println!("{}: non-incremental: {d2}", Runtime::worker_index());
                        // }
                        assert_eq!(d1, d2);
                    });

                Ok((
                    move || {
                        *counter.borrow_mut() += 1;
                        Ok(*counter.borrow() == MAX_ITERATIONS)
                    },
                    (),
                ))
            })
            .unwrap();
        Ok(())
    }

    proptest! {
        #[test]
        fn proptest_distinct_test_st(inputs in test_input()) {
            let iterations = inputs.len();
            let (circuit, (inc_output, noninc_output)) = RootCircuit::build(|circuit| distinct_test_circuit(circuit, inputs)).unwrap();

            for _ in 0..iterations {
                circuit.step().unwrap();
                assert_eq!(inc_output.consolidate(), noninc_output.consolidate());
            }
        }

        #[test]
        fn proptest_distinct_test_mt(inputs in test_input(), workers in (2..=16usize)) {
            let iterations = inputs.len();
            let (mut circuit, (inc_output, noninc_output)) = Runtime::init_circuit(workers, |circuit| distinct_test_circuit(circuit, inputs)).unwrap();

            for _ in 0..iterations {
                circuit.step().unwrap();
                assert_eq!(inc_output.consolidate(), noninc_output.consolidate());
            }

            circuit.kill().unwrap();
        }

        #[test]
        fn proptest_distinct_indexed_test_mt(inputs in test_indexed_input(), workers in (2..=4usize)) {
            let iterations = inputs.len();
            let (mut circuit, (inc_output, noninc_output)) = Runtime::init_circuit(workers, |circuit| distinct_indexed_test_circuit(circuit, inputs)).unwrap();

            for _ in 0..iterations {
                circuit.step().unwrap();
                assert_eq!(inc_output.consolidate(), noninc_output.consolidate());
            }

            circuit.kill().unwrap();
        }

        #[test]
        fn proptest_distinct_indexed_nested_test_mt(inputs in test_indexed_nested_input(), workers in (2..=4usize)) {
            let iterations = inputs.len();
            // for input in inputs.iter() {
            //     println!("round");
            //     for batch in input.iter() {
            //          println!("inputs: {batch}");
            //     }
            // }
            let mut circuit = Runtime::init_circuit(workers, |circuit| distinct_indexed_nested_test_circuit(circuit, inputs)).unwrap().0;

            for _ in 0..iterations {
                circuit.step().unwrap();
            }

            circuit.kill().unwrap();
        }
    }
}
