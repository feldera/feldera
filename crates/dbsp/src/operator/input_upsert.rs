use crate::{
    algebra::{AddAssignByRef, HasOne, HasZero, PartialOrder, ZRingValue},
    circuit::{
        operator_traits::{BinaryOperator, Operator},
        OwnershipPreference, Scope, WithClock,
    },
    operator::trace::{DelayedTraceId, TraceAppend, TraceBounds, TraceId, ValSpine, Z1Trace},
    trace::{
        consolidation::consolidate, cursor::Cursor, Batch, BatchReader, Builder, Filter, Trace,
    },
    utils::VecExt,
    Circuit, DBData, DBTimestamp, Stream, Timestamp,
};
use rkyv::{Archive, Deserialize, Serialize};
use size_of::SizeOf;
use std::{borrow::Cow, marker::PhantomData, ops::Neg};

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

pub type PatchFunc<V, U> = Box<dyn Fn(&mut V, U)>;

impl<C, K, V, U> Stream<C, Vec<(K, Update<V, U>)>>
where
    K: DBData,
    V: DBData,
    U: DBData,
    C: Circuit,
    <C as WithClock>::Time: DBTimestamp,
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
    pub fn input_upsert<B>(&self, patch_func: PatchFunc<V, U>) -> Stream<C, B>
    where
        B::R: DBData + ZRingValue,
        B: Batch<Key = K, Val = V, Time = ()>,
    {
        let circuit = self.circuit();

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
        //  self        ┌───────────┐    │        ┌───────────┐  trace
        // ────────────►│InputUpsert├────┴───────►│TraceAppend├────┐
        //              └───────────┘   delta     └───────────┘    │
        //                      ▲                  ▲               │
        //                      │                  │               │
        //                      │                  │   ┌───────┐   │
        //                      └──────────────────┴───┤Z1Trace│◄──┘
        //                         z1trace             └───────┘
        // ```
        circuit.region("input_upsert", || {
            let bounds = <TraceBounds<K, V>>::unbounded();

            let (local, z1feedback) =
                circuit.add_feedback(Z1Trace::new(false, circuit.root_scope(), bounds.clone()));
            local.mark_sharded_if(self);

            let delta = circuit
                .add_binary_operator(
                    <InputUpsert<ValSpine<B, C>, U, B>>::new(bounds.clone(), patch_func),
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

pub struct InputUpsert<T, U, B>
where
    T: BatchReader,
{
    time: T::Time,
    patch_func: PatchFunc<T::Val, U>,
    bounds: TraceBounds<T::Key, T::Val>,
    phantom: PhantomData<B>,
}

impl<T, U, B> InputUpsert<T, U, B>
where
    T: BatchReader,
{
    pub fn new(bounds: TraceBounds<T::Key, T::Val>, patch_func: PatchFunc<T::Val, U>) -> Self {
        Self {
            time: T::Time::clock_start(),
            bounds,
            patch_func,
            phantom: PhantomData,
        }
    }
}

impl<T, U, B> Operator for InputUpsert<T, U, B>
where
    T: BatchReader,
    U: 'static,
    B: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("InputUpsert")
    }
    fn clock_end(&mut self, scope: Scope) {
        self.time = self.time.advance(scope + 1);
    }
    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

fn passes_filter<T>(filter: &Option<Filter<T>>, val: &T) -> bool {
    if let Some(filter) = filter {
        filter(val)
    } else {
        true
    }
}

impl<T, U, B> BinaryOperator<T, Vec<(T::Key, Update<T::Val, U>)>, B> for InputUpsert<T, U, B>
where
    T: Trace,
    T::R: ZRingValue,
    U: DBData,
    B: Batch<Key = T::Key, Val = T::Val, Time = (), R = T::R>,
{
    fn eval(&mut self, trace: &T, updates: &Vec<(T::Key, Update<T::Val, U>)>) -> B {
        // Inputs must be sorted by key
        debug_assert!(updates.is_sorted_by(|(k1, _), (k2, _)| k1.partial_cmp(k2)));

        let mut trace_cursor = trace.cursor();

        let mut builder = B::Builder::with_capacity((), updates.len() * 2);
        let mut key_updates: Vec<(T::Val, T::R)> = Vec::new();

        let val_filter = self.bounds.effective_val_filter();
        let key_filter = self.bounds.key_filter();
        let key_bound = self.bounds.effective_key_bound();

        // Current key for which we are processing updates.
        let mut cur_key: Option<T::Key> = None;

        // Current value associated with the key after applying all processed updates
        // to it.
        let mut cur_val: Option<T::Val> = None;

        // Set to true when the value associated with the current key doesn't
        // satisfy `val_filter`, hence refuse to remove this value and process
        // all updates for this key.
        let mut skip_key = false;

        for (key, upd) in updates {
            if let Some(key_bound) = &key_bound {
                if key < key_bound {
                    // TODO: report lateness violation.
                    continue;
                }
            }

            if !passes_filter(&key_filter, key) {
                // TODO: report lateness violation.
                continue;
            }

            // We finished processing updates for the previous key. Push them to the
            // builder and generate a retraction for the new key.
            if cur_key.as_ref() != Some(key) {
                // Push updates for the previous key to the builder.
                if let Some(cur_key) = cur_key.take() {
                    if let Some(val) = cur_val.take() {
                        key_updates.push((val, HasOne::one()));
                    }
                    consolidate(&mut key_updates);
                    builder.extend(
                        key_updates
                            .drain(..)
                            .map(|(val, w)| (B::item_from(cur_key.clone(), val), w)),
                    );
                }

                skip_key = false;
                cur_key = Some(key.clone());
                cur_val = None;

                // Generate retraction if `key` is present in the trace.
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
                            let val = trace_cursor.val().clone();

                            if passes_filter(&val_filter, &val) {
                                key_updates.push((val.clone(), weight.neg()));
                                cur_val = Some(val);
                            } else {
                                skip_key = true;
                                // TODO: report lateness violation.
                            }
                        }

                        trace_cursor.step_val();
                    }
                }
            }

            if !skip_key {
                match upd {
                    Update::Delete => {
                        // TODO: if cur_val.is_none(), report missing key.
                        cur_val = None;
                    }
                    Update::Insert(val) => {
                        if passes_filter(&val_filter, val) {
                            cur_val = Some(val.clone());
                        } else {
                            // TODO: report lateness violation.
                        }
                    }
                    Update::Update(upd) => {
                        if let Some(val) = &cur_val {
                            let mut val = val.clone();
                            (self.patch_func)(&mut val, upd.clone());
                            if passes_filter(&val_filter, &val) {
                                cur_val = Some(val);
                            } else {
                                // TODO: report lateness violation.
                            }
                        } else {
                            // TODO: report missing key.
                        }
                    }
                }
            }
        }

        // Push updates for the last key.
        if let Some(cur_key) = cur_key.take() {
            if let Some(val) = cur_val.take() {
                key_updates.push((val, HasOne::one()));
            }

            consolidate(&mut key_updates);
            builder.extend(
                key_updates
                    .drain(..)
                    .map(|(val, w)| (B::item_from(cur_key.clone(), val), w)),
            );
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
