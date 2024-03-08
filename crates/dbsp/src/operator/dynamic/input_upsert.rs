use crate::{
    algebra::{AddAssignByRef, HasOne, HasZero, IndexedZSet, PartialOrder, ZTrace},
    circuit::{
        operator_traits::{BinaryOperator, Operator},
        OwnershipPreference, Scope, WithClock,
    },
    declare_trait_object,
    dynamic::{ClonableTrait, Data, DataTrait, DynOpt, DynPairs, Erase, Factory, WithFactory},
    operator::dynamic::trace::{
        DelayedTraceId, TraceAppend, TraceBounds, TraceId, ValSpine, Z1Trace,
    },
    trace::{
        cursor::Cursor, Batch, BatchFactories, BatchReader, BatchReaderFactories, Builder, Filter,
    },
    Circuit, DBData, NumEntries, Stream, Timestamp, ZWeight,
};
use rkyv::{Archive, Deserialize, Serialize};
use size_of::SizeOf;
use std::{borrow::Cow, marker::PhantomData, mem::take, ops::Neg};

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

pub struct InputUpsertFactories<T: Timestamp, B: IndexedZSet> {
    pub batch_factories: B::Factories,
    pub opt_key_factory: &'static dyn Factory<DynOpt<B::Key>>,
    pub opt_val_factory: &'static dyn Factory<DynOpt<B::Val>>,
    pub trace_factories: <T::OrdValBatch<B::Key, B::Val, B::R> as BatchReader>::Factories,
}

impl<T: Timestamp, B: IndexedZSet> Clone for InputUpsertFactories<T, B> {
    fn clone(&self) -> Self {
        Self {
            batch_factories: self.batch_factories.clone(),
            opt_key_factory: self.opt_key_factory,
            opt_val_factory: self.opt_val_factory,
            trace_factories: self.trace_factories.clone(),
        }
    }
}

impl<T, B> InputUpsertFactories<T, B>
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
            opt_key_factory: WithFactory::<Option<KType>>::FACTORY,
            opt_val_factory: WithFactory::<Option<VType>>::FACTORY,
            trace_factories: BatchReaderFactories::new::<KType, VType, ZWeight>(),
        }
    }
}

impl<C, K, V, U> Stream<C, Box<DynPairs<K, DynUpdate<V, U>>>>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    U: DataTrait + ?Sized,
    C: Circuit,
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
        factories: &InputUpsertFactories<<C as WithClock>::Time, B>,
        patch_func: PatchFunc<V, U>,
    ) -> Stream<C, B>
    where
        B: IndexedZSet<Key = K, Val = V>,
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

            let (local, z1feedback) = circuit.add_feedback(Z1Trace::new(
                &factories.trace_factories,
                false,
                circuit.root_scope(),
                bounds.clone(),
                self.origin_node_id().persistent_id(),
            ));

            local.mark_sharded_if(self);

            let delta = circuit
                .add_binary_operator(
                    <InputUpsert<ValSpine<B, C>, U, B>>::new(
                        factories.batch_factories.clone(),
                        factories.opt_key_factory,
                        factories.opt_val_factory,
                        bounds.clone(),
                        patch_func,
                    ),
                    &local,
                    &self.try_sharded_version(),
                )
                .mark_distinct();
            delta.mark_sharded_if(self);

            let trace = circuit.add_binary_operator_with_preference(
                <TraceAppend<ValSpine<B, C>, B, C>>::new(
                    &factories.trace_factories,
                    circuit.clone(),
                ),
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
    B: Batch,
    U: DataTrait + ?Sized,
{
    batch_factories: B::Factories,
    opt_key_factory: &'static dyn Factory<DynOpt<B::Key>>,
    opt_val_factory: &'static dyn Factory<DynOpt<B::Val>>,
    time: T::Time,
    patch_func: PatchFunc<T::Val, U>,
    bounds: TraceBounds<T::Key, T::Val>,
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
        bounds: TraceBounds<T::Key, T::Val>,
        patch_func: PatchFunc<T::Val, U>,
    ) -> Self {
        Self {
            batch_factories,
            opt_key_factory,
            opt_val_factory,
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
    U: DataTrait + ?Sized,
    B: Batch,
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

fn passes_filter<T: ?Sized>(filter: &Option<Filter<T>>, val: &T) -> bool {
    if let Some(filter) = filter {
        filter(val)
    } else {
        true
    }
}

impl<T, U, B> BinaryOperator<T, Box<DynPairs<T::Key, DynUpdate<T::Val, U>>>, B>
    for InputUpsert<T, U, B>
where
    T: ZTrace,
    U: DataTrait + ?Sized,
    B: IndexedZSet<Key = T::Key, Val = T::Val>,
{
    fn eval(&mut self, trace: &T, updates: &Box<DynPairs<T::Key, DynUpdate<T::Val, U>>>) -> B {
        // Inputs must be sorted by key
        debug_assert!(updates.is_sorted_by(&|u1, u2| u1.fst().cmp(u2.fst())));

        let mut key_updates = self.batch_factories.weighted_items_factory().default_box();

        let mut trace_cursor = trace.cursor();

        let mut builder = B::Builder::with_capacity(&self.batch_factories, (), updates.len() * 2);

        let val_filter = self.bounds.effective_val_filter();
        let key_filter = self.bounds.key_filter();
        let key_bound = self.bounds.effective_key_bound();

        // Current key for which we are processing updates.
        let mut cur_key: Box<DynOpt<T::Key>> = self.opt_key_factory.default_box();

        // Current value associated with the key after applying all processed updates
        // to it.
        let mut cur_val: Box<DynOpt<T::Val>> = self.opt_val_factory.default_box();

        // Set to true when the value associated with the current key doesn't
        // satisfy `val_filter`, hence refuse to remove this value and process
        // all updates for this key.
        let mut skip_key = false;

        for key_upd in updates.dyn_iter() {
            let (key, upd) = key_upd.split();

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
            if cur_key.get() != Some(key) {
                // Push updates for the previous key to the builder.
                if let Some(cur_key) = cur_key.get_mut() {
                    if let Some(val) = cur_val.get_mut() {
                        key_updates.push_with(&mut |item| {
                            let (kv, w) = item.split_mut();
                            let (k, v) = kv.split_mut();

                            cur_key.move_to(k);
                            val.move_to(v);
                            **w = HasOne::one();
                        });
                    }
                    key_updates.consolidate();
                    builder.extend(key_updates.dyn_iter_mut());
                }

                skip_key = false;
                cur_key.from_ref(key);
                cur_val.set_none();

                // Generate retraction if `key` is present in the trace.
                trace_cursor.seek_key(key);

                if trace_cursor.key_valid() && trace_cursor.key() == key {
                    // println!("{}: found key in trace_cursor", Runtime::worker_index());
                    while trace_cursor.val_valid() {
                        let mut weight = ZWeight::zero();
                        trace_cursor.map_times(&mut |t, w| {
                            if t.less_equal(&self.time) {
                                weight.add_assign_by_ref(w);
                            };
                        });

                        if !weight.is_zero() {
                            let val = trace_cursor.val();

                            if passes_filter(&val_filter, val) {
                                key_updates.push_with(&mut |item| {
                                    let (kv, w) = item.split_mut();
                                    let (k, v) = kv.split_mut();

                                    val.clone_to(v);
                                    key.clone_to(k);
                                    **w = weight.neg()
                                });
                                cur_val.from_ref(val);
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
                match upd.get() {
                    UpdateRef::Delete => {
                        // TODO: if cur_val.is_none(), report missing key.
                        cur_val.set_none();
                    }
                    UpdateRef::Insert(val) => {
                        if passes_filter(&val_filter, val) {
                            cur_val.from_ref(val);
                        } else {
                            // TODO: report lateness violation.
                        }
                    }
                    UpdateRef::Update(upd) => {
                        if let Some(val) = cur_val.get_mut() {
                            (self.patch_func)(val, upd);
                            if !passes_filter(&val_filter, val) {
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
        if let Some(cur_key) = cur_key.get_mut() {
            if let Some(val) = cur_val.get_mut() {
                key_updates.push_with(&mut |item| {
                    let (kv, w) = item.split_mut();
                    let (k, v) = kv.split_mut();

                    cur_key.move_to(k);
                    val.move_to(v);
                    **w = HasOne::one();
                });
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
