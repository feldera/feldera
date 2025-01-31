use crate::{
    algebra::{
        IndexedZSet, OrdIndexedZSet, OrdIndexedZSetFactories, OrdZSet, OrdZSetFactories, ZSet,
    },
    circuit::RootCircuit,
    dynamic::{
        ClonableTrait, DataTrait, DynBool, DynOpt, DynPair, DynPairs, DynUnit, DynWeightedPairs,
        Erase, Factory, LeanVec, WithFactory,
    },
    operator::{
        dynamic::{
            input_upsert::{DynUpdate, InputUpsertFactories, PatchFunc},
            upsert::UpdateSetFactories,
        },
        Input, InputHandle, Update,
    },
    trace::{Batch, BatchFactories, BatchReaderFactories},
    utils::Tup2,
    Circuit, DBData, DynZWeight, Stream, ZWeight,
};
use std::{
    mem::{replace, swap},
    panic::Location,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

pub type IndexedZSetStream<K, V> = Stream<RootCircuit, OrdIndexedZSet<K, V>>;
pub type ZSetStream<K> = Stream<RootCircuit, OrdZSet<K>>;

pub struct AddInputZSetFactories<K: DataTrait + ?Sized> {
    zset_factories: OrdZSetFactories<K>,
    weighted_pairs_factory: &'static dyn Factory<DynWeightedPairs<DynPair<K, DynUnit>, DynZWeight>>,
    pairs_factory: &'static dyn Factory<DynPairs<DynPair<K, DynUnit>, DynZWeight>>,
    pair_factory: &'static dyn Factory<DynPair<DynPair<K, DynUnit>, DynZWeight>>,
}

impl<K> AddInputZSetFactories<K>
where
    K: DataTrait + ?Sized,
{
    pub fn new<KType>() -> Self
    where
        KType: DBData + Erase<K>,
    {
        Self {
            zset_factories: BatchReaderFactories::new::<KType, (), ZWeight>(),
            weighted_pairs_factory: WithFactory::<LeanVec<Tup2<Tup2<KType, ()>, ZWeight>>>::FACTORY,
            pairs_factory: WithFactory::<LeanVec<Tup2<Tup2<KType, ()>, ZWeight>>>::FACTORY,
            pair_factory: WithFactory::<Tup2<Tup2<KType, ()>, ZWeight>>::FACTORY,
        }
    }
}

impl<K> Clone for AddInputZSetFactories<K>
where
    K: DataTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            zset_factories: self.zset_factories.clone(),
            weighted_pairs_factory: self.weighted_pairs_factory,
            pairs_factory: self.pairs_factory,
            pair_factory: self.pair_factory,
        }
    }
}

pub struct AddInputIndexedZSetFactories<K, V>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
{
    indexed_zset_factories: OrdIndexedZSetFactories<K, V>,
    pair_factory: &'static dyn Factory<DynPair<K, DynPair<V, DynZWeight>>>,
    pairs_factory: &'static dyn Factory<DynPairs<K, DynPair<V, DynZWeight>>>,
}

impl<K, V> Clone for AddInputIndexedZSetFactories<K, V>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            indexed_zset_factories: self.indexed_zset_factories.clone(),
            pair_factory: self.pair_factory,
            pairs_factory: self.pairs_factory,
        }
    }
}

impl<K, V> AddInputIndexedZSetFactories<K, V>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
{
    pub fn new<KType, VType>() -> Self
    where
        KType: DBData + Erase<K>,
        VType: DBData + Erase<V>,
    {
        Self {
            indexed_zset_factories: BatchReaderFactories::new::<KType, VType, ZWeight>(),
            pairs_factory: WithFactory::<LeanVec<Tup2<KType, Tup2<VType, ZWeight>>>>::FACTORY,
            pair_factory: WithFactory::<Tup2<KType, Tup2<VType, ZWeight>>>::FACTORY,
        }
    }
}

pub struct AddInputSetFactories<B>
where
    B: ZSet,
{
    update_set_factories: UpdateSetFactories<(), B>,
    input_pair_factory: &'static dyn Factory<DynPair<B::Key, DynBool>>,
    input_pairs_factory: &'static dyn Factory<DynPairs<B::Key, DynBool>>,
    upsert_pair_factory: &'static dyn Factory<DynPair<B::Key, DynOpt<DynUnit>>>,
    upsert_pairs_factory: &'static dyn Factory<DynPairs<B::Key, DynOpt<DynUnit>>>,
}

impl<B> Clone for AddInputSetFactories<B>
where
    B: ZSet,
{
    fn clone(&self) -> Self {
        Self {
            update_set_factories: self.update_set_factories.clone(),
            input_pair_factory: self.input_pair_factory,
            input_pairs_factory: self.input_pairs_factory,
            upsert_pair_factory: self.upsert_pair_factory,
            upsert_pairs_factory: self.upsert_pairs_factory,
        }
    }
}

impl<B> AddInputSetFactories<B>
where
    B: ZSet,
{
    pub fn new<KType>() -> Self
    where
        KType: DBData + Erase<B::Key>,
    {
        Self {
            update_set_factories: UpdateSetFactories::new::<KType>(),
            input_pair_factory: WithFactory::<Tup2<KType, bool>>::FACTORY,
            input_pairs_factory: WithFactory::<LeanVec<Tup2<KType, bool>>>::FACTORY,
            upsert_pair_factory: WithFactory::<Tup2<KType, Option<()>>>::FACTORY,
            upsert_pairs_factory: WithFactory::<LeanVec<Tup2<KType, Option<()>>>>::FACTORY,
        }
    }
}

pub struct AddInputMapFactories<B, U>
where
    B: IndexedZSet,
    U: DataTrait + ?Sized,
{
    upsert_factories: InputUpsertFactories<(), B>,
    input_pair_factory: &'static dyn Factory<DynPair<B::Key, DynUpdate<B::Val, U>>>,
    input_pairs_factory: &'static dyn Factory<DynPairs<B::Key, DynUpdate<B::Val, U>>>,
    upsert_pair_factory: &'static dyn Factory<DynPair<B::Key, DynOpt<DynUnit>>>,
}

impl<B, U> AddInputMapFactories<B, U>
where
    B: IndexedZSet,
    U: DataTrait + ?Sized,
{
    pub fn new<KType, VType, UType>() -> Self
    where
        KType: DBData + Erase<B::Key>,
        VType: DBData + Erase<B::Val>,
        UType: DBData + Erase<U>,
    {
        Self {
            upsert_factories: InputUpsertFactories::new::<KType, VType>(),
            input_pair_factory: WithFactory::<Tup2<KType, Update<VType, UType>>>::FACTORY,
            input_pairs_factory: WithFactory::<LeanVec<Tup2<KType, Update<VType, UType>>>>::FACTORY,
            upsert_pair_factory: WithFactory::<Tup2<KType, Option<()>>>::FACTORY,
        }
    }
}

impl<B, U> Clone for AddInputMapFactories<B, U>
where
    B: IndexedZSet,
    U: DataTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            upsert_factories: self.upsert_factories.clone(),
            input_pair_factory: self.input_pair_factory,
            input_pairs_factory: self.input_pairs_factory,
            upsert_pair_factory: self.upsert_pair_factory,
        }
    }
}

impl RootCircuit {
    /// Create an input stream that carries values of type [`OrdZSet<K>`](`OrdZSet`).
    ///
    /// Creates an input stream that carries values of type `OrdZSet<K>` and
    /// an input handle of type [`CollectionHandle<K, R>`](`CollectionHandle`)
    /// used to construct input Z-sets out of individual elements.  The
    /// client invokes [`CollectionHandle::dyn_push`] and
    /// [`CollectionHandle::dyn_append`] any number of times to add values to
    /// the input Z-set. These values are distributed across all worker
    /// threads (when running in a multithreaded [`Runtime`](`crate::Runtime`))
    /// in a round-robin fashion and buffered until the start of the next clock
    /// cycle.  At the start of a clock cycle (triggered by
    /// [`DBSPHandle::step`](`crate::DBSPHandle::step`) or
    /// [`CircuitHandle::step`](`crate::CircuitHandle::step`)), the circuit
    /// reads all buffered values and assembles them into an `OrdZSet`.
    ///
    /// See [`CollectionHandle`] for more details.
    #[track_caller]
    pub fn dyn_add_input_zset<K>(
        &self,
        factories: &AddInputZSetFactories<K>,
    ) -> (
        ZSetStream<K>,
        CollectionHandle<DynPair<K, DynUnit>, DynZWeight>,
    )
    where
        K: DataTrait + ?Sized,
    {
        let pairs_factory = factories.pairs_factory;
        let weighted_pairs_factory = factories.weighted_pairs_factory;

        let zset_factories = factories.zset_factories.clone();

        let (input, input_handle) = Input::new(
            Location::caller(),
            move |mut tuples: Box<DynPairs<_, _>>| {
                let mut pairs = weighted_pairs_factory.default_box();
                pairs.from_pairs(tuples.as_mut());
                OrdZSet::dyn_from_tuples(&zset_factories, (), &mut pairs)
            },
            Arc::new(|| pairs_factory.default_box()),
        );
        let stream = self.add_source(input);

        let zset_handle = <CollectionHandle<DynPair<K, DynUnit>, DynZWeight>>::new(
            factories.pair_factory,
            input_handle,
        );

        (stream, zset_handle)
    }

    /// Create an input stream that carries values of type
    /// [`OrdIndexedZSet<K, V>`](`OrdIndexedZSet`).
    ///
    /// Creates an input stream that carries values of type `OrdIndexedZSet<K, V>`
    /// and an input handle of type [`CollectionHandle<K, (V, R)>`](`CollectionHandle`)
    /// used to construct input Z-sets out of individual elements.  The client invokes
    /// [`CollectionHandle::dyn_push`] and [`CollectionHandle::dyn_append`] any number
    /// of times to add `key/value/weight` triples the indexed Z-set. These triples are
    /// distributed across all worker threads (when running in a multithreaded
    /// [`Runtime`](`crate::Runtime`)) in a round-robin fashion, and buffered until the
    /// start of the next clock cycle.  At the start of a clock cycle (triggered by
    /// [`DBSPHandle::step`](`crate::DBSPHandle::step`) or
    /// [`CircuitHandle::step`](`crate::CircuitHandle::step`)), the circuit
    /// reads all buffered values and assembles them into an `OrdIndexedZSet`.
    ///
    /// See [`CollectionHandle`] for more details.
    #[allow(clippy::type_complexity)]
    #[track_caller]
    pub fn dyn_add_input_indexed_zset<K, V>(
        &self,
        factories: &AddInputIndexedZSetFactories<K, V>,
    ) -> (
        IndexedZSetStream<K, V>,
        CollectionHandle<K, DynPair<V, DynZWeight>>,
    )
    where
        K: DataTrait + ?Sized,
        V: DataTrait + ?Sized,
    {
        let factories_clone = factories.clone();

        let (input, input_handle) = Input::new(
            Location::caller(),
            move |mut tuples: Box<DynPairs<K, DynPair<V, DynZWeight>>>| {
                let mut indexed_tuples = factories_clone
                    .indexed_zset_factories
                    .weighted_items_factory()
                    .default_box();
                let mut item = factories_clone
                    .indexed_zset_factories
                    .weighted_item_factory()
                    .default_box();

                for kvw in tuples.dyn_iter_mut() {
                    let (k, vw) = kvw.split_mut();
                    let (v, w) = vw.split_mut();
                    let (kv, item_w) = item.split_mut();
                    let (item_k, item_v) = kv.split_mut();
                    k.clone_to(item_k);
                    v.clone_to(item_v);
                    w.clone_to(item_w);
                    indexed_tuples.push_val(&mut *item);
                }
                OrdIndexedZSet::dyn_from_tuples(
                    &factories_clone.indexed_zset_factories,
                    (),
                    &mut indexed_tuples,
                )
            },
            Arc::new(|| factories.pairs_factory.default_box()),
        );
        let stream = self.add_source(input);

        let zset_handle = <CollectionHandle<K, DynPair<V, DynZWeight>>>::new(
            factories.pair_factory,
            input_handle,
        );

        (stream, zset_handle)
    }

    #[track_caller]
    fn add_set_update<K, B>(
        &self,
        factories: &AddInputSetFactories<B>,
        input_stream: Stream<Self, Box<DynPairs<K, DynBool>>>,
    ) -> Stream<Self, B>
    where
        K: DataTrait + ?Sized,
        B: ZSet<Key = K>,
    {
        let factories_clone = factories.clone();

        let sorted = input_stream
            .apply_owned(move |mut upserts| {
                // Sort the vector by key, preserving the history of updates for each key.
                // Upserts cannot be merged or reordered, therefore we cannot use unstable sort.
                upserts.sort_by_key();

                // Find the last upsert for each key, that's the only one that matters.
                upserts.dedup_by_key_keep_last();

                let mut result = factories_clone.upsert_pairs_factory.default_box();
                let mut tuple = factories_clone.upsert_pair_factory.default_box();

                for upsert in upserts.dyn_iter_mut() {
                    let (k, v) = upsert.split_mut();
                    let mut v = if **v { Some(()) } else { None };
                    tuple.from_vals(k, v.erase_mut());
                    result.push_val(&mut *tuple);
                }

                result
            })
            // UpsertHandle shards its inputs.
            .mark_sharded();

        sorted.update_set::<B>(&factories.update_set_factories)
    }

    #[track_caller]
    fn add_upsert_indexed<K, V, U, B>(
        &self,
        factories: &AddInputMapFactories<B, U>,
        input_stream: Stream<Self, Box<DynPairs<K, DynUpdate<V, U>>>>,
        patch_func: PatchFunc<V, U>,
    ) -> Stream<Self, B>
    where
        B: IndexedZSet<Key = K, Val = V>,
        K: DataTrait + ?Sized,
        V: DataTrait + ?Sized,
        U: DataTrait + ?Sized,
    {
        let sorted = input_stream
            .apply_owned(move |mut upserts| {
                // Sort the vector by key, preserving the history of updates for each key.
                // Upserts cannot be merged or reordered, therefore we cannot use unstable sort.
                upserts.sort_by_key();

                upserts
            })
            // UpsertHandle shards its inputs.
            .mark_sharded();

        sorted.input_upsert::<B>(&factories.upsert_factories, patch_func)
    }

    /// Create an input table with set semantics.
    ///
    /// The `dyn_add_input_set` operator creates an input table that internally
    /// appears as a Z-set with unit weights, but that ingests input data
    /// using set semantics. It returns a stream that carries values of type
    /// `OrdZSet<K>` and an input handle of type
    /// [`UpsertHandle<K,bool>`](`UpsertHandle`).  The client uses
    /// [`UpsertHandle::dyn_push`] and [`UpsertHandle::dyn_append`] to submit
    /// commands of the form `(val, true)` to insert an element to the set
    /// and `(val, false) ` to delete `val` from the set.  These commands
    /// are buffered until the start of the next clock cycle.
    ///
    /// At the start of a clock cycle (triggered by
    /// [`DBSPHandle::step`](`crate::DBSPHandle::step`) or
    /// [`CircuitHandle::step`](`crate::CircuitHandle::step`)), DBSP applies
    /// buffered commands in order and computes an update to the input set as
    /// an `OrdZSet` with weights `+1` and `-1` representing set insertions and
    /// deletions respectively. The following table illustrates the
    /// relationship between input commands, the contents of the set and the
    /// contents of the stream produced by this operator:
    ///
    /// ```text
    /// time │      input commands          │content of the   │ stream returned by     │  comment
    ///      │                              │input set        │ `add_input_set`        │
    /// ─────┼──────────────────────────────┼─────────────────┼────────────────────────┼───────────────────────────────────────────────────────
    ///    1 │{("foo",true),("bar",true)}   │  {"foo","bar"}  │ {("foo",+1),("bar",+1)}│
    ///    2 │{("foo",true),("bar",false)}  │  {"foo"}        │ {("bar",-1)}           │ignore duplicate insert of "foo"
    ///    3 │{("foo",false),("foo",true)}  │  {"foo"}        │ {}                     │deleting and re-inserting "foo" is a no-op
    ///    4 │{("foo",false),("bar",false)} │  {}             │ {("foo",-1)}           │deleting value "bar" that is not in the set is a no-op
    /// ─────┴──────────────────────────────┴─────────────────┴────────────────────────┴────────────────────────────────────────────────────────
    /// ```
    ///
    /// Internally, this operator maintains the contents of the input set
    /// partitioned across all worker threads based on the hash of the
    /// value.  Insert/delete commands are routed to the worker in charge of
    /// the given value.
    ///
    /// # Data retention
    ///
    /// Applying [`Stream::dyn_integrate_trace_retain_keys`], and
    /// [`Stream::dyn_integrate_trace_with_bound`] methods to the stream has the
    /// additional effect of filtering out all values that don't satisfy the
    /// retention policy configured by these methods from the stream.
    /// Specifically, retention conditions configured at logical time `t`
    /// are applied starting from logical time `t+1`.
    // TODO: Add a version that takes a custom hash function.
    #[track_caller]
    pub fn dyn_add_input_set<K>(
        &self,
        factories: &AddInputSetFactories<OrdZSet<K>>,
    ) -> (ZSetStream<K>, UpsertHandle<K, DynBool>)
    where
        K: DataTrait + ?Sized,
    {
        self.region("input_set", || {
            let (input, input_handle) = Input::new(
                Location::caller(),
                |tuples: Box<DynPairs<K, DynBool>>| tuples,
                Arc::new(|| factories.input_pairs_factory.default_box()),
            );
            let input_stream = self.add_source(input);
            let upsert_handle = <UpsertHandle<K, DynBool>>::new(
                factories.input_pair_factory,
                factories.input_pairs_factory,
                input_handle,
            );

            let upsert: Stream<RootCircuit, OrdZSet<K>> =
                self.add_set_update(factories, input_stream);

            (upsert, upsert_handle)
        })
    }

    /// Create an input table as a key-value map with upsert update semantics.
    ///
    /// # Details
    ///
    /// The `dyn_add_input_map` operator creates an input table that internally
    /// appears as an indexed Z-set with all unit weights, but that ingests
    /// input data using upsert semantics. It returns a stream that carries
    /// values of type `OrdIndexedZSet<K, V>` and an input handle of type
    /// [`UpsertHandle<K,Option<V>>`](`UpsertHandle`).  The client uses
    /// [`UpsertHandle::dyn_push`] and [`UpsertHandle::dyn_append`] to submit
    /// commands of the form `(key, Some(val))` to insert a new key-value
    /// pair and `(key, None) ` to delete the value associated with `key` is
    /// any. These commands are buffered until the start of the next clock
    /// cycle.
    ///
    /// At the start of a clock cycle (triggered by
    /// [`DBSPHandle::step`](`crate::DBSPHandle::step`) or
    /// [`CircuitHandle::step`](`crate::CircuitHandle::step`)),
    /// DBSP applies buffered commands in order and
    /// computes an update to the input set as an `OrdIndexedZSet` with weights
    /// `+1` and `-1` representing insertions and deletions respectively.
    /// The following table illustrates the relationship between input commands,
    /// the contents of the map and the contents of the stream produced by this
    /// operator:
    ///
    /// ```text
    /// time │      input commands               │content of the        │ stream returned by         │  comment
    ///      │                                   │input map             │ `add_input_map`            │
    /// ─────┼───────────────────────────────────┼──────────────────────┼────────────────────────────┼───────────────────────────────────────────────────────
    ///    1 │{(1,Some("foo"), (2,Some("bar"))}  │{(1,"foo"),(2,"bar")} │ {(1,"foo",+1),(2,"bar",+1)}│
    ///    2 │{(1,Some("foo"), (2,Some("baz"))}  │{(1,"foo"),(2,"baz")} │ {(2,"bar",-1),(2,"baz",+1)}│ Ignore duplicate insert of (1,"foo"). New value
    ///      |                                   |                      |                            | "baz" for key 2 overwrites the old value "bar".
    ///    3 │{(1,None),(2,Some("bar")),(2,None)}│{}                    │ {(1,"foo",-1),(2,"baz",-1)}│ Delete both keys. Upsert (2,"bar") is overridden
    ///      |                                   |                      |                            | by subsequent delete command.
    /// ─────┴───────────────────────────────────┴──────────────────────┴────────────────────────────┴────────────────────────────────────────────────────────
    /// ```
    ///
    /// Note that upsert commands cannot fail.  Duplicate inserts and deletes
    /// are simply ignored.
    ///
    /// Internally, this operator maintains the contents of the map
    /// partitioned across all worker threads based on the hash of the
    /// key.  Upsert/delete commands are routed to the worker in charge of
    /// the given key.
    ///
    ///
    /// # Data retention
    ///
    /// Applying the [`Stream::dyn_integrate_trace_retain_keys`] to the stream has the
    /// additional effect of filtering out all updates that don't satisfy the
    /// retention policy.
    /// In particular, this means that attempts to overwrite, update, or delete
    /// a key-value pair whose key doesn't satisfy current retention
    /// conditions are ignored, since all these operations involve deleting
    /// an existing tuple.
    ///
    /// Retention conditions configured at logical time `t`
    /// are applied starting from logical time `t+1`.
    ///
    /// FIXME: see <https://github.com/feldera/feldera/issues/2669>
    // TODO: Add a version that takes a custom hash function.
    #[track_caller]
    pub fn dyn_add_input_map<K, V, U>(
        &self,
        factories: &AddInputMapFactories<OrdIndexedZSet<K, V>, U>,
        patch_func: PatchFunc<V, U>,
    ) -> (IndexedZSetStream<K, V>, UpsertHandle<K, DynUpdate<V, U>>)
    where
        K: DataTrait + ?Sized,
        V: DataTrait + ?Sized,
        U: DataTrait + ?Sized,
    {
        self.region("input_map", || {
            let (input, input_handle) = Input::new(
                Location::caller(),
                |tuples: Box<DynPairs<K, DynUpdate<V, U>>>| tuples,
                Arc::new(|| factories.input_pairs_factory.default_box()),
            );
            let input_stream = self.add_source(input);
            let zset_handle = <UpsertHandle<K, DynUpdate<V, U>>>::new(
                factories.input_pair_factory,
                factories.input_pairs_factory,
                input_handle,
            );

            let upsert = self.add_upsert_indexed(factories, input_stream, patch_func);

            (upsert, zset_handle)
        })
    }
}

/*
// We may want to uncomment and use the following operator based on
// profiling data.  At the moment the `Input` operator assembles input
// tuples into batches as they are received from `CollectionHandle`s.
// Since `CollectionHandle` doesn't consistently map keys to workers,
// resulting batches may need to be re-sharded by the next operator.
// It may be more efficient to shard update vectors received from
// `CollectionHandle` directly without paying the cost of assembling
// them into batches first.  This is what this operator does.
impl<K, V> Stream<Circuit<()>, Vec<(K, V)>>
where
    K: Send + Hash + Clone + 'static,
    V: Send + Clone + 'static,
{
    fn shard_vec(&self) -> Stream<Circuit<()>, Vec<(K, V)>> {
        Runtime::runtime()
            .map(|runtime| {
                let num_workers = runtime.num_workers();

                if num_workers == 1 {
                    self.clone()
                } else {
                    let (sender, receiver) = self.circuit().new_exchange_operators(
                        &runtime,
                        Runtime::worker_index(),
                        move |batch: Vec<(K, V)>, batches: &mut Vec<Vec<(K, V)>>| {
                            for _ in 0..num_workers {
                                batches.push(Vec::with_capacity(batch.len() / num_workers));
                            }

                            for (key, val) in batch.into_iter() {
                                let batch_index = fxhash::hash(&key) % num_workers;
                                batches[batch_index].push((key, val))
                            }
                        },
                        move |output: &mut Vec<(K, V)>, batch: Vec<(K, V)>| {
                            if output.is_empty() {
                                output.reserve(batch.len() * num_workers);
                            }
                            output.extend(batch);
                        },
                    );

                    self.circuit().add_exchange(sender, receiver, self)
                }
            })
            .unwrap_or_else(|| self.clone())
    }
}
*/

/// A handle used to write data to an input stream created by
/// [`add_input_zset`](`RootCircuit::add_input_zset`),
/// and [`add_input_indexed_zset`](`RootCircuit::add_input_indexed_zset`)
/// methods.
///
/// The handle provides an API to push updates to the stream in
/// the form of `(key, value)` tuples:
///
///    * For `add_input_zset`, the tuples have the form `(key, weight)`.
///
///    * For `add_input_indexed_zset`, the tuples have the form `(key, (value,
///      weight)`).
///
/// See [`add_input_zset`](`RootCircuit::add_input_zset`),
/// [`add_input_indexed_zset`](`RootCircuit::add_input_indexed_zset`) and
/// documentation for the exact semantics of these updates.
///
/// Internally, the handle manages an array of mailboxes, one for each worker
/// thread on this host. It automatically partitions updates across mailboxes in
/// a round robin fashion.  At the start of each clock cycle, the circuit
/// consumes updates buffered in each mailbox, leaving the mailbox empty.
pub struct CollectionHandle<K: DataTrait + ?Sized, V: DataTrait + ?Sized> {
    pair_factory: &'static dyn Factory<DynPair<K, V>>,
    input_handle: InputHandle<Box<DynPairs<K, V>>>,
    // Used to send tuples to workers in round robin.  Oftentimes the
    // workers will immediately repartition the inputs based on the hash
    // of the key; however this is more efficient than doing it here, as
    // the work will be evenly split across workers.
    next_worker: Arc<AtomicUsize>,
}

impl<K: DataTrait + ?Sized, V: DataTrait + ?Sized> Clone for CollectionHandle<K, V> {
    fn clone(&self) -> Self {
        Self {
            pair_factory: self.pair_factory,
            input_handle: self.input_handle.clone(),
            next_worker: self.next_worker.clone(),
        }
    }
}

impl<K: DataTrait + ?Sized, V: DataTrait + ?Sized> CollectionHandle<K, V> {
    fn new(
        pair_factory: &'static dyn Factory<DynPair<K, V>>,
        input_handle: InputHandle<Box<DynPairs<K, V>>>,
    ) -> Self {
        Self {
            pair_factory,
            input_handle,
            next_worker: Arc::new(AtomicUsize::new(0)),
        }
    }

    #[inline]
    fn num_partitions(&self) -> usize {
        self.input_handle.0.mailbox.len()
    }

    /// Push a single `(key,value)` pair to the input stream.
    pub fn dyn_push(&self, k: &mut K, v: &mut V) {
        let mut tuple = self.pair_factory.default_box();
        let next_worker = match self.num_partitions() {
            1 => 0,
            n => self.next_worker.fetch_add(1, Ordering::AcqRel) % n,
        };
        self.input_handle.update_for_worker(
            next_worker + self.input_handle.workers().start,
            |tuples| {
                tuple.from_vals(k, v);
                tuples.push_val(&mut *tuple);
            },
        );
    }

    /// Push multiple `(key,value)` pairs to the input stream.
    ///
    /// This is more efficient than pushing values one-by-one using
    /// [`Self::dyn_push`].
    ///
    /// # Concurrency
    ///
    /// This method partitions updates across workers and then buffers them
    /// atomically with respect to each worker, i.e., each worker observes
    /// all updates in an `append` at the same logical time.  However the
    /// operation is not atomic as a whole: concurrent `append` and
    /// `clear_input` calls (performed via clones of the
    /// same `CollectionHandle`) may apply in different orders in different
    /// worker threads.  This method is also not atomic with respect to
    /// [`DBSPHandle::step`](`crate::DBSPHandle::step`) and
    /// [`CircuitHandle::step`](`crate::CircuitHandle::step`) methods: a
    /// `DBSPHandle::step` call performed concurrently with `append` may
    /// result in only a subset of the workers observing updates from this
    /// `append` operation.  The remaining updates will appear
    /// during subsequent logical clock cycles.
    pub fn dyn_append(&self, vals: &mut Box<DynPairs<K, V>>) {
        let num_partitions = self.num_partitions();
        let next_worker = if num_partitions > 1 {
            self.next_worker.load(Ordering::Acquire)
        } else {
            0
        };

        // We divide `val` across `num_partitions` workers as evenly as we can.  The
        // first `remainder` workers will receive `quotient + 1` values, and the
        // rest will receive `quotient`.
        let quotient = vals.len() / num_partitions;
        let remainder = vals.len() % num_partitions;
        let worker_ofs = self.input_handle.workers().start;
        for i in 0..num_partitions {
            let mut partition_size = quotient;
            if i < remainder {
                partition_size += 1;
            }

            let worker = (next_worker + i) % num_partitions + worker_ofs;
            if partition_size == vals.len() {
                self.input_handle.update_for_worker(worker, |tuples| {
                    if tuples.is_empty() {
                        swap(tuples, vals);
                    } else {
                        tuples.append(vals.as_vec_mut());
                    }
                });
                break;
            }

            // Draining from the end should be more efficient as it doesn't
            // require memcpy'ing the tail of the vector to the front.
            self.input_handle.update_for_worker(worker, |tuples| {
                let len = vals.len();
                tuples.append_range(vals.as_vec_mut(), len - partition_size, len);
            });
            vals.truncate(vals.len() - partition_size);
        }

        assert_eq!(vals.len(), 0);

        // If `remainder` is positive, then the values were not distributed completely
        // evenly. Advance `self.next_worker` so that the next batch of values
        // will give extra values to the ones that didn't get extra this time.
        if remainder > 0 {
            self.next_worker
                .store(next_worker + remainder, Ordering::Release);
        }
    }

    /// Clear all inputs buffered since the start of the last clock cycle.
    ///
    /// # Concurrency
    ///
    /// Similar to [`Self::dyn_append`], this method atomically clears updates
    /// buffered for each worker thread, i.e., the worker observes all or none
    /// of the updates buffered before the call to `clear_input`; however the
    /// operation is not atomic as a whole: concurrent `append` and
    /// `clear_input` calls (performed via clones of the
    /// same `CollectionHandle`) may apply in different orders in different
    /// worker threads.  This method is also not atomic with respect to
    /// [`DBSPHandle::step`](`crate::DBSPHandle::step`) and
    /// [`CircuitHandle::step`](`crate::CircuitHandle::step`) methods: a
    /// `DBSPHandle::step` call performed concurrently with `clear_input` may
    /// result in only a subset of the workers observing empty inputs, while
    /// other workers observe updates buffered prior to the `clear_input` call.
    pub fn clear_input(&self) {
        self.input_handle.clear_for_all();
    }
}

pub trait HashFunc<K: ?Sized>: Fn(&K) -> u32 + Send + Sync {}

impl<K: ?Sized, F> HashFunc<K> for F where F: Fn(&K) -> u32 + Send + Sync {}

/// A handle used to write data to an input stream created by
/// [`add_input_set`](`RootCircuit::add_input_set`) and
/// [`add_input_map`](`RootCircuit::add_input_map`)
/// methods.
///
/// The handle provides an API to push updates to the stream in
/// the form of `(key, value)` tuples:
///
///    * For `add_input_set`, the tuples have the form `(Key, bool)`.
///
///    * For `add_input_map`, the tuples have the form `(Key, Option<Value>)`.
///
/// See [`add_input_set`](`RootCircuit::add_input_set`) and
/// [`add_input_map`](`RootCircuit::add_input_map`) documentation for the exact
/// semantics of these updates.
///
/// Internally, the handle manages an array of mailboxes, one for
/// each worker thread. It automatically partitions updates across
/// mailboxes based on the hash of the key.
/// At the start of each clock cycle, the
/// circuit consumes updates buffered in each mailbox, leaving
/// the mailbox empty.
pub struct UpsertHandle<K: DataTrait + ?Sized, V: DataTrait + ?Sized> {
    pair_factory: &'static dyn Factory<DynPair<K, V>>,
    pairs_factory: &'static dyn Factory<DynPairs<K, V>>,
    buffers: Vec<Box<DynPairs<K, V>>>,
    input_handle: InputHandle<Box<DynPairs<K, V>>>,
    // Sharding the input collection based on the hash of the key is more
    // expensive than simple round robin partitioning used by
    // `CollectionHandle`; however it is necessary here, since the `Upsert`
    // operator requires that all updates to the same key are processed
    // by the same worker thread and in the same order they were pushed
    // by the client.
    hash_func: Arc<dyn HashFunc<K>>,
}

impl<K: DataTrait + ?Sized, V: DataTrait + ?Sized> Clone for UpsertHandle<K, V> {
    fn clone(&self) -> Self {
        // Don't clone buffers.
        Self::with_hasher(
            self.pair_factory,
            self.pairs_factory,
            self.input_handle.clone(),
            self.hash_func.clone(),
        )
    }
}

impl<K: DataTrait + ?Sized, V: DataTrait + ?Sized> UpsertHandle<K, V> {
    fn new(
        pair_factory: &'static dyn Factory<DynPair<K, V>>,
        pairs_factory: &'static dyn Factory<DynPairs<K, V>>,
        input_handle: InputHandle<Box<DynPairs<K, V>>>,
    ) -> Self {
        Self::with_hasher(
            pair_factory,
            pairs_factory,
            input_handle,
            Arc::new(|k: &K| k.default_hash() as u32) as Arc<dyn HashFunc<K>>,
        )
    }

    fn with_hasher(
        pair_factory: &'static dyn Factory<DynPair<K, V>>,
        pairs_factory: &'static dyn Factory<DynPairs<K, V>>,
        input_handle: InputHandle<Box<DynPairs<K, V>>>,
        hash_func: Arc<dyn HashFunc<K>>,
    ) -> Self {
        Self {
            pair_factory,
            pairs_factory,
            buffers: vec![pairs_factory.default_box(); input_handle.0.mailbox.len()],
            input_handle,
            hash_func,
        }
    }

    #[inline]
    fn num_partitions(&self) -> usize {
        self.buffers.len()
    }

    /// Push a single `(key,value)` pair to the input stream.
    pub fn dyn_push(&self, k: &mut K, v: &mut V) {
        let num_partitions = self.num_partitions();

        if num_partitions > 1 {
            self.input_handle.update_for_worker(
                ((self.hash_func)(k) as usize) % num_partitions,
                |tuples| {
                    let mut tuple = self.pair_factory.default_box();
                    tuple.from_vals(k, v);
                    tuples.push_val(&mut *tuple);
                },
            );
        } else {
            self.input_handle.update_for_worker(0, |tuples| {
                let mut tuple = self.pair_factory.default_box();
                tuple.from_vals(k, v);
                tuples.push_val(&mut *tuple);
            });
        }
    }

    /// Push multiple `(key,value)` pairs to the input stream.
    ///
    /// This is more efficient than pushing values one-by-one using
    /// [`Self::dyn_push`].
    ///
    /// # Concurrency
    ///
    /// This method partitions updates across workers and then buffers them
    /// atomically with respect to each worker, i.e., each worker observes
    /// all updates in an `append` at the same logical time.  However the
    /// operation is not atomic as a whole: concurrent `append` and
    /// `clear_input` calls (performed via clones of the
    /// same `UpsertHandle`) may apply in different orders in different
    /// worker threads.  This method is also not atomic with respect to
    /// [`DBSPHandle::step`](`crate::DBSPHandle::step`) and
    /// [`CircuitHandle::step`](`crate::CircuitHandle::step`) methods: a
    /// `DBSPHandle::step` call performed concurrently with `append` may
    /// result in only a subset of the workers observing updates from this
    /// `append` operation.  The remaining updates will appear
    /// during subsequent logical clock cycles.
    pub fn dyn_append(&mut self, vals: &mut Box<DynPairs<K, V>>) {
        let num_partitions = self.num_partitions();

        if num_partitions > 1 {
            for kv in vals.dyn_iter_mut() {
                let k = kv.fst();
                self.buffers[((self.hash_func)(k) as usize) % num_partitions].push_val(kv)
            }
            vals.clear();
            for worker in 0..num_partitions {
                self.input_handle.update_for_worker(worker, |tuples| {
                    if tuples.is_empty() {
                        *tuples =
                            replace(&mut self.buffers[worker], self.pairs_factory.default_box());
                    } else {
                        tuples.append(self.buffers[worker].as_vec_mut());
                    }
                })
            }
        } else {
            self.input_handle.update_for_worker(0, |tuples| {
                if tuples.is_empty() {
                    *tuples = replace(vals, self.pairs_factory.default_box());
                } else {
                    tuples.append(vals.as_vec_mut());
                }
            });
        }
    }

    /// Clear all inputs buffered since the start of the last clock cycle.
    ///
    /// # Concurrency
    ///
    /// Similar to [`Self::dyn_append`], this method atomically clears updates
    /// buffered for each worker thread, i.e., the worker observes all or none
    /// of the updates buffered before the call to `clear_input`; however the
    /// operation is not atomic as a whole: concurrent `append` and
    /// `clear_input` calls (performed via clones of the
    /// same `UpsertHandle`) may apply in different orders in different
    /// worker threads.  This method is also not atomic with respect to
    /// [`DBSPHandle::step`](`crate::DBSPHandle::step`) and
    /// [`CircuitHandle::step`](`crate::CircuitHandle::step`) methods: a
    /// `DBSPHandle::step` call performed concurrently with `clear_input` may
    /// result in only a subset of the workers observing empty inputs, while
    /// other workers observe updates buffered prior to the `clear_input` call.
    pub fn clear_input(&self) {
        self.input_handle.clear_for_all();
    }
}

#[cfg(test)]
mod test {
    use crate::{
        dynamic::{DowncastTrait, DynData, Erase},
        indexed_zset,
        operator::{
            input::InputHandle, IndexedZSetHandle, MapHandle, SetHandle, Update, ZSetHandle,
        },
        trace::{BatchReaderFactories, Builder, Cursor},
        typed_batch::{
            BatchReader, DynBatch, DynBatchReader, DynOrdZSet, OrdIndexedZSet, OrdZSet, TypedBatch,
            TypedBox,
        },
        utils::Tup2,
        zset, RootCircuit, Runtime, Stream, ZWeight,
    };
    use anyhow::Result as AnyResult;
    use std::{cmp::max, iter::once, ops::Mul};

    fn input_batches() -> Vec<OrdZSet<u64>> {
        vec![
            zset! { 1u64 => 1, 2 => 1, 3 => 1 },
            zset! { 5u64 => -1, 10 => 2, 11 => 11 },
            zset! {},
        ]
    }

    fn input_vecs() -> Vec<Vec<Tup2<u64, ZWeight>>> {
        input_batches()
            .into_iter()
            .map(|batch| {
                let mut cursor = batch.cursor();
                let mut result = Vec::new();

                while cursor.key_valid() {
                    result.push(Tup2(
                        *cursor.key().downcast_checked::<u64>(),
                        *cursor.weight().downcast_checked::<ZWeight>(),
                    ));
                    cursor.step_key();
                }
                result
            })
            .collect()
    }

    fn input_test_circuit(
        circuit: &RootCircuit,
        nworkers: usize,
    ) -> AnyResult<InputHandle<OrdZSet<u64>>> {
        let (stream, handle) = circuit.add_input_stream::<OrdZSet<u64>>();

        let mut expected_batches = input_batches().into_iter().chain(input_batches()).chain(
            input_batches().into_iter().map(move |batch| {
                //let mut result = batch.clone();
                let mut cursor = batch.inner().cursor();
                let mut result = <DynOrdZSet<DynData> as DynBatch>::Builder::with_capacity(
                    &BatchReaderFactories::new::<u64, (), ZWeight>(),
                    (),
                    batch.len(),
                );

                while cursor.key_valid() {
                    let w = cursor
                        .weight()
                        .downcast_checked::<ZWeight>()
                        .mul(nworkers as i64);
                    result.push_refs(cursor.key(), ().erase(), w.erase());
                    cursor.step_key();
                }
                TypedBatch::new(result.done())
            }),
        );

        stream.gather(0).inspect(move |batch| {
            if Runtime::worker_index() == 0 {
                assert_eq!(batch, &expected_batches.next().unwrap())
            }
        });

        Ok(handle)
    }

    #[test]
    fn input_test_st() {
        let (circuit, input_handle) =
            RootCircuit::build(move |circuit| input_test_circuit(circuit, 1)).unwrap();

        for batch in input_batches().into_iter() {
            input_handle.set_for_worker(0, batch);
            circuit.step().unwrap();
        }

        for batch in input_batches().into_iter() {
            input_handle.update_for_worker(0, |b| *b = batch);
            circuit.step().unwrap();
        }

        for batch in input_batches().into_iter() {
            input_handle.set_for_all(batch);
            circuit.step().unwrap();
        }
    }

    fn input_test_mt(workers: usize) {
        let (mut dbsp, input_handle) =
            Runtime::init_circuit(workers, move |circuit| input_test_circuit(circuit, workers))
                .unwrap();

        for (round, batch) in input_batches().into_iter().enumerate() {
            input_handle.set_for_worker(round % workers, batch);
            dbsp.step().unwrap();
        }

        for (round, batch) in input_batches().into_iter().enumerate() {
            input_handle.update_for_worker(round % workers, |b| *b = batch);
            dbsp.step().unwrap();
        }

        for batch in input_batches().into_iter() {
            input_handle.set_for_all(batch);
            dbsp.step().unwrap();
        }

        dbsp.kill().unwrap();
    }

    #[test]
    fn input_test_mt1() {
        input_test_mt(1);
    }

    #[test]
    fn input_test_mt4() {
        input_test_mt(4);
    }

    fn zset_test_circuit(circuit: &RootCircuit) -> AnyResult<ZSetHandle<u64>> {
        let (stream, handle) = circuit.add_input_zset::<u64>();

        let mut expected_batches = input_batches()
            .into_iter()
            .chain(input_batches())
            .chain(once(zset! {}));
        stream.gather(0).inspect(move |batch| {
            if Runtime::worker_index() == 0 {
                assert_eq!(batch, &expected_batches.next().unwrap())
            }
        });

        Ok(handle)
    }

    #[test]
    fn zset_test_st() {
        let (circuit, input_handle) =
            RootCircuit::build(move |circuit| zset_test_circuit(circuit)).unwrap();

        for mut vec in input_vecs().into_iter() {
            input_handle.append(&mut vec);
            circuit.step().unwrap();
        }

        for vec in input_vecs().into_iter() {
            for Tup2(k, w) in vec.into_iter() {
                input_handle.push(k, w);
            }
            input_handle.push(5, 1);
            input_handle.push(5, -1);
            circuit.step().unwrap();
        }

        for mut vec in input_vecs().into_iter() {
            input_handle.append(&mut vec);
        }
        input_handle.clear_input();
        circuit.step().unwrap();
    }

    fn zset_test_mt(workers: usize) {
        let (mut dbsp, input_handle) =
            Runtime::init_circuit(workers, |circuit| zset_test_circuit(circuit)).unwrap();

        for mut vec in input_vecs().into_iter() {
            input_handle.append(&mut vec);
            dbsp.step().unwrap();
        }

        for vec in input_vecs().into_iter() {
            for Tup2(k, w) in vec.into_iter() {
                input_handle.push(k, w);
            }
            input_handle.push(5, 1);
            input_handle.push(5, -1);
            dbsp.step().unwrap();
        }

        for mut vec in input_vecs().into_iter() {
            input_handle.append(&mut vec);
        }
        input_handle.clear_input();
        dbsp.step().unwrap();

        dbsp.kill().unwrap();
    }

    #[test]
    fn zset_test_mt1() {
        zset_test_mt(1);
    }

    #[test]
    fn zset_test_mt4() {
        zset_test_mt(4);
    }

    fn input_indexed_batches() -> Vec<OrdIndexedZSet<u64, u64>> {
        vec![
            indexed_zset! { 1u64 => {1u64 => 1, 2 => 1}, 2 => { 3 => 1 }, 3 => {4 => -1, 5 => 5} },
            indexed_zset! { 5u64 => {10u64 => -1}, 10 => {2 => 1, 3 => -1}, 11 => {11 => 11} },
            indexed_zset! {},
        ]
    }

    fn input_indexed_vecs() -> Vec<Vec<Tup2<u64, Tup2<u64, i64>>>> {
        input_indexed_batches()
            .into_iter()
            .map(|batch| {
                let mut cursor = batch.cursor();
                let mut result = Vec::new();

                while cursor.key_valid() {
                    while cursor.val_valid() {
                        result.push(Tup2(
                            *cursor.key().downcast_checked::<u64>(),
                            Tup2(
                                *cursor.val().downcast_checked::<u64>(),
                                *cursor.weight().downcast_checked::<ZWeight>(),
                            ),
                        ));
                        cursor.step_val();
                    }
                    cursor.step_key();
                }
                result
            })
            .collect()
    }

    fn indexed_zset_test_circuit(circuit: &RootCircuit) -> AnyResult<IndexedZSetHandle<u64, u64>> {
        let (stream, handle) = circuit.add_input_indexed_zset::<u64, u64>();

        let mut expected_batches = input_indexed_batches()
            .into_iter()
            .chain(input_indexed_batches());
        stream.gather(0).inspect(move |batch| {
            if Runtime::worker_index() == 0 {
                assert_eq!(batch, &expected_batches.next().unwrap())
            }
        });

        Ok(handle)
    }

    #[test]
    fn indexed_zset_test_st() {
        let (circuit, input_handle) =
            RootCircuit::build(move |circuit| indexed_zset_test_circuit(circuit)).unwrap();

        for mut vec in input_indexed_vecs().into_iter() {
            input_handle.append(&mut vec);
            circuit.step().unwrap();
        }

        for vec in input_indexed_vecs().into_iter() {
            for Tup2(k, v) in vec.into_iter() {
                input_handle.push(k, (v.0, v.1));
            }
            input_handle.push(5, (7, 1));
            input_handle.push(5, (7, -1));
            circuit.step().unwrap();
        }
    }

    fn indexed_zset_test_mt(workers: usize) {
        let (mut dbsp, input_handle) =
            Runtime::init_circuit(workers, |circuit| indexed_zset_test_circuit(circuit)).unwrap();

        for mut vec in input_indexed_vecs().into_iter() {
            input_handle.append(&mut vec);
            dbsp.step().unwrap();
        }

        for vec in input_indexed_vecs().into_iter() {
            for Tup2(k, v) in vec.into_iter() {
                input_handle.push(k, (v.0, v.1));
            }
            dbsp.step().unwrap();
        }

        dbsp.kill().unwrap();
    }

    #[test]
    fn indexed_zset_test_mt1() {
        indexed_zset_test_mt(1);
    }

    #[test]
    fn indexed_zset_test_mt4() {
        indexed_zset_test_mt(4);
    }

    fn input_set_updates() -> Vec<Vec<Tup2<u64, bool>>> {
        vec![
            vec![Tup2(1, true), Tup2(2, true), Tup2(3, false)],
            vec![Tup2(1, false), Tup2(2, true), Tup2(3, true), Tup2(4, true)],
            vec![Tup2(2, false), Tup2(2, true), Tup2(3, true), Tup2(4, false)],
            vec![Tup2(2, true), Tup2(2, false)],
            vec![Tup2(100, true)],
            vec![Tup2(95, true)],
            // below watermark
            vec![Tup2(80, true)],
        ]
    }

    fn output_set_updates() -> Vec<OrdZSet<u64>> {
        vec![
            zset! { 1u64 => 1,  2 => 1},
            zset! { 1 => -1, 3 => 1,  4 => 1 },
            zset! { 4 => -1 },
            zset! { 2 => -1 },
            zset! { 100 => 1 },
            zset! { 95 => 1 },
            zset! {},
        ]
    }

    fn set_test_circuit(circuit: &RootCircuit) -> AnyResult<SetHandle<u64>> {
        let (stream, handle) = circuit.add_input_set::<u64>();
        let watermark: Stream<_, TypedBox<u64, DynData>> =
            stream.waterline(|| 0, |k, ()| *k, |k1, k2| max(*k1, *k2));
        stream.integrate_trace_retain_keys(&watermark, |k, ts: &u64| *k >= ts.saturating_sub(10));

        let mut expected_batches = output_set_updates().into_iter();

        stream.gather(0).inspect(move |batch| {
            if Runtime::worker_index() == 0 {
                assert_eq!(batch, &expected_batches.next().unwrap())
            }
        });

        Ok(handle)
    }

    #[test]
    fn set_test_st() {
        let (circuit, mut input_handle) =
            RootCircuit::build(move |circuit| set_test_circuit(circuit)).unwrap();

        for mut vec in input_set_updates().into_iter() {
            input_handle.append(&mut vec);
            circuit.step().unwrap();
        }

        let (circuit, input_handle) =
            RootCircuit::build(move |circuit| set_test_circuit(circuit)).unwrap();

        for vec in input_set_updates().into_iter() {
            for Tup2(k, b) in vec.into_iter() {
                input_handle.push(k, b);
            }
            circuit.step().unwrap();
        }
    }

    fn set_test_mt(workers: usize) {
        let (mut dbsp, mut input_handle) =
            Runtime::init_circuit(workers, |circuit| set_test_circuit(circuit)).unwrap();

        for mut vec in input_set_updates().into_iter() {
            input_handle.append(&mut vec);
            dbsp.step().unwrap();
        }

        dbsp.kill().unwrap();

        let (mut dbsp, input_handle) =
            Runtime::init_circuit(workers, |circuit| set_test_circuit(circuit)).unwrap();

        for vec in input_set_updates().into_iter() {
            for Tup2(k, b) in vec.into_iter() {
                input_handle.push(k, b);
            }
            dbsp.step().unwrap();
        }

        dbsp.kill().unwrap();
    }

    #[test]
    fn set_test_mt1() {
        set_test_mt(1);
    }

    #[test]
    fn set_test_mt4() {
        set_test_mt(4);
    }

    fn input_map_updates1() -> Vec<Vec<Tup2<u64, Update<u64, i64>>>> {
        vec![
            vec![
                Tup2(1, Update::Insert(1)),
                Tup2(1, Update::Insert(2)),
                Tup2(2, Update::Delete),
                Tup2(3, Update::Insert(3)),
            ],
            vec![
                Tup2(1, Update::Insert(1)),
                Tup2(1, Update::Delete),
                Tup2(2, Update::Insert(2)),
                Tup2(3, Update::Insert(4)),
                Tup2(4, Update::Insert(4)),
                Tup2(4, Update::Delete),
                Tup2(4, Update::Insert(5)),
            ],
            vec![
                Tup2(1, Update::Insert(5)),
                Tup2(1, Update::Insert(6)),
                Tup2(3, Update::Delete),
                Tup2(4, Update::Insert(6)),
            ],
            // bump watermark
            vec![Tup2(1, Update::Insert(100))],
            // below watermark
            vec![Tup2(1, Update::Insert(80))],
            vec![Tup2(1, Update::Insert(91))],
            // bump watermark more
            vec![Tup2(5, Update::Insert(200))],
            // below watermark
            vec![Tup2(5, Update::Insert(91))],
            vec![Tup2(5, Update::Insert(191))],
        ]
    }

    fn output_map_updates1() -> Vec<OrdIndexedZSet<u64, u64>> {
        vec![
            indexed_zset! { 1u64 => {2u64 => 1},  3 => {3 => 1}},
            indexed_zset! { 1 => {2 => -1}, 2 => {2 => 1}, 3 => {3 => -1, 4 => 1}, 4 => {5 => 1}},
            indexed_zset! { 1 => {6 => 1},  3 => {4 => -1}, 4 => {5 => -1, 6 => 1}},
            indexed_zset! { 1 => {6 => -1, 100 => 1}},
            indexed_zset! { 1 => { 100 => -1, 80 => 1 }},
            indexed_zset! { 1 => {91 => 1, 80 => -1}},
            indexed_zset! { 5 => {200 => 1}},
            indexed_zset! { 5 => { 200 => -1, 91 => 1 }},
            indexed_zset! { 5 => {191 => 1, 91 => -1}},
        ]
    }
    fn input_map_updates2() -> Vec<Vec<Tup2<u64, Update<u64, i64>>>> {
        vec![
            vec![
                // Insert and instantly update: values add up.
                Tup2(1, Update::Insert(1)),
                Tup2(1, Update::Update(1)),
                // Insert and intantly overwrite: the last value is used.
                Tup2(2, Update::Insert(1)),
                Tup2(2, Update::Insert(1)),
                // Insert and instantly delete.
                Tup2(3, Update::Insert(1)),
                Tup2(3, Update::Delete),
                // Delete non-existing value - ignored.
                Tup2(4, Update::Delete),
            ],
            vec![
                // Two more updates added to existing value.
                Tup2(1, Update::Update(1)),
                Tup2(1, Update::Update(1)),
                // Delete and then try to update the value. The update is ignored.
                Tup2(2, Update::Delete),
                Tup2(2, Update::Update(1)),
                // Update missing value and then insert. The update is ignored.
                Tup2(3, Update::Update(1)),
                Tup2(3, Update::Insert(5)),
            ],
            vec![
                // Updates followed by a delete.
                Tup2(1, Update::Update(2)),
                Tup2(1, Update::Update(3)),
                Tup2(1, Update::Delete),
                // Insert -> update -> delete.
                Tup2(2, Update::Insert(3)),
                Tup2(2, Update::Update(4)),
                Tup2(2, Update::Delete),
                // Insert the same value - noop.
                Tup2(3, Update::Insert(5)),
            ],
            vec![Tup2(1, Update::Insert(1)), Tup2(2, Update::Insert(5))],
            // Push waterline to 15.
            vec![Tup2(3, Update::Update(10))],
            vec![
                // Attempt to update value below waterline - ignored
                Tup2(1, Update::Update(10)),
                // Update value above waterline - accepted.
                Tup2(2, Update::Update(10)),
            ],
            vec![
                // Attempt to delete value below waterline - ignored
                Tup2(1, Update::Delete),
                // Overwrite value above waterline with a value below - ignored
                Tup2(2, Update::Insert(4)),
                // Attempt to create new value below waterline - ignored
                Tup2(4, Update::Insert(1)),
            ],
            vec![
                // Attempt to insert new value overwriting value below waterline.
                //
                // This is commented out because the behavior depends on whether
                // the spine has already filtered out (1,1):
                //
                // - If it has, then the insertion succeeds.
                //
                // - If it hasn't, then the insertion is ignored.
                //Tup2(1, Update::Insert(20)),
                // Overwrite value above waterline with a new value above waterline - accepted
                Tup2(2, Update::Insert(10)),
                // Create new value above waterline - accepted, try to overwrite it with a value
                // below waterline - ignored.
                Tup2(4, Update::Insert(15)),
                Tup2(4, Update::Insert(4)),
            ],
            vec![
                // Attempt to update value below waterline.
                //
                // This is commented out because the behavior depends on whether
                // the spine has already filtered out (1,1):
                //
                // - If it has, then the update succeeds.
                //
                // - If it hasn't, then the update is ignored because the
                //   previous value was below waterline, even though the new value
                //   is above it.
                //Tup2(1, Update::Update(20)),
            ],
        ]
    }

    fn output_map_updates2() -> Vec<OrdIndexedZSet<u64, u64>> {
        vec![
            indexed_zset! { 1 => {2 => 1}, 2 => {1 => 1}},
            indexed_zset! { 1 => {2 => -1, 4 => 1}, 2 => {1 => -1}, 3 => { 5 => 1 } },
            indexed_zset! { 1 => {4 => -1} },
            indexed_zset! { 1 => {1 => 1}, 2 => {5=>1} },
            indexed_zset! { 3 => {5 => -1, 15 => 1} },
            indexed_zset! { 1 => {1 => -1, 11 => 1 } , 2 => {5 => -1, 15 => 1} },
            indexed_zset! { 1 => {11 => -1}, 2 => { 15 => -1, 4 =>  1}, 4 => { 1 => 1}},
            indexed_zset! {2 => {4 => -1, 10 => 1}, 4 => {1 => -1, 4 => 1}},
            indexed_zset! {},
        ]
    }

    fn map_test_circuit(
        circuit: &RootCircuit,
        expected_outputs: fn() -> Vec<OrdIndexedZSet<u64, u64>>,
    ) -> AnyResult<MapHandle<u64, u64, i64>> {
        let (stream, handle) =
            circuit.add_input_map::<u64, u64, i64, _>(|v, u| *v = ((*v as i64) + u) as u64);

        let mut expected_batches = expected_outputs().into_iter();

        stream.gather(0).inspect(move |batch| {
            if Runtime::worker_index() == 0 {
                assert_eq!(batch, &expected_batches.next().unwrap())
            }
        });

        Ok(handle)
    }

    // FIXME: the inputs to these tests are meant to exercise the logic that filters inputs based
    // on lateness, but it does not currently work correctly (see https://github.com/feldera/feldera/issues/2669).
    // We therefore don't use waterlines in tests and check for the standard upsert behavior
    // without filtering.
    #[test]
    fn map_test_st() {
        let (circuit, mut input_handle) =
            RootCircuit::build(move |circuit| map_test_circuit(circuit, output_map_updates1))
                .unwrap();

        for mut vec in input_map_updates1().into_iter() {
            input_handle.append(&mut vec);
            circuit.step().unwrap();
        }

        let (circuit, input_handle) =
            RootCircuit::build(move |circuit| map_test_circuit(circuit, output_map_updates1))
                .unwrap();

        for vec in input_map_updates1().into_iter() {
            for Tup2(k, v) in vec.into_iter() {
                input_handle.push(k, v);
            }
            circuit.step().unwrap();
        }
    }

    fn map_test_mt(
        workers: usize,
        inputs: fn() -> Vec<Vec<Tup2<u64, Update<u64, i64>>>>,
        expected_outputs: fn() -> Vec<OrdIndexedZSet<u64, u64>>,
    ) {
        let expected_outputs_clone = expected_outputs;

        let (mut dbsp, mut input_handle) = Runtime::init_circuit(workers, move |circuit| {
            map_test_circuit(circuit, expected_outputs_clone)
        })
        .unwrap();

        for mut vec in inputs().into_iter() {
            input_handle.append(&mut vec);
            dbsp.step().unwrap();
        }

        dbsp.kill().unwrap();

        let (mut dbsp, input_handle) = Runtime::init_circuit(workers, move |circuit| {
            map_test_circuit(circuit, expected_outputs)
        })
        .unwrap();

        for vec in inputs().into_iter() {
            for Tup2(k, v) in vec.into_iter() {
                input_handle.push(k, v);
            }
            dbsp.step().unwrap();
        }

        dbsp.kill().unwrap();
    }

    // FIXME: the inputs to these tests are meant to exercise the logic that filters inputs based
    // on lateness, but it does not currently work correctly (see https://github.com/feldera/feldera/issues/2669).
    // We therefore don't use waterlines in tests and check for the standard upsert behavior
    // without filtering.
    #[test]
    fn map_test_mt1() {
        map_test_mt(1, input_map_updates1, output_map_updates1);
        map_test_mt(1, input_map_updates2, output_map_updates2);
    }

    #[test]
    fn map_test_mt4() {
        map_test_mt(4, input_map_updates1, output_map_updates1);
        map_test_mt(4, input_map_updates2, output_map_updates2);
    }
}
