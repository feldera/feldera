use crate::{
    circuit::{
        metadata::OperatorLocation,
        operator_traits::{Operator, SourceOperator},
        LocalStoreMarker, Scope,
    },
    dynamic::{DowncastTrait, DynBool, DynData, DynPair, DynUnit, Erase, LeanVec},
    operator::dynamic::{
        input::{
            AddInputIndexedZSetFactories, AddInputMapFactories, AddInputSetFactories,
            AddInputZSetFactories, CollectionHandle, UpsertHandle,
        },
        input_upsert::DynUpdate,
    },
    typed_batch::{OrdIndexedZSet, OrdZSet},
    utils::Tup2,
    Circuit, DBData, DynZWeight, RootCircuit, Runtime, Stream, ZWeight,
};
use std::{
    borrow::{Borrow, Cow},
    fmt::Debug,
    hash::{Hash, Hasher},
    marker::PhantomData,
    mem::{replace, take, transmute},
    ops::{Deref, Range},
    panic::Location,
    sync::{Arc, Mutex},
};
use typedmap::TypedMapKey;

pub use crate::operator::dynamic::input_upsert::{PatchFunc, Update};

pub type IndexedZSetStream<K, V> = Stream<RootCircuit, OrdIndexedZSet<K, V>>;
pub type ZSetStream<K> = Stream<RootCircuit, OrdZSet<K>>;

#[repr(transparent)]
pub struct ZSetHandle<K> {
    handle: CollectionHandle<DynPair<DynData, DynUnit>, DynZWeight>,
    phantom: PhantomData<fn(&K)>,
}

impl<K> Clone for ZSetHandle<K> {
    fn clone(&self) -> Self {
        Self {
            handle: self.handle.clone(),
            phantom: PhantomData,
        }
    }
}

impl<K> Deref for ZSetHandle<K> {
    type Target = CollectionHandle<DynPair<DynData, DynUnit>, DynZWeight>;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}

impl<K> ZSetHandle<K>
where
    K: DBData,
{
    fn new(handle: CollectionHandle<DynPair<DynData, DynUnit>, DynZWeight>) -> Self {
        Self {
            handle,
            phantom: PhantomData,
        }
    }

    pub fn push(&self, k: K, mut w: ZWeight) {
        self.handle.dyn_push(Tup2(k, ()).erase_mut(), w.erase_mut())
    }

    pub fn append(&self, vals: &mut Vec<Tup2<K, ZWeight>>) {
        // SAFETY: `()` is a zero-sized type, more precisely it's a 1-ZST.
        // According to the Rust spec adding it to a tuple doesn't change
        // its memory layout.
        let vals: &mut Vec<Tup2<Tup2<K, ()>, ZWeight>> = unsafe { transmute(vals) };
        let vals = Box::new(LeanVec::from(take(vals)));

        self.handle.dyn_append(&mut vals.erase_box())
    }
}

#[derive(Clone)]
#[repr(transparent)]
pub struct IndexedZSetHandle<K, V> {
    handle: CollectionHandle<DynData, DynPair<DynData, DynZWeight>>,
    phantom: PhantomData<fn(&K, &V)>,
}

impl<K, V> IndexedZSetHandle<K, V>
where
    K: DBData,
    V: DBData,
{
    fn new(handle: CollectionHandle<DynData, DynPair<DynData, DynZWeight>>) -> Self {
        Self {
            handle,
            phantom: PhantomData,
        }
    }

    pub fn push(&self, mut k: K, (v, w): (V, ZWeight)) {
        self.handle.dyn_push(k.erase_mut(), Tup2(v, w).erase_mut())
    }

    pub fn append(&self, vals: &mut Vec<Tup2<K, Tup2<V, ZWeight>>>) {
        let vals = Box::new(LeanVec::from(take(vals)));
        self.handle.dyn_append(&mut vals.erase_box())
    }
}

#[repr(transparent)]
pub struct SetHandle<K> {
    handle: UpsertHandle<DynData, DynBool>,
    phantom: PhantomData<fn(&K)>,
}

impl<K> Clone for SetHandle<K> {
    fn clone(&self) -> Self {
        Self {
            handle: self.handle.clone(),
            phantom: PhantomData,
        }
    }
}

impl<K> SetHandle<K>
where
    K: DBData,
{
    fn new(handle: UpsertHandle<DynData, DynBool>) -> Self {
        Self {
            handle,
            phantom: PhantomData,
        }
    }

    pub fn push(&self, mut k: K, mut v: bool) {
        self.handle.dyn_push(k.erase_mut(), v.erase_mut())
    }

    pub fn append(&mut self, vals: &mut Vec<Tup2<K, bool>>) {
        let vals = Box::new(LeanVec::from(take(vals)));
        self.handle.dyn_append(&mut vals.erase_box())
    }
}

#[repr(transparent)]
pub struct MapHandle<K, V, U> {
    handle: UpsertHandle<DynData, DynUpdate<DynData, DynData>>,
    phantom: PhantomData<fn(&K, &V, &U)>,
}

impl<K, V, U> Clone for MapHandle<K, V, U> {
    fn clone(&self) -> Self {
        Self {
            handle: self.handle.clone(),
            phantom: PhantomData,
        }
    }
}

impl<K, V, U> MapHandle<K, V, U>
where
    K: DBData,
    V: DBData,
    U: DBData,
{
    fn new(handle: UpsertHandle<DynData, DynUpdate<DynData, DynData>>) -> Self {
        Self {
            handle,
            phantom: PhantomData,
        }
    }

    pub fn push(&self, mut k: K, mut upd: Update<V, U>) {
        self.handle.dyn_push(k.erase_mut(), upd.erase_mut())
    }

    pub fn append(&mut self, vals: &mut Vec<Tup2<K, Update<V, U>>>) {
        let vals = Box::new(LeanVec::from(take(vals)));
        self.handle.dyn_append(&mut vals.erase_box())
    }
}

impl RootCircuit {
    /// Create an input stream that carries values of type `T`.
    ///
    /// Input streams are used to push data to the circuit from the outside
    /// world via the [`InputHandle`] object returned by this method:
    ///
    /// ```text
    ///                   ┌──────────────────────┐
    ///                   │Circuit               │
    ///                   │                      │
    /// ┌───────────┐     │   stream             │
    /// │InputHandle├──────────────────►         │
    /// └───────────┘     │                      │
    ///                   │                      │
    ///                   └──────────────────────┘
    /// ```
    ///
    /// At each clock cycle, the stream consumes the last value placed in it via
    /// the `InputHandle` (or `<T as Default>::default()` if no value was
    /// placed in the stream since the last clock cycle) and yields this
    /// value to all downstream operators connected to it.
    ///
    /// See [`InputHandle`] for more details.
    #[track_caller]
    pub fn add_input_stream<T>(&self) -> (Stream<Self, T>, InputHandle<T>)
    where
        T: Default + Debug + Clone + Send + 'static,
    {
        let (input, input_handle) =
            Input::new(Location::caller(), |x| x, Arc::new(|| Default::default()));
        let stream = self.add_source(input);
        (stream, input_handle)
    }

    /// Create an input stream that carries values of type
    /// [`OrdZSet<K>`](`OrdZSet`).
    ///
    /// Creates an input stream that carries values of type `OrdZSet<K>` and
    /// an input handle of type [`ZSetHandle<K>`](`ZSetHandle`)
    /// used to construct input Z-sets out of individual elements.  The
    /// client invokes [`ZSetHandle::push`] and
    /// [`ZSetHandle::append`] any number of times to add values to
    /// the input Z-set. These values are distributed across all worker
    /// threads (when running in a multithreaded [`Runtime`]) in a round-robin
    /// fashion and buffered until the start of the next clock
    /// cycle.  At the start of a clock cycle (triggered by
    /// [`DBSPHandle::step`](`crate::DBSPHandle::step`) or
    /// [`CircuitHandle::step`](`crate::CircuitHandle::step`)), the circuit
    /// reads all buffered values and assembles them into an `OrdZSet`.
    ///
    /// See [`CollectionHandle`] for more details.
    #[track_caller]
    pub fn add_input_zset<K>(&self) -> (Stream<RootCircuit, OrdZSet<K>>, ZSetHandle<K>)
    where
        K: DBData,
    {
        let factories = AddInputZSetFactories::new::<K>();
        let (stream, handle) = self.dyn_add_input_zset_mono(&factories);

        (stream.typed(), ZSetHandle::new(handle))
    }

    /// Create an input stream that carries values of type
    /// [`OrdIndexedZSet<K, V>`](`OrdIndexedZSet`).
    ///
    /// Creates an input stream that carries values of type `OrdIndexedZSet<K, V>`
    /// and an input handle of type [`IndexedZSetHandle<K, V>`](`IndexedZSetHandle`)
    /// used to construct input Z-sets out of individual elements.  The client
    /// invokes [`IndexedZSetHandle::push`] and [`IndexedZSetHandle::append`] any number
    /// of times to add `key/value/weight` triples to the indexed Z-set. These triples
    /// are distributed across all worker threads (when running in a
    /// multithreaded [`Runtime`]) in a round-robin fashion, and
    /// buffered until the start of the next clock cycle.  At the start of a
    /// clock cycle (triggered by
    /// [`DBSPHandle::step`](`crate::DBSPHandle::step`) or
    /// [`CircuitHandle::step`](`crate::CircuitHandle::step`)), the circuit
    /// reads all buffered values and assembles them into an `OrdIndexedZSet`.
    ///
    /// See [`CollectionHandle`] for more details.
    #[allow(clippy::type_complexity)]
    #[track_caller]
    pub fn add_input_indexed_zset<K, V>(
        &self,
    ) -> (
        Stream<RootCircuit, OrdIndexedZSet<K, V>>,
        IndexedZSetHandle<K, V>,
    )
    where
        K: DBData,
        V: DBData,
    {
        let factories = AddInputIndexedZSetFactories::new::<K, V>();
        let (stream, handle) = self.dyn_add_input_indexed_zset_mono(&factories);

        (stream.typed(), IndexedZSetHandle::new(handle))
    }

    /// Create an input table with set semantics.
    ///
    /// # Motivation
    ///
    /// DBSP represents relational data using Z-sets, i.e., tables where each
    /// record has a weight, which denotes the number of times the record occurs
    /// in the table.  Updates to Z-sets are also Z-sets, with
    /// positive weights representing insertions and negative weights
    /// representing deletions.  The contents of the Z-set after an update
    /// is computed by summing up the weights associated with each record.
    /// Z-set updates are commutative, e.g., insert->insert->delete and
    /// insert->delete->insert sequences are both equivalent to a single
    /// insert.  This internal representation enables efficient incremental
    /// computation, but it does not always match the data model used by the
    /// outside world, and may require a translation layer to eliminate this
    /// mismatch when ingesting data into DBSP.
    ///
    /// In particular, input tables often behave as sets.  A set is a special
    /// case of a Z-set where all weights are equal to 1.  Duplicate
    /// insertions and deletions to sets are ignored, i.e., inserting an
    /// existing element or deleting an element not in the set are both
    /// no-ops.  Set updates are not commutative, e.g., the
    /// insert->delete->insert sequence is equivalent to a single insert,
    /// while insert->insert->delete is equivalent to a delete.
    ///
    /// # Details
    ///
    /// The `add_input_set` operator creates an input table that internally
    /// appears as a Z-set with unit weights, but that ingests input data
    /// using set semantics. It returns a stream that carries values of type
    /// `OrdZSet<K, R>` and an input handle of type
    /// [`SetHandle<K>`](`SetHandle`).  The client uses
    /// [`SetHandle::push`] and [`SetHandle::append`] to submit
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
    /// Applying [`Stream::integrate_trace_retain_keys`], and
    /// [`Stream::integrate_trace_with_bound`] methods to the stream has the
    /// additional effect of filtering out all values that don't satisfy the
    /// retention policy configured by these methods from the stream.
    /// Specifically, retention conditions configured at logical time `t`
    /// are applied starting from logical time `t+1`.
    // TODO: Add a version that takes a custom hash function.
    #[track_caller]
    pub fn add_input_set<K>(&self) -> (Stream<RootCircuit, OrdZSet<K>>, SetHandle<K>)
    where
        K: DBData,
    {
        let factories = AddInputSetFactories::new::<K>();
        let (stream, handle) = self.dyn_add_input_set_mono(None, &factories);

        (stream.typed(), SetHandle::new(handle))
    }

    /// Create an input table as a key-value map with upsert update semantics.
    ///
    /// # Motivation
    ///
    /// DBSP represents indexed data using indexed Z-sets, i.e., sets
    /// of `(key, value, weight)` tuples, where `weight`
    /// denotes the number of times the key-value pair occurs in
    /// the table.  Updates to indexed Z-sets are also indexed Z-sets, with
    /// positive weights representing insertions and negative weights
    /// representing deletions.  The contents of the indexed Z-set after an
    /// update is computed by summing up weights associated with each
    /// key-value pair. This representation enables efficient incremental
    /// computation, but it does not always match the data model used by the
    /// outside world, and may require a translation layer to eliminate this
    /// mismatch when ingesting indexed data into DBSP.
    ///
    /// In particular, input tables often behave as key-value maps.
    /// A map is a special case of an indexed Z-set where each key has
    /// a unique value associated with it and where all weights are 1.
    /// Map updates follow the update-or-insert (*upsert*) semantics,
    /// where inserting a new key-value pair overwrites the old value
    /// associated with the key, if any.
    ///
    /// # Details
    ///
    /// The `add_input_map` operator creates an input table that internally
    /// appears as an indexed Z-set with all unit weights, but that ingests
    /// input data using upsert semantics. It returns a stream that carries
    /// values of type `OrdIndexedZSet<K, V, R>` and an input handle of type
    /// [`MapHandle<K, V>`](`MapHandle`).  The client uses
    /// [`MapHandle::push`] and [`MapHandle::append`] to submit
    /// commands of the form `(key, Update::Insert(val))` to insert a new
    /// key-value pair, `(key, Update::Delete)` to delete the value
    /// associated with `key`, and `(key, Update::Update)` to modify the
    /// values associated with `key`, if it exists. These commands are
    /// buffered until the start of the next clock cycle.
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
    /// time │      input commands                     │content of the        │ stream returned by         │  comment
    ///      │                                         │input map             │ `add_input_map`            │
    /// ─────┼─────────────────────────────────────────┼──────────────────────┼────────────────────────────┼───────────────────────────────────────────────────────
    ///    1 │{(1,Insert("foo"), (2,Insert("bar"))}    │{(1,"foo"),(2,"bar")} │ {(1,"foo",+1),(2,"bar",+1)}│
    ///    2 │{(1,Insert("foo"), (2,Insert("baz"))}    │{(1,"foo"),(2,"baz")} │ {(2,"bar",-1),(2,"baz",+1)}│ Ignore duplicate insert of (1,"foo"). New value
    ///      |                                         |                      |                            | "baz" for key 2 overwrites the old value "bar".
    ///    3 │{(1,Delete),(2,Insert("bar")),(2,Delete)}│{}                    │ {(1,"foo",-1),(2,"baz",-1)}│ Delete both keys. Upsert (2,"bar") is overridden
    ///      |                                         |                      |                            | by subsequent delete command.
    ///    4 |{(1,Update("new")), (2,Update("bar"))}   |{(1,"foo")}           | {(1,"new")}                | Note that the second update is ignored because
    ///      |                                         |                      |                            | key 2 is not present in the map.
    /// ─────┴─────────────────────────────────────────┴──────────────────────┴────────────────────────────┴────────────────────────────────────────────────────────
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
    /// # Data retention
    ///
    /// Applying the [`Stream::integrate_trace_retain_keys`] to the stream has the
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
    pub fn add_input_map<K, V, U, PF>(
        &self,
        patch_func: PF,
    ) -> (
        Stream<RootCircuit, OrdIndexedZSet<K, V>>,
        MapHandle<K, V, U>,
    )
    where
        K: DBData,
        V: DBData,
        U: DBData + Erase<DynData>,
        PF: Fn(&mut V, &U) + 'static,
    {
        self.add_input_map_persistent(None, patch_func)
    }

    #[track_caller]
    pub fn add_input_map_persistent<K, V, U, PF>(
        &self,
        persistent_id: Option<&str>,
        patch_func: PF,
    ) -> (
        Stream<RootCircuit, OrdIndexedZSet<K, V>>,
        MapHandle<K, V, U>,
    )
    where
        K: DBData,
        V: DBData,
        U: DBData + Erase<DynData>,
        PF: Fn(&mut V, &U) + 'static,
    {
        let factories = AddInputMapFactories::new::<K, V, U>();
        let (stream, handle) = self.dyn_add_input_map_mono(
            persistent_id,
            &factories,
            Box::new(move |v: &mut DynData, u: &DynData| unsafe {
                patch_func(v.downcast_mut::<V>(), u.downcast::<U>())
            }),
        );

        (stream.typed(), MapHandle::new(handle))
    }
}

/// `TypedMapKey` entry used to share InputHandle objects across workers in a
/// runtime. The first worker to create the handle will store it in the map,
/// subsequent workers will get a clone of the same handle.
struct InputId<T> {
    id: usize,
    _marker: PhantomData<T>,
}

unsafe impl<T> Sync for InputId<T> {}

// Implement `Hash`, `Eq` manually to avoid `T: Hash` type bound.
impl<T> Hash for InputId<T> {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.id.hash(state);
    }
}

impl<T> PartialEq for InputId<T> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl<T> Eq for InputId<T> {}

impl<T> InputId<T> {
    fn new(id: usize) -> Self {
        Self {
            id,
            _marker: PhantomData,
        }
    }
}

impl<T> TypedMapKey<LocalStoreMarker> for InputId<T>
where
    T: 'static,
{
    type Value = InputHandle<T>;
}

/// Mailbox that buffers data between the circuit and the outside world.
/// It is used inside an `InputHandle` to store data sent to a worker
/// thread and inside an `OutputHandle` to store data sent by a worker
/// thread to the outside world.
#[derive(Clone)]
pub(crate) struct Mailbox<T> {
    empty: Arc<dyn Fn() -> T + Send + Sync>,
    value: Arc<Mutex<T>>,
}

impl<T: Clone> Mailbox<T> {
    pub(in crate::operator) fn new(empty: Arc<dyn Fn() -> T + Send + Sync>) -> Self {
        let v = empty();
        Self {
            empty,
            value: Arc::new(Mutex::new(v)),
        }
    }

    pub(in crate::operator) fn take(&self) -> T {
        replace(&mut *self.value.lock().unwrap(), (self.empty)())
    }

    pub(super) fn map<F, O: 'static>(&self, func: F) -> O
    where
        F: Fn(&T) -> O,
    {
        func(self.value.lock().unwrap().borrow())
    }

    fn update<F>(&self, f: F)
    where
        F: FnOnce(&mut T),
    {
        f(&mut *self.value.lock().unwrap());
    }

    pub(in crate::operator) fn set(&self, v: T) {
        *self.value.lock().unwrap() = v;
    }

    pub(in crate::operator) fn clear(&self) {
        *self.value.lock().unwrap() = (self.empty)();
    }
}

pub(crate) struct InputHandleInternal<T> {
    pub(crate) mailbox: Vec<Mailbox<T>>,
    offset: usize,
}

impl<T> InputHandleInternal<T>
where
    T: Clone,
{
    // Returns a new `InputHandleInternal` for workers with indexes in the range
    // of `workers`.
    fn new(workers: Range<usize>, empty_val: Arc<dyn Fn() -> T + Send + Sync>) -> Self {
        assert!(!workers.is_empty());
        Self {
            mailbox: workers
                .clone()
                .map(move |_| Mailbox::new(empty_val.clone()))
                .collect(),
            offset: workers.start,
        }
    }

    pub(crate) fn workers(&self) -> Range<usize> {
        self.offset..self.offset + self.mailbox.len()
    }

    fn set_for_worker(&self, worker: usize, v: T) {
        self.mailbox(worker).set(v);
    }

    fn update_for_worker<F>(&self, worker: usize, f: F)
    where
        F: FnOnce(&mut T),
    {
        self.mailbox(worker).update(f);
    }

    /// Send the same value to all workers.
    fn set_for_all(&self, v: T) {
        for i in 0..self.mailbox.len() - 1 {
            self.mailbox[i].set(v.clone());
        }
        self.mailbox[self.mailbox.len() - 1].set(v);
    }

    fn clear_for_all(&self) {
        for mailbox in self.mailbox.iter() {
            mailbox.clear();
        }
    }

    fn mailbox(&self, worker: usize) -> &Mailbox<T> {
        &self.mailbox[worker - self.offset]
    }
}

/// A handle used to write data to an input stream created by
/// the [`RootCircuit::add_input_stream`] method.
///
/// Internally, the handle manages an array of mailboxes, one for
/// each worker thread.  At the start of each clock cycle, the
/// circuit reads the current value from each mailbox and writes
/// it to the input stream associated with the handle, leaving
/// the mailbox empty (more precisely, the mailbox will contain
/// `T::default()`).  The handle is then used to write new values
/// to the mailboxes, which will be consumed at the next
/// logical clock tick.
pub struct InputHandle<T>(pub(crate) Arc<InputHandleInternal<T>>);

impl<T> Clone for InputHandle<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> InputHandle<T>
where
    T: Send + Clone + 'static,
{
    fn new(empty_val: Arc<dyn Fn() -> T + Send + Sync>) -> Self {
        match Runtime::runtime() {
            None => Self(Arc::new(InputHandleInternal::new(0..1, empty_val))),
            Some(runtime) => {
                let input_id = runtime.sequence_next();

                runtime
                    .local_store()
                    .entry(InputId::new(input_id))
                    .or_insert_with(|| {
                        Self(Arc::new(InputHandleInternal::new(
                            runtime.layout().local_workers(),
                            empty_val,
                        )))
                    })
                    .value()
                    .clone()
            }
        }
    }

    /// Returns the range of worker indexes that this input handle covers, that
    /// is, all of the workers on this host (all workers everywhere, for a
    /// single-host circuit).
    pub(crate) fn workers(&self) -> Range<usize> {
        self.0.workers()
    }

    fn mailbox(&self, worker: usize) -> &Mailbox<T> {
        self.0.mailbox(worker)
    }

    /// Write value `v` to the specified worker's mailbox,
    /// overwriting any previous value in the mailbox.
    pub fn set_for_worker(&self, worker: usize, v: T) {
        self.0.set_for_worker(worker, v);
    }

    /// Mutate the contents of the specified worker's mailbox
    /// using closure `f`.
    pub fn update_for_worker<F>(&self, worker: usize, f: F)
    where
        F: FnOnce(&mut T),
    {
        self.0.update_for_worker(worker, f);
    }

    /// Write value `v` to all worker mailboxes.
    pub fn set_for_all(&self, v: T) {
        self.0.set_for_all(v);
    }

    pub fn clear_for_all(&self) {
        self.0.clear_for_all();
    }
}

/// Source operator that injects data received via `InputHandle` to the circuit.
///
/// ```text
///                   ┌───────────────────┐
///                   │Circuit            │
///                   │                   │
/// ┌───────────┐     │    ┌─────┐        │
/// │InputHandle├─────────►│Input├─────►  │
/// └───────────┘     │    └─────┘        │
///                   │                   │
///                   └───────────────────┘
/// ```
pub struct Input<IT, OT, F> {
    location: &'static Location<'static>,
    mailbox: Mailbox<IT>,
    input_func: F,
    phantom: PhantomData<OT>,
}

impl<IT, OT, F> Input<IT, OT, F>
where
    IT: Clone + Send + 'static,
{
    pub fn new(
        location: &'static Location<'static>,
        input_func: F,
        default: Arc<dyn Fn() -> IT + Send + Sync>,
    ) -> (Self, InputHandle<IT>) {
        let handle = InputHandle::new(default);
        let mailbox = handle.mailbox(Runtime::worker_index()).clone();

        let input = Self {
            location,
            mailbox,
            input_func,
            phantom: PhantomData,
        };

        (input, handle)
    }
}

impl<IT, OT, F> Operator for Input<IT, OT, F>
where
    IT: 'static,
    OT: 'static,
    F: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("Input")
    }

    fn location(&self) -> OperatorLocation {
        Some(self.location)
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        false
    }
}

impl<IT, OT, F> SourceOperator<OT> for Input<IT, OT, F>
where
    IT: Clone + Debug + 'static,
    OT: 'static,
    F: Fn(IT) -> OT + 'static,
{
    async fn eval(&mut self) -> OT {
        let v = self.mailbox.take();
        (self.input_func)(v)
    }
}
