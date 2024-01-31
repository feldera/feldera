use crate::circuit::metadata::{
    ALLOCATED_BYTES_LABEL, NUM_ENTRIES_LABEL, SHARED_BYTES_LABEL, USED_BYTES_LABEL,
};
use crate::{
    circuit::{
        metadata::{MetaItem, OperatorMeta},
        operator_traits::{BinaryOperator, Operator, StrictOperator, StrictUnaryOperator},
        Circuit, ExportId, ExportStream, FeedbackConnector, GlobalNodeId, OwnershipPreference,
        Scope, Stream, WithClock,
    },
    circuit_cache_key,
    trace::{cursor::Cursor, Batch, BatchReader, Builder, Filter, Spine, Trace},
    DBData, Timestamp,
};
use crate::{DBTimestamp, IndexedZSet};
use size_of::SizeOf;
use std::{borrow::Cow, cell::RefCell, marker::PhantomData, ops::DerefMut, rc::Rc};

circuit_cache_key!(TraceId<B, D, K, V>(GlobalNodeId => (Stream<B, D>, TraceBounds<K, V>)));
circuit_cache_key!(DelayedTraceId<B, D>(GlobalNodeId => Stream<B, D>));

/// Lower bound on keys or values in a trace.
///
/// Setting the bound to `None` is equivalent to setting it to
/// `T::min_value()`, i.e., the contents of the trace will never
/// get truncated.
///
/// The writer can update the value of the bound at each clock
/// cycle.  The bound can only increase monotonically.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct TraceBound<T>(Rc<RefCell<Option<T>>>);

impl<K> Default for TraceBound<K> {
    fn default() -> Self {
        Self(Rc::new(RefCell::new(None)))
    }
}

impl<K> TraceBound<K>
where
    K: PartialOrd,
{
    pub fn new() -> Self {
        Default::default()
    }

    /// Set the new value of the bound.
    pub fn set(&self, bound: K) {
        debug_assert!(self.0.borrow().as_ref() <= Some(&bound));
        *self.0.borrow_mut() = Some(bound);
    }

    /// Get the current value of the bound.
    pub fn get(&self) -> Option<K>
    where
        K: Clone,
    {
        self.0.borrow().clone()
    }
}

/// Data structure that tracks key and value retainment policies for a
/// trace.
#[derive(Clone)]
pub struct TraceBounds<K, V>(Rc<RefCell<TraceBoundsInner<K, V>>>);

impl<K, V> TraceBounds<K, V>
where
    K: DBData,
    V: DBData,
{
    /// Instantiate `TraceBounds` with empty sets of key and value bounds.
    ///
    /// The caller must add at least one key and one value bound before
    /// running the circuit.
    pub(crate) fn new() -> Self {
        Self(Rc::new(RefCell::new(TraceBoundsInner {
            key_bounds: Vec::new(),
            key_filter: None,
            val_predicate: Predicate::Bounds(Vec::new()),
        })))
    }

    /// Returns `TraceBounds` that prevent any values in the trace from
    /// being truncated.
    pub(crate) fn unbounded() -> Self {
        Self(Rc::new(RefCell::new(TraceBoundsInner {
            key_bounds: vec![TraceBound::new()],
            key_filter: None,
            val_predicate: Predicate::Bounds(vec![TraceBound::new()]),
        })))
    }

    pub(crate) fn add_key_bound(&self, bound: TraceBound<K>) {
        self.0.borrow_mut().key_bounds.push(bound);
    }

    pub(crate) fn set_key_filter(&self, filter: Filter<K>) {
        self.0.borrow_mut().key_filter = Some(filter);
    }

    pub(crate) fn add_val_bound(&self, bound: TraceBound<V>) {
        match &mut self.0.borrow_mut().val_predicate {
            Predicate::Bounds(bounds) => bounds.push(bound),
            Predicate::Filter(_) => {}
        };
    }

    /// Set value retainment condition.  Disables any value bounds
    /// set using [`Self::add_val_bound`].
    pub(crate) fn set_val_filter(&self, filter: Filter<V>) {
        self.0.borrow_mut().val_predicate = Predicate::Filter(filter);
    }

    /// Compute effective key bound as the minimum of all individual
    /// key bounds applied to the trace.
    pub(crate) fn effective_key_bound(&self) -> Option<K> {
        self.0
            .borrow()
            .key_bounds
            .iter()
            .min()
            .expect("At least one trace bound must be set")
            .get()
    }

    /// Set key retainment condition.
    pub(crate) fn key_filter(&self) -> Option<Filter<K>> {
        self.0
            .borrow()
            .key_filter
            .as_ref()
            .map(|filter| filter.fork())
    }

    /// Returns effective value retention condition, computed as the
    /// minimum bound installed using [`Self::add_val_bound`] or as the
    /// condition installed using [`Self::set_val_filter`] (the latter
    /// takes precedence).
    pub(crate) fn effective_val_filter(&self) -> Option<Filter<V>> {
        match &self.0.borrow().val_predicate {
            Predicate::Bounds(bounds) => bounds
                .iter()
                .min()
                .expect("At least one trace bound must be set")
                .get()
                .map(|bound| Box::new(move |v: &V| *v >= bound) as Filter<V>),
            Predicate::Filter(filter) => Some(filter.fork()),
        }
    }
}

/// Value retainment predicate defined as either a set of bounds
/// or a filter condition.
///
/// See [`Stream::integrate_trace_retain_keys`] for details.
enum Predicate<V> {
    Bounds(Vec<TraceBound<V>>),
    Filter(Filter<V>),
}

struct TraceBoundsInner<K, V> {
    /// Key bounds.
    key_bounds: Vec<TraceBound<K>>,
    /// Key retainment condition (can be set at the same time as one
    /// or more key bounds).
    key_filter: Option<Filter<K>>,
    /// Value bounds _or_ retainment condition.
    val_predicate: Predicate<V>,
}

// TODO: add infrastructure to compact the trace during slack time.

/// Add `timestamp` to all tuples in the input batch.
///
/// Given an input batch without timing information (`BatchReader::Time = ()`),
/// generate an output batch by adding the same time `timestamp` to
/// each tuple.
///
/// Most DBSP operators output untimed batches.  When such a batch is
/// added to a trace, the current timestamp must be added to it.
// TODO: this can be implemented more efficiently by having a special batch type
// where all updates have the same timestamp, as this is the only kind of
// batch that we ever create directly in DBSP; batches with multiple timestamps
// are only created as a result of merging.  The main complication is that
// we will need to extend the trace implementation to work with batches of
// multiple types.  This shouldn't be too hard and is on the todo list.
fn batch_add_time<BI, TS, BO>(batch: &BI, timestamp: &TS) -> BO
where
    TS: Timestamp,
    BI: BatchReader<Time = ()>,
    BI::Key: Clone,
    BI::Val: Clone,
    BO: Batch<Key = BI::Key, Val = BI::Val, Time = TS, R = BI::R>,
{
    let mut builder = BO::Builder::with_capacity(timestamp.clone(), batch.len());
    let mut cursor = batch.cursor();
    while cursor.key_valid() {
        while cursor.val_valid() {
            let val = cursor.val().clone();
            let w = cursor.weight();
            builder.push((BO::item_from(cursor.key().clone(), val), w.clone()));
            cursor.step_val();
        }
        cursor.step_key();
    }
    builder.done()
}

/// A key-only [`Spine`] of `C`'s default batch type, with key and weight types
/// taken from `B`.
pub type KeySpine<B, C> = Spine<
    <<C as WithClock>::Time as Timestamp>::OrdKeyBatch<
        <B as BatchReader>::Key,
        <B as BatchReader>::R,
    >,
>;

/// A [`Spine`] of `C`'s default batch type, with key, value, and weight types
/// taken from `B`.
pub type ValSpine<B, C> = Spine<
    <<C as WithClock>::Time as Timestamp>::OrdValBatch<
        <B as BatchReader>::Key,
        <B as BatchReader>::Val,
        <B as BatchReader>::R,
    >,
>;

impl<C, B> Stream<C, B>
where
    C: Circuit,
    B: Clone + 'static,
{
    // TODO: derive timestamp type from the parent circuit.

    /// Record batches in `self` in a trace.
    ///
    /// This operator labels each untimed batch in the stream with the current
    /// timestamp and adds it to a trace.
    pub fn trace(&self) -> Stream<C, ValSpine<B, C>>
    where
        B: IndexedZSet,
        <C as WithClock>::Time: DBTimestamp,
    {
        self.trace_with_bound(TraceBound::new(), TraceBound::new())
    }

    /// Record batches in `self` in a trace with bounds `lower_key_bound` and
    /// `lower_val_bound`.
    ///
    /// ```text
    ///          ┌─────────────┐ trace
    /// self ───►│ TraceAppend ├─────────┐───► output
    ///          └─────────────┘         │
    ///            ▲                     │
    ///            │                     │
    ///            │ local   ┌───────┐   │z1feedback
    ///            └─────────┤Z1Trace│◄──┘
    ///                      └───────┘
    /// ```
    pub fn trace_with_bound(
        &self,
        lower_key_bound: TraceBound<B::Key>,
        lower_val_bound: TraceBound<B::Val>,
    ) -> Stream<C, ValSpine<B, C>>
    where
        B: IndexedZSet,
        <C as WithClock>::Time: DBTimestamp,
    {
        let mut trace_bounds = self.circuit().cache_get_or_insert_with(
            TraceId::new(self.origin_node_id().clone()),
            || {
                let circuit = self.circuit();
                let bounds = TraceBounds::new();

                circuit.region("trace", || {
                    let (local, z1feedback) = circuit.add_feedback(Z1Trace::new(
                        false,
                        circuit.root_scope(),
                        bounds.clone(),
                    ));
                    let trace = circuit.add_binary_operator_with_preference(
                        <TraceAppend<ValSpine<B, C>, B, C>>::new(circuit.clone()),
                        (&local, OwnershipPreference::STRONGLY_PREFER_OWNED),
                        (
                            &self.try_sharded_version(),
                            OwnershipPreference::PREFER_OWNED,
                        ),
                    );
                    if self.has_sharded_version() {
                        local.mark_sharded();
                        trace.mark_sharded();
                    }
                    z1feedback.connect_with_preference(
                        &trace,
                        OwnershipPreference::STRONGLY_PREFER_OWNED,
                    );

                    circuit
                        .cache_insert(DelayedTraceId::new(trace.origin_node_id().clone()), local);
                    (trace, bounds)
                })
            },
        );

        let (trace, bounds) = trace_bounds.deref_mut();

        bounds.add_key_bound(lower_key_bound);
        bounds.add_val_bound(lower_val_bound);

        trace.clone()
    }

    /// Like `integrate_trace`, but additionally applies a retainment policy to
    /// keys in the trace.
    ///
    /// ## Background
    ///
    /// Relations that store time series data typically have the property that
    /// any new updates can only affect records with recent timestamps.
    /// Depending on how the relation is used in queries this might mean
    /// that, while records with older timestamps still exist in the
    /// relation, they cannot affect any future incremental computation and
    /// therefore don't need to be stored.
    ///
    /// ## Design
    ///
    /// We support two mechanism to specify and eventually discard such unused
    /// records.
    ///
    /// The first mechanism, exposed via the
    /// [`integrate_trace_with_bound`](`Self::integrate_trace_with_bound`)
    /// method, is only applicable when keys and/or values in the collection
    /// are ordered by time.  It allows _each_ consumer of the trace to specify
    /// a lower bound on the keys and values it is interested in.  The
    /// effective bound is the minimum of all bounds specified by individual
    /// consumers.
    ///
    /// The second mechanism, implemented by this method and the
    /// [`integrate_trace_retain_values`](`Self::integrate_trace_retain_values`)
    /// method, is more general and allows the caller to specify an
    /// arbitrary condition on keys and values in the trace respectively.
    /// Keys or values that don't satisfy the condition are eventually
    /// reclaimed by the trace.  This mechanism is applicable to collections
    /// that are not ordered by time.  Hence it doesn't require rearranging
    /// the data in time order.  Furthermore, it is applicable to collections
    /// that contain multiple timestamp column.  Such multidimensional
    /// timestamps only form a partial order.
    ///
    /// Unlike the first mechanism, this mechanism only allows one global
    /// condition to be applied to the stream.  This bound affects _all_
    /// operators that use the trace of the stream, i.e., call
    /// `integrate_trace` (or `trace` in the root scope) on it. This includes
    /// for instance `join`, `aggregate`, and `distinct`.  All such operators
    /// will reference the same instance of a trace.  Therefore bounds
    /// specified by this API must be based on a global analysis of the
    /// entire program.
    ///
    /// The two mechanisms described above interact in different ways for keys
    /// and values. For keys, the lower bound and the retainment condition
    /// are independent and can be active at the same time.  Internally,
    /// they are enforced using different techniques. Lower bounds are
    /// enforced at essentially zero cost.  The retention condition is more
    /// expensive, but more general.
    ///
    /// For values, only one of the two mechanisms can be enabled for any given
    /// stream.  Whenever a retainment condition is specified it supersedes
    /// any lower bounds constraints.
    ///
    /// ## Arguments
    ///
    /// * `bounds_stream` - This stream carries scalar values (i.e., single
    ///   records, not Z-sets).  The key retainment condition is defined
    ///   relative to the last value received from this stream. Typically, this
    ///   value represents the lowest upper bound of all partially ordered
    ///   timestamps in `self` or some other stream, computed with the help of
    ///   the [`waterline`](`Stream::waterline`) operator and adjusted by some
    ///   contstant offsets, dictated, e.g., by window sizes used in the queries
    ///   and the maximal out-of-ordedness of data in the input streams.
    ///
    /// * `retain_key_func` - given the value received from the `bounds_stream`
    ///   at the last clock cycle and a key, returns `true` if the key should be
    ///   retained in the trace and `false` if it should be discarded.
    ///
    /// ## Correctness
    ///
    /// * As discussed above, the retainment policy set using this method
    ///   applies to all consumers of the trace.  An incorrect policy may
    ///   reclaim keys that are still needed by some of the operators, leading
    ///   to incorrect results.  Computing a correct retainment policy can be a
    ///   subtle and error prone task, which is probably best left to automatic
    ///   tools like compilers.
    ///
    /// * The retainment policy set using this method only applies to `self`,
    ///   but not any stream derived from it.  In particular, if `self` is
    ///   re-sharded using the `shard` operator, then it may be necessary to
    ///   call `integrate_trace_retain_keys` on the resulting stream. In
    ///   general, computing a correct retainment policy requires keep track of
    ///   * Streams that are sharded by construction and hence the `shard`
    ///     operator is a no-op for such streams.  For instance, the
    ///     `add_input_set` and `aggregate` operators produce sharded streams.
    ///   * Operators that `shard` their input streams, e.g., `join`.
    ///
    /// * This method should be invoked at most once for a stream.
    ///
    /// * `retain_key_func` must be monotone in its first argument: for any
    ///   timestamp `ts1` and key `k` such that `retain_key_func(ts1, k) =
    ///   false`, and for any `ts2 >= ts1` it must hold that
    ///   `retain_key_func(ts2, k) = false`, i.e., once a key is rejected, it
    ///   will remain rejected as the bound increases.
    #[track_caller]
    pub fn integrate_trace_retain_keys<TS, RK>(
        &self,
        bounds_stream: &Stream<C, TS>,
        retain_key_func: RK,
    ) -> Stream<C, Spine<B>>
    where
        B: Batch<Time = ()> + Send,
        TS: DBData,
        RK: Fn(&B::Key, &TS) -> bool + Clone + 'static,
    {
        // The following `shard` is important.  It makes sure that the
        // bound is applied to the sharded version of the stream, which is what
        // all operators that come with a retainment policy use today.
        // If this changes in the future, we may need two versions of the operator,
        // with and without sharding.
        let (trace, bounds) = self.shard().integrate_trace_inner();

        bounds_stream.inspect(move |ts| {
            let ts = ts.clone();
            let retain_key_func = retain_key_func.clone();
            bounds.set_key_filter(Box::new(move |key| retain_key_func(key, &ts)));
        });

        trace
    }

    /// Similar to
    /// [`integrate_trace_retain_keys`](`Self::integrate_trace_retain_keys`),
    /// but applies a retainment policy to values in the trace.
    #[track_caller]
    pub fn integrate_trace_retain_values<TS, RV>(
        &self,
        bounds_stream: &Stream<C, TS>,
        retain_value: RV,
    ) -> Stream<C, Spine<B>>
    where
        B: Batch<Time = ()> + Send,
        TS: DBData,
        RV: Fn(&B::Val, &TS) -> bool + Clone + 'static,
    {
        // The following `shard` is important.  It makes sure that the
        // bound is applied to the sharded version of the stream, which is what
        // all operators that come with a retainment policy use today.
        // If this changes in the future, we may need two versions of the operator,
        // with and without sharding.
        let (trace, bounds) = self.shard().integrate_trace_inner();

        bounds_stream.inspect(move |ts| {
            let ts = ts.clone();
            let retain_value = retain_value.clone();
            bounds.set_val_filter(Box::new(move |val| retain_value(val, &ts)));
        });

        trace
    }

    // TODO: this method should replace `Stream::integrate()`.
    #[track_caller]
    pub fn integrate_trace(&self) -> Stream<C, Spine<B>>
    where
        B: Batch<Time = ()>,
        Spine<B>: SizeOf,
    {
        self.integrate_trace_with_bound(TraceBound::new(), TraceBound::new())
    }

    pub fn integrate_trace_with_bound(
        &self,
        lower_key_bound: TraceBound<B::Key>,
        lower_val_bound: TraceBound<B::Val>,
    ) -> Stream<C, Spine<B>>
    where
        B: Batch<Time = ()>,
        Spine<B>: SizeOf,
    {
        let (trace, bounds) = self.integrate_trace_inner();

        bounds.add_key_bound(lower_key_bound);
        bounds.add_val_bound(lower_val_bound);

        trace
    }

    #[allow(clippy::type_complexity)]
    fn integrate_trace_inner(&self) -> (Stream<C, Spine<B>>, TraceBounds<B::Key, B::Val>)
    where
        B: Batch<Time = ()>,
        Spine<B>: SizeOf,
    {
        let mut trace_bounds = self.circuit().cache_get_or_insert_with(
            TraceId::new(self.origin_node_id().clone()),
            || {
                let circuit = self.circuit();
                let bounds = TraceBounds::new();

                circuit.region("integrate_trace", || {
                    let (ExportStream { local, export }, z1feedback) = circuit
                        .add_feedback_with_export(Z1Trace::new(
                            true,
                            circuit.root_scope(),
                            bounds.clone(),
                        ));

                    let trace = circuit.add_binary_operator_with_preference(
                        UntimedTraceAppend::<Spine<B>>::new(),
                        (&local, OwnershipPreference::STRONGLY_PREFER_OWNED),
                        (
                            &self.try_sharded_version(),
                            OwnershipPreference::PREFER_OWNED,
                        ),
                    );

                    if self.has_sharded_version() {
                        local.mark_sharded();
                        trace.mark_sharded();
                    }

                    z1feedback.connect_with_preference(
                        &trace,
                        OwnershipPreference::STRONGLY_PREFER_OWNED,
                    );

                    circuit
                        .cache_insert(DelayedTraceId::new(trace.origin_node_id().clone()), local);
                    circuit.cache_insert(ExportId::new(trace.origin_node_id().clone()), export);

                    (trace, bounds)
                })
            },
        );

        let (trace, bounds) = trace_bounds.deref_mut();

        (trace.clone(), bounds.clone())
    }
}

/// See [`trait TraceFeedback`] documentation.
pub struct TraceFeedbackConnector<C, T>
where
    C: Circuit,
    T: Trace,
{
    feedback: FeedbackConnector<C, T, T, Z1Trace<T>>,
    /// `delayed_trace` stream in the diagram in
    /// [`trait TraceFeedback`] documentation.
    pub delayed_trace: Stream<C, T>,
    export_trace: Stream<C::Parent, T>,
    bounds: TraceBounds<T::Key, T::Val>,
}

impl<C, T> TraceFeedbackConnector<C, T>
where
    T: Trace + Clone,
    C: Circuit,
{
    pub fn connect(self, stream: &Stream<C, T::Batch>) {
        let circuit = self.delayed_trace.circuit();

        let trace = circuit.add_binary_operator_with_preference(
            <UntimedTraceAppend<T>>::new(),
            (
                &self.delayed_trace,
                OwnershipPreference::STRONGLY_PREFER_OWNED,
            ),
            (
                &stream.try_sharded_version(),
                OwnershipPreference::PREFER_OWNED,
            ),
        );

        if stream.has_sharded_version() {
            self.delayed_trace.mark_sharded();
            trace.mark_sharded();
        }

        self.feedback
            .connect_with_preference(&trace, OwnershipPreference::STRONGLY_PREFER_OWNED);

        circuit.cache_insert(
            DelayedTraceId::new(trace.origin_node_id().clone()),
            self.delayed_trace.clone(),
        );
        circuit.cache_insert(
            TraceId::new(stream.origin_node_id().clone()),
            (trace.clone(), self.bounds.clone()),
        );
        circuit.cache_insert(
            ExportId::new(trace.origin_node_id().clone()),
            self.export_trace,
        );
    }
}

/// Extension trait to trait [`Circuit`] that provides a convenience API
/// to construct circuits of the following shape:
///
/// ```text
///  external inputs    ┌───┐    output    ┌──────────────────┐
/// ───────────────────►│ F ├─────────────►│UntimedTraceAppend├───┐
///                     └───┘              └──────────────────┘   │
///                       ▲                      ▲                │trace
///                       │                      │                │
///                       │    delayed_trace   ┌─┴──┐             │
///                       └────────────────────┤Z^-1│◄────────────┘
///                                            └────┘
/// ```
/// where `F` is an operator that consumes an integral of its own output
/// stream.
///
/// Use this method to create a
/// [`TraceFeedbackConnector`] struct.  The struct contains the `delayed_trace`
/// stream, which can be used as input to instantiate `F` and the `output`
/// stream.  Close the loop by calling
/// `TraceFeedbackConnector::connect(output)`.
pub trait TraceFeedback: Circuit {
    fn add_integrate_trace_feedback<T>(
        &self,
        bounds: TraceBounds<T::Key, T::Val>,
    ) -> TraceFeedbackConnector<Self, T>
    where
        T: Trace<Time = ()> + Clone,
    {
        let (ExportStream { local, export }, feedback) =
            self.add_feedback_with_export(Z1Trace::new(true, self.root_scope(), bounds.clone()));

        TraceFeedbackConnector {
            feedback,
            delayed_trace: local,
            export_trace: export,
            bounds,
        }
    }
}

impl<C: Circuit> TraceFeedback for C {}

impl<C, T> Stream<C, T>
where
    C: Circuit,
    T: Trace + 'static,
{
    pub fn delay_trace(&self) -> Stream<C, T> {
        // The delayed trace should be automatically created while the real trace is
        // created via `.trace()` or a similar function
        // FIXME: Create a trace if it doesn't exist
        self.circuit()
            .cache_get_or_insert_with(DelayedTraceId::new(self.origin_node_id().clone()), || {
                panic!("called `.delay_trace()` on a stream without a previously created trace")
            })
            .clone()
    }
}

pub struct UntimedTraceAppend<T>
where
    T: Trace,
{
    _phantom: PhantomData<T>,
}

impl<T> UntimedTraceAppend<T>
where
    T: Trace,
{
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<T> Default for UntimedTraceAppend<T>
where
    T: Trace,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Operator for UntimedTraceAppend<T>
where
    T: Trace + 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("UntimedTraceAppend")
    }
    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<T> BinaryOperator<T, T::Batch, T> for UntimedTraceAppend<T>
where
    T: Trace + 'static,
{
    fn eval(&mut self, _trace: &T, _batch: &T::Batch) -> T {
        // Refuse to accept trace by reference.  This should not happen in a correctly
        // constructed circuit.
        panic!("UntimedTraceAppend::eval(): cannot accept trace by reference")
    }

    fn eval_owned_and_ref(&mut self, mut trace: T, batch: &T::Batch) -> T {
        trace.insert(batch.clone());
        trace
    }

    fn eval_ref_and_owned(&mut self, _trace: &T, _batch: T::Batch) -> T {
        // Refuse to accept trace by reference.  This should not happen in a correctly
        // constructed circuit.
        panic!("UntimedTraceAppend::eval_ref_and_owned(): cannot accept trace by reference")
    }

    fn eval_owned(&mut self, mut trace: T, batch: T::Batch) -> T {
        trace.insert(batch);
        trace
    }

    fn input_preference(&self) -> (OwnershipPreference, OwnershipPreference) {
        (
            OwnershipPreference::PREFER_OWNED,
            OwnershipPreference::PREFER_OWNED,
        )
    }
}

pub struct TraceAppend<T, B, C> {
    clock: C,
    _phantom: PhantomData<(T, B)>,
}

impl<T, B, C> TraceAppend<T, B, C> {
    pub fn new(clock: C) -> Self {
        Self {
            clock,
            _phantom: PhantomData,
        }
    }
}

impl<T, B, Clk> Operator for TraceAppend<T, B, Clk>
where
    T: 'static,
    B: 'static,
    Clk: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("TraceAppend")
    }
    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<T, B, Clk> BinaryOperator<T, B, T> for TraceAppend<T, B, Clk>
where
    B: BatchReader<Time = ()>,
    Clk: WithClock + 'static,
    T: Trace<Key = B::Key, Val = B::Val, R = B::R, Time = Clk::Time>,
{
    fn eval(&mut self, _trace: &T, _batch: &B) -> T {
        // Refuse to accept trace by reference.  This should not happen in a correctly
        // constructed circuit.
        unimplemented!()
    }

    fn eval_owned_and_ref(&mut self, mut trace: T, batch: &B) -> T {
        // TODO: extend `trace` type to feed untimed batches directly
        // (adding fixed timestamp on the fly).
        trace.insert(batch_add_time(batch, &self.clock.time()));
        trace
    }

    fn eval_ref_and_owned(&mut self, _trace: &T, _batch: B) -> T {
        // Refuse to accept trace by reference.  This should not happen in a correctly
        // constructed circuit.
        unimplemented!()
    }

    fn eval_owned(&mut self, mut trace: T, batch: B) -> T {
        trace.insert(batch_add_time(&batch, &self.clock.time()));
        trace
    }

    fn input_preference(&self) -> (OwnershipPreference, OwnershipPreference) {
        (
            OwnershipPreference::PREFER_OWNED,
            OwnershipPreference::PREFER_OWNED,
        )
    }
}

pub struct Z1Trace<T: Trace> {
    time: T::Time,
    trace: Option<T>,
    // `dirty[scope]` is `true` iff at least one non-empty update was added to the trace
    // since the previous clock cycle at level `scope`.
    dirty: Vec<bool>,
    root_scope: Scope,
    reset_on_clock_start: bool,
    bounds: TraceBounds<T::Key, T::Val>,
    effective_key_bound: Option<T::Key>,
}

impl<T> Z1Trace<T>
where
    T: Trace,
{
    pub fn new(
        reset_on_clock_start: bool,
        root_scope: Scope,
        bounds: TraceBounds<T::Key, T::Val>,
    ) -> Self {
        Self {
            time: T::Time::clock_start(),
            trace: None,
            dirty: vec![false; root_scope as usize + 1],
            root_scope,
            reset_on_clock_start,
            bounds,
            effective_key_bound: None,
        }
    }
}

impl<T> Operator for Z1Trace<T>
where
    T: Trace,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("Z1 (trace)")
    }

    fn clock_start(&mut self, scope: Scope) {
        self.dirty[scope as usize] = false;

        if scope == 0 && self.trace.is_none() {
            // TODO: use T::with_effort with configurable effort?
            self.trace = Some(T::new(None));
        }
    }

    fn clock_end(&mut self, scope: Scope) {
        if scope + 1 == self.root_scope && !self.reset_on_clock_start {
            if let Some(tr) = self.trace.as_mut() {
                tr.recede_to(&self.time.epoch_end(self.root_scope).recede(self.root_scope));
            }
        }
        self.time.advance(scope + 1);
    }

    fn metadata(&self, meta: &mut OperatorMeta) {
        let total_size = self
            .trace
            .as_ref()
            .map(|trace| trace.num_entries_deep())
            .unwrap_or(0);

        let bytes = self
            .trace
            .as_ref()
            .map(|trace| trace.size_of())
            .unwrap_or_default();

        meta.extend(metadata! {
            NUM_ENTRIES_LABEL => total_size,
            ALLOCATED_BYTES_LABEL => MetaItem::bytes(bytes.total_bytes()),
            USED_BYTES_LABEL => MetaItem::bytes(bytes.used_bytes()),
            "allocations" => bytes.distinct_allocations(),
            SHARED_BYTES_LABEL => MetaItem::bytes(bytes.shared_bytes()),
        });
    }

    fn fixedpoint(&self, scope: Scope) -> bool {
        !self.dirty[scope as usize]
    }
}

impl<T> StrictOperator<T> for Z1Trace<T>
where
    T: Trace,
{
    fn get_output(&mut self) -> T {
        let mut result = self.trace.take().unwrap();
        result.clear_dirty_flag();
        result
    }

    fn get_final_output(&mut self) -> T {
        // We only create the operator using `add_feedback_with_export` if
        // `reset_on_clock_start` is true, so this should never get invoked
        // otherwise.
        assert!(self.reset_on_clock_start);
        self.get_output()
    }
}

impl<T> StrictUnaryOperator<T, T> for Z1Trace<T>
where
    T: Trace,
{
    fn eval_strict(&mut self, _i: &T) {
        unimplemented!()
    }

    fn eval_strict_owned(&mut self, mut i: T) {
        self.time = self.time.advance(0);

        let dirty = i.dirty();

        let effective_key_bound = self.bounds.effective_key_bound();
        if effective_key_bound != self.effective_key_bound {
            if let Some(bound) = &effective_key_bound {
                i.truncate_keys_below(bound);
            }
        }
        self.effective_key_bound = effective_key_bound;
        if let Some(filter) = self.bounds.key_filter() {
            i.retain_keys(filter);
        }

        if let Some(filter) = self.bounds.effective_val_filter() {
            i.retain_values(filter);
        }

        self.trace = Some(i);

        self.dirty[0] = dirty;
        if dirty {
            self.dirty.fill(true);
        }
    }

    fn input_preference(&self) -> OwnershipPreference {
        OwnershipPreference::PREFER_OWNED
    }
}

#[cfg(test)]
mod test {
    use std::cmp::max;

    use crate::utils::Tup2;
    use crate::{operator::FilterMap, Runtime, Stream};
    use proptest::{collection::vec, prelude::*};
    use size_of::SizeOf;

    fn quasi_monotone_batches(
        key_window_size: i32,
        key_window_step: i32,
        val_window_size: i32,
        val_window_step: i32,
        max_tuples: usize,
        batches: usize,
    ) -> impl Strategy<Value = Vec<Vec<((i32, i32), i32)>>> {
        (0..batches)
            .map(|i| {
                vec(
                    (
                        (
                            i as i32 * key_window_step
                                ..i as i32 * key_window_step + key_window_size,
                            i as i32 * val_window_step
                                ..i as i32 * val_window_step + val_window_size,
                        ),
                        1..2,
                    ),
                    0..max_tuples,
                )
            })
            .collect::<Vec<_>>()
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        #[test]
        fn test_integrate_trace_retain(batches in quasi_monotone_batches(100, 20, 1000, 200, 100, 100)) {
            let (mut dbsp, input_handle) = Runtime::init_circuit(4, move |circuit| {
                let (stream, handle) = circuit.add_input_indexed_zset::<i32, i32, i32>();
                let stream = stream.shard();
                let watermark: Stream<_, (i32, i32)> = stream
                    .waterline(
                        || (i32::MIN, i32::MIN),
                        |k, v| (*k, *v),
                        |(ts1_left, ts2_left), (ts1_right, ts2_right)| {
                            (max(*ts1_left, *ts1_right), max(*ts2_left, *ts2_right))
                        },
                    );

                let _trace = stream.integrate_trace();
                let retain_keys = stream.integrate_trace_retain_keys(&watermark, |key, ts| *key >= ts.0 - 100);
                retain_keys.apply(|trace| {
                    //println!("retain_keys: {}bytes", trace.size_of().total_bytes());
                    assert!(trace.size_of().total_bytes() < 15000);
                });

                let stream2 = stream.map_index(|(k, v)| (*k, *v)).shard();

                let _trace2 = stream2.integrate_trace();
                let retain_vals = stream2.integrate_trace_retain_values(&watermark, |val, ts| *val >= ts.1 - 1000);

                retain_vals.apply(|trace| {
                    //println!("retain_vals: {}bytes", trace.size_of().total_bytes());
                    assert!(trace.size_of().total_bytes() < 15000);
                });

                Ok(handle)
            })
            .unwrap();

            for batch in batches {
                let mut tuples = batch.into_iter().map(|((k, v), r)| (k, Tup2(v, r))).collect::<Vec<_>>();
                input_handle.append(&mut tuples);
                dbsp.step().unwrap();
            }
        }
    }
}
