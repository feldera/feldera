use crate::circuit::circuit_builder::{register_replay_stream, StreamId};
use crate::circuit::metadata::NUM_INPUTS;
use crate::dynamic::{Weight, WeightTrait};
use crate::operator::require_persistent_id;
use crate::trace::spine_async::WithSnapshot;
use crate::trace::{BatchReaderFactories, Builder, MergeCursor};
use crate::Runtime;
use crate::{
    circuit::{
        metadata::{
            MetaItem, OperatorMeta, ALLOCATED_BYTES_LABEL, NUM_ENTRIES_LABEL, SHARED_BYTES_LABEL,
            USED_BYTES_LABEL,
        },
        operator_traits::{BinaryOperator, Operator, StrictOperator, StrictUnaryOperator},
        Circuit, ExportId, ExportStream, FeedbackConnector, GlobalNodeId, OwnershipPreference,
        Scope, Stream, WithClock,
    },
    circuit_cache_key,
    dynamic::DataTrait,
    trace::{Batch, BatchReader, Filter, Spine, SpineSnapshot, Trace},
    Error, Timestamp,
};
use dyn_clone::clone_box;
use feldera_storage::StoragePath;
use minitrace::trace;
use ouroboros::self_referencing;
use size_of::SizeOf;
use std::any::TypeId;
use std::mem::transmute;
use std::{
    borrow::Cow,
    cell::{Ref, RefCell},
    cmp::Ordering,
    fmt::Debug,
    marker::PhantomData,
    ops::Deref,
    rc::Rc,
    sync::Arc,
};

circuit_cache_key!(TraceId<C, D: BatchReader>(StreamId => Stream<C, D>));
circuit_cache_key!(BoundsId<D: BatchReader>(StreamId => TraceBounds<<D as BatchReader>::Key, <D as BatchReader>::Val>));
circuit_cache_key!(DelayedTraceId<C, D>(StreamId => Stream<C, D>));

/// Lower bound on keys or values in a trace.
///
/// Setting the bound to `None` is equivalent to setting it to
/// `T::min_value()`, i.e., the contents of the trace will never
/// get truncated.
///
/// The writer can update the value of the bound at each clock
/// cycle.  The bound can only increase monotonically.
#[repr(transparent)]
pub struct TraceBound<T: ?Sized>(Rc<RefCell<Option<Box<T>>>>);

impl<T: DataTrait + ?Sized> Clone for TraceBound<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T: Debug + ?Sized> Debug for TraceBound<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.borrow().fmt(f)
    }
}

impl<T: DataTrait + ?Sized> PartialEq for TraceBound<T> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<T: DataTrait + ?Sized> Eq for TraceBound<T> {}

impl<T: DataTrait + ?Sized> PartialOrd for TraceBound<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: DataTrait + ?Sized> Ord for TraceBound<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

impl<K: DataTrait + ?Sized> Default for TraceBound<K> {
    fn default() -> Self {
        Self(Rc::new(RefCell::new(None)))
    }
}

impl<K: DataTrait + ?Sized> TraceBound<K> {
    pub fn new() -> Self {
        Default::default()
    }

    /// Set the new value of the bound.
    pub fn set(&self, bound: Box<K>) {
        //debug_assert!(self.0.borrow().as_ref() <= Some(&bound));
        *self.0.borrow_mut() = Some(bound);
    }

    /// Get the current value of the bound.
    pub fn get(&self) -> Ref<'_, Option<Box<K>>> {
        (*self.0).borrow()
    }
}

/// Data structure that tracks key and value retainment policies for a
/// trace.
pub struct TraceBounds<K: ?Sized + 'static, V: ?Sized + 'static>(
    Rc<RefCell<TraceBoundsInner<K, V>>>,
);

impl<K: DataTrait + ?Sized, V: DataTrait + ?Sized> Clone for TraceBounds<K, V> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<K: DataTrait + ?Sized, V: DataTrait + ?Sized> TraceBounds<K, V> {
    /// Instantiate `TraceBounds` with empty sets of key and value bounds.
    ///
    /// The caller must add at least one key and one value bound before
    /// running the circuit.
    pub(crate) fn new() -> Self {
        Self(Rc::new(RefCell::new(TraceBoundsInner {
            key_predicate: Predicate::Bounds(Vec::new()),
            unique_key_name: None,
            val_predicate: Predicate::Bounds(Vec::new()),
            unique_val_name: None,
        })))
    }

    /// Returns `TraceBounds` that prevent any values in the trace from
    /// being truncated.
    pub(crate) fn unbounded() -> Self {
        Self(Rc::new(RefCell::new(TraceBoundsInner {
            key_predicate: Predicate::Bounds(vec![TraceBound::new()]),
            unique_key_name: None,
            val_predicate: Predicate::Bounds(vec![TraceBound::new()]),
            unique_val_name: None,
        })))
    }

    pub(crate) fn add_key_bound(&self, bound: TraceBound<K>) {
        match &mut self.0.borrow_mut().key_predicate {
            Predicate::Bounds(bounds) => bounds.push(bound),
            Predicate::Filter(_) => {}
        };
    }

    /// Set key retainment condition.  Disables any key bounds
    /// set using [`Self::add_key_bound`].
    pub(crate) fn set_key_filter(&self, filter: Filter<K>) {
        self.0.borrow_mut().key_predicate = Predicate::Filter(filter);
    }

    pub(crate) fn set_unique_key_bound_name(&self, unique_name: Option<&str>) {
        self.0.borrow_mut().unique_key_name = unique_name.map(str::to_string);
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

    pub(crate) fn set_unique_val_bound_name(&self, unique_name: Option<&str>) {
        self.0.borrow_mut().unique_val_name = unique_name.map(str::to_string);
    }

    /// Returns effective key retention condition, computed as the
    /// minimum bound installed using [`Self::add_val_bound`] or as the
    /// condition installed using [`Self::set_val_filter`] (the latter
    /// takes precedence).
    pub(crate) fn effective_key_filter(&self) -> Option<Filter<K>> {
        match &(*self.0).borrow().key_predicate {
            Predicate::Bounds(bounds) => bounds
                .iter()
                .min()
                .expect("At least one trace bound must be set")
                .get()
                .deref()
                .as_ref()
                .map(|bx| Arc::from(clone_box(bx.as_ref())))
                .map(|bound: Arc<K>| {
                    let metadata = MetaItem::String(format!("{bound:?}"));
                    Filter::new(Box::new(move |k: &K| {
                        bound.as_ref().cmp(k) != Ordering::Greater
                    }))
                    .with_metadata(metadata)
                }),
            Predicate::Filter(filter) => Some(filter.clone()),
        }
    }

    /// Returns effective value retention condition, computed as the
    /// minimum bound installed using [`Self::add_val_bound`] or as the
    /// condition installed using [`Self::set_val_filter`] (the latter
    /// takes precedence).
    pub(crate) fn effective_val_filter(&self) -> Option<Filter<V>> {
        match &(*self.0).borrow().val_predicate {
            Predicate::Bounds(bounds) => bounds
                .iter()
                .min()
                .expect("At least one trace bound must be set")
                .get()
                .deref()
                .as_ref()
                .map(|bx| Arc::from(clone_box(bx.as_ref())))
                .map(|bound: Arc<V>| {
                    let metadata = MetaItem::String(format!("{bound:?}"));
                    Filter::new(Box::new(move |v: &V| {
                        bound.as_ref().cmp(v) != Ordering::Greater
                    }))
                    .with_metadata(metadata)
                }),
            Predicate::Filter(filter) => Some(filter.clone()),
        }
    }

    pub(crate) fn metadata(&self) -> MetaItem {
        self.0.borrow().metadata()
    }
}

/// Value retainment predicate defined as either a set of bounds
/// or a filter condition.
///
/// See [`Stream::dyn_integrate_trace_retain_keys`] for details.
enum Predicate<V: ?Sized> {
    Bounds(Vec<TraceBound<V>>),
    Filter(Filter<V>),
}

impl<V: Debug + ?Sized> Predicate<V> {
    pub fn metadata(&self) -> MetaItem {
        match self {
            Self::Bounds(bounds) => MetaItem::Array(
                bounds
                    .iter()
                    .map(|b| MetaItem::String(format!("{b:?}")))
                    .collect(),
            ),
            Self::Filter(filter) => filter.metadata().clone(),
        }
    }
}

struct TraceBoundsInner<K: ?Sized + 'static, V: ?Sized + 'static> {
    /// Key bounds _or_ retainment condition.
    key_predicate: Predicate<K>,
    unique_key_name: Option<String>,
    /// Value bounds _or_ retainment condition.
    val_predicate: Predicate<V>,
    unique_val_name: Option<String>,
}

impl<K: Debug + ?Sized + 'static, V: Debug + ?Sized + 'static> TraceBoundsInner<K, V> {
    pub fn metadata(&self) -> MetaItem {
        MetaItem::Map(
            metadata! {
                "key" => self.key_predicate.metadata(),
                "value" => self.val_predicate.metadata()
            }
            .into(),
        )
    }
}

/// An on-storage, key-only [`Spine`] of `C`'s default batch type, with key and
/// weight types taken from `B`.
pub type KeySpine<B, C> = Spine<
    <<C as WithClock>::Time as Timestamp>::KeyBatch<<B as BatchReader>::Key, <B as BatchReader>::R>,
>;

/// An on-storage [`Spine`] of `C`'s default batch type, with key, value, and
/// weight types taken from `B`.
pub type ValSpine<B, C> = Spine<
    <<C as WithClock>::Time as Timestamp>::ValBatch<
        <B as BatchReader>::Key,
        <B as BatchReader>::Val,
        <B as BatchReader>::R,
    >,
>;

impl<C, B> Stream<C, B>
where
    C: Circuit,
    B: Clone + Send + Sync + 'static,
{
    /// See [`Stream::trace`].
    pub fn dyn_trace(
        &self,
        output_factories: &<ValSpine<B, C> as BatchReader>::Factories,
        batch_factories: &B::Factories,
    ) -> Stream<C, ValSpine<B, C>>
    where
        B: Batch<Time = ()>,
    {
        self.dyn_trace_with_bound(
            output_factories,
            batch_factories,
            TraceBound::new(),
            TraceBound::new(),
        )
    }

    /// See [`Stream::trace_with_bound`].
    pub fn dyn_trace_with_bound(
        &self,
        output_factories: &<ValSpine<B, C> as BatchReader>::Factories,
        batch_factories: &B::Factories,
        lower_key_bound: TraceBound<B::Key>,
        lower_val_bound: TraceBound<B::Val>,
    ) -> Stream<C, ValSpine<B, C>>
    where
        B: Batch<Time = ()>,
    {
        let bounds = self.trace_bounds_with_bound(lower_key_bound, lower_val_bound);

        self.circuit()
            .cache_get_or_insert_with(TraceId::new(self.stream_id()), || {
                let circuit = self.circuit();

                circuit.region("trace", || {
                    let persistent_id = self.get_persistent_id();
                    let z1 = Z1Trace::new(
                        output_factories,
                        batch_factories,
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

                    let replay_stream = z1feedback.operator_mut().prepare_replay_stream(self);

                    let trace = circuit.add_binary_operator_with_preference(
                        <TraceAppend<ValSpine<B, C>, B, C>>::new(output_factories, circuit.clone()),
                        (&delayed_trace, OwnershipPreference::STRONGLY_PREFER_OWNED),
                        (self, OwnershipPreference::PREFER_OWNED),
                    );
                    if self.is_sharded() {
                        delayed_trace.mark_sharded();
                        trace.mark_sharded();
                    }

                    z1feedback.connect_with_preference(
                        &trace,
                        OwnershipPreference::STRONGLY_PREFER_OWNED,
                    );

                    register_replay_stream(circuit, self, &replay_stream);

                    circuit.cache_insert(DelayedTraceId::new(trace.stream_id()), delayed_trace);
                    trace
                })
            })
            .clone()
    }

    /// See [`Stream::integrate_trace_retain_keys`].
    #[track_caller]
    pub fn dyn_integrate_trace_retain_keys<TS>(
        &self,
        bounds_stream: &Stream<C, Box<TS>>,
        retain_key_func: Box<dyn Fn(&TS) -> Filter<B::Key>>,
    ) where
        B: Batch<Time = ()>,
        TS: DataTrait + ?Sized,
        Box<TS>: Clone,
    {
        let bounds = self.trace_bounds();
        bounds.set_unique_key_bound_name(bounds_stream.get_persistent_id().as_deref());
        bounds_stream.inspect(move |ts| {
            let filter = retain_key_func(ts.as_ref());
            bounds.set_key_filter(filter);
        });
    }

    /// See [`Stream::integrate_trace_retain_values`].
    #[track_caller]
    pub fn dyn_integrate_trace_retain_values<TS>(
        &self,
        bounds_stream: &Stream<C, Box<TS>>,
        retain_val_func: Box<dyn Fn(&TS) -> Filter<B::Val>>,
    ) where
        B: Batch<Time = ()>,
        TS: DataTrait + ?Sized,
        Box<TS>: Clone,
    {
        let bounds = self.trace_bounds();
        bounds.set_unique_val_bound_name(bounds_stream.get_persistent_id().as_deref());

        bounds_stream.inspect(move |ts| {
            let filter = retain_val_func(ts.as_ref());
            bounds.set_val_filter(filter);
        });
    }

    /// Retrieves trace bounds for `self`, creating them if necessary.
    ///
    /// It's important that a single `TraceBounds` includes all of the bounds
    /// relevant to a particular trace.  This can be tricky in the presence of
    /// multiple versions of a stream that code tends to treat as the same.  We
    /// manage it by mapping all of those versions to just one single version:
    ///
    /// * For a sharded version of some source stream, we use the source stream.
    ///
    /// * For a spilled version of some source stream, we use the source stream.
    ///
    /// Using the source stream is a safer choice than using the sharded (or
    /// spilled) version, because it always exists, whereas the sharded version
    /// might be created only *after* we get the trace bounds for the source
    /// stream.
    fn trace_bounds(&self) -> TraceBounds<B::Key, B::Val>
    where
        B: BatchReader,
    {
        // We handle moving from the sharded to unsharded stream directly here.
        // Moving from spilled to unspilled is handled by `spill()`.
        let stream_id = self.try_unsharded_version().stream_id();

        self.circuit()
            .cache_get_or_insert_with(BoundsId::<B>::new(stream_id), TraceBounds::new)
            .clone()
    }

    /// Retrieves trace bounds for `self`, or a sharded version of `self` if it
    /// exists, creating them if necessary, and adds bounds for
    /// `lower_key_bound` and `lower_val_bound`.
    fn trace_bounds_with_bound(
        &self,
        lower_key_bound: TraceBound<B::Key>,
        lower_val_bound: TraceBound<B::Val>,
    ) -> TraceBounds<B::Key, B::Val>
    where
        B: BatchReader,
    {
        let bounds = self.trace_bounds();
        bounds.add_key_bound(lower_key_bound);
        bounds.add_val_bound(lower_val_bound);
        bounds
    }

    // TODO: this method should replace `Stream::integrate()`.
    #[track_caller]
    pub fn dyn_integrate_trace(&self, factories: &B::Factories) -> Stream<C, Spine<B>>
    where
        B: Batch<Time = ()>,
        Spine<B>: SizeOf,
    {
        self.dyn_integrate_trace_with_bound(factories, TraceBound::new(), TraceBound::new())
    }

    pub fn dyn_integrate_trace_with_bound(
        &self,
        factories: &B::Factories,
        lower_key_bound: TraceBound<B::Key>,
        lower_val_bound: TraceBound<B::Val>,
    ) -> Stream<C, Spine<B>>
    where
        B: Batch<Time = ()>,
        Spine<B>: SizeOf,
    {
        self.integrate_trace_inner(
            factories,
            self.trace_bounds_with_bound(lower_key_bound, lower_val_bound),
        )
    }

    #[allow(clippy::type_complexity)]
    fn integrate_trace_inner(
        &self,
        input_factories: &B::Factories,
        bounds: TraceBounds<B::Key, B::Val>,
    ) -> Stream<C, Spine<B>>
    where
        B: Batch<Time = ()>,
        Spine<B>: SizeOf,
    {
        self.circuit()
            .cache_get_or_insert_with(TraceId::new(self.stream_id()), || {
                let circuit = self.circuit();
                let bounds = bounds.clone();

                let persistent_id = self.get_persistent_id();

                circuit.region("integrate_trace", || {
                    let z1 = Z1Trace::new(
                        input_factories,
                        input_factories,
                        true,
                        circuit.root_scope(),
                        bounds,
                    );

                    let (
                        ExportStream {
                            local: delayed_trace,
                            export,
                        },
                        z1feedback,
                    ) = circuit.add_feedback_with_export_persistent(
                        persistent_id
                            .map(|name| format!("{name}.integral"))
                            .as_deref(),
                        z1,
                    );

                    let replay_stream = z1feedback.operator_mut().prepare_replay_stream(self);

                    let trace = circuit.add_binary_operator_with_preference(
                        UntimedTraceAppend::<Spine<B>>::new(),
                        (&delayed_trace, OwnershipPreference::STRONGLY_PREFER_OWNED),
                        (self, OwnershipPreference::PREFER_OWNED),
                    );

                    if self.is_sharded() {
                        delayed_trace.mark_sharded();
                        trace.mark_sharded();
                    }

                    z1feedback.connect_with_preference(
                        &trace,
                        OwnershipPreference::STRONGLY_PREFER_OWNED,
                    );

                    register_replay_stream(circuit, self, &replay_stream);

                    circuit.cache_insert(DelayedTraceId::new(trace.stream_id()), delayed_trace);
                    circuit.cache_insert(ExportId::new(trace.stream_id()), export);

                    trace
                })
            })
            .clone()
    }
}

/// See [`trait TraceFeedback`] documentation.
pub struct TraceFeedbackConnector<C, T>
where
    C: Circuit,
    T: Trace,
{
    feedback: FeedbackConnector<C, T, T, Z1Trace<C, T::Batch, T>>,
    /// `delayed_trace` stream in the diagram in
    /// [`trait TraceFeedback`] documentation.
    pub delayed_trace: Stream<C, T>,
    export_trace: Stream<C::Parent, T>,
    bounds: TraceBounds<T::Key, T::Val>,
}

impl<C, T> TraceFeedbackConnector<C, T>
where
    T: Trace<Time = ()> + Clone,
    C: Circuit,
{
    pub fn connect(self, stream: &Stream<C, T::Batch>) {
        let circuit = self.delayed_trace.circuit();

        let replay_stream = self.feedback.operator_mut().prepare_replay_stream(stream);

        let trace = circuit.add_binary_operator_with_preference(
            <UntimedTraceAppend<T>>::new(),
            (
                &self.delayed_trace,
                OwnershipPreference::STRONGLY_PREFER_OWNED,
            ),
            (stream, OwnershipPreference::PREFER_OWNED),
        );

        if stream.is_sharded() {
            self.delayed_trace.mark_sharded();
            trace.mark_sharded();
        }

        self.feedback
            .connect_with_preference(&trace, OwnershipPreference::STRONGLY_PREFER_OWNED);

        register_replay_stream(circuit, stream, &replay_stream);

        circuit.cache_insert(
            DelayedTraceId::new(trace.stream_id()),
            self.delayed_trace.clone(),
        );

        circuit.cache_insert(TraceId::new(stream.stream_id()), trace.clone());
        circuit.cache_insert(
            BoundsId::<T::Batch>::new(stream.stream_id()),
            self.bounds.clone(),
        );
        circuit.cache_insert(ExportId::new(trace.stream_id()), self.export_trace);
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
        persistent_id: Option<&str>,
        factories: &T::Factories,
        bounds: TraceBounds<T::Key, T::Val>,
    ) -> TraceFeedbackConnector<Self, T>
    where
        T: Trace<Time = ()> + Clone,
    {
        // We'll give `Z1Trace` a real name inside `TraceFeedbackConnector::connect`, where we have the name of the input stream.
        let (ExportStream { local, export }, feedback) = self.add_feedback_with_export_persistent(
            persistent_id
                .map(|name| format!("{name}.integral"))
                .as_deref(),
            Z1Trace::new(
                factories,
                factories,
                true,
                self.root_scope(),
                bounds.clone(),
            ),
        );

        TraceFeedbackConnector {
            feedback,
            delayed_trace: local,
            export_trace: export,
            bounds,
        }
    }
}

impl<C: Circuit> TraceFeedback for C {}

impl<C, B> Stream<C, Spine<B>>
where
    C: Circuit,
    B: Batch,
{
    pub fn delay_trace(&self) -> Stream<C, SpineSnapshot<B>> {
        // The delayed trace should be automatically created while the real trace is
        // created via `.trace()` or a similar function
        // FIXME: Create a trace if it doesn't exist
        let delayed_trace = self
            .circuit()
            .cache_get_or_insert_with(DelayedTraceId::new(self.stream_id()), || {
                panic!("called `.delay_trace()` on a stream without a previously created trace")
            })
            .deref()
            .clone();
        delayed_trace.apply(|spine: &Spine<B>| spine.ro_snapshot())
    }
}

pub struct UntimedTraceAppend<T>
where
    T: Trace,
{
    // Total number of input tuples processed by the operator.
    num_inputs: usize,

    _phantom: PhantomData<T>,
}

impl<T> Default for UntimedTraceAppend<T>
where
    T: Trace,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T> UntimedTraceAppend<T>
where
    T: Trace,
{
    pub fn new() -> Self {
        Self {
            num_inputs: 0,
            _phantom: PhantomData,
        }
    }
}

impl<T> Operator for UntimedTraceAppend<T>
where
    T: Trace + 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("UntimedTraceAppend")
    }

    fn metadata(&self, meta: &mut OperatorMeta) {
        meta.extend(metadata! {
            NUM_INPUTS => MetaItem::Count(self.num_inputs),
        });
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<T> BinaryOperator<T, T::Batch, T> for UntimedTraceAppend<T>
where
    T: Trace + 'static,
{
    async fn eval(&mut self, _trace: &T, _batch: &T::Batch) -> T {
        // Refuse to accept trace by reference.  This should not happen in a correctly
        // constructed circuit.
        panic!("UntimedTraceAppend::eval(): cannot accept trace by reference")
    }

    #[trace]
    async fn eval_owned_and_ref(&mut self, mut trace: T, batch: &T::Batch) -> T {
        self.num_inputs += batch.len();
        trace.insert(batch.clone());
        trace
    }

    async fn eval_ref_and_owned(&mut self, _trace: &T, _batch: T::Batch) -> T {
        // Refuse to accept trace by reference.  This should not happen in a correctly
        // constructed circuit.
        panic!("UntimedTraceAppend::eval_ref_and_owned(): cannot accept trace by reference")
    }

    #[trace]
    async fn eval_owned(&mut self, mut trace: T, batch: T::Batch) -> T {
        self.num_inputs += batch.len();

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

pub struct TraceAppend<T: Trace, B: BatchReader, C> {
    clock: C,
    output_factories: T::Factories,

    // Total number of input tuples processed by the operator.
    num_inputs: usize,

    _phantom: PhantomData<(T, B)>,
}

impl<T: Trace, B: BatchReader, C> TraceAppend<T, B, C> {
    pub fn new(output_factories: &T::Factories, clock: C) -> Self {
        Self {
            clock,
            output_factories: output_factories.clone(),
            num_inputs: 0,
            _phantom: PhantomData,
        }
    }
}

impl<T, B, Clk> Operator for TraceAppend<T, B, Clk>
where
    T: Trace,
    B: BatchReader,
    Clk: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("TraceAppend")
    }
    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }

    fn metadata(&self, meta: &mut OperatorMeta) {
        meta.extend(metadata! {
            NUM_INPUTS => MetaItem::Count(self.num_inputs),
        });
    }
}

impl<T, B, Clk> BinaryOperator<T, B, T> for TraceAppend<T, B, Clk>
where
    B: BatchReader<Time = ()>,
    Clk: WithClock + 'static,
    T: Trace<Key = B::Key, Val = B::Val, R = B::R, Time = Clk::Time>,
{
    #[trace]
    async fn eval(&mut self, _trace: &T, _batch: &B) -> T {
        // Refuse to accept trace by reference.  This should not happen in a correctly
        // constructed circuit.
        unimplemented!()
    }

    #[trace]
    async fn eval_owned_and_ref(&mut self, mut trace: T, batch: &B) -> T {
        // TODO: extend `trace` type to feed untimed batches directly
        // (adding fixed timestamp on the fly).
        self.num_inputs += batch.len();
        trace.insert(T::Batch::from_batch(
            batch,
            &self.clock.time(),
            &self.output_factories,
        ));
        trace
    }

    async fn eval_ref_and_owned(&mut self, _trace: &T, _batch: B) -> T {
        // Refuse to accept trace by reference.  This should not happen in a correctly
        // constructed circuit.
        unimplemented!()
    }

    #[trace]
    async fn eval_owned(&mut self, mut trace: T, batch: B) -> T {
        self.num_inputs += batch.len();

        if TypeId::of::<B>() == TypeId::of::<T::Batch>() {
            let mut batch = Some(batch);
            let batch = unsafe { transmute::<&mut Option<B>, &mut Option<T::Batch>>(&mut batch) };
            trace.insert(batch.take().unwrap());
        } else {
            trace.insert(T::Batch::from_batch(
                &batch,
                &self.clock.time(),
                &self.output_factories,
            ));
        }
        trace
    }

    fn input_preference(&self) -> (OwnershipPreference, OwnershipPreference) {
        (
            OwnershipPreference::PREFER_OWNED,
            OwnershipPreference::PREFER_OWNED,
        )
    }
}

#[self_referencing]
struct ReplayState<T: Trace> {
    trace: T,
    #[borrows(trace)]
    #[covariant]
    cursor: Box<dyn MergeCursor<T::Key, T::Val, T::Time, T::R> + Send + 'this>,
}

impl<T: Trace> ReplayState<T> {
    fn create(trace: T) -> Self {
        ReplayStateBuilder {
            trace,
            cursor_builder: |trace| trace.merge_cursor(None, None),
        }
        .build()
    }
}

pub struct Z1Trace<C: Circuit, B: Batch, T: Trace> {
    // For error reporting.
    global_id: GlobalNodeId,
    time: T::Time,
    trace: Option<T>,
    replay_state: Option<ReplayState<T>>,
    replay_mode: bool,
    trace_factories: T::Factories,
    // `dirty[scope]` is `true` iff at least one non-empty update was added to the trace
    // since the previous clock cycle at level `scope`.
    dirty: Vec<bool>,
    root_scope: Scope,
    reset_on_clock_start: bool,
    bounds: TraceBounds<T::Key, T::Val>,

    // Metrics maintained by the trace.
    batch_factories: B::Factories,
    // Stream whose integral this Z1 operator stores, if any.
    delta_stream: Option<Stream<C, B>>,
}

impl<C, B, T> Z1Trace<C, B, T>
where
    C: Circuit,
    B: Batch,
    T: Trace,
{
    pub fn new(
        trace_factories: &T::Factories,
        batch_factories: &B::Factories,
        reset_on_clock_start: bool,
        root_scope: Scope,
        bounds: TraceBounds<T::Key, T::Val>,
    ) -> Self {
        Self {
            global_id: GlobalNodeId::root(),
            time: <T::Time as Timestamp>::clock_start(),
            trace: None,
            replay_state: None,
            replay_mode: false,
            trace_factories: trace_factories.clone(),
            batch_factories: batch_factories.clone(),
            dirty: vec![false; root_scope as usize + 1],
            root_scope,
            reset_on_clock_start,
            bounds,
            delta_stream: None,
        }
    }

    /// Creates a stream that will be used to replay the contents of `stream`.
    ///
    /// Given a circuit that implements an integral, the Z-1 operator can be used
    /// to replay the `delta` stream during bootstrapping.  This function sets this
    /// up at circuit construction time. It creates a new stream (`replay_stream`)
    /// that aliases `stream` internally.  In replay mode, Z-1 will send the contents
    /// of the integral to `replay_stream` chunk-by-chunk.
    ///
    ///   │stream
    ///   │
    ///   │
    ///   │◄............
    ///   │            .replay_stream
    ///   ▼            .
    /// ┌───┐        ┌───┐
    /// │ + ├───────►│Z-1│
    /// └───┘        └─┬─┘
    ///   ▲            │
    ///   │            │
    ///   └────────────┘
    ///
    /// Note that at most one of `stream` or `replay_stream` can be active at a time.
    /// During normal operation, `stream` is active and `replay_stream` is not.  During
    /// replay, `replay_stream` is active while the operator that normally write to
    /// stream is disabled.
    pub fn prepare_replay_stream(&mut self, stream: &Stream<C, B>) -> Stream<C, B> {
        let replay_stream = Stream::with_value(
            stream.circuit().clone(),
            self.global_id.local_node_id().unwrap(),
            stream.value(),
        );

        self.delta_stream = Some(replay_stream.clone());
        replay_stream
    }
}

impl<C, B, T> Operator for Z1Trace<C, B, T>
where
    C: Circuit,
    B: Batch,
    T: Trace,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("Z1 (trace)")
    }

    fn clock_start(&mut self, scope: Scope) {
        self.dirty[scope as usize] = false;

        if scope == 0 && self.trace.is_none() {
            self.trace = Some(T::new(&self.trace_factories));
        }
    }

    fn clock_end(&mut self, scope: Scope) {
        if scope + 1 == self.root_scope && !self.reset_on_clock_start {
            if let Some(tr) = self.trace.as_mut() {
                tr.set_frontier(&self.time.epoch_start(scope));
            }
        }
        self.time = self.time.advance(scope + 1);
    }

    fn init(&mut self, global_id: &GlobalNodeId) {
        self.global_id = global_id.clone();
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
            NUM_ENTRIES_LABEL => MetaItem::Count(total_size),
            ALLOCATED_BYTES_LABEL => MetaItem::bytes(bytes.total_bytes()),
            USED_BYTES_LABEL => MetaItem::bytes(bytes.used_bytes()),
            "allocations" => MetaItem::Count(bytes.distinct_allocations()),
            SHARED_BYTES_LABEL => MetaItem::bytes(bytes.shared_bytes()),
            "bounds" => self.bounds.metadata()
        });

        if let Some(trace) = self.trace.as_ref() {
            trace.metadata(meta);
        }
    }

    fn fixedpoint(&self, scope: Scope) -> bool {
        !self.dirty[scope as usize] && self.replay_state.is_none()
    }

    fn commit(&mut self, base: &StoragePath, pid: Option<&str>) -> Result<(), Error> {
        let pid = require_persistent_id(pid, &self.global_id)?;
        self.trace
            .as_mut()
            .map(|trace| trace.commit(base, pid))
            .unwrap_or(Ok(()))
    }

    fn restore(&mut self, base: &StoragePath, pid: Option<&str>) -> Result<(), Error> {
        let pid = require_persistent_id(pid, &self.global_id)?;

        self.trace
            .as_mut()
            .map(|trace| trace.restore(base, pid))
            .unwrap_or(Ok(()))
    }

    fn clear_state(&mut self) -> Result<(), Error> {
        // println!("Z1Trace-{}::clear_state", &self.global_id);
        self.trace = Some(T::new(&self.trace_factories));
        self.replay_state = None;
        self.dirty = vec![false; self.root_scope as usize + 1];

        Ok(())
    }

    fn start_replay(&mut self) -> Result<(), Error> {
        // The second condition is necessary if `start_replay` is called twice, for the input
        // and output halves of Z1.
        // println!(
        //     "Z1Trace-{}::start_replay delta_stream: {:?}",
        //     &self.global_id,
        //     self.delta_stream.is_some()
        // );
        self.replay_mode = true;
        if self.delta_stream.is_some() && self.replay_state.is_none() {
            let trace = self.trace.take().expect("Z1Trace::start_replay: no trace");
            self.trace = Some(T::new(&self.trace_factories));

            //println!("Z1Trace-{}::initializing replay_state", &self.global_id);

            self.replay_state = Some(ReplayState::create(trace));
        }

        Ok(())
    }

    fn is_replay_complete(&self) -> bool {
        self.replay_state.is_none()
    }

    fn end_replay(&mut self) -> Result<(), Error> {
        //println!("Z1Trace-{}::end_replay", &self.global_id);
        self.replay_mode = false;
        self.replay_state = None;

        Ok(())
    }
}

impl<C, B, T> StrictOperator<T> for Z1Trace<C, B, T>
where
    C: Circuit,
    B: Batch<Key = T::Key, Val = T::Val, Time = (), R = T::R>,
    T: Trace,
{
    fn get_output(&mut self) -> T {
        //println!("Z1-{}::get_output", &self.global_id);
        let replay_step_size = Runtime::replay_step_size();

        if self.replay_mode {
            if let Some(replay) = &mut self.replay_state {
                //println!("Z1-{}::get_output: replaying", &self.global_id);
                let mut builder = <B::Builder as Builder<B>>::with_capacity(
                    &self.batch_factories,
                    replay_step_size,
                );

                let mut num_values = 0;
                let mut weight = self.batch_factories.weight_factory().default_box();

                while replay.borrow_cursor().key_valid() && num_values < replay_step_size {
                    let mut values_added = false;
                    while replay.borrow_cursor().val_valid() && num_values < replay_step_size {
                        weight.set_zero();
                        replay.with_cursor_mut(|cursor| {
                            cursor.map_times(&mut |_t, w| weight.add_assign(w))
                        });

                        if !weight.is_zero() {
                            builder.push_val_diff(replay.borrow_cursor().val(), weight.as_ref());
                            values_added = true;
                            num_values += 1;
                        }
                        replay.with_cursor_mut(|cursor| cursor.step_val());
                    }
                    if values_added {
                        builder.push_key(replay.borrow_cursor().key());
                    }
                    if !replay.borrow_cursor().val_valid() {
                        replay.with_cursor_mut(|cursor| cursor.step_key());
                    }
                }

                let batch = builder.done();
                self.delta_stream.as_ref().unwrap().value().put(batch);
                if !replay.borrow_cursor().key_valid() {
                    self.replay_state = None;
                }
            } else {
                // Continue producing empty outputs as long as the circuit is in the replay mode.
                self.delta_stream
                    .as_ref()
                    .unwrap()
                    .value()
                    .put(B::dyn_empty(&self.batch_factories));
            }
        }

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

impl<C, B, T> StrictUnaryOperator<T, T> for Z1Trace<C, B, T>
where
    C: Circuit,
    B: Batch<Key = T::Key, Val = T::Val, Time = (), R = T::R>,
    T: Trace,
{
    async fn eval_strict(&mut self, _i: &T) {
        unimplemented!()
    }

    #[trace]
    async fn eval_strict_owned(&mut self, mut i: T) {
        // println!("Z1-{}::eval_strict_owned", &self.global_id);

        self.time = self.time.advance(0);

        let dirty = i.dirty();

        if let Some(filter) = self.bounds.effective_key_filter() {
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

    use crate::{dynamic::DynData, utils::Tup2, Runtime, Stream, TypedBox, ZWeight};
    use proptest::{collection::vec, prelude::*};
    use size_of::SizeOf;

    fn quasi_monotone_batches(
        key_window_size: i32,
        key_window_step: i32,
        val_window_size: i32,
        val_window_step: i32,
        max_tuples: usize,
        batches: usize,
    ) -> impl Strategy<Value = Vec<Vec<((i32, i32), ZWeight)>>> {
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
                        1..2i64,
                    ),
                    0..max_tuples,
                )
            })
            .collect::<Vec<_>>()
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(16))]
        #[test]
        #[ignore = "this test can be flaky especially on aarch64"]
        fn test_integrate_trace_retain(batches in quasi_monotone_batches(100, 20, 1000, 200, 100, 200)) {
            let (mut dbsp, input_handle) = Runtime::init_circuit(4, move |circuit| {
                let (stream, handle) = circuit.add_input_indexed_zset::<i32, i32>();
                let stream = stream.shard();
                let watermark: Stream<_, TypedBox<(i32, i32), DynData>> = stream
                    .waterline(
                        || (i32::MIN, i32::MIN),
                        |k, v| (*k, *v),
                        |(ts1_left, ts2_left), (ts1_right, ts2_right)| {
                            (max(*ts1_left, *ts1_right), max(*ts2_left, *ts2_right))
                        },
                    );

                let trace = stream.integrate_trace();
                stream.integrate_trace_retain_keys(&watermark, |key, ts| *key >= ts.0.saturating_sub(100));
                trace.apply(|trace| {
                    // println!("retain_keys: {}bytes", trace.size_of().total_bytes());
                    assert!(trace.size_of().total_bytes() < 100_000);
                });

                let stream2 = stream.map_index(|(k, v)| (*k, *v)).shard();

                let trace2 = stream2.integrate_trace();
                stream2.integrate_trace_retain_values(&watermark, |val, ts| *val >= ts.1.saturating_sub(1000));

                trace2.apply(|trace| {
                    // println!("retain_vals: {}bytes", trace.size_of().total_bytes());
                    assert!(trace.size_of().total_bytes() < 100_000);
                });

                Ok(handle)
            })
            .unwrap();

            for batch in batches {
                let mut tuples = batch.into_iter().map(|((k, v), r)| Tup2(k, Tup2(v, r))).collect::<Vec<_>>();
                input_handle.append(&mut tuples);
                dbsp.step().unwrap();
            }
        }
    }
}
