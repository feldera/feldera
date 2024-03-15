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
    trace::{copy_batch, Batch, BatchReader, Filter, Spillable, Spine, Stored, Trace},
    Error, Runtime, Timestamp,
};
use dyn_clone::clone_box;
use size_of::SizeOf;
use std::{
    borrow::Cow,
    cell::{Ref, RefCell},
    cmp::Ordering,
    marker::PhantomData,
    ops::Deref,
    rc::Rc,
};

circuit_cache_key!(TraceId<C, D: BatchReader>(GlobalNodeId => Stream<C, D>));
circuit_cache_key!(BoundsId<D: BatchReader>(GlobalNodeId => TraceBounds<<D as BatchReader>::Key, <D as BatchReader>::Val>));
circuit_cache_key!(DelayedTraceId<C, D>(GlobalNodeId => Stream<C, D>));
circuit_cache_key!(SpillId<C, D>(GlobalNodeId => Stream<C, D>));

/// Lower bound on keys or values in a trace.
///
/// Setting the bound to `None` is equivalent to setting it to
/// `T::min_value()`, i.e., the contents of the trace will never
/// get truncated.
///
/// The writer can update the value of the bound at each clock
/// cycle.  The bound can only increase monotonically.
#[derive(Debug)]
#[repr(transparent)]
pub struct TraceBound<T: ?Sized>(Rc<RefCell<Option<Box<T>>>>);

impl<T: DataTrait + ?Sized> Clone for TraceBound<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
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
    pub(crate) fn effective_key_bound(&self) -> Option<Box<K>> {
        (*self.0)
            .borrow()
            .key_bounds
            .iter()
            .min()
            .expect("At least one trace bound must be set")
            .get()
            .deref()
            .as_ref()
            .map(|bx| clone_box(bx.as_ref()))
    }

    /// Set key retainment condition.
    pub(crate) fn key_filter(&self) -> Option<Filter<K>> {
        (*self.0).borrow().key_filter.clone()
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
                .map(|bx| Rc::from(clone_box(bx.as_ref())))
                .map(|bound: Rc<V>| {
                    Box::new(move |v: &V| bound.as_ref().cmp(v) != Ordering::Greater) as Filter<V>
                }),
            Predicate::Filter(filter) => Some(filter.clone()),
        }
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

struct TraceBoundsInner<K: ?Sized + 'static, V: ?Sized + 'static> {
    /// Key bounds.
    key_bounds: Vec<TraceBound<K>>,
    /// Key retainment condition (can be set at the same time as one
    /// or more key bounds).
    key_filter: Option<Filter<K>>,
    /// Value bounds _or_ retainment condition.
    val_predicate: Predicate<V>,
}

// TODO: add infrastructure to compact the trace during slack time.

/// A key-only [`Spine`] of `C`'s default batch type, with key and weight types
/// taken from `B`.
pub type KeySpine<B, C> = Spine<
    <<C as WithClock>::Time as Timestamp>::MemKeyBatch<
        <B as BatchReader>::Key,
        <B as BatchReader>::R,
    >,
>;

/// A [`Spine`] of `C`'s default batch type, with key, value, and weight types
/// taken from `B`.
pub type ValSpine<B, C> = Spine<
    <<C as WithClock>::Time as Timestamp>::MemValBatch<
        <B as BatchReader>::Key,
        <B as BatchReader>::Val,
        <B as BatchReader>::R,
    >,
>;

/// An on-storage, key-only [`Spine`] of `C`'s default batch type, with key and
/// weight types taken from `B`.
pub type FileKeySpine<B, C> = Spine<
    <<C as WithClock>::Time as Timestamp>::FileKeyBatch<
        <B as BatchReader>::Key,
        <B as BatchReader>::R,
    >,
>;

/// An on-stroage [`Spine`] of `C`'s default batch type, with key, value, and
/// weight types taken from `B`.
pub type FileValSpine<B, C> = Spine<
    <<C as WithClock>::Time as Timestamp>::FileValBatch<
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
    /// Spills the in-memory batches in `self` to storage.
    pub fn dyn_spill(
        &self,
        output_factories: &<B::Spilled as BatchReader>::Factories,
    ) -> Stream<C, B::Spilled>
    where
        B: Spillable,
    {
        // We construct the trace bounds for the unspilled stream and copy it
        // into the spilled stream, to ensure that `spilled.trace_bounds()` is
        // the same as `self.trace_bounds()`.  This is an important property
        // (see [`Self::trace_bounds`]).
        //
        // (We can't do this the same way as for sharded streams, by mapping
        // from the spilled stream back to the unspilled stream as with
        // [`Stream::try_unsharded_version`], because the types are different;
        // we'd need an `Unspillable` trait.)
        let bounds = self.trace_bounds();
        let sharded = self.try_sharded_version();
        let spilled = self
            .circuit()
            .cache_get_or_insert_with(SpillId::new(self.origin_node_id().clone()), || {
                let output_factories = output_factories.clone();
                let spilled = sharded.apply(move |batch| batch.spill(&output_factories));
                self.circuit().cache_insert(
                    BoundsId::<B>::new(spilled.origin_node_id().clone()),
                    bounds.clone(),
                );
                spilled
            })
            .clone();
        spilled.mark_sharded_if(self);
        spilled
    }

    /// Reads stored batches back into memory.
    pub fn dyn_unspill(
        &self,
        output_factories: &<B::Unspilled as BatchReader>::Factories,
    ) -> Stream<C, B::Unspilled>
    where
        B: Stored,
    {
        let output_factories = output_factories.clone();
        let unspilled = self.apply(move |batch| batch.unspill(&output_factories));
        if self.is_sharded() {
            unspilled.mark_sharded();
        }
        if self.is_distinct() {
            unspilled.mark_distinct();
        }
        unspilled
    }

    /// See [`Stream::trace`].
    pub fn dyn_trace(
        &self,
        output_factories: &<FileValSpine<B, C> as BatchReader>::Factories,
    ) -> Stream<C, FileValSpine<B, C>>
    where
        B: Batch<Time = ()>,
    {
        self.dyn_trace_with_bound(output_factories, TraceBound::new(), TraceBound::new())
    }

    /// See [`Stream::trace_with_bound`].
    pub fn dyn_trace_with_bound(
        &self,
        output_factories: &<FileValSpine<B, C> as BatchReader>::Factories,
        lower_key_bound: TraceBound<B::Key>,
        lower_val_bound: TraceBound<B::Val>,
    ) -> Stream<C, FileValSpine<B, C>>
    where
        B: Batch<Time = ()>,
    {
        let bounds = self.trace_bounds_with_bound(lower_key_bound, lower_val_bound);

        self.circuit()
            .cache_get_or_insert_with(TraceId::new(self.origin_node_id().clone()), || {
                let circuit = self.circuit();

                circuit.region("trace", || {
                    let persistent_id = format!(
                        "{}-{:?}",
                        Runtime::worker_index(),
                        self.origin_node_id().clone()
                    );

                    let (local, z1feedback) = circuit.add_feedback(Z1Trace::new(
                        output_factories,
                        false,
                        circuit.root_scope(),
                        bounds.clone(),
                        persistent_id,
                    ));
                    let trace = circuit.add_binary_operator_with_preference(
                        <TraceAppend<FileValSpine<B, C>, B, C>>::new(
                            output_factories,
                            circuit.clone(),
                        ),
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
        self.circuit()
            .cache_get_or_insert_with(
                BoundsId::<B>::new(self.try_unsharded_version().origin_node_id().clone()),
                TraceBounds::new,
            )
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
        B: Batch<Time = ()> + Stored,
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
        B: Batch<Time = ()> + Stored,
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
            .cache_get_or_insert_with(TraceId::new(self.origin_node_id().clone()), || {
                let circuit = self.circuit();
                let bounds = bounds.clone();

                circuit.region("integrate_trace", || {
                    let (ExportStream { local, export }, z1feedback) = circuit
                        .add_feedback_with_export(Z1Trace::new(
                            input_factories,
                            true,
                            circuit.root_scope(),
                            bounds,
                            self.origin_node_id().persistent_id(),
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
        circuit.cache_insert(TraceId::new(stream.origin_node_id().clone()), trace.clone());
        circuit.cache_insert(
            BoundsId::<T>::new(stream.origin_node_id().clone()),
            self.bounds.clone(),
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
        factories: &T::Factories,
        bounds: TraceBounds<T::Key, T::Val>,
    ) -> TraceFeedbackConnector<Self, T>
    where
        T: Trace<Time = ()> + Clone,
    {
        let (ExportStream { local, export }, feedback) =
            self.add_feedback_with_export(Z1Trace::new(
                factories,
                true,
                self.root_scope(),
                bounds.clone(),
                self.global_node_id().persistent_id(),
            ));

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

pub struct TraceAppend<T: Trace, B: BatchReader, C> {
    clock: C,
    output_factories: T::Factories,
    _phantom: PhantomData<(T, B)>,
}

impl<T: Trace, B: BatchReader, C> TraceAppend<T, B, C> {
    pub fn new(output_factories: &T::Factories, clock: C) -> Self {
        Self {
            clock,
            output_factories: output_factories.clone(),
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
        trace.insert(copy_batch(
            batch,
            &self.clock.time(),
            &self.output_factories,
        ));
        trace
    }

    fn eval_ref_and_owned(&mut self, _trace: &T, _batch: B) -> T {
        // Refuse to accept trace by reference.  This should not happen in a correctly
        // constructed circuit.
        unimplemented!()
    }

    fn eval_owned(&mut self, mut trace: T, batch: B) -> T {
        trace.insert(copy_batch(
            &batch,
            &self.clock.time(),
            &self.output_factories,
        ));
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
    factories: T::Factories,
    // `dirty[scope]` is `true` iff at least one non-empty update was added to the trace
    // since the previous clock cycle at level `scope`.
    dirty: Vec<bool>,
    root_scope: Scope,
    reset_on_clock_start: bool,
    bounds: TraceBounds<T::Key, T::Val>,
    effective_key_bound: Option<Box<T::Key>>,
    persistent_id: String,
}

impl<T> Z1Trace<T>
where
    T: Trace,
{
    pub fn new<S: AsRef<str>>(
        factories: &T::Factories,
        reset_on_clock_start: bool,
        root_scope: Scope,
        bounds: TraceBounds<T::Key, T::Val>,
        persistent_id: S,
    ) -> Self {
        Self {
            time: <T::Time as Timestamp>::clock_start(),
            trace: None,
            factories: factories.clone(),
            dirty: vec![false; root_scope as usize + 1],
            root_scope,
            reset_on_clock_start,
            bounds,
            effective_key_bound: None,
            persistent_id: persistent_id.as_ref().to_string(),
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
            self.trace = Some(T::new(&self.factories, &self.persistent_id));
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

    fn commit(&self, cid: u64) -> Result<(), Error> {
        self.trace
            .as_ref()
            .map(|trace| trace.commit(cid))
            .unwrap_or(Ok(()))
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
        self.effective_key_bound = effective_key_bound.map(|b| clone_box(b.as_ref()));
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
        #![proptest_config(ProptestConfig::with_cases(32))]
        #[test]
        fn test_integrate_trace_retain(batches in quasi_monotone_batches(100, 20, 1000, 200, 100, 100)) {
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

                let trace = stream.spill().integrate_trace();
                stream.integrate_trace_retain_keys(&watermark, |key, ts| *key >= ts.0 - 100);
                trace.apply(|trace| {
                    //println!("retain_keys: {}bytes", trace.size_of().total_bytes());
                    assert!(trace.size_of().total_bytes() < 40000);
                });

                let stream2 = stream.map_index(|(k, v)| (*k, *v)).shard();

                let trace2 = stream2.spill().integrate_trace();
                stream2.integrate_trace_retain_values(&watermark, |val, ts| *val >= ts.1 - 1000);

                trace2.apply(|trace| {
                    //println!("retain_vals: {}bytes", trace.size_of().total_bytes());
                    assert!(trace.size_of().total_bytes() < 40000);
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
