use crate::circuit::metadata::{INPUT_RECORDS_COUNT, MEMORY_ALLOCATIONS_COUNT, RETAINMENT_BOUNDS};
use crate::circuit::{NodeId, splitter_output_chunk_size};
use crate::circuit::{
    bootstrap::register_replay_stream,
    circuit_builder::{StreamId, StreamMetadata},
};
use crate::dynamic::Factory;
use crate::operator::dynamic::replay::ReplayState;
use crate::operator::dynamic::trace::{DelayedTraceId, TraceBounds};
use crate::operator::{TraceBound, require_persistent_id};
use crate::trace::GroupFilter;
use crate::trace::spine_async::WithSnapshot;
use crate::{
    Error, Timestamp,
    circuit::{
        Circuit, ExportId, ExportStream, FeedbackConnector, GlobalNodeId, OwnershipPreference,
        Scope, Stream, WithClock,
        metadata::{
            ALLOCATED_MEMORY_BYTES, MetaItem, OperatorMeta, SHARED_MEMORY_BYTES,
            STATE_RECORDS_COUNT, USED_MEMORY_BYTES,
        },
        operator_traits::{BinaryOperator, Operator, StrictOperator, StrictUnaryOperator},
    },
    circuit_cache_key,
    dynamic::DataTrait,
    trace::{Batch, BatchReader, Filter, Spine, SpineSnapshot, Trace, TraceRole},
};
use feldera_storage::StoragePath;
use size_of::SizeOf;
use std::any::{Any, TypeId};
use std::ops::Range;
use std::{borrow::Cow, marker::PhantomData, ops::Deref};

// Trace of a collection updated once per clock cycle.
circuit_cache_key!(AccumulateTraceId<C, D: BatchReader>(StreamId => Stream<C, D>));

// Trace of a sharded collection updated once per clock cycle.
circuit_cache_key!(ShardedAccumulateTraceId<C, D: BatchReader>(StreamId => Stream<C, D>));

// Trace of a collection sharded to a range of workers, updated once per clock cycle.
circuit_cache_key!(ShardedWorkersAccumulateTraceId<C, D: BatchReader>((StreamId, Range<usize>) => Stream<C, D>));

circuit_cache_key!(AccumulateBoundsId<D: BatchReader>(StreamId => TraceBounds<<D as BatchReader>::Key, <D as BatchReader>::Val>));

// Trace of a collection delayed by one clock cycle.
circuit_cache_key!(AccumulateDelayedTraceId<C, D>(StreamId => Stream<C, D>));

pub type TimedSpine<B, C> = Spine<<<C as WithClock>::Time as Timestamp>::TimedBatch<B>>;

impl<C, B> Stream<C, B>
where
    C: Circuit,
    B: Clone + Send + Sync + 'static,
{
    /// See [`Stream::accumulate_trace`].
    pub fn dyn_accumulate_trace(
        &self,
        output_factories: &<TimedSpine<B, C> as BatchReader>::Factories,
        batch_factories: &B::Factories,
    ) -> Stream<C, TimedSpine<B, C>>
    where
        B: Batch<Time = ()>,
    {
        self.dyn_accumulate_trace_with_bound(
            output_factories,
            batch_factories,
            TraceBound::new(),
            TraceBound::new(),
        )
    }

    pub fn dyn_shard_accumulate_trace(
        &self,
        output_factories: &<TimedSpine<B, C> as BatchReader>::Factories,
        batch_factories: &B::Factories,
    ) -> Stream<C, TimedSpine<B, C>>
    where
        B: Batch<Time = ()>,
    {
        let bounds = self.accumulate_trace_bounds_with_bound(TraceBound::new(), TraceBound::new());

        // The stream is already sharded -- use regular accumulate_trace that doesn't shard.
        if let Some(sharded) = self.get_sharded_version() {
            return sharded.dyn_accumulate_trace_with_bound(
                output_factories,
                batch_factories,
                TraceBound::new(),
                TraceBound::new(),
            );
        }

        self.circuit()
            .cache_get_or_insert_with(ShardedAccumulateTraceId::new(self.stream_id()), || {
                let circuit = self.circuit();

                let accumulated: Stream<C, Option<Spine<B>>> = self
                    .dyn_shard_accumulate(batch_factories)
                    .into_enabled_stream();

                circuit.region("shard_accumulate_trace", || {
                    let persistent_id = self.get_persistent_id();
                    let z1: AccumulateZ1Trace<
                        C,
                        B,
                        Spine<<<C as WithClock>::Time as Timestamp>::TimedBatch<B>>,
                    > = AccumulateZ1Trace::new(
                        output_factories,
                        batch_factories,
                        false,
                        circuit.root_scope(),
                        bounds.clone(),
                    );
                    let (delayed_trace, z1feedback) = circuit.add_feedback_persistent(
                        persistent_id
                            .map(|name| format!("{name}.shard_accintegral"))
                            .as_deref(),
                        z1,
                    );

                    let replay_stream = z1feedback.operator_mut().prepare_replay_stream(self);

                    let trace = circuit.add_binary_operator_with_preference(
                        <AccumulateTraceAppend<TimedSpine<B, C>, B, C>>::new(
                            output_factories,
                            circuit.clone(),
                        ),
                        (&delayed_trace, OwnershipPreference::STRONGLY_PREFER_OWNED),
                        (&accumulated, OwnershipPreference::PREFER_OWNED),
                    );
                    delayed_trace.mark_sharded();
                    trace.mark_sharded();

                    z1feedback.connect_with_preference(
                        &trace,
                        OwnershipPreference::STRONGLY_PREFER_OWNED,
                    );

                    // Connect the replay stream to the original non-sharded stream, since the dyn_shard_accumulate operator
                    // doesn't expose a sharded stream.
                    // FIXME: this is suboptimal, since this will end up sharding the already sharded data again.
                    // We should instead replay by moving the entire integral into the accumulator. This will require
                    // integrating bootstrapping with transactions better.
                    register_replay_stream(circuit, self, &replay_stream, batch_factories);

                    circuit.cache_insert(DelayedTraceId::new(trace.stream_id()), delayed_trace);
                    trace
                })
            })
            .clone()
    }

    /// See [`Stream::accumulate_trace_with_bound`].
    pub fn dyn_accumulate_trace_with_bound(
        &self,
        output_factories: &<TimedSpine<B, C> as BatchReader>::Factories,
        batch_factories: &B::Factories,
        lower_key_bound: TraceBound<B::Key>,
        lower_val_bound: TraceBound<B::Val>,
    ) -> Stream<C, TimedSpine<B, C>>
    where
        B: Batch<Time = ()>,
    {
        let bounds = self.accumulate_trace_bounds_with_bound(lower_key_bound, lower_val_bound);

        self.circuit()
            .cache_get_or_insert_with(AccumulateTraceId::new(self.stream_id()), || {
                let circuit = self.circuit();

                let accumulated = self.dyn_accumulate(batch_factories).into_enabled_stream();

                circuit.region("accumulate_trace", || {
                    let persistent_id = self.get_persistent_id();
                    let z1 = AccumulateZ1Trace::new(
                        output_factories,
                        batch_factories,
                        false,
                        circuit.root_scope(),
                        bounds.clone(),
                    );
                    let (delayed_trace, z1feedback) = circuit.add_feedback_persistent(
                        persistent_id
                            .map(|name| format!("{name}.accintegral"))
                            .as_deref(),
                        z1,
                    );

                    let replay_stream = z1feedback.operator_mut().prepare_replay_stream(self);

                    let trace = circuit.add_binary_operator_with_preference(
                        <AccumulateTraceAppend<TimedSpine<B, C>, B, C>>::new(
                            output_factories,
                            circuit.clone(),
                        ),
                        (&delayed_trace, OwnershipPreference::STRONGLY_PREFER_OWNED),
                        (&accumulated, OwnershipPreference::PREFER_OWNED),
                    );
                    if self.is_sharded() {
                        delayed_trace.mark_sharded();
                        trace.mark_sharded();
                    }

                    z1feedback.connect_with_preference(
                        &trace,
                        OwnershipPreference::STRONGLY_PREFER_OWNED,
                    );

                    register_replay_stream(circuit, self, &replay_stream, batch_factories);

                    circuit.cache_insert(DelayedTraceId::new(trace.stream_id()), delayed_trace);
                    trace
                })
            })
            .clone()
    }

    /// See [`Stream::accumulate_integrate_trace_retain_keys`].
    #[track_caller]
    pub fn dyn_accumulate_integrate_trace_retain_keys<TS>(
        &self,
        bounds_stream: &Stream<C, Box<TS>>,
        retain_key_func: Box<dyn Fn(&TS) -> Filter<B::Key>>,
    ) where
        B: Batch<Time = ()>,
        TS: DataTrait + ?Sized,
        Box<TS>: Clone,
    {
        let bounds = self.accumulate_trace_bounds();
        bounds.set_unique_key_bound_name(bounds_stream.get_persistent_id().as_deref());
        bounds_stream.inspect(move |ts| {
            let filter = retain_key_func(ts.as_ref());
            bounds.set_key_filter(filter);
        });
    }

    /// See [`Stream::accumulate_integrate_trace_retain_values`].
    #[track_caller]
    pub fn dyn_accumulate_integrate_trace_retain_values<TS>(
        &self,
        bounds_stream: &Stream<C, Box<TS>>,
        retain_val_func: Box<dyn Fn(&TS) -> Filter<B::Val>>,
    ) where
        B: Batch<Time = ()>,
        TS: DataTrait + ?Sized,
        Box<TS>: Clone,
    {
        let bounds = self.accumulate_trace_bounds();
        bounds.set_unique_val_bound_name(bounds_stream.get_persistent_id().as_deref());

        bounds_stream.inspect(move |ts| {
            let filter = GroupFilter::Simple(retain_val_func(ts.as_ref()));
            bounds.set_val_filter(filter);
        });
    }

    /// See [`Stream::accumulate_integrate_trace_retain_values_last_n`].
    #[track_caller]
    pub fn dyn_accumulate_integrate_trace_retain_values_last_n<TS>(
        &self,
        bounds_stream: &Stream<C, Box<TS>>,
        retain_val_func: Box<dyn Fn(&TS) -> Filter<B::Val>>,
        n: usize,
    ) where
        B: Batch<Time = ()>,
        TS: DataTrait + ?Sized,
        Box<TS>: Clone,
    {
        assert!(
            n > 0,
            "dyn_accumulate_integrate_trace_retain_values_last_n called with n = 0"
        );

        let bounds = self.accumulate_trace_bounds();
        bounds.set_unique_val_bound_name(bounds_stream.get_persistent_id().as_deref());

        bounds_stream.inspect(move |ts| {
            let filter = GroupFilter::LastN(n, retain_val_func(ts.as_ref()));
            bounds.set_val_filter(filter);
        });
    }

    /// See [`Stream::accumulate_integrate_trace_retain_values_top_n`].
    #[track_caller]
    pub fn dyn_accumulate_integrate_trace_retain_values_top_n<TS>(
        &self,
        val_factory: &'static dyn Factory<B::Val>,
        bounds_stream: &Stream<C, Box<TS>>,
        retain_val_func: Box<dyn Fn(&TS) -> Filter<B::Val>>,
        n: usize,
    ) where
        B: Batch<Time = ()>,
        TS: DataTrait + ?Sized,
        Box<TS>: Clone,
    {
        assert!(
            n > 0,
            "dyn_accumulate_integrate_trace_retain_values_top_n called with n = 0"
        );

        let bounds = self.accumulate_trace_bounds();
        bounds.set_unique_val_bound_name(bounds_stream.get_persistent_id().as_deref());

        bounds_stream.inspect(move |ts| {
            let filter = GroupFilter::TopN(n, retain_val_func(ts.as_ref()), val_factory);
            bounds.set_val_filter(filter);
        });
    }

    /// See [`Stream::accumulate_integrate_trace_retain_values_bottom_n`].
    #[track_caller]
    pub fn dyn_accumulate_integrate_trace_retain_values_bottom_n<TS>(
        &self,
        val_factory: &'static dyn Factory<B::Val>,
        bounds_stream: &Stream<C, Box<TS>>,
        retain_val_func: Box<dyn Fn(&TS) -> Filter<B::Val>>,
        n: usize,
    ) where
        B: Batch<Time = ()>,
        TS: DataTrait + ?Sized,
        Box<TS>: Clone,
    {
        assert!(
            n > 0,
            "dyn_accumulate_integrate_trace_retain_values_bottom_n called with n = 0"
        );

        let bounds = self.accumulate_trace_bounds();
        bounds.set_unique_val_bound_name(bounds_stream.get_persistent_id().as_deref());

        bounds_stream.inspect(move |ts| {
            let filter = GroupFilter::BottomN(n, retain_val_func(ts.as_ref()), val_factory);
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
    /// Using the source stream is a safer choice than using the sharded version,
    /// because it always exists, whereas the sharded version
    /// might be created only *after* we get the trace bounds for the source
    /// stream.
    fn accumulate_trace_bounds(&self) -> TraceBounds<B::Key, B::Val>
    where
        B: BatchReader,
    {
        let stream_id = self.try_unsharded_version().stream_id();

        self.circuit()
            .cache_get_or_insert_with(AccumulateBoundsId::<B>::new(stream_id), TraceBounds::new)
            .clone()
    }

    /// Retrieves trace bounds for `self`, or a sharded version of `self` if it
    /// exists, creating them if necessary, and adds bounds for
    /// `lower_key_bound` and `lower_val_bound`.
    fn accumulate_trace_bounds_with_bound(
        &self,
        lower_key_bound: TraceBound<B::Key>,
        lower_val_bound: TraceBound<B::Val>,
    ) -> TraceBounds<B::Key, B::Val>
    where
        B: BatchReader,
    {
        let bounds = self.accumulate_trace_bounds();
        bounds.add_key_bound(lower_key_bound);
        bounds.add_val_bound(lower_val_bound);
        bounds
    }

    #[track_caller]
    pub fn dyn_accumulate_integrate_trace(&self, factories: &B::Factories) -> Stream<C, Spine<B>>
    where
        B: Batch<Time = ()>,
        Spine<B>: SizeOf,
    {
        self.dyn_accumulate_integrate_trace_with_bound(
            factories,
            TraceBound::new(),
            TraceBound::new(),
        )
    }

    #[track_caller]
    pub fn dyn_shard_accumulate_integrate_trace(
        &self,
        factories: &B::Factories,
    ) -> Stream<C, Spine<B>>
    where
        B: Batch<Time = ()>,
        Spine<B>: SizeOf,
    {
        self.dyn_shard_accumulate_integrate_trace_inner(factories, false)
    }

    #[track_caller]
    pub fn dyn_shard_accumulate_integrate_trace_legacy(
        &self,
        factories: &B::Factories,
    ) -> Stream<C, Spine<B>>
    where
        B: Batch<Time = ()>,
        Spine<B>: SizeOf,
    {
        self.dyn_shard_accumulate_integrate_trace_inner(factories, true)
    }

    #[track_caller]
    pub fn dyn_shard_accumulate_integrate_trace_inner(
        &self,
        factories: &B::Factories,
        legacy_pid: bool,
    ) -> Stream<C, Spine<B>>
    where
        B: Batch<Time = ()>,
        Spine<B>: SizeOf,
    {
        // The stream is already sharded -- use regular accumulate_trace that doesn't shard.
        if let Some(sharded) = self.get_sharded_version() {
            return sharded.dyn_accumulate_integrate_trace(factories);
        }

        self.circuit()
            .cache_get_or_insert_with(ShardedAccumulateTraceId::new(self.stream_id()), || {
                let circuit = self.circuit();
                let bounds =
                    self.accumulate_trace_bounds_with_bound(TraceBound::new(), TraceBound::new());

                let persistent_id = self.get_persistent_id();

                circuit.region("shard_accumulate_integrate_trace", || {
                    let z1: AccumulateZ1Trace<C, B, Spine<B>> = AccumulateZ1Trace::new(
                        factories,
                        factories,
                        true,
                        circuit.root_scope(),
                        bounds,
                    );

                    let accumulated = self.dyn_shard_accumulate(factories).into_enabled_stream();

                    let integral_pid = if legacy_pid {
                        persistent_id.map(|name| format!("{name}.shard.accintegral"))
                    } else {
                        persistent_id.map(|name| format!("{name}.shard_accintegral"))
                    };

                    let (
                        ExportStream {
                            local: delayed_trace,
                            export,
                        },
                        z1feedback,
                    ) = circuit.add_feedback_with_export_persistent(integral_pid.as_deref(), z1);

                    let replay_stream = z1feedback.operator_mut().prepare_replay_stream(self);

                    let trace = circuit.add_binary_operator_with_preference(
                        AccumulateUntimedTraceAppend::<Spine<B>>::new(),
                        (&delayed_trace, OwnershipPreference::STRONGLY_PREFER_OWNED),
                        (&accumulated, OwnershipPreference::PREFER_OWNED),
                    );

                    if self.is_sharded() {
                        delayed_trace.mark_sharded();
                        trace.mark_sharded();
                    }

                    z1feedback.connect_with_preference(
                        &trace,
                        OwnershipPreference::STRONGLY_PREFER_OWNED,
                    );

                    // Connect the replay stream to the original non-sharded stream, since the dyn_shard_accumulate operator
                    // doesn't expose a sharded stream.
                    // FIXME: this is suboptimal, since this will end up sharding the already sharded data again.
                    // We should instead replay by moving the entire integral into the accumulator. This will require
                    // integrating bootstrapping with transactions better. An additional complication is that the integral
                    // can be timed, while the accumulator stores untimed updates
                    register_replay_stream(circuit, self, &replay_stream, factories);

                    circuit.cache_insert(DelayedTraceId::new(trace.stream_id()), delayed_trace);
                    circuit.cache_insert(ExportId::new(trace.stream_id()), export);

                    trace
                })
            })
            .clone()
    }

    /// Shard `self` across the specified range of workers and integrate the resulting stream.
    #[track_caller]
    pub fn dyn_shard_workers_accumulate_integrate_trace(
        &self,
        factories: &B::Factories,
        workers: Range<usize>,
    ) -> Stream<C, Spine<B>>
    where
        B: Batch<Time = ()>,
        Spine<B>: SizeOf,
    {
        self.circuit()
            .cache_get_or_insert_with(
                ShardedWorkersAccumulateTraceId::new((self.stream_id(), workers.clone())),
                || {
                    let circuit = self.circuit();
                    let bounds = self
                        .accumulate_trace_bounds_with_bound(TraceBound::new(), TraceBound::new());
                    let range_str = format!("{}-{}", workers.start, workers.end);

                    let persistent_id = self.get_persistent_id();

                    circuit.region(
                        &format!("shard_workers_accumulate_integrate_trace-{range_str}"),
                        || {
                            let z1: AccumulateZ1Trace<C, B, Spine<B>> = AccumulateZ1Trace::new(
                                factories,
                                factories,
                                true,
                                circuit.root_scope(),
                                bounds,
                            );

                            let accumulated = self
                                .dyn_shard_workers_accumulate(factories, workers.clone())
                                .into_enabled_stream();

                            let (
                                ExportStream {
                                    local: delayed_trace,
                                    export,
                                },
                                z1feedback,
                            ) = circuit.add_feedback_with_export_persistent(
                                persistent_id
                                    .map(|name| format!("{name}.shard_accintegral-{range_str}"))
                                    .as_deref(),
                                z1,
                            );

                            let replay_stream =
                                z1feedback.operator_mut().prepare_replay_stream(self);

                            let trace = circuit.add_binary_operator_with_preference(
                                AccumulateUntimedTraceAppend::<Spine<B>>::new(),
                                (&delayed_trace, OwnershipPreference::STRONGLY_PREFER_OWNED),
                                (&accumulated, OwnershipPreference::PREFER_OWNED),
                            );

                            z1feedback.connect_with_preference(
                                &trace,
                                OwnershipPreference::STRONGLY_PREFER_OWNED,
                            );

                            // Connect the replay stream to the original non-sharded stream, since the dyn_shard_accumulate operator
                            // doesn't expose a sharded stream.
                            // FIXME: this is suboptimal, since this will end up sharding the already sharded data again.
                            // We should instead replay by moving the entire integral into the accumulator. This will require
                            // integrating bootstrapping with transactions better. An additional complication is that the integral
                            // can be timed, while the accumulator stores untimed updates
                            register_replay_stream(circuit, self, &replay_stream, factories);

                            circuit.cache_insert(
                                DelayedTraceId::new(trace.stream_id()),
                                delayed_trace,
                            );
                            circuit.cache_insert(ExportId::new(trace.stream_id()), export);

                            trace
                        },
                    )
                },
            )
            .clone()
    }

    pub fn dyn_accumulate_integrate_trace_with_bound(
        &self,
        factories: &B::Factories,
        lower_key_bound: TraceBound<B::Key>,
        lower_val_bound: TraceBound<B::Val>,
    ) -> Stream<C, Spine<B>>
    where
        B: Batch<Time = ()>,
        Spine<B>: SizeOf,
    {
        self.accumulate_integrate_trace_inner(
            factories,
            self.accumulate_trace_bounds_with_bound(lower_key_bound, lower_val_bound),
        )
    }

    #[allow(clippy::type_complexity)]
    fn accumulate_integrate_trace_inner(
        &self,
        input_factories: &B::Factories,
        bounds: TraceBounds<B::Key, B::Val>,
    ) -> Stream<C, Spine<B>>
    where
        B: Batch<Time = ()>,
        Spine<B>: SizeOf,
    {
        self.circuit()
            .cache_get_or_insert_with(AccumulateTraceId::new(self.stream_id()), || {
                let circuit = self.circuit();
                let bounds = bounds.clone();

                let persistent_id = self.get_persistent_id();

                circuit.region("accumulate_integrate_trace", || {
                    let z1 = AccumulateZ1Trace::new(
                        input_factories,
                        input_factories,
                        true,
                        circuit.root_scope(),
                        bounds,
                    );

                    let accumulated = self.dyn_accumulate(input_factories).into_enabled_stream();

                    let (
                        ExportStream {
                            local: delayed_trace,
                            export,
                        },
                        z1feedback,
                    ) = circuit.add_feedback_with_export_persistent(
                        persistent_id
                            .map(|name| format!("{name}.accintegral"))
                            .as_deref(),
                        z1,
                    );

                    let replay_stream = z1feedback.operator_mut().prepare_replay_stream(self);

                    let trace = circuit.add_binary_operator_with_preference(
                        AccumulateUntimedTraceAppend::<Spine<B>>::new(),
                        (&delayed_trace, OwnershipPreference::STRONGLY_PREFER_OWNED),
                        (&accumulated, OwnershipPreference::PREFER_OWNED),
                    );

                    if self.is_sharded() {
                        delayed_trace.mark_sharded();
                        trace.mark_sharded();
                    }

                    z1feedback.connect_with_preference(
                        &trace,
                        OwnershipPreference::STRONGLY_PREFER_OWNED,
                    );

                    register_replay_stream(circuit, self, &replay_stream, input_factories);

                    circuit.cache_insert(DelayedTraceId::new(trace.stream_id()), delayed_trace);
                    circuit.cache_insert(ExportId::new(trace.stream_id()), export);

                    trace
                })
            })
            .clone()
    }
}

/// See [`trait AccumulateTraceFeedback`] documentation.
pub struct AccumulateTraceFeedbackConnector<C, T>
where
    C: Circuit,
    T: Trace,
{
    feedback: FeedbackConnector<C, T, T, AccumulateZ1Trace<C, T::Batch, T>>,
    /// `delayed_trace` stream in the diagram in
    /// [`trait AccumulateTraceFeedback`] documentation.
    pub delayed_trace: Stream<C, T>,
    export_trace: Stream<C::Parent, T>,
    bounds: TraceBounds<T::Key, T::Val>,
}

impl<C, T> AccumulateTraceFeedbackConnector<C, T>
where
    T: Trace<Time = ()> + Clone,
    C: Circuit,
{
    pub fn connect(
        self,
        stream: &Stream<C, T::Batch>,
        factories: &<T::Batch as BatchReader>::Factories,
    ) {
        let circuit = self.delayed_trace.circuit();
        let accumulated = stream.dyn_accumulate(factories).into_enabled_stream();

        let replay_stream = self.feedback.operator_mut().prepare_replay_stream(stream);

        let trace = circuit.add_binary_operator_with_preference(
            <AccumulateUntimedTraceAppend<T>>::new(),
            (
                &self.delayed_trace,
                OwnershipPreference::STRONGLY_PREFER_OWNED,
            ),
            (&accumulated, OwnershipPreference::PREFER_OWNED),
        );

        if stream.is_sharded() {
            self.delayed_trace.mark_sharded();
            trace.mark_sharded();
        }

        self.feedback
            .connect_with_preference(&trace, OwnershipPreference::STRONGLY_PREFER_OWNED);

        register_replay_stream(circuit, stream, &replay_stream, factories);

        circuit.cache_insert(
            DelayedTraceId::new(trace.stream_id()),
            self.delayed_trace.clone(),
        );

        circuit.cache_insert(AccumulateTraceId::new(stream.stream_id()), trace.clone());
        circuit.cache_insert(
            AccumulateBoundsId::<T::Batch>::new(stream.stream_id()),
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
/// [`AccumulateTraceFeedbackConnector`] struct.  The struct contains the `delayed_trace`
/// stream, which can be used as input to instantiate `F` and the `output`
/// stream.  Close the loop by calling
/// `AccumulateTraceFeedbackConnector::connect(output)`.
pub trait AccumulateTraceFeedback: Circuit {
    fn add_accumulate_integrate_trace_feedback<T>(
        &self,
        persistent_id: Option<&str>,
        factories: &T::Factories,
        bounds: TraceBounds<T::Key, T::Val>,
    ) -> AccumulateTraceFeedbackConnector<Self, T>
    where
        T: Trace<Time = ()> + Clone,
    {
        // We'll give `AccumulateZ1Trace` a real name inside `AccumulateTraceFeedbackConnector::connect`, where we have the name of the input stream.
        let (ExportStream { local, export }, feedback) = self.add_feedback_with_export_persistent(
            persistent_id
                .map(|name| format!("{name}.accintegral"))
                .as_deref(),
            AccumulateZ1Trace::new(
                factories,
                factories,
                true,
                self.root_scope(),
                bounds.clone(),
            ),
        );

        AccumulateTraceFeedbackConnector {
            feedback,
            delayed_trace: local,
            export_trace: export,
            bounds,
        }
    }
}

impl<C: Circuit> AccumulateTraceFeedback for C {}

impl<C, B> Stream<C, Spine<B>>
where
    C: Circuit,
    B: Batch,
{
    /// Returns the trace of `self` delayed by one clock cycle.
    pub fn accumulate_delay_trace(&self) -> Stream<C, SpineSnapshot<B>> {
        let circuit = self.circuit();

        // The delayed trace should be automatically created while the real trace is
        // created via `.accumulate_trace()` or a similar function
        // FIXME: Create a trace if it doesn't exist

        circuit.cache_get_or_insert_with(AccumulateDelayedTraceId::new(self.stream_id()), || {
                let delayed_trace = circuit.cache_get_or_insert_with(DelayedTraceId::new(self.stream_id()), || {
                    panic!("called `.accumulate_delay_trace()` on a stream without a previously created trace")
                });

                circuit.add_unary_operator(AccumulateDelayTrace::new(), delayed_trace.deref())
            })
            .deref()
            .clone()
    }
}

// TODO: This operator converts `Spine` to `SpineSnapshot` and then adds it to the trace batch by batch. The
// original spine gets discarded along with any ongoing merges. This wastes work performed by partial merges. To fix
// this, we need a way to merge spines along with ongoing merges.
pub struct AccumulateUntimedTraceAppend<T>
where
    T: Trace,
{
    // Total number of input tuples processed by the operator.
    num_inputs: usize,

    _phantom: PhantomData<T>,
}

impl<T> Default for AccumulateUntimedTraceAppend<T>
where
    T: Trace,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T> AccumulateUntimedTraceAppend<T>
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

impl<T> Operator for AccumulateUntimedTraceAppend<T>
where
    T: Trace + 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("AccumulateUntimedTraceAppend")
    }

    fn metadata(&self, meta: &mut OperatorMeta) {
        meta.extend(metadata! {
            INPUT_RECORDS_COUNT => MetaItem::Count(self.num_inputs),
        });
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<T> BinaryOperator<T, Option<Spine<T::Batch>>, T> for AccumulateUntimedTraceAppend<T>
where
    T: Trace + 'static,
{
    async fn eval(&mut self, _trace: &T, _delta: &Option<Spine<T::Batch>>) -> T {
        // Refuse to accept trace by reference.  This should not happen in a correctly
        // constructed circuit.
        panic!("AccumulateUntimedTraceAppend::eval(): cannot accept trace by reference")
    }

    async fn eval_owned_and_ref(&mut self, mut trace: T, delta: &Option<Spine<T::Batch>>) -> T {
        if let Some(delta) = delta {
            self.num_inputs += delta.len_upper_bound();
            for batch in delta.ro_snapshot().batches() {
                trace.insert(batch.clone()).await;
            }
        }
        trace
    }

    async fn eval_ref_and_owned(&mut self, _trace: &T, _delta: Option<Spine<T::Batch>>) -> T {
        // Refuse to accept trace by reference.  This should not happen in a correctly
        // constructed circuit.
        panic!(
            "AccumulateUntimedTraceAppend::eval_ref_and_owned(): cannot accept trace by reference"
        )
    }

    async fn eval_owned(&mut self, trace: T, delta: Option<Spine<T::Batch>>) -> T {
        self.eval_owned_and_ref(trace, &delta).await
    }

    fn input_preference(&self) -> (OwnershipPreference, OwnershipPreference) {
        (
            OwnershipPreference::PREFER_OWNED,
            OwnershipPreference::PREFER_OWNED,
        )
    }
}

pub struct AccumulateTraceAppend<T: Trace, B: BatchReader, C> {
    clock: C,
    output_factories: T::Factories,

    // Total number of input tuples processed by the operator.
    num_inputs: usize,

    _phantom: PhantomData<(T, B)>,
}

impl<T: Trace, B: BatchReader, C> AccumulateTraceAppend<T, B, C> {
    pub fn new(output_factories: &T::Factories, clock: C) -> Self {
        Self {
            clock,
            output_factories: output_factories.clone(),
            num_inputs: 0,
            _phantom: PhantomData,
        }
    }
}

impl<T, B, Clk> Operator for AccumulateTraceAppend<T, B, Clk>
where
    T: Trace,
    B: BatchReader,
    Clk: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("AccumulateTraceAppend")
    }
    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }

    fn metadata(&self, meta: &mut OperatorMeta) {
        meta.extend(metadata! {
            INPUT_RECORDS_COUNT => MetaItem::Count(self.num_inputs),
        });
    }
}

impl<T, B, Clk> BinaryOperator<T, Option<Spine<B>>, T> for AccumulateTraceAppend<T, B, Clk>
where
    B: Batch<Time = ()>,
    Clk: WithClock + 'static,
    T: Trace<Key = B::Key, Val = B::Val, R = B::R, Time = Clk::Time>,
{
    async fn eval(&mut self, _trace: &T, _delta: &Option<Spine<B>>) -> T {
        // Refuse to accept trace by reference.  This should not happen in a correctly
        // constructed circuit.
        unimplemented!()
    }

    async fn eval_owned_and_ref(&mut self, mut trace: T, delta: &Option<Spine<B>>) -> T {
        if let Some(delta) = delta {
            // TODO: extend `trace` type to feed untimed batches directly
            // (adding fixed timestamp on the fly).
            self.num_inputs += delta.len_upper_bound();
            for batch in delta.ro_snapshot().batches() {
                trace
                    .insert(T::Batch::from_arc_batch(
                        batch,
                        &self.clock.time(),
                        &self.output_factories,
                    ))
                    .await;
            }
        }
        trace
    }

    async fn eval_ref_and_owned(&mut self, _trace: &T, _delta: Option<Spine<B>>) -> T {
        // Refuse to accept trace by reference.  This should not happen in a correctly
        // constructed circuit.
        unimplemented!()
    }

    async fn eval_owned(&mut self, trace: T, delta: Option<Spine<B>>) -> T {
        self.eval_owned_and_ref(trace, &delta).await
    }

    fn input_preference(&self) -> (OwnershipPreference, OwnershipPreference) {
        (
            OwnershipPreference::PREFER_OWNED,
            OwnershipPreference::PREFER_OWNED,
        )
    }
}

/// The streams that an [`AccumulateZ1Trace`] replays its trace into, and the
/// replay that it has yet to play back.
enum Replay<C, B, T>
where
    B: Batch,
    T: Trace,
{
    /// Replays into the stream whose integral the trace stores, one chunk per
    /// step.
    Delta {
        stream: Stream<C, B>,
        pending: Option<ReplayState<T>>,
    },

    /// Replays into the accumulated form of that stream, which carries one
    /// spine per transaction: the whole trace becomes the transaction's spine.
    Accumulated {
        stream: Stream<C, Option<Spine<B>>>,
        pending: Option<T>,
    },

    /// Replays into both streams: the chunks into `delta`, as in `Delta`, and
    /// the whole trace into `accumulated`, as in `Accumulated`, in the step
    /// that replays the last chunk.  If no scheduled operator reads `delta`,
    /// the replay skips the chunks and acts as `Accumulated`.
    DeltaAndAccumulated {
        delta: Stream<C, B>,
        accumulated: Stream<C, Option<Spine<B>>>,
        pending: Option<ReplayState<T>>,
    },
}

impl<C, B, T> Replay<C, B, T>
where
    B: Batch,
    T: Trace,
{
    /// Returns true if a replay is waiting to be played back.
    fn is_pending(&self) -> bool {
        match self {
            Self::Delta { pending, .. } | Self::DeltaAndAccumulated { pending, .. } => {
                pending.is_some()
            }
            Self::Accumulated { pending, .. } => pending.is_some(),
        }
    }

    /// Schedules the contents of `trace` to be played back.
    ///
    /// # Arguments
    ///
    /// * `trace` - The trace to replay.
    fn start(&mut self, trace: T) {
        match self {
            Self::Delta { pending, .. } | Self::DeltaAndAccumulated { pending, .. } => {
                *pending = Some(ReplayState::create(trace))
            }
            Self::Accumulated { pending, .. } => *pending = Some(trace),
        }
    }

    /// Discards the replay that is waiting to be played back, if any.
    fn cancel(&mut self) {
        match self {
            Self::Delta { pending, .. } | Self::DeltaAndAccumulated { pending, .. } => {
                *pending = None
            }
            Self::Accumulated { pending, .. } => *pending = None,
        }
    }
}

/// Converts the trace of a top-level replay source into the spine type of
/// the accumulated stream that it replays into.
///
/// In the top-level circuit, where replay sources live, `T` is `Spine<B>`
/// (see [`AccumulateZ1Trace::check_replay_source_types`]), so this changes
/// only the static type.
///
/// # Arguments
///
/// * `trace` - The trace to convert.
///
/// # Returns
///
/// `trace` as a `Spine<B>`.
fn into_spine<B, T>(trace: T) -> Spine<B>
where
    B: Batch,
    T: Trace,
{
    *(Box::new(trace) as Box<dyn Any>)
        .downcast::<Spine<B>>()
        .expect("an accumulated replay requires a trace of type Spine<B>")
}

pub struct AccumulateZ1Trace<C: Circuit, B: Batch, T: Trace> {
    // For error reporting.
    local_id: NodeId,
    name: OperatorName,
    time: T::Time,
    trace: Option<T>,
    replay_mode: bool,
    trace_factories: T::Factories,
    // `dirty[scope]` is `true` iff at least one non-empty update was added to the trace
    // since the previous clock cycle at level `scope`.
    dirty: Vec<bool>,
    root_scope: Scope,
    reset_on_clock_start: bool,
    bounds: TraceBounds<T::Key, T::Val>,
    batch_factories: B::Factories,
    // The streams that this Z1 operator replays its trace into, if it is a
    // replay source.
    replay: Option<Replay<C, B, T>>,
    flush_output: bool,
    flush_input: bool,
}

impl<C, B, T> AccumulateZ1Trace<C, B, T>
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
            local_id: NodeId::root(),
            name: OperatorName::new("AccumulateZ1Trace"),
            time: <T::Time as Timestamp>::clock_start(),
            trace: None,
            replay_mode: false,
            trace_factories: trace_factories.clone(),
            batch_factories: batch_factories.clone(),
            dirty: vec![false; root_scope as usize + 1],
            root_scope,
            reset_on_clock_start,
            bounds,
            replay: None,
            flush_output: false,
            flush_input: false,
        }
    }

    /// Checks that a replay source's trace has the type of the spines that it
    /// replays.
    ///
    /// The synchronization replay of a concurrent bootstrap downcasts a
    /// recorder's `Spine<B>` to `T`, and an accumulated replay converts `T`
    /// into the `Spine<B>` of the accumulated stream.  In the top-level
    /// circuit, where replay sources live, the two types must coincide.
    fn check_replay_source_types() {
        if TypeId::of::<C::Time>() == TypeId::of::<()>() {
            assert_eq!(
                TypeId::of::<T>(),
                TypeId::of::<Spine<B>>(),
                "a top-level replay source's trace type must be Spine<B>"
            );
        }
    }

    /// Sets the streams that this integral replays into.
    ///
    /// # Arguments
    ///
    /// * `replay` - The replay streams, with nothing to play back yet.
    ///
    /// # Panics
    ///
    /// Panics if the integral already replays into a stream.
    fn set_replay(&mut self, replay: Replay<C, B, T>) {
        assert!(
            self.replay.is_none(),
            "AccumulateZ1Trace: the integral already replays into a stream"
        );
        self.replay = Some(replay);
    }

    /// Creates a stream that will be used to replay the contents of `stream`
    /// chunk-by-chunk.
    ///
    /// Given a circuit that implements an integral, the Z-1 operator can be used
    /// to replay the `delta` stream during bootstrapping.  This function sets this
    /// up at circuit construction time. It creates a new stream (`replay_stream`)
    /// that aliases `stream` internally.  In replay mode, Z-1 will send the contents
    /// of the integral to `replay_stream` chunk-by-chunk.
    ///
    ///
    /// ```text
    ///                               │
    ///                               │stream
    ///                               │
    ///                               │
    /// other consumers of stream◄────┤◄....................
    ///                               ▼                    .replay_stream(deltas)
    ///                         ┌───────────┐              .
    ///                         │Accumulator│              .
    ///                         └─────┬─────┘              .
    ///                               │                    .
    ///                               │                    .
    ///                               │                    .
    ///                               ▼                    .
    ///                     ┌─────────────────────┐      ┌─────────────────┐
    ///                     │AccumulateTraceAppend├─────►│AccumulateZ1Trace│
    ///                     └─────────────────────┘      └─────────────────┘
    ///                               ▲                    │
    ///                               │                    │
    ///                               └────────────────────┘
    /// ```
    ///
    /// This is a relatively expensive replay method, as it breaks up the integral into
    /// small chunks, and then reassembles them later. A more efficient method, implemented
    /// in [`Self::prepare_accumulated_replay_stream`], feeds the entire integral directly
    /// into `AccumulateTraceAppend`. This advantage of this approach however is that it can
    /// be used to feed operators that consume `stream` directly ("other consumers of the stream
    /// in the diagram"), e.g., linear operators like `Map`. Therefore this is currently the
    /// default method used by most operators.
    ///
    /// Note that at most one of `stream` or `replay_stream` can be active at a time.
    /// During normal operation, `stream` is active and `replay_stream` is not.  During
    /// replay, `replay_stream` is active while the operator that normally write to
    /// stream is disabled.
    ///
    /// To replay into both `stream` and its accumulated form, use
    /// [`Self::prepare_both_replay_streams`].
    ///
    /// # Panics
    ///
    /// Panics if the integral already replays into a stream.
    pub fn prepare_replay_stream(&mut self, stream: &Stream<C, B>) -> Stream<C, B> {
        Self::check_replay_source_types();

        let replay_stream =
            Stream::with_value(stream.circuit().clone(), self.local_id, stream.value());

        self.set_replay(Replay::Delta {
            stream: replay_stream.clone(),
            pending: None,
        });
        replay_stream
    }

    /// Creates a stream that will be used to replay the contents of `accumulated`
    /// in a single step.
    ///
    /// This is the counterpart of [`Self::prepare_replay_stream`] for an
    /// integral fed by an accumulator, whose output carries one spine per
    /// transaction.  In replay mode, Z-1 sends the whole integral to
    /// `replay_stream` as the spine of the transaction, in the step in which
    /// it is flushed, instead of chunk-by-chunk.  The replay moves the
    /// integral's batches without copying them, and the operators that
    /// normally feed `accumulated` (the accumulator and any exchange in front
    /// of it) need not participate in the replay.
    ///
    /// ```text
    ///                                │
    ///                                │stream
    ///                                │
    ///                                │
    ///  other consumers of stream◄────┤
    ///                                ▼
    ///                          ┌───────────┐
    ///                          │Accumulator│
    ///                          └─────┬─────┘
    ///                                │
    ///                                │◄....................
    ///                                │                    . replay_stream
    ///                                ▼                    .
    ///                      ┌─────────────────────┐      ┌─────────────────┐
    ///                      │AccumulateTraceAppend├─────►│AccumulateZ1Trace│
    ///                      └─────────────────────┘      └─────────────────┘
    ///                                ▲                    │
    ///                                │                    │
    ///                                └────────────────────┘
    /// ```
    ///
    /// This replay mechanism can be used to bootstrap operators that ingest the
    /// accumulated stream or its integral, but not the operators that consume the
    /// raw stream of changes `stream`.  To bootstrap those too, use
    /// [`Self::prepare_both_replay_streams`].
    ///
    /// # Arguments
    ///
    /// * `accumulated` - The stream that the accumulator feeding this integral
    ///   outputs.
    ///
    /// # Returns
    ///
    /// The replay stream, which aliases `accumulated`.
    ///
    /// # Panics
    ///
    /// Panics if the integral already replays into a stream.
    pub fn prepare_accumulated_replay_stream(
        &mut self,
        accumulated: &Stream<C, Option<Spine<B>>>,
    ) -> Stream<C, Option<Spine<B>>> {
        Self::check_replay_source_types();

        let replay_stream = Stream::with_value(
            accumulated.circuit().clone(),
            self.local_id,
            accumulated.value(),
        );

        self.set_replay(Replay::Accumulated {
            stream: replay_stream.clone(),
            pending: None,
        });
        replay_stream
    }

    /// Creates the streams that will be used to replay the contents of
    /// `stream` chunk-by-chunk and the contents of `accumulated` in a single
    /// step.
    ///
    /// Only used when `stream` contains the same contents as the integral,
    /// but doesn't feed the integral, so the integral cannot be rebuilt by
    /// simply replaying into `stream`:
    ///
    /// ```text
    ///                                │
    ///                                │stream
    ///                                │
    ///                                │
    ///  other consumers of stream◄────┤◄..........................
    ///                                                           .
    ///                                                           .
    ///                                                           .
    ///                          ┌───────────┐                    .
    ///                          │Accumulator│                    .
    ///                          └─────┬─────┘                    .
    ///                                │                          .
    ///                                │◄....................     .
    ///                                │     replay_stream2 .     . replay_stream1
    ///                                ▼                    .     .
    ///                      ┌─────────────────────┐      ┌─────────────────┐
    ///                      │AccumulateTraceAppend├─────►│AccumulateZ1Trace│
    ///                      └─────────────────────┘      └─────────────────┘
    ///                                ▲                    │
    ///                                │                    │
    ///                                └────────────────────┘
    /// ```
    ///
    /// This combines [`Self::prepare_replay_stream`] and
    /// [`Self::prepare_accumulated_replay_stream`] for an integral that
    /// `accumulated` feeds while other operators consume `stream` directly.
    /// In replay mode, Z-1 sends the integral to the first replay stream
    /// chunk-by-chunk, one chunk per step, and the whole integral to the
    /// second, in the step of the last chunk.  If no scheduled operator reads
    /// `stream` during the replay, Z-1 skips the chunks and sends the whole
    /// integral in the first step.
    ///
    /// # Arguments
    ///
    /// * `stream` - The stream of changes that this integral stores.
    /// * `accumulated` - The stream that feeds this integral: in each
    ///   transaction, the sum of that transaction's changes in `stream`.
    ///
    /// # Returns
    ///
    /// The two replay streams, which alias `stream` and `accumulated`.
    ///
    /// # Panics
    ///
    /// Panics if the integral already replays into a stream.
    pub fn prepare_both_replay_streams(
        &mut self,
        stream: &Stream<C, B>,
        accumulated: &Stream<C, Option<Spine<B>>>,
    ) -> (Stream<C, B>, Stream<C, Option<Spine<B>>>) {
        Self::check_replay_source_types();

        let delta_replay_stream =
            Stream::with_value(stream.circuit().clone(), self.local_id, stream.value());
        let accumulated_replay_stream = Stream::with_value(
            accumulated.circuit().clone(),
            self.local_id,
            accumulated.value(),
        );

        self.set_replay(Replay::DeltaAndAccumulated {
            delta: delta_replay_stream.clone(),
            accumulated: accumulated_replay_stream.clone(),
            pending: None,
        });
        (delta_replay_stream, accumulated_replay_stream)
    }
}

impl<C, B, T> Operator for AccumulateZ1Trace<C, B, T>
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
            self.trace = Some(T::new(
                &self.trace_factories,
                self.name.get(),
                TraceRole::Integral,
            ));
        }
    }

    fn clock_end(&mut self, scope: Scope) {
        if scope + 1 == self.root_scope
            && !self.reset_on_clock_start
            && let Some(tr) = self.trace.as_mut()
        {
            tr.set_frontier(&self.time.epoch_start(scope));
        }
        self.time = self.time.advance(scope + 1);
    }

    fn init(&mut self, global_id: &GlobalNodeId) {
        self.name.init(global_id);
        self.local_id = global_id.local_node_id().unwrap();
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
            STATE_RECORDS_COUNT => MetaItem::Count(total_size),
            ALLOCATED_MEMORY_BYTES => MetaItem::bytes(bytes.total_bytes()),
            USED_MEMORY_BYTES => MetaItem::bytes(bytes.used_bytes()),
            MEMORY_ALLOCATIONS_COUNT => MetaItem::Count(bytes.distinct_allocations()),
            SHARED_MEMORY_BYTES => MetaItem::bytes(bytes.shared_bytes()),
            RETAINMENT_BOUNDS => self.bounds.metadata()
        });

        if let Some(trace) = self.trace.as_ref() {
            trace.metadata(meta);
        }
    }

    fn fixedpoint(&self, scope: Scope) -> bool {
        !self.dirty[scope as usize] && self.is_replay_complete()
    }

    fn checkpoint(
        &mut self,
        persistent_id: Option<&str>,
    ) -> Result<Option<Box<dyn OperatorCheckpoint>>, Error> {
        let persistent_id = require_persistent_id(persistent_id, &self.name)?;
        self.trace
            .get_or_insert_with(|| {
                T::new(&self.trace_factories, self.name.get(), TraceRole::Integral)
            })
            .save(persistent_id)
            .map(Some)
    }

    fn restore(&mut self, base: &StoragePath, pid: Option<&str>) -> Result<(), Error> {
        let pid = require_persistent_id(pid, &self.name)?;

        self.trace
            .get_or_insert_with(|| {
                T::new(&self.trace_factories, self.name.get(), TraceRole::Integral)
            })
            .restore(base, pid)
    }

    fn clear_state(&mut self) -> Result<(), Error> {
        // println!("AccumulateZ1Trace-{}::clear_state", &self.global_id);
        self.trace = Some(T::new(
            &self.trace_factories,
            self.name.get(),
            TraceRole::Integral,
        ));
        if let Some(replay) = &mut self.replay {
            replay.cancel();
        }
        self.dirty = vec![false; self.root_scope as usize + 1];

        Ok(())
    }

    fn start_replay(&mut self) -> Result<(), Error> {
        // The pending-replay check is necessary if `start_replay` is called twice, for the
        // input and output halves of Z1.
        self.replay_mode = true;
        if let Some(replay) = &mut self.replay
            && !replay.is_pending()
        {
            let trace = self
                .trace
                .take()
                .expect("AccumulateZ1Trace::start_replay: no trace");
            self.trace = Some(T::new(
                &self.trace_factories,
                self.name.get(),
                TraceRole::Integral,
            ));

            replay.start(trace);
        }

        Ok(())
    }

    fn is_replay_complete(&self) -> bool {
        self.replay
            .as_ref()
            .is_none_or(|replay| !replay.is_pending())
    }

    fn end_replay(&mut self) -> Result<(), Error> {
        //println!("AccumulateZ1Trace-{}::end_replay", &self.global_id);

        self.replay_mode = false;
        if let Some(replay) = &mut self.replay {
            replay.cancel();
        }

        Ok(())
    }

    fn start_sync_replay(&mut self, trace: Box<dyn Any>) -> Result<(), Error> {
        let trace = *trace
            .downcast::<T>()
            .expect("start_sync_replay: trace type mismatch");

        // Sync replay reuses the replay machinery of the bootstrap replay:
        // the operator must still be in replay mode (between the bootstrap
        // transaction and the cutover), with the previous replay drained.
        assert!(
            self.replay_mode && self.is_replay_complete(),
            "start_sync_replay: the operator is not ready for a sync replay"
        );
        self.replay
            .as_mut()
            .expect("start_sync_replay: the operator has no replay stream")
            .start(trace);

        Ok(())
    }

    fn swap_state(&mut self, other: &mut Self) -> Result<(), Error> {
        // Move the trace contents and the operator's clock (`time`/`dirty`)
        // from the bootstrap copy.  The clock must travel with the trace: a
        // nested circuit's clock is realigned wholesale at cutover (see
        // `ChildNode::swap_state_with`), so the per-operator clock here must
        // match the trace's timestamps, which are anchored to the bootstrap
        // copy's transactions.
        std::mem::swap(&mut self.trace, &mut other.trace);
        std::mem::swap(&mut self.time, &mut other.time);
        std::mem::swap(&mut self.dirty, &mut other.dirty);
        Ok(())
    }

    fn flush(&mut self) {
        self.flush_output = true;
    }

    fn is_flush_complete(&self) -> bool {
        !self.flush_output
    }

    fn start_compaction(&mut self) {
        if let Some(trace) = self.trace.as_mut() {
            trace.initiate_compaction()
        }
    }

    fn is_compaction_complete(&self) -> bool {
        self.trace
            .as_ref()
            .is_none_or(|t| t.is_compaction_complete())
    }
}

impl<C, B, T> StrictOperator<T> for AccumulateZ1Trace<C, B, T>
where
    C: Circuit,
    B: Batch<Key = T::Key, Val = T::Val, Time = (), R = T::R>,
    T: Trace,
{
    fn get_output(&mut self) -> T {
        //println!("Z1-{}::get_output", &self.global_id);
        let replay_step_size = splitter_output_chunk_size();

        if self.replay_mode {
            match self
                .replay
                .as_mut()
                .expect("AccumulateZ1Trace: replay mode without a replay stream")
            {
                Replay::Delta { stream, pending } => {
                    // One output per transaction.
                    if self.flush_output
                        && let Some(replay) = pending
                    {
                        //println!("Z1-{}::get_output: replaying", &self.global_id);
                        let batch: B = replay.next_chunk(&self.batch_factories, replay_step_size);
                        stream.value().put(batch);
                        if replay.is_exhausted() {
                            *pending = None;
                            self.flush_output = false;
                        }
                    } else {
                        // Continue producing empty outputs as long as the circuit is in the replay mode.
                        stream.value().put(B::dyn_empty(&self.batch_factories));
                        self.flush_output = false;
                    }
                }
                Replay::Accumulated { stream, pending } => {
                    // Like the accumulator that normally feeds `stream`, output a
                    // spine only when flushed, once per transaction.  Its consumers
                    // expect one even when there is nothing to replay.
                    let output = if self.flush_output {
                        self.flush_output = false;
                        Some(match pending.take() {
                            Some(trace) => into_spine(trace),
                            None => Spine::new(
                                &self.batch_factories,
                                self.name.get(),
                                TraceRole::Accumulator,
                            ),
                        })
                    } else {
                        None
                    };
                    stream.value().put(output);
                }
                Replay::DeltaAndAccumulated {
                    delta,
                    accumulated,
                    pending,
                } => {
                    // A flushed transaction replays one chunk per step into
                    // `delta`, as in `Delta`, and one spine into `accumulated`,
                    // as in `Accumulated`: the whole trace, in the step of the
                    // last chunk.  A stream that no scheduled operator reads
                    // drops what it gets, so without a reader of `delta` the
                    // whole trace goes out in the first step, unchunked.
                    if self.flush_output
                        && let Some(replay) = pending
                    {
                        let chunked = delta.num_consumers() > 0;
                        delta.value().put(if chunked {
                            replay.next_chunk(&self.batch_factories, replay_step_size)
                        } else {
                            B::dyn_empty(&self.batch_factories)
                        });
                        let output = if !chunked || replay.is_exhausted() {
                            self.flush_output = false;
                            pending.take().map(|replay| into_spine(replay.into_trace()))
                        } else {
                            None
                        };
                        accumulated.value().put(output);
                    } else {
                        delta.value().put(B::dyn_empty(&self.batch_factories));
                        let output = self.flush_output.then(|| {
                            Spine::new(
                                &self.batch_factories,
                                self.name.get(),
                                TraceRole::Accumulator,
                            )
                        });
                        accumulated.value().put(output);
                        self.flush_output = false;
                    }
                }
            }
        } else {
            self.flush_output = false;
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

impl<C, B, T> StrictUnaryOperator<T, T> for AccumulateZ1Trace<C, B, T>
where
    C: Circuit,
    B: Batch<Key = T::Key, Val = T::Val, Time = (), R = T::R>,
    T: Trace,
{
    fn flush_input(&mut self) {
        self.flush_input = true;
    }

    fn is_flush_input_complete(&self) -> bool {
        !self.flush_input
    }

    async fn eval_strict(&mut self, _i: &T) {
        unimplemented!()
    }

    async fn eval_strict_owned(&mut self, mut i: T) {
        // println!("Z1-{}::eval_strict_owned", &self.global_id);

        if self.flush_input {
            self.flush_input = false;
            self.time = self.time.advance(0);
        }

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

use crate::circuit::operator_traits::{OperatorCheckpoint, OperatorName, UnaryOperator};

pub struct AccumulateDelayTrace<B>
where
    B: Batch,
{
    spine: Option<SpineSnapshot<B>>,
    flush: bool,
}

impl<B> AccumulateDelayTrace<B>
where
    B: Batch,
{
    pub const fn new() -> Self {
        Self {
            spine: None,
            flush: false,
        }
    }
}

impl<B> Operator for AccumulateDelayTrace<B>
where
    B: Batch,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("AccumulateDelayTrace")
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }

    fn flush(&mut self) {
        self.flush = true;
    }
}

impl<B> UnaryOperator<Spine<B>, SpineSnapshot<B>> for AccumulateDelayTrace<B>
where
    B: Batch,
{
    async fn eval(&mut self, spine: &Spine<B>) -> SpineSnapshot<B> {
        if self.flush || self.spine.is_none() {
            self.flush = false;
            self.spine = Some(spine.ro_snapshot());
        }

        self.spine.as_ref().unwrap().clone()
    }
}

#[cfg(test)]
mod replay_test {
    use super::AccumulateZ1Trace;
    use crate::{
        DynZWeight, RootCircuit, Runtime, ZWeight,
        algebra::{OrdZSet, OrdZSetFactories},
        circuit::{
            CircuitBase, CircuitConfig, CircuitHandle, NodeId, Stream,
            circuit_builder::{RefStreamValue, StreamMetadata},
            operator_traits::{Operator, StrictOperator},
            splitter_output_chunk_size,
        },
        dynamic::{DowncastTrait, DynData, DynUnit},
        operator::dynamic::trace::TraceBounds,
        trace::{
            Batch, BatchReader, BatchReaderFactories, Spine, Trace, TraceRole,
            test::{test_batch::batch_to_tuples, zset_tuples},
        },
        utils::Tup2,
    };
    use feldera_storage::tokio::TOKIO;
    use std::{collections::BTreeMap, ops::Range, sync::Arc};

    type ZSet = OrdZSet<DynData>;
    type Z1 = AccumulateZ1Trace<RootCircuit, ZSet, Spine<ZSet>>;
    type DeltaStream = Stream<RootCircuit, ZSet>;
    type AccumulatedStream = Stream<RootCircuit, Option<Spine<ZSet>>>;

    /// Keys mapped to their weights.
    type Contents = BTreeMap<i32, ZWeight>;

    /// Reads the keys of a batch or a spine of `i32` keys.
    ///
    /// # Arguments
    ///
    /// * `batch` - The batch or spine to read.
    ///
    /// # Returns
    ///
    /// Each key with the sum of its weights.
    fn contents<R>(batch: &R) -> Contents
    where
        R: BatchReader<Key = DynData, Val = DynUnit, Time = (), R = DynZWeight>,
    {
        let mut contents = Contents::new();
        for ((key, _val, ()), weight) in batch_to_tuples(batch) {
            *contents.entry(*key.downcast_checked::<i32>()).or_default() +=
                *weight.downcast_checked::<ZWeight>();
        }
        contents
    }

    /// Builds a spine that holds each of `keys` with weight 1.
    ///
    /// # Arguments
    ///
    /// * `factories` - Factories for the spine's batches.
    /// * `keys` - The keys to insert.
    ///
    /// # Returns
    ///
    /// The spine.
    fn spine(factories: &OrdZSetFactories<DynData>, keys: Range<i32>) -> Spine<ZSet> {
        let mut tuples = zset_tuples(keys.map(|key| Tup2(key, 1)).collect());
        let mut spine = Spine::new(
            factories,
            Arc::new(String::from("test")),
            TraceRole::Integral,
        );
        TOKIO.block_on(spine.insert(ZSet::dyn_from_tuples(factories, (), &mut tuples)));
        spine
    }

    /// Creates a Z1 that replays into a new stream and its accumulated form.
    /// Neither stream has a consumer yet.
    ///
    /// # Arguments
    ///
    /// * `factories` - Factories for the Z1's batches and trace.
    ///
    /// # Returns
    ///
    /// The circuit that owns the streams, the Z1, the stream, and its
    /// accumulated form.
    fn replay_source(
        factories: &OrdZSetFactories<DynData>,
    ) -> (CircuitHandle, Z1, DeltaStream, AccumulatedStream) {
        let (circuit, (delta, accumulated, root_scope)) = RootCircuit::build(|circuit| {
            Ok((
                DeltaStream::with_value(circuit.clone(), NodeId::new(0), RefStreamValue::empty()),
                AccumulatedStream::with_value(
                    circuit.clone(),
                    NodeId::new(1),
                    RefStreamValue::empty(),
                ),
                circuit.root_scope(),
            ))
        })
        .unwrap();

        let mut z1 = Z1::new(
            factories,
            factories,
            false,
            root_scope,
            TraceBounds::unbounded(),
        );
        z1.prepare_both_replay_streams(&delta, &accumulated);
        (circuit, z1, delta, accumulated)
    }

    /// Runs the output half of `z1` for one step.
    ///
    /// # Arguments
    ///
    /// * `z1` - The operator.
    /// * `delta` - The stream that `z1` replays chunks into.
    /// * `accumulated` - The stream that `z1` replays its whole trace into.
    ///
    /// # Returns
    ///
    /// The chunk written to `delta`, empty if `delta` has no consumer, and the
    /// spine written to `accumulated` if there is one.
    fn step(
        z1: &mut Z1,
        delta: &DeltaStream,
        accumulated: &AccumulatedStream,
    ) -> (Contents, Option<Contents>) {
        // In a circuit, the input half gives the integral back after each step.
        let integral = z1.get_output();
        z1.trace = Some(integral);

        // A stream without consumers keeps nothing.
        let mut chunk = Contents::new();
        if delta.num_consumers() > 0 {
            chunk = delta.map_value(contents);
            delta.consume_token();
        }
        let spine = accumulated.map_value(|spine| spine.as_ref().map(contents));
        accumulated.consume_token();
        (chunk, spine)
    }

    /// Checks that a Z1 that replays into both a stream and its accumulated
    /// form writes its trace to the first in chunks and, in the step of the
    /// last chunk, to the second as a whole.
    #[test]
    fn replay_into_delta_and_accumulated() {
        // Spines need a DBSP runtime.
        let config = CircuitConfig::with_workers(1).with_splitter_chunk_size_records(2);
        Runtime::run(config, |_parker| {
            let factories = OrdZSetFactories::<DynData>::new::<i32, (), ZWeight>();
            let chunk_size = splitter_output_chunk_size() as i32;
            // Two full chunks and one with a single key.
            let num_keys = 2 * chunk_size + 1;
            let trace: Contents = (0..num_keys).map(|key| (key, 1)).collect();
            let recording: Contents = (0..chunk_size).map(|key| (key, 1)).collect();

            let (_circuit, mut z1, delta, accumulated) = replay_source(&factories);
            delta.register_consumer();
            accumulated.register_consumer();

            // The bootstrap replay plays back the checkpointed trace.
            z1.trace = Some(spine(&factories, 0..num_keys));
            z1.start_replay().unwrap();
            z1.flush();
            let mut chunks = Contents::new();
            for last in [false, false, true] {
                assert!(!z1.is_flush_complete());
                let (chunk, whole) = step(&mut z1, &delta, &accumulated);
                assert!(!chunk.is_empty());
                chunks.extend(chunk);
                assert_eq!(whole, last.then(|| trace.clone()));
            }
            assert!(z1.is_flush_complete());
            assert_eq!(chunks, trace);

            // A step outside a commit replays nothing.
            assert_eq!(step(&mut z1, &delta, &accumulated), (Contents::new(), None));

            // The synchronization replay plays back a recording that fills
            // one chunk, so the chunk and the whole recording share a step.
            z1.start_sync_replay(Box::new(spine(&factories, 0..chunk_size)))
                .unwrap();
            z1.flush();
            assert_eq!(
                step(&mut z1, &delta, &accumulated),
                (recording.clone(), Some(recording.clone()))
            );
            assert!(z1.is_flush_complete());

            // A commit with nothing to replay still gets its one spine.
            z1.flush();
            assert_eq!(
                step(&mut z1, &delta, &accumulated),
                (Contents::new(), Some(Contents::new()))
            );
        })
        .unwrap()
        .join()
        .unwrap();
    }

    /// Checks that a Z1 that replays into both a stream and its accumulated
    /// form skips the chunks when no scheduled operator reads the stream, and
    /// replays the whole trace in the first step of the commit.
    #[test]
    fn replay_without_a_delta_reader() {
        // Spines need a DBSP runtime.
        let config = CircuitConfig::with_workers(1).with_splitter_chunk_size_records(2);
        Runtime::run(config, |_parker| {
            let factories = OrdZSetFactories::<DynData>::new::<i32, (), ZWeight>();
            // Enough keys for three chunks.
            let keys = 0..2 * splitter_output_chunk_size() as i32 + 1;
            let trace: Contents = keys.clone().map(|key| (key, 1)).collect();

            let (_circuit, mut z1, delta, accumulated) = replay_source(&factories);
            accumulated.register_consumer();

            // The bootstrap replay plays back the checkpointed trace.
            z1.trace = Some(spine(&factories, keys.clone()));
            z1.start_replay().unwrap();
            z1.flush();
            assert_eq!(
                step(&mut z1, &delta, &accumulated),
                (Contents::new(), Some(trace.clone()))
            );
            assert!(z1.is_flush_complete());

            // So does the synchronization replay with a recording.
            z1.start_sync_replay(Box::new(spine(&factories, keys)))
                .unwrap();
            z1.flush();
            assert_eq!(
                step(&mut z1, &delta, &accumulated),
                (Contents::new(), Some(trace))
            );
            assert!(z1.is_flush_complete());
        })
        .unwrap()
        .join()
        .unwrap();
    }

    /// Checks that an integral gets its replay streams only once.
    #[test]
    #[should_panic(expected = "the integral already replays into a stream")]
    fn prepare_replay_streams_twice() {
        let factories = OrdZSetFactories::<DynData>::new::<i32, (), ZWeight>();
        let (_circuit, mut z1, delta, _accumulated) = replay_source(&factories);
        z1.prepare_replay_stream(&delta);
    }
}

#[cfg(test)]
mod tests {
    use crate::Runtime;

    /// Checks that integrating a stream over the same workers twice returns one
    /// integral, so that the circuit builds without a duplicate persistent id,
    /// and that integrating it over other workers returns another integral.
    #[test]
    fn shard_workers_integral_is_shared() {
        let (_dbsp, (first, second, other)) = Runtime::init_circuit(4, |circuit| {
            let (stream, _input_handle) = circuit.add_input_zset::<i32>();
            stream.set_persistent_id(Some("stream"));
            let integral = |workers| {
                stream
                    .shard_workers_accumulate_integrate_trace(workers)
                    .stream_id()
            };
            Ok((integral(2..4), integral(2..4), integral(0..2)))
        })
        .unwrap();
        assert_eq!(first, second);
        assert_ne!(first, other);
    }
}

// #[cfg(test)]
// mod test {
//     use std::cmp::max;

//     use crate::{dynamic::DynData, utils::Tup2, Runtime, Stream, TypedBox, ZWeight};
//     use proptest::{collection::vec, prelude::*};
//     use size_of::SizeOf;

//     fn quasi_monotone_batches(
//         key_window_size: i32,
//         key_window_step: i32,
//         val_window_size: i32,
//         val_window_step: i32,
//         max_tuples: usize,
//         batches: usize,
//     ) -> impl Strategy<Value = Vec<Vec<((i32, i32), ZWeight)>>> {
//         (0..batches)
//             .map(|i| {
//                 vec(
//                     (
//                         (
//                             i as i32 * key_window_step
//                                 ..i as i32 * key_window_step + key_window_size,
//                             i as i32 * val_window_step
//                                 ..i as i32 * val_window_step + val_window_size,
//                         ),
//                         1..2i64,
//                     ),
//                     0..max_tuples,
//                 )
//             })
//             .collect::<Vec<_>>()
//     }

//     proptest! {
//         #![proptest_config(ProptestConfig::with_cases(16))]
//         #[test]
//         #[ignore = "this test can be flaky especially on aarch64"]
//         fn test_integrate_trace_retain(batches in quasi_monotone_batches(100, 20, 1000, 200, 100, 200)) {
//             let (mut dbsp, input_handle) = Runtime::init_circuit(4, move |circuit| {
//                 let (stream, handle) = circuit.add_input_indexed_zset::<i32, i32>();
//                 let stream = stream.shard();
//                 let watermark: Stream<_, TypedBox<(i32, i32), DynData>> = stream
//                     .waterline(
//                         || (i32::MIN, i32::MIN),
//                         |k, v| (*k, *v),
//                         |(ts1_left, ts2_left), (ts1_right, ts2_right)| {
//                             (max(*ts1_left, *ts1_right), max(*ts2_left, *ts2_right))
//                         },
//                     );

//                 let trace = stream.integrate_trace();
//                 stream.integrate_trace_retain_keys(&watermark, |key, ts| *key >= ts.0.saturating_sub(100));
//                 trace.apply(|trace| {
//                     // println!("retain_keys: {}bytes", trace.size_of().total_bytes());
//                     assert!(trace.size_of().total_bytes() < 100_000);
//                 });

//                 let stream2 = stream.map_index(|(k, v)| (*k, *v)).shard();

//                 let trace2 = stream2.integrate_trace();
//                 stream2.integrate_trace_retain_values(&watermark, |val, ts| *val >= ts.1.saturating_sub(1000));

//                 trace2.apply(|trace| {
//                     // println!("retain_vals: {}bytes", trace.size_of().total_bytes());
//                     assert!(trace.size_of().total_bytes() < 100_000);
//                 });

//                 Ok(handle)
//             })
//             .unwrap();

//             for batch in batches {
//                 let mut tuples = batch.into_iter().map(|((k, v), r)| Tup2(k, Tup2(v, r))).collect::<Vec<_>>();
//                 input_handle.append(&mut tuples);
//                 dbsp.step().unwrap();
//             }
//         }
//     }
// }
