//! Recorder operator: accumulates the deltas that flow on a stream across
//! many transactions, for later replay.
//!
//! Concurrent bootstrapping runs two copies of a circuit: copy 1 keeps
//! evaluating pre-existing views while copy 2 backfills new and modified
//! views from checkpointed integrals.  The integrals replayed by copy 2 are
//! frozen at the checkpoint, so the deltas that copy 1 applies to the
//! *boundary streams* (the streams whose integrals copy 2 replays) while the
//! backfill runs must be captured and replayed into copy 2 afterward, in the
//! synchronization transaction.  The recorder is the operator that captures
//! them.
//!
//! A recorder is attached, disabled, to every stream registered as a replay
//! source (see `register_replay_stream` in `circuit_builder`), because any
//! of them can turn out to be a boundary stream when a checkpoint is
//! restored into a modified circuit.  Building recorders unconditionally
//! at construction time keeps circuit construction — and therefore
//! [`Runtime::sequence_next`](crate::circuit::Runtime::sequence_next)-based
//! exchange-id allocation — identical across workers.  A disabled recorder
//! holds no state and its evaluation is a no-op; in particular it does not
//! allocate a [`Spine`] (whose background merge tasks are costly to keep
//! idle).
//!
//! The recorder is a pure sink: it emits no output stream.  Its contents are
//! extracted directly through a [`RecorderHandle`], stored in the circuit
//! cache under [`RecorderId`], by the bootstrap orchestration code that runs
//! between circuit steps.

use std::{any::Any, borrow::Cow, cell::RefCell, panic::Location, rc::Rc, sync::Arc};

use crate::{
    Error, NumEntries, Scope,
    circuit::{
        GlobalNodeId,
        circuit_builder::StreamId,
        metadata::{
            ALLOCATED_MEMORY_BYTES, MetaItem, OperatorLocation, OperatorMeta, STATE_RECORDS_COUNT,
            USED_MEMORY_BYTES,
        },
        operator_traits::{Operator, OperatorName, SinkOperator},
    },
    circuit_cache_key,
    trace::{Batch, Spine, Trace},
};
use size_of::SizeOf;

circuit_cache_key!(RecorderId<B: Batch>(StreamId => RecorderHandle<B>));

// Type-erased access to the same recorders, for bootstrap orchestration code
// that works with type-erased streams.  Registered alongside `RecorderId` in
// `register_replay_stream`.
circuit_cache_key!(RecorderControlId(StreamId => Rc<dyn RecorderControl>));

/// Type-erased control interface to a [`RecorderHandle`].
///
/// Bootstrap orchestration operates on type-erased nodes and streams, so it
/// reaches recorders through this trait (cached under [`RecorderControlId`])
/// rather than through the typed [`RecorderHandle`].
pub trait RecorderControl {
    /// See [`RecorderHandle::start_recording`].
    fn start_recording(&self);

    /// See [`RecorderHandle::is_recording`].
    fn is_recording(&self) -> bool;

    /// Type-erased [`RecorderHandle::stop_recording`]: the box holds the
    /// recorded `Spine<B>`.  The consumer (the replay source of the same
    /// stream in the bootstrap circuit) recovers the type by downcasting.
    fn stop_recording_any(&self) -> Option<Box<dyn Any>>;
}

impl<B> RecorderControl for RecorderHandle<B>
where
    B: Batch,
{
    fn start_recording(&self) {
        RecorderHandle::start_recording(self)
    }

    fn is_recording(&self) -> bool {
        RecorderHandle::is_recording(self)
    }

    fn stop_recording_any(&self) -> Option<Box<dyn Any>> {
        self.stop_recording()
            .map(|spine| Box::new(spine) as Box<dyn Any>)
    }
}

/// Recording state of a [`Recorder`].
///
/// The recording flag is independent of the spine's presence so that the
/// spine can be temporarily checked out of the shared state during
/// evaluation: if an evaluation is cancelled mid-insert (dropping the spine
/// with it), the recorder remains in the `Recording` state with no trace,
/// which [`RecorderHandle::stop_recording`] reports as an error instead of
/// silently behaving as if recording never started.
enum RecordingState<B>
where
    B: Batch,
{
    /// Not recording.
    Disabled,
    /// Recording.  The spine accumulates every non-empty batch that flows on
    /// the recorded stream.  `None` while the spine is checked out by
    /// [`Recorder::eval_owned`].
    Recording(Option<Spine<B>>),
}

struct RecorderInner<B>
where
    B: Batch,
{
    factories: B::Factories,
    name: Arc<String>,
    state: RecordingState<B>,
}

/// Control handle for a [`Recorder`] operator.
///
/// The handle is shared between the operator and the circuit cache
/// ([`RecorderId`]), so that code outside the circuit graph can start and
/// stop recording and extract the recorded contents.
///
/// The handle must be used only between circuit steps: the operator's
/// evaluation temporarily takes the trace out of the shared state, so calls
/// concurrent with evaluation would observe a recorder that appears stopped.
pub struct RecorderHandle<B>(Rc<RefCell<RecorderInner<B>>>)
where
    B: Batch;

impl<B> Clone for RecorderHandle<B>
where
    B: Batch,
{
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<B> RecorderHandle<B>
where
    B: Batch,
{
    /// Starts recording.  No-op if the recorder is already recording (the
    /// recorded contents are kept).
    ///
    /// Must be called on the worker thread that owns the recorder's circuit:
    /// the spine allocated here is associated with the calling thread's
    /// worker.
    pub fn start_recording(&self) {
        let mut inner = self.0.borrow_mut();
        if matches!(inner.state, RecordingState::Disabled) {
            inner.state =
                RecordingState::Recording(Some(Spine::new(&inner.factories, inner.name.clone())));
        }
    }

    /// Stops recording and returns the recorded contents, or `None` if the
    /// recorder was not recording.  The recorder is left empty and disabled.
    ///
    /// # Panics
    ///
    /// Panics if the recorded contents were lost to a cancelled evaluation
    /// (see [`RecordingState`]): recording silently losing data would
    /// corrupt the bootstrap synchronization that consumes it.
    pub fn stop_recording(&self) -> Option<Spine<B>> {
        let mut inner = self.0.borrow_mut();
        match std::mem::replace(&mut inner.state, RecordingState::Disabled) {
            RecordingState::Disabled => None,
            RecordingState::Recording(Some(trace)) => Some(trace),
            RecordingState::Recording(None) => {
                panic!("recorder lost its contents: an evaluation was cancelled mid-insert")
            }
        }
    }

    /// True while the recorder is recording.
    pub fn is_recording(&self) -> bool {
        matches!(self.0.borrow().state, RecordingState::Recording(_))
    }

    fn take_trace(&self) -> Option<Spine<B>> {
        match &mut self.0.borrow_mut().state {
            RecordingState::Disabled | RecordingState::Recording(None) => None,
            RecordingState::Recording(trace @ Some(_)) => trace.take(),
        }
    }

    fn put_trace(&self, trace: Spine<B>) {
        let mut inner = self.0.borrow_mut();
        debug_assert!(matches!(inner.state, RecordingState::Recording(None)));
        inner.state = RecordingState::Recording(Some(trace));
    }
}

/// Sink operator that records the deltas flowing on a stream while enabled
/// through its [`RecorderHandle`].  See the module documentation.
pub struct Recorder<B>
where
    B: Batch,
{
    name: OperatorName,
    location: &'static Location<'static>,
    state: RecorderHandle<B>,
}

impl<B> Recorder<B>
where
    B: Batch,
{
    pub fn new(factories: &B::Factories, location: &'static Location<'static>) -> Self {
        let name = OperatorName::new("Recorder");
        Self {
            state: RecorderHandle(Rc::new(RefCell::new(RecorderInner {
                factories: factories.clone(),
                name: name.get(),
                state: RecordingState::Disabled,
            }))),
            name,
            location,
        }
    }

    /// Returns the handle that controls this recorder.
    pub fn handle(&self) -> RecorderHandle<B> {
        self.state.clone()
    }
}

impl<B> Operator for Recorder<B>
where
    B: Batch,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("Recorder")
    }

    fn init(&mut self, global_id: &GlobalNodeId) {
        self.name.init(global_id);
        self.state.0.borrow_mut().name = self.name.get();
    }

    fn location(&self) -> OperatorLocation {
        Some(self.location)
    }

    fn metadata(&self, meta: &mut OperatorMeta) {
        let inner = self.state.0.borrow();
        let trace = match &inner.state {
            RecordingState::Disabled | RecordingState::Recording(None) => None,
            RecordingState::Recording(Some(trace)) => Some(trace),
        };
        let total_size = trace.map(|trace| trace.num_entries_deep()).unwrap_or(0);
        let bytes = trace.map(|trace| trace.size_of()).unwrap_or_default();

        meta.extend(metadata! {
            STATE_RECORDS_COUNT => MetaItem::Count(total_size),
            ALLOCATED_MEMORY_BYTES => MetaItem::bytes(bytes.total_bytes()),
            USED_MEMORY_BYTES => MetaItem::bytes(bytes.used_bytes()),
        });

        if let Some(trace) = trace {
            trace.metadata(meta);
        }
    }

    /// The recorded contents are control-plane state consumed by the
    /// bootstrap orchestration, not part of the circuit's fixedpoint
    /// computation, so a recording recorder does not hold the circuit back
    /// from a fixedpoint.
    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }

    fn clear_state(&mut self) -> Result<(), Error> {
        self.state.0.borrow_mut().state = RecordingState::Disabled;
        Ok(())
    }

    // `checkpoint` and `restore` deliberately keep their default no-op
    // implementations: a recorder must not write checkpoint files.  Restore
    // detects operators that need backfill by a missing checkpoint file
    // (`CircuitHandle::restore` treats `NotFound` as "new operator"), so a
    // recorder that checkpointed state would spuriously seed the backfill
    // set on every restore from a checkpoint taken before the recorder
    // existed.  Recorder contents never need to survive a restart: a crash
    // during bootstrapping restarts the bootstrap from the checkpoint.
    //
    // A recorder can still end up in the backfill set indirectly: balancer
    // invalidation propagates `need_backfill` forward to all transitive
    // successors, which includes recorders (they consume the streams they
    // record).  This is benign — the recorder is empty and disabled at
    // restore time, so the `clear_state` call it receives is a no-op — but
    // it does mean recorder nodes can appear in `BootstrapInfo`.
}

impl<B> SinkOperator<B> for Recorder<B>
where
    B: Batch,
{
    async fn eval(&mut self, batch: &B) {
        // Check the recording flag before cloning: a disabled recorder must
        // not copy its input.
        if batch.is_empty() || !self.state.is_recording() {
            return;
        }
        self.eval_owned(batch.clone()).await
    }

    async fn eval_owned(&mut self, batch: B) {
        if batch.is_empty() {
            return;
        }
        // Take the trace out of the shared state instead of holding a
        // `RefCell` borrow across the `await` below (`insert` can block on
        // spine backpressure).  The handle is only used between circuit
        // steps, so the trace's temporary absence is unobservable.
        let Some(mut trace) = self.state.take_trace() else {
            return;
        };
        trace.insert(batch).await;
        self.state.put_trace(trace);
    }
}

#[cfg(test)]
mod test {
    use super::{Recorder, RecorderId};
    use crate::{
        Circuit, RootCircuit, Runtime, ZWeight,
        algebra::OrdZSet,
        circuit::{
            CircuitConfig, OwnershipPreference, circuit_builder::CircuitBase,
            operator_traits::Operator, schedule::util::ownership_constraints,
        },
        dynamic::{DowncastTrait, DynData},
        trace::{BatchReaderFactories, Trace, test::test_batch::batch_to_tuples},
    };
    use std::panic::Location;

    /// Returns `batch`'s contents as `(key, weight)` pairs.
    fn zset_contents(batch: &OrdZSet<DynData>) -> Vec<(u64, ZWeight)> {
        batch_to_tuples(batch)
            .into_iter()
            .map(|((k, _v, ()), r)| {
                (
                    *k.downcast_checked::<u64>(),
                    *r.downcast_checked::<ZWeight>(),
                )
            })
            .collect()
    }

    /// Checks the recorder's lifecycle against the deltas of an input stream:
    /// disabled recorders record nothing; recording spans transactions and
    /// consolidates retractions; stopping returns the contents exactly once;
    /// restarting begins from a clean slate.
    #[test]
    fn test_recorder() {
        Runtime::run(CircuitConfig::with_workers(1), move |_parker| {
            let (circuit, (input, recorder)) = RootCircuit::build(|circuit| {
                let (stream, input) = circuit.add_input_zset::<u64>();
                // `integrate_trace` registers the input stream as a replay
                // source, which attaches a recorder to it.
                let _trace = stream.integrate_trace();
                let recorder = circuit
                    .cache_get(&RecorderId::<OrdZSet<DynData>>::new(
                        stream.inner().stream_id(),
                    ))
                    .expect("registering a replay source attaches a recorder");
                Ok((input, recorder))
            })
            .unwrap();

            // A disabled recorder records nothing.
            assert!(!recorder.is_recording());
            input.push(1, 1);
            circuit.transaction().unwrap();
            assert!(recorder.stop_recording().is_none());

            // Recording spans transactions; weights of the same key
            // consolidate across them; empty transactions are no-ops.
            recorder.start_recording();
            assert!(recorder.is_recording());
            input.push(2, 1);
            input.push(3, 2);
            circuit.transaction().unwrap();
            input.push(2, -1);
            input.push(4, 1);
            circuit.transaction().unwrap();
            circuit.transaction().unwrap();

            let trace = recorder
                .stop_recording()
                .expect("a recording recorder returns its contents");
            assert!(!recorder.is_recording());
            let consolidated = trace.consolidate().expect("recorded data is non-empty");
            assert_eq!(zset_contents(&consolidated), vec![(3, 2), (4, 1)]);

            // Stopping disables the recorder.
            input.push(5, 1);
            circuit.transaction().unwrap();
            assert!(recorder.stop_recording().is_none());

            // Restarting begins from a clean slate.
            recorder.start_recording();
            input.push(6, 1);
            circuit.transaction().unwrap();
            let trace = recorder.stop_recording().unwrap();
            let consolidated = trace.consolidate().unwrap();
            assert_eq!(zset_contents(&consolidated), vec![(6, 1)]);
        })
        .unwrap()
        .join()
        .unwrap();
    }

    /// `clear_state` discards the recording and disables the recorder.
    #[test]
    fn test_recorder_clear_state() {
        Runtime::run(CircuitConfig::with_workers(1), move |_parker| {
            let factories = <<OrdZSet<DynData> as crate::trace::BatchReader>::Factories>::new::<
                u64,
                (),
                ZWeight,
            >();
            let mut recorder = Recorder::<OrdZSet<DynData>>::new(&factories, Location::caller());
            let handle = recorder.handle();

            handle.start_recording();
            assert!(handle.is_recording());

            recorder.clear_state().unwrap();
            assert!(!handle.is_recording());
            assert!(handle.stop_recording().is_none());
        })
        .unwrap()
        .join()
        .unwrap();
    }

    /// The recorder's `YIELD_OWNERSHIP` edge makes the scheduler evaluate it
    /// before the integral's appender, so the appender keeps receiving the
    /// stream's owned value (the recorder must not intercept it).
    #[test]
    fn test_recorder_scheduled_before_owned_consumers() {
        Runtime::run(CircuitConfig::with_workers(1), move |_parker| {
            RootCircuit::build(|circuit| {
                let (stream, _input) = circuit.add_input_zset::<u64>();
                let _trace = stream.integrate_trace();

                let (recorder_node, owned_consumer) = {
                    let edges = circuit.edges();
                    let recorder_edge = edges
                        .iter()
                        .find(|edge| {
                            edge.ownership_preference == Some(OwnershipPreference::YIELD_OWNERSHIP)
                        })
                        .expect("the recorder consumes with YIELD_OWNERSHIP");
                    let owned_edge = edges
                        .iter()
                        .find(|edge| {
                            edge.stream_id() == recorder_edge.stream_id()
                                && edge
                                    .ownership_preference
                                    .is_some_and(|pref| pref >= OwnershipPreference::PREFER_OWNED)
                        })
                        .expect("the integral's appender prefers owned input");
                    (recorder_edge.to, owned_edge.to)
                };

                let constraints = ownership_constraints(circuit).unwrap();
                assert!(
                    constraints.contains(&(recorder_node, owned_consumer)),
                    "missing recorder-before-appender constraint in {constraints:?}"
                );
                Ok(())
            })
            .unwrap();
        })
        .unwrap()
        .join()
        .unwrap();
    }
}
