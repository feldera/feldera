//! Bootstrapping a circuit from a checkpoint: replay source registration,
//! checkpoint analysis, and the replay and concurrent bootstrap steps of
//! [`CircuitHandle`].
//!
//! ```text
//!            ┌────────┐         │ boundary stream
//!            │Recorder│◄--------┤
//!            └────────┘         │
//!                               |
//! consumers of boundary stream◄─┤◄....................
//!                (need backfill)│                    .
//!                               ▼                    .replay stream
//!                         ┌───────────┐              .
//!                         │    ...    |              .
//!                         └─────┬─────┘              .
//!                               │                    .
//!                               │                    .
//!                               │                    .
//!                               ▼                    .
//!                        ┌──────────────────────────────────┐
//!                        │             integral             │<-replay source
//!                        └──────────────────────────────────┘
//! ```
//!
//! * Boundary stream - a stream that crosses from the part of the circuit that doesn't require backfill
//!   (because its state is restored from a checkpoint) to the part that requires backfill.
//! * Replay source - an operator that stores the integral of the boundary stream and that produces its
//!   contents into the circuit during replay.
//! * replay stream - a temporary stream that replaces the boundary stream during replay and that injects
//!   the contents of the boundary stream into the circuit. Its producer in the replay source and its consumers
//!   are all the consumers of the boundary stream that participate in replay.
//! * Recorder - used during concurrent bootstrapping to record new changes to the boundary stream accumulated
//!   during bootstraping.

use super::circuit_builder::{
    Circuit, CircuitBase, CircuitHandle, GlobalNodeId, LABEL_PERSISTENT_OPERATOR_ID, Node, NodeId,
    OwnershipPreference, Stream, StreamId, StreamMetadata,
};
use super::dbsp_handle::Mode;
use crate::{
    Error as DbspError, Runtime, RuntimeError,
    circuit::operator_traits::{Data, SinkOperator},
    circuit_cache_key,
    operator::dynamic::recorder::{Recorder, RecorderControl, RecorderControlId, RecorderId},
    trace::{Batch, Spine},
};
use feldera_storage::StoragePath;
use serde::Serialize;
#[cfg(debug_assertions)]
use std::ops::Deref;
use std::{
    any::{Any, TypeId},
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
    io::ErrorKind,
    panic::Location,
    rc::Rc,
};
use tracing::debug;

/// How a stream is registered for replay.
///
/// A replay source can play back several streams.  It records the changes
/// that it plays back on one of them, its recorded stream, which carries a
/// [`Recorder`].
#[derive(Clone)]
pub struct ReplayRegistration {
    /// The stream that the replay source writes during a replay.  It aliases
    /// the registered stream.
    pub(crate) replay_stream: Box<dyn StreamMetadata>,

    /// The stream on which the replay source records the changes that it
    /// plays back in the synchronization transaction of a concurrent
    /// bootstrap.
    pub(crate) recorded_stream: StreamId,
}

// `stream` => `registration` mapping designates `registration.replay_stream`
// as a replay source for `stream`.
circuit_cache_key!(ReplaySource(StreamId => ReplayRegistration));

/// Register `replay_stream` as a replay source for `stream`.
///
/// Also attaches a [`Recorder`]
/// to `stream`: every stream with a replay source can turn out to be a
/// boundary stream of the bootstrapped region during concurrent
/// bootstrapping, in which case the deltas applied to it while the bootstrap
/// circuit replays the (frozen) integral must be recorded for the
/// synchronization transaction.  The recorder is disabled (empty, no-op)
/// until bootstrap orchestration enables it through the
/// [`RecorderId`] cache
/// entry.  A second stream that the same replay source plays back shares
/// this recorder (see [`register_secondary_replay_stream`]).
#[track_caller]
pub(crate) fn register_replay_stream<C, B>(
    circuit: &C,
    stream: &Stream<C, B>,
    replay_stream: &Stream<C, B>,
    factories: &B::Factories,
) where
    C: Circuit,
    B: Batch<Time = ()>,
{
    if register_replay_source(circuit, stream, replay_stream, stream.stream_id()) {
        register_recorder::<C, B, B>(circuit, stream, factories);
    }
}

/// Register `replay_stream` as a replay source for `accumulated`, the output
/// of an accumulator, which carries one spine per transaction.
///
/// This is the counterpart of [`register_replay_stream`] for an integral fed
/// by an accumulator (see `AccumulateZ1Trace::prepare_accumulated_replay_stream`):
/// the replay plays the integral back into the accumulator's output, so the
/// accumulator and the operators that feed it do not participate in the
/// replay.  The [`Recorder`] attached to `accumulated` records whole
/// transactions' spines.
///
/// # Arguments
///
/// * `circuit` - The circuit that contains `accumulated`.
/// * `accumulated` - The accumulator's output stream.
/// * `replay_stream` - The replay stream, which aliases `accumulated`.
/// * `factories` - Factories for the accumulated batches.
#[track_caller]
pub(crate) fn register_accumulated_replay_stream<C, B>(
    circuit: &C,
    accumulated: &Stream<C, Option<Spine<B>>>,
    replay_stream: &Stream<C, Option<Spine<B>>>,
    factories: &B::Factories,
) where
    C: Circuit,
    B: Batch<Time = ()>,
{
    if register_replay_source(circuit, accumulated, replay_stream, accumulated.stream_id()) {
        register_recorder::<C, Option<Spine<B>>, B>(circuit, accumulated, factories);
    }
}

/// Registers `replay_stream` as a replay source for `stream`, a second stream
/// that the replay source of `primary` plays back.
///
/// Unlike [`register_replay_stream`], attaches no [`Recorder`] to `stream`.  A
/// replay source records the changes that it plays back on one stream, and
/// the synchronization transaction of a concurrent bootstrap replays that one
/// recording into all of the source's replay streams.  `primary` is that
/// stream: a [`register_replay_stream`] or [`register_accumulated_replay_stream`]
/// call must register it first.
///
/// # Arguments
///
/// * `circuit` - The circuit that contains `stream`.
/// * `stream` - The stream to replay.
/// * `replay_stream` - The replay stream, which aliases `stream`.
/// * `primary` - The stream whose recorder records the changes to `stream`.
///
/// # Panics
///
/// Panics if `primary` has no recorder.
pub(crate) fn register_secondary_replay_stream<C, D, P>(
    circuit: &C,
    stream: &Stream<C, D>,
    replay_stream: &Stream<C, D>,
    primary: &Stream<C, P>,
) where
    C: Circuit,
    D: Data,
{
    if register_replay_source(circuit, stream, replay_stream, primary.stream_id()) {
        assert!(
            circuit.cache_contains(&RecorderControlId::new(primary.stream_id())),
            "the primary replay stream {} of stream {} has no recorder",
            primary.stream_id(),
            stream.stream_id(),
        );
    }
}

/// Registers `replay_stream` as a replay source for `stream`.
///
/// Only operators in the top-level circuit can be replay sources.  This
/// function can be called multiple times for the same `stream`; only the first
/// call registers a replay source for it.
///
/// # Arguments
///
/// * `circuit` - The circuit that contains `stream`.
/// * `stream` - The stream to replay.
/// * `replay_stream` - The replay stream, which aliases `stream`.
/// * `recorded_stream` - The stream on which the replay source records the
///   changes that it plays back.
///
/// # Returns
///
/// `true` if this call registered the replay source.
fn register_replay_source<C, D>(
    circuit: &C,
    stream: &Stream<C, D>,
    replay_stream: &Stream<C, D>,
    recorded_stream: StreamId,
) -> bool
where
    C: Circuit,
    D: Data,
{
    // We currently only support using operators in the top-level circuit
    // as replay sources.
    if TypeId::of::<()>() != TypeId::of::<C::Time>() {
        return false;
    }

    // If a replay source already exists, don't overwrite it. This normally shouldn't
    // happen as we should not have more than one integral for each stream. One situation
    // where this does happen today is for input streams that have an integral without
    // an accumulator as part of input_upsert, and another integral with an accumulator
    // created by a downstream join or aggregate. In this case, we want to use the former
    // for replay, as the latter may have been added in the modified version of the program
    // and may be empty, while the former can have state (conversely, if the input integral
    // is empty, the downstream integral is guaranteed to be empty too).
    if circuit.cache_contains(&ReplaySource::new(stream.stream_id())) {
        return false;
    }

    circuit.cache_insert(
        ReplaySource::new(stream.stream_id()),
        ReplayRegistration {
            replay_stream: Box::new(replay_stream.clone()),
            recorded_stream,
        },
    );
    true
}

/// Attaches a disabled [`Recorder`] to `stream`, which carries values of type
/// `D` built from batches of type `B`.
///
/// Bootstrap orchestration starts and drains the recorder through its
/// `RecorderControlId` cache entry; its [`RecorderId`] entry gives typed
/// access to it.
///
/// # Arguments
///
/// * `circuit` - The circuit that contains `stream`.
/// * `stream` - The stream to record.
/// * `factories` - Factories for the batches that `stream` carries.
#[track_caller]
fn register_recorder<C, D, B>(circuit: &C, stream: &Stream<C, D>, factories: &B::Factories)
where
    C: Circuit,
    D: Data,
    B: Batch<Time = ()>,
    Recorder<B>: SinkOperator<D>,
{
    let recorder = Recorder::<B>::new(factories, Location::caller());
    let handle = recorder.handle();
    // `YIELD_OWNERSHIP` schedules the recorder before the stream's
    // owned-input consumers (the integral's appender), so that it
    // does not intercept the owned-value delivery they rely on.
    circuit.add_sink_with_preference(recorder, stream, OwnershipPreference::YIELD_OWNERSHIP);
    circuit.cache_insert(
        RecorderControlId::new(stream.stream_id()),
        Rc::new(handle.clone()) as Rc<dyn RecorderControl>,
    );
    circuit.cache_insert(RecorderId::new(stream.stream_id()), handle);
}

/// Operators involved in the replay phase of a circuit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BootstrapInfo {
    /// Operators that will replay their contents during the replay phase,
    /// and the replay streams that each of them writes.
    pub replay_sources: BTreeMap<NodeId, BTreeSet<StreamId>>,

    /// Operators that require backfill from upstream nodes, including their persistent IDs.
    #[allow(dead_code)]
    pub need_backfill: BTreeMap<NodeId, Option<String>>,
}

/// How a circuit differs from a checkpoint it was restored from.
///
/// Computed by `CircuitHandle::analyze_checkpoint`; consumed by
/// [`CircuitHandle::restore`] (which prepares the same circuit to replay the
/// missing state) and [`CircuitHandle::restore_concurrent`] (which keeps the
/// pre-existing parts of the circuit running while a second circuit copy
/// replays the missing state).
///
/// # Example
///
/// Consider the following circuit where the `integral` node feeds two replay
/// streams: the accumulator stream `acc`, used to construct the integral itself,
/// and the `delta` stream:
///
/// ```text
///          ┌────────────────────┐
///          │                    │      delta
///          │      circuit       ├────────────────────┐
///          │                    │                    │
///          └─────────┬──────────┘                    │
///                    │                               │
///    ┌────────┐      │                               │
///    │Recorder│◄.....│                               │
///    └────────┘      │acc                            │
///                    │                               │
///        ...........►│                               │
///        .           ▼                               │
///    acc'.       ┌────────┐        delta'            │
///        ........│integral│.........................►│
///                └────────┘                          │
///                                                    ▼
/// ```
///
/// This circuit topology is useful when `delta` is a stream whose contents are
/// identical to the contents of the integral, but `delta` does not feed the
/// integral: replaying `delta` alone does not reconstruct the integral.
/// Currently the only instance of this is the lazy input upsert operator.
///
/// Both streams must carry the same contents, represented as either a stream
/// of batches, one per step, or as a single accumulated output produced once
/// per transaction.
///
/// Exactly one of the replayed streams must have a recorder attached to it, used
/// to bring the integral in the bootstrap circuit up to speed during concurrent
/// bootstrapping.
///
/// In this example:
/// - `replay_sources: {integral -> {acc', delta'}}`
/// - `boundary_streams: {acc -> integral, delta -> integral}`
/// - `recorded_streams: {integral -> acc}`
pub(super) struct CheckpointAnalysis {
    /// Nodes that will act as replay sources during the replay phase, and
    /// their replay streams.
    replay_sources: BTreeMap<NodeId, BTreeSet<StreamId>>,

    /// The streams whose contents the replay sources play back, mapped to
    /// the replay source's node: the boundary between the circuit's
    /// pre-existing region and its bootstrapped region.  The replay connects
    /// each of them to its replay stream.
    boundary_streams: BTreeMap<StreamId, NodeId>,

    /// The replay sources mapped to their recorded streams.  A replay source
    /// that plays back several boundary streams records their changes on one stream.
    recorded_streams: BTreeMap<NodeId, StreamId>,

    /// New or invalidated nodes: nodes absent from the checkpoint, plus
    /// nodes whose checkpointed state the balancer invalidated.
    need_backfill: BTreeSet<NodeId>,

    /// All nodes in the replay circuit: `need_backfill` plus its transitive
    /// ancestors up to (and including) the replay sources.  Can overlap the
    /// pre-existing region: an old node on the path from a replay source to
    /// a new node participates in the replay and keeps serving old views.
    participate_in_backfill: BTreeSet<NodeId>,
}

/// The result of [`CircuitHandle::restore_concurrent`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConcurrentRestoreOutcome {
    /// The checkpoint matches the circuit: there is nothing to bootstrap,
    /// and the circuit is fully active.
    UpToDate,
    /// The circuit serves its pre-existing region while a separate
    /// bootstrap circuit replays the bootstrapped region.
    Concurrent(BootstrapInfo),
    /// The bootstrapped region cannot be processed concurrently; the
    /// circuit fell back to the non-concurrent replay, exactly as if
    /// [`CircuitHandle::restore`] had been called.
    FellBack {
        /// Why the concurrent bootstrap was refused.
        reason: String,
        /// The replay info of the non-concurrent bootstrap.
        info: Option<BootstrapInfo>,
    },
}

/// State a circuit keeps while it is the live half of a concurrent bootstrap
/// (see [`CircuitHandle::restore_concurrent`]).
pub(super) struct ConcurrentBootstrapInfo {
    /// Nodes excluded from the schedule: their state is installed from the
    /// bootstrap circuit when bootstrapping completes.
    need_backfill: BTreeSet<NodeId>,

    /// The replay sources mapped to their recorded streams, whose recorders
    /// are armed.  Each replay source plays the contents of its stream's
    /// recorder back in the bootstrap circuit.  The synchronization
    /// transaction drains the recorders, which disarms them.
    /// [`CircuitHandle::abort_concurrent_bootstrap`] disarms them and
    /// discards their contents.
    recorded_streams: BTreeMap<NodeId, StreamId>,
}

impl CircuitHandle {
    /// Restores the circuit from a checkpoint.
    /// Restores each operator's state from the checkpoint at `base` and
    /// computes how the circuit differs from the checkpointed one.
    ///
    /// 1. Find and restore the checkpointed state of each operator.
    /// 2. Identify stateful operators (such as integrals and output nodes)
    ///    that don't have a checkpoint and require backfill (the
    ///    `need_backfill` set), including operators whose state the balancer
    ///    invalidated.
    /// 3. Iterate backward from `need_backfill` nodes to find all operators
    ///    that should participate in the replay phase.  Iteration stops when
    ///    reaching a stream whose contents can be replayed from an existing
    ///    node that has a checkpoint, or an input node.
    ///
    /// When `splice_replay_edges` is true, the iteration also wires each
    /// replay source's replay stream to all consumers of the original
    /// stream, preparing this circuit to run the replay (see
    /// [`Self::restore`]).  Pass false to only analyze the circuit, e.g.,
    /// when the replay runs in a separate circuit copy (see
    /// [`Self::restore_concurrent`]).
    pub(super) fn analyze_checkpoint(
        &mut self,
        base: &StoragePath,
        splice_replay_edges: bool,
    ) -> Result<CheckpointAnalysis, DbspError> {
        // Nodes that will act as replay sources during the replay phase of the circuit.
        let mut replay_sources: BTreeMap<NodeId, BTreeSet<StreamId>> = BTreeMap::new();

        // The streams whose contents the replay sources play back.
        let mut boundary_streams: BTreeMap<StreamId, NodeId> = BTreeMap::new();

        // The streams on which the replay sources record their changes.
        let mut recorded_streams: BTreeMap<NodeId, StreamId> = BTreeMap::new();

        // Nodes that require backfill from upstream nodes.
        let mut need_backfill: BTreeSet<GlobalNodeId> = BTreeSet::new();

        // In persistent mode, refuse to silently treat a vanished state
        // file as backfill. dependencies.json records every state file at
        // commit time; if any are gone, the corruption is structural and
        // operators need to see it. A missing backend in persistent mode is
        // an invariant violation, surface it rather than skipping verify.
        if Runtime::mode() == Mode::Persistent {
            let backend = Runtime::storage_backend()?;
            crate::circuit::checkpointer::Checkpointer::verify_checkpoint_intact(
                backend.as_ref(),
                base,
            )?;
        }

        // Initialize `need_backfill` to operators without a checkpoint.
        // Fail if there are any errors other than NotFound.
        // By the end of this, `need_backfill` will contain all new integrals and output
        // nodes that need backfill.
        self.circuit.map_nodes_recursive_mut(
            &mut |node: &mut dyn Node| match node.restore(base) {
                Err(e) if Runtime::mode() == Mode::Ephemeral => Err(e),
                Err(DbspError::Storage(ioerror)) if ioerror.kind() == ErrorKind::NotFound => {
                    need_backfill.insert(node.global_id().clone());
                    Ok(())
                }
                Err(DbspError::IO(ioerror)) if ioerror.kind() == ErrorKind::NotFound => {
                    need_backfill.insert(node.global_id().clone());
                    Ok(())
                }
                Err(e) => Err(e),
                Ok(()) => Ok(()),
            },
        )?;

        // Additional nodes that must be backfilled to keep the balancer state consistent.
        let additional_need_backfill: BTreeSet<GlobalNodeId> =
            self.invalidate_balancer_clusters(&need_backfill);
        if Runtime::worker_index() == 0 {
            debug!(
                "CircuitHandle::restore: additional need backfill: {:?}",
                additional_need_backfill
            );
        }
        need_backfill.extend(additional_need_backfill);

        debug!(
            "worker {}: CircuitHandle::restore: found {} operators that require backfill: {:?}",
            Runtime::worker_index(),
            need_backfill.len(),
            need_backfill.iter().cloned().collect::<Vec<GlobalNodeId>>()
        );

        // We can only backfill a nested circuit as a whole, so map every node
        // that needs backfill to its top-level ancestor.
        let need_backfill = need_backfill
            .into_iter()
            .map(|gid| gid.top_level_ancestor())
            .collect::<BTreeSet<_>>();

        // Iterate backward from `need_backfill` nodes to find all operators that
        // should participate in the replay.

        // All nodes that participate in the replay phase, including replay sources and
        // nodes that need backfilling.
        let mut participate_in_backfill = need_backfill.clone();

        // New nodes computed at each iteration.
        let mut participate_in_backfill_new = need_backfill.clone();

        while !participate_in_backfill_new.is_empty() {
            participate_in_backfill_new = self.compute_replay_nodes_step(
                &mut replay_sources,
                &mut boundary_streams,
                &mut recorded_streams,
                splice_replay_edges,
                &need_backfill,
                participate_in_backfill_new,
                &mut participate_in_backfill,
            )?;
        }

        debug!(
            "worker {}: CircuitHandle::restore: replaying {} operators: {:?}\n  backfilling {} operators: {:?}\n  replay circuit consists of {} operators: {:?}",
            Runtime::worker_index(),
            replay_sources.len(),
            replay_sources.keys().cloned().collect::<Vec<NodeId>>(),
            need_backfill.len(),
            need_backfill.iter().cloned().collect::<Vec<NodeId>>(),
            participate_in_backfill.len(),
            participate_in_backfill
                .iter()
                .cloned()
                .collect::<Vec<NodeId>>()
        );

        Ok(CheckpointAnalysis {
            replay_sources,
            boundary_streams,
            recorded_streams,
            need_backfill,
            participate_in_backfill,
        })
    }

    /// Puts the circuit into replay mode for the bootstrapped region of
    /// `analysis` (the epilogue of [`Self::restore`]): replay sources enter
    /// replay mode, backfilled nodes are cleared, and the scheduler is
    /// restricted to the participating nodes.  The replay edges must already
    /// be spliced.
    pub(super) fn prepare_replay(
        &mut self,
        analysis: CheckpointAnalysis,
        for_concurrent_bootstrap: bool,
    ) -> Result<Option<BootstrapInfo>, DbspError> {
        let CheckpointAnalysis {
            replay_sources,
            recorded_streams,
            need_backfill,
            participate_in_backfill,
            ..
        } = analysis;

        let replay_nodes = replay_sources.keys().cloned().collect::<BTreeSet<_>>();

        assert!(
            replay_nodes
                .intersection(&need_backfill)
                .collect::<Vec<_>>()
                .is_empty()
        );

        // Nodes that will be backfilled from upstream nodes, including need_backfill nodes
        // and their transitive ancestors.
        let nodes_to_backfill = participate_in_backfill
            .difference(&replay_nodes)
            .cloned()
            .collect::<BTreeSet<_>>();

        if !participate_in_backfill.is_empty() {
            // Configure all `replay_nodes` to run in replay mode.
            for node_id in replay_nodes.iter() {
                self.circuit
                    .map_local_node_mut(*node_id, &mut |node| node.start_replay())?;
            }

            // Clear the state of `need_backfill` nodes.
            for node_id in nodes_to_backfill.iter() {
                self.circuit
                    .map_local_node_mut(*node_id, &mut |node| node.clear_state())?;
            }

            // In a concurrent bootstrap, the backfilled output operators have
            // no connector reading their mailboxes, so they cache the view's
            // accumulated output for transfer to the live circuit at cutover
            // (see `Operator::start_bootstrap_output_caching`).
            if for_concurrent_bootstrap {
                for node_id in nodes_to_backfill.iter() {
                    self.circuit.map_local_node_mut(*node_id, &mut |node| {
                        node.start_bootstrap_output_caching()
                    });
                }
            }

            // Prepare the scheduler to only run `participate_in_backfill`.
            self.executor
                .prepare(&self.circuit, Some(&participate_in_backfill))?;

            // info!("CircuitHandle::restore: replay circuit is ready");

            // if Runtime::worker_index() == 0 {
            //     self.circuit.to_dot_file(
            //         |node| {
            //             if !node.global_id().is_child_of(self.circuit.global_id()) {
            //                 return None;
            //             }
            //             let color = if replay_sources.contains_key(&node.local_id()) {
            //                 Some(0xff5555)
            //             } else if participate_in_backfill.contains(&node.local_id()) {
            //                 Some(0x5555ff)
            //             } else {
            //                 None
            //             };
            //             Some(
            //                 crate::utils::DotNodeAttributes::new()
            //                     .with_color(color)
            //                     .with_label(&format!("{}: {}", node.global_id(), node.name())),
            //             )
            //         },
            //         |edge| {
            //             let style = if edge.is_dependency() {
            //                 Some("dotted".to_string())
            //             } else {
            //                 None
            //             };
            //             let label = if let Some(stream) = &edge.stream {
            //                 Some(format!("consumers: {}", stream.num_consumers()))
            //             } else {
            //                 None
            //             };
            //             Some(
            //                 crate::utils::DotEdgeAttributes::new(edge.stream_id())
            //                     .with_style(style)
            //                     .with_label(label),
            //             )
            //         },
            //         "replay.dot",
            //     );
            //     println!("CircuitHandle::restore: replay circuit is written to replay.dot");
            // }

            // Add persistent IDs to the need_backfill set.
            let need_backfill = nodes_to_backfill
                .iter()
                .map(|node_id| {
                    let pid = self.circuit.map_local_node_mut(*node_id, &mut |node| {
                        node.get_label(LABEL_PERSISTENT_OPERATOR_ID)
                            .map(|s| s.to_string())
                    });

                    (*node_id, pid)
                })
                .collect::<BTreeMap<_, _>>();

            let replay_info = BootstrapInfo {
                replay_sources: replay_sources.clone(),
                need_backfill,
            };

            self.replay_info = Some(replay_info.clone());
            self.recorded_streams = Some(recorded_streams);

            Ok(Some(replay_info))
        } else {
            Ok(None)
        }
    }

    /// Restore the circuit from a checkpoint for *concurrent* bootstrapping:
    /// the circuit keeps serving its pre-existing views while a separate
    /// circuit copy (prepared with [`Self::restore`]) replays the new and
    /// modified parts.
    ///
    /// Differences from [`Self::restore`]:
    ///
    /// * No replay edges are spliced and no node enters replay mode; the
    ///   replay runs in the other circuit copy.
    /// * The scheduler is prepared with the pre-existing region: every node
    ///   that is not new or invalidated (`need_backfill`) and not downstream
    ///   of such a node.  A stateless node downstream of a new operator is
    ///   not in this region, although it has no state to restore: it cannot
    ///   run without its producer.
    /// * A recorder starts recording on the recorded stream of every replay
    ///   source.  The bootstrap circuit replays the sources' integrals as of
    ///   the checkpoint, so the deltas this circuit applies to them
    ///   afterward must be captured for the synchronization transaction
    ///   that brings the bootstrap circuit up to date.
    ///
    /// # Arguments
    ///
    /// * `base` - the checkpoint to restore from.
    ///
    /// # Returns
    ///
    /// * [`ConcurrentRestoreOutcome::UpToDate`] if the checkpoint matches the
    ///   circuit.  The circuit stays fully active, and no recorder is armed.
    /// * [`ConcurrentRestoreOutcome::Concurrent`] if this circuit serves the
    ///   pre-existing region while the bootstrap circuit replays the rest.
    /// * [`ConcurrentRestoreOutcome::FellBack`] if a stream that feeds the
    ///   bootstrapped region has no replay source, for example the input of
    ///   an unmaterialized table.  The circuit then runs the replay itself,
    ///   as if [`Self::restore`] had been called.  It falls back instead of
    ///   returning an error for two reasons: an error response from a worker
    ///   kills the whole runtime, and the analysis has already restored the
    ///   state of every operator, so a second restore would load it twice.
    /// * An error if a bootstrap is already in progress or the restore fails.
    pub fn restore_concurrent(
        &mut self,
        base: &StoragePath,
    ) -> Result<ConcurrentRestoreOutcome, DbspError> {
        if self.replay_info.is_some() || self.concurrent_bootstrap_info.is_some() {
            return Err(DbspError::Runtime(RuntimeError::BootstrapCircuit(
                "the circuit already has a bootstrap in progress".to_string(),
            )));
        }

        let analysis = self.analyze_checkpoint(base, false)?;

        if analysis.participate_in_backfill.is_empty() {
            return Ok(ConcurrentRestoreOutcome::UpToDate);
        }

        if let Some(reason) = self.concurrent_restore_refusal(&analysis) {
            // Fall back to the non-concurrent replay in this circuit:
            // splice the replay edges that `analyze_checkpoint` skipped,
            // then run [`Self::restore`]'s epilogue.
            for original_stream in analysis.boundary_streams.keys() {
                let registration = self
                    .circuit
                    .get_replay_source(*original_stream)
                    .expect("boundary streams have replay sources by construction");
                self.circuit
                    .add_replay_edges(*original_stream, registration.replay_stream.as_ref());
            }
            // The replay runs in this circuit, not a separate copy, so the
            // backfilled views' output must reach their mailboxes normally.
            let info = self.prepare_replay(analysis, false)?;
            return Ok(ConcurrentRestoreOutcome::FellBack { reason, info });
        }

        for stream_id in analysis.recorded_streams.values() {
            // A replay source's recorded stream gets a recorder at
            // construction time (see `register_replay_stream`), and recorded
            // streams are found through replay-source lookups, so the
            // recorder must exist.
            let control = self
                .circuit
                .cache_get(&RecorderControlId::new(*stream_id))
                .unwrap_or_else(|| panic!("recorded stream {stream_id} has no recorder attached"));
            control.start_recording();
        }

        // Exclude `need_backfill` nodes and all their transitive successors
        // from the schedule.  The successors are excluded because their
        // inputs originate at unscheduled nodes: a stateless node between a
        // new stateful operator and a new output restores "successfully"
        // (it has no checkpoint) but cannot run without its producer.  The
        // excluded nodes' state is installed from the bootstrap circuit
        // when bootstrapping completes.
        let excluded = self.propagate_need_backfill_forward(analysis.need_backfill.clone());

        let active = self
            .circuit
            .node_ids()
            .into_iter()
            .filter(|node_id| !excluded.contains(node_id))
            .collect::<BTreeSet<_>>();

        // The active set must be downstream-closed: every stream consumed by
        // an active node must originate at an active node.
        #[cfg(debug_assertions)]
        for edge in self.circuit.edges().deref().iter() {
            debug_assert!(
                !(edge.is_stream() && active.contains(&edge.to) && excluded.contains(&edge.from)),
                "active node {} consumes a stream produced by excluded node {}",
                edge.to,
                edge.from,
            );
        }

        self.executor.prepare(&self.circuit, Some(&active))?;

        // Tell the balancer which nodes the bootstrap copy is reconstructing, so
        // it freezes the policy of any cluster that copy is also solving.  A new
        // join over shared streams runs live in this copy yet is
        // re-evaluated in the bootstrap copy; without this the live copy could
        // rebalance the shared cluster and diverge from the bootstrap copy.
        if self.circuit.root_scope() == 0 {
            self.circuit
                .balancer()
                .set_bootstrap_backfill_region(&analysis.participate_in_backfill);
        }

        // Build the public info with the same content as [`Self::restore`]
        // computes for the bootstrap circuit, so that the caller can verify
        // that the two copies agree on the shape of the bootstrap.
        let replay_nodes = analysis
            .replay_sources
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        let need_backfill = analysis
            .participate_in_backfill
            .difference(&replay_nodes)
            .map(|node_id| {
                let pid = self.circuit.map_local_node_mut(*node_id, &mut |node| {
                    node.get_label(LABEL_PERSISTENT_OPERATOR_ID)
                        .map(|s| s.to_string())
                });

                (*node_id, pid)
            })
            .collect::<BTreeMap<_, _>>();

        let info = BootstrapInfo {
            replay_sources: analysis.replay_sources.clone(),
            need_backfill,
        };

        self.concurrent_bootstrap_info = Some(ConcurrentBootstrapInfo {
            need_backfill: excluded,
            recorded_streams: analysis.recorded_streams,
        });

        Ok(ConcurrentRestoreOutcome::Concurrent(info))
    }

    /// Returns the reason the bootstrapped region of `analysis` cannot be
    /// processed concurrently, or `None` if it can.
    fn concurrent_restore_refusal(&self, analysis: &CheckpointAnalysis) -> Option<String> {
        // If the backward walk reached a connector-fed input node, some stream
        // feeding the bootstrapped region has no replay source (an
        // unmaterialized table): the bootstrap circuit would silently
        // replay nothing for it.  In the non-concurrent bootstrap, the
        // controller re-ingests such tables from their connectors.
        //
        // A self-contained deterministic source (e.g. the `ConstantGenerator`
        // the SQL compiler emits for a global aggregate's empty-group default)
        // is NOT such an input: the rebuilt bootstrap circuit re-runs it and
        // reproduces its output, so it needs no replay source and must not
        // trip this check.
        for node_id in analysis.participate_in_backfill.iter() {
            let (is_input, is_deterministic_source) =
                self.circuit.map_local_node_mut(*node_id, &mut |node| {
                    (node.is_input(), node.is_deterministic_source())
                });
            if is_input && !is_deterministic_source {
                let name = self
                    .circuit
                    .map_local_node_mut(*node_id, &mut |node| node.name().into_owned());
                return Some(format!(
                    "a stream feeding the bootstrapped region has no replay source \
                     (input operator {node_id} ({name}) is an unmaterialized table?)"
                ));
            }
        }

        None
    }

    /// Stops the recorders armed by [`Self::restore_concurrent`] and returns
    /// their recorded contents, keyed by recorded stream.
    ///
    /// The synchronization transaction of a concurrent bootstrap feeds the
    /// result to the bootstrap circuit's [`Self::start_sync_replay`].
    pub fn drain_recorders(&mut self) -> Result<BTreeMap<StreamId, Box<dyn Any>>, DbspError> {
        let Some(info) = self.concurrent_bootstrap_info.as_ref() else {
            return Err(DbspError::Runtime(RuntimeError::BootstrapCircuit(
                "no concurrent bootstrap is in progress".to_string(),
            )));
        };

        let mut recorded = BTreeMap::new();
        for stream_id in info.recorded_streams.values() {
            let control = self
                .circuit
                .cache_get(&RecorderControlId::new(*stream_id))
                .unwrap_or_else(|| panic!("recorded stream {stream_id} has no recorder attached"));
            let contents = control.stop_recording_any().unwrap_or_else(|| {
                panic!("the recorder on recorded stream {stream_id} was not recording")
            });
            recorded.insert(*stream_id, contents);
        }

        Ok(recorded)
    }

    /// Starts the synchronization replay on a circuit prepared by
    /// [`Self::restore`]: each replay source replays the changes that the
    /// live circuit recorded on its recorded stream (the boxes hold the
    /// recorders' spines; see [`Self::drain_recorders`]).
    ///
    /// The caller must run a transaction afterwards; the replay drains
    /// chunk-by-chunk during the transaction's commit, exactly like the
    /// bootstrap replay.
    pub fn start_sync_replay(
        &mut self,
        mut recorded: BTreeMap<StreamId, Box<dyn Any>>,
    ) -> Result<(), DbspError> {
        let Some(recorded_streams) = self.recorded_streams.as_ref() else {
            return Err(DbspError::Runtime(RuntimeError::BootstrapCircuit(
                "the circuit has no replay prepared".to_string(),
            )));
        };

        for (node_id, stream_id) in recorded_streams.iter() {
            let contents = recorded
                .remove(stream_id)
                .unwrap_or_else(|| panic!("no recorded contents for recorded stream {stream_id}"));
            // `map_local_node_mut` takes an `FnMut` closure, which cannot
            // consume `contents` directly; it invokes the closure exactly
            // once, so route the move through an `Option`.
            let mut contents = Some(contents);
            self.circuit.map_local_node_mut(*node_id, &mut |node| {
                let contents = contents
                    .take()
                    .expect("map_local_node_mut invoked the closure twice");
                node.start_sync_replay(contents)
            })?;
        }

        assert!(
            recorded.is_empty(),
            "recorded contents for streams {:?} have no replay sources",
            recorded.keys().collect::<Vec<_>>(),
        );

        Ok(())
    }

    /// Completes a concurrent bootstrap on the live circuit: installs the
    /// state that the bootstrap circuit (`bootstrapped`) computed for the
    /// backfilled nodes and reactivates the full circuit.
    ///
    /// The caller must have completed the synchronization transaction on
    /// `bootstrapped` and must drop it after this call.
    pub fn cutover_from(&mut self, bootstrapped: &CircuitHandle) -> Result<(), DbspError> {
        let Some(info) = self.concurrent_bootstrap_info.take() else {
            return Err(DbspError::Runtime(RuntimeError::BootstrapCircuit(
                "no concurrent bootstrap is in progress".to_string(),
            )));
        };

        for node_id in info.need_backfill.iter() {
            self.circuit.map_local_node_mut(*node_id, &mut |node| {
                bootstrapped
                    .circuit
                    .map_local_node_mut(*node_id, &mut |bootstrapped_node| {
                        node.swap_state_with(bootstrapped_node)
                    })
            })?;
        }

        // Reactivate the full circuit.  The recorders were disabled when
        // the synchronization transaction drained them.
        self.executor.prepare(&self.circuit, None)?;

        Ok(())
    }

    /// Aborts a concurrent bootstrap on the live circuit: disarms the
    /// recorders (discarding their contents) and forgets the bootstrap
    /// state.  No-op if no concurrent bootstrap is in progress.
    ///
    /// The schedule remains restricted to the pre-existing region — the
    /// backfilled nodes have no state to run with.  The only way to
    /// populate them afterwards is to restart from a checkpoint.
    pub fn abort_concurrent_bootstrap(&mut self) {
        let Some(info) = self.concurrent_bootstrap_info.take() else {
            return;
        };

        for stream_id in info.recorded_streams.values() {
            if let Some(control) = self.circuit.cache_get(&RecorderControlId::new(*stream_id)) {
                let _ = control.stop_recording_any();
            }
        }
    }

    /// Compute additional nodes whose state must be discarded and backfilled in order
    /// to ensure that the balancer state is consistent.
    ///
    /// The new version of the circuit may have a different join graph in which the partitioning
    /// policy used to create the checkpoint may no longer be valid.
    ///
    /// Example:
    ///
    /// Consider the following checkpointed circuit:
    ///
    /// s1 = join(a, b)
    /// s2 = join(c, d)
    ///
    /// where streams a and c are partitioned using PartitioningPolicy::Broadcast, and
    /// b and d are partitioned using PartitioningPolicy::Shard.
    ///
    /// The new circuit adds
    ///
    /// s3 = join(a, c)
    ///
    /// This new circuit is in an inconsistent state since we can't join two broadcast streams.
    ///
    /// This function computes a conservative approximation of the set of nodes whose state must
    /// be discarded and backfilled to avoid such inconsistencies:
    ///
    /// 1. For each join cluster, check if there exists a solution that extends the partitioning
    ///    policies of the nodes restores from the checkpoint.
    /// 2. If no solution exists, mark all nodes in the cluster and their transitive successors
    ///    as needing backfill.
    fn invalidate_balancer_clusters(
        &self,
        need_backfill: &BTreeSet<GlobalNodeId>,
    ) -> BTreeSet<GlobalNodeId> {
        // Convert GlobalNodeId to top-level NodeId for comparison with balancer data.
        let need_backfill_node_ids: BTreeSet<NodeId> = need_backfill
            .iter()
            .map(|gid| gid.top_level_ancestor())
            .collect();

        // Compute exchange sender nodes that need to be discarded and backfilled.
        let additional_need_backfill = self
            .circuit
            .balancer()
            .invalidate_clusters_for_bootstrapping(&need_backfill_node_ids);

        // Invalidate all successors as well; otherwise we can end up with nodes that are not
        // marked for backfill but their ancestors are.
        let nodes_to_add = self.propagate_need_backfill_forward(
            additional_need_backfill
                .difference(&need_backfill_node_ids)
                .cloned()
                .collect(),
        );

        // Convert NodeIds back to GlobalNodeIds and add to additional_need_backfill
        nodes_to_add
            .into_iter()
            .map(|node_id| GlobalNodeId::root().child(node_id))
            .collect()
    }

    /// Returns `true` if node `node_id` is a waiter (see
    /// [`SourceOperator::is_waiter`](crate::circuit::operator_traits::SourceOperator::is_waiter)).
    fn is_waiter(&self, node_id: NodeId) -> bool {
        self.circuit
            .map_local_node_mut(node_id, &mut |node| node.is_waiter())
    }

    /// Compute all transitive successors of need_backfill nodes.
    fn propagate_need_backfill_forward(
        &self,
        mut need_backfill: BTreeSet<NodeId>,
    ) -> BTreeSet<NodeId> {
        // Recursively add all successors of these exchange sender nodes
        let mut worklist: Vec<NodeId> = need_backfill.iter().cloned().collect();
        let mut visited = BTreeSet::new();

        while let Some(node_id) = worklist.pop() {
            if visited.contains(&node_id) {
                continue;
            }
            visited.insert(node_id);

            // Get all successors of this node, except for waiters (see
            // `SourceOperator::is_waiter`): the live copy keeps running a
            // waiter for the other nodes that it waits for.
            let successors: Vec<NodeId> = self
                .circuit
                .edges()
                .by_source
                .get(&node_id)
                .into_iter()
                .flat_map(|edges| edges.iter().map(|edge| edge.to))
                .filter(|successor| !self.is_waiter(*successor))
                .collect();

            for successor in successors {
                if !visited.contains(&successor) {
                    worklist.push(successor);
                    need_backfill.insert(successor);
                }
            }

            // Add all dependencies of this node. Makes sure that the output half of Z-1 is marked for backfill.
            let dependencies: Vec<NodeId> = self
                .circuit
                .edges()
                .by_destination
                .get(&node_id)
                .into_iter()
                .flat_map(|edges| edges.iter())
                .filter(|edge| edge.is_dependency())
                .map(|edge| edge.from)
                .collect();

            for dependency in dependencies {
                if !visited.contains(&dependency) {
                    worklist.push(dependency);
                    need_backfill.insert(dependency);
                }
            }
        }

        need_backfill
    }

    /// Iterative step of computing the set of nodes that participate in the replay phase.
    ///
    /// Find all input streams of nodes in `participate_in_backfill_new`.
    ///
    /// * For streams that can be replayed from a replay source:
    ///  - add the replay source to `replay_sources` and `participate_in_backfill`.
    ///  - create replay streams from the replay source to each consumer of the original stream.
    ///  - record the replay source's recorded stream in `recorded_streams`.
    /// * For other streams, add their origin nodes to `participate_in_backfill`.
    ///
    /// Return the set of nodes newly added to `participate_in_backfill`.
    #[allow(clippy::too_many_arguments)]
    fn compute_replay_nodes_step(
        &self,
        replay_sources: &mut BTreeMap<NodeId, BTreeSet<StreamId>>,
        boundary_streams: &mut BTreeMap<StreamId, NodeId>,
        recorded_streams: &mut BTreeMap<NodeId, StreamId>,
        splice_replay_edges: bool,
        need_backfill: &BTreeSet<NodeId>,
        participate_in_backfill_new: BTreeSet<NodeId>,
        participate_in_backfill: &mut BTreeSet<NodeId>,
    ) -> Result<BTreeSet<NodeId>, DbspError> {
        let mut inputs = BTreeSet::new();

        for node_id in participate_in_backfill_new.iter() {
            // A waiter (see `SourceOperator::is_waiter`) joins the region, so
            // that the bootstrap runs it, but the nodes that it waits for do
            // not: they are unrelated to each other.
            if self.is_waiter(*node_id) {
                continue;
            }

            // Compute immediate ancestors of node_id, including:
            // 1. Nodes connected to node_id by a stream.
            // 2. Nodes that depend on node_id -- makes sure that if we schedule the output half of a strict operator,
            //    we will also schedule the input half.
            // 3. Nodes that node_id depends on -- Makes sure that if we schedule the output of an exchange operator,
            //    we will schedule the input part as well.

            // 1.
            let node_inputs = self
                .circuit
                .edges()
                .by_destination
                .get(node_id)
                .iter()
                .flat_map(|edges| edges.iter())
                .filter(|edge| edge.is_stream())
                .map(|edge| {
                    // If the origin of the stream is a node inside the nested circuit, we will clear and replay the
                    // entire nested circuit, as we don't currently have a way to replay state from inside a nested
                    // circuit into a parent circuit.
                    (Some(edge.stream_id().unwrap()), edge.from)
                })
                .collect::<Vec<_>>();

            for input in node_inputs.into_iter() {
                inputs.insert(input);
            }

            // 2.
            for edge in self.circuit.edges().dependencies_of(*node_id) {
                inputs.insert((None, edge.from));
            }

            // 3.
            for edge in self.circuit.edges().depend_on(*node_id) {
                inputs.insert((None, edge.to));
            }
        }

        let mut participate_in_backfill_new = BTreeSet::new();

        let mut replay_streams = BTreeMap::new();

        for (stream_id, mut node_id) in inputs.into_iter() {
            // println!("replay needed for ({stream_id}, {node_id})");

            // Add all ancestors of `participate_in_backfill_new` to the `participate_in_backfill` set, except streams
            // that can be replayed from a different node.
            if let Some(stream_id) = stream_id
                && let Some(registration) = self.circuit.get_replay_source(stream_id)
            {
                let replay_source = registration.replay_stream.local_node_id();

                // If the replay source is itself in the need_backfill set (i.e., it's an integral without
                // a checkpoint), it cannot be used for replay.
                if !need_backfill.contains(&replay_source) {
                    replay_streams.insert(stream_id, registration);
                    // trace!(
                    //     "worker {}: Replacing node_id {node_id} with replay source {}",
                    //     Runtime::worker_index(),
                    //     replay_source
                    // );

                    // Replace the node_id with the replay source.
                    node_id = replay_source;
                }
            }

            if !participate_in_backfill.contains(&node_id) {
                // println!("Adding {gid} to participate_in_backfill via {stream_id:?}");
                participate_in_backfill.insert(node_id);
                participate_in_backfill_new.insert(node_id);
            }
        }

        // Record the boundary streams and, when preparing this circuit for
        // replay (rather than only analyzing it), connect each replay stream
        // to all operators that consume its original stream.  A replay
        // source can play back several boundary streams, so this happens
        // once per replay stream rather than once per source.
        for (original_stream, registration) in replay_streams.into_iter() {
            let ReplayRegistration {
                replay_stream,
                recorded_stream,
            } = registration;
            let replay_source = replay_stream.local_node_id();

            boundary_streams.insert(original_stream, replay_source);
            let new_replay_stream = replay_sources
                .entry(replay_source)
                .or_default()
                .insert(replay_stream.stream_id());
            if new_replay_stream && splice_replay_edges {
                self.circuit
                    .add_replay_edges(original_stream, replay_stream.as_ref());
            }

            // The synchronization transaction of a concurrent bootstrap
            // replays each source's recording once, so all replay streams of
            // a source must name the same recorded stream.
            let source_recorded_stream = *recorded_streams
                .entry(replay_source)
                .or_insert(recorded_stream);
            assert_eq!(
                source_recorded_stream, recorded_stream,
                "replay source {replay_source} records its changes on two streams"
            );
        }

        Ok(participate_in_backfill_new)
    }

    /// Returns `true` if all replay sources have completed their replay.
    pub fn is_replay_complete(&self) -> bool {
        let Some(replay_info) = self.replay_info.as_ref() else {
            return true;
        };

        // Bootstrapping is finished when all replay sources have completed their replay and the
        // transaction has been committed.

        let all_complete = replay_info.replay_sources.keys().all(|node_id| {
            self.circuit
                .map_local_node_mut(*node_id, &mut |node| node.is_replay_complete())
        });

        all_complete && self.is_commit_complete()
    }

    /// Finalize the replay phase of the circuit.
    ///
    /// * Notify all replay sources to go back to normal operation.
    /// * Delete all replay streams.
    /// * Prepare the scheduler to run the full circuit.
    pub fn complete_replay(&mut self) -> Result<(), DbspError> {
        // info!("Replay complete");

        let Some(replay_info) = self.replay_info.take() else {
            return Ok(());
        };

        // End replay mode.
        for (node_id, stream_ids) in replay_info.replay_sources.iter() {
            self.circuit
                .map_local_node_mut(*node_id, &mut |node| node.end_replay())?;
            for stream_id in stream_ids {
                self.circuit.edges_mut().delete_stream(*stream_id);
            }
        }

        // Prepare the scheduler to run the full circuit.
        self.executor.prepare(&self.circuit, None)?;

        // if Runtime::worker_index() == 0 {
        //     self.circuit.to_dot_file(
        //         |_node| Some(crate::circuit::dot::DotNodeAttributes::new()),
        //         |edge| {
        //             let style = if edge.is_dependency() {
        //                 Some("dotted".to_string())
        //             } else {
        //                 None
        //             };
        //             let label = if let Some(stream) = &edge.stream {
        //                 Some(format!("consumers: {}", stream.num_consumers()))
        //             } else {
        //                 None
        //             };
        //             Some(
        //                 crate::circuit::dot::DotEdgeAttributes::new(edge.stream_id())
        //                     .with_style(style)
        //                     .with_label(label),
        //             )
        //         },
        //         "final.dot",
        //     );
        //     info!("CircuitHandle::restore: final circuit is written to final.dot");
        // }

        Ok(())
    }
}
