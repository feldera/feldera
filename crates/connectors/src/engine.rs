//! The engine, as the controller and the server see it.
//!
//! The controller and the server reach the engine that runs a pipeline's circuit only through
//! these traits, as trait objects, so this crate builds once for every engine. An engine
//! implements [`Engine`], and only the server's entry point,
//! [`server_main`](crate::server::server_main), is generic over it.

use std::any::Any;
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::fmt::Debug;
use std::io;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result as AnyResult;
use crossbeam::sync::Parker;
use feldera_adapterlib::bootstrap::{BootstrapInfo, ConcurrentRestoreOutcome};
use feldera_adapterlib::catalog::CircuitCatalog;
use feldera_adapterlib::errors::journal::StepError;
use feldera_adapterlib::transport::Step;
use feldera_ir::LirCircuit;
use feldera_storage::fbuf::slab::FBufSlabsStats;
use feldera_storage::histogram::ExponentialHistogramSnapshot;
use feldera_types::checkpoint::{
    CheckpointMetadata, CheckpointSyncMetrics, HostInfo, RemoteCheckpoint,
};
use feldera_types::circuit::Layout;
use feldera_types::config::{DevTweaks, PipelineConfig, PipelineIdentity, SyncConfig};
use feldera_types::memory_pressure::MemoryPressure;
use feldera_types::transaction::CommitProgressSummary;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::ControllerError;
pub use crate::controller::{Checkpoint, CheckpointOffsets, StepMetadata};
pub use crate::server::StoredStatus;

/// What the controller passes to the circuit factory.
#[derive(Clone, Debug)]
pub struct CircuitParams {
    /// The workers of this host, and of the other hosts of a multihost pipeline.
    pub layout: Layout,

    /// The host that writes each output relation of a multihost pipeline.
    pub output_assignment: BTreeMap<String, usize>,

    /// The pipeline's storage, if it has any.
    pub storage: Option<Arc<dyn PipelineStorage>>,

    /// The checkpoint to restore, or `None` to start fresh.
    pub init_checkpoint: Option<Uuid>,

    /// Leaves the restore of `init_checkpoint` to the concurrent bootstrap that
    /// [`CircuitDriver::start_concurrent_bootstrap`] starts, so that the circuit starts empty.
    pub defer_restore: bool,

    /// The memory limit of the circuit, in bytes.
    pub max_rss_bytes: Option<u64>,

    /// The memory, in bytes, that the circuit leaves to other components, such as the ad-hoc
    /// query engine.
    pub reserved_memory_bytes: u64,

    /// The CPUs to pin the workers to.
    pub pin_cpus: Vec<usize>,

    /// The developer settings of the pipeline that the engine reads.
    pub dev_tweaks: DevTweaks,
}

/// What the circuit factory returns.
pub struct EngineCircuit {
    /// Drives the circuit, on the circuit thread.
    pub driver: Box<dyn CircuitDriver>,

    /// The services of the engine that runs the circuit.
    pub runtime: Arc<dyn EngineRuntime>,

    /// The circuit's input and output streams, by relation.
    pub catalog: Box<dyn CircuitCatalog>,
}

/// Builds a pipeline's circuit. The controller calls it once, on the circuit thread.
pub type CircuitFactory =
    Box<dyn FnOnce(CircuitParams) -> Result<EngineCircuit, ControllerError> + Send + 'static>;

/// An engine that runs pipelines: one per pipeline binary.
pub trait Engine: Send + Sync + 'static {
    /// The engine's command-line arguments, which the server parses with its own.
    type Args: clap::Args + Clone + Debug + Send + Sync + 'static;

    /// Returns the factory of the circuit of the pipeline that `config` describes.
    fn circuit_factory(
        &self,
        config: &PipelineConfig,
        args: &Self::Args,
    ) -> Result<CircuitFactory, ControllerError>;

    /// Opens the storage that `config` names, or returns `None` if it names none. The server
    /// opens storage before any circuit exists.
    fn open_storage(
        &self,
        config: &PipelineConfig,
    ) -> Result<Option<Arc<dyn PipelineStorage>>, ControllerError>;

    /// Returns the loader that builds the circuits of programs at run time, or `None` if the
    /// engine cannot load a program at run time.
    fn program_loader(&self, args: &Self::Args) -> Option<Box<dyn ProgramLoader>>;
}

/// Builds the circuit of a program at run time, so that one process can run many programs.
pub trait ProgramLoader: Send + Sync + 'static {
    /// Returns the factory of the circuit of the program in `config`, whose Rust UDFs, if any,
    /// are in the shared library `udf_library`, which the engine links at run time.
    fn load(
        &self,
        config: &PipelineConfig,
        udf_library: Option<&Path>,
    ) -> Result<CircuitFactory, ControllerError>;
}

/// Drives a circuit.
pub trait CircuitDriver {
    /// Runs a transaction that starts and commits at once.
    fn transaction(&mut self) -> Result<(), ControllerError>;

    /// Opens a transaction, which spans steps until its commit ends.
    fn start_transaction(&mut self) -> Result<(), ControllerError>;

    /// Starts the commit of the open transaction, which the following steps carry out.
    fn start_commit_transaction(&mut self) -> Result<(), ControllerError>;

    /// Runs a step of the open transaction. Blocks until the engine makes progress or
    /// `max_wait` passes, and returns whether the transaction committed. An engine may
    /// ignore `max_wait` and block until its step ends.
    fn step(&mut self, max_wait: Duration) -> Result<bool, ControllerError>;

    /// The progress of the commit in progress.
    fn commit_progress(&mut self) -> Result<CommitProgressSummary, ControllerError>;

    /// The time that the engine spent in steps, multiplied by the number of its threads.
    fn runtime_elapsed(&self) -> Duration;

    /// Starts a checkpoint of the circuit after `steps` steps and `processed_records`
    /// records. The controller commits and publishes it on another thread.
    fn prepare_checkpoint(
        &mut self,
        steps: u64,
        processed_records: u64,
    ) -> Result<Box<dyn CheckpointCommitter>, ControllerError>;

    /// Deletes every checkpoint but the newest and those in `keep`.
    fn gc_checkpoint(&mut self, keep: HashSet<Uuid>) -> Result<(), ControllerError>;

    /// Whether the circuit is bootstrapping.
    fn bootstrap_in_progress(&self) -> bool;

    /// What the circuit rebuilds, if it is bootstrapping.
    fn bootstrap_info(&self) -> Option<BootstrapInfo>;

    /// Restores checkpoint `base`, and bootstraps the new and modified parts of the circuit
    /// beside the live ones, if it can.
    fn start_concurrent_bootstrap(
        &mut self,
        base: Uuid,
    ) -> Result<ConcurrentRestoreOutcome, ControllerError>;

    /// Runs a step of the concurrent bootstrap, and returns whether its transaction committed.
    fn step_bootstrap_circuit(&mut self) -> Result<bool, ControllerError>;

    /// Starts the transaction that moves the concurrent bootstrap's state into the live
    /// circuit.
    fn sync_concurrent_bootstrap(&mut self) -> Result<(), ControllerError>;

    /// Ends the concurrent bootstrap once its state is in the live circuit.
    fn complete_concurrent_bootstrap(&mut self) -> Result<(), ControllerError>;

    /// Drops the circuit copy that ran the concurrent bootstrap.
    fn destroy_bootstrap_circuit(&mut self) -> Result<(), ControllerError>;

    /// The progress of the concurrent bootstrap's commit.
    fn bootstrap_commit_progress(&mut self) -> Result<CommitProgressSummary, ControllerError>;

    /// The low-level form of the circuit.
    fn lir(&mut self) -> Result<LirCircuit, ControllerError>;

    /// A profile of the circuit graph, with the metrics of its operators.
    fn graph_profile(&mut self) -> Result<Box<dyn EngineGraphProfile>, ControllerError>;

    /// The profile of the circuit as JSON.
    fn json_profile(&mut self) -> Result<JsonProfileChunks, ControllerError>;

    /// Starts the engine's CPU profiler, whose readings the profiles then include.
    fn enable_cpu_profiler(&mut self) -> Result<(), ControllerError>;

    /// Turns the automatic rebalancing of work among the workers on or off.
    fn set_auto_rebalance(&mut self, enable: bool) -> Result<(), ControllerError>;

    /// Rebalances the work among the workers now.
    fn rebalance(&mut self) -> Result<(), ControllerError>;

    /// Starts compacting the circuit's stored state.
    fn start_compaction(&mut self) -> Result<(), ControllerError>;

    /// Stops the circuit and waits for its threads.
    fn kill(self: Box<Self>) -> Result<(), ControllerError>;
}

/// Writes the circuit's prepared checkpoint to stable storage.
pub trait CheckpointCommitter: Send {
    /// Writes the checkpoint to stable storage.
    fn commit(self: Box<Self>) -> Result<Box<dyn CheckpointPublisher>, ControllerError>;
}

/// Makes a checkpoint on stable storage the one that a restart uses.
pub trait CheckpointPublisher: Send {
    /// The checkpoint's metadata, which the controller records in its state before publication.
    fn metadata(&self) -> &CheckpointMetadata;

    /// Stores `state`, the controller's state at the checkpoint, with the checkpoint, makes the
    /// checkpoint the one that a restart uses, and records `state` as the state to resume
    /// from, in the order that the engine's storage needs.
    fn publish(self: Box<Self>, state: &Checkpoint) -> Result<CheckpointMetadata, ControllerError>;
}

/// Opens a pipeline's storage, as [`Engine::open_storage`] does, for the server, which is not
/// generic over the engine.
pub type OpenStorageFn = Arc<
    dyn Fn(&PipelineConfig) -> Result<Option<Arc<dyn PipelineStorage>>, ControllerError>
        + Send
        + Sync,
>;

/// A pipeline's persistent state, in the engine's own storage. The controller and the server
/// reach storage only through it.
pub trait PipelineStorage: Send + Sync {
    /// The status that the server stored, or `None` if there is none or it cannot be read.
    fn read_status(&self) -> Option<StoredStatus>;

    /// Stores the server's status. A failure is logged, not returned.
    fn write_status(&self, status: &StoredStatus);

    /// The published checkpoints, oldest first.
    fn checkpoints(&self) -> Result<VecDeque<CheckpointMetadata>, ControllerError>;

    /// Deletes the files that no published checkpoint uses.
    fn gc_startup(&self) -> Result<(), ControllerError>;

    /// The controller's state at the checkpoint to resume from, or `None` to start fresh.
    fn latest_checkpoint(&self) -> Result<Option<Checkpoint>, ControllerError>;

    /// The controller's state at checkpoint `uuid`.
    fn checkpoint(&self, uuid: Uuid) -> Result<Checkpoint, ControllerError>;

    /// Opens the journal of a fault-tolerant pipeline, as `mode` says.
    fn open_journal(&self, mode: JournalMode<'_>) -> Result<Box<dyn StepJournal>, ControllerError>;

    /// The bytes of storage in use, or `None` if the engine does not count them.
    fn usage_bytes(&self) -> Option<i64>;

    /// The local directory that holds the storage, if there is one.
    fn local_path(&self) -> Option<&Path>;

    /// Copies checkpoints to and from object storage, or `None` if the engine cannot.
    fn sync(&self) -> Option<&dyn CheckpointSync>;

    /// The engine's own type, which only its circuit factory reads.
    fn as_any(&self) -> &dyn Any;
}

impl Debug for dyn PipelineStorage {
    /// Names the storage by its local path, if it has one.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PipelineStorage")
            .field("local_path", &self.local_path())
            .finish_non_exhaustive()
    }
}

/// How [`PipelineStorage::open_journal`] opens the journal.
#[derive(Clone, Copy, Debug)]
pub enum JournalMode<'a> {
    /// Opens the journal to replay it from the given step.
    Open(Step),

    /// Empties the journal.
    Truncate,

    /// Starts fault tolerance: empties the journal and records the given state as the one to
    /// resume from.
    Create(&'a Checkpoint),
}

/// The journal of the steps of a fault-tolerant pipeline.
pub trait StepJournal: Send {
    /// The record of `step`, or `None` if the journal has none.
    fn read(&self, step: Step) -> Result<Option<StepMetadata>, StepError>;

    /// Records a step, durably.
    fn write(&self, record: &StepMetadata) -> Result<(), StepError>;

    /// Deletes the records of the steps in `steps`.
    fn delete(&self, steps: Range<Step>) -> Result<(), StepError>;

    /// The steps from `start` on that the journal records, in increasing order.
    fn list_steps(&self, start: Step) -> Result<Vec<Step>, StepError>;
}

/// Copies checkpoints between a pipeline's storage and object storage, an enterprise feature.
pub trait CheckpointSync: Send + Sync {
    /// Pushes checkpoint `checkpoint` to the object storage that `sync` names.
    fn push(
        &self,
        checkpoint: Uuid,
        sync: SyncConfig,
        host_info: Option<HostInfo>,
        pipeline: PipelineIdentity,
    ) -> AnyResult<Option<CheckpointSyncMetrics>>;

    /// Pulls the checkpoint that `sync` names into the pipeline's storage. A standby pipeline
    /// always pulls, and fails if there is no checkpoint.
    fn pull(
        &self,
        sync: SyncConfig,
        host_info: Option<HostInfo>,
        standby: bool,
        pipeline: PipelineIdentity,
    ) -> AnyResult<(CheckpointMetadata, Option<CheckpointSyncMetrics>)>;

    /// Makes `pipeline` the owner of the bucket that `sync` names.
    fn take_ownership(&self, sync: SyncConfig, pipeline: PipelineIdentity) -> AnyResult<()>;

    /// The checkpoints in the object storage that `sync` names.
    fn list_remote(&self, sync: SyncConfig) -> AnyResult<Vec<RemoteCheckpoint>>;

    /// `Some` if a standby pipeline was activated before, with the checkpoint it was activated
    /// from, if any.
    fn read_activation(&self) -> Option<Option<CheckpointMetadata>>;

    /// Records that a standby pipeline was activated from `checkpoint`.
    fn write_activation(&self, checkpoint: &Option<CheckpointMetadata>) -> AnyResult<()>;
}

/// The services of the engine that runs a circuit, for the threads of the controller. They
/// work after the circuit stops, and report nothing then.
pub trait EngineRuntime: Send + Sync {
    /// A token that the engine cancels when the circuit stops.
    fn shutdown_token(&self) -> CancellationToken;

    /// Runs `f` on a thread named `name`, which the engine joins when the circuit stops. The
    /// engine unparks `parker` then, so that a parked `f` sees the stop.
    fn spawn_aux_thread(&self, name: &str, parker: Parker, f: Box<dyn FnOnce(Parker) + Send>);

    /// The memory pressure level, and the number of times it reached high or critical.
    fn memory_pressure(&self) -> Option<(MemoryPressure, u64)>;

    /// Changes the process memory budget, in bytes, while the circuit runs.
    fn set_max_rss_bytes(&self, max_rss: Option<u64>);

    /// A snapshot of the engine's metrics.
    fn metrics(&self) -> EngineMetrics;

    /// Whether `NOW()` follows the wall clock, so that the controller connects its clock to
    /// the table `now`. An engine without a wall clock leaves `now` to the pipeline's other
    /// inputs.
    fn has_wall_clock(&self) -> bool {
        true
    }
}

/// A profile of the circuit graph.
pub trait EngineGraphProfile: Send {
    /// Writes the profile under directory `dir`, and returns the path written.
    fn dump(&self, dir: &Path) -> io::Result<PathBuf>;

    /// Writes the profile to `out` as a Zip archive.
    fn write_zip(&self, out: &mut dyn io::Write) -> io::Result<()>;
}

/// A JSON profile, in order, one chunk at a time.
pub type JsonProfileChunks = Box<dyn Iterator<Item = io::Result<Vec<u8>>> + Send>;

/// The engine's metrics, which the controller writes under the names that `/metrics` reports.
pub struct EngineMetrics {
    /// Records that lateness annotations dropped.
    pub late_records: u64,

    /// The time that a worker waited for merges, in nanoseconds.
    pub compaction_stall_nanoseconds: u64,

    /// Files that the engine created in storage.
    pub files_created: u64,

    /// Files that the engine deleted from storage.
    pub files_deleted: u64,

    /// Files synced to stable storage one by one.
    pub files_synced: u64,

    /// Steps that the circuit took.
    pub steps: u64,

    /// The latency of a step, in microseconds.
    pub step_latency_microseconds: ExponentialHistogramSnapshot,

    /// The time an operator took to write its part of a checkpoint, in microseconds.
    pub operator_checkpoint_latency_microseconds: ExponentialHistogramSnapshot,

    /// Messages that the workers received from each other.
    pub exchange_messages_received: u64,

    /// Exchange messages received more than once.
    pub duplicate_exchange_messages_received: u64,

    /// `None` once the circuit stops.
    pub runtime: Option<RuntimeGauges>,
}

/// Gauges of a running circuit.
#[derive(Clone, Debug)]
pub struct RuntimeGauges {
    /// The bytes in the engine's storage cache.
    pub cache_used_bytes: usize,

    /// The size limit of the storage cache, in bytes.
    pub cache_limit_bytes: usize,

    /// The statistics of the slab allocator of I/O buffers.
    pub fbuf_slabs: FBufSlabsStats,

    /// The memory limit, after other components take their share.
    pub max_rss_bytes: Option<u64>,

    /// The memory pressure level.
    pub memory_pressure: MemoryPressure,

    /// The number of times the memory pressure reached high or critical.
    pub memory_pressure_epoch: u64,
}
