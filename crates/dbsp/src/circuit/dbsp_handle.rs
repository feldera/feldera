use crate::circuit::GlobalNodeId;
use crate::circuit::checkpointer::Checkpointer;
use crate::circuit::circuit_builder::{CircuitHandle, ConcurrentRestoreOutcome};
use crate::circuit::metrics::{DBSP_STEP, DBSP_STEP_LATENCY_MICROSECONDS};
use crate::circuit::schedule::CommitProgress;
use crate::monitor::visual_graph::Graph;
use crate::operator::dynamic::balance::{BalancerHint, PartitioningPolicy};
use crate::storage::backend::StorageError;
use crate::trace::spine_async::MAX_LEVEL0_BATCH_SIZE_RECORDS;
use crate::{
    Error as DbspError, RootCircuit, Runtime, RuntimeError, circuit::runtime::RuntimeHandle,
    profile::Profiler,
};
use anyhow::Error as AnyError;
use crossbeam::channel::{Receiver, Select, Sender, TryRecvError, bounded};
use feldera_buffer_cache::ThreadType;
use feldera_ir::LirCircuit;
use feldera_storage::{FileCommitter, StorageBackend, StoragePath};
use feldera_types::checkpoint::CheckpointMetadata;
use feldera_types::config::DevTweaks;
use feldera_types::config::dev_tweaks::{BufferCacheAllocationStrategy, BufferCacheStrategy};
pub use feldera_types::config::{StorageCacheConfig, StorageConfig, StorageOptions};
use feldera_types::transaction::CommitProgressSummary;
use itertools::Either;
use std::collections::BTreeMap;
use std::net::TcpListener;
use std::num::NonZeroUsize;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{
    collections::HashSet,
    error::Error as StdError,
    fmt::{self, Debug, Display, Error as FmtError, Formatter},
    iter::empty,
    net::SocketAddr,
    ops::Range,
    path::{Path, PathBuf},
    thread::Result as ThreadResult,
    time::Instant,
};
use tracing::{debug, info};
use uuid::Uuid;

#[cfg(doc)]
use crate::circuit::circuit_builder::Stream;
use crate::profile::{DbspProfile, GraphProfile, WorkerProfile};

use super::SchedulerError;
use super::circuit_builder::BootstrapInfo;
use super::runtime::WorkerPanicInfo;

/// Default ratio of merger threads to worker threads.
const DEFAULT_MERGER_THREAD_RATIO: usize = 1;

/// A host for some workers in the [`Layout`] for a multi-host DBSP circuit.
#[allow(clippy::manual_non_exhaustive)]
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct Host {
    /// The IP address and TCP port on which the host listens and to which the
    /// other hosts connect.
    pub address: SocketAddr,

    /// The worker thread IDs implemented on this host.  Worker thread IDs start
    /// with 0 in the first host and increase sequentially from there.  A host
    /// has `workers.len()` workers.
    pub workers: Range<usize>,

    /// Prevents `Host` and `Layout::Multihost` from being instantiated without
    /// using the constructor (which checks the invariants).
    _private: (),
}

impl Debug for Host {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Host")
            .field("address", &self.address)
            .field("workers", &self.workers)
            .finish()
    }
}

/// How a DBSP circuit is laid out across one or more machines.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum Layout {
    /// A layout whose workers run on a single host.
    Solo {
        /// The number of worker threads.
        n_workers: usize,
    },

    /// A layout across multiple machines.
    Multihost {
        /// Each host in the layout.  There should be two or more, each with a
        /// unique network address.
        hosts: Vec<Host>,

        /// The index within `hosts` of the current host.
        local_host_idx: usize,
    },
}

impl Layout {
    /// Returns a new solo layout with `n_workers` worker threads.
    pub fn new_solo(n_workers: usize) -> Layout {
        assert_ne!(n_workers, 0);
        Layout::Solo { n_workers }
    }

    /// Returns a new multihost layout with as many hosts as specified in
    /// `params`.  Each tuple in `params` specifies a host's unique network
    /// address and the number of workers to run on that host.  `local_address`
    /// must be one of the addresses in `params`.
    ///
    /// To execute such a multihost circuit, one must create a `Runtime` for it
    /// on every host in `params`, passing the same `params` in each case.  Each
    /// host must pass its own `local_address`.  The `Runtime` on each host
    /// listens on its own address and connects to all the other addresses.
    pub fn new_multihost(
        params: &[(SocketAddr, usize)],
        local_address: SocketAddr,
    ) -> Result<Layout, LayoutError> {
        // Check that the addresses are unique.
        let mut uniq = HashSet::new();
        if let Some((duplicate, _)) = params.iter().find(|(address, _)| !uniq.insert(address)) {
            return Err(LayoutError::DuplicateAddress(*duplicate));
        }

        // Find `local_address` in `params`.
        let local_host_idx = params
            .iter()
            .position(|(address, _)| *address == local_address)
            .ok_or(LayoutError::NoSuchAddress(local_address))?;

        if params.len() == 1 {
            Ok(Self::new_solo(params[0].1))
        } else {
            let mut hosts = Vec::with_capacity(params.len());
            let mut total_workers = 0;
            for (address, n_workers) in params {
                assert_ne!(*n_workers, 0);
                hosts.push(Host {
                    address: *address,
                    workers: total_workers..total_workers + *n_workers,
                    _private: (),
                });
                total_workers += *n_workers;
            }
            Ok(Layout::Multihost {
                hosts,
                local_host_idx,
            })
        }
    }

    /// Returns the range of IDs for the workers on the local machine.
    pub fn local_workers(&self) -> Range<usize> {
        match self {
            Self::Solo { n_workers } => 0..*n_workers,
            Self::Multihost {
                hosts,
                local_host_idx,
                ..
            } => hosts[*local_host_idx].workers.clone(),
        }
    }

    /// Returns an iterator over `Host`s in this layout other than this one.  If
    /// this is a single-host layout, this will be an empty iterator.
    pub fn other_hosts(&self) -> impl Iterator<Item = &Host> {
        match self {
            Self::Solo { .. } => Either::Left(empty()),
            Self::Multihost {
                hosts,
                local_host_idx,
            } => Either::Right(
                hosts
                    .iter()
                    .enumerate()
                    .filter_map(|(i, host)| (i != *local_host_idx).then_some(host)),
            ),
        }
    }

    /// Returns an iterator across the ranges of workers on all the hosts in
    /// this layout.  A single-host layout will have a single range with all the
    /// workers, a two-host layout will have two ranges, and so on.
    pub fn all_hosts(&self) -> impl Iterator<Item = Range<usize>> {
        match self {
            Self::Solo { n_workers } => Either::Left(std::iter::once(0..*n_workers)),
            Self::Multihost { hosts, .. } => {
                Either::Right(hosts.iter().map(|host| host.workers.clone()))
            }
        }
    }

    /// Returns the network address for the current machine, or `None` if this
    /// is a solo layout.
    pub fn local_address(&self) -> Option<SocketAddr> {
        match self {
            Self::Solo { .. } => None,
            Self::Multihost {
                hosts,
                local_host_idx,
                ..
            } => Some(hosts[*local_host_idx].address),
        }
    }

    /// Returns the total number of worker threads in this layout.
    pub fn n_workers(&self) -> usize {
        match self {
            Self::Solo { n_workers } => *n_workers,
            Self::Multihost { hosts, .. } => hosts.iter().map(|host| host.workers.len()).sum(),
        }
    }

    pub fn is_multihost(&self) -> bool {
        matches!(self, Self::Multihost { .. })
    }

    pub fn is_solo(&self) -> bool {
        matches!(self, Self::Solo { .. })
    }

    pub fn n_hosts(&self) -> usize {
        match self {
            Layout::Solo { .. } => 1,
            Layout::Multihost { hosts, .. } => hosts.len(),
        }
    }

    pub fn local_host_idx(&self) -> usize {
        match self {
            Layout::Solo { .. } => 0,
            Layout::Multihost { local_host_idx, .. } => *local_host_idx,
        }
    }

    /// Returns [`HostInfo`] for this host when running in multihost mode,
    /// or `None` for solo pipelines.
    ///
    /// [`HostInfo`]: feldera_types::checkpoint::HostInfo
    pub fn host_info(&self) -> Option<feldera_types::checkpoint::HostInfo> {
        match self {
            Layout::Solo { .. } => None,
            Layout::Multihost {
                hosts,
                local_host_idx,
            } => Some(feldera_types::checkpoint::HostInfo {
                host_idx: *local_host_idx,
                n_hosts: hosts.len(),
            }),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub enum LayoutError {
    /// The socket address passed to `new_multihost()` isn't in the list of
    /// hosts.
    NoSuchAddress(SocketAddr),
    /// The list of socket addresses passed to `new_multihost()` contains a
    /// duplicate.
    DuplicateAddress(SocketAddr),
}

impl Display for LayoutError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), FmtError> {
        match self {
            Self::NoSuchAddress(address) => write!(f, "address {address} not in list of hosts"),
            Self::DuplicateAddress(address) => {
                write!(f, "duplicate address {address} in list of hosts")
            }
        }
    }
}

impl StdError for LayoutError {}

/// DBSP circuit execution mode.
#[derive(Copy, Clone, Default, PartialEq, Eq, Debug)]
pub enum Mode {
    /// Operators in the circuit have persistent id's.
    ///
    /// In this mode, operators are assigned persistent id's by the compiler. These id's
    /// are preserved across circuit restarts for parts of the circuit that remain
    /// unmodified.  This allows the circuit to partially or completely restore its state from
    /// a checkpoint.
    ///
    /// In persistent mode, the circuit may fail to start if not all operators are assigned
    /// persistent id's.
    Persistent,

    /// Circuit operators are assigned ephemeral id's.
    ///
    /// In this mode, operators are not assigned persistent id's. The circuit can only
    /// restore its state from a checkpoint if the entire circuit is unmodified.
    #[default]
    Ephemeral,
}

/// Whether the circuit runs with microsteps or full steps.
#[derive(Copy, Clone, Default, PartialEq, Eq, Debug)]
pub enum StepSize {
    /// The circuit supports microsteps.
    ///
    /// Microsteps break large batches that flow between operators into smaller
    /// batches, which increases performance.  The common case for DBSP is
    /// incremental circuits that support microsteps.
    #[default]
    Microsteps,

    /// The circuit does not support microsteps.
    ///
    /// This setting disables breaking large batches into smaller ones for
    /// performance.  It also disables optimizations in the ShardAccumulate
    /// operator that can cause batches of any size to be processed in multiple
    /// microsteps.
    ///
    /// Nonincremental circuits need this setting because their steps cannot be
    /// divided into microsteps without changing the results.
    FullSteps,
}

/// A config for instantiating a multithreaded/multihost runtime to execute
/// circuits.
///
/// As opposed to `RuntimeConfig`, this struct stores state about which hosts
/// run the circuit and where they store data, e.g., state typically not
/// tunable/exposed by the user.
pub struct CircuitConfig {
    /// How the circuit is laid out across one or multiple machines.
    pub layout: Layout,

    /// The maximum amount of memory, in bytes, that the process is allowed to use.
    /// Used to calculate the memory pressure level.
    pub max_rss_bytes: Option<u64>,

    /// Optionally, CPU numbers for pinning the worker threads.
    pub pin_cpus: Vec<usize>,

    pub mode: Mode,

    /// Whether the circuit can use microsteps.
    pub step_size: StepSize,

    /// Storage configuration. If present, then storage is enabled.
    pub storage: Option<CircuitStorageConfig>,

    /// Parsed from `RuntimeConfig` for use by the circuit.
    pub dev_tweaks: DevTweaks,

    /// Optionally, the TCP socket on which to listen for exchange.  This is
    /// relevant only if `layout` is multihost.  If it is provided, then the
    /// socket must be listening on the port indicated for this host in
    /// `layout`.
    pub exchange_listener: Option<TcpListener>,
}

/// Returns the chunk size for splitter operators, in records.
///
/// Operators that split their output into multiple chunks, such as joins,
/// distinct, and aggregation, should attempt to limit their output to this
/// chunk size.
pub fn splitter_output_chunk_size() -> usize {
    Runtime::with_dev_tweaks(|d| d.splitter_chunk_size_records() as usize)
}

/// Returns the number of records to preallocate in the first iteration of loops
/// that break records into groups by the chunk size.
///
/// Operators that split their output into multiple chunks, such as joins,
/// distinct, and aggregation, should attempt to limit their output to the chunk
/// size returned by [splitter_output_chunk_size].  However,
/// [splitter_output_chunk_size] can return an arbitrarily large value, even
/// [usize::MAX].  This means that blindly allocating enough memory for the
/// specified number of records can panic.  Instead, code should allocate at
/// most `splitter_output_first_chunk_size` records in the first iteration of a
/// loop that splits records by the chunk size.  In later iterations, after the
/// configured chunk size has been reached once (and thus one knows that the
/// given number of records fits in memory), the code can use the configured
/// chunk size directly.
pub fn splitter_output_first_chunk_size() -> usize {
    splitter_output_chunk_size().min(MAX_FIRST_CHUNK_SIZE)
}

/// The chunk size returned by [splitter_output_chunk_size] can be arbitrarily
/// large, even [usize::MAX].  This means that blindly allocating enough memory
/// for the specified number of records can panic.  Instead, code should
/// allocate at most memory for `MAX_FIRST_CHUNK_SIZE` records in the first
/// iteration of a loop that splits records by the chunk size.  In later
/// iterations, after the configured chunk size has been reached once (and thus
/// one knows that the given number of records fits in memory), the code can use
/// the configured chunk size directly.
pub const MAX_FIRST_CHUNK_SIZE: usize = 50_000;

pub fn balancer_min_absolute_improvement_threshold() -> u64 {
    Runtime::with_dev_tweaks(|d| d.balancer_min_absolute_improvement_threshold())
}

pub fn balancer_min_relative_improvement_threshold() -> f64 {
    Runtime::with_dev_tweaks(|d| d.balancer_min_relative_improvement_threshold())
}

pub fn balancer_balance_tax() -> f64 {
    Runtime::with_dev_tweaks(|d| d.balancer_balance_tax())
}

pub fn balancer_key_distribution_refresh_threshold() -> f64 {
    Runtime::with_dev_tweaks(|d| d.balancer_key_distribution_refresh_threshold())
}

pub fn adaptive_joins_enabled() -> bool {
    Runtime::with_dev_tweaks(|d| d.adaptive_joins())
}

pub fn max_level0_batch_size_records() -> u16 {
    Runtime::with_dev_tweaks(|d| {
        d.max_level0_batch_size_records
            .unwrap_or(MAX_LEVEL0_BATCH_SIZE_RECORDS)
    })
}

pub fn negative_weight_multiplier() -> u16 {
    Runtime::with_dev_tweaks(|d| d.negative_weight_multiplier())
}

/// Configuration for storage in a [Runtime]-hosted circuit.
#[derive(Clone, derive_more::Debug)]
pub struct CircuitStorageConfig {
    /// Runner configuration.
    pub config: StorageConfig,

    /// User options.
    pub options: StorageOptions,

    /// Storage backend.
    ///
    /// Presumably opened according to `config` and `options`.
    #[debug(skip)]
    pub backend: Arc<dyn StorageBackend>,

    /// The initial checkpoint to start the circuit from, or `None` to start
    /// fresh from a new circuit.
    pub init_checkpoint: Option<Uuid>,

    /// Suppress the automatic stop-the-world restore from `init_checkpoint`
    /// during [`Runtime::init_circuit`], so the caller can drive a concurrent
    /// bootstrap via [`DBSPHandle::start_concurrent_bootstrap`] instead.
    ///
    /// `init_checkpoint` is still carried (the caller needs it as the
    /// concurrent-bootstrap base), but the circuit comes up un-restored.
    pub defer_restore: bool,
}

impl CircuitStorageConfig {
    /// Opens a backend with `config` and `options` and returns a
    /// [CircuitStorageConfig] with that backend.
    pub fn for_config(
        config: StorageConfig,
        options: StorageOptions,
    ) -> Result<Self, StorageError> {
        let backend = <dyn StorageBackend>::new(&config, &options)?;
        Ok(Self {
            config,
            options,
            backend,
            init_checkpoint: None,
            defer_restore: false,
        })
    }

    pub fn with_init_checkpoint(self, init_checkpoint: Option<Uuid>) -> Self {
        Self {
            init_checkpoint,
            ..self
        }
    }

    pub fn with_defer_restore(self, defer_restore: bool) -> Self {
        Self {
            defer_restore,
            ..self
        }
    }
}

impl Default for CircuitConfig {
    fn default() -> Self {
        Self::with_workers(1)
    }
}

impl CircuitConfig {
    pub fn with_workers(n: usize) -> Self {
        Self {
            layout: Layout::new_solo(n),
            max_rss_bytes: None,
            pin_cpus: Vec::new(),
            mode: Mode::Ephemeral,
            step_size: StepSize::default(),
            storage: None,
            dev_tweaks: DevTweaks::default(),
            exchange_listener: None,
        }
    }

    pub fn with_max_rss_bytes(mut self, max_rss: Option<u64>) -> Self {
        self.max_rss_bytes = max_rss;
        self
    }

    pub fn with_mode(mut self, mode: Mode) -> Self {
        self.mode = mode;
        self
    }

    pub fn with_step_size(mut self, step_size: StepSize) -> Self {
        self.step_size = step_size;
        self
    }

    pub fn with_pin_cpus(mut self, pin_cpus: Vec<usize>) -> Self {
        self.pin_cpus = pin_cpus;
        self
    }

    pub fn with_storage(mut self, storage: Option<CircuitStorageConfig>) -> Self {
        self.storage = storage;
        self
    }

    pub fn with_dev_tweaks(mut self, dev_tweaks: DevTweaks) -> Self {
        self.dev_tweaks = dev_tweaks;
        self
    }

    pub fn with_streaming_exchange(mut self, enabled: bool) -> Self {
        self.dev_tweaks.streaming_exchange = Some(enabled);
        self
    }

    pub fn with_splitter_chunk_size_records(mut self, records: u64) -> Self {
        self.dev_tweaks.splitter_chunk_size_records = Some(records);
        self
    }

    pub fn with_buffer_cache_strategy(mut self, strategy: BufferCacheStrategy) -> Self {
        self.dev_tweaks.buffer_cache_strategy = Some(strategy);
        self
    }

    pub fn with_buffer_max_buckets(mut self, max_buckets: Option<usize>) -> Self {
        self.dev_tweaks.buffer_max_buckets = max_buckets;
        self
    }

    pub fn with_buffer_cache_allocation_strategy(
        mut self,
        strategy: BufferCacheAllocationStrategy,
    ) -> Self {
        self.dev_tweaks.buffer_cache_allocation_strategy = Some(strategy);
        self
    }

    #[cfg(test)]
    pub fn with_fbuf_slab_bytes_per_class(mut self, bytes_per_class: usize) -> Self {
        self.dev_tweaks.fbuf_slab_bytes_per_class = Some(bytes_per_class);
        self
    }

    pub fn with_balancer_min_relative_improvement_threshold(mut self, threshold: f64) -> Self {
        self.dev_tweaks.balancer_min_relative_improvement_threshold = Some(threshold);
        self
    }

    pub fn with_balancer_min_absolute_improvement_threshold(mut self, threshold: u64) -> Self {
        self.dev_tweaks.balancer_min_absolute_improvement_threshold = Some(threshold);
        self
    }

    pub fn with_balancer_balance_tax(mut self, tax: f64) -> Self {
        self.dev_tweaks.balancer_balance_tax = Some(tax);
        self
    }

    #[cfg(test)]
    pub fn with_temporary_storage(self, path: impl AsRef<Path>) -> Self {
        Self {
            storage: Some(
                CircuitStorageConfig::for_config(
                    StorageConfig {
                        path: path.as_ref().to_string_lossy().into_owned(),
                        cache: Default::default(),
                    },
                    Default::default(),
                )
                .unwrap(),
            ),
            ..self
        }
    }

    /// The number of merger threads per host.
    pub(crate) fn num_merger_threads(&self) -> usize {
        let num_workers = self.layout.local_workers().len();
        match self.dev_tweaks.merger_threads {
            Some(threads) => threads as usize,
            None => num_workers * DEFAULT_MERGER_THREAD_RATIO,
        }
    }

    pub fn with_exchange_listener(mut self, exchange_listener: TcpListener) -> Self {
        self.exchange_listener = Some(exchange_listener);
        self
    }
}

impl From<usize> for CircuitConfig {
    fn from(n_workers: usize) -> Self {
        Self::with_workers(n_workers)
    }
}

impl From<NonZeroUsize> for CircuitConfig {
    fn from(n_workers: NonZeroUsize) -> Self {
        Self::with_workers(n_workers.get())
    }
}

impl From<Layout> for CircuitConfig {
    fn from(layout: Layout) -> Self {
        Self {
            layout,
            ..Self::default()
        }
    }
}

impl Runtime {
    /// Instantiate a circuit in a multithreaded runtime.
    ///
    /// Creates a multithreaded runtime with the given `layout`, instantiates
    /// an identical circuit in each worker, by calling `constructor` once per
    /// worker.  `init_circuit` passes each call of `constructor` a new
    /// [`RootCircuit`], in which it should create input operators by calling
    /// [`RootCircuit::dyn_add_input_zset`] and related methods.  Each of these
    /// calls returns an input handle and a `Stream`.  The `constructor` can
    /// call [`Stream`] methods to construct more operators, each of which
    /// yields further `Stream`s.  It can also use [`Stream::output`] to obtain
    /// an output handle.
    ///
    /// The `layout` may be specified as a number of worker threads or as a
    /// [`Layout`].
    ///
    /// Returns a [`DBSPHandle`] that the caller can use to control the circuit
    /// and a user-defined value returned by the constructor.  The
    /// `constructor` should use the latter to return the input and output
    /// handles it obtains, because these allow the caller to feed input into
    /// the circuit and read output from the circuit.
    ///
    /// To ensure that the multithreaded runtime has identical input/output
    /// behavior to a single-threaded circuit, the `constructor` closure
    /// must satisfy certain constraints.  Most importantly, it must create
    /// identical circuits in all worker threads, adding and connecting
    /// operators in the same order.  This ensures that operators that shard
    /// their inputs across workers, e.g.,
    /// [`Stream::join`](`crate::Stream::join`), work correctly.
    /// The closure should return the same value in each worker thread; this
    /// function returns one of these values arbitrarily.
    ///
    /// TODO: Document other requirements.  Not all operators are currently
    /// thread-safe.
    pub fn init_circuit<F, T>(
        config: impl Into<CircuitConfig>,
        constructor: F,
    ) -> Result<(DBSPHandle, T), DbspError>
    where
        F: FnOnce(&mut RootCircuit) -> Result<T, AnyError> + Clone + Send + 'static,
        T: Send + 'static,
    {
        let config: CircuitConfig = config.into();
        let nworkers = config.layout.local_workers().len();

        // When a worker finishes building the circuit, it sends completion status back
        // to us via this channel.  The function returns after receiving a
        // notification from each worker.
        let (init_senders, init_receivers): (Vec<_>, Vec<_>) =
            (0..nworkers).map(|_| bounded(0)).unzip();

        // Channels used to send commands to workers.
        let (command_senders, command_receivers): (Vec<_>, Vec<_>) =
            (0..nworkers).map(|_| bounded(1)).unzip();

        // Channels used to signal command completion to the client.
        let (status_senders, status_receivers): (Vec<_>, Vec<_>) =
            (0..nworkers).map(|_| bounded(1)).unzip();

        let storage = config.storage.clone();

        let runtime = Self::run(config, move |parker| {
            let worker_index = Runtime::local_worker_offset();

            // Drop all but one channels.  This makes sure that if one of the worker panics
            // or exits, its channel will become disconnected.
            let init_sender = init_senders.into_iter().nth(worker_index).unwrap();
            let status_sender = status_senders.into_iter().nth(worker_index).unwrap();
            let command_receiver = command_receivers.into_iter().nth(worker_index).unwrap();

            // Retain a copy of the constructor: `Command::CreateBootstrapCircuit`
            // re-runs it to build a structurally identical second circuit.
            let bootstrap_constructor = constructor.clone();

            let circuit_fn = |circuit: &mut RootCircuit| {
                let profiler = Profiler::new(circuit);
                constructor(circuit).map(|res| (res, profiler))
            };
            let (mut circuit, profiler) = match RootCircuit::build(circuit_fn) {
                Ok((circuit, (res, profiler))) => {
                    if init_sender.send(Ok((res, circuit.fingerprint()))).is_err() {
                        return;
                    }
                    (circuit, profiler)
                }
                Err(e) => {
                    let _ = init_sender.send(Err(e));
                    return;
                }
            };

            // The bootstrap circuit (the second copy of the circuit used by
            // concurrent bootstrapping), when one exists.  All workers
            // create, step, and destroy their bootstrap circuits in lockstep
            // through broadcast commands; this keeps the per-worker
            // `Runtime::sequence_next` counters, which assign exchange ids,
            // identical across workers.
            let mut bootstrap_circuit: Option<CircuitHandle> = None;

            while !Runtime::kill_in_progress() {
                // Wait for command.
                match command_receiver.try_recv() {
                    Ok(Command::Transaction) => {
                        let status = circuit.transaction().map(|_| Response::Unit);
                        // Send response.
                        if status_sender.send(status).is_err() {
                            return;
                        }
                    }
                    Ok(Command::StartTransaction) => {
                        let status = circuit.start_transaction().map(|_| Response::Unit);
                        // Send response.
                        if status_sender.send(status).is_err() {
                            return;
                        }
                    }
                    Ok(Command::CommitTransaction) => {
                        let status = circuit.start_commit_transaction().map(|_| Response::Unit);
                        // Send response.
                        if status_sender.send(status).is_err() {
                            return;
                        }
                    }
                    Ok(Command::CommitProgress) => {
                        let status = Ok(Response::CommitProgress(circuit.commit_progress()));
                        // Send response.
                        if status_sender.send(status).is_err() {
                            return;
                        }
                    }
                    Ok(Command::Step) => {
                        let status = circuit
                            .step()
                            .map(|_| Response::CommitComplete(circuit.is_commit_complete()));
                        // Send response.
                        if status_sender.send(status).is_err() {
                            return;
                        }
                    }
                    Ok(Command::IsBootstrapComplete) => {
                        let complete = circuit.is_replay_complete();
                        if status_sender
                            .send(Ok(Response::BootstrapComplete(complete)))
                            .is_err()
                        {
                            return;
                        };
                    }
                    Ok(Command::CompleteBootstrap) => {
                        let status = circuit.complete_replay().map(|_| Response::Unit);
                        if status_sender.send(status).is_err() {
                            return;
                        }
                    }
                    Ok(Command::EnableProfiler) => {
                        profiler.enable_cpu_profiler();
                        // Send response.
                        if status_sender.send(Ok(Response::Unit)).is_err() {
                            return;
                        }
                    }
                    Ok(Command::DumpProfile { runtime_elapsed }) => {
                        if status_sender
                            .send(Ok(Response::ProfileDump(
                                profiler.dump_profile(runtime_elapsed),
                            )))
                            .is_err()
                        {
                            return;
                        }
                    }
                    Ok(Command::RetrieveGraph) => {
                        // This is implemented by just asking the profiler to dump the graph
                        // without any metadata.
                        if status_sender
                            .send(Ok(Response::ProfileDump(profiler.dump_graph())))
                            .is_err()
                        {
                            return;
                        }
                    }
                    Ok(Command::RetrieveProfile { runtime_elapsed }) => {
                        if status_sender
                            .send(Ok(Response::Profile(profiler.profile(runtime_elapsed))))
                            .is_err()
                        {
                            return;
                        }
                    }
                    Ok(Command::Checkpoint(base)) => {
                        let mut files = Vec::new();
                        let response = circuit
                            .checkpoint(&base, &mut files)
                            .map(|_| Response::CheckpointCreated(files));
                        if status_sender.send(response).is_err() {
                            return;
                        }
                    }
                    Ok(Command::Restore(base)) => {
                        let result = circuit
                            .restore(&base, false)
                            .map(Response::CheckpointRestored);
                        if status_sender.send(result).is_err() {
                            return;
                        }
                    }
                    Ok(Command::GetLir) => {
                        let lir = circuit.lir();
                        if status_sender.send(Ok(Response::Lir(lir))).is_err() {
                            return;
                        }
                    }
                    Ok(Command::SetAutoRebalance(enable)) => {
                        let result = circuit.set_auto_rebalance(enable).map(|_| Response::Unit);
                        if status_sender.send(result).is_err() {
                            return;
                        }
                    }
                    Ok(Command::SetBalancerHintsByGlobalId(hints)) => {
                        let results = hints
                            .into_iter()
                            .map(|(global_node_id, hint)| {
                                circuit.set_balancer_hint_by_global_id(&global_node_id, hint)
                            })
                            .collect::<Vec<Result<(), DbspError>>>();
                        if status_sender
                            .send(Ok(Response::SetBalancerHints(results)))
                            .is_err()
                        {
                            return;
                        }
                    }
                    Ok(Command::SetBalancerHints(hints)) => {
                        let results = hints
                            .into_iter()
                            .map(|(persistent_id, hint)| {
                                circuit.set_balancer_hint(&persistent_id, hint)
                            })
                            .collect::<Vec<Result<(), DbspError>>>();
                        if status_sender
                            .send(Ok(Response::SetBalancerHints(results)))
                            .is_err()
                        {
                            return;
                        }
                    }
                    Ok(Command::GetCurrentBalancerPolicies) => {
                        let policy = circuit.get_current_balancer_policies();
                        if status_sender
                            .send(Ok(Response::CurrentBalancerPolicies(policy)))
                            .is_err()
                        {
                            return;
                        }
                    }
                    Ok(Command::GetCurrentBalancerPolicy(persistent_id)) => {
                        let policy = circuit.get_current_balancer_policy(&persistent_id);
                        if status_sender
                            .send(Ok(Response::CurrentBalancerPolicy(policy)))
                            .is_err()
                        {
                            return;
                        }
                    }
                    Ok(Command::Rebalance) => {
                        circuit.rebalance();
                        if status_sender.send(Ok(Response::Unit)).is_err() {
                            return;
                        }
                    }
                    Ok(Command::StartCompaction) => {
                        circuit.start_compaction();
                        if status_sender.send(Ok(Response::Unit)).is_err() {
                            return;
                        }
                    }
                    Ok(Command::IsCompactionComplete) => {
                        let complete = circuit.is_compaction_complete();
                        if status_sender
                            .send(Ok(Response::IsCompactionComplete(complete)))
                            .is_err() {
                            return;
                        }
                    }
                    Ok(Command::CreateBootstrapCircuit(checkpoint)) => {
                        let status = if bootstrap_circuit.is_some() {
                            Err(DbspError::Runtime(RuntimeError::BootstrapCircuit(
                                "a bootstrap circuit already exists".to_string(),
                            )))
                        } else {
                            // Re-running the constructor produces a circuit
                            // whose nodes have the same `NodeId`s as the main
                            // circuit's, which is what lets bootstrapped
                            // state move between the copies.  The input and
                            // output handles it creates are fresh objects
                            // that the runtime's local store retains but
                            // nothing feeds or drains; the bootstrap
                            // circuit's inputs are driven by replay sources
                            // instead.  No profiler is attached: profiler
                            // hooks are circuit event handlers and do not
                            // affect node ids.
                            let constructor = bootstrap_constructor.clone();
                            RootCircuit::build(move |circuit| constructor(circuit)).and_then(
                                |(mut handle, _catalog)| {
                                    // A fingerprint mismatch means the
                                    // constructor is nondeterministic, which
                                    // also desyncs the per-worker sequence
                                    // counters; fail (fatally, by the
                                    // command protocol) rather than letting
                                    // the exchanges deadlock or cross-wire.
                                    if handle.fingerprint() != circuit.fingerprint() {
                                        return Err(DbspError::Runtime(
                                            RuntimeError::BootstrapCircuit(
                                                "the bootstrap circuit's fingerprint differs \
                                                 from the main circuit's: the circuit \
                                                 constructor is nondeterministic"
                                                    .to_string(),
                                            ),
                                        ));
                                    }
                                    // This handle is the bootstrap copy
                                    // (copy 2): its backfilled output
                                    // operators cache their output for
                                    // transfer at cutover.
                                    let replay_info = match &checkpoint {
                                        Some(base) => handle.restore(base, true)?,
                                        None => None,
                                    };
                                    bootstrap_circuit = Some(handle);
                                    Ok(Response::CheckpointRestored(replay_info))
                                },
                            )
                        };
                        if status_sender.send(status).is_err() {
                            return;
                        }
                    }
                    Ok(Command::StartBootstrapTransaction) => {
                        let status = match bootstrap_circuit.as_mut() {
                            Some(circuit) => circuit.start_transaction().map(|_| Response::Unit),
                            None => Err(no_bootstrap_circuit()),
                        };
                        if status_sender.send(status).is_err() {
                            return;
                        }
                    }
                    Ok(Command::CommitBootstrapTransaction) => {
                        let status = match bootstrap_circuit.as_mut() {
                            Some(circuit) => {
                                circuit.start_commit_transaction().map(|_| Response::Unit)
                            }
                            None => Err(no_bootstrap_circuit()),
                        };
                        if status_sender.send(status).is_err() {
                            return;
                        }
                    }
                    Ok(Command::StepBootstrapCircuit) => {
                        let status = match bootstrap_circuit.as_mut() {
                            Some(circuit) => circuit
                                .step()
                                .map(|_| Response::CommitComplete(circuit.is_commit_complete())),
                            None => Err(no_bootstrap_circuit()),
                        };
                        if status_sender.send(status).is_err() {
                            return;
                        }
                    }
                    Ok(Command::BootstrapCommitProgress) => {
                        let status = match bootstrap_circuit.as_ref() {
                            Some(circuit) => {
                                Ok(Response::CommitProgress(circuit.commit_progress()))
                            }
                            None => Err(no_bootstrap_circuit()),
                        };
                        if status_sender.send(status).is_err() {
                            return;
                        }
                    }
                    Ok(Command::DestroyBootstrapCircuit) => {
                        // Dropping the handle runs the circuit's teardown
                        // (`clock_end` plus breaking `Rc` cycles).
                        let status = match bootstrap_circuit.take() {
                            Some(_circuit) => {
                                // If this destroy aborts a concurrent
                                // bootstrap, disarm the main circuit's
                                // recorders so they stop accumulating.
                                circuit.abort_concurrent_bootstrap();
                                Ok(Response::Unit)
                            }
                            None => Err(no_bootstrap_circuit()),
                        };
                        if status_sender.send(status).is_err() {
                            return;
                        }
                    }
                    Ok(Command::RestoreConcurrent(base)) => {
                        let result = circuit
                            .restore_concurrent(&base)
                            .map(Response::ConcurrentRestore);
                        if status_sender.send(result).is_err() {
                            return;
                        }
                    }
                    Ok(Command::SyncBootstrapCircuit) => {
                        let status = match bootstrap_circuit.as_mut() {
                            Some(bootstrap) => circuit.drain_recorders().and_then(|recorded| {
                                bootstrap.start_sync_replay(recorded)?;
                                bootstrap.start_transaction()?;
                                bootstrap.start_commit_transaction()?;
                                Ok(Response::Unit)
                            }),
                            None => Err(no_bootstrap_circuit()),
                        };
                        if status_sender.send(status).is_err() {
                            return;
                        }
                    }
                    Ok(Command::CutoverBootstrapCircuit) => {
                        let status = match bootstrap_circuit.take() {
                            Some(bootstrap) => {
                                // Dropping `bootstrap` after the state
                                // transfer runs the bootstrap circuit's
                                // teardown.
                                circuit.cutover_from(&bootstrap).map(|_| Response::Unit)
                            }
                            None => Err(no_bootstrap_circuit()),
                        };
                        if status_sender.send(status).is_err() {
                            return;
                        }
                    }
                    // Nothing to do: do some housekeeping and relinquish the CPU if there's none
                    // left.
                    Err(TryRecvError::Empty) => {
                        parker.park();
                    }
                    Err(_) => {
                        break;
                    }
                }
            }
        })?;

        // Receive initialization status from all workers and take the return
        // value and fingerprint from the first worker, or the first error among
        // all the workers.
        //
        // The return value should return be the same in all workers (we don't
        // check).
        //
        // The fingerprint might differ between workers (even if the circuit is
        // really the same, because of differing effects of e.g. the "gather"
        // operator).
        let result = init_receivers
            .into_iter()
            .map(|receiver| {
                receiver.recv().unwrap_or_else(|_| {
                    Err(DbspError::Runtime(RuntimeError::WorkerPanic {
                        panic_info: runtime.collect_panic_info(),
                    }))
                })
            })
            .reduce(|old, new| {
                if old.is_ok() && new.is_err() {
                    new
                } else {
                    old
                }
            })
            .unwrap();
        let (ret, fingerprint) = match result {
            Err(error) => {
                let _ = runtime.kill();
                return Err(error);
            }
            Ok(result) => result,
        };

        let (backend, init_checkpoint, defer_restore) = match storage {
            Some(storage) => (
                Some(storage.backend.clone()),
                storage.init_checkpoint,
                storage.defer_restore,
            ),
            None => (None, None, false),
        };
        let mut dbsp = DBSPHandle::new(
            backend,
            runtime,
            command_senders,
            status_receivers,
            fingerprint,
        )?;
        // When `defer_restore` is set the caller drives a concurrent bootstrap
        // (`start_concurrent_bootstrap`) instead of the automatic stop-the-world
        // restore; the circuit comes up un-restored and the caller supplies the
        // checkpoint base itself.
        if let Some(init_checkpoint) = init_checkpoint
            && !defer_restore
        {
            dbsp.send_restore(init_checkpoint.to_string().into())?;
        }

        Ok((dbsp, ret))
    }
}

/// The error returned by bootstrap-circuit commands when no bootstrap
/// circuit exists.
fn no_bootstrap_circuit() -> DbspError {
    DbspError::Runtime(RuntimeError::BootstrapCircuit(
        "no bootstrap circuit exists".to_string(),
    ))
}

/// The phase of an in-progress concurrent bootstrap, tracked on
/// [`DBSPHandle`] to order its orchestration methods.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ConcurrentBootstrapPhase {
    /// The bootstrap circuit is replaying checkpointed state in the
    /// background while the main circuit serves pre-existing views.
    Backfill,
    /// The recorders' contents are being replayed into the bootstrap
    /// circuit; the main circuit must not process new inputs.
    Synchronizing,
}

/// Coordinator-side mirror of the bootstrap circuit's lifecycle, tracked on
/// [`DBSPHandle`] to validate bootstrap-circuit commands before
/// broadcasting them.  Validation must happen on the coordinator: a worker
/// that responds with an error (e.g., `StepWithoutTransaction` from the
/// scheduler) kills the whole pipeline, including the healthy main circuit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BootstrapCircuitState {
    /// No bootstrap circuit exists.
    None,
    /// A bootstrap circuit exists, with no transaction in progress.
    Idle,
    /// A transaction is in progress on the bootstrap circuit.
    InTransaction,
    /// The bootstrap circuit's transaction is committing; stepping it
    /// returns `true` when the commit completes.
    Committing,
}

#[derive(Clone)]
enum Command {
    StartTransaction,
    Step,
    CommitTransaction,
    CommitProgress,
    Transaction,
    IsBootstrapComplete,
    CompleteBootstrap,
    EnableProfiler,
    DumpProfile {
        runtime_elapsed: Duration,
    },
    /// Retrieve the circuit graph
    RetrieveGraph,
    RetrieveProfile {
        runtime_elapsed: Duration,
    },
    GetLir,
    Checkpoint(StoragePath),
    Restore(StoragePath),
    SetBalancerHintsByGlobalId(Vec<(GlobalNodeId, BalancerHint)>),
    SetBalancerHints(Vec<(String, BalancerHint)>),
    GetCurrentBalancerPolicies,
    GetCurrentBalancerPolicy(String),
    Rebalance,
    SetAutoRebalance(bool),
    StartCompaction,
    IsCompactionComplete,
    /// Build the bootstrap circuit: a second copy of the circuit, built from
    /// the same constructor as the main one (see
    /// [`DBSPHandle::create_bootstrap_circuit`]).  When a checkpoint path is
    /// given, the bootstrap circuit is restored from it and prepared for
    /// replay (see [`CircuitHandle::restore`]).
    CreateBootstrapCircuit(Option<StoragePath>),
    StartBootstrapTransaction,
    CommitBootstrapTransaction,
    StepBootstrapCircuit,
    /// Report the bootstrap circuit's transaction-commit progress.
    BootstrapCommitProgress,
    DestroyBootstrapCircuit,
    /// Restore the main circuit from a checkpoint for concurrent
    /// bootstrapping (see [`CircuitHandle::restore_concurrent`]).
    RestoreConcurrent(StoragePath),
    /// Drain the main circuit's recorders into the bootstrap circuit's
    /// replay sources and start the synchronization transaction.
    SyncBootstrapCircuit,
    /// Install the bootstrap circuit's state into the main circuit,
    /// reactivate the full main circuit, and delete the bootstrap circuit.
    CutoverBootstrapCircuit,
}

impl Debug for Command {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Command::StartTransaction => write!(f, "StartTransaction"),
            Command::Step => write!(f, "Step"),
            Command::CommitTransaction => write!(f, "CommitTransaction"),
            Command::CommitProgress => write!(f, "CommitProgress"),
            Command::Transaction => write!(f, "Transaction"),
            Command::IsBootstrapComplete => write!(f, "IsBootstrapComplete"),
            Command::CompleteBootstrap => write!(f, "CompleteBootstrap"),
            Command::EnableProfiler => write!(f, "EnableProfiler"),
            Command::RetrieveGraph => write!(f, "RetrieveGraph"),
            Command::DumpProfile { runtime_elapsed } => f
                .debug_struct("DumpProfile")
                .field("runtime_elapsed", runtime_elapsed)
                .finish(),
            Command::RetrieveProfile { runtime_elapsed } => f
                .debug_struct("RetrieveProfile")
                .field("runtime_elapsed", runtime_elapsed)
                .finish(),
            Command::GetLir => write!(f, "GetLir"),
            Command::Checkpoint(path) => f.debug_tuple("Checkpoint").field(path).finish(),
            Command::Restore(path) => f.debug_tuple("Restore").field(path).finish(),
            Command::SetBalancerHintsByGlobalId(hints) => f
                .debug_tuple("SetBalancerHintsByGlobalId")
                .field(hints)
                .finish(),
            Command::SetBalancerHints(hints) => {
                f.debug_tuple("SetBalancerHints").field(hints).finish()
            }
            Command::GetCurrentBalancerPolicies => write!(f, "GetCurrentBalancerPolicies"),
            Command::GetCurrentBalancerPolicy(persistent_id) => f
                .debug_tuple("GetCurrentBalancerPolicy")
                .field(persistent_id)
                .finish(),
            Command::Rebalance => write!(f, "Rebalance"),
            Command::SetAutoRebalance(enable) => {
                f.debug_tuple("SetAutoRebalance").field(enable).finish()
            }
            Command::StartCompaction => write!(f, "StartCompaction"),
            Command::IsCompactionComplete => write!(f, "IsCompactionComplete"),
            Command::CreateBootstrapCircuit(checkpoint) => f
                .debug_tuple("CreateBootstrapCircuit")
                .field(checkpoint)
                .finish(),
            Command::StartBootstrapTransaction => write!(f, "StartBootstrapTransaction"),
            Command::CommitBootstrapTransaction => write!(f, "CommitBootstrapTransaction"),
            Command::StepBootstrapCircuit => write!(f, "StepBootstrapCircuit"),
            Command::BootstrapCommitProgress => write!(f, "BootstrapCommitProgress"),
            Command::DestroyBootstrapCircuit => write!(f, "DestroyBootstrapCircuit"),
            Command::RestoreConcurrent(path) => {
                f.debug_tuple("RestoreConcurrent").field(path).finish()
            }
            Command::SyncBootstrapCircuit => write!(f, "SyncBootstrapCircuit"),
            Command::CutoverBootstrapCircuit => write!(f, "CutoverBootstrapCircuit"),
        }
    }
}

#[derive(Debug)]
enum Response {
    Unit,
    CommitComplete(bool),
    BootstrapComplete(bool),
    IsCompactionComplete(bool),
    CommitProgress(CommitProgress),
    ProfileDump(Graph),
    Profile(WorkerProfile),
    CheckpointCreated(Vec<Arc<dyn FileCommitter>>),
    CheckpointRestored(Option<BootstrapInfo>),
    ConcurrentRestore(ConcurrentRestoreOutcome),
    Lir(LirCircuit),
    SetBalancerHints(Vec<Result<(), DbspError>>),
    CurrentBalancerPolicies(BTreeMap<GlobalNodeId, PartitioningPolicy>),
    CurrentBalancerPolicy(Result<PartitioningPolicy, DbspError>),
}

/// A handle to control the execution of a circuit in a multithreaded runtime.
#[derive(Debug)]
pub struct DBSPHandle {
    /// Time when the handle was created.
    start_time: Instant,

    /// Time elapsed while the circuit is executing a step, multiplied by the
    /// number of foreground and background threads.
    runtime_elapsed: Duration,

    /// The underlying runtime.
    ///
    /// Normally this will be some runtime, but we take it out if we need to
    /// kill the runtime.
    runtime: Option<RuntimeHandle>,

    /// Channels used to send commands to workers.
    command_senders: Vec<Sender<Command>>,

    /// Channels used to receive command completion status from workers.
    status_receivers: Vec<Receiver<Result<Response, DbspError>>>,

    /// For creating checkpoints, if we can.
    checkpointer: Option<Arc<Mutex<Checkpointer>>>,

    /// Circuit fingerprint.
    fingerprint: u64,

    /// Information about operators that participate in bootstrapping the new parts of the circuit.
    bootstrap_info: Option<BootstrapInfo>,

    /// The bootstrap circuit's lifecycle state (see
    /// [`Self::create_bootstrap_circuit`]).
    ///
    /// This handle is the only source of worker commands, so the mirror
    /// cannot diverge from the workers' state.  Checking preconditions here
    /// keeps them non-fatal: an `Err` response from a worker kills the
    /// circuit (see [`Self::broadcast_command`]).
    bootstrap_circuit_state: BootstrapCircuitState,

    /// Information about an in-progress *concurrent* bootstrap (see
    /// [`Self::start_concurrent_bootstrap`]).
    ///
    /// Kept separate from `bootstrap_info`, which describes a replay running
    /// in the main circuit and drives its auto-completion after every
    /// transaction; a concurrent bootstrap's replay runs in the bootstrap
    /// circuit and completes through its own commit.
    concurrent_bootstrap_info: Option<BootstrapInfo>,

    /// The phase of the in-progress concurrent bootstrap, `None` outside
    /// one.  Orders [`Self::sync_concurrent_bootstrap`] and
    /// [`Self::complete_concurrent_bootstrap`].
    concurrent_bootstrap_phase: Option<ConcurrentBootstrapPhase>,

    /// True while a transaction is open on the main circuit (started with
    /// [`Self::start_transaction`] and not yet committed).  This handle is
    /// the only source of worker commands, so the mirror cannot diverge.
    main_transaction_open: bool,

    /// Sticky flag set when a concurrent bootstrap is aborted (see
    /// [`Self::destroy_bootstrap_circuit`]).  The backfilled nodes hold no
    /// state, so a checkpoint taken now would record them as empty and
    /// permanently mask the missing backfill; checkpoints stay blocked
    /// until the process restarts from a checkpoint.
    concurrent_bootstrap_aborted: bool,
}
pub struct WorkersCommitProgress(BTreeMap<u16, CommitProgress>);

impl Default for WorkersCommitProgress {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkersCommitProgress {
    pub fn new() -> Self {
        WorkersCommitProgress(BTreeMap::new())
    }

    pub fn insert(&mut self, worker_id: u16, progress: CommitProgress) {
        debug_assert!(!self.0.contains_key(&worker_id));
        self.0.insert(worker_id, progress);
    }

    pub fn summary(&self) -> CommitProgressSummary {
        let mut result = CommitProgressSummary::new();

        for worker_progress in self.0.values() {
            // println!(
            //     "{worker} in progress: {}",
            //     worker_progress
            //         .get_in_progress()
            //         .keys()
            //         .map(|k| k.to_string())
            //         .join(", ")
            // );
            result.merge(&worker_progress.summary());
        }

        result
    }
}

impl DBSPHandle {
    fn new(
        backend: Option<Arc<dyn StorageBackend>>,
        runtime: RuntimeHandle,
        command_senders: Vec<Sender<Command>>,
        status_receivers: Vec<Receiver<Result<Response, DbspError>>>,
        fingerprint: u64,
    ) -> Result<Self, DbspError> {
        // TODO: We allow the circuit to change between suspend and resume in Persistent mode;
        // we therefore only validate the fingerprint in ephemeral mode; however it can sometimes
        // be useful to validate it in persistent mode too, e.g., when recovering after a crash.
        // In that case, do we need to change the checkpointer logic to only validate the
        // fingerprint of the last checkpoint (it currently checks all checkpoints in the backend
        // directory)?
        let checkpointer = backend
            .map(|backend| {
                let checkpointer = Checkpointer::new(backend)?;
                if runtime.runtime().get_mode() == Mode::Ephemeral {
                    checkpointer.verify_fingerprint(fingerprint)?;
                };
                Ok::<_, DbspError>(checkpointer)
            })
            .transpose()?
            .map(|checkpointer| Arc::new(Mutex::new(checkpointer)));
        Ok(Self {
            start_time: Instant::now(),
            runtime: Some(runtime),
            command_senders,
            status_receivers,
            checkpointer,
            fingerprint,
            runtime_elapsed: Duration::ZERO,
            bootstrap_info: None,
            bootstrap_circuit_state: BootstrapCircuitState::None,
            concurrent_bootstrap_info: None,
            concurrent_bootstrap_phase: None,
            main_transaction_open: false,
            concurrent_bootstrap_aborted: false,
        })
    }

    pub fn runtime(&self) -> &Runtime {
        self.runtime.as_ref().unwrap().runtime()
    }

    fn kill_inner(&mut self) -> ThreadResult<()> {
        self.command_senders.clear();
        self.status_receivers.clear();
        self.runtime.take().unwrap().kill()
    }

    fn kill_async(&mut self) {
        self.command_senders.clear();
        self.status_receivers.clear();
        self.runtime.take().unwrap().kill_async()
    }

    fn collect_panic_info(&self) -> Option<Vec<(usize, ThreadType, WorkerPanicInfo)>> {
        self.runtime
            .as_ref()
            .map(|runtime| runtime.collect_panic_info())
    }

    fn panicked(&self) -> bool {
        self.runtime
            .as_ref()
            .is_some_and(|runtime| runtime.panicked())
    }

    fn broadcast_command<F>(&mut self, command: Command, mut handler: F) -> Result<(), DbspError>
    where
        F: FnMut(usize, Response),
    {
        if self.runtime.is_none() {
            return Err(DbspError::Runtime(RuntimeError::Terminated));
        }

        // Send command.
        for (worker, sender) in self.command_senders.iter().enumerate() {
            if sender.send(command.clone()).is_err() {
                let panic_info = self.collect_panic_info().unwrap_or_default();

                // Worker thread panicked. Exit without waiting for all workers to exit
                // to avoid deadlocks due to workers waiting for each other.
                self.kill_async();
                return Err(DbspError::Runtime(RuntimeError::WorkerPanic { panic_info }));
            }
            self.runtime.as_ref().unwrap().unpark_worker(worker);
        }

        // Use `Select` to wait for responses from all workers simultaneously.
        // This way if one of the workers panics, leaving other workers waiting
        // for it in exchange operators, we won't deadlock waiting for these
        // workers.
        let mut select = Select::new();
        for receiver in self.status_receivers.iter() {
            select.recv(receiver);
        }

        fn handle_panic(this: &mut DBSPHandle) -> Result<(), DbspError> {
            // Retrieve panic info before killing the circuit.
            let panic_info = this.collect_panic_info().unwrap_or_default();
            this.kill_async();

            Err(DbspError::Runtime(RuntimeError::WorkerPanic { panic_info }))
        }

        // Receive responses.
        for _ in 0..self.status_receivers.len() {
            let ready = select.select();
            let worker = ready.index();

            match ready.recv(&self.status_receivers[worker]) {
                Err(_) => return handle_panic(self),
                Ok(Err(e)) => {
                    let _ = self.kill_inner();
                    return Err(e);
                }
                Ok(Ok(resp)) => handler(worker, resp),
            }
        }
        if self.panicked() {
            return handle_panic(self);
        }

        Ok(())
    }

    fn unicast_command(&mut self, worker: usize, command: Command) -> Result<Response, DbspError> {
        if self.runtime.is_none() {
            return Err(DbspError::Runtime(RuntimeError::Terminated));
        }

        // Send command.
        if self.command_senders[worker].send(command.clone()).is_err() {
            let panic_info = self.collect_panic_info().unwrap_or_default();

            // Worker thread panicked. Exit without waiting for all workers to exit
            // to avoid deadlocks due to workers waiting for each other.
            self.kill_async();
            return Err(DbspError::Runtime(RuntimeError::WorkerPanic { panic_info }));
        }
        self.runtime.as_ref().unwrap().unpark_worker(worker);

        let reply = match self.status_receivers[worker].recv() {
            Err(_) => return handle_panic(self),
            Ok(Err(e)) => {
                let _ = self.kill_inner();
                return Err(e);
            }
            Ok(Ok(resp)) => resp,
        };

        // Receive responses.
        fn handle_panic(this: &mut DBSPHandle) -> Result<Response, DbspError> {
            // Retrieve panic info before killing the circuit.
            let panic_info = this.collect_panic_info().unwrap_or_default();
            this.kill_async();

            Err(DbspError::Runtime(RuntimeError::WorkerPanic { panic_info }))
        }
        if self.panicked() {
            return handle_panic(self);
        }

        Ok(reply)
    }

    /// Check if the bootstrap is complete and clear bootstrap info if it is.
    ///
    /// Should be called after a step or transaction is completed.
    fn check_bootstrap_complete(&mut self) -> Result<(), DbspError> {
        if self.bootstrap_in_progress() {
            let mut replay_complete = Vec::with_capacity(self.status_receivers.len());

            let result =
                self.broadcast_command(Command::IsBootstrapComplete, |_worker, response| {
                    let Response::BootstrapComplete(complete) = response else {
                        panic!("Expected BootstrapComplete response, got {response:?}");
                    };
                    replay_complete.push(complete);
                });

            result?;

            if replay_complete.iter().all(|complete| *complete) {
                info!("Bootstrap complete");
                self.send_complete_bootstrap()?;
            }
        }

        Ok(())
    }

    /// Start and instantly commit a transaction, waiting for the commit to complete.
    /// Returns an error if the main circuit must not process transactions
    /// right now: during the synchronization phase of a concurrent
    /// bootstrap, deltas applied to the boundary streams are no longer
    /// recorded, so they would be silently missing from the bootstrapped
    /// views after cutover.
    fn check_main_circuit_available(&self) -> Result<(), DbspError> {
        if self.concurrent_bootstrap_phase == Some(ConcurrentBootstrapPhase::Synchronizing) {
            return Err(DbspError::Runtime(RuntimeError::BootstrapCircuit(
                "the main circuit cannot process transactions while a concurrent bootstrap \
                 is synchronizing"
                    .to_string(),
            )));
        }
        Ok(())
    }

    pub fn transaction(&mut self) -> Result<(), DbspError> {
        self.check_main_circuit_available()?;
        DBSP_STEP.fetch_add(1, Ordering::Relaxed);
        let start = Instant::now();
        let result = self.broadcast_command(Command::Transaction, |_, _| {});
        DBSP_STEP_LATENCY_MICROSECONDS
            .lock()
            .unwrap()
            .record_elapsed(start);
        if let Some(handle) = self.runtime.as_ref() {
            self.runtime_elapsed +=
                start.elapsed() * handle.runtime().layout().local_workers().len() as u32 * 2;
        }

        self.check_bootstrap_complete()?;

        result
    }

    /// Start a transaction.
    ///
    /// A transaction consists of a sequence of steps that evaluate a set of inputs for a single logical
    /// clock tick.
    ///
    /// Transaction lifecycle:
    ///
    /// ```text
    ///                              is_commit_complete() = true
    ///    ┌────────────────────────────────────────────────────────────────────────────────────┐
    ///    ▼                                                                                    │
    /// ┌───────┐      start_transaction()      ┌───────────┐ start_commit_transaction()  ┌─────┴────┐
    /// │ idle  ├──────────────────────────────►│in progress├────────────────────────────►│committing│
    /// └───────┘                               └────────┬──┘                             └─────────┬┘
    ///                                           ▲      │                                    ▲     │
    ///                                           └──────┘                                    └─────┘
    ///                                            step()                                      step()
    /// ```
    ///
    /// The value of the circuit's logical clock remains unchanged during the transaction.
    /// The clock advances between transactions.
    pub fn start_transaction(&mut self) -> Result<(), DbspError> {
        self.check_main_circuit_available()?;
        let start = Instant::now();
        let result = self.broadcast_command(Command::StartTransaction, |_, _| {});
        if let Some(handle) = self.runtime.as_ref() {
            self.runtime_elapsed +=
                start.elapsed() * handle.runtime().layout().local_workers().len() as u32 * 2;
        }
        if result.is_ok() {
            self.main_transaction_open = true;
        }
        result
    }

    /// Evaluate the circuit for a single step.
    ///
    /// In the `in progress` state of the transaction, this method always returns `false`.
    ///
    /// In the `committing` state, this method returns `true` when the commit is complete and
    /// the circuit has produced all outputs for the inputs received during the transaction.
    pub fn step(&mut self) -> Result<bool, DbspError> {
        let start = Instant::now();
        let mut commit_complete = Vec::with_capacity(self.status_receivers.len());

        let result = self.broadcast_command(Command::Step, |_worker, response| {
            let Response::CommitComplete(complete) = response else {
                panic!("Expected CommitComplete response, got {response:?}");
            };
            commit_complete.push(complete);
        });
        if let Some(handle) = self.runtime.as_ref() {
            self.runtime_elapsed +=
                start.elapsed() * handle.runtime().layout().local_workers().len() as u32 * 2;
        }

        result?;

        let commit_complete = commit_complete.iter().any(|complete| *complete);

        if commit_complete {
            debug!("Commit complete");
            self.main_transaction_open = false;
            self.check_bootstrap_complete()?;
        }

        Ok(commit_complete)
    }

    /// Start committing the current transaction by forcing all operators to process
    /// their inputs to completion.
    ///
    /// The caller must invoke `step` repeatedly until the commit is complete.
    pub fn start_commit_transaction(&mut self) -> Result<(), DbspError> {
        let start = Instant::now();
        let result = self.broadcast_command(Command::CommitTransaction, |_, _| {});
        if let Some(handle) = self.runtime.as_ref() {
            self.runtime_elapsed +=
                start.elapsed() * handle.runtime().layout().local_workers().len() as u32 * 2;
        }
        result
    }

    /// Creates the bootstrap circuit: a second copy of the circuit, built on
    /// every worker thread from the same constructor as the main circuit.
    ///
    /// Concurrent bootstrapping uses the bootstrap circuit to backfill new
    /// and modified operators from a checkpoint while the main circuit keeps
    /// serving pre-existing views.  The copy's nodes have the same `NodeId`s
    /// as the main circuit's (both are built by the same constructor), which
    /// is what lets bootstrapped state move between the copies when the
    /// backfill completes.
    ///
    /// The bootstrap circuit is driven in lockstep on all workers through
    /// [`Self::start_bootstrap_transaction`],
    /// [`Self::start_commit_bootstrap_transaction`], and
    /// [`Self::step_bootstrap_circuit`], and deleted with
    /// [`Self::destroy_bootstrap_circuit`].
    ///
    /// # Resource usage
    ///
    /// Building the copy registers its exchange, input, and output objects
    /// in the runtime's local store, which retains them until runtime
    /// teardown — they are not reclaimed by
    /// [`Self::destroy_bootstrap_circuit`].  The cost per create/destroy
    /// cycle is small (empty mailboxes and counters), and the expected use
    /// is one bootstrap circuit per process lifetime.  While the bootstrap
    /// circuit replays storage-backed state, its spines spill to storage
    /// like the main circuit's; capacity planning should expect transient
    /// disk-usage growth proportional to the bootstrapped region's state.
    ///
    /// # Errors
    ///
    /// Fails if a bootstrap circuit already exists.
    pub fn create_bootstrap_circuit(&mut self) -> Result<(), DbspError> {
        self.check_bootstrap_circuit_state(&[BootstrapCircuitState::None], "create")?;
        self.broadcast_command(Command::CreateBootstrapCircuit(None), |_, _| {})?;
        self.bootstrap_circuit_state = BootstrapCircuitState::Idle;
        Ok(())
    }

    /// Returns an error unless the bootstrap circuit's state is one of
    /// `expected`.
    fn check_bootstrap_circuit_state(
        &self,
        expected: &[BootstrapCircuitState],
        operation: &str,
    ) -> Result<(), DbspError> {
        if expected.contains(&self.bootstrap_circuit_state) {
            Ok(())
        } else {
            Err(DbspError::Runtime(RuntimeError::BootstrapCircuit(format!(
                "cannot {operation} in bootstrap-circuit state {:?} (expected one of {expected:?})",
                self.bootstrap_circuit_state,
            ))))
        }
    }

    /// Starts a concurrent bootstrap from the checkpoint at `base`.
    ///
    /// Concurrent bootstrapping populates new and modified parts of the
    /// circuit without interrupting the pre-existing parts:
    ///
    /// 1. The main circuit restores from the checkpoint, schedules only its
    ///    pre-existing region, and arms recorders that capture the changes
    ///    flowing into the bootstrapped region (see
    ///    [`CircuitHandle::restore_concurrent`]).
    /// 2. A bootstrap circuit is created and restored from the same
    ///    checkpoint, prepared to replay the bootstrapped region exactly as
    ///    a non-concurrent bootstrap would (see [`CircuitHandle::restore`]).
    /// 3. A transaction is started and committed on the bootstrap circuit;
    ///    the replay makes progress as the caller pumps
    ///    [`Self::step_bootstrap_circuit`], which returns `true` when the
    ///    replay has completed.  The main circuit processes transactions
    ///    normally throughout.
    ///
    /// Returns [`ConcurrentRestoreOutcome::UpToDate`], leaving the circuit
    /// fully active with no bootstrap circuit created, if the checkpoint
    /// matches the circuit; returns [`ConcurrentRestoreOutcome::FellBack`]
    /// if the bootstrapped region cannot be processed concurrently, in
    /// which case the main circuit runs a non-concurrent bootstrap that
    /// completes through the standard machinery (drive it with
    /// [`Self::transaction`] until [`Self::bootstrap_in_progress`] clears).
    ///
    /// On [`ConcurrentRestoreOutcome::Concurrent`], complete the bootstrap
    /// with [`Self::step_bootstrap_circuit`] (pump until `true`),
    /// [`Self::sync_concurrent_bootstrap`], pump again, and
    /// [`Self::complete_concurrent_bootstrap`].
    pub fn start_concurrent_bootstrap(
        &mut self,
        base: StoragePath,
    ) -> Result<ConcurrentRestoreOutcome, DbspError> {
        self.check_bootstrap_circuit_state(
            &[BootstrapCircuitState::None],
            "start a concurrent bootstrap",
        )?;
        if self.bootstrap_info.is_some() {
            return Err(DbspError::Runtime(RuntimeError::BootstrapCircuit(
                "cannot start a concurrent bootstrap while a non-concurrent bootstrap \
                 is in progress"
                    .to_string(),
            )));
        }

        // Restore the main circuit; all workers must report the same
        // outcome (divergence is unrecoverable, as in
        // `collect_restore_info`).
        let mut worker_outcomes = BTreeMap::<usize, ConcurrentRestoreOutcome>::new();
        self.broadcast_command(Command::RestoreConcurrent(base.clone()), |worker, resp| {
            let Response::ConcurrentRestore(outcome) = resp else {
                panic!("Expected concurrent restore response, got {resp:?}");
            };
            worker_outcomes.insert(worker, outcome);
        })?;
        for i in 1..worker_outcomes.len() {
            if worker_outcomes[&i] != worker_outcomes[&0] {
                let _ = self.kill_inner();
                return Err(DbspError::Scheduler(SchedulerError::ReplayInfoConflict {
                    error: format!(
                        "worker 0 and worker {i} returned different outcomes during a \
                         concurrent restore; this can be caused by a bug or data corruption: \
                         worker 0: {:?}, worker {i}: {:?}",
                        worker_outcomes[&0], worker_outcomes[&i],
                    ),
                }));
            }
        }
        let outcome = worker_outcomes
            .remove(&0)
            .expect("at least one worker must respond");

        let main_info = match outcome {
            ConcurrentRestoreOutcome::UpToDate => {
                // The checkpoint matches the circuit: the main circuit is
                // fully active, and there is nothing to bootstrap.
                return Ok(ConcurrentRestoreOutcome::UpToDate);
            }
            ConcurrentRestoreOutcome::FellBack { reason, info } => {
                // The main circuit is running the replay itself, exactly as
                // after a non-concurrent restore; the standard
                // `check_bootstrap_complete` machinery completes it.
                info!(
                    "Concurrent bootstrap not possible ({reason}); falling back to a \
                     non-concurrent bootstrap"
                );
                self.bootstrap_info = info.clone();
                return Ok(ConcurrentRestoreOutcome::FellBack { reason, info });
            }
            ConcurrentRestoreOutcome::Concurrent(info) => info,
        };

        let bootstrap_info = self.collect_restore_info(
            Command::CreateBootstrapCircuit(Some(base)),
            "bootstrap-circuit restore",
        )?;
        // The workers hold bootstrap circuits from this point on; the only
        // non-fatal failure below is the cross-copy comparison, after which
        // the caller can clean up with `destroy_bootstrap_circuit`.
        self.bootstrap_circuit_state = BootstrapCircuitState::Idle;

        // Both computations analyze the same checkpoint against structurally
        // identical circuits, so they must agree.
        if bootstrap_info.as_ref() != Some(&main_info) {
            return Err(DbspError::Scheduler(SchedulerError::ReplayInfoConflict {
                error: format!(
                    "the main circuit and the bootstrap circuit returned different replay info \
                     for the same checkpoint; this can be caused by a bug or data corruption;\n  \
                     main circuit: {main_info:?}\n  bootstrap circuit: {bootstrap_info:?}"
                ),
            }));
        }

        // Run the replay as one large background transaction; each
        // `step_bootstrap_circuit` call replays a bounded chunk.
        self.start_bootstrap_transaction()?;
        self.start_commit_bootstrap_transaction()?;

        self.concurrent_bootstrap_info = Some(main_info.clone());
        self.concurrent_bootstrap_phase = Some(ConcurrentBootstrapPhase::Backfill);

        Ok(ConcurrentRestoreOutcome::Concurrent(main_info))
    }

    /// Starts the synchronization phase of a concurrent bootstrap: the
    /// changes that the main circuit recorded on the boundary streams since
    /// the bootstrap started are replayed into the bootstrap circuit.
    ///
    /// The backfill must have completed ([`Self::step_bootstrap_circuit`]
    /// returned `true`).  The main circuit must not process transactions
    /// between this call and [`Self::complete_concurrent_bootstrap`]: inputs
    /// it would apply to the boundary streams are no longer recorded.
    ///
    /// The caller pumps [`Self::step_bootstrap_circuit`] until it returns
    /// `true`, then calls [`Self::complete_concurrent_bootstrap`].
    pub fn sync_concurrent_bootstrap(&mut self) -> Result<(), DbspError> {
        if self.concurrent_bootstrap_phase != Some(ConcurrentBootstrapPhase::Backfill) {
            return Err(DbspError::Runtime(RuntimeError::BootstrapCircuit(format!(
                "cannot synchronize in concurrent-bootstrap phase {:?} (expected Backfill)",
                self.concurrent_bootstrap_phase,
            ))));
        }
        // `Idle` means the backfill transaction's commit has completed.
        self.check_bootstrap_circuit_state(
            &[BootstrapCircuitState::Idle],
            "start the synchronization transaction",
        )?;
        if self.main_transaction_open {
            return Err(DbspError::Runtime(RuntimeError::BootstrapCircuit(
                "cannot start the synchronization transaction while the main circuit has \
                 a transaction in progress"
                    .to_string(),
            )));
        }

        self.broadcast_command(Command::SyncBootstrapCircuit, |_, _| {})?;
        self.bootstrap_circuit_state = BootstrapCircuitState::Committing;
        self.concurrent_bootstrap_phase = Some(ConcurrentBootstrapPhase::Synchronizing);
        Ok(())
    }

    /// Completes a concurrent bootstrap: installs the state the bootstrap
    /// circuit computed for the backfilled nodes into the main circuit,
    /// reactivates the full main circuit, and deletes the bootstrap circuit.
    ///
    /// The synchronization transaction must have completed
    /// ([`Self::step_bootstrap_circuit`] returned `true` after
    /// [`Self::sync_concurrent_bootstrap`]).
    pub fn complete_concurrent_bootstrap(&mut self) -> Result<(), DbspError> {
        if self.concurrent_bootstrap_phase != Some(ConcurrentBootstrapPhase::Synchronizing) {
            return Err(DbspError::Runtime(RuntimeError::BootstrapCircuit(format!(
                "cannot cut over in concurrent-bootstrap phase {:?} (expected Synchronizing)",
                self.concurrent_bootstrap_phase,
            ))));
        }
        // `Idle` means the synchronization transaction's commit has
        // completed.
        self.check_bootstrap_circuit_state(&[BootstrapCircuitState::Idle], "cut over")?;

        self.broadcast_command(Command::CutoverBootstrapCircuit, |_, _| {})?;
        self.bootstrap_circuit_state = BootstrapCircuitState::None;
        self.concurrent_bootstrap_info = None;
        self.concurrent_bootstrap_phase = None;
        Ok(())
    }

    /// Broadcasts `command`, which must produce a
    /// [`Response::CheckpointRestored`] on every worker, and checks that all
    /// workers computed identical replay info.
    fn collect_restore_info(
        &mut self,
        command: Command,
        what: &str,
    ) -> Result<Option<BootstrapInfo>, DbspError> {
        let mut worker_replay_info = BTreeMap::<usize, Option<BootstrapInfo>>::new();

        self.broadcast_command(command, |worker, resp| {
            let Response::CheckpointRestored(replay_info) = resp else {
                panic!("Expected checkpoint restore response, got {resp:?}");
            };
            worker_replay_info.insert(worker, replay_info);
        })?;

        for i in 1..worker_replay_info.len() {
            if worker_replay_info[&i] != worker_replay_info[&0] {
                let mut info = Vec::new();
                for j in 0..worker_replay_info.len() {
                    info.push(format!(
                        "  worker {j} replay info: {:?}",
                        worker_replay_info[&j]
                    ));
                }
                let info = info.join("\n");
                // Workers that disagree on the replay info have divergent
                // scheduler state; the lockstep invariants are
                // unrecoverable, so kill the pipeline.
                let _ = self.kill_inner();
                return Err(DbspError::Scheduler(SchedulerError::ReplayInfoConflict {
                    error: format!(
                        "worker 0 and worker {i} returned different replay info during {what}; \
                         this can be caused by a bug or data corruption; replay info\n{info}"
                    ),
                }));
            }
        }

        Ok(worker_replay_info.remove(&0).flatten())
    }

    /// Returns information about an in-progress concurrent bootstrap, or
    /// `None` if no concurrent bootstrap is in progress.
    pub fn concurrent_bootstrap_info(&self) -> &Option<BootstrapInfo> {
        &self.concurrent_bootstrap_info
    }

    /// Starts a transaction on the bootstrap circuit (see
    /// [`Self::create_bootstrap_circuit`]).
    ///
    /// The bootstrap circuit's transaction is independent of the main
    /// circuit's: the two circuits have separate schedulers and can be in
    /// different transaction states.
    pub fn start_bootstrap_transaction(&mut self) -> Result<(), DbspError> {
        self.check_bootstrap_circuit_state(&[BootstrapCircuitState::Idle], "start a transaction")?;
        self.broadcast_command(Command::StartBootstrapTransaction, |_, _| {})?;
        self.bootstrap_circuit_state = BootstrapCircuitState::InTransaction;
        Ok(())
    }

    /// Starts committing the bootstrap circuit's current transaction.  The
    /// caller must invoke [`Self::step_bootstrap_circuit`] repeatedly until
    /// it returns `true`.
    pub fn start_commit_bootstrap_transaction(&mut self) -> Result<(), DbspError> {
        self.check_bootstrap_circuit_state(
            &[BootstrapCircuitState::InTransaction],
            "commit a transaction",
        )?;
        self.broadcast_command(Command::CommitBootstrapTransaction, |_, _| {})?;
        self.bootstrap_circuit_state = BootstrapCircuitState::Committing;
        Ok(())
    }

    /// Evaluates the bootstrap circuit for a single step on every worker.
    ///
    /// Returns `true` when the bootstrap circuit's commit is complete (see
    /// [`Self::step`] for the transaction state machine).
    pub fn step_bootstrap_circuit(&mut self) -> Result<bool, DbspError> {
        self.check_bootstrap_circuit_state(
            &[
                BootstrapCircuitState::InTransaction,
                BootstrapCircuitState::Committing,
            ],
            "step",
        )?;
        let start = Instant::now();
        let mut commit_complete = Vec::with_capacity(self.status_receivers.len());

        let result = self.broadcast_command(Command::StepBootstrapCircuit, |_worker, response| {
            let Response::CommitComplete(complete) = response else {
                panic!("Expected CommitComplete response, got {response:?}");
            };
            commit_complete.push(complete);
        });
        if let Some(handle) = self.runtime.as_ref() {
            // Bootstrap stepping is deliberately charged to the same time
            // base as the main circuit: `runtime_elapsed` measures total
            // worker occupancy, and the workers are busy either way.
            self.runtime_elapsed +=
                start.elapsed() * handle.runtime().layout().local_workers().len() as u32 * 2;
        }

        result?;

        // Commit completion is a cross-worker consensus inside the
        // scheduler, so all workers report the same value.
        let commit_complete = commit_complete.iter().any(|complete| *complete);
        if commit_complete {
            self.bootstrap_circuit_state = BootstrapCircuitState::Idle;
        }
        Ok(commit_complete)
    }

    /// Deletes the bootstrap circuit on every worker.
    ///
    /// Aborts an in-progress concurrent bootstrap (see
    /// [`Self::start_concurrent_bootstrap`]); the main circuit continues to
    /// serve its pre-existing views, and the only way to populate the
    /// bootstrapped views afterwards is to restart from a checkpoint.
    ///
    /// # Errors
    ///
    /// Fails if no bootstrap circuit exists or if it has a transaction in
    /// progress: tearing down a circuit mid-transaction trips operators'
    /// end-of-clock invariant checks.  Pump
    /// [`Self::step_bootstrap_circuit`] until it returns `true` first.
    pub fn destroy_bootstrap_circuit(&mut self) -> Result<(), DbspError> {
        self.check_bootstrap_circuit_state(&[BootstrapCircuitState::Idle], "destroy")?;
        self.broadcast_command(Command::DestroyBootstrapCircuit, |_, _| {})?;
        self.bootstrap_circuit_state = BootstrapCircuitState::None;
        if self.concurrent_bootstrap_info.is_some() {
            // Aborting a concurrent bootstrap leaves the backfilled nodes
            // empty; see `concurrent_bootstrap_aborted`.
            self.concurrent_bootstrap_aborted = true;
        }
        self.concurrent_bootstrap_info = None;
        self.concurrent_bootstrap_phase = None;
        Ok(())
    }

    /// Convenience method that calls `start_commit_transaction` and then repeatedly calls `step`
    /// until the commit is complete.
    pub fn commit_transaction(&mut self) -> Result<(), DbspError> {
        self.start_commit_transaction()?;

        loop {
            let commit_complete = self.step()?;
            if commit_complete {
                return Ok(());
            }
        }
    }

    /// Estimated commit progress.
    pub fn commit_progress(&mut self) -> Result<WorkersCommitProgress, DbspError> {
        let mut progress = WorkersCommitProgress::new();

        self.broadcast_command(Command::CommitProgress, |worker, response| {
            let Response::CommitProgress(worker_progress) = response else {
                panic!("Expected CommitProgress response, got {response:?}");
            };
            progress.insert(worker as u16, worker_progress);
        })?;

        Ok(progress)
    }

    /// The bootstrap circuit's transaction-commit progress, aggregated across
    /// workers.
    ///
    /// Reports the commit progress of the bootstrap circuit's in-flight
    /// transaction -- the backfill transaction during a concurrent bootstrap,
    /// then the synchronization transaction. The summary is empty while no
    /// commit is in progress (e.g. inputs are still being replayed).
    pub fn bootstrap_commit_progress(&mut self) -> Result<WorkersCommitProgress, DbspError> {
        self.check_bootstrap_circuit_state(
            &[
                BootstrapCircuitState::Idle,
                BootstrapCircuitState::InTransaction,
                BootstrapCircuitState::Committing,
            ],
            "report commit progress",
        )?;

        let mut progress = WorkersCommitProgress::new();

        self.broadcast_command(Command::BootstrapCommitProgress, |worker, response| {
            let Response::CommitProgress(worker_progress) = response else {
                panic!("Expected CommitProgress response, got {response:?}");
            };
            progress.insert(worker as u16, worker_progress);
        })?;

        Ok(progress)
    }

    /// The circuit has been resumed from a checkpoint and is currently bootstrapping the modified part of the circuit.
    pub fn bootstrap_in_progress(&self) -> bool {
        self.bootstrap_info.is_some()
    }

    /// In the bootstrap mode, returns information about operators involved in bootstrapping.
    pub fn bootstrap_info(&self) -> &Option<BootstrapInfo> {
        &self.bootstrap_info
    }

    /// Returns the time elapsed while the circuit is executing a step,
    /// multiplied by the number of foreground and background threads.
    pub fn runtime_elapsed(&self) -> Duration {
        self.runtime_elapsed
    }

    /// Fingerprint of this circuit.
    pub fn fingerprint(&self) -> u64 {
        self.fingerprint
    }

    /// Reset circuit state to the point of the given Commit.
    ///
    /// If the circuit needs bootstrapping new operators, put it in the bootstrap mode.
    fn send_restore(&mut self, base: StoragePath) -> Result<(), DbspError> {
        self.bootstrap_info =
            self.collect_restore_info(Command::Restore(base), "restart from a checkpoint")?;

        if let Some(bootstrap_info) = &self.bootstrap_info {
            info!(
                "Circuit restored from checkpoint, bootstrapping new parts of the circuit: {bootstrap_info:?}"
            );
        }

        Ok(())
    }

    /// Notifies workers to exit the replay phase and start normal operation
    /// (during the replay phase only the operators involved in replay are scheduled).
    fn send_complete_bootstrap(&mut self) -> Result<(), DbspError> {
        self.broadcast_command(Command::CompleteBootstrap, |_, _| {})?;

        self.bootstrap_info = None;

        Ok(())
    }

    fn checkpointer(&self) -> Result<&Arc<Mutex<Checkpointer>>, DbspError> {
        self.checkpointer
            .as_ref()
            .ok_or(DbspError::Storage(StorageError::StorageDisabled))
    }

    /// Allows creating a new checkpoint by taking a consistent snapshot of the
    /// state in dbsp.
    pub fn checkpoint(&mut self) -> CheckpointBuilder<'_> {
        CheckpointBuilder::new(self)
    }

    /// List all currently available checkpoints.
    pub fn list_checkpoints(&mut self) -> Result<Vec<CheckpointMetadata>, DbspError> {
        self.checkpointer()?.lock().unwrap().list_checkpoints()
    }

    /// Remove the oldest checkpoint from the list.
    /// - Prevents removing checkpoints whose UUID is in `except`.
    ///
    /// # Returns
    /// - Uuid of the removed checkpoints, if any were removed.
    /// - An empty set if there were no checkpoints to remove.
    pub fn gc_checkpoint(
        &mut self,
        except: HashSet<uuid::Uuid>,
    ) -> Result<HashSet<uuid::Uuid>, DbspError> {
        // The bootstrap circuit reads batch files belonging to the
        // checkpoint it restores from; checkpoint GC could delete them.
        self.check_bootstrap_circuit_state(
            &[BootstrapCircuitState::None],
            "garbage-collect checkpoints",
        )?;
        self.checkpointer()?.lock().unwrap().gc_checkpoint(except)
    }

    /// Enable CPU profiler.
    ///
    /// Enable recording of CPU usage info.  When CPU profiling is enabled,
    /// [`Self::dump_profile`] outputs CPU usage info along with memory
    /// usage and other circuit metadata.  CPU profiling introduces small
    /// runtime overhead.
    pub fn enable_cpu_profiler(&mut self) -> Result<(), DbspError> {
        self.broadcast_command(Command::EnableProfiler, |_, _| {})
    }

    /// Dump profiling information to the specified directory.
    ///
    /// Creates `dir_path` if it doesn't exist.  For each worker thread, creates
    /// `dir_path/<timestamp>/<worker>.dot` file containing worker profile in
    /// the graphviz format.  If CPU profiling was enabled (see
    /// [`Self::enable_cpu_profiler`]), the profile will contain both CPU and
    /// memory usage information; otherwise only memory usage details are
    /// reported.
    pub fn dump_profile<P: AsRef<Path>>(&mut self, dir_path: P) -> Result<PathBuf, DbspError> {
        Ok(self.graph_profile()?.dump(dir_path)?)
    }

    /// Returns an array of worker profiles in graphviz `.dot` format.  Each
    /// array element corresponds to the profile of the corresponding worker.
    /// If CPU profiling was enabled (see [`Self::enable_cpu_profiler`]), the
    /// profile will contain both CPU and memory usage information; otherwise
    /// only memory usage details are reported.
    pub fn graph_profile(&mut self) -> Result<GraphProfile, DbspError> {
        let mut worker_graphs = vec![Default::default(); self.status_receivers.len()];
        self.broadcast_command(
            Command::DumpProfile {
                runtime_elapsed: self.runtime_elapsed(),
            },
            |worker, resp| {
                if let Response::ProfileDump(prof) = resp {
                    worker_graphs[worker] = prof;
                }
            },
        )?;
        Ok(GraphProfile {
            elapsed_time: self.start_time.elapsed(),
            worker_offset: self.runtime().layout().local_workers().start,
            worker_graphs,
        })
    }

    pub fn retrieve_profile(&mut self) -> Result<DbspProfile, DbspError> {
        let mut profiles = vec![Default::default(); self.status_receivers.len()];
        let mut graphs = vec![Default::default(); self.status_receivers.len()];

        self.broadcast_command(
            Command::RetrieveProfile {
                runtime_elapsed: self.runtime_elapsed(),
            },
            |worker, resp| {
                if let Response::Profile(prof) = resp {
                    profiles[worker] = prof;
                }
            },
        )?;

        self.broadcast_command(Command::RetrieveGraph, |worker, resp| {
            // They are all identical, but the command returns one per worker.
            if let Response::ProfileDump(graph) = resp {
                graphs[worker] = graph;
            }
        })?;

        Ok(DbspProfile::new(profiles, graphs.pop()))
    }

    pub fn lir(&mut self) -> Result<LirCircuit, DbspError> {
        let mut lirs = vec![Default::default(); self.status_receivers.len()];

        self.broadcast_command(Command::GetLir, |worker, resp| {
            if let Response::Lir(lir) = resp {
                lirs[worker] = lir;
            }
        })?;

        Ok(lirs.remove(0))
    }

    /// Terminate the execution of the circuit, exiting all worker threads.
    ///
    /// If one or more of the worker threads panics, returns the argument the
    /// `panic!` macro was called with (see `std::thread::Result`).
    ///
    /// This is the preferred way of killing a circuit.  Simply dropping the
    /// handle will have the same effect, but without reporting the error
    /// status.
    pub fn kill(mut self) -> ThreadResult<()> {
        if self.runtime.is_none() {
            return Ok(());
        }

        self.kill_inner()
    }

    /// Enable/disable automatic rebalancing of the circuit.
    ///
    /// When enabled, the circuit will automatically rebalance skewed joins based on internal
    /// heuristics. Rebalancing can introduce pipeline stalls.
    ///
    /// When disabled, the circuit will not automatically rebalance skewed joins. Rebalancing can
    /// be triggered explicitly by calling `rebalance`.
    pub fn set_auto_rebalance(&mut self, enable: bool) -> Result<(), DbspError> {
        self.broadcast_command(Command::SetAutoRebalance(enable), |_, resp| {
            let Response::Unit = resp else {
                panic!("Expected Unit response, got {resp:?}");
            };
        })?;
        Ok(())
    }

    pub fn set_balancer_hint_by_global_id(
        &mut self,
        global_node_id: &GlobalNodeId,
        hint: BalancerHint,
    ) -> Result<(), DbspError> {
        let mut result =
            self.set_balancer_hints_by_global_id(vec![(global_node_id.clone(), hint)])?;
        result.pop().unwrap()
    }

    /// Set the balancer hint for the stream with the given persistent id.
    ///
    /// - `persistent_id` must exist in the circuit and belong to an input stream of a
    ///   balancing join operator. In practice, this means that the circuit should be running
    ///   in `Persistent` mode and the stream should have a persistent id assigned using
    ///   `set_persistent_id`.
    ///
    /// # Errors
    ///
    /// Fails if the above requirements are not met or the hint cannot be enforced in the current state,
    /// e.g., if the user is setting Broadcast policy on both inputs to a join.
    pub fn set_balancer_hint(
        &mut self,
        persistent_id: &str,
        hint: BalancerHint,
    ) -> Result<(), DbspError> {
        let mut result = self.set_balancer_hints(vec![(persistent_id.to_string(), hint)])?;
        result.pop().unwrap()
    }

    pub fn set_balancer_hints_by_global_id(
        &mut self,
        hints: Vec<(GlobalNodeId, BalancerHint)>,
    ) -> Result<Vec<Result<(), DbspError>>, DbspError> {
        let mut results = Vec::new();

        self.broadcast_command(Command::SetBalancerHintsByGlobalId(hints), |_, resp| {
            let Response::SetBalancerHints(worker_results) = resp else {
                panic!("Expected SetBalancerHints response, got {resp:?}");
            };
            results = worker_results;
        })?;

        Ok(results)
    }

    /// Set multiple balancer hints.
    ///
    /// Applies hints one by one.
    pub fn set_balancer_hints(
        &mut self,
        hints: Vec<(String, BalancerHint)>,
    ) -> Result<Vec<Result<(), DbspError>>, DbspError> {
        let mut results = Vec::new();

        self.broadcast_command(Command::SetBalancerHints(hints), |_, resp| {
            let Response::SetBalancerHints(worker_results) = resp else {
                panic!("Expected SetBalancerHints response, got {resp:?}");
            };
            results = worker_results;
        })?;

        Ok(results)
    }

    /// Get the current balancer policies for all operators in the circuit.
    pub fn get_current_balancer_policies(
        &mut self,
    ) -> Result<BTreeMap<GlobalNodeId, PartitioningPolicy>, DbspError> {
        let resp = self.unicast_command(0, Command::GetCurrentBalancerPolicies)?;

        let Response::CurrentBalancerPolicies(policy) = resp else {
            panic!("Expected GetCurrentBalancerPolicies response, got {resp:?}");
        };
        Ok(policy)
    }

    /// Get the current balancer policy for the operator with the given persistent id.
    pub fn get_current_balancer_policy(
        &mut self,
        persistent_id: &str,
    ) -> Result<PartitioningPolicy, DbspError> {
        let resp = self.unicast_command(
            0,
            Command::GetCurrentBalancerPolicy(persistent_id.to_string()),
        )?;

        let Response::CurrentBalancerPolicy(policy) = resp else {
            panic!("Expected GetCurrentBalancerPolicy response, got {resp:?}");
        };
        policy
    }

    pub fn rebalance(&mut self) -> Result<(), DbspError> {
        self.broadcast_command(Command::Rebalance, |_, _| {})?;
        Ok(())
    }

    pub fn start_compaction(&mut self) -> Result<(), DbspError> {
        self.broadcast_command(Command::StartCompaction, |_, _| {})?;
        Ok(())
    }

    /// Returns `true` when background compaction has fully converged on every
    /// worker: all compaction requests have been processed, no merge is in
    /// progress, and each spine has been reduced to at most one batch.
    ///
    /// This is a non-blocking point-in-time snapshot.  Callers that need to
    /// wait for compaction to finish should call
    /// [`wait_for_compaction`](Self::wait_for_compaction) or poll this method.
    pub fn is_compaction_complete(&mut self) -> Result<bool, DbspError> {
        let mut complete = true;
        self.broadcast_command(Command::IsCompactionComplete, |_, response| {
            if let Response::IsCompactionComplete(c) = response {
                complete &= c;
            }
        })?;
        Ok(complete)
    }

    /// Block until background compaction has fully converged on every worker,
    /// or until `timeout` elapses.
    ///
    /// Polls [`is_compaction_complete`](Self::is_compaction_complete) with
    /// exponential back-off, starting at 1 ms and doubling each iteration up
    /// to a cap of 1 s.
    ///
    /// Returns `Ok(())` when compaction is complete, or an error if the
    /// circuit fails or the timeout expires.
    pub fn wait_for_compaction(
        &mut self,
        timeout: std::time::Duration,
    ) -> Result<(), anyhow::Error> {
        use std::thread;
        use std::time::Instant;

        const INITIAL_SLEEP_MS: u64 = 1;
        const MAX_SLEEP_MS: u64 = 1_000;

        let deadline = Instant::now() + timeout;
        let mut sleep_ms = INITIAL_SLEEP_MS;

        loop {
            if self.is_compaction_complete()? {
                return Ok(());
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                anyhow::bail!("timed out after {timeout:?} waiting for compaction to complete");
            }
            let sleep = std::time::Duration::from_millis(sleep_ms).min(remaining);
            thread::sleep(sleep);
            sleep_ms = (sleep_ms * 2).min(MAX_SLEEP_MS);
        }
    }
}

impl Drop for DBSPHandle {
    fn drop(&mut self) {
        if self.runtime.is_some() {
            let _ = self.kill_inner();
        }
    }
}

/// Checkpoint builder.
#[derive(Debug)]
pub struct CheckpointBuilder<'a> {
    handle: &'a mut DBSPHandle,
    name: Option<String>,
    steps: Option<u64>,
    processed_records: Option<u64>,
}

impl<'a> CheckpointBuilder<'a> {
    fn new(handle: &'a mut DBSPHandle) -> Self {
        Self {
            handle,
            name: None,
            steps: None,
            processed_records: None,
        }
    }

    /// Gives the checkpoint a name.
    pub fn with_name(self, name: impl Into<String>) -> Self {
        Self {
            name: Some(name.into()),
            ..self
        }
    }

    /// Adds `steps` to the checkpoint to be created.
    pub fn with_steps(self, steps: u64) -> Self {
        Self {
            steps: Some(steps),
            ..self
        }
    }

    /// Adds `processed_records` to the checkpoint to be created.
    pub fn with_processed_records(self, processed_records: u64) -> Self {
        Self {
            processed_records: Some(processed_records),
            ..self
        }
    }

    /// Prepares and commits the checkpoint.
    pub fn run(self) -> Result<CheckpointMetadata, DbspError> {
        self.prepare().and_then(CheckpointCommitter::commit)
    }

    /// Prepares the checkpoint and returns a committer that can be used to
    /// commit it later.
    pub fn prepare(self) -> Result<CheckpointCommitter, DbspError> {
        // Both circuit copies derive checkpoint file names from the same
        // persistent ids, so checkpointing while a bootstrap circuit exists
        // is undefined; additionally, the bootstrap circuit reads batch
        // files of the checkpoint it restores from, which checkpoint GC
        // could delete.
        self.handle
            .check_bootstrap_circuit_state(&[BootstrapCircuitState::None], "create a checkpoint")?;
        if self.handle.concurrent_bootstrap_phase.is_some()
            || self.handle.concurrent_bootstrap_aborted
        {
            return Err(DbspError::Runtime(RuntimeError::BootstrapCircuit(
                "cannot create a checkpoint while a concurrent bootstrap is in progress \
                 or after one was aborted"
                    .to_string(),
            )));
        }

        let checkpointer = self.handle.checkpointer()?.clone();

        // Write an empty catalog before the UUID directory is created by
        // operators.  This prevents read_checkpoints from seeing orphaned UUID
        // directories during the first checkpoint commit on fresh storage.
        checkpointer.lock().unwrap().ensure_catalog_exists()?;

        let uuid = Uuid::now_v7();
        let checkpoint_dir = Checkpointer::checkpoint_dir(uuid);
        let mut readers = Vec::new();
        self.handle
            .broadcast_command(Command::Checkpoint(checkpoint_dir), |_worker, resp| {
                let Response::CheckpointCreated(r) = resp else {
                    panic!("Expected checkpoint response, got {resp:?}");
                };
                readers.push(r);
            })?;
        Ok(CheckpointCommitter {
            checkpointer,
            uuid,
            readers,
            fingerprint: self.handle.fingerprint,
            name: self.name,
            steps: self.steps,
            processed_records: self.processed_records,
        })
    }
}

/// Committer for a checkpoint.
pub struct CheckpointCommitter {
    checkpointer: Arc<Mutex<Checkpointer>>,
    uuid: Uuid,
    readers: Vec<Vec<Arc<dyn FileCommitter>>>,
    fingerprint: u64,
    name: Option<String>,
    steps: Option<u64>,
    processed_records: Option<u64>,
}

impl CheckpointCommitter {
    /// Commits the checkpoint.
    ///
    /// Committing a checkpoint ensures that its data is on stable storage.  It
    /// can run in the background while the circuit processes more steps.
    pub fn commit(self) -> Result<CheckpointMetadata, DbspError> {
        for reader in self.readers.into_iter().flatten() {
            reader.commit()?;
        }
        self.checkpointer.lock().unwrap().commit(
            self.uuid,
            self.fingerprint,
            self.name,
            self.steps,
            self.processed_records,
        )
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::fs::{File, create_dir_all};
    use std::io;
    use std::path::Path;
    use std::time::Duration;
    use std::{fs, vec};

    use super::CircuitStorageConfig;
    use crate::circuit::CircuitConfig;
    use crate::circuit::checkpointer::Checkpointer;
    use crate::circuit::runtime::TOKIO_WORKER_INDEX;
    use crate::dynamic::{ClonableTrait, DowncastTrait, DynData, Erase};
    use crate::operator::Generator;
    use crate::operator::TraceBound;
    use crate::storage::backend::StorageError;
    use crate::trace::BatchReaderFactories;
    use crate::utils::Tup2;
    use crate::{
        Circuit, DBSPHandle, Error as DbspError, IndexedZSetHandle, InputHandle, OrdZSet,
        OutputHandle, Runtime, RuntimeError, ZSetHandle, ZWeight,
    };
    use anyhow::anyhow;
    use feldera_buffer_cache::ThreadType;
    use feldera_types::config::{StorageCacheConfig, StorageConfig, StorageOptions};
    use tempfile::tempdir;
    use uuid::Uuid;

    /// Creates, drives, and destroys a bootstrap circuit while the main
    /// circuit keeps processing transactions.  `shard` puts exchange
    /// operators in both circuits, so the test also verifies that the two
    /// circuits' cross-worker rendezvous do not interfere: a mismatch in
    /// exchange-id allocation would deadlock or corrupt the outputs.
    #[test]
    fn test_bootstrap_circuit_lifecycle() {
        let (mut dbsp, (input, output)) = Runtime::init_circuit(4, |circuit| {
            let (stream, input) = circuit.add_input_zset::<u64>();
            let output = stream.shard().output();
            Ok((input, output))
        })
        .unwrap();

        let mut expected_key = 0;
        let push_and_check = |dbsp: &mut DBSPHandle, expected_key: &mut u64| {
            *expected_key += 1;
            input.push(*expected_key, 1);
            dbsp.transaction().unwrap();
            assert_eq!(output.consolidate(), crate::zset! { *expected_key => 1 });
        };

        // The main circuit works before the bootstrap circuit exists.
        push_and_check(&mut dbsp, &mut expected_key);

        // Bootstrap-circuit commands fail while no bootstrap circuit exists.
        assert!(dbsp.step_bootstrap_circuit().is_err());
        assert!(dbsp.destroy_bootstrap_circuit().is_err());

        dbsp.create_bootstrap_circuit().unwrap();
        assert!(dbsp.create_bootstrap_circuit().is_err());

        // Out-of-order transaction commands on an idle bootstrap circuit
        // are rejected on the coordinator (a worker-side scheduler error
        // would kill the pipeline).
        assert!(dbsp.step_bootstrap_circuit().is_err());
        assert!(dbsp.start_commit_bootstrap_transaction().is_err());

        // The main circuit works while an idle bootstrap circuit exists.
        push_and_check(&mut dbsp, &mut expected_key);

        // The main circuit works while the bootstrap circuit has a
        // transaction in progress; destroying the bootstrap circuit
        // mid-transaction is rejected.
        dbsp.start_bootstrap_transaction().unwrap();
        assert!(dbsp.start_bootstrap_transaction().is_err());
        assert!(dbsp.destroy_bootstrap_circuit().is_err());
        push_and_check(&mut dbsp, &mut expected_key);
        dbsp.start_commit_bootstrap_transaction().unwrap();
        assert!(dbsp.destroy_bootstrap_circuit().is_err());
        while !dbsp.step_bootstrap_circuit().unwrap() {}
        push_and_check(&mut dbsp, &mut expected_key);

        // Bootstrap steps interleave with an *open* transaction on the main
        // circuit.  The output mailbox holds one step's batch, so read the
        // delta right after the step that ingests the input.
        dbsp.start_bootstrap_transaction().unwrap();
        dbsp.start_commit_bootstrap_transaction().unwrap();
        expected_key += 1;
        input.push(expected_key, 1);
        dbsp.start_transaction().unwrap();
        dbsp.step().unwrap();
        assert_eq!(output.consolidate(), crate::zset! { expected_key => 1 });
        while !dbsp.step_bootstrap_circuit().unwrap() {}
        dbsp.commit_transaction().unwrap();
        // The commit steps process no further input.
        assert_eq!(output.consolidate(), crate::zset! {});

        dbsp.destroy_bootstrap_circuit().unwrap();
        assert!(dbsp.destroy_bootstrap_circuit().is_err());
        assert!(dbsp.step_bootstrap_circuit().is_err());

        // The main circuit works after the bootstrap circuit is destroyed,
        // and a new bootstrap circuit can be created.
        push_and_check(&mut dbsp, &mut expected_key);
        dbsp.create_bootstrap_circuit().unwrap();
        push_and_check(&mut dbsp, &mut expected_key);
        dbsp.destroy_bootstrap_circuit().unwrap();

        dbsp.kill().unwrap();
    }

    // Panic during initialization in worker thread.
    #[test]
    fn test_panic_in_worker1() {
        test_panic_in_worker(1);
    }

    #[test]
    fn test_panic_in_worker4() {
        test_panic_in_worker(4);
    }

    fn test_panic_in_worker(nworkers: usize) {
        let res = Runtime::init_circuit(nworkers, |circuit| {
            if Runtime::worker_index() == 0 {
                panic!();
            }

            circuit.add_source(Generator::new(|| 5usize));
            Ok(())
        });

        if let DbspError::Runtime(err) = res.unwrap_err() {
            // println!("error: {err}");
            assert!(matches!(err, RuntimeError::WorkerPanic { .. }));
        } else {
            panic!();
        }
    }

    // TODO: initialization error in worker thread (the `constructor` closure
    // currently does not return an error).
    // TODO: panic/error during GC.

    // Panic in `Circuit::step`.
    #[test]
    fn test_step_panic1() {
        test_step_panic(1);
    }

    #[test]
    fn test_step_panic4() {
        test_step_panic(4);
    }

    fn test_step_panic(nworkers: usize) {
        let (mut handle, _) = Runtime::init_circuit(nworkers, |circuit| {
            circuit.add_source(Generator::new(|| {
                if Runtime::worker_index() == 0 {
                    panic!()
                } else {
                    5usize
                }
            }));
            Ok(())
        })
        .unwrap();

        if let DbspError::Runtime(err) = handle.transaction().unwrap_err() {
            // println!("error: {err}");
            matches!(err, RuntimeError::WorkerPanic { .. });
        } else {
            panic!();
        }
    }

    #[test]
    fn test_panic_no_deadlock() {
        let (mut handle, _) = Runtime::init_circuit(4, |circuit| {
            circuit.add_source(Generator::new(|| {
                if Runtime::worker_index() == 1 {
                    panic!()
                } else {
                    std::thread::sleep(Duration::MAX)
                }
            }));
            Ok(())
        })
        .unwrap();

        if let DbspError::Runtime(err) = handle.transaction().unwrap_err() {
            // println!("error: {err}");
            matches!(err, RuntimeError::WorkerPanic { .. });
        } else {
            panic!();
        }
    }

    /// Check that a panic in the tokio merger runtime is propagated to the client.
    #[test]
    fn test_panic_in_tokio_merger_runtime() {
        let (panic_tx, panic_rx) = std::sync::mpsc::channel();
        let (mut handle, _) = Runtime::init_circuit(1, move |circuit| {
            let (_stream, _input_handle) = circuit.add_input_map::<u64, u64, i64, _>(|v, u| {
                *v = ((*v as i64) + *u) as u64;
            });

            if Runtime::worker_index() == 0 {
                let runtime = Runtime::runtime().unwrap();
                let panic_tx = panic_tx.clone();
                runtime.tokio_merger_runtime().unwrap().spawn(async move {
                    TOKIO_WORKER_INDEX
                        .scope(0, async move {
                            let _ = std::panic::catch_unwind(|| {
                                panic!("panic from tokio merger runtime task");
                            });
                            let _ = panic_tx.send(());
                        })
                        .await;
                });
            }

            Ok(())
        })
        .unwrap();

        panic_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("timed out waiting for panic task to complete");

        if let DbspError::Runtime(err) = handle.transaction().unwrap_err() {
            println!("error: {err}");
            match err {
                RuntimeError::WorkerPanic { panic_info } => {
                    assert!(
                        panic_info
                            .iter()
                            .any(|(_worker, thread_type, _info)| *thread_type
                                == ThreadType::Background),
                        "expected WorkerPanic to include background worker panic info"
                    );
                }
                _ => panic!(),
            }
        } else {
            panic!();
        }
    }

    /// Regression test for the deadlock in `RuntimeHandle::join` (commit 1 of
    /// PR #6331) where the `MutexGuard` from `tokio_merger_runtime.lock()` was
    /// held across the `drop` of the runtime, and for the panic on
    /// `.expect("tokio merger runtime has been shut down")` (commit 2 of the
    /// same PR) that the early-return in `MergeJob::run` replaced.
    ///
    /// Spawns a tokio merger task that hammers `Runtime::tokio_merger_runtime`
    /// in a tight loop, then drops the `DBSPHandle` on a side thread and
    /// asserts that:
    /// - the drop completes within a timeout (regressing commit 1 would deadlock
    ///   the blocking pool against the held mutex), and
    /// - the task exits cleanly via the `None` arm (regressing commit 2 would
    ///   panic on `.expect()` and the clean-exit signal would never arrive).
    #[test]
    fn test_drop_does_not_deadlock_with_active_merger_lookup() {
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (task_done_tx, task_done_rx) = std::sync::mpsc::channel();
        let (handle, _) = Runtime::init_circuit(1, move |circuit| {
            let (_stream, _input_handle) = circuit.add_input_map::<u64, u64, i64, _>(|v, u| {
                *v = ((*v as i64) + *u) as u64;
            });
            if Runtime::worker_index() == 0 {
                let runtime = Runtime::runtime().unwrap();
                let ready_tx = ready_tx.clone();
                let task_done_tx = task_done_tx.clone();
                runtime.tokio_merger_runtime().unwrap().spawn(async move {
                    TOKIO_WORKER_INDEX
                        .scope(0, async move {
                            let _ = ready_tx.send(());
                            // Hammer the accessor. Each batch of sync calls
                            // maximizes the chance of landing inside `lock()`
                            // when `RuntimeHandle::join` would (with the bug)
                            // hold the mutex across the runtime drop. Exits
                            // when the accessor returns `None` after `take()`.
                            'outer: loop {
                                for _ in 0..1000 {
                                    if runtime.tokio_merger_runtime().is_none() {
                                        break 'outer;
                                    }
                                }
                                tokio::task::yield_now().await;
                            }
                            let _ = task_done_tx.send(());
                        })
                        .await;
                });
            }
            Ok(())
        })
        .unwrap();

        ready_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("merger task did not start");

        let (drop_done_tx, drop_done_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            drop(handle);
            let _ = drop_done_tx.send(());
        });

        drop_done_rx
            .recv_timeout(Duration::from_secs(30))
            .expect("DBSPHandle::drop deadlocked");
        task_done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("merger task did not exit cleanly via the None arm");
    }

    // Kill the runtime.
    #[test]
    fn test_kill1() {
        test_kill(1);
    }

    #[test]
    fn test_kill4() {
        test_kill(4);
    }

    fn test_kill(nworkers: usize) {
        let (mut handle, _) = Runtime::init_circuit(nworkers, |circuit| {
            circuit.add_source(Generator::new(|| 5usize));
            Ok(())
        })
        .unwrap();

        handle.enable_cpu_profiler().unwrap();
        handle.transaction().unwrap();
        handle
            .dump_profile(std::env::temp_dir().join("test_kill"))
            .unwrap();
        handle.kill().unwrap();
    }

    // Drop the runtime.
    #[test]
    fn test_drop1() {
        test_drop(1);
    }

    #[test]
    fn test_drop4() {
        test_drop(4);
    }

    fn test_drop(nworkers: usize) {
        let (mut handle, _) = Runtime::init_circuit(nworkers, |circuit| {
            circuit.add_source(Generator::new(|| 5usize));
            Ok(())
        })
        .unwrap();

        handle.transaction().unwrap();
    }

    #[test]
    fn test_failing_constructor() {
        match Runtime::init_circuit(4, |_circuit| Err::<(), _>(anyhow!("constructor failed"))) {
            Err(DbspError::Constructor(msg)) => assert_eq!(msg.to_string(), "constructor failed"),
            _ => panic!(),
        }
    }

    type CircuitHandle = (
        IndexedZSetHandle<i32, i32>,
        OutputHandle<OrdZSet<Tup2<i32, i32>>>,
        InputHandle<usize>,
    );

    fn mkcircuit(cconf: CircuitConfig) -> Result<(DBSPHandle, CircuitHandle), DbspError> {
        Runtime::init_circuit(cconf, move |circuit| {
            let (stream, handle) = circuit.add_input_indexed_zset::<i32, i32>();
            let (sample_size_stream, sample_size_handle) = circuit.add_input_stream::<usize>();
            let map_factories = BatchReaderFactories::new::<Tup2<i32, i32>, (), ZWeight>();
            let sample_handle = stream
                .integrate_trace()
                .stream_sample_unique_key_vals(&sample_size_stream)
                .inner()
                .dyn_map(
                    &map_factories,
                    Box::new(|kinput, kv| {
                        let kinput: &DynData =
                            unsafe { kinput.downcast::<Tup2<i32, i32>>() }.erase();
                        kinput.clone_to(kv.split_mut().0);
                    }),
                )
                .typed()
                .output();
            Ok((handle, sample_handle, sample_size_handle))
        })
    }

    fn mkcircuit_different(cconf: CircuitConfig) -> Result<(DBSPHandle, CircuitHandle), DbspError> {
        Runtime::init_circuit(cconf, move |circuit| {
            let map_factories = BatchReaderFactories::new::<Tup2<i32, i32>, (), ZWeight>();
            let (stream, handle) = circuit.add_input_indexed_zset::<i32, i32>();
            let (sample_size_stream, sample_size_handle) = circuit.add_input_stream::<usize>();
            let sample_handle = stream
                .integrate_trace()
                .stream_sample_unique_key_vals(&sample_size_stream)
                .inner()
                .dyn_map(
                    &map_factories,
                    Box::new(|kinput, kv| {
                        let kinput: &DynData =
                            unsafe { kinput.downcast::<Tup2<i32, i32>>() }.erase();
                        kinput.clone_to(kv.split_mut().0);
                    }),
                )
                .typed()
                .output();
            let _sample_handle2: OutputHandle<OrdZSet<Tup2<i32, i32>>> = stream
                .integrate_trace()
                .stream_sample_unique_key_vals(&sample_size_stream)
                .inner()
                .dyn_map(
                    &map_factories,
                    Box::new(|kinput, kv| {
                        let kinput: &DynData =
                            unsafe { kinput.downcast::<Tup2<i32, i32>>() }.erase();
                        kinput.clone_to(kv.split_mut().0);
                    }),
                )
                .typed()
                .output();
            Ok((handle, sample_handle, sample_size_handle))
        })
    }

    #[allow(clippy::type_complexity)]
    fn mkcircuit_with_bounds(
        cconf: CircuitConfig,
    ) -> Result<(DBSPHandle, CircuitHandle), DbspError> {
        Runtime::init_circuit(cconf, move |circuit| {
            let (stream, handle) = circuit.add_input_indexed_zset::<i32, i32>();
            let (sample_size_stream, sample_size_handle) = circuit.add_input_stream::<usize>();
            let map_factories = BatchReaderFactories::new::<Tup2<i32, i32>, (), ZWeight>();
            let tb = TraceBound::new();
            tb.set(Box::new(10).erase_box());
            let sample_handle = stream
                .integrate_trace_with_bound(tb.clone(), tb)
                .stream_sample_unique_key_vals(&sample_size_stream)
                .inner()
                .dyn_map(
                    &map_factories,
                    Box::new(|kinput, kv| {
                        let kinput: &DynData =
                            unsafe { kinput.downcast::<Tup2<i32, i32>>() }.erase();
                        kinput.clone_to(kv.split_mut().0);
                    }),
                )
                .typed()
                .output();
            Ok((handle, sample_handle, sample_size_handle))
        })
    }

    pub(crate) fn mkconfig(path: &Path) -> CircuitConfig {
        CircuitConfig::with_workers(1).with_storage(Some(
            CircuitStorageConfig::for_config(
                StorageConfig {
                    path: path.to_string_lossy().into_owned(),
                    cache: StorageCacheConfig::default(),
                },
                StorageOptions {
                    min_storage_bytes: Some(0),
                    ..StorageOptions::default()
                },
            )
            .unwrap(),
        ))
    }

    /// Utility function that runs a circuit and takes a checkpoint at every
    /// step. It then restores the circuit to every checkpoint and checks that
    /// the state is consistent with what we would expect it to be at that
    /// point.
    fn generic_checkpoint_restore(
        input: Vec<Vec<Tup2<i32, Tup2<i32, i64>>>>,
        circuit_fun: fn(CircuitConfig) -> Result<(DBSPHandle, CircuitHandle), DbspError>,
    ) {
        const SAMPLE_SIZE: usize = 25; // should be bigger than #keys
        assert!(input.len() < SAMPLE_SIZE, "input should be <SAMPLE_SIZE");
        let _temp = tempdir().expect("Can't create temp dir for storage");
        let cconf = mkconfig(_temp.path());

        let mut committed = vec![];
        let mut checkpoints = vec![];

        // We create a circuit and push data into it, we also take a checkpoint at every
        // step.
        {
            let (mut dbsp, (input_handle, output_handle, sample_size_handle)) =
                circuit_fun(cconf).unwrap();
            for mut batch in input.clone() {
                let cpm = dbsp.checkpoint().run().expect("commit shouldn't fail");
                checkpoints.push(cpm);

                sample_size_handle.set_for_all(SAMPLE_SIZE);
                input_handle.append(&mut batch);
                dbsp.transaction().unwrap();

                let res = output_handle.take_from_all();
                committed.push(res[0].clone());
            }
        }
        assert_eq!(committed.len(), input.len());

        // Next, we instantiate every checkpoint and make sure the circuit state is
        // what we would expect it to be at the given point we restored it to
        let mut batches_to_insert = input.clone();
        for (i, cpm) in checkpoints.iter().enumerate() {
            let mut cconf = mkconfig(_temp.path());
            cconf.storage.as_mut().unwrap().init_checkpoint = Some(cpm.uuid);
            let (mut dbsp, (input_handle, output_handle, sample_size_handle)) =
                mkcircuit(cconf).unwrap();
            sample_size_handle.set_for_all(SAMPLE_SIZE);
            input_handle.append(&mut batches_to_insert[i]);
            dbsp.transaction().unwrap();

            let res = output_handle.take_from_all();
            let expected_zset = committed[i].clone();
            assert_eq!(expected_zset, res[0]);
        }
    }

    /// Smoke test for `gather_batches_for_checkpoint`.
    #[test]
    fn can_find_batches_for_checkpoint() {
        let _temp = tempdir().expect("Can't create temp dir for storage");
        let cconf = mkconfig(_temp.path());
        let (mut dbsp, (input_handle, _, _)) = mkcircuit(cconf).unwrap();
        let mut batch = vec![Tup2(1, Tup2(2, 1))];
        input_handle.append(&mut batch);
        dbsp.transaction().unwrap();
        let cpm = dbsp.checkpoint().run().expect("commit failed");
        let batchfiles = dbsp
            .checkpointer
            .as_ref()
            .unwrap()
            .lock()
            .unwrap()
            .gather_batches_for_checkpoint(&cpm)
            .expect("failed to gather batches");
        assert_eq!(batchfiles.len(), 1);
    }

    /// If we call commit, we should preserve the checkpoint list across circuit
    /// restarts.
    #[test]
    fn checkpoint_file() {
        let _temp = tempdir().expect("Can't create temp dir for storage");

        {
            let (mut dbsp, (_input_handle, _output_handle, sample_size_handle)) =
                mkcircuit(mkconfig(_temp.path())).unwrap();
            sample_size_handle.set_for_all(2);
            dbsp.transaction().unwrap();
            dbsp.checkpoint()
                .with_name("test-commit")
                .run()
                .expect("commit failed");
            dbsp.transaction().unwrap();
            dbsp.checkpoint().run().expect("commit failed");
        }

        {
            let (dbsp, _) = mkcircuit(mkconfig(_temp.path())).unwrap();
            let cpm = &dbsp
                .checkpointer
                .as_ref()
                .unwrap()
                .lock()
                .unwrap()
                .list_checkpoints()
                .unwrap()[0];
            assert_ne!(cpm.uuid, Uuid::nil());
            assert_eq!(cpm.identifier, Some(String::from("test-commit")));

            let cpm2 = &dbsp
                .checkpointer
                .as_ref()
                .unwrap()
                .lock()
                .unwrap()
                .list_checkpoints()
                .unwrap()[1];
            assert_ne!(cpm2.uuid, Uuid::nil());
            assert_ne!(cpm2.uuid, cpm.uuid);
            assert_eq!(cpm2.identifier, None);
        }
    }

    /// We should fail if we instantiate a circuit with the same storage
    /// directory twice because the directory is locked.
    #[test]
    fn circuit_takes_ownership_of_storage_dir() {
        let _temp = tempdir().expect("Can't create temp dir for storage");
        let (_dbsp, _) = mkcircuit(mkconfig(_temp.path())).unwrap();

        let r = Runtime::init_circuit(mkconfig(_temp.path()), |_circuit| Ok(()));
        assert!(matches!(
            r,
            Err(DbspError::Storage(StorageError::StorageLocked(_, _)))
        ));
    }

    /// We should fail if we revert to a checkpoint that doesn't exist.
    #[test]
    fn revert_to_unknown_checkpoint() {
        let _temp = tempdir().expect("Can't create temp dir for storage");
        let (dbsp, _) = mkcircuit(mkconfig(_temp.path())).unwrap();
        drop(dbsp); // makes sure we can take ownership of storage dir again

        let mut cconf = mkconfig(_temp.path());
        cconf.storage.as_mut().unwrap().init_checkpoint = Some(Uuid::now_v7()); // this checkpoint doesn't exist

        let res = mkcircuit(cconf);
        let Err(err) = res else {
            panic!("revert_to_unknown_checkpoint is supposed to fail");
        };

        assert!(matches!(
            err,
            DbspError::Storage(StorageError::CheckpointNotFound(_))
        ));
    }

    /// We panic if we initialize to a partially incomplete checkpoint.
    #[test]
    #[should_panic]
    fn revert_to_partial_checkpoint() {
        let temp = tempdir().expect("Can't create temp dir for storage");
        let (dbsp, _) = mkcircuit(mkconfig(temp.path())).unwrap();
        drop(dbsp); // makes sure we can take ownership of storage dir again

        let init_checkpoint = Uuid::now_v7(); // A made-up checkpoint, that does not have the necessary files
        let mut cconf = mkconfig(temp.path());
        cconf.storage.as_mut().unwrap().init_checkpoint = Some(init_checkpoint);
        let checkpoint_dir = temp.path().join(init_checkpoint.to_string());
        create_dir_all(checkpoint_dir).expect("can't create checkpoint dir");

        // Initializing this circuit again will panic because it won't find the
        // necessary files in the checkpoint directory.
        mkcircuit(cconf).unwrap();
    }

    fn init_test_tracing() {
        let _ = tracing_subscriber::fmt::try_init();
    }

    /// Checks that we end up cleaning old checkpoints on disk after calling
    /// `gc_checkpoint`.
    #[test]
    fn gc_commits() {
        init_test_tracing();
        let temp = tempdir().expect("Can't create temp dir for storage");
        let cconf = mkconfig(temp.path());

        fn count_directory_entries<P: AsRef<Path>>(path: P) -> io::Result<usize> {
            let mut file_count = 0;
            let entries = fs::read_dir(path)?;
            for entry in entries {
                let _entry = entry?;
                file_count += 1;
            }
            Ok(file_count)
        }

        let (mut dbsp, (input_handle, _, _)) = mkcircuit(cconf).unwrap();

        let _cpm = dbsp.checkpoint().run().expect("commit failed");
        let mut batches: Vec<Vec<Tup2<i32, Tup2<i32, i64>>>> = vec![
            vec![Tup2(1, Tup2(2, 1))],
            vec![Tup2(2, Tup2(3, 1))],
            vec![Tup2(3, Tup2(4, 1))],
            vec![Tup2(3, Tup2(4, 1))],
            vec![Tup2(1, Tup2(2, 1))],
            vec![Tup2(2, Tup2(3, 1))],
            vec![Tup2(3, Tup2(4, 1))],
            vec![Tup2(3, Tup2(4, 1))],
        ];
        for chunk in batches.chunks_mut(2) {
            input_handle.append(&mut chunk[0]);
            input_handle.append(&mut chunk[1]);
            dbsp.transaction().unwrap();
            let _cpm = dbsp.checkpoint().run().expect("commit failed");
        }

        let prev_count = count_directory_entries(temp.path()).unwrap();
        let num_checkpoints = dbsp.list_checkpoints().unwrap().len();

        assert!(num_checkpoints > Checkpointer::MIN_CHECKPOINT_THRESHOLD);

        // Only MIN_CHECKPONT_THRESHOLD checkpoints will be kept.
        let _r = dbsp.gc_checkpoint(std::collections::HashSet::new());
        let count = count_directory_entries(temp.path()).unwrap();
        assert!(count < prev_count);
        assert!(dbsp.list_checkpoints().unwrap().len() <= Checkpointer::MIN_CHECKPOINT_THRESHOLD);
    }

    /// Make sure that leftover files from uncompleted checkpoints that were
    /// written during a previous run are cleaned up when we start a new
    /// circuit with this storage directory.
    #[test]
    fn gc_on_startup() {
        init_test_tracing();

        let temp = tempdir().expect("Can't create temp dir for storage");
        let (mut dbsp, (input_handle, _, _)) = mkcircuit(mkconfig(temp.path())).unwrap();

        let mut batch: Vec<Tup2<i32, Tup2<i32, i64>>> = vec![Tup2(1, Tup2(2, 1))];
        input_handle.append(&mut batch);
        dbsp.checkpoint().run().expect("commit shouldn't fail");
        drop(dbsp);

        let incomplete_batch_path = temp.path().join("incomplete_batch.mut");
        let _ = File::create(&incomplete_batch_path).expect("can't create file");

        let incomplete_checkpoint_dir = temp.path().join(Uuid::now_v7().to_string());
        fs::create_dir(&incomplete_checkpoint_dir).expect("can't create checkpoint dir");
        let _ = File::create(incomplete_checkpoint_dir.join("filename.feldera"))
            .expect("can't create file");

        let complete_batch_unused = temp.path().join("complete_batch.feldera");
        let _ = File::create(&complete_batch_unused).expect("can't create file");

        // Initializing this circuit again will remove the
        // unnecessary files in the checkpoint directory.
        let (_dbsp, _) = mkcircuit(mkconfig(temp.path())).unwrap();

        assert!(!incomplete_checkpoint_dir.exists());
        assert!(!incomplete_batch_path.exists());
        assert!(!complete_batch_unused.exists());
    }

    /// Make sure we can take checkpoints of a simple spine and restore them.
    #[test]
    fn commit_restore() {
        init_test_tracing();
        let batches: Vec<Vec<Tup2<i32, Tup2<i32, i64>>>> = vec![
            vec![Tup2(1, Tup2(2, 1))],
            vec![Tup2(3, Tup2(4, 1))],
            vec![Tup2(5, Tup2(6, 1))],
            vec![Tup2(7, Tup2(8, 1))],
            vec![Tup2(9, Tup2(10, 1))],
        ];

        generic_checkpoint_restore(batches, mkcircuit);
    }

    // TODO: fix this test. The circuit uses integrate_trace_with_bounds,
    // which applied bounds lazily; however the correctness check assumes
    // that bounds are applied instantly.
    /// Make sure we can take checkpoints of a spine with a trace bound and
    /// restore them.
    #[test]
    #[ignore]
    fn commit_restore_bounds() {
        init_test_tracing();
        let batches: Vec<Vec<Tup2<i32, Tup2<i32, i64>>>> = vec![
            vec![Tup2(1, Tup2(2, 1))],
            vec![Tup2(7, Tup2(8, 1))],
            vec![Tup2(9, Tup2(10, 1))],
            vec![Tup2(12, Tup2(12, 1))],
            vec![Tup2(13, Tup2(13, 1))],
        ];

        generic_checkpoint_restore(batches, mkcircuit_with_bounds);
    }

    /// Make sure two circuits end up with a different fingerprint.
    #[test]
    fn fingerprint_is_different() {
        let _tempdir = tempdir().expect("Can't create temp dir for storage");
        let fid1 = mkcircuit(mkconfig(_tempdir.path()))
            .unwrap()
            .0
            .fingerprint();
        let fid2 = mkcircuit_different(mkconfig(_tempdir.path()))
            .unwrap()
            .0
            .fingerprint();
        assert_ne!(fid1, fid2);

        // Unfortunately, the fingerprint isn't perfect, e.g., it thinks these two
        // circuits are the same:
        let fid3 = mkcircuit_with_bounds(mkconfig(_tempdir.path()))
            .unwrap()
            .0
            .fingerprint();
        assert_eq!(fid1, fid3); // Ideally, should be assert_ne
    }

    /// Make sure if we create a new circuit with a different fingerprint in the
    /// same storage directory we don't allow it to start.
    #[test]
    #[should_panic]
    fn reject_different_fingerprint() {
        let _temp = tempdir().expect("Can't create temp dir for storage");
        let (mut dbsp, (input_handle, _, _)) = mkcircuit(mkconfig(_temp.path())).unwrap();
        let mut batch: Vec<Tup2<i32, Tup2<i32, i64>>> = vec![Tup2(1, Tup2(2, 1))];
        input_handle.append(&mut batch);
        let cpi = dbsp.checkpoint().run().expect("commit shouldn't fail");
        drop(dbsp);

        let mut cconf = mkconfig(_temp.path());
        cconf.storage.as_mut().unwrap().init_checkpoint = Some(cpi.uuid);
        let (dbsp_different, (_input_handle, _, _sample_size_handle)) =
            mkcircuit_different(cconf).unwrap();
        drop(dbsp_different);
    }

    /// This test exercises the checkpoint/restore path of the Z1 operator.
    #[test]
    #[allow(clippy::borrowed_box)]
    fn test_z1_checkpointing() {
        let _temp = tempdir().expect("Can't create temp dir for storage");

        //let expected_waterlines = vec![115, 115, 125, 145];
        let expected_waterlines = vec![115, 115, 125, 145];
        fn mkcircuit(
            cconf: CircuitConfig,
            mut expected_waterline: vec::IntoIter<i32>,
        ) -> (DBSPHandle, ZSetHandle<i32>) {
            Runtime::init_circuit(cconf, move |circuit| {
                let (stream, handle) = circuit.add_input_zset();
                stream
                    .waterline_monotonic(|| 0, |ts| ts + 5)
                    .inner_data()
                    .inspect(move |waterline: &Box<DynData>| {
                        if Runtime::worker_index() == 0 {
                            assert_eq!(
                                waterline.downcast_checked::<i32>(),
                                &expected_waterline.next().unwrap()
                            );
                        }
                    });
                Ok(handle)
            })
            .unwrap()
        }

        let batches = vec![
            vec![Tup2(100, 1), Tup2(110, 1), Tup2(50, 1)],
            vec![Tup2(90, 1), Tup2(90, 1), Tup2(50, 1)],
            vec![Tup2(110, 1), Tup2(120, 1), Tup2(100, 1)],
            vec![Tup2(130, 1), Tup2(140, 1), Tup2(0, 1)],
        ];

        let mut checkpoint = None;
        for (idx, mut batch) in batches.into_iter().enumerate() {
            let expected_waterlines = expected_waterlines.clone();
            let expected_waterlines: Vec<i32> = expected_waterlines[idx..].into();
            let mut cconf = mkconfig(_temp.path());
            cconf.storage.as_mut().unwrap().init_checkpoint = checkpoint;
            let (mut dbsp, input_handle) = mkcircuit(cconf, expected_waterlines.into_iter());
            input_handle.append(&mut batch);
            dbsp.transaction().unwrap();
            let cpm = dbsp.checkpoint().run().unwrap();
            checkpoint = Some(cpm.uuid);
            dbsp.kill().unwrap();
        }
    }

    /// `is_compaction_complete` / `wait_for_compaction` must converge after a
    /// compaction sweep and verify actual merging occurred.
    ///
    /// Uses in-memory storage so the test finishes quickly.
    #[test]
    fn test_is_compaction_complete() {
        use crate::circuit::GlobalNodeId;
        use crate::circuit::metadata::{MetaItem, SPINE_BATCHES_COUNT};
        use crate::utils::Tup2;
        use std::time::Duration;

        const BATCHES: usize = 30;
        const RECORDS_PER_BATCH: i32 = 500;

        let (mut dbsp, input_handle) =
            Runtime::init_circuit(CircuitConfig::with_workers(2), |circuit| {
                let (stream, handle) = circuit.add_input_indexed_zset::<i32, i32>();
                stream.shard().integrate_trace();
                Ok(handle)
            })
            .unwrap();

        // Feed enough batches to give each spine more than one batch to merge.
        for batch in 0..BATCHES as i32 {
            let mut tuples: Vec<_> = (0..RECORDS_PER_BATCH)
                .map(|r| Tup2(batch * RECORDS_PER_BATCH + r, Tup2(r, 1)))
                .collect();
            input_handle.append(&mut tuples);
            dbsp.transaction().unwrap();
        }

        // Collect per-operator spine batch counts from the profile.
        let batch_counts = |dbsp: &mut DBSPHandle| -> Vec<usize> {
            let root = GlobalNodeId::root();
            dbsp.retrieve_profile()
                .unwrap()
                .worker_profiles
                .iter()
                .flat_map(|p| p.attribute_profile(&SPINE_BATCHES_COUNT))
                .filter_map(|(id, value)| {
                    (id != root).then(|| match value {
                        MetaItem::Count(n) => n,
                        other => panic!("unexpected MetaItem: {other:?}"),
                    })
                })
                .collect()
        };

        // Before compaction there must be at least one spine with more than one
        // batch, confirming that actual merging work is needed.
        let counts_before = batch_counts(&mut dbsp);
        assert!(
            counts_before.iter().any(|&n| n > 1),
            "expected at least one spine with >1 batch before compaction, got {counts_before:?}"
        );

        // Request a full compaction and block until all spines converge.
        dbsp.start_compaction().unwrap();
        dbsp.wait_for_compaction(Duration::from_secs(60)).unwrap();

        // After convergence every spine must have at most one batch.
        let counts_after = batch_counts(&mut dbsp);
        assert!(
            counts_after.iter().all(|&n| n <= 1),
            "expected all spines to have <=1 batch after compaction, got {counts_after:?}"
        );

        dbsp.kill().unwrap();
    }
}
