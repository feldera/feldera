use crate::circuit::checkpointer::{CheckpointMetadata, Checkpointer};
use crate::monitor::visual_graph::Graph;
use crate::storage::backend::StorageError;
use crate::{
    circuit::runtime::RuntimeHandle, profile::Profiler, Error as DbspError, RootCircuit, Runtime,
    RuntimeError, SchedulerError,
};
use anyhow::Error as AnyError;
use crossbeam::channel::{bounded, Receiver, Select, Sender, TryRecvError};
pub use feldera_types::config::{StorageCacheConfig, StorageConfig, StorageOptions};
use itertools::Either;
use metrics::counter;
use minitrace::collector::SpanContext;
use minitrace::local::LocalSpan;
use minitrace::Span;
use std::fs::create_dir_all;
use std::sync::Arc;
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
use uuid::Uuid;

#[cfg(doc)]
use crate::circuit::circuit_builder::Stream;
use crate::profile::{DbspProfile, GraphProfile, WorkerProfile};

use super::runtime::WorkerPanicInfo;

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
        params: &Vec<(SocketAddr, usize)>,
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
}

#[derive(Clone, Debug, Eq, PartialEq)]
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

/// A config for instantiating a multithreaded/multihost runtime to execute
/// circuits.
///
/// As opposed to `RuntimeConfig`, this struct stores state about which hosts
/// run the circuit and where they store data, e.g., state typically not
/// tunable/exposed by the user.
#[derive(Clone)]
pub struct CircuitConfig {
    /// How the circuit is laid out across one or multiple machines.
    pub layout: Layout,

    /// Optionally, CPU numbers for pinning the worker threads.
    pub pin_cpus: Vec<usize>,

    /// Storage configuration. If present, then storage is enabled..
    pub storage: Option<CircuitStorageConfig>,
}

/// Configuration for storage in a [Runtime]-hosted circuit.
#[derive(Clone, Debug)]
pub struct CircuitStorageConfig {
    /// Runner configuration.
    pub config: StorageConfig,

    /// User options.
    pub options: StorageOptions,

    /// The initial checkpoint to start the circuit from, or `None` to start
    /// fresh from a new circuit.
    pub init_checkpoint: Option<Uuid>,
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
            pin_cpus: Vec::new(),
            storage: None,
        }
    }
}

impl From<&CircuitConfig> for CircuitConfig {
    fn from(value: &CircuitConfig) -> Self {
        value.clone()
    }
}

impl From<usize> for CircuitConfig {
    fn from(n_workers: usize) -> Self {
        Self::with_workers(n_workers)
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
        let worker_ofs = config.layout.local_workers().start;

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

        let runtime = Self::run(&config, move || {
            let worker_index = Runtime::worker_index() - worker_ofs;
            let worker_index_str: &'static str = worker_index.to_string().leak();

            // Drop all but one channels.  This makes sure that if one of the worker panics
            // or exits, its channel will become disconnected.
            let init_sender = init_senders.into_iter().nth(worker_index).unwrap();
            let status_sender = status_senders.into_iter().nth(worker_index).unwrap();
            let command_receiver = command_receivers.into_iter().nth(worker_index).unwrap();

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

            while !Runtime::kill_in_progress() {
                // Wait for command.
                match command_receiver.try_recv() {
                    Ok(Command::Step(span)) => {
                        let _guard = span.set_local_parent();
                        let _worker_span = LocalSpan::enter_with_local_parent("worker-step")
                            .with_property(|| ("worker", worker_index_str));
                        let status = circuit.step().map(|_| Response::Unit);
                        // Send response.
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
                    Ok(Command::DumpProfile) => {
                        if status_sender
                            .send(Ok(Response::ProfileDump(profiler.dump_profile())))
                            .is_err()
                        {
                            return;
                        }
                    }
                    Ok(Command::RetrieveProfile) => {
                        if status_sender
                            .send(Ok(Response::Profile(profiler.profile())))
                            .is_err()
                        {
                            return;
                        }
                    }
                    Ok(Command::Commit(base)) => {
                        circuit.commit(&base).expect("commit failed");
                        if status_sender.send(Ok(Response::CheckpointCreated)).is_err() {
                            return;
                        }
                    }
                    Ok(Command::Restore(base)) => {
                        circuit.restore(&base).expect("restore failed");
                        if status_sender
                            .send(Ok(Response::CheckpointRestored))
                            .is_err()
                        {
                            return;
                        }
                    }
                    // Nothing to do: do some housekeeping and relinquish the CPU if there's none
                    // left.
                    Err(TryRecvError::Empty) => std::thread::park(),
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

        let mut dbsp = DBSPHandle::new(runtime, command_senders, status_receivers, fingerprint)?;
        if let Some(storage) = &config.storage {
            if let Some(init_checkpoint) = storage.init_checkpoint {
                dbsp.send_restore(storage.config.path().join(init_checkpoint.to_string()))?;
            }
        }

        Ok((dbsp, ret))
    }
}

#[derive(Clone)]
enum Command {
    Step(Arc<Span>),
    EnableProfiler,
    DumpProfile,
    RetrieveProfile,
    Commit(PathBuf),
    Restore(PathBuf),
}

#[derive(Debug)]
enum Response {
    Unit,
    ProfileDump(Graph),
    Profile(WorkerProfile),
    CheckpointCreated,
    CheckpointRestored,
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
    status_receivers: Vec<Receiver<Result<Response, SchedulerError>>>,

    /// For creating checkpoints, if we can.
    checkpointer: Option<Checkpointer>,
}

impl DBSPHandle {
    fn new(
        runtime: RuntimeHandle,
        command_senders: Vec<Sender<Command>>,
        status_receivers: Vec<Receiver<Result<Response, SchedulerError>>>,
        fingerprint: u64,
    ) -> Result<Self, DbspError> {
        let checkpointer = runtime
            .runtime()
            .storage_path()
            .map(|path| Checkpointer::new(path.into(), fingerprint))
            .transpose()?;
        Ok(Self {
            start_time: Instant::now(),
            runtime: Some(runtime),
            command_senders,
            status_receivers,
            checkpointer,
            runtime_elapsed: Duration::ZERO,
        })
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

    fn collect_panic_info(&self) -> Option<Vec<(usize, WorkerPanicInfo)>> {
        self.runtime
            .as_ref()
            .map(|runtime| runtime.collect_panic_info())
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

        // Receive responses.
        for _ in 0..self.status_receivers.len() {
            let ready = select.select();
            let worker = ready.index();

            match ready.recv(&self.status_receivers[worker]) {
                Err(_) => {
                    // Retrieve panic info before killing the circuit.
                    let panic_info = self.collect_panic_info().unwrap_or_default();
                    self.kill_async();

                    return Err(DbspError::Runtime(RuntimeError::WorkerPanic { panic_info }));
                }
                Ok(Err(e)) => {
                    let _ = self.kill_inner();
                    return Err(DbspError::Scheduler(e));
                }
                Ok(Ok(resp)) => handler(worker, resp),
            }
        }

        Ok(())
    }

    /// Evaluate the circuit for one clock cycle.
    pub fn step(&mut self) -> Result<(), DbspError> {
        counter!("feldera.dbsp.step").increment(1);
        let start = Instant::now();
        let span = Arc::new(Span::root("step", SpanContext::random()));
        let _guard = span.set_local_parent();
        let result = self.broadcast_command(Command::Step(span), |_, _| {});
        if let Some(handle) = self.runtime.as_ref() {
            self.runtime_elapsed +=
                start.elapsed() * handle.runtime().layout().local_workers().len() as u32 * 2;
        }
        result
    }

    /// Returns the time elapsed while the circuit is executing a step,
    /// multiplied by the number of foreground and background threads.
    pub fn runtime_elapsed(&self) -> Duration {
        self.runtime_elapsed
    }

    /// Fingerprint of this circuit.
    ///
    /// We only keep the fingerprint if the circuit has storage, since it's to
    /// make sure that the running circuit matches whatever we stored.
    pub fn fingerprint(&self) -> Option<u64> {
        self.checkpointer
            .as_ref()
            .map(|checkpointer| checkpointer.fingerprint())
    }

    /// Create a new checkpoint by taking consistent snapshot of the state in
    /// dbsp.
    pub fn commit(&mut self) -> Result<CheckpointMetadata, DbspError> {
        self.commit_as(Uuid::now_v7(), None)
    }

    /// Create a new named checkpoint by taking consistent snapshot of the state
    /// in dbsp.
    pub fn commit_named<S: Into<String> + AsRef<str>>(
        &mut self,
        name: S,
    ) -> Result<CheckpointMetadata, DbspError> {
        self.commit_as(Uuid::now_v7(), Some(name.into()))
    }

    /// Used by the checkpointer to initiate a commit on the circuit.
    pub(super) fn send_commit(&mut self, base: PathBuf) -> Result<(), DbspError> {
        self.broadcast_command(Command::Commit(base), |_, _| {})?;
        Ok(())
    }

    /// Used to reset operator state to the point of the given Commit.
    fn send_restore(&mut self, base: PathBuf) -> Result<(), DbspError> {
        self.broadcast_command(Command::Restore(base), |_, _| {})?;
        Ok(())
    }

    fn checkpointer(&mut self) -> Result<&mut Checkpointer, DbspError> {
        self.checkpointer
            .as_mut()
            .ok_or(DbspError::Storage(StorageError::StorageDisabled))
    }

    fn commit_as(
        &mut self,
        uuid: Uuid,
        identifier: Option<String>,
    ) -> Result<CheckpointMetadata, DbspError> {
        let checkpoint_dir = self.checkpointer()?.checkpoint_dir(uuid);
        create_dir_all(&checkpoint_dir)?;
        self.send_commit(checkpoint_dir)?;
        self.checkpointer().unwrap().commit(uuid, identifier)
    }

    /// List all currently available checkpoints.
    pub fn list_checkpoints(&mut self) -> Result<Vec<CheckpointMetadata>, DbspError> {
        self.checkpointer()?.list_checkpoints()
    }

    /// Remove the oldest checkpoint from the list.
    ///
    /// # Returns
    /// - Metadata of the removed checkpoint, if one was removed.
    /// - None otherwise.
    pub fn gc_checkpoint(&mut self) -> Result<Option<CheckpointMetadata>, DbspError> {
        self.checkpointer()?.gc_checkpoint()
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
        self.broadcast_command(Command::DumpProfile, |worker, resp| {
            if let Response::ProfileDump(prof) = resp {
                worker_graphs[worker] = prof;
            }
        })?;
        Ok(GraphProfile {
            elapsed_time: self.start_time.elapsed(),
            worker_graphs,
        })
    }

    pub fn retrieve_profile(&mut self) -> Result<DbspProfile, DbspError> {
        let mut profiles = vec![Default::default(); self.status_receivers.len()];

        self.broadcast_command(Command::RetrieveProfile, |worker, resp| {
            if let Response::Profile(prof) = resp {
                profiles[worker] = prof;
            }
        })?;

        Ok(DbspProfile::new(profiles))
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
}

impl Drop for DBSPHandle {
    fn drop(&mut self) {
        if self.runtime.is_some() {
            let _ = self.kill_inner();
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::fs::{create_dir_all, File};
    use std::io;
    use std::path::Path;
    use std::time::Duration;
    use std::{fs, vec};

    use super::CircuitStorageConfig;
    use crate::circuit::checkpointer::Checkpointer;
    use crate::circuit::{CircuitConfig, Layout};
    use crate::dynamic::{ClonableTrait, DowncastTrait, DynData, Erase};
    use crate::operator::Generator;
    use crate::operator::TraceBound;
    use crate::storage::backend::StorageError;
    use crate::trace::BatchReaderFactories;
    use crate::utils::Tup2;
    use crate::{
        indexed_zset, zset, Circuit, DBSPHandle, Error as DbspError, IndexedZSetHandle,
        InputHandle, OrdIndexedZSet, OrdZSet, OutputHandle, Runtime, RuntimeError, Stream,
        TypedBox, ZSetHandle, ZWeight,
    };
    use anyhow::anyhow;
    use feldera_types::config::{StorageCacheConfig, StorageConfig, StorageOptions};
    use tempfile::{tempdir, TempDir};
    use uuid::Uuid;

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

        if let DbspError::Runtime(err) = handle.step().unwrap_err() {
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

        if let DbspError::Runtime(err) = handle.step().unwrap_err() {
            // println!("error: {err}");
            matches!(err, RuntimeError::WorkerPanic { .. });
        } else {
            panic!();
        }
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
        handle.step().unwrap();
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

        handle.step().unwrap();
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

    fn mkcircuit(cconf: &CircuitConfig) -> Result<(DBSPHandle, CircuitHandle), DbspError> {
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

    fn mkcircuit_different(
        cconf: &CircuitConfig,
    ) -> Result<(DBSPHandle, CircuitHandle), DbspError> {
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
        cconf: &CircuitConfig,
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

    pub(crate) fn mkconfig() -> (TempDir, CircuitConfig) {
        let temp = tempdir().expect("Can't create temp dir for storage");
        let cconf = CircuitConfig {
            layout: Layout::new_solo(1),
            pin_cpus: Vec::new(),
            storage: Some(CircuitStorageConfig {
                config: StorageConfig {
                    path: temp.path().to_string_lossy().into_owned(),
                    cache: StorageCacheConfig::default(),
                },
                options: StorageOptions {
                    min_storage_bytes: Some(0),
                    ..StorageOptions::default()
                },
                init_checkpoint: None,
            }),
        };
        (temp, cconf)
    }

    /// Utility function that runs a circuit and takes a checkpoint at every
    /// step. It then restores the circuit to every checkpoint and checks that
    /// the state is consistent with what we would expect it to be at that
    /// point.
    fn generic_checkpoint_restore(
        input: Vec<Vec<Tup2<i32, Tup2<i32, i64>>>>,
        circuit_fun: fn(&CircuitConfig) -> Result<(DBSPHandle, CircuitHandle), DbspError>,
    ) {
        const SAMPLE_SIZE: usize = 25; // should be bigger than #keys
        assert!(input.len() < SAMPLE_SIZE, "input should be <SAMPLE_SIZE");
        let (_temp, mut cconf) = mkconfig();

        let mut committed = vec![];
        let mut checkpoints = vec![];

        // We create a circuit and push data into it, we also take a checkpoint at every
        // step.
        {
            let (mut dbsp, (input_handle, output_handle, sample_size_handle)) =
                circuit_fun(&cconf).unwrap();
            for mut batch in input.clone() {
                let cpm = dbsp.commit().expect("commit shouldn't fail");
                checkpoints.push(cpm);

                sample_size_handle.set_for_all(SAMPLE_SIZE);
                input_handle.append(&mut batch);
                dbsp.step().unwrap();

                let res = output_handle.take_from_all();
                committed.push(res[0].clone());
            }
        }
        assert_eq!(committed.len(), input.len());

        // Next, we instantiate every checkpoint and make sure the circuit state is
        // what we would expect it to be at the given point we restored it to
        let mut batches_to_insert = input.clone();
        for (i, cpm) in checkpoints.iter().enumerate() {
            cconf.storage.as_mut().unwrap().init_checkpoint = Some(cpm.uuid);
            let (mut dbsp, (input_handle, output_handle, sample_size_handle)) =
                mkcircuit(&cconf).unwrap();
            sample_size_handle.set_for_all(SAMPLE_SIZE);
            input_handle.append(&mut batches_to_insert[i]);
            dbsp.step().unwrap();

            let res = output_handle.take_from_all();
            let expected_zset = committed[i].clone();
            assert_eq!(expected_zset, res[0]);
        }
    }

    /// Smoke test for `gather_batches_for_checkpoint`.
    #[test]
    fn can_find_batches_for_checkpoint() {
        let (_temp, cconf) = mkconfig();
        let (mut dbsp, (input_handle, _, _)) = mkcircuit(&cconf).unwrap();
        let mut batch = vec![Tup2(1, Tup2(2, 1))];
        input_handle.append(&mut batch);
        dbsp.step().unwrap();
        let cpm = dbsp.commit().expect("commit failed");
        let batchfiles = dbsp
            .checkpointer
            .as_ref()
            .unwrap()
            .gather_batches_for_checkpoint(&cpm)
            .expect("failed to gather batches");
        assert_eq!(batchfiles.len(), 1);
    }

    /// If we call commit, we should preserve the checkpoint list across circuit
    /// restarts.
    #[test]
    fn checkpoint_file() {
        let (_temp, cconf) = mkconfig();

        {
            let (mut dbsp, (_input_handle, _output_handle, sample_size_handle)) =
                mkcircuit(&cconf).unwrap();
            sample_size_handle.set_for_all(2);
            dbsp.step().unwrap();
            dbsp.commit_named("test-commit").expect("commit failed");
            dbsp.step().unwrap();
            dbsp.commit().expect("commit failed");
        }

        {
            let (dbsp, _) = mkcircuit(&cconf).unwrap();
            let cpm = &dbsp
                .checkpointer
                .as_ref()
                .unwrap()
                .list_checkpoints()
                .unwrap()[0];
            assert_ne!(cpm.uuid, Uuid::nil());
            assert_eq!(cpm.identifier, Some(String::from("test-commit")));

            let cpm2 = &dbsp
                .checkpointer
                .as_ref()
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
        let (_temp, cconf) = mkconfig();
        let (_dbsp, _) = mkcircuit(&cconf).unwrap();

        let r = Runtime::init_circuit(cconf, |_circuit| Ok(()));
        assert!(matches!(
            r,
            Err(DbspError::Storage(StorageError::StorageLocked(_, _)))
        ));
    }

    /// We should fail if we revert to a checkpoint that doesn't exist.
    #[test]
    fn revert_to_unknown_checkpoint() {
        let (_temp, mut cconf) = mkconfig();
        let (dbsp, _) = mkcircuit(&cconf).unwrap();
        drop(dbsp); // makes sure we can take ownership of storage dir again

        cconf.storage.as_mut().unwrap().init_checkpoint = Some(Uuid::now_v7()); // this checkpoint doesn't exist

        let res = mkcircuit(&cconf);
        assert!(matches!(
            res,
            Err(DbspError::Storage(StorageError::CheckpointNotFound(_)))
        ));
    }

    /// We panic if we initialize to a partially incomplete checkpoint.
    #[test]
    #[should_panic]
    fn revert_to_partial_checkpoint() {
        let (temp, mut cconf) = mkconfig();
        let (dbsp, _) = mkcircuit(&cconf).unwrap();
        drop(dbsp); // makes sure we can take ownership of storage dir again

        let init_checkpoint = Uuid::now_v7(); // A made-up checkpoint, that does not have the necessary files
        cconf.storage.as_mut().unwrap().init_checkpoint = Some(init_checkpoint);
        let checkpoint_dir = temp.path().join(init_checkpoint.to_string());
        create_dir_all(checkpoint_dir).expect("can't create checkpoint dir");

        // Initializing this circuit again will panic because it won't find the
        // necessary files in the checkpoint directory.
        mkcircuit(&cconf).unwrap();
    }

    /// Checks that we end up cleaning old checkpoints on disk after calling
    /// `gc_checkpoint`.
    #[test]
    fn gc_commits() {
        let _r = env_logger::try_init();
        let (temp, cconf) = mkconfig();

        fn count_directory_entries<P: AsRef<Path>>(path: P) -> io::Result<usize> {
            let mut file_count = 0;
            let entries = fs::read_dir(path)?;
            for entry in entries {
                let _entry = entry?;
                file_count += 1;
            }
            Ok(file_count)
        }

        let (mut dbsp, (input_handle, _, _)) = mkcircuit(&cconf).unwrap();

        let _cpm = dbsp.commit().expect("commit failed");
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
            dbsp.step().unwrap();
            let _cpm = dbsp.commit().expect("commit failed");
        }

        let mut prev_count = count_directory_entries(temp.path()).unwrap();
        let num_checkpoints = dbsp.list_checkpoints().unwrap().len();
        for _i in 0..num_checkpoints - Checkpointer::MIN_CHECKPOINT_THRESHOLD {
            let _r = dbsp.gc_checkpoint();
            let count = count_directory_entries(temp.path()).unwrap();
            assert!(count < prev_count);
            prev_count = count;
        }
    }

    /// Make sure that leftover files from uncompleted checkpoints that were
    /// written during a previous run are cleaned up when we start a new
    /// circuit with this storage directory.
    #[test]
    fn gc_on_startup() {
        let _r = env_logger::try_init();

        let (temp, cconf) = mkconfig();
        let (mut dbsp, (input_handle, _, _)) = mkcircuit(&cconf).unwrap();

        let mut batch: Vec<Tup2<i32, Tup2<i32, i64>>> = vec![Tup2(1, Tup2(2, 1))];
        input_handle.append(&mut batch);
        dbsp.commit().expect("commit shouldn't fail");
        drop(dbsp);

        let incomplete_batch_path = temp.path().join("incomplete_batch.mut");
        let _ = File::create(&incomplete_batch_path).expect("can't create file");

        let incomplete_checkpoint_dir = temp.path().join(Uuid::now_v7().to_string());
        fs::create_dir(&incomplete_checkpoint_dir).expect("can't create checkpoint dir");

        let complete_batch_unused = temp.path().join("complete_batch.feldera");
        let _ = File::create(&complete_batch_unused).expect("can't create file");

        // Initializing this circuit again will remove the
        // unnecessary files in the checkpoint directory.
        let (_dbsp, _) = mkcircuit(&cconf).unwrap();

        assert!(!incomplete_checkpoint_dir.exists());
        assert!(!incomplete_batch_path.exists());
        assert!(!complete_batch_unused.exists());
    }

    /// Make sure we can take checkpoints of a simple spine and restore them.
    #[test]
    fn commit_restore() {
        let _r = env_logger::try_init();
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
        let _r = env_logger::try_init();
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
        let (_tempdir, cconf) = mkconfig();
        let fid1 = mkcircuit(&cconf).unwrap().0.fingerprint().unwrap();
        let fid2 = mkcircuit_different(&cconf)
            .unwrap()
            .0
            .fingerprint()
            .unwrap();
        assert_ne!(fid1, fid2);

        // Unfortunately, the fingerprint isn't perfect, e.g., it thinks these two
        // circuits are the same:
        let fid3 = mkcircuit_with_bounds(&cconf)
            .unwrap()
            .0
            .fingerprint()
            .unwrap();
        assert_eq!(fid1, fid3); // Ideally, should be assert_ne
    }

    /// Make sure if we create a new circuit with a different fingerprint in the
    /// same storage directory we don't allow it to start.
    #[test]
    #[should_panic]
    fn reject_different_fingerprint() {
        let (_temp, mut cconf) = mkconfig();
        let (mut dbsp, (input_handle, _, _)) = mkcircuit(&cconf).unwrap();
        let mut batch: Vec<Tup2<i32, Tup2<i32, i64>>> = vec![Tup2(1, Tup2(2, 1))];
        input_handle.append(&mut batch);
        let cpi = dbsp.commit().expect("commit shouldn't fail");
        drop(dbsp);

        cconf.storage.as_mut().unwrap().init_checkpoint = Some(cpi.uuid);
        let (dbsp_different, (_input_handle, _, _sample_size_handle)) =
            mkcircuit_different(&cconf).unwrap();
        drop(dbsp_different);
    }

    /// This test exercises the checkpoint/restore path of the Z1 operator.
    #[test]
    #[allow(clippy::borrowed_box)]
    fn test_z1_checkpointing() {
        let (_temp, mut cconf) = mkconfig();

        //let expected_waterlines = vec![115, 115, 125, 145];
        let expected_waterlines = vec![115, 115, 125, 145];
        fn mkcircuit(
            cconf: &CircuitConfig,
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

        for (idx, mut batch) in batches.into_iter().enumerate() {
            let expected_waterlines = expected_waterlines.clone();
            let expected_waterlines: Vec<i32> = expected_waterlines[idx..].into();
            let (mut dbsp, input_handle) = mkcircuit(&cconf, expected_waterlines.into_iter());
            input_handle.append(&mut batch);
            dbsp.step().unwrap();
            let cpm = dbsp.commit().unwrap();
            cconf.storage.as_mut().unwrap().init_checkpoint = Some(cpm.uuid);
            dbsp.kill().unwrap();
        }
    }

    /// This test exercises the checkpoint/restore path of the Window operator.
    #[test]
    fn window_checkpointing() {
        type Time = u64;

        let (_temp, mut cconf) = mkconfig();
        for step_idx in 0..6 {
            let (mut circuit, ()) = Runtime::init_circuit(&cconf, move |circuit| {
                let input = vec![
                    zset! {
                    // old value before the first window, should never appear in the output.
                    Tup2(800, "800".to_string()) => 1i64,
                    Tup2(900, "900".to_string()) => 1,
                    Tup2(950, "950".to_string()) => 1,
                    Tup2(999, "999".to_string()) => 1,
                    // will appear in the next window
                    Tup2(1000, "1000".to_string()) => 1
                },
                    zset! {
                    // old value before the first window
                    Tup2(700, "700".to_string()) => 1,
                    // too late, the window already moved forward
                    Tup2(900, "900".to_string()) => 1,
                    Tup2(901, "901".to_string()) => 1,
                    Tup2(999, "999".to_string()) => 1,
                    Tup2(1000, "1000".to_string()) => 1,
                    Tup2(1001, "1001".to_string()) => 1, // will appear in the next window
                    Tup2(1002, "1002".to_string()) => 1, // will appear two windows later
                    Tup2(1003, "1003".to_string()) => 1, // will appear three windows later
                },
                    zset! { Tup2(1004, "1004".to_string()) => 1 }, // no new values in this window
                    zset! {},
                    zset! {},
                    zset! {},
                ];

                let output = vec![
                    indexed_zset! { 900 => {"900".to_string() => 1} , 950 => {"950".to_string() => 1} , 999 => {"999".to_string() => 1} },
                    indexed_zset! { 900 => {"900".to_string() => -1} , 901 => {"901".to_string() => 1} , 999 => {"999".to_string() => 1} , 1000 => {"1000".to_string() => 2} },
                    indexed_zset! { 901 => {"901".to_string() => -1} , 1001 => {"1001".to_string() => 1} },
                    indexed_zset! { 1002 => {"1002".to_string() => 1} },
                    indexed_zset! { 1003 => {"1003".to_string() => 1} },
                    indexed_zset! { 1004 => {"1004".to_string() => 1} },
                ];

                let bounds: Stream<_, (TypedBox<Time, DynData>, TypedBox<Time, DynData>)> = circuit.add_source(Generator::new(move || {
                    let clock = 1000 + step_idx as Time;

                    (TypedBox::new(clock - 100), TypedBox::new(clock))
                }));

                let index1: Stream<_, OrdIndexedZSet<Time, String>> = circuit
                    .add_source(Generator::new(move || input[step_idx].clone()))
                    .map_index(|Tup2(k, v)| (*k, v.clone()));
                index1
                    .window((true, false), &bounds)
                    .inspect(move |batch| assert_eq!(batch, &output[step_idx]));
                Ok(())
            })
                .unwrap();

            circuit.step().unwrap();

            let cpm = circuit.commit().unwrap();
            cconf.storage.as_mut().unwrap().init_checkpoint = Some(cpm.uuid);
            circuit.kill().unwrap();
        }
    }
}
