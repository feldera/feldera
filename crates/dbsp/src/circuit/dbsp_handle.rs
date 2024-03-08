use crate::{
    circuit::runtime::RuntimeHandle, profile::Profiler, Error as DBSPError, RootCircuit, Runtime,
    RuntimeError, SchedulerError,
};
use anyhow::Error as AnyError;
use core::fmt;
use crossbeam::channel::{bounded, Receiver, Select, Sender, TryRecvError};
use itertools::Either;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::{
    collections::HashSet,
    error::Error as StdError,
    fmt::{Debug, Display, Error as FmtError, Formatter},
    fs,
    fs::create_dir_all,
    iter::empty,
    net::SocketAddr,
    ops::Range,
    path::{Path, PathBuf},
    thread::Result as ThreadResult,
    time::Instant,
};

#[cfg(doc)]
use crate::circuit::circuit_builder::Stream;
use crate::profile::{DbspProfile, WorkerProfile};

use super::runtime::WorkerPanicInfo;

/// A host for some workers in the [`Layout`] for a multi-host DBSP circuit.
#[allow(clippy::manual_non_exhaustive)]
#[derive(Clone, Serialize, Deserialize)]
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
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Host")
            .field("address", &self.address)
            .field("workers", &self.workers)
            .finish()
    }
}

/// How a DBSP circuit is laid out across one or more machines.
#[derive(Clone, Debug, Serialize, Deserialize)]
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
    /// listens on its own address and connects to all of the other addresses.
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
pub struct CircuitConfig {
    pub layout: Layout,
    pub storage: Option<String>,
}

impl CircuitConfig {
    pub fn with_workers(n: usize) -> Self {
        Self {
            layout: Layout::new_solo(n),
            storage: None,
        }
    }
}

impl IntoCircuitConfig for CircuitConfig {
    fn layout(&self) -> Layout {
        self.layout.clone()
    }

    fn storage(&self) -> Option<String> {
        self.storage.clone()
    }
}

/// Convenience trait that allows specifying a [`Layout`] as a `usize` for a
/// single-machine layout with the specified number of worker threads,
pub trait IntoCircuitConfig {
    fn layout(&self) -> Layout;

    fn storage(&self) -> Option<String> {
        None
    }
}

impl IntoCircuitConfig for usize {
    fn layout(&self) -> Layout {
        Layout::new_solo(*self)
    }
}

impl IntoCircuitConfig for Layout {
    fn layout(&self) -> Layout {
        self.clone()
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
        cconf: impl IntoCircuitConfig,
        constructor: F,
    ) -> Result<(DBSPHandle, T), DBSPError>
    where
        F: FnOnce(&mut RootCircuit) -> Result<T, AnyError> + Clone + Send + 'static,
        T: Send + 'static,
    {
        let layout = cconf.layout();
        let nworkers = layout.local_workers().len();
        let worker_ofs = layout.local_workers().start;

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

        let runtime = Self::run(cconf, move || {
            let worker_index = Runtime::worker_index() - worker_ofs;

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
                    if init_sender.send(Ok(res)).is_err() {
                        return;
                    }
                    (circuit, profiler)
                }
                Err(e) => {
                    let _ = init_sender.send(Err(e));
                    return;
                }
            };

            // TODO: uncomment this when we have support for background compaction.
            // let mut moregc = true;

            while !Runtime::kill_in_progress() {
                // Wait for command.
                match command_receiver.try_recv() {
                    Ok(Command::Step) => {
                        //moregc = true;
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
                    Ok(Command::Commit(cid)) => {
                        circuit.commit(cid).expect("commit failed");
                        if status_sender.send(Ok(Response::CheckpointCreated)).is_err() {
                            return;
                        }
                    }
                    // Nothing to do: do some housekeeping and relinquish the CPU if there's none
                    // left.
                    Err(TryRecvError::Empty) => {
                        // GC
                        /*if moregc {
                            moregc = circuit.gc();
                        } else {*/
                        Runtime::parker().with(|parker| parker.park());
                        //}
                    }
                    Err(_) => {
                        break;
                    }
                }
            }
        });

        // Receive initialization status from all workers.

        let mut init_status = Vec::with_capacity(nworkers);

        for receiver in init_receivers.iter() {
            match receiver.recv() {
                Ok(Err(error)) => init_status.push(Some(Err(error))),
                Ok(Ok(ret)) => init_status.push(Some(Ok(ret))),
                Err(_) => {
                    let panic_info = runtime.collect_panic_info();
                    init_status.push(Some(Err(DBSPError::Runtime(RuntimeError::WorkerPanic {
                        panic_info,
                    }))))
                }
            }
        }

        // On error, kill the runtime.
        if init_status
            .iter()
            .any(|status| status.as_ref().unwrap().is_err())
        {
            let error = init_status
                .into_iter()
                .find_map(|status| status.unwrap().err())
                .unwrap();
            let _ = runtime.kill();
            return Err(error);
        }

        let dbsp = DBSPHandle::new(runtime, command_senders, status_receivers);

        let result = init_status[0].take();

        // `constructor` should return identical results in all workers.  Use
        // worker 0 output.
        Ok((dbsp, result.unwrap().unwrap()))
    }
}

#[derive(Clone)]
enum Command {
    Step,
    EnableProfiler,
    DumpProfile,
    RetrieveProfile,
    Commit(u64),
}

enum Response {
    Unit,
    ProfileDump(String),
    Profile(WorkerProfile),
    CheckpointCreated,
}

/// A handle to control the execution of a circuit in a multithreaded runtime.
#[derive(Debug)]
pub struct DBSPHandle {
    // Time when the handle was created.
    start_time: Instant,
    runtime: Option<RuntimeHandle>,
    // Channels used to send commands to workers.
    command_senders: Vec<Sender<Command>>,
    // Channels used to receive command completion status from
    // workers.
    status_receivers: Vec<Receiver<Result<Response, SchedulerError>>>,
    step_id: u64,
    checkpoint_list: VecDeque<u64>,
}

impl DBSPHandle {
    fn new(
        runtime: RuntimeHandle,
        command_senders: Vec<Sender<Command>>,
        status_receivers: Vec<Receiver<Result<Response, SchedulerError>>>,
    ) -> Self {
        Self {
            start_time: Instant::now(),
            runtime: Some(runtime),
            command_senders,
            status_receivers,
            step_id: 0,
            checkpoint_list: VecDeque::new(),
        }
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

    fn broadcast_command<F>(&mut self, command: Command, mut handler: F) -> Result<(), DBSPError>
    where
        F: FnMut(usize, Response),
    {
        if self.runtime.is_none() {
            return Err(DBSPError::Runtime(RuntimeError::Terminated));
        }

        // Send command.
        for (worker, sender) in self.command_senders.iter().enumerate() {
            if sender.send(command.clone()).is_err() {
                let panic_info = self.collect_panic_info().unwrap_or_default();

                // Worker thread panicked. Exit without waiting for all workers to exit
                // to avoid deadlocks due to workers waiting for each other.
                self.kill_async();
                return Err(DBSPError::Runtime(RuntimeError::WorkerPanic { panic_info }));
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

                    return Err(DBSPError::Runtime(RuntimeError::WorkerPanic { panic_info }));
                }
                Ok(Err(e)) => {
                    let _ = self.kill_inner();
                    return Err(DBSPError::Scheduler(e));
                }
                Ok(Ok(resp)) => handler(worker, resp),
            }
        }

        Ok(())
    }

    /// Evaluate the circuit for one clock cycle.
    pub fn step(&mut self) -> Result<(), DBSPError> {
        self.step_id += 1;
        self.broadcast_command(Command::Step, |_, _| {})
    }

    /// Create a new checkpoint by taking consistent snapshot of the state in
    /// dbsp.
    ///
    /// ## Returns
    /// The ID of the new checkpoint.
    pub fn commit(&mut self) -> Result<u64, DBSPError> {
        self.broadcast_command(Command::Commit(self.step_id), |_, _| {})?;
        // TODO: I think broadcast_command is synchronous, we can just return
        // `self.step_id` directly.
        // TODO: we need to handle the case where the commit fails in one of the
        // worker threads. The `handler` seemed awkward to use so I ignored it for now.

        self.checkpoint_list.push_back(self.step_id);
        Ok(self.step_id)
    }

    /// List all currently available checkpoints.
    pub fn list_checkpoints(&mut self) -> Result<Vec<u64>, DBSPError> {
        Ok(self.checkpoint_list.clone().into())
    }

    /// Enable CPU profiler.
    ///
    /// Enable recording of CPU usage info.  When CPU profiling is enabled,
    /// [`Self::dump_profile`] outputs CPU usage info along with memory
    /// usage and other circuit metadata.  CPU profiling introduces small
    /// runtime overhead.
    pub fn enable_cpu_profiler(&mut self) -> Result<(), DBSPError> {
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
    pub fn dump_profile<P: AsRef<Path>>(&mut self, dir_path: P) -> Result<PathBuf, DBSPError> {
        let elapsed = self.start_time.elapsed().as_micros();
        let mut profiles = vec![Default::default(); self.status_receivers.len()];

        let dir_path = dir_path.as_ref().join(elapsed.to_string());
        create_dir_all(&dir_path)?;

        self.broadcast_command(Command::DumpProfile, |worker, resp| {
            if let Response::ProfileDump(prof) = resp {
                profiles[worker] = prof;
            }
        })?;

        for (worker, profile) in profiles.into_iter().enumerate() {
            fs::write(dir_path.join(format!("{worker}.dot")), profile)?;
        }

        Ok(dir_path)
    }

    pub fn retrieve_profile(&mut self) -> Result<DbspProfile, DBSPError> {
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
mod tests {
    use std::time::Duration;

    use crate::{operator::Generator, Circuit, Error as DBSPError, Runtime, RuntimeError};
    use anyhow::anyhow;

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

        if let DBSPError::Runtime(err) = res.unwrap_err() {
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

        if let DBSPError::Runtime(err) = handle.step().unwrap_err() {
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

        if let DBSPError::Runtime(err) = handle.step().unwrap_err() {
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
            Err(DBSPError::Constructor(msg)) => assert_eq!(msg.to_string(), "constructor failed"),
            _ => panic!(),
        }
    }
}
