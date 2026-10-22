//! Runs a circuit on several hosts in one process, for tests.
//!
//! Each host is a [Runtime] of its own.  The hosts exchange data over
//! localhost TCP, as the hosts of a multihost pipeline do.  A step on one host
//! waits for the same step on the other hosts, so a test must drive all of the
//! hosts at the same time, with one thread for each host.  [TestHosts] does
//! this for the operations that tests use frequently.

use std::{
    iter::zip,
    mem,
    net::TcpListener,
    ops::Range,
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    path::Path,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use anyhow::Result as AnyResult;
use crossbeam::channel::{Select, unbounded};
use feldera_types::config::{StorageCacheConfig, StorageConfig, StorageOptions};
use uuid::Uuid;

use crate::{
    DBSPHandle, RootCircuit, Runtime,
    circuit::{CircuitConfig, CircuitStorageConfig, Layout, RuntimeHandle},
};

/// Returns a configuration for each of `hosts` hosts that run `workers`
/// workers in total.
///
/// The hosts divide the workers as equally as possible.  When `hosts` does not
/// divide `workers`, each of the first `workers % hosts` hosts runs one worker
/// more than the other hosts.  Each host listens on a port that the kernel
/// selects.  All of the ports are bound before a runtime starts, so a host that
/// connects to a host that does not run yet waits in the accept queue.  Thus,
/// the runtimes can start one after another.
///
/// # Arguments
///
/// * `workers` - the number of workers on all of the hosts together.
/// * `hosts` - the number of hosts.
///
/// # Returns
///
/// A configuration for each host, in order of host index.  For one host, a
/// single-host configuration.
pub(crate) fn host_configs(workers: usize, hosts: usize) -> Vec<CircuitConfig> {
    assert!(
        hosts > 0 && workers >= hosts,
        "{hosts} hosts cannot run {workers} workers"
    );
    if hosts == 1 {
        return vec![CircuitConfig::with_workers(workers)];
    }

    let listeners: Vec<TcpListener> = (0..hosts)
        .map(|_| TcpListener::bind("127.0.0.1:0").expect("cannot bind a port on localhost"))
        .collect();
    let params: Vec<_> = listeners
        .iter()
        .enumerate()
        .map(|(host, listener)| {
            (
                listener.local_addr().unwrap(),
                workers / hosts + usize::from(host < workers % hosts),
            )
        })
        .collect();
    zip(&params, listeners)
        .map(|(&(address, _), listener)| {
            CircuitConfig::from(Layout::new_multihost(&params, address).unwrap())
                .with_exchange_listener(listener)
        })
        .collect()
}

/// Runs `worker` on each worker of `hosts` hosts that run `workers` workers in
/// total, and waits until all of the workers finish.
///
/// # Arguments
///
/// * `workers` - the number of workers on all of the hosts together.
/// * `hosts` - the number of hosts.
/// * `worker` - the function that each worker runs.
pub(crate) fn run_on_hosts<F>(workers: usize, hosts: usize, worker: F)
where
    F: FnOnce() + Clone + Send + 'static,
{
    let runtimes: Vec<_> = host_configs(workers, hosts)
        .into_iter()
        .map(|config| {
            let worker = worker.clone();
            Runtime::run(config, move |_parker| worker()).expect("cannot start a host")
        })
        .collect();
    join_runtimes(runtimes);
}

/// Waits for all of `runtimes` to finish.
///
/// Panics as soon as any worker panics.  Waiting for the other workers
/// first could hang the test, because they can wait forever for data
/// from the worker that panicked.  Those workers are left behind.
fn join_runtimes(runtimes: Vec<RuntimeHandle>) {
    let panics: Vec<_> = runtimes
        .iter()
        .map(|runtime| runtime.panic_receiver())
        .collect();

    // Dropping a runtime also disconnects its panic receiver, so keep the
    // runtimes alive until we are done.
    let _keep_alive: Vec<_> = runtimes
        .iter()
        .map(|runtime| runtime.runtime().clone())
        .collect();

    let (done_sender, done_receiver) = unbounded();
    for runtime in runtimes {
        let done_sender = done_sender.clone();
        thread::spawn(move || {
            let _ = done_sender.send(runtime.join().is_ok());
        });
    }

    let mut select = Select::new();
    for panic in &panics {
        select.recv(panic);
    }
    let done_index = select.recv(&done_receiver);
    for _ in 0..panics.len() {
        let operation = select.select();
        let index = operation.index();
        if index == done_index {
            assert!(operation.recv(&done_receiver).unwrap(), "worker panicked");
        } else {
            let _ = operation.recv(&panics[index]);
            panic!("worker on host {index} panicked");
        }
    }
}

/// Returns `config` with storage for `host` in a directory of its own under
/// `path`, which this creates.
///
/// Each host needs storage of its own to checkpoint and to start again from a
/// checkpoint (see [TestHosts::restart]).
///
/// # Arguments
///
/// * `config` - the configuration of the host.
/// * `path` - the directory that holds the storage of all of the hosts.
/// * `host` - the index of the host.
///
/// # Returns
///
/// The configuration, with storage.
pub(crate) fn with_host_storage(config: CircuitConfig, path: &Path, host: usize) -> CircuitConfig {
    let path = path.join(format!("host{host}"));
    std::fs::create_dir_all(&path).unwrap();
    config.with_storage(Some(
        CircuitStorageConfig::for_config(
            StorageConfig {
                path: path.to_string_lossy().into_owned(),
                cache: StorageCacheConfig::default(),
            },
            StorageOptions::default(),
        )
        .unwrap(),
    ))
}

/// Changes the configuration of a host, given the index of the host.
type Configure = dyn Fn(usize, CircuitConfig) -> CircuitConfig + Send + Sync;

/// Builds the circuit on a host.
type Constructor<T> = dyn Fn(&mut RootCircuit) -> AnyResult<T> + Send + Sync;

/// A circuit that runs on one or more hosts in this process.
///
/// The handle of a host and the value that the circuit's constructor returned
/// on that host have the same index.
pub(crate) struct TestHosts<T> {
    /// The handle of each host.
    handles: Vec<DBSPHandle>,

    /// The value that the circuit's constructor returned on each host.
    results: Vec<T>,

    /// The number of workers on all of the hosts together.
    workers: usize,

    /// What the hosts started with, to start them again (see
    /// [TestHosts::restart]).
    configure: Arc<Configure>,
    constructor: Arc<Constructor<T>>,
}

impl<T> TestHosts<T>
where
    T: Send + 'static,
{
    /// Starts the circuit that `constructor` builds on `hosts` hosts that run
    /// `workers` workers in total.
    ///
    /// # Arguments
    ///
    /// * `workers` - the number of workers on all of the hosts together.
    /// * `hosts` - the number of hosts.
    /// * `constructor` - builds the circuit on each host.
    ///
    /// # Returns
    ///
    /// The hosts, which run.
    pub(crate) fn start<F>(workers: usize, hosts: usize, constructor: F) -> Self
    where
        F: FnOnce(&mut RootCircuit) -> AnyResult<T> + Clone + Send + Sync + 'static,
    {
        Self::start_with(workers, hosts, |_host, config| config, constructor)
    }

    /// Like [TestHosts::start], but `configure` changes the configuration of
    /// each host before the host starts.
    ///
    /// # Arguments
    ///
    /// * `workers` - the number of workers on all of the hosts together.
    /// * `hosts` - the number of hosts.
    /// * `configure` - receives the index of a host and its configuration
    ///   from [host_configs], and returns the configuration to start the host
    ///   with.
    /// * `constructor` - builds the circuit on each host.
    ///
    /// # Returns
    ///
    /// The hosts, which run.
    pub(crate) fn start_with<C, F>(
        workers: usize,
        hosts: usize,
        configure: C,
        constructor: F,
    ) -> Self
    where
        C: Fn(usize, CircuitConfig) -> CircuitConfig + Send + Sync + 'static,
        F: FnOnce(&mut RootCircuit) -> AnyResult<T> + Clone + Send + Sync + 'static,
    {
        Self::start_from(
            workers,
            hosts,
            Arc::new(configure),
            Arc::new(move |circuit: &mut RootCircuit| constructor.clone()(circuit)),
            None,
        )
    }

    /// Starts the hosts, each from its checkpoint in `checkpoints` if there
    /// is one.
    ///
    /// # Arguments
    ///
    /// * `workers` - the number of workers on all of the hosts together.
    /// * `hosts` - the number of hosts.
    /// * `configure` - see [TestHosts::start_with].
    /// * `constructor` - builds the circuit on each host.
    /// * `checkpoints` - the checkpoint of each host, in order of host index,
    ///   or `None` to start without checkpoints.
    ///
    /// # Returns
    ///
    /// The hosts, which run.
    fn start_from(
        workers: usize,
        hosts: usize,
        configure: Arc<Configure>,
        constructor: Arc<Constructor<T>>,
        checkpoints: Option<&[Uuid]>,
    ) -> Self {
        let (handles, results) = host_configs(workers, hosts)
            .into_iter()
            .enumerate()
            .map(|(host, config)| {
                let mut config = configure(host, config);
                if let Some(checkpoints) = checkpoints {
                    config
                        .storage
                        .as_mut()
                        .expect("a host must have storage to start from a checkpoint")
                        .init_checkpoint = Some(checkpoints[host]);
                }
                let constructor = constructor.clone();
                Runtime::init_circuit(config, move |circuit| constructor(circuit))
                    .expect("cannot start a host")
            })
            .unzip();
        Self {
            handles,
            results,
            workers,
            configure,
            constructor,
        }
    }

    /// Returns the number of hosts.
    pub(crate) fn n_hosts(&self) -> usize {
        self.handles.len()
    }

    /// Returns the workers that `host` runs.
    pub(crate) fn workers(&self, host: usize) -> Range<usize> {
        self.handles[host].runtime().layout().local_workers()
    }

    /// Returns the value that the circuit's constructor returned on `host`.
    pub(crate) fn result(&self, host: usize) -> &T {
        &self.results[host]
    }

    /// Returns the value that the circuit's constructor returned on each host.
    pub(crate) fn results(&self) -> &[T] {
        &self.results
    }

    /// Like [TestHosts::results], for a caller that changes the values.
    pub(crate) fn results_mut(&mut self) -> &mut [T] {
        &mut self.results
    }

    /// Calls `f` for each worker, in order of worker index.
    ///
    /// # Arguments
    ///
    /// * `f` - receives the value that the circuit's constructor returned on
    ///   the host of a worker, and the index of the worker.
    ///
    /// # Returns
    ///
    /// The value that `f` returned for each worker.
    pub(crate) fn map_workers<R>(&self, mut f: impl FnMut(&T, usize) -> R) -> Vec<R> {
        let mut values = Vec::new();
        for (host, result) in self.results.iter().enumerate() {
            for worker in self.workers(host) {
                values.push(f(result, worker));
            }
        }
        values
    }

    /// Calls `f` on the handle of each host, each in a thread of its own, and
    /// waits until all of the calls return.
    ///
    /// If a call panics, this panics with the same payload at once, without
    /// waiting for the other calls, which can wait forever for a worker that
    /// panicked.  Those calls and their hosts are left behind.
    ///
    /// # Returns
    ///
    /// The value that `f` returned for each host, in order of host index.
    pub(crate) fn map<R>(
        &mut self,
        f: impl Fn(&mut DBSPHandle) -> R + Send + Sync + 'static,
    ) -> Vec<R>
    where
        R: Send + 'static,
    {
        self.map_with_index(move |_host, handle| f(handle))
    }

    /// Like [TestHosts::map], but `f` also receives the index of the host,
    /// for a call that takes a different argument on each host.
    pub(crate) fn map_with_index<R>(
        &mut self,
        f: impl Fn(usize, &mut DBSPHandle) -> R + Send + Sync + 'static,
    ) -> Vec<R>
    where
        R: Send + 'static,
    {
        // Each thread owns the handle of its host and returns it with the
        // value, so that this can return while another thread still waits.
        let f = Arc::new(f);
        let (sender, receiver) = unbounded();
        let n_hosts = self.handles.len();
        for (host, mut handle) in mem::take(&mut self.handles).into_iter().enumerate() {
            let f = f.clone();
            let sender = sender.clone();
            thread::spawn(move || {
                let value = catch_unwind(AssertUnwindSafe(|| f(host, &mut handle)));
                let _ = sender.send((host, handle, value));
            });
        }

        let mut handles: Vec<Option<DBSPHandle>> = (0..n_hosts).map(|_| None).collect();
        let mut values: Vec<Option<R>> = (0..n_hosts).map(|_| None).collect();
        for _ in 0..n_hosts {
            let (host, handle, value) = receiver.recv().unwrap();
            handles[host] = Some(handle);
            values[host] = Some(value.unwrap_or_else(|payload| resume_unwind(payload)));
        }
        self.handles = handles.into_iter().map(Option::unwrap).collect();
        values.into_iter().map(Option::unwrap).collect()
    }

    /// Like [TestHosts::map], for a function that returns nothing.
    pub(crate) fn for_each(&mut self, f: impl Fn(&mut DBSPHandle) + Send + Sync + 'static) {
        self.map(f);
    }

    /// Starts a transaction on every host.
    pub(crate) fn start_transaction(&mut self) {
        self.for_each(|handle| handle.start_transaction().unwrap());
    }

    /// Runs one step on every host.
    ///
    /// # Returns
    ///
    /// True if the step completed the commit of the current transaction.  All
    /// of the hosts complete a commit in the same step.
    pub(crate) fn step(&mut self) -> bool {
        let committed = self.map(|handle| handle.step().unwrap());
        assert!(
            committed
                .iter()
                .all(|&host_committed| host_committed == committed[0]),
            "the hosts completed the commit in different steps: {committed:?}"
        );
        committed[0]
    }

    /// Commits the current transaction on every host.
    pub(crate) fn commit_transaction(&mut self) {
        self.for_each(|handle| handle.commit_transaction().unwrap());
    }

    /// Runs a transaction of one step on every host.
    pub(crate) fn transaction(&mut self) {
        self.for_each(|handle| handle.transaction().unwrap());
    }

    /// Checkpoints every host.
    ///
    /// # Returns
    ///
    /// The checkpoint of each host, in order of host index.
    pub(crate) fn checkpoint(&mut self) -> Vec<Uuid> {
        self.map(|handle| handle.checkpoint().run().unwrap().uuid)
    }

    /// Kills every host and starts it again from its checkpoint in
    /// `checkpoints`, with the configuration and the circuit that it started
    /// with.
    ///
    /// Each host must have storage (see [with_host_storage]).
    ///
    /// # Arguments
    ///
    /// * `checkpoints` - the checkpoint of each host, in order of host index,
    ///   from [TestHosts::checkpoint].
    ///
    /// # Returns
    ///
    /// The hosts, which run.
    pub(crate) fn restart(self, checkpoints: &[Uuid]) -> Self {
        let constructor = self.constructor.clone();
        self.restart_from(checkpoints, constructor)
    }

    /// Like [TestHosts::restart], but the hosts start again with the circuit
    /// that `constructor` builds, for a test of a bootstrap from the
    /// checkpoint of a different circuit.
    ///
    /// # Arguments
    ///
    /// * `checkpoints` - the checkpoint of each host, in order of host index,
    ///   from [TestHosts::checkpoint].
    /// * `constructor` - builds the new circuit on each host.
    ///
    /// # Returns
    ///
    /// The hosts, which run the new circuit.
    pub(crate) fn restart_with<U, F>(self, checkpoints: &[Uuid], constructor: F) -> TestHosts<U>
    where
        U: Send + 'static,
        F: FnOnce(&mut RootCircuit) -> AnyResult<U> + Clone + Send + Sync + 'static,
    {
        self.restart_from(
            checkpoints,
            Arc::new(move |circuit: &mut RootCircuit| constructor.clone()(circuit)),
        )
    }

    /// Kills every host and starts it again from its checkpoint in
    /// `checkpoints`, with the configuration that it started with and the
    /// circuit that `constructor` builds.
    ///
    /// # Arguments
    ///
    /// * `checkpoints` - the checkpoint of each host, in order of host index.
    /// * `constructor` - builds the circuit on each host.
    ///
    /// # Returns
    ///
    /// The hosts, which run.
    fn restart_from<U>(self, checkpoints: &[Uuid], constructor: Arc<Constructor<U>>) -> TestHosts<U>
    where
        U: Send + 'static,
    {
        assert_eq!(checkpoints.len(), self.n_hosts());
        let (workers, hosts) = (self.workers, self.n_hosts());
        let configure = self.configure.clone();
        self.kill();
        TestHosts::start_from(workers, hosts, configure, constructor, Some(checkpoints))
    }

    /// Kills every host, each in a thread of its own, and waits until all of
    /// them stop.
    ///
    /// Then waits until the runtime of every host is gone, so that the hosts
    /// release their storage and can start again from it.  A runtime can
    /// remain for a short time after the kill, while one of its network tasks
    /// finishes a delivery.
    ///
    /// # Panics
    ///
    /// If the runtime of a host remains for longer than 10 seconds.
    pub(crate) fn kill(self) {
        let Self {
            handles, results, ..
        } = self;
        let runtimes: Vec<_> = handles
            .iter()
            .map(|handle| handle.runtime().downgrade())
            .collect();
        thread::scope(|scope| {
            for handle in handles {
                scope.spawn(move || handle.kill().unwrap());
            }
        });

        // The constructor's results, such as input handles, can hold the
        // runtime too.
        drop(results);

        let deadline = Instant::now() + Duration::from_secs(10);
        for (host, runtime) in runtimes.iter().enumerate() {
            while runtime.upgrade().is_some() {
                assert!(
                    Instant::now() < deadline,
                    "host {host} keeps its runtime after the kill"
                );
                thread::sleep(Duration::from_millis(1));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::{TestHosts, with_host_storage};
    use crate::{
        OrdZSet, OutputHandle, Runtime, ZSetHandle, typed_batch::IndexedZSetReader, utils::Tup2,
    };

    /// The handles of the circuit in [restart_keeps_the_state_of_every_host].
    type Handles = (ZSetHandle<u64>, OutputHandle<OrdZSet<u64>>);

    /// Returns the integral that the circuit in
    /// [restart_keeps_the_state_of_every_host] outputs, over all workers.
    ///
    /// The input shards the records over the workers of both hosts, so each
    /// host holds a part of the integral.
    fn integral(hosts: &TestHosts<Handles>) -> OrdZSet<u64> {
        let tuples = hosts.map_workers(|(_, output), worker| {
            output
                .take_from_worker(worker)
                .unwrap()
                .iter()
                .collect::<Vec<_>>()
        });
        OrdZSet::from_keys(
            (),
            tuples
                .into_iter()
                .flatten()
                .map(|(key, (), weight)| Tup2(key, weight))
                .collect(),
        )
    }

    /// A circuit that two hosts start again from their checkpoints keeps the
    /// state of both of them.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn restart_keeps_the_state_of_every_host() {
        let temp = tempdir().unwrap();
        let path = temp.path().to_path_buf();
        let mut hosts = TestHosts::start_with(
            4,
            2,
            move |host, config| with_host_storage(config, &path, host),
            |circuit| {
                let (input, input_handle) = circuit.add_input_zset::<u64>();
                Ok((input_handle, input.integrate().output()))
            },
        );

        for key in 0..16 {
            hosts.result(0).0.push(key, 1);
        }
        hosts.transaction();
        assert_eq!(
            integral(&hosts),
            OrdZSet::from_keys((), (0..16).map(|key| Tup2(key, 1)).collect())
        );

        let checkpoints = hosts.checkpoint();
        assert_ne!(checkpoints[0], checkpoints[1]);
        let mut hosts = hosts.restart(&checkpoints);
        for key in 16..32 {
            hosts.result(1).0.push(key, 1);
        }
        hosts.transaction();
        assert_eq!(
            integral(&hosts),
            OrdZSet::from_keys((), (0..32).map(|key| Tup2(key, 1)).collect())
        );
    }

    /// A worker panic on one host fails the transaction at once, although the
    /// other host waits forever for the worker that panicked.
    #[test]
    #[should_panic(expected = "worker 0 fails in its first step")]
    fn step_fails_on_worker_panic() {
        let mut hosts = TestHosts::start(4, 2, |circuit| {
            let (input, handle) = circuit.add_input_zset::<u64>();
            input.inspect(|_| {
                if Runtime::worker_index() == 0 {
                    panic!("worker 0 fails in its first step");
                }
            });
            Ok(handle)
        });
        hosts.transaction();
    }
}
