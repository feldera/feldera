//! Runs a circuit on several hosts in one process, for tests.
//!
//! Each host is a [Runtime] of its own.  The hosts exchange data over
//! localhost TCP, as the hosts of a multihost pipeline do.  A step on one host
//! waits for the same step on the other hosts, so a test must drive all of the
//! hosts at the same time, with one thread for each host.  [TestHosts] does
//! this for the operations that tests use frequently.

use std::{
    iter::zip,
    net::TcpListener,
    panic::resume_unwind,
    thread,
    time::{Duration, Instant},
};

use anyhow::Result as AnyResult;
use crossbeam::channel::{Select, unbounded};

use crate::{
    DBSPHandle, RootCircuit, Runtime,
    circuit::{CircuitConfig, Layout, RuntimeHandle},
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

/// A circuit that runs on one or more hosts in this process.
///
/// The handle of a host and the value that the circuit's constructor returned
/// on that host have the same index.
pub(crate) struct TestHosts<T> {
    /// The handle of each host.
    handles: Vec<DBSPHandle>,

    /// The value that the circuit's constructor returned on each host.
    results: Vec<T>,
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
        F: FnOnce(&mut RootCircuit) -> AnyResult<T> + Clone + Send + 'static,
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
        C: Fn(usize, CircuitConfig) -> CircuitConfig,
        F: FnOnce(&mut RootCircuit) -> AnyResult<T> + Clone + Send + 'static,
    {
        let (handles, results) = host_configs(workers, hosts)
            .into_iter()
            .enumerate()
            .map(|(host, config)| {
                Runtime::init_circuit(configure(host, config), constructor.clone())
                    .expect("cannot start a host")
            })
            .unzip();
        Self { handles, results }
    }

    /// Returns the number of hosts.
    pub(crate) fn n_hosts(&self) -> usize {
        self.handles.len()
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

    /// Calls `f` on the handle of each host, each in a thread of its own, and
    /// waits until all of the calls return.
    ///
    /// If a call panics, this panics with the same payload after all of the
    /// calls end.
    ///
    /// # Returns
    ///
    /// The value that `f` returned for each host, in order of host index.
    pub(crate) fn map<R>(&mut self, f: impl Fn(&mut DBSPHandle) -> R + Sync) -> Vec<R>
    where
        R: Send,
    {
        thread::scope(|scope| {
            let threads: Vec<_> = self
                .handles
                .iter_mut()
                .map(|handle| {
                    let f = &f;
                    scope.spawn(move || f(handle))
                })
                .collect();
            threads
                .into_iter()
                .map(|thread| {
                    thread
                        .join()
                        .unwrap_or_else(|payload| resume_unwind(payload))
                })
                .collect()
        })
    }

    /// Like [TestHosts::map], for a function that returns nothing.
    pub(crate) fn for_each(&mut self, f: impl Fn(&mut DBSPHandle) + Sync) {
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
        let Self { handles, results } = self;
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
