use crate::{
    circuit::runtime::RuntimeHandle, profile::Profiler, Circuit, Error as DBSPError, Runtime,
    RuntimeError, SchedulerError,
};
use crossbeam::channel::{bounded, Receiver, Sender, TryRecvError};
use std::{fs, fs::create_dir_all, path::Path, thread::Result as ThreadResult, time::Instant};

impl Runtime {
    /// Instantiate a circuit in a multithreaded runtime.
    ///
    /// Creates a multithreaded runtime with `nworkers` worker threads and
    /// instantiates identical circuits in each worker, using `constructor`
    /// closure. Returns a [`DBSPHandle`] that can be used to control
    /// the circuit and a user-defined value returned by the constructor.
    /// This value typically contains one or more input handles used to
    /// push data to the circuit at runtime (see
    /// [`Circuit::add_input_stream`], [`Circuit::add_input_zset`], and related
    /// methods).
    ///
    /// To ensure that the multithreaded runtime has identical input/output
    /// behavior to a single-threaded circuit, the `constructor` closure
    /// must satisfy certain constraints.  Most importantly, it must create
    /// identical circuits in all worker threads, adding and connecting
    /// operators in the same order.  This ensures that operators that shard
    /// their inputs across workers, e.g.,
    /// [`Stream::join`](`crate::Stream::join`), work correctly.
    ///
    /// TODO: Document other requirements.  Not all operators are currently
    /// thread-safe.
    pub fn init_circuit<F, T>(nworkers: usize, constructor: F) -> Result<(DBSPHandle, T), DBSPError>
    where
        F: FnOnce(&mut Circuit<()>) -> T + Clone + Send + 'static,
        T: Clone + Send + 'static,
    {
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

        let runtime = Self::run(nworkers, move || {
            let worker_index = Runtime::worker_index();

            // Drop all but one channels.  This makes sure that if one of the worker panics
            // or exits, its channel will become disonnected.
            let init_sender = init_senders.into_iter().nth(worker_index).unwrap();
            let status_sender = status_senders.into_iter().nth(worker_index).unwrap();
            let command_receiver = command_receivers.into_iter().nth(worker_index).unwrap();

            let (circuit, profiler) = match Circuit::build(|circuit| {
                let profiler = Profiler::new(circuit);
                let res = constructor(circuit);
                (res, profiler)
            }) {
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
                        };
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
                            .send(Ok(Response::Profile(profiler.dump_profile())))
                            .is_err()
                        {
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

        for (worker, receiver) in init_receivers.iter().enumerate() {
            match receiver.recv() {
                Ok(Err(scheduler_error)) => {
                    init_status.push(Err(DBSPError::Scheduler(scheduler_error)))
                }
                Ok(Ok(ret)) => init_status.push(Ok(ret)),
                Err(_) => {
                    init_status.push(Err(DBSPError::Runtime(RuntimeError::WorkerPanic(worker))))
                }
            }
        }

        // On error, kill the runtime.
        if init_status.iter().any(Result::is_err) {
            let error = init_status
                .into_iter()
                .find_map(|status| status.err())
                .unwrap();
            let _ = runtime.kill();
            return Err(error);
        }

        let dbsp = DBSPHandle::new(runtime, command_senders, status_receivers);

        // `constructor` should return identical results in all workers.  Use
        // worker 0 output.
        Ok((dbsp, init_status[0].as_ref().unwrap().clone()))
    }
}

#[derive(Clone)]
enum Command {
    Step,
    EnableProfiler,
    DumpProfile,
}

enum Response {
    Unit,
    Profile(String),
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
        }
    }

    fn kill_inner(&mut self) -> ThreadResult<()> {
        self.command_senders.clear();
        self.status_receivers.clear();
        self.runtime.take().unwrap().kill()
    }

    fn broadcast_command<F>(&mut self, command: Command, mut handler: F) -> Result<(), DBSPError>
    where
        F: FnMut(Response),
    {
        if self.runtime.is_none() {
            return Err(DBSPError::Runtime(RuntimeError::Killed));
        }

        // Send command.
        for (worker, sender) in self.command_senders.iter().enumerate() {
            if matches!(sender.send(command.clone()), Err(_)) {
                let _ = self.kill_inner();
                return Err(DBSPError::Runtime(RuntimeError::WorkerPanic(worker)));
            }
            self.runtime.as_ref().unwrap().unpark_worker(worker);
        }

        // Receive responses.
        for (worker, receiver) in self.status_receivers.iter().enumerate() {
            match receiver.recv() {
                Err(_) => {
                    let _ = self.kill_inner();
                    return Err(DBSPError::Runtime(RuntimeError::WorkerPanic(worker)));
                }
                Ok(Err(e)) => {
                    let _ = self.kill_inner();
                    return Err(DBSPError::Scheduler(e));
                }
                Ok(Ok(resp)) => handler(resp),
            }
        }

        Ok(())
    }

    pub fn num_workers(&self) -> usize {
        self.status_receivers.len()
    }

    /// Evaluate the circuit for one clock cycle.
    pub fn step(&mut self) -> Result<(), DBSPError> {
        self.broadcast_command(Command::Step, |_| {})
    }

    /// Enable CPU profiler.
    ///
    /// Enable recording of CPU usage info.  When CPU profiling is enabled,
    /// [`Self::dump_profile`] outputs CPU usage info along with memory
    /// usage and other circuit metadata.  CPU profiling introduces small
    /// runtime overhead.
    pub fn enable_cpu_profiler(&mut self) -> Result<(), DBSPError> {
        self.broadcast_command(Command::EnableProfiler, |_| {})
    }

    /// Dump profiling information to the specified directory.
    ///
    /// Creates `dir_path` if it doesn't exist.  For each worker thread, creates
    /// `dir_path/<timestamp>/<worker>.dot` file containing worker profile in
    /// the graphviz format.  If CPU profiling was enabled (see
    /// [`Self::enable_cpu_profiler`]), the profile will contain both CPU and
    /// memory usage information; otherwise only memory usage details are
    /// reported.
    pub fn dump_profile<P: AsRef<Path>>(&mut self, dir_path: P) -> Result<(), DBSPError> {
        let elapsed = self.start_time.elapsed().as_micros();
        let mut profiles = Vec::with_capacity(self.num_workers());

        let dir_path = dir_path.as_ref().join(elapsed.to_string());
        create_dir_all(&dir_path)?;

        self.broadcast_command(Command::DumpProfile, |resp| {
            if let Response::Profile(prof) = resp {
                profiles.push(prof);
            }
        })?;

        for (worker, profile) in profiles.into_iter().enumerate() {
            fs::write(dir_path.join(format!("{worker}.dot")), profile)?;
        }

        Ok(())
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
    use crate::{operator::Generator, Error as DBSPError, Runtime, RuntimeError};

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
        });

        if let DBSPError::Runtime(err) = res.unwrap_err() {
            assert_eq!(err, RuntimeError::WorkerPanic(0));
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
        })
        .unwrap();

        if let DBSPError::Runtime(err) = handle.step().unwrap_err() {
            assert_eq!(err, RuntimeError::WorkerPanic(0));
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
        })
        .unwrap();

        handle.step().unwrap();
    }
}
