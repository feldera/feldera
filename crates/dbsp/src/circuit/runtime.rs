//! A multithreaded runtime for evaluating DBSP circuits in a data-parallel
//! fashion.

use crate::{
    storage::{
        backend::{new_default_backend, tempdir_for_thread, Backend},
        buffer_cache::BufferCache,
        file::cache::FileCacheEntry,
    },
    DetailedError,
};
use crossbeam::channel::bounded;
use crossbeam_utils::sync::{Parker, Unparker};
use once_cell::sync::Lazy;
use serde::Serialize;
use std::{
    backtrace::Backtrace,
    borrow::Cow,
    cell::{Cell, RefCell},
    error::Error as StdError,
    fmt,
    fmt::{Debug, Display, Error as FmtError, Formatter},
    panic::{self, Location, PanicInfo},
    path::{Path, PathBuf},
    rc::Rc,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
    thread::{Builder, JoinHandle, LocalKey, Result as ThreadResult},
};
use typedmap::{TypedDashMap, TypedMapKey};

use super::dbsp_handle::{IntoCircuitConfig, Layout};

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub enum Error {
    /// One of the worker threads terminated unexpectedly.
    WorkerPanic {
        // Detailed panic information from all threads that
        // reported panics.
        panic_info: Vec<(usize, WorkerPanicInfo)>,
    },
    Terminated,
}

impl DetailedError for Error {
    fn error_code(&self) -> Cow<'static, str> {
        match self {
            Self::WorkerPanic { .. } => Cow::from("WorkerPanic"),
            Self::Terminated => Cow::from("Terminated"),
        }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), FmtError> {
        match self {
            Self::WorkerPanic { panic_info } => {
                writeln!(f, "One or more worker threads terminated unexpectedly")?;

                for (worker, worker_panic_info) in panic_info.iter() {
                    writeln!(f, "worker thread {worker} panicked")?;
                    writeln!(f, "{worker_panic_info}")?;
                }
                Ok(())
            }
            Self::Terminated => f.write_str("circuit terminated by the user"),
        }
    }
}

impl StdError for Error {}

// Thread-local variables used by the termination protocol.
thread_local! {
    // Parker that must be used by all schedulers within the worker
    // thread so that the scheduler gets woken up by `RuntimeHandle::kill`.
    static PARKER: Parker = Parker::new();

    // Set to `true` by `RuntimeHandle::kill`.
    // Schedulers must check this signal before evaluating each operator
    // and exit immediately returning `SchedulerError::Terminated`.
    static KILL_SIGNAL: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));
}

// Thread-local variables used to store per-worker context.
thread_local! {
    // Reference to the `Runtime` that manages this worker thread or `None`
    // if the current thread is not running in a multithreaded runtime.
    static RUNTIME: RefCell<Option<Runtime>> = const { RefCell::new(None) };

    // 0-based index of the current worker thread within its runtime.
    // Returns `0` if the current thread in not running in a multithreaded
    // runtime.
    pub(crate) static WORKER_INDEX: Cell<usize> = const { Cell::new(0) };
}

pub struct LocalStoreMarker;

/// Local data store shared by all workers in a runtime.
pub type LocalStore = TypedDashMap<LocalStoreMarker>;

// Rust source code location of a panic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PanicLocation {
    file: String,
    line: u32,
    col: u32,
}

impl Display for PanicLocation {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}:{}", self.file, self.line, self.col)
    }
}

impl PanicLocation {
    fn new(loc: &Location) -> Self {
        Self {
            file: loc.file().to_string(),
            line: loc.line(),
            col: loc.column(),
        }
    }
}

/// Information about a panic in a worker thread.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WorkerPanicInfo {
    // Panic message, if any.
    message: Option<String>,
    // Panic location.
    location: Option<PanicLocation>,
    // Backtrace.
    backtrace: String,
}

impl Display for WorkerPanicInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if let Some(message) = &self.message {
            writeln!(f, "panic message: {message}")?;
        } else {
            writeln!(f, "panic message (none)")?;
        }

        if let Some(location) = &self.location {
            writeln!(f, "panic location: {location}")?;
        } else {
            writeln!(f, "panic location: unknown")?;
        }
        writeln!(f, "stack trace:\n{}", self.backtrace)
    }
}

impl WorkerPanicInfo {
    fn new(panic_info: &PanicInfo) -> Self {
        #[allow(clippy::manual_map)]
        let message = if let Some(v) = panic_info.payload().downcast_ref::<String>() {
            Some(v.clone())
        } else if let Some(v) = panic_info.payload().downcast_ref::<&str>() {
            Some(v.to_string())
        } else {
            None
        };
        let backtrace = Backtrace::force_capture().to_string();
        let location = panic_info
            .location()
            .map(|location| PanicLocation::new(location));

        Self {
            message,
            location,
            backtrace,
        }
    }
}

struct RuntimeInner {
    layout: Layout,
    storage: StorageLocation,
    store: LocalStore,
    // Panic info collected from failed worker threads.
    panic_info: Vec<RwLock<Option<WorkerPanicInfo>>>,
}

impl Drop for RuntimeInner {
    fn drop(&mut self) {
        match &self.storage {
            StorageLocation::Temporary(path) => {
                let did_runtime_panick = self
                    .panic_info
                    .iter()
                    .any(|info| info.read().map_or_else(|_| true, |i| i.is_some()));
                if std::thread::panicking() || did_runtime_panick {
                    eprintln!("Preserved runtime storage at: {:?} due to panic", path);
                } else {
                    let _ = std::fs::remove_dir_all(path);
                }
            }
            StorageLocation::Permanent(_) => {}
        }
    }
}

/// The location where the runtime stores its data.
#[derive(Debug, Clone)]
enum StorageLocation {
    Temporary(PathBuf),
    Permanent(PathBuf),
}

impl AsRef<Path> for StorageLocation {
    fn as_ref(&self) -> &Path {
        match self {
            Self::Temporary(path) => path.as_ref(),
            Self::Permanent(path) => path.as_ref(),
        }
    }
}

impl From<StorageLocation> for PathBuf {
    fn from(location: StorageLocation) -> PathBuf {
        match location {
            StorageLocation::Temporary(path) => path,
            StorageLocation::Permanent(path) => path,
        }
    }
}

impl Debug for RuntimeInner {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("RuntimeInner")
            .field("layout", &self.layout)
            .field("storage", &self.storage)
            .finish()
    }
}

impl RuntimeInner {
    fn new(layout: Layout, storage: Option<String>) -> Self {
        let local_workers = layout.local_workers().len();
        let mut panic_info = Vec::with_capacity(local_workers);
        for _ in 0..local_workers {
            panic_info.push(RwLock::new(None));
        }
        let storage = storage.map_or_else(
            // Note that we use into_path() here which avoids deleting the temporary directory
            // we still clean it up when the runtime is dropped -- but keep it around on panic.
            || StorageLocation::Temporary(tempfile::tempdir().unwrap().into_path()),
            |s| StorageLocation::Permanent(PathBuf::from(s)),
        );

        Self {
            layout,
            storage,
            store: TypedDashMap::new(),
            panic_info,
        }
    }
}

// Panic callback used to record worker thread panic information
// in the runtime.
//
// Note: this is a global hook shared by all threads in the process.
// It is installed when a new DBSP runtime starts, possibly overriding
// a hook installed by another DBSP instance.  However it should work
// correctly for threads from different DBSP runtimes or for threads
// that don't belong to any DBSP runtime since it uses `RUNTIME`
// thread-local variable to detect a DBSP runtime.
fn panic_hook(panic_info: &PanicInfo<'_>, default_panic_hook: &dyn Fn(&PanicInfo<'_>)) {
    // Call the default panic hook first.
    default_panic_hook(panic_info);

    RUNTIME.with(|runtime| {
        if let Some(runtime) = runtime.borrow().clone() {
            runtime.panic(panic_info)
        }
    })
}

/// A multithreaded runtime that hosts `N` circuits running in parallel worker
/// threads. Typically, all `N` circuits are identical, but this is not required
/// or enforced.
#[repr(transparent)]
#[derive(Clone, Debug)]
pub struct Runtime(Arc<RuntimeInner>);

/// Stores the default Rust panic hook, so we can invoke it as part of
/// the DBSP custom hook.
#[allow(clippy::type_complexity)]
static DEFAULT_PANIC_HOOK: Lazy<Box<dyn Fn(&PanicInfo<'_>) + 'static + Sync + Send>> =
    Lazy::new(|| {
        // Clear any hooks installed by other libraries.
        let _ = panic::take_hook();
        panic::take_hook()
    });

/// Returns the default Rust panic hook.
fn default_panic_hook() -> &'static (dyn Fn(&PanicInfo<'_>) + 'static + Sync + Send) {
    &*DEFAULT_PANIC_HOOK
}

impl Runtime {
    /// Create a new runtime with the specified `layout` and run user-provided
    /// closure `f` in each thread.  The closure should build a circuit and
    /// return handles for this function to pass along to its own caller.  The
    /// closure takes a reference to the `Runtime` as an argument, so that
    /// workers can access shared services provided by the runtime.
    ///
    /// The `layout` may be specified as a number of worker threads or as a
    /// [`Layout`].
    ///
    /// Returns a handle to the runtime as well as the closure's own return
    /// value. The closure should return the same value in each thread; this
    /// function returns one of them arbitrarily.
    ///
    /// # Examples
    /// ```
    /// # #[cfg(all(windows, miri))]
    /// # fn main() {}
    ///
    /// # #[cfg(not(all(windows, miri)))]
    /// # fn main() {
    /// use dbsp::circuit::{Circuit, RootCircuit, Runtime};
    ///
    /// // Create a runtime with 4 worker threads.
    /// let hruntime = Runtime::run(4, || {
    ///     // This closure runs within each worker thread.
    ///
    ///     let root = RootCircuit::build(move |circuit| {
    ///         // Populate `circuit` with operators.
    ///         Ok(())
    ///     })
    ///     .unwrap()
    ///     .0;
    ///
    ///     // Run circuit for 100 clock cycles.
    ///     for _ in 0..100 {
    ///         root.step().unwrap();
    ///     }
    /// });
    ///
    /// // Wait for all worker threads to terminate.
    /// hruntime.join().unwrap();
    /// # }
    /// ```
    pub fn run<F>(cconf: impl IntoCircuitConfig, circuit: F) -> RuntimeHandle
    where
        F: FnOnce() + Clone + Send + 'static,
    {
        let storage = cconf.storage();
        let layout = cconf.layout();

        let workers = layout.local_workers();
        let nworkers = workers.len();
        let runtime = Self(Arc::new(RuntimeInner::new(layout, storage)));

        // Install custom panic hook.

        let default_hook = default_panic_hook();
        panic::set_hook(Box::new(move |panic_info| {
            panic_hook(panic_info, default_hook)
        }));

        let mut handles = Vec::with_capacity(nworkers);
        handles.extend(workers.map(|worker_index| {
            let runtime = runtime.clone();
            let build_circuit = circuit.clone();

            let (init_sender, init_receiver) = bounded(1);
            let join_handle = Builder::new()
                .name(format!("dbsp-worker-{worker_index}"))
                .spawn(move || {
                    // Set the worker's runtime handle and index
                    RUNTIME.with(|rt| *rt.borrow_mut() = Some(runtime));
                    WORKER_INDEX.set(worker_index);

                    // Send the main thread our parker and kill signal
                    // TODO: Share a single kill signal across all workers
                    init_sender
                        .send((
                            PARKER.with(|parker| parker.unparker().clone()),
                            KILL_SIGNAL.with(|s| s.clone()),
                        ))
                        .unwrap();

                    // Build the worker's circuit
                    build_circuit();
                })
                .unwrap_or_else(|error| {
                    panic!("failed to spawn worker thread {worker_index}: {error}");
                });

            (join_handle, init_receiver)
        }));

        let mut workers = Vec::with_capacity(nworkers);
        workers.extend(handles.into_iter().map(|(handle, recv)| {
            let (unparker, kill_signal) = recv.recv().unwrap();
            WorkerHandle::new(handle, unparker, kill_signal)
        }));

        RuntimeHandle::new(runtime, workers)
    }

    /// Returns a reference to the multithreaded runtime that
    /// manages the current worker thread, or `None` if the thread
    /// runs without a runtime.
    ///
    /// Worker threads created by the [`Runtime::run`] method can access
    /// the services provided by this API via an instance of `struct Runtime`,
    /// which they can obtain by calling `Runtime::runtime()`.  DBSP circuits
    /// created without a managed runtime run in the context of the client
    /// thread.  When invoked by such a thread, this method returns `None`.
    #[allow(clippy::self_named_constructors)]
    pub fn runtime() -> Option<Runtime> {
        RUNTIME.with(|rt| rt.borrow().clone())
    }

    fn new_backend() -> Rc<Backend> {
        let rt = Runtime::runtime();
        let dir = if let Some(rt) = rt {
            rt.inner().storage.clone().as_ref().to_path_buf()
        } else {
            tempdir_for_thread()
        };
        Rc::new(new_default_backend(dir))
    }

    /// Returns the (thread-local) storage backend.
    pub fn backend() -> Rc<Backend> {
        thread_local! {
            pub static DEFAULT_BACKEND: Rc<Backend> = Runtime::new_backend();
        }
        DEFAULT_BACKEND.with(|rc| rc.clone())
    }

    /// Returns the (thread-local) storage backend.
    pub fn storage() -> Rc<BufferCache<Backend, FileCacheEntry>> {
        thread_local! {
            pub static CACHE: Rc<BufferCache<Backend, FileCacheEntry>> =
                Rc::new(BufferCache::new(Runtime::backend()));
        }
        CACHE.with(|rc| rc.clone())
    }

    /// Returns 0-based index of the current worker thread within its runtime.
    /// For threads that run without a runtime, this method returns `0`.  In a
    /// multihost runtime, this is a global index across all hosts.
    pub fn worker_index() -> usize {
        WORKER_INDEX.get()
    }

    fn inner(&self) -> &RuntimeInner {
        &self.0
    }

    /// Returns the number of workers in the runtime's [`Layout`].  In a
    /// multihost runtime, this is the total number of workers across all hosts.
    pub fn num_workers(&self) -> usize {
        self.inner().layout.n_workers()
    }

    /// Returns the [`Layout`] for this runtime.
    pub fn layout(&self) -> &Layout {
        &self.inner().layout
    }

    /// Returns reference to the data store shared by all workers within the
    /// runtime.  In a multihost runtime, this data store is local to this
    /// particular host.
    ///
    /// This low-level mechanism can be used by various services that
    /// require common state shared across all workers on a host.
    ///
    /// The [`LocalStore`] type is an alias to [`TypedDashMap`], a
    /// concurrent map type that can store key/value pairs of different
    /// types.  See `typedmap` crate documentation for details.
    pub fn local_store(&self) -> &LocalStore {
        &self.inner().store
    }

    /// Returns the path to the storage directory for this runtime.
    pub fn storage_path(&self) -> PathBuf {
        self.inner().storage.clone().into()
    }

    /// A per-worker sequential counter.
    ///
    /// This method can be used to generate unique identifiers that will be the
    /// same across all worker threads.  Repeated calls to this function
    /// with the same worker index generate numbers 0, 1, 2, ...
    pub fn sequence_next(&self, worker_index: usize) -> usize {
        debug_assert!(self.inner().layout.local_workers().contains(&worker_index));
        let mut entry = self
            .local_store()
            .entry(WorkerId(worker_index))
            .or_insert(0);
        let result = *entry;
        *entry += 1;
        result
    }

    /// Returns current worker's parker to be used by schedulers.
    ///
    /// Whenever a circuit scheduler needs to block waiting for
    /// an operator to become ready, it must use this parker.
    /// This ensures that the thread will be woken up when the
    /// user tries to terminate the runtime using
    /// [`RuntimeHandle::kill`].
    pub fn parker() -> &'static LocalKey<Parker> {
        &PARKER
    }

    /// `true` if the current worker thread has received a kill signal
    /// and should exit asap.  Schedulers should use this method before
    /// scheduling the next operator and after parking.
    pub fn kill_in_progress() -> bool {
        KILL_SIGNAL.with(|signal| signal.load(Ordering::SeqCst))
    }

    pub fn worker_panic_info(&self, worker: usize) -> Option<WorkerPanicInfo> {
        if let Ok(guard) = self.inner().panic_info[worker].read() {
            guard.clone()
        } else {
            None
        }
    }

    // Record information about a worker thread panic in `panic_info`
    fn panic(&self, panic_info: &PanicInfo) {
        let worker_index = Self::worker_index();
        let panic_info = WorkerPanicInfo::new(panic_info);
        let _ = self.inner().panic_info[worker_index]
            .write()
            .map(|mut guard| *guard = Some(panic_info));
    }
}

/// Per-worker controls.
#[derive(Debug)]
struct WorkerHandle {
    join_handle: JoinHandle<()>,
    unparker: Unparker,
    kill_signal: Arc<AtomicBool>,
}

impl WorkerHandle {
    fn new(join_handle: JoinHandle<()>, unparker: Unparker, kill_signal: Arc<AtomicBool>) -> Self {
        Self {
            join_handle,
            unparker,
            kill_signal,
        }
    }

    fn unpark(&self) {
        self.unparker.unpark();
    }
}

/// Handle returned by `Runtime::run`.
#[derive(Debug)]
pub struct RuntimeHandle {
    runtime: Runtime,
    workers: Vec<WorkerHandle>,
}

impl RuntimeHandle {
    fn new(runtime: Runtime, workers: Vec<WorkerHandle>) -> Self {
        Self { runtime, workers }
    }

    /// Unpark worker thread.
    ///
    /// Workers release the CPU by parking when they have no work to do.
    /// This method unparks a thread after sending a command to it or
    /// when killing a circuit.
    pub(super) fn unpark_worker(&self, worker: usize) {
        self.workers[worker].unpark();
    }

    /// Returns reference to the runtime.
    pub fn runtime(&self) -> &Runtime {
        &self.runtime
    }

    /// Terminate the runtime and all worker threads without waiting for any
    /// in-progress computation to complete.
    ///
    /// Signals all workers to exit.  Any operators already running are
    /// evaluated to completion, after which the worker thread terminates
    /// even if the circuit has not been fully evaluated for the current
    /// clock cycle.
    pub fn kill(self) -> ThreadResult<()> {
        self.kill_async();

        self.join()
    }

    // Signals all worker threads to exit, and returns immediately without
    // waiting for them to exit.
    pub fn kill_async(&self) {
        for worker in self.workers.iter() {
            worker.kill_signal.store(true, Ordering::SeqCst);
            worker.unpark();
        }
    }

    /// Wait for all workers in the runtime to terminate.
    ///
    /// The calling thread blocks until all worker threads have terminated.
    pub fn join(self) -> ThreadResult<()> {
        // Insist on joining all threads even if some of them fail.
        #[allow(clippy::needless_collect)]
        let results: Vec<ThreadResult<()>> = self
            .workers
            .into_iter()
            .map(|h| h.join_handle.join())
            .collect();
        results.into_iter().collect::<ThreadResult<()>>()
    }

    /// Retrieve panic info for a specific worker.
    pub fn worker_panic_info(&self, worker: usize) -> Option<WorkerPanicInfo> {
        self.runtime.worker_panic_info(worker)
    }

    // Retrieve panic info for all workers.
    pub fn collect_panic_info(&self) -> Vec<(usize, WorkerPanicInfo)> {
        let mut result = Vec::new();

        for worker in 0..self.workers.len() {
            if let Some(panic_info) = self.worker_panic_info(worker) {
                result.push((worker, panic_info))
            }
        }
        result
    }
}

#[derive(Hash, PartialEq, Eq)]
struct WorkerId(usize);

impl TypedMapKey<LocalStoreMarker> for WorkerId {
    type Value = usize;
}

#[cfg(test)]
mod tests {
    use super::Runtime;
    use crate::{
        circuit::{
            schedule::{DynamicScheduler, Scheduler, StaticScheduler},
            CircuitConfig, Layout,
        },
        operator::Generator,
        Circuit, RootCircuit,
    };
    use std::{
        cell::RefCell,
        rc::Rc,
        sync::{Arc, Mutex},
        thread::sleep,
        time::Duration,
    };

    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_runtime_static() {
        test_runtime::<StaticScheduler>();
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_runtime_dynamic() {
        test_runtime::<DynamicScheduler>();
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn storage_no_cleanup() {
        // Case 1: storage specified, runtime should not clean up storage when exiting
        let path = tempfile::tempdir().unwrap().into_path();
        let path_clone = path.clone();
        let cconf = CircuitConfig {
            layout: Layout::new_solo(4),
            storage: Some(path.to_str().unwrap().to_string()),
        };

        let hruntime = Runtime::run(cconf, move || {
            let runtime = Runtime::runtime().unwrap();
            assert_eq!(runtime.storage_path(), path_clone);
        });
        hruntime.join().unwrap();
        assert!(path.exists(), "persistent storage is not cleaned up");
    }
    #[test]
    #[cfg_attr(miri, ignore)]
    fn storage_temp_cleanup() {
        // Case 2: no storage specified, runtime should use temporary storage
        // and clean it up
        let storage_path = Arc::new(Mutex::new(None));
        let cconf = CircuitConfig {
            layout: Layout::new_solo(4),
            storage: None,
        };
        let storage_path_clone = storage_path.clone();
        let hruntime = Runtime::run(cconf, move || {
            let runtime = Runtime::runtime().unwrap();
            *storage_path_clone.lock().unwrap() = Some(runtime.storage_path());
        });

        hruntime.join().unwrap();
        let storage_path = storage_path.lock().unwrap().take().unwrap();
        assert!(!storage_path.exists(), "temporary storage is cleaned up");
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn storage_no_cleanup_on_panic() {
        // Case 3: if we panic, keep state around, even if temporary
        let storage_path = Arc::new(Mutex::new(None));
        let cconf = CircuitConfig {
            layout: Layout::new_solo(4),
            storage: None,
        };
        let storage_path_clone = storage_path.clone();
        let hruntime = Runtime::run(cconf, move || {
            let runtime = Runtime::runtime().unwrap();
            *storage_path_clone.lock().unwrap() = Some(runtime.storage_path());
            panic!("oh no");
        });
        sleep(Duration::from_millis(100));
        hruntime.kill().expect_err("kill shouldn't have worked");

        let storage_path = storage_path.lock().unwrap().take().unwrap();
        assert!(
            storage_path.exists(),
            "temporary storage is not cleaned up on panic"
        );
    }

    fn test_runtime<S>()
    where
        S: Scheduler + 'static,
    {
        let hruntime = Runtime::run(4, || {
            let data = Rc::new(RefCell::new(vec![]));
            let data_clone = data.clone();
            let root = RootCircuit::build_with_scheduler::<_, _, S>(move |circuit| {
                let runtime = Runtime::runtime().unwrap();
                // Generator that produces values using `sequence_next`.
                circuit
                    .add_source(Generator::new(move || {
                        runtime.sequence_next(Runtime::worker_index())
                    }))
                    .inspect(move |n: &usize| data_clone.borrow_mut().push(*n));
                Ok(())
            })
            .unwrap()
            .0;

            for _ in 0..100 {
                root.step().unwrap();
            }

            assert_eq!(&*data.borrow(), &(0..100).collect::<Vec<usize>>());
        });

        hruntime.join().unwrap();
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_kill_static() {
        test_kill::<StaticScheduler>();
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_kill_dynamic() {
        test_kill::<DynamicScheduler>();
    }

    // Test `RuntimeHandle::kill`.
    fn test_kill<S>()
    where
        S: Scheduler + 'static,
    {
        let hruntime = Runtime::run(16, || {
            // Create a nested circuit that iterates forever.
            let root = RootCircuit::build_with_scheduler::<_, _, S>(move |circuit| {
                circuit
                    .iterate_with_scheduler::<_, _, _, S>(|child| {
                        let mut n: usize = 0;
                        child
                            .add_source(Generator::new(move || {
                                n += 1;
                                n
                            }))
                            .inspect(|_: &usize| {});
                        Ok((|| Ok(false), ()))
                    })
                    .unwrap();
                Ok(())
            })
            .unwrap()
            .0;

            loop {
                if root.step().is_err() {
                    return;
                }
            }
        });

        sleep(Duration::from_millis(100));
        hruntime.kill().unwrap();
    }
}
