//! A multithreaded runtime for evaluating DBSP circuits in a data-parallel
//! fashion.

use crate::circuit::checkpointer::Checkpointer;
use crate::circuit::metrics::describe_metrics;
use crate::error::Error as DbspError;
use crate::storage::backend::StorageBackend;
use crate::storage::file::format::Compression;
use crate::storage::file::writer::Parameters;
use crate::{
    storage::{backend::StorageError, buffer_cache::BufferCache, dirlock::LockedDirectory},
    DetailedError,
};
use core_affinity::{get_core_ids, CoreId};
use enum_map::{enum_map, Enum, EnumMap};
use feldera_types::config::{StorageCompression, StorageConfig, StorageOptions};
use indexmap::IndexSet;
use once_cell::sync::Lazy;
use serde::Serialize;
use std::path::Path;
use std::sync::atomic::AtomicUsize;
use std::sync::{LazyLock, Mutex};
use std::thread::Thread;
use std::time::Duration;
use std::{
    backtrace::Backtrace,
    borrow::Cow,
    cell::{Cell, RefCell},
    error::Error as StdError,
    fmt,
    fmt::{Debug, Display, Error as FmtError, Formatter},
    panic::{self, Location, PanicHookInfo},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
    thread::{Builder, JoinHandle, Result as ThreadResult},
};
use tracing::{debug, info, warn};
use typedmap::TypedDashMap;

use super::dbsp_handle::{Layout, Mode};
use super::CircuitConfig;

/// The number of tuples a stateful operator outputs per step during replay.
pub const DEFAULT_REPLAY_STEP_SIZE: usize = 10000;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub enum Error {
    /// One of the worker threads terminated unexpectedly.
    WorkerPanic {
        // Detailed panic information from all threads that
        // reported panics.
        panic_info: Vec<(usize, WorkerPanicInfo)>,
    },
    /// The storage directory supplied does not match the runtime circuit.
    IncompatibleStorage,
    Terminated,
}

impl DetailedError for Error {
    fn error_code(&self) -> Cow<'static, str> {
        match self {
            Self::WorkerPanic { .. } => Cow::from("WorkerPanic"),
            Self::Terminated => Cow::from("Terminated"),
            Self::IncompatibleStorage => Cow::from("IncompatibleStorage"),
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
            Self::Terminated => f.write_str("circuit has been terminated"),
            Self::IncompatibleStorage => {
                f.write_str("Supplied storage directory does not fit the runtime circuit")
            }
        }
    }
}

impl StdError for Error {}

// Thread-local variables used to store per-worker context.
thread_local! {
    // Reference to the `Runtime` that manages this worker thread or `None`
    // if the current thread is not running in a multithreaded runtime.
    static RUNTIME: RefCell<Option<Runtime>> = const { RefCell::new(None) };

    // 0-based index of the current worker thread within its runtime.
    // Returns `0` if the current thread in not running in a multithreaded
    // runtime.
    static WORKER_INDEX: Cell<usize> = const { Cell::new(0) };
}

mod thread_type {
    use std::{cell::Cell, fmt::Display};

    #[cfg(doc)]
    use super::Runtime;
    use enum_map::Enum;

    thread_local! {
        static CURRENT: Cell<ThreadType> = const { Cell::new(ThreadType::Foreground) };
    }

    /// Type of a thread running in a [Runtime].
    #[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Enum)]
    pub enum ThreadType {
        /// Circuit thread.
        Foreground,

        /// Merger thread.
        Background,
    }

    impl ThreadType {
        /// Returns the kind of thread we're currently running in, if we're in a
        /// [Runtime].  Outside of a [Runtime], this returns
        /// [ThreadType::Foreground].
        pub fn current() -> Self {
            CURRENT.get()
        }

        pub(super) fn set_current(thread_type: Self) {
            CURRENT.set(thread_type);
        }
    }

    impl Display for ThreadType {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                ThreadType::Foreground => write!(f, "foreground"),
                ThreadType::Background => write!(f, "background"),
            }
        }
    }
}
pub use thread_type::ThreadType;

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
    fn new(panic_info: &PanicHookInfo) -> Self {
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

#[derive(derive_more::Debug)]
struct RuntimeStorage {
    /// Runner configuration.
    pub config: StorageConfig,

    /// User options.
    pub options: StorageOptions,

    /// Backend.
    #[debug(skip)]
    pub backend: Arc<dyn StorageBackend>,

    // This is just here for the `Drop` behavior.
    #[allow(dead_code)]
    locked_directory: LockedDirectory,
}

struct RuntimeInner {
    layout: Layout,
    mode: Mode,

    storage: Option<RuntimeStorage>,
    store: LocalStore,
    kill_signal: AtomicBool,
    background_threads: Mutex<Vec<JoinHandle<()>>>,
    buffer_caches: Vec<EnumMap<ThreadType, Arc<BufferCache>>>,
    pin_cpus: Vec<EnumMap<ThreadType, CoreId>>,
    worker_sequence_numbers: Vec<AtomicUsize>,
    // Panic info collected from failed worker threads.
    panic_info: Vec<RwLock<Option<WorkerPanicInfo>>>,
    replay_step_size: AtomicUsize,
}

impl Debug for RuntimeInner {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("RuntimeInner")
            .field("layout", &self.layout)
            .field("storage", &self.storage)
            .finish()
    }
}

fn display_core_ids<'a>(iter: impl Iterator<Item = &'a CoreId>) -> String {
    format!(
        "{:?}",
        iter.map(|core| core.id).collect::<Vec<_>>().as_slice()
    )
}

fn map_pin_cpus(nworkers: usize, pin_cpus: &[usize]) -> Vec<EnumMap<ThreadType, CoreId>> {
    let pin_cpus = pin_cpus
        .iter()
        .copied()
        .map(|id| CoreId { id })
        .collect::<IndexSet<_>>();
    if pin_cpus.len() < 2 * nworkers {
        if !pin_cpus.is_empty() {
            warn!("ignoring CPU pinning request because {nworkers} workers require {} pinned CPUs but only {} were specified",
                      2 * nworkers, pin_cpus.len())
        }
        return Vec::new();
    }

    let Some(core_ids) = get_core_ids() else {
        warn!("ignoring CPU pinning request because this system's core ids list could not be obtained");
        return Vec::new();
    };
    let core_ids = core_ids.iter().copied().collect::<IndexSet<_>>();

    let missing_cpus = pin_cpus.difference(&core_ids).copied().collect::<Vec<_>>();
    if !missing_cpus.is_empty() {
        warn!("ignoring CPU pinning request because requested CPUs {missing_cpus:?} are not available (available CPUs are: {})",
              display_core_ids(core_ids.iter()));
        return Vec::new();
    }

    let fg_cpus = &pin_cpus[0..nworkers];
    let bg_cpus = &pin_cpus[nworkers..nworkers * 2];
    info!(
        "pinning foreground workers to CPUs {} and background workers to CPUs {}",
        display_core_ids(fg_cpus.iter()),
        display_core_ids(bg_cpus.iter())
    );
    (0..nworkers)
        .map(|i| {
            enum_map! {
                ThreadType::Foreground => fg_cpus[i],
                ThreadType::Background => bg_cpus[i],
            }
        })
        .collect()
}

impl RuntimeInner {
    fn new(config: CircuitConfig) -> Result<Self, DbspError> {
        let nworkers = config.layout.local_workers().len();

        let storage = if let Some(storage) = config.storage {
            let locked_directory =
                LockedDirectory::new_blocking(storage.config.path(), Duration::from_secs(60))?;
            let backend = storage.backend;

            if let Some(init_checkpoint) = storage.init_checkpoint {
                if !backend
                    .exists(&Checkpointer::checkpoint_dir(init_checkpoint).child("CHECKPOINT"))?
                {
                    return Err(DbspError::Storage(StorageError::CheckpointNotFound(
                        init_checkpoint,
                    )));
                }
            }

            Some(RuntimeStorage {
                config: storage.config,
                options: storage.options,
                backend,
                locked_directory,
            })
        } else {
            None
        };

        let cache_size_bytes = if let Some(storage) = &storage {
            storage
                .options
                .cache_mib
                .map_or(256 * 1024 * 1024, |cache_mib| {
                    cache_mib.saturating_mul(1024 * 1024) / nworkers / ThreadType::LENGTH
                })
        } else {
            // Dummy buffer cache.
            1
        };

        Ok(Self {
            layout: config.layout,
            mode: config.mode,
            storage,
            store: TypedDashMap::new(),
            kill_signal: AtomicBool::new(false),
            background_threads: Mutex::new(Vec::new()),
            buffer_caches: (0..nworkers)
                .map(|_| EnumMap::from_fn(|_| Arc::new(BufferCache::new(cache_size_bytes))))
                .collect(),
            pin_cpus: map_pin_cpus(nworkers, &config.pin_cpus),
            worker_sequence_numbers: (0..nworkers).map(|_| AtomicUsize::new(0)).collect(),
            panic_info: (0..nworkers).map(|_| RwLock::new(None)).collect(),
            replay_step_size: AtomicUsize::new(DEFAULT_REPLAY_STEP_SIZE),
        })
    }

    fn pin_cpu(&self) {
        if !self.pin_cpus.is_empty() {
            let worker_index = Runtime::worker_index();
            let thread_type = ThreadType::current();
            let core = self.pin_cpus[worker_index][thread_type];
            if !core_affinity::set_for_current(core) {
                warn!(
                    "failed to pin worker {worker_index} {thread_type} thread to core {}",
                    core.id
                );
            }
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
fn panic_hook(panic_info: &PanicHookInfo<'_>, default_panic_hook: &dyn Fn(&PanicHookInfo<'_>)) {
    // Call the default panic hook first.
    default_panic_hook(panic_info);

    RUNTIME.with(|runtime| {
        if let Ok(runtime) = runtime.try_borrow() {
            if let Some(runtime) = runtime.as_ref() {
                runtime.panic(panic_info);
            }
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
static DEFAULT_PANIC_HOOK: Lazy<Box<dyn Fn(&PanicHookInfo<'_>) + 'static + Sync + Send>> =
    Lazy::new(|| {
        // Clear any hooks installed by other libraries.
        let _ = panic::take_hook();
        panic::take_hook()
    });

/// Returns the default Rust panic hook.
fn default_panic_hook() -> &'static (dyn Fn(&PanicHookInfo<'_>) + 'static + Sync + Send) {
    &*DEFAULT_PANIC_HOOK
}

impl Runtime {
    /// Creates a new runtime with the specified `layout` and run user-provided
    /// closure `f` in each thread, and returns a handle to the runtime.
    ///
    /// The `layout` may be specified as a number of worker threads or as a
    /// [`Layout`].
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
    /// })
    /// .unwrap();
    ///
    /// // Wait for all worker threads to terminate.
    /// hruntime.join().unwrap();
    /// # }
    /// ```
    pub fn run<F>(config: impl Into<CircuitConfig>, circuit: F) -> Result<RuntimeHandle, DbspError>
    where
        F: FnOnce() + Clone + Send + 'static,
    {
        let config: CircuitConfig = config.into();

        let workers = config.layout.local_workers();

        describe_metrics();

        let runtime = Self(Arc::new(RuntimeInner::new(config)?));

        // Install custom panic hook.
        let default_hook = default_panic_hook();
        panic::set_hook(Box::new(move |panic_info| {
            panic_hook(panic_info, default_hook)
        }));

        let workers = workers
            .map(|worker_index| {
                let runtime = runtime.clone();
                let build_circuit = circuit.clone();
                Builder::new()
                    .name(format!("dbsp-worker-{worker_index}"))
                    .spawn(move || {
                        // Set the worker's runtime handle and index
                        WORKER_INDEX.set(worker_index);
                        ThreadType::set_current(ThreadType::Foreground);
                        runtime.inner().pin_cpu();
                        RUNTIME.with(|rt| *rt.borrow_mut() = Some(runtime));

                        // Build the worker's circuit
                        build_circuit();
                    })
                    .unwrap_or_else(|error| {
                        panic!("failed to spawn worker thread {worker_index}: {error}");
                    })
            })
            .collect::<Vec<_>>();

        Ok(RuntimeHandle::new(runtime, workers))
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

    /// Returns this runtime's storage backend, if storage is configured.
    ///
    /// # Panic
    ///
    /// Panics if this thread is not in a [Runtime].
    pub fn storage_backend() -> Result<Arc<dyn StorageBackend>, StorageError> {
        Runtime::runtime()
            .unwrap()
            .inner()
            .storage
            .as_ref()
            .map_or(Err(StorageError::StorageDisabled), |storage| {
                Ok(storage.backend.clone())
            })
    }

    /// Returns this thread's buffer cache.  Every thread has a buffer cache,
    /// but:
    ///
    /// - If the thread's [Runtime] does not have storage configured, the cache
    ///   size is trivially small.
    ///
    /// - If the thread is not in a [Runtime], then the cache is shared among
    ///   all such threads. (Such a thread might be in a circuit that uses
    ///   storage, but there's no way to know because only [Runtime] makes that
    ///   available at a thread level.)
    pub fn buffer_cache() -> Arc<BufferCache> {
        // Fast path, look up from TLS
        thread_local! {
            static BUFFER_CACHE: RefCell<Option<Arc<BufferCache>>> = const { RefCell::new(None) };
        }
        if let Some(buffer_cache) = BUFFER_CACHE.with(|bc| bc.borrow().clone()) {
            return buffer_cache;
        }

        // Slow path for initializing the thread-local.
        let buffer_cache = if let Some(rt) = Runtime::runtime() {
            rt.get_buffer_cache(Runtime::worker_index(), ThreadType::current())
        } else {
            // No `Runtime` means there's only a single worker, so use a single
            // global cache.
            static NO_RUNTIME_CACHE: LazyLock<Arc<BufferCache>> =
                LazyLock::new(|| Arc::new(BufferCache::new(1024 * 1024 * 256)));
            NO_RUNTIME_CACHE.clone()
        };
        BUFFER_CACHE.set(Some(buffer_cache.clone()));
        buffer_cache
    }

    /// Returns this runtime's buffer cache for thread type `thread_type` in
    /// worker `worker_index`.
    ///
    /// Usually it's easier and faster to call [Runtime::buffer_cache] instead.
    pub fn get_buffer_cache(
        &self,
        worker_index: usize,
        thread_type: ThreadType,
    ) -> Arc<BufferCache> {
        self.0.buffer_caches[worker_index][thread_type].clone()
    }

    /// Returns 0-based index of the current worker thread within its runtime.
    /// For threads that run without a runtime, this method returns `0`.  In a
    /// multihost runtime, this is a global index across all hosts.
    pub fn worker_index() -> usize {
        WORKER_INDEX.get()
    }

    pub fn mode() -> Mode {
        RUNTIME
            .with(|rt| Some(rt.borrow().as_ref()?.get_mode()))
            .unwrap_or_default()
    }

    pub fn get_mode(&self) -> Mode {
        self.inner().mode.clone()
    }

    /// Configure the number of tuples a stateful operator outputs per step during replay.
    ///
    /// The default is `DEFAULT_REPLAY_STEP_SIZE`.
    pub fn set_replay_step_size(&self, step_size: usize) {
        self.inner()
            .replay_step_size
            .store(step_size, Ordering::Release);
    }

    /// Get currently configured replay step size.
    ///
    /// Returns `DEFAULT_REPLAY_STEP_SIZE` if the current thread doesn't have a runtime.
    pub fn replay_step_size() -> usize {
        RUNTIME
            .with(|rt| Some(rt.borrow().as_ref()?.get_replay_step_size()))
            .unwrap_or(DEFAULT_REPLAY_STEP_SIZE)
    }

    /// Get currently configured replay step size.
    pub fn get_replay_step_size(&self) -> usize {
        self.inner().replay_step_size.load(Ordering::Acquire)
    }

    /// Returns the worker index as a string.
    ///
    /// This is useful for metric labels.
    pub fn worker_index_str() -> &'static str {
        static WORKER_INDEX_STRS: Lazy<[&'static str; 256]> = Lazy::new(|| {
            let mut data: [&'static str; 256] = [""; 256];
            for (i, item) in data.iter_mut().enumerate() {
                *item = Box::leak(i.to_string().into_boxed_str());
            }
            data
        });

        WORKER_INDEX_STRS
            .get(WORKER_INDEX.get())
            .copied()
            .unwrap_or_else(|| {
                panic!("Limit workers to less than 256 or increase the limit in the code.")
            })
    }

    /// Returns the minimum number of bytes in a batch (one that persists from
    /// step to step) to spill it to storage, or `None` if this thread doesn't
    /// have a [Runtime] or if it doesn't have storage configured.
    pub fn min_index_storage_bytes() -> Option<usize> {
        RUNTIME.with(|rt| {
            Some(
                rt.borrow()
                    .as_ref()?
                    .inner()
                    .storage
                    .as_ref()?
                    .options
                    .min_storage_bytes
                    .unwrap_or({
                        // This reduces the files stored on disk to a reasonable number.

                        1024 * 1024
                    }),
            )
        })
    }

    /// Returns the minimum number of bytes in a batch (one that does not
    /// persist from step to step) to spill it to storage, or `None` if this
    /// thread doesn't have a [Runtime] or if it doesn't have storage
    /// configured.
    pub fn min_step_storage_bytes() -> Option<usize> {
        RUNTIME.with(|rt| {
            Some(
                rt.borrow()
                    .as_ref()?
                    .inner()
                    .storage
                    .as_ref()?
                    .options
                    .min_step_storage_bytes
                    .unwrap_or(usize::MAX),
            )
        })
    }

    pub fn file_writer_parameters() -> Parameters {
        let compression = Runtime::runtime()
            .unwrap()
            .inner()
            .storage
            .as_ref()
            .unwrap()
            .options
            .compression;
        let compression = match compression {
            StorageCompression::Default | StorageCompression::Snappy => Some(Compression::Snappy),
            StorageCompression::None => None,
        };
        Parameters::default().with_compression(compression)
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
    pub fn storage_path(&self) -> Option<&Path> {
        self.inner()
            .storage
            .as_ref()
            .map(|storage| storage.config.path())
    }

    /// A per-worker sequential counter.
    ///
    /// This method can be used to generate unique identifiers that will be the
    /// same across all worker threads.  Repeated calls to this function
    /// from the same worker generate numbers 0, 1, 2, ...
    pub fn sequence_next(&self) -> usize {
        self.inner().worker_sequence_numbers[Self::worker_index()].fetch_add(1, Ordering::Relaxed)
    }

    /// `true` if the current worker thread has received a kill signal
    /// and should exit asap.  Schedulers should use this method before
    /// scheduling the next operator and after parking.
    pub fn kill_in_progress() -> bool {
        // Only a circuit with a `Runtime` can receive a kill signal, which is
        // OK because a kill request can only be sent via a `RuntimeHandle`
        // anyway.
        RUNTIME.with(|runtime| {
            runtime
                .borrow()
                .as_ref()
                .map(|runtime| runtime.inner().kill_signal.load(Ordering::SeqCst))
                .unwrap_or(false)
        })
    }

    pub fn worker_panic_info(&self, worker: usize) -> Option<WorkerPanicInfo> {
        if let Ok(guard) = self.inner().panic_info[worker].read() {
            guard.clone()
        } else {
            None
        }
    }

    // Record information about a worker thread panic in `panic_info`
    fn panic(&self, panic_info: &PanicHookInfo) {
        let worker_index = Self::worker_index();
        let panic_info = WorkerPanicInfo::new(panic_info);
        let _ = self.inner().panic_info[worker_index]
            .write()
            .map(|mut guard| *guard = Some(panic_info));
    }

    /// Spawn a new thread using `builder` and `f`. If the current thread is
    /// associated with a runtime, then the new thread will also be associated
    /// with the same runtime and worker index.
    pub(crate) fn spawn_background_thread<F>(builder: Builder, f: F) -> Thread
    where
        F: FnOnce() + Send + 'static,
    {
        let runtime = Self::runtime();
        let worker_index = Self::worker_index();
        let join_handle = builder
            .spawn(move || {
                WORKER_INDEX.set(worker_index);
                ThreadType::set_current(ThreadType::Background);
                if let Some(runtime) = runtime {
                    runtime.inner().pin_cpu();
                    RUNTIME.with(|rt| *rt.borrow_mut() = Some(runtime));
                }
                f()
            })
            .unwrap_or_else(|error| {
                panic!("failed to spawn background worker thread {worker_index}: {error}");
            });
        let thread = join_handle.thread().clone();
        if let Some(runtime) = Self::runtime() {
            runtime
                .inner()
                .background_threads
                .lock()
                .unwrap()
                .push(join_handle);
        }
        thread
    }
}

/// Handle returned by `Runtime::run`.
#[derive(Debug)]
pub struct RuntimeHandle {
    runtime: Runtime,
    workers: Vec<JoinHandle<()>>,
}

impl RuntimeHandle {
    fn new(runtime: Runtime, workers: Vec<JoinHandle<()>>) -> Self {
        Self { runtime, workers }
    }

    /// Unpark worker thread.
    ///
    /// Workers release the CPU by parking when they have no work to do.
    /// This method unparks a thread after sending a command to it or
    /// when killing a circuit.
    pub(super) fn unpark_worker(&self, worker: usize) {
        self.workers[worker].thread().unpark();
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
        self.runtime
            .inner()
            .kill_signal
            .store(true, Ordering::SeqCst);
        for worker in self.workers.iter() {
            worker.thread().unpark();
        }
    }

    /// Wait for all workers in the runtime to terminate.
    ///
    /// The calling thread blocks until all worker threads have terminated.
    pub fn join(self) -> ThreadResult<()> {
        // Insist on joining all threads even if some of them fail.
        #[allow(clippy::needless_collect)]
        let results: Vec<ThreadResult<()>> = self.workers.into_iter().map(|h| h.join()).collect();

        // Wait for the background threads. They will exit automatically without
        // explicit signaling from us because the worker threads removed all of
        // their background work.
        self.runtime
            .inner()
            .background_threads
            .lock()
            .unwrap()
            .drain(..)
            .for_each(|h| {
                let _ = h.join();
            });

        // This must happen after we wait for the background threads, because
        // they might try to initiate another merge before they exit, which
        // would require them to have access to storage, which is kept in the
        // local store.
        self.runtime.local_store().clear();

        results.into_iter().collect()
    }

    /// Retrieve panic info for a specific worker.
    pub fn worker_panic_info(&self, worker: usize) -> Option<WorkerPanicInfo> {
        self.runtime.worker_panic_info(worker)
    }

    /// Retrieve panic info for all workers.
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

#[cfg(test)]
mod tests {
    use super::Runtime;
    use crate::{
        circuit::{
            dbsp_handle::{CircuitStorageConfig, Mode},
            schedule::{DynamicScheduler, Scheduler},
            CircuitConfig, Layout,
        },
        operator::Generator,
        Circuit, RootCircuit,
    };
    use feldera_types::config::{StorageCacheConfig, StorageConfig, StorageOptions};
    use std::{cell::RefCell, rc::Rc, thread::sleep, time::Duration};

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
            mode: Mode::Ephemeral,
            pin_cpus: Vec::new(),
            storage: Some(
                CircuitStorageConfig::for_config(
                    StorageConfig {
                        path: path.to_string_lossy().into_owned(),
                        cache: StorageCacheConfig::default(),
                    },
                    StorageOptions::default(),
                )
                .unwrap(),
            ),
        };

        let hruntime = Runtime::run(cconf, move || {
            let runtime = Runtime::runtime().unwrap();
            assert_eq!(runtime.storage_path(), Some(path_clone.as_ref()));
        })
        .expect("failed to start runtime");
        hruntime.join().unwrap();
        assert!(path.exists(), "persistent storage is not cleaned up");
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
                    .add_source(Generator::new(move || runtime.sequence_next()))
                    .inspect(move |n: &usize| data_clone.borrow_mut().push(*n));
                Ok(())
            })
            .unwrap()
            .0;

            for _ in 0..100 {
                root.step().unwrap();
            }

            assert_eq!(&*data.borrow(), &(0..100).collect::<Vec<usize>>());
        })
        .expect("failed to start runtime");

        hruntime.join().unwrap();
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
        })
        .expect("failed to start runtime");

        sleep(Duration::from_millis(100));
        hruntime.kill().unwrap();
    }
}
