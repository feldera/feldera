//! A multithreaded runtime for evaluating DBSP circuits in a data-parallel
//! fashion.

use super::CircuitConfig;
use super::dbsp_handle::{Layout, Mode};
use crate::SchedulerError;
use crate::circuit::checkpointer::Checkpointer;
use crate::error::Error as DbspError;
use crate::operator::communication::Exchange;
use crate::storage::backend::StorageBackend;
use crate::storage::file::format::Compression;
use crate::storage::file::writer::Parameters;
use crate::utils::process_rss_bytes;
use crate::{
    DetailedError,
    storage::{
        backend::StorageError,
        buffer_cache::{BufferCache, build_buffer_caches},
        dirlock::LockedDirectory,
    },
};
use core_affinity::{CoreId, get_core_ids};
use crossbeam::sync::{Parker, Unparker};
use enum_map::{Enum, EnumMap, enum_map};
use feldera_buffer_cache::ThreadType;
use feldera_storage::fbuf::FBuf;
use feldera_storage::fbuf::slab::{FBufSlabs, FBufSlabsStats, set_thread_slab_pool};
use feldera_types::config::{DevTweaks, StorageCompression, StorageConfig, StorageOptions};
use feldera_types::memory_pressure::{
    CRITICAL_MEMORY_PRESSURE_THRESHOLD, HIGH_MEMORY_PRESSURE_THRESHOLD,
    MODERATE_MEMORY_PRESSURE_THRESHOLD, MemoryPressure,
};
use indexmap::IndexSet;
use once_cell::sync::Lazy;
use serde::Serialize;
use std::convert::identity;
use std::iter::repeat;
use std::net::TcpListener;
use std::ops::Range;
use std::path::Path;
use std::sync::atomic::{AtomicU8, AtomicU64, AtomicUsize};
use std::sync::{LazyLock, Mutex};
use std::time::Duration;
use std::{
    backtrace::Backtrace,
    borrow::Cow,
    cell::{Cell, RefCell},
    collections::HashSet,
    error::Error as StdError,
    fmt,
    fmt::{Debug, Display, Error as FmtError, Formatter},
    panic::{self, Location, PanicHookInfo},
    sync::{
        Arc, RwLock, Weak,
        atomic::{AtomicBool, Ordering},
    },
    thread::{Builder, JoinHandle, Result as ThreadResult},
};
use tokio::runtime::Builder as TokioBuilder;
use tokio::runtime::Runtime as TokioRuntime;
use tokio::sync::Notify;
use tracing::{debug, error, info, warn};
use typedmap::TypedDashMap;

/// The number of tuples a stateful operator outputs per step during replay.
pub const DEFAULT_REPLAY_STEP_SIZE: usize = 10000;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub enum Error {
    /// One of the worker threads terminated unexpectedly.
    WorkerPanic {
        // Detailed panic information from all threads that
        // reported panics.
        panic_info: Vec<(usize, ThreadType, WorkerPanicInfo)>,
    },
    /// The storage directory supplied does not match the runtime circuit.
    IncompatibleStorage,
    /// Error deserializing checkpointed state.
    CheckpointParseError(String),
    Terminated,
}

impl DetailedError for Error {
    fn error_code(&self) -> Cow<'static, str> {
        match self {
            Self::WorkerPanic { .. } => Cow::from("WorkerPanic"),
            Self::Terminated => Cow::from("Terminated"),
            Self::IncompatibleStorage => Cow::from("IncompatibleStorage"),
            Self::CheckpointParseError(_) => Cow::from("CheckpointParseError"),
        }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), FmtError> {
        match self {
            Self::WorkerPanic { panic_info } => {
                writeln!(f, "One or more worker threads terminated unexpectedly")?;

                for (worker, thread_type, worker_panic_info) in panic_info.iter() {
                    writeln!(f, "{thread_type} worker thread {worker} panicked")?;
                    writeln!(f, "{worker_panic_info}")?;
                }
                Ok(())
            }
            Self::Terminated => f.write_str("circuit has been terminated"),
            Self::IncompatibleStorage => {
                f.write_str("Supplied storage directory does not fit the runtime circuit")
            }
            Self::CheckpointParseError(error) => {
                write!(f, "Error deserializing checkpointed state: {error}")
            }
        }
    }
}

impl StdError for Error {}

// Thread-local variables set for all threads in the DBSP runtime (foreground and background).
thread_local! {
    /// Reference to the `Runtime` that manages this worker thread or `None`
    /// if the current thread is not running in a multithreaded runtime.
    static RUNTIME: RefCell<Option<Runtime>> = const { RefCell::new(None) };

    /// `None` means that this is an auxiliary thread that runs inside the runtime
    /// but is neither a DBSP foreground nor a background thread.
    static CURRENT_THREAD_TYPE: Cell<Option<ThreadType>> = const { Cell::new(None) };

}

// Thread-local variables set for all foreground worker threads.
thread_local! {
    /// 0-based index of the current worker thread within its runtime.
    /// Returns `0` if the current thread in not running in a multithreaded
    /// runtime.
    static WORKER_INDEX: Cell<usize> = const { Cell::new(0) };

    /// Buffer cache for this foreground worker thread.
    static BUFFER_CACHE: RefCell<Option<Arc<BufferCache>>> = const { RefCell::new(None) };
}

// Task-local variables set for all tasks in the tokio merger runtime.
tokio::task_local! {
    /// The WORKER_INDEX of the FOREGROUND worker thread that this task is doing compaction for.
    ///
    /// Set for tasks in the tokio merger runtime.
    pub(crate) static TOKIO_WORKER_INDEX: usize;

    /// The buffer cache for this task (this cache is shared with the foreground worker thread).
    ///
    /// Set for tasks in the tokio merger runtime.
    pub(crate) static TOKIO_BUFFER_CACHE: Arc<BufferCache>;
}

pub(crate) fn current_thread_type() -> Option<ThreadType> {
    CURRENT_THREAD_TYPE.get()
}

fn set_current_thread_type(thread_type: ThreadType) {
    CURRENT_THREAD_TYPE.set(Some(thread_type));
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
    dev_tweaks: DevTweaks,

    /// User-configured process memory limit.
    max_rss: Option<u64>,

    /// Current process RSS.
    process_rss: AtomicU64,

    /// Current memory pressure updated every second as a function of process_rss, max_rss,
    /// and previous memory pressure.
    memory_pressure: AtomicU8,

    /// Monotonic counter incremented whenever memory pressure transitions to high/critical.
    memory_pressure_epoch: AtomicU64,

    /// Used to notify merger threads when memory pressure changes.
    memory_pressure_notify: Arc<Notify>,

    storage: Option<RuntimeStorage>,
    store: LocalStore,
    kill_signal: AtomicBool,
    aux_threads: Mutex<Vec<(JoinHandle<()>, Unparker)>>,
    buffer_caches: Vec<EnumMap<ThreadType, Arc<BufferCache>>>,

    /// Core IDs to pin foreground workers to.
    pin_cpus_fg: Vec<CoreId>,

    /// Core IDs to pin tokio merger background workers to.
    pin_cpus_bg: Vec<CoreId>,
    fbuf_slab_allocators: Vec<EnumMap<ThreadType, Arc<FBufSlabs>>>,
    worker_sequence_numbers: Vec<AtomicUsize>,
    /// Panic info collected from failed worker threads.
    panic_info: Vec<EnumMap<ThreadType, RwLock<Option<WorkerPanicInfo>>>>,
    panicked: AtomicBool,
    replay_step_size: AtomicUsize,

    /// Tokio runtime that runs async merger tasks (see `AsyncMerger`).
    tokio_merger_runtime: Mutex<Option<TokioRuntime>>,

    /// Optional socket for exchange to listen on.
    ///
    /// When exchange initializes, it takes this socket.
    ///
    /// Passing in a socket allows the client to let the kernel pick a port
    /// number by binding to port 0.  Port numbers need to be known for all the
    /// hosts before we start the circuit, so without this feature we couldn't
    /// be sure that we'd have a set of available ports.
    exchange_listener: Mutex<Option<TcpListener>>,
}

impl Drop for RuntimeInner {
    fn drop(&mut self) {
        debug!("dropping RuntimeInner");
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

fn display_core_ids<'a>(iter: impl Iterator<Item = &'a CoreId>) -> String {
    format!(
        "{:?}",
        iter.map(|core| core.id).collect::<Vec<_>>().as_slice()
    )
}

// Map CPU IDs to core IDs for foreground and background workers.
//
// Returns a pair of vectors of core IDs, one for foreground workers and one for background workers.
fn map_pin_cpus(config: &CircuitConfig) -> (Vec<CoreId>, Vec<CoreId>) {
    if config.layout.is_multihost() {
        if !config.pin_cpus.is_empty() {
            warn!("CPU pinning not yet supported with multihost DBSP");
        }
        return (Vec::new(), Vec::new());
    }

    let merger_threads = config.num_merger_threads();

    let nworkers = config.layout.n_workers();
    let pin_cpus = config
        .pin_cpus
        .iter()
        .copied()
        .map(|id| CoreId { id })
        .collect::<IndexSet<_>>();
    let expected_cpus = nworkers + merger_threads;
    if pin_cpus.len() < expected_cpus {
        if !pin_cpus.is_empty() {
            warn!(
                "ignoring CPU pinning request because {nworkers} foreground workers and {} merger workers require {} pinned CPUs but only {} were specified",
                merger_threads,
                expected_cpus,
                pin_cpus.len()
            )
        }
        return (Vec::new(), Vec::new());
    }

    let Some(core_ids) = get_core_ids() else {
        warn!(
            "ignoring CPU pinning request because this system's core ids list could not be obtained"
        );
        return (Vec::new(), Vec::new());
    };
    let core_ids = core_ids.iter().copied().collect::<IndexSet<_>>();

    let missing_cpus = pin_cpus.difference(&core_ids).copied().collect::<Vec<_>>();
    if !missing_cpus.is_empty() {
        warn!(
            "ignoring CPU pinning request because requested CPUs {missing_cpus:?} are not available (available CPUs are: {})",
            display_core_ids(core_ids.iter())
        );
        return (Vec::new(), Vec::new());
    }

    let fg_cpus = &pin_cpus[0..nworkers];
    let bg_cpus = &pin_cpus[nworkers..expected_cpus];
    info!(
        "pinning foreground workers to CPUs {} and background workers to CPUs {}",
        display_core_ids(fg_cpus.iter()),
        display_core_ids(bg_cpus.iter())
    );
    let fg_pinning = (0..nworkers).map(|i| fg_cpus[i]).collect();

    let bg_pinning = (0..merger_threads).map(|i| bg_cpus[i]).collect();

    (fg_pinning, bg_pinning)
}

impl RuntimeInner {
    fn new(config: CircuitConfig) -> Result<Self, DbspError> {
        let nworkers = config.layout.local_workers().len();
        let buffer_cache_strategy = config.dev_tweaks.buffer_cache_strategy();
        let buffer_max_buckets = config.dev_tweaks.buffer_max_buckets;
        let buffer_cache_allocation_strategy = config
            .dev_tweaks
            .effective_buffer_cache_allocation_strategy();
        let fbuf_slab_bytes_per_class = config
            .dev_tweaks
            .fbuf_slab_bytes_per_class
            .unwrap_or(FBufSlabs::DEFAULT_BYTES_PER_CLASS);
        let storage = if let Some(storage) = config.storage.clone() {
            let locked_directory =
                LockedDirectory::new_blocking(storage.config.path(), Duration::from_secs(60))?;
            let backend = storage.backend;

            if let Some(init_checkpoint) = storage.init_checkpoint
                && !backend
                    .exists(&Checkpointer::checkpoint_dir(init_checkpoint).child("CHECKPOINT"))?
            {
                return Err(DbspError::Storage(StorageError::CheckpointNotFound(
                    init_checkpoint,
                )));
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

        let total_cache_bytes = if let Some(storage) = &storage {
            match storage.options.cache_mib {
                Some(cache_mib) => cache_mib.saturating_mul(1024 * 1024),
                None => 256usize
                    .saturating_mul(1024 * 1024)
                    .saturating_mul(nworkers)
                    .saturating_mul(ThreadType::LENGTH),
            }
        } else {
            // Dummy buffer cache.
            1
        };

        info!(
            "Setting up buffer caches: {buffer_cache_strategy:?} {buffer_cache_allocation_strategy:?} buckets={buffer_max_buckets:?} total_size={:?} MiB",
            total_cache_bytes / (1024 * 1024)
        );
        let buffer_caches = build_buffer_caches(
            nworkers,
            total_cache_bytes,
            buffer_cache_strategy,
            buffer_max_buckets,
            buffer_cache_allocation_strategy,
        );

        info!("Setting up FBuf slab allocators: bytes_per_class={fbuf_slab_bytes_per_class}");
        let fbuf_slab_allocators = (0..nworkers)
            .map(|_| {
                let allocator = Arc::new(FBufSlabs::new(fbuf_slab_bytes_per_class));
                enum_map! {
                    ThreadType::Foreground => allocator.clone(),
                    ThreadType::Background => allocator.clone(),
                }
            })
            .collect();

        let (pin_cpus_fg, pin_cpus_bg) = map_pin_cpus(&config);

        Ok(Self {
            pin_cpus_fg,
            pin_cpus_bg,
            layout: config.layout,
            mode: config.mode,
            dev_tweaks: config.dev_tweaks,
            max_rss: config.max_rss_bytes,
            process_rss: AtomicU64::new(process_rss_bytes().unwrap_or_default()),
            memory_pressure: AtomicU8::new(MemoryPressure::Low as u8),
            memory_pressure_epoch: AtomicU64::new(0),
            memory_pressure_notify: Arc::new(Notify::new()),
            storage,
            store: TypedDashMap::new(),
            kill_signal: AtomicBool::new(false),
            aux_threads: Mutex::new(Vec::new()),
            buffer_caches,
            fbuf_slab_allocators,
            worker_sequence_numbers: (0..nworkers).map(|_| AtomicUsize::new(0)).collect(),
            panic_info: (0..nworkers)
                .map(|_| EnumMap::from_fn(|_| RwLock::new(None)))
                .collect(),
            panicked: AtomicBool::new(false),
            replay_step_size: AtomicUsize::new(DEFAULT_REPLAY_STEP_SIZE),
            tokio_merger_runtime: Mutex::new(None),
            exchange_listener: Mutex::new(config.exchange_listener),
        })
    }

    fn pin_cpu(&self) {
        match current_thread_type() {
            Some(ThreadType::Foreground) => {
                if !self.pin_cpus_fg.is_empty() {
                    let local_worker_offset = Runtime::local_worker_offset();
                    let core = self.pin_cpus_fg[local_worker_offset];
                    if !core_affinity::set_for_current(core) {
                        warn!(
                            "failed to pin foreground worker {local_worker_offset} to core {}",
                            core.id
                        );
                    }
                }
            }
            Some(ThreadType::Background) => {
                if !self.pin_cpus_bg.is_empty() {
                    let local_worker_offset = Runtime::local_worker_offset();
                    let core = self.pin_cpus_bg[local_worker_offset];
                    if !core_affinity::set_for_current(core) {
                        warn!(
                            "failed to pin background worker {local_worker_offset} to core {}",
                            core.id
                        );
                    }
                }
            }
            None => {
                panic!("pin_cpu() called outside of a runtime or on an aux thread");
            }
        }
    }

    fn memory_pressure(&self) -> MemoryPressure {
        MemoryPressure::try_from(self.memory_pressure.load(Ordering::Relaxed)).unwrap()
    }

    /// Compute memory pressure based on the current process RSS.
    ///
    /// Final memory pressure is computed taking into account the previous memory pressure level as well.
    fn raw_memory_pressure(&self, process_rss: u64) -> MemoryPressure {
        let process_rss = process_rss as f64;

        let Some(max_rss) = self.max_rss else {
            return MemoryPressure::Low;
        };

        if process_rss >= (CRITICAL_MEMORY_PRESSURE_THRESHOLD * max_rss as f64) {
            return MemoryPressure::Critical;
        }

        if process_rss >= (HIGH_MEMORY_PRESSURE_THRESHOLD * max_rss as f64) {
            return MemoryPressure::High;
        }

        if process_rss >= (MODERATE_MEMORY_PRESSURE_THRESHOLD * max_rss as f64) {
            return MemoryPressure::Moderate;
        }

        MemoryPressure::Low
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
        if let Ok(runtime) = runtime.try_borrow()
            && let Some(runtime) = runtime.as_ref()
        {
            runtime.panic(panic_info);
        }
    })
}

/// A multithreaded runtime that hosts `N` circuits running in parallel worker
/// threads. Typically, all `N` circuits are identical, but this is not required
/// or enforced.
#[repr(transparent)]
#[derive(Clone, Debug)]
pub struct Runtime(Arc<RuntimeInner>);

/// A weak reference to a [Runtime].
#[repr(transparent)]
#[derive(Clone, Debug)]
pub struct WeakRuntime(Weak<RuntimeInner>);

impl WeakRuntime {
    pub fn upgrade(&self) -> Option<Runtime> {
        self.0.upgrade().map(Runtime)
    }
}

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
    /// Creates a new runtime with the specified `layout` and runs user-provided
    /// closure `circuit` in each thread, and returns a handle to the runtime. The closure
    /// takes an unparker.  The runtime will use this unparker to wake up the thread
    /// when terminating the circuit.
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
    /// let hruntime = Runtime::run(4, |_parker| {
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
    ///         root.transaction().unwrap();
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
        F: FnOnce(Parker) + Clone + Send + 'static,
    {
        let config: CircuitConfig = config.into();

        let workers = config.layout.local_workers();
        let num_merger_threads = config.num_merger_threads();

        let runtime = Self(Arc::new(RuntimeInner::new(config)?));

        // Install custom panic hook.
        let default_hook = default_panic_hook();
        panic::set_hook(Box::new(move |panic_info| {
            panic_hook(panic_info, default_hook)
        }));

        // Create a tokio runtime for async merger tasks.
        let runtime_clone = runtime.clone();
        let tokio_merger_runtime: TokioRuntime = {
            info!("starting dbsp merger tokio runtime, workers: {num_merger_threads}",);

            TokioBuilder::new_multi_thread()
                .worker_threads(num_merger_threads)
                .thread_name_fn(|| {
                    use std::sync::atomic::{AtomicUsize, Ordering};
                    static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
                    let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
                    format!("merger-{}", id)
                })
                .on_thread_start(move || {
                    set_current_thread_type(ThreadType::Background);
                    RUNTIME.with(|rt| *rt.borrow_mut() = Some(runtime_clone.clone()));
                })
                .thread_stack_size(6 * 1024 * 1024)
                .enable_all()
                .build()
                .unwrap()
        };

        runtime
            .inner()
            .tokio_merger_runtime
            .lock()
            .unwrap()
            .replace(tokio_merger_runtime);

        // Monitor process RSS and update memory pressure.
        runtime.spawn_aux_thread("rss-monitor", Parker::new(), |parker| {
            let runtime = Runtime::runtime().unwrap();

            while !Runtime::kill_in_progress() {
                let process_rss = process_rss_bytes().unwrap_or_default();
                runtime
                    .inner()
                    .process_rss
                    .store(process_rss, Ordering::Relaxed);

                let previous_pressure = runtime.inner().memory_pressure();
                let current_memory_pressure = runtime.inner().raw_memory_pressure(process_rss);

                // Update effective memory pressure based on the previous and current memory pressure levels.
                //
                // Current memory pressure is computed based on the current process RSS.
                // Effective memory pressure determines how aggressively the circuit pushes batches to disk.
                // We introduce a delay between the current memory pressure decreases past the threshold of the
                // current level, and the effective memory pressure follows.
                //
                // - When memory pressure is low or medium, the effective memory pressure follows the current memory pressure.
                // - When memory pressure is high or critical, we keep the effective memory pressure at the previous level until
                //   the current memory pressure goes two levels down: high -> low or critical -> moderate.
                let new_memory_pressure = match (previous_pressure, current_memory_pressure) {
                    (MemoryPressure::Low | MemoryPressure::Moderate, _) => current_memory_pressure,
                    (MemoryPressure::High, MemoryPressure::Critical) => MemoryPressure::Critical,
                    (MemoryPressure::High, MemoryPressure::High | MemoryPressure::Moderate) => {
                        MemoryPressure::High
                    }
                    (MemoryPressure::High, MemoryPressure::Low) => MemoryPressure::Low,
                    (MemoryPressure::Critical, MemoryPressure::Critical | MemoryPressure::High) => {
                        MemoryPressure::Critical
                    }
                    (MemoryPressure::Critical, MemoryPressure::Moderate | MemoryPressure::Low) => {
                        current_memory_pressure
                    }
                };

                runtime
                    .inner()
                    .memory_pressure
                    .store(new_memory_pressure as u8, Ordering::Relaxed);

                // At high and critical levels, wakeup merger threads to push all in-memory batches to disk.
                if previous_pressure < MemoryPressure::High
                    && new_memory_pressure >= MemoryPressure::High
                {
                    runtime
                        .inner()
                        .memory_pressure_epoch
                        .fetch_add(1, Ordering::Relaxed);
                    runtime.inner().memory_pressure_notify.notify_waiters();
                }
                parker.park_timeout(Duration::from_secs(1));
            }
        });

        let workers = workers
            .map(|worker_index| {
                let runtime = runtime.clone();
                let build_circuit = circuit.clone();
                let parker = Parker::new();
                let unparker = parker.unparker().clone();
                let local_worker_offset =
                    worker_index - runtime.inner().layout.local_workers().start;
                let fbuf_slab_allocator =
                    runtime.get_fbuf_slab_allocator(local_worker_offset, ThreadType::Foreground);
                let handle = Builder::new()
                    .name(format!("dbsp-worker-{worker_index}"))
                    .spawn(move || {
                        // Set the worker's runtime handle and index
                        WORKER_INDEX.set(worker_index);
                        set_current_thread_type(ThreadType::Foreground);
                        set_thread_slab_pool(Some(fbuf_slab_allocator));
                        runtime.inner().pin_cpu();
                        RUNTIME.with(|rt| *rt.borrow_mut() = Some(runtime));

                        // Build the worker's circuit
                        build_circuit(parker);
                    })
                    .unwrap_or_else(|error| {
                        panic!("failed to spawn worker thread {worker_index}: {error}");
                    });
                (handle, unparker)
            })
            .collect::<Vec<_>>();

        Ok(RuntimeHandle::new(runtime, workers))
    }

    pub fn downgrade(&self) -> WeakRuntime {
        WeakRuntime(Arc::downgrade(&self.0))
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

    /// Returns this thread's buffer-cache handle:
    ///
    /// - If the thread is a foreground thread or a background task, returns
    ///   that thread or task's buffer cache.
    ///
    /// - If the thread's [Runtime] does not have storage configured, the cache
    ///   size is trivially small.
    ///
    /// - If the thread is not in a [Runtime], then the cache is shared among
    ///   all such threads. (Such a thread might be in a circuit that uses
    ///   storage, but there's no way to know because only [Runtime] makes that
    ///   available at a thread level.)
    ///
    /// - If the thread is a background thread, but no buffer cache is set, this
    ///   ordinarily indicates a bug.  However, this can happen when a
    ///   background task is dropped, because it's normal for task-local
    ///   variables to be unavailable in `Drop` during task shutdown.  The
    ///   function returns `None` in this case (and only in this case).
    pub fn buffer_cache() -> Option<Arc<BufferCache>> {
        if let Some(buffer_cache) = BUFFER_CACHE.with(|bc| bc.borrow().clone()) {
            // Fast path common case for foreground threads.
            Some(buffer_cache)
        } else if let Ok(buffer_cache) = TOKIO_BUFFER_CACHE.try_get() {
            // Background tasks case.
            Some(buffer_cache)
        } else if let Some(rt) = Runtime::runtime()
            && let Some(thread_type) = current_thread_type()
        {
            // Slow path for threads running in a [Runtime].
            match thread_type {
                ThreadType::Foreground => {
                    let buffer_cache =
                        rt.get_buffer_cache(Runtime::local_worker_offset(), thread_type);
                    BUFFER_CACHE.set(Some(buffer_cache.clone()));
                    Some(buffer_cache)
                }
                ThreadType::Background => {
                    // We are in a background thread in a Tokio runtime but no
                    // buffer cache is set in [TOKIO_BUFFER_CACHE].  This is
                    // probably a bug, but it can happen when a background task
                    // is canceled.  It is the reason that this function returns
                    // an `Option`.
                    None
                }
            }
        } else {
            // Fallback path for threads outside a [Runtime].
            //
            // This cache is shared by all auxiliary threads in the runtime.  In
            // particular, output connector threads use it to maintain their
            // output buffers.
            //
            // FIXME: We may need a tunable strategy for aux threads. We cannot
            // simply give each of them the same cache as DBSP worker threads,
            // as there can be dozens of aux threads (currently one per output
            // connector), which do not necessarily need a large cache. OTOH,
            // sharing the same cache across all of them may potentially cause
            // performance issues.
            static AUXILIARY_CACHE: LazyLock<Arc<BufferCache>> =
                LazyLock::new(|| Arc::new(BufferCache::new(1024 * 1024 * 256)));

            let buffer_cache = AUXILIARY_CACHE.clone();
            BUFFER_CACHE.set(Some(buffer_cache.clone()));
            Some(buffer_cache)
        }
    }

    /// Spawn an auxiliary thread inside the runtime.
    ///
    /// The auxiliary thread will have access to the runtime's resources, including the
    /// storage backend. The current use case for this is to be able to use spines outside
    /// of the DBSP worker threads, e.g., to maintain output buffers.
    ///
    /// # Arguments
    ///
    /// * `thread_name` - The name of the thread.
    /// * `parker` - The thread will use this parker when waiting for work. Use it to unpark
    ///   the thread when terminating the runtime.
    /// * `f` - The function to execute in the thread.
    pub fn spawn_aux_thread<F>(&self, thread_name: &str, parker: Parker, f: F)
    where
        F: FnOnce(Parker) + Send + 'static,
    {
        let runtime = self.clone();
        let unparker = parker.unparker().clone();
        let handle = Builder::new()
            .name(thread_name.to_string())
            .spawn(move || {
                RUNTIME.with(|rt| *rt.borrow_mut() = Some(runtime));
                f(parker)
            })
            .expect("failed to spawn auxiliary thread");

        self.inner()
            .aux_threads
            .lock()
            .unwrap()
            .push((handle, unparker))
    }

    /// Returns this runtime's buffer-cache handle for thread type `thread_type`
    /// in worker with local offset `local_worker_offset`.
    ///
    /// Usually it's easier and faster to call [Runtime::buffer_cache] instead.
    pub fn get_buffer_cache(
        &self,
        local_worker_offset: usize,
        thread_type: ThreadType,
    ) -> Arc<BufferCache> {
        self.0.buffer_caches[local_worker_offset][thread_type].clone()
    }

    pub(crate) fn get_fbuf_slab_allocator(
        &self,
        local_worker_offset: usize,
        thread_type: ThreadType,
    ) -> Arc<FBufSlabs> {
        self.0.fbuf_slab_allocators[local_worker_offset][thread_type].clone()
    }

    /// Returns `(current, max)`, reporting the amount of the buffer cache
    /// that is currently used and its maximum size, both in bytes.
    pub fn cache_occupancy(&self) -> (usize, usize) {
        if self.0.storage.is_some() {
            let mut seen = HashSet::new();
            self.0
                .buffer_caches
                .iter()
                .flat_map(|map| map.values())
                .filter(|cache| seen.insert(cache.backend_id()))
                .map(|cache| cache.occupancy())
                .fold((0, 0), |(a_cur, a_max), (b_cur, b_max)| {
                    (a_cur + b_cur, a_max + b_max)
                })
        } else {
            (0, 0)
        }
    }

    /// Returns aggregate statistics for the `FBuf` slab pools used by storage.
    pub fn fbuf_slabs_stats(&self) -> FBufSlabsStats {
        if self.0.storage.is_some() {
            let mut seen = HashSet::new();
            self.0
                .fbuf_slab_allocators
                .iter()
                .flat_map(|map| map.values())
                .filter(|allocator| seen.insert(allocator.backend_id()))
                .map(|allocator| allocator.stats())
                .fold(FBufSlabsStats::default(), |mut stats, pool_stats| {
                    stats += pool_stats;
                    stats
                })
        } else {
            FBufSlabsStats::default()
        }
    }

    /// Returns 0-based index of the current worker thread within its runtime.
    /// For threads that run without a runtime, this method returns `0`.  In a
    /// multihost runtime, this is a global index across all hosts.
    ///
    /// For threads that run in the tokio merger runtime, this method returns the
    /// index of the foreground worker thread that this task is doing compaction for.
    pub fn worker_index() -> usize {
        match current_thread_type() {
            Some(ThreadType::Foreground) => WORKER_INDEX.get(),
            Some(ThreadType::Background) => TOKIO_WORKER_INDEX.get(),
            None => 0,
        }
    }

    /// Returns the 0-based index of the current worker within its local host.
    pub fn local_worker_offset() -> usize {
        // Find the lowest-numbered local worker.
        let local_workers_start = RUNTIME
            .with(|rt| Some(rt.borrow().as_ref()?.layout().local_workers().start))
            .unwrap_or_default();
        Self::worker_index() - local_workers_start
    }

    pub fn mode() -> Mode {
        RUNTIME
            .with(|rt| Some(rt.borrow().as_ref()?.get_mode()))
            .unwrap_or_default()
    }

    pub fn with_dev_tweaks<F, T>(f: F) -> T
    where
        F: Fn(&DevTweaks) -> T,
    {
        static DEFAULT: Lazy<DevTweaks> = Lazy::new(DevTweaks::default);
        RUNTIME
            .with(|rt| Some(f(&rt.borrow().as_ref()?.inner().dev_tweaks)))
            .unwrap_or_else(|| f(&DEFAULT))
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
            .get(Runtime::worker_index())
            .copied()
            .unwrap_or_else(|| {
                panic!("Limit workers to less than 256 or increase the limit in the code.")
            })
    }

    pub fn memory_pressure() -> Option<MemoryPressure> {
        RUNTIME.with(|rt| Some(rt.borrow().as_ref()?.inner().memory_pressure()))
    }

    pub fn current_memory_pressure(&self) -> MemoryPressure {
        self.inner().memory_pressure()
    }

    pub fn max_rss_bytes(&self) -> Option<u64> {
        self.inner().max_rss
    }

    pub fn memory_pressure_epoch(&self) -> u64 {
        self.inner().memory_pressure_epoch.load(Ordering::Relaxed)
    }

    pub(crate) fn memory_pressure_notify(&self) -> Arc<Notify> {
        self.inner().memory_pressure_notify.clone()
    }

    /// Returns the minimum number of bytes in a batch produced by a merge operator to spill it to storage.
    ///
    /// The output is determined by the current memory pressure level and the user-configured `min_storage_bytes` option.
    /// When memory pressure is low, the output is `min_storage_bytes`; when memory pressure is moderate, high or critical,
    /// the output is `0`, meaning all batches are spilled to storage.
    ///
    /// # Returns
    ///
    /// - `None` - if this thread doesn't have a Runtime or if it doesn't have storage configured.
    /// - `Some(0)` - spill all batches to storage.
    /// - `Some(N)` - spill batches with size >= N to storage.
    pub fn min_merge_storage_bytes() -> Option<usize> {
        RUNTIME.with(|rt| {
            let rt = rt.borrow();
            let inner = rt.as_ref()?.inner();
            let storage = inner.storage.as_ref()?;

            if inner.memory_pressure() >= MemoryPressure::Moderate {
                Some(0)
            } else {
                Some(storage.options.min_storage_bytes.unwrap_or({
                    // This reduces the files stored on disk to a reasonable number.

                    10 * 1024 * 1024
                }))
            }
        })
    }

    /// Returns the minimum number of bytes in a batch inserted into a spine by a foreground worker to spill it to storage.
    ///
    /// The output is determined by the current memory pressure level and the user-configured `min_storage_bytes` option.
    /// When memory pressure is low the output is `usize::MAX`, when memory pressure is moderate, the output is `min_storage_bytes`,
    /// when memory pressure is high or critical, the output is `0`, meaning all batches are spilled to storage.
    ///
    /// # Returns
    ///
    /// - `None` - if this thread doesn't have a Runtime or if it doesn't have storage configured.
    /// - `Some(0)` - spill all batches to storage.
    /// - `Some(N)` - spill batches with size >= N to storage.
    pub fn min_insert_storage_bytes() -> Option<usize> {
        RUNTIME.with(|rt| {
            let rt = rt.borrow();
            let inner = rt.as_ref()?.inner();
            let storage = inner.storage.as_ref()?;

            if inner.memory_pressure() >= MemoryPressure::High {
                Some(0)
            } else if inner.memory_pressure() >= MemoryPressure::Moderate {
                // Moderate pressure: spill large batches to storage in the foreground; the merger will take care of the rest.
                Some(
                    storage
                        .options
                        .min_storage_bytes
                        .unwrap_or(10 * 1024 * 1024),
                )
            } else {
                // When there is no memory pressure, we leave it to the merger to write the batches to storage
                // eventually.
                Some(usize::MAX)
            }
        })
    }

    /// Returns the minimum number of bytes in a transient batch exchanged between DBSP operators during a step of the
    /// circuit to spill it to storage.
    ///
    /// The output is determined by the current memory pressure level and the user-configured `min_step_storage_bytes` option.
    /// When memory pressure is below critical, the output is `min_step_storage_bytes`, when memory pressure is critical,
    /// the output is `0`, meaning all batches are spilled to storage.
    ///
    /// # Returns
    ///
    /// - `None` - if this thread doesn't have a Runtime or if it doesn't have storage configured.
    /// - `Some(0)` - spill all batches to storage.
    /// - `Some(N)` - spill batches with size >= N to storage.
    pub fn min_step_storage_bytes() -> Option<usize> {
        RUNTIME.with(|rt| {
            let rt = rt.borrow();
            let inner = rt.as_ref()?.inner();
            let storage = inner.storage.as_ref()?;

            if inner.memory_pressure() >= MemoryPressure::Critical {
                Some(0)
            } else {
                Some(storage.options.min_step_storage_bytes.unwrap_or(usize::MAX))
            }
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
    ///
    /// If this thread is not in a [Runtime], returns 1.
    pub fn num_workers() -> usize {
        RUNTIME.with(|rt| {
            rt.borrow()
                .as_ref()
                .map_or(1, |runtime| runtime.layout().n_workers())
        })
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
        self.inner().worker_sequence_numbers[Self::local_worker_offset()]
            .fetch_add(1, Ordering::Relaxed)
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

    pub fn worker_panic_info(
        &self,
        worker: usize,
        thread_type: ThreadType,
    ) -> Option<WorkerPanicInfo> {
        if let Ok(guard) = self.inner().panic_info[worker][thread_type].read() {
            guard.clone()
        } else {
            warn!("poisoned panic_lock lock for {thread_type} worker {worker}");
            None
        }
    }

    // Record information about a worker thread panic in `panic_info`
    fn panic(&self, panic_info: &PanicHookInfo) {
        let local_worker_offset = Self::local_worker_offset();
        let Some(thread_type) = current_thread_type() else {
            // We only install panic hooks on foreground and background threads,
            // so this shouldn't happen, but we cannot panic here.
            error!("panic hook called outside of a runtime or on an aux thread");
            return;
        };
        let panic_info = WorkerPanicInfo::new(panic_info);
        let _ = self.inner().panic_info[local_worker_offset][thread_type]
            .write()
            .map(|mut guard| *guard = Some(panic_info));
        self.inner().panicked.store(true, Ordering::Release);
    }

    /// Tokio merger runtime associated with this DBSP runtime.
    pub(crate) fn tokio_merger_runtime(&self) -> tokio::runtime::Handle {
        self.inner()
            .tokio_merger_runtime
            .lock()
            .unwrap()
            .as_ref()
            .expect("tokio merger runtime has been shut down")
            .handle()
            .clone()
    }

    /// Takes and returns the TCP listener socket configured via
    /// [CircuitConfig], if any.
    ///
    /// [CircuitConfig]: crate::circuit::dbsp_handle::CircuitConfig
    pub(crate) fn take_exchange_listener(&self) -> Option<TcpListener> {
        self.inner().exchange_listener.lock().unwrap().take()
    }
}

/// A synchronization primitive that allows multiple threads within a runtime to agree
/// when a condition is satisfied.
pub(crate) struct Consensus(Broadcast<bool>);

impl Consensus {
    pub fn new() -> Self {
        Self(Broadcast::new())
    }

    /// Returns `true` if all workers vote `true`.
    ///
    /// # Arguments
    ///
    /// * `local` - Local vote by the current worker.
    pub async fn check(&self, local: bool) -> Result<bool, SchedulerError> {
        Ok(self.0.collect(local).await?.into_iter().all(identity))
    }
}

/// A synchronization primitive that allows multiple threads within a runtime to broadcast
/// a value to all other workers.
pub(crate) enum Broadcast<T> {
    SingleThreaded,
    MultiThreaded {
        notify_receiver: Arc<Notify>,
        exchange: Arc<Exchange<T>>,
    },
}

impl<T> Broadcast<T>
where
    T: Clone + Send + serde::Serialize + for<'de> serde::Deserialize<'de> + 'static,
{
    pub fn new() -> Self {
        match Runtime::runtime() {
            Some(runtime) if Runtime::num_workers() > 1 => {
                let worker_index = Runtime::worker_index();
                let exchange_id = runtime.sequence_next().try_into().unwrap();
                let exchange = Exchange::with_runtime(&runtime, exchange_id);

                let notify_receiver = Arc::new(Notify::new());
                let notify_receiver_clone = notify_receiver.clone();

                exchange.register_receiver_callback(worker_index, move || {
                    notify_receiver_clone.notify_one()
                });

                Self::MultiThreaded {
                    notify_receiver,
                    exchange,
                }
            }
            _ => Self::SingleThreaded,
        }
    }

    /// Returns values broadcast by all workers (including the current worker), indexed by worker id.
    ///
    /// # Arguments
    ///
    /// * `local` - Value broadcast by the current worker.
    pub async fn collect(&self, local: T) -> Result<Vec<T>, SchedulerError> {
        match self {
            Self::SingleThreaded => Ok(vec![local]),
            Self::MultiThreaded {
                notify_receiver,
                exchange,
            } => {
                let sender_notify = exchange.sender_notify(Runtime::worker_index());
                loop {
                    let notified = sender_notify.notified();
                    if exchange.try_send_all_with_serializer(repeat(local.clone()), |local| {
                        let mut fbuf = FBuf::new();
                        rmp_serde::encode::write(&mut fbuf, &local).unwrap();
                        fbuf
                    }) {
                        break;
                    }
                    if Runtime::kill_in_progress() {
                        return Err(SchedulerError::Killed);
                    }
                    notified.await;
                }
                // Receive and collect the status of each peer.
                let mut result = Vec::with_capacity(Runtime::num_workers());
                while !exchange.try_receive_all(
                    |status| result.push(status),
                    |data| rmp_serde::from_slice(&data).unwrap(),
                ) {
                    if Runtime::kill_in_progress() {
                        return Err(SchedulerError::Killed);
                    }
                    // Sleep if other threads are still working.
                    notify_receiver.notified().await;
                }
                Ok(result)
            }
        }
    }
}

/// Handle returned by `Runtime::run`.
#[derive(Debug)]
pub struct RuntimeHandle {
    runtime: Runtime,
    workers: Vec<(JoinHandle<()>, Unparker)>,
}

impl RuntimeHandle {
    fn new(runtime: Runtime, workers: Vec<(JoinHandle<()>, Unparker)>) -> Self {
        Self { runtime, workers }
    }

    /// Unpark worker thread.
    ///
    /// Workers release the CPU by parking when they have no work to do.
    /// This method unparks a thread after sending a command to it or
    /// when killing a circuit.
    pub(super) fn unpark_worker(&self, worker: usize) {
        self.workers[worker].1.unpark();
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
        for (_worker, unparker) in self.workers.iter() {
            unparker.unpark();
        }

        self.runtime
            .inner()
            .aux_threads
            .lock()
            .unwrap()
            .iter()
            .for_each(|(_h, unparker)| {
                unparker.unpark();
            });
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
            .map(|(h, _unparker)| h.join())
            .collect();

        // Once all fg threads have terminated, signal the aux threads to terminate as well.
        // Normally this is not needed, since this function is usually called from `kill_async`,
        // which already signals the aux threads to terminate, but it is useful when it is called
        // directly, e.g., in some tests.
        self.runtime
            .inner()
            .kill_signal
            .store(true, Ordering::SeqCst);

        self.runtime
            .inner()
            .aux_threads
            .lock()
            .unwrap()
            .iter()
            .for_each(|(_h, unparker)| {
                unparker.unpark();
            });

        // Wait for aux threads.
        self.runtime
            .inner()
            .aux_threads
            .lock()
            .unwrap()
            .drain(..)
            .for_each(|(h, _unparker)| {
                let _ = h.join();
            });

        // Terminate the tokio merger runtime.
        if let Some(tokio_merger_runtime) = self
            .runtime
            .inner()
            .tokio_merger_runtime
            .lock()
            .unwrap()
            .take()
        {
            // Block until all running merger tasks have yielded. At this point, the tokio runtime will have shut down automatically.
            drop(tokio_merger_runtime);
        }

        // This must happen after we wait for the background threads, because
        // they might try to initiate another merge before they exit, which
        // would require them to have access to storage, which is kept in the
        // local store.
        self.runtime.local_store().clear();

        results.into_iter().collect()
    }

    /// Retrieve panic info for a specific worker.
    pub fn worker_panic_info(
        &self,
        worker: usize,
        thread_type: ThreadType,
    ) -> Option<WorkerPanicInfo> {
        self.runtime.worker_panic_info(worker, thread_type)
    }

    /// Retrieve panic info for all workers.
    pub fn collect_panic_info(&self) -> Vec<(usize, ThreadType, WorkerPanicInfo)> {
        let mut result = Vec::new();

        for worker in 0..self.workers.len() {
            for thread_type in [ThreadType::Foreground, ThreadType::Background] {
                if let Some(panic_info) = self.worker_panic_info(worker, thread_type) {
                    result.push((worker, thread_type, panic_info))
                }
            }
        }
        result
    }

    /// Returns true if any worker has panicked.
    pub fn panicked(&self) -> bool {
        self.runtime.inner().panicked.load(Ordering::Acquire)
    }
}

/// Where a worker thread is.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum WorkerLocation {
    /// In this process.
    Local,
    /// Across the network.
    Remote,
}

/// An iterator for all of the workers in a runtime.
///
/// For every worker in a [Runtime], this iterator yields its [WorkerLocation].
#[derive(Clone, Debug)]
pub struct WorkerLocations {
    workers: Range<usize>,
    local_workers: Range<usize>,
}

impl Default for WorkerLocations {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkerLocations {
    /// Constructs a new iterator for all the worker locations in the current
    /// [Runtime].  Use [Iterator::enumerate] to also obtain the associated
    /// worker indexes.
    pub fn new() -> Self {
        if let Some(runtime) = Runtime::runtime() {
            let layout = runtime.layout();
            Self {
                workers: 0..layout.n_workers(),
                local_workers: layout.local_workers(),
            }
        } else {
            Self {
                workers: 0..1,
                local_workers: 0..1,
            }
        }
    }

    /// Returns the location of the worker with the given index.
    pub fn get(&self, worker: usize) -> WorkerLocation {
        debug_assert!(worker < self.workers.end);
        if self.local_workers.contains(&worker) {
            WorkerLocation::Local
        } else {
            WorkerLocation::Remote
        }
    }
}

impl Iterator for WorkerLocations {
    type Item = WorkerLocation;

    fn next(&mut self) -> Option<Self::Item> {
        let worker = self.workers.next()?;
        Some(self.get(worker))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.workers.len();
        (len, Some(len))
    }
}

impl ExactSizeIterator for WorkerLocations {}

#[cfg(test)]
mod tests {
    use super::{Runtime, RuntimeInner};
    use crate::{
        Circuit, RootCircuit,
        circuit::{
            CircuitConfig, Layout,
            dbsp_handle::{CircuitStorageConfig, Mode},
            metadata::{LOOSE_MEMORY_RECORDS_COUNT, MERGING_MEMORY_RECORDS_COUNT},
            schedule::{DynamicScheduler, Scheduler},
        },
        operator::Generator,
        profile::DbspProfile,
        storage::backend::FileId,
    };
    use enum_map::Enum;
    use feldera_buffer_cache::{CacheEntry, ThreadType};
    use feldera_storage::fbuf::{FBuf, slab::set_thread_slab_pool};
    use feldera_types::config::{
        DevTweaks, StorageCacheConfig, StorageConfig, StorageOptions,
        dev_tweaks::{BufferCacheAllocationStrategy, BufferCacheStrategy},
    };
    use feldera_types::memory_pressure::MemoryPressure;
    use std::{
        cell::RefCell,
        rc::Rc,
        sync::{Arc, LazyLock, Mutex},
        thread::sleep,
        time::{Duration, Instant},
    };

    struct TestCacheEntry(usize);

    impl CacheEntry for TestCacheEntry {
        fn cost(&self) -> usize {
            self.0
        }
    }

    // Serialize all tests that use MOCK_PROCESS_RSS_BYTES so they don't race with each other.
    static MOCK_RSS_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    fn set_mock_process_rss_bytes(bytes: u64) {
        // Safety: We assume that only DBSP code accesses the environment variables.
        unsafe {
            std::env::set_var("MOCK_PROCESS_RSS_BYTES", bytes.to_string());
        }
    }

    struct MockRssVarGuard;

    impl Drop for MockRssVarGuard {
        fn drop(&mut self) {
            // Safety: We assume that only DBSP code accesses the environment variables.
            unsafe {
                std::env::remove_var("MOCK_PROCESS_RSS_BYTES");
            }
        }
    }

    fn query_runtime_memory_state(runtime: &Runtime) -> (MemoryPressure, usize, usize, usize) {
        let (sender, receiver) = std::sync::mpsc::channel();
        runtime.spawn_aux_thread(
            "memory-pressure-test-query",
            super::Parker::new(),
            move |_parker| {
                let _ = sender.send((
                    Runtime::memory_pressure().expect("query thread should run inside runtime"),
                    Runtime::min_merge_storage_bytes().expect("runtime has storage configured"),
                    Runtime::min_insert_storage_bytes().expect("runtime has storage configured"),
                    Runtime::min_step_storage_bytes().expect("runtime has storage configured"),
                ));
            },
        );
        receiver
            .recv_timeout(Duration::from_secs(5))
            .expect("failed to query memory state from runtime thread")
    }

    fn count_metric(
        profile: &DbspProfile,
        metric: &'static crate::circuit::metadata::MetricId,
    ) -> usize {
        let root_node = crate::circuit::GlobalNodeId::root();
        profile
            .worker_profiles
            .iter()
            .map(|worker| {
                worker
                    .get_node_profile(&root_node)
                    .map(|meta| {
                        meta.iter()
                            .filter_map(|((metric_id, _labels), value)| {
                                if metric_id == metric {
                                    match value {
                                        crate::circuit::metadata::MetaItem::Count(count) => {
                                            Some(*count)
                                        }
                                        _ => None,
                                    }
                                } else {
                                    None
                                }
                            })
                            .sum::<usize>()
                    })
                    .unwrap_or(0)
            })
            .sum()
    }

    fn in_memory_spine_records(profile: &DbspProfile) -> usize {
        count_metric(profile, &LOOSE_MEMORY_RECORDS_COUNT)
            + count_metric(profile, &MERGING_MEMORY_RECORDS_COUNT)
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
        let path = tempfile::tempdir().unwrap().keep();
        let path_clone = path.clone();
        let cconf = CircuitConfig {
            layout: Layout::new_solo(4),
            max_rss_bytes: None,
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
            dev_tweaks: DevTweaks::default(),
            exchange_listener: None,
        };

        let hruntime = Runtime::run(cconf, move |_parker| {
            let runtime = Runtime::runtime().unwrap();
            assert_eq!(runtime.storage_path(), Some(path_clone.as_ref()));
        })
        .expect("failed to start runtime");
        hruntime.join().unwrap();
        assert!(path.exists(), "persistent storage is not cleaned up");
    }

    #[test]
    fn s3_fifo_is_the_default_buffer_cache_strategy() {
        let inner = RuntimeInner::new(CircuitConfig::with_workers(1)).unwrap();
        assert_eq!(
            inner.buffer_caches[0][ThreadType::Foreground].strategy(),
            BufferCacheStrategy::S3Fifo
        );
    }

    #[test]
    fn default_s3_fifo_cache_shares_backend_per_worker_pair() {
        let inner = RuntimeInner::new(CircuitConfig::with_workers(2)).unwrap();
        assert!(
            inner.buffer_caches[0][ThreadType::Foreground]
                .shares_backend_with(&inner.buffer_caches[0][ThreadType::Background])
        );
        assert!(
            inner.buffer_caches[1][ThreadType::Foreground]
                .shares_backend_with(&inner.buffer_caches[1][ThreadType::Background])
        );
        assert!(
            !inner.buffer_caches[0][ThreadType::Foreground]
                .shares_backend_with(&inner.buffer_caches[1][ThreadType::Foreground])
        );
    }

    #[test]
    fn lru_can_still_be_selected_explicitly() {
        let inner = RuntimeInner::new(
            CircuitConfig::with_workers(1).with_buffer_cache_strategy(BufferCacheStrategy::Lru),
        )
        .unwrap();
        assert_eq!(
            inner.buffer_caches[0][ThreadType::Foreground].strategy(),
            BufferCacheStrategy::Lru
        );
        assert!(
            !inner.buffer_caches[0][ThreadType::Foreground]
                .shares_backend_with(&inner.buffer_caches[0][ThreadType::Background])
        );
    }

    #[test]
    fn lru_uses_default_total_cache_capacity_when_cache_mib_is_unset() {
        let workers = 3usize;
        let path = tempfile::tempdir().unwrap();
        let storage = CircuitStorageConfig::for_config(
            StorageConfig {
                path: path.path().to_string_lossy().into_owned(),
                cache: StorageCacheConfig::default(),
            },
            StorageOptions::default(),
        )
        .unwrap();
        let runtime = Runtime(Arc::new(
            RuntimeInner::new(
                CircuitConfig::with_workers(workers)
                    .with_storage(storage)
                    .with_buffer_cache_strategy(BufferCacheStrategy::Lru),
            )
            .unwrap(),
        ));

        assert_eq!(
            runtime.cache_occupancy(),
            (0, workers * ThreadType::LENGTH * 256usize * 1024 * 1024)
        );
    }

    #[test]
    fn s3_fifo_cache_can_share_cache_per_worker_pair_when_requested() {
        let config = CircuitConfig::with_workers(2).with_buffer_cache_allocation_strategy(
            BufferCacheAllocationStrategy::SharedPerWorkerPair,
        );
        let inner = RuntimeInner::new(config).unwrap();
        assert!(
            inner.buffer_caches[0][ThreadType::Foreground]
                .shares_backend_with(&inner.buffer_caches[0][ThreadType::Background])
        );
        assert!(
            inner.buffer_caches[1][ThreadType::Foreground]
                .shares_backend_with(&inner.buffer_caches[1][ThreadType::Background])
        );
    }

    #[test]
    fn fbuf_slabs_are_shared_per_worker_pair() {
        let inner = RuntimeInner::new(CircuitConfig::with_workers(2)).unwrap();
        assert!(Arc::ptr_eq(
            &inner.fbuf_slab_allocators[0][ThreadType::Foreground],
            &inner.fbuf_slab_allocators[0][ThreadType::Background],
        ));
        assert!(Arc::ptr_eq(
            &inner.fbuf_slab_allocators[1][ThreadType::Foreground],
            &inner.fbuf_slab_allocators[1][ThreadType::Background],
        ));
        assert!(!Arc::ptr_eq(
            &inner.fbuf_slab_allocators[0][ThreadType::Foreground],
            &inner.fbuf_slab_allocators[1][ThreadType::Foreground],
        ));
    }

    #[test]
    fn fbuf_slabs_honor_bytes_per_class_dev_tweak() {
        let inner =
            RuntimeInner::new(CircuitConfig::with_workers(1).with_fbuf_slab_bytes_per_class(4096))
                .unwrap();
        let allocator = inner.fbuf_slab_allocators[0][ThreadType::Foreground].clone();
        let previous = set_thread_slab_pool(Some(allocator.clone()));

        let first = FBuf::with_capacity(4096);
        let second = FBuf::with_capacity(4096);
        drop(first);
        drop(second);

        set_thread_slab_pool(previous);

        assert_eq!(allocator.stats().cached_buffers, 1);
    }

    #[test]
    fn sieve_can_share_one_global_cache_when_requested() {
        let config = CircuitConfig::with_workers(2)
            .with_buffer_cache_allocation_strategy(BufferCacheAllocationStrategy::Global);
        let inner = RuntimeInner::new(config).unwrap();
        let global = inner.buffer_caches[0][ThreadType::Foreground].clone();
        assert!(global.shares_backend_with(&inner.buffer_caches[0][ThreadType::Background]));
        assert!(global.shares_backend_with(&inner.buffer_caches[1][ThreadType::Foreground]));
        assert!(global.shares_backend_with(&inner.buffer_caches[1][ThreadType::Background]));
    }

    #[test]
    fn lru_keeps_separate_foreground_and_background_caches() {
        let config = CircuitConfig::with_workers(2)
            .with_buffer_cache_strategy(BufferCacheStrategy::Lru)
            .with_buffer_cache_allocation_strategy(
                BufferCacheAllocationStrategy::SharedPerWorkerPair,
            );
        let inner = RuntimeInner::new(config).unwrap();
        assert!(
            !inner.buffer_caches[0][ThreadType::Foreground]
                .shares_backend_with(&inner.buffer_caches[0][ThreadType::Background])
        );
        assert!(
            !inner.buffer_caches[1][ThreadType::Foreground]
                .shares_backend_with(&inner.buffer_caches[1][ThreadType::Background])
        );
    }

    #[test]
    fn shared_sharded_cache_occupancy_is_not_double_counted() {
        let path = tempfile::tempdir().unwrap();
        let storage = CircuitStorageConfig::for_config(
            StorageConfig {
                path: path.path().to_string_lossy().into_owned(),
                cache: StorageCacheConfig::default(),
            },
            StorageOptions {
                cache_mib: Some(8),
                ..StorageOptions::default()
            },
        )
        .unwrap();
        let runtime = Runtime(Arc::new(
            RuntimeInner::new(
                CircuitConfig::with_workers(1)
                    .with_storage(storage)
                    .with_buffer_cache_allocation_strategy(
                        BufferCacheAllocationStrategy::SharedPerWorkerPair,
                    ),
            )
            .unwrap(),
        ));

        runtime.get_buffer_cache(0, ThreadType::Foreground).insert(
            FileId::new(),
            0,
            Arc::new(TestCacheEntry(1024)),
        );

        assert_eq!(runtime.cache_occupancy(), (1024, 8 * 1024 * 1024));
    }

    #[test]
    fn global_sharded_cache_occupancy_is_not_double_counted() {
        let path = tempfile::tempdir().unwrap();
        let storage = CircuitStorageConfig::for_config(
            StorageConfig {
                path: path.path().to_string_lossy().into_owned(),
                cache: StorageCacheConfig::default(),
            },
            StorageOptions {
                cache_mib: Some(8),
                ..StorageOptions::default()
            },
        )
        .unwrap();
        let runtime = Runtime(Arc::new(
            RuntimeInner::new(
                CircuitConfig::with_workers(2)
                    .with_storage(storage)
                    .with_buffer_cache_allocation_strategy(BufferCacheAllocationStrategy::Global),
            )
            .unwrap(),
        ));

        runtime.get_buffer_cache(1, ThreadType::Background).insert(
            FileId::new(),
            0,
            Arc::new(TestCacheEntry(1024)),
        );

        assert_eq!(runtime.cache_occupancy(), (1024, 8 * 1024 * 1024));
    }

    #[test]
    fn shared_fbuf_slab_stats_are_not_double_counted() {
        let path = tempfile::tempdir().unwrap();
        let storage = CircuitStorageConfig::for_config(
            StorageConfig {
                path: path.path().to_string_lossy().into_owned(),
                cache: StorageCacheConfig::default(),
            },
            StorageOptions::default(),
        )
        .unwrap();
        let runtime = Runtime(Arc::new(
            RuntimeInner::new(CircuitConfig::with_workers(1).with_storage(storage)).unwrap(),
        ));

        let previous = set_thread_slab_pool(Some(
            runtime.get_fbuf_slab_allocator(0, ThreadType::Foreground),
        ));

        let first = FBuf::with_capacity(4096);
        drop(first);
        let second = FBuf::with_capacity(3000);
        drop(second);

        set_thread_slab_pool(previous);

        let stats = runtime.fbuf_slabs_stats();
        let class = stats
            .classes
            .iter()
            .find(|class| class.size == 4096)
            .unwrap();

        assert_eq!(stats.alloc_requests(), 2);
        assert_eq!(stats.mallocs_saved(), 1);
        assert_eq!(stats.frees_saved(), 2);
        assert_eq!(stats.cached_buffers, 1);
        assert_eq!(class.alloc_requests, 2);
        assert_eq!(class.alloc_hits, 1);
        assert_eq!(class.recycle_requests, 2);
        assert_eq!(class.recycle_hits, 2);
    }

    fn test_runtime<S>()
    where
        S: Scheduler + 'static,
    {
        let hruntime = Runtime::run(4, |_parker| {
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
                root.transaction().unwrap();
            }

            // The scheduler allocates the first value for metadata exchange; therefore the output starts from 2.
            assert_eq!(&*data.borrow(), &(2..102).collect::<Vec<usize>>());
        })
        .expect("failed to start runtime");

        hruntime.join().unwrap();
    }

    #[test]
    fn test_kill_dynamic() {
        test_kill::<DynamicScheduler>();
    }

    // Test `RuntimeHandle::kill`.
    fn test_kill<S>()
    where
        S: Scheduler + 'static,
    {
        let hruntime = Runtime::run(16, |_parker| {
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
                        Ok((async || Ok(false), ()))
                    })
                    .unwrap();
                Ok(())
            })
            .unwrap()
            .0;

            loop {
                if root.transaction().is_err() {
                    return;
                }
            }
        })
        .expect("failed to start runtime");

        sleep(Duration::from_millis(100));
        hruntime.kill().unwrap();
    }

    // Test the memory pressure thresholds and how merger threads behave under different memory pressure levels.
    #[test]
    fn memory_pressure_thresholds_and_spill_behavior() {
        const GIB: u64 = 1024 * 1024 * 1024;
        const MIB: u64 = 1024 * 1024;
        const TEN_MIB: usize = 10 * 1024 * 1024;

        let _mock_guard = MOCK_RSS_LOCK.lock().unwrap();
        let _clear_mock_rss = MockRssVarGuard;
        set_mock_process_rss_bytes(0);

        let storage_dir = tempfile::tempdir().unwrap();
        let config = CircuitConfig::with_workers(1)
            .with_temporary_storage(storage_dir.path())
            .with_max_rss_bytes(Some(10 * GIB));

        // Create a circuit with a single spine.
        let (mut circuit, input_handle) = Runtime::init_circuit(config, move |circuit| {
            let (stream, input_handle) = circuit.add_input_zset::<u64>();
            stream.accumulate_integrate_trace();
            Ok(input_handle)
        })
        .unwrap();

        // Push 7 batches. This is below the merge threshold, so no batches should get merged,
        // and al of them should live in memory.
        for key in 0..7 {
            input_handle.push(key, 1);
            circuit.transaction().unwrap();
        }

        // Baseline (mock RSS = 0): no pressure and default spill thresholds.
        let (pressure, min_merge, min_insert, min_step) =
            query_runtime_memory_state(circuit.runtime());
        assert_eq!(pressure, MemoryPressure::Low);

        // Verify the spill thresholds: at low pressure, merge threshold is set to the user-configured `min_storage_bytes`,
        // insert threshold is set to `usize::MAX`, and step threshold is set to `usize::MAX`.
        assert_eq!(min_merge, TEN_MIB);
        assert_eq!(min_insert, usize::MAX);
        assert_eq!(min_step, usize::MAX);

        // Verify the spine currently holds in-memory tuples before pressure rises.
        let profile = circuit.retrieve_profile().unwrap();
        assert!(in_memory_spine_records(&profile) == 7);

        // Moderate pressure (8.8/10 GiB): merge threshold drops to 0, insert/step stay unchanged.
        set_mock_process_rss_bytes(8800 * MIB);
        sleep(Duration::from_secs(5));

        let (pressure, min_merge, min_insert, min_step) =
            query_runtime_memory_state(circuit.runtime());
        assert_eq!(pressure, MemoryPressure::Moderate);
        assert_eq!(min_merge, 0);
        assert_eq!(min_insert, TEN_MIB);
        assert_eq!(min_step, usize::MAX);

        // High pressure (9.4/10 GiB): insert threshold drops to 0 as well.
        set_mock_process_rss_bytes(9400 * MIB);
        sleep(Duration::from_secs(5));

        let (pressure, min_merge, min_insert, min_step) =
            query_runtime_memory_state(circuit.runtime());
        assert_eq!(pressure, MemoryPressure::High);
        assert_eq!(min_merge, 0);
        assert_eq!(min_insert, 0);
        assert_eq!(min_step, usize::MAX);

        // Under high pressure, merges should eventually spill all in-memory tuples to storage.
        // Poll profile until this converges to 0.
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let profile = circuit.retrieve_profile().unwrap();
            if in_memory_spine_records(&profile) == 0 {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "in-memory spine records did not drain to zero under high memory pressure"
            );
            sleep(Duration::from_millis(250));
        }

        // Critical pressure (9.8/10 GiB): all spill thresholds must be 0.
        set_mock_process_rss_bytes(9800 * MIB);
        sleep(Duration::from_secs(3));

        let (pressure, min_merge, min_insert, min_step) =
            query_runtime_memory_state(circuit.runtime());
        assert_eq!(pressure, MemoryPressure::Critical);
        assert_eq!(min_merge, 0);
        assert_eq!(min_insert, 0);
        assert_eq!(min_step, 0);

        circuit.kill().unwrap();
    }
}
