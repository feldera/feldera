use once_cell::sync::Lazy;
use std::{env, thread};
use tokio::runtime::{Builder, Runtime};
use tracing::debug;

/// The process running dbsp can share a single Tokio runtime.
///
/// This is `pub` so it can be used in dbsp and adapters too for IO.
pub static TOKIO: Lazy<Runtime> = Lazy::new(|| {
    debug!(
        "starting service dbsp io tokio runtime, workers: {}",
        env::var("TOKIO_WORKER_THREADS")
            .unwrap_or_else(|_| { thread::available_parallelism().unwrap().get().to_string() })
    );
    Builder::new_multi_thread()
        .thread_name_fn(|| {
            use std::sync::atomic::{AtomicUsize, Ordering};
            static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
            let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
            format!("feldera-tokio-{}", id)
        })
        .thread_stack_size(6 * 1024 * 1024)
        .enable_all()
        .build()
        .unwrap()
});

/// Some connectors (currently only delta) need a dedicated runtime that only runs IO operations.
/// This runtime can be used for this purpose. It should not be use for any CPU-intensive tasks
/// such as parsing.
pub static TOKIO_DEDICATED_IO: Lazy<Runtime> = Lazy::new(|| {
    debug!(
        "starting service dedicated io tokio runtime, workers: {}",
        env::var("TOKIO_WORKER_THREADS")
            .unwrap_or_else(|_| { thread::available_parallelism().unwrap().get().to_string() })
    );
    Builder::new_multi_thread()
        .thread_name_fn(|| {
            use std::sync::atomic::{AtomicUsize, Ordering};
            static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
            let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
            format!("feldera-io-tokio-{}", id)
        })
        .thread_stack_size(6 * 1024 * 1024)
        .enable_all()
        .build()
        .unwrap()
});
