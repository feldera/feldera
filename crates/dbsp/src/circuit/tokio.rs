use once_cell::sync::Lazy;
use tokio::runtime::{Builder, Runtime};

/// The process running dbsp can share a single Tokio runtime.
///
/// This is `pub` so it can be used in adapters too for IO.
pub static TOKIO: Lazy<Runtime> = Lazy::new(|| {
    Builder::new_multi_thread()
        .worker_threads(4)
        .thread_name("dbsp-io")
        .build()
        .unwrap()
});
