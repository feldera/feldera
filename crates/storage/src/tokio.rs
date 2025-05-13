use once_cell::sync::Lazy;
use tokio::runtime::{Builder, Runtime};

/// The process running dbsp can share a single Tokio runtime.
///
/// This is `pub` so it can be used in dbsp and adapters too for IO.
pub static TOKIO: Lazy<Runtime> = Lazy::new(|| {
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
