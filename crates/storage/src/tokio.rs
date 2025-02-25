use once_cell::sync::Lazy;
use tokio::runtime::Runtime;

/// The process running dbsp can share a single Tokio runtime.
///
/// This is `pub` so it can be used in dbsp and adapters too for IO.
pub static TOKIO: Lazy<Runtime> = Lazy::new(|| Runtime::new().unwrap());
