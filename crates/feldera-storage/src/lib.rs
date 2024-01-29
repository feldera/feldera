pub mod backend;
pub mod buffer_cache;
pub mod file;

use fdlimit::raise_fd_limit;
use fdlimit::Outcome::LimitRaised;
use log::warn;

use std::sync::Once;

/// Raise the fd limit to run the storage library to avoid surprises.  This
/// is a no-op on Windows, so it does not need to be behind an architectural
/// cfg.
fn init_fd_limit() {
    match raise_fd_limit() {
        Ok(LimitRaised { from, to }) => {
            const WARN_THRESHOLD: u64 = 1 << 19;
            if to < WARN_THRESHOLD {
                warn!("Raised fd limit from {} to {}. It's still very low -- try increasing the fd hard-limit (in your limits.conf).", from, to);
            }
        }
        Ok(_) => { /* not on unix */ }
        Err(e) => {
            warn!("Failed to raise fd limit: {}", e);
        }
    }
}

/// Performs global crate initialization.
///
/// On Unix, this raises the process's file descriptor and locked memory limits
/// from their "soft" to "hard" limits.  On Windows, this has no effect.
///
/// This function is idempotent and initializing any of the storage backends
/// will call it automatically.  It can log, so initialize logging before
/// calling it.
pub fn init() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        init_fd_limit();
    });
}
