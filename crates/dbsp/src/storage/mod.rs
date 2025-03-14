//! Storage APIs for Feldera.
//!
//! The API consists of a lower layer, [backend], that provides block-based
//! access to storage, a middle layer, [buffer_cache], that implements
//! buffering, and an upper layer, [mod@file], that implements data access.
pub mod backend;
pub mod buffer_cache;
pub mod dirlock;
pub mod file;
#[cfg(test)]
mod test;

use fdlimit::{raise_fd_limit, Outcome::LimitRaised};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use tracing::warn;

use crate::Error;
use std::sync::Once;

/// A helper function to write the commit metadata to a file.
///
/// The method ensures that the file does not exist before writing the metadata,
/// and that the content is flushed to disk.
///
/// # Arguments
/// - `path`: The path to write the metadata to.
/// - `content`: The content to write.
pub(crate) fn write_commit_metadata<P: AsRef<Path>>(path: P, content: &[u8]) -> Result<(), Error> {
    let mut f = OpenOptions::new().create_new(true).write(true).open(path)?;
    f.write_all(content)?;
    f.sync_all()?;
    Ok(())
}

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

/// Performs storage module initialization.
///
/// On Unix, this raises the process's maximum file descriptor from its "soft"
/// to "hard" limit.  On Windows, this has no effect.
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
