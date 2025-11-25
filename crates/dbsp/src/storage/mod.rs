//! Storage APIs for Feldera.
//!
//! The API consists of a lower layer, [backend], that provides block-based
//! access to storage, a middle layer, [buffer_cache], that implements
//! buffering, and an upper layer, [mod@file], that implements data access.
pub mod backend;
pub mod buffer_cache;
pub mod dirlock;
pub mod file;
pub mod parquet;
#[cfg(test)]
mod test;

use fdlimit::{raise_fd_limit, Outcome::LimitRaised};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use tracing::{info, warn};

use crate::{Error, Runtime};
use std::sync::Once;
use uuid::Uuid;

/// Return the absolute path for a checkpoint.
///
/// # Panics
/// - If the `Runtime` is unavailable.
///
/// # Arguments
/// - `cid`: The checkpoint id.
pub(crate) fn checkpoint_path(cid: Uuid) -> PathBuf {
    let rt = Runtime::runtime().unwrap();
    let mut path = rt.storage_path();
    path.push(cid.to_string());
    path
}

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

/// Raise the locked memory limit to avoid surprises.
#[cfg(unix)]
fn init_locked_memory() {
    use rlimit::{getrlimit, setrlimit, Resource};

    let (soft, hard) = match getrlimit(Resource::MEMLOCK) {
        Ok(limits) => limits,
        Err(error) => {
            warn!("Failed to query locked memory limit: {error}");
            return;
        }
    };

    if soft < hard {
        if let Err(error) = setrlimit(Resource::MEMLOCK, hard, hard) {
            warn!("Failed to raise locked memory from soft limit ({soft} bytes) to hard limit ({hard} bytes): {error}");
            return;
        }
    }

    const MIN_REASONABLE: u64 = 4 * 1024 * 1024 * 1024;
    if hard < MIN_REASONABLE {
        warn!("Locked memory hard limit is low ({hard} bytes) -- try increasing the configured value (in your limits.conf).");
    } else {
        info!("Locked memory limit is {hard} bytes.");
    }
}

#[cfg(not(unix))]
fn init_locked_memory() {}

/// Performs storage module initialization.
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
        init_locked_memory();
    });
}
