//! A simple PID-based locking mechanism.
//!
//! Makes sure we don't accidentally run multiple instances of the program
//! using the same data directory.

use log::{debug, warn};
use std::fs::{File, OpenOptions};
use std::io::{self, Error as IoError, ErrorKind, Read, Write};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use sysinfo::{Pid, System};

use crate::storage::backend::StorageError;

#[cfg(test)]
mod test;

/// Check whether a process exists, used to determine whether a pid file is
/// stale.
fn process_exists(pid: u32) -> bool {
    let s = System::new_all();
    s.process(Pid::from(pid as usize)).is_some()
}

/// An instance of a PID file.
///
/// The PID file is removed from the FS when the instance is dropped.
#[derive(Debug)]
pub struct LockedDirectory {
    base: PathBuf,
    pid: Pid,
}

impl Drop for LockedDirectory {
    fn drop(&mut self) {
        let pid_file = self.base.join(LockedDirectory::LOCKFILE_NAME);
        if pid_file.exists() {
            let r = std::fs::remove_file(&pid_file);
            if let Err(e) = r {
                warn!("Failed to remove pidfile: {}", e);
            }
        }
    }
}

/// A guard that holds an exclusive lock on a file.
///
/// The lock is dropped once the guard is dropped.
/// This avoids concurrency issues when two pipelines are started at the same
/// time.
struct FlockGuard(File);

impl FlockGuard {
    fn new(file: File) -> Result<FlockGuard, IoError> {
        #[cfg(unix)]
        {
            let r = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
            if r != 0 {
                return Err(io::Error::last_os_error());
            }
        }
        Ok(FlockGuard(file))
    }
}

impl Drop for FlockGuard {
    fn drop(&mut self) {
        #[cfg(unix)]
        {
            let r = unsafe { libc::flock(self.0.as_raw_fd(), libc::LOCK_UN) };
            if r != 0 {
                debug!("Failed to unlock pidfile: {}", io::Error::last_os_error());
            }
        }
    }
}

impl LockedDirectory {
    const LOCKFILE_NAME: &'static str = "feldera.pidlock";

    fn with_pid<P: AsRef<Path>>(base_path: P, pid: Pid) -> Result<LockedDirectory, StorageError> {
        let pid_str = pid.to_string();
        let pid_file = base_path.as_ref().join(LockedDirectory::LOCKFILE_NAME);
        let mut guard = if pid_file.exists() {
            // we set create(true) for both branches, to avoid concurrency issues when two
            // pipelines are started at the same time.
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&pid_file)?;
            let mut guard = FlockGuard::new(file).map_err(|e: IoError| {
                if e.kind() == ErrorKind::WouldBlock {
                    StorageError::StorageLocked(0, pid_file.clone())
                } else {
                    e.into()
                }
            })?;

            // From here on we have a lock on the file:
            let mut contents = String::new();
            guard.0.read_to_string(&mut contents)?;

            let old_pid = contents.trim().parse::<u32>();
            if let Ok(old_pid) = old_pid {
                if process_exists(old_pid) {
                    return Err(StorageError::StorageLocked(old_pid, pid_file));
                } else if old_pid == pid.as_u32() {
                    // The pidfile is ours, just leave it as is.
                } else {
                    // The process doesn't exist, so we can safely overwrite the pidfile.
                    log::debug!("Found stale pidfile: {}", pid_file.display());
                }
            } else {
                // If the pidfile is corrupt, we won't take ownership of the storage dir until
                // the user fixes it.
                log::error!(
                    "Invalid pidfile contents: '{}' in {}, pipeline refused to take ownership of storage dir.",
                    contents,
                    pid_file.display(),
                );
                return Err(StorageError::StorageLocked(0, pid_file));
            }

            guard
        } else {
            FlockGuard::new(
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(&pid_file)?,
            )?
        };
        guard.0.write_all(pid_str.as_bytes())?;

        Ok(LockedDirectory {
            base: base_path.as_ref().into(),
            pid,
        })
    }

    /// Attempts to create a new pidfile in the `base_path` directory,
    /// returning an error if the file was already created by a different
    /// process (and that process is still alive).
    ///
    /// # Arguments
    /// - `base_path`: The directory in which to create the pidfile. It must
    ///   already exist.
    ///
    /// # Panics
    /// - If the current process's PID cannot be determined.
    pub fn new<P: AsRef<Path>>(base_path: P) -> Result<LockedDirectory, StorageError> {
        let pid = sysinfo::get_current_pid().expect("failed to get current pid");
        if !base_path.as_ref().exists() {
            return Err(StorageError::StorageLocationNotFound(
                base_path.as_ref().to_path_buf(),
            ));
        }
        Self::with_pid(base_path, pid)
    }

    /// Returns the PID of the process that created the pidfile.
    pub fn pid(&self) -> Pid {
        self.pid
    }

    /// Returns the path to the directory in which the pidfile was created.
    pub fn base(&self) -> &Path {
        self.base.as_path()
    }
}
