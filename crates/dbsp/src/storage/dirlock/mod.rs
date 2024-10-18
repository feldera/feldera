//! A simple PID-based locking mechanism.
//!
//! Makes sure we don't accidentally run multiple instances of the program
//! using the same data directory.

use libc::{c_int, c_short};
use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{Error as IoError, ErrorKind};
use std::os::fd::AsRawFd;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::process;
use std::sync::{LazyLock, Mutex};

use crate::storage::backend::StorageError;

#[cfg(test)]
mod test;

/// Lock table.
///
/// We track the locks that we currently hold, in terms of the device and inode
/// of the lock file, because POSIX says that closing *any* file descriptor on
/// which a process holds a lock drops *all* locks on that file (see fcntl(2)).
/// Therefore, we can't afford to open such a file more than once.
///
/// Linux has its own "open file description locks", introduced in Linux 3.15,
/// that are non-POSIX, which avoid this problem. Maybe we should use those
/// instead, if portability and backward compatibility are not paramount.
static LOCKS: LazyLock<Mutex<HashSet<(u64, u64)>>> = LazyLock::new(|| Mutex::new(HashSet::new()));

/// An instance of a PID file.
#[derive(Debug)]
pub struct LockedDirectory {
    /// The directory that we've locked.
    base: PathBuf,

    /// The lockfile.
    ///
    /// This member just holds the file descriptor open until we're dropped, at
    /// which time `File` will close the descriptor and the OS will drop the
    /// lock.
    _file: File,

    /// Device and inode of the file, so that we can remove ourselves from
    /// [LOCKS] when we're dropped.
    dev_ino: (u64, u64),
}

impl Drop for LockedDirectory {
    fn drop(&mut self) {
        assert!(LOCKS.lock().unwrap().remove(&self.dev_ino));
    }
}

fn fcntl_lock(file: &File, cmd: c_int) -> Result<libc::flock, IoError> {
    let mut flock = libc::flock {
        l_type: libc::F_WRLCK as c_short,
        l_whence: libc::SEEK_SET as c_short,
        l_start: 0,
        l_len: 0,
        l_pid: 0,
    };
    match unsafe { libc::fcntl(file.as_raw_fd(), cmd, &mut flock as *mut libc::flock) } {
        -1 => Err(IoError::last_os_error()),
        _ => Ok(flock),
    }
}

fn write_lock(file: &File) -> Result<(), IoError> {
    fcntl_lock(file, libc::F_SETLK).map(|_| ())
}

fn get_lock(file: &File) -> Result<Option<u32>, IoError> {
    fcntl_lock(file, libc::F_GETLK).map(|flock| match flock.l_type as c_int {
        libc::F_UNLCK => None,
        _ => Some(flock.l_pid as u32),
    })
}

impl LockedDirectory {
    const LOCKFILE_NAME: &'static str = "feldera.pidlock";

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
        let base = base_path.as_ref().to_path_buf();
        let pid_file = base.join(LockedDirectory::LOCKFILE_NAME);

        // Did we already lock it?
        let mut locks = LOCKS.lock().unwrap();
        match fs::metadata(&pid_file) {
            Ok(metadata) => {
                let dev_ino = (metadata.dev(), metadata.ino());
                if locks.contains(&dev_ino) {
                    return Err(StorageError::StorageLocked(process::id(), base));
                }
            }
            Err(error) if error.kind() == ErrorKind::NotFound => (),
            Err(error) => return Err(error.into()),
        }

        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&pid_file)?;
        let metadata = file.metadata()?;
        let dev_ino = (metadata.dev(), metadata.ino());

        match write_lock(&file) {
            Err(error)
                if error.kind() == ErrorKind::PermissionDenied
                    || error.kind() == ErrorKind::WouldBlock =>
            {
                Err(StorageError::StorageLocked(
                    get_lock(&file).unwrap_or(None).unwrap_or(0),
                    base,
                ))
            }
            Err(error) => Err(error.into()),
            Ok(()) => {
                locks.insert(dev_ino);
                Ok(Self {
                    base,
                    _file: file,
                    dev_ino,
                })
            }
        }
    }

    /// Returns the path to the directory in which the pidfile was created.
    pub fn base(&self) -> &Path {
        self.base.as_path()
    }
}
