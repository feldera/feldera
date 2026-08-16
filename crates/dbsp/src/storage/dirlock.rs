//! A simple PID-based locking mechanism.
//!
//! Makes sure we don't accidentally run multiple instances of the program
//! using the same data directory.

#[cfg(unix)]
use libc::{c_int, c_short};
use std::collections::HashSet;
#[cfg(unix)]
use std::fs;
use std::fs::File;
use std::io::{Error as IoError, ErrorKind};
#[cfg(unix)]
use std::os::fd::AsRawFd;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::process;
use std::sync::{LazyLock, Mutex};
use std::thread::sleep;
use std::time::{Duration, Instant};
use tracing::{error, info};

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
#[cfg(unix)]
type LockId = (u64, u64);
#[cfg(windows)]
type LockId = (u32, u64);
static LOCKS: LazyLock<Mutex<HashSet<LockId>>> = LazyLock::new(|| Mutex::new(HashSet::new()));

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
    lock_id: LockId,
}

impl Drop for LockedDirectory {
    fn drop(&mut self) {
        assert!(LOCKS.lock().unwrap().remove(&self.lock_id));
    }
}

#[cfg(unix)]
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

#[cfg(unix)]
fn write_lock(file: &File) -> Result<(), IoError> {
    fcntl_lock(file, libc::F_SETLK).map(|_| ())
}

#[cfg(unix)]
fn get_lock(file: &File) -> Result<Option<u32>, IoError> {
    // workaround: the size / type of `libc::F_UNLCK` can be different in
    // different platforms. Resulting in the follow up pattern matching to fail.
    //
    // In linux, it is `c_int`:
    // https://github.com/rust-lang/libc/blob/b667ac1fffe3b07b2ac68b6e8dec252f6e3e9993/src/unix/linux_like/linux/gnu/b64/s390x.rs#L485
    // In bsd / macos, it is `c_short`:
    // https://github.com/rust-lang/libc/blob/b667ac1fffe3b07b2ac68b6e8dec252f6e3e9993/src/unix/bsd/mod.rs#L349
    const F_UNLCK: c_int = libc::F_UNLCK as c_int;

    fcntl_lock(file, libc::F_GETLK).map(|flock| match flock.l_type as c_int {
        F_UNLCK => None,
        _ => Some(flock.l_pid as u32),
    })
}

#[cfg(windows)]
/// Attempts to acquire an exclusive nonblocking lock over the entire file.
fn write_lock(file: &File) -> Result<(), IoError> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::{
        Storage::FileSystem::{LOCKFILE_EXCLUSIVE_LOCK, LOCKFILE_FAIL_IMMEDIATELY, LockFileEx},
        System::IO::OVERLAPPED,
    };

    let mut overlapped = OVERLAPPED::default();
    // SAFETY: `file` owns a valid handle and `overlapped` remains valid for
    // this synchronous whole-file lock request.
    let result = unsafe {
        LockFileEx(
            file.as_raw_handle(),
            LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY,
            0,
            u32::MAX,
            u32::MAX,
            &mut overlapped,
        )
    };
    if result == 0 {
        Err(IoError::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(windows)]
/// Windows does not expose the PID owning a conflicting file lock.
fn get_lock(_file: &File) -> Result<Option<u32>, IoError> {
    Ok(None)
}

#[cfg(windows)]
/// Returns a stable identity for `file` from its volume serial number and file index.
fn lock_id(file: &File) -> Result<LockId, IoError> {
    use std::{mem::MaybeUninit, os::windows::io::AsRawHandle};
    use windows_sys::Win32::Storage::FileSystem::{
        BY_HANDLE_FILE_INFORMATION, GetFileInformationByHandle,
    };

    let mut info = MaybeUninit::<BY_HANDLE_FILE_INFORMATION>::uninit();
    // SAFETY: `file` owns a valid handle and `info` provides writable storage
    // for GetFileInformationByHandle's output.
    let result = unsafe { GetFileInformationByHandle(file.as_raw_handle(), info.as_mut_ptr()) };
    if result == 0 {
        return Err(IoError::last_os_error());
    }
    // SAFETY: GetFileInformationByHandle returned success, initializing `info`.
    let info = unsafe { info.assume_init() };
    Ok((
        info.dwVolumeSerialNumber,
        (u64::from(info.nFileIndexHigh) << 32) | u64::from(info.nFileIndexLow),
    ))
}

impl LockedDirectory {
    pub const LOCKFILE_NAME: &'static str = "feldera.pidlock";

    /// Attempts to create a new pidfile in the `base_path` directory, returning
    /// an error if the file was already created by a different process (and
    /// that process is still alive), blocking as long as `patience` to wait for
    /// an existing process to release the lock.
    ///
    /// # Arguments
    /// - `base_path`: The directory in which to create the pidfile. It must
    ///   already exist.
    ///
    /// # Panics
    /// - If the current process's PID cannot be determined.
    pub fn new_blocking<P: AsRef<Path>>(
        base_path: P,
        patience: Duration,
    ) -> Result<LockedDirectory, StorageError> {
        let base = base_path.as_ref().to_path_buf();
        let pid_file = base.join(LockedDirectory::LOCKFILE_NAME);
        let start = Instant::now();
        let mut blocked = false;
        loop {
            // Did we already lock it?
            let mut locks = LOCKS.lock().unwrap();
            #[cfg(unix)]
            match fs::metadata(&pid_file) {
                Ok(metadata) => {
                    let dev_ino = (metadata.dev(), metadata.ino());
                    if locks.contains(&dev_ino) {
                        return Err(StorageError::StorageLocked(process::id(), base));
                    }
                }
                Err(error) if error.kind() == ErrorKind::NotFound => (),
                Err(error) => {
                    return Err(StorageError::stdio(
                        error.kind(),
                        "stat",
                        pid_file.display(),
                    ));
                }
            }

            let file = File::options()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&pid_file)
                .map_err(|e| StorageError::stdio(e.kind(), "create", pid_file.display()))?;
            #[cfg(unix)]
            let metadata = file
                .metadata()
                .map_err(|e| StorageError::stdio(e.kind(), "fstat", pid_file.display()))?;
            #[cfg(unix)]
            let lock_id = (metadata.dev(), metadata.ino());
            #[cfg(windows)]
            let lock_id = lock_id(&file)
                .map_err(|e| StorageError::stdio(e.kind(), "fstat", pid_file.display()))?;

            #[cfg(windows)]
            if locks.contains(&lock_id) {
                return Err(StorageError::StorageLocked(process::id(), base));
            }

            match write_lock(&file) {
                Err(error)
                    if error.kind() == ErrorKind::PermissionDenied
                        || error.kind() == ErrorKind::WouldBlock =>
                {
                    let pid = get_lock(&file).unwrap_or(None).unwrap_or(0);
                    if start.elapsed() >= patience {
                        if blocked {
                            error!(
                                "{}: gave up waiting for process {pid} to release lock after {:.1} seconds",
                                pid_file.display(),
                                start.elapsed().as_secs_f64()
                            );
                        }
                        return Err(StorageError::StorageLocked(pid, base));
                    }
                    if !blocked {
                        info!(
                            "{}: waiting up to {:.1} seconds for process {pid} to release lock",
                            pid_file.display(),
                            patience.as_secs_f64(),
                        );
                        blocked = true;
                    }
                    sleep(Duration::from_millis(100));
                }
                Err(error) => {
                    return Err(StorageError::stdio(
                        error.kind(),
                        "exclusive lock",
                        pid_file.display(),
                    ));
                }
                Ok(()) => {
                    if blocked {
                        info!(
                            "{}: acquired lock after {:.1} seconds",
                            pid_file.display(),
                            start.elapsed().as_secs_f64()
                        );
                    }
                    locks.insert(lock_id);
                    return Ok(Self {
                        base,
                        _file: file,
                        lock_id,
                    });
                }
            };
        }
    }

    pub fn new<P: AsRef<Path>>(base_path: P) -> Result<LockedDirectory, StorageError> {
        Self::new_blocking(base_path, Duration::ZERO)
    }

    /// Returns the path to the directory in which the pidfile was created.
    pub fn base(&self) -> &Path {
        self.base.as_path()
    }
}
