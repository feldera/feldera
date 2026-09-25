//! [StorageBackend] implementation using POSIX I/O.

use super::{
    BlockLocation, FileId, FileReader, FileRw, FileWriter, MUTABLE_EXTENSION, StorageCacheFlags,
    StorageError,
};
use crate::Runtime;
use crate::circuit::metrics::{FILES_CREATED, FILES_DELETED, FILES_SYNCED};
use crate::profile::{BlockingFor, ParkReason};
use crate::storage::{buffer_cache::FBuf, init};
use feldera_storage::metrics::{
    READ_BLOCKS_BYTES, READ_LATENCY_MICROSECONDS, SYNC_LATENCY_MICROSECONDS, WRITE_BLOCKS_BYTES,
    WRITE_LATENCY_MICROSECONDS,
};
use feldera_storage::tokio::TOKIO;
use feldera_storage::{
    FileCommitter, StorageBackend, StorageBackendFactory, StorageFileType, StoragePath,
    StoragePathPart, append_to_path, default_read_async,
};
use feldera_types::config::{
    FileBackendConfig, StorageBackendConfig, StorageCacheConfig, StorageConfig, StorageSyncMode,
};
use std::fmt::{self, Debug, Display, Formatter};
use std::fs::{DirEntry, create_dir_all};
use std::io::{ErrorKind, IoSlice, Write};
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::thread::{scope, sleep};
use std::time::{Duration, Instant};
use std::{
    fs::{self, File, OpenOptions},
    io::Error as IoError,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicI64, Ordering},
    },
};
use tracing::{debug, info, warn};

/// fsync the file `file`, named `path` for error reporting.
///
/// This is the only place that syncs a file, and only [FileCommitter::commit]
/// calls it, which is what keeps the sync off the threads that write files.
///
/// `files_synced_total` and `storage_sync_latency_seconds` therefore count
/// per-file fsyncs only. Under [SyncStrategy::Syncfs] a checkpoint's files
/// never come through here, so both stay near zero and
/// `storage_commit_all_latency_seconds` is the one that moves.
fn sync_file(file: &File, path: &Path) -> Result<(), StorageError> {
    SYNC_LATENCY_MICROSECONDS.record_callback(|| {
        let _blocked = BlockingFor::new(ParkReason::StorageSync);
        file.sync_all()
            .map_err(|e| StorageError::stdio(e.kind(), "fsync", path.display()))?;
        FILES_SYNCED.fetch_add(1, Ordering::Relaxed);
        Ok(())
    })
}

/// How many fsyncs [commit_in_parallel] keeps in flight.
///
/// An fsync blocks rather than burning CPU, and jbd2 merges the ones that
/// overlap into a single journal commit, so the useful figure is set by how
/// many commits we want to collapse rather than by the core count.
const CONCURRENT_SYNCS: usize = 16;

/// Everything `syncfs` needs.
///
/// `syncfs` is a Linux system call, and deciding whether it is safe to use
/// reads Linux-specific files, so all of it lives here behind one `cfg` rather
/// than as a dozen scattered ones. The module off Linux below answers for the
/// same calls.
#[cfg(target_os = "linux")]
mod syncfs {
    use super::{StorageError, SyncStrategy, SyncfsObstacle};
    use std::fs::{self, File, create_dir_all};
    use std::path::{Path, PathBuf};
    use tracing::warn;

    /// Where `/proc/self/mountinfo` lives. A constant so that tests can hand
    /// [mount_is_exclusive] a copy from somewhere else.
    const MOUNTINFO: &str = "/proc/self/mountinfo";

    /// As much of one `/proc/self/mountinfo` line as this decision needs.
    struct MountEntry {
        /// `major:minor` of the filesystem, shared by every mount of it.
        device: String,

        /// The subtree of the filesystem that was mounted: `/` for a whole
        /// filesystem, a path for a bind mount of part of one.
        root: String,

        /// Where it is mounted.
        mount_point: PathBuf,
    }

    /// Decodes the octal escapes `mountinfo` uses for space, tab, newline and
    /// backslash in paths.
    pub(super) fn unescape_mountinfo(field: &str) -> String {
        let mut out = String::with_capacity(field.len());
        let mut rest = field;
        while let Some(index) = rest.find('\\') {
            out.push_str(&rest[..index]);
            let escape = &rest[index..];
            let (decoded, width) = match escape.get(..4) {
                Some("\\040") => (' ', 4),
                Some("\\011") => ('\t', 4),
                Some("\\012") => ('\n', 4),
                Some("\\134") => ('\\', 4),
                _ => ('\\', 1),
            };
            out.push(decoded);
            rest = &escape[width..];
        }
        out.push_str(rest);
        out
    }

    /// Parses the `mountinfo` fields that [mount_is_exclusive] reads.
    ///
    /// Only the first six fields are fixed, with optional ones following until a
    /// `-` separator. Everything needed here comes before that, so the variable
    /// part can be ignored.
    fn parse_mountinfo(mountinfo: &str) -> Vec<MountEntry> {
        mountinfo
            .lines()
            .filter_map(|line| {
                let mut fields = line.split(' ');
                let device = fields.nth(2)?;
                let root = fields.next()?;
                let mount_point = fields.next()?;
                Some(MountEntry {
                    device: device.to_string(),
                    root: unescape_mountinfo(root),
                    mount_point: PathBuf::from(unescape_mountinfo(mount_point)),
                })
            })
            .collect()
    }

    /// Whether a filesystem is mounted at `base` and holds nothing else.
    ///
    /// This decides whether `syncfs` on `base` would write back data belonging to
    /// anything else. It takes `mountinfo` as text so that it can be tested against
    /// real layouts.
    ///
    /// Comparing device numbers with the parent directory is not enough, because a
    /// bind mount has a device of its own and containers bind mount constantly. A
    /// Kubernetes `subPath`, or a `hostPath` into a shared host directory, is
    /// indistinguishable that way from a volume of our own while sharing a
    /// filesystem with everything else on it. Three things disqualify a mount here,
    /// each one a case that occurs:
    ///
    /// * A mount root other than `/` means only part of the filesystem was mounted
    ///   here, which is exactly what those bind mounts look like.
    /// * The same device mounted more than once means the filesystem is reachable
    ///   elsewhere too, as it is for btrfs subvolumes and repeated bind mounts.
    /// * The root filesystem holds the operating system, so it is never ours
    ///   alone, however few times it is mounted.
    ///
    /// What this cannot see is a filesystem shared through another mount namespace,
    /// such as one read-write-many volume mounted by several pods. Only
    /// [StorageSyncMode] covers that.
    pub(super) fn mount_is_exclusive(mountinfo: &str, base: &Path) -> bool {
        let entries = parse_mountinfo(mountinfo);

        // A later mount at the same path shadows an earlier one, so take the last.
        let Some(mount) = entries.iter().rfind(|entry| entry.mount_point == base) else {
            // Nothing is mounted here, so `base` is a directory inside a filesystem
            // that holds more than it.
            return false;
        };

        // Every mount of one filesystem reports that filesystem's major:minor, so a
        // second entry carrying this device (common in practice for btrfs subtree
        // mounts) is that same filesystem reachable by another path. syncfs takes
        // the filesystem, not the mount, and would flush whatever is written
        // through that other path too.
        let mounted_once = entries
            .iter()
            .filter(|entry| entry.device == mount.device)
            .count()
            == 1;

        mount.root == "/" && mount.mount_point != Path::new("/") && mounted_once
    }

    /// Whether `base` has a filesystem to itself.
    ///
    /// `base` is resolved first, because `mountinfo` names mount points by their
    /// real paths. A `base` that does not exist answers false, which is right
    /// rather than merely safe: nothing can be mounted on a path that is not there.
    pub(super) fn has_own_filesystem(base: &Path) -> bool {
        let Ok(base) = base.canonicalize() else {
            return false;
        };
        fs::read_to_string(MOUNTINFO).is_ok_and(|mountinfo| mount_is_exclusive(&mountinfo, &base))
    }

    /// Whether the kernel release string `release` names Linux 5.8 or later.
    pub(super) fn release_reports_syncfs_errors(release: &str) -> bool {
        let mut numbers = release
            .split(|c: char| !c.is_ascii_digit())
            .filter(|field| !field.is_empty())
            .map(|field| field.parse::<u32>().unwrap_or(0));
        let (major, minor) = (numbers.next().unwrap_or(0), numbers.next().unwrap_or(0));
        (major, minor) >= (5, 8)
    }

    /// What stops this kernel from using `syncfs`, regardless of where storage is.
    ///
    /// An unreadable or unparsable release counts as an obstacle, so an
    /// unrecognized kernel takes the safe path rather than the fast one.
    pub(super) fn platform_obstacle() -> Option<SyncfsObstacle> {
        let Ok(utsname) = nix::sys::utsname::uname() else {
            return Some(SyncfsObstacle::KernelUnknown);
        };
        match utsname.release().to_str() {
            Some(release) if release_reports_syncfs_errors(release) => None,
            Some(release) => Some(SyncfsObstacle::KernelTooOld(release.to_string())),
            None => Some(SyncfsObstacle::KernelUnknown),
        }
    }

    /// Opens `base` for `syncfs`, or returns `None` with the reason logged.
    pub(super) fn open(base: &Path) -> Option<SyncStrategy> {
        // Only this path needs the directory to exist, and only because syncfs
        // takes a descriptor. Creating it here rather than leaving it to the first
        // write keeps a configured syncfs working on the very first run, instead of
        // falling back until some later start finds the directory already there.
        match create_dir_all(base).and_then(|()| File::open(base)) {
            Ok(dir) => Some(SyncStrategy::Syncfs(dir)),
            Err(error) => {
                warn!(
                    "{}: committing checkpoints one file at a time: \
                     the storage directory could not be opened for syncfs ({error})",
                    base.display()
                );
                None
            }
        }
    }

    /// syncfs the filesystem holding `dir`, named `path` for error reporting.
    pub(super) fn sync(dir: &File, path: &Path) -> Result<(), StorageError> {
        use std::io::Error as StdIoError;
        use std::os::fd::AsRawFd;

        nix::unistd::syncfs(dir.as_raw_fd()).map_err(|errno| {
            StorageError::stdio(
                StdIoError::from_raw_os_error(errno as i32).kind(),
                "syncfs",
                path.display(),
            )
        })
    }
}

/// Stands in for [syncfs] where the system call does not exist.
#[cfg(not(target_os = "linux"))]
mod syncfs {
    use super::{SyncStrategy, SyncfsObstacle};
    use std::path::Path;

    /// Always an obstacle: there is no `syncfs` to call.
    pub(super) fn platform_obstacle() -> Option<SyncfsObstacle> {
        Some(SyncfsObstacle::Unsupported)
    }

    /// Never consulted, because [platform_obstacle] has already refused.
    pub(super) fn has_own_filesystem(_base: &Path) -> bool {
        false
    }

    /// Never reached, for the same reason.
    pub(super) fn open(_base: &Path) -> Option<SyncStrategy> {
        None
    }
}

/// What stops storage at `base` from using `syncfs`.
///
/// Each variant is also the explanation logged for the choice, because a
/// pipeline that quietly settles on the slow strategy looks exactly like one
/// that chose the fast strategy and got no benefit from it.
///
/// Which variants can occur depends on the platform, hence the blanket allow:
/// `Unsupported` is the only one off Linux, and never happens on it.
#[derive(Debug)]
#[allow(dead_code)]
enum SyncfsObstacle {
    /// `syncfs` is a Linux system call and this is not Linux.
    Unsupported,

    /// Before Linux 5.8, `syncfs` returned success unconditionally and
    /// discarded writeback errors.
    KernelTooOld(String),

    /// The kernel release could not be read, so 5.8 cannot be ruled in.
    KernelUnknown,

    /// `base` is not a mount point, so its filesystem holds more than this
    /// pipeline and `syncfs` would write back that too.
    NotAMountPoint,
}

impl SyncfsObstacle {
    /// Whether [StorageSyncMode::Syncfs] may go ahead despite this obstacle.
    ///
    /// Sharing a filesystem only costs the writeback of data that is not ours,
    /// a tradeoff an operator is entitled to make. The other obstacles decide
    /// whether `syncfs` reports a failed writeback at all: before Linux 5.8 it
    /// returned success and discarded the error, so [PosixBackend::commit_all]
    /// would call a checkpoint durable whose data never reached the device and
    /// `publish` would then enter it in the catalog. No setting may ask for
    /// that.
    fn is_overridable(&self) -> bool {
        matches!(self, Self::NotAMountPoint)
    }
}

impl Display for SyncfsObstacle {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unsupported => {
                write!(f, "syncfs is a Linux system call and this is not Linux")
            }
            Self::KernelTooOld(release) => write!(
                f,
                "Linux {release} predates 5.8, which is where syncfs started reporting \
                 writeback errors instead of discarding them"
            ),
            Self::KernelUnknown => write!(
                f,
                "the kernel release is unreadable, and syncfs discarded writeback errors \
                 before Linux 5.8"
            ),
            Self::NotAMountPoint => write!(
                f,
                "storage is not a mount point, so syncfs would also write back whatever \
                 else shares its filesystem"
            ),
        }
    }
}

/// What stops storage at `base` from using `syncfs`, or `None` if nothing does.
fn syncfs_obstacle(base: &Path) -> Option<SyncfsObstacle> {
    syncfs::platform_obstacle()
        .or_else(|| (!syncfs::has_own_filesystem(base)).then_some(SyncfsObstacle::NotAMountPoint))
}

/// How [PosixBackend::commit_all] makes a checkpoint's files durable.
enum SyncStrategy {
    /// One `syncfs` on the open directory, covering every file at once.
    #[cfg(target_os = "linux")]
    Syncfs(File),

    /// An fsync per file, [CONCURRENT_SYNCS] at a time.
    PerFile,
}

impl SyncStrategy {
    /// Whether this strategy commits with one `syncfs`.
    #[cfg(test)]
    fn is_syncfs(&self) -> bool {
        #[cfg(target_os = "linux")]
        return matches!(self, Self::Syncfs(_));
        #[cfg(not(target_os = "linux"))]
        false
    }

    /// Chooses a strategy for storage at `base`, honoring `mode` as far as
    /// [SyncfsObstacle::is_overridable] allows, and logs both the choice and
    /// the reason for it.
    fn new(mode: StorageSyncMode, base: &Path) -> Self {
        let path = base.display();

        if mode == StorageSyncMode::PerFile {
            info!("{path}: committing checkpoints one file at a time, as configured");
            return Self::PerFile;
        }

        match &syncfs_obstacle(base) {
            // Configured explicitly, and the obstacle is one the operator is
            // free to accept. Name it anyway, because accepting it costs the
            // writeback of data that is not ours.
            Some(obstacle) if mode == StorageSyncMode::Syncfs && obstacle.is_overridable() => {
                warn!("{path}: syncfs requested despite {obstacle}");
            }

            // Either `auto` declining, or `syncfs` asking for what no setting
            // may have: see [SyncfsObstacle::is_overridable].
            Some(obstacle) => {
                if mode == StorageSyncMode::Syncfs {
                    warn!("{path}: syncfs requested but not usable: {obstacle}");
                }
                info!("{path}: committing checkpoints one file at a time: {obstacle}");
                return Self::PerFile;
            }

            None => (),
        }

        match syncfs::open(base) {
            Some(strategy) => {
                info!("{path}: committing checkpoints with one syncfs");
                strategy
            }
            None => Self::PerFile,
        }
    }
}

/// Commits `files`, [CONCURRENT_SYNCS] at a time, and returns the first error.
///
/// Overlapping the fsyncs is the point: the kernel collapses concurrent ones
/// into shared journal commits, which a sequential loop never gives it a chance
/// to do.
fn commit_in_parallel(files: &[Arc<dyn FileCommitter>]) -> Result<(), StorageError> {
    let threads = CONCURRENT_SYNCS.min(files.len());
    if threads <= 1 {
        return files.iter().try_for_each(|file| file.commit());
    }

    let next = AtomicUsize::new(0);
    let failed = AtomicBool::new(false);
    let failure: Mutex<Option<StorageError>> = Mutex::new(None);
    scope(|scope| {
        for _ in 0..threads {
            scope.spawn(|| {
                // The flag, not the mutex, ends the loop: this runs once per
                // file per worker, and only the first error needs the lock.
                while !failed.load(Ordering::Relaxed) {
                    let index = next.fetch_add(1, Ordering::Relaxed);
                    let Some(file) = files.get(index) else { break };
                    if let Err(error) = file.commit() {
                        failure.lock().unwrap().get_or_insert(error);
                        failed.store(true, Ordering::Relaxed);
                        break;
                    }
                }
            });
        }
    });
    match failure.into_inner().unwrap() {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

/// fsync the directory at `path` so a freshly-created child entry (a
/// rename target or a new subdirectory) becomes durable. Without this,
/// POSIX gives no guarantee that the directory entry survives a crash
/// even if the child file itself has been fully fsynced.
fn fsync_dir(path: &Path) -> Result<(), StorageError> {
    let dir = {
        let _blocked = BlockingFor::new(ParkReason::StorageMetadata);
        File::open(path)
            .map_err(|e| StorageError::stdio(e.kind(), "open dir for fsync", path.display()))?
    };
    let _blocked = BlockingFor::new(ParkReason::StorageSync);
    dir.sync_all()
        .map_err(|e| StorageError::stdio(e.kind(), "fsync dir", path.display()))
}

pub(super) struct PosixReader {
    path: StoragePath,
    file: Arc<File>,
    file_id: FileId,
    drop: DeleteOnDrop,

    /// Whether to use background threads for file I/O.
    async_threads: bool,

    /// Per-I/O operation sleep delay, for simulating slow storage devices.
    ioop_delay: Duration,
}

impl Debug for PosixReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PosixReader({})", self.path)
    }
}

impl PosixReader {
    fn new(
        path: StoragePath,
        file: Arc<File>,
        file_id: FileId,
        drop: DeleteOnDrop,
        async_threads: bool,
        ioop_delay: Duration,
    ) -> Self {
        Self {
            path,
            file,
            file_id,
            drop,
            async_threads,
            ioop_delay,
        }
    }
    fn open(
        path: StoragePath,
        file_name: PathBuf,
        cache: StorageCacheConfig,
        usage: Arc<AtomicI64>,
        async_threads: bool,
        ioop_delay: Duration,
    ) -> Result<Arc<dyn FileReader>, StorageError> {
        let _blocked = BlockingFor::new(ParkReason::StorageMetadata);
        let file = OpenOptions::new()
            .read(true)
            .cache_flags(&cache)
            .open(&file_name)
            .map_err(|e| StorageError::stdio(e.kind(), "open", file_name.display()))?;
        let size = file
            .metadata()
            .map_err(|e| StorageError::stdio(e.kind(), "fstat", file_name.display()))?
            .size();

        Ok(Arc::new(Self::new(
            path,
            Arc::new(file),
            FileId::new(),
            DeleteOnDrop::new(file_name, true, size, usage),
            async_threads,
            ioop_delay,
        )))
    }
}

impl FileRw for PosixReader {
    fn file_id(&self) -> FileId {
        self.file_id
    }
    fn path(&self) -> &StoragePath {
        &self.path
    }
}

impl FileCommitter for PosixReader {
    fn commit(&self) -> Result<(), StorageError> {
        sync_file(&self.file, &self.drop.path)
    }
}

impl FileReader for PosixReader {
    fn mark_for_checkpoint(&self) {
        self.drop.keep();
    }

    fn read_block(&self, location: BlockLocation) -> Result<Arc<FBuf>, StorageError> {
        READ_BLOCKS_BYTES.record(location.size);
        READ_LATENCY_MICROSECONDS.record_callback(|| {
            let mut buffer = FBuf::with_capacity(location.size);

            let _blocked = BlockingFor::new(ParkReason::StorageRead);
            sleep(self.ioop_delay);
            match buffer.read_exact_at(&self.file, location.offset, location.size) {
                Ok(()) => Ok(Arc::new(buffer)),
                Err(e) => Err(StorageError::stdio(
                    e.kind(),
                    "read",
                    self.drop.path.display(),
                )),
            }
        })
    }

    fn read_async(
        &self,
        blocks: Vec<BlockLocation>,
        callback: Box<dyn FnOnce(Vec<Result<Arc<FBuf>, StorageError>>) + Send>,
    ) {
        if self.async_threads {
            let file = self.file.clone();
            let ioop_delay = self.ioop_delay;
            let start = Instant::now();
            TOKIO.spawn_blocking(move || {
                // For background reads, we only sleep once, not once per block.
                sleep(ioop_delay);
                let blocks = blocks
                    .into_iter()
                    .map(|location| {
                        READ_BLOCKS_BYTES.record(location.size);
                        let mut buffer = FBuf::with_capacity(location.size);
                        match buffer.read_exact_at(&file, location.offset, location.size) {
                            Ok(()) => Ok(Arc::new(buffer)),
                            Err(e) => Err(StorageError::StdIo {
                                kind: e.kind(),
                                operation: "async read",
                                path: None,
                            }),
                        }
                    })
                    .collect();
                READ_LATENCY_MICROSECONDS.record_elapsed(start);
                callback(blocks);
            });
        } else {
            // This will call back into [Self::read_block], so don't measure the
            // latency directly (that would double-count).
            default_read_async(self, blocks, callback);
        }
    }

    fn get_size(&self) -> Result<u64, StorageError> {
        Ok(self.drop.size)
    }
}

/// Deletes a file when dropped (unless [Self::keep] is called first).
pub struct DeleteOnDrop {
    path: PathBuf,
    keep: AtomicBool,
    size: u64,
    usage: Arc<AtomicI64>,
}

impl Drop for DeleteOnDrop {
    fn drop(&mut self) {
        if !self.keep.load(Ordering::Relaxed) {
            let _blocked = BlockingFor::new(ParkReason::StorageMetadata);
            if let Err(e) = fs::remove_file(&self.path) {
                warn!(
                    "{}: unable to delete dropped file: {e}",
                    self.path.display(),
                );
            } else {
                self.usage.fetch_sub(self.size as i64, Ordering::Relaxed);
                FILES_DELETED.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

impl DeleteOnDrop {
    fn new(path: PathBuf, keep: bool, size: u64, usage: Arc<AtomicI64>) -> Self {
        Self {
            path,
            keep: AtomicBool::new(keep),
            size,
            usage,
        }
    }

    /// Disables deleting the file when dropped.
    pub fn keep(&self) {
        self.keep.store(true, Ordering::Relaxed);
    }
    fn with_path(mut self, path: PathBuf) -> Self {
        self.path = path;
        self
    }
}

/// Meta-data we keep per file we created.
struct PosixWriter {
    file_id: FileId,
    file: File,
    drop: DeleteOnDrop,
    name: StoragePath,

    buffers: Vec<Arc<FBuf>>,
    len: u64,

    async_threads: bool,
    ioop_delay: Duration,
}

impl FileRw for PosixWriter {
    fn file_id(&self) -> FileId {
        self.file_id
    }

    fn path(&self) -> &StoragePath {
        &self.name
    }
}

impl FileWriter for PosixWriter {
    fn write_block(&mut self, data: FBuf) -> Result<Arc<FBuf>, StorageError> {
        let block = Arc::new(data);
        self.write(&block)?;
        Ok(block)
    }

    fn complete(mut self: Box<Self>) -> Result<Arc<dyn FileReader>, StorageError> {
        if !self.buffers.is_empty() {
            self.flush()?;
        }

        // Remove the .mut extension from the file.
        let finalized_path = self.drop.path.with_extension("");
        {
            let _blocked = BlockingFor::new(ParkReason::StorageMetadata);
            self.drop.usage.fetch_sub(
                finalized_path
                    .metadata()
                    .map_or(0, |metadata| metadata.size() as i64),
                Ordering::Relaxed,
            );
            fs::rename(&self.drop.path, &finalized_path)
                .map_err(|e| StorageError::stdio(e.kind(), "rename", self.drop.path.display()))?;
        }

        Ok(Arc::new(PosixReader::new(
            self.name,
            Arc::new(self.file),
            self.file_id,
            self.drop.with_path(finalized_path),
            self.async_threads,
            self.ioop_delay,
        )) as Arc<dyn FileReader>)
    }
}

impl PosixWriter {
    fn new(
        file: File,
        name: StoragePath,
        path: PathBuf,
        usage: Arc<AtomicI64>,
        async_threads: bool,
        ioop_delay: Duration,
    ) -> Self {
        Self {
            file_id: FileId::new(),
            file,
            name,
            drop: DeleteOnDrop::new(path, false, 0, usage),
            buffers: Vec::new(),
            len: 0,
            async_threads,
            ioop_delay,
        }
    }

    fn flush(&mut self) -> Result<(), StorageError> {
        WRITE_LATENCY_MICROSECONDS.record_callback(|| {
            if let Some(storage_mb_max) = Runtime::with_dev_tweaks(|tweaks| tweaks.storage_mb_max) {
                let usage_mb = (self.drop.usage.load(Ordering::Relaxed) / 1024 / 1024)
                    .max(0)
                    .cast_unsigned();
                if usage_mb > storage_mb_max {
                    return Err(StorageError::stdio(
                        ErrorKind::StorageFull,
                        "write",
                        self.drop.path.display(),
                    ));
                }
            }

            let mut bufs = self
                .buffers
                .iter()
                .map(|buf| IoSlice::new(buf.as_slice()))
                .collect::<Vec<_>>();
            let mut cursor = bufs.as_mut_slice();
            let _blocked = BlockingFor::new(ParkReason::StorageWrite);
            sleep(self.ioop_delay);
            while !cursor.is_empty() {
                let n = self.file.write_vectored(cursor).map_err(|e| {
                    StorageError::stdio(e.kind(), "write", self.drop.path.display())
                })?;
                WRITE_BLOCKS_BYTES.record(n);
                self.drop.size += n as u64;
                self.drop.usage.fetch_add(n as i64, Ordering::Relaxed);
                IoSlice::advance_slices(&mut cursor, n);
            }
            self.buffers.clear();
            Ok(())
        })
    }

    fn write(&mut self, buffer: &Arc<FBuf>) -> Result<(), StorageError> {
        if self.len >= 1024 * 1024 {
            self.flush()?;
        }
        self.len += buffer.len() as u64;
        self.buffers.push(buffer.clone());
        Ok(())
    }
}

/// State of the backend needed to satisfy the storage APIs.
pub struct PosixBackend {
    /// Directory in which we keep the files.
    base: Arc<PathBuf>,

    /// Cache configuration.
    cache: StorageCacheConfig,

    /// Usage.
    usage: Arc<AtomicI64>,

    /// Whether to use background threads for file I/O.
    async_threads: bool,

    /// Per-I/O operation sleep delay, for simulating slow storage devices.
    ioop_delay: Duration,

    /// How to make a checkpoint's files durable.
    sync_strategy: SyncStrategy,
}

impl PosixBackend {
    /// Instantiates a new backend.
    ///
    /// ## Parameters
    /// - `base`: Directory in which we keep the files.
    ///   shared among all instances of the backend.
    pub fn new<P: AsRef<Path>>(
        base: P,
        cache: StorageCacheConfig,
        options: &FileBackendConfig,
    ) -> Self {
        init();
        let base = base.as_ref().to_path_buf();
        let sync_strategy = SyncStrategy::new(options.sync_mode.unwrap_or_default(), &base);
        Self {
            base: Arc::new(base),
            cache,
            usage: Arc::new(AtomicI64::new(0)),
            async_threads: options.async_threads.unwrap_or(true),
            ioop_delay: options.ioop_latency.unwrap_or_default().as_std(),
            sync_strategy,
        }
    }

    /// Returns the directory in which the backend creates files.
    pub fn path(&self) -> &Path {
        self.base.as_path()
    }

    /// Returns the filesystem path to `name` in this storage.
    fn fs_path(&self, name: &StoragePath) -> PathBuf {
        self.base.join(name.as_ref())
    }

    fn remove_dir_all(&self, path: &Path) -> Result<(), IoError> {
        let _blocked = BlockingFor::new(ParkReason::StorageMetadata);
        let file_type = fs::symlink_metadata(path)?.file_type();
        if file_type.is_symlink() {
            fs::remove_file(path)
        } else {
            self.remove_dir_all_recursive(path)
        }
    }

    fn remove_dir_all_recursive(&self, path: &Path) -> Result<(), IoError> {
        fn ignore_notfound(result: Result<(), IoError>) -> Result<(), IoError> {
            match result {
                Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
                _ => result,
            }
        }

        for child in fs::read_dir(path)? {
            let child = child?;
            let path = child.path();
            let result = child.file_type().and_then(|file_type| {
                if file_type.is_dir() {
                    self.remove_dir_all_recursive(&path)
                } else if file_type.is_file() {
                    let size = child.metadata().map_or(0, |metadata| metadata.size());
                    fs::remove_file(&path).inspect(|_| {
                        self.usage.fetch_sub(size as i64, Ordering::Relaxed);
                    })
                } else {
                    fs::remove_file(&path)
                }
            });
            ignore_notfound(result)?;
        }
        ignore_notfound(fs::remove_dir(path))
    }
}

impl StorageBackend for PosixBackend {
    fn create_named(&self, name: &StoragePath) -> Result<Box<dyn FileWriter>, StorageError> {
        fn try_create_named(this: &PosixBackend, path: &Path) -> Result<File, IoError> {
            OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .read(true)
                .cache_flags(&this.cache)
                .open(path)
        }

        let path = append_to_path(self.fs_path(name), MUTABLE_EXTENSION);
        let _blocked = BlockingFor::new(ParkReason::StorageMetadata);
        let file = match try_create_named(self, &path) {
            Err(error) if error.kind() == ErrorKind::NotFound => {
                if let Some(parent) = path.parent() {
                    create_dir_all(parent).map_err(|e| {
                        StorageError::stdio(e.kind(), "recursive mkdir", path.display())
                    })?;
                }
                try_create_named(self, &path)
            }
            other => other,
        }
        .map_err(|e| StorageError::stdio(e.kind(), "create", path.display()))?;
        FILES_CREATED.fetch_add(1, Ordering::Relaxed);
        Ok(Box::new(PosixWriter::new(
            file,
            name.clone(),
            path,
            self.usage.clone(),
            self.async_threads,
            self.ioop_delay,
        )))
    }

    fn open(&self, name: &StoragePath) -> Result<Arc<dyn FileReader>, StorageError> {
        PosixReader::open(
            name.clone(),
            self.fs_path(name),
            self.cache,
            self.usage.clone(),
            self.async_threads,
            self.ioop_delay,
        )
    }

    fn list(
        &self,
        parent: &StoragePath,
        cb: &mut dyn FnMut(feldera_storage::DirEntry),
    ) -> Result<(), StorageError> {
        fn get_file_type(entry: &DirEntry) -> Result<StorageFileType, StorageError> {
            let _blocked = BlockingFor::new(ParkReason::StorageMetadata);
            let file_type = entry.file_type().map_err(|e| {
                StorageError::stdio(e.kind(), "readdir type", entry.path().display())
            })?;
            let file_type = if file_type.is_file() {
                StorageFileType::File {
                    size: entry
                        .metadata()
                        .map_err(|e| {
                            StorageError::stdio(e.kind(), "readdir fstat", entry.path().display())
                        })?
                        .size(),
                }
            } else if file_type.is_dir() {
                StorageFileType::Directory
            } else {
                StorageFileType::Other
            };
            Ok(file_type)
        }

        let mut result = Ok(());
        let path = self.fs_path(parent);
        let entries = {
            let _blocked = BlockingFor::new(ParkReason::StorageMetadata);
            path.read_dir().map_err(|e| {
                StorageError::stdio(e.kind(), "readdir", self.fs_path(parent).display())
            })?
        };
        let mut warnings = 0usize..20;
        for entry in entries {
            match entry {
                Ok(entry) => {
                    let entry = feldera_storage::DirEntry {
                        name: parent
                            .clone()
                            .join(StoragePathPart::from(entry.file_name().as_encoded_bytes())),
                        file_type: get_file_type(&entry),
                    };
                    if let Err(e) = &entry.file_type
                        && e.kind() == ErrorKind::NotFound
                    {
                        // Ignore NotFound error.  The file was probably
                        // `status.json.mut`, renamed by the adapters server to
                        // `status.json` between the call to readdir and the
                        // call to stat.  Don't succumb to a race for it.
                    } else {
                        if let Err(e) = &entry.file_type {
                            match warnings.next_back() {
                                Some(1..) => warn!("I/O error listing {parent}: {e}"),
                                Some(0) => warn!(
                                    "I/O error listing {parent} (further warnings will be at debug level): {e}"
                                ),
                                None => debug!("I/O error listing {parent}: {e}"),
                            }
                        }
                        cb(entry);
                    }
                }
                Err(error) => {
                    result = Err(StorageError::stdio(
                        error.kind(),
                        "readdir entry",
                        path.display(),
                    ));
                }
            }
        }
        result
    }

    fn delete(&self, name: &StoragePath) -> Result<(), StorageError> {
        let path = self.fs_path(name);
        let _blocked = BlockingFor::new(ParkReason::StorageMetadata);
        let metadata = fs::metadata(&path)
            .map_err(|e| StorageError::stdio(e.kind(), "stat", path.display()))?;
        fs::remove_file(&path)
            .map_err(|e| StorageError::stdio(e.kind(), "unlink", path.display()))?;
        if metadata.file_type().is_file() {
            self.usage
                .fetch_sub(metadata.size() as i64, Ordering::Relaxed);
        }
        Ok(())
    }

    fn delete_recursive(&self, name: &StoragePath) -> Result<(), StorageError> {
        let path = self.fs_path(name);
        match self.remove_dir_all(&path) {
            Err(error) if error.kind() == ErrorKind::NotFound => (),
            Err(error) if error.kind() == ErrorKind::NotADirectory => self.delete(name)?,
            Err(error) => {
                return Err(StorageError::stdio(
                    error.kind(),
                    "recursive delete",
                    path.display(),
                ));
            }
            Ok(()) => (),
        }
        Ok(())
    }

    fn usage(&self) -> Arc<AtomicI64> {
        self.usage.clone()
    }

    fn file_system_path(&self) -> Option<&Path> {
        Some(self.base.as_path())
    }

    fn fsync_dir(&self, dir: &StoragePath) -> Result<(), StorageError> {
        fsync_dir(&self.fs_path(dir))
    }

    fn sync_files(&self, files: &[Arc<dyn FileCommitter>]) -> Result<(), StorageError> {
        let _blocked = BlockingFor::new(ParkReason::StorageSync);
        match &self.sync_strategy {
            #[cfg(target_os = "linux")]
            SyncStrategy::Syncfs(dir) => syncfs::sync(dir, &self.base),
            SyncStrategy::PerFile => commit_in_parallel(files),
        }
    }
}

pub(crate) struct DefaultBackendFactory;
impl StorageBackendFactory for DefaultBackendFactory {
    fn backend(&self) -> &'static str {
        "default"
    }

    fn create(
        &self,
        storage_config: &StorageConfig,
        _backend_config: &StorageBackendConfig,
    ) -> Result<Arc<dyn StorageBackend>, StorageError> {
        Ok(Arc::new(PosixBackend::new(
            storage_config.path(),
            storage_config.cache,
            &FileBackendConfig::default(),
        )))
    }
}

inventory::submit! {
    &DefaultBackendFactory as &dyn StorageBackendFactory
}

pub(crate) struct FileBackendFactory;
impl StorageBackendFactory for FileBackendFactory {
    fn backend(&self) -> &'static str {
        "file"
    }

    fn create(
        &self,
        storage_config: &StorageConfig,
        backend_config: &StorageBackendConfig,
    ) -> Result<Arc<dyn StorageBackend>, StorageError> {
        let StorageBackendConfig::File(config) = &backend_config else {
            return Err(StorageError::InvalidBackendConfig {
                backend: self.backend().into(),
                config: Box::new(backend_config.clone()),
            });
        };
        Ok(Arc::new(PosixBackend::new(
            storage_config.path(),
            storage_config.cache,
            config,
        )))
    }
}

inventory::submit! {
    &FileBackendFactory as &dyn StorageBackendFactory
}

#[cfg(test)]
mod tests {
    use feldera_storage::{StorageBackend, StoragePath};
    use feldera_types::config::{FileBackendConfig, StorageCacheConfig, StorageSyncMode};
    use std::{path::Path, sync::Arc};

    use crate::storage::backend::tests::{random_sizes, test_backend};
    use crate::storage::buffer_cache::FBuf;

    #[cfg(target_os = "linux")]
    use super::syncfs::{mount_is_exclusive, unescape_mountinfo};
    use super::{
        CONCURRENT_SYNCS, PosixBackend, SyncStrategy, SyncfsObstacle, commit_in_parallel,
        syncfs::has_own_filesystem,
    };
    use super::{FileId, FileRw, StorageError};
    use feldera_storage::FileCommitter;
    use std::io::ErrorKind;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A committer that records how often it was committed, so a test can check
    /// the fan-out without reaching for a process-wide counter.
    #[derive(Debug)]
    struct CountingCommitter {
        file_id: FileId,
        path: StoragePath,
        commits: Arc<AtomicUsize>,
        fail: bool,
    }

    impl CountingCommitter {
        fn arc(commits: &Arc<AtomicUsize>, fail: bool) -> Arc<dyn FileCommitter> {
            Arc::new(Self {
                file_id: FileId::new(),
                path: StoragePath::from("counting"),
                commits: commits.clone(),
                fail,
            })
        }
    }

    impl FileRw for CountingCommitter {
        fn file_id(&self) -> FileId {
            self.file_id
        }
        fn path(&self) -> &StoragePath {
            &self.path
        }
    }

    impl FileCommitter for CountingCommitter {
        fn commit(&self) -> Result<(), StorageError> {
            self.commits.fetch_add(1, Ordering::Relaxed);
            if self.fail {
                Err(StorageError::stdio(ErrorKind::Other, "fsync", "counting"))
            } else {
                Ok(())
            }
        }
    }

    /// Linux 5.8 is where `syncfs` started reporting writeback errors, and an
    /// unrecognized release must read as older so it takes the safe path.
    #[test]
    #[cfg(target_os = "linux")]
    fn syncfs_error_reporting_by_release() {
        use super::syncfs::release_reports_syncfs_errors;

        for release in [
            "5.8",
            "5.8.0",
            "5.9.1-arch",
            "6.19.11-200.fc43.x86_64",
            "10.0.0",
        ] {
            assert!(
                release_reports_syncfs_errors(release),
                "{release} should report syncfs errors"
            );
        }
        for release in ["5.7.19", "4.18.0-553.el8", "2.6.32", "", "not-a-version"] {
            assert!(
                !release_reports_syncfs_errors(release),
                "{release} should not report syncfs errors"
            );
        }
    }

    /// The kernel this test runs on must be identified, not fall through to
    /// the unreadable-release default. That default is safe but silent, so
    /// hitting it would disable syncfs with nothing to show for it.
    #[test]
    #[cfg(target_os = "linux")]
    fn running_kernel_release_parses() {
        let utsname = nix::sys::utsname::uname().expect("uname must work");
        let release = utsname.release().to_str().expect("release must be UTF-8");
        let mut numbers = release
            .split(|c: char| !c.is_ascii_digit())
            .filter(|field| !field.is_empty())
            .map(|field| field.parse::<u32>().unwrap_or(0));
        let (major, minor) = (numbers.next().unwrap_or(0), numbers.next().unwrap_or(0));
        assert!(major >= 2, "unparsable kernel release {release:?}");

        let obstacle = super::syncfs::platform_obstacle();
        if (major, minor) >= (5, 8) {
            assert!(
                obstacle.is_none(),
                "kernel {release:?} wrongly blocked by {obstacle:?}"
            );
        } else {
            assert!(
                matches!(obstacle, Some(SyncfsObstacle::KernelTooOld(_))),
                "kernel {release:?} should be reported as too old, got {obstacle:?}"
            );
        }
    }

    /// Every obstacle must say which one it is, and the too-old one must name
    /// the kernel it found. Reading "syncfs is off" without the version leaves
    /// no way to tell a genuinely old kernel from a misparse.
    #[test]
    fn obstacles_explain_themselves() {
        let messages = [
            SyncfsObstacle::Unsupported.to_string(),
            SyncfsObstacle::KernelTooOld("4.18.0-553.el8.x86_64".to_string()).to_string(),
            SyncfsObstacle::KernelUnknown.to_string(),
            SyncfsObstacle::NotAMountPoint.to_string(),
        ];
        assert!(
            messages[1].contains("4.18.0-553.el8.x86_64"),
            "an old kernel must be named: {}",
            messages[1]
        );
        for (i, message) in messages.iter().enumerate() {
            assert!(!message.is_empty());
            assert!(
                !messages[..i].contains(message),
                "obstacles must read differently: {message}"
            );
        }
    }

    /// Mount layouts that do and do not give storage a filesystem to itself.
    ///
    /// The bind mount cases are the point: a container's storage path nearly
    /// always is one, and comparing device numbers against the parent
    /// directory calls every one of them exclusive.
    #[test]
    #[cfg(target_os = "linux")]
    fn exclusive_mounts_by_layout() {
        // A volume of our own: whole filesystem, mounted once.
        let dedicated = "\
25 1 259:1 / / rw,relatime shared:1 - ext4 /dev/nvme0n1p1 rw
88 25 259:3 / /data rw,relatime shared:2 - ext4 /dev/nvme1n1 rw";
        assert!(mount_is_exclusive(dedicated, Path::new("/data")));

        // The root filesystem is never ours alone.
        assert!(!mount_is_exclusive(dedicated, Path::new("/")));

        // Nothing mounted there: a directory inside a filesystem holding more.
        assert!(!mount_is_exclusive(dedicated, Path::new("/data/pipeline")));

        // Kubernetes subPath: part of a volume shared with other subPaths.
        let subpath = "\
25 1 259:1 / / rw,relatime shared:1 - ext4 /dev/nvme0n1p1 rw
88 25 259:3 /pipelines/p1 /data rw,relatime shared:2 - ext4 /dev/nvme1n1 rw";
        assert!(!mount_is_exclusive(subpath, Path::new("/data")));

        // hostPath or local-path: a host directory bound in, sharing the node's
        // filesystem with every other pod on it.
        let hostpath = "\
25 1 259:1 / / rw,relatime shared:1 - ext4 /dev/nvme0n1p1 rw
88 25 259:1 /opt/local-path-provisioner/pvc-abc /data rw,relatime - ext4 /dev/nvme0n1p1 rw";
        assert!(!mount_is_exclusive(hostpath, Path::new("/data")));

        // The same filesystem mounted twice, as btrfs subvolumes are.
        let subvolumes = "\
25 1 0:34 /root / rw,relatime - btrfs /dev/mapper/luks rw
48 25 0:34 /home /home rw,relatime - btrfs /dev/mapper/luks rw";
        assert!(!mount_is_exclusive(subvolumes, Path::new("/home")));

        // A whole filesystem, but reachable at a second path as well.
        let bound_twice = "\
25 1 259:1 / / rw,relatime - ext4 /dev/nvme0n1p1 rw
88 25 259:3 / /data rw,relatime - ext4 /dev/nvme1n1 rw
89 25 259:3 / /mnt/also-data rw,relatime - ext4 /dev/nvme1n1 rw";
        assert!(!mount_is_exclusive(bound_twice, Path::new("/data")));

        // A later mount shadows an earlier one at the same path.
        let shadowed = "\
25 1 259:1 / / rw,relatime - ext4 /dev/nvme0n1p1 rw
88 25 259:3 / /data rw,relatime - ext4 /dev/nvme1n1 rw
89 25 259:4 /sub /data rw,relatime - ext4 /dev/nvme2n1 rw";
        assert!(!mount_is_exclusive(shadowed, Path::new("/data")));
    }

    /// Mount points with awkward characters are escaped in `mountinfo`, and a
    /// path that does not decode would silently never match its own mount.
    #[test]
    #[cfg(target_os = "linux")]
    fn mount_points_are_unescaped() {
        let spaced = "\
25 1 259:1 / / rw,relatime - ext4 /dev/nvme0n1p1 rw
88 25 259:3 / /var/my\\040data rw,relatime - ext4 /dev/nvme1n1 rw";
        assert!(mount_is_exclusive(spaced, Path::new("/var/my data")));
        assert_eq!(
            unescape_mountinfo("a\\040b\\011c\\012d\\134e"),
            "a b\tc\nd\\e"
        );
        assert_eq!(unescape_mountinfo("plain"), "plain");
    }

    /// An ordinary subdirectory shares its parent's filesystem, so syncing it
    /// would sync whatever else lives there.
    #[test]
    fn subdirectory_does_not_have_its_own_filesystem() {
        let tempdir = tempfile::tempdir().unwrap();
        let subdirectory = tempdir.path().join("storage");
        std::fs::create_dir(&subdirectory).unwrap();
        assert!(!has_own_filesystem(&subdirectory));
    }

    /// `sync_mode: syncfs` may accept the cost of syncing a filesystem shared
    /// with others, but it may not ask for a `syncfs` that cannot report a
    /// failed writeback. Overriding that one buys a checkpoint that reports
    /// success with its data still in memory, which is worse than a slow
    /// checkpoint by a wide margin.
    #[test]
    fn only_shared_storage_is_the_operators_to_override() {
        assert!(SyncfsObstacle::NotAMountPoint.is_overridable());
        for obstacle in [
            SyncfsObstacle::Unsupported,
            SyncfsObstacle::KernelTooOld("4.18.0-553.el8.x86_64".to_string()),
            SyncfsObstacle::KernelUnknown,
        ] {
            assert!(
                !obstacle.is_overridable(),
                "{obstacle:?} must not be overridable"
            );
        }
    }

    /// The decision that `sync_mode` exists to make: `auto` declines syncfs for
    /// storage that shares a filesystem, `per_file` declines it everywhere, and
    /// `syncfs` overrides the sharing obstacle rather than being silently
    /// downgraded by it.
    ///
    /// The inputs to this decision are covered above; this covers the decision,
    /// whose five-way match is easy to reorder into a different policy without
    /// anything noticing.
    #[test]
    fn strategy_honors_sync_mode() {
        let tempdir = tempfile::tempdir().unwrap();
        let shared = tempdir.path().join("storage");
        std::fs::create_dir(&shared).unwrap();
        assert!(!has_own_filesystem(&shared), "precondition");

        assert!(!SyncStrategy::new(StorageSyncMode::Auto, &shared).is_syncfs());
        assert!(!SyncStrategy::new(StorageSyncMode::PerFile, &shared).is_syncfs());
        assert_eq!(
            SyncStrategy::new(StorageSyncMode::Syncfs, &shared).is_syncfs(),
            cfg!(target_os = "linux"),
            "an explicit syncfs must survive a shared filesystem"
        );
    }

    /// A configured syncfs must work on the very first run, before anything has
    /// created the storage directory. Otherwise the first start falls back and
    /// only a later one, finding the directory already there, goes fast.
    #[test]
    fn strategy_creates_missing_storage_directory_for_syncfs() {
        let tempdir = tempfile::tempdir().unwrap();
        let missing = tempdir.path().join("not-created");
        let strategy = SyncStrategy::new(StorageSyncMode::Syncfs, &missing);
        assert_eq!(strategy.is_syncfs(), cfg!(target_os = "linux"));
        assert_eq!(missing.is_dir(), cfg!(target_os = "linux"));
    }

    /// A directory that does not exist is not a mount point, because nothing
    /// can be mounted on a path that is not there.
    ///
    /// Answering this correctly without the directory is what lets the backend
    /// skip creating one just to decide, and creating one would not change the
    /// answer anyway: a fresh directory sits on its parent's filesystem.
    #[test]
    fn missing_directory_does_not_have_its_own_filesystem() {
        let tempdir = tempfile::tempdir().unwrap();
        let missing = tempdir.path().join("not-created");
        assert!(!missing.exists());
        assert!(!has_own_filesystem(&missing));

        std::fs::create_dir(&missing).unwrap();
        assert!(
            !has_own_filesystem(&missing),
            "creating the directory must not change the answer"
        );
    }

    /// The root filesystem never has a filesystem to itself, by definition.
    /// Without this, `auto` could pick syncfs for storage sharing the root.
    #[test]
    fn root_does_not_have_its_own_filesystem() {
        assert!(!has_own_filesystem(Path::new("/")));
    }

    /// Every file must be committed exactly once, including well past the
    /// thread count, where the workers loop for more.
    #[test]
    fn parallel_commit_covers_every_file() {
        for count in [0, 1, 2, CONCURRENT_SYNCS, CONCURRENT_SYNCS * 7 + 3] {
            let commits = Arc::new(AtomicUsize::new(0));
            let files: Vec<_> = (0..count)
                .map(|_| CountingCommitter::arc(&commits, false))
                .collect();
            commit_in_parallel(&files).unwrap();
            assert_eq!(commits.load(Ordering::Relaxed), count, "with {count} files");
        }
    }

    /// A failing fsync must surface, not be swallowed by a worker thread.
    #[test]
    fn parallel_commit_reports_failure() {
        let commits = Arc::new(AtomicUsize::new(0));
        let mut files: Vec<_> = (0..CONCURRENT_SYNCS * 4)
            .map(|_| CountingCommitter::arc(&commits, false))
            .collect();
        files.push(CountingCommitter::arc(&commits, true));
        assert!(commit_in_parallel(&files).is_err());
    }

    fn create_posix_backend(path: &Path) -> Arc<dyn StorageBackend> {
        Arc::new(PosixBackend::new(
            path,
            StorageCacheConfig::default(),
            &FileBackendConfig::default(),
        ))
    }

    /// `fsync_dir` must succeed on a real directory and surface an error on
    /// missing or non-directory paths.
    #[test]
    fn fsync_dir_helper() {
        let tempdir = tempfile::tempdir().unwrap();
        super::fsync_dir(tempdir.path()).expect("fsync on tempdir should succeed");

        let missing = tempdir.path().join("does-not-exist");
        assert!(
            super::fsync_dir(&missing).is_err(),
            "fsync_dir must surface a missing-dir error",
        );
    }

    /// The names in `dir` that a write in progress leaves behind.
    fn temporary_files(dir: &Path) -> Vec<String> {
        std::fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| name.ends_with(".mut"))
            .collect()
    }

    /// A write abandoned before `complete` leaves a file of the same name as it
    /// was, and takes its temporary file with it.
    ///
    /// A writer fills a temporary name and renames it into place only in
    /// `complete`, so a write that dies partway through never touches what the
    /// name held.  Callers that overwrite a file others read depend on this,
    /// the checkpoint catalog most of all: a half-written catalog hides every
    /// checkpoint the pipeline has.
    #[test]
    fn abandoned_write_leaves_the_previous_file_intact() {
        let tempdir = tempfile::tempdir().unwrap();
        let backend = create_posix_backend(tempdir.path());
        let name = StoragePath::from("catalog.feldera");
        let original = b"the contents that are already there";

        backend
            .write(&name, FBuf::from_slice(original))
            .unwrap()
            .commit()
            .unwrap();

        // Write the same name again and abandon it, as a process dying partway
        // through the write would.
        let mut writer = backend.create_named(&name).unwrap();
        writer
            .write_block(FBuf::from_slice(b"a replacement that never lands"))
            .unwrap();
        drop(writer);

        assert_eq!(
            std::fs::read(tempdir.path().join("catalog.feldera")).unwrap(),
            original,
            "the abandoned write damaged the file that was already there"
        );
        assert_eq!(
            temporary_files(tempdir.path()),
            Vec::<String>::new(),
            "the abandoned write left its temporary file behind"
        );
    }

    /// Write 10 MiB total in 1 KiB chunks.  `VectoredWrite` flushes its buffer when it
    /// reaches 1 MiB of sequential data, and we limit the amount of queued work
    /// to 4 MiB, so this has a chance to trigger both limits.
    #[test]
    fn sequential_1024() {
        test_backend(Box::new(create_posix_backend), &[1024; 1024 * 10], true)
    }

    /// Verify that files get deleted if not marked for a checkpoint.
    #[test]
    fn delete_1024() {
        test_backend(Box::new(create_posix_backend), &[1024; 1024 * 10], false)
    }

    #[test]
    fn sequential_random() {
        test_backend(Box::new(create_posix_backend), &random_sizes(), true);
    }

    #[test]
    fn empty() {
        test_backend(Box::new(create_posix_backend), &[], true);
    }
}
