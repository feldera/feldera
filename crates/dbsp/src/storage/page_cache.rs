//! Drops storage files from the page cache, so that a lower memory limit can
//! take effect while the circuit runs.
//!
//! Kubernetes lowers a running container's memory limit only when the
//! cgroup's `memory.current`, which counts page cache, is below the new limit.
//! Until then it tries again every 10 to 15 seconds.

use std::{
    fs,
    path::Path,
    sync::{Mutex, PoisonError},
    time::{Duration, Instant},
};

use feldera_observability::system::{Cgroup, CgroupMemory};
use size_of::HumanBytes;
use tracing::{debug, info, warn};

use crate::Runtime;

/// The drop stops at this fraction of the target below it, so that reads that
/// fill the cache again before the next check stay under the target.
const HEADROOM_DIVISOR: u64 = 20;

/// How long to wait for the limit to go down before stopping.
const DEADLINE: Duration = Duration::from_secs(30 * 60);

/// How long one drop pass may take, so that the RSS monitor and a stop of the
/// circuit do not wait long. The next check continues the drop.
const PASS_BUDGET: Duration = Duration::from_millis(500);

/// A memory limit that the circuit waits for.
#[derive(Debug)]
pub(crate) struct Target {
    bytes: u64,
    since: Instant,
    drops: usize,
    files: usize,
    reported_blocked: bool,
}

impl Target {
    pub(crate) fn new(bytes: u64) -> Self {
        Self {
            bytes,
            since: Instant::now(),
            drops: 0,
            files: 0,
            reported_blocked: false,
        }
    }
}

/// What one check of the cgroup asks for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Step {
    /// The limit is at most the target, or there is no limit.
    Applied,
    /// The limit did not go down before the deadline.
    TimedOut,
    /// Usage is low enough. The kubelet applies the limit when it next tries.
    Wait,
    /// Memory that is not page cache is above the target, so a drop cannot help.
    Blocked,
    /// Drop page cache until usage is at most `until`.
    Drop { until: u64 },
}

fn step(target: u64, elapsed: Duration, memory: CgroupMemory) -> Step {
    if memory.max.is_none_or(|max| max <= target) {
        return Step::Applied;
    }
    if elapsed >= DEADLINE {
        return Step::TimedOut;
    }
    let until = target - target / HEADROOM_DIVISOR;
    if memory.current <= until {
        Step::Wait
    } else if memory.current.saturating_sub(memory.file) >= until {
        Step::Blocked
    } else {
        Step::Drop { until }
    }
}

/// Checks `cgroup` once against the target that `target` finds in `lock`, and
/// drops page cache of the files under `dir` when that can bring the memory
/// limit down. Does not hold `lock` during the drop.
pub(crate) fn check<T>(
    lock: &Mutex<T>,
    target: fn(&mut T) -> &mut Option<Target>,
    cgroup: &Cgroup,
    dir: &Path,
) {
    let mut guard = lock.lock().unwrap_or_else(PoisonError::into_inner);
    let slot = target(&mut guard);
    let Some(current) = slot.as_mut() else {
        return;
    };
    let Some(memory) = cgroup.memory() else {
        return;
    };
    let limit = HumanBytes::new(current.bytes);
    let until = match step(current.bytes, current.since.elapsed(), memory) {
        Step::Applied => {
            if current.drops > 0 {
                info!(
                    "memory limit is down to {limit} after dropping page cache {} times ({} files)",
                    current.drops, current.files
                );
            }
            *slot = None;
            return;
        }
        Step::TimedOut => {
            warn!(
                "memory limit is still {} after {DEADLINE:?}, not {limit}; no longer dropping page cache",
                HumanBytes::new(memory.max.unwrap_or_default())
            );
            *slot = None;
            return;
        }
        Step::Wait => return,
        Step::Blocked => {
            if !current.reported_blocked {
                current.reported_blocked = true;
                info!(
                    "memory use without page cache ({}) is too high for a memory limit of {limit}",
                    HumanBytes::new(memory.current - memory.file)
                );
            }
            return;
        }
        Step::Drop { until } => until,
    };
    if current.drops == 0 {
        info!(
            "dropping page cache ({}) so that the memory limit can go down to {limit} (memory use {})",
            HumanBytes::new(memory.file),
            HumanBytes::new(memory.current)
        );
    }
    let since = current.since;
    drop(guard);

    let files = drop_until(dir, cgroup, until, PASS_BUDGET);
    debug!(
        "dropped page cache of {files} files, memory use is now {}",
        HumanBytes::new(cgroup.memory_current().unwrap_or_default())
    );

    // A new target that arrived during the drop starts its own count.
    let mut guard = lock.lock().unwrap_or_else(PoisonError::into_inner);
    if let Some(current) = target(&mut guard).as_mut()
        && current.since == since
    {
        current.drops += 1;
        current.files += files;
    }
}

/// Drops page cache of the files under `dir` until `cgroup` uses at most
/// `until`, `budget` has passed, or the circuit stops. Returns the number of
/// files it asked about.
fn drop_until(dir: &Path, cgroup: &Cgroup, until: u64, budget: Duration) -> usize {
    let start = Instant::now();
    drop_page_cache(dir, &mut || {
        start.elapsed() >= budget
            || Runtime::kill_in_progress()
            || cgroup
                .memory_current()
                .is_none_or(|current| current <= until)
    })
}

/// Asks the kernel to drop the page cache of each file under `dir`, and stops
/// once `done` returns true. Returns the number of files it asked about.
fn drop_page_cache(dir: &Path, done: &mut dyn FnMut() -> bool) -> usize {
    let mut files = 0;
    let mut dirs = vec![dir.to_path_buf()];
    while let Some(dir) = dirs.pop() {
        let Ok(entries) = fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            let path = entry.path();
            if file_type.is_dir() {
                dirs.push(path);
            } else if file_type.is_file() {
                if done() {
                    return files;
                }
                if drop_file(&path) {
                    files += 1;
                }
            }
        }
    }
    files
}

#[cfg(target_os = "linux")]
fn drop_file(path: &Path) -> bool {
    use std::os::fd::AsRawFd;

    let Ok(file) = fs::File::open(path) else {
        return false;
    };
    // SAFETY: `file` holds an open file descriptor.
    unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED) == 0 }
}

#[cfg(not(target_os = "linux"))]
fn drop_file(_path: &Path) -> bool {
    false
}

#[cfg(test)]
pub(crate) mod tests {
    use std::{path::Path, sync::Mutex, time::Duration};

    use feldera_observability::system::{Cgroup, CgroupMemory};

    use super::{DEADLINE, Step, Target, check, step};

    const GIB: u64 = 1 << 30;

    fn memory(max: Option<u64>, current: u64, file: u64) -> CgroupMemory {
        CgroupMemory { max, current, file }
    }

    #[test]
    fn steps() {
        let target = 20 * GIB;
        let until = 19 * GIB;
        let now = Duration::ZERO;
        let cases = [
            // The kubelet applied the limit, or there is none to wait for.
            (memory(Some(target), 30 * GIB, 25 * GIB), now, Step::Applied),
            (memory(Some(GIB), 30 * GIB, 25 * GIB), now, Step::Applied),
            (memory(None, 30 * GIB, 25 * GIB), now, Step::Applied),
            // Page cache keeps the limit up.
            (
                memory(Some(100 * GIB), 80 * GIB, 70 * GIB),
                now,
                Step::Drop { until },
            ),
            // Low enough: only the kubelet is left to act.
            (memory(Some(100 * GIB), until, 5 * GIB), now, Step::Wait),
            // Dropping all the page cache would not be enough.
            (
                memory(Some(100 * GIB), 80 * GIB, 61 * GIB),
                now,
                Step::Blocked,
            ),
            (
                memory(Some(100 * GIB), 80 * GIB, 70 * GIB),
                DEADLINE,
                Step::TimedOut,
            ),
        ];
        for (memory, elapsed, expected) in cases {
            assert_eq!(step(target, elapsed, memory), expected, "{memory:?}");
        }
    }

    /// Writes the cgroup files that [Cgroup::memory] reads.
    pub(crate) fn write_cgroup(dir: &Path, max: u64, current: u64, file: u64) {
        std::fs::write(dir.join("memory.max"), format!("{max}\n")).unwrap();
        std::fs::write(dir.join("memory.current"), format!("{current}\n")).unwrap();
        std::fs::write(dir.join("memory.stat"), format!("anon 0\nfile {file}\n")).unwrap();
    }

    #[test]
    fn checks_until_the_limit_goes_down() {
        let cgroup_dir = tempfile::tempdir().unwrap();
        let storage = tempfile::tempdir().unwrap();
        std::fs::write(storage.path().join("a.feldera"), b"data").unwrap();
        let cgroup = Cgroup::at(cgroup_dir.path().to_path_buf());
        let target = Mutex::new(Some(Target::new(20 * GIB)));
        let drops =
            |target: &Mutex<Option<Target>>| target.lock().unwrap().as_ref().map(|t| t.drops);

        // Page cache holds the limit up, and stays put in this fake cgroup.
        write_cgroup(cgroup_dir.path(), 100 * GIB, 80 * GIB, 70 * GIB);
        check(&target, |target| target, &cgroup, storage.path());
        check(&target, |target| target, &cgroup, storage.path());
        assert_eq!(drops(&target), Some(2));

        // Low enough: wait for the kubelet without dropping.
        write_cgroup(cgroup_dir.path(), 100 * GIB, GIB, 0);
        check(&target, |target| target, &cgroup, storage.path());
        assert_eq!(drops(&target), Some(2));

        // The kubelet applied the limit.
        write_cgroup(cgroup_dir.path(), 20 * GIB, GIB, 0);
        check(&target, |target| target, &cgroup, storage.path());
        assert_eq!(drops(&target), None);
    }

    /// A target that replaces the old one during a drop starts with no drops.
    #[test]
    fn a_new_target_during_a_drop_starts_its_own_count() {
        struct State {
            target: Option<Target>,
            locks: usize,
        }

        // `check` locks once before the drop and once after it. Replace the
        // target at the second lock, as a resize during the drop does.
        fn target(state: &mut State) -> &mut Option<Target> {
            state.locks += 1;
            if state.locks == 2 {
                let mut new = Target::new(30 * GIB);
                new.since += Duration::from_secs(1);
                state.target = Some(new);
            }
            &mut state.target
        }

        let cgroup_dir = tempfile::tempdir().unwrap();
        let storage = tempfile::tempdir().unwrap();
        std::fs::write(storage.path().join("a.feldera"), b"data").unwrap();
        let cgroup = Cgroup::at(cgroup_dir.path().to_path_buf());
        write_cgroup(cgroup_dir.path(), 100 * GIB, 80 * GIB, 70 * GIB);
        let state = Mutex::new(State {
            target: Some(Target::new(20 * GIB)),
            locks: 0,
        });

        check(&state, target, &cgroup, storage.path());

        let state = state.lock().unwrap();
        let new = state.target.as_ref().unwrap();
        assert_eq!((new.bytes, new.drops, new.files), (30 * GIB, 0, 0));
    }

    #[cfg(target_os = "linux")]
    fn page_size() -> usize {
        // SAFETY: `sysconf` has no preconditions.
        unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize }
    }

    /// Returns how many pages of `path` are in the page cache.
    #[cfg(target_os = "linux")]
    fn cached_pages(path: &std::path::Path) -> usize {
        use std::os::fd::AsRawFd;

        let file = std::fs::File::open(path).unwrap();
        let len = file.metadata().unwrap().len() as usize;
        let pages = len.div_ceil(page_size());
        let mut residency = vec![0u8; pages];
        // SAFETY: the mapping covers `len` bytes of an open file, and
        // `residency` has one byte per page of it.
        unsafe {
            let map = libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ,
                libc::MAP_SHARED,
                file.as_raw_fd(),
                0,
            );
            assert_ne!(map, libc::MAP_FAILED);
            assert_eq!(libc::mincore(map, len, residency.as_mut_ptr()), 0);
            libc::munmap(map, len);
        }
        residency.iter().filter(|&&page| page & 1 != 0).count()
    }

    /// A pass stops when its time budget is spent, even while usage is high.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_drop_pass_stops_at_its_budget() {
        use super::drop_until;

        let cgroup_dir = tempfile::tempdir().unwrap();
        let storage = tempfile::tempdir().unwrap();
        std::fs::write(storage.path().join("a.feldera"), b"data").unwrap();
        std::fs::write(storage.path().join("b.feldera"), b"data").unwrap();
        let cgroup = Cgroup::at(cgroup_dir.path().to_path_buf());
        write_cgroup(cgroup_dir.path(), 100 * GIB, 80 * GIB, 70 * GIB);

        assert_eq!(drop_until(storage.path(), &cgroup, GIB, Duration::ZERO), 0);
        assert_eq!(drop_until(storage.path(), &cgroup, GIB, DEADLINE), 2);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn drops_page_cache() {
        use super::drop_page_cache;
        use std::io::Write;

        // `/tmp` may be `tmpfs`, which cannot drop its pages, so use the
        // directory that holds the test binary.
        let exe = std::env::current_exe().unwrap();
        let dir = tempfile::tempdir_in(exe.parent().unwrap()).unwrap();
        std::fs::create_dir(dir.path().join("checkpoint")).unwrap();
        let names = ["a.feldera", "checkpoint/b.feldera", "c.feldera.mut"];
        for name in names {
            let mut file = std::fs::File::create(dir.path().join(name)).unwrap();
            file.write_all(&vec![1u8; 1 << 20]).unwrap();
            file.sync_all().unwrap();
            std::fs::read(dir.path().join(name)).unwrap();
        }
        let pages = (1 << 20) / page_size();
        let cached = || -> usize {
            names
                .iter()
                .map(|name| cached_pages(&dir.path().join(name)))
                .sum()
        };
        assert_eq!(
            cached(),
            3 * pages,
            "the files must start in the page cache"
        );

        assert_eq!(drop_page_cache(dir.path(), &mut || true), 0);
        assert_eq!(cached(), 3 * pages);

        let mut checks = 0;
        let one = drop_page_cache(dir.path(), &mut || {
            checks += 1;
            checks > 1
        });
        assert_eq!(one, 1);
        assert_eq!(cached(), 2 * pages);

        // Files still being written are dropped too.
        assert_eq!(drop_page_cache(dir.path(), &mut || false), 3);
        assert_eq!(cached(), 0);
    }
}
