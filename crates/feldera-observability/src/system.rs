//! System introspection utilities.

use std::fs;
use std::path::{Path, PathBuf};

/// Returns the memory, in megabytes (MB), of the current system, or `None`
/// when the operating system does not report it.
///
/// This is the min of the host's available memory and the cgroup budget
/// (what a container or Kubernetes pod memory limit sets).
pub fn total_memory_megabyte() -> Option<u64> {
    total_memory_bytes()
        .map(|bytes| bytes / 1_000_000)
        .filter(|&mb| mb > 0)
}

/// Same as [`total_memory_megabyte`], but in bytes.
pub fn total_memory_bytes() -> Option<u64> {
    let mut system = sysinfo::System::new();
    system.refresh_memory();
    let mut available = system.total_memory();
    if let Some(limits) = own_cgroup_limits(&mut system).or_else(|| system.cgroup_limits()) {
        available = available.min(limits.total_memory);
    }
    // `sysinfo` reports 0 on platforms it does not support.
    if available > 0 { Some(available) } else { None }
}

/// Returns the resident set size of this process, in bytes, or `None` when the
/// operating system does not report it.
pub fn process_rss_bytes() -> Option<u64> {
    memory_stats::memory_stats().map(|usage| usage.physical_mem as u64)
}

/// Memory use and limit of a cgroup v2 cgroup.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CgroupMemory {
    /// `memory.max`, or `None` when the cgroup has no limit.
    pub max: Option<u64>,
    /// `memory.current`, which counts page cache too.
    pub current: u64,
    /// `file` in `memory.stat`: the page cache.
    pub file: u64,
}

/// This process's own cgroup v2 cgroup.
#[derive(Clone, Debug)]
pub struct Cgroup {
    dir: PathBuf,
}

impl Cgroup {
    /// Returns this process's cgroup, or `None` without cgroup v2.
    pub fn own() -> Option<Self> {
        let cgroups = fs::read_to_string("/proc/self/cgroup").ok()?;
        let path = cgroup_v2_path(&cgroups)?;
        Some(Self::at(
            Path::new("/sys/fs/cgroup").join(path.trim_start_matches('/')),
        ))
    }

    /// Returns the cgroup whose files are in `dir`.
    pub fn at(dir: PathBuf) -> Self {
        Self { dir }
    }

    fn read(&self, name: &str) -> Option<String> {
        fs::read_to_string(self.dir.join(name)).ok()
    }

    /// Returns `memory.current`, which is cheaper to read than
    /// [`Self::memory`].
    pub fn memory_current(&self) -> Option<u64> {
        self.read("memory.current")?.trim().parse().ok()
    }

    /// Returns the cgroup's memory use and limit.
    pub fn memory(&self) -> Option<CgroupMemory> {
        let max = match self.read("memory.max")?.trim() {
            "max" => None,
            max => Some(max.parse().ok()?),
        };
        let file = self
            .read("memory.stat")?
            .lines()
            .find_map(|line| line.strip_prefix("file ")?.parse().ok())?;
        Some(CgroupMemory {
            max,
            current: self.memory_current()?,
            file,
        })
    }
}

/// Returns the cgroup v2 path in `cgroups`, the contents of `/proc/self/cgroup`.
fn cgroup_v2_path(cgroups: &str) -> Option<&str> {
    cgroups.lines().find_map(|line| line.strip_prefix("0::"))
}

/// Returns the limits of this process's own cgroup, including limits set on
/// its parent cgroups. Unlike [`sysinfo::System::cgroup_limits`], this works
/// without a cgroup namespace, where `/sys/fs/cgroup` is the host's root.
fn own_cgroup_limits(system: &mut sysinfo::System) -> Option<sysinfo::CGroupLimits> {
    let pid = sysinfo::get_current_pid().ok()?;
    system.refresh_processes_specifics(
        sysinfo::ProcessesToUpdate::Some(&[pid]),
        false,
        sysinfo::ProcessRefreshKind::nothing(),
    );
    system.process(pid)?.cgroup_limits()
}

#[cfg(test)]
mod tests {
    use super::{Cgroup, CgroupMemory, cgroup_v2_path};

    #[test]
    fn finds_the_cgroup_v2_path() {
        assert_eq!(cgroup_v2_path("0::/\n"), Some("/"));
        assert_eq!(
            cgroup_v2_path("1:name=systemd:/a\n0::/kubepods/pod1/c1\n"),
            Some("/kubepods/pod1/c1")
        );
        assert_eq!(cgroup_v2_path("4:memory:/docker/c1\n"), None);
    }

    #[test]
    fn reads_cgroup_memory() {
        let dir = tempfile::tempdir().unwrap();
        let write = |name: &str, contents: &str| std::fs::write(dir.path().join(name), contents);
        let cgroup = Cgroup::at(dir.path().to_path_buf());
        assert_eq!(cgroup.memory(), None);

        write("memory.max", "max\n").unwrap();
        write("memory.current", "3000\n").unwrap();
        write("memory.stat", "anon 1000\nfile 2000\nfile_mapped 10\n").unwrap();
        assert_eq!(cgroup.memory_current(), Some(3000));
        assert_eq!(
            cgroup.memory(),
            Some(CgroupMemory {
                max: None,
                current: 3000,
                file: 2000,
            })
        );

        write("memory.max", "4096\n").unwrap();
        assert_eq!(cgroup.memory().unwrap().max, Some(4096));
    }

    /// Guards the `sysinfo` call sequence (`cgroup_limits` panics when memory
    /// was not refreshed first).
    #[cfg(target_os = "linux")]
    #[test]
    fn reports_nonzero_memory() {
        let mb = super::total_memory_megabyte().expect("Linux reports available memory");
        assert!(mb > 0, "available memory must be positive, got {mb} MB");
    }

    /// A running process always has a resident set.
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn reports_the_resident_set_of_this_process() {
        let bytes = super::process_rss_bytes().expect("Linux and macOS report the resident set");
        assert!(
            bytes > 0,
            "the resident set must be positive, got {bytes} bytes"
        );
    }

    /// Runs the real cgroup lookup, which the DBSP tests mock out.
    #[cfg(target_os = "linux")]
    #[test]
    fn available_memory_is_at_most_host_memory() {
        let mut system = sysinfo::System::new();
        system.refresh_memory();
        let bytes = super::total_memory_bytes().expect("Linux reports available memory");
        assert!(
            bytes <= system.total_memory(),
            "available memory {bytes} exceeds host memory {}",
            system.total_memory()
        );
    }
}
