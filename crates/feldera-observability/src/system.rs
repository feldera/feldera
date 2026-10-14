//! System introspection utilities.

/// True when this process runs inside a Kubernetes pod.
///
/// Kubernetes injects `KUBERNETES_SERVICE_HOST` into every pod, and the
/// pipeline manager rejects user-supplied `KUBERNETES_*` variables in the
/// pipeline's `env` config.
pub fn running_in_kubernetes() -> bool {
    std::env::var_os("KUBERNETES_SERVICE_HOST").is_some()
}

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
    /// Guards the `sysinfo` call sequence (`cgroup_limits` panics when memory
    /// was not refreshed first).
    #[cfg(target_os = "linux")]
    #[test]
    fn reports_nonzero_memory() {
        let mb = super::total_memory_megabyte().expect("Linux reports available memory");
        assert!(mb > 0, "available memory must be positive, got {mb} MB");
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
