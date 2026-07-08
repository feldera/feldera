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
    let mut system = sysinfo::System::new();
    system.refresh_memory();
    let mut available = system.total_memory();
    if let Some(limits) = system.cgroup_limits() {
        available = available.min(limits.total_memory);
    }
    let mb = available / 1_000_000;
    // `sysinfo` reports 0 on platforms it does not support.
    if mb > 0 { Some(mb) } else { None }
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
}
