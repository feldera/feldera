//! The host that a profile's workers ran on.

use feldera_observability::system::Cgroup;
use feldera_types::{
    circuit::Layout,
    config::{NODE_NAME_ENV, POD_NAME_ENV},
};
use serde::{Deserialize, Serialize};
use std::ops::Range;

/// One host of a circuit, with the workers that run on it.
///
/// The CPU and memory values come from the host's cgroup at the time of the
/// profile, so they show an in-place resize of the pod. Cgroups exist only on
/// Linux, and these values need cgroup v2; elsewhere they are absent.
///
/// The pod and node values come from the environment variables that the
/// Kubernetes runner sets. Outside Kubernetes they are absent. The profile
/// leaves out the pod's IP address and namespace on purpose, because a
/// profile can go to people outside the cluster, for example in a support
/// bundle.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct HostProfile {
    /// The host's index, from 0 to `hosts - 1`.
    pub ordinal: usize,

    /// The global numbers of the workers on this host.
    pub workers: Range<usize>,

    /// The name of the host's Kubernetes pod (from `FELDERA_POD_NAME`), for
    /// example `pipeline-<id>-<ordinal>`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pod_name: Option<String>,

    /// The name of the Kubernetes node that runs the pod (from
    /// `FELDERA_NODE_NAME`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_name: Option<String>,

    /// The cgroup's CPU limit (`cpu.max`), in cores. Absent when the cgroup
    /// has no limit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_limit_cores: Option<f64>,

    /// The cgroup's memory limit (`memory.max`). Absent when the cgroup has
    /// no limit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_limit_bytes: Option<u64>,

    /// The cgroup's memory use (`memory.current`), page cache included.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_used_bytes: Option<u64>,
}

impl HostProfile {
    /// Returns the profile of the local host in `layout`.
    pub fn local(layout: &Layout) -> Self {
        Self::new(layout, |name| std::env::var(name).ok(), Cgroup::own())
    }

    /// Returns the profile of the local host in `layout`, with `env` to read
    /// environment variables and `cgroup` for the resource values.
    fn new(layout: &Layout, env: impl Fn(&str) -> Option<String>, cgroup: Option<Cgroup>) -> Self {
        let env = |name| env(name).filter(|value| !value.is_empty());
        let memory = cgroup.as_ref().and_then(Cgroup::memory);
        Self {
            ordinal: layout.local_host_idx(),
            workers: layout.local_workers(),
            pod_name: env(POD_NAME_ENV),
            node_name: env(NODE_NAME_ENV),
            cpu_limit_cores: cgroup.as_ref().and_then(Cgroup::cpu_max_cores).flatten(),
            memory_limit_bytes: memory.and_then(|memory| memory.max),
            memory_used_bytes: memory.map(|memory| memory.current),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;

    /// A host with every value set, and one with none.
    #[test]
    fn host_profile_reads_env_and_cgroup() {
        let layout = Layout::new_multihost(
            &[
                ("127.0.0.1:1".parse().unwrap(), 2),
                ("127.0.0.1:2".parse().unwrap(), 3),
            ],
            "127.0.0.1:2".parse().unwrap(),
        )
        .unwrap();

        let dir = tempfile::tempdir().unwrap();
        let write = |name: &str, contents: &str| std::fs::write(dir.path().join(name), contents);
        write("cpu.max", "200000 100000\n").unwrap();
        write("memory.max", "4096\n").unwrap();
        write("memory.current", "3000\n").unwrap();
        write("memory.stat", "file 2000\n").unwrap();
        let env = HashMap::from([(POD_NAME_ENV, "pipeline-1"), (NODE_NAME_ENV, "node-a")]);
        let host = HostProfile::new(
            &layout,
            |name| env.get(name).map(|value| value.to_string()),
            Some(Cgroup::at(dir.path().to_path_buf())),
        );
        assert_eq!(
            serde_json::to_value(&host).unwrap(),
            json!({
                "ordinal": 1,
                "workers": {"start": 2, "end": 5},
                "pod_name": "pipeline-1",
                "node_name": "node-a",
                "cpu_limit_cores": 2.0,
                "memory_limit_bytes": 4096,
                "memory_used_bytes": 3000,
            })
        );

        // An empty variable counts as unset, and so does a missing cgroup.
        let host = HostProfile::new(&Layout::new_solo(4), |_| Some(String::new()), None);
        assert_eq!(
            serde_json::to_value(&host).unwrap(),
            json!({"ordinal": 0, "workers": {"start": 0, "end": 4}})
        );
    }

    /// A cgroup without limits gives only the memory use.
    #[test]
    fn host_profile_without_limits() {
        let dir = tempfile::tempdir().unwrap();
        let write = |name: &str, contents: &str| std::fs::write(dir.path().join(name), contents);
        write("cpu.max", "max 100000\n").unwrap();
        write("memory.max", "max\n").unwrap();
        write("memory.current", "3000\n").unwrap();
        write("memory.stat", "file 2000\n").unwrap();
        let host = HostProfile::new(
            &Layout::new_solo(4),
            |_| None,
            Some(Cgroup::at(dir.path().to_path_buf())),
        );
        assert_eq!(
            serde_json::to_value(&host).unwrap(),
            json!({
                "ordinal": 0,
                "workers": {"start": 0, "end": 4},
                "memory_used_bytes": 3000,
            })
        );
    }

    /// A cgroup directory without controller files gives no resource values.
    #[test]
    fn host_profile_without_cgroup_files() {
        let dir = tempfile::tempdir().unwrap();
        let host = HostProfile::new(
            &Layout::new_solo(4),
            |_| None,
            Some(Cgroup::at(dir.path().to_path_buf())),
        );
        assert_eq!(
            serde_json::to_value(&host).unwrap(),
            json!({"ordinal": 0, "workers": {"start": 0, "end": 4}})
        );
    }
}
