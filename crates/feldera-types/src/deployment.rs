//! What exists for a pipeline deployment: desired versus current resources per host, and the
//! state of each transition. State names follow the Kubernetes kstatus library.

use crate::config::ResourceConfig;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Spec and observed status of a pipeline deployment.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct PipelineDeployment {
    pub spec: DeploymentSpec,
    /// Absent when the runner does not observe its resources.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<DeploymentStatus>,
}

/// What the deployment asks for, from the pipeline's `deployment_config`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct DeploymentSpec {
    pub resources: DeploymentSpecResources,
}

/// Each `*_min` is a Kubernetes request and each `*_max` a limit.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct DeploymentSpecResources {
    pub cpu_cores_min: Option<f64>,
    pub cpu_cores_max: Option<f64>,
    pub memory_mb_min: Option<u64>,
    pub memory_mb_max: Option<u64>,
    pub storage_mb_min: Option<u64>,
    pub storage_mb_max: Option<u64>,
}

impl From<&ResourceConfig> for DeploymentSpecResources {
    fn from(r: &ResourceConfig) -> Self {
        Self {
            cpu_cores_min: r.cpu_cores_min,
            cpu_cores_max: r.cpu_cores_max,
            memory_mb_min: r.memory_mb_min,
            memory_mb_max: r.memory_mb_max,
            storage_mb_min: r.storage_mb_min,
            storage_mb_max: r.storage_mb_max,
        }
    }
}

/// kstatus status of an object or of the whole deployment.
/// <https://github.com/kubernetes-sigs/cli-utils/blob/master/pkg/kstatus/status/status.go>
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub enum DeploymentState {
    /// Matches its desired state.
    Current,
    /// On its way to the desired state.
    InProgress,
    /// Will not reach the desired state without a change.
    Failed,
    /// Being deleted.
    Terminating,
    /// Does not exist.
    NotFound,
    /// Cannot be determined.
    Unknown,
}

/// Who sets the desired value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub enum ResourceManager {
    /// The pipeline's configuration.
    User,
    /// The runner's autoscaler, within the configured bounds.
    Autoscaler,
}

/// State of one object and why it is in it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct Transition {
    pub state: DeploymentState,
    /// Machine-readable cause, passed through from the runner's platform.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// When the platform last changed the condition behind `state` (Kubernetes
    /// `lastTransitionTime`, or `deletionTimestamp` for `Terminating`). Not when `state` itself
    /// changed: a reason change keeps the time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<DateTime<Utc>>,
}

impl Transition {
    pub fn new(state: DeploymentState) -> Self {
        Self {
            state,
            reason: None,
            message: None,
            last_transition_time: None,
        }
    }

    pub fn with_reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = Some(reason.into());
        self
    }

    pub fn with_message(mut self, message: impl Into<String>) -> Self {
        self.message = Some(message.into());
        self
    }

    pub fn with_last_transition_time(mut self, since: Option<DateTime<Utc>>) -> Self {
        self.last_transition_time = since;
        self
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ComputeResources {
    pub cpu_cores_min: Option<f64>,
    pub cpu_cores_max: Option<f64>,
    pub memory_mb_min: Option<u64>,
    pub memory_mb_max: Option<u64>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct StorageResources {
    pub storage_mb: Option<u64>,
}

/// CPU and memory of one host.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct HostCompute {
    #[serde(flatten)]
    pub transition: Transition,
    pub manager: ResourceManager,
    pub desired: Option<ComputeResources>,
    pub current: Option<ComputeResources>,
}

/// Storage volume of one host.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct HostStorage {
    #[serde(flatten)]
    pub transition: Transition,
    pub manager: ResourceManager,
    pub desired: Option<StorageResources>,
    pub current: Option<StorageResources>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct HostStatus {
    pub ordinal: usize,
    pub compute: HostCompute,
    /// Absent when the pipeline has no storage.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage: Option<HostStorage>,
}

/// Observed status of the deployment. `state`, `reason` and `message` summarize `hosts`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct DeploymentStatus {
    pub state: DeploymentState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    pub hosts: Vec<HostStatus>,
}

impl DeploymentStatus {
    /// Summarizes `hosts` like kstatus `AggregateStatus`: `Failed` if any object failed, else
    /// `Unknown` if any is unknown, else the desired state if every object reached the state it
    /// is expected to (compute `compute_expected`, storage `storage_expected`), else
    /// `InProgress`. The desired state is `NotFound` when nothing is expected to exist, else
    /// `Current`. Reason and message come from the first object that decides the result.
    /// <https://github.com/kubernetes-sigs/cli-utils/blob/master/pkg/kstatus/polling/aggregator/aggregator.go>
    pub fn aggregate(
        hosts: Vec<HostStatus>,
        compute_expected: DeploymentState,
        storage_expected: DeploymentState,
    ) -> Self {
        let objects: Vec<(&Transition, DeploymentState)> = hosts
            .iter()
            .flat_map(|host| {
                std::iter::once((&host.compute.transition, compute_expected)).chain(
                    host.storage
                        .as_ref()
                        .map(|storage| (&storage.transition, storage_expected)),
                )
            })
            .collect();
        let desired = if compute_expected == DeploymentState::NotFound
            && objects
                .iter()
                .all(|(_, expected)| *expected == DeploymentState::NotFound)
        {
            DeploymentState::NotFound
        } else {
            DeploymentState::Current
        };
        let first = |matches: &dyn Fn(&Transition, DeploymentState) -> bool| {
            objects
                .iter()
                .find(|(transition, expected)| matches(transition, *expected))
                .map(|(transition, _)| *transition)
        };
        let (state, decider) = if let Some(t) = first(&|t, _| t.state == DeploymentState::Failed) {
            (DeploymentState::Failed, Some(t))
        } else if let Some(t) = first(&|t, _| t.state == DeploymentState::Unknown) {
            (DeploymentState::Unknown, Some(t))
        } else if let Some(t) = first(&|t, expected| t.state != expected) {
            (DeploymentState::InProgress, Some(t))
        } else {
            (desired, None)
        };
        Self {
            state,
            reason: decider.and_then(|t| t.reason.clone()),
            message: decider.and_then(|t| t.message.clone()),
            hosts,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use DeploymentState::*;

    fn host(compute: Transition, storage: Option<Transition>) -> HostStatus {
        HostStatus {
            ordinal: 0,
            compute: HostCompute {
                transition: compute,
                manager: ResourceManager::User,
                desired: None,
                current: None,
            },
            storage: storage.map(|transition| HostStorage {
                transition,
                manager: ResourceManager::User,
                desired: None,
                current: None,
            }),
        }
    }

    fn summary(status: &DeploymentStatus) -> (DeploymentState, Option<&str>) {
        (status.state, status.reason.as_deref())
    }

    #[test]
    fn all_objects_as_expected_is_current() {
        let running = DeploymentStatus::aggregate(
            vec![host(
                Transition::new(Current),
                Some(Transition::new(Current)),
            )],
            Current,
            Current,
        );
        assert_eq!(summary(&running), (Current, None));

        // A stopped pipeline keeps its storage and has no pod.
        let stopped = DeploymentStatus::aggregate(
            vec![host(
                Transition::new(NotFound),
                Some(Transition::new(Current)),
            )],
            NotFound,
            Current,
        );
        assert_eq!(summary(&stopped), (Current, None));
    }

    #[test]
    fn object_not_as_expected_is_in_progress() {
        // Stopping: the pod is still there.
        let stopping = DeploymentStatus::aggregate(
            vec![host(
                Transition::new(Terminating).with_reason("Deleting"),
                None,
            )],
            NotFound,
            Current,
        );
        assert_eq!(summary(&stopping), (InProgress, Some("Deleting")));

        // Starting: the pod is not there yet.
        let starting = DeploymentStatus::aggregate(
            vec![host(Transition::new(NotFound), None)],
            Current,
            Current,
        );
        assert_eq!(summary(&starting), (InProgress, None));
    }

    #[test]
    fn failed_wins_over_unknown_and_in_progress() {
        let status = DeploymentStatus::aggregate(
            vec![
                host(
                    Transition::new(InProgress).with_reason("Deferred"),
                    Some(Transition::new(Unknown).with_reason("ListFailed")),
                ),
                host(
                    Transition::new(Current),
                    Some(
                        Transition::new(Failed)
                            .with_reason("ControllerResizeInfeasible")
                            .with_message("too big"),
                    ),
                ),
            ],
            Current,
            Current,
        );
        assert_eq!(
            summary(&status),
            (Failed, Some("ControllerResizeInfeasible"))
        );
        assert_eq!(status.message.as_deref(), Some("too big"));
    }

    #[test]
    fn unknown_wins_over_in_progress() {
        let status = DeploymentStatus::aggregate(
            vec![
                host(Transition::new(InProgress).with_reason("Deferred"), None),
                host(Transition::new(Unknown).with_reason("GetFailed"), None),
            ],
            Current,
            Current,
        );
        assert_eq!(summary(&status), (Unknown, Some("GetFailed")));
    }

    #[test]
    fn first_deciding_object_gives_the_reason() {
        let status = DeploymentStatus::aggregate(
            vec![
                host(Transition::new(Current), None),
                host(
                    Transition::new(InProgress).with_reason("Deferred"),
                    Some(Transition::new(InProgress).with_reason("NodeResizePending")),
                ),
            ],
            Current,
            Current,
        );
        assert_eq!(summary(&status), (InProgress, Some("Deferred")));
    }

    #[test]
    fn no_hosts_is_the_desired_state() {
        let status = DeploymentStatus::aggregate(vec![], Current, Current);
        assert_eq!(summary(&status), (Current, None));
        let status = DeploymentStatus::aggregate(vec![], NotFound, NotFound);
        assert_eq!(summary(&status), (NotFound, None));
    }

    /// Stopped and cleared: kstatus reports `NotFound` when that is the desired state.
    #[test]
    fn nothing_expected_to_exist_is_not_found() {
        let cleared = DeploymentStatus::aggregate(
            vec![host(
                Transition::new(NotFound),
                Some(Transition::new(NotFound)),
            )],
            NotFound,
            NotFound,
        );
        assert_eq!(summary(&cleared), (NotFound, None));

        // Without storage, the pod alone decides.
        let stopped = DeploymentStatus::aggregate(
            vec![host(Transition::new(NotFound), None)],
            NotFound,
            Current,
        );
        assert_eq!(summary(&stopped), (NotFound, None));
    }

    #[test]
    fn serializes_flat() {
        let status = DeploymentStatus::aggregate(
            vec![host(
                Transition::new(InProgress).with_reason("InProgress"),
                None,
            )],
            Current,
            Current,
        );
        assert_eq!(
            serde_json::to_value(&status).unwrap(),
            serde_json::json!({
                "state": "InProgress",
                "reason": "InProgress",
                "hosts": [{
                    "ordinal": 0,
                    "compute": {
                        "state": "InProgress",
                        "reason": "InProgress",
                        "manager": "User",
                        "desired": null,
                        "current": null
                    }
                }]
            })
        );
    }
}
