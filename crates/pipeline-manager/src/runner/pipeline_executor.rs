use crate::config::CommonConfig;
use crate::db::types::pipeline::PipelineId;
use crate::db::types::program::RuntimeSelector;
use crate::db::types::version::Version;
use crate::error::ManagerError;
use crate::runner::pipeline_logs::LogsSender;
use async_trait::async_trait;
use feldera_types::config::LegacySpelling;
use feldera_types::config::{PipelineConfig, ResourceConfig, StorageConfig};
use feldera_types::deployment::{DeploymentSpec, DeploymentState, PipelineDeployment};
use feldera_types::runtime_status::{BootstrapConfig, RuntimeDesiredStatus};
use serde::Deserialize;
use std::time::Duration;
use uuid::Uuid;

pub enum ProvisionStatus {
    /// Pipeline resources are still being provisioned.
    Ongoing { details: serde_json::Value },
    /// Pipeline resources are provisioned.
    Provisioned {
        location: String,
        details: serde_json::Value,
    },
}

/// Trait to be implemented by any pipeline runner.
/// The `PipelineAutomaton` invokes these methods per pipeline.
#[async_trait]
pub trait PipelineExecutor: Sync + Send {
    /// Configuration unique to the runner type.
    type Config: Clone + Send + Sync + 'static;

    /// Timeout for the `Provisioning` stage in which resources are provisioned.
    const DEFAULT_PROVISIONING_TIMEOUT: Duration;

    /// Constructs a new runner for the provided pipeline and starts rejection logging.
    fn new(
        pipeline_id: PipelineId,
        common_config: CommonConfig,
        config: Self::Config,
        client: reqwest::Client,
        logs_sender: LogsSender,
    ) -> Self;

    /// Generates the storage configuration of the pipeline if storage is enabled.
    /// The storage configuration is part of the pipeline deployment configuration.
    async fn generate_storage_config(&self) -> StorageConfig;

    /// Checks whether it is able to manage (i.e., create, retrieve, update, delete) the compute and
    /// storage resources that would be provisioned to back the pipeline.
    ///
    /// If this method does not return an error, it guarantees `stop()` and `clear()` can (with the
    /// current runner configuration) succeed to check any provisioned compute and storage resources
    /// exist (even if provisioning failed), and be able to delete them if they exist.
    ///
    /// This is called during the `Stopped` resources status.
    async fn can_provision(&self, deployment_config: &PipelineConfig) -> Result<(), ManagerError>;

    /// Provisions compute and (optionally) storage resources.
    ///
    /// This is called during the `Provisioning` resources status.
    ///
    /// The provisioned resources are uniquely identifiable/addressable through the pipeline
    /// identifier, such that no runtime state is needed to manage them (as the runner can restart
    /// while the pipeline is running). This operation is idempotent and as non-blocking as possible
    /// such that the user can stop the provisioning. After calling this once, completion should be
    /// checked using `is_provisioned()`.
    ///
    /// Note: `program_info` is used to discover the set of secrets to mount into the pipeline.
    ///
    /// When `runtime_selector` selects the Gen2 engine, no pipeline binary was
    /// compiled: the executor launches the Gen2 engine and ignores
    /// `program_binary_url`. An executor runs the engine build `gen2:<sha>` names,
    /// or refuses the pipeline if it cannot select builds. A Gen2 program with Rust UDFs references its UDF
    /// library in the program info; the executor fetches it from the compiler
    /// server and passes it to the engine (`--udf-library`).
    ///
    /// `legacy_spelling` says which settings the pipeline's runtime reads only
    /// in their older spelling, because it is pinned to a runtime other than
    /// the platform's or was compiled by an earlier platform that predates a
    /// rename: the executor writes the configuration the pipeline reads through
    /// [`LegacySpelling::apply`].
    #[allow(clippy::too_many_arguments)]
    async fn provision(
        &mut self,
        deployment_initial: RuntimeDesiredStatus,
        bootstrap_config: Option<BootstrapConfig>,
        deployment_id: &Uuid,
        deployment_config: &PipelineConfig,
        program_info: &serde_json::Value,
        program_binary_url: &str,
        program_info_url: &str,
        program_version: Version,
        runtime_selector: &RuntimeSelector,
        legacy_spelling: LegacySpelling,
    ) -> Result<(), ManagerError>;

    // `is_provisioned()`, `check()`, `stop()` and `clear()` receive the stored deployment config
    // as JSON: it can be from an older platform version, and must not fail to parse.

    /// Validates whether the compute and possibly storage resources provisioning
    /// initiated by `provision()` is completed. An error is returned only if the resources
    /// encountered an irrecoverable failure.
    ///
    /// This is called during the `Provisioning` resources status.
    async fn is_provisioned(
        &mut self,
        deployment_config: &serde_json::Value,
    ) -> Result<ProvisionStatus, ManagerError>;

    /// Checks the healthiness of the pipeline compute and storage resources.
    /// An error is returned only if the resources encountered an irrecoverable failure.
    ///
    /// This is called during the `Provisioned` resources status.
    async fn check(
        &mut self,
        deployment_config: &serde_json::Value,
    ) -> Result<serde_json::Value, ManagerError>;

    /// Scales down to zero or deallocates the compute resources, but retains storage resources.
    /// This operation is idempotent and blocks until finished.
    ///
    /// This is called during the `Stopping` resources status.
    async fn stop(&mut self, deployment_config: &serde_json::Value) -> Result<(), ManagerError>;

    /// Clears the storage resources.
    /// This operation is idempotent and blocks until finished.
    ///
    /// This is called during the `Clearing` storage status.
    async fn clear(&mut self, deployment_config: &serde_json::Value) -> Result<(), ManagerError>;

    /// Observes what exists for the pipeline now. Called by the runner's HTTP server, not by
    /// the automaton, so it has no `self`. Compute is expected to reach `compute_expected` and
    /// storage `storage_expected`. The default returns the spec only.
    async fn observe_deployment(
        _config: &Self::Config,
        _pipeline_id: PipelineId,
        deployment_config: Option<&serde_json::Value>,
        _compute_expected: DeploymentState,
        _storage_expected: DeploymentState,
    ) -> Result<PipelineDeployment, ManagerError> {
        Ok(PipelineDeployment {
            spec: deployment_spec(deployment_config),
            status: None,
        })
    }
}

/// The spec part of a deployment, from the stored `deployment_config`. `resources` that do not
/// parse give an empty spec: the config was validated when it was stored, so this only happens
/// after an incompatible change to `ResourceConfig`.
pub fn deployment_spec(deployment_config: Option<&serde_json::Value>) -> DeploymentSpec {
    let resources = deployment_config
        .and_then(|config| config.get("resources"))
        .and_then(|resources| ResourceConfig::deserialize(resources).ok())
        .unwrap_or_default();
    DeploymentSpec {
        resources: (&resources).into(),
    }
}

/// Discovers the namespace from a pipeline configuration JSON and returns it.
///
/// The configuration itself is not deserialized to avoid any future backward incompatible
/// changes to it from affecting the ability to find out the namespace. This is a utility function
/// for runners that make use of the namespace field.
pub fn discover_namespace_from_pipeline_config_json(config: &serde_json::Value) -> Option<String> {
    config.as_object().and_then(|m| {
        m.get("resources").and_then(|resources_value| {
            resources_value.as_object().and_then(|resources_obj| {
                resources_obj
                    .get("namespace")
                    .and_then(|v| v.as_str().map(|s| s.to_string()))
            })
        })
    })
}

#[cfg(test)]
mod test {
    use super::{deployment_spec, discover_namespace_from_pipeline_config_json};
    use crate::db::types::pipeline::PipelineId;
    use crate::db::types::program::generate_pipeline_config;
    use crate::db::types::utils::validate_runtime_config;
    use feldera_types::size::ConfigSize;
    use serde_json::json;
    use uuid::Uuid;

    /// This test should detect if ever the placement or type of the namespace in the runtime
    /// configuration is changed, as such a change could cause orphaned Kubernetes resources.
    #[test]
    pub fn test_namespace_discovery_in_runtime_config() {
        // Default
        let runtime_config = validate_runtime_config(&json!({}), false).unwrap();
        let value = serde_json::to_value(runtime_config).unwrap();
        assert_eq!(discover_namespace_from_pipeline_config_json(&value), None);

        // Set explicitly to `null`
        let runtime_config = validate_runtime_config(
            &json!({
                "resources": {
                    "namespace": null
                }
            }),
            false,
        )
        .unwrap();
        let value = serde_json::to_value(runtime_config).unwrap();
        assert_eq!(discover_namespace_from_pipeline_config_json(&value), None);

        // Set explicitly to `default`
        let runtime_config = validate_runtime_config(
            &json!({
                "resources": {
                    "namespace": "default"
                }
            }),
            false,
        )
        .unwrap();
        let value = serde_json::to_value(runtime_config).unwrap();
        assert_eq!(
            discover_namespace_from_pipeline_config_json(&value),
            Some("default".to_string())
        );

        // Set explicitly to `example123`
        let runtime_config = validate_runtime_config(
            &json!({
                "resources": {
                    "namespace": "example123"
                }
            }),
            false,
        )
        .unwrap();
        let value = serde_json::to_value(runtime_config).unwrap();
        assert_eq!(
            discover_namespace_from_pipeline_config_json(&value),
            Some("example123".to_string())
        );

        // Set explicitly to `feldera`
        let runtime_config = validate_runtime_config(
            &json!({
                "resources": {
                    "namespace": "feldera"
                }
            }),
            false,
        )
        .unwrap();
        let value = serde_json::to_value(runtime_config).unwrap();
        assert_eq!(
            discover_namespace_from_pipeline_config_json(&value),
            Some("feldera".to_string())
        );
    }

    /// `stop()` and `clear()` read the namespace from the stored deployment config, which is a
    /// serialized `PipelineConfig`, while `provision()` reads it from the typed one.
    #[test]
    pub fn test_namespace_discovery_in_pipeline_config() {
        let runtime_config = validate_runtime_config(
            &json!({
                "resources": {
                    "namespace": "example"
                }
            }),
            false,
        )
        .unwrap();
        let pipeline_config =
            generate_pipeline_config(PipelineId(Uuid::nil()), "example", &runtime_config);
        let value = serde_json::to_value(&pipeline_config).unwrap();
        assert_eq!(
            discover_namespace_from_pipeline_config_json(&value),
            pipeline_config.global.resources.namespace
        );
        assert_eq!(
            discover_namespace_from_pipeline_config_json(&value),
            Some("example".to_string())
        );
    }

    /// The spec reads the resources of the deployment config, which a resize changes.
    #[test]
    fn deployment_spec_reads_deployment_config_resources() {
        // A config kept from before 0.367.0 has the megabyte spelling.
        for resources in [
            json!({"cpu_cores_min": 2.0, "memory_max": "8000M", "storage_min": "1G"}),
            json!({"cpu_cores_min": 2.0, "memory_mb_max": 8000, "storage_mb_min": 1000}),
        ] {
            let config = json!({"workers": 4, "resources": resources});
            let resources = deployment_spec(Some(&config)).resources;
            assert_eq!(resources.cpu_cores_min, Some(2.0));
            assert_eq!(resources.memory_max, Some(ConfigSize::from_mb(8000)));
            assert_eq!(resources.storage_min, Some(ConfigSize::from_mb(1000)));
            assert_eq!(resources.cpu_cores_max, None);
        }

        assert_eq!(deployment_spec(None), Default::default());
        let malformed = json!({"resources": {"cpu_cores_min": "two"}});
        assert_eq!(deployment_spec(Some(&malformed)), Default::default());
    }
}
