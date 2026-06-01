use crate::config::CommonConfig;
use crate::db::types::pipeline::PipelineId;
use crate::db::types::version::Version;
use crate::error::ManagerError;
use crate::runner::pipeline_logs::LogsSender;
use async_trait::async_trait;
use feldera_types::config::{PipelineConfig, StorageConfig};
use feldera_types::runtime_status::{BootstrapConfig, RuntimeDesiredStatus};
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
    type Config: Clone;

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
    async fn can_provision(
        &self,
        deployment_config: &PipelineConfig,
        runtime_config: &serde_json::Value,
    ) -> Result<(), ManagerError>;

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
        runtime_config: &serde_json::Value,
    ) -> Result<(), ManagerError>;

    /// Validates whether the compute and possibly storage resources provisioning
    /// initiated by `provision()` is completed. An error is returned only if the resources
    /// encountered an irrecoverable failure.
    ///
    /// This is called during the `Provisioning` resources status.
    async fn is_provisioned(
        &mut self,
        runtime_config: &serde_json::Value,
    ) -> Result<ProvisionStatus, ManagerError>;

    /// Checks the healthiness of the pipeline compute and storage resources.
    /// An error is returned only if the resources encountered an irrecoverable failure.
    ///
    /// This is called during the `Provisioned` resources status.
    async fn check(
        &mut self,
        runtime_config: &serde_json::Value,
    ) -> Result<serde_json::Value, ManagerError>;

    /// Scales down to zero or deallocates the compute resources, but retains storage resources.
    /// This operation is idempotent and blocks until finished.
    ///
    /// This is called during the `Stopping` resources status.
    async fn stop(&mut self, runtime_config: &serde_json::Value) -> Result<(), ManagerError>;

    /// Clears the storage resources.
    /// This operation is idempotent and blocks until finished.
    ///
    /// This is called during the `Clearing` storage status.
    async fn clear(&mut self, runtime_config: &serde_json::Value) -> Result<(), ManagerError>;
}

/// Discovers the namespace from the runtime configuration JSON and returns it.
///
/// The runtime configuration itself is not deserialized to avoid any future backward incompatible
/// changes to it from affecting the ability to find out the namespace. This is a utility function
/// for runners that make use of the namespace field.
pub fn discover_namespace_from_runtime_config_json(
    runtime_config: &serde_json::Value,
) -> Option<String> {
    runtime_config.as_object().and_then(|m| {
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
    use super::discover_namespace_from_runtime_config_json;
    use crate::db::types::utils::validate_runtime_config;
    use serde_json::json;

    /// This test should detect if ever the placement or type of the namespace in the runtime
    /// configuration is changed, as such a change could cause orphaned Kubernetes resources.
    #[test]
    pub fn test_namespace_discovery_in_runtime_config() {
        // Default
        let runtime_config = validate_runtime_config(&json!({}), false).unwrap();
        let value = serde_json::to_value(runtime_config).unwrap();
        assert_eq!(discover_namespace_from_runtime_config_json(&value), None);

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
        assert_eq!(discover_namespace_from_runtime_config_json(&value), None);

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
            discover_namespace_from_runtime_config_json(&value),
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
            discover_namespace_from_runtime_config_json(&value),
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
            discover_namespace_from_runtime_config_json(&value),
            Some("feldera".to_string())
        );
    }
}
