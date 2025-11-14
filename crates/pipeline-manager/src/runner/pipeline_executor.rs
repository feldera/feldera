use crate::config::CommonConfig;
use crate::db::types::pipeline::PipelineId;
use crate::db::types::version::Version;
use crate::error::ManagerError;
use crate::runner::pipeline_logs::LogsSender;
use async_trait::async_trait;
use feldera_types::config::{PipelineConfig, StorageConfig};
use feldera_types::runtime_status::{BootstrapPolicy, RuntimeDesiredStatus};
use std::time::Duration;
use uuid::Uuid;

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

    /// Provisions compute and (optionally) storage resources.
    ///
    /// The provisioned resources are uniquely identifiable/addressable through the pipeline
    /// identifier, such that no runtime state is needed to manage them (as the runner can restart
    /// while the pipeline is running). This operation is idempotent and as non-blocking as possible
    /// such that the user can stop the provisioning. After calling this once, completion should be
    /// checked using `is_provisioned()`.
    #[allow(clippy::too_many_arguments)]
    async fn provision(
        &mut self,
        deployment_initial: RuntimeDesiredStatus,
        bootstrap_policy: Option<BootstrapPolicy>,
        deployment_id: &Uuid,
        deployment_config: &PipelineConfig,
        program_binary_url: &str,
        program_version: Version,
        suspend_info: Option<serde_json::Value>,
    ) -> Result<(), ManagerError>;

    /// Validates whether the compute and possibly storage resources provisioning
    /// initiated by `provision()` is completed.
    ///
    /// Returns:
    /// - `Ok(Some(deployment_location))` if completed successfully
    /// - `Ok(None)` if still ongoing
    /// - `Err(...)` if resources encountered an irrecoverable failure
    async fn is_provisioned(&mut self) -> Result<Option<String>, ManagerError>;

    /// Checks the healthiness of the pipeline compute and storage resources.
    /// An error is returned only if the resources encountered an irrecoverable failure.
    async fn check(&mut self) -> Result<(), ManagerError>;

    /// Scales down to zero or deallocates the compute resources, but retains storage resources.
    /// This operation is idempotent and blocks until finished.
    async fn stop(&mut self) -> Result<(), ManagerError>;

    /// Clears the storage resources.
    /// This operation is idempotent and blocks until finished.
    async fn clear(&mut self) -> Result<(), ManagerError>;
}
