use super::{
    AttachedConnector, ConfigDescr, ConfigId, ConnectorDescr, ConnectorId, ConnectorType, DBError,
    PipelineDescr, PipelineId, ProjectDescr, ProjectId, Version,
};
use crate::{Direction, ProjectStatus};
use anyhow::{anyhow, Result as AnyResult};
use async_trait::async_trait;

/// The storage trait contains the methods to interact with the pipeline manager
/// storage layer (e.g., PostgresDB) to implement the public API.
///
/// We use a trait so we can mock the storage layer in tests.
#[async_trait]
pub(crate) trait Storage {
    async fn reset_project_status(&self) -> AnyResult<()>;

    async fn list_projects(&self) -> AnyResult<Vec<ProjectDescr>>;

    /// Retrieve project descriptor.
    ///
    /// Returns a `DBError:UnknownProject` error if `project_id` is not found in
    /// the database.
    async fn get_project(&self, project_id: ProjectId) -> AnyResult<ProjectDescr> {
        self.get_project_if_exists(project_id)
            .await?
            .ok_or_else(|| anyhow!(DBError::UnknownProject(project_id)))
    }

    /// Validate project version and retrieve project descriptor.
    ///
    /// Returns `DBError::UnknownProject` if `project_id` is not found in the
    /// database. Returns `DBError::OutdatedProjectVersion` if the current
    /// project version differs from `expected_version`.
    async fn get_project_guarded(
        &self,
        project_id: ProjectId,
        expected_version: Version,
    ) -> AnyResult<ProjectDescr> {
        let descr = self.get_project(project_id).await?;
        if descr.version != expected_version {
            return Err(anyhow!(DBError::OutdatedProjectVersion(expected_version)));
        }

        Ok(descr)
    }

    /// Queue project for compilation by setting its status to
    /// [`ProjectStatus::Pending`].
    ///
    /// Change project status to [`ProjectStatus::Pending`].
    async fn set_project_pending(
        &self,
        project_id: ProjectId,
        expected_version: Version,
    ) -> AnyResult<()> {
        let descr = self
            .get_project_guarded(project_id, expected_version)
            .await?;

        // Do nothing if the project is already pending (we don't want to bump its
        // `status_since` field, which would move it to the end of the queue) or
        // if compilation is alread in progress.
        if descr.status == ProjectStatus::Pending || descr.status.is_compiling() {
            return Ok(());
        }

        self.set_project_status(project_id, ProjectStatus::Pending)
            .await?;

        Ok(())
    }

    /// Cancel compilation request.
    ///
    /// Cancels compilation request if the project is pending in the queue
    /// or already being compiled.
    async fn cancel_project(
        &self,
        project_id: ProjectId,
        expected_version: Version,
    ) -> AnyResult<()> {
        let descr = self
            .get_project_guarded(project_id, expected_version)
            .await?;

        if descr.status != ProjectStatus::Pending || !descr.status.is_compiling() {
            return Ok(());
        }

        self.set_project_status(project_id, ProjectStatus::None)
            .await?;

        Ok(())
    }

    /// Retrieve code of the specified project along with the project's
    /// meta-data.
    async fn project_code(&self, project_id: ProjectId) -> AnyResult<(ProjectDescr, String)>;

    /// Create a new project.
    async fn new_project(
        &self,
        project_name: &str,
        project_description: &str,
        project_code: &str,
    ) -> AnyResult<(ProjectId, Version)>;

    /// Update project name, description and, optionally, code.
    /// XXX: Description should be optional too
    async fn update_project(
        &self,
        project_id: ProjectId,
        project_name: &str,
        project_description: &str,
        project_code: &Option<String>,
    ) -> AnyResult<Version>;

    /// Retrieve project descriptor.
    ///
    /// Returns `None` if `project_id` is not found in the database.
    async fn get_project_if_exists(&self, project_id: ProjectId)
        -> AnyResult<Option<ProjectDescr>>;

    /// Lookup project by name.
    async fn lookup_project(&self, project_name: &str) -> AnyResult<Option<ProjectDescr>>;

    /// Update project status.
    ///
    /// # Note
    /// - Doesn't check that the project exists.
    /// - Resets schema to null.
    async fn set_project_status(
        &self,
        project_id: ProjectId,
        status: ProjectStatus,
    ) -> AnyResult<()>;

    /// Update project status after a version check.
    ///
    /// Updates project status to `status` if the current project version in the
    /// database matches `expected_version`.
    ///
    /// # Note
    /// This intentionally does not throw an error if there is a project version
    /// mismatch and instead does just not update. It's used by the compiler to
    /// update status and in case there is a newer version it is expected that
    /// the compiler just picks up and runs the next job.
    async fn set_project_status_guarded(
        &self,
        project_id: ProjectId,
        expected_version: Version,
        status: ProjectStatus,
    ) -> AnyResult<()>;

    /// Update project schema.
    ///
    /// # Note
    /// This should be called after the SQL compilation succeeded, e.g., in the
    /// same transaction that sets status to  [`ProjectStatus::CompilingRust`].
    async fn set_project_schema(&self, project_id: ProjectId, schema: String) -> AnyResult<()>;

    /// Delete project from the database.
    ///
    /// This will delete all project configs and pipelines.
    async fn delete_project(&self, project_id: ProjectId) -> AnyResult<()>;

    /// Retrieves the first pending project from the queue.
    ///
    /// Returns a pending project with the most recent `status_since` or `None`
    /// if there are no pending projects in the DB.
    async fn next_job(&self) -> AnyResult<Option<(ProjectId, Version)>>;

    async fn list_configs(&self) -> AnyResult<Vec<ConfigDescr>>;

    async fn get_config(&self, config_id: ConfigId) -> AnyResult<ConfigDescr>;

    /// Create a new project config.
    async fn new_config(
        &self,
        project_id: Option<ProjectId>,
        config_name: &str,
        config_description: &str,
        config: &str,
        connectors: &Option<Vec<AttachedConnector>>,
    ) -> AnyResult<(ConfigId, Version)>;

    /// Add pipeline to the config.
    async fn add_pipeline_to_config(
        &self,
        config_id: ConfigId,
        pipeline_id: PipelineId,
    ) -> AnyResult<()>;

    /// Remove pipeline from the config.
    async fn remove_pipeline_from_config(&self, config_id: ConfigId) -> AnyResult<()>;

    /// Update existing project config.
    ///
    /// Update config name and, optionally, YAML.
    async fn update_config(
        &self,
        config_id: ConfigId,
        project_id: Option<ProjectId>,
        config_name: &str,
        config_description: &str,
        config: &Option<String>,
        connectors: &Option<Vec<AttachedConnector>>,
    ) -> AnyResult<Version>;

    /// Delete project config.
    async fn delete_config(&self, config_id: ConfigId) -> AnyResult<()>;

    /// Get an attached connector.
    async fn get_attached_connector_direction(&self, uuid: &str) -> AnyResult<Direction>;

    /// Insert a new record to the `pipeline` table.
    async fn new_pipeline(
        &self,
        config_id: ConfigId,
        config_version: Version,
    ) -> AnyResult<PipelineId>;

    async fn pipeline_set_port(&self, pipeline_id: PipelineId, port: u16) -> AnyResult<()>;

    /// Set `shutdown` flag to `true`.
    async fn set_pipeline_shutdown(&self, pipeline_id: PipelineId) -> AnyResult<bool>;

    /// Delete `pipeline` from the DB.
    async fn delete_pipeline(&self, pipeline_id: PipelineId) -> AnyResult<bool>;

    /// Retrieve project config.
    async fn get_pipeline(&self, pipeline_id: PipelineId) -> AnyResult<PipelineDescr>;

    /// List pipelines associated with `project_id`.
    async fn list_pipelines(&self) -> AnyResult<Vec<PipelineDescr>>;

    /// Create a new connector.
    async fn new_connector(
        &self,
        name: &str,
        description: &str,
        typ: ConnectorType,
        config: &str,
    ) -> AnyResult<ConnectorId>;

    /// Retrieve connectors list from the DB.
    async fn list_connectors(&self) -> AnyResult<Vec<ConnectorDescr>>;

    /// Retrieve connector descriptor.
    async fn get_connector(&self, connector_id: ConnectorId) -> AnyResult<ConnectorDescr>;

    /// Update existing connector config.
    ///
    /// Update connector name and, optionally, YAML.
    async fn update_connector(
        &self,
        connector_id: ConnectorId,
        connector_name: &str,
        description: &str,
        config: &Option<String>,
    ) -> AnyResult<()>;

    /// Delete connector from the database.
    ///
    /// This will delete all connector configs and pipelines.
    async fn delete_connector(&self, connector_id: ConnectorId) -> AnyResult<()>;
}
