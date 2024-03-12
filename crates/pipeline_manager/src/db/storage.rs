use super::{
    ApiKeyDescr, ApiPermission, AttachedConnector, ConnectorDescr, ConnectorId, DBError, Pipeline,
    PipelineDescr, PipelineId, PipelineRevision, PipelineRuntimeState, PipelineStatus,
    ProgramDescr, ProgramId, Revision, Version,
};
use crate::api::ProgramStatus;
use crate::auth::TenantId;
use crate::compiler::ProgramConfig;
use crate::db::service::{ServiceProbeDescr, ServiceProbeId};
use crate::db::{ServiceDescr, ServiceId};
use crate::prober::service::{ServiceProbeRequest, ServiceProbeResponse, ServiceProbeType};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use deadpool_postgres::Transaction;
use pipeline_types::service::ServiceConfig;
use pipeline_types::{
    config::{ConnectorConfig, RuntimeConfig},
    program_schema::ProgramSchema,
};
use uuid::Uuid;

/// The storage trait contains the methods to interact with the pipeline manager
/// storage layer (e.g., PostgresDB) to implement the public API.
///
/// We use a trait so we can mock the storage layer in tests.
#[async_trait]
pub(crate) trait Storage {
    async fn list_programs(
        &self,
        tenant_id: TenantId,
        with_code: bool,
    ) -> Result<Vec<ProgramDescr>, DBError>;

    /// Update program schema.
    ///
    /// # Note
    /// This should be called after the SQL compilation succeeded, e.g., in the
    /// same transaction that sets status to  [`ProgramStatus::CompilingRust`].
    async fn set_program_schema(
        &self,
        tenant_id: TenantId,
        program_id: ProgramId,
        schema: ProgramSchema,
    ) -> Result<(), DBError> {
        self.update_program(
            tenant_id,
            program_id,
            &None,
            &None,
            &None,
            &None,
            &Some(schema),
            &None,
            None,
            None,
        )
        .await?;
        Ok(())
    }

    /// Update program status after a version check.
    ///
    /// Updates program status to `status` if the current program version in the
    /// database matches `expected_version`. Setting the status to
    /// `ProgramStatus::Pending` resets the schema and is used to queue the
    /// program for compilation.
    async fn set_program_status_guarded(
        &self,
        tenant_id: TenantId,
        program_id: ProgramId,
        expected_version: Version,
        status: ProgramStatus,
    ) -> Result<(), DBError> {
        self.update_program(
            tenant_id,
            program_id,
            &None,
            &None,
            &None,
            &Some(status),
            &None,
            &None,
            Some(expected_version),
            None,
        )
        .await?;
        Ok(())
    }

    /// Create a new program.
    #[allow(clippy::too_many_arguments)]
    async fn new_program(
        &self,
        tenant_id: TenantId,
        id: Uuid,
        program_name: &str,
        program_description: &str,
        program_code: &str,
        program_config: &ProgramConfig,
        txn: Option<&Transaction<'_>>,
    ) -> Result<(ProgramId, Version), DBError>;

    /// Update program name, description and, optionally, code.
    #[allow(clippy::too_many_arguments)]
    async fn update_program(
        &self,
        tenant_id: TenantId,
        program_id: ProgramId,
        program_name: &Option<String>,
        program_description: &Option<String>,
        program_code: &Option<String>,
        status: &Option<ProgramStatus>,
        schema: &Option<ProgramSchema>,
        config: &Option<ProgramConfig>,
        guard: Option<Version>,
        txn: Option<&Transaction<'_>>,
    ) -> Result<Version, DBError>;

    /// Retrieve program descriptor.
    ///
    /// Returns `None` if `program_id` is not found in the database.
    async fn get_program_by_id(
        &self,
        tenant_id: TenantId,
        program_id: ProgramId,
        with_code: bool,
    ) -> Result<ProgramDescr, DBError>;

    /// Lookup program by name.
    async fn get_program_by_name(
        &self,
        tenant_id: TenantId,
        program_name: &str,
        with_code: bool,
        txn: Option<&Transaction<'_>>,
    ) -> Result<ProgramDescr, DBError>;

    /// Delete program from the database.
    async fn delete_program(&self, tenant_id: TenantId, program_name: &str) -> Result<(), DBError>;

    /// Retrieves all programs in the DB. Intended to be used by
    /// reconciliation loops.
    async fn all_programs(&self) -> Result<Vec<(TenantId, ProgramDescr)>, DBError>;

    /// Retrieves all pipelines in the DB. Intended to be used by
    /// reconciliation loops.
    async fn all_pipelines(&self) -> Result<Vec<(TenantId, PipelineId)>, DBError>;

    /// Retrieves the first pending program from the queue.
    ///
    /// Returns a pending program with the most recent `status_since` or `None`
    /// if there are no pending programs in the DB.
    async fn next_job(&self) -> Result<Option<(TenantId, ProgramId, Version)>, DBError>;

    /// Create a pipeline deployment, which is an immutable and complete
    /// configuration for running a pipeline.
    ///
    /// Returns the deployment ID for the pipeline.
    async fn create_pipeline_deployment(
        &self,
        deployment_id: Uuid,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
    ) -> Result<Revision, DBError>;

    /// Retrieves the deployment for a pipeline, if created
    async fn get_pipeline_deployment(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
    ) -> Result<PipelineRevision, DBError>;

    /// Create a new config.
    #[allow(clippy::too_many_arguments)]
    async fn new_pipeline(
        &self,
        tenant_id: TenantId,
        id: Uuid,
        program_name: &Option<String>,
        pipline_name: &str,
        pipeline_description: &str,
        config: &RuntimeConfig,
        connectors: &Option<Vec<AttachedConnector>>,
        txn: Option<&Transaction<'_>>,
    ) -> Result<(PipelineId, Version), DBError>;

    /// Update existing config.
    ///
    /// Update config name and, optionally, YAML.
    #[allow(clippy::too_many_arguments)]
    async fn update_pipeline(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        program_id: &Option<String>,
        pipline_name: &str,
        pipeline_description: &str,
        config: &Option<RuntimeConfig>,
        connectors: &Option<Vec<AttachedConnector>>,
        txn: Option<&Transaction<'_>>,
    ) -> Result<Version, DBError>;

    /// Get input/output status for an attached connector.
    async fn attached_connector_is_input(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        name: &str,
    ) -> Result<bool, DBError>;

    /// Delete pipeline from the DB.
    async fn delete_pipeline(
        &self,
        tenant_id: TenantId,
        pipeline_name: &str,
    ) -> Result<(), DBError>;

    /// Retrieve pipeline for a given id.
    async fn get_pipeline_descr_by_id(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        txn: Option<&Transaction<'_>>,
    ) -> Result<PipelineDescr, DBError>;

    async fn get_pipeline_by_id(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
    ) -> Result<Pipeline, DBError>;

    /// Retrieve pipeline for a given name.
    async fn get_pipeline_descr_by_name(
        &self,
        tenant_id: TenantId,
        name: &str,
        txn: Option<&Transaction<'_>>,
    ) -> Result<PipelineDescr, DBError>;

    async fn get_pipeline_by_name(
        &self,
        tenant_id: TenantId,
        name: &str,
    ) -> Result<Pipeline, DBError>;

    async fn get_pipeline_runtime_state_by_id(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
    ) -> Result<PipelineRuntimeState, DBError>;

    async fn get_pipeline_runtime_state_by_name(
        &self,
        tenant_id: TenantId,
        pipeline_name: &str,
    ) -> Result<PipelineRuntimeState, DBError>;

    async fn update_pipeline_runtime_state(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        state: &PipelineRuntimeState,
    ) -> Result<(), DBError>;

    async fn set_pipeline_desired_status(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        desired_status: PipelineStatus,
    ) -> Result<(), DBError>;

    async fn list_pipelines(&self, tenant_id: TenantId) -> Result<Vec<Pipeline>, DBError>;

    /// Create a new connector.
    async fn new_connector(
        &self,
        tenant_id: TenantId,
        id: Uuid,
        name: &str,
        description: &str,
        config: &ConnectorConfig,
        txn: Option<&Transaction<'_>>,
    ) -> Result<ConnectorId, DBError>;

    /// Retrieve connectors list from the DB.
    async fn list_connectors(&self, tenant_id: TenantId) -> Result<Vec<ConnectorDescr>, DBError>;

    /// Retrieve connector descriptor for the given `connector_id`.
    async fn get_connector_by_id(
        &self,
        tenant_id: TenantId,
        connector_id: ConnectorId,
    ) -> Result<ConnectorDescr, DBError>;

    /// Retrieve connector descriptor for the given `name`.
    async fn get_connector_by_name(
        &self,
        tenant_id: TenantId,
        name: &str,
        txn: Option<&Transaction<'_>>,
    ) -> Result<ConnectorDescr, DBError>;

    /// Updates the name, description and/or configuration of the existing
    /// connector, of which the identifier is provided. If new values are not
    /// provided, the existing values in storage are kept.
    ///
    /// Returns error if there does not exist a connector with the provided
    /// identifier.
    async fn update_connector(
        &self,
        tenant_id: TenantId,
        connector_id: ConnectorId,
        connector_name: &Option<&str>,
        description: &Option<&str>,
        config: &Option<ConnectorConfig>,
        txn: Option<&Transaction<'_>>,
    ) -> Result<(), DBError>;

    /// Delete connector from the database.
    ///
    /// This will delete all connector configs and pipelines.
    async fn delete_connector(
        &self,
        tenant_id: TenantId,
        connector_name: &str,
    ) -> Result<(), DBError>;

    /// Get a list of API key names
    async fn list_api_keys(&self, tenant_id: TenantId) -> Result<Vec<ApiKeyDescr>, DBError>;

    /// Get an API key by name
    async fn get_api_key(&self, tenant_id: TenantId, name: &str) -> Result<ApiKeyDescr, DBError>;

    /// Delete an API key by name
    async fn delete_api_key(&self, tenant_id: TenantId, name: &str) -> Result<(), DBError>;

    /// Persist an SHA-256 hash of an API key in the database
    async fn store_api_key_hash(
        &self,
        tenant_id: TenantId,
        id: Uuid,
        name: &str,
        key: &str,
        permissions: Vec<ApiPermission>,
    ) -> Result<(), DBError>;

    /// Validate an API key against the database by comparing its SHA-256 hash
    /// against the stored value.
    async fn validate_api_key(&self, key: &str) -> Result<(TenantId, Vec<ApiPermission>), DBError>;

    /// Get the tenant ID from the database for a given tenant name and
    /// provider, else create a new tenant ID
    async fn get_or_create_tenant_id(
        &self,
        tenant_name: String,
        provider: String,
    ) -> Result<TenantId, DBError>;

    /// Create a new tenant ID for a given tenant name and provider
    async fn create_tenant_if_not_exists(
        &self,
        tenant_id: Uuid,
        tenant_name: String,
        provider: String,
    ) -> Result<TenantId, DBError>;

    /// Record a URL pointing to a compile. binary Supported URL types are
    /// determined by the compiler service (e.g. file:///)
    async fn create_compiled_binary_ref(
        &self,
        program_id: ProgramId,
        version: Version,
        url: String,
    ) -> Result<(), DBError>;

    /// Retrieve a compiled binary's URL
    async fn get_compiled_binary_ref(
        &self,
        program_id: ProgramId,
        version: Version,
    ) -> Result<Option<String>, DBError>;

    /// Retrieve a compiled binary's URL
    async fn delete_compiled_binary_ref(
        &self,
        program_id: ProgramId,
        version: Version,
    ) -> Result<(), DBError>;

    /// Creates a new service.
    ///
    /// Returns error if there already exists a service with the given
    /// identifier or name.
    async fn new_service(
        &self,
        tenant_id: TenantId,
        id: Uuid,
        name: &str,
        description: &str,
        config: &ServiceConfig,
        txn: Option<&Transaction<'_>>,
    ) -> Result<ServiceId, DBError>;

    /// Retrieves a list of all services of a tenant.
    /// Optionally, filtered by service configuration type.
    async fn list_services(
        &self,
        tenant_id: TenantId,
        filter_config_type: &Option<&str>,
    ) -> Result<Vec<ServiceDescr>, DBError>;

    /// Retrieves service descriptor for the given `service_id`.
    ///
    /// Returns error if there does not exist a service with the provided
    /// identifier.
    async fn get_service_by_id(
        &self,
        tenant_id: TenantId,
        service_id: ServiceId,
        txn: Option<&Transaction<'_>>,
    ) -> Result<ServiceDescr, DBError>;

    /// Retrieves service descriptor for the given unique service `name`.
    ///
    /// Returns error if there does not exist a service with the provided name.
    async fn get_service_by_name(
        &self,
        tenant_id: TenantId,
        name: &str,
        txn: Option<&Transaction<'_>>,
    ) -> Result<ServiceDescr, DBError>;

    /// Updates the name, description and/or configuration of the existing
    /// service, of which the identifier is provided. If new values are not
    /// provided, the existing values in storage are kept.
    ///
    /// Returns error if there does not exist a service with the provided
    /// identifier.
    async fn update_service(
        &self,
        tenant_id: TenantId,
        service_id: ServiceId,
        name: &Option<&str>,
        description: &Option<&str>,
        config: &Option<ServiceConfig>,
        txn: Option<&Transaction<'_>>,
    ) -> Result<(), DBError>;

    /// Deletes the service by its provided name.
    ///
    /// Returns error if there does not exist a service with the provided name.
    async fn delete_service(&self, tenant_id: TenantId, service_name: &str) -> Result<(), DBError>;

    /// Creates a new service probe.
    ///
    /// Returns error if the tenant or service referenced by identifier does not
    /// exist.
    async fn new_service_probe(
        &self,
        tenant_id: TenantId,
        service_id: ServiceId,
        id: Uuid,
        request: ServiceProbeRequest,
        created_at: &DateTime<Utc>,
        txn: Option<&Transaction<'_>>,
    ) -> Result<ServiceProbeId, DBError>;

    /// Sets the status of an existing service probe to running and saves the
    /// starting timestamp.
    ///
    /// Returns error if the service_probe_id does not exist.
    async fn update_service_probe_set_running(
        &self,
        tenant_id: TenantId,
        service_probe_id: ServiceProbeId,
        started_at: &DateTime<Utc>,
    ) -> Result<(), DBError>;

    /// Sets the status of an existing service probe to either succeeded or
    /// failed based on the success,and saves the response and finishing
    /// timestamp.
    ///
    /// Returns error if the service_probe_id does not exist.
    async fn update_service_probe_set_finished(
        &self,
        tenant_id: TenantId,
        service_probe_id: ServiceProbeId,
        response: ServiceProbeResponse,
        finished_at: &DateTime<Utc>,
    ) -> Result<(), DBError>;

    /// Retrieves the next service probe to perform.
    async fn next_service_probe(
        &self,
    ) -> Result<Option<(ServiceProbeId, TenantId, ServiceProbeRequest, ServiceConfig)>, DBError>;

    /// Retrieves service probe by its identifier.
    async fn get_service_probe(
        &self,
        tenant_id: TenantId,
        service_id: ServiceId,
        service_probe_id: ServiceProbeId,
        txn: Option<&Transaction<'_>>,
    ) -> Result<ServiceProbeDescr, DBError>;

    /// Retrieves a list of all probes of a service.
    /// Optionally, limited to a specific number of the most recent probes.
    /// Optionally, filtered to a specific request type.
    async fn list_service_probes(
        &self,
        tenant_id: TenantId,
        service_id: ServiceId,
        limit: Option<u32>,
        probe_type: Option<ServiceProbeType>,
        txn: Option<&Transaction<'_>>,
    ) -> Result<Vec<ServiceProbeDescr>, DBError>;

    /// Check connectivity to the DB
    async fn check_connection(&self) -> Result<(), DBError>;
}
