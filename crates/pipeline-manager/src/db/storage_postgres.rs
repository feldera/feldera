#[cfg(feature = "postgresql_embedded")]
use crate::config::PgEmbedConfig;
use crate::db::error::DBError;
use crate::db::operations;
#[cfg(feature = "postgresql_embedded")]
use crate::db::pg_setup;
use crate::db::storage::{ExtendedPipelineDescrRunner, Storage};
use crate::db::types::api_key::{ApiKeyDescr, ApiPermission};
use crate::db::types::pipeline::{
    ExtendedPipelineDescr, ExtendedPipelineDescrMonitoring, PipelineDescr, PipelineDesiredStatus,
    PipelineId, PipelineStatus,
};
use crate::db::types::program::{
    ProgramConfig, ProgramInfo, ProgramStatus, RustCompilationInfo, SqlCompilationInfo,
};
use crate::db::types::tenant::TenantId;
use crate::db::types::version::Version;
use crate::{auth::TenantRecord, config::DatabaseConfig};
use async_trait::async_trait;
use deadpool_postgres::{Manager, Pool, RecyclingMethod};
use feldera_types::config::{PipelineConfig, RuntimeConfig};
use feldera_types::error::ErrorResponse;
use log::{debug, info, log, Level};
use tokio_postgres::Row;
use uuid::Uuid;

mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("./migrations/");
}

/// Postgres implementation of the `Storage` trait.
/// A trait method implementation consists of a single transaction
/// in which database operations are executed.
pub struct StoragePostgres {
    /// DatabaseConfig cmd line arguments.
    pub db_config: DatabaseConfig,
    /// Postgres configuration.
    pub config: tokio_postgres::Config,
    /// Pool from which clients can be taken.
    pub(crate) pool: Pool,
    /// Embedded Postgres instance which is used in development mode.
    /// It will not be persisted after termination.
    #[cfg(feature = "postgresql_embedded")]
    #[allow(dead_code)]
    // It has to stay alive until StoragePostgres is dropped.
    pub(crate) pg_inst: Option<postgresql_embedded::PostgreSQL>,
}

#[async_trait]
impl Storage for StoragePostgres {
    async fn check_connection(&self) -> Result<(), DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        operations::connectivity::check_connection(&txn).await?;
        txn.commit().await?;
        Ok(())
    }

    async fn get_or_create_tenant_id(
        &self,
        new_id: Uuid,
        name: String,
        provider: String,
    ) -> Result<TenantId, DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        let tenant_id =
            operations::tenant::get_or_create_tenant_id(&txn, new_id, name, provider).await?;
        txn.commit().await?;
        Ok(tenant_id)
    }

    async fn list_api_keys(&self, tenant_id: TenantId) -> Result<Vec<ApiKeyDescr>, DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        let api_keys = operations::api_key::list_api_keys(&txn, tenant_id).await?;
        txn.commit().await?;
        Ok(api_keys)
    }

    async fn get_api_key(&self, tenant_id: TenantId, name: &str) -> Result<ApiKeyDescr, DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        let api_key = operations::api_key::get_api_key(&txn, tenant_id, name).await?;
        txn.commit().await?;
        Ok(api_key)
    }

    async fn delete_api_key(&self, tenant_id: TenantId, name: &str) -> Result<(), DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        let result = operations::api_key::delete_api_key(&txn, tenant_id, name).await?;
        txn.commit().await?;
        Ok(result)
    }

    async fn store_api_key_hash(
        &self,
        tenant_id: TenantId,
        id: Uuid,
        name: &str,
        key: &str,
        permissions: Vec<ApiPermission>,
    ) -> Result<(), DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        let result =
            operations::api_key::store_api_key_hash(&txn, tenant_id, id, name, key, permissions)
                .await?;
        txn.commit().await?;
        Ok(result)
    }

    async fn validate_api_key(&self, key: &str) -> Result<(TenantId, Vec<ApiPermission>), DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        let result = operations::api_key::validate_api_key(&txn, key).await?;
        txn.commit().await?;
        Ok(result)
    }

    async fn list_pipelines(
        &self,
        tenant_id: TenantId,
    ) -> Result<Vec<ExtendedPipelineDescr>, DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        let pipelines = operations::pipeline::list_pipelines(&txn, tenant_id).await?;
        txn.commit().await?;
        Ok(pipelines)
    }

    async fn list_pipelines_for_monitoring(
        &self,
        tenant_id: TenantId,
    ) -> Result<Vec<ExtendedPipelineDescrMonitoring>, DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        let pipelines =
            operations::pipeline::list_pipelines_for_monitoring(&txn, tenant_id).await?;
        txn.commit().await?;
        Ok(pipelines)
    }

    async fn get_pipeline(
        &self,
        tenant_id: TenantId,
        name: &str,
    ) -> Result<ExtendedPipelineDescr, DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        let pipeline = operations::pipeline::get_pipeline(&txn, tenant_id, name).await?;
        txn.commit().await?;
        Ok(pipeline)
    }

    async fn get_pipeline_for_monitoring(
        &self,
        tenant_id: TenantId,
        name: &str,
    ) -> Result<ExtendedPipelineDescrMonitoring, DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        let pipeline =
            operations::pipeline::get_pipeline_for_monitoring(&txn, tenant_id, name).await?;
        txn.commit().await?;
        Ok(pipeline)
    }

    async fn get_pipeline_by_id(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
    ) -> Result<ExtendedPipelineDescr, DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        let pipeline =
            operations::pipeline::get_pipeline_by_id(&txn, tenant_id, pipeline_id).await?;
        txn.commit().await?;
        Ok(pipeline)
    }

    async fn get_pipeline_by_id_for_monitoring(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
    ) -> Result<ExtendedPipelineDescrMonitoring, DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        let pipeline =
            operations::pipeline::get_pipeline_by_id_for_monitoring(&txn, tenant_id, pipeline_id)
                .await?;
        txn.commit().await?;
        Ok(pipeline)
    }

    async fn get_pipeline_by_id_for_runner(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        platform_version: &str,
        provision_called: bool,
    ) -> Result<ExtendedPipelineDescrRunner, DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        let pipeline_monitoring =
            operations::pipeline::get_pipeline_by_id_for_monitoring(&txn, tenant_id, pipeline_id)
                .await?;
        let is_ready_compiled = pipeline_monitoring.program_status == ProgramStatus::Success
            && pipeline_monitoring.platform_version == platform_version;
        let pipeline_result = if matches!(
            (
                pipeline_monitoring.deployment_status,
                pipeline_monitoring.deployment_desired_status,
                is_ready_compiled,
                provision_called
            ),
            (
                PipelineStatus::Shutdown | PipelineStatus::Suspended,
                PipelineDesiredStatus::Paused | PipelineDesiredStatus::Running,
                true,
                _
            ) | (
                PipelineStatus::Provisioning,
                PipelineDesiredStatus::Paused | PipelineDesiredStatus::Running,
                _,
                false
            ) | (
                PipelineStatus::Paused | PipelineStatus::Running | PipelineStatus::Unavailable,
                PipelineDesiredStatus::Suspended,
                _,
                _
            ),
        ) {
            ExtendedPipelineDescrRunner::Complete(
                operations::pipeline::get_pipeline_by_id(&txn, tenant_id, pipeline_id).await?,
            )
        } else {
            ExtendedPipelineDescrRunner::Monitoring(pipeline_monitoring)
        };
        txn.commit().await?;
        Ok(pipeline_result)
    }

    async fn new_pipeline(
        &self,
        tenant_id: TenantId,
        new_id: Uuid,
        platform_version: &str,
        pipeline: PipelineDescr,
    ) -> Result<ExtendedPipelineDescr, DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;

        // Create new pipeline
        operations::pipeline::new_pipeline(
            &txn,
            tenant_id,
            new_id,
            platform_version,
            pipeline.clone(),
        )
        .await?;

        // Fetch newly created pipeline
        let extended_pipeline =
            operations::pipeline::get_pipeline(&txn, tenant_id, &pipeline.name).await?;

        txn.commit().await?;
        Ok(extended_pipeline)
    }

    async fn new_or_update_pipeline(
        &self,
        tenant_id: TenantId,
        new_id: Uuid,
        original_name: &str,
        platform_version: &str,
        pipeline: PipelineDescr,
    ) -> Result<(bool, ExtendedPipelineDescr), DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;

        // Check if pipeline exists
        let current = operations::pipeline::get_pipeline(&txn, tenant_id, original_name).await;
        let is_new: bool = match current {
            Ok(_) => {
                // Pipeline already exists, as such update it
                operations::pipeline::update_pipeline(
                    &txn,
                    false, // Done by user
                    tenant_id,
                    original_name,
                    &Some(pipeline.name.clone()),
                    &Some(pipeline.description.clone()),
                    platform_version,
                    &Some(pipeline.runtime_config.clone()),
                    &Some(pipeline.program_code.clone()),
                    &Some(pipeline.udf_rust.clone()),
                    &Some(pipeline.udf_toml.clone()),
                    &Some(pipeline.program_config.clone()),
                )
                .await?;
                false
            }
            Err(DBError::UnknownPipelineName { .. }) => {
                // Pipeline does not yet exist, as such create it
                if original_name != pipeline.name {
                    return Err(DBError::CannotRenameNonExistingPipeline);
                }
                operations::pipeline::new_pipeline(
                    &txn,
                    tenant_id,
                    new_id,
                    platform_version,
                    pipeline.clone(),
                )
                .await?;
                true
            }
            Err(e) => {
                // Another error occurred when fetching the extended pipeline,
                // return that if that is the case
                return Err(e);
            }
        };

        // Fetch new or updated pipeline
        let extended_pipeline =
            operations::pipeline::get_pipeline(&txn, tenant_id, &pipeline.name).await?;

        txn.commit().await?;
        Ok((is_new, extended_pipeline))
    }

    #[allow(clippy::too_many_arguments)]
    async fn update_pipeline(
        &self,
        tenant_id: TenantId,
        original_name: &str,
        name: &Option<String>,
        description: &Option<String>,
        platform_version: &str,
        runtime_config: &Option<serde_json::Value>,
        program_code: &Option<String>,
        udf_rust: &Option<String>,
        udf_toml: &Option<String>,
        program_config: &Option<serde_json::Value>,
    ) -> Result<ExtendedPipelineDescr, DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;

        // Update existing pipeline
        operations::pipeline::update_pipeline(
            &txn,
            false, // Done by user
            tenant_id,
            original_name,
            name,
            description,
            platform_version,
            runtime_config,
            program_code,
            udf_rust,
            udf_toml,
            program_config,
        )
        .await?;

        // Fetch updated pipeline
        let final_name = name.clone().unwrap_or(original_name.to_string());
        let extended_pipeline =
            operations::pipeline::get_pipeline(&txn, tenant_id, &final_name).await?;

        txn.commit().await?;
        Ok(extended_pipeline)
    }

    async fn delete_pipeline(
        &self,
        tenant_id: TenantId,
        pipeline_name: &str,
    ) -> Result<PipelineId, DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        let pipeline_id =
            operations::pipeline::delete_pipeline(&txn, tenant_id, pipeline_name).await?;
        txn.commit().await?;
        Ok(pipeline_id)
    }

    async fn transit_program_status_to_pending(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        program_version_guard: Version,
    ) -> Result<(), DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        operations::pipeline::set_program_status(
            &txn,
            tenant_id,
            pipeline_id,
            program_version_guard,
            &ProgramStatus::Pending,
            &None,
            &None,
            &None,
            &None,
            &None,
            &None,
            &None,
        )
        .await?;
        txn.commit().await?;
        Ok(())
    }

    async fn transit_program_status_to_compiling_sql(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        program_version_guard: Version,
    ) -> Result<(), DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        operations::pipeline::set_program_status(
            &txn,
            tenant_id,
            pipeline_id,
            program_version_guard,
            &ProgramStatus::CompilingSql,
            &None,
            &None,
            &None,
            &None,
            &None,
            &None,
            &None,
        )
        .await?;
        txn.commit().await?;
        Ok(())
    }

    async fn transit_program_status_to_sql_compiled(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        program_version_guard: Version,
        sql_compilation: &SqlCompilationInfo,
        program_info: &serde_json::Value,
    ) -> Result<(), DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        operations::pipeline::set_program_status(
            &txn,
            tenant_id,
            pipeline_id,
            program_version_guard,
            &ProgramStatus::SqlCompiled,
            &Some(sql_compilation.clone()),
            &None,
            &None,
            &Some(program_info.clone()),
            &None,
            &None,
            &None,
        )
        .await?;
        txn.commit().await?;
        Ok(())
    }

    async fn transit_program_status_to_compiling_rust(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        program_version_guard: Version,
    ) -> Result<(), DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        operations::pipeline::set_program_status(
            &txn,
            tenant_id,
            pipeline_id,
            program_version_guard,
            &ProgramStatus::CompilingRust,
            &None,
            &None,
            &None,
            &None,
            &None,
            &None,
            &None,
        )
        .await?;
        txn.commit().await?;
        Ok(())
    }

    async fn transit_program_status_to_success(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        program_version_guard: Version,
        rust_compilation: &RustCompilationInfo,
        program_binary_source_checksum: &str,
        program_binary_integrity_checksum: &str,
        program_binary_url: &str,
    ) -> Result<(), DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        operations::pipeline::set_program_status(
            &txn,
            tenant_id,
            pipeline_id,
            program_version_guard,
            &ProgramStatus::Success,
            &None,
            &Some(rust_compilation.clone()),
            &None,
            &None,
            &Some(program_binary_source_checksum.to_string()),
            &Some(program_binary_integrity_checksum.to_string()),
            &Some(program_binary_url.to_string()),
        )
        .await?;
        txn.commit().await?;
        Ok(())
    }

    async fn transit_program_status_to_sql_error(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        program_version_guard: Version,
        sql_compilation: &SqlCompilationInfo,
    ) -> Result<(), DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        operations::pipeline::set_program_status(
            &txn,
            tenant_id,
            pipeline_id,
            program_version_guard,
            &ProgramStatus::SqlError,
            &Some(sql_compilation.clone()),
            &None,
            &None,
            &None,
            &None,
            &None,
            &None,
        )
        .await?;
        txn.commit().await?;
        Ok(())
    }

    async fn transit_program_status_to_rust_error(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        program_version_guard: Version,
        rust_compilation: &RustCompilationInfo,
    ) -> Result<(), DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        operations::pipeline::set_program_status(
            &txn,
            tenant_id,
            pipeline_id,
            program_version_guard,
            &ProgramStatus::RustError,
            &None,
            &Some(rust_compilation.clone()),
            &None,
            &None,
            &None,
            &None,
            &None,
        )
        .await?;
        txn.commit().await?;
        Ok(())
    }

    async fn transit_program_status_to_system_error(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        program_version_guard: Version,
        system_error: &str,
    ) -> Result<(), DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        operations::pipeline::set_program_status(
            &txn,
            tenant_id,
            pipeline_id,
            program_version_guard,
            &ProgramStatus::SystemError,
            &None,
            &None,
            &Some(system_error.to_string()),
            &None,
            &None,
            &None,
            &None,
        )
        .await?;
        txn.commit().await?;
        Ok(())
    }

    async fn set_deployment_desired_status_running(
        &self,
        tenant_id: TenantId,
        pipeline_name: &str,
    ) -> Result<PipelineId, DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        let pipeline_id = operations::pipeline::set_deployment_desired_status(
            &txn,
            tenant_id,
            pipeline_name,
            PipelineDesiredStatus::Running,
        )
        .await?;
        txn.commit().await?;
        Ok(pipeline_id)
    }

    async fn set_deployment_desired_status_paused(
        &self,
        tenant_id: TenantId,
        pipeline_name: &str,
    ) -> Result<PipelineId, DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        let pipeline_id = operations::pipeline::set_deployment_desired_status(
            &txn,
            tenant_id,
            pipeline_name,
            PipelineDesiredStatus::Paused,
        )
        .await?;
        txn.commit().await?;
        Ok(pipeline_id)
    }

    async fn set_deployment_desired_status_suspended(
        &self,
        tenant_id: TenantId,
        pipeline_name: &str,
    ) -> Result<PipelineId, DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        let pipeline_id = operations::pipeline::set_deployment_desired_status(
            &txn,
            tenant_id,
            pipeline_name,
            PipelineDesiredStatus::Suspended,
        )
        .await?;
        txn.commit().await?;
        Ok(pipeline_id)
    }

    async fn set_deployment_desired_status_shutdown(
        &self,
        tenant_id: TenantId,
        pipeline_name: &str,
    ) -> Result<PipelineId, DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        let pipeline_id = operations::pipeline::set_deployment_desired_status(
            &txn,
            tenant_id,
            pipeline_name,
            PipelineDesiredStatus::Shutdown,
        )
        .await?;
        txn.commit().await?;
        Ok(pipeline_id)
    }

    async fn transit_deployment_status_to_provisioning(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        version_guard: Version,
        deployment_config: serde_json::Value,
    ) -> Result<(), DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        operations::pipeline::set_deployment_status(
            &txn,
            tenant_id,
            pipeline_id,
            version_guard,
            PipelineStatus::Provisioning,
            None,
            Some(deployment_config),
            None,
            None,
        )
        .await?;
        txn.commit().await?;
        Ok(())
    }

    async fn transit_deployment_status_to_initializing(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        version_guard: Version,
        deployment_location: &str,
    ) -> Result<(), DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        operations::pipeline::set_deployment_status(
            &txn,
            tenant_id,
            pipeline_id,
            version_guard,
            PipelineStatus::Initializing,
            None,
            None,
            Some(deployment_location.to_string()),
            None,
        )
        .await?;
        txn.commit().await?;
        Ok(())
    }

    async fn transit_deployment_status_to_running(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        version_guard: Version,
    ) -> Result<(), DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        operations::pipeline::set_deployment_status(
            &txn,
            tenant_id,
            pipeline_id,
            version_guard,
            PipelineStatus::Running,
            None,
            None,
            None,
            None,
        )
        .await?;
        txn.commit().await?;
        Ok(())
    }

    async fn transit_deployment_status_to_paused(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        version_guard: Version,
    ) -> Result<(), DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        operations::pipeline::set_deployment_status(
            &txn,
            tenant_id,
            pipeline_id,
            version_guard,
            PipelineStatus::Paused,
            None,
            None,
            None,
            None,
        )
        .await?;
        txn.commit().await?;
        Ok(())
    }

    async fn transit_deployment_status_to_unavailable(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        version_guard: Version,
    ) -> Result<(), DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        operations::pipeline::set_deployment_status(
            &txn,
            tenant_id,
            pipeline_id,
            version_guard,
            PipelineStatus::Unavailable,
            None,
            None,
            None,
            None,
        )
        .await?;
        txn.commit().await?;
        Ok(())
    }

    async fn transit_deployment_status_to_suspending_circuit(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        version_guard: Version,
    ) -> Result<(), DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        operations::pipeline::set_deployment_status(
            &txn,
            tenant_id,
            pipeline_id,
            version_guard,
            PipelineStatus::SuspendingCircuit,
            None,
            None,
            None,
            None,
        )
        .await?;
        txn.commit().await?;
        Ok(())
    }

    async fn transit_deployment_status_to_suspending_compute(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        version_guard: Version,
        suspend_info: serde_json::Value,
    ) -> Result<(), DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        operations::pipeline::set_deployment_status(
            &txn,
            tenant_id,
            pipeline_id,
            version_guard,
            PipelineStatus::SuspendingCompute,
            None,
            None,
            None,
            Some(suspend_info),
        )
        .await?;
        txn.commit().await?;
        Ok(())
    }

    async fn transit_deployment_status_to_suspended(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        version_guard: Version,
    ) -> Result<(), DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        operations::pipeline::set_deployment_status(
            &txn,
            tenant_id,
            pipeline_id,
            version_guard,
            PipelineStatus::Suspended,
            None,
            None,
            None,
            None,
        )
        .await?;
        txn.commit().await?;
        Ok(())
    }

    async fn transit_deployment_status_to_shutting_down(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        version_guard: Version,
    ) -> Result<(), DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        operations::pipeline::set_deployment_status(
            &txn,
            tenant_id,
            pipeline_id,
            version_guard,
            PipelineStatus::ShuttingDown,
            None,
            None,
            None,
            None,
        )
        .await?;
        txn.commit().await?;
        Ok(())
    }

    async fn transit_deployment_status_to_shutdown(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        version_guard: Version,
    ) -> Result<(), DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        operations::pipeline::set_deployment_status(
            &txn,
            tenant_id,
            pipeline_id,
            version_guard,
            PipelineStatus::Shutdown,
            None,
            None,
            None,
            None,
        )
        .await?;
        txn.commit().await?;
        Ok(())
    }

    async fn transit_deployment_status_to_failed(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        version_guard: Version,
        deployment_error: &ErrorResponse,
    ) -> Result<(), DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        operations::pipeline::set_deployment_status(
            &txn,
            tenant_id,
            pipeline_id,
            version_guard,
            PipelineStatus::Failed,
            Some(deployment_error.clone()),
            None,
            None,
            None,
        )
        .await?;
        txn.commit().await?;
        Ok(())
    }

    async fn list_pipeline_ids_across_all_tenants(
        &self,
    ) -> Result<Vec<(TenantId, PipelineId)>, DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        let pipeline_ids = operations::pipeline::list_pipeline_ids_across_all_tenants(&txn).await?;
        txn.commit().await?;
        Ok(pipeline_ids)
    }

    async fn list_pipelines_across_all_tenants_for_monitoring(
        &self,
    ) -> Result<Vec<(TenantId, ExtendedPipelineDescrMonitoring)>, DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        let pipelines =
            operations::pipeline::list_pipelines_across_all_tenants_for_monitoring(&txn).await?;
        txn.commit().await?;
        Ok(pipelines)
    }

    async fn clear_ongoing_sql_compilation(&self, platform_version: &str) -> Result<(), DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        let pipelines =
            operations::pipeline::list_pipelines_across_all_tenants_for_monitoring(&txn).await?;
        for (tenant_id, pipeline) in pipelines {
            if pipeline.deployment_status == PipelineStatus::Shutdown
                || pipeline.deployment_status == PipelineStatus::Suspended
            {
                if pipeline.platform_version == platform_version {
                    if pipeline.program_status == ProgramStatus::CompilingSql {
                        operations::pipeline::set_program_status(
                            &txn,
                            tenant_id,
                            pipeline.id,
                            pipeline.program_version,
                            &ProgramStatus::Pending,
                            &None,
                            &None,
                            &None,
                            &None,
                            &None,
                            &None,
                            &None,
                        )
                        .await?;
                    }
                } else if pipeline.program_status == ProgramStatus::Pending
                    || pipeline.program_status == ProgramStatus::CompilingSql
                {
                    operations::pipeline::update_pipeline(
                        &txn,
                        true, // Done by compiler
                        tenant_id,
                        &pipeline.name,
                        &None,
                        &None,
                        platform_version,
                        &None,
                        &None,
                        &None,
                        &None,
                        &None,
                    )
                    .await?;
                }
            }
        }
        txn.commit().await?;
        Ok(())
    }

    async fn get_next_sql_compilation(
        &self,
        platform_version: &str,
    ) -> Result<Option<(TenantId, ExtendedPipelineDescr)>, DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        let next_pipeline_program =
            operations::pipeline::get_next_sql_compilation(&txn, platform_version).await?;
        txn.commit().await?;
        Ok(next_pipeline_program)
    }

    async fn clear_ongoing_rust_compilation(&self, platform_version: &str) -> Result<(), DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        let pipelines =
            operations::pipeline::list_pipelines_across_all_tenants_for_monitoring(&txn).await?;
        for (tenant_id, pipeline) in pipelines {
            if pipeline.deployment_status == PipelineStatus::Shutdown
                || pipeline.deployment_status == PipelineStatus::Suspended
            {
                if pipeline.platform_version == platform_version {
                    if pipeline.program_status == ProgramStatus::CompilingRust {
                        // Because `program_info` can be rather large, it is only fetched when
                        // the program status needs to be reset to `SqlCompiled`
                        let pipeline_complete =
                            operations::pipeline::get_pipeline_by_id(&txn, tenant_id, pipeline.id)
                                .await?;
                        operations::pipeline::set_program_status(
                            &txn,
                            tenant_id,
                            pipeline.id,
                            pipeline.program_version,
                            &ProgramStatus::SqlCompiled,
                            &Some(pipeline_complete.program_error.sql_compilation.clone().expect("program_error.sql_compilation must be present if current status is CompilingRust")),
                            &None,
                            &None,
                            &Some(pipeline_complete.program_info.clone().expect(
                                "program_info must be present if current status is CompilingRust",
                            )),
                            &None,
                            &None,
                            &None,
                        )
                        .await?;
                    }
                } else if pipeline.program_status == ProgramStatus::SqlCompiled
                    || pipeline.program_status == ProgramStatus::CompilingRust
                {
                    operations::pipeline::update_pipeline(
                        &txn,
                        true, // Done by compiler
                        tenant_id,
                        &pipeline.name,
                        &None,
                        &None,
                        platform_version,
                        &None,
                        &None,
                        &None,
                        &None,
                        &None,
                    )
                    .await?;
                }
            }
        }
        txn.commit().await?;
        Ok(())
    }

    async fn get_next_rust_compilation(
        &self,
        platform_version: &str,
    ) -> Result<Option<(TenantId, ExtendedPipelineDescr)>, DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        let next_pipeline_program =
            operations::pipeline::get_next_rust_compilation(&txn, platform_version).await?;
        txn.commit().await?;
        Ok(next_pipeline_program)
    }

    async fn list_pipeline_programs_across_all_tenants(
        &self,
    ) -> Result<Vec<(PipelineId, Version, String, String)>, DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        let pipeline_programs =
            operations::pipeline::list_pipeline_programs_across_all_tenants(&txn).await?;
        txn.commit().await?;
        Ok(pipeline_programs)
    }
}

impl StoragePostgres {
    pub async fn connect(
        db_config: &DatabaseConfig,
        #[cfg(feature = "postgresql_embedded")] pg_embed_config: PgEmbedConfig,
    ) -> Result<Self, DBError> {
        #[cfg(feature = "postgresql_embedded")]
        if db_config.uses_postgres_embed() {
            let database_dir = pg_embed_config.pg_embed_data_dir();
            let pg_inst = pg_setup::install(database_dir, true, Some(8082)).await?;
            let db_name = "postgres";
            if !pg_inst.database_exists(db_name).await? {
                pg_inst.create_database(db_name).await?;
            }
            let connection_string = pg_inst.settings().url(db_name);

            let db_config = DatabaseConfig::new(connection_string, None);
            return Self::initialize(&db_config, Some(pg_inst)).await;
        };

        Self::initialize(
            db_config,
            #[cfg(feature = "postgresql_embedded")]
            None,
        )
        .await
    }

    pub(crate) async fn initialize(
        db_config: &DatabaseConfig,
        #[cfg(feature = "postgresql_embedded")] pg_inst: Option<postgresql_embedded::PostgreSQL>,
    ) -> Result<Self, DBError> {
        let config = db_config.tokio_postgres_config()?;
        debug!(
            "Opening Postgres connection with configuration: {:?}",
            config
        );
        let tls = db_config.tls_connector()?;
        let mgr_config = deadpool_postgres::ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        };
        let mgr = Manager::from_config(config.clone(), tls, mgr_config);
        let pool = Pool::builder(mgr).max_size(16).build().unwrap();
        #[cfg(feature = "postgresql_embedded")]
        return Ok(Self {
            db_config: db_config.clone(),
            config,
            pool,
            pg_inst,
        });
        #[cfg(not(feature = "postgresql_embedded"))]
        return Ok(Self {
            db_config: db_config.clone(),
            config,
            pool,
        });
    }

    /// Runs database migrations if needed.
    pub async fn run_migrations(&self) -> Result<(), DBError> {
        debug!("Applying database migrations if needed...");
        let mut client = self.pool.get().await?;
        let report = embedded::migrations::runner()
            .run_async(&mut **client)
            .await?;
        log!(
            if report.applied_migrations().is_empty() {
                Level::Debug
            } else {
                Level::Info
            },
            "Database migrations finished: {} migrations were applied",
            report.applied_migrations().len()
        );

        // YAML -> JSON migration
        self.perform_yaml_to_json_migration().await?;

        let default_tenant = TenantRecord::default();
        self.get_or_create_tenant_id(
            default_tenant.id.0,
            default_tenant.tenant,
            default_tenant.provider,
        )
        .await?;
        Ok(())
    }

    /// Performs the conversion of the pipeline table fields which are YAML to become JSON.
    async fn perform_yaml_to_json_migration(&self) -> Result<(), DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        let stmt = txn
            .prepare_cached(
                "SELECT p.id, p.runtime_config, p.program_config, p.program_info, p.deployment_error, p.deployment_config
                 FROM pipeline AS p
                 WHERE p.uses_json = FALSE
                 ORDER BY p.id ASC"
            )
            .await?;
        let rows: Vec<Row> = txn.query(&stmt, &[]).await?;
        if !rows.is_empty() {
            let stmt_update = txn
                .prepare_cached(
                    "UPDATE pipeline
                     SET runtime_config = $1,
                         program_config = $2,
                         program_info = $3,
                         deployment_error = $4,
                         deployment_config = $5,
                         uses_json = TRUE
                     WHERE id = $6",
                )
                .await?;
            let mut rows_affected = 0;
            for row in rows {
                let pipeline_id = PipelineId(row.get(0));
                let runtime_config = serde_yaml::from_str::<RuntimeConfig>(row.get(1))
                    .expect("Deserialize RuntimeConfig from YAML");
                let program_config = serde_yaml::from_str::<ProgramConfig>(row.get(2))
                    .expect("Deserialize ProgramConfig from YAML");
                let program_info = row.get::<_, Option<String>>(3).map(|s| {
                    serde_yaml::from_str::<ProgramInfo>(&s)
                        .expect("Deserialize ProgramInfo from YAML")
                });
                let deployment_error = row.get::<_, Option<String>>(4).map(|s| {
                    serde_yaml::from_str::<ErrorResponse>(&s)
                        .expect("Deserialize ErrorResponse from YAML")
                });
                let deployment_config = row.get::<_, Option<String>>(5).map(|s| {
                    serde_yaml::from_str::<PipelineConfig>(&s)
                        .expect("Deserialize PipelineConfig from YAML")
                });
                rows_affected += txn
                    .execute(
                        &stmt_update,
                        &[
                            &serde_json::to_string(&runtime_config)
                                .expect("Serialize RuntimeConfig as JSON"),
                            &serde_json::to_string(&program_config)
                                .expect("Serialize ProgramConfig as JSON"),
                            &program_info.as_ref().map(|v| {
                                serde_json::to_string(&v).expect("Serialize ProgramInfo as JSON")
                            }),
                            &deployment_error.as_ref().map(|v| {
                                serde_json::to_string(&v).expect("Serialize ErrorResponse as JSON")
                            }),
                            &deployment_config.as_ref().map(|v| {
                                serde_json::to_string(&v).expect("Serialize PipelineConfig as JSON")
                            }),
                            &pipeline_id.0,
                        ],
                    )
                    .await?;
            }
            txn.commit().await?;
            if rows_affected > 0 {
                info!("Converted YAML to JSON for {rows_affected} row(s)");
            }
        } else {
            txn.rollback().await?;
        }
        Ok(())
    }

    /// Check if the expected DB migrations have already been run
    pub async fn check_migrations(self) -> Result<Self, DBError> {
        debug!("Checking if DB migrations have been applied");
        let mut client = self.pool.get().await?;
        let runner = embedded::migrations::runner();
        let expected_max_version = runner
            .get_migrations()
            .iter()
            .map(|m| m.version())
            .fold(u32::MIN, |a, b| a.max(b));
        let migration = runner.get_last_applied_migration_async(&mut **client).await;
        if let Ok(Some(m)) = migration {
            let v = m.version();
            info!("Expected version = {expected_max_version}. Actual version = {v}.");
            if v == expected_max_version {
                Ok(self)
            } else {
                Err(DBError::MissingMigrations {
                    expected: expected_max_version,
                    actual: v,
                })
            }
        } else {
            info!("Expected version = {expected_max_version}. Actual version = None.");
            Err(DBError::MissingMigrations {
                expected: expected_max_version,
                actual: 0,
            })
        }
    }
}
