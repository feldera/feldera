#[cfg(feature = "pg-embed")]
use crate::config::ApiServerConfig;
use crate::db::error::DBError;
use crate::db::operations;
#[cfg(feature = "pg-embed")]
use crate::db::pg_setup;
use crate::db::storage::Storage;
use crate::db::types::api_key::{ApiKeyDescr, ApiPermission};
use crate::db::types::common::Version;
use crate::db::types::pipeline::{
    ExtendedPipelineDescr, PipelineDescr, PipelineId, PipelineStatus,
};
use crate::db::types::program::{ProgramConfig, ProgramInfo, ProgramStatus, SqlCompilerMessage};
use crate::db::types::tenant::TenantId;
use crate::{auth::TenantRecord, config::DatabaseConfig};
use async_trait::async_trait;
use deadpool_postgres::{Manager, Pool, RecyclingMethod};
use log::{debug, info, log, Level};
use pipeline_types::config::{PipelineConfig, RuntimeConfig};
use pipeline_types::error::ErrorResponse;
use tokio_postgres::NoTls;
use uuid::Uuid;

mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("./migrations/");
}

/// Postgres implementation of the `Storage` trait.
/// A trait method implementation consists of a single transaction
/// in which database operations are executed.
pub struct StoragePostgres {
    /// Postgres configuration.
    pub config: tokio_postgres::Config,
    /// Pool from which clients can be taken.
    pub(crate) pool: Pool,
    /// Embedded Postgres instance which is used in development mode.
    /// It will not be persisted after termination.
    #[cfg(feature = "pg-embed")]
    #[allow(dead_code)]
    // It has to stay alive until StoragePostgres is dropped.
    pub(crate) pg_inst: Option<pg_embed::postgres::PgEmbed>,
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

    async fn new_pipeline(
        &self,
        tenant_id: TenantId,
        new_id: Uuid,
        pipeline: PipelineDescr,
    ) -> Result<ExtendedPipelineDescr, DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;

        // Create new pipeline
        operations::pipeline::new_pipeline(&txn, tenant_id, new_id, pipeline.clone()).await?;

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
                    tenant_id,
                    original_name,
                    &Some(pipeline.name.clone()),
                    &Some(pipeline.description.clone()),
                    &Some(pipeline.runtime_config.clone()),
                    &Some(pipeline.program_code.clone()),
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
                operations::pipeline::new_pipeline(&txn, tenant_id, new_id, pipeline.clone())
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
        runtime_config: &Option<RuntimeConfig>,
        program_code: &Option<String>,
        program_config: &Option<ProgramConfig>,
    ) -> Result<ExtendedPipelineDescr, DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;

        // Update existing pipeline
        operations::pipeline::update_pipeline(
            &txn,
            tenant_id,
            original_name,
            name,
            description,
            runtime_config,
            program_code,
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
        program_info: &ProgramInfo,
    ) -> Result<(), DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        operations::pipeline::set_program_status(
            &txn,
            tenant_id,
            pipeline_id,
            program_version_guard,
            &ProgramStatus::CompilingRust,
            &Some(program_info.clone()),
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
        internal_sql_error: Vec<SqlCompilerMessage>,
    ) -> Result<(), DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        operations::pipeline::set_program_status(
            &txn,
            tenant_id,
            pipeline_id,
            program_version_guard,
            &ProgramStatus::SqlError(internal_sql_error),
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
        internal_rust_error: &str,
    ) -> Result<(), DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        operations::pipeline::set_program_status(
            &txn,
            tenant_id,
            pipeline_id,
            program_version_guard,
            &ProgramStatus::RustError(internal_rust_error.to_string()),
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
        internal_system_error: &str,
    ) -> Result<(), DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        operations::pipeline::set_program_status(
            &txn,
            tenant_id,
            pipeline_id,
            program_version_guard,
            &ProgramStatus::SystemError(internal_system_error.to_string()),
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
    ) -> Result<(), DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        operations::pipeline::set_deployment_desired_status(
            &txn,
            tenant_id,
            pipeline_name,
            PipelineStatus::Running,
        )
        .await?;
        txn.commit().await?;
        Ok(())
    }

    async fn set_deployment_desired_status_paused(
        &self,
        tenant_id: TenantId,
        pipeline_name: &str,
    ) -> Result<(), DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        operations::pipeline::set_deployment_desired_status(
            &txn,
            tenant_id,
            pipeline_name,
            PipelineStatus::Paused,
        )
        .await?;
        txn.commit().await?;
        Ok(())
    }

    async fn set_deployment_desired_status_shutdown(
        &self,
        tenant_id: TenantId,
        pipeline_name: &str,
    ) -> Result<(), DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        operations::pipeline::set_deployment_desired_status(
            &txn,
            tenant_id,
            pipeline_name,
            PipelineStatus::Shutdown,
        )
        .await?;
        txn.commit().await?;
        Ok(())
    }

    async fn transit_deployment_status_to_provisioning(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        deployment_config: PipelineConfig,
    ) -> Result<(), DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        operations::pipeline::set_deployment_status(
            &txn,
            tenant_id,
            pipeline_id,
            PipelineStatus::Provisioning,
            None,
            Some(deployment_config),
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
        deployment_location: &str,
    ) -> Result<(), DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        operations::pipeline::set_deployment_status(
            &txn,
            tenant_id,
            pipeline_id,
            PipelineStatus::Initializing,
            None,
            None,
            Some(deployment_location.to_string()),
        )
        .await?;
        txn.commit().await?;
        Ok(())
    }

    async fn transit_deployment_status_to_running(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
    ) -> Result<(), DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        operations::pipeline::set_deployment_status(
            &txn,
            tenant_id,
            pipeline_id,
            PipelineStatus::Running,
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
    ) -> Result<(), DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        operations::pipeline::set_deployment_status(
            &txn,
            tenant_id,
            pipeline_id,
            PipelineStatus::Paused,
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
    ) -> Result<(), DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        operations::pipeline::set_deployment_status(
            &txn,
            tenant_id,
            pipeline_id,
            PipelineStatus::ShuttingDown,
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
    ) -> Result<(), DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        operations::pipeline::set_deployment_status(
            &txn,
            tenant_id,
            pipeline_id,
            PipelineStatus::Shutdown,
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
        deployment_error: &ErrorResponse,
    ) -> Result<(), DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        operations::pipeline::set_deployment_status(
            &txn,
            tenant_id,
            pipeline_id,
            PipelineStatus::Failed,
            Some(deployment_error.clone()),
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

    async fn list_pipelines_across_all_tenants(
        &self,
    ) -> Result<Vec<(TenantId, ExtendedPipelineDescr)>, DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        let pipelines = operations::pipeline::list_pipelines_across_all_tenants(&txn).await?;
        txn.commit().await?;
        Ok(pipelines)
    }

    async fn get_next_pipeline_program_to_compile(
        &self,
    ) -> Result<Option<(TenantId, ExtendedPipelineDescr)>, DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        let next_pipeline_program =
            operations::pipeline::get_next_pipeline_program_to_compile(&txn).await?;
        txn.commit().await?;
        Ok(next_pipeline_program)
    }

    async fn is_pipeline_program_in_use(
        &self,
        pipeline_id: PipelineId,
        program_version: Version,
    ) -> Result<bool, DBError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        let is_used =
            operations::pipeline::is_pipeline_program_in_use(&txn, pipeline_id, program_version)
                .await?;
        txn.commit().await?;
        Ok(is_used)
    }
}

impl StoragePostgres {
    pub async fn connect(
        db_config: &DatabaseConfig,
        #[cfg(feature = "pg-embed")] api_config: Option<&ApiServerConfig>,
    ) -> Result<Self, DBError> {
        let connection_str = db_config.database_connection_string();

        #[cfg(feature = "pg-embed")]
        if connection_str.starts_with("postgres-embed") {
            let database_dir = api_config
                .expect("ApiServerConfig needs to be provided when using pg-embed")
                .postgres_embed_data_dir();
            let pg_inst = pg_setup::install(database_dir, true, Some(8082)).await?;
            let connection_string = pg_inst.db_uri.to_string();
            return Self::connect_inner(connection_string.as_str(), Some(pg_inst)).await;
        };

        Self::connect_inner(
            connection_str.as_str(),
            #[cfg(feature = "pg-embed")]
            None,
        )
        .await
    }

    /// Connect to the project database.
    ///
    /// # Arguments
    /// - `config` a tokio postgres config
    ///
    /// # Notes
    /// Maybe this should become the preferred way to create a ProjectDb
    /// together with `pg-client-config` (and drop `connect_inner`).
    #[cfg(all(test, not(feature = "pg-embed")))]
    pub(crate) async fn with_config(config: tokio_postgres::Config) -> Result<Self, DBError> {
        let db = StoragePostgres::initialize(
            config,
            #[cfg(feature = "pg-embed")]
            None,
        )
        .await?;
        Ok(db)
    }

    /// Connect to the project database.
    ///
    /// # Arguments
    /// - `connection_str`: The connection string to the database.
    pub(crate) async fn connect_inner(
        connection_str: &str,
        #[cfg(feature = "pg-embed")] pg_inst: Option<pg_embed::postgres::PgEmbed>,
    ) -> Result<Self, DBError> {
        if !connection_str.starts_with("postgres") {
            panic!("Unsupported connection string {}", connection_str)
        }
        let config = connection_str.parse::<tokio_postgres::Config>()?;
        debug!("Opening connection to {:?}", connection_str);

        let db = StoragePostgres::initialize(
            config,
            #[cfg(feature = "pg-embed")]
            pg_inst,
        )
        .await?;
        Ok(db)
    }

    async fn initialize(
        config: tokio_postgres::Config,
        #[cfg(feature = "pg-embed")] pg_inst: Option<pg_embed::postgres::PgEmbed>,
    ) -> Result<Self, DBError> {
        let mgr_config = deadpool_postgres::ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        };
        let mgr = Manager::from_config(config.clone(), NoTls, mgr_config);
        let pool = Pool::builder(mgr).max_size(16).build().unwrap();
        #[cfg(feature = "pg-embed")]
        return Ok(Self {
            config,
            pool,
            pg_inst,
        });
        #[cfg(not(feature = "pg-embed"))]
        return Ok(Self { config, pool });
    }

    /// Run database migrations
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
        let default_tenant = TenantRecord::default();
        self.get_or_create_tenant_id(
            default_tenant.id.0,
            default_tenant.tenant,
            default_tenant.provider,
        )
        .await?;
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
