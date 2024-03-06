#[cfg(feature = "pg-embed")]
use crate::config::ApiServerConfig;
use crate::{
    auth::{TenantId, TenantRecord},
    compiler::ProgramStatus,
    config::DatabaseConfig,
};
use async_trait::async_trait;
use deadpool_postgres::{Manager, Pool, RecyclingMethod, Transaction};
use log::{debug, info};
use openssl::sha;
use pipeline_types::{
    config::{PipelineConfig, RuntimeConfig},
    program_schema::ProgramSchema,
};
use serde::{Deserialize, Serialize};
use std::{fmt, fmt::Display, str::FromStr};
use storage::Storage;
use tokio_postgres::{error::Error as PgError, NoTls};
use utoipa::ToSchema;
use uuid::Uuid;

#[cfg(test)]
pub(crate) mod test;

#[cfg(feature = "pg-embed")]
mod pg_setup;
pub(crate) mod storage;

mod error;
use crate::api::{ConnectorConfig, ServiceConfig};
pub use error::DBError;

mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("./migrations/");
}

/// Project database API.
///
/// The API assumes that the caller holds a database lock, and therefore
/// doesn't use transactions (and hence doesn't need to deal with conflicts).
///
/// # Compilation queue
///
/// We use the `status` and `status_since` columns to maintain the compilation
/// queue.  A program is enqueued for compilation by setting its status to
/// `ProgramStatus::Pending`.  The `status_since` column is set to the current
/// time, which determines the position of the program in the queue.
pub struct ProjectDB {
    pub config: tokio_postgres::Config,
    pool: Pool,
    // Used in dev mode for having an embedded Postgres DB live through the
    // lifetime of the program.
    #[cfg(feature = "pg-embed")]
    #[allow(dead_code)] // It has to stay alive until ProjectDB is dropped.
    pg_inst: Option<pg_embed::postgres::PgEmbed>,
}

/// Version number.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[repr(transparent)]
#[serde(transparent)]
pub struct Version(#[cfg_attr(test, proptest(strategy = "1..3i64"))] pub i64);
impl Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// ApiKey ID.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Ord, PartialOrd, Serialize, Deserialize, ToSchema)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[repr(transparent)]
#[serde(transparent)]
pub struct ApiKeyId(#[cfg_attr(test, proptest(strategy = "test::limited_uuid()"))] pub Uuid);
impl Display for ApiKeyId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// ApiKey descriptor.
#[derive(Deserialize, Serialize, ToSchema, Debug, Clone, Eq, PartialEq)]
pub(crate) struct ApiKeyDescr {
    pub id: ApiKeyId,
    pub name: String,
    pub scopes: Vec<ApiPermission>,
}

/// Permission types for invoking pipeline manager APIs
#[derive(Deserialize, Serialize, ToSchema, Debug, Clone, Eq, PartialEq)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub(crate) enum ApiPermission {
    Read,
    Write,
}

const API_PERMISSION_READ: &str = "read";
const API_PERMISSION_WRITE: &str = "write";

impl FromStr for ApiPermission {
    type Err = ();

    fn from_str(input: &str) -> Result<ApiPermission, Self::Err> {
        match input {
            API_PERMISSION_READ => Ok(ApiPermission::Read),
            API_PERMISSION_WRITE => Ok(ApiPermission::Write),
            _ => Err(()),
        }
    }
}

// Re-exports
// Program
mod program;
pub(crate) use self::program::ProgramDescr;
pub use self::program::ProgramId;

// Connectors
mod connector;
pub(crate) use self::connector::ConnectorDescr;
pub use self::connector::ConnectorId;

// Pipelines
mod pipeline;
pub(crate) use self::pipeline::AttachedConnector;
pub(crate) use self::pipeline::AttachedConnectorId;
pub(crate) use self::pipeline::Pipeline;
pub(crate) use self::pipeline::PipelineDescr;
pub use self::pipeline::PipelineId;
pub use self::pipeline::PipelineRevision;
pub(crate) use self::pipeline::PipelineRuntimeState;
pub use self::pipeline::PipelineStatus;
pub(crate) use self::pipeline::Revision;

// Services
mod service;
pub(crate) use self::service::ServiceDescr;
pub use self::service::ServiceId;

// The goal for these methods is to avoid multiple DB interactions as much as
// possible and if not, use transactions
#[async_trait]
impl Storage for ProjectDB {
    async fn list_programs(
        &self,
        tenant_id: TenantId,
        with_code: bool,
    ) -> Result<Vec<ProgramDescr>, DBError> {
        Ok(program::list_programs(self, tenant_id, with_code).await?)
    }

    async fn new_program(
        &self,
        tenant_id: TenantId,
        id: Uuid,
        program_name: &str,
        program_description: &str,
        program_code: &str,
        txn: Option<&Transaction<'_>>,
    ) -> Result<(ProgramId, Version), DBError> {
        Ok(program::new_program(
            self,
            tenant_id,
            id,
            program_name,
            program_description,
            program_code,
            txn,
        )
        .await?)
    }

    /// Optionally update different fields of a program. This call
    /// also accepts an optional version to do guarded updates to the code.
    async fn update_program(
        &self,
        tenant_id: TenantId,
        program_id: ProgramId,
        program_name: &Option<String>,
        program_description: &Option<String>,
        program_code: &Option<String>,
        status: &Option<ProgramStatus>,
        schema: &Option<ProgramSchema>,
        guard: Option<Version>,
        txn: Option<&Transaction<'_>>,
    ) -> Result<Version, DBError> {
        if let Some(t) = txn {
            Ok(program::update_program(
                tenant_id,
                program_id,
                program_name,
                program_description,
                program_code,
                status,
                schema,
                guard,
                t,
            )
            .await?)
        } else {
            let mut manager = self.pool.get().await?;
            let txn_inner = manager.transaction().await?;
            let res = program::update_program(
                tenant_id,
                program_id,
                program_name,
                program_description,
                program_code,
                status,
                schema,
                guard,
                &txn_inner,
            )
            .await?;
            txn_inner.commit().await?;
            Ok(res)
        }
    }

    /// Retrieve program descriptor.
    ///
    /// Returns `None` if `program_id` is not found in the database.
    async fn get_program_by_id(
        &self,
        tenant_id: TenantId,
        program_id: ProgramId,
        with_code: bool,
    ) -> Result<ProgramDescr, DBError> {
        Ok(program::get_program_by_id(self, tenant_id, program_id, with_code).await?)
    }

    /// Lookup program by name.
    async fn get_program_by_name(
        &self,
        tenant_id: TenantId,
        program_name: &str,
        with_code: bool,
        txn: Option<&Transaction<'_>>,
    ) -> Result<ProgramDescr, DBError> {
        Ok(program::get_program_by_name(self, tenant_id, program_name, with_code, txn).await?)
    }

    async fn delete_program(&self, tenant_id: TenantId, program_name: &str) -> Result<(), DBError> {
        Ok(program::delete_program(self, tenant_id, program_name).await?)
    }

    async fn all_programs(&self) -> Result<Vec<(TenantId, ProgramDescr)>, DBError> {
        Ok(program::all_programs(self).await?)
    }

    async fn all_pipelines(&self) -> Result<Vec<(TenantId, PipelineId)>, DBError> {
        Ok(pipeline::all_pipelines(self).await?)
    }

    async fn next_job(&self) -> Result<Option<(TenantId, ProgramId, Version)>, DBError> {
        let manager = self.pool.get().await?;
        // Find the oldest pending project.
        let stmt = manager
            .prepare_cached(
                "SELECT id, version, tenant_id FROM program WHERE status = 'pending' AND status_since = (SELECT min(status_since) FROM program WHERE status = 'pending')"
            )
            .await?;
        let res = manager.query(&stmt, &[]).await?;

        if let Some(row) = res.first() {
            let program_id: ProgramId = ProgramId(row.get(0));
            let version: Version = Version(row.get(1));
            let tenant_id: TenantId = TenantId(row.get(2));
            Ok(Some((tenant_id, program_id, version)))
        } else {
            Ok(None)
        }
    }

    async fn create_pipeline_deployment(
        &self,
        deployment_id: Uuid,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
    ) -> Result<Revision, DBError> {
        Ok(
            pipeline::create_pipeline_deployment(self, deployment_id, tenant_id, pipeline_id)
                .await?,
        )
    }

    async fn get_pipeline_deployment(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
    ) -> Result<PipelineRevision, DBError> {
        Ok(pipeline::get_pipeline_deployment(self, tenant_id, pipeline_id).await?)
    }

    async fn list_pipelines(&self, tenant_id: TenantId) -> Result<Vec<Pipeline>, DBError> {
        Ok(pipeline::list_pipelines(self, tenant_id).await?)
    }

    async fn get_pipeline_by_id(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
    ) -> Result<Pipeline, DBError> {
        Ok(pipeline::get_pipeline_by_id(self, tenant_id, pipeline_id).await?)
    }

    async fn get_pipeline_descr_by_id(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        txn: Option<&Transaction<'_>>,
    ) -> Result<PipelineDescr, DBError> {
        Ok(pipeline::get_pipeline_descr_by_id(self, tenant_id, pipeline_id, txn).await?)
    }

    async fn get_pipeline_runtime_state_by_id(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
    ) -> Result<PipelineRuntimeState, DBError> {
        Ok(pipeline::get_pipeline_runtime_state_by_id(self, tenant_id, pipeline_id).await?)
    }

    async fn get_pipeline_runtime_state_by_name(
        &self,
        tenant_id: TenantId,
        pipeline_name: &str,
    ) -> Result<PipelineRuntimeState, DBError> {
        Ok(pipeline::get_pipeline_runtime_state_by_name(self, tenant_id, pipeline_name).await?)
    }

    async fn get_pipeline_descr_by_name(
        &self,
        tenant_id: TenantId,
        name: &str,
        txn: Option<&Transaction<'_>>,
    ) -> Result<PipelineDescr, DBError> {
        Ok(pipeline::get_pipeline_descr_by_name(self, tenant_id, name, txn).await?)
    }

    async fn get_pipeline_by_name(
        &self,
        tenant_id: TenantId,
        name: &str,
    ) -> Result<Pipeline, DBError> {
        Ok(pipeline::get_pipeline_by_name(self, tenant_id, name).await?)
    }

    async fn new_pipeline(
        &self,
        tenant_id: TenantId,
        id: Uuid,
        program_name: &Option<String>,
        pipeline_name: &str,
        pipeline_description: &str,
        config: &RuntimeConfig,
        connectors: &Option<Vec<AttachedConnector>>,
        txn: Option<&Transaction<'_>>,
    ) -> Result<(PipelineId, Version), DBError> {
        if let Some(t) = txn {
            Ok(pipeline::new_pipeline(
                tenant_id,
                id,
                program_name,
                pipeline_name,
                pipeline_description,
                config,
                connectors,
                t,
            )
            .await?)
        } else {
            let mut client = self.pool.get().await?;
            let txn = client.transaction().await?;
            let res = pipeline::new_pipeline(
                tenant_id,
                id,
                program_name,
                pipeline_name,
                pipeline_description,
                config,
                connectors,
                &txn,
            )
            .await?;
            txn.commit().await?;
            Ok(res)
        }
    }

    async fn update_pipeline(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        program_name: &Option<String>,
        pipeline_name: &str,
        pipeline_description: &str,
        config: &Option<RuntimeConfig>,
        connectors: &Option<Vec<AttachedConnector>>,
        txn: Option<&Transaction<'_>>,
    ) -> Result<Version, DBError> {
        if let Some(t) = txn {
            Ok(pipeline::update_pipeline(
                tenant_id,
                pipeline_id,
                program_name,
                pipeline_name,
                pipeline_description,
                config,
                connectors,
                t,
            )
            .await?)
        } else {
            let mut client = self.pool.get().await?;
            let txn = client.transaction().await?;
            let res = Ok(pipeline::update_pipeline(
                tenant_id,
                pipeline_id,
                program_name,
                pipeline_name,
                pipeline_description,
                config,
                connectors,
                &txn,
            )
            .await?);
            txn.commit().await?;
            res
        }
    }

    async fn update_pipeline_runtime_state(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        state: &PipelineRuntimeState,
    ) -> Result<(), DBError> {
        Ok(pipeline::update_pipeline_runtime_state(self, tenant_id, pipeline_id, state).await?)
    }

    async fn set_pipeline_desired_status(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        desired_status: PipelineStatus,
    ) -> Result<(), DBError> {
        Ok(
            pipeline::set_pipeline_desired_status(self, tenant_id, pipeline_id, desired_status)
                .await?,
        )
    }

    /// Returns true if the connector of a given name is an input connector.
    async fn attached_connector_is_input(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        name: &str,
    ) -> Result<bool, DBError> {
        Ok(pipeline::attached_connector_is_input(self, tenant_id, pipeline_id, name).await?)
    }

    async fn delete_pipeline(
        &self,
        tenant_id: TenantId,
        pipeline_name: &str,
    ) -> Result<(), DBError> {
        Ok(pipeline::delete_pipeline(self, tenant_id, pipeline_name).await?)
    }

    async fn new_connector(
        &self,
        tenant_id: TenantId,
        id: Uuid,
        name: &str,
        description: &str,
        config: &ConnectorConfig,
        txn: Option<&Transaction<'_>>,
    ) -> Result<ConnectorId, DBError> {
        Ok(connector::new_connector(self, tenant_id, id, name, description, config, txn).await?)
    }

    async fn list_connectors(&self, tenant_id: TenantId) -> Result<Vec<ConnectorDescr>, DBError> {
        Ok(connector::list_connectors(self, tenant_id).await?)
    }

    async fn get_connector_by_name(
        &self,
        tenant_id: TenantId,
        name: &str,
        txn: Option<&Transaction<'_>>,
    ) -> Result<ConnectorDescr, DBError> {
        Ok(connector::get_connector_by_name(self, tenant_id, name, txn).await?)
    }

    async fn get_connector_by_id(
        &self,
        tenant_id: TenantId,
        connector_id: ConnectorId,
    ) -> Result<ConnectorDescr, DBError> {
        Ok(connector::get_connector_by_id(self, tenant_id, connector_id).await?)
    }

    async fn update_connector(
        &self,
        tenant_id: TenantId,
        connector_id: ConnectorId,
        connector_name: &Option<&str>,
        description: &Option<&str>,
        config: &Option<ConnectorConfig>,
        txn: Option<&Transaction<'_>>,
    ) -> Result<(), DBError> {
        Ok(connector::update_connector(
            self,
            tenant_id,
            connector_id,
            connector_name,
            description,
            config,
            txn,
        )
        .await?)
    }

    async fn delete_connector(
        &self,
        tenant_id: TenantId,
        connector_name: &str,
    ) -> Result<(), DBError> {
        Ok(connector::delete_connector(self, tenant_id, connector_name).await?)
    }

    async fn list_api_keys(&self, tenant_id: TenantId) -> Result<Vec<ApiKeyDescr>, DBError> {
        let manager = self.pool.get().await?;
        let stmt = manager
            .prepare_cached("SELECT id, name, scopes FROM api_key WHERE tenant_id = $1")
            .await?;
        let rows = manager.query(&stmt, &[&tenant_id.0]).await?;
        let mut result = Vec::with_capacity(rows.len());
        for row in rows {
            let id: ApiKeyId = ApiKeyId(row.get(0));
            let name: String = row.get(1);
            let vec: Vec<String> = row.get(2);
            let scopes = vec
                .iter()
                .map(|s| {
                    ApiPermission::from_str(s).expect("Unexpected ApiPermission string in the DB")
                })
                .collect();
            result.push(ApiKeyDescr { id, name, scopes });
        }
        Ok(result)
    }

    async fn get_api_key(&self, tenant_id: TenantId, name: &str) -> Result<ApiKeyDescr, DBError> {
        let manager = self.pool.get().await?;
        let stmt = manager
            .prepare_cached(
                "SELECT id, name, scopes FROM api_key WHERE tenant_id = $1 and name = $2",
            )
            .await?;
        let maybe_row = manager.query_opt(&stmt, &[&tenant_id.0, &name]).await?;
        if let Some(row) = maybe_row {
            let id: ApiKeyId = ApiKeyId(row.get(0));
            let name: String = row.get(1);
            let vec: Vec<String> = row.get(2);
            let scopes = vec
                .iter()
                .map(|s| {
                    ApiPermission::from_str(s).expect("Unexpected ApiPermission string in the DB")
                })
                .collect();

            Ok(ApiKeyDescr { id, name, scopes })
        } else {
            Err(DBError::UnknownApiKey {
                name: name.to_string(),
            })
        }
    }

    async fn delete_api_key(&self, tenant_id: TenantId, name: &str) -> Result<(), DBError> {
        let manager = self.pool.get().await?;
        let stmt = manager
            .prepare_cached("DELETE FROM api_key WHERE tenant_id = $1 AND name = $2")
            .await?;
        let res = manager.execute(&stmt, &[&tenant_id.0, &name]).await?;
        if res > 0 {
            Ok(())
        } else {
            Err(DBError::UnknownApiKey {
                name: name.to_string(),
            })
        }
    }

    async fn store_api_key_hash(
        &self,
        tenant_id: TenantId,
        id: Uuid,
        name: &str,
        key: &str,
        scopes: Vec<ApiPermission>,
    ) -> Result<(), DBError> {
        let mut hasher = sha::Sha256::new();
        hasher.update(key.as_bytes());
        let hash = openssl::base64::encode_block(&hasher.finish());
        let manager = self.pool.get().await?;
        let stmt = manager
            .prepare_cached(
                "INSERT INTO api_key (id, tenant_id, name, hash, scopes) VALUES ($1, $2, $3, $4, $5)",
            )
            .await?;
        let res = manager
            .execute(
                &stmt,
                &[
                    &id,
                    &tenant_id.0,
                    &name,
                    &hash,
                    &scopes
                        .iter()
                        .map(|scope| match scope {
                            ApiPermission::Read => API_PERMISSION_READ,
                            ApiPermission::Write => API_PERMISSION_WRITE,
                        })
                        .collect::<Vec<&str>>(),
                ],
            )
            .await
            .map_err(Self::maybe_unique_violation)
            .map_err(|e| {
                ProjectDB::maybe_tenant_id_foreign_key_constraint_err(e, tenant_id, None)
            })?;
        if res > 0 {
            Ok(())
        } else {
            Err(DBError::duplicate_key())
        }
    }

    async fn validate_api_key(
        &self,
        api_key: &str,
    ) -> Result<(TenantId, Vec<ApiPermission>), DBError> {
        let mut hasher = sha::Sha256::new();
        hasher.update(api_key.as_bytes());
        let hash = openssl::base64::encode_block(&hasher.finish());
        let manager = self.pool.get().await?;
        let stmt = manager
            .prepare_cached("SELECT tenant_id, scopes FROM api_key WHERE hash = $1")
            .await?;
        let res = manager
            .query_one(&stmt, &[&hash])
            .await
            .map_err(|_| DBError::InvalidKey)?;
        let tenant_id = TenantId(res.get(0));
        let vec: Vec<String> = res.get(1);
        let vec = vec
            .iter()
            .map(|s| ApiPermission::from_str(s).expect("Unexpected ApiPermission string in the DB"))
            .collect();
        Ok((tenant_id, vec))
    }

    async fn get_or_create_tenant_id(
        &self,
        tenant_name: String,
        provider: String,
    ) -> Result<TenantId, DBError> {
        let manager = self.pool.get().await?;
        let stmt = manager
            .prepare_cached("SELECT id FROM tenant WHERE tenant = $1 AND provider = $2")
            .await?;

        let res = manager.query_opt(&stmt, &[&tenant_name, &provider]).await?;
        match res {
            Some(row) => Ok(TenantId(row.get(0))),
            None => {
                self.create_tenant_if_not_exists(Uuid::now_v7(), tenant_name, provider)
                    .await
            }
        }
    }

    async fn create_tenant_if_not_exists(
        &self,
        tenant_id: Uuid,
        tenant_name: String,
        provider: String,
    ) -> Result<TenantId, DBError> {
        let manager = self.pool.get().await?;
        let stmt = manager
            .prepare_cached("INSERT INTO tenant (id, tenant, provider) VALUES ($1, $2, $3) ON CONFLICT (tenant, provider) DO UPDATE SET tenant = excluded.tenant, provider = excluded.provider RETURNING id")
            .await?;

        // Unfortunately, doing a read-if-exists-else-insert is not very ergonomic. To
        // do so in a single query requires us to do a redundant UPDATE on conflict,
        // where we set the tenant and provider values to their existing values
        // (excluded.{tenant, provider})
        let res = manager
            .query_one(&stmt, &[&tenant_id, &tenant_name, &provider])
            .await?;
        Ok(TenantId(res.get(0)))
    }

    async fn create_compiled_binary_ref(
        &self,
        program_id: ProgramId,
        version: Version,
        url: String,
    ) -> Result<(), DBError> {
        let conn = self.pool.get().await?;
        let stmt = conn
            .prepare_cached("INSERT INTO compiled_binary VALUES ($1, $2, $3)")
            .await?;

        let _res = conn
            .execute(&stmt, &[&program_id.0, &version.0, &url])
            .await?;
        Ok(())
    }

    async fn get_compiled_binary_ref(
        &self,
        program_id: ProgramId,
        version: Version,
    ) -> Result<Option<String>, DBError> {
        let conn = self.pool.get().await?;
        let stmt = conn
            .prepare_cached(
                "SELECT url FROM compiled_binary WHERE program_id = $1 AND version = $2",
            )
            .await?;
        let res = conn.query_opt(&stmt, &[&program_id.0, &version.0]).await?;
        Ok(res.map(|e| e.get(0)))
    }

    async fn delete_compiled_binary_ref(
        &self,
        program_id: ProgramId,
        version: Version,
    ) -> Result<(), DBError> {
        let conn = self.pool.get().await?;
        let stmt = conn
            .prepare_cached("DELETE FROM compiled_binary WHERE program_id = $1 AND version = $2")
            .await?;
        let _res = conn.execute(&stmt, &[&program_id.0, &version.0]).await?;
        Ok(())
    }

    async fn check_connection(&self) -> Result<(), DBError> {
        let conn = self.pool.get().await?;
        let stmt = conn.prepare_cached("SELECT 1").await?;
        let _res = conn.execute(&stmt, &[]).await?;
        Ok(())
    }

    async fn new_service(
        &self,
        tenant_id: TenantId,
        id: Uuid,
        name: &str,
        description: &str,
        config: &ServiceConfig,
        txn: Option<&Transaction<'_>>,
    ) -> Result<ServiceId, DBError> {
        Ok(service::new_service(self, tenant_id, id, name, description, config, txn).await?)
    }

    async fn list_services(
        &self,
        tenant_id: TenantId,
        filter_config_type: &Option<&str>,
    ) -> Result<Vec<ServiceDescr>, DBError> {
        Ok(service::list_services(self, tenant_id, filter_config_type).await?)
    }

    async fn get_service_by_id(
        &self,
        tenant_id: TenantId,
        service_id: ServiceId,
        txn: Option<&Transaction<'_>>,
    ) -> Result<ServiceDescr, DBError> {
        Ok(service::get_service_by_id(self, tenant_id, service_id, txn).await?)
    }

    async fn get_service_by_name(
        &self,
        tenant_id: TenantId,
        name: &str,
        txn: Option<&Transaction<'_>>,
    ) -> Result<ServiceDescr, DBError> {
        Ok(service::get_service_by_name(self, tenant_id, name, txn).await?)
    }

    async fn update_service(
        &self,
        tenant_id: TenantId,
        service_id: ServiceId,
        name: &Option<&str>,
        description: &Option<&str>,
        config: &Option<ServiceConfig>,
        txn: Option<&Transaction<'_>>,
    ) -> Result<(), DBError> {
        Ok(
            service::update_service(self, tenant_id, service_id, name, description, config, txn)
                .await?,
        )
    }

    async fn delete_service(&self, tenant_id: TenantId, service_name: &str) -> Result<(), DBError> {
        Ok(service::delete_service(self, tenant_id, service_name).await?)
    }
}

impl ProjectDB {
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
    async fn with_config(config: tokio_postgres::Config) -> Result<Self, DBError> {
        let db = ProjectDB::initialize(
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
    async fn connect_inner(
        connection_str: &str,
        #[cfg(feature = "pg-embed")] pg_inst: Option<pg_embed::postgres::PgEmbed>,
    ) -> Result<Self, DBError> {
        if !connection_str.starts_with("postgres") {
            panic!("Unsupported connection string {}", connection_str)
        }
        let config = connection_str.parse::<tokio_postgres::Config>()?;
        debug!("Opening connection to {:?}", connection_str);

        let db = ProjectDB::initialize(
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

    /// We check if a program is 'in use' by checking if it is referenced by a
    /// pipeline or if it is used by a pipeline deployment.
    pub(crate) async fn is_program_version_in_use(
        &self,
        program_id: Uuid,
        version: i64,
    ) -> Result<bool, DBError> {
        let conn = self.pool.get().await?;
        let stmt = conn
            .prepare_cached(
                "SELECT EXISTS(SELECT 1 FROM program prog
                               WHERE prog.id = $1 AND prog.version = $2)
                        OR
                        EXISTS(SELECT 1 FROM pipeline pl
                               JOIN program pg ON pg.id = $1 AND pg.id = pl.program_id)",
            )
            .await?;
        let row = conn.query_one(&stmt, &[&program_id, &version]).await?;
        Ok(row.get(0))
    }

    /// Helper to convert postgres error into a `DBError` if the underlying
    /// low-level error thrown by the database matches.
    fn maybe_unique_violation(err: PgError) -> DBError {
        if let Some(dberr) = err.as_db_error() {
            if dberr.code() == &tokio_postgres::error::SqlState::UNIQUE_VIOLATION {
                match dberr.constraint() {
                    Some("program_pkey") => DBError::unique_key_violation("program_pkey"),
                    Some("connector_pkey") => DBError::unique_key_violation("connector_pkey"),
                    Some("service_pkey") => DBError::unique_key_violation("service_pkey"),
                    Some("pipeline_pkey") => DBError::unique_key_violation("pipeline_pkey"),
                    Some("api_key_pkey") => DBError::unique_key_violation("api_key_pkey"),
                    Some("unique_hash") => DBError::duplicate_key(),
                    Some(_constraint) => DBError::DuplicateName,
                    None => DBError::DuplicateName,
                }
            } else {
                DBError::from(err)
            }
        } else {
            DBError::from(err)
        }
    }

    /// Helper to convert program_id foreign key constraint error into an
    /// user-friendly error message.
    fn maybe_program_id_not_found_foreign_key_constraint_err(
        err: DBError,
        program_id: Option<ProgramId>,
    ) -> DBError {
        if let DBError::PostgresError { error, .. } = &err {
            let db_err = error.as_db_error();
            if let Some(db_err) = db_err {
                if db_err.code() == &tokio_postgres::error::SqlState::FOREIGN_KEY_VIOLATION
                    && db_err.constraint() == Some("pipeline_program_id_tenant_id_fkey")
                {
                    if let Some(program_id) = program_id {
                        return DBError::UnknownProgram { program_id };
                    } else {
                        unreachable!("program_id cannot be none");
                    }
                }
            }
        }

        err
    }

    /// Helper to convert program_id foreign key constraint error into an
    /// user-friendly error message.
    fn maybe_program_id_in_use_foreign_key_constraint_err(
        err: DBError,
        program_name: Option<&str>,
    ) -> DBError {
        if let DBError::PostgresError { error, .. } = &err {
            let db_err = error.as_db_error();
            if let Some(db_err) = db_err {
                if db_err.code() == &tokio_postgres::error::SqlState::FOREIGN_KEY_VIOLATION
                    && db_err.constraint() == Some("pipeline_program_id_tenant_id_fkey")
                {
                    if let Some(program_name) = program_name {
                        return DBError::ProgramInUseByPipeline {
                            program_name: program_name.to_string(),
                        };
                    } else {
                        unreachable!("program_id cannot be none");
                    }
                }
            }
        }

        err
    }

    /// Helper to convert pipeline_id foreign key constraint error into an
    /// user-friendly error message.
    fn maybe_pipeline_id_foreign_key_constraint_err(
        err: DBError,
        pipeline_id: PipelineId,
    ) -> DBError {
        if let DBError::PostgresError { error, .. } = &err {
            let db_err = error.as_db_error();
            if let Some(db_err) = db_err {
                if db_err.code() == &tokio_postgres::error::SqlState::FOREIGN_KEY_VIOLATION
                    && (db_err.constraint() == Some("pipeline_pipeline_id_fkey")
                        || db_err.constraint() == Some("attached_connector_pipeline_id_fkey"))
                {
                    return DBError::UnknownPipeline { pipeline_id };
                }
            }
        }

        err
    }

    /// Helper to convert tenant_id foreign key constraint error into an
    /// user-friendly error message.
    fn maybe_tenant_id_foreign_key_constraint_err(
        err: DBError,
        tenant_id: TenantId,
        missing_id: Option<Uuid>,
    ) -> DBError {
        if let DBError::PostgresError { error, .. } = &err {
            let db_err = error.as_db_error();
            if let Some(db_err) = db_err {
                if db_err.code() == &tokio_postgres::error::SqlState::FOREIGN_KEY_VIOLATION {
                    if let Some(constraint_name) = db_err.constraint() {
                        if constraint_name.ends_with("pipeline_program_id_tenant_id_fkey") {
                            return DBError::UnknownProgram {
                                program_id: ProgramId(missing_id.unwrap()),
                            };
                        }
                        if constraint_name.ends_with("tenant_id_fkey") {
                            return DBError::UnknownTenant { tenant_id };
                        }
                    }
                }
            }
        }

        err
    }

    // TODO: Should be part of the Storage trait
    pub(crate) async fn pipeline_config(
        &self,
        tenant_id: TenantId,
        pipeline_name: &str,
    ) -> Result<PipelineConfig, DBError> {
        pipeline::pipeline_config(self, tenant_id, pipeline_name).await
    }

    // TODO: Should be part of the Storage trait
    pub(crate) async fn is_pipeline_deployable(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
    ) -> Result<
        (
            PipelineDescr,
            ProgramDescr,
            Vec<ConnectorDescr>,
            Vec<Vec<ServiceDescr>>,
        ),
        DBError,
    > {
        pipeline::is_pipeline_deployable(self, tenant_id, pipeline_id).await
    }

    pub(crate) async fn create_or_replace_program(
        &self,
        tenant_id: TenantId,
        program_name: &str,
        program_description: &str,
        program_code: &str,
    ) -> Result<(bool, ProgramId, Version), DBError> {
        let mut manager = self.pool.get().await?;
        let txn = manager.transaction().await?;
        let program = self
            .get_program_by_name(tenant_id, program_name, false, Some(&txn))
            .await;
        let ret = match program {
            Ok(p) => Ok(self
                .update_program(
                    tenant_id,
                    p.program_id,
                    &Some(program_name.to_string()),
                    &Some(program_description.to_string()),
                    &Some(program_code.to_string()),
                    &None,
                    &None,
                    None,
                    Some(&txn),
                )
                .await
                .map(|v| (false, p.program_id, v))?),
            Err(DBError::UnknownProgramName { program_name }) => Ok(self
                .new_program(
                    tenant_id,
                    Uuid::now_v7(),
                    &program_name,
                    program_description,
                    program_code,
                    Some(&txn),
                )
                .await
                .map(|(p, v)| (true, p, v))?),
            Err(e) => Err(e),
        }?;
        txn.commit().await?;
        Ok(ret)
    }

    /// Updates a program by name by, within a transaction, resolving
    /// the name to its respective identifier and proceeding to use
    /// that in the update query.
    pub async fn update_program_by_name(
        &self,
        tenant_id: TenantId,
        original_name: &str,
        new_name: &Option<String>,
        description: &Option<String>,
        program_code: &Option<String>,
        guard: Option<Version>,
    ) -> Result<Version, DBError> {
        let mut manager = self.pool.get().await?;
        let txn = manager.transaction().await?;
        let program = self
            .get_program_by_name(tenant_id, original_name, false, Some(&txn))
            .await?;
        let version = self
            .update_program(
                tenant_id,
                program.program_id,
                new_name,
                description,
                program_code,
                &None,
                &None,
                guard,
                Some(&txn),
            )
            .await?;
        txn.commit().await?;
        Ok(version)
    }

    pub(crate) async fn create_or_replace_pipeline(
        &self,
        tenant_id: TenantId,
        pipeline_name: &str,
        program_name: &Option<String>,
        pipeline_description: &str,
        config: &RuntimeConfig,
        connectors: &Option<Vec<AttachedConnector>>,
    ) -> Result<(bool, PipelineId, Version), DBError> {
        let mut manager = self.pool.get().await?;
        let txn = manager.transaction().await?;
        let pipeline = self
            .get_pipeline_descr_by_name(tenant_id, pipeline_name, Some(&txn))
            .await;
        let res = match pipeline {
            Ok(p) => Ok(self
                .update_pipeline(
                    tenant_id,
                    p.pipeline_id,
                    program_name,
                    pipeline_name,
                    pipeline_description,
                    &Some(config.clone()),
                    connectors,
                    Some(&txn),
                )
                .await
                .map(|v| (false, p.pipeline_id, v))?),
            Err(DBError::UnknownPipelineName { pipeline_name }) => Ok(self
                .new_pipeline(
                    tenant_id,
                    Uuid::now_v7(),
                    program_name,
                    &pipeline_name,
                    pipeline_description,
                    config,
                    connectors,
                    Some(&txn),
                )
                .await
                .map(|(p, v)| (true, p, v))?),
            Err(e) => Err(e),
        }?;
        txn.commit().await?;
        Ok(res)
    }

    pub(crate) async fn create_or_replace_connector(
        &self,
        tenant_id: TenantId,
        connector_name: &str,
        description: &str,
        config: &ConnectorConfig,
    ) -> Result<(bool, ConnectorId), DBError> {
        let mut manager = self.pool.get().await?;
        let txn = manager.transaction().await?;
        let connector = self
            .get_connector_by_name(tenant_id, connector_name, Some(&txn))
            .await;
        let res = match connector {
            Ok(c) => Ok(self
                .update_connector(
                    tenant_id,
                    c.connector_id,
                    &Some(connector_name),
                    &Some(description),
                    &Some(config.clone()),
                    Some(&txn),
                )
                .await
                .map(|_| (false, c.connector_id))?),
            Err(DBError::UnknownConnectorName { connector_name }) => Ok(self
                .new_connector(
                    tenant_id,
                    Uuid::now_v7(),
                    &connector_name,
                    description,
                    config,
                    Some(&txn),
                )
                .await
                .map(|c| (true, c))?),
            Err(e) => Err(e),
        }?;
        txn.commit().await?;
        Ok(res)
    }

    /// Updates a connector by name by, within a transaction, resolving
    /// the name to its respective identifier and proceeding to use
    /// that in the update query.
    pub async fn update_connector_by_name(
        &self,
        tenant_id: TenantId,
        original_name: &str,
        new_name: &Option<&str>,
        description: &Option<&str>,
        config: &Option<ConnectorConfig>,
    ) -> Result<(), DBError> {
        let mut manager = self.pool.get().await?;
        let txn = manager.transaction().await?;
        let connector = self
            .get_connector_by_name(tenant_id, original_name, Some(&txn))
            .await?;
        self.update_connector(
            tenant_id,
            connector.connector_id,
            new_name,
            description,
            config,
            Some(&txn),
        )
        .await?;
        txn.commit().await?;
        Ok(())
    }

    /// Run database migrations
    pub async fn run_migrations(&self) -> Result<(), DBError> {
        info!("Running DB migrations");
        let mut client = self.pool.get().await?;
        embedded::migrations::runner()
            .run_async(&mut **client)
            .await?;
        let default_tenant = TenantRecord::default();
        self.create_tenant_if_not_exists(
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
            .fold(std::u32::MIN, |a, b| a.max(b));
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

    /// Create or replace a service by first, within a transaction,
    /// attempting to resolve the name to its respective identifier.
    /// If the service with that name already exists, it is updated.
    /// If not, the service is newly created.
    ///
    /// Returns a boolean indicating whether the service was created
    /// and the respective service identifier.
    pub async fn create_or_replace_service(
        &self,
        tenant_id: TenantId,
        name: &str,
        description: &str,
        config: &ServiceConfig,
    ) -> Result<(bool, ServiceId), DBError> {
        let mut manager = self.pool.get().await?;
        let txn = manager.transaction().await?;
        let result = match self.get_service_by_name(tenant_id, name, Some(&txn)).await {
            Ok(service) => {
                self.update_service(
                    tenant_id,
                    service.service_id,
                    &Some(name),
                    &Some(description),
                    &Some(config.clone()),
                    Some(&txn),
                )
                .await?;
                Ok((false, service.service_id))
            }
            Err(DBError::UnknownServiceName { .. }) => {
                let service_id = self
                    .new_service(
                        tenant_id,
                        Uuid::now_v7(),
                        name,
                        description,
                        config,
                        Some(&txn),
                    )
                    .await?;
                Ok((true, service_id))
            }
            Err(e) => Err(e),
        }?;
        txn.commit().await?;
        Ok(result)
    }

    /// Updates a service by name by, within a transaction, resolving
    /// the name to its respective identifier and proceeding to use
    /// that in the update query.
    pub async fn update_service_by_name(
        &self,
        tenant_id: TenantId,
        original_name: &str,
        new_name: &Option<&str>,
        description: &Option<&str>,
        config: &Option<ServiceConfig>,
    ) -> Result<(), DBError> {
        let mut manager = self.pool.get().await?;
        let txn = manager.transaction().await?;
        let service = self
            .get_service_by_name(tenant_id, original_name, Some(&txn))
            .await?;
        self.update_service(
            tenant_id,
            service.service_id,
            new_name,
            description,
            config,
            Some(&txn),
        )
        .await?;
        txn.commit().await?;
        Ok(())
    }
}
