use crate::{config::ManagerConfig, ProgramStatus};
use anyhow::{anyhow, Error as AnyError, Result as AnyResult};
use async_trait::async_trait;
use chrono::{DateTime, NaiveDateTime, Utc};
use deadpool_postgres::{Manager, Pool, RecyclingMethod, Transaction};
use futures_util::TryFutureExt;
use log::{debug, error};
use openssl::sha;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{error::Error as StdError, fmt, fmt::Display};
use storage::Storage;
use tokio_postgres::NoTls;
use utoipa::ToSchema;
use uuid::Uuid;

#[cfg(test)]
pub(crate) mod test;

#[cfg(feature = "pg-embed")]
mod pg_setup;
pub(crate) mod storage;

/// Project database API.
///
/// The API assumes that the caller holds a database lock, and therefore
/// doesn't use transactions (and hence doesn't need to deal with conflicts).
///
/// # Compilation queue
///
/// We use the `status` and `status_since` columns to maintain the compilation
/// queue.  A program is enqueued for compilation by setting its status to
/// [`ProgramStatus::Pending`].  The `status_since` column is set to the current
/// time, which determines the position of the program in the queue.
pub(crate) struct ProjectDB {
    pool: Pool,
    // Used in dev mode for having an embedded Postgres DB live through the
    // lifetime of the program.
    #[cfg(feature = "pg-embed")]
    #[allow(dead_code)] // It has to stay alive until ProjectDB is dropped.
    pg_inst: Option<pg_embed::postgres::PgEmbed>,
}

/// Unique program id.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Ord, PartialOrd, Serialize, Deserialize, ToSchema)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[repr(transparent)]
#[serde(transparent)]
pub(crate) struct ProgramId(
    #[cfg_attr(test, proptest(strategy = "test::limited_uuid()"))] pub Uuid,
);
impl Display for ProgramId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Unique pipeline id.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Ord, PartialOrd, Serialize, Deserialize, ToSchema)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[repr(transparent)]
#[serde(transparent)]
pub(crate) struct PipelineId(
    #[cfg_attr(test, proptest(strategy = "test::limited_uuid()"))] pub Uuid,
);
impl Display for PipelineId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Unique connector id.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Ord, PartialOrd, Serialize, Deserialize, ToSchema)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[repr(transparent)]
#[serde(transparent)]
pub(crate) struct ConnectorId(
    #[cfg_attr(test, proptest(strategy = "test::limited_uuid()"))] pub Uuid,
);
impl Display for ConnectorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Unique attached connector id.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[repr(transparent)]
#[serde(transparent)]
pub(crate) struct AttachedConnectorId(
    #[cfg_attr(test, proptest(strategy = "test::limited_uuid()"))] pub Uuid,
);
impl Display for AttachedConnectorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Version number.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[repr(transparent)]
#[serde(transparent)]
pub(crate) struct Version(#[cfg_attr(test, proptest(strategy = "1..25i64"))] i64);
impl Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug)]
pub(crate) enum DBError {
    UnknownProgram(ProgramId),
    OutdatedProgramVersion(Version),
    UnknownPipeline(PipelineId),
    UnknownConnector(ConnectorId),
    UnknownName(String),
    DuplicateName,
    DuplicateKey,
    InvalidKey,
    UniqueKeyViolation(&'static str),
    UnknownPipelineStatus,
}

impl Display for DBError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DBError::UnknownProgram(program_id) => write!(f, "Unknown program id '{program_id}'"),
            DBError::OutdatedProgramVersion(version) => {
                write!(f, "Outdated program version '{version}'")
            }
            DBError::UnknownPipeline(pipeline_id) => {
                write!(f, "Unknown pipeline id '{pipeline_id}'")
            }
            DBError::UnknownConnector(connector_id) => {
                write!(f, "Unknown connector id '{connector_id}'")
            }
            DBError::DuplicateName => {
                write!(f, "An entity with this name already exists")
            }
            DBError::DuplicateKey => {
                write!(f, "A key with the same hash already exists")
            }
            DBError::InvalidKey => {
                write!(f, "Could not validate API")
            }
            DBError::UnknownName(name) => {
                write!(f, "An entity with name {name} was not found")
            }
            DBError::UniqueKeyViolation(id) => write!(f, "Unique key violation for '{id}'"),
            DBError::UnknownPipelineStatus => write!(f, "Unknown pipeline status encountered"),
        }
    }
}

impl StdError for DBError {}

/// The database encodes program status using two columns: `status`, which has
/// type `string`, but acts as an enum, and `error`, only used if `status` is
/// one of `"sql_error"` or `"rust_error"`.
impl ProgramStatus {
    /// Decode `ProgramStatus` from the values of `error` and `status` columns.
    fn from_columns(status_string: Option<&str>, error_string: Option<String>) -> AnyResult<Self> {
        match status_string {
            None => Ok(Self::None),
            Some("success") => Ok(Self::Success),
            Some("pending") => Ok(Self::Pending),
            Some("compiling_sql") => Ok(Self::CompilingSql),
            Some("compiling_rust") => Ok(Self::CompilingRust),
            Some("sql_error") => {
                let error = error_string.unwrap_or_default();
                if let Ok(messages) = serde_json::from_str(&error) {
                    Ok(Self::SqlError(messages))
                } else {
                    error!("Expected valid json for SqlCompilerMessage but got {:?}, did you update the struct without adjusting the database?", error);
                    Ok(Self::SystemError(error))
                }
            }
            Some("rust_error") => Ok(Self::RustError(error_string.unwrap_or_default())),
            Some("system_error") => Ok(Self::SystemError(error_string.unwrap_or_default())),
            Some(status) => Err(AnyError::msg(format!("invalid status string '{status}'"))),
        }
    }
    fn to_columns(&self) -> (Option<String>, Option<String>) {
        match self {
            ProgramStatus::None => (None, None),
            ProgramStatus::Success => (Some("success".to_string()), None),
            ProgramStatus::Pending => (Some("pending".to_string()), None),
            ProgramStatus::CompilingSql => (Some("compiling_sql".to_string()), None),
            ProgramStatus::CompilingRust => (Some("compiling_rust".to_string()), None),
            ProgramStatus::SqlError(error) => {
                if let Ok(error_string) = serde_json::to_string(&error) {
                    (Some("sql_error".to_string()), Some(error_string))
                } else {
                    error!("Expected valid json for SqlError, but got {:?}", error);
                    (Some("sql_error".to_string()), None)
                }
            }
            ProgramStatus::RustError(error) => {
                (Some("rust_error".to_string()), Some(error.clone()))
            }
            ProgramStatus::SystemError(error) => {
                (Some("system_error".to_string()), Some(error.clone()))
            }
        }
    }
}

/// Program descriptor.
#[derive(Serialize, ToSchema, Debug, Eq, PartialEq, Clone)]
pub(crate) struct ProgramDescr {
    /// Unique program id.
    pub program_id: ProgramId,
    /// Program name (doesn't have to be unique).
    pub name: String,
    /// Program description.
    pub description: String,
    /// Program version, incremented every time program code is modified.
    pub version: Version,
    /// Program compilation status.
    pub status: ProgramStatus,
    /// A JSON description of the SQL tables and view declarations including
    /// field names and types.
    ///
    /// The schema is set/updated whenever the `status` field reaches >=
    /// `ProgramStatus::CompilingRust`.
    ///
    /// # Example
    ///
    /// The given SQL program:
    ///
    /// ```no_run
    /// CREATE TABLE USERS ( name varchar );
    /// CREATE VIEW OUTPUT_USERS as SELECT * FROM USERS;
    /// ```
    ///
    /// Would lead the following JSON string in `schema`:
    ///
    /// ```no_run
    /// {
    ///   "inputs": [{
    ///       "name": "USERS",
    ///       "fields": [{ "name": "NAME", "type": "VARCHAR", "nullable": true }]
    ///     }],
    ///   "outputs": [{
    ///       "name": "OUTPUT_USERS",
    ///       "fields": [{ "name": "NAME", "type": "VARCHAR", "nullable": true }]
    ///     }]
    /// }
    /// ```
    pub schema: Option<String>,
}

/// Lifecycle of a pipeline.
#[derive(Serialize, ToSchema, Eq, PartialEq, Debug, Clone, Copy)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub(crate) enum PipelineStatus {
    Shutdown,
    Deployed,
    FailedToDeploy,
    Running,
    Paused,
    // Failure isn't in-use yet -- we don't have failure detection.
    Failed,
}

impl TryFrom<String> for PipelineStatus {
    type Error = DBError;
    fn try_from(value: String) -> Result<Self, DBError> {
        match value.as_str() {
            "shutdown" => Ok(Self::Shutdown),
            "deployed" => Ok(Self::Deployed),
            "failedtodeploy" => Ok(Self::FailedToDeploy),
            "running" => Ok(Self::Running),
            "paused" => Ok(Self::Paused),
            "failed" => Ok(Self::Failed),
            _ => Err(DBError::UnknownPipelineStatus),
        }
    }
}

impl From<PipelineStatus> for &'static str {
    fn from(val: PipelineStatus) -> Self {
        match val {
            PipelineStatus::Shutdown => "shutdown",
            PipelineStatus::Deployed => "deployed",
            PipelineStatus::FailedToDeploy => "failedtodeploy",
            PipelineStatus::Running => "running",
            PipelineStatus::Paused => "paused",
            PipelineStatus::Failed => "failed",
        }
    }
}

/// Pipeline descriptor.
#[derive(Serialize, ToSchema, Eq, PartialEq, Debug, Clone)]
pub(crate) struct PipelineDescr {
    pub pipeline_id: PipelineId,
    pub program_id: Option<ProgramId>,
    pub version: Version,
    pub name: String,
    pub description: String,
    pub config: String,
    pub attached_connectors: Vec<AttachedConnector>,
    pub status: PipelineStatus,
    pub port: u16,
    pub created: Option<DateTime<Utc>>,
}

/// Format to add attached connectors during a config update.
#[derive(Deserialize, Serialize, ToSchema, Eq, PartialEq, Debug, Clone)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub(crate) struct AttachedConnector {
    /// A unique identifier for this attachement.
    pub name: String,
    /// Is this an input or an output?
    pub is_input: bool,
    /// The id of the connector to attach.
    pub connector_id: ConnectorId,
    /// The YAML config for this attached connector.
    pub config: String,
}

/// Connector descriptor.
#[derive(Serialize, ToSchema, Debug, Clone, Eq, PartialEq)]
pub(crate) struct ConnectorDescr {
    pub connector_id: ConnectorId,
    pub name: String,
    pub description: String,
    pub config: String,
}

/// Permission types for invoking pipeline manager APIs
#[derive(Serialize, ToSchema, Debug, Clone, Eq, PartialEq)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub(crate) enum ApiPermission {
    Read,
    Write,
}

// Helper type for composable error handling when dealing
// with DB errors
enum EitherError {
    Tokio(tokio_postgres::Error),
    Any(AnyError),
}

impl std::convert::From<EitherError> for AnyError {
    fn from(value: EitherError) -> Self {
        match value {
            EitherError::Tokio(e) => anyhow!(e),
            EitherError::Any(e) => e,
        }
    }
}

fn convert_bigint_to_time(created_secs: Option<i64>) -> Result<Option<DateTime<Utc>>, AnyError> {
    if let Some(created_secs) = created_secs {
        let created_naive =
            NaiveDateTime::from_timestamp_millis(created_secs * 1000).ok_or_else(|| {
                AnyError::msg(format!(
                    "Invalid timestamp in 'pipeline.created' column: {created_secs}"
                ))
            })?;

        Ok(Some(DateTime::<Utc>::from_utc(created_naive, Utc)))
    } else {
        Ok(None)
    }
}

// The goal for these methods is to avoid multiple DB interactions as much as
// possible and if not, use transactions
#[async_trait]
impl Storage for ProjectDB {
    async fn reset_program_status(&self) -> AnyResult<()> {
        self.pool
            .get()
            .await?
            .execute(
                "UPDATE program SET status = NULL, error = NULL, schema = NULL",
                &[],
            )
            .await?;
        Ok(())
    }

    async fn list_programs(&self) -> AnyResult<Vec<ProgramDescr>> {
        let rows = self
            .pool
            .get()
            .await?
            .query(
                r#"SELECT id, name, description, version, status, error, schema FROM program"#,
                &[],
            )
            .await?;

        let mut result = Vec::with_capacity(rows.len());
        for row in rows {
            let status: Option<String> = row.get(4);
            let error: Option<String> = row.get(5);
            let status = ProgramStatus::from_columns(status.as_deref(), error)?;
            let schema: Option<String> = row.get(6);

            result.push(ProgramDescr {
                program_id: ProgramId(row.get(0)),
                name: row.get(1),
                description: row.get(2),
                version: Version(row.get(3)),
                schema,
                status,
            });
        }

        Ok(result)
    }

    async fn program_code(&self, program_id: ProgramId) -> AnyResult<(ProgramDescr, String)> {
        let row = self.pool.get().await?.query_opt(
            "SELECT name, description, version, status, error, code, schema FROM program WHERE id = $1", &[&program_id.0]
        )
        .await?
        .ok_or(DBError::UnknownProgram(program_id))?;

        let name: String = row.get(0);
        let description: String = row.get(1);
        let version: Version = Version(row.get(2));
        let status: Option<String> = row.get(3);
        let error: Option<String> = row.get(4);
        let code: String = row.get(5);
        let schema: Option<String> = row.get(6);

        let status = ProgramStatus::from_columns(status.as_deref(), error)?;

        Ok((
            ProgramDescr {
                program_id,
                name,
                description,
                version,
                status,
                schema,
            },
            code,
        ))
    }

    async fn new_program(
        &self,
        id: Uuid,
        program_name: &str,
        program_description: &str,
        program_code: &str,
    ) -> AnyResult<(ProgramId, Version)> {
        debug!("new_program {program_name} {program_description} {program_code}");
        self.pool.get().await?.execute(
                    "INSERT INTO program (id, version, name, description, code, schema, status, error, status_since)
                        VALUES($1, 1, $2, $3, $4, NULL, NULL, NULL, now());",
                &[&id, &program_name, &program_description, &program_code]
            )
            .await
            .map_err(|e| ProjectDB::maybe_unique_violation(EitherError::Tokio(e)))?;

        Ok((ProgramId(id), Version(1)))
    }

    /// Update program name, description and, optionally, code.
    /// XXX: Description should be optional too
    async fn update_program(
        &self,
        program_id: ProgramId,
        program_name: &str,
        program_description: &str,
        program_code: &Option<String>,
    ) -> AnyResult<Version> {
        let row = match program_code {
            Some(code) => {
                // Only increment `version` if new code actually differs from the
                // current version.
                self.pool.get().await?
                    .query_opt(
                        "UPDATE program
                            SET
                                version = (CASE WHEN code = $3 THEN version ELSE version + 1 END),
                                name = $1,
                                description = $2,
                                code = $3,
                                status = (CASE WHEN code = $3 THEN status ELSE NULL END),
                                error = (CASE WHEN code = $3 THEN error ELSE NULL END),
                                schema = (CASE WHEN code = $3 THEN schema ELSE NULL END)
                        WHERE id = $4
                        RETURNING version
                    ",
                        &[
                            &program_name,
                            &program_description,
                            &code,
                            &program_id.0,
                        ],
                    )
                    .await
                    .map_err(|e| ProjectDB::maybe_unique_violation(EitherError::Tokio(e)))?
            }
            _ => {
                self.pool.get().await?
                    .query_opt(
                        "UPDATE program SET name = $1, description = $2 WHERE id = $3 RETURNING version",
                        &[&program_name, &program_description, &program_id.0],
                    )
                    .await
                    .map_err(|e| ProjectDB::maybe_unique_violation(EitherError::Tokio(e)))?
            }
        };

        if let Some(row) = row {
            Ok(Version(row.get(0)))
        } else {
            Err(DBError::UnknownProgram(program_id).into())
        }
    }

    /// Retrieve program descriptor.
    ///
    /// Returns `None` if `program_id` is not found in the database.
    async fn get_program_if_exists(
        &self,
        program_id: ProgramId,
    ) -> AnyResult<Option<ProgramDescr>> {
        let row = self.pool.get().await?.query_opt(
                "SELECT name, description, version, status, error, schema FROM program WHERE id = $1",
                &[&program_id.0],
            )
            .await?;

        if let Some(row) = row {
            let name: String = row.get(0);
            let description: String = row.get(1);
            let version: Version = Version(row.get(2));
            let status: Option<String> = row.get(3);
            let error: Option<String> = row.get(4);
            let schema: Option<String> = row.get(5);
            let status = ProgramStatus::from_columns(status.as_deref(), error)?;

            Ok(Some(ProgramDescr {
                program_id,
                name,
                description,
                version,
                status,
                schema,
            }))
        } else {
            Ok(None)
        }
    }

    /// Lookup program by name.
    async fn lookup_program(&self, program_name: &str) -> AnyResult<Option<ProgramDescr>> {
        let row = self.pool.get().await?.query_opt(
                "SELECT id, description, version, status, error, schema FROM program WHERE name = $1",
                &[&program_name],
            )
            .await?;

        if let Some(row) = row {
            let program_id: ProgramId = ProgramId(row.get(0));
            let description: String = row.get(1);
            let version: Version = Version(row.get(2));
            let status: Option<String> = row.get(3);
            let error: Option<String> = row.get(4);
            let schema: Option<String> = row.get(5);
            let status = ProgramStatus::from_columns(status.as_deref(), error)?;

            Ok(Some(ProgramDescr {
                program_id,
                name: program_name.to_string(),
                description,
                version,
                status,
                schema,
            }))
        } else {
            Ok(None)
        }
    }

    async fn set_program_status(
        &self,
        program_id: ProgramId,
        status: ProgramStatus,
    ) -> AnyResult<()> {
        let (status, error) = status.to_columns();
        self.pool.get().await?.execute(
                "UPDATE program SET status = $1, error = $2, schema = '', status_since = now() WHERE id = $3",
            &[&status, &error, &program_id.0])
            .await?;

        Ok(())
    }

    async fn set_program_status_guarded(
        &self,
        program_id: ProgramId,
        expected_version: Version,
        status: ProgramStatus,
    ) -> AnyResult<()> {
        let (status, error) = status.to_columns();
        // We could perform the guard in the WHERE clause, but that does not
        // tell us whether the ID existed or not.
        // Instead, we use a case statement for the guard.
        let row = self
            .pool
            .get()
            .await?
            .query_opt(
                "UPDATE program SET
                 status = (CASE WHEN version = $4 THEN $1 ELSE status END),
                 error = (CASE WHEN version = $4 THEN $2 ELSE error END),
                 status_since = (CASE WHEN version = $4 THEN now()
                                 ELSE status_since END)
                 WHERE id = $3 RETURNING id",
                &[&status, &error, &program_id.0, &expected_version.0],
            )
            .await?;
        if row.is_none() {
            Err(anyhow!(DBError::UnknownProgram(program_id)))
        } else {
            Ok(())
        }
    }

    async fn set_program_schema(&self, program_id: ProgramId, schema: String) -> AnyResult<()> {
        self.pool
            .get()
            .await?
            .execute(
                "UPDATE program SET schema = $1 WHERE id = $2",
                &[&schema, &program_id.0],
            )
            .await?;

        Ok(())
    }

    async fn delete_program(&self, program_id: ProgramId) -> AnyResult<()> {
        let res = self
            .pool
            .get()
            .await?
            .execute("DELETE FROM program WHERE id = $1", &[&program_id.0])
            .await?;

        if res > 0 {
            Ok(())
        } else {
            Err(anyhow!(DBError::UnknownProgram(program_id)))
        }
    }

    async fn next_job(&self) -> AnyResult<Option<(ProgramId, Version)>> {
        // Find the oldest pending project.
        let res = self.pool.get().await?.query("SELECT id, version FROM program WHERE status = 'pending' AND status_since = (SELECT min(status_since) FROM program WHERE status = 'pending')", &[])
            .await?;

        if let Some(row) = res.get(0) {
            let program_id: ProgramId = ProgramId(row.get(0));
            let version: Version = Version(row.get(1));
            Ok(Some((program_id, version)))
        } else {
            Ok(None)
        }
    }

    async fn list_pipelines(&self) -> AnyResult<Vec<PipelineDescr>> {
        let rows = self
            .pool
            .get()
            .await?
            // For every pipeline, produce a JSON representation of all connectors
            .query(
                "SELECT p.id, version, p.name, description, p.config, program_id,
                        created, port, status,
                COALESCE(json_agg(json_build_object('name', ac.name,
                                                    'connector_id', connector_id,
                                                    'config', ac.config,
                                                    'is_input', is_input))
                                FILTER (WHERE ac.name IS NOT NULL),
                        '[]')

                FROM pipeline p
                LEFT JOIN attached_connector ac on p.id = ac.pipeline_id
                GROUP BY p.id;",
                &[],
            )
            .await?;
        let mut result = Vec::with_capacity(rows.len());
        for row in rows {
            let pipeline_id = PipelineId(row.get(0));
            let program_id = row.get::<_, Option<Uuid>>(5).map(ProgramId);
            let created = convert_bigint_to_time(row.get(6))?;
            let attached_connectors = self.json_to_attached_connectors(row.get(9)).await?;

            result.push(PipelineDescr {
                pipeline_id,
                version: Version(row.get(1)),
                name: row.get(2),
                description: row.get(3),
                config: row.get(4),
                program_id,
                attached_connectors,
                created,
                port: row.get::<_, Option<i16>>(7).unwrap_or(0) as u16,
                status: row.get::<_, String>(8).try_into()?,
            });
        }

        Ok(result)
    }

    async fn get_pipeline_by_id(&self, pipeline_id: PipelineId) -> AnyResult<PipelineDescr> {
        let row = self
            .pool
            .get()
            .await?
            .query_opt(
                "SELECT p.id, version, p.name as cname, description, p.config, program_id,
                        created, port, status,
                COALESCE(json_agg(json_build_object('name', ac.name,
                                                    'connector_id', connector_id,
                                                    'config', ac.config,
                                                    'is_input', is_input))
                                FILTER (WHERE ac.name IS NOT NULL),
                        '[]')
                FROM pipeline p
                LEFT JOIN attached_connector ac on p.id = ac.pipeline_id
                WHERE p.id = $1
                GROUP BY p.id
                ",
                &[&pipeline_id.0],
            )
            .await?;

        if let Some(row) = row {
            let program_id = row.get::<_, Option<Uuid>>(5).map(ProgramId);
            let created = convert_bigint_to_time(row.get(6))?;

            let descr = PipelineDescr {
                pipeline_id,
                program_id,
                version: Version(row.get(1)),
                name: row.get(2),
                description: row.get(3),
                config: row.get(4),
                attached_connectors: self.json_to_attached_connectors(row.get(9)).await?,
                created,
                port: row.get::<_, Option<i16>>(7).unwrap_or(0) as u16,
                status: row.get::<_, String>(8).try_into()?,
            };

            Ok(descr)
        } else {
            Err(DBError::UnknownPipeline(pipeline_id).into())
        }
    }

    async fn get_pipeline_by_name(&self, name: String) -> AnyResult<PipelineDescr> {
        let row = self
            .pool
            .get()
            .await?
            .query_opt(
                "SELECT p.id, version, description, p.config, program_id,
                        created, port, status,
                COALESCE(json_agg(json_build_object('name', ac.name,
                                                    'connector_id', connector_id,
                                                    'config', ac.config,
                                                    'is_input', is_input))
                                FILTER (WHERE ac.name IS NOT NULL),
                        '[]')
                FROM pipeline p
                LEFT JOIN attached_connector ac on p.id = ac.pipeline_id
                WHERE p.name = $1
                GROUP BY p.id
                ",
                &[&name],
            )
            .await?;

        if let Some(row) = row {
            let pipeline_id = PipelineId(row.get(0));
            let program_id = row.get::<_, Option<Uuid>>(4).map(ProgramId);
            let created = convert_bigint_to_time(row.get(5))?;

            let descr = PipelineDescr {
                pipeline_id,
                program_id,
                version: Version(row.get(1)),
                name,
                description: row.get(2),
                config: row.get(3),
                attached_connectors: self.json_to_attached_connectors(row.get(8)).await?,
                created,
                port: row.get::<_, Option<i16>>(6).unwrap_or(0) as u16,
                status: row.get::<_, String>(7).try_into()?,
            };

            Ok(descr)
        } else {
            Err(DBError::UnknownName(name).into())
        }
    }

    // XXX: Multiple statements
    async fn new_pipeline(
        &self,
        id: Uuid,
        program_id: Option<ProgramId>,
        pipline_name: &str,
        pipeline_description: &str,
        config: &str,
        connectors: &Option<Vec<AttachedConnector>>,
    ) -> AnyResult<(PipelineId, Version)> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        txn.execute(
            "INSERT INTO pipeline (id, program_id, version, name, description, config, status) VALUES($1, $2, 1, $3, $4, $5, 'shutdown')",
            &[&id, &program_id.map(|id| id.0),
            &pipline_name,
            &pipeline_description,
            &config])
            .await
            .map_err(|e| ProjectDB::maybe_unique_violation(EitherError::Tokio(e)))
            .map_err(|e| ProjectDB::maybe_program_id_foreign_key_constraint_err(e, program_id))?;
        let pipeline_id = PipelineId(id);

        if let Some(connectors) = connectors {
            // Add the connectors.
            for ac in connectors {
                self.attach_connector(&txn, pipeline_id, ac).await?;
            }
        }
        txn.commit().await?;

        Ok((pipeline_id, Version(1)))
    }

    // XXX: Multiple statements
    async fn update_pipeline(
        &self,
        pipeline_id: PipelineId,
        program_id: Option<ProgramId>,
        pipline_name: &str,
        pipeline_description: &str,
        config: &Option<String>,
        connectors: &Option<Vec<AttachedConnector>>,
    ) -> AnyResult<Version> {
        log::trace!(
            "Updating config {} {} {} {} {:?} {:?}",
            pipeline_id.0,
            program_id
                .map(|pid| pid.0.to_string())
                .unwrap_or("<not set>".into()),
            pipline_name,
            pipeline_description,
            config,
            connectors
        );
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        if let Some(connectors) = connectors {
            // Delete all existing attached connectors.
            txn.execute(
                "DELETE FROM attached_connector WHERE pipeline_id = $1",
                &[&pipeline_id.0],
            )
            .await?;

            // Rewrite the new set of connectors.
            for ac in connectors {
                self.attach_connector(&txn, pipeline_id, ac).await?;
            }
        }
        let row = txn.query_opt("UPDATE pipeline SET version = version + 1, name = $1, description = $2, config = COALESCE($3, config), program_id = $4 WHERE id = $5 RETURNING version",
            &[&pipline_name, &pipeline_description, &config, &program_id.map(|id| id.0), &pipeline_id.0])
            .await
            .map_err(|e| ProjectDB::maybe_unique_violation(EitherError::Tokio(e)))
            .map_err(|e| ProjectDB::maybe_program_id_foreign_key_constraint_err(e, program_id))?;
        txn.commit().await?;
        match row {
            Some(row) => Ok(Version(row.get(0))),
            None => Err(DBError::UnknownPipeline(pipeline_id).into()),
        }
    }

    async fn delete_config(&self, pipeline_id: PipelineId) -> AnyResult<()> {
        let res = self
            .pool
            .get()
            .await?
            .execute("DELETE FROM pipeline WHERE id = $1", &[&pipeline_id.0])
            .await?;
        if res > 0 {
            Ok(())
        } else {
            Err(anyhow!(DBError::UnknownPipeline(pipeline_id)))
        }
    }

    /// Returns true if the connector of a given name is an input connector.
    async fn attached_connector_is_input(&self, name: &str) -> AnyResult<bool> {
        let row = self
            .pool
            .get()
            .await?
            .query_one(
                "SELECT is_input FROM attached_connector WHERE name = $1",
                &[&name],
            )
            .await?;

        Ok(row.get(0))
    }

    async fn set_pipeline_deployed(&self, pipeline_id: PipelineId, port: u16) -> AnyResult<()> {
        let status: &'static str = PipelineStatus::Deployed.into();

        let res = self
            .pool
            .get()
            .await?
            .execute(
                "UPDATE pipeline SET status = $3, port = $1, created = extract(epoch from now()) where id = $2",
                &[&(port as i16), &pipeline_id.0, &status],
            )
            .await?;

        if res > 0 {
            Ok(())
        } else {
            Err(DBError::UnknownPipeline(pipeline_id).into())
        }
    }

    async fn set_pipeline_status(
        &self,
        pipeline_id: PipelineId,
        status: PipelineStatus,
    ) -> AnyResult<bool> {
        let status: &str = status.into();

        let res = self
            .pool
            .get()
            .await?
            .execute(
                "UPDATE pipeline SET status=$2 WHERE id = $1",
                &[&pipeline_id.0, &status],
            )
            .await?;
        Ok(res > 0)
    }

    async fn delete_pipeline(&self, pipeline_id: PipelineId) -> AnyResult<bool> {
        let res = self
            .pool
            .get()
            .await?
            .execute("DELETE FROM pipeline WHERE id = $1", &[&pipeline_id.0])
            .await?;
        Ok(res > 0)
    }

    async fn new_connector(
        &self,
        id: Uuid,
        name: &str,
        description: &str,
        config: &str,
    ) -> AnyResult<ConnectorId> {
        debug!("new_connector {name} {description} {config}");
        self.pool
            .get()
            .await?
            .execute(
                "INSERT INTO connector (id, name, description, config) VALUES($1, $2, $3, $4)",
                &[&id, &name, &description, &config],
            )
            .await
            .map_err(|e| ProjectDB::maybe_unique_violation(EitherError::Tokio(e)))?;
        Ok(ConnectorId(id))
    }

    async fn list_connectors(&self) -> AnyResult<Vec<ConnectorDescr>> {
        let rows = self
            .pool
            .get()
            .await?
            .query("SELECT id, name, description, config FROM connector", &[])
            .await?;

        let mut result = Vec::with_capacity(rows.len());

        for row in rows {
            result.push(ConnectorDescr {
                connector_id: ConnectorId(row.get(0)),
                name: row.get(1),
                description: row.get(2),
                config: row.get(3),
            });
        }

        Ok(result)
    }

    async fn get_connector_by_name(&self, name: String) -> AnyResult<ConnectorDescr> {
        let row = self
            .pool
            .get()
            .await?
            .query_opt(
                "SELECT id, description, config FROM connector WHERE name = $1",
                &[&name],
            )
            .await?;

        if let Some(row) = row {
            let connector_id: ConnectorId = ConnectorId(row.get(0));
            let description: String = row.get(1);
            let config: String = row.get(2);

            Ok(ConnectorDescr {
                connector_id,
                name,
                description,
                config,
            })
        } else {
            Err(DBError::UnknownName(name).into())
        }
    }

    async fn get_connector_by_id(&self, connector_id: ConnectorId) -> AnyResult<ConnectorDescr> {
        let row = self
            .pool
            .get()
            .await?
            .query_opt(
                "SELECT name, description, config FROM connector WHERE id = $1",
                &[&connector_id.0],
            )
            .await?;

        if let Some(row) = row {
            let name: String = row.get(0);
            let description: String = row.get(1);
            let config: String = row.get(2);

            Ok(ConnectorDescr {
                connector_id,
                name,
                description,
                config,
            })
        } else {
            Err(DBError::UnknownConnector(connector_id).into())
        }
    }

    async fn update_connector(
        &self,
        connector_id: ConnectorId,
        connector_name: &str,
        description: &str,
        config: &Option<String>,
    ) -> AnyResult<()> {
        let descr = self.get_connector_by_id(connector_id).await?;
        let config = config.clone().unwrap_or(descr.config);

        self.pool
            .get()
            .await?
            .execute(
                "UPDATE connector SET name = $1, description = $2, config = $3 WHERE id = $4",
                &[
                    &connector_name,
                    &description,
                    &config.as_str(),
                    &connector_id.0,
                ],
            )
            .await
            .map_err(EitherError::Tokio)
            .map_err(Self::maybe_unique_violation)?;

        Ok(())
    }

    async fn delete_connector(&self, connector_id: ConnectorId) -> AnyResult<()> {
        let res = self
            .pool
            .get()
            .await?
            .execute("DELETE FROM connector WHERE id = $1", &[&connector_id.0])
            .await?;

        if res > 0 {
            Ok(())
        } else {
            Err(anyhow!(DBError::UnknownConnector(connector_id)))
        }
    }

    async fn store_api_key_hash(&self, key: String, scopes: Vec<ApiPermission>) -> AnyResult<()> {
        let mut hasher = sha::Sha256::new();
        hasher.update(key.as_bytes());
        let hash = openssl::base64::encode_block(&hasher.finish());
        let res = self
            .pool
            .get()
            .await?
            .execute(
                "INSERT INTO api_key VALUES ($1, $2)",
                &[
                    &hash,
                    &scopes
                        .iter()
                        .map(|scope| match scope {
                            ApiPermission::Read => "read",
                            ApiPermission::Write => "write",
                        })
                        .collect::<Vec<&str>>(),
                ],
            )
            .await
            .map_err(|_| anyhow!(DBError::DuplicateKey))?;
        if res > 0 {
            Ok(())
        } else {
            Err(anyhow!(DBError::DuplicateKey))
        }
    }

    async fn validate_api_key(&self, api_key: String) -> AnyResult<Vec<ApiPermission>> {
        let mut hasher = sha::Sha256::new();
        hasher.update(api_key.as_bytes());
        let hash = openssl::base64::encode_block(&hasher.finish());
        let res = self
            .pool
            .get()
            .await?
            .query_one("SELECT scopes FROM api_key WHERE hash = $1", &[&hash])
            .await
            .map_err(|_| anyhow!(DBError::InvalidKey))?;
        let vec: Vec<String> = res.get(0);
        let vec = vec
            .iter()
            .map(|s| {
                if s == "read" {
                    ApiPermission::Read
                } else {
                    ApiPermission::Write
                }
            })
            .collect();
        Ok(vec)
    }
}

impl ProjectDB {
    pub(crate) async fn connect(config: &ManagerConfig) -> AnyResult<Self> {
        let connection_str = config.database_connection_string();
        let initial_sql = &config.initial_sql;

        #[cfg(feature = "pg-embed")]
        if connection_str.starts_with("postgres-embed") {
            let database_dir = config.postgres_embed_data_dir();
            let pg_inst = pg_setup::install(database_dir, true, Some(8082)).await?;
            let connection_string = pg_inst.db_uri.to_string();
            return Self::connect_inner(connection_string.as_str(), initial_sql, Some(pg_inst))
                .await;
        };

        Self::connect_inner(
            connection_str.as_str(),
            initial_sql,
            #[cfg(feature = "pg-embed")]
            None,
        )
        .await
    }

    /// Connect to the project database.
    ///
    /// # Arguments
    /// - `config` a tokio postgres config
    /// - `initial_sql`: The initial SQL to execute on the database.
    ///
    /// # Notes
    /// Maybe this should become the preferred way to create a ProjectDb
    /// together with `pg-client-config` (and drop `connect_inner`).
    #[cfg(all(test, not(feature = "pg-embed")))]
    async fn with_config(
        config: tokio_postgres::Config,
        initial_sql: &Option<String>,
    ) -> AnyResult<Self> {
        ProjectDB::initialize(
            config,
            initial_sql,
            #[cfg(feature = "pg-embed")]
            None,
        )
        .await
    }

    /// Connect to the project database.
    ///
    /// # Arguments
    /// - `connection_str`: The connection string to the database.
    /// - `initial_sql`: The initial SQL to execute on the database.
    async fn connect_inner(
        connection_str: &str,
        initial_sql: &Option<String>,
        #[cfg(feature = "pg-embed")] pg_inst: Option<pg_embed::postgres::PgEmbed>,
    ) -> AnyResult<Self> {
        if !connection_str.starts_with("postgres") {
            panic!("Unsupported connection string {}", connection_str)
        }
        let config = connection_str.parse::<tokio_postgres::Config>()?;
        debug!("Opening connection to {:?}", connection_str);

        ProjectDB::initialize(
            config,
            initial_sql,
            #[cfg(feature = "pg-embed")]
            pg_inst,
        )
        .await
    }

    async fn initialize(
        config: tokio_postgres::Config,
        initial_sql: &Option<String>,
        #[cfg(feature = "pg-embed")] pg_inst: Option<pg_embed::postgres::PgEmbed>,
    ) -> AnyResult<Self> {
        let mgr_config = deadpool_postgres::ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        };
        let mgr = Manager::from_config(config, NoTls, mgr_config);
        let pool = Pool::builder(mgr).max_size(16).build().unwrap();
        let client = pool.get().await?;

        client
            .execute(
                "
        CREATE TABLE IF NOT EXISTS program (
            id uuid PRIMARY KEY,
            version bigint NOT NULL,
            name varchar UNIQUE NOT NULL,
            description varchar NOT NULL,
            code varchar NOT NULL,
            schema varchar,
            status varchar,
            error varchar,
            status_since timestamp NOT NULL)",
                &[],
            )
            .await?;

        client
            .execute(
                "
        CREATE TABLE IF NOT EXISTS pipeline (
            id uuid PRIMARY KEY,
            program_id uuid,
            version bigint NOT NULL,
            name varchar UNIQUE NOT NULL,
            description varchar NOT NULL,
            config varchar NOT NULL,
            -- TODO: add 'host' field when we support remote pipelines.
            port smallint,
            status varchar NOT NULL,
            created bigint,
            FOREIGN KEY (program_id) REFERENCES program(id) ON DELETE CASCADE);",
                &[],
            )
            .await?;

        client
            .execute(
                "
        CREATE TABLE IF NOT EXISTS connector (
            id uuid PRIMARY KEY,
            name varchar UNIQUE NOT NULL,
            description varchar NOT NULL,
            config varchar NOT NULL)",
                &[],
            )
            .await?;

        client
            .execute(
                "
        CREATE TABLE IF NOT EXISTS attached_connector (
            pipeline_id uuid NOT NULL,
            connector_id uuid NOT NULL,
            name varchar,
            config varchar,
            is_input bool NOT NULL,
            PRIMARY KEY (pipeline_id, name),
            FOREIGN KEY (pipeline_id) REFERENCES pipeline(id) ON DELETE CASCADE,
            FOREIGN KEY (connector_id) REFERENCES connector(id) ON DELETE CASCADE)",
                &[],
            )
            .await?;

        // Add user and tenant info after API hardening pass
        client
            .execute(
                "
            CREATE TABLE IF NOT EXISTS api_key (
                hash varchar PRIMARY KEY,
                scopes text[] NOT NULL)",
                &[],
            )
            .await?;
        if let Some(initial_sql_file) = &initial_sql {
            if let Ok(initial_sql) = std::fs::read_to_string(initial_sql_file) {
                client.execute(&initial_sql, &[]).await?;
            } else {
                log::warn!("initial SQL file '{}' does not exist", initial_sql_file);
            }
        }

        #[cfg(feature = "pg-embed")]
        return Ok(Self { pool, pg_inst });
        #[cfg(not(feature = "pg-embed"))]
        return Ok(Self { pool });
    }

    /// Attach connector to the pipeline.
    ///
    /// # Precondition
    /// - A valid pipeline for `pipeline_id` must exist.
    async fn attach_connector(
        &self,
        txn: &Transaction<'_>,
        pipeline_id: PipelineId,
        ac: &AttachedConnector,
    ) -> AnyResult<()> {
        txn.execute(
            "INSERT INTO attached_connector (name, pipeline_id, connector_id, is_input, config) VALUES($1, $2, $3, $4, $5)",
            &[&ac.name, &pipeline_id.0, &ac.connector_id.0, &ac.is_input, &ac.config])
            .map_err(EitherError::Tokio)
            .map_err(|e| Self::maybe_connector_id_foreign_key_constraint_err(e, ac.connector_id))
            .map_err(|e| Self::maybe_pipeline_id_foreign_key_constraint_err(e, pipeline_id))
            .map_err(Self::maybe_unique_violation)
            .await?;
        Ok(())
    }

    async fn json_to_attached_connectors(
        &self,
        connectors_json: Value,
    ) -> AnyResult<Vec<AttachedConnector>> {
        let connector_arr = connectors_json.as_array().unwrap();
        let mut attached_connectors = Vec::with_capacity(connector_arr.len());
        for connector in connector_arr {
            let obj = connector.as_object().unwrap();
            let is_input: bool = obj.get("is_input").unwrap().as_bool().unwrap();

            let uuid_str = obj.get("connector_id").unwrap().as_str().unwrap();
            let connector_id = ConnectorId(Uuid::parse_str(uuid_str)?);

            attached_connectors.push(AttachedConnector {
                name: obj.get("name").unwrap().as_str().unwrap().to_owned(),
                connector_id,
                config: obj.get("config").unwrap().as_str().unwrap().to_owned(),
                is_input,
            });
        }
        Ok(attached_connectors)
    }

    /// Helper to convert postgres error into a `DBError` if the underlying
    /// low-level error thrown by the database matches.
    fn maybe_unique_violation(err: EitherError) -> EitherError {
        if let EitherError::Tokio(e) = &err {
            if let Some(dberr) = e.as_db_error() {
                if dberr.code() == &tokio_postgres::error::SqlState::UNIQUE_VIOLATION {
                    match dberr.constraint() {
                        Some("program_pkey") => {
                            return EitherError::Any(anyhow!(DBError::UniqueKeyViolation(
                                "program_pkey"
                            )))
                        }
                        Some("connector_pkey") => {
                            return EitherError::Any(anyhow!(DBError::UniqueKeyViolation(
                                "connector_pkey"
                            )))
                        }
                        Some("pipeline_pkey") => {
                            return EitherError::Any(anyhow!(DBError::UniqueKeyViolation(
                                "pipeline_pkey"
                            )))
                        }
                        Some(_constraint) => {
                            return EitherError::Any(anyhow!(DBError::DuplicateName));
                        }
                        None => {
                            return EitherError::Any(anyhow!(DBError::DuplicateName));
                        }
                    }
                }
            }
        }
        err
    }

    /// Helper to convert program_id foreign key constraint error into an
    /// user-friendly error message.
    fn maybe_program_id_foreign_key_constraint_err(
        err: EitherError,
        program_id: Option<ProgramId>,
    ) -> EitherError {
        if let EitherError::Tokio(e) = &err {
            let db_err = e.as_db_error();
            if let Some(db_err) = db_err {
                if db_err.code() == &tokio_postgres::error::SqlState::FOREIGN_KEY_VIOLATION
                    && db_err.constraint() == Some("pipeline_program_id_fkey")
                {
                    if let Some(program_id) = program_id {
                        return EitherError::Any(anyhow!(DBError::UnknownProgram(program_id)));
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
        err: EitherError,
        pipeline_id: PipelineId,
    ) -> EitherError {
        if let EitherError::Tokio(e) = &err {
            let db_err = e.as_db_error();
            if let Some(db_err) = db_err {
                if db_err.code() == &tokio_postgres::error::SqlState::FOREIGN_KEY_VIOLATION
                    && (db_err.constraint() == Some("pipeline_pipeline_id_fkey")
                        || db_err.constraint() == Some("attached_connector_pipeline_id_fkey"))
                {
                    return EitherError::Any(anyhow!(DBError::UnknownPipeline(pipeline_id)));
                }
            }
        }
        err
    }

    /// Helper to convert connector_id foreign key constraint error into an
    /// user-friendly error message.
    fn maybe_connector_id_foreign_key_constraint_err(
        err: EitherError,
        connector_id: ConnectorId,
    ) -> EitherError {
        if let EitherError::Tokio(e) = &err {
            let db_err = e.as_db_error();
            if let Some(db_err) = db_err {
                if db_err.code() == &tokio_postgres::error::SqlState::FOREIGN_KEY_VIOLATION
                    && db_err.constraint() == Some("attached_connector_connector_id_fkey")
                {
                    return EitherError::Any(anyhow!(DBError::UnknownConnector(connector_id)));
                }
            }
        }
        err
    }
}
