use crate::{config::ManagerConfig, Direction, ProjectStatus};
use anyhow::{anyhow, Error as AnyError, Result as AnyResult};
use async_trait::async_trait;
use chrono::{DateTime, NaiveDateTime, Utc};
use log::{debug, error};
use serde::{Deserialize, Serialize};
use std::{error::Error as StdError, fmt, fmt::Display};
use storage::Storage;
use tokio_postgres::{Client, NoTls};
use utoipa::ToSchema;

#[cfg(test)]
mod test;

#[cfg(any(test, feature = "dev"))]
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
/// queue.  A project is enqueued for compilation by setting its status to
/// [`ProjectStatus::Pending`].  The `status_since` column is set to the current
/// time, which determines the position of the project in the queue.
pub(crate) struct ProjectDB {
    conn: Client,
}

// Used in dev mode for having an embedded Postgres DB live through the lifetime
// of the program.
#[cfg(feature = "dev")]
static PG: once_cell::sync::OnceCell<pg_embed::postgres::PgEmbed> =
    once_cell::sync::OnceCell::new();

/// Unique project id.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Ord, PartialOrd, Serialize, Deserialize, ToSchema)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[repr(transparent)]
#[serde(transparent)]
pub(crate) struct ProjectId(#[cfg_attr(test, proptest(strategy = "1..25i64"))] pub i64);
impl Display for ProjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Unique configuration id.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[repr(transparent)]
#[serde(transparent)]
pub(crate) struct ConfigId(#[cfg_attr(test, proptest(strategy = "1..25i64"))] pub i64);
impl Display for ConfigId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Unique pipeline id.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Ord, PartialOrd, Serialize, Deserialize, ToSchema)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[repr(transparent)]
#[serde(transparent)]
pub(crate) struct PipelineId(#[cfg_attr(test, proptest(strategy = "1..25i64"))] pub i64);
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
pub(crate) struct ConnectorId(#[cfg_attr(test, proptest(strategy = "1..25i64"))] pub i64);
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
pub(crate) struct AttachedConnectorId(#[cfg_attr(test, proptest(strategy = "1..25i64"))] pub i64);
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

impl Version {
    fn increment(&self) -> Self {
        Self(self.0 + 1)
    }
}

#[derive(Debug)]
pub(crate) enum DBError {
    UnknownProject(ProjectId),
    DuplicateProjectName(String),
    OutdatedProjectVersion(Version),
    UnknownConfig(ConfigId),
    UnknownPipeline(PipelineId),
    UnknownConnector(ConnectorId),
}

impl Display for DBError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DBError::UnknownProject(project_id) => write!(f, "Unknown project id '{project_id}'"),
            DBError::DuplicateProjectName(name) => {
                write!(f, "A project named '{name}' already exists")
            }
            DBError::OutdatedProjectVersion(version) => {
                write!(f, "Outdated project version '{version}'")
            }
            DBError::UnknownConfig(config_id) => {
                write!(f, "Unknown project config id '{config_id}'")
            }
            DBError::UnknownPipeline(pipeline_id) => {
                write!(f, "Unknown pipeline id '{pipeline_id}'")
            }
            DBError::UnknownConnector(connector_id) => {
                write!(f, "Unknown connector id '{connector_id}'")
            }
        }
    }
}

impl StdError for DBError {}

/// The database encodes project status using two columns: `status`, which has
/// type `string`, but acts as an enum, and `error`, only used if `status` is
/// one of `"sql_error"` or `"rust_error"`.
impl ProjectStatus {
    /// Decode `ProjectStatus` from the values of `error` and `status` columns.
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
            ProjectStatus::None => (None, None),
            ProjectStatus::Success => (Some("success".to_string()), None),
            ProjectStatus::Pending => (Some("pending".to_string()), None),
            ProjectStatus::CompilingSql => (Some("compiling_sql".to_string()), None),
            ProjectStatus::CompilingRust => (Some("compiling_rust".to_string()), None),
            ProjectStatus::SqlError(error) => {
                if let Ok(error_string) = serde_json::to_string(&error) {
                    (Some("sql_error".to_string()), Some(error_string))
                } else {
                    error!("Expected valid json for SqlError, but got {:?}", error);
                    (Some("sql_error".to_string()), None)
                }
            }
            ProjectStatus::RustError(error) => {
                (Some("rust_error".to_string()), Some(error.clone()))
            }
            ProjectStatus::SystemError(error) => {
                (Some("system_error".to_string()), Some(error.clone()))
            }
        }
    }
}

/// Project descriptor.
#[derive(Serialize, ToSchema, Debug, Eq, PartialEq, Clone)]
pub(crate) struct ProjectDescr {
    /// Unique project id.
    pub project_id: ProjectId,
    /// Project name (doesn't have to be unique).
    pub name: String,
    /// Project description.
    pub description: String,
    /// Project version, incremented every time project code is modified.
    pub version: Version,
    /// Project compilation status.
    pub status: ProjectStatus,
    /// A JSON description of the SQL tables and view declarations including
    /// field names and types.
    ///
    /// The schema is set/updated whenever the `status` field reaches >=
    /// `ProjectStatus::CompilingRust`.
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

/// Project configuration descriptor.
#[derive(Serialize, ToSchema, Eq, PartialEq, Debug, Clone)]
pub(crate) struct ConfigDescr {
    pub config_id: ConfigId,
    pub project_id: Option<ProjectId>,
    pub pipeline: Option<PipelineDescr>,
    pub version: Version,
    pub name: String,
    pub description: String,
    pub config: String,
    pub attached_connectors: Vec<AttachedConnector>,
}

/// Format to add attached connectors during a config update.
#[derive(Deserialize, Serialize, ToSchema, Eq, PartialEq, Debug, Clone)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub(crate) struct AttachedConnector {
    /// A unique identifier for this attachement.
    pub uuid: String,
    /// Is this an input or an output?
    pub direction: Direction,
    /// The id of the connector to attach.
    pub connector_id: ConnectorId,
    /// The YAML config for this attached connector.
    pub config: String,
}

/// Pipeline descriptor.
#[derive(Serialize, ToSchema, Eq, PartialEq, Debug, Clone)]
pub(crate) struct PipelineDescr {
    pub pipeline_id: PipelineId,
    pub config_id: Option<ConfigId>,
    pub port: u16,
    pub killed: bool,
    pub created: DateTime<Utc>,
}

/// Type of new data connector.
#[derive(Serialize, Deserialize, ToSchema, Debug, Eq, PartialEq, Copy, Clone)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub enum ConnectorType {
    KafkaIn = 0,
    KafkaOut = 1,
    File = 2,
    HttpIn = 3,
    HttpOut = 4,
}

impl From<i64> for ConnectorType {
    fn from(val: i64) -> Self {
        match val {
            0 => ConnectorType::KafkaIn,
            1 => ConnectorType::KafkaOut,
            2 => ConnectorType::File,
            3 => ConnectorType::HttpIn,
            4 => ConnectorType::HttpOut,
            _ => panic!("invalid connector type"),
        }
    }
}

#[allow(clippy::from_over_into)]
impl Into<Direction> for ConnectorType {
    fn into(self) -> Direction {
        match self {
            ConnectorType::KafkaIn | ConnectorType::HttpIn => Direction::Input,
            ConnectorType::KafkaOut | ConnectorType::HttpOut => Direction::Output,
            ConnectorType::File => Direction::InputOutput,
        }
    }
}

/// Connector descriptor.
#[derive(Serialize, ToSchema, Debug, Clone, Eq, PartialEq)]
pub(crate) struct ConnectorDescr {
    pub connector_id: ConnectorId,
    pub name: String,
    pub description: String,
    pub typ: ConnectorType,
    pub config: String,
    pub direction: Direction,
}

#[async_trait]
impl Storage for ProjectDB {
    async fn reset_project_status(&self) -> AnyResult<()> {
        self.conn
            .execute(
                "UPDATE project SET status = NULL, error = NULL, schema = NULL",
                &[],
            )
            .await?;
        Ok(())
    }

    async fn list_projects(&self) -> AnyResult<Vec<ProjectDescr>> {
        let rows = self
            .conn
            .query(
                r#"SELECT id, name, description, version, status, error, schema FROM project"#,
                &[],
            )
            .await?;

        let mut result = Vec::with_capacity(rows.len());
        for row in rows {
            let status: Option<String> = row.get(4);
            let error: Option<String> = row.get(5);
            let status = ProjectStatus::from_columns(status.as_deref(), error)?;
            let schema: Option<String> = row.get(6);

            result.push(ProjectDescr {
                project_id: ProjectId(row.get(0)),
                name: row.get(1),
                description: row.get(2),
                version: Version(row.get(3)),
                schema,
                status,
            });
        }

        Ok(result)
    }

    async fn project_code(&self, project_id: ProjectId) -> AnyResult<(ProjectDescr, String)> {
        let row = self.conn.query_opt(
            "SELECT name, description, version, status, error, code, schema FROM project WHERE id = $1", &[&project_id.0]
        )
        .await?
        .ok_or(DBError::UnknownProject(project_id))?;

        let name: String = row.get(0);
        let description: String = row.get(1);
        let version: Version = Version(row.get(2));
        let status: Option<String> = row.get(3);
        let error: Option<String> = row.get(4);
        let code: String = row.get(5);
        let schema: Option<String> = row.get(6);

        let status = ProjectStatus::from_columns(status.as_deref(), error)?;

        Ok((
            ProjectDescr {
                project_id,
                name,
                description,
                version,
                status,
                schema,
            },
            code,
        ))
    }

    async fn new_project(
        &self,
        project_name: &str,
        project_description: &str,
        project_code: &str,
    ) -> AnyResult<(ProjectId, Version)> {
        debug!("new_project {project_name} {project_description} {project_code}");
        self.conn.execute(
                    "INSERT INTO project (version, name, description, code, schema, status, error, status_since)
                        VALUES(1, $1, $2, $3, NULL, NULL, NULL, extract(epoch from now()));",
                &[&project_name, &project_description, &project_code]
            )
            .await
            .map_err(|e| ProjectDB::maybe_duplicate_project_name_err(e, project_name))?;

        // name has a UNIQUE constraint
        let id = self
            .conn
            .query_one("SELECT id FROM project WHERE name = $1", &[&project_name])
            .await?
            .get(0);

        Ok((ProjectId(id), Version(1)))
    }

    /// Update project name, description and, optionally, code.
    /// XXX: Description should be optional too
    async fn update_project(
        &self,
        project_id: ProjectId,
        project_name: &str,
        project_description: &str,
        project_code: &Option<String>,
    ) -> AnyResult<Version> {
        let (mut version, old_code): (Version, String) = self
            .conn
            .query_one(
                "SELECT version, code FROM project where id = $1",
                &[&project_id.0],
            )
            .await
            .map(|row| (Version(row.get(0)), row.get(1)))
            .map_err(|_| DBError::UnknownProject(project_id))?;

        match project_code {
            Some(code) if &old_code != code => {
                // Only increment `version` if new code actually differs from the
                // current version.
                version = version.increment();
                self.conn.execute(
                            "UPDATE project SET version = $1, name = $2, description = $3, code = $4, status = NULL, error = NULL, schema = NULL WHERE id = $5",
                            &[&version.0, &project_name, &project_description, &code, &project_id.0])
                            .await
                            .map_err(|e| ProjectDB::maybe_duplicate_project_name_err(e, project_name))?;
            }
            _ => {
                self.conn
                    .execute(
                        "UPDATE project SET name = $1, description = $2 WHERE id = $3",
                        &[&project_name, &project_description, &project_id.0],
                    )
                    .await
                    .map_err(|e| ProjectDB::maybe_duplicate_project_name_err(e, project_name))?;
            }
        }

        Ok(version)
    }

    /// Retrieve project descriptor.
    ///
    /// Returns `None` if `project_id` is not found in the database.
    async fn get_project_if_exists(
        &self,
        project_id: ProjectId,
    ) -> AnyResult<Option<ProjectDescr>> {
        let row = self.conn.query_opt(
                "SELECT name, description, version, status, error, schema FROM project WHERE id = $1",
                &[&project_id.0],
            )
            .await?;

        if let Some(row) = row {
            let name: String = row.get(0);
            let description: String = row.get(1);
            let version: Version = Version(row.get(2));
            let status: Option<String> = row.get(3);
            let error: Option<String> = row.get(4);
            let schema: Option<String> = row.get(5);
            let status = ProjectStatus::from_columns(status.as_deref(), error)?;

            Ok(Some(ProjectDescr {
                project_id,
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

    /// Lookup project by name.
    async fn lookup_project(&self, project_name: &str) -> AnyResult<Option<ProjectDescr>> {
        let row = self.conn.query_opt(
                "SELECT id, description, version, status, error, schema FROM project WHERE name = $1",
                &[&project_name],
            )
            .await?;

        if let Some(row) = row {
            let project_id: ProjectId = ProjectId(row.get(0));
            let description: String = row.get(1);
            let version: Version = Version(row.get(2));
            let status: Option<String> = row.get(3);
            let error: Option<String> = row.get(4);
            let schema: Option<String> = row.get(5);
            let status = ProjectStatus::from_columns(status.as_deref(), error)?;

            Ok(Some(ProjectDescr {
                project_id,
                name: project_name.to_string(),
                description,
                version,
                status,
                schema,
            }))
        } else {
            Ok(None)
        }
    }

    async fn set_project_status(
        &self,
        project_id: ProjectId,
        status: ProjectStatus,
    ) -> AnyResult<()> {
        let (status, error) = status.to_columns();
        self.conn.execute(
                "UPDATE project SET status = $1, error = $2, schema = '', status_since = extract(epoch from now()) WHERE id = $3",
            &[&status, &error, &project_id.0])
            .await?;

        Ok(())
    }

    async fn set_project_status_guarded(
        &self,
        project_id: ProjectId,
        expected_version: Version,
        status: ProjectStatus,
    ) -> AnyResult<()> {
        let (status, error) = status.to_columns();

        let descr = self.get_project(project_id).await?;
        if descr.version == expected_version {
            self.conn.execute(
                    "UPDATE project SET status = $1, error = $2, status_since = extract(epoch from now()) WHERE id = $3",
            &[&status, &error, &project_id.0])
            .await?;
        }

        Ok(())
    }

    async fn set_project_schema(&self, project_id: ProjectId, schema: String) -> AnyResult<()> {
        self.conn
            .execute(
                "UPDATE project SET schema = $1 WHERE id = $2",
                &[&schema, &project_id.0],
            )
            .await?;

        Ok(())
    }

    async fn delete_project(&self, project_id: ProjectId) -> AnyResult<()> {
        let res = self
            .conn
            .execute("DELETE FROM project WHERE id = $1", &[&project_id.0])
            .await?;

        if res > 0 {
            Ok(())
        } else {
            Err(anyhow!(DBError::UnknownProject(project_id)))
        }
    }

    async fn next_job(&self) -> AnyResult<Option<(ProjectId, Version)>> {
        // Find the oldest pending project.
        let res = self.conn.query_one("SELECT id, version FROM project WHERE status = 'pending' AND status_since = (SELECT min(status_since) FROM project WHERE status = 'pending')", &[])
            .await;

        if let Ok(row) = res {
            let project_id: ProjectId = ProjectId(row.get(0));
            let version: Version = Version(row.get(1));
            Ok(Some((project_id, version)))
        } else {
            Ok(None)
        }
    }

    async fn list_configs(&self) -> AnyResult<Vec<ConfigDescr>> {
        let rows = self.conn.query(
            "SELECT id, version, name, description, config, pipeline_id, project_id FROM project_config", &[])
            .await?;

        let mut result = Vec::with_capacity(rows.len());
        for row in rows {
            let config_id = ConfigId(row.get(0));
            let project_id = row.get::<_, Option<i64>>(6).map(ProjectId);
            let attached_connectors = self.get_attached_connectors(config_id).await?;
            let pipeline = if let Some(pipeline_id) = row.get::<_, Option<i64>>(5).map(PipelineId) {
                Some(self.get_pipeline(pipeline_id).await?)
            } else {
                None
            };

            result.push(ConfigDescr {
                config_id,
                version: Version(row.get(1)),
                name: row.get(2),
                description: row.get(3),
                config: row.get(4),
                pipeline,
                project_id,
                attached_connectors,
            });
        }

        Ok(result)
    }

    async fn get_config(&self, config_id: ConfigId) -> AnyResult<ConfigDescr> {
        let row = self.conn.query_opt(
            "SELECT id, version, name, description, config, pipeline_id, project_id FROM project_config WHERE id = $1",
        &[&config_id.0])
        .await?;

        if let Some(row) = row {
            let pipeline_id: Option<PipelineId> = row.get::<_, Option<i64>>(5).map(PipelineId);
            let project_id = row.get::<_, Option<i64>>(6).map(ProjectId);
            let mut descr = ConfigDescr {
                config_id,
                project_id,
                version: Version(row.get(1)),
                name: row.get(2),
                description: row.get(3),
                config: row.get(4),
                pipeline: None,
                attached_connectors: Vec::new(),
            };

            descr.attached_connectors = self.get_attached_connectors(config_id).await?;
            if let Some(pipeline_id) = pipeline_id {
                descr.pipeline = Some(self.get_pipeline(pipeline_id).await?);
            }

            Ok(descr)
        } else {
            Err(DBError::UnknownConfig(config_id).into())
        }
    }

    async fn new_config(
        &self,
        project_id: Option<ProjectId>,
        config_name: &str,
        config_description: &str,
        config: &str,
        connectors: &Option<Vec<AttachedConnector>>,
    ) -> AnyResult<(ConfigId, Version)> {
        let row = self.conn.query_one(
            "INSERT INTO project_config (project_id, version, name, description, config) VALUES($1, 1, $2, $3, $4) RETURNING id",
            &[&project_id.map(|id| id.0),
            &config_name,
            &config_description,
            &config])
            .await
            .map_err(|e| ProjectDB::maybe_project_id_foreign_key_constraint_err(e, project_id))?;
        let config_id = ConfigId(row.get(0));

        if let Some(connectors) = connectors {
            // Add the connectors.
            // TODO: This should be done in a transaction with the query above.
            for ac in connectors {
                self.attach_connector(config_id, ac).await?;
            }
        }

        Ok((config_id, Version(1)))
    }

    async fn add_pipeline_to_config(
        &self,
        config_id: ConfigId,
        pipeline_id: PipelineId,
    ) -> AnyResult<()> {
        let rows = self
            .conn
            .execute(
                "UPDATE project_config SET pipeline_id = $1 WHERE id = $2",
                &[&pipeline_id.0, &config_id.0],
            )
            .await
            .map_err(|e| ProjectDB::maybe_pipeline_id_foreign_key_constraint_err(e, pipeline_id))?;
        if rows > 0 {
            Ok(())
        } else {
            return Err(DBError::UnknownConfig(config_id).into());
        }
    }

    async fn remove_pipeline_from_config(&self, config_id: ConfigId) -> AnyResult<()> {
        let rows = self
            .conn
            .execute(
                "UPDATE project_config SET pipeline_id = NULL WHERE id = $1",
                &[&config_id.0],
            )
            .await?;
        if rows > 0 {
            Ok(())
        } else {
            return Err(DBError::UnknownConfig(config_id).into());
        }
    }

    async fn update_config(
        &self,
        config_id: ConfigId,
        project_id: Option<ProjectId>,
        config_name: &str,
        config_description: &str,
        config: &Option<String>,
        connectors: &Option<Vec<AttachedConnector>>,
    ) -> AnyResult<Version> {
        let _descr = self.get_config(config_id).await?;

        log::trace!(
            "Updating config {} {} {} {} {:?} {:?}",
            config_id.0,
            project_id.map(|pid| pid.0).unwrap_or(-1),
            config_name,
            config_description,
            config,
            connectors
        );
        let descr = self.get_config(config_id).await?;
        let config = config.clone().unwrap_or(descr.config);

        if let Some(connectors) = connectors {
            // Delete all existing attached connectors.
            self.conn
                .execute(
                    "DELETE FROM attached_connector WHERE config_id = $1",
                    &[&config_id.0],
                )
                .await?;

            // Rewrite the new set of connectors.
            for ac in connectors {
                // TODO: This should be done in a transaction with the query above.
                self.attach_connector(config_id, ac).await?;
            }
        }

        let version = descr.version.increment();
        self.conn.execute("UPDATE project_config SET version = $1, name = $2, description = $3, config = $4, project_id = $5 WHERE id = $6",
            &[&version.0, &config_name, &config_description, &config, &project_id.map(|id| id.0), &config_id.0])
            .await
            .map_err(|e| ProjectDB::maybe_project_id_foreign_key_constraint_err(e, project_id))?;

        Ok(version)
    }

    async fn delete_config(&self, config_id: ConfigId) -> AnyResult<()> {
        let res = self
            .conn
            .execute("DELETE FROM project_config WHERE id = $1", &[&config_id.0])
            .await?;
        if res > 0 {
            Ok(())
        } else {
            Err(anyhow!(DBError::UnknownConfig(config_id)))
        }
    }

    async fn get_attached_connector_direction(&self, uuid: &str) -> AnyResult<Direction> {
        let row = self
            .conn
            .query_one(
                "SELECT is_input FROM attached_connector WHERE uuid = $1",
                &[&uuid],
            )
            .await?;

        if row.get(0) {
            Ok(Direction::Input)
        } else {
            Ok(Direction::Output)
        }
    }

    async fn new_pipeline(
        &self,
        config_id: ConfigId,
        config_version: Version,
    ) -> AnyResult<PipelineId> {
        let row = self.conn.query_one(
                "INSERT INTO pipeline (config_id, config_version, killed, created) VALUES($1, $2, false, extract(epoch from now())) RETURNING id",
            &[&config_id.0, &config_version.0])
            .await
            .map_err(|e| ProjectDB::maybe_config_id_foreign_key_constraint_err(e, config_id))?;

        Ok(PipelineId(row.get(0)))
    }

    async fn pipeline_set_port(&self, pipeline_id: PipelineId, port: u16) -> AnyResult<()> {
        let _ = self
            .conn
            .execute(
                "UPDATE pipeline SET port = $1 where id = $2",
                &[&(port as i16), &pipeline_id.0],
            )
            .await?;
        Ok(())
    }

    async fn set_pipeline_killed(&self, pipeline_id: PipelineId) -> AnyResult<bool> {
        let res = self
            .conn
            .execute(
                "UPDATE pipeline SET killed=true WHERE id = $1",
                &[&pipeline_id.0],
            )
            .await?;
        Ok(res > 0)
    }

    async fn delete_pipeline(&self, pipeline_id: PipelineId) -> AnyResult<bool> {
        let res = self
            .conn
            .execute("DELETE FROM pipeline WHERE id = $1", &[&pipeline_id.0])
            .await?;
        Ok(res > 0)
    }

    async fn get_pipeline(&self, pipeline_id: PipelineId) -> AnyResult<PipelineDescr> {
        let row = self
            .conn
            .query_one(
                "SELECT id, config_id, port, killed, created FROM pipeline WHERE id = $1",
                &[&pipeline_id.0],
            )
            .await
            .map_err(|_| DBError::UnknownPipeline(pipeline_id))?;

        let created_secs: i64 = row.get(4);
        let created_naive =
            NaiveDateTime::from_timestamp_millis(created_secs * 1000).ok_or_else(|| {
                AnyError::msg(format!(
                    "Invalid timestamp in 'pipeline.created' column: {created_secs}"
                ))
            })?;

        Ok(PipelineDescr {
            pipeline_id: PipelineId(row.get(0)),
            config_id: row.get::<_, Option<i64>>(1).map(ConfigId),
            port: row.get::<_, Option<i16>>(2).unwrap_or(0) as u16,
            killed: row.get(3),
            created: DateTime::<Utc>::from_utc(created_naive, Utc),
        })
    }

    async fn list_pipelines(&self) -> AnyResult<Vec<PipelineDescr>> {
        let rows = self
            .conn
            .query(
                "SELECT id, config_id, port, killed, created FROM pipeline",
                &[],
            )
            .await?;

        let mut result = Vec::with_capacity(rows.len());
        for row in rows {
            let created_secs: i64 = row.get(4);
            let created_naive = NaiveDateTime::from_timestamp_millis(created_secs * 1000)
                .ok_or_else(|| {
                    AnyError::msg(format!(
                        "Invalid timestamp in 'pipeline.created' column: {created_secs}"
                    ))
                })?;

            result.push(PipelineDescr {
                pipeline_id: PipelineId(row.get(0)),
                config_id: row.get::<_, Option<i64>>(1).map(ConfigId),
                port: row.get::<_, Option<i16>>(2).unwrap_or(0) as u16,
                killed: row.get(3),
                created: DateTime::<Utc>::from_utc(created_naive, Utc),
            });
        }

        Ok(result)
    }

    async fn new_connector(
        &self,
        name: &str,
        description: &str,
        typ: ConnectorType,
        config: &str,
    ) -> AnyResult<ConnectorId> {
        debug!("new_connector {name} {description} {config}");
        let row = self.conn.query_one("INSERT INTO connector (name, description, typ, config) VALUES($1, $2, $3, $4) RETURNING id",
            &[&name, &description, &(typ as i64), &config])
            .await?;
        Ok(ConnectorId(row.get(0)))
    }

    async fn list_connectors(&self) -> AnyResult<Vec<ConnectorDescr>> {
        let rows = self
            .conn
            .query(
                "SELECT id, name, description, typ, config FROM connector",
                &[],
            )
            .await?;

        let mut result = Vec::with_capacity(rows.len());

        for row in rows {
            let typ = row.get::<_, i64>(3).into();
            result.push(ConnectorDescr {
                connector_id: ConnectorId(row.get(0)),
                name: row.get(1),
                description: row.get(2),
                typ,
                direction: typ.into(),
                config: row.get(4),
            });
        }

        Ok(result)
    }

    async fn get_connector(&self, connector_id: ConnectorId) -> AnyResult<ConnectorDescr> {
        let row = self
            .conn
            .query_opt(
                "SELECT name, description, typ, config FROM connector WHERE id = $1",
                &[&connector_id.0],
            )
            .await?;

        if let Some(row) = row {
            let name: String = row.get(0);
            let description: String = row.get(1);
            let typ: ConnectorType = row.get::<_, i64>(2).into();
            let config: String = row.get(3);

            Ok(ConnectorDescr {
                connector_id,
                name,
                description,
                typ,
                direction: typ.into(),
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
        let descr = self.get_connector(connector_id).await?;
        let config = config.clone().unwrap_or(descr.config);

        self.conn
            .execute(
                "UPDATE connector SET name = $1, description = $2, config = $3 WHERE id = $4",
                &[
                    &connector_name,
                    &description,
                    &config.as_str(),
                    &connector_id.0,
                ],
            )
            .await?;

        Ok(())
    }

    async fn delete_connector(&self, connector_id: ConnectorId) -> AnyResult<()> {
        let res = self
            .conn
            .execute("DELETE FROM connector WHERE id = $1", &[&connector_id.0])
            .await?;

        if res > 0 {
            Ok(())
        } else {
            Err(anyhow!(DBError::UnknownConnector(connector_id)))
        }
    }
}

impl ProjectDB {
    pub(crate) async fn connect(config: &ManagerConfig) -> AnyResult<Self> {
        let connection_str = config.database_connection_string();
        let initial_sql = &config.initial_sql;

        #[cfg(feature = "dev")]
        let connection_str = if connection_str.starts_with("postgres-embed") {
            let database_dir = config.postgres_embed_data_dir();
            let pg = pg_setup::install(database_dir, true, Some(8082)).await?;
            let connection_string = pg.db_uri.to_string();
            let _ = PG.set(pg);
            connection_string
        } else {
            connection_str
        };

        Self::connect_inner(connection_str.as_str(), initial_sql).await
    }

    /// Connect to the project database.
    ///
    /// # Arguments
    /// - `connection_str`: The connection string to the database.
    /// - `pool_options`: The pool options to use.
    /// - `initial_sql`: The initial SQL to execute on the database.
    /// - `database_dir`: The directory to use for the embedded Postgres
    ///   database.
    /// - `is_persistent`: Whether the embedded postgres database should be
    ///   persistent or removed on shutdown.
    /// - `port`: The port to use for the embedded Postgres database to run on.
    async fn connect_inner(connection_str: &str, initial_sql: &Option<String>) -> AnyResult<Self> {
        if !connection_str.starts_with("postgres") {
            panic!("Unsupported connection string {}", connection_str)
        }

        debug!("Opening connection to {:?}", connection_str);
        let (client, conn) = tokio_postgres::connect(connection_str, NoTls).await?;

        // The `tokio_postgres` API requires allocating a thread to `connection`,
        // which will handle datbase I/O and should automatically terminate once
        // the `dbclient` is dropped.
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                eprintln!("connection error: {}", e);
            }
        });

        client
            .execute(
                "
        CREATE TABLE IF NOT EXISTS project (
            id bigserial PRIMARY KEY,
            version bigint NOT NULL,
            name varchar UNIQUE NOT NULL,
            description varchar NOT NULL,
            code varchar NOT NULL,
            schema varchar,
            status varchar,
            error varchar,
            status_since bigint NOT NULL)",
                &[],
            )
            .await?;

        client
            .execute(
                "
        CREATE TABLE IF NOT EXISTS pipeline (
            id bigserial PRIMARY KEY,
            config_id bigint,
            config_version bigint NOT NULL,
            -- TODO: add 'host' field when we support remote pipelines.
            port smallint,
            killed bool NOT NULL,
            created bigint NOT NULL)",
                &[],
            )
            .await?;

        client
            .execute(
                "
        CREATE TABLE IF NOT EXISTS project_config (
            id bigserial PRIMARY KEY,
            pipeline_id bigint,
            project_id bigint,
            version bigint NOT NULL,
            name varchar NOT NULL,
            description varchar NOT NULL,
            config varchar NOT NULL,
            FOREIGN KEY (project_id) REFERENCES project(id) ON DELETE CASCADE,
            FOREIGN KEY (pipeline_id) REFERENCES pipeline(id) ON DELETE SET NULL);",
                &[],
            )
            .await?;

        client
            .execute(
                "
                -- We can't add this in the create statement due to the circular dependency
                ALTER TABLE pipeline
                ADD CONSTRAINT pipeline_config_id_fkey
                FOREIGN KEY (config_id)
                REFERENCES project_config(id)
                ON DELETE SET NULL;",
                &[],
            )
            .await?;

        client
            .execute(
                "
        CREATE TABLE IF NOT EXISTS connector (
            id bigserial PRIMARY KEY,
            name varchar NOT NULL,
            description varchar NOT NULL,
            typ bigint NOT NULL,
            config varchar NOT NULL)",
                &[],
            )
            .await?;

        client
            .execute(
                "
        CREATE TABLE IF NOT EXISTS attached_connector (
            id bigserial PRIMARY KEY,
            uuid varchar UNIQUE NOT NULL,
            config_id bigint NOT NULL,
            connector_id bigint NOT NULL,
            config varchar,
            is_input bool NOT NULL,
            FOREIGN KEY (config_id) REFERENCES project_config(id) ON DELETE CASCADE,
            FOREIGN KEY (connector_id) REFERENCES connector(id) ON DELETE CASCADE)",
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

        Ok(Self { conn: client })
    }

    /// Attach connector to the config.
    ///
    /// # Precondition
    /// - A valid config for `config_id` must exist.
    async fn attach_connector(
        &self,
        config_id: ConfigId,
        ac: &AttachedConnector,
    ) -> AnyResult<AttachedConnectorId> {
        //let _descr = self.get_config(config_id).await?;
        let _descr = self.get_connector(ac.connector_id).await?;
        let is_input = ac.direction == Direction::Input;

        let row = self.conn.query_one(
            "INSERT INTO attached_connector (uuid, config_id, connector_id, is_input, config) VALUES($1, $2, $3, $4, $5) RETURNING id",
            &[&ac.uuid, &config_id.0, &ac.connector_id.0, &is_input, &ac.config])
            .await?;

        Ok(AttachedConnectorId(row.get(0)))
    }

    async fn get_attached_connectors(
        &self,
        config_id: ConfigId,
    ) -> AnyResult<Vec<AttachedConnector>> {
        let rows = self.conn.query(
            "SELECT uuid, connector_id, config, is_input FROM attached_connector WHERE config_id = $1",
            &[&config_id.0])
            .await?;
        let mut result = Vec::with_capacity(rows.len());

        for row in rows {
            let direction = if row.get(3) {
                Direction::Input
            } else {
                Direction::Output
            };

            result.push(AttachedConnector {
                uuid: row.get(0),
                connector_id: ConnectorId(row.get(1)),
                config: row.get(2),
                direction,
            });
        }

        Ok(result)
    }

    /// Helper to convert postgres error into a `DBError::DuplicateProjectName`
    /// if the underlying low-level error thrown by the database matches.
    fn maybe_duplicate_project_name_err(e: tokio_postgres::Error, _project_name: &str) -> AnyError {
        if let Some(code) = e.code() {
            if code == &tokio_postgres::error::SqlState::UNIQUE_VIOLATION {
                return anyhow!(DBError::DuplicateProjectName(_project_name.to_string()));
            }
        }

        anyhow!(e)
    }

    /// Helper to convert project_id foreign key constraint error into an
    /// user-friendly error message.
    fn maybe_project_id_foreign_key_constraint_err(
        e: tokio_postgres::Error,
        project_id: Option<ProjectId>,
    ) -> AnyError {
        let db_err = e.as_db_error();
        if let Some(db_err) = db_err {
            if db_err.code() == &tokio_postgres::error::SqlState::FOREIGN_KEY_VIOLATION
                && db_err.constraint() == Some("project_config_project_id_fkey")
            {
                if let Some(project_id) = project_id {
                    return anyhow!(DBError::UnknownProject(project_id));
                } else {
                    log::error!("A `project_config_project_id_fkey` error, but the project_id was None? This should not happen.");
                    return anyhow!(e);
                }
            }
        }

        anyhow!(e)
    }

    /// Helper to convert config_id foreign key constraint error into an
    /// user-friendly error message.
    fn maybe_config_id_foreign_key_constraint_err(
        e: tokio_postgres::Error,
        config_id: ConfigId,
    ) -> AnyError {
        let db_err = e.as_db_error();
        if let Some(db_err) = db_err {
            if db_err.code() == &tokio_postgres::error::SqlState::FOREIGN_KEY_VIOLATION
                && db_err.constraint() == Some("pipeline_config_id_fkey")
            {
                return anyhow!(DBError::UnknownConfig(config_id));
            }
        }

        anyhow!(e)
    }

    /// Helper to convert config_id foreign key constraint error into an
    /// user-friendly error message.
    fn maybe_pipeline_id_foreign_key_constraint_err(
        e: tokio_postgres::Error,
        pipeline_id: PipelineId,
    ) -> AnyError {
        let db_err = e.as_db_error();
        if let Some(db_err) = db_err {
            if db_err.code() == &tokio_postgres::error::SqlState::FOREIGN_KEY_VIOLATION
                && db_err.constraint() == Some("project_config_pipeline_id_fkey")
            {
                return anyhow!(DBError::UnknownPipeline(pipeline_id));
            }
        }

        anyhow!(e)
    }
}
