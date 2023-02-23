use crate::{ManagerConfig, ProjectStatus};
use anyhow::{anyhow, Error as AnyError, Result as AnyResult};
use chrono::{DateTime, NaiveDateTime, Utc};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use std::{error::Error as StdError, fmt, fmt::Display};
use utoipa::ToSchema;

/// Project database API.
///
/// The API assumes that the caller holds a database lock, and therefore
/// doesn't use transactions (and hence doesn't need to deal with conflicts).
///
/// The database schema is defined in `create_db.sql`.
///
/// # Compilation queue
///
/// We use the `status` and `status_since` columns to maintain the compilation
/// queue.  A project is enqueued for compilation by setting its status to
/// [`ProjectStatus::Pending`].  The `status_since` column is set to the current
/// time, which determines the position of the project in the queue.
pub(crate) struct ProjectDB {
    dbclient: Connection,
}

/// Unique project id.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[repr(transparent)]
#[serde(transparent)]
pub(crate) struct ProjectId(pub i64);
impl Display for ProjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Unique configuration id.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[repr(transparent)]
#[serde(transparent)]
pub(crate) struct ConfigId(pub i64);
impl Display for ConfigId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Unique pipeline id.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[repr(transparent)]
#[serde(transparent)]
pub(crate) struct PipelineId(pub i64);
impl Display for PipelineId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Version number.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[repr(transparent)]
#[serde(transparent)]
pub(crate) struct Version(i64);
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
            Some("compiling") => Ok(Self::Compiling),
            Some("sql_error") => Ok(Self::SqlError(error_string.unwrap_or_default())),
            Some("rust_error") => Ok(Self::RustError(error_string.unwrap_or_default())),
            Some(status) => Err(AnyError::msg(format!("invalid status string '{status}'"))),
        }
    }
    fn to_columns(&self) -> (Option<String>, Option<String>) {
        match self {
            ProjectStatus::None => (None, None),
            ProjectStatus::Success => (Some("success".to_string()), None),
            ProjectStatus::Pending => (Some("pending".to_string()), None),
            ProjectStatus::Compiling => (Some("compiling".to_string()), None),
            ProjectStatus::SqlError(error) => (Some("sql_error".to_string()), Some(error.clone())),
            ProjectStatus::RustError(error) => {
                (Some("rust_error".to_string()), Some(error.clone()))
            }
        }
    }
}

/// Project descriptor.
#[derive(Serialize, ToSchema, Debug)]
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
}

/// Project configuration descriptor.
#[derive(Serialize, ToSchema, Debug)]
pub(crate) struct ConfigDescr {
    pub config_id: ConfigId,
    pub project_id: ProjectId,
    pub version: Version,
    pub name: String,
    pub config: String,
}

/// Pipeline descriptor.
#[derive(Serialize, ToSchema, Debug)]
pub(crate) struct PipelineDescr {
    pub pipeline_id: PipelineId,
    pub project_id: ProjectId,
    pub project_version: Version,
    pub port: u16,
    pub killed: bool,
    pub created: DateTime<Utc>,
}

impl ProjectDB {
    /// Connect to the project database.
    ///
    /// `config.pg_connection_string` must specify location of a project
    /// database created with `create_db.sql` along with access credentials.
    pub(crate) fn connect(config: &ManagerConfig) -> AnyResult<Self> {
        unsafe {
            rusqlite::trace::config_log(Some(|errcode, msg| {
                log::debug!("sqlite: {msg}:{errcode}")
            }))?
        };
        let dbclient = Connection::open(config.database_file_path())?;

        dbclient.execute(
            r#"
CREATE TABLE IF NOT EXISTS project (
    id integer PRIMARY KEY AUTOINCREMENT,
    version integer,
    name varchar UNIQUE,
    description varchar,
    code varchar,
    status varchar,
    error varchar,
    status_since integer)"#,
            (),
        )?;

        dbclient.execute(
            r#"
CREATE TABLE IF NOT EXISTS project_config (
    id integer PRIMARY KEY AUTOINCREMENT,
    project_id integer,
    version integer,
    name varchar,
    config varchar,
    FOREIGN KEY (project_id) REFERENCES project(id) ON DELETE CASCADE
);
)"#,
            (),
        )?;

        dbclient.execute(
            r#"
CREATE TABLE IF NOT EXISTS pipeline (
    id integer PRIMARY KEY AUTOINCREMENT,
    project_id integer,
    project_version integer,
    -- TODO: add 'host' field when we support remote pipelines.
    port integer,
    killed bool NOT NULL,
    created integer,
    FOREIGN KEY (project_id) REFERENCES project(id) ON DELETE CASCADE)"#,
            (),
        )?;

        if let Some(initial_sql_file) = &config.initial_sql {
            if let Ok(initial_sql) = std::fs::read_to_string(initial_sql_file) {
                dbclient.execute(&initial_sql, ())?;
            } else {
                log::warn!("initial SQL file '{}' does not exist", initial_sql_file);
            }
        }

        Ok(Self { dbclient })
    }

    /// Reset all project statuses to `ProjectStatus::None` after server
    /// restart.
    pub(crate) fn reset_project_status(&self) -> AnyResult<()> {
        self.dbclient
            .execute("UPDATE project SET status = NULL, error = NULL", ())?;

        Ok(())
    }

    /// Retrieve project list from the DB.
    pub(crate) async fn list_projects(&self) -> AnyResult<Vec<ProjectDescr>> {
        let mut statement = self
            .dbclient
            .prepare("SELECT id, name, description, version, status, error FROM project")?;
        let mut rows = statement.query([])?;

        let mut result = Vec::new();

        while let Some(row) = rows.next()? {
            let status: Option<String> = row.get(4)?;
            let error: Option<String> = row.get(5)?;

            let status = ProjectStatus::from_columns(status.as_deref(), error)?;

            result.push(ProjectDescr {
                project_id: ProjectId(row.get(0)?),
                name: row.get(1)?,
                description: row.get(2)?,
                version: Version(row.get(3)?),
                status,
            });
        }

        Ok(result)
    }

    /// Retrieve code of the specified project.
    pub(crate) fn project_code(&self, project_id: ProjectId) -> AnyResult<(Version, String)> {
        let (version, code) = self
            .dbclient
            .query_row(
                "SELECT version, code FROM project WHERE id = $1",
                [&project_id.0],
                |row| Ok((Version(row.get(0)?), row.get(1)?)),
            )
            .map_err(|_| DBError::UnknownProject(project_id))?;

        Ok((version, code))
    }

    /// Create a new project.
    pub(crate) fn new_project(
        &self,
        project_name: &str,
        project_description: &str,
        project_code: &str,
    ) -> AnyResult<(ProjectId, Version)> {
        self.dbclient
            .execute(
                "INSERT INTO project (version, name, description, code, status_since) VALUES(1, $1, $2, $3, unixepoch('now'))",
                (&project_name, &project_description, &project_code),
            )?;

        let id = self
            .dbclient
            .query_row("SELECT last_insert_rowid()", (), |row| {
                Ok(ProjectId(row.get(0)?))
            })?;

        Ok((id, Version(1)))
    }

    /// Update project name and, optionally, code.
    pub(crate) fn update_project(
        &mut self,
        project_id: ProjectId,
        project_name: &str,
        project_description: &str,
        project_code: &Option<String>,
    ) -> AnyResult<Version> {
        let (mut version, old_code): (Version, String) = self
            .dbclient
            .query_row(
                "SELECT version, code FROM project where id = $1",
                [&project_id.0],
                |row| Ok((Version(row.get(0)?), row.get(1)?)),
            )
            .map_err(|_| DBError::UnknownProject(project_id))?;

        match project_code {
            Some(code) if &old_code != code => {
                // Only increment `version` if new code actually differs from the
                // current version.
                version = version.increment();
                self.dbclient
                    .execute(
                        "UPDATE project SET version = $1, name = $2, description = $3, code = $4, status = NULL, error = NULL WHERE id = $4",
                        (&version.0, &project_name, &project_description, code, &project_id.0),
                    ).map_err(|_| DBError::DuplicateProjectName(project_name.to_string()))?;
            }
            _ => {
                self.dbclient
                    .execute(
                        "UPDATE project SET name = $1, description = $2 WHERE id = $3",
                        (&project_name, &project_description, &project_id.0),
                    )
                    .map_err(|_| DBError::DuplicateProjectName(project_name.to_string()))?;
            }
        }

        Ok(version)
    }

    /// Retrieve project descriptor.
    ///
    /// Returns `None` if `project_id` is not found in the database.
    pub(crate) fn get_project_if_exists(
        &self,
        project_id: ProjectId,
    ) -> AnyResult<Option<ProjectDescr>> {
        let mut statement = self.dbclient.prepare(
            "SELECT name, description, version, status, error FROM project WHERE id = $1",
        )?;
        let mut rows = statement.query([&project_id.0])?;

        if let Some(row) = rows.next()? {
            let name: String = row.get(0)?;
            let description: String = row.get(1)?;
            let version: Version = Version(row.get(2)?);
            let status: Option<String> = row.get(3)?;
            let error: Option<String> = row.get(4)?;

            let status = ProjectStatus::from_columns(status.as_deref(), error)?;

            Ok(Some(ProjectDescr {
                project_id,
                name,
                description,
                version,
                status,
            }))
        } else {
            Ok(None)
        }
    }

    /// Retrieve project descriptor.
    ///
    /// Returns a `DBError:UnknownProject` error if `project_id` is not found in
    /// the database.
    pub(crate) fn get_project(&self, project_id: ProjectId) -> AnyResult<ProjectDescr> {
        self.get_project_if_exists(project_id)?
            .ok_or_else(|| anyhow!(DBError::UnknownProject(project_id)))
    }

    /// Validate project version and retrieve project descriptor.
    ///
    /// Returns `DBError::UnknownProject` if `project_id` is not found in the
    /// database. Returns `DBError::OutdatedProjectVersion` if the current
    /// project version differs from `expected_version`.
    pub(crate) fn get_project_guarded(
        &self,
        project_id: ProjectId,
        expected_version: Version,
    ) -> AnyResult<ProjectDescr> {
        let descr = self.get_project(project_id)?;
        if descr.version != expected_version {
            return Err(anyhow!(DBError::OutdatedProjectVersion(expected_version)));
        }

        Ok(descr)
    }

    /// Update project status.
    ///
    /// Note: doesn't check that the project exists.
    fn set_project_status(&self, project_id: ProjectId, status: ProjectStatus) -> AnyResult<()> {
        let (status, error) = status.to_columns();

        self.dbclient
            .execute(
                "UPDATE project SET status = $1, error = $2, status_since = unixepoch('now') WHERE id = $3",
                (&status, &error, &project_id.0),
            )?;

        Ok(())
    }

    /// Update project status after a version check.
    ///
    /// Updates project status to `status` if the current project version
    /// in the database matches `expected_version`.
    pub(crate) fn set_project_status_guarded(
        &mut self,
        project_id: ProjectId,
        expected_version: Version,
        status: ProjectStatus,
    ) -> AnyResult<()> {
        let (status, error) = status.to_columns();

        let _descr = self.get_project_guarded(project_id, expected_version)?;

        self.dbclient
            .execute(
                "UPDATE project SET status = $1, error = $2, status_since = unixepoch('now') WHERE id = $3",
                (&status, &error, &project_id.0),
            )?;

        Ok(())
    }

    /// Queue project for compilation by setting its status to
    /// [`ProjectStatus::Pending`].
    ///
    /// Change project status to [`ProjectStatus::Pending`].
    pub(crate) fn set_project_pending(
        &self,
        project_id: ProjectId,
        expected_version: Version,
    ) -> AnyResult<()> {
        let descr = self.get_project_guarded(project_id, expected_version)?;

        // Do nothing if the project is already pending (we don't want to bump its
        // `status_since` field, which would move it to the end of the queue) or
        // if compilation is alread in progress.
        if descr.status == ProjectStatus::Pending || descr.status == ProjectStatus::Compiling {
            return Ok(());
        }

        self.set_project_status(project_id, ProjectStatus::Pending)?;

        Ok(())
    }

    /// Cancel compilation request.
    ///
    /// Cancels compilation request if the project is pending in the queue
    /// or already being compiled.
    pub(crate) fn cancel_project(
        &self,
        project_id: ProjectId,
        expected_version: Version,
    ) -> AnyResult<()> {
        let descr = self.get_project_guarded(project_id, expected_version)?;

        if descr.status != ProjectStatus::Pending || descr.status != ProjectStatus::Compiling {
            return Ok(());
        }

        self.set_project_status(project_id, ProjectStatus::None)?;

        Ok(())
    }

    /// Delete project from the database.
    ///
    /// This will delete all project configs and pipelines.
    pub(crate) fn delete_project(&self, project_id: ProjectId) -> AnyResult<()> {
        let num_deleted = self
            .dbclient
            .execute("DELETE FROM project WHERE id = $1", [&project_id.0])?;

        if num_deleted > 0 {
            Ok(())
        } else {
            Err(anyhow!(DBError::UnknownProject(project_id)))
        }
    }

    /// Retrieves the first pending project from the queue.
    ///
    /// Returns a pending project with the most recent `status_since` or `None`
    /// if there are no pending projects in the DB.
    pub(crate) fn next_job(&self) -> AnyResult<Option<(ProjectId, Version)>> {
        // Find the oldest pending project.
        let mut statement = self
            .dbclient
            .prepare("SELECT id, version FROM project WHERE status = 'pending' AND status_since = (SELECT min(status_since) FROM project WHERE status = 'pending')")?;
        let mut rows = statement.query([])?;

        if let Some(row) = rows.next()? {
            let project_id: ProjectId = ProjectId(row.get(0)?);
            let version: Version = Version(row.get(1)?);

            Ok(Some((project_id, version)))
        } else {
            Ok(None)
        }
    }

    /// List configs associated with `project_id`.
    pub(crate) fn list_project_configs(
        &self,
        project_id: ProjectId,
    ) -> AnyResult<Vec<ConfigDescr>> {
        // Check that the project exists, so we return an error instead of an
        // empty list of configs.
        let _descr = self.get_project(project_id)?;

        let mut statement = self.dbclient.prepare(
            "SELECT id, version, name, config FROM project_config WHERE project_id = $1",
        )?;
        let mut rows = statement.query([&project_id.0])?;

        let mut result = Vec::new();

        while let Some(row) = rows.next()? {
            result.push(ConfigDescr {
                config_id: ConfigId(row.get(0)?),
                project_id,
                version: Version(row.get(1)?),
                name: row.get(2)?,
                config: row.get(3)?,
            });
        }

        Ok(result)
    }

    /// Retrieve project config.
    pub(crate) fn get_config(&self, config_id: ConfigId) -> AnyResult<ConfigDescr> {
        let descr = self
            .dbclient
            .query_row(
                "SELECT project_id, version, name, config FROM project_config WHERE id = $1",
                [&config_id.0],
                |row| {
                    Ok(ConfigDescr {
                        config_id,
                        project_id: ProjectId(row.get(0)?),
                        version: Version(row.get(1)?),
                        name: row.get(2)?,
                        config: row.get(3)?,
                    })
                },
            )
            .map_err(|_| anyhow!(DBError::UnknownConfig(config_id)))?;

        Ok(descr)
    }

    /// Create a new project config.
    pub(crate) fn new_config(
        &self,
        project_id: ProjectId,
        config_name: &str,
        config: &str,
    ) -> AnyResult<(ConfigId, Version)> {
        // Check that the project exists, so we return correct error status
        // instead of Internal Server Error due to the next query failing.
        let _descr = self.get_project(project_id)?;

        self.dbclient.execute(
            "INSERT INTO project_config (project_id, version, name, config) VALUES($1, 1, $2, $3)",
            (&project_id.0, &config_name, &config),
        )?;

        let id = self
            .dbclient
            .query_row("SELECT last_insert_rowid()", (), |row| {
                Ok(ConfigId(row.get(0)?))
            })?;

        Ok((id, Version(1)))
    }

    /// Update existing project config.
    ///
    /// Update config name and, optionally, YAML.
    pub(crate) fn update_config(
        &mut self,
        config_id: ConfigId,
        config_name: &str,
        config: &Option<String>,
    ) -> AnyResult<Version> {
        let descr = self.get_config(config_id)?;

        let config = config.clone().unwrap_or(descr.config);

        let version = descr.version.increment();
        self.dbclient.execute(
            "UPDATE project_config SET version = $1, name = $2, config = $3 WHERE id = $4",
            (&version.0, &config_name, &config, &config_id.0),
        )?;

        Ok(version)
    }

    /// Delete project config.
    pub(crate) fn delete_config(&self, config_id: ConfigId) -> AnyResult<()> {
        let num_deleted = self
            .dbclient
            .execute("DELETE FROM project_config WHERE id = $1", [&config_id.0])?;

        if num_deleted > 0 {
            Ok(())
        } else {
            Err(anyhow!(DBError::UnknownConfig(config_id)))
        }
    }

    /// Insert a new record to the `pipeline` table.
    pub(crate) fn new_pipeline(
        &self,
        project_id: ProjectId,
        project_version: Version,
    ) -> AnyResult<PipelineId> {
        self.dbclient
            .execute(
                "INSERT INTO pipeline (project_id, project_version, killed, created) VALUES($1, $2, false, unixepoch('now'))",
                (&project_id.0, &project_version.0),
            )?;

        let id = self
            .dbclient
            .query_row("SELECT last_insert_rowid()", (), |row| {
                Ok(PipelineId(row.get(0)?))
            })?;

        Ok(id)
    }

    pub(crate) fn pipeline_set_port(&self, pipeline_id: PipelineId, port: u16) -> AnyResult<()> {
        self.dbclient.execute(
            "UPDATE pipeline SET port = $1 where id = $2",
            (&port, &pipeline_id.0),
        )?;

        Ok(())
    }

    /// Read pipeline status.
    ///
    /// Returns pipeline port number and `killed` flag.
    pub(crate) async fn pipeline_status(&self, pipeline_id: PipelineId) -> AnyResult<(u16, bool)> {
        let (port, killed): (i32, bool) = self
            .dbclient
            .query_row(
                "SELECT port, killed FROM pipeline WHERE id = $1",
                [&pipeline_id.0],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(|_| anyhow!(DBError::UnknownPipeline(pipeline_id)))?;

        Ok((port as u16, killed))
    }

    /// Set `killed` flag to `true`.
    pub(crate) fn set_pipeline_killed(&self, pipeline_id: PipelineId) -> AnyResult<bool> {
        let num_updated = self.dbclient.execute(
            "UPDATE pipeline SET killed=true WHERE id = $1",
            [&pipeline_id.0],
        )?;

        Ok(num_updated > 0)
    }

    /// Delete `pipeline` from the DB.
    pub(crate) fn delete_pipeline(&self, pipeline_id: PipelineId) -> AnyResult<bool> {
        let num_deleted = self
            .dbclient
            .execute("DELETE FROM pipeline WHERE id = $1", [&pipeline_id.0])?;

        Ok(num_deleted > 0)
    }

    /// List pipelines associated with `project_id`.
    pub(crate) fn list_project_pipelines(
        &self,
        project_id: ProjectId,
    ) -> AnyResult<Vec<PipelineDescr>> {
        // Check that the project exists, so we return an error instead of an
        // empty list of pipelines.
        let _descr = self.get_project(project_id)?;

        let mut statement = self.dbclient.prepare(
            "SELECT id, project_version, port, killed, created FROM pipeline WHERE project_id = $1",
        )?;
        let mut rows = statement.query([&project_id.0])?;

        let mut result = Vec::new();

        while let Some(row) = rows.next()? {
            let created_secs: i64 = row.get(4)?;
            let created_naive = NaiveDateTime::from_timestamp_millis(created_secs * 1000)
                .ok_or_else(|| {
                    AnyError::msg(format!(
                        "Invalid timestamp in 'pipeline.created' column: {created_secs}"
                    ))
                })?;

            result.push(PipelineDescr {
                pipeline_id: PipelineId(row.get(0)?),
                project_id,
                project_version: Version(row.get(1)?),
                port: row.get::<_, i32>(2)? as u16,
                killed: row.get(3)?,
                created: DateTime::<Utc>::from_utc(created_naive, Utc),
            });
        }

        Ok(result)
    }
}
