use crate::ProjectStatus;
use anyhow::{anyhow, Error as AnyError, Result as AnyResult};
use chrono::{DateTime, NaiveDateTime, Utc};
use log::{debug, error};
use serde::{Deserialize, Serialize};
use sqlx::{
    any::{AnyConnectOptions, AnyRow},
    pool::PoolOptions,
    Any, ConnectOptions, Pool, Row,
};
use std::{error::Error as StdError, fmt, fmt::Display, str::FromStr};
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
    conn: Pool<Any>,
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
#[derive(Serialize, ToSchema, Debug, Eq, PartialEq)]
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

fn to_option(val: String) -> Option<String> {
    if val.is_empty() {
        None
    } else {
        Some(val)
    }
}

impl ProjectDB {
    pub(crate) async fn connect(
        connection_str: &str,
        initial_sql: &Option<String>,
    ) -> AnyResult<Self> {
        Self::connect_inner(connection_str, PoolOptions::new(), initial_sql).await
    }

    /// Connect to the project database.
    async fn connect_inner(
        connection_str: &str,
        pool_options: PoolOptions<Any>,
        initial_sql: &Option<String>,
    ) -> AnyResult<Self> {
        sqlx::any::install_default_drivers();
        debug!("Opening connection to {:?}", connection_str);
        let options =
            AnyConnectOptions::from_str(connection_str)?.log_statements(log::LevelFilter::Trace);
        let conn = pool_options.connect_with(options).await?;
        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS project (
            id integer PRIMARY KEY AUTOINCREMENT,
            version integer NOT NULL,
            name varchar UNIQUE NOT NULL,
            description varchar NOT NULL,
            code varchar NOT NULL,
            schema varchar NOT NULL,
            status varchar NOT NULL,
            error varchar NOT NULL,
            status_since integer NOT NULL)"#,
        )
        .execute(&conn)
        .await?;

        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS project_config (
            id integer PRIMARY KEY AUTOINCREMENT,
            project_id integer NOT NULL,
            version integer NOT NULL,
            name varchar NOT NULL,
            config varchar NOT NULL,
            FOREIGN KEY (project_id) REFERENCES project(id) ON DELETE CASCADE);"#,
        )
        .execute(&conn)
        .await?;

        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS pipeline (
            id integer PRIMARY KEY AUTOINCREMENT,
            project_id integer NOT NULL,
            project_version integer NOT NULL,
            -- TODO: add 'host' field when we support remote pipelines.
            port integer,
            killed integer NOT NULL,
            created integer NOT NULL,
            FOREIGN KEY (project_id) REFERENCES project(id) ON DELETE CASCADE)"#,
        )
        // killed should be a boolean but the Any driver does not support it for sqlite
        .execute(&conn)
        .await?;

        if let Some(initial_sql_file) = &initial_sql {
            if let Ok(initial_sql) = std::fs::read_to_string(initial_sql_file) {
                sqlx::query(&initial_sql).execute(&conn).await?;
            } else {
                log::warn!("initial SQL file '{}' does not exist", initial_sql_file);
            }
        }

        Ok(Self { conn })
    }

    /// Reset everything that is set through compilation of the project.
    ///
    /// - Set status to `ProjectStatus::None` after server restart.
    /// - Reset `schema` to None.
    pub(crate) async fn reset_project_status(&self) -> AnyResult<()> {
        sqlx::query("UPDATE project SET status = '', error = '', schema = ''")
            .execute(&self.conn)
            .await?;
        Ok(())
    }

    /// Retrieve project list from the DB.
    pub(crate) async fn list_projects(&self) -> AnyResult<Vec<ProjectDescr>> {
        let rows = sqlx::query(
            r#"SELECT id, name, description, version, 
                         status, 
                         error, 
                         schema
                         FROM project"#,
        )
        .fetch_all(&self.conn)
        .await?;

        let mut result = Vec::new();
        for row in rows {
            let status: Option<String> = to_option(row.get(4));
            let error: Option<String> = to_option(row.get(5));
            let status = ProjectStatus::from_columns(status.as_deref(), error)?;
            let schema: Option<String> = to_option(row.get(6));

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

    /// Retrieve code of the specified project along with the project's
    /// meta-data.
    pub(crate) async fn project_code(
        &self,
        project_id: ProjectId,
    ) -> AnyResult<(ProjectDescr, String)> {
        let fetched = sqlx::query(
            "SELECT name, description, version, status, error, code, schema FROM project WHERE id = ?",
        ).bind(project_id.0).fetch_one(&self.conn).await;

        if let Ok(row) = fetched {
            let name: String = row.get(0);
            let description: String = row.get(1);
            let version: Version = Version(row.get(2));
            let status: Option<String> = to_option(row.get(3));
            let error: Option<String> = to_option(row.get(4));
            let code: String = row.get(5);
            let schema: Option<String> = to_option(row.get(6));

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
        } else {
            Err(DBError::UnknownProject(project_id).into())
        }
    }

    /// Helper to convert sqlx error into a `DBError::DuplicateProjectName`
    /// if the underlying low-level error thrown by the database matches.
    fn maybe_duplicate_project_name_err(e: sqlx::Error, project_name: &str) -> AnyError {
        match e {
            sqlx::Error::Database(db_error) => {
                if (*db_error).is_unique_violation() {
                    anyhow!(DBError::DuplicateProjectName(project_name.to_string()))
                } else {
                    anyhow!(db_error)
                }
            }
            _ => anyhow!(e),
        }
    }

    /// Create a new project.
    pub(crate) fn new_project(
        &self,
        project_name: &str,
        project_description: &str,
        project_code: &str,
    ) -> AnyResult<(ProjectId, Version)> {
        debug!("new_project {project_name} {project_description} {project_code}");
        // Avoid nulls until this bug is fixed https://github.com/launchbadge/sqlx/issues/2416

        sqlx::query("INSERT INTO project (version, name, description, code, schema, status, error, status_since) VALUES(1, ?, ?, ?, '', '', '', unixepoch('now'))")
            .bind(project_name)
            .bind(project_description)
            .bind(project_code)
            .execute(&self.conn)
            .await
            .map_err(|e| ProjectDB::maybe_duplicate_project_name_err(e, project_name))?;

        // name has a UNIQUE constraint
        let id = sqlx::query("SELECT id FROM project WHERE name = ?")
            .bind(project_name)
            .fetch_one(&self.conn)
            .await?
            .get(0);

        Ok((ProjectId(id), Version(1)))
    }

    /// Update project name, description and, optionally, code.
    /// XXX: Description should be optional too
    pub(crate) async fn update_project(
        &self,
        project_id: ProjectId,
        project_name: &str,
        project_description: &str,
        project_code: &Option<String>,
    ) -> AnyResult<Version> {
        let (mut version, old_code): (Version, String) =
            sqlx::query("SELECT version, code FROM project where id = ?")
                .bind(project_id.0)
                .map(|row: AnyRow| (Version(row.get(0)), row.get(1)))
                .fetch_one(&self.conn)
                .await
                .map_err(|_| DBError::UnknownProject(project_id))?;

        match project_code {
            Some(code) if &old_code != code => {
                // Only increment `version` if new code actually differs from the
                // current version.
                version = version.increment();
                sqlx::query(
                        "UPDATE project SET version = ?, name = ?, description = ?, code = ?, status = '', error = '' WHERE id = ?")
                        .bind(version.0)
                        .bind(project_name)
                        .bind(project_description)
                        .bind(code)
                        .bind(project_id.0)
                        .execute(&self.conn)
                        .await
                        .map_err(|e| ProjectDB::maybe_duplicate_project_name_err(e, project_name))?;
            }
            _ => {
                sqlx::query("UPDATE project SET name = ?, description = ? WHERE id = ?")
                    .bind(project_name)
                    .bind(project_description)
                    .bind(project_id.0)
                    .execute(&self.conn)
                    .await
                    .map_err(|e| ProjectDB::maybe_duplicate_project_name_err(e, project_name))?;
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
        let row = sqlx::query(
            "SELECT name, description, version, status, error, schema FROM project WHERE id = ?",
        )
        .bind(project_id.0)
        .fetch_one(&self.conn)
        .await;

        if let Ok(row) = row {
            let name: String = row.get(0);
            let description: String = row.get(1);
            let version: Version = Version(row.get(2));
            let status: Option<String> = to_option(row.get(3));
            let error: Option<String> = to_option(row.get(4));
            let schema: Option<String> = to_option(row.get(5));

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

    /// Lookup project by name
    pub(crate) async fn lookup_project(
        &self,
        project_name: &str,
    ) -> AnyResult<Option<ProjectDescr>> {
        let row = sqlx::query(
            "SELECT id, description, version, status, error, schema FROM project WHERE name = ?",
        )
        .bind(project_name)
        .fetch_one(&self.conn)
        .await;

        if let Ok(row) = row {
            let project_id: ProjectId = ProjectId(row.get(0));
            let description: String = row.get(1);
            let version: Version = Version(row.get(2));
            let status: Option<String> = to_option(row.get(3));
            let error: Option<String> = to_option(row.get(4));
            let schema: Option<String> = to_option(row.get(5));
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
    /// # Note
    /// - Doesn't check that the project exists.
    /// - Resets schema to null.
    async fn set_project_status(
        &self,
        project_id: ProjectId,
        status: ProjectStatus,
    ) -> AnyResult<()> {
        let (status, error) = status.to_columns();
        sqlx::query(
                "UPDATE project SET status = ?, error = ?, schema = '', status_since = unixepoch('now') WHERE id = ?"
            )
            .bind(status.unwrap_or_default())
            .bind(error.unwrap_or_default())
            .bind(project_id.0)
            .execute(&self.conn)
            .await?;

        Ok(())
    }

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
    pub(crate) async fn set_project_status_guarded(
        &self,
        project_id: ProjectId,
        expected_version: Version,
        status: ProjectStatus,
    ) -> AnyResult<()> {
        let (status, error) = status.to_columns();

        let descr = self.get_project(project_id).await?;
        if descr.version == expected_version {
            sqlx::query(
                "UPDATE project SET status = ?, error = ?, status_since = unixepoch('now') WHERE id = ?")
                .bind(status.unwrap_or_default())
                .bind(error.unwrap_or_default())
                .bind(project_id.0)
                .execute(&self.conn)
            .await?;
        }

        Ok(())
    }

    /// Update project schema.
    ///
    /// # Note
    /// This should be called after the SQL compilation succeeded, e.g., in the
    /// same transaction that sets status to  [`ProjectStatus::CompilingRust`].
    pub(crate) async fn set_project_schema(
        &self,
        project_id: ProjectId,
        schema: String,
    ) -> AnyResult<()> {
        sqlx::query("UPDATE project SET schema = ? WHERE id = ?")
            .bind(schema)
            .bind(project_id.0)
            .execute(&self.conn)
            .await?;

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
        if descr.status == ProjectStatus::Pending || descr.status.is_compiling() {
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

        if descr.status != ProjectStatus::Pending || !descr.status.is_compiling() {
            return Ok(());
        }

        self.set_project_status(project_id, ProjectStatus::None)?;

        Ok(())
    }

    /// Delete project from the database.
    ///
    /// This will delete all project configs and pipelines.
    pub(crate) async fn delete_project(&self, project_id: ProjectId) -> AnyResult<()> {
        let res = sqlx::query("DELETE FROM project WHERE id = ?")
            .bind(project_id.0)
            .execute(&self.conn)
            .await?;

        if res.rows_affected() > 0 {
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
        let res = sqlx::query("SELECT id, version FROM project WHERE status = 'pending' AND status_since = (SELECT min(status_since) FROM project WHERE status = 'pending')")
            .fetch_one(&self.conn).await;

        if let Ok(row) = res {
            let project_id: ProjectId = ProjectId(row.get(0));
            let version: Version = Version(row.get(1));
            Ok(Some((project_id, version)))
        } else {
            Ok(None)
        }

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
        let _descr = self.get_project(project_id).await?;
        let rows = sqlx::query(
            "SELECT id, version, name, config FROM project_config WHERE project_id = ?",
        )
        .bind(project_id.0)
        .fetch_all(&self.conn)
        .await?;

        let mut result = Vec::new();

        for row in rows {
            result.push(ConfigDescr {
                config_id: ConfigId(row.get(0)),
                project_id,
                version: Version(row.get(1)),
                name: row.get(2),
                config: row.get(3),
            });
        }

        Ok(result)
    }

    /// Retrieve project config.
    pub(crate) async fn get_config(&self, config_id: ConfigId) -> AnyResult<ConfigDescr> {
        let descr: Result<ConfigDescr, DBError> = sqlx::query(
            "SELECT project_id, version, name, config FROM project_config WHERE id = ?",
        )
        .bind(config_id.0)
        .fetch_one(&self.conn)
        .await
        .map(|row| {
            Ok(ConfigDescr {
                config_id,
                project_id: ProjectId(row.get(0)),
                version: Version(row.get(1)),
                name: row.get(2),
                config: row.get(3),
            })
        })
        .map_err(|_| anyhow!(DBError::UnknownConfig(config_id)))?;
        Ok(descr?)
    }

    /// Create a new project config.
    pub(crate) fn new_config(
        &self,
        project_id: ProjectId,
        config_name: &str,
        config: &str,
    ) -> AnyResult<(ConfigId, Version)> {
        debug!("new_config {project_id} {config_name} {config}");
        // Check that the project exists, so we return correct error status
        // instead of Internal Server Error due to the next query failing.
        let _descr = self.get_project(project_id)?;

        let row = sqlx::query(
            "INSERT INTO project_config (project_id, version, name, config) VALUES(?, 1, ?, ?) RETURNING id")
            .bind(project_id.0)
            .bind(config_name)
            .bind(config)
            .fetch_one(&self.conn)
            .await?;
        Ok((ConfigId(row.get(0)), Version(1)))
    }

    /// Update existing project config.
    ///
    /// Update config name and, optionally, YAML.
    pub(crate) async fn update_config(
        &self,
        config_id: ConfigId,
        config_name: &str,
        config: &Option<String>,
    ) -> AnyResult<Version> {
        let descr = self.get_config(config_id)?;

        let config = config.clone().unwrap_or(descr.config);

        let version = descr.version.increment();
        sqlx::query("UPDATE project_config SET version = ?, name = ?, config = ? WHERE id = ?")
            .bind(version.0)
            .bind(config_name)
            .bind(config)
            .bind(config_id.0)
            .execute(&self.conn)
            .await?;

        Ok(version)
    }

    /// Delete project config.
    pub(crate) async fn delete_config(&self, config_id: ConfigId) -> AnyResult<()> {
        let res = sqlx::query("DELETE FROM project_config WHERE id = ?")
            .bind(config_id.0)
            .execute(&self.conn)
            .await?;
        if res.rows_affected() > 0 {
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
        let row = sqlx::query(
                "INSERT INTO pipeline (project_id, project_version, killed, created) VALUES(?, ?, 0, unixepoch('now')) RETURNING id"
            )
            .bind(project_id.0)
            .bind(project_version.0)
            .fetch_one(&self.conn)
            .await?;

        Ok(PipelineId(row.get(0)))
    }

    pub(crate) async fn pipeline_set_port(
        &self,
        pipeline_id: PipelineId,
        port: u16,
    ) -> AnyResult<()> {
        let _ = sqlx::query("UPDATE pipeline SET port = ? where id = ?")
            .bind(port as i32) // bind(u64) forces the typechecker to expect Sqlite
            .bind(pipeline_id.0)
            .execute(&self.conn)
            .await?;
        Ok(())
    }

    /// Read pipeline status.
    ///
    /// Returns pipeline port number and `killed` flag.
    pub(crate) async fn pipeline_status(&self, pipeline_id: PipelineId) -> AnyResult<(u16, bool)> {
        let row = sqlx::query("SELECT port, killed FROM pipeline WHERE id = ?")
            .bind(pipeline_id.0)
            .fetch_one(&self.conn)
            .await
            .map_err(|_| anyhow!(DBError::UnknownPipeline(pipeline_id)))?;
        Ok((
            row.get::<i32, usize>(0) as u16,
            row.get::<i32, usize>(1) != 0,
        ))
    }

    /// Set `killed` flag to `true`.
    pub(crate) async fn set_pipeline_killed(&self, pipeline_id: PipelineId) -> AnyResult<bool> {
        let res = sqlx::query("UPDATE pipeline SET killed=1 WHERE id = ?")
            .bind(pipeline_id.0)
            .execute(&self.conn)
            .await?;
        Ok(res.rows_affected() > 0)
    }

    /// Delete `pipeline` from the DB.
    pub(crate) async fn delete_pipeline(&self, pipeline_id: PipelineId) -> AnyResult<bool> {
        let res = sqlx::query("DELETE FROM pipeline WHERE id = $1")
            .bind(pipeline_id.0)
            .execute(&self.conn)
            .await?;
        Ok(res.rows_affected() > 0)
    }

    /// List pipelines associated with `project_id`.
    pub(crate) fn list_project_pipelines(
        &self,
        project_id: ProjectId,
    ) -> AnyResult<Vec<PipelineDescr>> {
        // Check that the project exists, so we return an error instead of an
        // empty list of pipelines.
        let _descr = self.get_project(project_id)?;

        let rows = sqlx::query(
            "SELECT id, project_version, port, killed, created FROM pipeline WHERE project_id = ?",
        )
        .bind(project_id.0)
        .fetch_all(&self.conn)
        .await?;

        let mut result = Vec::new();

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
                project_id,
                project_version: Version(row.get(1)),
                port: row.get::<i32, _>(2) as u16,
                killed: row.get::<i32, _>(3) != 0,
                created: DateTime::<Utc>::from_utc(created_naive, Utc),
            });
        }

        Ok(result)
    }
}

#[cfg(test)]
mod test {
    use super::{ProjectDB, ProjectDescr, ProjectStatus};
    use crate::db::DBError;
    use sqlx::pool::PoolOptions;
    use sqlx::{Any, Row};
    use tempfile::TempDir;

    async fn test_setup() -> (ProjectDB, TempDir) {
        let temp_dir = tempfile::tempdir().unwrap();
        let s = format!(
            "sqlite:{}",
            temp_dir.path().join("db.sqlite?mode=rwc").to_str().unwrap()
        );
        (
            ProjectDB::connect_inner(s.as_str(), PoolOptions::<Any>::new(), &Some("".to_string()))
                .await
                .unwrap(),
            temp_dir,
        )
    }

    #[tokio::test]
    async fn schema_creation() {
        let (db, _dir) = test_setup().await;
        let rows = sqlx::query("SELECT name FROM sqlite_schema WHERE type = 'table'")
            .fetch_all(&db.conn)
            .await
            .unwrap();
        let mut expected = std::collections::HashSet::new();
        expected.insert("project");
        expected.insert("project_config");
        expected.insert("pipeline");
        expected.insert("endpoint");
        expected.insert("source");
        for row in rows {
            let table_name: String = row.get(0);
            expected.contains(table_name.as_str());
        }
    }

    #[tokio::test]
    async fn project_creation() {
        let (db, _dir) = test_setup().await;
        let res = db
            .new_project("test1", "project desc", "ignored")
            .await
            .unwrap();
        let rows = db.list_projects().await.unwrap();
        assert_eq!(1, rows.len());
        let expected = ProjectDescr {
            project_id: res.0,
            name: "test1".to_string(),
            description: "project desc".to_string(),
            version: res.1,
            status: ProjectStatus::None,
            schema: None,
        };
        let actual = rows.get(0).unwrap();
        assert_eq!(&expected, actual);
    }

    #[tokio::test]
    async fn duplicate_project() {
        let (db, _dir) = test_setup().await;
        let _ = db.new_project("test1", "project desc", "ignored").await;
        let res = db
            .new_project("test1", "project desc", "ignored")
            .await
            .expect_err("Expecting unique violation");
        let expected = anyhow::anyhow!(DBError::DuplicateProjectName("test1".to_string()));
        assert_eq!(format!("{}", res), format!("{}", expected));
    }

    #[tokio::test]
    async fn project_reset() {
        let (db, _dir) = test_setup().await;
        db.new_project("test1", "project desc", "ignored")
            .await
            .unwrap();
        db.new_project("test2", "project desc", "ignored")
            .await
            .unwrap();
        db.reset_project_status().await.unwrap();
        let results = db.list_projects().await.unwrap();
        for p in results {
            assert_eq!(ProjectStatus::None, p.status);
            assert_eq!(None, p.schema); //can't check for error fields directly
        }
        let results =
            sqlx::query("SELECT * FROM project WHERE status != '' OR error != '' OR schema != ''")
                .fetch_all(&db.conn)
                .await;
        assert_eq!(0, results.unwrap().len());
    }

    #[tokio::test]
    async fn project_code() {
        let (db, _dir) = test_setup().await;
        let (project_id, _) = db
            .new_project("test1", "project desc", "create table t1(c1 integer);")
            .await
            .unwrap();
        let results = db.project_code(project_id).await.unwrap();
        assert_eq!("test1", results.0.name);
        assert_eq!("project desc", results.0.description);
        assert_eq!("create table t1(c1 integer);".to_owned(), results.1);
    }

    #[tokio::test]
    async fn update_project() {
        let (db, _dir) = test_setup().await;
        let (project_id, _) = db
            .new_project("test1", "project desc", "create table t1(c1 integer);")
            .await
            .unwrap();
        let _ = db
            .update_project(project_id, "updated_test1", "some new description", &None)
            .await;
        let results = db.list_projects().await.unwrap();
        assert_eq!(1, results.len());
        let row = results.get(0).unwrap();
        assert_eq!("updated_test1", row.name);
        assert_eq!("some new description", row.description);
    }

    #[tokio::test]
    async fn project_queries() {
        let (db, _dir) = test_setup().await;
        let (project_id, _) = db
            .new_project("test1", "project desc", "create table t1(c1 integer);")
            .await
            .unwrap();
        let desc = db.get_project_if_exists(project_id).await.unwrap().unwrap();
        assert_eq!("test1", desc.name);
        let desc = db.lookup_project("test1").await.unwrap().unwrap();
        assert_eq!("test1", desc.name);
        let desc = db.lookup_project("test2").await.unwrap();
        assert!(desc.is_none());
    }

    #[tokio::test]
    async fn update_status() {
        let (db, _dir) = test_setup().await;
        let (project_id, _) = db
            .new_project("test1", "project desc", "create table t1(c1 integer);")
            .await
            .unwrap();
        let desc = db.get_project_if_exists(project_id).await.unwrap().unwrap();
        assert_eq!(ProjectStatus::None, desc.status);
        let _ = db
            .set_project_status(project_id, ProjectStatus::CompilingRust)
            .await
            .unwrap();
        let desc = db.get_project_if_exists(project_id).await.unwrap().unwrap();
        assert_eq!(ProjectStatus::CompilingRust, desc.status);
    }
}
