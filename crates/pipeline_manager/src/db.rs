use crate::{AttachedConnector, Direction, ProjectStatus};
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
    db_type: DbType,
}

#[derive(Eq, PartialEq, Debug, Clone, Copy)]
enum DbType {
    Sqlite,
    Postgres,
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

/// Unique connector id.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[repr(transparent)]
#[serde(transparent)]
pub(crate) struct ConnectorId(pub i64);
impl Display for ConnectorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Unique attached connector id.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[repr(transparent)]
#[serde(transparent)]
pub(crate) struct AttachedConnectorId(pub i64);
impl Display for AttachedConnectorId {
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
    pub project_id: Option<ProjectId>,
    pub pipeline: Option<PipelineDescr>,
    pub version: Version,
    pub name: String,
    pub description: String,
    pub config: String,
    pub attached_connectors: Vec<AttachedConnector>,
}

/// Pipeline descriptor.
#[derive(Serialize, ToSchema, Debug)]
pub(crate) struct PipelineDescr {
    pub pipeline_id: PipelineId,
    pub config_id: ConfigId,
    pub port: u16,
    pub killed: bool,
    pub created: DateTime<Utc>,
}

/// Type of new data connector.
#[derive(Serialize, Deserialize, ToSchema, Debug, Copy, Clone)]
pub enum ConnectorType {
    KafkaIn = 0,
    KafkaOut = 1,
    File = 2,
}

impl From<i64> for ConnectorType {
    fn from(val: i64) -> Self {
        match val {
            0 => ConnectorType::KafkaIn,
            1 => ConnectorType::KafkaOut,
            2 => ConnectorType::File,
            _ => panic!("invalid connector type"),
        }
    }
}

#[allow(clippy::from_over_into)]
impl Into<Direction> for ConnectorType {
    fn into(self) -> Direction {
        match self {
            ConnectorType::KafkaIn => Direction::Input,
            ConnectorType::KafkaOut => Direction::Output,
            ConnectorType::File => Direction::InputOutput,
        }
    }
}

/// Connector descriptor.
#[derive(Serialize, ToSchema, Debug)]
pub(crate) struct ConnectorDescr {
    pub connector_id: ConnectorId,
    pub name: String,
    pub description: String,
    pub typ: ConnectorType,
    pub config: String,
    pub direction: Direction,
}

impl ProjectDB {
    pub(crate) async fn connect(
        connection_str: &str,
        initial_sql: &Option<String>,
    ) -> AnyResult<Self> {
        Self::connect_inner(connection_str, PoolOptions::new(), initial_sql).await
    }

    fn auto_increment(db_type: &DbType) -> &str {
        match db_type {
            DbType::Sqlite => "INTEGER PRIMARY KEY AUTOINCREMENT",
            DbType::Postgres => "SERIAL PRIMARY KEY",
        }
    }

    fn current_time(&self) -> &str {
        match self.db_type {
            DbType::Sqlite => "unixepoch('now')",
            DbType::Postgres => "extract(epoch from now())",
        }
    }

    /// Connect to the project database.
    async fn connect_inner(
        connection_str: &str,
        pool_options: PoolOptions<Any>,
        initial_sql: &Option<String>,
    ) -> AnyResult<Self> {
        let db_type = if connection_str.starts_with("sqlite") {
            DbType::Sqlite
        } else if connection_str.starts_with("postgres") {
            DbType::Postgres
        } else {
            panic!("Unsupported connection string {}", connection_str)
        };
        //sqlx::any::install_default_drivers();
        debug!("Opening connection to {:?}", connection_str);
        let mut options = AnyConnectOptions::from_str(connection_str)?;
        options.log_statements(log::LevelFilter::Trace);
        let conn = pool_options.connect_with(options).await?;
        sqlx::query(
            format!(
                "
        CREATE TABLE IF NOT EXISTS project (
            id {},
            version integer NOT NULL,
            name varchar UNIQUE NOT NULL,
            description varchar NOT NULL,
            code varchar NOT NULL,
            schema varchar,
            status varchar,
            error varchar,
            status_since integer NOT NULL)",
                Self::auto_increment(&db_type),
            )
            .as_str(),
        )
        .execute(&conn)
        .await?;

        sqlx::query(
            format!(
                "
        CREATE TABLE IF NOT EXISTS pipeline (
            id {},
            config_id integer NOT NULL,
            config_version integer NOT NULL,
            -- TODO: add 'host' field when we support remote pipelines.
            port integer,
            killed boolean NOT NULL,
            created integer NOT NULL)",
                Self::auto_increment(&db_type)
            )
            .as_str(),
        )
        .execute(&conn)
        .await?;

        sqlx::query(
            format!(
                "
        CREATE TABLE IF NOT EXISTS project_config (
            id {},
            pipeline_id integer,
            project_id integer,
            version integer NOT NULL,
            name varchar NOT NULL,
            description varchar NOT NULL,
            config varchar NOT NULL,
            FOREIGN KEY (project_id) REFERENCES project(id) ON DELETE CASCADE,
            FOREIGN KEY (pipeline_id) REFERENCES pipeline(id) ON DELETE SET NULL);",
                Self::auto_increment(&db_type)
            )
            .as_str(),
        )
        .execute(&conn)
        .await?;

        sqlx::query(
            format!(
                "
        CREATE TABLE IF NOT EXISTS connector (
            id {},
            name varchar NOT NULL,
            description varchar NOT NULL,
            typ integer NOT NULL,
            config varchar NOT NULL)",
                Self::auto_increment(&db_type)
            )
            .as_str(),
        )
        .execute(&conn)
        .await?;

        sqlx::query(
            format!(
                "
        CREATE TABLE IF NOT EXISTS attached_connector (
            id {},
            uuid varchar UNIQUE NOT NULL,
            config_id integer NOT NULL,
            connector_id integer NOT NULL,
            config varchar,
            is_input boolean NOT NULL,
            FOREIGN KEY (config_id) REFERENCES project_config(id) ON DELETE CASCADE,
            FOREIGN KEY (connector_id) REFERENCES connector(id) ON DELETE CASCADE)",
                Self::auto_increment(&db_type)
            )
            .as_str(),
        )
        .execute(&conn)
        .await?;

        if let Some(initial_sql_file) = &initial_sql {
            if let Ok(initial_sql) = std::fs::read_to_string(initial_sql_file) {
                sqlx::query(&initial_sql).execute(&conn).await?;
            } else {
                log::warn!("initial SQL file '{}' does not exist", initial_sql_file);
            }
        }

        Ok(Self { conn, db_type })
    }

    /// Reset everything that is set through compilation of the project.
    ///
    /// - Set status to `ProjectStatus::None` after server restart.
    /// - Reset `schema` to None.
    pub(crate) async fn reset_project_status(&self) -> AnyResult<()> {
        sqlx::query("UPDATE project SET status = NULL, error = NULL, schema = NULL")
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
            let status: Option<String> = row.get(4);
            let error: Option<String> = row.get(5);
            let status = ProjectStatus::from_columns(status.as_deref(), error)?;
            let schema: Option<String> = row.get(6);

            result.push(ProjectDescr {
                project_id: ProjectId(row.get::<i32, _>(0) as i64),
                name: row.get(1),
                description: row.get(2),
                version: Version(row.get::<i32, _>(3) as i64),
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
            "SELECT name, description, version, status, error, code, schema FROM project WHERE id = $1",
        ).bind(project_id.0).fetch_one(&self.conn).await;

        if let Ok(row) = fetched {
            let name: String = row.get(0);
            let description: String = row.get(1);
            let version: Version = Version(row.get::<i32, _>(2) as i64);
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
        } else {
            Err(DBError::UnknownProject(project_id).into())
        }
    }

    /// Helper to convert sqlx error into a `DBError::DuplicateProjectName`
    /// if the underlying low-level error thrown by the database matches.
    fn maybe_duplicate_project_name_err(e: sqlx::Error, _project_name: &str) -> AnyError {
        match e {
            sqlx::Error::Database(db_error) => {
                // UPGRADE: sqlx 0.7 use is_unique_violation()
                if db_error
                    .message()
                    .to_lowercase()
                    .contains("unique constraint")
                {
                    anyhow!(DBError::DuplicateProjectName(_project_name.to_string()))
                } else {
                    anyhow!(db_error)
                }
            }
            _ => anyhow!(e),
        }
    }

    /// Create a new project.
    pub(crate) async fn new_project(
        &self,
        project_name: &str,
        project_description: &str,
        project_code: &str,
    ) -> AnyResult<(ProjectId, Version)> {
        debug!("new_project {project_name} {project_description} {project_code}");
        sqlx::query(
            format!(
                "INSERT INTO project (version, name, description, code, schema, status,
                    error, status_since) VALUES(1, $1, $2, $3, NULL, NULL, NULL, {});",
                self.current_time()
            )
            .as_str(),
        )
        .bind(project_name)
        .bind(project_description)
        .bind(project_code)
        .execute(&self.conn)
        .await
        .map_err(|e| ProjectDB::maybe_duplicate_project_name_err(e, project_name))?;

        // name has a UNIQUE constraint
        let id: i32 = sqlx::query("SELECT id FROM project WHERE name = $1")
            .bind(project_name)
            .fetch_one(&self.conn)
            .await?
            .get(0);

        Ok((ProjectId(id as i64), Version(1)))
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
            sqlx::query("SELECT version, code FROM project where id = $1")
                .bind(project_id.0)
                .map(|row: AnyRow| (Version(row.get::<i32, _>(0) as i64), row.get(1)))
                .fetch_one(&self.conn)
                .await
                .map_err(|_| DBError::UnknownProject(project_id))?;

        match project_code {
            Some(code) if &old_code != code => {
                // Only increment `version` if new code actually differs from the
                // current version.
                version = version.increment();
                sqlx::query(
                        "UPDATE project SET version = $1, name = $2, description = $3, code = $4, status = NULL, error = NULL, schema = NULL WHERE id = $5")
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
                sqlx::query("UPDATE project SET name = $1, description = $2 WHERE id = $3")
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
    pub(crate) async fn get_project_if_exists(
        &self,
        project_id: ProjectId,
    ) -> AnyResult<Option<ProjectDescr>> {
        let row = sqlx::query(
            "SELECT name, description, version, status, error, schema FROM project WHERE id = $1",
        )
        .bind(project_id.0)
        .fetch_one(&self.conn)
        .await;

        if let Ok(row) = row {
            let name: String = row.get(0);
            let description: String = row.get(1);
            let version: Version = Version(row.get::<i32, _>(2) as i64);
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

    /// Lookup project by name
    pub(crate) async fn lookup_project(
        &self,
        project_name: &str,
    ) -> AnyResult<Option<ProjectDescr>> {
        let row = sqlx::query(
            "SELECT id, description, version, status, error, schema FROM project WHERE name = $1",
        )
        .bind(project_name)
        .fetch_one(&self.conn)
        .await;

        if let Ok(row) = row {
            let project_id: ProjectId = ProjectId(row.get::<i32, _>(0) as i64);
            let description: String = row.get(1);
            let version: Version = Version(row.get::<i32, _>(2) as i64);
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

    /// Retrieve project descriptor.
    ///
    /// Returns a `DBError:UnknownProject` error if `project_id` is not found in
    /// the database.
    pub(crate) async fn get_project(&self, project_id: ProjectId) -> AnyResult<ProjectDescr> {
        self.get_project_if_exists(project_id)
            .await?
            .ok_or_else(|| anyhow!(DBError::UnknownProject(project_id)))
    }

    /// Validate project version and retrieve project descriptor.
    ///
    /// Returns `DBError::UnknownProject` if `project_id` is not found in the
    /// database. Returns `DBError::OutdatedProjectVersion` if the current
    /// project version differs from `expected_version`.
    pub(crate) async fn get_project_guarded(
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
            format!(
                "UPDATE project SET status = $1, error = $2, schema = '', status_since = {} WHERE id = $3",
                self.current_time()).as_str()
            )
            .bind(status)
            .bind(error)
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
                format!(
                    "UPDATE project SET status = $1, error = $2, status_since = {} WHERE id = $3",
                    self.current_time()
                )
                .as_str(),
            )
            .bind(status)
            .bind(error)
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
        sqlx::query("UPDATE project SET schema = $1 WHERE id = $2")
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
    pub(crate) async fn set_project_pending(
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
    pub(crate) async fn cancel_project(
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

    /// Delete project from the database.
    ///
    /// This will delete all project configs and pipelines.
    pub(crate) async fn delete_project(&self, project_id: ProjectId) -> AnyResult<()> {
        let res = sqlx::query("DELETE FROM project WHERE id = $1")
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
    pub(crate) async fn next_job(&self) -> AnyResult<Option<(ProjectId, Version)>> {
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
    }

    pub(crate) async fn list_configs(&self) -> AnyResult<Vec<ConfigDescr>> {
        let rows = sqlx::query(
            "SELECT id, version, name, description, config, pipeline_id, project_id FROM project_config",
            )
            .fetch_all(&self.conn)
            .await?;

        let mut result = Vec::new();
        for row in rows {
            let config_id = ConfigId(row.get(0));
            let project_id = row.get::<Option<i32>, _>(6).map(|id| ProjectId(id as i64));
            let attached_connectors = self.get_attached_connectors(config_id).await?;
            let pipeline = if let Some(pipeline_id) =
                row.get::<Option<i32>, _>(5).map(|id| PipelineId(id as i64))
            {
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

    pub(crate) async fn get_config(&self, config_id: ConfigId) -> AnyResult<ConfigDescr> {
        let (mut descr, pipeline_id): (ConfigDescr, Option<PipelineId>) = sqlx::query(
            "SELECT id, version, name, description, config, pipeline_id, project_id FROM project_config WHERE id = ?",
        )
        .bind(config_id.0)
        .fetch_one(&self.conn)
        .await
        .map(|row| {
            let pipeline_id: Option<PipelineId> = row.get::<Option<i64>, _>(5).map(PipelineId);
            let project_id = row.get::<Option<i64>, _>(6).map(ProjectId);

            (ConfigDescr {
                config_id,
                project_id,
                version: Version(row.get(1)),
                name: row.get(2),
                description: row.get(3),
                config: row.get(4),
                pipeline: None,
                attached_connectors: Vec::new(),
            }, pipeline_id)
        })
        .map_err(|_| anyhow!(DBError::UnknownConfig(config_id)))?;

        descr.attached_connectors = self.get_attached_connectors(config_id).await?;
        if let Some(pipeline_id) = pipeline_id {
            descr.pipeline = Some(self.get_pipeline(pipeline_id).await?);
        }

        Ok(descr)
    }

    /// Create a new project config.
    pub(crate) async fn new_config(
        &self,
        project_id: Option<ProjectId>,
        config_name: &str,
        config_description: &str,
        config: &str,
        connectors: &Option<Vec<AttachedConnector>>,
    ) -> AnyResult<(ConfigId, Version)> {
        let row = sqlx::query(
            "INSERT INTO project_config (project_id, version, name, description, config) VALUES($1, 1, $2, $3, $4) RETURNING id")
            .bind(project_id.map(|id| id.0))
            .bind(config_name)
            .bind(config_description)
            .bind(config)
            .fetch_one(&self.conn)
            .await?;
        let config_id = ConfigId(row.get(0));

        if let Some(connectors) = connectors {
            // Add the connectors.
            // TODO: This should be done in a transaction with the query above.
            // Unclear if this is currently the case.
            for ac in connectors {
                self.attach_connector(config_id, ac).await?;
            }
        }

        Ok((config_id, Version(1)))
    }

    /// Add pipeline to the config.
    pub(crate) async fn add_pipeline_to_config(
        &self,
        config_id: ConfigId,
        pipeline_id: PipelineId,
    ) -> AnyResult<()> {
        let _row = sqlx::query("UPDATE project_config SET pipeline_id = ? WHERE id = ?")
            .bind(pipeline_id.0)
            .bind(config_id.0)
            .execute(&self.conn)
            .await?;
        Ok(())
    }

    /// Remove pipeline from the config.
    pub(crate) async fn remove_pipeline_from_config(&self, config_id: ConfigId) -> AnyResult<()> {
        let _row = sqlx::query("UPDATE project_config SET pipeline_id = NULL WHERE id = ?")
            .bind(config_id.0)
            .execute(&self.conn)
            .await?;
        Ok(())
    }

    /// Update existing project config.
    ///
    /// Update config name and, optionally, YAML.
    pub(crate) async fn update_config(
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
            sqlx::query("DELETE FROM attached_connector WHERE config_id = ?")
                .bind(config_id.0)
                .execute(&self.conn)
                .await?;

            // Rewrite the new set of connectors.
            for ac in connectors {
                // TODO: This should be done in a transaction with the query above.
                // Unclear if this is currently the case.
                self.attach_connector(config_id, ac).await?;
            }
        }

        let version = descr.version.increment();
        sqlx::query("UPDATE project_config SET version = $1, name = $2, description = $3, config = $4, project_id = $5 WHERE id = $6")
            .bind(version.0)
            .bind(config_name)
            .bind(config_description)
            .bind(config)
            .bind(project_id.map(|id| id.0))
            .bind(config_id.0)
            .execute(&self.conn)
            .await?;

        Ok(version)
    }

    /// Delete project config.
    pub(crate) async fn delete_config(&self, config_id: ConfigId) -> AnyResult<()> {
        let res = sqlx::query("DELETE FROM project_config WHERE id = $1")
            .bind(config_id.0)
            .execute(&self.conn)
            .await?;
        if res.rows_affected() > 0 {
            Ok(())
        } else {
            Err(anyhow!(DBError::UnknownConfig(config_id)))
        }
    }

    async fn get_attached_connectors(
        &self,
        config_id: ConfigId,
    ) -> AnyResult<Vec<AttachedConnector>> {
        let rows = sqlx::query(
            "SELECT uuid, connector_id, config, is_input FROM attached_connector WHERE config_id = ?",
            )
            .bind(config_id.0)
            .fetch_all(&self.conn)
            .await?;
        let mut result = Vec::new();

        for row in rows {
            let direction = if row.get::<bool, _>(3) {
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

    /// Attach connector to the config.
    ///
    /// # Precondition
    /// - A valid config for `config_id` must exist.
    async fn attach_connector(
        &self,
        config_id: ConfigId,
        ac: &AttachedConnector,
    ) -> AnyResult<AttachedConnectorId> {
        let _descr = self.get_config(config_id).await?;
        let _descr = self.get_connector(ac.connector_id).await?;
        let is_input = ac.direction == Direction::Input;

        let row = sqlx::query(
            "INSERT INTO attached_connector (uuid, config_id, connector_id, is_input, config) VALUES($1, $2, $3, $4, $5) RETURNING id",
            )
            .bind(&ac.uuid)
            .bind(config_id.0)
            .bind(ac.connector_id.0)
            .bind(is_input)
            .bind(&ac.config)
            .fetch_one(&self.conn)
            .await?;

        Ok(AttachedConnectorId(row.get(0)))
    }

    /// Insert a new record to the `pipeline` table.
    pub(crate) async fn new_pipeline(
        &self,
        config_id: ConfigId,
        config_version: Version,
    ) -> AnyResult<PipelineId> {
        let row = sqlx::query(
            format!(
                "INSERT INTO pipeline (config_id, config_version, killed, created) VALUES($1, $2, 0, {}) RETURNING id",
            self.current_time()).as_str()
            )
            .bind(config_id.0)
            .bind(config_version.0)
            .fetch_one(&self.conn)
            .await?;

        Ok(PipelineId(row.get(0)))
    }

    pub(crate) async fn pipeline_set_port(
        &self,
        pipeline_id: PipelineId,
        port: u16,
    ) -> AnyResult<()> {
        let _ = sqlx::query("UPDATE pipeline SET port = $1 where id = $2")
            .bind(port as i32) // bind(u64) forces the typechecker to expect Sqlite
            .bind(pipeline_id.0)
            .execute(&self.conn)
            .await?;
        Ok(())
    }

    /// Set `killed` flag to `true`.
    pub(crate) async fn set_pipeline_killed(&self, pipeline_id: PipelineId) -> AnyResult<bool> {
        let res = sqlx::query("UPDATE pipeline SET killed=true WHERE id = $1")
            .bind(pipeline_id.0)
            .execute(&self.conn)
            .await?;
        Ok(res.rows_affected() > 0)
    }

    /// Delete `pipeline` from the DB.
    pub(crate) async fn delete_pipeline(&self, pipeline_id: PipelineId) -> AnyResult<bool> {
        let res = sqlx::query("DELETE FROM pipeline WHERE id = ?")
            .bind(pipeline_id.0)
            .execute(&self.conn)
            .await?;
        Ok(res.rows_affected() > 0)
    }

    /// Retrieve project config.
    pub(crate) async fn get_pipeline(&self, pipeline_id: PipelineId) -> AnyResult<PipelineDescr> {
        let row =
            sqlx::query("SELECT id, config_id, port, killed, created FROM pipeline WHERE id = ?")
                .bind(pipeline_id.0)
                .fetch_one(&self.conn)
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
            config_id: ConfigId(row.get(1)),
            port: row.get::<i32, _>(2) as u16,
            killed: row.get(3),
            created: DateTime::<Utc>::from_utc(created_naive, Utc),
        })
    }

    /// List pipelines associated with `project_id`.
    pub(crate) async fn list_pipelines(&self) -> AnyResult<Vec<PipelineDescr>> {
        let rows = sqlx::query("SELECT id, config_id, port, killed, created FROM pipeline")
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
                config_id: ConfigId(row.get(1)),
                port: row.get::<i32, _>(2) as u16,
                killed: row.get::<i32, _>(3) != 0,
                created: DateTime::<Utc>::from_utc(created_naive, Utc),
            });
        }

        Ok(result)
    }

    /// Create a new connector.
    pub(crate) async fn new_connector(
        &self,
        name: &str,
        description: &str,
        typ: ConnectorType,
        config: &str,
    ) -> AnyResult<ConnectorId> {
        debug!("new_connector {name} {description} {config}");
        let row = sqlx::query("INSERT INTO connector (name, description, typ, config) VALUES($1, $2, $3, $4) RETURNING id")
            .bind(name)
            .bind(description)
            .bind(typ as i32)
            .bind(config)
            .fetch_one(&self.conn)
            .await?;
        Ok(ConnectorId(row.get(0)))
    }

    /// Retrieve connectors list from the DB.
    pub(crate) async fn list_connectors(&self) -> AnyResult<Vec<ConnectorDescr>> {
        let rows = sqlx::query("SELECT id, name, description, typ, config FROM connector")
            .fetch_all(&self.conn)
            .await?;

        let mut result = Vec::new();

        for row in rows {
            let typ = row.get::<i64, _>(3).into();
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

    /// Retrieve connector descriptor.
    pub(crate) async fn get_connector(
        &self,
        connector_id: ConnectorId,
    ) -> AnyResult<ConnectorDescr> {
        let row = sqlx::query("SELECT name, description, typ, config FROM connector WHERE id = ?")
            .bind(connector_id.0)
            .fetch_one(&self.conn)
            .await?;

        let name: String = row.get(0);
        let description: String = row.get(1);
        let typ: ConnectorType = row.get::<i64, _>(2).into();
        let config: String = row.get(3);

        Ok(ConnectorDescr {
            connector_id,
            name,
            description,
            typ,
            direction: typ.into(),
            config,
        })
    }

    /// Update existing connector config.
    ///
    /// Update connector name and, optionally, YAML.
    pub(crate) async fn update_connector(
        &mut self,
        connector_id: ConnectorId,
        connector_name: &str,
        description: &str,
        config: &Option<String>,
    ) -> AnyResult<()> {
        let descr = self.get_connector(connector_id).await?;
        let config = config.clone().unwrap_or(descr.config);

        sqlx::query("UPDATE connector SET name = $1, description = $2, config = $3 WHERE id = $4")
            .bind(connector_name)
            .bind(description)
            .bind(config.as_str())
            .bind(connector_id.0)
            .execute(&self.conn)
            .await?;

        Ok(())
    }

    /// Delete connector from the database.
    ///
    /// This will delete all connector configs and pipelines.
    pub(crate) async fn delete_connector(&self, connector_id: ConnectorId) -> AnyResult<()> {
        let res = sqlx::query("DELETE FROM connector WHERE id = ?")
            .bind(connector_id.0)
            .execute(&self.conn)
            .await?;

        if res.rows_affected() > 0 {
            Ok(())
        } else {
            Err(anyhow!(DBError::UnknownConnector(connector_id)))
        }
    }
}

#[cfg(test)]
mod test {
    use super::{DbType, ProjectDB, ProjectDescr, ProjectStatus};
    use crate::db::DBError;
    use rstest::rstest;
    use serial_test::serial;
    use sqlx::pool::PoolOptions;
    use sqlx::{Any, Row};
    use tempfile::TempDir;

    async fn test_setup(db_type: &DbType) -> (ProjectDB, TempDir) {
        let temp_dir = tempfile::tempdir().unwrap();
        match db_type {
            DbType::Sqlite => {
                let s = format!(
                    "sqlite:{}",
                    temp_dir.path().join("db.sqlite?mode=rwc").to_str().unwrap()
                );
                (
                    ProjectDB::connect_inner(
                        s.as_str(),
                        PoolOptions::<Any>::new(),
                        &Some("".to_string()),
                    )
                    .await
                    .unwrap(),
                    temp_dir,
                )
            }
            DbType::Postgres => {
                let s = "postgres://";
                let conn =
                    ProjectDB::connect_inner(s, PoolOptions::<Any>::new(), &Some("".to_string()))
                        .await
                        .unwrap();
                for table in vec![
                    "project",
                    "project_config",
                    "pipeline",
                    "attached_connector",
                    "connector",
                ] {
                    sqlx::query(format!("truncate table {} cascade;", table).as_str())
                        .execute(&conn.conn)
                        .await
                        .unwrap();
                }
                (conn, temp_dir)
            }
        }
    }

    #[tokio::test]
    #[serial]
    async fn schema_creation() {
        let (db, _dir) = test_setup(&DbType::Sqlite).await;
        let rows = sqlx::query("SELECT name FROM sqlite_schema WHERE type = 'table'")
            .fetch_all(&db.conn)
            .await
            .unwrap();
        let mut expected = std::collections::HashSet::new();
        expected.insert("project");
        expected.insert("project_config");
        expected.insert("pipeline");
        expected.insert("attached_connector");
        expected.insert("connector");
        for row in rows {
            let table_name: String = row.get(0);
            expected.contains(table_name.as_str());
        }
    }

    fn ignore_postgres(db: DbType) -> bool {
        let toggle = std::env::var("RUN_POSTGRES_TESTS");
        db == DbType::Postgres && (toggle.is_err() || toggle.unwrap() == "0")
    }

    #[rstest]
    #[serial]
    #[trace]
    #[tokio::test]
    async fn project_creation(#[values(DbType::Sqlite, DbType::Postgres)] db_type: DbType) {
        if ignore_postgres(db_type) {
            println!("Ignoring postgres test");
            return;
        };
        let (db, _dir) = test_setup(&db_type).await;
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

    #[rstest]
    #[serial]
    #[trace]
    #[tokio::test]
    async fn duplicate_project(#[values(DbType::Sqlite, DbType::Postgres)] db_type: DbType) {
        if ignore_postgres(db_type) {
            println!("Ignoring postgres test");
            return;
        };
        let (db, _dir) = test_setup(&db_type).await;
        let _ = db.new_project("test1", "project desc", "ignored").await;
        let res = db
            .new_project("test1", "project desc", "ignored")
            .await
            .expect_err("Expecting unique violation");
        let expected = anyhow::anyhow!(DBError::DuplicateProjectName("test1".to_string()));
        assert_eq!(format!("{}", res), format!("{}", expected));
    }

    #[rstest]
    #[serial]
    #[trace]
    #[tokio::test]
    async fn project_reset(#[values(DbType::Sqlite, DbType::Postgres)] db_type: DbType) {
        if ignore_postgres(db_type) {
            println!("Ignoring postgres test");
            return;
        };
        let (db, _dir) = test_setup(&db_type).await;
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

    #[rstest]
    #[serial]
    #[trace]
    #[tokio::test]
    async fn project_code(#[values(DbType::Sqlite, DbType::Postgres)] db_type: DbType) {
        if ignore_postgres(db_type) {
            println!("Ignoring postgres test");
            return;
        };
        let (db, _dir) = test_setup(&db_type).await;
        let (project_id, _) = db
            .new_project("test1", "project desc", "create table t1(c1 integer);")
            .await
            .unwrap();
        let results = db.project_code(project_id).await.unwrap();
        assert_eq!("test1", results.0.name);
        assert_eq!("project desc", results.0.description);
        assert_eq!("create table t1(c1 integer);".to_owned(), results.1);
    }

    #[rstest]
    #[serial]
    #[trace]
    #[tokio::test]
    async fn update_project(#[values(DbType::Sqlite, DbType::Postgres)] db_type: DbType) {
        if ignore_postgres(db_type) {
            println!("Ignoring postgres test");
            return;
        };
        let (db, _dir) = test_setup(&db_type).await;
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

    #[rstest]
    #[serial]
    #[trace]
    #[tokio::test]
    async fn project_queries(#[values(DbType::Sqlite, DbType::Postgres)] db_type: DbType) {
        if ignore_postgres(db_type) {
            println!("Ignoring postgres test");
            return;
        };
        let (db, _dir) = test_setup(&db_type).await;
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

    #[rstest]
    #[serial]
    #[trace]
    #[tokio::test]
    async fn update_status(#[values(DbType::Sqlite, DbType::Postgres)] db_type: DbType) {
        if ignore_postgres(db_type) {
            println!("Ignoring postgres test");
            return;
        };
        let (db, _dir) = test_setup(&db_type).await;
        let (project_id, _) = db
            .new_project("test1", "project desc", "create table t1(c1 integer);")
            .await
            .unwrap();
        let desc = db.get_project_if_exists(project_id).await.unwrap().unwrap();
        assert_eq!(ProjectStatus::None, desc.status);
        db.set_project_status(project_id, ProjectStatus::CompilingRust)
            .await
            .unwrap();
        let desc = db.get_project_if_exists(project_id).await.unwrap().unwrap();
        assert_eq!(ProjectStatus::CompilingRust, desc.status);
    }
}
