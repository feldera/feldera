use crate::{ManagerConfig, ProjectStatus};
use anyhow::{anyhow, Error as AnyError, Result as AnyResult};
use log::error;
use serde::Serialize;
use std::{error::Error as StdError, fmt, fmt::Display, time::SystemTime};
use tokio_postgres::{Client, NoTls};

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
    dbclient: Client,
}

pub(crate) type ProjectId = i64;
pub(crate) type ConfigId = i64;
pub(crate) type PipelineId = i64;
pub(crate) type Version = i64;

#[derive(Debug)]
pub(crate) enum DBError {
    UnknownProject(ProjectId),
    OutdatedProjectVersion(Version),
    UnknownConfig(ConfigId),
    UnknownPipeline(PipelineId),
}

impl Display for DBError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DBError::UnknownProject(project_id) => write!(f, "Unknown project id '{project_id}'"),
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
///
/// This is a Rust representation of a record from the `project` table, sans
/// the potentially large `code` field, which must be retrieved separately.
#[derive(Serialize)]
pub(crate) struct ProjectDescr {
    /// Unique project id.
    pub project_id: ProjectId,
    /// Project name (doesn't have to be unique).
    pub name: String,
    /// Project version, incremented every time project code is modified.
    pub version: Version,
    /// Project compilation status.
    pub status: ProjectStatus,
}

/// Project config descriptor.
///
/// This is a Rust representation of a record from the `project_config` table.
#[derive(Serialize)]
pub(crate) struct ConfigDescr {
    pub config_id: ConfigId,
    pub project_id: ProjectId,
    pub version: Version,
    pub name: String,
    pub config: String,
}

/// Pipeline descriptor.
///
/// This is a Rust represenation of a record from the `pipeline` table.
#[derive(Serialize)]
pub(crate) struct PipelineDescr {
    pub pipeline_id: ConfigId,
    pub project_id: ProjectId,
    pub project_version: Version,
    pub port: u16,
    pub killed: bool,
    pub created: SystemTime,
}

impl ProjectDB {
    /// Connect to the project database.
    ///
    /// `config.pg_connection_string` must specify location of a project
    /// database created with `create_db.sql` along with access credentials.
    pub(crate) async fn connect(config: &ManagerConfig) -> AnyResult<Self> {
        let (dbclient, connection) =
            tokio_postgres::connect(&config.pg_connection_string, NoTls).await?;

        // The `tokio_postgres` API requires allocating a thread to `connection`,
        // which will handle datbase I/O and should automatically terminate once
        // the `dbclient` is dropped.
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("database connection error: {}", e);
            }
        });

        Ok(Self { dbclient })
    }

    /// Reset all project statuses to `ProjectStatus::None` after server
    /// restart.
    pub(crate) async fn reset_project_status(&self) -> AnyResult<()> {
        self.dbclient
            .execute("UPDATE project SET status = NULL, error = NULL;", &[])
            .await?;

        Ok(())
    }

    /// Retrieve project list from the DB.
    pub(crate) async fn list_projects(&self) -> AnyResult<Vec<ProjectDescr>> {
        let rows = self
            .dbclient
            .query("SELECT id, name, version, status, error FROM project", &[])
            .await?;
        let mut result = Vec::with_capacity(rows.len());

        for row in rows.into_iter() {
            let status: Option<&str> = row.try_get(3)?;
            let error: Option<String> = row.try_get(4)?;

            let status = ProjectStatus::from_columns(status, error)?;

            result.push(ProjectDescr {
                project_id: row.try_get(0)?,
                name: row.try_get(1)?,
                version: row.try_get(2)?,
                status,
            });
        }

        Ok(result)
    }

    /// Retrieve code of the specified project.
    pub(crate) async fn project_code(&self, project_id: ProjectId) -> AnyResult<(Version, String)> {
        let row = self
            .dbclient
            .query_opt(
                "SELECT version, code FROM project WHERE id = $1",
                &[&project_id],
            )
            .await?
            .ok_or(DBError::UnknownProject(project_id))?;

        Ok((row.try_get(0)?, row.try_get(1)?))
    }

    /// Create a new project.
    pub(crate) async fn new_project(
        &self,
        project_name: &str,
        project_code: &str,
    ) -> AnyResult<(ProjectId, Version)> {
        let row = self
            .dbclient
            .query_one("SELECT nextval('project_id_seq')", &[])
            .await?;
        let id: ProjectId = row.try_get(0)?;

        self.dbclient
            .execute(
                "INSERT INTO project (id, version, name, code, status_since) VALUES($1, 1, $2, $3, now())",
                &[&id, &project_name, &project_code],
            )
            .await?;

        Ok((id, 1))
    }

    /// Update project name and, optionally, code.
    pub(crate) async fn update_project(
        &mut self,
        project_id: ProjectId,
        project_name: &str,
        project_code: &Option<String>,
    ) -> AnyResult<Version> {
        let res = self
            .dbclient
            .query_opt(
                "SELECT version, code FROM project where id = $1",
                &[&project_id],
            )
            .await?
            .ok_or(DBError::UnknownProject(project_id))?;

        let mut version: Version = res.try_get(0)?;
        let old_code: String = res.try_get(1)?;

        match project_code {
            Some(code) if &old_code != code => {
                // Only increment `version` if new code actually differs from the
                // current version.
                version += 1;
                self.dbclient
                    .execute(
                        "UPDATE project SET version = $1, name = $2, code = $3, status = NULL, error = NULL WHERE id = $4",
                        &[&version, &project_name, code, &project_id],
                    )
                    .await?;
            }
            _ => {
                self.dbclient
                    .execute(
                        "UPDATE project SET name = $1 WHERE id = $2",
                        &[&project_name, &project_id],
                    )
                    .await?;
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
        let row = self
            .dbclient
            .query_opt(
                "SELECT name, version, status, error FROM project WHERE id = $1",
                &[&project_id],
            )
            .await?;

        if let Some(row) = row {
            let name: String = row.try_get(0)?;
            let version: Version = row.try_get(1)?;
            let status: Option<&str> = row.try_get(2)?;
            let error: Option<String> = row.try_get(3)?;

            let status = ProjectStatus::from_columns(status, error)?;

            Ok(Some(ProjectDescr {
                project_id,
                name,
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
    /// Note: doesn't check that the project exists.
    async fn set_project_status(
        &self,
        project_id: ProjectId,
        status: ProjectStatus,
    ) -> AnyResult<()> {
        let (status, error) = status.to_columns();

        self.dbclient
            .execute(
                "UPDATE project SET status = $1, error = $2, status_since = now() WHERE id = $3",
                &[&status, &error, &project_id],
            )
            .await?;

        Ok(())
    }

    /// Update project status after a version check.
    ///
    /// Updates project status to `status` if the current project version
    /// in the database matches `expected_version`.
    pub(crate) async fn set_project_status_guarded(
        &mut self,
        project_id: ProjectId,
        expected_version: Version,
        status: ProjectStatus,
    ) -> AnyResult<()> {
        let (status, error) = status.to_columns();

        let _descr = self
            .get_project_guarded(project_id, expected_version)
            .await?;

        self.dbclient
            .execute(
                "UPDATE project SET status = $1, error = $2, status_since = now() WHERE id = $3",
                &[&status, &error, &project_id],
            )
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
        if descr.status == ProjectStatus::Pending || descr.status == ProjectStatus::Compiling {
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

        if descr.status != ProjectStatus::Pending || descr.status != ProjectStatus::Compiling {
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
        let num_deleted = self
            .dbclient
            .execute("DELETE FROM project WHERE id = $1", &[&project_id])
            .await?;

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
    pub(crate) async fn next_job(&self) -> AnyResult<Option<(ProjectId, Version)>> {
        // Find the oldest pending project.
        let rows = self
            .dbclient
            .query("SELECT id, version FROM project WHERE status = 'pending' AND status_since = (SELECT min(status_since) FROM project WHERE status = 'pending')", &[])
            .await?;

        if rows.is_empty() {
            return Ok(None);
        }

        let project_id: ProjectId = rows[0].try_get(0)?;
        let version: Version = rows[0].try_get(1)?;

        Ok(Some((project_id, version)))
    }

    /// List configs associated with `project_id`.
    pub(crate) async fn list_project_configs(
        &self,
        project_id: ProjectId,
    ) -> AnyResult<Vec<ConfigDescr>> {
        // Check that the project exists, so we return an error instead of an
        // empty list of configs.
        let _descr = self.get_project(project_id).await?;

        let rows = self
            .dbclient
            .query(
                "SELECT id, version, name, config FROM project_config WHERE project_id = $1",
                &[&project_id],
            )
            .await?;
        let mut result = Vec::with_capacity(rows.len());

        for row in rows.into_iter() {
            result.push(ConfigDescr {
                config_id: row.try_get(0)?,
                project_id,
                version: row.try_get(1)?,
                name: row.try_get(2)?,
                config: row.try_get(3)?,
            });
        }

        Ok(result)
    }

    /// Retrieve project config.
    pub(crate) async fn get_config(&self, config_id: ConfigId) -> AnyResult<ConfigDescr> {
        let res = self
            .dbclient
            .query_opt(
                "SELECT project_id, version, name, config FROM project_config WHERE id = $1",
                &[&config_id],
            )
            .await?;

        match res {
            None => Err(anyhow!(DBError::UnknownConfig(config_id))),
            Some(row) => Ok(ConfigDescr {
                config_id,
                project_id: row.try_get(0)?,
                version: row.try_get(1)?,
                name: row.try_get(2)?,
                config: row.try_get(3)?,
            }),
        }
    }

    /// Create a new project config.
    pub(crate) async fn new_config(
        &self,
        project_id: ProjectId,
        config_name: &str,
        config: &str,
    ) -> AnyResult<(ConfigId, Version)> {
        // Check that the project exists, so we return correct error status
        // instead of Internal Server Error due to the next query failing.
        let _descr = self.get_project(project_id).await?;

        let row = self
            .dbclient
            .query_one("SELECT nextval('project_config_id_seq')", &[])
            .await?;
        let id: ConfigId = row.try_get(0)?;

        self.dbclient
            .execute(
                "INSERT INTO project_config (id, project_id, version, name, config) VALUES($1, $2, 1, $3, $4)",
                &[&id, &project_id, &config_name, &config],
            )
            .await?;

        Ok((id, 1))
    }

    /// Update existing project config.
    ///
    /// Update config name and, optionally, YAML.
    pub(crate) async fn update_config(
        &mut self,
        config_id: ConfigId,
        config_name: &str,
        config: &Option<String>,
    ) -> AnyResult<Version> {
        let descr = self.get_config(config_id).await?;

        let config = config.clone().unwrap_or(descr.config);

        let version = descr.version + 1;
        self.dbclient
            .execute(
                "UPDATE project_config SET version = $1, name = $2, config = $3 WHERE id = $4",
                &[&version, &config_name, &config, &config_id],
            )
            .await?;

        Ok(version)
    }

    /// Delete project config.
    pub(crate) async fn delete_config(&self, config_id: ConfigId) -> AnyResult<()> {
        let num_deleted = self
            .dbclient
            .execute("DELETE FROM project_config WHERE id = $1", &[&config_id])
            .await?;

        if num_deleted > 0 {
            Ok(())
        } else {
            Err(anyhow!(DBError::UnknownConfig(config_id)))
        }
    }

    /// Allocate a unique pipeline id.
    pub(crate) async fn alloc_pipeline_id(&self) -> AnyResult<PipelineId> {
        let row = self
            .dbclient
            .query_one("SELECT nextval('pipeline_id_seq')", &[])
            .await?;
        let id: PipelineId = row.try_get(0)?;

        Ok(id)
    }

    /// Insert a new record to the `pipeline` table.
    pub(crate) async fn new_pipeline(
        &self,
        pipeline_id: PipelineId,
        project_id: ProjectId,
        project_version: Version,
        port: u16,
    ) -> AnyResult<()> {
        // Convert port to a SQL-compatible type (see `trait ToSql`).
        let port = port as i32;

        self.dbclient
            .execute(
                "INSERT INTO pipeline (id, project_id, project_version, port, killed, created) VALUES($1, $2, $3, $4, false, now())",
                &[&pipeline_id, &project_id, &project_version, &port],
            )
            .await?;

        Ok(())
    }

    /// Read pipeline status.
    ///
    /// Returns pipeline port number and `killed` flag.
    pub(crate) async fn pipeline_status(&self, pipeline_id: PipelineId) -> AnyResult<(u16, bool)> {
        let row = self
            .dbclient
            .query_opt(
                "SELECT port, killed FROM pipeline WHERE id = $1",
                &[&pipeline_id],
            )
            .await?;

        if let Some(row) = row {
            let port: i32 = row.try_get(0)?;
            let killed: bool = row.try_get(1)?;

            Ok((port as u16, killed))
        } else {
            Err(anyhow!(DBError::UnknownPipeline(pipeline_id)))
        }
    }

    /// Set `killed` flag to `true`.
    pub(crate) async fn set_pipeline_killed(&self, pipeline_id: PipelineId) -> AnyResult<bool> {
        let num_updated = self
            .dbclient
            .execute(
                "UPDATE pipeline SET killed=true WHERE id = $1",
                &[&pipeline_id],
            )
            .await?;

        Ok(num_updated > 0)
    }

    /// Delete `pipeline` from the DB.
    pub(crate) async fn delete_pipeline(&self, pipeline_id: PipelineId) -> AnyResult<bool> {
        let num_deleted = self
            .dbclient
            .execute("DELETE FROM pipeline WHERE id = $1", &[&pipeline_id])
            .await?;

        Ok(num_deleted > 0)
    }

    /// List pipelines associated with `project_id`.
    pub(crate) async fn list_project_pipelines(
        &self,
        project_id: ProjectId,
    ) -> AnyResult<Vec<PipelineDescr>> {
        // Check that the project exists, so we return an error instead of an
        // empty list of pipelines.
        let _descr = self.get_project(project_id).await?;

        let rows = self
            .dbclient
            .query(
                "SELECT id, project_version, port, killed, created FROM pipeline WHERE project_id = $1",
                &[&project_id],
            )
            .await?;
        let mut result = Vec::with_capacity(rows.len());

        for row in rows.into_iter() {
            result.push(PipelineDescr {
                pipeline_id: row.try_get(0)?,
                project_id,
                project_version: row.try_get(1)?,
                port: row.try_get::<_, i32>(2)? as u16,
                killed: row.try_get(3)?,
                created: row.try_get(4)?,
            });
        }

        Ok(result)
    }
}
