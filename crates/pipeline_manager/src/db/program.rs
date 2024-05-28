use std::{
    fmt::{self, Display},
    str::FromStr,
};

use deadpool_postgres::Transaction;
use log::{debug, error};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::{
    api::ProgramStatus, auth::TenantId, compiler::ProgramConfig, config::CompilationProfile,
};

use super::{DBError, ProjectDB, Version};
use serde::{Deserialize, Serialize};

#[cfg(test)]
use super::test::limited_uuid;

use pipeline_types::program_schema::ProgramSchema;

/// Unique program id.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Ord, PartialOrd, Serialize, Deserialize, ToSchema)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[repr(transparent)]
#[serde(transparent)]
pub struct ProgramId(#[cfg_attr(test, proptest(strategy = "limited_uuid()"))] pub Uuid);
impl Display for ProgramId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// The database encodes program status using two columns: `status`, which has
/// type `string`, but acts as an enum, and `error`, only used if `status` is
/// one of `"sql_error"` or `"rust_error"`.
impl ProgramStatus {
    /// Decode `ProgramStatus` from the values of `error` and `status` columns.
    fn from_columns(status_string: &str, error_string: Option<String>) -> Result<Self, DBError> {
        match status_string {
            "success" => Ok(Self::Success),
            "pending" => Ok(Self::Pending),
            "compiling_sql" => Ok(Self::CompilingSql),
            "compiling_rust" => Ok(Self::CompilingRust),
            "sql_error" => {
                let error = error_string.unwrap_or_default();
                if let Ok(messages) = serde_json::from_str(&error) {
                    Ok(Self::SqlError(messages))
                } else {
                    error!("Expected valid json for SqlCompilerMessage but got {:?}, did you update the struct without adjusting the database?", error);
                    Ok(Self::SystemError(error))
                }
            }
            "rust_error" => Ok(Self::RustError(error_string.unwrap_or_default())),
            "system_error" => Ok(Self::SystemError(error_string.unwrap_or_default())),
            status => Err(DBError::invalid_status(status.to_string())),
        }
    }
    fn to_columns(&self) -> (Option<String>, Option<String>) {
        match self {
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
#[derive(Deserialize, Serialize, ToSchema, Debug, Eq, PartialEq, Clone)]
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
    /// ```ignore
    /// CREATE TABLE USERS ( name varchar );
    /// CREATE VIEW OUTPUT_USERS as SELECT * FROM USERS;
    /// ```
    ///
    /// Would lead the following JSON string in `schema`:
    ///
    /// ```ignore
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
    pub schema: Option<ProgramSchema>,

    /// SQL code
    pub code: Option<String>,

    /// Program configuration
    pub config: ProgramConfig,
}

pub(crate) async fn list_programs(
    db: &ProjectDB,
    tenant_id: TenantId,
    with_code: bool,
) -> Result<Vec<ProgramDescr>, DBError> {
    let manager = db.pool.get().await?;
    let stmt = manager
        .prepare_cached(
            r#"SELECT id, name, description, version, status, error, schema,
                CASE WHEN $2 IS TRUE THEN code ELSE null END,
                compilation_profile
                FROM program WHERE tenant_id = $1"#,
        )
        .await?;
    let rows = manager.query(&stmt, &[&tenant_id.0, &with_code]).await?;

    let mut result = Vec::with_capacity(rows.len());
    for row in rows {
        let status: String = row.get(4);
        let error: Option<String> = row.get(5);
        let status = ProgramStatus::from_columns(&status, error)?;
        let schema: Option<ProgramSchema> = row
            .get::<_, Option<String>>(6)
            .map(|s| serde_json::from_str(&s))
            .transpose()
            .map_err(|e| DBError::invalid_data(format!("Error parsing program schema: {e}")))?;
        let profile_str = row.get::<_, Option<String>>(8);
        let profile = profile_str.map(|profile_str| {
            CompilationProfile::from_str(&profile_str).expect("Expected valid compilation profile")
        });

        result.push(ProgramDescr {
            program_id: ProgramId(row.get(0)),
            name: row.get(1),
            description: row.get(2),
            version: Version(row.get(3)),
            schema,
            status,
            code: row.get(7),
            config: ProgramConfig { profile },
        });
    }

    Ok(result)
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn new_program(
    db: &ProjectDB,
    tenant_id: TenantId,
    id: Uuid,
    program_name: &str,
    program_description: &str,
    program_code: &str,
    config: &ProgramConfig,
    txn: Option<&Transaction<'_>>,
) -> Result<(ProgramId, Version), DBError> {
    debug!("new_program {program_name} {program_description} {program_code}");
    let query = "INSERT INTO program (id, version, tenant_id, name, description, 
                                      code, schema, status, error, status_since, compilation_profile)
                 VALUES($1, 1, $2, $3, $4, $5, NULL, $6, $7, now(), $8);";
    let (status, error) = ProgramStatus::Pending.to_columns();
    let row = if let Some(txn) = txn {
        let stmt = txn.prepare_cached(query).await?;
        txn.execute(
            &stmt,
            &[
                &id,
                &tenant_id.0,
                &program_name,
                &program_description,
                &program_code,
                &status,
                &error,
                &config.profile.as_ref().map(|p| p.to_string()),
            ],
        )
        .await
    } else {
        let manager = db.pool.get().await?;
        let stmt = manager.prepare_cached(query).await?;
        manager
            .execute(
                &stmt,
                &[
                    &id,
                    &tenant_id.0,
                    &program_name,
                    &program_description,
                    &program_code,
                    &status,
                    &error,
                    &config.profile.as_ref().map(|p| p.to_string()),
                ],
            )
            .await
    };
    row.map_err(ProjectDB::maybe_unique_violation)
        .map_err(|e| ProjectDB::maybe_tenant_id_foreign_key_constraint_err(e, tenant_id, None))?;
    Ok((ProgramId(id), Version(1)))
}

/// Optionally update different fields of a program. This call
/// also accepts an optional version to do guarded updates to the code.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn update_program(
    tenant_id: TenantId,
    program_id: ProgramId,
    program_name: &Option<String>,
    program_description: &Option<String>,
    program_code: &Option<String>,
    status: &Option<ProgramStatus>,
    schema: &Option<ProgramSchema>,
    config: &Option<ProgramConfig>,
    guard: Option<Version>,
    txn: &Transaction<'_>,
) -> Result<Version, DBError> {
    // Only increment `version` if new code actually differs from the
    // current version. Only apply a change if the version matched the
    // guard.
    let get_version = txn
        .prepare_cached("SELECT version, code, compilation_profile FROM program WHERE tenant_id = $1 AND id = $2 ")
        .await?;
    let row = txn
        .query_opt(&get_version, &[&tenant_id.0, &program_id.0])
        .await
        .map_err(ProjectDB::maybe_unique_violation)?
        .ok_or(DBError::UnknownProgram { program_id })?;
    let latest_version = Version(row.get(0));
    let code: String = row.get(1);
    let profile_str = row.get::<_, Option<String>>(2);
    let profile = profile_str.map(|profile_str| {
        CompilationProfile::from_str(&profile_str).expect("Expected valid compilation profile")
    });

    if let Some(guard) = guard {
        if guard.0 != latest_version.0 {
            return Err(DBError::OutdatedProgramVersion { latest_version });
        }
    }
    // Reset status, error and schema if the code (i.e., the version) changes.
    let stmt = txn
            .prepare_cached(
                r#"UPDATE program
                        SET
                            name = COALESCE($3, name),
                            description = COALESCE($4, description),
                            code = COALESCE($5, code),
                            version = $6,
                            status = (CASE WHEN version = $6 THEN COALESCE($7, status) ELSE 'pending' END),
                            error = (CASE WHEN version = $6 THEN COALESCE($8, error) ELSE NULL END),
                            status_since = (CASE WHEN $10 THEN now() ELSE status_since END),
                            schema = (CASE WHEN $11 THEN NULL
                                           WHEN version = $6 THEN COALESCE($9, schema)
                                           ELSE NULL END),
                            compilation_profile = $12
                    WHERE tenant_id = $1 AND id = $2
                    RETURNING version
                "#,
            )
            .await?;
    let has_code_changed = program_code.as_ref().is_some_and(|c| *c != code);
    let has_profile_changed = config.as_ref().is_some_and(|c| c.profile != profile);

    // Changing the program compilation profile currently counts as a new version, which in turn
    // triggers recompilation from scratch (including the SQL compilation and resetting the
    // schema). This will likely cause redundant work, but is simpler and is not expected
    // to be the common mode of operation.
    let new_version = if has_code_changed || has_profile_changed {
        latest_version.0 + 1
    } else {
        latest_version.0
    };
    let new_profile = config
        .as_ref()
        .map(|c| c.profile.clone())
        .unwrap_or(profile);
    let status_change = !has_code_changed && (status.is_some() || schema.is_some());
    let reset_schema = status
        .as_ref()
        .is_some_and(|s| *s == ProgramStatus::Pending);
    let schema = if reset_schema {
        None
    } else if let Some(s) = schema {
        Some(serde_json::to_string(&s).map_err(|e| {
            DBError::invalid_data(format!(
                "Error serializing program schema '{schema:?}'.\nError: {e}"
            ))
        })?)
    } else {
        None
    };
    let status = status.as_ref().map(|s| s.to_columns()); // split into (status, error) Strings
    let row = txn
        .query_opt(
            &stmt,
            &[
                &tenant_id.0,
                &program_id.0,
                &program_name,
                &program_description,
                &program_code,
                &new_version,
                &status.as_ref().map(|s| s.0.clone()),
                &status.as_ref().map(|s| s.1.clone()),
                &schema,
                &status_change,
                &reset_schema,
                &new_profile.as_ref().map(|p| p.to_string()),
            ],
        )
        .await
        .map_err(ProjectDB::maybe_unique_violation)?;
    if let Some(row) = row {
        Ok(Version(row.get(0)))
    } else {
        Err(DBError::UnknownProgram { program_id })
    }
}

/// Retrieve program descriptor.
pub(crate) async fn get_program_by_id(
    db: &ProjectDB,
    tenant_id: TenantId,
    program_id: ProgramId,
    with_code: bool,
) -> Result<ProgramDescr, DBError> {
    let manager = db.pool.get().await?;
    let stmt = manager
        .prepare_cached(
            "SELECT name, description, version, status, error, schema,
                CASE WHEN $3 IS TRUE THEN code ELSE null END,
                compilation_profile
                FROM program WHERE id = $1 AND tenant_id = $2",
        )
        .await?;
    let row = manager
        .query_opt(&stmt, &[&program_id.0, &tenant_id.0, &with_code])
        .await?;

    if let Some(row) = row {
        let name: String = row.get(0);
        let description: String = row.get(1);
        let version: Version = Version(row.get(2));
        let status: String = row.get(3);
        let error: Option<String> = row.get(4);
        let schema: Option<ProgramSchema> = row
            .get::<_, Option<String>>(5)
            .map(|s| serde_json::from_str(&s))
            .transpose()
            .map_err(|e| DBError::invalid_data(format!("Error parsing program schema: {e}")))?;
        let code: Option<String> = row.get(6);
        let profile_str = row.get::<_, Option<String>>(7);
        let profile = profile_str.map(|profile_str| {
            CompilationProfile::from_str(&profile_str).expect("Expected valid compilation profile")
        });

        let status = ProgramStatus::from_columns(&status, error)?;
        Ok(ProgramDescr {
            program_id,
            name,
            description,
            version,
            status,
            schema,
            code,
            config: ProgramConfig { profile },
        })
    } else {
        Err(DBError::UnknownProgram { program_id })
    }
}

/// Lookup program by name.
pub(crate) async fn get_program_by_name(
    db: &ProjectDB,
    tenant_id: TenantId,
    program_name: &str,
    with_code: bool,
    txn: Option<&Transaction<'_>>,
) -> Result<ProgramDescr, DBError> {
    let query = "SELECT id, description, version, status, error, schema, tenant_id,
                 CASE WHEN $3 IS TRUE THEN code ELSE null END,
                 compilation_profile
                 FROM program WHERE name = $1 AND tenant_id = $2";
    let row = if let Some(txn) = txn {
        let stmt = txn.prepare_cached(query).await?;
        txn.query_opt(&stmt, &[&program_name, &tenant_id.0, &with_code])
            .await?
    } else {
        let manager = db.pool.get().await?;
        let stmt = manager.prepare_cached(query).await?;
        manager
            .query_opt(&stmt, &[&program_name, &tenant_id.0, &with_code])
            .await?
    };

    if let Some(row) = row {
        let program_id: ProgramId = ProgramId(row.get(0));
        let description: String = row.get(1);
        let version: Version = Version(row.get(2));
        let status: String = row.get(3);
        let error: Option<String> = row.get(4);
        let schema: Option<ProgramSchema> = row
            .get::<_, Option<String>>(5)
            .map(|s| serde_json::from_str(&s))
            .transpose()
            .map_err(|e| DBError::invalid_data(format!("Error parsing program schema: {e}")))?;
        let code: Option<String> = row.get(7);
        let profile_str = row.get::<_, Option<String>>(8);
        let profile = profile_str.map(|profile_str| {
            CompilationProfile::from_str(&profile_str).expect("Expected valid compilation profile")
        });
        let status = ProgramStatus::from_columns(&status, error)?;
        Ok(ProgramDescr {
            program_id,
            name: program_name.to_string(),
            description,
            version,
            status,
            schema,
            code,
            config: ProgramConfig { profile },
        })
    } else {
        Err(DBError::UnknownProgramName {
            program_name: program_name.to_string(),
        })
    }
}

pub(crate) async fn delete_program(
    db: &ProjectDB,
    tenant_id: TenantId,
    program_name: &str,
) -> Result<(), DBError> {
    let manager = db.pool.get().await?;
    let stmt = manager
        .prepare_cached("DELETE FROM program WHERE name = $1 AND tenant_id = $2")
        .await?;
    let res = manager
        .execute(&stmt, &[&program_name, &tenant_id.0])
        .await
        .map_err(|e| {
            ProjectDB::maybe_program_id_in_use_foreign_key_constraint_err(
                e.into(),
                Some(program_name),
            )
        })?;
    if res > 0 {
        Ok(())
    } else {
        Err(DBError::UnknownProgramName {
            program_name: program_name.to_string(),
        })
    }
}

pub(crate) async fn all_programs(db: &ProjectDB) -> Result<Vec<(TenantId, ProgramDescr)>, DBError> {
    let manager = db.pool.get().await?;
    let stmt = manager
        .prepare_cached(
            r#"SELECT id, name, description, version, status, error, schema, tenant_id, compilation_profile
                   FROM program"#,
        )
        .await?;
    let rows = manager.query(&stmt, &[]).await?;

    let mut result = Vec::with_capacity(rows.len());
    for row in rows {
        let status: String = row.get(4);
        let error: Option<String> = row.get(5);
        let status = ProgramStatus::from_columns(&status, error)?;
        let schema: Option<ProgramSchema> = row
            .get::<_, Option<String>>(6)
            .map(|s| serde_json::from_str(&s))
            .transpose()
            .map_err(|e| DBError::invalid_data(format!("Error parsing program schema: {e}")))?;
        let tenant_id = TenantId(row.get(7));
        let profile_str = row.get::<_, Option<String>>(8);
        let profile = profile_str.map(|profile_str| {
            CompilationProfile::from_str(&profile_str).expect("Expected valid compilation profile")
        });
        result.push((
            tenant_id,
            ProgramDescr {
                program_id: ProgramId(row.get(0)),
                name: row.get(1),
                description: row.get(2),
                version: Version(row.get(3)),
                schema,
                status,
                code: None,
                config: ProgramConfig { profile },
            },
        ));
    }
    Ok(result)
}
