use crate::db::error::DBError;
use crate::db::types::tenant::TenantId;
use tokio_postgres::error::Error as PgError;

/// Converts the Postgres error into our `DBError`.
/// If the underlying Postgres error is a unique constraint violation,
/// it is already mapped to the corresponding specific `DBError`.
pub(crate) fn maybe_unique_violation(err: PgError) -> DBError {
    if let Some(db_err) = err.as_db_error() {
        if db_err.code() == &tokio_postgres::error::SqlState::UNIQUE_VIOLATION {
            // Unique constraints
            match db_err.constraint() {
                Some("pipeline_pkey") => DBError::unique_key_violation("pipeline_pkey"),
                Some("api_key_pkey") => DBError::unique_key_violation("api_key_pkey"),
                Some("cluster_monitor_event_pkey") => {
                    DBError::unique_key_violation("cluster_monitor_event_pkey")
                }
                Some("unique_hash") => DBError::duplicate_key(),
                Some(_constraint) => DBError::DuplicateName,
                None => DBError::DuplicateName,
            }
        } else {
            // Other database errors
            DBError::from(err)
        }
    } else {
        // It was a Postgres error which was not directly related to the database.
        // For example: network, parsing, serialization, TLS, I/O, timeouts etc.
        DBError::from(err)
    }
}

/// Checks whether the database error is because a foreign key constraint tenant_id to
/// the tenant table was not met. If this is the case, convert the error in a specialized
/// one which communicates clearly that the referenced tenant does not exist.
pub(crate) fn maybe_tenant_id_foreign_key_constraint_err(
    err: DBError,
    tenant_id: TenantId,
) -> DBError {
    if let DBError::PostgresError { error, .. } = &err {
        let db_err = error.as_db_error();
        if let Some(db_err) = db_err {
            if db_err.code() == &tokio_postgres::error::SqlState::FOREIGN_KEY_VIOLATION {
                if let Some(constraint_name) = db_err.constraint() {
                    if constraint_name.ends_with("tenant_id_fkey") {
                        return DBError::UnknownTenant { tenant_id };
                    }
                }
            }
        }
    }
    err
}
