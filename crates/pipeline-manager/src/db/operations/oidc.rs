use crate::db::error::DBError;
use crate::db::operations::utils::maybe_unique_violation;
use crate::db::types::oidc::{Provider, Tenant, User, UserId};
use crate::db::types::tenant::TenantId;
use deadpool_postgres::Transaction;
use tokio_postgres::Row;
use uuid::Uuid;

/// Validates that the provided issuer follows basic format rules.
/// Specifically, it cannot be empty.
// TODO: additional validation, e.g. that it must start with `https://`, though that would
//       interfere with local testing
fn validate_issuer(issuer: &str) -> Result<(), DBError> {
    if issuer.is_empty() {
        return Err(DBError::InvalidIssuer {
            error: "cannot be empty".to_string(),
        });
    }
    Ok(())
}

/// Validates that the provided subject follows basic format rules (cannot be empty) and passes
/// the provided filter.
// TODO: more precise subject filter formatting
fn validate_subject(subject: &str, subject_filter: &str) -> Result<(), DBError> {
    if subject.is_empty() {
        return Err(DBError::InvalidSubject {
            error: "cannot be empty".to_string(),
        });
    }
    // Only two filters are supported:
    // - '*': any subject matches
    // - Else, it does exact matching
    if subject_filter == "*" || subject == subject_filter {
        Ok(())
    } else {
        Err(DBError::InvalidSubject {
            error: format!("subject '{subject}' does not match filter '{subject_filter}'"),
        })
    }
}

/// Validates that the provided subject filter follows basic format rules.
/// - It cannot be empty
// TODO: additional filter rules, like the present of wildcard asterisks
fn validate_subject_filter(subject_filter: &str) -> Result<(), DBError> {
    // TODO: check presence of wildcard asterisks
    if subject_filter.is_empty() {
        return Err(DBError::InvalidSubjectFilter {
            error: "cannot be empty".to_string(),
        });
    }
    Ok(())
}

/// Validates that the provided client identifier follows basic format rules.
/// Specifically, that it cannot be empty.
fn validate_client_id(client_id: &str) -> Result<(), DBError> {
    if client_id.is_empty() {
        return Err(DBError::InvalidClientId {
            error: "cannot be empty".to_string(),
        });
    }
    Ok(())
}

/// Validates that the provided tenant name follows basic format rules.
/// Specifically, that it cannot be empty.
// TODO: length restriction?
// TODO: if it is user dedicated, the name must match `user-dedicated-<user UUID>`.
//       if it is not, it must not match that.
fn validate_tenant_name(name: &str, is_user_dedicated: bool) -> Result<(), DBError> {
    if name.is_empty() {
        return Err(DBError::InvalidTenantName {
            error: "cannot be empty".to_string(),
        });
    }
    Ok(())
}

/// Parses the provided row from the `oidc_provider` table into a [`Provider`].
pub fn parse_provider_row(row: &Row) -> Result<Provider, DBError> {
    Ok(Provider {
        issuer: row.get("issuer"),
        subject_filter: row.get("subject_filter"),
        client_id: row.get("client_id"),
    })
}

/// Lists all OIDC providers.
pub(crate) async fn list_providers(txn: &Transaction<'_>) -> Result<Vec<Provider>, DBError> {
    let stmt = txn
        .prepare_cached(
            "SELECT issuer, subject_filter, client_id
             FROM oidc_provider
             ORDER BY issuer",
        )
        .await?;
    let rows: Vec<Row> = txn.query(&stmt, &[]).await?;
    let mut result = Vec::with_capacity(rows.len());
    for row in rows {
        result.push(parse_provider_row(&row)?);
    }
    Ok(result)
}

/// Creates a new OIDC provider.
pub(crate) async fn new_provider(
    txn: &Transaction<'_>,
    issuer: &str,
    subject_filter: &str,
    client_id: &str,
) -> Result<(), DBError> {
    // Field validation
    validate_issuer(issuer)?;
    validate_subject_filter(subject_filter)?;
    validate_client_id(client_id)?;

    // Query
    let stmt = txn
        .prepare_cached(
            "INSERT INTO oidc_provider (issuer, subject_filter, client_id)
            VALUES ($1, $2, $3)",
        )
        .await?;
    txn.execute(
        &stmt,
        &[
            &issuer,         // $1: issuer
            &subject_filter, // $2: subject_filter
            &client_id,      // $3: client_id
        ],
    )
    .await
    .map_err(maybe_unique_violation)?; // TODO: additional constraint violations
    Ok(())
}

/// Updates an existing OIDC provider.
pub(crate) async fn update_provider(
    txn: &Transaction<'_>,
    issuer: &str,
    new_subject_filter: Option<String>,
    new_client_id: Option<String>,
) -> Result<(), DBError> {
    // Field validation
    validate_issuer(issuer)?;
    if let Some(new_subject_filter) = new_subject_filter.as_ref() {
        validate_subject_filter(new_subject_filter)?;
    }
    if let Some(new_client_id) = new_client_id.as_ref() {
        validate_client_id(new_client_id)?;
    }

    // Query
    let stmt = txn
        .prepare_cached(
            "UPDATE oidc_provider
                     SET subject_filter = COALESCE($1, subject_filter),
                         client_id = COALESCE($2, client_id),
                    WHERE issuer = $3",
        )
        .await?;
    txn.execute(
        &stmt,
        &[
            &new_subject_filter, // $1: subject_filter
            &new_client_id,      // $2: client_id
            &issuer,             // $3: issuer
        ],
    )
    .await
    .map_err(maybe_unique_violation)?; // TODO: additional constraint violations
    Ok(())
}

/// Deletes an existing OIDC provider.
pub(crate) async fn delete_provider(txn: &Transaction<'_>, issuer: &str) -> Result<(), DBError> {
    let stmt = txn
        .prepare_cached("DELETE FROM oidc_provider WHERE issuer = $1")
        .await?;
    let res = txn.execute(&stmt, &[&issuer]).await?;
    if res > 0 {
        Ok(())
    } else {
        Err(DBError::UnknownProvider {
            issuer: issuer.to_string(),
        })
    }
}

/// Parses the provided row from the `oidc_user` table into a [`User`].
pub fn parse_user_row(row: &Row) -> Result<User, DBError> {
    Ok(User {
        id: UserId(row.get("id")),
        issuer: row.get("issuer"),
        subject: row.get("subject"),
    })
}

/// Lists all OIDC users, ordered by (issuer, subject).
pub(crate) async fn list_users(txn: &Transaction<'_>) -> Result<Vec<User>, DBError> {
    let stmt = txn
        .prepare_cached(
            "SELECT id, issuer, subject
             FROM oidc_user
             ORDER BY (issuer, subject)",
        )
        .await?;
    let rows: Vec<Row> = txn.query(&stmt, &[]).await?;
    let mut result = Vec::with_capacity(rows.len());
    for row in rows {
        result.push(parse_user_row(&row)?);
    }
    Ok(result)
}

/// Creates a new OIDC user if it does not yet exist. This is called when it has passed
/// authentication and is in search for the user identifier that can be used to retrieve
/// its tenants.
pub(crate) async fn new_user_if_not_exists(
    txn: &Transaction<'_>,
    issuer: &str,
    subject_filter: &str, // TODO: will this be provided, or do we need to separately fetch from `oidc_provider` table?
    subject: &str,
    new_user_id: Uuid,
    new_tenant_id: Uuid,
) -> Result<User, DBError> {
    // Field validation
    validate_issuer(issuer)?;
    validate_subject(subject, subject_filter)?;

    // Check if user already exists
    let stmt = txn
        .prepare_cached(
            "SELECT id, issuer, subject
             FROM oidc_user
             WHERE issuer = $1 AND subject = $2",
        )
        .await?;
    if let Some(row) = txn.query_opt(&stmt, &[&issuer, &subject]).await? {
        return parse_user_row(&row);
    }

    // If it does not, insert it
    let stmt = txn
        .prepare_cached(
            "INSERT INTO oidc_user (id, issuer, subject)
            VALUES ($1, $2, $3)",
        )
        .await?;
    txn.execute(
        &stmt,
        &[
            &new_user_id, // $1: id
            &issuer,      // $2: issuer
            &subject,     // $3: subject
        ],
    )
    .await
    .map_err(maybe_unique_violation)?; // TODO: additional constraint violations

    // Create a dedicated tenant for the user
    new_tenant(
        txn,
        &format!("user-dedicated-{new_user_id}"),
        new_tenant_id,
        true,
    )
    .await?;

    // Map the dedicated tenant to the user
    let stmt = txn
        .prepare_cached(
            "INSERT INTO user_tenant (user_id, tenant_id, role)
            VALUES ($1, $2, 'owner')",
        )
        .await?;
    txn.execute(
        &stmt,
        &[
            &new_user_id,   // $1: user_id
            &new_tenant_id, // $2: tenant_id
        ],
    )
    .await?;

    Ok(User {
        id: UserId(new_user_id),
        issuer: issuer.to_string(),
        subject: subject.to_string(),
    })
}

/// Deletes an existing OIDC user.
pub(crate) async fn delete_user(txn: &Transaction<'_>, user_id: UserId) -> Result<(), DBError> {
    let stmt = txn
        .prepare_cached("DELETE FROM oidc_user WHERE user_id = $1")
        .await?;
    let res = txn.execute(&stmt, &[&user_id.0]).await?;
    if res > 0 {
        Ok(())
    } else {
        Err(DBError::UnknownUser { id: user_id })
    }
}

// TODO: fn grant_user_tenant_access()
// TODO: fn revoke_user_tenant_access()

/// Parses the provided row from the `tenant` table into a [`Tenant`].
pub fn parse_tenant_row(row: &Row) -> Result<Tenant, DBError> {
    Ok(Tenant {
        id: TenantId(row.get("id")),
        name: row.get("name"),
    })
}

/// Lists all tenants.
pub(crate) async fn list_tenants(txn: &Transaction<'_>) -> Result<Vec<Tenant>, DBError> {
    let stmt = txn
        .prepare_cached(
            "SELECT id, name
             FROM tenant
             ORDER BY name",
        )
        .await?;
    let rows: Vec<Row> = txn.query(&stmt, &[]).await?;
    let mut result = Vec::with_capacity(rows.len());
    for row in rows {
        result.push(parse_tenant_row(&row)?);
    }
    Ok(result)
}

/// Creates a new tenant.
pub(crate) async fn new_tenant(
    txn: &Transaction<'_>,
    name: &str,
    new_id: Uuid,
    is_user_dedicated: bool,
) -> Result<(), DBError> {
    // Field validation
    validate_tenant_name(name, is_user_dedicated)?;

    // Query
    let stmt = txn
        .prepare_cached(
            "INSERT INTO tenant (id, name)
            VALUES ($1, $2)",
        )
        .await?;
    txn.execute(
        &stmt,
        &[
            &new_id, // $1: id
            &name,   // $2: name
        ],
    )
    .await?;
    Ok(())
}

/// Updates an existing tenant.
pub(crate) async fn update_tenant(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    new_name: Option<String>,
) -> Result<(), DBError> {
    // Field validation
    // TODO: deny name update if it is a user-dedicated tenant

    // Query
    let stmt = txn
        .prepare_cached(
            "UPDATE tenant
                     SET name = COALESCE($1, name)
                    WHERE id = $2",
        )
        .await?;
    txn.execute(
        &stmt,
        &[
            &new_name,  // $1: name
            &tenant_id.0, // $2: tenant_id
        ],
    )
    .await
    .map_err(maybe_unique_violation)?; // TODO: additional constraint violations
    Ok(())
}

/// Deletes an existing tenant.
pub(crate) async fn delete_tenant(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
) -> Result<(), DBError> {
    // TODO: check that the tenant is not dedicated to a user
    let stmt = txn
        .prepare_cached("DELETE FROM tenant WHERE id = $1")
        .await?;
    let res = txn.execute(&stmt, &[&tenant_id.0]).await?;
    if res > 0 {
        Ok(())
    } else {
        Err(DBError::UnknownTenant { tenant_id })
    }
}

#[cfg(test)]
mod test {
    // TODO: unit test that validation works
}
