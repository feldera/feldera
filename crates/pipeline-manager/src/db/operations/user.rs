//! User identity and tenant-membership operations for RBAC.

use crate::db::error::DBError;
use crate::db::operations::tenant::get_or_create_tenant_id_created;
use crate::db::operations::utils::{
    maybe_tenant_id_foreign_key_constraint_err, maybe_user_id_foreign_key_constraint_err,
};
use crate::db::types::role::Role;
use crate::db::types::tenant::TenantId;
use crate::db::types::user::{TenantMember, UserId};
use deadpool_postgres::Transaction;
use std::str::FromStr;
use uuid::Uuid;

/// Get the persisted user for an OIDC `(provider, subject)`, creating it if
/// absent and refreshing the stored email. Returns its identifier.
pub async fn get_or_create_user(
    txn: &Transaction<'_>,
    new_id: Uuid,
    provider: &str,
    subject: &str,
    email: Option<&str>,
) -> Result<UserId, DBError> {
    let stmt = txn
        .prepare_cached(
            // `EXCLUDED` is PostgreSQL's name for the row this statement tried
            // to insert, so `EXCLUDED.email` is the email from this login.
            // COALESCE keeps a previously stored email when a later token omits
            // the claim (some IdPs drop email on refresh-derived access tokens),
            // rather than overwriting it with NULL.
            "INSERT INTO app_user (id, provider, subject, email) VALUES ($1, $2, $3, $4) \
             ON CONFLICT (provider, subject) DO UPDATE SET email = COALESCE(EXCLUDED.email, app_user.email) \
             RETURNING id",
        )
        .await?;
    let row = txn
        .query_one(&stmt, &[&new_id, &provider, &subject, &email])
        .await?;
    Ok(UserId(row.get(0)))
}

/// Pre-provision a tenant member by identity: create the user record if it does
/// not exist yet and set its role. Lets an admin grant access before the user's
/// first login (the grant is dormant until the identity authenticates through
/// the IdP into this tenant). Returns the user id.
pub async fn preprovision_member(
    txn: &Transaction<'_>,
    new_user_id: Uuid,
    tenant_id: TenantId,
    provider: &str,
    subject: &str,
    email: Option<&str>,
    role: Role,
) -> Result<UserId, DBError> {
    let user_id = get_or_create_user(txn, new_user_id, provider, subject, email).await?;
    upsert_member_role(txn, tenant_id, user_id, role).await?;
    Ok(user_id)
}

/// Returns the user's role within a tenant, or `None` if not a member.
pub async fn get_member_role(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    user_id: UserId,
) -> Result<Option<Role>, DBError> {
    let stmt = txn
        .prepare_cached("SELECT role FROM tenant_membership WHERE tenant_id = $1 AND user_id = $2")
        .await?;
    let row = txn.query_opt(&stmt, &[&tenant_id.0, &user_id.0]).await?;
    match row {
        Some(row) => Ok(Some(Role::from_str(&row.get::<_, String>(0))?)),
        None => Ok(None),
    }
}

/// Inserts or updates a membership row. The role must be `<= admin`
/// (`owner` is never stored); the caller enforces the cap.
pub async fn upsert_member_role(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    user_id: UserId,
    role: Role,
) -> Result<(), DBError> {
    let stmt = txn
        .prepare_cached(
            // On conflict the membership already exists, so overwrite its role
            // with `EXCLUDED.role`, PostgreSQL's name for the value this
            // statement tried to insert. Insert and update in one statement.
            "INSERT INTO tenant_membership (tenant_id, user_id, role) VALUES ($1, $2, $3) \
             ON CONFLICT (tenant_id, user_id) DO UPDATE SET role = EXCLUDED.role",
        )
        .await?;
    txn.execute(&stmt, &[&tenant_id.0, &user_id.0, &role.as_str()])
        .await
        .map_err(DBError::from)
        .map_err(|e| maybe_tenant_id_foreign_key_constraint_err(e, tenant_id))
        .map_err(|e| maybe_user_id_foreign_key_constraint_err(e, user_id))?;
    Ok(())
}

/// Removes a user from a tenant.
pub async fn remove_member(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    user_id: UserId,
) -> Result<(), DBError> {
    let stmt = txn
        .prepare_cached("DELETE FROM tenant_membership WHERE tenant_id = $1 AND user_id = $2")
        .await?;
    let res = txn.execute(&stmt, &[&tenant_id.0, &user_id.0]).await?;
    if res > 0 {
        Ok(())
    } else {
        Err(DBError::UnknownUser {
            user_id: user_id.to_string(),
        })
    }
}

/// Lists the members of a tenant joined with their identity, for the admin UI.
pub async fn list_tenant_members(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
) -> Result<Vec<TenantMember>, DBError> {
    let stmt = txn
        .prepare_cached(
            // `provider` breaks the tie: two identities can share a subject
            // across providers, and both may have no email, which would
            // otherwise leave the order for those rows unspecified.
            "SELECT u.id, u.provider, u.subject, u.email, m.role \
             FROM tenant_membership m JOIN app_user u ON u.id = m.user_id \
             WHERE m.tenant_id = $1 ORDER BY u.email, u.subject, u.provider",
        )
        .await?;
    let rows = txn.query(&stmt, &[&tenant_id.0]).await?;
    let mut result = Vec::with_capacity(rows.len());
    for row in rows {
        result.push(TenantMember {
            user_id: UserId(row.get(0)),
            provider: row.get(1),
            subject: row.get(2),
            email: row.get(3),
            role: Role::from_str(&row.get::<_, String>(4))?,
        });
    }
    Ok(result)
}

/// Atomic login resolution for a non-owner principal: resolve (or create) the
/// acting tenant, ensure the user record, and determine the role. The login
/// that first creates a tenant is granted `first_user_role` (`admin` by
/// default); an existing member keeps its stored role; any other principal is
/// admitted at `default_role` and a membership row is recorded so admins can
/// see and adjust it. Returns the acting tenant, the user, and the effective
/// role.
#[allow(clippy::too_many_arguments)]
pub async fn resolve_login(
    txn: &Transaction<'_>,
    new_tenant_id: Uuid,
    new_user_id: Uuid,
    tenant_name: String,
    provider: String,
    subject: String,
    email: Option<String>,
    default_role: Role,
    first_user_role: Role,
) -> Result<(TenantId, UserId, Role), DBError> {
    let (tenant_id, created) =
        get_or_create_tenant_id_created(txn, new_tenant_id, tenant_name, provider.clone()).await?;
    let user_id =
        get_or_create_user(txn, new_user_id, &provider, &subject, email.as_deref()).await?;

    let role = match get_member_role(txn, tenant_id, user_id).await? {
        Some(role) => role,
        None => {
            let role = if created {
                first_user_role
            } else {
                default_role
            };
            upsert_member_role(txn, tenant_id, user_id, role).await?;
            role
        }
    };
    Ok((tenant_id, user_id, role))
}
