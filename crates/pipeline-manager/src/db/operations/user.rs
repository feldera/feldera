//! User identity and tenant-membership operations for RBAC.

use crate::db::error::DBError;
use crate::db::operations::tenant::get_or_create_tenant_id_created;
use crate::db::operations::utils::{
    maybe_tenant_id_foreign_key_constraint_err, maybe_user_id_foreign_key_constraint_err,
};
use crate::db::types::role::Role;
use crate::db::types::tenant::TenantId;
use crate::db::types::user::{MembershipOrigin, TenantMember, UserId, UserMembership, UserProfile};
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
            //
            // An address the provider vouched for outranks both callers here: a
            // token claim, and the email an administrator types into
            // `preprovision_member`. Neither carries verification, so letting
            // either overwrite would strip a verified address of its mark or,
            // worse, leave the mark next to a different address.
            "INSERT INTO app_user (id, provider, subject, email) VALUES ($1, $2, $3, $4) \
             ON CONFLICT (provider, subject) DO UPDATE SET \
             email = CASE WHEN app_user.email_verified THEN app_user.email \
                          ELSE COALESCE(EXCLUDED.email, app_user.email) END \
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
/// first login; the membership authorizes on its own as soon as that identity
/// authenticates through the IdP. Returns the user id.
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
    upsert_member_role(txn, tenant_id, user_id, role, MembershipOrigin::Api).await?;
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
/// (`owner` is never stored); the caller enforces the cap. `origin` and the
/// creation timestamp record provenance and only apply to a fresh row: a
/// conflict updates the role and keeps how and when the membership was first
/// created.
pub async fn upsert_member_role(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    user_id: UserId,
    role: Role,
    origin: MembershipOrigin,
) -> Result<(), DBError> {
    let stmt = txn
        .prepare_cached(
            // On conflict the membership already exists, so overwrite its role
            // with `EXCLUDED.role`, PostgreSQL's name for the value this
            // statement tried to insert. Insert and update in one statement.
            "INSERT INTO tenant_membership (tenant_id, user_id, role, created_at, origin) \
             VALUES ($1, $2, $3, now(), $4) \
             ON CONFLICT (tenant_id, user_id) DO UPDATE SET role = EXCLUDED.role",
        )
        .await?;
    txn.execute(
        &stmt,
        &[&tenant_id.0, &user_id.0, &role.as_str(), &origin.as_str()],
    )
    .await
    .map_err(DBError::from)
    .map_err(|e| maybe_tenant_id_foreign_key_constraint_err(e, tenant_id))
    .map_err(|e| maybe_user_id_foreign_key_constraint_err(e, user_id))?;
    Ok(())
}

/// Removes a user from a tenant. The membership is keyed by both ids, so a
/// caller holding one tenant can never delete a membership in another: the
/// tenant here is the caller's acting tenant, fixed when the request was
/// authenticated, not something the request body carries.
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

/// Lists the members of a tenant, each joined with the identity it belongs to.
pub async fn list_tenant_members(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
) -> Result<Vec<TenantMember>, DBError> {
    let stmt = txn
        .prepare_cached(
            // `provider` breaks the tie: two identities can share a subject
            // across providers, and both may have no email, which would
            // otherwise leave the order for those rows unspecified.
            "SELECT u.id, u.provider, u.subject, u.email, u.email_verified, u.display_name, \
             m.role, m.origin \
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
            email_verified: row.get(4),
            display_name: row.get(5),
            role: Role::from_str(&row.get::<_, String>(6))?,
            origin: row
                .get::<_, Option<&str>>(7)
                .map(MembershipOrigin::from_str)
                .transpose()?,
        });
    }
    Ok(result)
}

/// Claim the right to refresh this identity's profile from the provider, and
/// report whether this caller won it. Creates the identity's row when the user
/// has none yet, which is the case for a platform owner: an owner's role comes
/// from configuration, so nothing else on the login path records one.
///
/// A refresh is due when none has run, when the last one has aged past
/// `ttl_seconds`, or when `auth_time` says the user authenticated more recently
/// than the last one covered. Deciding and recording in one statement is what
/// makes the claim exclusive: PostgreSQL locks the row for the update, so
/// exactly one caller sees a row returned however many api-servers ask at once.
pub async fn claim_profile_refresh(
    txn: &Transaction<'_>,
    new_id: Uuid,
    provider: &str,
    subject: &str,
    auth_time: Option<i64>,
    ttl_seconds: i64,
) -> Result<bool, DBError> {
    let stmt = txn
        .prepare_cached(
            "INSERT INTO app_user (id, provider, subject, profile_refreshed_at, profile_auth_time) \
             VALUES ($1, $2, $3, now(), $4) \
             ON CONFLICT (provider, subject) DO UPDATE \
             SET profile_refreshed_at = now(), profile_auth_time = EXCLUDED.profile_auth_time \
             WHERE app_user.profile_refreshed_at IS NULL \
                OR app_user.profile_refreshed_at < now() - ($5::bigint * INTERVAL '1 second') \
                OR (EXCLUDED.profile_auth_time IS NOT NULL \
                    AND (app_user.profile_auth_time IS NULL \
                         OR EXCLUDED.profile_auth_time > app_user.profile_auth_time)) \
             RETURNING id",
        )
        .await?;
    let claimed = txn
        .query_opt(
            &stmt,
            &[&new_id, &provider, &subject, &auth_time, &ttl_seconds],
        )
        .await?;
    Ok(claimed.is_some())
}

/// Store what the provider reports about an identity. Absent fields leave the
/// stored ones alone, so a token scoped too narrowly to see the email does not
/// erase one an earlier, wider login recorded.
pub async fn store_user_profile(
    txn: &Transaction<'_>,
    provider: &str,
    subject: &str,
    profile: &UserProfile,
) -> Result<(), DBError> {
    let stmt = txn
        .prepare_cached(
            // Whether an address is verified is a statement about that address,
            // so the two move together: keeping the stored email means keeping
            // the verdict that came with it.
            "UPDATE app_user SET \
             email = COALESCE($3, email), \
             email_verified = CASE WHEN $3 IS NULL THEN email_verified ELSE $4 END, \
             display_name = COALESCE($5, display_name) \
             WHERE provider = $1 AND subject = $2",
        )
        .await?;
    txn.execute(
        &stmt,
        &[
            &provider,
            &subject,
            &profile.email,
            &profile.email_verified,
            &profile.display_name,
        ],
    )
    .await?;
    Ok(())
}

/// Enrolls a user into the listed tenants where the tenant already exists and
/// the user is not yet a member. Never creates a tenant and never changes an
/// existing membership: a passively listed claim entry must not mint a tenant
/// with the logger-in as its admin, nor overwrite a role an admin set.
#[allow(clippy::too_many_arguments)]
pub async fn enroll_in_existing_tenants(
    txn: &Transaction<'_>,
    new_user_id: Uuid,
    provider: &str,
    subject: &str,
    email: Option<&str>,
    names: &[String],
    role: Role,
    origin: MembershipOrigin,
) -> Result<(), DBError> {
    let user_id = get_or_create_user(txn, new_user_id, provider, subject, email).await?;
    let stmt = txn
        .prepare_cached(
            "INSERT INTO tenant_membership (tenant_id, user_id, role, created_at, origin) \
             SELECT t.id, $2, $3, now(), $4 FROM tenant t WHERE t.tenant = $1 \
             ON CONFLICT (tenant_id, user_id) DO NOTHING",
        )
        .await?;
    for name in names {
        txn.execute(&stmt, &[name, &user_id.0, &role.as_str(), &origin.as_str()])
            .await?;
    }
    Ok(())
}

/// Lists the tenants a user may act in, joined with each tenant's name and
/// the user's role there. The authorization source of truth at login.
pub async fn list_user_memberships(
    txn: &Transaction<'_>,
    provider: &str,
    subject: &str,
) -> Result<Vec<UserMembership>, DBError> {
    let stmt = txn
        .prepare_cached(
            "SELECT m.tenant_id, t.tenant, m.role \
             FROM tenant_membership m \
             JOIN app_user u ON u.id = m.user_id \
             JOIN tenant t ON t.id = m.tenant_id \
             WHERE u.provider = $1 AND u.subject = $2 ORDER BY t.tenant",
        )
        .await?;
    let rows = txn.query(&stmt, &[&provider, &subject]).await?;
    let mut result = Vec::with_capacity(rows.len());
    for row in rows {
        result.push(UserMembership {
            tenant_id: TenantId(row.get(0)),
            name: row.get(1),
            role: Role::from_str(&row.get::<_, String>(2))?,
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
    origin: MembershipOrigin, // How the token named the tenant: claim or derived.
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
            // Insert-if-absent: an admin grant committed between the read
            // above and this write must win, not be overwritten.
            if insert_membership_if_absent(txn, tenant_id, user_id, role, origin).await? {
                role
            } else {
                get_member_role(txn, tenant_id, user_id)
                    .await?
                    .unwrap_or(role)
            }
        }
    };
    Ok((tenant_id, user_id, role))
}

/// Inserts a membership only if none exists; `true` when this call created it.
async fn insert_membership_if_absent(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    user_id: UserId,
    role: Role,
    origin: MembershipOrigin,
) -> Result<bool, DBError> {
    let stmt = txn
        .prepare_cached(
            "INSERT INTO tenant_membership (tenant_id, user_id, role, created_at, origin) \
             VALUES ($1, $2, $3, now(), $4) ON CONFLICT (tenant_id, user_id) DO NOTHING",
        )
        .await?;
    let inserted = txn
        .execute(
            &stmt,
            &[&tenant_id.0, &user_id.0, &role.as_str(), &origin.as_str()],
        )
        .await
        .map_err(DBError::from)
        .map_err(|e| maybe_tenant_id_foreign_key_constraint_err(e, tenant_id))
        .map_err(|e| maybe_user_id_foreign_key_constraint_err(e, user_id))?;
    Ok(inserted == 1)
}
