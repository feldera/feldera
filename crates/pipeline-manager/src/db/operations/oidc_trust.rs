use crate::db::error::DBError;
use crate::db::operations::utils::{
    maybe_tenant_id_foreign_key_constraint_err, maybe_unique_violation,
};
use crate::db::types::oidc_trust::{claim_matches, OidcTrustDescr, OidcTrustId};
use crate::db::types::role::Role;
use crate::db::types::tenant::TenantId;
use crate::db::types::utils::validate_oidc_trust_name;
use deadpool_postgres::Transaction;
use std::str::FromStr;
use uuid::Uuid;

fn parse_role(s: &str) -> Result<Role, DBError> {
    Role::from_str(s).map_err(|_| DBError::InvalidRoleString {
        value: s.to_string(),
    })
}

fn row_to_descr(row: &tokio_postgres::Row) -> Result<OidcTrustDescr, DBError> {
    let id: Uuid = row.get(0);
    let name: String = row.get(1);
    let description: Option<String> = row.get(2);
    let issuer: String = row.get(3);
    let subject: String = row.get(4);
    let audience: Option<String> = row.get(5);
    let role = parse_role(&row.get::<_, String>(6))?;
    Ok(OidcTrustDescr {
        id: OidcTrustId(id),
        name,
        description,
        issuer,
        subject,
        audience,
        role,
    })
}

// `tenant_id` scopes the query: `Some(t)` selects that tenant's trusts;
// `None` selects the platform-wide owner trusts (rows with NULL tenant_id).
pub async fn list_oidc_trust(
    txn: &Transaction<'_>,
    tenant_id: Option<TenantId>,
) -> Result<Vec<OidcTrustDescr>, DBError> {
    const COLS: &str =
        "SELECT id, name, description, issuer, subject, audience, role FROM oidc_trust_relationship";
    let rows = match tenant_id {
        Some(t) => {
            let stmt = txn
                .prepare_cached(&format!("{COLS} WHERE tenant_id = $1"))
                .await?;
            txn.query(&stmt, &[&t.0]).await?
        }
        None => {
            let stmt = txn
                .prepare_cached(&format!("{COLS} WHERE tenant_id IS NULL"))
                .await?;
            txn.query(&stmt, &[]).await?
        }
    };
    rows.iter().map(row_to_descr).collect()
}

pub async fn get_oidc_trust(
    txn: &Transaction<'_>,
    tenant_id: Option<TenantId>,
    name: &str,
) -> Result<OidcTrustDescr, DBError> {
    const COLS: &str =
        "SELECT id, name, description, issuer, subject, audience, role FROM oidc_trust_relationship";
    let maybe_row = match tenant_id {
        Some(t) => {
            let stmt = txn
                .prepare_cached(&format!("{COLS} WHERE tenant_id = $1 AND name = $2"))
                .await?;
            txn.query_opt(&stmt, &[&t.0, &name]).await?
        }
        None => {
            let stmt = txn
                .prepare_cached(&format!("{COLS} WHERE tenant_id IS NULL AND name = $1"))
                .await?;
            txn.query_opt(&stmt, &[&name]).await?
        }
    };
    match maybe_row {
        Some(row) => row_to_descr(&row),
        None => Err(DBError::UnknownOidcTrust {
            name: name.to_string(),
        }),
    }
}

pub async fn delete_oidc_trust(
    txn: &Transaction<'_>,
    tenant_id: Option<TenantId>,
    name: &str,
) -> Result<(), DBError> {
    let res = match tenant_id {
        Some(t) => {
            let stmt = txn
                .prepare_cached(
                    "DELETE FROM oidc_trust_relationship WHERE tenant_id = $1 AND name = $2",
                )
                .await?;
            txn.execute(&stmt, &[&t.0, &name]).await?
        }
        None => {
            let stmt = txn
                .prepare_cached(
                    "DELETE FROM oidc_trust_relationship WHERE tenant_id IS NULL AND name = $1",
                )
                .await?;
            txn.execute(&stmt, &[&name]).await?
        }
    };
    if res > 0 {
        Ok(())
    } else {
        Err(DBError::UnknownOidcTrust {
            name: name.to_string(),
        })
    }
}

// `tenant_id` is `None` for a platform-wide owner trust and `Some` for a
// tenant-scoped one; the caller pairs it with the role (owner iff None), which
// the `oidc_trust_owner_is_platform` CHECK also enforces.
#[allow(clippy::too_many_arguments)]
pub async fn create_oidc_trust(
    txn: &Transaction<'_>,
    tenant_id: Option<TenantId>,
    id: Uuid,
    name: &str,
    description: Option<&str>,
    issuer: &str,
    subject: &str,
    audience: Option<&str>,
    role: Role,
) -> Result<(), DBError> {
    validate_oidc_trust_name(name)?;
    if issuer.is_empty() {
        return Err(DBError::EmptyOidcTrustField {
            field: "issuer".to_string(),
        });
    }
    if subject.is_empty() {
        return Err(DBError::EmptyOidcTrustField {
            field: "subject".to_string(),
        });
    }
    let stmt = txn
        .prepare_cached(
            "INSERT INTO oidc_trust_relationship \
             (id, tenant_id, name, description, issuer, subject, audience, role) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .await?;
    let res = txn
        .execute(
            &stmt,
            &[
                &id,
                &tenant_id.map(|t| t.0),
                &name,
                &description,
                &issuer,
                &subject,
                &audience,
                &role.as_str(),
            ],
        )
        .await
        .map_err(maybe_unique_violation);
    // The FK only exists for a concrete tenant; owner trusts (NULL) cannot
    // violate it.
    let res = match tenant_id {
        Some(t) => res.map_err(|e| maybe_tenant_id_foreign_key_constraint_err(e, t))?,
        None => res?,
    };
    if res > 0 {
        Ok(())
    } else {
        Err(DBError::duplicate_key())
    }
}

/// Cheap indexed check: is `issuer` named by at least one trust relationship?
///
/// The federated auth path calls this before any OIDC discovery / JWKS fetch,
/// so an unregistered issuer is rejected without an outbound request. Without
/// this gate an unauthenticated caller could make the manager fetch arbitrary
/// URLs (SSRF) and amplify one request into repeated discovery fetches (DoS).
pub async fn is_trusted_issuer(txn: &Transaction<'_>, issuer: &str) -> Result<bool, DBError> {
    let stmt = txn
        .prepare_cached("SELECT EXISTS (SELECT 1 FROM oidc_trust_relationship WHERE issuer = $1)")
        .await?;
    let row = txn.query_one(&stmt, &[&issuer]).await?;
    Ok(row.get(0))
}

/// Resolve a federated token to the tenant and role it is authorized for.
///
/// Every trust matching this token, one entry per scope with the most
/// permissive matching role. A trust is a candidate when it is registered for
/// `issuer`, its subject pattern matches `subject`, and, if it sets an audience
/// pattern, that pattern matches one of `audiences` (the audience is a security
/// filter, not the tenant key). The scope is the trust's `tenant_id`: `Some`
/// for a tenant-scoped trust, `None` for a platform-wide owner trust. When the
/// result spans several scopes the caller disambiguates with the
/// `Feldera-Tenant` header. Sorted (owner scope first) for a deterministic order.
pub async fn match_oidc_trust(
    txn: &Transaction<'_>,
    issuer: &str,
    subject: &str,
    audiences: &[String],
) -> Result<Vec<(Option<TenantId>, Role)>, DBError> {
    let stmt = txn
        .prepare_cached(
            "SELECT tenant_id, subject, audience, role \
             FROM oidc_trust_relationship WHERE issuer = $1",
        )
        .await?;
    let rows = txn.query(&stmt, &[&issuer]).await?;
    let mut matched: Vec<(Option<TenantId>, Role)> = Vec::new();
    for row in rows {
        let tenant_id = row.get::<_, Option<Uuid>>(0).map(TenantId);
        let pattern_subject: String = row.get(1);
        let pattern_audience: Option<String> = row.get(2);
        let role = parse_role(&row.get::<_, String>(3))?;

        if !claim_matches(&pattern_subject, subject) {
            continue;
        }
        if let Some(aud_pattern) = &pattern_audience {
            if !audiences.iter().any(|a| claim_matches(aud_pattern, a)) {
                continue;
            }
        }
        match matched.iter_mut().find(|(t, _)| *t == tenant_id) {
            Some(entry) => entry.1 = entry.1.max(role),
            None => matched.push((tenant_id, role)),
        }
    }
    matched.sort_by_key(|(t, _)| t.map(|x| x.0));
    Ok(matched)
}
