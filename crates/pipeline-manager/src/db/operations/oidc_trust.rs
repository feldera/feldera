use crate::db::error::DBError;
use crate::db::operations::utils::{
    maybe_tenant_id_foreign_key_constraint_err, maybe_unique_violation,
};
use crate::db::types::oidc_trust::{OidcTrustDescr, OidcTrustId, claim_matches};
use crate::db::types::role::Role;
use crate::db::types::tenant::TenantId;
use crate::oidc::destination::{TenantIssuerPolicy, validate_tenant_oidc_url};
use crate::oidc::trust_name::validate_oidc_trust_name;
use deadpool_postgres::Transaction;
use std::str::FromStr;
use uuid::Uuid;

/// Build a trust from a row of the `SELECT` list used throughout this module.
/// `row.get` panics if a column's type or position does not match, which is a
/// bug in that `SELECT`, not a runtime condition; only the role, stored as text,
/// can fail on data and returns an error.
fn row_to_descr(row: &tokio_postgres::Row) -> Result<OidcTrustDescr, DBError> {
    let id: Uuid = row.get(0);
    let name: String = row.get(1);
    let description: Option<String> = row.get(2);
    let issuer: String = row.get(3);
    let subject: String = row.get(4);
    let audience: Option<String> = row.get(5);
    let role = Role::from_str(&row.get::<_, String>(6))?;
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

pub async fn list_oidc_trust(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
) -> Result<Vec<OidcTrustDescr>, DBError> {
    let stmt = txn
        .prepare_cached(
            "SELECT id, name, description, issuer, subject, audience, role \
             FROM oidc_trust_relationship WHERE tenant_id = $1",
        )
        .await?;
    let rows = txn.query(&stmt, &[&tenant_id.0]).await?;
    rows.iter().map(row_to_descr).collect()
}

pub async fn get_oidc_trust(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    name: &str,
) -> Result<OidcTrustDescr, DBError> {
    let stmt = txn
        .prepare_cached(
            "SELECT id, name, description, issuer, subject, audience, role \
             FROM oidc_trust_relationship WHERE tenant_id = $1 AND name = $2",
        )
        .await?;
    let maybe_row = txn.query_opt(&stmt, &[&tenant_id.0, &name]).await?;
    match maybe_row {
        Some(row) => row_to_descr(&row),
        None => Err(DBError::UnknownOidcTrust {
            name: name.to_string(),
        }),
    }
}

pub async fn delete_oidc_trust(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    name: &str,
) -> Result<(), DBError> {
    let stmt = txn
        .prepare_cached("DELETE FROM oidc_trust_relationship WHERE tenant_id = $1 AND name = $2")
        .await?;
    let res = txn.execute(&stmt, &[&tenant_id.0, &name]).await?;
    if res > 0 {
        Ok(())
    } else {
        Err(DBError::UnknownOidcTrust {
            name: name.to_string(),
        })
    }
}

// A trust always belongs to one tenant: `owner` is configuration only.
#[allow(clippy::too_many_arguments)]
pub async fn create_oidc_trust(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    id: Uuid,
    name: &str,
    description: Option<&str>,
    issuer: &str,
    subject: &str,
    audience: Option<&str>,
    role: Role,
    issuer_policy: TenantIssuerPolicy,
) -> Result<(), DBError> {
    validate_oidc_trust_name(name)?;
    if issuer.is_empty() {
        return Err(DBError::EmptyOidcTrustField {
            field: "issuer".to_string(),
        });
    }
    // The manager fetches this issuer from its own network position before it
    // verifies any signature, so a registration may not name an internal
    // service unless the operator permits it. A hostname is checked again when
    // the connection is made.
    validate_tenant_oidc_url(issuer, issuer_policy).map_err(|e| DBError::InvalidOidcIssuerUrl {
        issuer: issuer.to_string(),
        reason: e.to_string(),
    })?;
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
                &tenant_id.0,
                &name,
                &description,
                &issuer,
                &subject,
                &audience,
                &role.as_str(),
            ],
        )
        .await
        .map_err(maybe_unique_violation)
        .map_err(|e| maybe_tenant_id_foreign_key_constraint_err(e, tenant_id))?;
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

/// Resolve a federated token to the tenants and roles it is authorized for.
///
/// One entry per tenant, carrying the most permissive role that matched there.
/// A trust is a candidate when it is registered for `issuer`, its subject
/// pattern matches `subject`, and, if it sets an audience pattern, that pattern
/// matches one of `audiences` (the audience is a security filter, not the
/// tenant key). When the result spans several tenants the caller disambiguates
/// with the `Feldera-Tenant` header. Sorted by tenant for a deterministic order.
///
/// For a GitHub Actions token with
/// `iss = https://token.actions.githubusercontent.com`,
/// `sub = repo:acme/api:ref:refs/heads/main`, `aud = https://github.com/acme`:
///
/// ```text
/// registered trusts
///   ("acme-ci",   tenant=acme, subject="repo:acme/*",     audience=None,    role=write)
///   ("acme-main", tenant=acme, subject="repo:acme/api:*", audience="https://github.com/acme", role=admin)
///   ("other",     tenant=beta, subject="repo:beta/*",     audience=None,    role=write)
///
/// match_oidc_trust(..) => [(acme, Admin)]
/// ```
///
/// `beta` does not appear because its subject pattern does not match. `acme`
/// appears once, at `admin`, the most permissive of its two matching trusts.
pub async fn match_oidc_trust(
    txn: &Transaction<'_>,
    issuer: &str,
    subject: &str,
    audiences: &[String],
) -> Result<Vec<(TenantId, Role)>, DBError> {
    let stmt = txn
        .prepare_cached(
            "SELECT tenant_id, subject, audience, role \
             FROM oidc_trust_relationship WHERE issuer = $1",
        )
        .await?;
    let rows = txn.query(&stmt, &[&issuer]).await?;
    let mut matched: Vec<(TenantId, Role)> = Vec::new();
    for row in rows {
        let tenant_id = TenantId(row.get::<_, Uuid>(0));
        let pattern_subject: String = row.get(1);
        let pattern_audience: Option<String> = row.get(2);
        let role = Role::from_str(&row.get::<_, String>(3))?;

        if !claim_matches(&pattern_subject, subject) {
            continue;
        }
        if let Some(aud_pattern) = &pattern_audience
            && !audiences.iter().any(|a| claim_matches(aud_pattern, a))
        {
            continue;
        }
        match matched.iter_mut().find(|(t, _)| *t == tenant_id) {
            Some(entry) => entry.1 = entry.1.max(role),
            None => matched.push((tenant_id, role)),
        }
    }
    matched.sort_by_key(|(t, _)| t.0);
    Ok(matched)
}
