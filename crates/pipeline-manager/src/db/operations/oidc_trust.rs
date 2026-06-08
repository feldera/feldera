use crate::db::error::DBError;
use crate::db::operations::utils::{
    maybe_tenant_id_foreign_key_constraint_err, maybe_unique_violation,
};
use crate::db::types::oidc_trust::{claim_matches, OidcTrustDescr, OidcTrustId};
use crate::db::types::role::Role;
use crate::db::types::tenant::TenantId;
use crate::db::types::utils::validate_name;
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
) -> Result<(), DBError> {
    validate_name(name)?;
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

/// Resolve a federated token to the tenant and role it is authorized for.
///
/// All trusts registered for `issuer` whose subject pattern matches `subject`
/// and (if present) audience pattern matches one of `audiences` are candidates.
/// The `aud` claim is the disambiguator: a tenant scopes its trust with a
/// tenant-specific audience. If candidates resolve to more than one distinct
/// tenant, the match is ambiguous and rejected (fail closed) rather than
/// silently crossing tenants. Within a single tenant the most permissive
/// matching role wins.
pub async fn match_oidc_trust(
    txn: &Transaction<'_>,
    issuer: &str,
    subject: &str,
    audiences: &[String],
) -> Result<Option<(TenantId, Role)>, DBError> {
    let stmt = txn
        .prepare_cached(
            "SELECT tenant_id, subject, audience, role \
             FROM oidc_trust_relationship WHERE issuer = $1",
        )
        .await?;
    let rows = txn.query(&stmt, &[&issuer]).await?;
    let mut matched: Option<(TenantId, Role)> = None;
    for row in rows {
        let tenant_id = TenantId(row.get(0));
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
        match matched {
            None => matched = Some((tenant_id, role)),
            Some((prev_tenant, prev_role)) => {
                if prev_tenant != tenant_id {
                    // Ambiguous cross-tenant match: fail closed. Operators must
                    // disambiguate with tenant-specific audiences.
                    return Err(DBError::UnauthorizedOidcToken);
                }
                matched = Some((tenant_id, prev_role.max(role)));
            }
        }
    }
    Ok(matched)
}
