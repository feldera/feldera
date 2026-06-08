use crate::db::error::DBError;
use crate::db::operations::utils::{
    maybe_tenant_id_foreign_key_constraint_err, maybe_unique_violation,
};
use crate::db::types::api_key::{ApiKeyDescr, ApiKeyId};
use crate::db::types::role::{MintableKeyRole, Role};
use crate::db::types::tenant::TenantId;
use crate::db::types::utils::validate_name;
use deadpool_postgres::Transaction;
use openssl::sha;
use std::str::FromStr;
use uuid::Uuid;

/// Parse a role string read from the DB, turning an unexpected value into a
/// request error rather than a panic (this runs on the auth path).
fn parse_role(s: &str) -> Result<Role, DBError> {
    Role::from_str(s).map_err(|_| DBError::InvalidRoleString {
        value: s.to_string(),
    })
}

pub async fn list_api_keys(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
) -> Result<Vec<ApiKeyDescr>, DBError> {
    let stmt = txn
        .prepare_cached("SELECT id, name, role FROM api_key WHERE tenant_id = $1")
        .await?;
    let rows = txn.query(&stmt, &[&tenant_id.0]).await?;
    let mut result = Vec::with_capacity(rows.len());
    for row in rows {
        let id: ApiKeyId = ApiKeyId(row.get(0));
        let name: String = row.get(1);
        let role = parse_role(&row.get::<_, String>(2))?;
        result.push(ApiKeyDescr { id, name, role });
    }
    Ok(result)
}

pub async fn get_api_key(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    name: &str,
) -> Result<ApiKeyDescr, DBError> {
    let stmt = txn
        .prepare_cached("SELECT id, name, role FROM api_key WHERE tenant_id = $1 and name = $2")
        .await?;
    let maybe_row = txn.query_opt(&stmt, &[&tenant_id.0, &name]).await?;
    if let Some(row) = maybe_row {
        let id: ApiKeyId = ApiKeyId(row.get(0));
        let name: String = row.get(1);
        let role = parse_role(&row.get::<_, String>(2))?;
        Ok(ApiKeyDescr { id, name, role })
    } else {
        Err(DBError::UnknownApiKey {
            name: name.to_string(),
        })
    }
}

pub async fn delete_api_key(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    name: &str,
) -> Result<(), DBError> {
    let stmt = txn
        .prepare_cached("DELETE FROM api_key WHERE tenant_id = $1 AND name = $2")
        .await?;
    let res = txn.execute(&stmt, &[&tenant_id.0, &name]).await?;
    if res > 0 {
        Ok(())
    } else {
        Err(DBError::UnknownApiKey {
            name: name.to_string(),
        })
    }
}

/// Persists the SHA-256 hash of an API key. The role is a [`MintableKeyRole`]
/// (read/write only) so `admin`/`owner` cannot reach storage by construction.
pub async fn store_api_key_hash(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    id: Uuid,
    name: &str,
    key: &str,
    role: MintableKeyRole,
) -> Result<(), DBError> {
    validate_name(name)?;
    let mut hasher = sha::Sha256::new();
    hasher.update(key.as_bytes());
    let hash = openssl::base64::encode_block(&hasher.finish());
    let stmt = txn
        .prepare_cached(
            "INSERT INTO api_key (id, tenant_id, name, hash, role) VALUES ($1, $2, $3, $4, $5)",
        )
        .await?;
    let res = txn
        .execute(
            &stmt,
            &[&id, &tenant_id.0, &name, &hash, &role.role().as_str()],
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

pub async fn validate_api_key(
    txn: &Transaction<'_>,
    api_key: &str,
) -> Result<(TenantId, Role), DBError> {
    let mut hasher = sha::Sha256::new();
    hasher.update(api_key.as_bytes());
    let hash = openssl::base64::encode_block(&hasher.finish());
    let stmt = txn
        .prepare_cached("SELECT tenant_id, role FROM api_key WHERE hash = $1")
        .await?;
    let res = txn.query(&stmt, &[&hash]).await?;
    let res = res.first().ok_or(DBError::InvalidApiKey)?;
    let tenant_id = TenantId(res.get(0));
    let role = parse_role(&res.get::<_, String>(1))?;
    Ok((tenant_id, role))
}
