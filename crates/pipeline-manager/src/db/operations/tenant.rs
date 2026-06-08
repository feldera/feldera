use crate::db::error::DBError;
use crate::db::types::tenant::TenantId;
use crate::db::types::user::TenantInfo;
use deadpool_postgres::Transaction;
use uuid::Uuid;

/// Retrieves tenant, which is uniquely identified by the tuple (name, provider).
/// If the (name, provider) does not yet exist, creates it with the provided new identifier.
pub async fn get_or_create_tenant_id(
    txn: &Transaction<'_>,
    new_id: Uuid, // Used only if the tenant does not yet exist
    name: String,
    provider: String,
) -> Result<TenantId, DBError> {
    Ok(get_or_create_tenant_id_created(txn, new_id, name, provider)
        .await?
        .0)
}

/// As [`get_or_create_tenant_id`], but also reports whether the tenant was
/// newly created by this call. The boolean lets the login path grant the very
/// first principal of a fresh tenant the `admin` role.
pub async fn get_or_create_tenant_id_created(
    txn: &Transaction<'_>,
    new_id: Uuid,
    name: String,
    provider: String,
) -> Result<(TenantId, bool), DBError> {
    // Atomic get-or-create: a single INSERT ... ON CONFLICT DO NOTHING avoids
    // the SELECT-then-INSERT race where two concurrent first-logins to a fresh
    // (name, provider) both miss the SELECT, then one INSERT wins and the other
    // fails with a unique violation. `inserted` (1 vs 0 rows affected) tells us
    // whether THIS call created the tenant, which decides the first-member-admin
    // grant in `resolve_login`. A subsequent SELECT always finds the row.
    let stmt_insert = txn
        .prepare_cached(
            "INSERT INTO tenant (id, tenant, provider) VALUES ($1, $2, $3) \
             ON CONFLICT (tenant, provider) DO NOTHING",
        )
        .await?;
    let inserted = txn
        .execute(&stmt_insert, &[&new_id, &name, &provider])
        .await?;
    let stmt_select = txn
        .prepare_cached("SELECT id FROM tenant WHERE tenant = $1 AND provider = $2")
        .await?;
    let row = txn.query_one(&stmt_select, &[&name, &provider]).await?;
    Ok((TenantId(row.get(0)), inserted == 1))
}

/// Strict lookup of a tenant by name only, used for owner cross-tenant
/// resolution from the `Feldera-Tenant` header. Never creates a tenant; errors
/// on miss or ambiguity (a name shared across providers cannot be resolved).
pub async fn get_tenant_id_by_name(txn: &Transaction<'_>, name: &str) -> Result<TenantId, DBError> {
    let stmt = txn
        .prepare_cached("SELECT id FROM tenant WHERE tenant = $1")
        .await?;
    let rows = txn.query(&stmt, &[&name]).await?;
    match rows.len() {
        1 => Ok(TenantId(rows[0].get(0))),
        _ => Err(DBError::UnknownTenantName {
            name: name.to_string(),
        }),
    }
}

/// Lists all tenants in the installation (platform-wide, owner-only).
pub async fn list_tenants(txn: &Transaction<'_>) -> Result<Vec<TenantInfo>, DBError> {
    let stmt = txn
        .prepare_cached("SELECT id, tenant, provider FROM tenant ORDER BY tenant")
        .await?;
    let rows = txn.query(&stmt, &[]).await?;
    Ok(rows
        .iter()
        .map(|row| TenantInfo {
            id: TenantId(row.get(0)),
            name: row.get(1),
            provider: row.get(2),
        })
        .collect())
}

/// Retrieves the tenant name for a given tenant ID.
pub async fn get_tenant_name(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
) -> Result<String, DBError> {
    let stmt = txn
        .prepare_cached("SELECT tenant FROM tenant WHERE id = $1")
        .await?;
    let row = txn.query_one(&stmt, &[&tenant_id.0]).await?;
    Ok(row.get(0))
}
