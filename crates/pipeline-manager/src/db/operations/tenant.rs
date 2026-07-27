use crate::db::error::DBError;
use crate::db::operations::utils::maybe_unique_violation;
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
/// first principal of a fresh tenant the role configured by
/// `--first-user-role` / `FELDERA_AUTH_FIRST_USER_ROLE` (`admin` by default).
pub async fn get_or_create_tenant_id_created(
    txn: &Transaction<'_>,
    new_id: Uuid,
    name: String,
    provider: String,
) -> Result<(TenantId, bool), DBError> {
    // Atomic get-or-create: a single INSERT ... ON CONFLICT DO NOTHING avoids
    // the SELECT-then-INSERT race where two concurrent first-logins to a fresh
    // name both miss the SELECT, then one INSERT wins and the other fails with a
    // unique violation. `inserted` (1 vs 0 rows affected) tells us whether THIS
    // call created the tenant, which decides the first-member grant in
    // `resolve_login`. A subsequent SELECT always finds the row.
    //
    // The name alone identifies the tenant. `provider` is recorded as the issuer
    // it was first seen under, and deliberately not matched on: were it part of
    // the key, changing the configured issuer would miss here and fork a second
    // tenant of the same name, stranding the pipelines on the first.
    let stmt_insert = txn
        .prepare_cached(
            "INSERT INTO tenant (id, tenant, provider) VALUES ($1, $2, $3) \
             ON CONFLICT (tenant) DO NOTHING",
        )
        .await?;
    let inserted = txn
        .execute(&stmt_insert, &[&new_id, &name, &provider])
        .await?;
    let stmt_select = txn
        .prepare_cached("SELECT id FROM tenant WHERE tenant = $1")
        .await?;
    let row = txn.query_one(&stmt_select, &[&name]).await?;
    Ok((TenantId(row.get(0)), inserted == 1))
}

/// Strict lookup of a tenant by name, used to resolve a `Feldera-Tenant` header.
/// Never creates a tenant; a miss is an error. The name is unique, so at most
/// one tenant can match.
pub async fn get_tenant_id_by_name(txn: &Transaction<'_>, name: &str) -> Result<TenantId, DBError> {
    let stmt = txn
        .prepare_cached("SELECT id FROM tenant WHERE tenant = $1")
        .await?;
    let row = txn.query_opt(&stmt, &[&name]).await?;
    row.map(|row| TenantId(row.get(0)))
        .ok_or(DBError::UnknownTenantName {
            name: name.to_string(),
        })
}

/// Strict resolution of a `Feldera-Tenant` selector, used wherever a principal
/// picks one of the tenants it is authorized for. A selector that parses as a
/// UUID is resolved by tenant id; otherwise it is resolved by name. Never
/// creates a tenant; errors with `UnknownTenantName` (HTTP 404) on miss, so a
/// typo cannot silently create or cross into the wrong tenant. The caller is
/// responsible for checking that the resolved tenant is one the principal may
/// act in.
pub async fn resolve_tenant_selector(
    txn: &Transaction<'_>,
    selector: &str,
) -> Result<TenantId, DBError> {
    if let Ok(uuid) = Uuid::parse_str(selector) {
        let stmt = txn
            .prepare_cached("SELECT id FROM tenant WHERE id = $1")
            .await?;
        let row = txn.query_opt(&stmt, &[&uuid]).await?;
        return row
            .map(|r| TenantId(r.get(0)))
            .ok_or_else(|| DBError::UnknownTenantName {
                name: selector.to_string(),
            });
    }
    get_tenant_id_by_name(txn, selector).await
}

/// Create a tenant, failing with a conflict if `(name, provider)` already
/// exists. Distinct from the get-or-create login path: the owner-only explicit
/// create endpoint should report a duplicate rather than silently returning the
/// existing tenant.
pub async fn create_tenant(
    txn: &Transaction<'_>,
    id: Uuid,
    name: &str,
    provider: &str,
) -> Result<TenantId, DBError> {
    let stmt = txn
        .prepare_cached("INSERT INTO tenant (id, tenant, provider) VALUES ($1, $2, $3)")
        .await?;
    txn.execute(&stmt, &[&id, &name, &provider])
        .await
        .map_err(maybe_unique_violation)?;
    Ok(TenantId(id))
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
