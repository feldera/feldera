use crate::db::error::DBError;
use crate::db::operations::utils::maybe_unique_violation;
use crate::db::types::tenant::TenantId;
use crate::db::types::user::TenantInfo;
use deadpool_postgres::Transaction;
use uuid::Uuid;

/// Retrieves the tenant with this name, creating it with the provided new
/// identifier if it does not exist yet.
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

/// Retrieves the tenant with this name, creating it with `new_id` if it does
/// not exist yet. The second component is `true` if this call created the
/// tenant, and `false` if it already existed.
///
/// Atomic get-or-create: a single INSERT ... ON CONFLICT DO NOTHING avoids
/// the SELECT-then-INSERT race where two concurrent creations of a fresh
/// name both miss the SELECT, then one INSERT wins and the other fails with a
/// unique violation. `inserted` (1 vs 0 rows affected) tells us whether THIS
/// call created the tenant. The SELECT after it misses only if a concurrent
/// delete commits in between, which surfaces as an error, not a wrong result.
pub async fn get_or_create_tenant(
    txn: &Transaction<'_>,
    new_id: Uuid,
    name: &str,
    provider: &str,
) -> Result<(TenantInfo, bool), DBError> {
    let stmt_insert = txn
        .prepare_cached(
            "INSERT INTO tenant (id, tenant, initial_provider) VALUES ($1, $2, $3) \
             ON CONFLICT (tenant) DO NOTHING",
        )
        .await?;
    let inserted = txn
        .execute(&stmt_insert, &[&new_id, &name, &provider])
        .await?;
    let stmt_select = txn
        .prepare_cached("SELECT id, tenant, initial_provider FROM tenant WHERE tenant = $1")
        .await?;
    let row = txn.query_one(&stmt_select, &[&name]).await?;
    Ok((
        TenantInfo {
            id: TenantId(row.get(0)),
            name: row.get(1),
            initial_provider: row.get(2),
        },
        inserted == 1,
    ))
}

/// Ensures the default tenant exists, which every start does.
///
/// Keyed by id rather than by name: the default tenant may have been renamed
/// since, and the row is still the same tenant. `ON CONFLICT DO NOTHING`
/// without a target covers the primary key as well as the unique name, so
/// neither a rename nor a second tenant taking the name `default` stops the
/// manager from starting.
pub async fn ensure_default_tenant(
    txn: &Transaction<'_>,
    id: Uuid,
    name: &str,
    provider: &str,
) -> Result<(), DBError> {
    let stmt = txn
        .prepare_cached(
            "INSERT INTO tenant (id, tenant, initial_provider) VALUES ($1, $2, $3) \
             ON CONFLICT DO NOTHING",
        )
        .await?;
    txn.execute(&stmt, &[&id, &name, &provider]).await?;
    Ok(())
}

/// As [`get_or_create_tenant_id`]. The second component of the returned value
/// is `true` if this call created the tenant, and `false` if it already existed.
/// The created flag decides the first-member grant in `resolve_login`.
pub async fn get_or_create_tenant_id_created(
    txn: &Transaction<'_>,
    new_id: Uuid,
    name: String,
    provider: String,
) -> Result<(TenantId, bool), DBError> {
    let (tenant, created) = get_or_create_tenant(txn, new_id, &name, &provider).await?;
    Ok((tenant.id, created))
}

/// Strict resolution of a `Feldera-Tenant` selector, used wherever a principal
/// picks one of the tenants it is authorized for. A selector that parses as a
/// UUID is resolved by tenant id; otherwise it is resolved by name. Never
/// creates a tenant; errors with `UnknownTenantName` on miss, so a typo cannot
/// silently create or cross into the wrong tenant. The caller is
/// responsible for checking that the resolved tenant is one the principal may
/// act in.
pub async fn resolve_tenant_selector(
    txn: &Transaction<'_>,
    selector: &str,
) -> Result<TenantId, DBError> {
    Ok(get_tenant(txn, selector).await?.id)
}

/// Retrieves a single tenant by selector, as [`resolve_tenant_selector`]:
/// a selector that parses as a UUID is looked up by tenant id, otherwise by
/// name. Never creates a tenant; errors with `UnknownTenantName` on miss.
pub async fn get_tenant(txn: &Transaction<'_>, selector: &str) -> Result<TenantInfo, DBError> {
    let row = if let Ok(uuid) = Uuid::parse_str(selector) {
        let stmt = txn
            .prepare_cached("SELECT id, tenant, initial_provider FROM tenant WHERE id = $1")
            .await?;
        txn.query_opt(&stmt, &[&uuid]).await?
    } else {
        let stmt = txn
            .prepare_cached("SELECT id, tenant, initial_provider FROM tenant WHERE tenant = $1")
            .await?;
        txn.query_opt(&stmt, &[&selector]).await?
    };
    row.map(|row| TenantInfo {
        id: TenantId(row.get(0)),
        name: row.get(1),
        initial_provider: row.get(2),
    })
    .ok_or(DBError::UnknownTenantName {
        name: selector.to_string(),
    })
}

/// Renames a tenant, failing with a conflict if the name is already taken.
///
/// Only the name changes: every other table references a tenant by its id, so
/// no membership, key, pipeline or trust is affected. The name is what a login
/// resolves, though, so renaming changes which tenant those users land in.
///
/// With `displace_existing`, the tenant that currently holds `new_name` is
/// renamed to `<name> (<id>)` so that this one can have the name, and is
/// returned. It keeps its pipelines, keys, members and trusts; nothing is
/// merged or deleted.
pub async fn rename_tenant(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    new_name: &str,
    displace_existing: bool,
) -> Result<Option<TenantInfo>, DBError> {
    // Both renames run in this one transaction. A login re-creates the name it
    // resolves on its very next request, so freeing the name and claiming it as
    // two calls loses the race every time.
    let displaced = if displace_existing {
        displace_name_holder(txn, tenant_id, new_name).await?
    } else {
        None
    };
    let stmt = txn
        .prepare_cached("UPDATE tenant SET tenant = $2 WHERE id = $1")
        .await?;
    let updated = txn
        .execute(&stmt, &[&tenant_id.0, &new_name])
        .await
        .map_err(maybe_unique_violation)?;
    if updated > 0 {
        Ok(displaced)
    } else {
        Err(DBError::UnknownTenant { tenant_id })
    }
}

/// Renames whichever tenant holds `name` to `<name> (<id>)`, leaving `name`
/// for the caller to claim. Returns that tenant, or `None` when no tenant held
/// the name or it is `keep`'s own name already.
async fn displace_name_holder(
    txn: &Transaction<'_>,
    keep: TenantId,
    name: &str,
) -> Result<Option<TenantInfo>, DBError> {
    let stmt = txn
        .prepare_cached(
            "UPDATE tenant SET tenant = tenant || ' (' || id || ')' \
             WHERE tenant = $1 AND id <> $2 \
             RETURNING id, tenant, initial_provider",
        )
        .await?;
    let row = txn
        .query_opt(&stmt, &[&name, &keep.0])
        .await
        .map_err(maybe_unique_violation)?;
    Ok(row.map(|row| TenantInfo {
        id: TenantId(row.get(0)),
        name: row.get(1),
        initial_provider: row.get(2),
    }))
}

/// Deletes a tenant that holds nothing, failing otherwise.
///
/// Every tenant-scoped table cascades on this delete, so an unguarded delete
/// would take pipelines with it, silently and with no undo. The guard is
/// emptiness: no pipelines, API keys or OIDC trust relationships. Memberships
/// are not counted, since a login re-creates its own on the next request, and
/// they are the only thing a leftover tenant usually holds.
pub async fn delete_tenant(txn: &Transaction<'_>, tenant_id: TenantId) -> Result<(), DBError> {
    let stmt = txn
        .prepare_cached(
            "SELECT (SELECT count(*) FROM pipeline WHERE tenant_id = $1), \
                    (SELECT count(*) FROM api_key WHERE tenant_id = $1), \
                    (SELECT count(*) FROM oidc_trust_relationship WHERE tenant_id = $1)",
        )
        .await?;
    let row = txn.query_one(&stmt, &[&tenant_id.0]).await?;
    let (pipelines, api_keys, oidc_trusts): (i64, i64, i64) = (row.get(0), row.get(1), row.get(2));
    if pipelines > 0 || api_keys > 0 || oidc_trusts > 0 {
        return Err(DBError::TenantNotEmpty {
            tenant_id,
            pipelines,
            api_keys,
            oidc_trusts,
        });
    }

    let stmt = txn
        .prepare_cached("DELETE FROM tenant WHERE id = $1")
        .await?;
    let deleted = txn.execute(&stmt, &[&tenant_id.0]).await?;
    if deleted > 0 {
        Ok(())
    } else {
        Err(DBError::UnknownTenant { tenant_id })
    }
}

/// Lists all tenants in the installation (platform-wide, owner-only).
pub async fn list_tenants(txn: &Transaction<'_>) -> Result<Vec<TenantInfo>, DBError> {
    let stmt = txn
        .prepare_cached("SELECT id, tenant, initial_provider FROM tenant ORDER BY tenant")
        .await?;
    let rows = txn.query(&stmt, &[]).await?;
    Ok(rows
        .iter()
        .map(|row| TenantInfo {
            id: TenantId(row.get(0)),
            name: row.get(1),
            initial_provider: row.get(2),
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
