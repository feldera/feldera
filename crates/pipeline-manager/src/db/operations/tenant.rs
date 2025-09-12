use crate::db::error::DBError;
use crate::db::types::tenant::TenantId;
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
    let stmt_select = txn
        .prepare_cached("SELECT id FROM tenant WHERE tenant = $1 AND provider = $2")
        .await?;
    let row = txn.query_opt(&stmt_select, &[&name, &provider]).await?;
    let id = match row {
        None => {
            let stmt_insert = txn
                .prepare_cached("INSERT INTO tenant (id, tenant, provider) VALUES ($1, $2, $3)")
                .await?;
            txn.execute(&stmt_insert, &[&new_id, &name, &provider])
                .await?;
            new_id
        }
        Some(row) => row.get(0),
    };
    Ok(TenantId(id))
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
