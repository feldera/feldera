//! Per-tenant connector dependency configuration (the contents of
//! `connectors.toml`) stored in the `tenant_connector_config` table.
//!
//! The table holds one row per tenant. Rows are created lazily by
//! [`get_or_bootstrap`] on first read; the seed content (when configured)
//! comes from `CompilerConfig::connectors_toml_path`. After bootstrap the
//! database is authoritative — the seed file is never re-read for that
//! tenant.
//!
//! Updates use optimistic concurrency: the caller passes the
//! `content_hash` (ETag) it observed on the previous read; if the row has
//! since changed, [`put`] returns
//! [`DBError::OutdatedConnectorsConfigHash`].
use crate::db::error::DBError;
use crate::db::types::tenant::TenantId;
use chrono::{DateTime, Utc};
use deadpool_postgres::Transaction;
use sha2::{Digest, Sha256};

/// One row from `tenant_connector_config`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantConnectorConfig {
    /// The verbatim `connectors.toml` blob (one Cargo dep per line, no
    /// section header). May be empty, meaning "bundled connectors only".
    pub content: String,
    /// `sha256(content)` as a lowercase hex string. Used as the describer
    /// cache key and as the ETag for optimistic concurrency.
    pub content_hash: String,
    /// Monotonic per-tenant version, incremented on every successful
    /// [`put`]. Starts at 1.
    pub version: i64,
    pub edited_at: DateTime<Utc>,
    /// Free-form identifier of the user (or `"system"` for bootstrap)
    /// who performed the last edit.
    pub edited_by: String,
}

/// Compute the content hash used as the ETag and describer cache key.
pub fn compute_content_hash(content: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(content.as_bytes());
    hex_encode(&hasher.finalize())
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

const COLUMNS: &str = "content, content_hash, version, edited_at, edited_by";

fn row_to_config(row: &tokio_postgres::Row) -> TenantConnectorConfig {
    TenantConnectorConfig {
        content: row.get(0),
        content_hash: row.get(1),
        version: row.get(2),
        edited_at: row.get(3),
        edited_by: row.get(4),
    }
}

/// Retrieve a tenant's connector config, returning `None` when no row
/// exists yet for the tenant.
pub async fn get(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
) -> Result<Option<TenantConnectorConfig>, DBError> {
    let stmt = txn
        .prepare_cached(&format!(
            "SELECT {COLUMNS} FROM tenant_connector_config WHERE tenant_id = $1"
        ))
        .await?;
    let row = txn.query_opt(&stmt, &[&tenant_id.0]).await?;
    Ok(row.as_ref().map(row_to_config))
}

/// Insert a row for `tenant_id` if and only if no row already exists.
///
/// The bootstrap is idempotent: a second call with a different `seed_content`
/// after a row already exists has no effect. `edited_by` is recorded as
/// `"system"` when called from the bootstrap path.
///
/// Returns `true` if a new row was inserted, `false` if a row already existed.
pub async fn bootstrap_if_absent(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    seed_content: &str,
) -> Result<bool, DBError> {
    let stmt = txn
        .prepare_cached(
            "INSERT INTO tenant_connector_config
                 (tenant_id, content, content_hash, version, edited_at, edited_by)
             VALUES ($1, $2, $3, 1, $4, 'system')
             ON CONFLICT (tenant_id) DO NOTHING",
        )
        .await?;
    let hash = compute_content_hash(seed_content);
    let now: DateTime<Utc> = Utc::now();
    let n = txn
        .execute(&stmt, &[&tenant_id.0, &seed_content, &hash, &now])
        .await?;
    Ok(n == 1)
}

/// Retrieve a tenant's connector config, lazily inserting a bootstrap row
/// from `seed_content` if no row exists yet.
///
/// `seed_content` is typically the contents of
/// `CompilerConfig::connectors_toml_path` (or `""` when unset). The seed
/// is consulted only on the very first call for the tenant; subsequent
/// calls return the row whatever its current content.
pub async fn get_or_bootstrap(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    seed_content: &str,
) -> Result<TenantConnectorConfig, DBError> {
    if let Some(row) = get(txn, tenant_id).await? {
        return Ok(row);
    }
    bootstrap_if_absent(txn, tenant_id, seed_content).await?;
    // Re-read after the insert: another transaction could have raced with
    // ours and inserted a different seed, in which case ON CONFLICT DO
    // NOTHING leaves theirs in place. Either way, the row now exists.
    get(txn, tenant_id)
        .await?
        .ok_or_else(|| panic!("tenant_connector_config row missing after bootstrap_if_absent"))
}

/// Update a tenant's connector config with optimistic concurrency.
///
/// `expected_hash` is the `content_hash` the caller observed on the row
/// when it began the edit (the value the client echoed back via the
/// `If-Match` header). If the row's current hash differs, the update is
/// rejected with [`DBError::OutdatedConnectorsConfigHash`].
///
/// On success the row's `content_hash` is recomputed, `version` is
/// incremented, and `edited_at`/`edited_by` are updated. The new row is
/// returned.
///
/// Note: this function does *not* lazily bootstrap. Call
/// [`get_or_bootstrap`] first to ensure a row exists.
pub async fn put(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    new_content: String,
    edited_by: String,
    expected_hash: &str,
) -> Result<TenantConnectorConfig, DBError> {
    let new_hash = compute_content_hash(&new_content);
    let now: DateTime<Utc> = Utc::now();
    let stmt = txn
        .prepare_cached(&format!(
            "UPDATE tenant_connector_config
                SET content = $1,
                    content_hash = $2,
                    version = version + 1,
                    edited_at = $3,
                    edited_by = $4
              WHERE tenant_id = $5 AND content_hash = $6
              RETURNING {COLUMNS}"
        ))
        .await?;
    let row = txn
        .query_opt(
            &stmt,
            &[
                &new_content,
                &new_hash,
                &now,
                &edited_by,
                &tenant_id.0,
                &expected_hash,
            ],
        )
        .await?;
    if let Some(row) = row {
        return Ok(row_to_config(&row));
    }

    // Update affected zero rows. Either the row does not exist (caller
    // forgot to bootstrap) or the hash did not match. Fetch the current
    // state so the error carries the actual hash for the client.
    let current = get(txn, tenant_id).await?;
    match current {
        Some(c) => Err(DBError::OutdatedConnectorsConfigHash {
            provided_hash: expected_hash.to_string(),
            current_hash: c.content_hash,
        }),
        None => Err(DBError::UnknownTenant { tenant_id }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compute_content_hash_is_sha256_hex() {
        // Known value: sha256("") = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
        assert_eq!(
            compute_content_hash(""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        // Sanity check: differing inputs produce differing hashes.
        assert_ne!(compute_content_hash("a"), compute_content_hash("b"));
        // Whitespace is significant.
        assert_ne!(compute_content_hash("a"), compute_content_hash("a "));
    }
}
