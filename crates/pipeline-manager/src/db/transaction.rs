//! Transaction creation for the management database.
//!
//! Every transaction bounds how long its statements wait for a lock. The API
//! server and the runner both lock pipeline rows with `SELECT ... FOR UPDATE`
//! before acting on them; without a bound, a caller that finds a row locked
//! waits indefinitely instead of being told to retry.
//!
//! The bound is applied with `SET LOCAL` inside each transaction rather than
//! through the connection's `options` startup parameter, because the management
//! database is often reached through a connection pooler:
//!
//! - PgBouncer rejects startup parameters it does not know
//!   (`unsupported startup parameter: options`), which leaves the manager unable
//!   to connect at all. Listing `options` in `ignore_startup_parameters` silences
//!   the rejection but makes PgBouncer drop the setting, so the timeout would
//!   silently not apply.
//! - A session-level `SET` fares no better: in transaction pooling mode the
//!   server connection it lands on is not the one the next transaction gets, so
//!   the setting both misses its target and leaks to an unrelated client.
//!
//! `SET LOCAL` travels with the transaction, which a pooler keeps pinned to one
//! server connection from `BEGIN` to `COMMIT`, and Postgres reverts at commit.
//! It therefore holds for a direct connection and for every pooling mode, at the
//! cost of one round trip per transaction.

use crate::db::error::DBError;
use deadpool_postgres::{Client, Transaction};
use std::sync::LazyLock;
use std::time::Duration;
use tokio_postgres::IsolationLevel;

/// How long a statement waits for a lock before Postgres aborts it with
/// `lock_not_available`, which reaches the caller as [`DBError::LockTookTooLong`]
/// (HTTP code 503: retry). It has to stay at a millisecond or more, as Postgres
/// reads `lock_timeout = 0` as no timeout at all.
pub(crate) const LOCK_TIMEOUT: Duration = Duration::from_secs(10);

/// Statement that applies [`LOCK_TIMEOUT`] to the current transaction.
static SET_LOCK_TIMEOUT: LazyLock<String> =
    LazyLock::new(|| format!("SET LOCAL lock_timeout = {}", LOCK_TIMEOUT.as_millis()));

/// Starts a read-write transaction with a bounded lock wait.
pub(crate) async fn begin(client: &mut Client) -> Result<Transaction<'_>, DBError> {
    let txn = client.transaction().await?;
    set_lock_timeout(&txn).await?;
    Ok(txn)
}

/// Starts a read-only `REPEATABLE READ` transaction with a bounded lock wait.
/// All its statements read the same snapshot, and none of them can write.
pub(crate) async fn begin_read_only(client: &mut Client) -> Result<Transaction<'_>, DBError> {
    let txn = client
        .build_transaction()
        .isolation_level(IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()
        .await?;
    set_lock_timeout(&txn).await?;
    Ok(txn)
}

/// Bounds by [`LOCK_TIMEOUT`] how long each statement of `txn` waits for a lock.
async fn set_lock_timeout(txn: &Transaction<'_>) -> Result<(), DBError> {
    // A simple query, not `execute`, to keep this off the statement cache: a
    // pooler supports prepared statements only in specific configurations.
    txn.batch_execute(&SET_LOCK_TIMEOUT).await?;
    Ok(())
}
