use crate::db::error::DBError;
use deadpool_postgres::Transaction;

/// Checks that Postgres can be reached by executing a simple query.
pub async fn check_connection(txn: &Transaction<'_>) -> Result<(), DBError> {
    let stmt = txn.prepare_cached("SELECT 1").await?;
    let _res = txn.execute(&stmt, &[]).await?;
    Ok(())
}
