use crate::db::error::DBError;
use crate::db::operations::utils::maybe_unique_violation;
use crate::db::types::monitor::{
    ClusterMonitorEvent, ClusterMonitorEventId, ExtendedClusterMonitorEvent, NewClusterMonitorEvent,
};
use chrono::{DateTime, Utc};
use deadpool_postgres::Transaction;
use std::time::Duration;
use tokio_postgres::Row;
use uuid::Uuid;

/// All cluster monitor event columns.
const RETRIEVE_EXTENDED_CLUSTER_EVENT_COLUMNS: &str = "e.id, e.recorded_at,
     e.api_status, e.api_self_info, e.api_resources_info,
     e.compiler_status, e.compiler_self_info, e.compiler_resources_info,
     e.runner_status, e.runner_self_info, e.runner_resources_info";

fn row_to_extended_cluster_monitor_event(
    row: &Row,
) -> Result<ExtendedClusterMonitorEvent, DBError> {
    assert_eq!(row.len(), 11);
    Ok(ExtendedClusterMonitorEvent {
        id: ClusterMonitorEventId(row.get(0)),
        recorded_at: row.get(1),
        api_status: row.get::<_, String>(2).try_into()?,
        api_self_info: row.get(3),
        api_resources_info: row.get(4),
        compiler_status: row.get::<_, String>(5).try_into()?,
        compiler_self_info: row.get(6),
        compiler_resources_info: row.get(7),
        runner_status: row.get::<_, String>(8).try_into()?,
        runner_self_info: row.get(9),
        runner_resources_info: row.get(10),
    })
}

/// Only the status cluster monitor event columns.
const RETRIEVE_SHORT_CLUSTER_EVENT_COLUMNS: &str =
    "e.id, e.recorded_at, e.api_status, e.compiler_status, e.runner_status";

fn row_to_short_cluster_monitor_event(row: &Row) -> Result<ClusterMonitorEvent, DBError> {
    assert_eq!(row.len(), 5);
    Ok(ClusterMonitorEvent {
        id: ClusterMonitorEventId(row.get(0)),
        recorded_at: row.get(1),
        api_status: row.get::<_, String>(2).try_into()?,
        compiler_status: row.get::<_, String>(3).try_into()?,
        runner_status: row.get::<_, String>(4).try_into()?,
    })
}

/// Retrieve specific cluster monitor event (short).
pub(crate) async fn get_cluster_monitor_event_short(
    txn: &Transaction<'_>,
    event_id: ClusterMonitorEventId,
) -> Result<ClusterMonitorEvent, DBError> {
    let stmt = txn
        .prepare_cached(&format!(
            "SELECT {RETRIEVE_SHORT_CLUSTER_EVENT_COLUMNS}
             FROM cluster_monitor_event AS e
             WHERE e.id = $1
            "
        ))
        .await?;
    let row = txn
        .query_opt(&stmt, &[&event_id.0])
        .await?
        .ok_or(DBError::UnknownClusterMonitorEvent { event_id })?;
    row_to_short_cluster_monitor_event(&row)
}

/// Retrieve specific cluster monitor event (extended).
pub(crate) async fn get_cluster_monitor_event_extended(
    txn: &Transaction<'_>,
    event_id: ClusterMonitorEventId,
) -> Result<ExtendedClusterMonitorEvent, DBError> {
    let stmt = txn
        .prepare_cached(&format!(
            "SELECT {RETRIEVE_EXTENDED_CLUSTER_EVENT_COLUMNS}
             FROM cluster_monitor_event AS e
             WHERE e.id = $1
            "
        ))
        .await?;
    let row = txn
        .query_opt(&stmt, &[&event_id.0])
        .await?
        .ok_or(DBError::UnknownClusterMonitorEvent { event_id })?;
    row_to_extended_cluster_monitor_event(&row)
}

/// Retrieve the latest cluster monitor event (short).
pub(crate) async fn get_latest_cluster_monitor_event_short(
    txn: &Transaction<'_>,
) -> Result<ClusterMonitorEvent, DBError> {
    let stmt = txn
        .prepare_cached(&format!(
            "SELECT {RETRIEVE_SHORT_CLUSTER_EVENT_COLUMNS}
             FROM cluster_monitor_event AS e
             ORDER BY e.recorded_at DESC, e.id DESC
             LIMIT 1
            "
        ))
        .await?;
    let row = txn
        .query_opt(&stmt, &[])
        .await?
        .ok_or(DBError::NoClusterMonitorEventsAvailable)?;
    row_to_short_cluster_monitor_event(&row)
}

/// Retrieve the latest cluster monitor event (extended).
pub(crate) async fn get_latest_cluster_monitor_event_extended(
    txn: &Transaction<'_>,
) -> Result<ExtendedClusterMonitorEvent, DBError> {
    let stmt = txn
        .prepare_cached(&format!(
            "SELECT {RETRIEVE_EXTENDED_CLUSTER_EVENT_COLUMNS}
             FROM cluster_monitor_event AS e
             ORDER BY e.recorded_at DESC, e.id DESC
             LIMIT 1
            "
        ))
        .await?;
    let row = txn
        .query_opt(&stmt, &[])
        .await?
        .ok_or(DBError::NoClusterMonitorEventsAvailable)?;
    row_to_extended_cluster_monitor_event(&row)
}

/// Retrieve all cluster monitor events (short).
pub(crate) async fn list_cluster_monitor_events_short(
    txn: &Transaction<'_>,
) -> Result<Vec<ClusterMonitorEvent>, DBError> {
    let stmt = txn
        .prepare_cached(&format!(
            "SELECT {RETRIEVE_SHORT_CLUSTER_EVENT_COLUMNS}
             FROM cluster_monitor_event AS e
             ORDER BY e.recorded_at DESC, e.id DESC
            "
        ))
        .await?;
    let rows: Vec<Row> = txn.query(&stmt, &[]).await?;
    let mut result = Vec::with_capacity(rows.len());
    for row in rows {
        result.push(row_to_short_cluster_monitor_event(&row)?);
    }
    Ok(result)
}

/// Creates a new monitoring event.
pub(crate) async fn new_cluster_monitor_event(
    txn: &Transaction<'_>,
    new_id: Uuid,
    event_descr: NewClusterMonitorEvent,
) -> Result<(), DBError> {
    let stmt = txn
        .prepare_cached(
            "INSERT INTO cluster_monitor_event
                (
                    id,
                    api_status, api_self_info, api_resources_info,
                    compiler_status, compiler_self_info, compiler_resources_info,
                    runner_status, runner_self_info, runner_resources_info
                )
                VALUES
                (
                    $1,
                    $2, $3, $4,
                    $5, $6, $7,
                    $8, $9, $10
                )",
        )
        .await?;
    txn.execute(
        &stmt,
        &[
            &new_id,                                  // $1: id
            &event_descr.api_status.to_string(),      // $2: api_status
            &event_descr.api_self_info,               // $3: api_self_info
            &event_descr.api_resources_info,          // $4: api_resources_info
            &event_descr.compiler_status.to_string(), // $5: compiler_status
            &event_descr.compiler_self_info,          // $6: compiler_self_info
            &event_descr.compiler_resources_info,     // $7: compiler_resources_info
            &event_descr.runner_status.to_string(),   // $8: runner_status
            &event_descr.runner_self_info,            // $9: runner_self_info
            &event_descr.runner_resources_info,       // $10: runner_resources_info
        ],
    )
    .await
    .map_err(maybe_unique_violation)?;
    Ok(())
}

/// Deletes from table `cluster_monitor_event` all events that are either:
/// - ... were recorded earlier than the retention time.
/// - ... at most the `n` most recent events are kept.
///
/// If the latest event exists, it is exempted from the cleanup.
///
/// Returns 2-tuple with (number of rows deleted due to timestamp, number of rows deleted due
/// to numerical limit).
pub(crate) async fn delete_cluster_monitor_events_beyond_retention(
    txn: &Transaction<'_>,
    retention_hours: u16,
    retention_num: u16,
) -> Result<(u64, u64), DBError> {
    // Identifier of the latest cluster monitor event, such that it can be excluded from deletion
    let stmt = txn
        .prepare_cached(
            "SELECT e.id
             FROM cluster_monitor_event AS e
             ORDER BY e.recorded_at DESC, e.id DESC
             LIMIT 1
            ",
        )
        .await?;
    let latest_event_id = txn
        .query_opt(&stmt, &[])
        .await?
        .map(|row| ClusterMonitorEventId(row.get(0)));

    // Current timestamp -- workaround for not being able to supply an $hours argument into INTERVAL
    let stmt = txn
        .prepare_cached(
            "SELECT NOW()
            ",
        )
        .await?;
    let now_ts: DateTime<Utc> = txn.query_one(&stmt, &[]).await?.get(0);
    let retain_threshold = now_ts - Duration::from_secs((retention_hours as u64) * 3600);

    // Time-based retention
    let stmt = txn
        .prepare_cached(
            "DELETE FROM cluster_monitor_event AS e \
                   WHERE e.recorded_at < $1 AND (CASE WHEN $2 THEN TRUE ELSE e.id != $3 END)",
        )
        .await?;
    let num_deleted_due_to_timestamp = txn
        .execute(
            &stmt,
            &[
                &retain_threshold,
                &latest_event_id.is_none(),
                &latest_event_id.map(|v| v.0),
            ],
        )
        .await?;

    // Number-based retention
    let stmt = txn
        .prepare_cached(
            "DELETE FROM cluster_monitor_event AS e1 \
                   WHERE e1.id NOT IN ( \
                          SELECT e2.id \
                            FROM cluster_monitor_event AS e2 \
                        ORDER BY e2.recorded_at DESC, e2.id DESC \
                           LIMIT $1 \
                   ) AND (CASE WHEN $2 THEN TRUE ELSE e1.id != $3 END)
            ",
        )
        .await?;
    let num_deleted_due_to_limit = txn
        .execute(
            &stmt,
            &[
                &(retention_num as i64),
                &latest_event_id.is_none(),
                &latest_event_id.map(|v| v.0),
            ],
        )
        .await?;

    // Return numbers of events deleted
    Ok((num_deleted_due_to_timestamp, num_deleted_due_to_limit))
}
