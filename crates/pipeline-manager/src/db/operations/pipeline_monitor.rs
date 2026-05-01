use crate::db::error::DBError;
use crate::db::operations::pipeline::{
    get_pipeline_by_id_for_event_info, get_pipeline_for_monitoring,
};
use crate::db::operations::pipeline_parsing::{
    parse_pipeline_event_row_extended, parse_pipeline_event_row_short, serialize_error_response,
    PIPELINE_EVENT_COLUMNS_ALL, PIPELINE_EVENT_COLUMNS_SHORT,
};
use crate::db::operations::utils::maybe_unique_violation;
use crate::db::types::monitor::{
    ExtendedPipelineMonitorEvent, PipelineMonitorEvent, PipelineMonitorEventId,
};
use crate::db::types::pipeline::{
    runtime_desired_status_to_string, runtime_status_to_string, PipelineId,
};
use crate::db::types::tenant::TenantId;
use deadpool_postgres::Transaction;
use tokio_postgres::Row;
use uuid::Uuid;

/// Retrieve specific pipeline monitor event (short).
pub(crate) async fn get_pipeline_monitor_event_short(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    pipeline_name: String,
    event_id: PipelineMonitorEventId,
) -> Result<PipelineMonitorEvent, DBError> {
    let pipeline_id = get_pipeline_for_monitoring(txn, tenant_id, &pipeline_name)
        .await?
        .id;

    let stmt = txn
        .prepare_cached(&format!(
            "SELECT {PIPELINE_EVENT_COLUMNS_SHORT}
             FROM pipeline_monitor_event AS e, pipeline AS p
             WHERE p.tenant_id = $1 AND p.id = e.pipeline_id AND e.pipeline_id = $2 AND e.event_id = $3
            "
        ))
        .await?;
    let row = txn
        .query_opt(&stmt, &[&tenant_id.0, &pipeline_id.0, &event_id.0])
        .await?
        .ok_or(DBError::UnknownPipelineMonitorEvent { event_id })?;
    parse_pipeline_event_row_short(&row)
}

/// Retrieve specific pipeline monitor event (extended).
pub(crate) async fn get_pipeline_monitor_event_extended(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    pipeline_name: String,
    event_id: PipelineMonitorEventId,
) -> Result<ExtendedPipelineMonitorEvent, DBError> {
    let pipeline_id = get_pipeline_for_monitoring(txn, tenant_id, &pipeline_name)
        .await?
        .id;

    let stmt = txn
        .prepare_cached(&format!(
            "SELECT {PIPELINE_EVENT_COLUMNS_ALL}
             FROM pipeline_monitor_event AS e, pipeline AS p
             WHERE p.tenant_id = $1 AND p.id = e.pipeline_id AND e.pipeline_id = $2 AND e.event_id = $3
            "
        ))
        .await?;
    let row = txn
        .query_opt(&stmt, &[&tenant_id.0, &pipeline_id.0, &event_id.0])
        .await?
        .ok_or(DBError::UnknownPipelineMonitorEvent { event_id })?;
    parse_pipeline_event_row_extended(&row)
}

/// Retrieve the latest pipeline monitor event (short).
pub(crate) async fn get_latest_pipeline_monitor_event_short(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    pipeline_name: String,
) -> Result<PipelineMonitorEvent, DBError> {
    let pipeline_id = get_pipeline_for_monitoring(txn, tenant_id, &pipeline_name)
        .await?
        .id;

    let stmt = txn
        .prepare_cached(&format!(
            "SELECT {PIPELINE_EVENT_COLUMNS_SHORT}
             FROM pipeline_monitor_event AS e, pipeline AS p
             WHERE p.tenant_id = $1 AND p.id = e.pipeline_id AND e.pipeline_id = $2
             ORDER BY e.recorded_at DESC, e.event_id DESC
             LIMIT 1
            "
        ))
        .await?;
    let row = txn
        .query_opt(&stmt, &[&tenant_id.0, &pipeline_id.0])
        .await?
        .ok_or(DBError::NoPipelineMonitorEventsAvailable)?;
    parse_pipeline_event_row_short(&row)
}

/// Retrieve the latest pipeline monitor event (extended).
pub(crate) async fn get_latest_pipeline_monitor_event_extended(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    pipeline_name: String,
) -> Result<ExtendedPipelineMonitorEvent, DBError> {
    let pipeline_id = get_pipeline_for_monitoring(txn, tenant_id, &pipeline_name)
        .await?
        .id;

    let stmt = txn
        .prepare_cached(&format!(
            "SELECT {PIPELINE_EVENT_COLUMNS_ALL}
             FROM pipeline_monitor_event AS e, pipeline AS p
             WHERE p.tenant_id = $1 AND p.id = e.pipeline_id AND e.pipeline_id = $2
             ORDER BY e.recorded_at DESC, e.event_id DESC
             LIMIT 1
            "
        ))
        .await?;
    let row = txn
        .query_opt(&stmt, &[&tenant_id.0, &pipeline_id.0])
        .await?
        .ok_or(DBError::NoPipelineMonitorEventsAvailable)?;
    parse_pipeline_event_row_extended(&row)
}

/// Retrieve all pipeline monitor events (short).
pub(crate) async fn list_pipeline_monitor_events_short(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    pipeline_name: String,
) -> Result<Vec<PipelineMonitorEvent>, DBError> {
    let pipeline_id = get_pipeline_for_monitoring(txn, tenant_id, &pipeline_name)
        .await?
        .id;

    let stmt = txn
        .prepare_cached(&format!(
            "SELECT {PIPELINE_EVENT_COLUMNS_SHORT}
             FROM pipeline_monitor_event AS e, pipeline AS p
             WHERE p.tenant_id = $1 AND p.id = e.pipeline_id AND e.pipeline_id = $2
             ORDER BY e.recorded_at DESC, e.event_id DESC
            "
        ))
        .await?;
    let rows: Vec<Row> = txn.query(&stmt, &[&tenant_id.0, &pipeline_id.0]).await?;
    let mut result = Vec::with_capacity(rows.len());
    for row in rows {
        result.push(parse_pipeline_event_row_short(&row)?);
    }
    Ok(result)
}

/// Retrieve all pipeline monitor events (extended).
pub(crate) async fn list_pipeline_monitor_events_extended(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    pipeline_name: String,
) -> Result<Vec<ExtendedPipelineMonitorEvent>, DBError> {
    let pipeline_id = get_pipeline_for_monitoring(txn, tenant_id, &pipeline_name)
        .await?
        .id;

    let stmt = txn
        .prepare_cached(&format!(
            "SELECT {PIPELINE_EVENT_COLUMNS_ALL}
             FROM pipeline_monitor_event AS e, pipeline AS p
             WHERE p.tenant_id = $1 AND p.id = e.pipeline_id AND e.pipeline_id = $2
             ORDER BY e.recorded_at DESC, e.event_id DESC
            "
        ))
        .await?;
    let rows: Vec<Row> = txn.query(&stmt, &[&tenant_id.0, &pipeline_id.0]).await?;
    let mut result = Vec::with_capacity(rows.len());
    for row in rows {
        result.push(parse_pipeline_event_row_extended(&row)?);
    }
    Ok(result)
}

/// Creates a new pipeline monitoring event.
pub(crate) async fn new_pipeline_monitor_event(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    pipeline_id: PipelineId,
    new_event_id: Uuid,
) -> Result<(), DBError> {
    let pipeline = get_pipeline_by_id_for_event_info(txn, tenant_id, pipeline_id).await?;

    let stmt = txn
        .prepare_cached(
            "INSERT INTO pipeline_monitor_event
                (
                    event_id, pipeline_id,
                    deployment_resources_status, deployment_resources_status_details, deployment_resources_desired_status,
                    deployment_runtime_status, deployment_runtime_status_details, deployment_runtime_desired_status,
                    deployment_error,
                    program_status, storage_status, storage_status_details
                )
                VALUES
                (
                    $1, $2,
                    $3, $4, $5,
                    $6, $7, $8,
                    $9,
                    $10, $11, $12
                )",
        )
        .await?;
    txn.execute(
        &stmt,
        &[
            &new_event_id,                                     // $1: event_id
            &pipeline_id.0,                                    // $2: pipeline_id
            &pipeline.deployment_resources_status.to_string(), // $3: deployment_resources_status
            &pipeline
                .deployment_resources_status_details
                .map(|v| v.to_string()), // $4: deployment_resources_status_details
            &pipeline.deployment_resources_desired_status.to_string(), // $5: deployment_resources_desired_status
            &pipeline
                .deployment_runtime_status
                .map(runtime_status_to_string), // $6: deployment_runtime_status
            &pipeline
                .deployment_runtime_status_details
                .map(|v| v.to_string()), // $7: deployment_runtime_status_details
            &pipeline
                .deployment_runtime_desired_status
                .map(runtime_desired_status_to_string), // $8: deployment_runtime_desired_status
            &match pipeline.deployment_error {
                None => None,
                Some(v) => Some(serialize_error_response(&v)?),
            }, // $9: deployment_error
            &pipeline.program_status.to_string(),                      // $10: program_status
            &pipeline.storage_status.to_string(),                      // $11: storage_status
            &pipeline.storage_status_details.map(|v| v.to_string()), // $12: storage_status_details
        ],
    )
    .await
    .map_err(maybe_unique_violation)?;
    Ok(())
}

/// Deletes pipeline monitor events that exceed retention limits.
/// Returns the number of deleted events.
pub(crate) async fn delete_pipeline_monitor_events_exceeding_retention(
    txn: &Transaction<'_>,
    retention_num: u32,
) -> Result<u64, DBError> {
    // Number-based retention
    let stmt = txn
        .prepare_cached(
            "DELETE FROM pipeline_monitor_event
                   WHERE event_id IN (
                       SELECT event_id
                       FROM (
                           SELECT event_id,
                                  ROW_NUMBER() OVER (PARTITION BY pipeline_id ORDER BY recorded_at DESC, event_id DESC) AS rn
                           FROM pipeline_monitor_event
                       ) AS ranked
                       WHERE rn > $1
                   );
            ",
        )
        .await?;
    let num_deleted_due_to_limit = txn.execute(&stmt, &[&(retention_num as i64)]).await?;
    Ok(num_deleted_due_to_limit)
}
