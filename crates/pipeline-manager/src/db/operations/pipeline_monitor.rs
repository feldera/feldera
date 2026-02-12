use crate::db::error::DBError;
use crate::db::operations::pipeline::{
    deserialize_json_value, get_pipeline_by_id, get_pipeline_by_name_for_monitoring,
};
use crate::db::operations::utils::maybe_unique_violation;
use crate::db::types::monitor::{
    ExtendedPipelineMonitorEvent, PipelineMonitorEvent, PipelineMonitorEventId,
};
use crate::db::types::pipeline::{
    parse_string_as_runtime_desired_status, parse_string_as_runtime_status,
    runtime_desired_status_to_string, runtime_status_to_string, PipelineId,
};
use crate::db::types::tenant::TenantId;
use deadpool_postgres::Transaction;
use serde_json::json;
use tokio_postgres::Row;
use uuid::Uuid;

/// All pipeline monitor event columns.
const RETRIEVE_EXTENDED_PIPELINE_EVENT_COLUMNS: &str = "e.id, e.recorded_at,
     e.resources_status, e.resources_status_details, e.resources_desired_status,
     e.runtime_status, e.runtime_status_details, e.runtime_desired_status,
     e.program_status, e.storage_status";

pub const MONITOR_PIPELINE_RETENTION_NUM: u16 = 720;

fn row_to_extended_pipeline_monitor_event(
    row: &Row,
) -> Result<ExtendedPipelineMonitorEvent, DBError> {
    assert_eq!(row.len(), 10);
    let runtime_status = match row.get::<_, Option<String>>(5) {
        None => None,
        Some(s) => Some(parse_string_as_runtime_status(s)?),
    };
    let runtime_status_details = match row.get::<_, Option<String>>(6) {
        None => None,
        Some(s) => Some(deserialize_json_value(&s)?),
    };
    let runtime_desired_status = match row.get::<_, Option<String>>(7) {
        None => None,
        Some(s) => Some(parse_string_as_runtime_desired_status(s)?),
    };
    Ok(ExtendedPipelineMonitorEvent {
        id: PipelineMonitorEventId(row.get(0)),
        recorded_at: row.get(1),
        resources_status: row.get::<_, String>(2).try_into()?,
        resources_status_details: deserialize_json_value(&row.get::<_, String>(3))?,
        resources_desired_status: row.get::<_, String>(4).try_into()?,
        runtime_status,
        runtime_status_details,
        runtime_desired_status,
        program_status: row.get::<_, String>(8).try_into()?,
        storage_status: row.get::<_, String>(9).try_into()?,
    })
}

/// Only the status pipeline monitor event columns.
const RETRIEVE_SHORT_PIPELINE_EVENT_COLUMNS: &str = "e.id, e.recorded_at,
    e.resources_status, e.resources_desired_status,
    e.runtime_status, e.runtime_desired_status,
    e.program_status, e.storage_status";

fn row_to_short_pipeline_monitor_event(row: &Row) -> Result<PipelineMonitorEvent, DBError> {
    assert_eq!(row.len(), 8);
    let runtime_status = match row.get::<_, Option<String>>(4) {
        None => None,
        Some(s) => Some(parse_string_as_runtime_status(s)?),
    };
    let runtime_desired_status = match row.get::<_, Option<String>>(5) {
        None => None,
        Some(s) => Some(parse_string_as_runtime_desired_status(s)?),
    };
    Ok(PipelineMonitorEvent {
        id: PipelineMonitorEventId(row.get(0)),
        recorded_at: row.get(1),
        resources_status: row.get::<_, String>(2).try_into()?,
        resources_desired_status: row.get::<_, String>(3).try_into()?,
        runtime_status,
        runtime_desired_status,
        program_status: row.get::<_, String>(6).try_into()?,
        storage_status: row.get::<_, String>(7).try_into()?,
    })
}

/// Retrieve specific pipeline monitor event (short).
pub(crate) async fn get_pipeline_monitor_event_short(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    pipeline_name: String,
    event_id: PipelineMonitorEventId,
) -> Result<PipelineMonitorEvent, DBError> {
    let pipeline_id = get_pipeline_by_name_for_monitoring(txn, tenant_id, pipeline_name)
        .await?
        .id;

    let stmt = txn
        .prepare_cached(&format!(
            "SELECT {RETRIEVE_SHORT_PIPELINE_EVENT_COLUMNS}
             FROM pipeline_monitor_event AS e, pipeline AS p
             WHERE p.tenant_id = $1 AND p.id = e.pipeline_id AND e.pipeline_id = $2 AND e.id = $3
            "
        ))
        .await?;
    let row = txn
        .query_opt(&stmt, &[&tenant_id.0, &pipeline_id.0, &event_id.0])
        .await?
        .ok_or(DBError::UnknownPipelineMonitorEvent { event_id })?;
    row_to_short_pipeline_monitor_event(&row)
}

/// Retrieve specific pipeline monitor event (extended).
pub(crate) async fn get_pipeline_monitor_event_extended(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    pipeline_name: String,
    event_id: PipelineMonitorEventId,
) -> Result<ExtendedPipelineMonitorEvent, DBError> {
    let pipeline_id = get_pipeline_by_name_for_monitoring(txn, tenant_id, pipeline_name)
        .await?
        .id;

    let stmt = txn
        .prepare_cached(&format!(
            "SELECT {RETRIEVE_EXTENDED_PIPELINE_EVENT_COLUMNS}
             FROM pipeline_monitor_event AS e, pipeline AS p
             WHERE p.tenant_id = $1 AND p.id = e.pipeline_id AND e.pipeline_id = $2 AND e.id = $3
            "
        ))
        .await?;
    let row = txn
        .query_opt(&stmt, &[&tenant_id.0, &pipeline_id.0, &event_id.0])
        .await?
        .ok_or(DBError::UnknownPipelineMonitorEvent { event_id })?;
    row_to_extended_pipeline_monitor_event(&row)
}

/// Retrieve the latest pipeline monitor event (short).
pub(crate) async fn get_latest_pipeline_monitor_event_short(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    pipeline_name: String,
) -> Result<PipelineMonitorEvent, DBError> {
    let pipeline_id = get_pipeline_by_name_for_monitoring(txn, tenant_id, pipeline_name)
        .await?
        .id;

    let stmt = txn
        .prepare_cached(&format!(
            "SELECT {RETRIEVE_SHORT_PIPELINE_EVENT_COLUMNS}
             FROM pipeline_monitor_event AS e, pipeline AS p
             WHERE p.tenant_id = $1 AND p.id = e.pipeline_id AND e.pipeline_id = $2
             ORDER BY e.recorded_at DESC, e.id DESC
             LIMIT 1
            "
        ))
        .await?;
    let row = txn
        .query_opt(&stmt, &[&tenant_id.0, &pipeline_id.0])
        .await?
        .ok_or(DBError::NoPipelineMonitorEventsAvailable)?;
    row_to_short_pipeline_monitor_event(&row)
}

/// Retrieve the latest pipeline monitor event (extended).
pub(crate) async fn get_latest_pipeline_monitor_event_extended(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    pipeline_name: String,
) -> Result<ExtendedPipelineMonitorEvent, DBError> {
    let pipeline_id = get_pipeline_by_name_for_monitoring(txn, tenant_id, pipeline_name)
        .await?
        .id;

    let stmt = txn
        .prepare_cached(&format!(
            "SELECT {RETRIEVE_EXTENDED_PIPELINE_EVENT_COLUMNS}
             FROM pipeline_monitor_event AS e, pipeline AS p
             WHERE p.tenant_id = $1 AND p.id = e.pipeline_id AND e.pipeline_id = $2
             ORDER BY e.recorded_at DESC, e.id DESC
             LIMIT 1
            "
        ))
        .await?;
    let row = txn
        .query_opt(&stmt, &[&tenant_id.0, &pipeline_id.0])
        .await?
        .ok_or(DBError::NoPipelineMonitorEventsAvailable)?;
    row_to_extended_pipeline_monitor_event(&row)
}

/// Retrieve all pipeline monitor events (short).
pub(crate) async fn list_pipeline_monitor_events_short(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    pipeline_name: String,
) -> Result<Vec<PipelineMonitorEvent>, DBError> {
    let pipeline_id = get_pipeline_by_name_for_monitoring(txn, tenant_id, pipeline_name)
        .await?
        .id;

    let stmt = txn
        .prepare_cached(&format!(
            "SELECT {RETRIEVE_SHORT_PIPELINE_EVENT_COLUMNS}
             FROM pipeline_monitor_event AS e, pipeline AS p
             WHERE p.tenant_id = $1 AND p.id = e.pipeline_id AND e.pipeline_id = $2
             ORDER BY e.recorded_at DESC, e.id DESC
            "
        ))
        .await?;
    let rows: Vec<Row> = txn.query(&stmt, &[&tenant_id.0, &pipeline_id.0]).await?;
    let mut result = Vec::with_capacity(rows.len());
    for row in rows {
        result.push(row_to_short_pipeline_monitor_event(&row)?);
    }
    Ok(result)
}

/// Creates a new monitoring event and enforces the limit of number of events.
/// Returns the number of deleted events.
pub(crate) async fn new_pipeline_monitor_event(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    pipeline_id: PipelineId,
    new_id: Uuid,
) -> Result<u64, DBError> {
    let pipeline = get_pipeline_by_id(txn, tenant_id, pipeline_id).await?;

    let stmt = txn
        .prepare_cached(
            "INSERT INTO pipeline_monitor_event
                (
                    id, pipeline_id,
                    resources_status, resources_status_details, resources_desired_status,
                    runtime_status, runtime_status_details, runtime_desired_status,
                    program_status, storage_status
                )
                VALUES
                (
                    $1, $2,
                    $3, $4, $5,
                    $6, $7, $8,
                    $9, $10
                )",
        )
        .await?;
    txn.execute(
        &stmt,
        &[
            &new_id,                                           // $1: id
            &pipeline_id.0,                                    // $2: pipeline_id
            &pipeline.deployment_resources_status.to_string(), // $3: resources_status
            &pipeline
                .deployment_resources_status_details
                .unwrap_or(json!({}))
                .to_string(), // $3: resources_status_details
            &pipeline.deployment_resources_desired_status.to_string(), // $4: resources_desired_status
            &pipeline
                .deployment_runtime_status
                .map(runtime_status_to_string), // $6: runtime_status
            &pipeline
                .deployment_runtime_status_details
                .map(|v| v.to_string()), // $7: runtime_status_details
            &pipeline
                .deployment_runtime_desired_status
                .map(runtime_desired_status_to_string), // $8: runtime_desired_status
            &pipeline.program_status.to_string(),                      // $9: program_status
            &pipeline.storage_status.to_string(),                      // $10: storage_status
        ],
    )
    .await
    .map_err(maybe_unique_violation)?;

    // Number-based retention
    let stmt = txn
        .prepare_cached(
            "DELETE FROM pipeline_monitor_event AS e1 \
                   WHERE e1.id NOT IN ( \
                          SELECT e2.id \
                            FROM pipeline_monitor_event AS e2 \
                        WHERE pipeline_id = $1
                        ORDER BY e2.recorded_at DESC, e2.id DESC \
                           LIMIT $2 \
                   ) AND pipeline_id = $1
            ",
        )
        .await?;
    let num_deleted_due_to_limit = txn
        .execute(
            &stmt,
            &[&pipeline_id.0, &(MONITOR_PIPELINE_RETENTION_NUM as i64)],
        )
        .await?;
    Ok(num_deleted_due_to_limit)
}
