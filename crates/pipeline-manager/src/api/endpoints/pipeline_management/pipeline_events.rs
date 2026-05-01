use crate::api::error::ApiError;
use crate::api::main::ServerState;
use crate::db::storage::Storage;
use crate::db::types::monitor::{
    ExtendedPipelineMonitorEvent, PipelineMonitorEvent, PipelineMonitorEventId,
};
use crate::db::types::program::ProgramStatus;
use crate::db::types::resources_status::{ResourcesDesiredStatus, ResourcesStatus};
use crate::db::types::storage::StorageStatus;
use crate::db::types::tenant::TenantId;
use crate::error::ManagerError;
use actix_web::{
    get,
    http::header::{CacheControl, CacheDirective},
    web::{self, Data as WebData, ReqData},
    HttpResponse,
};
use chrono::{DateTime, Utc};
use feldera_types::error::ErrorResponse;
use feldera_types::runtime_status::{RuntimeDesiredStatus, RuntimeStatus};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

/// Pipeline monitor event information which has a selected subset of optional fields.
/// If an optional field is not selected (i.e., is `None`), it will not be serialized.
#[derive(Serialize, ToSchema, PartialEq, Debug, Clone)]
pub struct PipelineMonitorEventSelectedInfo {
    pub event_id: PipelineMonitorEventId,
    pub recorded_at: DateTime<Utc>,
    pub deployment_resources_status: ResourcesStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deployment_resources_status_details: Option<Option<serde_json::Value>>,
    pub deployment_resources_desired_status: ResourcesDesiredStatus,
    pub deployment_runtime_status: Option<RuntimeStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deployment_runtime_status_details: Option<Option<serde_json::Value>>,
    pub deployment_runtime_desired_status: Option<RuntimeDesiredStatus>,
    pub deployment_has_error: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deployment_error: Option<Option<ErrorResponse>>,
    pub program_status: ProgramStatus,
    pub storage_status: StorageStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_status_details: Option<Option<serde_json::Value>>,
}

impl PipelineMonitorEventSelectedInfo {
    fn new_all(event: ExtendedPipelineMonitorEvent) -> Self {
        PipelineMonitorEventSelectedInfo {
            event_id: event.event_id,
            recorded_at: event.recorded_at,
            deployment_resources_status: event.deployment_resources_status,
            deployment_resources_status_details: Some(event.deployment_resources_status_details),
            deployment_resources_desired_status: event.deployment_resources_desired_status,
            deployment_runtime_status: event.deployment_runtime_status,
            deployment_runtime_status_details: Some(event.deployment_runtime_status_details),
            deployment_runtime_desired_status: event.deployment_runtime_desired_status,
            deployment_has_error: event.deployment_has_error,
            deployment_error: Some(event.deployment_error),
            program_status: event.program_status,
            storage_status: event.storage_status,
            storage_status_details: Some(event.storage_status_details),
        }
    }

    fn new_status(event: PipelineMonitorEvent) -> Self {
        PipelineMonitorEventSelectedInfo {
            event_id: event.event_id,
            recorded_at: event.recorded_at,
            deployment_resources_status: event.deployment_resources_status,
            deployment_resources_status_details: None,
            deployment_resources_desired_status: event.deployment_resources_desired_status,
            deployment_runtime_status: event.deployment_runtime_status,
            deployment_runtime_status_details: None,
            deployment_runtime_desired_status: event.deployment_runtime_desired_status,
            deployment_has_error: event.deployment_has_error,
            deployment_error: None,
            program_status: event.program_status,
            storage_status: event.storage_status,
            storage_status_details: None,
        }
    }
}

#[derive(Deserialize, Serialize, ToSchema, Eq, PartialEq, Debug, Clone, Default)]
#[serde(rename_all = "snake_case")]
pub enum PipelineMonitorEventFieldSelector {
    /// Select all fields of a pipeline monitor event, including all details.
    All,
    /// Select only the fields required to know the status of a pipeline monitor event.
    /// This excludes details.
    #[default]
    Status,
}

/// Query parameters to GET a pipeline monitor event.
#[derive(Debug, Deserialize, IntoParams, ToSchema, Default)]
pub struct GetPipelineEventParameters {
    /// The `selector` parameter limits which fields are returned.
    /// Limiting which fields is particularly handy for instance when frequently
    /// monitoring over low bandwidth connections while being only interested
    /// in status.
    #[serde(default)]
    selector: PipelineMonitorEventFieldSelector,
}

/// List Pipeline Events
///
/// Retrieve monitoring events in reverse chronological order.
///
/// Pipeline health is monitored regularly every several seconds.
/// Not every monitoring action results in a pipeline monitor event being
/// constructed and inserted into the database. This happens if:
/// - Any status changed
/// - Only the status details changed, and it has been 10s since the last event
/// - Nothing has changed for more than 10 minutes
///
/// This endpoint returns the most recent persisted events, up to by default approximately 720.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(
        ("pipeline_name" = String, Path, description = "Unique pipeline name"),
        GetPipelineEventParameters
    ),
    responses(
        (status = OK, body = [PipelineMonitorEventSelectedInfo]),
        (status = NOT_FOUND, body = ErrorResponse),
        (status = INTERNAL_SERVER_ERROR, body = ErrorResponse),
    ),
    tag = "Metrics & Debugging"
)]
#[get("/pipelines/{pipeline_name}/events")]
pub(crate) async fn list_pipeline_events(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    path: web::Path<String>,
    query: web::Query<GetPipelineEventParameters>,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_name = path.into_inner();
    let selector = &query.selector;
    let events: Vec<PipelineMonitorEventSelectedInfo> = match selector {
        PipelineMonitorEventFieldSelector::All => state
            .db
            .lock()
            .await
            .list_pipeline_monitor_events_extended(*tenant_id, pipeline_name)
            .await?
            .into_iter()
            .map(PipelineMonitorEventSelectedInfo::new_all)
            .collect(),
        PipelineMonitorEventFieldSelector::Status => state
            .db
            .lock()
            .await
            .list_pipeline_monitor_events_short(*tenant_id, pipeline_name)
            .await?
            .into_iter()
            .map(PipelineMonitorEventSelectedInfo::new_status)
            .collect(),
    };
    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(events))
}

/// Get Pipeline Event
///
/// Get a specific pipeline monitor event.
///
/// The identifiers of the events can be retrieved via `GET /v0/pipelines/<pipeline>/events`.
/// The most recent approximately 720 (default) events are retained.
/// This endpoint can return a 404 for an event that no longer exists due to a cleanup.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(
        ("event_id" = String, Path, description = "Pipeline monitor event identifier or `latest`"),
        ("pipeline_name" = String, Path, description = "Unique pipeline name"),
        GetPipelineEventParameters
    ),
    responses(
        (status = OK, body = PipelineMonitorEventSelectedInfo),
        (status = BAD_REQUEST, body = ErrorResponse),
        (status = NOT_FOUND, body = ErrorResponse),
        (status = INTERNAL_SERVER_ERROR, body = ErrorResponse),
    ),
    tag = "Metrics & Debugging"
)]
#[get("/pipelines/{pipeline_name}/events/{event_id}")]
pub(crate) async fn get_pipeline_event(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    path: web::Path<(String, String)>,
    query: web::Query<GetPipelineEventParameters>,
) -> Result<HttpResponse, ManagerError> {
    let (pipeline_name, event_id) = path.into_inner();
    let selector = &query.selector;
    let event = if event_id == "latest" {
        match selector {
            PipelineMonitorEventFieldSelector::All => PipelineMonitorEventSelectedInfo::new_all(
                state
                    .db
                    .lock()
                    .await
                    .get_latest_pipeline_monitor_event_extended(*tenant_id, pipeline_name)
                    .await?,
            ),
            PipelineMonitorEventFieldSelector::Status => {
                PipelineMonitorEventSelectedInfo::new_status(
                    state
                        .db
                        .lock()
                        .await
                        .get_latest_pipeline_monitor_event_short(*tenant_id, pipeline_name)
                        .await?,
                )
            }
        }
    } else {
        let event_id = PipelineMonitorEventId(Uuid::from_str(&event_id).map_err(|e| {
            ApiError::InvalidUuidParam {
                value: event_id.clone(),
                error: e.to_string(),
            }
        })?);
        match selector {
            PipelineMonitorEventFieldSelector::All => PipelineMonitorEventSelectedInfo::new_all(
                state
                    .db
                    .lock()
                    .await
                    .get_pipeline_monitor_event_extended(*tenant_id, pipeline_name, event_id)
                    .await?,
            ),
            PipelineMonitorEventFieldSelector::Status => {
                PipelineMonitorEventSelectedInfo::new_status(
                    state
                        .db
                        .lock()
                        .await
                        .get_pipeline_monitor_event_short(*tenant_id, pipeline_name, event_id)
                        .await?,
                )
            }
        }
    };
    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(&event))
}
