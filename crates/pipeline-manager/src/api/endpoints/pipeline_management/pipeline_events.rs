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
use feldera_types::runtime_status::{RuntimeDesiredStatus, RuntimeStatus};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

/// Pipeline monitor event information which has a selected subset of optional fields.
/// If an optional field is not selected (i.e., is `None`), it will not be serialized.
#[derive(Serialize, ToSchema, PartialEq, Debug, Clone)]
pub struct PipelineMonitorEventSelectedInfo {
    pub id: PipelineMonitorEventId,
    pub recorded_at: DateTime<Utc>,
    pub resources_status: ResourcesStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resources_status_details: Option<serde_json::Value>,
    pub resources_desired_status: ResourcesDesiredStatus,
    pub runtime_status: Option<RuntimeStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime_status_details: Option<Option<serde_json::Value>>,
    pub runtime_desired_status: Option<RuntimeDesiredStatus>,
    pub program_status: ProgramStatus,
    pub storage_status: StorageStatus,
}

impl PipelineMonitorEventSelectedInfo {
    fn new_all(event: ExtendedPipelineMonitorEvent) -> Self {
        PipelineMonitorEventSelectedInfo {
            id: event.id,
            recorded_at: event.recorded_at,
            resources_status: event.resources_status,
            resources_status_details: Some(event.resources_status_details),
            resources_desired_status: event.resources_desired_status,
            runtime_status: event.runtime_status,
            runtime_status_details: Some(event.runtime_status_details),
            runtime_desired_status: event.runtime_desired_status,
            program_status: event.program_status,
            storage_status: event.storage_status,
        }
    }

    fn new_status(event: PipelineMonitorEvent) -> Self {
        PipelineMonitorEventSelectedInfo {
            id: event.id,
            recorded_at: event.recorded_at,
            resources_status: event.resources_status,
            resources_status_details: None,
            resources_desired_status: event.resources_desired_status,
            runtime_status: event.runtime_status,
            runtime_status_details: None,
            runtime_desired_status: event.runtime_desired_status,
            program_status: event.program_status,
            storage_status: event.storage_status,
        }
    }
}

#[derive(Deserialize, Serialize, ToSchema, Eq, PartialEq, Debug, Clone, Default)]
#[serde(rename_all = "snake_case")]
pub enum PipelineMonitorEventFieldSelector {
    /// Select all fields of a pipeline monitor event.
    All,
    /// Select only the fields required to know the status of a pipeline monitor event.
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
/// Retrieve a list of retained pipeline monitor events ordered from most recent to least recent.
///
/// The returned events only have limited details, the full details can be retrieved using
/// the `GET /v0/pipelines/<pipeline>/events/<event-id>` endpoint.
///
/// Pipeline monitor events are collected at a periodic interval (every 10s), however only
/// every 10 minutes or if the overall health changes, does it get inserted into the database
/// (and thus, served by this endpoint). The most recent 720 events are retained.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    responses(
        (status = OK, body = [PipelineMonitorEventSelectedInfo]),
        (status = NOT_IMPLEMENTED, body = ErrorResponse),
        (status = INTERNAL_SERVER_ERROR, body = ErrorResponse),
    ),
    tag = "Metrics & Debugging"
)]
#[get("/pipelines/{pipeline_name}/events")]
pub(crate) async fn list_pipeline_events(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    path: web::Path<String>,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_name = path.into_inner();
    let events: Vec<PipelineMonitorEventSelectedInfo> = state
        .db
        .lock()
        .await
        .list_pipeline_monitor_events(*tenant_id, pipeline_name)
        .await?
        .into_iter()
        .map(PipelineMonitorEventSelectedInfo::new_status)
        .collect();
    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(events))
}

/// Get Pipeline Event
///
/// Get specific pipeline monitor event.
///
/// The identifiers of the events can be retrieved via `GET /v0/pipelines/<pipeline>/events`.
/// The most recent 720 events are retained.
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
