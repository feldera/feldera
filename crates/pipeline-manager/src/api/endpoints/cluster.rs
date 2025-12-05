use crate::db::storage::Storage;
use crate::db::types::monitor::{
    ClusterMonitorEvent, ClusterMonitorEventId, ExtendedClusterMonitorEvent, MonitorStatus,
};
use crate::{
    api::{error::ApiError, main::ServerState},
    error::ManagerError,
};
use actix_web::http::header::{CacheControl, CacheDirective};
use actix_web::{get, web, web::Data as WebData, HttpResponse};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

/// Cluster monitor event information which has a selected subset of optional fields.
/// If an optional field is not selected (i.e., is `None`), it will not be serialized.
#[derive(Serialize, ToSchema, PartialEq, Debug, Clone)]
pub struct ClusterMonitorEventSelectedInfo {
    pub id: ClusterMonitorEventId,
    pub recorded_at: DateTime<Utc>,
    pub all_healthy: bool,
    pub api_status: MonitorStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_self_info: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_resources_info: Option<String>,
    pub compiler_status: MonitorStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compiler_self_info: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compiler_resources_info: Option<String>,
    pub runner_status: MonitorStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runner_self_info: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runner_resources_info: Option<String>,
}

impl ClusterMonitorEventSelectedInfo {
    fn new_all(event: ExtendedClusterMonitorEvent) -> Self {
        ClusterMonitorEventSelectedInfo {
            id: event.id,
            recorded_at: event.recorded_at,
            all_healthy: (event.api_status, event.compiler_status, event.runner_status)
                == (
                    MonitorStatus::Healthy,
                    MonitorStatus::Healthy,
                    MonitorStatus::Healthy,
                ),
            api_status: event.api_status,
            api_self_info: Some(event.api_self_info),
            api_resources_info: Some(event.api_resources_info),
            compiler_status: event.compiler_status,
            compiler_self_info: Some(event.compiler_self_info),
            compiler_resources_info: Some(event.compiler_resources_info),
            runner_status: event.runner_status,
            runner_self_info: Some(event.runner_self_info),
            runner_resources_info: Some(event.runner_resources_info),
        }
    }

    fn new_status(event: ClusterMonitorEvent) -> Self {
        ClusterMonitorEventSelectedInfo {
            id: event.id,
            recorded_at: event.recorded_at,
            all_healthy: (event.api_status, event.compiler_status, event.runner_status)
                == (
                    MonitorStatus::Healthy,
                    MonitorStatus::Healthy,
                    MonitorStatus::Healthy,
                ),
            api_status: event.api_status,
            api_self_info: None,
            api_resources_info: None,
            compiler_status: event.compiler_status,
            compiler_self_info: None,
            compiler_resources_info: None,
            runner_status: event.runner_status,
            runner_self_info: None,
            runner_resources_info: None,
        }
    }
}

#[derive(Deserialize, Serialize, ToSchema, Eq, PartialEq, Debug, Clone, Default)]
#[serde(rename_all = "snake_case")]
pub enum ClusterMonitorEventFieldSelector {
    /// Select all fields of a cluster monitor event.
    All,
    /// Select only the fields required to know the status of a cluster monitor event.
    #[default]
    Status,
}

/// Query parameters to GET a cluster monitor event.
#[derive(Debug, Deserialize, IntoParams, ToSchema, Default)]
pub struct GetClusterEventParameters {
    /// The `selector` parameter limits which fields are returned.
    /// Limiting which fields is particularly handy for instance when frequently
    /// monitoring over low bandwidth connections while being only interested
    /// in status.
    #[serde(default)]
    selector: ClusterMonitorEventFieldSelector,
}

/// List of retained cluster monitor events ordered from most recent to least recent.
///
/// The returned events only have limited details, the full details can be retrieved using
/// the `GET /v0/cluster/events/<event-id>` endpoint.
///
/// Cluster monitor events are collected at a periodic interval (every 10s), however only
/// every 10 minutes or if the overall health changes, does it get inserted into the database
/// (and thus, served by this endpoint). At most 1000 events are retained (newest first),
/// and events older than 72h are deleted. The latest event, if it already exists, is never
/// cleaned up.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    responses(
        (status = OK, body = [ClusterMonitorEventSelectedInfo]),
        (status = NOT_IMPLEMENTED, body = ErrorResponse),
        (status = INTERNAL_SERVER_ERROR, body = ErrorResponse),
    ),
    tag = "Platform"
)]
#[get("/cluster/events")]
pub(crate) async fn list_cluster_events(
    state: WebData<ServerState>,
) -> Result<HttpResponse, ManagerError> {
    let events: Vec<ClusterMonitorEventSelectedInfo> = state
        .db
        .lock()
        .await
        .list_cluster_monitor_events()
        .await?
        .into_iter()
        .map(ClusterMonitorEventSelectedInfo::new_status)
        .collect();
    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(events))
}

/// Get specific cluster monitor event.
///
/// The identifiers of the events can be retrieved via `GET /v0/cluster/events`.
/// At most 1000 events are retained (newest first), and events older than 72h are deleted.
/// The latest event, if it already exists, is never cleaned up.
/// This endpoint can return a 404 for an event that no longer exists due to clean-up.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(
        ("event_id" = String, Path, description = "Cluster monitor event identifier or `latest`"),
        GetClusterEventParameters
    ),
    responses(
        (status = OK, body = ClusterMonitorEventSelectedInfo),
        (status = NOT_FOUND, body = ErrorResponse),
        (status = NOT_IMPLEMENTED, body = ErrorResponse),
        (status = INTERNAL_SERVER_ERROR, body = ErrorResponse),
    ),
    tag = "Platform"
)]
#[get("/cluster/events/{event_id}")]
pub(crate) async fn get_cluster_event(
    state: WebData<ServerState>,
    path: web::Path<String>,
    query: web::Query<GetClusterEventParameters>,
) -> Result<HttpResponse, ManagerError> {
    let event_id = path.into_inner();
    let selector = &query.selector;
    let event = if event_id == "latest" {
        match selector {
            ClusterMonitorEventFieldSelector::All => ClusterMonitorEventSelectedInfo::new_all(
                state
                    .db
                    .lock()
                    .await
                    .get_latest_cluster_monitor_event_extended()
                    .await?,
            ),
            ClusterMonitorEventFieldSelector::Status => {
                ClusterMonitorEventSelectedInfo::new_status(
                    state
                        .db
                        .lock()
                        .await
                        .get_latest_cluster_monitor_event_short()
                        .await?,
                )
            }
        }
    } else {
        let event_id = ClusterMonitorEventId(Uuid::from_str(&event_id).map_err(|e| {
            ApiError::InvalidUuidParam {
                value: event_id.clone(),
                error: e.to_string(),
            }
        })?);
        match selector {
            ClusterMonitorEventFieldSelector::All => ClusterMonitorEventSelectedInfo::new_all(
                state
                    .db
                    .lock()
                    .await
                    .get_cluster_monitor_event_extended(event_id)
                    .await?,
            ),
            ClusterMonitorEventFieldSelector::Status => {
                ClusterMonitorEventSelectedInfo::new_status(
                    state
                        .db
                        .lock()
                        .await
                        .get_cluster_monitor_event_short(event_id)
                        .await?,
                )
            }
        }
    };
    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(&event))
}

#[derive(Debug, Clone, Serialize, PartialEq, ToSchema)]
pub struct HealthStatus {
    pub all_healthy: bool,
    pub api: ServiceStatus,
    pub compiler: ServiceStatus,
    pub runner: ServiceStatus,
}

#[derive(Debug, Clone, Serialize, PartialEq, ToSchema)]
pub struct ServiceStatus {
    pub healthy: bool,
    pub message: String,
    pub unchanged_since: DateTime<Utc>,
    pub checked_at: DateTime<Utc>,
}

/// Check Cluster Health
///
/// Determine the latest cluster health via the latest cluster monitor event.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    responses(
        (status = 200, description = "All services healthy", body = HealthStatus),
        (status = 503, description = "One or more services unhealthy", body = HealthStatus)
    ),
    tag = "Platform"
)]
#[get("/cluster_healthz")]
pub(crate) async fn get_cluster_health(
    state: WebData<ServerState>,
) -> Result<HttpResponse, ManagerError> {
    let latest_event = state
        .db
        .lock()
        .await
        .get_latest_cluster_monitor_event_extended()
        .await?;
    let health_status = HealthStatus {
        all_healthy: latest_event.api_status == MonitorStatus::Healthy
            && latest_event.compiler_status == MonitorStatus::Healthy
            && latest_event.runner_status == MonitorStatus::Healthy,
        api: ServiceStatus {
            healthy: latest_event.api_status == MonitorStatus::Healthy,
            message: latest_event.api_self_info,
            unchanged_since: latest_event.recorded_at,
            checked_at: latest_event.recorded_at,
        },
        compiler: ServiceStatus {
            healthy: latest_event.compiler_status == MonitorStatus::Healthy,
            message: latest_event.compiler_self_info,
            unchanged_since: latest_event.recorded_at,
            checked_at: latest_event.recorded_at,
        },
        runner: ServiceStatus {
            healthy: latest_event.runner_status == MonitorStatus::Healthy,
            message: latest_event.runner_self_info,
            unchanged_since: latest_event.recorded_at,
            checked_at: latest_event.recorded_at,
        },
    };
    if health_status.api.healthy && health_status.compiler.healthy && health_status.runner.healthy {
        Ok(HttpResponse::Ok().json(health_status))
    } else {
        Ok(HttpResponse::ServiceUnavailable().json(health_status))
    }
}
