use crate::db::storage::Storage;
use crate::db::types::monitor::{
    ClusterMonitorEvent, ClusterMonitorEventId, ExtendedClusterMonitorEvent, MonitorStatus,
};
use crate::{
    api::{error::ApiError, main::ServerState},
    error::ManagerError,
};
use actix_web::http::header::{CacheControl, CacheDirective};
use actix_web::{HttpResponse, get, web, web::Data as WebData};
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

/// List Cluster Events
///
/// Retrieve a list of retained cluster monitor events ordered from most recent to least recent.
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

/// Get Cluster Event
///
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

/// Health of the cluster as a whole and of each of its services.
#[derive(Debug, Clone, Serialize, PartialEq, ToSchema)]
pub struct HealthStatus {
    /// Whether every service is healthy.
    pub all_healthy: bool,
    /// Health of the API server(s).
    pub api: ServiceStatus,
    /// Health of the compiler server(s).
    pub compiler: ServiceStatus,
    /// Health of the runner(s).
    pub runner: ServiceStatus,
}

/// Health of a single service derived from the retained cluster monitor events.
#[derive(Debug, Clone, Serialize, PartialEq, ToSchema)]
pub struct ServiceStatus {
    /// Whether the service passed its most recent health check.
    pub healthy: bool,
    /// Human-readable report from the most recent health check.
    pub message: String,
    /// Approximate time the service last transitioned between healthy and unhealthy:
    /// the timestamp of the oldest retained consecutive cluster monitor event with the
    /// same `healthy` conclusion. Bounded by event retention.
    pub unchanged_since: DateTime<Utc>,
    /// Timestamp of the most recent cluster monitor event.
    pub checked_at: DateTime<Utc>,
}

/// Timestamp of the oldest event in the run of consecutive events (newest first) that
/// share the health conclusion that `service_status` extracts from the event with
/// `latest_event_id`. The run is bounded by event retention. Returns `None` if that
/// event is not in the list.
///
/// # Example
///
/// ```ignore
/// // Newest first:      e1         e2         e3
/// // recorded_at:       100        90         80
/// // api_status:        Unhealthy  Unhealthy  Healthy
/// let events = [e1, e2, e3];
///
/// // Anchored at e1, the unhealthy run spans e1..=e2: the api service
/// // became unhealthy at t=90.
/// let since = service_unchanged_since(&events, e1.id, |event| event.api_status);
/// assert_eq!(since, Some(e2.recorded_at));
/// ```
fn service_unchanged_since(
    events_newest_first: &[ClusterMonitorEvent],
    latest_event_id: ClusterMonitorEventId,
    service_status: fn(&ClusterMonitorEvent) -> MonitorStatus,
) -> Option<DateTime<Utc>> {
    let mut run = events_newest_first
        .iter()
        .skip_while(|event| event.id != latest_event_id);
    let latest_event = run.next()?;
    let latest_healthy = service_status(latest_event) == MonitorStatus::Healthy;
    let mut unchanged_since = latest_event.recorded_at;
    for event in run {
        if (service_status(event) == MonitorStatus::Healthy) != latest_healthy {
            break;
        }
        unchanged_since = event.recorded_at;
    }
    Some(unchanged_since)
}

/// Check Cluster Health
///
/// Determine the latest cluster health via the latest cluster monitor event.
/// Each service's `unchanged_since` reports the approximate time it last transitioned
/// between healthy and unhealthy, bounded by event retention.
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
    let db = state.db.lock().await;
    let latest_event = db.get_latest_cluster_monitor_event_extended().await?;
    let events = db.list_cluster_monitor_events().await?;
    drop(db);
    let unchanged_since = |service_status| {
        service_unchanged_since(&events, latest_event.id, service_status)
            .unwrap_or(latest_event.recorded_at)
    };
    let api_unchanged_since = unchanged_since(|event: &ClusterMonitorEvent| event.api_status);
    let compiler_unchanged_since =
        unchanged_since(|event: &ClusterMonitorEvent| event.compiler_status);
    let runner_unchanged_since = unchanged_since(|event: &ClusterMonitorEvent| event.runner_status);
    let health_status = HealthStatus {
        all_healthy: latest_event.api_status == MonitorStatus::Healthy
            && latest_event.compiler_status == MonitorStatus::Healthy
            && latest_event.runner_status == MonitorStatus::Healthy,
        api: ServiceStatus {
            healthy: latest_event.api_status == MonitorStatus::Healthy,
            message: latest_event.api_self_info,
            unchanged_since: api_unchanged_since,
            checked_at: latest_event.recorded_at,
        },
        compiler: ServiceStatus {
            healthy: latest_event.compiler_status == MonitorStatus::Healthy,
            message: latest_event.compiler_self_info,
            unchanged_since: compiler_unchanged_since,
            checked_at: latest_event.recorded_at,
        },
        runner: ServiceStatus {
            healthy: latest_event.runner_status == MonitorStatus::Healthy,
            message: latest_event.runner_self_info,
            unchanged_since: runner_unchanged_since,
            checked_at: latest_event.recorded_at,
        },
    };
    if health_status.all_healthy {
        Ok(HttpResponse::Ok().json(health_status))
    } else {
        Ok(HttpResponse::ServiceUnavailable().json(health_status))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(id: u128, seconds: i64, api_status: MonitorStatus) -> ClusterMonitorEvent {
        ClusterMonitorEvent {
            id: ClusterMonitorEventId(Uuid::from_u128(id)),
            recorded_at: DateTime::from_timestamp(seconds, 0).unwrap(),
            api_status,
            compiler_status: MonitorStatus::Healthy,
            runner_status: MonitorStatus::Healthy,
        }
    }

    /// `service_unchanged_since` returns the start of the health run anchored at the
    /// given latest event.
    #[test]
    fn unchanged_since_finds_run_start() {
        let events = vec![
            event(1, 100, MonitorStatus::Unhealthy),
            event(2, 90, MonitorStatus::Unhealthy),
            event(3, 80, MonitorStatus::InitialUnhealthy),
            event(4, 70, MonitorStatus::Healthy),
        ];
        let at = |seconds| DateTime::from_timestamp(seconds, 0).unwrap();
        let api_status = |event: &ClusterMonitorEvent| event.api_status;
        let compiler_status = |event: &ClusterMonitorEvent| event.compiler_status;

        // The unhealthy run spans events 1..=3 (`InitialUnhealthy` counts as unhealthy).
        let since = service_unchanged_since(
            &events,
            ClusterMonitorEventId(Uuid::from_u128(1)),
            api_status,
        );
        assert_eq!(since, Some(at(80)));
        // Anchoring mid-list skips newer events; a single-element run yields its own timestamp.
        let since = service_unchanged_since(
            &events,
            ClusterMonitorEventId(Uuid::from_u128(4)),
            api_status,
        );
        assert_eq!(since, Some(at(70)));
        // A run with no transition extends to the oldest retained event.
        let since = service_unchanged_since(
            &events,
            ClusterMonitorEventId(Uuid::from_u128(1)),
            compiler_status,
        );
        assert_eq!(since, Some(at(70)));
        // An anchor absent from the list yields `None`.
        let since = service_unchanged_since(
            &events,
            ClusterMonitorEventId(Uuid::from_u128(99)),
            api_status,
        );
        assert_eq!(since, None);
    }
}
