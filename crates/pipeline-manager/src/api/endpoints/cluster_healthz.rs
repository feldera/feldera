use actix_web::{get, web::Data as WebData, HttpResponse};
use chrono::Utc;

use crate::{
    api::{error::ApiError, main::ServerState},
    cluster_health::{HealthStatus, ServiceStatus, STATUS_READ_LOCK_TIMEOUT},
    error::ManagerError,
};

#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    responses(
        (status = 200, description = "All services healthy", body = HealthStatus),
        (status = 503, description = "One or more services unhealthy", body = HealthStatus)
    )
)]
#[get("/cluster_healthz")]
pub(crate) async fn get_health(state: WebData<ServerState>) -> Result<HttpResponse, ManagerError> {
    let status_check =
        match tokio::time::timeout(STATUS_READ_LOCK_TIMEOUT, state.health_check.read()).await {
            Ok(status_check) => status_check.clone(),
            Err(_elapsed) => {
                return Err(ManagerError::from(ApiError::LockTimeout {
                    value: "cluster health status".to_string(),
                    timeout: STATUS_READ_LOCK_TIMEOUT,
                }));
            }
        };

    match status_check {
        Some(status) => {
            if status.runner.healthy && status.compiler.healthy {
                Ok(HttpResponse::Ok().json(status))
            } else {
                Ok(HttpResponse::ServiceUnavailable().json(status))
            }
        }
        None => {
            // Health status not yet available (transient)
            let unavailable_status = HealthStatus {
                runner: ServiceStatus {
                    healthy: false,
                    message: "Health status not available yet".to_string(),
                    unchanged_since: Utc::now(),
                    checked_at: Utc::now(),
                },
                compiler: ServiceStatus {
                    healthy: false,
                    message: "Health status not available yet".to_string(),
                    unchanged_since: Utc::now(),
                    checked_at: Utc::now(),
                },
            };
            Ok(HttpResponse::ServiceUnavailable().json(unavailable_status))
        }
    }
}
