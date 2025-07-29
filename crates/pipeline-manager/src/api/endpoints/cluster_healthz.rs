use actix_web::{get, web::Data as WebData, HttpResponse, Responder};
use chrono::Utc;

use crate::{
    api::main::ServerState,
    cluster_health::{HealthStatus, ServiceStatus},
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
pub(crate) async fn get_health(state: WebData<ServerState>) -> impl Responder {
    let status_opt = state.health_check.read().await;
    match &*status_opt {
        Some(status) => {
            if status.runner.healthy && status.compiler.healthy {
                HttpResponse::Ok().json(status)
            } else {
                HttpResponse::ServiceUnavailable().json(status)
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
            HttpResponse::ServiceUnavailable().json(unavailable_status)
        }
    }
}
