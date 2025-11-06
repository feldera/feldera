use crate::api::main::ServerState;
use crate::db::{storage::Storage as _, types::tenant::TenantId};
use crate::error::ManagerError;
use actix_web::{
    get,
    http::Method,
    web::{Data as WebData, ReqData},
    HttpResponse,
};
use awc::body::MessageBody as _;
use feldera_types::runtime_status::RuntimeStatus;

/// List All Metrics
///
/// Retrieve the metrics of all running pipelines belonging to this tenant.
///
/// The metrics are collected by making individual HTTP requests to `/metrics`
/// endpoint of each pipeline, of which only successful responses are included
/// in the returned list.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    responses(
        (status = OK
        , description = "Metrics of all running pipelines belonging to this tenant in Prometheus format"
        , content_type = "text/plain"
        , body = Vec<u8>),
    ),
    tag = "Metrics & Debugging"
)]
#[get("/metrics")]
pub(crate) async fn get_metrics(
    state: WebData<ServerState>,
    client: WebData<awc::Client>,
    tenant_id: ReqData<TenantId>,
) -> Result<HttpResponse, ManagerError> {
    let pipelines = state
        .db
        .lock()
        .await
        .list_pipelines_for_monitoring(*tenant_id)
        .await?;

    const NEWLINE: u8 = b'\n';
    let mut result = Vec::new();

    for pipeline in pipelines {
        if pipeline.deployment_runtime_status == Some(RuntimeStatus::Running)
            || pipeline.deployment_runtime_status == Some(RuntimeStatus::Paused)
        {
            if let Ok(res) = state
                .runner
                .forward_http_request_to_pipeline_by_name(
                    client.as_ref(),
                    *tenant_id,
                    &pipeline.name,
                    Method::GET,
                    "metrics",
                    "",
                    None,
                )
                .await
            {
                if res.status().is_success() {
                    if let Ok(bytes) = res.into_body().try_into_bytes() {
                        result.extend(bytes);
                        result.push(NEWLINE);
                    }
                }
            }
        }
    }

    Ok(HttpResponse::Ok().content_type("text/plain").body(result))
}
