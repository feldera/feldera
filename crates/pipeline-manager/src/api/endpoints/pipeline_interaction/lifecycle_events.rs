use actix_web::{
    get,
    web::{self, Data as WebData, ReqData},
    HttpResponse,
};

use crate::{
    api::{examples, main::ServerState},
    db::{storage::Storage, types::tenant::TenantId},
    error::ManagerError,
};

/// Query parameters for lifecycle events endpoint.
#[derive(serde::Deserialize)]
pub struct EventsParameters {
    pub max_events: u32,
}

/// Get lifecycle events for a pipeline.
///
/// Returns a list of lifecycle events for the specified pipeline.
/// The number of events returned is controlled by the `max_events` query parameter.
///
/// # Parameters
/// - `pipeline_name`: Unique pipeline name (path parameter)
/// - `max_events`: Maximum number of events to return (query parameter, required)
///
/// # Returns
/// - 200 OK: List of lifecycle events for the pipeline
/// - 404 NOT_FOUND: Pipeline with that name does not exist
/// - 503 SERVICE_UNAVAILABLE: Disconnected or timeout during response
/// - 500 INTERNAL_SERVER_ERROR: Internal error
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(
        ("pipeline_name" = String, Path, description = "Unique pipeline name"),
        ("max_events" = u32, Query, description = "Maximum number of events to return")
    ),
    responses(
        (status = OK
            , description = "List of lifecycle events for the pipeline"
            , content_type = "application/json"
            , body = Vec<PipelineLifecycleEvent>),
        (status = NOT_FOUND
            , description = "Pipeline with that name does not exist"
            , body = ErrorResponse
            , example = json!(examples::error_unknown_pipeline_name())),
        (status = SERVICE_UNAVAILABLE
            , body = ErrorResponse
            , examples(
                ("Disconnected during response" = (value = json!(examples::error_pipeline_interaction_disconnected()))),
                ("Response timeout" = (value = json!(examples::error_pipeline_interaction_timeout())))
            )
        ),
        (status = INTERNAL_SERVER_ERROR, body = ErrorResponse),
    ),
    tag = "Pipeline interaction"
)]
#[get("/pipelines/{pipeline_name}/lifecycle_events")]
pub(crate) async fn get_pipeline_lifecycle_events(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    path: web::Path<String>,
    query: web::Query<EventsParameters>,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_name = path.into_inner();
    let max_events = query.max_events;

    let events = state
        .db
        .lock()
        .await
        .get_pipeline_lifecycle_events(*tenant_id, &pipeline_name, max_events)
        .await?;

    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .json(events))
}
