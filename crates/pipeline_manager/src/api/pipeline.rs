use super::ManagerError;
use crate::api::ServerState;
use crate::api::{examples, parse_string_param};
use crate::db::error::DBError;
use crate::db::storage::Storage;
use crate::db::types::pipeline::{ExtendedPipelineDescr, PipelineDescr};
use crate::db::types::program::ProgramConfig;
use crate::db::types::tenant::TenantId;
use actix_web::{
    delete, get,
    http::header::{CacheControl, CacheDirective},
    http::Method,
    patch, post, put,
    web::{self, Data as WebData, ReqData},
    HttpRequest, HttpResponse,
};
use log::{debug, info};
use pipeline_types::config::RuntimeConfig;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

// REGULAR ENDPOINTS

/// Default for the `code` query parameter when GET the list of pipelines.
fn default_list_pipelines_query_parameter_code() -> bool {
    true
}

/// Query parameters for GET the list of pipelines.
#[derive(Debug, Deserialize, IntoParams, ToSchema)]
pub struct ListPipelinesQueryParameters {
    /// Whether to include program code in the response (default: `true`).
    /// Passing `false` reduces the response size, which is particularly handy
    /// when frequently monitoring the endpoint over low bandwidth connections.
    #[serde(default = "default_list_pipelines_query_parameter_code")]
    code: bool,
}

/// Patch (partially) update the pipeline.
///
/// Note that the patching only applies to the main fields, not subfields.
/// For instance, it is not possible to update only the number of workers;
/// it is required to again pass the whole runtime configuration with the
/// change.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct PatchPipeline {
    pub name: Option<String>,
    pub description: Option<String>,
    pub runtime_config: Option<RuntimeConfig>,
    pub program_code: Option<String>,
    pub program_config: Option<ProgramConfig>,
}

/// Retrieve the list of pipelines.
/// Inclusion of program code is configured with by the `code` boolean query parameter.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(ListPipelinesQueryParameters),
    responses(
        (status = OK
            , description = "List of pipelines retrieved successfully"
            , body = [ExtendedPipelineDescr<Option<String>>]
            , example = json!(examples::list_extended_pipeline()))
    ),
    tag = "Pipelines"
)]
#[get("/pipelines")]
pub(crate) async fn list_pipelines(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    query: web::Query<ListPipelinesQueryParameters>,
) -> Result<HttpResponse, DBError> {
    debug!(
        "API: tenant {} requests to GET list of pipelines",
        *tenant_id
    );
    let pipelines = state.db.lock().await.list_pipelines(*tenant_id).await?;
    let pipelines: Vec<ExtendedPipelineDescr<Option<String>>> = pipelines
        .iter()
        .map(|v| ExtendedPipelineDescr {
            id: v.id,
            name: v.name.clone(),
            description: v.description.clone(),
            created_at: v.created_at,
            version: v.version,
            runtime_config: v.runtime_config.clone(),
            program_code: if query.code {
                Some(v.program_code.clone())
            } else {
                None
            },
            program_config: v.program_config.clone(),
            program_version: v.program_version,
            program_status: v.program_status.clone(),
            program_status_since: v.program_status_since,
            program_schema: v.program_schema.clone(),
            program_binary_url: v.program_binary_url.clone(),
            deployment_status: v.deployment_status,
            deployment_status_since: v.deployment_status_since,
            deployment_desired_status: v.deployment_desired_status,
            deployment_error: v.deployment_error.clone(),
            deployment_config: v.deployment_config.clone(),
            deployment_location: v.deployment_location.clone(),
        })
        .collect();
    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(pipelines))
}

/// Retrieve a pipeline.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(
        ("pipeline_name" = String, Path, description = "Unique pipeline name"),
    ),
    responses(
        (status = OK
            , description = "Pipeline retrieved successfully"
            , body = ExtendedPipelineDescr<String>
            , example = json!(examples::extended_pipeline_1())),
        (status = NOT_FOUND
            , description = "Pipeline with that name does not exist"
            , body = ErrorResponse
            , example = json!(examples::error_unknown_pipeline()))
    ),
    tag = "Pipelines"
)]
#[get("/pipelines/{pipeline_name}")]
pub(crate) async fn get_pipeline(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_name = parse_string_param(&req, "pipeline_name")?;
    debug!(
        "API: tenant {} requests to GET pipeline {pipeline_name}",
        *tenant_id
    );
    let pipeline = state
        .db
        .lock()
        .await
        .get_pipeline(*tenant_id, &pipeline_name)
        .await?;
    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(&pipeline))
}

/// Create a new pipeline.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    request_body(
        content = PipelineDescr, example = json!(examples::pipeline_1())
    ),
    responses(
        (status = CREATED
            , description = "Pipeline successfully created"
            , body = ExtendedPipelineDescr<String>
            , example = json!(examples::extended_pipeline_1())),
        (status = NOT_FOUND
            , description = "Pipeline with that name does not exist"
            , body = ErrorResponse
            , example = json!(examples::error_unknown_pipeline()))
    ),
    tag = "Pipelines"
)]
#[post("/pipelines")]
pub(crate) async fn post_pipeline(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    body: web::Json<PipelineDescr>,
) -> Result<HttpResponse, ManagerError> {
    debug!(
        "API: tenant {} requests to POST pipeline {body:?}",
        *tenant_id
    );
    let pipeline = state
        .db
        .lock()
        .await
        .new_pipeline(*tenant_id, Uuid::now_v7(), body.into_inner())
        .await?;

    info!("Created pipeline {} (tenant: {})", pipeline.id, *tenant_id);
    Ok(HttpResponse::Created()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(pipeline))
}

/// Fully update a pipeline if it already exists, otherwise create a new pipeline.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(
        ("pipeline_name" = String, Path, description = "Unique pipeline name"),
    ),
    request_body(
        content = PipelineDescr, example = json!(examples::pipeline_1())
    ),
    responses(
        (status = CREATED
            , description = "Pipeline successfully created"
            , body = ExtendedPipelineDescr<String>
            , example = json!(examples::extended_pipeline_1())),
        (status = OK
            , description = "Pipeline successfully updated"
            , body = ExtendedPipelineDescr<String>
            , example = json!(examples::extended_pipeline_1())),
        (status = CONFLICT
            , description = "Cannot rename pipeline as the name already exists"
            , body = ErrorResponse
            , example = json!(examples::error_duplicate_name())),
        (status = BAD_REQUEST
            , description = "Pipeline is not shutdown"
            , body = ErrorResponse
            , example = json!(examples::error_cannot_update_non_shutdown_pipeline()))
    ),
    tag = "Pipelines"
)]
#[put("/pipelines/{pipeline_name}")]
async fn put_pipeline(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    request: HttpRequest,
    body: web::Json<PipelineDescr>,
) -> Result<HttpResponse, ManagerError> {
    debug!(
        "API: tenant {} requests to PUT pipeline {request:?} {body:?}",
        *tenant_id
    );
    let pipeline_name = parse_string_param(&request, "pipeline_name")?;
    let (is_new, pipeline) = state
        .db
        .lock()
        .await
        .new_or_update_pipeline(
            *tenant_id,
            Uuid::now_v7(),
            &pipeline_name,
            body.into_inner(),
        )
        .await?;
    if is_new {
        info!("Created pipeline {} (tenant: {})", pipeline.id, *tenant_id);
        Ok(HttpResponse::Created()
            .insert_header(CacheControl(vec![CacheDirective::NoCache]))
            .json(pipeline))
    } else {
        info!(
            "Updated pipeline {} to version {} (tenant: {})",
            pipeline.id, pipeline.version, *tenant_id
        );
        Ok(HttpResponse::Ok()
            .insert_header(CacheControl(vec![CacheDirective::NoCache]))
            .json(pipeline))
    }
}

/// Partially update a pipeline.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(
        ("pipeline_name" = String, Path, description = "Unique pipeline name"),
    ),
    request_body(
        content = PatchPipeline, example = json!(examples::patch_pipeline())
    ),
    responses(
        (status = OK
            , description = "Pipeline successfully updated"
            , body = ExtendedPipelineDescr<String>
            , example = json!(examples::extended_pipeline_1())),
        (status = NOT_FOUND
            , description = "Pipeline with that name does not exist"
            , body = ErrorResponse
            , example = json!(examples::error_unknown_pipeline())),
        (status = CONFLICT
            , description = "Cannot rename pipeline as the name already exists"
            , body = ErrorResponse
            , example = json!(examples::error_duplicate_name())),
        (status = BAD_REQUEST
            , description = "Pipeline is not shutdown"
            , body = ErrorResponse
            , example = json!(examples::error_cannot_update_non_shutdown_pipeline()))
    ),
    tag = "Pipelines"
)]
#[patch("/pipelines/{pipeline_name}")]
pub(crate) async fn patch_pipeline(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    request: HttpRequest,
    body: web::Json<PatchPipeline>,
) -> Result<HttpResponse, ManagerError> {
    debug!(
        "API: tenant {} requests to PATCH pipeline {request:?} {body:?}",
        *tenant_id
    );
    let pipeline_name = parse_string_param(&request, "pipeline_name")?;
    let pipeline = state
        .db
        .lock()
        .await
        .update_pipeline(
            *tenant_id,
            &pipeline_name,
            &body.name,
            &body.description,
            &body.runtime_config,
            &body.program_code,
            &body.program_config,
        )
        .await?;

    info!(
        "Updated pipeline {} to version {} (tenant: {})",
        pipeline.id, pipeline.version, *tenant_id
    );
    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(pipeline))
}

/// Delete a pipeline.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(
        ("pipeline_name" = String, Path, description = "Unique pipeline name"),
    ),
    responses(
        (status = OK
            , description = "Pipeline successfully deleted"),
        (status = NOT_FOUND
            , description = "Pipeline with that name does not exist"
            , body = ErrorResponse
            , example = json!(examples::error_unknown_pipeline())),
        (status = BAD_REQUEST
            , description = "Pipeline is not shutdown"
            , body = ErrorResponse
            , example = json!(examples::error_cannot_delete_non_shutdown_pipeline()))
    ),
    tag = "Pipelines"
)]
#[delete("/pipelines/{pipeline_name}")]
pub(crate) async fn delete_pipeline(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    request: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    debug!(
        "API: tenant {} requests to DELETE pipeline {request:?}",
        *tenant_id
    );
    let pipeline_name = parse_string_param(&request, "pipeline_name")?;
    let pipeline_id = state
        .db
        .lock()
        .await
        .delete_pipeline(*tenant_id, &pipeline_name)
        .await?;

    info!("Deleted pipeline {} (tenant: {})", pipeline_id, *tenant_id);
    Ok(HttpResponse::Ok().finish())
}

// SPECIAL ENDPOINTS

/// Parses the action to take on the pipeline.
fn parse_pipeline_action(req: &HttpRequest) -> Result<&str, ManagerError> {
    match req.match_info().get("action") {
        None => Err(ManagerError::MissingUrlEncodedParam { param: "action" }),
        Some(action) => Ok(action),
    }
}

/// Change the desired deployment state of the pipeline.
///
/// The endpoint returns immediately after performing initial request validation
/// (e.g., upon start checking the program is compiled) and initiating the relevant
/// procedure (e.g., informing the runner or the already running pipeline).
/// The state changes completely asynchronously. On error, the pipeline
/// transitions to the `Failed` state. The user can monitor the current status
/// of the pipeline by polling the `GET /pipelines` and
/// `GET /pipelines/{pipeline_name}` endpoint.
///
/// The following values of the `action` argument are accepted:
/// - `start`: Start the pipeline
/// - `pause`: Pause the pipeline
/// - `shutdown`: Terminate the pipeline
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(
        ("pipeline_name" = String, Path, description = "Unique pipeline name"),
        ("action" = String, Path, description = "Pipeline action (one of: start, pause, shutdown)")
    ),
    responses(
        (status = ACCEPTED
            , description = "Action accepted and is being performed"),
        (status = NOT_FOUND
            , description = "Pipeline with that name does not exist"
            , body = ErrorResponse
            , example = json!(examples::error_unknown_pipeline())),
        (status = BAD_REQUEST
            , description = "Unable to accept action"
            , body = ErrorResponse
            , examples(
                ("Program not (yet) compiled" = (description = "Program has not (yet) been compiled", value = json!(examples::error_program_not_yet_compiled()))),
                ("Program failed compilation" = (description = "Program failed compilation", value = json!(examples::error_program_failed_compilation()))),
                ("Illegal action" = (description = "Action is not applicable in the current state", value = json!(examples::error_illegal_pipeline_action()))),
                ("Invalid action" = (description = "Invalid action specified", value = json!(examples::error_invalid_pipeline_action()))),
            )
        ),
    ),
    tag = "Pipelines"
)]
#[post("/pipelines/{pipeline_name}/{action}")]
pub(crate) async fn post_pipeline_action(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    request: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    debug!(
        "API: tenant {} requests to POST pipeline action {request:?}",
        *tenant_id
    );
    let pipeline_name = parse_string_param(&request, "pipeline_name")?;
    let action = parse_pipeline_action(&request)?;
    match action {
        "start" => {
            state
                .db
                .lock()
                .await
                .set_deployment_desired_status_running(*tenant_id, &pipeline_name)
                .await?
        }
        "pause" => {
            state
                .db
                .lock()
                .await
                .set_deployment_desired_status_paused(*tenant_id, &pipeline_name)
                .await?
        }
        "shutdown" => {
            state
                .db
                .lock()
                .await
                .set_deployment_desired_status_shutdown(*tenant_id, &pipeline_name)
                .await?
        }
        _ => Err(ManagerError::InvalidPipelineAction {
            action: action.to_string(),
        })?,
    }

    info!(
        "Accepted '{action}' action for pipeline {pipeline_name} (tenant: {})",
        *tenant_id
    );
    Ok(HttpResponse::Accepted().finish())
}

/// Change the desired state of an input endpoint.
///
/// The following values of the `action` argument are accepted by this endpoint:
///
/// - 'start': Start processing data.
/// - 'pause': Pause the pipeline.
#[utoipa::path(
    responses(
        (status = ACCEPTED
            , description = "Request accepted."),
        (status = NOT_FOUND
            , description = "Specified pipeline id does not exist."
            , body = ErrorResponse
            , example = json!(examples::error_unknown_pipeline())),
        (status = NOT_FOUND
                , description = "Specified endpoint does not exist."
                , body = ErrorResponse),
    ),
    params(
        ("pipeline_name" = String, Path, description = "Unique pipeline name"),
        ("endpoint_name" = String, Path, description = "Input endpoint name"),
        ("action" = String, Path, description = "Endpoint action [start, pause]")
    ),
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    tag = "Pipelines"
)]
#[post("/pipelines/{pipeline_name}/input_endpoints/{endpoint_name}/{action}")]
pub(crate) async fn input_endpoint_action(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_name = parse_string_param(&req, "pipeline_name")?;
    let endpoint_name = parse_string_param(&req, "endpoint_name")?;
    let action = parse_pipeline_action(&req)?;

    state
        .runner
        .forward_to_pipeline(
            *tenant_id,
            &pipeline_name,
            Method::GET,
            &format!("input_endpoints/{endpoint_name}/{action}"),
        )
        .await?;

    info!(
        "Accepted '{action}' action for pipeline '{pipeline_name}', endpoint '{endpoint_name}' (tenant:{})",
        *tenant_id
    );
    Ok(HttpResponse::Accepted().finish())
}

/// Retrieve pipeline statistics (e.g., metrics, performance counters).
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(
        ("pipeline_name" = String, Path, description = "Unique pipeline name"),
    ),
    responses(
        // TODO: implement `ToSchema` for `ControllerStatus`, which is the
        //       actual type returned by this endpoint and move it to pipeline-types.
        (status = OK
            , description = "Pipeline metrics retrieved successfully."
            , body = Object),
        (status = NOT_FOUND
            , description = "Pipeline with that name does not exist"
            , body = ErrorResponse
            , example = json!(examples::error_unknown_pipeline())),
        (status = BAD_REQUEST
            , description = "Pipeline is not running or paused"
            , body = ErrorResponse
            , example = json!(examples::error_pipeline_not_running_or_paused()))
    ),
    tag = "Pipelines"
)]
#[get("/pipelines/{pipeline_name}/stats")]
pub(crate) async fn get_pipeline_stats(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    request: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    debug!(
        "API: tenant {} requests to GET pipeline statistics {request:?}",
        *tenant_id
    );
    let pipeline_name = parse_string_param(&request, "pipeline_name")?;
    state
        .runner
        .forward_to_pipeline(*tenant_id, &pipeline_name, Method::GET, "stats")
        .await
}

/// Retrieve the circuit performance profile of a running or paused pipeline.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(
        ("pipeline_name" = String, Path, description = "Unique pipeline name"),
    ),
    responses(
        (status = OK
            , description = "Obtains a circuit performance profile."
            , content_type = "application/zip"
            , body = Object),
        (status = NOT_FOUND
            , description = "Pipeline with that name does not exist"
            , body = ErrorResponse
            , example = json!(examples::error_unknown_pipeline())),
        (status = BAD_REQUEST
            , description = "Pipeline is not running or paused"
            , body = ErrorResponse
            , example = json!(examples::error_pipeline_not_running_or_paused()))
    ),
    tag = "Pipelines"
)]
#[get("/pipelines/{pipeline_name}/circuit_profile")]
pub(crate) async fn get_pipeline_circuit_profile(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    request: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    debug!(
        "API: tenant {} requests to GET circuit profile {request:?}",
        *tenant_id
    );
    let pipeline_name = parse_string_param(&request, "pipeline_name")?;
    state
        .runner
        .forward_to_pipeline(*tenant_id, &pipeline_name, Method::GET, "dump_profile")
        .await
}

/// Retrieve the heap profile of a running or paused pipeline.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(
        ("pipeline_name" = String, Path, description = "Unique pipeline name"),
    ),
    responses(
        (status = OK
            , description = "Pipeline's heap usage profile as a gzipped protobuf that can be inspected by the pprof tool"
            , content_type = "application/protobuf"
            , body = Vec<u8>),
        (status = NOT_FOUND
            , description = "Pipeline with that name does not exist"
            , body = ErrorResponse
            , example = json!(examples::error_unknown_pipeline())),
        (status = BAD_REQUEST
            , description = "Pipeline is not running or paused"
            , body = ErrorResponse
            , example = json!(examples::error_pipeline_not_running_or_paused()))
    ),
    tag = "Pipelines"
)]
#[get("/pipelines/{pipeline_name}/heap_profile")]
pub(crate) async fn get_pipeline_heap_profile(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    request: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    debug!(
        "API: tenant {} requests to GET heap profile {request:?}",
        *tenant_id
    );
    let pipeline_name = parse_string_param(&request, "pipeline_name")?;
    state
        .runner
        .forward_to_pipeline(*tenant_id, &pipeline_name, Method::GET, "heap_profile")
        .await
}
