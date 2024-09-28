use super::ManagerError;
use crate::api::ServerState;
use crate::api::{examples, parse_string_param};
use crate::db::error::DBError;
use crate::db::storage::Storage;
use crate::db::types::common::Version;
use crate::db::types::pipeline::{
    ExtendedPipelineDescr, PipelineDescr, PipelineDesiredStatus, PipelineId, PipelineStatus,
};
use crate::db::types::program::{ProgramConfig, ProgramInfo, ProgramStatus};
use crate::db::types::tenant::TenantId;
use actix_web::{
    delete, get,
    http::header::{CacheControl, CacheDirective},
    http::Method,
    patch, post, put,
    web::{self, Data as WebData, ReqData},
    HttpRequest, HttpResponse,
};
use chrono::{DateTime, Utc};
use feldera_types::config::{PipelineConfig, RuntimeConfig};
use feldera_types::error::ErrorResponse;
use log::info;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

// REGULAR ENDPOINTS

/// Extended pipeline descriptor with code being optionally included.
#[derive(Deserialize, Serialize, ToSchema, Eq, PartialEq, Debug, Clone)]
pub struct ExtendedPipelineDescrOptionalCode {
    pub id: PipelineId,
    pub name: String,
    pub description: String,
    pub version: Version,
    pub created_at: DateTime<Utc>,
    pub runtime_config: RuntimeConfig,
    pub program_code: Option<String>,
    pub program_config: ProgramConfig,
    pub program_version: Version,
    pub program_status: ProgramStatus,
    pub program_status_since: DateTime<Utc>,
    pub program_info: Option<ProgramInfo>,
    pub program_binary_url: Option<String>,
    pub deployment_status: PipelineStatus,
    pub deployment_status_since: DateTime<Utc>,
    pub deployment_desired_status: PipelineDesiredStatus,
    pub deployment_error: Option<ErrorResponse>,
    pub deployment_config: Option<PipelineConfig>,
    pub deployment_location: Option<String>,
}

impl ExtendedPipelineDescrOptionalCode {
    pub(crate) fn new(extended_pipeline: ExtendedPipelineDescr, include_code: bool) -> Self {
        ExtendedPipelineDescrOptionalCode {
            id: extended_pipeline.id,
            name: extended_pipeline.name,
            description: extended_pipeline.description,
            version: extended_pipeline.version,
            created_at: extended_pipeline.created_at,
            runtime_config: extended_pipeline.runtime_config,
            program_code: if include_code {
                Some(extended_pipeline.program_code)
            } else {
                None
            },
            program_config: extended_pipeline.program_config,
            program_version: extended_pipeline.program_version,
            program_status: extended_pipeline.program_status,
            program_status_since: extended_pipeline.program_status_since,
            program_info: extended_pipeline.program_info,
            program_binary_url: extended_pipeline.program_binary_url,
            deployment_status: extended_pipeline.deployment_status,
            deployment_status_since: extended_pipeline.deployment_status_since,
            deployment_desired_status: extended_pipeline.deployment_desired_status,
            deployment_error: extended_pipeline.deployment_error,
            deployment_config: extended_pipeline.deployment_config,
            deployment_location: extended_pipeline.deployment_location,
        }
    }
}

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
            , body = [ExtendedPipelineDescrOptionalCode]
            , example = json!(examples::list_extended_pipeline_optional_code())),
        (status = INTERNAL_SERVER_ERROR, body = ErrorResponse),
    ),
    tag = "Pipelines"
)]
#[get("/pipelines")]
pub(crate) async fn list_pipelines(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    query: web::Query<ListPipelinesQueryParameters>,
) -> Result<HttpResponse, DBError> {
    let pipelines = state.db.lock().await.list_pipelines(*tenant_id).await?;
    let pipelines: Vec<ExtendedPipelineDescrOptionalCode> = pipelines
        .iter()
        .map(|v| ExtendedPipelineDescrOptionalCode::new(v.clone(), query.code))
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
            , body = ExtendedPipelineDescr
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
            , body = ExtendedPipelineDescr
            , example = json!(examples::extended_pipeline_1())),
        (status = CONFLICT
            , description = "Cannot create pipeline as the name already exists"
            , body = ErrorResponse
            , example = json!(examples::error_duplicate_name()))
    ),
    tag = "Pipelines"
)]
#[post("/pipelines")]
pub(crate) async fn post_pipeline(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    body: web::Json<PipelineDescr>,
) -> Result<HttpResponse, ManagerError> {
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
            , body = ExtendedPipelineDescr
            , example = json!(examples::extended_pipeline_1())),
        (status = OK
            , description = "Pipeline successfully updated"
            , body = ExtendedPipelineDescr
            , example = json!(examples::extended_pipeline_1())),
        (status = CONFLICT
            , description = "Cannot rename pipeline as the name already exists"
            , body = ErrorResponse
            , example = json!(examples::error_duplicate_name())),
        (status = BAD_REQUEST
            , description = "Pipeline needs to be shutdown to be modified"
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
            , body = ExtendedPipelineDescr
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
            , description = "Pipeline needs to be shutdown to be modified"
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
            , description = "Pipeline needs to be shutdown to be deleted"
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

/// Start, pause or shutdown a pipeline.
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
        "Accepted {action} action for pipeline {pipeline_name} (tenant: {})",
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
        .forward_http_request_to_pipeline_by_name(
            *tenant_id,
            &pipeline_name,
            Method::GET,
            &format!("input_endpoints/{endpoint_name}/{action}"),
            "",
            None,
        )
        .await?;

    info!(
        "Accepted '{action}' action for pipeline '{pipeline_name}', endpoint '{endpoint_name}' (tenant:{})",
        *tenant_id
    );
    Ok(HttpResponse::Accepted().finish())
}

/// Retrieve pipeline logs as a stream.
///
/// The logs stream catches up to the extent of the internally configured per-pipeline
/// circular logs buffer (limited to a certain byte size and number of lines, whichever
/// is reached first). After the catch-up, new lines are pushed whenever they become
/// available.
///
/// The logs stream will end when the pipeline is shut down. It is also possible for the
/// logs stream to end prematurely due to the runner back-end (temporarily) losing
/// connectivity to the pipeline instance (e.g., process). In this case, it is needed
/// to issue again a new request to this endpoint.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(
        ("pipeline_name" = String, Path, description = "Unique pipeline name"),
    ),
    responses(
        (status = OK
            , description = "Pipeline logs retrieved successfully"
            , body = Object),
        (status = NOT_FOUND
            , description = "Pipeline with that name does not exist"
            , body = ErrorResponse
            , example = json!(examples::error_unknown_pipeline()))
    ),
    tag = "Pipelines"
)]
#[get("/pipelines/{pipeline_name}/logs")]
pub(crate) async fn get_pipeline_logs(
    client: WebData<awc::Client>,
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    request: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_name = parse_string_param(&request, "pipeline_name")?;
    state
        .runner
        .http_streaming_logs_from_pipeline_by_name(&client, *tenant_id, &pipeline_name)
        .await
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
        //       actual type returned by this endpoint and move it to feldera-types.
        (status = OK
            , description = "Pipeline metrics retrieved successfully"
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
    let pipeline_name = parse_string_param(&request, "pipeline_name")?;
    state
        .runner
        .forward_http_request_to_pipeline_by_name(
            *tenant_id,
            &pipeline_name,
            Method::GET,
            "stats",
            request.query_string(),
            None,
        )
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
    let pipeline_name = parse_string_param(&request, "pipeline_name")?;
    state
        .runner
        .forward_http_request_to_pipeline_by_name(
            *tenant_id,
            &pipeline_name,
            Method::GET,
            "dump_profile",
            request.query_string(),
            Some(Duration::from_secs(120)),
        )
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
            , description = "Pipeline is not running or paused, or getting a heap profile is not supported on this platform"
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
    let pipeline_name = parse_string_param(&request, "pipeline_name")?;
    state
        .runner
        .forward_http_request_to_pipeline_by_name(
            *tenant_id,
            &pipeline_name,
            Method::GET,
            "heap_profile",
            request.query_string(),
            None,
        )
        .await
}

/// Execute an ad-hoc query in a running or paused pipeline.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(
        ("pipeline_name" = String, Path, description = "Unique pipeline name"),
        ("sql" = String, Query, description = "The SQL query to execute."),
        ("format" = AdHocResultFormat, Query, description = "Input data format, e.g., 'text', 'json' or 'parquet'."),
    ),
    responses(
        (status = OK
        , description = "Executes an ad-hoc SQL query in a running or paused pipeline. The evaluation is not incremental."
        , content_type = "text/plain"
        , body = Vec<u8>),
        (status = NOT_FOUND
        , description = "Pipeline with that name does not exist"
        , body = ErrorResponse
        , example = json!(examples::error_unknown_pipeline())),
        (status = BAD_REQUEST
        , description = "Pipeline is shutdown or an invalid SQL query was supplied"
        , body = ErrorResponse
        , example = json!(examples::error_pipeline_not_running_or_paused())),
        (status = INTERNAL_SERVER_ERROR
        , description = "A fatal error occurred during query processing (after streaming response was already initiated)"
        , body = ErrorResponse
        , example = json!(examples::error_stream_terminated()))
    ),
    tag = "Pipelines"
)]
#[get("/pipelines/{pipeline_name}/query")]
pub(crate) async fn pipeline_adhoc_sql(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    client: WebData<awc::Client>,
    request: HttpRequest,
    body: web::Payload,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_name = parse_string_param(&request, "pipeline_name")?;
    state
        .runner
        .forward_streaming_http_request_to_pipeline_by_name(
            *tenant_id,
            &pipeline_name,
            "query",
            request,
            body,
            client.as_ref(),
            Some(Duration::MAX),
        )
        .await
}
