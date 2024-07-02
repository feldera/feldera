/// API to create, modify, delete, and deploy Feldera pipelines
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

use crate::{
    api::{examples, parse_string_param},
    auth::TenantId,
    db::{storage::Storage, AttachedConnector, DBError, PipelineId, Version},
};

use super::{ManagerError, ServerState};
use uuid::Uuid;

/// Request to create a new pipeline.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct NewPipelineRequest {
    /// Unique pipeline name.
    name: String,
    /// Pipeline description.
    description: String,
    /// Name of the program to create a pipeline for.
    program_name: Option<String>,
    /// Pipeline configuration parameters (e.g. number of workers).
    /// These knobs are independent of any connector attached to the pipeline.
    config: RuntimeConfig,
    /// Attached connectors.
    connectors: Option<Vec<AttachedConnector>>,
}

/// Response to a pipeline creation request.
#[derive(Serialize, ToSchema)]
pub(crate) struct NewPipelineResponse {
    /// ID of the newly created pipeline.
    pipeline_id: PipelineId,
    /// Initial pipeline version (this field is always set to 1).
    version: Version,
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct PipelineIdOrNameQuery {
    /// Unique pipeline id.
    id: Option<Uuid>,
    /// Unique pipeline name.
    name: Option<String>,
}

/// Request to update an existing pipeline.
#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct UpdatePipelineRequest {
    /// New pipeline name.
    name: String,
    /// New pipeline description.
    description: String,
    /// New program to create a pipeline for. If absent, program will be set to
    /// NULL.
    program_name: Option<String>,
    /// New pipeline configuration. If absent, the existing configuration will
    /// be kept unmodified.
    config: Option<RuntimeConfig>,
    /// Attached connectors.
    ///
    /// - If absent, existing connectors will be kept unmodified.
    ///
    /// - If present all existing connectors will be replaced with the new
    /// specified list.
    connectors: Option<Vec<AttachedConnector>>,
}

/// Response to a config update request.
#[derive(Serialize, ToSchema)]
pub(crate) struct UpdatePipelineResponse {
    /// New config version. Equals the previous version +1.
    version: Version,
}

/// Request to create or replace an existing pipeline.
#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct CreateOrReplacePipelineRequest {
    /// Pipeline description.
    description: String,
    /// Name of the program to create a pipeline for.
    program_name: Option<String>,
    /// Pipeline configuration parameters (e.g. number of workers).
    /// These knobs are independent of any connector attached to the pipeline.
    config: RuntimeConfig,
    /// Attached connectors.
    connectors: Option<Vec<AttachedConnector>>,
}

/// Response to a pipeline create or replace request.
#[derive(Serialize, ToSchema)]
pub(crate) struct CreateOrReplacePipelineResponse {
    /// ID of the newly created pipeline.
    pipeline_id: PipelineId,
    /// Initial pipeline version (this field is always set to 1).
    version: Version,
}

fn parse_pipeline_action(req: &HttpRequest) -> Result<&str, ManagerError> {
    match req.match_info().get("action") {
        None => Err(ManagerError::MissingUrlEncodedParam { param: "action" }),
        Some(action) => Ok(action),
    }
}

/// Create a new pipeline.
#[utoipa::path(
    request_body = NewPipelineRequest,
    responses(
        (status = OK, description = "Pipeline successfully created.", body = NewPipelineResponse),
        (status = NOT_FOUND
            , description = "Specified program id or connector ids do not exist."
            , body = ErrorResponse
            , examples (
                ("Unknown program" =
                    (description = "Specified program id does not exist",
                      value = json!(examples::unknown_program()))),
                ("Unknown connector" =
                    (description = "One or more connector ids do not exist.",
                     value = json!(examples::unknown_connector()))),
            )
        ),
    ),
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    tag = "Pipelines"
)]
#[post("/pipelines")]
pub(crate) async fn new_pipeline(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    request: web::Json<NewPipelineRequest>,
) -> Result<HttpResponse, ManagerError> {
    debug!("Received new-pipeline request: {request:?}");
    let (pipeline_id, version) = state
        .db
        .lock()
        .await
        .new_pipeline(
            *tenant_id,
            Uuid::now_v7(),
            &request.program_name,
            &request.name,
            &request.description,
            &request.config,
            &request.connectors,
            None,
        )
        .await?;

    info!("Created pipeline {pipeline_id} (tenant:{})", *tenant_id);
    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(&NewPipelineResponse {
            pipeline_id,
            version,
        }))
}

/// Change a pipeline's name, description, code, configuration, or connectors.
/// On success, increments the pipeline's version by 1.
#[utoipa::path(
    request_body = UpdatePipelineRequest,
    responses(
        (status = OK, description = "Pipeline successfully updated.", body = UpdatePipelineResponse),
        (status = NOT_FOUND
            , description = "Specified pipeline or connector does not exist."
            , body = ErrorResponse
            , examples (
                ("Unknown pipeline name" = (value = json!(examples::unknown_name()))),
                ("Unknown connector name" = (value = json!(examples::unknown_name()))),
            )),
    ),
    params(
        ("pipeline_name" = String, Path, description = "Unique pipeline name"),
    ),
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    tag = "Pipelines"
)]
#[patch("/pipelines/{pipeline_name}")]
pub(crate) async fn update_pipeline(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: HttpRequest,
    body: web::Json<UpdatePipelineRequest>,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_name = parse_string_param(&req, "pipeline_name")?;
    let db = state.db.lock().await;
    let pipeline = db.get_pipeline_by_name(*tenant_id, &pipeline_name).await?;
    let version = db
        .update_pipeline(
            *tenant_id,
            pipeline.descriptor.pipeline_id,
            &body.program_name,
            &body.name,
            &body.description,
            &body.config,
            &body.connectors,
            None,
        )
        .await?;

    info!("Updated pipeline {pipeline_name} (tenant:{})", *tenant_id);
    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(&UpdatePipelineResponse { version }))
}

/// Create or replace a pipeline.
#[utoipa::path(
    request_body = CreateOrReplacePipelineRequest,
    responses(
        (status = CREATED, description = "Pipeline created successfully", body = CreateOrReplacePipelineResponse),
        (status = OK, description = "Pipeline updated successfully", body = CreateOrReplacePipelineResponse),
        (status = CONFLICT
            , description = "A pipeline with this name already exists in the database."
            , body = ErrorResponse
            , example = json!(examples::duplicate_name())),
    ),
    params(
        ("pipeline_name" = String, Path, description = "Unique pipeline name"),
    ),
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    tag = "Pipelines"
)]
#[put("/pipelines/{pipeline_name}")]
async fn create_or_replace_pipeline(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    request: HttpRequest,
    body: web::Json<CreateOrReplacePipelineRequest>,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_name = parse_string_param(&request, "pipeline_name")?;
    let (created, pipeline_id, version) = state
        .db
        .lock()
        .await
        .create_or_replace_pipeline(
            *tenant_id,
            &pipeline_name,
            &body.program_name,
            &body.description,
            &body.config,
            &body.connectors,
        )
        .await?;
    if created {
        Ok(HttpResponse::Created()
            .insert_header(CacheControl(vec![CacheDirective::NoCache]))
            .json(&CreateOrReplacePipelineResponse {
                pipeline_id,
                version,
            }))
    } else {
        Ok(HttpResponse::Ok()
            .insert_header(CacheControl(vec![CacheDirective::NoCache]))
            .json(&CreateOrReplacePipelineResponse {
                pipeline_id,
                version,
            }))
    }
}

/// Fetch pipelines, optionally filtered by name or ID.
#[utoipa::path(
    responses(
        (status = OK, description = "Pipeline list retrieved successfully.", body = [Pipeline])
    ),
    params(PipelineIdOrNameQuery),
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    tag = "Pipelines"
)]
#[get("/pipelines")]
pub(crate) async fn list_pipelines(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    query: web::Query<PipelineIdOrNameQuery>,
) -> Result<HttpResponse, DBError> {
    let pipelines = if let Some(id) = query.id {
        let pipeline = state
            .db
            .lock()
            .await
            .get_pipeline_by_id(*tenant_id, PipelineId(id))
            .await?;
        vec![pipeline]
    } else if let Some(name) = query.name.clone() {
        let pipeline = state
            .db
            .lock()
            .await
            .get_pipeline_by_name(*tenant_id, &name)
            .await?;
        vec![pipeline]
    } else {
        state.db.lock().await.list_pipelines(*tenant_id).await?
    };
    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(pipelines))
}

/// Return the currently deployed version of the pipeline, if any.
#[utoipa::path(
    responses(
        (status = OK, description = "Last deployed version of the pipeline retrieved successfully (returns null if pipeline was never deployed yet).", body = Option<PipelineRevision>),
        (status = NOT_FOUND
            , description = "Specified `pipeline_id` does not exist."
            , body = ErrorResponse
            , example = json!(examples::unknown_pipeline())),
    ),
    params(
        ("pipeline_name" = String, Path, description = "Unique pipeline name"),
    ),
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    tag = "Pipelines"
)]
#[get("/pipelines/{pipeline_name}/deployed")]
pub(crate) async fn pipeline_deployed(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_name = parse_string_param(&req, "pipeline_name")?;
    let db = state.db.lock().await;
    let pipeline = db.get_pipeline_by_name(*tenant_id, &pipeline_name).await?;
    let descr: Option<crate::db::PipelineRevision> = match db
        .get_pipeline_deployment(*tenant_id, pipeline.descriptor.pipeline_id)
        .await
    {
        Ok(revision) => Some(revision),
        Err(e) => {
            if matches!(e, DBError::NoRevisionAvailable { .. }) {
                None
            } else {
                Err(e)?
            }
        }
    };

    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(&descr))
}

/// Retrieve pipeline metrics and performance counters.
#[utoipa::path(
    responses(
        // TODO: Implement `ToSchema` for `ControllerStatus`, which is the
        // actual type returned by this endpoint.
        (status = OK, description = "Pipeline metrics retrieved successfully.", body = Object),
        (status = BAD_REQUEST
            , description = "Specified pipeline id is not a valid uuid."
            , body = ErrorResponse
            , example = json!(examples::invalid_uuid_param())),
        (status = NOT_FOUND
            , description = "Specified pipeline id does not exist."
            , body = ErrorResponse
            , example = json!(examples::unknown_pipeline())),
    ),
    params(
        ("pipeline_name" = String, Path, description = "Unique pipeline name"),
    ),
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    tag = "Pipelines"
)]
#[get("/pipelines/{pipeline_name}/stats")]
pub(crate) async fn pipeline_stats(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_name = parse_string_param(&req, "pipeline_name")?;
    state
        .runner
        .forward_to_pipeline(*tenant_id, &pipeline_name, Method::GET, "stats")
        .await
}

/// Fetch a pipeline by ID.
#[utoipa::path(
    responses(
        (status = OK, description = "Pipeline descriptor retrieved successfully.",content(
            ("application/json" = Pipeline, example = json!(examples::pipeline_config())),
        )),
        (status = NOT_FOUND
            , description = "Specified pipeline ID does not exist."
            , body = ErrorResponse
            , example = json!(examples::unknown_pipeline())),
    ),
    params(
        ("pipeline_name" = String, Path, description = "Unique pipeline name"),
    ),
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    tag = "Pipelines"
)]
#[get("/pipelines/{pipeline_name}")]
pub(crate) async fn get_pipeline(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_name = parse_string_param(&req, "pipeline_name")?;
    let pipeline: crate::db::Pipeline = state
        .db
        .lock()
        .await
        .get_pipeline_by_name(*tenant_id, &pipeline_name)
        .await?;
    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(&pipeline))
}

/// Fetch a pipeline's configuration.
///
/// When defining a pipeline, clients have to provide an optional
/// `RuntimeConfig` for the pipelines and references to existing
/// connectors to attach to the pipeline. This endpoint retrieves
/// the *expanded* definition of the pipeline's configuration,
/// which comprises both the `RuntimeConfig` and the complete
/// definitions of the attached connectors.
#[utoipa::path(
    responses(
        (status = OK, description = "Expanded pipeline configuration retrieved successfully.",content(
            ("application/json" = PipelineConfig),
        )),
        (status = NOT_FOUND
            , description = "Specified pipeline ID does not exist."
            , body = ErrorResponse
            , example = json!(examples::unknown_pipeline())),
    ),
    params(
      ("pipeline_name" = String, Path, description = "Unique pipeline name")
    ),
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    tag = "Pipelines"
)]
#[get("/pipelines/{pipeline_name}/config")]
pub(crate) async fn get_pipeline_config(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_name = parse_string_param(&req, "pipeline_name")?;
    let expanded_config = state
        .db
        .lock()
        .await
        .pipeline_config(*tenant_id, &pipeline_name)
        .await?;
    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(&expanded_config))
}

/// Validate a pipeline.
///
/// Checks whether a pipeline is configured correctly. This includes
/// checking whether the pipeline references a valid compiled program,
/// whether the connectors reference valid tables/views in the program,
/// and more.
#[utoipa::path(
    responses(
        (status = OK
            , description = "Validate a Pipeline config."
            , content_type = "application/json"
            , body = String),
        (status = BAD_REQUEST
            , description = "Invalid pipeline."
            , body = ErrorResponse
            , examples(
                ("Invalid Pipeline ID" = (description = "Specified pipeline id is not a valid uuid.", value = json!(examples::invalid_uuid_param()))),
                ("Program not set" = (description = "Pipeline does not have a program set.", value = json!(examples::program_not_set()))),
                ("Program not compiled" = (description = "The program associated with this pipeline has not been compiled.", value = json!(examples::program_not_compiled()))),
                ("Program has compilation errors" = (description = "The program associated with the pipeline raised compilation error.", value = json!(examples::program_has_errors()))),
                ("Invalid table reference" = (description = "Connectors reference a table that doesn't exist.", value = json!(examples::pipeline_invalid_input_ac()))),
                ("Invalid table or view reference" = (description = "Connectors reference a view that doesn't exist.", value = json!(examples::pipeline_invalid_output_ac()))),
            )
        ),
        (status = NOT_FOUND
            , description = "Specified pipeline id does not exist."
            , body = ErrorResponse
            , example = json!(examples::unknown_pipeline())),
    ),
    params(
        ("pipeline_name" = String, Path, description = "Unique pipeline name"),
    ),
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    tag = "Pipelines"
)]
#[get("/pipelines/{pipeline_name}/validate")]
pub(crate) async fn pipeline_validate(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_name = parse_string_param(&req, "pipeline_name")?;
    let db = state.db.lock().await;
    let pipeline = db.get_pipeline_by_name(*tenant_id, &pipeline_name).await?;
    Ok(db
        .is_pipeline_deployable(*tenant_id, pipeline.descriptor.pipeline_id)
        .await
        .map(|_| HttpResponse::Ok().json("Pipeline successfully validated."))?)
}

/// Change the desired state of the pipeline.
///
/// This endpoint allows the user to control the execution of the pipeline,
/// by changing its desired state attribute (see the discussion of the desired
/// state model in the [`PipelineStatus`] documentation).
///
/// The endpoint returns immediately after validating the request and forwarding
/// it to the pipeline. The requested status change completes asynchronously.
/// On success, the pipeline enters the requested desired state.  On error, the
/// pipeline transitions to the `Failed` state. The user
/// can monitor the current status of the pipeline by polling the `GET
/// /pipeline` endpoint.
///
/// The following values of the `action` argument are accepted by this endpoint:
///
/// - 'start': Start processing data.
/// - 'pause': Pause the pipeline.
/// - 'shutdown': Terminate the execution of the pipeline.
#[utoipa::path(
    responses(
        (status = ACCEPTED
            , description = "Request accepted."),
        (status = BAD_REQUEST
            , description = "Pipeline desired state is not valid."
            , body = ErrorResponse
            , examples(
                ("Invalid Pipeline ID" = (description = "Specified pipeline id is not a valid uuid.", value = json!(examples::invalid_uuid_param()))),
                ("Program not set" = (description = "Pipeline does not have a program set.", value = json!(examples::program_not_set()))),
                ("Program not compiled" = (description = "The program associated with this pipeline has not been compiled.", value = json!(examples::program_not_compiled()))),
                ("Program has compilation errors" = (description = "The program associated with the pipeline raised compilation error.", value = json!(examples::program_has_errors()))),
                ("Invalid table reference" = (description = "Connectors reference a table that doesn't exist.", value = json!(examples::pipeline_invalid_input_ac()))),
                ("Invalid table or view reference" = (description = "Connectors reference a view that doesn't exist.", value = json!(examples::pipeline_invalid_output_ac()))),
                ("Invalidtable or view reference" = (description = "Connectors reference a view that doesn't exist.", value = json!(examples::pipeline_invalid_output_ac()))),
                ("Invalid action" = (description = "Invalid action specified", value = json!(examples::invalid_pipeline_action()))),
                ("Action cannot be applied" = (description = "Action is not applicable in the current state of the pipeline.", value = json!(examples::illegal_pipeline_action()))),
            )
        ),
        (status = NOT_FOUND
            , description = "Specified pipeline id does not exist."
            , body = ErrorResponse
            , example = json!(examples::unknown_pipeline())),
        (status = INTERNAL_SERVER_ERROR
            , description = "Timeout waiting for the pipeline to initialize."
            , body = ErrorResponse
            , example = json!(examples::pipeline_timeout())),
    ),
    params(
        ("pipeline_name" = String, Path, description = "Unique pipeline name"),
        ("action" = String, Path, description = "Pipeline action [start, pause, shutdown]")
    ),
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    tag = "Pipelines"
)]
#[post("/pipelines/{pipeline_name}/{action}")]
pub(crate) async fn pipeline_action(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_name = parse_string_param(&req, "pipeline_name")?;
    let action = parse_pipeline_action(&req)?;

    match action {
        "start" => {
            state
                .runner
                .start_pipeline(*tenant_id, &pipeline_name)
                .await?
        }
        "pause" => {
            state
                .runner
                .pause_pipeline(*tenant_id, &pipeline_name)
                .await?
        }
        "shutdown" => {
            state
                .runner
                .shutdown_pipeline(*tenant_id, &pipeline_name)
                .await?
        }
        _ => Err(ManagerError::InvalidPipelineAction {
            action: action.to_string(),
        })?,
    }

    info!(
        "Accepted '{action}' action for pipeline {pipeline_name} (tenant:{})",
        *tenant_id
    );
    Ok(HttpResponse::Accepted().finish())
}

/// Delete a pipeline. The pipeline must be in the shutdown state.
#[utoipa::path(
    responses(
        (status = OK
            , description = "Pipeline successfully deleted."),
        (status = NOT_FOUND
            , description = "Specified pipeline id does not exist."
            , body = ErrorResponse
            , example = json!(examples::unknown_pipeline())),
        (status = BAD_REQUEST
            , description = "Pipeline ID is invalid or pipeline is already running."
            , body = ErrorResponse
            , examples(
                ("Pipeline is running" =
                    (description = "Pipeline cannot be deleted while executing. Shutdown the pipeline first.",
                    value = json!(examples::cannot_delete_when_running()))),
                ("Invalid Pipeline ID" =
                    (value = json!(examples::invalid_uuid_param())))
        )),
    ),
    params(
        ("pipeline_name" = String, Path, description = "Unique pipeline name"),
    ),
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    tag = "Pipelines"
)]
#[delete("/pipelines/{pipeline_name}")]
pub(crate) async fn pipeline_delete(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_name = parse_string_param(&req, "pipeline_name")?;

    state
        .runner
        .delete_pipeline(*tenant_id, &pipeline_name)
        .await?;

    info!("Deleted pipeline {pipeline_name} (tenant:{})", *tenant_id);
    Ok(HttpResponse::Ok().finish())
}

/// Initiate profile dump.
#[utoipa::path(
    responses(
    (status = OK
    , description = "Obtains a circuit performance profile."
    , content_type = "application/zip"
    , body = Object),
    (status = BAD_REQUEST
    , description = "Specified pipeline id is not a valid uuid."
    , body = ErrorResponse
    , example = json!(examples::invalid_uuid_param())),
        (status = NOT_FOUND
        , description = "Specified pipeline id does not exist."
        , body = ErrorResponse
        , example = json!(examples::unknown_pipeline())),
    ),
    params(
        ("pipeline_name" = String, Path, description = "Unique pipeline name"),
    ),
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    tag = "Pipelines"
)]
#[get("/pipelines/{pipeline_name}/dump_profile")]
pub(crate) async fn dump_profile(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_name = parse_string_param(&req, "pipeline_name")?;
    state
        .runner
        .forward_to_pipeline(*tenant_id, &pipeline_name, Method::GET, "dump_profile")
        .await
}

/// Retrieve heap profile of the pipeline.
#[utoipa::path(
responses(
    (status = OK
    , description = "Pipeline's heap usage profile as a gzipped protobuf that can be inspected by the pprof tool."
    , content_type = "application/protobuf"
    , body = Vec<u8>),
    (status = BAD_REQUEST
    , description = "Specified pipeline id is not a valid uuid."
    , body = ErrorResponse
    , example = json!(examples::invalid_uuid_param())),
    (status = NOT_FOUND
    , description = "Specified pipeline id does not exist."
    , body = ErrorResponse
    , example = json!(examples::unknown_pipeline())),
    ),
    params(
    ("pipeline_name" = String, Path, description = "Unique pipeline name"),
    ),
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    tag = "Pipelines"
)]
#[get("/pipelines/{pipeline_name}/heap_profile")]
pub(crate) async fn heap_profile(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_name = parse_string_param(&req, "pipeline_name")?;
    state
        .runner
        .forward_to_pipeline(*tenant_id, &pipeline_name, Method::GET, "heap_profile")
        .await
}
