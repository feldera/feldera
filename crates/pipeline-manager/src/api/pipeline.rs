use super::ManagerError;
use crate::api::error::ApiError;
use crate::api::examples;
use crate::api::ServerState;
#[cfg(not(feature = "feldera-enterprise"))]
use crate::common_error::CommonError;
use crate::db::error::DBError;
use crate::db::storage::Storage;
use crate::db::types::common::Version;
use crate::db::types::pipeline::{
    ExtendedPipelineDescr, ExtendedPipelineDescrMonitoring, PipelineDescr, PipelineDesiredStatus,
    PipelineId, PipelineStatus,
};
use crate::db::types::program::{ProgramConfig, ProgramInfo, ProgramStatus};
use crate::db::types::tenant::TenantId;
use actix_http::StatusCode;
use actix_web::{
    delete, get,
    http::header::{CacheControl, CacheDirective},
    http::Method,
    patch, post, put,
    web::{self, Data as WebData, ReqData},
    HttpRequest, HttpResponse,
};
use chrono::{DateTime, Utc};
use feldera_types::config::RuntimeConfig;
use feldera_types::error::ErrorResponse;
use feldera_types::program_schema::SqlIdentifier;
use log::info;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

/// Pipeline information.
#[derive(Serialize, ToSchema, Eq, PartialEq, Debug, Clone)]
pub struct PipelineInfo {
    pub id: PipelineId,
    pub name: String,
    pub description: String,
    pub created_at: DateTime<Utc>,
    pub version: Version,
    pub platform_version: String,
    pub runtime_config: RuntimeConfig,
    pub program_code: String,
    pub udf_rust: String,
    pub udf_toml: String,
    pub program_config: ProgramConfig,
    pub program_version: Version,
    pub program_status: ProgramStatus,
    pub program_status_since: DateTime<Utc>,
    pub program_info: Option<ProgramInfo>,
    pub deployment_status: PipelineStatus,
    pub deployment_status_since: DateTime<Utc>,
    pub deployment_desired_status: PipelineDesiredStatus,
    pub deployment_error: Option<ErrorResponse>,
}

impl PipelineInfo {
    pub(crate) fn new(extended_pipeline: &ExtendedPipelineDescr) -> Self {
        PipelineInfo {
            id: extended_pipeline.id,
            name: extended_pipeline.name.clone(),
            description: extended_pipeline.description.clone(),
            created_at: extended_pipeline.created_at,
            version: extended_pipeline.version,
            platform_version: extended_pipeline.platform_version.clone(),
            runtime_config: extended_pipeline.runtime_config.clone(),
            program_code: extended_pipeline.program_code.clone(),
            udf_rust: extended_pipeline.udf_rust.clone(),
            udf_toml: extended_pipeline.udf_toml.clone(),
            program_config: extended_pipeline.program_config.clone(),
            program_version: extended_pipeline.program_version,
            program_status: extended_pipeline.program_status.clone(),
            program_status_since: extended_pipeline.program_status_since,
            program_info: extended_pipeline.program_info.clone(),
            deployment_status: extended_pipeline.deployment_status,
            deployment_status_since: extended_pipeline.deployment_status_since,
            deployment_desired_status: extended_pipeline.deployment_desired_status,
            deployment_error: extended_pipeline.deployment_error.clone(),
        }
    }
}

/// Pipeline information which has a selected subset of optional fields.
/// If an optional field is not selected (i.e., is `None`), it will not be serialized.
#[derive(Serialize, ToSchema, Eq, PartialEq, Debug, Clone)]
pub struct PipelineSelectedInfo {
    pub id: PipelineId,
    pub name: String,
    pub description: String,
    pub created_at: DateTime<Utc>,
    pub version: Version,
    pub platform_version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime_config: Option<RuntimeConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub program_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub udf_rust: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub udf_toml: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub program_config: Option<ProgramConfig>,
    pub program_version: Version,
    pub program_status: ProgramStatus,
    pub program_status_since: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub program_info: Option<Option<ProgramInfo>>,
    pub deployment_status: PipelineStatus,
    pub deployment_status_since: DateTime<Utc>,
    pub deployment_desired_status: PipelineDesiredStatus,
    pub deployment_error: Option<ErrorResponse>,
}

impl PipelineSelectedInfo {
    pub(crate) fn new_all(extended_pipeline: &ExtendedPipelineDescr) -> Self {
        PipelineSelectedInfo {
            id: extended_pipeline.id,
            name: extended_pipeline.name.clone(),
            description: extended_pipeline.description.clone(),
            created_at: extended_pipeline.created_at,
            version: extended_pipeline.version,
            platform_version: extended_pipeline.platform_version.clone(),
            runtime_config: Some(extended_pipeline.runtime_config.clone()),
            program_code: Some(extended_pipeline.program_code.clone()),
            udf_rust: Some(extended_pipeline.udf_rust.clone()),
            udf_toml: Some(extended_pipeline.udf_toml.clone()),
            program_config: Some(extended_pipeline.program_config.clone()),
            program_version: extended_pipeline.program_version,
            program_status: extended_pipeline.program_status.clone(),
            program_status_since: extended_pipeline.program_status_since,
            program_info: Some(extended_pipeline.program_info.clone()),
            deployment_status: extended_pipeline.deployment_status,
            deployment_status_since: extended_pipeline.deployment_status_since,
            deployment_desired_status: extended_pipeline.deployment_desired_status,
            deployment_error: extended_pipeline.deployment_error.clone(),
        }
    }

    pub(crate) fn new_status(extended_pipeline: &ExtendedPipelineDescrMonitoring) -> Self {
        PipelineSelectedInfo {
            id: extended_pipeline.id,
            name: extended_pipeline.name.clone(),
            description: extended_pipeline.description.clone(),
            created_at: extended_pipeline.created_at,
            version: extended_pipeline.version,
            platform_version: extended_pipeline.platform_version.clone(),
            runtime_config: None,
            program_code: None,
            udf_rust: None,
            udf_toml: None,
            program_config: None,
            program_version: extended_pipeline.program_version,
            program_status: extended_pipeline.program_status.clone(),
            program_status_since: extended_pipeline.program_status_since,
            program_info: None,
            deployment_status: extended_pipeline.deployment_status,
            deployment_status_since: extended_pipeline.deployment_status_since,
            deployment_desired_status: extended_pipeline.deployment_desired_status,
            deployment_error: extended_pipeline.deployment_error.clone(),
        }
    }
}

#[derive(Deserialize, Serialize, ToSchema, Eq, PartialEq, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub enum PipelineFieldSelector {
    /// Select all fields of a pipeline.
    ///
    /// The selection includes the following fields:
    /// - `id`
    /// - `name`
    /// - `description`
    /// - `created_at`
    /// - `version`
    /// - `platform_version`
    /// - `runtime_config`
    /// - `program_code`
    /// - `udf_rust`
    /// - `udf_toml`
    /// - `program_config`
    /// - `program_version`
    /// - `program_status`
    /// - `program_status_since`
    /// - `program_info`
    /// - `deployment_status`
    /// - `deployment_status_since`
    /// - `deployment_desired_status`
    /// - `deployment_error`
    All,
    /// Select only the fields required to know the status of a pipeline.
    ///
    /// The selection includes the following fields:
    /// - `id`
    /// - `name`
    /// - `description`
    /// - `created_at`
    /// - `version`
    /// - `platform_version`
    /// - `program_version`
    /// - `program_status`
    /// - `program_status_since`
    /// - `deployment_status`
    /// - `deployment_status_since`
    /// - `deployment_desired_status`
    /// - `deployment_error`
    Status,
}

/// Default for the `selector` query parameter when GET a pipeline or a list of pipelines.
fn default_pipeline_field_selector() -> PipelineFieldSelector {
    PipelineFieldSelector::All
}

/// Query parameters to GET a pipeline or a list of pipelines.
#[derive(Debug, Deserialize, IntoParams, ToSchema)]
pub struct GetPipelineParameters {
    /// The `selector` parameter limits which fields are returned for a pipeline.
    /// Limiting which fields is particularly handy for instance when frequently
    /// monitoring over low bandwidth connections while being only interested
    /// in pipeline status.
    #[serde(default = "default_pipeline_field_selector")]
    selector: PipelineFieldSelector,
}

/// Create a new pipeline (POST), or fully update an existing pipeline (PUT).
/// Left-out fields will be set to their default value.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct PostPutPipeline {
    pub name: String,
    pub description: Option<String>,
    pub runtime_config: Option<RuntimeConfig>,
    pub program_code: String,
    pub udf_rust: Option<String>,
    pub udf_toml: Option<String>,
    pub program_config: Option<ProgramConfig>,
}

impl PostPutPipeline {
    /// Converts the optional fields with the default value
    /// as alternative (e.g., a string will be empty).
    fn to_descr(&self) -> PipelineDescr {
        PipelineDescr {
            name: self.name.clone(),
            description: self.description.clone().unwrap_or_default(),
            runtime_config: self.runtime_config.clone().unwrap_or_default(),
            program_code: self.program_code.clone(),
            udf_rust: self.udf_rust.clone().unwrap_or_default(),
            udf_toml: self.udf_toml.clone().unwrap_or_default(),
            program_config: self.program_config.clone().unwrap_or_default(),
        }
    }
}

/// Partially update the pipeline (PATCH).
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
    pub udf_rust: Option<String>,
    pub udf_toml: Option<String>,
    pub program_config: Option<ProgramConfig>,
}

/// Retrieve the list of pipelines.
/// Configure which fields are included using the `selector` query parameter.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(GetPipelineParameters),
    responses(
        (status = OK
            , description = "List of pipelines retrieved successfully"
            , body = [PipelineSelectedInfo]
            , example = json!(examples::list_pipeline_selected_info())),
        (status = INTERNAL_SERVER_ERROR, body = ErrorResponse),
    ),
    tag = "Pipelines"
)]
#[get("/pipelines")]
pub(crate) async fn list_pipelines(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    query: web::Query<GetPipelineParameters>,
) -> Result<HttpResponse, DBError> {
    let returned_pipelines: Vec<PipelineSelectedInfo> = match &query.selector {
        PipelineFieldSelector::All => {
            let pipelines = state.db.lock().await.list_pipelines(*tenant_id).await?;
            pipelines
                .iter()
                .map(PipelineSelectedInfo::new_all)
                .collect()
        }
        PipelineFieldSelector::Status => {
            let pipelines = state
                .db
                .lock()
                .await
                .list_pipelines_for_monitoring(*tenant_id)
                .await?;
            pipelines
                .iter()
                .map(PipelineSelectedInfo::new_status)
                .collect()
        }
    };

    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(returned_pipelines))
}

/// Retrieve a pipeline.
/// Configure which fields are included using the `selector` query parameter.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(
        ("pipeline_name" = String, Path, description = "Unique pipeline name"),
        GetPipelineParameters,
    ),
    responses(
        (status = OK
            , description = "Pipeline retrieved successfully"
            , body = PipelineSelectedInfo
            , example = json!(examples::pipeline_1_selected_info())),
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
    path: web::Path<String>,
    query: web::Query<GetPipelineParameters>,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_name = path.into_inner();
    let returned_pipeline = match &query.selector {
        PipelineFieldSelector::All => {
            let pipeline = state
                .db
                .lock()
                .await
                .get_pipeline(*tenant_id, &pipeline_name)
                .await?;
            PipelineSelectedInfo::new_all(&pipeline)
        }
        PipelineFieldSelector::Status => {
            let pipeline = state
                .db
                .lock()
                .await
                .get_pipeline_for_monitoring(*tenant_id, &pipeline_name)
                .await?;
            PipelineSelectedInfo::new_status(&pipeline)
        }
    };

    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(&returned_pipeline))
}

/// Create a new pipeline.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    request_body(
        content = PostPutPipeline, example = json!(examples::pipeline_post_put())
    ),
    responses(
        (status = CREATED
            , description = "Pipeline successfully created"
            , body = PipelineInfo
            , example = json!(examples::pipeline_1_info())),
        (status = CONFLICT
            , description = "Cannot create pipeline as the name already exists"
            , body = ErrorResponse
            , example = json!(examples::error_duplicate_name())),
        (status = BAD_REQUEST
            , description = "Invalid name specified"
            , body = ErrorResponse
            , example = json!(examples::error_invalid_name()))
    ),
    tag = "Pipelines"
)]
#[post("/pipelines")]
pub(crate) async fn post_pipeline(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    body: web::Json<PostPutPipeline>,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_descr = body.into_inner().to_descr();
    check_runtime_config(&pipeline_descr.runtime_config)?;
    let pipeline = state
        .db
        .lock()
        .await
        .new_pipeline(
            *tenant_id,
            Uuid::now_v7(),
            &state.common_config.platform_version,
            pipeline_descr,
        )
        .await?;
    let returned_pipeline = PipelineInfo::new(&pipeline);

    info!("Created pipeline {} (tenant: {})", pipeline.id, *tenant_id);
    Ok(HttpResponse::Created()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(returned_pipeline))
}

/// Fully update a pipeline if it already exists, otherwise create a new pipeline.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(
        ("pipeline_name" = String, Path, description = "Unique pipeline name"),
    ),
    request_body(
        content = PostPutPipeline, example = json!(examples::pipeline_post_put())
    ),
    responses(
        (status = CREATED
            , description = "Pipeline successfully created"
            , body = PipelineInfo
            , example = json!(examples::pipeline_1_info())),
        (status = OK
            , description = "Pipeline successfully updated"
            , body = PipelineInfo
            , example = json!(examples::pipeline_1_info())),
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
    path: web::Path<String>,
    body: web::Json<PostPutPipeline>,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_name = path.into_inner();
    let pipeline_descr = body.into_inner().to_descr();
    check_runtime_config(&pipeline_descr.runtime_config)?;
    let (is_new, pipeline) = state
        .db
        .lock()
        .await
        .new_or_update_pipeline(
            *tenant_id,
            Uuid::now_v7(),
            &pipeline_name,
            &state.common_config.platform_version,
            pipeline_descr,
        )
        .await?;
    let returned_pipeline = PipelineInfo::new(&pipeline);

    if is_new {
        info!("Created pipeline {} (tenant: {})", pipeline.id, *tenant_id);
        Ok(HttpResponse::Created()
            .insert_header(CacheControl(vec![CacheDirective::NoCache]))
            .json(returned_pipeline))
    } else {
        info!(
            "Fully updated pipeline {} to version {} (tenant: {})",
            pipeline.id, pipeline.version, *tenant_id
        );
        Ok(HttpResponse::Ok()
            .insert_header(CacheControl(vec![CacheDirective::NoCache]))
            .json(returned_pipeline))
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
            , body = PipelineInfo
            , example = json!(examples::pipeline_1_info())),
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
    path: web::Path<String>,
    body: web::Json<PatchPipeline>,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_name = path.into_inner();
    if let Some(config) = &body.runtime_config {
        check_runtime_config(config)?;
    }
    let pipeline = state
        .db
        .lock()
        .await
        .update_pipeline(
            *tenant_id,
            &pipeline_name,
            &body.name,
            &body.description,
            &state.common_config.platform_version,
            &body.runtime_config,
            &body.program_code,
            &body.udf_rust,
            &body.udf_toml,
            &body.program_config,
        )
        .await?;
    let returned_pipeline = PipelineInfo::new(&pipeline);

    info!(
        "Partially updated pipeline {} to version {} (tenant: {})",
        pipeline.id, pipeline.version, *tenant_id
    );
    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(returned_pipeline))
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
    path: web::Path<String>,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_name = path.into_inner();
    let pipeline_id = state
        .db
        .lock()
        .await
        .delete_pipeline(*tenant_id, &pipeline_name)
        .await?;

    info!("Deleted pipeline {} (tenant: {})", pipeline_id, *tenant_id);
    Ok(HttpResponse::Ok().finish())
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
    path: web::Path<(String, String)>,
) -> Result<HttpResponse, ManagerError> {
    let (pipeline_name, action) = path.into_inner();
    let verb = match action.as_str() {
        "start" => {
            state
                .db
                .lock()
                .await
                .set_deployment_desired_status_running(*tenant_id, &pipeline_name)
                .await?;
            "starting"
        }
        "pause" => {
            state
                .db
                .lock()
                .await
                .set_deployment_desired_status_paused(*tenant_id, &pipeline_name)
                .await?;
            "pausing"
        }
        "shutdown" => {
            state
                .db
                .lock()
                .await
                .set_deployment_desired_status_shutdown(*tenant_id, &pipeline_name)
                .await?;
            "shutting down"
        }
        _ => Err(ManagerError::from(ApiError::InvalidPipelineAction {
            action: action.to_string(),
        }))?,
    };

    info!(
        "Accepted action: {verb} pipeline '{pipeline_name}' (tenant: {})",
        *tenant_id
    );
    Ok(HttpResponse::Accepted().finish())
}

/// Start (resume) or pause the input connector.
///
/// The following values of the `action` argument are accepted: `start` and `pause`.
///
/// Input connectors can be in either the `Running` or `Paused` state. By default,
/// connectors are initialized in the `Running` state when a pipeline is deployed.
/// In this state, the connector actively fetches data from its configured data
/// source and forwards it to the pipeline. If needed, a connector can be created
/// in the `Paused` state by setting its
/// [`paused`](https://docs.feldera.com/connectors/#generic-attributes) property
/// to `true`. When paused, the connector remains idle until reactivated using the
/// `start` command. Conversely, a connector in the `Running` state can be paused
/// at any time by issuing the `pause` command.
///
/// The current connector state can be retrieved via the
/// `GET /v0/pipelines/{pipeline_name}/stats` endpoint.
///
/// Note that only if both the pipeline *and* the connector state is `Running`,
/// is the input connector active.
/// ```text
/// Pipeline state    Connector state    Connector is active?
/// --------------    ---------------    --------------------
/// Paused            Paused             No
/// Paused            Running            No
/// Running           Paused             No
/// Running           Running            Yes
/// ```
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(
        ("pipeline_name" = String, Path, description = "Unique pipeline name"),
        ("table_name" = String, Path, description = "Unique table name"),
        ("connector_name" = String, Path, description = "Unique input connector name"),
        ("action" = String, Path, description = "Input connector action (one of: start, pause)")
    ),
    responses(
        (status = OK
            , description = "Action has been processed"),
        (status = NOT_FOUND
            , description = "Pipeline with that name does not exist"
            , body = ErrorResponse
            , example = json!(examples::error_unknown_pipeline())),
        (status = NOT_FOUND
            , description = "Table with that name does not exist"
            , body = ErrorResponse),
        (status = NOT_FOUND
            , description = "Input connector with that name does not exist"
            , body = ErrorResponse),
    ),
    tag = "Pipelines"
)]
#[post("/pipelines/{pipeline_name}/tables/{table_name}/connectors/{connector_name}/{action}")]
pub(crate) async fn post_pipeline_input_connector_action(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    path: web::Path<(String, String, String, String)>,
) -> Result<HttpResponse, ManagerError> {
    // Parse the URL path parameters
    let (pipeline_name, table_name, connector_name, action) = path.into_inner();

    // Validate action
    let verb = match action.as_str() {
        "start" => "starting",
        "pause" => "pausing",
        _ => {
            return Err(ApiError::InvalidConnectorAction { action }.into());
        }
    };

    // The table name provided by the user is interpreted as
    // a SQL identifier to account for case (in-)sensitivity
    let actual_table_name = SqlIdentifier::from(&table_name).name();
    let endpoint_name = format!("{actual_table_name}.{connector_name}");

    // URL encode endpoint name to account for special characters
    let encoded_endpoint_name = urlencoding::encode(&endpoint_name).to_string();

    // Forward the action request to the pipeline
    let response = state
        .runner
        .forward_http_request_to_pipeline_by_name(
            *tenant_id,
            &pipeline_name,
            Method::GET,
            &format!("input_endpoints/{encoded_endpoint_name}/{action}"),
            "",
            None,
        )
        .await?;

    // Log only if the response indicates success
    if response.status() == StatusCode::OK {
        info!(
            "Connector action: {verb} pipeline '{pipeline_name}' on table '{table_name}' on connector '{connector_name}' (tenant: {})",
            *tenant_id
        );
    }
    Ok(response)
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
            , content_type = "text/plain"
            , body = Vec<u8>),
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
    path: web::Path<String>,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_name = path.into_inner();
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
    path: web::Path<String>,
    request: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_name = path.into_inner();
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
    path: web::Path<String>,
    request: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_name = path.into_inner();
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

/// Checkpoint a running or paused pipeline.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(
        ("pipeline_name" = String, Path, description = "Unique pipeline name"),
    ),
    responses(
        (status = OK
            , description = "Checkpoint completed."),
        (status = NOT_FOUND
            , description = "Pipeline with that name does not exist"
            , body = ErrorResponse
            , example = json!(examples::error_unknown_pipeline())),
        (status = BAD_REQUEST
            , description = "Pipeline is not running or paused, or fault tolerance is not enabled for this pipeline"
            , body = ErrorResponse
            , example = json!(examples::error_pipeline_not_running_or_paused()))
    ),
    tag = "Pipelines"
)]
#[post("/pipelines/{pipeline_name}/checkpoint")]
pub(crate) async fn checkpoint_pipeline(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    path: web::Path<String>,
    request: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    #[cfg(not(feature = "feldera-enterprise"))]
    {
        let _ = (state, tenant_id, path.into_inner(), request);
        Err(CommonError::EnterpriseFeature("checkpoint").into())
    }

    #[cfg(feature = "feldera-enterprise")]
    {
        let pipeline_name = path.into_inner();
        state
            .runner
            .forward_http_request_to_pipeline_by_name(
                *tenant_id,
                &pipeline_name,
                Method::POST,
                "checkpoint",
                request.query_string(),
                Some(Duration::from_secs(120)),
            )
            .await
    }
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
    path: web::Path<String>,
    request: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_name = path.into_inner();
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
    path: web::Path<String>,
    request: HttpRequest,
    body: web::Payload,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_name = path.into_inner();
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

fn check_runtime_config(config: &RuntimeConfig) -> Result<(), ManagerError> {
    #[cfg(not(feature = "feldera-enterprise"))]
    if config.fault_tolerance.is_some() {
        return Err(CommonError::EnterpriseFeature("fault tolerance").into());
    }

    #[cfg(feature = "feldera-enterprise")]
    let _ = config;

    Ok(())
}
