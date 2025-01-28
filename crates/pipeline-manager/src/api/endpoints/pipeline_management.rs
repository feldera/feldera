use crate::api::error::ApiError;
use crate::api::examples;
use crate::api::main::ServerState;
use crate::db::error::DBError;
use crate::db::storage::Storage;
use crate::db::types::pipeline::{
    ExtendedPipelineDescr, ExtendedPipelineDescrMonitoring, PipelineDescr, PipelineDesiredStatus,
    PipelineId, PipelineStatus,
};
use crate::db::types::program::{ProgramConfig, ProgramStatus};
use crate::db::types::tenant::TenantId;
use crate::db::types::version::Version;
use crate::error::ManagerError;
use actix_web::{
    delete, get,
    http::header::{CacheControl, CacheDirective},
    patch, post, put,
    web::{self, Data as WebData, ReqData},
    HttpResponse,
};
use chrono::{DateTime, Utc};
use feldera_types::config::{InputEndpointConfig, OutputEndpointConfig, RuntimeConfig};
use feldera_types::error::ErrorResponse;
use feldera_types::program_schema::ProgramSchema;
use log::info;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::borrow::Cow;
use std::collections::BTreeMap;
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

/// Program information is the result of the SQL compilation.
#[derive(Deserialize, Serialize, ToSchema, Eq, PartialEq, Debug, Clone)]
pub struct PartialProgramInfo {
    /// Schema of the compiled SQL.
    pub schema: ProgramSchema,

    /// Generated user defined function (UDF) stubs Rust code: stubs.rs
    pub udf_stubs: String,

    /// Input connectors derived from the schema.
    pub input_connectors: BTreeMap<Cow<'static, str>, InputEndpointConfig>,

    /// Output connectors derived from the schema.
    pub output_connectors: BTreeMap<Cow<'static, str>, OutputEndpointConfig>,
}

/// Removes the `main_rust` field from the JSON of `program_info`.
fn remove_main_rust_from_program_info(
    mut program_info: Option<serde_json::Value>,
) -> Option<serde_json::Value> {
    if let Some(serde_json::Value::Object(m)) = &mut program_info {
        let _ = m.shift_remove("main_rust");
    }
    program_info
}

/// Pipeline information.
/// It both includes fields which are user-provided and system-generated.
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
    pub program_info: Option<PartialProgramInfo>,
    pub deployment_status: PipelineStatus,
    pub deployment_status_since: DateTime<Utc>,
    pub deployment_desired_status: PipelineDesiredStatus,
    pub deployment_error: Option<ErrorResponse>,
    pub refresh_version: Version,
}

/// Pipeline information (internal).
///
/// This is the struct that is actually serialized when a response body type
/// is [`PipelineInfo`] according to the OpenAPI specification.
/// The difference are the types of `runtime_config`, `program_config` and
/// `program_info` fields, which are JSON values rather than their actual ones.
/// This ensures that even when a backward incompatible change occurred for
/// any of these fields, the API still works (i.e., able to serialize them in
/// order to return pipeline(s)).
#[derive(Serialize, Eq, PartialEq, Debug, Clone)]
pub struct PipelineInfoInternal {
    pub id: PipelineId,
    pub name: String,
    pub description: String,
    pub created_at: DateTime<Utc>,
    pub version: Version,
    pub platform_version: String,
    pub runtime_config: serde_json::Value,
    pub program_code: String,
    pub udf_rust: String,
    pub udf_toml: String,
    pub program_config: serde_json::Value,
    pub program_version: Version,
    pub program_status: ProgramStatus,
    pub program_status_since: DateTime<Utc>,
    pub program_info: Option<serde_json::Value>,
    pub deployment_status: PipelineStatus,
    pub deployment_status_since: DateTime<Utc>,
    pub deployment_desired_status: PipelineDesiredStatus,
    pub deployment_error: Option<ErrorResponse>,
    pub refresh_version: Version,
}

impl PipelineInfoInternal {
    pub(crate) fn new(extended_pipeline: ExtendedPipelineDescr) -> Self {
        PipelineInfoInternal {
            id: extended_pipeline.id,
            name: extended_pipeline.name,
            description: extended_pipeline.description,
            created_at: extended_pipeline.created_at,
            version: extended_pipeline.version,
            platform_version: extended_pipeline.platform_version,
            runtime_config: extended_pipeline.runtime_config,
            program_code: extended_pipeline.program_code,
            udf_rust: extended_pipeline.udf_rust,
            udf_toml: extended_pipeline.udf_toml,
            program_config: extended_pipeline.program_config,
            program_version: extended_pipeline.program_version,
            program_status: extended_pipeline.program_status,
            program_status_since: extended_pipeline.program_status_since,
            program_info: remove_main_rust_from_program_info(extended_pipeline.program_info),
            deployment_status: extended_pipeline.deployment_status,
            deployment_status_since: extended_pipeline.deployment_status_since,
            deployment_desired_status: extended_pipeline.deployment_desired_status,
            deployment_error: extended_pipeline.deployment_error,
            refresh_version: extended_pipeline.refresh_version,
        }
    }
}

/// Pipeline information which has a selected subset of optional fields.
/// It both includes fields which are user-provided and system-generated.
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
    pub program_info: Option<Option<PartialProgramInfo>>,
    pub deployment_status: PipelineStatus,
    pub deployment_status_since: DateTime<Utc>,
    pub deployment_desired_status: PipelineDesiredStatus,
    pub deployment_error: Option<ErrorResponse>,
    pub refresh_version: Version,
}

/// Pipeline information which has a selected subset of optional fields (internal).
///
/// This is the struct that is actually serialized when a response body type
/// is [`PipelineSelectedInfo`] according to the OpenAPI specification.
/// This distinction is to have the API work even if there were backward
/// incompatible changes in the complex struct fields, for more information see:
/// [`PipelineInfoInternal`].
#[derive(Serialize, Eq, PartialEq, Debug, Clone)]
pub struct PipelineSelectedInfoInternal {
    pub id: PipelineId,
    pub name: String,
    pub description: String,
    pub created_at: DateTime<Utc>,
    pub version: Version,
    pub platform_version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime_config: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub program_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub udf_rust: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub udf_toml: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub program_config: Option<serde_json::Value>,
    pub program_version: Version,
    pub program_status: ProgramStatus,
    pub program_status_since: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub program_info: Option<Option<serde_json::Value>>,
    pub deployment_status: PipelineStatus,
    pub deployment_status_since: DateTime<Utc>,
    pub deployment_desired_status: PipelineDesiredStatus,
    pub deployment_error: Option<ErrorResponse>,
    pub refresh_version: Version,
}

impl PipelineSelectedInfoInternal {
    pub(crate) fn new_all(extended_pipeline: ExtendedPipelineDescr) -> Self {
        PipelineSelectedInfoInternal {
            id: extended_pipeline.id,
            name: extended_pipeline.name,
            description: extended_pipeline.description,
            created_at: extended_pipeline.created_at,
            version: extended_pipeline.version,
            platform_version: extended_pipeline.platform_version,
            runtime_config: Some(extended_pipeline.runtime_config),
            program_code: Some(extended_pipeline.program_code),
            udf_rust: Some(extended_pipeline.udf_rust),
            udf_toml: Some(extended_pipeline.udf_toml),
            program_config: Some(extended_pipeline.program_config),
            program_version: extended_pipeline.program_version,
            program_status: extended_pipeline.program_status,
            program_status_since: extended_pipeline.program_status_since,
            program_info: Some(remove_main_rust_from_program_info(
                extended_pipeline.program_info,
            )),
            deployment_status: extended_pipeline.deployment_status,
            deployment_status_since: extended_pipeline.deployment_status_since,
            deployment_desired_status: extended_pipeline.deployment_desired_status,
            deployment_error: extended_pipeline.deployment_error,
            refresh_version: extended_pipeline.refresh_version,
        }
    }

    pub(crate) fn new_status(extended_pipeline: ExtendedPipelineDescrMonitoring) -> Self {
        PipelineSelectedInfoInternal {
            id: extended_pipeline.id,
            name: extended_pipeline.name,
            description: extended_pipeline.description,
            created_at: extended_pipeline.created_at,
            version: extended_pipeline.version,
            platform_version: extended_pipeline.platform_version,
            runtime_config: None,
            program_code: None,
            udf_rust: None,
            udf_toml: None,
            program_config: None,
            program_version: extended_pipeline.program_version,
            program_status: extended_pipeline.program_status,
            program_status_since: extended_pipeline.program_status_since,
            program_info: None,
            deployment_status: extended_pipeline.deployment_status,
            deployment_status_since: extended_pipeline.deployment_status_since,
            deployment_desired_status: extended_pipeline.deployment_desired_status,
            deployment_error: extended_pipeline.deployment_error,
            refresh_version: extended_pipeline.refresh_version,
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
    /// - `refresh_version`
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
    /// - `refresh_version`
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
/// Fields which are optional and not provided will be set to their empty type value
/// (for strings: an empty string `""`, for objects: an empty dictionary `{}`).
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

/// Create a new pipeline (POST), or fully update an existing pipeline (PUT) (internal).
/// Fields which are optional and not provided will be set to their default value.
///
/// This is the struct that is actually serialized when a request body type
/// is [`PostPutPipeline`] according to the OpenAPI specification.
/// This preserves the original JSON for the `runtime_config` and `program_config` fields.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct PostPutPipelineInternal {
    pub name: String,
    pub description: Option<String>,
    pub runtime_config: Option<serde_json::Value>,
    pub program_code: String,
    pub udf_rust: Option<String>,
    pub udf_toml: Option<String>,
    pub program_config: Option<serde_json::Value>,
}

impl From<PostPutPipelineInternal> for PipelineDescr {
    /// Fills in any missing optional field with its empty type value
    /// (for strings: an empty string `""`, for objects: an empty dictionary `{}`).
    fn from(value: PostPutPipelineInternal) -> Self {
        PipelineDescr {
            name: value.name.clone(),
            description: value.description.clone().unwrap_or("".to_string()),
            runtime_config: value.runtime_config.clone().unwrap_or(json!({})),
            program_code: value.program_code.clone(),
            udf_rust: value.udf_rust.clone().unwrap_or("".to_string()),
            udf_toml: value.udf_toml.clone().unwrap_or("".to_string()),
            program_config: value.program_config.clone().unwrap_or(json!({})),
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

/// Partially update the pipeline (PATCH) (internal).
///
/// This is the struct that is actually serialized when a request body type
/// is [`PatchPipeline`] according to the OpenAPI specification.
/// This preserves the original JSON for the `runtime_config` and `program_config` fields.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct PatchPipelineInternal {
    pub name: Option<String>,
    pub description: Option<String>,
    pub runtime_config: Option<serde_json::Value>,
    pub program_code: Option<String>,
    pub udf_rust: Option<String>,
    pub udf_toml: Option<String>,
    pub program_config: Option<serde_json::Value>,
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
    tag = "Pipeline management"
)]
#[get("/pipelines")]
pub(crate) async fn list_pipelines(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    query: web::Query<GetPipelineParameters>,
) -> Result<HttpResponse, DBError> {
    let returned_pipelines: Vec<PipelineSelectedInfoInternal> = match &query.selector {
        PipelineFieldSelector::All => {
            let pipelines = state.db.lock().await.list_pipelines(*tenant_id).await?;
            pipelines
                .into_iter()
                .map(PipelineSelectedInfoInternal::new_all)
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
                .into_iter()
                .map(PipelineSelectedInfoInternal::new_status)
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
    tag = "Pipeline management"
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
            PipelineSelectedInfoInternal::new_all(pipeline)
        }
        PipelineFieldSelector::Status => {
            let pipeline = state
                .db
                .lock()
                .await
                .get_pipeline_for_monitoring(*tenant_id, &pipeline_name)
                .await?;
            PipelineSelectedInfoInternal::new_status(pipeline)
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
    tag = "Pipeline management"
)]
#[post("/pipelines")]
pub(crate) async fn post_pipeline(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    body: web::Json<PostPutPipelineInternal>,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_descr: PipelineDescr = body.into_inner().into();
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
    let returned_pipeline = PipelineInfoInternal::new(pipeline);

    info!(
        "Created pipeline {} (tenant: {})",
        returned_pipeline.id, *tenant_id
    );
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
    tag = "Pipeline management"
)]
#[put("/pipelines/{pipeline_name}")]
async fn put_pipeline(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    path: web::Path<String>,
    body: web::Json<PostPutPipelineInternal>,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_name = path.into_inner();
    let pipeline_descr: PipelineDescr = body.into_inner().into();
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
    let returned_pipeline = PipelineInfoInternal::new(pipeline);

    if is_new {
        info!(
            "Created pipeline {} (tenant: {})",
            returned_pipeline.id, *tenant_id
        );
        Ok(HttpResponse::Created()
            .insert_header(CacheControl(vec![CacheDirective::NoCache]))
            .json(returned_pipeline))
    } else {
        info!(
            "Fully updated pipeline {} to version {} (tenant: {})",
            returned_pipeline.id, returned_pipeline.version, *tenant_id
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
    tag = "Pipeline management"
)]
#[patch("/pipelines/{pipeline_name}")]
pub(crate) async fn patch_pipeline(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    path: web::Path<String>,
    body: web::Json<PatchPipelineInternal>,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_name = path.into_inner();
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
    let returned_pipeline = PipelineInfoInternal::new(pipeline);

    info!(
        "Partially updated pipeline {} to version {} (tenant: {})",
        returned_pipeline.id, returned_pipeline.version, *tenant_id
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
    tag = "Pipeline management"
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

/// Sets the desired deployment state of a pipeline.
///
/// The desired state is set based on the `action` path parameter:
/// - `/start` sets desired state to `Running`
/// - `/pause` sets desired state to `Paused`
/// - `/shutdown` sets desired state to `Shutdown`
///
/// The endpoint returns immediately after setting the desired state.
/// The relevant procedure to get to the desired state is performed asynchronously,
/// and, as such, progress should be monitored by polling the pipeline using the
/// `GET` endpoints.
///
/// Note the following:
/// - A shutdown pipeline can be started through calling either `/start` or `/pause`
/// - Both starting as running and resuming a pipeline is done by calling `/start`
/// - Both starting as paused and pausing a pipeline is done by calling `/pause`
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
                ("Illegal action" = (description = "Action is not applicable in the current state", value = json!(examples::error_illegal_pipeline_action()))),
                ("Invalid action" = (description = "Invalid action specified", value = json!(examples::error_invalid_pipeline_action()))),
            )
        ),
    ),
    tag = "Pipeline management"
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
