#[cfg(feature = "feldera-enterprise")]
use crate::api::error::ApiError;
use crate::api::examples;
use crate::api::main::ServerState;
#[cfg(not(feature = "feldera-enterprise"))]
use crate::common_error::CommonError;
use crate::db::error::DBError;
use crate::db::storage::Storage;
use crate::db::types::pipeline::{
    ExtendedPipelineDescr, ExtendedPipelineDescrMonitoring, PipelineDescr, PipelineDesiredStatus,
    PipelineId, PipelineStatus,
};
use crate::db::types::program::{ProgramConfig, ProgramError, ProgramStatus};
use crate::db::types::storage::StorageStatus;
use crate::db::types::tenant::TenantId;
use crate::db::types::version::Version;
use crate::error::ManagerError;
#[cfg(feature = "feldera-enterprise")]
use actix_http::body::MessageBody;
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
#[cfg(feature = "feldera-enterprise")]
use feldera_types::suspend::SuspendableResponse;
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
fn remove_large_fields_from_program_info(
    mut program_info: Option<serde_json::Value>,
) -> Option<serde_json::Value> {
    if let Some(serde_json::Value::Object(m)) = &mut program_info {
        let _ = m.shift_remove("main_rust");
        let _ = m.shift_remove("dataflow");
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
    pub program_error: ProgramError,
    pub program_info: Option<PartialProgramInfo>,
    pub deployment_status: PipelineStatus,
    pub deployment_status_since: DateTime<Utc>,
    pub deployment_desired_status: PipelineDesiredStatus,
    pub deployment_error: Option<ErrorResponse>,
    pub refresh_version: Version,
    pub storage_status: StorageStatus,
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
    pub program_error: ProgramError,
    pub program_info: Option<serde_json::Value>,
    pub deployment_status: PipelineStatus,
    pub deployment_status_since: DateTime<Utc>,
    pub deployment_desired_status: PipelineDesiredStatus,
    pub deployment_error: Option<ErrorResponse>,
    pub refresh_version: Version,
    pub storage_status: StorageStatus,
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
            program_error: extended_pipeline.program_error,
            program_info: remove_large_fields_from_program_info(extended_pipeline.program_info),
            deployment_status: extended_pipeline.deployment_status,
            deployment_status_since: extended_pipeline.deployment_status_since,
            deployment_desired_status: extended_pipeline.deployment_desired_status,
            deployment_error: extended_pipeline.deployment_error,
            refresh_version: extended_pipeline.refresh_version,
            storage_status: extended_pipeline.storage_status,
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
    pub program_error: Option<ProgramError>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub program_info: Option<Option<PartialProgramInfo>>,
    pub deployment_status: PipelineStatus,
    pub deployment_status_since: DateTime<Utc>,
    pub deployment_desired_status: PipelineDesiredStatus,
    pub deployment_error: Option<ErrorResponse>,
    pub refresh_version: Version,
    pub storage_status: StorageStatus,
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
    pub program_error: Option<ProgramError>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub program_info: Option<Option<serde_json::Value>>,
    pub deployment_status: PipelineStatus,
    pub deployment_status_since: DateTime<Utc>,
    pub deployment_desired_status: PipelineDesiredStatus,
    pub deployment_error: Option<ErrorResponse>,
    pub refresh_version: Version,
    pub storage_status: StorageStatus,
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
            program_error: Some(extended_pipeline.program_error),
            program_info: Some(remove_large_fields_from_program_info(
                extended_pipeline.program_info,
            )),
            deployment_status: extended_pipeline.deployment_status,
            deployment_status_since: extended_pipeline.deployment_status_since,
            deployment_desired_status: extended_pipeline.deployment_desired_status,
            deployment_error: extended_pipeline.deployment_error,
            refresh_version: extended_pipeline.refresh_version,
            storage_status: extended_pipeline.storage_status,
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
            program_error: None,
            program_info: None,
            deployment_status: extended_pipeline.deployment_status,
            deployment_status_since: extended_pipeline.deployment_status_since,
            deployment_desired_status: extended_pipeline.deployment_desired_status,
            deployment_error: extended_pipeline.deployment_error,
            refresh_version: extended_pipeline.refresh_version,
            storage_status: extended_pipeline.storage_status,
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
    /// - `program_error`
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
    /// - `program_error`
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

/// Default for the `force` query parameter when POST a pipeline stop.
fn default_pipeline_stop_force() -> bool {
    false
}

/// Query parameters to POST a pipeline stop.
#[derive(Debug, Deserialize, IntoParams, ToSchema)]
pub struct PostStopPipelineParameters {
    /// The `force` parameter determines whether to immediately deprovision the pipeline compute
    /// resources (`force=true`) or first attempt to atomically checkpoint before doing so
    /// (`force=false`, which is the default).
    #[serde(default = "default_pipeline_stop_force")]
    force: bool,
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
            , example = json!(examples::error_unknown_pipeline_name())),
        (status = INTERNAL_SERVER_ERROR, body = ErrorResponse),
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

/// Retrieve the program info of a pipeline.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(
        ("pipeline_name" = String, Path, description = "Unique pipeline name"),
    ),
    responses(
        (status = OK
            , description = "Pipeline retrieved successfully"
            , body = ProgramInfo
            , example = json!(examples::pipeline_1_selected_info())),
        (status = NOT_FOUND
            , description = "Pipeline with that name does not exist"
            , body = ErrorResponse
            , example = json!(examples::error_unknown_pipeline_name())),
        (status = INTERNAL_SERVER_ERROR, body = ErrorResponse),
    ),
    tag = "Pipeline management"
)]
#[get("/pipelines/{pipeline_name}/program_info")]
pub(crate) async fn get_program_info(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    path: web::Path<String>,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_name = path.into_inner();
    log::error!("pipeline_name {:?}", pipeline_name);
    let pipeline = state
        .db
        .lock()
        .await
        .get_pipeline(*tenant_id, &pipeline_name)
        .await?;
    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(&pipeline.program_info))
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
            , body = ErrorResponse
            , examples(
                ("Name does not match pattern" = (value = json!(examples::error_name_does_not_match_pattern())))
            )
        ),
        (status = INTERNAL_SERVER_ERROR, body = ErrorResponse),
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
            , description = "Cannot rename pipeline as the new name already exists"
            , body = ErrorResponse
            , example = json!(examples::error_duplicate_name())),
        (status = BAD_REQUEST
            , body = ErrorResponse
            , examples(
                ("Name does not match pattern" = (value = json!(examples::error_name_does_not_match_pattern()))),
                ("Cannot update non-shutdown pipeline" = (value = json!(examples::error_cannot_update_non_shutdown_pipeline())))
            )
        ),
        (status = INTERNAL_SERVER_ERROR, body = ErrorResponse),
    ),
    tag = "Pipeline management"
)]
#[put("/pipelines/{pipeline_name}")]
pub(crate) async fn put_pipeline(
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
            , example = json!(examples::error_unknown_pipeline_name())),
        (status = CONFLICT
            , description = "Cannot rename pipeline as the name already exists"
            , body = ErrorResponse
            , example = json!(examples::error_duplicate_name())),
        (status = BAD_REQUEST
            , body = ErrorResponse
            , examples(
                ("Name does not match pattern" = (value = json!(examples::error_name_does_not_match_pattern()))),
                ("Cannot update non-shutdown pipeline" = (value = json!(examples::error_cannot_update_non_shutdown_pipeline())))
            )
        ),
        (status = INTERNAL_SERVER_ERROR, body = ErrorResponse),
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
            , example = json!(examples::error_unknown_pipeline_name())),
        (status = BAD_REQUEST
            , description = "Pipeline needs to be shutdown to be deleted"
            , body = ErrorResponse
            , example = json!(examples::error_cannot_delete_non_shutdown_pipeline())),
        (status = INTERNAL_SERVER_ERROR, body = ErrorResponse),
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

/// Start the pipeline asynchronously by updating the desired state.
///
/// The endpoint returns immediately after setting the desired state to `Running`.
/// The procedure to get to the desired state is performed asynchronously.
/// Progress should be monitored by polling the pipeline `GET` endpoints.
///
/// Note the following:
/// - A stopped pipeline can be started through calling either `/start` or `/pause`
/// - Both starting as running and resuming a pipeline is done by calling `/start`
/// - A pipeline which is in the process of suspending or stopping cannot be started
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(
        ("pipeline_name" = String, Path, description = "Unique pipeline name")
    ),
    responses(
        (status = ACCEPTED
            , description = "Action is accepted and is being performed"),
        (status = NOT_FOUND
            , description = "Pipeline with that name does not exist"
            , body = ErrorResponse
            , example = json!(examples::error_unknown_pipeline_name())),
        (status = BAD_REQUEST
            , description = "Action could not be performed"
            , body = ErrorResponse
            , example = json!(examples::error_illegal_pipeline_action())),
        (status = INTERNAL_SERVER_ERROR, body = ErrorResponse),
    ),
    tag = "Pipeline management"
)]
#[post("/pipelines/{pipeline_name}/start")]
pub(crate) async fn post_pipeline_start(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    path: web::Path<String>,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_name = path.into_inner();
    let pipeline_id = state
        .db
        .lock()
        .await
        .set_deployment_desired_status_running(*tenant_id, &pipeline_name)
        .await?;
    info!(
        "Accepted action: going to start pipeline {pipeline_id} (tenant: {})",
        *tenant_id
    );
    Ok(HttpResponse::Accepted().finish())
}

/// Pause the pipeline asynchronously by updating the desired state.
///
/// The endpoint returns immediately after setting the desired state to `Paused`.
/// The procedure to get to the desired state is performed asynchronously.
/// Progress should be monitored by polling the pipeline `GET` endpoints.
///
/// Note the following:
/// - A stopped pipeline can be started through calling either `/start` or `/pause`
/// - Both starting as paused and pausing a pipeline is done by calling `/pause`
/// - A pipeline which is in the process of suspending or stopping cannot be paused
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(
        ("pipeline_name" = String, Path, description = "Unique pipeline name")
    ),
    responses(
        (status = ACCEPTED
            , description = "Action is accepted and is being performed"),
        (status = NOT_FOUND
            , description = "Pipeline with that name does not exist"
            , body = ErrorResponse
            , example = json!(examples::error_unknown_pipeline_name())),
        (status = BAD_REQUEST
            , description = "Action could not be performed"
            , body = ErrorResponse
            , example = json!(examples::error_illegal_pipeline_action())),
        (status = INTERNAL_SERVER_ERROR, body = ErrorResponse),
    ),
    tag = "Pipeline management"
)]
#[post("/pipelines/{pipeline_name}/pause")]
pub(crate) async fn post_pipeline_pause(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    path: web::Path<String>,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_name = path.into_inner();
    let pipeline_id = state
        .db
        .lock()
        .await
        .set_deployment_desired_status_paused(*tenant_id, &pipeline_name)
        .await?;
    info!(
        "Accepted action: going to pause pipeline {pipeline_id} (tenant: {})",
        *tenant_id
    );
    Ok(HttpResponse::Accepted().finish())
}

/// Stop the pipeline asynchronously by updating the desired state.
///
/// There are two variants:
/// - `/stop?force=false` (default): the pipeline will first atomically checkpoint before
///   deprovisioning the compute resources. When resuming, the pipeline will start from this
//    checkpoint.
/// - `/stop?force=true`: the compute resources will be immediately deprovisioned. When resuming,
///   it will pick up the latest checkpoint made by the periodic checkpointer or by a prior
///   `/checkpoint` call.
///
/// The endpoint returns immediately after setting the desired state to `Suspended` for
/// `?force=false` or `Stopped` for `?force=true`. In the former case, once the pipeline has
/// successfully passes the `Suspending` state, the desired state will become `Stopped` as well.
/// The procedure to get to the desired state is performed asynchronously. Progress should be
/// monitored by polling the pipeline `GET` endpoints.
///
/// Note the following:
/// - The suspending that is done with `/stop?force=false` is not guaranteed to succeed:
///   - If an error is returned during the suspension, the pipeline will be forcefully stopped with
///     that error set
///   - Otherwise, it will keep trying to suspend, in which case it is possible to cancel suspending
///     by calling `/stop?force=true`
/// - `/stop?force=true` cannot be cancelled: the pipeline must first reach `Stopped` before another
///    action can be done
/// - A pipeline which is in the process of suspending or stopping can only be forcefully stopped
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(
        ("pipeline_name" = String, Path, description = "Unique pipeline name"),
        PostStopPipelineParameters,
    ),
    responses(
        (status = ACCEPTED
            , description = "Action is accepted and is being performed"),
        (status = NOT_FOUND
            , description = "Pipeline with that name does not exist"
            , body = ErrorResponse
            , example = json!(examples::error_unknown_pipeline_name())),
        (status = BAD_REQUEST
            , description = "Action could not be performed"
            , body = ErrorResponse
            , example = json!(examples::error_illegal_pipeline_action())),
        (status = SERVICE_UNAVAILABLE
            , description = "Action can not be performed (maybe because the pipeline is already suspended)"
            , body = ErrorResponse
        ),
        (status = METHOD_NOT_ALLOWED
            , description = "Action is not supported"
            , body = ErrorResponse
            , examples(
                ("Unsupported action" = (value = json!(examples::error_unsupported_pipeline_action()))),
            )
        ),
        (status = NOT_IMPLEMENTED
            , description = "Action is not implemented because it is only available in the Enterprise edition"
            , body = ErrorResponse
        ),
        (status = INTERNAL_SERVER_ERROR, body = ErrorResponse),
    ),
    tag = "Pipeline management"
)]
#[post("/pipelines/{pipeline_name}/stop")]
pub(crate) async fn post_pipeline_stop(
    state: WebData<ServerState>,
    _client: WebData<awc::Client>,
    tenant_id: ReqData<TenantId>,
    path: web::Path<String>,
    query: web::Query<PostStopPipelineParameters>,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_name = path.into_inner();
    let pipeline_id = if query.force {
        state
            .db
            .lock()
            .await
            .set_deployment_desired_status_stopped(*tenant_id, &pipeline_name)
            .await?
    } else {
        #[cfg(not(feature = "feldera-enterprise"))]
        {
            Err(ManagerError::from(CommonError::EnterpriseFeature(
                "stop?force=false",
            )))?
        }
        #[cfg(feature = "feldera-enterprise")]
        {
            use crate::runner::error::RunnerError;
            // Check whether the pipeline can be suspended
            let response = state
                .runner
                .forward_http_request_to_pipeline_by_name(
                    _client.as_ref(),
                    *tenant_id,
                    &pipeline_name,
                    actix_http::Method::GET,
                    "suspendable",
                    "",
                    None,
                )
                .await?;
            let suspendable_response = if response.status().is_success() {
                let body = response.into_body();
                if let Ok(b) = body.try_into_bytes() {
                    if let Ok(v) = serde_json::from_slice::<SuspendableResponse>(&b) {
                        v
                    } else {
                        Err(RunnerError::PipelineInteractionInvalidResponse { error: format!("Pipeline returned an invalid response to a /suspendable request: {}", String::from_utf8_lossy(&b)) })?
                    }
                } else {
                    Err(RunnerError::PipelineInteractionInvalidResponse {
                        error: "Error processing pipelines's response to a /suspendable request: failed to extract response body".to_string()
                    })?
                }
            } else {
                return Ok(response);
            };
            if suspendable_response.suspendable {
                state
                    .db
                    .lock()
                    .await
                    .set_deployment_desired_status_suspended(*tenant_id, &pipeline_name)
                    .await?
            } else {
                let reasons = suspendable_response
                    .reasons
                    .iter()
                    .map(|reason| format!("   - {reason}"))
                    .collect::<Vec<_>>()
                    .join("\n");
                Err(ManagerError::from(ApiError::UnsupportedPipelineAction {
                    action: "stop?force=false".to_string(),
                    reason: format!(
                        "this pipeline does not support the stop without force (\"suspend\") operation for the following reason(s):\n{reasons}"
                    ),
                }))?
            }
        }
    };

    info!(
        "Accepted action: going to {}stop pipeline {pipeline_id} (tenant: {})",
        if query.force { "forcefully " } else { "" },
        *tenant_id
    );
    Ok(HttpResponse::Accepted().finish())
}

/// Unbind the pipeline storage asynchronously.
///
/// IMPORTANT: Unbinding means disassociating the storage from the pipeline.
///            Depending on the storage type this can include its deletion.
///
/// It sets the storage state to `Unbinding`, after which the unbinding process is
/// performed asynchronously. Progress should be monitored by polling the pipeline
/// using the `GET` endpoints. An `/unbind` cannot be cancelled.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(
        ("pipeline_name" = String, Path, description = "Unique pipeline name")
    ),
    responses(
        (status = ACCEPTED
            , description = "Action is accepted and is being performed"),
        (status = NOT_FOUND
            , description = "Pipeline with that name does not exist"
            , body = ErrorResponse
            , example = json!(examples::error_unknown_pipeline_name())),
        (status = BAD_REQUEST
            , description = "Action could not be performed"
            , body = ErrorResponse
            , examples(
                ("Illegal action" = (value = json!(examples::error_illegal_pipeline_storage_action()))),
            )
        ),
        (status = INTERNAL_SERVER_ERROR, body = ErrorResponse),
    ),
    tag = "Pipeline management"
)]
#[post("/pipelines/{pipeline_name}/unbind")]
pub(crate) async fn post_pipeline_unbind(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    path: web::Path<String>,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_name = path.into_inner();
    let pipeline_id = state
        .db
        .lock()
        .await
        .transit_storage_status_to_unbinding(*tenant_id, &pipeline_name)
        .await?;

    info!(
        "Accepted storage action: going to unbind storage of pipeline {pipeline_id} (tenant: {})",
        *tenant_id
    );
    Ok(HttpResponse::Accepted().finish())
}

/// Retrieve logs of a pipeline as a stream.
///
/// The logs stream catches up to the extent of the internally configured per-pipeline
/// circular logs buffer (limited to a certain byte size and number of lines, whichever
/// is reached first). After the catch-up, new lines are pushed whenever they become
/// available.
///
/// It is possible for the logs stream to end prematurely due to the API server temporarily losing
/// connection to the runner. In this case, it is needed to issue again a new request to this
/// endpoint.
///
/// The logs stream will end when the pipeline is deleted, or if the runner restarts. Note that in
/// both cases the logs will be cleared.
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
            , example = json!(examples::error_unknown_pipeline_name())),
        (status = SERVICE_UNAVAILABLE
            , body = ErrorResponse
            , examples(
                ("Pipeline is shutdown" = (value = json!(examples::error_runner_interaction_shutdown()))),
                ("Runner response timeout" = (value = json!(examples::error_runner_interaction_timeout())))
            )
        ),
        (status = INTERNAL_SERVER_ERROR, body = ErrorResponse),
    ),
    tag = "Pipeline interaction"
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
