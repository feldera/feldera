use crate::api::error::ApiError;
use crate::api::examples;
use crate::api::main::ServerState;
#[cfg(not(feature = "feldera-enterprise"))]
use crate::common_error::CommonError;
use crate::db::error::DBError;
use crate::db::storage::Storage;
use crate::db::types::combined_status::{combine_since, CombinedDesiredStatus, CombinedStatus};
use crate::db::types::pipeline::{
    ExtendedPipelineDescr, ExtendedPipelineDescrMonitoring, PipelineDescr, PipelineId,
};
use crate::db::types::program::{ProgramConfig, ProgramError, ProgramStatus};
use crate::db::types::resources_status::{ResourcesDesiredStatus, ResourcesStatus};
use crate::db::types::storage::StorageStatus;
use crate::db::types::tenant::TenantId;
use crate::db::types::version::Version;
use crate::error::ManagerError;
use crate::has_unstable_feature;
#[cfg(feature = "feldera-enterprise")]
use actix_web::http::Method;
use actix_web::{
    delete, get,
    http::header::{CacheControl, CacheDirective},
    patch, post, put,
    web::{self, Data as WebData, ReqData},
    HttpResponse,
};
use chrono::{DateTime, Utc};
use feldera_types::adapter_stats::PipelineStatsErrorsResponse;
use feldera_types::config::{InputEndpointConfig, OutputEndpointConfig, RuntimeConfig};
use feldera_types::error::ErrorResponse;
use feldera_types::program_schema::ProgramSchema;
use feldera_types::runtime_status::{BootstrapPolicy, RuntimeDesiredStatus, RuntimeStatus};
use futures_util::future::join_all;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::borrow::Cow;
use std::collections::BTreeMap;
#[cfg(feature = "feldera-enterprise")]
use std::time::Duration;
use tracing::{debug, error, info};
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

/// Aggregated connector error statistics.
///
/// This structure contains the sum of all error counts across all input and output connectors
/// for a pipeline.
#[derive(Serialize, Deserialize, ToSchema, Eq, PartialEq, Debug, Clone)]
pub struct ConnectorStats {
    /// Total number of errors across all connectors.
    ///
    /// This is the sum of:
    /// - `num_transport_errors` from all input connectors
    /// - `num_parse_errors` from all input connectors
    /// - `num_encode_errors` from all output connectors
    /// - `num_transport_errors` from all output connectors
    pub num_errors: u64,
}

/// Pipeline information.
/// It both includes fields which are user-provided and system-generated.
#[derive(Serialize, ToSchema, PartialEq, Debug, Clone)]
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
    pub deployment_error: Option<ErrorResponse>,
    pub refresh_version: Version,
    pub storage_status: StorageStatus,
    pub deployment_id: Option<Uuid>,
    pub deployment_initial: Option<RuntimeDesiredStatus>,
    pub deployment_status: CombinedStatus,
    pub deployment_status_since: DateTime<Utc>,
    pub deployment_desired_status: CombinedDesiredStatus,
    pub deployment_desired_status_since: DateTime<Utc>,
    pub deployment_resources_status: ResourcesStatus,
    pub deployment_resources_status_since: DateTime<Utc>,
    pub deployment_resources_desired_status: ResourcesDesiredStatus,
    pub deployment_resources_desired_status_since: DateTime<Utc>,
    pub deployment_runtime_status: Option<RuntimeStatus>,
    pub deployment_runtime_status_since: Option<DateTime<Utc>>,
    pub deployment_runtime_desired_status: Option<RuntimeDesiredStatus>,
    pub deployment_runtime_desired_status_since: Option<DateTime<Utc>>,
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
    pub deployment_error: Option<ErrorResponse>,
    pub refresh_version: Version,
    pub storage_status: StorageStatus,
    pub deployment_id: Option<Uuid>,
    pub deployment_initial: Option<RuntimeDesiredStatus>,
    pub deployment_status: CombinedStatus,
    pub deployment_status_since: DateTime<Utc>,
    pub deployment_desired_status: CombinedDesiredStatus,
    pub deployment_desired_status_since: DateTime<Utc>,
    pub deployment_resources_status: ResourcesStatus,
    pub deployment_resources_status_since: DateTime<Utc>,
    pub deployment_resources_desired_status: ResourcesDesiredStatus,
    pub deployment_resources_desired_status_since: DateTime<Utc>,
    pub deployment_runtime_status: Option<RuntimeStatus>,
    pub deployment_runtime_status_since: Option<DateTime<Utc>>,
    pub deployment_runtime_desired_status: Option<RuntimeDesiredStatus>,
    pub deployment_runtime_desired_status_since: Option<DateTime<Utc>>,
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
            deployment_error: extended_pipeline.deployment_error,
            refresh_version: extended_pipeline.refresh_version,
            storage_status: extended_pipeline.storage_status,
            deployment_id: extended_pipeline.deployment_id,
            deployment_initial: extended_pipeline.deployment_initial,
            deployment_status: CombinedStatus::new(
                extended_pipeline.deployment_resources_status,
                extended_pipeline.deployment_runtime_status,
            ),
            deployment_status_since: combine_since(
                extended_pipeline.deployment_resources_status_since,
                extended_pipeline.deployment_runtime_status_since,
            ),
            deployment_desired_status: CombinedDesiredStatus::new(
                extended_pipeline.deployment_resources_desired_status,
                extended_pipeline.deployment_initial,
                extended_pipeline.deployment_runtime_desired_status,
            ),
            deployment_desired_status_since: combine_since(
                extended_pipeline.deployment_resources_desired_status_since,
                extended_pipeline.deployment_runtime_desired_status_since,
            ),
            deployment_resources_status: extended_pipeline.deployment_resources_status,
            deployment_resources_status_since: extended_pipeline.deployment_resources_status_since,
            deployment_resources_desired_status: extended_pipeline
                .deployment_resources_desired_status,
            deployment_resources_desired_status_since: extended_pipeline
                .deployment_resources_desired_status_since,
            deployment_runtime_status: extended_pipeline.deployment_runtime_status,
            deployment_runtime_status_since: extended_pipeline.deployment_runtime_status_since,
            deployment_runtime_desired_status: extended_pipeline.deployment_runtime_desired_status,
            deployment_runtime_desired_status_since: extended_pipeline
                .deployment_runtime_desired_status_since,
        }
    }
}

/// Pipeline information which has a selected subset of optional fields.
/// It both includes fields which are user-provided and system-generated.
/// If an optional field is not selected (i.e., is `None`), it will not be serialized.
#[derive(Serialize, ToSchema, PartialEq, Debug, Clone)]
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
    pub deployment_error: Option<ErrorResponse>,
    pub refresh_version: Version,
    pub storage_status: StorageStatus,
    pub deployment_id: Option<Uuid>,
    pub deployment_initial: Option<RuntimeDesiredStatus>,
    pub deployment_status: CombinedStatus,
    pub deployment_status_since: DateTime<Utc>,
    pub deployment_desired_status: CombinedDesiredStatus,
    pub deployment_desired_status_since: DateTime<Utc>,
    pub deployment_resources_status: ResourcesStatus,
    pub deployment_resources_status_since: DateTime<Utc>,
    pub deployment_resources_desired_status: ResourcesDesiredStatus,
    pub deployment_resources_desired_status_since: DateTime<Utc>,
    pub deployment_runtime_status: Option<RuntimeStatus>,
    pub deployment_runtime_status_details: Option<serde_json::Value>,
    pub deployment_runtime_status_since: Option<DateTime<Utc>>,
    pub deployment_runtime_desired_status: Option<RuntimeDesiredStatus>,
    pub deployment_runtime_desired_status_since: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connectors: Option<ConnectorStats>,
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
    pub deployment_error: Option<ErrorResponse>,
    pub refresh_version: Version,
    pub storage_status: StorageStatus,
    pub deployment_id: Option<Uuid>,
    pub deployment_initial: Option<RuntimeDesiredStatus>,
    pub deployment_status: CombinedStatus,
    pub deployment_status_since: DateTime<Utc>,
    pub deployment_desired_status: CombinedDesiredStatus,
    pub deployment_desired_status_since: DateTime<Utc>,
    pub deployment_resources_status: ResourcesStatus,
    pub deployment_resources_status_since: DateTime<Utc>,
    pub deployment_resources_desired_status: ResourcesDesiredStatus,
    pub deployment_resources_desired_status_since: DateTime<Utc>,
    pub deployment_runtime_status: Option<RuntimeStatus>,
    pub deployment_runtime_status_details: Option<serde_json::Value>,
    pub deployment_runtime_status_since: Option<DateTime<Utc>>,
    pub deployment_runtime_desired_status: Option<RuntimeDesiredStatus>,
    pub deployment_runtime_desired_status_since: Option<DateTime<Utc>>,
    pub bootstrap_policy: Option<BootstrapPolicy>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connectors: Option<ConnectorStats>,
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
            deployment_error: extended_pipeline.deployment_error,
            refresh_version: extended_pipeline.refresh_version,
            storage_status: extended_pipeline.storage_status,
            deployment_id: extended_pipeline.deployment_id,
            deployment_initial: extended_pipeline.deployment_initial,
            deployment_status: CombinedStatus::new(
                extended_pipeline.deployment_resources_status,
                extended_pipeline.deployment_runtime_status,
            ),
            deployment_status_since: combine_since(
                extended_pipeline.deployment_resources_status_since,
                extended_pipeline.deployment_runtime_status_since,
            ),
            deployment_desired_status: CombinedDesiredStatus::new(
                extended_pipeline.deployment_resources_desired_status,
                extended_pipeline.deployment_initial,
                extended_pipeline.deployment_runtime_desired_status,
            ),
            deployment_desired_status_since: combine_since(
                extended_pipeline.deployment_resources_desired_status_since,
                extended_pipeline.deployment_runtime_desired_status_since,
            ),
            deployment_resources_status: extended_pipeline.deployment_resources_status,
            deployment_resources_status_since: extended_pipeline.deployment_resources_status_since,
            deployment_resources_desired_status: extended_pipeline
                .deployment_resources_desired_status,
            deployment_resources_desired_status_since: extended_pipeline
                .deployment_resources_desired_status_since,
            deployment_runtime_status: extended_pipeline.deployment_runtime_status,
            deployment_runtime_status_details: extended_pipeline.deployment_runtime_status_details,
            deployment_runtime_status_since: extended_pipeline.deployment_runtime_status_since,
            deployment_runtime_desired_status: extended_pipeline.deployment_runtime_desired_status,
            deployment_runtime_desired_status_since: extended_pipeline
                .deployment_runtime_desired_status_since,
            bootstrap_policy: extended_pipeline.bootstrap_policy,
            connectors: None,
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
            program_config: Some(extended_pipeline.program_config),
            program_version: extended_pipeline.program_version,
            program_status: extended_pipeline.program_status,
            program_status_since: extended_pipeline.program_status_since,
            program_error: None,
            program_info: None,
            deployment_error: extended_pipeline.deployment_error,
            refresh_version: extended_pipeline.refresh_version,
            storage_status: extended_pipeline.storage_status,
            deployment_id: extended_pipeline.deployment_id,
            deployment_initial: extended_pipeline.deployment_initial,
            deployment_status: CombinedStatus::new(
                extended_pipeline.deployment_resources_status,
                extended_pipeline.deployment_runtime_status,
            ),
            deployment_status_since: combine_since(
                extended_pipeline.deployment_resources_status_since,
                extended_pipeline.deployment_runtime_status_since,
            ),
            deployment_desired_status: CombinedDesiredStatus::new(
                extended_pipeline.deployment_resources_desired_status,
                extended_pipeline.deployment_initial,
                extended_pipeline.deployment_runtime_desired_status,
            ),
            deployment_desired_status_since: combine_since(
                extended_pipeline.deployment_resources_desired_status_since,
                extended_pipeline.deployment_runtime_desired_status_since,
            ),
            deployment_resources_status: extended_pipeline.deployment_resources_status,
            deployment_resources_status_since: extended_pipeline.deployment_resources_status_since,
            deployment_resources_desired_status: extended_pipeline
                .deployment_resources_desired_status,
            deployment_resources_desired_status_since: extended_pipeline
                .deployment_resources_desired_status_since,
            deployment_runtime_status: extended_pipeline.deployment_runtime_status,
            deployment_runtime_status_details: extended_pipeline.deployment_runtime_status_details,
            deployment_runtime_status_since: extended_pipeline.deployment_runtime_status_since,
            deployment_runtime_desired_status: extended_pipeline.deployment_runtime_desired_status,
            deployment_runtime_desired_status_since: extended_pipeline
                .deployment_runtime_desired_status_since,
            bootstrap_policy: extended_pipeline.bootstrap_policy,
            connectors: None,
        }
    }

    pub(crate) fn new_status_with_connectors(
        extended_pipeline: ExtendedPipelineDescrMonitoring,
        connectors: Option<ConnectorStats>,
    ) -> Self {
        let mut result = Self::new_status(extended_pipeline);
        result.connectors = connectors;
        result
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
    /// - `deployment_error`
    /// - `refresh_version`
    /// - `storage_status`
    /// - `deployment_id`
    /// - `deployment_initial`
    /// - `deployment_status`
    /// - `deployment_status_since`
    /// - `deployment_desired_status`
    /// - `deployment_desired_status_since`
    /// - `deployment_resources_status`
    /// - `deployment_resources_status_since`
    /// - `deployment_resources_desired_status`
    /// - `deployment_resources_desired_status_since`
    /// - `deployment_runtime_status`
    /// - `deployment_runtime_status_details`
    /// - `deployment_runtime_status_since`
    /// - `deployment_runtime_desired_status`
    /// - `deployment_runtime_desired_status_since`
    /// - `bootstrap_policy`
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
    /// - `program_config`
    /// - `program_version`
    /// - `program_status`
    /// - `program_status_since`
    /// - `deployment_error`
    /// - `refresh_version`
    /// - `storage_status`
    /// - `deployment_id`
    /// - `deployment_initial`
    /// - `deployment_status`
    /// - `deployment_status_since`
    /// - `deployment_desired_status`
    /// - `deployment_desired_status_since`
    /// - `deployment_resources_status`
    /// - `deployment_resources_status_since`
    /// - `deployment_resources_desired_status`
    /// - `deployment_resources_desired_status_since`
    /// - `deployment_runtime_status`
    /// - `deployment_runtime_status_details`
    /// - `deployment_runtime_status_since`
    /// - `deployment_runtime_desired_status`
    /// - `deployment_runtime_desired_status_since`
    /// - `bootstrap_policy`
    Status,
    /// Select the fields included in `Status` plus aggregated connector error statistics.
    ///
    /// In addition to all fields from `Status`, this selector includes:
    /// - `connectors`: Aggregated error statistics across all input and output connectors
    ///   - `num_errors`: Sum of `num_transport_errors`, `num_parse_errors`, and `num_encode_errors`
    ///
    /// If a pipeline is unavailable, the `connectors` field will be null.
    StatusWithConnectors,
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

/// Default for the `initial` query parameter when POST a pipeline start.
fn default_pipeline_start_desired() -> String {
    "running".to_string()
}

/// Query parameters to POST a pipeline start.
#[derive(Debug, Deserialize, IntoParams, ToSchema)]
pub struct PostStartPipelineParameters {
    /// The `initial` parameter determines whether to after provisioning the pipeline make it
    /// become `standby`, `paused` or `running` (only valid values).
    #[serde(default = "default_pipeline_start_desired")]
    initial: String,
    #[serde(default)]
    bootstrap_policy: BootstrapPolicy,
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

/// List Pipelines
///
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
    tag = "Pipeline CRUD"
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
        PipelineFieldSelector::StatusWithConnectors => {
            let pipelines = state
                .db
                .lock()
                .await
                .list_pipelines_for_monitoring(*tenant_id)
                .await?;

            // Fetch connector stats for all pipelines in parallel
            let stats_futures: Vec<_> = pipelines
                .iter()
                .map(|pipeline| {
                    let state = state.clone();
                    let tenant_id = *tenant_id;
                    let pipeline_name = pipeline.name.clone();
                    async move {
                        fetch_connector_error_stats(
                            &state,
                            tenant_id,
                            &pipeline_name,
                            pipeline.deployment_runtime_status,
                        )
                        .await
                    }
                })
                .collect();

            let connector_stats = join_all(stats_futures).await;

            pipelines
                .into_iter()
                .zip(connector_stats.into_iter())
                .map(|(pipeline, stats)| {
                    PipelineSelectedInfoInternal::new_status_with_connectors(pipeline, stats)
                })
                .collect()
        }
    };
    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(returned_pipelines))
}

/// Aggregate Connector Stats
///
/// Fetch and aggregate connector error statistics for a pipeline.
///
/// Returns `None` if the pipeline is unavailable, if there's an error fetching stats,
/// or if the endpoint returns 404 (not implemented on older pipeline versions).
///
/// This endpoint is only used by the web-console so it is not published on openapi
/// and subject to breakage.
async fn fetch_connector_error_stats(
    state: &WebData<ServerState>,
    tenant_id: TenantId,
    pipeline_name: &str,
    deployment_runtime_status: Option<RuntimeStatus>,
) -> Option<ConnectorStats> {
    // Only forward the request if the pipeline is in a valid runtime status
    match deployment_runtime_status {
        Some(RuntimeStatus::Bootstrapping)
        | Some(RuntimeStatus::Replaying)
        | Some(RuntimeStatus::Running)
        | Some(RuntimeStatus::Paused) => {
            // Pipeline is in a valid state, proceed with the request
        }
        _ => {
            // Pipeline is not in a valid state to fetch connector stats
            return None;
        }
    }

    // Use the existing method to forward the request to the pipeline
    let client = awc::Client::default();
    let response = state
        .runner
        .forward_http_request_to_pipeline_by_name(
            &client,
            tenant_id,
            pipeline_name,
            actix_web::http::Method::GET,
            "stats/errors",
            "",
            Some(std::time::Duration::from_millis(500)),
        )
        .await
        .ok()?;

    // Check status code - quietly ignore 404 (endpoint not available on older pipelines)
    if response.status() == actix_web::http::StatusCode::NOT_FOUND {
        debug!(
            "Pipeline '{}' does not support /stats/errors endpoint (404), skipping error stats",
            pipeline_name
        );
        return None;
    }

    // Parse the response body
    let body = response.into_body();
    let bytes = actix_web::body::to_bytes(body).await.ok()?;
    let stats_response: PipelineStatsErrorsResponse = match serde_json::from_slice(&bytes) {
        Ok(response) => response,
        Err(e) => {
            error!(
                "Failed to deserialize pipeline stats response for '{}': {}",
                pipeline_name, e
            );
            return None;
        }
    };

    // Extract and aggregate error counts
    let mut total_errors = 0u64;

    // Aggregate input connector errors
    for endpoint in stats_response.inputs {
        total_errors = total_errors
            .saturating_add(endpoint.metrics.num_transport_errors)
            .saturating_add(endpoint.metrics.num_parse_errors);
    }

    // Aggregate output connector errors
    for endpoint in stats_response.outputs {
        total_errors = total_errors
            .saturating_add(endpoint.metrics.num_encode_errors)
            .saturating_add(endpoint.metrics.num_transport_errors);
    }

    Some(ConnectorStats {
        num_errors: total_errors,
    })
}

/// Get Pipeline
///
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
    tag = "Pipeline CRUD"
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
        PipelineFieldSelector::StatusWithConnectors => {
            let pipeline = state
                .db
                .lock()
                .await
                .get_pipeline_for_monitoring(*tenant_id, &pipeline_name)
                .await?;
            let connector_stats = fetch_connector_error_stats(
                &state,
                *tenant_id,
                &pipeline_name,
                pipeline.deployment_runtime_status,
            )
            .await;
            PipelineSelectedInfoInternal::new_status_with_connectors(pipeline, connector_stats)
        }
    };
    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(&returned_pipeline))
}

/// Create Pipeline
///
/// Create a new pipeline with the provided configuration.
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
    tag = "Pipeline CRUD"
)]
#[post("/pipelines")]
pub(crate) async fn post_pipeline(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    body: web::Json<PostPutPipelineInternal>,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_descr: PipelineDescr = body.into_inner().into();
    let name = pipeline_descr.name.clone();
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
        pipeline = %returned_pipeline.name,
        pipeline_id = %returned_pipeline.id,
        tenant_id = %tenant_id.0,
        "Created pipeline {name:?} ({}) (tenant: {})",
        returned_pipeline.id,
        *tenant_id
    );
    Ok(HttpResponse::Created()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(returned_pipeline))
}

/// Upsert Pipeline
///
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
                ("Cannot update non-stopped pipeline" = (value = json!(examples::error_update_restricted_to_stopped())))
            )
        ),
        (status = INTERNAL_SERVER_ERROR, body = ErrorResponse),
    ),
    tag = "Pipeline CRUD"
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
            true,
            pipeline_descr,
        )
        .await?;
    let returned_pipeline = PipelineInfoInternal::new(pipeline);

    if is_new {
        info!(
            pipeline = %returned_pipeline.name,
            pipeline_id = %returned_pipeline.id,
            tenant_id = %tenant_id.0,
            "Created pipeline {pipeline_name:?} ({}) (tenant: {})",
            returned_pipeline.id,
            *tenant_id
        );
        Ok(HttpResponse::Created()
            .insert_header(CacheControl(vec![CacheDirective::NoCache]))
            .json(returned_pipeline))
    } else {
        info!(
            pipeline = %returned_pipeline.name,
            pipeline_id = %returned_pipeline.id,
            tenant_id = %tenant_id.0,
            "Fully updated pipeline {pipeline_name:?} ({}) to version {} (tenant: {})",
            returned_pipeline.id,
            returned_pipeline.version,
            *tenant_id
        );
        Ok(HttpResponse::Ok()
            .insert_header(CacheControl(vec![CacheDirective::NoCache]))
            .json(returned_pipeline))
    }
}

/// Patch Pipeline
///
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
                ("Cannot update non-stopped pipeline" = (value = json!(examples::error_update_restricted_to_stopped())))
            )
        ),
        (status = INTERNAL_SERVER_ERROR, body = ErrorResponse),
    ),
    tag = "Pipeline CRUD"
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
            false,
            &body.runtime_config,
            &body.program_code,
            &body.udf_rust,
            &body.udf_toml,
            &body.program_config,
        )
        .await?;
    let returned_pipeline = PipelineInfoInternal::new(pipeline);

    info!(
        "Partially updated pipeline {pipeline_name:?} ({}) to version {} (tenant: {})",
        returned_pipeline.id, returned_pipeline.version, *tenant_id
    );
    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(returned_pipeline))
}

/// Recompile Pipeline
///
/// Recompile a pipeline with the Feldera runtime version included in the
/// currently installed Feldera platform.
///
/// Use this endpoint after upgrading Feldera to rebuild pipelines that were
/// compiled with older platform versions. In most cases, recompilation is not
/// required; pipelines compiled with older versions will continue to run on the
/// upgraded platform.
///
/// Situations where recompilation may be necessary:
/// - To benefit from the latest bug fixes and performance optimizations.
/// - When backward-incompatible changes are introduced in Feldera. In this case,
///   attempting to start a pipeline compiled with an unsupported version will
///   result in an error.
///
/// If the pipeline is already compiled with the current platform version,
/// this operation is a no-op.
///
/// Note that recompiling the pipeline with a new platform version may change its
/// query plan. If the modified pipeline is started from an existing checkpoint,
/// it may require bootstrapping parts of its state from scratch.  See Feldera
/// documentation for details on the bootstrapping process.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(
        ("pipeline_name" = String, Path, description = "Unique pipeline name"),
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
        (status = BAD_REQUEST
            , body = ErrorResponse
            , examples(
                ("Name does not match pattern" = (value = json!(examples::error_name_does_not_match_pattern()))),
                ("Cannot update non-stopped pipeline" = (value = json!(examples::error_update_restricted_to_stopped())))
            )
        ),
        (status = INTERNAL_SERVER_ERROR, body = ErrorResponse),
    ),
    tag = "Pipeline Lifecycle"
)]
#[post("/pipelines/{pipeline_name}/update_runtime")]
pub(crate) async fn post_update_runtime(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    path: web::Path<String>,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_name = path.into_inner();
    let pipeline = state
        .db
        .lock()
        .await
        .update_pipeline(
            *tenant_id,
            &pipeline_name,
            &None,
            &None,
            &state.common_config.platform_version,
            true, // bump platform version.
            &None,
            &None,
            &None,
            &None,
            &None,
        )
        .await?;
    let returned_pipeline = PipelineInfoInternal::new(pipeline);

    info!(
        "Updated pipeline {pipeline_name:?} ({}) platform_version to {} (tenant: {})",
        returned_pipeline.id, returned_pipeline.platform_version, *tenant_id
    );
    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(returned_pipeline))
}

/// Delete Pipeline
///
/// Delete an existing pipeline by name.
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
            , description = "Pipeline must be fully stopped and cleared to be deleted"
            , body = ErrorResponse
            , example = json!(examples::error_delete_restricted_to_fully_stopped())),
        (status = INTERNAL_SERVER_ERROR, body = ErrorResponse),
    ),
    tag = "Pipeline CRUD"
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

    info!(
        pipeline = %pipeline_name,
        pipeline_id = %pipeline_id,
        tenant_id = %tenant_id.0,
        "Deleted pipeline {pipeline_name:?} ({}) (tenant: {})",
        pipeline_id,
        *tenant_id
    );
    Ok(HttpResponse::Ok().finish())
}

/// Start Pipeline
///
/// Start the pipeline asynchronously by updating the desired status.
///
/// The endpoint returns immediately after setting the desired status.
/// The procedure to get to the desired status is performed asynchronously.
/// Progress should be monitored by polling the pipeline `GET` endpoints.
///
/// Note the following:
/// - A stopped pipeline can be started through calling `/start?initial=running`,
///   `/start?initial=paused`, or `/start?initial=standby`.
/// - If the pipeline is already (being) started (provisioned), it will still return success
/// - It is not possible to call `/start` when the pipeline has already had `/stop` called and is
///   in the process of suspending or stopping.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(
        ("pipeline_name" = String, Path, description = "Unique pipeline name"),
        PostStartPipelineParameters
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
    tag = "Pipeline Lifecycle"
)]
#[post("/pipelines/{pipeline_name}/start")]
pub(crate) async fn post_pipeline_start(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    path: web::Path<String>,
    query: web::Query<PostStartPipelineParameters>,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_name = path.into_inner();
    let PostStartPipelineParameters {
        initial,
        bootstrap_policy,
    } = query.into_inner();

    let pipeline_id = match initial.as_str() {
        "standby" => {
            state
                .db
                .lock()
                .await
                .set_deployment_resources_desired_status_provisioned(
                    *tenant_id,
                    &pipeline_name,
                    RuntimeDesiredStatus::Standby,
                    bootstrap_policy,
                )
                .await?
        }
        "paused" => {
            state
                .db
                .lock()
                .await
                .set_deployment_resources_desired_status_provisioned(
                    *tenant_id,
                    &pipeline_name,
                    RuntimeDesiredStatus::Paused,
                    bootstrap_policy,
                )
                .await?
        }
        "running" => {
            state
                .db
                .lock()
                .await
                .set_deployment_resources_desired_status_provisioned(
                    *tenant_id,
                    &pipeline_name,
                    RuntimeDesiredStatus::Running,
                    bootstrap_policy,
                )
                .await?
        }
        _ => Err(ManagerError::from(ApiError::UnsupportedPipelineAction {
            action: format!("/start?initial={initial}"),
            reason: format!("unknown initial runtime desired status: {initial}"),
        }))?,
    };

    info!(
        "Accepted action: going to start pipeline {pipeline_name:?} ({pipeline_id}) as {} (tenant: {})",
        initial, *tenant_id
    );
    Ok(HttpResponse::Accepted().json(json!("Pipeline is starting")))
}

/// Stop Pipeline
///
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
        PostStopPipelineParameters
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
    tag = "Pipeline Lifecycle"
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
    if query.force {
        let pipeline_id = state
            .db
            .lock()
            .await
            .set_deployment_resources_desired_status_stopped(*tenant_id, &pipeline_name)
            .await?;
        info!(
            "Accepted action: going to forcefully stop pipeline {pipeline_name:?} ({pipeline_id}) (tenant: {})",
            *tenant_id
        );
        Ok(HttpResponse::Accepted().json(json!("Pipeline is forcefully stopping")))
    } else {
        #[cfg(not(feature = "feldera-enterprise"))]
        {
            Err(ManagerError::from(CommonError::EnterpriseFeature {
                feature: "stop?force=false".to_string(),
            }))?
        }
        #[cfg(feature = "feldera-enterprise")]
        {
            let (was_set, pipeline_id) = state
                .db
                .lock()
                .await
                .set_deployment_resources_desired_status_stopped_if_not_provisioned(
                    *tenant_id,
                    &pipeline_name,
                )
                .await?;
            if was_set {
                info!(
                    "Accepted action: going to forcefully stop pipeline {pipeline_name:?} ({pipeline_id}) (tenant: {}) because it is not provisioned",
                    *tenant_id
                );
                Ok(HttpResponse::Accepted().json(json!("Pipeline is forcefully stopping")))
            } else {
                let response = state
                    .runner
                    .forward_http_request_to_pipeline_by_name(
                        _client.as_ref(),
                        *tenant_id,
                        &pipeline_name,
                        Method::POST,
                        "suspend",
                        "",
                        Some(Duration::from_secs(120)),
                    )
                    .await;
                state
                    .db
                    .lock()
                    .await
                    .increment_notify_counter(*tenant_id, &pipeline_name)
                    .await?;
                if response
                    .as_ref()
                    .is_ok_and(|v| v.status() == actix_web::http::StatusCode::ACCEPTED)
                {
                    info!(
                        "Accepted action: going to non-forcefully stop pipeline {pipeline_name:?} ({pipeline_id}) (tenant: {})",
                        *tenant_id
                    );
                }
                response
            }
        }
    }
}

/// Clear Storage
///
/// Clears the pipeline storage asynchronously.
///
/// IMPORTANT: Clearing means disassociating the storage from the pipeline.
///            Depending on the storage type this can include its deletion.
///
/// It sets the storage state to `Clearing`, after which the clearing process is
/// performed asynchronously. Progress should be monitored by polling the pipeline
/// using the `GET` endpoints. An `/clear` cannot be cancelled.
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
    tag = "Pipeline Lifecycle"
)]
#[post("/pipelines/{pipeline_name}/clear")]
pub(crate) async fn post_pipeline_clear(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    path: web::Path<String>,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_name = path.into_inner();
    let pipeline_id = state
        .db
        .lock()
        .await
        .transit_storage_status_to_clearing_if_not_cleared(*tenant_id, &pipeline_name)
        .await?;

    info!(
        "Accepted storage action: going to clear storage of pipeline {pipeline_name:?} ({pipeline_id}) (tenant: {})",
        *tenant_id
    );
    Ok(HttpResponse::Accepted().json(json!("Pipeline storage is being cleared")))
}

/// Stream Pipeline Logs
///
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
                ("Runner response timeout" = (value = json!(examples::error_runner_interaction_timeout())))
            )
        ),
        (status = INTERNAL_SERVER_ERROR, body = ErrorResponse),
    ),
    tag = "Metrics & Debugging"
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

#[derive(Debug, Serialize, Deserialize, ToSchema, IntoParams)]
pub struct PostPipelineTesting {
    #[serde(default)]
    pub set_platform_version: Option<String>,
}

/// Test Endpoint
///
/// This endpoint is used as part of the test harness. Only available if the `testing`
/// unstable feature is enabled. Do not use in production.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(
        ("pipeline_name" = String, Path, description = "Unique pipeline name"),
        PostPipelineTesting,
    ),
    responses(
        (status = OK
            , description = "Request successfully processed"),
        (status = NOT_FOUND
            , description = "Pipeline with that name does not exist"
            , body = ErrorResponse
            , example = json!(examples::error_unknown_pipeline_name())),
        (status = METHOD_NOT_ALLOWED
            , description = "Endpoint is disabled. Set FELDERA_UNSTABLE_FEATURES=\"testing\" to enable."
            , body = ErrorResponse
        )
    ),
    tag = "Metrics & Debugging"
)]
#[post("/pipelines/{pipeline_name}/testing")]
pub(crate) async fn post_pipeline_testing(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    path: web::Path<String>,
    query: web::Query<PostPipelineTesting>,
) -> Result<HttpResponse, ManagerError> {
    if !has_unstable_feature("testing") {
        Err(ApiError::UnsupportedPipelineAction {
            action: "/testing".to_string(),
            reason: "/testing endpoint is disabled. Set FELDERA_UNSTABLE_FEATURES=\"testing\" to enable.".to_string()
        })?;
    };

    let pipeline_name = path.into_inner();

    if let Some(platform_version) = &query.set_platform_version {
        state
            .db
            .lock()
            .await
            .testing_force_update_platform_version(*tenant_id, &pipeline_name, platform_version)
            .await?;
    }

    Ok(HttpResponse::Ok().finish())
}
