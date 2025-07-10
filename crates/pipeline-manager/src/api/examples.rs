//! Example data and error types for use in OpenAPI docs.
use std::collections::BTreeMap;

use crate::api::endpoints::pipeline_management::{
    PartialProgramInfo, PatchPipeline, PipelineInfo, PipelineInfoInternal, PipelineSelectedInfo,
    PipelineSelectedInfoInternal, PostPutPipeline,
};
use crate::api::error::ApiError;
use crate::db::error::DBError;
use crate::db::types::pipeline::{
    ExtendedPipelineDescr, PipelineDesiredStatus, PipelineId, PipelineStatus,
};
use crate::db::types::program::{CompilationProfile, ProgramConfig, ProgramError, ProgramStatus};
use crate::db::types::storage::StorageStatus;
use crate::db::types::utils::{
    validate_program_config, validate_program_info, validate_runtime_config,
};
use crate::db::types::version::Version;
use crate::runner::error::RunnerError;
use crate::runner::interaction::{
    format_disconnected_error_message, format_timeout_error_message, RunnerInteraction,
};
use feldera_types::config::{FtConfig, ResourceConfig, StorageOptions};
use feldera_types::{config::RuntimeConfig, error::ErrorResponse};
use uuid::uuid;

////////////////////////////////////////
// EXAMPLE REQUEST AND RESPONSE BODIES

/// First example [`ExtendedPipelineDescr`] the database could return.
fn extended_pipeline_1() -> ExtendedPipelineDescr {
    ExtendedPipelineDescr {
        id: PipelineId(uuid!("67e55044-10b1-426f-9247-bb680e5fe0c8")),
        name: "example1".to_string(),
        description: "Description of the pipeline example1".to_string(),
        created_at: Default::default(),
        version: Version(4),
        platform_version: "v0".to_string(),
        runtime_config: serde_json::to_value(RuntimeConfig {
            workers: 16,
            tracing_endpoint_jaeger: "".to_string(),
            ..RuntimeConfig::default()
        })
        .unwrap(),
        program_code: "CREATE TABLE table1 ( col1 INT );".to_string(),
        udf_rust: "".to_string(),
        udf_toml: "".to_string(),
        program_config: serde_json::to_value(ProgramConfig {
            profile: Some(CompilationProfile::Optimized),
            cache: true,
            runtime_version: None,
        })
        .unwrap(),
        program_version: Version(2),
        program_info: None,
        program_status: ProgramStatus::Pending,
        program_status_since: Default::default(),
        program_error: ProgramError {
            sql_compilation: None,
            rust_compilation: None,
            system_error: None,
        },
        program_binary_source_checksum: None,
        program_binary_integrity_checksum: None,
        program_binary_url: None,
        deployment_config: None,
        deployment_location: None,
        deployment_status: PipelineStatus::Stopped,
        deployment_status_since: Default::default(),
        deployment_desired_status: PipelineDesiredStatus::Stopped,
        deployment_error: None,
        refresh_version: Version(4),
        suspend_info: None,
        storage_status: StorageStatus::Cleared,
    }
}

/// Second example [`ExtendedPipelineDescr`] the database could return.
fn extended_pipeline_2() -> ExtendedPipelineDescr {
    ExtendedPipelineDescr {
        id: PipelineId(uuid!("67e55044-10b1-426f-9247-bb680e5fe0c9")),
        name: "example2".to_string(),
        description: "Description of the pipeline example2".to_string(),
        created_at: Default::default(),
        version: Version(1),
        platform_version: "v0".to_string(),
        runtime_config: serde_json::to_value(RuntimeConfig {
            workers: 10,
            storage: Some(StorageOptions::default()),
            fault_tolerance: FtConfig::default(),
            cpu_profiler: false,
            tracing: false,
            tracing_endpoint_jaeger: "".to_string(),
            min_batch_size_records: 100000,
            max_buffering_delay_usecs: 0,
            resources: ResourceConfig {
                cpu_cores_min: None,
                cpu_cores_max: None,
                memory_mb_min: Some(1000),
                memory_mb_max: None,
                storage_mb_max: Some(10000),
                storage_class: None,
            },
            clock_resolution_usecs: Some(100_000),
            pin_cpus: Vec::new(),
            provisioning_timeout_secs: Some(1200),
            max_parallel_connector_init: Some(10),
            init_containers: None,
            checkpoint_during_suspend: false,
            io_workers: None,
            http_workers: None,
            dev_tweaks: BTreeMap::new(),
            logging: None,
        })
        .unwrap(),
        program_code: "CREATE TABLE table2 ( col2 VARCHAR );".to_string(),
        udf_rust: "".to_string(),
        udf_toml: "".to_string(),
        program_config: serde_json::to_value(ProgramConfig {
            profile: Some(CompilationProfile::Unoptimized),
            cache: true,
            runtime_version: None,
        })
        .unwrap(),
        program_version: Version(1),
        program_info: None,
        program_status: ProgramStatus::Pending,
        program_status_since: Default::default(),
        program_error: ProgramError {
            sql_compilation: None,
            rust_compilation: None,
            system_error: None,
        },
        program_binary_source_checksum: None,
        program_binary_integrity_checksum: None,
        program_binary_url: None,
        deployment_config: None,
        deployment_location: None,
        deployment_status: PipelineStatus::Stopped,
        deployment_status_since: Default::default(),
        deployment_desired_status: PipelineDesiredStatus::Stopped,
        deployment_error: None,
        refresh_version: Version(1),
        suspend_info: None,
        storage_status: StorageStatus::Cleared,
    }
}

/// Converts the actual serialized type [`PipelineInfoInternal`] to the type the endpoint
/// OpenAPI specification states it will return ([`PipelineInfo`]). The conversion for this
/// example should always succeed as it uses field values that were directly serialized prior.
fn pipeline_info_internal_to_external(pipeline: PipelineInfoInternal) -> PipelineInfo {
    PipelineInfo {
        id: pipeline.id,
        name: pipeline.name,
        description: pipeline.description,
        created_at: pipeline.created_at,
        version: pipeline.version,
        platform_version: pipeline.platform_version,
        runtime_config: validate_runtime_config(&pipeline.runtime_config, true)
            .expect("example must have a valid runtime_config"),
        program_code: pipeline.program_code,
        udf_rust: pipeline.udf_rust,
        udf_toml: pipeline.udf_toml,
        program_config: validate_program_config(&pipeline.program_config, true)
            .expect("example must have a valid program_config"),
        program_version: pipeline.program_version,
        program_status: pipeline.program_status,
        program_status_since: pipeline.program_status_since,
        program_error: pipeline.program_error,
        program_info: pipeline.program_info.map(|v| {
            let program_info = validate_program_info(&v)
                .expect("example must have a valid program_info if specified");
            PartialProgramInfo {
                schema: program_info.schema,
                udf_stubs: program_info.udf_stubs,
                input_connectors: program_info.input_connectors,
                output_connectors: program_info.output_connectors,
            }
        }),
        deployment_status: pipeline.deployment_status,
        deployment_status_since: pipeline.deployment_status_since,
        deployment_desired_status: pipeline.deployment_desired_status,
        deployment_error: pipeline.deployment_error,
        refresh_version: pipeline.refresh_version,
        storage_status: pipeline.storage_status,
    }
}

/// Converts the actual serialized type [`PipelineSelectedInfoInternal`] to the type the endpoint
/// OpenAPI specification states it will return ([`PipelineSelectedInfo`]). The conversion for this
/// example should always succeed as it uses field values that were directly serialized prior.
fn pipeline_selected_info_internal_to_external(
    pipeline: PipelineSelectedInfoInternal,
) -> PipelineSelectedInfo {
    PipelineSelectedInfo {
        id: pipeline.id,
        name: pipeline.name,
        description: pipeline.description,
        created_at: pipeline.created_at,
        version: pipeline.version,
        platform_version: pipeline.platform_version,
        runtime_config: pipeline.runtime_config.map(|v| {
            validate_runtime_config(&v, true).expect("example must have a valid runtime_config")
        }),
        program_code: pipeline.program_code,
        udf_rust: pipeline.udf_rust,
        udf_toml: pipeline.udf_toml,
        program_config: pipeline.program_config.map(|v| {
            validate_program_config(&v, true).expect("example must have a valid program_config")
        }),
        program_version: pipeline.program_version,
        program_status: pipeline.program_status,
        program_status_since: pipeline.program_status_since,
        program_error: pipeline.program_error,
        program_info: pipeline.program_info.map(|v| {
            v.map(|v| {
                let program_info = validate_program_info(&v)
                    .expect("example must have a valid program_info if specified");
                PartialProgramInfo {
                    schema: program_info.schema,
                    udf_stubs: program_info.udf_stubs,
                    input_connectors: program_info.input_connectors,
                    output_connectors: program_info.output_connectors,
                }
            })
        }),
        deployment_status: pipeline.deployment_status,
        deployment_status_since: pipeline.deployment_status_since,
        deployment_desired_status: pipeline.deployment_desired_status,
        deployment_error: pipeline.deployment_error,
        refresh_version: pipeline.refresh_version,
        storage_status: pipeline.storage_status,
    }
}

/// Example response body of pipeline POST/PUT/PATCH.
pub(crate) fn pipeline_1_info() -> PipelineInfo {
    pipeline_info_internal_to_external(PipelineInfoInternal::new(extended_pipeline_1()))
}

/// First example response body of pipeline GET.
pub(crate) fn pipeline_1_selected_info() -> PipelineSelectedInfo {
    pipeline_selected_info_internal_to_external(PipelineSelectedInfoInternal::new_all(
        extended_pipeline_1(),
    ))
}

/// Second example response body of pipeline GET.
fn pipeline_2_selected_info() -> PipelineSelectedInfo {
    pipeline_selected_info_internal_to_external(PipelineSelectedInfoInternal::new_all(
        extended_pipeline_2(),
    ))
}

/// Example response body of list of pipelines GET.
pub(crate) fn list_pipeline_selected_info() -> Vec<PipelineSelectedInfo> {
    vec![pipeline_1_selected_info(), pipeline_2_selected_info()]
}

/// Example request body of pipeline POST/PUT.
pub(crate) fn pipeline_post_put() -> PostPutPipeline {
    PostPutPipeline {
        name: "example1".to_string(),
        description: Some("Description of the pipeline example1".to_string()),
        runtime_config: Some(RuntimeConfig {
            workers: 16,
            tracing_endpoint_jaeger: "".to_string(),
            ..RuntimeConfig::default()
        }),
        program_code: "CREATE TABLE table1 ( col1 INT );".to_string(),
        udf_rust: None,
        udf_toml: None,
        program_config: Some(ProgramConfig {
            profile: Some(CompilationProfile::Optimized),
            cache: true,
            runtime_version: None,
        }),
    }
}

/// Example request body of pipeline PATCH.
pub(crate) fn patch_pipeline() -> PatchPipeline {
    PatchPipeline {
        name: None,
        description: Some("This is a new description".to_string()),
        runtime_config: None,
        program_code: Some("CREATE TABLE table3 ( col3 INT );".to_string()),
        udf_rust: None,
        udf_toml: None,
        program_config: None,
    }
}

////////////////////////////
// GENERAL ERROR RESPONSES

pub(crate) fn error_duplicate_name() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&DBError::DuplicateName)
}

pub(crate) fn error_name_does_not_match_pattern() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&DBError::NameDoesNotMatchPattern {
        name: "name-with-invalid-char-#".to_string(),
    })
}

////////////////////////////
// API KEY ERROR RESPONSES

pub(crate) fn error_unknown_api_key() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&DBError::UnknownApiKey {
        name: "non-existent-api-key".to_string(),
    })
}

////////////////////////////////////////
// PIPELINE MANAGEMENT ERROR RESPONSES

pub(crate) fn error_unknown_pipeline_name() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&DBError::UnknownPipelineName {
        pipeline_name: "non-existent-pipeline".to_string(),
    })
}

pub(crate) fn error_update_restricted_to_stopped() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&DBError::UpdateRestrictedToStopped)
}

pub(crate) fn error_delete_restricted_to_fully_stopped() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&DBError::DeleteRestrictedToFullyStopped)
}

pub(crate) fn error_unsupported_pipeline_action() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&ApiError::UnsupportedPipelineAction {
        action: "suspend".to_string(),
        reason: "this pipeline does not support the suspend action for the following reason(s):\n    - Storage must be configured".to_string()
    })
}

pub(crate) fn error_illegal_pipeline_action() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&DBError::IllegalPipelineAction {
            pipeline_status: PipelineStatus::Stopping,
            current_desired_status: PipelineDesiredStatus::Stopped,
            new_desired_status: PipelineDesiredStatus::Running,
            hint: "Cannot restart the pipeline while it is stopping. Wait for it to stop before starting a new instance of the pipeline.".to_string(),
    })
}

pub(crate) fn error_illegal_pipeline_storage_action() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&DBError::InvalidStorageStatusTransition {
        current_status: StorageStatus::Cleared,
        new_status: StorageStatus::Clearing,
    })
}

/////////////////////////////////////////
// PIPELINE INTERACTION ERROR RESPONSES

pub(crate) fn error_pipeline_interaction_not_deployed() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&RunnerError::PipelineInteractionNotDeployed {
        status: PipelineStatus::Stopped,
        desired_status: PipelineDesiredStatus::Running,
    })
}

pub(crate) fn error_pipeline_interaction_currently_unavailable() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&RunnerError::PipelineInteractionUnreachable {
        error: "deployment status is currently 'unavailable' -- wait for it to become 'running' or 'paused' again".to_string()
    })
}

pub(crate) fn error_pipeline_interaction_disconnected() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&RunnerError::PipelineInteractionUnreachable {
        error: format_disconnected_error_message("".to_string()),
    })
}

pub(crate) fn error_pipeline_interaction_timeout() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&RunnerError::PipelineInteractionUnreachable {
        error: format_timeout_error_message(
            RunnerInteraction::PIPELINE_HTTP_REQUEST_TIMEOUT,
            "Timeout while waiting for response".to_string(),
        ),
    })
}

pub(crate) fn error_runner_interaction_timeout() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&RunnerError::RunnerInteractionUnreachable {
        error: format_timeout_error_message(
            RunnerInteraction::RUNNER_HTTP_REQUEST_TIMEOUT,
            "Timeout while waiting for response".to_string(),
        ),
    })
}

///////////////////////////////////////////////////////
// PIPELINE INTERACTION ERROR RESPONSES FROM PIPELINE

//
// TODO: The below errors can be returned when forwarding data to/from a running pipeline.
//       These all require dependencies to the adapter crate and are used
//       nowhere else. We might have to manually write out these responses.
//
// fn example_unknown_input_table(table: &str) -> ErrorResponse {
//     ErrorResponse::from_error_nolog(&ControllerError::unknown_input_stream(
//         "input_endpoint1",
//         table,
//     ))
// }
//
// fn example_unknown_output_table(table: &str) -> ErrorResponse {
//     ErrorResponse::from_error_nolog(&ControllerError::unknown_output_stream(
//         "output_endpoint1",
//         table,
//     ))
// }
//
// fn example_unknown_input_format() -> ErrorResponse {
//     ErrorResponse::from_error_nolog(&ControllerError::unknown_input_format(
//         "input_endpoint1",
//         "xml",
//     ))
// }
//
// fn example_parse_errors() -> ErrorResponse {
//     let errors = [
//         ParseError::text_envelope_error("failed to parse string as a JSON
// document: EOF while parsing a value at line 1 column 27".to_string(),
// "{\"b\": false, \"i\": 100, \"s\":", None),
//         ParseError::text_event_error("failed to deserialize JSON record
// '{\"b\": false}'", "missing field `i` at line 3 column 12", 3, Some("{\"b\":
// false}"), None),     ];
//     ErrorResponse::from_error_nolog(&PipelineError::parse_errors(errors.
// len(), errors.iter())) }
//
// fn example_unknown_output_format() -> ErrorResponse {
//     ErrorResponse::from_error_nolog(&ControllerError::unknown_output_format(
//         "output_endpoint1",
//         "xml",
//     ))
// }
