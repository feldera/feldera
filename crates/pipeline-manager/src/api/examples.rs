// Example errors for use in OpenAPI docs.
use crate::api::endpoints::pipeline_management::{
    PartialProgramInfo, PatchPipeline, PipelineInfo, PipelineInfoInternal, PipelineSelectedInfo,
    PipelineSelectedInfoInternal, PostPutPipeline,
};
use crate::api::error::ApiError;
use crate::db::error::DBError;
use crate::db::types::pipeline::{
    ExtendedPipelineDescr, PipelineDesiredStatus, PipelineId, PipelineStatus,
};
use crate::db::types::program::{CompilationProfile, ProgramConfig, ProgramStatus};
use crate::db::types::utils::{
    validate_program_config, validate_program_info, validate_runtime_config,
};
use crate::db::types::version::Version;
use crate::error::ManagerError;
use crate::runner::error::RunnerError;
use feldera_types::config::ResourceConfig;
use feldera_types::{config::RuntimeConfig, error::ErrorResponse};
use uuid::uuid;

pub(crate) fn error_duplicate_name() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&DBError::DuplicateName)
}

pub(crate) fn error_invalid_name() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&DBError::NameDoesNotMatchPattern {
        name: "invalid-name.db".to_string(),
    })
}

pub(crate) fn error_invalid_uuid_param() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&ManagerError::from(ApiError::InvalidUuidParam {
        value: "not_a_uuid".to_string(),
        error: "invalid character: expected an optional prefix of `urn:uuid:` followed by [0-9a-fA-F-], found `n` at 1".to_string()
    }))
}

pub(crate) fn error_unknown_api_key() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&DBError::UnknownApiKey {
        name: "unknown_api_key".to_string(),
    })
}

pub(crate) fn error_stream_terminated() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&RunnerError::PipelineEndpointSendError {
        pipeline_id: PipelineId(uuid!("67e55044-10b1-426f-9247-bb680e5fe0c8")),
        pipeline_name: Some("example".to_string()),
        url: "https://localhost:1234/query".to_string(),
        error: "Failed to connect to host: Internal error: connector has been disconnected"
            .to_string(),
    })
}

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
        })
        .unwrap(),
        program_version: Version(2),
        program_info: None,
        program_status: ProgramStatus::Pending,
        program_status_since: Default::default(),
        program_binary_source_checksum: None,
        program_binary_integrity_checksum: None,
        program_binary_url: None,
        deployment_config: None,
        deployment_location: None,
        deployment_status: PipelineStatus::Shutdown,
        deployment_status_since: Default::default(),
        deployment_desired_status: PipelineDesiredStatus::Shutdown,
        deployment_error: None,
        refresh_version: Version(4),
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
            storage: true,
            fault_tolerance: None,
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
            min_storage_bytes: None,
            clock_resolution_usecs: Some(100_000),
        })
        .unwrap(),
        program_code: "CREATE TABLE table2 ( col2 VARCHAR );".to_string(),
        udf_rust: "".to_string(),
        udf_toml: "".to_string(),
        program_config: serde_json::to_value(ProgramConfig {
            profile: Some(CompilationProfile::Unoptimized),
            cache: true,
        })
        .unwrap(),
        program_version: Version(1),
        program_info: None,
        program_status: ProgramStatus::Pending,
        program_status_since: Default::default(),
        program_binary_source_checksum: None,
        program_binary_integrity_checksum: None,
        program_binary_url: None,
        deployment_config: None,
        deployment_location: None,
        deployment_status: PipelineStatus::Shutdown,
        deployment_status_since: Default::default(),
        deployment_desired_status: PipelineDesiredStatus::Shutdown,
        deployment_error: None,
        refresh_version: Version(1),
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
    }
}

pub(crate) fn pipeline_1_info() -> PipelineInfo {
    pipeline_info_internal_to_external(PipelineInfoInternal::new(extended_pipeline_1()))
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
    }
}

pub(crate) fn pipeline_1_selected_info() -> PipelineSelectedInfo {
    pipeline_selected_info_internal_to_external(PipelineSelectedInfoInternal::new_all(
        extended_pipeline_1(),
    ))
}

pub(crate) fn pipeline_2_selected_info() -> PipelineSelectedInfo {
    pipeline_selected_info_internal_to_external(PipelineSelectedInfoInternal::new_all(
        extended_pipeline_2(),
    ))
}

pub(crate) fn list_pipeline_selected_info() -> Vec<PipelineSelectedInfo> {
    vec![pipeline_1_selected_info(), pipeline_2_selected_info()]
}

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
        }),
    }
}

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

pub(crate) fn error_unknown_pipeline() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&DBError::UnknownPipeline {
        pipeline_id: PipelineId(uuid!("2e79afe1-ff4d-44d3-af5f-9397de7746c0")),
    })
}

pub(crate) fn error_cannot_update_non_shutdown_pipeline() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&DBError::CannotUpdateNonShutdownPipeline)
}

pub(crate) fn error_cannot_delete_non_shutdown_pipeline() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&DBError::CannotDeleteNonShutdownPipeline)
}

pub(crate) fn error_pipeline_not_running_or_paused() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&RunnerError::PipelineNotRunningOrPaused {
        pipeline_id: PipelineId(uuid!("2e79afe1-ff4d-44d3-af5f-9397de7746c0")),
        pipeline_name: "example".to_string(),
    })
}

pub(crate) fn error_invalid_pipeline_action() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&ManagerError::from(ApiError::InvalidPipelineAction {
        action: "dance".to_string(),
    }))
}

pub(crate) fn error_illegal_pipeline_action() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&DBError::IllegalPipelineStateTransition {
            hint: "Cannot restart the pipeline while it is shutting down. Wait for the shutdown to complete before starting a new instance of the pipeline.".to_string(),
            status: PipelineStatus::ShuttingDown,
            desired_status: PipelineDesiredStatus::Shutdown,
            requested_desired_status: PipelineDesiredStatus::Running,
    })
}

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
