// Example errors for use in OpenAPI docs.
use crate::api::pipeline::{ExtendedPipelineDescrOptionalCode, PatchPipeline};
use crate::db::error::DBError;
use crate::db::types::common::Version;
use crate::db::types::pipeline::{
    ExtendedPipelineDescr, PipelineDescr, PipelineId, PipelineStatus,
};
use crate::db::types::program::{CompilationProfile, ProgramConfig, ProgramStatus};
use crate::error::ManagerError;
use crate::runner::RunnerError;
use pipeline_types::config::ResourceConfig;
use pipeline_types::{config::RuntimeConfig, error::ErrorResponse};
use uuid::uuid;

pub(crate) fn error_duplicate_name() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&DBError::DuplicateName)
}

pub(crate) fn error_invalid_uuid_param() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&ManagerError::InvalidUuidParam {
        value: "not_a_uuid".to_string(),
        error: "invalid character: expected an optional prefix of `urn:uuid:` followed by [0-9a-fA-F-], found `n` at 1".to_string()
    })
}

pub(crate) fn error_unknown_api_key() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&DBError::UnknownApiKey {
        name: "unknown_api_key".to_string(),
    })
}

pub(crate) fn pipeline_1() -> PipelineDescr {
    PipelineDescr {
        name: "example1".to_string(),
        description: "Description of the pipeline example1".to_string(),
        runtime_config: RuntimeConfig {
            workers: 16,
            storage: false,
            cpu_profiler: false,
            tracing: false,
            tracing_endpoint_jaeger: "".to_string(),
            min_batch_size_records: 0,
            max_buffering_delay_usecs: 0,
            resources: Default::default(),
            min_storage_bytes: None,
        },
        program_code: "CREATE TABLE table1 ( col1 INT );".to_string(),
        program_config: ProgramConfig {
            profile: Some(CompilationProfile::Optimized),
        },
    }
}

pub(crate) fn extended_pipeline_1() -> ExtendedPipelineDescr {
    ExtendedPipelineDescr {
        id: PipelineId(uuid!("67e55044-10b1-426f-9247-bb680e5fe0c8")),
        name: "example1".to_string(),
        description: "Description of the pipeline example1".to_string(),
        version: Version(4),
        created_at: Default::default(),
        runtime_config: RuntimeConfig {
            workers: 16,
            storage: false,
            cpu_profiler: false,
            tracing: false,
            tracing_endpoint_jaeger: "".to_string(),
            min_batch_size_records: 0,
            max_buffering_delay_usecs: 0,
            resources: Default::default(),
            min_storage_bytes: None,
        },
        program_code: "CREATE TABLE table1 ( col1 INT );".to_string(),
        program_config: ProgramConfig {
            profile: Some(CompilationProfile::Optimized),
        },
        program_version: Version(2),
        program_info: None,
        program_status: ProgramStatus::Pending,
        program_status_since: Default::default(),
        program_binary_url: None,
        deployment_config: None,
        deployment_location: None,
        deployment_status: PipelineStatus::Shutdown,
        deployment_status_since: Default::default(),
        deployment_desired_status: PipelineStatus::Shutdown,
        deployment_error: None,
    }
}

pub(crate) fn extended_pipeline_2() -> ExtendedPipelineDescr {
    ExtendedPipelineDescr {
        id: PipelineId(uuid!("67e55044-10b1-426f-9247-bb680e5fe0c9")),
        name: "example2".to_string(),
        description: "Description of the pipeline example2".to_string(),
        version: Version(1),
        created_at: Default::default(),
        runtime_config: RuntimeConfig {
            workers: 10,
            storage: true,
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
        },
        program_code: "CREATE TABLE table2 ( col2 VARCHAR );".to_string(),
        program_config: ProgramConfig {
            profile: Some(CompilationProfile::Unoptimized),
        },
        program_version: Version(1),
        program_info: None,
        program_status: ProgramStatus::Pending,
        program_status_since: Default::default(),
        program_binary_url: None,
        deployment_config: None,
        deployment_location: None,
        deployment_status: PipelineStatus::Shutdown,
        deployment_status_since: Default::default(),
        deployment_desired_status: PipelineStatus::Shutdown,
        deployment_error: None,
    }
}

pub(crate) fn list_extended_pipeline_optional_code() -> Vec<ExtendedPipelineDescrOptionalCode> {
    vec![
        ExtendedPipelineDescrOptionalCode::new(extended_pipeline_1(), false),
        ExtendedPipelineDescrOptionalCode::new(extended_pipeline_2(), false),
    ]
}

pub(crate) fn patch_pipeline() -> PatchPipeline {
    PatchPipeline {
        name: None,
        description: Some("This is a new description".to_string()),
        runtime_config: None,
        program_code: Some("CREATE TABLE table3 ( col3 INT );".to_string()),
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
    })
}

pub(crate) fn error_program_not_yet_compiled() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&DBError::ProgramNotYetCompiled)
}

pub(crate) fn error_program_failed_compilation() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&DBError::ProgramFailedToCompile)
}

pub(crate) fn error_invalid_pipeline_action() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&ManagerError::InvalidPipelineAction {
        action: "dance".to_string(),
    })
}

pub(crate) fn error_illegal_pipeline_action() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&DBError::IllegalPipelineStateTransition {
            hint: "Cannot restart the pipeline while it is shutting down. Wait for the shutdown to complete before starting a new instance of the pipeline.".to_string(),
            status: PipelineStatus::ShuttingDown,
            desired_status: PipelineStatus::Shutdown,
            requested_desired_status: PipelineStatus::Running,
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
