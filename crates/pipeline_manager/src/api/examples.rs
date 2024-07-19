use crate::db::error::DBError;
use crate::db::types::common::Version;
use crate::db::types::pipeline::{
    ExtendedPipelineDescr, PipelineDescr, PipelineId, PipelineStatus,
};
use crate::db::types::program::{CompilationProfile, ProgramConfig, ProgramStatus};
use crate::runner::RunnerError;
/// Example errors for use in OpenApi docs.
use pipeline_types::{config::RuntimeConfig, error::ErrorResponse};
use uuid::uuid;

use super::ManagerError;

pub(crate) fn unknown_api_key() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&DBError::UnknownApiKey {
        name: "unknown_api_key".to_string(),
    })
}

pub(crate) fn invalid_uuid_param() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&ManagerError::InvalidUuidParam{value: "not_a_uuid".to_string(), error: "invalid character: expected an optional prefix of `urn:uuid:` followed by [0-9a-fA-F-], found `n` at 1".to_string()})
}

pub(crate) fn duplicate_name() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&DBError::DuplicateName)
}

pub(crate) fn unknown_pipeline() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&DBError::UnknownPipeline {
        pipeline_id: PipelineId(uuid!("2e79afe1-ff4d-44d3-af5f-9397de7746c0")),
    })
}

pub(crate) fn pipeline_shutdown() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&RunnerError::PipelineShutdown {
        pipeline_id: PipelineId(uuid!("2e79afe1-ff4d-44d3-af5f-9397de7746c0")),
    })
}

pub(crate) fn program_not_compiled() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&DBError::ProgramNotYetCompiled)
}

pub(crate) fn program_has_errors() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&DBError::ProgramFailedToCompile)
}

pub(crate) fn invalid_pipeline_action() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&ManagerError::InvalidPipelineAction {
        action: "dance".to_string(),
    })
}

pub(crate) fn illegal_pipeline_action() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&RunnerError::IllegalPipelineStateTransition {
            pipeline_id: PipelineId(uuid!("2e79afe1-ff4d-44d3-af5f-9397de7746c0")),
            error: "Cannot restart the pipeline while it is shutting down. Wait for the shutdown to complete before starting a new instance of the pipeline.".to_string(),
            current_status: PipelineStatus::ShuttingDown,
            desired_status: PipelineStatus::Shutdown,
            requested_status: Some(PipelineStatus::Running),
    })
}

pub(crate) fn pipeline_not_shutdown() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&RunnerError::IllegalPipelineStateTransition {
            pipeline_id: PipelineId(uuid!("2e79afe1-ff4d-44d3-af5f-9397de7746c0")),
            error: "Cannot delete a running pipeline. Shutdown the pipeline first by invoking the '/shutdown' endpoint.".to_string(),
            current_status: PipelineStatus::Running,
            desired_status: PipelineStatus::Running,
            requested_status: None,
    })
}

pub(crate) fn pipeline() -> PipelineDescr {
    PipelineDescr {
        name: "example".to_string(),
        description: "Description of the example pipeline".to_string(),
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

pub(crate) fn extended_pipeline() -> ExtendedPipelineDescr<String> {
    ExtendedPipelineDescr {
        id: PipelineId(uuid!("67e55044-10b1-426f-9247-bb680e5fe0c8")),
        name: "example".to_string(),
        description: "Description of the example pipeline".to_string(),
        version: Version(1),
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
        program_version: Version(1),
        program_schema: None,
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

// TODO: remove any no longer relevant below
//
// TODO: These all require dependencies to the adapter crate and are used
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
