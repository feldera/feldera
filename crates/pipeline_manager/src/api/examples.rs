use std::collections::BTreeMap;
/// Example errors for use in OpenApi docs.
use std::time::Duration;

use pipeline_types::{
    config::{PipelineConfig, RuntimeConfig},
    error::ErrorResponse,
};

use crate::api::{
    ConnectorConfig, KafkaOutput, KafkaService, ServiceConfig, ServiceConfigType, UrlInput,
};
use crate::{
    db::{
        ConnectorId, DBError, PipelineId, PipelineRevision, PipelineStatus, ProgramId, ServiceId,
        Version,
    },
    runner::RunnerError,
};
use pipeline_types::config::default_max_buffered_records;
use pipeline_types::format::csv::CsvParserConfig;
use pipeline_types::transport::kafka::{
    default_initialization_timeout_secs, default_max_inflight_messages,
};
use uuid::uuid;

use super::ManagerError;

pub(crate) fn unknown_program() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&DBError::UnknownProgram {
        program_id: ProgramId(uuid!("67e55044-10b1-426f-9247-bb680e5fe0c8")),
    })
}

pub(crate) fn unknown_name() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&DBError::UnknownName {
        name: "unknown_name".to_string(),
    })
}

pub(crate) fn duplicate_name() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&DBError::DuplicateName)
}

pub(crate) fn program_in_use_by_pipeline() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&DBError::ProgramInUseByPipeline {
        program_name: "unknown_name".to_string(),
    })
}

pub(crate) fn invalid_uuid_param() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&ManagerError::InvalidUuidParam{value: "not_a_uuid".to_string(), error: "invalid character: expected an optional prefix of `urn:uuid:` followed by [0-9a-fA-F-], found `n` at 1".to_string()})
}

pub(crate) fn outdated_program_version() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&DBError::OutdatedProgramVersion {
        latest_version: Version(5),
    })
}

pub(crate) fn unknown_pipeline() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&DBError::UnknownPipeline {
        pipeline_id: PipelineId(uuid!("2e79afe1-ff4d-44d3-af5f-9397de7746c0")),
    })
}

pub(crate) fn unknown_connector() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&DBError::UnknownConnector {
        connector_id: ConnectorId(uuid!("d764b9e2-19f2-4572-ba20-8b42641b07c4")),
    })
}

pub(crate) fn unknown_service() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&DBError::UnknownService {
        service_id: ServiceId(uuid!("12345678-9123-4567-8912-345678912345")),
    })
}

pub(crate) fn pipeline_shutdown() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&RunnerError::PipelineShutdown {
        pipeline_id: PipelineId(uuid!("2e79afe1-ff4d-44d3-af5f-9397de7746c0")),
    })
}

pub(crate) fn program_not_set() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&DBError::ProgramNotSet)
}

pub(crate) fn program_not_compiled() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&DBError::ProgramNotCompiled)
}

pub(crate) fn program_has_errors() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&DBError::ProgramFailedToCompile)
}

pub(crate) fn pipeline_invalid_input_ac() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&DBError::TablesNotInSchema {
        missing: vec![("ac_name".to_string(), "my_table".to_string())],
    })
}

pub(crate) fn pipeline_invalid_output_ac() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&DBError::ViewsNotInSchema {
        missing: vec![("ac_name".to_string(), "my_view".to_string())],
    })
}

pub(crate) fn pipeline_timeout() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&RunnerError::PipelineInitializationTimeout {
        pipeline_id: PipelineId(uuid!("2e79afe1-ff4d-44d3-af5f-9397de7746c0")),
        timeout: Duration::from_millis(10_000),
    })
}

pub(crate) fn invalid_pipeline_action() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&ManagerError::InvalidPipelineAction {
        action: "my_action".to_string(),
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

pub(crate) fn cannot_delete_when_running() -> ErrorResponse {
    ErrorResponse::from_error_nolog(&RunnerError::IllegalPipelineStateTransition {
            pipeline_id: PipelineId(uuid!("2e79afe1-ff4d-44d3-af5f-9397de7746c0")),
            error: "Cannot delete a running pipeline. Shutdown the pipeline first by invoking the '/shutdown' endpoint.".to_string(),
            current_status: PipelineStatus::Running,
            desired_status: PipelineStatus::Running,
            requested_status: None,
    })
}

pub(crate) fn pipeline_config() -> PipelineConfig {
    let input_connector = crate::db::ConnectorDescr {
        connector_id: ConnectorId(uuid!("01890c99-376f-743e-ac30-87b6c0ce74ef")),
        name: "Input".into(),
        description: "My Input Connector".into(),
        config: ConnectorConfig::UrlInput(UrlInput {
            url: "http://example.com/file.csv".to_string(),
            format: crate::api::FormatConfig::Csv(CsvParserConfig {}),
            max_buffered_records: 1000000,
        }),
    };
    let input = crate::db::AttachedConnector {
        name: "Input-To-Table".into(),
        is_input: true,
        connector_name: input_connector.name.clone(),
        relation_name: "my_input_table".into(),
    };
    let kafka_service = crate::db::ServiceDescr {
        service_id: ServiceId(uuid!("01890c99-376f-abcd-ac30-87b6c0ce1234")),
        name: "kafka-example-service".to_string(),
        description: "Some description".to_string(),
        config: ServiceConfig::Kafka(KafkaService {
            bootstrap_servers: vec!["example.com".to_string()],
            options: Default::default(),
        }),
        config_type: KafkaService::config_type(),
    };
    let mut service_name_to_id = BTreeMap::new();
    service_name_to_id.insert(kafka_service.name.clone(), kafka_service.service_id);
    let output_connector = crate::db::ConnectorDescr {
        connector_id: ConnectorId(uuid!("01890c99-3734-7052-9e97-55c0679a5adb")),
        name: "Output ".into(),
        description: "My Output Connector".into(),
        config: ConnectorConfig::KafkaOutput(KafkaOutput {
            kafka_service: kafka_service.name.clone(),
            kafka_options: BTreeMap::new(),
            topic: "example_topic".to_string(),
            format: crate::api::FormatConfig::Csv(CsvParserConfig {}),
            max_buffered_records: default_max_buffered_records(),
            log_level: None,
            max_inflight_messages: default_max_inflight_messages(),
            initialization_timeout_secs: default_initialization_timeout_secs(),
            fault_tolerance: None,
        }),
    };
    let output = crate::db::AttachedConnector {
        name: "Output-To-View".into(),
        is_input: false,
        connector_name: output_connector.name.clone(),
        relation_name: "my_output_view".into(),
    };
    let pipeline = crate::db::PipelineDescr {
        pipeline_id: PipelineId(uuid!("67e55044-10b1-426f-9247-bb680e5fe0c8")),
        program_name: Some("my-program".to_string()),
        name: "My Pipeline".into(),
        description: "My Description".into(),
        config: RuntimeConfig::from_yaml("workers: 8\n"),
        attached_connectors: vec![input, output],
        version: Version(1),
    };

    let connectors = vec![input_connector, output_connector];
    let services_for_connectors = vec![vec![], vec![kafka_service]];
    PipelineRevision::generate_pipeline_config(&pipeline, &connectors, &services_for_connectors)
        .unwrap()
}

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
