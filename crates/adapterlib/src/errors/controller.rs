use std::{
    backtrace::Backtrace,
    borrow::Cow,
    error::Error as StdError,
    fmt::{Display, Error as FmtError, Formatter},
    io::{Error as IoError, ErrorKind},
    string::ToString,
};

use actix_web::body::BoxBody;
use actix_web::http::StatusCode;
use actix_web::{HttpResponse, HttpResponseBuilder, ResponseError};
use anyhow::Error as AnyError;
use dbsp::{storage::backend::StorageError, Error as DbspError};
use feldera_types::{
    error::{DetailedError, ErrorResponse},
    runtime_status::RuntimeDesiredStatus,
    suspend::SuspendError,
};
use serde::{ser::SerializeStruct, Serialize, Serializer};

use super::journal::StepError;
use crate::{format::ParseError, transport::Step, DbspDetailedError};

/// Controller configuration error.
#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum ConfigError {
    /// Failed to parse pipeline configuration.
    PipelineConfigParseError {
        error: String,
    },

    /// Failed to parse parser configuration for an endpoint.
    ParserConfigParseError {
        endpoint_name: String,
        error: String,
        config: String,
    },

    /// Failed to parse encoder configuration for an endpoint.
    EncoderConfigParseError {
        endpoint_name: String,
        error: String,
        config: String,
    },

    /// Input endpoint with this name already exists.
    DuplicateInputEndpoint {
        endpoint_name: String,
    },

    /// Input table with this name already exists.
    DuplicateInputStream {
        stream_name: String,
    },

    /// Output endpoint with this name already exists.
    DuplicateOutputEndpoint {
        endpoint_name: String,
    },

    /// Output view with this name already exists.
    DuplicateOutputStream {
        stream_name: String,
    },

    /// Endpoint configuration specifies unknown input format name.
    UnknownInputFormat {
        endpoint_name: String,
        format_name: String,
    },

    /// Endpoint configuration specifies unknown output format name.
    UnknownOutputFormat {
        endpoint_name: String,
        format_name: String,
    },

    /// Endpoint configuration specifies unknown input transport name.
    UnknownInputTransport {
        endpoint_name: String,
        transport_name: String,
    },

    /// Endpoint configuration specifies unknown output transport name.
    UnknownOutputTransport {
        endpoint_name: String,
        transport_name: String,
    },

    /// Endpoint configuration specifies an input stream name
    /// that is not found in the circuit catalog.
    UnknownInputStream {
        endpoint_name: String,
        stream_name: String,
    },

    /// Endpoint configuration specifies an output stream name
    /// that is not found in the circuit catalog.
    UnknownOutputStream {
        endpoint_name: String,
        stream_name: String,
    },

    /// Endpoint configuration specifies an index name
    /// that is not found in the circuit catalog.
    UnknownIndex {
        endpoint_name: String,
        index_name: String,
    },

    NotAnIndex {
        endpoint_name: String,
        index_name: String,
    },

    InputFormatNotSupported {
        endpoint_name: String,
        error: String,
    },

    OutputFormatNotSupported {
        endpoint_name: String,
        error: String,
    },

    InputFormatNotSpecified {
        endpoint_name: String,
    },

    OutputFormatNotSpecified {
        endpoint_name: String,
    },

    InvalidEncoderConfig {
        endpoint_name: String,
        error: String,
    },

    InvalidParserConfig {
        endpoint_name: String,
        error: String,
    },

    InvalidTransportConfig {
        endpoint_name: String,
        error: String,
    },

    InvalidOutputBufferConfig {
        endpoint_name: String,
        error: String,
    },

    CyclicDependency {
        cycle: Vec<(String, String)>,
    },

    EmptyStartAfter {
        endpoint_name: String,
    },

    FtRequiresStorage,
    FtRequiresFtInput,
}

impl StdError for ConfigError {}

impl DbspDetailedError for ConfigError {
    fn error_code(&self) -> Cow<'static, str> {
        match self {
            Self::PipelineConfigParseError { .. } => Cow::from("PipelineConfigParseError"),
            Self::ParserConfigParseError { .. } => Cow::from("ParserConfigParseError"),
            Self::EncoderConfigParseError { .. } => Cow::from("EncoderConfigParseError"),
            Self::DuplicateInputEndpoint { .. } => Cow::from("DuplicateInputEndpoint"),
            Self::DuplicateInputStream { .. } => Cow::from("DuplicateInputStream"),
            Self::DuplicateOutputEndpoint { .. } => Cow::from("DuplicateOutputEndpoint"),
            Self::DuplicateOutputStream { .. } => Cow::from("DuplicateOutputStream"),
            Self::UnknownInputFormat { .. } => Cow::from("UnknownInputFormat"),
            Self::UnknownOutputFormat { .. } => Cow::from("UnknownOutputFormat"),
            Self::UnknownInputTransport { .. } => Cow::from("UnknownInputTransport"),
            Self::UnknownOutputTransport { .. } => Cow::from("UnknownOutputTransport"),
            Self::UnknownInputStream { .. } => Cow::from("UnknownInputStream"),
            Self::UnknownOutputStream { .. } => Cow::from("UnknownOutputStream"),
            Self::UnknownIndex { .. } => Cow::from("UnknownIndex"),
            Self::NotAnIndex { .. } => Cow::from("NotAnIndex"),
            Self::InputFormatNotSupported { .. } => Cow::from("InputFormatNotSupported"),
            Self::OutputFormatNotSupported { .. } => Cow::from("OutputFormatNotSupported"),
            Self::InputFormatNotSpecified { .. } => Cow::from("InputFormatNotSpecified"),
            Self::OutputFormatNotSpecified { .. } => Cow::from("OutputFormatNotSpecified"),
            Self::InvalidEncoderConfig { .. } => Cow::from("InvalidEncoderConfig"),
            Self::InvalidParserConfig { .. } => Cow::from("InvalidParserConfig"),
            Self::InvalidTransportConfig { .. } => Cow::from("InvalidTransportConfig"),
            Self::InvalidOutputBufferConfig { .. } => Cow::from("InvalidOutputBufferConfig"),
            Self::FtRequiresStorage => Cow::from("FtRequiresStorage"),
            Self::FtRequiresFtInput => Cow::from("FtWithNonFtInput"),
            Self::CyclicDependency { .. } => Cow::from("CyclicDependency"),
            Self::EmptyStartAfter { .. } => Cow::from("EmptyStartAfter"),
        }
    }
}

impl Display for ConfigError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), FmtError> {
        match self {
            Self::PipelineConfigParseError { error } => {
                write!(f, "Failed to parse pipeline configuration: {error}")
            }
            Self::ParserConfigParseError {
                endpoint_name,
                error,
                config,
            } => {
                write!(
                    f,
                    "Error parsing format configuration for input endpoint '{endpoint_name}': {error}\nInvalid configuration: {config}"
                )
            }
            Self::EncoderConfigParseError {
                endpoint_name,
                error,
                config,
            } => {
                write!(
                    f,
                    "Error parsing format configuration for output endpoint '{endpoint_name}': {error}\nInvalid configuration: {config}"
                )
            }
            Self::DuplicateInputEndpoint { endpoint_name } => {
                write!(f, "Input endpoint '{endpoint_name}' already exists")
            }
            Self::DuplicateInputStream { stream_name } => {
                write!(f, "Duplicate table name '{stream_name}'")
            }
            Self::UnknownInputFormat {
                endpoint_name,
                format_name,
            } => {
                write!(f, "Input endpoint '{endpoint_name}' specifies unknown input format '{format_name}'")
            }
            Self::UnknownInputTransport {
                endpoint_name,
                transport_name,
            } => {
                write!(f, "Input endpoint '{endpoint_name}' specifies unknown input transport '{transport_name}'")
            }
            Self::DuplicateOutputEndpoint { endpoint_name } => {
                write!(f, "Output endpoint '{endpoint_name}' already exists")
            }
            Self::DuplicateOutputStream { stream_name } => {
                write!(f, "Duplicate table or view name '{stream_name}'")
            }
            Self::UnknownOutputFormat {
                endpoint_name,
                format_name,
            } => {
                write!(f, "Output endpoint '{endpoint_name}' specifies unknown output format '{format_name}'")
            }
            Self::UnknownOutputTransport {
                endpoint_name,
                transport_name,
            } => {
                write!(f, "Output endpoint '{endpoint_name}' specifies unknown output transport '{transport_name}'")
            }
            Self::UnknownInputStream {
                endpoint_name,
                stream_name,
            } => {
                write!(
                    f,
                    "Input endpoint '{endpoint_name}' specifies unknown table '{stream_name}'"
                )
            }
            Self::UnknownOutputStream {
                endpoint_name,
                stream_name,
            } => {
                write!(f, "Output endpoint '{endpoint_name}' specifies unknown output table or view '{stream_name}'")
            }
            Self::UnknownIndex {
                endpoint_name,
                index_name,
            } => {
                write!(f, "Output endpoint '{endpoint_name}' specifies index name '{index_name}'; however, the '{index_name}' relation is not an index")
            }

            Self::NotAnIndex {
                endpoint_name,
                index_name,
            } => {
                write!(f, "Output endpoint '{endpoint_name}' specifies unknown index '{index_name}'")
            }

            Self::InputFormatNotSupported {
                endpoint_name,
                error,
            } => {
                write!(
                    f,
                    "Format not supported on input endpoint '{endpoint_name}': {error}"
                )
            }
            Self::OutputFormatNotSupported {
                endpoint_name,
                error,
            } => {
                write!(
                    f,
                    "Format not supported on output endpoint '{endpoint_name}': {error}"
                )
            }
            Self::InputFormatNotSpecified { endpoint_name } => {
                write!(
                    f,
                    "Data format is not specified for input endpoint '{endpoint_name}' (set the 'format' field inside connector configuration)"
                )
            }
            Self::OutputFormatNotSpecified { endpoint_name } => {
                write!(
                    f,
                    "Data format is not specified for output endpoint '{endpoint_name}' (set the 'format' field inside connector configuration)"
                )
            }
            Self::InvalidEncoderConfig {
                endpoint_name,
                error,
            } => {
                write!(
                    f,
                    "invalid format configuration for output endpoint '{endpoint_name}': {error}"
                )
            }
            Self::InvalidParserConfig {
                endpoint_name,
                error,
            } => {
                write!(
                    f,
                    "invalid format configuration for input endpoint '{endpoint_name}': {error}"
                )
            }
            Self::InvalidTransportConfig {
                endpoint_name,
                error,
            } => {
                write!(
                    f,
                    "invalid transport configuration for endpoint '{endpoint_name}': {error}"
                )
            }
            Self::InvalidOutputBufferConfig {
                endpoint_name,
                error,
            } => {
                write!(
                    f,
                    "invalid output buffer configuration for endpoint '{endpoint_name}': {error}"
                )
            }
            Self::CyclicDependency { cycle } => {
                let mut cycle = cycle.clone();
                cycle.push(cycle[0].clone());
                let tail = cycle[1..].iter().map(|(endpoint, label)| format!("waits for endpoint '{endpoint}' with label '{label}'")).collect::<Vec<_>>().join(", which ");
                write!(f, "cyclic 'start_after' dependency detected: endpoint '{}' with label '{}' {}", cycle[0].0, cycle[0].1, tail)
            }
            Self::EmptyStartAfter { endpoint_name } => {
                write!(f, "empty 'start_after' field for input endpoint '{}'", endpoint_name)
            }
            Self::FtRequiresStorage => write!(f, "Fault tolerance is configured, which requires storage, but storage is not enabled"),
            Self::FtRequiresFtInput => write!(f, "Fault tolerance is configured, but it cannot be enabled because the pipeline has at least one non-fault-tolerant input adapter"),
        }
    }
}

impl ConfigError {
    pub fn pipeline_config_parse_error<E>(error: &E) -> Self
    where
        E: ToString,
    {
        Self::PipelineConfigParseError {
            error: error.to_string(),
        }
    }

    pub fn parser_config_parse_error<E>(endpoint_name: &str, error: &E, config: &str) -> Self
    where
        E: ToString,
    {
        Self::ParserConfigParseError {
            endpoint_name: endpoint_name.to_owned(),
            error: error.to_string(),
            config: config.to_string(),
        }
    }

    pub fn encoder_config_parse_error<E>(endpoint_name: &str, error: &E, config: &str) -> Self
    where
        E: ToString,
    {
        Self::EncoderConfigParseError {
            endpoint_name: endpoint_name.to_owned(),
            error: error.to_string(),
            config: config.to_string(),
        }
    }

    pub fn duplicate_input_endpoint(endpoint_name: &str) -> Self {
        Self::DuplicateInputEndpoint {
            endpoint_name: endpoint_name.to_owned(),
        }
    }

    pub fn duplicate_input_stream(stream_name: &str) -> Self {
        Self::DuplicateInputStream {
            stream_name: stream_name.to_owned(),
        }
    }

    pub fn unknown_input_format(endpoint_name: &str, format_name: &str) -> Self {
        Self::UnknownInputFormat {
            endpoint_name: endpoint_name.to_owned(),
            format_name: format_name.to_owned(),
        }
    }

    pub fn unknown_input_transport(endpoint_name: &str, transport_name: &str) -> Self {
        Self::UnknownInputTransport {
            endpoint_name: endpoint_name.to_owned(),
            transport_name: transport_name.to_owned(),
        }
    }

    pub fn duplicate_output_endpoint(endpoint_name: &str) -> Self {
        Self::DuplicateOutputEndpoint {
            endpoint_name: endpoint_name.to_owned(),
        }
    }

    pub fn duplicate_output_stream(stream_name: &str) -> Self {
        Self::DuplicateOutputStream {
            stream_name: stream_name.to_owned(),
        }
    }

    pub fn unknown_output_format(endpoint_name: &str, format_name: &str) -> Self {
        Self::UnknownOutputFormat {
            endpoint_name: endpoint_name.to_owned(),
            format_name: format_name.to_owned(),
        }
    }

    pub fn unknown_output_transport(endpoint_name: &str, transport_name: &str) -> Self {
        Self::UnknownOutputTransport {
            endpoint_name: endpoint_name.to_owned(),
            transport_name: transport_name.to_owned(),
        }
    }

    pub fn unknown_input_stream(endpoint_name: &str, stream_name: &str) -> Self {
        Self::UnknownInputStream {
            endpoint_name: endpoint_name.to_owned(),
            stream_name: stream_name.to_owned(),
        }
    }

    pub fn unknown_output_stream(endpoint_name: &str, stream_name: &str) -> Self {
        Self::UnknownOutputStream {
            endpoint_name: endpoint_name.to_owned(),
            stream_name: stream_name.to_owned(),
        }
    }

    pub fn unknown_index(endpoint_name: &str, index_name: &str) -> Self {
        Self::UnknownIndex {
            endpoint_name: endpoint_name.to_owned(),
            index_name: index_name.to_owned(),
        }
    }

    pub fn not_an_index(endpoint_name: &str, index_name: &str) -> Self {
        Self::NotAnIndex {
            endpoint_name: endpoint_name.to_owned(),
            index_name: index_name.to_owned(),
        }
    }

    pub fn input_format_not_supported(endpoint_name: &str, error: &str) -> Self {
        Self::InputFormatNotSupported {
            endpoint_name: endpoint_name.to_owned(),
            error: error.to_owned(),
        }
    }

    pub fn output_format_not_supported(endpoint_name: &str, error: &str) -> Self {
        Self::OutputFormatNotSupported {
            endpoint_name: endpoint_name.to_owned(),
            error: error.to_owned(),
        }
    }

    pub fn input_format_not_specified(endpoint_name: &str) -> Self {
        Self::InputFormatNotSpecified {
            endpoint_name: endpoint_name.to_owned(),
        }
    }

    pub fn output_format_not_specified(endpoint_name: &str) -> Self {
        Self::OutputFormatNotSpecified {
            endpoint_name: endpoint_name.to_owned(),
        }
    }

    pub fn invalid_encoder_configuration(endpoint_name: &str, error: &str) -> Self {
        Self::InvalidEncoderConfig {
            endpoint_name: endpoint_name.to_string(),
            error: error.to_string(),
        }
    }

    pub fn invalid_parser_configuration(endpoint_name: &str, error: &str) -> Self {
        Self::InvalidParserConfig {
            endpoint_name: endpoint_name.to_string(),
            error: error.to_string(),
        }
    }

    pub fn invalid_transport_configuration(endpoint_name: &str, error: &str) -> Self {
        Self::InvalidTransportConfig {
            endpoint_name: endpoint_name.to_string(),
            error: error.to_string(),
        }
    }

    pub fn invalid_output_buffer_configuration(endpoint_name: &str, error: &str) -> Self {
        Self::InvalidOutputBufferConfig {
            endpoint_name: endpoint_name.to_string(),
            error: error.to_string(),
        }
    }

    pub fn cyclic_dependency(cycle: Vec<(String, String)>) -> Self {
        Self::CyclicDependency { cycle }
    }

    pub fn empty_start_after(endpoint_name: &str) -> Self {
        Self::EmptyStartAfter {
            endpoint_name: endpoint_name.to_string(),
        }
    }
}

/// Controller error.
///
/// Reports all errors that arise from operating a streaming pipeline consisting
/// of input adapters, output adapters, and a DBSP circuit, via the controller
/// API.
#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum ControllerError {
    /// I/O error.
    #[serde(serialize_with = "serialize_io_error")]
    IoError {
        /// Describes the context where the error occurred.
        context: String,
        io_error: IoError,
        backtrace: Backtrace,
    },

    /// Error parsing program schema.
    SchemaParseError {
        error: String,
    },

    /// Error validating program schema.
    SchemaValidationError {
        error: String,
    },

    /// Error parsing the checkpoint.
    CheckpointParseError {
        error: String,
    },

    CheckpointDoesNotMatchPipeline,

    /// Operation cannot be initiated now because the pipeline is being restored
    /// from a checkpoint.
    RestoreInProgress,

    /// Operation cannot be initiated now because the pipeline is bootstrapping new/modified views.
    BootstrapInProgress,

    /// Error in journal metadata.
    StepError(StepError),

    /// Unexpected step number.
    UnexpectedStep {
        actual: Step,
        expected: Step,
    },

    /// Step replay failure.
    ReplayFailure {
        error: String,
    },

    /// Feature is not supported.
    NotSupported {
        error: String,
    },

    /// Error parsing program IR file.
    IrParseError {
        error: String,
    },

    /// Error parsing CLI arguments.
    CliArgsError {
        error: String,
    },

    /// Invalid controller configuration.
    Config {
        config_error: Box<ConfigError>,
    },

    /// Unknown input endpoint name.
    UnknownInputEndpoint {
        endpoint_name: String,
    },

    /// Unknown output endpoint name.
    UnknownOutputEndpoint {
        endpoint_name: String,
    },

    /// Error parsing input data.
    ///
    /// Parser errors are expected to be
    /// recoverable, i.e., the parser should be able to successfully parse
    /// new valid inputs after an error.
    ParseError {
        endpoint_name: String,
        error: Box<ParseError>,
    },

    /// Encode error.
    ///
    /// Error encoding the last output batch.  Encoder errors are expected to
    /// be recoverable, i.e., the encoder should be able to successfully parse
    /// new valid inputs after an error.
    #[serde(serialize_with = "serialize_encode_error")]
    EncodeError {
        endpoint_name: String,
        error: AnyError,
    },

    /// Input transport endpoint error.
    #[serde(serialize_with = "serialize_input_transport_error")]
    InputTransportError {
        endpoint_name: String,
        fatal: bool,
        error: AnyError,
    },

    /// Output transport endpoint error.
    #[serde(serialize_with = "serialize_output_transport_error")]
    OutputTransportError {
        endpoint_name: String,
        fatal: bool,
        error: AnyError,
    },

    /// Error evaluating the DBSP circuit.
    DbspError {
        error: DbspError,
    },

    /// Error inside the Prometheus module.
    PrometheusError {
        error: String,
    },

    // TODO: we currently don't have a way to include more info about the panic.
    /// Panic inside the DBSP runtime.
    DbspPanic,

    /// Panic inside the DBSP controller.
    ControllerPanic,

    /// Controller terminated before command could be executed.
    ControllerExit,

    /// Storage error.
    #[serde(serialize_with = "serialize_storage_error")]
    StorageError {
        /// Describes the context where the error occurred.
        context: String,
        error: StorageError,
        backtrace: Backtrace,
    },

    /// Enterprise-only feature.
    EnterpriseFeature(&'static str),

    /// Cannot checkpoint or suspend.
    SuspendError(SuspendError),

    /// An unexpected JSON serialized structure was encountered while processing the /stats endpoint.
    UnexpectedJsonStructure {
        reason: String,
    },

    /// The request relates to an old incarnation of the pipeline.
    PipelineRestarted {
        error: String,
    },

    /// Completion token specified non-existing endpoint id. This indicates that the endpoint was removed
    /// or the token is invalid.
    UnknownEndpointInCompletionToken {
        endpoint_id: u64,
    },

    /// Error fetching checkpoint from remote object storage.
    CheckpointFetchError {
        error: String,
    },

    /// Error pushing checkpoint to remote object storage.
    CheckpointPushError {
        error: String,
    },

    TransactionInProgress,
    NoTransactionInProgress,

    /// Invalid initial desired status.
    InvalidInitialStatus(RuntimeDesiredStatus),

    /// Invalid standby configuration,
    InvalidStandby(&'static str),
}

impl ResponseError for ControllerError {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::Config { config_error }
                if matches!(**config_error, ConfigError::UnknownInputStream { .. }) =>
            {
                StatusCode::NOT_FOUND
            }
            Self::Config { config_error }
                if matches!(**config_error, ConfigError::UnknownOutputStream { .. }) =>
            {
                StatusCode::NOT_FOUND
            }
            Self::Config { .. } => StatusCode::BAD_REQUEST,
            Self::UnknownInputEndpoint { .. } => StatusCode::NOT_FOUND,
            Self::UnknownOutputEndpoint { .. } => StatusCode::NOT_FOUND,
            Self::ParseError { .. } => StatusCode::BAD_REQUEST,
            Self::NotSupported { .. } => StatusCode::BAD_REQUEST,
            Self::EnterpriseFeature(_) => StatusCode::NOT_IMPLEMENTED,
            Self::RestoreInProgress => StatusCode::SERVICE_UNAVAILABLE,
            Self::BootstrapInProgress => StatusCode::SERVICE_UNAVAILABLE,
            Self::PipelineRestarted { .. } => StatusCode::GONE,
            Self::UnknownEndpointInCompletionToken { .. } => StatusCode::GONE,
            Self::TransactionInProgress => StatusCode::CONFLICT,
            Self::NoTransactionInProgress => StatusCode::BAD_REQUEST,
            Self::InvalidInitialStatus(_) => StatusCode::GONE,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn error_response(&self) -> HttpResponse<BoxBody> {
        HttpResponseBuilder::new(self.status_code()).json(ErrorResponse::from_error(self))
    }
}

fn serialize_io_error<S>(
    context: &String,
    io_error: &IoError,
    backtrace: &Backtrace,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut ser = serializer.serialize_struct("IoError", 4)?;
    ser.serialize_field("context", context)?;
    ser.serialize_field("kind", &io_error.kind().to_string())?;
    ser.serialize_field("os_error", &io_error.raw_os_error())?;
    ser.serialize_field("backtrace", &backtrace.to_string())?;
    ser.end()
}

fn serialize_storage_error<S>(
    context: &String,
    error: &StorageError,
    backtrace: &Backtrace,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut ser = serializer.serialize_struct("StorageError", 4)?;
    ser.serialize_field("context", context)?;
    ser.serialize_field("error", &error.to_string())?;
    ser.serialize_field("backtrace", &backtrace.to_string())?;
    ser.end()
}

fn serialize_encode_error<S>(
    endpoint: &String,
    error: &AnyError,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut ser = serializer.serialize_struct("EncodeError", 3)?;
    ser.serialize_field("endpoint_name", endpoint)?;
    ser.serialize_field("error", &error.to_string())?;
    ser.serialize_field("backtrace", &error.backtrace().to_string())?;
    ser.end()
}

fn serialize_input_transport_error<S>(
    endpoint: &String,
    fatal: &bool,
    error: &AnyError,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut ser = serializer.serialize_struct("InputTransportError", 4)?;
    ser.serialize_field("endpoint_name", endpoint)?;
    ser.serialize_field("fatal", fatal)?;
    ser.serialize_field("error", &error.to_string())?;
    ser.serialize_field("backtrace", &error.backtrace().to_string())?;
    ser.end()
}

fn serialize_output_transport_error<S>(
    endpoint: &String,
    fatal: &bool,
    error: &AnyError,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut ser = serializer.serialize_struct("OutputTransportError", 4)?;
    ser.serialize_field("endpoint_name", endpoint)?;
    ser.serialize_field("fatal", fatal)?;
    ser.serialize_field("error", &error.to_string())?;
    ser.serialize_field("backtrace", &error.backtrace().to_string())?;
    ser.end()
}

impl DbspDetailedError for ControllerError {
    // TODO: attempts to cast `AnyError` to `DetailedError`.
    fn error_code(&self) -> Cow<'static, str> {
        match self {
            Self::IoError { .. } => Cow::from("ControllerIoError"),
            Self::NotSupported { .. } => Cow::from("NotSupported"),
            Self::SchemaParseError { .. } => Cow::from("SchemaParseError"),
            Self::SchemaValidationError { .. } => Cow::from("SchemaParseError"),
            Self::CheckpointParseError { .. } => Cow::from("CheckpointParseError"),
            Self::CheckpointDoesNotMatchPipeline => Cow::from("CheckpointDoesNotMatchPipeline"),
            Self::RestoreInProgress => Cow::from("RestoreInProgress"),
            Self::BootstrapInProgress => Cow::from("BootstrapInProgress"),
            Self::StepError { .. } => Cow::from("StepError"),
            Self::UnexpectedStep { .. } => Cow::from("UnexpectedStep"),
            Self::ReplayFailure { .. } => Cow::from("ReplayFailure"),
            Self::IrParseError { .. } => Cow::from("IrParseError"),
            Self::CliArgsError { .. } => Cow::from("ControllerCliArgsError"),
            Self::Config { config_error } => {
                Cow::from(format!("ConfigError.{}", config_error.error_code()))
            }
            Self::UnknownInputEndpoint { .. } => Cow::from("UnknownInputEndpoint"),
            Self::UnknownOutputEndpoint { .. } => Cow::from("UnknownOutputEndpoint"),
            Self::ParseError { .. } => Cow::from("ParseError"),
            Self::EncodeError { .. } => Cow::from("EncodeError"),
            Self::InputTransportError { .. } => Cow::from("InputTransportError"),
            Self::OutputTransportError { .. } => Cow::from("OutputTransportError"),
            Self::PrometheusError { .. } => Cow::from("PrometheusError"),
            Self::DbspError { error } => error.error_code(),
            Self::DbspPanic => Cow::from("DbspPanic"),
            Self::ControllerPanic => Cow::from("ControllerPanic"),
            Self::ControllerExit => Cow::from("ControllerExit"),
            Self::EnterpriseFeature(_) => Cow::from("EnterpriseFeature"),
            Self::StorageError { .. } => Cow::from("StorageError"),
            Self::SuspendError(_) => Cow::from("SuspendError"),
            Self::UnexpectedJsonStructure { .. } => Cow::from("UnexpectedJsonStructure"),
            Self::PipelineRestarted { .. } => Cow::from("PipelineRestarted"),
            Self::UnknownEndpointInCompletionToken { .. } => {
                Cow::from("UnknownEndpointInCompletionToken")
            }
            Self::CheckpointFetchError { .. } => Cow::from("CheckpointFetchError"),
            Self::CheckpointPushError { .. } => Cow::from("CheckpointPushError"),
            Self::TransactionInProgress => Cow::from("TransactionInProgress"),
            Self::NoTransactionInProgress => Cow::from("NoTransactionInProgress"),
            Self::InvalidInitialStatus(_) => Cow::from("InvalidInitialStatus"),
            Self::InvalidStandby(_) => Cow::from("InvalidStandby"),
        }
    }
}

impl DetailedError for ControllerError {
    // TODO: attempts to cast `AnyError` to `DetailedError`.
    fn error_code(&self) -> Cow<'static, str> {
        DbspDetailedError::error_code(self)
    }
}

impl StdError for ControllerError {}

impl Display for ControllerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), FmtError> {
        match self {
            Self::IoError {
                context, io_error, ..
            } => {
                write!(f, "I/O error {context}: {io_error}")
            }
            Self::NotSupported { error } => {
                write!(f, "Not supported: {error}")
            }
            Self::SchemaParseError { error } => {
                write!(f, "Error parsing program schema: {error}")
            }
            Self::SchemaValidationError { error } => {
                write!(f, "Error validating program schema: {error}")
            }
            Self::CheckpointParseError { error } => {
                write!(f, "Error parsing checkpoint file: {error}")
            }
            Self::CheckpointDoesNotMatchPipeline => {
                write!(f, "Recovery failed: the pipeline has been recovered from a checkpoint, but the checkpoint does not match the current pipeline definition. This can be caused by a corrupted checkpoint or an internal error.")
            }
            Self::RestoreInProgress => {
                write!(f, "Operation cannot be initiated now because the pipeline is restoring from a checkpoint.")
            }
            Self::BootstrapInProgress => {
                write!(
                    f,
                    "Operation cannot be initiated while the pipeline is bootstrapping."
                )
            }
            Self::StepError(error) => write!(f, "Error with persistent input steps: {error}"),
            Self::UnexpectedStep { actual, expected } => {
                write!(f, "Read step {actual}, expected {expected}")
            }
            Self::ReplayFailure { error } => {
                write!(f, "{error}")
            }
            Self::IrParseError { error } => {
                write!(f, "Error parsing program IR: {error}")
            }
            Self::CliArgsError { error } => {
                write!(f, "Error parsing command line arguments: {error}")
            }
            Self::Config { config_error } => {
                write!(f, "invalid controller configuration: {config_error}")
            }
            Self::UnknownInputEndpoint { endpoint_name } => {
                write!(f, "unknown input endpoint name '{endpoint_name}'")
            }
            Self::UnknownOutputEndpoint { endpoint_name } => {
                write!(f, "unknown output endpoint name '{endpoint_name}'")
            }
            Self::InputTransportError {
                endpoint_name,
                fatal,
                error,
            } => {
                write!(
                    f,
                    "{}error on input endpoint '{endpoint_name}': {}",
                    if *fatal { "FATAL " } else { "" },
                    error.root_cause()
                )
            }
            Self::OutputTransportError {
                endpoint_name,
                fatal,
                error,
            } => {
                write!(
                    f,
                    "{}error on output endpoint '{endpoint_name}': {}",
                    if *fatal { "FATAL " } else { "" },
                    error.root_cause()
                )
            }
            Self::ParseError {
                endpoint_name,
                error,
            } => {
                write!(
                    f,
                    "parse error on input endpoint '{endpoint_name}': {error}"
                )
            }
            Self::EncodeError {
                endpoint_name,
                error,
            } => {
                write!(
                    f,
                    "encoder error on output endpoint '{endpoint_name}': {error}"
                )
            }
            Self::PrometheusError { error } => {
                write!(f, "Error in the Prometheus metrics module: '{error}'")
            }
            Self::DbspError { error } => {
                write!(f, "DBSP error: {error}")
            }
            Self::DbspPanic => {
                write!(f, "Panic inside the DBSP runtime")
            }
            Self::ControllerPanic => {
                write!(f, "Panic inside the DBSP controller")
            }
            Self::ControllerExit => {
                write!(f, "Controller exited before command could be executed")
            }
            Self::EnterpriseFeature(feature) => {
                write!(
                    f,
                    "Cannot use enterprise-only feature ({feature}) in Feldera community edition."
                )
            }
            Self::StorageError { context, error, .. } => {
                write!(f, "I/O error {context}: {error}")
            }
            Self::SuspendError(error) => write!(f, "{error}"),
            Self::UnexpectedJsonStructure { reason } => {
                write!(f, "An unexpected JSON structure was detected: {reason}")
            }
            Self::PipelineRestarted { error } => {
                write!(f, "{error}")
            }
            Self::UnknownEndpointInCompletionToken { endpoint_id } => {
                write!(f, "completion token specifies input endpoint id {endpoint_id}, which doesn't exist; this indicates that the input connector was deleted after the completion token was generated")
            }
            Self::CheckpointFetchError { error } => {
                write!(f, "Error fetching checkpoint from object store: {error}")
            }
            Self::CheckpointPushError { error } => {
                write!(f, "Error pushing checkpoint to object store: {error}")
            }
            Self::TransactionInProgress => {
                write!(
                    f,
                    "Cannot perform this operation while there is a transaction in progress"
                )
            }
            Self::NoTransactionInProgress => {
                write!(
                    f,
                    "This operation requires an active transaction, but none is currently in progress"
                )
            }
            Self::InvalidInitialStatus(status) => {
                write!(f, "Invalid initial status {status:?} provided on command line or read from storage (only running, paused, and standby are valid)")
            }
            Self::InvalidStandby(standby) => {
                write!(f, "Cannot enter standby mode: {standby}")
            }
        }
    }
}

impl ControllerError {
    pub fn io_error(context: String, io_error: IoError) -> Self {
        Self::IoError {
            context,
            io_error,
            backtrace: Backtrace::capture(),
        }
    }

    pub fn not_supported(error: &str) -> Self {
        Self::NotSupported {
            error: error.to_string(),
        }
    }

    pub fn schema_parse_error(error: &str) -> Self {
        Self::SchemaParseError {
            error: error.to_string(),
        }
    }

    pub fn checkpoint_does_not_match_pipeline() -> Self {
        Self::CheckpointDoesNotMatchPipeline
    }

    pub fn checkpoint_fetch_error(error: String) -> Self {
        Self::CheckpointFetchError { error }
    }

    pub fn checkpoint_push_error(error: String) -> Self {
        Self::CheckpointPushError { error }
    }

    pub fn schema_validation_error(error: &str) -> Self {
        Self::SchemaValidationError {
            error: error.to_string(),
        }
    }

    pub fn ir_parse_error(error: &str) -> Self {
        Self::IrParseError {
            error: error.to_string(),
        }
    }

    pub fn cli_args_error<E>(error: &E) -> Self
    where
        E: ToString,
    {
        Self::CliArgsError {
            error: error.to_string(),
        }
    }

    pub fn unknown_input_endpoint(endpoint_name: &str) -> Self {
        Self::UnknownInputEndpoint {
            endpoint_name: endpoint_name.to_string(),
        }
    }

    pub fn unknown_output_endpoint(endpoint_name: &str) -> Self {
        Self::UnknownOutputEndpoint {
            endpoint_name: endpoint_name.to_string(),
        }
    }

    pub fn pipeline_config_parse_error<E>(error: &E) -> Self
    where
        E: ToString,
    {
        Self::Config {
            config_error: Box::new(ConfigError::pipeline_config_parse_error(error)),
        }
    }

    pub fn parser_config_parse_error<E>(endpoint_name: &str, error: &E, config: &str) -> Self
    where
        E: ToString,
    {
        Self::Config {
            config_error: Box::new(ConfigError::parser_config_parse_error(
                endpoint_name,
                error,
                config,
            )),
        }
    }

    pub fn encoder_config_parse_error<E>(endpoint_name: &str, error: &E, config: &str) -> Self
    where
        E: ToString,
    {
        Self::Config {
            config_error: Box::new(ConfigError::encoder_config_parse_error(
                endpoint_name,
                error,
                config,
            )),
        }
    }

    pub fn duplicate_input_endpoint(endpoint_name: &str) -> Self {
        Self::Config {
            config_error: Box::new(ConfigError::duplicate_input_endpoint(endpoint_name)),
        }
    }

    pub fn duplicate_input_stream(stream_name: &str) -> Self {
        Self::Config {
            config_error: Box::new(ConfigError::duplicate_input_stream(stream_name)),
        }
    }

    pub fn unknown_input_format(endpoint_name: &str, format_name: &str) -> Self {
        Self::Config {
            config_error: Box::new(ConfigError::unknown_input_format(
                endpoint_name,
                format_name,
            )),
        }
    }

    pub fn unknown_input_transport(endpoint_name: &str, transport_name: &str) -> Self {
        Self::Config {
            config_error: Box::new(ConfigError::unknown_input_transport(
                endpoint_name,
                transport_name,
            )),
        }
    }

    pub fn duplicate_output_endpoint(endpoint_name: &str) -> Self {
        Self::Config {
            config_error: Box::new(ConfigError::duplicate_output_endpoint(endpoint_name)),
        }
    }

    pub fn duplicate_output_stream(stream_name: &str) -> Self {
        Self::Config {
            config_error: Box::new(ConfigError::duplicate_output_stream(stream_name)),
        }
    }

    pub fn unknown_output_format(endpoint_name: &str, format_name: &str) -> Self {
        Self::Config {
            config_error: Box::new(ConfigError::unknown_output_format(
                endpoint_name,
                format_name,
            )),
        }
    }

    pub fn unknown_output_transport(endpoint_name: &str, transport_name: &str) -> Self {
        Self::Config {
            config_error: Box::new(ConfigError::unknown_output_transport(
                endpoint_name,
                transport_name,
            )),
        }
    }

    pub fn unknown_input_stream(endpoint_name: &str, stream_name: &str) -> Self {
        Self::Config {
            config_error: Box::new(ConfigError::unknown_input_stream(
                endpoint_name,
                stream_name,
            )),
        }
    }

    pub fn unknown_output_stream(endpoint_name: &str, stream_name: &str) -> Self {
        Self::Config {
            config_error: Box::new(ConfigError::unknown_output_stream(
                endpoint_name,
                stream_name,
            )),
        }
    }

    pub fn unknown_index(endpoint_name: &str, index_name: &str) -> Self {
        Self::Config {
            config_error: Box::new(ConfigError::unknown_index(endpoint_name, index_name)),
        }
    }

    pub fn not_an_index(endpoint_name: &str, index_name: &str) -> Self {
        Self::Config {
            config_error: Box::new(ConfigError::not_an_index(endpoint_name, index_name)),
        }
    }

    pub fn input_format_not_supported(endpoint_name: &str, error: &str) -> Self {
        Self::Config {
            config_error: Box::new(ConfigError::input_format_not_supported(
                endpoint_name,
                error,
            )),
        }
    }

    pub fn output_format_not_supported(endpoint_name: &str, error: &str) -> Self {
        Self::Config {
            config_error: Box::new(ConfigError::output_format_not_supported(
                endpoint_name,
                error,
            )),
        }
    }

    pub fn input_format_not_specified(endpoint_name: &str) -> Self {
        Self::Config {
            config_error: Box::new(ConfigError::input_format_not_specified(endpoint_name)),
        }
    }

    pub fn output_format_not_specified(endpoint_name: &str) -> Self {
        Self::Config {
            config_error: Box::new(ConfigError::output_format_not_specified(endpoint_name)),
        }
    }

    pub fn invalid_encoder_configuration(endpoint_name: &str, error: &str) -> Self {
        Self::Config {
            config_error: Box::new(ConfigError::invalid_encoder_configuration(
                endpoint_name,
                error,
            )),
        }
    }

    pub fn invalid_parser_configuration(endpoint_name: &str, error: &str) -> Self {
        Self::Config {
            config_error: Box::new(ConfigError::invalid_parser_configuration(
                endpoint_name,
                error,
            )),
        }
    }

    pub fn invalid_transport_configuration(endpoint_name: &str, error: &str) -> Self {
        Self::Config {
            config_error: Box::new(ConfigError::invalid_transport_configuration(
                endpoint_name,
                error,
            )),
        }
    }

    pub fn invalid_output_buffer_configuration(endpoint_name: &str, error: &str) -> Self {
        Self::Config {
            config_error: Box::new(ConfigError::invalid_output_buffer_configuration(
                endpoint_name,
                error,
            )),
        }
    }

    pub fn input_transport_error(endpoint_name: &str, fatal: bool, error: AnyError) -> Self {
        Self::InputTransportError {
            endpoint_name: endpoint_name.to_owned(),
            fatal,
            error,
        }
    }

    pub fn output_transport_error(endpoint_name: &str, fatal: bool, error: AnyError) -> Self {
        Self::OutputTransportError {
            endpoint_name: endpoint_name.to_owned(),
            fatal,
            error,
        }
    }

    pub fn parse_error(endpoint_name: &str, error: ParseError) -> Self {
        Self::ParseError {
            endpoint_name: endpoint_name.to_owned(),
            error: Box::new(error),
        }
    }

    pub fn encode_error(endpoint_name: &str, error: AnyError) -> Self {
        Self::EncodeError {
            endpoint_name: endpoint_name.to_owned(),
            error,
        }
    }

    pub fn prometheus_error<E>(error: &E) -> Self
    where
        E: ToString,
    {
        Self::PrometheusError {
            error: error.to_string(),
        }
    }

    pub fn dbsp_error(error: DbspError) -> Self {
        Self::DbspError { error }
    }

    pub fn dbsp_panic() -> Self {
        Self::DbspPanic
    }

    pub fn controller_panic() -> Self {
        Self::ControllerPanic
    }

    pub fn storage_error(context: impl Into<String>, error: StorageError) -> Self {
        Self::StorageError {
            context: context.into(),
            error,
            backtrace: Backtrace::capture(),
        }
    }

    pub fn pipeline_restarted(error: &str) -> Self {
        Self::PipelineRestarted {
            error: error.to_string(),
        }
    }

    pub fn unknown_endpoint_in_completion_token(endpoint_id: u64) -> Self {
        Self::UnknownEndpointInCompletionToken { endpoint_id }
    }

    pub fn kind(&self) -> ErrorKind {
        match self {
            Self::IoError { io_error, .. } => io_error.kind(),
            Self::StorageError { error, .. } => error.kind(),
            Self::StepError(error) => error.kind(),
            Self::RestoreInProgress
            | Self::BootstrapInProgress
            | Self::SuspendError(SuspendError::Temporary(_))
            | Self::TransactionInProgress => ErrorKind::ResourceBusy,
            Self::NotSupported { .. } | Self::SuspendError(SuspendError::Permanent(_)) => {
                ErrorKind::Unsupported
            }
            Self::DbspError {
                error: DbspError::IO(error),
            } => error.kind(),
            Self::DbspError { .. }
            | Self::SchemaParseError { .. }
            | Self::SchemaValidationError { .. }
            | Self::CheckpointParseError { .. }
            | Self::CheckpointDoesNotMatchPipeline
            | Self::UnexpectedStep { .. }
            | Self::ReplayFailure { .. }
            | Self::IrParseError { .. }
            | Self::CliArgsError { .. }
            | Self::Config { .. }
            | Self::UnknownInputEndpoint { .. }
            | Self::UnknownOutputEndpoint { .. }
            | Self::ParseError { .. }
            | Self::EncodeError { .. }
            | Self::InputTransportError { .. }
            | Self::OutputTransportError { .. }
            | Self::PrometheusError { .. }
            | Self::DbspPanic
            | Self::ControllerPanic
            | Self::ControllerExit
            | Self::EnterpriseFeature(_)
            | Self::UnexpectedJsonStructure { .. }
            | Self::UnknownEndpointInCompletionToken { .. }
            | Self::CheckpointFetchError { .. }
            | Self::CheckpointPushError { .. }
            | Self::PipelineRestarted { .. }
            | Self::NoTransactionInProgress
            | Self::InvalidInitialStatus(_)
            | Self::InvalidStandby(_) => ErrorKind::Other,
        }
    }
}

impl From<ConfigError> for ControllerError {
    fn from(config_error: ConfigError) -> Self {
        Self::Config {
            config_error: Box::new(config_error),
        }
    }
}

impl From<DbspError> for ControllerError {
    fn from(error: DbspError) -> Self {
        Self::DbspError { error }
    }
}

impl From<SuspendError> for ControllerError {
    fn from(error: SuspendError) -> Self {
        Self::SuspendError(error)
    }
}
