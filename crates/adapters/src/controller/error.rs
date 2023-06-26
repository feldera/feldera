use crate::DetailedError;
use anyhow::Error as AnyError;
use dbsp::Error as DBSPError;
use serde::Serialize;
use std::{
    borrow::Cow,
    error::Error as StdError,
    fmt::{Display, Error as FmtError, Formatter},
    string::ToString,
};

/// Controller configuration error.
#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum ConfigError {
    /// Input endpoint with this name already exists.
    DuplicateInputEndpoint { endpoint_name: String },

    /// Output endpoint with this name already exists.
    DuplicateOutputEndpoint { endpoint_name: String },

    /// Endpoint configuration specifies unknown input format name.
    UnknownInputFormat { format_name: String },

    /// Endpoint configuration specifies unknown output format name.
    UnknownOutputFormat { format_name: String },

    /// Endpoint configuration specifies unknown input transport name.
    UnknownInputTransport { transport_name: String },

    /// Endpoint configuration specifies unknown output transport name.
    UnknownOutputTransport { transport_name: String },

    /// Endpoint configuration specifies an input stream name
    /// that is not found in the circuit catalog.
    UnknownInputStream { stream_name: String },

    /// Endpoint configuration specifies an output stream name
    /// that is not found in the circuit catalog.
    UnknownOutputStream { stream_name: String },
}

impl StdError for ConfigError {}

impl DetailedError for ConfigError {
    fn error_code(&self) -> Cow<'static, str> {
        match self {
            Self::DuplicateInputEndpoint { .. } => Cow::from("DuplicateInputEndpoint"),
            Self::DuplicateOutputEndpoint { .. } => Cow::from("DuplicateOutputEndpoint"),
            Self::UnknownInputFormat { .. } => Cow::from("UnknownInputFormat"),
            Self::UnknownOutputFormat { .. } => Cow::from("UnknownOutputFormat"),
            Self::UnknownInputTransport { .. } => Cow::from("UnknownInputTransport"),
            Self::UnknownOutputTransport { .. } => Cow::from("UnknownOutputTransport"),
            Self::UnknownInputStream { .. } => Cow::from("UnknownInputStream"),
            Self::UnknownOutputStream { .. } => Cow::from("UnknownOutputStream"),
        }
    }
}

impl Display for ConfigError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), FmtError> {
        match self {
            Self::DuplicateInputEndpoint { endpoint_name } => {
                write!(f, "Input endpoint '{endpoint_name}' already exists")
            }
            Self::UnknownInputFormat { format_name } => {
                write!(f, "Unknown input format '{format_name}'")
            }
            Self::UnknownInputTransport { transport_name } => {
                write!(f, "Unknown input transport '{transport_name}'")
            }
            Self::DuplicateOutputEndpoint { endpoint_name } => {
                write!(f, "Output endpoint '{endpoint_name}' already exists")
            }
            Self::UnknownOutputFormat { format_name } => {
                write!(f, "Unknown output format '{format_name}'")
            }
            Self::UnknownOutputTransport { transport_name } => {
                write!(f, "Unknown output transport '{transport_name}'")
            }
            Self::UnknownInputStream { stream_name } => {
                write!(f, "Unknown table '{stream_name}'")
            }
            Self::UnknownOutputStream { stream_name } => {
                write!(f, "Unknown output table or view '{stream_name}'")
            }
        }
    }
}

impl ConfigError {
    pub fn duplicate_input_endpoint(endpoint_name: &str) -> Self {
        Self::DuplicateInputEndpoint {
            endpoint_name: endpoint_name.to_owned(),
        }
    }

    pub fn unknown_input_format(format_name: &str) -> Self {
        Self::UnknownInputFormat {
            format_name: format_name.to_owned(),
        }
    }

    pub fn unknown_input_transport(transport_name: &str) -> Self {
        Self::UnknownInputTransport {
            transport_name: transport_name.to_owned(),
        }
    }

    pub fn duplicate_output_endpoint(endpoint_name: &str) -> Self {
        Self::DuplicateOutputEndpoint {
            endpoint_name: endpoint_name.to_owned(),
        }
    }

    pub fn unknown_output_format(format_name: &str) -> Self {
        Self::UnknownOutputFormat {
            format_name: format_name.to_owned(),
        }
    }

    pub fn unknown_output_transport(transport_name: &str) -> Self {
        Self::UnknownOutputTransport {
            transport_name: transport_name.to_owned(),
        }
    }

    pub fn unknown_input_stream(stream_name: &str) -> Self {
        Self::UnknownInputStream {
            stream_name: stream_name.to_owned(),
        }
    }

    pub fn unknown_output_stream(stream_name: &str) -> Self {
        Self::UnknownOutputStream {
            stream_name: stream_name.to_owned(),
        }
    }
}

/// Controller error.
#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum ControllerError {
    /// Invalid controller configuration.
    Config { config_error: ConfigError },

    /// Parser error.
    ///
    /// Error parsing the last input batch.  Parser errors are expected to be
    /// recoverable, i.e., the parser should be able to successfully parse
    /// new valid inputs after an error.
    ParseError {
        endpoint_name: String,
        error: String,
    },

    /// Encode error.
    ///
    /// Error encoding the last output batch.  Encoder errors are expected to
    /// be recoverable, i.e., the encoder should be able to successfully parse
    /// new valid inputs after an error.
    EncodeError {
        endpoint_name: String,
        #[serde(skip)]
        error: AnyError,
    },

    /// Input transport endpoint error.
    InputTransportError {
        endpoint_name: String,
        fatal: bool,
        #[serde(skip)]
        error: AnyError,
    },

    /// Output transport endpoint error.
    OutputTransportError {
        endpoint_name: String,
        fatal: bool,
        #[serde(skip)]
        error: AnyError,
    },

    /// Operation failed to complete because the pipeline is shutting down.
    PipelineTerminating,

    /// Error evaluating the DBSP circuit.
    DbspError { error: DBSPError },
}

impl DetailedError for ControllerError {
    // TODO: attempts to cast `AnyError` to `DetailedError`.
    fn error_code(&self) -> Cow<'static, str> {
        match self {
            Self::Config { config_error } => {
                Cow::from(format!("ConfigError.{}", config_error.error_code()))
            }
            Self::ParseError { .. } => Cow::from("ParseError"),
            Self::EncodeError { .. } => Cow::from("EncodeError"),
            Self::InputTransportError { .. } => Cow::from("InputTransportError"),
            Self::OutputTransportError { .. } => Cow::from("OutputTransportError"),
            Self::DbspError { error } => error.error_code(),
            Self::PipelineTerminating => Cow::from("PipelineTerminating"),
        }
    }
}

impl StdError for ControllerError {}

impl Display for ControllerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), FmtError> {
        match self {
            Self::Config { config_error } => {
                write!(f, "invalid controller configuration: '{config_error}'")
            }
            Self::InputTransportError {
                endpoint_name,
                fatal,
                error,
            } => {
                write!(
                    f,
                    "{}error on input endpoint '{endpoint_name}': '{error}'",
                    if *fatal { "FATAL " } else { "" }
                )
            }
            Self::OutputTransportError {
                endpoint_name,
                fatal,
                error,
            } => {
                write!(
                    f,
                    "{}error on output endpoint '{endpoint_name}': '{error}'",
                    if *fatal { "FATAL " } else { "" }
                )
            }
            Self::ParseError {
                endpoint_name,
                error,
            } => {
                write!(
                    f,
                    "parse error on input endpoint '{endpoint_name}': '{error}'"
                )
            }
            Self::EncodeError {
                endpoint_name,
                error,
            } => {
                write!(
                    f,
                    "encoder error on output endpoint '{endpoint_name}': '{error}'"
                )
            }
            Self::DbspError { error } => {
                write!(f, "DBSP error: '{error}'")
            }
            Self::PipelineTerminating => {
                f.write_str("Operation failed to complete because the pipeline is shutting down")
            }
        }
    }
}

impl ControllerError {
    pub fn duplicate_input_endpoint(endpoint_name: &str) -> Self {
        Self::Config {
            config_error: ConfigError::duplicate_input_endpoint(endpoint_name),
        }
    }

    pub fn unknown_input_format(format_name: &str) -> Self {
        Self::Config {
            config_error: ConfigError::unknown_input_format(format_name),
        }
    }

    pub fn unknown_input_transport(transport_name: &str) -> Self {
        Self::Config {
            config_error: ConfigError::unknown_input_transport(transport_name),
        }
    }

    pub fn duplicate_output_endpoint(endpoint_name: &str) -> Self {
        Self::Config {
            config_error: ConfigError::duplicate_output_endpoint(endpoint_name),
        }
    }

    pub fn unknown_output_format(format_name: &str) -> Self {
        Self::Config {
            config_error: ConfigError::unknown_output_format(format_name),
        }
    }

    pub fn unknown_output_transport(transport_name: &str) -> Self {
        Self::Config {
            config_error: ConfigError::unknown_output_transport(transport_name),
        }
    }

    pub fn unknown_input_stream(stream_name: &str) -> Self {
        Self::Config {
            config_error: ConfigError::unknown_input_stream(stream_name),
        }
    }

    pub fn unknown_output_stream(stream_name: &str) -> Self {
        Self::Config {
            config_error: ConfigError::unknown_output_stream(stream_name),
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

    pub fn parse_error<E>(endpoint_name: &str, error: &E) -> Self
    where
        E: ToString,
    {
        Self::ParseError {
            endpoint_name: endpoint_name.to_owned(),
            error: error.to_string(),
        }
    }

    pub fn encode_error(endpoint_name: &str, error: AnyError) -> Self {
        Self::EncodeError {
            endpoint_name: endpoint_name.to_owned(),
            error,
        }
    }

    pub fn pipeline_terminating() -> Self {
        Self::PipelineTerminating
    }

    pub fn dbsp_error(error: DBSPError) -> Self {
        Self::DbspError { error }
    }
}
