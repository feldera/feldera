use anyhow::Error as AnyError;
use dbsp::Error as DBSPError;
use std::{
    error::Error as StdError,
    fmt::{Display, Error as FmtError, Formatter},
};

/// Controller configuration error.
#[derive(Debug)]
pub enum ConfigError {
    /// Input endpoint with this name already exists.
    DuplicateInputEndpoint { endpoint_name: String },

    /// Output endpoint with this name already exists.
    DuplicateOutputEndpoint { endpoint_name: String },

    /// An output stream cannot be connected to multiple output endpoints.
    DuplicateOutputStreamConsumer {
        stream_name: String,
        endpoint_name1: String,
        endpoint_name2: String,
    },

    /// Endpoint configuration specifies unknown input format name.
    UnknownInputFormat { format_name: String },

    /// Endpoint configuration specifies unknown output format name.
    UnknownOutputFormat { format_name: String },

    /// Endpoint configuration specifies unknown input transport name.
    UnknownInputTransport { transport_name: String },

    /// Endpoint configuration specifies unknown output transport name.
    UnknownOutputTransport { transport_name: String },

    /// Controller configuration specifies output stream name
    /// that is not found in the circuit catalog.
    UnknownOutputStream { stream_name: String },
}

impl Display for ConfigError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), FmtError> {
        match self {
            Self::DuplicateInputEndpoint { endpoint_name } => {
                write!(f, "input endpoint '{endpoint_name}' already exists")
            }
            Self::UnknownInputFormat { format_name } => {
                write!(f, "unknown input format '{format_name}'")
            }
            Self::UnknownInputTransport { transport_name } => {
                write!(f, "unknown input transport '{transport_name}'")
            }
            Self::DuplicateOutputEndpoint { endpoint_name } => {
                write!(f, "output endpoint '{endpoint_name}' already exists")
            }
            Self::DuplicateOutputStreamConsumer {
                stream_name,
                endpoint_name1,
                endpoint_name2,
            } => {
                write!(f, "output stream '{stream_name}' is connected to multiple output endpoints: '{endpoint_name1}' and '{endpoint_name2}'")
            }
            Self::UnknownOutputFormat { format_name } => {
                write!(f, "unknown output format '{format_name}'")
            }
            Self::UnknownOutputTransport { transport_name } => {
                write!(f, "unknown output transport '{transport_name}'")
            }
            Self::UnknownOutputStream { stream_name } => {
                write!(f, "unknown output stream '{stream_name}'")
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

    pub fn duplicate_output_stream_consumer(
        stream_name: &str,
        endpoint_name1: &str,
        endpoint_name2: &str,
    ) -> Self {
        Self::DuplicateOutputStreamConsumer {
            stream_name: stream_name.to_owned(),
            endpoint_name1: endpoint_name1.to_owned(),
            endpoint_name2: endpoint_name2.to_owned(),
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

    pub fn unknown_output_stream(stream_name: &str) -> Self {
        Self::UnknownOutputStream {
            stream_name: stream_name.to_owned(),
        }
    }
}

/// Controller error.
#[derive(Debug)]
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
        error: AnyError,
    },

    /// Encoder error.
    ///
    /// Error encoding the last output batch.  Encoder errors are expected to
    /// be recoverable, i.e., the encoder should be able to successfully parse
    /// new valid inputs after an error.
    EncoderError {
        endpoint_name: String,
        error: AnyError,
    },

    /// Input transport endpoint error.
    InputTransportError {
        endpoint_name: String,
        fatal: bool,
        error: AnyError,
    },

    /// Output transport endpoint error.
    OutputTransportError {
        endpoint_name: String,
        fatal: bool,
        error: AnyError,
    },

    /// Error evaluating the DBSP circuit.
    DbspError { error: DBSPError },
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
            Self::EncoderError {
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

    pub fn duplicate_output_stream_consumer(
        stream_name: &str,
        endpoint_name1: &str,
        endpoint_name2: &str,
    ) -> Self {
        Self::Config {
            config_error: ConfigError::duplicate_output_stream_consumer(
                stream_name,
                endpoint_name1,
                endpoint_name2,
            ),
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

    pub fn parse_error(endpoint_name: &str, error: AnyError) -> Self {
        Self::ParseError {
            endpoint_name: endpoint_name.to_owned(),
            error,
        }
    }

    pub fn encoder_error(endpoint_name: &str, error: AnyError) -> Self {
        Self::EncoderError {
            endpoint_name: endpoint_name.to_owned(),
            error,
        }
    }

    pub fn dbsp_error(error: DBSPError) -> Self {
        Self::DbspError { error }
    }
}
