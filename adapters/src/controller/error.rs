use anyhow::Error as AnyError;
use dbsp::Error as DBSPError;
use std::{
    error::Error as StdError,
    fmt::{Display, Error as FmtError, Formatter},
};

/// Controller error.
#[derive(Debug)]
pub enum ControllerError {
    /// Input endpoint with this name already exists.
    DuplicateInputEndpoint { endpoint_name: String },

    /// Endpoint configuration specifies unknown input format name.
    UnknownInputFormat { format_name: String },

    /// Endpoint configuration specifies unknown input transport name.
    UnknownInputTransport { transport_name: String },

    /// Parser error.
    ///
    /// Error parsing the last input batch.  Parser errors are usually
    /// recoverable, i.e., the parser should be able to successfully parse
    /// new valid inputs after an error.
    ParseError {
        endpoint_name: String,
        error: AnyError,
    },

    /// Transport endpoint error.
    ///
    /// Transport errors are usually non-recoverable.  An error indicates
    /// that the endpoint has failed to re-establish lost connection and
    /// is unable to continue receiving data.
    TransportError {
        endpoint_name: String,
        error: AnyError,
    },

    /// Error evaluating the DBSP circuit.
    DbspError { error: DBSPError },
}

impl StdError for ControllerError {}

impl Display for ControllerError {
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
            Self::TransportError {
                endpoint_name,
                error,
            } => {
                write!(
                    f,
                    "transport error on endpoint '{endpoint_name}': '{error}'"
                )
            }
            Self::ParseError {
                endpoint_name,
                error,
            } => {
                write!(f, "parse error on endpoint '{endpoint_name}': '{error}'")
            }
            Self::DbspError { error } => {
                write!(f, "DBSP error: '{error}'")
            }
        }
    }
}

impl ControllerError {
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

    pub fn transport_error(endpoint_name: &str, error: AnyError) -> Self {
        Self::TransportError {
            endpoint_name: endpoint_name.to_owned(),
            error,
        }
    }

    pub fn parse_error(endpoint_name: &str, error: AnyError) -> Self {
        Self::ParseError {
            endpoint_name: endpoint_name.to_owned(),
            error,
        }
    }

    pub fn dbsp_error(error: DBSPError) -> Self {
        Self::DbspError { error }
    }
}
