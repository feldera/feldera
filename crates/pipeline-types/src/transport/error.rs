use thiserror::Error as ThisError;

#[derive(ThisError, Debug)]
#[error("Expected {expected} service names but instead got {actual}")]
pub struct TransportReplaceError {
    pub expected: usize,
    pub actual: usize,
}

#[derive(ThisError, Debug)]
pub enum TransportResolveError {
    #[error("Expected {expected} services in the mapping but instead got {actual}")]
    IncorrectNumServices { expected: usize, actual: usize },
    #[error("Unexpected service type: expected {expected} but instead got {actual}")]
    UnexpectedServiceType { expected: String, actual: String },
    #[error("Invalid configuration: {reason}")]
    InvalidConfig { reason: String },
}
