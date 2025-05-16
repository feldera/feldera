use std::fmt::Display;

use serde::{Deserialize, Serialize};
use thiserror::Error as ThisError;
use utoipa::ToSchema;

/// Whether a pipeline supports checkpointing and suspend-and-resume.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub enum SuspendError {
    /// Pipeline does not support suspend-and-resume.
    ///
    /// These reasons only change if the pipeline's configuration changes, e.g.
    /// if a pipeline has an input connector that does not support
    /// suspend-and-resume, and then that input connector is removed.
    Permanent(
        /// Reasons why the pipeline does not support suspend-and-resume.
        Vec<PermanentSuspendError>,
    ),

    /// Pipeline supports suspend-and-resume, but a suspend requested now will
    /// be delayed.
    Temporary(
        /// Reasons that the suspend will be delayed.
        Vec<TemporarySuspendError>,
    ),
}

impl Display for SuspendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SuspendError::Permanent(reasons) => {
                write!(
                    f,
                    "The pipeline does not support checkpointing for the following reasons:"
                )?;
                for (index, reason) in reasons.iter().enumerate() {
                    if index > 0 {
                        write!(f, ",")?;
                    }
                    write!(f, " {reason}")?;
                }
            }
            SuspendError::Temporary(delays) => {
                write!(
                    f,
                    "Checkpointing the pipeline will be temporarily delayed for the following reasons:"
                )?;
                for (index, delay) in delays.iter().enumerate() {
                    if index > 0 {
                        write!(f, ",")?;
                    }
                    write!(f, " {delay}")?;
                }
            }
        }
        Ok(())
    }
}

/// Reasons why a pipeline does not support suspend and resume operations.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ThisError, ToSchema)]
pub enum PermanentSuspendError {
    #[error("Storage must be configured")]
    StorageRequired,

    #[error("Suspend is an enterprise feature")]
    EnterpriseFeature,

    #[error("Input endpoint {0:?} does not support suspend")]
    UnsupportedInputEndpoint(String),
}

/// Reasons why a pipeline cannot be suspended at this time.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ThisError, ToSchema)]
pub enum TemporarySuspendError {
    #[error("The pipeline is replaying the journal")]
    Replaying,

    #[error("The pipeline is bootstrapping")]
    Bootstrapping,

    #[error("Input endpoint {0:?} is blocking suspend")]
    InputEndpointBarrier(String),
}

/// Response to a `/suspendable` request.
///
/// Reports whether the pipeline supports suspend and resume operations.
/// If not, provides the reasons why suspending is not supported.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct SuspendableResponse {
    /// Is the pipeline suspendable?
    pub suspendable: bool,

    /// If the pipeline is not suspendable, why not?
    pub reasons: Vec<PermanentSuspendError>,
}

impl SuspendableResponse {
    /// Create a new suspendable response.
    pub fn new(suspendable: bool, reasons: Vec<PermanentSuspendError>) -> Self {
        Self {
            suspendable,
            reasons,
        }
    }
}
