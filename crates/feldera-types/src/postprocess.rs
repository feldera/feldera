//! Configuration describing a postprocessor

use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;

/// Configuration for describing a postprocessor
#[derive(Debug, Deserialize, Clone, Eq, PartialEq, Serialize, ToSchema)]
pub struct PostprocessorConfig {
    /// Name of the postprocessor.
    /// All postprocessors with the same name will perform the same task.
    pub name: String,
    /// Arbitrary additional configuration expected by the postprocessor
    /// encoded as a JSON Value.
    pub config: Value,
}
