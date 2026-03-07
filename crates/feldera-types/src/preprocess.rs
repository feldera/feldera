//! Configuration describing a preprocessor

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Configuration for describing a preprocessor
#[derive(Debug, Deserialize, Clone, Eq, PartialEq, Serialize)]
pub struct PreprocessorConfig {
    /// Name of the preprocessor.
    /// All preprocessors with the same name will perform the same task.
    pub name: String,
    /// True if the preprocessor is message-oriented: true if each preprocessor
    /// output record corresponds to a whole number of of parser records.
    pub message_oriented: bool,
    /// Arbitrary additional configuration.
    pub config: Value,
}
