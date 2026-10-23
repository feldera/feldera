//! Configuration describing a preprocessor

use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;

use crate::{
    duration::{ConfigDuration, LegacyUnit},
    nullable_duration_setting,
};

/// Default for [PreprocessorConfig::max_allowed_call_duration].
pub const DEFAULT_PREPROCESSOR_MAX_ALLOWED_CALL_DURATION: ConfigDuration =
    ConfigDuration::from_secs(1);

fn default_max_allowed_call_duration() -> String {
    DEFAULT_PREPROCESSOR_MAX_ALLOWED_CALL_DURATION.to_string()
}

nullable_duration_setting!(
    duration_max_allowed_call_duration,
    "max_allowed_call_duration",
    LegacyUnit::Secs
);

/// Configuration for describing a preprocessor
#[derive(Debug, Deserialize, Clone, Eq, PartialEq, Serialize, ToSchema)]
pub struct PreprocessorConfig {
    /// Name of the preprocessor.
    /// All preprocessors with the same name will perform the same task.
    pub name: String,
    /// True if the preprocessor is message-oriented: true if each preprocessor
    /// output record corresponds to a whole number of of parser records.
    pub message_oriented: bool,
    /// Arbitrary additional configuration.
    pub config: Value,
    /// Maximum allowed duration of one call to the preprocessor, for example
    /// `10s`.  While a call runs longer, the connector is unhealthy and the
    /// pipeline logs warnings.
    ///
    /// When not set it defaults to `1s`; `null` disables the check.  Values
    /// less than 1 second are raised to 1 second.
    #[serde(
        default,
        deserialize_with = "duration_max_allowed_call_duration",
        skip_serializing_if = "Option::is_none"
    )]
    #[schema(value_type = Option<String>, default = default_max_allowed_call_duration)]
    pub max_allowed_call_duration: Option<Option<ConfigDuration>>,
}

impl PreprocessorConfig {
    /// The maximum allowed call duration in effect, or `None` if the check is
    /// disabled.
    pub fn max_allowed_call_duration(&self) -> Option<Duration> {
        let limit = self
            .max_allowed_call_duration
            .unwrap_or(Some(DEFAULT_PREPROCESSOR_MAX_ALLOWED_CALL_DURATION))?;
        Some(Duration::from(limit).max(Duration::from_secs(1)))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde_json::json;

    use super::PreprocessorConfig;

    /// The limit of a configuration whose `max_allowed_call_duration` key is
    /// `field`, or that has no such key if `field` is `None`.
    fn limit(field: Option<serde_json::Value>) -> Option<Duration> {
        let mut config = json!({"name": "p", "message_oriented": true, "config": {}});
        if let Some(field) = field {
            config["max_allowed_call_duration"] = field;
        }
        serde_json::from_value::<PreprocessorConfig>(config)
            .unwrap()
            .max_allowed_call_duration()
    }

    #[test]
    fn max_allowed_call_duration() {
        assert_eq!(limit(None), Some(Duration::from_secs(1)));
        assert_eq!(limit(Some(json!("10s"))), Some(Duration::from_secs(10)));
        assert_eq!(limit(Some(json!("0s"))), Some(Duration::from_secs(1)));
        assert_eq!(limit(Some(json!("100ms"))), Some(Duration::from_secs(1)));
        assert_eq!(limit(Some(json!(null))), None);
    }
}
