use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{
    duration::{Duration, LegacyUnit},
    duration_setting,
};

/// Default timeout before a paused input adapter drops its connection.
pub const DEFAULT_PAUSE_TIMEOUT: Duration = Duration::from_secs(60);

duration_setting!(duration_pause_linger, "pause_linger", LegacyUnit::Secs);

/// Configuration for reading data from an HTTP or HTTPS URL with
/// `UrlInputTransport`.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct UrlInputConfig {
    /// URL.
    pub path: String,

    /// Timeout before disconnection when paused, for example `60s`.
    ///
    /// If the pipeline is paused, or if the input adapter reads data faster
    /// than the pipeline can process it, then the controller will pause the
    /// input adapter. If the input adapter stays paused longer than this
    /// timeout, it will drop the network connection to the server. It will
    /// automatically reconnect when the input adapter starts running again.
    ///
    /// The default is 60 seconds.
    #[serde(
        default,
        alias = "pause_timeout",
        deserialize_with = "duration_pause_linger",
        skip_serializing_if = "Option::is_none"
    )]
    pub pause_linger: Option<Duration>,
}

impl UrlInputConfig {
    /// The timeout in effect, or [`DEFAULT_PAUSE_TIMEOUT`].
    pub fn pause_linger(&self) -> Duration {
        self.pause_linger.unwrap_or(DEFAULT_PAUSE_TIMEOUT)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(json: &str) -> UrlInputConfig {
        serde_json::from_str(json).unwrap()
    }

    /// The pause timeout reads back from the current spelling and from the one
    /// it deprecates.
    #[test]
    fn pause_linger_accepts_both_spellings() {
        for (json, expected) in [
            (r#"{"path": "http://x"}"#, DEFAULT_PAUSE_TIMEOUT),
            (
                r#"{"path": "http://x", "pause_linger": "5m"}"#,
                Duration::from_secs(300),
            ),
            (
                r#"{"path": "http://x", "pause_timeout": 10}"#,
                Duration::from_secs(10),
            ),
        ] {
            assert_eq!(parse(json).pause_linger(), expected, "parsing {json}");
        }
    }

    #[test]
    fn unset_fields_do_not_serialize() {
        let json = serde_json::to_string(&parse(r#"{"path": "http://x"}"#)).unwrap();
        assert_eq!(json, r#"{"path":"http://x"}"#);
    }
}
