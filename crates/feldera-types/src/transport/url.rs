use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Configuration for reading data from an HTTP or HTTPS URL with
/// `UrlInputTransport`.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct UrlInputConfig {
    /// URL.
    pub path: String,

    /// Timeout before disconnection when paused, in seconds.
    ///
    /// If the pipeline is paused, or if the input adapter reads data faster
    /// than the pipeline can process it, then the controller will pause the
    /// input adapter. If the input adapter stays paused longer than this
    /// timeout, it will drop the network connection to the server. It will
    /// automatically reconnect when the input adapter starts running again.
    #[serde(default = "default_pause_timeout")]
    pub pause_timeout: u32,
}

const fn default_pause_timeout() -> u32 {
    60
}
