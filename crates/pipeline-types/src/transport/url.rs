use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Configuration for reading data from an HTTP or HTTPS URL with
/// `UrlInputTransport`.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct UrlInputConfig {
    /// URL.
    pub path: String,
}
