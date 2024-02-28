use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Configuration for reading data from an HTTP or HTTPS URL with
/// `UrlInputTransport`.
#[derive(Serialize, Clone, Deserialize, ToSchema)]
pub struct UrlInputConfig {
    /// URL.
    pub path: String,
}
