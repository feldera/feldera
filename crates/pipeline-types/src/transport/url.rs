use serde::Deserialize;
use utoipa::ToSchema;

/// Configuration for reading data from an HTTP or HTTPS URL with
/// `UrlInputTransport`.
#[derive(Clone, Deserialize, ToSchema)]
pub struct UrlInputConfig {
    /// URL.
    pub path: String,
}
