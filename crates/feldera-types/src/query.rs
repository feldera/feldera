use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Display, Formatter, Result};
use utoipa::ToSchema;

/// The maximum size of a WebSocket frames we're sending in bytes.
pub const MAX_WS_FRAME_SIZE: usize = 1024 * 1024 * 2;

/// URL-encoded `format` argument to the `/query` endpoint.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Copy, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum AdHocResultFormat {
    /// Serialize results as a human-readable text table.
    Text,
    /// Serialize results as new-line delimited JSON records.
    ///
    /// # Deprecation Notice
    /// This format is deprecated and will be removed in the future.
    /// Users are encouraged to use the `arrow_ipc` format instead,
    /// See <https://github.com/feldera/feldera/issues/4219> for more details.
    Json,
    /// Download results in a parquet file.
    Parquet,
    /// Stream data in the arrow IPC format.
    ArrowIpc,
    /// Returns a hash of the results instead of the actual data.
    ///
    /// The output in this case is a single string/line containing a
    /// SHA256 hash (or a JSON formatted error message in case the query
    /// failed to execute).
    ///
    /// This is useful for verifying the integrity of the data
    /// without transferring the entire dataset.
    ///
    /// Note that supplying a query with this format will implicitly
    /// add an `ORDER BY` clause for all fields of the result to the query
    /// to ensure consistent ordering of results.
    ///
    /// e.g., a query like `select * from materialized_view` will be rewritten as
    /// `select * from materialized_view order by col1, col2, ..., colN`
    Hash,
}

impl Display for AdHocResultFormat {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            AdHocResultFormat::Text => write!(f, "text"),
            AdHocResultFormat::Json => write!(f, "json"),
            AdHocResultFormat::Parquet => write!(f, "parquet"),
            AdHocResultFormat::ArrowIpc => write!(f, "arrow_ipc"),
            AdHocResultFormat::Hash => write!(f, "hash"),
        }
    }
}

impl Default for AdHocResultFormat {
    fn default() -> Self {
        Self::Text
    }
}

fn default_format() -> AdHocResultFormat {
    AdHocResultFormat::default()
}

/// Arguments to the `/query` endpoint.
///
/// The arguments can be provided in two ways:
///
/// - In case a normal HTTP connection is established to the endpoint,
///   these arguments are passed as URL-encoded parameters.
///   Note: this mode is deprecated and will be removed in the future.
///
/// - If a Websocket connection is opened to `/query`, the arguments are passed
///   to the server over the websocket as a JSON encoded string.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct AdhocQueryArgs {
    /// The SQL query to run.
    #[serde(default)]
    pub sql: String,
    /// In what format the data is sent to the client.
    #[serde(default = "default_format")]
    pub format: AdHocResultFormat,
}
