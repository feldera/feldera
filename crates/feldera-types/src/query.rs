use serde::Deserialize;
use std::fmt::{Debug, Display, Formatter, Result};
use utoipa::ToSchema;

/// URL-encoded `format` argument to the `/query` endpoint.
#[derive(Debug, Deserialize, PartialEq, Clone, Copy, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum AdHocResultFormat {
    /// Serialize results as a human-readable text table.
    Text,
    /// Serialize results as new-line delimited JSON records.
    Json,
    /// Download results in a parquet file.
    Parquet,
    /// Stream data in the arrow IPC format.
    ArrowIpc,
}

impl Display for AdHocResultFormat {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            AdHocResultFormat::Text => write!(f, "text"),
            AdHocResultFormat::Json => write!(f, "json"),
            AdHocResultFormat::Parquet => write!(f, "parquet"),
            AdHocResultFormat::ArrowIpc => write!(f, "arrow_ipc"),
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

/// URL-encoded arguments to the `/query` endpoint.
#[derive(Clone, Debug, PartialEq, Deserialize, ToSchema)]
pub struct AdhocQueryArgs {
    /// The SQL query to run.
    pub sql: String,
    /// In what format the data is sent to the client.
    #[serde(default = "default_format")]
    pub format: AdHocResultFormat,
}
