use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Display, Formatter, Result};
use utoipa::ToSchema;

/// URL-encoded `format` argument to the `/query` endpoint.
#[derive(Debug, Deserialize, PartialEq, Clone, Copy, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum AdHocQueryFormat {
    /// Print results as a human-readable text table.
    Text,
    /// Print results as new-line delimited JSON records.
    Json,
    /// Downloads results in a parquet file.
    Parquet,
}

impl Display for AdHocQueryFormat {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            AdHocQueryFormat::Text => write!(f, "text"),
            AdHocQueryFormat::Json => write!(f, "json"),
            AdHocQueryFormat::Parquet => write!(f, "parquet"),
        }
    }
}

impl Default for AdHocQueryFormat {
    fn default() -> Self {
        Self::Text
    }
}

fn default_format() -> AdHocQueryFormat {
    AdHocQueryFormat::default()
}

/// URL-encoded arguments to the `/query` endpoint.
#[derive(Clone, Debug, PartialEq, Deserialize, ToSchema)]
pub struct AdhocQueryArgs {
    /// The SQL query to run.
    pub sql: String,
    /// In what format the data is sent to the client.
    #[serde(default = "default_format")]
    pub format: AdHocQueryFormat,
}

/// A query over an output stream.
///
/// We currently do not support ad hoc queries.  Instead the client can use
/// three pre-defined queries to inspect the contents of a table or view.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, PartialOrd, ToSchema, Ord)]
pub enum OutputQuery {
    /// Query the entire contents of the table (similar to `SELECT * FROM`).
    #[serde(rename = "table")]
    Table,
    /// Neighborhood query (see `Stream::neighborhood` in dbsp)
    #[serde(rename = "neighborhood")]
    Neighborhood,
    /// Quantiles query (see `Stream::stream_key_quantiles` in dbsp).
    #[serde(rename = "quantiles")]
    Quantiles,
}

impl Default for OutputQuery {
    fn default() -> Self {
        Self::Table
    }
}

impl Display for OutputQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OutputQuery::Table => write!(f, "table"),
            OutputQuery::Neighborhood => write!(f, "neighborhood"),
            OutputQuery::Quantiles => write!(f, "quantiles"),
        }
    }
}

// This is only here so we can derive `ToSchema` for it without adding
// a `utoipa` dependency to the `dbsp` crate to derive ToSchema for
// `NeighborhoodDescr`.
/// A request to output a specific neighborhood of a table or view.
/// The neighborhood is defined in terms of its central point (`anchor`)
/// and the number of rows preceding and following the anchor to output.
#[derive(Clone, Serialize, Deserialize, ToSchema)]
pub struct NeighborhoodQuery {
    pub anchor: Option<utoipa::openapi::Object>,
    pub before: u32,
    pub after: u32,
}

impl Debug for NeighborhoodQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NeighborhoodQuery")
    }
}
