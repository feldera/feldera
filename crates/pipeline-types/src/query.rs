use serde::Deserialize;
use utoipa::ToSchema;

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

// This is only here so we can derive `ToSchema` for it without adding
// a `utoipa` dependency to the `dbsp` crate to derive ToSchema for
// `NeighborhoodDescr`.
/// A request to output a specific neighborhood of a table or view.
/// The neighborhood is defined in terms of its central point (`anchor`)
/// and the number of rows preceding and following the anchor to output.
#[derive(Deserialize, ToSchema)]
pub struct NeighborhoodQuery {
    pub anchor: Option<utoipa::openapi::Object>,
    pub before: u32,
    pub after: u32,
}
