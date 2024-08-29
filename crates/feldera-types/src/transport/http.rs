use serde::Deserialize;
use serde_json::Value as JsonValue;
use std::fmt::Display;
use utoipa::ToSchema;

/// A set of updates to a SQL table or view.
///
/// The `sequence_number` field stores the offset of the chunk relative to the
/// start of the stream and can be used to implement reliable delivery.
/// The payload is stored in the `bin_data`, `text_data`, or `json_data` field
/// depending on the data format used.
#[derive(Deserialize, ToSchema)]
pub struct Chunk {
    pub sequence_number: u64,

    // Exactly one of the following fields must be set.
    // This should be an enum inlined with `#[serde(flatten)]`, but `utoipa`
    // struggles to generate a schema for that.
    /// Base64 encoded binary payload, e.g., bincode.
    pub bin_data: Option<Vec<u8>>,

    /// Text payload, e.g., CSV.
    pub text_data: Option<String>,

    /// JSON payload.
    #[schema(value_type = Option<Object>)]
    pub json_data: Option<JsonValue>,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, ToSchema)]
pub enum EgressMode {
    /// Continuously monitor the output of the query.
    ///
    /// For queries that support snapshots, e.g.,
    /// `OutputQuery::Neighborhood` queries,
    /// the endpoint outputs the initial snapshot followed
    /// by a stream of deltas.  For queries that don't support
    /// snapshots, the endpoint outputs the stream of deltas
    /// relative to the current output of the query.
    #[serde(rename = "watch")]
    Watch,
    /// Output a single snapshot of query results.
    ///
    /// Currently only supported for `OutputQuery::Quantiles`
    /// and `OutputQuery::Neighborhood` queries.
    #[serde(rename = "snapshot")]
    Snapshot,
}

impl Default for EgressMode {
    /// If `mode` is not specified, default to `Watch`.
    fn default() -> Self {
        Self::Watch
    }
}

impl Display for EgressMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EgressMode::Watch => write!(f, "watch"),
            EgressMode::Snapshot => write!(f, "snapshot"),
        }
    }
}

// This file indicates the port used by the server
pub const SERVER_PORT_FILE: &str = "port";
