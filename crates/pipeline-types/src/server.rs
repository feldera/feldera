use serde::Deserialize;
use utoipa::ToSchema;

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, ToSchema)]
pub enum EgressMode {
    /// Continuously monitor the output of the query.
    ///
    /// For queries that support snapshots, e.g.,
    /// [neighborhood](`OutputQuery::Neighborhood`) queries,
    /// the endpoint outputs the initial snapshot followed
    /// by a stream of deltas.  For queries that don't support
    /// snapshots, the endpoint outputs the stream of deltas
    /// relative to the current output of the query.
    #[serde(rename = "watch")]
    Watch,
    /// Output a single snapshot of query results.
    ///
    /// Currently only supported for [quantile](`OutputQuery::Quantiles`)
    /// and [neighborhood](`OutputQuery::Neighborhood`) queries.
    #[serde(rename = "snapshot")]
    Snapshot,
}

impl Default for EgressMode {
    /// If `mode` is not specified, default to `Watch`.
    fn default() -> Self {
        Self::Watch
    }
}

// This file indicates the port used by the server
pub const SERVER_PORT_FILE: &str = "port";
