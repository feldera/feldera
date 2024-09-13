use enum_map::Enum;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Configuration for generating Nexmark input data.
///
/// This connector must be used exactly three times in a pipeline if it is used
/// at all, once for each [`NexmarkTable`].
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct NexmarkInputConfig {
    /// Which table is this?
    ///
    /// Each table must appear in one connector.
    pub table: NexmarkTable,

    /// Overall behavior of the three linked input connectors.
    ///
    /// This may be specified only on one of the connectors and applies to all
    /// them as a whole.
    #[serde(default)]
    pub options: Option<NexmarkInputOptions>,
}

/// Table in Nexmark.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema, Enum)]
#[serde(rename_all = "snake_case")]
pub enum NexmarkTable {
    /// 92% of the events.
    Bid,

    /// 6% of the events.
    Auction,

    /// 2% of the events.
    Person,
}

/// Configuration for generating Nexmark input data.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
#[serde(default)]
pub struct NexmarkInputOptions {
    /// Number of events to generate.
    pub events: u64,

    /// Number of event generator threads.
    ///
    /// It's reasonable to choose the same number of generator threads as worker
    /// threads.
    pub threads: usize,

    /// Number of events to generate and submit together.
    pub batch_size: u64,

    /// Maximum number of events to submit in a single step.  This should be a
    /// multiple of `batch_size`.
    ///
    /// This stands in for `max_batch_size` from the connector configuration
    /// because it must be a constant across all three of the nexmark tables.
    pub max_step_size: u64,
}

impl Default for NexmarkInputOptions {
    fn default() -> Self {
        Self {
            events: 100_000_000,
            threads: 4,
            batch_size: 4_000,
            max_step_size: 4_000_000,
        }
    }
}
