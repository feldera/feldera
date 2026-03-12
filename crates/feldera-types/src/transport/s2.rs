use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Where to start reading from the S2 stream.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub enum S2StartFrom {
    /// Start from a specific sequence number.
    SeqNum(u64),
    /// Start from a specific timestamp (milliseconds since epoch).
    Timestamp(u64),
    /// Start from N records before the tail.
    TailOffset(u64),
    /// Start from the beginning (sequence number 0).
    Beginning,
    /// Start from the tail (new records only).
    Tail,
}

impl Default for S2StartFrom {
    fn default() -> Self {
        Self::Beginning
    }
}

/// Configuration for reading from an S2 stream.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct S2InputConfig {
    /// S2 basin name.
    pub basin: String,
    /// S2 stream name.
    pub stream: String,
    /// S2 authentication token.
    pub auth_token: String,
    /// Custom S2 endpoint URL (e.g., "http://localhost:8080").
    /// If not set, uses the default S2 cloud endpoint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    /// Where to start reading when no checkpoint exists.
    #[serde(default)]
    pub start_from: S2StartFrom,
}

/// Configuration for writing to an S2 stream.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct S2OutputConfig {
    /// S2 basin name.
    pub basin: String,
    /// S2 stream name.
    pub stream: String,
    /// S2 authentication token.
    pub auth_token: String,
    /// Custom S2 endpoint URL (e.g., "http://localhost:8080").
    /// If not set, uses the default S2 cloud endpoint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
}
