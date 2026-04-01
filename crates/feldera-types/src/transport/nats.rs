use std::path::PathBuf;
use time::OffsetDateTime;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use utoipa::ToSchema;

fn is_default<T: Default + Eq>(t: &T) -> bool {
    t == &T::default()
}

// TODO How does the user choose? Think about what "UI" you would prefer.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub enum Credentials {
    FromString(String),
    #[schema(value_type = String, example = "/path/to/credentials.json")]
    FromFile(PathBuf),
}

#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct UserAndPassword {
    pub user: String,
    pub password: String,
}

#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema, Default)]
pub struct Auth {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credentials: Option<Credentials>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jwt: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nkey: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_and_password: Option<UserAndPassword>,
}

pub const fn default_connection_timeout_secs() -> u64 {
    10
}

pub const fn default_request_timeout_secs() -> u64 {
    10
}

pub const fn default_inactivity_timeout_secs() -> u64 {
    60
}

pub const fn default_retry_interval_secs() -> u64 {
    5
}

/// Options for connecting to a NATS server.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct ConnectOptions {
    /// NATS server URL (e.g., "nats://localhost:4222").
    pub server_url: String,

    /// Authentication configuration.
    #[serde(default, skip_serializing_if = "is_default")]
    pub auth: Auth,

    /// Connection timeout
    ///
    /// How long to wait when establishing the initial connection to the
    /// NATS server.
    #[serde(default = "default_connection_timeout_secs")]
    pub connection_timeout_secs: u64,

    /// Request timeout in seconds.
    ///
    /// How long to wait for responses to requests.
    #[serde(default = "default_request_timeout_secs")]
    pub request_timeout_secs: u64,
}

#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema, Default)]
pub enum ReplayPolicy {
    #[default]
    Instant,
    Original,
}

#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub enum DeliverPolicy {
    All,
    Last,
    New,
    ByStartSequence {
        start_sequence: u64,
    },
    ByStartTime {
        #[schema(value_type = String, format = "date-time", example = "2023-01-15T09:30:00Z")]
        start_time: OffsetDateTime,
    },
    LastPerSubject,
}

#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct ConsumerConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "is_default")]
    pub filter_subjects: Vec<String>,
    #[serde(default, skip_serializing_if = "is_default")]
    pub replay_policy: ReplayPolicy,
    #[serde(default, skip_serializing_if = "is_default")]
    pub rate_limit: u64,
    pub deliver_policy: DeliverPolicy,
    #[serde(default, skip_serializing_if = "is_default")]
    pub max_waiting: i64,
    #[serde(default, skip_serializing_if = "is_default")]
    pub metadata: HashMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_batch: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_expires: Option<std::time::Duration>,
}

#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct NatsInputConfig {
    pub connection_config: ConnectOptions,
    pub stream_name: String,
    /// Maximum time in seconds to wait for the next message before running
    /// a stream/server health check. Must be at least 1.
    #[serde(default = "default_inactivity_timeout_secs")]
    pub inactivity_timeout_secs: u64,
    /// Delay between automatic reconnect attempts while in retry mode.
    /// Must be at least 1.
    #[serde(default = "default_retry_interval_secs")]
    pub retry_interval_secs: u64,
    pub consumer_config: ConsumerConfig,
}
