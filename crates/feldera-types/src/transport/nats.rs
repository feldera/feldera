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
    /// Credentials in the NATS `.creds` format (user JWT + NKey seed).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credentials: Option<Credentials>,
    /// User JWT for decentralized (operator-mode) authentication.
    ///
    /// Requires `nkey` to be set as well: the connection nonce is signed
    /// with the NKey seed. Equivalent to `credentials`, for deployments
    /// that store the JWT and seed separately (e.g. as two secrets)
    /// rather than as one `.creds` file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jwt: Option<String>,
    /// NKey seed (`SU...`) for NKey challenge-response authentication.
    ///
    /// On its own, authenticates as a bare NKey user (a `nkey:` user in
    /// the server configuration). Combined with `jwt`, signs the
    /// connection nonce for decentralized authentication.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nkey: Option<String>,
    /// Token for token-based authentication.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    /// Username and password for password-based authentication.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_and_password: Option<UserAndPassword>,
}

/// TLS options for connecting to a NATS server.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema, Default)]
pub struct Tls {
    /// Require an encrypted connection; refuse to connect to servers that
    /// do not offer TLS.
    #[serde(default, skip_serializing_if = "is_default")]
    pub require_tls: bool,
    /// Path to a PEM file with additional root certificates to trust when
    /// verifying the server certificate, for servers whose certificates
    /// are not signed by a public CA.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(value_type = String, example = "/path/to/ca.crt")]
    pub root_certificates_file: Option<PathBuf>,
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

    /// TLS configuration.
    #[serde(default, skip_serializing_if = "is_default")]
    pub tls: Tls,

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
