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

pub const fn default_publish_timeout_secs() -> u64 {
    5
}

/// Default max message payload when the bound stream's `max_message_size` is
/// unset (the server reports -1) and no override is configured: the nats-server
/// default.
pub const DEFAULT_MAX_MESSAGE_SIZE_BYTES: usize = 1024 * 1024;

/// Configuration for writing data to a NATS subject with `nats_output`.
///
/// The connector publishes to JetStream: each message is published with an
/// acknowledgment request and the publish is retried until the server
/// acknowledges it or the pipeline shuts down, so data is not lost to a
/// server that is momentarily unavailable. The subject must be bound to a
/// JetStream stream; publishing to a subject with no stream raises
/// "no responders" and the publish fails.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct NatsOutputConfig {
    /// Options for connecting to the NATS server.
    pub connection_config: ConnectOptions,

    /// NATS subject to publish to (e.g., "orders.created"). The subject
    /// must be bound to a JetStream stream on the target server.
    pub subject: String,

    /// Headers to add to every message published by this connector, as
    /// key/value pairs. Values are UTF-8 strings.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub headers: Vec<NatsHeader>,

    /// How the `Nats-Msg-Id` header of each message is chosen. Within a
    /// stream's duplicate window (the stream's `duplicate_window`), the
    /// server discards a publish whose id it has already stored, which makes
    /// output replayed after a restart idempotent. Default: `content_hash`.
    #[serde(default, skip_serializing_if = "is_default")]
    pub message_id: NatsMessageId,

    /// How long to wait for the server's publish acknowledgment before
    /// retrying the publish. Must be at least 1.
    #[serde(default = "default_publish_timeout_secs")]
    pub publish_timeout_secs: u64,

    /// Override for the maximum message size in bytes the connector offers to
    /// the encoder. When unset, the connector discovers the limit at connect
    /// time: the bound stream's `max_message_size` when the stream sets one,
    /// else the server's max payload. The encoder splits output records across
    /// messages by this limit; a record larger than the limit fails the
    /// pipeline with an error naming the record rather than reaching the
    /// server.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_message_size: Option<usize>,
}

/// A header attached to every message published by `nats_output`.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct NatsHeader {
    pub key: String,
    pub value: String,
}

/// How `nats_output` chooses the `Nats-Msg-Id` header of each message.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Deserialize, Serialize, ToSchema, Default)]
#[serde(rename_all = "snake_case")]
pub enum NatsMessageId {
    /// The id is derived from the message itself: the number of the
    /// transaction that produced the output, a hash of the encoded payload,
    /// and the count of earlier messages in the same transaction with the
    /// same payload. The id is independent of the data format and is
    /// reproduced when the pipeline replays the same output after a restart,
    /// so the server stores each message once within the stream's duplicate
    /// window. Two messages are only deduplicated when their encoded bytes
    /// are identical, so exact replay deduplication requires the format to
    /// emit one record per message (`buffer_size_records: 1` for `json`).
    #[default]
    ContentHash,
    /// No `Nats-Msg-Id` header: output replayed after a restart is stored
    /// again, and every consumer of the stream must tolerate duplicates.
    None,
}
