use std::path::PathBuf;
use time::OffsetDateTime;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use utoipa::ToSchema;

use crate::{
    duration::{ConfigDuration, LegacyUnit},
    duration_setting, duration_struct_setting,
};

duration_setting!(
    duration_connection_timeout,
    "connection_timeout",
    LegacyUnit::Secs
);
duration_setting!(
    duration_request_timeout,
    "request_timeout",
    LegacyUnit::Secs
);
duration_setting!(
    duration_inactivity_timeout,
    "inactivity_timeout",
    LegacyUnit::Secs
);
duration_setting!(duration_retry_interval, "retry_interval", LegacyUnit::Secs);
duration_setting!(
    duration_publish_timeout,
    "publish_timeout",
    LegacyUnit::Secs
);
duration_struct_setting!(duration_max_expiry, "max_expiry");

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

/// Default time to wait for the initial connection to the NATS server.
pub const DEFAULT_CONNECTION_TIMEOUT: ConfigDuration = ConfigDuration::from_secs(10);

/// Default time to wait for a response to a NATS request.
pub const DEFAULT_REQUEST_TIMEOUT: ConfigDuration = ConfigDuration::from_secs(10);

/// Default time to wait for the next message before running a health check.
pub const DEFAULT_INACTIVITY_TIMEOUT: ConfigDuration = ConfigDuration::from_secs(60);

/// Default delay before the first automatic reconnect attempt.
pub const DEFAULT_RETRY_INTERVAL: ConfigDuration = ConfigDuration::from_secs(5);

/// Default upper bound on the delay between reconnect attempts.
pub const DEFAULT_RETRY_MAX_INTERVAL: ConfigDuration = ConfigDuration::from_secs(300);

// The defaults as the OpenAPI schema shows them, one function per
// `#[schema(default = ...)]`.
fn default_connection_timeout() -> String {
    DEFAULT_CONNECTION_TIMEOUT.to_string()
}
fn default_request_timeout() -> String {
    DEFAULT_REQUEST_TIMEOUT.to_string()
}
fn default_inactivity_timeout() -> String {
    DEFAULT_INACTIVITY_TIMEOUT.to_string()
}
fn default_retry_interval() -> String {
    DEFAULT_RETRY_INTERVAL.to_string()
}
fn default_retry_max_interval() -> String {
    DEFAULT_RETRY_MAX_INTERVAL.to_string()
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

    /// Connection timeout, for example `10s`.
    ///
    /// How long to wait when establishing the initial connection to the
    /// NATS server. Unset selects the default, `DEFAULT_CONNECTION_TIMEOUT`.
    #[serde(
        default,
        alias = "connection_timeout_secs",
        deserialize_with = "duration_connection_timeout",
        skip_serializing_if = "Option::is_none"
    )]
    #[schema(value_type = Option<String>, default = default_connection_timeout)]
    pub connection_timeout: Option<ConfigDuration>,

    /// Request timeout, for example `10s`.
    ///
    /// How long to wait for responses to requests. Unset selects
    /// the default, `DEFAULT_REQUEST_TIMEOUT`.
    #[serde(
        default,
        alias = "request_timeout_secs",
        deserialize_with = "duration_request_timeout",
        skip_serializing_if = "Option::is_none"
    )]
    #[schema(value_type = Option<String>, default = default_request_timeout)]
    pub request_timeout: Option<ConfigDuration>,
}

impl ConnectOptions {
    /// The initial-connection timeout in effect, or
    /// [`DEFAULT_CONNECTION_TIMEOUT`].
    pub fn connection_timeout(&self) -> ConfigDuration {
        self.connection_timeout
            .unwrap_or(DEFAULT_CONNECTION_TIMEOUT)
    }

    /// The request timeout in effect, or [`DEFAULT_REQUEST_TIMEOUT`].
    pub fn request_timeout(&self) -> ConfigDuration {
        self.request_timeout.unwrap_or(DEFAULT_REQUEST_TIMEOUT)
    }
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
    /// How long a pull request may stay parked on the server before it
    /// expires, for example `30s`.
    ///
    /// Unset leaves the NATS server's own default in place.
    #[serde(
        default,
        alias = "max_expires",
        deserialize_with = "duration_max_expiry",
        skip_serializing_if = "Option::is_none"
    )]
    #[schema(value_type = Option<String>)]
    pub max_expiry: Option<ConfigDuration>,
}

#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct NatsInputConfig {
    pub connection_config: ConnectOptions,
    pub stream_name: String,
    /// Maximum time to wait for the next message before running a
    /// stream/server health check, for example `60s`. Must be at least one
    /// second. Unset selects the default, `DEFAULT_INACTIVITY_TIMEOUT`.
    #[serde(
        default,
        alias = "inactivity_timeout_secs",
        deserialize_with = "duration_inactivity_timeout",
        skip_serializing_if = "Option::is_none"
    )]
    #[schema(value_type = Option<String>, default = default_inactivity_timeout)]
    pub inactivity_timeout: Option<ConfigDuration>,

    /// Delay before the first automatic reconnect attempt after a failure,
    /// for example `5s`. The delay doubles on every consecutive failure, up
    /// to `retry_max_interval`, and is jittered to avoid synchronized retries
    /// across connectors. Must be at least one second. Unset selects the
    /// default, `DEFAULT_RETRY_INTERVAL`.
    #[serde(
        default,
        alias = "retry_interval_secs",
        deserialize_with = "duration_retry_interval",
        skip_serializing_if = "Option::is_none"
    )]
    #[schema(value_type = Option<String>, default = default_retry_interval)]
    pub retry_interval: Option<ConfigDuration>,

    /// Upper bound on the delay between consecutive reconnect attempts, for
    /// example `5m`. Must be at least `retry_interval`. Unset selects the
    /// default, `DEFAULT_RETRY_MAX_INTERVAL`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<String>, default = default_retry_max_interval)]
    pub retry_max_interval: Option<ConfigDuration>,

    /// Maximum number of consecutive failed reconnect attempts before the
    /// connector gives up and reports a fatal error. When unset, the connector
    /// retries indefinitely. Must be at least 1 when set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_max_attempts: Option<u32>,
    pub consumer_config: ConsumerConfig,
}

/// Default time to wait for the server's publish acknowledgment.
pub const DEFAULT_PUBLISH_TIMEOUT: ConfigDuration = ConfigDuration::from_secs(5);

/// The default as the OpenAPI schema shows it.
fn default_publish_timeout() -> String {
    DEFAULT_PUBLISH_TIMEOUT.to_string()
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
    /// retrying the publish, for example `5s`. Must be at least one second.
    /// Unset selects the default, `DEFAULT_PUBLISH_TIMEOUT`.
    #[serde(
        default,
        alias = "publish_timeout_secs",
        deserialize_with = "duration_publish_timeout",
        skip_serializing_if = "Option::is_none"
    )]
    #[schema(value_type = Option<String>, default = default_publish_timeout)]
    pub publish_timeout: Option<ConfigDuration>,

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

impl NatsOutputConfig {
    /// The publish timeout in effect, or [`DEFAULT_PUBLISH_TIMEOUT`].
    pub fn publish_timeout(&self) -> ConfigDuration {
        self.publish_timeout.unwrap_or(DEFAULT_PUBLISH_TIMEOUT)
    }
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

impl NatsInputConfig {
    /// The inactivity timeout in effect, or [`DEFAULT_INACTIVITY_TIMEOUT`].
    pub fn inactivity_timeout(&self) -> ConfigDuration {
        self.inactivity_timeout
            .unwrap_or(DEFAULT_INACTIVITY_TIMEOUT)
    }

    /// The first reconnect delay in effect, or [`DEFAULT_RETRY_INTERVAL`].
    pub fn retry_interval(&self) -> ConfigDuration {
        self.retry_interval.unwrap_or(DEFAULT_RETRY_INTERVAL)
    }

    /// The reconnect delay bound in effect, or [`DEFAULT_RETRY_MAX_INTERVAL`].
    pub fn retry_max_interval(&self) -> ConfigDuration {
        self.retry_max_interval
            .unwrap_or(DEFAULT_RETRY_MAX_INTERVAL)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Wraps the fields under test in the keys `ConnectOptions` requires.
    fn connect_options_json(fields: &str) -> String {
        let separator = if fields.is_empty() { "" } else { ", " };
        format!(r#"{{"server_url": "nats://localhost:4222"{separator}{fields}}}"#)
    }

    /// The legacy `{secs, nanos}` expiry rejects every other shape with a
    /// message naming the setting: an unknown key, an empty object, a bare
    /// number (never a spelling of this setting), a fractional number.
    #[test]
    fn the_legacy_expiry_object_rejects_other_shapes() {
        for (expiry, expected) in [
            (r#"{"secs": 45, "millis": 0}"#, "unknown field `millis`"),
            (r#"{}"#, "{secs, nanos} object"),
            ("30", "invalid type: integer `30`"),
            ("30.5", "the number 30.5"),
        ] {
            let json = format!(r#"{{"deliver_policy": "All", "max_expires": {expiry}}}"#);
            let error = serde_json::from_str::<ConsumerConfig>(&json)
                .unwrap_err()
                .to_string();
            assert!(error.contains(expected), "{expiry}: {error}");
        }
    }

    /// Wraps the fields under test in the keys `ConsumerConfig` requires.
    fn consumer_config_json(fields: &str) -> String {
        let separator = if fields.is_empty() { "" } else { ", " };
        format!(r#"{{"deliver_policy": "All"{separator}{fields}}}"#)
    }

    /// Wraps the fields under test in the keys `NatsInputConfig` requires.
    fn input_config_json(fields: &str) -> String {
        let separator = if fields.is_empty() { "" } else { ", " };
        format!(
            r#"{{"connection_config": {{"server_url": "nats://localhost:4222"}}, "stream_name": "stream", "consumer_config": {{"deliver_policy": "All"}}{separator}{fields}}}"#
        )
    }

    /// The connection timeout reads back from both spellings. They reach one
    /// field, so writing both is rejected as a duplicate.
    #[test]
    fn connection_timeout_accepts_both_spellings() {
        for (fields, expected) in [
            ("", DEFAULT_CONNECTION_TIMEOUT),
            (
                r#""connection_timeout": "500ms""#,
                ConfigDuration::from_millis(500),
            ),
            (
                r#""connection_timeout_secs": 3"#,
                ConfigDuration::from_secs(3),
            ),
        ] {
            let json = connect_options_json(fields);
            let config: ConnectOptions = serde_json::from_str(&json).unwrap();
            assert_eq!(config.connection_timeout(), expected, "parsing {json}");
        }

        // Both spellings reach one field, so writing both is a duplicate.
        let json =
            connect_options_json(r#""connection_timeout": "1s", "connection_timeout_secs": 2"#);
        let error = serde_json::from_str::<ConnectOptions>(&json)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("duplicate field `connection_timeout`"),
            "{error}"
        );
    }

    /// See [`connection_timeout_accepts_both_spellings`].
    #[test]
    fn request_timeout_accepts_both_spellings() {
        for (fields, expected) in [
            ("", DEFAULT_REQUEST_TIMEOUT),
            (
                r#""request_timeout": "250ms""#,
                ConfigDuration::from_millis(250),
            ),
            (r#""request_timeout_secs": 7"#, ConfigDuration::from_secs(7)),
        ] {
            let json = connect_options_json(fields);
            let config: ConnectOptions = serde_json::from_str(&json).unwrap();
            assert_eq!(config.request_timeout(), expected, "parsing {json}");
        }
    }

    /// The pull-request expiry has no default of its own: with neither field
    /// set the accessor yields `None`, which leaves the NATS server's own
    /// expiry in place.  The deprecated field is a `std::time::Duration`, so a
    /// user writes it as a `{"secs": .., "nanos": ..}` object.
    #[test]
    fn max_expiry_accepts_both_spellings() {
        for (fields, expected) in [
            ("", None),
            (
                r#""max_expiry": "30s""#,
                Some(ConfigDuration::from_secs(30)),
            ),
            (
                r#""max_expires": {"secs": 45, "nanos": 500000000}"#,
                Some(ConfigDuration::from_millis(45_500)),
            ),
            // An explicit `null` means "unset" on every duration setting.
            // This one used to reject it as an unmatched variant, because its
            // superseded spelling was an object rather than a number.
            (r#""max_expiry": null"#, None),
            (r#""max_expires": null"#, None),
        ] {
            let json = consumer_config_json(fields);
            let config: ConsumerConfig = serde_json::from_str(&json).unwrap();
            assert_eq!(config.max_expiry, expected, "parsing {json}");
        }
    }

    /// The inactivity timeout reads back from both spellings.
    #[test]
    fn inactivity_timeout_accepts_both_spellings() {
        for (fields, expected) in [
            ("", DEFAULT_INACTIVITY_TIMEOUT),
            (
                r#""inactivity_timeout": "90s""#,
                ConfigDuration::from_secs(90),
            ),
            (
                r#""inactivity_timeout_secs": 15"#,
                ConfigDuration::from_secs(15),
            ),
        ] {
            let json = input_config_json(fields);
            let config: NatsInputConfig = serde_json::from_str(&json).unwrap();
            assert_eq!(config.inactivity_timeout(), expected, "parsing {json}");
        }
    }

    /// See [`inactivity_timeout_accepts_both_spellings`].
    #[test]
    fn retry_interval_accepts_both_spellings() {
        for (fields, expected) in [
            ("", DEFAULT_RETRY_INTERVAL),
            (r#""retry_interval": "2s""#, ConfigDuration::from_secs(2)),
            (
                r#""retry_interval_secs": 30"#,
                ConfigDuration::from_secs(30),
            ),
        ] {
            let json = input_config_json(fields);
            let config: NatsInputConfig = serde_json::from_str(&json).unwrap();
            assert_eq!(config.retry_interval(), expected, "parsing {json}");
        }
    }

    /// The publish timeout reads back from both spellings.
    #[test]
    fn publish_timeout_accepts_both_spellings() {
        for (fields, expected) in [
            ("", DEFAULT_PUBLISH_TIMEOUT),
            (r#""publish_timeout": "2s""#, ConfigDuration::from_secs(2)),
            (
                r#""publish_timeout_secs": 30"#,
                ConfigDuration::from_secs(30),
            ),
        ] {
            let separator = if fields.is_empty() { "" } else { ", " };
            let json = format!(
                r#"{{"connection_config": {{"server_url": "nats://localhost:4222"}}, "subject": "s"{separator}{fields}}}"#
            );
            let config: NatsOutputConfig = serde_json::from_str(&json).unwrap();
            assert_eq!(config.publish_timeout(), expected, "parsing {json}");
        }
    }
}
