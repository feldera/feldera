use std::path::PathBuf;
use time::OffsetDateTime;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use utoipa::ToSchema;

use crate::{
    duration::{Duration, LegacyUnit},
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
pub const DEFAULT_CONNECTION_TIMEOUT: Duration = Duration::from_secs(10);

/// Default time to wait for a response to a NATS request.
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Default time to wait for the next message before running a health check.
pub const DEFAULT_INACTIVITY_TIMEOUT: Duration = Duration::from_secs(60);

/// Default delay between automatic reconnect attempts.
pub const DEFAULT_RETRY_INTERVAL: Duration = Duration::from_secs(5);

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
    /// NATS server. Defaults to 10 seconds.
    #[serde(
        default,
        alias = "connection_timeout_secs",
        deserialize_with = "duration_connection_timeout",
        skip_serializing_if = "Option::is_none"
    )]
    pub connection_timeout: Option<Duration>,

    /// Request timeout, for example `10s`.
    ///
    /// How long to wait for responses to requests. Defaults to 10 seconds.
    #[serde(
        default,
        alias = "request_timeout_secs",
        deserialize_with = "duration_request_timeout",
        skip_serializing_if = "Option::is_none"
    )]
    pub request_timeout: Option<Duration>,
}

impl ConnectOptions {
    /// The initial-connection timeout in effect, or
    /// [`DEFAULT_CONNECTION_TIMEOUT`].
    pub fn connection_timeout(&self) -> Duration {
        self.connection_timeout
            .unwrap_or(DEFAULT_CONNECTION_TIMEOUT)
    }

    /// The request timeout in effect, or [`DEFAULT_REQUEST_TIMEOUT`].
    pub fn request_timeout(&self) -> Duration {
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
    pub max_expiry: Option<Duration>,
}

impl ConsumerConfig {}

#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct NatsInputConfig {
    pub connection_config: ConnectOptions,
    pub stream_name: String,
    /// Maximum time to wait for the next message before running a
    /// stream/server health check, for example `60s`. Must be at least one
    /// second. Defaults to 60 seconds.
    #[serde(
        default,
        alias = "inactivity_timeout_secs",
        deserialize_with = "duration_inactivity_timeout",
        skip_serializing_if = "Option::is_none"
    )]
    pub inactivity_timeout: Option<Duration>,

    /// Delay between automatic reconnect attempts while in retry mode, for
    /// example `5s`. Must be at least one second. Defaults to 5 seconds.
    #[serde(
        default,
        alias = "retry_interval_secs",
        deserialize_with = "duration_retry_interval",
        skip_serializing_if = "Option::is_none"
    )]
    pub retry_interval: Option<Duration>,

    pub consumer_config: ConsumerConfig,
}

impl NatsInputConfig {
    /// The inactivity timeout in effect, or [`DEFAULT_INACTIVITY_TIMEOUT`].
    pub fn inactivity_timeout(&self) -> Duration {
        self.inactivity_timeout
            .unwrap_or(DEFAULT_INACTIVITY_TIMEOUT)
    }

    /// The reconnect delay in effect, or [`DEFAULT_RETRY_INTERVAL`].
    pub fn retry_interval(&self) -> Duration {
        self.retry_interval.unwrap_or(DEFAULT_RETRY_INTERVAL)
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
                Duration::from_millis(500),
            ),
            (r#""connection_timeout_secs": 3"#, Duration::from_secs(3)),
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
            (r#""request_timeout": "250ms""#, Duration::from_millis(250)),
            (r#""request_timeout_secs": 7"#, Duration::from_secs(7)),
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
            (r#""max_expiry": "30s""#, Some(Duration::from_secs(30))),
            (
                r#""max_expires": {"secs": 45, "nanos": 500000000}"#,
                Some(Duration::from_millis(45_500)),
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
            (r#""inactivity_timeout": "90s""#, Duration::from_secs(90)),
            (r#""inactivity_timeout_secs": 15"#, Duration::from_secs(15)),
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
            (r#""retry_interval": "2s""#, Duration::from_secs(2)),
            (r#""retry_interval_secs": 30"#, Duration::from_secs(30)),
        ] {
            let json = input_config_json(fields);
            let config: NatsInputConfig = serde_json::from_str(&json).unwrap();
            assert_eq!(config.retry_interval(), expected, "parsing {json}");
        }
    }
}
