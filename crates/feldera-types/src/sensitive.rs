//! Credential declarations supplement diagnostic filtering without changing
//! operational configuration or declassifying explicit Secret references.

use crate::config::{ConnectorConfig, FormatConfig, TransportConfig};
use crate::secret_redaction::secret_redactor;

/// Visits known credential fields as (canonical field path, value).
///
/// Absence from this list does not make a field safe to print. Source diagnostics
/// still use allowlisted projections; the resolver still registers references
/// in every field, independently of this trait.
pub trait SensitiveValues {
    fn visit_sensitive_values(&self, visit: &mut dyn FnMut(&str, &str));

    /// Supplements reference registration before validation or construction can
    /// report credentials. Repeated calls preserve reference provenance.
    fn register_sensitive_values(&self) {
        self.visit_sensitive_values(&mut |_, value| secret_redactor().register_credential(value));
    }
}

impl SensitiveValues for ConnectorConfig {
    fn visit_sensitive_values(&self, visit: &mut dyn FnMut(&str, &str)) {
        self.transport.visit_sensitive_values(visit);
        if let Some(format) = &self.format {
            format.visit_sensitive_values(visit);
        }
    }
}

impl SensitiveValues for TransportConfig {
    fn visit_sensitive_values(&self, visit: &mut dyn FnMut(&str, &str)) {
        match self {
            Self::NatsInput(c) => c.visit_sensitive_values(visit),
            Self::NatsOutput(c) => c.visit_sensitive_values(visit),
            Self::KafkaInput(c) => c.visit_sensitive_values(visit),
            Self::KafkaOutput(c) => c.visit_sensitive_values(visit),
            Self::PubSubInput(c) => c.visit_sensitive_values(visit),
            Self::UrlInput(c) => c.visit_sensitive_values(visit),
            Self::S3Input(c) => c.visit_sensitive_values(visit),
            Self::DeltaTableInput(c) => c.visit_sensitive_values(visit),
            Self::DeltaTableOutput(c) => c.visit_sensitive_values(visit),
            Self::DynamoDBOutput(c) => c.visit_sensitive_values(visit),
            Self::RedisOutput(c) => c.visit_sensitive_values(visit),
            Self::IcebergInput(c) => c.visit_sensitive_values(visit),
            Self::PostgresInput(c) => c.visit_sensitive_values(visit),
            Self::PostgresCdcInput(c) => c.visit_sensitive_values(visit),
            Self::PostgresOutput(c) => c.visit_sensitive_values(visit),
            Self::S2Input(c) => c.visit_sensitive_values(visit),
            Self::S2Output(c) => c.visit_sensitive_values(visit),
            Self::FileInput(_)
            | Self::FileOutput(_)
            | Self::Datagen(_)
            | Self::Nexmark(_)
            | Self::HttpInput(_)
            | Self::HttpOutput(_)
            | Self::AdHocInput(_)
            | Self::ClockInput(_)
            | Self::NullOutput
            | Self::EmptyInput => {}
        }
    }
}

impl SensitiveValues for FormatConfig {
    fn visit_sensitive_values(&self, visit: &mut dyn FnMut(&str, &str)) {
        // The input and output Avro formats share registry credentials. Inspect
        // raw JSON before deserialization, which can itself fail. Other formats
        // currently declare no credentials; opaque values remain omitted at
        // diagnostic sources, and their Secret references remain registered.
        if self.name == "avro" {
            crate::format::avro::visit_sensitive_config(&self.config, visit);
        }
    }
}

pub(crate) fn visit_optional(path: &str, value: Option<&str>, visit: &mut dyn FnMut(&str, &str)) {
    if let Some(value) = value {
        visit(path, value);
    }
}

/// Visit URLs with userinfo or query values: a username alone can hold a token.
/// Register the whole URL in that case; the registry also derives its password.
/// A scheme-less address such as `host:4222`, or an invalid URL, holds a
/// credential only where a `@` or a `?` can hide one.
pub(crate) fn visit_url(path: &str, value: &str, visit: &mut dyn FnMut(&str, &str)) {
    let has_credentials = match url::Url::parse(value) {
        Ok(url) if !url.cannot_be_a_base() => {
            !url.username().is_empty() || url.password().is_some() || url.query().is_some()
        }
        _ => value.contains('@') || value.contains('?'),
    };
    if has_credentials {
        visit(path, value);
    }
}

/// Visits the credentials of a connection string. A URL is registered whole
/// when it has userinfo or a query, and the registry derives its password;
/// a URL without them stays visible in diagnostics. A libpq keyword string
/// contributes the values of its `password` and `sslpassword` keywords, as
/// the driver reads them and, when escapes differ, as written.
pub(crate) fn visit_connection_string(path: &str, value: &str, visit: &mut dyn FnMut(&str, &str)) {
    if value.contains("://") {
        visit_url(path, value, visit);
        return;
    }
    for (keyword, raw, unescaped) in keyword_values(value) {
        if matches!(keyword, "password" | "sslpassword") {
            let field = format!("{path}.{keyword}");
            visit(&field, &unescaped);
            if unescaped != raw {
                visit(&format!("{field}.raw"), raw);
            }
        }
    }
}

/// The `keyword=value` pairs of a libpq connection string: each value as
/// written, without its quotes, and with its backslash escapes removed.
fn keyword_values(text: &str) -> Vec<(&str, &str, String)> {
    let mut pairs = Vec::new();
    let mut rest = text.trim_start();
    while let Some(equals) = rest.find('=') {
        let keyword = rest[..equals].trim();
        rest = rest[equals + 1..].trim_start();
        let (raw, after) = match rest.strip_prefix('\'') {
            Some(quoted) => {
                let end = unescaped_end(quoted, |c| c == '\'');
                (&quoted[..end], quoted.get(end + 1..).unwrap_or_default())
            }
            None => {
                let end = unescaped_end(rest, char::is_whitespace);
                (&rest[..end], &rest[end..])
            }
        };
        pairs.push((keyword, raw, unescape(raw)));
        rest = after.trim_start();
    }
    pairs
}

/// Index of the first unescaped character matching `stop`, or the end.
fn unescaped_end(text: &str, stop: impl Fn(char) -> bool) -> usize {
    let mut chars = text.char_indices();
    while let Some((index, c)) = chars.next() {
        if c == '\\' {
            chars.next();
        } else if stop(c) {
            return index;
        }
    }
    text.len()
}

fn unescape(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut chars = raw.chars();
    while let Some(c) = chars.next() {
        match c {
            '\\' => out.extend(chars.next()),
            c => out.push(c),
        }
    }
    out
}

pub(crate) fn credential_header(name: &str) -> bool {
    ["authorization", "proxy-authorization", "x-api-key"]
        .iter()
        .any(|known| name.eq_ignore_ascii_case(known))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};
    use std::collections::BTreeMap;

    fn visited(config: &impl SensitiveValues) -> BTreeMap<String, String> {
        let mut values = BTreeMap::new();
        config.visit_sensitive_values(&mut |path, value| {
            assert!(
                values.insert(path.to_string(), value.to_string()).is_none(),
                "duplicate path {path}"
            );
        });
        values
    }

    /// Every credential-bearing transport goes through the enum dispatch.
    /// Configuration is serialized unchanged, including unknown option values.
    #[test]
    fn transport_declarations_preserve_operational_configuration() {
        for (name, config, expected) in [
            (
                "s3_input",
                json!({"region":"r", "bucket_name":"b", "aws_secret_access_key":"s3-secret-2136"}),
                "aws_secret_access_key",
            ),
            (
                "dynamodb_output",
                json!({"region":"r", "table":"t", "aws_secret_access_key":"dynamo-secret-2136"}),
                "aws_secret_access_key",
            ),
            (
                "pub_sub_input",
                json!({"subscription":"s", "credentials":"pubsub-secret-2136"}),
                "credentials",
            ),
            (
                "s2_input",
                json!({"basin":"b", "stream":"s", "auth_token":"s2-secret-2136"}),
                "auth_token",
            ),
            (
                "s2_output",
                json!({"basin":"b", "stream":"s", "auth_token":"s2-secret-2136"}),
                "auth_token",
            ),
            (
                "redis_output",
                json!({"connection_string":"redis://u:redis-secret-2136@host"}),
                "connection_string",
            ),
            (
                "url_input",
                json!({"path":"https://u:url-secret-2136@host/data"}),
                "path",
            ),
            (
                "kafka_input",
                json!({"topic":"public-topic", "sasl.password":"kafka-secret-2136"}),
                "sasl.password",
            ),
            (
                "kafka_output",
                json!({"topic":"public-topic", "sasl.password":"kafka-secret-2136"}),
                "sasl.password",
            ),
            (
                "delta_table_input",
                json!({"uri":"s3://bucket/table", "mode":"snapshot", "AWS_SECRET_ACCESS_KEY":"delta-secret-2136"}),
                "aws_secret_access_key",
            ),
            (
                "delta_table_output",
                json!({"uri":"s3://bucket/table", "secret_access_key":"delta-secret-2136"}),
                "aws_secret_access_key",
            ),
            (
                "iceberg_input",
                json!({"mode":"snapshot", "rest.token":"iceberg-secret-2136"}),
                "rest.token",
            ),
            (
                "postgres_input",
                json!({"uri":"host=h password=pg-secret-2136", "query":"SELECT 1"}),
                "uri.password",
            ),
            (
                "postgres_cdc_input",
                json!({"uri":"host=h password=pg-secret-2136", "publication":"p", "source_table":"t"}),
                "uri.password",
            ),
            (
                "postgres_output",
                json!({"uri":"host=h password=pg-secret-2136", "table":"t"}),
                "uri.password",
            ),
            (
                "nats_input",
                json!({"connection_config":{"server_url":"nats://host", "auth":{"token":"nats-secret-2136"}}, "stream_name":"s", "consumer_config":{"deliver_policy":"All"}}),
                "connection_config.auth.token",
            ),
            (
                "nats_output",
                json!({"connection_config":{"server_url":"nats://host", "auth":{"token":"nats-secret-2136"}}, "subject":"s"}),
                "connection_config.auth.token",
            ),
        ] {
            let transport: TransportConfig =
                serde_json::from_value(json!({"name": name, "config":config}))
                    .unwrap_or_else(|e| panic!("{name}: {e}"));
            let before = serde_json::to_value(&transport).unwrap();
            let values = visited(&transport);
            assert!(values.contains_key(expected), "{name}: {values:?}");
            assert!(
                values.values().all(|value| value.contains("secret-2136")),
                "{name}: {values:?}"
            );
            assert_eq!(before, serde_json::to_value(&transport).unwrap());
        }
    }

    #[test]
    fn kafka_credentials_include_nested_options_and_aliases() {
        let config: TransportConfig = serde_json::from_value(json!({"name":"kafka_output", "config":{
            "topic":"public-topic", "bootstrap.servers":"public-host", "unknown.password":"opaque-public",
            "sasl.password":"sasl-value", "ssl.key.password":"key-password", "ssl.key.pem":"key-pem",
            "ssl.keystore.password":"keystore-password", "sasl.oauthbearer.client.credentials.client.secret":"oauth-value",
            "sasl.oauthbearer.assertion.private.key.pem":"assertion-pem",
            "sasl.oauthbearer.assertion.private.key.passphrase":"assertion-passphrase",
            "fault_tolerance":{"consumer_options":{"sasl.password":"consumer-password"}, "producer_options":{"ssl.key.pem":"producer-pem"}}
        }})).unwrap();
        assert_eq!(
            visited(&config),
            BTreeMap::from([
                ("sasl.password".into(), "sasl-value".into()),
                ("ssl.key.password".into(), "key-password".into()),
                ("ssl.key.pem".into(), "key-pem".into()),
                ("ssl.keystore.password".into(), "keystore-password".into()),
                (
                    "sasl.oauthbearer.assertion.private.key.pem".into(),
                    "assertion-pem".into()
                ),
                (
                    "sasl.oauthbearer.assertion.private.key.passphrase".into(),
                    "assertion-passphrase".into()
                ),
                (
                    "sasl.oauthbearer.client.secret".into(),
                    "oauth-value".into()
                ),
                (
                    "fault_tolerance.consumer_options.sasl.password".into(),
                    "consumer-password".into()
                ),
                (
                    "fault_tolerance.producer_options.ssl.key.pem".into(),
                    "producer-pem".into()
                ),
            ])
        );
    }

    #[test]
    fn token_only_userinfo_is_a_credential() {
        let config: ConnectorConfig = serde_json::from_value(json!({
            "transport":{"name":"url_input","config":{"path":"https://input-token-2136@host/data"}},
            "format":{"name":"avro","config":{"registry_urls":["https://registry-token-2136@host/schemas"]}}
        })).unwrap();
        let before = config.clone();
        assert_eq!(
            visited(&config),
            BTreeMap::from([
                (
                    "path".to_string(),
                    "https://input-token-2136@host/data".to_string()
                ),
                (
                    "registry_urls.0".to_string(),
                    "https://registry-token-2136@host/schemas".to_string()
                ),
            ])
        );
        let redactor = crate::secret_redaction::SecretRedactor::new();
        config.visit_sensitive_values(&mut |_, value| redactor.register_credential(value));
        assert_eq!(
            redactor.redact("fetch https://input-token-2136@host/data"),
            "fetch [REDACTED]"
        );
        assert_eq!(
            redactor.redact("fetch https://registry-token-2136@host/schemas"),
            "fetch [REDACTED]"
        );
        assert_eq!(config, before);
    }

    /// Only the credentials of a connection string are registered: a URI
    /// without any stays visible, and a libpq keyword password is covered on
    /// its own, as the driver reads it.
    #[test]
    fn connection_strings_register_only_their_credentials() {
        let postgres = |uri: &str| -> TransportConfig {
            serde_json::from_value(
                json!({"name":"postgres_input", "config":{"uri":uri, "query":"SELECT 1"}}),
            )
            .unwrap()
        };
        let redis = |connection_string: &str| -> TransportConfig {
            serde_json::from_value(
                json!({"name":"redis_output", "config":{"connection_string":connection_string}}),
            )
            .unwrap()
        };
        for public in [
            postgres("postgres://db.example:5432/analytics"),
            postgres("host=db.example dbname=analytics"),
            redis("redis://cache.example:6379"),
        ] {
            assert!(visited(&public).is_empty());
        }
        assert_eq!(
            visited(&postgres(
                "host=db.example password=pg-keyword-secret-2136 dbname=db"
            )),
            BTreeMap::from([(
                "uri.password".to_string(),
                "pg-keyword-secret-2136".to_string()
            )])
        );
        assert_eq!(
            visited(&postgres(
                r"host=h password='quoted \'secret\' 2136' sslpassword=key-secret-2136"
            )),
            BTreeMap::from([
                (
                    "uri.password".to_string(),
                    "quoted 'secret' 2136".to_string()
                ),
                (
                    "uri.password.raw".to_string(),
                    r"quoted \'secret\' 2136".to_string()
                ),
                ("uri.sslpassword".to_string(), "key-secret-2136".to_string()),
            ])
        );
        let redactor = crate::secret_redaction::SecretRedactor::new();
        for config in [
            postgres("host=db.example password=pg-keyword-secret-2136"),
            postgres("postgres://u:pg-url-secret-2136@db.example/db"),
        ] {
            config.visit_sensitive_values(&mut |_, value| redactor.register_credential(value));
        }
        assert_eq!(
            redactor.redact("auth failed for password pg-keyword-secret-2136"),
            "auth failed for password [REDACTED]"
        );
        assert_eq!(
            redactor.redact("auth failed for password pg-url-secret-2136"),
            "auth failed for password [REDACTED]"
        );
        let public = "error connecting to postgres://db.example:5432/analytics: refused";
        assert_eq!(redactor.redact(public), public);
    }

    /// async-nats accepts `host:port` without a scheme; `url` reads it as an
    /// opaque URL. Without a `@` or a `?` it holds no credential.
    #[test]
    fn a_scheme_less_server_address_is_not_a_credential() {
        let nats = |server_url: &str| -> TransportConfig {
            serde_json::from_value(json!({"name":"nats_input", "config":{
                "connection_config":{"server_url":server_url}, "stream_name":"s",
                "consumer_config":{"deliver_policy":"All"}
            }}))
            .unwrap()
        };
        for public in [
            "nats-1.internal:4222",
            "nats://nats-1.internal:4222",
            "localhost",
        ] {
            assert!(visited(&nats(public)).is_empty(), "{public}");
        }
        for private in [
            "u:url-secret-2136@nats-1.internal:4222",
            "nats://u:url-secret-2136@host",
            "host:4222?token=url-secret-2136",
        ] {
            assert_eq!(
                visited(&nats(private)),
                BTreeMap::from([(
                    "connection_config.server_url".to_string(),
                    private.to_string()
                )]),
                "{private}"
            );
        }
    }

    #[test]
    fn avro_visits_raw_credentials_even_when_parsing_will_fail() {
        let config = FormatConfig {
            name: "avro".into(),
            config: json!({
                "registry_password":"password-value", "registry_authorization_token":"token-value",
                "registry_headers":{"Authorization":"header-value", "X-Request-ID":"public-value"},
                "registry_urls":["https://u:uri-secret-2136@host", 1, "https://host/public"],
                "registry_proxy":"https://u:proxy-secret-2136@proxy", "update_format":"invalid"
            }),
        };
        let before = config.clone();
        let values = visited(&config);
        assert_eq!(values.len(), 5, "{values:?}");
        for path in [
            "registry_password",
            "registry_authorization_token",
            "registry_headers.authorization",
            "registry_urls.0",
            "registry_proxy",
        ] {
            assert!(values.contains_key(path), "{path}: {values:?}");
        }
        assert_eq!(config, before);
        for raw in [
            Value::Null,
            json!({}),
            json!({"registry_password":1, "registry_headers":[], "registry_urls":"bad"}),
        ] {
            assert!(
                visited(&FormatConfig {
                    name: "avro".into(),
                    config: raw
                })
                .is_empty()
            );
        }
        for name in ["csv", "json", "parquet", "unknown"] {
            assert!(
                visited(&FormatConfig {
                    name: name.into(),
                    config: config.config.clone()
                })
                .is_empty()
            );
        }
    }

    #[test]
    fn nested_nats_postgres_and_iceberg_credentials_are_declared() {
        let nats: TransportConfig = serde_json::from_value(json!({"name":"nats_output", "config":{
            "subject":"public", "connection_config":{"server_url":"nats://u:url-secret-2136@host", "auth":{
                "credentials":{"FromString":"creds-secret-2136"}, "jwt":"jwt-secret-2136", "nkey":"nkey-secret-2136",
                "token":"token-secret-2136", "user_and_password":{"user":"public", "password":"password-secret-2136"}
            }}
        }})).unwrap();
        let nats = visited(&nats);
        assert_eq!(nats.len(), 6, "{nats:?}");
        for suffix in [
            "credentials.FromString",
            "jwt",
            "nkey",
            "token",
            "user_and_password.password",
        ] {
            assert!(nats.contains_key(&format!("connection_config.auth.{suffix}")));
        }
        for name in ["postgres_input", "postgres_output"] {
            let config: TransportConfig = serde_json::from_value(json!({"name":name, "config":{
                "uri":"postgres://u:password-secret-2136@host/db", "query":"SELECT 1", "table":"t",
                "ssl_client_key":"pem-secret-2136"
            }}))
            .unwrap();
            assert_eq!(visited(&config)["ssl_client_key"], "pem-secret-2136");
        }
        let keys = [
            "glue.secret-access-key",
            "glue.session-token",
            "s3tables.secret-access-key",
            "s3tables.session-token",
            "rest.credential",
            "rest.token",
            "s3.secret-access-key",
            "s3.session-token",
            "s3.sse.key",
            "gcs.credentials-json",
            "gcs.oauth2.token",
            "oss.access-key-secret",
            "adls.connection-string",
            "adls.account-key",
            "adls.sas-token",
            "adls.client-secret",
        ];
        let mut config = json!({"mode":"snapshot", "rest.headers":[["Authorization", "header-secret-2136"], ["X-Request-ID", "public"]]});
        for key in keys {
            config[key] = json!(format!("{key}-secret-2136"));
        }
        let config: TransportConfig =
            serde_json::from_value(json!({"name":"iceberg_input", "config":config})).unwrap();
        let values = visited(&config);
        assert_eq!(values.len(), keys.len() + 1, "{values:?}");
        for key in keys {
            assert_eq!(values[key], format!("{key}-secret-2136"));
        }
        assert_eq!(values["rest.headers.authorization"], "header-secret-2136");
    }

    #[test]
    fn object_store_aliases_are_canonical_and_unknown_keys_are_opaque() {
        for (alias, canonical) in [
            ("secret_access_key", "aws_secret_access_key"),
            ("AWS_SECRET_ACCESS_KEY", "aws_secret_access_key"),
            ("aws_token", "aws_session_token"),
            ("session_token", "aws_session_token"),
            ("sse_customer_key_base64", "aws_sse_customer_key_base64"),
            ("azure_storage_access_key", "azure_storage_account_key"),
            ("master_key", "azure_storage_account_key"),
            ("access_key", "azure_storage_account_key"),
            ("account_key", "azure_storage_account_key"),
            ("azure_client_secret", "azure_storage_client_secret"),
            ("client_secret", "azure_storage_client_secret"),
            ("sas_token", "azure_storage_sas_key"),
            ("azure_storage_sas_token", "azure_storage_sas_key"),
            ("fabric_session_token", "azure_fabric_session_token"),
            ("encryption_key", "azure_storage_encryption_key"),
            ("service_account_key", "google_service_account_key"),
            ("google_bearer_token", "google_bearer_token"),
            ("databricks_token", "unity_access_token"),
            ("access_token", "unity_access_token"),
            ("databricks_client_secret", "unity_client_secret"),
        ] {
            let config: TransportConfig = serde_json::from_value(json!({"name":"delta_table_output", "config": {
                "uri":"s3://bucket/table", alias:"credential-value", "unknown_password":"opaque-value", "aws_region":"public-region"
            }})).unwrap();
            assert_eq!(
                visited(&config),
                BTreeMap::from([(canonical.to_string(), "credential-value".to_string())]),
                "{alias}"
            );
        }
    }
}
