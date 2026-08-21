use serde::{Deserialize, Serialize};
use std::fmt::Display;
use utoipa::ToSchema;

/// PostgreSQL write mode.
///
/// Determines how the PostgreSQL output connector writes data to the target table.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema, Default)]
pub enum PostgresWriteMode {
    /// Materialized mode: perform direct INSERT, UPDATE, and DELETE operations on the table.
    /// This is the default behavior and maintains the postgres table as a materialized snapshot of the output view.
    #[default]
    #[serde(rename = "materialized")]
    Materialized,

    /// CDC (Change Data Capture) mode: write all operations as INSERT operations
    /// into a Postgres table that serves as an append-only event log.
    /// In this mode, inserts, updates, and deletes are all represented as new rows
    /// with metadata columns describing the operation type and timestamp.
    #[serde(rename = "cdc")]
    Cdc,
}

impl Display for PostgresWriteMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Materialized => write!(f, "materialized"),
            Self::Cdc => write!(f, "cdc"),
        }
    }
}

/// TLS/SSL configuration for PostgreSQL connectors.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema, Default)]
pub struct PostgresTlsConfig {
    /// A sequence of CA certificates in PEM format.
    pub ssl_ca_pem: Option<String>,

    /// Path to a file containing a sequence of CA certificates in PEM format.
    pub ssl_ca_location: Option<String>,

    /// The client certificate in PEM format.
    pub ssl_client_pem: Option<String>,

    /// Path to the client certificate.
    pub ssl_client_location: Option<String>,

    /// The client certificate key in PEM format.
    pub ssl_client_key: Option<String>,

    /// Path to the client certificate key.
    pub ssl_client_key_location: Option<String>,

    /// The path to the certificate chain file.
    /// The file must contain a sequence of PEM-formatted certificates,
    /// the first being the leaf certificate, and the remainder forming
    /// the chain of certificates up to and including the trusted root certificate.
    pub ssl_certificate_chain_location: Option<String>,

    /// True to enable hostname verification when using TLS. True by default.
    ///
    /// When false, the certificate chain is still verified against the
    /// trusted CA; only the requirement that the server name appears in the
    /// certificate is lifted.
    pub verify_hostname: Option<bool>,
}

impl PostgresTlsConfig {
    pub fn has_tls(&self) -> bool {
        self.ssl_ca_pem.is_some() || self.ssl_ca_location.is_some()
    }
}

/// Batch processing configuration for the Postgres CDC connector.
///
/// Controls how replication events are buffered into a batch before being
/// flushed to Feldera. Mirrors the batch configuration of the underlying
/// `etl` replication library; fields omitted from the configuration fall
/// back to `etl`'s own defaults.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct PostgresCdcBatchConfig {
    /// Maximum time, in milliseconds, to wait before flushing a partially
    /// filled batch. This is the latency bound for batching: once the first
    /// event enters a batch, the batch is flushed when this timer elapses,
    /// even if `max_bytes` was not reached.
    ///
    /// Default: 10000 (10 seconds).
    #[serde(default = "default_batch_max_fill_ms")]
    #[schema(default = default_batch_max_fill_ms)]
    pub max_fill_ms: u64,

    /// Ratio of process memory reserved for incoming replication batch
    /// bytes, in the `(0.0, 1.0]` interval. The configured memory is divided
    /// by the number of active streams at runtime, so each stream gets only
    /// a per-stream share of the global memory budget.
    ///
    /// Default: 0.2.
    #[serde(default = "default_batch_memory_budget_ratio")]
    #[schema(default = default_batch_memory_budget_ratio)]
    pub memory_budget_ratio: f32,

    /// Maximum preferred byte size for one batch per active stream. This is
    /// a ceiling, not a target: the runtime still chooses the smaller value
    /// between this limit and the memory-ratio budget computed from
    /// `memory_budget_ratio`.
    ///
    /// Default: 8388608 (8 MiB).
    #[serde(default = "default_batch_max_bytes")]
    #[schema(default = default_batch_max_bytes)]
    pub max_bytes: usize,
}

impl Default for PostgresCdcBatchConfig {
    fn default() -> Self {
        Self {
            max_fill_ms: default_batch_max_fill_ms(),
            memory_budget_ratio: default_batch_memory_budget_ratio(),
            max_bytes: default_batch_max_bytes(),
        }
    }
}

// `f32` doesn't implement `Eq` because of NaN, so it can't be derived here.
// `validate()` rejects NaN/out-of-range ratios, so equality is reflexive for
// any value that passes validation; this lets `PostgresCdcReaderConfig` (and
// its containing `TransportConfig` enum) keep deriving `Eq`.
impl Eq for PostgresCdcBatchConfig {}

impl PostgresCdcBatchConfig {
    pub fn validate(&self) -> Result<(), String> {
        if !(0.0..=1.0).contains(&self.memory_budget_ratio) || self.memory_budget_ratio == 0.0 {
            return Err("batch.memory_budget_ratio must be in the (0.0, 1.0] interval".to_string());
        }

        if self.max_bytes == 0 {
            return Err("batch.max_bytes must be greater than 0".to_string());
        }

        Ok(())
    }
}

fn default_batch_max_fill_ms() -> u64 {
    10_000
}

fn default_batch_memory_budget_ratio() -> f32 {
    0.2
}

fn default_batch_max_bytes() -> usize {
    8 * 1024 * 1024
}

/// Memory-based backpressure configuration for the Postgres CDC connector.
///
/// When the connector's memory usage rises above `activate_threshold`, it
/// pauses reading further replication events until usage drops back below
/// `resume_threshold`. Mirrors the memory backpressure configuration of the
/// underlying `etl` replication library.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct PostgresCdcMemoryBackpressureConfig {
    /// Memory usage ratio above which backpressure is activated, in the
    /// `(0.0, 1.0]` interval.
    ///
    /// Default: 0.85.
    #[serde(default = "default_memory_backpressure_activate_threshold")]
    #[schema(default = default_memory_backpressure_activate_threshold)]
    pub activate_threshold: f32,

    /// Memory usage ratio below which backpressure is released, in the
    /// `[0.0, 1.0)` interval. Must be lower than `activate_threshold`.
    ///
    /// Default: 0.75.
    #[serde(default = "default_memory_backpressure_resume_threshold")]
    #[schema(default = default_memory_backpressure_resume_threshold)]
    pub resume_threshold: f32,
}

impl Default for PostgresCdcMemoryBackpressureConfig {
    fn default() -> Self {
        Self {
            activate_threshold: default_memory_backpressure_activate_threshold(),
            resume_threshold: default_memory_backpressure_resume_threshold(),
        }
    }
}

// See the corresponding `impl Eq for PostgresCdcBatchConfig` above for why
// this is sound despite the `f32` fields.
impl Eq for PostgresCdcMemoryBackpressureConfig {}

impl PostgresCdcMemoryBackpressureConfig {
    pub fn validate(&self) -> Result<(), String> {
        if !(0.0..=1.0).contains(&self.activate_threshold) || self.activate_threshold == 0.0 {
            return Err(
                "memory_backpressure.activate_threshold must be in the (0.0, 1.0] interval"
                    .to_string(),
            );
        }

        if !(0.0..=1.0).contains(&self.resume_threshold) || self.resume_threshold == 1.0 {
            return Err(
                "memory_backpressure.resume_threshold must be in the [0.0, 1.0) interval"
                    .to_string(),
            );
        }

        if self.resume_threshold >= self.activate_threshold {
            return Err("memory_backpressure.resume_threshold must be lower than \
                 memory_backpressure.activate_threshold"
                .to_string());
        }

        Ok(())
    }
}

fn default_memory_backpressure_activate_threshold() -> f32 {
    0.85
}

fn default_memory_backpressure_resume_threshold() -> f32 {
    0.75
}

/// Postgres CDC input connector configuration.
///
/// Uses logical replication to capture ongoing changes from a Postgres database.
/// Requires a pre-created publication and a user with REPLICATION privilege.
/// Tables must have primary keys and `REPLICA IDENTITY FULL` is recommended
/// for UPDATE/DELETE support.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct PostgresCdcReaderConfig {
    /// Postgres connection URI. The user must have REPLICATION privilege.
    /// See: <https://docs.rs/tokio-postgres/0.7.12/tokio_postgres/config/struct.Config.html>
    pub uri: String,

    /// Name of the pre-created Postgres publication to replicate from.
    pub publication: String,

    /// Postgres table to replicate (e.g. "public.orders").
    /// Must be included in the publication.
    pub source_table: String,

    /// TLS/SSL configuration.
    #[serde(flatten)]
    #[schema(inline)]
    pub tls: PostgresTlsConfig,

    /// Batch processing configuration.
    ///
    /// Controls how replication events are buffered before being flushed to
    /// Feldera. When omitted, uses the underlying `etl` library's defaults.
    #[serde(default)]
    pub batch: PostgresCdcBatchConfig,

    /// Memory-based backpressure configuration.
    ///
    /// Controls when the connector pauses reading further replication
    /// events to avoid exceeding available memory. When omitted, uses the
    /// underlying `etl` library's defaults.
    #[serde(default)]
    pub memory_backpressure: PostgresCdcMemoryBackpressureConfig,
}

impl PostgresCdcReaderConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.publication.trim().is_empty() {
            return Err("publication cannot be empty".to_string());
        }

        if self.source_table.trim().is_empty() {
            return Err("source_table cannot be empty".to_string());
        }

        self.batch.validate()?;
        self.memory_backpressure.validate()?;

        if self.tls.ssl_client_pem.is_some()
            || self.tls.ssl_client_location.is_some()
            || self.tls.ssl_client_key.is_some()
            || self.tls.ssl_client_key_location.is_some()
            || self.tls.ssl_certificate_chain_location.is_some()
        {
            return Err(
                "client-certificate TLS options (ssl_client_pem, ssl_client_location, \
                 ssl_client_key, ssl_client_key_location, ssl_certificate_chain_location) \
                 are not supported by the Postgres CDC connector as the underlying etl crate \
                 doesn't support client-certificate TLS yet. CA-based TLS via ssl_ca_pem \
                 or ssl_ca_location is supported. Please file an issue if you require \
                 client-certificate TLS support: https://github.com/feldera/feldera/issues/
                 "
                .to_string(),
            );
        }

        if self.tls.verify_hostname == Some(false) {
            return Err(
                "disabling hostname verification is not supported by the Postgres CDC connector"
                    .to_string(),
            );
        }

        Ok(())
    }
}

/// Postgres input connector configuration.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct PostgresReaderConfig {
    /// Postgres URI.
    /// See: <https://docs.rs/tokio-postgres/0.7.12/tokio_postgres/config/struct.Config.html>
    pub uri: String,

    /// Query that specifies what data to fetch from postgres.
    pub query: String,

    /// TLS/SSL configuration.
    #[serde(flatten)]
    #[schema(inline)]
    pub tls: PostgresTlsConfig,
}

/// Postgres output connector configuration.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct PostgresWriterConfig {
    /// Postgres URI.
    /// See: <https://docs.rs/tokio-postgres/0.7.12/tokio_postgres/config/struct.Config.html>
    pub uri: String,

    /// The table to write the output to.
    pub table: String,

    /// Write mode for the connector.
    ///
    /// - `materialized` (default): Perform direct INSERT, UPDATE, and DELETE operations on the table.
    /// - `cdc`: Write all operations as INSERT operations into an append-only event log
    ///   with additional metadata columns describing the operation type and timestamp.
    #[serde(default)]
    #[schema(default = PostgresWriteMode::default)]
    pub mode: PostgresWriteMode,

    /// Name of the operation metadata column in CDC mode.
    ///
    /// Only used when `mode = "cdc"`. This column will contain:
    /// - `"i"` for insert operations
    /// - `"u"` for upsert operations
    /// - `"d"` for delete operations
    ///
    /// Default: `"__feldera_op"`
    #[serde(default = "default_cdc_op_column")]
    #[schema(default = default_cdc_op_column)]
    pub cdc_op_column: String,

    /// Name of the timestamp metadata column in CDC mode.
    ///
    /// Only used when `mode = "cdc"`. This column will contain the timestamp
    /// (in RFC 3339 format) when the batch of updates was output
    /// by the pipeline.
    ///
    /// Default: `"__feldera_ts"`
    #[serde(default = "default_cdc_ts_column")]
    #[schema(default = default_cdc_ts_column)]
    pub cdc_ts_column: String,

    /// TLS/SSL configuration.
    #[serde(flatten)]
    #[schema(inline)]
    pub tls: PostgresTlsConfig,

    /// The maximum number of records in a single buffer.
    pub max_records_in_buffer: Option<usize>,

    /// The maximum buffer size in for a single operation.
    /// Note that the buffers of `INSERT`, `UPDATE` and `DELETE` queries are
    /// separate.
    /// Default: 1 MiB
    #[schema(default = default_max_buffer_size)]
    #[serde(default = "default_max_buffer_size")]
    pub max_buffer_size_bytes: usize,

    /// Specifies how the connector handles conflicts when executing an `INSERT`
    /// into a table with a primary key. By default, an existing row with the same
    /// key is overwritten. Setting this flag to `true` preserves the existing row
    /// and ignores the new insert.
    ///
    /// This setting does not affect `UPDATE` statements, which always replace the
    /// value associated with the key.
    ///
    /// This setting is not supported when `mode = "cdc"`, since all operations
    /// are performed as append-only `INSERT`s into the target table.
    /// Any conflict in CDC mode will result in an error.
    ///
    /// Default: `false`
    #[serde(default)]
    pub on_conflict_do_nothing: bool,

    /// The number of threads to use during encoding.
    ///
    /// Default: 1
    #[serde(default = "default_writer_threads")]
    #[schema(default = default_writer_threads)]
    pub threads: usize,

    /// The names of the extra columns in the Postgres table that are not part of the view schema.
    ///
    /// These connector can write user-defined values, configured using the `set_extra_columns` connector command,
    /// to these columns.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extra_columns: Vec<String>,
}

fn default_max_buffer_size() -> usize {
    usize::pow(2, 20)
}

fn default_writer_threads() -> usize {
    1
}

fn default_cdc_op_column() -> String {
    "__feldera_op".to_string()
}

fn default_cdc_ts_column() -> String {
    "__feldera_ts".to_string()
}

impl PostgresWriterConfig {
    pub fn validate(&self) -> Result<(), String> {
        match self.mode {
            PostgresWriteMode::Cdc => {
                if self.cdc_op_column.trim().is_empty() {
                    return Err("cdc_op_column cannot be empty in CDC mode".to_string());
                }
                if self.cdc_ts_column.trim().is_empty() {
                    return Err("cdc_ts_column cannot be empty in CDC mode".to_string());
                }

                if !self.cdc_op_column.is_ascii() {
                    return Err("cdc_op_column must contain only ASCII characters".to_string());
                }

                if !self.cdc_ts_column.is_ascii() {
                    return Err("cdc_ts_column must contain only ASCII characters".to_string());
                }

                if self.on_conflict_do_nothing {
                    return Err("on_conflict_do_nothing not supported in CDC mode since all operations are performed as append-only INSERTs into the target table".to_string());
                }
            }
            PostgresWriteMode::Materialized => {
                if self.cdc_ts_column != default_cdc_ts_column()
                    && !self.cdc_ts_column.trim().is_empty()
                {
                    return Err(
                        "cdc_ts_column must not be set when in MATERIALIZED mode".to_string()
                    );
                }
                if self.cdc_op_column != default_cdc_op_column()
                    && !self.cdc_op_column.trim().is_empty()
                {
                    return Err(
                        "cdc_op_column must not be set when in MATERIALIZED mode".to_string()
                    );
                }
            }
        };

        if self.threads == 0 {
            return Err("threads must be at least 1".to_string());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn postgres_cdc_config(tls: PostgresTlsConfig) -> PostgresCdcReaderConfig {
        PostgresCdcReaderConfig {
            uri: "postgres://user:password@localhost:5432/database".to_string(),
            publication: "publication".to_string(),
            source_table: "public.table".to_string(),
            tls,
            batch: PostgresCdcBatchConfig::default(),
            memory_backpressure: PostgresCdcMemoryBackpressureConfig::default(),
        }
    }

    #[test]
    fn postgres_cdc_config_rejects_client_certificate_tls_options() {
        let config = postgres_cdc_config(PostgresTlsConfig {
            ssl_client_pem: Some("client".to_string()),
            ..Default::default()
        });

        let err = config.validate().unwrap_err();
        assert!(err.contains("client-certificate TLS options"));
        assert!(err.contains("client-certificate TLS support"));
        assert!(!err.contains("doesn't support TLS yet"));
    }

    #[test]
    fn postgres_cdc_config_rejects_disabled_hostname_verification() {
        let config = postgres_cdc_config(PostgresTlsConfig {
            verify_hostname: Some(false),
            ..Default::default()
        });

        let err = config.validate().unwrap_err();
        assert!(err.contains("disabling hostname verification"));
    }

    #[test]
    fn postgres_cdc_config_accepts_default_tls() {
        let config = postgres_cdc_config(PostgresTlsConfig::default());

        assert!(config.validate().is_ok());
    }

    #[test]
    fn postgres_cdc_config_rejects_empty_publication() {
        let mut config = postgres_cdc_config(PostgresTlsConfig::default());
        config.publication = "   ".to_string();

        let err = config.validate().unwrap_err();
        assert!(err.contains("publication cannot be empty"));
    }

    #[test]
    fn postgres_cdc_config_rejects_empty_source_table() {
        let mut config = postgres_cdc_config(PostgresTlsConfig::default());
        config.source_table = "\t".to_string();

        let err = config.validate().unwrap_err();
        assert!(err.contains("source_table cannot be empty"));
    }

    #[test]
    fn postgres_cdc_config_deserializes_without_batch_or_memory_backpressure() {
        let json = r#"{
            "uri": "postgres://user:password@localhost:5432/database",
            "publication": "publication",
            "source_table": "public.table"
        }"#;
        let config: PostgresCdcReaderConfig = serde_json::from_str(json).unwrap();

        assert_eq!(config.batch, PostgresCdcBatchConfig::default());
        assert_eq!(
            config.memory_backpressure,
            PostgresCdcMemoryBackpressureConfig::default()
        );
        assert!(config.validate().is_ok());
    }

    #[test]
    fn postgres_cdc_batch_config_deserializes_with_partial_fields() {
        let json = r#"{"max_bytes": 4194304}"#;
        let config: PostgresCdcBatchConfig = serde_json::from_str(json).unwrap();

        assert_eq!(config.max_bytes, 4 * 1024 * 1024);
        assert_eq!(config.max_fill_ms, default_batch_max_fill_ms());
        assert_eq!(
            config.memory_budget_ratio,
            default_batch_memory_budget_ratio()
        );
        config.validate().unwrap();
    }

    #[test]
    fn postgres_cdc_batch_config_rejects_zero_memory_budget_ratio() {
        let config = PostgresCdcBatchConfig {
            memory_budget_ratio: 0.0,
            ..Default::default()
        };

        let err = config.validate().unwrap_err();
        assert!(err.contains("batch.memory_budget_ratio"));
    }

    #[test]
    fn postgres_cdc_batch_config_rejects_out_of_range_memory_budget_ratio() {
        let config = PostgresCdcBatchConfig {
            memory_budget_ratio: 1.5,
            ..Default::default()
        };

        let err = config.validate().unwrap_err();
        assert!(err.contains("batch.memory_budget_ratio"));
    }

    #[test]
    fn postgres_cdc_batch_config_rejects_zero_max_bytes() {
        let config = PostgresCdcBatchConfig {
            max_bytes: 0,
            ..Default::default()
        };

        let err = config.validate().unwrap_err();
        assert!(err.contains("batch.max_bytes"));
    }

    #[test]
    fn postgres_cdc_memory_backpressure_config_rejects_zero_activate_threshold() {
        let config = PostgresCdcMemoryBackpressureConfig {
            activate_threshold: 0.0,
            ..Default::default()
        };

        let err = config.validate().unwrap_err();
        assert!(err.contains("memory_backpressure.activate_threshold"));
    }

    #[test]
    fn postgres_cdc_memory_backpressure_config_rejects_resume_threshold_of_one() {
        let config = PostgresCdcMemoryBackpressureConfig {
            resume_threshold: 1.0,
            ..Default::default()
        };

        let err = config.validate().unwrap_err();
        assert!(err.contains("memory_backpressure.resume_threshold"));
    }

    #[test]
    fn postgres_cdc_memory_backpressure_config_rejects_resume_gte_activate() {
        let config = PostgresCdcMemoryBackpressureConfig {
            activate_threshold: 0.5,
            resume_threshold: 0.5,
        };

        let err = config.validate().unwrap_err();
        assert!(err.contains("must be lower than"));
    }

    #[test]
    fn postgres_cdc_config_rejects_invalid_batch_config() {
        let mut config = postgres_cdc_config(PostgresTlsConfig::default());
        config.batch.max_bytes = 0;

        let err = config.validate().unwrap_err();
        assert!(err.contains("batch.max_bytes"));
    }

    #[test]
    fn postgres_cdc_config_rejects_invalid_memory_backpressure_config() {
        let mut config = postgres_cdc_config(PostgresTlsConfig::default());
        config.memory_backpressure.activate_threshold = 0.0;

        let err = config.validate().unwrap_err();
        assert!(err.contains("memory_backpressure.activate_threshold"));
    }
}
