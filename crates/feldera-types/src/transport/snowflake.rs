use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use utoipa::ToSchema;

/// Snowflake table read mode.
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub enum SnowflakeIngestMode {
    /// Read a snapshot of the table and stop.
    #[default]
    #[serde(rename = "snapshot")]
    Snapshot,
}

/// Snowflake authentication method.
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub enum SnowflakeAuthenticator {
    /// Key-pair authentication using a JWT signed with an RSA private key.
    #[default]
    #[serde(rename = "SNOWFLAKE_JWT")]
    SnowflakeJwt,
}

/// Snowflake input connector transaction mode.
///
/// Determines whether the connector wraps snapshot ingestion in a Feldera
/// transaction.
///
/// * `none` - the connector does not start or commit transactions.
/// * `snapshot` - the connector ingests the snapshot in one Feldera
///   transaction.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Deserialize, Serialize, ToSchema, Default)]
pub enum SnowflakeTransactionMode {
    /// Do not request Feldera transaction boundaries.
    #[default]
    #[serde(rename = "none")]
    None,

    /// Ingest the snapshot in one Feldera transaction.
    ///
    /// If snapshot ingestion fails after records have been queued, the partial
    /// transaction is committed because connector transactions do not support
    /// rollback. Atomic rollback on failure is not currently supported.
    #[serde(rename = "snapshot")]
    Snapshot,
}

/// Representation of scaled Snowflake `NUMBER` values.
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub enum SnowflakeNumberMode {
    /// Convert scaled `NUMBER` values to Arrow Decimal128, preserving precision.
    #[default]
    #[serde(rename = "decimal")]
    Decimal,

    /// Convert scaled `NUMBER` values to Arrow Float64.
    ///
    /// This can lose precision. Scale-zero `NUMBER` values remain integral.
    #[serde(rename = "double")]
    Double,
}

const DEFAULT_MAX_CONCURRENT_READERS: u32 = 4;

fn default_num_parsers() -> u32 {
    4
}

/// Snowflake input connector configuration.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct SnowflakeReaderConfig {
    /// Snowflake account identifier.
    ///
    /// Uses the same account identifier accepted by Snowflake drivers, for
    /// example `"org-account"` or `"xy12345.us-east-1"`.
    pub account: String,

    /// Snowflake login name.
    pub user: String,

    /// Snowflake authenticator.
    ///
    /// Defaults to `"SNOWFLAKE_JWT"`.
    #[serde(default)]
    pub authenticator: SnowflakeAuthenticator,

    /// Snowflake role to use for the session.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,

    /// Snowflake warehouse to use for the session.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warehouse: Option<String>,

    /// Snowflake database to use for the session.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,

    /// Snowflake schema to use for the session.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,

    /// RSA private key file for `SNOWFLAKE_JWT` authentication.
    ///
    /// The file must contain a PEM-encoded PKCS#8 RSA private key.
    pub private_key_file: String,

    /// RSA private key passphrase for `SNOWFLAKE_JWT` authentication.
    ///
    /// Required when `private_key_file` contains an encrypted PKCS#8 private
    /// key.
    #[serde(
        default,
        alias = "private_key_passphrase",
        alias = "private_key_file_password",
        skip_serializing_if = "Option::is_none"
    )]
    pub private_key_file_pwd: Option<String>,

    /// Source table name.
    ///
    /// The value can be a fully-qualified Snowflake table name, for example
    /// `"MY_DATABASE.MY_SCHEMA.MY_TABLE"`, or an unqualified table name when
    /// `database` and `schema` are configured on the Snowflake session.
    pub table: String,

    /// Map Feldera table columns to Snowflake source columns.
    ///
    /// Each key is the bare name of a column in the Feldera SQL table this
    /// connector is attached to (without SQL quote delimiters). Each value is
    /// the corresponding Snowflake SQL column identifier. The connector aliases
    /// mapped columns back to their Feldera names in the generated snapshot
    /// query. Quote a value as a Snowflake SQL identifier when its spelling is
    /// case-sensitive.
    ///
    /// For example, `{"uuid": "UUID"}` reads the Snowflake column `UUID`
    /// into the Feldera column `"uuid"`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub column_mapping: BTreeMap<String, String>,

    /// Representation of scaled Snowflake `NUMBER` values.
    ///
    /// Defaults to `"decimal"`, which preserves precision. Set to `"double"`
    /// to convert these values to double-precision floating point.
    #[serde(default)]
    pub number_mode: SnowflakeNumberMode,

    /// Table read mode.
    ///
    /// Only `"snapshot"` is supported.
    #[serde(default)]
    pub mode: SnowflakeIngestMode,

    /// Transaction mode.
    ///
    /// Determines whether the connector wraps snapshot ingestion in a Feldera
    /// transaction. Defaults to `"none"`.
    #[serde(default)]
    pub transaction_mode: SnowflakeTransactionMode,

    /// Optional row filter.
    ///
    /// When specified, only rows that satisfy this predicate are included in
    /// the snapshot. The predicate is appended to the generated Snowflake query
    /// as the body of a `WHERE` clause.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_filter: Option<String>,

    /// Number of parallel tasks used to deserialize Arrow record batches.
    ///
    /// Recommended range: 1–10. Default: 4.
    #[serde(default = "default_num_parsers")]
    #[schema(minimum = 1)]
    pub num_parsers: u32,

    /// Maximum number of concurrent reads of Snowflake result chunks.
    ///
    /// Snowflake returns large result sets as multiple Arrow IPC chunks stored
    /// behind pre-signed URLs. Increasing this value can improve snapshot
    /// throughput when chunk download/decompression is the bottleneck.
    ///
    /// Default: 4.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(minimum = 1)]
    pub max_concurrent_readers: Option<u32>,
}

impl SnowflakeReaderConfig {
    pub fn validate(&self) -> Result<(), String> {
        for (name, value) in [
            ("account", &self.account),
            ("user", &self.user),
            ("private_key_file", &self.private_key_file),
            ("table", &self.table),
        ] {
            if value.trim().is_empty() {
                return Err(format!("'{name}' must not be empty"));
            }
        }

        for (feldera_column, snowflake_column) in &self.column_mapping {
            if feldera_column.trim().is_empty() {
                return Err("'column_mapping' contains an empty Feldera column name".to_string());
            }
            if snowflake_column.trim().is_empty() {
                return Err(format!(
                    "'column_mapping' contains an empty Snowflake column name for Feldera column '{feldera_column}'"
                ));
            }
        }

        for (name, value) in [
            ("role", &self.role),
            ("warehouse", &self.warehouse),
            ("database", &self.database),
            ("schema", &self.schema),
            ("private_key_file_pwd", &self.private_key_file_pwd),
        ] {
            if value.as_ref().is_some_and(|value| value.trim().is_empty()) {
                return Err(format!("'{name}' must not be empty when specified"));
            }
        }

        if self
            .snapshot_filter
            .as_ref()
            .is_some_and(|filter| filter.trim().is_empty())
        {
            return Err("'snapshot_filter' must not be empty when specified".to_string());
        }

        if self
            .max_concurrent_readers
            .is_some_and(|readers| readers == 0)
        {
            return Err("'max_concurrent_readers' must be greater than 0".to_string());
        }

        if self.num_parsers == 0 {
            return Err("'num_parsers' must be greater than 0".to_string());
        }

        Ok(())
    }

    pub fn max_concurrent_readers(&self) -> usize {
        self.max_concurrent_readers
            .unwrap_or(DEFAULT_MAX_CONCURRENT_READERS) as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn minimal_config() -> SnowflakeReaderConfig {
        SnowflakeReaderConfig {
            account: "org-account".to_string(),
            user: "svc_user".to_string(),
            authenticator: SnowflakeAuthenticator::SnowflakeJwt,
            role: None,
            warehouse: None,
            database: None,
            schema: None,
            private_key_file: "/secrets/key.p8".to_string(),
            private_key_file_pwd: None,
            table: "DB.SCHEMA.TABLE".to_string(),
            column_mapping: BTreeMap::new(),
            number_mode: SnowflakeNumberMode::Decimal,
            mode: SnowflakeIngestMode::Snapshot,
            transaction_mode: SnowflakeTransactionMode::None,
            snapshot_filter: None,
            num_parsers: default_num_parsers(),
            max_concurrent_readers: None,
        }
    }

    #[test]
    fn validates_minimal_config() {
        let config = minimal_config();
        config.validate().unwrap();
        assert_eq!(config.max_concurrent_readers(), 4);
        assert_eq!(config.num_parsers, 4);
    }

    #[test]
    fn rejects_zero_concurrent_readers() {
        let mut config = minimal_config();
        config.max_concurrent_readers = Some(0);
        assert_eq!(
            config.validate().unwrap_err(),
            "'max_concurrent_readers' must be greater than 0"
        );
    }

    #[test]
    fn rejects_zero_parsers() {
        let mut config = minimal_config();
        config.num_parsers = 0;
        assert_eq!(
            config.validate().unwrap_err(),
            "'num_parsers' must be greater than 0"
        );
    }

    #[test]
    fn parses_snapshot_transaction_mode() {
        let config: SnowflakeReaderConfig = serde_json::from_str(
            r#"{
                "account": "org-account",
                "user": "svc_user",
                "private_key_file": "/secrets/key.p8",
                "table": "DB.SCHEMA.TABLE",
                "transaction_mode": "snapshot"
            }"#,
        )
        .unwrap();

        assert_eq!(config.transaction_mode, SnowflakeTransactionMode::Snapshot);
        assert_eq!(config.number_mode, SnowflakeNumberMode::Decimal);
        config.validate().unwrap();
    }

    #[test]
    fn accepts_private_key_passphrase_aliases() {
        let config: SnowflakeReaderConfig = serde_json::from_str(
            r#"{
                "account": "org-account",
                "user": "svc_user",
                "private_key_file": "/secrets/key.p8",
                "private_key_passphrase": "secret",
                "table": "DB.SCHEMA.TABLE"
            }"#,
        )
        .unwrap();

        assert_eq!(config.private_key_file_pwd.as_deref(), Some("secret"));
        config.validate().unwrap();
    }

    #[test]
    fn parses_column_mapping() {
        let config: SnowflakeReaderConfig = serde_json::from_str(
            r#"{
                "account": "org-account",
                "user": "svc_user",
                "private_key_file": "/secrets/key.p8",
                "table": "DB.SCHEMA.TABLE",
                "column_mapping": {
                    "uuid": "UUID"
                }
            }"#,
        )
        .unwrap();

        assert_eq!(config.column_mapping.get("uuid").unwrap(), "UUID");
        config.validate().unwrap();
    }

    #[test]
    fn parses_number_mode() {
        let config: SnowflakeReaderConfig = serde_json::from_str(
            r#"{
                "account": "org-account",
                "user": "svc_user",
                "private_key_file": "/secrets/key.p8",
                "table": "DB.SCHEMA.TABLE",
                "number_mode": "double"
            }"#,
        )
        .unwrap();

        assert_eq!(config.number_mode, SnowflakeNumberMode::Double);
    }

    #[test]
    fn rejects_empty_column_mapping_names() {
        let mut config = minimal_config();
        config
            .column_mapping
            .insert("uuid".to_string(), String::new());
        assert!(
            config
                .validate()
                .unwrap_err()
                .contains("Snowflake column name")
        );

        let mut config = minimal_config();
        config
            .column_mapping
            .insert(" ".to_string(), "UUID".to_string());
        assert!(
            config
                .validate()
                .unwrap_err()
                .contains("Feldera column name")
        );
    }
}
