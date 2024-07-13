use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use utoipa::ToSchema;

/// Supported Avro data change event formats.
#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq, ToSchema)]
pub enum AvroUpdateFormat {
    /// Raw encoding.
    ///
    /// Every event in the stream represents a single record to be stored
    /// in the table that the stream is connected to.  This format can represent
    /// inserts and upsert, but not detetes.
    #[serde(rename = "raw")]
    Raw,

    /// Debezium data change event format.
    #[serde(rename = "debezium")]
    Debezium,
}

impl Default for AvroUpdateFormat {
    fn default() -> Self {
        Self::Raw
    }
}

/// Schema registry configuration.
#[derive(Clone, Serialize, Deserialize, Debug, Default, ToSchema)]
pub struct AvroSchemaRegistryConfig {
    /// List of schema registry URLs. When non-empty, the connector will
    /// post the schema to the registry and use the schema id returned
    /// by the registry.  Otherwise, schema id 0 is used.
    #[serde(default)]
    pub registry_urls: Vec<String>,
    /// Custom headers that will be added to every call to the schema registry.
    ///
    /// This option requires `registry_urls` to be set.
    #[serde(default)]
    pub registry_headers: HashMap<String, String>,
    /// Proxy that will be used to access the schema registry.
    ///
    /// Requires `registry_urls` to be set.
    pub registry_proxy: Option<String>,
    /// Timeout in seconds used to connect to the registry.
    ///
    /// Requires `registry_urls` to be set.
    pub registry_timeout_secs: Option<u64>,
    /// Username used to authenticate with the registry.
    ///
    /// Requires `registry_urls` to be set. This option is mutually exclusive with
    /// token-based authentication (see `registry_authorization_token`).
    pub registry_username: Option<String>,
    /// Password used to authenticate with the registry.
    ///
    /// Requires `registry_urls` to be set.
    pub registry_password: Option<String>,
    /// Token used to authenticate with the registry.
    ///
    /// Requires `registry_urls` to be set. This option is mutually exclusive with
    /// password-based authentication (see `registry_username` and `registry_password`).
    pub registry_authorization_token: Option<String>,
}

/// Avro output format configuration.
#[derive(Clone, Serialize, Deserialize, Debug, Default, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct AvroParserConfig {
    /// Format used to encode data change events in this stream.
    ///
    /// The default value is 'raw'.
    #[serde(default)]
    pub update_format: AvroUpdateFormat,

    /// Avro schema used to encode input records.
    ///
    /// Specified as a string containing schema definition in JSON format.
    /// This schema must match precisely the SQL view definition, including
    /// nullability of columns.
    pub schema: Option<String>,

    /// `true` if serialized messages only contain raw data without the
    /// header carrying schema ID.
    /// `False` by default.
    ///
    /// See <https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format>
    #[serde(default)]
    pub no_schema_id: bool,

    /// Schema registry configuration.
    #[serde(flatten)]
    pub registry_config: AvroSchemaRegistryConfig,
}

/// Avro output format configuration.
#[derive(Serialize, Deserialize, Debug, Default, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct AvroEncoderConfig {
    /// Avro schema used to encode output records.
    ///
    /// Specified as a string containing schema definition in JSON format.
    /// This schema must match precisely the SQL view definition, including
    /// nullability of columns.
    pub schema: String,

    /// Set to `true` if serialized messages should only contain raw data
    /// without the header carrying schema ID.
    /// `False` by default.
    ///
    /// See <https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format>
    #[serde(default)]
    pub skip_schema_id: bool,

    /// Schema registry configuration.
    #[serde(flatten)]
    pub registry_config: AvroSchemaRegistryConfig,
}
