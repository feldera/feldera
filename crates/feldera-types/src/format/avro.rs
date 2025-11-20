use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt::Display};
use utoipa::ToSchema;

/// Supported Avro data change event formats.
#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq, ToSchema, Default)]
pub enum AvroUpdateFormat {
    /// Raw encoding.
    ///
    /// Each message in the stream represents a single-record update: an insert, upsert, or delete.
    ///
    /// ### Input Connectors
    /// Raw Avro encoding can be used for insert and upsert operations, but not deletes.
    /// - The message value contains the record to be inserted or updated.
    /// - The message key and headers are ignored.
    ///
    /// ### Output Connectors
    /// The raw format supports inserts, upserts, and deletes.
    /// - The message value contains the record to be inserted or deleted.
    /// - The operation type is specified in the `op` message header field, which can be
    ///   `insert`, `update`, or `delete`.
    /// - The message key can optionally store the primary key (see the `key_mode` property).
    #[serde(rename = "raw")]
    #[default]
    Raw,

    /// Debezium data change event format.
    #[serde(rename = "debezium")]
    Debezium,

    /// Confluent JDBC connector change event format.
    #[serde(rename = "confluent_jdbc")]
    ConfluentJdbc,
}

impl Display for AvroUpdateFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Raw => f.write_str("raw"),
            Self::Debezium => f.write_str("debezium"),
            Self::ConfluentJdbc => f.write_str("confluent_jdbc"),
        }
    }
}

/// Schema registry configuration.
#[derive(Clone, Serialize, Deserialize, Debug, Default, ToSchema)]
pub struct AvroSchemaRegistryConfig {
    /// List of schema registry URLs.
    ///
    /// * **Input connector**: When non-empty, the connector retrieves Avro
    ///   message schemas from the registry.
    ///
    /// * **Output connector**: When non-empty, the connector will
    ///   post the schema to the registry and embed the schema id returned
    ///   by the registry in Avro messages.  Otherwise, schema id 0 is used.
    #[serde(default)]
    pub registry_urls: Vec<String>,

    /// Custom headers that will be added to every call to the schema registry.
    ///
    /// This property is only applicable to output connectors.
    ///
    /// Requires `registry_urls` to be set.
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

    /// Avro schema used to encode all records in this stream, specified as a JSON-encoded string.
    ///
    /// When this property is set, the connector uses the provided schema instead of
    /// retrieving the schema from the schema registry. This setting is mutually exclusive
    /// with `registry_urls`.
    pub schema: Option<String>,

    /// `true` if serialized messages only contain raw data without the
    /// header carrying schema ID.
    ///
    /// See <https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format>
    ///
    /// The default value is `false`.
    #[serde(default)]
    pub skip_schema_id: bool,

    /// Schema registry configuration.
    #[serde(flatten)]
    pub registry_config: AvroSchemaRegistryConfig,
}

/// Subject name strategies used in registering key and value schemas
/// with the schema registry.
#[derive(Clone, Serialize, Deserialize, Debug, ToSchema)]
pub enum SubjectNameStrategy {
    /// The subject name is derived directly from the Kafka topic name.
    ///
    /// For update formats with both key and value components, use subject names
    /// `{topic_name}-key` and `{topic_name}-value` for key and value schemas respectively.
    /// For update formats without a key (e.g., `raw`), publish value schema
    /// under the subject name `{topic_name}`.
    ///
    /// Only applicable when using Kafka as a transport.
    #[serde(rename = "topic_name")]
    TopicName,

    /// The name of the SQL relation that the schema is derived from is used as the subject name:
    /// * the SQL view name for the message value schema.
    /// * the SQL index name for the message key schema.
    #[serde(rename = "record_name")]
    RecordName,

    /// Combines both the topic name and the record name to form the subject.
    ///
    /// For update formats with both key and value components, use subject names
    /// `{topic_name}-{record_name}-key` and `{topic_name}-{record_name}-value` for
    /// key and value schemas respectively.
    /// For update formats without a key (e.g., `raw`), publish value schema
    /// under the subject name `{topic_name}-{record_name}`.
    ///
    /// `{record_name}` is the name of the SQL view or index that this connector
    /// is attached to.
    ///
    /// Only applicable when using Kafka as a transport.
    #[serde(rename = "topic_record_name")]
    TopicRecordName,
}

/// Determines how the message key is generated when the Avro encoder is configured
/// in the `raw` mode.
#[derive(Clone, Serialize, Deserialize, Debug, ToSchema, PartialEq, Eq)]
pub enum AvroEncoderKeyMode {
    /// Produce messages without a key.
    #[serde(rename = "none")]
    None,

    /// Uses the unique key columns of the view as the message key.
    ///
    /// This setting is supported when the output connector is configured with the `index` property.
    /// It utilizes the values of the index columns specified in the associated `CREATE INDEX` statement
    /// as the Avro message key.
    ///
    /// A separate Avro schema will be created and registered in the schema registry
    /// for the key component of the message.
    #[serde(rename = "key_fields")]
    KeyFields,
}

/// Avro output format configuration.
#[derive(Clone, Serialize, Deserialize, Debug, Default, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct AvroEncoderConfig {
    /// Format used to encode data change events in this stream.
    ///
    /// The default value is `raw`.
    #[serde(default)]
    pub update_format: AvroUpdateFormat,

    /// Determines how the message key is generated when the Avro encoder is configured
    /// in the `raw` mode.
    ///
    /// The default is `key_fields` when the `index` property of the connector is configured and `none` otherwise.
    pub key_mode: Option<AvroEncoderKeyMode>,

    /// Avro schema used to encode output records.
    ///
    /// When specified, the encoder will use this schema; otherwise it will automatically
    /// generate an Avro schema based on the SQL view definition.
    ///
    /// Specified as a string containing schema definition in JSON format.
    /// This schema must match precisely the SQL view definition, modulo
    /// nullability of columns.
    pub schema: Option<String>,

    /// Optional name of the field used for Change Data Capture (CDC) annotations.
    ///
    /// Use this setting with data sinks that expect operation type
    /// (insert, delete, or update) encoded as a column in the Avro record, such
    /// as the [Iceberg Sink Kafka Connector](https://docs.feldera.com/connectors/sinks/iceberg).
    ///
    /// When set (e.g., `"cdc_field": "op"`), the specified field will be added to each record
    /// to indicate the type of change:
    /// - `"I"` for insert operations
    /// - `"U"` for upserts
    /// - `"D"` for deletions
    ///
    /// If not set, CDC metadata will not be included in the records.
    /// Only works with the `raw` update format.
    pub cdc_field: Option<String>,

    /// Avro namespace for the generated Avro schemas.
    pub namespace: Option<String>,

    /// Subject name strategy used to publish Avro schemas used by the connector
    /// in the schema registry.
    ///
    /// When this property is not specified, the connector chooses subject name strategy automatically:
    /// * `topic_name` for `confluent_jdbc` update format
    /// * `record_name` for `raw` update format
    pub subject_name_strategy: Option<SubjectNameStrategy>,

    /// Set to `true` if serialized messages should only contain raw data
    /// without the header carrying schema ID.
    /// `False` by default.
    ///
    /// See <https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format>
    #[serde(default)]
    pub skip_schema_id: bool,

    /// Schema registry configuration.
    ///
    /// When configured, the connector will push the Avro schema, whether it is specified as part of
    /// connector configuration or generated automatically, to the schema registry and use the schema id
    /// assigned by the registry in the
    #[serde(flatten)]
    pub registry_config: AvroSchemaRegistryConfig,
}
