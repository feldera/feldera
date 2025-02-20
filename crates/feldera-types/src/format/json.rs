use serde::{Deserialize, Serialize};
use std::fmt::Display;
use utoipa::ToSchema;

/// JSON parser configuration.
///
/// Describes the shape of an input JSON stream.
///
/// # Examples
///
/// A configuration with `update_format="raw"` and `array=false`
/// is used to parse a stream of JSON objects without any envelope
/// that get inserted in the input table.
///
/// ```json
/// {"b": false, "i": 100, "s": "foo"}
/// {"b": true, "i": 5, "s": "bar"}
/// ```
///
/// A configuration with `update_format="insert_delete"` and
/// `array=false` is used to parse a stream of JSON data change events
/// in the insert/delete format:
///
/// ```json
/// {"delete": {"b": false, "i": 15, "s": ""}}
/// {"insert": {"b": false, "i": 100, "s": "foo"}}
/// ```
///
/// A configuration with `update_format="insert_delete"` and
/// `array=true` is used to parse a stream of JSON arrays
/// where each array contains multiple data change events in
/// the insert/delete format.
///
/// ```json
/// [{"insert": {"b": true, "i": 0}}, {"delete": {"b": false, "i": 100, "s": "foo"}}]
/// ```
#[derive(Clone, Debug, Default, Deserialize, Serialize, ToSchema)]
#[serde(default)]
pub struct JsonParserConfig {
    /// JSON update format.
    pub update_format: JsonUpdateFormat,

    /// Specifies JSON encoding used for individual table records.
    pub json_flavor: JsonFlavor,

    /// Set to `true` if updates in this stream are packaged into JSON arrays.
    ///
    /// # Example
    ///
    /// ```json
    /// [{"b": true, "i": 0},{"b": false, "i": 100, "s": "foo"}]
    /// ```
    pub array: bool,

    /// Whether JSON elements can span multiple lines.
    ///
    /// This only affects JSON input.
    pub lines: JsonLines,
}

/// Whether JSON values can span multiple lines.
#[derive(Deserialize, Serialize, Clone, Debug, Default, PartialEq, Eq, ToSchema)]
pub enum JsonLines {
    /// JSON values may span multiple lines.
    ///
    /// This supports general-purpose JSON input.
    #[default]
    #[serde(rename = "multiple")]
    Multiple,

    /// A given JSON value never contains a new-line.
    ///
    /// Suitable for parsing [NDJSON](https://github.com/ndjson/ndjson-spec).
    /// Lines are allowed to contain multiple JSON values.  The parser ignores
    /// empty lines.
    #[serde(rename = "single")]
    Single,
}

/// Supported JSON data change event formats.
///
/// Each element in a JSON-formatted input stream specifies
/// an update to one or more records in an input table.  We support
/// several different ways to represent such updates.
///
/// ### `InsertDelete`
///
/// Each element in the input stream consists of an "insert" or "delete"
/// command and a record to be inserted to or deleted from the input table.
///
/// ```json
/// {"insert": {"column1": "hello, world!", "column2": 100}}
/// ```
///
/// ### `Weighted`
///
/// Each element in the input stream consists of a record and a weight
/// which indicates how many times the row appears.
///
/// ```json
/// {"weight": 2, "data": {"column1": "hello, world!", "column2": 100}}
/// ```
///
/// Note that the line above would be equivalent to the following input in the `InsertDelete` format:
///
/// ```json
/// {"insert": {"column1": "hello, world!", "column2": 100}}
/// {"insert": {"column1": "hello, world!", "column2": 100}}
/// ```
///
/// Similarly, negative weights are equivalent to deletions:
///
/// ```json
/// {"weight": -1, "data": {"column1": "hello, world!", "column2": 100}}
/// ```
///
/// is equivalent to in the `InsertDelete` format:
///
/// ```json
/// {"delete": {"column1": "hello, world!", "column2": 100}}
/// ```
///
/// ### `Debezium`
///
/// Debezium CDC format.  Refer to [Debezium input connector documentation](https://docs.feldera.com/connectors/sources/debezium) for details.
///
/// ### `Snowflake`
///
/// Uses flat structure so that fields can get parsed directly into SQL
/// columns.  Defines three metadata fields:
///
/// * `__action` - "insert" or "delete"
/// * `__stream_id` - unique 64-bit ID of the output stream (records within
///   a stream are totally ordered)
/// * `__seq_number` - monotonically increasing sequence number relative to
///   the start of the stream.
///
/// ```json
/// {"PART":1,"VENDOR":2,"EFFECTIVE_SINCE":"2019-05-21","PRICE":"10000","__action":"insert","__stream_id":4523666124030717756,"__seq_number":1}
/// ```
///
/// ### `Raw`
///
/// This format is suitable for insert-only streams (no deletions).
/// Each element in the input stream contains a record without any
/// additional envelope that gets inserted in the input table.
#[derive(Deserialize, Serialize, Clone, Debug, Default, PartialEq, Eq, ToSchema)]
pub enum JsonUpdateFormat {
    #[default]
    #[serde(rename = "insert_delete")]
    InsertDelete,

    #[serde(rename = "weighted")]
    Weighted,

    #[serde(rename = "debezium")]
    Debezium,

    #[serde(rename = "snowflake")]
    Snowflake,

    #[serde(rename = "raw")]
    Raw,

    #[serde(rename = "redis")]
    Redis,
}

impl Display for JsonUpdateFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JsonUpdateFormat::InsertDelete => write!(f, "insert_delete"),
            JsonUpdateFormat::Weighted => write!(f, "weighted"),
            JsonUpdateFormat::Debezium => write!(f, "debezium"),
            JsonUpdateFormat::Snowflake => write!(f, "snowflake"),
            JsonUpdateFormat::Raw => write!(f, "raw"),
            JsonUpdateFormat::Redis => write!(f, "redis"),
        }
    }
}

/// Specifies JSON encoding used of table records.
#[derive(Clone, Default, Deserialize, Serialize, Debug, PartialEq, Eq, ToSchema)]
pub enum JsonFlavor {
    /// Default encoding used by Feldera, documented
    /// [here](https://docs.feldera.com/formats/json#types).
    #[default]
    #[serde(rename = "default")]
    Default,
    /// Debezium MySQL JSON produced by the default configuration of the
    /// Debezium [Kafka Connect connector](https://debezium.io/documentation/reference/stable/connectors/mysql.html#mysql-data-types)
    /// with `decimal.handling.mode` set to "string".
    #[serde(rename = "debezium_mysql")]
    DebeziumMySql,
    /// Debezium Postgres JSON produced by the default configuration of the
    /// Debezium [Kafka Connect connector](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-data-types)
    /// with `decimal.handling.mode` set to "string".
    #[serde(rename = "debezium_postgres")]
    DebeziumPostgres,
    /// JSON format accepted by Snowflake using default settings.
    #[serde(rename = "snowflake")]
    Snowflake,
    /// JSON format accepted by the Kafka Connect `JsonConverter` class.
    #[serde(rename = "kafka_connect_json_converter")]
    KafkaConnectJsonConverter,
    #[serde(rename = "pandas")]
    Pandas,
    /// Parquet to-json format.
    /// (For internal use only)
    #[serde(skip)]
    ParquetConverter,
    /// Datagen format.
    /// (For internal use only)
    #[serde(rename = "datagen")]
    Datagen,
}

// TODO: support multiple update formats, e.g., `WeightedUpdate`
// supports arbitrary weights beyond `MAX_DUPLICATES`.
#[derive(Deserialize, Serialize, ToSchema)]
#[serde(default)]
pub struct JsonEncoderConfig {
    pub update_format: JsonUpdateFormat,
    pub json_flavor: Option<JsonFlavor>,
    pub buffer_size_records: usize,
    pub array: bool,

    /// When this option is set, only the listed fields appear in the Debezium message key.
    ///
    /// This option is useful when writing to a table with primary keys.
    /// For such tables, Debezium expects the message key to contain only
    /// the primary key columns.
    ///
    /// This option is only valid with the `debezium` update format.
    pub key_fields: Option<Vec<String>>,
}

impl Default for JsonEncoderConfig {
    fn default() -> Self {
        Self {
            update_format: JsonUpdateFormat::default(),
            json_flavor: None,
            buffer_size_records: 10_000,
            array: false,
            key_fields: None,
        }
    }
}
