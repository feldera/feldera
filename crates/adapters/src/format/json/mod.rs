use serde::{Deserialize, Serialize};

mod input;
mod output;

pub use input::{JsonInputFormat, JsonParserConfig};
pub use output::{JsonEncoderConfig, JsonOutputFormat};
use utoipa::ToSchema;

/// Supported JSON data change event formats.
///
/// Each element in a JSON-formatted input stream specifies
/// an update to one or more records in an input table.  We support
/// several different ways to represent such updates.
#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq, ToSchema)]
pub enum JsonUpdateFormat {
    /// Insert/delete format.
    ///
    /// Each element in the input stream consists of an "insert" or "delete"
    /// command and a record to be inserted to or deleted from the input table.
    ///
    /// # Example
    ///
    /// ```json
    /// {"insert": {"column1": "hello, world!", "column2": 100}}
    /// ```
    #[serde(rename = "insert_delete")]
    InsertDelete,

    #[serde(rename = "weighted")]
    Weighted,

    /// Simplified Debezium CDC format.
    ///
    /// We support a simplified version of the Debezium CDC format.  All fields
    /// except `payload` are ignored.
    ///
    /// # Example
    ///
    /// ```json
    /// {"payload": {"op": "u", "before": {"b": true, "i": 123}, "after": {"b": true, "i": 0}}}
    /// ```
    #[serde(rename = "debezium")]
    Debezium,

    /// Raw input format.
    ///
    /// This format is suitable for insert-only streams (no deletions).
    /// Each element in the input stream contains a record without any
    /// additional envelope that gets inserted in the input table.
    #[serde(rename = "raw")]
    Raw,
}

impl Default for JsonUpdateFormat {
    fn default() -> Self {
        Self::InsertDelete
    }
}

/// Debezium CDC operation.
///
/// A record in a Debezium CDC stream contains an `op` field, which specifies
/// one of create ("c"), delete ("d") or update ("u") operations.
#[derive(Debug, Deserialize)]
pub enum DebeziumOp {
    #[serde(rename = "c")]
    Create,
    #[serde(rename = "d")]
    Delete,
    #[serde(rename = "u")]
    Update,
    #[serde(rename = "r")]
    Read,
}

/// Debezium CDC source specification describes the origin of the record,
/// including the name of the table the record belongs to.
#[derive(Debug, Deserialize)]
pub struct DebeziumSource {
    #[allow(dead_code)]
    table: String,
}

/// A Debezium data change event.
///
/// Only the `payload` field is currently supported; other fields are ignored.
#[derive(Debug, Deserialize)]
pub struct DebeziumUpdate<T> {
    payload: DebeziumPayload<T>,
}

/// Schema of the `payload` field of a Debezium data change event.
#[derive(Debug, Deserialize)]
pub struct DebeziumPayload<T> {
    // source: Option<DebeziumSource>,
    #[allow(dead_code)]
    op: DebeziumOp,
    /// When present and not `null`, this field specifies a record to be deleted
    /// from the table.
    before: Option<T>,
    /// When present and not `null`, this field specifies a record to be
    /// inserted to the table.
    after: Option<T>,
}

/// A data change event in the insert/delete format.
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct InsDelUpdate<T> {
    // This field is currently ignored.  We will add support for it in the future.
    #[doc(hidden)]
    #[allow(dead_code)]
    #[serde(skip_serializing_if = "Option::is_none")]
    table: Option<String>,
    /// When present and not `null`, this field specifies a record to be
    /// inserted to the table.
    #[serde(skip_serializing_if = "Option::is_none")]
    insert: Option<T>,
    /// When present and not `null`, this field specifies a record to be deleted
    /// from the table.
    #[serde(skip_serializing_if = "Option::is_none")]
    delete: Option<T>,
}

// TODO: implement support for parsing this format.
/// A data change event in the weighted update format.
#[doc(hidden)]
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[allow(dead_code)]
pub struct WeightedUpdate<T: ?Sized> {
    #[serde(skip_serializing_if = "Option::is_none")]
    table: Option<String>,
    weight: i64,
    data: T,
}
