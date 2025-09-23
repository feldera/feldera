use serde::{Deserialize, Serialize};

mod input;
mod output;
mod schema;

pub use input::JsonInputFormat;
pub use output::JsonOutputFormat;

/// Debezium CDC operation.
///
/// A record in a Debezium CDC stream contains an `op` field, which specifies
/// one of create ("c"), delete ("d") or update ("u") operations.
#[derive(Debug, Deserialize, PartialOrd, Ord, PartialEq, Eq)]
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

/// A Debezium data change event.
///
/// Only the `payload` field is currently supported; other fields are ignored.
#[derive(Debug, Deserialize, PartialOrd, Ord, PartialEq, Eq)]
pub struct DebeziumUpdate<T> {
    payload: DebeziumPayload<T>,
}

/// Schema of the `payload` field of a Debezium data change event.
#[derive(Debug, Deserialize, PartialOrd, Ord, PartialEq, Eq)]
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
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
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
    /// When present and not `null`, this field specifies an update to an
    /// existing record.
    #[serde(skip_serializing_if = "Option::is_none")]
    update: Option<T>,
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
