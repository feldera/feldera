//! SQL program schema output by the SQL compiler.
//
// FIXME: There is another Rust spec of the same JSON schema
// in `pipeline_manager/src/db/mod.rs`.  That one also defines
// proptest strategies for schema types, which are tricky to
// move here because crates cannot easily export code gated
// by `cfg(test)`.  It's doable, but it seems easier to duplicate
// the code.

use serde::Deserialize;

/// SQL program schema consisting of input and output relations.
///
/// This is a type-safe representation of the JSON schema file
/// output by the SQL compiler.
#[derive(Deserialize)]
pub struct ProgramSchema {
    #[serde(default)]
    pub inputs: Vec<TableSchema>,
    #[serde(default)]
    pub outputs: Vec<TableSchema>,
}

/// SQL table or view schema.
#[derive(Deserialize)]
pub struct TableSchema {
    pub name: String,
    pub fields: Vec<ColumnSchema>,
    pub primary_key: Option<Vec<String>>,
}

/// Table column schema.
#[derive(Deserialize)]
pub struct ColumnSchema {
    pub name: String,
    pub columntype: ColumnType,
}

#[derive(Deserialize)]
pub struct ColumnType {
    #[serde(rename = "type")]
    pub typ: String,
}
