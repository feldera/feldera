use pipeline_types::json::JsonFlavor;

use super::schema::{ColumnSchema, TableSchema};
use dataflow_jit::{
    codegen::json::{JsonColumn, JsonDeserConfig, JsonSerConfig},
    ir::LayoutId,
};

fn column_from_schema(column: &ColumnSchema, slash: bool, flavor: &JsonFlavor) -> JsonColumn {
    let slash = if slash { "/" } else { "" };
    match (column.columntype.typ.as_str(), flavor) {
        ("DATE", JsonFlavor::Default) => {
            JsonColumn::datetime(format!("{slash}{}", column.name), "%Y-%m-%d")
        }
        ("DATE", JsonFlavor::DebeziumMySql) => {
            JsonColumn::date_from_days(format!("{slash}{}", column.name))
        }
        ("TIME", JsonFlavor::Default) => {
            JsonColumn::datetime(format!("{slash}{}", column.name), "%H:%M:%S%.f")
        }
        ("TIME", JsonFlavor::DebeziumMySql) => {
            JsonColumn::time_from_micros(format!("{slash}{}", column.name))
        }
        ("TIMESTAMP", JsonFlavor::Default) => {
            JsonColumn::datetime(format!("{slash}{}", column.name), "%F %T%.f")
        }
        ("TIMESTAMP", JsonFlavor::DebeziumMySql) => {
            JsonColumn::datetime(format!("{slash}{}", column.name), "%Y-%m-%dT%H:%M:%S%Z")
        }
        _ => JsonColumn::normal(format!("{slash}{}", column.name)),
    }
}

/// Build JSON deserializer configuration for specified layout and table schema.
pub(crate) fn build_json_deser_config(
    layout: LayoutId,
    table_schema: &TableSchema,
    flavor: &JsonFlavor,
) -> JsonDeserConfig {
    let mappings = table_schema
        .fields
        .iter()
        .enumerate()
        .map(|(index, column)| (index, column_from_schema(column, true, flavor)))
        .collect();
    JsonDeserConfig { layout, mappings }
}

/// Build JSON serializer configuration for specified layout and table schema.
pub(crate) fn build_json_ser_config(layout: LayoutId, table_schema: &TableSchema) -> JsonSerConfig {
    let mappings = table_schema
        .fields
        .iter()
        .enumerate()
        .map(|(index, column)| {
            (
                index,
                column_from_schema(column, false, &JsonFlavor::Default),
            )
        })
        .collect();
    JsonSerConfig { layout, mappings }
}
