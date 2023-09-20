use super::schema::{ColumnSchema, TableSchema};
use dataflow_jit::{
    codegen::json::{JsonColumn, JsonDeserConfig, JsonSerConfig},
    ir::LayoutId,
};

fn column_from_schema(column: &ColumnSchema, slash: bool) -> JsonColumn {
    let slash = if slash { "/" } else { "" };
    match column.columntype.typ.as_str() {
        "DATE" => JsonColumn::datetime(format!("{slash}{}", column.name), "%Y-%m-%d"),
        "TIME" => JsonColumn::datetime(format!("{slash}{}", column.name), "%H:%M:%S%.f"),
        "TIMESTAMP" => JsonColumn::datetime(format!("{slash}{}", column.name), "%F %T%.f"),
        _ => JsonColumn::normal(format!("{slash}{}", column.name)),
    }
}

/// Build JSON deserializer configuration for specified layout and table schema.
pub(crate) fn build_json_deser_config(
    layout: LayoutId,
    table_schema: &TableSchema,
) -> JsonDeserConfig {
    let mappings = table_schema
        .fields
        .iter()
        .enumerate()
        .map(|(index, column)| (index, column_from_schema(column, true)))
        .collect();
    JsonDeserConfig { layout, mappings }
}

/// Build JSON serializer configuration for specified layout and table schema.
pub(crate) fn build_json_ser_config(layout: LayoutId, table_schema: &TableSchema) -> JsonSerConfig {
    let mappings = table_schema
        .fields
        .iter()
        .enumerate()
        .map(|(index, column)| (index, column_from_schema(column, false)))
        .collect();
    JsonSerConfig { layout, mappings }
}
