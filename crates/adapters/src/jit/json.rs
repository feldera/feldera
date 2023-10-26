use pipeline_types::format::json::JsonFlavor;

use super::schema::{ColumnSchema, TableSchema};
use dataflow_jit::{
    codegen::json::{JsonColumn, JsonDeserConfig, JsonSerConfig},
    facade::Demands,
    ir::{stream_layout::StreamLayout, DemandId, LayoutId},
};

fn column_from_schema(column: &ColumnSchema, slash: bool, flavor: &JsonFlavor) -> JsonColumn {
    let slash = if slash { "/" } else { "" };
    match (column.columntype.typ.as_str(), flavor) {
        ("DATE", JsonFlavor::Default) | ("DATE", JsonFlavor::Snowflake) => {
            JsonColumn::datetime(format!("{slash}{}", column.name), "%Y-%m-%d")
        }
        ("DATE", JsonFlavor::DebeziumMySql) => {
            JsonColumn::date_from_days(format!("{slash}{}", column.name))
        }
        ("TIME", JsonFlavor::Default) | ("TIME", JsonFlavor::Snowflake) => {
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
        ("TIMESTAMP", JsonFlavor::Snowflake) => {
            JsonColumn::datetime(format!("{slash}{}", column.name), "%Y-%m-%dT%H:%M:%S%.f%:z")
        }
        _ => JsonColumn::normal(format!("{slash}{}", column.name)),
    }
}

pub(crate) struct JsonTableDeserConfig {
    row_config: JsonDeserConfig,
    primary_key_config: Option<JsonDeserConfig>,
}

pub(crate) struct JsonTableDeserDemands {
    pub(crate) row: DemandId,
    pub(crate) primary_key: Option<DemandId>,
}

impl JsonTableDeserConfig {
    pub(crate) fn create_demands(self, demands: &mut Demands) -> JsonTableDeserDemands {
        JsonTableDeserDemands {
            row: demands.add_json_deserialize(self.row_config),
            primary_key: self
                .primary_key_config
                .map(|config| demands.add_json_deserialize(config)),
        }
    }
}

/// Build JSON deserializer configuration for specified layout and table schema.
pub(crate) fn build_json_deser_config(
    layout: &StreamLayout,
    table_schema: &TableSchema,
    flavor: &JsonFlavor,
) -> JsonTableDeserConfig {
    let (value_layout, primary_key_layout) = match layout {
        StreamLayout::Set(value_layout) => (value_layout, None),
        StreamLayout::Map(key_layout, value_layout) => (value_layout, Some(key_layout)),
    };
    let mappings = table_schema
        .fields
        .iter()
        .enumerate()
        .map(|(index, column)| (index, column_from_schema(column, true, flavor)))
        .collect();
    let value_config = JsonDeserConfig {
        layout: *value_layout,
        mappings,
    };

    let primary_key_config = table_schema.primary_key.as_ref().map(|key_columns| {
        let mappings = key_columns
            .iter()
            .map(|column_name| {
                table_schema
                    .fields
                    .iter()
                    .enumerate()
                    .find(|(_index, field)| &field.name == column_name)
                    .unwrap_or_else(|| panic!("Primary key constraint for table '{}' includes column '{column_name}', which is not declared in the table schema", table_schema.name))
            })
            .map(|(index, column)| (index, column_from_schema(column, true, flavor)))
            .collect();
        JsonDeserConfig{
            layout: *primary_key_layout.unwrap_or_else(|| panic!("Table '{}' has a primary key, but specifies a Set layout instead of Map", table_schema.name)),
            mappings
        }
    });

    JsonTableDeserConfig {
        row_config: value_config,
        primary_key_config,
    }
}

/// Build JSON serializer configuration for specified layout and table schema.
pub(crate) fn build_json_ser_config(
    layout: LayoutId,
    table_schema: &TableSchema,
    flavor: &JsonFlavor,
) -> JsonSerConfig {
    let mappings = table_schema
        .fields
        .iter()
        .enumerate()
        .map(|(index, column)| (index, column_from_schema(column, false, flavor)))
        .collect();
    JsonSerConfig { layout, mappings }
}
