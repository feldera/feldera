use pipeline_types::config::PipelineFormatConfig;
use pipeline_types::format::csv::CsvParserConfig;
use pipeline_types::format::json::JsonParserConfig;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use utoipa::ToSchema;

/// Format configuration.
///
/// The connector format indicates in what format data is being input
/// into or output by the connector (depending on if it is an input or
/// output connector).
///
/// Not all connectors support all formats, as some either fundamentally
/// cannot, or for others because it does not make sense.
/// For example, some data sources changes over time, whereas others are
/// simply immutable streams of tuples. The former cannot just supply
/// data rows as input as then it cannot convey if it is a insert, delete,
/// or update. For the latter conveying the operation is unnecessary
/// overhead as all of them are appends.
///
/// As such, only some API connectors allow specifying blankly any format
/// configuration. For those connectors (e.g., URL, Kafka), this
/// enumeration with the variants straight from the pipeline types is
/// made available.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
// snake_case such that the enumeration variants are not capitalized in (de-)serialization
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
pub enum FormatConfig {
    Csv(CsvParserConfig),
    Json(JsonParserConfig),
}

impl FormatConfig {
    /// Converts the API connector format configuration
    /// to the pipeline connector format configuration.
    /// It is a 1-to-1 mapping.
    pub fn to_pipeline_format_config(&self) -> PipelineFormatConfig {
        match self {
            FormatConfig::Csv(config) => PipelineFormatConfig {
                name: Cow::from("csv"),
                config: serde_yaml::to_value(config).unwrap(),
            },
            FormatConfig::Json(config) => PipelineFormatConfig {
                name: Cow::from("json"),
                config: serde_yaml::to_value(config).unwrap(),
            },
        }
    }
}
