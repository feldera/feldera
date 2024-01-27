//! Generate JSON schema for formats that require an explicit data schema.
//!
//! The schema can be inlined in each message or distributed separately, e.g.,
//! via a schema registry.

use crate::ControllerError;
use pipeline_types::format::json::{JsonEncoderConfig, JsonFlavor, JsonUpdateFormat};
use pipeline_types::program_schema::Relation;

/// Build a schema for the value component of the payload.
pub fn build_value_schema(
    config: &JsonEncoderConfig,
    schema: &Relation,
) -> Result<Option<String>, ControllerError> {
    match config.json_flavor {
        Some(JsonFlavor::KafkaConnectJsonConverter) => {
            if matches!(config.update_format, JsonUpdateFormat::Debezium) {
                Ok(Some(
                    kafka_connect_json_converter::debezium_value_schema_str(schema)?,
                ))
            } else {
                Ok(None)
            }
        }
        _ => Ok(None),
    }
}

/// Build a schema for the key component of the payload.
pub fn build_key_schema(
    config: &JsonEncoderConfig,
    schema: &Relation,
) -> Result<Option<String>, ControllerError> {
    match config.json_flavor {
        Some(JsonFlavor::KafkaConnectJsonConverter) => {
            if matches!(config.update_format, JsonUpdateFormat::Debezium) {
                Ok(Some(kafka_connect_json_converter::relation_schema_str(
                    schema,
                )?))
            } else {
                Ok(None)
            }
        }
        _ => Ok(None),
    }
}

/// Generate JSON schema supported by the `JsonConverter` Java class used
/// with many Kafka Connect connectors. This schema is inlined in each
/// Kafka message along with the data. For some reason this format is
/// different from the standard JSON schema format.
///
/// A field in this schema has a type, which represents its "wire format".
/// In addition it can optionally specify a logical type, which this wire
/// format represents.  The latter is the name of Java class known to Kafka
/// Connect, such as `org.apache.kafka.connect.data.Time`.  Finally, some
/// types can have additional parameters, e.g., scale and precision for
/// decimals.
mod kafka_connect_json_converter {
    use crate::ControllerError;
    use pipeline_types::program_schema::{ColumnType, Field, Relation};
    use serde::{Deserialize, Serialize};
    use std::collections::BTreeMap;

    #[derive(Serialize, Deserialize)]
    struct Type {
        #[serde(flatten)]
        representation_type: RepresentationType,
        #[serde(flatten)]
        logical_type: Option<LogicalType>,
    }

    #[derive(Serialize, Deserialize)]
    struct LogicalType {
        name: String,
        #[serde(default)]
        parameters: BTreeMap<String, String>,
    }

    #[derive(Serialize, Deserialize)]
    #[serde(tag = "type")]
    enum RepresentationType {
        #[serde(rename = "struct")]
        Struct { fields: Vec<JsonField> },
        #[serde(rename = "string")]
        String,
        #[serde(rename = "boolean")]
        Boolean,
        #[serde(rename = "int8")]
        Int8,
        #[serde(rename = "int16")]
        Int16,
        #[serde(rename = "int32")]
        Int32,
        #[serde(rename = "int64")]
        Int64,
        #[serde(rename = "float")]
        Float,
        #[serde(rename = "double")]
        Double,
        #[serde(rename = "bytes")]
        Bytes,
        #[serde(rename = "array")]
        Array {
            // unsupported
            items: Box<Type>,
        },
        #[serde(rename = "map")]
        Map {
            // unsupported
            keys: Box<Type>,
            values: Box<Type>,
        },
    }

    #[derive(Serialize, Deserialize)]
    struct JsonField {
        field: String,
        #[serde(flatten)]
        typ: Type,
        optional: bool,
    }

    pub fn debezium_value_schema_str(schema: &Relation) -> Result<String, ControllerError> {
        Ok(serde_json::to_string(&debezium_value_schema(schema)?).unwrap())
    }

    fn debezium_value_schema(schema: &Relation) -> Result<Type, ControllerError> {
        Ok(Type {
            representation_type: RepresentationType::Struct {
                fields: vec![
                    JsonField {
                        field: "after".to_string(),
                        typ: relation_schema(schema)?,
                        optional: true,
                    },
                    JsonField {
                        field: "op".to_string(),
                        typ: Type {
                            representation_type: RepresentationType::String,
                            logical_type: None,
                        },
                        optional: false,
                    },
                ],
            },
            logical_type: None,
        })
    }

    pub fn relation_schema_str(schema: &Relation) -> Result<String, ControllerError> {
        Ok(serde_json::to_string(&relation_schema(schema)?).unwrap())
    }

    fn relation_schema(schema: &Relation) -> Result<Type, ControllerError> {
        let mut fields = Vec::new();

        for field in schema.fields.iter() {
            fields.push(field_schema(field)?)
        }

        Ok(Type {
            representation_type: RepresentationType::Struct { fields },
            logical_type: None,
        })
    }

    fn field_schema(schema: &Field) -> Result<JsonField, ControllerError> {
        Ok(JsonField {
            field: schema.name.clone(),
            typ: type_schema(&schema.columntype)?,
            optional: schema.columntype.nullable,
        })
    }

    fn type_schema(schema: &ColumnType) -> Result<Type, ControllerError> {
        Ok(Type {
            representation_type: representation_type_schema(schema)?,
            logical_type: logical_type_schema(schema)?,
        })
    }

    fn logical_type_schema(schema: &ColumnType) -> Result<Option<LogicalType>, ControllerError> {
        Ok(match schema.typ.to_lowercase().as_str() {
            "time" => Some(LogicalType {
                name: "org.apache.kafka.connect.data.Time".to_string(),
                parameters: BTreeMap::new(),
            }),
            "timestamp" => Some(LogicalType {
                name: "org.apache.kafka.connect.data.Timestamp".to_string(),
                parameters: BTreeMap::new(),
            }),
            "date" => Some(LogicalType {
                name: "org.apache.kafka.connect.data.Date".to_string(),
                parameters: BTreeMap::new(),
            }),
            _ => None,
        })
    }

    fn representation_type_schema(
        schema: &ColumnType,
    ) -> Result<RepresentationType, ControllerError> {
        match schema.typ.to_lowercase().as_str() {
            "boolean" | "bool" => Ok(RepresentationType::Boolean),
            "varchar" | "character varying" | "char" | "character" | "string" | "text" => {
                Ok(RepresentationType::String)
            }
            "tinyint" => Ok(RepresentationType::Int8),
            "smallint" | "int2" => Ok(RepresentationType::Int16),
            "integer" | "int" | "signed" | "int4" => Ok(RepresentationType::Int32),
            "bigint" | "int64" => Ok(RepresentationType::Int64),
            // This requires Kafka connector to be configure with `"decimal.handling.mode":
            // "string"`. Other encodings would require serde to know the scale of the
            // decimal number, which isn't preserved in Rust.
            "decimal" | "dec" | "numeric" | "number" => Ok(RepresentationType::String),
            "real" | "float4" | "float32" => Ok(RepresentationType::Float),
            "double" | "float8" | "float64" => Ok(RepresentationType::Double),
            "time" => Ok(RepresentationType::Int64),
            "timestamp" => Ok(RepresentationType::Int64),
            "date" => Ok(RepresentationType::Int32),
            "binary" | "varbinary" | "bytea" => Ok(RepresentationType::Bytes),
            "array" => Ok(RepresentationType::Array {
                items: Box::new(type_schema(schema.component.as_ref().ok_or_else(
                    || {
                        ControllerError::schema_validation_error(&format!(
                            "element type is not specified for array type {schema:?}"
                        ))
                    },
                )?)?),
            }),
            _ => Err(ControllerError::not_supported(&format!(
                "column type {schema:?} is not supported by the JSON encoder"
            ))),
        }
    }
}
