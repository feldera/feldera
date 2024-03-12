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
                Ok(Some(kafka_connect_json_converter::debezium_key_schema_str(
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
    use pipeline_types::program_schema::{ColumnType, Field, Relation, SqlType};
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
        #[serde(skip_serializing_if = "BTreeMap::is_empty")]
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
        // not supported by the Debezium JDBC sink connector
        #[serde(rename = "array")]
        Array { items: Box<Type> },
        // not supported by the Debezium JDBC sink connector
        #[serde(rename = "map")]
        Map { keys: Box<Type>, values: Box<Type> },
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

    pub fn debezium_key_schema_str(schema: &Relation) -> Result<String, ControllerError> {
        Ok(serde_json::to_string(&debezium_key_schema(schema)?).unwrap())
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
            logical_type: Some(LogicalType {
                name: "Envelope".to_string(),
                parameters: Default::default(),
            }),
        })
    }

    fn debezium_key_schema(schema: &Relation) -> Result<Type, ControllerError> {
        let mut typ = relation_schema(schema)?;
        typ.logical_type = Some(LogicalType {
            name: "Key".to_string(),
            parameters: Default::default(),
        });

        Ok(typ)
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
        Ok(match schema.typ {
            SqlType::Time => Some(LogicalType {
                name: "org.apache.kafka.connect.data.Time".to_string(),
                parameters: BTreeMap::new(),
            }),
            SqlType::Timestamp => Some(LogicalType {
                name: "org.apache.kafka.connect.data.Timestamp".to_string(),
                parameters: BTreeMap::new(),
            }),
            SqlType::Date => Some(LogicalType {
                name: "org.apache.kafka.connect.data.Date".to_string(),
                parameters: BTreeMap::new(),
            }),
            // TODO: add serialization for intervals to `sqllib`.
            SqlType::Interval => Some(LogicalType {
                name: "io.debezium.time.Interval".to_string(),
                parameters: BTreeMap::new(),
            }),
            _ => None,
        })
    }

    fn representation_type_schema(
        schema: &ColumnType,
    ) -> Result<RepresentationType, ControllerError> {
        match schema.typ {
            SqlType::Boolean => Ok(RepresentationType::Boolean),
            SqlType::Varchar | SqlType::Char => Ok(RepresentationType::String),
            SqlType::TinyInt => Ok(RepresentationType::Int8),
            SqlType::SmallInt => Ok(RepresentationType::Int16),
            SqlType::Int => Ok(RepresentationType::Int32),
            SqlType::BigInt => Ok(RepresentationType::Int64),
            // This requires Kafka connector to be configure with `"decimal.handling.mode":
            // "string"`. Other encodings would require serde to know the scale of the
            // decimal number, which isn't preserved in Rust.
            SqlType::Decimal => Ok(RepresentationType::String),
            SqlType::Real => Ok(RepresentationType::Float),
            SqlType::Double => Ok(RepresentationType::Double),
            SqlType::Time => Ok(RepresentationType::Int64),
            SqlType::Timestamp => Ok(RepresentationType::Int64),
            SqlType::Date => Ok(RepresentationType::Int32),
            SqlType::Binary | SqlType::Varbinary => Ok(RepresentationType::Bytes),
            SqlType::Array => Ok(RepresentationType::Array {
                // unwrap() is ok here as Array type is guaranteed to have a component.
                items: Box::new(type_schema(schema.component.as_ref().unwrap())?),
            }),
            SqlType::Interval => Ok(RepresentationType::String),
            SqlType::Null => Ok(RepresentationType::String),
        }
    }

    #[cfg(test)]
    mod test {
        use crate::format::json::schema::kafka_connect_json_converter::debezium_value_schema_str;
        use pipeline_types::program_schema::Relation;

        #[test]
        fn test_schema_encoder() {
            let schema: Relation = serde_json::from_str(
                r#"{
    "name" : "test_table",
    "case_sensitive" : false,
    "fields" : [ {
      "name" : "id",
      "case_sensitive" : false,
      "columntype" : {
        "type" : "BIGINT",
        "nullable" : false
      }
    }, {
      "name" : "f1",
      "case_sensitive" : false,
      "columntype" : {
        "type" : "BOOLEAN",
        "nullable" : true
      }
    }, {
      "name" : "f2",
      "case_sensitive" : false,
      "columntype" : {
        "type" : "VARCHAR",
        "nullable" : true,
        "precision" : -1
      }
    }, {
      "name" : "f3",
      "case_sensitive" : false,
      "columntype" : {
        "type" : "TINYINT",
        "nullable" : true
      }
    }, {
      "name" : "f4",
      "case_sensitive" : false,
      "columntype" : {
        "type" : "DECIMAL",
        "nullable" : true,
        "precision" : 5,
        "scale" : 2
      }
    }, {
      "name" : "f5",
      "case_sensitive" : false,
      "columntype" : {
        "type" : "DOUBLE",
        "nullable" : true
      }
    }, {
      "name" : "f6",
      "case_sensitive" : false,
      "columntype" : {
        "type" : "TIME",
        "nullable" : true,
        "precision" : 0
      }
    }, {
      "name" : "f7",
      "case_sensitive" : false,
      "columntype" : {
        "type" : "TIMESTAMP",
        "nullable" : true,
        "precision" : 0
      }
    }, {
      "name" : "f8",
      "case_sensitive" : false,
      "columntype" : {
        "type" : "DATE",
        "nullable" : true
      }
    }, {
      "name" : "f9",
      "case_sensitive" : false,
      "columntype" : {
        "type" : "BINARY",
        "nullable" : true,
        "precision" : 1
      }
    } ],
    "primary_key" : [ "id" ]
}"#,
            )
            .unwrap();
            let connect_schema = debezium_value_schema_str(&schema).unwrap();
            let connect_schema_value: serde_json::Value =
                serde_json::from_str(&connect_schema).unwrap();
            let expected_value: serde_json::Value = serde_json::from_str(
                r#"{
  "type": "struct",
  "fields": [
    {
      "field": "after",
      "type": "struct",
      "fields": [
        {
          "field": "id",
          "type": "int64",
          "optional": false
        },
        {
          "field": "f1",
          "type": "boolean",
          "optional": true
        },
        {
          "field": "f2",
          "type": "string",
          "optional": true
        },
        {
          "field": "f3",
          "type": "int8",
          "optional": true
        },
        {
          "field": "f4",
          "type": "string",
          "optional": true
        },
        {
          "field": "f5",
          "type": "double",
          "optional": true
        },
        {
          "field": "f6",
          "type": "int64",
          "name": "org.apache.kafka.connect.data.Time",
          "optional": true
        },
        {
          "field": "f7",
          "type": "int64",
          "name": "org.apache.kafka.connect.data.Timestamp",
          "optional": true
        },
        {
          "field": "f8",
          "type": "int32",
          "name": "org.apache.kafka.connect.data.Date",
          "optional": true
        },
        {
          "field": "f9",
          "type": "bytes",
          "optional": true
        }
      ],
      "optional": true
    },
    {
      "field": "op",
      "type": "string",
      "optional": false
    }
  ],
  "name": "Envelope"
}"#,
            )
            .unwrap();
            println!("{}", connect_schema);
            assert_eq!(&connect_schema_value, &expected_value);
        }
    }
}
