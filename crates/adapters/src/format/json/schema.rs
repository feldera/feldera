//! Generate JSON schema for formats that require an explicit data schema.
//!
//! The schema can be inlined in each message or distributed separately, e.g.,
//! via a schema registry.

use feldera_types::format::json::{JsonEncoderConfig, JsonFlavor, JsonUpdateFormat};
use feldera_types::program_schema::Relation;

/// Build a schema for the value component of the payload.
pub fn build_value_schema(config: &JsonEncoderConfig, schema: &Relation) -> Option<String> {
    match config.json_flavor {
        Some(JsonFlavor::KafkaConnectJsonConverter) => {
            if matches!(config.update_format, JsonUpdateFormat::Debezium) {
                Some(kafka_connect_json_converter::debezium_value_schema_str(
                    schema,
                ))
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Build a schema for the key component of the payload.
pub fn build_key_schema(config: &JsonEncoderConfig, schema: &Relation) -> Option<String> {
    match config.json_flavor {
        Some(JsonFlavor::KafkaConnectJsonConverter) => {
            if matches!(config.update_format, JsonUpdateFormat::Debezium) {
                Some(kafka_connect_json_converter::debezium_key_schema_str(
                    schema,
                    config.key_fields.as_ref(),
                ))
            } else {
                None
            }
        }
        _ => None,
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
    use feldera_types::program_schema::{ColumnType, Field, Relation, SqlType};
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
        Array { items: Box<TypeSchema> },
        // not supported by the Debezium JDBC sink connector
        #[serde(rename = "map")]
        Map {
            keys: Box<TypeSchema>,
            values: Box<TypeSchema>,
        },
    }

    #[derive(Serialize, Deserialize)]
    struct TypeSchema {
        #[serde(flatten)]
        typ: Type,
        optional: bool,
    }

    #[derive(Serialize, Deserialize)]
    struct JsonField {
        field: String,
        #[serde(flatten)]
        schema: TypeSchema,
    }

    pub fn debezium_value_schema_str(schema: &Relation) -> String {
        serde_json::to_string(&debezium_value_schema(schema)).unwrap()
    }

    /// Generate JSON schema for the Debezium key field.
    ///
    /// When `key_fields` is specified, only includes those fields in the schema.
    pub fn debezium_key_schema_str(schema: &Relation, key_fields: Option<&Vec<String>>) -> String {
        serde_json::to_string(&debezium_key_schema(schema, key_fields)).unwrap()
    }

    fn debezium_value_schema(schema: &Relation) -> Type {
        Type {
            representation_type: RepresentationType::Struct {
                fields: vec![
                    JsonField {
                        field: "after".to_string(),
                        schema: TypeSchema {
                            typ: relation_schema(schema, None),
                            optional: true,
                        },
                    },
                    JsonField {
                        field: "op".to_string(),
                        schema: TypeSchema {
                            typ: Type {
                                representation_type: RepresentationType::String,
                                logical_type: None,
                            },
                            optional: false,
                        },
                    },
                ],
            },
            logical_type: Some(LogicalType {
                name: "Envelope".to_string(),
                parameters: Default::default(),
            }),
        }
    }

    fn debezium_key_schema(schema: &Relation, key_fields: Option<&Vec<String>>) -> Type {
        let mut typ = relation_schema(schema, key_fields);
        typ.logical_type = Some(LogicalType {
            name: "Key".to_string(),
            parameters: Default::default(),
        });

        typ
    }

    fn relation_schema(schema: &Relation, key_fields: Option<&Vec<String>>) -> Type {
        let mut fields = Vec::new();

        for field in schema.fields.iter() {
            if key_fields.is_none() || key_fields.unwrap().iter().any(|f| field.name == f) {
                fields.push(field_schema(field))
            }
        }

        Type {
            representation_type: RepresentationType::Struct { fields },
            logical_type: None,
        }
    }

    fn field_schema(schema: &Field) -> JsonField {
        JsonField {
            field: schema.name.name().clone(),
            schema: type_schema(&schema.columntype),
        }
    }

    fn type_schema(schema: &ColumnType) -> TypeSchema {
        TypeSchema {
            typ: Type {
                representation_type: representation_type_schema(schema),
                logical_type: logical_type_schema(schema),
            },
            optional: schema.nullable,
        }
    }

    fn logical_type_schema(schema: &ColumnType) -> Option<LogicalType> {
        match schema.typ {
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
            SqlType::Interval(_) => Some(LogicalType {
                name: "io.debezium.time.Interval".to_string(),
                parameters: BTreeMap::new(),
            }),
            _ => None,
        }
    }

    fn representation_type_schema(schema: &ColumnType) -> RepresentationType {
        match schema.typ {
            SqlType::Boolean => RepresentationType::Boolean,
            SqlType::Varchar | SqlType::Char => RepresentationType::String,
            SqlType::TinyInt => RepresentationType::Int8,
            SqlType::SmallInt => RepresentationType::Int16,
            SqlType::Int => RepresentationType::Int32,
            SqlType::BigInt => RepresentationType::Int64,
            SqlType::UTinyInt => RepresentationType::Int16,
            SqlType::USmallInt => RepresentationType::Int32,
            SqlType::UInt => RepresentationType::Int64,
            SqlType::UBigInt => RepresentationType::String,
            // This requires Kafka connector to be configure with `"decimal.handling.mode":
            // "string"`. Other encodings would require serde to know the scale of the
            // decimal number, which isn't preserved in Rust.
            SqlType::Decimal => RepresentationType::String,
            SqlType::Real => RepresentationType::Float,
            SqlType::Double => RepresentationType::Double,
            SqlType::Time => RepresentationType::Int64,
            SqlType::Timestamp => RepresentationType::Int64,
            SqlType::Date => RepresentationType::Int32,
            SqlType::Binary | SqlType::Varbinary => RepresentationType::Bytes,
            SqlType::Array => RepresentationType::Array {
                // unwrap() is ok here as Array type is guaranteed to have a component.
                items: Box::new(type_schema(schema.component.as_ref().unwrap())),
            },
            SqlType::Struct => RepresentationType::Struct {
                fields: schema
                    .fields
                    .as_ref()
                    // `SqlType::Struct` implies `fields.is_some()`
                    .unwrap()
                    .iter()
                    .map(field_schema)
                    .collect::<Vec<_>>(),
            },
            SqlType::Map => {
                let key_type = schema.key.as_ref().unwrap();
                let val_type = schema.value.as_ref().unwrap();
                RepresentationType::Map {
                    keys: Box::new(type_schema(key_type)),
                    values: Box::new(type_schema(val_type)),
                }
            }
            SqlType::Interval(_) => RepresentationType::String,
            SqlType::Variant => RepresentationType::String,
            SqlType::Null => RepresentationType::String,
            SqlType::Uuid => RepresentationType::String,
        }
    }

    #[cfg(test)]
    mod test {
        use crate::format::json::schema::kafka_connect_json_converter::debezium_value_schema_str;
        use feldera_types::program_schema::Relation;

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
    }, {
      "name": "f10",
      "case_sensitive": false,
      "columntype": {
        "fields": [],
        "nullable": true
      }
    }, {
      "name": "f11",
      "case_sensitive": false,
      "columntype":  {
        "fields": [
            {  "name": "f11_0", "columntype": { "type": "BOOLEAN", "nullable": false } },
            {  "name": "f11_1", "columntype": { "fields": [{ "name": "f11_1_0", "columntype": { "type": "BOOLEAN", "nullable": false } }], "nullable": false } }
        ],
        "nullable": false
      }
    }, {
      "name": "f12",
      "case_sensitive": false,
      "columntype":  {
        "type": "MAP",
        "key": {
          "type" : "BIGINT",
          "nullable" : false
        },
        "value": {
          "type" : "TIMESTAMP",
          "nullable" : true,
          "precision" : 0
        },
        "nullable": false
      }
    }],
    "primary_key" : [ "id" ]
}"#,
            )
            .unwrap();
            let connect_schema = debezium_value_schema_str(&schema);
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
        },
        {
          "field": "f10",
          "type": "struct",
          "optional": true,
          "fields": []
        },
        {
          "field": "f11",
          "type": "struct",
          "optional": false,
          "fields": [
            { "field": "f11_0", "type": "boolean", "optional": false },
            { "field": "f11_1", "type": "struct", "optional": false, "fields": [ { "field": "f11_1_0", "type": "boolean", "optional": false } ] }
          ]
        },
        {
          "field": "f12",
          "type": "map",
          "keys": {
            "type": "int64",
            "optional": false
          },
          "values": {
            "type": "int64",
            "name": "org.apache.kafka.connect.data.Timestamp",
            "optional": true
          },
          "optional": false
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
