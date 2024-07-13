//! Helpers for working with Avro schemas.

use apache_avro::{schema::RecordField, Schema as AvroSchema};
use feldera_types::program_schema::{canonical_identifier, ColumnType, Field, SqlType};

/// Convert schema to JSON format.
pub fn schema_json(schema: &AvroSchema) -> String {
    serde_json::to_string(schema).unwrap_or_else(
        // This should never happen, but just in case.
        |_| "Avro schema cannot be converted to JSON".to_string(),
    )
}

/// Extract value type schema from a nullable field schema, which has the following shape:
/// Union[null, type] or Union[type, null]. Returns `None` if `schema` doesn't match this
/// pattern.
pub fn nullable_schema_value_schema(schema: &AvroSchema) -> Option<&AvroSchema> {
    match schema {
        AvroSchema::Union(union_schema) => match union_schema.variants() {
            [AvroSchema::Null, s] => Some(s),
            [s, AvroSchema::Null] => Some(s),
            _ => None,
        },
        _ => None,
    }
}

/// Find a field in a record schema.
fn lookup_field<'a>(fields: &'a [RecordField], field: &'a Field) -> Option<&'a RecordField> {
    let name = field.name.name();

    // TODO: check `record_field.aliases`.
    fields
        .iter()
        .find(|&record_field| canonical_identifier(&record_field.name) == name)
}

/// Check that Avro schema can be deserialized into a struct with
/// specified field.
pub fn validate_struct_schema(
    avro_schema: &AvroSchema,
    struct_schema: &[Field],
) -> Result<(), String> {
    let AvroSchema::Record(record_schema) = avro_schema else {
        return Err(format!(
            "expected schema of type 'record', but found {}",
            schema_json(avro_schema)
        ));
    };

    for field in struct_schema {
        let avro_field = lookup_field(&record_schema.fields, field).ok_or_else(|| {
            format!(
                "column '{}' is missing in the Avro schema",
                field.name.name()
            )
        })?;

        validate_field_schema(&avro_field.schema, &field.columntype).map_err(|e| {
            format!(
                "error validating schema for column '{}': {e}",
                field.name.name()
            )
        })?;
    }

    Ok(())
}

/// Check that Avro schema can be deserialized into an array with
/// specified element type.
fn validate_array_schema(
    avro_schema: &AvroSchema,
    component_schema: &ColumnType,
) -> Result<(), String> {
    let AvroSchema::Array(element_schema) = avro_schema else {
        return Err(format!(
            "expected schema of type 'array', but found {}",
            schema_json(avro_schema)
        ));
    };

    validate_field_schema(element_schema, component_schema)
        .map_err(|e| format!("error validating array element schema: {e}"))?;

    Ok(())
}

/// Check that Avro schema can be deserialized into a map with
/// specified value type (assumes that map keys are strings).
fn validate_map_schema(avro_schema: &AvroSchema, value_schema: &ColumnType) -> Result<(), String> {
    let AvroSchema::Map(element_schema) = avro_schema else {
        return Err(format!(
            "expected schema of type 'map', but found {}",
            schema_json(avro_schema)
        ));
    };

    validate_field_schema(element_schema, value_schema)
        .map_err(|e| format!("error validating map value schema: {e}"))?;

    Ok(())
}

/// Check that Avro schema can be deserialized as SQL `TIMESTAMP` type.
fn validate_timestamp_schema(avro_schema: &AvroSchema) -> Result<(), String> {
    // TODO: we can support TimestampMillis by transforming them to micros on the fly.
    if avro_schema == &AvroSchema::TimestampMillis {
        return Err("Avro timestamp encoding using 'timestamp-millis' type is currently not supported; use 'timestamp-micros instead'".to_string());
    }

    if avro_schema != &AvroSchema::TimestampMicros && avro_schema != &AvroSchema::Long {
        return Err(format!(
            "invalid Avro schema for a column of type 'TIMESTAMP': expected 'timestamp-micros' or 'long', but found {}",
            schema_json(avro_schema)
        ));
    }

    Ok(())
}

/// Check that Avro schema can be deserialized as SQL `TIME` type.
fn validate_time_schema(avro_schema: &AvroSchema) -> Result<(), String> {
    // TODO: we can support TimeMillis by transforming them to micros on the fly.
    if avro_schema == &AvroSchema::TimeMillis {
        return Err("Avro time encoding using 'time-millis' type is currently not supported; use 'time-micros instead'".to_string());
    }

    if avro_schema != &AvroSchema::TimeMicros && avro_schema != &AvroSchema::Long {
        return Err(format!(
            "invalid Avro schema for a column of type 'TIME': expected 'time-micros' ot 'long', but found {}",
            schema_json(avro_schema)
        ));
    }

    Ok(())
}

/// Check that Avro schema can be deserialized as SQL `DATE` type.
fn validate_date_schema(avro_schema: &AvroSchema) -> Result<(), String> {
    if avro_schema != &AvroSchema::Int && avro_schema != &AvroSchema::Date {
        return Err(format!(
            "invalid Avro schema for a column of type 'DATE': expected 'date' ot 'int', but found {}",
            schema_json(avro_schema)
        ));
    }

    Ok(())
}

/// Check that Avro schema can be deserialized a SQL column with the given
/// column type.
pub fn validate_field_schema(
    avro_schema: &AvroSchema,
    field_schema: &ColumnType,
) -> Result<(), String> {
    if field_schema.nullable {
        let avro_inner = nullable_schema_value_schema(avro_schema).ok_or_else(|| {
            format!(
                "nullable column with non-nullable Avro schema {}",
                schema_json(avro_schema)
            )
        })?;
        let mut field_schema = field_schema.clone();
        field_schema.nullable = false;
        return validate_field_schema(avro_inner, &field_schema);
    };

    let expected = match field_schema.typ {
        SqlType::Boolean => AvroSchema::Boolean,
        SqlType::TinyInt | SqlType::SmallInt | SqlType::Int => AvroSchema::Int,
        SqlType::BigInt => AvroSchema::Long,
        SqlType::Real => AvroSchema::Float,
        SqlType::Double => AvroSchema::Double,
        SqlType::Decimal => {
            return Err("not implemented: Avro deserialization for the 'DECIMAL' type".to_string());
        }
        SqlType::Char | SqlType::Varchar => AvroSchema::String,
        SqlType::Binary | SqlType::Varbinary => AvroSchema::Bytes,
        SqlType::Time => {
            return validate_time_schema(avro_schema);
        }
        SqlType::Date => {
            return validate_date_schema(avro_schema);
        }
        SqlType::Timestamp => {
            return validate_timestamp_schema(avro_schema);
        }
        SqlType::Interval(_) => {
            // This type currently cannot occur in SQL table declarations.
            return Err("not implemented: Avro deserialization for 'INTERVAL' type".to_string());
        }
        SqlType::Array => {
            // This schema is generated by the SQL compiler, so this should never happen.
            if field_schema.component.is_none() {
                return Err("internal error: relation schema contains an array field with a missing component type".to_string());
            }
            return validate_array_schema(avro_schema, field_schema.component.as_ref().unwrap());
        }
        SqlType::Struct => {
            return validate_struct_schema(
                avro_schema,
                field_schema.fields.as_ref().unwrap_or(&vec![]),
            );
        }
        SqlType::Map => {
            let Some(key_type) = &field_schema.key else {
                return Err(
                    "internal error: relation schema contains a map field, with a missing key type"
                        .to_string(),
                );
            };
            let Some(value_type) = &field_schema.value else {
                return Err("internal error: relation schema contains a map field, with a missing value type".to_string());
            };

            if key_type.typ != SqlType::Char || key_type.typ != SqlType::Varchar {
                return Err(format!(
                    "cannot deserialize map with key type '{}': Avro only allows string keys",
                    serde_json::to_string(&key_type.typ).unwrap()
                ));
            }

            return validate_map_schema(avro_schema, value_type);
        }
        SqlType::Null => AvroSchema::Null,
    };

    if avro_schema != &expected {
        return Err(format!(
            "expected Avro schema '{}', but found '{}'",
            schema_json(&expected),
            schema_json(avro_schema)
        ));
    }

    Ok(())
}
