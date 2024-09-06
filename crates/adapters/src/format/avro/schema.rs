//! Helpers for working with Avro schemas.

use std::collections::BTreeMap;

use apache_avro::{
    schema::{DecimalSchema, Name, RecordField, RecordFieldOrder, RecordSchema, UnionSchema},
    Schema as AvroSchema,
};
use feldera_types::program_schema::{
    canonical_identifier, ColumnType, Field, Relation, SqlIdentifier, SqlType,
};

use crate::ControllerError;

/// Indicates whether the field has an optional type (`["null", T]`) and,
/// if so, whether the non-null element of the union is at position 0 or 1.
pub enum OptionalField {
    NonOptional,
    Optional(u32),
}

/// Convert schema to JSON format.
pub fn schema_json(schema: &AvroSchema) -> String {
    serde_json::to_string(schema).unwrap_or_else(
        // This should never happen, but just in case.
        |_| "Avro schema cannot be converted to JSON".to_string(),
    )
}

pub fn schema_unwrap_optional(schema: &AvroSchema) -> (&AvroSchema, OptionalField) {
    match schema {
        AvroSchema::Union(union_schema) => match union_schema.variants() {
            [AvroSchema::Null, s] => (s, OptionalField::Optional(1)),
            [s, AvroSchema::Null] => (s, OptionalField::Optional(0)),
            _ => (schema, OptionalField::NonOptional),
        },
        _ => (schema, OptionalField::NonOptional),
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
        let Some(avro_field) = lookup_field(&record_schema.fields, field) else {
            // Allow nullable fields to be missing in the Avro schema. This is useful to, e.g.,
            // support inputs encoded using older versions of the schema missing some fields.
            if field.columntype.nullable {
                return Ok(());
            } else {
                return Err(format!(
                    "column '{}' is missing in the Avro schema",
                    field.name.name()
                ));
            }
        };

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
    if avro_schema != &AvroSchema::TimestampMicros
        && avro_schema != &AvroSchema::TimestampMillis
        && avro_schema != &AvroSchema::Long
    {
        return Err(format!(
            "invalid Avro schema for a column of type 'TIMESTAMP': expected 'timestamp-micros', 'timestamp-millis', or 'long', but found {}",
            schema_json(avro_schema)
        ));
    }

    Ok(())
}

/// Check that Avro schema can be deserialized as SQL `TIME` type.
fn validate_time_schema(avro_schema: &AvroSchema) -> Result<(), String> {
    if avro_schema != &AvroSchema::TimeMillis
        && avro_schema != &AvroSchema::TimeMicros
        && avro_schema != &AvroSchema::Long
    {
        return Err(format!(
            "invalid Avro schema for a column of type 'TIME': expected 'time-micros', 'timestamp-millis', or 'long', but found {}",
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
        let avro_inner = schema_unwrap_optional(avro_schema).0;
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

            if key_type.typ != SqlType::Char && key_type.typ != SqlType::Varchar {
                return Err(format!(
                    "cannot deserialize map with key type '{}': Avro only allows string keys",
                    &key_type.typ
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

pub fn is_valid_avro_identifier(ident: &str) -> bool {
    if ident.is_empty() {
        return false;
    }
    let first = ident.chars().next().unwrap();

    (first.is_ascii_alphabetic() || first == '_')
        && ident.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

pub fn gen_key_schema(
    record_schema: &RecordSchema,
    key_fields: &[SqlIdentifier],
) -> Result<AvroSchema, ControllerError> {
    let key_fields = key_fields.iter().map(|f| f.name()).collect::<Vec<_>>();

    let mut fields = Vec::new();
    let mut lookup = BTreeMap::new();

    for field in record_schema.fields.iter() {
        if key_fields.contains(&field.name) {
            lookup.insert(field.name.clone(), fields.len());
            fields.push(field.clone());
        }
    }

    let key_record_schema = RecordSchema {
        fields,
        lookup,
        name: Name {
            name: format!("__{}__Key", &record_schema.name.name),
            namespace: record_schema.name.namespace.clone(),
        },
        aliases: None,
        doc: None,
        attributes: BTreeMap::new(),
    };

    Ok(AvroSchema::Record(key_record_schema))
}

#[derive(Default)]
pub struct AvroSchemaBuilder {
    namespace: Option<String>,
    key_fields: Option<Vec<SqlIdentifier>>,
}

impl AvroSchemaBuilder {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn with_namespace(mut self, namespace: Option<&str>) -> Self {
        self.namespace = namespace.map(|ns| ns.to_string());
        self
    }

    pub fn with_key_fields(mut self, key_fields: Option<&Vec<SqlIdentifier>>) -> Self {
        self.key_fields = key_fields.cloned();
        self
    }

    pub fn relation_to_avro_schema(
        &self,
        relation_schema: &Relation,
    ) -> Result<AvroSchema, String> {
        Ok(AvroSchema::Record(self.struct_to_avro_schema(
            &relation_schema.name,
            &relation_schema.fields,
            true,
        )?))
    }

    fn struct_to_avro_schema(
        &self,
        name: &SqlIdentifier,
        struct_fields: &[Field],
        top_level: bool,
    ) -> Result<RecordSchema, String> {
        let name = name.name();
        if !is_valid_avro_identifier(&name) {
            return Err(format!("'{name}' is not a valid Avro identifier"));
        }

        let mut fields = Vec::with_capacity(struct_fields.len());
        let mut lookup = BTreeMap::new();

        for (i, field) in struct_fields.iter().enumerate() {
            let key_field = self.key_fields.is_none()
                || self.key_fields.as_ref().unwrap().contains(&field.name);
            let f = self.field_to_avro_schema(field, i, top_level && !key_field)?;
            lookup.insert(f.name.clone(), i);
            fields.push(f);
        }

        Ok(RecordSchema {
            name: Name {
                name: name.to_string(),
                namespace: self.namespace.clone(),
            },
            aliases: None,
            doc: None,
            fields,
            lookup,
            attributes: BTreeMap::new(),
        })
    }

    fn field_to_avro_schema(
        &self,
        field: &Field,
        position: usize,
        force_optional: bool,
    ) -> Result<RecordField, String> {
        let name = field.name.name();
        if !is_valid_avro_identifier(&name) {
            return Err(format!("'{name}' is not a valid Avro identifier"));
        }

        Ok(RecordField {
            name: name.clone(),
            doc: None,
            aliases: None,
            default: None,
            schema: self
                .column_type_to_avro_schema(&field.columntype, force_optional)
                .map_err(|e| format!("error generating Avro schema for field '{}': {e}", &name))?,
            order: RecordFieldOrder::Ascending,
            position,
            custom_attributes: BTreeMap::new(),
        })
    }

    fn column_type_to_avro_schema(
        &self,
        column_type: &ColumnType,
        force_optional: bool,
    ) -> Result<AvroSchema, String> {
        let inner = self.column_type_to_avro_schema_inner(column_type)?;

        if column_type.nullable || force_optional {
            Ok(AvroSchema::Union(
                UnionSchema::new(vec![AvroSchema::Null, inner])
                    .map_err(|e| format!("error generating union schema: {e}"))?,
            ))
        } else {
            Ok(inner)
        }
    }

    fn column_type_to_avro_schema_inner(
        &self,
        column_type: &ColumnType,
    ) -> Result<AvroSchema, String> {
        Ok(match column_type.typ {
            SqlType::Boolean => AvroSchema::Boolean,
            SqlType::TinyInt => AvroSchema::Int,
            SqlType::SmallInt => AvroSchema::Int,
            SqlType::Int => AvroSchema::Int,
            SqlType::BigInt => AvroSchema::Long,
            SqlType::Real => AvroSchema::Float,
            SqlType::Double => AvroSchema::Double,
            SqlType::Decimal => {
                let precision = column_type
                    .precision
                    .ok_or("internal error: decimal type is missing precision")?
                    as usize;
                let scale = column_type
                    .scale
                    .ok_or("internal error: decimal type is missing scale")?
                    as usize;
                AvroSchema::Decimal(DecimalSchema {
                    precision,
                    scale,
                    inner: Box::new(AvroSchema::Bytes),
                })
            }
            SqlType::Char => AvroSchema::String,
            SqlType::Varchar => AvroSchema::String,
            SqlType::Binary => AvroSchema::Bytes,
            SqlType::Varbinary => AvroSchema::Bytes,
            SqlType::Time => AvroSchema::TimeMicros,
            SqlType::Date => AvroSchema::Date,
            SqlType::Timestamp => AvroSchema::TimestampMicros,
            SqlType::Interval(_) => {
                return Err("not implemented: Avro encoding for the SQL interval type".to_string())
            }
            SqlType::Array => {
                let component = column_type
                    .component
                    .as_ref()
                    .ok_or("internal error: array type is missing array element type")?;
                AvroSchema::Array(Box::new(self.column_type_to_avro_schema(component, false)?))
            }
            SqlType::Struct => {
                return Err("not implemented: Avro encoding for user-defined SQL types".to_string())
            }
            SqlType::Map => {
                let key_type = column_type.value.as_ref().ok_or(
                    "internal error: relation schema contains a map field, with a missing key type",
                )?;
                if !key_type.typ.is_string() {
                    return Err(format!(
                        "cannot serialize map with key type '{}': Avro only allows string keys",
                        &key_type.typ
                    ));
                }
                let value_type = column_type.value.as_ref().ok_or("internal error: relation schema contains a map field, with a missing value type")?;
                AvroSchema::Map(Box::new(
                    self.column_type_to_avro_schema(value_type, false)?,
                ))
            }
            SqlType::Null => AvroSchema::Null,
        })
    }
}
