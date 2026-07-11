//! Value coercions for the Avro input connector.
//!
//! Some source systems encode a column in a form that differs from the Avro
//! type Feldera expects for the target column. Debezium, for example, encodes a
//! microsecond timestamp as a plain `long` tagged with a `connect.name`
//! attribute, and a variable-scale decimal as a `{scale, value}` record. A
//! [`Coercion`] recognizes such a field and converts its decoded value into a
//! form the target column's deserializer accepts.
//!
//! This module is the generic layer: it identifies coercions from schema
//! metadata, converts values, and validates schemas. The per-type mechanics
//! live in source-specific modules (currently [`super::debezium`]).
//!
//! # Adding a coercion
//!
//! 1. Add a variant to [`Coercion`].
//! 2. Recognize it in [`Coercion::from_connect_name`] (for Kafka Connect
//!    `connect.name` annotations) or extend [`field_coercion`] with a new
//!    detector (for other sources).
//! 3. Implement its conversion in [`Coercion::coerce`] and its schema check in
//!    [`Coercion::validate`].
//! 4. If the annotation sits on a primitive Avro type, whose attributes
//!    `apache-avro` drops during parsing, [`hoist_coercible_types`] recovers it
//!    automatically once step 2 recognizes it.

use std::collections::BTreeMap;

use apache_avro::{Schema as AvroSchema, types::Value};
use feldera_adapterlib::catalog::AvroSchemaRefs;
use feldera_types::program_schema::{ColumnType, SqlType};
use serde_json::{Map, Value as JsonValue};

use super::debezium::{self, DebeziumTimeType};
use super::schema::{schema_json, schema_unwrap_optional};
use crate::format::avro::resolve_ref;

/// `connect.name` of the Debezium variable-scale decimal type.
const DEBEZIUM_VARIABLE_SCALE_DECIMAL: &str = "io.debezium.data.VariableScaleDecimal";

/// A value coercion applied to a single Avro field.
///
/// Identified from schema metadata and applied by the deserializer, a coercion
/// bridges the gap between a field's on-the-wire Avro type and the Feldera
/// column type it feeds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Coercion {
    /// A Debezium temporal type, converted to microseconds.
    DebeziumTime(DebeziumTimeType),
    /// The `io.debezium.data.VariableScaleDecimal` record, converted to a
    /// decimal string.
    VariableScaleDecimal,
}

/// The Serde value a coercion produces. The deserializer dispatches it onto the
/// target column's visitor.
pub enum Coerced {
    /// A 64-bit integer, visited as `i64`.
    I64(i64),
    /// A string, visited as `str`.
    Str(String),
}

impl Coercion {
    /// Recognize a coercion from a Kafka Connect `connect.name` annotation.
    pub fn from_connect_name(connect_name: &str) -> Option<Coercion> {
        if let Some(time_type) = DebeziumTimeType::from_connect_name(connect_name) {
            Some(Coercion::DebeziumTime(time_type))
        } else if connect_name == DEBEZIUM_VARIABLE_SCALE_DECIMAL {
            Some(Coercion::VariableScaleDecimal)
        } else {
            None
        }
    }

    /// A representative name for this coercion, used in error messages.
    fn name(self) -> &'static str {
        match self {
            Coercion::DebeziumTime(time_type) => time_type.canonical_connect_name(),
            Coercion::VariableScaleDecimal => DEBEZIUM_VARIABLE_SCALE_DECIMAL,
        }
    }

    /// Convert a decoded Avro value into the form the target column expects.
    pub fn coerce(self, value: &Value) -> Result<Coerced, String> {
        Ok(match self {
            Coercion::DebeziumTime(time_type) => Coerced::I64(time_type.to_micros(value)?),
            Coercion::VariableScaleDecimal => {
                Coerced::Str(debezium::variable_scale_decimal_to_string(value)?)
            }
        })
    }

    /// Check that this coercion can populate `column_type` from `avro_schema`.
    pub fn validate(
        self,
        avro_schema: &AvroSchema,
        refs: &AvroSchemaRefs,
        column_type: &ColumnType,
    ) -> Result<(), String> {
        let (avro_schema, _) = schema_unwrap_optional(avro_schema);

        match self {
            Coercion::DebeziumTime(time_type) => {
                let (expected_sql, matches_sql) = if time_type.is_timestamp() {
                    (
                        "TIMESTAMP",
                        matches!(column_type.typ, SqlType::Timestamp | SqlType::TimestampTz),
                    )
                } else {
                    ("TIME", matches!(column_type.typ, SqlType::Time))
                };
                if !matches_sql {
                    return Err(self.wrong_column_error(expected_sql, column_type));
                }

                let matches_primitive = match time_type.avro_primitive() {
                    "int" => avro_schema == &AvroSchema::Int,
                    "long" => avro_schema == &AvroSchema::Long,
                    "string" => avro_schema == &AvroSchema::String,
                    _ => false,
                };
                if !matches_primitive {
                    return Err(format!(
                        "invalid Avro schema for Debezium type '{}': expected '{}', but found {}",
                        self.name(),
                        time_type.avro_primitive(),
                        schema_json(avro_schema)
                    ));
                }
            }
            Coercion::VariableScaleDecimal => {
                if column_type.typ != SqlType::Decimal {
                    return Err(self.wrong_column_error("DECIMAL", column_type));
                }

                let resolved = resolve_ref(avro_schema, refs)
                    .map_err(|name| format!("error resolving Avro schema reference: {name}"))?;
                let valid = matches!(
                    resolved,
                    AvroSchema::Record(record)
                        if record.lookup.contains_key("scale") && record.lookup.contains_key("value")
                );
                if !valid {
                    return Err(format!(
                        "invalid Avro schema for Debezium type '{}': expected a record with 'scale' and 'value' fields, but found {}",
                        self.name(),
                        schema_json(resolved)
                    ));
                }
            }
        }

        Ok(())
    }

    fn wrong_column_error(self, expected_sql: &str, column_type: &ColumnType) -> String {
        format!(
            "Debezium type '{}' can only be deserialized into a SQL {expected_sql} column, but the column has type '{}'",
            self.name(),
            column_type.typ
        )
    }
}

/// Detect the coercion, if any, implied by a record field.
///
/// Annotations on primitive types are recovered from the field's custom
/// attributes, where [`hoist_coercible_types`] places them. Annotations on
/// named types (records, enums) are read from the (ref-resolved) schema, which
/// preserves them natively.
pub fn field_coercion(
    custom_attributes: &BTreeMap<String, JsonValue>,
    schema: &AvroSchema,
    refs: &AvroSchemaRefs,
) -> Option<Coercion> {
    if let Some(coercion) = custom_attributes
        .get("connect.name")
        .and_then(JsonValue::as_str)
        .and_then(Coercion::from_connect_name)
    {
        return Some(coercion);
    }

    let (schema, _) = schema_unwrap_optional(schema);
    let schema = resolve_ref(schema, refs).ok()?;
    let AvroSchema::Record(record) = schema else {
        return None;
    };
    record
        .attributes
        .get("connect.name")
        .and_then(JsonValue::as_str)
        .and_then(Coercion::from_connect_name)
}

/// Rewrite an Avro schema so that coercion annotations on primitive types
/// survive parsing by `apache-avro`.
///
/// `apache-avro` discards attributes on primitive schemas, so a `connect.name`
/// nested inside a field's `type` object is lost. This copies each recognized
/// annotation up to its enclosing record field, where the crate preserves it as
/// a field-level custom attribute. Named types (records, enums) keep their
/// attributes natively and are left untouched.
///
/// Returns `None` if the input is not valid JSON, letting the caller fall back
/// to the original string so the normal schema-parsing error surfaces.
pub fn hoist_coercible_types(schema_json: &str) -> Option<String> {
    let mut value: JsonValue = serde_json::from_str(schema_json).ok()?;
    hoist_walk(&mut value);
    serde_json::to_string(&value).ok()
}

/// Recursively visit every record schema, hoisting primitive annotations onto
/// its fields.
fn hoist_walk(value: &mut JsonValue) {
    match value {
        JsonValue::Object(map) => {
            if let Some(JsonValue::Array(fields)) = map.get_mut("fields") {
                for field in fields.iter_mut() {
                    if let JsonValue::Object(field_map) = field {
                        hoist_field(field_map);
                    }
                }
            }
            for child in map.values_mut() {
                hoist_walk(child);
            }
        }
        JsonValue::Array(items) => {
            for item in items.iter_mut() {
                hoist_walk(item);
            }
        }
        _ => {}
    }
}

/// Copy a coercion annotation from a field's primitive `type` up to the field.
fn hoist_field(field: &mut Map<String, JsonValue>) {
    if field.contains_key("connect.name") {
        return;
    }
    if let Some(connect_name) = field.get("type").and_then(type_hoistable_connect_name) {
        field.insert("connect.name".to_string(), JsonValue::String(connect_name));
    }
}

/// Find a hoistable annotation inside a field's `type`, whether the type is a
/// bare object or a nullable union.
fn type_hoistable_connect_name(type_value: &JsonValue) -> Option<String> {
    match type_value {
        JsonValue::Object(obj) => object_hoistable_connect_name(obj),
        JsonValue::Array(variants) => variants.iter().find_map(|variant| match variant {
            JsonValue::Object(obj) => object_hoistable_connect_name(obj),
            _ => None,
        }),
        _ => None,
    }
}

/// Return an object schema's `connect.name` if it is a recognized coercion on a
/// primitive type. Named types keep their attributes natively, so they are not
/// hoisted.
fn object_hoistable_connect_name(obj: &Map<String, JsonValue>) -> Option<String> {
    let type_name = obj.get("type").and_then(JsonValue::as_str)?;
    if !is_primitive_type(type_name) {
        return None;
    }
    let connect_name = obj.get("connect.name")?.as_str()?;
    Coercion::from_connect_name(connect_name).map(|_| connect_name.to_string())
}

fn is_primitive_type(type_name: &str) -> bool {
    matches!(
        type_name,
        "null" | "boolean" | "int" | "long" | "float" | "double" | "bytes" | "string"
    )
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn detects_and_hoists_primitive_annotation() {
        // A temporal annotation on a primitive type is hoisted onto the field
        // and then recognized.
        let schema = r#"{
            "type": "record",
            "name": "Row",
            "fields": [
                { "name": "ts", "type": { "type": "long", "connect.name": "io.debezium.time.MicroTimestamp" } },
                { "name": "t", "type": ["null", { "type": "long", "connect.name": "io.debezium.time.NanoTime" }] },
                { "name": "e", "type": { "type": "string", "connect.name": "io.debezium.data.Enum" } },
                { "name": "plain", "type": "long" }
            ]
        }"#;

        let hoisted = hoist_coercible_types(schema).unwrap();
        let avro = AvroSchema::parse_str(&hoisted).unwrap();
        let AvroSchema::Record(record) = avro else {
            panic!("expected record schema");
        };
        let refs = AvroSchemaRefs::new();

        assert_eq!(
            field_coercion(
                &record.fields[0].custom_attributes,
                &record.fields[0].schema,
                &refs
            ),
            Some(Coercion::DebeziumTime(DebeziumTimeType::TimestampMicros))
        );
        assert_eq!(
            field_coercion(
                &record.fields[1].custom_attributes,
                &record.fields[1].schema,
                &refs
            ),
            Some(Coercion::DebeziumTime(DebeziumTimeType::TimeNanos))
        );
        // Non-temporal and plain fields are not coerced.
        assert_eq!(
            field_coercion(
                &record.fields[2].custom_attributes,
                &record.fields[2].schema,
                &refs
            ),
            None
        );
        assert_eq!(
            field_coercion(
                &record.fields[3].custom_attributes,
                &record.fields[3].schema,
                &refs
            ),
            None
        );
    }

    #[test]
    fn detects_named_type_annotation_without_hoisting() {
        // A record annotation is recognized natively, and the same annotation is
        // recognized through a by-name reference (the second field).
        let schema = r#"{
            "type": "record",
            "name": "Row",
            "fields": [
                { "name": "amount", "type": {
                    "type": "record",
                    "name": "VariableScaleDecimal",
                    "namespace": "io.debezium.data",
                    "fields": [
                        { "name": "scale", "type": "int" },
                        { "name": "value", "type": "bytes" }
                    ],
                    "connect.name": "io.debezium.data.VariableScaleDecimal"
                }},
                { "name": "amount_ref", "type": ["null", "io.debezium.data.VariableScaleDecimal"] }
            ]
        }"#;

        // The record annotation is not hoisted onto the field.
        let hoisted = hoist_coercible_types(schema).unwrap();
        let parsed: JsonValue = serde_json::from_str(&hoisted).unwrap();
        assert!(parsed["fields"][0].get("connect.name").is_none());

        let avro = AvroSchema::parse_str(&hoisted).unwrap();
        let resolved = apache_avro::schema::ResolvedSchema::try_from(&avro).unwrap();
        let refs: AvroSchemaRefs = resolved
            .get_names()
            .iter()
            .map(|(name, schema)| (name.clone(), (*schema).clone()))
            .collect();
        let AvroSchema::Record(record) = &avro else {
            panic!("expected record schema");
        };

        for field in &record.fields {
            assert_eq!(
                field_coercion(&field.custom_attributes, &field.schema, &refs),
                Some(Coercion::VariableScaleDecimal),
                "field {} not recognized",
                field.name
            );
        }
    }
}
