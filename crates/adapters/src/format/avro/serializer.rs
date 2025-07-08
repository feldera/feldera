use apache_avro::schema::{Name, NamesRef, RecordSchema};
use apache_avro::Decimal;
use apache_avro::{types::Value as AvroValue, Schema as AvroSchema};
use feldera_types::serde_with_context::serde_config::{BinaryFormat, DecimalFormat, UuidFormat};
use feldera_types::serde_with_context::{DateFormat, SqlSerdeConfig, TimeFormat, TimestampFormat};
use serde::ser::{
    Error as _, SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant,
    SerializeTuple, SerializeTupleStruct, SerializeTupleVariant,
};
use serde::{Serialize, Serializer};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::iter::once;
use std::mem::take;

use super::schema::{schema_unwrap_optional, OptionalField};

/// Serde configuration expected by [`AvroSchemaSerializer`].
pub fn avro_ser_config() -> SqlSerdeConfig {
    SqlSerdeConfig::default()
        .with_timestamp_format(TimestampFormat::MicrosSinceEpoch)
        .with_time_format(TimeFormat::Micros)
        .with_date_format(DateFormat::DaysSinceEpoch)
        .with_decimal_format(DecimalFormat::I128)
        .with_uuid_format(UuidFormat::String)
        .with_binary_format(BinaryFormat::Bytes)
}

#[derive(Debug)]
pub enum AvroSerializerError {
    OutOfBounds {
        value: String,
        typ: String,
        schema: Box<AvroSchema>,
    },
    Incompatible {
        typ: String,
        schema: Box<AvroSchema>,
    },
    MapKeyNotAString {
        key: Option<String>,
    },
    UnknownField {
        field: String,
        struct_name: String,
    },
    WrongNumberOfFields {
        typ: String,
        expected: usize,
        actual: usize,
    },
    UnknownRef {
        name: Name,
    },
    Custom {
        error: String,
    },
}

impl AvroSerializerError {
    fn out_of_bounds<V: Display>(value: &V, typ: &str, schema: &AvroSchema) -> Self {
        AvroSerializerError::OutOfBounds {
            value: value.to_string(),
            typ: typ.to_string(),
            schema: Box::new(schema.clone()),
        }
    }

    fn incompatible(typ: &str, schema: &AvroSchema) -> Self {
        AvroSerializerError::Incompatible {
            typ: typ.to_string(),
            schema: Box::new(schema.clone()),
        }
    }

    fn map_key_not_a_string(key: Option<String>) -> Self {
        AvroSerializerError::MapKeyNotAString { key }
    }

    fn unknown_field(field: &str, struct_name: String) -> Self {
        AvroSerializerError::UnknownField {
            field: field.to_string(),
            struct_name,
        }
    }

    fn unknown_ref(name: &Name) -> Self {
        AvroSerializerError::UnknownRef { name: name.clone() }
    }

    fn wrong_number_of_fields(typ: &str, expected: usize, actual: usize) -> Self {
        AvroSerializerError::WrongNumberOfFields {
            typ: typ.to_string(),
            expected,
            actual,
        }
    }
}

impl std::error::Error for AvroSerializerError {}

impl Display for AvroSerializerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AvroSerializerError::OutOfBounds { value, typ, schema } => {
                write!(
                    f,
                    "value '{value}' of type '{typ}' is out of bounds of AVRO type {schema:?}"
                )
            }
            AvroSerializerError::Incompatible { typ, schema } => {
                write!(
                    f,
                    "value of type '{typ}' cannot be serialized using AVRO schema {schema:?}"
                )
            }
            AvroSerializerError::MapKeyNotAString { key: Some(key) } => {
                write!(f, "map key '{key}' is not a string")
            }
            AvroSerializerError::MapKeyNotAString { key: None } => {
                write!(f, "map key is not a string")
            }
            AvroSerializerError::UnknownField { field, struct_name } => {
                write!(
                    f,
                    "field '{field}' is not a member of struct '{struct_name}'"
                )
            }
            AvroSerializerError::UnknownRef { name } => {
                write!(f, "unable to resolve Avro schema reference {name}")
            }
            AvroSerializerError::WrongNumberOfFields {
                typ,
                expected,
                actual,
            } => {
                write!(
                    f,
                    "wrong number of fields in '{typ}' (expected: {expected}, provided: {actual})"
                )
            }
            AvroSerializerError::Custom { error } => f.write_str(error),
        }
    }
}

impl serde::ser::Error for AvroSerializerError {
    fn custom<T>(msg: T) -> Self
    where
        T: Display,
    {
        Self::Custom {
            error: msg.to_string(),
        }
    }
}

pub struct StructSerializer<'a> {
    schema: &'a RecordSchema,
    optional: OptionalField,
    fields: Vec<(String, AvroValue)>,
    refs: &'a NamesRef<'a>,
    strict: bool,
}

impl<'a> StructSerializer<'a> {
    fn new(
        schema: &'a RecordSchema,
        refs: &'a NamesRef<'a>,
        strict: bool,
        optional: OptionalField,
    ) -> Self {
        Self {
            schema,
            refs,
            strict,
            optional,
            fields: Vec::with_capacity(schema.fields.len()),
        }
    }
}

impl SerializeStruct for StructSerializer<'_> {
    type Ok = AvroValue;
    type Error = AvroSerializerError;

    fn serialize_field<T>(&mut self, name: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize + ?Sized,
    {
        let Some(&field_schema_idx) = self.schema.lookup.get(name) else {
            if self.strict {
                return Err(AvroSerializerError::unknown_field(
                    name,
                    self.schema.name.to_string(),
                ));
            } else {
                return Ok(());
            }
        };
        self.fields.push((
            name.to_owned(),
            value.serialize(AvroSchemaSerializer::new(
                &self.schema.fields[field_schema_idx].schema,
                self.refs,
                self.strict,
            ))?,
        ));
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        match self.optional {
            OptionalField::NonOptional => Ok(AvroValue::Record(self.fields)),
            OptionalField::Optional(i) => Ok(AvroValue::Union(
                i,
                Box::new(AvroValue::Record(self.fields)),
            )),
        }
    }
}

pub struct SeqSerializer<'a> {
    schema: &'a AvroSchema,
    refs: &'a NamesRef<'a>,
    strict: bool,
    items: Vec<AvroValue>,
    optional: OptionalField,
}

impl<'a> SeqSerializer<'a> {
    fn new(
        schema: &'a AvroSchema,
        refs: &'a NamesRef<'a>,
        strict: bool,
        len: Option<usize>,
        optional: OptionalField,
    ) -> Self {
        Self {
            schema,
            refs,
            strict,
            items: Vec::with_capacity(len.unwrap_or(0)),
            optional,
        }
    }
}

impl SerializeSeq for SeqSerializer<'_> {
    type Ok = AvroValue;
    type Error = AvroSerializerError;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize + ?Sized,
    {
        self.items.push(value.serialize(AvroSchemaSerializer::new(
            self.schema,
            self.refs,
            self.strict,
        ))?);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        match self.optional {
            OptionalField::NonOptional => Ok(AvroValue::Array(self.items)),
            OptionalField::Optional(i) => {
                Ok(AvroValue::Union(i, Box::new(AvroValue::Array(self.items))))
            }
        }
    }
}

pub struct MapSerializer<'a> {
    schema: &'a AvroSchema,
    refs: &'a NamesRef<'a>,
    strict: bool,
    optional: OptionalField,
    key: String,
    map: HashMap<String, AvroValue>,
}

impl<'a> MapSerializer<'a> {
    fn new(
        schema: &'a AvroSchema,
        refs: &'a NamesRef<'a>,
        strict: bool,
        len: Option<usize>,
        optional: OptionalField,
    ) -> Self {
        Self {
            schema,
            refs,
            strict,
            optional,
            key: String::new(),
            map: HashMap::with_capacity(len.unwrap_or_default()),
        }
    }
}

impl SerializeMap for MapSerializer<'_> {
    type Ok = AvroValue;
    type Error = AvroSerializerError;

    fn serialize_key<T>(&mut self, key: &T) -> Result<(), Self::Error>
    where
        T: Serialize + ?Sized,
    {
        let key = key
            .serialize(AvroSchemaSerializer::new(
                &AvroSchema::String,
                self.refs,
                self.strict,
            ))
            .map_err(|_| AvroSerializerError::map_key_not_a_string(None))?;

        if let AvroValue::String(key) = key {
            self.key = key;
            Ok(())
        } else {
            Err(AvroSerializerError::map_key_not_a_string(Some(format!(
                "{key:?}"
            ))))
        }
    }

    fn serialize_value<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize + ?Sized,
    {
        let value = value.serialize(AvroSchemaSerializer::new(
            self.schema,
            self.refs,
            self.strict,
        ))?;
        self.map.insert(take(&mut self.key), value);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        match self.optional {
            OptionalField::NonOptional => Ok(AvroValue::Map(self.map)),
            OptionalField::Optional(i) => {
                Ok(AvroValue::Union(i, Box::new(AvroValue::Map(self.map))))
            }
        }
    }
}

// Never constructed, required by `trait Serializer`.
pub struct TupleSerializer;

impl SerializeTuple for TupleSerializer {
    type Ok = AvroValue;
    type Error = AvroSerializerError;

    fn serialize_element<T>(&mut self, _value: &T) -> Result<(), Self::Error>
    where
        T: Serialize + ?Sized,
    {
        unreachable!()
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        unreachable!()
    }
}

// Never constructed, required by `trait Serializer`.
pub struct TupleStructSerializer;

impl SerializeTupleStruct for TupleStructSerializer {
    type Ok = AvroValue;
    type Error = AvroSerializerError;

    fn serialize_field<T>(&mut self, _value: &T) -> Result<(), Self::Error>
    where
        T: Serialize + ?Sized,
    {
        unreachable!()
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        unreachable!()
    }
}

// Never constructed, required by `trait Serializer`.
pub struct TupleVariantSerializer;

impl SerializeTupleVariant for TupleVariantSerializer {
    type Ok = AvroValue;
    type Error = AvroSerializerError;

    fn serialize_field<T>(&mut self, _value: &T) -> Result<(), Self::Error>
    where
        T: Serialize + ?Sized,
    {
        unreachable!()
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        unreachable!()
    }
}

// Never constructed, required by `trait Serializer`.
pub struct StructVariantSerializer;

impl SerializeStructVariant for StructVariantSerializer {
    type Ok = AvroValue;
    type Error = AvroSerializerError;

    fn serialize_field<T>(&mut self, _key: &'static str, _value: &T) -> Result<(), Self::Error>
    where
        T: Serialize + ?Sized,
    {
        unreachable!()
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        unreachable!()
    }
}
fn serialize_maybe_optional<F>(schema: &AvroSchema, f: F) -> Result<AvroValue, AvroSerializerError>
where
    F: Fn(&AvroSchema) -> Result<AvroValue, AvroSerializerError>,
{
    match schema {
        AvroSchema::Union(union_schema) => match union_schema.variants() {
            [AvroSchema::Null, s] => Ok(AvroValue::Union(1, Box::new(f(s)?))),
            [s, AvroSchema::Null] => Ok(AvroValue::Union(0, Box::new(f(s)?))),
            _ => f(schema),
        },
        _ => f(schema),
    }
}

/// Serializer that encodes data into Avro [`Value`](apache_avro::types::Value)s
/// that match a schema.  This value can then be serialized into a binary representation.
///
/// Assumes that the `Serialize` implementation driving this serializer is generated by
/// the [`serialize_table_record`](feldera_types::serialize_table_record) macro and configured
/// using [`avro_serde_config`].
///
/// Performs the following transformations on data:
/// - Converts input timestamps and times in microseconds to microseconds or millisecond,
///   as required by the Avro schema.
/// - Converts decimals serialized as `u128` to Avro decimals by adjusting their scale
///   according to the Avro schema.
///
// FIXME: This is not the most efficient implementation. First, it traverses the Avro schema
// for every value it encodes. This is necessary to correctly encode times and decimals (see
// above).  The schema will be traversed again when encoding the value into bytes. Second,
// `Value` is an expensive representation to begin with, as it uses dynamic allocation for
// strings, arrays, and decimals.
pub struct AvroSchemaSerializer<'a> {
    schema: &'a AvroSchema,
    refs: &'a NamesRef<'a>,
    strict: bool,
}

impl<'a> AvroSchemaSerializer<'a> {
    pub fn new(schema: &'a AvroSchema, refs: &'a NamesRef<'a>, strict: bool) -> Self {
        Self {
            schema,
            refs,
            strict,
        }
    }

    fn resolve_ref(&self, schema: &'a AvroSchema) -> Result<&'a AvroSchema, AvroSerializerError> {
        match schema {
            AvroSchema::Ref { name } => {
                if let Some(schema) = self.refs.get(name) {
                    Ok(*schema)
                } else {
                    Err(AvroSerializerError::unknown_ref(name))
                }
            }
            _ => Ok(schema),
        }
    }
}

impl<'a> Serializer for AvroSchemaSerializer<'a> {
    type Ok = AvroValue;
    type Error = AvroSerializerError;
    type SerializeSeq = SeqSerializer<'a>;
    type SerializeTuple = TupleSerializer;
    type SerializeTupleStruct = TupleStructSerializer;
    type SerializeTupleVariant = TupleVariantSerializer;
    type SerializeMap = MapSerializer<'a>;
    type SerializeStruct = StructSerializer<'a>;
    type SerializeStructVariant = StructVariantSerializer;

    fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
        serialize_maybe_optional(self.schema, |_| Ok(AvroValue::Boolean(v)))
    }

    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        serialize_maybe_optional(self.schema, |_| Ok(AvroValue::Int(v as i32)))
    }

    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        serialize_maybe_optional(self.schema, |_| Ok(AvroValue::Int(v as i32)))
    }

    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        serialize_maybe_optional(self.schema, |schema| match schema {
            AvroSchema::Date => Ok(AvroValue::Date(v)),
            _ => Ok(AvroValue::Int(v)),
        })
    }

    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        serialize_maybe_optional(self.schema, |schema| match schema {
            AvroSchema::TimestampMicros => Ok(AvroValue::TimestampMicros(v)),
            AvroSchema::TimestampMillis => Ok(AvroValue::TimestampMillis(v / 1000)),
            _ => Ok(AvroValue::Long(v)),
        })
    }

    fn serialize_i128(self, v: i128) -> Result<Self::Ok, Self::Error> {
        serialize_maybe_optional(self.schema, |schema| match schema {
            AvroSchema::Decimal(_) => Ok(AvroValue::Decimal(Decimal::from(v.to_be_bytes()))),
            _ => Err(AvroSerializerError::incompatible("i128", schema)),
        })
    }

    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        serialize_maybe_optional(self.schema, |_| Ok(AvroValue::Int(v as i32)))
    }

    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        serialize_maybe_optional(self.schema, |_| Ok(AvroValue::Int(v as i32)))
    }

    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        serialize_maybe_optional(self.schema, |schema| match schema {
            AvroSchema::Long => Ok(AvroValue::Long(v as i64)),
            AvroSchema::Int if v <= i32::MAX as u32 => Ok(AvroValue::Int(v as i32)),
            AvroSchema::Int => Err(AvroSerializerError::out_of_bounds(&v, "u32", schema)),
            _ => Err(AvroSerializerError::incompatible("u32", schema)),
        })
    }

    fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
        serialize_maybe_optional(self.schema, |schema| match schema {
            AvroSchema::Long if v < i64::MAX as u64 => Ok(AvroValue::Long(v as i64)),
            AvroSchema::Long => Err(AvroSerializerError::out_of_bounds(&v, "u64", schema)),
            AvroSchema::TimeMicros => Ok(AvroValue::TimeMicros(v as i64)),
            AvroSchema::TimeMillis => Ok(AvroValue::TimeMillis((v / 1000) as i32)),
            _ => Err(AvroSerializerError::incompatible("u64", schema)),
        })
    }

    fn serialize_u128(self, _v: u128) -> Result<Self::Ok, Self::Error> {
        Err(AvroSerializerError::incompatible("u128", self.schema))
    }

    fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
        serialize_maybe_optional(self.schema, |schema| match schema {
            AvroSchema::Float => Ok(AvroValue::Float(v)),
            AvroSchema::Double => Ok(AvroValue::Double(v as f64)),
            _ => Err(AvroSerializerError::incompatible("f32", schema)),
        })
    }

    fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
        serialize_maybe_optional(self.schema, |schema| match schema {
            AvroSchema::Double => Ok(AvroValue::Double(v)),
            _ => Err(AvroSerializerError::incompatible("f64", schema)),
        })
    }

    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        serialize_maybe_optional(self.schema, |_| {
            Ok(AvroValue::String(once(v).collect::<String>()))
        })
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        serialize_maybe_optional(self.schema, |schema| match schema {
            AvroSchema::String => Ok(AvroValue::String(v.to_owned())),
            _ => Err(AvroSerializerError::incompatible("string", schema)),
        })
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        serialize_maybe_optional(self.schema, |schema| match schema {
            AvroSchema::Bytes => Ok(AvroValue::Bytes(v.to_vec())),
            AvroSchema::Fixed(fixed_schema) => {
                if v.len() == fixed_schema.size {
                    Ok(AvroValue::Fixed(fixed_schema.size, v.to_vec()))
                } else {
                    Err(AvroSerializerError::Custom {
                        error: format!("Error serializing byte array to Avro: Avro schema specifies size {}, but byte array has length {}", fixed_schema.size, v.len()),
                    })
                }
            }
            _ => Err(AvroSerializerError::incompatible("byte array", schema)),
        })
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        match self.schema {
            AvroSchema::Union(union_schema) => match union_schema.variants() {
                [AvroSchema::Null, _] => Ok(AvroValue::Union(0, Box::new(AvroValue::Null))),
                [_, AvroSchema::Null] => Ok(AvroValue::Union(1, Box::new(AvroValue::Null))),
                _ => Err(AvroSerializerError::incompatible("Option<>", self.schema)),
            },
            _ => Err(AvroSerializerError::incompatible("Option<>", self.schema)),
        }
    }

    fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize + ?Sized,
    {
        match self.schema {
            AvroSchema::Union(union_schema) => match union_schema.variants() {
                [AvroSchema::Null, inner_schema] => {
                    let serializer =
                        AvroSchemaSerializer::new(inner_schema, self.refs, self.strict);
                    let val = value.serialize(serializer)?;
                    Ok(AvroValue::Union(1, Box::new(val)))
                }
                [inner_schema, AvroSchema::Null] => {
                    let serializer =
                        AvroSchemaSerializer::new(inner_schema, self.refs, self.strict);
                    let val = value.serialize(serializer)?;
                    Ok(AvroValue::Union(0, Box::new(val)))
                }
                _ => Err(AvroSerializerError::incompatible("Option<>", self.schema)),
            },
            // Attempt to serialize Some(T) as T.
            _ => value.serialize(self),
        }
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        serialize_maybe_optional(self.schema, |_| Ok(AvroValue::Null))
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        serialize_maybe_optional(self.schema, |_| Ok(AvroValue::Null))
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        serialize_maybe_optional(self.schema, |schema| match schema {
            AvroSchema::Enum(_enum_schema) => {
                Ok(AvroValue::Enum(variant_index, variant.to_string()))
            }
            _ => Err(AvroSerializerError::incompatible(
                &format!("enum variant '{variant}'"),
                schema,
            )),
        })
    }

    fn serialize_newtype_struct<T>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize + ?Sized,
    {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T>(
        self,
        name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        _value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize + ?Sized,
    {
        Err(AvroSerializerError::custom(format!("unable to serialize newtype variant '{name}::{variant}': newtype variant serialization is not implemented")))
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        match schema_unwrap_optional(self.schema) {
            (AvroSchema::Array(schema), location) => Ok(SeqSerializer::new(
                &schema.items,
                self.refs,
                self.strict,
                len,
                location,
            )),
            _ => Err(AvroSerializerError::incompatible("sequence", self.schema)),
        }
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Err(AvroSerializerError::custom(format!(
            "unable to serialize a {len}-tuple: tuple serialization is not implemented"
        )))
    }

    fn serialize_tuple_struct(
        self,
        name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Err(AvroSerializerError::custom(format!("unable to serialize a tuple struct {name}: tuple struct serialization is not implemented")))
    }

    fn serialize_tuple_variant(
        self,
        name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Err(AvroSerializerError::custom(format!("unable to serialize tuple variant '{name}::{variant}': tuple variant serialization is not implemented")))
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        match schema_unwrap_optional(self.schema) {
            (AvroSchema::Map(schema), optional) => Ok(MapSerializer::new(
                &schema.types,
                self.refs,
                self.strict,
                len,
                optional,
            )),
            _ => Err(AvroSerializerError::incompatible("map", self.schema)),
        }
    }

    fn serialize_struct(
        self,
        struct_name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        let (schema, optional) = schema_unwrap_optional(self.schema);
        let schema = self.resolve_ref(schema)?;
        match (schema, optional) {
            (AvroSchema::Record(record_schema), optional)
                if !self.strict || record_schema.fields.len() == len =>
            {
                Ok(StructSerializer::new(
                    record_schema,
                    self.refs,
                    self.strict,
                    optional,
                ))
            }
            (AvroSchema::Record(record_schema), _) => {
                Err(AvroSerializerError::wrong_number_of_fields(
                    &format!("struct {struct_name}"),
                    record_schema.fields.len(),
                    len,
                ))
            }
            _ => Err(AvroSerializerError::incompatible(
                &format!("struct {struct_name}"),
                self.schema,
            )),
        }
    }

    fn serialize_struct_variant(
        self,
        name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Err(AvroSerializerError::custom(format!("unable to serialize struct variant '{name}::{variant}': struct variant serialization is not implemented")))
    }

    fn is_human_readable(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod test {
    use crate::format::avro::serializer::{avro_ser_config, AvroSchemaSerializer};
    use crate::test::{EmbeddedStruct, TestStruct2};
    use apache_avro::schema::ResolvedSchema;
    use apache_avro::{
        from_avro_datum, to_avro_datum, types::Value as AvroValue, Decimal, Schema as AvroSchema,
    };
    use dbsp::algebra::{F32, F64};
    use feldera_sqllib::{Date, SqlDecimal, Timestamp};
    use feldera_types::{serde_with_context::SerializeWithContext, serialize_table_record};
    use num_bigint::BigInt;
    use serde::Serialize;
    use std::collections::{BTreeMap, HashMap};
    use std::str::FromStr;

    #[derive(Serialize)]
    struct TestStruct1 {
        id: u32,
        name: Option<String>,
        b: Option<bool>,
    }

    serialize_table_record!(TestStruct1[3] {
        id["id"]: u32,
        name["name"]: Option<String>,
        b["b"]: Option<bool>
    });

    const SCHEMA1: &str = r#"{
  "type": "record",
  "name": "Test1",
  "fields": [
    { "name": "id", "type": "int" },
    { "name": "name", "type": ["string", "null"] },
    { "name": "b", "type": ["boolean", "null"] }
  ]
}"#;

    #[derive(Serialize)]
    struct TestNumeric {
        float32: F32,
        float64: F64,
        dec1: SqlDecimal<4, 2>,
        dec2: SqlDecimal<8, 0>,
        dec3: SqlDecimal<6, 3>,
    }

    serialize_table_record!(TestNumeric[5] {
        float32["float32"]: F32,
        float64["float64"]: F64,
        dec1["dec1"]: SqlDecimal,
        dec2["dec2"]: SqlDecimal,
        dec3["dec3"]: SqlDecimal
    });

    const SCHEMA_NUMERIC: &str = r#"{
  "type": "record",
  "name": "Numeric",
  "fields": [
    { "name": "float32", "type": "float" },
    { "name": "float64", "type": "double" },
    { "name": "dec1", "type": {"type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 2} },
    { "name": "dec2", "type": {"type": "bytes", "logicalType": "decimal", "precision": 8, "scale": 0} },
    { "name": "dec3", "type": {"type": "bytes", "logicalType": "decimal", "precision": 6, "scale": 3} }
  ]
}"#;

    // Serializer should be able to serialize non-nullable fields into nullable schema.
    const SCHEMA_NUMERIC_OPTIONAL: &str = r#"{
    "type": "record",
    "name": "Numeric",
    "fields": [
      { "name": "float32", "type": ["float", "null"] },
      { "name": "float64", "type": ["double", "null"] },
      { "name": "dec1", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 2}] },
      { "name": "dec2", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 8, "scale": 0}] },
      { "name": "dec3", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 6, "scale": 3}] }
    ]
  }"#;

    macro_rules! serializer_test {
        ($schema: expr, $record: ident, $avro: ident) => {
            let schema = serde_json::Value::from_str($schema).unwrap();
            let schema = AvroSchema::parse(&schema).unwrap();
            let resolved =
                ResolvedSchema::new_with_known_schemata(vec![&schema], &None, &HashMap::new())
                    .unwrap();

            let serializer = AvroSchemaSerializer::new(&schema, resolved.get_names(), true);
            let val = $record
                .serialize_with_context(serializer, &avro_ser_config())
                .unwrap();
            assert_eq!(val, $avro);

            let avro = to_avro_datum(&schema, val.clone()).unwrap();
            let parsed_val = from_avro_datum::<&[u8]>(&schema, &mut avro.as_ref(), None).unwrap();
            assert_eq!(val, parsed_val);
        };
    }

    #[test]
    fn test_avro_serializer() {
        let record1_1: TestStruct1 = TestStruct1 {
            id: 1,
            name: Some("foo".to_string()),
            b: Some(true),
        };

        let avro1_1: AvroValue = AvroValue::Record(vec![
            ("id".to_string(), AvroValue::Int(1)),
            (
                "name".to_string(),
                AvroValue::Union(0, Box::new(AvroValue::String("foo".to_string()))),
            ),
            (
                "b".to_string(),
                AvroValue::Union(0, Box::new(AvroValue::Boolean(true))),
            ),
        ]);

        let record1_2: TestStruct1 = TestStruct1 {
            id: 2,
            name: None,
            b: None,
        };

        let avro1_2: AvroValue = AvroValue::Record(vec![
            ("id".to_string(), AvroValue::Int(2)),
            (
                "name".to_string(),
                AvroValue::Union(1, Box::new(AvroValue::Null)),
            ),
            (
                "b".to_string(),
                AvroValue::Union(1, Box::new(AvroValue::Null)),
            ),
        ]);

        let record2_1: TestStruct2 = TestStruct2 {
            field: 1,
            field_0: Some("foo".to_string()),
            field_1: false,
            field_2: Timestamp::new(1713597703),
            field_3: Date::new(19833),
            field_5: Some(EmbeddedStruct { field: true }),
            field_6: Some(BTreeMap::from([
                ("foo".to_string(), 1),
                ("bar".to_string(), 2),
            ])),
            field_7: SqlDecimal::<10, 3>::new(10000i128, 3i32).unwrap(),
        };

        let avro2_1: AvroValue = AvroValue::Record(vec![
            ("id".to_string(), AvroValue::Long(1)),
            (
                "nAmE".to_string(),
                AvroValue::Union(0, Box::new(AvroValue::String("foo".to_string()))),
            ),
            ("b".to_string(), AvroValue::Boolean(false)),
            ("ts".to_string(), AvroValue::TimestampMicros(1713597703000)),
            ("dt".to_string(), AvroValue::Date(19833)),
            (
                "es".to_string(),
                AvroValue::Union(
                    0,
                    Box::new(AvroValue::Record(vec![(
                        "a".to_string(),
                        AvroValue::Boolean(true),
                    )])),
                ),
            ),
            (
                "m".to_string(),
                AvroValue::Union(
                    0,
                    Box::new(AvroValue::Map(HashMap::from([
                        ("foo".to_string(), AvroValue::Long(1)),
                        ("bar".to_string(), AvroValue::Long(2)),
                    ]))),
                ),
            ),
            (
                "dec".to_string(),
                AvroValue::Decimal(Decimal::from(&BigInt::from(10000).to_signed_bytes_be())),
            ),
        ]);

        let record_numeric_1: TestNumeric = TestNumeric {
            float32: F32::new(123.45),
            float64: F64::new(12345E-5),
            dec1: "12.34".parse().unwrap(),
            dec2: "1234.56".parse().unwrap(),
            dec3: "123.12345".parse().unwrap(),
        };

        let avro_numeric_1: AvroValue = AvroValue::Record(vec![
            ("float32".to_string(), AvroValue::Float(123.45)),
            ("float64".to_string(), AvroValue::Double(12345E-5)),
            (
                "dec1".to_string(),
                AvroValue::Decimal(Decimal::from(&BigInt::from(1234).to_signed_bytes_be())),
            ),
            (
                "dec2".to_string(),
                AvroValue::Decimal(Decimal::from(&BigInt::from(1235).to_signed_bytes_be())),
            ),
            (
                "dec3".to_string(),
                AvroValue::Decimal(Decimal::from(&BigInt::from(123123).to_signed_bytes_be())),
            ),
        ]);

        let avro_numeric_1_optional: AvroValue = AvroValue::Record(vec![
            (
                "float32".to_string(),
                AvroValue::Union(0, Box::new(AvroValue::Float(123.45))),
            ),
            (
                "float64".to_string(),
                AvroValue::Union(0, Box::new(AvroValue::Double(12345E-5))),
            ),
            (
                "dec1".to_string(),
                AvroValue::Union(
                    1,
                    Box::new(AvroValue::Decimal(Decimal::from(
                        &BigInt::from(1234).to_signed_bytes_be(),
                    ))),
                ),
            ),
            (
                "dec2".to_string(),
                AvroValue::Union(
                    1,
                    Box::new(AvroValue::Decimal(Decimal::from(
                        &BigInt::from(1235).to_signed_bytes_be(),
                    ))),
                ),
            ),
            (
                "dec3".to_string(),
                AvroValue::Union(
                    1,
                    Box::new(AvroValue::Decimal(Decimal::from(
                        &BigInt::from(123123).to_signed_bytes_be(),
                    ))),
                ),
            ),
        ]);

        serializer_test!(SCHEMA1, record1_1, avro1_1);
        serializer_test!(SCHEMA1, record1_2, avro1_2);
        serializer_test!(TestStruct2::avro_schema(), record2_1, avro2_1);
        serializer_test!(SCHEMA_NUMERIC, record_numeric_1, avro_numeric_1);
        serializer_test!(
            SCHEMA_NUMERIC_OPTIONAL,
            record_numeric_1,
            avro_numeric_1_optional
        );
    }
}
