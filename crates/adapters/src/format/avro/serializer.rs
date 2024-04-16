use apache_avro::schema::RecordSchema;
use apache_avro::{types::Value as AvroValue, Decimal, Schema as AvroSchema};
use pipeline_types::serde_with_context::serde_config::DecimalFormat;
use pipeline_types::serde_with_context::{DateFormat, SqlSerdeConfig, TimeFormat, TimestampFormat};
use rust_decimal::Decimal as RustDecimal;
use serde::ser::{
    Error as _, SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant,
    SerializeTuple, SerializeTupleStruct, SerializeTupleVariant,
};
use serde::{Serialize, Serializer};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::iter::once;
use std::mem::take;

/// Serde configuration expected by [`AvroSchemaSerializer`].
pub fn avro_serde_config() -> SqlSerdeConfig {
    SqlSerdeConfig::default()
        .with_timestamp_format(TimestampFormat::MicrosSinceEpoch)
        .with_time_format(TimeFormat::Micros)
        .with_date_format(DateFormat::DaysSinceEpoch)
        .with_decimal_format(DecimalFormat::U128)
}

#[derive(Debug)]
pub enum AvroSerializerError {
    OutOfBounds {
        value: String,
        typ: String,
        schema: AvroSchema,
    },
    Incompatible {
        typ: String,
        schema: AvroSchema,
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
    Custom {
        error: String,
    },
}

impl AvroSerializerError {
    fn out_of_bounds<V: Display>(value: &V, typ: &str, schema: &AvroSchema) -> Self {
        AvroSerializerError::OutOfBounds {
            value: value.to_string(),
            typ: typ.to_string(),
            schema: schema.clone(),
        }
    }

    fn incompatible(typ: &str, schema: &AvroSchema) -> Self {
        AvroSerializerError::Incompatible {
            typ: typ.to_string(),
            schema: schema.clone(),
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
    fields: Vec<(String, AvroValue)>,
}

impl<'a> StructSerializer<'a> {
    pub fn new(schema: &'a RecordSchema) -> Self {
        Self {
            schema,
            fields: Vec::with_capacity(schema.fields.len()),
        }
    }
}

impl<'a> SerializeStruct for StructSerializer<'a> {
    type Ok = AvroValue;
    type Error = AvroSerializerError;

    fn serialize_field<T: ?Sized>(
        &mut self,
        name: &'static str,
        value: &T,
    ) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        let field_schema_idx = *self.schema.lookup.get(name).ok_or_else(|| {
            AvroSerializerError::unknown_field(name, self.schema.name.to_string())
        })?;
        self.fields.push((
            name.to_owned(),
            value.serialize(AvroSchemaSerializer::new(
                &self.schema.fields[field_schema_idx].schema,
            ))?,
        ));
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(AvroValue::Record(self.fields))
    }
}

pub struct SeqSerializer<'a> {
    schema: &'a AvroSchema,
    items: Vec<AvroValue>,
}

impl<'a> SeqSerializer<'a> {
    fn new(schema: &'a AvroSchema, len: Option<usize>) -> Self {
        Self {
            schema,
            items: Vec::with_capacity(len.unwrap_or(0)),
        }
    }
}

impl<'a> SerializeSeq for SeqSerializer<'a> {
    type Ok = AvroValue;
    type Error = AvroSerializerError;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        self.items
            .push(value.serialize(AvroSchemaSerializer::new(self.schema))?);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(AvroValue::Array(self.items))
    }
}

pub struct MapSerializer<'a> {
    schema: &'a AvroSchema,
    key: String,
    map: HashMap<String, AvroValue>,
}

impl<'a> MapSerializer<'a> {
    fn new(schema: &'a AvroSchema, len: Option<usize>) -> Self {
        Self {
            schema,
            key: String::new(),
            map: HashMap::with_capacity(len.unwrap_or_default()),
        }
    }
}

impl<'a> SerializeMap for MapSerializer<'a> {
    type Ok = AvroValue;
    type Error = AvroSerializerError;

    fn serialize_key<T: ?Sized>(&mut self, key: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        let key = key
            .serialize(AvroSchemaSerializer::new(&AvroSchema::String))
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

    fn serialize_value<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        let value = value.serialize(AvroSchemaSerializer::new(self.schema))?;
        self.map.insert(take(&mut self.key), value);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(AvroValue::Map(self.map))
    }
}

// Never constructed, required by `trait Serializer`.
pub struct TupleSerializer;

impl SerializeTuple for TupleSerializer {
    type Ok = AvroValue;
    type Error = AvroSerializerError;

    fn serialize_element<T: ?Sized>(&mut self, _value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
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

    fn serialize_field<T: ?Sized>(&mut self, _value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
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

    fn serialize_field<T: ?Sized>(&mut self, _value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
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

    fn serialize_field<T: ?Sized>(
        &mut self,
        _key: &'static str,
        _value: &T,
    ) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        unreachable!()
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        unreachable!()
    }
}

/// Serializer that encodes data into Avro [`Value`](apache_avro::types::Value)s
/// that match a schema.  This value can then be serialized into a binary representation.
///
/// Assumes that the `Serialize` implementation driving this serializer is generated by
/// the [`serialize_table_record`](pipeline_types::serialize_table_record) macro and configured
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
}

impl<'a> AvroSchemaSerializer<'a> {
    pub fn new(schema: &'a AvroSchema) -> Self {
        Self { schema }
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
        Ok(AvroValue::Boolean(v))
    }

    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        Ok(AvroValue::Int(v as i32))
    }

    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        Ok(AvroValue::Int(v as i32))
    }

    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        match self.schema {
            AvroSchema::Date => Ok(AvroValue::Date(v)),
            _ => Ok(AvroValue::Int(v)),
        }
    }

    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        match self.schema {
            AvroSchema::TimestampMicros => Ok(AvroValue::TimestampMicros(v)),
            AvroSchema::TimestampMillis => Ok(AvroValue::TimestampMillis(v / 1000)),
            _ => Ok(AvroValue::Long(v)),
        }
    }

    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        Ok(AvroValue::Int(v as i32))
    }

    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        Ok(AvroValue::Int(v as i32))
    }

    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        match self.schema {
            AvroSchema::Long => Ok(AvroValue::Long(v as i64)),
            AvroSchema::Int if v <= i32::MAX as u32 => Ok(AvroValue::Int(v as i32)),
            AvroSchema::Int => Err(AvroSerializerError::out_of_bounds(&v, "u32", self.schema)),
            _ => Err(AvroSerializerError::incompatible("u32", self.schema)),
        }
    }

    fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
        match self.schema {
            AvroSchema::Long if v < i64::MAX as u64 => Ok(AvroValue::Long(v as i64)),
            AvroSchema::Long => Err(AvroSerializerError::out_of_bounds(&v, "u64", self.schema)),
            AvroSchema::TimeMicros => Ok(AvroValue::TimeMicros(v as i64)),
            AvroSchema::TimeMillis => Ok(AvroValue::TimeMillis((v / 1000) as i32)),
            _ => Err(AvroSerializerError::incompatible("u64", self.schema)),
        }
    }

    fn serialize_u128(self, v: u128) -> Result<Self::Ok, Self::Error> {
        match self.schema {
            AvroSchema::Decimal(decimal_schema) => {
                let mut decimal = RustDecimal::deserialize(v.to_be_bytes());
                decimal.rescale(decimal_schema.scale as u32);
                if decimal.round_sf(decimal_schema.precision as u32) != Some(decimal) {
                    return Err(AvroSerializerError::out_of_bounds(
                        &v,
                        "decimal",
                        &AvroSchema::Decimal(decimal_schema.clone()),
                    ));
                }
                Ok(AvroValue::Decimal(Decimal::from(
                    decimal.mantissa().to_be_bytes(),
                )))
            }
            _ => Err(AvroSerializerError::incompatible("u128", self.schema)),
        }
    }

    fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
        match self.schema {
            AvroSchema::Float => Ok(AvroValue::Float(v)),
            AvroSchema::Double => Ok(AvroValue::Double(v as f64)),
            _ => Err(AvroSerializerError::incompatible("f32", self.schema)),
        }
    }

    fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
        match self.schema {
            AvroSchema::Double => Ok(AvroValue::Double(v)),
            _ => Err(AvroSerializerError::incompatible("f64", self.schema)),
        }
    }

    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        self.serialize_str(&once(v).collect::<String>())
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        Ok(AvroValue::String(v.to_owned()))
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        match self.schema {
            AvroSchema::Bytes => Ok(AvroValue::Bytes(v.to_vec())),
            _ => Err(AvroSerializerError::incompatible("byte array", self.schema)),
        }
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

    fn serialize_some<T: ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
    {
        match self.schema {
            AvroSchema::Union(union_schema) => match union_schema.variants() {
                [AvroSchema::Null, inner_schema] => {
                    let serializer = AvroSchemaSerializer::new(inner_schema);
                    let val = value.serialize(serializer)?;
                    Ok(AvroValue::Union(1, Box::new(val)))
                }
                [inner_schema, AvroSchema::Null] => {
                    let serializer = AvroSchemaSerializer::new(inner_schema);
                    let val = value.serialize(serializer)?;
                    Ok(AvroValue::Union(0, Box::new(val)))
                }
                _ => Err(AvroSerializerError::incompatible("Option<>", self.schema)),
            },
            _ => Err(AvroSerializerError::incompatible("Option<>", self.schema)),
        }
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Ok(AvroValue::Null)
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        Ok(AvroValue::Null)
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        match self.schema {
            AvroSchema::Enum(_enum_schema) => {
                Ok(AvroValue::Enum(variant_index, variant.to_string()))
            }
            _ => Err(AvroSerializerError::incompatible(
                &format!("enum variant '{variant}'"),
                self.schema,
            )),
        }
    }

    fn serialize_newtype_struct<T: ?Sized>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
    {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T: ?Sized>(
        self,
        name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        _value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
    {
        Err(AvroSerializerError::custom(format!("unable to serialize newtype variant '{name}::{variant}': newtype variant serialization is not implemented")))
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        match self.schema {
            AvroSchema::Array(schema) => Ok(SeqSerializer::new(schema, len)),
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
        match self.schema {
            AvroSchema::Map(schema) => Ok(MapSerializer::new(schema, len)),
            _ => Err(AvroSerializerError::incompatible("map", self.schema)),
        }
    }

    fn serialize_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        match self.schema {
            AvroSchema::Record(record_schema) if record_schema.fields.len() == len => {
                Ok(StructSerializer::new(record_schema))
            }
            AvroSchema::Record(record_schema) => Err(AvroSerializerError::wrong_number_of_fields(
                &format!("struct {name}"),
                record_schema.fields.len(),
                len,
            )),
            _ => Err(AvroSerializerError::incompatible(
                &format!("struct {name}"),
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
    use crate::format::avro::serializer::{avro_serde_config, AvroSchemaSerializer};
    use crate::test::{EmbeddedStruct, TestStruct2};
    use apache_avro::{
        from_avro_datum, to_avro_datum, types::Value as AvroValue, Decimal, Schema as AvroSchema,
    };
    use dbsp::algebra::{F32, F64};
    use num_bigint::BigInt;
    use pipeline_types::{serde_with_context::SerializeWithContext, serialize_table_record};
    use rust_decimal::Decimal as RustDecimal;
    use rust_decimal_macros::dec;
    use serde::Serialize;
    use sqllib::{Date, Timestamp};
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

    const SCHEMA1: &'static str = r#"{
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
        dec1: RustDecimal,
        dec2: RustDecimal,
        dec3: RustDecimal,
    }

    serialize_table_record!(TestNumeric[5] {
        float32["float32"]: F32,
        float64["float64"]: F64,
        dec1["dec1"]: RustDecimal,
        dec2["dec2"]: RustDecimal,
        dec3["dec3"]: RustDecimal
    });

    const SCHEMA_NUMERIC: &'static str = r#"{
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

    macro_rules! serializer_test {
        ($schema: expr, $record: ident, $avro: ident) => {
            let schema = serde_json::Value::from_str($schema).unwrap();
            let schema = AvroSchema::parse(&schema).unwrap();

            let serializer = AvroSchemaSerializer::new(&schema);
            let val = $record
                .serialize_with_context(serializer, &avro_serde_config())
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
            field_5: EmbeddedStruct { field: true },
        };

        let avro2_1: AvroValue = AvroValue::Record(vec![
            ("id".to_string(), AvroValue::Long(1)),
            (
                "name".to_string(),
                AvroValue::Union(0, Box::new(AvroValue::String("foo".to_string()))),
            ),
            ("b".to_string(), AvroValue::Boolean(false)),
            ("ts".to_string(), AvroValue::TimestampMicros(1713597703000)),
            ("dt".to_string(), AvroValue::Date(19833)),
            (
                "es".to_string(),
                AvroValue::Record(vec![("a".to_string(), AvroValue::Boolean(true))]),
            ),
        ]);

        let record_numeric_1: TestNumeric = TestNumeric {
            float32: F32::new(123.45),
            float64: F64::new(12345E-5),
            dec1: dec!(12.34),
            dec2: dec!(1234.56),
            dec3: dec!(123.12345),
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

        serializer_test!(SCHEMA1, record1_1, avro1_1);
        serializer_test!(SCHEMA1, record1_2, avro1_2);
        serializer_test!(TestStruct2::avro_schema(), record2_1, avro2_1);
        serializer_test!(SCHEMA_NUMERIC, record_numeric_1, avro_numeric_1);
    }
}
