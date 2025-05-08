//! Deserializer backed by the Avro `Value` type, adapted from the `apache-avro`
//! crate with several modifications:
//!
//! * Convert time and timestamp values from microseconds to milliseconds, as
//!   expected by our `Timestamp` and `Time` types.
//! * Deserializes optional types from non-nullable schemas (normally
//!   `Option<T>` can only be serialized from an Avro value of the shape `["null", V]`).
//! * Deserializes non-optional types from nullable schemas.

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use apache_avro::{
    schema::{RecordSchema, SchemaKind},
    types::Value,
    BigDecimal, Decimal, Error, Schema,
};
use erased_serde::Deserializer as ErasedDeserializer;
use feldera_types::serde_with_context::{DeserializeWithContext, SqlSerdeConfig};
use num_bigint::BigInt;
use serde::{
    de::{self, DeserializeSeed, Visitor},
    forward_to_deserialize_any,
};
use std::{
    collections::{
        hash_map::{Keys, Values},
        HashMap,
    },
    slice::Iter,
};

use super::input::avro_de_config;

fn deserialize_decimal<E: serde::de::Error>(d: &Decimal, schema: &Schema) -> Result<String, E> {
    let Schema::Decimal(schema) = schema else {
        return Err(E::custom(format!(
            "expected Decimal schema, but found {schema:?}"
        )));
    };

    // TODO: this is really expensive, and can be optimized by passing a binary representation of
    // mantissa + scale. This will require a custom `DeserializeWithContext` implementation for
    // SqlDecimal.
    let bigdecimal = BigDecimal::new(BigInt::from(d.clone()), schema.scale as i64);
    Ok(bigdecimal.to_string())
}

pub struct Deserializer<'de> {
    // We have to keep the schema around, since `Value` isn't sufficiently self-descriptive
    // to deserialize from it. Specifically, `Decimal` only stores the mantissa; scale must be
    // extracted from the schema.
    schema: &'de Schema,
    input: &'de Value,
}

struct SeqDeserializer<'de> {
    input: Iter<'de, Value>,
    item_schema: &'de Schema,
}

struct MapDeserializer<'de> {
    value_schema: &'de Schema,
    input_keys: Keys<'de, String, Value>,
    input_values: Values<'de, String, Value>,
}

struct RecordDeserializer<'de> {
    record_schema: &'de RecordSchema,
    input: Iter<'de, (String, Value)>,
    value: Option<&'de Value>,
    next_field_index: usize,
}

impl<'de> Deserializer<'de> {
    pub fn new(input: &'de Value, schema: &'de Schema) -> Self {
        Deserializer { input, schema }
    }
}

impl<'de> SeqDeserializer<'de> {
    pub fn new<E: de::Error>(input: &'de [Value], schema: &'de Schema) -> Result<Self, E> {
        let Schema::Array(array_schema) = schema else {
            return Err(de::Error::custom(format!(
                "expected array schema, found {schema:?}"
            )));
        };

        Ok(SeqDeserializer {
            input: input.iter(),
            item_schema: array_schema.items.as_ref(),
        })
    }
}

impl<'de> MapDeserializer<'de> {
    pub fn new<E: de::Error>(
        input: &'de HashMap<String, Value>,
        schema: &'de Schema,
    ) -> Result<Self, E> {
        let Schema::Map(map_schema) = schema else {
            return Err(de::Error::custom(format!(
                "expected map schema, found {schema:?}"
            )));
        };

        Ok(MapDeserializer {
            value_schema: map_schema.types.as_ref(),
            input_keys: input.keys(),
            input_values: input.values(),
        })
    }
}

impl<'de> RecordDeserializer<'de> {
    pub fn new<E: de::Error>(
        input: &'de [(String, Value)],
        schema: &'de Schema,
    ) -> Result<Self, E> {
        let Schema::Record(record_schema) = schema else {
            return Err(de::Error::custom(format!(
                "expected record schema, found {schema:?}"
            )));
        };

        Ok(RecordDeserializer {
            record_schema,
            input: input.iter(),
            value: None,
            next_field_index: 0,
        })
    }
}

fn unwrap_union<'de>(v: &'de Value, schema: &'de Schema) -> (&'de Value, &'de Schema) {
    match (v, schema) {
        (Value::Union(index, v), Schema::Union(schema)) => {
            (v.as_ref(), &schema.variants()[*index as usize])
        }
        _ => (v, schema),
    }
}

impl<'de> de::Deserializer<'de> for &'_ Deserializer<'de> {
    type Error = Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let (val, schema) = unwrap_union(self.input, self.schema);
        match  val {
            Value::Null => visitor.visit_unit(),
            &Value::Boolean(b) => visitor.visit_bool(b),
            Value::Int(i) | Value::Date(i) => visitor.visit_i32(*i),
            Value::Long(i)
            | Value::TimeMicros(i)
            | Value::TimestampMicros(i)
            //| Value::TimestampNanos(i)
            | Value::LocalTimestampMicros(i)
            /*| Value::LocalTimestampNanos(i)*/ => visitor.visit_i64(*i),
            // Convert millis to micros
            Value::TimestampMillis(i) | Value::LocalTimestampMillis(i) => visitor.visit_i64(i * 1000),
            Value::TimeMillis(i) => visitor.visit_i64(*i as i64 * 1000),
            Value::Float(f) => visitor.visit_f32(*f),
            Value::Double(d) => visitor.visit_f64(*d),
            Value::Record(ref fields) => visitor.visit_map(RecordDeserializer::new(fields, schema)?),
            Value::Array(ref fields) => visitor.visit_seq(SeqDeserializer::new(fields, schema)?),
            Value::String(ref s) => visitor.visit_borrowed_str(s),
            Value::Uuid(uuid) => visitor.visit_str(&uuid.to_string()),
            Value::Map(ref items) => visitor.visit_map(MapDeserializer::new(items, schema)?),
            Value::Bytes(ref bytes) | Value::Fixed(_, ref bytes) => visitor.visit_bytes(bytes),
            Value::Decimal(ref d) => visitor.visit_str(&deserialize_decimal(d, schema)?),
            value => Err(de::Error::custom(format!(
                "incorrect value of type: {:?}",
                SchemaKind::from(value)
            ))),
        }
    }

    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64
    }

    fn deserialize_char<V>(self, _: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(de::Error::custom("avro does not support char"))
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match unwrap_union(self.input, self.schema).0 {
            Value::String(ref s) => visitor.visit_borrowed_str(s),
            Value::Bytes(ref bytes) | Value::Fixed(_, ref bytes) => ::std::str::from_utf8(bytes)
                .map_err(|e| de::Error::custom(e.to_string()))
                .and_then(|s| visitor.visit_borrowed_str(s)),
            Value::Uuid(ref u) => visitor.visit_str(&u.to_string()),
            v => Err(de::Error::custom(format!(
                "expected a String|Bytes|Fixed|Uuid, but got {v:?}"
            ))),
        }
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match unwrap_union(self.input, self.schema).0 {
            Value::Enum(_, ref s) | Value::String(ref s) => visitor.visit_borrowed_str(s),
            Value::Bytes(ref bytes) | Value::Fixed(_, ref bytes) => {
                String::from_utf8(bytes.to_owned())
                    .map_err(|e| de::Error::custom(e.to_string()))
                    .and_then(|s| visitor.visit_string(s))
            }
            Value::Uuid(ref u) => visitor.visit_str(&u.to_string()),
            v => Err(de::Error::custom(format!(
                "expected a String|Bytes|Fixed|Uuid|Union|Enum, but got {v:?}"
            ))),
        }
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let (value, schema) = unwrap_union(self.input, self.schema);

        match value {
            Value::String(ref s) => visitor.visit_bytes(s.as_bytes()),
            Value::Bytes(ref bytes) | Value::Fixed(_, ref bytes) => visitor.visit_bytes(bytes),
            Value::Uuid(ref u) => visitor.visit_bytes(u.as_bytes()),
            Value::Decimal(ref d) => visitor.visit_str(&deserialize_decimal(d, schema)?),
            v => Err(de::Error::custom(format!(
                "expected a String|Bytes|Fixed|Uuid|Decimal, but got {v:?}",
            ))),
        }
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match unwrap_union(self.input, self.schema).0 {
            Value::String(ref s) => visitor.visit_byte_buf(s.clone().into_bytes()),
            Value::Bytes(ref bytes) | Value::Fixed(_, ref bytes) => {
                visitor.visit_byte_buf(bytes.to_owned())
            }
            v => Err(de::Error::custom(format!(
                "expected a String|Bytes|Fixed, but got {v:?}",
            ))),
        }
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.input {
            Value::Union(_i, inner) if inner.as_ref() == &Value::Null => visitor.visit_none(),
            Value::Union(i, inner) => {
                let Schema::Union(union_schema) = self.schema else {
                    return Err(de::Error::custom(format!(
                        "expected union schema, but found {:?}",
                        self.schema
                    )));
                };
                visitor.visit_some(&Deserializer::new(
                    inner,
                    &union_schema.variants()[*i as usize],
                ))
            }
            v => visitor.visit_some(&Deserializer::new(v, self.schema)),
        }
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match unwrap_union(self.input, self.schema).0 {
            Value::Null => visitor.visit_unit(),
            v => Err(de::Error::custom(
                format!("expected a Null, but got {v:?}",),
            )),
        }
    }

    fn deserialize_unit_struct<V>(
        self,
        _struct_name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_unit(visitor)
    }

    fn deserialize_newtype_struct<V>(
        self,
        _struct_name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let (value, schema) = unwrap_union(self.input, self.schema);
        match value {
            Value::Array(ref items) => visitor.visit_seq(SeqDeserializer::new(items, schema)?),
            Value::Null => visitor.visit_seq(SeqDeserializer::new(&[], schema)?),
            v => Err(de::Error::custom(format!(
                "expected an Array or Null, but got: {v:?}",
            ))),
        }
    }

    fn deserialize_tuple<V>(self, _: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_seq(visitor)
    }

    fn deserialize_tuple_struct<V>(
        self,
        _struct_name: &'static str,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_seq(visitor)
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let (value, schema) = unwrap_union(self.input, self.schema);
        match value {
            Value::Map(ref items) => visitor.visit_map(MapDeserializer::new(items, schema)?),
            v => Err(de::Error::custom(format_args!(
                "expected a record or a map, but got: {v:?}",
            ))),
        }
    }

    fn deserialize_struct<V>(
        self,
        _struct_name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let (value, schema) = unwrap_union(self.input, self.schema);

        match value {
            Value::Record(ref fields) => {
                visitor.visit_map(RecordDeserializer::new(fields, schema)?)
            }
            v => Err(de::Error::custom(format!("expected a Record, got: {v:?}",))),
        }
    }

    fn deserialize_enum<V>(
        self,
        _enum_name: &'static str,
        _variants: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(de::Error::custom("unexpected enum"))
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_str(visitor)
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_any(visitor)
    }

    fn is_human_readable(&self) -> bool {
        false
    }
}

impl<'de> de::SeqAccess<'de> for SeqDeserializer<'de> {
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        match self.input.next() {
            Some(item) => seed
                .deserialize(&Deserializer::new(item, self.item_schema))
                .map(Some),
            None => Ok(None),
        }
    }
}

impl<'de> de::MapAccess<'de> for MapDeserializer<'de> {
    type Error = Error;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: DeserializeSeed<'de>,
    {
        match self.input_keys.next() {
            Some(key) => seed.deserialize(StrDeserializer { input: key }).map(Some),
            None => Ok(None),
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        match self.input_values.next() {
            Some(value) => seed.deserialize(&Deserializer::new(value, self.value_schema)),
            None => Err(de::Error::custom("should not happen - too many values")),
        }
    }
}

impl<'de> de::MapAccess<'de> for RecordDeserializer<'de> {
    type Error = Error;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: DeserializeSeed<'de>,
    {
        match self.input.next() {
            Some(item) => {
                let (field, value) = item;
                self.value = Some(value);
                seed.deserialize(StrDeserializer { input: field }).map(Some)
            }
            None => Ok(None),
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        match self.value.take() {
            Some(value) => {
                let result = seed.deserialize(&Deserializer::new(
                    value,
                    &self.record_schema.fields[self.next_field_index].schema,
                ));
                self.next_field_index += 1;
                result
            }
            None => Err(de::Error::custom("should not happen - too many values")),
        }
    }
}

#[derive(Clone)]
struct StrDeserializer<'de> {
    input: &'de str,
}

impl<'de> de::Deserializer<'de> for StrDeserializer<'de> {
    type Error = Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_str(self.input)
    }

    forward_to_deserialize_any! {
        bool u8 u16 u32 u64 i8 i16 i32 i64 f32 f64 char str string unit option
        seq bytes byte_buf map unit_struct newtype_struct
        tuple_struct struct tuple enum identifier ignored_any
    }
}

// This one-liner function is needed to force the erased deserializer implementation
// for the Avro deserializer to get monomorphized in the adapters crate rather than
// every crate that uses it.
#[inline(never)]
pub fn avro_deserializer<'a, 'de>(
    deserializer: &'a Deserializer<'de>,
) -> Box<dyn ErasedDeserializer<'de> + 'a> {
    Box::new(<dyn ErasedDeserializer>::erase(deserializer))
}

/// Interpret a `Value` as an instance of type `D`.
///
/// This conversion can fail if the structure of the `Value` does not match the
/// structure expected by `D`.
pub fn from_avro_value<'de, D: DeserializeWithContext<'de, SqlSerdeConfig>>(
    value: &'de Value,
    schema: &'de Schema,
) -> Result<D, erased_serde::Error> {
    let deserializer: Deserializer<'de> = Deserializer::new(value, schema);

    let deserializer = avro_deserializer(&deserializer);

    D::deserialize_with_context(deserializer, avro_de_config())
}
