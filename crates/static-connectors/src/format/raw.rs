//! The raw format's record layer: a message read as a record whose one column holds the
//! message's bytes.

use core::str;
use feldera_types::serde_with_context::{SqlSerdeConfig, serde_config::BinaryFormat};
use serde::{
    Deserializer,
    de::{Error as _, MapAccess, value::StrDeserializer},
    forward_to_deserialize_any,
};
use std::fmt::Display;

/// The serde configuration that reads a raw message into its column.
pub fn raw_serde_config() -> SqlSerdeConfig {
    SqlSerdeConfig::default().with_binary_format(BinaryFormat::Bytes)
}

/// Deserializer implementation that deserializes a byte slice as a struct with one column that contains
/// these bytes.
pub(crate) struct RawDeserializer<'de> {
    column_name: &'de str,
    bytes: &'de [u8],
}

impl<'de> RawDeserializer<'de> {
    pub(crate) fn new(column_name: &'de str, bytes: &'de [u8]) -> Self {
        Self { column_name, bytes }
    }
}

#[derive(Clone)]
struct RawMapDeserializer<'de> {
    column_name: &'de str,
    bytes: &'de [u8],
    done: bool,
}

impl<'de> RawMapDeserializer<'de> {
    fn new(column_name: &'de str, bytes: &'de [u8]) -> Self {
        Self {
            column_name,
            bytes,
            done: false,
        }
    }
}

#[derive(Debug)]
pub(crate) struct RawDeserializeError {
    message: String,
}

impl Display for RawDeserializeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.message.fmt(f)
    }
}

impl std::error::Error for RawDeserializeError {}

impl serde::de::Error for RawDeserializeError {
    fn custom<T>(msg: T) -> Self
    where
        T: Display,
    {
        Self {
            message: msg.to_string(),
        }
    }
}

impl<'de> MapAccess<'de> for RawMapDeserializer<'de> {
    type Error = RawDeserializeError;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: serde::de::DeserializeSeed<'de>,
    {
        if self.done {
            Ok(None)
        } else {
            self.done = true;
            seed.deserialize(StrDeserializer::new(self.column_name))
                .map(Some)
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::DeserializeSeed<'de>,
    {
        seed.deserialize(self.clone())
    }
}

impl<'de> Deserializer<'de> for RawMapDeserializer<'de> {
    type Error = RawDeserializeError;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        visitor.visit_bytes(self.bytes)
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let s = str::from_utf8(self.bytes).map_err(|e| {
            RawDeserializeError::custom(format!("unable to convert buffer to UTF-8 string (consider changing column type to 'VARBINARY'): {e}"))
        })?;
        visitor.visit_str(s)
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        self.deserialize_str(visitor)
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        visitor.visit_bytes(self.bytes)
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        visitor.visit_some(self)
    }

    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char
        byte_buf unit unit_struct newtype_struct seq tuple
        tuple_struct map enum struct identifier ignored_any
    }
}

impl<'de> Deserializer<'de> for RawDeserializer<'de> {
    type Error = RawDeserializeError;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        visitor.visit_map(RawMapDeserializer::new(self.column_name, self.bytes))
    }
    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf option unit unit_struct newtype_struct seq tuple
        tuple_struct map enum struct identifier ignored_any
    }
}
