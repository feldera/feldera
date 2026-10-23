use crate::{
    ControllerError,
    catalog::{DeCollectionStream, RecordFormat},
    format::{InputFormat, ParseError, Parser},
};
use core::str;
use dbsp::operator::StagedBuffers;
use erased_serde::Serialize as ErasedSerialize;
use feldera_adapterlib::ConnectorMetadata;
use feldera_types::{
    format::raw::{RawParserConfig, RawParserMode},
    serde_with_context::{SqlSerdeConfig, serde_config::BinaryFormat},
};
use serde::{
    Deserialize, Deserializer,
    de::{Error as _, MapAccess, value::StrDeserializer},
    forward_to_deserialize_any,
};
use serde_json::json;
use serde_urlencoded::Deserializer as UrlDeserializer;
use std::{borrow::Cow, fmt::Display};

use crate::catalog::InputCollectionHandle;

use super::{InputBuffer, LineSplitter, SpongeSplitter};

/// Raw format parser.
///
/// Ingest raw byte stream into a table with a single column of type VARCHAR or VARBINARY.
pub struct RawInputFormat;

pub fn raw_serde_config() -> SqlSerdeConfig {
    SqlSerdeConfig::default().with_binary_format(BinaryFormat::Bytes)
}

impl InputFormat for RawInputFormat {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("raw")
    }

    fn config_from_http_query(
        &self,
        endpoint_name: &str,
        query: &str,
    ) -> Result<Box<dyn ErasedSerialize>, ControllerError> {
        Ok(Box::new(
            RawParserConfig::deserialize(UrlDeserializer::new(form_urlencoded::parse(
                query.as_bytes(),
            )))
            .map_err(|e| ControllerError::parser_config_parse_error(endpoint_name, &e, query))?,
        ))
    }

    fn new_parser(
        &self,
        endpoint_name: &str,
        input_stream: &InputCollectionHandle,
        config: &serde_json::Value,
    ) -> Result<Box<dyn Parser>, ControllerError> {
        let config = if config.is_null() { &json!({}) } else { config };
        let config = RawParserConfig::deserialize(config).map_err(|e| {
            ControllerError::parser_config_parse_error(endpoint_name, &e, &config.to_string())
        })?;

        let num_columns = input_stream.schema.fields.len();

        let field = if let Some(column_name) = &config.column_name {
            input_stream
                .schema
                .field(column_name)
                .ok_or_else(|| {
                    ControllerError::invalid_parser_configuration(
                        endpoint_name,
                        &format!(
                            "column '{}' not found in table '{}'",
                            column_name, input_stream.schema.name
                        ),
                    )
                })?
                .clone()
        } else {
            if num_columns != 1 {
                return Err(ControllerError::invalid_parser_configuration(
                    endpoint_name,
                    &format!(
                        "The 'raw' input format is used with table '{}', which has {num_columns} columns. Use the 'column_name' configuration option to specify which column should store the raw input data.",
                        input_stream.schema.name
                    ),
                ));
            }

            input_stream.schema.fields[0].clone()
        };

        let typ = field.columntype.typ;

        if !typ.is_varchar() && !typ.is_varbinary() {
            return Err(ControllerError::invalid_parser_configuration(
                endpoint_name,
                &format!(
                    "'raw' input format can only be used with a column of type 'VARCHAR' or 'VARBINARY', but column {} has type {typ}",
                    field.name
                ),
            ));
        }

        let input_stream = input_stream
            .handle
            .configure_deserializer(RecordFormat::Raw(field.name.to_string()))?;
        Ok(Box::new(RawParser::new(input_stream, config)) as Box<dyn Parser>)
    }
}

struct RawParser {
    /// Input handle to push parsed data to.
    input_stream: Box<dyn DeCollectionStream>,

    config: RawParserConfig,

    last_event_number: u64,
}

impl RawParser {
    fn new(input_stream: Box<dyn DeCollectionStream>, config: RawParserConfig) -> Self {
        Self {
            input_stream,
            last_event_number: 0,
            config,
        }
    }

    fn parse_record(
        &mut self,
        record: &[u8],
        metadata: &Option<ConnectorMetadata>,
        errors: &mut Vec<ParseError>,
    ) {
        if let Err(e) = self.input_stream.insert(record, metadata) {
            errors.push(ParseError::bin_event_error(
                format!("failed to deserialize raw record: {e}"),
                self.last_event_number + 1,
                record,
                None,
            ));
        }
    }
}

impl Parser for RawParser {
    fn fork(&self) -> Box<dyn Parser> {
        Box::new(Self::new(self.input_stream.fork(), self.config.clone()))
    }

    fn splitter(&self) -> Box<dyn super::Splitter> {
        match self.config.mode {
            RawParserMode::Lines => Box::new(LineSplitter),
            RawParserMode::Blob => Box::new(SpongeSplitter),
        }
    }

    fn parse(
        &mut self,
        data: &[u8],
        metadata: Option<ConnectorMetadata>,
    ) -> (Option<Box<dyn InputBuffer>>, Vec<ParseError>) {
        let mut errors = Vec::new();

        match self.config.mode {
            RawParserMode::Blob => {
                self.parse_record(data, &metadata, &mut errors);
                self.last_event_number += 1;
            }
            RawParserMode::Lines => {
                for line in data.split(|b| b == &b'\n') {
                    if !line.is_empty() {
                        self.parse_record(line, &metadata, &mut errors);
                        self.last_event_number += 1;
                    }
                }
            }
        }

        (self.input_stream.take_all(), errors)
    }

    fn stage(&self, buffers: Vec<Box<dyn InputBuffer>>) -> Box<dyn StagedBuffers> {
        self.input_stream.stage(buffers)
    }
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

#[cfg(any(test, feature = "testing"))]
pub mod tests;
