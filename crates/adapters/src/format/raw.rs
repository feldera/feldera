use crate::{
    catalog::{DeCollectionStream, RecordFormat},
    format::{InputFormat, ParseError, Parser},
    ControllerError,
};
use axum::http::Request;
use axum::body::Body;
use core::str;
use dbsp::operator::StagedBuffers;
use erased_serde::Serialize as ErasedSerialize;
use feldera_types::{
    format::raw::{RawParserConfig, RawParserMode},
    serde_with_context::{serde_config::BinaryFormat, SqlSerdeConfig},
};
use serde::{de::Error as _, de::SeqAccess, forward_to_deserialize_any, Deserialize, Deserializer};
use serde_json::json;
use serde_urlencoded::Deserializer as UrlDeserializer;
use std::{borrow::Cow, fmt::Display};

use crate::catalog::InputCollectionHandle;

use super::{InputBuffer, LineSplitter, Sponge};

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

    fn config_from_http_request(
        &self,
        endpoint_name: &str,
        request: &Request<Body>,
    ) -> Result<Box<dyn ErasedSerialize>, ControllerError> {
        Ok(Box::new(
            RawParserConfig::deserialize(UrlDeserializer::new(form_urlencoded::parse(
                request.uri().query().unwrap_or("").as_bytes(),
            )))
            .map_err(|e| {
                ControllerError::parser_config_parse_error(
                    endpoint_name,
                    &e,
                    request.uri().query().unwrap_or(""),
                )
            })?,
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

        if num_columns != 1 {
            return Err(ControllerError::invalid_parser_configuration(
                endpoint_name,
                &format!("'raw' input format can only be used with tables that have a single column of type 'VARCHAR' or 'VARBINARY', but table '{}' has {num_columns} columns", input_stream.schema.name),
            ));
        }

        let typ = input_stream.schema.fields[0].columntype.typ;

        if !typ.is_varchar() && !typ.is_varbinary() {
            return Err(ControllerError::invalid_parser_configuration(
                endpoint_name,
                &format!("'raw' input format can only be used with tables that have a single column of type 'VARCHAR' or 'VARBINARY', but table '{}' has a column of type {typ}", input_stream.schema.name),
            ));
        }

        let input_stream = input_stream
            .handle
            .configure_deserializer(RecordFormat::Raw)?;
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

    fn parse_record(&mut self, record: &[u8], errors: &mut Vec<ParseError>) {
        if let Err(e) = self.input_stream.insert(record) {
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
            RawParserMode::Blob => Box::new(Sponge),
        }
    }

    fn parse(&mut self, data: &[u8]) -> (Option<Box<dyn InputBuffer>>, Vec<ParseError>) {
        let mut errors = Vec::new();

        match self.config.mode {
            RawParserMode::Blob => {
                self.parse_record(data, &mut errors);
                self.last_event_number += 1;
            }
            RawParserMode::Lines => {
                for line in data.split(|b| b == &b'\n') {
                    if !line.is_empty() {
                        self.parse_record(line, &mut errors);
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
    bytes: &'de [u8],
}

impl<'de> RawDeserializer<'de> {
    pub(crate) fn new(bytes: &'de [u8]) -> Self {
        Self { bytes }
    }
}

#[derive(Clone)]
struct RawSeqDeserializer<'de> {
    bytes: &'de [u8],
    done: bool,
}

impl<'de> RawSeqDeserializer<'de> {
    fn new(bytes: &'de [u8]) -> Self {
        Self { bytes, done: false }
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

impl<'de> SeqAccess<'de> for RawSeqDeserializer<'de> {
    type Error = RawDeserializeError;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: serde::de::DeserializeSeed<'de>,
    {
        if self.done {
            Ok(None)
        } else {
            self.done = true;
            Ok(Some(seed.deserialize(self.clone())?))
        }
    }
}

impl<'de> Deserializer<'de> for RawSeqDeserializer<'de> {
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
        visitor.visit_seq(RawSeqDeserializer::new(self.bytes))
    }
    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf option unit unit_struct newtype_struct seq tuple
        tuple_struct map enum struct identifier ignored_any
    }
}

#[cfg(test)]
mod test {
    use crate::test::{mock_parser_pipeline, MockUpdate};
    use crate::FormatConfig;
    use feldera_adapterlib::{
        format::{InputBuffer, ParseError, Parser},
        transport::InputConsumer,
    };
    use feldera_sqllib::{ByteArray, SqlString};
    use feldera_types::{
        deserialize_table_record,
        format::raw::{RawParserConfig, RawParserMode},
        program_schema::{ColumnType, Field, Relation, SqlIdentifier},
        serde_with_context::{DeserializeWithContext, SqlSerdeConfig},
    };
    use std::{borrow::Cow, collections::BTreeMap, fmt::Debug, hash::Hash};

    #[derive(Eq, PartialEq, Debug, Hash, Clone)]
    struct Binary {
        data: ByteArray,
    }

    fn binary_schema() -> Relation {
        Relation::new(
            SqlIdentifier::new("binary", false),
            vec![Field::new(
                SqlIdentifier::new("data", false),
                ColumnType::varbinary(false),
            )],
            false,
            BTreeMap::new(),
        )
    }

    deserialize_table_record!(Binary["Binary", 1] {
        (data, "data", false, ByteArray, None)
    });

    #[derive(Eq, PartialEq, Debug, Hash, Clone)]
    struct OptBinary {
        data: Option<ByteArray>,
    }

    fn opt_binary_schema() -> Relation {
        Relation::new(
            SqlIdentifier::new("opt_binary", false),
            vec![Field::new(
                SqlIdentifier::new("data", false),
                ColumnType::varbinary(true),
            )],
            false,
            BTreeMap::new(),
        )
    }

    deserialize_table_record!(OptBinary["OptBinary", 1] {
        (data, "data", false, Option<ByteArray>, Some(None))
    });

    #[derive(Eq, PartialEq, Debug, Hash, Clone)]
    struct Varchar {
        data: SqlString,
    }

    deserialize_table_record!(Varchar["Varchar", 1] {
        (data, "data", false, SqlString, None)
    });

    fn varchar_schema() -> Relation {
        Relation::new(
            SqlIdentifier::new("string_table", false),
            vec![Field::new(
                SqlIdentifier::new("data", false),
                ColumnType::varchar(false),
            )],
            false,
            BTreeMap::new(),
        )
    }

    #[derive(Eq, PartialEq, Debug, Hash, Clone)]
    struct OptVarchar {
        data: Option<SqlString>,
    }

    deserialize_table_record!(OptVarchar["OptVarchar", 1] {
        (data, "data", false, Option<SqlString>, Some(None))
    });

    fn opt_varchar_schema() -> Relation {
        Relation::new(
            SqlIdentifier::new("opt_string_table", false),
            vec![Field::new(
                SqlIdentifier::new("data", false),
                ColumnType::varchar(true),
            )],
            false,
            BTreeMap::new(),
        )
    }

    #[derive(Debug)]
    struct TestCase<T> {
        config: RawParserConfig,
        /// Input data, expected result.
        input_batches: Vec<(Vec<u8>, Vec<ParseError>)>,
        /// Expected contents at the end of the test.
        expected_output: Vec<MockUpdate<T, ()>>,
        schema: Relation,
    }

    impl<T> TestCase<T> {
        #[track_caller]
        fn new(
            config: RawParserConfig,
            input_batches: Vec<(Vec<u8>, Vec<ParseError>)>,
            expected_output: Vec<MockUpdate<T, ()>>,
            schema: Relation,
        ) -> Self {
            Self {
                config,
                input_batches,
                expected_output,
                schema,
            }
        }
    }

    fn run_test_cases<T>(test_cases: Vec<TestCase<T>>)
    where
        T: Debug
            + Eq
            + Hash
            + for<'de> DeserializeWithContext<'de, SqlSerdeConfig>
            + Send
            + Sync
            + Debug
            + Clone
            + 'static,
    {
        for test in test_cases {
            println!("test: {test:?}");
            let format_config = FormatConfig {
                name: Cow::from("raw"),
                config: serde_json::to_value(test.config).unwrap(),
            };

            let (consumer, mut parser, outputs) =
                mock_parser_pipeline(&test.schema, &format_config).unwrap();
            consumer.on_error(Some(Box::new(|_, _| {})));
            parser.on_error(Some(Box::new(|_, _| {})));
            for (data, expected_errors) in test.input_batches {
                let (mut buffer, errors) = parser.parse(&data);
                assert_eq!(&errors, &expected_errors);
                buffer.flush();
            }
            consumer.eoi();
            assert_eq!(&test.expected_output, &outputs.state().flushed);
        }
    }

    #[test]
    fn test_raw_varchar() {
        let test1 = TestCase::new(
            RawParserConfig {
                mode: RawParserMode::Lines,
            },
            vec![(b"foo\nbar".to_vec(), vec![])],
            vec![
                MockUpdate::with_polarity(Varchar { data: "foo".into() }, true),
                MockUpdate::with_polarity(Varchar { data: "bar".into() }, true),
            ],
            varchar_schema(),
        );

        let test_cases = vec![test1];
        run_test_cases(test_cases);
    }

    #[test]
    fn test_raw_opt_varchar() {
        let test1 = TestCase::new(
            RawParserConfig {
                mode: RawParserMode::Blob,
            },
            vec![(b"foo\nbar".to_vec(), vec![])],
            vec![MockUpdate::with_polarity(
                OptVarchar {
                    data: Some("foo\nbar".into()),
                },
                true,
            )],
            opt_varchar_schema(),
        );

        let test_cases = vec![test1];
        run_test_cases(test_cases);
    }

    #[test]
    fn test_raw_varbinary() {
        let test1 = TestCase::new(
            RawParserConfig {
                mode: RawParserMode::Lines,
            },
            vec![(b"foo\nbar".to_vec(), vec![])],
            vec![
                MockUpdate::with_polarity(
                    Binary {
                        data: b"foo".as_slice().into(),
                    },
                    true,
                ),
                MockUpdate::with_polarity(
                    Binary {
                        data: b"bar".as_slice().into(),
                    },
                    true,
                ),
            ],
            binary_schema(),
        );

        let test_cases = vec![test1];
        run_test_cases(test_cases);
    }

    #[test]
    fn test_raw_opt_varbinary() {
        let test1 = TestCase::new(
            RawParserConfig {
                mode: RawParserMode::Blob,
            },
            vec![(b"foo\nbar".to_vec(), vec![])],
            vec![MockUpdate::with_polarity(
                OptBinary {
                    data: Some(b"foo\nbar".as_slice().into()),
                },
                true,
            )],
            opt_binary_schema(),
        );

        let test_cases = vec![test1];
        run_test_cases(test_cases);
    }
}
