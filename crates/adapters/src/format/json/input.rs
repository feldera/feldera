//! JSON format parser.

use super::{DebeziumUpdate, InsDelUpdate};
use crate::catalog::InputCollectionHandle;
use crate::format::{InputBuffer, LineSplitter};
use crate::{
    ControllerError,
    catalog::{DeCollectionStream, RecordFormat},
    format::{InputFormat, ParseError, Parser},
};
use dbsp::operator::StagedBuffers;
use erased_serde::Serialize as ErasedSerialize;
use feldera_adapterlib::ConnectorMetadata;
use feldera_adapterlib::format::Splitter;
use feldera_types::format::json::{JsonLines, JsonParserConfig, JsonUpdateFormat};
use serde::Deserialize;
use serde_json::value::RawValue;
use serde_json::{Value as JsonValue, json};
use serde_urlencoded::Deserializer as UrlDeserializer;
use std::borrow::Cow;

/// JSON format parser.
pub struct JsonInputFormat;

trait UpdateFormat {
    fn error() -> &'static str;
    fn array_error() -> &'static str;
    fn example() -> Option<&'static str>;
    fn array_example() -> Option<&'static str>;
    fn apply(
        self,
        parser: &mut JsonParser,
        metadata: &Option<ConnectorMetadata>,
    ) -> Result<usize, ParseError>;
}

impl UpdateFormat for InsDelUpdate<&'_ RawValue> {
    fn error() -> &'static str {
        "error deserializing JSON string as a single-row update"
    }

    fn array_error() -> &'static str {
        "error deserializing string as a JSON array of updates"
    }

    fn example() -> Option<&'static str> {
        Some("Example valid JSON: '{{\"insert\": {{...}} }}'")
    }

    fn array_example() -> Option<&'static str> {
        Some("Example valid JSON: '[{{\"insert\": {{...}} }}, {{\"delete\": {{...}} }}]'")
    }

    fn apply(
        self,
        parser: &mut JsonParser,
        metadata: &Option<ConnectorMetadata>,
    ) -> Result<usize, ParseError> {
        let mut updates = 0;

        if let Some(val) = self.delete {
            parser.delete(val, metadata)?;
            updates += 1;
        }

        if let Some(val) = self.insert {
            parser.insert(val, metadata)?;
            updates += 1;
        }

        if let Some(val) = self.update {
            parser.update(val, metadata)?;
            updates += 1;
        }

        Ok(updates)
    }
}

impl UpdateFormat for DebeziumUpdate<&'_ RawValue> {
    fn error() -> &'static str {
        "error deserializing JSON string as a Debezium CDC event"
    }

    fn array_error() -> &'static str {
        "error deserializing string as a JSON array of Debezium CDC events"
    }

    fn example() -> Option<&'static str> {
        Some(
            "Example valid JSON: '{{\"payload\": {{\"op\": \"u\", \"before\": {{...}}, \"after\": {{...}} }} }}'",
        )
    }

    fn array_example() -> Option<&'static str> {
        Some(
            "Example valid JSON: '[{{\"payload\": {{\"op\": \"u\", \"before\": {{...}}, \"after\": {{...}} }} }}]'",
        )
    }

    fn apply(
        self,
        parser: &mut JsonParser,
        metadata: &Option<ConnectorMetadata>,
    ) -> Result<usize, ParseError> {
        // TODO: validate table name.
        // We currently allow a JSON connector to feed data to a single table.
        // The name of the table may or may not match table name in the CDC
        // stream.  In the future we will allow demultiplexing a JSON stream
        // to multiple tables.  Connector config will specify available tables
        // and mapping between CDC and DBSP table names.
        /*if let Some(table) = &self.paylolad.table {
            check that table name matches??
        };*/

        // TODO: validate CDC op code (c|d|u).  This opcode seems redundant.
        // We must always delete the `before` record and insert the `after`
        // record (if present).
        /*
        match update.payload.op {
            CdcOp::Create =>,
            CdcOp::Delete =>,
            CdcOp::Update =>
        }*/

        let mut updates = 0;

        if let Some(before) = &self.payload.before {
            parser.delete(before, metadata)?;
            updates += 1;
        };

        if let Some(after) = &self.payload.after {
            parser.insert(after, metadata)?;
            updates += 1;
        };

        Ok(updates)
    }
}

impl UpdateFormat for &'_ RawValue {
    fn error() -> &'static str {
        "failed to parse JSON string"
    }

    fn array_error() -> &'static str {
        "error deserializing string as a JSON array"
    }

    fn example() -> Option<&'static str> {
        None
    }

    fn array_example() -> Option<&'static str> {
        None
    }

    fn apply(
        self,
        parser: &mut JsonParser,
        metadata: &Option<ConnectorMetadata>,
    ) -> Result<usize, ParseError> {
        parser.insert(self, metadata)?;
        Ok(1)
    }
}

impl InputFormat for JsonInputFormat {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("json")
    }

    fn new_parser(
        &self,
        endpoint_name: &str,
        input_handle: &InputCollectionHandle,
        config: &serde_json::Value,
    ) -> Result<Box<dyn Parser>, ControllerError> {
        let config = if config.is_null() { &json!({}) } else { config };
        let config = JsonParserConfig::deserialize(config).map_err(|e| {
            ControllerError::parser_config_parse_error(endpoint_name, &e, &config.to_string())
        })?;
        validate_parser_config(&config, endpoint_name)?;
        let input_stream = input_handle
            .handle
            .configure_deserializer(RecordFormat::Json(config.json_flavor.clone()))?;
        Ok(Box::new(JsonParser::new(input_stream, config)) as Box<dyn Parser>)
    }

    fn config_from_http_query(
        &self,
        endpoint_name: &str,
        query: &str,
    ) -> Result<Box<dyn ErasedSerialize>, ControllerError> {
        Ok(Box::new(
            JsonParserConfig::deserialize(UrlDeserializer::new(form_urlencoded::parse(
                query.as_bytes(),
            )))
            .map_err(|e| ControllerError::parser_config_parse_error(endpoint_name, &e, query))?,
        ))
    }
}

fn validate_parser_config(
    config: &JsonParserConfig,
    endpoint_name: &str,
) -> Result<(), ControllerError> {
    // No wildcard: a new update format must be classified here, because the
    // parser panics on a format it does not support.
    match config.update_format {
        JsonUpdateFormat::InsertDelete | JsonUpdateFormat::Debezium | JsonUpdateFormat::Raw => {
            Ok(())
        }
        JsonUpdateFormat::Snowflake | JsonUpdateFormat::Redis => {
            Err(ControllerError::input_format_not_supported(
                endpoint_name,
                &unsupported_update_format(&config.update_format),
            ))
        }
    }
}

/// Error message for an update format that JSON input does not support.
///
/// The list of formats here must match the ones that `validate_parser_config`
/// accepts.
fn unsupported_update_format(update_format: &JsonUpdateFormat) -> String {
    format!(
        "update format '{update_format}' is not supported for JSON input streams; use one of \
         'insert_delete', 'debezium', or 'raw'"
    )
}

fn looks_like_insert_delete_envelope_object(value: &JsonValue) -> bool {
    let Some(object) = value.as_object() else {
        return false;
    };

    let mut has_action_key = false;
    for (key, value) in object {
        match key.as_str() {
            "table" => (),
            "insert" | "delete" | "update" => {
                if has_action_key || (!value.is_object() && !value.is_array()) {
                    return false;
                }
                has_action_key = true;
            }
            _ => return false,
        }
    }
    has_action_key
}

fn looks_like_insert_delete_envelope(update: &RawValue) -> bool {
    let Ok(value) = serde_json::from_str::<JsonValue>(update.get()) else {
        return false;
    };

    looks_like_insert_delete_envelope_object(&value)
        || value.as_array().is_some_and(|array| {
            !array.is_empty() && array.iter().all(looks_like_insert_delete_envelope_object)
        })
}

struct JsonParser {
    /// Input handle to push parsed data to.
    input_stream: Box<dyn DeCollectionStream>,
    config: JsonParserConfig,
    last_event_number: u64,
}

impl JsonParser {
    fn raw_format_insert_delete_mismatch_error(update: &RawValue) -> Option<ParseError> {
        looks_like_insert_delete_envelope(update).then(|| {
            ParseError::text_envelope_error(
                "payload looks like an insert/delete envelope; the configured `raw` update format expects plain rows".to_string(),
                update.get(),
                Some(Cow::from(
                    "Set `format.config.update_format` to `insert_delete` for payloads like {\"insert\": {...}} or {\"delete\": {...}}.",
                )),
            )
        })
    }

    fn new(input_stream: Box<dyn DeCollectionStream>, config: JsonParserConfig) -> Self {
        Self {
            input_stream,
            config,
            last_event_number: 0,
        }
    }

    fn delete(
        &mut self,
        val: &RawValue,
        metadata: &Option<ConnectorMetadata>,
    ) -> Result<(), ParseError> {
        self.input_stream
            .delete(val.get().as_bytes(), metadata)
            .map_err(|e| {
                ParseError::text_event_error(
                    "failed to deserialize JSON record",
                    e,
                    self.last_event_number + 1,
                    Some(val.get()),
                    None,
                )
            })
    }

    fn insert(
        &mut self,
        val: &RawValue,
        metadata: &Option<ConnectorMetadata>,
    ) -> Result<(), ParseError> {
        self.input_stream
            .insert(val.get().as_bytes(), metadata)
            .map_err(|e| {
                ParseError::text_event_error(
                    "failed to deserialize JSON record",
                    e,
                    self.last_event_number + 1,
                    Some(val.get()),
                    None,
                )
            })
    }

    fn update(
        &mut self,
        val: &RawValue,
        metadata: &Option<ConnectorMetadata>,
    ) -> Result<(), ParseError> {
        self.input_stream
            .update(val.get().as_bytes(), metadata)
            .map_err(|e| {
                ParseError::text_event_error(
                    "failed to deserialize JSON record",
                    e,
                    self.last_event_number + 1,
                    Some(val.get()),
                    None,
                )
            })
    }

    fn apply_update<'de, F>(
        &mut self,
        update: &'de RawValue,
        metadata: &Option<ConnectorMetadata>,
        errors: &mut Vec<ParseError>,
    ) where
        F: UpdateFormat + Deserialize<'de>,
    {
        if self.config.array {
            match serde_json::from_str::<Vec<F>>(update.get()) {
                Err(e) => {
                    errors.push(ParseError::text_envelope_error(
                        format!("{}: {e}", F::array_error()),
                        update.get(),
                        F::array_example().map(Cow::from),
                    ));
                }
                Ok(updates) => {
                    let mut error = false;
                    let old_n_records = self.input_stream.len().records;
                    for update in updates {
                        if let Err(e) = update.apply(self, metadata) {
                            error = true;
                            errors.push(e);
                        }
                        self.last_event_number += 1;
                    }
                    if error {
                        self.input_stream.truncate(old_n_records);
                    }
                }
            };
        } else {
            match serde_json::from_str::<F>(update.get()) {
                Err(e) => {
                    errors.push(ParseError::text_event_error(
                        F::error(),
                        e,
                        self.last_event_number + 1,
                        Some(update.get()),
                        F::example().map(Cow::from),
                    ));
                }
                Ok(update) => {
                    if let Err(e) = update.apply(self, metadata) {
                        errors.push(e);
                    }
                }
            }
            self.last_event_number += 1;
        }
    }
}

impl Parser for JsonParser {
    fn parse(
        &mut self,
        data: &[u8],
        metadata: Option<ConnectorMetadata>,
    ) -> (Option<Box<dyn InputBuffer>>, Vec<ParseError>) {
        let mut errors = Vec::new();

        let mut stream = serde_json::Deserializer::from_slice(data).into_iter::<&RawValue>();

        while let Some(update) = stream.next() {
            let update = match update {
                Err(e) => {
                    let json_str = String::from_utf8_lossy(&data[stream.byte_offset()..]);
                    errors.push(ParseError::text_envelope_error(
                        format!("failed to parse string as a JSON document: {e}"),
                        &json_str,
                        None,
                    ));
                    break;
                }
                Ok(update) => update,
            };

            match self.config.update_format {
                JsonUpdateFormat::InsertDelete => {
                    self.apply_update::<InsDelUpdate<_>>(update, &metadata, &mut errors)
                }
                JsonUpdateFormat::Debezium => {
                    self.apply_update::<DebeziumUpdate<_>>(update, &metadata, &mut errors)
                }
                JsonUpdateFormat::Raw => {
                    let errors_before = errors.len();
                    self.apply_update::<&RawValue>(update, &metadata, &mut errors);
                    // On parse failure, check if the user sent insert/delete
                    // envelopes with the raw format and provide a better hint.
                    if errors.len() > errors_before
                        && let Some(error) = Self::raw_format_insert_delete_mismatch_error(update)
                    {
                        errors.truncate(errors_before);
                        errors.push(error);
                    }
                }
                JsonUpdateFormat::Redis | JsonUpdateFormat::Snowflake => {
                    // `validate_parser_config` rejects these formats, so this
                    // is unreachable.  Report an error instead of panicking:
                    // a panic here would happen while a transport holds locks.
                    errors.push(ParseError::text_envelope_error(
                        unsupported_update_format(&self.config.update_format),
                        update.get(),
                        None,
                    ));
                    break;
                }
            }
        }

        (self.input_stream.take_all(), errors)
    }

    fn stage(&self, buffers: Vec<Box<dyn InputBuffer>>) -> Box<dyn StagedBuffers> {
        self.input_stream.stage(buffers)
    }

    fn fork(&self) -> Box<dyn Parser> {
        Box::new(Self::new(self.input_stream.fork(), self.config.clone()))
    }

    fn splitter(&self) -> Box<dyn Splitter> {
        match self.config.lines {
            JsonLines::Single => Box::new(LineSplitter),
            JsonLines::Multiple => Box::new(JsonSplitter::default()),
        }
    }
}

/// Splits between JSON top-level objects and arrays.
///
/// This does the minimal amount of parsing necessary to find breakpoints in RFC
/// 8259-compliant JSON.
#[derive(Copy, Clone, Debug, Default, PartialEq)]
enum JsonSplitter {
    /// Only whitespace has been seen so far.
    #[default]
    Start,

    /// Parsing a value, nested inside `.0` levels of braces and brackets.
    Value(usize),

    /// Inside a quoted string, nested inside `.0` levels of braces and
    /// brackets.
    Quote(usize),

    /// Inside a quoted string and the previous character was a backslash,
    /// nested inside `.0` levels of braces and brackets.
    Backslash(usize),
}

/// Character category.
enum Cc {
    /// ASCII white space.
    Space,

    /// `[` or `{`.
    Left,

    /// `}` or ]`.
    Right,

    /// `"`.
    Quote,

    /// `\`.
    Backslash,

    /// Any other character.
    Other,
}

impl From<u8> for Cc {
    fn from(value: u8) -> Self {
        match value {
            b'\t' | b'\n' | b'\x0C' | b'\r' | b' ' => Self::Space,
            b'[' | b'{' => Self::Left,
            b']' | b'}' => Self::Right,
            b'"' => Self::Quote,
            b'\\' => Self::Backslash,
            _ => Self::Other,
        }
    }
}

impl Splitter for JsonSplitter {
    fn input(&mut self, data: &[u8]) -> Option<usize> {
        let mut iter = data.iter().copied().enumerate();

        // Splitter reached end of fragment when parsing a string--finish parsing the string.
        if matches!(self, Self::Quote(_) | Self::Backslash(_)) {
            *self = self.parse_string(&mut iter);
        }

        while let Some((index, c)) = iter.next() {
            *self = match (*self, Cc::from(c)) {
                (Self::Start, Cc::Space) => *self,
                (Self::Start, Cc::Left) => Self::Value(1),
                (Self::Start, Cc::Quote) => Self::Quote(0).parse_string(&mut iter),
                (Self::Start, _) => Self::Value(0),
                (Self::Value(0), Cc::Other) => *self,
                (Self::Value(0), _) => {
                    *self = Self::Start;
                    return Some(index);
                }
                (Self::Value(1), Cc::Right) => {
                    *self = Self::Start;
                    return Some(index + 1);
                }
                (Self::Value(nest), Cc::Right) => Self::Value(nest - 1),
                (Self::Value(nest), Cc::Left) => Self::Value(nest + 1),
                (Self::Value(nest), Cc::Quote) => Self::Quote(nest).parse_string(&mut iter),
                (Self::Value(_), _) => *self,
                _ => panic!("unexpected JsonSplitter state"),
            };
        }
        None
    }

    fn clear(&mut self) {
        *self = Self::default();
    }
}

impl JsonSplitter {
    /// Parse quoted string.
    ///
    /// This is an optimization: we can iterate ove the string faster by only considering
    /// relevant states.
    fn parse_string(mut self, iter: &mut impl Iterator<Item = (usize, u8)>) -> Self {
        for (_, c) in iter.by_ref() {
            match (self, c) {
                (Self::Quote(nest), b'"') => {
                    return Self::Value(nest);
                }
                (Self::Quote(nest), b'\\') => self = Self::Backslash(nest),
                (Self::Backslash(nest), _) => self = Self::Quote(nest),
                _ => {}
            }
        }
        self
    }
}

#[cfg(test)]
mod test {
    use crate::{
        FormatConfig, ParseError,
        format::{InputBuffer, Parser},
        test::{MockUpdate, init_test_logger, mock_parser_pipeline},
        transport::InputConsumer,
    };
    use feldera_adapterlib::format::Splitter;
    use feldera_sqllib::Variant;
    use feldera_types::{
        deserialize_table_record,
        format::json::{JsonFlavor, JsonLines, JsonParserConfig, JsonUpdateFormat},
        program_schema::Relation,
        serde_with_context::{DeserializeWithContext, SqlSerdeConfig},
    };
    use std::{borrow::Cow, fmt::Debug, hash::Hash, panic::Location};
    use tracing::trace;

    use super::JsonSplitter;

    #[derive(PartialEq, Debug, Eq, Hash, Clone)]
    struct TestStruct {
        b: bool,
        i: i32,
        s: Option<String>,
    }

    deserialize_table_record!(TestStruct["TestStruct", Variant, 3] {
        (b, "b", false, bool, |_| None),
        (i, "i", false, i32, |_| None),
        (s, "s", false, Option<String>, |_| Some(None))
    });

    // TODO: tests for RecordFormat::Raw.

    // Used to test RecordFormat::Raw.
    /*#[derive(Deserialize, PartialEq, Debug, Eq)]
    struct OneColumn {
        json: JsonValue,
    }*/

    #[derive(PartialEq, Debug, Eq, Hash, Clone)]
    struct TestStructUpd {
        b: Option<bool>,
        i: i32,
        s: Option<Option<String>>,
    }

    deserialize_table_record!(TestStructUpd["TestStructUpd", Variant, 3] {
        (b, "b", false, Option<bool>, |_| Some(None)),
        (i, "i", false, i32, |_| None),
        (s, "s", false, Option<Option<String>>, |_| Some(None), |x| if x.is_none() { Some(None) } else {x})
    });

    impl TestStructUpd {
        fn new(b: Option<bool>, i: i32, s: Option<Option<&str>>) -> Self {
            Self {
                b,
                i,
                s: s.map(|s| s.map(str::to_string)),
            }
        }
    }

    #[derive(Debug)]
    struct TestCase<T, U> {
        /// Putting the location in here makes it easier to find failing test
        /// cases because they get logged.
        #[allow(dead_code)]
        location: &'static Location<'static>,

        config: JsonParserConfig,
        /// Input data, expected result.
        input_batches: Vec<(String, Vec<ParseError>)>,
        /// Expected contents at the end of the test.
        expected_output: Vec<MockUpdate<T, U>>,
    }

    fn run_test_cases<T, U>(test_cases: Vec<TestCase<T, U>>)
    where
        T: Debug
            + Eq
            + for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>
            + Hash
            + Send
            + Sync
            + Debug
            + Clone
            + 'static,
        U: Debug
            + Eq
            + for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>
            + Hash
            + Send
            + Sync
            + Debug
            + Clone
            + 'static,
    {
        for test in test_cases {
            trace!("test: {test:?}");
            let format_config = FormatConfig {
                name: Cow::from("json"),
                config: serde_json::to_value(test.config).unwrap(),
            };

            let (consumer, mut parser, outputs) =
                mock_parser_pipeline(&Relation::empty(), &format_config).unwrap();
            consumer.on_error(Some(Box::new(|_, _| {})));
            parser.on_error(Some(Box::new(|_, _| {})));
            for (json, expected_errors) in test.input_batches {
                let (mut buffer, errors) = parser.parse(json.as_bytes(), None);
                assert_eq!(&errors, &expected_errors);
                buffer.flush().unwrap();
            }
            consumer.eoi();
            assert_eq!(&test.expected_output, &outputs.state().flushed);
        }
    }

    impl<T, U> TestCase<T, U> {
        #[track_caller]
        fn new(
            config: JsonParserConfig,
            input_batches: Vec<(String, Vec<ParseError>)>,
            expected_output: Vec<MockUpdate<T, U>>,
        ) -> Self {
            Self {
                location: Location::caller(),
                config,
                input_batches,
                expected_output,
            }
        }
    }

    /// An update record in the insert/delete format may skip non-key columns, and `null`
    /// sets a column to NULL. The connector suites cover the other records; updates need a
    /// keyed table, which the suites' fixture does not have yet.
    #[test]
    fn test_json_update_records() {
        init_test_logger();
        let test_cases: Vec<TestCase<TestStruct, TestStructUpd>> = vec![TestCase::new(
            JsonParserConfig {
                update_format: JsonUpdateFormat::InsertDelete,
                json_flavor: JsonFlavor::Default,
                array: false,
                lines: JsonLines::Single,
            },
            vec![
                (r#"{"update": {"b": true, "i": 0}}"#.to_string(), Vec::new()),
                (
                    r#"{"update": {"b": true, "i": 0, "s": "foo"}}"#.to_string(),
                    Vec::new(),
                ),
                (r#"{"update": {"i": 0, "s": null}}"#.to_string(), Vec::new()),
            ],
            vec![
                MockUpdate::Update(TestStructUpd::new(Some(true), 0, None)),
                MockUpdate::Update(TestStructUpd::new(Some(true), 0, Some(Some("foo")))),
                MockUpdate::Update(TestStructUpd::new(None, 0, Some(None))),
            ],
        )];

        run_test_cases(test_cases);
    }

    /// Update formats that JSON input does not support must be rejected when
    /// the parser is created.  Otherwise, the parser panics on the first
    /// record.
    #[test]
    fn unsupported_update_formats_are_rejected() {
        for update_format in [JsonUpdateFormat::Snowflake, JsonUpdateFormat::Redis] {
            let config = JsonParserConfig {
                update_format: update_format.clone(),
                ..JsonParserConfig::default()
            };
            let error = super::validate_parser_config(&config, "endpoint")
                .expect_err(&format!("{update_format:?} was accepted for input"));
            assert!(
                error.to_string().contains("not supported for JSON input"),
                "{error}"
            );
        }
    }

    #[test]
    fn test_json_splitter() {
        // Checks how we split different syntax. The `%` in each string is the
        // expected split point.
        for s in [
            // Correct syntax.
            "{ \r}%",
            "\n[\u{000c}]%",
            r#" {"hi there": "b"}%"#,
            r#"{"}]"}%"#,
            r#"["}]"]%"#,
            r#"{"\"": "\\"}%"#,
            r#"{"a": {"b":{"c": 123}}}%"#,
            r#"[{"a": {"b":{"c": }}},[{"d": 1234}]]%"#,
            "\r[1,\n2\n,3\n4\n4]%",
            " null% ",
            " false% ",
            "\ttrue% ",
            r#""xyz[zy"% "#,
            r#""xy\"z]zy"% "#,
            // Invalid syntax.
            "[}%",
            "{]%",
            "}%\\",
            "]%]",
            "null%[",
            "false%}",
            "true%{",
            r#""xyz[zy"%""#,
            r#""xy\"z]zy"%\\"#,
        ] {
            let mut splitter = JsonSplitter::default();
            let (before, after) = s.split_once('%').unwrap();
            assert_eq!(
                splitter.input(format!("{before}{after}").as_bytes()),
                Some(before.len()),
                "splitting {s:?} failed"
            );
            assert_eq!(splitter, JsonSplitter::default());
        }

        // Make sure that the splitter will not hang on invalid input, as would
        // happen if a given character caused it to emit zero-length output.
        for c in u8::MIN..u8::MAX {
            let mut splitter = JsonSplitter::default();
            assert_ne!(splitter.input(&[c]), Some(0));
        }
    }
}
