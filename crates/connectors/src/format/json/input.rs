//! JSON format parser.

use super::{DebeziumUpdate, InsDelUpdate};
use crate::catalog::InputCollectionHandle;
use crate::format::{InputBuffer, LineSplitter};
use crate::{
    ControllerError,
    catalog::{DeCollectionStream, RecordFormat},
    format::{InputFormat, ParseError, Parser},
};
use erased_serde::Serialize as ErasedSerialize;
use feldera_adapterlib::ConnectorMetadata;
use feldera_adapterlib::catalog::StagedBuffers;
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
        let config = JsonParserConfig::deserialize(config)
            .map_err(|e| ControllerError::parser_config_parse_error(endpoint_name, &e))?;
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
            .map_err(|e| ControllerError::parser_config_parse_error(endpoint_name, &e))?,
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

#[cfg(any(test, feature = "testing"))]
pub mod test {
    #[cfg(test)]
    use super::JsonSplitter;
    use crate::test::{engine_test, parser, relations, render, rows};
    use JsonLines::{Multiple, Single};
    use JsonUpdateFormat::{Debezium, InsertDelete, Raw};
    use chrono::DateTime;
    #[cfg(test)]
    use feldera_adapterlib::format::Splitter;
    use feldera_adapterlib::{
        ConnectorMetadata,
        format::{InputBuffer, ParseError, Parser},
        test::{ConnectorTestEngine, InputOptions, Updates},
    };
    use feldera_types::{
        config::FormatConfig,
        format::json::{JsonFlavor, JsonLines, JsonParserConfig, JsonUpdateFormat},
        program_schema::{ColumnType, Field, Relation},
        variant::VariantValue,
    };
    use proptest::prelude::*;
    use std::{borrow::Cow, collections::BTreeMap, panic::Location};

    /// The table that most cases parse into.
    pub fn relation() -> Relation {
        Relation::new(
            "json_input".into(),
            vec![
                Field::new("b".into(), ColumnType::boolean(false)),
                Field::new("i".into(), ColumnType::int(false)),
                Field::new("s".into(), ColumnType::varchar(true)),
            ],
            false,
            BTreeMap::new(),
        )
    }

    /// A table whose columns other than `i` take their values from the Kafka connector
    /// attributes of the same names, as [`metadata_options`] declares.
    pub fn metadata_relation() -> Relation {
        Relation::new(
            "json_metadata".into(),
            vec![
                Field::new("i".into(), ColumnType::int(false)),
                Field::new("kafka_headers".into(), ColumnType::variant(false)),
                Field::new("kafka_topic".into(), ColumnType::varchar(false)),
                Field::new("kafka_timestamp".into(), ColumnType::timestamp(false)),
                Field::new("kafka_partition".into(), ColumnType::int(false)),
                Field::new("kafka_offset".into(), ColumnType::bigint(false)),
            ],
            false,
            BTreeMap::new(),
        )
    }

    /// The input options of [`metadata_relation`]: each Kafka column's DEFAULT reads the
    /// attribute of the same name.
    pub fn metadata_options() -> InputOptions {
        InputOptions {
            soft_delete: false,
            attribute_defaults: [
                "kafka_headers",
                "kafka_topic",
                "kafka_timestamp",
                "kafka_partition",
                "kafka_offset",
            ]
            .into_iter()
            .map(|name| (name.to_string(), name.to_string()))
            .collect(),
        }
    }

    /// One case: the chunks that the parser reads, each with the errors that it reports, and
    /// the updates that the table receives, in order.
    struct Case {
        location: &'static Location<'static>,
        config: JsonParserConfig,
        chunks: Vec<(&'static str, Vec<ParseError>)>,
        updates: Vec<(i64, &'static str)>,
        metadata: Option<ConnectorMetadata>,
    }

    impl Case {
        #[track_caller]
        fn new(
            config: JsonParserConfig,
            chunks: Vec<(&'static str, Vec<ParseError>)>,
            updates: Vec<(i64, &'static str)>,
        ) -> Self {
            Self {
                location: Location::caller(),
                config,
                chunks,
                updates,
                metadata: None,
            }
        }

        fn with_metadata(self, metadata: ConnectorMetadata) -> Self {
            Self {
                metadata: Some(metadata),
                ..self
            }
        }
    }

    /// A JSON parser configuration of the default flavor.
    fn config(update_format: JsonUpdateFormat, array: bool, lines: JsonLines) -> JsonParserConfig {
        JsonParserConfig {
            update_format,
            json_flavor: JsonFlavor::Default,
            array,
            lines,
        }
    }

    /// The error for a chunk that is not JSON.
    fn document_error(message: &str, text: &str) -> ParseError {
        ParseError::text_envelope_error(message.to_string(), text, None)
    }

    /// The error for record number `event`, which does not fit the table.
    fn record_error(message: &str, event: u64, field: Option<&str>, text: &str) -> ParseError {
        ParseError::new(
            message.to_string(),
            Some(event),
            field.map(str::to_string),
            Some(text),
            None,
            None,
            Some("text_event_err".to_string()),
        )
    }

    /// Runs each case on a fresh input collection of `relation`.
    fn run(
        engine: &dyn ConnectorTestEngine,
        relation: &Relation,
        options: &InputOptions,
        cases: Vec<Case>,
    ) {
        for case in cases {
            let collection = engine.input_collection(relation, options).unwrap();
            let format = FormatConfig {
                name: Cow::from("json"),
                config: serde_json::to_value(&case.config).unwrap(),
            };
            let mut parser = parser(collection.as_ref(), &format);
            parser.on_error(Some(Box::new(|_, _| {})));
            for (chunk, errors) in &case.chunks {
                let (mut buffer, reported) = parser.parse(chunk.as_bytes(), case.metadata.clone());
                assert_eq!(&reported, errors, "{}", case.location);
                buffer.flush().unwrap();
            }
            let expected = Updates::from_json(collection.schema(), &case.updates).unwrap();
            assert_eq!(collection.updates().unwrap(), expected, "{}", case.location);
        }
    }

    /// Raw rows, alone or several to a chunk, as objects or as arrays of column values.
    #[engine_test(requires(JsonArrayRecords))]
    pub fn raw_records(engine: &dyn ConnectorTestEngine) {
        run(
            engine,
            &relation(),
            &InputOptions::default(),
            vec![
                Case::new(
                    config(Raw, false, Single),
                    vec![(r#"{"b": true, "i": 0}"#, vec![])],
                    vec![(1, r#"{"b": true, "i": 0}"#)],
                ),
                Case::new(
                    config(Raw, false, Multiple),
                    vec![("{\n    \"b\": true,\n    \"i\": 0\n}\n", vec![])],
                    vec![(1, r#"{"b": true, "i": 0}"#)],
                ),
                Case::new(
                    config(Raw, false, Single),
                    vec![(r#"[true, 0, "a"]"#, vec![])],
                    vec![(1, r#"{"b": true, "i": 0, "s": "a"}"#)],
                ),
                Case::new(
                    config(Raw, true, Single),
                    vec![(r#"[{"b": true, "i": 0}]"#, vec![])],
                    vec![(1, r#"{"b": true, "i": 0}"#)],
                ),
                Case::new(
                    config(Raw, true, Single),
                    vec![(r#"[[true, 0, "b"]]"#, vec![])],
                    vec![(1, r#"{"b": true, "i": 0, "s": "b"}"#)],
                ),
                // Two records in one chunk.
                Case::new(
                    config(Raw, false, Single),
                    vec![(
                        r#"{"b": true, "i": 0}{"b": false, "i": 100, "s": "foo"}"#,
                        vec![],
                    )],
                    vec![
                        (1, r#"{"b": true, "i": 0}"#),
                        (1, r#"{"b": false, "i": 100, "s": "foo"}"#),
                    ],
                ),
                Case::new(
                    config(Raw, false, Single),
                    vec![(r#"[true, 0, "c"][false, 100, "foo"]"#, vec![])],
                    vec![
                        (1, r#"{"b": true, "i": 0, "s": "c"}"#),
                        (1, r#"{"b": false, "i": 100, "s": "foo"}"#),
                    ],
                ),
                Case::new(
                    config(Raw, true, Single),
                    vec![(
                        r#"[{"b": true, "i": 0},{"b": false, "i": 100, "s": "foo"}]"#,
                        vec![],
                    )],
                    vec![
                        (1, r#"{"b": true, "i": 0}"#),
                        (1, r#"{"b": false, "i": 100, "s": "foo"}"#),
                    ],
                ),
                Case::new(
                    config(Raw, true, Single),
                    vec![(r#"[[true, 0, "d"],[false, 100, "foo"]]"#, vec![])],
                    vec![
                        (1, r#"{"b": true, "i": 0, "s": "d"}"#),
                        (1, r#"{"b": false, "i": 100, "s": "foo"}"#),
                    ],
                ),
                // Two chunks of one record each.
                Case::new(
                    config(Raw, false, Single),
                    vec![
                        (r#"{"b": true, "i": 0}"#, vec![]),
                        (r#"{"b": false, "i": 100, "s": "foo"}"#, vec![]),
                    ],
                    vec![
                        (1, r#"{"b": true, "i": 0}"#),
                        (1, r#"{"b": false, "i": 100, "s": "foo"}"#),
                    ],
                ),
                Case::new(
                    config(Raw, false, Single),
                    vec![
                        (r#"[true, 0, "e"]"#, vec![]),
                        (r#"[false, 100, "foo"]"#, vec![]),
                    ],
                    vec![
                        (1, r#"{"b": true, "i": 0, "s": "e"}"#),
                        (1, r#"{"b": false, "i": 100, "s": "foo"}"#),
                    ],
                ),
                Case::new(
                    config(Raw, true, Single),
                    vec![
                        (r#"[{"b": true, "i": 0}]"#, vec![]),
                        (r#"[{"b": false, "i": 100, "s": "foo"}]"#, vec![]),
                    ],
                    vec![
                        (1, r#"{"b": true, "i": 0}"#),
                        (1, r#"{"b": false, "i": 100, "s": "foo"}"#),
                    ],
                ),
                Case::new(
                    config(Raw, true, Single),
                    vec![
                        (r#"[[true, 0, "e"]]"#, vec![]),
                        (r#"[[false, 100, "foo"]]"#, vec![]),
                    ],
                    vec![
                        (1, r#"{"b": true, "i": 0, "s": "e"}"#),
                        (1, r#"{"b": false, "i": 100, "s": "foo"}"#),
                    ],
                ),
            ],
        );
    }

    /// Raw input that is an insert/delete envelope, that is not JSON, or whose records do not
    /// fit the table. A record that does not fit is dropped, along with the rest of its array.
    #[engine_test(requires(SerdeParseErrors))]
    pub fn raw_errors(engine: &dyn ConnectorTestEngine) {
        let envelope_hint = |text: &str| {
            ParseError::text_envelope_error(
                "payload looks like an insert/delete envelope; the configured `raw` update format expects plain rows".to_string(),
                text,
                Some(Cow::from(
                    "Set `format.config.update_format` to `insert_delete` for payloads like {\"insert\": {...}} or {\"delete\": {...}}.",
                )),
            )
        };
        run(
            engine,
            &relation(),
            &InputOptions::default(),
            vec![
                Case::new(
                    config(Raw, false, Single),
                    vec![(
                        r#"{"insert": {"b": true, "i": 0}}"#,
                        vec![envelope_hint(r#"{"insert": {"b": true, "i": 0}}"#)],
                    )],
                    vec![],
                ),
                Case::new(
                    config(Raw, true, Single),
                    vec![(
                        r#"[{"insert": {"b": true, "i": 0}}]"#,
                        vec![envelope_hint(r#"[{"insert": {"b": true, "i": 0}}]"#)],
                    )],
                    vec![],
                ),
                // Not JSON.
                Case::new(
                    config(Raw, false, Single),
                    vec![
                        (r#"{"b": true, "i": 0}"#, vec![]),
                        (
                            r#"{"b": false, "i": 100, "s":"#,
                            vec![document_error(
                                "failed to parse string as a JSON document: EOF while parsing a value at line 1 column 27",
                                r#"{"b": false, "i": 100, "s":"#,
                            )],
                        ),
                    ],
                    vec![(1, r#"{"b": true, "i": 0}"#)],
                ),
                Case::new(
                    config(Raw, false, Single),
                    vec![
                        (r#"[true, 0, "f"]"#, vec![]),
                        (
                            r#"[false, 100, "#,
                            vec![document_error(
                                "failed to parse string as a JSON document: EOF while parsing a value at line 1 column 13",
                                r#"[false, 100, "#,
                            )],
                        ),
                    ],
                    vec![(1, r#"{"b": true, "i": 0, "s": "f"}"#)],
                ),
                Case::new(
                    config(Raw, true, Single),
                    vec![
                        (r#"[{"b": true, "i": 0}]"#, vec![]),
                        (
                            r#"[{"b": false, "i": 100, "s":"#,
                            vec![document_error(
                                "failed to parse string as a JSON document: EOF while parsing a value at line 1 column 28",
                                r#"[{"b": false, "i": 100, "s":"#,
                            )],
                        ),
                    ],
                    vec![(1, r#"{"b": true, "i": 0}"#)],
                ),
                Case::new(
                    config(Raw, true, Single),
                    vec![
                        (r#"[[true, 0, "g"]]"#, vec![]),
                        (
                            r#"[[false, 100, "s":"#,
                            vec![document_error(
                                "failed to parse string as a JSON document: expected `,` or `]` at line 1 column 18",
                                r#"[[false, 100, "s":"#,
                            )],
                        ),
                    ],
                    vec![(1, r#"{"b": true, "i": 0, "s": "g"}"#)],
                ),
                // JSON whose records do not fit the table.
                Case::new(
                    config(Raw, false, Single),
                    vec![
                        (r#"{"b": true, "i": 0}"#, vec![]),
                        (
                            r#"{"b": false, "i": 5}{"b": false}{"b": false, "i": "hello"}"#,
                            vec![
                                record_error(
                                    "failed to deserialize JSON record: missing field `i` at line 1 column 12",
                                    3,
                                    None,
                                    r#"{"b": false}"#,
                                ),
                                record_error(
                                    "failed to deserialize JSON record: error parsing field 'i': invalid type: string \"hello\", expected i32 at line 1 column 25",
                                    4,
                                    Some("i"),
                                    r#"{"b": false, "i": "hello"}"#,
                                ),
                            ],
                        ),
                    ],
                    vec![
                        (1, r#"{"b": true, "i": 0}"#),
                        (1, r#"{"b": false, "i": 5}"#),
                    ],
                ),
                Case::new(
                    config(Raw, true, Single),
                    vec![
                        (r#"[{"b": true, "i": 0}]"#, vec![]),
                        (
                            r#"[{"b": false, "i": 5},{"b": false}]"#,
                            vec![record_error(
                                "failed to deserialize JSON record: missing field `i` at line 1 column 12",
                                3,
                                None,
                                r#"{"b": false}"#,
                            )],
                        ),
                        (
                            r#"[{"b": false, "i": 5},{"b": 20, "i": 10}]"#,
                            vec![record_error(
                                "failed to deserialize JSON record: error parsing field 'b': invalid type: integer `20`, expected a boolean at line 1 column 8",
                                5,
                                Some("b"),
                                r#"{"b": 20, "i": 10}"#,
                            )],
                        ),
                    ],
                    vec![(1, r#"{"b": true, "i": 0}"#)],
                ),
                Case::new(
                    config(Raw, true, Single),
                    vec![
                        (r#"[[true, 0, "h"]]"#, vec![]),
                        (
                            r#"[{"b": false, "i": 5},[false]]"#,
                            vec![record_error(
                                "failed to deserialize JSON record: invalid length 1, expected 3 columns at line 1 column 7",
                                3,
                                None,
                                "[false]",
                            )],
                        ),
                    ],
                    vec![(1, r#"{"b": true, "i": 0, "s": "h"}"#)],
                ),
            ],
        );
    }

    /// Inserts and deletes in the insert/delete format, alone or several to a chunk.
    #[engine_test(requires(JsonArrayRecords))]
    pub fn insert_delete_records(engine: &dyn ConnectorTestEngine) {
        run(
            engine,
            &relation(),
            &InputOptions::default(),
            vec![
                Case::new(
                    config(InsertDelete, false, Single),
                    vec![(r#"{"insert": {"b": true, "i": 0}}"#, vec![])],
                    vec![(1, r#"{"b": true, "i": 0}"#)],
                ),
                Case::new(
                    config(InsertDelete, false, Multiple),
                    vec![(
                        "\n{\n    \"insert\": {\n        \"b\": true,\n        \"i\": 0\n    }\n}\n",
                        vec![],
                    )],
                    vec![(1, r#"{"b": true, "i": 0}"#)],
                ),
                Case::new(
                    config(InsertDelete, true, Single),
                    vec![(r#"[{"insert": {"b": true, "i": 0}}]"#, vec![])],
                    vec![(1, r#"{"b": true, "i": 0}"#)],
                ),
                // Two records in one chunk.
                Case::new(
                    config(InsertDelete, false, Single),
                    vec![(
                        r#"{"insert": {"b": true, "i": 0}}{"delete": {"b": false, "i": 100, "s": "foo"}}"#,
                        vec![],
                    )],
                    vec![
                        (1, r#"{"b": true, "i": 0}"#),
                        (-1, r#"{"b": false, "i": 100, "s": "foo"}"#),
                    ],
                ),
                Case::new(
                    config(InsertDelete, true, Single),
                    vec![(
                        r#"[{"insert": {"b": true, "i": 0}}, {"delete": {"b": false, "i": 100, "s": "foo"}}]"#,
                        vec![],
                    )],
                    vec![
                        (1, r#"{"b": true, "i": 0}"#),
                        (-1, r#"{"b": false, "i": 100, "s": "foo"}"#),
                    ],
                ),
                Case::new(
                    config(InsertDelete, true, Single),
                    vec![(
                        r#"[{"insert": [true, 0, "a"]}, {"delete": {"b": false, "i": 100, "s": "foo"}}]"#,
                        vec![],
                    )],
                    vec![
                        (1, r#"{"b": true, "i": 0, "s": "a"}"#),
                        (-1, r#"{"b": false, "i": 100, "s": "foo"}"#),
                    ],
                ),
                // Two chunks of one record each.
                Case::new(
                    config(InsertDelete, false, Single),
                    vec![
                        (r#"{"insert": {"b": true, "i": 0}}"#, vec![]),
                        (r#"{"delete": {"b": false, "i": 100, "s": "foo"}}"#, vec![]),
                    ],
                    vec![
                        (1, r#"{"b": true, "i": 0}"#),
                        (-1, r#"{"b": false, "i": 100, "s": "foo"}"#),
                    ],
                ),
            ],
        );
    }

    /// Insert/delete input that is not JSON, that is not an envelope, or whose records do not
    /// fit the table.
    #[engine_test(requires(SerdeParseErrors))]
    pub fn insert_delete_errors(engine: &dyn ConnectorTestEngine) {
        let missing_i = |event| {
            record_error(
                "failed to deserialize JSON record: missing field `i` at line 1 column 12",
                event,
                None,
                r#"{"b": false}"#,
            )
        };
        run(engine, &relation(), &InputOptions::default(), vec![
            // Not JSON.
            Case::new(
                config(InsertDelete, false, Single),
                vec![
                    (r#"{"insert": {"b": true, "i": 0}}"#, vec![]),
                    (r#"{"delete": {"b": false, "i": 100, "s":"#, vec![document_error("failed to parse string as a JSON document: EOF while parsing a value at line 1 column 38", r#"{"delete": {"b": false, "i": 100, "s":"#)]),
                ],
                vec![(1, r#"{"b": true, "i": 0}"#)],
            ),
            Case::new(
                config(InsertDelete, true, Single),
                vec![
                    (r#"[{"insert": {"b": true, "i": 0}}]"#, vec![]),
                    (r#"[{"delete": {"b": false, "i": 100, "s":"#, vec![document_error("failed to parse string as a JSON document: EOF while parsing a value at line 1 column 39", r#"[{"delete": {"b": false, "i": 100, "s":"#)]),
                ],
                vec![(1, r#"{"b": true, "i": 0}"#)],
            ),
            // JSON whose records do not fit the table.
            Case::new(
                config(InsertDelete, false, Single),
                vec![
                    (r#"{"insert": {"b": true, "i": 0}}"#, vec![]),
                    (r#"{"insert": {"b": false, "i": 5}}{"delete": {"b": false}}"#, vec![missing_i(3)]),
                ],
                vec![(1, r#"{"b": true, "i": 0}"#), (1, r#"{"b": false, "i": 5}"#)],
            ),
            Case::new(
                config(InsertDelete, true, Single),
                vec![
                    (r#"[{"insert": {"b": true, "i": 0}}]"#, vec![]),
                    (r#"[{"insert": {"b": false, "i": 5}},{"delete": {"b": false}}]"#, vec![missing_i(3)]),
                ],
                vec![(1, r#"{"b": true, "i": 0}"#)],
            ),
            Case::new(
                config(InsertDelete, true, Single),
                vec![
                    (r#"[{"insert": {"b": true, "i": 0}}]"#, vec![]),
                    (r#"[{"insert": {"b": false, "i": 5}},{"delete": {"b": false}}]"#, vec![missing_i(3)]),
                    (r#"[{"insert": {"b": true, "i": 0}}]"#, vec![]),
                    (r#"[{"delete": {"b": false}}]"#, vec![missing_i(5)]),
                    (r#"[{"b": false}]"#, vec![ParseError::text_envelope_error(
                        "error deserializing string as a JSON array of updates: unknown field `b`, expected one of `table`, `insert`, `delete`, `update` at line 1 column 5".to_string(),
                        r#"[{"b": false}]"#,
                        Some(Cow::from("Example valid JSON: '[{{\"insert\": {{...}} }}, {{\"delete\": {{...}} }}]'")),
                    )]),
                ],
                vec![(1, r#"{"b": true, "i": 0}"#), (1, r#"{"b": true, "i": 0}"#)],
            ),
        ]);
    }

    /// Debezium change events: creates, updates as a delete and an insert, and deletes.
    #[engine_test(requires(JsonArrayRecords))]
    pub fn debezium_records(engine: &dyn ConnectorTestEngine) {
        run(
            engine,
            &relation(),
            &InputOptions::default(),
            vec![
                Case::new(
                    config(Debezium, false, Single),
                    vec![(
                        r#"{"payload": {"op": "c", "after": {"b": true, "i": 0}}}"#,
                        vec![],
                    )],
                    vec![(1, r#"{"b": true, "i": 0}"#)],
                ),
                Case::new(
                    config(Debezium, false, Single),
                    vec![(
                        r#"{"payload": {"op": "u", "before": {"b": true, "i": 123}, "after": {"b": true, "i": 0}}}"#,
                        vec![],
                    )],
                    vec![
                        (-1, r#"{"b": true, "i": 123}"#),
                        (1, r#"{"b": true, "i": 0}"#),
                    ],
                ),
                Case::new(
                    config(Debezium, false, Single),
                    vec![(
                        r#"{"payload": {"op": "u", "before": [true, 123, "abc"], "after": [true, 0, "def"]}}"#,
                        vec![],
                    )],
                    vec![
                        (-1, r#"{"b": true, "i": 123, "s": "abc"}"#),
                        (1, r#"{"b": true, "i": 0, "s": "def"}"#),
                    ],
                ),
                // Two records in one chunk.
                Case::new(
                    config(Debezium, false, Single),
                    vec![(
                        r#"{"payload": {"op": "c", "after": {"b": true, "i": 0}}}{"payload": {"op": "d", "before": {"b": false, "i": 100, "s": "foo"}}}"#,
                        vec![],
                    )],
                    vec![
                        (1, r#"{"b": true, "i": 0}"#),
                        (-1, r#"{"b": false, "i": 100, "s": "foo"}"#),
                    ],
                ),
                // Two chunks of one record each.
                Case::new(
                    config(Debezium, false, Single),
                    vec![
                        (
                            r#"{"payload": {"op": "c", "after": {"b": true, "i": 0}}}"#,
                            vec![],
                        ),
                        (
                            r#"{"payload": {"op": "d", "before": {"b": false, "i": 100, "s": "foo"}}}"#,
                            vec![],
                        ),
                    ],
                    vec![
                        (1, r#"{"b": true, "i": 0}"#),
                        (-1, r#"{"b": false, "i": 100, "s": "foo"}"#),
                    ],
                ),
            ],
        );
    }

    /// Debezium input that is not JSON, or whose records do not fit the table.
    #[engine_test(requires(SerdeParseErrors))]
    pub fn debezium_errors(engine: &dyn ConnectorTestEngine) {
        run(
            engine,
            &relation(),
            &InputOptions::default(),
            vec![
                Case::new(
                    config(Debezium, false, Single),
                    vec![
                        (
                            r#"{"payload": {"op": "c", "after": {"b": true, "i": 0}}}"#,
                            vec![],
                        ),
                        (
                            r#"{"payload": {"op": "d", "before": {"b": false, "i": 100, "s":"#,
                            vec![document_error(
                                "failed to parse string as a JSON document: EOF while parsing a value at line 1 column 61",
                                r#"{"payload": {"op": "d", "before": {"b": false, "i": 100, "s":"#,
                            )],
                        ),
                    ],
                    vec![(1, r#"{"b": true, "i": 0}"#)],
                ),
                Case::new(
                    config(Debezium, false, Single),
                    vec![
                        (
                            r#"{"payload": {"op": "c", "after": {"b": true, "i": 0}}}"#,
                            vec![],
                        ),
                        (
                            r#"{"payload": {"op": "c", "after": {"b": false, "i": 5}}}{"payload": {"op": "d", "before": {"b": false}}}"#,
                            vec![record_error(
                                "failed to deserialize JSON record: missing field `i` at line 1 column 12",
                                3,
                                None,
                                r#"{"b": false}"#,
                            )],
                        ),
                    ],
                    vec![
                        (1, r#"{"b": true, "i": 0}"#),
                        (1, r#"{"b": false, "i": 5}"#),
                    ],
                ),
            ],
        );
    }

    /// Columns whose DEFAULTs read connector attributes take them from the metadata that the
    /// transport attaches to the chunk.
    #[engine_test]
    pub fn metadata(engine: &dyn ConnectorTestEngine) {
        let mut metadata = ConnectorMetadata::new();
        metadata.insert("kafka_headers", VariantValue::Object(BTreeMap::new()));
        metadata.insert("kafka_topic", VariantValue::String("my_topic".into()));
        metadata.insert(
            "kafka_timestamp",
            VariantValue::TimestampNtzMicros(
                DateTime::from_timestamp_millis(1763626606441)
                    .unwrap()
                    .naive_utc(),
            ),
        );
        metadata.insert("kafka_partition", VariantValue::Int32(10));
        metadata.insert("kafka_offset", VariantValue::Int32(1_000_000));

        run(engine, &metadata_relation(), &metadata_options(), vec![
            Case::new(
                config(Raw, false, Single),
                vec![(r#"{"i": 0}"#, vec![])],
                vec![(1, r#"{"i": 0, "kafka_headers": "{}", "kafka_topic": "my_topic", "kafka_timestamp": "2025-11-20T08:16:46.441", "kafka_partition": 10, "kafka_offset": 1000000}"#)],
            )
            .with_metadata(metadata),
        ]);
    }

    /// The table of [`random_rows`]: a column of each type that JSON holds.
    pub fn every_type_relation() -> Relation {
        relations::every_type("json_every_type")
    }

    /// Random rows of every type, written one object per line in the default flavor, read back
    /// unchanged and in order.
    #[engine_test]
    pub fn random_rows(engine: &dyn ConnectorTestEngine) {
        let relation = every_type_relation();
        let format = FormatConfig {
            name: Cow::from("json"),
            config: serde_json::to_value(config(Raw, false, Multiple)).unwrap(),
        };
        proptest!(ProptestConfig::with_cases(32), |(batches in rows::batches(&relation, "bigint", 1..=1, 20))| {
            let rows = &batches[0];
            let collection = engine
                .input_collection(&relation, &InputOptions::default())
                .unwrap();
            let mut parser = parser(collection.as_ref(), &format);
            let (mut buffer, errors) =
                parser.parse(render::json_lines(&relation, rows.rows()).as_bytes(), None);
            prop_assert_eq!(errors, vec![]);
            buffer.flush().unwrap();
            prop_assert_eq!(&collection.updates().unwrap(), rows);
        });
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
