//! JSON format parser.

use super::{DebeziumUpdate, InsDelUpdate, WeightedUpdate};
use crate::catalog::InputCollectionHandle;
use crate::{
    catalog::{DeCollectionStream, RecordFormat},
    format::{InputFormat, ParseError, Parser},
    util::split_on_newline,
    ControllerError,
};
use actix_web::HttpRequest;
use erased_serde::Serialize as ErasedSerialize;
use feldera_types::format::json::{JsonParserConfig, JsonUpdateFormat};
use serde::Deserialize;
use serde_json::value::RawValue;
use serde_urlencoded::Deserializer as UrlDeserializer;
use serde_yaml::Value as YamlValue;
use std::{borrow::Cow, mem::take};

/// JSON format parser.
pub struct JsonInputFormat;

trait UpdateFormat {
    fn error() -> &'static str;
    fn array_error() -> &'static str;
    fn example() -> Option<&'static str>;
    fn array_example() -> Option<&'static str>;
    fn apply(self, parser: &mut JsonParser) -> Result<usize, ParseError>;
}

impl<'a> UpdateFormat for InsDelUpdate<&'a RawValue> {
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

    fn apply(self, parser: &mut JsonParser) -> Result<usize, ParseError> {
        let mut updates = 0;

        if let Some(val) = self.delete {
            parser.delete(val)?;
            updates += 1;
        }

        if let Some(val) = self.insert {
            parser.insert(val)?;
            updates += 1;
        }

        if let Some(val) = self.update {
            parser.update(val)?;
            updates += 1;
        }

        Ok(updates)
    }
}

impl<'a> UpdateFormat for DebeziumUpdate<&'a RawValue> {
    fn error() -> &'static str {
        "error deserializing JSON string as a Debezium CDC event"
    }

    fn array_error() -> &'static str {
        "error deserializing string as a JSON array of Debezium CDC events"
    }

    fn example() -> Option<&'static str> {
        Some("Example valid JSON: '{{\"payload\": {{\"op\": \"u\", \"before\": {{...}}, \"after\": {{...}} }} }}'")
    }

    fn array_example() -> Option<&'static str> {
        Some("Example valid JSON: '[{{\"payload\": {{\"op\": \"u\", \"before\": {{...}}, \"after\": {{...}} }} }}]'")
    }

    fn apply(self, parser: &mut JsonParser) -> Result<usize, ParseError> {
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
            parser.delete(before)?;
            updates += 1;
        };

        if let Some(after) = &self.payload.after {
            parser.insert(after)?;
            updates += 1;
        };

        Ok(updates)
    }
}

impl<'a> UpdateFormat for WeightedUpdate<&'a RawValue> {
    fn error() -> &'static str {
        "error deserializing JSON string as a weighted record"
    }

    fn array_error() -> &'static str {
        "error deserializing string as a JSON array of weighted records"
    }

    fn example() -> Option<&'static str> {
        Some("Example valid JSON: '{{\"weight\": 1, \"data\": {{...}} }}'")
    }

    fn array_example() -> Option<&'static str> {
        Some("Example valid JSON: '[{{\"weight\": 1, \"data\": {{...}} }}, {{\"weight\": -1, \"data\": {{...}} }}]'")
    }

    fn apply(self, _parser: &mut JsonParser) -> Result<usize, ParseError> {
        todo!()
    }
}

impl<'a> UpdateFormat for &'a RawValue {
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

    fn apply(self, parser: &mut JsonParser) -> Result<usize, ParseError> {
        parser.insert(self)?;
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
        config: &YamlValue,
    ) -> Result<Box<dyn Parser>, ControllerError> {
        let config = JsonParserConfig::deserialize(config).map_err(|e| {
            ControllerError::parser_config_parse_error(
                endpoint_name,
                &e,
                &serde_yaml::to_string(config).unwrap_or_default(),
            )
        })?;
        validate_parser_config(&config, endpoint_name)?;
        let input_stream = input_handle
            .handle
            .configure_deserializer(RecordFormat::Json(config.json_flavor.clone()))?;
        Ok(Box::new(JsonParser::new(input_stream, config)) as Box<dyn Parser>)
    }

    fn config_from_http_request(
        &self,
        endpoint_name: &str,
        request: &HttpRequest,
    ) -> Result<Box<dyn ErasedSerialize>, ControllerError> {
        Ok(Box::new(
            JsonParserConfig::deserialize(UrlDeserializer::new(form_urlencoded::parse(
                request.query_string().as_bytes(),
            )))
            .map_err(|e| {
                ControllerError::parser_config_parse_error(
                    endpoint_name,
                    &e,
                    request.query_string(),
                )
            })?,
        ))
    }
}

fn validate_parser_config(
    config: &JsonParserConfig,
    endpoint_name: &str,
) -> Result<(), ControllerError> {
    if config.update_format == JsonUpdateFormat::Snowflake {
        return Err(ControllerError::input_format_not_supported(
            endpoint_name,
            &format!(
                "{:?} is not supported for JSON input streams",
                config.update_format
            ),
        ));
    }

    Ok(())
}

struct JsonParser {
    /// Input handle to push parsed data to.
    input_stream: Box<dyn DeCollectionStream>,
    config: JsonParserConfig,
    leftover: Vec<u8>,
    last_event_number: u64,
}

impl JsonParser {
    fn new(input_stream: Box<dyn DeCollectionStream>, config: JsonParserConfig) -> Self {
        Self {
            input_stream,
            config,
            leftover: Vec::new(),
            last_event_number: 0,
        }
    }

    fn flush(&mut self) {
        self.input_stream.flush();
    }

    fn clear(&mut self) {
        self.input_stream.clear_buffer();
    }

    fn delete(&mut self, val: &RawValue) -> Result<(), ParseError> {
        self.input_stream.delete(val.get().as_bytes()).map_err(|e| {
            ParseError::text_event_error(
                "failed to deserialize JSON record",
                e,
                self.last_event_number + 1,
                Some(val.get()),
                None,
            )
        })
    }

    fn insert(&mut self, val: &RawValue) -> Result<(), ParseError> {
        self.input_stream.insert(val.get().as_bytes()).map_err(|e| {
            ParseError::text_event_error(
                "failed to deserialize JSON record",
                e,
                self.last_event_number + 1,
                Some(val.get()),
                None,
            )
        })
    }

    fn update(&mut self, val: &RawValue) -> Result<(), ParseError> {
        self.input_stream.update(val.get().as_bytes()).map_err(|e| {
            ParseError::text_event_error(
                "failed to deserialize JSON record",
                e,
                self.last_event_number + 1,
                Some(val.get()),
                None,
            )
        })
    }

    fn apply_update<'de, F>(&mut self, update: &'de RawValue, errors: &mut Vec<ParseError>) -> usize
    where
        F: UpdateFormat + Deserialize<'de>,
    {
        let mut num_updates = 0;

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
                    for update in updates {
                        match update.apply(self) {
                            Err(e) => {
                                error = true;
                                errors.push(e);
                            }
                            Ok(nupdates) => {
                                num_updates += nupdates;
                            }
                        }
                        self.last_event_number += 1;
                    }
                    if error {
                        self.clear();
                    } else {
                        self.flush();
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
                Ok(update) => match update.apply(self) {
                    Err(e) => {
                        errors.push(e);
                    }
                    Ok(nupdates) => {
                        num_updates += nupdates;
                    }
                },
            }
            self.last_event_number += 1;
        }

        num_updates
    }

    fn input_from_slice(&mut self, bytes: &[u8]) -> (usize, Vec<ParseError>) {
        let mut num_updates = 0;
        let mut errors = Vec::new();

        let mut stream = serde_json::Deserializer::from_slice(bytes).into_iter::<&RawValue>();

        while let Some(update) = stream.next() {
            let update = match update {
                Err(e) => {
                    let json_str = String::from_utf8_lossy(&bytes[stream.byte_offset()..]);
                    errors.push(ParseError::text_envelope_error(
                        format!("failed to parse string as a JSON document: {e}"),
                        &json_str,
                        None,
                    ));
                    if !self.config.array {
                        self.flush();
                    }
                    return (num_updates, errors);
                }
                Ok(update) => update,
            };

            num_updates += match self.config.update_format {
                JsonUpdateFormat::InsertDelete => {
                    self.apply_update::<InsDelUpdate<_>>(update, &mut errors)
                }
                JsonUpdateFormat::Debezium => {
                    self.apply_update::<DebeziumUpdate<_>>(update, &mut errors)
                }
                JsonUpdateFormat::Weighted => {
                    self.apply_update::<WeightedUpdate<_>>(update, &mut errors)
                }
                JsonUpdateFormat::Raw => self.apply_update::<&RawValue>(update, &mut errors),
                JsonUpdateFormat::Snowflake { .. } => {
                    panic!("Unexpected update format: {:?}", &self.config.update_format)
                }
            }
        }

        if !self.config.array {
            self.flush();
        }
        (num_updates, errors)
    }
}

impl Parser for JsonParser {
    fn input_fragment(&mut self, data: &[u8]) -> (usize, Vec<ParseError>) {
        // println!("input_fragment {}", std::str::from_utf8(data).unwrap());
        let leftover = split_on_newline(data);

        if leftover == 0 {
            // `data` doesn't contain a new-line character; append it to
            // the `leftover` buffer so it gets processed with the next input
            // buffer.
            self.leftover.extend_from_slice(data);
            (0, Vec::new())
        } else {
            self.leftover.extend_from_slice(&data[0..leftover]);
            let mut leftover_data = take(&mut self.leftover);
            let res = self.input_from_slice(&leftover_data);
            leftover_data.clear();
            leftover_data.extend_from_slice(&data[leftover..]);
            self.leftover = leftover_data;
            res
        }
    }

    fn input_chunk(&mut self, data: &[u8]) -> (usize, Vec<ParseError>) {
        self.input_from_slice(data)
    }

    fn eoi(&mut self) -> (usize, Vec<ParseError>) {
        /*println!(
            "eoi: leftover: {}",
            std::str::from_utf8(&self.leftover).unwrap()
        );*/
        if self.leftover.is_empty() {
            return (0, Vec::new());
        }

        // Try to interpret the leftover chunk as a complete JSON.
        let leftover = take(&mut self.leftover);
        let res = self.input_from_slice(leftover.as_slice());
        res
    }

    fn fork(&self) -> Box<dyn Parser> {
        Box::new(Self::new(self.input_stream.fork(), self.config.clone()))
    }
}

#[cfg(test)]
mod test {
    use crate::{
        test::{mock_parser_pipeline, MockUpdate},
        transport::InputConsumer,
        FormatConfig, ParseError,
    };
    use feldera_types::{
        deserialize_table_record,
        format::json::{JsonFlavor, JsonParserConfig, JsonUpdateFormat},
        program_schema::Relation,
        serde_with_context::{DeserializeWithContext, SqlSerdeConfig},
    };
    use log::trace;
    use std::{borrow::Cow, fmt::Debug};

    #[derive(PartialEq, Debug, Eq)]
    struct TestStruct {
        b: bool,
        i: i32,
        s: Option<String>,
    }

    deserialize_table_record!(TestStruct["TestStruct", 3] {
        (b, "b", false, bool, None),
        (i, "i", false, i32, None),
        (s, "s", false, Option<String>, Some(None))
    });

    // TODO: tests for RecordFormat::Raw.

    // Used to test RecordFormat::Raw.
    /*#[derive(Deserialize, PartialEq, Debug, Eq)]
    struct OneColumn {
        json: JsonValue,
    }*/

    impl TestStruct {
        fn new(b: bool, i: i32, s: Option<&str>) -> Self {
            Self {
                b,
                i,
                s: s.map(str::to_string),
            }
        }
    }

    #[derive(PartialEq, Debug, Eq)]
    struct TestStructUpd {
        b: Option<bool>,
        i: i32,
        s: Option<Option<String>>,
    }

    deserialize_table_record!(TestStructUpd["TestStructUpd", 3] {
        (b, "b", false, Option<bool>, Some(None)),
        (i, "i", false, i32, None),
        (s, "s", false, Option<Option<String>>, Some(None), |x| if x.is_none() { Some(None) } else {x})
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
        chunks: bool,
        config: JsonParserConfig,
        /// Input data, expected result.
        input_batches: Vec<(String, Vec<ParseError>)>,
        final_result: Vec<ParseError>,
        /// Expected contents at the end of the test.
        expected_output: Vec<MockUpdate<T, U>>,
    }

    fn run_test_cases<T, U>(test_cases: Vec<TestCase<T, U>>)
    where
        T: Debug + Eq + for<'de> DeserializeWithContext<'de, SqlSerdeConfig> + Send + 'static,
        U: Debug + Eq + for<'de> DeserializeWithContext<'de, SqlSerdeConfig> + Send + 'static,
    {
        for test in test_cases {
            trace!("test: {test:?}");
            let format_config = FormatConfig {
                name: Cow::from("json"),
                config: serde_yaml::to_value(test.config).unwrap(),
            };

            let (mut consumer, outputs) =
                mock_parser_pipeline(&Relation::empty(), &format_config).unwrap();
            consumer.on_error(Some(Box::new(|_, _| {})));
            for (json, expected_result) in test.input_batches {
                let res = if test.chunks {
                    consumer.input_chunk(json.as_bytes())
                } else {
                    consumer.input_fragment(json.as_bytes())
                };
                assert_eq!(&res, &expected_result);
            }
            let res = consumer.eoi();
            assert_eq!(&res, &test.final_result);
            assert_eq!(&test.expected_output, &outputs.state().flushed);
        }
    }

    impl<T, U> TestCase<T, U> {
        fn new(
            chunks: bool,
            config: JsonParserConfig,
            input_batches: Vec<(String, Vec<ParseError>)>,
            expected_output: Vec<MockUpdate<T, U>>,
            final_result: Vec<ParseError>,
        ) -> Self {
            Self {
                chunks,
                config,
                input_batches,
                expected_output,
                final_result,
            }
        }
    }

    #[test]
    fn test_json_variants() {
        let test_cases: Vec<TestCase<TestStruct, TestStructUpd>> = vec! [
            /* Raw update format */

            // raw: single-record chunk.
            TestCase::new(
                true,
                JsonParserConfig {
                    update_format: JsonUpdateFormat::Raw,
                    json_flavor: JsonFlavor::Default,
                    array: false,
                },
                vec![(r#"{"b": true, "i": 0}"#.to_string(), Vec::new())],
                vec![MockUpdate::with_polarity(TestStruct::new(true, 0, None), true)],
                Vec::new(),
            ),
            TestCase::new(
                true,
                JsonParserConfig {
                    update_format: JsonUpdateFormat::Raw,
                    json_flavor: JsonFlavor::Default,
                    array: false,
                },
                vec![(r#"[true, 0, "a"]"#.to_string(), Vec::new())],
                vec![MockUpdate::with_polarity(TestStruct::new(true, 0, Some("a")), true)],
                Vec::new(),
            ),
            TestCase::new(
                true,
                JsonParserConfig {
                    update_format: JsonUpdateFormat::Raw,
                    json_flavor: JsonFlavor::Default,
                    array: true,
                },
                vec![(r#"[{"b": true, "i": 0}]"#.to_string(), Vec::new())],
                vec![MockUpdate::with_polarity(TestStruct::new(true, 0, None), true)],
                Vec::new()
            ),
            TestCase::new(
                true,
                JsonParserConfig {
                    update_format: JsonUpdateFormat::Raw,
                    json_flavor: JsonFlavor::Default,
                    array: true,
                },
                vec![(r#"[[true, 0, "b"]]"#.to_string(), Vec::new())],
                vec![MockUpdate::with_polarity(TestStruct::new(true, 0, Some("b")), true)],
                Vec::new()
            ),
            // raw: one chunk, two records.
            TestCase::new(
                true,
                JsonParserConfig {
                    update_format: JsonUpdateFormat::Raw,
                    json_flavor: JsonFlavor::Default,
                    array: false,
                },
                vec![(r#"{"b": true, "i": 0}{"b": false, "i": 100, "s": "foo"}"#.to_string(), Vec::new())],
                vec![MockUpdate::with_polarity(TestStruct::new(true, 0, None), true), MockUpdate::with_polarity(TestStruct::new(false, 100, Some("foo")), true)],
                Vec::new()
            ),
            TestCase::new(
                true,
                JsonParserConfig {
                    update_format: JsonUpdateFormat::Raw,
                    json_flavor: JsonFlavor::Default,
                    array: false,
                },
                vec![(r#"[true, 0, "c"][false, 100, "foo"]"#.to_string(), Vec::new())],
                vec![MockUpdate::with_polarity(TestStruct::new(true, 0, Some("c")), true), MockUpdate::with_polarity(TestStruct::new(false, 100, Some("foo")), true)],
                Vec::new()
            ),
            TestCase::new(
                true,
                JsonParserConfig {
                    update_format: JsonUpdateFormat::Raw,
                    json_flavor: JsonFlavor::Default,
                    array: true,
                },
                vec![(r#"[{"b": true, "i": 0},{"b": false, "i": 100, "s": "foo"}]"#.to_string(), Vec::new())],
                vec![MockUpdate::with_polarity(TestStruct::new(true, 0, None), true), MockUpdate::with_polarity(TestStruct::new(false, 100, Some("foo")), true)],
                Vec::new()
            ),
            TestCase::new(
                true,
                JsonParserConfig {
                    update_format: JsonUpdateFormat::Raw,
                    json_flavor: JsonFlavor::Default,
                    array: true,
                },
                vec![(r#"[[true, 0, "d"],[false, 100, "foo"]]"#.to_string(), Vec::new())],
                vec![MockUpdate::with_polarity(TestStruct::new(true, 0, Some("d")), true), MockUpdate::with_polarity(TestStruct::new(false, 100, Some("foo")), true)],
                Vec::new()
            ),
            // raw: two chunks, one record each.
            TestCase::new(
                true,
                JsonParserConfig {
                    update_format: JsonUpdateFormat::Raw,
                    json_flavor: JsonFlavor::Default,
                    array: false,
                },
                vec![ (r#"{"b": true, "i": 0}"#.to_string(), Vec::new())
                    , (r#"{"b": false, "i": 100, "s": "foo"}"#.to_string(), Vec::new())],
                vec![MockUpdate::with_polarity(TestStruct::new(true, 0, None), true), MockUpdate::with_polarity(TestStruct::new(false, 100, Some("foo")), true)],
                Vec::new()
            ),
            TestCase::new(
                true,
                JsonParserConfig {
                    update_format: JsonUpdateFormat::Raw,
                    json_flavor: JsonFlavor::Default,
                    array: false,
                },
                vec![ (r#"[true, 0, "e"]"#.to_string(), Vec::new())
                    , (r#"[false, 100, "foo"]"#.to_string(), Vec::new())],
                vec![MockUpdate::with_polarity(TestStruct::new(true, 0, Some("e")), true), MockUpdate::with_polarity(TestStruct::new(false, 100, Some("foo")), true)],
                Vec::new()
            ),
            TestCase::new(
                true,
                JsonParserConfig {
                    update_format: JsonUpdateFormat::Raw,
                    json_flavor: JsonFlavor::Default,
                    array: true,
                },
                vec![ (r#"[{"b": true, "i": 0}]"#.to_string(), Vec::new())
                    , (r#"[{"b": false, "i": 100, "s": "foo"}]"#.to_string(), Vec::new())],
                vec![MockUpdate::with_polarity(TestStruct::new(true, 0, None), true), MockUpdate::with_polarity(TestStruct::new(false, 100, Some("foo")), true)],
                Vec::new()
            ),
            TestCase::new(
                true,
                JsonParserConfig {
                    update_format: JsonUpdateFormat::Raw,
                    json_flavor: JsonFlavor::Default,
                    array: true,
                },
                vec![ (r#"[[true, 0, "e"]]"#.to_string(), Vec::new())
                    , (r#"[[false, 100, "foo"]]"#.to_string(), Vec::new())],
                vec![MockUpdate::with_polarity(TestStruct::new(true, 0, Some("e")), true), MockUpdate::with_polarity(TestStruct::new(false, 100, Some("foo")), true)],
                Vec::new()
            ),
            // raw: invalid json.
            TestCase::new(
                true,
                JsonParserConfig {
                    update_format: JsonUpdateFormat::Raw,
                    json_flavor: JsonFlavor::Default,
                    array: false,
                },
                vec![ (r#"{"b": true, "i": 0}"#.to_string(), Vec::new())
                    , (r#"{"b": false, "i": 100, "s":"#.to_string(), vec![ParseError::text_envelope_error("failed to parse string as a JSON document: EOF while parsing a value at line 1 column 27".to_string(), "{\"b\": false, \"i\": 100, \"s\":", None)])],
                vec![MockUpdate::with_polarity(TestStruct::new(true, 0, None), true)],
                Vec::new()
            ),
            TestCase::new(
                true,
                JsonParserConfig {
                    update_format: JsonUpdateFormat::Raw,
                    json_flavor: JsonFlavor::Default,
                    array: false,
                },
                vec![ (r#"[true, 0, "f"]"#.to_string(), Vec::new())
                    , (r#"[false, 100, "#.to_string(), vec![ParseError::text_envelope_error("failed to parse string as a JSON document: EOF while parsing a value at line 1 column 13".to_string(), "[false, 100, ", None)])],
                vec![MockUpdate::with_polarity(TestStruct::new(true, 0, Some("f")), true)],
                Vec::new()
            ),
            TestCase::new(
                true,
                JsonParserConfig {
                    update_format: JsonUpdateFormat::Raw,
                    json_flavor: JsonFlavor::Default,
                    array: true,
                },
                vec![ (r#"[{"b": true, "i": 0}]"#.to_string(), Vec::new())
                    , (r#"[{"b": false, "i": 100, "s":"#.to_string(), vec![ParseError::text_envelope_error("failed to parse string as a JSON document: EOF while parsing a value at line 1 column 28".to_string(), "[{\"b\": false, \"i\": 100, \"s\":", None)])],
                vec![MockUpdate::with_polarity(TestStruct::new(true, 0, None), true)],
                Vec::new()
            ),
            TestCase::new(
                true,
                JsonParserConfig {
                    update_format: JsonUpdateFormat::Raw,
                    json_flavor: JsonFlavor::Default,
                    array: true,
                },
                vec![ (r#"[[true, 0, "g"]]"#.to_string(), Vec::new())
                    , (r#"[[false, 100, "s":"#.to_string(), vec![ParseError::text_envelope_error("failed to parse string as a JSON document: expected `,` or `]` at line 1 column 18".to_string(), "[[false, 100, \"s\":", None)])],
                vec![MockUpdate::with_polarity(TestStruct::new(true, 0, Some("g")), true)],
                Vec::new()
            ),
            // raw: valid json, but data doesn't match type definition.
            TestCase::new(
                true,
                JsonParserConfig {
                    update_format: JsonUpdateFormat::Raw,
                    json_flavor: JsonFlavor::Default,
                    array: false,
                },
                vec![ (r#"{"b": true, "i": 0}"#.to_string(), Vec::new())
                    , (r#"{"b": false, "i": 5}{"b": false}{"b": false, "i": "hello"}"#.to_string(), vec![ParseError::new("failed to deserialize JSON record: missing field `i` at line 1 column 12".to_string(), Some(3), None, Some("{\"b\": false}"), None, None), ParseError::new("failed to deserialize JSON record: error parsing field 'i': invalid type: string \"hello\", expected i32 at line 1 column 25".to_string(), Some(4), Some("i".to_string()), Some("{\"b\": false, \"i\": \"hello\"}"), None, None)])],
                vec![MockUpdate::with_polarity(TestStruct::new(true, 0, None), true), MockUpdate::with_polarity(TestStruct::new(false, 5, None), true)],
                Vec::new()
            ),
            TestCase::new(
                true,
                JsonParserConfig {
                    update_format: JsonUpdateFormat::Raw,
                    json_flavor: JsonFlavor::Default,
                    array: true,
                },
                vec![ (r#"[{"b": true, "i": 0}]"#.to_string(), Vec::new())
                    , (r#"[{"b": false, "i": 5},{"b": false}]"#.to_string(), vec![ParseError::new("failed to deserialize JSON record: missing field `i` at line 1 column 12".to_string(), Some(3), None, Some("{\"b\": false}"), None, None)])
                    , (r#"[{"b": false, "i": 5},{"b": 20, "i": 10}]"#.to_string(), vec![ParseError::new("failed to deserialize JSON record: error parsing field 'b': invalid type: integer `20`, expected a boolean at line 1 column 8".to_string(), Some(5), Some("b".to_string()), Some("{\"b\": 20, \"i\": 10}"), None, None)])
                ],
                vec![MockUpdate::with_polarity(TestStruct::new(true, 0, None), true)],
                Vec::new()
            ),
            TestCase::new(
                true,
                JsonParserConfig {
                    update_format: JsonUpdateFormat::Raw,
                    json_flavor: JsonFlavor::Default,
                    array: true,
                },
                vec![ (r#"[[true, 0, "h"]]"#.to_string(), Vec::new())
                    , (r#"[{"b": false, "i": 5},[false]]"#.to_string(), vec![ParseError::new("failed to deserialize JSON record: invalid length 1, expected 3 columns at line 1 column 7".to_string(), Some(3), None, Some("[false]"), None, None)])],
                vec![MockUpdate::with_polarity(TestStruct::new(true, 0, Some("h")), true)],
                Vec::new()
            ),
            // raw: streaming mode; record split across two fragments.
            TestCase::new(
                false,
                JsonParserConfig {
                    update_format: JsonUpdateFormat::Raw,
                    json_flavor: JsonFlavor::Default,
                    array: false,
                },
                vec![ (r#"{"b": true, "i": 0}"#.to_string(), Vec::new())
                    , (r#"{"b": false, "i": 5}
                       {"b": false, "i":"#.to_string(), Vec::new())
                    , (r#"5}"#.to_string(), Vec::new()) ],
                vec![MockUpdate::with_polarity(TestStruct::new(true, 0, None), true), MockUpdate::with_polarity(TestStruct::new(false, 5, None), true), MockUpdate::with_polarity(TestStruct::new(false, 5, None), true)],
                Vec::new()
            ),
            TestCase::new(
                false,
                JsonParserConfig {
                    update_format: JsonUpdateFormat::Raw,
                    json_flavor: JsonFlavor::Default,
                    array: false,
                },
                vec![ (r#"[true, 0, "i"]"#.to_string(), Vec::new())
                    , (r#"{"b": false, "i": 5}
                       [false, "#.to_string(), Vec::new())
                    , (r#"5, "j"]"#.to_string(), Vec::new()) ],
                vec![MockUpdate::with_polarity(TestStruct::new(true, 0, Some("i")), true), MockUpdate::with_polarity(TestStruct::new(false, 5, None), true), MockUpdate::with_polarity(TestStruct::new(false, 5, Some("j")), true)],
                Vec::new()
            ),
            TestCase::new(
                false,
                JsonParserConfig {
                    update_format: JsonUpdateFormat::Raw,
                    json_flavor: JsonFlavor::Default,
                    array: true,
                },
                vec![ (r#"[{"b": true, "i": 0}]"#.to_string(), Vec::new())
                    , (r#"[{"b": false, "i": 5}, {"b": false, "i":"#.to_string(), Vec::new())
                    , (r#"5}]"#.to_string(), Vec::new()) ],
                vec![MockUpdate::with_polarity(TestStruct::new(true, 0, None), true), MockUpdate::with_polarity(TestStruct::new(false, 5, None), true), MockUpdate::with_polarity(TestStruct::new(false, 5, None), true)],
                Vec::new()
            ),
            // raw: streaming mode; record split across several fragments.
            TestCase::new(
                false,
                JsonParserConfig {
                    update_format: JsonUpdateFormat::Raw,
                    json_flavor: JsonFlavor::Default,
                    array: false,
                },
                vec![ (r#"{"b": true, "i": 0}"#.to_string(), Vec::new())
                    , (r#"{"b": false, "i": 5}
                       {"#.to_string(), Vec::new())
                    , (r#""b": false, "i":"#.to_string(), Vec::new())
                    , (r#"5}"#.to_string(), Vec::new()) ],
                vec![MockUpdate::with_polarity(TestStruct::new(true, 0, None), true), MockUpdate::with_polarity(TestStruct::new(false, 5, None), true), MockUpdate::with_polarity(TestStruct::new(false, 5, None), true)],
                Vec::new()
            ),
            TestCase::new(
                false,
                JsonParserConfig {
                    update_format: JsonUpdateFormat::Raw,
                    json_flavor: JsonFlavor::Default,
                    array: false,
                },
                vec![ (r#"{"b": true, "i": 0}"#.to_string(), Vec::new())
                    , (r#"[false, 5, ""]
                       ["#.to_string(), Vec::new())
                    , (r#"false, "#.to_string(), Vec::new())
                    , (r#"5, "k"]"#.to_string(), Vec::new()) ],
                vec![MockUpdate::with_polarity(TestStruct::new(true, 0, None), true), MockUpdate::with_polarity(TestStruct::new(false, 5, Some("")), true), MockUpdate::with_polarity(TestStruct::new(false, 5, Some("k")), true)],
                Vec::new()
            ),

            /* InsertDelete format. */

            // insert_delete: single-record chunk.
            TestCase::new(
                true,
                JsonParserConfig {
                    update_format: JsonUpdateFormat::InsertDelete,
                    json_flavor: JsonFlavor::Default,
                    array: false,
                },
                vec![(r#"{"insert": {"b": true, "i": 0}}"#.to_string(), Vec::new())],
                vec![MockUpdate::with_polarity(TestStruct::new(true, 0, None), true)],
                Vec::new()
            ),
            TestCase::new(
                true,
                JsonParserConfig {
                    update_format: JsonUpdateFormat::InsertDelete,
                    json_flavor: JsonFlavor::Default,
                    array: true,
                },
                vec![(r#"[{"insert": {"b": true, "i": 0}}]"#.to_string(), Vec::new())],
                vec![MockUpdate::with_polarity(TestStruct::new(true, 0, None), true)],
                Vec::new()
            ),
            // Parse update record.
            TestCase::new(
                true,
                JsonParserConfig {
                    update_format: JsonUpdateFormat::InsertDelete,
                    json_flavor: JsonFlavor::Default,
                    array: false,
                },
                vec![(r#"{"update": {"b": true, "i": 0}}"#.to_string(), Vec::new()),
                     (r#"{"update": {"b": true, "i": 0, "s": "foo"}}"#.to_string(), Vec::new()),
                     // Ok to skip non-key fields.
                     // `null` parses as `Some(None)`, i.e., set table field to Null.
                     (r#"{"update": {"i": 0, "s": null}}"#.to_string(), Vec::new()),
                ],
                vec![MockUpdate::Update(TestStructUpd::new(Some(true), 0, None)),
                     MockUpdate::Update(TestStructUpd::new(Some(true), 0, Some(Some("foo")))),
                     MockUpdate::Update(TestStructUpd::new(None, 0, Some(None))),
                ],
                Vec::new()
            ),
            // insert_delete: one chunk, two records.
            TestCase::new(
                true,
                JsonParserConfig {
                    update_format: JsonUpdateFormat::InsertDelete,
                    json_flavor: JsonFlavor::Default,
                    array: false,
                },
                vec![(r#"{"insert": {"b": true, "i": 0}}{"delete": {"b": false, "i": 100, "s": "foo"}}"#.to_string(), Vec::new())],
                vec![MockUpdate::with_polarity(TestStruct::new(true, 0, None), true), MockUpdate::with_polarity(TestStruct::new(false, 100, Some("foo")), false)],
                Vec::new()
            ),
            TestCase::new(
                true,
                JsonParserConfig {
                    update_format: JsonUpdateFormat::InsertDelete,
                    json_flavor: JsonFlavor::Default,
                    array: true,
                },
                vec![(r#"[{"insert": {"b": true, "i": 0}}, {"delete": {"b": false, "i": 100, "s": "foo"}}]"#.to_string(), Vec::new())],
                vec![MockUpdate::with_polarity(TestStruct::new(true, 0, None), true), MockUpdate::with_polarity(TestStruct::new(false, 100, Some("foo")), false)],
                Vec::new()
            ),
            TestCase::new(
                true,
                JsonParserConfig {
                    update_format: JsonUpdateFormat::InsertDelete,
                    json_flavor: JsonFlavor::Default,
                    array: true,
                },
                vec![(r#"[{"insert": [true, 0, "a"]}, {"delete": {"b": false, "i": 100, "s": "foo"}}]"#.to_string(), Vec::new())],
                vec![MockUpdate::with_polarity(TestStruct::new(true, 0, Some("a")), true), MockUpdate::with_polarity(TestStruct::new(false, 100, Some("foo")), false)],
                Vec::new()
            ),
            // insert_delete: two chunks, one record each.
            TestCase::new(
                true,
                JsonParserConfig {
                    update_format: JsonUpdateFormat::InsertDelete,
                    json_flavor: JsonFlavor::Default,
                    array: false,
                },
                vec![ (r#"{"insert": {"b": true, "i": 0}}"#.to_string(), Vec::new())
                    , (r#"{"delete": {"b": false, "i": 100, "s": "foo"}}"#.to_string(), Vec::new())],
                vec![MockUpdate::with_polarity(TestStruct::new(true, 0, None), true), MockUpdate::with_polarity(TestStruct::new(false, 100, Some("foo")), false)],
                Vec::new()
            ),
            // insert_delete: invalid json.
            TestCase::new(
                true,
                JsonParserConfig {
                    update_format: JsonUpdateFormat::InsertDelete,
                    json_flavor: JsonFlavor::Default,
                    array: false,
                },
                vec![ (r#"{"insert": {"b": true, "i": 0}}"#.to_string(), Vec::new())
                    , (r#"{"delete": {"b": false, "i": 100, "s":"#.to_string(), vec![ParseError::text_envelope_error("failed to parse string as a JSON document: EOF while parsing a value at line 1 column 38".to_string(), "{\"delete\": {\"b\": false, \"i\": 100, \"s\":", None)])],
                vec![MockUpdate::with_polarity(TestStruct::new(true, 0, None), true)],
                Vec::new()
            ),
            TestCase::new(
                true,
                JsonParserConfig {
                    update_format: JsonUpdateFormat::InsertDelete,
                    json_flavor: JsonFlavor::Default,
                    array: true,
                },
                vec![ (r#"[{"insert": {"b": true, "i": 0}}]"#.to_string(), Vec::new())
                    , (r#"[{"delete": {"b": false, "i": 100, "s":"#.to_string(), vec![ParseError::text_envelope_error("failed to parse string as a JSON document: EOF while parsing a value at line 1 column 39".to_string(), "[{\"delete\": {\"b\": false, \"i\": 100, \"s\":", None)])],
                vec![MockUpdate::with_polarity(TestStruct::new(true, 0, None), true)],
                Vec::new()
            ),
            // insert_delete: valid json, but data doesn't match type definition.
            TestCase::new(
                true,
                JsonParserConfig {
                    update_format: JsonUpdateFormat::InsertDelete,
                    json_flavor: JsonFlavor::Default,
                    array: false,
                },
                vec![ (r#"{"insert": {"b": true, "i": 0}}"#.to_string(), Vec::new())
                    , (r#"{"insert": {"b": false, "i": 5}}{"delete": {"b": false}}"#.to_string(), vec![ParseError::new("failed to deserialize JSON record: missing field `i` at line 1 column 12".to_string(), Some(3), None, Some("{\"b\": false}"), None, None)])],
                vec![MockUpdate::with_polarity(TestStruct::new(true, 0, None), true), MockUpdate::with_polarity(TestStruct::new(false, 5, None), true)],
                Vec::new()
            ),
            TestCase::new(
                true,
                JsonParserConfig {
                    update_format: JsonUpdateFormat::InsertDelete,
                    json_flavor: JsonFlavor::Default,
                    array: true,
                },
                vec![ (r#"[{"insert": {"b": true, "i": 0}}]"#.to_string(), Vec::new())
                    , (r#"[{"insert": {"b": false, "i": 5}},{"delete": {"b": false}}]"#.to_string(), vec![ParseError::new("failed to deserialize JSON record: missing field `i` at line 1 column 12".to_string(), Some(3), None, Some("{\"b\": false}"), None, None)])],
                vec![MockUpdate::with_polarity(TestStruct::new(true, 0, None), true)],
                Vec::new()
            ),
            TestCase::new(
                true,
                JsonParserConfig {
                    update_format: JsonUpdateFormat::InsertDelete,
                    json_flavor: JsonFlavor::Default,
                    array: true,
                },
                vec![ (r#"[{"insert": {"b": true, "i": 0}}]"#.to_string(), Vec::new())
                    , (r#"[{"insert": {"b": false, "i": 5}},{"delete": {"b": false}}]"#.to_string(), vec![ParseError::new("failed to deserialize JSON record: missing field `i` at line 1 column 12".to_string(), Some(3), None, Some("{\"b\": false}"), None, None)])
                    , (r#"[{"insert": {"b": true, "i": 0}}]"#.to_string(), Vec::new())
                    , (r#"[{"delete": {"b": false}}]"#.to_string(), vec![ParseError::new("failed to deserialize JSON record: missing field `i` at line 1 column 12".to_string(), Some(5), None, Some("{\"b\": false}"), None, None)])
                    , (r#"[{"b": false}]"#.to_string(), vec![ParseError::text_envelope_error("error deserializing string as a JSON array of updates: unknown field `b`, expected one of `table`, `insert`, `delete`, `update` at line 1 column 5".to_string(), "[{\"b\": false}]", Some(Cow::from("Example valid JSON: '[{{\"insert\": {{...}} }}, {{\"delete\": {{...}} }}]'")))])],
                vec![MockUpdate::with_polarity(TestStruct::new(true, 0, None), true), MockUpdate::with_polarity(TestStruct::new(true, 0, None), true)],
                Vec::new()
            ),
            // insert_delete: streaming mode; record split across two fragments.
            TestCase::new(
                false,
                JsonParserConfig {
                    update_format: JsonUpdateFormat::InsertDelete,
                    json_flavor: JsonFlavor::Default,
                    array: false,
                },
                vec![ (r#"{"insert": {"b": true, "i": 0}}"#.to_string(), Vec::new())
                    , (r#"{"insert": {"b": false, "i": 5}}
                       {"delete": {"b": false, "i":"#.to_string(), Vec::new())
                    , (r#"5}}"#.to_string(), Vec::new()) ],
                vec![MockUpdate::with_polarity(TestStruct::new(true, 0, None), true), MockUpdate::with_polarity(TestStruct::new(false, 5, None), true), MockUpdate::with_polarity(TestStruct::new(false, 5, None), false)],
                Vec::new()
            ),
            TestCase::new(
                false,
                JsonParserConfig {
                    update_format: JsonUpdateFormat::InsertDelete,
                    json_flavor: JsonFlavor::Default,
                    array: true,
                },
                vec![ (r#"[{"insert": {"b": true, "i": 0}}]"#.to_string(), Vec::new())
                    , (r#"[{"insert": {"b": false, "i": 5}}, {"delete": {"b": false, "i":"#.to_string(), Vec::new())
                    , (r#"5}}]"#.to_string(), Vec::new()) ],
                vec![MockUpdate::with_polarity(TestStruct::new(true, 0, None), true), MockUpdate::with_polarity(TestStruct::new(false, 5, None), true), MockUpdate::with_polarity(TestStruct::new(false, 5, None), false)],
                Vec::new()
            ),
            // insert_delete: streaming mode; record split across several fragments.
            TestCase::new(
                false,
                JsonParserConfig {
                    update_format: JsonUpdateFormat::InsertDelete,
                    json_flavor: JsonFlavor::Default,
                    array: false,
                },
                vec![ (r#"{"insert": {"b": true, "i": 0}}"#.to_string(), Vec::new())
                    , (r#"{"insert": {"b": false, "i": 5}}
                       {"delete""#.to_string(), Vec::new())
                    , (r#": {"b": false, "i":"#.to_string(), Vec::new())
                    , (r#"5}}"#.to_string(), Vec::new()) ],
                vec![MockUpdate::with_polarity(TestStruct::new(true, 0, None), true), MockUpdate::with_polarity(TestStruct::new(false, 5, None), true), MockUpdate::with_polarity(TestStruct::new(false, 5, None), false)],
                Vec::new()
            ),
            TestCase::new(
                false,
                JsonParserConfig {
                    update_format: JsonUpdateFormat::InsertDelete,
                    json_flavor: JsonFlavor::Default,
                    array: true,
                },
                vec![ (r#"[{"insert": {"b": true, "i": 0}}]"#.to_string(), Vec::new())
                    , (r#"[{"insert": {"b": false, "i": 5}},{"delete""#.to_string(), Vec::new())
                    , (r#": {"b": false, "i":"#.to_string(), Vec::new())
                    , (r#"5}}]"#.to_string(), Vec::new()) ],
                vec![MockUpdate::with_polarity(TestStruct::new(true, 0, None), true), MockUpdate::with_polarity(TestStruct::new(false, 5, None), true), MockUpdate::with_polarity(TestStruct::new(false, 5, None), false)],
                Vec::new()
            ),
            TestCase::new(
                false,
                JsonParserConfig {
                    update_format: JsonUpdateFormat::InsertDelete,
                    json_flavor: JsonFlavor::Default,
                    array: true,
                },
                vec![ (r#"[{"insert": [true, 0, "a"]}]"#.to_string(), Vec::new())
                    , (r#"[{"insert": [false, 5, "b"]},{"delete""#.to_string(), Vec::new())
                    , (r#": [false, "#.to_string(), Vec::new())
                    , (r#"5, "c"]}]"#.to_string(), Vec::new()) ],
                vec![MockUpdate::with_polarity(TestStruct::new(true, 0, Some("a")), true), MockUpdate::with_polarity(TestStruct::new(false, 5, Some("b")), true), MockUpdate::with_polarity(TestStruct::new(false, 5, Some("c")), false)],
                Vec::new()
            ),

            /* Debezium format */

            // debezium: single-record chunk.
            TestCase::new(
                true,
                JsonParserConfig {
                    update_format: JsonUpdateFormat::Debezium,
                    json_flavor: JsonFlavor::Default,
                    array: false,
                },
                vec![(r#"{"payload": {"op": "c", "after": {"b": true, "i": 0}}}"#.to_string(), Vec::new())],
                vec![MockUpdate::with_polarity(TestStruct::new(true, 0, None), true)],
                Vec::new()
            ),
            // debezium: "u" record.
            TestCase::new(
                true,
                JsonParserConfig {
                    update_format: JsonUpdateFormat::Debezium,
                    json_flavor: JsonFlavor::Default,
                    array: false,
                },
                vec![(r#"{"payload": {"op": "u", "before": {"b": true, "i": 123}, "after": {"b": true, "i": 0}}}"#.to_string(), Vec::new())],
                vec![MockUpdate::with_polarity(TestStruct::new(true, 123, None), false), MockUpdate::with_polarity(TestStruct::new(true, 0, None), true)],
                Vec::new()
            ),
            TestCase::new(
                true,
                JsonParserConfig {
                    update_format: JsonUpdateFormat::Debezium,
                    json_flavor: JsonFlavor::Default,
                    array: false,
                },
                vec![(r#"{"payload": {"op": "u", "before": [true, 123, "abc"], "after": [true, 0, "def"]}}"#.to_string(), Vec::new())],
                vec![MockUpdate::with_polarity(TestStruct::new(true, 123, Some("abc")), false), MockUpdate::with_polarity(TestStruct::new(true, 0, Some("def")), true)],
                Vec::new()
            ),
            // debezium: one chunk, two records.
            TestCase::new(
                true,
                JsonParserConfig {
                    update_format: JsonUpdateFormat::Debezium,
                    json_flavor: JsonFlavor::Default,
                    array: false,
                },
                vec![(r#"{"payload": {"op": "c", "after": {"b": true, "i": 0}}}{"payload": {"op": "d", "before": {"b": false, "i": 100, "s": "foo"}}}"#.to_string(), Vec::new())],
                vec![MockUpdate::with_polarity(TestStruct::new(true, 0, None), true), MockUpdate::with_polarity(TestStruct::new(false, 100, Some("foo")), false)],
                Vec::new()
            ),
            // debezium: two chunks, one record each.
            TestCase::new(
                true,
                JsonParserConfig {
                    update_format: JsonUpdateFormat::Debezium,
                    json_flavor: JsonFlavor::Default,
                    array: false,
                },
                vec![ (r#"{"payload": {"op": "c", "after": {"b": true, "i": 0}}}"#.to_string(), Vec::new())
                    , (r#"{"payload": {"op": "d", "before": {"b": false, "i": 100, "s": "foo"}}}"#.to_string(), Vec::new())],
                vec![MockUpdate::with_polarity(TestStruct::new(true, 0, None), true), MockUpdate::with_polarity(TestStruct::new(false, 100, Some("foo")), false)],
                Vec::new()
            ),
            // debezium: invalid json.
            TestCase::new(
                true,
                JsonParserConfig {
                    update_format: JsonUpdateFormat::Debezium,
                    json_flavor: JsonFlavor::Default,
                    array: false,
                },
                vec![ (r#"{"payload": {"op": "c", "after": {"b": true, "i": 0}}}"#.to_string(), Vec::new())
                    , (r#"{"payload": {"op": "d", "before": {"b": false, "i": 100, "s":"#.to_string(), vec![ParseError::text_envelope_error("failed to parse string as a JSON document: EOF while parsing a value at line 1 column 61".to_string(), "{\"payload\": {\"op\": \"d\", \"before\": {\"b\": false, \"i\": 100, \"s\":", None)])],
                vec![MockUpdate::with_polarity(TestStruct::new(true, 0, None), true)],
                Vec::new()
            ),
            // debezium: valid json, but data doesn't match type definition.
            TestCase::new(
                true,
                JsonParserConfig {
                    update_format: JsonUpdateFormat::Debezium,
                    json_flavor: JsonFlavor::Default,
                    array: false,
                },
                vec![ (r#"{"payload": {"op": "c", "after": {"b": true, "i": 0}}}"#.to_string(), Vec::new())
                    , (r#"{"payload": {"op": "c", "after": {"b": false, "i": 5}}}{"payload": {"op": "d", "before": {"b": false}}}"#.to_string(), vec![ParseError::new("failed to deserialize JSON record: missing field `i` at line 1 column 12".to_string(), Some(3), None, Some("{\"b\": false}"), None, None)])],
                vec![MockUpdate::with_polarity(TestStruct::new(true, 0, None), true), MockUpdate::with_polarity(TestStruct::new(false, 5, None), true)],
                Vec::new()
            ),
            // debezium: streaming mode; record split across two fragments.
            TestCase::new(
                false,
                JsonParserConfig {
                    update_format: JsonUpdateFormat::Debezium,
                    json_flavor: JsonFlavor::Default,
                    array: false,
                },
                vec![ (r#"{"payload": {"op": "c", "after": {"b": true, "i": 0}}}"#.to_string(), Vec::new())
                    , (r#"{"payload": {"op": "c", "after": {"b": false, "i": 5}}}
                       {"payload": {"op": "d", "before": {"b": false, "i":"#.to_string(), Vec::new())
                    , (r#"5}}}"#.to_string(), Vec::new()) ],
                vec![MockUpdate::with_polarity(TestStruct::new(true, 0, None), true), MockUpdate::with_polarity(TestStruct::new(false, 5, None), true), MockUpdate::with_polarity(TestStruct::new(false, 5, None), false)],
                Vec::new()
            ),
            // debezium: streaming mode; record split across several fragments.
            TestCase::new(
                false,
                JsonParserConfig {
                    update_format: JsonUpdateFormat::Debezium,
                    json_flavor: JsonFlavor::Default,
                    array: false,
                },
                vec![ (r#"{"payload": {"op": "c", "after": {"b": true, "i": 0}}}"#.to_string(), Vec::new())
                    , (r#"{"payload": {"op": "c", "after": {"b": false, "i": 5}}}
                       {"payload": {"op": "d", "before""#.to_string(), Vec::new())
                    , (r#""#.to_string(), Vec::new())
                    , (r#": {"b": false, "i":5}}}"#.to_string(), Vec::new()) ],
                vec![MockUpdate::with_polarity(TestStruct::new(true, 0, None), true), MockUpdate::with_polarity(TestStruct::new(false, 5, None), true), MockUpdate::with_polarity(TestStruct::new(false, 5, None), false)],
                Vec::new()
            ),
        ];

        run_test_cases(test_cases);
    }
}
