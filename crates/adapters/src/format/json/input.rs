//! JSON format parser.

use crate::{
    format::{InputFormat, Parser, RecordFormat},
    util::split_on_newline,
    DeCollectionHandle,
};
use actix_web::HttpRequest;
use anyhow::{anyhow, Result as AnyResult};
use erased_serde::{Deserializer as ErasedDeserializer, Serialize as ErasedSerialize};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use serde_urlencoded::Deserializer as UrlDeserializer;
use std::{borrow::Cow, mem::take};
use utoipa::ToSchema;

/// JSON format parser.
pub struct JsonInputFormat;

/// Supported JSON data change event formats.
///
/// Each element in a JSON-formatted input stream specifies
/// an update to one or more records in an input table.  We support
/// several different ways to represent such updates.
#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq)]
pub enum JsonUpdateFormat {
    /// Insert/delete format.
    ///
    /// Each element in the input stream consists of an "insert" or "delete"
    /// command and a record to be inserted to or deleted from the input table.
    ///
    /// # Example
    ///
    /// ```json
    /// {"insert": {"column1": "hello, world!", "column2": 100}}
    /// ```
    #[serde(rename = "insert_delete")]
    InsertDelete,
    #[serde(rename = "weighted")]
    Weighted,

    /// Simplified Debezium CDC format.
    ///
    /// We support a simplified version of the Debezium CDC format.  All fields
    /// except `payload` are ignored.
    ///
    /// # Example
    ///
    /// ```json
    /// {"payload": {"op": "u", "before": {"b": true, "i": 123}, "after": {"b": true, "i": 0}}}
    /// ```
    #[serde(rename = "debezium")]
    Debezium,

    /// Raw input format.
    ///
    /// This format is suitable for insert-only streams (no deletions).
    /// Each element in the input stream contains a record without any
    /// additional envelope that gets inserted in the input table.
    #[serde(rename = "raw")]
    Raw,
}

impl Default for JsonUpdateFormat {
    fn default() -> Self {
        Self::Raw
    }
}

/// JSON parser configuration.
///
/// Describes the shape of an input JSON stream.
///
/// # Examples
///
/// A configuration with `update_format="raw"` and `array=false`
/// is used to parse a stream of JSON objects without any envelope
/// that get inserted in the input table.
///
/// ```json
/// {"b": false, "i": 100, "s": "foo"}
/// {"b": true, "i": 5, "s": "bar"}
/// ```
///
/// A configuration with `update_format="insert_delete"` and
/// `array=false` is used to parse a stream of JSON data change events
/// in the insert/delete format:
///
/// ```json
/// {"delete": {"b": false, "i": 15, "s": ""}}
/// {"insert": {"b": false, "i": 100, "s": "foo"}}
/// ```
///
/// A configuration with `update_format="insert_delete"` and
/// `array=true` is used to parse a stream of JSON arrays
/// where each array contains multiple data change events in
/// the insert/delete format.
///
/// ```json
/// [{"insert": {"b": true, "i": 0}}, {"delete": {"b": false, "i": 100, "s": "foo"}}]
/// ```
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct JsonParserConfig {
    // We only support one record format at the moment.
    #[doc(hidden)]
    #[serde(skip)]
    #[allow(dead_code)]
    record_format: RecordFormat,
    /// JSON update format.
    #[serde(default)]
    update_format: JsonUpdateFormat,

    /// Set to `true` if updates in this stream are packaged into JSON arrays.
    ///
    /// # Example
    ///
    /// ```json
    /// [{"b": true, "i": 0},{"b": false, "i": 100, "s": "foo"}]
    /// ```
    #[serde(default)]
    array: bool,
}

impl InputFormat for JsonInputFormat {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("json")
    }

    fn new_parser(
        &self,
        input_stream: &dyn DeCollectionHandle,
        config: &mut dyn ErasedDeserializer,
    ) -> AnyResult<Box<dyn Parser>> {
        let config = JsonParserConfig::deserialize(config)?;
        Ok(Box::new(JsonParser::new(input_stream, config)) as Box<dyn Parser>)
    }

    fn config_from_http_request(
        &self,
        request: &HttpRequest,
    ) -> AnyResult<Box<dyn ErasedSerialize>> {
        Ok(Box::new(JsonParserConfig::deserialize(
            UrlDeserializer::new(form_urlencoded::parse(request.query_string().as_bytes())),
        )?))
    }
}

/// Debezium CDC operation.
///
/// A record in a Debezium CDC stream contains an `op` field, which specifies
/// one of create ("c"), delete ("d") or update ("u") operations.
#[derive(Debug, Deserialize)]
pub enum DebeziumOp {
    #[serde(rename = "c")]
    Create,
    #[serde(rename = "d")]
    Delete,
    #[serde(rename = "u")]
    Update,
}

/// Debezium CDC source specification describes the origin of the record,
/// including the name of the table the record belongs to.
#[derive(Debug, Deserialize)]
pub struct DebeziumSource {
    #[allow(dead_code)]
    table: String,
}

/// A Debezium data change event.
///
/// Only the `payload` field is currently supported; other fields are ignored.
#[derive(Debug, Deserialize)]
pub struct DebeziumUpdate<'a> {
    #[serde(borrow)]
    payload: DebeziumPayload<'a>,
}

/// Schema of the `payload` field of a Debezium data change event.
#[derive(Debug, Deserialize)]
pub struct DebeziumPayload<'a> {
    #[allow(dead_code)]
    source: Option<DebeziumSource>,
    #[allow(dead_code)]
    op: DebeziumOp,
    /// When present and not `null`, this field specifies a record to be deleted from the table.
    #[serde(borrow)]
    before: Option<&'a RawValue>,
    /// When present and not `null`, this field specifies a record to be inserted to the table.
    #[serde(borrow)]
    after: Option<&'a RawValue>,
}

/// A data change event in the insert/delete format.
#[derive(Debug, Deserialize)]
pub struct InsDelUpdate<'a> {
    // This field is currently ignored.  We will add support for it in the future.
    #[doc(hidden)]
    #[allow(dead_code)]
    table: Option<String>,
    /// When present and not `null`, this field specifies a record to be inserted to the table.
    #[serde(borrow)]
    insert: Option<&'a RawValue>,
    /// When present and not `null`, this field specifies a record to be deleted from the table.
    #[serde(borrow)]
    delete: Option<&'a RawValue>,
}

// TODO: implement support for this format.
/// A data change event in the weighted update format.
#[doc(hidden)]
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct WeightedUpdate<'a> {
    table: Option<String>,
    weight: i64,
    #[serde(borrow)]
    data: &'a RawValue,
}

struct JsonParser {
    /// Input handle to push parsed data to.
    input_stream: Box<dyn DeCollectionHandle>,
    config: JsonParserConfig,
    leftover: Vec<u8>,
}

impl JsonParser {
    fn new(input_stream: &dyn DeCollectionHandle, config: JsonParserConfig) -> Self {
        Self {
            input_stream: input_stream.fork(),
            config,
            leftover: Vec::new(),
        }
    }

    fn apply_debezium_update(&mut self, update: DebeziumUpdate) -> AnyResult<usize> {
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

        if let Some(before) = &update.payload.before {
            let mut deserializer = serde_json::Deserializer::from_str(before.get());
            let mut deserializer = <dyn ErasedDeserializer>::erase(&mut deserializer);
            self.input_stream
                .delete(&mut deserializer)
                .map_err(|e| anyhow!("failed to deserialize JSON record '{before}': {e}"))?;
            updates += 1;
        };

        if let Some(after) = &update.payload.after {
            let mut deserializer = serde_json::Deserializer::from_str(after.get());
            let mut deserializer = <dyn ErasedDeserializer>::erase(&mut deserializer);
            self.input_stream
                .insert(&mut deserializer)
                .map_err(|e| anyhow!("failed to deserialize JSON record '{after}': {e}"))?;
            updates += 1;
        };

        Ok(updates)
    }

    fn apply_insdel_update(&mut self, update: InsDelUpdate) -> AnyResult<usize> {
        // TODO: validate table name.
        // We currently allow a JSON connector to feed data to a single table.
        // The name of the table may or may not match table name in the CDC
        // stream.  In the future we will allow demultiplexing a JSON stream
        // to multiple tables.  Connector config will specify available tables.
        /*if let Some(table) = &self.paylolad.table {
            check that table name matches??
        };*/

        let mut updates = 0;

        if let Some(val) = update.insert {
            let mut deserializer = serde_json::Deserializer::from_str(val.get());
            let mut deserializer = <dyn ErasedDeserializer>::erase(&mut deserializer);
            self.input_stream
                .insert(&mut deserializer)
                .map_err(|e| anyhow!("failed to deserialize JSON record '{val}': {e}"))?;
            updates += 1;
        }

        if let Some(val) = update.delete {
            let mut deserializer = serde_json::Deserializer::from_str(val.get());
            let mut deserializer = <dyn ErasedDeserializer>::erase(&mut deserializer);
            self.input_stream
                .delete(&mut deserializer)
                .map_err(|e| anyhow!("failed to deserialize JSON record '{val}': {e}"))?;
            updates += 1;
        }

        Ok(updates)
    }

    fn apply_weighted_update(&mut self, _update: WeightedUpdate) -> AnyResult<usize> {
        todo!()
    }

    fn apply_raw_update(&mut self, update: &RawValue) -> AnyResult<usize> {
        let mut deserializer = serde_json::Deserializer::from_str(update.get());
        let mut deserializer = <dyn ErasedDeserializer>::erase(&mut deserializer);
        self.input_stream
            .insert(&mut deserializer)
            .map_err(|e| anyhow!("failed to deserialize JSON record '{update}': {e}"))?;
        Ok(1)
    }

    fn input_from_slice(&mut self, bytes: &[u8]) -> AnyResult<usize> {
        let mut num_updates = 0;

        match self.config.update_format {
            JsonUpdateFormat::InsertDelete => {
                if self.config.array {
                    for updates in
                        serde_json::Deserializer::from_slice(bytes).into_iter::<Vec<InsDelUpdate>>()
                    {
                        let updates = updates.map_err(|e| {
                            let json_str = String::from_utf8_lossy(bytes);
                            anyhow!("error deserializing string as a JSON array of updates: {e}\nInvalid JSON: {json_str}\nExample valid JSON: '[{{\"insert\": {{...}} }}, {{\"delete\": {{...}} }}]'")
                        })?;
                        for update in updates.into_iter() {
                            num_updates += self.apply_insdel_update(update)?;
                        }
                    }
                } else {
                    for update in
                        serde_json::Deserializer::from_slice(bytes).into_iter::<InsDelUpdate>()
                    {
                        // println!("update: {update:?}");
                        let update = update.map_err(|e| {
                            let json_str = String::from_utf8_lossy(bytes);
                            anyhow!("error deserializing string as a single-row update: {e}\nInvalid JSON: {json_str}\nExample valid JSON: '{{\"insert\": {{...}} }}'")
                        })?;
                        num_updates += self.apply_insdel_update(update)?;
                    }
                }
            }
            JsonUpdateFormat::Debezium => {
                if self.config.array {
                    for updates in serde_json::Deserializer::from_slice(bytes)
                        .into_iter::<Vec<DebeziumUpdate>>()
                    {
                        let updates = updates.map_err(|e| {
                            let json_str = String::from_utf8_lossy(bytes);
                            anyhow!("error deserializing string as a JSON array of Debezium CDC records: {e}\nInvalid JSON: {json_str}\nExample valid JSON: '[{{\"payload\": {{\"op\": \"u\", \"before\": {{...}}, \"after\": {{...}} }} }}]'")
                        })?;
                        for update in updates.into_iter() {
                            num_updates += self.apply_debezium_update(update)?;
                        }
                    }
                } else {
                    for update in
                        serde_json::Deserializer::from_slice(bytes).into_iter::<DebeziumUpdate>()
                    {
                        let update = update.map_err(|e| {
                            let json_str = String::from_utf8_lossy(bytes);
                            anyhow!("error deserializing string as a Debezium CDC record: {e}\nInvalid JSON: {json_str}\nExample valid JSON: '{{\"payload\": {{\"op\": \"u\", \"before\": {{...}}, \"after\": {{...}} }} }}'")
                        })?;
                        num_updates += self.apply_debezium_update(update)?;
                    }
                }
            }
            JsonUpdateFormat::Weighted => {
                if self.config.array {
                    for updates in serde_json::Deserializer::from_slice(bytes)
                        .into_iter::<Vec<WeightedUpdate>>()
                    {
                        let updates = updates.map_err(|e| {
                            let json_str = String::from_utf8_lossy(bytes);
                            anyhow!("error deserializing string as a JSON array of weighted records: {e}\nInvalid JSON: {json_str}\nExample valid JSON: '[{{\"weight\": 1, \"data\": {{...}} }}, {{\"weight\": -1, \"data\": {{...}} }}]'")
                        })?;
                        for update in updates.into_iter() {
                            num_updates += self.apply_weighted_update(update)?;
                        }
                    }
                } else {
                    for update in
                        serde_json::Deserializer::from_slice(bytes).into_iter::<WeightedUpdate>()
                    {
                        let update = update.map_err(|e| {
                            let json_str = String::from_utf8_lossy(bytes);
                            anyhow!("error deserializing string as a weighted record: {e}\nInvalid JSON: {json_str}\nExample valid JSON: '{{\"weight\": 1, \"data\": {{...}} }}'")
                        })?;
                        num_updates += self.apply_weighted_update(update)?;
                    }
                }
            }
            JsonUpdateFormat::Raw => {
                if self.config.array {
                    for updates in
                        serde_json::Deserializer::from_slice(bytes).into_iter::<Vec<&RawValue>>()
                    {
                        let updates = updates.map_err(|e| {
                            let json_str = String::from_utf8_lossy(bytes);
                            anyhow!("error deserializing string as a JSON array: {e}\nInvalid JSON: {json_str}'")
                        })?;
                        for update in updates.into_iter() {
                            num_updates += self.apply_raw_update(update)?;
                        }
                    }
                } else {
                    for update in
                        serde_json::Deserializer::from_slice(bytes).into_iter::<&RawValue>()
                    {
                        // println!("update: {update:?}");
                        let update = update.map_err(|e| {
                            let json_str = String::from_utf8_lossy(bytes);
                            anyhow!("error parsing JSON string: {e}\nInvalid JSON: {json_str}")
                        })?;
                        num_updates += self.apply_raw_update(update)?;
                    }
                }
            }
        }
        Ok(num_updates)
    }
}

impl Parser for JsonParser {
    fn input_fragment(&mut self, data: &[u8]) -> AnyResult<usize> {
        // println!("input_fragment {}", std::str::from_utf8(data).unwrap());
        let leftover = split_on_newline(data);

        if leftover == 0 {
            // `data` doesn't contain a new-line character; append it to
            // the `leftover` buffer so it gets processed with the next input
            // buffer.
            self.leftover.extend_from_slice(data);
            Ok(0)
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

    fn input_chunk(&mut self, data: &[u8]) -> AnyResult<usize> {
        self.input_from_slice(data)
    }

    fn eoi(&mut self) -> AnyResult<usize> {
        /*println!(
            "eoi: leftover: {}",
            std::str::from_utf8(&self.leftover).unwrap()
        );*/
        if self.leftover.is_empty() {
            return Ok(0);
        }

        // Try to interpret the leftover chunk as a complete JSON.
        let leftover = take(&mut self.leftover);
        let res = self.input_from_slice(leftover.as_slice());
        res
    }

    fn flush(&mut self) {
        self.input_stream.flush();
    }

    fn clear(&mut self) {
        self.input_stream.clear_buffer();
    }

    fn fork(&self) -> Box<dyn Parser> {
        Box::new(Self::new(&*self.input_stream, self.config.clone()))
    }
}

#[cfg(test)]
mod test {
    use crate::{
        format::{JsonParserConfig, JsonUpdateFormat, RecordFormat},
        test::mock_parser_pipeline,
        transport::InputConsumer,
        FormatConfig,
    };
    use serde::Deserialize;
    use std::{borrow::Cow, fmt::Debug};

    #[derive(Deserialize, PartialEq, Debug, Eq)]
    struct TestStruct {
        b: bool,
        i: i32,
        #[serde(default)]
        s: String,
    }

    // TODO: tests for RecordFormat::Raw.

    // Used to test RecordFormat::Raw.
    /*#[derive(Deserialize, PartialEq, Debug, Eq)]
    struct OneColumn {
        json: JsonValue,
    }*/

    impl TestStruct {
        fn new(b: bool, i: i32, s: &str) -> Self {
            Self {
                b,
                i,
                s: s.to_string(),
            }
        }
    }

    #[derive(Debug)]
    struct TestCase<T> {
        chunks: bool,
        config: JsonParserConfig,
        /// Input data, expected result.
        input_batches: Vec<(String, Result<(), String>)>,
        final_result: Result<(), String>,
        /// Expected contents at the end of the test.
        expected_output: Vec<(T, bool)>,
    }

    fn run_test_cases<T>(test_cases: Vec<TestCase<T>>)
    where
        T: Debug + Eq + for<'de> Deserialize<'de> + Send + 'static,
    {
        for test in test_cases {
            println!("test: {test:?}");
            let format_config = FormatConfig {
                name: Cow::from("json"),
                config: serde_yaml::to_value(test.config).unwrap(),
            };

            let (mut consumer, outputs) = mock_parser_pipeline(&format_config).unwrap();
            consumer.on_error(Some(Box::new(|_| {})));
            for (json, expected_result) in test.input_batches {
                let res = if test.chunks {
                    consumer.input_chunk(json.as_bytes())
                } else {
                    consumer.input_fragment(json.as_bytes())
                };
                assert_eq!(&res.map_err(|e| e.to_string()), &expected_result);
            }
            let res = consumer.eoi();
            assert_eq!(&res.map_err(|e| e.to_string()), &test.final_result);
            assert_eq!(&test.expected_output, &outputs.state().flushed);
        }
    }

    impl<T> TestCase<T> {
        fn new(
            chunks: bool,
            config: JsonParserConfig,
            input_batches: Vec<(String, Result<(), String>)>,
            expected_output: Vec<(T, bool)>,
            final_result: Result<(), String>,
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
        let test_cases: Vec<TestCase<_>> = vec! [
            /* Raw update format */

            // raw: single-record chunk.
            TestCase::new(
                true,
                JsonParserConfig {
                    record_format: RecordFormat::Map,
                    update_format: JsonUpdateFormat::Raw,
                    array: false,
                },
                vec![(r#"{"b": true, "i": 0}"#.to_string(), Ok(()))],
                vec![(TestStruct::new(true, 0, ""), true)],
                Ok(())
            ),
            TestCase::new(
                true,
                JsonParserConfig {
                    record_format: RecordFormat::Map,
                    update_format: JsonUpdateFormat::Raw,
                    array: false,
                },
                vec![(r#"[true, 0, "a"]"#.to_string(), Ok(()))],
                vec![(TestStruct::new(true, 0, "a"), true)],
                Ok(())
            ),
            TestCase::new(
                true,
                JsonParserConfig {
                    record_format: RecordFormat::Map,
                    update_format: JsonUpdateFormat::Raw,
                    array: true,
                },
                vec![(r#"[{"b": true, "i": 0}]"#.to_string(), Ok(()))],
                vec![(TestStruct::new(true, 0, ""), true)],
                Ok(())
            ),
            TestCase::new(
                true,
                JsonParserConfig {
                    record_format: RecordFormat::Map,
                    update_format: JsonUpdateFormat::Raw,
                    array: true,
                },
                vec![(r#"[[true, 0, "b"]]"#.to_string(), Ok(()))],
                vec![(TestStruct::new(true, 0, "b"), true)],
                Ok(())
            ),
            // raw: one chunk, two records.
            TestCase::new(
                true,
                JsonParserConfig {
                    record_format: RecordFormat::Map,
                    update_format: JsonUpdateFormat::Raw,
                    array: false,
                },
                vec![(r#"{"b": true, "i": 0}{"b": false, "i": 100, "s": "foo"}"#.to_string(), Ok(()))],
                vec![(TestStruct::new(true, 0, ""), true), (TestStruct::new(false, 100, "foo"), true)],
                Ok(())
            ),
            TestCase::new(
                true,
                JsonParserConfig {
                    record_format: RecordFormat::Map,
                    update_format: JsonUpdateFormat::Raw,
                    array: false,
                },
                vec![(r#"[true, 0, "c"][false, 100, "foo"]"#.to_string(), Ok(()))],
                vec![(TestStruct::new(true, 0, "c"), true), (TestStruct::new(false, 100, "foo"), true)],
                Ok(())
            ),
            TestCase::new(
                true,
                JsonParserConfig {
                    record_format: RecordFormat::Map,
                    update_format: JsonUpdateFormat::Raw,
                    array: true,
                },
                vec![(r#"[{"b": true, "i": 0},{"b": false, "i": 100, "s": "foo"}]"#.to_string(), Ok(()))],
                vec![(TestStruct::new(true, 0, ""), true), (TestStruct::new(false, 100, "foo"), true)],
                Ok(())
            ),
            TestCase::new(
                true,
                JsonParserConfig {
                    record_format: RecordFormat::Map,
                    update_format: JsonUpdateFormat::Raw,
                    array: true,
                },
                vec![(r#"[[true, 0, "d"],[false, 100, "foo"]]"#.to_string(), Ok(()))],
                vec![(TestStruct::new(true, 0, "d"), true), (TestStruct::new(false, 100, "foo"), true)],
                Ok(())
            ),
            // raw: two chunks, one record each.
            TestCase::new(
                true,
                JsonParserConfig {
                    record_format: RecordFormat::Map,
                    update_format: JsonUpdateFormat::Raw,
                    array: false,
                },
                vec![ (r#"{"b": true, "i": 0}"#.to_string(), Ok(()))
                    , (r#"{"b": false, "i": 100, "s": "foo"}"#.to_string(), Ok(()))],
                vec![(TestStruct::new(true, 0, ""), true), (TestStruct::new(false, 100, "foo"), true)],
                Ok(())
            ),
            TestCase::new(
                true,
                JsonParserConfig {
                    record_format: RecordFormat::Map,
                    update_format: JsonUpdateFormat::Raw,
                    array: false,
                },
                vec![ (r#"[true, 0, "e"]"#.to_string(), Ok(()))
                    , (r#"[false, 100, "foo"]"#.to_string(), Ok(()))],
                vec![(TestStruct::new(true, 0, "e"), true), (TestStruct::new(false, 100, "foo"), true)],
                Ok(())
            ),
            TestCase::new(
                true,
                JsonParserConfig {
                    record_format: RecordFormat::Map,
                    update_format: JsonUpdateFormat::Raw,
                    array: true,
                },
                vec![ (r#"[{"b": true, "i": 0}]"#.to_string(), Ok(()))
                    , (r#"[{"b": false, "i": 100, "s": "foo"}]"#.to_string(), Ok(()))],
                vec![(TestStruct::new(true, 0, ""), true), (TestStruct::new(false, 100, "foo"), true)],
                Ok(())
            ),
            TestCase::new(
                true,
                JsonParserConfig {
                    record_format: RecordFormat::Map,
                    update_format: JsonUpdateFormat::Raw,
                    array: true,
                },
                vec![ (r#"[[true, 0, "e"]]"#.to_string(), Ok(()))
                    , (r#"[[false, 100, "foo"]]"#.to_string(), Ok(()))],
                vec![(TestStruct::new(true, 0, "e"), true), (TestStruct::new(false, 100, "foo"), true)],
                Ok(())
            ),
            // raw: invalid json.
            TestCase::new(
                true,
                JsonParserConfig {
                    record_format: RecordFormat::Map,
                    update_format: JsonUpdateFormat::Raw,
                    array: false,
                },
                vec![ (r#"{"b": true, "i": 0}"#.to_string(), Ok(()))
                    , (r#"{"b": false, "i": 100, "s":"#.to_string(), Err("error parsing JSON string: EOF while parsing a value at line 1 column 27\nInvalid JSON: {\"b\": false, \"i\": 100, \"s\":".to_string()))],
                vec![(TestStruct::new(true, 0, ""), true)],
                Ok(())
            ),
            TestCase::new(
                true,
                JsonParserConfig {
                    record_format: RecordFormat::Map,
                    update_format: JsonUpdateFormat::Raw,
                    array: false,
                },
                vec![ (r#"[true, 0, "f"]"#.to_string(), Ok(()))
                    , (r#"[false, 100, "#.to_string(), Err("error parsing JSON string: EOF while parsing a value at line 1 column 13\nInvalid JSON: [false, 100, ".to_string()))],
                vec![(TestStruct::new(true, 0, "f"), true)],
                Ok(())
            ),
            TestCase::new(
                true,
                JsonParserConfig {
                    record_format: RecordFormat::Map,
                    update_format: JsonUpdateFormat::Raw,
                    array: true,
                },
                vec![ (r#"[{"b": true, "i": 0}]"#.to_string(), Ok(()))
                    , (r#"[{"b": false, "i": 100, "s":"#.to_string(), Err("error deserializing string as a JSON array: EOF while parsing a value at line 1 column 28\nInvalid JSON: [{\"b\": false, \"i\": 100, \"s\":'".to_string()))],
                vec![(TestStruct::new(true, 0, ""), true)],
                Ok(())
            ),
            TestCase::new(
                true,
                JsonParserConfig {
                    record_format: RecordFormat::Map,
                    update_format: JsonUpdateFormat::Raw,
                    array: true,
                },
                vec![ (r#"[[true, 0, "g"]]"#.to_string(), Ok(()))
                    , (r#"[[false, 100, "s":"#.to_string(), Err("error deserializing string as a JSON array: expected `,` or `]` at line 1 column 18\nInvalid JSON: [[false, 100, \"s\":'".to_string()))],
                vec![(TestStruct::new(true, 0, "g"), true)],
                Ok(())
            ),
            // raw: valid json, but data doesn't match type definition.
            TestCase::new(
                true,
                JsonParserConfig {
                    record_format: RecordFormat::Map,
                    update_format: JsonUpdateFormat::Raw,
                    array: false,
                },
                vec![ (r#"{"b": true, "i": 0}"#.to_string(), Ok(()))
                    , (r#"{"b": false, "i": 5}{"b": false}"#.to_string(), Err("failed to deserialize JSON record '{\"b\": false}': missing field `i` at line 1 column 12".to_string()))],
                vec![(TestStruct::new(true, 0, ""), true), (TestStruct::new(false, 5, ""), true)],
                Ok(())
            ),
            TestCase::new(
                true,
                JsonParserConfig {
                    record_format: RecordFormat::Map,
                    update_format: JsonUpdateFormat::Raw,
                    array: true,
                },
                vec![ (r#"[{"b": true, "i": 0}]"#.to_string(), Ok(()))
                    , (r#"[{"b": false, "i": 5},{"b": false}]"#.to_string(), Err("failed to deserialize JSON record '{\"b\": false}': missing field `i` at line 1 column 12".to_string()))],
                vec![(TestStruct::new(true, 0, ""), true), (TestStruct::new(false, 5, ""), true)],
                Ok(())
            ),
            TestCase::new(
                true,
                JsonParserConfig {
                    record_format: RecordFormat::Map,
                    update_format: JsonUpdateFormat::Raw,
                    array: true,
                },
                vec![ (r#"[[true, 0, "h"]]"#.to_string(), Ok(()))
                    , (r#"[{"b": false, "i": 5},[false]]"#.to_string(), Err("failed to deserialize JSON record '[false]': invalid length 1, expected struct TestStruct with 3 elements at line 1 column 7".to_string()))],
                vec![(TestStruct::new(true, 0, "h"), true), (TestStruct::new(false, 5, ""), true)],
                Ok(())
            ),
            // raw: streaming mode; record split across two fragments.
            TestCase::new(
                false,
                JsonParserConfig {
                    record_format: RecordFormat::Map,
                    update_format: JsonUpdateFormat::Raw,
                    array: false,
                },
                vec![ (r#"{"b": true, "i": 0}"#.to_string(), Ok(()))
                    , (r#"{"b": false, "i": 5}
                       {"b": false, "i":"#.to_string(), Ok(()))
                    , (r#"5}"#.to_string(), Ok(())) ],
                vec![(TestStruct::new(true, 0, ""), true), (TestStruct::new(false, 5, ""), true), (TestStruct::new(false, 5, ""), true)],
                Ok(())
            ),
            TestCase::new(
                false,
                JsonParserConfig {
                    record_format: RecordFormat::Map,
                    update_format: JsonUpdateFormat::Raw,
                    array: false,
                },
                vec![ (r#"[true, 0, "i"]"#.to_string(), Ok(()))
                    , (r#"{"b": false, "i": 5}
                       [false, "#.to_string(), Ok(()))
                    , (r#"5, "j"]"#.to_string(), Ok(())) ],
                vec![(TestStruct::new(true, 0, "i"), true), (TestStruct::new(false, 5, ""), true), (TestStruct::new(false, 5, "j"), true)],
                Ok(())
            ),
            TestCase::new(
                false,
                JsonParserConfig {
                    record_format: RecordFormat::Map,
                    update_format: JsonUpdateFormat::Raw,
                    array: true,
                },
                vec![ (r#"[{"b": true, "i": 0}]"#.to_string(), Ok(()))
                    , (r#"[{"b": false, "i": 5}, {"b": false, "i":"#.to_string(), Ok(()))
                    , (r#"5}]"#.to_string(), Ok(())) ],
                vec![(TestStruct::new(true, 0, ""), true), (TestStruct::new(false, 5, ""), true), (TestStruct::new(false, 5, ""), true)],
                Ok(())
            ),
            // raw: streaming mode; record split across several fragments.
            TestCase::new(
                false,
                JsonParserConfig {
                    record_format: RecordFormat::Map,
                    update_format: JsonUpdateFormat::Raw,
                    array: false,
                },
                vec![ (r#"{"b": true, "i": 0}"#.to_string(), Ok(()))
                    , (r#"{"b": false, "i": 5}
                       {"#.to_string(), Ok(()))
                    , (r#""b": false, "i":"#.to_string(), Ok(()))
                    , (r#"5}"#.to_string(), Ok(())) ],
                vec![(TestStruct::new(true, 0, ""), true), (TestStruct::new(false, 5, ""), true), (TestStruct::new(false, 5, ""), true)],
                Ok(())
            ),
            TestCase::new(
                false,
                JsonParserConfig {
                    record_format: RecordFormat::Map,
                    update_format: JsonUpdateFormat::Raw,
                    array: false,
                },
                vec![ (r#"{"b": true, "i": 0}"#.to_string(), Ok(()))
                    , (r#"[false, 5, ""]
                       ["#.to_string(), Ok(()))
                    , (r#"false, "#.to_string(), Ok(()))
                    , (r#"5, "k"]"#.to_string(), Ok(())) ],
                vec![(TestStruct::new(true, 0, ""), true), (TestStruct::new(false, 5, ""), true), (TestStruct::new(false, 5, "k"), true)],
                Ok(())
            ),

            /* InsertDelete format. */

            // insert_delete: single-record chunk.
            TestCase::new(
                true,
                JsonParserConfig {
                    record_format: RecordFormat::Map,
                    update_format: JsonUpdateFormat::InsertDelete,
                    array: false,
                },
                vec![(r#"{"insert": {"b": true, "i": 0}}"#.to_string(), Ok(()))],
                vec![(TestStruct::new(true, 0, ""), true)],
                Ok(())
            ),
            TestCase::new(
                true,
                JsonParserConfig {
                    record_format: RecordFormat::Map,
                    update_format: JsonUpdateFormat::InsertDelete,
                    array: true,
                },
                vec![(r#"[{"insert": {"b": true, "i": 0}}]"#.to_string(), Ok(()))],
                vec![(TestStruct::new(true, 0, ""), true)],
                Ok(())
            ),
            // insert_delete: one chunk, two records.
            TestCase::new(
                true,
                JsonParserConfig {
                    record_format: RecordFormat::Map,
                    update_format: JsonUpdateFormat::InsertDelete,
                    array: false,
                },
                vec![(r#"{"insert": {"b": true, "i": 0}}{"delete": {"b": false, "i": 100, "s": "foo"}}"#.to_string(), Ok(()))],
                vec![(TestStruct::new(true, 0, ""), true), (TestStruct::new(false, 100, "foo"), false)],
                Ok(())
            ),
            TestCase::new(
                true,
                JsonParserConfig {
                    record_format: RecordFormat::Map,
                    update_format: JsonUpdateFormat::InsertDelete,
                    array: true,
                },
                vec![(r#"[{"insert": {"b": true, "i": 0}}, {"delete": {"b": false, "i": 100, "s": "foo"}}]"#.to_string(), Ok(()))],
                vec![(TestStruct::new(true, 0, ""), true), (TestStruct::new(false, 100, "foo"), false)],
                Ok(())
            ),
            TestCase::new(
                true,
                JsonParserConfig {
                    record_format: RecordFormat::Map,
                    update_format: JsonUpdateFormat::InsertDelete,
                    array: true,
                },
                vec![(r#"[{"insert": [true, 0, "a"]}, {"delete": {"b": false, "i": 100, "s": "foo"}}]"#.to_string(), Ok(()))],
                vec![(TestStruct::new(true, 0, "a"), true), (TestStruct::new(false, 100, "foo"), false)],
                Ok(())
            ),
            // insert_delete: two chunks, one record each.
            TestCase::new(
                true,
                JsonParserConfig {
                    record_format: RecordFormat::Map,
                    update_format: JsonUpdateFormat::InsertDelete,
                    array: false,
                },
                vec![ (r#"{"insert": {"b": true, "i": 0}}"#.to_string(), Ok(()))
                    , (r#"{"delete": {"b": false, "i": 100, "s": "foo"}}"#.to_string(), Ok(()))],
                vec![(TestStruct::new(true, 0, ""), true), (TestStruct::new(false, 100, "foo"), false)],
                Ok(())
            ),
            // insert_delete: invalid json.
            TestCase::new(
                true,
                JsonParserConfig {
                    record_format: RecordFormat::Map,
                    update_format: JsonUpdateFormat::InsertDelete,
                    array: false,
                },
                vec![ (r#"{"insert": {"b": true, "i": 0}}"#.to_string(), Ok(()))
                    , (r#"{"delete": {"b": false, "i": 100, "s":"#.to_string(), Err("error deserializing string as a single-row update: EOF while parsing a value at line 1 column 38\nInvalid JSON: {\"delete\": {\"b\": false, \"i\": 100, \"s\":\nExample valid JSON: '{\"insert\": {...} }'".to_string()))],
                vec![(TestStruct::new(true, 0, ""), true)],
                Ok(())
            ),
            TestCase::new(
                true,
                JsonParserConfig {
                    record_format: RecordFormat::Map,
                    update_format: JsonUpdateFormat::InsertDelete,
                    array: true,
                },
                vec![ (r#"[{"insert": {"b": true, "i": 0}}]"#.to_string(), Ok(()))
                    , (r#"[{"delete": {"b": false, "i": 100, "s":"#.to_string(), Err("error deserializing string as a JSON array of updates: EOF while parsing a value at line 1 column 39\nInvalid JSON: [{\"delete\": {\"b\": false, \"i\": 100, \"s\":\nExample valid JSON: '[{\"insert\": {...} }, {\"delete\": {...} }]'".to_string()))],
                vec![(TestStruct::new(true, 0, ""), true)],
                Ok(())
            ),
            // insert_delete: valid json, but data doesn't match type definition.
            TestCase::new(
                true,
                JsonParserConfig {
                    record_format: RecordFormat::Map,
                    update_format: JsonUpdateFormat::InsertDelete,
                    array: false,
                },
                vec![ (r#"{"insert": {"b": true, "i": 0}}"#.to_string(), Ok(()))
                    , (r#"{"insert": {"b": false, "i": 5}}{"delete": {"b": false}}"#.to_string(), Err("failed to deserialize JSON record '{\"b\": false}': missing field `i` at line 1 column 12".to_string()))],
                vec![(TestStruct::new(true, 0, ""), true), (TestStruct::new(false, 5, ""), true)],
                Ok(())
            ),
            TestCase::new(
                true,
                JsonParserConfig {
                    record_format: RecordFormat::Map,
                    update_format: JsonUpdateFormat::InsertDelete,
                    array: true,
                },
                vec![ (r#"[{"insert": {"b": true, "i": 0}}]"#.to_string(), Ok(()))
                    , (r#"[{"insert": {"b": false, "i": 5}},{"delete": {"b": false}}]"#.to_string(), Err("failed to deserialize JSON record '{\"b\": false}': missing field `i` at line 1 column 12".to_string()))],
                vec![(TestStruct::new(true, 0, ""), true), (TestStruct::new(false, 5, ""), true)],
                Ok(())
            ),
            // insert_delete: streaming mode; record split across two fragments.
            TestCase::new(
                false,
                JsonParserConfig {
                    record_format: RecordFormat::Map,
                    update_format: JsonUpdateFormat::InsertDelete,
                    array: false,
                },
                vec![ (r#"{"insert": {"b": true, "i": 0}}"#.to_string(), Ok(()))
                    , (r#"{"insert": {"b": false, "i": 5}}
                       {"delete": {"b": false, "i":"#.to_string(), Ok(()))
                    , (r#"5}}"#.to_string(), Ok(())) ],
                vec![(TestStruct::new(true, 0, ""), true), (TestStruct::new(false, 5, ""), true), (TestStruct::new(false, 5, ""), false)],
                Ok(())
            ),
            TestCase::new(
                false,
                JsonParserConfig {
                    record_format: RecordFormat::Map,
                    update_format: JsonUpdateFormat::InsertDelete,
                    array: true,
                },
                vec![ (r#"[{"insert": {"b": true, "i": 0}}]"#.to_string(), Ok(()))
                    , (r#"[{"insert": {"b": false, "i": 5}}, {"delete": {"b": false, "i":"#.to_string(), Ok(()))
                    , (r#"5}}]"#.to_string(), Ok(())) ],
                vec![(TestStruct::new(true, 0, ""), true), (TestStruct::new(false, 5, ""), true), (TestStruct::new(false, 5, ""), false)],
                Ok(())
            ),
            // insert_delete: streaming mode; record split across several fragments.
            TestCase::new(
                false,
                JsonParserConfig {
                    record_format: RecordFormat::Map,
                    update_format: JsonUpdateFormat::InsertDelete,
                    array: false,
                },
                vec![ (r#"{"insert": {"b": true, "i": 0}}"#.to_string(), Ok(()))
                    , (r#"{"insert": {"b": false, "i": 5}}
                       {"delete""#.to_string(), Ok(()))
                    , (r#": {"b": false, "i":"#.to_string(), Ok(()))
                    , (r#"5}}"#.to_string(), Ok(())) ],
                vec![(TestStruct::new(true, 0, ""), true), (TestStruct::new(false, 5, ""), true), (TestStruct::new(false, 5, ""), false)],
                Ok(())
            ),
            TestCase::new(
                false,
                JsonParserConfig {
                    record_format: RecordFormat::Map,
                    update_format: JsonUpdateFormat::InsertDelete,
                    array: true,
                },
                vec![ (r#"[{"insert": {"b": true, "i": 0}}]"#.to_string(), Ok(()))
                    , (r#"[{"insert": {"b": false, "i": 5}},{"delete""#.to_string(), Ok(()))
                    , (r#": {"b": false, "i":"#.to_string(), Ok(()))
                    , (r#"5}}]"#.to_string(), Ok(())) ],
                vec![(TestStruct::new(true, 0, ""), true), (TestStruct::new(false, 5, ""), true), (TestStruct::new(false, 5, ""), false)],
                Ok(())
            ),
            TestCase::new(
                false,
                JsonParserConfig {
                    record_format: RecordFormat::Map,
                    update_format: JsonUpdateFormat::InsertDelete,
                    array: true,
                },
                vec![ (r#"[{"insert": [true, 0, "a"]}]"#.to_string(), Ok(()))
                    , (r#"[{"insert": [false, 5, "b"]},{"delete""#.to_string(), Ok(()))
                    , (r#": [false, "#.to_string(), Ok(()))
                    , (r#"5, "c"]}]"#.to_string(), Ok(())) ],
                vec![(TestStruct::new(true, 0, "a"), true), (TestStruct::new(false, 5, "b"), true), (TestStruct::new(false, 5, "c"), false)],
                Ok(())
            ),

            /* Debezium format */

            // debezium: single-record chunk.
            TestCase::new(
                true,
                JsonParserConfig {
                    record_format: RecordFormat::Map,
                    update_format: JsonUpdateFormat::Debezium,
                    array: false,
                },
                vec![(r#"{"payload": {"op": "c", "after": {"b": true, "i": 0}}}"#.to_string(), Ok(()))],
                vec![(TestStruct::new(true, 0, ""), true)],
                Ok(())
            ),
            // debezium: "u" record.
            TestCase::new(
                true,
                JsonParserConfig {
                    record_format: RecordFormat::Map,
                    update_format: JsonUpdateFormat::Debezium,
                    array: false,
                },
                vec![(r#"{"payload": {"op": "u", "before": {"b": true, "i": 123}, "after": {"b": true, "i": 0}}}"#.to_string(), Ok(()))],
                vec![(TestStruct::new(true, 123, ""), false), (TestStruct::new(true, 0, ""), true)],
                Ok(())
            ),
            TestCase::new(
                true,
                JsonParserConfig {
                    record_format: RecordFormat::Map,
                    update_format: JsonUpdateFormat::Debezium,
                    array: false,
                },
                vec![(r#"{"payload": {"op": "u", "before": [true, 123, "abc"], "after": [true, 0, "def"]}}"#.to_string(), Ok(()))],
                vec![(TestStruct::new(true, 123, "abc"), false), (TestStruct::new(true, 0, "def"), true)],
                Ok(())
            ),
            // debezium: one chunk, two records.
            TestCase::new(
                true,
                JsonParserConfig {
                    record_format: RecordFormat::Map,
                    update_format: JsonUpdateFormat::Debezium,
                    array: false,
                },
                vec![(r#"{"payload": {"op": "c", "after": {"b": true, "i": 0}}}{"payload": {"op": "d", "before": {"b": false, "i": 100, "s": "foo"}}}"#.to_string(), Ok(()))],
                vec![(TestStruct::new(true, 0, ""), true), (TestStruct::new(false, 100, "foo"), false)],
                Ok(())
            ),
            // debezium: two chunks, one record each.
            TestCase::new(
                true,
                JsonParserConfig {
                    record_format: RecordFormat::Map,
                    update_format: JsonUpdateFormat::Debezium,
                    array: false,
                },
                vec![ (r#"{"payload": {"op": "c", "after": {"b": true, "i": 0}}}"#.to_string(), Ok(()))
                    , (r#"{"payload": {"op": "d", "before": {"b": false, "i": 100, "s": "foo"}}}"#.to_string(), Ok(()))],
                vec![(TestStruct::new(true, 0, ""), true), (TestStruct::new(false, 100, "foo"), false)],
                Ok(())
            ),
            // debezium: invalid json.
            TestCase::new(
                true,
                JsonParserConfig {
                    record_format: RecordFormat::Map,
                    update_format: JsonUpdateFormat::Debezium,
                    array: false,
                },
                vec![ (r#"{"payload": {"op": "c", "after": {"b": true, "i": 0}}}"#.to_string(), Ok(()))
                    , (r#"{"payload": {"op": "d", "before": {"b": false, "i": 100, "s":"#.to_string(), Err("error deserializing string as a Debezium CDC record: EOF while parsing a value at line 1 column 61\nInvalid JSON: {\"payload\": {\"op\": \"d\", \"before\": {\"b\": false, \"i\": 100, \"s\":\nExample valid JSON: '{\"payload\": {\"op\": \"u\", \"before\": {...}, \"after\": {...} } }'".to_string()))],
                vec![(TestStruct::new(true, 0, ""), true)],
                Ok(())
            ),
            // debezium: valid json, but data doesn't match type definition.
            TestCase::new(
                true,
                JsonParserConfig {
                    record_format: RecordFormat::Map,
                    update_format: JsonUpdateFormat::Debezium,
                    array: false,
                },
                vec![ (r#"{"payload": {"op": "c", "after": {"b": true, "i": 0}}}"#.to_string(), Ok(()))
                    , (r#"{"payload": {"op": "c", "after": {"b": false, "i": 5}}}{"payload": {"op": "d", "before": {"b": false}}}"#.to_string(), Err("failed to deserialize JSON record '{\"b\": false}': missing field `i` at line 1 column 12".to_string()))],
                vec![(TestStruct::new(true, 0, ""), true), (TestStruct::new(false, 5, ""), true)],
                Ok(())
            ),
            // debezium: streaming mode; record split across two fragments.
            TestCase::new(
                false,
                JsonParserConfig {
                    record_format: RecordFormat::Map,
                    update_format: JsonUpdateFormat::Debezium,
                    array: false,
                },
                vec![ (r#"{"payload": {"op": "c", "after": {"b": true, "i": 0}}}"#.to_string(), Ok(()))
                    , (r#"{"payload": {"op": "c", "after": {"b": false, "i": 5}}}
                       {"payload": {"op": "d", "before": {"b": false, "i":"#.to_string(), Ok(()))
                    , (r#"5}}}"#.to_string(), Ok(())) ],
                vec![(TestStruct::new(true, 0, ""), true), (TestStruct::new(false, 5, ""), true), (TestStruct::new(false, 5, ""), false)],
                Ok(())
            ),
            // debezium: streaming mode; record split across several fragments.
            TestCase::new(
                false,
                JsonParserConfig {
                    record_format: RecordFormat::Map,
                    update_format: JsonUpdateFormat::Debezium,
                    array: false,
                },
                vec![ (r#"{"payload": {"op": "c", "after": {"b": true, "i": 0}}}"#.to_string(), Ok(()))
                    , (r#"{"payload": {"op": "c", "after": {"b": false, "i": 5}}}
                       {"payload": {"op": "d", "before""#.to_string(), Ok(()))
                    , (r#""#.to_string(), Ok(()))
                    , (r#": {"b": false, "i":5}}}"#.to_string(), Ok(())) ],
                vec![(TestStruct::new(true, 0, ""), true), (TestStruct::new(false, 5, ""), true), (TestStruct::new(false, 5, ""), false)],
                Ok(())
            ),
        ];

        run_test_cases(test_cases);
    }
}
