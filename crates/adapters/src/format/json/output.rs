use crate::catalog::SerBatchReader;
use crate::format::json::schema::{build_key_schema, build_value_schema};
use crate::format::{MAX_DUPLICATES, MAX_RECORD_LEN_IN_ERRMSG};
use crate::{
    ControllerError, Encoder, OutputConsumer, OutputFormat,
    catalog::{CursorWithPolarity, RecordFormat, SerCursor},
    util::truncate_ellipse,
};
use anyhow::{Result as AnyResult, bail};
use erased_serde::Serialize as ErasedSerialize;
use feldera_adapterlib::catalog::SerCursorFlattened;
use feldera_types::config::{ConnectorConfig, TransportConfig};
use feldera_types::format::json::{JsonEncoderConfig, JsonFlavor, JsonUpdateFormat};
use feldera_types::program_schema::{Relation, canonical_identifier};
use rand::{Rng, SeedableRng, rngs::StdRng};
use serde::Deserialize;
use serde_json::json;
use serde_urlencoded::Deserializer as UrlDeserializer;
use std::collections::HashSet;
use std::sync::Arc;
use std::{borrow::Cow, io::Write, mem::take};

/// JSON format encoder.
pub struct JsonOutputFormat;

impl OutputFormat for JsonOutputFormat {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("json")
    }

    fn config_from_http_query(
        &self,
        endpoint_name: &str,
        query: &str,
    ) -> Result<Box<dyn ErasedSerialize>, ControllerError> {
        let mut config = JsonEncoderConfig::deserialize(UrlDeserializer::new(
            form_urlencoded::parse(query.as_bytes()),
        ))
        .map_err(|e| ControllerError::encoder_config_parse_error(endpoint_name, &e, query))?;

        // We currently always break output into chunks, which requires encoding
        // JSON data as a valid JSON document (can't use ND-JSON), so we set `array`
        // to `true` for the result to be valid.
        // TODO: When we support raw output mode (no chunks), check whatever http
        // request field we use to choose the mode and only override the `array`
        // flag in the chunked mode.
        config.array = true;
        Ok(Box::new(config))
    }

    fn new_encoder(
        &self,
        endpoint_name: &str,
        config: &ConnectorConfig,
        key_schema: &Option<Relation>,
        value_schema: &Relation,
        consumer: Box<dyn OutputConsumer>,
        is_index: bool,
    ) -> Result<Box<dyn Encoder>, ControllerError> {
        let format_config = &config.format.as_ref().unwrap().config;
        let format_config = if format_config.is_null() {
            &json!({})
        } else {
            format_config
        };
        let mut json_config = JsonEncoderConfig::deserialize(format_config).map_err(|e| {
            ControllerError::encoder_config_parse_error(
                endpoint_name,
                &e,
                &serde_json::to_string(config).unwrap_or_default(),
            )
        })?;

        if matches!(&config.transport, TransportConfig::RedisOutput(_)) {
            json_config.update_format = JsonUpdateFormat::Redis;
        };

        validate(&json_config, endpoint_name, is_index, value_schema)?;

        // Snowflake and Debezium require one record per message.
        if matches!(
            json_config.update_format,
            JsonUpdateFormat::Snowflake | JsonUpdateFormat::Debezium | JsonUpdateFormat::Redis
        ) {
            json_config.buffer_size_records = 1;
        }

        let key_separator = match &config.transport {
            TransportConfig::RedisOutput(config) => Some(config.key_separator.clone()),
            _ => None,
        };

        Ok(Box::new(JsonEncoder::new(
            consumer,
            json_config,
            value_schema,
            key_separator,
            key_schema.is_some(),
        )))
    }
}

fn validate(
    config: &JsonEncoderConfig,
    endpoint_name: &str,
    is_index: bool,
    value_schema: &Relation,
) -> Result<(), ControllerError> {
    if is_index {
        return Err(ControllerError::invalid_encoder_configuration(
            endpoint_name,
            "JSON encoder cannot be attached to an index",
        ));
    }

    if !matches!(
        config.update_format,
        JsonUpdateFormat::InsertDelete
            | JsonUpdateFormat::Snowflake
            | JsonUpdateFormat::Redis
            | JsonUpdateFormat::Debezium
    ) {
        return Err(ControllerError::output_format_not_supported(
            endpoint_name,
            &format!(
                "{:?} update format is not supported for output JSON streams",
                config.update_format
            ),
        ));
    }

    if let Some(key_fields) = &config.key_fields {
        if !matches!(
            config.update_format,
            JsonUpdateFormat::Debezium | JsonUpdateFormat::Redis
        ) {
            return Err(ControllerError::invalid_encoder_configuration(
                endpoint_name,
                "'key_fields' property is only supported for the 'debezium' or 'redis' update format",
            ));
        }
        if key_fields.is_empty() {
            return Err(ControllerError::invalid_encoder_configuration(
                endpoint_name,
                "'key_fields' is empty: at least one key column must be specified",
            ));
        }

        for key_field in key_fields.iter() {
            if value_schema.field(key_field).is_none() {
                return Err(ControllerError::invalid_encoder_configuration(
                    endpoint_name,
                    &format!("'key_fields' references unknown field '{key_field}'"),
                ));
            }
        }
    }

    Ok(())
}

pub(crate) struct JsonEncoder {
    /// Input handle to push serialized data to.
    output_consumer: Box<dyn OutputConsumer>,
    config: JsonEncoderConfig,
    input_is_indexed: bool,
    value_schema_str: Option<String>,
    key_schema_str: Option<String>,
    buffer: Vec<u8>,
    key_buffer: Vec<u8>,
    max_buffer_size: usize,
    /// Unique id of this encoder instance.
    pub(crate) stream_id: u64,
    /// Sequence number of the last record produced by this encoder.
    seq_number: u64,
    /// Primary key fields.  When specified, only
    /// these fields must be included in the message key.
    key_fields: Option<HashSet<String>>,
    ordered_key_fields: Option<Vec<String>>,
    key_separator: Option<String>,
}

impl JsonEncoder {
    pub(crate) fn new(
        output_consumer: Box<dyn OutputConsumer>,
        mut config: JsonEncoderConfig,
        schema: &Relation,
        key_separator: Option<String>,
        input_is_indexed: bool,
    ) -> Self {
        let max_buffer_size = output_consumer.max_buffer_size_bytes();

        if config.json_flavor.is_none() {
            config.json_flavor = Some(match config.update_format {
                JsonUpdateFormat::Snowflake => JsonFlavor::Snowflake,
                JsonUpdateFormat::Debezium => JsonFlavor::KafkaConnectJsonConverter,
                _ => JsonFlavor::Default,
            });
        }

        let value_schema_str = build_value_schema(&config, schema);
        let key_schema_str = build_key_schema(&config, schema);

        let canonical_key_fields = config
            .key_fields
            .as_ref()
            .map(|fields| fields.iter().map(|f| canonical_identifier(f)));

        let key_fields = canonical_key_fields.clone().map(HashSet::from_iter);
        let ordered_key_fields = canonical_key_fields.map(|f| f.collect());

        Self {
            output_consumer,
            config,
            input_is_indexed,
            value_schema_str,
            key_schema_str,
            buffer: Vec::new(),
            key_buffer: Vec::new(),
            max_buffer_size,
            // Make sure broken JSON parsers/encoders don't convert stream
            // id into a negative number.
            stream_id: StdRng::from_entropy().gen_range(0..i64::MAX) as u64,
            seq_number: 0,
            key_fields,
            ordered_key_fields,
            key_separator,
        }
    }
}

impl Encoder for JsonEncoder {
    fn consumer(&mut self) -> &mut dyn OutputConsumer {
        self.output_consumer.as_mut()
    }

    fn encode(&mut self, batch: Arc<dyn SerBatchReader>) -> AnyResult<()> {
        let mut buffer = take(&mut self.buffer);
        let mut key_buffer = take(&mut self.key_buffer);

        // Reserve one extra byte for the closing bracket `]`.
        let max_buffer_size = if self.config.array {
            self.max_buffer_size - 1
        } else {
            self.max_buffer_size
        };

        let mut num_records = 0;
        let mut cursor = if self.input_is_indexed {
            CursorWithPolarity::new(Box::new(SerCursorFlattened::new(
                batch.cursor(RecordFormat::Json(self.config.json_flavor.clone().unwrap()))?,
            )))
        } else {
            CursorWithPolarity::new(
                batch.cursor(RecordFormat::Json(self.config.json_flavor.clone().unwrap()))?,
            )
        };

        let mut redis_deletion = false;

        while cursor.key_valid() {
            if !cursor.val_valid() {
                cursor.step_key()?;
                continue;
            }
            let mut w = cursor.weight();

            if !(-MAX_DUPLICATES..=MAX_DUPLICATES).contains(&w) {
                let mut key_str = String::new();
                let _ = cursor.serialize_key(unsafe { key_str.as_mut_vec() });
                bail!(
                    "Unable to output record '{}' with very large weight {w}. Consider adjusting your SQL queries to avoid duplicate output records, e.g., using 'SELECT DISTINCT'.",
                    key_str
                );
            }

            while w != 0 {
                let prev_len = buffer.len();

                if self.config.array {
                    if num_records == 0 {
                        buffer.push(b'[');
                    } else {
                        buffer.push(b',');
                    }
                }

                // FIXME: an alternative to building JSON manually is to create an
                // `InsDelUpdate` instance and serialize that, but it would require
                // packaging the serialized key as `serde_json::RawValue`, which is
                // not supported by the `RawValue` API.  So we need a custom
                // implementation of `RawValue`. If we ever decide to build one,
                // check out the "$serde_json::private::RawValue" magic string in
                // crate `serde_json`.
                match self.config.update_format {
                    JsonUpdateFormat::InsertDelete => {
                        if w > 0 {
                            buffer.extend_from_slice(br#"{"insert":"#);
                        } else {
                            buffer.extend_from_slice(br#"{"delete":"#);
                        }
                        cursor.serialize_key(&mut buffer)?;
                        buffer.push(b'}');
                    }
                    JsonUpdateFormat::Snowflake => {
                        cursor.serialize_key(&mut buffer)?;

                        // Remove the closing brace and add '__action' field.
                        if buffer.pop() != Some(b'}') {
                            bail!("Serialized JSON value does not end in '}}'");
                        }
                        if buffer.last() != Some(&b'{') {
                            buffer.push(b',');
                        }
                        let action = if w > 0 { "insert" } else { "delete" };
                        write!(
                            buffer,
                            r#""__action":"{action}","__stream_id":{},"__seq_number":{}}}"#,
                            self.stream_id, self.seq_number
                        )?;
                    }
                    JsonUpdateFormat::Redis => {
                        let key_fields = self
                            .key_fields
                            .as_ref()
                            .expect("key field cannot be empty in redis output config");

                        let ordered_key_fields: &[String] = self
                            .ordered_key_fields
                            .as_ref()
                            .expect("key field cannot be empty in redis output config");

                        // Encode the key.
                        let mut buf = Vec::new();

                        cursor.serialize_key_fields(key_fields, &mut buf)?;
                        let json: serde_json::Value = serde_json::from_slice(&buf)?;
                        let keys: Vec<String> = ordered_key_fields
                            .iter()
                            .map(|k| {
                                let x = json.get(k).expect("unreachable");
                                if x.is_string() {
                                    // we don't want the quotes if it is a string
                                    x.as_str().expect("unreachable").to_owned()
                                } else {
                                    x.to_string()
                                }
                            })
                            .collect();
                        let sep = self.key_separator.as_deref().unwrap_or(":");
                        let mut temp = keys.join(sep).into_bytes();
                        key_buffer = std::mem::take(&mut temp);

                        redis_deletion = w <= 0;

                        cursor.serialize_key(&mut buffer)?;
                    }
                    JsonUpdateFormat::Debezium => {
                        // Crate a Debezium-compliant Kafka message.  The key of the messages
                        // contains the entire record being inserted or deleted, including its
                        // schema. The value part of the message is a Debezium change record
                        // with either 'd'elete of 'c'reate operation. When `op` is
                        // 'c'reate, only the `after` component (no `before`) containing the
                        // second copy of the record is present. The value also contains a
                        // schema for the entire payload.  If `op` is 'd'elete, neither
                        // `before` nor `after` is present.
                        //
                        // This encoding is redundant and expensive, but is necessary due
                        // to several limitations:
                        //
                        // - We do not currently work with schema registries and don't implement
                        //   any other way to transfer the schema to the consumer.  We assume
                        //   that the output is consumed by Kafka Connect using the
                        //   `JsonConverter` class, which requires inline schema.
                        //
                        // - Feldera doesn't know which part of the output record forms a key.
                        //   The list of key fields must be configured in the downstream Kafka
                        //   Connector. As a result we must include all fields in the key and
                        //   rely on the connector To extract the actual key.
                        //
                        // - The specific connector implementation we currently work with is
                        //   Debezium JDBC sink, which only supports deletions when the primary
                        //   key of the table is encoded in the key component of the Kafka
                        //   message, i.e., it cannot extract it from the value.
                        //
                        // This encoding assumes the following Debezium sink connector config:
                        // ```
                        // "primary.key.mode": "record_key",
                        // "delete.enabled": True,
                        // "primary.key.fields": "<list_of_primary_key_columns>"
                        // ```

                        // Encode key.
                        if let Some(key_schema_str) = &self.key_schema_str {
                            key_buffer.extend_from_slice(br#"{"schema":"#);
                            key_buffer.extend_from_slice(key_schema_str.as_bytes());
                            key_buffer.extend_from_slice(br#","payload":"#);
                        } else {
                            key_buffer.extend_from_slice(br#"{"payload":"#);
                        }

                        if let Some(key_fields) = &self.key_fields {
                            cursor.serialize_key_fields(key_fields, &mut key_buffer)?;
                        } else {
                            cursor.serialize_key(&mut key_buffer)?;
                        }
                        key_buffer.extend_from_slice(br#"}"#);

                        // Encode value.
                        let (op, when) = if w > 0 {
                            ("c", "after")
                        } else {
                            ("d", "before")
                        };
                        if let Some(schema_str) = &self.value_schema_str {
                            buffer.extend_from_slice(br#"{"schema":"#);
                            buffer.extend_from_slice(schema_str.as_bytes());
                            write!(buffer, r#","payload":{{"op":"{op}","{when}":"#)?;
                        } else {
                            write!(buffer, r#"{{"payload":{{"op":"{op}","{when}":"#)?;
                        }
                        cursor.serialize_key(&mut buffer)?;
                        buffer.extend_from_slice(br#"}}"#);
                    }
                    _ => {
                        // Should never happen.  Unsupported formats are rejected during
                        // initialization.
                        bail!(
                            "Unsupported JSON serialization format: {:?}",
                            self.config.update_format
                        )
                    }
                }

                // Drop the last encoded record if it exceeds max_buffer_size.
                // The record will be included in the next buffer.
                let buffer_full = buffer.len() > max_buffer_size;
                if buffer_full {
                    if num_records == 0 {
                        let record = std::str::from_utf8(&buffer[prev_len..buffer.len()])
                            .unwrap_or_default();
                        // We should be able to fit at least one record in the buffer.
                        bail!(
                            "JSON record exceeds maximum buffer size supported by the output transport. Max supported buffer size is {} bytes, but the following record requires {} bytes: '{}'.",
                            self.max_buffer_size,
                            buffer.len() - prev_len,
                            truncate_ellipse(record, MAX_RECORD_LEN_IN_ERRMSG, "...")
                        );
                    }
                    buffer.truncate(prev_len);
                } else {
                    if w > 0 {
                        w -= 1;
                    } else {
                        w += 1;
                    }
                    num_records += 1;
                    self.seq_number += 1;
                }
                if !self.config.array {
                    buffer.push(b'\n');
                }

                if num_records >= self.config.buffer_size_records || buffer_full {
                    if self.config.array {
                        buffer.push(b']');
                    }

                    // println!(
                    //     "push_buffer: {} bytes",
                    //     buffer.len() /*std::str::from_utf8(&buffer).unwrap()*/
                    // );
                    if !key_buffer.is_empty() {
                        self.output_consumer.push_key(
                            Some(&key_buffer),
                            if redis_deletion { None } else { Some(&buffer) },
                            &[],
                            num_records,
                        );
                    } else {
                        self.output_consumer.push_buffer(&buffer, num_records);
                    }
                    buffer.clear();
                    key_buffer.clear();
                    num_records = 0;
                }
            }

            cursor.step_key()?;
        }

        if num_records > 0 {
            if self.config.array {
                buffer.extend_from_slice(b"]\n");
            }
            if !key_buffer.is_empty() {
                self.output_consumer.push_key(
                    Some(&key_buffer),
                    if redis_deletion { None } else { Some(&buffer) },
                    &[],
                    num_records,
                );
            } else {
                self.output_consumer.push_buffer(&buffer, num_records);
            }

            buffer.clear();
            key_buffer.clear();
        }

        self.buffer = buffer;
        self.key_buffer = key_buffer;

        Ok(())
    }
}

#[cfg(any(test, feature = "testing"))]
pub mod test {
    use crate::{
        format::json::output::JsonEncoder,
        test::{engine_test, published_schema, read, relations, rows},
    };
    use JsonUpdateFormat::{Debezium, InsertDelete, Redis, Snowflake};
    use arrow::datatypes::SchemaRef;
    use feldera_adapterlib::{
        catalog::SerBatchReader,
        format::Encoder,
        test::{ConnectorTestEngine, MockOutputConsumer, Updates},
        transport::OutputBatchType,
    };
    use feldera_types::{
        format::json::{JsonEncoderConfig, JsonUpdateFormat},
        program_schema::Relation,
    };
    use proptest::prelude::*;
    use serde_json::{Value, json};
    use std::sync::{Arc, Mutex};

    /// The view that the cases encode.
    pub fn relation() -> Relation {
        relations::basic("json_output")
    }

    /// Three batches of changes to the view: rows with their weights.
    fn batches() -> Vec<Vec<(i64, &'static str)>> {
        vec![
            vec![
                (1, r#"{"id": 0, "b": true, "s": "foo"}"#),
                (-1, r#"{"id": 1, "b": false, "i": 10, "s": "bar"}"#),
            ],
            vec![
                (-2, r#"{"id": 2, "b": true, "s": "foo"}"#),
                (2, r#"{"id": 3, "b": false, "i": 10, "s": "bar"}"#),
            ],
            vec![
                (-1, r#"{"id": 4, "b": true, "i": 15, "s": "buzz"}"#),
                (3, r#"{"id": 5, "b": false, "s": ""}"#),
            ],
        ]
    }

    /// An encoder of `relation` with `config`, and the output that its consumer collects.
    type Output = Arc<
        Mutex<
            Vec<(
                Option<Vec<u8>>,
                Option<Vec<u8>>,
                Vec<(String, Option<Vec<u8>>)>,
            )>,
        >,
    >;

    fn encoder(config: JsonEncoderConfig, consumer: MockOutputConsumer) -> (JsonEncoder, Output) {
        let output = consumer.data.clone();
        let encoder = JsonEncoder::new(Box::new(consumer), config, &relation(), None, false);
        (encoder, output)
    }

    /// The engine's output batch of `rows`.
    fn batch(engine: &dyn ConnectorTestEngine, rows: &[(i64, &str)]) -> Arc<dyn SerBatchReader> {
        let rows = Updates::from_json(published_schema(&relation()), rows).unwrap();
        engine
            .output_batch(&relation(), rows.rows(), rows.weights())
            .unwrap()
    }

    /// Returns [`batches`] as rows.
    fn fixed_batches() -> Vec<Updates> {
        let schema = published_schema(&relation());
        batches()
            .into_iter()
            .map(|rows| Updates::from_json(schema.clone(), &rows).unwrap())
            .collect()
    }

    /// Encodes `batches` in `update_format`, as lines or arrays, and checks that each batch
    /// encodes every change once per unit of weight: the deletes, then the inserts, each in the
    /// order of the batch's cursor.
    fn check(
        engine: &dyn ConnectorTestEngine,
        update_format: JsonUpdateFormat,
        array: bool,
        batches: &[Updates],
    ) {
        let config = JsonEncoderConfig {
            update_format: update_format.clone(),
            json_flavor: None,
            buffer_size_records: 3,
            array,
            key_fields: None,
        };
        let (mut encoder, output) = encoder(config, MockOutputConsumer::new());
        let schema = published_schema(&relation());
        let mut next_sequence_number = 0;
        for (step, rows) in batches.iter().enumerate() {
            let batch = engine
                .output_batch(&relation(), rows.rows(), rows.weights())
                .unwrap();
            encoder
                .consumer()
                .batch_start(step as u64, OutputBatchType::Delta);
            encoder.encode(batch.clone()).unwrap();
            encoder.consumer().batch_end();

            let bytes: Vec<u8> = output
                .lock()
                .unwrap()
                .drain(..)
                .filter_map(|(_key, value, _headers)| value)
                .flatten()
                .collect();
            let encoded = decode(
                &update_format,
                array,
                &bytes,
                schema.clone(),
                encoder.stream_id,
                &mut next_sequence_number,
            );
            assert_eq!(
                encoded.zset().unwrap(),
                rows.zset().unwrap(),
                "batch {step}"
            );
            assert_eq!(
                encoded,
                read::encoding_order(batch.as_ref(), &relation()).unwrap(),
                "batch {step} is not one envelope per unit of weight, deletes first, in cursor order"
            );
        }
    }

    /// Decodes the envelopes in `bytes`, as lines or arrays, into one row each: +1 for an insert,
    /// -1 for a delete. Checks that Snowflake records carry `stream_id` and sequence numbers that
    /// count up from `next_sequence_number`.
    fn decode(
        update_format: &JsonUpdateFormat,
        array: bool,
        bytes: &[u8],
        schema: SchemaRef,
        stream_id: u64,
        next_sequence_number: &mut u64,
    ) -> Updates {
        let mut envelopes = Vec::new();
        for value in serde_json::Deserializer::from_slice(bytes).into_iter::<Value>() {
            let value = value.unwrap();
            if array {
                envelopes.extend(value.as_array().unwrap().iter().cloned());
            } else {
                envelopes.push(value);
            }
        }
        let rows: Vec<(i64, String)> = envelopes
            .into_iter()
            .map(|envelope| {
                unwrap_envelope(update_format, envelope, stream_id, next_sequence_number)
            })
            .collect();
        let rows: Vec<(i64, &str)> = rows
            .iter()
            .map(|(weight, row)| (*weight, row.as_str()))
            .collect();
        Updates::from_json(schema, &rows).unwrap()
    }

    /// Returns the row in `envelope` as JSON, with +1 for an insert and -1 for a delete.
    ///
    /// Checks the shape of the envelope: an insert/delete envelope has no other member, and a
    /// Debezium event has no `before` for a create and no `after` for a delete.
    fn unwrap_envelope(
        update_format: &JsonUpdateFormat,
        mut envelope: Value,
        stream_id: u64,
        next_sequence_number: &mut u64,
    ) -> (i64, String) {
        match update_format {
            InsertDelete => {
                let object = envelope.as_object_mut().unwrap();
                let change = match (object.remove("insert"), object.remove("delete")) {
                    (Some(row), None) => (1, row.to_string()),
                    (None, Some(row)) => (-1, row.to_string()),
                    _ => panic!("not an insert/delete envelope: {envelope}"),
                };
                assert!(
                    object.is_empty(),
                    "an insert/delete envelope has other members: {object:?}"
                );
                change
            }
            Snowflake => {
                let object = envelope.as_object_mut().unwrap();
                assert_eq!(object.remove("__stream_id"), Some(json!(stream_id)));
                assert_eq!(
                    object.remove("__seq_number"),
                    Some(json!(*next_sequence_number))
                );
                *next_sequence_number += 1;
                let weight = match object.remove("__action").as_ref().and_then(Value::as_str) {
                    Some("insert") => 1,
                    Some("delete") => -1,
                    action => panic!("unexpected Snowflake action {action:?}"),
                };
                (weight, envelope.to_string())
            }
            Debezium => {
                let payload = &envelope["payload"];
                let absent = |side: &str| payload.get(side).is_none_or(Value::is_null);
                match payload["op"].as_str() {
                    Some("c") => {
                        assert!(absent("before"), "a create with a 'before': {envelope}");
                        (1, payload["after"].to_string())
                    }
                    Some("d") => {
                        assert!(absent("after"), "a delete with an 'after': {envelope}");
                        (-1, payload["before"].to_string())
                    }
                    op => panic!("unexpected Debezium operation {op:?}"),
                }
            }
            _ => unreachable!("no envelope for {update_format:?}"),
        }
    }

    /// Insert/delete envelopes, one per line.
    #[engine_test]
    pub fn insert_delete_lines(engine: &dyn ConnectorTestEngine) {
        check(engine, InsertDelete, false, &fixed_batches());
    }

    /// Insert/delete envelopes in JSON arrays.
    #[engine_test]
    pub fn insert_delete_arrays(engine: &dyn ConnectorTestEngine) {
        check(engine, InsertDelete, true, &fixed_batches());
    }

    /// Snowflake records, one per line, numbered in order.
    #[engine_test]
    pub fn snowflake_lines(engine: &dyn ConnectorTestEngine) {
        check(engine, Snowflake, false, &fixed_batches());
    }

    /// Snowflake records in JSON arrays, numbered in order.
    #[engine_test]
    pub fn snowflake_arrays(engine: &dyn ConnectorTestEngine) {
        check(engine, Snowflake, true, &fixed_batches());
    }

    /// Debezium change events, one per line.
    #[engine_test]
    pub fn debezium_lines(engine: &dyn ConnectorTestEngine) {
        check(engine, Debezium, false, &fixed_batches());
    }

    /// Runs [`check`] on random batches of rows with weights from -2 to 2.
    fn check_random(
        engine: &dyn ConnectorTestEngine,
        update_format: JsonUpdateFormat,
        array: bool,
    ) {
        let relation = relation();
        proptest!(|(batches in rows::weighted_batches(&relation, "id", 0..=10, 20, -2..=2))| {
            check(engine, update_format.clone(), array, &batches);
        });
    }

    /// Random insert/delete envelopes, one per line.
    #[engine_test]
    pub fn random_insert_delete_lines(engine: &dyn ConnectorTestEngine) {
        check_random(engine, InsertDelete, false);
    }

    /// Random insert/delete envelopes in JSON arrays.
    #[engine_test]
    pub fn random_insert_delete_arrays(engine: &dyn ConnectorTestEngine) {
        check_random(engine, InsertDelete, true);
    }

    /// Random Snowflake records, one per line.
    #[engine_test]
    pub fn random_snowflake_lines(engine: &dyn ConnectorTestEngine) {
        check_random(engine, Snowflake, false);
    }

    /// Random Snowflake records in JSON arrays.
    #[engine_test]
    pub fn random_snowflake_arrays(engine: &dyn ConnectorTestEngine) {
        check_random(engine, Snowflake, true);
    }

    /// Random Debezium change events, one per line.
    #[engine_test]
    pub fn random_debezium_lines(engine: &dyn ConnectorTestEngine) {
        check_random(engine, Debezium, false);
    }

    /// A record longer than the transport's largest buffer is an error that shows the record.
    #[engine_test]
    pub fn record_longer_than_buffer(engine: &dyn ConnectorTestEngine) {
        let config = JsonEncoderConfig {
            update_format: InsertDelete,
            json_flavor: None,
            buffer_size_records: 3,
            array: false,
            key_fields: None,
        };
        let (mut encoder, _output) =
            encoder(config, MockOutputConsumer::with_max_buffer_size_bytes(32));
        let error = encoder.encode(batch(engine, &batches()[0])).unwrap_err();
        assert_eq!(
            error.to_string(),
            "JSON record exceeds maximum buffer size supported by the output transport. Max supported buffer size is 32 bytes, but the following record requires 46 bytes: '{\"delete\":{\"id\":1,\"b\":false,\"i\":10,\"s\":\"bar\"}}'."
        );
    }

    /// With `key_fields`, a Debezium record carries a key of those fields, both with their
    /// schemas.
    #[engine_test]
    pub fn debezium_key_fields(engine: &dyn ConnectorTestEngine) {
        let config = JsonEncoderConfig {
            update_format: Debezium,
            json_flavor: None,
            buffer_size_records: 1,
            array: false,
            key_fields: Some(vec!["id".to_string(), "s".to_string()]),
        };
        let (mut encoder, output) = encoder(config, MockOutputConsumer::new());
        encoder.encode(batch(engine, &batches()[0])).unwrap();

        let key_schema = json!({"type":"struct","fields":[{"field":"id","optional":false,"type":"int64"},{"field":"s","optional":false,"type":"string"}],"name":"Key"});
        let value_schema = json!({"type":"struct","fields":[{"field":"after","type":"struct","fields":[{"field":"id","type":"int64","optional":false},{"field":"b","type":"boolean","optional":false},{"field":"i","type":"int64","optional":true},{"field":"s","type":"string","optional":false}],"optional":true},{"field":"op","type":"string","optional":false}],"name":"Envelope"});
        let records: Vec<(Option<Value>, Value)> = output
            .lock()
            .unwrap()
            .iter()
            .map(|(key, value, _headers)| {
                (
                    key.as_ref().map(|key| serde_json::from_slice(key).unwrap()),
                    serde_json::from_slice(value.as_ref().unwrap()).unwrap(),
                )
            })
            .collect();
        assert_eq!(
            records,
            vec![
                (
                    Some(json!({"schema": key_schema, "payload": {"id": 1, "s": "bar"}})),
                    json!({"schema": value_schema, "payload": {"op": "d", "before": {"b": false, "i": 10, "id": 1, "s": "bar"}}}),
                ),
                (
                    Some(json!({"schema": key_schema, "payload": {"id": 0, "s": "foo"}})),
                    json!({"schema": value_schema, "payload": {"op": "c", "after": {"id": 0, "b": true, "i": null, "s": "foo"}}}),
                ),
            ]
        );
    }

    /// Redis output keys a record by its key fields: a delete has no value, and an insert's
    /// value is the whole record.
    #[engine_test]
    pub fn redis(engine: &dyn ConnectorTestEngine) {
        let config = JsonEncoderConfig {
            update_format: Redis,
            json_flavor: None,
            buffer_size_records: 1,
            array: false,
            key_fields: Some(vec!["id".to_string(), "s".to_string()]),
        };
        let (mut encoder, output) = encoder(config, MockOutputConsumer::new());
        encoder.encode(batch(engine, &batches()[0])).unwrap();

        let records: Vec<(String, Option<Value>)> = output
            .lock()
            .unwrap()
            .iter()
            .map(|(key, value, _headers)| {
                (
                    String::from_utf8(key.clone().unwrap()).unwrap(),
                    value
                        .as_ref()
                        .map(|value| serde_json::from_slice(value).unwrap()),
                )
            })
            .collect();
        assert_eq!(
            records,
            vec![
                ("1:bar".to_string(), None),
                (
                    "0:foo".to_string(),
                    Some(json!({"id": 0, "s": "foo", "b": true, "i": null}))
                ),
            ]
        );
    }
}
