use crate::catalog::SerBatchReader;
use crate::format::json::schema::{build_key_schema, build_value_schema};
use crate::format::{MAX_DUPLICATES, MAX_RECORD_LEN_IN_ERRMSG};
use crate::{
    ControllerError, Encoder, OutputConsumer, OutputFormat,
    catalog::{CursorWithPolarity, RecordFormat, SerCursor},
    util::{IndexedOperationType, indexed_operation_type, truncate_ellipse},
};
use anyhow::{Result as AnyResult, anyhow, bail};
use erased_serde::Serialize as ErasedSerialize;
use feldera_adapterlib::catalog::SerCursorFlattened;
use feldera_types::config::{ConnectorConfig, TransportConfig};
use feldera_types::format::json::{JsonEncoderConfig, JsonFlavor, JsonUpdateFormat};
use feldera_types::program_schema::{Relation, SqlIdentifier, canonical_identifier};
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
            key_schema.as_ref(),
            is_index,
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
        if !matches!(config.update_format, JsonUpdateFormat::Debezium) {
            return Err(ControllerError::invalid_encoder_configuration(
                endpoint_name,
                "JSON encoder supports 'index' only with the 'debezium' update format",
            ));
        }
        if config.key_fields.is_some() {
            return Err(ControllerError::invalid_encoder_configuration(
                endpoint_name,
                "'key_fields' cannot be used with 'index': the columns of the index are the key",
            ));
        }
    } else if config.tombstones_on_delete.is_some() {
        return Err(ControllerError::invalid_encoder_configuration(
            endpoint_name,
            "'tombstones_on_delete' is only valid with 'index'",
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

/// The error for the encoded `record`, which needs `required` bytes and does not fit in a
/// buffer of `max_buffer_size` bytes. `required` includes the key of a keyed message.
fn record_too_long(max_buffer_size: usize, required: usize, record: &[u8]) -> anyhow::Error {
    anyhow!(
        "JSON record exceeds maximum buffer size supported by the output transport. Max supported buffer size is {} bytes, but the following record requires {} bytes: '{}'.",
        max_buffer_size,
        required,
        truncate_ellipse(
            std::str::from_utf8(record).unwrap_or_default(),
            MAX_RECORD_LEN_IN_ERRMSG,
            "..."
        )
    )
}

pub(crate) struct JsonEncoder {
    /// Input handle to push serialized data to.
    output_consumer: Box<dyn OutputConsumer>,
    config: JsonEncoderConfig,
    input_is_indexed: bool,
    /// The key relation of the index that the connector's `index` property names. With an
    /// index, the encoder writes one Debezium record for each key.
    index: Option<Relation>,
    view_name: SqlIdentifier,
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
        key_schema: Option<&Relation>,
        is_index: bool,
    ) -> Self {
        let max_buffer_size = output_consumer.max_buffer_size_bytes();

        if config.json_flavor.is_none() {
            config.json_flavor = Some(match config.update_format {
                JsonUpdateFormat::Snowflake => JsonFlavor::Snowflake,
                JsonUpdateFormat::Debezium => JsonFlavor::KafkaConnectJsonConverter,
                _ => JsonFlavor::Default,
            });
        }

        let index = key_schema.filter(|_| is_index).cloned();
        let value_schema_str = build_value_schema(&config, schema, index.is_some());
        let key_schema_str = build_key_schema(&config, index.as_ref().unwrap_or(schema));

        let canonical_key_fields = config
            .key_fields
            .as_ref()
            .map(|fields| fields.iter().map(|f| canonical_identifier(f)));

        let key_fields = canonical_key_fields.clone().map(HashSet::from_iter);
        let ordered_key_fields = canonical_key_fields.map(|f| f.collect());

        Self {
            output_consumer,
            config,
            input_is_indexed: key_schema.is_some(),
            index,
            view_name: schema.name.clone(),
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

    /// Encodes each key of the indexed `batch` as one Debezium record, keyed by the index
    /// columns: `c` with `after` for an insert, `d` with `before` for a delete, and `u` with
    /// both for an update. A `d` record is followed by a tombstone, unless
    /// `tombstones_on_delete` is `false`.
    fn encode_indexed(&mut self, batch: Arc<dyn SerBatchReader>) -> AnyResult<()> {
        let index_name = &self.index.as_ref().unwrap().name;
        let tombstones = self.config.tombstones_on_delete.unwrap_or(true);
        let mut cursor =
            batch.cursor(RecordFormat::Json(self.config.json_flavor.clone().unwrap()))?;

        while cursor.key_valid() {
            let Some(operation) =
                indexed_operation_type(&self.view_name, index_name, cursor.as_mut())?
            else {
                cursor.step_key()?;
                continue;
            };

            let key = &mut self.key_buffer;
            key.clear();
            match &self.key_schema_str {
                Some(schema) => write!(key, r#"{{"schema":{schema},"payload":"#)?,
                None => key.extend_from_slice(br#"{"payload":"#),
            }
            cursor.serialize_key(key)?;
            key.push(b'}');

            let value = &mut self.buffer;
            value.clear();
            if self.config.array {
                value.push(b'[');
            }
            let start = value.len();
            let op = match operation {
                IndexedOperationType::Insert => "c",
                IndexedOperationType::Delete => "d",
                IndexedOperationType::Upsert => "u",
            };
            match &self.value_schema_str {
                Some(schema) => write!(value, r#"{{"schema":{schema},"payload":{{"op":"{op}""#)?,
                None => write!(value, r#"{{"payload":{{"op":"{op}""#)?,
            }
            // `indexed_operation_type` leaves at most one value of each weight.
            for (field, weight) in [("before", -1), ("after", 1)] {
                cursor.rewind_vals();
                while cursor.val_valid() {
                    if cursor.weight() == weight {
                        write!(value, r#","{field}":"#)?;
                        cursor.serialize_val(value)?;
                    }
                    cursor.step_val();
                }
            }
            value.extend_from_slice(b"}}");

            // The message holds the key and the value with its closing `]` or `\n`.
            let required = self.key_buffer.len() + value.len() + 1;
            if required > self.max_buffer_size {
                return Err(record_too_long(
                    self.max_buffer_size,
                    required,
                    &value[start..],
                ));
            }
            value.push(if self.config.array { b']' } else { b'\n' });

            self.output_consumer
                .push_key(Some(&self.key_buffer), Some(&self.buffer), &[], 1);
            if tombstones && matches!(operation, IndexedOperationType::Delete) {
                // A tombstone counts as 0 records in the output statistics.
                self.output_consumer
                    .push_key(Some(&self.key_buffer), None, &[], 0);
            }
            cursor.step_key()?;
        }

        Ok(())
    }
}

impl Encoder for JsonEncoder {
    fn consumer(&mut self) -> &mut dyn OutputConsumer {
        self.output_consumer.as_mut()
    }

    fn encode(&mut self, batch: Arc<dyn SerBatchReader>) -> AnyResult<()> {
        if self.index.is_some() {
            return self.encode_indexed(batch);
        }

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
                        // schema for the entire payload.  If `op` is 'd'elete, only `before`
                        // is present, and the schema does not declare it.
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
                        // We should be able to fit at least one record in the buffer.
                        let record = &buffer[prev_len..];
                        return Err(record_too_long(self.max_buffer_size, record.len(), record));
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
        OutputFormat,
        format::json::output::{JsonEncoder, JsonOutputFormat},
        test::{engine_test, parser, published_schema, read, relations, rows},
    };
    use JsonUpdateFormat::{Debezium, InsertDelete, Redis, Snowflake};
    use anyhow::Result as AnyResult;
    use arrow::datatypes::SchemaRef;
    use feldera_adapterlib::{
        catalog::SerBatchReader,
        format::{Encoder, InputBuffer, Parser},
        test::{ConnectorTestEngine, InputOptions, MockOutputConsumer, Updates},
        transport::OutputBatchType,
    };
    use feldera_types::{
        config::{ConnectorConfig, FormatConfig},
        format::json::{JsonEncoderConfig, JsonUpdateFormat},
        program_schema::Relation,
    };
    use proptest::prelude::*;
    use serde_json::{Value, json};
    use std::{
        borrow::Cow,
        sync::{Arc, Mutex},
    };

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
        let encoder = JsonEncoder::new(Box::new(consumer), config, &relation(), None, None, false);
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
            tombstones_on_delete: None,
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
            tombstones_on_delete: None,
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
            tombstones_on_delete: None,
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

    /// The key of the index on [`relation`].
    pub fn key_relation() -> Relation {
        relations::basic_key("json_output_key")
    }

    /// Returns the error of a JSON encoder with `format_config` on [`relation`], if any. With
    /// `is_index`, the encoder is attached to the index [`key_relation`].
    fn encoder_error(format_config: Value, is_index: bool) -> Option<String> {
        let mut connector = json!({
            "transport": { "name": "file_output", "config": { "path": "/dev/null" } },
            "format": { "name": "json", "config": format_config },
        });
        if is_index {
            connector["index"] = json!("json_output_key");
        }
        let connector: ConnectorConfig = serde_json::from_value(connector).unwrap();
        JsonOutputFormat
            .new_encoder(
                "test_output",
                &connector,
                &is_index.then(key_relation),
                &relation(),
                Box::new(MockOutputConsumer::new()),
                is_index,
            )
            .err()
            .map(|error| error.to_string())
    }

    /// With an index, the encoder takes only the Debezium format, without `key_fields`.
    #[engine_test]
    pub fn index_requires_debezium_without_key_fields(_engine: &dyn ConnectorTestEngine) {
        let indexed_encoder_error = |format_config| encoder_error(format_config, true);
        assert_eq!(
            indexed_encoder_error(json!({"update_format": "insert_delete"})).as_deref(),
            Some(
                "invalid controller configuration: invalid format configuration for output \
                 endpoint 'test_output': JSON encoder supports 'index' only with the 'debezium' \
                 update format"
            )
        );
        assert_eq!(
            indexed_encoder_error(json!({"update_format": "debezium", "key_fields": ["id"]}))
                .as_deref(),
            Some(
                "invalid controller configuration: invalid format configuration for output \
                 endpoint 'test_output': 'key_fields' cannot be used with 'index': the columns \
                 of the index are the key"
            )
        );
        assert_eq!(
            indexed_encoder_error(json!({"update_format": "debezium"})),
            None
        );
    }

    /// `tombstones_on_delete` is valid only with an index.
    #[engine_test]
    pub fn tombstones_on_delete_requires_an_index(_engine: &dyn ConnectorTestEngine) {
        let config = json!({"update_format": "debezium", "tombstones_on_delete": false});
        assert_eq!(
            encoder_error(config.clone(), false).as_deref(),
            Some(
                "invalid controller configuration: invalid format configuration for output \
                 endpoint 'test_output': 'tombstones_on_delete' is only valid with 'index'"
            )
        );
        assert_eq!(encoder_error(config, true), None);
    }

    /// A keyed message: the key, and the value, which a tombstone does not have.
    type Message = (Vec<u8>, Option<Vec<u8>>);

    /// Encodes `rows` as one indexed batch of the index [`key_relation`] with `encoder`, and
    /// returns the messages that it writes.
    fn encode_indexed(
        engine: &dyn ConnectorTestEngine,
        encoder: &mut JsonEncoder,
        output: &Output,
        rows: &[(i64, &str)],
    ) -> AnyResult<Vec<Message>> {
        let rows = Updates::from_json(published_schema(&relation()), rows).unwrap();
        let batch = engine
            .indexed_output_batch(&key_relation(), &relation(), rows.rows(), rows.weights())
            .unwrap();
        encoder.consumer().batch_start(0, OutputBatchType::Delta);
        encoder.encode(batch)?;
        encoder.consumer().batch_end();
        let messages = output
            .lock()
            .unwrap()
            .drain(..)
            .map(|(key, value, _headers)| (key.unwrap(), value))
            .collect();
        Ok(messages)
    }

    /// Returns the key and the value of each of `messages` as JSON. The value of a tombstone
    /// is `null`.
    fn messages_json(messages: &[Message]) -> Vec<(Value, Value)> {
        messages
            .iter()
            .map(|(key, value)| {
                let value = value.as_ref();
                (
                    serde_json::from_slice(key).unwrap(),
                    value.map_or(Value::Null, |value| serde_json::from_slice(value).unwrap()),
                )
            })
            .collect()
    }

    /// Returns the updates that the Debezium JSON parser reads from the values of `messages`,
    /// without the tombstones, as the Kafka input transport skips them.
    fn parse_debezium(engine: &dyn ConnectorTestEngine, messages: &[Message]) -> Updates {
        let collection = engine
            .input_collection(&relation(), &InputOptions::default())
            .unwrap();
        let format = FormatConfig {
            name: Cow::from("json"),
            config: json!({
                "update_format": "debezium",
                "json_flavor": "kafka_connect_json_converter",
            }),
        };
        let mut parser = parser(collection.as_ref(), &format);
        for value in messages.iter().filter_map(|(_key, value)| value.as_ref()) {
            let (mut buffer, errors) = parser.parse(value, None);
            assert_eq!(errors, []);
            buffer.flush().unwrap();
        }
        collection.updates().unwrap()
    }

    /// Returns a Debezium encoder of the index [`key_relation`] on [`relation`] that writes to
    /// `consumer`, and the messages that it writes.
    fn indexed_encoder(
        tombstones_on_delete: Option<bool>,
        consumer: MockOutputConsumer,
    ) -> (JsonEncoder, Output) {
        let config = JsonEncoderConfig {
            update_format: Debezium,
            json_flavor: None,
            buffer_size_records: 1,
            array: false,
            key_fields: None,
            tombstones_on_delete,
        };
        let output = consumer.data.clone();
        let encoder = JsonEncoder::new(
            Box::new(consumer),
            config,
            &relation(),
            None,
            Some(&key_relation()),
            true,
        );
        (encoder, output)
    }

    /// With an index, each key that a step changes is one Debezium record keyed by the index
    /// columns: `c` for an insert, `u` for a delete and an insert, and `d` for a delete, followed
    /// by a tombstone. The value schema declares `before`. The Debezium JSON parser reads the
    /// messages back as the changes of the step.
    #[engine_test]
    pub fn debezium_indexed(engine: &dyn ConnectorTestEngine) {
        let (mut encoder, output) = indexed_encoder(None, MockOutputConsumer::new());
        let key_schema = json!({"type":"struct","fields":[{"field":"id","optional":false,"type":"int64"}],"name":"Key"});
        let row_schema = json!({"type":"struct","fields":[{"field":"id","type":"int64","optional":false},{"field":"b","type":"boolean","optional":false},{"field":"i","type":"int64","optional":true},{"field":"s","type":"string","optional":false}],"optional":true});
        let mut before_schema = row_schema.clone();
        before_schema["field"] = json!("before");
        let mut after_schema = row_schema;
        after_schema["field"] = json!("after");
        let value_schema = json!({"type":"struct","fields":[before_schema, after_schema, {"field":"op","type":"string","optional":false}],"name":"Envelope"});
        let key = |id: i64| json!({"schema": key_schema, "payload": {"id": id}});
        let value = |payload: Value| json!({"schema": value_schema, "payload": payload});

        let old_0 = r#"{"id": 0, "b": true, "i": null, "s": "foo"}"#;
        let new_0 = r#"{"id": 0, "b": false, "i": 5, "s": "foo"}"#;
        let old_1 = r#"{"id": 1, "b": false, "i": 10, "s": "bar"}"#;
        let row = |row: &str| serde_json::from_str::<Value>(row).unwrap();

        let steps = [
            (
                vec![(1, old_0), (1, old_1)],
                vec![
                    (key(0), value(json!({"op": "c", "after": row(old_0)}))),
                    (key(1), value(json!({"op": "c", "after": row(old_1)}))),
                ],
            ),
            (
                vec![(-1, old_0), (1, new_0), (-1, old_1)],
                vec![
                    (
                        key(0),
                        value(json!({"op": "u", "before": row(old_0), "after": row(new_0)})),
                    ),
                    (key(1), value(json!({"op": "d", "before": row(old_1)}))),
                    (key(1), Value::Null),
                ],
            ),
            (
                vec![(-1, new_0)],
                vec![
                    (key(0), value(json!({"op": "d", "before": row(new_0)}))),
                    (key(0), Value::Null),
                ],
            ),
        ];
        for (step, (rows, expected)) in steps.into_iter().enumerate() {
            let messages = encode_indexed(engine, &mut encoder, &output, &rows).unwrap();
            assert_eq!(messages_json(&messages), expected, "step {step}");

            let parsed = parse_debezium(engine, &messages);
            let changes = Updates::from_json(parsed.rows().schema(), &rows).unwrap();
            assert_eq!(
                parsed.zset().unwrap(),
                changes.zset().unwrap(),
                "step {step}"
            );
        }
    }

    /// With `tombstones_on_delete` set to `false`, no tombstone follows a `d` record.
    #[engine_test]
    pub fn debezium_indexed_without_tombstones(engine: &dyn ConnectorTestEngine) {
        let (mut encoder, output) = indexed_encoder(Some(false), MockOutputConsumer::new());
        let row = r#"{"id": 0, "b": true, "i": null, "s": "foo"}"#;
        encode_indexed(engine, &mut encoder, &output, &[(1, row)]).unwrap();
        let messages = encode_indexed(engine, &mut encoder, &output, &[(-1, row)]).unwrap();
        let messages = messages_json(&messages);
        let ops: Vec<&Value> = messages
            .iter()
            .map(|(_key, value)| &value["payload"]["op"])
            .collect();
        assert_eq!(ops, [&json!("d")]);
    }

    /// With `array`, the value of each indexed message is an array of one event, and a tombstone
    /// follows a `d` event.
    #[engine_test]
    pub fn debezium_indexed_array(engine: &dyn ConnectorTestEngine) {
        let config = JsonEncoderConfig {
            update_format: Debezium,
            json_flavor: None,
            buffer_size_records: 1,
            array: true,
            key_fields: None,
            tombstones_on_delete: None,
        };
        let consumer = MockOutputConsumer::new();
        let output = consumer.data.clone();
        let mut encoder = JsonEncoder::new(
            Box::new(consumer),
            config,
            &relation(),
            None,
            Some(&key_relation()),
            true,
        );
        let row = r#"{"id": 0, "b": true, "i": null, "s": "foo"}"#;
        for (weight, op, message_count) in [(1, "c", 1), (-1, "d", 2)] {
            let messages = encode_indexed(engine, &mut encoder, &output, &[(weight, row)]).unwrap();
            assert_eq!(messages.len(), message_count, "{op}");
            let tombstones = &messages[1..];
            assert!(tombstones.iter().all(|(_key, value)| value.is_none()));
            let value = messages[0].1.as_ref().unwrap();
            assert_eq!(value.last(), Some(&b']'), "{op}");
            let events: Vec<Value> = serde_json::from_slice(value).unwrap();
            assert_eq!(events.len(), 1, "{op}");
            assert_eq!(events[0]["payload"]["op"], json!(op));
        }
    }

    /// With an index, a message fits in the transport's buffer when its key, its value, and the
    /// closing `\n` of the value fit.
    #[engine_test]
    pub fn debezium_indexed_record_too_long(engine: &dyn ConnectorTestEngine) {
        let rows = [(1, r#"{"id": 0, "b": true, "i": null, "s": "foo"}"#)];
        let (mut encoder, output) = indexed_encoder(None, MockOutputConsumer::new());
        let messages = encode_indexed(engine, &mut encoder, &output, &rows).unwrap();
        let (key, value) = &messages[0];
        let size = key.len() + value.as_ref().unwrap().len();

        let consumer = MockOutputConsumer::with_max_buffer_size_bytes(size);
        let (mut encoder, output) = indexed_encoder(None, consumer);
        let fitting = encode_indexed(engine, &mut encoder, &output, &rows).unwrap();
        assert_eq!(fitting, messages);

        let consumer = MockOutputConsumer::with_max_buffer_size_bytes(size - 1);
        let (mut encoder, output) = indexed_encoder(None, consumer);
        let error = encode_indexed(engine, &mut encoder, &output, &rows).unwrap_err();
        let expected = format!(
            "JSON record exceeds maximum buffer size supported by the output transport. Max \
             supported buffer size is {} bytes, but the following record requires {size} bytes: ",
            size - 1
        );
        assert!(error.to_string().starts_with(&expected), "{error}");
    }

    /// With an index, a key with two new values is an error.
    #[engine_test]
    pub fn debezium_indexed_non_unique_key(engine: &dyn ConnectorTestEngine) {
        let (mut encoder, output) = indexed_encoder(None, MockOutputConsumer::new());
        let error = encode_indexed(
            engine,
            &mut encoder,
            &output,
            &[
                (1, r#"{"id": 0, "b": true, "i": null, "s": "foo"}"#),
                (1, r#"{"id": 0, "b": false, "i": null, "s": "foo"}"#),
            ],
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Error description: Multiple new values for the same key."),
            "{error}"
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
            tombstones_on_delete: None,
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
