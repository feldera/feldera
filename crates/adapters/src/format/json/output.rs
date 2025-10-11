use crate::catalog::SerBatchReader;
use crate::format::json::schema::{build_key_schema, build_value_schema};
use crate::format::{MAX_DUPLICATES, MAX_RECORD_LEN_IN_ERRMSG};
use crate::{
    catalog::{CursorWithPolarity, RecordFormat, SerCursor},
    util::truncate_ellipse,
    ControllerError, Encoder, OutputConsumer, OutputFormat,
};
use axum::http::Request;
use axum::body::Body;
use anyhow::{bail, Result as AnyResult};
use erased_serde::Serialize as ErasedSerialize;
use feldera_types::config::{ConnectorConfig, TransportConfig};
use feldera_types::format::json::{JsonEncoderConfig, JsonFlavor, JsonUpdateFormat};
use feldera_types::program_schema::{canonical_identifier, Relation};
use rand::{rngs::StdRng, Rng, SeedableRng};
use serde::Deserialize;
use serde_json::json;
use serde_urlencoded::Deserializer as UrlDeserializer;
use std::collections::HashSet;
use std::{borrow::Cow, io::Write, mem::take};

/// JSON format encoder.
pub struct JsonOutputFormat;

impl OutputFormat for JsonOutputFormat {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("json")
    }

    fn config_from_http_request(
        &self,
        endpoint_name: &str,
        request: &Request<Body>,
    ) -> Result<Box<dyn ErasedSerialize>, ControllerError> {
        let query_string = request.uri().query().unwrap_or("");
        let mut config = JsonEncoderConfig::deserialize(UrlDeserializer::new(
            form_urlencoded::parse(query_string.as_bytes()),
        ))
        .map_err(|e| {
            ControllerError::encoder_config_parse_error(endpoint_name, &e, query_string)
        })?;

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

        validate(&json_config, endpoint_name, key_schema, value_schema)?;

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
        )))
    }
}

fn validate(
    config: &JsonEncoderConfig,
    endpoint_name: &str,
    key_schema: &Option<Relation>,
    value_schema: &Relation,
) -> Result<(), ControllerError> {
    if key_schema.is_some() {
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

struct JsonEncoder {
    /// Input handle to push serialized data to.
    output_consumer: Box<dyn OutputConsumer>,
    config: JsonEncoderConfig,
    value_schema_str: Option<String>,
    key_schema_str: Option<String>,
    buffer: Vec<u8>,
    key_buffer: Vec<u8>,
    max_buffer_size: usize,
    /// Unique id of this encoder instance.
    stream_id: u64,
    /// Sequence number of the last record produced by this encoder.
    seq_number: u64,
    /// Primary key fields.  When specified, only
    /// these fields must be included in the message key.
    key_fields: Option<HashSet<String>>,
    ordered_key_fields: Option<Vec<String>>,
    key_separator: Option<String>,
}

impl JsonEncoder {
    fn new(
        output_consumer: Box<dyn OutputConsumer>,
        mut config: JsonEncoderConfig,
        schema: &Relation,
        key_separator: Option<String>,
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

    fn encode(&mut self, batch: &dyn SerBatchReader) -> AnyResult<()> {
        let mut buffer = take(&mut self.buffer);
        let mut key_buffer = take(&mut self.key_buffer);

        // Reserve one extra byte for the closing bracket `]`.
        let max_buffer_size = if self.config.array {
            self.max_buffer_size - 1
        } else {
            self.max_buffer_size
        };

        let mut num_records = 0;
        let mut cursor = CursorWithPolarity::new(
            batch.cursor(RecordFormat::Json(self.config.json_flavor.clone().unwrap()))?,
        );

        let mut redis_deletion = false;

        while cursor.key_valid() {
            if !cursor.val_valid() {
                cursor.step_key();
                continue;
            }
            let mut w = cursor.weight();

            if !(-MAX_DUPLICATES..=MAX_DUPLICATES).contains(&w) {
                let mut key_str = String::new();
                let _ = cursor.serialize_key(unsafe { key_str.as_mut_vec() });
                bail!(
                        "Unable to output record '{}' with very large weight {w}. Consider adjusting your SQL queries to avoid duplicate output records, e.g., using 'SELECT DISTINCT'.",
                        &key_str
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
                        if w > 0 {
                            if let Some(schema_str) = &self.value_schema_str {
                                buffer.extend_from_slice(br#"{"schema":"#);
                                buffer.extend_from_slice(schema_str.as_bytes());
                                write!(buffer, r#","payload":{{"op":"c","after":"#)?;
                            } else {
                                write!(buffer, r#"{{"payload":{{"op":"c","after":"#)?;
                            }

                            cursor.serialize_key(&mut buffer)?;
                            buffer.extend_from_slice(br#"}}"#);
                        } else if let Some(schema_str) = &self.value_schema_str {
                            write!(
                                buffer,
                                r#"{{"schema":{schema_str},"payload":{{"op":"d"}}}}"#
                            )?;
                        } else {
                            write!(buffer, r#"{{"payload":{{"op":"d"}}}}"#)?;
                        }
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
                        bail!("JSON record exceeds maximum buffer size supported by the output transport. Max supported buffer size is {} bytes, but the following record requires {} bytes: '{}'.",
                                  self.max_buffer_size,
                                  buffer.len() - prev_len,
                                  truncate_ellipse(record, MAX_RECORD_LEN_IN_ERRMSG, "..."));
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

            cursor.step_key();
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

#[cfg(test)]
mod test {
    use super::{JsonEncoder, JsonEncoderConfig};
    use crate::catalog::SerBatchReader;
    use crate::format::json::{DebeziumOp, DebeziumPayload, DebeziumUpdate};
    use crate::{
        catalog::SerBatch,
        format::{json::InsDelUpdate, Encoder},
        static_compile::seroutput::SerBatchImpl,
        test::{generate_test_batches_with_weights, MockOutputConsumer, TestStruct},
    };
    use dbsp::{utils::Tup2, OrdZSet};
    use feldera_types::format::json::JsonUpdateFormat;
    use feldera_types::program_schema::Relation;
    use proptest::prelude::*;
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use std::collections::BTreeMap;
    use std::{cell::RefCell, fmt::Debug, rc::Rc, sync::Arc};
    use tracing::trace;

    #[derive(Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
    pub enum SnowflakeAction {
        #[serde(rename = "insert")]
        Insert,
        #[serde(rename = "delete")]
        Delete,
    }

    #[derive(Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
    #[serde(deny_unknown_fields)]
    pub struct SnowflakeUpdate<T> {
        __stream_id: u64,
        __seq_number: u64,
        __action: SnowflakeAction,
        #[serde(flatten)]
        value: T,
    }

    trait OutputUpdate: Debug + for<'de> Deserialize<'de> + Eq + Ord {
        type Val;

        fn update_format() -> JsonUpdateFormat;
        fn update(insert: bool, val: Self::Val, stream_id: u64, sequence_num: u64) -> Self;
    }

    impl<T> OutputUpdate for InsDelUpdate<T>
    where
        T: Debug + Eq + Ord + for<'de> Deserialize<'de>,
    {
        type Val = T;

        fn update_format() -> JsonUpdateFormat {
            JsonUpdateFormat::InsertDelete
        }

        fn update(insert: bool, val: Self::Val, _stream_id: u64, _sequence_num: u64) -> Self {
            if insert {
                Self {
                    table: None,
                    insert: Some(val),
                    delete: None,
                    update: None,
                }
            } else {
                Self {
                    table: None,
                    insert: None,
                    delete: Some(val),
                    update: None,
                }
            }
        }
    }

    impl<T> OutputUpdate for SnowflakeUpdate<T>
    where
        T: Debug + Eq + Ord + for<'de> Deserialize<'de>,
    {
        type Val = T;

        fn update_format() -> JsonUpdateFormat {
            JsonUpdateFormat::Snowflake
        }

        fn update(insert: bool, value: Self::Val, stream_id: u64, sequence_num: u64) -> Self {
            SnowflakeUpdate {
                value,
                __action: if insert {
                    SnowflakeAction::Insert
                } else {
                    SnowflakeAction::Delete
                },
                __seq_number: sequence_num,
                __stream_id: stream_id,
            }
        }
    }

    impl<T> OutputUpdate for DebeziumUpdate<T>
    where
        T: Debug + Eq + Ord + for<'de> Deserialize<'de>,
    {
        type Val = T;

        fn update_format() -> JsonUpdateFormat {
            JsonUpdateFormat::Debezium
        }

        fn update(insert: bool, value: Self::Val, _stream_id: u64, _sequence_num: u64) -> Self {
            DebeziumUpdate {
                payload: DebeziumPayload {
                    op: if insert {
                        DebeziumOp::Create
                    } else {
                        DebeziumOp::Delete
                    },
                    before: None,
                    after: if insert { Some(value) } else { None },
                },
            }
        }
    }

    fn test_json<U: OutputUpdate<Val = TestStruct>>(
        array: bool,
        batches: Vec<Vec<Tup2<TestStruct, i64>>>,
    ) {
        let config = JsonEncoderConfig {
            update_format: U::update_format(),
            json_flavor: None,
            buffer_size_records: 3,
            array,
            key_fields: None,
        };

        let consumer = MockOutputConsumer::new();
        let consumer_data = consumer.data.clone();
        let mut encoder = JsonEncoder::new(
            Box::new(consumer),
            config,
            &Relation::new(
                "TestStruct".into(),
                TestStruct::schema(),
                false,
                BTreeMap::new(),
            ),
            None,
        );
        let zsets = batches
            .iter()
            .map(|batch| {
                let zset = OrdZSet::from_keys(
                    (),
                    batch
                        .iter()
                        .map(|Tup2(x, w)| Tup2(x.clone(), *w))
                        .collect::<Vec<_>>(),
                );
                Arc::new(<SerBatchImpl<_, TestStruct, ()>>::new(zset)) as Arc<dyn SerBatch>
            })
            .collect::<Vec<_>>();
        for (step, zset) in zsets.iter().enumerate() {
            encoder.consumer().batch_start(step as u64);
            encoder.encode(zset.as_batch_reader()).unwrap();
            encoder.consumer().batch_end();
        }

        let seq_number = Rc::new(RefCell::new(0));
        let expected_output = batches
            .into_iter()
            .flat_map(|batch| {
                let zset = OrdZSet::from_keys(
                    (),
                    batch
                        .iter()
                        .map(|Tup2(x, w)| Tup2(x.clone(), *w))
                        .collect::<Vec<_>>(),
                );
                let mut deletes = zset
                    .iter()
                    .flat_map(|(data, (), weight)| {
                        trace!("data: {data:?}, weight: {weight}");
                        let range = if weight < 0 { weight..0 } else { 0..0 };

                        let seq_number = seq_number.clone();
                        range.map(move |_| {
                            let upd = U::update(
                                false,
                                data.clone(),
                                encoder.stream_id,
                                *seq_number.borrow(),
                            );
                            *seq_number.borrow_mut() += 1;
                            upd
                        })
                    })
                    .collect::<Vec<_>>();
                let mut inserts = zset
                    .iter()
                    .flat_map(|(data, (), weight)| {
                        trace!("data: {data:?}, weight: {weight}");
                        let range = if weight > 0 { 0..weight } else { 0..0 };

                        let seq_number = seq_number.clone();
                        range.map(move |_| {
                            let upd = U::update(
                                true,
                                data.clone(),
                                encoder.stream_id,
                                *seq_number.borrow(),
                            );
                            *seq_number.borrow_mut() += 1;
                            upd
                        })
                    })
                    .collect::<Vec<_>>();

                deletes.append(&mut inserts);
                deletes
            })
            .collect::<Vec<_>>();

        trace!(
            "output: {}",
            std::str::from_utf8(
                &consumer_data
                    .lock()
                    .unwrap()
                    .iter()
                    .filter_map(|(_k, v, _headers)| v.clone())
                    .flatten()
                    .collect::<Vec<_>>()
            )
            .unwrap()
        );

        let consumer_data = consumer_data
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(_k, v, _headers)| v.clone())
            .flatten()
            .collect::<Vec<_>>();
        let deserializer = serde_json::Deserializer::from_slice(&consumer_data);

        let actual_output = if array {
            deserializer
                .into_iter::<Vec<U>>()
                .flat_map(|item| item.unwrap())
                .collect::<Vec<_>>()
        } else {
            deserializer
                .into_iter::<U>()
                .map(|item| item.unwrap())
                .collect::<Vec<_>>()
        };

        assert_eq!(actual_output, expected_output);
    }

    fn test_data() -> Vec<Vec<Tup2<TestStruct, i64>>> {
        vec![
            vec![
                Tup2(
                    TestStruct {
                        id: 0,
                        b: true,
                        i: None,
                        s: "foo".to_string(),
                    },
                    1,
                ),
                Tup2(
                    TestStruct {
                        id: 1,
                        b: false,
                        i: Some(10),
                        s: "bar".to_string(),
                    },
                    -1,
                ),
            ],
            vec![
                Tup2(
                    TestStruct {
                        id: 2,
                        b: true,
                        i: None,
                        s: "foo".to_string(),
                    },
                    -2,
                ),
                Tup2(
                    TestStruct {
                        id: 3,
                        b: false,
                        i: Some(10),
                        s: "bar".to_string(),
                    },
                    2,
                ),
            ],
            vec![
                Tup2(
                    TestStruct {
                        id: 4,
                        b: true,
                        i: Some(15),
                        s: "buzz".to_string(),
                    },
                    -1,
                ),
                Tup2(
                    TestStruct {
                        id: 5,
                        b: false,
                        i: None,
                        s: "".to_string(),
                    },
                    3,
                ),
            ],
        ]
    }

    #[test]
    fn test_long_record_error() {
        let config = JsonEncoderConfig {
            update_format: JsonUpdateFormat::InsertDelete,
            json_flavor: None,
            buffer_size_records: 3,
            array: false,
            key_fields: None,
        };

        let consumer = MockOutputConsumer::with_max_buffer_size_bytes(32);
        let mut encoder = JsonEncoder::new(
            Box::new(consumer),
            config,
            &Relation::new(
                "TestStruct".into(),
                TestStruct::schema(),
                false,
                BTreeMap::new(),
            ),
            None,
        );
        let zset = OrdZSet::from_keys((), test_data()[0].clone());

        let err = encoder
            .encode(&SerBatchImpl::<_, TestStruct, ()>::new(zset) as &dyn SerBatchReader)
            .unwrap_err();
        assert_eq!(format!("{err}"), "JSON record exceeds maximum buffer size supported by the output transport. Max supported buffer size is 32 bytes, but the following record requires 46 bytes: '{\"delete\":{\"id\":1,\"b\":false,\"i\":10,\"s\":\"bar\"}}'.");
    }

    /// Test the `key_fields` option.
    #[test]
    fn test_debezium_key_fields() {
        let config = JsonEncoderConfig {
            update_format: JsonUpdateFormat::Debezium,
            json_flavor: None,
            buffer_size_records: 1,
            array: false,
            key_fields: Some(vec!["id".to_string(), "s".to_string()]),
        };

        let consumer = MockOutputConsumer::new();
        let consumer_data = consumer.data.clone();

        let mut encoder = JsonEncoder::new(
            Box::new(consumer),
            config,
            &Relation::new(
                "TestStruct".into(),
                TestStruct::schema(),
                false,
                BTreeMap::new(),
            ),
            None,
        );
        let zset = OrdZSet::from_keys((), test_data()[0].clone());

        encoder
            .encode(&SerBatchImpl::<_, TestStruct, ()>::new(zset) as &dyn SerBatchReader)
            .unwrap();

        let actual_output = consumer_data
            .lock()
            .unwrap()
            .iter()
            .map(|(k, v, _headers)| {
                (
                    k.clone()
                        .map(|k| serde_json::from_slice::<serde_json::Value>(&k).unwrap()),
                    serde_json::from_slice::<serde_json::Value>(v.as_ref().unwrap()).unwrap(),
                )
            })
            .collect::<Vec<_>>();

        let expected_output = vec![
            (
                Some(json!({
                    "schema":{"type":"struct","fields":[{"field":"id","optional":false,"type":"int64"},{"field":"s","optional":false,"type":"string"}],"name":"Key"},

                    "payload":{"id":1,"s":"bar"}
                })),
                json!({
                    "schema":{"type":"struct","fields":[{"field":"after","type":"struct","fields":[{"field":"id","type":"int64","optional":false},{"field":"b","type":"boolean","optional":false},{"field":"i","type":"int64","optional":true},{"field":"s","type":"string","optional":false}],"optional":true},{"field":"op","type":"string","optional":false}],"name":"Envelope"},
                    "payload":{"op":"d"}
                }),
            ),
            (
                Some(json!({
                    "schema":{"type":"struct","fields":[{"field":"id","optional":false,"type":"int64"},{"field":"s","optional":false,"type":"string"}],"name":"Key"},
                    "payload":{"id":0,"s":"foo"}
                })),
                json!({
                    "schema":{"type":"struct","fields":[{"field":"after","type":"struct","fields":[{"field":"id","type":"int64","optional":false},{"field":"b","type":"boolean","optional":false},{"field":"i","type":"int64","optional":true},{"field":"s","type":"string","optional":false}],"optional":true},{"field":"op","type":"string","optional":false}],"name":"Envelope"},
                    "payload":{"op":"c","after":{"id":0,"b":true,"i":null,"s":"foo"}}
                }),
            ),
        ];

        assert_eq!(actual_output, expected_output)
    }

    #[test]
    fn test_redis() {
        let config = JsonEncoderConfig {
            update_format: JsonUpdateFormat::Redis,
            json_flavor: None,
            buffer_size_records: 1,
            array: false,
            key_fields: Some(vec!["id".to_owned(), "s".to_owned()]),
        };

        let consumer = MockOutputConsumer::new();
        let consumer_data = consumer.data.clone();

        let mut encoder = JsonEncoder::new(
            Box::new(consumer),
            config,
            &Relation::new(
                "TestStruct".into(),
                TestStruct::schema(),
                false,
                BTreeMap::new(),
            ),
            None,
        );

        let zset = OrdZSet::from_keys((), test_data()[0].clone());

        encoder
            .encode(&SerBatchImpl::<_, TestStruct, ()>::new(zset) as &dyn SerBatchReader)
            .unwrap();

        let actual_output = consumer_data
            .lock()
            .unwrap()
            .iter()
            .map(|(k, v, _headers)| {
                (
                    String::from_utf8(k.clone().unwrap()).unwrap(),
                    v.as_ref()
                        .map(|v| serde_json::from_slice::<serde_json::Value>(v).unwrap()),
                )
            })
            .collect::<Vec<_>>();

        let expected_output = vec![
            ("1:bar".to_owned(), None),
            (
                "0:foo".to_owned(),
                Some(json!({"id":0, "s":"foo", "b":true, "i": null})),
            ),
        ];

        assert_eq!(actual_output, expected_output);
    }

    #[test]
    fn test_ndjson_insdel() {
        test_json::<InsDelUpdate<TestStruct>>(false, test_data());
    }

    #[test]
    fn test_arrayjson_insdel() {
        test_json::<InsDelUpdate<TestStruct>>(true, test_data());
    }

    #[test]
    fn test_ndjson_snowflake() {
        test_json::<SnowflakeUpdate<TestStruct>>(false, test_data());
    }

    #[test]
    fn test_arrayjson_snowflake() {
        test_json::<SnowflakeUpdate<TestStruct>>(true, test_data());
    }

    #[test]
    fn test_debezium() {
        test_json::<DebeziumUpdate<TestStruct>>(false, test_data());
    }

    proptest! {
        #[test]
        fn proptest_arrayjson_insdel(data in generate_test_batches_with_weights(10, 20))
        {
            test_json::<InsDelUpdate<TestStruct>>(true, data)
        }

        #[test]
        fn proptest_ndjson_insdel(data in generate_test_batches_with_weights(10, 20))
        {
            test_json::<InsDelUpdate<TestStruct>>(false, data)
        }

        #[test]
        fn proptest_arrayjson_snowflake(data in generate_test_batches_with_weights(10, 20))
        {
            test_json::<SnowflakeUpdate<TestStruct>>(true, data)
        }

        #[test]
        fn proptest_ndjson_snowflake(data in generate_test_batches_with_weights(10, 20))
        {
            test_json::<SnowflakeUpdate<TestStruct>>(false, data)
        }

        #[test]
        fn proptest_debezium(data in generate_test_batches_with_weights(10, 20))
        {
            test_json::<DebeziumUpdate<TestStruct>>(false, data)
        }

    }
}
