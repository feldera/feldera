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
