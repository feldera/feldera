use crate::{
    catalog::{CursorWithPolarity, RecordFormat, SerBatch, SerCursor},
    util::truncate_ellipse,
    ControllerError, Encoder, OutputConsumer, OutputFormat,
};
use actix_web::HttpRequest;
use anyhow::{bail, Result as AnyResult};
use erased_serde::Serialize as ErasedSerialize;
use pipeline_types::format::json::{JsonEncoderConfig, JsonFlavor, JsonUpdateFormat};
use pipeline_types::program_schema::Relation;
use rand::{rngs::StdRng, Rng, SeedableRng};
use serde::Deserialize;
use serde_urlencoded::Deserializer as UrlDeserializer;
use serde_yaml::Value as YamlValue;
use std::io::Write;
use std::{borrow::Cow, mem::take, sync::Arc};

/// JSON format encoder.
pub struct JsonOutputFormat;

/// The largest weight of a record that can be output using
/// a JSON format without explicit weights.  Such formats require
/// duplicating the record `w` times, which is expensive for large
/// weights (and is most likely not what the user intends).
const MAX_DUPLICATES: i64 = 1_000_000;

/// When including a long JSON record in an error message,
/// truncate it to `MAX_RECORD_LEN_IN_ERRMSG` bytes.
static MAX_RECORD_LEN_IN_ERRMSG: usize = 4096;

impl OutputFormat for JsonOutputFormat {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("json")
    }

    fn config_from_http_request(
        &self,
        endpoint_name: &str,
        request: &HttpRequest,
    ) -> Result<Box<dyn ErasedSerialize>, ControllerError> {
        let mut config = JsonEncoderConfig::deserialize(UrlDeserializer::new(
            form_urlencoded::parse(request.query_string().as_bytes()),
        ))
        .map_err(|e| {
            ControllerError::encoder_config_parse_error(endpoint_name, &e, request.query_string())
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
        config: &YamlValue,
        schema: &Relation,
        consumer: Box<dyn OutputConsumer>,
    ) -> Result<Box<dyn Encoder>, ControllerError> {
        let mut config = JsonEncoderConfig::deserialize(config).map_err(|e| {
            ControllerError::encoder_config_parse_error(
                endpoint_name,
                &e,
                &serde_yaml::to_string(config).unwrap_or_default(),
            )
        })?;

        validate(&config, endpoint_name)?;

        // Snowflake requires one record per message.
        if config.update_format == JsonUpdateFormat::Snowflake {
            config.buffer_size_records = 1;
        }

        Ok(Box::new(JsonEncoder::new(consumer, config, schema)))
    }
}

fn validate(config: &JsonEncoderConfig, endpoint_name: &str) -> Result<(), ControllerError> {
    if !matches!(
        config.update_format,
        JsonUpdateFormat::InsertDelete | JsonUpdateFormat::Snowflake
    ) {
        return Err(ControllerError::output_format_not_supported(
            endpoint_name,
            &format!(
                "{:?} update format is not supported for output JSON streams",
                config.update_format
            ),
        ));
    }

    Ok(())
}

struct JsonEncoder {
    /// Input handle to push serialized data to.
    output_consumer: Box<dyn OutputConsumer>,
    config: JsonEncoderConfig,
    buffer: Vec<u8>,
    max_buffer_size: usize,
    /// Unique id of this encoder instance.
    stream_id: u64,
    /// Sequence number of the last record produced by this encoder.
    seq_number: u64,
}

impl JsonEncoder {
    fn new(
        output_consumer: Box<dyn OutputConsumer>,
        mut config: JsonEncoderConfig,
        _schema: &Relation,
    ) -> Self {
        let max_buffer_size = output_consumer.max_buffer_size_bytes();

        if config.json_flavor.is_none() {
            config.json_flavor = Some(match config.update_format {
                JsonUpdateFormat::Snowflake => JsonFlavor::Snowflake,
                _ => JsonFlavor::Default,
            });
        }

        Self {
            output_consumer,
            config,
            buffer: Vec::new(),
            max_buffer_size,
            // Make sure broken JSON parsers/encoders don't convert stream
            // id into a negative number.
            stream_id: StdRng::from_entropy().gen_range(0..i64::MAX) as u64,
            seq_number: 0,
        }
    }
}

impl Encoder for JsonEncoder {
    fn consumer(&mut self) -> &mut dyn OutputConsumer {
        self.output_consumer.as_mut()
    }

    fn encode(&mut self, batches: &[Arc<dyn SerBatch>]) -> AnyResult<()> {
        let mut buffer = take(&mut self.buffer);

        // Reserve one extra byte for the closing bracket `]`.
        let max_buffer_size = if self.config.array {
            self.max_buffer_size - 1
        } else {
            self.max_buffer_size
        };

        let mut num_records = 0;
        for batch in batches.iter() {
            let mut cursor = CursorWithPolarity::new(
                batch.cursor(RecordFormat::Json(self.config.json_flavor.clone().unwrap()))?,
            );

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
                        self.output_consumer.push_buffer(&buffer);
                        buffer.clear();
                        num_records = 0;
                    }
                }

                cursor.step_key();
            }
        }

        if num_records > 0 {
            if self.config.array {
                buffer.extend_from_slice(b"]\n");
            }
            self.output_consumer.push_buffer(&buffer);
            buffer.clear();
        }

        self.buffer = buffer;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::{JsonEncoder, JsonEncoderConfig};
    use crate::test::generate_test_batches_with_weights;
    use crate::{
        catalog::SerBatch,
        format::{
            json::{InsDelUpdate, SnowflakeAction, SnowflakeUpdate},
            Encoder,
        },
        static_compile::seroutput::SerBatchImpl,
        test::{MockOutputConsumer, TestStruct},
    };
    use dbsp::{trace::Batch, IndexedZSet, OrdZSet};
    use log::trace;
    use pipeline_types::format::json::JsonUpdateFormat;
    use proptest::prelude::*;
    use serde::Deserialize;
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::{fmt::Debug, sync::Arc};

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

    fn test_json<U: OutputUpdate<Val = TestStruct>>(
        array: bool,
        batches: Vec<Vec<(TestStruct, i64)>>,
    ) {
        let config = JsonEncoderConfig {
            update_format: U::update_format(),
            json_flavor: None,
            buffer_size_records: 3,
            array,
        };

        let consumer = MockOutputConsumer::new();
        let consumer_data = consumer.data.clone();
        let mut encoder = JsonEncoder::new(Box::new(consumer), config);
        let zsets = batches
            .iter()
            .map(|batch| {
                let zset = OrdZSet::from_keys(
                    (),
                    batch
                        .iter()
                        .map(|(x, w)| (x.clone(), *w))
                        .collect::<Vec<_>>(),
                );
                Arc::new(<SerBatchImpl<_, TestStruct, ()>>::new(zset)) as Arc<dyn SerBatch>
            })
            .collect::<Vec<_>>();
        encoder.encode(zsets.as_slice()).unwrap();

        let seq_number = Rc::new(RefCell::new(0));
        let expected_output = batches
            .into_iter()
            .flat_map(|batch| {
                let zset = OrdZSet::from_keys(
                    (),
                    batch
                        .iter()
                        .map(|(x, w)| (x.clone(), *w))
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
            std::str::from_utf8(&consumer_data.lock().unwrap()).unwrap()
        );

        let consumer_data = consumer_data.lock().unwrap();
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

    fn test_data() -> Vec<Vec<(TestStruct, i64)>> {
        vec![
            vec![
                (
                    TestStruct {
                        id: 0,
                        b: true,
                        i: None,
                        s: "foo".to_string(),
                    },
                    1,
                ),
                (
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
                (
                    TestStruct {
                        id: 2,
                        b: true,
                        i: None,
                        s: "foo".to_string(),
                    },
                    -2,
                ),
                (
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
                (
                    TestStruct {
                        id: 4,
                        b: true,
                        i: Some(15),
                        s: "buzz".to_string(),
                    },
                    -1,
                ),
                (
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
        };

        let consumer = MockOutputConsumer::with_max_buffer_size_bytes(32);
        let mut encoder = JsonEncoder::new(Box::new(consumer), config);
        let zset = OrdZSet::from_keys((), test_data()[0].clone());

        let err = encoder
            .encode(&[Arc::new(<SerBatchImpl<_, TestStruct, ()>>::new(zset)) as Arc<dyn SerBatch>])
            .unwrap_err();
        assert_eq!(format!("{err}"), "JSON record exceeds maximum buffer size supported by the output transport. Max supported buffer size is 32 bytes, but the following record requires 46 bytes: '{\"delete\":{\"id\":1,\"b\":false,\"i\":10,\"s\":\"bar\"}}'.");
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
    }
}
