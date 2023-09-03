use super::InsDelUpdate;
use crate::{
    util::truncate_ellipse, ControllerError, Encoder, OutputConsumer, OutputFormat, SerBatch,
};
use actix_web::HttpRequest;
use anyhow::{bail, Result as AnyResult};
use erased_serde::Serialize as ErasedSerialize;
use serde::{Deserialize, Serialize};
use serde_urlencoded::Deserializer as UrlDeserializer;
use serde_yaml::Value as YamlValue;
use std::{borrow::Cow, mem::take, sync::Arc};
use utoipa::ToSchema;

/// JSON format encoder.
pub struct JsonOutputFormat;

const fn default_buffer_size_records() -> usize {
    10_000
}

/// The largest weight of a record that can be output using
/// a JSON format without explicit weights.  Such formats require
/// duplicating the record `w` times, which is expensive for large
/// weights (and is most likely not what the user intends).
const MAX_DUPLICATES: i64 = 1_000_000;

/// When including a long JSON record in an error message,
/// truncate it to `MAX_RECORD_LEN_IN_ERRMSG` bytes.
static MAX_RECORD_LEN_IN_ERRMSG: usize = 4096;

// TODO: support multiple update formats, e.g., `WeightedUpdate`
// suppors arbitrary weights beyond `MAX_DUPLICATES`.
#[derive(Deserialize, Serialize, ToSchema)]
pub struct JsonEncoderConfig {
    #[serde(default = "default_buffer_size_records")]
    buffer_size_records: usize,
    #[serde(default)]
    array: bool,
}

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
        config: &YamlValue,
        consumer: Box<dyn OutputConsumer>,
    ) -> AnyResult<Box<dyn Encoder>> {
        let config = JsonEncoderConfig::deserialize(config)?;

        Ok(Box::new(JsonEncoder::new(consumer, config)))
    }
}

struct JsonEncoder {
    /// Input handle to push serialized data to.
    output_consumer: Box<dyn OutputConsumer>,
    config: JsonEncoderConfig,
    buffer: Vec<u8>,
    max_buffer_size: usize,
}

impl JsonEncoder {
    fn new(output_consumer: Box<dyn OutputConsumer>, config: JsonEncoderConfig) -> Self {
        let max_buffer_size = output_consumer.max_buffer_size_bytes();

        Self {
            output_consumer,
            config,
            buffer: Vec::new(),
            max_buffer_size,
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
            let mut cursor = batch.cursor();

            while cursor.key_valid() {
                let mut w = cursor.weight();

                if !(-MAX_DUPLICATES..=MAX_DUPLICATES).contains(&w) {
                    bail!(
                        "Unable to output record '{}' with very large weight {w}. Consider adjusting your SQL queries to avoid duplicate output records, e.g., using 'SELECT DISTINCT'.",
                        serde_json::to_string(cursor.key()).unwrap_or_default()
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
                    } else if num_records > 0 {
                        buffer.push(b'\n');
                    }
                    let update = InsDelUpdate {
                        table: None,
                        insert: if w > 0 { Some(cursor.key()) } else { None },
                        delete: if w < 0 { Some(cursor.key()) } else { None },
                    };
                    serde_json::to_writer(&mut buffer, &update)?;

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
                buffer.push(b']');
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
    use crate::{
        format::{json::InsDelUpdate, Encoder},
        seroutput::SerBatchImpl,
        test::{MockOutputConsumer, TestStruct},
        SerBatch,
    };
    use dbsp::{trace::Batch, IndexedZSet, OrdZSet};
    use log::trace;
    use std::sync::Arc;

    fn test_json(array: bool, batches: Vec<Vec<(TestStruct, i64)>>) {
        let config = JsonEncoderConfig {
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
                zset.iter()
                    .flat_map(|(data, (), weight)| {
                        trace!("data: {data:?}, weight: {weight}");
                        let range = if weight > 0 { 0..weight } else { weight..0 };

                        range.map(move |_| InsDelUpdate {
                            table: None,
                            insert: if weight > 0 { Some(data.clone()) } else { None },
                            delete: if weight < 0 { Some(data.clone()) } else { None },
                        })
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        trace!(
            "output: {}",
            std::str::from_utf8(&consumer_data.lock().unwrap()).unwrap()
        );

        let actual_output = if array {
            serde_json::Deserializer::from_slice(&consumer_data.lock().unwrap())
                .into_iter::<Vec<InsDelUpdate<TestStruct>>>()
                .flat_map(|item| item.unwrap())
                .collect::<Vec<_>>()
        } else {
            serde_json::Deserializer::from_slice(&consumer_data.lock().unwrap())
                .into_iter::<InsDelUpdate<TestStruct>>()
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
            buffer_size_records: 3,
            array: false,
        };

        let consumer = MockOutputConsumer::with_max_buffer_size_bytes(32);
        let mut encoder = JsonEncoder::new(Box::new(consumer), config);
        let zset = OrdZSet::from_keys((), test_data()[0].clone());

        let err = encoder
            .encode(&[Arc::new(<SerBatchImpl<_, TestStruct, ()>>::new(zset)) as Arc<dyn SerBatch>])
            .unwrap_err();
        assert_eq!(format!("{err}"), "JSON record exceeds maximum buffer size supported by the output transport. Max supported buffer size is 32 bytes, but the following record requires 47 bytes: '{\"insert\":{\"id\":0,\"b\":true,\"i\":null,\"s\":\"foo\"}}'.");
    }

    #[test]
    fn test_ndjson() {
        test_json(false, test_data());
    }

    #[test]
    fn test_arrayjson() {
        test_json(true, test_data());
    }

    use crate::test::generate_test_batches_with_weights;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn proptest_arrayjson(data in generate_test_batches_with_weights(10, 20))
        {
            test_json(true, data)
        }

        #[test]
        fn proptest_ndjson(data in generate_test_batches_with_weights(10, 20))
        {
            test_json(false, data)
        }
    }
}
