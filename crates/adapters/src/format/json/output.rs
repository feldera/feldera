use super::WeightedUpdate;
use crate::{ControllerError, Encoder, OutputConsumer, OutputFormat, SerBatch};
use actix_web::HttpRequest;
use anyhow::Result as AnyResult;
use erased_serde::{Deserializer as ErasedDeserializer, Serialize as ErasedSerialize};
use serde::{Deserialize, Serialize};
use serde_urlencoded::Deserializer as UrlDeserializer;
use std::{borrow::Cow, mem::take, sync::Arc};
use utoipa::ToSchema;

/// JSON format encoder.
pub struct JsonOutputFormat;

const fn default_buffer_size_records() -> usize {
    10_000
}

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
        .map_err(|e| ControllerError::encoder_config_parse_error(endpoint_name, &e))?;
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
        config: &mut dyn ErasedDeserializer,
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
}

impl JsonEncoder {
    fn new(output_consumer: Box<dyn OutputConsumer>, config: JsonEncoderConfig) -> Self {
        Self {
            output_consumer,
            config,
            buffer: Vec::new(),
        }
    }
}

impl Encoder for JsonEncoder {
    fn consumer(&mut self) -> &mut dyn OutputConsumer {
        self.output_consumer.as_mut()
    }

    fn encode(&mut self, batches: &[Arc<dyn SerBatch>]) -> AnyResult<()> {
        let mut buffer = take(&mut self.buffer);

        let mut num_records = 0;

        for batch in batches.iter() {
            let mut cursor = batch.cursor();

            while cursor.key_valid() {
                let w = cursor.weight();
                if self.config.array {
                    if num_records == 0 {
                        buffer.push(b'[');
                    } else {
                        buffer.push(b',');
                    }
                }
                // `WeightedUpdate` format is the only one that supports non-unit weights,
                // so we use it to serialize output records.
                serde_json::to_writer(
                    &mut buffer,
                    &WeightedUpdate {
                        table: None,
                        weight: w,
                        data: cursor.key(),
                    },
                )?;
                if !self.config.array {
                    buffer.push(b'\n');
                }
                num_records += 1;

                if num_records >= self.config.buffer_size_records {
                    if self.config.array {
                        buffer.push(b']');
                    }

                    // println!("push_buffer {}", std::str::from_utf8(&buffer).unwrap());
                    self.output_consumer.push_buffer(&buffer);
                    buffer.clear();
                    num_records = 0;
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
        format::{json::WeightedUpdate, Encoder},
        seroutput::SerBatchImpl,
        test::{MockOutputConsumer, TestStruct},
        SerBatch,
    };
    use dbsp::{trace::Batch, IndexedZSet, OrdZSet};
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
                let zset = OrdZSet::from_keys((), batch.clone());
                Arc::new(<SerBatchImpl<_, TestStruct, ()>>::new(zset)) as Arc<dyn SerBatch>
            })
            .collect::<Vec<_>>();
        encoder.encode(zsets.as_slice()).unwrap();

        let expected_output = batches
            .into_iter()
            .flat_map(|batch| {
                let zset = OrdZSet::from_keys((), batch);
                zset.iter()
                    .map(|(data, (), weight)| WeightedUpdate {
                        table: None,
                        weight,
                        data,
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        println!(
            "output: {}",
            std::str::from_utf8(&consumer_data.lock().unwrap()).unwrap()
        );

        let actual_output = if array {
            serde_json::Deserializer::from_slice(&consumer_data.lock().unwrap())
                .into_iter::<Vec<WeightedUpdate<TestStruct>>>()
                .flat_map(|item| item.unwrap())
                .collect::<Vec<_>>()
        } else {
            serde_json::Deserializer::from_slice(&consumer_data.lock().unwrap())
                .into_iter::<WeightedUpdate<TestStruct>>()
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
                    5,
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
                    -1,
                ),
                (
                    TestStruct {
                        id: 3,
                        b: false,
                        i: Some(10),
                        s: "bar".to_string(),
                    },
                    -2,
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
                    1,
                ),
                (
                    TestStruct {
                        id: 5,
                        b: false,
                        i: None,
                        s: "".to_string(),
                    },
                    2,
                ),
            ],
        ]
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
