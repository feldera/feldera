use crate::catalog::{CursorWithPolarity, SerBatchReader};
use crate::format::MAX_DUPLICATES;
use crate::{ControllerError, Encoder, OutputConsumer, OutputFormat, RecordFormat, SerCursor};
use actix_web::HttpRequest;
use anyhow::{anyhow, bail, Result as AnyResult};
use apache_avro::{to_avro_datum, Schema as AvroSchema};
use erased_serde::Serialize as ErasedSerialize;
use log::{debug, error};
use pipeline_types::format::avro::AvroEncoderConfig;
use pipeline_types::program_schema::Relation;
use schema_registry_converter::avro_common::get_supplied_schema;
use schema_registry_converter::blocking::schema_registry::{post_schema, SrSettings};
use serde::Deserialize;
use serde_urlencoded::Deserializer as UrlDeserializer;
use serde_yaml::Value as YamlValue;
use std::borrow::Cow;
use std::str::FromStr;
use std::time::Duration;

// TODOs:
// - This connector currently only supports raw Avro format, i.e., deletes cannot be represented.
//   Add support for other variants such as Debezium that are able to represent deletions.
// - Support multiple subject name strategies.  Currently, the record name strategy is used
//   to name the schema in the schema registry
// - Add options to specify schema by id or subject name and retrieve it from the registry.
// - Verify that the Avro schema matches table declaration, including nullability of columns
//   by matching the schema against the SQL relation schema.
// - Add an option to generate Avro schema from relation schema.
// - Support complex schemas with cross-references.
// - Make the serializer less strict, e.g., allow non-nullable columns to be encoded
//   as nullable columns and smaller int's to be encoded as long's.
// - The serializer doesn't currently support the Avro `fixed` type.
// - Add a Kafka end-to-end test to `kafka/test.rs`.  This requires implementing an Avro parser,

/// Avro format encoder.
pub struct AvroOutputFormat;

impl OutputFormat for AvroOutputFormat {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("avro")
    }

    fn config_from_http_request(
        &self,
        endpoint_name: &str,
        request: &HttpRequest,
    ) -> Result<Box<dyn ErasedSerialize>, ControllerError> {
        let config = AvroEncoderConfig::deserialize(UrlDeserializer::new(form_urlencoded::parse(
            request.query_string().as_bytes(),
        )))
        .map_err(|e| {
            ControllerError::encoder_config_parse_error(endpoint_name, &e, request.query_string())
        })?;

        Ok(Box::new(config))
    }

    fn new_encoder(
        &self,
        endpoint_name: &str,
        config: &YamlValue,
        _schema: &Relation,
        consumer: Box<dyn OutputConsumer>,
    ) -> Result<Box<dyn Encoder>, ControllerError> {
        let config = AvroEncoderConfig::deserialize(config).map_err(|e| {
            ControllerError::encoder_config_parse_error(
                endpoint_name,
                &e,
                &serde_yaml::to_string(config).unwrap_or_default(),
            )
        })?;

        Ok(Box::new(AvroEncoder::create(
            endpoint_name,
            consumer,
            config,
        )?))
    }
}

struct AvroEncoder {
    endpoint_name: String,
    /// Consumer to push serialized data to.
    output_consumer: Box<dyn OutputConsumer>,
    schema: AvroSchema,
    /// Buffer to store serialized avro records, reused across `encode` invocations.
    buffer: Vec<u8>,
    /// Count of skipped deletes, used to rate-limit error messages.
    skipped_deletes: usize,
    /// `True` if the serialized result should not include the schema ID.
    skip_schema_id: bool,
}

impl AvroEncoder {
    fn create(
        endpoint_name: &str,
        output_consumer: Box<dyn OutputConsumer>,
        config: AvroEncoderConfig,
    ) -> Result<Self, ControllerError> {
        debug!("Creating Avro encoder; config: {config:#?}");

        let schema_json = serde_json::Value::from_str(&config.schema).map_err(|e| {
            ControllerError::encoder_config_parse_error(
                endpoint_name,
                &format!(
                    "'schema' string '{}' is not a valid JSON document: {e}",
                    &config.schema
                ),
                &serde_yaml::to_string(&config).unwrap_or_default(),
            )
        })?;
        let schema = AvroSchema::parse(&schema_json).map_err(|e| {
            ControllerError::encoder_config_parse_error(
                endpoint_name,
                &format!("invalid Avro schema: {e}"),
                &serde_yaml::to_string(&config).unwrap_or_default(),
            )
        })?;

        if config.registry_urls.is_empty() {
            if config.registry_username.is_some() {
                return Err(ControllerError::invalid_encoder_configuration(
                    endpoint_name,
                    "'registry_username' requires 'registry_urls' to be set",
                ));
            }
            if config.registry_authorization_token.is_some() {
                return Err(ControllerError::invalid_encoder_configuration(
                    endpoint_name,
                    "'registry_authorization_token' requires 'registry_urls' to be set",
                ));
            }
            if config.registry_proxy.is_some() {
                return Err(ControllerError::invalid_encoder_configuration(
                    endpoint_name,
                    "'registry_proxy' requires 'registry_urls' to be set",
                ));
            }
            if !config.registry_headers.is_empty() {
                return Err(ControllerError::invalid_encoder_configuration(
                    endpoint_name,
                    "'registry_headers' requires 'registry_urls' to be set",
                ));
            }
        }

        if config.registry_username.is_some() && config.registry_authorization_token.is_some() {
            return Err(ControllerError::invalid_encoder_configuration(
                endpoint_name,
                "'registry_username' and 'registry_authorization_token' options are mutually exclusive"));
        }

        if config.registry_password.is_some() && config.registry_username.is_none() {
            return Err(ControllerError::invalid_encoder_configuration(
                endpoint_name,
                "'registry_password' option provided without 'registry_username'",
            ));
        }

        let sr_settings = if !config.registry_urls.is_empty() {
            let mut sr_settings_builder = SrSettings::new_builder(config.registry_urls[0].clone());
            for url in &config.registry_urls[1..] {
                sr_settings_builder.add_url(url.clone());
            }
            if let Some(username) = &config.registry_username {
                sr_settings_builder
                    .set_basic_authorization(username, config.registry_password.as_deref());
            }
            if let Some(token) = &config.registry_authorization_token {
                sr_settings_builder.set_token_authorization(token);
            }

            for (key, val) in config.registry_headers.iter() {
                sr_settings_builder.add_header(key.as_str(), val.as_str());
            }

            if let Some(proxy) = &config.registry_proxy {
                sr_settings_builder.set_proxy(proxy.as_str());
            }

            if let Some(timeout) = config.registry_timeout_secs {
                sr_settings_builder.set_timeout(Duration::from_secs(timeout));
            }

            Some(sr_settings_builder.build().map_err(|e| {
                ControllerError::invalid_encoder_configuration(
                    endpoint_name,
                    &format!("invalid schema registry configuration: {e}"),
                )
            })?)
        } else {
            None
        };

        let schema_id = if let Some(sr_settings) = &sr_settings {
            let supplied_schema = get_supplied_schema(&schema);
            let name = supplied_schema
                .name
                .as_ref()
                .ok_or_else(|| {
                    ControllerError::invalid_encoder_configuration(
                        endpoint_name,
                        "Avro schema must be of type 'record'",
                    )
                })?
                .clone();
            let registered_schema =
                post_schema(sr_settings, name, supplied_schema).map_err(|e| {
                    ControllerError::encode_error(
                        endpoint_name,
                        anyhow!("failed to post Avro schema to the schema registry: {e}"),
                    )
                })?;
            debug!(
                "avro encoder {endpoint_name}: registered new avro schema with id {}",
                registered_schema.id
            );
            registered_schema.id
        } else {
            0
        };

        let mut buffer = vec![0u8; 5];
        if !config.skip_schema_id {
            buffer[1..].clone_from_slice(&schema_id.to_be_bytes());
        }

        Ok(Self {
            endpoint_name: endpoint_name.to_string(),
            output_consumer,
            schema,
            buffer,
            skipped_deletes: 0,
            skip_schema_id: config.skip_schema_id,
        })
    }
}

impl Encoder for AvroEncoder {
    fn consumer(&mut self) -> &mut dyn OutputConsumer {
        self.output_consumer.as_mut()
    }

    fn encode(&mut self, batch: &dyn SerBatchReader) -> AnyResult<()> {
        let mut cursor = CursorWithPolarity::new(batch.cursor(RecordFormat::Avro)?);

        while cursor.key_valid() {
            if !cursor.val_valid() {
                cursor.step_key();
                continue;
            }
            let mut w = cursor.weight();

            if w < 0 {
                // TODO: we currently only support the "plain" Avro flavor that does not
                // support deletes.  Other formats, e.g., Debezium will allow deletes.

                // Log the first delete, and then each 10,000's delete.
                if self.skipped_deletes % 10_000 == 0 {
                    error!(
                        "avro encoder {}: received a 'delete' record, but the encoder does not currently support deletes; record will be dropped (total number of dropped deletes: {})",
                        self.endpoint_name,
                        self.skipped_deletes + 1,
                    );
                }
                self.skipped_deletes += 1;
                cursor.step_key();
                continue;
            }

            if !(-MAX_DUPLICATES..=MAX_DUPLICATES).contains(&w) {
                bail!(
                        "Unable to output record with very large weight {w}. Consider adjusting your SQL queries to avoid duplicate output records, e.g., using 'SELECT DISTINCT'."
                    );
            }

            while w != 0 {
                let avro_value = cursor
                    .key_to_avro(&self.schema)
                    .map_err(|e| anyhow!("error converting record to Avro format: {e}"))?;
                let mut avro_buffer = to_avro_datum(&self.schema, avro_value)
                    .map_err(|e| anyhow!("error serializing Avro value: {e}"))?;

                if !self.skip_schema_id {
                    // 5 is the length of the Avro message header (magic byte + 4-byte schema id).
                    self.buffer.truncate(5);
                } else {
                    self.buffer.clear();
                }

                self.buffer.append(&mut avro_buffer);
                self.output_consumer.push_buffer(&self.buffer, 1);

                if w > 0 {
                    w -= 1;
                } else {
                    w += 1;
                }
            }
            cursor.step_key()
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::format::avro::output::{AvroEncoder, AvroEncoderConfig};
    use crate::static_compile::seroutput::SerBatchImpl;
    use crate::test::{generate_test_batches_with_weights, MockOutputConsumer, TestStruct};
    use crate::{Encoder, SerBatch};
    use dbsp::utils::Tup2;
    use dbsp::{DBData, OrdZSet};
    use log::trace;
    use pipeline_types::serde_with_context::{SerializeWithContext, SqlSerdeConfig};
    use proptest::proptest;
    use serde::Deserialize;
    use std::sync::Arc;

    fn test_avro<T>(avro_schema: &str, batches: Vec<Vec<Tup2<T, i64>>>)
    where
        T: DBData + for<'de> Deserialize<'de> + SerializeWithContext<SqlSerdeConfig>,
    {
        let config = AvroEncoderConfig {
            schema: avro_schema.to_string(),
            ..Default::default()
        };

        let consumer = MockOutputConsumer::new();
        let consumer_data = consumer.data.clone();
        let mut encoder =
            AvroEncoder::create("avro_test_endpoint", Box::new(consumer), config).unwrap();
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
                Arc::new(<SerBatchImpl<_, T, ()>>::new(zset)) as Arc<dyn SerBatch>
            })
            .collect::<Vec<_>>();
        for (step, zset) in zsets.iter().enumerate() {
            encoder.consumer().batch_start(step as u64);
            encoder.encode(zset.as_batch_reader()).unwrap();
            encoder.consumer().batch_end();
        }

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
                /*let mut deletes = zset
                .iter()
                .flat_map(|(data, (), weight)| {
                    trace!("data: {data:?}, weight: {weight}");
                    let range = if weight < 0 { weight..0 } else { 0..0 };

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
                .collect::<Vec<_>>();*/
                let inserts = zset
                    .iter()
                    .flat_map(|(data, (), weight)| {
                        trace!("data: {data:?}, weight: {weight}");
                        let range = if weight > 0 { 0..weight } else { 0..0 };

                        range.map(move |_| data.clone())
                    })
                    .collect::<Vec<_>>();

                inserts
            })
            .collect::<Vec<_>>();

        let mut actual_output = Vec::new();
        for avro_datum in consumer_data.lock().unwrap().iter() {
            let avro_value =
                apache_avro::from_avro_datum(&encoder.schema, &mut &avro_datum[5..], None).unwrap();
            // FIXME: this will use the default `serde::Deserialize` implementation and will only work
            // for types where it happens to do the right thing. We should use Avro-compatible
            // deserialization logic instead, when it exists.
            let val = apache_avro::from_value::<T>(&avro_value).unwrap();
            actual_output.push(val);
        }

        assert_eq!(actual_output, expected_output);
    }

    proptest! {
        #[test]
        fn proptest_avro_output(data in generate_test_batches_with_weights(10, 20))
        {
            test_avro::<TestStruct>(TestStruct::avro_schema(), data)
        }
    }
}
