use crate::catalog::{CursorWithPolarity, SerBatchReader};
use crate::format::avro::schema_registry_settings;
use crate::format::MAX_DUPLICATES;
use crate::{ControllerError, Encoder, OutputConsumer, OutputFormat, RecordFormat, SerCursor};
use actix_web::HttpRequest;
use anyhow::{anyhow, bail, Result as AnyResult};
use apache_avro::{to_avro_datum, Schema as AvroSchema};
use erased_serde::Serialize as ErasedSerialize;
use feldera_types::format::avro::AvroEncoderConfig;
use feldera_types::program_schema::Relation;
use log::{debug, error};
use schema_registry_converter::avro_common::get_supplied_schema;
use schema_registry_converter::blocking::schema_registry::post_schema;
use serde::Deserialize;
use serde_urlencoded::Deserializer as UrlDeserializer;
use serde_yaml::Value as YamlValue;
use std::borrow::Cow;
use std::collections::HashMap;
use std::str::FromStr;

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
// - The serializer doesn't currently support the Avro `fixed` type.
// - Add a Kafka end-to-end test to `kafka/test.rs`.  This requires implementing an Avro parser.

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

pub(crate) struct AvroEncoder {
    endpoint_name: String,
    /// Consumer to push serialized data to.
    output_consumer: Box<dyn OutputConsumer>,
    pub(crate) schema: AvroSchema,
    /// Buffer to store serialized avro records, reused across `encode` invocations.
    buffer: Vec<u8>,
    /// Count of skipped deletes, used to rate-limit error messages.
    skipped_deletes: usize,
    /// `True` if the serialized result should not include the schema ID.
    skip_schema_id: bool,
}

impl AvroEncoder {
    pub(crate) fn create(
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

        let sr_settings = schema_registry_settings(&config.registry_config)
            .map_err(|e| ControllerError::invalid_encoder_configuration(endpoint_name, &e))?;

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
                // TODO: resolve schema
                let avro_value = cursor
                    .key_to_avro(&self.schema, &HashMap::new())
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
