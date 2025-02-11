use crate::catalog::{CursorWithPolarity, SerBatchReader};
use crate::format::avro::schema::{gen_key_schema, schema_json, AvroSchemaBuilder};
use crate::format::avro::schema_registry_settings;
use crate::format::MAX_DUPLICATES;
use crate::{ControllerError, Encoder, OutputConsumer, OutputFormat, RecordFormat, SerCursor};
use actix_web::HttpRequest;
use anyhow::{anyhow, bail, Result as AnyResult};
use apache_avro::{to_avro_datum, types::Value as AvroValue, Schema as AvroSchema};
use erased_serde::Serialize as ErasedSerialize;
use feldera_types::config::{ConnectorConfig, TransportConfig};
use feldera_types::format::avro::{AvroEncoderConfig, AvroUpdateFormat, SubjectNameStrategy};
use feldera_types::program_schema::{Relation, SqlIdentifier};
use schema_registry_converter::blocking::schema_registry::post_schema;
use schema_registry_converter::blocking::schema_registry::SrSettings;
use schema_registry_converter::schema_registry_common::{SchemaType, SuppliedSchema};
use serde::Deserialize;
use serde_urlencoded::Deserializer as UrlDeserializer;
use std::borrow::Cow;
use std::collections::HashMap;
use tracing::debug;

// TODOs:
// - Add options to specify schema by id or subject name and retrieve it from the registry.
// - Verify that the Avro schema matches table declaration, including nullability of columns
//   by matching the schema against the SQL relation schema.
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
        config: &ConnectorConfig,
        schema: &Relation,
        consumer: Box<dyn OutputConsumer>,
    ) -> Result<Box<dyn Encoder>, ControllerError> {
        let avro_config = AvroEncoderConfig::deserialize(&config.format.as_ref().unwrap().config)
            .map_err(|e| {
            ControllerError::encoder_config_parse_error(
                endpoint_name,
                &e,
                &serde_yaml::to_string(config).unwrap_or_default(),
            )
        })?;

        let topic = match &config.transport {
            TransportConfig::KafkaOutput(kafka_config) => Some(kafka_config.topic.clone()),
            _ => None,
        };

        Ok(Box::new(AvroEncoder::create(
            endpoint_name,
            schema,
            consumer,
            avro_config,
            topic,
        )?))
    }
}

pub(crate) struct AvroEncoder {
    /// Consumer to push serialized data to.
    output_consumer: Box<dyn OutputConsumer>,
    pub(crate) value_schema: AvroSchema,
    /// Only set when using a separate schema for the key.
    key_schema: Option<AvroSchema>,
    /// Buffer to store serialized avro records, reused across `encode` invocations.
    value_buffer: Vec<u8>,
    key_buffer: Vec<u8>,
    /// `True` if the serialized result should not include the schema ID.
    skip_schema_id: bool,
    update_format: AvroUpdateFormat,
}

impl AvroEncoder {
    pub(crate) fn create(
        endpoint_name: &str,
        relation_schema: &Relation,
        output_consumer: Box<dyn OutputConsumer>,
        config: AvroEncoderConfig,
        topic: Option<String>,
    ) -> Result<Self, ControllerError> {
        debug!("Creating Avro encoder; config: {config:#?}");

        match config.update_format {
            AvroUpdateFormat::Raw | AvroUpdateFormat::ConfluentJdbc => (),
            AvroUpdateFormat::Debezium => {
                return Err(ControllerError::invalid_encoder_configuration(
                    endpoint_name,
                    "'debezium' data change event format is not yet supported by the Avro encoder",
                ));
            }
        }

        let key_fields = config.key_fields.as_ref().map(|fs| {
            fs.iter()
                .map(|f| SqlIdentifier::from(&f))
                .collect::<Vec<_>>()
        });

        let value_schema = match &config.schema {
            None => AvroSchemaBuilder::new()
                .with_key_fields(key_fields.as_ref())
                .with_namespace(config.namespace.as_deref())
                .relation_to_avro_schema(relation_schema)
                .map_err(|e| {
                    ControllerError::invalid_encoder_configuration(
                        endpoint_name,
                        &format!(
                            "error generating Avro schema for the SQL relation {}: {e}",
                            relation_schema.name.name()
                        ),
                    )
                })?,
            Some(schema) => AvroSchema::parse_str(schema).map_err(|e| {
                ControllerError::encoder_config_parse_error(
                    endpoint_name,
                    &format!("invalid Avro schema: {e}"),
                    &serde_yaml::to_string(&config).unwrap_or_default(),
                )
            })?,
        };

        debug!(
            "Avro encoder {endpoint_name}: value schema: {}",
            schema_json(&value_schema)
        );

        let AvroSchema::Record(record_schema) = &value_schema else {
            return Err(ControllerError::invalid_encoder_configuration(
                endpoint_name,
                "expected Avro schema of type 'record'",
            ));
        };

        if let Some(key_fields) = &key_fields {
            if !config.update_format.has_key() {
                return Err(ControllerError::invalid_encoder_configuration(
                    endpoint_name,
                    "'key_fields' property is only supported for the 'confluent_jdbc' update format",
                ));
            }
            if key_fields.is_empty() {
                return Err(ControllerError::invalid_encoder_configuration(
                    endpoint_name,
                    "'key_fields' list is empty: at least one key column must be specified",
                ));
            }

            for key_field in key_fields.iter() {
                if !record_schema.lookup.contains_key(&key_field.name()) {
                    return Err(ControllerError::invalid_encoder_configuration(
                        endpoint_name,
                        &format!("'key_fields' references field '{key_field}', which is not part of the schema"),
                    ));
                }
            }
        }

        let key_schema = if config.update_format.has_key() {
            if let Some(key_fields) = &key_fields {
                let key_schema = gen_key_schema(record_schema, key_fields)?;

                debug!(
                    "Avro encoder {endpoint_name}: key schema: {}",
                    schema_json(&key_schema)
                );

                Some(key_schema)
            } else {
                debug!("Avro encoder {endpoint_name}: 'key_fields' not specified; using the same schema for the key as for the value");
                Some(value_schema.clone())
            }
        } else {
            None
        };

        let sr_settings = schema_registry_settings(&config.registry_config)
            .map_err(|e| ControllerError::invalid_encoder_configuration(endpoint_name, &e))?;

        let mut value_schema_id = 0;
        let mut key_schema_id = 0;

        if let Some(sr_settings) = &sr_settings {
            let subject_name_strategy = if let Some(strategy) = config.subject_name_strategy {
                strategy
            } else {
                match config.update_format {
                    AvroUpdateFormat::ConfluentJdbc => SubjectNameStrategy::TopicName,
                    AvroUpdateFormat::Raw => SubjectNameStrategy::RecordName,
                    AvroUpdateFormat::Debezium => SubjectNameStrategy::TopicName,
                }
            };

            let key_subject = if let Some(key_schema) = &key_schema {
                match subject_name_strategy {
                    SubjectNameStrategy::RecordName => {
                        Some(key_schema.name().unwrap().fullname(None))
                    }
                    SubjectNameStrategy::TopicName => {
                        if let Some(topic) = &topic {
                            Some(format!("{topic}-key"))
                        } else {
                            return Err(ControllerError::invalid_encoder_configuration(endpoint_name, "'topic_name' subject strategy is only valid for connectors with Kafka transport"));
                        }
                    }
                    SubjectNameStrategy::TopicRecordName => {
                        // use `value_schema``, since the `-key` suffix encodes the fact that this is a key.
                        if let Some(topic) = &topic {
                            Some(format!(
                                "{topic}-{}-key",
                                value_schema.name().unwrap().fullname(None)
                            ))
                        } else {
                            return Err(ControllerError::invalid_encoder_configuration(endpoint_name, "'topic_record_name' subject strategy is only valid for connectors with Kafka transport"));
                        }
                    }
                }
            } else {
                None
            };

            let value_subject = match subject_name_strategy {
                SubjectNameStrategy::RecordName => value_schema.name().unwrap().fullname(None),
                SubjectNameStrategy::TopicName => {
                    if let Some(topic) = &topic {
                        if config.update_format.has_key() {
                            format!("{topic}-value")
                        } else {
                            topic.to_string()
                        }
                    } else {
                        return Err(ControllerError::invalid_encoder_configuration(endpoint_name, "'topic_name' subject strategy is only valid for connectors with Kafka transport"));
                    }
                }
                SubjectNameStrategy::TopicRecordName => {
                    if let Some(topic) = &topic {
                        if config.update_format.has_key() {
                            format!(
                                "{topic}-{}-value",
                                value_schema.name().unwrap().fullname(None)
                            )
                        } else {
                            format!("{topic}-{}", value_schema.name().unwrap().fullname(None))
                        }
                    } else {
                        return Err(ControllerError::invalid_encoder_configuration(endpoint_name, "'topic_record_name' subject strategy is only valid for connectors with Kafka transport"));
                    }
                }
            };

            value_schema_id =
                publish_schema(endpoint_name, &value_schema, &value_subject, sr_settings)?;
            if let Some(key_schema) = &key_schema {
                key_schema_id = publish_schema(
                    endpoint_name,
                    key_schema,
                    key_subject.as_ref().unwrap(),
                    sr_settings,
                )?;
            }
        };

        let mut value_buffer = vec![0u8; 5];
        let mut key_buffer = vec![0u8; 5];

        if !config.skip_schema_id {
            value_buffer[1..].clone_from_slice(&value_schema_id.to_be_bytes());
            key_buffer[1..].clone_from_slice(&key_schema_id.to_be_bytes());
        }

        Ok(Self {
            output_consumer,
            value_schema,
            key_schema,
            value_buffer,
            key_buffer,
            skip_schema_id: config.skip_schema_id,
            update_format: config.update_format,
        })
    }

    fn serialize_avro_value(
        skip_schema_id: bool,
        value: AvroValue,
        schema: &AvroSchema,
        buffer: &mut Vec<u8>,
    ) -> AnyResult<()> {
        let mut avro_buffer = to_avro_datum(schema, value)
            .map_err(|e| anyhow!("error serializing Avro value: {e}"))?;

        if !skip_schema_id {
            // 5 is the length of the Avro message header (magic byte + 4-byte schema id).
            buffer.truncate(5);
        } else {
            buffer.clear();
        }

        buffer.append(&mut avro_buffer);

        Ok(())
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

            if !(-MAX_DUPLICATES..=MAX_DUPLICATES).contains(&w) {
                bail!(
                        "Unable to output record with very large weight {w}. Consider adjusting your SQL queries to avoid duplicate output records, e.g., using 'SELECT DISTINCT'."
                    );
            }

            while w != 0 {
                // TODO: resolve schema
                let avro_value = if w > 0 || self.update_format == AvroUpdateFormat::Raw {
                    cursor
                        .key_to_avro(&self.value_schema, &HashMap::new())
                        .map_err(|e| anyhow!("error converting record to Avro format: {e}"))?
                } else {
                    AvroValue::Union(0, Box::new(AvroValue::Null))
                };

                let avro_key = if let Some(key_schema) = &self.key_schema {
                    Some(
                        cursor
                            .key_to_avro(key_schema, &HashMap::new())
                            .map_err(|e| {
                                anyhow!("error converting record key to Avro format: {e}")
                            })?,
                    )
                } else {
                    None
                };

                match self.update_format {
                    AvroUpdateFormat::Raw => {
                        let op = if w > 0 { b"insert" } else { b"delete" };

                        Self::serialize_avro_value(
                            self.skip_schema_id,
                            avro_value,
                            &self.value_schema,
                            &mut self.value_buffer,
                        )?;

                        self.output_consumer.push_key(
                            None,
                            Some(&self.value_buffer),
                            &[("op", Some(op))],
                            1,
                        );
                    }
                    AvroUpdateFormat::ConfluentJdbc if w > 0 => {
                        Self::serialize_avro_value(
                            self.skip_schema_id,
                            avro_value.clone(),
                            &self.value_schema,
                            &mut self.value_buffer,
                        )?;

                        Self::serialize_avro_value(
                            self.skip_schema_id,
                            avro_key.unwrap(),
                            self.key_schema.as_ref().unwrap(),
                            &mut self.key_buffer,
                        )?;

                        self.output_consumer.push_key(
                            Some(&self.key_buffer),
                            Some(&self.value_buffer),
                            &[],
                            1,
                        );
                    }
                    AvroUpdateFormat::ConfluentJdbc => {
                        Self::serialize_avro_value(
                            self.skip_schema_id,
                            avro_key.unwrap(),
                            self.key_schema.as_ref().unwrap(),
                            &mut self.key_buffer,
                        )?;

                        self.output_consumer
                            .push_key(Some(&self.key_buffer), None, &[], 1);
                    }
                    AvroUpdateFormat::Debezium => unreachable!(),
                }

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

fn publish_schema(
    endpoint_name: &str,
    schema: &AvroSchema,
    subject: &str,
    sr_settings: &SrSettings,
) -> Result<u32, ControllerError> {
    let supplied_schema = SuppliedSchema {
        name: schema.name().map(|n| n.fullname(None)),
        schema_type: SchemaType::Avro,
        schema: serde_json::to_string(schema).unwrap(),
        references: vec![],
    };

    /*let name = supplied_schema
    .name
    .as_ref()
    .ok_or_else(|| {
        ControllerError::invalid_encoder_configuration(
            endpoint_name,
            "Avro schema must be of type 'record'",
        )
    })?
    .clone();*/
    let registered_schema = post_schema(sr_settings, subject.to_string(), supplied_schema)
        .map_err(|e| {
            ControllerError::encode_error(
                endpoint_name,
                anyhow!("failed to post Avro schema to the schema registry: {e}"),
            )
        })?;
    debug!(
        "avro encoder {endpoint_name}: registered new avro schema '{subject}' with id {}, schema: {:?}",
        registered_schema.id,
        registered_schema
    );
    Ok(registered_schema.id)
}
