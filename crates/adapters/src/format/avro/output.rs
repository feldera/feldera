use crate::catalog::{CursorWithPolarity, SerBatchReader};
use crate::format::avro::schema::{schema_json, AvroSchemaBuilder};
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

#[derive(Debug)]
enum OperationType {
    Insert,
    Delete,
    Upsert,
    //DeleteInsert,
}

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
        key_schema: &Option<Relation>,
        value_schema: &Relation,
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

        if matches!(
            config.transport,
            feldera_types::config::TransportConfig::RedisOutput(_)
        ) {
            return Err(ControllerError::invalid_encoder_configuration(
                endpoint_name,
                "'avro' format not yet supported with Redis connector",
            ));
        }

        let topic = match &config.transport {
            TransportConfig::KafkaOutput(kafka_config) => Some(kafka_config.topic.clone()),
            _ => None,
        };

        Ok(Box::new(AvroEncoder::create(
            endpoint_name,
            key_schema,
            value_schema,
            consumer,
            avro_config,
            topic,
        )?))
    }
}

pub(crate) struct AvroEncoder {
    /// Consumer to push serialized data to.
    output_consumer: Box<dyn OutputConsumer>,

    value_sql_schema: Relation,

    pub(crate) value_avro_schema: AvroSchema,

    /// Only set when connected to an indexed stream.
    key_sql_schema: Option<Relation>,

    /// Only set when using a separate schema for the key.
    pub(crate) key_avro_schema: Option<AvroSchema>,

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
        key_schema: &Option<Relation>,
        value_schema: &Relation,
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

        // We make all value fields optional for better format compatibility, except key fields.
        let key_fields = key_schema.as_ref().map(|key_schema| {
            key_schema
                .fields
                .iter()
                .map(|field| field.name.clone())
                .collect::<Vec<_>>()
        });

        let value_avro_schema = match &config.schema {
            None => AvroSchemaBuilder::new()
                .with_key_fields(key_fields.as_ref())
                .with_namespace(config.namespace.as_deref())
                .relation_to_avro_schema(value_schema)
                .map_err(|e| {
                    ControllerError::invalid_encoder_configuration(
                        endpoint_name,
                        &format!(
                            "error generating Avro schema for the SQL relation {}: {e}",
                            value_schema.name.name()
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
            schema_json(&value_avro_schema)
        );

        let key_avro_schema = if config.update_format.has_key() {
            if let Some(key_schema) = &key_schema {
                let key_avro_schema = AvroSchemaBuilder::new()
                    .with_namespace(config.namespace.as_deref())
                    .relation_to_avro_schema(key_schema)
                    .map_err(|e| {
                        ControllerError::invalid_encoder_configuration(
                            endpoint_name,
                            &format!(
                                "error generating Avro schema for the SQL index {}: {e}",
                                key_schema.name.name()
                            ),
                        )
                    })?;

                debug!(
                    "Avro encoder {endpoint_name}: key schema: {}",
                    schema_json(&key_avro_schema)
                );

                Some(key_avro_schema)
            } else {
                debug!("Avro encoder {endpoint_name}: using the same schema for the key as for the value");
                Some(value_avro_schema.clone())
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

            let key_subject = if let Some(key_avro_schema) = &key_avro_schema {
                match subject_name_strategy {
                    SubjectNameStrategy::RecordName => {
                        Some(key_avro_schema.name().unwrap().fullname(None))
                    }
                    SubjectNameStrategy::TopicName => {
                        if let Some(topic) = &topic {
                            Some(format!("{topic}-key"))
                        } else {
                            return Err(ControllerError::invalid_encoder_configuration(endpoint_name, "'topic_name' subject strategy is only valid for connectors with Kafka transport"));
                        }
                    }
                    SubjectNameStrategy::TopicRecordName => {
                        // use `value_schema`, since the `-key` suffix encodes the fact that this is a key.
                        if let Some(topic) = &topic {
                            Some(format!(
                                "{topic}-{}-key",
                                value_avro_schema.name().unwrap().fullname(None)
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
                SubjectNameStrategy::RecordName => value_avro_schema.name().unwrap().fullname(None),
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
                                value_avro_schema.name().unwrap().fullname(None)
                            )
                        } else {
                            format!(
                                "{topic}-{}",
                                value_avro_schema.name().unwrap().fullname(None)
                            )
                        }
                    } else {
                        return Err(ControllerError::invalid_encoder_configuration(endpoint_name, "'topic_record_name' subject strategy is only valid for connectors with Kafka transport"));
                    }
                }
            };

            value_schema_id = publish_schema(
                endpoint_name,
                &value_avro_schema,
                &value_subject,
                sr_settings,
            )?;
            if let Some(key_avro_schema) = &key_avro_schema {
                key_schema_id = publish_schema(
                    endpoint_name,
                    key_avro_schema,
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
            value_sql_schema: value_schema.clone(),
            value_avro_schema,
            key_sql_schema: key_schema.clone(),
            key_avro_schema,
            value_buffer,
            key_buffer,
            skip_schema_id: config.skip_schema_id,
            update_format: config.update_format,
        })
    }

    fn view_name(&self) -> &SqlIdentifier {
        &self.value_sql_schema.name
    }

    fn index_name(&self) -> Option<&SqlIdentifier> {
        self.key_sql_schema.as_ref().map(|schema| &schema.name)
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

    /// Encode a non-indexed batch.
    ///
    /// Generates inserts and deletes, but not updates.
    fn encode_plain(&mut self, batch: &dyn SerBatchReader) -> AnyResult<()> {
        let mut cursor = CursorWithPolarity::new(batch.cursor(RecordFormat::Avro)?);

        while cursor.key_valid() {
            while cursor.val_valid() {
                let mut w = cursor.weight();

                if !(-MAX_DUPLICATES..=MAX_DUPLICATES).contains(&w) {
                    bail!(
                        "Unable to output record with very large weight {w}. Consider adjusting your SQL queries to avoid duplicate output records, e.g., using 'SELECT DISTINCT'."
                    );
                }

                while w != 0 {
                    let avro_value = cursor
                        .key_to_avro(&self.value_avro_schema, &HashMap::new())
                        .map_err(|e| anyhow!("error converting record to Avro format: {e}"))?;

                    Self::serialize_avro_value(
                        self.skip_schema_id,
                        avro_value,
                        &self.value_avro_schema,
                        &mut self.value_buffer,
                    )?;

                    match self.update_format {
                        AvroUpdateFormat::Raw => {
                            let op = if w > 0 { b"insert" } else { b"delete" };

                            self.output_consumer.push_key(
                                None,
                                Some(&self.value_buffer),
                                &[("op", Some(op))],
                                1,
                            );
                        }
                        AvroUpdateFormat::ConfluentJdbc if w > 0 => {
                            self.output_consumer.push_key(
                                Some(&self.value_buffer),
                                Some(&self.value_buffer),
                                &[],
                                1,
                            );
                        }
                        AvroUpdateFormat::ConfluentJdbc => {
                            self.output_consumer
                                .push_key(Some(&self.value_buffer), None, &[], 1);
                        }
                        AvroUpdateFormat::Debezium => unreachable!(),
                    }

                    if w > 0 {
                        w -= 1;
                    } else {
                        w += 1;
                    }
                }
                cursor.step_val();
            }
            cursor.step_key()
        }

        Ok(())
    }

    fn non_unique_key_error(&self) -> String {
        format!("Avro connector configured with 'index={}' encountered multiple values with the same key. When configured with SQL index, the connector expects keys to be unique. To resolve this, either remove the 'index' attribute from the connector configuration or fix the '{}' view definition to ensure that '{}' is a unique index.",
        self.index_name().unwrap(),
        self.view_name(),
        self.index_name().unwrap())
    }

    /// A helper method for serializing indexed batches.
    /// 1. Determine whether the key under the cursor is an insert, delete or upsert.
    /// 2. Serialize key and/or value needed for this operation by the given output format.
    fn operation_type(&mut self, cursor: &mut dyn SerCursor) -> AnyResult<Option<OperationType>> {
        let mut found_insert = false;
        let mut found_delete = false;

        // First pass: determine the operation type.
        while cursor.val_valid() {
            let w = cursor.weight();

            if w == 0 {
                cursor.step_val();
                continue;
            }

            if w != 1 && w != -1 {
                // TODO: print the key
                bail!(self.non_unique_key_error());
            }

            if w == 1 {
                if found_insert {
                    bail!(self.non_unique_key_error());
                }

                found_insert = true;
            }

            if w == -1 {
                if found_delete {
                    bail!(self.non_unique_key_error());
                }

                found_delete = true;
            }

            cursor.step_val();
        }

        let operation_type = match (found_insert, found_delete) {
            (true, false) => OperationType::Insert,
            (false, true) => OperationType::Delete,
            (true, true) => OperationType::Upsert,
            (false, false) => return Ok(None),
        };

        // Second pass: serialize the key and value for the operation.
        cursor.rewind_vals();

        if let Some(key_schema) = self.key_avro_schema.as_ref() {
            let avro_key = cursor
                .key_to_avro(key_schema, &HashMap::new())
                .map_err(|e| anyhow!("error converting record key to Avro format: {e}"))?;

            Self::serialize_avro_value(
                self.skip_schema_id,
                avro_key,
                key_schema,
                &mut self.key_buffer,
            )?;
        }

        while cursor.val_valid() {
            let w = cursor.weight();

            if w == 1 {
                match operation_type {
                    OperationType::Insert | OperationType::Upsert => {
                        // println!("schema: {:#?}", self.value_avro_schema);
                        let avro_value = cursor
                            .val_to_avro(&self.value_avro_schema, &HashMap::new())
                            .map_err(|e| anyhow!("error converting record to Avro format: {e}"))?;

                        Self::serialize_avro_value(
                            self.skip_schema_id,
                            avro_value,
                            &self.value_avro_schema,
                            &mut self.value_buffer,
                        )?;
                    }
                    _ => (),
                }
            }

            if w == -1 {
                match operation_type {
                    OperationType::Delete if self.update_format == AvroUpdateFormat::Raw => {
                        let avro_value = cursor
                            .val_to_avro(&self.value_avro_schema, &HashMap::new())
                            .map_err(|e| anyhow!("error converting record to Avro format: {e}"))?;

                        Self::serialize_avro_value(
                            self.skip_schema_id,
                            avro_value,
                            &self.value_avro_schema,
                            &mut self.value_buffer,
                        )?;
                    }
                    _ => (),
                }
            }

            cursor.step_val();
        }

        Ok(Some(operation_type))
    }

    /// Encode an indexed batch.
    fn encode_indexed(&mut self, batch: &dyn SerBatchReader) -> AnyResult<()> {
        let mut cursor = batch.cursor(RecordFormat::Avro)?;

        while cursor.key_valid() {
            let operation_type = self.operation_type(cursor.as_mut())?;

            match (operation_type, self.update_format.clone()) {
                (None, _) => (),
                (Some(OperationType::Delete), AvroUpdateFormat::ConfluentJdbc) => {
                    self.output_consumer
                        .push_key(Some(&self.key_buffer), None, &[], 1);
                }
                (Some(OperationType::Delete), AvroUpdateFormat::Raw) => {
                    self.output_consumer.push_key(
                        None,
                        Some(&self.value_buffer),
                        &[("op", Some(b"delete"))],
                        1,
                    );
                }
                (
                    Some(OperationType::Insert) | Some(OperationType::Upsert),
                    AvroUpdateFormat::ConfluentJdbc,
                ) => {
                    self.output_consumer.push_key(
                        Some(&self.key_buffer),
                        Some(&self.value_buffer),
                        &[],
                        1,
                    );
                }
                (Some(OperationType::Insert), AvroUpdateFormat::Raw) => {
                    self.output_consumer.push_key(
                        None,
                        Some(&self.value_buffer),
                        &[("op", Some(b"insert"))],
                        1,
                    );
                }
                (Some(OperationType::Upsert), AvroUpdateFormat::Raw) => {
                    self.output_consumer.push_key(
                        None,
                        Some(&self.value_buffer),
                        &[("op", Some(b"update"))],
                        1,
                    );
                }
                (Some(operation_type), _) => bail!(
                    "internal error: unexpected operation type {:?} for update format {:?}",
                    operation_type,
                    self.update_format
                ),
            }

            cursor.step_key()
        }

        Ok(())
    }
}

impl Encoder for AvroEncoder {
    fn consumer(&mut self) -> &mut dyn OutputConsumer {
        self.output_consumer.as_mut()
    }

    fn encode(&mut self, batch: &dyn SerBatchReader) -> AnyResult<()> {
        if self.key_sql_schema.is_some() {
            self.encode_indexed(batch)
        } else {
            self.encode_plain(batch)
        }
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
