use crate::{
    catalog::{AvroStream, InputCollectionHandle},
    format::{
        avro::schema::{schema_json, validate_struct_schema},
        Splitter, Sponge,
    },
    ControllerError, DeCollectionHandle, InputBuffer, InputFormat, ParseError, Parser,
};
use actix_web::HttpRequest;
use apache_avro::{from_avro_datum, types::Value as AvroValue, Schema as AvroSchema};
use erased_serde::Serialize as ErasedSerialize;
use feldera_types::{
    format::avro::{AvroParserConfig, AvroUpdateFormat},
    program_schema::Relation,
    serde_with_context::{
        serde_config::{DecimalFormat, VariantFormat},
        DateFormat, SqlSerdeConfig, TimeFormat, TimestampFormat,
    },
};
use log::{debug, info};
use schema_registry_converter::blocking::schema_registry::{get_schema_by_id, SrSettings};
use serde::Deserialize;
use serde_urlencoded::Deserializer as UrlDeserializer;
use serde_yaml::Value as YamlValue;
use std::{
    borrow::Cow,
    collections::HashMap,
    sync::{Arc, Mutex},
};

// TODO: Error handling here is a bit of a mess. Schema parsing and validation happens
// as part of message processing; however since input_XXX methods must return `ParseError`,
// we end up wrapping other error types like schema validation errors in ParseError.
// The right solution is to allow `input_chunk` to return the more general
// `ControllerError` type.  There are multiple concurrent refactorings of this crate
// going on at the moment, so we'll do this later.

use super::{schema::schema_unwrap_optional, schema_registry_settings};

pub const fn avro_de_config() -> &'static SqlSerdeConfig {
    &SqlSerdeConfig {
        timestamp_format: TimestampFormat::MicrosSinceEpoch,
        time_format: TimeFormat::Micros,
        date_format: DateFormat::DaysSinceEpoch,
        decimal_format: DecimalFormat::String,
        variant_format: VariantFormat::JsonString,
    }
}

pub struct AvroInputFormat;

impl InputFormat for AvroInputFormat {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("avro")
    }

    fn new_parser(
        &self,
        endpoint_name: &str,
        input_handle: &InputCollectionHandle,
        config: &YamlValue,
    ) -> Result<Box<dyn Parser>, ControllerError> {
        let config = AvroParserConfig::deserialize(config).map_err(|e| {
            ControllerError::parser_config_parse_error(
                endpoint_name,
                &e,
                &serde_yaml::to_string(config).unwrap_or_default(),
            )
        })?;

        let parser = AvroParser::create(
            endpoint_name,
            &input_handle.schema,
            input_handle.handle.fork(),
            config.clone(),
        )?;

        Ok(Box::new(parser) as Box<dyn Parser>)
    }

    fn config_from_http_request(
        &self,
        endpoint_name: &str,
        request: &HttpRequest,
    ) -> Result<Box<dyn ErasedSerialize>, ControllerError> {
        Ok(Box::new(
            AvroParserConfig::deserialize(UrlDeserializer::new(form_urlencoded::parse(
                request.query_string().as_bytes(),
            )))
            .map_err(|e| {
                ControllerError::parser_config_parse_error(
                    endpoint_name,
                    &e,
                    request.query_string(),
                )
            })?,
        ))
    }
}

struct AvroParser {
    endpoint_name: String,
    sr_settings: Option<SrSettings>,
    input_handle: Box<dyn DeCollectionHandle>,
    input_stream: Option<Box<dyn AvroStream>>,
    config: AvroParserConfig,
    relation_schema: Relation,

    schema_id: Option<u32>,
    schema: Option<AvroSchema>,
    last_event_number: u64,

    /// Schema cache shared between all clones of this connector.
    ///
    /// When the connector first starts, the first worker to get a message will retrieve
    /// the schema; the other workers will get it from the cache.  Likewise, if the schema
    /// changes later, the new one will only be retrieved once.
    ///
    /// Note that we cache errors to avoid spamming the schema registry when the
    /// schema is invalid.
    schema_cache: Arc<Mutex<HashMap<u32, Result<AvroSchema, String>>>>,
}

impl AvroParser {
    fn create(
        endpoint_name: &str,
        relation_schema: &Relation,
        input_handle: Box<dyn DeCollectionHandle>,
        config: AvroParserConfig,
    ) -> Result<Self, ControllerError> {
        let sr_settings = schema_registry_settings(&config.registry_config)
            .map_err(|e| ControllerError::invalid_parser_configuration(endpoint_name, &e))?;

        if config.schema.is_none() && sr_settings.is_none() {
            return Err(ControllerError::invalid_parser_configuration(
                endpoint_name,
                "Avro connector configuration missing schema information; please specify either a static schema using the 'schema' property or a schema registry using the 'registry_urls' property",
            ));
        }

        if config.schema.is_some() && sr_settings.is_some() {
            return Err(ControllerError::invalid_parser_configuration(
                endpoint_name,
                "'schema' and 'registry_urls' properties are mutually exclusive; please specify either a static schema using the 'schema' property or a schema registry using the 'registry_urls' property, but not both",
            ));
        }

        if config.schema.is_none() && config.skip_schema_id {
            return Err(ControllerError::invalid_parser_configuration(
                endpoint_name,
                "Avro connector, configured with 'skip_schema_id' flag, does not specify an Avro schema; please provide the schema definition in the 'schema' field",
            ));
        }

        match config.update_format {
            AvroUpdateFormat::ConfluentJdbc => {
                return Err(ControllerError::invalid_parser_configuration(
                    endpoint_name,
                    "'confluent_jdbc' data change event format is not yet supported by the Avro parser",
                ));
            }
            AvroUpdateFormat::Debezium | AvroUpdateFormat::Raw => (),
        }

        let mut parser = Self {
            endpoint_name: endpoint_name.to_string(),
            input_handle,
            sr_settings,
            input_stream: None,
            config: config.clone(),
            schema_id: None,
            relation_schema: relation_schema.clone(),
            schema: None,
            last_event_number: 0,
            schema_cache: Arc::new(Mutex::new(HashMap::new())),
        };

        if let Some(schema) = &config.schema {
            let schema = parser.validate_schema(schema)?;
            parser.set_schema(schema)?;
        }

        Ok(parser)
    }

    fn lookup_schema(&mut self, schema_id: u32) -> Result<AvroSchema, String> {
        let mut cache = self.schema_cache.lock().unwrap();

        if let Some(res) = cache.get(&schema_id) {
            return res.clone();
        }

        info!(
            "avro parser {}: encountered new schema id {schema_id}",
            self.endpoint_name
        );

        match get_schema_by_id(schema_id, self.sr_settings.as_ref().unwrap()) {
            Err(e) => {
                let error = format!(
                    "error retrieving Avro schema {schema_id} from the schema registry: {e}"
                );

                cache.insert(schema_id, Err(error.clone()));
                Err(error)
            }
            Ok(schema) => {
                debug!(
                    "avro parser: {}: retrieved new schema (id: {schema_id}): {}",
                    self.endpoint_name, &schema.schema
                );

                match self.validate_schema(&schema.schema) {
                    Err(e) => {
                        cache.insert(schema_id, Err(e.to_string()));
                        Err(e.to_string())
                    }
                    Ok(schema) => {
                        cache.insert(schema_id, Ok(schema.clone()));
                        Ok(schema)
                    }
                }
            }
        }
    }

    fn set_schema(&mut self, schema: AvroSchema) -> Result<(), ControllerError> {
        debug!("Setting Avro schema: {}", schema_json(&schema));
        self.schema = Some(schema);

        self.input_stream = Some(self.input_handle.configure_avro_deserializer()?);

        Ok(())
    }

    fn value_schema<'a>(&self, schema: &'a AvroSchema) -> Result<&'a AvroSchema, ControllerError> {
        match self.config.update_format {
            AvroUpdateFormat::Raw => Ok(schema),
            AvroUpdateFormat::Debezium => match schema {
                AvroSchema::Record(record_schema) => {
                    let before_field = *record_schema.lookup.get("before").ok_or_else(|| {
                        ControllerError::schema_validation_error(&format!(
                            "invalid Debezium Avro schema: schema is missing the 'before' field: {}", schema_json(schema)
                        ))
                    })?;
                    let before_schema = &record_schema.fields[before_field].schema;
                    Ok(schema_unwrap_optional(before_schema).0)
                }
                _ => Err(ControllerError::schema_validation_error(&format!(
                    "invalid Debezium Avro schema: expected schema of type 'record', but found: {}",
                    schema_json(schema)
                ))),
            },
            AvroUpdateFormat::ConfluentJdbc => {
                unreachable!()
            }
        }
    }

    fn validate_schema(&self, schema_str: &str) -> Result<AvroSchema, ControllerError> {
        let schema = AvroSchema::parse_str(schema_str).map_err(|e| {
            ControllerError::invalid_parser_configuration(
                &self.endpoint_name,
                &format!("error parsing Avro schema: {e}"),
            )
        })?;

        let value_schema = self.value_schema(&schema)?;

        validate_struct_schema(value_schema, &self.relation_schema.fields).map_err(|e| {
            ControllerError::schema_validation_error(&format!("error validating Avro schema: {e}"))
        })?;

        Ok(schema)
    }

    fn input(&mut self, data: &[u8]) -> Result<(), ParseError> {
        self.last_event_number += 1;

        let mut record = if !self.config.skip_schema_id {
            if data.len() < 5 {
                return Err(ParseError::bin_event_error(
                        "Avro record is less than 5 bytes long (valid Avro records must include a 5-byte header)".to_string(),
                        self.last_event_number,
                        data,
                        None,
                    ));
            }

            if data[0] != 0 {
                return Err(ParseError::bin_event_error(
                        "the first byte in an Avro record is not 0 (valid Avro records start with a 0 magic byte)".to_string(),
                        self.last_event_number,
                        data,
                        None,
                    ));
            }

            let schema_id = u32::from_be_bytes(data[1..5].try_into().unwrap());

            // Ignore the schema id if the connector has a statically configured schema.
            if self.config.schema.is_none() {
                // New or modified schema detected - retrieve the schema from the registry.
                if self.schema_id != Some(schema_id) {
                    let schema = self.lookup_schema(schema_id).map_err(|e| {
                        ParseError::bin_event_error(e, self.last_event_number, data, None)
                    })?;

                    self.set_schema(schema).map_err(|e| {
                        ParseError::bin_event_error(
                            e.to_string(),
                            self.last_event_number,
                            data,
                            None,
                        )
                    })?;

                    self.schema_id = Some(schema_id);

                    // TODO: rate-limit errors.
                }
            }

            &data[5..]
        } else {
            data
        };

        // At this point we should either have a statically configured schema or a successfully installed schema from
        // the schema registry.
        assert!(self.input_stream.is_some());
        assert!(self.schema.is_some());
        let input_stream = self.input_stream.as_mut().unwrap();

        let record_copy = record;

        let avro_value = from_avro_datum(self.schema.as_ref().unwrap(), &mut record, None)
            .map_err(|e| {
                ParseError::bin_envelope_error(
                    format!("error parsing avro record: {e}"),
                    record_copy,
                    None,
                )
            })?;

        match self.config.update_format {
            AvroUpdateFormat::Raw => input_stream.insert(&avro_value).map_err(|e| {
                ParseError::bin_event_error(
                    format!(
                        "error converting avro record to table row (record: {avro_value:?}): {e}"
                    ),
                    self.last_event_number,
                    data,
                    None,
                )
            })?,
            AvroUpdateFormat::Debezium => {
                let (before, after) = Self::extract_debezium_values(&avro_value)?;
                if let Some(before) = before {
                    input_stream.delete(before).map_err(|e| {
                        ParseError::bin_event_error(
                            format!(
                                "error converting 'before' record to table row (record: {before:?}): {e}"
                            ),
                            self.last_event_number,
                            data,
                            None,
                        )
                    })?;
                }
                if let Some(after) = after {
                    input_stream.insert(after).map_err(|e| {
                            ParseError::bin_event_error(
                                format!(
                                    "error converting 'after' record to table row (record: {after:?}): {e}"
                                ),
                                self.last_event_number,
                                data,
                                None,
                            )
                        })?;
                }
            }
            AvroUpdateFormat::ConfluentJdbc => {
                unreachable!()
            }
        }

        Ok(())
    }

    /// Extract before and after fields from a debezium value.
    fn extract_debezium_values(
        value: &AvroValue,
    ) -> Result<(Option<&AvroValue>, Option<&AvroValue>), ParseError> {
        let AvroValue::Record(fields) = value else {
            return Err(ParseError::bin_envelope_error(
                format!(
                    "not a valid Debezium message: expected Avro record, but found '{value:?}'"
                ),
                &[],
                None,
            ));
        };

        let mut before = None;
        let mut after = None;
        let mut fields = fields.iter();

        let mut field = fields.next();

        while let Some((name, value)) = field {
            if name == "before" {
                if let AvroValue::Union(_, val) = value {
                    if **val != AvroValue::Null {
                        before = Some(&**val);
                    }
                }
            }

            if name == "after" {
                if let AvroValue::Union(_, val) = value {
                    if **val != AvroValue::Null {
                        after = Some(&**val);
                    }
                }
                break;
            }
            field = fields.next();
        }

        Ok((before, after))
    }
}

impl Parser for AvroParser {
    fn splitter(&self) -> Box<dyn Splitter> {
        Box::new(Sponge)
    }
    fn parse(&mut self, data: &[u8]) -> (Option<Box<dyn InputBuffer>>, Vec<ParseError>) {
        let errors = self.input(data).map_or_else(|e| vec![e], |_| Vec::new());
        let buffer = self.input_stream.as_mut().and_then(|avro| avro.take());
        (buffer, errors)
    }

    fn fork(&self) -> Box<dyn Parser> {
        Box::new(AvroParser {
            endpoint_name: self.endpoint_name.clone(),
            input_handle: self.input_handle.fork(),
            sr_settings: self.sr_settings.clone(),
            input_stream: self.input_stream.as_ref().map(|s| s.fork()),
            config: self.config.clone(),
            schema_id: self.schema_id,
            relation_schema: self.relation_schema.clone(),
            schema: self.schema.clone(),
            last_event_number: 0,
            schema_cache: self.schema_cache.clone(),
        })
    }
}
