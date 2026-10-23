use crate::{
    ControllerError,
    catalog::{DeCollectionStream, RecordFormat},
    format::{InputFormat, ParseError, Parser},
};
use erased_serde::Serialize as ErasedSerialize;
use feldera_adapterlib::ConnectorMetadata;
use feldera_types::circuit::StagedBuffers;
use feldera_types::format::raw::{RawParserConfig, RawParserMode};
use serde::Deserialize;
use serde_json::json;
use serde_urlencoded::Deserializer as UrlDeserializer;
use std::borrow::Cow;

use crate::catalog::InputCollectionHandle;

use super::{InputBuffer, LineSplitter, SpongeSplitter};

/// Raw format parser.
///
/// Ingest raw byte stream into a table with a single column of type VARCHAR or VARBINARY.
pub struct RawInputFormat;

impl InputFormat for RawInputFormat {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("raw")
    }

    fn config_from_http_query(
        &self,
        endpoint_name: &str,
        query: &str,
    ) -> Result<Box<dyn ErasedSerialize>, ControllerError> {
        Ok(Box::new(
            RawParserConfig::deserialize(UrlDeserializer::new(form_urlencoded::parse(
                query.as_bytes(),
            )))
            .map_err(|e| ControllerError::parser_config_parse_error(endpoint_name, &e, query))?,
        ))
    }

    fn new_parser(
        &self,
        endpoint_name: &str,
        input_stream: &InputCollectionHandle,
        config: &serde_json::Value,
    ) -> Result<Box<dyn Parser>, ControllerError> {
        let config = if config.is_null() { &json!({}) } else { config };
        let config = RawParserConfig::deserialize(config).map_err(|e| {
            ControllerError::parser_config_parse_error(endpoint_name, &e, &config.to_string())
        })?;

        let num_columns = input_stream.schema.fields.len();

        let field = if let Some(column_name) = &config.column_name {
            input_stream
                .schema
                .field(column_name)
                .ok_or_else(|| {
                    ControllerError::invalid_parser_configuration(
                        endpoint_name,
                        &format!(
                            "column '{}' not found in table '{}'",
                            column_name, input_stream.schema.name
                        ),
                    )
                })?
                .clone()
        } else {
            if num_columns != 1 {
                return Err(ControllerError::invalid_parser_configuration(
                    endpoint_name,
                    &format!(
                        "The 'raw' input format is used with table '{}', which has {num_columns} columns. Use the 'column_name' configuration option to specify which column should store the raw input data.",
                        input_stream.schema.name
                    ),
                ));
            }

            input_stream.schema.fields[0].clone()
        };

        let typ = field.columntype.typ;

        if !typ.is_varchar() && !typ.is_varbinary() {
            return Err(ControllerError::invalid_parser_configuration(
                endpoint_name,
                &format!(
                    "'raw' input format can only be used with a column of type 'VARCHAR' or 'VARBINARY', but column {} has type {typ}",
                    field.name
                ),
            ));
        }

        let input_stream = input_stream
            .handle
            .configure_deserializer(RecordFormat::Raw(field.name.to_string()))?;
        Ok(Box::new(RawParser::new(input_stream, config)) as Box<dyn Parser>)
    }
}

struct RawParser {
    /// Input handle to push parsed data to.
    input_stream: Box<dyn DeCollectionStream>,

    config: RawParserConfig,

    last_event_number: u64,
}

impl RawParser {
    fn new(input_stream: Box<dyn DeCollectionStream>, config: RawParserConfig) -> Self {
        Self {
            input_stream,
            last_event_number: 0,
            config,
        }
    }

    fn parse_record(
        &mut self,
        record: &[u8],
        metadata: &Option<ConnectorMetadata>,
        errors: &mut Vec<ParseError>,
    ) {
        if let Err(e) = self.input_stream.insert(record, metadata) {
            errors.push(ParseError::bin_event_error(
                format!("failed to deserialize raw record: {e}"),
                self.last_event_number + 1,
                record,
                None,
            ));
        }
    }
}

impl Parser for RawParser {
    fn fork(&self) -> Box<dyn Parser> {
        Box::new(Self::new(self.input_stream.fork(), self.config.clone()))
    }

    fn splitter(&self) -> Box<dyn super::Splitter> {
        match self.config.mode {
            RawParserMode::Lines => Box::new(LineSplitter),
            RawParserMode::Blob => Box::new(SpongeSplitter),
        }
    }

    fn parse(
        &mut self,
        data: &[u8],
        metadata: Option<ConnectorMetadata>,
    ) -> (Option<Box<dyn InputBuffer>>, Vec<ParseError>) {
        let mut errors = Vec::new();

        match self.config.mode {
            RawParserMode::Blob => {
                self.parse_record(data, &metadata, &mut errors);
                self.last_event_number += 1;
            }
            RawParserMode::Lines => {
                for line in data.split(|b| b == &b'\n') {
                    if !line.is_empty() {
                        self.parse_record(line, &metadata, &mut errors);
                        self.last_event_number += 1;
                    }
                }
            }
        }

        (self.input_stream.take_all(), errors)
    }

    fn stage(&self, buffers: Vec<Box<dyn InputBuffer>>) -> Box<dyn StagedBuffers> {
        self.input_stream.stage(buffers)
    }
}

#[cfg(any(test, feature = "testing"))]
pub mod tests;
