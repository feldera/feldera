use crate::{
    ControllerError,
    catalog::{DeCollectionStream, RecordFormat},
    format::{InputFormat, ParseError, Parser},
};
use erased_serde::Serialize as ErasedSerialize;
use feldera_adapterlib::ConnectorMetadata;
use feldera_adapterlib::catalog::StagedBuffers;
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
            .map_err(|e| ControllerError::parser_config_parse_error(endpoint_name, &e))?,
        ))
    }

    fn new_parser(
        &self,
        endpoint_name: &str,
        input_stream: &InputCollectionHandle,
        config: &serde_json::Value,
    ) -> Result<Box<dyn Parser>, ControllerError> {
        let config = if config.is_null() { &json!({}) } else { config };
        let config = RawParserConfig::deserialize(config)
            .map_err(|e| ControllerError::parser_config_parse_error(endpoint_name, &e))?;

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
pub mod test {
    use crate::test::{engine_test, parser};
    use feldera_adapterlib::{
        ConnectorMetadata,
        format::{InputBuffer, Parser},
        test::{ConnectorTestEngine, InputOptions, Updates},
    };
    use feldera_types::{
        config::FormatConfig,
        format::raw::{RawParserConfig, RawParserMode},
        program_schema::{ColumnType, Field, Relation},
        variant::VariantValue,
    };
    use std::{borrow::Cow, collections::BTreeMap};

    /// Returns a table named `name` with a column `data` of type `data`, and, if `kafka_topic`,
    /// a nullable column `kafka_topic`.
    fn table(name: &str, data: ColumnType, kafka_topic: bool) -> Relation {
        let mut fields = vec![Field::new("data".into(), data)];
        if kafka_topic {
            fields.push(Field::new("kafka_topic".into(), ColumnType::varchar(true)));
        }
        Relation::new(name.into(), fields, false, BTreeMap::new())
    }

    /// A table of one VARCHAR column.
    pub fn varchar_relation() -> Relation {
        table("raw_varchar", ColumnType::varchar(false), false)
    }

    /// A table of one VARCHAR column and the Kafka topic, as [`metadata_options`] declares.
    pub fn varchar_metadata_relation() -> Relation {
        table("raw_varchar_metadata", ColumnType::varchar(false), true)
    }

    /// A table of one nullable VARCHAR column.
    pub fn nullable_varchar_relation() -> Relation {
        table("raw_nullable_varchar", ColumnType::varchar(true), false)
    }

    /// A table of one VARBINARY column.
    pub fn varbinary_relation() -> Relation {
        table("raw_varbinary", ColumnType::varbinary(false), false)
    }

    /// A table of one nullable VARBINARY column.
    pub fn nullable_varbinary_relation() -> Relation {
        table("raw_nullable_varbinary", ColumnType::varbinary(true), false)
    }

    /// A table of one nullable VARBINARY column and the Kafka topic, as [`metadata_options`]
    /// declares.
    pub fn nullable_varbinary_metadata_relation() -> Relation {
        table(
            "raw_nullable_varbinary_metadata",
            ColumnType::varbinary(true),
            true,
        )
    }

    /// A table of one nullable VARBINARY column and a nullable column without a DEFAULT.
    pub fn nullable_varbinary_nulls_relation() -> Relation {
        table(
            "raw_nullable_varbinary_nulls",
            ColumnType::varbinary(true),
            true,
        )
    }

    /// The input options of the tables with a Kafka topic: the DEFAULT of `kafka_topic` reads the
    /// attribute of the same name.
    pub fn metadata_options() -> InputOptions {
        InputOptions {
            soft_delete: false,
            attribute_defaults: BTreeMap::from([(
                "kafka_topic".to_string(),
                "kafka_topic".to_string(),
            )]),
        }
    }

    /// Connector metadata that names the Kafka topic `my_topic`.
    fn kafka_topic() -> ConnectorMetadata {
        let mut metadata = ConnectorMetadata::new();
        metadata.insert("kafka_topic", VariantValue::String("my_topic".into()));
        metadata
    }

    /// Parses `foo\nbar` in `mode`, into the column `column_name`, with `metadata`, into a fresh
    /// table of `relation`, and checks that it reports no error and inserts `rows`, in order.
    #[track_caller]
    fn check(
        engine: &dyn ConnectorTestEngine,
        relation: &Relation,
        options: &InputOptions,
        config: RawParserConfig,
        metadata: Option<ConnectorMetadata>,
        rows: &[&str],
    ) {
        let collection = engine.input_collection(relation, options).unwrap();
        let format = FormatConfig {
            name: Cow::from("raw"),
            config: serde_json::to_value(config).unwrap(),
        };
        let mut parser = parser(collection.as_ref(), &format);
        let (mut buffer, errors) = parser.parse(b"foo\nbar", metadata);
        assert_eq!(errors, []);
        buffer.flush().unwrap();

        let rows: Vec<(i64, &str)> = rows.iter().map(|row| (1, *row)).collect();
        let expected = Updates::from_json(collection.schema(), &rows).unwrap();
        assert_eq!(collection.updates().unwrap(), expected);
    }

    /// Each line becomes a VARCHAR row.
    #[engine_test(requires(RawFormat))]
    pub fn varchar_lines(engine: &dyn ConnectorTestEngine) {
        check(
            engine,
            &varchar_relation(),
            &InputOptions::default(),
            RawParserConfig {
                mode: RawParserMode::Lines,
                column_name: None,
            },
            None,
            &[r#"{"data": "foo"}"#, r#"{"data": "bar"}"#],
        );
    }

    /// Each line becomes a VARBINARY row.
    #[engine_test(requires(RawFormat))]
    pub fn varbinary_lines(engine: &dyn ConnectorTestEngine) {
        check(
            engine,
            &varbinary_relation(),
            &InputOptions::default(),
            RawParserConfig {
                mode: RawParserMode::Lines,
                column_name: None,
            },
            None,
            // The bytes of `foo` and `bar`.
            &[r#"{"data": "666f6f"}"#, r#"{"data": "626172"}"#],
        );
    }

    /// The column that `column_name` names takes each line, and the other columns take their
    /// DEFAULTs from the connector attributes.
    #[engine_test(requires(RawFormat))]
    pub fn varchar_lines_with_metadata(engine: &dyn ConnectorTestEngine) {
        check(
            engine,
            &varchar_metadata_relation(),
            &metadata_options(),
            RawParserConfig {
                mode: RawParserMode::Lines,
                column_name: Some("data".to_string()),
            },
            Some(kafka_topic()),
            &[
                r#"{"data": "foo", "kafka_topic": "my_topic"}"#,
                r#"{"data": "bar", "kafka_topic": "my_topic"}"#,
            ],
        );
    }

    /// The whole message becomes one nullable VARCHAR row.
    #[engine_test(requires(RawFormat))]
    pub fn nullable_varchar_blob(engine: &dyn ConnectorTestEngine) {
        check(
            engine,
            &nullable_varchar_relation(),
            &InputOptions::default(),
            RawParserConfig {
                mode: RawParserMode::Blob,
                column_name: None,
            },
            None,
            &[r#"{"data": "foo\nbar"}"#],
        );
    }

    /// The whole message becomes one nullable VARBINARY row.
    #[engine_test(requires(RawFormat))]
    pub fn nullable_varbinary_blob(engine: &dyn ConnectorTestEngine) {
        check(
            engine,
            &nullable_varbinary_relation(),
            &InputOptions::default(),
            RawParserConfig {
                mode: RawParserMode::Blob,
                column_name: None,
            },
            None,
            // The bytes of `foo\nbar`.
            &[r#"{"data": "666f6f0a626172"}"#],
        );
    }

    /// The whole message goes into the column that `column_name` names, and the other columns
    /// take their DEFAULTs from the connector attributes.
    #[engine_test(requires(RawFormat))]
    pub fn nullable_varbinary_blob_with_metadata(engine: &dyn ConnectorTestEngine) {
        check(
            engine,
            &nullable_varbinary_metadata_relation(),
            &metadata_options(),
            RawParserConfig {
                mode: RawParserMode::Blob,
                column_name: Some("data".to_string()),
            },
            Some(kafka_topic()),
            &[r#"{"data": "666f6f0a626172", "kafka_topic": "my_topic"}"#],
        );
    }

    /// A nullable column without a DEFAULT is NULL when the parser fills another column.
    #[engine_test(requires(RawFormat))]
    pub fn nullable_varbinary_blob_with_nulls(engine: &dyn ConnectorTestEngine) {
        check(
            engine,
            &nullable_varbinary_nulls_relation(),
            &InputOptions::default(),
            RawParserConfig {
                mode: RawParserMode::Blob,
                column_name: Some("data".to_string()),
            },
            None,
            &[r#"{"data": "666f6f0a626172", "kafka_topic": null}"#],
        );
    }
}
