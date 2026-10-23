//! The raw parser: each line of a message, or the whole message, as one column of a row,
//! with connector attributes in the other columns.

use crate::test::engine_test;
use std::borrow::Cow;
use std::collections::BTreeMap;

use feldera_adapterlib::ConnectorMetadata;
use feldera_adapterlib::format::{InputBuffer, Parser};
use feldera_adapterlib::test::{ConnectorTestEngine, InputOptions, Updates};
use feldera_types::config::FormatConfig;
use feldera_types::format::raw::{RawParserConfig, RawParserMode};
use feldera_types::program_schema::{ColumnType, Field, Relation};
use feldera_types::variant::VariantValue;

use crate::test::parser;

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
        attribute_defaults: vec![("kafka_topic".to_string(), "kafka_topic".to_string())],
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
