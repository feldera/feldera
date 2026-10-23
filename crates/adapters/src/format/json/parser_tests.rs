//! The JSON parser: object and array framing, the raw, insert/delete, and Debezium update
//! formats, record errors, and connector metadata.

use crate::test::engine_test;
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::panic::Location;

use chrono::DateTime;
use feldera_adapterlib::ConnectorMetadata;
use feldera_adapterlib::format::{InputBuffer, ParseError, Parser};
use feldera_adapterlib::test::{ConnectorTestEngine, InputOptions, Updates};
use feldera_types::config::FormatConfig;
use feldera_types::format::json::{JsonFlavor, JsonLines, JsonParserConfig, JsonUpdateFormat};
use feldera_types::program_schema::{ColumnType, Field, Relation};
use feldera_types::variant::VariantValue;
use proptest::prelude::*;

use crate::test::{parser, relations, render, rows};

use JsonLines::{Multiple, Single};
use JsonUpdateFormat::{Debezium, InsertDelete, Raw};

/// The table that most cases parse into.
pub fn relation() -> Relation {
    Relation::new(
        "json_input".into(),
        vec![
            Field::new("b".into(), ColumnType::boolean(false)),
            Field::new("i".into(), ColumnType::int(false)),
            Field::new("s".into(), ColumnType::varchar(true)),
        ],
        false,
        BTreeMap::new(),
    )
}

/// A table whose columns other than `i` take their values from the Kafka connector
/// attributes of the same names, as [`metadata_options`] declares.
pub fn metadata_relation() -> Relation {
    Relation::new(
        "json_metadata".into(),
        vec![
            Field::new("i".into(), ColumnType::int(false)),
            Field::new("kafka_headers".into(), ColumnType::variant(false)),
            Field::new("kafka_topic".into(), ColumnType::varchar(false)),
            Field::new("kafka_timestamp".into(), ColumnType::timestamp(false)),
            Field::new("kafka_partition".into(), ColumnType::int(false)),
            Field::new("kafka_offset".into(), ColumnType::bigint(false)),
        ],
        false,
        BTreeMap::new(),
    )
}

/// The input options of [`metadata_relation`]: each Kafka column's DEFAULT reads the
/// attribute of the same name.
pub fn metadata_options() -> InputOptions {
    InputOptions {
        soft_delete: false,
        attribute_defaults: [
            "kafka_headers",
            "kafka_topic",
            "kafka_timestamp",
            "kafka_partition",
            "kafka_offset",
        ]
        .into_iter()
        .map(|name| (name.to_string(), name.to_string()))
        .collect(),
    }
}

/// One case: the chunks that the parser reads, each with the errors that it reports, and
/// the updates that the table receives, in order.
struct Case {
    location: &'static Location<'static>,
    config: JsonParserConfig,
    chunks: Vec<(&'static str, Vec<ParseError>)>,
    updates: Vec<(i64, &'static str)>,
    metadata: Option<ConnectorMetadata>,
}

impl Case {
    #[track_caller]
    fn new(
        config: JsonParserConfig,
        chunks: Vec<(&'static str, Vec<ParseError>)>,
        updates: Vec<(i64, &'static str)>,
    ) -> Self {
        Self {
            location: Location::caller(),
            config,
            chunks,
            updates,
            metadata: None,
        }
    }

    fn with_metadata(self, metadata: ConnectorMetadata) -> Self {
        Self {
            metadata: Some(metadata),
            ..self
        }
    }
}

/// A JSON parser configuration of the default flavor.
fn config(update_format: JsonUpdateFormat, array: bool, lines: JsonLines) -> JsonParserConfig {
    JsonParserConfig {
        update_format,
        json_flavor: JsonFlavor::Default,
        array,
        lines,
    }
}

/// The error for a chunk that is not JSON.
fn document_error(message: &str, text: &str) -> ParseError {
    ParseError::text_envelope_error(message.to_string(), text, None)
}

/// The error for record number `event`, which does not fit the table.
fn record_error(message: &str, event: u64, field: Option<&str>, text: &str) -> ParseError {
    ParseError::new(
        message.to_string(),
        Some(event),
        field.map(str::to_string),
        Some(text),
        None,
        None,
        Some("text_event_err".to_string()),
    )
}

/// Runs each case on a fresh input collection of `relation`.
fn run(
    engine: &dyn ConnectorTestEngine,
    relation: &Relation,
    options: &InputOptions,
    cases: Vec<Case>,
) {
    for case in cases {
        let collection = engine.input_collection(relation, options).unwrap();
        let format = FormatConfig {
            name: Cow::from("json"),
            config: serde_json::to_value(&case.config).unwrap(),
        };
        let mut parser = parser(collection.as_ref(), &format);
        parser.on_error(Some(Box::new(|_, _| {})));
        for (chunk, errors) in &case.chunks {
            let (mut buffer, reported) = parser.parse(chunk.as_bytes(), case.metadata.clone());
            assert_eq!(&reported, errors, "{}", case.location);
            buffer.flush().unwrap();
        }
        let expected = Updates::from_json(collection.schema(), &case.updates).unwrap();
        assert_eq!(collection.updates().unwrap(), expected, "{}", case.location);
    }
}

/// Raw rows, alone or several to a chunk, as objects or as arrays of column values.
#[engine_test(requires(JsonArrayRecords))]
pub fn raw_records(engine: &dyn ConnectorTestEngine) {
    run(
        engine,
        &relation(),
        &InputOptions::default(),
        vec![
            Case::new(
                config(Raw, false, Single),
                vec![(r#"{"b": true, "i": 0}"#, vec![])],
                vec![(1, r#"{"b": true, "i": 0}"#)],
            ),
            Case::new(
                config(Raw, false, Multiple),
                vec![("{\n    \"b\": true,\n    \"i\": 0\n}\n", vec![])],
                vec![(1, r#"{"b": true, "i": 0}"#)],
            ),
            Case::new(
                config(Raw, false, Single),
                vec![(r#"[true, 0, "a"]"#, vec![])],
                vec![(1, r#"{"b": true, "i": 0, "s": "a"}"#)],
            ),
            Case::new(
                config(Raw, true, Single),
                vec![(r#"[{"b": true, "i": 0}]"#, vec![])],
                vec![(1, r#"{"b": true, "i": 0}"#)],
            ),
            Case::new(
                config(Raw, true, Single),
                vec![(r#"[[true, 0, "b"]]"#, vec![])],
                vec![(1, r#"{"b": true, "i": 0, "s": "b"}"#)],
            ),
            // Two records in one chunk.
            Case::new(
                config(Raw, false, Single),
                vec![(
                    r#"{"b": true, "i": 0}{"b": false, "i": 100, "s": "foo"}"#,
                    vec![],
                )],
                vec![
                    (1, r#"{"b": true, "i": 0}"#),
                    (1, r#"{"b": false, "i": 100, "s": "foo"}"#),
                ],
            ),
            Case::new(
                config(Raw, false, Single),
                vec![(r#"[true, 0, "c"][false, 100, "foo"]"#, vec![])],
                vec![
                    (1, r#"{"b": true, "i": 0, "s": "c"}"#),
                    (1, r#"{"b": false, "i": 100, "s": "foo"}"#),
                ],
            ),
            Case::new(
                config(Raw, true, Single),
                vec![(
                    r#"[{"b": true, "i": 0},{"b": false, "i": 100, "s": "foo"}]"#,
                    vec![],
                )],
                vec![
                    (1, r#"{"b": true, "i": 0}"#),
                    (1, r#"{"b": false, "i": 100, "s": "foo"}"#),
                ],
            ),
            Case::new(
                config(Raw, true, Single),
                vec![(r#"[[true, 0, "d"],[false, 100, "foo"]]"#, vec![])],
                vec![
                    (1, r#"{"b": true, "i": 0, "s": "d"}"#),
                    (1, r#"{"b": false, "i": 100, "s": "foo"}"#),
                ],
            ),
            // Two chunks of one record each.
            Case::new(
                config(Raw, false, Single),
                vec![
                    (r#"{"b": true, "i": 0}"#, vec![]),
                    (r#"{"b": false, "i": 100, "s": "foo"}"#, vec![]),
                ],
                vec![
                    (1, r#"{"b": true, "i": 0}"#),
                    (1, r#"{"b": false, "i": 100, "s": "foo"}"#),
                ],
            ),
            Case::new(
                config(Raw, false, Single),
                vec![
                    (r#"[true, 0, "e"]"#, vec![]),
                    (r#"[false, 100, "foo"]"#, vec![]),
                ],
                vec![
                    (1, r#"{"b": true, "i": 0, "s": "e"}"#),
                    (1, r#"{"b": false, "i": 100, "s": "foo"}"#),
                ],
            ),
            Case::new(
                config(Raw, true, Single),
                vec![
                    (r#"[{"b": true, "i": 0}]"#, vec![]),
                    (r#"[{"b": false, "i": 100, "s": "foo"}]"#, vec![]),
                ],
                vec![
                    (1, r#"{"b": true, "i": 0}"#),
                    (1, r#"{"b": false, "i": 100, "s": "foo"}"#),
                ],
            ),
            Case::new(
                config(Raw, true, Single),
                vec![
                    (r#"[[true, 0, "e"]]"#, vec![]),
                    (r#"[[false, 100, "foo"]]"#, vec![]),
                ],
                vec![
                    (1, r#"{"b": true, "i": 0, "s": "e"}"#),
                    (1, r#"{"b": false, "i": 100, "s": "foo"}"#),
                ],
            ),
        ],
    );
}

/// Raw input that is an insert/delete envelope, that is not JSON, or whose records do not
/// fit the table. A record that does not fit is dropped, along with the rest of its array.
#[engine_test(requires(SerdeParseErrors))]
pub fn raw_errors(engine: &dyn ConnectorTestEngine) {
    let envelope_hint = |text: &str| {
        ParseError::text_envelope_error(
            "payload looks like an insert/delete envelope; the configured `raw` update format expects plain rows".to_string(),
            text,
            Some(Cow::from(
                "Set `format.config.update_format` to `insert_delete` for payloads like {\"insert\": {...}} or {\"delete\": {...}}.",
            )),
        )
    };
    run(
        engine,
        &relation(),
        &InputOptions::default(),
        vec![
            Case::new(
                config(Raw, false, Single),
                vec![(
                    r#"{"insert": {"b": true, "i": 0}}"#,
                    vec![envelope_hint(r#"{"insert": {"b": true, "i": 0}}"#)],
                )],
                vec![],
            ),
            Case::new(
                config(Raw, true, Single),
                vec![(
                    r#"[{"insert": {"b": true, "i": 0}}]"#,
                    vec![envelope_hint(r#"[{"insert": {"b": true, "i": 0}}]"#)],
                )],
                vec![],
            ),
            // Not JSON.
            Case::new(
                config(Raw, false, Single),
                vec![
                    (r#"{"b": true, "i": 0}"#, vec![]),
                    (
                        r#"{"b": false, "i": 100, "s":"#,
                        vec![document_error(
                            "failed to parse string as a JSON document: EOF while parsing a value at line 1 column 27",
                            r#"{"b": false, "i": 100, "s":"#,
                        )],
                    ),
                ],
                vec![(1, r#"{"b": true, "i": 0}"#)],
            ),
            Case::new(
                config(Raw, false, Single),
                vec![
                    (r#"[true, 0, "f"]"#, vec![]),
                    (
                        r#"[false, 100, "#,
                        vec![document_error(
                            "failed to parse string as a JSON document: EOF while parsing a value at line 1 column 13",
                            r#"[false, 100, "#,
                        )],
                    ),
                ],
                vec![(1, r#"{"b": true, "i": 0, "s": "f"}"#)],
            ),
            Case::new(
                config(Raw, true, Single),
                vec![
                    (r#"[{"b": true, "i": 0}]"#, vec![]),
                    (
                        r#"[{"b": false, "i": 100, "s":"#,
                        vec![document_error(
                            "failed to parse string as a JSON document: EOF while parsing a value at line 1 column 28",
                            r#"[{"b": false, "i": 100, "s":"#,
                        )],
                    ),
                ],
                vec![(1, r#"{"b": true, "i": 0}"#)],
            ),
            Case::new(
                config(Raw, true, Single),
                vec![
                    (r#"[[true, 0, "g"]]"#, vec![]),
                    (
                        r#"[[false, 100, "s":"#,
                        vec![document_error(
                            "failed to parse string as a JSON document: expected `,` or `]` at line 1 column 18",
                            r#"[[false, 100, "s":"#,
                        )],
                    ),
                ],
                vec![(1, r#"{"b": true, "i": 0, "s": "g"}"#)],
            ),
            // JSON whose records do not fit the table.
            Case::new(
                config(Raw, false, Single),
                vec![
                    (r#"{"b": true, "i": 0}"#, vec![]),
                    (
                        r#"{"b": false, "i": 5}{"b": false}{"b": false, "i": "hello"}"#,
                        vec![
                            record_error(
                                "failed to deserialize JSON record: missing field `i` at line 1 column 12",
                                3,
                                None,
                                r#"{"b": false}"#,
                            ),
                            record_error(
                                "failed to deserialize JSON record: error parsing field 'i': invalid type: string \"hello\", expected i32 at line 1 column 25",
                                4,
                                Some("i"),
                                r#"{"b": false, "i": "hello"}"#,
                            ),
                        ],
                    ),
                ],
                vec![
                    (1, r#"{"b": true, "i": 0}"#),
                    (1, r#"{"b": false, "i": 5}"#),
                ],
            ),
            Case::new(
                config(Raw, true, Single),
                vec![
                    (r#"[{"b": true, "i": 0}]"#, vec![]),
                    (
                        r#"[{"b": false, "i": 5},{"b": false}]"#,
                        vec![record_error(
                            "failed to deserialize JSON record: missing field `i` at line 1 column 12",
                            3,
                            None,
                            r#"{"b": false}"#,
                        )],
                    ),
                    (
                        r#"[{"b": false, "i": 5},{"b": 20, "i": 10}]"#,
                        vec![record_error(
                            "failed to deserialize JSON record: error parsing field 'b': invalid type: integer `20`, expected a boolean at line 1 column 8",
                            5,
                            Some("b"),
                            r#"{"b": 20, "i": 10}"#,
                        )],
                    ),
                ],
                vec![(1, r#"{"b": true, "i": 0}"#)],
            ),
            Case::new(
                config(Raw, true, Single),
                vec![
                    (r#"[[true, 0, "h"]]"#, vec![]),
                    (
                        r#"[{"b": false, "i": 5},[false]]"#,
                        vec![record_error(
                            "failed to deserialize JSON record: invalid length 1, expected 3 columns at line 1 column 7",
                            3,
                            None,
                            "[false]",
                        )],
                    ),
                ],
                vec![(1, r#"{"b": true, "i": 0, "s": "h"}"#)],
            ),
        ],
    );
}

/// Inserts and deletes in the insert/delete format, alone or several to a chunk.
#[engine_test(requires(JsonArrayRecords))]
pub fn insert_delete_records(engine: &dyn ConnectorTestEngine) {
    run(
        engine,
        &relation(),
        &InputOptions::default(),
        vec![
            Case::new(
                config(InsertDelete, false, Single),
                vec![(r#"{"insert": {"b": true, "i": 0}}"#, vec![])],
                vec![(1, r#"{"b": true, "i": 0}"#)],
            ),
            Case::new(
                config(InsertDelete, false, Multiple),
                vec![(
                    "\n{\n    \"insert\": {\n        \"b\": true,\n        \"i\": 0\n    }\n}\n",
                    vec![],
                )],
                vec![(1, r#"{"b": true, "i": 0}"#)],
            ),
            Case::new(
                config(InsertDelete, true, Single),
                vec![(r#"[{"insert": {"b": true, "i": 0}}]"#, vec![])],
                vec![(1, r#"{"b": true, "i": 0}"#)],
            ),
            // Two records in one chunk.
            Case::new(
                config(InsertDelete, false, Single),
                vec![(
                    r#"{"insert": {"b": true, "i": 0}}{"delete": {"b": false, "i": 100, "s": "foo"}}"#,
                    vec![],
                )],
                vec![
                    (1, r#"{"b": true, "i": 0}"#),
                    (-1, r#"{"b": false, "i": 100, "s": "foo"}"#),
                ],
            ),
            Case::new(
                config(InsertDelete, true, Single),
                vec![(
                    r#"[{"insert": {"b": true, "i": 0}}, {"delete": {"b": false, "i": 100, "s": "foo"}}]"#,
                    vec![],
                )],
                vec![
                    (1, r#"{"b": true, "i": 0}"#),
                    (-1, r#"{"b": false, "i": 100, "s": "foo"}"#),
                ],
            ),
            Case::new(
                config(InsertDelete, true, Single),
                vec![(
                    r#"[{"insert": [true, 0, "a"]}, {"delete": {"b": false, "i": 100, "s": "foo"}}]"#,
                    vec![],
                )],
                vec![
                    (1, r#"{"b": true, "i": 0, "s": "a"}"#),
                    (-1, r#"{"b": false, "i": 100, "s": "foo"}"#),
                ],
            ),
            // Two chunks of one record each.
            Case::new(
                config(InsertDelete, false, Single),
                vec![
                    (r#"{"insert": {"b": true, "i": 0}}"#, vec![]),
                    (r#"{"delete": {"b": false, "i": 100, "s": "foo"}}"#, vec![]),
                ],
                vec![
                    (1, r#"{"b": true, "i": 0}"#),
                    (-1, r#"{"b": false, "i": 100, "s": "foo"}"#),
                ],
            ),
        ],
    );
}

/// Insert/delete input that is not JSON, that is not an envelope, or whose records do not
/// fit the table.
#[engine_test(requires(SerdeParseErrors))]
pub fn insert_delete_errors(engine: &dyn ConnectorTestEngine) {
    let missing_i = |event| {
        record_error(
            "failed to deserialize JSON record: missing field `i` at line 1 column 12",
            event,
            None,
            r#"{"b": false}"#,
        )
    };
    run(engine, &relation(), &InputOptions::default(), vec![
        // Not JSON.
        Case::new(
            config(InsertDelete, false, Single),
            vec![
                (r#"{"insert": {"b": true, "i": 0}}"#, vec![]),
                (r#"{"delete": {"b": false, "i": 100, "s":"#, vec![document_error("failed to parse string as a JSON document: EOF while parsing a value at line 1 column 38", r#"{"delete": {"b": false, "i": 100, "s":"#)]),
            ],
            vec![(1, r#"{"b": true, "i": 0}"#)],
        ),
        Case::new(
            config(InsertDelete, true, Single),
            vec![
                (r#"[{"insert": {"b": true, "i": 0}}]"#, vec![]),
                (r#"[{"delete": {"b": false, "i": 100, "s":"#, vec![document_error("failed to parse string as a JSON document: EOF while parsing a value at line 1 column 39", r#"[{"delete": {"b": false, "i": 100, "s":"#)]),
            ],
            vec![(1, r#"{"b": true, "i": 0}"#)],
        ),
        // JSON whose records do not fit the table.
        Case::new(
            config(InsertDelete, false, Single),
            vec![
                (r#"{"insert": {"b": true, "i": 0}}"#, vec![]),
                (r#"{"insert": {"b": false, "i": 5}}{"delete": {"b": false}}"#, vec![missing_i(3)]),
            ],
            vec![(1, r#"{"b": true, "i": 0}"#), (1, r#"{"b": false, "i": 5}"#)],
        ),
        Case::new(
            config(InsertDelete, true, Single),
            vec![
                (r#"[{"insert": {"b": true, "i": 0}}]"#, vec![]),
                (r#"[{"insert": {"b": false, "i": 5}},{"delete": {"b": false}}]"#, vec![missing_i(3)]),
            ],
            vec![(1, r#"{"b": true, "i": 0}"#)],
        ),
        Case::new(
            config(InsertDelete, true, Single),
            vec![
                (r#"[{"insert": {"b": true, "i": 0}}]"#, vec![]),
                (r#"[{"insert": {"b": false, "i": 5}},{"delete": {"b": false}}]"#, vec![missing_i(3)]),
                (r#"[{"insert": {"b": true, "i": 0}}]"#, vec![]),
                (r#"[{"delete": {"b": false}}]"#, vec![missing_i(5)]),
                (r#"[{"b": false}]"#, vec![ParseError::text_envelope_error(
                    "error deserializing string as a JSON array of updates: unknown field `b`, expected one of `table`, `insert`, `delete`, `update` at line 1 column 5".to_string(),
                    r#"[{"b": false}]"#,
                    Some(Cow::from("Example valid JSON: '[{{\"insert\": {{...}} }}, {{\"delete\": {{...}} }}]'")),
                )]),
            ],
            vec![(1, r#"{"b": true, "i": 0}"#), (1, r#"{"b": true, "i": 0}"#)],
        ),
    ]);
}

/// Debezium change events: creates, updates as a delete and an insert, and deletes.
#[engine_test(requires(JsonArrayRecords))]
pub fn debezium_records(engine: &dyn ConnectorTestEngine) {
    run(
        engine,
        &relation(),
        &InputOptions::default(),
        vec![
            Case::new(
                config(Debezium, false, Single),
                vec![(
                    r#"{"payload": {"op": "c", "after": {"b": true, "i": 0}}}"#,
                    vec![],
                )],
                vec![(1, r#"{"b": true, "i": 0}"#)],
            ),
            Case::new(
                config(Debezium, false, Single),
                vec![(
                    r#"{"payload": {"op": "u", "before": {"b": true, "i": 123}, "after": {"b": true, "i": 0}}}"#,
                    vec![],
                )],
                vec![
                    (-1, r#"{"b": true, "i": 123}"#),
                    (1, r#"{"b": true, "i": 0}"#),
                ],
            ),
            Case::new(
                config(Debezium, false, Single),
                vec![(
                    r#"{"payload": {"op": "u", "before": [true, 123, "abc"], "after": [true, 0, "def"]}}"#,
                    vec![],
                )],
                vec![
                    (-1, r#"{"b": true, "i": 123, "s": "abc"}"#),
                    (1, r#"{"b": true, "i": 0, "s": "def"}"#),
                ],
            ),
            // Two records in one chunk.
            Case::new(
                config(Debezium, false, Single),
                vec![(
                    r#"{"payload": {"op": "c", "after": {"b": true, "i": 0}}}{"payload": {"op": "d", "before": {"b": false, "i": 100, "s": "foo"}}}"#,
                    vec![],
                )],
                vec![
                    (1, r#"{"b": true, "i": 0}"#),
                    (-1, r#"{"b": false, "i": 100, "s": "foo"}"#),
                ],
            ),
            // Two chunks of one record each.
            Case::new(
                config(Debezium, false, Single),
                vec![
                    (
                        r#"{"payload": {"op": "c", "after": {"b": true, "i": 0}}}"#,
                        vec![],
                    ),
                    (
                        r#"{"payload": {"op": "d", "before": {"b": false, "i": 100, "s": "foo"}}}"#,
                        vec![],
                    ),
                ],
                vec![
                    (1, r#"{"b": true, "i": 0}"#),
                    (-1, r#"{"b": false, "i": 100, "s": "foo"}"#),
                ],
            ),
        ],
    );
}

/// Debezium input that is not JSON, or whose records do not fit the table.
#[engine_test(requires(SerdeParseErrors))]
pub fn debezium_errors(engine: &dyn ConnectorTestEngine) {
    run(
        engine,
        &relation(),
        &InputOptions::default(),
        vec![
            Case::new(
                config(Debezium, false, Single),
                vec![
                    (
                        r#"{"payload": {"op": "c", "after": {"b": true, "i": 0}}}"#,
                        vec![],
                    ),
                    (
                        r#"{"payload": {"op": "d", "before": {"b": false, "i": 100, "s":"#,
                        vec![document_error(
                            "failed to parse string as a JSON document: EOF while parsing a value at line 1 column 61",
                            r#"{"payload": {"op": "d", "before": {"b": false, "i": 100, "s":"#,
                        )],
                    ),
                ],
                vec![(1, r#"{"b": true, "i": 0}"#)],
            ),
            Case::new(
                config(Debezium, false, Single),
                vec![
                    (
                        r#"{"payload": {"op": "c", "after": {"b": true, "i": 0}}}"#,
                        vec![],
                    ),
                    (
                        r#"{"payload": {"op": "c", "after": {"b": false, "i": 5}}}{"payload": {"op": "d", "before": {"b": false}}}"#,
                        vec![record_error(
                            "failed to deserialize JSON record: missing field `i` at line 1 column 12",
                            3,
                            None,
                            r#"{"b": false}"#,
                        )],
                    ),
                ],
                vec![
                    (1, r#"{"b": true, "i": 0}"#),
                    (1, r#"{"b": false, "i": 5}"#),
                ],
            ),
        ],
    );
}

/// Columns whose DEFAULTs read connector attributes take them from the metadata that the
/// transport attaches to the chunk.
#[engine_test]
pub fn metadata(engine: &dyn ConnectorTestEngine) {
    let mut metadata = ConnectorMetadata::new();
    metadata.insert("kafka_headers", VariantValue::Object(BTreeMap::new()));
    metadata.insert("kafka_topic", VariantValue::String("my_topic".into()));
    metadata.insert(
        "kafka_timestamp",
        VariantValue::TimestampNtzMicros(
            DateTime::from_timestamp_millis(1763626606441)
                .unwrap()
                .naive_utc(),
        ),
    );
    metadata.insert("kafka_partition", VariantValue::Int32(10));
    metadata.insert("kafka_offset", VariantValue::Int32(1_000_000));

    run(engine, &metadata_relation(), &metadata_options(), vec![
        Case::new(
            config(Raw, false, Single),
            vec![(r#"{"i": 0}"#, vec![])],
            vec![(1, r#"{"i": 0, "kafka_headers": "{}", "kafka_topic": "my_topic", "kafka_timestamp": "2025-11-20T08:16:46.441", "kafka_partition": 10, "kafka_offset": 1000000}"#)],
        )
        .with_metadata(metadata),
    ]);
}

/// The table of [`random_rows`]: a column of each type that JSON holds.
pub fn every_type_relation() -> Relation {
    relations::every_type("json_every_type")
}

/// Random rows of every type, written one object per line in the default flavor, read back
/// unchanged and in order.
#[engine_test]
pub fn random_rows(engine: &dyn ConnectorTestEngine) {
    let relation = every_type_relation();
    let format = FormatConfig {
        name: Cow::from("json"),
        config: serde_json::to_value(config(Raw, false, Multiple)).unwrap(),
    };
    proptest!(ProptestConfig::with_cases(32), |(batches in rows::batches(&relation, "bigint", 1..=1, 20))| {
        let rows = &batches[0];
        let collection = engine
            .input_collection(&relation, &InputOptions::default())
            .unwrap();
        let mut parser = parser(collection.as_ref(), &format);
        let (mut buffer, errors) =
            parser.parse(render::json_lines(&relation, rows.rows()).as_bytes(), None);
        prop_assert_eq!(errors, vec![]);
        buffer.flush().unwrap();
        prop_assert_eq!(&collection.updates().unwrap(), rows);
    });
}
