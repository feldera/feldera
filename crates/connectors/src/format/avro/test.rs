//! Unit tests of the Avro schema validation that need no engine. The connector tests in
//! `parser_tests` and `encoder_tests` cover the format with an engine.

use crate::{
    format::{
        avro::{output::AvroEncoder, schema::schema_json},
        get_input_format,
    },
    test::{
        avro::{self, Names},
        engine_test, parser, published_schema, read, relations, rows,
    },
};
#[cfg(test)]
use apache_avro::schema::ResolvedSchema;
use apache_avro::{Schema as AvroSchema, from_avro_datum, types::Value};
use arrow::{
    array::{AsArray, BooleanArray, RecordBatch},
    compute::{concat_batches, not},
};
use feldera_adapterlib::{
    catalog::SerBatchReader,
    format::{Encoder, InputBuffer, ParseError, Parser},
    test::{ConnectorTestEngine, InputOptions, MockOutputConsumer, Updates, ZSet},
    transport::OutputBatchType,
};
use feldera_types::{
    config::FormatConfig,
    format::avro::{AvroEncoderConfig, AvroEncoderKeyMode, AvroParserConfig, AvroUpdateFormat},
    program_schema::{ColumnType, Field, Relation, SqlIdentifier},
};
use num_bigint::BigInt;
use proptest::{
    collection::{btree_map, vec},
    prelude::*,
    strategy::ValueTree,
    test_runner::TestRunner,
};
use serde_json::Value as JsonValue;
use std::{
    borrow::Cow,
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

/// The Avro schema of [`assorted_relation`].
pub const ASSORTED_SCHEMA: &str = r#"{
    "type": "record",
    "name": "TestStruct2",
    "connect.name": "test_namespace.TestStruct2",
    "fields": [
        { "name": "id", "type": "long" },
        { "name": "nAmE", "type": ["string", "null"] },
        { "name": "b", "type": "boolean" },
        { "name": "ts", "type": "long", "logicalType": "timestamp-micros" },
        { "name": "dt", "type": "int", "logicalType": "date" },
        {
            "name": "es",
            "type": [{
                "type": "record",
                "name": "EmbeddedStruct",
                "fields": [{ "name": "a", "type": "boolean" }]
            }, "null"]
        },
        { "name": "m", "type": [{ "type": "map", "values": "long" }, "null"] },
        {
            "name": "dec",
            "type": { "type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 3 }
        }
    ]
}"#;

/// A table of assorted types.
pub fn assorted_relation() -> Relation {
    relations::assorted("avro_assorted")
}

/// Two rows of [`assorted_relation`].
const ASSORTED_ROWS: [&str; 2] = [
    r#"{"id": 1, "nAmE": "test", "b": false, "ts": "1970-01-01T00:00:01", "dt": "1970-01-02", "es": {"a": false}, "m": {"foo": 100, "bar": 200}, "dec": "10.000"}"#,
    r#"{"id": 2, "nAmE": null, "b": true, "ts": "1970-01-01T00:00:02", "dt": "1970-01-13", "es": {"a": true}, "m": {}, "dec": "0.001"}"#,
];

/// A table with soft deletes.
pub fn soft_delete_relation() -> Relation {
    relations::soft_delete("avro_soft_delete")
}

/// A table with one BIGINT column, `id`.
pub fn id_relation() -> Relation {
    relations::basic_key("avro_id")
}

/// The table of the `cdc_field` cases: `id BIGINT`, the primary key, and `s VARCHAR`.
pub fn cdc_field_relation() -> Relation {
    Relation::new(
        "avro_cdc_field".into(),
        vec![
            Field::new("id".into(), ColumnType::bigint(false)),
            Field::new("s".into(), ColumnType::varchar(false)),
        ],
        false,
        BTreeMap::new(),
    )
    .with_primary_key(&[SqlIdentifier::from("id")])
}

/// A table of a fixed-size and a variable-size binary column.
pub fn binary_relation() -> Relation {
    Relation::new(
        "avro_binary".into(),
        vec![
            Field::new("binary32".into(), ColumnType::fixed(32, false)),
            Field::new("varbinary".into(), ColumnType::varbinary(false)),
        ],
        false,
        BTreeMap::new(),
    )
}

/// A table of two UUID columns and a VARCHAR column.
pub fn uuid_relation() -> Relation {
    Relation::new(
        "avro_uuid".into(),
        vec![
            Field::new("uuid1".into(), ColumnType::uuid(false)),
            Field::new("uuid2".into(), ColumnType::uuid(false)),
            Field::new("varchar".into(), ColumnType::varchar(false)),
        ],
        false,
        BTreeMap::new(),
    )
}

/// A table of two VARIANT columns.
pub fn variant_relation() -> Relation {
    Relation::new(
        "avro_variant".into(),
        vec![
            Field::new("v1".into(), ColumnType::variant(false)),
            Field::new("v2".into(), ColumnType::variant(false)),
        ],
        false,
        BTreeMap::new(),
    )
}

/// The Avro schema of [`variant_relation`]: each VARIANT is JSON in an Avro string.
pub const VARIANT_SCHEMA: &str = r#"{
    "type": "record",
    "name": "TestVariant",
    "connect.name": "test_namespace.TestVariant",
    "fields": [
        { "name": "v1", "type": "string" },
        { "name": "v2", "type": "string" }
    ]
}"#;

/// A JSON value with an integer, a string, a NULL, a float, and a boolean.
pub const VARIANT_JSON: &str = r#"{"a": 1, "b": ["x", null], "c": 2.5, "ok": true}"#;

/// The row of [`variant_relation`] whose VARIANTs both hold [`VARIANT_JSON`].
pub const VARIANT_ROW: &str = r#"{"v1": "{\"a\":1,\"b\":[\"x\",null],\"c\":2.5,\"ok\":true}", "v2": "{\"a\":1,\"b\":[\"x\",null],\"c\":2.5,\"ok\":true}"}"#;

/// A table of one VARCHAR column that an Avro enum fills.
pub fn enum_relation() -> Relation {
    Relation::new(
        "avro_enum".into(),
        vec![Field::new("enum_val".into(), ColumnType::varchar(false))],
        false,
        BTreeMap::new(),
    )
}

/// A table whose `schema_id` takes the Avro schema id, as [`metadata_options`] declares.
pub fn metadata_relation() -> Relation {
    Relation::new(
        "avro_metadata".into(),
        vec![
            Field::new("id".into(), ColumnType::bigint(false)),
            Field::new("schema_id".into(), ColumnType::uint(true)),
        ],
        false,
        BTreeMap::new(),
    )
}

/// The input options of [`metadata_relation`]: the DEFAULT of `schema_id` reads the
/// attribute `avro_schema_id`.
pub fn metadata_options() -> InputOptions {
    InputOptions {
        soft_delete: false,
        attribute_defaults: BTreeMap::from([(
            "schema_id".to_string(),
            "avro_schema_id".to_string(),
        )]),
    }
}

/// A table of a column of each type that a Debezium temporal type fills.
pub fn temporal_relation() -> Relation {
    let column = |name: &str, column: ColumnType| Field::new(name.into(), column);
    Relation::new(
        "avro_temporal".into(),
        vec![
            column("id", ColumnType::bigint(false)),
            column("ts_millis", ColumnType::timestamp(false)),
            column("ts_micros", ColumnType::timestamp(false)),
            column("ts_nanos", ColumnType::timestamp(false)),
            column("ts_zoned", ColumnType::timestamp_tz(false)),
            column("ts_connect", ColumnType::timestamp(false)),
            column("ts_opt", ColumnType::timestamp(true)),
            column("t_millis", ColumnType::time(false)),
            column("t_micros", ColumnType::time(false)),
            column("t_nanos", ColumnType::time(false)),
            column("t_zoned", ColumnType::time(false)),
            column("t_connect", ColumnType::time(false)),
            column("dt", ColumnType::date(false)),
        ],
        false,
        BTreeMap::new(),
    )
}

/// A table of TIMESTAMP and TIMESTAMP WITH TIME ZONE columns that Debezium timestamps fill.
pub fn timestamp_targets_relation() -> Relation {
    Relation::new(
        "avro_timestamp_targets".into(),
        vec![
            Field::new("z_ts".into(), ColumnType::timestamp(false)),
            Field::new("z_tstz".into(), ColumnType::timestamp_tz(false)),
            Field::new("n_ts".into(), ColumnType::timestamp(false)),
            Field::new("n_tstz".into(), ColumnType::timestamp_tz(false)),
        ],
        false,
        BTreeMap::new(),
    )
}

/// A table of DECIMAL columns that Debezium variable-scale decimals fill.
pub fn decimal_relation() -> Relation {
    Relation::new(
        "avro_decimal".into(),
        vec![
            Field::new("id".into(), ColumnType::bigint(false)),
            Field::new("amount".into(), ColumnType::decimal(10, 3, false)),
            Field::new("amount_opt".into(), ColumnType::decimal(10, 3, true)),
        ],
        false,
        BTreeMap::new(),
    )
}

/// A table of integer columns of four widths and signs, which Avro `int` fields fill.
pub fn integers_relation() -> Relation {
    Relation::new(
        "avro_integers".into(),
        vec![
            Field::new("uint".into(), ColumnType::uint(false)),
            Field::new("ulong".into(), ColumnType::ubigint(false)),
            Field::new("int".into(), ColumnType::int(false)),
            Field::new("long".into(), ColumnType::bigint(false)),
        ],
        false,
        BTreeMap::new(),
    )
}

/// An Avro parser configuration with a static `schema`.
fn config(update_format: AvroUpdateFormat, schema: &str) -> AvroParserConfig {
    AvroParserConfig {
        update_format,
        schema: Some(schema.to_string()),
        skip_schema_id: false,
        cdc_field: None,
        registry_config: Default::default(),
    }
}

/// Returns `config` as the format of a parser.
fn format(config: &AvroParserConfig) -> FormatConfig {
    FormatConfig {
        name: Cow::from("avro"),
        config: serde_json::to_value(config).unwrap(),
    }
}

/// Returns `rows`, JSON objects with weights, in the published form of `relation`.
fn updates(relation: &Relation, rows: &[(i64, &str)]) -> Updates {
    Updates::from_json(published_schema(relation), rows).unwrap()
}

/// Parses `messages` with `config` into a fresh table of `relation`, and checks that no
/// message is an error and that the table receives `expected`, in order.
#[track_caller]
fn check(
    engine: &dyn ConnectorTestEngine,
    relation: &Relation,
    options: &InputOptions,
    config: &AvroParserConfig,
    messages: &[Vec<u8>],
    expected: &Updates,
) {
    let collection = engine.input_collection(relation, options).unwrap();
    let mut parser = parser(collection.as_ref(), &format(config));
    for message in messages {
        let (mut buffer, errors) = parser.parse(message, None);
        assert_eq!(errors, []);
        buffer.flush().unwrap();
    }
    assert_eq!(&collection.updates().unwrap(), expected);
}

/// Returns each row of `rows` as a message of the record `schema`.
fn record_messages(relation: &Relation, rows: &Updates, schema: &AvroSchema) -> Vec<Vec<u8>> {
    let names = avro::names(schema);
    (0..rows.len())
        .map(|index| {
            let record = avro::record(relation, rows.rows(), index, schema, &names).unwrap();
            avro::message(schema, record)
        })
        .collect()
}

/// Returns a Debezium envelope schema whose `before` and `after` hold `value_schema`, a record
/// named `value_type`, as Debezium writes it, with `connect.name` annotations.
fn debezium_schema_text(value_schema: &str, value_type: &str) -> String {
    // `after` comes before `before`, so that `before` refers to the record type by name.
    r#"{
    "type": "record",
    "name": "Envelope",
    "namespace": "test_namespace",
    "fields": [
        { "name": "after", "type": ["null", VALUE_SCHEMA], "default": null },
        { "name": "before", "type": ["null", "VALUE_TYPE"], "default": null },
        {
            "name": "source",
            "type": {
                "type": "record",
                "name": "Source",
                "namespace": "io.debezium.connector.postgresql",
                "fields": [
                    { "name": "version", "type": "string" },
                    { "name": "connector", "type": "string" },
                    { "name": "name", "type": "string" },
                    { "name": "ts_ms", "type": "long" },
                    { "name": "snapshot", "type": [ { "type": "string", "connect.version": 1, "connect.parameters": { "allowed": "true,last,false,incremental" }, "connect.default": "false", "connect.name": "io.debezium.data.Enum" }, "null" ], "default": "false" },
                    { "name": "db", "type": "string" },
                    { "name": "sequence", "type": [ "null", "string" ], "default": null },
                    { "name": "schema", "type": "string" },
                    { "name": "table", "type": "string" },
                    { "name": "txId", "type": [ "null", "long" ], "default": null },
                    { "name": "lsn", "type": [ "null", "long" ], "default": null },
                    { "name": "xmin", "type": [ "null", "long" ], "default": null }
                ],
                "connect.name": "io.debezium.connector.postgresql.Source"
            }
        },
        { "name": "op", "type": "string" },
        { "name": "ts_ms", "type": ["null", "long"], "default": null },
        {
            "name": "transaction",
            "type": [
                "null",
                {
                    "type": "record",
                    "name": "block",
                    "namespace": "event",
                    "fields": [
                        { "name": "id", "type": "string" },
                        { "name": "total_order", "type": "long" },
                        { "name": "data_collection_order", "type": "long" }
                    ],
                    "connect.version": 1,
                    "connect.name": "event.block"
                }
            ],
            "default": null
        }
    ],
    "connect.version": 1,
    "connect.name": "test_namespace.Envelope"
}"#
    .replace("VALUE_SCHEMA", value_schema)
    .replace("VALUE_TYPE", value_type)
}

/// The Debezium envelope schema of `value_schema`, and its named types.
struct Envelope {
    schema: AvroSchema,
    names: Names,
}

impl Envelope {
    fn new(text: &str) -> Self {
        let schema = AvroSchema::parse_str(text).unwrap();
        let names = avro::names(&schema);
        Self { schema, names }
    }

    /// Returns a message of an update with `before` and `after`.
    fn update(&self, before: Value, after: Value) -> Vec<u8> {
        self.message("u", Some(before), Some(after))
    }

    /// Returns a message of `op` with `before` and `after`.
    fn message(&self, op: &str, before: Option<Value>, after: Option<Value>) -> Vec<u8> {
        let event = avro::debezium(&self.schema, &self.names, op, before, after).unwrap();
        avro::message(&self.schema, event)
    }
}

/// Returns the parser configuration and the messages of Debezium updates whose before and
/// after are both a row of `rows`, a record `value_schema` named `value_type`, and the
/// updates that the table receives: a delete and an insert of each row.
fn debezium_updates(
    relation: &Relation,
    rows: &Updates,
    value_schema: &str,
    value_type: &str,
) -> (AvroParserConfig, Vec<Vec<u8>>, Updates) {
    let envelope = Envelope::new(&debezium_schema_text(value_schema, value_type));
    let value_schema = AvroSchema::parse_str(value_schema).unwrap();
    let names = avro::names(&value_schema);
    let messages = (0..rows.len())
        .map(|index| {
            let row = avro::record(relation, rows.rows(), index, &value_schema, &names).unwrap();
            envelope.update(row.clone(), row)
        })
        .collect();
    let indices: Vec<u32> = (0..rows.len() as u32)
        .flat_map(|index| [index, index])
        .collect();
    let weights = (0..rows.len()).flat_map(|_| [-1, 1]).collect();
    let expected = rows.take(&indices, weights).unwrap();
    let config = config(AvroUpdateFormat::Debezium, &schema_json(&envelope.schema));
    (config, messages, expected)
}

/// Raw records with optional fields, a record, a map, and a decimal.
#[engine_test]
pub fn raw_records(engine: &dyn ConnectorTestEngine) {
    let relation = assorted_relation();
    let rows = updates(&relation, &ASSORTED_ROWS.map(|row| (1, row)));
    let schema = AvroSchema::parse_str(ASSORTED_SCHEMA).unwrap();
    check(
        engine,
        &relation,
        &InputOptions::default(),
        &config(AvroUpdateFormat::Raw, ASSORTED_SCHEMA),
        &record_messages(&relation, &rows, &schema),
        &rows,
    );
}

/// A Debezium update deletes the record before and inserts the record after, in that order.
#[engine_test]
pub fn debezium_records(engine: &dyn ConnectorTestEngine) {
    let relation = assorted_relation();
    let rows = updates(&relation, &ASSORTED_ROWS.map(|row| (1, row)));
    let (config, messages, expected) =
        debezium_updates(&relation, &rows, ASSORTED_SCHEMA, "TestStruct2");
    check(
        engine,
        &relation,
        &InputOptions::default(),
        &config,
        &messages,
        &expected,
    );
}

/// With soft deletes, a Debezium delete is an insert with `is_delete` set.
#[engine_test]
pub fn soft_delete_debezium(engine: &dyn ConnectorTestEngine) {
    let value_schema = r#"{
        "type": "record",
        "name": "TestStructSoftDelete",
        "fields": [
            { "name": "id", "type": "long" },
            { "name": "s", "type": "string" }
        ]
    }"#;
    let envelope = Envelope::new(&debezium_schema_text(value_schema, "TestStructSoftDelete"));
    let record = || {
        Value::Record(vec![
            ("id".to_string(), Value::Long(1)),
            ("s".to_string(), Value::String("one".to_string())),
        ])
    };
    let relation = soft_delete_relation();
    check(
        engine,
        &relation,
        &relations::soft_delete_options(),
        &config(AvroUpdateFormat::Debezium, &schema_json(&envelope.schema)),
        &[
            envelope.message("c", None, Some(record())),
            envelope.message("d", Some(record()), None),
        ],
        &updates(
            &relation,
            &[
                (1, r#"{"id": 1, "s": "one", "is_delete": null}"#),
                (1, r#"{"id": 1, "s": "one", "is_delete": true}"#),
            ],
        ),
    );
}

/// A Debezium field that is not a column is skipped, even when Debezium marks it as a type
/// to convert and its value is NULL in one record and set in the other.
#[engine_test]
pub fn debezium_ignored_coerced_field(engine: &dyn ConnectorTestEngine) {
    let value_schema = r#"{
        "type": "record",
        "name": "IgnoredCoercedField",
        "connect.name": "test_namespace.IgnoredCoercedField",
        "fields": [
            { "name": "id", "type": "long" },
            {
                "name": "timestamp",
                "type": ["null", { "type": "string", "connect.name": "io.debezium.time.ZonedTimestamp" }],
                "default": null
            }
        ]
    }"#;
    let text = debezium_schema_text(value_schema, "IgnoredCoercedField");
    let envelope = Envelope::new(&text);
    let record = |id: i64, timestamp: Option<&str>| {
        Value::Record(vec![
            ("id".to_string(), Value::Long(id)),
            (
                "timestamp".to_string(),
                match timestamp {
                    None => Value::Union(0, Box::new(Value::Null)),
                    Some(timestamp) => {
                        Value::Union(1, Box::new(Value::String(timestamp.to_string())))
                    }
                },
            ),
        ])
    };
    let relation = id_relation();
    check(
        engine,
        &relation,
        &InputOptions::default(),
        &config(AvroUpdateFormat::Debezium, &text),
        &[envelope.update(
            record(1, None),
            record(2, Some("2021-06-15T14:30:45.123+02:00")),
        )],
        &updates(&relation, &[(-1, r#"{"id": 1}"#), (1, r#"{"id": 2}"#)]),
    );
}

/// Returns the Avro schema of [`cdc_field_relation`] with an `op` field of type `op_type`.
fn cdc_field_schema(op_type: &str) -> String {
    format!(
        r#"{{"type": "record", "name": "CdcField", "fields": [
            {{"name": "id", "type": "long"}},
            {{"name": "s", "type": "string"}},
            {{"name": "op", "type": {op_type}}}
        ]}}"#
    )
}

/// Returns the message of the row `id` of [`cdc_field_relation`] with the operation `op`.
fn cdc_field_message(id: i64, op: Value, schema: &AvroSchema) -> Vec<u8> {
    avro::message(
        schema,
        Value::Record(vec![
            ("id".to_string(), Value::Long(id)),
            ("s".to_string(), Value::String(format!("s{id}"))),
            ("op".to_string(), op),
        ]),
    )
}

/// Returns the row `id` of [`cdc_field_relation`] as JSON.
fn cdc_field_row(id: i64) -> String {
    format!(r#"{{"id": {id}, "s": "s{id}"}}"#)
}

/// A parser configuration whose `cdc_field` names the field of the operation.
fn cdc_field_config(
    update_format: AvroUpdateFormat,
    schema: &str,
    cdc_field: &str,
) -> AvroParserConfig {
    AvroParserConfig {
        cdc_field: Some(cdc_field.to_string()),
        ..config(update_format, schema)
    }
}

/// The field that `cdc_field` names selects an insert, an upsert, or a delete, whether it is
/// a string, a nullable string, or an enum.
#[engine_test]
pub fn cdc_field(engine: &dyn ConnectorTestEngine) {
    /// The Avro type of `op`, and a function that returns an `op` value.
    type OpType = (&'static str, fn(&str) -> Value);
    let op_types: [OpType; 3] = [
        (r#""string""#, |op| Value::String(op.to_string())),
        (r#"["null", "string"]"#, |op| {
            Value::Union(1, Box::new(Value::String(op.to_string())))
        }),
        (
            r#"{"type": "enum", "name": "Op", "symbols": ["I", "U", "D"]}"#,
            |op| {
                let index = ["I", "U", "D"].iter().position(|s| *s == op).unwrap();
                Value::Enum(index as u32, op.to_string())
            },
        ),
    ];
    let relation = cdc_field_relation();
    let (row1, row2) = (cdc_field_row(1), cdc_field_row(2));
    for (op_type, op) in op_types {
        let text = cdc_field_schema(op_type);
        let schema = AvroSchema::parse_str(&text).unwrap();
        check(
            engine,
            &relation,
            &InputOptions::default(),
            &cdc_field_config(AvroUpdateFormat::Raw, &text, "op"),
            &[(1, "I"), (2, "I"), (1, "U"), (2, "D")]
                .map(|(id, code)| cdc_field_message(id, op(code), &schema)),
            &updates(
                &relation,
                &[(1, &row1), (1, &row2), (1, &row1), (-1, &row2)],
            ),
        );
    }
}

/// An unknown or NULL operation is an error of that record only.
#[engine_test]
pub fn cdc_field_invalid_op(engine: &dyn ConnectorTestEngine) {
    let text = cdc_field_schema(r#"["null", "string"]"#);
    let schema = AvroSchema::parse_str(&text).unwrap();
    let relation = cdc_field_relation();
    let collection = engine
        .input_collection(&relation, &InputOptions::default())
        .unwrap();
    let mut parser = parser(
        collection.as_ref(),
        &format(&cdc_field_config(AvroUpdateFormat::Raw, &text, "op")),
    );
    parser.on_error(Some(Box::new(|_, _| {})));

    let string_op = |op: &str| Value::Union(1, Box::new(Value::String(op.to_string())));
    for (id, op, expected_error) in [
        (
            1,
            string_op("X"),
            Some("invalid value 'X' in the 'op' field"),
        ),
        (
            2,
            Value::Union(0, Box::new(Value::Null)),
            Some("invalid value 'Null' in the 'op' field"),
        ),
        (3, string_op("I"), None),
    ] {
        let (mut buffer, errors) = parser.parse(&cdc_field_message(id, op, &schema), None);
        match expected_error {
            Some(expected_error) => {
                assert_eq!(errors.len(), 1);
                assert!(
                    errors[0].to_string().contains(expected_error),
                    "expected '{expected_error}' in '{}'",
                    errors[0]
                );
            }
            None => assert_eq!(errors, Vec::<ParseError>::new()),
        }
        buffer.flush().unwrap();
    }
    assert_eq!(
        collection.updates().unwrap(),
        updates(&relation, &[(1, &cdc_field_row(3))])
    );
}

/// A parser refuses a `cdc_field` with an update format other than raw, an empty name, a
/// name that is not a field, or a field that is not a string or an enum.
#[engine_test]
pub fn cdc_field_config_errors(engine: &dyn ConnectorTestEngine) {
    let collection = engine
        .input_collection(&cdc_field_relation(), &InputOptions::default())
        .unwrap();
    let string_schema = cdc_field_schema(r#""string""#);
    for (config, expected_error) in [
        (
            cdc_field_config(AvroUpdateFormat::Debezium, &string_schema, "op"),
            "`cdc_field` is only supported with 'raw' update format",
        ),
        (
            cdc_field_config(AvroUpdateFormat::Raw, &string_schema, ""),
            "`cdc_field` must not be empty",
        ),
        (
            cdc_field_config(AvroUpdateFormat::Raw, &string_schema, "action"),
            "field 'action', specified in the 'cdc_field' property, is missing in the Avro schema",
        ),
        (
            cdc_field_config(AvroUpdateFormat::Raw, &cdc_field_schema(r#""int""#), "op"),
            "field 'op', specified in the 'cdc_field' property, must be of type 'string' or 'enum'",
        ),
    ] {
        let error = match get_input_format("avro").unwrap().new_parser(
            "test_endpoint",
            &collection.input_handle(),
            &serde_json::to_value(config).unwrap(),
        ) {
            Ok(_) => panic!("the parser accepts a configuration that it must refuse"),
            Err(error) => error.to_string(),
        };
        assert!(
            error.contains(expected_error),
            "expected '{expected_error}' in '{error}'"
        );
    }
}

/// A nullable column that the Avro schema lacks is NULL.
#[engine_test]
pub fn extra_columns(engine: &dyn ConnectorTestEngine) {
    let text = r#"{
        "type": "record",
        "name": "TestStruct2Short",
        "fields": [
            { "name": "id", "type": "long" },
            { "name": "b", "type": "boolean" },
            { "name": "ts", "type": "long", "logicalType": "timestamp-micros" },
            { "name": "dt", "type": "int", "logicalType": "date" },
            {
                "name": "es",
                "type": { "type": "record", "name": "EmbeddedStruct", "fields": [{ "name": "a", "type": "boolean" }] }
            },
            { "name": "m", "type": { "type": "map", "values": "long" } },
            {
                "name": "dec",
                "type": { "type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 3 }
            }
        ]
    }"#;
    let relation = assorted_relation();
    let rows = updates(&relation, &ASSORTED_ROWS.map(|row| (1, row)));
    let schema = AvroSchema::parse_str(text).unwrap();
    let expected: Vec<String> = ASSORTED_ROWS
        .iter()
        .map(|row| row.replace(r#""nAmE": "test""#, r#""nAmE": null"#))
        .collect();
    check(
        engine,
        &relation,
        &InputOptions::default(),
        &config(AvroUpdateFormat::Raw, text),
        &record_messages(&relation, &rows, &schema),
        &updates(
            &relation,
            &expected
                .iter()
                .map(|row| (1, row.as_str()))
                .collect::<Vec<_>>(),
        ),
    );
}

/// Checks that a row of [`assorted_relation`] reads through the Avro schema `text` unchanged.
fn check_nullability(engine: &dyn ConnectorTestEngine, text: &str) {
    let relation = assorted_relation();
    let rows = updates(&relation, &[(1, NULLABILITY_ROW)]);
    let schema = AvroSchema::parse_str(text).unwrap();
    check(
        engine,
        &relation,
        &InputOptions::default(),
        &config(AvroUpdateFormat::Raw, text),
        &record_messages(&relation, &rows, &schema),
        &rows,
    );
}

/// A field that is not nullable fills a nullable column.
#[engine_test]
pub fn non_null_to_nullable(engine: &dyn ConnectorTestEngine) {
    check_nullability(engine, &nullability_schema(r#""long""#, r#""string""#));
}

/// The row of the nullability cases.
const NULLABILITY_ROW: &str = r#"{"id": 1, "nAmE": "test", "b": false, "ts": "1970-01-01T00:00:00", "dt": "1970-01-01", "es": null, "m": null, "dec": "0.000"}"#;

/// Returns an Avro schema of [`assorted_relation`] in which `id` has the type `id`, `nAmE`
/// the type `name`, and `es` and `m` are nullable.
fn nullability_schema(id: &str, name: &str) -> String {
    format!(
        r#"{{
        "type": "record",
        "name": "TestStruct2",
        "connect.name": "test_namespace.TestStruct2",
        "fields": [
            {{ "name": "id", "type": {id} }},
            {{ "name": "nAmE", "type": {name} }},
            {{ "name": "b", "type": "boolean" }},
            {{ "name": "ts", "type": "long", "logicalType": "timestamp-micros" }},
            {{ "name": "dt", "type": "int", "logicalType": "date" }},
            {{
                "name": "es",
                "type": [{{ "type": "record", "name": "EmbeddedStruct", "fields": [{{ "name": "a", "type": "boolean" }}] }}, "null"]
            }},
            {{ "name": "m", "type": [{{ "type": "map", "values": "long" }}, "null"] }},
            {{
                "name": "dec",
                "type": {{ "type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 3 }}
            }}
        ]
    }}"#
    )
}

/// A nullable field fills a column that is not nullable.
#[engine_test]
pub fn nullable_to_non_nullable(engine: &dyn ConnectorTestEngine) {
    check_nullability(
        engine,
        &nullability_schema(r#"["long", "null"]"#, r#"["string", "null"]"#),
    );
}

/// A fixed-size field fills a BINARY(32) column, and a bytes field a VARBINARY column.
#[engine_test]
pub fn binary(engine: &dyn ConnectorTestEngine) {
    let text = r#"{
        "type": "record",
        "name": "TestBinary",
        "connect.name": "test_namespace.TestBinary",
        "fields": [
            { "name": "binary32", "type": {"type": "fixed", "name": "binary32", "size": 32} },
            { "name": "varbinary", "type": "bytes" }
        ]
    }"#;
    let relation = binary_relation();
    let hex = |bytes: &[u8]| -> String { bytes.iter().map(|byte| format!("{byte:02x}")).collect() };
    let row = format!(
        r#"{{"binary32": "{}", "varbinary": "{}"}}"#,
        hex(b"012345678901234567890123456789ab"),
        hex(b"foobar")
    );
    let rows = updates(&relation, &[(1, &row)]);
    let schema = AvroSchema::parse_str(text).unwrap();
    check(
        engine,
        &relation,
        &InputOptions::default(),
        &config(AvroUpdateFormat::Raw, text),
        &record_messages(&relation, &rows, &schema),
        &rows,
    );
}

/// A UUID column reads a plain string and a string with the `uuid` logical type, and a VARCHAR
/// column reads a `uuid` string.
#[engine_test]
pub fn uuid(engine: &dyn ConnectorTestEngine) {
    let text = r#"{
        "type": "record",
        "name": "TestUuid",
        "connect.name": "test_namespace.TestUuid",
        "fields": [
            { "name": "uuid1", "type": "string" },
            { "name": "uuid2", "type": {"type": "string", "logicalType": "uuid"} },
            { "name": "varchar", "type": {"type": "string", "logicalType": "uuid"} }
        ]
    }"#;
    let relation = uuid_relation();
    let rows = updates(
        &relation,
        &[(
            1,
            r#"{"uuid1": "550e8400-e29b-41d4-a716-446655440000", "uuid2": "550e8400-e29b-41d4-a716-446655440001", "varchar": "550e8400-e29b-41d4-a716-446655440002"}"#,
        )],
    );
    let schema = AvroSchema::parse_str(text).unwrap();
    check(
        engine,
        &relation,
        &InputOptions::default(),
        &config(AvroUpdateFormat::Raw, text),
        &record_messages(&relation, &rows, &schema),
        &rows,
    );
}

/// A VARIANT column reads JSON in an Avro string.
#[engine_test]
pub fn variant(engine: &dyn ConnectorTestEngine) {
    let relation = variant_relation();
    let schema = AvroSchema::parse_str(VARIANT_SCHEMA).unwrap();
    let record = Value::Record(vec![
        ("v1".to_string(), Value::String(VARIANT_JSON.to_string())),
        ("v2".to_string(), Value::String(VARIANT_JSON.to_string())),
    ]);
    check(
        engine,
        &relation,
        &InputOptions::default(),
        &config(AvroUpdateFormat::Raw, VARIANT_SCHEMA),
        &[avro::message(&schema, record)],
        &updates(&relation, &[(1, VARIANT_ROW)]),
    );
}

/// A VARCHAR column reads the symbol of an Avro enum.
#[engine_test]
pub fn enums(engine: &dyn ConnectorTestEngine) {
    let text = r#"{
        "type": "record",
        "name": "TestEnum",
        "connect.name": "test_namespace.TestEnum",
        "fields": [
            { "name": "enum_val", "type": { "type": "enum", "name": "Suit", "symbols" : ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"] } }
        ]
    }"#;
    let relation = enum_relation();
    let schema = AvroSchema::parse_str(text).unwrap();
    let record = Value::Record(vec![(
        "enum_val".to_string(),
        Value::Enum(0, "SPADES".to_string()),
    )]);
    check(
        engine,
        &relation,
        &InputOptions::default(),
        &config(AvroUpdateFormat::Raw, text),
        &[avro::message(&schema, record)],
        &updates(&relation, &[(1, r#"{"enum_val": "SPADES"}"#)]),
    );
}

/// A column whose DEFAULT reads `avro_schema_id` takes the id of the schema, 0 for a static
/// schema.
#[engine_test]
pub fn metadata(engine: &dyn ConnectorTestEngine) {
    let text = r#"{
        "type": "record",
        "name": "TestMetadata",
        "connect.name": "test_namespace.TestMetadata",
        "fields": [{ "name": "id", "type": "long" }]
    }"#;
    let relation = metadata_relation();
    let schema = AvroSchema::parse_str(text).unwrap();
    let record = Value::Record(vec![("id".to_string(), Value::Long(5))]);
    check(
        engine,
        &relation,
        &metadata_options(),
        &config(AvroUpdateFormat::Raw, text),
        &[avro::message(&schema, record)],
        &updates(&relation, &[(1, r#"{"id": 5, "schema_id": 0}"#)]),
    );
}

/// A field that declares `timestamp-millis` where the field, not its type, carries the logical
/// type, is a plain `long` of microseconds.
#[engine_test]
pub fn timestamp_millis(engine: &dyn ConnectorTestEngine) {
    let text = r#"{
        "type": "record",
        "name": "TestStruct2",
        "connect.name": "test_namespace.TestStruct2",
        "fields": [
            { "name": "id", "type": "long" },
            { "name": "nAmE", "type": ["string", "null"] },
            { "name": "b", "type": "boolean" },
            { "name": "ts", "type": "long", "logicalType": "timestamp-millis" },
            { "name": "dt", "type": "int", "logicalType": "date" },
            {
                "name": "es",
                "type": { "type": "record", "name": "EmbeddedStruct", "fields": [{ "name": "a", "type": "boolean" }] }
            },
            { "name": "m", "type": { "type": "map", "values": "long" } },
            {
                "name": "dec",
                "type": { "type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 3 }
            }
        ]
    }"#;
    let relation = assorted_relation();
    let rows = updates(&relation, &ASSORTED_ROWS.map(|row| (1, row)));
    let schema = AvroSchema::parse_str(text).unwrap();
    check(
        engine,
        &relation,
        &InputOptions::default(),
        &config(AvroUpdateFormat::Raw, text),
        &record_messages(&relation, &rows, &schema),
        &rows,
    );
}

/// Every Debezium temporal type fills the matching TIMESTAMP, TIMESTAMP WITH TIME ZONE, TIME,
/// or DATE column.
#[engine_test]
pub fn debezium_temporal_types(engine: &dyn ConnectorTestEngine) {
    let value_schema = r#"{
        "type": "record",
        "name": "Temporal",
        "connect.name": "test_namespace.Temporal",
        "fields": [
            { "name": "id", "type": "long" },
            { "name": "ts_millis", "type": { "type": "long", "connect.name": "io.debezium.time.Timestamp" } },
            { "name": "ts_micros", "type": { "type": "long", "connect.name": "io.debezium.time.MicroTimestamp" } },
            { "name": "ts_nanos", "type": { "type": "long", "connect.name": "io.debezium.time.NanoTimestamp" } },
            { "name": "ts_zoned", "type": { "type": "string", "connect.name": "io.debezium.time.ZonedTimestamp" } },
            { "name": "ts_connect", "type": { "type": "long", "connect.name": "org.apache.kafka.connect.data.Timestamp" } },
            { "name": "ts_opt", "type": ["null", { "type": "long", "connect.name": "io.debezium.time.MicroTimestamp" }], "default": null },
            { "name": "t_millis", "type": { "type": "int", "connect.name": "io.debezium.time.Time" } },
            { "name": "t_micros", "type": { "type": "long", "connect.name": "io.debezium.time.MicroTime" } },
            { "name": "t_nanos", "type": { "type": "long", "connect.name": "io.debezium.time.NanoTime" } },
            { "name": "t_zoned", "type": { "type": "string", "connect.name": "io.debezium.time.ZonedTime" } },
            { "name": "t_connect", "type": { "type": "int", "connect.name": "org.apache.kafka.connect.data.Time" } },
            { "name": "dt", "type": { "type": "int", "connect.name": "io.debezium.time.Date" } }
        ]
    }"#;
    // 2021-06-15T12:30:45.123Z, at millisecond precision so that its millisecond,
    // microsecond, and nanosecond forms name the same instant.
    let ts_micros: i64 = 1_623_760_245_123_000;
    let time_micros: i64 = (12 * 3_600 + 30 * 60 + 45) * 1_000_000 + 123_000;
    let dt_days = (ts_micros / 86_400_000_000) as i32;
    let record = |id: i64, ts_opt: Option<i64>| {
        Value::Record(vec![
            ("id".to_string(), Value::Long(id)),
            ("ts_millis".to_string(), Value::Long(ts_micros / 1_000)),
            ("ts_micros".to_string(), Value::Long(ts_micros)),
            ("ts_nanos".to_string(), Value::Long(ts_micros * 1_000)),
            (
                "ts_zoned".to_string(),
                Value::String("2021-06-15T14:30:45.123+02:00".to_string()),
            ),
            ("ts_connect".to_string(), Value::Long(ts_micros / 1_000)),
            (
                "ts_opt".to_string(),
                match ts_opt {
                    None => Value::Union(0, Box::new(Value::Null)),
                    Some(micros) => Value::Union(1, Box::new(Value::Long(micros))),
                },
            ),
            (
                "t_millis".to_string(),
                Value::Int((time_micros / 1_000) as i32),
            ),
            ("t_micros".to_string(), Value::Long(time_micros)),
            ("t_nanos".to_string(), Value::Long(time_micros * 1_000)),
            (
                "t_zoned".to_string(),
                Value::String("14:30:45.123+02:00".to_string()),
            ),
            (
                "t_connect".to_string(),
                Value::Int((time_micros / 1_000) as i32),
            ),
            ("dt".to_string(), Value::Int(dt_days)),
        ])
    };
    let row = |id: i64, ts_opt: &str| {
        format!(
            r#"{{"id": {id}, "ts_millis": "2021-06-15T12:30:45.123", "ts_micros": "2021-06-15T12:30:45.123", "ts_nanos": "2021-06-15T12:30:45.123", "ts_zoned": "2021-06-15T12:30:45.123Z", "ts_connect": "2021-06-15T12:30:45.123", "ts_opt": {ts_opt}, "t_millis": "12:30:45.123", "t_micros": "12:30:45.123", "t_nanos": "12:30:45.123", "t_zoned": "12:30:45.123", "t_connect": "12:30:45.123", "dt": "2021-06-15"}}"#
        )
    };
    let text = debezium_schema_text(value_schema, "Temporal");
    let envelope = Envelope::new(&text);
    let relation = temporal_relation();
    let (row1, row2) = (row(1, r#""2021-06-15T12:30:45.123""#), row(2, "null"));
    check(
        engine,
        &relation,
        &InputOptions::default(),
        &config(AvroUpdateFormat::Debezium, &text),
        &[
            envelope.update(record(1, Some(ts_micros)), record(1, Some(ts_micros))),
            envelope.update(record(2, None), record(2, None)),
        ],
        &updates(
            &relation,
            &[(-1, &row1), (1, &row1), (-1, &row2), (1, &row2)],
        ),
    );
}

/// A Debezium temporal type mapped to an incompatible SQL column must be
/// rejected during schema validation.
#[test]
fn test_debezium_temporal_type_mismatch() {
    use super::schema::validate_struct_schema;

    let value_schema = r#"{
        "type": "record",
        "name": "Temporal",
        "connect.name": "test_namespace.Temporal",
        "fields": [
            { "name": "id", "type": { "type": "long", "connect.name": "io.debezium.time.Timestamp" } }
        ]
    }"#;

    let hoisted = super::coercion::hoist_coercible_types(value_schema).unwrap();
    let schema = AvroSchema::parse_str(&hoisted).unwrap();
    let resolved = ResolvedSchema::try_from(&schema).unwrap();
    let refs = resolved
        .get_names()
        .iter()
        .map(|(name, schema)| (name.clone(), (*schema).clone()))
        .collect();

    // The `id` column is BIGINT, but the Avro field is a Debezium timestamp.
    let fields = vec![Field::new("id".into(), ColumnType::bigint(false))];
    let err = validate_struct_schema(&schema, &refs, &fields).unwrap_err();
    assert!(
        err.contains("io.debezium.time.Timestamp") && err.contains("TIMESTAMP"),
        "unexpected error message: {err}"
    );
}

/// A Debezium zoned timestamp and a Debezium microsecond timestamp each fill a TIMESTAMP and a
/// TIMESTAMP WITH TIME ZONE column.
#[engine_test]
pub fn debezium_timestamp_column_targets(engine: &dyn ConnectorTestEngine) {
    let value_schema = r#"{
        "type": "record",
        "name": "TsTargets",
        "connect.name": "test_namespace.TsTargets",
        "fields": [
            { "name": "z_ts", "type": { "type": "string", "connect.name": "io.debezium.time.ZonedTimestamp" } },
            { "name": "z_tstz", "type": { "type": "string", "connect.name": "io.debezium.time.ZonedTimestamp" } },
            { "name": "n_ts", "type": { "type": "long", "connect.name": "io.debezium.time.MicroTimestamp" } },
            { "name": "n_tstz", "type": { "type": "long", "connect.name": "io.debezium.time.MicroTimestamp" } }
        ]
    }"#;
    // 2021-06-15T12:30:45.123Z, as a +02:00 wall clock and as microseconds.
    let micros: i64 = 1_623_760_245_123_000;
    let zoned = || Value::String("2021-06-15T14:30:45.123+02:00".to_string());
    let record = Value::Record(vec![
        ("z_ts".to_string(), zoned()),
        ("z_tstz".to_string(), zoned()),
        ("n_ts".to_string(), Value::Long(micros)),
        ("n_tstz".to_string(), Value::Long(micros)),
    ]);
    let text = debezium_schema_text(value_schema, "TsTargets");
    let envelope = Envelope::new(&text);
    let relation = timestamp_targets_relation();
    let row = r#"{"z_ts": "2021-06-15T12:30:45.123", "z_tstz": "2021-06-15T12:30:45.123Z", "n_ts": "2021-06-15T12:30:45.123", "n_tstz": "2021-06-15T12:30:45.123Z"}"#;
    check(
        engine,
        &relation,
        &InputOptions::default(),
        &config(AvroUpdateFormat::Debezium, &text),
        &[envelope.update(record.clone(), record)],
        &updates(&relation, &[(-1, row), (1, row)]),
    );
}

/// A Debezium variable-scale decimal fills a DECIMAL column of another scale, also through a
/// reference to its record type by name.
#[engine_test]
pub fn debezium_variable_scale_decimal(engine: &dyn ConnectorTestEngine) {
    let value_schema = r#"{
        "type": "record",
        "name": "Dec",
        "connect.name": "test_namespace.Dec",
        "fields": [
            { "name": "id", "type": "long" },
            { "name": "amount", "type": {
                "type": "record",
                "name": "VariableScaleDecimal",
                "namespace": "io.debezium.data",
                "fields": [
                    { "name": "scale", "type": "int" },
                    { "name": "value", "type": "bytes" }
                ],
                "connect.version": 1,
                "connect.name": "io.debezium.data.VariableScaleDecimal"
            }},
            { "name": "amount_opt", "type": ["null", "io.debezium.data.VariableScaleDecimal"], "default": null }
        ]
    }"#;
    let decimal = |scale: i32, unscaled: i64| {
        Value::Record(vec![
            ("scale".to_string(), Value::Int(scale)),
            (
                "value".to_string(),
                Value::Bytes(BigInt::from(unscaled).to_signed_bytes_be()),
            ),
        ])
    };
    let record = |id: i64, amount: Value, amount_opt: Option<Value>| {
        Value::Record(vec![
            ("id".to_string(), Value::Long(id)),
            ("amount".to_string(), amount),
            (
                "amount_opt".to_string(),
                match amount_opt {
                    None => Value::Union(0, Box::new(Value::Null)),
                    Some(amount) => Value::Union(1, Box::new(amount)),
                },
            ),
        ])
    };
    // The wire scales differ from the column scale, 3.
    let record1 = record(1, decimal(2, 12345), Some(decimal(3, -6789)));
    let record2 = record(2, decimal(0, 42), None);
    let text = debezium_schema_text(value_schema, "Dec");
    let envelope = Envelope::new(&text);
    let relation = decimal_relation();
    let row1 = r#"{"id": 1, "amount": "123.450", "amount_opt": "-6.789"}"#;
    let row2 = r#"{"id": 2, "amount": "42.000", "amount_opt": null}"#;
    check(
        engine,
        &relation,
        &InputOptions::default(),
        &config(AvroUpdateFormat::Debezium, &text),
        &[
            envelope.update(record1.clone(), record1),
            envelope.update(record2.clone(), record2),
        ],
        &updates(&relation, &[(-1, row1), (1, row1), (-1, row2), (1, row2)]),
    );
}

/// Avro `int` fields fill unsigned and signed integer columns of 32 and 64 bits.
#[engine_test]
pub fn int_conversions(engine: &dyn ConnectorTestEngine) {
    let text = r#"{
        "type": "record",
        "name": "TestIntConversions",
        "connect.name": "test_namespace.TestIntConversions",
        "fields": [
            { "name": "uint", "type": "int" },
            { "name": "ulong", "type": "int" },
            { "name": "int", "type": "int" },
            { "name": "long", "type": "int" }
        ]
    }"#;
    let relation = integers_relation();
    let schema = AvroSchema::parse_str(text).unwrap();
    let record = Value::Record(vec![
        ("uint".to_string(), Value::Int(1)),
        ("ulong".to_string(), Value::Int(2)),
        ("int".to_string(), Value::Int(3)),
        ("long".to_string(), Value::Int(4)),
    ]);
    check(
        engine,
        &relation,
        &InputOptions::default(),
        &config(AvroUpdateFormat::Raw, text),
        &[avro::message(&schema, record)],
        &updates(
            &relation,
            &[(1, r#"{"uint": 1, "ulong": 2, "int": 3, "long": 4}"#)],
        ),
    );
}

/// Returns a strategy for up to `max_rows` rows of [`assorted_relation`], each inserted once.
///
/// Besides the values of [`rows::value`], the rows hold any `id`, dates from 1970 to 2243, and
/// map keys of up to 32 characters, whose Avro encoding can need a length of two bytes.
fn assorted_rows(max_rows: usize) -> BoxedStrategy<Updates> {
    let relation = assorted_relation();
    let columns: Vec<(String, BoxedStrategy<JsonValue>)> = relation
        .fields
        .iter()
        .map(|field| {
            let name = field.name.name();
            let strategy = match name.as_str() {
                "id" => any::<i64>().prop_map(JsonValue::from).boxed(),
                "dt" => (0..100_000i64)
                    .prop_map(|days| {
                        let date = chrono::DateTime::UNIX_EPOCH.date_naive()
                            + chrono::TimeDelta::days(days);
                        JsonValue::from(date.to_string())
                    })
                    .boxed(),
                "m" => prop_oneof![
                    Just(JsonValue::Null),
                    btree_map("\\PC{0,32}", any::<i64>().prop_map(JsonValue::from), 0..4)
                        .prop_map(|map| JsonValue::Object(map.into_iter().collect())),
                ]
                .boxed(),
                _ => rows::value(&field.columntype),
            };
            (name, strategy)
        })
        .collect();
    let (names, values): (Vec<String>, Vec<BoxedStrategy<JsonValue>>) = columns.into_iter().unzip();
    vec(values, 0..=max_rows)
        .prop_map(move |rows| {
            let rows: Vec<String> = rows
                .into_iter()
                .map(|row| JsonValue::Object(names.iter().cloned().zip(row).collect()).to_string())
                .collect();
            let rows: Vec<(i64, &str)> = rows.iter().map(|row| (1, row.as_str())).collect();
            Updates::from_json(published_schema(&assorted_relation()), &rows).unwrap()
        })
        .boxed()
}

/// Random raw records read back unchanged and in order.
#[engine_test]
pub fn random_raw_records(engine: &dyn ConnectorTestEngine) {
    let relation = assorted_relation();
    let schema = AvroSchema::parse_str(ASSORTED_SCHEMA).unwrap();
    proptest!(ProptestConfig::with_cases(2), |(rows in assorted_rows(10000))| {
        check(
            engine,
            &relation,
            &InputOptions::default(),
            &config(AvroUpdateFormat::Raw, ASSORTED_SCHEMA),
            &record_messages(&relation, &rows, &schema),
            &rows,
        );
    });
}

/// Random Debezium updates delete and insert each record, in order.
#[engine_test]
pub fn random_debezium_records(engine: &dyn ConnectorTestEngine) {
    let relation = assorted_relation();
    proptest!(ProptestConfig::with_cases(2), |(rows in assorted_rows(10000))| {
        let (config, messages, expected) =
            debezium_updates(&relation, &rows, ASSORTED_SCHEMA, "TestStruct2");
        check(engine, &relation, &InputOptions::default(), &config, &messages, &expected);
    });
}

/// The view that most cases encode.
pub fn relation() -> Relation {
    relations::basic("avro_output")
}

/// The key of the index on [`relation`].
pub fn key_relation() -> Relation {
    relations::basic_key("avro_output_key")
}

/// The Avro schema of [`relation`].
const SCHEMA: &str = r#"{
    "type": "record",
    "name": "TestStruct",
    "fields": [
        { "name": "id", "type": "long" },
        { "name": "b", "type": "boolean" },
        { "name": "i", "type": ["null", "long"] },
        { "name": "s", "type": "string" }
    ]
}"#;

/// [`SCHEMA`] with the CDC field `op`, which an encoder with a `cdc_field` appends.
const SCHEMA_WITH_OP: &str = r#"{
    "type": "record",
    "name": "TestStruct",
    "fields": [
        { "name": "id", "type": "long" },
        { "name": "b", "type": "boolean" },
        { "name": "i", "type": ["null", "long"] },
        { "name": "s", "type": "string" },
        { "name": "op", "type": "string" }
    ]
}"#;

/// A message that an encoder writes: its key, value, and headers.
type Message = (
    Option<Vec<u8>>,
    Option<Vec<u8>>,
    Vec<(String, Option<Vec<u8>>)>,
);

/// What the consumer of an encoder collects: each message.
type Output = Arc<Mutex<Vec<Message>>>;

/// Returns an encoder of `relation`, or of the index `key` on it, with `config`, and the
/// messages that it writes.
fn encoder(
    key: Option<&Relation>,
    relation: &Relation,
    config: AvroEncoderConfig,
) -> (AvroEncoder, Output) {
    let consumer = MockOutputConsumer::new();
    let output = consumer.data.clone();
    let encoder = AvroEncoder::create(
        "avro_test_endpoint",
        &key.cloned(),
        relation,
        Box::new(consumer),
        config,
        None,
        key.is_some(),
    )
    .unwrap();
    (encoder, output)
}

/// Encodes `batch` as one step, and returns the messages that the encoder writes for it.
fn encode(
    encoder: &mut AvroEncoder,
    output: &Output,
    batch: Arc<dyn SerBatchReader>,
) -> Vec<Message> {
    encoder.consumer().batch_start(0, OutputBatchType::Delta);
    encoder.encode(batch).unwrap();
    encoder.consumer().batch_end();
    output.lock().unwrap().drain(..).collect()
}

/// Returns the row of `relation` in a message, `bytes` with a 5-byte header, decoded with
/// `schema`, as JSON in the arrow-json form.
fn decode(relation: &Relation, schema: &AvroSchema, bytes: &[u8]) -> String {
    let value = from_avro_datum(schema, &mut &bytes[5..], None).unwrap();
    avro::to_json(relation, &value).unwrap().to_string()
}

/// Returns `rows` of `relation`, JSON in the arrow-json form, each with its weight.
fn output_updates(relation: &Relation, rows: &[(i64, String)]) -> Updates {
    let rows: Vec<(i64, &str)> = rows
        .iter()
        .map(|(weight, row)| (*weight, row.as_str()))
        .collect();
    Updates::from_json(published_schema(relation), &rows).unwrap()
}

/// Returns the operation in the `op` header of a message.
fn operation(headers: &[(String, Option<Vec<u8>>)]) -> String {
    let (name, value) = &headers[0];
    assert_eq!(name, "op");
    String::from_utf8(value.clone().unwrap()).unwrap()
}

/// Returns the rows with positive weights in `zset`, and the rows with negative weights,
/// negated.
fn split_by_sign(zset: &ZSet) -> (ZSet, ZSet) {
    let (rows, weights) = zset.to_batch().unwrap();
    let mut inserts = ZSet::empty(rows.schema()).unwrap();
    let mut deletes = ZSet::empty(rows.schema()).unwrap();
    for (index, weight) in weights.iter().enumerate() {
        let row = rows.slice(index, 1);
        if *weight > 0 {
            inserts.insert(&row, &[*weight]).unwrap();
        } else {
            deletes.insert(&row, &[-weight]).unwrap();
        }
    }
    (inserts, deletes)
}

/// Returns the columns of `key` in `row`, a row of the indexed relation as JSON.
fn key_of(key: &Relation, row: &str) -> JsonValue {
    let row: JsonValue = serde_json::from_str(row).unwrap();
    let columns = key
        .fields
        .iter()
        .map(|field| {
            let name = field.name.name();
            let value = row[&name].clone();
            (name, value)
        })
        .collect();
    JsonValue::Object(columns)
}

/// Returns `rows` with the column `b` negated.
fn flip_b(rows: &RecordBatch) -> RecordBatch {
    let index = rows.schema().index_of("b").unwrap();
    let mut columns = rows.columns().to_vec();
    let flipped: BooleanArray = not(columns[index].as_boolean()).unwrap();
    columns[index] = Arc::new(flipped);
    RecordBatch::try_new(rows.schema(), columns).unwrap()
}

/// The VARIANT columns encode as JSON in Avro strings.
#[engine_test]
pub fn variant_output(engine: &dyn ConnectorTestEngine) {
    let relation = variant_relation();
    let config = AvroEncoderConfig {
        schema: Some(VARIANT_SCHEMA.to_string()),
        ..Default::default()
    };
    let schema = AvroSchema::parse_str(VARIANT_SCHEMA).unwrap();
    let (mut encoder, output) = encoder(None, &relation, config);
    let rows = Updates::from_json(published_schema(&relation), &[(1, VARIANT_ROW)]).unwrap();
    let messages = encode(
        &mut encoder,
        &output,
        engine
            .output_batch(&relation, rows.rows(), rows.weights())
            .unwrap(),
    );
    let decoded: Vec<(i64, String)> = messages
        .iter()
        .map(|(_key, value, _headers)| (1, decode(&relation, &schema, value.as_ref().unwrap())))
        .collect();
    assert_eq!(output_updates(&relation, &decoded), rows);
}

/// Each change of each random batch encodes once per unit of weight, with its operation in
/// the `op` header.
#[engine_test]
pub fn random_raw_output(engine: &dyn ConnectorTestEngine) {
    let relation = relation();
    let schema = AvroSchema::parse_str(SCHEMA).unwrap();
    proptest!(|(batches in rows::weighted_batches(&relation, "id", 0..=10, 20, -2..=2))| {
        let config = AvroEncoderConfig {
            schema: Some(SCHEMA.to_string()),
            ..Default::default()
        };
        let (mut encoder, output) = encoder(None, &relation, config);
        for rows in &batches {
            let messages = encode(
                &mut encoder,
                &output,
                engine.output_batch(&relation, rows.rows(), rows.weights()).unwrap(),
            );
            let decoded: Vec<(i64, String)> = messages
                .iter()
                .map(|(_key, value, headers)| {
                    let weight = if operation(headers) == "insert" { 1 } else { -1 };
                    (weight, decode(&relation, &schema, value.as_ref().unwrap()))
                })
                .collect();
            prop_assert_eq!(output_updates(&relation, &decoded).zset().unwrap(), rows.zset().unwrap());
        }
    });
}

/// Encodes three steps of indexed batches for each batch of `rows`: an insert of each row, an
/// update of each row to its copy with `b` negated, and a delete of the copy. Checks that the
/// steps encode the rows with the operations `insert`, `update`, and `delete`, and, when the
/// encoder writes keys, that each key holds the `id` of its row.
fn check_indexed_steps(
    engine: &dyn ConnectorTestEngine,
    config: AvroEncoderConfig,
    batches: &[Updates],
) {
    let (relation, key) = (relation(), key_relation());
    let (mut encoder, output) = encoder(Some(&key), &relation, config.clone());
    let key_schema = encoder.key_avro_schema.clone();
    if config.key_mode != Some(AvroEncoderKeyMode::None) {
        assert!(key_schema.is_some());
    }
    let value_schema = encoder.value_avro_schema.clone();
    for rows in batches {
        let old = rows.rows().clone();
        let new = flip_b(&old);
        let both = concat_batches(&old.schema(), [&old, &new]).unwrap();
        let n = old.num_rows();
        let steps = [
            ("insert", old.clone(), vec![1; n], &old),
            ("update", both, [vec![-1; n], vec![1; n]].concat(), &new),
            ("delete", new.clone(), vec![-1; n], &new),
        ];
        for (expected_operation, step_rows, weights, expected_rows) in steps {
            let messages = encode(
                &mut encoder,
                &output,
                engine
                    .indexed_output_batch(&key, &relation, &step_rows, &weights)
                    .unwrap(),
            );
            let mut decoded = Vec::new();
            for (message_key, value, headers) in &messages {
                assert_eq!(operation(headers), expected_operation);
                let row = decode(&relation, &value_schema, value.as_ref().unwrap());
                if let Some(key_schema) = &key_schema {
                    let key_row = decode(&key, key_schema, message_key.as_ref().unwrap());
                    let id = serde_json::from_str::<serde_json::Value>(&row).unwrap()["id"].clone();
                    assert_eq!(key_row, serde_json::json!({"id": id}).to_string());
                }
                decoded.push((1, row));
            }
            assert_eq!(
                output_updates(&relation, &decoded).zset().unwrap(),
                ZSet::from_batch(expected_rows, &vec![1; n]).unwrap(),
                "{expected_operation}"
            );
        }
    }
}

/// An indexed view encodes inserts, updates, and deletes, without keys.
#[engine_test]
pub fn random_raw_output_indexed(engine: &dyn ConnectorTestEngine) {
    proptest!(|(batches in rows::batches(&relation(), "id", 10..=10, 20))| {
        check_indexed_steps(
            engine,
            AvroEncoderConfig {
                schema: Some(SCHEMA.to_string()),
                key_mode: Some(AvroEncoderKeyMode::None),
                ..Default::default()
            },
            &batches,
        );
    });
}

/// An indexed batch with a key that has two values, or one value with a weight past one, is
/// an error that says so.
#[engine_test]
pub fn non_unique_keys(engine: &dyn ConnectorTestEngine) {
    let (relation, key) = (relation(), key_relation());
    let config = AvroEncoderConfig {
        schema: Some(SCHEMA.to_string()),
        key_mode: Some(AvroEncoderKeyMode::None),
        ..Default::default()
    };
    let (mut encoder, _output) = encoder(Some(&key), &relation, config);
    let v1 = r#"{"id": 1, "b": true, "s": "foo"}"#;
    let v2 = r#"{"id": 1, "b": false, "s": "bar"}"#;
    for (rows, expected_error) in [
        (vec![(2, v1)], "is inserted 2 times"),
        (vec![(-2, v1)], "is deleted 2 times"),
        (
            vec![(1, v1), (1, v2)],
            "Error description: Multiple new values for the same key.",
        ),
        (
            vec![(-1, v1), (-1, v2)],
            "Error description: Multiple deleted values for the same key.",
        ),
    ] {
        let rows = Updates::from_json(published_schema(&relation), &rows).unwrap();
        let batch = engine
            .indexed_output_batch(&key, &relation, rows.rows(), rows.weights())
            .unwrap();
        encoder.consumer().batch_start(0, OutputBatchType::Delta);
        let error = encoder.encode(batch).unwrap_err();
        assert!(
            error.to_string().contains(expected_error),
            "expected '{expected_error}' in '{error}'"
        );
        encoder.consumer().batch_end();
    }
}

/// An encoder with ten threads encodes each change of a large indexed batch once.
#[engine_test]
pub fn parallel_indexed_output(engine: &dyn ConnectorTestEngine) {
    let (relation, key) = (relation(), key_relation());
    let config = AvroEncoderConfig {
        schema: Some(SCHEMA.to_string()),
        key_mode: Some(AvroEncoderKeyMode::None),
        threads: 10,
        ..Default::default()
    };
    let (mut encoder, output) = encoder(Some(&key), &relation, config);
    let schema = AvroSchema::parse_str(SCHEMA).unwrap();
    let rows: Vec<(i64, String)> = (0..2000)
        .map(|id| {
            let weight = if id % 3 == 0 { -1 } else { 1 };
            let i = if id % 2 == 0 {
                id.to_string()
            } else {
                "null".to_string()
            };
            (
                weight,
                format!(
                    r#"{{"id": {id}, "b": {}, "i": {i}, "s": "{}"}}"#,
                    id % 5 == 0,
                    id * 7
                ),
            )
        })
        .collect();
    let rows = output_updates(&relation, &rows);
    let messages = encode(
        &mut encoder,
        &output,
        engine
            .indexed_output_batch(&key, &relation, rows.rows(), rows.weights())
            .unwrap(),
    );
    let decoded: Vec<(i64, String)> = messages
        .iter()
        .map(|(_key, value, headers)| {
            let weight = match operation(headers).as_str() {
                "insert" => 1,
                "delete" => -1,
                operation => panic!("unexpected operation {operation}"),
            };
            (weight, decode(&relation, &schema, value.as_ref().unwrap()))
        })
        .collect();
    assert_eq!(decoded.len(), rows.len());
    assert_eq!(
        output_updates(&relation, &decoded).zset().unwrap(),
        rows.zset().unwrap()
    );
}

/// An indexed encoder with a `cdc_field` writes each change with the field set to its
/// operation: `I` for an insert, `U` for an update, and `D` for a delete.
#[engine_test]
pub fn cdc_field_output(engine: &dyn ConnectorTestEngine) {
    let (relation, key) = (relation(), key_relation());
    let config = AvroEncoderConfig {
        schema: Some(SCHEMA.to_string()),
        key_mode: Some(AvroEncoderKeyMode::None),
        cdc_field: Some("op".to_string()),
        ..Default::default()
    };
    let (mut encoder, output) = encoder(Some(&key), &relation, config);
    let rows = output_updates(
        &relation,
        &[
            (
                1,
                r#"{"id": 1, "b": true, "i": null, "s": "new"}"#.to_string(),
            ),
            (
                -1,
                r#"{"id": 2, "b": true, "i": 2, "s": "old"}"#.to_string(),
            ),
            (
                1,
                r#"{"id": 2, "b": false, "i": 2, "s": "new"}"#.to_string(),
            ),
            (
                -1,
                r#"{"id": 3, "b": false, "i": 3, "s": "gone"}"#.to_string(),
            ),
        ],
    );
    let messages = encode(
        &mut encoder,
        &output,
        engine
            .indexed_output_batch(&key, &relation, rows.rows(), rows.weights())
            .unwrap(),
    );

    let mut with_op = relation.clone();
    with_op
        .fields
        .push(Field::new("op".into(), ColumnType::varchar(false)));
    let schema = AvroSchema::parse_str(SCHEMA_WITH_OP).unwrap();
    let decoded: Vec<(i64, String)> = messages
        .iter()
        .map(|(_key, value, _headers)| (1, decode(&with_op, &schema, value.as_ref().unwrap())))
        .collect();
    let expected = [
        r#"{"id": 1, "b": true, "i": null, "s": "new", "op": "I"}"#,
        r#"{"id": 2, "b": false, "i": 2, "s": "new", "op": "U"}"#,
        r#"{"id": 3, "b": false, "i": 3, "s": "gone", "op": "D"}"#,
    ]
    .map(|row| (1, row.to_string()));
    assert_eq!(decoded.len(), expected.len());
    assert_eq!(
        output_updates(&with_op, &decoded).zset().unwrap(),
        output_updates(&with_op, &expected).zset().unwrap()
    );
}

/// A relation of assorted types, with a nullable ROW column.
pub fn output_assorted_relation() -> Relation {
    relations::assorted("avro_output_assorted")
}

/// A relation of every type, with ROW columns, an ARRAY of ROW, and a MAP of ROW.
pub fn every_type_relation() -> Relation {
    relations::every_type("avro_output_every_type")
}

/// Rows of every type, ROW columns among them, encode under the schema that the encoder
/// generates, in the raw and the Confluent JDBC formats, and decode to the rows of the batch.
#[engine_test]
pub fn generated_schema_encodes_row_columns(engine: &dyn ConnectorTestEngine) {
    for (relation, numbered) in [
        (output_assorted_relation(), "id"),
        (every_type_relation(), "bigint"),
    ] {
        let rows = rows::batches(&relation, numbered, 1..=1, 5)
            .prop_filter("some rows", |batches| !batches[0].is_empty())
            .new_tree(&mut TestRunner::deterministic())
            .unwrap()
            .current()
            .remove(0);
        for update_format in [AvroUpdateFormat::Raw, AvroUpdateFormat::ConfluentJdbc] {
            let config = AvroEncoderConfig {
                update_format: update_format.clone(),
                ..Default::default()
            };
            let (mut encoder, output) = encoder(None, &relation, config);
            let schema = encoder.value_avro_schema.clone();
            AvroSchema::parse_str(&crate::format::avro::schema::schema_json(&schema)).unwrap();
            let messages = encode(
                &mut encoder,
                &output,
                engine
                    .output_batch(&relation, rows.rows(), rows.weights())
                    .unwrap(),
            );
            let decoded: Vec<(i64, String)> = messages
                .iter()
                .map(|(_key, value, _headers)| {
                    (1, decode(&relation, &schema, value.as_ref().unwrap()))
                })
                .collect();
            assert_eq!(
                output_updates(&relation, &decoded).zset().unwrap(),
                rows.zset().unwrap(),
                "{} in {update_format:?}",
                relation.name
            );
        }
    }
}

/// By default, an indexed view encodes each change with a key of the key columns, under
/// schemas that the encoder derives from the relations.
#[engine_test]
pub fn random_raw_output_indexed_with_key(engine: &dyn ConnectorTestEngine) {
    proptest!(|(batches in rows::batches(&relation(), "id", 10..=10, 20))| {
        check_indexed_steps(engine, AvroEncoderConfig::default(), &batches);
    });
}

/// In the Confluent JDBC format, an insert is a record once per unit of weight, and a delete is
/// a tombstone whose key is the whole record. The deletes come first, then the inserts, each in
/// the order of the batch's cursor.
#[engine_test]
pub fn random_confluent_output(engine: &dyn ConnectorTestEngine) {
    let relation = relation();
    let schema = AvroSchema::parse_str(SCHEMA).unwrap();
    proptest!(|(batches in rows::weighted_batches(&relation, "id", 0..=10, 20, -2..=2))| {
        let config = AvroEncoderConfig {
            schema: Some(SCHEMA.to_string()),
            namespace: Some("foo.bar".to_string()),
            update_format: AvroUpdateFormat::ConfluentJdbc,
            ..Default::default()
        };
        let (mut encoder, output) = encoder(None, &relation, config);
        for rows in &batches {
            let batch = engine.output_batch(&relation, rows.rows(), rows.weights()).unwrap();
            let messages = encode(&mut encoder, &output, batch.clone());
            let (mut inserts, mut deletes, mut changes) = (Vec::new(), Vec::new(), Vec::new());
            for (key, value, _headers) in &messages {
                match value {
                    Some(value) => {
                        let row = decode(&relation, &schema, value);
                        inserts.push((1, row.clone()));
                        changes.push((1, row));
                    }
                    None => {
                        let row = decode(&relation, &schema, key.as_ref().unwrap());
                        deletes.push((1, row.clone()));
                        changes.push((-1, row));
                    }
                }
            }
            let (expected_inserts, expected_deletes) = split_by_sign(&rows.zset().unwrap());
            prop_assert_eq!(output_updates(&relation, &inserts).zset().unwrap(), expected_inserts);
            prop_assert_eq!(output_updates(&relation, &deletes).zset().unwrap(), expected_deletes);
            prop_assert_eq!(
                output_updates(&relation, &changes),
                read::encoding_order(batch.as_ref(), &relation).unwrap()
            );
        }
    });
}

/// In the Confluent JDBC format, an indexed view encodes an insert and an update as a record
/// under its own key, and a delete as a tombstone of the key.
#[engine_test]
pub fn random_confluent_output_indexed(engine: &dyn ConnectorTestEngine) {
    let (relation, key) = (relation(), key_relation());
    proptest!(|(batches in rows::batches(&relation, "id", 10..=10, 20))| {
        let config = AvroEncoderConfig {
            namespace: Some("foo.bar".to_string()),
            update_format: AvroUpdateFormat::ConfluentJdbc,
            ..Default::default()
        };
        let (mut encoder, output) = encoder(Some(&key), &relation, config);
        let key_schema = encoder.key_avro_schema.clone().unwrap();
        let value_schema = encoder.value_avro_schema.clone();
        for rows in &batches {
            let old = rows.rows().clone();
            let new = flip_b(&old);
            let both = concat_batches(&old.schema(), [&old, &new]).unwrap();
            let n = old.num_rows();
            let steps = [
                (old.clone(), vec![1; n], Some(&old)),
                (both, [vec![-1; n], vec![1; n]].concat(), Some(&new)),
                (new.clone(), vec![-1; n], None),
            ];
            for (step_rows, weights, expected_values) in steps {
                let messages = encode(
                    &mut encoder,
                    &output,
                    engine.indexed_output_batch(&key, &relation, &step_rows, &weights).unwrap(),
                );
                let mut keys = Vec::new();
                let mut values = Vec::new();
                for (message_key, value, _headers) in &messages {
                    let message_key = decode(&key, &key_schema, message_key.as_ref().unwrap());
                    if let Some(value) = value {
                        let value = decode(&relation, &value_schema, value);
                        prop_assert_eq!(
                            serde_json::from_str::<JsonValue>(&message_key).unwrap(),
                            key_of(&key, &value),
                            "the key of a message is not the key of its value {}",
                            value
                        );
                        values.push((1, value));
                    }
                    keys.push((1, message_key));
                }
                let expected_keys = Updates::new(
                    old.project(&[old.schema().index_of("id").unwrap()]).unwrap(),
                    vec![1; n],
                )
                .unwrap();
                prop_assert_eq!(
                    output_updates(&key, &keys).zset().unwrap(),
                    expected_keys.zset().unwrap()
                );
                match expected_values {
                    Some(expected_values) => prop_assert_eq!(
                        output_updates(&relation, &values).zset().unwrap(),
                        ZSet::from_batch(expected_values, &vec![1; n]).unwrap()
                    ),
                    None => prop_assert!(values.is_empty()),
                }
            }
        }
    });
}
