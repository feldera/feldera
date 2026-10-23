//! The Avro parser: raw records and Debezium change events, Avro schemas that differ from
//! the table, logical and Debezium types, the `cdc_field` operations, soft deletes, and
//! connector metadata.

use std::borrow::Cow;
use std::collections::BTreeMap;

use apache_avro::Schema as AvroSchema;
use apache_avro::types::Value;
use feldera_adapterlib::format::{InputBuffer, ParseError, Parser};
use feldera_adapterlib::test::{ConnectorTestEngine, InputOptions, Updates};
use feldera_types::config::FormatConfig;
use feldera_types::format::avro::{AvroParserConfig, AvroUpdateFormat};
use feldera_types::program_schema::{ColumnType, Field, Relation, SqlIdentifier};
use num_bigint::BigInt;
use proptest::collection::{btree_map, vec};
use proptest::prelude::*;
use serde_json::Value as JsonValue;

use crate::format::avro::schema::schema_json;
use crate::format::get_input_format;
use crate::test::avro::{self, Names};
use crate::test::{parser, published_schema, relations, rows};

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
pub fn non_null_to_nullable(engine: &dyn ConnectorTestEngine) {
    check_nullability(engine, &nullability_schema(r#""long""#, r#""string""#));
}

/// A nullable field fills a column that is not nullable.
pub fn nullable_to_non_nullable(engine: &dyn ConnectorTestEngine) {
    check_nullability(
        engine,
        &nullability_schema(r#"["long", "null"]"#, r#"["string", "null"]"#),
    );
}

/// A fixed-size field fills a BINARY(32) column, and a bytes field a VARBINARY column.
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

/// A Debezium zoned timestamp and a Debezium microsecond timestamp each fill a TIMESTAMP and a
/// TIMESTAMP WITH TIME ZONE column.
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
pub fn random_debezium_records(engine: &dyn ConnectorTestEngine) {
    let relation = assorted_relation();
    proptest!(ProptestConfig::with_cases(2), |(rows in assorted_rows(10000))| {
        let (config, messages, expected) =
            debezium_updates(&relation, &rows, ASSORTED_SCHEMA, "TestStruct2");
        check(engine, &relation, &InputOptions::default(), &config, &messages, &expected);
    });
}
