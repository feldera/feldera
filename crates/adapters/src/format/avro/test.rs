use super::{
    output::AvroEncoder,
    schema::schema_json,
    serializer::{AvroSchemaSerializer, avro_ser_config},
};
use crate::{
    Encoder, FormatConfig, ParseError, SerBatch,
    format::{InputBuffer, Parser, avro::from_avro_value},
    static_compile::seroutput::SerBatchImpl,
    test::{
        KeyStruct, MockOutputConsumer, MockUpdate, TestStruct, TestStruct2, generate_test_batches,
        generate_test_batches_with_weights, mock_parser_pipeline,
    },
};
use crate::{catalog::SerBatchReader, util::run_in_posix_runtime};
use apache_avro::{
    Schema as AvroSchema, from_avro_datum, schema::ResolvedSchema, to_avro_datum, types::Value,
};
use chrono::{DateTime, Utc};
use dbsp::trace::BatchReaderFactories;
use dbsp::typed_batch::{DynSpineSnapshot, SpineSnapshot as TypedSpineSnapshot, TypedBatch};
use dbsp::{DBData, IndexedZSetReader, OrdIndexedZSet, OrdZSet, ZWeight, utils::Tup2};
use feldera_adapterlib::transport::OutputBatchType;
use feldera_sqllib::{ByteArray, Date, SqlDecimal, Time, Timestamp, TimestampTz, Uuid, Variant};
use feldera_types::{
    deserialize_table_record,
    format::avro::{AvroEncoderConfig, AvroEncoderKeyMode},
    program_schema::{ColumnType, Field, Relation, SqlIdentifier},
    serde_with_context::{DeserializeWithContext, SerializeWithContext, SqlSerdeConfig},
    serialize_table_record,
};
use feldera_types::{
    format::avro::{AvroParserConfig, AvroUpdateFormat},
    serialize_struct,
};
use itertools::Itertools;
use num_bigint::BigInt;
use proptest::prelude::*;
use proptest::proptest;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::Serialize;
use size_of::SizeOf;
use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
    fmt::Debug,
    hash::Hash,
};
use std::{iter::repeat, sync::Arc};

#[derive(Debug)]
struct TestCase<T> {
    relation_schema: Relation,
    config: AvroParserConfig,
    /// Input data, expected result.
    input_batches: Vec<(Vec<u8>, Vec<ParseError>)>,
    /// Expected contents at the end of the test.
    expected_output: Vec<MockUpdate<T, ()>>,
}

#[derive(Debug, Default, Serialize)]
struct DebeziumSource {
    version: String,
    connector: String,
    name: String,
    ts_ms: i64,
    snapshot: Option<()>,
    db: String,
    sequence: Option<String>,
    schema: String,
    table: String,
    tx_id: Option<i64>,
    lsn: Option<i64>,
    xmin: Option<i64>,
}

serialize_struct!(DebeziumSource()[12]{
    version["version"]: String,
    connector["connector"]: String,
    name["name"]: String,
    ts_ms["ts_ms"]: i64,
    snapshot["snapshot"]: Option<()>,
    db["db"]: String,
    sequence["sequence"]: Option<String>,
    schema["schema"]: String,
    table["table"]: String,
    tx_id["txId"]: Option<i64>,
    lsn["lsn"]: Option<i64>,
    xmin["xmin"]: Option<i64>
});

#[derive(Debug, Serialize)]
struct DebeziumMessage<T> {
    before: Option<T>,
    after: Option<T>,
    source: DebeziumSource,
    op: String,
    ts_ms: Option<i64>,
    transaction: Option<()>,
}

serialize_struct!(DebeziumMessage(T)[6]{
    before["before"]: Option<T>,
    after["after"]: Option<T>,
    source["source"]: DebeziumSource,
    op["op"]: String,
    ts_ms["ts_ms"]: Option<i64>,
    transaction["transaction"]: Option<()>
});

impl<T> DebeziumMessage<T> {
    fn new(op: &str, before: Option<T>, after: Option<T>) -> Self {
        Self {
            before,
            after,
            source: Default::default(),
            op: op.to_string(),
            ts_ms: None,
            transaction: None,
        }
    }
}

/// Debezium message Avro schema with the specified inner record schema.
fn debezium_avro_schema(value_schema: &str, value_type_name: &str) -> AvroSchema {
    let schema_str = debezium_avro_schema_str(value_schema, value_type_name);

    println!("Debezium Avro schema: {schema_str}");

    AvroSchema::parse_str(&schema_str).unwrap()
}

/// Raw JSON of the Debezium envelope schema embedding the given inner record
/// schema. Unlike [`debezium_avro_schema`], the string is returned verbatim so
/// that Debezium `connect.name` annotations survive (parsing them into an
/// `AvroSchema` drops attributes on primitive types).
fn debezium_avro_schema_str(value_schema: &str, value_type_name: &str) -> String {
    // Note: placing `after` before `before` to trigger schema reference resolution.
    let schema_str = r#"{
    "type": "record",
    "name": "Envelope",
    "namespace": "test_namespace",
    "fields": [
        {
            "name": "after",
            "type": [
                "null",
                VALUE_SCHEMA
            ],
            "default": null
        },
        {
            "name": "before",
            "type": [
                "null",
                "VALUE_TYPE"
            ],
            "default": null
        },
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
                    { "name": "schema", "type": "string" }, { "name": "table", "type": "string" },
                    { "name": "txId", "type": [ "null", "long" ], "default": null },
                    { "name": "lsn", "type": [ "null", "long" ], "default": null },
                    { "name": "xmin", "type": [ "null", "long" ], "default": null }
                ],
                "connect.name": "io.debezium.connector.postgresql.Source"
            }
        },
        {
            "name": "op",
            "type": "string"
        },
        {
            "name": "ts_ms",
            "type": [
                "null",
                "long"
            ],
            "default": null
        },
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
}"#.replace("VALUE_SCHEMA", value_schema).replace("VALUE_TYPE", value_type_name);

    schema_str
}

fn serialize_record<T>(x: &T, schema: &AvroSchema) -> Vec<u8>
where
    T: Clone
        + Debug
        + Eq
        + SerializeWithContext<SqlSerdeConfig>
        + for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>
        + Send
        + 'static,
{
    let refs = HashMap::new();
    let serializer = AvroSchemaSerializer::new(schema, &refs, false);
    let val = x
        .serialize_with_context(serializer, &avro_ser_config())
        .unwrap();
    serialize_value(val, schema)
}

fn serialize_value(x: Value, schema: &AvroSchema) -> Vec<u8> {
    // 5-byte header
    let mut buffer = vec![0; 5];
    let mut avro_record = to_avro_datum(schema, x).unwrap();
    buffer.append(&mut avro_record);
    buffer
}

/// Generate a test case using raw Avro update format.
fn gen_raw_parser_test<T>(
    data: &[T],
    relation_schema: &Relation,
    avro_schema_str: &str,
) -> TestCase<T>
where
    T: Clone
        + Debug
        + Eq
        + SerializeWithContext<SqlSerdeConfig>
        + for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>
        + Send
        + 'static,
{
    let config = AvroParserConfig {
        update_format: AvroUpdateFormat::Raw,
        schema: Some(avro_schema_str.to_string()),
        skip_schema_id: false,
        registry_config: Default::default(),
    };

    let avro_schema = AvroSchema::parse_str(avro_schema_str).unwrap();

    let input_batches = data
        .iter()
        .map(|x| {
            let buffer = serialize_record(x, &avro_schema);
            (buffer, vec![])
        })
        .collect::<Vec<_>>();

    let expected_output = data
        .iter()
        .map(|x| MockUpdate::Insert(x.clone()))
        .collect::<Vec<_>>();

    TestCase {
        relation_schema: relation_schema.clone(),
        config,
        input_batches,
        expected_output,
    }
}

/// Generate a test case using Debezium Avro update format.
fn gen_debezium_parser_test<T>(
    data: &[T],
    relation_schema: &Relation,
    avro_schema_str: &str,
    type_name: &str,
) -> TestCase<T>
where
    T: Clone
        + Debug
        + Eq
        + SerializeWithContext<SqlSerdeConfig>
        + for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>
        + Send
        + 'static,
{
    let debezium_schema = debezium_avro_schema(avro_schema_str, type_name);
    let resolved = ResolvedSchema::try_from(&debezium_schema).unwrap();

    let config = AvroParserConfig {
        update_format: AvroUpdateFormat::Debezium,
        schema: Some(schema_json(&debezium_schema)),
        skip_schema_id: false,
        registry_config: Default::default(),
    };

    let input_batches = data
        .iter()
        .map(|x| {
            // 5-byte header
            let mut buffer = vec![0; 5];
            let serializer =
                AvroSchemaSerializer::new(&debezium_schema, resolved.get_names(), true);
            let dbz_message = DebeziumMessage::new("u", Some(x.clone()), Some(x.clone()));
            let val = dbz_message
                .serialize_with_context(serializer, &avro_ser_config())
                .unwrap();
            let mut avro_record = to_avro_datum(&debezium_schema, val).unwrap();
            buffer.append(&mut avro_record);
            (buffer, vec![])
        })
        .collect::<Vec<_>>();

    let expected_output = data
        .iter()
        .flat_map(|x| vec![MockUpdate::Delete(x.clone()), MockUpdate::Insert(x.clone())])
        .collect::<Vec<_>>();

    TestCase {
        relation_schema: relation_schema.clone(),
        config,
        input_batches,
        expected_output,
    }
}

fn run_parser_test<T>(test_cases: Vec<TestCase<T>>)
where
    T: Debug
        + Eq
        + for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>
        + Hash
        + Send
        + Sync
        + Debug
        + Clone
        + 'static,
{
    for test in test_cases {
        let format_config = FormatConfig {
            name: Cow::from("avro"),
            config: serde_json::to_value(test.config).unwrap(),
        };

        let (consumer, mut parser, outputs) =
            mock_parser_pipeline(&test.relation_schema, &format_config).unwrap();
        consumer.on_error(Some(Box::new(|_, _| {})));
        for (avro, expected_errors) in test.input_batches {
            let (mut buffer, errors) = parser.parse(&avro, None);
            assert_eq!(&errors, &expected_errors);
            buffer.flush();
        }
        assert_eq!(&test.expected_output, &outputs.state().flushed);
    }
}

#[test]
fn test_raw_avro_parser() {
    let test_case = gen_raw_parser_test(
        &TestStruct2::data(),
        &TestStruct2::relation_schema(),
        TestStruct2::avro_schema(),
    );

    run_parser_test(vec![test_case])
}

#[test]
fn test_debezium_avro_parser() {
    let test_case = gen_debezium_parser_test(
        &TestStruct2::data(),
        &TestStruct2::relation_schema(),
        TestStruct2::avro_schema(),
        "TestStruct2",
    );

    run_parser_test(vec![test_case])
}

/// SQL table can have nullable columns that are not in the Avro schema.
#[test]
fn test_extra_columns() {
    // Schema sans one field.
    let schema_str = r#"{
        "type": "record",
        "name": "TestStruct2Short",
        "fields": [
            { "name": "id", "type": "long" },
            { "name": "b", "type": "boolean" },
            { "name": "ts", "type": "long", "logicalType": "timestamp-micros" },
            { "name": "dt", "type": "int", "logicalType": "date" },
            {
                "name": "es",
                "type":
                    {
                        "type": "record",
                        "name": "EmbeddedStruct",
                        "fields": [
                            { "name": "a", "type": "boolean" }
                        ]
                    }
            },
            {
                "name": "m",
                "type":
                    {
                        "type": "map",
                        "values": "long"
                    }
            },
            {
                "name": "dec",
                "type": {
                    "type": "bytes",
                    "logicalType": "decimal",
                    "precision": 10,
                    "scale": 3
                }
            }
        ]
    }"#;

    let schema = AvroSchema::parse_str(schema_str).unwrap();
    let vals = TestStruct2::data();
    let input_batches = vals
        .iter()
        .map(|v| (serialize_record(v, &schema), vec![]))
        .collect::<Vec<_>>();
    let expected_output = vals
        .iter()
        .map(|v| {
            let mut v = v.clone();
            // set missing field to NULL
            v.field_0 = None;
            MockUpdate::Insert(v)
        })
        .collect::<Vec<_>>();

    let test = TestCase {
        relation_schema: TestStruct2::relation_schema(),
        config: AvroParserConfig {
            update_format: AvroUpdateFormat::Raw,
            schema: Some(schema_str.to_string()),
            skip_schema_id: false,
            registry_config: Default::default(),
        },
        input_batches,
        expected_output,
    };

    run_parser_test(vec![test]);
}

/// Deserializing non-optional fields into NULL-able columns.
#[test]
fn test_non_null_to_nullable() {
    // Make `name` column non-optional.
    let schema_str = r#"{
        "type": "record",
        "name": "TestStruct2",
        "connect.name": "test_namespace.TestStruct2",
        "fields": [
            { "name": "id", "type": "long" },
            { "name": "nAmE", "type": "string" },
            { "name": "b", "type": "boolean" },
            { "name": "ts", "type": "long", "logicalType": "timestamp-micros" },
            { "name": "dt", "type": "int", "logicalType": "date" },
            {
                "name": "es",
                "type":
                    [{
                        "type": "record",
                        "name": "EmbeddedStruct",
                        "fields": [
                            { "name": "a", "type": "boolean" }
                        ]
                    }, "null"]
            },
            {
                "name": "m",
                "type":
                    [{
                        "type": "map",
                        "values": "long"
                    }, "null"]
            },
            {
                "name": "dec",
                "type": {
                    "type": "bytes",
                    "logicalType": "decimal",
                    "precision": 10,
                    "scale": 3
                }
            }

        ]
    }"#;

    let schema = AvroSchema::parse_str(schema_str).unwrap();
    let vals = [TestStruct2 {
        field: 1,
        field_0: Some("test".to_string()),
        ..Default::default()
    }];
    let input_batches = vals
        .iter()
        .map(|v| (serialize_record(v, &schema), vec![]))
        .collect::<Vec<_>>();
    let expected_output = vals
        .iter()
        .map(|v| MockUpdate::Insert(v.clone()))
        .collect::<Vec<_>>();

    let test = TestCase {
        relation_schema: TestStruct2::relation_schema(),
        config: AvroParserConfig {
            update_format: AvroUpdateFormat::Raw,
            schema: Some(schema_str.to_string()),
            skip_schema_id: false,
            registry_config: Default::default(),
        },
        input_batches,
        expected_output,
    };

    run_parser_test(vec![test]);
}

/// Deserializing optional fields into non-NULL-able columns.
#[test]
fn test_nullable_to_non_nullable() {
    // Make `name` column non-optional.
    let schema_str = r#"{
        "type": "record",
        "name": "TestStruct2",
        "connect.name": "test_namespace.TestStruct2",
        "fields": [
            { "name": "id", "type": ["long", "null"] },
            { "name": "nAmE", "type": ["string", "null"] },
            { "name": "b", "type": "boolean" },
            { "name": "ts", "type": "long", "logicalType": "timestamp-micros" },
            { "name": "dt", "type": "int", "logicalType": "date" },
            {
                "name": "es",
                "type":
                    [{
                        "type": "record",
                        "name": "EmbeddedStruct",
                        "fields": [
                            { "name": "a", "type": "boolean" }
                        ]
                    }, "null"]
            },
            {
                "name": "m",
                "type":
                    [{
                        "type": "map",
                        "values": "long"
                    }, "null"]
            },
            {
                "name": "dec",
                "type": {
                    "type": "bytes",
                    "logicalType": "decimal",
                    "precision": 10,
                    "scale": 3
                }
            }

        ]
    }"#;

    let schema = AvroSchema::parse_str(schema_str).unwrap();
    let vals = [TestStruct2 {
        field: 1,
        field_0: Some("test".to_string()),
        ..Default::default()
    }];
    let input_batches = vals
        .iter()
        .map(|v| (serialize_record(v, &schema), vec![]))
        .collect::<Vec<_>>();
    let expected_output = vals
        .iter()
        .map(|v| MockUpdate::Insert(v.clone()))
        .collect::<Vec<_>>();

    let test = TestCase {
        relation_schema: TestStruct2::relation_schema(),
        config: AvroParserConfig {
            update_format: AvroUpdateFormat::Raw,
            schema: Some(schema_str.to_string()),
            skip_schema_id: false,
            registry_config: Default::default(),
        },
        input_batches,
        expected_output,
    };

    run_parser_test(vec![test]);
}

#[derive(
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    serde::Serialize,
    Clone,
    Hash,
    SizeOf,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
struct TestBinary {
    binary32: ByteArray,
    varbinary: ByteArray,
}

impl TestBinary {
    pub fn avro_schema() -> &'static str {
        r#"{
            "type": "record",
            "name": "TestBinary",
            "connect.name": "test_namespace.TestBinary",
            "fields": [
                { "name": "binary32", "type": {"type": "fixed", "name": "binary32", "size": 32} },
                { "name": "varbinary", "type": "bytes" }
            ]
        }"#
    }

    pub fn schema() -> Vec<Field> {
        vec![
            Field::new("binary32".into(), ColumnType::fixed(32, false)),
            Field::new("varbinary".into(), ColumnType::varbinary(false)),
        ]
    }

    pub fn relation_schema() -> Relation {
        Relation {
            name: SqlIdentifier::new("TestBinary", false),
            fields: Self::schema(),
            materialized: false,
            properties: BTreeMap::new(),
            primary_key: None,
        }
    }
}

serialize_table_record!(TestBinary[2]{
    r#binary32["binary32"]: ByteArray,
    r#varbinary["varbinary"]: ByteArray
});

deserialize_table_record!(TestBinary["TestBinary", Variant, 2] {
    (r#binary32, "binary32", false, ByteArray, |_| None),
    (r#varbinary, "varbinary", false, ByteArray, |_| None)
});

#[test]
fn test_parse_binary() {
    let schema = AvroSchema::parse_str(TestBinary::avro_schema()).unwrap();
    let vals = [TestBinary {
        binary32: ByteArray::new(b"012345678901234567890123456789ab"),
        varbinary: ByteArray::new(b"foobar"),
    }];
    let input_batches = vals
        .iter()
        .map(|v| (serialize_record(v, &schema), vec![]))
        .collect::<Vec<_>>();
    let expected_output = vals
        .iter()
        .map(|v| MockUpdate::Insert(v.clone()))
        .collect::<Vec<_>>();

    let test = TestCase {
        relation_schema: TestBinary::relation_schema(),
        config: AvroParserConfig {
            update_format: AvroUpdateFormat::Raw,
            schema: Some(TestBinary::avro_schema().to_string()),
            skip_schema_id: false,
            registry_config: Default::default(),
        },
        input_batches,
        expected_output,
    };

    run_parser_test(vec![test]);
}

#[derive(
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Clone,
    Hash,
    SizeOf,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
struct TestUuid {
    uuid1: Uuid,
    uuid2: Uuid,
    varchar: String,
}

impl TestUuid {
    pub fn avro_schema() -> &'static str {
        r#"{
            "type": "record",
            "name": "TestUuid",
            "connect.name": "test_namespace.TestUuid",
            "fields": [
                { "name": "uuid1", "type": "string" },
                { "name": "uuid2", "type": {"type": "string", "logicalType": "uuid"} },
                { "name": "varchar", "type": {"type": "string", "logicalType": "uuid"} }
            ]
        }"#
    }

    pub fn schema() -> Vec<Field> {
        vec![
            Field::new("uuid1".into(), ColumnType::uuid(false)),
            Field::new("uuid2".into(), ColumnType::uuid(false)),
            Field::new("varchar".into(), ColumnType::varchar(false)),
        ]
    }

    pub fn relation_schema() -> Relation {
        Relation {
            name: SqlIdentifier::new("TestUuid", false),
            fields: Self::schema(),
            materialized: false,
            properties: BTreeMap::new(),
            primary_key: None,
        }
    }
}

serialize_table_record!(TestUuid[3]{
    r#uuid1["uuid1"]: Uuid,
    r#uuid2["uuid2"]: Uuid,
    r#varchar["varchar"]: String
});

deserialize_table_record!(TestUuid["TestUuid", Variant, 3] {
    (r#uuid1, "uuid1", false, Uuid, |_| None),
    (r#uuid2, "uuid2", false, Uuid, |_| None),
    (r#varchar, "varchar", false, String, |_| None)
});

// Test for issue #4722: make sure that we can deserialize UUIDs from both plain string and logicalType uuid.
// Test for issue #4837: deserialize logical UUID type into a string.
#[test]
fn test_issue4722_issue4837() {
    let schema = AvroSchema::parse_str(TestUuid::avro_schema()).unwrap();
    let vals = [TestUuid {
        uuid1: Uuid::from(uuid::uuid!("550e8400-e29b-41d4-a716-446655440000")),
        uuid2: Uuid::from(uuid::uuid!("550e8400-e29b-41d4-a716-446655440001")),
        varchar: "550e8400-e29b-41d4-a716-446655440002".to_string(),
    }];
    let input_batches = vals
        .iter()
        .map(|v| (serialize_record(v, &schema), vec![]))
        .collect::<Vec<_>>();
    let expected_output = vals
        .iter()
        .map(|v| MockUpdate::Insert(v.clone()))
        .collect::<Vec<_>>();

    let test = TestCase {
        relation_schema: TestUuid::relation_schema(),
        config: AvroParserConfig {
            update_format: AvroUpdateFormat::Raw,
            schema: Some(TestUuid::avro_schema().to_string()),
            skip_schema_id: false,
            registry_config: Default::default(),
        },
        input_batches,
        expected_output,
    };

    run_parser_test(vec![test]);
}

#[derive(
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Clone,
    Hash,
    SizeOf,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
struct TestEnum {
    enum_val: String,
}

impl TestEnum {
    pub fn avro_schema() -> &'static str {
        r#"{
            "type": "record",
            "name": "TestEnum",
            "connect.name": "test_namespace.TestEnum",
            "fields": [
                { "name": "enum_val", "type": { "type": "enum", "name": "Suit", "symbols" : ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"] } }
            ]
        }"#
    }

    pub fn schema() -> Vec<Field> {
        vec![Field::new("enum_val".into(), ColumnType::varchar(false))]
    }

    pub fn relation_schema() -> Relation {
        Relation {
            name: SqlIdentifier::new("TestEnum", false),
            fields: Self::schema(),
            materialized: false,
            properties: BTreeMap::new(),
            primary_key: None,
        }
    }
}

serialize_table_record!(TestEnum[1]{
    r#enum_val["enum_val"]: String
});

deserialize_table_record!(TestEnum["TestEnum", Variant, 1] {
    (r#enum_val, "enum_val", false, String, |_| None)
});

#[test]
fn test_enums() {
    let schema = AvroSchema::parse_str(TestEnum::avro_schema()).unwrap();
    let vals = [TestEnum {
        enum_val: "SPADES".to_string(),
    }];

    let input_batches = vec![(
        serialize_value(
            Value::Record(vec![(
                "enum_val".to_string(),
                Value::Enum(0, "SPADES".to_string()),
            )]),
            &schema,
        ),
        vec![],
    )];
    let expected_output = vals
        .iter()
        .map(|v| MockUpdate::Insert(v.clone()))
        .collect::<Vec<_>>();

    let test = TestCase {
        relation_schema: TestEnum::relation_schema(),
        config: AvroParserConfig {
            update_format: AvroUpdateFormat::Raw,
            schema: Some(TestEnum::avro_schema().to_string()),
            skip_schema_id: false,
            registry_config: Default::default(),
        },
        input_batches,
        expected_output,
    };

    run_parser_test(vec![test]);
}

#[derive(
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Clone,
    Hash,
    SizeOf,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
struct TestMetadata {
    id: i64,
    schema_id: Option<u32>,
}

impl TestMetadata {
    pub fn avro_schema() -> &'static str {
        r#"{
            "type": "record",
            "name": "TestMetadata",
            "connect.name": "test_namespace.TestMetadata",
            "fields": [
                { "name": "id", "type": "long" }
            ]
        }"#
    }

    pub fn schema() -> Vec<Field> {
        vec![
            Field::new("id".into(), ColumnType::bigint(false)),
            Field::new("schema_id".into(), ColumnType::uint(true)),
        ]
    }

    pub fn relation_schema() -> Relation {
        Relation {
            name: SqlIdentifier::new("TestMetadata", false),
            fields: Self::schema(),
            materialized: false,
            properties: BTreeMap::new(),
            primary_key: None,
        }
    }
}

serialize_table_record!(TestMetadata[2]{
    r#id["id"]: i64,
    r#schema_id["schema_id"]: Option<u32>
});

deserialize_table_record!(TestMetadata["TestMetadata", Variant, 2] {
    (r#id, "id", false, i64, |_| None),
    (r#schema_id, "schema_id", true, Option<u32>,  |metadata: &Option<Variant>| metadata.as_ref().map(|metadata| u32::try_from(metadata.index_string("avro_schema_id")).ok()))
});

#[test]
fn test_metadata() {
    let schema = AvroSchema::parse_str(TestMetadata::avro_schema()).unwrap();
    let vals = [TestMetadata {
        id: 5,
        schema_id: Some(0),
    }];

    let input_batches = vec![(
        serialize_value(
            Value::Record(vec![("id".to_string(), Value::Long(5))]),
            &schema,
        ),
        vec![],
    )];
    let expected_output = vals
        .iter()
        .map(|v| MockUpdate::Insert(v.clone()))
        .collect::<Vec<_>>();

    let test = TestCase {
        relation_schema: TestMetadata::relation_schema(),
        config: AvroParserConfig {
            update_format: AvroUpdateFormat::Raw,
            schema: Some(TestMetadata::avro_schema().to_string()),
            skip_schema_id: false,
            registry_config: Default::default(),
        },
        input_batches,
        expected_output,
    };

    run_parser_test(vec![test]);
}

/// Deserialize timestamp encoded as timestamp-millis instead of micros.
#[test]
fn test_ms_time() {
    // timestamp-millis instead of timestamp-micros
    let schema_str = r#"{
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
                "type":
                    {
                        "type": "record",
                        "name": "EmbeddedStruct",
                        "fields": [
                            { "name": "a", "type": "boolean" }
                        ]
                    }
            },
            {
                "name": "m",
                "type":
                    {
                        "type": "map",
                        "values": "long"
                    }
            },
            {
                "name": "dec",
                "type": {
                    "type": "bytes",
                    "logicalType": "decimal",
                    "precision": 10,
                    "scale": 3
                }
            }
        ]
    }"#;

    let schema = AvroSchema::parse_str(schema_str).unwrap();
    let vals = TestStruct2::data();
    let input_batches = vals
        .iter()
        .map(|v| (serialize_record(v, &schema), vec![]))
        .collect::<Vec<_>>();
    let expected_output = vals
        .iter()
        .map(|v| MockUpdate::Insert(v.clone()))
        .collect::<Vec<_>>();

    let test = TestCase {
        relation_schema: TestStruct2::relation_schema(),
        config: AvroParserConfig {
            update_format: AvroUpdateFormat::Raw,
            schema: Some(schema_str.to_string()),
            skip_schema_id: false,
            registry_config: Default::default(),
        },
        input_batches,
        expected_output,
    };

    run_parser_test(vec![test]);
}

/// Raw wire representation of a row carrying every Debezium temporal type.
///
/// The fields hold the exact values Debezium puts on the wire (milliseconds,
/// microseconds, or nanoseconds since epoch/midnight, or ISO-8601 strings) so
/// that the test can encode a realistic Debezium message. The connector must
/// convert these into the [`TestTemporal`] representation on the way in.
#[derive(Debug, Clone, PartialEq, Eq)]
struct TestTemporalRaw {
    id: i64,
    ts_millis: i64,
    ts_micros: i64,
    ts_nanos: i64,
    ts_zoned: String,
    ts_connect: i64,
    ts_opt: Option<i64>,
    t_millis: i32,
    t_micros: i64,
    t_nanos: i64,
    t_zoned: String,
    t_connect: i32,
    dt: i32,
}

serialize_table_record!(TestTemporalRaw[13]{
    r#id["id"]: i64,
    r#ts_millis["ts_millis"]: i64,
    r#ts_micros["ts_micros"]: i64,
    r#ts_nanos["ts_nanos"]: i64,
    r#ts_zoned["ts_zoned"]: String,
    r#ts_connect["ts_connect"]: i64,
    r#ts_opt["ts_opt"]: Option<i64>,
    r#t_millis["t_millis"]: i32,
    r#t_micros["t_micros"]: i64,
    r#t_nanos["t_nanos"]: i64,
    r#t_zoned["t_zoned"]: String,
    r#t_connect["t_connect"]: i32,
    r#dt["dt"]: i32
});

deserialize_table_record!(TestTemporalRaw["Temporal", Variant, 13] {
    (r#id, "id", false, i64, |_| None),
    (r#ts_millis, "ts_millis", false, i64, |_| None),
    (r#ts_micros, "ts_micros", false, i64, |_| None),
    (r#ts_nanos, "ts_nanos", false, i64, |_| None),
    (r#ts_zoned, "ts_zoned", false, String, |_| None),
    (r#ts_connect, "ts_connect", false, i64, |_| None),
    (r#ts_opt, "ts_opt", true, Option<i64>, |_| Some(None)),
    (r#t_millis, "t_millis", false, i32, |_| None),
    (r#t_micros, "t_micros", false, i64, |_| None),
    (r#t_nanos, "t_nanos", false, i64, |_| None),
    (r#t_zoned, "t_zoned", false, String, |_| None),
    (r#t_connect, "t_connect", false, i32, |_| None),
    (r#dt, "dt", false, i32, |_| None)
});

/// Decoded representation of [`TestTemporalRaw`]: every temporal column parsed
/// into the matching Feldera type.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TestTemporal {
    id: i64,
    ts_millis: Timestamp,
    ts_micros: Timestamp,
    ts_nanos: Timestamp,
    ts_zoned: TimestampTz,
    ts_connect: Timestamp,
    ts_opt: Option<Timestamp>,
    t_millis: Time,
    t_micros: Time,
    t_nanos: Time,
    t_zoned: Time,
    t_connect: Time,
    dt: Date,
}

serialize_table_record!(TestTemporal[13]{
    r#id["id"]: i64,
    r#ts_millis["ts_millis"]: Timestamp,
    r#ts_micros["ts_micros"]: Timestamp,
    r#ts_nanos["ts_nanos"]: Timestamp,
    r#ts_zoned["ts_zoned"]: TimestampTz,
    r#ts_connect["ts_connect"]: Timestamp,
    r#ts_opt["ts_opt"]: Option<Timestamp>,
    r#t_millis["t_millis"]: Time,
    r#t_micros["t_micros"]: Time,
    r#t_nanos["t_nanos"]: Time,
    r#t_zoned["t_zoned"]: Time,
    r#t_connect["t_connect"]: Time,
    r#dt["dt"]: Date
});

deserialize_table_record!(TestTemporal["Temporal", Variant, 13] {
    (r#id, "id", false, i64, |_| None),
    (r#ts_millis, "ts_millis", false, Timestamp, |_| None),
    (r#ts_micros, "ts_micros", false, Timestamp, |_| None),
    (r#ts_nanos, "ts_nanos", false, Timestamp, |_| None),
    (r#ts_zoned, "ts_zoned", false, TimestampTz, |_| None),
    (r#ts_connect, "ts_connect", false, Timestamp, |_| None),
    (r#ts_opt, "ts_opt", true, Option<Timestamp>, |_| Some(None)),
    (r#t_millis, "t_millis", false, Time, |_| None),
    (r#t_micros, "t_micros", false, Time, |_| None),
    (r#t_nanos, "t_nanos", false, Time, |_| None),
    (r#t_zoned, "t_zoned", false, Time, |_| None),
    (r#t_connect, "t_connect", false, Time, |_| None),
    (r#dt, "dt", false, Date, |_| None)
});

impl TestTemporal {
    /// Avro value schema covering every supported Debezium temporal type. Each
    /// column is a plain `int`/`long`/`string` tagged with a Debezium
    /// `connect.name`, exactly as Debezium emits them.
    fn value_avro_schema() -> &'static str {
        r#"{
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
        }"#
    }

    fn schema() -> Vec<Field> {
        vec![
            Field::new("id".into(), ColumnType::bigint(false)),
            Field::new("ts_millis".into(), ColumnType::timestamp(false)),
            Field::new("ts_micros".into(), ColumnType::timestamp(false)),
            Field::new("ts_nanos".into(), ColumnType::timestamp(false)),
            Field::new("ts_zoned".into(), ColumnType::timestamp_tz(false)),
            Field::new("ts_connect".into(), ColumnType::timestamp(false)),
            Field::new("ts_opt".into(), ColumnType::timestamp(true)),
            Field::new("t_millis".into(), ColumnType::time(false)),
            Field::new("t_micros".into(), ColumnType::time(false)),
            Field::new("t_nanos".into(), ColumnType::time(false)),
            Field::new("t_zoned".into(), ColumnType::time(false)),
            Field::new("t_connect".into(), ColumnType::time(false)),
            Field::new("dt".into(), ColumnType::date(false)),
        ]
    }

    fn relation_schema() -> Relation {
        Relation {
            name: SqlIdentifier::new("Temporal", false),
            fields: Self::schema(),
            materialized: false,
            properties: BTreeMap::new(),
            primary_key: None,
        }
    }
}

/// Parse every Debezium temporal semantic type into the matching Feldera
/// `TIME`/`TIMESTAMP`/`DATE` column.
#[test]
fn test_debezium_temporal_types() {
    // Base instant with millisecond precision so its millisecond, microsecond,
    // and nanosecond encodings all denote the same point in time.
    let instant = DateTime::parse_from_rfc3339("2021-06-15T12:30:45.123Z")
        .unwrap()
        .with_timezone(&Utc);
    let ts_micros = instant.timestamp_micros();
    let ts_millis = ts_micros / 1_000;
    let ts_nanos = ts_micros * 1_000;
    let dt_days = (instant.timestamp() / 86_400) as i32;

    // Time of day 12:30:45.123, again at millisecond precision.
    let time_micros = (12 * 3_600 + 30 * 60 + 45) * 1_000_000 + 123_000;
    let time_millis = (time_micros / 1_000) as i32;
    let time_nanos = time_micros * 1_000;
    let time_as_time = Time::from_nanoseconds((time_micros * 1_000) as u64);

    let raw = TestTemporalRaw {
        id: 1,
        ts_millis,
        ts_micros,
        ts_nanos,
        // +02:00 wall clock for the same instant, to exercise offset handling.
        ts_zoned: "2021-06-15T14:30:45.123+02:00".to_string(),
        ts_connect: ts_millis,
        ts_opt: Some(ts_micros),
        t_millis: time_millis,
        t_micros: time_micros,
        t_nanos: time_nanos,
        t_zoned: "14:30:45.123+02:00".to_string(),
        t_connect: time_millis,
        dt: dt_days,
    };

    let expected = TestTemporal {
        id: 1,
        ts_millis: Timestamp::from_microseconds(ts_micros),
        ts_micros: Timestamp::from_microseconds(ts_micros),
        ts_nanos: Timestamp::from_microseconds(ts_micros),
        ts_zoned: TimestampTz::from_microseconds(ts_micros),
        ts_connect: Timestamp::from_microseconds(ts_micros),
        ts_opt: Some(Timestamp::from_microseconds(ts_micros)),
        t_millis: time_as_time,
        t_micros: time_as_time,
        t_nanos: time_as_time,
        t_zoned: time_as_time,
        t_connect: time_as_time,
        dt: Date::from_days(dt_days),
    };

    // Second row exercises a NULL value in the nullable timestamp column.
    let raw_null = TestTemporalRaw {
        id: 2,
        ts_opt: None,
        ..raw.clone()
    };
    let expected_null = TestTemporal {
        id: 2,
        ts_opt: None,
        ..expected.clone()
    };

    // Encode a Debezium message from the raw values. The envelope schema is
    // parsed here purely to drive Avro encoding; the connector receives the raw
    // schema string (below) with the `connect.name` annotations intact.
    let envelope_str = debezium_avro_schema_str(TestTemporal::value_avro_schema(), "Temporal");
    let envelope_schema = AvroSchema::parse_str(&envelope_str).unwrap();
    let resolved = ResolvedSchema::try_from(&envelope_schema).unwrap();

    let encode = |row: &TestTemporalRaw| {
        let mut buffer = vec![0; 5];
        let serializer = AvroSchemaSerializer::new(&envelope_schema, resolved.get_names(), true);
        let dbz_message = DebeziumMessage::new("u", Some(row.clone()), Some(row.clone()));
        let val = dbz_message
            .serialize_with_context(serializer, &avro_ser_config())
            .unwrap();
        let mut avro_record = to_avro_datum(&envelope_schema, val).unwrap();
        buffer.append(&mut avro_record);
        (buffer, vec![])
    };

    let test = TestCase {
        relation_schema: TestTemporal::relation_schema(),
        config: AvroParserConfig {
            update_format: AvroUpdateFormat::Debezium,
            schema: Some(envelope_str),
            skip_schema_id: false,
            registry_config: Default::default(),
        },
        input_batches: vec![encode(&raw), encode(&raw_null)],
        expected_output: vec![
            MockUpdate::Delete(expected.clone()),
            MockUpdate::Insert(expected.clone()),
            MockUpdate::Delete(expected_null.clone()),
            MockUpdate::Insert(expected_null.clone()),
        ],
    };

    run_parser_test(vec![test]);
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

/// Raw wire representation of the same instant encoded four ways: as a
/// `ZonedTimestamp` (string) and a `MicroTimestamp` (long), each destined for
/// both a `TIMESTAMP` and a `TIMESTAMP WITH TIME ZONE` column.
#[derive(Debug, Clone)]
struct TsTargetsRaw {
    z_ts: String,
    z_tstz: String,
    n_ts: i64,
    n_tstz: i64,
}

serialize_table_record!(TsTargetsRaw[4]{
    r#z_ts["z_ts"]: String,
    r#z_tstz["z_tstz"]: String,
    r#n_ts["n_ts"]: i64,
    r#n_tstz["n_tstz"]: i64
});

/// Decoded form of [`TsTargetsRaw`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TsTargets {
    z_ts: Timestamp,
    z_tstz: TimestampTz,
    n_ts: Timestamp,
    n_tstz: TimestampTz,
}

deserialize_table_record!(TsTargets["TsTargets", Variant, 4] {
    (r#z_ts, "z_ts", false, Timestamp, |_| None),
    (r#z_tstz, "z_tstz", false, TimestampTz, |_| None),
    (r#n_ts, "n_ts", false, Timestamp, |_| None),
    (r#n_tstz, "n_tstz", false, TimestampTz, |_| None)
});

impl TsTargets {
    fn value_avro_schema() -> &'static str {
        r#"{
            "type": "record",
            "name": "TsTargets",
            "connect.name": "test_namespace.TsTargets",
            "fields": [
                { "name": "z_ts", "type": { "type": "string", "connect.name": "io.debezium.time.ZonedTimestamp" } },
                { "name": "z_tstz", "type": { "type": "string", "connect.name": "io.debezium.time.ZonedTimestamp" } },
                { "name": "n_ts", "type": { "type": "long", "connect.name": "io.debezium.time.MicroTimestamp" } },
                { "name": "n_tstz", "type": { "type": "long", "connect.name": "io.debezium.time.MicroTimestamp" } }
            ]
        }"#
    }

    fn relation_schema() -> Relation {
        Relation {
            name: SqlIdentifier::new("TsTargets", false),
            fields: vec![
                Field::new("z_ts".into(), ColumnType::timestamp(false)),
                Field::new("z_tstz".into(), ColumnType::timestamp_tz(false)),
                Field::new("n_ts".into(), ColumnType::timestamp(false)),
                Field::new("n_tstz".into(), ColumnType::timestamp_tz(false)),
            ],
            materialized: false,
            properties: BTreeMap::new(),
            primary_key: None,
        }
    }
}

/// A Debezium timestamp type deserializes into either a `TIMESTAMP` or a
/// `TIMESTAMP WITH TIME ZONE` column. This covers both the string-encoded
/// `ZonedTimestamp` and a numeric type against both column types.
#[test]
fn test_debezium_timestamp_column_targets() {
    let instant = DateTime::parse_from_rfc3339("2021-06-15T12:30:45.123Z")
        .unwrap()
        .with_timezone(&Utc);
    let micros = instant.timestamp_micros();
    // Same instant expressed in a +02:00 offset.
    let zoned = "2021-06-15T14:30:45.123+02:00".to_string();

    let raw = TsTargetsRaw {
        z_ts: zoned.clone(),
        z_tstz: zoned.clone(),
        n_ts: micros,
        n_tstz: micros,
    };
    let expected = TsTargets {
        z_ts: Timestamp::from_microseconds(micros),
        z_tstz: TimestampTz::from_microseconds(micros),
        n_ts: Timestamp::from_microseconds(micros),
        n_tstz: TimestampTz::from_microseconds(micros),
    };

    let envelope_str = debezium_avro_schema_str(TsTargets::value_avro_schema(), "TsTargets");
    let envelope_schema = AvroSchema::parse_str(&envelope_str).unwrap();
    let resolved = ResolvedSchema::try_from(&envelope_schema).unwrap();

    let mut buffer = vec![0; 5];
    let serializer = AvroSchemaSerializer::new(&envelope_schema, resolved.get_names(), true);
    let dbz_message = DebeziumMessage::new("u", Some(raw.clone()), Some(raw.clone()));
    let val = dbz_message
        .serialize_with_context(serializer, &avro_ser_config())
        .unwrap();
    let mut avro_record = to_avro_datum(&envelope_schema, val).unwrap();
    buffer.append(&mut avro_record);

    let test = TestCase {
        relation_schema: TsTargets::relation_schema(),
        config: AvroParserConfig {
            update_format: AvroUpdateFormat::Debezium,
            schema: Some(envelope_str),
            skip_schema_id: false,
            registry_config: Default::default(),
        },
        input_batches: vec![(buffer, vec![])],
        expected_output: vec![
            MockUpdate::Delete(expected.clone()),
            MockUpdate::Insert(expected.clone()),
        ],
    };

    run_parser_test(vec![test]);
}

/// Raw wire representation of a Debezium `VariableScaleDecimal`: a record with
/// the scale and the big-endian two's-complement unscaled value.
#[derive(Debug, Clone)]
struct VariableScaleDecimalRaw {
    scale: i32,
    value: ByteArray,
}

serialize_table_record!(VariableScaleDecimalRaw[2]{
    r#scale["scale"]: i32,
    r#value["value"]: ByteArray
});

/// Raw wire representation of a row with variable-scale decimal columns, one of
/// them nullable and encoded via a by-name reference to the same record type.
#[derive(Debug, Clone)]
struct TestDecimalRaw {
    id: i64,
    amount: VariableScaleDecimalRaw,
    amount_opt: Option<VariableScaleDecimalRaw>,
}

serialize_table_record!(TestDecimalRaw[3]{
    r#id["id"]: i64,
    r#amount["amount"]: VariableScaleDecimalRaw,
    r#amount_opt["amount_opt"]: Option<VariableScaleDecimalRaw>
});

/// Decoded representation of [`TestDecimalRaw`]: every variable-scale decimal
/// parsed into a Feldera `DECIMAL` column.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TestDecimal {
    id: i64,
    amount: SqlDecimal<10, 3>,
    amount_opt: Option<SqlDecimal<10, 3>>,
}

deserialize_table_record!(TestDecimal["Dec", Variant, 3] {
    (r#id, "id", false, i64, |_| None),
    (r#amount, "amount", false, SqlDecimal<10, 3>, |_| None),
    (r#amount_opt, "amount_opt", true, Option<SqlDecimal<10, 3>>, |_| Some(None))
});

impl TestDecimal {
    /// Avro value schema with two `VariableScaleDecimal` columns; `amount_opt`
    /// references the record type by name, exercising reference resolution.
    fn value_avro_schema() -> &'static str {
        r#"{
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
        }"#
    }

    fn relation_schema() -> Relation {
        Relation {
            name: SqlIdentifier::new("Dec", false),
            fields: vec![
                Field::new("id".into(), ColumnType::bigint(false)),
                Field::new("amount".into(), ColumnType::decimal(10, 3, false)),
                Field::new("amount_opt".into(), ColumnType::decimal(10, 3, true)),
            ],
            materialized: false,
            properties: BTreeMap::new(),
            primary_key: None,
        }
    }
}

/// Parse Debezium `io.debezium.data.VariableScaleDecimal` records into Feldera
/// `DECIMAL` columns, including a nullable column and a by-name type reference.
#[test]
fn test_debezium_variable_scale_decimal() {
    let raw_dec = |scale: i32, unscaled: i64| VariableScaleDecimalRaw {
        scale,
        value: ByteArray::new(&BigInt::from(unscaled).to_signed_bytes_be()),
    };

    // The wire scale differs from the column scale (3), which is the whole point
    // of the variable-scale type.
    let row1 = TestDecimalRaw {
        id: 1,
        amount: raw_dec(2, 12345),           // 123.45
        amount_opt: Some(raw_dec(3, -6789)), // -6.789
    };
    let row2 = TestDecimalRaw {
        id: 2,
        amount: raw_dec(0, 42), // 42
        amount_opt: None,
    };

    let expected1 = TestDecimal {
        id: 1,
        amount: SqlDecimal::<10, 3>::new(12345, 2).unwrap(),
        amount_opt: Some(SqlDecimal::<10, 3>::new(-6789, 3).unwrap()),
    };
    let expected2 = TestDecimal {
        id: 2,
        amount: SqlDecimal::<10, 3>::new(42, 0).unwrap(),
        amount_opt: None,
    };

    let envelope_str = debezium_avro_schema_str(TestDecimal::value_avro_schema(), "Dec");
    let envelope_schema = AvroSchema::parse_str(&envelope_str).unwrap();
    let resolved = ResolvedSchema::try_from(&envelope_schema).unwrap();

    let encode = |row: &TestDecimalRaw| {
        let mut buffer = vec![0; 5];
        let serializer = AvroSchemaSerializer::new(&envelope_schema, resolved.get_names(), true);
        let dbz_message = DebeziumMessage::new("u", Some(row.clone()), Some(row.clone()));
        let val = dbz_message
            .serialize_with_context(serializer, &avro_ser_config())
            .unwrap();
        let mut avro_record = to_avro_datum(&envelope_schema, val).unwrap();
        buffer.append(&mut avro_record);
        (buffer, vec![])
    };

    let test = TestCase {
        relation_schema: TestDecimal::relation_schema(),
        config: AvroParserConfig {
            update_format: AvroUpdateFormat::Debezium,
            schema: Some(envelope_str),
            skip_schema_id: false,
            registry_config: Default::default(),
        },
        input_batches: vec![encode(&row1), encode(&row2)],
        expected_output: vec![
            MockUpdate::Delete(expected1.clone()),
            MockUpdate::Insert(expected1.clone()),
            MockUpdate::Delete(expected2.clone()),
            MockUpdate::Insert(expected2.clone()),
        ],
    };

    run_parser_test(vec![test]);
}

/// Type used to serialize different integer types as Avro `int`.
#[derive(
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    serde::Serialize,
    Clone,
    Hash,
    SizeOf,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
struct TestIntConversionsSrc {
    uint: i32,
    ulong: i32,
    int: i32,
    long: i32,
}

impl TestIntConversionsSrc {
    pub fn avro_schema() -> &'static str {
        r#"{
            "type": "record",
            "name": "TestIntConversions",
            "connect.name": "test_namespace.TestIntConversions",
            "fields": [
                { "name": "uint", "type": "int" },
                { "name": "ulong", "type": "int" },
                { "name": "int", "type": "int" },
                { "name": "long", "type": "int" }
            ]
        }"#
    }

    pub fn schema() -> Vec<Field> {
        vec![
            Field::new("uint".into(), ColumnType::int(false)),
            Field::new("ulong".into(), ColumnType::int(false)),
            Field::new("int".into(), ColumnType::int(false)),
            Field::new("long".into(), ColumnType::int(false)),
        ]
    }

    pub fn relation_schema() -> Relation {
        Relation {
            name: SqlIdentifier::new("TestIntConversions", false),
            fields: Self::schema(),
            materialized: false,
            properties: BTreeMap::new(),
            primary_key: None,
        }
    }
}

serialize_table_record!(TestIntConversionsSrc[4]{
    r#uint["uint"]: i32,
    r#ulong["ulong"]: i32,
    r#int["int"]: i32,
    r#long["long"]: i32
});

deserialize_table_record!(TestIntConversionsSrc["TestIntConversions", Variant, 4] {
    (r#uint, "uint", false, i32, |_| None),
    (r#ulong, "ulong", false, i32, |_| None),
    (r#int, "int", false, i32, |_| None),
    (r#long, "long", false, i32, |_| None)
});

/// Type used to deserialize different integer types from Avro `int`.
#[derive(
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    serde::Serialize,
    Clone,
    Hash,
    SizeOf,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
struct TestIntConversionsDst {
    uint: u32,
    ulong: u64,
    int: i32,
    long: i64,
}

serialize_table_record!(TestIntConversionsDst[4]{
    r#uint["uint"]: u32,
    r#ulong["ulong"]: u64,
    r#int["int"]: i32,
    r#long["long"]: i64
});

deserialize_table_record!(TestIntConversionsDst["TestIntConversions", Variant, 4] {
    (r#uint, "uint", false, u32, |_| None),
    (r#ulong, "ulong", false, u64, |_| None),
    (r#int, "int", false, i32, |_| None),
    (r#long, "long", false, i64, |_| None)
});

/// Test for issue #4664: make sure that we can deserialize different 32-bit and 64-bit integer types from `int`.
#[test]
fn test_issue4664() {
    let input_vals = [TestIntConversionsSrc {
        uint: 1,
        ulong: 2,
        int: 3,
        long: 4,
    }];

    let output_vals = [TestIntConversionsDst {
        uint: 1,
        ulong: 2,
        int: 3,
        long: 4,
    }];

    let schema = AvroSchema::parse_str(TestIntConversionsSrc::avro_schema()).unwrap();

    let input_batches = input_vals
        .iter()
        .map(|v| (serialize_record(v, &schema), vec![]))
        .collect::<Vec<_>>();

    let expected_output = output_vals
        .iter()
        .map(|v| MockUpdate::Insert(v.clone()))
        .collect::<Vec<_>>();

    let test = TestCase {
        relation_schema: TestIntConversionsSrc::relation_schema(),
        config: AvroParserConfig {
            update_format: AvroUpdateFormat::Raw,
            schema: Some(TestIntConversionsSrc::avro_schema().to_string()),
            skip_schema_id: false,
            registry_config: Default::default(),
        },
        input_batches,
        expected_output: expected_output.clone(),
    };

    run_parser_test(vec![test]);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2))]

    #[test]
    fn proptest_raw_avro_parser(data in proptest::collection::vec(any::<TestStruct2>(), 0..=10000))
    {
        let test_case = gen_raw_parser_test(&data, &TestStruct2::relation_schema(),  TestStruct2::avro_schema());

        run_parser_test(vec![test_case])
    }

    #[test]
    fn proptest_debezium_avro_parser(data in proptest::collection::vec(any::<TestStruct2>(), 0..=10000))
    {
        let test_case = gen_debezium_parser_test(&data, &TestStruct2::relation_schema(), TestStruct2::avro_schema(), "TestStruct2");

        run_parser_test(vec![test_case])
    }

}

fn test_raw_avro_output<T>(config: AvroEncoderConfig, batches: Vec<Vec<Tup2<T, i64>>>)
where
    T: DBData
        + for<'de> DeserializeWithContext<'de, SqlSerdeConfig>
        + SerializeWithContext<SqlSerdeConfig>,
{
    let schema = AvroSchema::parse_str(config.schema.as_ref().unwrap()).unwrap();
    let consumer = MockOutputConsumer::new();
    let consumer_data = consumer.data.clone();
    let mut encoder = AvroEncoder::create(
        "avro_test_endpoint",
        &None,
        &Relation::empty(),
        Box::new(consumer),
        config,
        None,
        false,
    )
    .unwrap();
    let zsets = batches
        .iter()
        .map(|batch| {
            let zset = OrdZSet::from_keys((), batch.clone());
            Arc::new(<SerBatchImpl<_, T, ()>>::new(zset)) as Arc<dyn SerBatch>
        })
        .collect::<Vec<_>>();
    for (step, zset) in zsets.into_iter().enumerate() {
        encoder
            .consumer()
            .batch_start(step as u64, OutputBatchType::Delta);
        encoder.encode(zset.arc_as_batch_reader()).unwrap();
        encoder.consumer().batch_end();
    }

    let expected_output = OrdZSet::from_keys((), batches.concat().into_iter().collect());

    let actual_output = OrdZSet::from_keys(
        (),
        consumer_data
            .lock()
            .unwrap()
            .iter()
            .map(|(_k, v, headers)| {
                let val = from_avro_datum(&schema, &mut &v.as_ref().unwrap()[5..], None).unwrap();
                let value =
                    from_avro_value::<T, ()>(&val, &schema, &HashMap::new(), &None).unwrap();
                let w = if headers[0] == ("op".to_string(), Some(b"insert".to_vec())) {
                    1
                } else {
                    -1
                };
                Tup2(value, w)
            })
            .collect(),
    );

    assert_eq!(
        actual_output.iter().sorted().collect::<Vec<_>>(),
        expected_output.iter().sorted().collect::<Vec<_>>()
    );
}

fn test_raw_avro_output_indexed<K, T>(
    config: AvroEncoderConfig,
    key_sql_schema: &Relation,
    val_sql_schema: &Relation,
    key_func: impl Fn(&T) -> K,
    batches: Vec<Vec<(T, T)>>,
) where
    T: DBData
        + for<'de> DeserializeWithContext<'de, SqlSerdeConfig>
        + SerializeWithContext<SqlSerdeConfig>,
    K: DBData
        + for<'de> DeserializeWithContext<'de, SqlSerdeConfig>
        + SerializeWithContext<SqlSerdeConfig>,
{
    let consumer = MockOutputConsumer::new();
    let consumer_data = consumer.data.clone();
    let mut encoder = AvroEncoder::create(
        "avro_test_endpoint",
        &Some(key_sql_schema.clone()),
        val_sql_schema,
        Box::new(consumer),
        config.clone(),
        None,
        true,
    )
    .unwrap();

    let key_schema = encoder.key_avro_schema.clone();
    if config.key_mode != Some(AvroEncoderKeyMode::None) {
        assert!(key_schema.is_some());
    }

    let val_schema = encoder.value_avro_schema.clone();

    let zsets = batches
        .iter()
        .flat_map(|batch| {
            let inserts = batch
                .iter()
                .map(|(t1, _t2)| Tup2(Tup2(key_func(t1), t1.clone()), 1))
                .collect::<Vec<_>>();
            let upserts = batch
                .iter()
                .flat_map(|(t1, t2)| {
                    [
                        Tup2(Tup2(key_func(t1), t1.clone()), -1),
                        Tup2(Tup2(key_func(t1), t2.clone()), 1),
                    ]
                })
                .collect::<Vec<_>>();
            let deletes = batch
                .iter()
                .map(|(_t1, t2)| Tup2(Tup2(key_func(t2), t2.clone()), -1))
                .collect::<Vec<_>>();

            [inserts, upserts, deletes]
                .iter()
                .map(|batch| {
                    let zset = OrdIndexedZSet::from_tuples((), batch.clone());
                    Arc::new(<SerBatchImpl<_, K, T>>::new(zset)) as Arc<dyn SerBatch>
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    for (step, zset) in zsets.into_iter().enumerate() {
        encoder
            .consumer()
            .batch_start(step as u64, OutputBatchType::Delta);
        encoder.encode(zset.arc_as_batch_reader()).unwrap();
        encoder.consumer().batch_end();
    }

    let expected_output = batches
        .iter()
        .flat_map(|batch| {
            let inserts = batch
                .iter()
                .map(|(t1, _t2)| (t1.clone(), "insert"))
                .collect::<Vec<_>>();
            let upserts = batch
                .iter()
                .map(|(_t1, t2)| (t2.clone(), "update"))
                .collect::<Vec<_>>();
            let deletes = batch
                .iter()
                .map(|(_t1, t2)| (t2.clone(), "delete"))
                .collect::<Vec<_>>();

            [inserts, upserts, deletes].concat()
        })
        .collect::<Vec<_>>();

    // println!("expected: {:#?}", expected_output);

    let data = consumer_data.lock().unwrap();
    let actual_output = data
        .iter()
        .map(|(k, v, headers)| {
            let val = from_avro_datum(&val_schema, &mut &v.as_ref().unwrap()[5..], None).unwrap();
            let value =
                from_avro_value::<T, ()>(&val, &val_schema, &HashMap::new(), &None).unwrap();

            if let Some(key_schema) = &key_schema {
                let key =
                    from_avro_datum(key_schema, &mut &k.as_ref().unwrap()[5..], None).unwrap();
                let key =
                    from_avro_value::<K, ()>(&key, key_schema, &HashMap::new(), &None).unwrap();
                assert_eq!(key, key_func(&value));
            }

            (
                value,
                std::str::from_utf8(headers[0].1.as_ref().unwrap().as_slice()).unwrap(),
            )
        })
        .collect::<Vec<_>>();

    assert_eq!(
        actual_output.iter().sorted().collect::<Vec<_>>(),
        expected_output.iter().sorted().collect::<Vec<_>>()
    );
}

fn test_confluent_avro_output<K, V, KF>(
    config: AvroEncoderConfig,
    batches: Vec<Vec<Tup2<V, i64>>>,
    key_func: KF,
    key_schema: &str,
) where
    K: DBData
        + for<'de> DeserializeWithContext<'de, SqlSerdeConfig>
        + SerializeWithContext<SqlSerdeConfig>,
    V: DBData
        + for<'de> DeserializeWithContext<'de, SqlSerdeConfig>
        + SerializeWithContext<SqlSerdeConfig>,
    KF: Fn(&V) -> K,
{
    let schema = AvroSchema::parse_str(config.schema.as_ref().unwrap()).unwrap();
    let key_schema = AvroSchema::parse_str(key_schema).unwrap();

    let consumer = MockOutputConsumer::new();
    let consumer_data = consumer.data.clone();
    let mut encoder = AvroEncoder::create(
        "avro_test_endpoint",
        &None,
        &Relation::empty(),
        Box::new(consumer),
        config,
        None,
        false,
    )
    .unwrap();
    let zsets = batches
        .iter()
        .map(|batch| {
            let zset = OrdZSet::from_keys((), batch.clone());
            Arc::new(<SerBatchImpl<_, V, ()>>::new(zset)) as Arc<dyn SerBatch>
        })
        .collect::<Vec<_>>();
    for (step, zset) in zsets.into_iter().enumerate() {
        encoder
            .consumer()
            .batch_start(step as u64, OutputBatchType::Delta);
        encoder.encode(zset.arc_as_batch_reader()).unwrap();
        encoder.consumer().batch_end();
    }

    let (expected_inserts, expected_deletes): (Vec<_>, Vec<_>) = batches
        .concat()
        .into_iter()
        .flat_map(|Tup2(v, w)| {
            if w > 0 {
                repeat(Tup2(v.clone(), 1)).take(w as usize)
            } else {
                repeat(Tup2(v.clone(), -1)).take(-w as usize)
            }
        })
        .partition(|Tup2(_, w)| *w > 0);
    let expected_deletes = expected_deletes
        .into_iter()
        .map(|Tup2(v, w)| Tup2(key_func(&v), w))
        .collect::<Vec<_>>();

    let (inserts, deletes): (Vec<_>, Vec<_>) = consumer_data
        .lock()
        .unwrap()
        .iter()
        .map(|(k, v, _headers)| {
            if let Some(v) = v {
                let val = from_avro_datum(&schema, &mut &v[5..], None).unwrap();
                let value =
                    from_avro_value::<V, ()>(&val, &schema, &HashMap::new(), &None).unwrap();
                (Some(Tup2(value, 1)), None)
            } else {
                let val =
                    from_avro_datum(&key_schema, &mut &k.as_ref().unwrap()[5..], None).unwrap();
                let value =
                    from_avro_value::<K, ()>(&val, &key_schema, &HashMap::new(), &None).unwrap();
                (None, Some(Tup2(value, -1)))
            }
        })
        .unzip();

    let inserts = inserts.into_iter().flatten().collect::<Vec<_>>();
    let deletes = deletes.into_iter().flatten().collect::<Vec<_>>();

    assert_eq!(inserts, expected_inserts);
    assert_eq!(deletes, expected_deletes);
}

fn test_confluent_avro_output_indexed<K, V>(
    config: AvroEncoderConfig,
    key_sql_schema: &Relation,
    val_sql_schema: &Relation,
    key_func: impl Fn(&V) -> K,
    batches: Vec<Vec<(V, V)>>,
) where
    K: DBData
        + for<'de> DeserializeWithContext<'de, SqlSerdeConfig>
        + SerializeWithContext<SqlSerdeConfig>,
    V: DBData
        + for<'de> DeserializeWithContext<'de, SqlSerdeConfig>
        + SerializeWithContext<SqlSerdeConfig>,
{
    let consumer = MockOutputConsumer::new();
    let consumer_data = consumer.data.clone();
    let mut encoder = AvroEncoder::create(
        "avro_test_endpoint",
        &Some(key_sql_schema.clone()),
        val_sql_schema,
        Box::new(consumer),
        config,
        None,
        true,
    )
    .unwrap();

    let key_schema = encoder.key_avro_schema.clone().unwrap();
    let value_schema = encoder.value_avro_schema.clone();

    let zsets = batches
        .iter()
        .flat_map(|batch| {
            let inserts = batch
                .iter()
                .map(|(t1, _t2)| Tup2(Tup2(key_func(t1), t1.clone()), 1))
                .collect::<Vec<_>>();
            let upserts = batch
                .iter()
                .flat_map(|(t1, t2)| {
                    [
                        Tup2(Tup2(key_func(t1), t1.clone()), -1),
                        Tup2(Tup2(key_func(t1), t2.clone()), 1),
                    ]
                })
                .collect::<Vec<_>>();
            let deletes = batch
                .iter()
                .map(|(_t1, t2)| Tup2(Tup2(key_func(t2), t2.clone()), -1))
                .collect::<Vec<_>>();

            [inserts, upserts, deletes]
                .iter()
                .map(|batch| {
                    let zset = OrdIndexedZSet::from_tuples((), batch.clone());
                    Arc::new(<SerBatchImpl<_, K, V>>::new(zset)) as Arc<dyn SerBatch>
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    for (step, zset) in zsets.into_iter().enumerate() {
        encoder
            .consumer()
            .batch_start(step as u64, OutputBatchType::Delta);
        encoder.encode(zset.arc_as_batch_reader()).unwrap();
        encoder.consumer().batch_end();
    }

    let expected_output = batches
        .iter()
        .flat_map(|batch| {
            let inserts = batch
                .iter()
                .map(|(t1, _t2)| (key_func(t1), Some(t1.clone())))
                .collect::<Vec<_>>();
            let upserts = batch
                .iter()
                .map(|(_t1, t2)| (key_func(t2), Some(t2.clone())))
                .collect::<Vec<_>>();
            let deletes = batch
                .iter()
                .map(|(_t1, t2)| (key_func(t2), None))
                .collect::<Vec<_>>();

            [inserts, upserts, deletes].concat()
        })
        .collect::<Vec<_>>();

    // println!("expected: {:#?}", expected_output);

    let actual_outputs = consumer_data
        .lock()
        .unwrap()
        .iter()
        .map(|(k, v, _headers)| {
            let key = from_avro_datum(&key_schema, &mut &k.as_ref().unwrap()[5..], None).unwrap();
            let key = from_avro_value::<K, ()>(&key, &key_schema, &HashMap::new(), &None).unwrap();

            let val = v.as_ref().map(|v| {
                let val = from_avro_datum(&value_schema, &mut &v[5..], None).unwrap();
                from_avro_value::<V, ()>(&val, &value_schema, &HashMap::new(), &None).unwrap()
            });

            (key, val)
        })
        .collect::<Vec<_>>();

    assert_eq!(
        actual_outputs.iter().sorted().collect::<Vec<_>>(),
        expected_output.iter().sorted().collect::<Vec<_>>()
    );
}

#[test]
fn test_non_unique_keys() {
    let schema_str = TestStruct::avro_schema().to_string();
    let config: AvroEncoderConfig = AvroEncoderConfig {
        schema: Some(schema_str.clone()),
        key_mode: Some(AvroEncoderKeyMode::None),
        ..Default::default()
    };
    let consumer = MockOutputConsumer::new();

    let k1 = KeyStruct { id: 1 };
    let v1 = TestStruct {
        id: 1,
        b: true,
        i: None,
        s: "foo".to_string(),
    };
    let v2 = TestStruct {
        id: 1,
        b: false,
        i: None,
        s: "bar".to_string(),
    };

    let mut encoder = AvroEncoder::create(
        "avro_test_endpoint",
        &Some(KeyStruct::relation_schema()),
        &TestStruct::relation_schema(),
        Box::new(consumer),
        config,
        None,
        true,
    )
    .unwrap();

    let zset = OrdIndexedZSet::from_tuples((), vec![Tup2(Tup2(k1.clone(), v1.clone()), 2)]);
    let zset = Arc::new(<SerBatchImpl<_, KeyStruct, TestStruct>>::new(zset)) as Arc<dyn SerBatch>;

    encoder.consumer().batch_start(0, OutputBatchType::Delta);
    let err = encoder.encode(zset.arc_as_batch_reader()).unwrap_err();
    assert!(err.to_string().contains(r#"is inserted 2 times"#));
    encoder.consumer().batch_end();

    let zset = OrdIndexedZSet::from_tuples((), vec![Tup2(Tup2(k1.clone(), v1.clone()), -2)]);
    let zset = Arc::new(<SerBatchImpl<_, KeyStruct, TestStruct>>::new(zset)) as Arc<dyn SerBatch>;

    encoder.consumer().batch_start(0, OutputBatchType::Delta);
    let err = encoder.encode(zset.arc_as_batch_reader()).unwrap_err();
    assert!(err.to_string().contains(r#"is deleted 2 times"#));
    encoder.consumer().batch_end();

    let zset = OrdIndexedZSet::from_tuples(
        (),
        vec![
            Tup2(Tup2(k1.clone(), v1.clone()), 1),
            Tup2(Tup2(k1.clone(), v2.clone()), 1),
        ],
    );
    let zset = Arc::new(<SerBatchImpl<_, KeyStruct, TestStruct>>::new(zset)) as Arc<dyn SerBatch>;

    encoder.consumer().batch_start(0, OutputBatchType::Delta);
    let err = encoder.encode(zset.arc_as_batch_reader()).unwrap_err();
    println!("{err}");
    assert!(
        err.to_string()
            .contains(r#"Error description: Multiple new values for the same key."#)
    );
    encoder.consumer().batch_end();

    let zset = OrdIndexedZSet::from_tuples(
        (),
        vec![
            Tup2(Tup2(k1.clone(), v1.clone()), -1),
            Tup2(Tup2(k1.clone(), v2.clone()), -1),
        ],
    );
    let zset = Arc::new(<SerBatchImpl<_, KeyStruct, TestStruct>>::new(zset)) as Arc<dyn SerBatch>;

    encoder.consumer().batch_start(0, OutputBatchType::Delta);
    let err = encoder.encode(zset.arc_as_batch_reader()).unwrap_err();
    println!("{err}");
    assert!(
        err.to_string()
            .contains(r#"Error description: Multiple deleted values for the same key."#)
    );
    encoder.consumer().batch_end();
}

fn datagen_indexed_spine_snapshot_test_struct(
    seed: u64,
    batches: usize,
    max_batch_size: usize,
) -> (Arc<dyn SerBatchReader>, Vec<(TestStruct, String)>) {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut all_tuples = Vec::new();
    let mut dyn_batches = Vec::with_capacity(batches);
    for batch_idx in 0..batches {
        let batch_size = rng.gen_range(0..=max_batch_size);
        let mut tuples = Vec::with_capacity(batch_size);
        let mut next_id = batch_idx as u32;

        for _ in 0..batch_size {
            let id = next_id;
            next_id += batches as u32;

            let key = KeyStruct { id };
            let value = TestStruct {
                id,
                b: rng.r#gen(),
                i: rng.gen_bool(0.5).then(|| rng.r#gen()),
                s: rng.r#gen::<u32>().to_string(),
            };
            let weight = if rng.gen_bool(0.5) { 1 } else { -1 };

            tuples.push(Tup2(Tup2(key, value.clone()), weight));
            all_tuples.push(Tup2(Tup2(KeyStruct { id }, value), weight));
        }

        let batch = OrdIndexedZSet::<KeyStruct, TestStruct>::from_tuples((), tuples);

        dyn_batches.push(Arc::new(batch.into_inner()));
    }

    let expected_output = OrdIndexedZSet::<KeyStruct, TestStruct>::from_tuples((), all_tuples)
        .iter()
        .map(|(_k, v, w)| {
            (
                v,
                if w > 0 {
                    "insert".to_string()
                } else {
                    "delete".to_string()
                },
            )
        })
        .collect::<Vec<_>>();

    let factories = BatchReaderFactories::new::<KeyStruct, TestStruct, ZWeight>();
    let snapshot: TypedSpineSnapshot<OrdIndexedZSet<KeyStruct, TestStruct>> =
        TypedBatch::new(DynSpineSnapshot::with_batches(&factories, dyn_batches));
    let snapshot = Arc::new(SerBatchImpl::<
        TypedSpineSnapshot<OrdIndexedZSet<KeyStruct, TestStruct>>,
        KeyStruct,
        TestStruct,
    >::new(snapshot)) as Arc<dyn SerBatchReader>;

    (snapshot, expected_output)
}

fn run_avro_encoder_spine_snapshot_indexed<V>(
    avro_schema: &str,
    key_schema: &Relation,
    value_schema: &Relation,
    snapshot: Arc<dyn SerBatchReader>,
    expected_output: Vec<(V, String)>,
) where
    V: DBData
        + for<'de> DeserializeWithContext<'de, SqlSerdeConfig>
        + SerializeWithContext<SqlSerdeConfig>,
{
    let config: AvroEncoderConfig = AvroEncoderConfig {
        schema: Some(avro_schema.to_string()),
        key_mode: Some(AvroEncoderKeyMode::None),
        threads: 10,
        ..Default::default()
    };

    let consumer = MockOutputConsumer::new();
    let consumer_data = consumer.data.clone();
    let mut encoder = AvroEncoder::create(
        "avro_test_endpoint",
        &Some(key_schema.clone()),
        value_schema,
        Box::new(consumer),
        config,
        None,
        true,
    )
    .unwrap();

    encoder.consumer().batch_start(0, OutputBatchType::Delta);
    encoder.encode(snapshot).unwrap();
    encoder.consumer().batch_end();

    let val_schema = encoder.value_avro_schema.clone();
    let actual_output = consumer_data
        .lock()
        .unwrap()
        .iter()
        .map(|(_k, v, headers)| {
            let val = from_avro_datum(&val_schema, &mut &v.as_ref().unwrap()[5..], None).unwrap();
            let value =
                from_avro_value::<V, ()>(&val, &val_schema, &HashMap::new(), &None).unwrap();
            let op = std::str::from_utf8(headers[0].1.as_ref().unwrap().as_slice()).unwrap();
            (value, op.to_string())
        })
        .collect::<Vec<_>>();

    assert_eq!(
        actual_output.iter().sorted().collect::<Vec<_>>(),
        expected_output.iter().sorted().collect::<Vec<_>>()
    );
}

/// Test Avro encoder over a SpineSnapshot batch.
///
/// This exercises split cursors over more complex batches consisting of multiple.
/// This test forces all batches to be FileIndexedWSet batches by setting min storage bytes to 0.
#[test]
fn proptest_avro_encoder_spine_snapshot_indexed_posix() {
    run_in_posix_runtime(Some(0usize), Some(0usize), move || {
        let (snapshot, expected_output) =
            datagen_indexed_spine_snapshot_test_struct(0xD00D_F00D, 20, 100);

        run_avro_encoder_spine_snapshot_indexed::<TestStruct>(
            TestStruct::avro_schema(),
            &KeyStruct::relation_schema(),
            &TestStruct::relation_schema(),
            snapshot,
            expected_output,
        );
    });
}

/// This test forces all batches to be in-memory batches by setting min storage bytes to None.
#[test]
fn proptest_avro_encoder_spine_snapshot_indexed_mem() {
    run_in_posix_runtime(None, None, move || {
        let (snapshot, expected_output) =
            datagen_indexed_spine_snapshot_test_struct(0xD00D_F00D, 20, 100);

        run_avro_encoder_spine_snapshot_indexed::<TestStruct>(
            TestStruct::avro_schema(),
            &KeyStruct::relation_schema(),
            &TestStruct::relation_schema(),
            snapshot,
            expected_output,
        );
    });
}

proptest! {
    #[test]
    fn proptest_raw_avro_output(data in generate_test_batches_with_weights(10, 20))
    {
        let schema_str = TestStruct::avro_schema().to_string();
        let config: AvroEncoderConfig = AvroEncoderConfig {
            schema: Some(schema_str.clone()),
            ..Default::default()
        };

        test_raw_avro_output::<TestStruct>(config, data)
    }

    #[test]
    fn proptest_raw_avro_output_indexed(data in generate_test_batches(10, 10, 20))
    {
        let schema_str = TestStruct::avro_schema().to_string();
        let config: AvroEncoderConfig = AvroEncoderConfig {
            schema: Some(schema_str.clone()),
            key_mode: Some(AvroEncoderKeyMode::None),
            ..Default::default()
        };

        let data = data.into_iter().map(|batch| {
            batch.into_iter().map(|v| {
                let v1 = v.clone();
                let mut v2 = v.clone();
                v2.b = !v2.b;
                (v1, v2)
            }).collect::<Vec<_>>()
        }).collect::<Vec<_>>();

        test_raw_avro_output_indexed::<KeyStruct, TestStruct>(config, &KeyStruct::relation_schema(), &TestStruct::relation_schema(), |test_struct| KeyStruct{id: test_struct.id}, data)
    }

    #[test]
    fn proptest_raw_avro_output_indexed_with_key(data in generate_test_batches(10, 10, 20))
    {
        let config: AvroEncoderConfig = AvroEncoderConfig {
            ..Default::default()
        };

        let data = data.into_iter().map(|batch| {
            batch.into_iter().map(|v| {
                let v1 = v.clone();
                let mut v2 = v.clone();
                v2.b = !v2.b;
                (v1, v2)
            }).collect::<Vec<_>>()
        }).collect::<Vec<_>>();

        test_raw_avro_output_indexed::<KeyStruct, TestStruct>(config, &KeyStruct::relation_schema(), &TestStruct::relation_schema(), |test_struct| KeyStruct{id: test_struct.id}, data)
    }

    #[test]
    fn proptest_confluent_avro_output(data in generate_test_batches_with_weights(10, 20))
    {
        let schema_str = TestStruct::avro_schema();

        let config: AvroEncoderConfig = AvroEncoderConfig {
            schema: Some(schema_str.to_string()),
            namespace: Some("foo.bar".to_string()),
            update_format: AvroUpdateFormat::ConfluentJdbc,
            //key_fields: Some(vec!["b".to_string(), "id".to_string(), "i".to_string(), "s".to_string()]),
            ..Default::default()
        };

        test_confluent_avro_output::<TestStruct, TestStruct, _>(config, data, |v| v.clone(), schema_str);
    }

    #[test]
    fn proptest_confluent_avro_output_indexed(data in generate_test_batches(10, 10, 20))
    {
        let config: AvroEncoderConfig = AvroEncoderConfig {
            namespace: Some("foo.bar".to_string()),
            update_format: AvroUpdateFormat::ConfluentJdbc,
            ..Default::default()
        };

        let data = data.into_iter().map(|batch| {
            batch.into_iter().map(|v| {
                let v1 = v.clone();
                let mut v2 = v.clone();
                v2.b = !v2.b;
                (v1, v2)
            }).collect::<Vec<_>>()
        }).collect::<Vec<_>>();

        test_confluent_avro_output_indexed::<KeyStruct, TestStruct>(config,  &KeyStruct::relation_schema(), &TestStruct::relation_schema(), |test_struct| KeyStruct{id: test_struct.id}, data);
    }
}
