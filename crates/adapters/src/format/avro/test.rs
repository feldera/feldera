use super::{
    output::AvroEncoder,
    schema::schema_json,
    serializer::{avro_ser_config, AvroSchemaSerializer},
};
use crate::{
    format::{avro::from_avro_value, InputBuffer, Parser},
    static_compile::seroutput::SerBatchImpl,
    test::{
        generate_test_batches, generate_test_batches_with_weights, mock_parser_pipeline, KeyStruct,
        MockOutputConsumer, MockUpdate, TestStruct, TestStruct2,
    },
    Encoder, FormatConfig, ParseError, SerBatch,
};
use apache_avro::{
    from_avro_datum, schema::ResolvedSchema, to_avro_datum, types::Value, Schema as AvroSchema,
};
use dbsp::{utils::Tup2, OrdIndexedZSet};
use dbsp::{DBData, OrdZSet};
use feldera_sqllib::{ByteArray, Uuid};
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
use proptest::prelude::*;
use proptest::proptest;
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

    println!("Debezium Avro schema: {schema_str}");

    AvroSchema::parse_str(&schema_str).unwrap()
}

fn serialize_record<T>(x: &T, schema: &AvroSchema) -> Vec<u8>
where
    T: Clone
        + Debug
        + Eq
        + SerializeWithContext<SqlSerdeConfig>
        + for<'de> DeserializeWithContext<'de, SqlSerdeConfig>
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
        + for<'de> DeserializeWithContext<'de, SqlSerdeConfig>
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
        + for<'de> DeserializeWithContext<'de, SqlSerdeConfig>
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
        + for<'de> DeserializeWithContext<'de, SqlSerdeConfig>
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
            let (mut buffer, errors) = parser.parse(&avro);
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
        }
    }
}

serialize_table_record!(TestBinary[2]{
    r#binary32["binary32"]: ByteArray,
    r#varbinary["varbinary"]: ByteArray
});

deserialize_table_record!(TestBinary["TestBinary", 2] {
    (r#binary32, "binary32", false, ByteArray, None),
    (r#varbinary, "varbinary", false, ByteArray, None)
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
        }
    }
}

serialize_table_record!(TestUuid[3]{
    r#uuid1["uuid1"]: Uuid,
    r#uuid2["uuid2"]: Uuid,
    r#varchar["varchar"]: String
});

deserialize_table_record!(TestUuid["TestUuid", 3] {
    (r#uuid1, "uuid1", false, Uuid, None),
    (r#uuid2, "uuid2", false, Uuid, None),
    (r#varchar, "varchar", false, String, None)
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
        }
    }
}

serialize_table_record!(TestEnum[1]{
    r#enum_val["enum_val"]: String
});

deserialize_table_record!(TestEnum["TestEnum", 1] {
    (r#enum_val, "enum_val", false, String, None)
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
        }
    }
}

serialize_table_record!(TestIntConversionsSrc[4]{
    r#uint["uint"]: i32,
    r#ulong["ulong"]: i32,
    r#int["int"]: i32,
    r#long["long"]: i32
});

deserialize_table_record!(TestIntConversionsSrc["TestIntConversions", 4] {
    (r#uint, "uint", false, i32, None),
    (r#ulong, "ulong", false, i32, None),
    (r#int, "int", false, i32, None),
    (r#long, "long", false, i32, None)
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

deserialize_table_record!(TestIntConversionsDst["TestIntConversions", 4] {
    (r#uint, "uint", false, u32, None),
    (r#ulong, "ulong", false, u64, None),
    (r#int, "int", false, i32, None),
    (r#long, "long", false, i64, None)
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
    )
    .unwrap();
    let zsets = batches
        .iter()
        .map(|batch| {
            let zset = OrdZSet::from_keys((), batch.clone());
            Arc::new(<SerBatchImpl<_, T, ()>>::new(zset)) as Arc<dyn SerBatch>
        })
        .collect::<Vec<_>>();
    for (step, zset) in zsets.iter().enumerate() {
        encoder.consumer().batch_start(step as u64);
        encoder.encode(zset.as_batch_reader()).unwrap();
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
                let value = from_avro_value::<T>(&val, &schema, &HashMap::new()).unwrap();
                let w = if headers[0] == ("op".to_string(), Some(b"insert".to_vec())) {
                    1
                } else {
                    -1
                };
                Tup2(value, w)
            })
            .collect(),
    );

    assert_eq!(actual_output, expected_output);
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
    for (step, zset) in zsets.iter().enumerate() {
        encoder.consumer().batch_start(step as u64);
        encoder.encode(zset.as_batch_reader()).unwrap();
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
            let value = from_avro_value::<T>(&val, &val_schema, &HashMap::new()).unwrap();

            if let Some(key_schema) = &key_schema {
                let key =
                    from_avro_datum(key_schema, &mut &k.as_ref().unwrap()[5..], None).unwrap();
                let key = from_avro_value::<K>(&key, key_schema, &HashMap::new()).unwrap();
                assert_eq!(key, key_func(&value));
            }

            (
                value,
                std::str::from_utf8(headers[0].1.as_ref().unwrap().as_slice()).unwrap(),
            )
        })
        .collect::<Vec<_>>();

    assert_eq!(actual_output, expected_output);
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
    )
    .unwrap();
    let zsets = batches
        .iter()
        .map(|batch| {
            let zset = OrdZSet::from_keys((), batch.clone());
            Arc::new(<SerBatchImpl<_, V, ()>>::new(zset)) as Arc<dyn SerBatch>
        })
        .collect::<Vec<_>>();
    for (step, zset) in zsets.iter().enumerate() {
        encoder.consumer().batch_start(step as u64);
        encoder.encode(zset.as_batch_reader()).unwrap();
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
                let value = from_avro_value::<V>(&val, &schema, &HashMap::new()).unwrap();
                (Some(Tup2(value, 1)), None)
            } else {
                let val =
                    from_avro_datum(&key_schema, &mut &k.as_ref().unwrap()[5..], None).unwrap();
                let value = from_avro_value::<K>(&val, &key_schema, &HashMap::new()).unwrap();
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

    for (step, zset) in zsets.iter().enumerate() {
        encoder.consumer().batch_start(step as u64);
        encoder.encode(zset.as_batch_reader()).unwrap();
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
            let key = from_avro_value::<K>(&key, &key_schema, &HashMap::new()).unwrap();

            let val = v.as_ref().map(|v| {
                let val = from_avro_datum(&value_schema, &mut &v[5..], None).unwrap();
                from_avro_value::<V>(&val, &value_schema, &HashMap::new()).unwrap()
            });

            (key, val)
        })
        .collect::<Vec<_>>();

    assert_eq!(expected_output, actual_outputs);
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
    )
    .unwrap();

    let zset = OrdIndexedZSet::from_tuples((), vec![Tup2(Tup2(k1.clone(), v1.clone()), 2)]);
    let zset = Arc::new(<SerBatchImpl<_, KeyStruct, TestStruct>>::new(zset)) as Arc<dyn SerBatch>;

    encoder.consumer().batch_start(0);
    let err = encoder.encode(zset.as_batch_reader()).unwrap_err();
    assert!(err.to_string().contains(r#"is inserted 2 times"#));
    encoder.consumer().batch_end();

    let zset = OrdIndexedZSet::from_tuples((), vec![Tup2(Tup2(k1.clone(), v1.clone()), -2)]);
    let zset = Arc::new(<SerBatchImpl<_, KeyStruct, TestStruct>>::new(zset)) as Arc<dyn SerBatch>;

    encoder.consumer().batch_start(0);
    let err = encoder.encode(zset.as_batch_reader()).unwrap_err();
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

    encoder.consumer().batch_start(0);
    let err = encoder.encode(zset.as_batch_reader()).unwrap_err();
    println!("{err}");
    assert!(err
        .to_string()
        .contains(r#"Error description: Multiple new values for the same key."#));
    encoder.consumer().batch_end();

    let zset = OrdIndexedZSet::from_tuples(
        (),
        vec![
            Tup2(Tup2(k1.clone(), v1.clone()), -1),
            Tup2(Tup2(k1.clone(), v2.clone()), -1),
        ],
    );
    let zset = Arc::new(<SerBatchImpl<_, KeyStruct, TestStruct>>::new(zset)) as Arc<dyn SerBatch>;

    encoder.consumer().batch_start(0);
    let err = encoder.encode(zset.as_batch_reader()).unwrap_err();
    println!("{err}");
    assert!(err
        .to_string()
        .contains(r#"Error description: Multiple deleted values for the same key."#));
    encoder.consumer().batch_end();
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
