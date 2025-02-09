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
use apache_avro::{from_avro_datum, schema::ResolvedSchema, to_avro_datum, Schema as AvroSchema};
use dbsp::{utils::Tup2, OrdIndexedZSet};
use dbsp::{DBData, OrdZSet};
use feldera_types::{
    format::avro::AvroEncoderConfig,
    program_schema::Relation,
    serde_with_context::{DeserializeWithContext, SerializeWithContext, SqlSerdeConfig},
};
use feldera_types::{
    format::avro::{AvroParserConfig, AvroUpdateFormat},
    serialize_struct,
};
use proptest::prelude::*;
use proptest::proptest;
use serde::Serialize;
use std::{borrow::Cow, collections::HashMap, fmt::Debug, hash::Hash};
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
    let schema_str = r#"{
    "type": "record",
    "name": "Envelope",
    "namespace": "test_namespace",
    "fields": [
        {
            "name": "before",
            "type": [
                "null",
                VALUE_SCHEMA
            ],
            "default": null
        },
        {
            "name": "after",
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
    // 5-byte header
    let mut buffer = vec![0; 5];
    let refs = HashMap::new();
    let serializer = AvroSchemaSerializer::new(schema, &refs, false);
    let val = x
        .serialize_with_context(serializer, &avro_ser_config())
        .unwrap();
    let mut avro_record = to_avro_datum(schema, val).unwrap();
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
            config: serde_yaml::to_value(test.config).unwrap(),
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
            { "name": "name", "type": "string" },
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
            { "name": "name", "type": ["string", "null"] },
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
                let value = from_avro_value::<T>(&val, &schema).unwrap();
                let w = if headers[0] == ("op".to_string(), Some(b"insert".to_vec())) {
                    1
                } else {
                    -1
                };
                Tup2::new(value, w)
            })
            .collect(),
    );

    assert_eq!(actual_output, expected_output);
}

fn test_raw_avro_output_indexed<K, T>(
    config: AvroEncoderConfig,
    key_sql_schema: &Relation,
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
    let schema = AvroSchema::parse_str(config.schema.as_ref().unwrap()).unwrap();

    let consumer = MockOutputConsumer::new();
    let consumer_data = consumer.data.clone();
    let mut encoder = AvroEncoder::create(
        "avro_test_endpoint",
        &Some(key_sql_schema.clone()),
        &Relation::empty(),
        Box::new(consumer),
        config,
        None,
    )
        .unwrap();

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
        .map(|(_k, v, headers)| {
            let val = from_avro_datum(&schema, &mut &v.as_ref().unwrap()[5..], None).unwrap();
            let value = from_avro_value::<T>(&val, &schema).unwrap();
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
        .flat_map(|t| {
            let (v, w) = t.into();
            if w > 0 {
                repeat(Tup2::new(v.clone(), 1)).take(w as usize)
            } else {
                repeat(Tup2::new(v.clone(), -1)).take(-w as usize)
            }
        })
        .partition(|t| *t.snd() > 0);
    let expected_deletes = expected_deletes
        .into_iter()
        .map(|t| Tup2::new(key_func(t.fst()), *t.snd()))
        .collect::<Vec<_>>();

    let (inserts, deletes): (Vec<_>, Vec<_>) = consumer_data
        .lock()
        .unwrap()
        .iter()
        .map(|(k, v, _headers)| {
            if let Some(v) = v {
                let val = from_avro_datum(&schema, &mut &v[5..], None).unwrap();
                let value = from_avro_value::<V>(&val, &schema).unwrap();
                (Some(Tup2::new(value, 1)), None)
            } else {
                let val =
                    from_avro_datum(&key_schema, &mut &k.as_ref().unwrap()[5..], None).unwrap();
                let value = from_avro_value::<K>(&val, &key_schema).unwrap();
                (None, Some(Tup2::new(value, -1)))
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
            let key = from_avro_value::<K>(&key, &key_schema).unwrap();

            let val = v.as_ref().map(|v| {
                let val = from_avro_datum(&value_schema, &mut &v[5..], None).unwrap();
                from_avro_value::<V>(&val, &value_schema).unwrap()
            });

            (key, val)
        })
        .collect::<Vec<_>>();

    assert_eq!(expected_output, actual_outputs);
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

        test_raw_avro_output_indexed::<KeyStruct, TestStruct>(config, &KeyStruct::relation_schema(), |test_struct| KeyStruct{id: test_struct.id}, data)
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