use super::{
    output::AvroEncoder,
    schema::schema_json,
    serializer::{avro_ser_config, AvroSchemaSerializer},
};
use crate::{
    static_compile::seroutput::SerBatchImpl,
    test::{
        generate_test_batches_with_weights, mock_parser_pipeline, MockOutputConsumer, MockUpdate,
        TestStruct, TestStruct2,
    },
    transport::InputConsumer,
    Encoder, FormatConfig, ParseError, SerBatch,
};
use apache_avro::{schema::ResolvedSchema, to_avro_datum, Schema as AvroSchema};
use dbsp::utils::Tup2;
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
use log::trace;
use proptest::prelude::*;
use proptest::proptest;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;
use std::{borrow::Cow, collections::HashMap, fmt::Debug};

#[derive(Debug)]
struct TestCase<T> {
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
}"#.replace("VALUE_SCHEMA", &value_schema).replace("VALUE_TYPE", value_type_name);

    println!("Debezium Avro schema: {schema_str}");

    AvroSchema::parse_str(&schema_str).unwrap()
}

/// Generate a test case using raw Avro update format.
fn gen_raw_parser_test<T>(data: &[T], avro_schema_str: &str) -> TestCase<T>
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
        no_schema_id: false,
        registry_config: Default::default(),
    };

    let avro_schema = AvroSchema::parse_str(avro_schema_str).unwrap();

    let input_batches = data
        .iter()
        .map(|x| {
            // 5-byte header
            let mut buffer = vec![0; 5];
            let refs = HashMap::new();
            let serializer = AvroSchemaSerializer::new(&avro_schema, &refs, true);
            let val = x
                .serialize_with_context(serializer, &avro_ser_config())
                .unwrap();
            let mut avro_record = to_avro_datum(&avro_schema, val).unwrap();
            buffer.append(&mut avro_record);
            (buffer, vec![])
        })
        .collect::<Vec<_>>();

    let expected_output = data
        .iter()
        .map(|x| MockUpdate::Insert(x.clone()))
        .collect::<Vec<_>>();

    TestCase {
        config,
        input_batches,
        expected_output,
    }
}

/// Generate a test case using Debezium Avro update format.
fn gen_debezium_parser_test<T>(data: &[T], avro_schema_str: &str, type_name: &str) -> TestCase<T>
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
        no_schema_id: false,
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
        .map(|x| vec![MockUpdate::Delete(x.clone()), MockUpdate::Insert(x.clone())])
        .flatten()
        .collect::<Vec<_>>();

    TestCase {
        config,
        input_batches,
        expected_output,
    }
}

fn run_parser_test<T>(test_cases: Vec<TestCase<T>>)
where
    T: Debug + Eq + for<'de> DeserializeWithContext<'de, SqlSerdeConfig> + Send + 'static,
{
    for test in test_cases {
        let format_config = FormatConfig {
            name: Cow::from("avro"),
            config: serde_yaml::to_value(test.config).unwrap(),
        };

        let (mut consumer, outputs) = mock_parser_pipeline(&format_config).unwrap();
        consumer.on_error(Some(Box::new(|_, _| {})));
        for (avro, expected_result) in test.input_batches {
            let res = consumer.input_chunk(&avro);
            assert_eq!(&res, &expected_result);
        }
        consumer.eoi();
        assert_eq!(&test.expected_output, &outputs.state().flushed);
    }
}

#[test]
fn test_raw_avro_parser() {
    let test_case = gen_raw_parser_test(&TestStruct2::data(), &TestStruct2::avro_schema());

    run_parser_test(vec![test_case])
}

#[test]
fn test_debezium_avro_parser() {
    let test_case = gen_debezium_parser_test(
        &TestStruct2::data(),
        &TestStruct2::avro_schema(),
        "TestStruct2",
    );

    run_parser_test(vec![test_case])
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2))]

    #[test]
    fn proptest_raw_avro_parser(data in proptest::collection::vec(any::<TestStruct2>(), 0..=10000))
    {
        let test_case = gen_raw_parser_test(&data, &TestStruct2::avro_schema());

        run_parser_test(vec![test_case])
    }

    #[test]
    fn proptest_debezium_avro_parser(data in proptest::collection::vec(any::<TestStruct2>(), 0..=10000))
    {
        let test_case = gen_debezium_parser_test(&data, &TestStruct2::avro_schema(), "TestStruct2");

        run_parser_test(vec![test_case])
    }

}

fn test_avro_output<T>(avro_schema: &str, batches: Vec<Vec<Tup2<T, i64>>>)
where
    T: DBData + for<'de> Deserialize<'de> + SerializeWithContext<SqlSerdeConfig>,
{
    let config = AvroEncoderConfig {
        schema: Some(avro_schema.to_string()),
        ..Default::default()
    };

    let consumer = MockOutputConsumer::new();
    let consumer_data = consumer.data.clone();
    let mut encoder = AvroEncoder::create(
        "avro_test_endpoint",
        &Relation::empty(),
        Box::new(consumer),
        config,
        None,
    )
    .unwrap();
    let zsets = batches
        .iter()
        .map(|batch| {
            let zset = OrdZSet::from_keys(
                (),
                batch
                    .iter()
                    .map(|Tup2(x, w)| Tup2(x.clone(), *w))
                    .collect::<Vec<_>>(),
            );
            Arc::new(<SerBatchImpl<_, T, ()>>::new(zset)) as Arc<dyn SerBatch>
        })
        .collect::<Vec<_>>();
    for (step, zset) in zsets.iter().enumerate() {
        encoder.consumer().batch_start(step as u64);
        encoder.encode(zset.as_batch_reader()).unwrap();
        encoder.consumer().batch_end();
    }

    let expected_output = batches
        .into_iter()
        .flat_map(|batch| {
            let zset = OrdZSet::from_keys(
                (),
                batch
                    .iter()
                    .map(|Tup2(x, w)| Tup2(x.clone(), *w))
                    .collect::<Vec<_>>(),
            );
            /*let mut deletes = zset
            .iter()
            .flat_map(|(data, (), weight)| {
                trace!("data: {data:?}, weight: {weight}");
                let range = if weight < 0 { weight..0 } else { 0..0 };

                range.map(move |_| {
                    let upd = U::update(
                        false,
                        data.clone(),
                        encoder.stream_id,
                        *seq_number.borrow(),
                    );
                    *seq_number.borrow_mut() += 1;
                    upd
                })
            })
            .collect::<Vec<_>>();*/
            let inserts = zset
                .iter()
                .flat_map(|(data, (), weight)| {
                    trace!("data: {data:?}, weight: {weight}");
                    let range = if weight > 0 { 0..weight } else { 0..0 };

                    range.map(move |_| data.clone())
                })
                .collect::<Vec<_>>();

            inserts
        })
        .collect::<Vec<_>>();

    let mut actual_output = Vec::new();
    for (_, avro_datum) in consumer_data.lock().unwrap().iter() {
        let avro_value =
            apache_avro::from_avro_datum(&encoder.value_schema, &mut &avro_datum[5..], None)
                .unwrap();
        // FIXME: this will use the default `serde::Deserialize` implementation and will only work
        // for types where it happens to do the right thing. We should use Avro-compatible
        // deserialization logic instead, when it exists.
        let val = apache_avro::from_value::<T>(&avro_value).unwrap();
        actual_output.push(val);
    }

    assert_eq!(actual_output, expected_output);
}

proptest! {
    #[test]
    fn proptest_avro_output(data in generate_test_batches_with_weights(10, 20))
    {
        test_avro_output::<TestStruct>(TestStruct::avro_schema(), data)
    }
}
