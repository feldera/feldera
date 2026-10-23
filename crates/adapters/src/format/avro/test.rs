//! Tests of the Avro format that stay with the static engine: its schema validation, its
//! spine snapshots under the encoder, its record layer's Avro streams, and its typed VARIANT
//! values. The connector tests in `parser_tests` and `encoder_tests` cover the rest.

use super::{
    output::AvroEncoder,
    serializer::{AvroSchemaSerializer, avro_ser_config},
};
use crate::{
    Encoder, InputBuffer, Parser,
    format::avro::from_avro_value,
    static_compile::seroutput::SerBatchImpl,
    test::{KeyStruct, MockOutputConsumer, MockUpdate, TestStruct, mock_parser_pipeline},
};
use crate::{catalog::SerBatchReader, util::run_in_posix_runtime};
use apache_avro::{Schema as AvroSchema, from_avro_datum, schema::ResolvedSchema, to_avro_datum};
use dbsp::trace::BatchReaderFactories;
use dbsp::typed_batch::{DynSpineSnapshot, SpineSnapshot as TypedSpineSnapshot, TypedBatch};
use dbsp::{DBData, IndexedZSetReader, OrdIndexedZSet, ZWeight, utils::Tup2};
use feldera_adapterlib::transport::OutputBatchType;
use feldera_sqllib::{FlatVariant, Variant};
use feldera_types::config::FormatConfig;
use feldera_types::format::avro::{AvroParserConfig, AvroUpdateFormat};
use feldera_types::size::ConfigSize;
use feldera_types::{deserialize_table_record, serialize_table_record};
use feldera_types::{
    format::avro::{AvroEncoderConfig, AvroEncoderKeyMode},
    program_schema::{ColumnType, Field, Relation, SqlIdentifier},
    serde_with_context::{DeserializeWithContext, SerializeWithContext, SqlSerdeConfig},
};
use itertools::Itertools;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use size_of::SizeOf;
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

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

// Typed VARIANT checks stay with the static engine: the shared suites see a VARIANT as JSON text.

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
    feldera_macros::IsNone,
    feldera_macros::OrdRepr,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
struct TestVariant {
    v1: Variant,
    v2: FlatVariant,
}

impl TestVariant {
    pub fn avro_schema() -> &'static str {
        r#"{
            "type": "record",
            "name": "TestVariant",
            "connect.name": "test_namespace.TestVariant",
            "fields": [
                { "name": "v1", "type": "string" },
                { "name": "v2", "type": "string" }
            ]
        }"#
    }

    pub fn relation_schema() -> Relation {
        Relation {
            name: SqlIdentifier::new("TestVariant", false),
            fields: vec![
                Field::new("v1".into(), ColumnType::variant(false)),
                Field::new("v2".into(), ColumnType::variant(false)),
            ],
            materialized: false,
            properties: BTreeMap::new(),
            primary_key: None,
        }
    }

    pub fn sample() -> Self {
        let json = r#"{"a": 1, "b": ["x", null], "c": 2.5, "ok": true}"#;
        TestVariant {
            v1: serde_json::from_str::<Variant>(json).unwrap(),
            v2: serde_json::from_str::<FlatVariant>(json).unwrap(),
        }
    }
}

serialize_table_record!(TestVariant[2]{
    r#v1["v1"]: Variant,
    r#v2["v2"]: FlatVariant
});

deserialize_table_record!(TestVariant["TestVariant", Variant, 2] {
    (r#v1, "v1", false, Variant, |_| None),
    (r#v2, "v2", false, FlatVariant, |_| None)
});

/// VARIANT columns (both runtime representations) parse from an Avro
/// string field carrying JSON.
#[test]
fn test_parse_variant() {
    let schema = AvroSchema::parse_str(TestVariant::avro_schema()).unwrap();
    let sample = TestVariant::sample();
    let record = sample
        .serialize_with_context(
            AvroSchemaSerializer::new(&schema, &HashMap::new(), false),
            &avro_ser_config(),
        )
        .unwrap();
    // A 5-byte header, then the datum.
    let mut message = vec![0; 5];
    message.append(&mut to_avro_datum(&schema, record).unwrap());

    let config = AvroParserConfig {
        update_format: AvroUpdateFormat::Raw,
        schema: Some(TestVariant::avro_schema().to_string()),
        skip_schema_id: false,
        cdc_field: None,
        registry_config: Default::default(),
    };
    let format_config = FormatConfig {
        name: Cow::from("avro"),
        config: serde_json::to_value(config).unwrap(),
    };
    let (consumer, mut parser, outputs) = mock_parser_pipeline::<TestVariant, TestVariant>(
        &TestVariant::relation_schema(),
        &format_config,
    )
    .unwrap();
    consumer.on_error(Some(Box::new(|_, _| {})));
    let (mut buffer, errors) = parser.parse(&message, None);
    assert_eq!(errors, []);
    buffer.flush().unwrap();
    assert_eq!(outputs.state().flushed, [MockUpdate::Insert(sample)]);
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
    run_in_posix_runtime(Some(ConfigSize::ZERO), Some(ConfigSize::ZERO), move || {
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

/// The record layer's Avro streams, for a table without and with a primary key, decode with
/// the schema that the parser installed last; a fork keeps that schema, and a record before
/// any schema is an error.
#[test]
fn avro_stream_decodes_with_the_installed_schema() {
    let config = || dbsp::circuit::CircuitConfig::with_workers(1);
    let circuits = [
        crate::test::test_circuit::<TestStruct>(config(), &TestStruct::schema(), &[None]),
        crate::test::test_circuit_with_index::<TestStruct, KeyStruct, _>(
            config(),
            &TestStruct::schema(),
            &[SqlIdentifier::from("id")],
            |record| KeyStruct { id: record.id },
            &[None],
            false,
        ),
    ];
    let schema = AvroSchema::parse_str(TestStruct::avro_schema()).unwrap();
    let refs = HashMap::new();
    let record = TestStruct::for_id(1)
        .serialize_with_context(
            AvroSchemaSerializer::new(&schema, &HashMap::new(), false),
            &avro_ser_config(),
        )
        .unwrap();

    for (circuit, catalog) in circuits {
        let mut stream = catalog
            .input_collection_handle(&SqlIdentifier::from("test_input1"))
            .unwrap()
            .handle
            .configure_avro_deserializer()
            .unwrap();
        let error = stream.insert(&record, 1, &None).unwrap_err();
        assert!(error.to_string().contains("no schema"), "{error}");

        stream.configure_schema(&schema, &refs).unwrap();
        stream.insert(&record, 1, &None).unwrap();
        let mut fork = stream.fork();
        fork.insert(&record, 1, &None).unwrap();
        assert_eq!(stream.len().records, 1);
        assert_eq!(fork.len().records, 1);

        // A later schema replaces the first, and the record does not match it.
        stream.configure_schema(&AvroSchema::Long, &refs).unwrap();
        let error = stream.insert(&record, 1, &None).unwrap_err();
        assert!(
            error.to_string().contains("expected record schema"),
            "{error}"
        );

        circuit.kill().unwrap();
    }
}
