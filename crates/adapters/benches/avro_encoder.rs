use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use dbsp::utils::Tup2;
use dbsp::{OrdIndexedZSet, OrdZSet};
use dbsp_adapters::format::avro::output::AvroEncoder;
use dbsp_adapters::static_compile::seroutput::SerBatchImpl;
use dbsp_adapters::{Encoder, OutputConsumer, SerBatch};
use feldera_macros::IsNone;
use feldera_types::format::avro::{AvroEncoderConfig, AvroUpdateFormat};
use feldera_types::program_schema::{ColumnType, Field, Relation, SqlIdentifier};
use feldera_types::{deserialize_without_context, serialize_struct};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use size_of::SizeOf;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

// ---------------------------------------------------------------------------
// Inline type definitions (mirror of test::data types, without test-only deps)
// ---------------------------------------------------------------------------

#[derive(
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    serde::Serialize,
    serde::Deserialize,
    Clone,
    Hash,
    SizeOf,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    IsNone,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
struct BenchTestStruct {
    id: u32,
    b: bool,
    i: Option<i64>,
    s: String,
}

deserialize_without_context!(BenchTestStruct);

serialize_struct!(BenchTestStruct()[4]{
    id["id"]: u32,
    b["b"]: bool,
    i["i"]: Option<i64>,
    s["s"]: String
});

impl BenchTestStruct {
    fn relation_schema() -> Relation {
        Relation {
            name: SqlIdentifier::new("BenchTestStruct", false),
            fields: vec![
                Field::new("id".into(), ColumnType::bigint(false)),
                Field::new("b".into(), ColumnType::boolean(false)),
                Field::new("i".into(), ColumnType::bigint(true)),
                Field::new("s".into(), ColumnType::varchar(false)),
            ],
            materialized: false,
            properties: BTreeMap::new(),
        }
    }

    fn avro_schema() -> &'static str {
        r#"{
            "type": "record",
            "name": "BenchTestStruct",
            "fields": [
                { "name": "id", "type": "long" },
                { "name": "b", "type": "boolean" },
                { "name": "i", "type": ["null", "long"] },
                { "name": "s", "type": "string" }
            ]
        }"#
    }
}

#[derive(
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    serde::Serialize,
    serde::Deserialize,
    Clone,
    Hash,
    SizeOf,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    IsNone,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
struct BenchKeyStruct {
    id: u32,
}

deserialize_without_context!(BenchKeyStruct);

serialize_struct!(BenchKeyStruct()[1]{
    id["id"]: u32
});

impl BenchKeyStruct {
    fn relation_schema() -> Relation {
        Relation {
            name: SqlIdentifier::new("BenchKeyStruct", false),
            fields: vec![Field::new("id".into(), ColumnType::bigint(false))],
            materialized: false,
            properties: BTreeMap::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Inline MockOutputConsumer (mirrors test::MockOutputConsumer)
// ---------------------------------------------------------------------------

#[allow(clippy::type_complexity)]
struct BenchOutputConsumer {
    data: Arc<
        Mutex<
            Vec<(
                Option<Vec<u8>>,
                Option<Vec<u8>>,
                Vec<(String, Option<Vec<u8>>)>,
            )>,
        >,
    >,
}

impl BenchOutputConsumer {
    fn new() -> Self {
        Self {
            data: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl OutputConsumer for BenchOutputConsumer {
    fn max_buffer_size_bytes(&self) -> usize {
        usize::MAX
    }

    fn batch_start(&mut self, _step: u64) {}

    fn push_buffer(&mut self, buffer: &[u8], _num_records: usize) {
        self.data
            .lock()
            .unwrap()
            .push((None, Some(buffer.to_vec()), vec![]));
    }

    fn push_key(
        &mut self,
        key: Option<&[u8]>,
        val: Option<&[u8]>,
        headers: &[(&str, Option<&[u8]>)],
        _num_records: usize,
    ) {
        self.data.lock().unwrap().push((
            key.map(|k| k.to_vec()),
            val.map(|v| v.to_vec()),
            headers
                .iter()
                .map(|(k, v)| (k.to_string(), v.map(|bytes| bytes.to_vec())))
                .collect(),
        ));
    }

    fn batch_end(&mut self) {}
}

// ---------------------------------------------------------------------------
// Benchmark helpers
// ---------------------------------------------------------------------------

const SEED: u64 = 0xdeadbeef;

fn generate_test_data(num_records: usize) -> Vec<BenchTestStruct> {
    let mut rng = SmallRng::seed_from_u64(SEED);
    (0..num_records)
        .map(|i| BenchTestStruct {
            id: i as u32,
            b: rng.r#gen(),
            i: if rng.r#gen::<bool>() {
                Some(rng.gen_range(-1_000_000..1_000_000))
            } else {
                None
            },
            s: format!("record_{i}_{}", rng.gen_range(0u32..100_000)),
        })
        .collect()
}

fn build_indexed_batch(data: &[BenchTestStruct]) -> Arc<dyn SerBatch> {
    let tuples: Vec<_> = data
        .iter()
        .map(|v| Tup2(Tup2(BenchKeyStruct { id: v.id }, v.clone()), 1i64))
        .collect();
    let zset = OrdIndexedZSet::from_tuples((), tuples);
    Arc::new(<SerBatchImpl<_, BenchKeyStruct, BenchTestStruct>>::new(zset))
}

fn build_plain_batch(data: &[BenchTestStruct]) -> Arc<dyn SerBatch> {
    let tuples: Vec<_> = data.iter().map(|v| Tup2(v.clone(), 1i64)).collect();
    let zset = OrdZSet::from_keys((), tuples);
    Arc::new(<SerBatchImpl<_, BenchTestStruct, ()>>::new(zset))
}

fn create_indexed_encoder(workers: usize) -> AvroEncoder {
    let config = AvroEncoderConfig {
        workers,
        update_format: AvroUpdateFormat::Raw,
        skip_schema_id: true,
        ..Default::default()
    };
    let consumer = BenchOutputConsumer::new();
    AvroEncoder::create(
        "bench_endpoint",
        &Some(BenchKeyStruct::relation_schema()),
        &BenchTestStruct::relation_schema(),
        Box::new(consumer),
        config,
        None,
    )
    .unwrap()
}

fn create_plain_encoder(workers: usize) -> AvroEncoder {
    let config = AvroEncoderConfig {
        workers,
        schema: Some(BenchTestStruct::avro_schema().to_string()),
        update_format: AvroUpdateFormat::Raw,
        skip_schema_id: true,
        ..Default::default()
    };
    let consumer = BenchOutputConsumer::new();
    AvroEncoder::create(
        "bench_endpoint",
        &None,
        &BenchTestStruct::relation_schema(),
        Box::new(consumer),
        config,
        None,
    )
    .unwrap()
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

/// Benchmark parallel (indexed) Avro encoding with 100k records across 1/2/4/8 workers.
fn bench_indexed_encode(c: &mut Criterion) {
    let num_records = 100_000;
    let data = generate_test_data(num_records);
    let batch = build_indexed_batch(&data);

    let mut group = c.benchmark_group("avro_indexed_encode");
    group.throughput(criterion::Throughput::Elements(num_records as u64));

    for workers in [1, 2, 4, 8] {
        group.bench_with_input(
            BenchmarkId::new("workers", workers),
            &workers,
            |b, &workers| {
                let mut encoder = create_indexed_encoder(workers);
                b.iter(|| {
                    encoder.consumer().batch_start(0);
                    encoder
                        .encode(batch.clone().arc_as_batch_reader())
                        .unwrap();
                    encoder.consumer().batch_end();
                });
            },
        );
    }

    group.finish();
}

/// Benchmark indexed encoding scaling: 1k/10k/100k records x 1/2/4/8 workers.
fn bench_indexed_encode_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("avro_indexed_encode_scaling");

    for num_records in [1_000, 10_000, 100_000] {
        let data = generate_test_data(num_records);
        let batch = build_indexed_batch(&data);

        for workers in [1, 2, 4, 8] {
            group.throughput(criterion::Throughput::Elements(num_records as u64));
            group.bench_with_input(
                BenchmarkId::new(format!("{num_records}_records"), workers),
                &workers,
                |b, &workers| {
                    let mut encoder = create_indexed_encoder(workers);
                    b.iter(|| {
                        encoder.consumer().batch_start(0);
                        encoder
                            .encode(batch.clone().arc_as_batch_reader())
                            .unwrap();
                        encoder.consumer().batch_end();
                    });
                },
            );
        }
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_indexed_encode,
    bench_indexed_encode_scaling,
);
criterion_main!(benches);
