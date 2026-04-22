mod bench_common;

use bench_common::{
    BenchKeyStruct, BenchOutputConsumer, BenchTestStruct, build_indexed_batch, generate_test_data,
};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use dbsp_adapters::Encoder;
use dbsp_adapters::format::avro::output::AvroEncoder;
use feldera_adapterlib::transport::OutputBatchType;
use feldera_types::format::avro::{AvroEncoderConfig, AvroUpdateFormat};

// ---------------------------------------------------------------------------
// Avro-specific helpers
// ---------------------------------------------------------------------------

fn create_indexed_encoder(threads: usize) -> AvroEncoder {
    let config = AvroEncoderConfig {
        threads,
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
                    encoder.consumer().batch_start(0, OutputBatchType::Delta);
                    encoder.encode(batch.clone().arc_as_batch_reader()).unwrap();
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
                        encoder.consumer().batch_start(0, OutputBatchType::Delta);
                        encoder.encode(batch.clone().arc_as_batch_reader()).unwrap();
                        encoder.consumer().batch_end();
                    });
                },
            );
        }
    }

    group.finish();
}

criterion_group!(benches, bench_indexed_encode, bench_indexed_encode_scaling,);
criterion_main!(benches);
