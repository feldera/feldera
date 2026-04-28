mod bench_common;

use bench_common::{
    BenchKeyStruct, BenchTestStruct, NoOpControllerRef, build_indexed_batch, generate_test_data,
};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use feldera_adapterlib::connector::connector_by_name;
use feldera_adapterlib::transport::IntegratedOutputEndpoint;
use feldera_types::transport::delta_table::{DeltaTableWriteMode, DeltaTableWriterConfig};
use std::sync::Arc;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Delta-specific helpers
// ---------------------------------------------------------------------------

fn create_indexed_writer(threads: usize, table_uri: &str) -> Box<dyn IntegratedOutputEndpoint> {
    let config = DeltaTableWriterConfig {
        uri: table_uri.to_string(),
        mode: DeltaTableWriteMode::Truncate,
        max_retries: Some(0),
        threads: Some(threads),
        object_store_config: Default::default(),
        checkpoint_interval: None,
    };
    let key_schema = Some(BenchKeyStruct::relation_schema());
    let mut value_schema = BenchTestStruct::relation_schema();
    value_schema.materialized = true;
    let descriptor = connector_by_name("delta_table_output")
        .expect("delta_table_output descriptor not registered");
    let build = descriptor
        .build_integrated_output
        .expect("delta_table_output is not an integrated output connector");
    let config_value =
        serde_json::to_value(&config).expect("failed to serialize DeltaTableWriterConfig");
    build(
        0,
        "bench_endpoint",
        &config_value,
        &key_schema,
        &value_schema,
        Arc::new(NoOpControllerRef),
        false,
    )
    .unwrap()
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

/// Benchmark parallel Delta table encoding with 100k records across 1/2/4/8 workers.
fn bench_indexed_encode(c: &mut Criterion) {
    let num_records = 100_000;
    let data = generate_test_data(num_records);
    let batch = build_indexed_batch(&data);

    let mut group = c.benchmark_group("delta_indexed_encode");
    group.throughput(criterion::Throughput::Elements(num_records as u64));

    for workers in [1, 2, 4, 8] {
        group.bench_with_input(
            BenchmarkId::new("workers", workers),
            &workers,
            |b, &workers| {
                // Each iteration needs a fresh directory since the writer
                // creates real Parquet files.
                b.iter_with_setup(
                    || {
                        let table_dir = TempDir::new().unwrap();
                        let table_uri = table_dir.path().display().to_string();
                        let writer = create_indexed_writer(workers, &table_uri);
                        (writer, table_dir)
                    },
                    |(mut writer, _table_dir)| {
                        writer.consumer().batch_start(0);
                        writer.encode(batch.clone().arc_as_batch_reader()).unwrap();
                        writer.consumer().batch_end();
                    },
                );
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_indexed_encode);
criterion_main!(benches);
