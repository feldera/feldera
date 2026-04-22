mod bench_common;

use bench_common::{BenchKeyStruct, BenchTestStruct, build_indexed_batch, generate_test_data};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use dbsp_adapters::Encoder;
use dbsp_adapters::SerBatch;
use dbsp_adapters::integrated::PostgresOutputEndpoint;
use feldera_adapterlib::transport::OutputBatchType;
use feldera_types::transport::postgres::{
    PostgresTlsConfig, PostgresWriteMode, PostgresWriterConfig,
};
use postgres::NoTls;
use std::sync::{Arc, Weak};

// ---------------------------------------------------------------------------
// Postgres-specific helpers
// ---------------------------------------------------------------------------

const BENCH_TABLE: &str = "bench_postgres_output";

fn postgres_url() -> String {
    std::env::var("POSTGRES_URL")
        .unwrap_or_else(|_| "postgres://postgres:password@localhost:5432".to_string())
}

fn postgres_client() -> postgres::Client {
    postgres::Client::connect(&postgres_url(), NoTls).expect("failed to connect to postgres")
}

fn create_bench_table(client: &mut postgres::Client) {
    client
        .execute(
            &format!(
                r#"CREATE TABLE IF NOT EXISTS "{BENCH_TABLE}" (
                    id INTEGER PRIMARY KEY,
                    b BOOLEAN NOT NULL,
                    i BIGINT,
                    s VARCHAR NOT NULL
                )"#
            ),
            &[],
        )
        .expect("failed to create benchmark table");
}

fn truncate_bench_table(client: &mut postgres::Client) -> u64 {
    client
        .execute(&format!(r#"DELETE FROM "{BENCH_TABLE}""#), &[])
        .expect("failed to truncate benchmark table")
}

fn drop_bench_table(client: &mut postgres::Client) {
    client
        .execute(&format!(r#"DROP TABLE IF EXISTS "{BENCH_TABLE}""#), &[])
        .expect("failed to drop benchmark table");
}

fn make_config(threads: usize) -> PostgresWriterConfig {
    PostgresWriterConfig {
        uri: postgres_url(),
        table: BENCH_TABLE.to_string(),
        mode: PostgresWriteMode::Materialized,
        cdc_op_column: "__feldera_op".to_string(),
        cdc_ts_column: "__feldera_ts".to_string(),
        extra_columns: Vec::new(),
        tls: PostgresTlsConfig::default(),
        max_records_in_buffer: None,
        max_buffer_size_bytes: usize::pow(2, 20),
        on_conflict_do_nothing: false,
        threads,
    }
}

fn create_endpoint(config: &PostgresWriterConfig) -> PostgresOutputEndpoint {
    PostgresOutputEndpoint::new(
        Default::default(),
        "bench_endpoint",
        config,
        &Some(BenchKeyStruct::relation_schema()),
        &BenchTestStruct::relation_schema(),
        Weak::new(),
    )
    .expect("failed to create PostgresOutputEndpoint")
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

fn bench_encode_iter(
    endpoint: &mut PostgresOutputEndpoint,
    batch: &Arc<dyn SerBatch>,
    pg_client: &mut postgres::Client,
    num_records: usize,
    iters: u64,
) -> std::time::Duration {
    let mut total = std::time::Duration::ZERO;
    for _ in 0..iters {
        let start = std::time::Instant::now();
        endpoint.consumer().batch_start(0, OutputBatchType::Delta);
        endpoint
            .encode(batch.clone().arc_as_batch_reader())
            .unwrap();
        endpoint.consumer().batch_end();
        total += start.elapsed();

        let truncated = truncate_bench_table(pg_client);
        assert_eq!(truncated, num_records as u64);
    }
    total
}

/// Benchmark Postgres output with 100k records across 1/2/4/8 worker threads.
fn bench_postgres_encode(c: &mut Criterion) {
    let mut pg_client = postgres_client();
    create_bench_table(&mut pg_client);

    let num_records = 100_000;
    let data = generate_test_data(num_records);
    let batch = build_indexed_batch(&data);

    let mut group = c.benchmark_group("postgres_output_encode");
    group.throughput(criterion::Throughput::Elements(num_records as u64));
    group.sample_size(10);

    for workers in [1, 2, 4, 8] {
        let config = make_config(workers);
        group.bench_with_input(BenchmarkId::new("workers", workers), &workers, |b, _| {
            let mut endpoint = create_endpoint(&config);
            b.iter_custom(|iters| {
                bench_encode_iter(&mut endpoint, &batch, &mut pg_client, num_records, iters)
            });
        });
    }

    group.finish();
    drop_bench_table(&mut pg_client);
}

/// Benchmark Postgres output scaling: 100k/1M/2M records x 1/2/4/8 workers.
fn bench_postgres_encode_scaling(c: &mut Criterion) {
    let mut pg_client = postgres_client();
    create_bench_table(&mut pg_client);

    let mut group = c.benchmark_group("postgres_output_encode_scaling");
    group.sample_size(10);

    for num_records in [100_000, 1_000_000, 2_000_000] {
        let data = generate_test_data(num_records);
        let batch = build_indexed_batch(&data);

        for workers in [1, 2, 4, 8] {
            let config = make_config(workers);
            group.throughput(criterion::Throughput::Elements(num_records as u64));
            group.bench_with_input(
                BenchmarkId::new(format!("{num_records}_records"), workers),
                &workers,
                |b, _| {
                    let mut endpoint = create_endpoint(&config);
                    b.iter_custom(|iters| {
                        bench_encode_iter(&mut endpoint, &batch, &mut pg_client, num_records, iters)
                    });
                },
            );
        }
    }

    group.finish();
    drop_bench_table(&mut pg_client);
}

criterion_group!(
    benches,
    bench_postgres_encode,
    bench_postgres_encode_scaling,
);
criterion_main!(benches);
