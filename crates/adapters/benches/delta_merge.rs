//! What one merge-mode flush costs, and what drives that cost.
//!
//! Merge mode locates the row a key supersedes by probing the data files the key could be
//! in, so a flush's cost follows the size of the *table*, not the size of the batch. A
//! benchmark that varies only the batch size therefore measures the wrong variable. These
//! hold the batch fixed and vary the table instead.

mod bench_common;

use bench_common::{
    BenchKeyStruct, BenchTestStruct, build_indexed_batch, build_update_batch, generate_test_data,
};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use dbsp_adapters::Encoder;
use dbsp_adapters::integrated::delta_table::DeltaTableWriter;
use feldera_adapterlib::transport::OutputBatchType;
use feldera_types::transport::delta_table::{
    DeltaTableUpdateMode, DeltaTableWriteMode, DeltaTableWriterConfig,
};
use std::fs;
use std::path::Path;
use std::sync::Weak;
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

/// Rows the table holds before a benchmarked flush touches it.
const TABLE_ROWS: usize = 100_000;
/// Rows one benchmarked flush changes.
const BATCH_ROWS: usize = 10_000;

fn writer(
    table_uri: &str,
    mode: DeltaTableWriteMode,
    update: DeltaTableUpdateMode,
) -> DeltaTableWriter {
    let config = DeltaTableWriterConfig {
        uri: table_uri.to_string(),
        mode,
        variant_encoding: Default::default(),
        max_retries: Some(0),
        threads: Some(1),
        object_store_config: Default::default(),
        checkpoint_interval: None,
        log_retention_duration: None,
        enable_expired_log_cleanup: None,
        update_mode: update,
        lookup_chunk_bytes: 1 << 20,
        max_concurrent_probes: 4,
        optimize_interval_secs: None,
    };
    let key_schema = Some(BenchKeyStruct::relation_schema());
    let mut value_schema = BenchTestStruct::relation_schema();
    value_schema.materialized = true;
    DeltaTableWriter::new(
        Default::default(),
        "bench_endpoint",
        &config,
        &key_schema,
        &value_schema,
        Weak::new(),
        CancellationToken::new(),
        false,
        true,
        true,
    )
    .unwrap()
}

/// Push one batch through a writer the way the controller does.
fn flush(
    writer: &mut DeltaTableWriter,
    batch: &std::sync::Arc<dyn feldera_adapterlib::catalog::SerBatch>,
) {
    writer.consumer().batch_start(0, OutputBatchType::Delta);
    writer.encode(batch.clone().arc_as_batch_reader()).unwrap();
    writer.consumer().batch_end();
}

/// Seed a merge-mode table with `TABLE_ROWS` rows spread over `files` data files.
///
/// One flush writes one file, which is also how a running pipeline accumulates them.
fn seed(uri: &str, data: &[BenchTestStruct], files: usize) {
    let mut w = writer(
        uri,
        DeltaTableWriteMode::Truncate,
        DeltaTableUpdateMode::Merge,
    );
    for chunk in data.chunks(data.len().div_ceil(files)) {
        flush(&mut w, &build_indexed_batch(chunk));
    }
}

/// Copy a seeded table so each iteration starts from the same one.
///
/// A flush mutates the table it measures -- it appends a file and grows a deletion vector --
/// so repeating one against the same table would drift the very variable being varied.
fn copy_dir(from: &Path, to: &Path) {
    fs::create_dir_all(to).unwrap();
    for entry in fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            fs::copy(entry.path(), target).unwrap();
        }
    }
}

/// How a flush's cost grows with the number of files it may have to probe.
///
/// The batch is the same every time, so anything this shows is the table's doing. A 1B-row
/// run spent minutes per 100k-row batch against 2,164 files; this is that shape in miniature.
fn bench_files(c: &mut Criterion) {
    let data = generate_test_data(TABLE_ROWS);
    // Spread across the whole key space, not the first `BATCH_ROWS` keys. Rows are seeded in
    // id order, so a contiguous batch lands in one file and every other file prunes away on
    // its statistics -- which measures the pruning rather than the probing, and makes a
    // bigger table look cheaper.
    let stride = TABLE_ROWS / BATCH_ROWS;
    let touched: Vec<BenchTestStruct> = data.iter().step_by(stride).cloned().collect();
    let updated: Vec<BenchTestStruct> = touched
        .iter()
        .map(|r| BenchTestStruct {
            s: format!("updated_{}", r.id),
            ..r.clone()
        })
        .collect();
    let batch = build_update_batch(&touched, &updated);

    let mut group = c.benchmark_group("delta_merge_update");
    group.throughput(criterion::Throughput::Elements(touched.len() as u64));
    group.sample_size(10);

    for files in [1usize, 4, 16, 64] {
        let seeded = TempDir::new().unwrap();
        seed(seeded.path().to_str().unwrap(), &data, files);

        group.bench_with_input(BenchmarkId::new("files", files), &files, |b, _| {
            b.iter_batched(
                || {
                    let dir = TempDir::new().unwrap();
                    copy_dir(seeded.path(), dir.path());
                    let w = writer(
                        dir.path().to_str().unwrap(),
                        DeltaTableWriteMode::Append,
                        DeltaTableUpdateMode::Merge,
                    );
                    (w, dir)
                },
                |(mut w, _dir)| flush(&mut w, &batch),
                criterion::BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

/// What merge mode costs over cdc when no row has to be located.
///
/// Inserts into a table the connector owns skip the lookup, so this isolates the rest of the
/// write path from the probe that `bench_files` measures.
fn bench_insert_against_cdc(c: &mut Criterion) {
    let data = generate_test_data(BATCH_ROWS);
    let batch = build_indexed_batch(&data);

    let mut group = c.benchmark_group("delta_insert");
    group.throughput(criterion::Throughput::Elements(BATCH_ROWS as u64));
    group.sample_size(10);

    for (name, update_mode) in [
        ("merge", DeltaTableUpdateMode::Merge),
        ("cdc", DeltaTableUpdateMode::Cdc),
    ] {
        group.bench_function(name, |b| {
            b.iter_batched(
                || {
                    let dir = TempDir::new().unwrap();
                    let w = writer(
                        dir.path().to_str().unwrap(),
                        DeltaTableWriteMode::Truncate,
                        update_mode,
                    );
                    (w, dir)
                },
                |(mut w, _dir)| flush(&mut w, &batch),
                criterion::BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

/// How a flush's cost follows the share of the table its keys cover.
///
/// The lookup's pre-filter rejects a key the batch does not hold for one hash, so it pays
/// off in proportion to how much of the table it can reject. This sweeps that ratio from a
/// steady-state flush up to one that rewrites every row, where nothing is rejected and the
/// filter is pure overhead. The table is fixed, so anything this shows is the batch's doing.
fn bench_key_share(c: &mut Criterion) {
    const FILES: usize = 16;
    let data = generate_test_data(TABLE_ROWS);
    let seeded = TempDir::new().unwrap();
    seed(seeded.path().to_str().unwrap(), &data, FILES);

    let mut group = c.benchmark_group("delta_merge_key_share");
    group.sample_size(10);

    for keys in [1_000usize, 10_000, TABLE_ROWS] {
        // Spread across the whole key space, so no file prunes away on its statistics.
        let stride = TABLE_ROWS / keys;
        let touched: Vec<BenchTestStruct> = data.iter().step_by(stride).cloned().collect();
        let updated: Vec<BenchTestStruct> = touched
            .iter()
            .map(|r| BenchTestStruct {
                s: format!("updated_{}", r.id),
                ..r.clone()
            })
            .collect();
        let batch = build_update_batch(&touched, &updated);

        group.throughput(criterion::Throughput::Elements(touched.len() as u64));
        group.bench_with_input(BenchmarkId::new("keys", keys), &keys, |b, _| {
            b.iter_batched(
                || {
                    let dir = TempDir::new().unwrap();
                    copy_dir(seeded.path(), dir.path());
                    let w = writer(
                        dir.path().to_str().unwrap(),
                        DeltaTableWriteMode::Append,
                        DeltaTableUpdateMode::Merge,
                    );
                    (w, dir)
                },
                |(mut w, _dir)| flush(&mut w, &batch),
                criterion::BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_files,
    bench_key_share,
    bench_insert_against_cdc
);
criterion_main!(benches);
