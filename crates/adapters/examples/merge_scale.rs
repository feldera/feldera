//! Scale probe for Delta merge mode. Not part of the test suite; run by hand.
//!
//! Seeds a table one file per flush (the shape merge mode itself produces), then times a
//! single update flush against it through the public writer. Sweeps the two axes that
//! decide the per-flush cost: how many files the table holds, and how many of them the
//! flush's keys reach.
//!
//! Env: MERGE_FILES, MERGE_ROWS_PER_FILE, MERGE_FLUSH_KEYS (comma-separated sweeps).

// Shared with the benchmarks, which use more of it than this probe does.
#[allow(dead_code)]
#[path = "../benches/bench_common.rs"]
mod bench_common;

use bench_common::{BenchKeyStruct, BenchTestStruct};
use dbsp::OrdIndexedZSet;
use dbsp::utils::Tup2;
use dbsp_adapters::integrated::delta_table::DeltaTableWriter;
use dbsp_adapters::static_compile::seroutput::SerBatchImpl;
use dbsp_adapters::{Encoder, SerBatch};
use feldera_adapterlib::transport::OutputBatchType;
use feldera_types::transport::delta_table::{
    DeltaTableUpdateMode, DeltaTableWriteMode, DeltaTableWriterConfig, DeltaVariantEncoding,
};
use std::sync::{Arc, Weak};
use std::time::Instant;
use tempfile::TempDir;

/// How the seeded table lays its keys out across files.
#[derive(Clone, Copy, PartialEq)]
enum Layout {
    /// File `f` holds one contiguous key range, as compaction or a sorted write leaves it.
    Clustered,
    /// Every file's key range spans the whole space, so stats prune nothing.
    Shuffled,
}

impl Layout {
    fn name(self) -> &'static str {
        match self {
            Layout::Clustered => "clustered",
            Layout::Shuffled => "shuffled",
        }
    }

    /// The key the `n`th inserted row gets, out of `space` (a power of two).
    ///
    /// Clustered hands out keys in order, so each file holds one contiguous range and a
    /// lookup can prune to it. Shuffled multiplies by an odd constant, a bijection modulo
    /// a power of two, so every file's range spans the whole space and stats prune nothing.
    fn key_for(self, n: usize, space: usize) -> u32 {
        match self {
            Layout::Clustered => n as u32,
            Layout::Shuffled => (n.wrapping_mul(2_654_435_761) & (space - 1)) as u32,
        }
    }
}

fn writer(uri: &str, mode: DeltaTableWriteMode) -> DeltaTableWriter {
    let config = DeltaTableWriterConfig {
        uri: uri.to_string(),
        mode,
        variant_encoding: DeltaVariantEncoding::default(),
        update_mode: DeltaTableUpdateMode::Merge,
        lookup_chunk_bytes: 256 << 20,
        max_concurrent_probes: 4,
        checkpoint_interval: Some(100),
        log_retention_duration: None,
        enable_expired_log_cleanup: None,
        max_retries: Some(0),
        threads: Some(1),
        optimize_interval_secs: None,

        object_store_config: Default::default(),
    };
    let key_schema = Some(BenchKeyStruct::relation_schema());
    let mut value_schema = BenchTestStruct::relation_schema();
    value_schema.materialized = true;
    DeltaTableWriter::new(
        Default::default(),
        "merge_scale",
        &config,
        &key_schema,
        &value_schema,
        Weak::new(),
        false,
        true,
    )
    .unwrap()
}

/// One batch of insertions for `keys`; `tag` distinguishes row versions.
fn batch(keys: &[u32], tag: &str) -> Arc<dyn SerBatch> {
    let tuples: Vec<_> = keys
        .iter()
        .map(|&id| {
            let row = BenchTestStruct {
                id,
                b: false,
                i: Some(id as i64),
                s: format!("{tag}_{id}"),
            };
            Tup2(Tup2(BenchKeyStruct { id }, row), 1i64)
        })
        .collect();
    let zset = OrdIndexedZSet::from_tuples((), tuples);
    Arc::new(<SerBatchImpl<_, BenchKeyStruct, BenchTestStruct>>::new(
        zset,
    ))
}

fn flush(w: &mut DeltaTableWriter, step: u64, b: Arc<dyn SerBatch>) {
    w.consumer().batch_start(step, OutputBatchType::Delta);
    w.encode(b.arc_as_batch_reader()).unwrap();
    w.consumer().batch_end();
}

/// Run one writer against a growing table and report what a flush costs as it grows.
///
/// Merge mode writes one file and one commit per flush, so the file count is the flush
/// count and every flush pays for the ones before it. The writer is built on a table that
/// already holds data, as a pipeline adopting an existing table is, so it runs in the
/// default regime and looks up every key in the flush. That is the steady-state cost.
fn grow(layout: Layout, flushes: usize, inserts: usize, updates: usize, step: usize) {
    let dir = TempDir::new().unwrap();
    let uri = dir.path().display().to_string();
    let space = (flushes * inserts).next_power_of_two();

    let mut inserted: Vec<u32> = Vec::with_capacity(flushes * inserts);
    let mut next = 0usize;

    // One file of existing data, so the measuring writer starts in the default regime.
    let mut creator = writer(&uri, DeltaTableWriteMode::Truncate);
    let first: Vec<u32> = (0..inserts)
        .map(|_| {
            let k = layout.key_for(next, space);
            next += 1;
            k
        })
        .collect();
    inserted.extend(&first);
    flush(&mut creator, 0, batch(&first, "v0"));
    drop(creator);

    let mut w = writer(&uri, DeltaTableWriteMode::Append);
    let mut window = Instant::now();
    for i in 0..flushes {
        // Keys the flush changes: `updates` of the keys already in the table, spread evenly
        // over them as arriving updates are, plus `inserts` never-seen keys.
        let stride = (inserted.len() / updates.max(1)).max(1);
        let mut ids: Vec<u32> = (0..updates)
            .filter_map(|u| inserted.get(u * stride).copied())
            .collect();
        for _ in 0..inserts {
            let k = layout.key_for(next, space);
            next += 1;
            inserted.push(k);
            ids.push(k);
        }

        flush(&mut w, i as u64 + 1, batch(&ids, "v1"));

        if (i + 1) % step == 0 {
            println!(
                "{:<10} {:>7} {:>11} {:>12.1}",
                layout.name(),
                i + 2,
                inserted.len(),
                window.elapsed().as_secs_f64() * 1e3 / step as f64,
            );
            window = Instant::now();
        }
    }
}

/// Seed a table of `files` files holding `rows` rows each, then time update-only flushes.
///
/// Holding the file count fixed and varying `rows` isolates what file size costs: the
/// probe reads the row groups of every file a key might live in, and every flush that
/// tombstones a row rewrites that file's whole deletion vector.
///
/// With `hot` set, all updates fall inside the first file's key range, so one file's
/// deletion vector absorbs every tombstone. That is the shape that exposes vector growth.
fn by_size(files: usize, rows: usize, flushes: usize, updates: usize, step: usize, hot: bool) {
    let dir = TempDir::new().unwrap();
    let uri = dir.path().display().to_string();
    let live = files * rows;

    // The seeding writer starts on an empty table, so it runs in the owned regime and skips
    // the lookup. Keys are handed out in order: file f holds [f*rows, (f+1)*rows).
    let mut seeder = writer(&uri, DeltaTableWriteMode::Truncate);
    for file in 0..files {
        let ids: Vec<u32> = ((file * rows) as u32..((file + 1) * rows) as u32).collect();
        flush(&mut seeder, file as u64, batch(&ids, "v0"));
    }
    drop(seeder);

    // Updates are drawn from the whole table, or from the first file alone when `hot`.
    let span = if hot { rows } else { live };
    let mut w = writer(&uri, DeltaTableWriteMode::Append);
    let mut window = Instant::now();
    let mut drawn = 0usize;
    for i in 0..flushes {
        let ids: Vec<u32> = (0..updates)
            .map(|_| {
                let k = ((drawn * 2_654_435_761) % span) as u32;
                drawn += 1;
                k
            })
            .collect();
        flush(&mut w, i as u64 + 1, batch(&ids, "v1"));

        if (i + 1) % step == 0 {
            println!(
                "{:>7} {:>10} {:>11} {:>8} {:>14.2}",
                files,
                rows,
                live,
                i + 1,
                window.elapsed().as_secs_f64() * 1e3 / step as f64,
            );
            window = Instant::now();
        }
    }
}

/// Seed big files whose key ranges all span the whole space, then update keys that live in
/// only one of them.
///
/// The one shape where a bloom filter can prune something a `[min, max]` test cannot. Every
/// file's range covers every key, so the range test has to keep all of them; the keys sought
/// are physically in one file, so every other file's filter can reject them. A table left
/// unsorted by `OPTIMIZE` and updated on a hot subset of keys looks like this.
fn by_overlap(files: usize, rows: usize, flushes: usize, updates: usize, step: usize) {
    let dir = TempDir::new().unwrap();
    let uri = dir.path().display().to_string();
    let space = (files * rows).next_power_of_two();

    let mut seeder = writer(&uri, DeltaTableWriteMode::Truncate);
    for file in 0..files {
        let ids: Vec<u32> = (file * rows..(file + 1) * rows)
            .map(|n| Layout::Shuffled.key_for(n, space))
            .collect();
        flush(&mut seeder, file as u64, batch(&ids, "v0"));
    }
    drop(seeder);

    // Drawn from the first file's rows alone, so one file holds every sought key while every
    // file's range still contains them.
    let mut w = writer(&uri, DeltaTableWriteMode::Append);
    let mut window = Instant::now();
    let mut drawn = 0usize;
    for i in 0..flushes {
        let ids: Vec<u32> = (0..updates)
            .map(|_| {
                let k = Layout::Shuffled.key_for(drawn % rows, space);
                drawn += 1;
                k
            })
            .collect();
        flush(&mut w, i as u64 + 1, batch(&ids, "v1"));

        if (i + 1) % step == 0 {
            println!(
                "{:>7} {:>10} {:>11} {:>8} {:>14.2}",
                files,
                rows,
                files * rows,
                i + 1,
                window.elapsed().as_secs_f64() * 1e3 / step as f64,
            );
            window = Instant::now();
        }
    }
}

fn sweep(var: &str, default: &[usize]) -> Vec<usize> {
    match std::env::var(var) {
        Ok(v) => v.split(',').map(|s| s.trim().parse().unwrap()).collect(),
        Err(_) => default.to_vec(),
    }
}

fn one(var: &str, default: usize) -> usize {
    sweep(var, &[default])[0]
}

fn main() {
    let step = one("MERGE_STEP", 100);
    let updates = one("MERGE_UPDATES", 8);

    match std::env::var("MERGE_MODE").unwrap_or_default().as_str() {
        // Fixed file count, varying file size: what does a bigger file cost?
        mode @ ("size" | "hot") => {
            let files = one("MERGE_SEED_FILES", 50);
            let flushes = one("MERGE_FLUSHES", 300);
            println!(
                "{updates} updates per flush, {} | {step}-flush windows",
                if mode == "hot" {
                    "all inside one file"
                } else {
                    "spread over the table"
                }
            );
            println!(
                "{:>7} {:>10} {:>11} {:>8} {:>14}",
                "files", "rows/file", "live rows", "flush", "ms per flush"
            );
            for rows in sweep("MERGE_SEED_ROWS", &[1_000, 10_000, 100_000]) {
                by_size(files, rows, flushes, updates, step, mode == "hot");
            }
        }
        // Big files whose ranges overlap: the case only a filter can prune.
        "overlap" => {
            let files = one("MERGE_SEED_FILES", 20);
            let rows = one("MERGE_SEED_ROWS", 100_000);
            let flushes = one("MERGE_FLUSHES", 300);
            println!(
                "{updates} updates per flush, all in one of {files} range-overlapping files \
                 | {step}-flush windows"
            );
            println!(
                "{:>7} {:>10} {:>11} {:>8} {:>14}",
                "files", "rows/file", "live rows", "flush", "ms per flush"
            );
            by_overlap(files, rows, flushes, updates, step);
        }
        // Growing file count: the default experiment.
        _ => {
            let flushes = one("MERGE_FLUSHES", 2000);
            let inserts = one("MERGE_INSERTS", 8);
            println!("{inserts} inserts + {updates} updates per flush");
            println!(
                "{:<10} {:>7} {:>11} {:>12}",
                "layout", "files", "live rows", "ms per flush"
            );
            let only = std::env::var("MERGE_LAYOUT").unwrap_or_default();
            for layout in [Layout::Clustered, Layout::Shuffled] {
                if only.is_empty() || only == layout.name() {
                    grow(layout, flushes, inserts, updates, step);
                }
            }
        }
    }
}
