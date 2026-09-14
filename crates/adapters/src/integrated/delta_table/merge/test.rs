//! Shared fixtures for the merge-mode tests, plus the protocol-level checks that
//! the whole design rests on.
//!
//! Three assumptions the design rests on, all of them in delta-rs and the Delta protocol
//! rather than in our code. A regression in any one is data loss no care on our side catches:
//!
//! 1. delta-rs will write to a table whose protocol has deletion vectors enabled.
//! 2. A `remove` plus an `add` for the same path in one commit installs a new vector, and a
//!    reader then skips exactly the tombstoned rows.
//! 3. Table maintenance does not delete a vector file that live `add` actions reference.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field as ArrowField, Schema as ArrowSchema};
use deltalake::datafusion::prelude::{SessionConfig, SessionContext};
use deltalake::kernel::transaction::{CommitBuilder, CommitProperties, PROTOCOL, TableReference};
use deltalake::kernel::{Action, DataType as DeltaDataType, PrimitiveType, StructField};
use deltalake::operations::create::CreateBuilder;
use deltalake::operations::vacuum::{VacuumMetrics, VacuumMode};
use deltalake::protocol::{DeltaOperation, SaveMode};
use deltalake::{DeltaTable, TableProperty};
use feldera_types::program_schema::{ColumnType, Field, Relation, SqlIdentifier, SqlType};
use tempfile::TempDir;

use deltalake::operations::get_num_idx_cols_and_stats_columns;
use deltalake::operations::write::writer::{DeltaWriter, WriterConfig};
use deltalake::parquet::file::properties::WriterProperties;
use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};

use deltalake::logstore::object_store;

use super::chunk::LookupChunk;
use super::key::KeyEncoder;
use super::probe::{Candidate, Pruning, locate};
use super::prune::PartitionFilter;
use super::tombstone::{Tombstones, write_deletion_vectors};

/// Arrow schema of the fixture table: an `id` key and a `payload` value.
pub(super) fn arrow_schema() -> ArrowSchema {
    ArrowSchema::new(vec![
        ArrowField::new("id", DataType::Int64, true),
        ArrowField::new("payload", DataType::Utf8, true),
    ])
}

/// Key relation naming `id`, matching what the `index` property would produce.
pub(super) fn key_relation() -> Relation {
    Relation {
        name: SqlIdentifier::new("k", false),
        fields: vec![Field::new(
            "id".into(),
            ColumnType {
                typ: SqlType::BigInt,
                nullable: false,
                precision: None,
                scale: None,
                component: None,
                fields: None,
                key: None,
                value: None,
            },
        )],
        materialized: false,
        properties: Default::default(),
        primary_key: None,
    }
}

/// Key relation naming both `id` and the partition column `payload`.
pub(super) fn partitioned_key_relation() -> Relation {
    let mut relation = key_relation();
    relation.fields.push(Field::new(
        "payload".into(),
        ColumnType {
            typ: SqlType::Varchar,
            nullable: false,
            precision: None,
            scale: None,
            component: None,
            fields: None,
            key: None,
            value: None,
        },
    ));
    relation
}

/// Key relation naming only the partition column `payload`, so no key column is stored in
/// the data files.
pub(super) fn partition_only_key_relation() -> Relation {
    let mut relation = partitioned_key_relation();
    relation.fields.remove(0);
    relation
}

/// Delta columns of the fixture table.
pub(super) fn fixture_columns() -> Vec<StructField> {
    vec![
        StructField::new("id", DeltaDataType::Primitive(PrimitiveType::Long), true),
        StructField::new(
            "payload",
            DeltaDataType::Primitive(PrimitiveType::String),
            true,
        ),
    ]
}

/// A partitioned table whose partition column is also a key column, with each
/// `(partition, id)` pair in its own partition directory.
pub(super) async fn partitioned_fixture_table(dir: &TempDir, rows: &[(i64, &str)]) -> DeltaTable {
    let table = CreateBuilder::new()
        .with_location(dir.path().to_str().unwrap())
        .with_save_mode(SaveMode::Ignore)
        .with_columns(fixture_columns())
        .with_partition_columns(["payload"])
        .with_configuration_property(TableProperty::EnableDeletionVectors, Some("true"))
        .await
        .unwrap();

    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema()),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|(_, p)| *p).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();

    table.write(vec![batch]).await.unwrap()
}

/// Create a table at `dir` and append `ids` as one data file. `deletion_vectors` selects
/// whether the table advertises the feature, which the negative test needs to omit.
pub(super) async fn fixture_table(
    dir: &TempDir,
    ids: &[i64],
    deletion_vectors: bool,
) -> DeltaTable {
    let table = CreateBuilder::new()
        .with_location(dir.path().to_str().unwrap())
        .with_save_mode(SaveMode::Ignore)
        .with_columns(fixture_columns())
        .with_configuration_property(
            TableProperty::EnableDeletionVectors,
            deletion_vectors.then_some("true"),
        )
        .await
        .unwrap();

    append_ids(table, ids).await
}

/// Append `ids` to `table` as one data file.
pub(super) async fn append_ids(table: DeltaTable, ids: &[i64]) -> DeltaTable {
    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema()),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(
                ids.iter().map(|i| format!("v{i}")).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();

    table.write(vec![batch]).await.unwrap()
}

/// A payload that Parquet cannot shrink: `width` hex characters of a hash of `id`.
///
/// File size is what decides whether a scan splits a file, and a repetitive payload
/// compresses away to nothing, so a fixture built from one cannot reach the threshold.
fn incompressible_payload(id: i64, width: usize) -> String {
    let mut out = String::with_capacity(width);
    let mut state = id as u64;
    while out.len() < width {
        // splitmix64, which needs no dependency and passes for random here.
        state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        out.push_str(&format!("{:016x}", z ^ (z >> 31)));
    }
    out.truncate(width);
    out
}

/// Append `ids` as one data file whose row groups hold `rows_per_group` rows each, every
/// row carrying `payload_bytes` of payload.
///
/// A one-row-group file is read as a single scan partition, so a reader can neither split
/// nor reorder it. Several row groups, in a file large enough for DataFusion to bother
/// repartitioning, is what lets a scan do either.
pub(super) async fn append_ids_in_row_groups(
    mut table: DeltaTable,
    ids: &[i64],
    rows_per_group: usize,
    payload_bytes: usize,
) -> DeltaTable {
    let schema = Arc::new(arrow_schema());
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(
                ids.iter()
                    .map(|i| incompressible_payload(*i, payload_bytes))
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();

    let (num_indexed_cols, stats_columns) =
        get_num_idx_cols_and_stats_columns(None, HashMap::new());
    let mut writer = DeltaWriter::new(
        table.object_store(),
        WriterConfig::new(
            schema,
            vec![],
            Some(
                WriterProperties::builder()
                    .set_max_row_group_row_count(Some(rows_per_group))
                    .build(),
            ),
            None,
            Some(rows_per_group),
            num_indexed_cols,
            stats_columns,
        ),
    );
    writer.write(&batch).await.unwrap();
    let actions = writer
        .close()
        .await
        .unwrap()
        .into_iter()
        .map(Action::Add)
        .collect();
    commit(&mut table, actions).await;
    table
}

/// Every data file in the current snapshot.
pub(super) fn candidates(table: &DeltaTable) -> Vec<Candidate> {
    let encoder = KeyEncoder::new(&key_relation(), &arrow_schema()).unwrap();
    table
        .snapshot()
        .unwrap()
        .log_data()
        .into_iter()
        .map(|f| Candidate::from_log(&f, Default::default(), &encoder, true))
        .collect()
}

/// Ids a reader sees, in ascending order. Through DataFusion rather than reading the parquet
/// files directly, since a test that read them itself would pass no matter what we wrote.
pub(super) async fn live_ids(table: &DeltaTable) -> Vec<i64> {
    let ctx = SessionContext::new();
    ctx.register_table("t", table.table_provider().await.unwrap())
        .unwrap();
    collect_ids(&ctx).await
}

/// The `id` column of table `t` registered in `ctx`, ascending.
async fn collect_ids(ctx: &SessionContext) -> Vec<i64> {
    let batches = ctx
        .sql("select id from t order by id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .map(|v| v.unwrap())
                .collect::<Vec<_>>()
        })
        .collect()
}

/// Ids a reader sees when the planner is free to split a file across partitions, which is
/// what it does to any file past `repartition_file_min_size`.
pub(super) async fn split_scan_ids(table: &DeltaTable) -> Vec<i64> {
    let mut config = SessionConfig::new().with_target_partitions(8);
    config.options_mut().optimizer.repartition_file_scans = true;
    let ctx = SessionContext::new_with_config(config);
    ctx.register_table("t", table.table_provider().await.unwrap())
        .unwrap();
    collect_ids(&ctx).await
}

/// Names of the deletion vector files present under the table directory.
pub(super) fn deletion_vector_files(dir: &TempDir) -> Vec<String> {
    let mut found = Vec::new();
    let mut stack = vec![dir.path().to_path_buf()];
    while let Some(path) = stack.pop() {
        for entry in std::fs::read_dir(&path).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                stack.push(entry.path());
            } else if entry.file_name().to_string_lossy().ends_with(".bin") {
                found.push(entry.file_name().to_string_lossy().into_owned());
            }
        }
    }
    found.sort();
    found
}

/// Tombstone `ids` in `table` and commit: the merge-mode flush minus the append side, so
/// tests drive the table through the same action sequence the connector produces.
pub(super) async fn tombstone_ids(mut table: DeltaTable, ids: &[i64]) -> DeltaTable {
    let encoder = KeyEncoder::new(&key_relation(), &arrow_schema()).unwrap();
    let mut chunk = LookupChunk::new(usize::MAX);
    chunk
        .extend(
            &encoder
                .encode_columns(&[Arc::new(Int64Array::from(ids.to_vec()))])
                .unwrap(),
        )
        .unwrap();
    chunk.sort();

    let mut tombstones = Tombstones::new();
    let candidates = candidates(&table);
    locate(
        &chunk,
        &candidates,
        &table,
        &encoder,
        4,
        Pruning::new(true, None),
        &mut tombstones,
    )
    .await
    .unwrap();

    let dv = write_deletion_vectors(&tombstones, &table).await.unwrap();
    // Empty actions are not committed, as the flush does not commit them.
    if !dv.actions.is_empty() {
        commit(&mut table, dv.actions).await;
    }
    table
}

/// Commit `actions` against the table's current snapshot.
pub(super) async fn commit(table: &mut DeltaTable, actions: Vec<Action>) {
    CommitBuilder::from(CommitProperties::default())
        .with_actions(actions)
        .build(
            table.state.as_ref().map(|s| s as &dyn TableReference),
            table.log_store(),
            DeltaOperation::Write {
                mode: SaveMode::Append,
                partition_by: None,
                predicate: None,
            },
        )
        .await
        .unwrap();
    table.update_incremental(None).await.unwrap();
}

/// Vacuum the table, deleting everything the operation considers unreferenced. Zero retention
/// with the duration check off is the most aggressive vacuum a user can run.
pub(super) async fn vacuum_everything(
    table: DeltaTable,
    mode: VacuumMode,
) -> (DeltaTable, VacuumMetrics) {
    table
        .vacuum()
        .with_mode(mode)
        .with_retention_period(chrono::Duration::zero())
        .with_enforce_retention_duration(false)
        .await
        .unwrap()
}

/// Candidates carrying each file's `payload` partition value, which the log holds instead of
/// the data file.
fn partitioned_candidates(table: &DeltaTable, encoder: &KeyEncoder) -> Vec<Candidate> {
    table
        .snapshot()
        .unwrap()
        .log_data()
        .into_iter()
        .map(|file| {
            let mut partition_keys = std::collections::HashMap::new();
            if let Some(values) = file.partition_values() {
                let index = values
                    .fields()
                    .iter()
                    .position(|f| f.name() == "payload")
                    .unwrap();
                partition_keys.insert("payload".to_string(), values.values()[index].clone());
            }
            Candidate::from_log(&file, partition_keys, encoder, true)
        })
        .collect()
}

/// A key column that is also a partition column must still be found: its value is in the log,
/// not the data file. Two partitions hold the same `id`, so a probe that ignored the column
/// rather than reconstructing it would tombstone both rows.
#[tokio::test]
async fn a_partition_column_key_is_reconstructed_from_the_log() {
    let dir = TempDir::new().unwrap();
    let table = partitioned_fixture_table(&dir, &[(1, "a"), (2, "a"), (1, "b"), (2, "b")]).await;

    let encoder = KeyEncoder::new(&partitioned_key_relation(), &arrow_schema()).unwrap();
    let key_columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![1])),
        Arc::new(StringArray::from(vec!["b"])),
    ];
    let mut chunk = LookupChunk::new(usize::MAX);
    chunk
        .extend(&encoder.encode_columns(&key_columns).unwrap())
        .unwrap();
    chunk.sort();

    // `payload` is both a key column and the partition column, so the flush knows which
    // partition every key belongs to and files in any other partition hold nothing.
    let key_fields: Vec<ArrowField> = encoder
        .column_indices()
        .iter()
        .map(|i| arrow_schema().field(*i).clone())
        .collect();
    let mut partitions = PartitionFilter::new(
        encoder.column_names(),
        &key_fields,
        &["payload".to_string()],
    )
    .unwrap()
    .unwrap();
    partitions.record(&key_columns).unwrap();

    let candidates = partitioned_candidates(&table, &encoder);

    let mut tombstones = Tombstones::new();
    let metrics = locate(
        &chunk,
        &candidates,
        &table,
        &encoder,
        4,
        Pruning::new(true, Some(&partitions)),
        &mut tombstones,
    )
    .await
    .unwrap();

    assert_eq!(
        metrics.rows_located, 1,
        "exactly the (1, \"b\") row must be located, not both rows with id 1: {metrics:?}"
    );
    assert_eq!(metrics.keys_not_found, 0);
    // The partition value is an exact statistic, so partition "a" is pruned from the log.
    assert_eq!(metrics.files_pruned, 1, "{metrics:?}");
}

/// Every key column being a partition column leaves the probe projecting no columns at all.
///
/// The row ordinals still have to be right, so the reader must report the file's real row
/// count for an empty projection. If it ever reports zero, `probe_file`'s row-group invariant
/// fires rather than tombstoning the wrong rows.
#[tokio::test]
async fn a_key_made_only_of_partition_columns_still_locates_rows() {
    let dir = TempDir::new().unwrap();
    // Partition "a" holds two rows, so a located ordinal has to distinguish them.
    let table = partitioned_fixture_table(&dir, &[(1, "a"), (2, "a"), (3, "b")]).await;

    let encoder = KeyEncoder::new(&partition_only_key_relation(), &arrow_schema()).unwrap();
    let key_columns: Vec<ArrayRef> = vec![Arc::new(StringArray::from(vec!["a"]))];
    let mut chunk = LookupChunk::new(usize::MAX);
    chunk
        .extend(&encoder.encode_columns(&key_columns).unwrap())
        .unwrap();
    chunk.sort();

    let candidates = partitioned_candidates(&table, &encoder);
    let mut tombstones = Tombstones::new();
    let metrics = locate(
        &chunk,
        &candidates,
        &table,
        &encoder,
        4,
        Pruning::none(),
        &mut tombstones,
    )
    .await
    .unwrap();

    assert_eq!(
        metrics.rows_located, 2,
        "both rows of partition \"a\" share the key and must be located: {metrics:?}"
    );
    assert_eq!(metrics.keys_not_found, 0, "{metrics:?}");
    assert_eq!(
        metrics.row_groups_scanned, 2,
        "pruning is off, so both partitions must be read rather than skipped: {metrics:?}"
    );

    let located: Vec<u64> = tombstones
        .ordinals_for(
            &candidates
                .iter()
                .find(|c| c.path.starts_with("payload=a/"))
                .unwrap()
                .path,
        )
        .expect("partition \"a\" contributed no rows")
        .iter()
        .collect();
    assert_eq!(located, vec![0, 1], "ordinals must span the whole file");
}

/// A partition value needing percent-encoding must round-trip. delta-rs decodes the path when
/// it reads the log, so encoding it again names an object that does not exist.
#[tokio::test]
async fn a_partition_value_needing_escaping_round_trips() {
    let dir = TempDir::new().unwrap();
    let table =
        partitioned_fixture_table(&dir, &[(1, "p%q"), (2, "p%q"), (3, "x y"), (4, "x y")]).await;

    let table = tombstone_ids(table, &[2, 3]).await;

    assert_eq!(live_ids(&table).await, vec![1, 4]);
}

// ── protocol-level assumptions ────────────────────────────────────────────────

/// Enabling the table property must produce a protocol delta-rs will write to. If this fails,
/// merge mode cannot commit at all: every flush ends in a `remove`/`add` pair.
#[tokio::test]
async fn delta_rs_writes_to_a_deletion_vector_table() {
    let dir = TempDir::new().unwrap();
    let table = fixture_table(&dir, &[1, 2, 3], true).await;
    let snapshot = table.snapshot().unwrap();

    let protocol = snapshot.protocol();
    assert_eq!(protocol.min_reader_version(), 3);
    assert_eq!(protocol.min_writer_version(), 7);
    assert!(
        PROTOCOL.can_write_to(snapshot).is_ok(),
        "delta-rs refuses to write to a deletion vector table"
    );
}

/// A `remove` and an `add` for the same path must install a new vector that a reader honours.
/// The whole mechanism in one test.
#[tokio::test]
async fn tombstoned_rows_disappear_from_the_reader() {
    let dir = TempDir::new().unwrap();
    let table = fixture_table(&dir, &[1, 2, 3, 4, 5], true).await;
    let original_files = candidates(&table);

    let table = tombstone_ids(table, &[2, 4]).await;

    assert_eq!(live_ids(&table).await, vec![1, 3, 5]);
    assert_eq!(
        candidates(&table)
            .iter()
            .map(|c| c.path.clone())
            .collect::<Vec<_>>(),
        original_files
            .iter()
            .map(|c| c.path.clone())
            .collect::<Vec<_>>(),
        "the data file must stay live, only its deletion vector changes"
    );
    assert_eq!(deletion_vector_files(&dir).len(), 1);
}

/// A second flush must union its rows into the first flush's vector, not replace it.
/// Replacing it would resurrect every row tombstoned earlier.
#[tokio::test]
async fn later_flushes_keep_earlier_tombstones() {
    let dir = TempDir::new().unwrap();
    let table = fixture_table(&dir, &[1, 2, 3, 4, 5], true).await;

    let table = tombstone_ids(table, &[2]).await;
    let table = tombstone_ids(table, &[4]).await;

    assert_eq!(live_ids(&table).await, vec![1, 3, 5]);
}

/// Tombstoning a row that is already tombstoned must write nothing.
///
/// A retried flush re-locates rows an earlier attempt already covered. Re-committing that
/// vector unchanged would bump the table version and leave a second vector object behind for
/// VACUUM, once per attempt.
#[tokio::test]
async fn tombstoning_an_already_tombstoned_row_is_a_no_op() {
    let dir = TempDir::new().unwrap();
    let table = fixture_table(&dir, &[1, 2, 3], true).await;
    let table = tombstone_ids(table, &[2]).await;
    let version = table.version();

    let table = tombstone_ids(table, &[2]).await;

    assert_eq!(table.version(), version, "the table gained an empty commit");
    assert_eq!(deletion_vector_files(&dir).len(), 1);
    assert_eq!(live_ids(&table).await, vec![1, 3]);
}

/// Vacuum in its default mode must not disturb a live deletion vector.
///
/// `Lite` is what a `deltalake` caller gets without asking, so it is what a scheduled cleanup
/// most likely runs. It deletes only paths named by expired `remove` actions, so a vector is
/// safe by construction -- but that is a claim about someone else's code, hence the test.
///
/// `Full` mode is covered in delta-rs, where the behaviour lives, by
/// `test_vacuum_full_keeps_a_live_deletion_vector`.
#[tokio::test]
async fn vacuum_preserves_live_deletion_vectors() {
    let dir = TempDir::new().unwrap();
    let table = fixture_table(&dir, &[1, 2, 3, 4, 5], true).await;
    let table = tombstone_ids(table, &[2, 4]).await;
    let vectors_before = deletion_vector_files(&dir);
    assert_eq!(vectors_before.len(), 1);

    let (table, metrics) = vacuum_everything(table, VacuumMode::Lite).await;

    assert_eq!(
        deletion_vector_files(&dir),
        vectors_before,
        "vacuum deleted a live deletion vector file: {:?}",
        metrics.files_deleted
    );
    assert_eq!(
        live_ids(&table).await,
        vec![1, 3, 5],
        "vacuum resurrected tombstoned rows"
    );
}

/// A file whose every row is tombstoned must leave the table rather than linger behind a full
/// vector, which is what keeps a delete-heavy pipeline from accumulating empty files.
#[tokio::test]
async fn fully_tombstoned_files_are_dropped() {
    let dir = TempDir::new().unwrap();
    let table = fixture_table(&dir, &[1, 2, 3], true).await;

    let table = tombstone_ids(table, &[1, 2, 3]).await;

    assert!(live_ids(&table).await.is_empty());
    assert!(
        candidates(&table).is_empty(),
        "the emptied data file must be removed from the table"
    );
    assert!(
        deletion_vector_files(&dir).is_empty(),
        "no vector object should be written when every file is dropped whole"
    );
}

/// Vectors must land at the table root, under the name a reader derives from the descriptor.
///
/// Asserted directly, because the vacuum test would pass on a non-standard layout too: vacuum
/// skips what it does not recognize, and skipping is what leaves objects to accumulate.
#[tokio::test]
async fn vectors_land_at_the_table_root() {
    let dir = TempDir::new().unwrap();
    let table = fixture_table(&dir, &[1, 2, 3], true).await;
    let table = tombstone_ids(table, &[2]).await;

    let vectors = deletion_vector_files(&dir);
    assert_eq!(vectors.len(), 1);
    assert!(
        vectors[0].starts_with("deletion_vector_") && vectors[0].ends_with(".bin"),
        "unexpected vector object name: {vectors:?}"
    );
    assert!(
        dir.path().join(&vectors[0]).is_file(),
        "the vector must sit at the table root, not in a subdirectory: {vectors:?}"
    );
    assert_eq!(live_ids(&table).await, vec![1, 3]);
}

/// How `actual` differs from `expected`, or `None` when they match.
///
/// The corruption this guards against keeps the row count right, so a failure needs to name
/// the rows, and there are too many to print: counts each way plus a few examples.
fn difference(actual: &[i64], expected: &[i64]) -> Option<String> {
    let actual: BTreeSet<i64> = actual.iter().copied().collect();
    let expected: BTreeSet<i64> = expected.iter().copied().collect();
    let extra: Vec<i64> = actual.difference(&expected).copied().collect();
    let missing: Vec<i64> = expected.difference(&actual).copied().collect();
    if extra.is_empty() && missing.is_empty() {
        return None;
    }
    Some(format!(
        "{} tombstoned row(s) came back (e.g. {:?}) and {} live row(s) vanished (e.g. {:?}); \
         the count is {} against {} expected",
        extra.len(),
        &extra[..extra.len().min(5)],
        missing.len(),
        &missing[..missing.len().min(5)],
        actual.len(),
        expected.len(),
    ))
}

/// A scan must honour a deletion vector even when the planner splits the file.
///
/// delta-rs consumes a file's keep mask in physical row order, which holds only while the
/// file arrives whole and in order. Split the file across scan partitions and the pieces are
/// merged back in completion order, so the mask lands at the wrong offsets: the row *count*
/// stays right and the *rows* are wrong, tombstoned rows coming back and live ones vanishing.
/// On a merge-mode table that means two live rows for one key.
///
/// Merge mode's own probe reads Parquet directly and never takes this path, but `OPTIMIZE`
/// does, and so does every reader of the table.
///
/// The fixture clears two bars that every other fixture here misses, and both are why the
/// small ones never caught this:
///
/// | Bar | Why |
/// |---|---|
/// | The file exceeds 10 MiB | Below `repartition_file_min_size` DataFusion leaves a file in one partition, and one partition cannot misorder anything |
/// | The tombstoned rows are contiguous | Every Nth row is invariant under a misordered mask: permuting the segments still masks every Nth row |
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_split_scan_honours_the_deletion_vector() {
    const ROWS: i64 = 120_000;
    const ROWS_PER_GROUP: usize = 5_000;
    const PAYLOAD_BYTES: usize = 96;
    const BLOCK: i64 = 2_000;
    const MIN_SPLIT_SIZE: i64 = 10 << 20;

    let dir = TempDir::new().unwrap();
    let table = fixture_table(&dir, &[], true).await;
    let ids: Vec<i64> = (0..ROWS).collect();
    let mut table = append_ids_in_row_groups(table, &ids, ROWS_PER_GROUP, PAYLOAD_BYTES).await;

    let file_size = table
        .snapshot()
        .unwrap()
        .log_data()
        .into_iter()
        .map(|f| f.size())
        .next()
        .unwrap();
    assert!(
        file_size > MIN_SPLIT_SIZE,
        "the fixture file is {file_size} bytes, too small for the planner to split"
    );

    let half = ROWS / 2;
    let path = candidates(&table).first().unwrap().path.clone();
    let mut tombstones = Tombstones::new();
    for ordinal in (0..BLOCK as u64).chain(half as u64..(half + BLOCK) as u64) {
        tombstones.insert(&path, ordinal);
    }
    let dv = write_deletion_vectors(&tombstones, &table).await.unwrap();
    commit(&mut table, dv.actions).await;

    // A second file holding the new versions, as a merge flush leaves behind. OPTIMIZE
    // drops a single-file bin, so without it there is nothing for it to rewrite.
    let new_versions: Vec<i64> = (ROWS..ROWS + 100).collect();
    let table = append_ids(table, &new_versions).await;

    let tombstoned = |id: &i64| *id < BLOCK || (*id >= half && *id < half + BLOCK);
    let mut expected: Vec<i64> = ids.into_iter().filter(|id| !tombstoned(id)).collect();
    expected.extend(new_versions);

    if let Some(diff) = difference(&split_scan_ids(&table).await, &expected) {
        panic!("a split scan returned the wrong rows: {diff}");
    }

    // The same mask, now read by OPTIMIZE: what it keeps is what the table becomes.
    let (table, metrics) = table.optimize().await.unwrap();
    assert!(
        metrics.num_files_removed > 0,
        "the fixture gave OPTIMIZE nothing to rewrite"
    );
    if let Some(diff) = difference(&live_ids(&table).await, &expected) {
        panic!("OPTIMIZE rewrote the file with the wrong rows: {diff}");
    }
}

/// A file OPTIMIZE replaced under the lookup is transient: redoing the flush is the fix.
///
/// Classifying it as deterministic would abandon the batch on a routine collision with
/// table maintenance, which is the one failure merge mode has to survive.
#[tokio::test]
async fn a_file_maintenance_replaced_is_transient() {
    use crate::integrated::delta_table::WriteError;

    let dir = TempDir::new().unwrap();
    let table = fixture_table(&dir, &[1, 2, 3], true).await;

    let mut tombstones = Tombstones::new();
    tombstones.insert("part-00000-does-not-exist.parquet", 0);

    let error = match write_deletion_vectors(&tombstones, &table).await {
        Err(e) => e,
        Ok(_) => panic!("tombstoning a file the snapshot no longer holds must fail"),
    };
    assert!(matches!(error, WriteError::Transient(_)), "{error:?}");
    assert!(error.to_string().contains("must be redone"), "{error}");
}

/// An object store that fails the first few requests of a kind, then behaves.
///
/// The connector's own retries are what keep a flush alive across an object store having a
/// bad minute, and nothing exercised them end to end: the unit tests drive the retry helper
/// directly, and the probe test only shows a failure that never recovers.
#[derive(Debug)]
struct FlakyStore {
    inner: Arc<dyn deltalake::ObjectStore>,
    /// Remaining reads of a data file to fail.
    fail_reads: AtomicUsize,
    /// Remaining writes of a deletion vector object to fail.
    fail_writes: AtomicUsize,
    injected: AtomicUsize,
}

impl FlakyStore {
    /// Wraps the whole local filesystem: `with_storage_backend` wants a store rooted at "/"
    /// and resolves the table's own location against it.
    fn new(fail_reads: usize, fail_writes: usize) -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(object_store::local::LocalFileSystem::new()),
            fail_reads: AtomicUsize::new(fail_reads),
            fail_writes: AtomicUsize::new(fail_writes),
            injected: AtomicUsize::new(0),
        })
    }

    /// Whether this request is one of the remaining ones to fail.
    fn should_fail(&self, budget: &AtomicUsize) -> bool {
        let claimed = budget
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |left| {
                left.checked_sub(1)
            })
            .is_ok();
        if claimed {
            self.injected.fetch_add(1, Ordering::Relaxed);
        }
        claimed
    }

    fn outage() -> object_store::Error {
        object_store::Error::Generic {
            store: "flaky",
            source: "injected outage".into(),
        }
    }
}

impl std::fmt::Display for FlakyStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FlakyStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl deltalake::ObjectStore for FlakyStore {
    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        // Only the deletion vector object: failing the log write would fail the commit,
        // which is a different path with its own retry.
        if location.as_ref().ends_with(".bin") && self.should_fail(&self.fail_writes) {
            return Err(Self::outage());
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        if location.as_ref().ends_with(".parquet") && self.should_fail(&self.fail_reads) {
            return Err(Self::outage());
        }
        self.inner.get_opts(location, options).await
    }

    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<
            'static,
            object_store::Result<object_store::path::Path>,
        >,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>> {
        self.inner.delete_stream(locations)
    }

    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// Open `dir`'s table through `store`, so the merge paths read and write through it.
async fn table_over(store: Arc<FlakyStore>, dir: &TempDir) -> DeltaTable {
    let url = deltalake::table::builder::ensure_table_uri(dir.path().to_str().unwrap()).unwrap();
    deltalake::DeltaTableBuilder::from_url(url.clone())
        .unwrap()
        .with_storage_backend(store, url)
        .load()
        .await
        .unwrap()
}

/// A flush survives an object store that drops requests, and lands the same table.
///
/// Both halves are covered: the probe's read of a data file and the write of the packed
/// deletion vector object. Each is retried where it happens, so a flush pays for one dropped
/// request with one re-request rather than by redoing the whole lookup.
#[tokio::test]
async fn a_flush_rides_out_a_flaky_object_store() {
    let dir = TempDir::new().unwrap();
    drop(fixture_table(&dir, &[1, 2, 3, 4], true).await);

    // Within `IO_ATTEMPTS`, so the retries are expected to absorb them.
    let store = FlakyStore::new(2, 2);
    let table = table_over(store.clone(), &dir).await;
    let table = tombstone_ids(table, &[2, 3]).await;

    assert_eq!(live_ids(&table).await, vec![1, 4]);
    assert_eq!(
        store.injected.load(Ordering::Relaxed),
        4,
        "the store did not actually drop the requests the test injects"
    );
}
