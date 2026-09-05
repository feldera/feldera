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
use deltalake::datafusion::prelude::SessionContext;
use deltalake::kernel::transaction::{CommitBuilder, CommitProperties, PROTOCOL, TableReference};
use deltalake::kernel::{Action, DataType as DeltaDataType, PrimitiveType, StructField};
use deltalake::operations::create::CreateBuilder;
use deltalake::operations::vacuum::{VacuumMetrics, VacuumMode};
use deltalake::protocol::{DeltaOperation, SaveMode};
use deltalake::{DeltaTable, TableProperty};
use feldera_types::program_schema::{ColumnType, Field, Relation, SqlIdentifier, SqlType};
use tempfile::TempDir;

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

/// Every data file in the current snapshot.
pub(super) fn candidates(table: &DeltaTable) -> Vec<Candidate> {
    table
        .snapshot()
        .unwrap()
        .log_data()
        .into_iter()
        .map(|f| Candidate::from_log(&f, Default::default(), true))
        .collect()
}

/// Ids a reader sees, in ascending order. Through DataFusion rather than reading the parquet
/// files directly, since a test that read them itself would pass no matter what we wrote.
pub(super) async fn live_ids(table: &DeltaTable) -> Vec<i64> {
    let ctx = SessionContext::new();
    ctx.register_table("t", table.table_provider().await.unwrap())
        .unwrap();
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
    commit(&mut table, dv.actions).await;
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

    // Each candidate carries the partition value of its own file.
    let candidates: Vec<Candidate> = table
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
            Candidate::from_log(&file, partition_keys, true)
        })
        .collect();

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
