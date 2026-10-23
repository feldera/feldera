//! The Delta Lake output connector on output batches: the rows that it writes in cdc mode, and
//! the table that it keeps equal to the view in merge mode, through updates, deletes, retries,
//! restarts, compaction, and key columns of every type; with the commits, metrics, and
//! failures along the way.

use std::collections::BTreeMap;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use arrow::array::AsArray;
use arrow::datatypes::{Float64Type, Int64Type};
use arrow::json::ReaderBuilder;
use async_trait::async_trait;
use deltalake::logstore::ObjectStoreRef;
use deltalake::logstore::object_store::path::Path as ObjectPath;
use deltalake::logstore::object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as ObjectStoreResult,
    UploadPart,
};
use feldera_adapterlib::test::{ConnectorTestEngine, Updates, ZSet};
use feldera_adapterlib::transport::OutputBatchType;
use feldera_storage::tokio::TOKIO;
use feldera_types::program_schema::{ColumnType, Field, Relation, SqlIdentifier};
use feldera_types::transport::delta_table::{
    DeltaTableUpdateMode, DeltaTableWriteMode, DeltaTableWriterConfig, DeltaVariantEncoding,
};
use futures::stream::BoxStream;
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

use super::DeltaTableWriter;
use crate::catalog::SerBatchReader;
use crate::controller::EndpointId;
use crate::format::Encoder;
use crate::test::{delta, published_schema, read, relations};

/// The index of [`value_relation`] on `bigint`.
pub fn key_relation() -> Relation {
    key_relation_on("bigint")
}

/// The view that each case writes: a column of every type that a Delta table holds.
pub fn value_relation() -> Relation {
    let mut relation = Relation::new(
        "delta_output".into(),
        vec![
            Field::new("bigint".into(), ColumnType::bigint(false)),
            Field::new("binary".into(), ColumnType::varbinary(false)),
            Field::new("boolean".into(), ColumnType::boolean(false)),
            Field::new("date".into(), ColumnType::date(false)),
            Field::new("decimal_10_3".into(), ColumnType::decimal(10, 3, false)),
            Field::new("double".into(), ColumnType::double(false)),
            Field::new("float".into(), ColumnType::real(false)),
            Field::new("int".into(), ColumnType::int(false)),
            Field::new("smallint".into(), ColumnType::smallint(false)),
            Field::new("string".into(), ColumnType::varchar(false)),
            Field::new("unused".into(), ColumnType::varchar(true)).with_unused(true),
            Field::new("timestamp_ntz".into(), ColumnType::timestamp(false)),
            Field::new("timestamp_tz".into(), ColumnType::timestamp_tz(false)),
            Field::new("tinyint".into(), ColumnType::tinyint(false)),
            Field::new(
                "string_array".into(),
                ColumnType::array(false, ColumnType::varchar(false)),
            ),
            Field::new(
                "struct1".into(),
                ColumnType::structure(false, &relations::basic_columns()),
            ),
            Field::new(
                "struct_array".into(),
                ColumnType::array(
                    false,
                    ColumnType::structure(false, &relations::basic_columns()),
                ),
            ),
            Field::new(
                "string_string_map".into(),
                ColumnType::map(
                    false,
                    ColumnType::varchar(false),
                    ColumnType::varchar(false),
                ),
            ),
            Field::new(
                "string_struct_map".into(),
                ColumnType::map(
                    false,
                    ColumnType::varchar(false),
                    ColumnType::structure(false, &relations::basic_columns()),
                ),
            ),
            Field::new("variant".into(), ColumnType::variant(false)),
            Field::new("uuid".into(), ColumnType::uuid(false)),
        ],
        false,
        BTreeMap::new(),
    );
    relation.materialized = true;
    relation
}

/// The columns that the connector adds to each row in cdc mode.
fn cdc_relation() -> Relation {
    Relation::new(
        "delta_output_cdc".into(),
        vec![
            Field::new("__feldera_op".into(), ColumnType::varchar(false)),
            Field::new("__feldera_ts".into(), ColumnType::bigint(false)),
        ],
        false,
        BTreeMap::new(),
    )
}

/// A row that the connector wrote in cdc mode.
struct OutputRecord {
    /// The row of [`value_relation`].
    data: Updates,
    __feldera_op: String,
    __feldera_ts: i64,
}

impl OutputRecord {
    fn to_data_record(&self) -> Updates {
        self.data.clone()
    }
}

/// Returns the `bigint` column of `rows`, in order.
fn bigints(rows: &Updates) -> Vec<i64> {
    let index = rows.rows().schema().index_of("bigint").unwrap();
    rows.rows()
        .column(index)
        .as_primitive::<Int64Type>()
        .values()
        .to_vec()
}

/// Returns the `bigint` column of `row`.
fn bigint(row: &Value) -> i64 {
    row["bigint"].as_i64().unwrap()
}

/// Returns `rows`, rows of [`value_relation`] in the default JSON flavor, each inserted once.
fn values_updates(rows: &[Value]) -> Updates {
    weighted_updates(rows.iter().map(|row| (row.clone(), 1)).collect())
}

/// Returns `rows`, rows of [`value_relation`] in the default JSON flavor, each with its weight.
fn weighted_updates(rows: Vec<(Value, i64)>) -> Updates {
    let relation = value_relation();
    let rows: Vec<(i64, String)> = rows
        .into_iter()
        .map(|(row, weight)| {
            (
                weight,
                read::arrow_form(&relation, row).unwrap().to_string(),
            )
        })
        .collect();
    let rows: Vec<(i64, &str)> = rows
        .iter()
        .map(|(weight, row)| (*weight, row.as_str()))
        .collect();
    Updates::from_json(published_schema(&relation), &rows).unwrap()
}

/// Returns the consolidated rows of `rows`.
fn values_zset(rows: &[Value]) -> ZSet {
    values_updates(rows).zset().unwrap()
}

/// Returns the consolidated rows of `updates`.
fn updates_zset(updates: impl IntoIterator<Item = Updates>) -> ZSet {
    let mut zset = ZSet::empty(published_schema(&value_relation())).unwrap();
    for updates in updates {
        zset.add(&updates.zset().unwrap()).unwrap();
    }
    zset
}

pub(super) fn make_endpoint(threads: usize, table_uri: &str, indexed: bool) -> DeltaTableWriter {
    make_endpoint_ex(
        threads,
        table_uri,
        indexed,
        DeltaTableWriteMode::Truncate,
        false,
    )
}

fn make_endpoint_ex(
    threads: usize,
    table_uri: &str,
    indexed: bool,
    mode: DeltaTableWriteMode,
    continue_previous_state: bool,
) -> DeltaTableWriter {
    make_endpoint_with_retries(
        threads,
        table_uri,
        indexed,
        mode,
        continue_previous_state,
        Some(0),
    )
}

fn make_endpoint_with_retries(
    threads: usize,
    table_uri: &str,
    indexed: bool,
    mode: DeltaTableWriteMode,
    continue_previous_state: bool,
    max_retries: Option<u32>,
) -> DeltaTableWriter {
    try_make_endpoint(
        threads,
        table_uri,
        indexed,
        mode,
        continue_previous_state,
        max_retries,
    )
    .expect("failed to create endpoint")
}

pub(super) fn try_make_endpoint(
    threads: usize,
    table_uri: &str,
    indexed: bool,
    mode: DeltaTableWriteMode,
    continue_previous_state: bool,
    max_retries: Option<u32>,
) -> Result<DeltaTableWriter, crate::ControllerError> {
    let key_schema = if indexed { Some(key_relation()) } else { None };
    DeltaTableWriter::new(
        EndpointId::default(),
        "test_endpoint",
        &DeltaTableWriterConfig {
            variant_encoding: Default::default(),
            uri: table_uri.to_string(),
            mode,
            max_retries,
            threads: Some(threads),
            auto_compact: false,

            object_store_config: Default::default(),
            checkpoint_interval: None,
            log_retention_duration: None,
            update_mode: Default::default(),
            lookup_chunk_size: feldera_types::size::ConfigSize::from_mib(1),
            max_concurrent_probes: 4,
            enable_expired_log_cleanup: None,
        },
        &key_schema,
        &value_relation(),
        Weak::new(),
        CancellationToken::new(),
        continue_previous_state,
        indexed,
        true,
    )
}

pub(super) fn make_merge_endpoint(table_uri: &str, mode: DeltaTableWriteMode) -> DeltaTableWriter {
    make_merge_endpoint_ex(table_uri, mode, 1 << 20, 0, key_relation())
}

fn make_merge_endpoint_ex(
    table_uri: &str,
    mode: DeltaTableWriteMode,
    lookup_chunk_bytes: usize,
    max_retries: u32,
    key_schema: Relation,
) -> DeltaTableWriter {
    DeltaTableWriter::new(
        EndpointId::default(),
        "test_merge_endpoint",
        &DeltaTableWriterConfig {
            uri: table_uri.to_string(),
            mode,
            variant_encoding: DeltaVariantEncoding::default(),
            update_mode: DeltaTableUpdateMode::Merge,
            max_retries: Some(max_retries),
            threads: Some(1),
            auto_compact: false,

            object_store_config: Default::default(),
            checkpoint_interval: None,
            log_retention_duration: None,
            lookup_chunk_size: feldera_types::size::ConfigSize::from_bytes(
                lookup_chunk_bytes as u64,
            ),
            max_concurrent_probes: 4,
            enable_expired_log_cleanup: None,
        },
        &Some(key_schema),
        &value_relation(),
        Weak::new(),
        CancellationToken::new(),
        false,
        true,
        true,
    )
    .expect("failed to create merge endpoint")
}

/// Rows the table currently holds, as any reader would see them.
///
/// Through DataFusion, so deletion vectors are applied. Reading the parquet files directly
/// as the cdc tests do would show every superseded row still present.
fn read_merge_output(table_uri: &str) -> Updates {
    delta::table_rows(&value_relation(), table_uri).unwrap()
}

/// Assert the table holds exactly the model's rows.
fn assert_matches_model(table_uri: &str, model: &BTreeMap<i64, Value>, step: &str) {
    let actual = read_merge_output(table_uri);
    let expected: Vec<Value> = model.values().cloned().collect();

    assert_eq!(
        actual.len(),
        expected.len(),
        "after {step}: table has {} row(s), the view has {}",
        actual.len(),
        expected.len()
    );
    assert_eq!(
        actual.zset().unwrap(),
        values_zset(&expected),
        "after {step}: table diverged from the view"
    );
}

/// A merge flush bigger than `TARGET_FILE_SIZE` must land in more than one Parquet file.
///
/// Merge mode builds its own writer config, so it needs the target size cdc mode sets:
/// without one, a flush writes a single object, which an object store rejects once the
/// multipart upload passes 10000 parts.
pub fn merge_batch_larger_than_target_file_size_rolls_over(engine: &dyn ConnectorTestEngine) {
    const PAYLOAD_LEN: usize = 8 * 1024;
    let dir = TempDir::new().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    let target = super::TARGET_FILE_SIZE.get() as usize;
    let rows = (target * 5 / 4).div_ceil(PAYLOAD_LEN);
    let records: Vec<Value> = (0..rows)
        .map(|i| {
            let mut row = make_record(i);
            row["string"] = json!(incompressible_string(i as u64, PAYLOAD_LEN));
            row
        })
        .collect();

    let mut endpoint = make_merge_endpoint(&uri, DeltaTableWriteMode::Append);
    encode_batch(&mut endpoint, &build_insert_batch(engine, &records));

    let files = delta::parquet_files(Path::new(&uri));
    assert!(
        files.len() > 1,
        "{rows} rows of {PAYLOAD_LEN} bytes each exceed the {target} byte target \
         but landed in {} file(s)",
        files.len()
    );
    assert_eq!(read_merge_output(&uri).len(), rows);
}

/// Merge mode must leave the table equal to the view after every commit.
///
/// Walks every transition a key can make -- absent to present, updated, deleted, deleted
/// again, reinserted -- then mixes them in one batch, where a per-key branch that works in
/// isolation tends to break.
pub fn merge_tracks_the_view(engine: &dyn ConnectorTestEngine) {
    let dir = TempDir::new().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut endpoint = make_merge_endpoint(uri, DeltaTableWriteMode::Append);
    let mut model: BTreeMap<i64, Value> = BTreeMap::new();

    let records = make_records(12);
    let updated: Vec<Value> = (100..112).map(make_record).collect();

    // An empty batch must not change the table.
    encode_batch(&mut endpoint, &build_insert_batch(engine, &[]));
    assert_matches_model(uri, &model, "an empty batch");

    // Inserts.
    encode_batch(&mut endpoint, &build_insert_batch(engine, &records[0..6]));
    for r in &records[0..6] {
        model.insert(bigint(r), r.clone());
    }
    assert_matches_model(uri, &model, "six inserts");

    // Updates: the old row is superseded, not duplicated. `updated[i]` carries the same
    // key as `records[i]` only if the key is the record index, so rebuild it here.
    let updates: Vec<(Value, Value)> = (0..3)
        .map(|i| {
            let mut new = updated[i].clone();
            new["bigint"] = records[i]["bigint"].clone();
            (records[i].clone(), new)
        })
        .collect();
    encode_batch(&mut endpoint, &build_upsert_batch(engine, &updates));
    for (_, new) in &updates {
        model.insert(bigint(new), new.clone());
    }
    assert_matches_model(uri, &model, "three updates");

    // Deletes.
    encode_batch(&mut endpoint, &build_delete_batch(engine, &records[4..6]));
    for r in &records[4..6] {
        model.remove(&bigint(r));
    }
    assert_matches_model(uri, &model, "two deletes");

    // Deleting a key the table does not hold is a no-op, not an error.
    encode_batch(&mut endpoint, &build_delete_batch(engine, &records[8..10]));
    assert_matches_model(uri, &model, "deleting absent keys");

    // Reinserting a deleted key must leave one live row, not two.
    encode_batch(&mut endpoint, &build_insert_batch(engine, &records[4..5]));
    model.insert(bigint(&records[4]), records[4].clone());
    assert_matches_model(uri, &model, "reinserting a deleted key");

    // One batch mixing all three operations.
    let mut mixed = Vec::new();
    let mut new_third = updated[3].clone();
    new_third["bigint"] = records[3]["bigint"].clone();
    mixed.push((records[3].clone(), new_third.clone()));
    encode_batch(&mut endpoint, &build_upsert_batch(engine, &mixed));
    model.insert(bigint(&new_third), new_third);
    encode_batch(&mut endpoint, &build_insert_batch(engine, &records[6..8]));
    for r in &records[6..8] {
        model.insert(bigint(r), r.clone());
    }
    assert_matches_model(uri, &model, "a mixed batch");

    // Everything deleted: no live rows, and the table should hold no data files either.
    let live: Vec<Value> = model.values().cloned().collect();
    encode_batch(&mut endpoint, &build_delete_batch(engine, &live));
    model.clear();
    assert_matches_model(uri, &model, "deleting every row");
}

/// A connector that opens a table it did not fill must still supersede what is in it.
///
/// The insert shortcut is only sound when the table started empty. Here a second connector
/// opens a populated table and is fed an *insert* for a key already in it; the old row
/// must be tombstoned anyway. This is also what a pipeline restart looks like.
pub fn merge_supersedes_a_row_left_by_an_earlier_run(engine: &dyn ConnectorTestEngine) {
    let dir = TempDir::new().unwrap();
    let uri = dir.path().to_str().unwrap();
    let records = make_records(4);

    {
        let mut first = make_merge_endpoint(uri, DeltaTableWriteMode::Append);
        encode_batch(&mut first, &build_insert_batch(engine, &records));
    }

    // A second connector on the same table sees data it did not write.
    let mut second = make_merge_endpoint(uri, DeltaTableWriteMode::Append);
    let mut replacement = make_record(500);
    replacement["bigint"] = records[1]["bigint"].clone();
    encode_batch(
        &mut second,
        &build_insert_batch(engine, &[replacement.clone()]),
    );

    let mut model: BTreeMap<i64, Value> = records.iter().map(|r| (bigint(r), r.clone())).collect();
    model.insert(bigint(&replacement), replacement);
    assert_matches_model(uri, &model, "an insert onto a row from an earlier run");
}

/// A key set larger than `lookup_chunk_size` is split into several lookup passes, and
/// the result must not depend on how many.
///
/// The budget below is small enough that a few keys cross it, so the flush runs the
/// multi-pass path that a default-sized budget never reaches.
pub fn merge_is_unaffected_by_chunking(engine: &dyn ConnectorTestEngine) {
    let dir = TempDir::new().unwrap();
    let uri = dir.path().to_str().unwrap();
    let records = make_records(40);

    let mut endpoint =
        make_merge_endpoint_ex(uri, DeltaTableWriteMode::Append, 1, 0, key_relation());
    encode_batch(&mut endpoint, &build_insert_batch(engine, &records));

    // Update half the rows in one batch, so the removal side spans many chunks.
    let updates: Vec<(Value, Value)> = (0..20)
        .map(|i| {
            let mut new = make_record(1000 + i);
            new["bigint"] = records[i]["bigint"].clone();
            (records[i].clone(), new)
        })
        .collect();
    encode_batch(&mut endpoint, &build_upsert_batch(engine, &updates));

    let mut model: BTreeMap<i64, Value> = records.iter().map(|r| (bigint(r), r.clone())).collect();
    for (_, new) in &updates {
        model.insert(bigint(new), new.clone());
    }
    assert_matches_model(uri, &model, "20 updates across many lookup chunks");
}

/// Deletion vector objects at the table root.
fn vector_files(table_uri: &str) -> Vec<String> {
    std::fs::read_dir(table_uri)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|name| name.starts_with("deletion_vector_"))
        .collect()
}

/// Value of one of the connector's exported metrics.
fn merge_metric(endpoint: &DeltaTableWriter, name: &str) -> f64 {
    use feldera_adapterlib::metrics::ConnectorMetrics;

    endpoint
        .merge_metrics
        .as_ref()
        .expect("not a merge-mode endpoint")
        .metrics()
        .into_iter()
        .find(|(n, ..)| *n == name)
        .unwrap_or_else(|| panic!("no metric named {name}"))
        .3
}

/// An insert-only flush onto a table that started empty must not read the table at all.
/// That shortcut is what makes a bootstrap affordable.
pub fn merge_skips_the_lookup_for_inserts_into_an_empty_table(engine: &dyn ConnectorTestEngine) {
    let dir = TempDir::new().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut endpoint = make_merge_endpoint(uri, DeltaTableWriteMode::Append);

    encode_batch(
        &mut endpoint,
        &build_insert_batch(engine, &make_records(50)),
    );

    assert_eq!(
        merge_metric(
            &endpoint,
            "output_connector_delta_merge_rows_appended_total"
        ),
        50.0
    );
    assert_eq!(
        merge_metric(&endpoint, "output_connector_delta_merge_keys_probed_total"),
        0.0,
        "an insert-only flush onto an empty table looked rows up anyway"
    );
    assert_eq!(
        merge_metric(
            &endpoint,
            "output_connector_delta_merge_probe_files_scanned_total"
        ),
        0.0
    );
}

/// The lookup must skip files whose key range cannot hold any changed key, or the cost of
/// a flush grows with the table rather than with the change.
///
/// Five flushes leave five files with disjoint key ranges; a sixth updating two keys from
/// one of them must open that file and skip the other four.
pub fn merge_prunes_files_outside_the_changed_key_range(engine: &dyn ConnectorTestEngine) {
    let dir = TempDir::new().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut endpoint = make_merge_endpoint(uri, DeltaTableWriteMode::Append);

    // Disjoint key ranges, one file each. `make_record(i)` keys on `i`.
    for range in 0..5 {
        let batch: Vec<Value> = (range * 100..range * 100 + 20).map(make_record).collect();
        encode_batch(&mut endpoint, &build_insert_batch(engine, &batch));
    }
    let scanned_before = merge_metric(
        &endpoint,
        "output_connector_delta_merge_probe_files_scanned_total",
    );

    // Update two keys that live in the third file only.
    let updates: Vec<(Value, Value)> = [205i64, 206]
        .iter()
        .map(|key| {
            let old = make_record(*key as usize);
            let mut new = make_record(9000 + *key as usize);
            new["bigint"] = json!(*key);
            (old, new)
        })
        .collect();
    encode_batch(&mut endpoint, &build_upsert_batch(engine, &updates));

    let scanned = merge_metric(
        &endpoint,
        "output_connector_delta_merge_probe_files_scanned_total",
    ) - scanned_before;
    let pruned = merge_metric(
        &endpoint,
        "output_connector_delta_merge_probe_files_pruned_total",
    );
    assert_eq!(scanned, 1.0, "opened {scanned} files, expected 1");
    assert_eq!(pruned, 4.0, "pruned {pruned} files, expected 4");

    // And the answer is still right, which is what makes the skipping meaningful.
    let mut model: BTreeMap<i64, Value> = BTreeMap::new();
    for range in 0..5 {
        for i in range * 100..range * 100 + 20 {
            let record = make_record(i);
            model.insert(bigint(&record), record);
        }
    }
    for (_, new) in &updates {
        model.insert(bigint(new), new.clone());
    }
    assert_matches_model(uri, &model, "an update confined to one file");
}

/// A flush must converge after a compaction replaced the files it was addressing.
///
/// The one concurrency claim merge mode makes to administrators: OPTIMIZE is safe to run
/// against a table a pipeline is writing. The commit conflicts and the flush redoes the
/// lookup against the new files. Without the reload it would conflict for ever, so this
/// fails rather than merely running slow.
pub fn merge_converges_after_a_concurrent_compaction(engine: &dyn ConnectorTestEngine) {
    use dbsp::circuit::tokio::TOKIO;
    use deltalake::open_table;

    let dir = TempDir::new().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut endpoint =
        make_merge_endpoint_ex(uri, DeltaTableWriteMode::Append, 1 << 20, 2, key_relation());

    let records = make_records(8);
    encode_batch(&mut endpoint, &build_insert_batch(engine, &records[0..4]));
    encode_batch(&mut endpoint, &build_insert_batch(engine, &records[4..8]));

    // Compact behind the endpoint's back. Its in-memory snapshot still names the two
    // files this replaces, which is exactly the state the retry has to recover from.
    let url = url::Url::from_file_path(uri).unwrap();
    let compacted = TOKIO.block_on(async move {
        let table = open_table(url).await.unwrap();
        table
            .optimize()
            .with_target_size(std::num::NonZeroU64::new(64 << 20).unwrap())
            .await
            .unwrap()
            .1
    });
    assert!(
        compacted.num_files_removed >= 2,
        "the fixture did not compact anything: {compacted:?}"
    );

    let updates: Vec<(Value, Value)> = (0..2)
        .map(|i| {
            let mut new = make_record(500 + i);
            new["bigint"] = records[i]["bigint"].clone();
            (records[i].clone(), new)
        })
        .collect();
    encode_batch(&mut endpoint, &build_upsert_batch(engine, &updates));

    let mut model: BTreeMap<i64, Value> = records.iter().map(|r| (bigint(r), r.clone())).collect();
    for (_, new) in &updates {
        model.insert(bigint(new), new.clone());
    }
    assert_matches_model(uri, &model, "an update after a concurrent compaction");
}

/// Auto compaction must never make a flush lose its batch, here with `max_retries: 0`.
pub fn auto_compaction_never_races_a_flush(engine: &dyn ConnectorTestEngine) {
    use crate::integrated::delta_table::merge::compact::Compactor;
    use futures::StreamExt;

    let dir = TempDir::new().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut endpoint =
        make_merge_endpoint_ex(uri, DeltaTableWriteMode::Append, 1 << 20, 0, key_relation());
    let records = make_records(20_000);
    for chunk in records.chunks(5_000) {
        encode_batch(&mut endpoint, &build_insert_batch(engine, chunk));
    }

    // The inserts are the large files; each update flush adds a small one.
    let url = url::Url::from_file_path(uri).unwrap();
    TOKIO.block_on(async move {
        let table = deltalake::open_table(url).await.unwrap();
        let smallest = table
            .snapshot()
            .unwrap()
            .log_data()
            .into_iter()
            .map(|f| f.size())
            .min()
            .unwrap();
        table
            .set_tbl_properties()
            .with_properties(std::collections::HashMap::from([(
                deltalake::TableProperty::TargetFileSize
                    .as_ref()
                    .to_string(),
                smallest.to_string(),
            )]))
            .await
            .unwrap();
    });
    // A property change is a metadata change the next flush must read first.
    endpoint.merge_reload = true;
    let mut config = endpoint.inner.config.clone();
    config.auto_compact = true;
    endpoint.compactor = Compactor::new(
        &config,
        "test_merge_endpoint",
        endpoint.merge_metrics.clone().unwrap(),
    );

    let mut model: BTreeMap<i64, Value> = records.iter().map(|r| (bigint(r), r.clone())).collect();
    let mut seed: u64 = 42;
    for round in 0..24 {
        // Keys from the whole table, a fixed sequence, so a flush changes rows in the small
        // files compaction packs, as real updates do.
        let mut keys = std::collections::BTreeSet::new();
        while keys.len() < 500 {
            seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            keys.insert((seed >> 33) as usize % records.len());
        }
        let updates: Vec<(Value, Value)> = keys
            .into_iter()
            .enumerate()
            .map(|(i, k)| {
                let old = model[&bigint(&records[k])].clone();
                let mut new = make_record(100_000 + round * 500 + i);
                new["bigint"] = old["bigint"].clone();
                (old, new)
            })
            .collect();
        // `encode_batch` unwraps: with no retries, a lost race fails here.
        encode_batch(&mut endpoint, &build_upsert_batch(engine, &updates));
        for (_, new) in updates {
            model.insert(bigint(&new), new);
        }
    }
    endpoint.compactor.as_mut().unwrap().wait_for_step();

    let url = url::Url::from_file_path(uri).unwrap();
    let optimizes = TOKIO.block_on(async move {
        let table = deltalake::open_table(url).await.unwrap();
        table
            .history(None)
            .filter(|c| {
                std::future::ready(
                    c.as_ref()
                        .is_ok_and(|c| c.operation.as_deref() == Some("OPTIMIZE")),
                )
            })
            .count()
            .await
    });
    assert!(
        optimizes > 0,
        "no compaction committed: it lost to the flushes, or never ran"
    );
    assert_matches_model(uri, &model, "flushes with auto compaction");
}

/// A batch is acknowledged before its step ends, and the next flush commits after the step.
pub fn the_next_flush_waits_for_the_compaction_step(engine: &dyn ConnectorTestEngine) {
    use crate::integrated::delta_table::merge::compact::Compactor;
    use crate::integrated::delta_table::merge::tests::{FlakyStore, table_over};
    use feldera_adapterlib::metrics::ConnectorMetrics;
    use futures::StreamExt;

    let dir = TempDir::new().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let mut endpoint = make_merge_endpoint_ex(
        &uri,
        DeltaTableWriteMode::Append,
        1 << 20,
        0,
        key_relation(),
    );
    let records = make_records(40);
    for chunk in records[..30].chunks(10) {
        encode_batch(&mut endpoint, &build_insert_batch(engine, chunk));
    }

    // Each file is a third of the target, so with the next one they fill it and the next
    // commit starts a pack.
    let store = FlakyStore::new(0, 0);
    endpoint.task.delta_table = TOKIO.block_on(async {
        let table = table_over(store.clone(), &dir).await;
        let size: i64 = table
            .snapshot()
            .unwrap()
            .log_data()
            .into_iter()
            .map(|f| f.size())
            .sum();
        table
            .set_tbl_properties()
            .with_properties(std::collections::HashMap::from([(
                deltalake::TableProperty::TargetFileSize
                    .as_ref()
                    .to_string(),
                size.to_string(),
            )]))
            .await
            .unwrap()
    });
    let mut config = endpoint.inner.config.clone();
    config.auto_compact = true;
    endpoint.compactor = Compactor::new(
        &config,
        "test_merge_endpoint",
        endpoint.merge_metrics.clone().unwrap(),
    );

    store.hang_data_reads();
    let (returned, endpoint_back) = std::sync::mpsc::channel();
    // The engine stays on this thread, so each batch is built before its flush starts.
    let batch = build_insert_batch(engine, &records[30..35]);
    let returned_first = returned.clone();
    std::thread::spawn(move || {
        encode_batch(&mut endpoint, &batch);
        returned_first.send(endpoint).unwrap();
    });
    let mut endpoint = endpoint_back
        .recv_timeout(Duration::from_secs(60))
        .expect("the compaction step held up the batch that started it");
    let start = Instant::now();
    while store.hung_reads() == 0 {
        assert!(
            start.elapsed() < Duration::from_secs(60),
            "no compaction step started"
        );
        std::thread::sleep(Duration::from_millis(10));
    }

    let batch = build_insert_batch(engine, &records[35..]);
    std::thread::spawn(move || {
        encode_batch(&mut endpoint, &batch);
        returned.send(endpoint).unwrap();
    });
    // Time for a flush that does not wait to commit before the step. A flush that waits
    // passes whatever this is.
    std::thread::sleep(Duration::from_secs(2));
    store.release_data_reads();
    let mut endpoint = endpoint_back
        .recv_timeout(Duration::from_secs(60))
        .expect("the flush did not finish once the step could");
    endpoint.compactor.as_mut().unwrap().wait_for_step();

    let url = url::Url::from_file_path(&uri).unwrap();
    let operations: Vec<String> = TOKIO.block_on(async move {
        let table = deltalake::open_table(url).await.unwrap();
        let mut history: Vec<_> = table.history(None).collect().await;
        // Oldest first.
        history.reverse();
        history
            .into_iter()
            .map(|c| c.unwrap().operation.unwrap_or_default())
            .collect()
    });
    let first_pack = operations.iter().position(|op| op == "OPTIMIZE");
    let last_flush = operations.iter().rposition(|op| op != "OPTIMIZE");
    assert!(
        first_pack.is_some_and(|pack| Some(pack) < last_flush),
        "the flush committed before the step it had to wait for: {operations:?}"
    );
    let waited = endpoint
        .merge_metrics
        .as_ref()
        .unwrap()
        .metrics()
        .into_iter()
        .find(|(name, ..)| {
            *name == "output_connector_delta_merge_compaction_wait_microseconds_total"
        })
        .unwrap()
        .3;
    assert!(waited > 0.0, "the wait was not recorded");
    let model: BTreeMap<i64, Value> = records.iter().map(|r| (bigint(r), r.clone())).collect();
    assert_matches_model(&uri, &model, "a flush after a compaction step");
}

/// A duplicate key must be reported once per batch, not once per attempt.
///
/// The retry walks the same batch, so a conflicting commit would otherwise repeat every
/// violation on every attempt -- without a `max_retries` limit, for ever.
pub fn a_uniqueness_violation_is_reported_once_across_retries(engine: &dyn ConnectorTestEngine) {
    use dbsp::circuit::tokio::TOKIO;
    use deltalake::open_table;

    let dir = TempDir::new().unwrap();
    let uri = dir.path().to_str().unwrap();
    // One retry allowed, so a conflict produces exactly two attempts.
    let mut endpoint =
        make_merge_endpoint_ex(uri, DeltaTableWriteMode::Append, 1 << 20, 1, key_relation());

    let records = make_records(8);
    encode_batch(&mut endpoint, &build_insert_batch(engine, &records[0..4]));
    encode_batch(&mut endpoint, &build_insert_batch(engine, &records[4..8]));

    // Compact behind the endpoint's back so its next commit conflicts and retries.
    let url = url::Url::from_file_path(uri).unwrap();
    TOKIO.block_on(async move {
        let table = open_table(url).await.unwrap();
        table
            .optimize()
            .with_target_size(std::num::NonZeroU64::new(64 << 20).unwrap())
            .await
            .unwrap();
    });

    // One real update (to force the conflicting commit) plus one key inserted twice.
    let mut new = make_record(500);
    new["bigint"] = records[0]["bigint"].clone();
    let batch = keyed_batch(
        engine,
        "bigint",
        vec![
            (records[0].clone(), -1),
            (new.clone(), 1),
            (records[5].clone(), 2),
        ],
    );

    encode_batch(&mut endpoint, &batch);
    assert_eq!(
        endpoint.merge_violations, 1,
        "the retry reported the same duplicate key again"
    );
}

/// A failed commit must leave its deletion vector object alone.
///
/// An error can also mean the commit landed and only its response was lost, in which case
/// the object is one the live version references and deleting it breaks every reader.
/// Forced here with a conflict, the one commit failure a test can produce on demand.
pub fn a_failed_commit_keeps_its_deletion_vector_file(engine: &dyn ConnectorTestEngine) {
    use dbsp::circuit::tokio::TOKIO;
    use deltalake::open_table;

    let dir = TempDir::new().unwrap();
    let uri = dir.path().to_str().unwrap();
    // No retries, so the conflicting attempt is the last one.
    let mut endpoint =
        make_merge_endpoint_ex(uri, DeltaTableWriteMode::Append, 1 << 20, 0, key_relation());

    let records = make_records(8);
    encode_batch(&mut endpoint, &build_insert_batch(engine, &records[0..4]));
    encode_batch(&mut endpoint, &build_insert_batch(engine, &records[4..8]));

    // Compact behind the endpoint's back so its next commit conflicts.
    let url = url::Url::from_file_path(uri).unwrap();
    TOKIO.block_on(async move {
        let table = open_table(url).await.unwrap();
        table
            .optimize()
            .with_target_size(std::num::NonZeroU64::new(64 << 20).unwrap())
            .await
            .unwrap();
    });

    let mut new = make_record(500);
    new["bigint"] = records[0]["bigint"].clone();
    let batch = build_upsert_batch(engine, &[(records[0].clone(), new)]);
    endpoint.consumer().batch_start(0, OutputBatchType::Delta);
    assert!(
        endpoint.encode(batch).is_err(),
        "the fixture did not produce a conflicting commit"
    );

    assert_eq!(
        vector_files(uri).len(),
        1,
        "the failed commit deleted its deletion vector file"
    );
}

/// A retry must not append rows that a failed attempt already committed.
///
/// The commit lands, but reading the log afterwards fails, so the attempt reports an error.
/// The reload before the retry fails too. A retry on the old snapshot would miss the
/// committed rows and append every one of them again.
pub fn a_retry_after_a_commit_that_landed_does_not_duplicate(engine: &dyn ConnectorTestEngine) {
    let dir = TempDir::new().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut endpoint = make_merge_endpoint_ex(
        uri,
        DeltaTableWriteMode::Append,
        1 << 20,
        10,
        key_relation(),
    );
    let records = make_records(8);
    encode_batch(&mut endpoint, &build_insert_batch(engine, &records[0..4]));

    let store = Arc::new(LogOutageStore {
        inner: Arc::new(deltalake::logstore::object_store::local::LocalFileSystem::new()),
        armed: AtomicUsize::new(1),
        failures_left: AtomicUsize::new(0),
        injected: AtomicUsize::new(0),
    });
    reopen_over(&mut endpoint, uri, store.clone());

    encode_batch(&mut endpoint, &build_insert_batch(engine, &records[4..8]));
    assert!(
        store.injected.load(Ordering::Relaxed) >= OUTAGE_READS,
        "the store did not fail the reads after the commit"
    );
    let model: BTreeMap<i64, Value> = records.iter().map(|r| (bigint(r), r.clone())).collect();
    assert_matches_model(uri, &model, "a retry after a commit that landed");
}

/// Endpoint whose table reads and writes its log through `store`.
fn reopen_over(endpoint: &mut DeltaTableWriter, uri: &str, store: Arc<LogOutageStore>) {
    use dbsp::circuit::tokio::TOKIO;
    let url = deltalake::table::builder::ensure_table_uri(uri).unwrap();
    endpoint.task.delta_table = TOKIO.block_on(async {
        deltalake::DeltaTableBuilder::from_url(url.clone())
            .unwrap()
            .with_storage_backend(store, url)
            .load()
            .await
            .unwrap()
    });
}

/// Hand `batch` to the endpoint, expecting the encoder to fail it.
fn encode_failing_batch(endpoint: &mut DeltaTableWriter, batch: &Arc<dyn SerBatchReader>) {
    endpoint.consumer().batch_start(0, OutputBatchType::Delta);
    assert!(
        endpoint.encode(batch.clone()).is_err(),
        "the fixture did not fail the batch"
    );
    endpoint.consumer().batch_end();
}

/// The batch after a failed one must see what the failed one committed.
///
/// The failed batch's commit landed, but reading the log afterwards failed and no retry
/// was allowed. An update of a key it inserted must find that row, not add a second one.
pub fn a_batch_after_a_failed_batch_whose_commit_landed_does_not_duplicate(
    engine: &dyn ConnectorTestEngine,
) {
    let dir = TempDir::new().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut endpoint =
        make_merge_endpoint_ex(uri, DeltaTableWriteMode::Append, 1 << 20, 0, key_relation());
    let records = make_records(8);
    encode_batch(&mut endpoint, &build_insert_batch(engine, &records[0..4]));

    let store = Arc::new(LogOutageStore {
        inner: Arc::new(deltalake::logstore::object_store::local::LocalFileSystem::new()),
        armed: AtomicUsize::new(1),
        failures_left: AtomicUsize::new(0),
        injected: AtomicUsize::new(0),
    });
    reopen_over(&mut endpoint, uri, store.clone());
    encode_failing_batch(&mut endpoint, &build_insert_batch(engine, &records[4..8]));
    store.failures_left.store(0, Ordering::Relaxed);

    let mut new = make_record(500);
    new["bigint"] = records[5]["bigint"].clone();
    encode_batch(
        &mut endpoint,
        &build_upsert_batch(engine, &[(records[5].clone(), new.clone())]),
    );

    let mut model: BTreeMap<i64, Value> = records.iter().map(|r| (bigint(r), r.clone())).collect();
    model.insert(bigint(&new), new);
    assert_matches_model(uri, &model, "an update after a failed batch that committed");
}

/// A dropped delete leaves its row live, so a later insert of that key must find it.
///
/// The table started empty, so inserts skip the lookup. The delete conflicts with a
/// compaction and no retry is allowed, which drops the batch.
pub fn the_insert_shortcut_does_not_survive_a_dropped_batch(engine: &dyn ConnectorTestEngine) {
    use dbsp::circuit::tokio::TOKIO;
    use deltalake::open_table;

    let dir = TempDir::new().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut endpoint =
        make_merge_endpoint_ex(uri, DeltaTableWriteMode::Append, 1 << 20, 0, key_relation());
    let records = make_records(8);
    encode_batch(&mut endpoint, &build_insert_batch(engine, &records[0..4]));
    encode_batch(&mut endpoint, &build_insert_batch(engine, &records[4..8]));

    let url = url::Url::from_file_path(uri).unwrap();
    TOKIO.block_on(async move {
        open_table(url)
            .await
            .unwrap()
            .optimize()
            .with_target_size(std::num::NonZeroU64::new(64 << 20).unwrap())
            .await
            .unwrap();
    });
    encode_failing_batch(&mut endpoint, &build_delete_batch(engine, &records[0..1]));

    let mut new = make_record(500);
    new["bigint"] = records[0]["bigint"].clone();
    encode_batch(
        &mut endpoint,
        &build_insert_batch(engine, std::slice::from_ref(&new)),
    );

    let rows = read_merge_output(uri);
    assert_eq!(
        bigints(&rows)
            .into_iter()
            .filter(|key| *key == bigint(&new))
            .count(),
        1,
        "the key has two live rows: {rows:?}"
    );
}

/// A key skipped for a uniqueness violation keeps its old row, so a later insert of that
/// key must find it.
pub fn the_insert_shortcut_does_not_survive_a_skipped_key(engine: &dyn ConnectorTestEngine) {
    let dir = TempDir::new().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut endpoint =
        make_merge_endpoint_ex(uri, DeltaTableWriteMode::Append, 1 << 20, 0, key_relation());
    let old = make_record(0);
    encode_batch(
        &mut endpoint,
        &build_insert_batch(engine, std::slice::from_ref(&old)),
    );

    let with_key = |i: usize| {
        let mut record = make_record(i);
        record["bigint"] = old["bigint"].clone();
        record
    };
    let batch = |rows: Vec<(Value, i64)>| keyed_batch(engine, "bigint", rows);
    // Two new values for one key, then both gone: each step is a violation the connector
    // skips, so the table keeps `old`.
    encode_batch(
        &mut endpoint,
        &batch(vec![(old.clone(), -1), (with_key(1), 1), (with_key(2), 1)]),
    );
    encode_batch(
        &mut endpoint,
        &batch(vec![(with_key(1), -1), (with_key(2), -1)]),
    );
    assert_eq!(endpoint.merge_violations, 2);

    let new = with_key(3);
    encode_batch(&mut endpoint, &batch(vec![(new.clone(), 1)]));

    let rows = read_merge_output(uri);
    assert_eq!(
        rows.zset().unwrap(),
        values_zset(&[new]),
        "the skipped key's old row is still live"
    );
}

/// Log reads the store fails after the armed commit: the read that follows the commit,
/// and the reload before the retry.
const OUTAGE_READS: usize = 2;

/// Fails the next [`OUTAGE_READS`] log reads after one commit lands.
#[derive(Debug)]
struct LogOutageStore {
    inner: ObjectStoreRef,
    armed: AtomicUsize,
    failures_left: AtomicUsize,
    injected: AtomicUsize,
}

impl LogOutageStore {
    fn fail(&self, location: Option<&ObjectPath>) -> bool {
        let in_log = location.is_some_and(|l| l.as_ref().contains("_delta_log"));
        let claimed = in_log
            && self
                .failures_left
                .try_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1))
                .is_ok();
        if claimed {
            self.injected.fetch_add(1, Ordering::Relaxed);
        }
        claimed
    }

    fn outage() -> deltalake::logstore::object_store::Error {
        deltalake::logstore::object_store::Error::Generic {
            store: "outage",
            source: "injected outage".into(),
        }
    }
}

impl std::fmt::Display for LogOutageStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LogOutageStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for LogOutageStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        let result = self.inner.put_opts(location, payload, opts).await;
        let is_commit =
            location.as_ref().contains("_delta_log/") && location.as_ref().ends_with(".json");
        if result.is_ok()
            && is_commit
            && self
                .armed
                .try_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1))
                .is_ok()
        {
            self.failures_left.store(OUTAGE_READS, Ordering::Relaxed);
        }
        result
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        opts: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> ObjectStoreResult<GetResult> {
        if self.fail(Some(location)) {
            return Err(Self::outage());
        }
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, ObjectStoreResult<ObjectPath>>,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectPath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        if self.fail(prefix) {
            return Box::pin(futures::stream::once(async { Err(Self::outage()) }));
        }
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&ObjectPath>,
        offset: &ObjectPath,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        if self.fail(prefix) {
            return Box::pin(futures::stream::once(async { Err(Self::outage()) }));
        }
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> ObjectStoreResult<ListResult> {
        if self.fail(prefix) {
            return Err(Self::outage());
        }
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        options: CopyOptions,
    ) -> ObjectStoreResult<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// OPTIMIZE must drop the rows the connector's vectors mark deleted, not resurrect them.
///
/// We tell administrators to run OPTIMIZE, and a compaction that rewrote a file while
/// ignoring its vector would bring every superseded row back. Nothing in the connector
/// could detect that afterwards.
pub fn compaction_drops_the_rows_a_vector_marks_deleted(engine: &dyn ConnectorTestEngine) {
    use dbsp::circuit::tokio::TOKIO;
    use deltalake::open_table;

    let dir = TempDir::new().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut endpoint = make_merge_endpoint(uri, DeltaTableWriteMode::Append);

    // Five rows in one file, then supersede two of them. Superseding a strict subset is
    // what leaves a vector behind: superseding all five would drop the file whole.
    let records = make_records(5);
    encode_batch(&mut endpoint, &build_insert_batch(engine, &records));

    let updates: Vec<(Value, Value)> = (0..2)
        .map(|i| {
            let mut new = make_record(900 + i);
            new["bigint"] = records[i]["bigint"].clone();
            (records[i].clone(), new)
        })
        .collect();
    encode_batch(&mut endpoint, &build_upsert_batch(engine, &updates));

    let mut model: BTreeMap<i64, Value> = records.iter().map(|r| (bigint(r), r.clone())).collect();
    for (_, new) in &updates {
        model.insert(bigint(new), new.clone());
    }
    assert_matches_model(uri, &model, "before compaction");

    // Without this the test would pass on a table that has no vector to respect.
    assert_eq!(
        vector_files(uri).len(),
        1,
        "the fixture wrote no deletion vector"
    );

    let url = url::Url::from_file_path(uri).unwrap();
    let compacted = TOKIO.block_on(async move {
        let table = open_table(url).await.unwrap();
        table
            .optimize()
            .with_target_size(std::num::NonZeroU64::new(64 << 20).unwrap())
            .await
            .unwrap()
            .1
    });
    assert!(
        compacted.num_files_removed >= 2,
        "the fixture did not compact anything: {compacted:?}"
    );

    assert_matches_model(uri, &model, "after compaction");
}

/// Reclaiming storage must rewrite every column type unchanged.
///
/// The rewrite reads the live rows back through DataFusion and casts them to the table's
/// schema, so a lossy cast would alter values with no row missing for its count check
/// to notice. [`value_relation`] holds every type the connector writes.
pub fn reclaim_rewrites_every_column_type_unchanged(engine: &dyn ConnectorTestEngine) {
    use crate::integrated::delta_table::merge::rewrite::reclaim_superseded_rows;
    use dbsp::circuit::tokio::TOKIO;
    use deltalake::open_table;

    let dir = TempDir::new().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut endpoint = make_merge_endpoint(uri, DeltaTableWriteMode::Append);

    // Supersede three of five rows: past the half that makes a file worth rewriting.
    let records = make_records(5);
    encode_batch(&mut endpoint, &build_insert_batch(engine, &records));
    let updates: Vec<(Value, Value)> = (0..3)
        .map(|i| {
            let mut new = make_record(900 + i);
            new["bigint"] = records[i]["bigint"].clone();
            (records[i].clone(), new)
        })
        .collect();
    encode_batch(&mut endpoint, &build_upsert_batch(engine, &updates));

    let mut model: BTreeMap<i64, Value> = records.iter().map(|r| (bigint(r), r.clone())).collect();
    for (_, new) in &updates {
        model.insert(bigint(new), new.clone());
    }

    let url = url::Url::from_file_path(uri).unwrap();
    let reclaimed = TOKIO.block_on(async move {
        let mut table = open_table(url).await.unwrap();
        reclaim_superseded_rows(&mut table, usize::MAX)
            .await
            .unwrap()
    });
    assert_eq!(
        reclaimed.files_rewritten, 1,
        "the fixture left nothing to reclaim"
    );
    assert_matches_model(uri, &model, "reclaiming the superseded rows");
}

/// No data file is ever rewritten, which is the point of the design. A file may leave the
/// table once every row in it is tombstoned, but no file's contents move to a new path.
pub fn merge_never_rewrites_a_data_file(engine: &dyn ConnectorTestEngine) {
    use dbsp::circuit::tokio::TOKIO;
    use deltalake::open_table;

    let dir = TempDir::new().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut endpoint = make_merge_endpoint(uri, DeltaTableWriteMode::Append);
    let records = make_records(8);

    let paths = |uri: &str| -> Vec<String> {
        let url = url::Url::from_file_path(uri).unwrap();
        TOKIO.block_on(async move {
            let table = open_table(url).await.unwrap();
            let mut paths: Vec<String> = table
                .snapshot()
                .unwrap()
                .log_data()
                .into_iter()
                .map(|f| f.path().to_string())
                .collect();
            paths.sort();
            paths
        })
    };

    encode_batch(&mut endpoint, &build_insert_batch(engine, &records[0..4]));
    let after_insert = paths(uri);
    assert_eq!(after_insert.len(), 1);

    // Update every row in that file. Its path must survive, carrying a vector.
    let updates: Vec<(Value, Value)> = (0..3)
        .map(|i| {
            let mut new = make_record(200 + i);
            new["bigint"] = records[i]["bigint"].clone();
            (records[i].clone(), new)
        })
        .collect();
    encode_batch(&mut endpoint, &build_upsert_batch(engine, &updates));

    let after_update = paths(uri);
    assert!(
        after_update.contains(&after_insert[0]),
        "the original data file was rewritten: {after_insert:?} -> {after_update:?}"
    );
}

/// A key relation naming one column of the test schema.
///
/// The column type comes from the view, so the test cannot drift from what the view
/// actually declares.
pub fn key_relation_on(column: &str) -> Relation {
    let field = value_relation()
        .fields
        .into_iter()
        .find(|f| f.name.name() == column)
        .unwrap_or_else(|| panic!("no column '{column}' in the test schema"));
    Relation {
        name: SqlIdentifier::new(format!("delta_output_key_{column}"), false),
        fields: vec![field],
        materialized: false,
        properties: BTreeMap::new(),
        primary_key: None,
    }
}

/// One batch of `(row, weight)` pairs, keyed on `column`.
fn keyed_batch(
    engine: &dyn ConnectorTestEngine,
    column: &str,
    rows: Vec<(Value, i64)>,
) -> Arc<dyn SerBatchReader> {
    let updates = weighted_updates(rows);
    engine
        .indexed_output_batch(
            &key_relation_on(column),
            &value_relation(),
            updates.rows(),
            updates.weights(),
        )
        .unwrap()
}

/// One batch of `row` with weight 1, keyed on `column`, with the values exactly as `row` holds
/// them: unlike [`keyed_batch`], it keeps the sign of a zero.
fn exact_keyed_batch(
    engine: &dyn ConnectorTestEngine,
    column: &str,
    row: &Value,
) -> Arc<dyn SerBatchReader> {
    let relation = value_relation();
    let row = read::arrow_form(&relation, row.clone())
        .unwrap()
        .to_string();
    let rows = ReaderBuilder::new(published_schema(&relation))
        .with_strict_mode(true)
        .build(row.as_bytes())
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    engine
        .indexed_output_batch(&key_relation_on(column), &relation, &rows, &[1])
        .unwrap()
}

/// Returns the values of the DOUBLE column `column` in the Delta table at `table_uri`, exactly
/// as the table holds them.
fn table_doubles(table_uri: &str, column: &str) -> Vec<f64> {
    delta::table_batches(table_uri)
        .unwrap()
        .iter()
        .flat_map(|batch| {
            batch
                .column_by_name(column)
                .unwrap()
                .as_primitive::<Float64Type>()
                .values()
                .to_vec()
        })
        .collect()
}

/// Assert the table holds exactly `expected`, as a reader applying deletion vectors sees it.
fn assert_table_rows(table_uri: &str, expected: &[Value], step: &str) {
    let actual = read_merge_output(table_uri);
    assert_eq!(
        actual.len(),
        expected.len(),
        "after {step}: table has {} row(s), the view has {}",
        actual.len(),
        expected.len()
    );
    assert_eq!(
        actual.zset().unwrap(),
        values_zset(expected),
        "after {step}: table diverged from the view"
    );
}

/// Drive one key type through insert, update and delete, and check that the table still
/// equals the view.
///
/// An update copies the key column from the old row to the new one, which is what makes it
/// an update instead of a second insert.
fn merge_key_type_case(engine: &dyn ConnectorTestEngine, column: &str) {
    let dir = TempDir::new().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut endpoint = make_merge_endpoint_ex(
        uri,
        DeltaTableWriteMode::Append,
        1 << 20,
        0,
        key_relation_on(column),
    );

    // Small indices keep every column of `make_record` distinct, so whichever column
    // the test keys on is unique across the batch.
    let records = make_records(12);
    encode_batch(
        &mut endpoint,
        &keyed_batch(
            engine,
            column,
            records.iter().map(|r| (r.clone(), 1i64)).collect(),
        ),
    );
    assert_table_rows(uri, &records, &format!("12 inserts keyed on '{column}'"));

    // Update the first four: same key, every other column different. A connector that
    // did not find the old rows would leave 16 rows here instead of 12.
    let updates: Vec<(Value, Value)> = (0..4)
        .map(|i| {
            let mut new = make_record(500 + i);
            new[column] = records[i][column].clone();
            (records[i].clone(), new)
        })
        .collect();
    let mut rows = Vec::new();
    for (old, new) in &updates {
        rows.push((old.clone(), -1i64));
        rows.push((new.clone(), 1i64));
    }
    encode_batch(&mut endpoint, &keyed_batch(engine, column, rows));

    let mut expected: Vec<Value> = updates.iter().map(|(_, new)| new.clone()).collect();
    expected.extend_from_slice(&records[4..]);
    assert_table_rows(uri, &expected, &format!("4 updates keyed on '{column}'"));

    // Delete two rows the table really holds.
    encode_batch(
        &mut endpoint,
        &keyed_batch(
            engine,
            column,
            records[10..12].iter().map(|r| (r.clone(), -1i64)).collect(),
        ),
    );
    expected.retain(|r| bigint(r) != bigint(&records[10]) && bigint(r) != bigint(&records[11]));
    assert_table_rows(uri, &expected, &format!("2 deletes keyed on '{column}'"));
}

/// `-0.0` and `0.0` are one key to Feldera, so they must locate one row.
///
/// The row encoding orders floats by their bits, so without normalizing them the second
/// run looks up a key the table does not hold and leaves two live rows for it.
///
/// Shaped as a second run over an existing table because that is what reproduces against
/// a live pipeline: within one run the view's index keeps whichever sign it saw first, so
/// the connector is handed a key that already matches what it wrote.
pub fn merge_treats_signed_zeroes_as_one_key(engine: &dyn ConnectorTestEngine) {
    let dir = TempDir::new().unwrap();
    let uri = dir.path().to_str().unwrap();

    let row_of = |i: usize, d: f64| {
        let mut r = make_record(i);
        r["double"] = json!(d);
        r
    };

    let stored = row_of(1, -0.0);
    let mut first = make_merge_endpoint_ex(
        uri,
        DeltaTableWriteMode::Append,
        1 << 20,
        0,
        key_relation_on("double"),
    );
    encode_batch(&mut first, &exact_keyed_batch(engine, "double", &stored));
    assert_table_rows(uri, &[stored], "a first run writing -0.0");
    let doubles = table_doubles(uri, "double");
    assert!(
        doubles.len() == 1 && doubles[0] == 0.0 && doubles[0].is_sign_negative(),
        "the first run stored {doubles:?}, not -0.0"
    );

    // A second run adopts the table and knows only +0.0 for that key.
    let updated = row_of(2, 0.0);
    let mut second = make_merge_endpoint_ex(
        uri,
        DeltaTableWriteMode::Append,
        1 << 20,
        0,
        key_relation_on("double"),
    );
    encode_batch(
        &mut second,
        &keyed_batch(engine, "double", vec![(updated.clone(), 1i64)]),
    );
    assert_table_rows(uri, &[updated], "a second run inserting +0.0");
}

/// A null key must still find its row. Statistics leave nulls out, so a chunk holding one
/// turns pruning off; a flush that pruned anyway would leave two live rows for that key.
pub fn merge_supersedes_a_row_whose_key_is_null(engine: &dyn ConnectorTestEngine) {
    let dir = TempDir::new().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut endpoint = make_merge_endpoint_ex(
        uri,
        DeltaTableWriteMode::Append,
        1 << 20,
        0,
        key_relation_on("unused"),
    );
    // `make_record` nulls `unused` on every third index, so index 3 is the null key and
    // the others are distinct. One null key per batch: two would not be a unique key.
    let records: Vec<Value> = [1, 2, 3, 4].into_iter().map(make_record).collect();
    encode_batch(
        &mut endpoint,
        &keyed_batch(
            engine,
            "unused",
            records.iter().map(|r| (r.clone(), 1i64)).collect(),
        ),
    );
    assert_table_rows(uri, &records, "4 inserts, one keyed on null");

    // Supersede the null-keyed row, in a later flush so the lookup has to find it on disk.
    let null_keyed = &records[2];
    let mut replacement = make_record(500);
    replacement["unused"] = Value::Null;
    encode_batch(
        &mut endpoint,
        &keyed_batch(
            engine,
            "unused",
            vec![(null_keyed.clone(), -1i64), (replacement.clone(), 1i64)],
        ),
    );

    let mut expected = vec![records[0].clone(), records[1].clone(), records[3].clone()];
    expected.push(replacement);
    assert_table_rows(uri, &expected, "the null-keyed row superseded");
}

fn build_insert_batch(
    engine: &dyn ConnectorTestEngine,
    records: &[Value],
) -> Arc<dyn SerBatchReader> {
    keyed_batch(
        engine,
        "bigint",
        records.iter().map(|r| (r.clone(), 1i64)).collect(),
    )
}

fn build_delete_batch(
    engine: &dyn ConnectorTestEngine,
    records: &[Value],
) -> Arc<dyn SerBatchReader> {
    keyed_batch(
        engine,
        "bigint",
        records.iter().map(|r| (r.clone(), -1i64)).collect(),
    )
}

fn build_upsert_batch(
    engine: &dyn ConnectorTestEngine,
    updates: &[(Value, Value)],
) -> Arc<dyn SerBatchReader> {
    let mut rows = Vec::new();
    for (old, new) in updates {
        assert_eq!(bigint(old), bigint(new));
        rows.push((old.clone(), -1i64));
        rows.push((new.clone(), 1i64));
    }
    keyed_batch(engine, "bigint", rows)
}

fn build_non_indexed_batch(
    engine: &dyn ConnectorTestEngine,
    records: &[Value],
    weight: i64,
) -> Arc<dyn SerBatchReader> {
    let updates = weighted_updates(records.iter().map(|r| (r.clone(), weight)).collect());
    engine
        .output_batch(&value_relation(), updates.rows(), updates.weights())
        .unwrap()
}

fn encode_batch(endpoint: &mut DeltaTableWriter, batch: &Arc<dyn SerBatchReader>) {
    endpoint.consumer().batch_start(0, OutputBatchType::Delta);
    endpoint.encode(batch.clone()).unwrap();
    endpoint.consumer().batch_end();
}

/// Returns the rows of every Parquet file of the table, with the columns the connector adds
/// in cdc mode.
fn read_output(table_uri: &str) -> Vec<OutputRecord> {
    let parquet_files = delta::parquet_files(Path::new(table_uri));
    output_records(&parquet_files)
}

/// Returns the rows of the Parquet files at `paths`, with the columns the connector adds in
/// cdc mode.
fn output_records(paths: &[std::path::PathBuf]) -> Vec<OutputRecord> {
    let data = delta::file_rows(&value_relation(), paths).unwrap();
    let cdc = delta::file_rows(&cdc_relation(), paths).unwrap();
    let ops = cdc.rows().column(0).as_string::<i32>();
    let timestamps = cdc.rows().column(1).as_primitive::<Int64Type>();
    (0..data.len())
        .map(|index| OutputRecord {
            data: data.take(&[index as u32], vec![1]).unwrap(),
            __feldera_op: ops.value(index).to_string(),
            __feldera_ts: timestamps.value(index),
        })
        .collect()
}

/// Read only the files referenced by the current delta table snapshot,
/// ignoring orphaned parquet files left behind by `SaveMode::Overwrite`.
fn read_delta_output(table_uri: &str) -> Vec<OutputRecord> {
    use deltalake::open_table;

    let url = url::Url::from_file_path(table_uri).unwrap();
    let table = TOKIO.block_on(async move { open_table(url).await.unwrap() });
    let base = Path::new(table_uri);
    let paths: Vec<std::path::PathBuf> = table
        .get_file_uris()
        .unwrap()
        .map(|uri| base.join(&*uri))
        .collect();
    output_records(&paths)
}

/// Returns row `i` of [`value_relation`], in the default JSON flavor: each column a value that
/// `i` decides.
fn make_record(i: usize) -> Value {
    let date = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()
        + chrono::Days::new((i % 100_000) as u64);
    let timestamp =
        chrono::DateTime::from_timestamp_millis(1704070800000 + i as i64 * 1000).unwrap();
    json!({
        "bigint": i as i64,
        "binary": [i as u8, (i >> 8) as u8],
        "boolean": i.is_multiple_of(2),
        "date": date.format("%Y-%m-%d").to_string(),
        "decimal_10_3": format!("{}.000", i % 1_000_000),
        "double": (i as f64).trunc(),
        "float": (i as f32).trunc(),
        "int": i as i32,
        "smallint": (i % 32000) as i16,
        "string": format!("record_{i}"),
        "unused": if i.is_multiple_of(3) { None } else { Some(format!("unused_{i}")) },
        "timestamp_ntz": timestamp.format("%F %T%.f").to_string(),
        "timestamp_tz": timestamp.format("%F %T%.f%:z").to_string(),
        "tinyint": (i % 120) as i8,
        "string_array": [format!("arr_{i}")],
        "struct1": {"id": i, "b": i.is_multiple_of(2), "i": i, "s": format!("s_{i}")},
        "struct_array": [{"id": i, "b": false, "i": null, "s": format!("sa_{i}")}],
        "string_string_map": {format!("key_{i}"): format!("val_{i}")},
        "string_struct_map": {
            format!("sk_{i}"): {"id": i, "b": true, "i": i as i64 * 2, "s": format!("sm_{i}")}
        },
        "variant": {"foo": i.to_string()},
        "uuid": uuid::Uuid::from_bytes([i as u8; 16]).to_string(),
    })
}

fn make_records(n: usize) -> Vec<Value> {
    (0..n).map(make_record).collect()
}

fn insert_test(engine: &dyn ConnectorTestEngine, threads: usize) {
    let table_dir = TempDir::new().unwrap();
    let table_uri = table_dir.path().display().to_string();

    let records = make_records(100);
    let batch = build_insert_batch(engine, &records);
    let mut endpoint = make_endpoint(threads, &table_uri, true);

    let before = chrono::Utc::now().timestamp_micros();
    encode_batch(&mut endpoint, &batch);
    let after = chrono::Utc::now().timestamp_micros();

    let output = read_output(&table_uri);
    assert_eq!(output.len(), 100);
    for rec in &output {
        assert_eq!(rec.__feldera_op, "i");
        // The time at which the writer encoded the batch.
        assert!(
            (before..=after).contains(&rec.__feldera_ts),
            "__feldera_ts {} is not within [{before}, {after}]",
            rec.__feldera_ts
        );
    }
    // Verify data fields match
    let output_data = updates_zset(output.iter().map(|r| r.to_data_record()));
    assert_eq!(output_data, values_zset(&records));
}

pub fn insert_single_thread(engine: &dyn ConnectorTestEngine) {
    insert_test(engine, 1);
}

/// A shutdown aborts the write rather than letting it retry on.
///
/// `encode` runs on the controller's output thread, which is a DBSP aux
/// thread that `RuntimeHandle::kill` joins, and the retry loop in
/// `encode_and_write_range` is unbounded unless `max_retries` says
/// otherwise.  A write that outlives the shutdown hangs the teardown.
pub fn shutdown_aborts_the_write(engine: &dyn ConnectorTestEngine) {
    let table_dir = TempDir::new().unwrap();
    let table_uri = table_dir.path().display().to_string();

    let batch = build_insert_batch(engine, &make_records(100));
    let mut endpoint = make_endpoint(1, &table_uri, true);

    endpoint.inner.shutdown.cancel();

    endpoint.consumer().batch_start(0, OutputBatchType::Delta);
    let error = endpoint
        .encode(batch.clone())
        .expect_err("a shutdown must abort the write");
    assert!(
        error.to_string().contains("shutting down"),
        "unexpected error: {error}"
    );

    // Aborting before the first attempt writes nothing at all.  An abort
    // part-way leaves orphan Parquet files instead, which `VACUUM` takes.
    assert!(
        read_output(&table_uri).is_empty(),
        "the aborted write must not have written any rows"
    );
}

/// Set table properties from outside the pipeline, as an `ALTER TABLE` would.
fn alter_table(uri: &str, property: &str, value: &str) {
    dbsp::circuit::tokio::TOKIO.block_on(async {
        deltalake::open_table(url::Url::from_file_path(uri).unwrap())
            .await
            .unwrap()
            .set_tbl_properties()
            .with_properties(std::collections::HashMap::from([(
                property.to_string(),
                value.to_string(),
            )]))
            .await
            .unwrap();
    });
}

/// An `ALTER TABLE` that breaks a startup requirement stops merge mode, rather than its
/// retry reloading the table and committing past the change.
pub fn merge_stops_when_the_table_no_longer_fits(engine: &dyn ConnectorTestEngine) {
    let dir = TempDir::new().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut endpoint =
        make_merge_endpoint_ex(uri, DeltaTableWriteMode::Append, 1 << 20, 3, key_relation());
    encode_batch(&mut endpoint, &build_insert_batch(engine, &make_records(4)));

    alter_table(uri, "delta.enableChangeDataFeed", "true");

    endpoint.consumer().batch_start(1, OutputBatchType::Delta);
    let error = endpoint
        .encode(build_insert_batch(engine, &[make_record(10)]))
        .expect_err("a table with a change data feed must stop merge mode");
    assert!(
        format!("{error:#}").contains("changed while the pipeline was running"),
        "unexpected error: {error:#}"
    );
}

/// A change merge mode does not depend on must not stop it.
pub fn merge_survives_an_unrelated_table_change(engine: &dyn ConnectorTestEngine) {
    let dir = TempDir::new().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut endpoint =
        make_merge_endpoint_ex(uri, DeltaTableWriteMode::Append, 1 << 20, 3, key_relation());
    encode_batch(&mut endpoint, &build_insert_batch(engine, &make_records(4)));

    alter_table(uri, "delta.logRetentionDuration", "interval 7 days");

    encode_batch(
        &mut endpoint,
        &build_insert_batch(engine, &[make_record(10)]),
    );
    assert_eq!(read_merge_output(uri).len(), 5);
}

/// A shutdown aborts a merge flush, as it does a cdc write, rather than letting it run on.
pub fn shutdown_aborts_the_merge_flush(engine: &dyn ConnectorTestEngine) {
    let dir = TempDir::new().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut endpoint = make_merge_endpoint(uri, DeltaTableWriteMode::Append);

    endpoint.inner.shutdown.cancel();

    endpoint.consumer().batch_start(0, OutputBatchType::Delta);
    let error = endpoint
        .encode(build_insert_batch(engine, &make_records(10)))
        .expect_err("a shutdown must abort the flush");
    assert!(
        error.to_string().contains("shutting down"),
        "unexpected error: {error}"
    );
    assert!(
        read_merge_output(uri).is_empty(),
        "the aborted flush must not have committed any rows"
    );
}

/// A shutdown during the retry backoff ends the write instead of retrying
/// for ever.
///
/// `max_retries: None` is the documented default and an unbounded loop, so
/// `encode` cannot return here on its own: the table directory is
/// read-only, every attempt fails, and only the shutdown ends it.
pub fn a_shutdown_ends_an_unbounded_retry_loop(engine: &dyn ConnectorTestEngine) {
    let table_dir = TempDir::new().unwrap();
    let table_uri = table_dir.path().display().to_string();

    let batch = build_insert_batch(engine, &make_records(10));
    let mut endpoint = make_endpoint_with_retries(
        1,
        &table_uri,
        true,
        DeltaTableWriteMode::Truncate,
        false,
        None,
    );

    // Read-only from here on, so the first attempt fails and every retry
    // after it fails the same way.
    std::fs::set_permissions(table_dir.path(), std::fs::Permissions::from_mode(0o555)).unwrap();

    // Encode on a thread of its own, so that a regression fails this test
    // rather than hanging the whole run.
    let shutdown = endpoint.inner.shutdown.clone();
    let (done, encode_returned) = std::sync::mpsc::channel();
    let encoder = std::thread::spawn(move || {
        endpoint.consumer().batch_start(0, OutputBatchType::Delta);
        let result = endpoint.encode(batch);
        let _ = done.send((result.err(), records_written(&endpoint)));
    });

    // Long enough for the first attempt to fail and the write to reach the
    // one-second backoff, where a shutdown finds it in production.
    std::thread::sleep(std::time::Duration::from_millis(200));
    shutdown.cancel();

    let (error, progress) = encode_returned
        .recv_timeout(std::time::Duration::from_secs(30))
        .expect("a shutdown must end the retry loop");
    encoder.join().unwrap();

    std::fs::set_permissions(table_dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();

    let error = error.expect("an aborted write must report an error");
    assert!(
        error.to_string().contains("shutting down"),
        "unexpected error: {error}"
    );
    assert_eq!(
        progress, 0,
        "an aborted write must leave no progress behind"
    );
}

/// A shutdown ends the commit's retry loop too, not just the write's.
///
/// `batch_end` drives `commit_with_retry` with `TOKIO.block_on` on the
/// controller's output thread, and that loop is unbounded when
/// `max_retries` is `None`.  Data that reached the object store but no
/// `_delta_log` entry hangs the teardown exactly like a failed write.
pub fn a_shutdown_ends_an_unbounded_commit_loop(engine: &dyn ConnectorTestEngine) {
    let table_dir = TempDir::new().unwrap();
    let table_uri = table_dir.path().display().to_string();

    let batch = build_insert_batch(engine, &make_records(10));
    let mut endpoint = make_endpoint_with_retries(
        1,
        &table_uri,
        true,
        DeltaTableWriteMode::Truncate,
        false,
        None,
    );

    // Only the log directory is read-only, so the Parquet files still
    // land and `encode` succeeds; just the commit cannot record them.
    let log_dir = table_dir.path().join("_delta_log");
    let versions_before = count_log_versions(&log_dir);
    std::fs::set_permissions(&log_dir, std::fs::Permissions::from_mode(0o555)).unwrap();

    // Commit on a thread of its own, so that a regression fails this test
    // rather than hanging the whole run.
    let shutdown = endpoint.inner.shutdown.clone();
    let (done, batch_ended) = std::sync::mpsc::channel();
    let committer = std::thread::spawn(move || {
        endpoint.consumer().batch_start(0, OutputBatchType::Delta);
        endpoint
            .encode(batch)
            .expect("the write itself must succeed");
        endpoint.consumer().batch_end();
        let _ = done.send(());
    });

    // Long enough for the first commit attempt to fail and the retry to
    // reach the one-second backoff.
    std::thread::sleep(std::time::Duration::from_millis(200));
    shutdown.cancel();

    batch_ended
        .recv_timeout(std::time::Duration::from_secs(30))
        .expect("a shutdown must end the commit loop");
    committer.join().unwrap();

    std::fs::set_permissions(&log_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert_eq!(
        count_log_versions(&log_dir),
        versions_before,
        "an aborted commit must not add a version to the Delta log"
    );
}

/// How many versions the Delta log records.
fn count_log_versions(log_dir: &Path) -> usize {
    std::fs::read_dir(log_dir)
        .unwrap()
        .filter(|entry| {
            entry
                .as_ref()
                .unwrap()
                .path()
                .extension()
                .is_some_and(|ext| ext == "json")
        })
        .count()
}

pub fn insert_multi_thread(engine: &dyn ConnectorTestEngine) {
    insert_test(engine, 4);
}

/// A batch bigger than `TARGET_FILE_SIZE` must land in more than one Parquet
/// file. delta-rs rolls over only when the writer config carries a target
/// size; without one it writes a single object per key range, which an object
/// store rejects once the multipart upload passes 10000 parts.
///
/// It pins `WRITE_BATCH_ROWS` too: at delta-rs's 8192-row default these
/// 16000 rows are two slices, and the size first passes the target after
/// the last one, so they still land in one file.
pub fn batch_larger_than_target_file_size_rolls_over(engine: &dyn ConnectorTestEngine) {
    const PAYLOAD_LEN: usize = 8 * 1024;
    let table_dir = TempDir::new().unwrap();
    let table_uri = table_dir.path().display().to_string();

    let target = super::TARGET_FILE_SIZE.get() as usize;
    let rows = (target * 5 / 4).div_ceil(PAYLOAD_LEN);
    let records: Vec<Value> = (0..rows)
        .map(|i| {
            let mut row = make_record(i);
            row["string"] = json!(incompressible_string(i as u64, PAYLOAD_LEN));
            row
        })
        .collect();

    let mut endpoint = make_endpoint(1, &table_uri, true);
    encode_batch(&mut endpoint, &build_insert_batch(engine, &records));

    let files = delta::parquet_files(Path::new(&table_uri));
    assert!(
        files.len() > 1,
        "{rows} rows of {PAYLOAD_LEN} bytes each exceed the {target} byte target \
         but landed in {} file(s)",
        files.len()
    );
    // Read through the delta log, so a rolled-over file that never made it
    // into the commit shows up as missing rows instead of being picked up
    // off disk.
    assert_eq!(read_delta_output(&table_uri).len(), rows);
}

/// A slow object store must stall the encoder, not grow memory by the size of the output.
pub fn a_stalled_object_store_stops_the_encoder(engine: &dyn ConnectorTestEngine) {
    const PAYLOAD_LEN: usize = 8 * 1024;
    let table_dir = TempDir::new().unwrap();
    let table_uri = table_dir.path().display().to_string();

    let target = super::TARGET_FILE_SIZE.get() as usize;
    let rows = (target * 5 / 4).div_ceil(PAYLOAD_LEN);
    let records: Vec<Value> = (0..rows)
        .map(|i| {
            let mut row = make_record(i);
            row["string"] = json!(incompressible_string(i as u64, PAYLOAD_LEN));
            row
        })
        .collect();
    let batch = build_insert_batch(engine, &records);

    let mut endpoint = make_endpoint(1, &table_uri, true);
    let (open, gate) = tokio::sync::watch::channel(false);
    let stalled_parts = Arc::new(AtomicUsize::new(0));
    endpoint.object_store = Arc::new(GatedStore {
        inner: endpoint.object_store.clone(),
        gate,
        stalled_parts: stalled_parts.clone(),
    });
    let inner = endpoint.inner.clone();
    let encoder = std::thread::spawn(move || encode_batch(&mut endpoint, &batch));

    // delta-rs keeps at most 10 parts of a file in flight.
    let start = Instant::now();
    while stalled_parts.load(Ordering::Relaxed) < 10 {
        assert!(
            start.elapsed() < Duration::from_secs(60),
            "no upload started"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    // Time for an unbounded encoder to finish the rest of the batch.
    std::thread::sleep(Duration::from_secs(2));
    let encoded = inner.records_written.load(Ordering::Relaxed);
    open.send(true).unwrap();
    encoder.join().unwrap();

    assert!(
        encoded < rows as u64,
        "encoded all {rows} rows while the object store accepted nothing"
    );
    assert_eq!(read_delta_output(&table_uri).len(), rows);
}

/// Holds every multipart part until the gate opens.
#[derive(Debug)]
struct GatedStore {
    inner: ObjectStoreRef,
    gate: tokio::sync::watch::Receiver<bool>,
    stalled_parts: Arc<AtomicUsize>,
}

impl std::fmt::Display for GatedStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GatedStore({})", self.inner)
    }
}

#[derive(Debug)]
struct GatedUpload {
    inner: Box<dyn MultipartUpload>,
    gate: tokio::sync::watch::Receiver<bool>,
    stalled_parts: Arc<AtomicUsize>,
}

#[async_trait]
impl MultipartUpload for GatedUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        self.stalled_parts.fetch_add(1, Ordering::Relaxed);
        let mut gate = self.gate.clone();
        let put = self.inner.put_part(data);
        Box::pin(async move {
            let _ = gate.wait_for(|open| *open).await;
            put.await
        })
    }

    async fn complete(&mut self) -> ObjectStoreResult<PutResult> {
        self.inner.complete().await
    }

    async fn abort(&mut self) -> ObjectStoreResult<()> {
        self.inner.abort().await
    }
}

#[async_trait]
impl ObjectStore for GatedStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        opts: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        Ok(Box::new(GatedUpload {
            inner: self.inner.put_multipart_opts(location, opts).await?,
            gate: self.gate.clone(),
            stalled_parts: self.stalled_parts.clone(),
        }))
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> ObjectStoreResult<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, ObjectStoreResult<ObjectPath>>,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectPath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> ObjectStoreResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        options: CopyOptions,
    ) -> ObjectStoreResult<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// Deterministic printable ASCII that Snappy cannot shrink and that Parquet
/// cannot dictionary-encode away, so the written file size tracks `len`.
fn incompressible_string(seed: u64, len: usize) -> String {
    let mut state = seed | 1;
    let mut bytes = Vec::with_capacity(len + 8);
    while bytes.len() < len {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        bytes.extend((state >> 8).to_le_bytes().map(|b| b' ' + b % 95));
    }
    bytes.truncate(len);
    String::from_utf8(bytes).unwrap()
}

fn upsert_test(engine: &dyn ConnectorTestEngine, threads: usize) {
    let table_dir = TempDir::new().unwrap();
    let table_uri = table_dir.path().display().to_string();

    let records = make_records(50);
    let insert_batch = build_insert_batch(engine, &records);
    let mut endpoint = make_endpoint(threads, &table_uri, true);

    encode_batch(&mut endpoint, &insert_batch);

    // Upsert: update records 0..10
    let updates: Vec<_> = (0..10)
        .map(|i| {
            let old = records[i].clone();
            let new = {
                let mut row = old.clone();
                row["boolean"] = json!(!old["boolean"].as_bool().unwrap());
                row["int"] = json!(old["int"].as_i64().unwrap() + 1000);
                row["string"] = json!(format!("updated_{}", bigint(&old)));
                row
            };
            (old, new)
        })
        .collect();
    let upsert_batch = build_upsert_batch(engine, &updates);
    encode_batch(&mut endpoint, &upsert_batch);

    let output = read_output(&table_uri);
    // First batch: 50 inserts, second batch: 10 upserts
    let inserts: Vec<_> = output.iter().filter(|r| r.__feldera_op == "i").collect();
    let upserts: Vec<_> = output.iter().filter(|r| r.__feldera_op == "u").collect();
    assert_eq!(inserts.len(), 50);
    assert_eq!(upserts.len(), 10);
    // An upsert writes the new row of its key, not the row it replaces.
    let new_rows: Vec<Value> = updates.iter().map(|(_, new)| new.clone()).collect();
    assert_eq!(
        updates_zset(upserts.iter().map(|r| r.to_data_record())),
        values_zset(&new_rows)
    );
}

pub fn upsert_single_thread(engine: &dyn ConnectorTestEngine) {
    upsert_test(engine, 1);
}

pub fn upsert_multi_thread(engine: &dyn ConnectorTestEngine) {
    upsert_test(engine, 4);
}

fn delete_test(engine: &dyn ConnectorTestEngine, threads: usize) {
    let table_dir = TempDir::new().unwrap();
    let table_uri = table_dir.path().display().to_string();

    let records = make_records(50);
    let insert_batch = build_insert_batch(engine, &records);
    let mut endpoint = make_endpoint(threads, &table_uri, true);

    encode_batch(&mut endpoint, &insert_batch);

    // Delete records 0..10
    let to_delete: Vec<_> = records[0..10].to_vec();
    let delete_batch = build_delete_batch(engine, &to_delete);
    encode_batch(&mut endpoint, &delete_batch);

    let output = read_output(&table_uri);
    let inserts: Vec<_> = output.iter().filter(|r| r.__feldera_op == "i").collect();
    let deletes: Vec<_> = output.iter().filter(|r| r.__feldera_op == "d").collect();
    assert_eq!(inserts.len(), 50);
    assert_eq!(deletes.len(), 10);
}

pub fn delete_single_thread(engine: &dyn ConnectorTestEngine) {
    delete_test(engine, 1);
}

pub fn delete_multi_thread(engine: &dyn ConnectorTestEngine) {
    delete_test(engine, 4);
}

fn non_indexed_insert_test(engine: &dyn ConnectorTestEngine, threads: usize) {
    let table_dir = TempDir::new().unwrap();
    let table_uri = table_dir.path().display().to_string();

    let records = make_records(100);
    let batch = build_non_indexed_batch(engine, &records, 1);
    let mut endpoint = make_endpoint(threads, &table_uri, false);

    encode_batch(&mut endpoint, &batch);

    let output = read_output(&table_uri);
    assert_eq!(output.len(), 100);
    for rec in &output {
        assert_eq!(rec.__feldera_op, "i");
    }
    let output_data = updates_zset(output.iter().map(|r| r.to_data_record()));
    assert_eq!(output_data, values_zset(&records));
}

pub fn non_indexed_insert_single_thread(engine: &dyn ConnectorTestEngine) {
    non_indexed_insert_test(engine, 1);
}

fn empty_batch_test(engine: &dyn ConnectorTestEngine, threads: usize) {
    let table_dir = TempDir::new().unwrap();
    let table_uri = table_dir.path().display().to_string();

    let batch = build_insert_batch(engine, &[]);
    let mut endpoint = make_endpoint(threads, &table_uri, true);

    // Should not crash on empty batch.
    encode_batch(&mut endpoint, &batch);

    let output = read_output(&table_uri);
    assert_eq!(output.len(), 0);
}

pub fn empty_batch_single_thread(engine: &dyn ConnectorTestEngine) {
    empty_batch_test(engine, 1);
}

pub fn empty_batch_multi_thread(engine: &dyn ConnectorTestEngine) {
    empty_batch_test(engine, 4);
}

fn multiple_batches_test(engine: &dyn ConnectorTestEngine, threads: usize) {
    let table_dir = TempDir::new().unwrap();
    let table_uri = table_dir.path().display().to_string();

    let mut endpoint = make_endpoint(threads, &table_uri, true);

    // Batch 1: insert 50 records
    let records = make_records(50);
    let insert_batch = build_insert_batch(engine, &records);
    encode_batch(&mut endpoint, &insert_batch);

    // Batch 2: insert 50 more records (ids 50..100)
    let more_records: Vec<Value> = (50..100).map(make_record).collect();
    let insert_batch2 = build_insert_batch(engine, &more_records);
    encode_batch(&mut endpoint, &insert_batch2);

    // Batch 3: upsert records 0..5
    let updates: Vec<_> = (0..5)
        .map(|i| {
            let old = records[i].clone();
            let new = {
                let mut row = old.clone();
                row["boolean"] = json!(!old["boolean"].as_bool().unwrap());
                row["int"] = json!(old["int"].as_i64().unwrap() + 1000);
                row["string"] = json!(format!("updated_{}", bigint(&old)));
                row
            };
            (old, new)
        })
        .collect();
    let upsert_batch = build_upsert_batch(engine, &updates);
    encode_batch(&mut endpoint, &upsert_batch);

    // Batch 4: delete records 90..100
    let to_delete: Vec<_> = more_records[40..50].to_vec();
    let delete_batch = build_delete_batch(engine, &to_delete);
    encode_batch(&mut endpoint, &delete_batch);

    let output = read_output(&table_uri);
    let inserts = output.iter().filter(|r| r.__feldera_op == "i").count();
    let upserts = output.iter().filter(|r| r.__feldera_op == "u").count();
    let deletes = output.iter().filter(|r| r.__feldera_op == "d").count();

    assert_eq!(inserts, 100); // 50 + 50
    assert_eq!(upserts, 5);
    assert_eq!(deletes, 10);
}

pub fn multiple_batches_single_thread(engine: &dyn ConnectorTestEngine) {
    multiple_batches_test(engine, 1);
}

pub fn multiple_batches_multi_thread(engine: &dyn ConnectorTestEngine) {
    multiple_batches_test(engine, 4);
}

/// Write to a read-only directory should fail with no retries.
pub fn write_failure_readonly_dir(engine: &dyn ConnectorTestEngine) {
    let table_dir = TempDir::new().unwrap();
    let table_uri = table_dir.path().display().to_string();

    // Create the endpoint first (needs writable dir to create the table).
    let records = make_records(10);
    let batch = build_insert_batch(engine, &records);
    let mut endpoint = make_endpoint(1, &table_uri, true);

    // Make directory read-only to trigger write failure.
    std::fs::set_permissions(table_dir.path(), std::fs::Permissions::from_mode(0o555)).unwrap();

    endpoint.consumer().batch_start(0, OutputBatchType::Delta);
    let result = endpoint.encode(batch);

    // Restore permissions before asserting (so TempDir cleanup succeeds).
    std::fs::set_permissions(table_dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();

    assert!(result.is_err(), "write to read-only dir should fail");
}

/// Exhausting max_retries should propagate the error.
pub fn retry_exhaustion(engine: &dyn ConnectorTestEngine) {
    let table_dir = TempDir::new().unwrap();
    let table_uri = table_dir.path().display().to_string();

    let records = make_records(10);
    let batch = build_insert_batch(engine, &records);

    // Create endpoint with max_retries=1.
    let key_schema = Some(key_relation());
    let mut endpoint = DeltaTableWriter::new(
        EndpointId::default(),
        "test_endpoint",
        &DeltaTableWriterConfig {
            variant_encoding: Default::default(),
            uri: table_uri.clone(),
            mode: DeltaTableWriteMode::Truncate,
            max_retries: Some(1),
            threads: Some(1),
            auto_compact: false,

            object_store_config: Default::default(),
            checkpoint_interval: None,
            log_retention_duration: None,
            update_mode: Default::default(),
            lookup_chunk_size: feldera_types::size::ConfigSize::from_mib(1),
            max_concurrent_probes: 4,
            enable_expired_log_cleanup: None,
        },
        &key_schema,
        &value_relation(),
        Weak::new(),
        CancellationToken::new(),
        false,
        true,
        true,
    )
    .expect("failed to create endpoint");

    // Make directory read-only to trigger write failure.
    std::fs::set_permissions(table_dir.path(), std::fs::Permissions::from_mode(0o555)).unwrap();

    endpoint.consumer().batch_start(0, OutputBatchType::Delta);
    let result = endpoint.encode(batch);

    // Restore permissions.
    std::fs::set_permissions(table_dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();

    assert!(result.is_err(), "should fail after exhausting retries");
}

pub(super) fn records_written(endpoint: &DeltaTableWriter) -> u64 {
    endpoint.inner.records_written.load(Ordering::Relaxed)
}

/// Merge mode must report progress the way cdc does, or a long flush looks idle.
///
/// Both sides count: a flush that only supersedes rows appends nothing, and reporting
/// just the appends would show no progress at all while it works.
pub fn merge_reports_the_rows_it_writes(engine: &dyn ConnectorTestEngine) {
    let table_dir = TempDir::new().unwrap();
    let table_uri = table_dir.path().display().to_string();
    let mut endpoint = make_merge_endpoint(&table_uri, DeltaTableWriteMode::Append);

    let records = make_records(10);
    assert_eq!(records_written(&endpoint), 0);
    endpoint.consumer().batch_start(0, OutputBatchType::Delta);
    endpoint
        .encode(build_insert_batch(engine, &records))
        .unwrap();
    assert_eq!(records_written(&endpoint), 10, "ten rows appended");
    endpoint.consumer().batch_end();
    assert_eq!(records_written(&endpoint), 0);

    // Deleting supersedes the row in the table without appending one.
    endpoint.consumer().batch_start(1, OutputBatchType::Delta);
    endpoint
        .encode(build_delete_batch(engine, &records[..4]))
        .unwrap();
    assert_eq!(
        records_written(&endpoint),
        4,
        "a delete-only flush must report the rows it tombstoned"
    );
    endpoint.consumer().batch_end();
    assert_eq!(records_written(&endpoint), 0);
}

pub fn progress_counter_single_thread(engine: &dyn ConnectorTestEngine) {
    let table_dir = TempDir::new().unwrap();
    let table_uri = table_dir.path().display().to_string();

    let records = make_records(100);
    let batch = build_insert_batch(engine, &records);
    let mut endpoint = make_endpoint(1, &table_uri, true);

    assert_eq!(records_written(&endpoint), 0);
    endpoint.consumer().batch_start(0, OutputBatchType::Delta);
    endpoint.encode(batch.clone()).unwrap();
    assert_eq!(records_written(&endpoint), 100);
    endpoint.consumer().batch_end();
    assert_eq!(records_written(&endpoint), 0);
}

pub fn progress_counter_multi_thread(engine: &dyn ConnectorTestEngine) {
    let table_dir = TempDir::new().unwrap();
    let table_uri = table_dir.path().display().to_string();

    let records = make_records(100);
    let batch = build_insert_batch(engine, &records);
    let mut endpoint = make_endpoint(4, &table_uri, true);

    endpoint.consumer().batch_start(0, OutputBatchType::Delta);
    endpoint.encode(batch.clone()).unwrap();
    assert_eq!(records_written(&endpoint), 100);
    endpoint.consumer().batch_end();
    assert_eq!(records_written(&endpoint), 0);
}

pub fn progress_counter_empty_batch(engine: &dyn ConnectorTestEngine) {
    let table_dir = TempDir::new().unwrap();
    let table_uri = table_dir.path().display().to_string();

    let batch = build_insert_batch(engine, &[]);
    let mut endpoint = make_endpoint(1, &table_uri, true);

    endpoint.consumer().batch_start(0, OutputBatchType::Delta);
    endpoint.encode(batch.clone()).unwrap();
    assert_eq!(records_written(&endpoint), 0);
    endpoint.consumer().batch_end();
    assert_eq!(records_written(&endpoint), 0);
}

pub fn progress_counter_multiple_batches(engine: &dyn ConnectorTestEngine) {
    let table_dir = TempDir::new().unwrap();
    let table_uri = table_dir.path().display().to_string();

    let mut endpoint = make_endpoint(1, &table_uri, true);

    // Batch 1: 50 records.
    let records1 = make_records(50);
    let batch1 = build_insert_batch(engine, &records1);
    endpoint.consumer().batch_start(0, OutputBatchType::Delta);
    endpoint.encode(batch1.clone()).unwrap();
    assert_eq!(records_written(&endpoint), 50);
    endpoint.consumer().batch_end();
    assert_eq!(records_written(&endpoint), 0);

    // Batch 2: 30 records (ids 50..80).
    let records2: Vec<_> = (50..80).map(make_record).collect();
    let batch2 = build_insert_batch(engine, &records2);
    endpoint.consumer().batch_start(1, OutputBatchType::Delta);
    endpoint.encode(batch2.clone()).unwrap();
    assert_eq!(records_written(&endpoint), 30);
    endpoint.consumer().batch_end();
    assert_eq!(records_written(&endpoint), 0);
}

pub fn progress_counter_resets_on_batch_start(engine: &dyn ConnectorTestEngine) {
    let table_dir = TempDir::new().unwrap();
    let table_uri = table_dir.path().display().to_string();

    let records = make_records(50);
    let batch = build_insert_batch(engine, &records);
    let mut endpoint = make_endpoint(1, &table_uri, true);

    endpoint.consumer().batch_start(0, OutputBatchType::Delta);
    endpoint.encode(batch.clone()).unwrap();
    assert_eq!(records_written(&endpoint), 50);

    // batch_start without batch_end resets the counter.
    endpoint.consumer().batch_start(1, OutputBatchType::Delta);
    assert_eq!(records_written(&endpoint), 0);
}

pub fn progress_counter_resets_on_write_failure(engine: &dyn ConnectorTestEngine) {
    let table_dir = TempDir::new().unwrap();
    let table_uri = table_dir.path().display().to_string();

    let records = make_records(10);
    let batch = build_insert_batch(engine, &records);
    let mut endpoint = make_endpoint(1, &table_uri, true);

    // Make directory read-only to trigger write failure.
    std::fs::set_permissions(table_dir.path(), std::fs::Permissions::from_mode(0o555)).unwrap();

    endpoint.consumer().batch_start(0, OutputBatchType::Delta);
    let result = endpoint.encode(batch);

    // Restore permissions before asserting.
    std::fs::set_permissions(table_dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();

    assert!(result.is_err());
    // encode() resets progress to 0 on failure.
    assert_eq!(records_written(&endpoint), 0);
}

pub fn progress_counter_resets_after_retries(engine: &dyn ConnectorTestEngine) {
    let table_dir = TempDir::new().unwrap();
    let table_uri = table_dir.path().display().to_string();

    let records = make_records(10);
    let batch = build_insert_batch(engine, &records);

    let key_schema = Some(key_relation());
    let mut endpoint = DeltaTableWriter::new(
        EndpointId::default(),
        "test_endpoint",
        &DeltaTableWriterConfig {
            variant_encoding: Default::default(),
            uri: table_uri.clone(),
            mode: DeltaTableWriteMode::Truncate,
            max_retries: Some(1),
            threads: Some(1),
            auto_compact: false,

            object_store_config: Default::default(),
            checkpoint_interval: None,
            log_retention_duration: None,
            update_mode: Default::default(),
            lookup_chunk_size: feldera_types::size::ConfigSize::from_mib(1),
            max_concurrent_probes: 4,
            enable_expired_log_cleanup: None,
        },
        &key_schema,
        &value_relation(),
        Weak::new(),
        CancellationToken::new(),
        false,
        true,
        true,
    )
    .expect("failed to create endpoint");

    // Make directory read-only to trigger write failure with retries.
    std::fs::set_permissions(table_dir.path(), std::fs::Permissions::from_mode(0o555)).unwrap();

    endpoint.consumer().batch_start(0, OutputBatchType::Delta);
    let result = endpoint.encode(batch);

    // Restore permissions.
    std::fs::set_permissions(table_dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();

    assert!(result.is_err(), "should fail after exhausting retries");
    // encode() resets progress to 0 after all ranges fail.
    assert_eq!(records_written(&endpoint), 0);
}

pub fn progress_large_batch_increments(engine: &dyn ConnectorTestEngine) {
    let table_dir = TempDir::new().unwrap();
    let table_uri = table_dir.path().display().to_string();

    // Use > CHUNK_SIZE records to trigger multiple flush_chunk calls.
    let num = super::CHUNK_SIZE + 500;
    let records = make_records(num);
    let batch = build_insert_batch(engine, &records);
    let mut endpoint = make_endpoint(1, &table_uri, true);

    endpoint.consumer().batch_start(0, OutputBatchType::Delta);
    endpoint.encode(batch.clone()).unwrap();
    assert_eq!(records_written(&endpoint), num as u64);

    // Verify data was written correctly.
    endpoint.consumer().batch_end();
    assert_eq!(records_written(&endpoint), 0);
    let output = read_output(&table_uri);
    assert_eq!(output.len(), num);
}

/// Simulate a pipeline restart: drop the first endpoint, create a new one
/// on the same table with `continue_previous_state=true`. Data written before the
/// restart must survive.
pub fn truncate_preserves_data_across_restart(engine: &dyn ConnectorTestEngine) {
    let table_dir = TempDir::new().unwrap();
    let table_uri = table_dir.path().display().to_string();

    // First start: write 50 records.
    let records = make_records(50);
    let batch = build_insert_batch(engine, &records);
    {
        let mut endpoint =
            make_endpoint_ex(1, &table_uri, true, DeltaTableWriteMode::Truncate, false);
        encode_batch(&mut endpoint, &batch);
    }

    assert_eq!(read_delta_output(&table_uri).len(), 50);

    // Restart: create a new endpoint with continue_previous_state=true.
    let more_records: Vec<Value> = (50..80).map(make_record).collect();
    let batch2 = build_insert_batch(engine, &more_records);
    {
        let mut endpoint =
            make_endpoint_ex(1, &table_uri, true, DeltaTableWriteMode::Truncate, true);
        encode_batch(&mut endpoint, &batch2);
    }

    // All 80 records (50 original + 30 new) must be present in the
    // delta log.  read_delta_output reads through the log, so orphaned
    // parquet files left by SaveMode::Overwrite are not counted.
    let output = read_delta_output(&table_uri);
    assert_eq!(output.len(), 80);
}

/// First-start truncation still clears pre-existing data.
pub fn truncate_clears_data_on_first_start(engine: &dyn ConnectorTestEngine) {
    let table_dir = TempDir::new().unwrap();
    let table_uri = table_dir.path().display().to_string();

    // Seed the table via a first endpoint (simulates pre-existing data).
    let records = make_records(50);
    let batch = build_insert_batch(engine, &records);
    {
        let mut endpoint =
            make_endpoint_ex(1, &table_uri, true, DeltaTableWriteMode::Truncate, false);
        encode_batch(&mut endpoint, &batch);
    }
    assert_eq!(read_output(&table_uri).len(), 50);

    // New pipeline start (continue_previous_state=false) with truncate: old data is wiped.
    let new_records: Vec<Value> = (100..110).map(make_record).collect();
    let batch2 = build_insert_batch(engine, &new_records);
    {
        let mut endpoint =
            make_endpoint_ex(1, &table_uri, true, DeltaTableWriteMode::Truncate, false);
        encode_batch(&mut endpoint, &batch2);
    }

    // Only the 10 new records should be present in the delta table snapshot.
    // (Orphaned parquet files from the truncated table may still exist on
    // disk, so we read through the delta log rather than scanning all files.)
    let output = read_delta_output(&table_uri);
    assert_eq!(output.len(), 10);
}

/// `error_if_exists` mode must not fail on restart.
pub fn error_if_exists_succeeds_on_restart(engine: &dyn ConnectorTestEngine) {
    let table_dir = TempDir::new().unwrap();
    let table_uri = table_dir.path().display().to_string();

    // First start: create table.
    {
        let mut endpoint = make_endpoint_ex(
            1,
            &table_uri,
            true,
            DeltaTableWriteMode::ErrorIfExists,
            false,
        );
        let batch = build_insert_batch(engine, &make_records(10));
        encode_batch(&mut endpoint, &batch);
    }

    // Restart: should open the existing table without error.
    let endpoint = make_endpoint_ex(
        1,
        &table_uri,
        true,
        DeltaTableWriteMode::ErrorIfExists,
        true,
    );
    drop(endpoint);
}

/// Merge mode keyed on `int`.
pub fn merge_keyed_on_int(engine: &dyn ConnectorTestEngine) {
    merge_key_type_case(engine, "int");
}

/// Merge mode keyed on `string`.
pub fn merge_keyed_on_string(engine: &dyn ConnectorTestEngine) {
    merge_key_type_case(engine, "string");
}

/// Merge mode keyed on `binary`.
pub fn merge_keyed_on_binary(engine: &dyn ConnectorTestEngine) {
    merge_key_type_case(engine, "binary");
}

/// Merge mode keyed on `timestamp_ntz`.
pub fn merge_keyed_on_timestamp(engine: &dyn ConnectorTestEngine) {
    merge_key_type_case(engine, "timestamp_ntz");
}

/// Merge mode keyed on `decimal_10_3`.
pub fn merge_keyed_on_decimal(engine: &dyn ConnectorTestEngine) {
    merge_key_type_case(engine, "decimal_10_3");
}

/// Merge mode keyed on `uuid`.
pub fn merge_keyed_on_uuid(engine: &dyn ConnectorTestEngine) {
    merge_key_type_case(engine, "uuid");
}

/// Merge mode keyed on `date`.
pub fn merge_keyed_on_date(engine: &dyn ConnectorTestEngine) {
    merge_key_type_case(engine, "date");
}

/// Merge mode keyed on `struct1`.
pub fn merge_keyed_on_row(engine: &dyn ConnectorTestEngine) {
    merge_key_type_case(engine, "struct1");
}

/// Merge mode keyed on `double`.
pub fn merge_keyed_on_double(engine: &dyn ConnectorTestEngine) {
    merge_key_type_case(engine, "double");
}

/// Merge mode keyed on `float`.
pub fn merge_keyed_on_float(engine: &dyn ConnectorTestEngine) {
    merge_key_type_case(engine, "float");
}

/// Merge mode keyed on `tinyint`.
pub fn merge_keyed_on_tinyint(engine: &dyn ConnectorTestEngine) {
    merge_key_type_case(engine, "tinyint");
}

/// Merge mode keyed on `smallint`.
pub fn merge_keyed_on_smallint(engine: &dyn ConnectorTestEngine) {
    merge_key_type_case(engine, "smallint");
}
