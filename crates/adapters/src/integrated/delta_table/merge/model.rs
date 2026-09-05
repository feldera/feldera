//! Model-based test: random operation sequences against an in-memory oracle.
//!
//! The per-mechanism tests each pin one property of one stage. What they cannot cover is how
//! the stages interact over a long sequence, where the table at flush `n` is whatever the
//! previous flushes left behind: a key updated repeatedly, deleted and reinserted, replayed
//! after a restart.
//!
//! So this drives the real writer with a random sequence and compares the table against a
//! `HashMap` after every flush, covering sequences nobody thought to write down. Seeds are
//! fixed, so a failure reproduces exactly.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::compute::kernels::cast::cast;
use arrow::datatypes::DataType;
use dbsp::OrdIndexedZSet;
use dbsp::utils::Tup2;
use deltalake::datafusion::prelude::SessionContext;
use deltalake::operations::create::CreateBuilder;
use deltalake::protocol::SaveMode;
use deltalake::{DeltaTable, TableProperty};
use feldera_adapterlib::catalog::SerBatch;
use feldera_macros::IsNone;
use feldera_types::program_schema::SqlIdentifier;
use feldera_types::transport::delta_table::DeltaVariantEncoding;
use feldera_types::{deserialize_without_context, serialize_struct};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use serde_arrow::schema::SerdeArrowSchema;
use size_of::SizeOf;
use tempfile::TempDir;

use super::flush::MergeWriter;
use super::startup::{Regime, prepare};
use super::test::{arrow_schema, fixture_columns, key_relation};
use crate::catalog::RecordFormat;
use crate::integrated::delta_table::output::delta_output_serde_config;
use crate::static_compile::seroutput::SerBatchImpl;

// The record and key types the circuit would hand the connector. `id` is the key, `payload`
// the value that an update changes.
#[derive(
    Debug,
    Default,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    SizeOf,
    serde::Serialize,
    serde::Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    IsNone,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
pub struct Row {
    id: i64,
    payload: String,
}

deserialize_without_context!(Row);
serialize_struct!(Row()[2]{
    id["id"]: i64,
    payload["payload"]: String
});

#[derive(
    Debug,
    Default,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    SizeOf,
    serde::Serialize,
    serde::Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    IsNone,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
pub struct Key {
    id: i64,
}

deserialize_without_context!(Key);
serialize_struct!(Key()[1]{
    id["id"]: i64
});

/// One key's change, as the indexed output would present it.
#[derive(Debug, Clone, PartialEq)]
enum Change {
    Insert(i64, String),
    /// Old payload, then new: a delete and an insert of one key in one batch.
    Update(i64, String, String),
    Delete(i64, String),
}

/// A batch of changes, as an indexed Z-set of `(key, row)` pairs with weights.
fn build_batch(changes: &[Change]) -> Arc<dyn SerBatch> {
    let mut tuples = Vec::new();
    let mut push = |id: i64, payload: &str, weight: i64| {
        tuples.push(Tup2(
            Tup2(
                Key { id },
                Row {
                    id,
                    payload: payload.to_string(),
                },
            ),
            weight,
        ));
    };

    for change in changes {
        match change {
            Change::Insert(id, payload) => push(*id, payload, 1),
            Change::Delete(id, payload) => push(*id, payload, -1),
            Change::Update(id, old, new) => {
                push(*id, old, -1);
                push(*id, new, 1);
            }
        }
    }

    let zset = OrdIndexedZSet::from_tuples((), tuples);
    Arc::new(SerBatchImpl::<_, Key, Row>::new(zset))
}

/// Every live row in the table, keyed by `id`. Read through delta-rs's own reader, so
/// deletion vectors are applied the way any reader would apply them.
async fn live_rows(table: &DeltaTable) -> HashMap<i64, String> {
    let ctx = SessionContext::new();
    ctx.register_table("t", table.table_provider().await.unwrap())
        .unwrap();
    let batches = ctx
        .sql("select id, payload from t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let mut rows = HashMap::new();
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        // Cast rather than downcast: datafusion may hand back a string view rather than a
        // plain string array, and which one is not this test's business.
        let payloads = cast(batch.column(1), &DataType::Utf8).unwrap();
        let payloads = payloads.as_any().downcast_ref::<StringArray>().unwrap();
        for i in 0..batch.num_rows() {
            let id = ids.value(i);
            if let Some(previous) = rows.insert(id, payloads.value(i).to_string()) {
                panic!(
                    "two live rows for key {id}: {previous:?} and {:?}",
                    payloads.value(i)
                );
            }
        }
    }
    rows
}

/// Apply one batch through the real writer, failing on a uniqueness violation.
async fn apply(writer: &MergeWriter, table: &mut DeltaTable, changes: &[Change]) {
    apply_attempt(writer, table, changes, false).await
}

/// The same, choosing whether the writer treats this as a retry of an earlier attempt.
async fn apply_attempt(
    writer: &MergeWriter,
    table: &mut DeltaTable,
    changes: &[Change],
    retrying: bool,
) {
    let batch = build_batch(changes);
    let format = RecordFormat::Parquet(delta_output_serde_config(DeltaVariantEncoding::default()));
    let mut cursor = batch.cursor(format).unwrap();
    let object_store = table.object_store();

    writer
        .flush(table, object_store, &mut *cursor, retrying, &mut |e| {
            panic!("unexpected uniqueness violation: {e}")
        })
        .await
        .unwrap();
}

async fn create_table(dir: &TempDir) -> DeltaTable {
    CreateBuilder::new()
        .with_location(dir.path().to_str().unwrap())
        .with_save_mode(SaveMode::Ignore)
        .with_columns(fixture_columns())
        .with_configuration_property(TableProperty::EnableDeletionVectors, Some("true"))
        .await
        .unwrap()
}

/// Build a writer against `table`, the way a starting pipeline would. The replay test calls
/// it a second time, since building a fresh writer is what a restart does.
fn writer_for(table: &DeltaTable) -> (MergeWriter, Regime) {
    let setup = prepare(table, &Some(key_relation()), &fixture_columns(), 1).unwrap();
    let regime = setup.regime;
    let schema = Arc::new(arrow_schema());
    let writer = MergeWriter::new(
        setup,
        &key_relation(),
        SerdeArrowSchema::try_from(schema.fields().as_ref()).unwrap(),
        schema,
        1 << 20,
        1,
        SqlIdentifier::new("v", false),
    )
    .unwrap();

    (writer, regime)
}

/// The oracle: what the view holds, which is what the table must hold.
#[derive(Default)]
struct Model {
    rows: HashMap<i64, String>,
    version: u64,
}

impl Model {
    /// A batch of changes drawn from the model's own state, so every delete and update
    /// names a row that is really there -- which is what an indexed view emits.
    fn next_batch(&mut self, rng: &mut SmallRng, key_space: i64) -> Vec<Change> {
        let mut changes = Vec::new();
        let mut touched = Vec::new();

        for _ in 0..rng.gen_range(1..6) {
            let live: Vec<i64> = self.rows.keys().copied().collect();
            let choice = rng.gen_range(0..100);

            // Deletes and updates need a live key; with none, only an insert is possible.
            let change = if live.is_empty() || choice < 40 {
                let id = rng.gen_range(0..key_space);
                if self.rows.contains_key(&id) {
                    continue;
                }
                self.version += 1;
                Change::Insert(id, format!("v{}", self.version))
            } else if choice < 80 {
                let id = live[rng.gen_range(0..live.len())];
                self.version += 1;
                Change::Update(id, self.rows[&id].clone(), format!("v{}", self.version))
            } else {
                let id = live[rng.gen_range(0..live.len())];
                Change::Delete(id, self.rows[&id].clone())
            };

            // One key may appear at most once per batch: two changes to one key in one
            // batch is the uniqueness violation the connector reports, not a valid input.
            let id = match &change {
                Change::Insert(id, _) | Change::Update(id, ..) | Change::Delete(id, _) => *id,
            };
            if touched.contains(&id) {
                continue;
            }
            touched.push(id);
            self.apply(&change);
            changes.push(change);
        }

        changes
    }

    fn apply(&mut self, change: &Change) {
        match change {
            Change::Insert(id, payload) => {
                self.rows.insert(*id, payload.clone());
            }
            Change::Update(id, _, new) => {
                self.rows.insert(*id, new.clone());
            }
            Change::Delete(id, _) => {
                self.rows.remove(id);
            }
        }
    }
}

/// The table must equal the view after every flush of a random sequence.
///
/// Several short seeds rather than one long sequence: a short failing sequence is easier to
/// read, and independent seeds explore more shapes than one long walk.
#[tokio::test]
async fn the_table_tracks_the_view_across_random_sequences() {
    for seed in 0..6u64 {
        let dir = TempDir::new().unwrap();
        let mut table = create_table(&dir).await;
        let (writer, _) = writer_for(&table);
        let mut rng = SmallRng::seed_from_u64(seed);
        let mut model = Model::default();

        // A small key space relative to the flush count, so keys are revisited: repeated
        // updates and delete-then-reinsert are the interesting sequences.
        for round in 0..12 {
            let changes = model.next_batch(&mut rng, 20);
            if changes.is_empty() {
                continue;
            }
            apply(&writer, &mut table, &changes).await;

            assert_eq!(
                live_rows(&table).await,
                model.rows,
                "seed {seed}, round {round}: table diverged from the view after {changes:?}"
            );
        }
    }
}

/// Replaying a batch after a restart must leave the table where it was.
///
/// The connector is not fault tolerant, so a restart can re-apply the last batch. A Feldera
/// delta states the final value per key, so re-applying one tombstones the row it appended and
/// appends an identical one.
///
/// The restart must be modelled as one. A writer built against an empty table runs in
/// `Regime::Owned` and skips the lookup for inserts, so replaying through *that* writer would
/// append a second live row. Convergence rests on the regime being re-derived at startup.
#[tokio::test]
async fn replaying_a_batch_converges_across_a_restart() {
    let dir = TempDir::new().unwrap();
    let mut table = create_table(&dir).await;
    let mut rng = SmallRng::seed_from_u64(99);
    let mut model = Model::default();

    for round in 0..8 {
        let changes = model.next_batch(&mut rng, 12);
        if changes.is_empty() {
            continue;
        }

        let (writer, _) = writer_for(&table);
        apply(&writer, &mut table, &changes).await;
        assert_eq!(
            live_rows(&table).await,
            model.rows,
            "round {round}: first application"
        );

        // The restart: a new writer over the table as it now stands, then the same batch.
        let (restarted, regime) = writer_for(&table);
        assert_eq!(
            regime,
            Regime::Default,
            "round {round}: a restart over a non-empty table must look up inserts"
        );
        apply(&restarted, &mut table, &changes).await;

        assert_eq!(
            live_rows(&table).await,
            model.rows,
            "round {round}: replaying {changes:?} after a restart did not converge"
        );
    }
}

/// A retried batch must converge even in `Regime::Owned`.
///
/// A commit error can mean the commit landed and only its response was lost, so the retry
/// re-walks a batch whose rows are already in the table. Nothing outside the writer can tell
/// the two apart, so the retry must look up the row an insert supersedes even though the
/// table was empty when the writer opened it. Skipping it leaves two live rows for one key,
/// which `live_rows` panics on.
#[tokio::test]
async fn retrying_a_landed_batch_converges_in_the_owned_regime() {
    let dir = TempDir::new().unwrap();
    let mut table = create_table(&dir).await;
    let (writer, regime) = writer_for(&table);
    assert_eq!(
        regime,
        Regime::Owned,
        "the fixture must exercise the insert shortcut"
    );

    let mut rng = SmallRng::seed_from_u64(7);
    let mut model = Model::default();

    for round in 0..8 {
        let changes = model.next_batch(&mut rng, 12);
        if changes.is_empty() {
            continue;
        }

        // The attempt whose commit landed, and the retry that cannot know it did.
        apply(&writer, &mut table, &changes).await;
        apply_attempt(&writer, &mut table, &changes, true).await;

        assert_eq!(
            live_rows(&table).await,
            model.rows,
            "round {round}: retrying {changes:?} did not converge"
        );
    }
}
