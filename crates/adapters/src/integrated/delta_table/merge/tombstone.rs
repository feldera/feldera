//! Tombstoning superseded rows with Delta deletion vectors.
//!
//! A row is superseded by appending its new version and marking the old one deleted in the
//! file's vector. The protocol expresses that as a `remove` of the file paired with an `add`
//! of the same path carrying the new vector; log replay keys files by (path, vector id), so
//! the pair is unambiguous.
//!
//! Every vector a flush touches is packed into one object at the table root, named
//! `deletion_vector_<uuid>.bin` as Delta Spark names it. Packing keeps the number of objects
//! written independent of the number of files touched. VACUUM reclaims them like any other
//! file the log stops referencing.
//!
//! Caveat: delta-rs `VacuumMode::Full` deletes these as orphans, because a vector is named
//! only inside a descriptor, which resurrects every row they tombstoned. The fix is written
//! but not in the pinned rev, so it is still live: use `Lite`, the default, which deletes
//! only what an expired `remove` names, or Spark, which gets full mode right.

use std::collections::BTreeMap;

use anyhow::{Result as AnyResult, anyhow};
use delta_kernel::actions::deletion_vector_writer::{
    KernelDeletionVector, StreamingDeletionVectorWriter,
};
use deltalake::kernel::{Action, Add, DeletionVectorDescriptor, StorageType};
use deltalake::logstore::object_store::ObjectStoreExt as _;
use deltalake::{DeltaTable, Path};
use roaring::RoaringTreemap;
use uuid::Uuid;

use super::super::deletion_vector::read_deletion_vector;

/// Physical row ordinals to tombstone, grouped by data file.
///
/// Ordinals ignore any existing deletion vector, which is the space a vector addresses.
#[derive(Debug, Default)]
pub struct Tombstones {
    files: BTreeMap<String, RoaringTreemap>,
}

impl Tombstones {
    pub fn new() -> Self {
        Self::default()
    }

    /// Mark one row. Marking a row already marked in this flush is a no-op.
    pub fn insert(&mut self, path: &str, ordinal: u64) {
        match self.files.get_mut(path) {
            Some(bitmap) => bitmap.insert(ordinal),
            None => self
                .files
                .entry(path.to_string())
                .or_default()
                .insert(ordinal),
        };
    }

    pub fn is_empty(&self) -> bool {
        self.files.is_empty()
    }

    #[cfg(test)]
    pub fn touched_files(&self) -> usize {
        self.files.len()
    }

    #[cfg(test)]
    pub fn total_rows(&self) -> u64 {
        self.files.values().map(|b| b.len()).sum()
    }

    #[cfg(test)]
    pub fn ordinals_for(&self, path: &str) -> Option<&RoaringTreemap> {
        self.files.get(path)
    }
}

/// Name of the object holding a vector, relative to the table root. A reader derives it from
/// the UUID in the descriptor, so this and `path_or_inline_dv` must use the same UUID.
fn dv_object_name(uuid: &Uuid) -> String {
    format!("deletion_vector_{uuid}.bin")
}

/// What one call to [`write_deletion_vectors`] produced, for metrics and logging.
#[derive(Debug, Default, Clone, Copy)]
pub struct DvWriteMetrics {
    /// Data files whose deletion vector was replaced.
    pub files_touched: usize,
    /// Data files dropped whole because every row in them is now tombstoned.
    pub files_dropped: usize,
    /// Rows this flush newly tombstoned.
    pub rows_tombstoned: u64,
    /// Size of the packed deletion vector object.
    pub dv_bytes: usize,
}

/// The log actions that install a flush's tombstones, plus what it took to build them.
pub struct DvWrite {
    /// `remove`/`add` pairs to commit, and lone `remove`s for files dropped whole.
    pub actions: Vec<Action>,
    pub metrics: DvWriteMetrics,
}

/// Install `tombstones` into the table, returning the actions that do so.
///
/// Per file: read the existing vector, union in the new ordinals, append the result to the
/// packed object. One file's bitmap is live at a time.
///
/// A file whose every row is now tombstoned is removed outright rather than re-added with a
/// full vector, which keeps a delete-heavy workload from growing the file count without
/// bound. That needs the file's row count, so a file without statistics keeps its vector.
pub async fn write_deletion_vectors(
    tombstones: &Tombstones,
    table: &DeltaTable,
) -> AnyResult<DvWrite> {
    if tombstones.is_empty() {
        return Ok(DvWrite {
            actions: Vec::new(),
            metrics: DvWriteMetrics::default(),
        });
    }

    let live = resolve_live_files(tombstones, table)?;

    let uuid = Uuid::new_v4();
    let dv_path = Path::from(dv_object_name(&uuid));
    // `path_or_inline_dv` is an optional directory prefix plus the z85-encoded UUID. No
    // prefix here, so it is the UUID alone. Built by hand because the kernel's own
    // `DeletionVectorPath` has no public constructor, so `to_descriptor` is out of reach.
    let encoded_path = z85::encode(uuid.as_bytes());

    let mut buffer: Vec<u8> = Vec::new();
    let mut writer = StreamingDeletionVectorWriter::new(&mut buffer);
    let mut actions = Vec::with_capacity(tombstones.files.len() * 2);
    let mut metrics = DvWriteMetrics::default();

    for (path, new_ordinals) in &tombstones.files {
        let add = live.get(path).ok_or_else(|| {
            anyhow!(
                "data file '{path}' is no longer in the Delta table snapshot; \
                 a concurrent maintenance job replaced it and the lookup must be redone"
            )
        })?;

        let mut bitmap = match &add.deletion_vector {
            Some(existing) => read_deletion_vector(existing, table).await?,
            None => RoaringTreemap::new(),
        };
        let before = bitmap.len();
        bitmap |= new_ordinals;
        metrics.rows_tombstoned += bitmap.len() - before;

        if Some(bitmap.len()) == physical_rows(add) {
            actions.push(Action::Remove(remove_for(add)));
            metrics.files_dropped += 1;
            continue;
        }

        let mut dv = KernelDeletionVector::new();
        dv.add_deleted_row_indexes(&bitmap);
        drop(bitmap);

        let result = writer
            .write_deletion_vector(dv)
            .map_err(|e| anyhow!("error serializing deletion vector for '{path}': {e}"))?;

        actions.push(Action::Remove(remove_for(add)));
        actions.push(Action::Add(Add {
            deletion_vector: Some(DeletionVectorDescriptor {
                storage_type: StorageType::UuidRelativePath,
                path_or_inline_dv: encoded_path.clone(),
                offset: Some(result.offset),
                size_in_bytes: result.size_in_bytes,
                cardinality: result.cardinality,
            }),
            data_change: true,
            ..add.clone()
        }));
        metrics.files_touched += 1;
    }

    writer
        .finalize()
        .map_err(|e| anyhow!("error finalizing deletion vector file: {e}"))?;

    if metrics.files_touched == 0 {
        // Every touched file was dropped whole, so the packed object holds no vector.
        return Ok(DvWrite { actions, metrics });
    }

    metrics.dv_bytes = buffer.len();
    let payload = bytes::Bytes::from(buffer);
    table
        .object_store()
        .put(&dv_path, payload.into())
        .await
        .map_err(|e| anyhow!("error writing deletion vector file '{dv_path}': {e}"))?;

    Ok(DvWrite { actions, metrics })
}

/// The `add` action of every file `tombstones` names, resolved against the snapshot.
///
/// Done before anything is written, so a flush naming a file that OPTIMIZE has since replaced
/// fails without leaving a deletion vector object behind.
fn resolve_live_files(
    tombstones: &Tombstones,
    table: &DeltaTable,
) -> AnyResult<BTreeMap<String, Add>> {
    let state = table
        .snapshot()
        .map_err(|e| anyhow!("Delta table has no snapshot to tombstone rows in: {e}"))?;

    let mut live = BTreeMap::new();
    for view in state.log_data() {
        let path = view.path().to_string();
        if tombstones.files.contains_key(&path) {
            // The re-added file keeps its size, partition values and statistics; only the
            // deletion vector changes. The suggested replacement yields no re-committable `Add`.
            #[allow(deprecated)]
            live.insert(path, view.add_action());
        }
    }
    Ok(live)
}

/// Rows physically in the file, ignoring any deletion vector, or `None` without statistics.
fn physical_rows(add: &Add) -> Option<u64> {
    add.get_stats()
        .ok()
        .flatten()
        .map(|stats| stats.num_records as u64)
}

/// The `remove` half of a deletion vector update.
///
/// It carries the file's *current* vector: a file is identified by path plus vector id, so
/// without it log replay cannot tell which version is being removed.
fn remove_for(add: &Add) -> deltalake::kernel::Remove {
    deltalake::kernel::Remove {
        path: add.path.clone(),
        deletion_timestamp: Some(chrono::Utc::now().timestamp_millis()),
        data_change: true,
        extended_file_metadata: Some(true),
        partition_values: Some(add.partition_values.clone()),
        size: Some(add.size),
        deletion_vector: add.deletion_vector.clone(),
        tags: add.tags.clone(),
        base_row_id: add.base_row_id,
        default_row_commit_version: add.default_row_commit_version,
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn tombstones_group_by_file_and_dedup() {
        let mut t = Tombstones::new();
        assert!(t.is_empty());

        t.insert("a.parquet", 5);
        t.insert("a.parquet", 1);
        t.insert("a.parquet", 5); // repeat: no-op
        t.insert("b.parquet", 7);

        assert!(!t.is_empty());
        assert_eq!(t.touched_files(), 2);
        assert_eq!(t.total_rows(), 3);
    }

    #[test]
    fn empty_tombstones_have_no_rows() {
        let t = Tombstones::new();
        assert_eq!(t.touched_files(), 0);
        assert_eq!(t.total_rows(), 0);
    }
}
