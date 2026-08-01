//! Tombstoning superseded rows with Delta deletion vectors.
//!
//! Merge mode never rewrites a data file. A row is superseded by appending its new
//! version and marking the old one deleted in the file's deletion vector, which the
//! Delta protocol expresses as a `remove` of the file paired with an `add` of the same
//! path carrying the new vector. Log replay keys files by (path, deletion vector id), so
//! the pair is unambiguous within one commit.
//!
//! Every vector touched by a flush is packed into a single object.
//! [`StreamingDeletionVectorWriter`] lays them out one after another with per-vector
//! offsets, so the number of files a flush touches drives the number of small log
//! records, never the number of objects written.

use std::collections::BTreeMap;

use anyhow::{Result as AnyResult, anyhow};
use delta_kernel::actions::deletion_vector_writer::{
    KernelDeletionVector, StreamingDeletionVectorWriter,
};
use deltalake::kernel::{Action, Add, DeletionVectorDescriptor, StorageType};
use deltalake::{DeltaTable, Path};
// `put` lives on the extension trait as of object_store 0.13.
use object_store::ObjectStoreExt as _;
use roaring::RoaringTreemap;
use uuid::Uuid;

use super::super::deletion_vector::read_deletion_vector;

/// Physical row ordinals to tombstone, grouped by data file.
///
/// Ordinals are positions within the file counted as if no deletion vector were applied,
/// which is the space a deletion vector addresses.
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
        self.files.entry_ref_or_insert(path).insert(ordinal);
    }

    pub fn is_empty(&self) -> bool {
        self.files.is_empty()
    }

    /// Number of data files this flush tombstones rows in.
    pub fn touched_files(&self) -> usize {
        self.files.len()
    }

    /// Total rows tombstoned across all files.
    pub fn total_rows(&self) -> u64 {
        self.files.values().map(|b| b.len()).sum()
    }
}

/// Helper so `insert` reads as one line without cloning the path on every hit.
trait EntryRef {
    fn entry_ref_or_insert(&mut self, key: &str) -> &mut RoaringTreemap;
}

impl EntryRef for BTreeMap<String, RoaringTreemap> {
    fn entry_ref_or_insert(&mut self, key: &str) -> &mut RoaringTreemap {
        if !self.contains_key(key) {
            self.insert(key.to_string(), RoaringTreemap::new());
        }
        self.get_mut(key).expect("just inserted")
    }
}

/// What one call to [`write_deletion_vectors`] produced, for metrics and logging.
#[derive(Debug, Default, Clone, Copy)]
pub struct DvWriteMetrics {
    /// Data files whose deletion vector was replaced.
    pub files_touched: usize,
    /// Rows this flush newly tombstoned.
    pub rows_tombstoned: u64,
    /// Size of the packed deletion vector object.
    pub dv_bytes: usize,
}

/// Install `tombstones` into the table, returning the actions that do so.
///
/// For each touched file this reads the existing vector, unions the new ordinals into it,
/// and appends the result to one packed object. Vectors are processed to completion one
/// file at a time so peak memory stays at a single file's bitmap rather than the whole
/// flush's.
///
/// Returns an empty action list when there is nothing to tombstone, without writing an
/// object.
pub async fn write_deletion_vectors(
    tombstones: &Tombstones,
    table: &DeltaTable,
) -> AnyResult<(Vec<Action>, DvWriteMetrics)> {
    if tombstones.is_empty() {
        return Ok((Vec::new(), DvWriteMetrics::default()));
    }

    let state = table
        .snapshot()
        .map_err(|e| anyhow!("Delta table has no snapshot to tombstone rows in: {e}"))?;

    // Resolve every path up front so a stale path fails before anything is written.
    let mut live: BTreeMap<String, Add> = BTreeMap::new();
    for view in state.log_data() {
        let path = view.path().to_string();
        if tombstones.files.contains_key(&path) {
            // The re-added file must carry the original size, partition values, and
            // statistics; only its deletion vector changes. The arrow-array alternative
            // the deprecation suggests does not produce an `Add` we can re-commit.
            #[allow(deprecated)]
            live.insert(path, view.add_action());
        }
    }

    let uuid = Uuid::new_v4();
    let dv_path = Path::from(format!("deletion_vector_{uuid}.bin"));
    // `path_or_inline_dv` for a relative vector is the z85-encoded UUID, optionally
    // preceded by a directory prefix. We use no prefix.
    let encoded_path = z85::encode(uuid.as_bytes());

    let mut buffer: Vec<u8> = Vec::new();
    let mut writer = StreamingDeletionVectorWriter::new(&mut buffer);
    let mut actions = Vec::with_capacity(tombstones.files.len() * 2);
    let mut rows_tombstoned = 0u64;

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
        rows_tombstoned += bitmap.len() - before;

        let mut dv = KernelDeletionVector::new();
        dv.add_deleted_row_indexes(&bitmap);
        drop(bitmap);

        let result = writer
            .write_deletion_vector(dv)
            .map_err(|e| anyhow!("error serializing deletion vector for '{path}': {e}"))?;

        let descriptor = DeletionVectorDescriptor {
            storage_type: StorageType::UuidRelativePath,
            path_or_inline_dv: encoded_path.clone(),
            offset: Some(result.offset),
            size_in_bytes: result.size_in_bytes,
            cardinality: result.cardinality,
        };

        actions.push(Action::Remove(remove_for(add)));
        actions.push(Action::Add(Add {
            deletion_vector: Some(descriptor),
            data_change: true,
            ..add.clone()
        }));
    }

    writer
        .finalize()
        .map_err(|e| anyhow!("error finalizing deletion vector file: {e}"))?;

    let dv_bytes = buffer.len();
    let payload = bytes::Bytes::from(buffer);
    table
        .object_store()
        .put(&dv_path, payload.into())
        .await
        .map_err(|e| anyhow!("error writing deletion vector file '{dv_path}': {e}"))?;

    Ok((
        actions,
        DvWriteMetrics {
            files_touched: tombstones.files.len(),
            rows_tombstoned,
            dv_bytes,
        },
    ))
}

/// The `remove` half of a deletion vector update.
///
/// It carries the file's *current* vector, because a file is identified by path together
/// with its vector id; without it, log replay cannot tell which version is being removed.
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
