//! Logic to manage persistent checkpoints for a circuit.

use crate::dynamic::{self, data::DataTyped};
use crate::storage::file::SerializerInner;
use crate::{Error, NumEntries, TypedBox};
use feldera_types::checkpoint::{
    CheckpointDependencies, CheckpointDependenciesWrite, CheckpointMetadata,
};
use feldera_types::constants::{
    ACTIVATION_MARKER_FILE, ADHOC_TEMP_DIR, CHECKPOINT_DEPENDENCIES, CHECKPOINT_FILE_NAME,
    DBSP_FILE_EXTENSION, STATE_FILE, STATUS_FILE, STEPS_FILE,
};
use size_of::SizeOf;

use std::io::ErrorKind;
use std::sync::atomic::Ordering;
use std::{
    collections::{HashSet, VecDeque},
    sync::Arc,
};

use feldera_storage::error::StorageError;
use feldera_storage::fbuf::FBuf;
use feldera_storage::{StorageBackend, StorageFileType, StoragePath};
use uuid::Uuid;

use super::RuntimeError;

/// A "checkpointer" is responsible for the creation, and removal of
/// checkpoints for a circuit.
///
/// It handles list of available checkpoints, and the files associated
/// with each checkpoint.
#[derive(derive_more::Debug, Clone)]
pub struct Checkpointer {
    #[debug(skip)]
    backend: Arc<dyn StorageBackend>,
    checkpoint_list: VecDeque<CheckpointMetadata>,
}

impl Checkpointer {
    /// We keep at least this many checkpoints around.
    pub(super) const MIN_CHECKPOINT_THRESHOLD: usize = 2;

    /// Creates a new checkpointer for directory `storage_path`.  Deletes any
    /// unreferenced files in the directory.
    pub fn new(backend: Arc<dyn StorageBackend>) -> Result<Self, Error> {
        let checkpoint_list = Self::read_checkpoints(&*backend)?;

        let this = Checkpointer {
            backend,
            checkpoint_list,
        };

        this.init_storage()?;

        Ok(this)
    }

    /// Verifies that existing checkpoints have the specified fingerprint.
    pub fn verify_fingerprint(&self, fingerprint: u64) -> Result<(), Error> {
        if self
            .checkpoint_list
            .iter()
            .any(|cpm| cpm.fingerprint != fingerprint)
        {
            Err(Error::Runtime(RuntimeError::IncompatibleStorage))
        } else {
            Ok(())
        }
    }

    fn init_storage(&self) -> Result<(), Error> {
        let usage = self.gc_startup()?;

        // We measured the amount of storage in use. Give it to the backend as
        // the initial value.
        self.backend.usage().store(usage as i64, Ordering::Relaxed);

        Ok(())
    }

    pub(super) fn measure_checkpoint_storage_use(&self, uuid: uuid::Uuid) -> Result<u64, Error> {
        let mut usage = 0;
        StorageError::ignore_notfound(self.backend.list(
            &Self::checkpoint_dir(uuid),
            &mut |_path, file_type| {
                if let StorageFileType::File { size } = file_type {
                    usage += size;
                }
            },
        ))?;
        Ok(usage)
    }

    pub(super) fn gather_batches_for_checkpoint(
        &self,
        cpm: &CheckpointMetadata,
    ) -> Result<HashSet<StoragePath>, StorageError> {
        self.backend.gather_batches_for_checkpoint(cpm)
    }

    /// Remove unexpected/leftover files from a previous run in the storage
    /// directory.  Returns the amount of storage still in use.
    pub fn gc_startup(&self) -> Result<u64, Error> {
        // Collect all directories and files still referenced by a checkpoint
        let mut in_use_paths: HashSet<StoragePath> = HashSet::new();
        in_use_paths.insert(CHECKPOINT_FILE_NAME.into());
        in_use_paths.insert(STEPS_FILE.into());
        in_use_paths.insert(STATE_FILE.into());
        // Don't delete either `status.json` or `status.json.mut` either because
        // these files get updated asynchronously and we must not interfere with
        // it.
        in_use_paths.insert(STATUS_FILE.into());
        in_use_paths.insert(format!("{}.mut", STATUS_FILE).into());
        in_use_paths.insert(ADHOC_TEMP_DIR.into());
        in_use_paths.insert(ACTIVATION_MARKER_FILE.into());
        for cpm in self.checkpoint_list.iter() {
            in_use_paths.insert(cpm.uuid.to_string().into());
            let batches = self.gather_batches_for_checkpoint(cpm)?;
            for batch in batches {
                in_use_paths.insert(batch);
            }
        }
        // Give the coordinator a namespace for persistent files.
        in_use_paths.insert("coordinator".into());

        /// True if `path` is a name that we might have created ourselves.
        fn is_feldera_filename(path: &StoragePath) -> bool {
            path.extension()
                .is_some_and(|extension| DBSP_FILE_EXTENSION.contains(&extension))
        }

        // Collect everything found in the storage directory
        let mut usage = 0;
        self.backend.list(&StoragePath::default(), &mut |path, file_type| {
            if !in_use_paths.contains(path) && (is_feldera_filename(path) || file_type == StorageFileType::Directory) {
                match self.backend.delete_recursive(path) {
                    Ok(_) => {
                        tracing::debug!("Removed unused {file_type:?} '{path}'");
                    }
                    Err(e) => {
                        tracing::warn!("Unable to remove old-checkpoint file {path}: {e} (the pipeline will try to delete the file again on a restart)");
                    }
                }
            } else if let StorageFileType::File { size } = file_type {
                usage += size;
            }
        })?;

        Ok(usage)
    }

    pub(super) fn checkpoint_dir(uuid: Uuid) -> StoragePath {
        uuid.to_string().into()
    }

    /// Confirm every state file recorded for this checkpoint at commit time
    /// is still on disk.
    ///
    /// Callers use this before restoring a persistent-mode checkpoint to
    /// distinguish "operator is new since the checkpoint" (file legitimately
    /// absent) from "operator state was committed but the file vanished"
    /// (storage corruption). V1 checkpoints have no state-file list, so
    /// the check is a no-op for them, preserving backwards compatibility.
    pub fn verify_checkpoint_intact(
        backend: &dyn StorageBackend,
        cp_dir: &StoragePath,
    ) -> Result<(), StorageError> {
        let deps: CheckpointDependencies =
            match backend.read_json(&cp_dir.child(CHECKPOINT_DEPENDENCIES)) {
                Ok(d) => d,
                Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
                Err(error) => return Err(error),
            };
        let state_files = deps.state_files();
        // V1 checkpoints have no state-file manifest; nothing to verify against.
        if state_files.is_empty() {
            return Ok(());
        }

        let mut present: HashSet<String> = HashSet::new();
        backend.list(cp_dir, &mut |path, file_type| {
            if file_type != StorageFileType::Directory {
                if let Some(name) = path.filename() {
                    present.insert(name.to_string());
                }
            }
        })?;

        let missing: Vec<String> = state_files
            .iter()
            .filter(|name| !present.contains(*name))
            .cloned()
            .collect();
        if !missing.is_empty() {
            return Err(StorageError::StdIo {
                kind: ErrorKind::InvalidData,
                operation: "checkpoint state files missing from checkpoint dir",
                path: Some(format!("{}: {}", cp_dir, missing.join(", "))),
            });
        }
        Ok(())
    }

    pub(super) fn commit(
        &mut self,
        uuid: Uuid,
        fingerprint: u64,
        identifier: Option<String>,
        steps: Option<u64>,
        processed_records: Option<u64>,
    ) -> Result<CheckpointMetadata, Error> {
        // Write marker file to ensure that this directory is detected as a
        // checkpoint.
        self.backend
            .write(&Self::checkpoint_dir(uuid).child("CHECKPOINT"), FBuf::new())?;

        let mut md = CheckpointMetadata {
            uuid,
            identifier,
            fingerprint,
            size: None,
            processed_records,
            steps,
        };

        let batches = self.gather_batches_for_checkpoint(&md)?;
        let batches_vec: Vec<String> = batches.into_iter().map(|p| p.to_string()).collect();

        // Single source of truth for what this checkpoint owns:
        //   * `batches`     -> the w*.feldera files at the storage root the
        //                       checkpoint references (used by GC)
        //   * `state_files` -> every per-operator state file in this
        //                       checkpoint dir (used by restore to detect
        //                       silent state-file loss in persistent mode)
        // Listing the dir captures every file that operators wrote during
        // their checkpoint phase. Dependencies.json itself is excluded
        // because it is what we are about to write. The CHECKPOINT marker
        // written above is captured here too, so verify_checkpoint_intact
        // also catches a missing marker on restore.
        let cp_dir = Self::checkpoint_dir(uuid);
        let mut state_files: Vec<String> = Vec::new();
        self.backend.list(&cp_dir, &mut |path, file_type| {
            if file_type != StorageFileType::Directory {
                if let Some(name) = path.filename() {
                    if name != CHECKPOINT_DEPENDENCIES {
                        state_files.push(name.to_string());
                    }
                }
            }
        })?;
        state_files.sort_unstable();

        self.backend
            .write_json(
                &cp_dir.child(CHECKPOINT_DEPENDENCIES),
                &CheckpointDependenciesWrite {
                    batches: &batches_vec,
                    state_files: &state_files,
                },
            )
            .and_then(|reader| reader.commit())?;

        // Barrier fsync the checkpoint dir so every rename done by operators
        // committing their state files into this dir becomes durable.
        self.backend.fsync_dir(&cp_dir)?;

        md.size = Some(self.measure_checkpoint_storage_use(uuid)?);

        // Persist the catalog first, then add to the in-memory list only on
        // success. Otherwise a failed write leaves callers with a phantom
        // checkpoint that no future restart will recognize. The same rollback
        // applies when the post-rename parent-dir fsync fails: the rename is
        // not yet durable, so a crash can drop it and leave the same phantom.
        self.checkpoint_list.push_back(md.clone());
        let result = self
            .update_checkpoint_file()
            .and_then(|_| Ok(self.backend.fsync_dir(&StoragePath::default())?));
        if let Err(e) = result {
            self.checkpoint_list.pop_back();
            return Err(e);
        }
        Ok(md)
    }

    /// List all currently available checkpoints.
    pub(super) fn list_checkpoints(&self) -> Result<Vec<CheckpointMetadata>, Error> {
        Ok(self.checkpoint_list.clone().into())
    }

    /// Reads the list of checkpoints available through `backend`.
    ///
    /// A missing `checkpoints.feldera` is treated as "no checkpoints yet"
    /// only when the storage directory holds no UUID-shaped subdirectories.
    /// If UUID directories exist, the catalog has been lost while the
    /// checkpoints themselves are likely still on disk; proceeding would
    /// let `gc_startup` recursively delete them. Refuse to start instead.
    pub fn read_checkpoints(
        backend: &dyn StorageBackend,
    ) -> Result<VecDeque<CheckpointMetadata>, Error> {
        match backend.read_json(&StoragePath::from(CHECKPOINT_FILE_NAME)) {
            Ok(checkpoints) => Ok(checkpoints),
            Err(error) if error.kind() == ErrorKind::NotFound => {
                let mut orphan_uuid_dirs: Vec<String> = Vec::new();
                backend.list(&StoragePath::default(), &mut |path, file_type| {
                    if file_type == StorageFileType::Directory {
                        if let Some(name) = path.filename() {
                            if Uuid::parse_str(name).is_ok() {
                                orphan_uuid_dirs.push(name.to_string());
                            }
                        }
                    }
                })?;
                if !orphan_uuid_dirs.is_empty() {
                    return Err(Error::Storage(StorageError::StdIo {
                        kind: ErrorKind::InvalidData,
                        operation: "checkpoint catalog (checkpoints.feldera) missing while \
                                    UUID-named checkpoint directories remain in the storage \
                                    root; restore the catalog from backup, or delete those \
                                    directories to start with a fresh catalog",
                        path: Some(CHECKPOINT_FILE_NAME.to_string()),
                    }));
                }
                Ok(VecDeque::new())
            }
            Err(error) => Err(error)?,
        }
    }

    fn update_checkpoint_file(&self) -> Result<(), Error> {
        Ok(self
            .backend
            .write_json(&CHECKPOINT_FILE_NAME.into(), &self.checkpoint_list)
            .and_then(|reader| reader.commit())?)
    }

    /// Removes `file` and logs any error.
    fn remove_batch_file(&self, file: &StoragePath) {
        match self.backend.delete_if_exists(file) {
            Ok(_) => {
                tracing::debug!("Removed file {file}");
            }
            Err(e) => {
                tracing::warn!(
                    "Unable to remove old-checkpoint file {file}: {e} (the pipeline will try to delete the file again on a restart)"
                );
            }
        }
    }

    /// Removes all meta-data files associated with the checkpoint given by
    /// `cpm` by removing the folder associated with the checkpoint.
    fn remove_checkpoint_dir(&self, cpm: uuid::Uuid) -> Result<(), Error> {
        assert_ne!(cpm, Uuid::nil());
        self.backend.delete_recursive(&cpm.to_string().into())?;
        Ok(())
    }

    /// Remove the oldest checkpoints from the list.
    /// - Preserves at least `MIN_CHECKPOINT_THRESHOLD` checkpoints.
    /// - Does not remove any checkpoints whose UUID is in the `except` list.
    ///
    /// # Returns
    /// - Uuid of the removed checkpoints, if there are more than `MIN_CHECKPOINT_THRESHOLD`.
    /// - Empty set otherwise.
    pub fn gc_checkpoint(
        &mut self,
        except: HashSet<uuid::Uuid>,
    ) -> Result<HashSet<uuid::Uuid>, Error> {
        if self.checkpoint_list.len() <= Self::MIN_CHECKPOINT_THRESHOLD {
            return Ok(HashSet::new());
        }

        // Make sure to keep every batch referenced by a checkpoint in `except`.
        // Errors must propagate; silently swallowing them would let GC delete
        // batches we promised to keep.
        let mut batch_files_to_keep: HashSet<StoragePath> = HashSet::new();
        for uuid in &except {
            let batches = self.backend.gather_batches_for_checkpoint_uuid(*uuid)?;
            batch_files_to_keep.extend(batches);
        }

        let to_remove: HashSet<_> = self
            .checkpoint_list
            .iter()
            .take(
                self.checkpoint_list
                    .len()
                    .saturating_sub(Self::MIN_CHECKPOINT_THRESHOLD),
            )
            .map(|cpm| cpm.uuid)
            .filter(|cpm| !except.contains(cpm))
            .collect();

        self.checkpoint_list
            .retain(|cpm| !to_remove.contains(&cpm.uuid));

        // Update the checkpoint list file, we do this first intentionally, in case
        // later operations fail we don't want the checkpoint list to
        // contain a checkpoint that only has part of the files.
        //
        // If any of the later operations fail, restarting the circuit will try
        // to remove the checkpoint files again (see also [`Self::gc_startup`]).
        self.update_checkpoint_file()?;

        // Also keep batches of the newest retained checkpoint not in `except`
        // (its merger may still depend on them). Errors must propagate so we
        // never delete a batch we couldn't prove safe.
        if let Some(retained) = self
            .checkpoint_list
            .iter()
            .find(|c| !except.contains(&c.uuid))
        {
            let batches = self.backend.gather_batches_for_checkpoint(retained)?;
            batch_files_to_keep.extend(batches);
        }

        for cpm in &to_remove {
            for batch_file in self
                .backend
                .gather_batches_for_checkpoint_uuid(*cpm)?
                .difference(&batch_files_to_keep)
            {
                self.remove_batch_file(batch_file);
            }

            self.remove_checkpoint_dir(*cpm)?;
        }

        tracing::info!(
            "cleaned up {} checkpoints; exception list: {except:?}, retaining checkpoints: {:?}",
            to_remove.len(),
            self.checkpoint_list
                .iter()
                .map(|cpm| cpm.uuid)
                .collect::<Vec<_>>()
        );

        Ok(to_remove)
    }
}

/// Build a `StorageError::StdIo { InvalidData, .. }` from an operator's
/// checkpoint-restore failure. `operation` is the static stage tag (e.g.
/// "Z1 checkpoint validation failed"); `detail` is the pre-formatted
/// `"<path>: <err>"` payload the call site already constructs.
pub(crate) fn checkpoint_invalid_data_error(operation: &'static str, detail: String) -> Error {
    Error::Storage(StorageError::StdIo {
        kind: ErrorKind::InvalidData,
        operation,
        path: Some(detail),
    })
}

/// Trait for types that can be check-pointed and restored.
///
/// This is to be used for any additional state within circuit operators
/// that's not stored within a batch (which are already stored in files).
pub trait Checkpoint {
    fn checkpoint(&self) -> Result<Vec<u8>, Error>;
    fn restore(&mut self, data: &[u8]) -> Result<(), Error>;
}

impl Checkpoint for isize {
    fn checkpoint(&self) -> Result<Vec<u8>, Error> {
        todo!()
    }

    fn restore(&mut self, _data: &[u8]) -> Result<(), Error> {
        todo!()
    }
}

impl Checkpoint for usize {
    fn checkpoint(&self) -> Result<Vec<u8>, Error> {
        todo!()
    }

    fn restore(&mut self, _data: &[u8]) -> Result<(), Error> {
        todo!()
    }
}

impl Checkpoint for i32 {
    fn checkpoint(&self) -> Result<Vec<u8>, Error> {
        todo!()
    }

    fn restore(&mut self, _data: &[u8]) -> Result<(), Error> {
        todo!()
    }
}

impl<N> Checkpoint for Box<N>
where
    N: Checkpoint + ?Sized,
{
    fn checkpoint(&self) -> Result<Vec<u8>, Error> {
        self.as_ref().checkpoint()
    }

    fn restore(&mut self, data: &[u8]) -> Result<(), Error> {
        self.as_mut().restore(data)
    }
}

impl<T> Checkpoint for Option<T>
where
    T: Checkpoint,
{
    fn checkpoint(&self) -> Result<Vec<u8>, Error> {
        todo!()
    }

    fn restore(&mut self, _data: &[u8]) -> Result<(), Error> {
        todo!()
    }
}

impl<T, D: ?Sized> Checkpoint for TypedBox<T, D> {
    fn checkpoint(&self) -> Result<Vec<u8>, Error> {
        todo!()
    }
    fn restore(&mut self, _data: &[u8]) -> Result<(), Error> {
        todo!()
    }
}

impl Checkpoint for dyn dynamic::data::Data + 'static {
    fn checkpoint(&self) -> Result<Vec<u8>, Error> {
        Ok(SerializerInner::to_fbuf_with_thread_local(|s| self.serialize(s)).into_vec())
    }

    fn restore(&mut self, data: &[u8]) -> Result<(), Error> {
        unsafe { self.deserialize_from_bytes(data, 0) };
        Ok(())
    }
}

impl Checkpoint for dyn DataTyped<Type = u64> + 'static {
    fn checkpoint(&self) -> Result<Vec<u8>, Error> {
        todo!()
    }

    fn restore(&mut self, _data: &[u8]) -> Result<(), Error> {
        todo!()
    }
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Hash, SizeOf)]
pub struct EmptyCheckpoint<T: Default> {
    pub val: T,
}

impl<T> NumEntries for EmptyCheckpoint<T>
where
    T: Default + NumEntries,
{
    const CONST_NUM_ENTRIES: Option<usize> = T::CONST_NUM_ENTRIES;

    fn num_entries_shallow(&self) -> usize {
        self.val.num_entries_shallow()
    }

    fn num_entries_deep(&self) -> usize {
        self.val.num_entries_deep()
    }
}

impl<T: Default> EmptyCheckpoint<T> {
    pub fn new(val: T) -> Self {
        Self { val }
    }
}

impl<T: Default> Checkpoint for EmptyCheckpoint<T> {
    fn checkpoint(&self) -> Result<Vec<u8>, Error> {
        Ok(vec![])
    }

    fn restore(&mut self, _data: &[u8]) -> Result<(), Error> {
        self.val = T::default();
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use feldera_storage::{StorageBackend, StoragePath};
    use feldera_types::config::{FileBackendConfig, StorageCacheConfig};
    use feldera_types::constants::CHECKPOINT_FILE_NAME;
    use std::collections::HashSet;

    use crate::storage::backend::posixio_impl::PosixBackend;

    use super::Checkpointer;

    /// Backend wrapper that delegates everything to an inner backend but
    /// fails writes whose path equals `fail_on`.
    struct CatalogFailingBackend {
        inner: Arc<dyn StorageBackend>,
        fail_on: StoragePath,
    }

    impl feldera_storage::StorageBackend for CatalogFailingBackend {
        fn create_named(
            &self,
            name: &StoragePath,
        ) -> Result<Box<dyn feldera_storage::FileWriter>, feldera_storage::error::StorageError>
        {
            if name == &self.fail_on {
                return Err(feldera_storage::error::StorageError::StdIo {
                    kind: std::io::ErrorKind::PermissionDenied,
                    operation: "injected catalog write failure",
                    path: None,
                });
            }
            self.inner.create_named(name)
        }

        fn open(
            &self,
            name: &StoragePath,
        ) -> Result<Arc<dyn feldera_storage::FileReader>, feldera_storage::error::StorageError>
        {
            self.inner.open(name)
        }

        fn list(
            &self,
            parent: &StoragePath,
            cb: &mut dyn FnMut(&StoragePath, feldera_storage::StorageFileType),
        ) -> Result<(), feldera_storage::error::StorageError> {
            self.inner.list(parent, cb)
        }

        fn delete(&self, name: &StoragePath) -> Result<(), feldera_storage::error::StorageError> {
            self.inner.delete(name)
        }

        fn delete_recursive(
            &self,
            name: &StoragePath,
        ) -> Result<(), feldera_storage::error::StorageError> {
            self.inner.delete_recursive(name)
        }

        fn usage(&self) -> Arc<std::sync::atomic::AtomicI64> {
            self.inner.usage()
        }
    }

    /// `verify_checkpoint_intact` errors when a state file listed in
    /// `dependencies.json` is missing from the checkpoint dir.
    #[test]
    fn verify_checkpoint_intact_detects_missing_state_file() {
        let tempdir = tempfile::tempdir().unwrap();
        let backend: Arc<dyn StorageBackend> = Arc::new(PosixBackend::new(
            tempdir.path(),
            StorageCacheConfig::default(),
            &FileBackendConfig::default(),
        ));
        let mut checkpointer = Checkpointer::new(backend.clone()).unwrap();

        let uuid = uuid::Uuid::now_v7();
        // Pre-populate a per-operator state file before commit so the
        // dependencies record captures it in state_files.
        let cp_dir = tempdir.path().join(uuid.to_string());
        std::fs::create_dir_all(&cp_dir).unwrap();
        let state_file = cp_dir.join("pspine-trace.dat");
        std::fs::write(&state_file, b"spine state").unwrap();

        checkpointer.commit(uuid, 0, None, Some(0), Some(0)).unwrap();
        drop(checkpointer);

        let cp_path: StoragePath = uuid.to_string().into();
        Checkpointer::verify_checkpoint_intact(backend.as_ref(), &cp_path)
            .expect("intact checkpoint should verify");

        // Lose the per-operator file. The dependencies record still names it.
        std::fs::remove_file(&state_file).unwrap();
        let err = Checkpointer::verify_checkpoint_intact(backend.as_ref(), &cp_path)
            .expect_err("missing state file should be reported");
        assert!(
            format!("{err}").contains("pspine-trace.dat"),
            "error should mention the missing file, got: {err}",
        );
    }

    /// Verifies that GC uses `dependencies.json` as the authoritative list
    /// of batches to keep, so a checkpoint survives missing or corrupted
    /// `pspine-batches-*.dat` files.
    #[test]
    fn missing_pspine_batches_preserves_batches() {
        let tempdir = tempfile::tempdir().unwrap();
        let make_backend = || -> Arc<dyn StorageBackend> {
            Arc::new(PosixBackend::new(
                tempdir.path(),
                StorageCacheConfig::default(),
                &FileBackendConfig::default(),
            ))
        };

        let mut checkpointer = Checkpointer::new(make_backend()).unwrap();
        let uuid = uuid::Uuid::now_v7();
        // Pre-populate a batch file and a pspine-batches entry that
        // references it before calling commit, so the commit-time
        // dependencies.json captures the batch in its snapshot.
        let cp_dir = tempdir.path().join(uuid.to_string());
        std::fs::create_dir_all(&cp_dir).unwrap();
        let batch_filename = "w0-aaaaaaaa.feldera";
        let batch_path = tempdir.path().join(batch_filename);
        std::fs::write(&batch_path, b"batch payload").unwrap();
        let pspine_batches_path = cp_dir.join("pspine-batches-trace.dat");
        std::fs::write(
            &pspine_batches_path,
            serde_json::to_vec(&serde_json::json!({
                "files": [batch_filename]
            }))
            .unwrap(),
        )
        .unwrap();

        checkpointer.commit(uuid, 0, None, Some(0), Some(0)).unwrap();
        drop(checkpointer);

        // Now lose the per-spine metadata, keeping dependencies.json intact.
        std::fs::remove_file(&pspine_batches_path).unwrap();
        assert!(cp_dir.join("dependencies.json").exists());
        assert!(batch_path.exists());

        let _restarted = Checkpointer::new(make_backend()).unwrap();
        assert!(
            batch_path.exists(),
            "startup GC deleted live batch {} after losing pspine-batches metadata",
            batch_path.display(),
        );
    }

    /// Older checkpoints predate `dependencies.json` and only carry the
    /// per-spine `pspine-batches-*.dat` files. GC must still be able to
    /// gather their batches from those files alone.
    #[test]
    fn legacy_checkpoint_without_dependencies_json_preserves_batches() {
        let tempdir = tempfile::tempdir().unwrap();
        let make_backend = || -> Arc<dyn StorageBackend> {
            Arc::new(PosixBackend::new(
                tempdir.path(),
                StorageCacheConfig::default(),
                &FileBackendConfig::default(),
            ))
        };

        let mut checkpointer = Checkpointer::new(make_backend()).unwrap();
        let uuid = uuid::Uuid::now_v7();
        let cp_dir = tempdir.path().join(uuid.to_string());
        std::fs::create_dir_all(&cp_dir).unwrap();
        let batch_filename = "w0-legacy.feldera";
        let batch_path = tempdir.path().join(batch_filename);
        std::fs::write(&batch_path, b"batch payload").unwrap();
        let pspine_batches_path = cp_dir.join("pspine-batches-trace.dat");
        std::fs::write(
            &pspine_batches_path,
            serde_json::to_vec(&serde_json::json!({
                "files": [batch_filename]
            }))
            .unwrap(),
        )
        .unwrap();

        checkpointer.commit(uuid, 0, None, Some(0), Some(0)).unwrap();
        drop(checkpointer);

        // Simulate an old checkpoint: drop `dependencies.json` entirely,
        // keep the per-spine file. The fallback scan should still find
        // the referenced batch.
        std::fs::remove_file(cp_dir.join("dependencies.json")).unwrap();
        assert!(pspine_batches_path.exists());
        assert!(batch_path.exists());

        let _restarted = Checkpointer::new(make_backend()).unwrap();
        assert!(
            batch_path.exists(),
            "startup GC deleted batch {} on legacy checkpoint with no dependencies.json",
            batch_path.display(),
        );
    }

    /// Losing `checkpoints.feldera` while UUID checkpoint dirs survive on
    /// disk must surface as `InvalidData` from `Checkpointer::new`. The dirs
    /// must remain untouched so an operator can recover the catalog.
    #[test]
    fn missing_catalog_preserves_checkpoint_dirs() {
        let tempdir = tempfile::tempdir().unwrap();
        let make_backend = || -> Arc<dyn StorageBackend> {
            Arc::new(PosixBackend::new(
                tempdir.path(),
                StorageCacheConfig::default(),
                &FileBackendConfig::default(),
            ))
        };

        let mut checkpointer = Checkpointer::new(make_backend()).unwrap();
        let uuids: Vec<_> = (0..Checkpointer::MIN_CHECKPOINT_THRESHOLD)
            .map(|i| {
                let uuid = uuid::Uuid::now_v7();
                checkpointer
                    .commit(uuid, 0, None, Some(i as u64), Some(0))
                    .unwrap();
                uuid
            })
            .collect();
        drop(checkpointer);

        std::fs::remove_file(tempdir.path().join(CHECKPOINT_FILE_NAME)).unwrap();

        let err = Checkpointer::new(make_backend())
            .err()
            .expect("missing catalog with orphan UUID dirs must error");
        assert!(
            format!("{err}").contains("checkpoint catalog (checkpoints.feldera) missing"),
            "expected catalog-missing error, got: {err}",
        );

        for uuid in &uuids {
            let path = tempdir.path().join(uuid.to_string());
            assert!(
                path.exists(),
                "startup wiped UUID dir {uuid} after losing the catalog",
            );
        }
    }

    /// `gc_checkpoint` must not silently drop a "protect this checkpoint"
    /// request when the protected checkpoint has corrupt metadata. Otherwise
    /// shared batches are deleted out from under a retained checkpoint.
    #[test]
    fn gc_checkpoint_propagates_corrupt_protected_metadata() {
        let tempdir = tempfile::tempdir().unwrap();
        let backend: Arc<dyn StorageBackend> = Arc::new(PosixBackend::new(
            tempdir.path(),
            StorageCacheConfig::default(),
            &FileBackendConfig::default(),
        ));
        let mut checkpointer = Checkpointer::new(backend).unwrap();

        // Need more than MIN_CHECKPOINT_THRESHOLD checkpoints so gc has work.
        let mut uuids = Vec::new();
        for i in 0..Checkpointer::MIN_CHECKPOINT_THRESHOLD + 1 {
            let uuid = uuid::Uuid::now_v7();
            checkpointer
                .commit(uuid, 0, None, Some(i as u64), Some(0))
                .unwrap();
            uuids.push(uuid);
        }

        // Corrupt the metadata of the newest checkpoint (the one we'll protect).
        let protected = *uuids.last().unwrap();
        let deps = tempdir
            .path()
            .join(protected.to_string())
            .join("dependencies.json");
        std::fs::write(&deps, b"{not valid json").unwrap();

        let except: HashSet<_> = [protected].into_iter().collect();
        let result = checkpointer.gc_checkpoint(except);
        assert!(
            result.is_err(),
            "gc_checkpoint should not return Ok with corrupt metadata on protected checkpoint {protected}",
        );
    }

    /// Startup must not panic when a checkpoint dir contains malformed
    /// dependency metadata. The corrupt metadata should surface as an
    /// `Err` from `Checkpointer::new` so the caller can decide what to do.
    #[test]
    fn corrupt_pspine_batches_returns_error_not_panic() {
        let tempdir = tempfile::tempdir().unwrap();
        let make_backend = || -> Arc<dyn StorageBackend> {
            Arc::new(PosixBackend::new(
                tempdir.path(),
                StorageCacheConfig::default(),
                &FileBackendConfig::default(),
            ))
        };

        let mut checkpointer = Checkpointer::new(make_backend()).unwrap();
        let uuid = uuid::Uuid::now_v7();
        checkpointer
            .commit(uuid, 0, None, Some(0), Some(0))
            .unwrap();
        drop(checkpointer);

        let deps = tempdir
            .path()
            .join(uuid.to_string())
            .join("dependencies.json");
        std::fs::write(&deps, b"{not valid json").unwrap();

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            Checkpointer::new(make_backend())
        }));
        match result {
            Ok(Err(_)) => {}
            Ok(Ok(_)) => panic!("Checkpointer accepted corrupt metadata without an error"),
            Err(_) => panic!("Checkpointer panicked on corrupt pspine-batches"),
        }
    }

    /// A failed `commit` must not leave the new checkpoint in the in-memory
    /// list. Otherwise callers see a checkpoint that no restart will find.
    #[test]
    fn failed_commit_rolls_back_in_memory_list() {
        use feldera_types::constants::CHECKPOINT_FILE_NAME;
        let tempdir = tempfile::tempdir().unwrap();
        let posix: Arc<dyn StorageBackend> = Arc::new(PosixBackend::new(
            tempdir.path(),
            StorageCacheConfig::default(),
            &FileBackendConfig::default(),
        ));
        let backend: Arc<dyn StorageBackend> = Arc::new(CatalogFailingBackend {
            inner: posix,
            fail_on: CHECKPOINT_FILE_NAME.into(),
        });
        let mut checkpointer = Checkpointer::new(backend).unwrap();

        let uuid = uuid::Uuid::now_v7();
        let result = checkpointer.commit(uuid, 0, None, Some(0), Some(0));
        assert!(
            result.is_err(),
            "commit should fail when catalog write fails"
        );

        assert!(
            !checkpointer
                .checkpoint_list
                .iter()
                .any(|cpm| cpm.uuid == uuid),
            "failed commit left UUID {uuid} in the in-memory checkpoint list",
        );
    }

    struct Empty;
    struct MinCheckpoints;
    struct ExtraCheckpoints;

    struct TestState<S> {
        checkpointer: Checkpointer,
        tempdir: tempfile::TempDir,
        _phantom: std::marker::PhantomData<S>,
    }

    impl<S> TestState<S> {
        fn extras(&self) -> Vec<uuid::Uuid> {
            self.checkpointer
                .checkpoint_list
                .iter()
                .map(|cpm| cpm.uuid)
                .take(
                    self.checkpointer
                        .checkpoint_list
                        .len()
                        .saturating_sub(Checkpointer::MIN_CHECKPOINT_THRESHOLD),
                )
                .collect()
        }

        fn oldest_extra(&self) -> uuid::Uuid {
            self.extras().first().cloned().unwrap()
        }

        fn newest_extra(&self) -> uuid::Uuid {
            self.extras().last().cloned().unwrap()
        }
    }

    impl TestState<Empty> {
        fn new() -> Self {
            let tempdir = tempfile::tempdir().unwrap();

            let backend: Arc<dyn StorageBackend> = Arc::new(PosixBackend::new(
                tempdir.path(),
                StorageCacheConfig::default(),
                &FileBackendConfig::default(),
            ));

            Self {
                checkpointer: Checkpointer::new(backend).unwrap(),
                tempdir,
                _phantom: std::marker::PhantomData,
            }
        }

        fn precondition(&self) {
            assert_eq!(self.checkpointer.checkpoint_list.len(), 0);
        }

        fn checkpoint(mut self) -> TestState<MinCheckpoints> {
            self.precondition();

            for i in 0..Checkpointer::MIN_CHECKPOINT_THRESHOLD {
                self.checkpointer
                    .commit(uuid::Uuid::now_v7(), 0, None, Some(i as u64), Some(0))
                    .unwrap();
            }

            TestState::<MinCheckpoints> {
                checkpointer: self.checkpointer,
                tempdir: self.tempdir,
                _phantom: std::marker::PhantomData,
            }
        }
    }

    impl TestState<MinCheckpoints> {
        fn precondition(&self) {
            assert_eq!(
                self.checkpointer.checkpoint_list.len(),
                Checkpointer::MIN_CHECKPOINT_THRESHOLD
            );

            assert!(self.extras().is_empty());
        }

        fn checkpoint(mut self) -> TestState<ExtraCheckpoints> {
            self.precondition();

            let uuid = uuid::Uuid::now_v7();
            self.checkpointer
                .commit(uuid, 0, None, Some(2), Some(0))
                .unwrap();

            TestState::<ExtraCheckpoints> {
                checkpointer: self.checkpointer,
                tempdir: self.tempdir,
                _phantom: std::marker::PhantomData,
            }
        }
    }

    impl TestState<ExtraCheckpoints> {
        fn precondition(&self) {
            assert!(
                self.checkpointer.checkpoint_list.len() > Checkpointer::MIN_CHECKPOINT_THRESHOLD
            );

            assert!(!self.extras().is_empty());
        }

        fn checkpoint(mut self) -> TestState<ExtraCheckpoints> {
            self.precondition();

            self.checkpointer
                .commit(uuid::Uuid::now_v7(), 0, None, Some(3), Some(0))
                .unwrap();

            TestState::<ExtraCheckpoints> {
                checkpointer: self.checkpointer,
                tempdir: self.tempdir,
                _phantom: std::marker::PhantomData,
            }
        }

        fn gc(mut self) -> TestState<MinCheckpoints> {
            self.precondition();

            let removed = self.checkpointer.gc_checkpoint(HashSet::new()).unwrap();
            assert!(!removed.is_empty());

            TestState::<MinCheckpoints> {
                checkpointer: self.checkpointer,
                tempdir: self.tempdir,
                _phantom: std::marker::PhantomData,
            }
        }

        fn gc_with_except(mut self, except: uuid::Uuid) -> TestState<ExtraCheckpoints> {
            self.precondition();

            self.checkpointer.gc_checkpoint([except].into()).unwrap();

            assert!(self.extras().contains(&except));

            TestState::<ExtraCheckpoints> {
                checkpointer: self.checkpointer,
                tempdir: self.tempdir,
                _phantom: std::marker::PhantomData,
            }
        }
    }

    #[test]
    fn test_checkpointer() {
        // Empty checkpointer.
        let empty_checkpoints = TestState::<Empty>::new();

        // Add minimum number of checkpoints.
        let min_checkpoints = empty_checkpoints.checkpoint();

        // Add one extra checkpoint.
        let extra_checkpoints = min_checkpoints.checkpoint();

        // Veify we can GC back to minimum.
        let min_checkpoints = extra_checkpoints.gc();

        // Add two extra checkpoints.
        let one_extra = min_checkpoints.checkpoint();
        let two_extra = one_extra.checkpoint();

        // There should be more than the minimum number of checkpoints.
        let keep = two_extra.newest_extra();

        // And GC while keeping the newest extra checkpoint.
        // This should be the only extra checkpoint remaining.
        let one_extra = two_extra.gc_with_except(keep);
        assert!(one_extra.extras().contains(&keep) && one_extra.extras().len() == 1);

        // Add one extra checkpoint.
        let two_extra = one_extra.checkpoint();

        // Now lets try to GC while keeping the oldest extra checkpoint.
        let keep = two_extra.oldest_extra();
        let one_extra = two_extra.gc_with_except(keep);
        assert!(one_extra.extras().contains(&keep) && one_extra.extras().len() == 1);

        // Finally, GC back to minimum.
        let min_checkpoints = one_extra.gc();
        // Verify that this is a valid minimum checkpoint state.
        min_checkpoints.precondition();
    }
}
