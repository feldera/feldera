//! Logic to manage persistent checkpoints for a circuit.

use crate::dynamic::{self, data::DataTyped};
use crate::{Error, NumEntries, TypedBox};
use feldera_types::checkpoint::CheckpointMetadata;
use feldera_types::constants::{
    ACTIVATION_MARKER_FILE, ADHOC_TEMP_DIR, CHECKPOINT_DEPENDENCIES, CHECKPOINT_FILE_NAME,
    DBSP_FILE_EXTENSION, STATE_FILE, STATUS_FILE, STEPS_FILE,
};
use itertools::Itertools;
use size_of::SizeOf;

use std::io::ErrorKind;
use std::sync::atomic::Ordering;
use std::{
    collections::{HashSet, VecDeque},
    sync::Arc,
};

use crate::trace::Serializer;
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
        let usage = if !self.checkpoint_list.is_empty() {
            self.gc_startup()?
        } else {
            // There's no checkpoint file, or we couldn't read it. Don't run GC,
            // to ensure that we don't accidentally remove everything.
            //
            // We still know the amount of storage in use.
            self.measure_storage_use()?
        };

        // We measured the amount of storage in use. Give it to the backend as
        // the initial value.
        self.backend.usage().store(usage as i64, Ordering::Relaxed);

        Ok(())
    }

    fn measure_storage_use(&self) -> Result<u64, Error> {
        let mut usage = 0;
        StorageError::ignore_notfound(self.backend.list(
            &StoragePath::default(),
            &mut |_path, file_type| {
                if let StorageFileType::File { size } = file_type {
                    usage += size;
                }
            },
        ))?;
        Ok(usage)
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
            let batches = self
                .gather_batches_for_checkpoint(cpm)
                .expect("Batches for a checkpoint should be discoverable");
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

        self.backend
            .write_json(
                &Self::checkpoint_dir(uuid).child(CHECKPOINT_DEPENDENCIES),
                &batches.into_iter().map(|p| p.to_string()).collect_vec(),
            )
            .and_then(|reader| reader.commit())?;

        md.size = Some(self.measure_checkpoint_storage_use(uuid)?);

        self.checkpoint_list.push_back(md.clone());
        self.update_checkpoint_file()?;
        Ok(md)
    }

    /// List all currently available checkpoints.
    pub(super) fn list_checkpoints(&self) -> Result<Vec<CheckpointMetadata>, Error> {
        Ok(self.checkpoint_list.clone().into())
    }

    /// Reads the list of checkpoints available through `backend`.
    pub fn read_checkpoints(
        backend: &dyn StorageBackend,
    ) -> Result<VecDeque<CheckpointMetadata>, Error> {
        match backend.read_json(&StoragePath::from(CHECKPOINT_FILE_NAME)) {
            Ok(checkpoints) => Ok(checkpoints),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(VecDeque::new()),
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

        let mut batch_files_to_keep: HashSet<_> = except
            .iter()
            .filter_map(|uuid| self.backend.gather_batches_for_checkpoint_uuid(*uuid).ok())
            .flatten()
            .collect();

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

        // Find the first checkpoint in checkpoint list that is not in `except`.
        self.checkpoint_list
            .iter()
            .filter(|c| !except.contains(&c.uuid))
            .take(1)
            .filter_map(|c| self.backend.gather_batches_for_checkpoint(c).ok())
            .for_each(|batches| {
                for batch in batches {
                    batch_files_to_keep.insert(batch);
                }
            });

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
        let mut s = Serializer::default();
        let _r = self.serialize(&mut s).unwrap();
        let fbuf = s.into_serializer().into_inner();
        Ok(fbuf.into_vec())
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

    use feldera_storage::StorageBackend;
    use feldera_types::config::{FileBackendConfig, StorageCacheConfig};
    use std::collections::HashSet;

    use crate::storage::backend::posixio_impl::PosixBackend;

    use super::Checkpointer;

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
