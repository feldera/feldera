//! Logic to manage persistent checkpoints for a circuit.

use crate::dynamic::{self, data::DataTyped};
use crate::{Error, TypedBox};
use feldera_types::checkpoint::CheckpointMetadata;
use feldera_types::constants::{
    ACTIVATION_MARKER_FILE, ADHOC_TEMP_DIR, CHECKPOINT_DEPENDENCIES, CHECKPOINT_FILE_NAME,
    DBSP_FILE_EXTENSION, STATE_FILE, STEPS_FILE,
};
use itertools::Itertools;

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
    fingerprint: u64,
}

impl Checkpointer {
    /// We keep at least this many checkpoints around.
    pub(super) const MIN_CHECKPOINT_THRESHOLD: usize = 2;

    /// Create a new checkpointer for directory `storage_path`, delete any unreferenced
    /// files in the directory.
    ///
    /// If fingerprint_must_match is true, verify that any existing checkpoints in that
    /// directory have the given `fingerprint`.
    pub fn new(
        backend: Arc<dyn StorageBackend>,
        fingerprint: u64,
        fingerprint_must_match: bool,
    ) -> Result<Self, Error> {
        let checkpoint_list = Self::try_read_checkpoints(&*backend)?;

        let this = Checkpointer {
            backend,
            checkpoint_list,
            fingerprint,
        };

        this.init_storage()?;

        if fingerprint_must_match {
            for cpm in &this.checkpoint_list {
                if cpm.fingerprint != fingerprint {
                    return Err(Error::Runtime(RuntimeError::IncompatibleStorage));
                }
            }
        }

        Ok(this)
    }

    pub fn fingerprint(&self) -> u64 {
        self.fingerprint
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
            fingerprint: self.fingerprint,
            size: None,
            processed_records,
            steps,
        };

        let batches = self.gather_batches_for_checkpoint(&md)?;

        self.backend.write_json(
            &Self::checkpoint_dir(uuid).child(CHECKPOINT_DEPENDENCIES),
            &batches.into_iter().map(|p| p.to_string()).collect_vec(),
        )?;

        md.size = Some(self.measure_checkpoint_storage_use(uuid)?);

        self.checkpoint_list.push_back(md.clone());
        self.update_checkpoint_file()?;
        Ok(md)
    }

    /// List all currently available checkpoints.
    pub(super) fn list_checkpoints(&self) -> Result<Vec<CheckpointMetadata>, Error> {
        Ok(self.checkpoint_list.clone().into())
    }

    fn try_read_checkpoints(
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
            .write_json(&CHECKPOINT_FILE_NAME.into(), &self.checkpoint_list)?)
    }

    /// Removes `file` and logs any error.
    fn remove_batch_file(&self, file: &StoragePath) {
        match self.backend.delete_if_exists(file) {
            Ok(_) => {
                tracing::debug!("Removed file {file}");
            }
            Err(e) => {
                tracing::warn!("Unable to remove old-checkpoint file {file}: {e} (the pipeline will try to delete the file again on a restart)");
            }
        }
    }

    /// Removes all meta-data files associated with the checkpoint given by
    /// `cpm` by removing the folder associated with the checkpoint.
    fn remove_checkpoint_dir(&self, cpm: &CheckpointMetadata) -> Result<(), Error> {
        assert_ne!(cpm.uuid, Uuid::nil());
        self.backend
            .delete_recursive(&cpm.uuid.to_string().into())?;
        Ok(())
    }

    /// Remove the oldest checkpoint from the list.
    ///
    /// # Returns
    /// - Metadata of the removed checkpoint, if there are more than
    ///   `MIN_CHECKPOINT_THRESHOLD`
    /// - None otherwise.
    pub fn gc_checkpoint(&mut self) -> Result<Option<CheckpointMetadata>, Error> {
        // Ensures that we can unwrap the call to pop_front, and front later:
        static_assertions::const_assert!(Checkpointer::MIN_CHECKPOINT_THRESHOLD >= 2);
        if self.checkpoint_list.len() > Self::MIN_CHECKPOINT_THRESHOLD {
            let cp_to_remove = self.checkpoint_list.pop_front().unwrap();
            let next_in_line = self.checkpoint_list.front().unwrap();

            // Update the checkpoint list file, we do this first intentionally, in case
            // later operations fail we don't want the checkpoint list to
            // contain a checkpoint that only has part of the files.
            //
            // If any of the later operations fail, restarting the circuit will try
            // to remove the checkpoint files again (see also [`Self::gc_startup`]).
            self.update_checkpoint_file()?;

            let potentially_remove = self.gather_batches_for_checkpoint(&cp_to_remove)?;
            let need_to_keep = self.gather_batches_for_checkpoint(next_in_line)?;
            for file in potentially_remove.difference(&need_to_keep) {
                self.remove_batch_file(file);
            }
            self.remove_checkpoint_dir(&cp_to_remove)?;

            Ok(Some(cp_to_remove))
        } else {
            Ok(None)
        }
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
