//! Logic to manage persistent checkpoints for a circuit.

use super::RuntimeError;
use crate::dynamic::{self, data::DataTyped, DataTrait, WeightTrait};
use crate::trace::ord::{vec::VecIndexedWSet, FallbackIndexedWSet};
use crate::{Error, TypedBox};

use std::io::ErrorKind;
use std::{
    collections::{HashSet, VecDeque},
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::trace::Serializer;
use feldera_storage::{StorageBackend, StorageFileType};
use rkyv::{Archive, Deserialize, Serialize};
use serde::{Deserialize as SerdeDeserialize, Serialize as SerdeSerialize};
use uuid::Uuid;

/// Holds meta-data about a checkpoint that was taken for persistent storage
/// and recovery of a circuit's state.
#[derive(
    Debug, Clone, Default, Serialize, Deserialize, Archive, SerdeSerialize, SerdeDeserialize,
)]
pub struct CheckpointMetadata {
    /// A unique identifier for the given checkpoint.
    ///
    /// This is used to identify the checkpoint in the file-system hierarchy.
    pub uuid: Uuid,
    /// An optional name for the checkpoint.
    pub identifier: Option<String>,
    /// Fingerprint of the circuit at the time of the checkpoint.
    pub fingerprint: u64,
}

impl CheckpointMetadata {
    pub fn new(uuid: Uuid, identifier: Option<String>, fingerprint: u64) -> Self {
        CheckpointMetadata {
            uuid,
            identifier,
            fingerprint,
        }
    }
}

/// A "checkpointer" is responsible for the creation, and removal of
/// checkpoints for a circuit.
///
/// It handles list of available checkpoints, and the files associated
/// with each checkpoint.
#[derive(derive_more::Debug)]
pub(crate) struct Checkpointer {
    #[debug(skip)]
    backend: Arc<dyn StorageBackend>,
    checkpoint_list: VecDeque<CheckpointMetadata>,
    fingerprint: u64,
}

impl Checkpointer {
    /// We keep at least this many checkpoints around.
    pub(super) const MIN_CHECKPOINT_THRESHOLD: usize = 2;
    /// Name of the checkpoint list file.
    ///
    /// File will be stored inside the runtime storage directory with this
    /// name.
    const CHECKPOINT_FILE_NAME: &'static str = "checkpoints.feldera";
    /// A slice of all file-extension the system can create.
    const DBSP_FILE_EXTENSION: &'static [&'static str] = &["mut", "feldera"];

    /// Create a new checkpointer for directory `storage_path`, verify that any
    /// existing checkpoints in that directory have the given `fingerprint`, and
    /// delete any unreferenced files in the directory.
    pub fn new(backend: Arc<dyn StorageBackend>, fingerprint: u64) -> Result<Self, Error> {
        let checkpoint_list = Self::try_read_checkpoints(&*backend)?;

        let this = Checkpointer {
            backend,
            checkpoint_list,
            fingerprint,
        };
        this.gc_startup()?;
        for cpm in &this.checkpoint_list {
            if cpm.fingerprint != fingerprint {
                return Err(Error::Runtime(RuntimeError::IncompatibleStorage));
            }
        }

        Ok(this)
    }

    pub fn fingerprint(&self) -> u64 {
        self.fingerprint
    }

    /// Remove unexpected/leftover files from a previous run in the storage
    /// directory.
    fn gc_startup(&self) -> Result<(), Error> {
        if self.checkpoint_list.is_empty() {
            // This is a safety measure we take to ensure that we don't
            // accidentally remove files just in case we fail to read the checkpoint
            // file. We don't remove anything.
            return Ok(());
        }

        // Collect all directories and files still referenced by a checkpoint
        let mut in_use_paths: HashSet<PathBuf> = HashSet::new();
        in_use_paths.insert(Checkpointer::CHECKPOINT_FILE_NAME.into());
        in_use_paths.insert("steps.bin".into());
        for cpm in self.checkpoint_list.iter() {
            in_use_paths.insert(cpm.uuid.to_string().into());
            let batches = self
                .gather_batches_for_checkpoint(cpm)
                .expect("Batches for a checkpoint should be discoverable");
            for batch in batches {
                in_use_paths.insert(batch.into());
            }
        }

        /// True if `path` is a name that we might have created ourselves.
        fn is_feldera_filename(path: &Path) -> bool {
            let extension = &path
                .extension()
                .unwrap_or_default()
                .to_str()
                .unwrap_or_default();
            Checkpointer::DBSP_FILE_EXTENSION.contains(extension)
        }

        // Collect everything found in the storage directory
        self.backend.list(Path::new(""), &mut |path, file_type| {
            if !in_use_paths.contains(path) && (is_feldera_filename(path) || file_type == StorageFileType::Directory){
                match self.backend.delete_recursive(path) {
                    Ok(_) => {
                        tracing::debug!("Removed unused {file_type:?} '{}'", path.display());
                    }
                    Err(e) => {
                        tracing::warn!("Unable to remove old-checkpoint file {}: {} (the pipeline will try to delete the file again on a restart)", path.display(), e);
                    }
            }
            }})?;

        Ok(())
    }

    pub(super) fn checkpoint_dir(&self, uuid: Uuid) -> PathBuf {
        uuid.to_string().into()
    }

    pub(super) fn commit(
        &mut self,
        uuid: Uuid,
        identifier: Option<String>,
    ) -> Result<CheckpointMetadata, Error> {
        let md = CheckpointMetadata {
            uuid,
            identifier,
            fingerprint: self.fingerprint,
        };
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
        match backend.read(Path::new(Self::CHECKPOINT_FILE_NAME)) {
            Ok(content) => {
                let archived =
                    unsafe { rkyv::archived_root::<VecDeque<CheckpointMetadata>>(&content) };
                let checkpoint_list: VecDeque<CheckpointMetadata> =
                    archived.deserialize(&mut rkyv::Infallible).unwrap();
                Ok(checkpoint_list)
            }
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(VecDeque::new()),
            Err(error) => Err(error)?,
        }
    }

    pub(super) fn gather_batches_for_checkpoint(
        &self,
        cpm: &CheckpointMetadata,
    ) -> Result<HashSet<String>, Error> {
        assert!(!cpm.uuid.is_nil());

        let mut spines = Vec::new();
        self.backend
            .list(Path::new(&cpm.uuid.to_string()), &mut |path, _file_type| {
                if path
                    .file_name()
                    .unwrap_or_default()
                    .as_encoded_bytes()
                    .starts_with(b"pspine-batches")
                {
                    spines.push(path.to_path_buf());
                }
            })?;

        let mut batch_files_in_commit: HashSet<String> = HashSet::new();
        for spine in spines {
            let content = self.backend.read(&spine)?;
            let archived = rkyv::check_archived_root::<Vec<String>>(&content).unwrap();
            for batch in archived.iter() {
                match self.backend.exists(&Path::new(&batch.to_string())) {
                    Ok(true) => {
                        batch_files_in_commit.insert(batch.to_string());
                    }
                    _ => (),
                }
            }
        }
        Ok(batch_files_in_commit)
    }

    fn update_checkpoint_file(&self) -> Result<(), Error> {
        let content = crate::storage::file::to_bytes(&self.checkpoint_list)
            .expect("failed to serialize checkpoint-list data");
        self.backend
            .write(&Path::new(Self::CHECKPOINT_FILE_NAME), content)?;

        Ok(())
    }

    /// Removes all the files in `files` from the storage base directory.
    fn remove_batch_files<P: AsRef<Path>>(&self, files: &Vec<P>) -> Result<(), Error> {
        for file in files {
            match self.backend.delete(file.as_ref()) {
                Ok(_) => {
                    tracing::debug!("Removed file {}", file.as_ref().display());
                }
                Err(e) => {
                    tracing::warn!("Unable to remove old-checkpoint file {}: {} (the pipeline will try to delete the file again on a restart)", file.as_ref().display(), e);
                }
            }
        }
        Ok(())
    }

    /// Removes all meta-data files associated with the checkpoint given by
    /// `cpm` by removing the folder associated with the checkpoint.
    fn remove_checkpoint_dir(&self, cpm: &CheckpointMetadata) -> Result<(), Error> {
        assert_ne!(cpm.uuid, Uuid::nil());
        self.backend
            .delete_recursive(&Path::new(&cpm.uuid.to_string()))?;
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
            let to_remove = potentially_remove
                .difference(&need_to_keep)
                .map(PathBuf::from)
                .collect::<Vec<_>>();

            self.remove_batch_files(&to_remove)?;
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

impl<K, V, R> Checkpoint for VecIndexedWSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn checkpoint(&self) -> Result<Vec<u8>, Error> {
        todo!()
    }

    fn restore(&mut self, _data: &[u8]) -> Result<(), Error> {
        todo!()
    }
}

impl<K, V, R> Checkpoint for FallbackIndexedWSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn checkpoint(&self) -> Result<Vec<u8>, Error> {
        todo!()
    }

    fn restore(&mut self, _data: &[u8]) -> Result<(), Error> {
        todo!()
    }
}
